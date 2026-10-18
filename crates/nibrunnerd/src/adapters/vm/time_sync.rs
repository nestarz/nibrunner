use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::io::{AsyncWriteExt, BufReader};
use tokio::net::UnixStream;
use tokio::time::Instant;

use crate::domain::metrics::sleep_wake::WakePhase;
use crate::ports::VmError;

// A guest that is draining requests may take its time to agree to be held.
const FREEZE_TIMEOUT: Duration = Duration::from_millis(guest_contract::control::TENANT_CONTROL_TIMEOUT_MS);
// A resumed guest's init answers its control port in well under ten milliseconds; a second is a
// guest, or a vsock device, that will not.
pub(super) const DIAL_BUDGET: Duration = Duration::from_secs(1);
const REDIAL_INTERVAL: Duration = Duration::from_millis(10);
pub(super) const SYNC_BUDGET: Duration = Duration::from_secs(1);

/// The stretch of a wake that did not complete inside its budget, and what it saw.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{} did not complete: {reason}", phase.as_str())]
pub(super) struct WakeStalled {
    pub phase: WakePhase,
    pub reason: String,
}

impl WakeStalled {
    pub(super) fn new(phase: WakePhase, reason: impl std::fmt::Display) -> Self {
        Self {
            phase,
            reason: reason.to_string(),
        }
    }
}

/// How long the two guest stretches of a wake took.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct Synced {
    pub dial: Duration,
    pub sync: Duration,
}

fn failed(reason: impl std::fmt::Display) -> VmError {
    VmError::Host(format!("guest clock synchronization failed: {reason}"))
}

fn millis(budget: Duration) -> u128 {
    budget.as_millis()
}

async fn line(wire: &mut BufReader<UnixStream>, deadline: Instant, awaited: &str) -> Result<String, String> {
    crate::domain::guest_line::read(wire, deadline.saturating_duration_since(Instant::now()))
        .await
        .ok_or_else(|| format!("{awaited} was missing, invalid or not sent in time"))
}

/// The guest's control port, through the VMM's vsock socket, within `budget`. A guest that hangs
/// up on `CONNECT` is not listening yet and is dialled again; one that never answers it is not
/// coming back inside the budget.
async fn dial(path: &Path, budget: Duration) -> Result<BufReader<UnixStream>, String> {
    let port = guest_contract::vsock::GUEST_CONTROL_VSOCK_PORT;
    let deadline = Instant::now() + budget;
    let mut last;
    loop {
        match crate::unix_socket::connect(path).await {
            Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => {
                return Err(error.to_string())
            }
            Err(error) => {
                tracing::debug!(%error, socket = %path.display(), "waiting for guest control socket");
                last = error.to_string();
            }
            Ok(stream) => {
                let mut wire = BufReader::new(stream);
                let asked = wire
                    .get_mut()
                    .write_all(guest_contract::vsock::connect_request(port).as_bytes())
                    .await;
                match asked {
                    Err(error) => last = error.to_string(),
                    Ok(()) => match line(&mut wire, deadline, "the VMM's reply to CONNECT").await {
                        Ok(reply) => match guest_contract::vsock::read_connect_reply(&reply, port) {
                            Ok(()) => return Ok(wire),
                            Err(error) => last = error.to_string(),
                        },
                        Err(_) if Instant::now() >= deadline => {
                            return Err(format!(
                                "CONNECT {port} on {} had no answer within {} ms",
                                path.display(),
                                millis(budget)
                            ));
                        }
                        Err(error) => last = error,
                    },
                }
            }
        }
        if Instant::now() + REDIAL_INTERVAL >= deadline {
            return Err(format!(
                "CONNECT {port} on {} was not accepted within {} ms: {last}",
                path.display(),
                millis(budget)
            ));
        }
        tokio::time::sleep(REDIAL_INTERVAL).await;
    }
}

pub(super) async fn freeze_tenant(path: &Path) -> Result<(), VmError> {
    tokio::time::timeout(FREEZE_TIMEOUT, freeze_connected_tenant(path))
        .await
        .map_err(|_| failed("tenant freeze timed out"))?
}

async fn freeze_connected_tenant(path: &Path) -> Result<(), VmError> {
    let deadline = Instant::now() + FREEZE_TIMEOUT;
    let mut wire = dial(path, FREEZE_TIMEOUT).await.map_err(failed)?;
    wire.get_mut()
        .write_all(format!("{}\n", guest_contract::control::TENANT_FREEZE_REQUEST).as_bytes())
        .await
        .map_err(failed)?;
    if line(&mut wire, deadline, "the guest's reply to SLEEP")
        .await
        .map_err(failed)?
        != guest_contract::control::TENANT_FREEZE_READY
    {
        return Err(failed("the guest does not support tenant freezing"));
    }
    wire.get_mut()
        .write_all(format!("{}\n", guest_contract::control::TENANT_FREEZE_COMMIT).as_bytes())
        .await
        .map_err(failed)?;
    if line(&mut wire, deadline, "the guest's reply to HOLD")
        .await
        .map_err(failed)?
        != guest_contract::control::TENANT_FREEZE_HELD
    {
        return Err(failed("the tenant cgroup would not freeze"));
    }
    Ok(())
}

/// Sets a resumed guest's clock and releases its tenant: the dial, then the sync, each within its
/// own budget, so a wake that stalls says where.
pub(super) async fn wake(path: &Path) -> Result<Synced, WakeStalled> {
    let dialing = Instant::now();
    let mut wire = dial(path, DIAL_BUDGET)
        .await
        .map_err(|reason| WakeStalled::new(WakePhase::Dial, reason))?;
    let dial = dialing.elapsed();
    let syncing = Instant::now();
    release_connected_tenant(&mut wire, syncing + SYNC_BUDGET)
        .await
        .map_err(|reason| WakeStalled::new(WakePhase::Sync, reason))?;
    Ok(Synced {
        dial,
        sync: syncing.elapsed(),
    })
}

async fn release_connected_tenant(wire: &mut BufReader<UnixStream>, deadline: Instant) -> Result<(), String> {
    use guest_contract::control::{
        TENANT_CLOCK_READY, TENANT_CLOCK_RELEASE, TENANT_CLOCK_RELEASED, TENANT_CLOCK_REQUEST,
    };
    let budget = millis(SYNC_BUDGET);
    wire.get_mut()
        .write_all(format!("{TENANT_CLOCK_REQUEST}\n").as_bytes())
        .await
        .map_err(|error| error.to_string())?;
    let reply = line(
        wire,
        deadline,
        &format!("{TENANT_CLOCK_READY} to {TENANT_CLOCK_REQUEST} within {budget} ms"),
    )
    .await?;
    if reply != TENANT_CLOCK_READY {
        return Err(format!("the guest answered {TENANT_CLOCK_REQUEST} with {reply}"));
    }
    let sent = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| error.to_string())?;
    wire.get_mut()
        .write_all(format!("{TENANT_CLOCK_RELEASE}{}\n", sent.as_nanos()).as_bytes())
        .await
        .map_err(|error| error.to_string())?;
    let released = line(
        wire,
        deadline,
        &format!(
            "{TENANT_CLOCK_RELEASED} to {}<ns> within {budget} ms",
            TENANT_CLOCK_RELEASE.trim_end()
        ),
    )
    .await?;
    if released != TENANT_CLOCK_RELEASED {
        return Err("the tenant cgroup was not released".into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncBufReadExt;
    use tokio::net::UnixListener;

    async fn heard(wire: &mut BufReader<UnixStream>) -> String {
        crate::domain::guest_line::read(wire, Duration::from_secs(5))
            .await
            .expect("the host sent a line")
    }

    async fn guest(listener: UnixListener, ready: bool) -> Option<String> {
        let (stream, _) = listener
            .accept()
            .await
            .expect("guest control socket accepts the host");
        let mut wire = BufReader::new(stream);
        assert_eq!(heard(&mut wire).await, "CONNECT 51001");
        wire.get_mut().write_all(b"OK 1234\n").await.unwrap();
        let request = heard(&mut wire).await;
        assert_eq!(request, guest_contract::control::TENANT_CLOCK_REQUEST);
        wire.get_mut()
            .write_all(if ready { b"READY\n" } else { b"REFUSED\n" })
            .await
            .unwrap();
        let mut reply = String::new();
        wire.read_line(&mut reply).await.unwrap();
        if reply.starts_with(guest_contract::control::TENANT_CLOCK_RELEASE) {
            wire.get_mut().write_all(b"OK\n").await.unwrap();
            Some(reply)
        } else {
            None
        }
    }

    #[tokio::test]
    async fn a_restored_guest_is_released_after_its_clock_is_set() {
        let directory = tempfile::tempdir().unwrap();
        let socket = directory.path().join("control.vsock");
        let listener = UnixListener::bind(&socket).unwrap();
        let answer = tokio::spawn(guest(listener, true));

        wake(&socket).await.unwrap();

        let release = answer.await.unwrap().unwrap();
        let timestamp = release
            .trim()
            .strip_prefix(guest_contract::control::TENANT_CLOCK_RELEASE)
            .unwrap()
            .parse::<u128>()
            .unwrap();
        assert!(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
                .abs_diff(timestamp)
                < Duration::from_secs(1).as_nanos()
        );
    }

    #[tokio::test]
    async fn the_host_samples_time_after_a_delayed_guest_says_ready() {
        let directory = tempfile::tempdir().unwrap();
        let socket = directory.path().join("control.vsock");
        let listener = UnixListener::bind(&socket).unwrap();
        let answer = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut wire = BufReader::new(stream);
            assert_eq!(heard(&mut wire).await, "CONNECT 51001");
            wire.get_mut().write_all(b"OK 1234\n").await.unwrap();
            assert_eq!(heard(&mut wire).await, "WAKE");
            tokio::time::sleep(Duration::from_millis(50)).await;
            let ready_at = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
            wire.get_mut().write_all(b"READY\n").await.unwrap();
            let release = heard(&mut wire).await;
            let timestamp = release.strip_prefix("GO ").unwrap().parse::<u128>().unwrap();
            assert!(timestamp >= ready_at);
            wire.get_mut().write_all(b"OK\n").await.unwrap();
        });
        wake(&socket).await.unwrap();
        answer.await.unwrap();
    }

    #[tokio::test]
    async fn a_guest_that_cannot_set_its_clock_is_not_released() {
        let directory = tempfile::tempdir().unwrap();
        let socket = directory.path().join("control.vsock");
        let listener = UnixListener::bind(&socket).unwrap();
        let answer = tokio::spawn(guest(listener, false));

        let failure = wake(&socket).await.unwrap_err();

        assert_eq!(failure.phase, WakePhase::Sync);
        assert!(failure.to_string().contains("REFUSED"), "{failure}");
        assert_eq!(answer.await.unwrap(), None);
    }

    #[tokio::test]
    async fn a_guest_that_never_answers_connect_stalls_in_the_dial_within_its_budget() {
        let directory = tempfile::tempdir().unwrap();
        let socket = directory.path().join("control.vsock");
        let listener = UnixListener::bind(&socket).unwrap();
        let _silent = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut wire = BufReader::new(stream);
            assert_eq!(heard(&mut wire).await, "CONNECT 51001");
            std::future::pending::<()>().await;
            drop(wire);
        });

        let started = std::time::Instant::now();
        let failure = wake(&socket).await.unwrap_err();

        assert_eq!(failure.phase, WakePhase::Dial);
        assert!(
            failure
                .to_string()
                .starts_with("dial did not complete: CONNECT 51001"),
            "{failure}"
        );
        assert!(started.elapsed() >= DIAL_BUDGET);
        assert!(started.elapsed() < DIAL_BUDGET + Duration::from_millis(500));
    }

    #[tokio::test]
    async fn a_guest_that_hangs_up_on_connect_is_dialled_again_until_it_listens() {
        let directory = tempfile::tempdir().unwrap();
        let socket = directory.path().join("control.vsock");
        let listener = UnixListener::bind(&socket).unwrap();
        let answer = tokio::spawn(async move {
            for _ in 0..3 {
                let (stream, _) = listener.accept().await.unwrap();
                drop(stream);
            }
            guest(listener, true).await
        });

        let synced = wake(&socket).await.unwrap();

        assert!(answer.await.unwrap().is_some());
        assert!(synced.dial < DIAL_BUDGET);
    }

    #[tokio::test]
    async fn a_guest_that_accepts_the_dial_but_never_says_ready_stalls_in_the_sync() {
        let directory = tempfile::tempdir().unwrap();
        let socket = directory.path().join("control.vsock");
        let listener = UnixListener::bind(&socket).unwrap();
        let _mute = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut wire = BufReader::new(stream);
            assert_eq!(heard(&mut wire).await, "CONNECT 51001");
            wire.get_mut().write_all(b"OK 1234\n").await.unwrap();
            assert_eq!(heard(&mut wire).await, "WAKE");
            std::future::pending::<()>().await;
            drop(wire);
        });

        let started = std::time::Instant::now();
        let failure = wake(&socket).await.unwrap_err();

        assert_eq!(failure.phase, WakePhase::Sync);
        assert!(
            failure.to_string().contains("READY to WAKE within 1000 ms"),
            "{failure}"
        );
        assert!(started.elapsed() < SYNC_BUDGET + Duration::from_millis(500));
    }
}
