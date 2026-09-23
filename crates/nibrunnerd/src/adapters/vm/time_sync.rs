use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;

use crate::ports::VmError;

const TIMEOUT: Duration = Duration::from_secs(5);
const MAX_CLOCK_ERROR: Duration = Duration::from_secs(1);

fn failed(reason: impl std::fmt::Display) -> VmError {
    VmError::Host(format!("guest clock synchronization failed: {reason}"))
}

async fn line(wire: &mut BufReader<UnixStream>) -> Result<String, VmError> {
    let mut reply = String::new();
    tokio::time::timeout(TIMEOUT, wire.read_line(&mut reply))
        .await
        .map_err(|_| failed("guest control reply timed out"))?
        .map_err(failed)?;
    if reply.is_empty() {
        return Err(failed("guest control connection closed"));
    }
    Ok(reply.trim_end().to_string())
}

async fn connect(path: &Path) -> Result<BufReader<UnixStream>, VmError> {
    let deadline = tokio::time::Instant::now() + TIMEOUT;
    let stream = loop {
        match UnixStream::connect(path).await {
            Ok(stream) => break stream,
            Err(error) if tokio::time::Instant::now() < deadline => {
                tracing::debug!(%error, socket = %path.display(), "waiting for guest control socket");
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            Err(error) => return Err(failed(error)),
        }
    };
    let mut wire = BufReader::new(stream);
    wire.get_mut()
        .write_all(
            guest_contract::vsock::connect_request(guest_contract::vsock::GUEST_CONTROL_VSOCK_PORT)
                .as_bytes(),
        )
        .await
        .map_err(failed)?;
    guest_contract::vsock::read_connect_reply(
        &line(&mut wire).await?,
        guest_contract::vsock::GUEST_CONTROL_VSOCK_PORT,
    )
    .map_err(failed)?;
    Ok(wire)
}

pub(super) async fn sleep(path: &Path) -> Result<(), VmError> {
    let mut wire = connect(path).await?;
    wire.get_mut().write_all(b"SLEEP\n").await.map_err(failed)?;
    if line(&mut wire).await? != "OK" {
        return Err(failed("the tenant cgroup would not freeze"));
    }
    Ok(())
}

pub(super) async fn wake(path: &Path) -> Result<(), VmError> {
    let mut wire = connect(path).await?;
    let sent = SystemTime::now().duration_since(UNIX_EPOCH).map_err(failed)?;
    wire.get_mut()
        .write_all(format!("WAKE {}\n", sent.as_nanos()).as_bytes())
        .await
        .map_err(failed)?;
    let reply = line(&mut wire).await?;
    let guest_nanos = reply
        .strip_prefix("READY ")
        .ok_or_else(|| failed(&reply))?
        .parse::<u128>()
        .map_err(failed)?;
    let host_nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(failed)?
        .as_nanos();
    if host_nanos.abs_diff(guest_nanos) > MAX_CLOCK_ERROR.as_nanos() {
        return Err(failed(
            "guest clock differs from the host by more than one second",
        ));
    }
    wire.get_mut().write_all(b"GO\n").await.map_err(failed)?;
    if line(&mut wire).await? != "OK" {
        return Err(failed("the tenant cgroup was not released"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::UnixListener;

    async fn guest(listener: UnixListener, clock_lag: Duration) -> Option<String> {
        let (stream, _) = listener
            .accept()
            .await
            .expect("guest control socket accepts the host");
        let mut wire = BufReader::new(stream);
        assert_eq!(line(&mut wire).await.unwrap(), "CONNECT 51001");
        wire.get_mut().write_all(b"OK 1234\n").await.unwrap();
        let request = line(&mut wire).await.unwrap();
        let host_nanos = request
            .strip_prefix("WAKE ")
            .expect("the host sends a clock value")
            .parse::<u128>()
            .unwrap();
        wire.get_mut()
            .write_all(format!("READY {}\n", host_nanos - clock_lag.as_nanos()).as_bytes())
            .await
            .unwrap();
        let mut reply = String::new();
        wire.read_line(&mut reply).await.unwrap();
        if reply == "GO\n" {
            wire.get_mut().write_all(b"OK\n").await.unwrap();
            Some(reply)
        } else {
            None
        }
    }

    #[tokio::test]
    async fn a_restored_guest_is_released_after_its_clock_is_checked() {
        let directory = tempfile::tempdir().unwrap();
        let socket = directory.path().join("control.vsock");
        let listener = UnixListener::bind(&socket).unwrap();
        let answer = tokio::spawn(guest(listener, Duration::ZERO));

        wake(&socket).await.unwrap();

        assert_eq!(answer.await.unwrap(), Some("GO\n".to_string()));
    }

    #[tokio::test]
    async fn a_restored_guest_with_stale_time_is_not_released() {
        let directory = tempfile::tempdir().unwrap();
        let socket = directory.path().join("control.vsock");
        let listener = UnixListener::bind(&socket).unwrap();
        let answer = tokio::spawn(guest(listener, Duration::from_secs(60)));

        let failure = wake(&socket).await.unwrap_err();

        assert!(failure.message().contains("differs from the host"));
        assert_eq!(answer.await.unwrap(), None);
    }
}
