use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;

use crate::ports::VmError;

const TIMEOUT: Duration = Duration::from_secs(5);

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

pub(super) async fn freeze_tenant(path: &Path) -> Result<(), VmError> {
    let mut wire = connect(path).await?;
    wire.get_mut()
        .write_all(format!("{}\n", guest_contract::control::TENANT_FREEZE_REQUEST).as_bytes())
        .await
        .map_err(failed)?;
    if line(&mut wire).await? != guest_contract::control::TENANT_FREEZE_HELD {
        return Err(failed("the tenant cgroup would not freeze"));
    }
    Ok(())
}

pub(super) async fn wake(path: &Path) -> Result<(), VmError> {
    let mut wire = connect(path).await?;
    let sent = SystemTime::now().duration_since(UNIX_EPOCH).map_err(failed)?;
    wire.get_mut()
        .write_all(
            format!(
                "{}{}\n",
                guest_contract::control::TENANT_CLOCK_REQUEST,
                sent.as_nanos()
            )
            .as_bytes(),
        )
        .await
        .map_err(failed)?;
    let reply = line(&mut wire).await?;
    if reply != guest_contract::control::TENANT_CLOCK_READY {
        return Err(failed(&reply));
    }
    wire.get_mut()
        .write_all(format!("{}\n", guest_contract::control::TENANT_CLOCK_RELEASE).as_bytes())
        .await
        .map_err(failed)?;
    if line(&mut wire).await? != guest_contract::control::TENANT_CLOCK_RELEASED {
        return Err(failed("the tenant cgroup was not released"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::UnixListener;

    async fn guest(listener: UnixListener, ready: bool) -> Option<String> {
        let (stream, _) = listener
            .accept()
            .await
            .expect("guest control socket accepts the host");
        let mut wire = BufReader::new(stream);
        assert_eq!(line(&mut wire).await.unwrap(), "CONNECT 51001");
        wire.get_mut().write_all(b"OK 1234\n").await.unwrap();
        let request = line(&mut wire).await.unwrap();
        request
            .strip_prefix(guest_contract::control::TENANT_CLOCK_REQUEST)
            .expect("the host sends a clock value")
            .parse::<u128>()
            .unwrap();
        wire.get_mut()
            .write_all(if ready { b"READY\n" } else { b"REFUSED\n" })
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
    async fn a_restored_guest_is_released_after_its_clock_is_set() {
        let directory = tempfile::tempdir().unwrap();
        let socket = directory.path().join("control.vsock");
        let listener = UnixListener::bind(&socket).unwrap();
        let answer = tokio::spawn(guest(listener, true));

        wake(&socket).await.unwrap();

        assert_eq!(answer.await.unwrap(), Some("GO\n".to_string()));
    }

    #[tokio::test]
    async fn a_guest_that_cannot_set_its_clock_is_not_released() {
        let directory = tempfile::tempdir().unwrap();
        let socket = directory.path().join("control.vsock");
        let listener = UnixListener::bind(&socket).unwrap();
        let answer = tokio::spawn(guest(listener, false));

        let failure = wake(&socket).await.unwrap_err();

        assert!(failure.message().contains("REFUSED"));
        assert_eq!(answer.await.unwrap(), None);
    }
}
