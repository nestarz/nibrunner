use guest_contract::channels::ChannelEndpoint;
use std::time::Duration;

use protocol::AppId;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;

const FREEZE_REQUEST: &str = "FREEZE\n";
const FREEZE_HELD: &str = "OK";

const REPLY_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug, thiserror::Error)]
pub enum FreezeError {
    #[error("{socket_path} took the request and never answered")]
    Silent { socket_path: String },
    #[error("the guest running {app_id} would not freeze its filesystem: {reply}")]
    Refused { app_id: AppId, reply: String },
    #[error("the guest running {app_id} thawed before the checkpoint was recorded")]
    Lost { app_id: AppId },
}

impl FreezeError {
    pub fn message(&self) -> String {
        self.to_string()
    }
}

pub struct FreezeLease {
    app_id: AppId,
    held: Option<UnixStream>,
}

impl FreezeLease {
    pub fn assert_held(&self) -> Result<(), FreezeError> {
        let Some(stream) = &self.held else {
            return Ok(());
        };
        let mut byte = [0u8; 1];
        match stream.try_read(&mut byte) {
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => Ok(()),
            _ => Err(FreezeError::Lost {
                app_id: self.app_id.clone(),
            }),
        }
    }
}

pub async fn frozen(app_id: &AppId, endpoint: &ChannelEndpoint) -> Result<FreezeLease, FreezeError> {
    let Ok(stream) = UnixStream::connect(&endpoint.path).await else {
        tracing::info!(
            %app_id,
            socket_path = %endpoint.path.display(),
            "no running guest to freeze; reading the volume as it lies"
        );
        return Ok(FreezeLease {
            app_id: app_id.clone(),
            held: None,
        });
    };
    let socket_path = endpoint.path.display().to_string();
    let silent = || FreezeError::Silent {
        socket_path: socket_path.clone(),
    };

    let mut wire = BufReader::new(stream);
    if let Some(port) = endpoint.vsock_port {
        let connect = guest_contract::vsock::connect_request(port);
        write_all(&mut wire, connect.as_bytes())
            .await
            .map_err(|()| silent())?;
        let reply = read_line(&mut wire).await.ok_or_else(silent)?;
        guest_contract::vsock::read_connect_reply(&reply, port).map_err(|error| FreezeError::Refused {
            app_id: app_id.clone(),
            reply: error.to_string(),
        })?;
    }

    write_all(&mut wire, FREEZE_REQUEST.as_bytes())
        .await
        .map_err(|()| silent())?;
    let reply = read_line(&mut wire).await.ok_or_else(silent)?;
    if reply != FREEZE_HELD {
        return Err(FreezeError::Refused {
            app_id: app_id.clone(),
            reply,
        });
    }
    tracing::info!(%app_id, "the guest froze its filesystem");
    Ok(FreezeLease {
        app_id: app_id.clone(),
        held: Some(wire.into_inner()),
    })
}

async fn write_all(wire: &mut BufReader<UnixStream>, bytes: &[u8]) -> Result<(), ()> {
    wire.get_mut().write_all(bytes).await.map_err(|_| ())
}

async fn read_line(wire: &mut BufReader<UnixStream>) -> Option<String> {
    let mut line = String::new();
    match tokio::time::timeout(REPLY_TIMEOUT, wire.read_line(&mut line)).await {
        Ok(Ok(read)) if read > 0 => Some(line.trim_end().to_string()),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    fn endpoint(path: &std::path::Path) -> guest_contract::channels::ChannelEndpoint {
        guest_contract::channels::ChannelEndpoint {
            path: path.into(),
            vsock_port: Some(guest_contract::vsock::GUEST_CONTROL_VSOCK_PORT),
        }
    }

    use super::*;
    use crate::test_support::app_id;
    use tokio::net::UnixListener;

    async fn guest_that(
        answers: &'static [&'static str],
        hangs_up: bool,
    ) -> (tempfile::TempDir, std::path::PathBuf) {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("guest.vsock");
        let listener = UnixListener::bind(&path).unwrap();
        tokio::spawn(async move {
            let Ok((stream, _)) = listener.accept().await else {
                return;
            };
            let mut wire = BufReader::new(stream);
            for answer in answers {
                let mut request = String::new();
                if wire.read_line(&mut request).await.unwrap_or(0) == 0 {
                    return;
                }
                let _ = wire.get_mut().write_all(format!("{answer}\n").as_bytes()).await;
            }
            if hangs_up {
                return;
            }
            std::future::pending::<()>().await;
        });
        (directory, path)
    }

    #[tokio::test]
    async fn a_unix_runtime_holds_the_same_export_lease_until_the_client_disconnects() {
        use tokio::io::AsyncReadExt;
        let directory = tempfile::tempdir().unwrap();
        let endpoint = guest_contract::channels::ChannelTransport::Unix
            .endpoint(directory.path(), guest_contract::channels::Channel::Control);
        let listener = UnixListener::bind(&endpoint.path).unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut wire = BufReader::new(stream);
            let mut request = String::new();
            wire.read_line(&mut request).await.unwrap();
            assert_eq!(request, FREEZE_REQUEST);
            wire.get_mut().write_all(b"OK\n").await.unwrap();
            let mut byte = [0];
            assert_eq!(wire.read(&mut byte).await.unwrap(), 0);
        });
        let lease = frozen(&app_id(), &endpoint).await.unwrap();
        lease.assert_held().unwrap();
        drop(lease);
        tokio::time::timeout(Duration::from_secs(1), server)
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn no_vmm_to_ask_is_a_lease_over_nothing_rather_than_a_failure() {
        let directory = tempfile::tempdir().unwrap();
        let lease = frozen(&app_id(), &endpoint(&directory.path().join("nothing-here.vsock")))
            .await
            .unwrap();
        assert!(lease.assert_held().is_ok());
    }

    #[tokio::test]
    async fn a_guest_that_takes_the_freeze_holds_it_until_this_side_lets_go() {
        let (_directory, path) = guest_that(&["OK 1234", "OK"], false).await;
        let lease = frozen(&app_id(), &endpoint(&path)).await.unwrap();
        assert!(lease.assert_held().is_ok());
    }

    #[tokio::test]
    async fn a_guest_that_thawed_early_is_a_lease_that_cannot_be_vouched_for() {
        let (_directory, path) = guest_that(&["OK 1234", "OK"], true).await;
        let lease = frozen(&app_id(), &endpoint(&path)).await.unwrap();
        tokio::task::yield_now().await;
        for _ in 0..50 {
            if lease.assert_held().is_err() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        panic!("a guest that hung up was still reported as holding the freeze");
    }

    #[tokio::test]
    async fn a_guest_that_refuses_is_not_read_from() {
        let (_directory, path) = guest_that(&["OK 1234", "BUSY"], false).await;
        let Err(error) = frozen(&app_id(), &endpoint(&path)).await else {
            panic!("a guest that refused the freeze was read from anyway");
        };
        assert!(matches!(error, FreezeError::Refused { .. }), "{error}");
        assert!(error.message().contains("BUSY"));
    }

    #[tokio::test]
    async fn a_guest_that_hung_up_before_answering_is_not_read_from_either() {
        let (_directory, path) = guest_that(&[], true).await;
        let Err(error) = frozen(&app_id(), &endpoint(&path)).await else {
            panic!("a guest that never answered was treated as frozen");
        };
        assert!(matches!(error, FreezeError::Silent { .. }), "{error}");
        assert!(error.message().contains("never answered"), "{error}");
    }

    #[tokio::test]
    async fn a_guest_with_nothing_listening_on_the_control_port_is_not_read_from() {
        let (_directory, path) = guest_that(&["FAILED"], false).await;
        let Err(error) = frozen(&app_id(), &endpoint(&path)).await else {
            panic!("a guest with no control port was treated as frozen");
        };
        assert!(matches!(error, FreezeError::Refused { .. }), "{error}");
        assert!(error.message().contains("would not freeze"), "{error}");
    }

    #[tokio::test]
    async fn a_lease_names_the_app_whose_freeze_it_could_not_vouch_for() {
        let lost = FreezeError::Lost { app_id: app_id() };
        assert!(lost.message().contains(app_id().as_str()));
        assert!(lost
            .message()
            .contains("thawed before the checkpoint was recorded"));
    }
}
