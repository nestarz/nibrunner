use std::future::Future;
use std::path::Path;
use std::time::Duration;

use guest_contract::cron_execution::{self as wire, ExecutionFrame, ExecutionRequest};
use protocol::TenantLogStream;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;

use crate::domain::cron::execution::CronRunOutput;
use crate::domain::cron::scheduler::ScheduledRun;
use crate::ports::LogSink;

const RESPONSE_TIMEOUT: Duration = Duration::from_secs(5);
const CONNECT_REPLY_MAX_BYTES: u64 = 64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CronExitStatus {
    pub code: u32,
    pub signal: u32,
}

#[derive(Debug, thiserror::Error)]
pub enum CronExecutionError {
    #[error("the guest connection closed before cron execution completed")]
    Disconnected,
    #[error("the guest sent a malformed cron execution reply")]
    Malformed,
    #[error("the guest refused the cron execution: {0:?}")]
    Rejected(wire::Rejection),
    #[error("the cron command does not fit the guest execution protocol")]
    TooLarge,
    #[error("the cron run was cancelled because its deployment is no longer running")]
    Cancelled,
}

pub struct GuestCronExecution;

impl GuestCronExecution {
    pub async fn run(
        path: &Path,
        run: ScheduledRun,
        sink: &dyn LogSink,
        cancellation: impl Future<Output = ()> + Send,
    ) -> Result<CronExitStatus, CronExecutionError> {
        let request = ExecutionRequest {
            command: run.job.command.expose().to_owned(),
            environment: run.job.environment.as_ref().map_or_else(Vec::new, |environment| {
                environment
                    .iter()
                    .map(|(name, value)| format!("{name}={}", value.expose()))
                    .collect()
            }),
        };
        let request = wire::encode_request(&request).map_err(|_| CronExecutionError::TooLarge)?;
        let mut output = CronRunOutput::new(run);
        let outcome = tokio::select! {
            result = Self::execute(path, &request, &mut output, sink) => result,
            () = cancellation => Err(CronExecutionError::Cancelled),
        };
        sink.publish(output.close()).await;
        outcome
    }

    async fn execute(
        path: &Path,
        request: &[u8],
        output: &mut CronRunOutput,
        sink: &dyn LogSink,
    ) -> Result<CronExitStatus, CronExecutionError> {
        let connect = async {
            let stream = crate::unix_socket::connect(path)
                .await
                .map_err(|_| CronExecutionError::Disconnected)?;
            let mut stream = BufReader::new(stream);
            let port = guest_contract::vsock::CRON_EXECUTION_PORT;
            Self::send(
                &mut stream,
                guest_contract::vsock::connect_request(port).as_bytes(),
            )
            .await?;
            Self::connected(&mut stream).await?;
            Self::quiescent(&mut stream)?;
            Self::send(&mut stream, request).await?;
            let reply = Self::receive(&mut stream).await?;
            Ok::<_, CronExecutionError>((stream, reply))
        };
        let (mut stream, reply) = tokio::time::timeout(RESPONSE_TIMEOUT, connect)
            .await
            .map_err(|_| CronExecutionError::Disconnected)??;
        match reply {
            ExecutionFrame::Started => {}
            ExecutionFrame::Rejected(reason) => return Err(CronExecutionError::Rejected(reason)),
            _ => return Err(CronExecutionError::Malformed),
        }
        Self::ack(&mut stream).await?;
        loop {
            let reply = Self::receive(&mut stream).await?;
            let (kind, bytes) = match reply {
                ExecutionFrame::Stdout(bytes) => (TenantLogStream::Stdout, bytes),
                ExecutionFrame::Stderr(bytes) => (TenantLogStream::Stderr, bytes),
                ExecutionFrame::Exit { code, signal } => return Ok(CronExitStatus { code, signal }),
                _ => return Err(CronExecutionError::Malformed),
            };
            let events = output.receive(kind, &bytes);
            if !events.is_empty() {
                sink.publish(events).await;
            }
            Self::ack(&mut stream).await?;
        }
    }

    fn quiescent(stream: &mut BufReader<UnixStream>) -> Result<(), CronExecutionError> {
        if !stream.buffer().is_empty() {
            return Err(CronExecutionError::Malformed);
        }
        let mut extra = [0u8; 1];
        match stream.get_mut().try_read(&mut extra) {
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => Ok(()),
            Ok(0) => Err(CronExecutionError::Disconnected),
            Ok(_) => Err(CronExecutionError::Malformed),
            Err(_) => Err(CronExecutionError::Disconnected),
        }
    }

    async fn connected(stream: &mut BufReader<UnixStream>) -> Result<(), CronExecutionError> {
        let mut line = Vec::new();
        (&mut *stream)
            .take(CONNECT_REPLY_MAX_BYTES)
            .read_until(b'\n', &mut line)
            .await
            .map_err(|_| CronExecutionError::Disconnected)?;
        let number = line
            .strip_prefix(b"OK ")
            .and_then(|line| line.strip_suffix(b"\n"))
            .filter(|digits| {
                !digits.is_empty() && digits.len() <= 10 && digits.iter().all(u8::is_ascii_digit)
            });
        let valid = number
            .and_then(|number| std::str::from_utf8(number).ok())
            .and_then(|number| number.parse::<u32>().ok())
            .is_some_and(|number| number > 0);
        if valid {
            Ok(())
        } else {
            Err(CronExecutionError::Malformed)
        }
    }

    async fn receive(stream: &mut BufReader<UnixStream>) -> Result<ExecutionFrame, CronExecutionError> {
        let mut header = [0u8; wire::HEADER_BYTES];
        stream
            .read_exact(&mut header[..1])
            .await
            .map_err(|_| CronExecutionError::Disconnected)?;
        let frame = async {
            stream
                .read_exact(&mut header[1..])
                .await
                .map_err(|_| CronExecutionError::Disconnected)?;
            let header = wire::decode_reply_header(&header).map_err(|_| CronExecutionError::Malformed)?;
            let mut body = vec![0u8; header.body_length];
            stream
                .read_exact(&mut body)
                .await
                .map_err(|_| CronExecutionError::Disconnected)?;
            if !stream.buffer().is_empty() {
                return Err(CronExecutionError::Malformed);
            }
            wire::decode_reply(header, &body).map_err(|_| CronExecutionError::Malformed)
        };
        tokio::time::timeout(RESPONSE_TIMEOUT, frame)
            .await
            .map_err(|_| CronExecutionError::Disconnected)?
    }

    async fn ack(stream: &mut BufReader<UnixStream>) -> Result<(), CronExecutionError> {
        Self::quiescent(stream)?;
        Self::send(stream, &[wire::ACK]).await
    }

    async fn send(stream: &mut BufReader<UnixStream>, bytes: &[u8]) -> Result<(), CronExecutionError> {
        match tokio::time::timeout(RESPONSE_TIMEOUT, stream.get_mut().write_all(bytes)).await {
            Ok(Ok(())) => Ok(()),
            _ => Err(CronExecutionError::Disconnected),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};
    use tokio::io::AsyncBufReadExt;
    use tokio::net::UnixListener;

    use super::*;
    use crate::ports::{TenantLogBody, TenantLogEvent};
    use crate::test_support::cron_run;

    #[derive(Default)]
    struct Output(Mutex<Vec<TenantLogEvent>>);
    #[async_trait::async_trait]
    impl LogSink for Output {
        async fn publish(&self, events: Vec<TenantLogEvent>) {
            self.0.lock().unwrap().extend(events);
        }
    }

    fn run() -> ScheduledRun {
        cron_run("echo secret")
    }

    async fn guest(
        replies: Vec<ExecutionFrame>,
    ) -> (
        tempfile::TempDir,
        std::path::PathBuf,
        tokio::task::JoinHandle<Vec<u8>>,
    ) {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("guest.vsock");
        let listener = UnixListener::bind(&path).unwrap();
        let task = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut stream = BufReader::new(stream);
            let mut connect = String::new();
            stream.read_line(&mut connect).await.unwrap();
            assert_eq!(connect, "CONNECT 51004\n");
            stream.get_mut().write_all(b"OK 1234\n").await.unwrap();
            let mut header = [0u8; wire::HEADER_BYTES];
            stream.read_exact(&mut header).await.unwrap();
            let header = wire::decode_request_header(&header).unwrap();
            let mut body = vec![0; header.body_length];
            stream.read_exact(&mut body).await.unwrap();
            assert_eq!(
                wire::decode_request(header, &body).unwrap().command,
                "echo secret"
            );
            let mut acknowledgements = Vec::new();
            for reply in replies {
                stream
                    .get_mut()
                    .write_all(&wire::encode_reply(&reply).unwrap())
                    .await
                    .unwrap();
                if matches!(
                    reply,
                    ExecutionFrame::Started | ExecutionFrame::Stdout(_) | ExecutionFrame::Stderr(_)
                ) {
                    acknowledgements.push(stream.read_u8().await.unwrap());
                }
            }
            acknowledgements
        });
        (directory, path, task)
    }

    #[tokio::test]
    async fn output_is_published_before_each_ack_and_exit_is_reported() {
        let (_directory, path, guest) = guest(vec![
            ExecutionFrame::Started,
            ExecutionFrame::Stdout(b"hello\n".to_vec()),
            ExecutionFrame::Stderr(b"last words".to_vec()),
            ExecutionFrame::Exit { code: 7, signal: 0 },
        ])
        .await;
        let sink = Output::default();
        let status = GuestCronExecution::run(&path, run(), &sink, std::future::pending())
            .await
            .unwrap();
        assert_eq!(status, CronExitStatus { code: 7, signal: 0 });
        assert_eq!(guest.await.unwrap(), [wire::ACK; 3]);
        let events = sink.0.lock().unwrap();
        assert_eq!(events.len(), 2);
        assert!(matches!(&events[0].body, TenantLogBody::CronData { text, .. } if text == "hello"));
        assert!(matches!(&events[1].body, TenantLogBody::CronData { text, .. } if text == "last words"));
        assert!(events.iter().all(|event| event.source_id == events[0].source_id));
    }

    #[tokio::test]
    async fn dropping_a_cancelled_execution_closes_the_guest_connection() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("guest.vsock");
        let listener = UnixListener::bind(&path).unwrap();
        let (ready, cancel) = tokio::sync::oneshot::channel();
        let guest = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut stream = BufReader::new(stream);
            let mut connect = String::new();
            stream.read_line(&mut connect).await.unwrap();
            stream.get_mut().write_all(b"OK 1234\n").await.unwrap();
            let mut header = [0; wire::HEADER_BYTES];
            stream.read_exact(&mut header).await.unwrap();
            let header = wire::decode_request_header(&header).unwrap();
            let mut body = vec![0; header.body_length];
            stream.read_exact(&mut body).await.unwrap();
            stream
                .get_mut()
                .write_all(&wire::encode_reply(&ExecutionFrame::Started).unwrap())
                .await
                .unwrap();
            assert_eq!(stream.read_u8().await.unwrap(), wire::ACK);
            ready.send(()).unwrap();
            assert!(stream.read_u8().await.is_err());
        });
        let sink = Arc::new(Output::default());
        let outcome = GuestCronExecution::run(&path, run(), sink.as_ref(), async {
            let _ = cancel.await;
        })
        .await;
        assert!(matches!(outcome, Err(CronExecutionError::Cancelled)));
        guest.await.unwrap();
    }

    #[tokio::test]
    async fn a_guest_that_exits_without_started_is_refused() {
        let (_directory, path, guest) = guest(vec![ExecutionFrame::Exit { code: 0, signal: 0 }]).await;
        assert!(matches!(
            GuestCronExecution::run(&path, run(), &Output::default(), std::future::pending()).await,
            Err(CronExecutionError::Malformed)
        ));
        guest.await.unwrap();
    }

    #[tokio::test]
    async fn a_connect_reply_requires_an_unsigned_nonzero_port_and_exact_terminator() {
        for reply in [
            b"OK 1\n".as_slice(),
            b"OK 0\n",
            b"OK -1\n",
            b"OK +1\n",
            b"OK 1 \n",
            b"OK 1\r\n",
            b"OK 4294967296\n",
        ] {
            let (client, mut server) = UnixStream::pair().unwrap();
            server.write_all(reply).await.unwrap();
            let result = GuestCronExecution::connected(&mut BufReader::new(client)).await;
            assert_eq!(result.is_ok(), reply == b"OK 1\n", "{reply:?}");
        }
    }

    #[tokio::test]
    async fn output_sent_before_acknowledgement_is_refused_even_outside_the_read_buffer() {
        let (client, mut server) = UnixStream::pair().unwrap();
        let mut stream = BufReader::new(client);
        assert!(GuestCronExecution::quiescent(&mut stream).is_ok());
        server.write_all(b"unsolicited output").await.unwrap();
        stream.get_ref().readable().await.unwrap();
        assert!(matches!(
            GuestCronExecution::ack(&mut stream).await,
            Err(CronExecutionError::Malformed)
        ));
        let mut ack = [0; 1];
        assert!(
            matches!(server.try_read(&mut ack), Err(error) if error.kind() == std::io::ErrorKind::WouldBlock)
        );
    }
}
