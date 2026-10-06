use std::time::Duration;

use guest_contract::channels::ChannelEndpoint;
use guest_contract::filesystem::{
    decode_compute, decode_details, decode_header, decode_listing, decode_usage, decode_written,
    encode_request, fits_one_request, is_refusal, refusal_for, FilesystemDetails, GuestFilesystemRequest,
    MeasuredBytes, MeasuredCompute, FRAME_HEADER_BYTES, GUEST_FILESYSTEM_CHUNK_BYTES,
};
use protocol::{AppId, DirectoryListing, GuestPath};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;

const REPLY_TIMEOUT: Duration = Duration::from_secs(20);

#[derive(Debug, thiserror::Error)]
pub enum GuestFilesystemError {
    #[error("no runtime is running on this host for {app_id}, so its files cannot be reached")]
    Unreachable { app_id: AppId },
    #[error("the guest running {app_id} took a request about its files and never answered")]
    Silent { app_id: AppId },
    #[error("the guest running {app_id} would not do that with its files: {refusal}")]
    Refused { app_id: AppId, refusal: &'static str },
    #[error("more was asked of the guest running {app_id} at once than one request carries")]
    TooLarge { app_id: AppId },
    #[error("the guest answered about its files with bytes this host cannot read: {reason}")]
    Malformed { reason: String },
}

impl GuestFilesystemError {
    pub fn message(&self) -> String {
        self.to_string()
    }
}

pub struct GuestFilesystem {
    app_id: AppId,
    wire: BufReader<UnixStream>,
}

impl GuestFilesystem {
    pub async fn dial(app_id: &AppId, endpoint: &ChannelEndpoint) -> Result<Self, GuestFilesystemError> {
        let unreachable = || GuestFilesystemError::Unreachable {
            app_id: app_id.clone(),
        };
        let stream = UnixStream::connect(&endpoint.path)
            .await
            .map_err(|_| unreachable())?;
        let mut client = Self {
            app_id: app_id.clone(),
            wire: BufReader::new(stream),
        };
        if let Some(port) = endpoint.vsock_port {
            client
                .send(guest_contract::vsock::connect_request(port).as_bytes())
                .await?;
            let reply = client.receive_line().await?;
            guest_contract::vsock::read_connect_reply(&reply, port).map_err(|_| unreachable())?;
        }
        Ok(client)
    }

    pub async fn list(&mut self, path: &GuestPath) -> Result<DirectoryListing, GuestFilesystemError> {
        let body = self
            .exchange(&GuestFilesystemRequest::List { path: path.clone() })
            .await?;
        decode_listing(&body, path).map_err(|error| self.malformed(&error))
    }

    pub async fn stat(&mut self, path: &GuestPath) -> Result<FilesystemDetails, GuestFilesystemError> {
        let body = self
            .exchange(&GuestFilesystemRequest::Stat { path: path.clone() })
            .await?;
        decode_details(&body).map_err(|error| self.malformed(&error))
    }

    pub async fn usage(&mut self) -> Result<MeasuredBytes, GuestFilesystemError> {
        let body = self.exchange(&GuestFilesystemRequest::Usage).await?;
        decode_usage(&body).map_err(|error| self.malformed(&error))
    }

    pub async fn compute(&mut self) -> Result<MeasuredCompute, GuestFilesystemError> {
        let body = self.exchange(&GuestFilesystemRequest::Compute).await?;
        decode_compute(&body).map_err(|error| self.malformed(&error))
    }

    pub async fn read(
        &mut self,
        path: &GuestPath,
        offset: u64,
        length: u32,
    ) -> Result<Vec<u8>, GuestFilesystemError> {
        self.exchange(&GuestFilesystemRequest::Read {
            path: path.clone(),
            offset,
            length: length.min(GUEST_FILESYSTEM_CHUNK_BYTES as u32),
        })
        .await
    }

    pub async fn write(
        &mut self,
        path: &GuestPath,
        offset: u64,
        content: Vec<u8>,
        truncate: bool,
    ) -> Result<u32, GuestFilesystemError> {
        let body = self
            .exchange(&GuestFilesystemRequest::Write {
                path: path.clone(),
                offset,
                content,
                truncate,
            })
            .await?;
        decode_written(&body).map_err(|error| self.malformed(&error))
    }

    pub async fn make_directory(&mut self, path: &GuestPath) -> Result<(), GuestFilesystemError> {
        self.exchange(&GuestFilesystemRequest::MakeDirectory { path: path.clone() })
            .await
            .map(|_| ())
    }

    pub async fn remove(&mut self, path: &GuestPath) -> Result<(), GuestFilesystemError> {
        self.exchange(&GuestFilesystemRequest::Remove { path: path.clone() })
            .await
            .map(|_| ())
    }

    pub async fn move_entry(
        &mut self,
        path: &GuestPath,
        destination: &GuestPath,
    ) -> Result<(), GuestFilesystemError> {
        self.exchange(&GuestFilesystemRequest::Move {
            path: path.clone(),
            destination: destination.clone(),
        })
        .await
        .map(|_| ())
    }

    async fn exchange(&mut self, request: &GuestFilesystemRequest) -> Result<Vec<u8>, GuestFilesystemError> {
        if !fits_one_request(request) {
            return Err(GuestFilesystemError::TooLarge {
                app_id: self.app_id.clone(),
            });
        }
        self.send(&encode_request(request)).await?;
        let header = self.receive(FRAME_HEADER_BYTES).await?;
        let header = decode_header(&header).map_err(|error| self.malformed(&error))?;
        let body = if header.body_length == 0 {
            Vec::new()
        } else {
            self.receive(header.body_length).await?
        };
        if is_refusal(header.status) {
            return Err(GuestFilesystemError::Refused {
                app_id: self.app_id.clone(),
                refusal: refusal_for(header.status),
            });
        }
        Ok(body)
    }

    fn malformed(&self, error: &guest_contract::filesystem::MalformedGuestReply) -> GuestFilesystemError {
        GuestFilesystemError::Malformed {
            reason: error.to_string(),
        }
    }

    fn silent(&self) -> GuestFilesystemError {
        GuestFilesystemError::Silent {
            app_id: self.app_id.clone(),
        }
    }

    async fn send(&mut self, bytes: &[u8]) -> Result<(), GuestFilesystemError> {
        self.wire
            .get_mut()
            .write_all(bytes)
            .await
            .map_err(|_| self.silent())
    }

    async fn receive(&mut self, count: usize) -> Result<Vec<u8>, GuestFilesystemError> {
        let mut buffer = vec![0u8; count];
        match tokio::time::timeout(REPLY_TIMEOUT, self.wire.read_exact(&mut buffer)).await {
            Ok(Ok(_)) => Ok(buffer),
            _ => Err(self.silent()),
        }
    }

    async fn receive_line(&mut self) -> Result<String, GuestFilesystemError> {
        let mut line = String::new();
        match tokio::time::timeout(REPLY_TIMEOUT, self.wire.read_line(&mut line)).await {
            Ok(Ok(read)) if read > 0 => Ok(line.trim_end().to_string()),
            _ => Err(self.silent()),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    fn endpoint(path: &Path) -> guest_contract::channels::ChannelEndpoint {
        guest_contract::channels::ChannelEndpoint {
            path: path.into(),
            vsock_port: Some(guest_contract::vsock::GUEST_FILESYSTEM_VSOCK_PORT),
        }
    }

    use super::*;
    use crate::test_support::app_id;
    use guest_contract::filesystem::FRAME_MAGIC;
    use protocol::FilesystemEntryKind;
    use tokio::net::UnixListener;

    type Asked = std::sync::Arc<std::sync::Mutex<Vec<Vec<u8>>>>;

    struct Guest {
        connect_reply: &'static str,
        replies: Vec<(u8, Vec<u8>)>,
        hangs_up: bool,
    }

    impl Default for Guest {
        fn default() -> Self {
            Self {
                connect_reply: "OK 1234",
                replies: Vec::new(),
                hangs_up: false,
            }
        }
    }

    impl Guest {
        async fn start(self) -> (tempfile::TempDir, std::path::PathBuf, Asked) {
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("guest.vsock");
            let listener = UnixListener::bind(&path).unwrap();
            let asked: Asked = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
            let recorded = asked.clone();
            let Guest {
                connect_reply,
                replies,
                hangs_up,
            } = self;
            tokio::spawn(async move {
                let Ok((stream, _)) = listener.accept().await else {
                    return;
                };
                let mut wire = BufReader::new(stream);
                let mut connect = String::new();
                let _ = wire.read_line(&mut connect).await;
                let _ = wire
                    .get_mut()
                    .write_all(format!("{connect_reply}\n").as_bytes())
                    .await;
                for (status, body) in replies {
                    let mut request = [0u8; FRAME_HEADER_BYTES];
                    if wire.read_exact(&mut request).await.is_err() {
                        return;
                    }
                    let length =
                        u32::from_be_bytes([request[5], request[6], request[7], request[8]]) as usize;
                    let mut rest = vec![0u8; length];
                    if wire.read_exact(&mut rest).await.is_err() {
                        return;
                    }
                    let mut whole = request.to_vec();
                    whole.extend_from_slice(&rest);
                    recorded.lock().unwrap().push(whole);

                    let mut reply = Vec::new();
                    reply.extend_from_slice(FRAME_MAGIC);
                    reply.push(status);
                    reply.extend_from_slice(&(body.len() as u32).to_be_bytes());
                    reply.extend_from_slice(&body);
                    let _ = wire.get_mut().write_all(&reply).await;
                }
                if hangs_up {
                    return;
                }
                std::future::pending::<()>().await;
            });
            (directory, path, asked)
        }
    }

    async fn guest_answering(replies: Vec<(u8, Vec<u8>)>) -> (tempfile::TempDir, std::path::PathBuf) {
        let (directory, path, _) = Guest {
            replies,
            ..Default::default()
        }
        .start()
        .await;
        (directory, path)
    }

    fn usage_body(total: u64, used: u64) -> Vec<u8> {
        let mut body = Vec::new();
        body.extend_from_slice(&total.to_be_bytes());
        body.extend_from_slice(&used.to_be_bytes());
        body
    }

    fn details_body(kind: u8, size: u64, modified_seconds: u64) -> Vec<u8> {
        let mut body = vec![kind];
        body.extend_from_slice(&size.to_be_bytes());
        body.extend_from_slice(&modified_seconds.to_be_bytes());
        body
    }

    fn compute_body(readings: [u64; 4]) -> Vec<u8> {
        readings.iter().flat_map(|value| value.to_be_bytes()).collect()
    }

    fn listing_body(entries: &[(&str, u8, u64, i64)], truncated: bool) -> Vec<u8> {
        let mut body = vec![u8::from(truncated)];
        for (name, kind, size, modified) in entries {
            body.push(*kind);
            body.extend_from_slice(&size.to_be_bytes());
            body.extend_from_slice(&modified.to_be_bytes());
            body.push(name.len() as u8);
            body.extend_from_slice(name.as_bytes());
        }
        body
    }

    #[tokio::test]
    async fn a_unix_runtime_uses_the_same_framed_filesystem_protocol_without_a_vsock_handshake() {
        let directory = tempfile::tempdir().unwrap();
        let endpoint = guest_contract::channels::ChannelTransport::Unix
            .endpoint(directory.path(), guest_contract::channels::Channel::Filesystem);
        let listener = tokio::net::UnixListener::bind(&endpoint.path).unwrap();
        let server = tokio::spawn(async move {
            let (mut wire, _) = listener.accept().await.unwrap();
            let expected = encode_request(&GuestFilesystemRequest::Usage);
            let mut request = vec![0; expected.len()];
            wire.read_exact(&mut request).await.unwrap();
            assert_eq!(request, expected);
            wire.write_all(&guest_contract::filesystem::encode_usage(&MeasuredBytes {
                total_bytes: 4096,
                used_bytes: 1024,
            }))
            .await
            .unwrap();
        });
        let mut client = GuestFilesystem::dial(&app_id(), &endpoint).await.unwrap();
        let usage = client.usage().await.unwrap();
        assert_eq!(
            usage,
            MeasuredBytes {
                total_bytes: 4096,
                used_bytes: 1024
            }
        );
        server.await.unwrap();
    }

    #[tokio::test]
    async fn a_guest_that_is_not_running_cannot_be_browsed() {
        let directory = tempfile::tempdir().unwrap();
        let Err(error) =
            GuestFilesystem::dial(&app_id(), &endpoint(&directory.path().join("nothing.vsock"))).await
        else {
            panic!("a guest that is not there is not browsable");
        };
        assert!(
            matches!(error, GuestFilesystemError::Unreachable { .. }),
            "{error}"
        );
    }

    #[tokio::test]
    async fn a_name_a_tokeniser_would_have_choked_on_comes_back_whole() {
        let awkward = "it's a \"file\"\nreally";
        let body = listing_body(&[(awkward, 1, 42, 1_760_000_000)], false);
        let (_directory, path) = guest_answering(vec![(0, body)]).await;

        let mut client = GuestFilesystem::dial(&app_id(), &endpoint(&path)).await.unwrap();
        let listing = client.list(&GuestPath::parse("/").unwrap()).await.unwrap();
        assert_eq!(listing.entries.len(), 1);
        assert_eq!(listing.entries[0].name, awkward);
        assert_eq!(listing.entries[0].kind, FilesystemEntryKind::File);
        assert_eq!(listing.entries[0].size_bytes, 42);
        assert!(!listing.truncated);
    }

    #[tokio::test]
    async fn what_mke2fs_left_at_the_root_is_not_a_tenants_to_see() {
        let entries = [("lost+found", 2u8, 0u64, 0i64), ("notes.txt", 1, 10, 0)];
        let (_directory, path) = guest_answering(vec![
            (0, listing_body(&entries, false)),
            (0, listing_body(&entries, false)),
        ])
        .await;

        let mut client = GuestFilesystem::dial(&app_id(), &endpoint(&path)).await.unwrap();
        let root = client.list(&GuestPath::parse("/").unwrap()).await.unwrap();
        assert_eq!(
            root.entries
                .iter()
                .map(|entry| entry.name.as_str())
                .collect::<Vec<_>>(),
            vec!["notes.txt"]
        );
        let deeper = client.list(&GuestPath::parse("/data").unwrap()).await.unwrap();
        assert_eq!(
            deeper.entries.len(),
            2,
            "a tenant's own directory of that name is theirs"
        );
    }

    #[tokio::test]
    async fn a_refusal_reads_as_a_sentence_and_never_names_the_path() {
        let (_directory, path) = guest_answering(vec![(1, Vec::new())]).await;
        let mut client = GuestFilesystem::dial(&app_id(), &endpoint(&path)).await.unwrap();
        let Err(error) = client.list(&GuestPath::parse("/secrets").unwrap()).await else {
            panic!("a refusal is not a listing");
        };
        assert!(
            error.message().contains("there is nothing at that path"),
            "{error}"
        );
        assert!(!error.message().contains("/secrets"), "{error}");
    }

    #[tokio::test]
    async fn more_than_one_frame_carries_is_refused_before_the_connection_pays_for_it() {
        let (_directory, path) = guest_answering(vec![(0, usage_body(1_000, 400))]).await;
        let mut client = GuestFilesystem::dial(&app_id(), &endpoint(&path)).await.unwrap();
        let Err(error) = client
            .write(
                &GuestPath::parse("/big").unwrap(),
                0,
                vec![0u8; guest_contract::filesystem::BODY_MAX_BYTES + 1],
                false,
            )
            .await
        else {
            panic!("a body past the ceiling is not sent");
        };
        assert!(matches!(error, GuestFilesystemError::TooLarge { .. }), "{error}");

        assert!(client.usage().await.is_ok());
    }

    #[tokio::test]
    async fn one_connection_serves_as_many_requests_as_the_caller_makes() {
        let (_directory, path) = guest_answering(vec![
            (0, listing_body(&[("a", 1, 1, 0)], false)),
            (0, listing_body(&[("b", 1, 2, 0)], false)),
            (0, usage_body(1_000, 400)),
        ])
        .await;

        let mut client = GuestFilesystem::dial(&app_id(), &endpoint(&path)).await.unwrap();
        assert_eq!(
            client
                .list(&GuestPath::parse("/one").unwrap())
                .await
                .unwrap()
                .entries
                .len(),
            1
        );
        assert_eq!(
            client
                .list(&GuestPath::parse("/two").unwrap())
                .await
                .unwrap()
                .entries
                .len(),
            1
        );
        let measured = client.usage().await.unwrap();
        assert_eq!(measured.total_bytes, 1_000);
        assert_eq!(measured.used_bytes, 400);
    }

    #[tokio::test]
    async fn a_guest_with_nothing_listening_on_the_filesystem_port_cannot_be_browsed_either() {
        let (_directory, path, _) = Guest {
            connect_reply: "FAILED",
            ..Default::default()
        }
        .start()
        .await;
        let Err(error) = GuestFilesystem::dial(&app_id(), &endpoint(&path)).await else {
            panic!("a guest that refused the handshake is not browsable");
        };
        assert!(
            matches!(error, GuestFilesystemError::Unreachable { .. }),
            "{error}"
        );
    }

    #[tokio::test]
    async fn a_guest_that_hung_up_mid_request_is_silent_rather_than_answering_with_nothing() {
        let (_directory, path, _) = Guest {
            hangs_up: true,
            ..Default::default()
        }
        .start()
        .await;
        let mut client = GuestFilesystem::dial(&app_id(), &endpoint(&path)).await.unwrap();
        let Err(error) = client.usage().await else {
            panic!("a guest that hung up did not measure anything");
        };
        assert!(matches!(error, GuestFilesystemError::Silent { .. }), "{error}");
        assert!(error.message().contains("never answered"), "{error}");
    }

    #[tokio::test]
    async fn a_reading_the_guest_cut_short_is_not_read_as_a_smaller_one() {
        let (_directory, path) = guest_answering(vec![
            (0, vec![0u8; 8]),
            (0, compute_body([1, 2, 3, 4])[..24].to_vec()),
            (0, Vec::new()),
        ])
        .await;
        let mut client = GuestFilesystem::dial(&app_id(), &endpoint(&path)).await.unwrap();

        for outcome in [
            client.usage().await.err(),
            client.compute().await.err(),
            client.stat(&GuestPath::parse("/notes.txt").unwrap()).await.err(),
        ] {
            let error = outcome.expect("a reply the host cannot read is not a reading");
            assert!(matches!(error, GuestFilesystemError::Malformed { .. }), "{error}");
        }
    }

    #[tokio::test]
    async fn what_a_guest_says_about_one_entry_comes_back_as_the_kind_size_and_time_it_named() {
        let (_directory, path) = guest_answering(vec![
            (0, details_body(2, 0, 1_760_000_000)),
            (0, details_body(1, 4_096, 1_760_000_000)),
        ])
        .await;
        let mut client = GuestFilesystem::dial(&app_id(), &endpoint(&path)).await.unwrap();

        let directory = client.stat(&GuestPath::parse("/data").unwrap()).await.unwrap();
        assert_eq!(directory.kind, FilesystemEntryKind::Directory);
        assert_eq!(directory.size_bytes, 0);

        let file = client
            .stat(&GuestPath::parse("/data/notes.txt").unwrap())
            .await
            .unwrap();
        assert_eq!(file.kind, FilesystemEntryKind::File);
        assert_eq!(file.size_bytes, 4_096);
        assert_eq!(file.modified_at.epoch_ms(), 1_760_000_000_000);
    }

    #[tokio::test]
    async fn what_a_guest_says_about_its_own_load_comes_back_whole() {
        let readings = [1_031_012_352u64, 412_401_664, 900_000, 162_000];
        let (_directory, path) = guest_answering(vec![(0, compute_body(readings))]).await;
        let mut client = GuestFilesystem::dial(&app_id(), &endpoint(&path)).await.unwrap();
        let measured = client.compute().await.unwrap();
        assert_eq!(measured.memory_total_bytes, readings[0]);
        assert_eq!(measured.memory_used_bytes, readings[1]);
        assert_eq!(measured.cpu_total_ticks, readings[2]);
        assert_eq!(measured.cpu_busy_ticks, readings[3]);
    }

    #[tokio::test]
    async fn a_read_larger_than_one_chunk_is_trimmed_before_the_guest_is_asked_for_it() {
        let (_directory, path, asked) = Guest {
            replies: vec![(0, vec![7u8; 4])],
            ..Default::default()
        }
        .start()
        .await;
        let mut client = GuestFilesystem::dial(&app_id(), &endpoint(&path)).await.unwrap();
        let wanted = GuestPath::parse("/big").unwrap();
        assert_eq!(client.read(&wanted, 12, u32::MAX).await.unwrap(), vec![7u8; 4]);

        assert_eq!(
            asked.lock().unwrap()[0],
            encode_request(&GuestFilesystemRequest::Read {
                path: wanted,
                offset: 12,
                length: GUEST_FILESYSTEM_CHUNK_BYTES as u32,
            })
        );
    }

    #[tokio::test]
    async fn a_write_is_carried_out_as_the_caller_spelled_it_and_answers_with_what_landed() {
        let (_directory, path, asked) = Guest {
            replies: vec![(0, 11u32.to_be_bytes().to_vec())],
            ..Default::default()
        }
        .start()
        .await;
        let mut client = GuestFilesystem::dial(&app_id(), &endpoint(&path)).await.unwrap();
        let wanted = GuestPath::parse("/notes.txt").unwrap();
        let written = client
            .write(&wanted, 3, b"tenant data".to_vec(), true)
            .await
            .unwrap();
        assert_eq!(written, 11);

        assert_eq!(
            asked.lock().unwrap()[0],
            encode_request(&GuestFilesystemRequest::Write {
                path: wanted,
                offset: 3,
                content: b"tenant data".to_vec(),
                truncate: true,
            })
        );
    }

    #[tokio::test]
    async fn the_changes_a_caller_asks_for_are_spelled_out_to_the_guest_one_frame_each() {
        let (_directory, path, asked) = Guest {
            replies: vec![(0, Vec::new()), (0, Vec::new()), (0, Vec::new())],
            ..Default::default()
        }
        .start()
        .await;
        let mut client = GuestFilesystem::dial(&app_id(), &endpoint(&path)).await.unwrap();
        let made = GuestPath::parse("/data/new").unwrap();
        let gone = GuestPath::parse("/data/old").unwrap();
        let moved = GuestPath::parse("/data/renamed").unwrap();

        client.make_directory(&made).await.unwrap();
        client.remove(&gone).await.unwrap();
        client.move_entry(&gone, &moved).await.unwrap();

        let seen = asked.lock().unwrap().clone();
        assert_eq!(
            seen,
            vec![
                encode_request(&GuestFilesystemRequest::MakeDirectory { path: made }),
                encode_request(&GuestFilesystemRequest::Remove { path: gone.clone() }),
                encode_request(&GuestFilesystemRequest::Move {
                    path: gone,
                    destination: moved
                }),
            ]
        );
    }

    #[tokio::test]
    async fn a_change_the_guest_would_not_make_is_a_refusal_and_not_a_change_that_happened() {
        let (_directory, path) = guest_answering(vec![
            (3, Vec::new()),
            (4, Vec::new()),
            (5, Vec::new()),
            (200, Vec::new()),
        ])
        .await;
        let mut client = GuestFilesystem::dial(&app_id(), &endpoint(&path)).await.unwrap();
        let somewhere = GuestPath::parse("/data/thing").unwrap();

        for (attempt, expected) in [
            (
                client.make_directory(&somewhere).await,
                "something is there already",
            ),
            (
                client.remove(&somewhere).await,
                "the directory still holds something",
            ),
            (
                client.move_entry(&somewhere, &somewhere).await,
                "that path leads out of the volume",
            ),
        ] {
            let Err(error) = attempt else {
                panic!("a change the guest refused was reported as made");
            };
            assert!(error.message().contains(expected), "{error}");
            assert!(!error.message().contains("/data/thing"), "{error}");
        }

        let Err(error) = client.remove(&somewhere).await else {
            panic!("a refusal this host does not recognise is still a refusal");
        };
        assert!(
            error
                .message()
                .contains("it gave no reason this host understands"),
            "{error}"
        );
    }
}
