use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use protocol::{AppId, DeploymentId, TenantLogStream, Timestamp};
use tokio::io::AsyncReadExt;
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{watch, Mutex};

use crate::clock::now_timestamp;
use crate::ports::{LogSink, TenantLogBody, TenantLogEvent};
use guest_contract::logs::{decode_frames, GuestLogFrame};

const MAX_GUEST_CONNECTIONS: usize = 4;

/// How long a line the guest has not ended is held for its newline before it is handed over as
/// it stands: long enough that a frame boundary never splits a line, short enough that a prompt
/// or a last line without a newline does not wait for ever.
const OPEN_LINE_IDLE: Duration = Duration::from_millis(250);

/// A line no newline ends is cut here rather than held without bound.
const LONGEST_LINE_BYTES: usize = 64 * 1024;

struct Attachment {
    source: Arc<Mutex<(AppId, DeploymentId)>>,
    socket_path: PathBuf,
    task: tokio::task::JoinHandle<()>,
    // Dropped with the attachment, which is how every connection from the guest learns to hand
    // over the line it was holding and stop.
    _detached: watch::Sender<()>,
}

#[derive(Default)]
pub struct TenantLogReceiver {
    attachments: Mutex<BTreeMap<AppId, Attachment>>,
}

impl TenantLogReceiver {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub async fn attach(
        self: &Arc<Self>,
        app_id: AppId,
        deployment_id: DeploymentId,
        socket_path: PathBuf,
        sink: Arc<dyn LogSink>,
    ) -> std::io::Result<()> {
        self.attach_as(app_id.clone(), app_id, deployment_id, socket_path, sink)
            .await
    }

    /// Listens for the guest of the microVM run under `app_id`, and says what it writes is
    /// `source`'s: the same app, unless the microVM is a new deployment booted beside `source`.
    pub async fn attach_as(
        self: &Arc<Self>,
        app_id: AppId,
        source: AppId,
        deployment_id: DeploymentId,
        socket_path: PathBuf,
        sink: Arc<dyn LogSink>,
    ) -> std::io::Result<()> {
        let mut attachments = self.attachments.lock().await;
        if let Some(existing) = attachments.get(&app_id) {
            if existing.socket_path == socket_path {
                *existing.source.lock().await = (source, deployment_id);
                return Ok(());
            }
        }
        if let Some(previous) = attachments.remove(&app_id) {
            previous.task.abort();
            let _ = std::fs::remove_file(&previous.socket_path);
        }
        if let Some(parent) = socket_path.parent() {
            crate::json_store::make_directory(parent, 0o700)?;
        }
        let _ = std::fs::remove_file(&socket_path);
        let listener = crate::unix_socket::bind(&socket_path)?;
        let source = Arc::new(Mutex::new((source, deployment_id)));
        let (detached, detaching) = watch::channel(());
        let task = tokio::spawn(serve(listener, source.clone(), sink, detaching));
        attachments.insert(
            app_id,
            Attachment {
                source,
                socket_path,
                task,
                _detached: detached,
            },
        );
        Ok(())
    }

    pub async fn detach(&self, app_id: &AppId) {
        let mut attachments = self.attachments.lock().await;
        if let Some(attachment) = attachments.remove(app_id) {
            attachment.task.abort();
            let _ = std::fs::remove_file(&attachment.socket_path);
        }
    }

    pub async fn attached(&self) -> Vec<AppId> {
        self.attachments.lock().await.keys().cloned().collect()
    }
}

async fn serve(
    listener: UnixListener,
    source: Arc<Mutex<(AppId, DeploymentId)>>,
    sink: Arc<dyn LogSink>,
    detaching: watch::Receiver<()>,
) {
    let connections = Arc::new(tokio::sync::Semaphore::new(MAX_GUEST_CONNECTIONS));
    let source_id = uuid::Uuid::new_v4().to_string();
    let sequence = Arc::new(AtomicU64::new(0));
    loop {
        let Ok((stream, _)) = listener.accept().await else {
            return;
        };
        let Ok(permit) = connections.clone().try_acquire_owned() else {
            drop(stream);
            continue;
        };
        let source = source.clone();
        let sink = sink.clone();
        let source_id = source_id.clone();
        let sequence = sequence.clone();
        let detaching = detaching.clone();
        tokio::spawn(async move {
            let _permit = permit;
            pump(stream, source, sink, source_id, sequence, detaching).await;
        });
    }
}

async fn pump(
    mut stream: UnixStream,
    source: Arc<Mutex<(AppId, DeploymentId)>>,
    sink: Arc<dyn LogSink>,
    source_id: String,
    sequence: Arc<AtomicU64>,
    mut detaching: watch::Receiver<()>,
) {
    let mut buffered: Vec<u8> = Vec::new();
    let mut chunk = vec![0u8; 16 * 1024];
    let mut lines = OpenLines::default();
    let mut idle_armed = false;
    loop {
        let read = tokio::select! {
            // What the guest has already written is read before an idle or a detach is acted on.
            biased;
            read = stream.read(&mut chunk) => match read {
                Ok(0) | Err(_) => break,
                Ok(read) => read,
            },
            _ = tokio::time::sleep(OPEN_LINE_IDLE), if idle_armed => {
                idle_armed = false;
                let stamp = Stamp::taken(&source, &source_id, &sequence).await;
                let mut events = Vec::new();
                lines.flush(|stream, text| events.push(stamp.line(stream, text)));
                publish(&sink, events).await;
                continue;
            }
            _ = detaching.changed() => break,
        };
        let decoded = match decode_frames(&buffered, &chunk[..read]) {
            Ok(decoded) => decoded,
            Err(error) => {
                tracing::warn!(%error, "a guest sent log frames this host cannot read");
                break;
            }
        };
        buffered = decoded.1;
        let stamp = Stamp::taken(&source, &source_id, &sequence).await;
        let mut events = Vec::new();
        for frame in decoded.0 {
            match frame {
                GuestLogFrame::Gap { dropped_bytes } => {
                    // What follows a gap is not the rest of the line before it.
                    lines.close(|stream, text| events.push(stamp.line(stream, text)));
                    events.push(stamp.event(TenantLogBody::Gap { dropped_bytes }));
                }
                GuestLogFrame::Restart(restart) => {
                    // The tenant that was writing the line is gone: what follows is the next
                    // one's, and the restart sits between them.
                    lines.close(|stream, text| events.push(stamp.line(stream, text)));
                    events.push(stamp.event(TenantLogBody::Restart(restart)));
                }
                GuestLogFrame::Data { stream, bytes } => {
                    lines.extend(stream, &bytes, |stream, text| {
                        events.push(stamp.line(stream, text))
                    });
                }
            }
        }
        idle_armed = lines.holding();
        publish(&sink, events).await;
    }
    let stamp = Stamp::taken(&source, &source_id, &sequence).await;
    let mut events = Vec::new();
    lines.close(|stream, text| events.push(stamp.line(stream, text)));
    publish(&sink, events).await;
}

async fn publish(sink: &Arc<dyn LogSink>, events: Vec<TenantLogEvent>) {
    if !events.is_empty() {
        sink.publish(events).await;
    }
}

/// What every event in one batch is stamped with: the source as it is named now, and the
/// instant the batch was read, which is when each of its lines was observed.
struct Stamp<'a> {
    app_id: AppId,
    deployment_id: DeploymentId,
    source_id: &'a str,
    sequence: &'a AtomicU64,
    observed_at: Timestamp,
}

impl<'a> Stamp<'a> {
    async fn taken(
        source: &Mutex<(AppId, DeploymentId)>,
        source_id: &'a str,
        sequence: &'a AtomicU64,
    ) -> Self {
        let (app_id, deployment_id) = source.lock().await.clone();
        Self {
            app_id,
            deployment_id,
            source_id,
            sequence,
            observed_at: now_timestamp(),
        }
    }

    fn line(&self, stream: TenantLogStream, text: String) -> TenantLogEvent {
        self.event(TenantLogBody::Data { stream, text })
    }

    fn event(&self, body: TenantLogBody) -> TenantLogEvent {
        TenantLogEvent {
            app_id: self.app_id.clone(),
            deployment_id: self.deployment_id.clone(),
            source_id: self.source_id.to_string(),
            sequence: self.sequence.fetch_add(1, Ordering::SeqCst),
            observed_at: self.observed_at.clone(),
            body,
        }
    }
}

/// What each stream has written since its last newline: the line under way, kept until the
/// newline that ends it, an idle long enough to stop waiting for one, or the end of the stream.
#[derive(Default)]
struct OpenLines {
    stdout: Vec<u8>,
    stderr: Vec<u8>,
}

impl OpenLines {
    fn on(&mut self, stream: TenantLogStream) -> &mut Vec<u8> {
        match stream {
            TenantLogStream::Stdout => &mut self.stdout,
            TenantLogStream::Stderr => &mut self.stderr,
        }
    }

    fn holding(&self) -> bool {
        !self.stdout.is_empty() || !self.stderr.is_empty()
    }

    /// Takes in what the guest forwarded, handing over each line it ends without its newline and
    /// keeping whatever follows the last one.
    fn extend(
        &mut self,
        stream: TenantLogStream,
        bytes: &[u8],
        mut emit: impl FnMut(TenantLogStream, String),
    ) {
        let held = self.on(stream);
        let mut rest = bytes;
        while let Some(at) = rest.iter().position(|&byte| byte == b'\n') {
            let text = if held.is_empty() {
                lossy(&rest[..at])
            } else {
                held.extend_from_slice(&rest[..at]);
                let text = lossy(held);
                held.clear();
                text
            };
            emit(stream, text);
            rest = &rest[at + 1..];
        }
        held.extend_from_slice(rest);
        if held.len() >= LONGEST_LINE_BYTES {
            let cut = complete_prefix(held);
            emit(stream, lossy(&held[..cut]));
            held.drain(..cut);
        }
    }

    /// Hands over every line still open, less a character the guest has only half written: the
    /// idle that asks for this is no sign the guest will not finish what it started.
    fn flush(&mut self, mut emit: impl FnMut(TenantLogStream, String)) {
        for stream in [TenantLogStream::Stdout, TenantLogStream::Stderr] {
            let held = self.on(stream);
            let whole = complete_prefix(held);
            if whole > 0 {
                emit(stream, lossy(&held[..whole]));
                held.drain(..whole);
            }
        }
    }

    /// Hands over everything still open as it stands, a half-written character and all: nothing
    /// is coming to finish it.
    fn close(&mut self, mut emit: impl FnMut(TenantLogStream, String)) {
        for stream in [TenantLogStream::Stdout, TenantLogStream::Stderr] {
            let held = self.on(stream);
            if !held.is_empty() {
                emit(stream, lossy(held));
                held.clear();
            }
        }
    }
}

fn lossy(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}

/// How much of `bytes` is whole characters: everything but a multi-byte sequence cut off at the
/// end, whose remaining bytes may yet arrive.
fn complete_prefix(bytes: &[u8]) -> usize {
    let mut offset = 0;
    loop {
        match std::str::from_utf8(&bytes[offset..]) {
            Ok(_) => return bytes.len(),
            Err(error) => match error.error_len() {
                None => return offset + error.valid_up_to(),
                Some(invalid) => offset += error.valid_up_to() + invalid,
            },
        }
    }
}

pub fn tenant_log_socket_path(working_dir: &Path) -> PathBuf {
    working_dir.join(guest_contract::vsock::tenant_log_socket_name())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::mocks;
    use crate::test_support::{app_id, deployment_id, tenant_restart};
    use guest_contract::logs::{
        encode_frame, encode_restart, ENCODE_KIND_GAP, ENCODE_KIND_STDERR, ENCODE_KIND_STDOUT,
    };
    use tokio::io::AsyncWriteExt;

    async fn until(condition: impl Fn() -> bool) {
        for _ in 0..200 {
            if condition() {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        panic!("the receiver never saw what the guest wrote");
    }

    #[tokio::test]
    async fn what_a_guest_writes_arrives_as_events_in_order() {
        let directory = tempfile::tempdir().unwrap();
        let socket_path = tenant_log_socket_path(directory.path());
        let (sink, spy) = mocks::log_sink();
        let receiver = TenantLogReceiver::new();
        receiver
            .attach(app_id(), deployment_id(), socket_path.clone(), sink.clone())
            .await
            .unwrap();

        let mut guest = UnixStream::connect(&socket_path).await.unwrap();
        let frame = encode_frame(ENCODE_KIND_STDOUT, "listening\n".as_bytes());
        guest.write_all(&frame[..5]).await.unwrap();
        guest.write_all(&frame[5..]).await.unwrap();
        guest
            .write_all(&encode_frame(ENCODE_KIND_GAP, &4096u64.to_be_bytes()))
            .await
            .unwrap();
        guest.flush().await.unwrap();

        until(|| spy.events().len() == 2).await;
        let events = spy.events();
        assert_eq!(events[0].app_id, app_id());
        assert_eq!(
            events[0].body,
            TenantLogBody::Data {
                stream: TenantLogStream::Stdout,
                text: "listening".into()
            }
        );
        assert_eq!(events[1].body, TenantLogBody::Gap { dropped_bytes: 4096 });
        assert_eq!(events[0].source_id, events[1].source_id);
        assert_eq!((events[0].sequence, events[1].sequence), (0, 1));
    }

    #[tokio::test]
    async fn a_restart_closes_the_line_the_tenant_was_writing_and_takes_its_place_after_it() {
        let directory = tempfile::tempdir().unwrap();
        let socket_path = tenant_log_socket_path(directory.path());
        let (sink, spy) = mocks::log_sink();
        let receiver = TenantLogReceiver::new();
        receiver
            .attach(app_id(), deployment_id(), socket_path.clone(), sink.clone())
            .await
            .unwrap();

        let mut guest = UnixStream::connect(&socket_path).await.unwrap();
        guest
            .write_all(&encode_frame(ENCODE_KIND_STDERR, b"out of memory\nallocating 4"))
            .await
            .unwrap();
        guest
            .write_all(&encode_restart(&tenant_restart(|_| {})))
            .await
            .unwrap();
        guest
            .write_all(&encode_frame(ENCODE_KIND_STDOUT, b"listening\n"))
            .await
            .unwrap();
        guest.flush().await.unwrap();

        until(|| spy.events().len() == 4).await;
        let bodies: Vec<TenantLogBody> = spy.events().into_iter().map(|event| event.body).collect();
        assert_eq!(
            bodies,
            vec![
                TenantLogBody::Data {
                    stream: TenantLogStream::Stderr,
                    text: "out of memory".into()
                },
                TenantLogBody::Data {
                    stream: TenantLogStream::Stderr,
                    text: "allocating 4".into()
                },
                TenantLogBody::Restart(tenant_restart(|_| {})),
                TenantLogBody::Data {
                    stream: TenantLogStream::Stdout,
                    text: "listening".into()
                },
            ]
        );
        let sequences: Vec<u64> = spy.events().iter().map(|event| event.sequence).collect();
        assert_eq!(sequences, vec![0, 1, 2, 3]);
    }

    #[tokio::test]
    async fn a_character_split_across_frames_is_not_emitted_in_halves() {
        let directory = tempfile::tempdir().unwrap();
        let socket_path = tenant_log_socket_path(directory.path());
        let (sink, spy) = mocks::log_sink();
        let receiver = TenantLogReceiver::new();
        receiver
            .attach(app_id(), deployment_id(), socket_path.clone(), sink.clone())
            .await
            .unwrap();

        let mut guest = UnixStream::connect(&socket_path).await.unwrap();
        let snowman = "☃\n".as_bytes();
        guest
            .write_all(&encode_frame(ENCODE_KIND_STDOUT, &snowman[..1]))
            .await
            .unwrap();
        guest
            .write_all(&encode_frame(ENCODE_KIND_STDOUT, &snowman[1..]))
            .await
            .unwrap();
        guest.flush().await.unwrap();

        until(|| !spy.events().is_empty()).await;
        assert_eq!(
            spy.events()[0].body,
            TenantLogBody::Data {
                stream: TenantLogStream::Stdout,
                text: "☃".into()
            }
        );
    }

    #[tokio::test]
    async fn an_idle_does_not_cut_a_character_the_guest_has_only_half_written() {
        let directory = tempfile::tempdir().unwrap();
        let socket_path = tenant_log_socket_path(directory.path());
        let (sink, spy) = mocks::log_sink();
        let receiver = TenantLogReceiver::new();
        receiver
            .attach(app_id(), deployment_id(), socket_path.clone(), sink.clone())
            .await
            .unwrap();

        let mut guest = UnixStream::connect(&socket_path).await.unwrap();
        let snowman = "☃\n".as_bytes();
        guest
            .write_all(&encode_frame(ENCODE_KIND_STDOUT, &snowman[..1]))
            .await
            .unwrap();
        guest.flush().await.unwrap();
        tokio::time::sleep(OPEN_LINE_IDLE * 2).await;
        assert!(spy.events().is_empty(), "{:?}", spy.events());

        guest
            .write_all(&encode_frame(ENCODE_KIND_STDOUT, &snowman[1..]))
            .await
            .unwrap();
        guest.flush().await.unwrap();
        until(|| !spy.events().is_empty()).await;
        assert_eq!(
            spy.events()[0].body,
            TenantLogBody::Data {
                stream: TenantLogStream::Stdout,
                text: "☃".into()
            }
        );
    }

    #[tokio::test]
    async fn a_line_split_across_two_frames_arrives_once_and_whole() {
        let directory = tempfile::tempdir().unwrap();
        let socket_path = tenant_log_socket_path(directory.path());
        let (sink, spy) = mocks::log_sink();
        let receiver = TenantLogReceiver::new();
        receiver
            .attach(app_id(), deployment_id(), socket_path.clone(), sink.clone())
            .await
            .unwrap();

        let mut guest = UnixStream::connect(&socket_path).await.unwrap();
        guest
            .write_all(&encode_frame(ENCODE_KIND_STDOUT, b"line 1359 of "))
            .await
            .unwrap();
        guest
            .write_all(&encode_frame(ENCODE_KIND_STDOUT, b"100000\n"))
            .await
            .unwrap();
        guest.flush().await.unwrap();

        until(|| !spy.events().is_empty()).await;
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let events = spy.events();
        assert_eq!(events.len(), 1, "{events:?}");
        assert_eq!(
            events[0].body,
            TenantLogBody::Data {
                stream: TenantLogStream::Stdout,
                text: "line 1359 of 100000".into()
            }
        );
    }

    #[tokio::test]
    async fn a_frame_holding_many_lines_arrives_as_one_event_per_line() {
        let directory = tempfile::tempdir().unwrap();
        let socket_path = tenant_log_socket_path(directory.path());
        let (sink, spy) = mocks::log_sink();
        let receiver = TenantLogReceiver::new();
        receiver
            .attach(app_id(), deployment_id(), socket_path.clone(), sink.clone())
            .await
            .unwrap();

        let mut guest = UnixStream::connect(&socket_path).await.unwrap();
        guest
            .write_all(&encode_frame(ENCODE_KIND_STDOUT, b"one\ntwo\n\nfour\n"))
            .await
            .unwrap();
        guest.flush().await.unwrap();

        until(|| spy.events().len() == 4).await;
        let events = spy.events();
        let lines: Vec<(u64, String)> = events
            .iter()
            .map(|event| match &event.body {
                TenantLogBody::Data { text, .. } => (event.sequence, text.clone()),
                other => panic!("{other:?}"),
            })
            .collect();
        assert_eq!(
            lines,
            vec![
                (0, "one".to_string()),
                (1, "two".to_string()),
                (2, String::new()),
                (3, "four".to_string())
            ]
        );
        assert!(events
            .iter()
            .all(|event| event.observed_at == events[0].observed_at));
    }

    #[tokio::test]
    async fn a_line_under_way_on_one_stream_does_not_hold_up_the_lines_the_other_ends() {
        let directory = tempfile::tempdir().unwrap();
        let socket_path = tenant_log_socket_path(directory.path());
        let (sink, spy) = mocks::log_sink();
        let receiver = TenantLogReceiver::new();
        receiver
            .attach(app_id(), deployment_id(), socket_path.clone(), sink.clone())
            .await
            .unwrap();

        let mut guest = UnixStream::connect(&socket_path).await.unwrap();
        for (kind, bytes) in [
            (ENCODE_KIND_STDOUT, &b"part"[..]),
            (ENCODE_KIND_STDERR, &b"warning\n"[..]),
            (ENCODE_KIND_STDOUT, &b"ial\n"[..]),
        ] {
            guest.write_all(&encode_frame(kind, bytes)).await.unwrap();
        }
        guest.flush().await.unwrap();

        until(|| spy.events().len() == 2).await;
        let events = spy.events();
        assert_eq!(
            events[0].body,
            TenantLogBody::Data {
                stream: TenantLogStream::Stderr,
                text: "warning".into()
            }
        );
        assert_eq!(
            events[1].body,
            TenantLogBody::Data {
                stream: TenantLogStream::Stdout,
                text: "partial".into()
            }
        );
    }

    #[tokio::test]
    async fn a_gap_keeps_its_place_between_the_lines_around_it() {
        let directory = tempfile::tempdir().unwrap();
        let socket_path = tenant_log_socket_path(directory.path());
        let (sink, spy) = mocks::log_sink();
        let receiver = TenantLogReceiver::new();
        receiver
            .attach(app_id(), deployment_id(), socket_path.clone(), sink.clone())
            .await
            .unwrap();

        let mut guest = UnixStream::connect(&socket_path).await.unwrap();
        guest
            .write_all(&encode_frame(ENCODE_KIND_STDOUT, b"one\ntw"))
            .await
            .unwrap();
        guest
            .write_all(&encode_frame(ENCODE_KIND_GAP, &512u64.to_be_bytes()))
            .await
            .unwrap();
        guest
            .write_all(&encode_frame(ENCODE_KIND_STDOUT, b"o\n"))
            .await
            .unwrap();
        guest.flush().await.unwrap();

        until(|| spy.events().len() == 4).await;
        let bodies: Vec<TenantLogBody> = spy.events().into_iter().map(|event| event.body).collect();
        let line = |text: &str| TenantLogBody::Data {
            stream: TenantLogStream::Stdout,
            text: text.into(),
        };
        assert_eq!(
            bodies,
            vec![
                line("one"),
                line("tw"),
                TenantLogBody::Gap { dropped_bytes: 512 },
                line("o")
            ],
            "what follows a gap is not the rest of the line before it"
        );
        let sequences: Vec<u64> = spy.events().iter().map(|event| event.sequence).collect();
        assert_eq!(sequences, vec![0, 1, 2, 3]);
    }

    #[tokio::test]
    async fn a_line_the_guest_has_not_ended_is_handed_over_once_it_has_been_idle() {
        let directory = tempfile::tempdir().unwrap();
        let socket_path = tenant_log_socket_path(directory.path());
        let (sink, spy) = mocks::log_sink();
        let receiver = TenantLogReceiver::new();
        receiver
            .attach(app_id(), deployment_id(), socket_path.clone(), sink.clone())
            .await
            .unwrap();

        let mut guest = UnixStream::connect(&socket_path).await.unwrap();
        guest
            .write_all(&encode_frame(ENCODE_KIND_STDOUT, b"Enter a name: "))
            .await
            .unwrap();
        guest.flush().await.unwrap();

        until(|| !spy.events().is_empty()).await;
        assert_eq!(
            spy.events()[0].body,
            TenantLogBody::Data {
                stream: TenantLogStream::Stdout,
                text: "Enter a name: ".into()
            }
        );

        guest
            .write_all(&encode_frame(ENCODE_KIND_STDOUT, b"ada\n"))
            .await
            .unwrap();
        guest.flush().await.unwrap();
        until(|| spy.events().len() == 2).await;
        assert_eq!(
            spy.events()[1].body,
            TenantLogBody::Data {
                stream: TenantLogStream::Stdout,
                text: "ada".into()
            },
            "what comes after the idle is a line of its own"
        );
    }

    #[tokio::test]
    async fn a_line_the_guest_hangs_up_on_is_handed_over_as_it_stands() {
        let directory = tempfile::tempdir().unwrap();
        let socket_path = tenant_log_socket_path(directory.path());
        let (sink, spy) = mocks::log_sink();
        let receiver = TenantLogReceiver::new();
        receiver
            .attach(app_id(), deployment_id(), socket_path.clone(), sink.clone())
            .await
            .unwrap();

        let mut guest = UnixStream::connect(&socket_path).await.unwrap();
        let half_a_snowman = &"☃".as_bytes()[..1];
        guest
            .write_all(&encode_frame(
                ENCODE_KIND_STDOUT,
                &[b"bye ", half_a_snowman].concat(),
            ))
            .await
            .unwrap();
        guest.flush().await.unwrap();
        drop(guest);

        until(|| !spy.events().is_empty()).await;
        assert_eq!(
            spy.events()[0].body,
            TenantLogBody::Data {
                stream: TenantLogStream::Stdout,
                text: "bye \u{FFFD}".into()
            },
            "the idle would have kept the half-written character; a hang-up does not"
        );
    }

    #[tokio::test]
    async fn a_line_still_open_when_its_app_is_detached_is_not_lost() {
        let directory = tempfile::tempdir().unwrap();
        let socket_path = tenant_log_socket_path(directory.path());
        let (sink, spy) = mocks::log_sink();
        let receiver = TenantLogReceiver::new();
        receiver
            .attach(app_id(), deployment_id(), socket_path.clone(), sink.clone())
            .await
            .unwrap();

        let mut guest = UnixStream::connect(&socket_path).await.unwrap();
        guest
            .write_all(&encode_frame(ENCODE_KIND_STDOUT, b"up\ngoing down"))
            .await
            .unwrap();
        guest.flush().await.unwrap();
        until(|| spy.events().len() == 1).await;
        receiver.detach(&app_id()).await;

        until(|| spy.events().len() == 2).await;
        assert_eq!(
            spy.events()[1].body,
            TenantLogBody::Data {
                stream: TenantLogStream::Stdout,
                text: "going down".into()
            }
        );
        let mut nothing = [0u8; 1];
        assert_eq!(
            guest.read(&mut nothing).await.unwrap(),
            0,
            "a detached receiver hangs up on the guest rather than reading on"
        );
    }

    #[tokio::test]
    async fn a_line_no_newline_ever_ends_is_cut_rather_than_held_without_bound() {
        let directory = tempfile::tempdir().unwrap();
        let socket_path = tenant_log_socket_path(directory.path());
        let (sink, spy) = mocks::log_sink();
        let receiver = TenantLogReceiver::new();
        receiver
            .attach(app_id(), deployment_id(), socket_path.clone(), sink.clone())
            .await
            .unwrap();

        let mut guest = UnixStream::connect(&socket_path).await.unwrap();
        let endless = vec![b'x'; LONGEST_LINE_BYTES];
        for _ in 0..2 {
            guest
                .write_all(&encode_frame(ENCODE_KIND_STDOUT, &endless))
                .await
                .unwrap();
        }
        guest.flush().await.unwrap();

        until(|| spy.events().len() == 2).await;
        for event in spy.events() {
            match event.body {
                TenantLogBody::Data { text, .. } => assert_eq!(text.len(), LONGEST_LINE_BYTES),
                other => panic!("{other:?}"),
            }
        }
    }

    #[tokio::test]
    async fn every_line_lands_in_the_file_under_its_own_header_whichever_frames_it_straddled() {
        let directory = tempfile::tempdir().unwrap();
        let socket_path = tenant_log_socket_path(directory.path());
        let sink = Arc::new(crate::adapters::logs::FileLogSink::new(
            directory.path().join("logs"),
            crate::config::LogsConfig::default().keep_bytes_per_app,
        ));
        let receiver = TenantLogReceiver::new();
        receiver
            .attach(app_id(), deployment_id(), socket_path.clone(), sink.clone())
            .await
            .unwrap();

        let mut guest = UnixStream::connect(&socket_path).await.unwrap();
        for frame in [
            &b"line 1358 of 100000 at 1789308052995\nline 1359 of "[..],
            &b"100000 at 1789308052995\nline 1360 of 100000 at 1789308052995\n"[..],
        ] {
            guest
                .write_all(&encode_frame(ENCODE_KIND_STDOUT, frame))
                .await
                .unwrap();
        }
        guest.flush().await.unwrap();

        let path = sink.path_for(&app_id());
        until(|| {
            std::fs::read_to_string(&path)
                .map(|written| written.lines().count() == 3)
                .unwrap_or(false)
        })
        .await;
        let written = std::fs::read_to_string(&path).unwrap();
        for (line, (sequence, number)) in written.lines().zip((0..).zip(1358..)) {
            let [observed_at, stream, source, text]: [&str; 4] =
                line.splitn(4, ' ').collect::<Vec<_>>().try_into().unwrap();
            assert!(Timestamp::parse(observed_at).is_ok(), "{line}");
            assert_eq!(stream, "stdout", "{line}");
            assert!(source.ends_with(&format!("/{sequence}")), "{line}");
            assert_eq!(
                text,
                format!("line {number} of 100000 at 1789308052995"),
                "{line}"
            );
        }
    }

    #[tokio::test]
    async fn attaching_the_same_path_again_only_restamps_the_deployment() {
        let directory = tempfile::tempdir().unwrap();
        let socket_path = tenant_log_socket_path(directory.path());
        let (sink, spy) = mocks::log_sink();
        let receiver = TenantLogReceiver::new();
        receiver
            .attach(app_id(), deployment_id(), socket_path.clone(), sink.clone())
            .await
            .unwrap();
        let guest = UnixStream::connect(&socket_path).await.unwrap();
        let newer = DeploymentId::parse("dep-2").unwrap();
        receiver
            .attach(app_id(), newer.clone(), socket_path.clone(), sink.clone())
            .await
            .unwrap();
        drop(guest);
        let mut second = UnixStream::connect(&socket_path).await.unwrap();
        second
            .write_all(&encode_frame(ENCODE_KIND_STDOUT, b"after\n"))
            .await
            .unwrap();
        second.flush().await.unwrap();
        until(|| !spy.events().is_empty()).await;
        assert_eq!(spy.events()[0].deployment_id, newer);

        receiver.detach(&app_id()).await;
        assert!(receiver.attached().await.is_empty());
        assert!(UnixStream::connect(&socket_path).await.is_err());
    }

    #[tokio::test]
    async fn a_receiver_that_was_never_attached_is_detached_without_complaint() {
        let receiver = TenantLogReceiver::new();
        assert!(receiver.attached().await.is_empty());
        receiver.detach(&app_id()).await;
        assert!(receiver.attached().await.is_empty());
    }

    #[tokio::test]
    async fn a_socket_that_moved_takes_the_old_one_down_rather_than_leaving_two_listening() {
        let directory = tempfile::tempdir().unwrap();
        let (sink, _) = mocks::log_sink();
        let receiver = TenantLogReceiver::new();
        let first = tenant_log_socket_path(&directory.path().join("one"));
        let second = tenant_log_socket_path(&directory.path().join("two"));
        receiver
            .attach(app_id(), deployment_id(), first.clone(), sink.clone())
            .await
            .unwrap();
        receiver
            .attach(app_id(), deployment_id(), second.clone(), sink)
            .await
            .unwrap();
        assert_eq!(receiver.attached().await, vec![app_id()]);
        assert!(!first.exists(), "the socket nothing writes to is removed");
        assert!(UnixStream::connect(&second).await.is_ok());
    }

    #[tokio::test]
    async fn a_directory_the_socket_cannot_be_bound_in_is_a_failure_rather_than_a_silent_loss() {
        let directory = tempfile::tempdir().unwrap();
        let occupied = directory.path().join("vm");
        std::fs::write(&occupied, b"a file, not a directory").unwrap();
        let (sink, _) = mocks::log_sink();
        let receiver = TenantLogReceiver::new();
        assert!(receiver
            .attach(app_id(), deployment_id(), tenant_log_socket_path(&occupied), sink)
            .await
            .is_err());
        assert!(receiver.attached().await.is_empty());
    }

    #[tokio::test]
    async fn frames_this_host_cannot_read_end_the_stream_rather_than_being_guessed_at() {
        let directory = tempfile::tempdir().unwrap();
        let socket_path = tenant_log_socket_path(directory.path());
        let (sink, spy) = mocks::log_sink();
        let receiver = TenantLogReceiver::new();
        receiver
            .attach(app_id(), deployment_id(), socket_path.clone(), sink.clone())
            .await
            .unwrap();

        let mut guest = UnixStream::connect(&socket_path).await.unwrap();
        guest
            .write_all(b"not a frame this host would ever have written")
            .await
            .unwrap();
        guest.flush().await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        assert!(spy.events().is_empty());

        let mut second = UnixStream::connect(&socket_path).await.unwrap();
        second
            .write_all(&encode_frame(ENCODE_KIND_STDOUT, b"after\n"))
            .await
            .unwrap();
        second.flush().await.unwrap();
        until(|| !spy.events().is_empty()).await;
    }

    #[tokio::test]
    async fn a_frame_with_nothing_in_it_is_not_published_as_an_empty_line() {
        let directory = tempfile::tempdir().unwrap();
        let socket_path = tenant_log_socket_path(directory.path());
        let (sink, spy) = mocks::log_sink();
        let receiver = TenantLogReceiver::new();
        receiver
            .attach(app_id(), deployment_id(), socket_path.clone(), sink)
            .await
            .unwrap();

        let mut guest = UnixStream::connect(&socket_path).await.unwrap();
        guest
            .write_all(&encode_frame(ENCODE_KIND_STDOUT, b""))
            .await
            .unwrap();
        guest
            .write_all(&encode_frame(ENCODE_KIND_STDOUT, b"something\n"))
            .await
            .unwrap();
        guest.flush().await.unwrap();

        until(|| !spy.events().is_empty()).await;
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let events = spy.events();
        assert_eq!(events.len(), 1);
        assert_eq!(
            events[0].sequence, 0,
            "an empty frame takes no place in the stream"
        );
    }

    #[tokio::test]
    async fn two_guests_on_the_same_socket_share_one_run_of_sequence_numbers() {
        let directory = tempfile::tempdir().unwrap();
        let socket_path = tenant_log_socket_path(directory.path());
        let (sink, spy) = mocks::log_sink();
        let receiver = TenantLogReceiver::new();
        receiver
            .attach(app_id(), deployment_id(), socket_path.clone(), sink)
            .await
            .unwrap();

        for line in ["first\n", "second\n"] {
            let mut guest = UnixStream::connect(&socket_path).await.unwrap();
            guest
                .write_all(&encode_frame(ENCODE_KIND_STDOUT, line.as_bytes()))
                .await
                .unwrap();
            guest.flush().await.unwrap();
            until({
                let spy = spy.clone();
                let wanted = if line == "first\n" { 1 } else { 2 };
                move || spy.events().len() == wanted
            })
            .await;
        }
        let events = spy.events();
        assert_eq!((events[0].sequence, events[1].sequence), (0, 1));
        assert_eq!(events[0].source_id, events[1].source_id);
    }

    #[test]
    fn the_socket_a_guest_writes_to_sits_beside_the_machine_description_it_boots_from() {
        let working_dir = Path::new("/var/lib/nibrunner/vm/app-1");
        let socket_path = tenant_log_socket_path(working_dir);
        assert_eq!(socket_path.parent(), Some(working_dir));
        assert_ne!(
            socket_path,
            tenant_log_socket_path(Path::new("/var/lib/nibrunner/vm/app-2"))
        );
    }
}
