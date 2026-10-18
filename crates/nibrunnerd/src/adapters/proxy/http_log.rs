use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, ErrorKind, Write};
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use tokio::sync::{
    mpsc::{
        channel,
        error::{TryRecvError, TrySendError},
        Receiver, Sender,
    },
    oneshot, Mutex,
};

use hyper::Uri;
use protocol::{AppId, DeploymentId, InstanceLabels, Timestamp};
use serde::Serialize;

use crate::adapters::logs::ObjectSink;
use crate::json_store::make_directory;

const QUEUED_RECORDS: usize = 1024;
const BATCH_RECORDS: usize = 128;
const KEPT_BYTES_PER_APP: u64 = 1024 * 1024;

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct HttpRequestRecord {
    pub(crate) at: Timestamp,
    pub(crate) method: String,
    pub(crate) path: String,
    pub(crate) status: u16,
    pub(crate) duration_micros: u64,
    deployment_id: DeploymentId,
    #[serde(skip_serializing_if = "InstanceLabels::is_empty")]
    pub(crate) labels: InstanceLabels,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) wake_micros: Option<u64>,
}

/// The header on which the activator hands back how long it held a request for its app to wake,
/// to the router that writes the request record and strips it. Named afresh each run, because a
/// guest that is up answers the router itself and could otherwise claim a wake that never was.
pub(super) fn wake_header() -> &'static hyper::header::HeaderName {
    static NAME: std::sync::OnceLock<hyper::header::HeaderName> = std::sync::OnceLock::new();
    NAME.get_or_init(|| {
        hyper::header::HeaderName::try_from(format!("x-nibrunner-wake-{}", uuid::Uuid::new_v4().simple()))
            .expect("a UUID in hex is a header name")
    })
}

/// How long the activator held the response's request for a wake, taken off the response so that
/// no client sees it.
pub(super) fn take_wake_micros(headers: &mut hyper::HeaderMap) -> Option<u64> {
    headers
        .remove(wake_header())
        .and_then(|value| value.to_str().ok()?.parse().ok())
}

impl HttpRequestRecord {
    pub fn new(
        method: &str,
        uri: &Uri,
        status: u16,
        duration_micros: u64,
        deployment_id: DeploymentId,
    ) -> Self {
        Self {
            at: Timestamp::now(),
            method: method.chars().take(16).collect(),
            path: uri.path().chars().take(256).collect(),
            status,
            duration_micros,
            deployment_id,
            labels: InstanceLabels::default(),
            wake_micros: None,
        }
    }

    /// The part of `duration_micros` the request spent held for a restore or a cold boot.
    pub fn woken_for(mut self, wake_micros: Option<u64>) -> Self {
        self.wake_micros = wake_micros;
        self
    }

    pub fn labelled(mut self, labels: InstanceLabels) -> Self {
        self.labels = labels;
        self
    }
}

pub struct HttpRequestLog {
    sender: Option<Sender<HttpLogCommand>>,
    apps: Mutex<BTreeMap<AppId, Arc<AtomicBool>>>,
    worker: Option<JoinHandle<()>>,
    dropped: AtomicU64,
    objects: Option<Arc<ObjectSink>>,
}

pub(super) struct HttpRequestScope {
    app_id: AppId,
    active: Arc<AtomicBool>,
}

enum HttpLogCommand {
    Record(HttpRequestScope, HttpRequestRecord),
    Discard(AppId, oneshot::Sender<()>),
}

impl HttpRequestLog {
    pub fn new(directory: PathBuf) -> std::io::Result<Self> {
        make_directory(&directory, 0o700)?;
        let (sender, receiver) = channel(QUEUED_RECORDS);
        let worker = std::thread::Builder::new()
            .name("nibrunner-http-log".into())
            .spawn(move || write_records(receiver, directory))?;
        Ok(Self {
            sender: Some(sender),
            apps: Mutex::new(BTreeMap::new()),
            worker: Some(worker),
            dropped: AtomicU64::new(0),
            objects: None,
        })
    }

    /// Every request is also shipped to a control plane's bucket.
    pub fn with_objects(mut self, objects: Arc<ObjectSink>) -> Self {
        self.objects = Some(objects);
        self
    }

    pub(super) async fn begin(&self, app_id: &AppId) -> HttpRequestScope {
        let active = self
            .apps
            .lock()
            .await
            .entry(app_id.clone())
            .or_insert_with(|| Arc::new(AtomicBool::new(true)))
            .clone();
        HttpRequestScope {
            app_id: app_id.clone(),
            active,
        }
    }

    pub(super) fn record(&self, scope: HttpRequestScope, record: HttpRequestRecord) {
        if let Some(objects) = &self.objects {
            objects.request(&scope.app_id, &record);
        }
        let Some(sender) = &self.sender else {
            return;
        };
        match sender.try_send(HttpLogCommand::Record(scope, record)) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) => {
                let dropped = self.dropped.fetch_add(1, Ordering::Relaxed) + 1;
                if dropped % 1000 == 1 {
                    tracing::warn!(
                        dropped,
                        "HTTP request log writer could not keep up with proxy traffic"
                    );
                }
            }
            Err(TrySendError::Closed(_)) => {
                tracing::warn!("HTTP request log writer stopped before the proxy");
            }
        }
    }

    pub async fn discard(&self, app_id: &AppId) {
        if let Some(active) = self.apps.lock().await.remove(app_id) {
            active.store(false, Ordering::Release);
        }
        if let Some(sender) = &self.sender {
            let (done, finished) = oneshot::channel();
            if sender
                .send(HttpLogCommand::Discard(app_id.clone(), done))
                .await
                .is_err()
                || finished.await.is_err()
            {
                tracing::warn!(%app_id, "HTTP request log writer stopped before app removal");
            }
        }
    }
}

impl Drop for HttpRequestLog {
    fn drop(&mut self) {
        drop(self.sender.take());
        if let Some(worker) = self.worker.take() {
            if worker.join().is_err() {
                tracing::warn!("HTTP request log writer stopped unexpectedly");
            }
        }
    }
}

struct AppFile {
    writer: BufWriter<File>,
    size: u64,
}

struct HttpLogWriter {
    directory: PathBuf,
    files: BTreeMap<AppId, AppFile>,
}

impl HttpLogWriter {
    fn path(&self, app_id: &AppId) -> PathBuf {
        self.directory.join(format!("{app_id}.http.jsonl"))
    }

    fn previous_path(&self, app_id: &AppId) -> PathBuf {
        self.directory.join(format!("{app_id}.http.jsonl.1"))
    }

    fn open(path: &Path) -> std::io::Result<AppFile> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .mode(0o600)
            .open(path)?;
        let size = file.metadata()?.len();
        Ok(AppFile {
            writer: BufWriter::new(file),
            size,
        })
    }

    fn write(&mut self, app_id: AppId, record: HttpRequestRecord) -> std::io::Result<()> {
        let mut line = serde_json::to_vec(&record).map_err(std::io::Error::other)?;
        line.push(b'\n');
        if !self.files.contains_key(&app_id) {
            self.files
                .insert(app_id.clone(), Self::open(&self.path(&app_id))?);
        }
        if self
            .files
            .get(&app_id)
            .is_some_and(|file| file.size > 0 && file.size + line.len() as u64 > KEPT_BYTES_PER_APP)
        {
            if let Some(mut full) = self.files.remove(&app_id) {
                full.writer.flush()?;
            }
            std::fs::rename(self.path(&app_id), self.previous_path(&app_id))?;
            self.files
                .insert(app_id.clone(), Self::open(&self.path(&app_id))?);
        }
        if let Some(file) = self.files.get_mut(&app_id) {
            file.writer.write_all(&line)?;
            file.size += line.len() as u64;
        }
        Ok(())
    }

    fn flush(&mut self) {
        for (app_id, file) in &mut self.files {
            if let Err(error) = file.writer.flush() {
                tracing::warn!(%app_id, %error, "HTTP request log could not be flushed");
            }
        }
    }

    fn discard(&mut self, app_id: &AppId) {
        drop(self.files.remove(app_id));
        for path in [self.path(app_id), self.previous_path(app_id)] {
            match std::fs::remove_file(path) {
                Ok(()) => {}
                Err(error) if error.kind() == ErrorKind::NotFound => {}
                Err(error) => tracing::warn!(%app_id, %error, "HTTP request log outlived the app"),
            }
        }
    }
}

fn write_records(mut receiver: Receiver<HttpLogCommand>, directory: PathBuf) {
    let mut writer = HttpLogWriter {
        directory,
        files: BTreeMap::new(),
    };
    while let Some(first) = receiver.blocking_recv() {
        let mut records = vec![first];
        while records.len() < BATCH_RECORDS {
            match receiver.try_recv() {
                Ok(record) => records.push(record),
                Err(TryRecvError::Empty | TryRecvError::Disconnected) => break,
            }
        }
        for command in records {
            match command {
                HttpLogCommand::Record(scope, record) => {
                    if scope.active.load(Ordering::Acquire) {
                        if let Err(error) = writer.write(scope.app_id.clone(), record) {
                            tracing::warn!(app_id = %scope.app_id, %error, "HTTP request log could not be written");
                        }
                    }
                }
                HttpLogCommand::Discard(app_id, done) => {
                    writer.discard(&app_id);
                    let _ = done.send(());
                }
            }
        }
        writer.flush();
    }
    writer.flush();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_record_says_how_long_its_request_waited_for_a_wake_only_when_it_did() {
        let uri: Uri = "/".parse().expect("a request URI");
        let record = |wake_micros| {
            serde_json::to_value(
                HttpRequestRecord::new("GET", &uri, 200, 312_000, crate::test_support::deployment_id())
                    .woken_for(wake_micros),
            )
            .expect("a record")
        };
        assert_eq!(record(Some(240_000))["wakeMicros"], 240_000);
        assert!(record(None).get("wakeMicros").is_none());
    }

    #[test]
    fn the_wake_a_response_carries_is_taken_off_it_before_any_client_sees_it() {
        let mut headers = hyper::HeaderMap::new();
        headers.insert(wake_header(), hyper::header::HeaderValue::from(240_000u64));
        headers.insert("x-nibrunner-wake", hyper::header::HeaderValue::from_static("1"));
        assert_eq!(take_wake_micros(&mut headers), Some(240_000));
        assert!(headers.get(wake_header()).is_none());
        assert_eq!(
            take_wake_micros(&mut headers),
            None,
            "a name a guest could guess is not it"
        );
    }

    #[test]
    fn a_record_carries_its_instances_labels_only_when_the_document_gave_some() {
        let uri: Uri = "/".parse().expect("a request URI");
        let record = |labels| {
            serde_json::to_value(
                HttpRequestRecord::new("GET", &uri, 200, 1, crate::test_support::deployment_id())
                    .labelled(labels),
            )
            .expect("a record")
        };
        let labels: InstanceLabels =
            serde_json::from_value(serde_json::json!({ "app": "shop", "rev": "r-12" })).expect("labels");
        assert_eq!(
            record(labels)["labels"],
            serde_json::json!({ "app": "shop", "rev": "r-12" })
        );
        assert!(record(InstanceLabels::default()).get("labels").is_none());
    }

    #[tokio::test]
    async fn http_records_leave_query_strings_out_of_per_app_files() {
        let directory = tempfile::tempdir().expect("a test directory");
        let logs = HttpRequestLog::new(directory.path().to_path_buf()).expect("HTTP request logs");
        let app = AppId::parse("fleet-shop").expect("an app ID");
        let uri: Uri = "/shop/item?token=private".parse().expect("a request URI");
        for _ in 0..5000 {
            logs.record(
                logs.begin(&app).await,
                HttpRequestRecord::new("GET", &uri, 200, 123, crate::test_support::deployment_id()),
            );
        }
        drop(logs);
        let current = std::fs::read_to_string(directory.path().join("fleet-shop.http.jsonl"))
            .expect("the current HTTP request log");
        assert!(current.contains("\"path\":\"/shop/item\""));
        assert!(!current.contains("private"));
        assert!(current
            .lines()
            .all(|line| serde_json::from_str::<serde_json::Value>(line).is_ok()));
        assert!(current.len() as u64 <= KEPT_BYTES_PER_APP);
    }

    #[test]
    fn http_writer_rotates_complete_lines_at_one_megabyte() {
        let directory = tempfile::tempdir().expect("a test directory");
        let mut writer = HttpLogWriter {
            directory: directory.path().to_path_buf(),
            files: BTreeMap::new(),
        };
        let app = AppId::parse("fleet-shop").expect("an app ID");
        let uri: Uri = "/shop/item".parse().expect("a request URI");
        for _ in 0..20_000 {
            writer
                .write(
                    app.clone(),
                    HttpRequestRecord::new("GET", &uri, 200, 123, crate::test_support::deployment_id()),
                )
                .expect("write an HTTP request record");
        }
        writer.flush();
        for name in ["fleet-shop.http.jsonl", "fleet-shop.http.jsonl.1"] {
            let content = std::fs::read_to_string(directory.path().join(name)).expect("rotated log");
            assert!(content.len() as u64 <= KEPT_BYTES_PER_APP);
            assert!(content
                .lines()
                .all(|line| serde_json::from_str::<serde_json::Value>(line).is_ok()));
        }
    }

    #[tokio::test]
    async fn discarding_an_app_removes_its_http_history() {
        let directory = tempfile::tempdir().expect("a test directory");
        let logs = HttpRequestLog::new(directory.path().to_path_buf()).expect("HTTP request logs");
        let app = AppId::parse("fleet-shop").expect("an app ID");
        let uri: Uri = "/shop/item".parse().expect("a request URI");
        logs.record(
            logs.begin(&app).await,
            HttpRequestRecord::new("GET", &uri, 200, 123, crate::test_support::deployment_id()),
        );
        logs.discard(&app).await;
        drop(logs);
        assert!(!directory.path().join("fleet-shop.http.jsonl").exists());
    }

    #[tokio::test]
    async fn a_request_from_a_forgotten_app_cannot_write_into_its_next_incarnation() {
        let directory = tempfile::tempdir().unwrap();
        let logs = HttpRequestLog::new(directory.path().to_path_buf()).unwrap();
        let app = crate::test_support::app_id();
        let old = logs.begin(&app).await;
        std::fs::write(
            directory.path().join(format!("{app}.http.jsonl.1")),
            b"old history\n",
        )
        .unwrap();
        logs.discard(&app).await;
        assert!(!directory.path().join(format!("{app}.http.jsonl.1")).exists());
        let current = logs.begin(&app).await;
        logs.record(
            current,
            HttpRequestRecord::new(
                "GET",
                &"/current".parse().unwrap(),
                200,
                1,
                crate::test_support::deployment_id(),
            ),
        );
        logs.record(
            old,
            HttpRequestRecord::new(
                "GET",
                &"/forgotten".parse().unwrap(),
                200,
                1,
                crate::test_support::deployment_id(),
            ),
        );
        drop(logs);
        let content = std::fs::read_to_string(directory.path().join(format!("{app}.http.jsonl"))).unwrap();
        assert!(content.contains("/current"));
        assert!(!content.contains("/forgotten"));
        assert_eq!(content.lines().count(), 1);
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            std::fs::metadata(directory.path().join(format!("{app}.http.jsonl")))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
    }

    #[tokio::test]
    async fn discarding_with_a_full_queue_yields_to_the_async_runtime() {
        let (sender, mut receiver) = channel(1);
        let logs = HttpRequestLog {
            sender: Some(sender),
            apps: Mutex::new(BTreeMap::new()),
            worker: None,
            dropped: AtomicU64::new(0),
            objects: None,
        };
        let app = crate::test_support::app_id();
        let scope = logs.begin(&app).await;
        logs.record(
            scope,
            HttpRequestRecord::new(
                "GET",
                &"/".parse().unwrap(),
                200,
                1,
                crate::test_support::deployment_id(),
            ),
        );
        let removal = logs.discard(&app);
        tokio::pin!(removal);
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(10), &mut removal)
                .await
                .is_err()
        );
        let HttpLogCommand::Record(scope, _) = receiver.recv().await.unwrap() else {
            panic!("the queued request");
        };
        assert!(!scope.active.load(Ordering::Acquire));
        tokio::join!(removal, async {
            let HttpLogCommand::Discard(_, done) = receiver.recv().await.unwrap() else {
                panic!("the deletion command");
            };
            let _ = done.send(());
        });
    }
}
