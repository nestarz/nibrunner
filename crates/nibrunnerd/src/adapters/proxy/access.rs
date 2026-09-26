use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, ErrorKind, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{sync_channel, Receiver, SyncSender, TryRecvError, TrySendError};
use std::thread::JoinHandle;

use hyper::Uri;
use protocol::{AppId, DeploymentId, Timestamp};
use serde::Serialize;

use crate::json_store::make_directory;

const QUEUED_RECORDS: usize = 1024;
const BATCH_RECORDS: usize = 128;
const KEPT_BYTES_PER_APP: u64 = 1024 * 1024;

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AccessRecord {
    at: Timestamp,
    method: String,
    path: String,
    status: u16,
    duration_micros: u64,
    deployment_id: DeploymentId,
}

impl AccessRecord {
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
        }
    }
}

pub struct AccessLog {
    sender: Option<SyncSender<AccessCommand>>,
    worker: Option<JoinHandle<()>>,
    dropped: AtomicU64,
}

enum AccessCommand {
    Record(AppId, AccessRecord),
    Discard(AppId),
}

impl AccessLog {
    pub fn new(directory: PathBuf) -> std::io::Result<Self> {
        make_directory(&directory, 0o700)?;
        let (sender, receiver) = sync_channel(QUEUED_RECORDS);
        let worker = std::thread::Builder::new()
            .name("nibrunner-access-log".into())
            .spawn(move || write_records(receiver, directory))?;
        Ok(Self {
            sender: Some(sender),
            worker: Some(worker),
            dropped: AtomicU64::new(0),
        })
    }

    pub fn record(&self, app_id: AppId, record: AccessRecord) {
        let Some(sender) = &self.sender else {
            return;
        };
        match sender.try_send(AccessCommand::Record(app_id, record)) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) => {
                let dropped = self.dropped.fetch_add(1, Ordering::Relaxed) + 1;
                if dropped % 1000 == 1 {
                    tracing::warn!(dropped, "access log writer could not keep up with proxy traffic");
                }
            }
            Err(TrySendError::Disconnected(_)) => {
                tracing::warn!("access log writer stopped before the proxy");
            }
        }
    }

    pub fn discard(&self, app_id: &AppId) {
        if let Some(sender) = &self.sender {
            if sender.send(AccessCommand::Discard(app_id.clone())).is_err() {
                tracing::warn!(%app_id, "access log writer stopped before app removal");
            }
        }
    }
}

impl Drop for AccessLog {
    fn drop(&mut self) {
        drop(self.sender.take());
        if let Some(worker) = self.worker.take() {
            if worker.join().is_err() {
                tracing::warn!("access log writer stopped unexpectedly");
            }
        }
    }
}

struct AppFile {
    writer: BufWriter<File>,
    size: u64,
}

struct AccessWriter {
    directory: PathBuf,
    files: BTreeMap<AppId, AppFile>,
}

impl AccessWriter {
    fn path(&self, app_id: &AppId) -> PathBuf {
        self.directory.join(format!("{app_id}.access.jsonl"))
    }

    fn previous_path(&self, app_id: &AppId) -> PathBuf {
        self.directory.join(format!("{app_id}.access.jsonl.1"))
    }

    fn open(path: &Path) -> std::io::Result<AppFile> {
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        let size = file.metadata()?.len();
        Ok(AppFile {
            writer: BufWriter::new(file),
            size,
        })
    }

    fn write(&mut self, app_id: AppId, record: AccessRecord) -> std::io::Result<()> {
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
                tracing::warn!(%app_id, %error, "access log could not be flushed");
            }
        }
    }

    fn discard(&mut self, app_id: &AppId) {
        drop(self.files.remove(app_id));
        for path in [self.path(app_id), self.previous_path(app_id)] {
            match std::fs::remove_file(path) {
                Ok(()) => {}
                Err(error) if error.kind() == ErrorKind::NotFound => {}
                Err(error) => tracing::warn!(%app_id, %error, "access log outlived the app"),
            }
        }
    }
}

fn write_records(receiver: Receiver<AccessCommand>, directory: PathBuf) {
    let mut writer = AccessWriter {
        directory,
        files: BTreeMap::new(),
    };
    while let Ok(first) = receiver.recv() {
        let mut records = vec![first];
        while records.len() < BATCH_RECORDS {
            match receiver.try_recv() {
                Ok(record) => records.push(record),
                Err(TryRecvError::Empty | TryRecvError::Disconnected) => break,
            }
        }
        for command in records {
            match command {
                AccessCommand::Record(app_id, record) => {
                    if let Err(error) = writer.write(app_id.clone(), record) {
                        tracing::warn!(%app_id, %error, "access log could not be written");
                    }
                }
                AccessCommand::Discard(app_id) => writer.discard(&app_id),
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
    fn access_records_leave_query_strings_out_of_per_app_files() {
        let directory = tempfile::tempdir().expect("a test directory");
        let logs = AccessLog::new(directory.path().to_path_buf()).expect("access logs");
        let app = AppId::parse("fleet-shop").expect("an app ID");
        let uri: Uri = "/shop/item?token=private".parse().expect("a request URI");
        for _ in 0..5000 {
            logs.record(
                app.clone(),
                AccessRecord::new("GET", &uri, 200, 123, crate::test_support::deployment_id()),
            );
        }
        drop(logs);
        let current = std::fs::read_to_string(directory.path().join("fleet-shop.access.jsonl"))
            .expect("the current access log");
        assert!(current.contains("\"path\":\"/shop/item\""));
        assert!(!current.contains("private"));
        assert!(current
            .lines()
            .all(|line| serde_json::from_str::<serde_json::Value>(line).is_ok()));
        assert!(current.len() as u64 <= KEPT_BYTES_PER_APP);
    }

    #[test]
    fn access_writer_rotates_complete_lines_at_one_megabyte() {
        let directory = tempfile::tempdir().expect("a test directory");
        let mut writer = AccessWriter {
            directory: directory.path().to_path_buf(),
            files: BTreeMap::new(),
        };
        let app = AppId::parse("fleet-shop").expect("an app ID");
        let uri: Uri = "/shop/item".parse().expect("a request URI");
        for _ in 0..20_000 {
            writer
                .write(
                    app.clone(),
                    AccessRecord::new("GET", &uri, 200, 123, crate::test_support::deployment_id()),
                )
                .expect("write an access record");
        }
        writer.flush();
        for name in ["fleet-shop.access.jsonl", "fleet-shop.access.jsonl.1"] {
            let content = std::fs::read_to_string(directory.path().join(name)).expect("rotated log");
            assert!(content.len() as u64 <= KEPT_BYTES_PER_APP);
            assert!(content
                .lines()
                .all(|line| serde_json::from_str::<serde_json::Value>(line).is_ok()));
        }
    }

    #[test]
    fn discarding_an_app_removes_its_access_history() {
        let directory = tempfile::tempdir().expect("a test directory");
        let logs = AccessLog::new(directory.path().to_path_buf()).expect("access logs");
        let app = AppId::parse("fleet-shop").expect("an app ID");
        let uri: Uri = "/shop/item".parse().expect("a request URI");
        logs.record(
            app.clone(),
            AccessRecord::new("GET", &uri, 200, 123, crate::test_support::deployment_id()),
        );
        logs.discard(&app);
        drop(logs);
        assert!(!directory.path().join("fleet-shop.access.jsonl").exists());
    }
}
