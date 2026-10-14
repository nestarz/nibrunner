use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use guest_contract::cron_registration::{
    decode_request, decode_request_header, encode_reply, HEADER_BYTES, STATUS_REJECTED,
};
use protocol::{AppId, DeploymentId};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{Mutex, Semaphore};
use tokio::task::{JoinHandle, JoinSet};

use crate::domain::cron::registration::answer;
use crate::domain::cron::registry::CronRegistry;

const MAX_CONNECTIONS: usize = 4;
const REQUEST_TIMEOUT: Duration = Duration::from_secs(5);

pub fn cron_registration_socket_path(working_dir: &Path) -> PathBuf {
    working_dir.join(format!(
        "{}_{}",
        guest_contract::vsock::GUEST_VSOCK_FILENAME,
        guest_contract::vsock::CRON_REGISTRATION_PORT
    ))
}

struct Attachment {
    source: AppId,
    deployment_id: DeploymentId,
    socket_path: PathBuf,
    task: JoinHandle<()>,
}

impl Drop for Attachment {
    fn drop(&mut self) {
        self.task.abort();
        let _ = std::fs::remove_file(&self.socket_path);
    }
}

pub struct CronRegistrationReceiver {
    registry: Arc<CronRegistry>,
    attachments: Mutex<BTreeMap<AppId, Attachment>>,
}

impl CronRegistrationReceiver {
    pub fn new(registry: Arc<CronRegistry>) -> Arc<Self> {
        Arc::new(Self {
            registry,
            attachments: Mutex::new(BTreeMap::new()),
        })
    }

    pub async fn attach(
        &self,
        app_id: AppId,
        deployment_id: DeploymentId,
        socket_path: PathBuf,
    ) -> std::io::Result<()> {
        self.attach_as(app_id.clone(), app_id, deployment_id, socket_path)
            .await
    }

    /// Answers the guest of the microVM run under `app_id` as `source`, whose crontab it
    /// registers: the same app, unless the microVM is a new deployment booted beside `source`.
    pub async fn attach_as(
        &self,
        app_id: AppId,
        source: AppId,
        deployment_id: DeploymentId,
        socket_path: PathBuf,
    ) -> std::io::Result<()> {
        let mut attachments = self.attachments.lock().await;
        if attachments.get(&app_id).is_some_and(|existing| {
            existing.socket_path == socket_path
                && existing.deployment_id == deployment_id
                && existing.source == source
        }) {
            return Ok(());
        }
        attachments.remove(&app_id);
        if let Some(parent) = socket_path.parent() {
            crate::json_store::make_directory(parent, 0o700)?;
        }
        match std::fs::remove_file(&socket_path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
        let listener = crate::unix_socket::bind(&socket_path)?;
        let task = tokio::spawn(serve(
            listener,
            self.registry.clone(),
            source.clone(),
            deployment_id.clone(),
        ));
        attachments.insert(
            app_id,
            Attachment {
                source,
                deployment_id,
                socket_path,
                task,
            },
        );
        Ok(())
    }

    pub async fn detach(&self, app_id: &AppId) {
        self.attachments.lock().await.remove(app_id);
    }

    pub async fn attached(&self) -> Vec<AppId> {
        self.attachments.lock().await.keys().cloned().collect()
    }
}

async fn serve(
    listener: UnixListener,
    registry: Arc<CronRegistry>,
    app_id: AppId,
    deployment_id: DeploymentId,
) {
    let connections = Arc::new(Semaphore::new(MAX_CONNECTIONS));
    let mut tasks = JoinSet::new();
    loop {
        tokio::select! {
            connection = listener.accept() => {
                let Ok((mut stream, _)) = connection else { return };
                let permit = connections.clone().try_acquire_owned();
                let registry = registry.clone();
                let app_id = app_id.clone();
                let deployment_id = deployment_id.clone();
                tasks.spawn(async move {
                    let _ = tokio::time::timeout(REQUEST_TIMEOUT, async {
                        match permit {
                            Ok(_permit) => pump(stream, &registry, &app_id, &deployment_id).await,
                            Err(_) => stream.write_all(&encode_reply(STATUS_REJECTED, "too many concurrent cron registration requests")).await,
                        }
                    }).await;
                });
            }
            _ = tasks.join_next(), if !tasks.is_empty() => {}
        }
    }
}

async fn pump(
    mut stream: UnixStream,
    registry: &CronRegistry,
    app_id: &AppId,
    deployment_id: &DeploymentId,
) -> std::io::Result<()> {
    let mut bytes = [0u8; HEADER_BYTES];
    stream.read_exact(&mut bytes).await?;
    let header = match decode_request_header(&bytes) {
        Ok(header) => header,
        Err(error) => {
            return stream
                .write_all(&encode_reply(STATUS_REJECTED, &error.to_string()))
                .await
        }
    };
    let mut body = vec![0u8; header.body_length];
    stream.read_exact(&mut body).await?;
    let mut trailing = [0u8; 1];
    if matches!(stream.try_read(&mut trailing), Ok(1)) {
        return stream
            .write_all(&encode_reply(
                STATUS_REJECTED,
                "the guest sent more than one cron registration frame",
            ))
            .await;
    }
    let request = match decode_request(header, &body) {
        Ok(request) => request,
        Err(error) => {
            return stream
                .write_all(&encode_reply(STATUS_REJECTED, &error.to_string()))
                .await
        }
    };
    let reply = answer(registry, app_id, deployment_id, request, chrono::Utc::now()).await;
    stream.write_all(&encode_reply(reply.status, &reply.text)).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::store::in_memory;
    use crate::repositories::cron_repository::SqliteCron;
    use guest_contract::cron_registration::{
        decode_reply, decode_reply_header, encode_request, RegistrationReply, RegistrationRequest, STATUS_OK,
    };
    use protocol::Crontab;

    fn app() -> AppId {
        AppId::parse("app-1").unwrap()
    }
    fn deployment(value: &str) -> DeploymentId {
        DeploymentId::parse(value).unwrap()
    }

    async fn request(path: &std::path::Path, request: RegistrationRequest) -> RegistrationReply {
        let mut stream = UnixStream::connect(path).await.unwrap();
        stream.write_all(&encode_request(&request)).await.unwrap();
        let mut header = [0u8; HEADER_BYTES];
        stream.read_exact(&mut header).await.unwrap();
        let header = decode_reply_header(&header).unwrap();
        let mut body = vec![0u8; header.body_length];
        stream.read_exact(&mut body).await.unwrap();
        decode_reply(header, &body).unwrap()
    }

    async fn registry() -> Arc<CronRegistry> {
        let registry = Arc::new(CronRegistry::new(
            Arc::new(SqliteCron::new(in_memory().await)),
            10,
            chrono_tz::UTC,
        ));
        registry
            .synchronize(&[(app(), deployment("dep-1"))])
            .await
            .unwrap();
        registry
    }

    #[tokio::test]
    async fn a_guest_installs_lists_and_removes_its_original_crontab() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("cron.sock");
        let receiver = CronRegistrationReceiver::new(registry().await);
        receiver
            .attach(app(), deployment("dep-1"), path.clone())
            .await
            .unwrap();
        let text = Crontab::parse("# tenant-secret\n@daily echo ok\n").unwrap();
        assert_eq!(
            request(&path, RegistrationRequest::Replace(text.clone()))
                .await
                .status,
            STATUS_OK
        );
        assert_eq!(
            request(&path, RegistrationRequest::List).await.text,
            text.expose()
        );
        assert_eq!(
            request(&path, RegistrationRequest::Replace(Crontab::parse("").unwrap()))
                .await
                .status,
            STATUS_OK
        );
        assert_eq!(request(&path, RegistrationRequest::List).await.text, "");
        receiver.detach(&app()).await;
        assert!(!path.exists());
    }

    #[tokio::test]
    async fn a_stale_guest_cannot_read_or_replace_the_new_deployments_table() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("cron.sock");
        let registry = registry().await;
        let receiver = CronRegistrationReceiver::new(registry.clone());
        receiver
            .attach(app(), deployment("dep-1"), path.clone())
            .await
            .unwrap();
        registry
            .synchronize(&[(app(), deployment("dep-2"))])
            .await
            .unwrap();
        assert_eq!(
            request(&path, RegistrationRequest::List).await.status,
            STATUS_REJECTED
        );
        assert_eq!(
            request(
                &path,
                RegistrationRequest::Replace(Crontab::parse("@daily echo stale").unwrap())
            )
            .await
            .status,
            STATUS_REJECTED
        );
        receiver
            .attach(app(), deployment("dep-2"), path.clone())
            .await
            .unwrap();
        assert_eq!(request(&path, RegistrationRequest::List).await.status, STATUS_OK);
    }

    #[tokio::test]
    async fn dropping_a_receiver_removes_its_socket_and_listener() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("cron.sock");
        let receiver = CronRegistrationReceiver::new(registry().await);
        receiver
            .attach(app(), deployment("dep-1"), path.clone())
            .await
            .unwrap();
        drop(receiver);
        assert!(!path.exists());
        assert!(UnixStream::connect(&path).await.is_err());
    }

    #[tokio::test]
    async fn one_request_cannot_contain_two_registration_frames() {
        let registry = registry().await;
        let (host, mut guest) = UnixStream::pair().unwrap();
        let frames = encode_request(&RegistrationRequest::List).repeat(2);
        guest.write_all(&frames).await.unwrap();
        pump(host, &registry, &app(), &deployment("dep-1")).await.unwrap();
        let mut bytes = [0u8; HEADER_BYTES];
        guest.read_exact(&mut bytes).await.unwrap();
        assert_eq!(decode_reply_header(&bytes).unwrap().status, STATUS_REJECTED);
    }

    #[tokio::test]
    async fn malformed_wire_input_cannot_replace_an_installed_table() {
        let registry = registry().await;
        registry
            .replace(
                &app(),
                &deployment("dep-1"),
                "@daily echo retained",
                chrono::Utc::now(),
            )
            .await
            .unwrap();
        for bytes in [
            b"NBC1\x01\x00\x01\x00\x01".as_slice(),
            b"NBC1\x01\x00\x00\x00\x02\xff\xfe",
            b"NBC1\x01\x00\x00\x00\x02a\0",
            b"NBC1\x02\x00\x00\x00\x01",
        ] {
            let (host, mut guest) = UnixStream::pair().unwrap();
            guest.write_all(bytes).await.unwrap();
            pump(host, &registry, &app(), &deployment("dep-1")).await.unwrap();
            let mut header = [0u8; HEADER_BYTES];
            guest.read_exact(&mut header).await.unwrap();
            assert_eq!(decode_reply_header(&header).unwrap().status, STATUS_REJECTED);
        }
        assert_eq!(
            registry
                .list(&app(), &deployment("dep-1"))
                .await
                .unwrap()
                .expose(),
            "@daily echo retained"
        );
    }

    #[tokio::test]
    async fn a_body_cut_short_never_becomes_a_partial_replacement() {
        let registry = registry().await;
        let (host, mut guest) = UnixStream::pair().unwrap();
        guest.write_all(b"NBC1\x01\x00\x00\x00\x05ab").await.unwrap();
        guest.shutdown().await.unwrap();
        assert!(pump(host, &registry, &app(), &deployment("dep-1")).await.is_err());
        assert!(registry.tables().await.unwrap()[0].jobs.is_empty());
    }
}
