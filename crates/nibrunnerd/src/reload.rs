use std::io::{self, Read, Write};
use std::os::unix::fs::{FileTypeExt, MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::Mutex;

use crate::adapters::net::allocator::SlotAllocator;
use crate::config::HostConfig;
use crate::host::Host;

const MAX_REQUEST: usize = 1024 * 1024;
const MAX_REPLY: usize = 4096;
const TIMEOUT: Duration = Duration::from_secs(5);

#[derive(PartialEq, Eq)]
struct FileStamp {
    device: u64,
    inode: u64,
    length: u64,
    changed: i64,
    changed_nanos: i64,
}

pub struct Inputs(Vec<(PathBuf, Option<FileStamp>)>);

impl Inputs {
    pub fn read(config: &HostConfig) -> io::Result<Self> {
        let mut paths = vec![crate::install::environment_file(&HostConfig::configured_file())];
        for name in ["manifest.json", "vmlinux", "rootfs.ext4"] {
            paths.push(config.guest_image_dir.join(name));
        }
        if let Some(tls) = config.proxy.http.as_ref().and_then(|http| http.tls.as_ref()) {
            paths.extend([tls.certificate.clone(), tls.key.clone()]);
            paths.extend(tls.client_ca.iter().cloned());
        }
        paths
            .into_iter()
            .map(|path| Ok((path.clone(), Self::stamp(&path)?)))
            .collect::<io::Result<Vec<_>>>()
            .map(Self)
    }

    fn stamp(path: &Path) -> io::Result<Option<FileStamp>> {
        match std::fs::metadata(path) {
            Ok(metadata) => Ok(Some(FileStamp {
                device: metadata.dev(),
                inode: metadata.ino(),
                length: metadata.len(),
                changed: metadata.ctime(),
                changed_nanos: metadata.ctime_nsec(),
            })),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error),
        }
    }

    fn unchanged(&self) -> bool {
        self.0
            .iter()
            .all(|(path, stamp)| Self::stamp(path).is_ok_and(|now| now == *stamp))
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct Program {
    device: u64,
    inode: u64,
}

impl Program {
    fn current() -> io::Result<Self> {
        // The pathname may already name an upgrade; /proc/self/exe still names
        // the running image. A new executable must retain start's restart path.
        let path = if cfg!(target_os = "linux") {
            PathBuf::from("/proc/self/exe")
        } else {
            std::env::current_exe()?
        };
        let metadata = std::fs::metadata(path)?;
        Ok(Self {
            device: metadata.dev(),
            inode: metadata.ino(),
        })
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Request {
    program: Program,
    configuration: String,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
enum Reply {
    Unchanged,
    Grown,
    RestartRequired,
    Rejected,
}

pub(crate) enum Applied {
    Unchanged,
    Grown,
}

fn socket(config: &HostConfig) -> PathBuf {
    config.runtime_dir.join("configuration.sock")
}

pub(crate) fn request(config: &HostConfig) -> io::Result<Option<Applied>> {
    let mut stream = match std::os::unix::net::UnixStream::connect(socket(config)) {
        Ok(stream) => stream,
        Err(error)
            if matches!(
                error.kind(),
                io::ErrorKind::NotFound | io::ErrorKind::ConnectionRefused
            ) =>
        {
            return Ok(None);
        }
        Err(error) => return Err(error),
    };
    stream.set_read_timeout(Some(TIMEOUT))?;
    stream.set_write_timeout(Some(TIMEOUT))?;
    let bytes = serde_json::to_vec(&Request {
        program: Program::current()?,
        configuration: config.to_toml(),
    })?;
    if bytes.len() > MAX_REQUEST {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "configuration request is too large",
        ));
    }
    stream.write_all(&(bytes.len() as u32).to_be_bytes())?;
    stream.write_all(&bytes)?;
    let mut length = [0; 4];
    stream.read_exact(&mut length)?;
    let length = u32::from_be_bytes(length) as usize;
    if length > MAX_REPLY {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "configuration reply is too large",
        ));
    }
    let mut bytes = vec![0; length];
    stream.read_exact(&mut bytes)?;
    match serde_json::from_slice(&bytes)? {
        Reply::Unchanged => Ok(Some(Applied::Unchanged)),
        Reply::Grown => Ok(Some(Applied::Grown)),
        Reply::RestartRequired => Ok(None),
        Reply::Rejected => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "the running host refused the configuration",
        )),
    }
}

struct Current {
    configuration: HostConfig,
    program: Program,
    allocator: Arc<Mutex<SlotAllocator>>,
    inputs: Inputs,
    policy: Arc<crate::runtime_policy::RuntimePolicy>,
}

impl Current {
    async fn apply(&mut self, request: Request) -> Reply {
        if request.program != self.program || !self.inputs.unchanged() {
            return Reply::RestartRequired;
        }
        let next = match HostConfig::from_toml(&request.configuration) {
            Ok(next) => next,
            Err(_) => return Reply::Rejected,
        };
        let mut supported = self.configuration.clone();
        supported.max_apps = next.max_apps;
        supported.http_admission.clone_from(&next.http_admission);
        supported.vm_budgets.clone_from(&next.vm_budgets);
        if supported != next || next.max_apps < self.configuration.max_apps {
            return Reply::RestartRequired;
        }
        if next == self.configuration {
            return Reply::Unchanged;
        }
        // The ZeroFS export reader occupies the device just past the last app slot.
        // Growing the ring would hand a guest the reader's still-attached device.
        if next.volumes.zerofs().is_some() && next.max_apps != self.configuration.max_apps {
            return Reply::RestartRequired;
        }
        let mut allocator = self.allocator.lock().await;
        allocator.grow(next.max_apps);
        self.policy
            .replace(next.http_admission.clone(), next.vm_budgets.clone());
        self.configuration = next;
        Reply::Grown
    }
}

async fn bind(path: &Path) -> io::Result<tokio::net::UnixListener> {
    if let Ok(metadata) = std::fs::symlink_metadata(path) {
        if !metadata.file_type().is_socket() {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "configuration socket path is occupied",
            ));
        }
        match tokio::time::timeout(TIMEOUT, tokio::net::UnixStream::connect(path)).await {
            Ok(Err(error)) if error.kind() == io::ErrorKind::ConnectionRefused => std::fs::remove_file(path)?,
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::AddrInUse,
                    "another host owns the configuration socket",
                ))
            }
        }
    }
    let listener = tokio::net::UnixListener::bind(path)?;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
    Ok(listener)
}

async fn answer(mut stream: tokio::net::UnixStream, current: Arc<Mutex<Current>>) -> io::Result<()> {
    let length = stream.read_u32().await? as usize;
    if length > MAX_REQUEST {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "configuration request is too large",
        ));
    }
    let mut bytes = vec![0; length];
    stream.read_exact(&mut bytes).await?;
    let request = serde_json::from_slice(&bytes)?;
    let reply = current.lock().await.apply(request).await;
    let bytes = serde_json::to_vec(&reply)?;
    stream.write_u32(bytes.len() as u32).await?;
    stream.write_all(&bytes).await
}

pub async fn serve(host: &Arc<Host>, inputs: Inputs) -> io::Result<tokio::task::JoinHandle<()>> {
    let listener = bind(&socket(&host.config)).await?;
    let current = Arc::new(Mutex::new(Current {
        configuration: host.config.clone(),
        program: Program::current()?,
        allocator: host.allocator.clone(),
        inputs,
        policy: host.runtime_policy.clone(),
    }));
    Ok(tokio::spawn(async move {
        loop {
            let (stream, _) = match listener.accept().await {
                Ok(accepted) => accepted,
                Err(error) => {
                    tracing::error!(%error, "host configuration listener failed");
                    return;
                }
            };
            let current = current.clone();
            tokio::spawn(async move {
                let _ = tokio::time::timeout(TIMEOUT, answer(stream, current)).await;
            });
        }
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use protocol::AppId;

    fn current() -> Current {
        let mut configuration = HostConfig::under(Path::new("/srv/nibrunner"));
        configuration.max_apps = 2;
        Current {
            configuration,
            program: Program::current().unwrap(),
            allocator: Arc::new(Mutex::new(SlotAllocator::addressing(2))),
            inputs: Inputs(Vec::new()),
            policy: Arc::new(crate::runtime_policy::RuntimePolicy::default()),
        }
    }

    fn request(current: &Current, config: &HostConfig) -> Request {
        Request {
            program: current.program,
            configuration: config.to_toml(),
        }
    }

    #[tokio::test]
    async fn growing_capacity_preserves_live_assignments_and_allows_another_app() {
        let mut current = current();
        let first = AppId::parse("one").unwrap();
        let second = AppId::parse("two").unwrap();
        let third = AppId::parse("three").unwrap();
        let before = current.allocator.lock().await.allocate(&first).unwrap();
        current.allocator.lock().await.allocate(&second).unwrap();
        assert!(current.allocator.lock().await.allocate(&third).is_err());
        let mut next = current.configuration.clone();
        next.max_apps = 3;
        assert_eq!(current.apply(request(&current, &next)).await, Reply::Grown);
        assert_eq!(current.allocator.lock().await.lookup(&first), Some(before));
        assert!(current.allocator.lock().await.allocate(&third).is_ok());
        assert_eq!(current.apply(request(&current, &next)).await, Reply::Unchanged);
    }

    #[tokio::test]
    async fn other_configuration_changes_and_shrinking_require_a_restart_without_partial_application() {
        let mut current = current();
        let mut next = current.configuration.clone();
        next.max_apps = 4;
        next.desired_state_file = PathBuf::from("/another/document.json");
        assert_eq!(
            current.apply(request(&current, &next)).await,
            Reply::RestartRequired
        );
        assert_eq!(current.allocator.lock().await.limit(), 2);
        next = current.configuration.clone();
        next.max_apps = 1;
        assert_eq!(
            current.apply(request(&current, &next)).await,
            Reply::RestartRequired
        );
        assert_eq!(current.allocator.lock().await.limit(), 2);
    }

    #[tokio::test]
    async fn upgrading_the_executable_still_requires_a_restart() {
        let mut current = current();
        let mut requested = request(&current, &current.configuration);
        requested.program.inode += 1;
        assert_eq!(current.apply(requested).await, Reply::RestartRequired);
    }

    #[tokio::test]
    async fn runtime_policies_are_applied_together_only_when_the_whole_change_is_supported() {
        let mut current = current();
        let app = AppId::parse("app-one").unwrap();
        let mut next = current.configuration.clone();
        next.http_admission = Some(crate::config::HttpAdmission {
            host_concurrent: 16.try_into().unwrap(),
            app_concurrent: 4.try_into().unwrap(),
            apps: Default::default(),
        });
        next.vm_budgets = Some(crate::config::VmBudgets {
            default: crate::config::VmBudget {
                cpu_percent: 50.try_into().unwrap(),
                memory_mib: 512.try_into().unwrap(),
            },
            apps: Default::default(),
        });
        next.max_concurrent_vm_starts = 2.try_into().ok();
        assert_eq!(
            current.apply(request(&current, &next)).await,
            Reply::RestartRequired
        );
        assert_eq!(current.policy.http_limits(&app), None);
        assert_eq!(current.policy.vm_budget(&app), None);
        next.max_concurrent_vm_starts = None;
        assert_eq!(current.apply(request(&current, &next)).await, Reply::Grown);
        assert_eq!(current.policy.http_limits(&app), Some((16, 4)));
        assert_eq!(current.policy.vm_budget(&app).unwrap().cpu_percent.get(), 50);
        assert_eq!(current.apply(request(&current, &next)).await, Reply::Unchanged);
    }

    #[tokio::test]
    async fn growing_a_zerofs_host_requires_a_restart_without_moving_the_export_reader() {
        let mut current = current();
        current.configuration.volumes =
            crate::config::VolumeBackend::Zerofs(Box::new(crate::test_support::zerofs_settings(|_| {})));
        let mut next = current.configuration.clone();
        assert_eq!(current.apply(request(&current, &next)).await, Reply::Unchanged);
        next.max_apps += 1;
        assert_eq!(
            current.apply(request(&current, &next)).await,
            Reply::RestartRequired
        );
        assert_eq!(current.allocator.lock().await.limit(), 2);
    }

    #[tokio::test]
    async fn changing_a_startup_file_requires_a_restart_even_when_its_path_stays_the_same() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("host.env");
        std::fs::write(&path, b"before").unwrap();
        let mut current = current();
        current.inputs = Inputs(vec![(path.clone(), Inputs::stamp(&path).unwrap())]);
        assert_eq!(
            current.apply(request(&current, &current.configuration)).await,
            Reply::Unchanged
        );
        let replacement = directory.path().join("replacement");
        std::fs::write(&replacement, b"after").unwrap();
        std::fs::rename(replacement, path).unwrap();
        assert_eq!(
            current.apply(request(&current, &current.configuration)).await,
            Reply::RestartRequired
        );
    }

    #[tokio::test]
    async fn the_client_waits_for_the_running_allocator_to_accept_the_new_capacity() {
        let directory = tempfile::tempdir().unwrap();
        let mut current = current();
        current.configuration.runtime_dir = directory.path().to_path_buf();
        current.configuration.firecracker_dir = directory.path().join("firecracker");
        let mut next = current.configuration.clone();
        next.max_apps += 1;
        let listener = bind(&socket(&next)).await.unwrap();
        let current = Arc::new(Mutex::new(current));
        let serving = current.clone();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            answer(stream, serving).await.unwrap();
        });
        let reply = tokio::task::spawn_blocking(move || super::request(&next))
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(reply, Some(Applied::Grown)));
        server.await.unwrap();
        assert_eq!(current.lock().await.allocator.lock().await.limit(), 3);
    }

    #[tokio::test]
    async fn oversized_requests_are_refused_before_reading_the_body() {
        let (mut client, server) = tokio::net::UnixStream::pair().unwrap();
        client.write_u32(MAX_REQUEST as u32 + 1).await.unwrap();
        let result = answer(server, Arc::new(Mutex::new(current()))).await;
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::InvalidInput);
    }

    #[tokio::test]
    async fn a_configuration_socket_never_replaces_an_unrelated_file_or_live_listener() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("configuration.sock");
        std::fs::write(&path, b"keep").unwrap();
        assert!(bind(&path).await.is_err());
        assert_eq!(std::fs::read(&path).unwrap(), b"keep");
        std::fs::remove_file(&path).unwrap();
        let listener = bind(&path).await.unwrap();
        assert_eq!(
            std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o600
        );
        assert!(bind(&path).await.is_err());
        drop(listener);
        assert!(bind(&path).await.is_ok());
    }
}
