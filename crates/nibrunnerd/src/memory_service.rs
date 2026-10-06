use std::collections::BTreeMap;
use std::io;
use std::num::NonZeroU32;
use std::os::unix::fs::{FileTypeExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use protocol::memory::{Reply, Request, MAX_FRAME_BYTES, SOCKET_NAME};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::Mutex;

use crate::adapters::vm::process::{process_start_ticks, read_host_boot_id};
use crate::host::Host;
use crate::ports::CommandRequest;

const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
const SWEEP_INTERVAL: Duration = Duration::from_secs(15);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Owner {
    pid: i32,
    start_ticks: u64,
    cgroup: String,
}

impl Owner {
    fn for_pid(pid: i32) -> io::Result<Self> {
        let start_ticks = process_start_ticks(pid)
            .filter(|_| pid > 0)
            .ok_or_else(|| io::Error::other("the memory lease owner cannot be identified"))?;
        let membership = std::fs::read_to_string(format!("/proc/{pid}/cgroup"))?;
        let cgroup = membership
            .lines()
            .find_map(|line| line.strip_prefix("0::"))
            .filter(|group| isolated_group(group).is_some())
            .ok_or_else(|| io::Error::other("the memory lease owner needs a service or scope cgroup"))?
            .to_owned();
        if process_start_ticks(pid) != Some(start_ticks) {
            return Err(io::Error::other("the memory lease owner exited"));
        }
        Ok(Self {
            pid,
            start_ticks,
            cgroup,
        })
    }

    fn gone(&self) -> bool {
        process_start_ticks(self.pid) != Some(self.start_ticks)
            && isolated_group(&self.cgroup).is_some_and(|path| group_empty(&path).unwrap_or(false))
    }
}

fn isolated_group(group: &str) -> Option<PathBuf> {
    let path = crate::adapters::cgroup::cgroup_path(&format!("0::{group}"))?;
    let name = path.file_name()?.to_str()?;
    (name.ends_with(".service") || name.ends_with(".scope")).then_some(path)
}

fn group_empty(path: &Path) -> io::Result<bool> {
    match std::fs::read_to_string(path.join("cgroup.events")) {
        Ok(events) => Ok(events.lines().any(|line| line == "populated 0")),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(true),
        Err(error) => Err(error),
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Lease {
    owner: Owner,
    unit: String,
    memory_mib: NonZeroU32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    range: Option<MemoryRange>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct MemoryRange {
    minimum: NonZeroU32,
    preferred: NonZeroU32,
}

impl Lease {
    fn requested(&self) -> MemoryRange {
        self.range.unwrap_or(MemoryRange {
            minimum: self.memory_mib,
            preferred: self.memory_mib,
        })
    }
}

fn smaller_budget(current: NonZeroU32, minimum: NonZeroU32, shortfall_mib: u64) -> Option<NonZeroU32> {
    let reduced = u64::from(current.get()).checked_sub(shortfall_mib)?;
    (reduced >= u64::from(minimum.get()) && reduced < u64::from(current.get()))
        .then(|| u32::try_from(reduced).ok().and_then(NonZeroU32::new))
        .flatten()
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Ledger {
    boot_id: String,
    leases: BTreeMap<String, Lease>,
}

pub struct MemoryService {
    host: Arc<Host>,
    path: PathBuf,
    held: Mutex<Ledger>,
}

fn valid_id(id: &str) -> bool {
    uuid::Uuid::parse_str(id).is_ok_and(|parsed| parsed.hyphenated().to_string() == id)
}

fn valid_unit(unit: &str) -> bool {
    unit.len() <= 200
        && unit.len() > ".service".len()
        && (unit.ends_with(".service") || unit.ends_with(".slice"))
        && unit.as_bytes()[0].is_ascii_alphanumeric()
        && unit
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"-_.".contains(&byte))
}

impl MemoryService {
    // Restore before starting the lifecycle: an old build can outlive this daemon.
    pub fn restore(host: Arc<Host>) -> io::Result<Arc<Self>> {
        Self::restore_for_boot(host, read_host_boot_id()?.trim().to_owned())
    }

    fn restore_for_boot(host: Arc<Host>, boot_id: String) -> io::Result<Arc<Self>> {
        let path = host.config.state_dir.join("external-memory.json");
        let mut ledger = match crate::json_store::read_bytes(&path).map_err(io::Error::other)? {
            Some(bytes) => serde_json::from_slice::<Ledger>(&bytes)?,
            None => Ledger {
                boot_id: boot_id.clone(),
                leases: BTreeMap::new(),
            },
        };
        if ledger.boot_id != boot_id {
            ledger = Ledger {
                boot_id,
                leases: BTreeMap::new(),
            };
        }
        for (id, lease) in &ledger.leases {
            if !valid_id(id)
                || !valid_unit(&lease.unit)
                || lease.owner.pid <= 0
                || isolated_group(&lease.owner.cgroup).is_none()
                || lease.requested().minimum > lease.memory_mib
                || lease.memory_mib > lease.requested().preferred
            {
                return Err(io::Error::other("the memory lease ledger is invalid"));
            }
        }
        for (id, lease) in &ledger.leases {
            host.state.restore_external(id, lease.memory_mib);
        }
        Ok(Arc::new(Self {
            host,
            path,
            held: Mutex::new(ledger),
        }))
    }

    fn persist(&self, ledger: &Ledger) -> io::Result<()> {
        crate::json_store::write_json(&self.path, ledger).map_err(io::Error::other)
    }

    async fn handle(&self, owner: Owner, request: Request) -> io::Result<Reply> {
        let mut held = self.held.lock().await;
        match request {
            Request::Acquire {
                id,
                unit,
                memory_mib,
                minimum_mib,
            } => {
                if !valid_id(&id) || !valid_unit(&unit) {
                    return Ok(Reply::Rejected {
                        reason: "a lease needs a canonical UUID and a service or slice unit name".into(),
                    });
                }
                if minimum_mib.is_some_and(|minimum| minimum > memory_mib) {
                    return Ok(Reply::Rejected {
                        reason: "the minimum memory exceeds the preferred budget".into(),
                    });
                }
                let mut lease = Lease {
                    owner,
                    unit,
                    memory_mib,
                    range: minimum_mib.map(|minimum| MemoryRange {
                        minimum,
                        preferred: memory_mib,
                    }),
                };
                if let Some(existing) = held.leases.get(&id) {
                    return Ok(
                        if existing.owner == lease.owner
                            && existing.unit == lease.unit
                            && existing.requested() == lease.requested()
                        {
                            Reply::Granted {
                                memory_mib: existing.memory_mib,
                            }
                        } else {
                            Reply::Rejected {
                                reason: "that lease belongs to different work".into(),
                            }
                        },
                    );
                }
                if held.leases.values().any(|existing| existing.unit == lease.unit) {
                    return Ok(Reply::Waiting {
                        reason: "that service already holds a memory lease".into(),
                    });
                }
                let (reservation, memory_mib) = match self.reserve(&id, lease.requested()).await {
                    Ok(granted) => granted,
                    Err(reply) => return Ok(reply),
                };
                lease.memory_mib = memory_mib;
                held.leases.insert(id.clone(), lease);
                if let Err(error) = self.persist(&held) {
                    held.leases.remove(&id);
                    return Err(error);
                }
                reservation.retain();
                Ok(Reply::Granted { memory_mib })
            }
            Request::Release { id } => {
                let Some(lease) = held.leases.get(&id) else {
                    return Ok(Reply::Released);
                };
                if lease.owner != owner {
                    return Ok(Reply::Rejected {
                        reason: "only the lease owner can release it".into(),
                    });
                }
                if !self.finished(&lease.unit).await? {
                    return Ok(Reply::Waiting {
                        reason: "the service still has work or a queued job".into(),
                    });
                }
                self.release(&mut held, &id)?;
                Ok(Reply::Released)
            }
        }
    }

    async fn reserve(
        &self,
        id: &str,
        range: MemoryRange,
    ) -> Result<(crate::state::MemoryReservation, NonZeroU32), Reply> {
        let mut memory_mib = range.preferred;
        for _ in 0..4 {
            let Some(readings) = self.host.memory_readings(None).await else {
                return Err(Reply::Waiting {
                    reason: "host memory cannot be measured".into(),
                });
            };
            match self
                .host
                .state
                .reserve_external(self.host.guest_memory_mib, id, memory_mib, readings)
                .await
            {
                Ok(reservation) => return Ok((reservation, memory_mib)),
                Err(shortfall_mib) => match smaller_budget(memory_mib, range.minimum, shortfall_mib) {
                    Some(reduced) => memory_mib = reduced,
                    None => {
                        return Err(Reply::Waiting {
                            reason: format!("memory admission needs {shortfall_mib} MiB more headroom"),
                        })
                    }
                },
            }
        }
        Err(Reply::Waiting {
            reason: "host memory changed during admission; retry the reservation".into(),
        })
    }

    fn release(&self, held: &mut Ledger, id: &str) -> io::Result<()> {
        let Some(lease) = held.leases.remove(id) else {
            return Ok(());
        };
        if let Err(error) = self.persist(held) {
            held.leases.insert(id.into(), lease);
            return Err(error);
        }
        self.host.state.release_external(id);
        self.host.state.signal_refresh();
        Ok(())
    }

    async fn sweep(&self) -> io::Result<()> {
        let mut held = self.held.lock().await;
        let abandoned: Vec<_> = held
            .leases
            .iter()
            .filter(|(_, lease)| lease.owner.gone())
            .map(|(id, lease)| (id.clone(), lease.unit.clone()))
            .collect();
        for (id, unit) in abandoned {
            if self.finished(&unit).await? {
                self.release(&mut held, &id)?;
            }
        }
        Ok(())
    }

    async fn finished(&self, unit: &str) -> io::Result<bool> {
        let mut request = CommandRequest::new(&[
            "systemctl",
            "show",
            "--all",
            "--property=LoadState,ActiveState,Job,ControlGroup",
            "--",
            unit,
        ]);
        request.timeout = Duration::from_secs(3);
        let response = self.host.commands.run(request).await.map_err(io::Error::other)?;
        let properties: BTreeMap<_, _> = response
            .stdout
            .lines()
            .filter_map(|line| line.split_once('='))
            .collect();
        let value = |key| properties.get(key).copied();
        if !matches!(value("LoadState"), Some("loaded" | "not-found"))
            || !(matches!(value("ActiveState"), Some("inactive" | "failed"))
                || (unit.ends_with(".slice") && value("ActiveState") == Some("active")))
            || value("Job") != Some("")
        {
            return Ok(false);
        }
        match value("ControlGroup") {
            Some("") => Ok(value("ActiveState") != Some("active")
                && (response.code == 0 || value("LoadState") == Some("not-found"))),
            Some(group) if response.code == 0 => {
                let path = crate::adapters::cgroup::cgroup_path(&format!("0::{group}"))
                    .ok_or_else(|| io::Error::other("the service has an invalid cgroup"))?;
                group_empty(&path)
            }
            _ => Ok(false),
        }
    }

    pub async fn serve(self: Arc<Self>) -> io::Result<tokio::task::JoinHandle<()>> {
        let listener = bind(&self.host.config.runtime_dir.join(SOCKET_NAME)).await?;
        Ok(tokio::spawn(async move {
            let permits = Arc::new(tokio::sync::Semaphore::new(32));
            let mut sweep = tokio::time::interval(SWEEP_INTERVAL);
            loop {
                tokio::select! {
                    _ = sweep.tick() => {
                        if let Err(error) = self.sweep().await {
                            tracing::warn!(%error, "memory lease cleanup waits for a valid service observation");
                        }
                    }
                    accepted = listener.accept() => {
                        let (stream, _) = match accepted {
                            Ok(accepted) => accepted,
                            Err(error) => {
                                tracing::error!(%error, "memory admission listener failed");
                                return;
                            }
                        };
                        let Ok(permit) = permits.clone().try_acquire_owned() else { continue; };
                        let service = self.clone();
                        tokio::spawn(async move {
                            let _permit = permit;
                            if let Ok(Err(error)) = tokio::time::timeout(REQUEST_TIMEOUT, service.answer(stream)).await {
                                tracing::debug!(%error, "memory admission request failed");
                            }
                        });
                    }
                }
            }
        }))
    }

    async fn answer(&self, mut stream: UnixStream) -> io::Result<()> {
        let credentials = stream.peer_cred()?;
        if credentials.uid() != 0 {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "memory admission requires root",
            ));
        }
        let owner = Owner::for_pid(
            credentials
                .pid()
                .ok_or_else(|| io::Error::other("missing peer process"))?,
        )?;
        let length = stream.read_u32().await? as usize;
        if length > MAX_FRAME_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "memory request is too large",
            ));
        }
        let mut bytes = vec![0; length];
        stream.read_exact(&mut bytes).await?;
        let reply = self.handle(owner, serde_json::from_slice(&bytes)?).await?;
        let bytes = serde_json::to_vec(&reply)?;
        stream.write_u32(bytes.len() as u32).await?;
        stream.write_all(&bytes).await
    }
}

async fn bind(path: &Path) -> io::Result<UnixListener> {
    if let Ok(metadata) = std::fs::symlink_metadata(path) {
        if !metadata.file_type().is_socket() {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "memory socket path is occupied",
            ));
        }
        match tokio::time::timeout(REQUEST_TIMEOUT, UnixStream::connect(path)).await {
            Ok(Err(error)) if error.kind() == io::ErrorKind::ConnectionRefused => std::fs::remove_file(path)?,
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::AddrInUse,
                    "another host owns the memory socket",
                ))
            }
        }
    }
    let listener = UnixListener::bind(path)?;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
    Ok(listener)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{app_id, mocks, test_host};

    const ID: &str = "03ae9139-c7fc-4ad4-a94b-6e3c81d3b406";
    const UNIT: &str = "mf-build-example-0123456789ab-sandbox.service";

    #[test]
    fn flexible_grants_shrink_by_the_shortfall_without_crossing_the_minimum() {
        let preferred = 2048.try_into().unwrap();
        let minimum = 512.try_into().unwrap();
        assert_eq!(smaller_budget(preferred, minimum, 512).unwrap().get(), 1536);
        assert_eq!(smaller_budget(preferred, minimum, 1536).unwrap().get(), 512);
        for shortfall in [0, 1537, 2048, u64::MAX] {
            assert!(smaller_budget(preferred, minimum, shortfall).is_none());
        }
    }

    fn owner() -> Owner {
        Owner {
            pid: i32::MAX,
            start_ticks: 1,
            cgroup: "/nibrunner-memory-test-absent.scope".into(),
        }
    }

    fn ledger() -> Ledger {
        Ledger {
            boot_id: "boot-1".into(),
            leases: BTreeMap::from([(
                ID.into(),
                Lease {
                    owner: owner(),
                    unit: UNIT.into(),
                    memory_mib: 512.try_into().unwrap(),
                    range: None,
                },
            )]),
        }
    }

    fn seed(host: &Arc<Host>) {
        crate::json_store::write_json(&host.config.state_dir.join("external-memory.json"), &ledger())
            .unwrap();
    }

    #[tokio::test]
    async fn restored_leases_block_apps_before_any_client_reconnects() {
        let host = test_host().await;
        seed(host.arc());
        let state_dir = host.config.state_dir.clone();
        let service = MemoryService::restore_for_boot(host.arc().clone(), "boot-1".into()).unwrap();
        drop(service);
        assert!(host
            .state
            .reserve_memory(512, &app_id(), protocol::DEFAULT_INSTANCE_RESOURCES)
            .await
            .is_err());
        for (boot, blocked) in [("boot-1", true), ("boot-2", false)] {
            let mut restarted = test_host().await;
            Arc::get_mut(&mut restarted.host).unwrap().config.state_dir = state_dir.clone();
            let service = MemoryService::restore_for_boot(restarted.arc().clone(), boot.into()).unwrap();
            assert_eq!(
                restarted
                    .state
                    .reserve_memory(512, &app_id(), protocol::DEFAULT_INSTANCE_RESOURCES)
                    .await
                    .is_err(),
                blocked
            );
            assert_eq!(service.held.lock().await.leases.is_empty(), !blocked);
        }
    }

    #[tokio::test]
    async fn retrying_a_grant_is_idempotent_but_cannot_change_its_owner_unit_or_budget() {
        let host = test_host().await;
        seed(host.arc());
        let service = MemoryService::restore_for_boot(host.arc().clone(), "boot-1".into()).unwrap();
        let generation = host.state.memory_generation();
        let request = Request::Acquire {
            minimum_mib: None,
            id: ID.into(),
            unit: UNIT.into(),
            memory_mib: 512.try_into().unwrap(),
        };
        assert!(matches!(
            service.handle(owner(), request.clone()).await.unwrap(),
            Reply::Granted { .. }
        ));
        assert_eq!(host.state.memory_generation(), generation);
        let mut other = owner();
        other.start_ticks += 1;
        assert!(matches!(
            service.handle(other, request).await.unwrap(),
            Reply::Rejected { .. }
        ));
        for (unit, memory_mib) in [(UNIT, 256), ("different.service", 512)] {
            let request = Request::Acquire {
                minimum_mib: None,
                id: ID.into(),
                unit: unit.into(),
                memory_mib: memory_mib.try_into().unwrap(),
            };
            assert!(matches!(
                service.handle(owner(), request).await.unwrap(),
                Reply::Rejected { .. }
            ));
        }
        let duplicate = Request::Acquire {
            minimum_mib: None,
            id: uuid::Uuid::new_v4().to_string(),
            unit: UNIT.into(),
            memory_mib: 512.try_into().unwrap(),
        };
        assert!(matches!(
            service.handle(owner(), duplicate).await.unwrap(),
            Reply::Waiting { .. }
        ));
    }

    #[tokio::test]
    async fn abandoned_work_keeps_its_budget_until_the_unit_has_no_processes_or_job() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let phase = Arc::new(AtomicUsize::new(0));
        let current = phase.clone();
        let mut host = test_host().await;
        Arc::get_mut(&mut host.host).unwrap().commands = mocks::commands_answering(move |_| {
            Ok(crate::ports::CommandResult::with_stdout(
                match current.load(Ordering::SeqCst) {
                    0 => "LoadState=loaded\nActiveState=active\nJob=\nControlGroup=\n",
                    1 => "LoadState=loaded\nActiveState=inactive\nJob=23\nControlGroup=\n",
                    2 => "LoadState=loaded\nActiveState=inactive\n",
                    _ => "LoadState=loaded\nActiveState=inactive\nJob=\nControlGroup=\n",
                },
            ))
        })
        .0;
        seed(host.arc());
        let service = MemoryService::restore_for_boot(host.arc().clone(), "boot-1".into()).unwrap();
        for step in 0..3 {
            phase.store(step, Ordering::SeqCst);
            service.sweep().await.unwrap();
            assert_eq!(service.held.lock().await.leases.len(), 1);
            assert!(matches!(
                service
                    .handle(owner(), Request::Release { id: ID.into() })
                    .await
                    .unwrap(),
                Reply::Waiting { .. }
            ));
        }
        phase.store(3, Ordering::SeqCst);
        service.sweep().await.unwrap();
        assert!(service.held.lock().await.leases.is_empty());
        let persisted: Ledger = serde_json::from_slice(&std::fs::read(&service.path).unwrap()).unwrap();
        assert!(persisted.leases.is_empty());
        assert!(host
            .state
            .reserve_memory(512, &app_id(), protocol::DEFAULT_INSTANCE_RESOURCES)
            .await
            .is_ok());
    }

    #[tokio::test]
    async fn a_failed_release_write_retains_the_lease_and_its_budget() {
        let mut host = test_host().await;
        Arc::get_mut(&mut host.host).unwrap().commands = mocks::commands_answering(|_| {
            Ok(crate::ports::CommandResult::with_stdout(
                "LoadState=loaded\nActiveState=inactive\nJob=\nControlGroup=\n",
            ))
        })
        .0;
        seed(host.arc());
        let service = MemoryService::restore_for_boot(host.arc().clone(), "boot-1".into()).unwrap();
        std::fs::remove_file(&service.path).unwrap();
        std::fs::create_dir(&service.path).unwrap();
        assert!(service
            .handle(owner(), Request::Release { id: ID.into() })
            .await
            .is_err());
        assert_eq!(service.held.lock().await.leases.len(), 1);
        assert!(host
            .state
            .reserve_memory(512, &app_id(), protocol::DEFAULT_INSTANCE_RESOURCES)
            .await
            .is_err());
    }

    #[tokio::test]
    async fn a_corrupt_ledger_never_becomes_free_capacity() {
        let host = test_host().await;
        let path = host.config.state_dir.join("external-memory.json");
        for bytes in [b"".as_slice(), b"not-json", b"{}"] {
            crate::json_store::write_text(&path, std::str::from_utf8(bytes).unwrap(), 0o600).unwrap();
            assert!(MemoryService::restore_for_boot(host.arc().clone(), "boot-1".into()).is_err());
        }
    }

    #[test]
    fn unit_names_and_owner_groups_cannot_escape_into_paths_options_or_shared_slices() {
        assert!(valid_unit(UNIT));
        assert!(valid_unit("mfbuild-0123456789abcdef.slice"));
        for invalid in [
            "-test.service",
            "../test.service",
            "test*.service",
            "test.service\n",
            "test.scope",
            ".service",
        ] {
            assert!(!valid_unit(invalid));
        }
        for invalid in ["/", "/system.slice", "/../escape.service", "relative.service"] {
            assert!(isolated_group(invalid).is_none());
        }
    }

    #[tokio::test]
    async fn an_existing_listener_or_unrelated_file_is_never_unlinked() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("memory.sock");
        std::fs::write(&path, "keep").unwrap();
        assert!(bind(&path).await.is_err());
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "keep");
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
