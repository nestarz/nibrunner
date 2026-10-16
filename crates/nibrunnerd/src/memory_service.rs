use std::collections::BTreeMap;
use std::io;
use std::num::NonZeroU32;
use std::os::unix::fs::{FileTypeExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use protocol::memory::{Reply, Request, MAX_FRAME_BYTES, SOCKET_NAME};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::Mutex;

use crate::adapters::vm::process::{process_start_ticks, read_host_boot_id};
use crate::host::Host;
use crate::ports::CommandRequest;
use crate::repositories::memory_leases_repository::StoredLease;

const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
const SWEEP_INTERVAL: Duration = Duration::from_secs(15);

#[derive(Debug, Clone, PartialEq, Eq)]
struct Owner {
    pid: i32,
    start_ticks: u64,
    cgroup: String,
    group: Option<crate::adapters::cgroup::MemoryGroup>,
}

impl Owner {
    fn same_process(&self, other: &Self) -> bool {
        self.pid == other.pid
            && self.start_ticks == other.start_ticks
            && self.cgroup == other.cgroup
            && self
                .group
                .as_ref()
                .zip(other.group.as_ref())
                .is_none_or(|(a, b)| a == b)
    }

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
        let unit = Path::new(&cgroup)
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| io::Error::other("the memory lease owner has no cgroup name"))?;
        let group = crate::adapters::cgroup::MemoryGroup::capture(&cgroup, unit)
            .ok_or_else(|| io::Error::other("the memory lease owner's cgroup cannot be identified"))?;
        if process_start_ticks(pid) != Some(start_ticks) || group.replaced() {
            return Err(io::Error::other("the memory lease owner exited"));
        }
        Ok(Self {
            pid,
            start_ticks,
            cgroup,
            group: Some(group),
        })
    }

    fn gone(&self) -> bool {
        process_start_ticks(self.pid) != Some(self.start_ticks)
            && (self.group.as_ref().is_some_and(|group| group.replaced())
                || isolated_group(&self.cgroup).is_some_and(|path| group_empty(&path).unwrap_or(false)))
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

#[derive(Debug, Clone, PartialEq, Eq)]
struct Lease {
    owner: Owner,
    unit: String,
    memory_mib: NonZeroU32,
    range: Option<MemoryRange>,
    group: Option<crate::adapters::cgroup::MemoryGroup>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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

struct Ledger {
    boot_id: String,
    leases: BTreeMap<String, Lease>,
}

pub struct MemoryService {
    host: Arc<Host>,
    held: Mutex<Ledger>,
}

impl Lease {
    fn stored(&self, id: &str, boot_id: &str) -> StoredLease {
        StoredLease {
            id: id.into(),
            boot_id: boot_id.into(),
            unit: self.unit.clone(),
            memory_mib: self.memory_mib.get(),
            range: self
                .range
                .map(|range| (range.minimum.get(), range.preferred.get())),
            owner_pid: self.owner.pid,
            owner_start_ticks: self.owner.start_ticks,
            owner_cgroup: self.owner.cgroup.clone(),
            owner_group: self.owner.group.clone(),
            budget_group: self.group.clone(),
        }
    }

    fn from_stored(stored: StoredLease) -> Option<Self> {
        let range = match stored.range {
            Some((minimum, preferred)) => Some(MemoryRange {
                minimum: NonZeroU32::new(minimum)?,
                preferred: NonZeroU32::new(preferred)?,
            }),
            None => None,
        };
        Some(Self {
            owner: Owner {
                pid: stored.owner_pid,
                start_ticks: stored.owner_start_ticks,
                cgroup: stored.owner_cgroup,
                group: stored.owner_group,
            },
            unit: stored.unit,
            memory_mib: NonZeroU32::new(stored.memory_mib)?,
            range,
            group: stored.budget_group,
        })
    }
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
    pub async fn restore(host: Arc<Host>) -> io::Result<Arc<Self>> {
        Self::restore_for_boot(host, read_host_boot_id()?.trim().to_owned()).await
    }

    async fn restore_for_boot(host: Arc<Host>, boot_id: String) -> io::Result<Arc<Self>> {
        let stored = host
            .repositories
            .memory_leases
            .all()
            .await
            .map_err(|error| io::Error::other(error.message()))?;
        let mut leases = BTreeMap::new();
        // Leases are only good for the boot they were granted in; their owners died with it.
        for stored in stored.into_iter().filter(|stored| stored.boot_id == boot_id) {
            let id = stored.id.clone();
            let lease = Lease::from_stored(stored)
                .ok_or_else(|| io::Error::other("the memory lease ledger is invalid"))?;
            leases.insert(id, lease);
        }
        let ledger = Ledger { boot_id, leases };
        for (id, lease) in &ledger.leases {
            if !valid_id(id)
                || !valid_unit(&lease.unit)
                || lease.owner.pid <= 0
                || isolated_group(&lease.owner.cgroup).is_none()
                || lease.requested().minimum > lease.memory_mib
                || lease.memory_mib > lease.requested().preferred
                || lease
                    .group
                    .as_ref()
                    .is_some_and(|group| !group.belongs_to(&lease.owner.cgroup, &lease.unit))
            {
                return Err(io::Error::other("the memory lease ledger is invalid"));
            }
        }
        for (id, lease) in &ledger.leases {
            host.state.restore_external(id, lease.memory_mib);
            if let Some(group) = &lease.group {
                host.state.track_external_group(id, group.clone());
            }
        }
        Ok(Arc::new(Self {
            host,
            held: Mutex::new(ledger),
        }))
    }

    async fn persist(&self, ledger: &Ledger) -> io::Result<()> {
        let stored: Vec<_> = ledger
            .leases
            .iter()
            .map(|(id, lease)| lease.stored(id, &ledger.boot_id))
            .collect();
        self.host
            .repositories
            .memory_leases
            .replace_all(&stored)
            .await
            .map_err(|error| io::Error::other(error.message()))
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
                    group: crate::adapters::cgroup::MemoryGroup::capture(&owner.cgroup, &unit),
                    owner,
                    unit,
                    memory_mib,
                    range: minimum_mib.map(|minimum| MemoryRange {
                        minimum,
                        preferred: memory_mib,
                    }),
                };
                if self
                    .host
                    .config
                    .memory_admission
                    .as_ref()
                    .and_then(|policy| policy.pool.as_ref())
                    .is_some_and(|pool| {
                        !lease
                            .group
                            .as_ref()
                            .is_some_and(|group| group.within_membership(&pool.membership()))
                    })
                {
                    return Ok(Reply::Rejected {
                        reason: "start the bounded service or slice inside the workload pool before requesting memory".into(),
                    });
                }
                if let Some(existing) = held.leases.get(&id) {
                    return Ok(
                        if existing.owner.same_process(&lease.owner)
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
                let group = lease.group.clone();
                held.leases.insert(id.clone(), lease);
                if let Err(error) = self.persist(&held).await {
                    held.leases.remove(&id);
                    return Err(error);
                }
                reservation.retain();
                if let Some(group) = group {
                    self.host.state.track_external_group(&id, group);
                }
                Ok(Reply::Granted { memory_mib })
            }
            Request::Release { id } => {
                let Some(lease) = held.leases.get(&id) else {
                    return Ok(Reply::Released);
                };
                if !lease.owner.same_process(&owner) {
                    return Ok(Reply::Rejected {
                        reason: "only the lease owner can release it".into(),
                    });
                }
                if !self.finished(&lease.unit).await? {
                    return Ok(Reply::Waiting {
                        reason: "the service still has work or a queued job".into(),
                    });
                }
                self.release(&mut held, &id).await?;
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

    async fn release(&self, held: &mut Ledger, id: &str) -> io::Result<()> {
        let Some(lease) = held.leases.remove(id) else {
            return Ok(());
        };
        if let Err(error) = self.persist(held).await {
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
            .map(|(id, lease)| {
                (
                    id.clone(),
                    lease.unit.clone(),
                    lease.group.as_ref().is_some_and(|group| group.replaced()),
                )
            })
            .collect();
        for (id, unit, replaced) in abandoned {
            if replaced || self.finished(&unit).await? {
                self.release(&mut held, &id).await?;
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
            group: None,
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
                    group: None,
                },
            )]),
        }
    }

    async fn seed(host: &Arc<Host>) {
        let ledger = ledger();
        let stored: Vec<_> = ledger
            .leases
            .iter()
            .map(|(id, lease)| lease.stored(id, &ledger.boot_id))
            .collect();
        host.repositories
            .memory_leases
            .replace_all(&stored)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn a_configured_pool_refuses_external_work_without_a_child_cgroup() {
        let mut host = test_host().await;
        Arc::get_mut(&mut host.host).unwrap().config.memory_admission =
            Some(crate::config::MemoryAdmission {
                mode: crate::config::MemoryAdmissionMode::Observe,
                headroom_mib: 1024.try_into().unwrap(),
                reclaim: true,
                freeze_after_ms: None,
                pool: Some(crate::config::WorkloadPool {
                    slice: "workloads.slice".into(),
                    memory_mib: 4096.try_into().unwrap(),
                }),
            });
        let service = MemoryService::restore_for_boot(host.arc().clone(), "boot-1".into())
            .await
            .unwrap();
        let generation = host.state.memory_generation();
        let reply = service
            .handle(
                owner(),
                Request::Acquire {
                    id: ID.into(),
                    unit: UNIT.into(),
                    memory_mib: 512.try_into().unwrap(),
                    minimum_mib: None,
                },
            )
            .await
            .unwrap();
        assert!(matches!(reply, Reply::Rejected { reason } if reason.contains("inside the workload pool")));
        assert_eq!(host.state.memory_generation(), generation);
        assert!(service.held.lock().await.leases.is_empty());
    }

    #[tokio::test]
    async fn restored_leases_block_apps_before_any_client_reconnects() {
        for (boot, blocked) in [("boot-1", true), ("boot-2", false)] {
            let host = test_host().await;
            seed(host.arc()).await;
            let service = MemoryService::restore_for_boot(host.arc().clone(), boot.into())
                .await
                .unwrap();
            assert_eq!(
                host.state
                    .reserve_memory(
                        512,
                        &app_id(),
                        protocol::DEFAULT_INSTANCE_RESOURCES,
                        crate::domain::memory_admission::MemoryOperation::Start,
                        None
                    )
                    .await
                    .is_err(),
                blocked,
                "{boot}"
            );
            assert_eq!(service.held.lock().await.leases.is_empty(), !blocked);
        }
    }

    #[tokio::test]
    async fn retrying_a_grant_is_idempotent_but_cannot_change_its_owner_unit_or_budget() {
        let host = test_host().await;
        seed(host.arc()).await;
        let service = MemoryService::restore_for_boot(host.arc().clone(), "boot-1".into())
            .await
            .unwrap();
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
        seed(host.arc()).await;
        let service = MemoryService::restore_for_boot(host.arc().clone(), "boot-1".into())
            .await
            .unwrap();
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
        assert!(host.repositories.memory_leases.all().await.unwrap().is_empty());
        assert!(host
            .state
            .reserve_memory(
                512,
                &app_id(),
                protocol::DEFAULT_INSTANCE_RESOURCES,
                crate::domain::memory_admission::MemoryOperation::Start,
                None
            )
            .await
            .is_ok());
    }

    #[tokio::test]
    async fn a_replaced_budget_group_does_not_keep_an_abandoned_lease_alive() {
        let host = test_host().await;
        seed(host.arc()).await;
        let service = MemoryService::restore_for_boot(host.arc().clone(), "boot-1".into())
            .await
            .unwrap();
        let group = serde_json::from_value(serde_json::json!({
            "membership": "/nibrunner-memory-test-absent.scope", "device": 0, "inode": 1
        }))
        .unwrap();
        {
            let mut held = service.held.lock().await;
            held.leases.get_mut(ID).unwrap().group = Some(group);
        }
        service.sweep().await.unwrap();
        assert!(service.held.lock().await.leases.is_empty());
        assert!(host
            .state
            .reserve_memory(
                512,
                &app_id(),
                protocol::DEFAULT_INSTANCE_RESOURCES,
                crate::domain::memory_admission::MemoryOperation::Start,
                None
            )
            .await
            .is_ok());
    }

    #[test]
    fn a_legacy_lease_still_recognizes_its_owner_after_an_upgrade() {
        let previous = owner();
        let mut current = previous.clone();
        current.group = Some(
            serde_json::from_value(serde_json::json!({
                "membership": previous.cgroup, "device": 0, "inode": 1
            }))
            .unwrap(),
        );
        assert!(previous.same_process(&current));
        let mut replaced = current.clone();
        replaced.group = Some(
            serde_json::from_value(serde_json::json!({
                "membership": previous.cgroup, "device": 0, "inode": 2
            }))
            .unwrap(),
        );
        assert!(!current.same_process(&replaced));
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
        seed(host.arc()).await;
        let stored = host.repositories.memory_leases.all().await.unwrap();
        let mut failing = crate::repositories::memory_leases_repository::MockMemoryLeaseRepository::new();
        failing.expect_all().returning(move || Ok(stored.clone()));
        failing
            .expect_replace_all()
            .returning(|_| Err(crate::domain::store::StoreError::Unreadable("disk full".into())));
        Arc::get_mut(&mut host.host).unwrap().repositories.memory_leases = Arc::new(failing);
        let service = MemoryService::restore_for_boot(host.arc().clone(), "boot-1".into())
            .await
            .unwrap();
        assert!(service
            .handle(owner(), Request::Release { id: ID.into() })
            .await
            .is_err());
        assert_eq!(service.held.lock().await.leases.len(), 1);
        assert!(host
            .state
            .reserve_memory(
                512,
                &app_id(),
                protocol::DEFAULT_INSTANCE_RESOURCES,
                crate::domain::memory_admission::MemoryOperation::Start,
                None
            )
            .await
            .is_err());
    }

    #[tokio::test]
    async fn an_invalid_ledger_never_becomes_free_capacity() {
        let host = test_host().await;
        let mut invalid = ledger();
        invalid.leases.get_mut(ID).unwrap().unit = "../escape.service".into();
        let stored: Vec<_> = invalid
            .leases
            .iter()
            .map(|(id, lease)| lease.stored(id, &invalid.boot_id))
            .collect();
        host.repositories
            .memory_leases
            .replace_all(&stored)
            .await
            .unwrap();
        assert!(
            MemoryService::restore_for_boot(host.arc().clone(), "boot-1".into())
                .await
                .is_err()
        );
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
