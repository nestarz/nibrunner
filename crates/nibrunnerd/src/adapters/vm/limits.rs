use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

use guest_contract::firecracker::MachineConfig;
use protocol::{InstanceLimits, InstanceResources, MemoryPriority};

pub(crate) const CGROUP_PARENT: &str = "nibrunner-jailer";
const CPU_PERIOD_US: u64 = 100_000;
const BYTES_PER_MIB: u64 = 1024 * 1024;
const VMM_OVERHEAD_MIB: u64 = 64;
const MEMORY_OVERHEAD_DIVISOR: u64 = 8;

pub(crate) struct VmLimits {
    cpu_quota_us: u64,
    memory_max_bytes: u64,
    snapshot_memory_max_bytes: u64,
}

impl VmLimits {
    pub(crate) fn for_machine(machine: &MachineConfig) -> std::io::Result<Self> {
        if machine.vcpu_count == 0 || machine.mem_size_mib == 0 {
            return Err(std::io::Error::other(
                "a microVM needs positive CPU and memory resources",
            ));
        }
        let guest_bytes = u64::from(machine.mem_size_mib) * BYTES_PER_MIB;
        let memory_max_bytes = memory_max_bytes(machine.mem_size_mib);
        Ok(Self {
            cpu_quota_us: u64::from(machine.vcpu_count) * CPU_PERIOD_US,
            memory_max_bytes,
            snapshot_memory_max_bytes: memory_max_bytes + guest_bytes,
        })
    }

    pub(crate) fn configure(&self, command: &mut tokio::process::Command) {
        command
            .arg("--cgroup")
            .arg(format!("cpu.max={} {CPU_PERIOD_US}", self.cpu_quota_us))
            .arg("--cgroup")
            .arg(format!("memory.max={}", self.memory_max_bytes))
            .arg("--cgroup")
            .arg("memory.swap.max=0");
    }

    pub(crate) fn reserve_snapshot_memory(
        &self,
        cgroup: &Path,
    ) -> std::io::Result<Option<SnapshotMemoryLimit>> {
        let mut limit = File::options()
            .read(true)
            .write(true)
            .open(cgroup.join("memory.max"))?;
        let mut previous = String::new();
        limit.read_to_string(&mut previous)?;
        let previous = previous.trim();
        if previous == "max" {
            return Ok(None);
        }
        let previous = previous.parse::<u64>().map_err(std::io::Error::other)?;
        if previous >= self.snapshot_memory_max_bytes {
            return Ok(None);
        }
        // Firecracker's buffered snapshot writes charge a second copy of guest RAM to its cgroup.
        write_limit(&mut limit, self.snapshot_memory_max_bytes)?;
        Ok(Some(SnapshotMemoryLimit {
            limit,
            previous,
            restored: false,
        }))
    }
}

/// What the memory controller lets a VM of `mem_size_mib` charge: it counts VMM allocations and KVM
/// page tables alongside guest RAM.
pub(crate) fn memory_max_bytes(mem_size_mib: u32) -> u64 {
    let guest_bytes = u64::from(mem_size_mib) * BYTES_PER_MIB;
    guest_bytes + VMM_OVERHEAD_MIB * BYTES_PER_MIB + guest_bytes / MEMORY_OVERHEAD_DIVISOR
}

/// What an instance's `limits` add to the ceilings its resources give its cgroup. With no
/// `limits` every value is the one the jailer already set, so applying them changes nothing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RuntimeControls {
    cpu_quota_us: u64,
    memory_low_bytes: u64,
    memory_high_bytes: Option<u64>,
    swap_max_bytes: u64,
    oom_score_adj: i32,
}

impl RuntimeControls {
    pub(crate) fn for_instance(resources: InstanceResources, limits: Option<&InstanceLimits>) -> Self {
        let cores_us = u64::from(resources.vcpu_count) * CPU_PERIOD_US;
        let cpu_quota_us = limits
            .and_then(|limits| limits.cpu_percent)
            .map_or(cores_us, |percent| {
                (u64::from(percent.get()) * CPU_PERIOD_US / 100).min(cores_us)
            });
        let mut controls = Self {
            cpu_quota_us,
            memory_low_bytes: 0,
            memory_high_bytes: None,
            swap_max_bytes: 0,
            oom_score_adj: 0,
        };
        if let Some(policy) = limits.and_then(|limits| limits.memory) {
            let target = u64::from(policy.target_mib.get().min(resources.memory_mib)) * BYTES_PER_MIB;
            controls.swap_max_bytes = u64::from(policy.swap_mib) * BYTES_PER_MIB;
            match policy.priority {
                MemoryPriority::Production => {
                    controls.memory_low_bytes = target;
                    controls.oom_score_adj = -500;
                }
                MemoryPriority::Standard => {}
                MemoryPriority::Preview => {
                    controls.memory_high_bytes = Some(target);
                    controls.oom_score_adj = 500;
                }
                MemoryPriority::Build => {
                    controls.memory_high_bytes = Some(target);
                    controls.oom_score_adj = 1000;
                }
            }
        }
        controls
    }

    /// Writes the controls into a running VMM's cgroup and its OOM score. Each cgroup file takes its
    /// value whole, so a VMM is never seen with half of a change.
    pub(crate) fn write(&self, cgroup: &Path, pid: i32) -> std::io::Result<()> {
        let write = |name: &str, value: String| std::fs::write(cgroup.join(name), value);
        write("cpu.max", format!("{} {CPU_PERIOD_US}", self.cpu_quota_us))?;
        write("memory.low", self.memory_low_bytes.to_string())?;
        write(
            "memory.high",
            self.memory_high_bytes
                .map_or_else(|| "max".to_string(), |bytes| bytes.to_string()),
        )?;
        write("memory.swap.max", self.swap_max_bytes.to_string())?;
        std::fs::write(
            format!("/proc/{pid}/oom_score_adj"),
            self.oom_score_adj.to_string(),
        )
    }
}

/// Asks the kernel to reclaim up to `bytes` from a cgroup. The write blocks for as long as the
/// kernel works, so it runs off the runtime's threads and is given up on after half a second.
pub(crate) async fn reclaim(cgroup: &Path, bytes: u64) -> std::io::Result<()> {
    let path = cgroup.join("memory.reclaim");
    tokio::time::timeout(
        std::time::Duration::from_millis(500),
        tokio::task::spawn_blocking(move || std::fs::write(path, bytes.to_string())),
    )
    .await
    .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "cgroup reclaim timed out"))?
    .map_err(std::io::Error::other)?
}

/// `memory.high` lifted for as long as a snapshot is being written: the buffered writes are charged
/// to the VMM's cgroup, and a throttled preview or build would take that much longer to sleep.
pub(crate) struct MemoryHighLift {
    high: File,
    previous: String,
}

pub(crate) fn lift_memory_high(cgroup: &Path) -> std::io::Result<Option<MemoryHighLift>> {
    let mut high = match File::options()
        .read(true)
        .write(true)
        .open(cgroup.join("memory.high"))
    {
        Ok(high) => high,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    let mut previous = String::new();
    high.read_to_string(&mut previous)?;
    let previous = previous.trim().to_string();
    if previous == "max" {
        return Ok(None);
    }
    high.seek(SeekFrom::Start(0))?;
    high.write_all(b"max")?;
    Ok(Some(MemoryHighLift { high, previous }))
}

impl Drop for MemoryHighLift {
    fn drop(&mut self) {
        let restored = self
            .high
            .seek(SeekFrom::Start(0))
            .and_then(|_| self.high.write_all(self.previous.as_bytes()));
        if let Err(error) = restored {
            tracing::error!(%error, "the VM's memory.high could not be restored after snapshotting");
        }
    }
}

static POOLED_PARENT: std::sync::OnceLock<String> = std::sync::OnceLock::new();

/// Called once at startup, before any VMM is spawned: a workload pool puts every jail's cgroup
/// under its slice, so that the slice's limits bound them all together.
pub(crate) fn place_under_pool(pool: Option<&crate::config::WorkloadPool>) {
    if let Some(pool) = pool {
        let _ = POOLED_PARENT.set(format!("{}/{CGROUP_PARENT}", pool.slice));
    }
}

/// Where the jailer makes each VMM's cgroup, relative to the unified hierarchy's root.
pub(crate) fn cgroup_parent() -> &'static str {
    POOLED_PARENT.get().map_or(CGROUP_PARENT, String::as_str)
}

/// The cgroup a VMM spawned now is given. One spawned under an earlier configuration is found
/// through its process instead.
pub(crate) fn cgroup_path(app_id: &protocol::AppId) -> std::path::PathBuf {
    std::path::Path::new("/sys/fs/cgroup")
        .join(cgroup_parent())
        .join(super::jailer::jail_id(app_id))
}

#[cfg(target_os = "linux")]
pub(crate) async fn release_stopped_cgroup(app_id: &protocol::AppId) -> std::io::Result<()> {
    let path = cgroup_path(app_id);
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        match std::fs::remove_dir(&path) {
            Ok(()) => return Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(error)
                if error.raw_os_error() == Some(libc::EBUSY) && tokio::time::Instant::now() < deadline =>
            {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
            Err(error) => return Err(error),
        }
    }
}

fn write_limit(limit: &mut File, bytes: u64) -> std::io::Result<()> {
    limit.seek(SeekFrom::Start(0))?;
    // cgroup files interpret each write as a complete value, including a formatting fragment.
    limit.write_all(bytes.to_string().as_bytes())
}

pub(crate) struct SnapshotMemoryLimit {
    limit: File,
    previous: u64,
    restored: bool,
}

impl SnapshotMemoryLimit {
    pub(crate) fn restore(&mut self) -> std::io::Result<()> {
        write_limit(&mut self.limit, self.previous)?;
        self.restored = true;
        Ok(())
    }
}

impl Drop for SnapshotMemoryLimit {
    fn drop(&mut self) {
        if !self.restored {
            if let Err(error) = self.restore() {
                tracing::error!(%error, "the VM's memory ceiling could not be restored after snapshotting");
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn machine(vcpu_count: u32, mem_size_mib: u32) -> MachineConfig {
        MachineConfig {
            vcpu_count,
            mem_size_mib,
            smt: false,
        }
    }

    #[test]
    fn host_limits_follow_guest_resources_with_room_for_vmm_and_kernel_memory() {
        for (cpus, memory, ceiling) in [(1, 128, 208), (2, 256, 352), (32, 4096, 4672)] {
            let limits = VmLimits::for_machine(&machine(cpus, memory)).unwrap();
            assert_eq!(limits.cpu_quota_us, u64::from(cpus) * 100_000);
            assert_eq!(limits.memory_max_bytes, ceiling * BYTES_PER_MIB);
            let mut command = tokio::process::Command::new("jailer");
            limits.configure(&mut command);
            let args: Vec<_> = command
                .as_std()
                .get_args()
                .map(|arg| arg.to_str().unwrap())
                .collect();
            assert_eq!(
                args,
                [
                    "--cgroup",
                    &format!("cpu.max={} 100000", u64::from(cpus) * 100_000),
                    "--cgroup",
                    &format!("memory.max={}", ceiling * BYTES_PER_MIB),
                    "--cgroup",
                    "memory.swap.max=0"
                ]
            );
        }
    }

    fn resources(vcpu_count: u32, memory_mib: u32) -> InstanceResources {
        InstanceResources {
            vcpu_count,
            memory_mib,
        }
    }

    fn limits(cpu_percent: Option<u16>, memory: Option<protocol::MemoryPolicy>) -> InstanceLimits {
        InstanceLimits {
            concurrent: None,
            cpu_percent: cpu_percent.map(|percent| percent.try_into().unwrap()),
            memory,
        }
    }

    #[test]
    fn without_limits_the_controls_are_the_ones_the_jailer_already_set() {
        let controls = RuntimeControls::for_instance(resources(2, 512), None);
        assert_eq!(
            controls,
            RuntimeControls {
                cpu_quota_us: 200_000,
                memory_low_bytes: 0,
                memory_high_bytes: None,
                swap_max_bytes: 0,
                oom_score_adj: 0,
            }
        );
        assert_eq!(
            RuntimeControls::for_instance(resources(2, 512), Some(&InstanceLimits::default())),
            controls
        );
    }

    #[test]
    fn a_cpu_percent_only_ever_tightens_the_cores_the_resources_give() {
        for (vcpus, percent, quota) in [
            (1, 25, 25_000),
            (2, 150, 150_000),
            (1, 400, 100_000),
            (4, 1, 1_000),
        ] {
            assert_eq!(
                RuntimeControls::for_instance(resources(vcpus, 256), Some(&limits(Some(percent), None)))
                    .cpu_quota_us,
                quota,
                "{vcpus} vCPUs at {percent}%"
            );
        }
    }

    #[test]
    fn memory_priorities_protect_production_and_throttle_disposable_work_without_lowering_hard_limits() {
        let policy = |priority| protocol::MemoryPolicy {
            target_mib: 384.try_into().unwrap(),
            swap_mib: 512,
            priority,
        };
        let production = RuntimeControls::for_instance(
            resources(1, 1024),
            Some(&limits(None, Some(policy(MemoryPriority::Production)))),
        );
        assert_eq!(production.memory_low_bytes, 384 * BYTES_PER_MIB);
        assert_eq!(production.memory_high_bytes, None);
        assert_eq!(production.swap_max_bytes, 512 * BYTES_PER_MIB);
        assert_eq!(production.oom_score_adj, -500);
        let standard = RuntimeControls::for_instance(
            resources(1, 1024),
            Some(&limits(None, Some(policy(MemoryPriority::Standard)))),
        );
        assert_eq!(
            (
                standard.memory_low_bytes,
                standard.memory_high_bytes,
                standard.oom_score_adj
            ),
            (0, None, 0)
        );
        for (priority, score) in [(MemoryPriority::Preview, 500), (MemoryPriority::Build, 1000)] {
            let disposable = RuntimeControls::for_instance(
                resources(1, 1024),
                Some(&limits(None, Some(policy(priority)))),
            );
            assert_eq!(disposable.memory_low_bytes, 0);
            assert_eq!(disposable.memory_high_bytes, Some(384 * BYTES_PER_MIB));
            assert_eq!(disposable.oom_score_adj, score);
        }
        let mut larger = policy(MemoryPriority::Build);
        larger.target_mib = 4096.try_into().unwrap();
        assert_eq!(
            RuntimeControls::for_instance(resources(1, 1024), Some(&limits(None, Some(larger))))
                .memory_high_bytes,
            Some(1024 * BYTES_PER_MIB),
            "a target is capped at the guest's memory"
        );
    }

    #[test]
    fn a_snapshot_lifts_memory_high_while_it_is_written_and_puts_it_back() {
        let directory = tempfile::tempdir().unwrap();
        assert!(
            lift_memory_high(directory.path()).unwrap().is_none(),
            "no file, nothing to lift"
        );
        let path = directory.path().join("memory.high");
        std::fs::write(&path, "max\n").unwrap();
        assert!(lift_memory_high(directory.path()).unwrap().is_none());
        std::fs::write(&path, "402653184\n").unwrap();
        let lifted = lift_memory_high(directory.path()).unwrap().unwrap();
        // A cgroup file takes each write whole; a plain file keeps what the write did not cover.
        assert!(std::fs::read_to_string(&path).unwrap().starts_with("max"));
        drop(lifted);
        assert_eq!(std::fs::read_to_string(&path).unwrap().trim(), "402653184");
    }

    #[test]
    fn resource_limits_reject_zero_and_do_not_overflow_at_the_protocols_integer_boundary() {
        assert!(VmLimits::for_machine(&machine(0, 256)).is_err());
        assert!(VmLimits::for_machine(&machine(1, 0)).is_err());
        let limits = VmLimits::for_machine(&machine(u32::MAX, u32::MAX)).unwrap();
        assert_eq!(limits.cpu_quota_us, u64::from(u32::MAX) * CPU_PERIOD_US);
        assert!(limits.memory_max_bytes > u64::from(u32::MAX) * BYTES_PER_MIB);
        assert!(limits.snapshot_memory_max_bytes > limits.memory_max_bytes);
    }

    #[test]
    fn snapshot_headroom_is_one_guest_memory_and_restores_the_previous_ceiling() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("memory.max");
        let limits = VmLimits::for_machine(&machine(1, 128)).unwrap();
        std::fs::write(&path, (208 * BYTES_PER_MIB).to_string()).unwrap();
        let mut headroom = limits.reserve_snapshot_memory(directory.path()).unwrap().unwrap();
        assert_eq!(
            std::fs::read_to_string(&path).unwrap().trim(),
            (336 * BYTES_PER_MIB).to_string()
        );
        headroom.restore().unwrap();
        assert_eq!(
            std::fs::read_to_string(&path).unwrap().trim(),
            (208 * BYTES_PER_MIB).to_string()
        );
    }

    #[test]
    fn abandoning_snapshot_headroom_restores_its_original_cgroup_and_not_a_replacement() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("memory.max");
        let limits = VmLimits::for_machine(&machine(1, 128)).unwrap();
        std::fs::write(&path, (208 * BYTES_PER_MIB).to_string()).unwrap();
        let headroom = limits.reserve_snapshot_memory(directory.path()).unwrap().unwrap();
        let original = directory.path().join("original");
        std::fs::rename(&path, &original).unwrap();
        std::fs::write(&path, "max").unwrap();
        drop(headroom);
        assert_eq!(
            std::fs::read_to_string(original).unwrap().trim(),
            (208 * BYTES_PER_MIB).to_string()
        );
        assert_eq!(std::fs::read_to_string(path).unwrap(), "max");
    }

    #[test]
    fn snapshot_headroom_never_reduces_an_existing_larger_or_unlimited_ceiling() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("memory.max");
        let limits = VmLimits::for_machine(&machine(1, 128)).unwrap();
        for ceiling in ["max".to_string(), (512 * BYTES_PER_MIB).to_string()] {
            std::fs::write(&path, &ceiling).unwrap();
            assert!(limits
                .reserve_snapshot_memory(directory.path())
                .unwrap()
                .is_none());
            assert_eq!(std::fs::read_to_string(&path).unwrap(), ceiling);
        }
    }
}
