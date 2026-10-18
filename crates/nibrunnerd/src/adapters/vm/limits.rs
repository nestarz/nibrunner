use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

use guest_contract::firecracker::MachineConfig;

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
        // The memory controller charges VMM allocations and KVM page tables alongside guest RAM.
        let memory_max_bytes =
            guest_bytes + VMM_OVERHEAD_MIB * BYTES_PER_MIB + guest_bytes / MEMORY_OVERHEAD_DIVISOR;
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

pub(crate) fn cgroup_path(app_id: &protocol::AppId) -> std::path::PathBuf {
    std::path::Path::new("/sys/fs/cgroup")
        .join(CGROUP_PARENT)
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
            huge_pages: guest_contract::firecracker::HugePages::Transparent,
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
