//! The cgroup the tenant's ceiling is enforced through. What the ceiling is and what thrashing
//! looks like are `crate::ceiling`, which has no Linux in it and is tested everywhere.

use std::ffi::CString;

use crate::ceiling::{ceiling_for, field, Reading};
use crate::guest::mounts::MountFailed;

const CGROUP_ROOT: &str = "/sys/fs/cgroup";
const TENANT_CGROUP: &str = "/sys/fs/cgroup/tenant";

#[derive(Debug, Clone)]
pub(crate) struct Ceiling {
    pub limit_bytes: u64,
    procs_file: CString,
}

impl Ceiling {
    /// The file the tenant writes its own pid into, prepared here because the write happens
    /// between fork and exec, where nothing may allocate.
    pub(crate) fn procs_file(&self) -> &CString {
        &self.procs_file
    }
}

pub(crate) fn mount() -> Result<(), MountFailed> {
    crate::guest::mounts::cgroup2(CGROUP_ROOT)
}

/// The cgroup the tenant will be put in, limited before anything is in it. A kill takes every
/// process in it together: a tenant that forks would otherwise limp on without the one that
/// held its memory.
pub(crate) fn prepare(guest_total_bytes: u64) -> Result<Ceiling, String> {
    // Linux 6.1 resets the boot parameter when the balloon registers. Set the order after
    // device initialization, so 64 KiB free blocks can actually reach the host.
    if let Err(error) = std::fs::write("/sys/module/page_reporting/parameters/page_reporting_order", "4") {
        crate::guest::log(&format!(
            "small free-page reporting could not be enabled: {error}"
        ));
    }
    let limit_bytes = ceiling_for(guest_total_bytes);
    let unwritable = |path: &str, error: std::io::Error| format!("{path} could not be written: {error}");

    let subtree_control = format!("{CGROUP_ROOT}/cgroup.subtree_control");
    std::fs::write(&subtree_control, "+memory").map_err(|error| unwritable(&subtree_control, error))?;
    if let Err(error) = std::fs::create_dir(TENANT_CGROUP) {
        if error.kind() != std::io::ErrorKind::AlreadyExists {
            return Err(format!("{TENANT_CGROUP} could not be made: {error}"));
        }
    }
    for (name, value) in [
        ("memory.max", limit_bytes.to_string()),
        ("memory.swap.max", "0".to_string()),
        ("memory.oom.group", "1".to_string()),
    ] {
        let path = format!("{TENANT_CGROUP}/{name}");
        std::fs::write(&path, value).map_err(|error| unwritable(&path, error))?;
    }
    let procs_file = CString::new(format!("{TENANT_CGROUP}/cgroup.procs"))
        .map_err(|_| "the cgroup path holds a nul byte".to_string())?;
    Ok(Ceiling {
        limit_bytes,
        procs_file,
    })
}

pub(crate) fn adopt() -> Result<Ceiling, String> {
    let filesystem = nix::sys::statfs::statfs(CGROUP_ROOT).map_err(|error| error.to_string())?;
    if filesystem.filesystem_type() != nix::sys::statfs::CGROUP2_SUPER_MAGIC {
        return Err("the runtime did not provide a cgroup v2 hierarchy".into());
    }
    let read_limit = |path: &str| -> Result<u64, String> {
        let value = std::fs::read_to_string(path).map_err(|error| format!("{path}: {error}"))?;
        bounded_limit(&value).ok_or_else(|| format!("{path} is not a positive finite memory limit"))
    };
    let parent = read_limit(&format!("{CGROUP_ROOT}/memory.max"))?;
    let tenant = read_limit(&format!("{TENANT_CGROUP}/memory.max"))?;
    Ok(Ceiling {
        limit_bytes: parent.min(tenant),
        procs_file: CString::new(format!("{TENANT_CGROUP}/cgroup.procs"))
            .map_err(|_| "the cgroup path holds a nul byte".to_string())?,
    })
}

fn bounded_limit(value: &str) -> Option<u64> {
    value.trim().parse::<u64>().ok().filter(|limit| *limit > 0)
}

/// Puts the calling process into the tenant's cgroup. Called by the forked child before it
/// execs, so it uses nothing that allocates: the path was made into a C string before the fork
/// and the pid is formatted on the stack.
pub(crate) fn join(procs_file: &CString) -> nix::Result<()> {
    let pid = nix::unistd::getpid().as_raw();
    let mut digits = [0u8; 20];
    let mut end = digits.len();
    let mut rest = pid.unsigned_abs();
    loop {
        end -= 1;
        digits[end] = b'0' + (rest % 10) as u8;
        rest /= 10;
        if rest == 0 {
            break;
        }
    }
    let fd = nix::fcntl::open(
        procs_file.as_c_str(),
        nix::fcntl::OFlag::O_WRONLY | nix::fcntl::OFlag::O_CLOEXEC,
        nix::sys::stat::Mode::empty(),
    )?;
    let written = nix::unistd::write(&fd, &digits[end..]);
    drop(fd);
    written.map(|_| ())
}

/// Every process in the cgroup, with SIGKILL, at once.
pub(crate) fn kill_everything() -> std::io::Result<()> {
    std::fs::write(format!("{TENANT_CGROUP}/cgroup.kill"), "1")
}

pub(crate) fn read() -> Option<Reading> {
    let value = |name: &str| std::fs::read_to_string(format!("{TENANT_CGROUP}/{name}")).ok();
    Some(Reading {
        current_bytes: value("memory.current")?.trim().parse().ok()?,
        major_faults: field(&value("memory.stat")?, "pgmajfault")?,
        oom_kills: field(&value("memory.events")?, "oom_kill")?,
    })
}

pub(crate) fn reclaim(reclaimer: &mut crate::reclaim::Reclaimer) -> std::io::Result<()> {
    let stat = std::fs::read_to_string(format!("{TENANT_CGROUP}/memory.stat"))?;
    let bytes = field(&stat, "file")
        .unwrap_or(0)
        .saturating_sub(field(&stat, "shmem").unwrap_or(0));
    let bytes = bytes.min(64 * 1024 * 1024);
    if bytes == 0 {
        return Ok(());
    }
    let file = std::fs::OpenOptions::new()
        .write(true)
        .open(format!("{TENANT_CGROUP}/memory.reclaim"))?;
    reclaimer.reclaim(file, bytes, std::time::Duration::from_millis(500))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_prepared_runtime_must_have_finite_kernel_limits() {
        assert_eq!(bounded_limit("104857600\n"), Some(104857600));
        for value in ["max", "0", "-1", "", "104857600M", "18446744073709551616"] {
            assert_eq!(bounded_limit(value), None);
        }
    }
}
