use std::path::Path;
use std::time::{Duration, Instant};

use nix::mount::{mount, MsFlags};

const TENANT_TMPFS_SIZE: &str = "size=25%";

const RUNTIME_TMPFS_SIZE: &str = "size=1M";

const DEVICE_TIMEOUT: Duration = Duration::from_secs(5);
const DEVICE_POLL: Duration = Duration::from_millis(10);

#[derive(Debug, thiserror::Error)]
#[error("{what} could not be mounted at {target}: {reason}")]
pub(crate) struct MountFailed {
    pub what: &'static str,
    pub target: String,
    pub reason: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Existing {
    Tolerate,
    Refuse,
}

fn mounted(
    what: &'static str,
    source: &str,
    target: &str,
    filesystem: &str,
    flags: MsFlags,
    data: Option<&str>,
    existing: Existing,
) -> Result<(), MountFailed> {
    match mount(Some(source), Path::new(target), Some(filesystem), flags, data) {
        Ok(()) => Ok(()),
        Err(nix::errno::Errno::EBUSY) if existing == Existing::Tolerate => {
            crate::guest::log(&format!("{filesystem} was already mounted on {target}"));
            Ok(())
        }
        Err(error) => Err(MountFailed {
            what,
            target: target.to_string(),
            reason: error.to_string(),
        }),
    }
}

fn ensure_directory(path: &str, mode: u32) -> Result<(), MountFailed> {
    match nix::unistd::mkdir(Path::new(path), nix::sys::stat::Mode::from_bits_truncate(mode)) {
        Ok(()) | Err(nix::errno::Errno::EEXIST) => Ok(()),
        Err(error) => Err(MountFailed {
            what: "a directory",
            target: path.to_string(),
            reason: error.to_string(),
        }),
    }
}

pub(crate) fn dev() -> Result<(), MountFailed> {
    if Path::new("/dev/null").exists() {
        return Ok(());
    }
    mounted(
        "devtmpfs",
        "devtmpfs",
        "/dev",
        "devtmpfs",
        MsFlags::MS_NOSUID | MsFlags::MS_NOEXEC,
        Some("mode=0755"),
        Existing::Tolerate,
    )
}

pub(crate) fn pseudo_filesystems() -> Result<(), MountFailed> {
    let no_privileges = MsFlags::MS_NOSUID | MsFlags::MS_NODEV | MsFlags::MS_NOEXEC;
    let writable = MsFlags::MS_NOSUID | MsFlags::MS_NODEV;
    ensure_directory("/dev/shm", 0o1777)?;

    mounted(
        "proc",
        "proc",
        "/proc",
        "proc",
        no_privileges,
        None,
        Existing::Tolerate,
    )?;
    mounted(
        "sysfs",
        "sysfs",
        "/sys",
        "sysfs",
        no_privileges,
        None,
        Existing::Tolerate,
    )?;
    mounted(
        "a tmpfs",
        "tmpfs",
        "/run",
        "tmpfs",
        writable,
        Some(&format!("mode=0755,{RUNTIME_TMPFS_SIZE}")),
        Existing::Refuse,
    )?;
    mounted(
        "a tmpfs",
        "tmpfs",
        "/tmp",
        "tmpfs",
        writable,
        Some(&format!("mode=1777,{TENANT_TMPFS_SIZE}")),
        Existing::Refuse,
    )?;
    mounted(
        "a tmpfs",
        "tmpfs",
        "/dev/shm",
        "tmpfs",
        writable,
        Some(&format!("mode=1777,{TENANT_TMPFS_SIZE}")),
        Existing::Refuse,
    )
}

/// Where the tenant's ceiling is set. Mounted here, in this runtime's root, and never in the
/// tenant's: what it may spend is not its to change.
pub(crate) fn cgroup2(target: &str) -> Result<(), MountFailed> {
    mounted(
        "the cgroup filesystem",
        "cgroup2",
        target,
        "cgroup2",
        MsFlags::MS_NOSUID | MsFlags::MS_NODEV | MsFlags::MS_NOEXEC,
        None,
        Existing::Tolerate,
    )
}

fn squashfs(what: &'static str, device: &str, target: &str, extra: MsFlags) -> Result<(), MountFailed> {
    wait_for_device(device)?;
    ensure_directory(target, 0o755)?;
    mounted(
        what,
        device,
        target,
        "squashfs",
        MsFlags::MS_RDONLY | MsFlags::MS_NOSUID | MsFlags::MS_NODEV | extra,
        None,
        Existing::Refuse,
    )
}

pub(crate) fn config(device: &str, target: &str) -> Result<(), MountFailed> {
    squashfs("the config drive", device, target, MsFlags::MS_NOEXEC)
}

const SQUASHFS_MAGIC: &[u8; 4] = b"hsqs";
const EXT4_MAGIC_OFFSET: u64 = 0x438;
const EXT4_MAGIC: [u8; 2] = [0x53, 0xEF];

/// A layer is whichever of the two the host accepted it as; the drive says which.
fn filesystem_of(device: &str) -> Result<&'static str, MountFailed> {
    use std::io::{Read, Seek, SeekFrom};
    let unreadable = |reason: String| MountFailed {
        what: "a layer drive",
        target: device.to_string(),
        reason,
    };
    let mut file = std::fs::File::open(device).map_err(|error| unreadable(error.to_string()))?;
    let mut head = [0u8; 4];
    file.read_exact(&mut head)
        .map_err(|error| unreadable(error.to_string()))?;
    if &head == SQUASHFS_MAGIC {
        return Ok("squashfs");
    }
    let mut magic = [0u8; 2];
    file.seek(SeekFrom::Start(EXT4_MAGIC_OFFSET))
        .and_then(|_| file.read_exact(&mut magic))
        .map_err(|error| unreadable(error.to_string()))?;
    if magic == EXT4_MAGIC {
        return Ok("ext4");
    }
    Err(unreadable(
        "it is neither a squashfs nor an ext4 image".to_string(),
    ))
}

pub(crate) fn layer(device: &str, target: &str) -> Result<(), MountFailed> {
    wait_for_device(device)?;
    let filesystem = filesystem_of(device)?;
    mounted(
        "a layer drive",
        device,
        target,
        filesystem,
        MsFlags::MS_RDONLY | MsFlags::MS_NOSUID | MsFlags::MS_NODEV,
        None,
        Existing::Refuse,
    )
}

/// This image, bound without its submounts, so the stack's bottom is the Debian the guest is
/// and never the /proc, /run or /mnt the guest put over it.
pub(crate) fn base(target: &str) -> Result<(), MountFailed> {
    mount(
        Some("/"),
        Path::new(target),
        None::<&str>,
        MsFlags::MS_BIND | MsFlags::MS_RDONLY,
        None::<&str>,
    )
    .map_err(|error| MountFailed {
        what: "the guest's own root",
        target: target.to_string(),
        reason: error.to_string(),
    })
}

pub(crate) fn volume(device: &str, target: &str) -> Result<(), MountFailed> {
    wait_for_device(device)?;
    mounted(
        "the volume",
        device,
        target,
        "ext4",
        MsFlags::MS_NOSUID | MsFlags::MS_NODEV | MsFlags::MS_NOATIME,
        None,
        Existing::Refuse,
    )
}

fn memory_scratch_options(mib: u32) -> String {
    format!("mode=0755,size={mib}m")
}

/// The writable root of a guest with no volume. A tmpfs, so it costs the guest's memory, goes with
/// the guest, and is kept through a sleep only because the snapshot keeps that memory.
pub(crate) fn memory_scratch(mib: u32, target: &str) -> Result<(), MountFailed> {
    mounted(
        "the scratch",
        "tmpfs",
        target,
        "tmpfs",
        MsFlags::MS_NOSUID | MsFlags::MS_NODEV | MsFlags::MS_NOATIME,
        Some(&memory_scratch_options(mib)),
        Existing::Refuse,
    )
}

/// `lowers` bottom first, the way the document lists layers; overlayfs wants the top first.
pub(crate) fn overlay(lowers: &[String], upper: &str, work: &str, target: &str) -> Result<(), MountFailed> {
    ensure_directory(upper, 0o755)?;
    ensure_directory(work, 0o755)?;
    let mut lowerdir: Vec<&str> = lowers.iter().map(String::as_str).collect();
    lowerdir.reverse();
    let options = format!("lowerdir={},upperdir={upper},workdir={work}", lowerdir.join(":"));
    mounted(
        "the stacked root",
        "overlay",
        target,
        "overlay",
        MsFlags::empty(),
        Some(&options),
        Existing::Refuse,
    )
}

/// What a program expects to find under its root that no layer carries: the devices, the
/// kernel's views of itself, and somewhere to write that is not the volume.
pub(crate) fn into_root(root: &str) -> Result<(), MountFailed> {
    let no_privileges = MsFlags::MS_NOSUID | MsFlags::MS_NODEV | MsFlags::MS_NOEXEC;
    let writable = MsFlags::MS_NOSUID | MsFlags::MS_NODEV;
    for directory in ["dev", "proc", "sys", "run", "tmp"] {
        ensure_directory(&format!("{root}/{directory}"), 0o755)?;
    }
    mount(
        Some("/dev"),
        Path::new(&format!("{root}/dev")),
        None::<&str>,
        MsFlags::MS_BIND | MsFlags::MS_REC,
        None::<&str>,
    )
    .map_err(|error| MountFailed {
        what: "the devices",
        target: format!("{root}/dev"),
        reason: error.to_string(),
    })?;
    mounted(
        "proc",
        "proc",
        &format!("{root}/proc"),
        "proc",
        no_privileges,
        None,
        Existing::Refuse,
    )?;
    mounted(
        "sysfs",
        "sysfs",
        &format!("{root}/sys"),
        "sysfs",
        no_privileges | MsFlags::MS_RDONLY,
        None,
        Existing::Refuse,
    )?;
    mounted(
        "a tmpfs",
        "tmpfs",
        &format!("{root}/run"),
        "tmpfs",
        writable,
        Some(&format!("mode=0755,{RUNTIME_TMPFS_SIZE}")),
        Existing::Refuse,
    )?;
    mounted(
        "a tmpfs",
        "tmpfs",
        &format!("{root}/tmp"),
        "tmpfs",
        writable,
        Some(&format!("mode=1777,{TENANT_TMPFS_SIZE}")),
        Existing::Refuse,
    )
}

/// The program's working directory, made where it is not and given to the uid that runs there.
pub(crate) fn working_directory(path: &str, uid: u32, gid: u32) -> Result<(), MountFailed> {
    std::fs::create_dir_all(path).map_err(|error| MountFailed {
        what: "the working directory",
        target: path.to_string(),
        reason: error.to_string(),
    })?;
    nix::unistd::chown(
        Path::new(path),
        Some(nix::unistd::Uid::from_raw(uid)),
        Some(nix::unistd::Gid::from_raw(gid)),
    )
    .map_err(|error| MountFailed {
        what: "the working directory",
        target: path.to_string(),
        reason: format!("it could not be given to uid {uid}: {error}"),
    })
}

fn wait_for_device(path: &str) -> Result<(), MountFailed> {
    let started = Instant::now();
    let deadline = started + DEVICE_TIMEOUT;
    while Instant::now() < deadline {
        if Path::new(path).exists() {
            if started.elapsed() > DEVICE_POLL {
                crate::guest::log(&format!(
                    "{path} appeared after {}ms",
                    started.elapsed().as_millis()
                ));
            }
            return Ok(());
        }
        std::thread::sleep(DEVICE_POLL);
    }
    Err(MountFailed {
        what: "a drive",
        target: path.to_string(),
        reason: "it never appeared, so it was never attached".to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_memory_scratch_is_bounded_by_the_mib_the_document_gave_it() {
        assert_eq!(memory_scratch_options(512), "mode=0755,size=512m");
    }
}
