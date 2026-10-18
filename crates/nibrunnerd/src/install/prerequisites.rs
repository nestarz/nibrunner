//! What a host has to be before anything is laid down on it. Every one of these is something the
//! daemon would otherwise discover at the moment it mattered — at startup, or on the first tenant
//! to boot — and say so from too far away to act on.

use std::path::{Path, PathBuf};

use crate::config::{HostConfig, VolumeBackend};

pub struct Check {
    pub what: String,
    pub met: bool,
    /// What to do about it, said as an instruction rather than as a complaint.
    pub remedy: String,
}

impl Check {
    fn new(what: impl Into<String>, met: bool, remedy: impl Into<String>) -> Self {
        Self {
            what: what.into(),
            met,
            remedy: remedy.into(),
        }
    }
}

/// Every cheap check, in one pass. All of them run even after one fails: a host missing three
/// things should be told three times, not three times over.
pub fn check(config: &HostConfig) -> Vec<Check> {
    let mut checks = vec![Check::new(
        "/dev/kvm",
        Path::new("/dev/kvm").exists(),
        "a machine with hardware virtualisation; a microVM cannot be booted without it",
    )];
    for tool in ["nft", "mke2fs"] {
        checks.push(binary(tool, "install the nftables and e2fsprogs packages"));
    }
    checks.push(binary("useradd", "install the passwd package"));
    checks.push(binary("getent", "install the libc-bin package"));
    // What sets this host's kernel settings, which `install` does rather than asks for.
    checks.push(binary("modprobe", "install the kmod package"));
    checks.push(Check::new(
        TRANSPARENT_HUGEPAGE,
        std::fs::read_to_string(TRANSPARENT_HUGEPAGE).is_ok_and(|setting| huge_pages_allowed(&setting)),
        "set transparent huge pages to madvise or always (echo madvise > /sys/kernel/mm/transparent_hugepage/enabled, and transparent_hugepage=madvise on the kernel command line); guest memory asks for them",
    ));

    match &config.volumes {
        VolumeBackend::LocalFile => {}
        VolumeBackend::Zerofs(_) => {
            for (tool, remedy) in [
                (
                    "nbd-client",
                    "install the nbd-client, e2fsprogs and fuse3 packages",
                ),
                ("debugfs", "install the nbd-client, e2fsprogs and fuse3 packages"),
                (
                    "fusermount3",
                    "install the nbd-client, e2fsprogs and fuse3 packages",
                ),
            ] {
                checks.push(binary(tool, remedy));
            }
        }
    }

    checks
}

const TRANSPARENT_HUGEPAGE: &str = "/sys/kernel/mm/transparent_hugepage/enabled";

/// Firecracker backs guest memory with `madvise(MADV_HUGEPAGE)`, which the kernel ignores only
/// when the setting in force (the bracketed word) is `never`.
fn huge_pages_allowed(setting: &str) -> bool {
    setting
        .split_whitespace()
        .find_map(|word| word.strip_prefix('[')?.strip_suffix(']'))
        .is_some_and(|chosen| chosen != "never")
}

/// Not one of the checks above, because it hashes two files a few hundred megabytes long: asked
/// once, where the answer is kept, rather than on every pass over the list.
pub fn guest_image(config: &HostConfig) -> Result<String, String> {
    verify(&config.guest_image_dir)
}

/// The same check the daemon makes before it boots anything: the manifest read, and both artifacts
/// hashed against what it says they are.
pub fn verify(directory: &Path) -> Result<String, String> {
    crate::adapters::vm::manager::verify_guest_image(directory).map_err(|error| error.message())
}

fn binary(name: &str, remedy: &str) -> Check {
    Check::new(name, on_path(name).is_some(), remedy)
}

pub fn on_path(name: &str) -> Option<PathBuf> {
    on_path_in(&std::env::var_os("PATH")?, name)
}

/// Taking the search path rather than reading it, so a test can ask about a directory it made
/// without setting a variable every other thread in the process is also reading.
fn on_path_in(path: &std::ffi::OsStr, name: &str) -> Option<PathBuf> {
    std::env::split_paths(path)
        .map(|directory| directory.join(name))
        .find(|candidate| is_executable(candidate))
}

#[cfg(unix)]
fn is_executable(path: &Path) -> bool {
    use std::os::unix::fs::PermissionsExt;
    std::fs::metadata(path).is_ok_and(|info| info.is_file() && info.permissions().mode() & 0o111 != 0)
}

#[cfg(not(unix))]
fn is_executable(path: &Path) -> bool {
    path.is_file()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_tool_is_found_by_walking_the_path_it_would_actually_be_run_from() {
        let directory = tempfile::tempdir().unwrap();
        let tool = directory.path().join("nibrunner-test-tool");
        std::fs::write(&tool, "#!/bin/sh\n").unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&tool, std::fs::Permissions::from_mode(0o755)).unwrap();
        }
        let path = directory.path().as_os_str();
        assert_eq!(on_path_in(path, "nibrunner-test-tool"), Some(tool.clone()));
        assert_eq!(on_path_in(path, "nibrunner-test-absent"), None);
    }

    // A file that is on PATH but not executable is not a tool this host can run, and reporting it
    // as present would send an operator looking at the wrong thing.
    #[test]
    fn something_on_the_path_that_cannot_be_run_is_not_a_tool() {
        let directory = tempfile::tempdir().unwrap();
        std::fs::write(directory.path().join("nibrunner-test-data"), "not a program").unwrap();
        assert_eq!(
            on_path_in(directory.path().as_os_str(), "nibrunner-test-data"),
            None
        );
    }

    #[test]
    fn guest_memory_gets_huge_pages_unless_the_host_says_never() {
        assert!(huge_pages_allowed("always [madvise] never\n"));
        assert!(huge_pages_allowed("[always] madvise never\n"));
        assert!(!huge_pages_allowed("always madvise [never]\n"));
        assert!(!huge_pages_allowed(""));
    }

    #[test]
    fn a_local_file_host_is_not_asked_for_the_tools_only_zerofs_needs() {
        let config = HostConfig::under(Path::new("/srv/nibrunner"));
        let named: Vec<String> = check(&config).into_iter().map(|check| check.what).collect();
        for zerofs_only in ["nbd-client", "fusermount3"] {
            assert!(!named.iter().any(|what| what == zerofs_only), "{named:?}");
        }
        assert!(named.iter().any(|what| what == "nft"), "{named:?}");
    }
}
