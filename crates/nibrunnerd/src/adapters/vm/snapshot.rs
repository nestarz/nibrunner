use std::io::Read;
use std::path::{Path, PathBuf};

use protocol::{AppId, DeploymentId};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::domain::report::capacity::FilesystemSpace;
use crate::json_store::{make_directory, read_json, write_json};
use crate::ports::VmError;

use super::process::FIRECRACKER_VERSION;

pub const SNAPSHOT_STATE_FILENAME: &str = "vmstate";
pub const SNAPSHOT_MEMORY_FILENAME: &str = "memory";

pub const SNAPSHOT_STAMP_FILENAME: &str = "stamp.json";

pub(crate) const EXCHANGE_BOOT_ID_FILENAME: &str = "host-boot-id";

pub(crate) fn exchange(snapshot_dir: &Path, app_id: &AppId) -> PathBuf {
    snapshot_dir.join(".jailer").join(app_id.as_str())
}

pub(crate) fn publish_snapshot_file(source: &Path, destination: &Path) -> std::io::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    std::fs::rename(source, destination)?;
    if !std::fs::symlink_metadata(destination)?.is_file() {
        let _ = std::fs::remove_file(destination);
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "the VMM did not produce a regular snapshot file",
        ));
    }
    std::os::unix::fs::chown(destination, Some(0), Some(0))?;
    std::fs::set_permissions(destination, std::fs::Permissions::from_mode(0o400))
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SnapshotStamp {
    pub deployment_id: DeploymentId,
    pub guest_image_version: String,
    pub host_boot_id: String,
    pub slot: u32,
    /// The Firecracker that took it. A stamp an earlier daemon wrote does not say, and is
    /// restored only on the boot it was taken.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub firecracker_version: Option<String>,
    #[serde(flatten)]
    pub host: SnapshotHost,
}

/// What of the machine a snapshot can only be restored on the same of, read once a run: KVM
/// restores a guest's state only under the kernel it was saved from, and a guest given no CPU
/// template was handed the host CPU's own features, which a microcode update can take away.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SnapshotHost {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub host_kernel: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cpu_template: Option<String>,
}

// nibrunner sets no Firecracker CPU template, so the guest sees the host's CPUID: the template is
// named for that, with the host CPU's identity and features after it.
const NO_CPU_TEMPLATE: &str = "none";

fn kernel_from(release: &str, version: &str) -> String {
    format!("{} {}", release.trim(), version.trim())
}

fn cpu_template_from(cpuinfo: &str) -> Option<String> {
    use sha2::Digest;
    let first_processor = cpuinfo.split("\n\n").next()?;
    let identity: Vec<&str> = first_processor
        .lines()
        .filter_map(|line| line.split_once(':'))
        .filter(|(key, _)| {
            matches!(
                key.trim(),
                "vendor_id" | "cpu family" | "model" | "stepping" | "flags"
            )
        })
        .map(|(_, value)| value.trim())
        .collect();
    if identity.is_empty() {
        return None;
    }
    let digest = sha2::Sha256::digest(identity.join("\n").as_bytes());
    Some(format!("{NO_CPU_TEMPLATE}/{}", hex::encode(&digest[..8])))
}

impl SnapshotHost {
    pub fn this_one() -> &'static Self {
        static THIS_HOST: std::sync::OnceLock<SnapshotHost> = std::sync::OnceLock::new();
        THIS_HOST.get_or_init(|| {
            let read = |path: &str| std::fs::read_to_string(path).ok();
            Self {
                host_kernel: read("/proc/sys/kernel/osrelease")
                    .zip(read("/proc/sys/kernel/version"))
                    .map(|(release, version)| kernel_from(&release, &version)),
                cpu_template: read("/proc/cpuinfo").as_deref().and_then(cpu_template_from),
            }
        })
    }
}

/// Why a snapshot taken on an earlier boot cannot be restored on this one, if it cannot. One
/// taken this boot was taken on this kernel and CPU, whatever its stamp has room for; one from a
/// boot before is restored only onto the same of both, and a stamp that does not say is not.
fn host_drift(stored: &SnapshotStamp, boot_id: &str, host: &SnapshotHost) -> Option<&'static str> {
    if stored.host_boot_id == boot_id {
        return None;
    }
    if stored.host.host_kernel.is_none() || stored.host.host_kernel != host.host_kernel {
        return Some("the host has rebooted into another kernel");
    }
    if stored.host.cpu_template.is_none() || stored.host.cpu_template != host.cpu_template {
        return Some("the host has rebooted onto another CPU");
    }
    None
}

fn drift_reason(stored: &SnapshotStamp, expected: &SnapshotStamp) -> Option<&'static str> {
    if stored.deployment_id != expected.deployment_id {
        return Some("the app has been deployed again since");
    }
    if stored.guest_image_version != expected.guest_image_version {
        return Some("the guest image has changed");
    }
    if let Some(reason) = host_drift(stored, &expected.host_boot_id, &expected.host) {
        return Some(reason);
    }
    if stored.firecracker_version != expected.firecracker_version
        && (stored.firecracker_version.is_some() || stored.host_boot_id != expected.host_boot_id)
    {
        return Some("the snapshot was taken by another Firecracker");
    }
    if stored.slot != expected.slot {
        return Some("the app has moved to another slot");
    }
    None
}

pub fn drift_from(stored: &SnapshotStamp, expected: &SnapshotStamp) -> Option<String> {
    drift_reason(stored, expected).map(str::to_string)
}

pub fn refusal_to_sleep(subject: Option<SleepSubject>) -> Option<&'static str> {
    let Some(subject) = subject else {
        return Some("this host holds no record of it");
    };
    if subject.stop_requested || !subject.desired_running {
        return Some("it has already been asked to stop");
    }
    if !subject.ever_healthy {
        return Some("it has never answered, so it may not have finished booting");
    }
    None
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SleepSubject {
    pub stop_requested: bool,
    pub desired_running: bool,
    pub ever_healthy: bool,
}

const DISK_RESERVE_GIB: u64 = 8;

const BYTES_PER_MIB: u64 = 1_048_576;
const BYTES_PER_GIB: u64 = 1_073_741_824;
const DISK_RESERVE_BYTES: u64 = DISK_RESERVE_GIB * BYTES_PER_GIB;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SnapshotDisk {
    pub total_bytes: u64,
    pub available_bytes: u64,
    pub cache_bytes: u64,
    pub snapshot_bytes: u64,
}

pub fn snapshot_bytes_for(memory_mib: u32) -> u64 {
    u64::from(memory_mib) * BYTES_PER_MIB
}

pub fn snapshot_budget(disk: &SnapshotDisk) -> u64 {
    disk.total_bytes
        .saturating_sub(disk.cache_bytes)
        .saturating_sub(DISK_RESERVE_BYTES)
}

fn gibibytes(bytes: u64) -> String {
    format!("{:.1} GiB", bytes as f64 / BYTES_PER_GIB as f64)
}

pub fn refusal_for_disk(disk: &SnapshotDisk, wanted_bytes: u64) -> Option<String> {
    let budget = snapshot_budget(disk);
    if disk.snapshot_bytes + wanted_bytes > budget {
        return Some(format!(
            "snapshots on this host may hold {} and already hold {}",
            gibibytes(budget),
            gibibytes(disk.snapshot_bytes)
        ));
    }
    if disk.available_bytes.saturating_sub(wanted_bytes) < DISK_RESERVE_BYTES {
        return Some(format!(
            "the disk it would be written to has {} left, which the filesystem every app runs from needs more than it does",
            gibibytes(disk.available_bytes)
        ));
    }
    None
}

/// What the snapshots in flight are going to put on the disk. A sleep is admitted against the
/// disk as measured less what is already spoken for, and holds its own share until its snapshot
/// is on the disk or has failed, so that four admitted together are each measured against the
/// room the others will have taken rather than all four against the room they all saw.
#[derive(Debug, Default)]
pub struct SnapshotsInFlight {
    bytes: std::sync::Mutex<u64>,
}

impl SnapshotsInFlight {
    fn held(&self) -> std::sync::MutexGuard<'_, u64> {
        self.bytes.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    pub fn bytes(&self) -> u64 {
        *self.held()
    }

    pub fn admit(&self, disk: &SnapshotDisk, wanted_bytes: u64) -> Result<Reserved<'_>, String> {
        let mut held = self.held();
        let spoken_for = SnapshotDisk {
            available_bytes: disk.available_bytes.saturating_sub(*held),
            snapshot_bytes: disk.snapshot_bytes.saturating_add(*held),
            ..*disk
        };
        if let Some(refusal) = refusal_for_disk(&spoken_for, wanted_bytes) {
            return Err(refusal);
        }
        *held += wanted_bytes;
        Ok(Reserved {
            of: self,
            bytes: wanted_bytes,
        })
    }
}

/// One admitted snapshot's share of the disk, given back when this is dropped.
#[derive(Debug)]
#[must_use = "the share is given back the moment this is dropped"]
pub struct Reserved<'a> {
    of: &'a SnapshotsInFlight,
    bytes: u64,
}

impl Drop for Reserved<'_> {
    fn drop(&mut self) {
        let mut held = self.of.held();
        *held = held.saturating_sub(self.bytes);
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotPaths {
    pub directory: PathBuf,
    pub state_path: PathBuf,
    pub memory_path: PathBuf,
    pub stamp_path: PathBuf,
}

pub fn snapshot_paths(snapshot_dir: &Path, app_id: &AppId) -> SnapshotPaths {
    paths_in(snapshot_dir.join(app_id.as_str()))
}

fn paths_in(directory: PathBuf) -> SnapshotPaths {
    SnapshotPaths {
        state_path: directory.join(SNAPSHOT_STATE_FILENAME),
        memory_path: directory.join(SNAPSHOT_MEMORY_FILENAME),
        stamp_path: directory.join(SNAPSHOT_STAMP_FILENAME),
        directory,
    }
}

pub fn read_snapshot_bytes(snapshot_dir: &Path) -> u64 {
    fn walk(directory: &Path, held: &mut u64) {
        let Ok(entries) = std::fs::read_dir(directory) else {
            return;
        };
        for entry in entries.filter_map(Result::ok) {
            match entry.metadata() {
                Ok(info) if info.is_file() => *held += info.len(),
                Ok(info) if info.is_dir() => walk(&entry.path(), held),
                _ => {}
            }
        }
    }
    let mut held = 0;
    walk(snapshot_dir, &mut held);
    held
}

/// What reaping the snapshots of an earlier boot freed.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Reaped {
    pub snapshots: usize,
    pub bytes: u64,
}

/// Removes every snapshot under `snapshot_dir` this boot can never restore: one with no stamp
/// this host can read, and one from an earlier boot taken under another Firecracker, kernel or
/// CPU. A wake would refuse each by name, and each holds the memory its app was promised, so left
/// alone they are the disk a reboot leaves behind, for good. One a reboot left restorable is kept,
/// so that a nightly reboot does not cold-boot every app asleep. Whatever else a kept stamp says
/// — another deployment, another slot — is the wake's to judge, and to say why. Unpublished
/// staging directories are removed even during the same host boot.
pub fn reap_stale_snapshots(snapshot_dir: &Path, boot_id: &str, host: &SnapshotHost) -> Reaped {
    let mut reaped = Reaped::default();
    let Ok(entries) = std::fs::read_dir(snapshot_dir) else {
        return reaped;
    };
    for entry in entries.filter_map(Result::ok) {
        if !entry.file_type().is_ok_and(|kind| kind.is_dir()) {
            continue;
        }
        let directory = entry.path();
        if entry.file_name() == ".jailer"
            && crate::json_store::read_text(&directory.join(EXCHANGE_BOOT_ID_FILENAME))
                .ok()
                .flatten()
                .is_some_and(|stored| stored == boot_id)
        {
            continue;
        }
        let stamp: Option<SnapshotStamp> = read_json(&directory.join(SNAPSHOT_STAMP_FILENAME)).ok().flatten();
        let restorable = |stamp: &SnapshotStamp| {
            stamp.host_boot_id == boot_id
                || (host_drift(stamp, boot_id, host).is_none()
                    && stamp.firecracker_version.as_deref() == Some(FIRECRACKER_VERSION))
        };
        if !entry.file_name().to_string_lossy().starts_with(STAGING_PREFIX)
            && stamp.as_ref().is_some_and(restorable)
        {
            continue;
        }
        let bytes = read_snapshot_bytes(&directory);
        match std::fs::remove_dir_all(&directory) {
            Ok(()) => {
                reaped.snapshots += 1;
                reaped.bytes += bytes;
            }
            Err(error) => {
                tracing::warn!(path = %directory.display(), %error, "a snapshot this boot cannot restore could not be removed");
            }
        }
    }
    reaped
}

pub fn measure_snapshot_disk(snapshot_dir: &Path, cache_bytes: u64) -> std::io::Result<SnapshotDisk> {
    crate::json_store::make_directory(snapshot_dir, 0o700)?;
    measure_disk_under(snapshot_dir, cache_bytes)
}

/// The disk the snapshots land on, whether or not there is a directory for them yet: one that is
/// not there is measured at its nearest ancestor that is, which is the filesystem it would be
/// made on. Nothing is made, so this is what `install` may size a host against before it has
/// laid anything down.
pub fn measure_disk_under(snapshot_dir: &Path, cache_bytes: u64) -> std::io::Result<SnapshotDisk> {
    let landing = snapshot_dir
        .ancestors()
        .find(|ancestor| ancestor.exists())
        .ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("nothing of {} is there", snapshot_dir.display()),
            )
        })?;
    let FilesystemSpace {
        total_bytes,
        available_bytes,
    } = crate::domain::report::capacity::read_filesystem_space(landing)?;
    Ok(SnapshotDisk {
        total_bytes,
        available_bytes,
        cache_bytes,
        snapshot_bytes: read_snapshot_bytes(snapshot_dir),
    })
}

const STAGING_PREFIX: &str = ".pending-";
const HASH_BUFFER_BYTES: usize = 65_536;

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SnapshotFile {
    bytes: u64,
    sha256: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct SnapshotManifest {
    #[serde(flatten)]
    stamp: SnapshotStamp,
    state: SnapshotFile,
    memory: SnapshotFile,
}

fn hash_file(file: &mut std::fs::File) -> std::io::Result<SnapshotFile> {
    let mut digest = Sha256::new();
    let mut buffer = [0; HASH_BUFFER_BYTES];
    let mut bytes = 0;
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
        bytes += read as u64;
    }
    if bytes == 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "the file is empty",
        ));
    }
    Ok(SnapshotFile {
        bytes,
        sha256: hex::encode(digest.finalize()),
    })
}

// The state and memory files are created without an fsync (`sync_snapshot_files: false`) and only
// hashed here: a file a host crash leaves torn fails `verify_file` on wake and the unit boots
// afresh. The stamp and the directories are synced, so a published snapshot is never half-named.
fn seal_file(path: &Path) -> Result<SnapshotFile, VmError> {
    let failed =
        |error: std::io::Error| VmError::Host(format!("{} could not be committed: {error}", path.display()));
    let mut file = std::fs::File::open(path).map_err(failed)?;
    hash_file(&mut file).map_err(failed)
}

fn sync_directory(path: &Path) -> Result<(), VmError> {
    std::fs::File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| VmError::Host(error.to_string()))
}

pub(crate) struct StagedSnapshot {
    pub(crate) paths: SnapshotPaths,
}

impl StagedSnapshot {
    pub(crate) fn create(snapshot_dir: &Path) -> Result<Self, VmError> {
        let paths = paths_in(snapshot_dir.join(format!("{STAGING_PREFIX}{}", uuid::Uuid::new_v4())));
        make_directory(&paths.directory, 0o700).map_err(|error| VmError::Host(error.to_string()))?;
        Ok(Self { paths })
    }

    pub(crate) fn publish(self, published: &SnapshotPaths, stamp: SnapshotStamp) -> Result<u64, VmError> {
        let manifest = SnapshotManifest {
            stamp,
            state: seal_file(&self.paths.state_path)?,
            memory: seal_file(&self.paths.memory_path)?,
        };
        write_json(&self.paths.stamp_path, &manifest).map_err(|error| VmError::Host(error.message()))?;
        std::fs::File::open(&self.paths.stamp_path)
            .and_then(|stamp| stamp.sync_all())
            .map_err(|error| VmError::Host(error.to_string()))?;
        sync_directory(&self.paths.directory)?;
        std::fs::rename(&self.paths.directory, &published.directory)
            .map_err(|error| VmError::Host(error.to_string()))?;
        sync_directory(
            published
                .directory
                .parent()
                .expect("a snapshot has a parent directory"),
        )?;
        Ok(manifest.memory.bytes)
    }
}

impl Drop for StagedSnapshot {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.paths.directory);
    }
}

fn verify_file(path: &Path, expected: &SnapshotFile) -> Result<(), VmError> {
    let verify = || -> std::io::Result<bool> {
        let mut file = std::fs::File::open(path)?;
        if file.metadata()?.len() != expected.bytes {
            return Ok(false);
        }
        Ok(hash_file(&mut file)? == *expected)
    };
    let reason = match verify() {
        Ok(true) => return Ok(()),
        Ok(false) => format!(
            "{} no longer matches its recorded size or SHA-256",
            path.display()
        ),
        Err(error) => format!("{} could not be verified: {error}", path.display()),
    };
    Err(VmError::SnapshotUnusable { reason })
}

pub fn ensure_loadable(paths: &SnapshotPaths, expected: &SnapshotStamp) -> Result<(), VmError> {
    let stored: Option<SnapshotStamp> = read_json(&paths.stamp_path).ok().flatten();
    let Some(stored) = stored else {
        return Err(VmError::SnapshotUnusable {
            reason: "this host kept none".into(),
        });
    };
    if let Some(reason) = drift_from(&stored, expected) {
        return Err(VmError::SnapshotUnusable { reason });
    }
    let manifest: SnapshotManifest =
        read_json(&paths.stamp_path)
            .ok()
            .flatten()
            .ok_or_else(|| VmError::SnapshotUnusable {
                reason: "its stamp has no readable integrity record".into(),
            })?;
    verify_file(&paths.state_path, &manifest.state)?;
    verify_file(&paths.memory_path, &manifest.memory)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::json_store::write_json;
    use crate::test_support::{app_id, deployment_id};

    fn stamp() -> SnapshotStamp {
        SnapshotStamp {
            firecracker_version: Some(FIRECRACKER_VERSION.to_string()),
            deployment_id: deployment_id(),
            guest_image_version: "6.1.180-98db6df338f0".into(),
            host_boot_id: "b6b8f0d2-0000-4000-8000-000000000001".into(),
            slot: 7,
            host: this_host(),
        }
    }

    fn this_host() -> SnapshotHost {
        SnapshotHost {
            host_kernel: Some("6.8.0-90-generic #91-Ubuntu SMP".into()),
            cpu_template: Some("none/0123456789abcdef".into()),
        }
    }

    #[test]
    fn a_snapshot_is_three_files_under_the_app_it_belongs_to() {
        let paths = snapshot_paths(Path::new("/data/snapshots"), &app_id());
        assert_eq!(paths.directory, Path::new("/data/snapshots/app-1"));
        assert_eq!(paths.stamp_path, paths.directory.join(SNAPSHOT_STAMP_FILENAME));
        assert!(paths.state_path.starts_with(&paths.directory));
        assert!(paths.memory_path.starts_with(&paths.directory));
    }

    #[test]
    fn every_way_a_snapshot_stops_being_loadable_is_named() {
        assert_eq!(drift_from(&stamp(), &stamp()), None);
        let redeployed = SnapshotStamp {
            deployment_id: DeploymentId::parse("dep-2").unwrap(),
            ..stamp()
        };
        assert!(drift_from(&redeployed, &stamp())
            .unwrap()
            .contains("deployed again"));
        let newer_image = SnapshotStamp {
            guest_image_version: "6.1.181-x".into(),
            ..stamp()
        };
        assert!(drift_from(&newer_image, &stamp())
            .unwrap()
            .contains("guest image"));
        let rebooted = SnapshotStamp {
            host_boot_id: "another".into(),
            ..stamp()
        };
        assert_eq!(
            drift_from(&rebooted, &stamp()),
            None,
            "the same kernel on the same CPU"
        );
        let patched = SnapshotStamp {
            host: SnapshotHost {
                host_kernel: Some("6.8.0-91-generic #92-Ubuntu SMP".into()),
                ..this_host()
            },
            ..rebooted.clone()
        };
        assert!(drift_from(&patched, &stamp()).unwrap().contains("another kernel"));
        let microcode = SnapshotStamp {
            host: SnapshotHost {
                cpu_template: Some("none/fedcba9876543210".into()),
                ..this_host()
            },
            ..rebooted
        };
        assert!(drift_from(&microcode, &stamp()).unwrap().contains("another CPU"));
        let moved = SnapshotStamp { slot: 8, ..stamp() };
        assert!(drift_from(&moved, &stamp()).unwrap().contains("another slot"));
    }

    #[test]
    fn the_moments_a_microvm_must_not_be_snapshotted() {
        let sleepable = SleepSubject {
            stop_requested: false,
            desired_running: true,
            ever_healthy: true,
        };
        assert_eq!(refusal_to_sleep(Some(sleepable)), None);
        assert!(refusal_to_sleep(Some(SleepSubject {
            stop_requested: true,
            ..sleepable
        }))
        .unwrap()
        .contains("asked to stop"));
        assert!(refusal_to_sleep(Some(SleepSubject {
            desired_running: false,
            ..sleepable
        }))
        .unwrap()
        .contains("asked to stop"));
        assert!(refusal_to_sleep(Some(SleepSubject {
            ever_healthy: false,
            ..sleepable
        }))
        .unwrap()
        .contains("finished booting"));
        assert!(refusal_to_sleep(None).is_some());
    }

    const GIB: u64 = 1_073_741_824;

    fn host_disk() -> SnapshotDisk {
        SnapshotDisk {
            total_bytes: 110 * GIB,
            available_bytes: 38 * GIB,
            cache_bytes: 70 * GIB,
            snapshot_bytes: 0,
        }
    }

    #[test]
    fn what_snapshots_may_hold_on_a_host() {
        let asleep = u64::from(crate::config::HostConfig::example().max_apps) * snapshot_bytes_for(256);
        let roomy = SnapshotDisk {
            total_bytes: 512 * GIB,
            ..host_disk()
        };
        assert!(asleep < snapshot_budget(&roomy));
        assert_eq!(
            refusal_for_disk(
                &SnapshotDisk {
                    snapshot_bytes: asleep,
                    ..roomy
                },
                snapshot_bytes_for(256)
            ),
            None
        );
        assert!(refusal_for_disk(
            &SnapshotDisk {
                snapshot_bytes: 30 * GIB,
                ..host_disk()
            },
            snapshot_bytes_for(4096)
        )
        .unwrap()
        .contains("already hold"));
        let cold_cache = SnapshotDisk {
            available_bytes: 105 * GIB,
            snapshot_bytes: 30 * GIB,
            ..host_disk()
        };
        assert!(refusal_for_disk(&cold_cache, snapshot_bytes_for(4096)).is_some());
        let crowded = SnapshotDisk {
            available_bytes: 8 * GIB,
            snapshot_bytes: GIB,
            ..host_disk()
        };
        assert!(refusal_for_disk(&crowded, snapshot_bytes_for(256))
            .unwrap()
            .contains("every app"));
        assert_eq!(
            snapshot_budget(&SnapshotDisk {
                total_bytes: 0,
                ..host_disk()
            }),
            0
        );
    }

    #[test]
    fn what_snapshots_hold_is_measured_from_the_directory_they_are_in() {
        let directory = tempfile::tempdir().unwrap();
        for (app, size) in [("inst-1", 1024), ("inst-2", 512)] {
            let held = directory.path().join(app);
            std::fs::create_dir_all(&held).unwrap();
            std::fs::write(held.join(SNAPSHOT_MEMORY_FILENAME), vec![b'x'; size]).unwrap();
        }
        assert_eq!(read_snapshot_bytes(directory.path()), 1536);
        assert_eq!(read_snapshot_bytes(&directory.path().join("nowhere")), 0);
    }

    #[test]
    fn what_a_wake_checks_before_anything_is_started() {
        let directory = tempfile::tempdir().unwrap();
        let paths = snapshot_paths(directory.path(), &app_id());
        assert!(matches!(
            ensure_loadable(&paths, &stamp()),
            Err(VmError::SnapshotUnusable { .. })
        ));
        let staging = StagedSnapshot::create(directory.path()).unwrap();
        std::fs::write(&staging.paths.state_path, b"state").unwrap();
        std::fs::write(&staging.paths.memory_path, b"memory").unwrap();
        staging.publish(&paths, stamp()).unwrap();
        ensure_loadable(&paths, &stamp()).unwrap();
        let rebooted = SnapshotStamp {
            host_boot_id: "after-a-reboot".into(),
            ..stamp()
        };
        ensure_loadable(&paths, &rebooted).unwrap();
        let upgraded = SnapshotStamp {
            host: SnapshotHost {
                host_kernel: Some("6.8.0-91-generic".into()),
                ..this_host()
            },
            ..rebooted
        };
        let refused = ensure_loadable(&paths, &upgraded).unwrap_err();
        assert!(refused.message().contains("rebooted into another kernel"));
    }

    #[test]
    fn a_stamp_this_host_cannot_read_is_no_snapshot_at_all() {
        let directory = tempfile::tempdir().unwrap();
        let paths = snapshot_paths(directory.path(), &app_id());
        std::fs::create_dir_all(&paths.directory).unwrap();
        std::fs::write(&paths.stamp_path, "{ not a stamp").unwrap();
        let refused = ensure_loadable(&paths, &stamp()).unwrap_err();
        assert!(refused.message().contains("kept none"), "{refused}");
    }

    fn published_snapshot(directory: &Path) -> SnapshotPaths {
        let paths = snapshot_paths(directory, &app_id());
        let staged = StagedSnapshot::create(directory).unwrap();
        std::fs::write(&staged.paths.state_path, b"state").unwrap();
        std::fs::write(&staged.paths.memory_path, b"memory").unwrap();
        assert_eq!(staged.publish(&paths, stamp()).unwrap(), 6);
        paths
    }

    #[test]
    fn both_snapshot_files_are_checked_even_when_corruption_keeps_the_same_size() {
        for filename in [SNAPSHOT_STATE_FILENAME, SNAPSHOT_MEMORY_FILENAME] {
            let directory = tempfile::tempdir().unwrap();
            let paths = published_snapshot(directory.path());
            let damaged = paths.directory.join(filename);
            let mut bytes = std::fs::read(&damaged).unwrap();
            bytes[0] ^= 1;
            std::fs::write(&damaged, bytes).unwrap();
            let error = ensure_loadable(&paths, &stamp()).unwrap_err();
            assert!(error.message().contains(filename), "{error}");
            assert!(error.message().contains("SHA-256"), "{error}");
        }
    }

    #[test]
    fn missing_and_truncated_snapshot_files_are_refused_before_restoration() {
        for filename in [SNAPSHOT_STATE_FILENAME, SNAPSHOT_MEMORY_FILENAME] {
            for missing in [false, true] {
                let directory = tempfile::tempdir().unwrap();
                let paths = published_snapshot(directory.path());
                let damaged = paths.directory.join(filename);
                if missing {
                    std::fs::remove_file(&damaged).unwrap();
                } else {
                    std::fs::write(&damaged, b"x").unwrap();
                }
                let error = ensure_loadable(&paths, &stamp()).unwrap_err();
                assert!(matches!(error, VmError::SnapshotUnusable { .. }), "{error}");
                assert!(error.message().contains(filename), "{error}");
            }
        }
    }

    #[test]
    fn an_older_snapshot_without_integrity_records_is_refused() {
        let directory = tempfile::tempdir().unwrap();
        let paths = snapshot_paths(directory.path(), &app_id());
        write_json(&paths.stamp_path, &stamp()).unwrap();
        let error = ensure_loadable(&paths, &stamp()).unwrap_err();
        assert!(error.message().contains("integrity record"), "{error}");
    }

    #[test]
    fn an_interrupted_snapshot_is_never_published_and_its_staging_files_are_removed() {
        let directory = tempfile::tempdir().unwrap();
        let paths = snapshot_paths(directory.path(), &app_id());
        let staged = StagedSnapshot::create(directory.path()).unwrap();
        let staging_directory = staged.paths.directory.clone();
        std::fs::write(&staged.paths.state_path, b"state").unwrap();
        assert!(!paths.directory.exists());
        assert!(ensure_loadable(&paths, &stamp()).is_err());
        assert!(staged.publish(&paths, stamp()).is_err());
        assert!(!paths.directory.exists());
        assert!(!staging_directory.exists());
    }

    #[test]
    fn a_snapshot_can_only_be_published_with_two_complete_nonempty_files() {
        let directory = tempfile::tempdir().unwrap();
        let paths = snapshot_paths(directory.path(), &app_id());
        let staged = StagedSnapshot::create(directory.path()).unwrap();
        std::fs::write(&staged.paths.state_path, b"state").unwrap();
        std::fs::write(&staged.paths.memory_path, b"").unwrap();
        assert!(staged.publish(&paths, stamp()).is_err());
        assert!(!paths.directory.exists());
        assert_eq!(read_snapshot_bytes(directory.path()), 0);
    }

    #[test]
    fn a_failed_manifest_write_or_directory_rename_publishes_nothing() {
        for failed_manifest in [false, true] {
            let directory = tempfile::tempdir().unwrap();
            let paths = snapshot_paths(directory.path(), &app_id());
            let staged = StagedSnapshot::create(directory.path()).unwrap();
            let staging_directory = staged.paths.directory.clone();
            std::fs::write(&staged.paths.state_path, b"state").unwrap();
            std::fs::write(&staged.paths.memory_path, b"memory").unwrap();
            if failed_manifest {
                std::fs::create_dir(&staged.paths.stamp_path).unwrap();
            } else {
                std::fs::write(&paths.directory, b"occupied").unwrap();
            }
            assert!(staged.publish(&paths, stamp()).is_err());
            assert!(!staging_directory.exists());
            assert!(!paths.stamp_path.exists());
            assert!(ensure_loadable(&paths, &stamp()).is_err());
        }
    }

    #[test]
    fn publication_exposes_the_files_and_integrity_record_together() {
        let directory = tempfile::tempdir().unwrap();
        let paths = published_snapshot(directory.path());
        let manifest: SnapshotManifest = read_json(&paths.stamp_path).unwrap().unwrap();
        assert_eq!(manifest.state.sha256, hex::encode(Sha256::digest(b"state")));
        assert_eq!(manifest.memory.sha256, hex::encode(Sha256::digest(b"memory")));
        ensure_loadable(&paths, &stamp()).unwrap();
        assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 1);
    }

    #[test]
    fn a_staging_directory_left_by_an_interruption_is_reaped_even_during_the_same_host_boot() {
        let directory = tempfile::tempdir().unwrap();
        let staged = StagedSnapshot::create(directory.path()).unwrap();
        std::fs::write(&staged.paths.memory_path, b"memory").unwrap();
        write_json(&staged.paths.stamp_path, &stamp()).unwrap();
        let reaped = reap_stale_snapshots(directory.path(), &stamp().host_boot_id, &this_host());
        assert_eq!(reaped.snapshots, 1);
        assert!(!staged.paths.directory.exists());
    }

    #[test]
    fn the_first_reason_a_snapshot_drifted_is_the_one_the_operator_is_told() {
        let everything_moved = SnapshotStamp {
            firecracker_version: Some(FIRECRACKER_VERSION.to_string()),
            deployment_id: DeploymentId::parse("dep-2").unwrap(),
            guest_image_version: "another".into(),
            host_boot_id: "another".into(),
            slot: 8,
            host: SnapshotHost::default(),
        };
        assert!(drift_from(&everything_moved, &stamp())
            .unwrap()
            .contains("deployed again"));
    }

    #[test]
    fn a_snapshot_is_the_memory_the_app_was_promised_rather_than_what_it_had_touched() {
        assert_eq!(snapshot_bytes_for(0), 0);
        assert_eq!(snapshot_bytes_for(1), 1_048_576);
        assert_eq!(snapshot_bytes_for(256), 256 * 1_048_576);
    }

    #[test]
    fn what_holds_a_snapshot_is_measured_where_it_would_be_written_and_made_if_absent() {
        let directory = tempfile::tempdir().unwrap();
        let snapshots = directory.path().join("snapshots");
        let disk = measure_snapshot_disk(&snapshots, 4 * GIB).unwrap();
        assert!(snapshots.is_dir());
        assert!(disk.total_bytes > 0);
        assert!(disk.available_bytes <= disk.total_bytes);
        assert_eq!(disk.cache_bytes, 4 * GIB);
        assert_eq!(disk.snapshot_bytes, 0);

        std::fs::write(snapshots.join("memory"), vec![b'x'; 2048]).unwrap();
        assert_eq!(measure_snapshot_disk(&snapshots, 0).unwrap().snapshot_bytes, 2048);
    }

    #[test]
    fn a_snapshot_directory_not_made_yet_is_measured_on_the_disk_it_would_be_made_on() {
        let directory = tempfile::tempdir().unwrap();
        let unmade = directory.path().join("state").join("snapshots");
        let disk = measure_disk_under(&unmade, 4 * GIB).unwrap();
        assert!(!unmade.exists());
        assert!(!directory.path().join("state").exists());
        assert_eq!(
            disk.total_bytes,
            measure_disk_under(directory.path(), 4 * GIB).unwrap().total_bytes
        );
        assert_eq!(disk.cache_bytes, 4 * GIB);
        assert_eq!(disk.snapshot_bytes, 0);
        assert!(measure_disk_under(Path::new("nowhere-relative"), 0).is_err());
    }

    #[test]
    fn a_snapshot_directory_that_cannot_be_made_is_not_measured_as_empty() {
        let directory = tempfile::tempdir().unwrap();
        let occupied = directory.path().join("snapshots");
        std::fs::write(&occupied, b"a file, not a directory").unwrap();
        assert!(measure_snapshot_disk(&occupied, 0).is_err());
    }

    #[test]
    fn a_cache_that_grew_past_the_disk_leaves_no_budget_rather_than_wrapping_round() {
        let disk = SnapshotDisk {
            total_bytes: 10 * GIB,
            available_bytes: GIB,
            cache_bytes: 100 * GIB,
            snapshot_bytes: 0,
        };
        assert_eq!(snapshot_budget(&disk), 0);
        assert!(refusal_for_disk(&disk, 1).is_some());
    }

    #[test]
    fn a_refusal_names_what_the_host_holds_in_units_an_operator_reads() {
        let refused = refusal_for_disk(
            &SnapshotDisk {
                snapshot_bytes: 30 * GIB,
                ..host_disk()
            },
            snapshot_bytes_for(4096),
        )
        .unwrap();
        assert!(refused.contains("GiB"), "{refused}");
        assert!(refused.contains("30.0 GiB"), "{refused}");
    }

    #[test]
    fn a_snapshot_that_would_exactly_fill_the_budget_is_still_taken() {
        let disk = SnapshotDisk {
            total_bytes: 100 * GIB,
            available_bytes: 100 * GIB,
            cache_bytes: 0,
            snapshot_bytes: 0,
        };
        assert_eq!(snapshot_budget(&disk), 92 * GIB);
        assert_eq!(refusal_for_disk(&disk, 92 * GIB), None);
        assert!(refusal_for_disk(&disk, 92 * GIB + 1).is_some());
    }

    /// A snapshot as `sleep` leaves one: memory, state, and whatever stamp is handed in.
    fn asleep(snapshot_dir: &Path, app: &str, memory_bytes: usize, stamp: Option<&str>) -> PathBuf {
        let paths = snapshot_paths(snapshot_dir, &AppId::parse(app).unwrap());
        std::fs::create_dir_all(&paths.directory).unwrap();
        std::fs::write(&paths.memory_path, vec![b'x'; memory_bytes]).unwrap();
        std::fs::write(&paths.state_path, b"vmstate").unwrap();
        if let Some(stamp) = stamp {
            std::fs::write(&paths.stamp_path, stamp).unwrap();
        }
        paths.directory
    }

    fn stamped_by(boot_id: &str) -> String {
        stamped_on(boot_id, SnapshotHost::default())
    }

    fn stamped_on(boot_id: &str, host: SnapshotHost) -> String {
        serde_json::to_string(&SnapshotStamp {
            host_boot_id: boot_id.into(),
            host,
            ..stamp()
        })
        .unwrap()
    }

    // A wake refuses a snapshot this boot cannot restore by name, but nothing else ever removed
    // one, and a host was found holding 9 GiB of them from the boot before.
    #[test]
    fn snapshots_this_boot_cannot_restore_are_removed_and_those_of_this_boot_are_kept() {
        let directory = tempfile::tempdir().unwrap();
        let this_boot = stamp().host_boot_id;
        let kept = asleep(directory.path(), "app-1", 100, Some(&stamped_by(&this_boot)));
        let rebooted = asleep(
            directory.path(),
            "app-2",
            1_000,
            Some(&stamped_by("the-boot-before")),
        );
        let never_stamped = asleep(directory.path(), "app-3", 10_000, None);
        let unreadable = asleep(directory.path(), "app-4", 100_000, Some("{ not a stamp"));
        std::fs::write(directory.path().join("a-file"), b"not a snapshot").unwrap();

        let reaped = reap_stale_snapshots(directory.path(), &this_boot, &this_host());

        assert_eq!(
            reaped,
            Reaped {
                snapshots: 3,
                bytes: 111_000
                    + 3 * "vmstate".len() as u64
                    + "{ not a stamp".len() as u64
                    + stamped_by("the-boot-before").len() as u64,
            }
        );
        for gone in [&rebooted, &never_stamped, &unreadable] {
            assert!(!gone.exists(), "{}", gone.display());
        }
        assert!(kept.join(SNAPSHOT_MEMORY_FILENAME).is_file());
        assert!(kept.join(SNAPSHOT_STATE_FILENAME).is_file());
        assert!(kept.join(SNAPSHOT_STAMP_FILENAME).is_file());
        assert!(directory.path().join("a-file").is_file());
    }

    // Deployed again, moved to another slot, taken under an older guest image: each is a reason
    // the wake gives, with the snapshot in front of it, and none is this reaper's to give.
    #[test]
    fn a_snapshot_of_this_boot_is_kept_whatever_else_its_stamp_says() {
        let directory = tempfile::tempdir().unwrap();
        let this_boot = stamp().host_boot_id;
        let drifted = serde_json::to_string(&SnapshotStamp {
            deployment_id: DeploymentId::parse("dep-2").unwrap(),
            guest_image_version: "older".into(),
            slot: 8,
            ..stamp()
        })
        .unwrap();
        let kept = asleep(directory.path(), "app-1", 100, Some(&drifted));

        assert_eq!(
            reap_stale_snapshots(directory.path(), &this_boot, &this_host()),
            Reaped::default()
        );
        assert!(kept.join(SNAPSHOT_STAMP_FILENAME).is_file());
    }

    // A nightly reboot onto the same kernel is the common one, and cold-booting every app asleep
    // after it is what keeping the snapshots saves.
    #[test]
    fn a_snapshot_an_earlier_boot_took_on_this_kernel_this_cpu_and_this_firecracker_survives_the_reboot() {
        let directory = tempfile::tempdir().unwrap();
        let survives = asleep(
            directory.path(),
            "app-1",
            100,
            Some(&stamped_on("the-boot-before", this_host())),
        );
        let other_kernel = asleep(
            directory.path(),
            "app-2",
            100,
            Some(&stamped_on(
                "the-boot-before",
                SnapshotHost {
                    host_kernel: Some("6.8.0-91-generic".into()),
                    ..this_host()
                },
            )),
        );
        let older_firecracker = asleep(
            directory.path(),
            "app-3",
            100,
            Some(
                &serde_json::to_string(&SnapshotStamp {
                    firecracker_version: Some("v1.15.0".into()),
                    host_boot_id: "the-boot-before".into(),
                    ..stamp()
                })
                .unwrap(),
            ),
        );

        let reaped = reap_stale_snapshots(directory.path(), &stamp().host_boot_id, &this_host());

        assert_eq!(reaped.snapshots, 2);
        assert!(survives.join(SNAPSHOT_STAMP_FILENAME).is_file());
        assert!(!other_kernel.exists());
        assert!(!older_firecracker.exists());
    }

    #[test]
    fn the_cpu_a_guest_was_given_is_named_by_its_identity_and_features_and_not_its_microcode() {
        let cpuinfo = |microcode: &str, flags: &str| {
            format!(
                "processor\t: 0\nvendor_id\t: GenuineIntel\ncpu family\t: 6\nmodel\t\t: 85\nstepping\t: 7\nmicrocode\t: {microcode}\nflags\t\t: {flags}\n\nprocessor\t: 1\nflags\t\t: other\n"
            )
        };
        let before = cpu_template_from(&cpuinfo("0x5003604", "fpu vme avx512f rtm")).unwrap();
        assert!(before.starts_with("none/"));
        assert_eq!(
            cpu_template_from(&cpuinfo("0x5003707", "fpu vme avx512f rtm")).unwrap(),
            before
        );
        assert_ne!(
            cpu_template_from(&cpuinfo("0x5003707", "fpu vme avx512f")).unwrap(),
            before
        );
        assert_eq!(cpu_template_from(""), None);
        assert_eq!(
            kernel_from("6.8.0-90-generic\n", "#91-Ubuntu SMP\n"),
            "6.8.0-90-generic #91-Ubuntu SMP"
        );
    }

    #[test]
    fn a_stamp_that_names_no_firecracker_is_restored_on_its_own_boot_and_refused_after_a_reboot() {
        let unnamed = SnapshotStamp {
            firecracker_version: None,
            ..stamp()
        };
        assert_eq!(drift_from(&unnamed, &stamp()), None);
        let rebooted = SnapshotStamp {
            host_boot_id: "after-a-reboot".into(),
            ..stamp()
        };
        assert!(drift_from(&unnamed, &rebooted)
            .unwrap()
            .contains("another Firecracker"));
        let other = SnapshotStamp {
            firecracker_version: Some("v0.0.0".into()),
            ..stamp()
        };
        assert!(drift_from(&other, &stamp())
            .unwrap()
            .contains("another Firecracker"));
    }

    #[test]
    fn a_host_with_no_snapshots_yet_has_nothing_to_reap() {
        let directory = tempfile::tempdir().unwrap();
        assert_eq!(
            reap_stale_snapshots(directory.path(), "boot-1", &this_host()),
            Reaped::default()
        );
        assert_eq!(
            reap_stale_snapshots(&directory.path().join("nowhere"), "boot-1", &this_host()),
            Reaped::default()
        );
    }

    /// Room for one snapshot of the default guest memory and half of another, both in the
    /// budget and over the reserve the filesystem keeps.
    fn room_for_one() -> SnapshotDisk {
        let one = snapshot_bytes_for(256);
        SnapshotDisk {
            total_bytes: 8 * GIB + one + one / 2,
            available_bytes: 8 * GIB + one + one / 2,
            cache_bytes: 0,
            snapshot_bytes: 0,
        }
    }

    #[test]
    fn a_disk_with_room_for_one_snapshot_admits_the_first_and_refuses_the_second_while_it_is_in_flight() {
        let in_flight = SnapshotsInFlight::default();
        let disk = room_for_one();
        let one = snapshot_bytes_for(256);

        let first = in_flight.admit(&disk, one).expect("the first fits");
        assert_eq!(in_flight.bytes(), one);
        let refused = in_flight.admit(&disk, one).unwrap_err();
        assert!(refused.contains("already hold"), "{refused}");
        assert_eq!(in_flight.bytes(), one, "a refused sleep reserves nothing");

        drop(first);
        assert_eq!(in_flight.bytes(), 0);
        let _second = in_flight
            .admit(&disk, one)
            .expect("the room is there once the first has landed");
    }

    #[test]
    fn what_is_in_flight_is_taken_off_the_room_the_filesystem_has_left_as_well() {
        let in_flight = SnapshotsInFlight::default();
        let one = snapshot_bytes_for(256);
        let budget_but_not_room = SnapshotDisk {
            total_bytes: 100 * GIB,
            ..room_for_one()
        };
        let _first = in_flight.admit(&budget_but_not_room, one).unwrap();
        let refused = in_flight.admit(&budget_but_not_room, one).unwrap_err();
        assert!(refused.contains("every app"), "{refused}");
    }

    #[test]
    fn what_is_held_under_a_snapshot_directory_is_counted_however_deep_it_sits() {
        let directory = tempfile::tempdir().unwrap();
        let nested = directory.path().join("app-1").join("deeper");
        std::fs::create_dir_all(&nested).unwrap();
        std::fs::write(nested.join("memory"), vec![b'x'; 100]).unwrap();
        std::fs::write(directory.path().join("stamp.json"), vec![b'x'; 10]).unwrap();
        assert_eq!(read_snapshot_bytes(directory.path()), 110);
    }
    #[test]
    fn a_snapshot_output_symlink_is_rejected_without_changing_the_file_it_points_at() {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        let directory = tempfile::tempdir().unwrap();
        let protected = directory.path().join("host-file");
        std::fs::write(&protected, b"host state").unwrap();
        std::fs::set_permissions(&protected, std::fs::Permissions::from_mode(0o640)).unwrap();
        let before = std::fs::metadata(&protected).unwrap();
        let output = directory.path().join("output");
        let destination = directory.path().join("snapshot");
        std::os::unix::fs::symlink(&protected, &output).unwrap();
        assert!(publish_snapshot_file(&output, &destination).is_err());
        let after = std::fs::metadata(&protected).unwrap();
        assert_eq!(
            (after.uid(), after.gid(), after.mode()),
            (before.uid(), before.gid(), before.mode())
        );
        assert_eq!(std::fs::read(&protected).unwrap(), b"host state");
        assert!(!destination.exists());
    }

    #[test]
    fn live_jail_exchanges_survive_readoption_and_old_boot_exchanges_are_reclaimed() {
        let directory = tempfile::tempdir().unwrap();
        let exchange = directory.path().join(".jailer");
        std::fs::create_dir_all(exchange.join("app-1")).unwrap();
        std::fs::write(exchange.join("app-1/memory"), b"unpublished memory").unwrap();
        std::fs::write(exchange.join(EXCHANGE_BOOT_ID_FILENAME), "boot-1").unwrap();
        assert_eq!(
            reap_stale_snapshots(directory.path(), "boot-1", &this_host()),
            Reaped::default()
        );
        assert!(exchange.join("app-1/memory").exists());
        assert!(reap_stale_snapshots(directory.path(), "boot-2", &this_host()).bytes > 0);
        assert!(!exchange.exists());
    }
}
