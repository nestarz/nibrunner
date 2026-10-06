use std::path::{Path, PathBuf};

use protocol::{AppId, DeploymentId};
use serde::{Deserialize, Serialize};

use crate::domain::report::capacity::FilesystemSpace;
use crate::json_store::read_json;
use crate::ports::VmError;

pub const SNAPSHOT_STATE_FILENAME: &str = "vmstate";
pub const SNAPSHOT_MEMORY_FILENAME: &str = "memory";

pub const SNAPSHOT_STAMP_FILENAME: &str = "stamp.json";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SnapshotStamp {
    #[serde(default)]
    pub firecracker_version: Option<String>,
    pub deployment_id: DeploymentId,
    pub guest_image_version: String,
    pub host_boot_id: String,
    pub slot: u32,
}

fn drift_reason(stored: &SnapshotStamp, expected: &SnapshotStamp) -> Option<&'static str> {
    if stored.deployment_id != expected.deployment_id {
        return Some("the app has been deployed again since");
    }
    if stored.firecracker_version != expected.firecracker_version {
        return Some("the Firecracker snapshot format has changed");
    }
    if stored.guest_image_version != expected.guest_image_version {
        return Some("the guest image has changed");
    }
    if stored.host_boot_id != expected.host_boot_id {
        return Some("the host has rebooted");
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
    let directory = snapshot_dir.join(app_id.as_str());
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

/// Removes every snapshot under `snapshot_dir` that was not taken this boot: one stamped with
/// another boot id, and one with no stamp this host can read. Neither can ever be woken from —
/// a wake refuses them by name — and each holds the memory its app was promised, so left alone
/// they are the disk a reboot leaves behind, for good. One stamped this boot is never touched,
/// whatever else its stamp says: that is the wake's to judge, and to say why.
pub fn reap_stale_snapshots(snapshot_dir: &Path, host_boot_id: &str) -> Reaped {
    let mut reaped = Reaped::default();
    let Ok(entries) = std::fs::read_dir(snapshot_dir) else {
        return reaped;
    };
    for entry in entries.filter_map(Result::ok) {
        if !entry.file_type().is_ok_and(|kind| kind.is_dir()) {
            continue;
        }
        let directory = entry.path();
        let stamp: Option<SnapshotStamp> = read_json(&directory.join(SNAPSHOT_STAMP_FILENAME)).ok().flatten();
        if stamp.is_some_and(|stamp| stamp.host_boot_id == host_boot_id) {
            continue;
        }
        let bytes = read_snapshot_bytes(&directory);
        match std::fs::remove_dir_all(&directory) {
            Ok(()) => {
                reaped.snapshots += 1;
                reaped.bytes += bytes;
            }
            Err(error) => {
                tracing::warn!(path = %directory.display(), %error, "a snapshot left by an earlier boot could not be removed");
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

pub fn ensure_loadable(stamp_path: &Path, expected: &SnapshotStamp) -> Result<(), VmError> {
    let stored: Option<SnapshotStamp> = read_json(stamp_path).ok().flatten();
    let Some(stored) = stored else {
        return Err(VmError::SnapshotUnusable {
            reason: "this host kept none".into(),
        });
    };
    match drift_from(&stored, expected) {
        None => Ok(()),
        Some(reason) => Err(VmError::SnapshotUnusable { reason }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::json_store::write_json;
    use crate::test_support::{app_id, deployment_id};

    fn stamp() -> SnapshotStamp {
        SnapshotStamp {
            firecracker_version: Some(crate::adapters::vm::process::FIRECRACKER_VERSION.into()),
            deployment_id: deployment_id(),
            guest_image_version: "6.1.180-98db6df338f0".into(),
            host_boot_id: "b6b8f0d2-0000-4000-8000-000000000001".into(),
            slot: 7,
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
        for version in [None, Some("v1.16.1".into())] {
            let old_format = SnapshotStamp {
                firecracker_version: version,
                ..stamp()
            };
            assert_eq!(
                drift_from(&old_format, &stamp()).as_deref(),
                Some("the Firecracker snapshot format has changed")
            );
        }
        let redeployed = SnapshotStamp {
            firecracker_version: Some(crate::adapters::vm::process::FIRECRACKER_VERSION.into()),
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
        assert!(drift_from(&rebooted, &stamp()).unwrap().contains("rebooted"));
        let moved = SnapshotStamp { slot: 8, ..stamp() };
        assert!(drift_from(&moved, &stamp()).unwrap().contains("another slot"));
    }

    #[test]
    fn a_legacy_snapshot_without_a_vmm_version_is_readable_but_not_restorable() {
        let mut legacy = serde_json::to_value(stamp()).unwrap();
        legacy.as_object_mut().unwrap().remove("firecrackerVersion");
        let legacy: SnapshotStamp = serde_json::from_value(legacy).unwrap();
        assert!(legacy.firecracker_version.is_none());
        assert!(drift_from(&legacy, &stamp()).is_some());
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
            ensure_loadable(&paths.stamp_path, &stamp()),
            Err(VmError::SnapshotUnusable { .. })
        ));
        write_json(&paths.stamp_path, &stamp()).unwrap();
        ensure_loadable(&paths.stamp_path, &stamp()).unwrap();
        let rebooted = SnapshotStamp {
            host_boot_id: "after-a-reboot".into(),
            ..stamp()
        };
        let refused = ensure_loadable(&paths.stamp_path, &rebooted).unwrap_err();
        assert!(refused.message().contains("rebooted"));
    }

    #[test]
    fn a_stamp_this_host_cannot_read_is_no_snapshot_at_all() {
        let directory = tempfile::tempdir().unwrap();
        let paths = snapshot_paths(directory.path(), &app_id());
        std::fs::create_dir_all(&paths.directory).unwrap();
        std::fs::write(&paths.stamp_path, "{ not a stamp").unwrap();
        let refused = ensure_loadable(&paths.stamp_path, &stamp()).unwrap_err();
        assert!(refused.message().contains("kept none"), "{refused}");
    }

    #[test]
    fn the_first_reason_a_snapshot_drifted_is_the_one_the_operator_is_told() {
        let everything_moved = SnapshotStamp {
            firecracker_version: Some(crate::adapters::vm::process::FIRECRACKER_VERSION.into()),
            deployment_id: DeploymentId::parse("dep-2").unwrap(),
            guest_image_version: "another".into(),
            host_boot_id: "another".into(),
            slot: 8,
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
        serde_json::to_string(&SnapshotStamp {
            host_boot_id: boot_id.into(),
            ..stamp()
        })
        .unwrap()
    }

    // After a reboot every snapshot on the disk is of a kernel that is gone, and a wake refuses
    // each by name — but nothing else ever removed them, and a host was found holding 9 GiB of
    // them from the boot before.
    #[test]
    fn snapshots_left_by_an_earlier_boot_are_removed_and_those_of_this_boot_are_kept() {
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

        let reaped = reap_stale_snapshots(directory.path(), &this_boot);

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
            firecracker_version: Some(crate::adapters::vm::process::FIRECRACKER_VERSION.into()),
            deployment_id: DeploymentId::parse("dep-2").unwrap(),
            guest_image_version: "older".into(),
            slot: 8,
            ..stamp()
        })
        .unwrap();
        let kept = asleep(directory.path(), "app-1", 100, Some(&drifted));

        assert_eq!(
            reap_stale_snapshots(directory.path(), &this_boot),
            Reaped::default()
        );
        assert!(kept.join(SNAPSHOT_STAMP_FILENAME).is_file());
    }

    #[test]
    fn a_host_with_no_snapshots_yet_has_nothing_to_reap() {
        let directory = tempfile::tempdir().unwrap();
        assert_eq!(
            reap_stale_snapshots(directory.path(), "boot-1"),
            Reaped::default()
        );
        assert_eq!(
            reap_stale_snapshots(&directory.path().join("nowhere"), "boot-1"),
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
}
