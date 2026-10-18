use protocol::{AppId, HostCapacity, InstanceResources, InstanceState};

use crate::domain::backoff::NO_START_ATTEMPTS;
use crate::domain::report::InstanceRecord;

const BYTES_PER_MIB: u64 = 1_048_576;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct FilesystemSpace {
    pub total_bytes: u64,
    pub available_bytes: u64,
}

pub const HOST_BASELINE_MIB: u64 = 640;

pub fn guest_memory_mib(host_memory_mib: u64, storage_cache_mib: u64) -> u64 {
    host_memory_mib
        .saturating_sub(storage_cache_mib)
        .saturating_sub(HOST_BASELINE_MIB)
}

/// How many apps of `memory_mib` each are up at once in what a host has for guests.
pub fn apps_up_at_once(guest_memory_mib: u64, memory_mib: u32) -> u32 {
    apps_that_fit(guest_memory_mib, u64::from(memory_mib))
}

/// How many apps taking `bytes_each` of the disk its budget holds.
pub fn apps_held_on_disk(disk_budget_bytes: u64, bytes_each: u64) -> u32 {
    apps_that_fit(disk_budget_bytes, bytes_each)
}

fn apps_that_fit(room: u64, each: u64) -> u32 {
    room.checked_div(each)
        .map_or(0, |fit| u32::try_from(fit).unwrap_or(u32::MAX))
}

const HOLDS_NOTHING: [InstanceState; 4] = [
    InstanceState::Idle,
    InstanceState::Stopped,
    InstanceState::Failed,
    InstanceState::Expired,
];

/// A record holds what it was given while there is a microVM behind it or one on its way. A
/// boot in flight is pending with its attempt spent; a record pending with none spent is one
/// waiting for room, with nothing behind it yet.
pub(crate) fn holds_something(record: &InstanceRecord) -> bool {
    !HOLDS_NOTHING.contains(&record.state)
        && !(record.state == InstanceState::Pending && record.start_attempts == NO_START_ATTEMPTS)
}

pub fn committed_resources(records: &[InstanceRecord]) -> Vec<InstanceResources> {
    records
        .iter()
        .filter(|record| holds_something(record))
        .map(|record| record.resources)
        .collect()
}

fn committed_memory_mib(committed: &[InstanceResources]) -> u64 {
    committed.iter().map(|entry| u64::from(entry.memory_mib)).sum()
}

/// What this host is short of bringing `app_id` up wanting `wanted`, beside everything else it
/// holds; nothing where it fits. The app's own record is left out: it is the one being measured.
pub fn memory_shortfall_for(
    guest_memory_mib: u64,
    records: &[InstanceRecord],
    app_id: &AppId,
    wanted: &InstanceResources,
) -> Option<u64> {
    let others: Vec<InstanceResources> = records
        .iter()
        .filter(|record| &record.app_id != app_id && holds_something(record))
        .map(|record| record.resources)
        .collect();
    let shortfall = memory_shortfall_mib(guest_memory_mib, &others, wanted);
    (shortfall > 0).then_some(shortfall)
}

pub fn allocatable_capacity(
    capacity: &HostCapacity,
    committed: &[InstanceResources],
    available_cache_bytes: u64,
) -> HostCapacity {
    let used_vcpu: u32 = committed.iter().map(|entry| entry.vcpu_count).sum();
    HostCapacity {
        vcpu_count: capacity.vcpu_count.saturating_sub(used_vcpu),
        memory_mib: capacity
            .memory_mib
            .saturating_sub(committed_memory_mib(committed)),
        cache_bytes: available_cache_bytes.min(capacity.cache_bytes),
    }
}

pub fn memory_shortfall_mib(
    host_memory_mib: u64,
    committed: &[InstanceResources],
    wanted: &InstanceResources,
) -> u64 {
    (committed_memory_mib(committed) + u64::from(wanted.memory_mib)).saturating_sub(host_memory_mib)
}

pub fn read_host_memory_mib() -> u64 {
    read_meminfo_kib("MemTotal:").map_or(0, |kib| (kib * 1024) / BYTES_PER_MIB)
}

pub fn read_memory_available_bytes() -> Option<u64> {
    read_meminfo_kib("MemAvailable:").map(|kib| kib * 1024)
}

#[cfg(target_os = "linux")]
fn read_meminfo_kib(field: &str) -> Option<u64> {
    let meminfo = std::fs::read_to_string("/proc/meminfo").ok()?;
    meminfo
        .lines()
        .find(|line| line.starts_with(field))
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|value| value.parse().ok())
}

#[cfg(not(target_os = "linux"))]
fn read_meminfo_kib(_field: &str) -> Option<u64> {
    None
}

pub fn read_vcpu_count() -> u32 {
    std::thread::available_parallelism().map_or(1, |count| count.get() as u32)
}

pub fn read_filesystem_space(directory: &std::path::Path) -> std::io::Result<FilesystemSpace> {
    #[cfg(unix)]
    {
        let stats = nix::sys::statvfs::statvfs(directory)
            .map_err(|error| std::io::Error::from_raw_os_error(error as i32))?;
        let block_size = stats.fragment_size() as u64;
        Ok(FilesystemSpace {
            total_bytes: stats.blocks() as u64 * block_size,
            available_bytes: stats.blocks_available() as u64 * block_size,
        })
    }
    #[cfg(not(unix))]
    {
        let _ = directory;
        Ok(FilesystemSpace::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::instance_record;
    use protocol::{AppId, DEFAULT_INSTANCE_RESOURCES};

    const APP_MEMORY_MIB: u64 = DEFAULT_INSTANCE_RESOURCES.memory_mib as u64;
    const NEIGHBOURS_THAT_FIT: u64 = 3;
    const HOST_MEMORY_MIB: u64 = APP_MEMORY_MIB * (NEIGHBOURS_THAT_FIT + 1);

    fn neighbours(count: u64, state: InstanceState) -> Vec<InstanceRecord> {
        (0..count)
            .map(|index| {
                instance_record(|record| {
                    record.app_id = AppId::parse(format!("neighbour-{index}")).unwrap();
                    record.state = state;
                })
            })
            .collect()
    }

    fn shortfall(count: u64, state: InstanceState) -> u64 {
        memory_shortfall_mib(
            HOST_MEMORY_MIB,
            &committed_resources(&neighbours(count, state)),
            &DEFAULT_INSTANCE_RESOURCES,
        )
    }

    #[test]
    fn a_host_has_room_for_one_more_microvm_until_it_does_not() {
        assert_eq!(shortfall(0, InstanceState::Running), 0);
        assert_eq!(shortfall(NEIGHBOURS_THAT_FIT, InstanceState::Running), 0);
        assert_eq!(
            shortfall(NEIGHBOURS_THAT_FIT + 1, InstanceState::Running),
            APP_MEMORY_MIB
        );
        assert_eq!(shortfall(NEIGHBOURS_THAT_FIT * 2, InstanceState::Idle), 0);
    }

    #[test]
    fn what_a_host_is_short_for_an_app_leaves_out_the_app_itself_and_everything_holding_nothing() {
        let app = AppId::parse("app-1").unwrap();
        let mut records = neighbours(NEIGHBOURS_THAT_FIT, InstanceState::Running);
        assert_eq!(
            memory_shortfall_for(HOST_MEMORY_MIB, &records, &app, &DEFAULT_INSTANCE_RESOURCES),
            None
        );
        // The app's own record is what the start or the wake puts back, so it is not in the way.
        records.push(instance_record(|record| {
            record.app_id = app.clone();
            record.state = InstanceState::Starting;
        }));
        assert_eq!(
            memory_shortfall_for(HOST_MEMORY_MIB, &records, &app, &DEFAULT_INSTANCE_RESOURCES),
            None
        );
        records.push(instance_record(|record| {
            record.app_id = AppId::parse("neighbour-asleep").unwrap();
            record.state = InstanceState::Idle;
        }));
        assert_eq!(
            memory_shortfall_for(HOST_MEMORY_MIB, &records, &app, &DEFAULT_INSTANCE_RESOURCES),
            None
        );
        records.push(instance_record(|record| {
            record.app_id = AppId::parse("neighbour-up").unwrap();
        }));
        assert_eq!(
            memory_shortfall_for(HOST_MEMORY_MIB, &records, &app, &DEFAULT_INSTANCE_RESOURCES),
            Some(APP_MEMORY_MIB)
        );
        let twice_as_much = InstanceResources {
            memory_mib: DEFAULT_INSTANCE_RESOURCES.memory_mib * 2,
            ..DEFAULT_INSTANCE_RESOURCES
        };
        assert_eq!(
            memory_shortfall_for(HOST_MEMORY_MIB, &records, &app, &twice_as_much),
            Some(APP_MEMORY_MIB * 2)
        );
    }

    #[test]
    fn memory_the_host_needs_is_not_memory_a_guest_may_be_given() {
        const HOST_MIB: u64 = 7779;
        const CACHE_MIB: u64 = 2048;
        assert_eq!(
            guest_memory_mib(HOST_MIB, CACHE_MIB),
            HOST_MIB - CACHE_MIB - HOST_BASELINE_MIB
        );
        let roomier = guest_memory_mib(HOST_MIB, CACHE_MIB);
        let tighter = guest_memory_mib(HOST_MIB, CACHE_MIB + 1024);
        assert_eq!(roomier - tighter, 1024);
        assert_eq!(guest_memory_mib(512, CACHE_MIB), 0);
        let fits = roomier / APP_MEMORY_MIB;
        assert!(
            memory_shortfall_mib(
                roomier,
                &committed_resources(&neighbours(fits, InstanceState::Running)),
                &DEFAULT_INSTANCE_RESOURCES
            ) > 0
        );
    }

    #[test]
    fn how_many_apps_of_one_size_a_host_runs_at_once_and_holds_on_disk() {
        let memory_mib = DEFAULT_INSTANCE_RESOURCES.memory_mib;
        assert_eq!(apps_up_at_once(245 * APP_MEMORY_MIB, memory_mib), 245);
        assert_eq!(apps_up_at_once(246 * APP_MEMORY_MIB - 1, memory_mib), 245);
        assert_eq!(apps_up_at_once(0, memory_mib), 0);

        let snapshot = APP_MEMORY_MIB * BYTES_PER_MIB;
        assert_eq!(apps_held_on_disk(1600 * snapshot, snapshot), 1600);
        assert_eq!(apps_held_on_disk(snapshot - 1, snapshot), 0);
        assert_eq!(apps_held_on_disk(u64::MAX, 1), u32::MAX);
        assert_eq!(apps_held_on_disk(u64::MAX, 0), 0);
    }

    #[test]
    fn allocatable_is_what_is_left_once_every_booted_app_is_taken_off() {
        let capacity = HostCapacity {
            vcpu_count: 4,
            memory_mib: 8192,
            cache_bytes: 1000,
        };
        let booted = vec![DEFAULT_INSTANCE_RESOURCES, DEFAULT_INSTANCE_RESOURCES];
        assert_eq!(
            allocatable_capacity(&capacity, &booted, 400),
            HostCapacity {
                vcpu_count: 4 - DEFAULT_INSTANCE_RESOURCES.vcpu_count * 2,
                memory_mib: 8192 - APP_MEMORY_MIB * 2,
                cache_bytes: 400,
            }
        );
        let small = HostCapacity {
            vcpu_count: 1,
            memory_mib: APP_MEMORY_MIB,
            cache_bytes: 1000,
        };
        assert_eq!(
            allocatable_capacity(
                &small,
                &[InstanceResources {
                    vcpu_count: 4,
                    memory_mib: 8192
                }],
                400
            ),
            HostCapacity {
                vcpu_count: 0,
                memory_mib: 0,
                cache_bytes: 400
            }
        );
    }

    #[test]
    fn every_state_with_a_microvm_behind_it_still_holds_what_it_was_given() {
        let states = [
            InstanceState::Running,
            InstanceState::Starting,
            InstanceState::Unhealthy,
            InstanceState::Stopping,
            InstanceState::Idle,
            InstanceState::Stopped,
            InstanceState::Failed,
        ];
        let records: Vec<InstanceRecord> = states
            .iter()
            .map(|state| instance_record(|record| record.state = *state))
            .collect();
        assert_eq!(committed_resources(&records).len(), 4);
        assert!(committed_resources(&[instance_record(|record| {
            record.state = InstanceState::Idle;
            record.on_request = true;
        })])
        .is_empty());
    }

    #[test]
    fn a_boot_in_flight_holds_its_memory_and_a_record_waiting_for_room_does_not() {
        let booting = instance_record(|record| {
            record.state = InstanceState::Pending;
            record.start_attempts = crate::domain::backoff::AttemptWindow {
                attempts: 1,
                last_attempt_at_ms: Some(0),
            };
        });
        assert_eq!(committed_resources(&[booting]).len(), 1);
        let waiting = instance_record(|record| record.state = InstanceState::Pending);
        assert!(committed_resources(&[waiting]).is_empty());
    }

    #[test]
    fn a_cache_reading_larger_than_the_disk_is_never_reported_as_room_that_exists() {
        let capacity = HostCapacity {
            vcpu_count: 4,
            memory_mib: 8192,
            cache_bytes: 1_000,
        };
        assert_eq!(
            allocatable_capacity(&capacity, &[], u64::MAX).cache_bytes,
            capacity.cache_bytes
        );
        assert_eq!(allocatable_capacity(&capacity, &[], 0).cache_bytes, 0);
    }

    #[test]
    fn the_disk_this_host_keeps_its_state_on_is_measured_and_a_path_that_is_not_there_is_not() {
        let directory = tempfile::tempdir().unwrap();
        let space = read_filesystem_space(directory.path()).unwrap();
        assert!(space.total_bytes > 0);
        assert!(space.available_bytes <= space.total_bytes);
        assert!(read_filesystem_space(&directory.path().join("no-such-place")).is_err());
    }

    #[test]
    fn a_host_that_cannot_count_its_own_cpus_still_reports_one_rather_than_none() {
        assert!(read_vcpu_count() >= 1);
    }

    #[test]
    fn nothing_is_left_for_a_guest_on_a_host_whose_memory_cannot_be_read() {
        assert_eq!(guest_memory_mib(0, 0), 0);
        assert!(guest_memory_mib(read_host_memory_mib(), 0) <= read_host_memory_mib());
    }
}
