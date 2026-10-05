use std::collections::{BTreeMap, BTreeSet};

use guest_contract::filesystem::MeasuredCompute;
use nft_render::AppTraffic;
use protocol::{AppId, FilesystemUsage, InstanceState, ReportedVolume, UsageMeters, VolumeState};

use crate::domain::report::InstanceRecord;
use crate::ports::GuestReading;

// A microVM exists and is holding the memory it was promised. Pending has not been given any yet,
// Idle handed it back to a snapshot, and Stopped and Failed are holding none.
const HOLDS_MEMORY: [InstanceState; 4] = [
    InstanceState::Starting,
    InstanceState::Running,
    InstanceState::Unhealthy,
    InstanceState::Stopping,
];

// `/proc/stat` counts in USER_HZ, which Linux fixes at 100 for anything reading it from userspace
// whatever the kernel was built to tick at.
const MILLIS_PER_TICK: u64 = 10;

// What a pass that ran late may bill for, as a multiple of the interval it meant to run at. A host
// that was suspended and a daemon that was stopped and started again both come back to a clock
// that moved further than they watched, and time nobody observed is not time this host will claim
// an app was using.
const LATE_PASS_ALLOWANCE: u64 = 2;

/// What a counter that only grows has grown by. A reading below the one before it is a counter
/// that restarted — nftables forgets on a reload, a guest forgets on a reboot — so what it holds
/// now is all of it this host can still account for. A first reading is not usage: nothing is
/// known about what came before it.
pub fn advanced(before: Option<u64>, after: u64) -> u64 {
    match before {
        None => 0,
        Some(before) if after >= before => after - before,
        Some(_) => after,
    }
}

pub fn elapsed_since(before: Option<i64>, now_ms: i64, interval_ms: u64) -> u64 {
    let Some(before) = before else { return 0 };
    u64::try_from(now_ms.saturating_sub(before))
        .unwrap_or(0)
        .min(interval_ms * LATE_PASS_ALLOWANCE)
}

// A volume that is holding bytes on the backend. Ready is attached to a guest and Detached is not,
// and the difference costs nothing to store: what was written is still written either way. Pending
// has nothing yet, Failed never got it, and Deleted has given it back.
const STORES_BYTES: [VolumeState; 2] = [VolumeState::Ready, VolumeState::Detached];

const BYTES_PER_MIB: u64 = 1_048_576;
const MILLIS_PER_SECOND: u64 = 1_000;

/// What holding a level for a stretch comes to.
///
/// Disk is not a flow the way transferred bytes and spent ticks are, and the difference between
/// two readings of it is not usage: eight gibibytes now and eight gibibytes in an hour is an app
/// that held eight gibibytes for an hour, not one that used nothing. So what accumulates is the
/// level multiplied by the time it was held for, and never a subtraction.
///
/// Counted in mebibyte-seconds because byte-milliseconds do not fit: eight gibibytes held for a
/// month already outruns a `u64`, where these leave a terabyte a decade's headroom. Rounding is
/// down and under a mebibyte-second a pass, which is nobody's bill.
fn held_for(bytes: u64, elapsed_ms: u64) -> u64 {
    (bytes / BYTES_PER_MIB).saturating_mul(elapsed_ms) / MILLIS_PER_SECOND
}

fn provisioned_bytes(volumes: &[ReportedVolume], app_id: &AppId) -> u64 {
    volumes
        .iter()
        .filter(|volume| &volume.app_id == app_id && STORES_BYTES.contains(&volume.state))
        .map(|volume| volume.size_bytes)
        .sum()
}

pub struct MeterInputs<'a> {
    pub records: &'a BTreeMap<AppId, InstanceRecord>,
    pub traffic_before: &'a BTreeMap<AppId, AppTraffic>,
    pub traffic_after: &'a BTreeMap<AppId, AppTraffic>,
    pub volumes: &'a [ReportedVolume],
    pub volume_usage: &'a BTreeMap<AppId, FilesystemUsage>,
    pub elapsed_ms: u64,
}

/// The pass that meters wall time, what arrived at each guest, and what each app is holding on
/// disk. All of it is the host's own observation bar the last, so most of this holds for a guest
/// that answers nothing at all.
///
/// Over every app this host holds a record of *or* is storing bytes for, because a volume kept
/// after its instance went away is still a volume somebody is paying to keep.
pub fn metered_after(
    previous: &BTreeMap<AppId, UsageMeters>,
    inputs: &MeterInputs<'_>,
) -> BTreeMap<AppId, UsageMeters> {
    let stored = inputs
        .volumes
        .iter()
        .filter(|volume| STORES_BYTES.contains(&volume.state))
        .map(|volume| &volume.app_id);
    let apps: BTreeSet<&AppId> = inputs.records.keys().chain(stored).collect();

    apps.into_iter()
        .map(|app_id| {
            let mut meter = previous.get(app_id).copied().unwrap_or_default();
            if let Some(record) = inputs.records.get(app_id) {
                if HOLDS_MEMORY.contains(&record.state) {
                    meter.running_ms += inputs.elapsed_ms;
                } else if record.is_idle() {
                    meter.idle_ms += inputs.elapsed_ms;
                }
            }
            if let Some(after) = inputs.traffic_after.get(app_id) {
                let before = inputs.traffic_before.get(app_id);
                meter.rx_bytes += advanced(before.map(|before| before.received.bytes), after.received.bytes);
                meter.tx_bytes += advanced(before.map(|before| before.sent.bytes), after.sent.bytes);
            }
            meter.disk_provisioned_mib_seconds +=
                held_for(provisioned_bytes(inputs.volumes, app_id), inputs.elapsed_ms);
            // Kept while an app sleeps, unlike everything a guest has to be awake to answer: the
            // last reading stands until a running guest replaces it, because the bytes are still
            // on the disk whether or not there is anything up to be asked about them.
            if let Some(usage) = inputs.volume_usage.get(app_id) {
                meter.disk_used_mib_seconds += held_for(usage.used_bytes, inputs.elapsed_ms);
            }
            (app_id.clone(), meter)
        })
        .collect()
}

/// The pass that meters what the guests said they spent, which only a guest that answers can. It
/// runs on the measurement interval rather than the activity one, so most passes over an app add
/// nothing and the pass after a reading adds the whole stretch since the last one.
pub fn cpu_metered_after(
    previous: &BTreeMap<AppId, UsageMeters>,
    previous_ticks: &BTreeMap<AppId, MeasuredCompute>,
    taken: &[(AppId, GuestReading)],
) -> BTreeMap<AppId, UsageMeters> {
    let mut metered = previous.clone();
    for (app_id, reading) in taken {
        let Some(measured) = reading.compute else {
            continue;
        };
        let spent = advanced(
            previous_ticks.get(app_id).map(|before| before.cpu_busy_ticks),
            measured.cpu_busy_ticks,
        );
        metered.entry(app_id.clone()).or_default().cpu_ms += spent * MILLIS_PER_TICK;
    }
    metered
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::*;
    use nft_render::Counted;
    use protocol::INSTANCE_STATES;

    const TICK_MS: u64 = 5_000;

    fn compute(busy: u64) -> MeasuredCompute {
        MeasuredCompute {
            memory_total_bytes: 268_435_456,
            memory_used_bytes: 1024,
            cpu_total_ticks: busy * 4,
            cpu_busy_ticks: busy,
        }
    }

    fn traffic(received: u64, sent: u64) -> BTreeMap<AppId, AppTraffic> {
        BTreeMap::from([(
            app_id(),
            AppTraffic {
                received: Counted {
                    packets: 1,
                    bytes: received,
                },
                sent: Counted {
                    packets: 1,
                    bytes: sent,
                },
                activity: Counted::default(),
            },
        )])
    }

    fn records(state: InstanceState) -> BTreeMap<AppId, InstanceRecord> {
        BTreeMap::from([(app_id(), instance_record(|record| record.state = state))])
    }

    fn held(metered: &BTreeMap<AppId, UsageMeters>) -> UsageMeters {
        metered.get(&app_id()).copied().unwrap_or_default()
    }

    const GIB: u64 = 1_073_741_824;

    #[derive(Default)]
    struct Inputs {
        records: BTreeMap<AppId, InstanceRecord>,
        traffic_before: BTreeMap<AppId, AppTraffic>,
        traffic_after: BTreeMap<AppId, AppTraffic>,
        volumes: Vec<ReportedVolume>,
        volume_usage: BTreeMap<AppId, FilesystemUsage>,
    }

    fn metered(previous: &BTreeMap<AppId, UsageMeters>, given: Inputs) -> BTreeMap<AppId, UsageMeters> {
        metered_after(
            previous,
            &MeterInputs {
                records: &given.records,
                traffic_before: &given.traffic_before,
                traffic_after: &given.traffic_after,
                volumes: &given.volumes,
                volume_usage: &given.volume_usage,
                elapsed_ms: TICK_MS,
            },
        )
    }

    fn volume(state: VolumeState, size_bytes: u64) -> Vec<ReportedVolume> {
        vec![crate::test_support::reported_volume(|volume| {
            volume.state = state;
            volume.size_bytes = size_bytes;
        })]
    }

    fn filled(used_bytes: u64) -> BTreeMap<AppId, FilesystemUsage> {
        BTreeMap::from([(
            app_id(),
            FilesystemUsage {
                total_bytes: 8 * GIB,
                used_bytes,
                measured_at: observed_at(),
            },
        )])
    }

    #[test]
    fn a_counter_that_grew_is_metered_by_what_it_grew_by() {
        assert_eq!(advanced(Some(1_000), 1_500), 500);
        assert_eq!(advanced(Some(1_000), 1_000), 0);
    }

    #[test]
    fn a_counter_that_restarted_is_metered_by_what_it_holds_rather_than_read_as_a_fall() {
        assert_eq!(
            advanced(Some(9_000_000), 16),
            16,
            "the ruleset was reloaded; 16 bytes have moved since"
        );
    }

    #[test]
    fn a_first_reading_is_not_usage_because_nothing_is_known_of_what_came_before_it() {
        assert_eq!(advanced(None, 4_096), 0);
        assert_eq!(elapsed_since(None, 10_000, TICK_MS), 0);
    }

    #[test]
    fn a_pass_that_ran_late_bills_the_interval_it_meant_to_rather_than_the_clock_it_woke_to() {
        assert_eq!(elapsed_since(Some(1_000), 6_000, TICK_MS), TICK_MS);
        assert_eq!(
            elapsed_since(Some(0), 86_400_000, TICK_MS),
            TICK_MS * LATE_PASS_ALLOWANCE,
            "a host that was suspended for a day watched none of it"
        );
        assert_eq!(
            elapsed_since(Some(9_000), 1_000, TICK_MS),
            0,
            "a clock that went backwards is not time an app spent"
        );
    }

    #[test]
    fn time_is_metered_against_what_the_app_was_holding_while_it_passed() {
        let running = metered(
            &BTreeMap::new(),
            Inputs {
                records: records(InstanceState::Running),
                ..Default::default()
            },
        );
        assert_eq!(held(&running).running_ms, TICK_MS);
        assert_eq!(held(&running).idle_ms, 0);

        let idle = metered(
            &BTreeMap::new(),
            Inputs {
                records: records(InstanceState::Idle),
                ..Default::default()
            },
        );
        assert_eq!(idle.get(&app_id()).map(|meter| meter.idle_ms), Some(TICK_MS));
        assert_eq!(held(&idle).running_ms, 0);
    }

    #[test]
    fn every_state_meters_against_exactly_one_of_the_two_or_neither() {
        for state in INSTANCE_STATES {
            let counted = metered(
                &BTreeMap::new(),
                Inputs {
                    records: records(state),
                    ..Default::default()
                },
            );
            let meter = held(&counted);
            assert!(
                meter.running_ms == 0 || meter.idle_ms == 0,
                "{state:?} was metered as holding memory and as asleep at once"
            );
            let billed = meter.running_ms + meter.idle_ms;
            assert!(billed == 0 || billed == TICK_MS, "{state:?} billed {billed}");
        }
    }

    #[test]
    fn a_meter_carries_what_it_already_held_forward() {
        let before = BTreeMap::from([(
            app_id(),
            UsageMeters {
                running_ms: 60_000,
                rx_bytes: 4_096,
                tx_bytes: 8_192,
                ..UsageMeters::default()
            },
        )]);
        let after = metered(
            &before,
            Inputs {
                records: records(InstanceState::Running),
                traffic_before: traffic(1_000, 20_000),
                traffic_after: traffic(1_500, 90_000),
                ..Default::default()
            },
        );
        assert_eq!(held(&after).running_ms, 60_000 + TICK_MS);
        assert_eq!(held(&after).rx_bytes, 4_096 + 500);
        assert_eq!(held(&after).tx_bytes, 8_192 + 70_000);
    }

    #[test]
    fn an_app_nothing_could_be_read_about_keeps_its_bytes_rather_than_losing_them() {
        let before = BTreeMap::from([(
            app_id(),
            UsageMeters {
                rx_bytes: 4_096,
                ..UsageMeters::default()
            },
        )]);
        let after = metered(
            &before,
            Inputs {
                records: records(InstanceState::Running),
                traffic_before: traffic(1_000, 1_000),
                ..Default::default()
            },
        );
        assert_eq!(held(&after).rx_bytes, 4_096);
    }

    #[test]
    fn an_app_the_host_no_longer_holds_a_record_of_is_metered_no_further() {
        let before = BTreeMap::from([(
            app_id(),
            UsageMeters {
                running_ms: 60_000,
                ..UsageMeters::default()
            },
        )]);
        let after = metered(
            &before,
            Inputs {
                traffic_after: traffic(9_999, 9_999),
                ..Default::default()
            },
        );
        assert!(after.is_empty());
    }

    #[test]
    fn disk_is_metered_as_what_was_held_multiplied_by_how_long_it_was_held_for() {
        let after = metered(
            &BTreeMap::new(),
            Inputs {
                records: records(InstanceState::Running),
                volumes: volume(VolumeState::Ready, 8 * GIB),
                volume_usage: filled(2 * GIB),
                ..Default::default()
            },
        );
        // 8 GiB and 2 GiB, each held for the five seconds this pass covered.
        assert_eq!(held(&after).disk_provisioned_mib_seconds, 8 * 1024 * 5);
        assert_eq!(held(&after).disk_used_mib_seconds, 2 * 1024 * 5);
    }

    #[test]
    fn a_volume_that_did_not_change_is_still_disk_that_was_held_rather_than_disk_nobody_used() {
        let steady = || Inputs {
            records: records(InstanceState::Running),
            volumes: volume(VolumeState::Ready, 8 * GIB),
            volume_usage: filled(2 * GIB),
            ..Default::default()
        };
        let once = metered(&BTreeMap::new(), steady());
        let twice = metered(&once, steady());
        assert_eq!(
            held(&twice).disk_used_mib_seconds,
            held(&once).disk_used_mib_seconds * 2,
            "two readings that said the same thing are two passes of holding it"
        );
    }

    #[test]
    fn an_app_asleep_is_still_holding_the_disk_it_filled() {
        let after = metered(
            &BTreeMap::new(),
            Inputs {
                records: records(InstanceState::Idle),
                volumes: volume(VolumeState::Ready, 8 * GIB),
                volume_usage: filled(2 * GIB),
                ..Default::default()
            },
        );
        assert_eq!(held(&after).idle_ms, TICK_MS);
        assert_eq!(held(&after).cpu_ms, 0, "an asleep guest spends nothing");
        assert_eq!(held(&after).disk_used_mib_seconds, 2 * 1024 * 5);
        assert_eq!(held(&after).disk_provisioned_mib_seconds, 8 * 1024 * 5);
    }

    #[test]
    fn a_volume_kept_after_its_instance_went_away_is_still_metered() {
        let after = metered(
            &BTreeMap::new(),
            Inputs {
                volumes: volume(VolumeState::Detached, 8 * GIB),
                volume_usage: filled(2 * GIB),
                ..Default::default()
            },
        );
        assert_eq!(held(&after).disk_provisioned_mib_seconds, 8 * 1024 * 5);
        assert_eq!(held(&after).running_ms, 0);
    }

    #[test]
    fn a_volume_holding_nothing_yet_or_nothing_any_more_is_metered_as_nothing() {
        for state in [VolumeState::Pending, VolumeState::Deleted, VolumeState::Failed] {
            let after = metered(
                &BTreeMap::new(),
                Inputs {
                    volumes: volume(state, 8 * GIB),
                    ..Default::default()
                },
            );
            assert!(after.is_empty(), "{state:?} was billed for storage");
        }
    }

    #[test]
    fn a_volume_this_host_never_measured_is_metered_for_what_it_was_given_and_no_more() {
        let after = metered(
            &BTreeMap::new(),
            Inputs {
                records: records(InstanceState::Running),
                volumes: volume(VolumeState::Ready, 8 * GIB),
                ..Default::default()
            },
        );
        assert_eq!(held(&after).disk_provisioned_mib_seconds, 8 * 1024 * 5);
        assert_eq!(
            held(&after).disk_used_mib_seconds,
            0,
            "a guest that has said nothing is not a guest that filled nothing, and is not billed as if it had"
        );
    }

    #[test]
    fn a_level_held_for_no_time_at_all_comes_to_nothing() {
        assert_eq!(held_for(8 * GIB, 0), 0);
        assert_eq!(held_for(0, TICK_MS), 0);
        // A terabyte for a decade, well inside what the counter carries.
        assert_eq!(held_for(1024 * GIB, 1_000), 1_048_576);
        assert!(held_for(1024 * GIB, 1_000) * 315_360_000 < u64::MAX / 1_000);
    }

    #[test]
    fn what_a_guest_spent_is_metered_in_milliseconds_of_the_vcpus_it_was_given() {
        let taken = vec![(
            app_id(),
            GuestReading {
                filesystem: None,
                compute: Some(compute(1_800)),
            },
        )];
        let ticks = BTreeMap::from([(app_id(), compute(1_000))]);

        let after = cpu_metered_after(&BTreeMap::new(), &ticks, &taken);
        assert_eq!(held(&after).cpu_ms, 8_000, "800 ticks of 10ms each");
    }

    #[test]
    fn a_guest_that_rebooted_is_metered_from_what_it_has_spent_since() {
        let taken = vec![(
            app_id(),
            GuestReading {
                filesystem: None,
                compute: Some(compute(30)),
            },
        )];
        let ticks = BTreeMap::from([(app_id(), compute(500_000))]);

        let after = cpu_metered_after(&BTreeMap::new(), &ticks, &taken);
        assert_eq!(held(&after).cpu_ms, 300);
    }

    #[test]
    fn a_guest_that_answered_nothing_leaves_every_meter_where_it_was() {
        let before = BTreeMap::from([(
            app_id(),
            UsageMeters {
                cpu_ms: 12_000,
                running_ms: 60_000,
                ..UsageMeters::default()
            },
        )]);
        let taken = vec![(app_id(), GuestReading::default())];

        let after = cpu_metered_after(&before, &BTreeMap::new(), &taken);
        assert_eq!(held(&after), held(&before));
    }
}
