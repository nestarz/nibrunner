use std::collections::BTreeMap;

use protocol::{AppId, InstanceResources, InstanceState, ReportedMemory};

use crate::domain::report::InstanceRecord;

const BYTES_PER_MIB: u64 = 1_048_576;
const MAX_SAMPLE_AGE_MS: i64 = 5_000;
const MIN_MARGIN_BYTES: u64 = 64 * BYTES_PER_MIB;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum MemoryOperation {
    Start,
    Snapshot,
}

pub(crate) struct MemoryReadings {
    pub reservation_generation: u64,
    pub available_bytes: u64,
    pub headroom_bytes: u64,
    pub measured_at_ms: i64,
    pub apps: BTreeMap<AppId, ReportedMemory>,
    pub ceilings: BTreeMap<AppId, u64>,
}

impl MemoryReadings {
    pub(crate) fn is_fresh(&self, now_ms: i64) -> bool {
        now_ms
            .checked_sub(self.measured_at_ms)
            .is_some_and(|age| (0..=MAX_SAMPLE_AGE_MS).contains(&age))
    }

    pub(crate) fn ceiling(&self, app: &AppId, resources: InstanceResources) -> u64 {
        self.ceilings
            .get(app)
            .copied()
            .unwrap_or_else(|| (u64::from(resources.memory_mib) + 64) * BYTES_PER_MIB)
    }

    fn resident_memory(&self, record: &InstanceRecord) -> Option<&ReportedMemory> {
        if !matches!(record.state, InstanceState::Running | InstanceState::Frozen) {
            return None;
        }
        let memory = self.apps.get(&record.app_id)?;
        let age = self.measured_at_ms.checked_sub(memory.measured_at.epoch_ms())?;
        if !(0..=MAX_SAMPLE_AGE_MS).contains(&age)
            || memory.measured_at.epoch_ms() < record.started_at.as_ref()?.epoch_ms()
            || memory.limits.as_ref()?.max_bytes.is_none()
            || memory.proportional_set_bytes.is_none()
        {
            return None;
        }
        Some(memory)
    }

    fn growth_bytes(&self, record: &InstanceRecord) -> u64 {
        let ceiling = self.ceiling(&record.app_id, record.resources);
        let Some(memory) = self.resident_memory(record) else {
            return ceiling;
        };
        if record.state == InstanceState::Frozen {
            // A paused VM cannot fault its swapped pages back in until wake or snapshot reserves them.
            return memory
                .proportional_set_bytes
                .unwrap_or(0)
                .saturating_sub(memory.current_bytes);
        }
        let ceiling = ceiling.max(
            memory
                .limits
                .as_ref()
                .and_then(|limits| limits.max_bytes)
                .unwrap_or(ceiling),
        );
        // Swapped pages can fault back in. Compressed swap is never extra physical capacity.
        let peak = memory
            .peak_bytes
            .unwrap_or(memory.current_bytes)
            .max(memory.current_bytes)
            .max(memory.proportional_set_bytes.unwrap_or(0))
            .saturating_add(memory.swap_bytes);
        let target = peak.saturating_add((peak / 4).max(MIN_MARGIN_BYTES)).min(ceiling);
        target.saturating_sub(memory.current_bytes)
    }

    pub(crate) fn shortfall_mib(
        &self,
        capacity_mib: u64,
        records: &[InstanceRecord],
        reserved_bytes: u64,
        wanted_bytes: u64,
    ) -> u64 {
        let mut future = reserved_bytes.saturating_add(wanted_bytes);
        let mut targets = future;
        for record in records
            .iter()
            .filter(|record| super::report::capacity::holds_something(record))
        {
            let growth = self.growth_bytes(record);
            future = future.saturating_add(growth);
            targets = targets.saturating_add(growth).saturating_add(
                self.resident_memory(record)
                    .map_or(0, |memory| memory.current_bytes),
            );
        }
        let physical_shortfall = future
            .saturating_add(self.headroom_bytes)
            .saturating_sub(self.available_bytes);
        let budget_shortfall = targets.saturating_sub(capacity_mib.saturating_mul(BYTES_PER_MIB));
        physical_shortfall.max(budget_shortfall).div_ceil(BYTES_PER_MIB)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{app_id, instance_record};
    use protocol::{ReportedMemoryLimits, Timestamp};

    fn readings() -> MemoryReadings {
        MemoryReadings {
            reservation_generation: 0,
            available_bytes: 2048 * BYTES_PER_MIB,
            headroom_bytes: 1024 * BYTES_PER_MIB,
            measured_at_ms: 10_000,
            apps: BTreeMap::from([(
                app_id(),
                ReportedMemory {
                    cgroup: None,
                    proportional_set_bytes: Some(256 * BYTES_PER_MIB),
                    measured_at: Timestamp::from_epoch_ms(10_000),
                    limits: Some(ReportedMemoryLimits {
                        low_bytes: 0,
                        high_bytes: None,
                        max_bytes: Some(2048 * BYTES_PER_MIB),
                        swap_max_bytes: None,
                    }),
                    current_bytes: 256 * BYTES_PER_MIB,
                    peak_bytes: Some(512 * BYTES_PER_MIB),
                    swap_bytes: 0,
                    high_events: 0,
                    oom_kills: 0,
                    pressure_some_us: 0,
                    pressure_full_us: 0,
                },
            )]),
            ceilings: BTreeMap::from([(app_id(), 2048 * BYTES_PER_MIB)]),
        }
    }

    fn running() -> InstanceRecord {
        instance_record(|record| {
            record.state = InstanceState::Running;
            record.started_at = Some(Timestamp::from_epoch_ms(1000));
        })
    }

    #[test]
    fn a_running_app_reserves_its_peak_margin_without_counting_resident_memory_twice() {
        let observed = readings();
        assert_eq!(observed.growth_bytes(&running()), 384 * BYTES_PER_MIB);
        assert_eq!(
            observed.shortfall_mib(8192, &[running()], 0, 512 * BYTES_PER_MIB),
            0
        );
        assert_eq!(
            observed.shortfall_mib(8192, &[running()], 0, 1024 * BYTES_PER_MIB),
            384
        );
    }

    #[test]
    fn swaps_reservations_and_host_headroom_are_not_free_capacity() {
        let mut observed = readings();
        observed.apps.get_mut(&app_id()).unwrap().swap_bytes = 256 * BYTES_PER_MIB;
        assert_eq!(observed.growth_bytes(&running()), 704 * BYTES_PER_MIB);
        assert_eq!(
            observed.shortfall_mib(8192, &[running()], 512 * BYTES_PER_MIB, 512 * BYTES_PER_MIB),
            704
        );
    }

    #[test]
    fn absent_stale_future_unbounded_or_previous_process_samples_reserve_the_ceiling() {
        for case in 0..6 {
            let mut observed = readings();
            let memory = observed.apps.get_mut(&app_id()).unwrap();
            match case {
                0 => {
                    observed.apps.clear();
                }
                1 => memory.measured_at = Timestamp::from_epoch_ms(4999),
                2 => memory.measured_at = Timestamp::from_epoch_ms(10_001),
                3 => memory.limits = None,
                4 => memory.proportional_set_bytes = None,
                _ => memory.measured_at = Timestamp::from_epoch_ms(999),
            }
            assert_eq!(observed.growth_bytes(&running()), 2048 * BYTES_PER_MIB);
        }
    }

    #[test]
    fn booting_apps_reserve_the_ceiling_even_when_their_first_pages_have_been_measured() {
        let mut record = running();
        record.state = InstanceState::Starting;
        assert_eq!(readings().growth_bytes(&record), 2048 * BYTES_PER_MIB);
        record.state = InstanceState::Idle;
        assert_eq!(
            readings().shortfall_mib(8192, &[record], 0, 1024 * BYTES_PER_MIB),
            0
        );
    }

    #[test]
    fn reclaimable_pages_cannot_make_targets_exceed_the_host_guest_budget() {
        let mut observed = readings();
        observed.available_bytes = 8192 * BYTES_PER_MIB;
        assert_eq!(
            observed.shortfall_mib(1024, &[running()], 0, 512 * BYTES_PER_MIB),
            128
        );
    }

    #[test]
    fn a_lower_desired_ceiling_does_not_hide_a_still_running_larger_budget() {
        let mut observed = readings();
        observed.ceilings.insert(app_id(), 512 * BYTES_PER_MIB);
        assert_eq!(observed.growth_bytes(&running()), 384 * BYTES_PER_MIB);
    }

    #[test]
    fn a_host_sample_that_waited_too_long_for_admission_is_no_longer_usable() {
        let observed = readings();
        assert!(observed.is_fresh(15_000));
        assert!(!observed.is_fresh(15_001));
        assert!(!observed.is_fresh(9_999));
    }

    #[test]
    fn frozen_working_sets_do_not_reserve_swapped_pages_until_a_wake_or_snapshot() {
        let mut observed = readings();
        let mut record = running();
        record.state = InstanceState::Frozen;
        observed.apps.get_mut(&app_id()).unwrap().swap_bytes = 1024 * BYTES_PER_MIB;
        assert_eq!(observed.growth_bytes(&record), 0);
        assert_eq!(
            observed.shortfall_mib(8192, &[record.clone()], 0, 1024 * BYTES_PER_MIB),
            0
        );
        observed.apps.get_mut(&app_id()).unwrap().proportional_set_bytes = Some(512 * BYTES_PER_MIB);
        assert_eq!(observed.growth_bytes(&record), 256 * BYTES_PER_MIB);
        observed.apps.clear();
        assert_eq!(observed.growth_bytes(&record), 2048 * BYTES_PER_MIB);
    }

    #[test]
    fn snapshot_mappings_charged_elsewhere_still_reserve_their_resident_working_set() {
        let mut observed = readings();
        observed.apps.get_mut(&app_id()).unwrap().proportional_set_bytes = Some(1024 * BYTES_PER_MIB);
        assert_eq!(observed.growth_bytes(&running()), 1024 * BYTES_PER_MIB);
    }
}
