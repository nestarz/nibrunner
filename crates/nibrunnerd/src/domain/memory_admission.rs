use std::collections::BTreeMap;

use protocol::{AppId, InstanceResources, InstanceState, ReportedMemory};

use crate::domain::report::InstanceRecord;

const BYTES_PER_MIB: u64 = 1_048_576;
const MAX_SAMPLE_AGE_MS: i64 = 5_000;
const MIN_MARGIN_BYTES: u64 = 64 * BYTES_PER_MIB;

/// What a start or a wake is measured against: what the host has available now, what each
/// running VM has been seen to use, and what each VM may grow to.
pub(crate) struct MemoryReadings {
    pub available_bytes: u64,
    pub headroom_bytes: u64,
    pub measured_at_ms: i64,
    pub apps: BTreeMap<AppId, ReportedMemory>,
}

impl MemoryReadings {
    /// The most a VM of these resources can make its cgroup charge: the ceiling the jailer gives it.
    pub(crate) fn ceiling(resources: InstanceResources) -> u64 {
        crate::adapters::vm::limits::memory_max_bytes(resources.memory_mib)
    }

    fn running_memory(&self, record: &InstanceRecord) -> Option<&ReportedMemory> {
        if record.state != InstanceState::Running {
            return None;
        }
        let memory = self.apps.get(&record.app_id)?;
        let age = self.measured_at_ms.checked_sub(memory.measured_at.epoch_ms())?;
        if !(0..=MAX_SAMPLE_AGE_MS).contains(&age)
            || memory.measured_at.epoch_ms() < record.started_at.as_ref()?.epoch_ms()
            || memory.limits.as_ref()?.max_bytes.is_none()
        {
            return None;
        }
        Some(memory)
    }

    fn growth_bytes(&self, record: &InstanceRecord) -> u64 {
        let ceiling = Self::ceiling(record.resources);
        let Some(memory) = self.running_memory(record) else {
            return ceiling;
        };
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
            .saturating_add(memory.swap_bytes);
        let target = peak.saturating_add((peak / 4).max(MIN_MARGIN_BYTES)).min(ceiling);
        target.saturating_sub(memory.current_bytes)
    }

    /// What the host is short of growing every VM that holds memory to its measured target and
    /// starting one more of `wanted_bytes`, beside `capacity_mib` and what is available now.
    pub(crate) fn shortfall_mib(
        &self,
        capacity_mib: u64,
        records: &[InstanceRecord],
        wanted_bytes: u64,
    ) -> u64 {
        let mut future = wanted_bytes;
        let mut targets = future;
        for record in records
            .iter()
            .filter(|record| super::report::capacity::holds_something(record))
        {
            let growth = self.growth_bytes(record);
            future = future.saturating_add(growth);
            targets = targets.saturating_add(growth).saturating_add(
                self.running_memory(record)
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

    const CEILING_MIB: u64 = 2048;

    fn readings() -> MemoryReadings {
        MemoryReadings {
            available_bytes: 2048 * BYTES_PER_MIB,
            headroom_bytes: 1024 * BYTES_PER_MIB,
            measured_at_ms: 10_000,
            apps: BTreeMap::from([(
                app_id(),
                ReportedMemory {
                    measured_at: Timestamp::from_epoch_ms(10_000),
                    cgroup: None,
                    proportional_set_bytes: None,
                    anonymous_set_bytes: None,
                    limits: Some(ReportedMemoryLimits {
                        low_bytes: 0,
                        high_bytes: None,
                        max_bytes: Some(CEILING_MIB * BYTES_PER_MIB),
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
        assert_eq!(observed.shortfall_mib(8192, &[running()], 512 * BYTES_PER_MIB), 0);
        assert_eq!(
            observed.shortfall_mib(8192, &[running()], 1024 * BYTES_PER_MIB),
            384
        );
    }

    #[test]
    fn swapped_pages_and_host_headroom_are_not_free_capacity() {
        let mut observed = readings();
        observed.apps.get_mut(&app_id()).unwrap().swap_bytes = 256 * BYTES_PER_MIB;
        assert_eq!(observed.growth_bytes(&running()), 704 * BYTES_PER_MIB);
        assert_eq!(
            observed.shortfall_mib(8192, &[running()], 1024 * BYTES_PER_MIB),
            704
        );
    }

    #[test]
    fn absent_stale_future_unbounded_or_previous_process_samples_reserve_the_ceiling() {
        let ceiling = MemoryReadings::ceiling(running().resources);
        for case in 0..5 {
            let mut observed = readings();
            let memory = observed.apps.get_mut(&app_id()).unwrap();
            match case {
                0 => {
                    observed.apps.clear();
                }
                1 => memory.measured_at = Timestamp::from_epoch_ms(4999),
                2 => memory.measured_at = Timestamp::from_epoch_ms(10_001),
                3 => memory.limits = None,
                _ => memory.measured_at = Timestamp::from_epoch_ms(999),
            }
            assert_eq!(observed.growth_bytes(&running()), ceiling, "case {case}");
        }
    }

    #[test]
    fn booting_apps_reserve_the_ceiling_even_when_their_first_pages_have_been_measured() {
        let mut record = running();
        record.state = InstanceState::Starting;
        assert_eq!(
            readings().growth_bytes(&record),
            MemoryReadings::ceiling(record.resources)
        );
        record.state = InstanceState::Idle;
        assert_eq!(readings().shortfall_mib(8192, &[record], 1024 * BYTES_PER_MIB), 0);
    }

    #[test]
    fn reclaimable_pages_cannot_make_targets_exceed_the_host_guest_budget() {
        let mut observed = readings();
        observed.available_bytes = 8192 * BYTES_PER_MIB;
        assert_eq!(
            observed.shortfall_mib(1024, &[running()], 512 * BYTES_PER_MIB),
            128
        );
    }
}
