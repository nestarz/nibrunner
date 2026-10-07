use std::collections::BTreeMap;

use protocol::{AppId, InstanceResources, InstanceState, ReportedMemory};

use crate::domain::report::InstanceRecord;

const BYTES_PER_MIB: u64 = 1_048_576;
const MAX_SAMPLE_AGE_MS: i64 = 5_000;
const MIN_MARGIN_BYTES: u64 = 64 * BYTES_PER_MIB;

pub(crate) type MemoryProfiles = BTreeMap<(AppId, protocol::DeploymentId), MemoryProfile>;
const MAX_PROFILES: usize = 1024;
const PROFILE_REFRESH_MS: i64 = 60 * 60 * 1000;
const PROFILE_LIFETIME_MS: i64 = 30 * 24 * 60 * 60 * 1000;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct MemoryProfile {
    pub app_id: AppId,
    pub deployment_id: protocol::DeploymentId,
    pub resources: InstanceResources,
    pub peak_bytes: u64,
    pub observed_at_ms: i64,
}

impl MemoryProfile {
    pub(crate) fn peak_for(&self, resources: InstanceResources, now_ms: i64) -> Option<u64> {
        (self.resources == resources
            && (0..PROFILE_LIFETIME_MS).contains(&now_ms.saturating_sub(self.observed_at_ms)))
        .then_some(self.peak_bytes)
    }
}

pub(crate) fn prune_profiles(profiles: &mut MemoryProfiles, now_ms: i64) {
    profiles.retain(|_, p| (0..PROFILE_LIFETIME_MS).contains(&now_ms.saturating_sub(p.observed_at_ms)));
    while profiles.len() > MAX_PROFILES {
        let Some(key) = profiles
            .iter()
            .min_by_key(|(_, p)| p.observed_at_ms)
            .map(|(key, _)| key.clone())
        else {
            break;
        };
        profiles.remove(&key);
    }
}

pub(crate) fn remember_profile(profiles: &mut MemoryProfiles, record: &InstanceRecord, now_ms: i64) {
    if let Some(peak_bytes) = record.memory_peak_bytes.filter(|peak| *peak > 0) {
        let key = (record.app_id.clone(), record.deployment_id.clone());
        if profiles.get(&key).is_some_and(|p| {
            p.resources == record.resources
                && p.peak_bytes >= peak_bytes
                && (0..PROFILE_REFRESH_MS).contains(&now_ms.saturating_sub(p.observed_at_ms))
        }) {
            prune_profiles(profiles, now_ms);
            return;
        }
        let peak_bytes = profiles
            .get(&key)
            .filter(|p| p.resources == record.resources)
            .map_or(peak_bytes, |p| p.peak_bytes.max(peak_bytes));
        profiles.insert(
            key,
            MemoryProfile {
                app_id: record.app_id.clone(),
                deployment_id: record.deployment_id.clone(),
                resources: record.resources,
                peak_bytes,
                observed_at_ms: now_ms,
            },
        );
    }
    prune_profiles(profiles, now_ms);
}

pub(crate) fn restore_profile(profiles: &MemoryProfiles, record: &mut InstanceRecord, now_ms: i64) {
    if let Some(profile) = profiles
        .get(&(record.app_id.clone(), record.deployment_id.clone()))
        .filter(|p| {
            p.resources == record.resources
                && (0..PROFILE_LIFETIME_MS).contains(&now_ms.saturating_sub(p.observed_at_ms))
        })
    {
        record.memory_peak_bytes = Some(record.memory_peak_bytes.unwrap_or(0).max(profile.peak_bytes));
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum MemoryOperation {
    Start,
    Snapshot,
}

pub(crate) struct StartupMemory {
    pub deployment_id: protocol::DeploymentId,
    pub resources: InstanceResources,
    pub peak_bytes: Option<u64>,
}

pub(crate) fn target_from_peak(ceiling: u64, peak: u64) -> u64 {
    peak.saturating_add((peak / 4).max(MIN_MARGIN_BYTES)).min(ceiling)
}

pub(crate) struct MemoryReadings {
    pub startup: Option<StartupMemory>,
    pub reservation_generation: u64,
    pub available_bytes: u64,
    pub headroom_bytes: u64,
    pub production_wake_bytes: u64,
    pub measured_at_ms: i64,
    pub apps: BTreeMap<AppId, ReportedMemory>,
    pub external_resident_bytes: BTreeMap<String, u64>,
    pub ceilings: BTreeMap<AppId, u64>,
    pub pool: Option<PoolMemory>,
}

pub(crate) struct PoolMemory {
    pub membership: String,
    pub current_bytes: u64,
    pub reclaimable_file_bytes: u64,
    pub high_bytes: u64,
    pub max_bytes: u64,
    pub all_workloads_contained: bool,
}

impl PoolMemory {
    pub(crate) fn working_set_bytes(&self) -> u64 {
        self.current_bytes.saturating_sub(self.reclaimable_file_bytes)
    }

    pub(crate) fn contains(&self, membership: &str) -> bool {
        std::path::Path::new(membership).starts_with(&self.membership) && membership != self.membership
    }
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

    pub(crate) fn strict_pool_shortfall_mib(
        &self,
        records: &[InstanceRecord],
        reserved_bytes: u64,
        wanted_bytes: u64,
    ) -> u64 {
        let Some(pool) = &self.pool else { return 0 };
        let committed = records
            .iter()
            .filter(|record| super::report::capacity::holds_something(record))
            .fold(reserved_bytes.saturating_add(wanted_bytes), |total, record| {
                total.saturating_add(self.ceiling(&record.app_id, record.resources))
            });
        committed.saturating_sub(pool.max_bytes).div_ceil(BYTES_PER_MIB)
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

    pub(crate) fn observed_peak_bytes(&self, record: &InstanceRecord) -> Option<u64> {
        let memory = self.resident_memory(record)?;
        Some(
            memory
                .peak_bytes
                .unwrap_or(memory.current_bytes)
                .max(memory.current_bytes)
                .max(memory.proportional_set_bytes.unwrap_or(0))
                .saturating_add(memory.swap_bytes),
        )
    }

    pub(crate) fn wake_target_bytes(&self, record: &InstanceRecord) -> u64 {
        let ceiling = self.ceiling(&record.app_id, record.resources).max(
            self.resident_memory(record)
                .and_then(|memory| memory.limits.as_ref())
                .and_then(|limits| limits.max_bytes)
                .unwrap_or(0),
        );
        let Some(peak) = self
            .observed_peak_bytes(record)
            .into_iter()
            .chain(record.memory_peak_bytes)
            .max()
        else {
            return ceiling;
        };
        target_from_peak(ceiling, peak)
    }

    pub(crate) fn wake_headroom_bytes(&self, record: &InstanceRecord, adaptive: bool) -> u64 {
        match (record.state, adaptive) {
            (InstanceState::Idle, false) => self.ceiling(&record.app_id, record.resources),
            (InstanceState::Idle | InstanceState::Frozen, true) => self
                .wake_target_bytes(record)
                .saturating_sub(self.anonymous_resident_bytes(record)),
            _ => 0,
        }
    }

    fn growth_bytes(&self, record: &InstanceRecord) -> u64 {
        let ceiling = self.ceiling(&record.app_id, record.resources);
        let Some(memory) = self.resident_memory(record) else {
            if record.state == InstanceState::Starting && record.memory_peak_bytes.is_some() {
                return self.wake_target_bytes(record);
            }
            return ceiling;
        };
        if record.state == InstanceState::Frozen {
            // A paused VM cannot fault its swapped pages back in until wake or snapshot reserves them.
            return memory
                .proportional_set_bytes
                .unwrap_or(0)
                .saturating_sub(memory.current_bytes);
        }
        self.wake_target_bytes(record)
            .saturating_sub(memory.current_bytes)
    }

    pub(crate) fn anonymous_resident_bytes(&self, record: &InstanceRecord) -> u64 {
        self.resident_memory(record)
            .map(|memory| {
                memory
                    .anonymous_set_bytes
                    .unwrap_or(0)
                    .min(memory.proportional_set_bytes.unwrap_or(0))
                    .min(memory.current_bytes)
            })
            .unwrap_or(0)
    }

    pub(crate) fn shortfall_mib(
        &self,
        capacity_mib: u64,
        records: &[InstanceRecord],
        reserved_bytes: u64,
        resident_reserved_bytes: u64,
        wanted_bytes: u64,
    ) -> u64 {
        let mut future = reserved_bytes
            .saturating_sub(resident_reserved_bytes)
            .saturating_add(wanted_bytes);
        let mut targets = reserved_bytes.saturating_add(wanted_bytes);
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
        let pool_shortfall = self.pool.as_ref().map_or(0, |pool| {
            let budget = pool.high_bytes.min(pool.max_bytes);
            let available = budget.saturating_sub(pool.working_set_bytes());
            future
                .saturating_sub(available)
                .max(targets.saturating_sub(budget))
        });
        physical_shortfall
            .max(budget_shortfall)
            .max(pool_shortfall)
            .div_ceil(BYTES_PER_MIB)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{app_id, instance_record};
    use protocol::{ReportedMemoryLimits, Timestamp};

    fn readings() -> MemoryReadings {
        MemoryReadings {
            startup: None,
            pool: None,
            reservation_generation: 0,
            external_resident_bytes: BTreeMap::new(),
            available_bytes: 2048 * BYTES_PER_MIB,
            headroom_bytes: 1024 * BYTES_PER_MIB,
            production_wake_bytes: 0,
            measured_at_ms: 10_000,
            apps: BTreeMap::from([(
                app_id(),
                ReportedMemory {
                    cgroup: None,
                    proportional_set_bytes: Some(256 * BYTES_PER_MIB),
                    anonymous_set_bytes: Some(256 * BYTES_PER_MIB),
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

    fn pool(current_mib: u64) -> PoolMemory {
        PoolMemory {
            membership: "/workloads.slice".into(),
            current_bytes: current_mib * BYTES_PER_MIB,
            reclaimable_file_bytes: 0,
            high_bytes: 1800 * BYTES_PER_MIB,
            max_bytes: 2048 * BYTES_PER_MIB,
            all_workloads_contained: true,
        }
    }

    #[test]
    fn startup_history_expires_is_bounded_and_never_crosses_resource_changes() {
        let now = PROFILE_LIFETIME_MS * 2;
        let mut profiles = MemoryProfiles::new();
        for index in 0..MAX_PROFILES + 5 {
            let record = crate::test_support::instance_record(|r| {
                r.app_id = AppId::parse(format!("app-{index}")).unwrap();
                r.memory_peak_bytes = Some(128 * BYTES_PER_MIB);
            });
            remember_profile(&mut profiles, &record, now + index as i64);
        }
        assert_eq!(profiles.len(), MAX_PROFILES);
        let mut record = crate::test_support::instance_record(|r| {
            r.app_id = AppId::parse(format!("app-{}", MAX_PROFILES)).unwrap();
        });
        restore_profile(&profiles, &mut record, now + MAX_PROFILES as i64);
        assert_eq!(record.memory_peak_bytes, Some(128 * BYTES_PER_MIB));
        record.memory_peak_bytes = None;
        record.resources.memory_mib += 1;
        restore_profile(&profiles, &mut record, now + MAX_PROFILES as i64);
        assert_eq!(record.memory_peak_bytes, None);
        prune_profiles(&mut profiles, now + PROFILE_LIFETIME_MS + 2048);
        assert!(profiles.is_empty());
    }

    #[test]
    fn parent_usage_limits_admission_even_when_the_host_has_free_memory() {
        let mut observed = readings();
        observed.available_bytes = 8192 * BYTES_PER_MIB;
        observed.pool = Some(pool(1700));
        assert_eq!(observed.shortfall_mib(8192, &[], 0, 0, 256 * BYTES_PER_MIB), 156);
        assert_eq!(observed.shortfall_mib(8192, &[], 0, 0, 512 * BYTES_PER_MIB), 412);
        observed.pool = Some(pool(2200));
        assert_eq!(observed.shortfall_mib(8192, &[], 0, 0, 256 * BYTES_PER_MIB), 256);
    }

    #[test]
    fn clean_inactive_file_cache_does_not_block_admission_or_expand_the_hard_budget() {
        let mut observed = readings();
        observed.available_bytes = 8192 * BYTES_PER_MIB;
        let mut pool = pool(1700);
        pool.reclaimable_file_bytes = 300 * BYTES_PER_MIB;
        observed.pool = Some(pool);
        assert_eq!(observed.shortfall_mib(8192, &[], 0, 0, 256 * BYTES_PER_MIB), 0);
        assert_eq!(observed.shortfall_mib(8192, &[], 0, 0, 512 * BYTES_PER_MIB), 112);
        assert_eq!(
            observed.strict_pool_shortfall_mib(&[], 0, 2304 * BYTES_PER_MIB),
            256
        );
        observed.available_bytes = 1024 * BYTES_PER_MIB;
        assert_eq!(observed.shortfall_mib(8192, &[], 0, 0, 256 * BYTES_PER_MIB), 256);
    }

    #[test]
    fn strict_pool_admission_charges_host_limits_including_vm_overhead() {
        let mut observed = readings();
        observed.pool = Some(pool(256));
        assert_eq!(
            observed.strict_pool_shortfall_mib(&[running()], 0, 256 * BYTES_PER_MIB),
            256
        );
        assert_eq!(
            observed.shortfall_mib(8192, &[running()], 0, 0, 256 * BYTES_PER_MIB),
            0
        );
    }

    #[test]
    fn a_changing_soft_budget_queues_growth_without_changing_the_hard_quota() {
        let mut observed = readings();
        observed.available_bytes = 8192 * BYTES_PER_MIB;
        let mut measured = pool(1024);
        measured.high_bytes = 1280 * BYTES_PER_MIB;
        observed.pool = Some(measured);
        assert_eq!(observed.shortfall_mib(8192, &[], 0, 0, 512 * BYTES_PER_MIB), 256);
        observed.pool.as_mut().unwrap().high_bytes = 1800 * BYTES_PER_MIB;
        assert_eq!(observed.shortfall_mib(8192, &[], 0, 0, 512 * BYTES_PER_MIB), 0);
        assert_eq!(observed.pool.as_ref().unwrap().max_bytes, 2048 * BYTES_PER_MIB);
        assert_eq!(
            observed.strict_pool_shortfall_mib(&[], 0, 2304 * BYTES_PER_MIB),
            256
        );
    }

    #[test]
    fn containment_requires_a_child_group_and_checks_path_components() {
        let pool = pool(0);
        assert!(pool.contains("/workloads.slice/build.slice/compile.service"));
        assert!(!pool.contains("/workloads.slice"));
        assert!(!pool.contains("/workloads.slice-other/app.scope"));
        assert!(!pool.contains("/system.slice/app.scope"));
    }

    #[test]
    fn known_sleeping_revisions_reserve_measured_growth_and_running_apps_are_not_charged_twice() {
        let observed = readings();
        let mut record = running();
        assert_eq!(observed.wake_headroom_bytes(&record, true), 0);
        record.state = InstanceState::Frozen;
        assert_eq!(observed.wake_headroom_bytes(&record, true), 384 * BYTES_PER_MIB);
        assert_eq!(observed.wake_headroom_bytes(&record, false), 0);
        record.state = InstanceState::Idle;
        assert_eq!(observed.wake_headroom_bytes(&record, true), 2048 * BYTES_PER_MIB);
        record.memory_peak_bytes = Some(512 * BYTES_PER_MIB);
        assert_eq!(observed.wake_headroom_bytes(&record, true), 640 * BYTES_PER_MIB);
        assert_eq!(observed.wake_headroom_bytes(&record, false), 2048 * BYTES_PER_MIB);
    }

    #[test]
    fn resident_build_memory_reduces_future_growth_but_not_the_reserved_budget() {
        let observed = readings();
        assert_eq!(
            observed.shortfall_mib(8192, &[], 1536 * BYTES_PER_MIB, 0, 512 * BYTES_PER_MIB),
            1024
        );
        assert_eq!(
            observed.shortfall_mib(
                8192,
                &[],
                1536 * BYTES_PER_MIB,
                1024 * BYTES_PER_MIB,
                512 * BYTES_PER_MIB
            ),
            0
        );
        assert_eq!(
            observed.shortfall_mib(
                1024,
                &[],
                1536 * BYTES_PER_MIB,
                1024 * BYTES_PER_MIB,
                512 * BYTES_PER_MIB
            ),
            1024
        );
    }

    #[test]
    fn a_running_app_reserves_its_peak_margin_without_counting_resident_memory_twice() {
        let observed = readings();
        assert_eq!(observed.growth_bytes(&running()), 384 * BYTES_PER_MIB);
        assert_eq!(
            observed.shortfall_mib(8192, &[running()], 0, 0, 512 * BYTES_PER_MIB),
            0
        );
        assert_eq!(
            observed.shortfall_mib(8192, &[running()], 0, 0, 1024 * BYTES_PER_MIB),
            384
        );
    }

    #[test]
    fn swaps_reservations_and_host_headroom_are_not_free_capacity() {
        let mut observed = readings();
        observed.apps.get_mut(&app_id()).unwrap().swap_bytes = 256 * BYTES_PER_MIB;
        assert_eq!(observed.growth_bytes(&running()), 704 * BYTES_PER_MIB);
        assert_eq!(
            observed.shortfall_mib(8192, &[running()], 512 * BYTES_PER_MIB, 0, 512 * BYTES_PER_MIB),
            704
        );
    }

    #[test]
    fn a_known_boot_keeps_its_startup_target_until_fresh_running_measurements_arrive() {
        let mut observed = readings();
        observed.available_bytes = 8192 * BYTES_PER_MIB;
        observed.pool = Some(pool(300));
        let mut record = running();
        record.state = InstanceState::Starting;
        record.memory_peak_bytes = Some(512 * BYTES_PER_MIB);
        assert_eq!(observed.growth_bytes(&record), 640 * BYTES_PER_MIB);
        assert_eq!(observed.anonymous_resident_bytes(&record), 0);
        assert_eq!(
            observed.shortfall_mib(8192, &[record.clone()], 640 * BYTES_PER_MIB, 0, 0),
            0
        );
        record.memory_peak_bytes = None;
        assert_eq!(observed.growth_bytes(&record), 2048 * BYTES_PER_MIB);
        assert_eq!(
            observed.shortfall_mib(8192, &[record], 640 * BYTES_PER_MIB, 0, 0),
            1188
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
            readings().shortfall_mib(8192, &[record], 0, 0, 1024 * BYTES_PER_MIB),
            0
        );
    }

    #[test]
    fn reclaimable_pages_cannot_make_targets_exceed_the_host_guest_budget() {
        let mut observed = readings();
        observed.available_bytes = 8192 * BYTES_PER_MIB;
        assert_eq!(
            observed.shortfall_mib(1024, &[running()], 0, 0, 512 * BYTES_PER_MIB),
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
            observed.shortfall_mib(8192, &[record.clone()], 0, 0, 1024 * BYTES_PER_MIB),
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
