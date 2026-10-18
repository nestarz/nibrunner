use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

use nft_render::AppTraffic;
use protocol::{
    AppId, ComputeUsage, FilesystemUsage, ReportedVolume, Revision, Sha256Digest, StateMessage, UsageMeters,
    VolumeId,
};
use tokio::sync::{Notify, OwnedMutexGuard, RwLock};

use crate::domain::backoff::AttemptWindow;
use crate::domain::metrics::converge::Deploy;
use crate::domain::report::InstanceRecord;

#[derive(Debug, Default, Clone)]
pub struct HostSnapshot {
    pub records: BTreeMap<AppId, InstanceRecord>,
    pub(crate) memory_profiles: crate::domain::memory_admission::MemoryProfiles,
    pub(crate) memory_pressure: crate::domain::reconcile::pressure::PressureState,
    // When each app was last asked to freeze, so a refusal is not retried every pass.
    pub(crate) freeze_attempted_at_ms: BTreeMap<AppId, i64>,
    // Only ever this daemon's own account of what it set out on: a restart starts each over,
    // and so is measured as one.
    pub deploys: BTreeMap<AppId, Deploy>,
    pub deleted_volumes: BTreeMap<VolumeId, ReportedVolume>,
    pub volume_reports: Vec<ReportedVolume>,
    pub checkpoint_reports: Vec<protocol::ReportedCheckpoint>,
    pub export_reports: Vec<protocol::ReportedExport>,
    pub next_probe_at_ms: BTreeMap<AppId, i64>,
    pub snapshotting: BTreeSet<AppId>,
    // Each app whose last sleep did not happen and that has been due ever since. Only ever this
    // daemon's own note, so a restart tries every app afresh.
    pub failed_sleeps: BTreeMap<AppId, AttemptWindow>,
    pub app_traffic: BTreeMap<AppId, AppTraffic>,
    pub last_active_at_ms: BTreeMap<AppId, i64>,
    // When each app's traffic counters were last read. Only ever this daemon's own note of what
    // it watched, so a restart has measured nothing and must take a reading before it may call
    // an app quiet.
    pub last_measured_at_ms: BTreeMap<AppId, i64>,
    pub volume_usage: BTreeMap<AppId, FilesystemUsage>,
    pub compute_usage: BTreeMap<AppId, ComputeUsage>,
    pub compute_ticks: BTreeMap<AppId, guest_contract::filesystem::MeasuredCompute>,
    pub meters: BTreeMap<AppId, UsageMeters>,
    // Only ever this daemon's own note of when it last metered, so a restart credits nothing
    // for the stretch it was not there to watch.
    pub metered_at_ms: Option<i64>,
    pub converged: bool,
    pub deferred_work: bool,
    pub isolated: bool,
    // The document this host took up, by the digest of the bytes it was read from, and the
    // refusal the last one was met with. Both are surfaced at the top of `reported.json`, so a
    // control plane reading only that file sees which of its writes this host is on and why a
    // later one did not land.
    pub accepted_digest: Option<Sha256Digest>,
    pub accepted_revision: Option<Revision>,
    pub desired_refusal: Option<StateMessage>,
}

pub type SharedState = Arc<HostState>;

type Transitions = BTreeMap<AppId, Arc<tokio::sync::Mutex<()>>>;
type CronRuns = Arc<Mutex<BTreeMap<AppId, usize>>>;

pub struct CronActivity {
    runs: CronRuns,
    app_id: AppId,
}

impl Drop for CronActivity {
    fn drop(&mut self) {
        let mut runs = self.runs.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(count) = runs.get_mut(&self.app_id) {
            *count -= 1;
            if *count == 0 {
                runs.remove(&self.app_id);
            }
        }
    }
}

#[derive(Clone)]
struct ReservedMemory {
    resources: protocol::InstanceResources,
    bytes: u64,
}

#[derive(Default)]
struct MemoryReservations {
    // Moves with every reservation taken or given back, so that readings taken before one cannot
    // discount what it holds.
    generation: u64,
    held: BTreeMap<AppId, ReservedMemory>,
}

type SharedReservations = Arc<Mutex<MemoryReservations>>;

/// Memory a start, a wake or a snapshot holds until its instance record accounts for it. Two of
/// them never admit against the same room.
pub(crate) struct MemoryReservation {
    app_id: AppId,
    reservations: SharedReservations,
}

impl Drop for MemoryReservation {
    fn drop(&mut self) {
        let mut reservations = self
            .reservations
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        reservations.held.remove(&self.app_id);
        reservations.generation = reservations.generation.wrapping_add(1);
    }
}

pub struct HostState {
    snapshot: RwLock<HostSnapshot>,
    refresh: Notify,
    report: Notify,
    transitions: Mutex<Transitions>,
    cron_runs: CronRuns,
    pub(crate) persistence: tokio::sync::Mutex<()>,
    memory_reservations: SharedReservations,
    /// Held by whatever is putting quiet apps to sleep to make room, so that two of them do not
    /// pick the same victims.
    pub(crate) reclaim: tokio::sync::Mutex<()>,
}

impl HostState {
    pub fn shared() -> SharedState {
        Arc::new(Self {
            snapshot: RwLock::new(HostSnapshot::default()),
            refresh: Notify::new(),
            report: Notify::new(),
            transitions: Mutex::new(BTreeMap::new()),
            cron_runs: Arc::new(Mutex::new(BTreeMap::new())),
            persistence: tokio::sync::Mutex::new(()),
            memory_reservations: Arc::default(),
            reclaim: tokio::sync::Mutex::new(()),
        })
    }

    pub(crate) fn memory_generation(&self) -> u64 {
        self.memory_reservations
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .generation
    }

    /// Whether anything other than `app` is starting or holds a reservation: something whose
    /// memory may settle soon enough for a wake to wait for it.
    pub(crate) async fn other_memory_transitions(&self, app: &AppId) -> bool {
        let snapshot = self.snapshot.read().await;
        if snapshot
            .records
            .values()
            .any(|record| &record.app_id != app && record.state == protocol::InstanceState::Starting)
        {
            return true;
        }
        self.memory_reservations
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .held
            .keys()
            .any(|held| held != app)
    }

    /// Called under the app's transition lock. Admits `wanted` beside every record that holds memory
    /// and every reservation not yet accounted for by one, strictly against ceilings and, when
    /// `measured` is given, against what is measured too: `adaptive` decides by the measurement,
    /// `observe` only logs it, and a pool bounds both.
    pub(crate) async fn reserve_memory(
        &self,
        capacity_mib: u64,
        app_id: &AppId,
        wanted: protocol::InstanceResources,
        operation: crate::domain::memory_admission::MemoryOperation,
        measured: Option<(
            crate::config::MemoryAdmissionMode,
            crate::domain::memory_admission::MemoryReadings,
        )>,
    ) -> Result<MemoryReservation, u64> {
        use crate::config::MemoryAdmissionMode;
        use crate::domain::memory_admission::MemoryOperation;
        let snapshot = self.snapshot.read().await;
        let mut reservations = self
            .memory_reservations
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let records: Vec<_> = snapshot
            .records
            .values()
            .filter(|record| &record.app_id != app_id && !reservations.held.contains_key(&record.app_id))
            .cloned()
            .collect();
        let mut committed = crate::domain::report::capacity::committed_resources(&records);
        committed.extend(reservations.held.values().map(|reserved| reserved.resources));
        let strict_shortfall = match operation {
            MemoryOperation::Start => {
                crate::domain::report::capacity::memory_shortfall_mib(capacity_mib, &committed, &wanted)
            }
            // Strict admission already charged this VM's whole ceiling; sleeping it gives that back.
            MemoryOperation::Snapshot => 0,
        };
        let mut shortfall = strict_shortfall;
        let mut bytes = crate::adapters::vm::limits::memory_max_bytes(wanted.memory_mib);
        if measured.as_ref().is_some_and(|(mode, readings)| {
            (*mode == MemoryAdmissionMode::Adaptive || readings.pool.is_some())
                && !readings.is_fresh(crate::clock::now_ms())
        }) {
            // A reading that waited this long for the lock may no longer describe the host.
            return Err(bytes.div_ceil(1_048_576));
        }
        if let Some((mode, mut readings)) = measured {
            if readings.reservation_generation != reservations.generation {
                // A reservation taken or given back since the reading: its working sets no longer
                // stand for what is held.
                readings.apps.clear();
                if let Some(pool) = &mut readings.pool {
                    pool.all_workloads_contained = false;
                }
            }
            bytes = readings.ceiling(app_id, wanted);
            let adaptive = mode == MemoryAdmissionMode::Adaptive
                && readings
                    .pool
                    .as_ref()
                    .is_none_or(|pool| pool.all_workloads_contained);
            if adaptive
                && readings.reservation_generation == reservations.generation
                && operation == MemoryOperation::Start
            {
                let current = snapshot
                    .records
                    .get(app_id)
                    .filter(|record| {
                        record.resources == wanted
                            && readings.startup.as_ref().is_none_or(|startup| {
                                startup.deployment_id == record.deployment_id && startup.resources == wanted
                            })
                            && matches!(
                                record.state,
                                protocol::InstanceState::Pending
                                    | protocol::InstanceState::Starting
                                    | protocol::InstanceState::Idle
                                    | protocol::InstanceState::Running
                            )
                    })
                    .map(|record| readings.wake_target_bytes(record));
                let historical = readings
                    .startup
                    .as_ref()
                    .filter(|startup| startup.resources == wanted)
                    .and_then(|startup| startup.peak_bytes)
                    .map(|peak| {
                        crate::domain::memory_admission::target_from_peak(
                            readings.ceiling(app_id, wanted),
                            peak,
                        )
                    });
                bytes = current.into_iter().chain(historical).max().unwrap_or(bytes);
            }
            let reserved_bytes = reservations
                .held
                .values()
                .fold(0u64, |total, held| total.saturating_add(held.bytes));
            let resident_reserved_bytes = reservations.held.iter().fold(0u64, |total, (held_app, held)| {
                let resident = snapshot
                    .records
                    .get(held_app)
                    .map_or(0, |record| readings.anonymous_resident_bytes(record))
                    .min(held.bytes);
                total.saturating_add(resident)
            });
            let wanted_resident_bytes = snapshot
                .records
                .get(app_id)
                .map_or(0, |record| readings.anonymous_resident_bytes(record))
                .min(bytes);
            let measured_shortfall = readings.shortfall_mib(
                capacity_mib,
                &records,
                reserved_bytes.saturating_add(bytes),
                resident_reserved_bytes.saturating_add(wanted_resident_bytes),
                0,
            );
            tracing::info!(
                %app_id,
                ?mode,
                adaptive,
                reservation_mib = bytes.div_ceil(1_048_576),
                strict_shortfall_mib = strict_shortfall,
                measured_shortfall_mib = measured_shortfall,
                "memory admission evaluated"
            );
            if adaptive {
                shortfall = measured_shortfall;
            } else if readings.pool.is_some() {
                shortfall = strict_shortfall
                    .max(measured_shortfall)
                    .max(readings.strict_pool_shortfall_mib(&records, reserved_bytes, bytes));
            }
        }
        if shortfall > 0 {
            return Err(shortfall);
        }
        reservations.held.insert(
            app_id.clone(),
            ReservedMemory {
                resources: wanted,
                bytes,
            },
        );
        reservations.generation = reservations.generation.wrapping_add(1);
        Ok(MemoryReservation {
            app_id: app_id.clone(),
            reservations: self.memory_reservations.clone(),
        })
    }

    /// What the last runs of this deployment were measured to need, for a start that has nothing
    /// running to measure yet.
    pub(crate) async fn startup_memory(
        &self,
        desired: &protocol::DesiredInstance,
    ) -> crate::domain::memory_admission::StartupMemory {
        let snapshot = self.snapshot.read().await;
        let peak_bytes = snapshot
            .memory_profiles
            .get(&(desired.app_id.clone(), desired.deployment_id.clone()))
            .and_then(|profile| profile.peak_for(desired.config.resources, crate::clock::now_ms()));
        crate::domain::memory_admission::StartupMemory {
            deployment_id: desired.deployment_id.clone(),
            resources: desired.config.resources,
            peak_bytes,
        }
    }

    pub(crate) fn try_transition(&self, app_id: &AppId) -> Option<OwnedMutexGuard<()>> {
        let lock = {
            let mut transitions = self
                .transitions
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            transitions.retain(|_, lock| Arc::strong_count(lock) > 1);
            transitions.entry(app_id.clone()).or_default().clone()
        };
        lock.try_lock_owned().ok()
    }

    /// The lock a sleep or a wake of this app holds for as long as it is moving the microVM.
    /// One transition of an app at a time, so that neither finds the other half way through a
    /// snapshot; and each app's own, so that the sleeps a pass runs side by side wait on nothing
    /// but the disk.
    pub async fn transition(&self, app_id: &AppId) -> OwnedMutexGuard<()> {
        self.transition_lock(app_id).lock_owned().await
    }

    /// The transition lock when nothing holds it: a pass that would only tidy up after an app
    /// leaves one mid-sleep or mid-wake to the next pass rather than waiting on it.
    pub(crate) fn try_transition(&self, app_id: &AppId) -> Option<OwnedMutexGuard<()>> {
        self.transition_lock(app_id).try_lock_owned().ok()
    }

    fn transition_lock(&self, app_id: &AppId) -> Arc<tokio::sync::Mutex<()>> {
        let mut transitions = self
            .transitions
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        // A guard and a waiter each hold a reference, so an app's lock is kept only while
        // something is at it, and the map does not grow by one for every app ever moved.
        transitions.retain(|_, lock| Arc::strong_count(lock) > 1);
        transitions.entry(app_id.clone()).or_default().clone()
    }

    pub async fn snapshot(&self) -> HostSnapshot {
        self.snapshot.read().await.clone()
    }

    pub fn cron_activity(&self, app_id: &AppId) -> CronActivity {
        let mut runs = self
            .cron_runs
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        *runs.entry(app_id.clone()).or_default() += 1;
        CronActivity {
            runs: self.cron_runs.clone(),
            app_id: app_id.clone(),
        }
    }

    pub fn cron_running(&self, app_id: &AppId) -> bool {
        self.cron_runs
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .contains_key(app_id)
    }

    pub async fn records(&self) -> Vec<InstanceRecord> {
        self.snapshot.read().await.records.values().cloned().collect()
    }

    pub async fn record(&self, app_id: &AppId) -> Option<InstanceRecord> {
        self.snapshot.read().await.records.get(app_id).cloned()
    }

    /// The snapshot held for writing, for a decision that must not race what it decides on.
    pub(crate) async fn locked_snapshot(&self) -> tokio::sync::RwLockWriteGuard<'_, HostSnapshot> {
        self.snapshot.write().await
    }

    pub async fn modify<T>(&self, change: impl FnOnce(&mut HostSnapshot) -> T) -> T {
        change(&mut *self.snapshot.write().await)
    }

    pub async fn put_record(&self, mut record: InstanceRecord) {
        let mut snapshot = self.snapshot.write().await;
        if let Some(previous) = snapshot.records.get(&record.app_id) {
            if previous.deployment_id == record.deployment_id
                && record.expiry.is_some()
                && previous.expired_at_ms.is_some()
            {
                record.expired_at_ms = previous.expired_at_ms;
                record.state = protocol::InstanceState::Expired;
            }
        }
        let now = crate::clock::now_ms();
        crate::domain::memory_admission::restore_profile(&snapshot.memory_profiles, &mut record, now);
        crate::domain::memory_admission::remember_profile(&mut snapshot.memory_profiles, &record, now);
        snapshot.records.insert(record.app_id.clone(), record);
    }

    pub async fn update_record(&self, app_id: &AppId, change: impl FnOnce(&mut InstanceRecord)) {
        let mut snapshot = self.snapshot.write().await;
        let HostSnapshot {
            records,
            memory_profiles,
            ..
        } = &mut *snapshot;
        if let Some(record) = records.get_mut(app_id) {
            let terminal = record.expired_at_ms;
            let deployment = record.deployment_id.clone();
            change(record);
            if terminal.is_some() && record.expiry.is_some() && record.deployment_id == deployment {
                record.expired_at_ms = terminal;
                record.state = protocol::InstanceState::Expired;
            }
            crate::domain::memory_admission::remember_profile(
                memory_profiles,
                record,
                crate::clock::now_ms(),
            );
        }
    }

    pub async fn drop_record(&self, app_id: &AppId) {
        let mut snapshot = self.snapshot.write().await;
        if let Some(record) = snapshot.records.remove(app_id) {
            crate::domain::memory_admission::remember_profile(
                &mut snapshot.memory_profiles,
                &record,
                crate::clock::now_ms(),
            );
        }
    }

    /// Admit and count the request under the same lock that makes expiry terminal.
    pub(crate) async fn admit<T>(&self, app_id: &AppId, now_ms: i64, open: impl FnOnce() -> T) -> Option<T> {
        let mut snapshot = self.snapshot.write().await;
        if snapshot.records.get(app_id).is_some_and(|r| {
            r.expired_at_ms.is_some()
                || r.expiry
                    .as_ref()
                    .is_some_and(|policy| policy.deadline_reached(now_ms))
        }) {
            return None;
        }
        snapshot.last_active_at_ms.insert(app_id.clone(), now_ms);
        Some(open())
    }

    pub async fn mark_active(&self, app_id: &AppId, now_ms: i64) {
        self.snapshot
            .write()
            .await
            .last_active_at_ms
            .insert(app_id.clone(), now_ms);
    }

    pub async fn mark_snapshotting(&self, app_id: &AppId, active: bool) {
        let mut snapshot = self.snapshot.write().await;
        if active {
            snapshot.snapshotting.insert(app_id.clone());
        } else {
            snapshot.snapshotting.remove(app_id);
        }
    }

    pub async fn is_snapshotting(&self, app_id: &AppId) -> bool {
        self.snapshot.read().await.snapshotting.contains(app_id)
    }

    pub async fn probe_at_once(&self, app_id: &AppId) {
        self.snapshot.write().await.next_probe_at_ms.remove(app_id);
    }

    pub async fn remember_deleted_volume(&self, report: ReportedVolume) {
        self.snapshot
            .write()
            .await
            .deleted_volumes
            .insert(report.volume_id.clone(), report);
    }

    pub async fn forget_deleted_volumes(&self, keep: &BTreeSet<VolumeId>) {
        self.snapshot
            .write()
            .await
            .deleted_volumes
            .retain(|volume_id, _| keep.contains(volume_id));
    }

    pub fn signal_refresh(&self) {
        self.refresh.notify_one();
    }

    pub fn signal_report(&self) {
        self.report.notify_one();
    }

    pub async fn refresh_signalled(&self) {
        self.refresh.notified().await;
    }

    pub async fn report_signalled(&self) {
        self.report.notified().await;
    }
}

pub fn merge_volume_reports(
    existing: Vec<ReportedVolume>,
    updates: Vec<ReportedVolume>,
) -> Vec<ReportedVolume> {
    let mut merged: BTreeMap<VolumeId, ReportedVolume> = existing
        .into_iter()
        .map(|report| (report.volume_id.clone(), report))
        .collect();
    for report in updates {
        merged.insert(report.volume_id.clone(), report);
    }
    merged.into_values().collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{app_id, instance_record, volume_id};
    use protocol::{InstanceState, VolumeState};

    use crate::config::MemoryAdmissionMode::{Adaptive, Observe};
    use crate::domain::memory_admission::{MemoryOperation, MemoryReadings, PoolMemory};

    type Measured = Option<(crate::config::MemoryAdmissionMode, MemoryReadings)>;

    fn measured(mode: crate::config::MemoryAdmissionMode, available_mib: u64) -> Measured {
        Some((
            mode,
            MemoryReadings {
                startup: None,
                pool: None,
                reservation_generation: 0,
                available_bytes: available_mib * 1_048_576,
                headroom_bytes: 1024 * 1_048_576,
                measured_at_ms: crate::clock::now_ms(),
                apps: BTreeMap::new(),
            },
        ))
    }

    async fn reserve(
        state: &HostState,
        room_mib: u64,
        app: &AppId,
        measured: Measured,
    ) -> Result<MemoryReservation, u64> {
        state
            .reserve_memory(
                room_mib,
                app,
                protocol::DEFAULT_INSTANCE_RESOURCES,
                MemoryOperation::Start,
                measured,
            )
            .await
    }

    /// A running VM measured at 64 MiB of a 2 GiB ceiling, with a 2 GiB peak behind it.
    fn running_readings(state: &HostState) -> MemoryReadings {
        use protocol::{ReportedMemory, ReportedMemoryLimits};
        MemoryReadings {
            startup: None,
            pool: None,
            reservation_generation: state.memory_generation(),
            available_bytes: 1500 * 1_048_576,
            headroom_bytes: 1024 * 1_048_576,
            measured_at_ms: crate::clock::now_ms(),
            apps: BTreeMap::from([(
                app_id(),
                ReportedMemory {
                    measured_at: crate::clock::now_timestamp(),
                    cgroup: Some("/nibrunner-jailer/test".into()),
                    proportional_set_bytes: Some(64 * 1_048_576),
                    anonymous_set_bytes: None,
                    current_bytes: 64 * 1_048_576,
                    peak_bytes: Some(2048 * 1_048_576),
                    swap_bytes: 0,
                    high_events: 0,
                    oom_kills: 0,
                    pressure_some_us: 0,
                    pressure_full_us: 0,
                    limits: Some(ReportedMemoryLimits {
                        low_bytes: 0,
                        high_bytes: None,
                        max_bytes: Some(2048 * 1_048_576),
                        swap_max_bytes: None,
                    }),
                },
            )]),
        }
    }

    /// The same VM, its peak no higher than what it holds now: a working set that leaves room.
    fn settled_readings(state: &HostState) -> MemoryReadings {
        let mut readings = running_readings(state);
        readings.apps.get_mut(&app_id()).unwrap().peak_bytes = Some(64 * 1_048_576);
        readings
    }

    async fn running_app(state: &HostState) {
        state
            .put_record(instance_record(|record| {
                record.state = InstanceState::Running;
                record.started_at = Some(protocol::Timestamp::from_epoch_ms(1));
            }))
            .await;
    }

    #[tokio::test]
    async fn measured_starts_reserve_headroom_atomically_and_release_cancelled_capacity() {
        let state = HostState::shared();
        let ceiling_mib =
            crate::adapters::vm::limits::memory_max_bytes(protocol::DEFAULT_INSTANCE_RESOURCES.memory_mib)
                .div_ceil(1_048_576);
        let available = 1024 + ceiling_mib;
        let second = AppId::parse("app-2").unwrap();
        let first = app_id();
        let (one, two) = tokio::join!(
            reserve(&state, 8192, &first, measured(Adaptive, available)),
            reserve(&state, 8192, &second, measured(Adaptive, available))
        );
        assert_eq!(usize::from(one.is_ok()) + usize::from(two.is_ok()), 1);
        drop(one);
        drop(two);
        assert!(reserve(&state, 8192, &second, measured(Adaptive, available))
            .await
            .is_ok());
    }

    #[tokio::test]
    async fn observation_preserves_strict_admission_even_when_the_host_measurement_disagrees() {
        let state = HostState::shared();
        assert!(reserve(&state, 8192, &app_id(), measured(Observe, 0))
            .await
            .is_ok());
        assert!(reserve(&state, 8192, &app_id(), measured(Adaptive, 0))
            .await
            .is_err());
        assert!(reserve(&state, 0, &app_id(), measured(Observe, 8192))
            .await
            .is_err());
    }

    #[tokio::test]
    async fn adaptive_pool_admission_waits_for_every_workload_to_be_inside_the_pool() {
        let state = HostState::shared();
        running_app(&state).await;
        let next = AppId::parse("next-app").unwrap();
        for (contained, allowed) in [(false, false), (true, true)] {
            let mut readings = settled_readings(&state);
            readings.available_bytes = 8192 * 1_048_576;
            readings.pool = Some(PoolMemory {
                membership: "/workloads.slice".into(),
                current_bytes: 64 * 1_048_576,
                reclaimable_file_bytes: 0,
                // The running VM's ceiling and the next one's do not fit; their working sets do.
                high_bytes: 2048 * 1_048_576,
                max_bytes: 2048 * 1_048_576,
                all_workloads_contained: contained,
            });
            let result = reserve(&state, 8192, &next, Some((Adaptive, readings))).await;
            assert_eq!(result.is_ok(), allowed, "contained: {contained}");
        }
    }

    #[tokio::test]
    async fn a_pool_or_adaptive_admission_refuses_a_stale_reading() {
        let state = HostState::shared();
        for (mode, pooled) in [(Observe, true), (Adaptive, false)] {
            let (mode, mut readings) = measured(mode, 8192).unwrap();
            readings.measured_at_ms -= 5001;
            readings.pool = pooled.then(|| PoolMemory {
                membership: "/workloads.slice".into(),
                current_bytes: 0,
                reclaimable_file_bytes: 0,
                high_bytes: 1800 * 1_048_576,
                max_bytes: 2048 * 1_048_576,
                all_workloads_contained: true,
            });
            assert!(reserve(&state, 8192, &app_id(), Some((mode, readings)))
                .await
                .is_err());
            for operation in [MemoryOperation::Start, MemoryOperation::Snapshot] {
                let (mode, mut old) = measured(Adaptive, 8192).unwrap();
                old.measured_at_ms -= 5001;
                assert!(state
                    .reserve_memory(
                        8192,
                        &app_id(),
                        protocol::DEFAULT_INSTANCE_RESOURCES,
                        operation,
                        Some((mode, old))
                    )
                    .await
                    .is_err());
            }
        }
    }

    #[tokio::test]
    async fn a_known_revision_starts_from_its_persisted_peak_and_another_deployment_does_not() {
        let host = crate::test_support::test_host().await;
        host.state
            .put_record(instance_record(|record| {
                record.state = InstanceState::Idle;
                record.memory_peak_bytes = Some(128 * 1_048_576);
            }))
            .await;
        host.persist().await;
        host.state
            .modify(|snapshot| snapshot.memory_profiles.clear())
            .await;
        host.load().await;
        assert_eq!(
            host.state.record(&app_id()).await.unwrap().memory_peak_bytes,
            Some(128 * 1_048_576)
        );
        let tight = |state: &HostState| {
            let (mode, mut readings) = measured(Adaptive, 1024 + 200).unwrap();
            readings.reservation_generation = state.memory_generation();
            Some((mode, readings))
        };
        assert!(reserve(&host.state, 8192, &app_id(), tight(&host.state))
            .await
            .is_ok());
        host.state
            .update_record(&app_id(), |record| {
                let mut fields = crate::test_support::record_fields();
                fields.deployment_id = protocol::DeploymentId::parse("different-deployment").unwrap();
                record.adopt(fields);
            })
            .await;
        assert_eq!(
            host.state.record(&app_id()).await.unwrap().memory_peak_bytes,
            None
        );
        assert!(reserve(&host.state, 8192, &app_id(), tight(&host.state))
            .await
            .is_err());
    }

    #[tokio::test]
    async fn a_cold_start_uses_its_deployments_history_before_a_record_exists() {
        let state = HostState::shared();
        let desired = crate::test_support::desired_instance(|_| {});
        state
            .put_record(instance_record(|record| {
                record.memory_peak_bytes = Some(128 * 1_048_576)
            }))
            .await;
        state.drop_record(&app_id()).await;
        let tight = |startup| {
            let (mode, mut readings) = measured(Adaptive, 1024 + 200).unwrap();
            readings.reservation_generation = state.memory_generation();
            readings.startup = Some(startup);
            Some((mode, readings))
        };
        assert!(reserve(
            &state,
            8192,
            &app_id(),
            tight(state.startup_memory(&desired).await)
        )
        .await
        .is_ok());
        let mut replacement = desired;
        replacement.deployment_id = protocol::DeploymentId::parse("new-deployment").unwrap();
        assert!(reserve(
            &state,
            8192,
            &app_id(),
            tight(state.startup_memory(&replacement).await)
        )
        .await
        .is_err());
    }

    #[tokio::test]
    async fn a_reservation_taken_after_a_reading_voids_the_working_sets_it_measured() {
        let state = HostState::shared();
        running_app(&state).await;
        let ceiling =
            crate::adapters::vm::limits::memory_max_bytes(protocol::DEFAULT_INSTANCE_RESOURCES.memory_mib);
        let reading = |state: &HostState| {
            let mut readings = settled_readings(state);
            readings.available_bytes = 8192 * 1_048_576;
            readings
                .apps
                .get_mut(&app_id())
                .unwrap()
                .limits
                .as_mut()
                .unwrap()
                .max_bytes = Some(ceiling);
            readings
        };
        let old = reading(&state);
        let other = AppId::parse("app-2").unwrap();
        drop(reserve(&state, 8192, &other, None).await.unwrap());
        // Two ceilings do not fit; one ceiling and the running VM's settled working set do.
        let capacity_mib = ceiling.div_ceil(1_048_576) + 200;
        assert!(reserve(&state, capacity_mib, &other, Some((Adaptive, old)))
            .await
            .is_err());
        assert!(
            reserve(&state, capacity_mib, &other, Some((Adaptive, reading(&state))))
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn a_snapshot_holds_its_vms_whole_ceiling_until_it_is_done() {
        let state = HostState::shared();
        running_app(&state).await;
        let other = AppId::parse("app-2").unwrap();
        let readings = |state: &HostState| {
            let mut readings = settled_readings(state);
            readings.available_bytes = 3072 * 1_048_576;
            Some((Adaptive, readings))
        };
        let snapshot = state
            .reserve_memory(
                8192,
                &app_id(),
                protocol::DEFAULT_INSTANCE_RESOURCES,
                MemoryOperation::Snapshot,
                readings(&state),
            )
            .await
            .unwrap();
        assert!(reserve(&state, 8192, &other, readings(&state)).await.is_err());
        drop(snapshot);
        assert!(reserve(&state, 8192, &other, readings(&state)).await.is_ok());
    }

    #[tokio::test]
    async fn simultaneous_starts_and_wakes_cannot_reserve_the_same_capacity() {
        let state = HostState::shared();
        let room = u64::from(protocol::DEFAULT_INSTANCE_RESOURCES.memory_mib);
        let second = AppId::parse("app-2").unwrap();
        let first = app_id();
        let (one, two) = tokio::join!(
            reserve(&state, room, &first, None),
            reserve(&state, room, &second, None)
        );
        assert_eq!(usize::from(one.is_ok()) + usize::from(two.is_ok()), 1);
        drop(one);
        drop(two);
        assert!(reserve(&state, room, &second, None).await.is_ok());
    }

    #[tokio::test]
    async fn a_reservation_and_its_starting_record_are_counted_once_until_the_guard_is_released() {
        let state = HostState::shared();
        let room = u64::from(protocol::DEFAULT_INSTANCE_RESOURCES.memory_mib) * 2;
        let second = AppId::parse("app-2").unwrap();
        let held = reserve(&state, room, &app_id(), None).await.ok().unwrap();
        state
            .put_record(instance_record(|record| record.state = InstanceState::Starting))
            .await;
        assert!(reserve(&state, room, &second, None).await.is_ok());
        drop(held);
        assert!(reserve(&state, room, &second, None).await.is_ok());
        assert!(reserve(&state, room / 2, &second, None).await.is_err());
    }

    #[test]
    fn cron_activity_lasts_until_the_last_command_finishes() {
        let state = HostState::shared();
        let first = state.cron_activity(&app_id());
        let second = state.cron_activity(&app_id());
        assert!(state.cron_running(&app_id()));
        drop(first);
        assert!(state.cron_running(&app_id()));
        drop(second);
        assert!(!state.cron_running(&app_id()));
    }

    #[test]
    fn a_cron_command_keeps_only_its_own_app_active() {
        let state = HostState::shared();
        let _command = state.cron_activity(&app_id());
        assert!(!state.cron_running(&AppId::parse("app-2").unwrap()));
    }

    fn reported(state: VolumeState) -> ReportedVolume {
        ReportedVolume {
            volume_id: volume_id(),
            app_id: app_id(),
            state,
            size_bytes: 1,
            storage_prefix: None,
            device_path: None,
            message: None,
        }
    }

    #[tokio::test]
    async fn a_record_is_merged_rather_than_written_over() {
        let state = HostState::shared();
        state
            .put_record(instance_record(|record| record.stop_requested = true))
            .await;
        state
            .update_record(&app_id(), |record| record.state = InstanceState::Starting)
            .await;
        let record = state.record(&app_id()).await.unwrap();
        assert_eq!(record.state, InstanceState::Starting);
        assert!(record.stop_requested);
    }

    #[tokio::test]
    async fn an_instance_dropped_mid_pass_is_not_brought_back_by_a_write_that_lands_after() {
        let state = HostState::shared();
        state.put_record(instance_record(|_| {})).await;
        state.drop_record(&app_id()).await;
        state
            .update_record(&app_id(), |record| record.state = InstanceState::Running)
            .await;
        assert!(state.record(&app_id()).await.is_none());
    }

    #[tokio::test]
    async fn one_transition_of_an_app_at_a_time_and_any_app_beside_any_other() {
        let state = HostState::shared();
        let (this, other) = (app_id(), AppId::parse("app-2").unwrap());
        let held = state.transition(&this).await;

        let beside = tokio::time::timeout(std::time::Duration::from_secs(1), state.transition(&other)).await;
        assert!(beside.is_ok(), "another app's transition waited on this one's");

        let mut same = std::pin::pin!(state.transition(&this));
        let waited = tokio::time::timeout(std::time::Duration::from_millis(20), &mut same).await;
        assert!(
            waited.is_err(),
            "a second transition of the same app began before the first was done"
        );

        drop(held);
        assert!(tokio::time::timeout(std::time::Duration::from_secs(1), same)
            .await
            .is_ok());
    }

    #[tokio::test]
    async fn an_app_nothing_is_moving_any_more_keeps_no_lock_behind() {
        let state = HostState::shared();
        drop(state.transition(&app_id()).await);
        drop(state.transition(&AppId::parse("app-2").unwrap()).await);
        let kept = state.transitions.lock().unwrap().len();
        assert_eq!(
            kept, 1,
            "only the app whose lock was taken last is still on the map"
        );
    }

    #[tokio::test]
    async fn a_snapshot_mark_is_set_and_cleared_and_a_removal_is_remembered_until_taken_in() {
        let state = HostState::shared();
        state.mark_snapshotting(&app_id(), true).await;
        assert!(state.snapshot().await.snapshotting.contains(&app_id()));
        state.mark_snapshotting(&app_id(), false).await;
        assert!(state.snapshot().await.snapshotting.is_empty());

        state
            .remember_deleted_volume(reported(VolumeState::Deleted))
            .await;
        state.forget_deleted_volumes(&BTreeSet::from([volume_id()])).await;
        assert_eq!(state.snapshot().await.deleted_volumes.len(), 1);
        state.forget_deleted_volumes(&BTreeSet::new()).await;
        assert!(state.snapshot().await.deleted_volumes.is_empty());
    }

    #[test]
    fn what_just_happened_to_a_volume_wins_over_what_was_observed_of_it() {
        let merged = merge_volume_reports(
            vec![reported(VolumeState::Ready)],
            vec![reported(VolumeState::Deleted)],
        );
        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0].state, VolumeState::Deleted);
    }

    #[test]
    fn a_volume_only_one_side_knows_of_is_kept_rather_than_dropped_by_the_merge() {
        let elsewhere = ReportedVolume {
            volume_id: VolumeId::parse("vol-2").unwrap(),
            ..reported(VolumeState::Ready)
        };
        let merged = merge_volume_reports(vec![reported(VolumeState::Ready)], vec![elsewhere.clone()]);
        assert_eq!(merged.len(), 2);
        assert!(merged
            .iter()
            .any(|report| report.volume_id == elsewhere.volume_id));
        assert!(merge_volume_reports(vec![], vec![]).is_empty());
    }

    #[tokio::test]
    async fn a_host_that_has_done_nothing_yet_holds_nothing_and_claims_nothing() {
        let state = HostState::shared();
        let snapshot = state.snapshot().await;
        assert!(snapshot.records.is_empty());
        assert!(!snapshot.converged);
        assert!(!snapshot.isolated);
        assert!(!snapshot.deferred_work);
        assert!(state.records().await.is_empty());
        assert!(state.record(&app_id()).await.is_none());
    }

    #[tokio::test]
    async fn every_record_the_host_holds_is_handed_back_in_one_reading() {
        let state = HostState::shared();
        state.put_record(instance_record(|_| {})).await;
        state
            .put_record(instance_record(|record| {
                record.app_id = AppId::parse("app-2").unwrap()
            }))
            .await;
        let records = state.records().await;
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].app_id, app_id());

        state
            .put_record(instance_record(|record| record.state = InstanceState::Failed))
            .await;
        assert_eq!(state.records().await.len(), 2);
        assert_eq!(
            state.record(&app_id()).await.unwrap().state,
            InstanceState::Failed
        );
    }

    #[tokio::test]
    async fn a_change_made_under_one_lock_is_seen_whole_and_hands_back_what_it_decided() {
        let state = HostState::shared();
        let decided = state
            .modify(|snapshot| {
                snapshot.converged = true;
                snapshot.isolated = true;
                snapshot.records.len()
            })
            .await;
        assert_eq!(decided, 0);
        let snapshot = state.snapshot().await;
        assert!(snapshot.converged);
        assert!(snapshot.isolated);
    }

    #[tokio::test]
    async fn a_request_that_reached_an_app_moves_its_last_activity_forward() {
        let state = HostState::shared();
        state.mark_active(&app_id(), 1_000).await;
        assert_eq!(
            state.snapshot().await.last_active_at_ms.get(&app_id()),
            Some(&1_000)
        );
        state.mark_active(&app_id(), 2_000).await;
        assert_eq!(
            state.snapshot().await.last_active_at_ms.get(&app_id()),
            Some(&2_000)
        );
    }

    #[tokio::test]
    async fn asking_for_a_probe_at_once_forgets_when_the_next_one_was_due() {
        let state = HostState::shared();
        state
            .modify(|snapshot| snapshot.next_probe_at_ms.insert(app_id(), 9_999))
            .await;
        state.probe_at_once(&app_id()).await;
        assert!(state.snapshot().await.next_probe_at_ms.is_empty());
        state.probe_at_once(&app_id()).await;
    }

    #[tokio::test]
    async fn a_signal_raised_before_anybody_waited_still_wakes_the_next_waiter() {
        let state = HostState::shared();
        state.signal_refresh();
        state.signal_report();
        let waited = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            state.refresh_signalled().await;
            state.report_signalled().await;
        })
        .await;
        assert!(waited.is_ok(), "a signal raised first was lost");
    }

    #[tokio::test]
    async fn a_loop_waiting_on_a_signal_is_woken_by_the_pass_that_raises_it() {
        let state = HostState::shared();
        let waiting = tokio::spawn({
            let state = state.clone();
            async move { state.refresh_signalled().await }
        });
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        state.signal_refresh();
        assert!(tokio::time::timeout(std::time::Duration::from_secs(5), waiting)
            .await
            .is_ok());
    }
}
