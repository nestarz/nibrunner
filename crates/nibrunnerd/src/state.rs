use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

use nft_render::AppTraffic;
use protocol::{
    AppId, ComputeUsage, FilesystemUsage, ReportedVolume, Revision, Sha256Digest, StateMessage, UsageMeters,
    VolumeId,
};
use tokio::sync::{Notify, OwnedMutexGuard, RwLock};

use crate::domain::metrics::converge::Deploy;
use crate::domain::report::InstanceRecord;

#[derive(Debug, Default, Clone)]
pub struct HostSnapshot {
    pub records: BTreeMap<AppId, InstanceRecord>,
    // Only ever this daemon's own account of what it set out on: a restart starts each over,
    // and so is measured as one.
    pub deploys: BTreeMap<AppId, Deploy>,
    pub deleted_volumes: BTreeMap<VolumeId, ReportedVolume>,
    pub volume_reports: Vec<ReportedVolume>,
    pub checkpoint_reports: Vec<protocol::ReportedCheckpoint>,
    pub export_reports: Vec<protocol::ReportedExport>,
    pub next_probe_at_ms: BTreeMap<AppId, i64>,
    pub snapshotting: BTreeSet<AppId>,
    pub app_traffic: BTreeMap<AppId, AppTraffic>,
    pub last_active_at_ms: BTreeMap<AppId, i64>,
    // When each app's traffic counters were last read. Only ever this daemon's own note of what
    // it watched, so a restart has measured nothing and must take a reading before it may call
    // an app quiet.
    pub last_measured_at_ms: BTreeMap<AppId, i64>,
    pub reclaimed_at_ms: BTreeMap<AppId, i64>,
    pub(crate) freeze_attempted_at_ms: BTreeMap<AppId, i64>,
    pub(crate) memory_pressure: crate::domain::reconcile::pressure::PressureState,
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
#[derive(Clone)]
struct ReservedMemory {
    resources: protocol::InstanceResources,
    bytes: u64,
    group: Option<crate::adapters::cgroup::MemoryGroup>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum ReservationOwner {
    App(AppId),
    External(String),
}

#[derive(Default)]
struct MemoryReservations {
    generation: u64,
    held: BTreeMap<ReservationOwner, ReservedMemory>,
}

type SharedReservations = Arc<Mutex<MemoryReservations>>;

pub(crate) struct MemoryReservation {
    owner: Option<ReservationOwner>,
    reservations: SharedReservations,
}

impl MemoryReservation {
    pub(crate) fn retain(mut self) {
        // A persisted external lease outlives its listener and is released only after verified exit.
        self.owner = None;
    }
}

impl Drop for MemoryReservation {
    fn drop(&mut self) {
        let Some(owner) = &self.owner else {
            return;
        };
        let mut reservations = self
            .reservations
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        reservations.held.remove(owner);
        reservations.generation = reservations.generation.wrapping_add(1);
    }
}

pub(crate) struct InstanceTransition {
    state: crate::state::SharedState,
    app: AppId,
    held: Option<tokio::sync::OwnedMutexGuard<()>>,
}

impl Drop for InstanceTransition {
    fn drop(&mut self) {
        let Some(held) = self.held.take() else {
            return;
        };
        let state = self.state.clone();
        let app = self.app.clone();
        tokio::spawn(async move {
            let _held = held;
            state.mark_snapshotting(&app, false).await;
            state.signal_refresh();
        });
    }
}

impl InstanceTransition {
    pub(crate) fn new(state: SharedState, app: AppId, held: OwnedMutexGuard<()>) -> Self {
        Self {
            state,
            app,
            held: Some(held),
        }
    }
}

pub struct HostState {
    snapshot: RwLock<HostSnapshot>,
    refresh: Notify,
    report: Notify,
    transitions: Mutex<Transitions>,
    memory_reservations: SharedReservations,
    pub(crate) persistence: tokio::sync::Mutex<()>,
    pub(crate) reclaim: tokio::sync::Mutex<()>,
}

impl HostState {
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
            .any(|owner| matches!(owner, ReservationOwner::App(id) if id != app))
    }

    pub(crate) fn memory_generation(&self) -> u64 {
        self.memory_reservations
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .generation
    }

    pub fn shared() -> SharedState {
        Arc::new(Self {
            snapshot: RwLock::new(HostSnapshot::default()),
            refresh: Notify::new(),
            report: Notify::new(),
            transitions: Mutex::new(BTreeMap::new()),
            memory_reservations: Arc::default(),
            persistence: tokio::sync::Mutex::new(()),
            reclaim: tokio::sync::Mutex::new(()),
        })
    }

    /// Called under the app's transition lock. Starts and wakes share the reservation until
    /// their instance record accounts for the running process, including cancelled attempts.
    #[cfg(test)]
    pub(crate) async fn reserve_memory(
        &self,
        capacity_mib: u64,
        app_id: &AppId,
        wanted: protocol::InstanceResources,
    ) -> Result<MemoryReservation, u64> {
        self.reserve_with_readings(capacity_mib, app_id, wanted, None)
            .await
    }

    #[cfg(test)]
    pub(crate) async fn reserve_with_readings(
        &self,
        capacity_mib: u64,
        app_id: &AppId,
        wanted: protocol::InstanceResources,
        measured: Option<(
            crate::config::MemoryAdmissionMode,
            crate::domain::memory_admission::MemoryReadings,
        )>,
    ) -> Result<MemoryReservation, u64> {
        self.reserve_for_operation(
            capacity_mib,
            app_id,
            wanted,
            crate::domain::memory_admission::MemoryOperation::Start,
            measured,
        )
        .await
    }

    pub(crate) async fn reserve_for_operation(
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
        self.reserve_for_owner(
            capacity_mib,
            ReservationOwner::App(app_id.clone()),
            wanted,
            operation,
            measured,
        )
        .await
    }

    pub(crate) async fn reserve_external(
        &self,
        capacity_mib: u64,
        id: &str,
        memory_mib: std::num::NonZeroU32,
        measured: (
            crate::config::MemoryAdmissionMode,
            crate::domain::memory_admission::MemoryReadings,
        ),
    ) -> Result<MemoryReservation, u64> {
        self.reserve_for_owner(
            capacity_mib,
            ReservationOwner::External(id.into()),
            protocol::InstanceResources {
                memory_mib: memory_mib.get(),
                vcpu_count: 0,
            },
            crate::domain::memory_admission::MemoryOperation::Start,
            Some(measured),
        )
        .await
    }

    pub(crate) fn restore_external(&self, id: &str, memory_mib: std::num::NonZeroU32) {
        let owner = ReservationOwner::External(id.into());
        let mut reservations = self
            .memory_reservations
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        reservations.held.insert(
            owner.clone(),
            ReservedMemory {
                resources: protocol::InstanceResources {
                    memory_mib: memory_mib.get(),
                    vcpu_count: 0,
                },
                bytes: u64::from(memory_mib.get()) * 1_048_576,
                group: None,
            },
        );
        reservations.generation = reservations.generation.wrapping_add(1);
    }

    pub(crate) fn release_external(&self, id: &str) {
        let mut reservations = self
            .memory_reservations
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if reservations
            .held
            .remove(&ReservationOwner::External(id.into()))
            .is_some()
        {
            reservations.generation = reservations.generation.wrapping_add(1);
        }
    }

    pub(crate) fn track_external_group(&self, id: &str, group: crate::adapters::cgroup::MemoryGroup) {
        let mut reservations = self.memory_reservations.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(held) = reservations.held.get_mut(&ReservationOwner::External(id.into())) {
            held.group = Some(group);
            reservations.generation = reservations.generation.wrapping_add(1);
        }
    }

    pub(crate) fn external_resident_memory(&self) -> BTreeMap<String, u64> {
        let held = self
            .memory_reservations
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .held
            .clone();
        held.iter()
            .filter_map(|(owner, reservation)| {
                let ReservationOwner::External(id) = owner else {
                    return None;
                };
                let group = reservation.group.as_ref()?;
                if held.iter().any(|(other, reservation)| {
                    other != owner
                        && reservation
                            .group
                            .as_ref()
                            .is_some_and(|other| group.overlaps(other))
                }) {
                    return None;
                }
                Some((id.clone(), group.resident_bytes(reservation.bytes)?))
            })
            .collect()
    }

    pub(crate) fn external_groups_within(&self, pool: &crate::domain::memory_admission::PoolMemory) -> bool {
        self.memory_reservations
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .held
            .iter()
            .all(|(owner, held)| {
                !matches!(owner, ReservationOwner::External(_))
                    || held.group.as_ref().is_some_and(|group| group.within(pool))
            })
    }

    async fn reserve_for_owner(
        &self,
        capacity_mib: u64,
        owner: ReservationOwner,
        wanted: protocol::InstanceResources,
        operation: crate::domain::memory_admission::MemoryOperation,
        measured: Option<(
            crate::config::MemoryAdmissionMode,
            crate::domain::memory_admission::MemoryReadings,
        )>,
    ) -> Result<MemoryReservation, u64> {
        let app_id = match &owner {
            ReservationOwner::App(app) => Some(app),
            ReservationOwner::External(_) => None,
        };
        let snapshot = self.snapshot.read().await;
        let mut reservations = self
            .memory_reservations
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let records: Vec<_> = snapshot
            .records
            .values()
            .filter(|record| {
                Some(&record.app_id) != app_id
                    && !reservations
                        .held
                        .contains_key(&ReservationOwner::App(record.app_id.clone()))
            })
            .cloned()
            .collect();
        let mut committed = crate::domain::report::capacity::committed_resources(&records);
        committed.extend(reservations.held.values().map(|reserved| reserved.resources));
        let strict_shortfall = match operation {
            crate::domain::memory_admission::MemoryOperation::Start => {
                crate::domain::report::capacity::memory_shortfall_mib(capacity_mib, &committed, &wanted)
            }
            // Strict admission already charged this VM's full budget. Sleeping it releases that budget.
            crate::domain::memory_admission::MemoryOperation::Snapshot => 0,
        };
        let mut shortfall = strict_shortfall;
        let mut bytes = (u64::from(wanted.memory_mib) + if app_id.is_some() { 64 } else { 0 }) * 1_048_576;
        if measured.as_ref().is_some_and(|(mode, readings)| {
            (*mode == crate::config::MemoryAdmissionMode::Adaptive
                || app_id.is_none()
                || readings.pool.is_some())
                && !readings.is_fresh(crate::clock::now_ms())
        }) {
            return Err(bytes.div_ceil(1_048_576));
        }
        if let Some((mode, mut readings)) =
            measured.filter(|(_, readings)| readings.is_fresh(crate::clock::now_ms()))
        {
            if readings.reservation_generation != reservations.generation {
                // A completed allocation can outlive its guard. Old working sets cannot discount it.
                readings.apps.clear();
                readings.external_resident_bytes.clear();
                if let Some(pool) = &mut readings.pool {
                    pool.all_workloads_contained = false;
                }
            }
            bytes = app_id.map_or(bytes, |app| readings.ceiling(app, wanted));
            let reserved_bytes = reservations
                .held
                .values()
                .fold(0u64, |total, held| total.saturating_add(held.bytes));
            let resident_reserved_bytes = reservations.held.iter().fold(0u64, |total, (owner, held)| {
                let resident = match owner {
                    ReservationOwner::External(id) => readings
                        .external_resident_bytes
                        .get(id)
                        .copied()
                        .unwrap_or(0)
                        .min(held.bytes),
                    ReservationOwner::App(id) => snapshot
                        .records
                        .get(id)
                        .map_or(0, |record| readings.anonymous_resident_bytes(record))
                        .min(held.bytes),
                };
                total.saturating_add(resident)
            });
            let snapshot_outside_pool = operation
                == crate::domain::memory_admission::MemoryOperation::Snapshot
                && readings.pool.as_ref().is_some_and(|pool| {
                    app_id
                        .and_then(|app| readings.apps.get(app))
                        .and_then(|memory| memory.cgroup.as_deref())
                        .is_some_and(|group| !pool.contains(group))
                });
            // Legacy scopes must be able to drain even when their ceilings exceed the new pool.
            if snapshot_outside_pool {
                readings.pool = None;
            }
            let wanted_resident_bytes = app_id
                .and_then(|app| snapshot.records.get(app))
                .map_or(0, |record| readings.anonymous_resident_bytes(record))
                .min(bytes);
            let wake_headroom = if app_id.is_none() {
                readings.production_wake_bytes
            } else {
                0
            };
            let measured_shortfall = readings.shortfall_mib(
                if snapshot_outside_pool {
                    u64::MAX
                } else {
                    capacity_mib
                },
                &records,
                reserved_bytes.saturating_add(bytes).saturating_add(wake_headroom),
                resident_reserved_bytes.saturating_add(wanted_resident_bytes),
                0,
            );
            let effective_adaptive = mode == crate::config::MemoryAdmissionMode::Adaptive
                && readings
                    .pool
                    .as_ref()
                    .is_none_or(|pool| pool.all_workloads_contained);
            tracing::info!(
                ?owner,
                ?mode,
                effective_adaptive,
                snapshot_outside_pool,
                strict_shortfall_mib = strict_shortfall,
                measured_shortfall_mib = measured_shortfall,
                "memory admission evaluated"
            );
            if effective_adaptive || snapshot_outside_pool {
                shortfall = measured_shortfall;
            } else if readings.pool.is_some() {
                shortfall = strict_shortfall
                    .max(measured_shortfall)
                    .max(readings.strict_pool_shortfall_mib(&records, reserved_bytes, bytes));
            } else if app_id.is_none() {
                shortfall = strict_shortfall.max(measured_shortfall);
            }
        }
        if shortfall > 0 {
            return Err(shortfall);
        }
        reservations.held.insert(
            owner.clone(),
            ReservedMemory {
                resources: wanted,
                bytes,
                group: None,
            },
        );
        reservations.generation = reservations.generation.wrapping_add(1);
        Ok(MemoryReservation {
            owner: Some(owner),
            reservations: self.memory_reservations.clone(),
        })
    }

    /// The lock a sleep or a wake of this app holds for as long as it is moving the microVM.
    /// One transition of an app at a time, so that neither finds the other half way through a
    /// snapshot; and each app's own, so that the sleeps a pass runs side by side wait on nothing
    /// but the disk.
    pub async fn transition(&self, app_id: &AppId) -> OwnedMutexGuard<()> {
        self.transition_lock(app_id).lock_owned().await
    }

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

    pub async fn records(&self) -> Vec<InstanceRecord> {
        self.snapshot.read().await.records.values().cloned().collect()
    }

    pub async fn record(&self, app_id: &AppId) -> Option<InstanceRecord> {
        self.snapshot.read().await.records.get(app_id).cloned()
    }

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
        snapshot.records.insert(record.app_id.clone(), record);
    }

    pub async fn update_record(&self, app_id: &AppId, change: impl FnOnce(&mut InstanceRecord)) {
        let mut snapshot = self.snapshot.write().await;
        if let Some(record) = snapshot.records.get_mut(app_id) {
            let terminal = record.expired_at_ms;
            let deployment = record.deployment_id.clone();
            change(record);
            if terminal.is_some() && record.expiry.is_some() && record.deployment_id == deployment {
                record.expired_at_ms = terminal;
                record.state = protocol::InstanceState::Expired;
            }
        }
    }

    pub async fn drop_record(&self, app_id: &AppId) {
        self.snapshot.write().await.records.remove(app_id);
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

    fn measured(
        mode: crate::config::MemoryAdmissionMode,
        available_mib: u64,
    ) -> Option<(
        crate::config::MemoryAdmissionMode,
        crate::domain::memory_admission::MemoryReadings,
    )> {
        Some((
            mode,
            crate::domain::memory_admission::MemoryReadings {
                pool: None,
                reservation_generation: 0,
                external_resident_bytes: BTreeMap::new(),
                available_bytes: available_mib * 1_048_576,
                headroom_bytes: 1024 * 1_048_576,
                production_wake_bytes: 0,
                measured_at_ms: crate::clock::now_ms(),
                apps: BTreeMap::new(),
                ceilings: BTreeMap::new(),
            },
        ))
    }

    #[tokio::test]
    async fn measured_starts_reserve_headroom_atomically_and_release_cancelled_capacity() {
        use crate::config::MemoryAdmissionMode::Adaptive;
        let state = HostState::shared();
        let wanted = protocol::DEFAULT_INSTANCE_RESOURCES;
        let available = 1024 + u64::from(wanted.memory_mib) + 64;
        let first_id = app_id();
        let second_id = AppId::parse("app-2").unwrap();
        let (first, second) = tokio::join!(
            state.reserve_with_readings(8192, &first_id, wanted, measured(Adaptive, available)),
            state.reserve_with_readings(8192, &second_id, wanted, measured(Adaptive, available))
        );
        assert_eq!(usize::from(first.is_ok()) + usize::from(second.is_ok()), 1);
        drop(first);
        drop(second);
        assert!(state
            .reserve_with_readings(8192, &second_id, wanted, measured(Adaptive, available))
            .await
            .is_ok());
    }

    #[tokio::test]
    async fn observation_preserves_strict_admission_even_when_the_host_measurement_disagrees() {
        use crate::config::MemoryAdmissionMode::{Adaptive, Observe};
        let state = HostState::shared();
        let wanted = protocol::DEFAULT_INSTANCE_RESOURCES;
        assert!(state
            .reserve_with_readings(8192, &app_id(), wanted, measured(Observe, 0))
            .await
            .is_ok());
        assert!(state
            .reserve_with_readings(8192, &app_id(), wanted, measured(Adaptive, 0))
            .await
            .is_err());
        assert!(state
            .reserve_with_readings(0, &app_id(), wanted, measured(Observe, 8192))
            .await
            .is_err());
    }

    fn frozen_readings(state: &HostState) -> crate::domain::memory_admission::MemoryReadings {
        use protocol::{ReportedMemory, ReportedMemoryLimits};
        crate::domain::memory_admission::MemoryReadings {
            pool: None,
            reservation_generation: state.memory_generation(),
            external_resident_bytes: BTreeMap::new(),
            available_bytes: 1500 * 1_048_576,
            headroom_bytes: 1024 * 1_048_576,
            production_wake_bytes: 0,
            measured_at_ms: crate::clock::now_ms(),
            ceilings: BTreeMap::from([(app_id(), 2048 * 1_048_576)]),
            apps: BTreeMap::from([(
                app_id(),
                ReportedMemory {
                    measured_at: crate::clock::now_timestamp(),
                    cgroup: Some("/test.scope".into()),
                    proportional_set_bytes: Some(64 * 1_048_576),
                    anonymous_set_bytes: None,
                    current_bytes: 64 * 1_048_576,
                    peak_bytes: Some(2048 * 1_048_576),
                    swap_bytes: 1024 * 1_048_576,
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

    #[tokio::test]
    async fn adaptive_pool_admission_waits_for_legacy_workloads_to_move_inside() {
        use crate::config::MemoryAdmissionMode::Adaptive;
        let state = HostState::shared();
        state
            .put_record(instance_record(|record| {
                record.state = InstanceState::Frozen;
                record.started_at = Some(protocol::Timestamp::from_epoch_ms(1));
            }))
            .await;
        let next = AppId::parse("next-app").unwrap();
        for (contained, allowed) in [(false, false), (true, true)] {
            let mut readings = frozen_readings(&state);
            readings.available_bytes = 8192 * 1_048_576;
            readings.pool = Some(crate::domain::memory_admission::PoolMemory {
                membership: "/workloads.slice".into(),
                current_bytes: 64 * 1_048_576,
                reclaimable_file_bytes: 0,
                high_bytes: 1800 * 1_048_576,
                max_bytes: 2048 * 1_048_576,
                all_workloads_contained: contained,
            });
            let result = state
                .reserve_with_readings(
                    8192,
                    &next,
                    protocol::DEFAULT_INSTANCE_RESOURCES,
                    Some((Adaptive, readings)),
                )
                .await;
            assert_eq!(result.is_ok(), allowed);
        }
    }

    #[tokio::test]
    async fn legacy_snapshots_can_drain_into_a_smaller_pool_but_still_need_physical_headroom() {
        use crate::config::MemoryAdmissionMode::Observe;
        use crate::domain::memory_admission::MemoryOperation::Snapshot;
        let state = HostState::shared();
        state
            .put_record(instance_record(|record| {
                record.state = InstanceState::Frozen;
                record.started_at = Some(protocol::Timestamp::from_epoch_ms(1));
            }))
            .await;
        for (available, membership, allowed) in [
            (8192, "/legacy.scope", true),
            (1024, "/legacy.scope", false),
            (8192, "/workloads.slice/app.scope", false),
        ] {
            let mut readings = frozen_readings(&state);
            readings.available_bytes = available * 1_048_576;
            readings.apps.get_mut(&app_id()).unwrap().cgroup = Some(membership.into());
            readings.pool = Some(crate::domain::memory_admission::PoolMemory {
                membership: "/workloads.slice".into(),
                current_bytes: 1024 * 1_048_576,
                reclaimable_file_bytes: 0,
                high_bytes: 900 * 1_048_576,
                max_bytes: 1024 * 1_048_576,
                all_workloads_contained: false,
            });
            let result = state
                .reserve_for_operation(
                    1024,
                    &app_id(),
                    protocol::DEFAULT_INSTANCE_RESOURCES,
                    Snapshot,
                    Some((Observe, readings)),
                )
                .await;
            assert_eq!(result.is_ok(), allowed, "{available}: {membership}");
        }
    }

    #[tokio::test]
    async fn an_observed_pool_refuses_stale_capacity_readings() {
        use crate::config::MemoryAdmissionMode::Observe;
        let state = HostState::shared();
        let (mode, mut readings) = measured(Observe, 8192).unwrap();
        readings.measured_at_ms -= 5001;
        readings.pool = Some(crate::domain::memory_admission::PoolMemory {
            membership: "/workloads.slice".into(),
            current_bytes: 0,
            reclaimable_file_bytes: 0,
            high_bytes: 1800 * 1_048_576,
            max_bytes: 2048 * 1_048_576,
            all_workloads_contained: true,
        });
        assert!(state
            .reserve_with_readings(
                8192,
                &app_id(),
                protocol::DEFAULT_INSTANCE_RESOURCES,
                Some((mode, readings))
            )
            .await
            .is_err());
    }

    #[tokio::test]
    async fn a_wake_credits_only_its_fresh_resident_anonymous_pages_and_keeps_the_full_reservation() {
        use crate::config::MemoryAdmissionMode::Adaptive;
        let state = HostState::shared();
        state
            .put_record(instance_record(|record| {
                record.state = InstanceState::Frozen;
                record.started_at = Some(protocol::Timestamp::from_epoch_ms(1));
            }))
            .await;
        for (anonymous, stale, admitted) in [
            (None, false, false),
            (Some(512), true, false),
            (Some(512), false, true),
        ] {
            let mut readings = frozen_readings(&state);
            readings.available_bytes = 2600 * 1_048_576;
            let memory = readings.apps.get_mut(&app_id()).unwrap();
            memory.current_bytes = 600 * 1_048_576;
            memory.proportional_set_bytes = Some(550 * 1_048_576);
            memory.anonymous_set_bytes = anonymous.map(|mib| mib * 1_048_576);
            if stale {
                memory.measured_at = protocol::Timestamp::from_epoch_ms(0);
            }
            let reservation = state
                .reserve_with_readings(
                    8192,
                    &app_id(),
                    protocol::DEFAULT_INSTANCE_RESOURCES,
                    Some((Adaptive, readings)),
                )
                .await;
            assert_eq!(reservation.is_ok(), admitted);
            if admitted {
                let held = state.memory_reservations.lock().unwrap();
                assert_eq!(
                    held.held[&ReservationOwner::App(app_id())].bytes,
                    2048 * 1_048_576
                );
            }
            drop(reservation);
        }
    }

    #[tokio::test]
    async fn a_build_leaves_room_for_a_production_wake_before_it_starts() {
        use crate::config::MemoryAdmissionMode::Adaptive;
        let state = HostState::shared();
        for (grant, admitted) in [(2500, false), (2000, true)] {
            let (mode, mut readings) = measured(Adaptive, 4096).unwrap();
            readings.reservation_generation = state.memory_generation();
            readings.production_wake_bytes = 1024 * 1_048_576;
            let reservation = state
                .reserve_external(8192, "build", grant.try_into().unwrap(), (mode, readings))
                .await;
            assert_eq!(reservation.is_ok(), admitted);
            if admitted {
                let (mode, mut readings) = measured(Adaptive, 4096).unwrap();
                readings.reservation_generation = state.memory_generation();
                readings.ceilings.insert(app_id(), 1024 * 1_048_576);
                assert!(state
                    .reserve_with_readings(
                        8192,
                        &app_id(),
                        protocol::DEFAULT_INSTANCE_RESOURCES,
                        Some((mode, readings))
                    )
                    .await
                    .is_ok());
            }
            drop(reservation);
        }
    }

    #[tokio::test]
    async fn builds_and_apps_cannot_reserve_the_same_capacity_or_replace_each_other() {
        use crate::config::MemoryAdmissionMode::{Adaptive, Observe};
        for mode in [Observe, Adaptive] {
            let state = HostState::shared();
            let build = state
                .reserve_external(
                    512,
                    app_id().as_str(),
                    512.try_into().unwrap(),
                    measured(mode, 8192).unwrap(),
                )
                .await
                .unwrap();
            assert!(state
                .reserve_with_readings(
                    512,
                    &app_id(),
                    protocol::DEFAULT_INSTANCE_RESOURCES,
                    measured(mode, 8192),
                )
                .await
                .is_err());
            drop(build);
            let app = state
                .reserve_memory(512, &app_id(), protocol::DEFAULT_INSTANCE_RESOURCES)
                .await
                .unwrap();
            assert!(state
                .reserve_external(
                    512,
                    app_id().as_str(),
                    512.try_into().unwrap(),
                    measured(mode, 8192).unwrap(),
                )
                .await
                .is_err());
            drop(app);
        }
    }

    #[tokio::test]
    async fn builds_need_physical_headroom_even_when_app_admission_only_observes() {
        use crate::config::MemoryAdmissionMode::Observe;
        let state = HostState::shared();
        let memory = 512.try_into().unwrap();
        assert!(state
            .reserve_external(8192, "build", memory, measured(Observe, 1200).unwrap())
            .await
            .is_err());
        let mut stale = measured(Observe, 8192).unwrap();
        stale.1.measured_at_ms = crate::clock::now_ms() - 5001;
        assert!(state
            .reserve_external(8192, "build", memory, stale)
            .await
            .is_err());
        assert!(state
            .reserve_external(8192, "build", memory, measured(Observe, 8192).unwrap())
            .await
            .is_ok());
    }

    #[tokio::test]
    async fn stale_or_unrelated_build_samples_cannot_discount_held_capacity() {
        use crate::config::MemoryAdmissionMode::Adaptive;
        for case in 0..4 {
            let state = HostState::shared();
            state.restore_external("build", 1536.try_into().unwrap());
            let mut readings = measured(Adaptive, 2048).unwrap();
            readings.1.reservation_generation = state.memory_generation();
            readings
                .1
                .external_resident_bytes
                .insert(if case == 1 { "other" } else { "build" }.into(), 1024 * 1_048_576);
            if case == 2 {
                readings.1.reservation_generation = 0;
            }
            if case == 3 {
                readings.1.measured_at_ms -= 5001;
            }
            let granted = state
                .reserve_with_readings(
                    8192,
                    &app_id(),
                    protocol::InstanceResources {
                        memory_mib: 256,
                        vcpu_count: 1,
                    },
                    Some(readings),
                )
                .await;
            assert_eq!(granted.is_ok(), case == 0);
        }
    }

    #[tokio::test]
    async fn a_completed_allocation_invalidates_samples_taken_before_its_reservation() {
        use crate::config::MemoryAdmissionMode::Adaptive;
        let state = HostState::shared();
        state
            .put_record(instance_record(|record| {
                record.state = InstanceState::Frozen;
                record.started_at = Some(protocol::Timestamp::from_epoch_ms(crate::clock::now_ms() - 1000));
            }))
            .await;
        let old = frozen_readings(&state);
        let wanted = protocol::DEFAULT_INSTANCE_RESOURCES;
        let other = AppId::parse("app-2").unwrap();
        drop(state.reserve_memory(8192, &other, wanted).await.unwrap());
        assert!(state
            .reserve_with_readings(8192, &other, wanted, Some((Adaptive, old)))
            .await
            .is_err());
        assert!(state
            .reserve_with_readings(8192, &other, wanted, Some((Adaptive, frozen_readings(&state))))
            .await
            .is_ok());
    }

    #[tokio::test]
    async fn a_snapshot_reservation_keeps_a_frozen_apps_full_budget_until_it_releases() {
        use crate::config::MemoryAdmissionMode::Adaptive;
        let state = HostState::shared();
        state
            .put_record(instance_record(|record| {
                record.state = InstanceState::Frozen;
                record.started_at = Some(protocol::Timestamp::from_epoch_ms(crate::clock::now_ms() - 1000));
            }))
            .await;
        let wanted = protocol::DEFAULT_INSTANCE_RESOURCES;
        let other = AppId::parse("app-2").unwrap();
        let snapshot = state.reserve_memory(8192, &app_id(), wanted).await.unwrap();
        assert!(state
            .reserve_with_readings(8192, &other, wanted, Some((Adaptive, frozen_readings(&state))))
            .await
            .is_err());
        drop(snapshot);
        assert!(state
            .reserve_with_readings(8192, &other, wanted, Some((Adaptive, frozen_readings(&state))))
            .await
            .is_ok());
    }

    #[tokio::test]
    async fn adaptive_snapshots_and_starts_cannot_fall_back_to_strict_when_readings_expire() {
        use crate::config::MemoryAdmissionMode::Adaptive;
        use crate::domain::memory_admission::MemoryOperation;
        let state = HostState::shared();
        for operation in [MemoryOperation::Start, MemoryOperation::Snapshot] {
            let mut old = measured(Adaptive, 8192).unwrap();
            old.1.measured_at_ms = crate::clock::now_ms() - 5001;
            assert!(state
                .reserve_for_operation(
                    8192,
                    &app_id(),
                    protocol::DEFAULT_INSTANCE_RESOURCES,
                    operation,
                    Some(old)
                )
                .await
                .is_err());
        }
    }

    #[tokio::test]
    async fn simultaneous_starts_and_wakes_cannot_reserve_the_same_capacity() {
        let state = HostState::shared();
        let wanted = protocol::DEFAULT_INSTANCE_RESOURCES;
        let room = u64::from(wanted.memory_mib);
        let first_id = app_id();
        let second_id = AppId::parse("app-2").unwrap();
        let (first, second) = tokio::join!(
            state.reserve_memory(room, &first_id, wanted),
            state.reserve_memory(room, &second_id, wanted)
        );
        assert_eq!(usize::from(first.is_ok()) + usize::from(second.is_ok()), 1);
        drop(first);
        drop(second);
        assert!(state.reserve_memory(room, &second_id, wanted).await.is_ok());
    }

    #[tokio::test]
    async fn a_reservation_and_its_starting_record_are_counted_once_until_the_guard_is_released() {
        let state = HostState::shared();
        let wanted = protocol::DEFAULT_INSTANCE_RESOURCES;
        let room = u64::from(wanted.memory_mib) * 2;
        let first_id = app_id();
        let second_id = AppId::parse("app-2").unwrap();
        let held = state.reserve_memory(room, &first_id, wanted).await.ok().unwrap();
        state
            .put_record(instance_record(|record| record.state = InstanceState::Starting))
            .await;
        assert!(state.reserve_memory(room, &second_id, wanted).await.is_ok());
        drop(held);
        assert!(state.reserve_memory(room, &second_id, wanted).await.is_ok());
        assert!(state.reserve_memory(room / 2, &second_id, wanted).await.is_err());
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
