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
#[derive(Clone, Copy)]
struct ReservedMemory {
    resources: protocol::InstanceResources,
    bytes: u64,
}

type MemoryReservations = Arc<Mutex<BTreeMap<AppId, ReservedMemory>>>;

pub(crate) struct MemoryReservation {
    app_id: AppId,
    reservations: MemoryReservations,
}

impl Drop for MemoryReservation {
    fn drop(&mut self) {
        self.reservations
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(&self.app_id);
    }
}

pub struct HostState {
    snapshot: RwLock<HostSnapshot>,
    refresh: Notify,
    report: Notify,
    transitions: Mutex<Transitions>,
    memory_reservations: MemoryReservations,
    pub(crate) persistence: tokio::sync::Mutex<()>,
    pub(crate) reclaim: tokio::sync::Mutex<()>,
}

impl HostState {
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
    pub(crate) async fn reserve_memory(
        &self,
        capacity_mib: u64,
        app_id: &AppId,
        wanted: protocol::InstanceResources,
    ) -> Result<MemoryReservation, u64> {
        self.reserve_with_readings(capacity_mib, app_id, wanted, None)
            .await
    }

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
        let snapshot = self.snapshot.read().await;
        let mut reservations = self
            .memory_reservations
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let records: Vec<_> = snapshot
            .records
            .values()
            .filter(|record| &record.app_id != app_id && !reservations.contains_key(&record.app_id))
            .cloned()
            .collect();
        let mut committed = crate::domain::report::capacity::committed_resources(&records);
        committed.extend(reservations.values().map(|reserved| reserved.resources));
        let strict_shortfall =
            crate::domain::report::capacity::memory_shortfall_mib(capacity_mib, &committed, &wanted);
        let mut shortfall = strict_shortfall;
        let mut bytes = (u64::from(wanted.memory_mib) + 64) * 1_048_576;
        if let Some((mode, readings)) =
            measured.filter(|(_, readings)| readings.is_fresh(crate::clock::now_ms()))
        {
            bytes = readings.ceiling(app_id, wanted);
            let reserved_bytes = reservations
                .values()
                .fold(0u64, |total, held| total.saturating_add(held.bytes));
            let measured_shortfall = readings.shortfall_mib(capacity_mib, &records, reserved_bytes, bytes);
            tracing::info!(%app_id, ?mode, strict_shortfall_mib = strict_shortfall,
                measured_shortfall_mib = measured_shortfall, "memory admission evaluated");
            if mode == crate::config::MemoryAdmissionMode::Adaptive {
                shortfall = measured_shortfall;
            }
        }
        if shortfall > 0 {
            return Err(shortfall);
        }
        reservations.insert(
            app_id.clone(),
            ReservedMemory {
                resources: wanted,
                bytes,
            },
        );
        Ok(MemoryReservation {
            app_id: app_id.clone(),
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
        if snapshot
            .records
            .get(app_id)
            .is_some_and(|r| r.expired_at_ms.is_some())
        {
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
                available_bytes: available_mib * 1_048_576,
                headroom_bytes: 1024 * 1_048_576,
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
