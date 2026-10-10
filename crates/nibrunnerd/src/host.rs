use std::sync::Arc;

use protocol::AppId;
use tokio::sync::Mutex;

use crate::adapters::logs::FileLogSink;
use crate::adapters::net::allocator::SlotAllocator;
use crate::adapters::net::firewall::HostFirewall;
use crate::adapters::proxy::activator::AppActivator;
use crate::adapters::proxy::datagram_activator::DatagramActivator;
use crate::adapters::proxy::stream_activator::StreamActivator;
use crate::adapters::proxy::Router;
use crate::adapters::volumes::nbd::NbdDevices;
use crate::adapters::volumes::VolumeBackend;
use crate::config::HostConfig;
use crate::desired::{AcceptedDocument, DesiredStateCache};
use crate::domain::exports::reader::CheckpointServers;
use crate::domain::exports::store::ExportStore;
use crate::domain::memory_admission::MemoryOperation;
use crate::domain::metrics::HostMetrics;
use crate::ports::{ArtifactStore, CommandRunner, PayloadBuilder, Vmm};
use crate::state::SharedState;

pub struct Host {
    pub config: HostConfig,
    pub runtime_policy: Arc<crate::runtime_policy::RuntimePolicy>,
    pub guest_memory_mib: u64,
    pub guest_image_version: String,
    pub state: SharedState,
    pub allocator: Arc<Mutex<SlotAllocator>>,
    pub cache: Mutex<DesiredStateCache>,
    pub vms: Arc<dyn Vmm>,
    /// What every tenant's output is written to, held here as well as behind the receiver so that
    /// a pass can let an app's output go once the app itself is no longer on this host.
    pub logs: Arc<FileLogSink>,
    pub volumes: Arc<dyn VolumeBackend>,
    pub artifacts: Arc<dyn ArtifactStore>,
    pub payloads: Arc<dyn PayloadBuilder>,
    pub repositories: crate::repositories::Repositories,
    pub exports: Option<Arc<dyn ExportStore>>,
    pub checkpoint_servers: Option<CheckpointServers>,
    pub nbd: NbdDevices,
    pub commands: Arc<dyn CommandRunner>,
    pub firewall: Arc<HostFirewall>,
    pub router: Arc<Router>,
    /// What `[proxy.http.tls]` names, read on the way up: a certificate file that is short or
    /// wrong refuses the start, rather than every handshake after it.
    pub tls: Option<tokio_rustls::TlsAcceptor>,
    pub metrics: Arc<HostMetrics>,
    /// Brings an app up for whatever asked for it: a request at the proxy, a raw port, a question
    /// about a guest's files.
    pub waker: Arc<dyn crate::ports::Waker>,
    pub activator: Arc<AppActivator>,
    /// Absent on a host whose configuration names no `[proxy.tcp]`, which is a host that offers
    /// no stream port. A document asking one of those for such a port is refused.
    pub stream_activator: Option<Arc<StreamActivator>>,
    /// The same, for `[proxy.udp]`.
    pub datagram_activator: Option<Arc<DatagramActivator>>,
}

impl Host {
    pub(crate) async fn reserve_memory(
        self: &Arc<Self>,
        app_id: &AppId,
        wanted: protocol::InstanceResources,
        purpose: crate::domain::reconcile::pressure::ReclaimPurpose,
    ) -> Result<crate::state::MemoryReservation, u64> {
        let first = self
            .reserve_memory_once(app_id, wanted, MemoryOperation::Start)
            .await;
        if first.is_ok()
            || !self
                .config
                .memory_admission
                .as_ref()
                .is_some_and(|config| config.reclaim)
        {
            return first;
        }
        let Ok(_reclaiming) =
            tokio::time::timeout(std::time::Duration::from_secs(30), self.state.reclaim.lock()).await
        else {
            return first;
        };
        let mut result = self
            .reserve_memory_once(app_id, wanted, MemoryOperation::Start)
            .await;
        for _ in 0..4 {
            if result.is_ok()
                || !crate::domain::reconcile::pressure::reclaim_one(self, purpose, Some(app_id)).await
            {
                return result;
            }
            result = self
                .reserve_memory_once(app_id, wanted, MemoryOperation::Start)
                .await;
        }
        result
    }

    pub(crate) async fn reserve_memory_once(
        &self,
        app_id: &AppId,
        wanted: protocol::InstanceResources,
        operation: MemoryOperation,
    ) -> Result<crate::state::MemoryReservation, u64> {
        let Some(policy) = &self.config.memory_admission else {
            return self
                .state
                .reserve_for_operation(self.guest_memory_mib, app_id, wanted, operation, None)
                .await;
        };
        let measured = self.memory_readings(Some(app_id)).await;
        if measured.is_none()
            && (policy.mode == crate::config::MemoryAdmissionMode::Adaptive || policy.pool.is_some())
        {
            tracing::warn!(%app_id, "memory admission waits for a host memory reading");
            return Err(u64::from(wanted.memory_mib));
        }
        self.state
            .reserve_for_operation(self.guest_memory_mib, app_id, wanted, operation, measured)
            .await
    }

    pub(crate) async fn memory_readings(
        &self,
        additional_app: Option<&AppId>,
    ) -> Option<(
        crate::config::MemoryAdmissionMode,
        crate::domain::memory_admission::MemoryReadings,
    )> {
        let reservation_generation = self.state.memory_generation();
        let records = self.state.records().await;
        let ids: Vec<_> = records.iter().map(|record| record.app_id.clone()).collect();
        let apps = self.vms.memory(&ids).await;
        let external_resident_bytes = self.state.external_resident_memory();
        let ceilings = ids
            .iter()
            .chain(additional_app)
            .filter_map(|app| {
                self.runtime_policy.vm_budget(app).map(|budget| {
                    let observed = apps
                        .get(app)
                        .and_then(|memory| memory.limits.as_ref())
                        .and_then(|limits| limits.max_bytes)
                        .unwrap_or(0);
                    (
                        app.clone(),
                        (u64::from(budget.memory_mib.get()) * 1_048_576).max(observed),
                    )
                })
            })
            .collect();
        let policy = self.config.memory_admission.as_ref();
        let pool = match policy.and_then(|policy| policy.pool.as_ref()) {
            None => None,
            Some(configuration) => {
                let mut pool = crate::adapters::cgroup::read_pool(configuration).ok()?;
                pool.all_workloads_contained = self.state.external_groups_within(&pool)
                    && records
                        .iter()
                        .filter(|record| crate::domain::report::capacity::holds_something(record))
                        .all(|record| {
                            apps.get(&record.app_id)
                                .and_then(|memory| memory.cgroup.as_deref())
                                .is_some_and(|group| pool.contains(group))
                        });
                Some(pool)
            }
        };
        let mode = policy.map_or(crate::config::MemoryAdmissionMode::Observe, |policy| policy.mode);
        let startup = if let Some(app) = additional_app {
            let desired = self
                .cache
                .lock()
                .await
                .latest()
                .and_then(|desired| desired.instances.iter().find(|i| &i.app_id == app))
                .cloned();
            match desired {
                Some(desired) => Some(self.state.startup_memory(&desired).await),
                None => None,
            }
        } else {
            None
        };
        let mut readings = crate::domain::memory_admission::MemoryReadings {
            startup,
            pool,
            reservation_generation,
            available_bytes: crate::domain::report::capacity::read_memory_available_bytes()?,
            headroom_bytes: u64::from(policy.map_or(1024, |policy| policy.headroom_mib.get())) * 1_048_576,
            production_wake_bytes: 0,
            measured_at_ms: crate::clock::now_ms(),
            apps,
            external_resident_bytes,
            ceilings,
        };
        for record in &records {
            if let Some(peak) = readings.observed_peak_bytes(record) {
                self.state
                    .update_record(&record.app_id, |held| {
                        if held.deployment_id == record.deployment_id && held.resources == record.resources {
                            held.memory_peak_bytes = Some(held.memory_peak_bytes.unwrap_or(0).max(peak));
                        }
                    })
                    .await;
            }
        }
        readings.production_wake_bytes = records
            .iter()
            .filter(|record| {
                record.on_request
                    && record.expired_at_ms.is_none()
                    && matches!(
                        record.state,
                        protocol::InstanceState::Idle | protocol::InstanceState::Frozen
                    )
            })
            .filter(|record| {
                self.runtime_policy
                    .vm_budget(&record.app_id)
                    .and_then(|budget| budget.memory)
                    .is_some_and(|memory| memory.priority == protocol::MemoryPriority::Production)
            })
            .map(|record| {
                readings.wake_headroom_bytes(record, mode == crate::config::MemoryAdmissionMode::Adaptive)
            })
            .max()
            .unwrap_or(0);
        Some((mode, readings))
    }

    pub async fn slot_for(
        &self,
        app_id: &AppId,
    ) -> Result<nft_render::AppSlot, crate::adapters::net::allocator::SlotExhausted> {
        self.allocator.lock().await.allocate(app_id)
    }

    pub async fn slot_of(&self, app_id: &AppId) -> Option<nft_render::AppSlot> {
        self.allocator.lock().await.lookup(app_id)
    }

    /// Widens the slot ring to the document's `maxApps`. It never narrows: a slot handed out stays
    /// addressable until the daemon restarts on a ring that no longer reaches it.
    pub async fn widen_slots(&self, desired: &protocol::HostDesiredState) {
        let Some(asked) = desired.max_apps else {
            return;
        };
        match self.config.widened_to(asked) {
            Ok(max_apps) => self.allocator.lock().await.grow(max_apps),
            Err(error) => tracing::warn!(
                error = %error.message(),
                max_apps = self.config.max_apps,
                "the desired state's maxApps is not taken up; the host keeps the slots config.toml lays it out for"
            ),
        }
    }

    pub async fn slots(&self) -> Vec<nft_render::AppSlot> {
        self.allocator.lock().await.slots()
    }

    pub async fn known_host_id(&self) -> Option<protocol::HostId> {
        let held = self.repositories.identity.read().await.ok()??;
        protocol::HostId::parse(held).ok()
    }

    pub async fn remember_host_id(&self, host_id: &str) {
        if let Err(error) = self.repositories.identity.remember(host_id).await {
            tracing::warn!(error = %error.message(), "this host could not write down the id it registered under");
        }
    }

    pub async fn persist(&self) {
        if let Err(error) = self.write_down().await {
            tracing::warn!(error = %error.message(), "this host could not write down what it is running");
        }
    }

    pub(crate) async fn write_down(&self) -> Result<(), crate::domain::store::StoreError> {
        let _writing = self.state.persistence.lock().await;
        let mut snapshot = self.state.snapshot().await;
        crate::domain::memory_admission::prune_profiles(
            &mut snapshot.memory_profiles,
            crate::clock::now_ms(),
        );
        let records: Vec<_> = snapshot.records.values().cloned().collect();
        let (assignments, cursor) = {
            let allocator = self.allocator.lock().await;
            (allocator.assignments().clone(), allocator.cursor())
        };

        self.repositories.slots.replace_all(&assignments, cursor).await?;
        self.repositories
            .memory_profiles
            .replace_all(&snapshot.memory_profiles)
            .await?;
        self.repositories.instances.replace_all(&records).await?;
        self.repositories
            .activity
            .replace_all(&snapshot.last_active_at_ms)
            .await?;
        self.repositories.meters.replace_all(&snapshot.meters).await?;
        self.repositories
            .deleted_volumes
            .replace_all(&snapshot.deleted_volumes)
            .await
    }

    pub async fn load(&self) {
        let mut records = self.repositories.instances.all().await.unwrap_or_default();
        // A request admitted just before a crash may not have reached the activity repository.
        // Give non-terminal policies a full observed window; durable expiry itself never revives.
        for record in &mut records {
            if record.expiry.is_some() && record.expired_at_ms.is_none() {
                record.expiry_since_ms = Some(crate::clock::now_ms());
            }
        }
        let mut memory_profiles = self.repositories.memory_profiles.all().await.unwrap_or_default();
        for record in &records {
            crate::domain::memory_admission::remember_profile(
                &mut memory_profiles,
                record,
                crate::clock::now_ms(),
            );
        }
        crate::domain::memory_admission::prune_profiles(&mut memory_profiles, crate::clock::now_ms());
        let last_active = self.repositories.activity.all().await.unwrap_or_default();
        let meters = self.repositories.meters.all().await.unwrap_or_default();
        let deleted = self.repositories.deleted_volumes.all().await.unwrap_or_default();
        let assignments = self.repositories.slots.all().await.unwrap_or_default();
        let cursor = self.repositories.slots.cursor().await.unwrap_or_default();
        let accepted = self
            .repositories
            .accepted_document
            .read()
            .await
            .unwrap_or_default();

        let held = records.len();
        // Before the restore, which drops every slot past the ring: a document that widened it
        // handed out slots the configuration alone does not reach.
        if let Some(document) = &accepted {
            self.widen_slots(&document.desired).await;
        }
        self.allocator.lock().await.restore(assignments, cursor);
        self.state
            .modify(|snapshot| {
                snapshot.records = records
                    .into_iter()
                    .map(|record| (record.app_id.clone(), record))
                    .collect();
                snapshot.memory_profiles = memory_profiles;
                snapshot.last_active_at_ms = last_active;
                snapshot.meters = meters;
                snapshot.deleted_volumes = deleted;
                if let Some(document) = accepted {
                    snapshot.accepted_revision = Some(document.desired.revision.clone());
                    snapshot.accepted_digest = Some(document.digest);
                }
            })
            .await;
        tracing::info!(
            instances = held,
            slots = self.slots().await.len(),
            "host state loaded"
        );
    }

    pub async fn accepted_document(&self) -> Option<AcceptedDocument> {
        match self.repositories.accepted_document.read().await {
            Ok(held) => held,
            Err(error) => {
                tracing::warn!(error = %error.message(), "the document this host last took up could not be read back");
                None
            }
        }
    }

    /// The row is written only when the bytes moved: a document read again unchanged — which the
    /// watch's backstop brings round every half minute — is the one already there.
    pub async fn remember_accepted_document(&self, document: &AcceptedDocument) {
        let moved = self
            .state
            .modify(|snapshot| {
                let moved = snapshot.accepted_digest.as_ref() != Some(&document.digest);
                snapshot.accepted_digest = Some(document.digest.clone());
                snapshot.accepted_revision = Some(document.desired.revision.clone());
                moved
            })
            .await;
        if !moved {
            return;
        }
        if let Err(error) = self.repositories.accepted_document.remember(document).await {
            tracing::warn!(error = %error.message(), "this host could not write down the document it took up");
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use crate::domain::store::StoreError;
    use crate::repositories::accepted_document_repository::MockAcceptedDocumentRepository;
    use crate::repositories::activity_repository::MockActivityRepository;
    use crate::repositories::deleted_volumes_repository::MockDeletedVolumeRepository;
    use crate::repositories::host_identity_repository::MockHostIdentityRepository;
    use crate::repositories::instances_repository::MockInstanceRepository;
    use crate::repositories::meters_repository::MockMeterRepository;
    use crate::repositories::slots_repository::MockSlotRepository;
    use crate::repositories::Repositories;
    use crate::test_support::*;

    fn mocked() -> (
        MockInstanceRepository,
        MockSlotRepository,
        MockActivityRepository,
        MockDeletedVolumeRepository,
        MockHostIdentityRepository,
    ) {
        (
            MockInstanceRepository::new(),
            MockSlotRepository::new(),
            MockActivityRepository::new(),
            MockDeletedVolumeRepository::new(),
            MockHostIdentityRepository::new(),
        )
    }

    // Nothing a test of the other repositories asserts about, so it answers and counts nothing.
    fn quiet_meters() -> MockMeterRepository {
        let mut meters = MockMeterRepository::new();
        meters.expect_all().returning(|| Ok(Default::default()));
        meters.expect_replace_all().returning(|_| Ok(()));
        meters
    }

    fn quiet_accepted_document() -> MockAcceptedDocumentRepository {
        let mut accepted = MockAcceptedDocumentRepository::new();
        accepted.expect_read().returning(|| Ok(None));
        accepted.expect_remember().returning(|_| Ok(()));
        accepted
    }

    fn bundle(
        instances: MockInstanceRepository,
        slots: MockSlotRepository,
        activity: MockActivityRepository,
        deleted_volumes: MockDeletedVolumeRepository,
        identity: MockHostIdentityRepository,
    ) -> Repositories {
        let mut profiles =
            crate::repositories::memory_profiles_repository::MockMemoryProfileRepository::new();
        profiles.expect_all().returning(|| Ok(Default::default()));
        profiles.expect_replace_all().returning(|_| Ok(()));
        Repositories {
            memory_profiles: Arc::new(profiles),
            instances: Arc::new(instances),
            slots: Arc::new(slots),
            activity: Arc::new(activity),
            meters: Arc::new(quiet_meters()),
            deleted_volumes: Arc::new(deleted_volumes),
            identity: Arc::new(identity),
            accepted_document: Arc::new(quiet_accepted_document()),
        }
    }

    #[tokio::test]
    async fn a_document_read_again_unchanged_is_not_written_down_a_second_time() {
        let (instances, slots, activity, deleted, identity) = mocked();
        let written = Arc::new(AtomicUsize::new(0));
        let counted = written.clone();
        let mut accepted = MockAcceptedDocumentRepository::new();
        accepted.expect_remember().returning(move |_| {
            counted.fetch_add(1, Ordering::SeqCst);
            Ok(())
        });
        let mut repositories = bundle(instances, slots, activity, deleted, identity);
        repositories.accepted_document = Arc::new(accepted);
        let host = test_host_with(repositories).await;

        let document = accepted_document(desired_state(|_| {}));
        host.remember_accepted_document(&document).await;
        host.remember_accepted_document(&document).await;
        assert_eq!(written.load(Ordering::SeqCst), 1);
        assert_eq!(host.state.snapshot().await.accepted_digest, Some(document.digest));

        let later = accepted_document(desired_state(|state| {
            state.instances = vec![desired_instance(|_| {})]
        }));
        host.remember_accepted_document(&later).await;
        assert_eq!(written.load(Ordering::SeqCst), 2);
        assert_eq!(host.state.snapshot().await.accepted_digest, Some(later.digest));
    }

    #[tokio::test]
    async fn a_pass_writes_the_slots_before_the_records_that_depend_on_them() {
        let order = Arc::new(AtomicUsize::new(0));
        let (mut instances, mut slots, mut activity, mut deleted, identity) = mocked();

        let at = order.clone();
        let slots_written = Arc::new(AtomicUsize::new(usize::MAX));
        let recorded = slots_written.clone();
        slots.expect_replace_all().returning(move |_, _| {
            recorded.store(at.fetch_add(1, Ordering::SeqCst), Ordering::SeqCst);
            Ok(())
        });
        let at = order.clone();
        let instances_written = Arc::new(AtomicUsize::new(usize::MAX));
        let recorded = instances_written.clone();
        instances.expect_replace_all().returning(move |_| {
            recorded.store(at.fetch_add(1, Ordering::SeqCst), Ordering::SeqCst);
            Ok(())
        });
        activity.expect_replace_all().returning(|_| Ok(()));
        deleted.expect_replace_all().returning(|_| Ok(()));

        let host = test_host_with(bundle(instances, slots, activity, deleted, identity)).await;
        host.persist().await;

        assert!(
            slots_written.load(Ordering::SeqCst) < instances_written.load(Ordering::SeqCst),
            "a record written before its slot is one the next pass allocates a second slot for"
        );
    }

    #[tokio::test]
    async fn a_pass_hands_every_repository_what_the_host_is_holding() {
        let (mut instances, mut slots, mut activity, mut deleted, identity) = mocked();
        instances
            .expect_replace_all()
            .times(1)
            .withf(|records| records.len() == 1 && records[0].app_id == app_id())
            .returning(|_| Ok(()));
        slots
            .expect_replace_all()
            .times(1)
            .withf(|assignments, cursor| assignments.get(&app_id()) == Some(&0) && *cursor == 1)
            .returning(|_, _| Ok(()));
        activity
            .expect_replace_all()
            .times(1)
            .withf(|held| held.get(&app_id()) == Some(&77))
            .returning(|_| Ok(()));
        deleted
            .expect_replace_all()
            .times(1)
            .withf(|held| held.contains_key(&volume_id()))
            .returning(|_| Ok(()));

        let host = test_host_with(bundle(instances, slots, activity, deleted, identity)).await;
        host.slot_for(&app_id()).await.unwrap();
        host.state
            .modify(|snapshot| {
                snapshot.records = BTreeMap::from([(app_id(), instance_record(|_| {}))]);
                snapshot.last_active_at_ms = BTreeMap::from([(app_id(), 77)]);
                snapshot.deleted_volumes = BTreeMap::from([(volume_id(), reported_volume(|_| {}))]);
            })
            .await;
        host.persist().await;
    }

    #[tokio::test]
    async fn a_write_that_failed_leaves_the_host_running_rather_than_taking_it_down() {
        let (mut instances, mut slots, activity, deleted, identity) = mocked();
        slots
            .expect_replace_all()
            .returning(|_, _| Err(StoreError::Unwritable("the disk is full".into())));
        instances.expect_replace_all().never();

        let host = test_host_with(bundle(instances, slots, activity, deleted, identity)).await;
        host.persist().await;
    }

    #[tokio::test]
    async fn what_was_written_down_is_what_the_host_comes_back_holding() {
        let (mut instances, mut slots, mut activity, mut deleted, identity) = mocked();
        instances
            .expect_all()
            .returning(|| Ok(vec![instance_record(|_| {})]));
        slots
            .expect_all()
            .returning(|| Ok(BTreeMap::from([(app_id(), 4)])));
        slots.expect_cursor().returning(|| Ok(5));
        activity
            .expect_all()
            .returning(|| Ok(BTreeMap::from([(app_id(), 77)])));
        deleted
            .expect_all()
            .returning(|| Ok(BTreeMap::from([(volume_id(), reported_volume(|_| {}))])));

        let host = test_host_with(bundle(instances, slots, activity, deleted, identity)).await;
        host.load().await;

        assert!(host.state.record(&app_id()).await.is_some());
        assert_eq!(host.slot_of(&app_id()).await.map(|slot| slot.slot), Some(4));
        let snapshot = host.state.snapshot().await;
        assert_eq!(snapshot.last_active_at_ms.get(&app_id()), Some(&77));
        assert!(snapshot.deleted_volumes.contains_key(&volume_id()));
    }

    #[tokio::test]
    async fn a_slot_past_max_apps_that_a_document_widened_the_ring_to_is_kept_across_a_restart() {
        let host = test_host().await;
        let past = host.config.max_apps;
        let document = accepted_document(desired_state(|state| state.max_apps = Some(past + 1)));
        host.remember_accepted_document(&document).await;
        host.widen_slots(&document.desired).await;
        let app = protocol::AppId::parse("past-the-file").unwrap();
        host.allocator
            .lock()
            .await
            .restore(BTreeMap::from([(app.clone(), past)]), 0);
        host.persist().await;

        *host.allocator.lock().await =
            crate::adapters::net::allocator::SlotAllocator::addressing(host.config.max_apps);
        host.load().await;

        assert_eq!(host.slot_of(&app).await.map(|slot| slot.slot), Some(past));
    }

    #[tokio::test]
    async fn a_host_whose_notes_will_not_be_read_comes_up_knowing_nothing_rather_than_not_at_all() {
        let (mut instances, mut slots, mut activity, mut deleted, identity) = mocked();
        let unreadable = || StoreError::Unreadable("the file is not a database".into());
        instances.expect_all().returning(move || Err(unreadable()));
        slots.expect_all().returning(move || Err(unreadable()));
        slots.expect_cursor().returning(move || Err(unreadable()));
        activity.expect_all().returning(move || Err(unreadable()));
        deleted.expect_all().returning(move || Err(unreadable()));

        let host = test_host_with(bundle(instances, slots, activity, deleted, identity)).await;
        host.load().await;
        assert!(host.state.records().await.is_empty());
    }

    #[tokio::test]
    async fn the_id_the_control_plane_assigned_is_written_once_and_read_back() {
        let (instances, slots, activity, deleted, mut identity) = mocked();
        identity
            .expect_remember()
            .times(1)
            .withf(|host_id| host_id == "host-7")
            .returning(|_| Ok(()));
        identity
            .expect_read()
            .returning(|| Ok(Some("host-7".to_string())));

        let host = test_host_with(bundle(instances, slots, activity, deleted, identity)).await;
        host.remember_host_id("host-7").await;
        assert_eq!(
            host.known_host_id().await.map(|held| held.as_str().to_string()),
            Some("host-7".to_string())
        );
    }

    #[tokio::test]
    async fn a_host_id_the_notes_cannot_hold_is_absent_rather_than_wrong() {
        let (instances, slots, activity, deleted, mut identity) = mocked();
        identity
            .expect_read()
            .returning(|| Ok(Some("not a host id".to_string())));

        let host = test_host_with(bundle(instances, slots, activity, deleted, identity)).await;
        assert_eq!(host.known_host_id().await, None);
    }

    #[tokio::test]
    async fn an_id_that_could_not_be_written_is_logged_rather_than_raised() {
        let (instances, slots, activity, deleted, mut identity) = mocked();
        identity
            .expect_remember()
            .returning(|_| Err(StoreError::Unwritable("read-only".into())));

        let host = test_host_with(bundle(instances, slots, activity, deleted, identity)).await;
        host.remember_host_id("host-7").await;
    }
}
