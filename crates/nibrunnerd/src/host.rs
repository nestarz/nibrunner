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
use crate::domain::metrics::HostMetrics;
use crate::ports::{ArtifactStore, CommandRunner, PayloadBuilder, Vmm};
use crate::state::SharedState;

pub struct Host {
    pub cron: Arc<crate::domain::cron::registry::CronRegistry>,
    pub cron_runs: Arc<crate::domain::cron::runs::CronRuns>,
    pub config: HostConfig,
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
    pub exports: Arc<dyn ExportStore>,
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
    /// Under `[memory_admission] mode = "observe"`, what a measured decision would have said about
    /// a start or a wake the ceilings decided, logged beside it. It never changes the decision.
    pub(crate) async fn observe_memory_admission(
        &self,
        app_id: &AppId,
        wanted: &protocol::InstanceResources,
        records: &[crate::domain::report::InstanceRecord],
        strict_shortfall_mib: u64,
    ) {
        let Some(policy) = &self.config.memory_admission else {
            return;
        };
        let Some(available_bytes) = crate::domain::report::capacity::read_memory_available_bytes() else {
            return;
        };
        let others: Vec<_> = records
            .iter()
            .filter(|record| &record.app_id != app_id)
            .cloned()
            .collect();
        let ids: Vec<_> = others.iter().map(|record| record.app_id.clone()).collect();
        let readings = crate::domain::memory_admission::MemoryReadings {
            available_bytes,
            headroom_bytes: u64::from(policy.headroom_mib.get()) * 1_048_576,
            measured_at_ms: crate::clock::now_ms(),
            apps: self.vms.memory(&ids).await,
        };
        let measured_shortfall_mib = readings.shortfall_mib(
            self.guest_memory_mib,
            &others,
            crate::domain::memory_admission::MemoryReadings::ceiling(*wanted),
        );
        tracing::info!(
            %app_id,
            mode = ?policy.mode,
            strict_shortfall_mib,
            measured_shortfall_mib,
            "memory admission evaluated"
        );
    }

    /// Takes up every instance's `limits` without a new deployment: the proxy's per-instance
    /// concurrency for the next request, and a running microVM's cgroup and OOM score now. A boot
    /// or a wake applies what the record holds, so an instance asleep picks them up as it wakes.
    pub(crate) async fn apply_instance_limits(&self, desired: &protocol::HostDesiredState) {
        self.router.set_instance_concurrency(
            desired
                .instances
                .iter()
                .filter_map(|instance| {
                    instance
                        .limits
                        .and_then(|limits| limits.concurrent)
                        .map(|limit| (instance.app_id.clone(), limit))
                })
                .collect(),
        );
        for instance in &desired.instances {
            let Some(record) = self.state.record(&instance.app_id).await else {
                continue;
            };
            if record.deployment_id != instance.deployment_id || record.limits == instance.limits {
                continue;
            }
            self.state
                .update_record(&instance.app_id, |record| record.limits = instance.limits)
                .await;
            if let Err(error) = self
                .vms
                .apply_limits(&instance.app_id, record.resources, instance.limits)
                .await
            {
                tracing::warn!(app_id = %instance.app_id, error = %error.message(), "limits not applied");
            }
        }
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

    async fn write_down(&self) -> Result<(), crate::domain::store::StoreError> {
        let snapshot = self.state.snapshot().await;
        let records: Vec<_> = snapshot.records.values().cloned().collect();
        let (assignments, cursor) = {
            let allocator = self.allocator.lock().await;
            (allocator.assignments().clone(), allocator.cursor())
        };

        self.repositories.slots.replace_all(&assignments, cursor).await?;
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
        let records = self.repositories.instances.all().await.unwrap_or_default();
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
        if let Some(document) = &accepted {
            self.remember_host_id(document.desired.host_id.as_str()).await;
        }

        let held = records.len();
        self.allocator.lock().await.restore(assignments, cursor);
        self.state
            .modify(|snapshot| {
                snapshot.records = records
                    .into_iter()
                    .map(|record| (record.app_id.clone(), record))
                    .collect();
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
        self.remember_host_id(document.desired.host_id.as_str()).await;
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

    fn limited(concurrent: u16) -> protocol::InstanceLimits {
        protocol::InstanceLimits {
            concurrent: Some(concurrent.try_into().unwrap()),
            cpu_percent: Some(25.try_into().unwrap()),
            memory: Some(protocol::MemoryPolicy {
                target_mib: 128.try_into().unwrap(),
                swap_mib: 0,
                priority: protocol::MemoryPriority::Preview,
            }),
        }
    }

    #[tokio::test]
    async fn limits_a_document_changes_reach_the_record_without_a_new_deployment() {
        let host = test_host().await;
        host.state.put_record(instance_record(|_| {})).await;
        let mut desired = desired_state(|state| {
            state.instances = vec![desired_instance(|instance| instance.limits = Some(limited(2)))]
        });
        host.apply_instance_limits(&desired).await;
        assert_eq!(
            host.state.record(&app_id()).await.unwrap().limits,
            Some(limited(2))
        );

        desired.instances[0].limits = None;
        host.apply_instance_limits(&desired).await;
        assert_eq!(host.state.record(&app_id()).await.unwrap().limits, None);
        assert_eq!(host.vms.calls().len(), 2);
    }

    #[tokio::test]
    async fn another_deployments_limits_are_left_for_its_own_boot() {
        let host = test_host().await;
        host.state.put_record(instance_record(|_| {})).await;
        let desired = desired_state(|state| {
            state.instances = vec![desired_instance(|instance| {
                instance.deployment_id = protocol::DeploymentId::parse("dep-2").unwrap();
                instance.limits = Some(limited(2));
            })]
        });
        host.apply_instance_limits(&desired).await;
        assert_eq!(host.state.record(&app_id()).await.unwrap().limits, None);
        assert!(host.vms.calls().is_empty());
    }

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
        let mut cron = crate::repositories::cron_repository::MockCronRepository::new();
        cron.expect_all().returning(|| Ok(Vec::new()));
        cron.expect_replace_all().returning(|_| Ok(()));
        Repositories {
            cron: Arc::new(cron),
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
        let (instances, slots, activity, deleted, mut identity) = mocked();
        identity.expect_remember().returning(|_| Ok(()));
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
    async fn an_identity_write_that_failed_is_retried_when_the_document_has_not_moved() {
        let (instances, slots, activity, deleted, mut identity) = mocked();
        let mut sequence = mockall::Sequence::new();
        identity
            .expect_remember()
            .times(1)
            .in_sequence(&mut sequence)
            .withf(|host_id| host_id == "hetzner-test")
            .returning(|_| Err(StoreError::Unwritable("the disk is full".into())));
        identity
            .expect_remember()
            .times(1)
            .in_sequence(&mut sequence)
            .withf(|host_id| host_id == "hetzner-test")
            .returning(|_| Ok(()));
        let host = test_host_with(bundle(instances, slots, activity, deleted, identity)).await;
        let document = accepted_document(desired_state(|state| {
            state.host_id = protocol::HostId::parse("hetzner-test").unwrap();
        }));

        host.remember_accepted_document(&document).await;
        host.remember_accepted_document(&document).await;
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
