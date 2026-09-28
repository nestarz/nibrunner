use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;

use protocol::HostDesiredState;

use crate::controllers::Controller;
use crate::desired::{Changes, DesiredStateWatch};
use crate::domain::metrics::converge::{self, Cause};
use crate::domain::metrics::passes::Trigger;
use crate::host::Host;
use crate::services::reconcile_service::ReconcileService;

/// How long a quiet host goes between passes. The watch runs one when the document moves, and
/// only a pass sees what moved on the host under a document that did not — a device that died
/// under its guest, a stop that finished so a volume can go. Ten guests once sat on dead disks,
/// reported running, for the four minutes until an unrelated edit ran one. A pass over a host
/// that is what its document asks for is milliseconds, so this can be short.
pub const RECONCILE_TICK: Duration = Duration::from_secs(5);

pub struct ConvergeController {
    host: Arc<Host>,
    reconciler: Arc<dyn ReconcileService>,
}

impl ConvergeController {
    pub fn new(host: Arc<Host>, reconciler: Arc<dyn ReconcileService>) -> Arc<Self> {
        Arc::new(Self { host, reconciler })
    }

    pub async fn converge_accepted(&self) -> bool {
        let Some(accepted) = self.host.accepted_document().await else {
            return false;
        };
        let Some(changes) = self.accept(&accepted.desired).await else {
            return false;
        };
        converge::detected(&self.host, &changes, Cause::Restart, crate::clock::now_ms()).await;
        self.reconcile(&accepted.desired, Trigger::Restart).await;
        true
    }

    pub async fn converge_once(&self) -> bool {
        match crate::desired::read_desired_state(&self.host.config.desired_state_file) {
            Ok(Some(document)) => {
                // A readable document clears any refusal the report was still carrying.
                self.forget_refusal().await;
                // Taken up before anything is converged on, and whether or not the ask moved: a
                // document rewritten to the same ask is still the one the report answers for.
                self.host.remember_accepted_document(&document).await;
                let desired = document.desired;
                let trigger = match self.accept(&desired).await {
                    Some(changes) => {
                        converge::detected(&self.host, &changes, Cause::Change, crate::clock::now_ms()).await;
                        Trigger::Change
                    }
                    None if !self.host.state.snapshot().await.deferred_work => return false,
                    None => Trigger::Deferred,
                };
                self.reconcile(&desired, trigger).await;
                true
            }
            Ok(None) => false,
            Err(error) => {
                self.host.metrics.passes.desired_state_unreadable();
                let message = error.message();
                tracing::error!(error = %message, "the desired state file was not read");
                self.note_refusal(message).await;
                false
            }
        }
    }

    /// The timer went off: a full pass over the document this host last took up, so that what
    /// moved on the host while the document stood still is seen. The file is the watch's to
    /// read, and a document it refused is refused once rather than once a tick.
    pub async fn converge_on_tick(&self) -> bool {
        let latest = self.host.cache.lock().await.latest().cloned();
        let Some(desired) = latest else {
            return false;
        };
        let trigger = if self.host.state.snapshot().await.deferred_work {
            Trigger::Deferred
        } else {
            Trigger::Tick
        };
        self.reconcile(&desired, trigger).await;
        true
    }

    /// Records that the last document was refused, so the next report carries it. The counter
    /// says how often; this says which document and why, for an operator reading only the file.
    async fn note_refusal(&self, why: String) {
        let message = protocol::StateMessage::new(why);
        self.host
            .state
            .modify(|snapshot| snapshot.desired_refusal = Some(message))
            .await;
    }

    async fn forget_refusal(&self) {
        self.host
            .state
            .modify(|snapshot| snapshot.desired_refusal = None)
            .await;
    }

    async fn reconcile(&self, desired: &HostDesiredState, trigger: Trigger) {
        let started = std::time::Instant::now();
        self.reconciler.reconcile(desired, trigger).await;
        self.host
            .metrics
            .passes
            .reconciled(trigger, started.elapsed(), crate::clock::now_ms());
    }

    /// What moved, when the document did; nothing when it stood still.
    async fn accept(&self, desired: &HostDesiredState) -> Option<Changes> {
        let mut cache = self.host.cache.lock().await;
        let changes = cache.changes_in(desired);
        self.host.runtime_policy.replace_instances(&desired.instances);
        cache.accept(desired.clone()).then_some(changes)
    }
}

#[async_trait]
impl Controller for ConvergeController {
    fn name(&self) -> &'static str {
        "converge"
    }

    async fn run(&self) {
        let watch = DesiredStateWatch::on(&self.host.config.desired_state_file);
        self.converge_accepted().await;
        self.converge_once().await;
        // The watch outlives a tick so that its backstop, which is the only way a host without
        // inotify hears of the document moving, is not put off by a timer that fires first.
        let mut changed = std::pin::pin!(watch.changed());
        loop {
            tokio::select! {
                _ = &mut changed => {
                    self.converge_once().await;
                    changed.set(watch.changed());
                }
                _ = tokio::time::sleep(RECONCILE_TICK) => {
                    self.converge_on_tick().await;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::services::reconcile_service::MockReconcileService;
    use crate::test_support::*;

    fn controller(host: &TestHost, reconciler: MockReconcileService) -> Arc<ConvergeController> {
        ConvergeController::new(host.arc().clone(), Arc::new(reconciler))
    }

    #[tokio::test]
    async fn limits_are_taken_up_without_starting_a_new_deployment() {
        let host = test_host().await;
        let controller = controller(&host, MockReconcileService::new());
        let mut desired = desired_state(|state| state.instances = vec![desired_instance(|_| {})]);
        controller.accept(&desired).await;
        desired.instances[0].limits = Some(protocol::InstanceLimits {
            concurrent: 3.try_into().unwrap(),
            cpu_percent: 150.try_into().unwrap(),
            memory_mib: 640.try_into().unwrap(),
        });
        assert_eq!(controller.accept(&desired).await, Some(Changes::default()));
        assert_eq!(host.runtime_policy.http_limits(&app_id()).unwrap().1, 3);
        assert_eq!(
            host.runtime_policy
                .vm_budget(&app_id())
                .unwrap()
                .cpu_percent
                .get(),
            150
        );
        desired.instances[0].limits = None;
        assert_eq!(controller.accept(&desired).await, Some(Changes::default()));
        assert_eq!(host.runtime_policy.vm_budget(&app_id()), None);
    }

    #[tokio::test]
    async fn a_document_that_appeared_is_converged_on() {
        let host = test_host().await;
        let desired = desired_state(|state| state.instances = vec![desired_instance(|_| {})]);
        write_desired_state(&host.config.desired_state_file, &desired);

        let mut reconciler = MockReconcileService::new();
        let wanted = desired.clone();
        reconciler
            .expect_reconcile()
            .times(1)
            .withf(move |seen, _| *seen == wanted)
            .returning(|_, _| ());

        assert!(controller(&host, reconciler).converge_once().await);
    }

    #[tokio::test]
    async fn the_moment_a_change_is_noticed_is_written_down_before_the_pass_sets_out() {
        let host = test_host().await;
        let desired = desired_state(|state| state.instances = vec![desired_instance(|_| {})]);
        write_desired_state(&host.config.desired_state_file, &desired);
        let mut reconciler = MockReconcileService::new();
        reconciler.expect_reconcile().times(1).returning(|_, _| ());

        let before = crate::clock::now_ms();
        assert!(controller(&host, reconciler).converge_once().await);

        let deploy = &host.state.snapshot().await.deploys[&app_id()];
        assert_eq!(deploy.cause, Cause::Change);
        assert!(deploy.detected_at_ms >= before);
        assert!(deploy.is_open());
        let page = page(&host).await;
        assert!(
            page.contains("nibrunner_reconcile_seconds_count{trigger=\"change\"} 1\n"),
            "{page}"
        );
        assert!(!page.contains("nibrunner_reconcile_last_pass_timestamp_seconds 0.000\n"));
    }

    #[tokio::test]
    async fn a_file_broken_while_the_daemon_was_down_leaves_the_document_it_took_up_running() {
        let host = test_host().await;
        let desired = desired_state(|state| state.instances = vec![desired_instance(|_| {})]);
        write_desired_state(&host.config.desired_state_file, &desired);
        let mut reconciler = MockReconcileService::new();
        reconciler.expect_reconcile().times(1).returning(|_, _| ());
        assert!(controller(&host, reconciler).converge_once().await);

        std::fs::write(&host.config.desired_state_file, b"{").unwrap();
        // A restart keeps the store and forgets everything else.
        *host.cache.lock().await = crate::desired::DesiredStateCache::new();
        let wanted = desired.clone();
        let mut reconciler = MockReconcileService::new();
        reconciler
            .expect_reconcile()
            .times(1)
            .withf(move |seen, trigger| *seen == wanted && *trigger == Trigger::Restart)
            .returning(|_, _| ());
        let restarted = controller(&host, reconciler);

        assert!(restarted.converge_accepted().await);
        assert!(
            !restarted.converge_once().await,
            "the file is refused as it would be on a running daemon, and nothing else moves"
        );
    }

    #[tokio::test]
    async fn what_a_restart_sets_out_on_is_measured_as_a_restart() {
        let host = test_host().await;
        let desired = desired_state(|state| state.instances = vec![desired_instance(|_| {})]);
        host.repositories
            .accepted_document
            .remember(&accepted_document(desired.clone()))
            .await
            .unwrap();
        let mut reconciler = MockReconcileService::new();
        reconciler.expect_reconcile().times(1).returning(|_, _| ());

        assert!(controller(&host, reconciler).converge_accepted().await);
        assert_eq!(
            host.state.snapshot().await.deploys[&app_id()].cause,
            Cause::Restart
        );
        assert!(page(&host)
            .await
            .contains("nibrunner_reconcile_seconds_count{trigger=\"restart\"} 1\n"));
    }

    async fn page(host: &TestHost) -> String {
        crate::domain::metrics::tests::page(
            &crate::domain::metrics::tests::report(),
            &host.metrics,
            &host.state.snapshot().await,
            0,
        )
    }

    #[tokio::test]
    async fn a_missing_document_is_the_ordinary_state_of_a_fresh_host() {
        let host = test_host().await;
        let mut reconciler = MockReconcileService::new();
        reconciler.expect_reconcile().never();
        assert!(!controller(&host, reconciler).converge_once().await);
    }

    #[tokio::test]
    async fn a_document_that_has_not_moved_does_not_run_a_second_pass() {
        let host = test_host().await;
        write_desired_state(&host.config.desired_state_file, &desired_state(|_| {}));

        let mut reconciler = MockReconcileService::new();
        reconciler.expect_reconcile().times(1).returning(|_, _| ());
        let controller = controller(&host, reconciler);
        assert!(controller.converge_once().await);
        assert!(!controller.converge_once().await);
    }

    #[tokio::test]
    async fn work_the_last_pass_deferred_is_carried_even_though_the_document_stood_still() {
        let host = test_host().await;
        write_desired_state(&host.config.desired_state_file, &desired_state(|_| {}));

        let mut reconciler = MockReconcileService::new();
        reconciler.expect_reconcile().times(2).returning(|_, _| ());
        let controller = controller(&host, reconciler);
        assert!(controller.converge_once().await);
        host.state.modify(|snapshot| snapshot.deferred_work = true).await;
        assert!(controller.converge_once().await);
        assert!(page(&host)
            .await
            .contains("nibrunner_reconcile_seconds_count{trigger=\"deferred\"} 1\n"));
    }

    #[tokio::test]
    async fn a_document_that_cannot_be_read_is_logged_rather_than_converged_on() {
        let host = test_host().await;
        crate::json_store::make_directory(host.config.desired_state_file.parent().unwrap(), 0o700).unwrap();
        std::fs::write(&host.config.desired_state_file, b"not json at all").unwrap();

        let mut reconciler = MockReconcileService::new();
        reconciler.expect_reconcile().never();
        assert!(!controller(&host, reconciler).converge_once().await);
        assert!(page(&host)
            .await
            .contains("nibrunner_desired_state_unreadable_total 1\n"));
    }

    #[tokio::test]
    async fn a_refused_document_is_left_on_the_report_until_a_readable_one_replaces_it() {
        let host = test_host().await;
        crate::json_store::make_directory(host.config.desired_state_file.parent().unwrap(), 0o700).unwrap();
        // Valid JSON, but not the document this host reads.
        std::fs::write(&host.config.desired_state_file, br#"{"hostId":"host-1"}"#).unwrap();

        let mut reconciler = MockReconcileService::new();
        reconciler.expect_reconcile().never();
        assert!(!controller(&host, reconciler).converge_once().await);

        let refusal = host
            .state
            .snapshot()
            .await
            .desired_refusal
            .expect("the refusal is surfaced on the report, not just in the log and the counter");
        assert!(
            refusal.as_str().contains("cannot read"),
            "the reason the document was refused is there: {}",
            refusal.as_str()
        );

        // A readable document clears it, so the report stops claiming the last write did not land.
        write_desired_state(
            &host.config.desired_state_file,
            &desired_state(|state| state.instances = vec![desired_instance(|_| {})]),
        );
        let mut reconciler = MockReconcileService::new();
        reconciler.expect_reconcile().times(1).returning(|_, _| ());
        assert!(controller(&host, reconciler).converge_once().await);
        assert!(host.state.snapshot().await.desired_refusal.is_none());
    }

    #[tokio::test]
    async fn the_last_document_this_host_was_given_is_what_a_restart_converges_on_first() {
        let host = test_host().await;
        host.repositories
            .accepted_document
            .remember(&accepted_document(desired_state(|_| {})))
            .await
            .unwrap();

        let mut reconciler = MockReconcileService::new();
        reconciler.expect_reconcile().times(1).returning(|_, _| ());
        assert!(controller(&host, reconciler).converge_accepted().await);
    }

    #[tokio::test]
    async fn a_host_that_was_never_given_a_document_has_nothing_cached_to_converge_on() {
        let host = test_host().await;
        let mut reconciler = MockReconcileService::new();
        reconciler.expect_reconcile().never();
        assert!(!controller(&host, reconciler).converge_accepted().await);
    }

    #[tokio::test]
    async fn a_tick_passes_over_the_document_the_host_holds_though_it_has_not_moved() {
        let host = test_host().await;
        let desired = desired_state(|state| state.instances = vec![desired_instance(|_| {})]);
        write_desired_state(&host.config.desired_state_file, &desired);

        let mut sequence = mockall::Sequence::new();
        let mut reconciler = MockReconcileService::new();
        reconciler
            .expect_reconcile()
            .times(1)
            .in_sequence(&mut sequence)
            .withf(|_, trigger| *trigger == Trigger::Change)
            .returning(|_, _| ());
        let wanted = desired.clone();
        reconciler
            .expect_reconcile()
            .times(1)
            .in_sequence(&mut sequence)
            .withf(move |seen, trigger| *seen == wanted && *trigger == Trigger::Tick)
            .returning(|_, _| ());
        let controller = controller(&host, reconciler);

        assert!(controller.converge_once().await);
        assert!(
            !controller.converge_once().await,
            "the watch has nothing to set going"
        );
        assert!(controller.converge_on_tick().await);

        let page = page(&host).await;
        assert!(
            page.contains("nibrunner_reconcile_seconds_count{trigger=\"tick\"} 1\n"),
            "{page}"
        );
        assert!(page.contains("nibrunner_reconcile_seconds_count{trigger=\"change\"} 1\n"));
    }

    #[tokio::test]
    async fn the_loop_passes_over_a_document_that_stands_still_once_a_tick() {
        let host = test_host().await;
        write_desired_state(&host.config.desired_state_file, &desired_state(|_| {}));
        let mut reconciler = MockReconcileService::new();
        reconciler
            .expect_reconcile()
            .withf(|_, trigger| *trigger == Trigger::Change)
            .times(1)
            .returning(|_, _| ());
        reconciler
            .expect_reconcile()
            .withf(|_, trigger| *trigger == Trigger::Tick)
            .times(2..)
            .returning(|_, _| ());
        let controller = controller(&host, reconciler);

        let running = controller.clone();
        let converging = tokio::spawn(async move { running.run().await });
        // The first pass writes the document it took up to the store, which waits on a thread; a
        // clock that leaps to the next timer whenever nothing is runnable reads that as the pool
        // timing out. So the clock is paused only once that pass is behind the loop, and the
        // ticks, which touch no store, are what run on it.
        for _ in 0..200 {
            if page(&host)
                .await
                .contains("nibrunner_reconcile_seconds_count{trigger=\"change\"} 1\n")
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        tokio::time::pause();
        tokio::time::sleep(RECONCILE_TICK * 2 + Duration::from_millis(100)).await;
        converging.abort();
        let _ = converging.await;

        let page = page(&host).await;
        assert!(
            page.contains("nibrunner_reconcile_seconds_count{trigger=\"tick\"} 2\n"),
            "{page}"
        );
    }

    #[tokio::test]
    async fn a_tick_on_a_host_that_was_never_given_a_document_has_nothing_to_pass_over() {
        let host = test_host().await;
        let mut reconciler = MockReconcileService::new();
        reconciler.expect_reconcile().never();
        assert!(!controller(&host, reconciler).converge_on_tick().await);
    }

    #[tokio::test]
    async fn a_tick_that_carries_deferred_work_is_measured_as_deferred_rather_than_as_a_tick() {
        let host = test_host().await;
        write_desired_state(&host.config.desired_state_file, &desired_state(|_| {}));
        let mut reconciler = MockReconcileService::new();
        reconciler.expect_reconcile().times(2).returning(|_, _| ());
        let controller = controller(&host, reconciler);
        assert!(controller.converge_once().await);

        host.state.modify(|snapshot| snapshot.deferred_work = true).await;
        assert!(controller.converge_on_tick().await);

        let page = page(&host).await;
        assert!(page.contains("nibrunner_reconcile_seconds_count{trigger=\"deferred\"} 1\n"));
        assert!(page.contains("nibrunner_reconcile_seconds_count{trigger=\"tick\"} 0\n"));
    }

    #[tokio::test]
    async fn a_tick_does_not_reread_the_file_so_a_refused_document_is_refused_once_and_not_once_a_tick() {
        let host = test_host().await;
        let desired = desired_state(|state| state.instances = vec![desired_instance(|_| {})]);
        write_desired_state(&host.config.desired_state_file, &desired);
        let mut reconciler = MockReconcileService::new();
        reconciler.expect_reconcile().times(2).returning(|_, _| ());
        let controller = controller(&host, reconciler);
        assert!(controller.converge_once().await);

        std::fs::write(&host.config.desired_state_file, b"not json at all").unwrap();
        assert!(!controller.converge_once().await);
        assert!(
            controller.converge_on_tick().await,
            "the document it last took up is still the one to keep"
        );

        let page = page(&host).await;
        assert!(
            page.contains("nibrunner_desired_state_unreadable_total 1\n"),
            "{page}"
        );
        assert!(
            host.state.snapshot().await.desired_refusal.is_some(),
            "the refusal stays on the report until a readable document replaces it"
        );
    }

    /// The pass a tick runs is the real one, over a host whose volume backend can have a device
    /// pulled out from under a running guest, the way a ZeroFS restart pulls every one.
    mod over_the_real_reconciler {
        use super::*;
        use crate::adapters::vm::VmStatus;
        use crate::adapters::volumes::{AttachedVolume, MockVolumeBackend, ObservedBacking};
        use crate::ports::VmCall;
        use crate::services::reconcile_service::HostReconciler;
        use std::sync::atomic::{AtomicBool, Ordering};

        fn running_vm() -> VmStatus {
            VmStatus {
                loaded: true,
                active: true,
                failed: false,
                started_this_boot: true,
                exit: None,
            }
        }

        fn attached() -> AttachedVolume {
            AttachedVolume {
                volume_id: volume_id(),
                device_path: "/dev/nbd0".to_string(),
                size_bytes: VOLUME_SIZE_BYTES,
                storage_prefix: protocol::ObjectKey::parse(HOST_STORAGE_PREFIX).unwrap(),
            }
        }

        /// One volume, read as attached until the flag is cleared.
        fn volumes_on_a_device_that_can_die() -> (MockVolumeBackend, Arc<AtomicBool>) {
            let usable = Arc::new(AtomicBool::new(true));
            let mut volumes = MockVolumeBackend::new();
            let observed = usable.clone();
            volumes.expect_observe().returning(move |_| {
                let usable = observed.load(Ordering::SeqCst);
                vec![ObservedBacking {
                    volume_id: volume_id(),
                    size_bytes: VOLUME_SIZE_BYTES,
                    attached: usable,
                    formatted: usable,
                    device_path: Some("/dev/nbd0".to_string()),
                    storage_prefix: protocol::ObjectKey::parse(HOST_STORAGE_PREFIX).unwrap(),
                }]
            });
            volumes.expect_provision().returning(|_| Ok(attached()));
            volumes.expect_attach().returning(|_, _| Ok(attached()));
            volumes.expect_flush().returning(|| Ok(()));
            volumes.expect_observe_checkpoints().returning(Vec::new);
            (volumes, usable)
        }

        async fn host_over(volumes: MockVolumeBackend) -> TestHost {
            let mut host = test_host().await;
            Arc::get_mut(&mut host.host)
                .expect("nothing else holds this host yet")
                .volumes = Arc::new(volumes);
            host
        }

        fn running_app() -> HostDesiredState {
            desired_state(|state| {
                state.volumes = vec![desired_volume(|_| {})];
                state.instances = vec![desired_instance(|_| {})];
            })
        }

        async fn converged(host: &TestHost) -> Arc<ConvergeController> {
            write_desired_state(&host.config.desired_state_file, &running_app());
            let controller =
                ConvergeController::new(host.arc().clone(), HostReconciler::new(host.arc().clone()));
            assert!(controller.converge_once().await);
            assert_eq!(host.vms.calls(), vec![VmCall::Boot]);
            host.vms.set_status(running_vm());
            controller
        }

        #[tokio::test]
        async fn a_tick_sees_the_device_that_died_under_a_running_guest_and_recovers_it() {
            let _serial = ONE_HOST_AT_A_TIME.lock().await;
            let (volumes, usable) = volumes_on_a_device_that_can_die();
            let host = host_over(volumes).await;
            let controller = converged(&host).await;

            usable.store(false, Ordering::SeqCst);
            assert!(
                !controller.converge_once().await,
                "the document stood still, so nothing but the timer would look"
            );
            assert_eq!(host.vms.calls(), vec![VmCall::Boot]);

            assert!(controller.converge_on_tick().await);

            assert_eq!(
                host.vms.calls(),
                vec![VmCall::Boot, VmCall::Stop, VmCall::Discard, VmCall::Boot],
                "the guest on the dead disk is taken down and booted afresh onto the re-attached one"
            );
            let record = host.state.record(&app_id()).await.unwrap();
            assert_eq!(record.state, protocol::InstanceState::Starting);
            assert!(page(&host)
                .await
                .contains("nibrunner_reconcile_seconds_count{trigger=\"tick\"} 1\n"));
        }

        #[tokio::test]
        async fn a_tick_with_nothing_to_do_changes_nothing_says_nothing_and_counts_only_itself() {
            let _serial = ONE_HOST_AT_A_TIME.lock().await;
            let (said, _listening) = Said::listening();

            let (volumes, _usable) = volumes_on_a_device_that_can_die();
            let host = host_over(volumes).await;
            let controller = converged(&host).await;
            assert!(
                said.lines().iter().any(|line| line.contains("instance started")),
                "the pass that booted the app said so, so silence below is silence: {:?}",
                said.lines()
            );

            let before = host.state.snapshot().await;
            let page_before = page(&host).await;
            said.forget();

            assert!(controller.converge_on_tick().await);

            let after = host.state.snapshot().await;
            assert_eq!(after.records, before.records);
            assert_eq!(after.deploys, before.deploys);
            assert_eq!(after.volume_reports, before.volume_reports);
            assert_eq!(after.checkpoint_reports, before.checkpoint_reports);
            assert_eq!(after.export_reports, before.export_reports);
            assert_eq!(after.last_active_at_ms, before.last_active_at_ms);
            assert_eq!(host.vms.calls(), vec![VmCall::Boot]);
            assert_eq!(
                said.lines(),
                Vec::<String>::new(),
                "a quiet tick has nothing to say"
            );
            assert_eq!(
                but_the_pass_itself(&page(&host).await),
                but_the_pass_itself(&page_before),
                "no counter but the pass histogram moved"
            );
        }

        #[tokio::test]
        async fn a_refused_app_stays_refused_tick_after_tick_and_never_runs_out_of_restarts() {
            let _serial = ONE_HOST_AT_A_TIME.lock().await;
            let (said, _listening) = Said::listening();
            let host = test_host().await;
            let port = |name: &str, number: u16| protocol::InstancePort {
                name: protocol::PortName::parse(name).unwrap(),
                guest_port: protocol::GuestPort::new(number).unwrap(),
            };
            // Two ports beside the HTTP one, on a host whose [proxy.raw] allows each guest one.
            let asking_too_much = desired_state(|state| {
                state.volumes = vec![desired_volume(|_| {})];
                state.instances = vec![desired_instance(|instance| {
                    instance.config.ports = vec![port("ssh", 22), port("git", 9418)]
                })];
            });
            let budget = asking_too_much.instances[0].config.restart_policy.max_restarts;
            write_desired_state(&host.config.desired_state_file, &asking_too_much);
            let controller =
                ConvergeController::new(host.arc().clone(), HostReconciler::new(host.arc().clone()));
            assert!(controller.converge_once().await);

            for _ in 0..budget + 3 {
                assert!(controller.converge_on_tick().await);
            }

            let record = host.state.record(&app_id()).await.unwrap();
            assert_eq!(record.state, protocol::InstanceState::Failed);
            assert_eq!(record.start_attempts, crate::domain::backoff::NO_START_ATTEMPTS);
            let reason = record.message.unwrap();
            assert!(
                reason.as_str().contains("allows 1"),
                "the refusal is still what the record says: {}",
                reason.as_str()
            );
            assert!(host.vms.calls().is_empty());
            let refusals = said
                .lines()
                .into_iter()
                .filter(|line| line.contains("refused before it was started"))
                .count();
            assert_eq!(refusals, 1, "said once: {:?}", said.lines());
            assert_eq!(
                host.metrics.health.of(&app_id()).failures,
                [1, 0, 0, 0, 0, 0, 0, 0],
                "refused once, and nothing else"
            );
            assert!(page(&host).await.contains("nibrunner_layers_cached_total 0\n"));
        }

        fn but_the_pass_itself(page: &str) -> Vec<&str> {
            page.lines()
                .filter(|line| !line.starts_with("nibrunner_reconcile_"))
                .collect()
        }
    }
}
