pub mod checkpoints;
pub mod exports;
pub mod idle;
pub mod ingress;
pub mod instances;
pub mod network;
pub mod plan;
pub mod volumes;

pub use plan::*;

use std::collections::BTreeSet;
use std::sync::Arc;

use protocol::{DesiredInstanceState, HostDesiredState, InstanceState};

use crate::adapters::vm::UNKNOWN_VM;
use crate::domain::backoff::NO_START_ATTEMPTS;
use crate::domain::metrics::converge;
use crate::domain::metrics::passes::Trigger;
use crate::domain::metrics::resources::StartRefusal;
use crate::domain::report::capacity::memory_shortfall_for;
use crate::host::Host;

pub async fn observe(host: &Host, desired: &HostDesiredState) -> ObservedState {
    let snapshot = host.state.snapshot().await;
    let mut app_ids: BTreeSet<protocol::AppId> = host.vms.adopted_app_ids().await.into_iter().collect();
    app_ids.extend(snapshot.records.keys().cloned());
    let app_ids: Vec<_> = app_ids.into_iter().collect();
    let statuses = host.vms.statuses(&app_ids).await;

    ObservedState {
        instances: app_ids
            .iter()
            .map(|app_id| {
                let status = statuses.get(app_id).copied().unwrap_or(UNKNOWN_VM);
                let record = snapshot.records.get(app_id);
                ObservedInstance {
                    app_id: app_id.clone(),
                    volume_id: record.map(|record| record.volume_id.clone()),
                    deployment_id: record.map(|record| record.deployment_id.clone()),
                    present: status.loaded || record.is_some(),
                    running: status.active,
                    exited: !status.active
                        && status.started_this_boot
                        && record.is_some_and(|record| record.started_at.is_some() && !record.stop_requested),
                    // A refusal leaves the record failed — or pending, waiting for room — with
                    // nothing spent on it; a boot that failed, or a guest that exited, cost an
                    // attempt or came up first.
                    refused: record.is_some_and(|record| {
                        matches!(record.state, InstanceState::Failed | InstanceState::Pending)
                            && record.started_at.is_none()
                            && record.start_attempts == NO_START_ATTEMPTS
                    }),
                }
            })
            .collect(),
        volumes: volumes::observe_volumes(host, &volumes::volume_owners(desired, &snapshot.records)).await,
        checkpoints: checkpoints::observe_checkpoints(host, desired).await,
        exports: exports::observe_exports(host, desired).await,
    }
}

async fn sync_desired(host: &Host, desired: &HostDesiredState) {
    for wanted in &desired.instances {
        host.state
            .update_record(&wanted.app_id, |record| {
                record.hostnames = wanted.hostnames.clone();
                record.health_check = wanted.config.health_check.clone();
                record.resources = wanted.config.resources;
                record.restart_policy = wanted.config.restart_policy.clone();
                record.desired_running = wanted.desired_state != DesiredInstanceState::Stopped;
                record.on_request = wanted.desired_state == DesiredInstanceState::OnRequest;
                record.http_port = wanted.config.http_port;
            })
            .await;
    }
}

async fn apply_stops(host: &Host, plan: &ReconcilePlan) {
    for action in &plan.instances {
        match action {
            InstancePlan::Stop { app_id, reason } => {
                instances::stop_instance(host, app_id, reason.as_str()).await;
            }
            InstancePlan::Replace { desired } => {
                instances::stop_instance(host, &desired.app_id, InstanceStopReason::Superseded.as_str())
                    .await;
                let _ = host.vms.discard(&desired.app_id).await;
                host.state.drop_record(&desired.app_id).await;
            }
            InstancePlan::Recover { desired } => {
                // Same clean teardown as a replacement — stop, discard, forget the record — so the
                // start below is a fresh boot that re-attaches the volume, rather than a wake onto
                // the snapshot whose disk is already dead. The slot (and its device) is kept.
                instances::stop_instance(host, &desired.app_id, InstanceStopReason::VolumeLost.as_str())
                    .await;
                let _ = host.vms.discard(&desired.app_id).await;
                host.state.drop_record(&desired.app_id).await;
            }
            // The one plan an app the document no longer names reaches, so the only one its
            // output goes with: a replacement or a recovery is the same app, and keeps what it
            // wrote. The discard detaches the log receiver, so nothing is still writing to the
            // files taken below.
            InstancePlan::Forget { app_id } => {
                let _ = host.vms.discard(app_id).await;
                host.state.drop_record(app_id).await;
                host.logs.discard(app_id);
                host.router.discard_access(app_id);
            }
            _ => {}
        }
    }
}

async fn apply_sleeps(host: &Host, plan: &ReconcilePlan) {
    for action in &plan.instances {
        if let InstancePlan::Sleep { desired } = action {
            instances::sleep_instance(host, desired).await;
        }
    }
}

async fn apply_holds(host: &Host, plan: &ReconcilePlan) {
    for action in &plan.instances {
        if let InstancePlan::Hold { desired } = action {
            instances::hold_instance(host, desired).await;
        }
    }
}

async fn apply_starts(host: &Host, plan: &ReconcilePlan) {
    let starts: Vec<_> = plan
        .instances
        .iter()
        .filter_map(|action| match action {
            InstancePlan::Start { desired }
            | InstancePlan::Replace { desired }
            | InstancePlan::Recover { desired } => Some(desired),
            _ => None,
        })
        .collect();
    if starts.is_empty() {
        return;
    }
    if !host.state.snapshot().await.isolated {
        tracing::error!(
            refused = starts.len(),
            "instance starts refused: isolation ruleset not applied"
        );
        host.metrics
            .resources
            .starts_refused(StartRefusal::NotIsolated, starts.len());
        return;
    }

    // Each start is measured against what the host holds, the starts before it in this pass
    // included, so a wave of them boots what fits and no more. The records are read once and
    // the one a start touched read back, rather than the thousand of them once per start.
    let mut records = host.state.records().await;
    let (mut waiting, mut waiting_mib) = (0, 0u64);
    for desired in starts {
        let wanted = &desired.config.resources;
        if let Some(shortfall_mib) =
            memory_shortfall_for(host.guest_memory_mib, &records, &desired.app_id, wanted)
        {
            instances::wait_for_room(host, desired, shortfall_mib).await;
            waiting += 1;
            waiting_mib += u64::from(wanted.memory_mib);
            continue;
        }
        instances::start_instance(host, desired).await;
        records.retain(|record| record.app_id != desired.app_id);
        records.extend(host.state.record(&desired.app_id).await);
    }
    if waiting > 0 {
        tracing::warn!(
            waiting,
            memory_mib = waiting_mib,
            guest_memory_mib = host.guest_memory_mib,
            "instance starts wait for room on this host"
        );
        host.metrics
            .resources
            .starts_refused(StartRefusal::NoRoom, waiting);
    }
}

/// One pass from the document to the host. Every step of it is written so that a pass over a
/// host that is already what the document asks for changes nothing and says nothing, because the
/// timer runs one whether or not anything moved: that is how a volume whose device died under a
/// standing document gets noticed.
pub async fn reconcile(host: &Arc<Host>, desired: &HostDesiredState, trigger: Trigger) {
    let observed = observe(host, desired).await;
    let plan = plan_reconcile(desired, &observed);
    host.state
        .modify(|snapshot| snapshot.deferred_work = plan.has_deferred_work())
        .await;
    sync_desired(host, desired).await;

    let prefetch = instances::prefetch_layers(host, &plan);
    let stops = apply_stops(host, &plan);
    tokio::join!(prefetch, stops);

    volumes::apply_volumes(host, &plan, &observed, desired, trigger).await;
    apply_sleeps(host, &plan).await;
    apply_holds(host, &plan).await;
    network::apply_activators(host).await;
    network::apply_network(host).await;
    apply_starts(host, &plan).await;
    volumes::apply_teardowns(host, &plan).await;
    checkpoints::apply_checkpoints(host, &plan).await;
    exports::apply_exports(host, &plan, trigger).await;
    network::apply_activators(host).await;
    network::apply_network(host).await;
    network::apply_routes(host).await;
    converge::observe(host, crate::clock::now_ms()).await;
    host.persist().await;

    host.state.modify(|snapshot| snapshot.converged = true).await;
    host.state.signal_report();
}

pub async fn refresh(host: &Arc<Host>) {
    instances::refresh_states(host).await;
    converge::observe(host, crate::clock::now_ms()).await;
    network::apply_network(host).await;
    network::apply_routes(host).await;
    host.persist().await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapters::vm::{VmExit, VmStatus};
    use crate::ports::{VmCall, VmError};
    use crate::test_support::*;

    fn running_vm() -> VmStatus {
        VmStatus {
            loaded: true,
            active: true,
            failed: false,
            started_this_boot: true,
            exit: None,
        }
    }

    fn stopped_vm() -> VmStatus {
        VmStatus {
            loaded: true,
            active: false,
            failed: false,
            started_this_boot: true,
            exit: Some(VmExit::Code(0)),
        }
    }

    fn failed_vm() -> VmStatus {
        VmStatus {
            loaded: true,
            active: false,
            failed: true,
            started_this_boot: false,
            exit: None,
        }
    }

    fn running_app() -> protocol::HostDesiredState {
        desired_state(|state| {
            state.volumes = vec![desired_volume(|_| {})];
            state.instances = vec![desired_instance(|instance| {
                instance.hostnames = vec![app_hostname()]
            })];
        })
    }

    fn stopped_app() -> protocol::HostDesiredState {
        desired_state(|state| {
            state.volumes = vec![desired_volume(|_| {})];
            state.instances = vec![desired_instance(|instance| {
                instance.desired_state = DesiredInstanceState::Stopped;
                instance.hostnames = vec![app_hostname()];
            })];
        })
    }

    fn starting(desired: protocol::DesiredInstance) -> ReconcilePlan {
        ReconcilePlan {
            instances: vec![InstancePlan::Start { desired }],
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn an_app_is_woken_by_putting_back_the_microvm_it_had() {
        let host = test_host().await;
        host.volumes.provision(&desired_volume(|_| {})).await.unwrap();
        host.vms.set_status(stopped_vm());
        host.state
            .put_record(instance_record(|record| {
                record.on_request = true;
                record.state = InstanceState::Idle;
            }))
            .await;

        let on_request =
            desired_instance(|instance| instance.desired_state = DesiredInstanceState::OnRequest);
        let outcome = instances::resume_instance(&host, &on_request).await.unwrap();
        assert_eq!(outcome, crate::ports::WakeOutcome::Restored);
        assert_eq!(host.vms.calls(), vec![VmCall::Wake]);
        assert!(host.state.record(&app_id()).await.unwrap().started_at.is_some());
    }

    #[tokio::test]
    async fn a_snapshot_nothing_can_load_is_a_cold_boot_instead() {
        let host = test_host().await;
        host.volumes.provision(&desired_volume(|_| {})).await.unwrap();
        host.vms.set_status(stopped_vm());
        host.vms.refuse_wake(VmError::SnapshotUnusable {
            reason: "the host has rebooted".into(),
        });
        host.state.modify(|snapshot| snapshot.isolated = true).await;
        host.state
            .put_record(instance_record(|record| {
                record.on_request = true;
                record.state = InstanceState::Idle;
            }))
            .await;

        let on_request =
            desired_instance(|instance| instance.desired_state = DesiredInstanceState::OnRequest);
        let outcome = instances::resume_instance(&host, &on_request).await.unwrap();
        assert_eq!(outcome, crate::ports::WakeOutcome::ColdBoot);
        assert_eq!(host.vms.calls(), vec![VmCall::Wake, VmCall::Boot]);
    }

    #[tokio::test]
    async fn a_restore_is_not_a_restart_so_it_costs_the_app_nothing() {
        let host = test_host().await;
        host.volumes.provision(&desired_volume(|_| {})).await.unwrap();
        host.vms.set_status(stopped_vm());
        host.state
            .put_record(instance_record(|record| {
                record.on_request = true;
                record.state = InstanceState::Idle;
                record.restart_count = 4;
                record.last_restart = Some(reported_restart(|_| {}));
            }))
            .await;

        let on_request =
            desired_instance(|instance| instance.desired_state = DesiredInstanceState::OnRequest);
        let outcome = instances::resume_instance(&host, &on_request).await.unwrap();
        assert_eq!(outcome, crate::ports::WakeOutcome::Restored);
        let record = host.state.record(&app_id()).await.unwrap();
        assert_eq!(record.restart_count, 4, "the guest that counted them is back");
        assert_eq!(record.last_restart, Some(reported_restart(|_| {})));
        assert_eq!(record.start_attempts.attempts, 0);
    }

    #[tokio::test]
    async fn a_microvm_that_is_already_up_is_left_alone_rather_than_restored_onto() {
        let host = test_host().await;
        host.vms.set_status(running_vm());
        host.state
            .put_record(instance_record(|record| record.on_request = true))
            .await;
        let on_request =
            desired_instance(|instance| instance.desired_state = DesiredInstanceState::OnRequest);
        let outcome = instances::resume_instance(&host, &on_request).await.unwrap();
        assert_eq!(outcome, crate::ports::WakeOutcome::AlreadyRunning);
        assert!(host.vms.calls().is_empty());
    }

    #[tokio::test]
    async fn an_app_that_has_gone_quiet_is_put_down_where_it_can_be_picked_up() {
        let host = test_host().await;
        host.slot_for(&app_id()).await.unwrap();
        host.state
            .put_record(instance_record(|record| record.on_request = true))
            .await;

        instances::suspend_instance(&host, &app_id(), crate::domain::activation::SleepReason::Quiet).await;

        assert_eq!(host.vms.calls(), vec![VmCall::Sleep]);
        let record = host.state.record(&app_id()).await.unwrap();
        assert_eq!(record.state, InstanceState::Idle);
        assert!(record.stop_requested);
        assert!(!host.state.snapshot().await.snapshotting.contains(&app_id()));
        let page = metrics_page(&host).await;
        assert!(page.contains("nibrunner_sleep_outcomes_total{reason=\"idle\",outcome=\"slept\"} 1\n"));
        assert!(page.contains("nibrunner_sleep_phase_seconds_count{phase=\"flush\",reason=\"idle\"} 1\n"));
        assert!(page.contains("nibrunner_sleep_phase_seconds_count{phase=\"total\",reason=\"idle\"} 1\n"));
        assert!(page.contains("nibrunner_app_sleeps_total{app=\"app-1\",outcome=\"slept\"} 1\n"));
        assert!(
            page.contains("nibrunner_app_last_sleep_seconds{app=\"app-1\"} 0."),
            "{page}"
        );
    }

    async fn metrics_page(host: &TestHost) -> String {
        crate::domain::metrics::tests::page(
            &crate::domain::metrics::tests::report(),
            &host.metrics,
            &host.state.snapshot().await,
            0,
        )
    }

    /// Every ruleset this host loaded, in the order it loaded them.
    fn rulesets_loaded(host: &TestHost) -> Vec<String> {
        host.commands
            .calls()
            .into_iter()
            .filter(|request| request.command == ["nft", "-f", "-"])
            .filter_map(|request| request.stdin)
            .collect()
    }

    /// Whether a ruleset carries a host port to any guest at all; this host holds one app.
    fn forwards_the_guest(ruleset: &str) -> bool {
        ruleset.contains("dnat to")
    }

    #[tokio::test]
    async fn the_port_is_the_activators_before_the_guest_is_asked_to_sleep() {
        let mut host = test_host().await;
        let (held, spy) = mocks::vmm_holding_sleeps();
        Arc::get_mut(&mut host.host)
            .expect("nothing else holds this host yet")
            .vms = held.clone();
        host.vms = spy;
        host.slot_for(&app_id()).await.unwrap();
        host.state
            .put_record(instance_record(|record| record.on_request = true))
            .await;
        network::apply_network(&host).await;
        assert!(forwards_the_guest(&rulesets_loaded(&host)[0]));

        let sleeping = tokio::spawn({
            let host = host.arc().clone();
            async move {
                instances::suspend_instance(&host, &app_id(), crate::domain::activation::SleepReason::Quiet)
                    .await
            }
        });
        held.held_up(1).await;

        let loaded = rulesets_loaded(&host);
        assert_eq!(
            loaded.len(),
            2,
            "the ruleset was loaded again before the microVM was asked to sleep"
        );
        assert!(
            !forwards_the_guest(&loaded[1]),
            "the guest is about to be paused and its port still points at it"
        );

        held.let_through(1);
        sleeping.await.unwrap();
        assert_eq!(
            host.state.record(&app_id()).await.unwrap().state,
            InstanceState::Idle
        );
        assert_eq!(
            rulesets_loaded(&host).len(),
            2,
            "an app that slept is Idle, which the ruleset already left out"
        );
    }

    #[tokio::test]
    async fn a_guest_whose_sleep_did_not_happen_gets_its_port_back() {
        for error in [
            VmError::SleepRefused {
                reason: "the disk has no room for its snapshot".into(),
            },
            VmError::Host("the snapshot could not be written".into()),
        ] {
            let host = test_host().await;
            host.slot_for(&app_id()).await.unwrap();
            host.vms.refuse_sleep(error.clone());
            host.state
                .put_record(instance_record(|record| record.on_request = true))
                .await;
            network::apply_network(&host).await;

            instances::suspend_instance(&host, &app_id(), crate::domain::activation::SleepReason::Quiet)
                .await;

            let loaded = rulesets_loaded(&host);
            assert_eq!(
                loaded.len(),
                3,
                "taken for the sleep and given back after {error:?}"
            );
            assert!(!forwards_the_guest(&loaded[1]));
            assert!(
                forwards_the_guest(&loaded[2]),
                "a guest left up after {error:?} is unreachable without its port"
            );
            assert!(!host.state.snapshot().await.snapshotting.contains(&app_id()));
        }
    }

    #[tokio::test]
    async fn one_that_may_not_be_snapshotted_is_left_up_rather_than_called_broken() {
        let host = test_host().await;
        host.slot_for(&app_id()).await.unwrap();
        host.vms.refuse_sleep(VmError::SleepRefused {
            reason: "it has already been asked to stop".into(),
        });
        host.state
            .put_record(instance_record(|record| record.on_request = true))
            .await;

        instances::suspend_instance(&host, &app_id(), crate::domain::activation::SleepReason::Quiet).await;

        assert_eq!(
            host.state.record(&app_id()).await.unwrap().state,
            InstanceState::Running
        );
        assert!(!host.state.snapshot().await.snapshotting.contains(&app_id()));
        let page = metrics_page(&host).await;
        assert!(page.contains("nibrunner_sleep_outcomes_total{reason=\"idle\",outcome=\"refused\"} 1\n"));
        assert!(
            page.contains("nibrunner_sleep_phase_seconds_count{phase=\"total\",reason=\"idle\"} 0\n"),
            "a sleep that did not happen took no time"
        );
    }

    #[tokio::test]
    async fn one_with_no_slot_to_come_back_to_is_stopped() {
        let host = test_host().await;
        host.state
            .put_record(instance_record(|record| record.on_request = true))
            .await;
        instances::suspend_instance(&host, &app_id(), crate::domain::activation::SleepReason::Quiet).await;
        assert_eq!(host.vms.calls(), vec![VmCall::Stop]);
    }

    #[tokio::test]
    async fn a_pass_that_lands_mid_capture_reads_the_microvm_as_asleep_rather_than_crashed() {
        let host = test_host().await;
        host.vms.set_status(stopped_vm());
        host.state
            .put_record(instance_record(|record| {
                record.on_request = true;
                record.started_at = Some(observed_at());
            }))
            .await;
        host.state.mark_snapshotting(&app_id(), true).await;

        instances::refresh_states(host.arc()).await;

        let record = host.state.record(&app_id()).await.unwrap();
        assert_eq!(record.state, InstanceState::Idle);
        assert!(record.message.is_none());
    }

    #[tokio::test]
    async fn and_fails_it_once_the_snapshot_is_no_longer_in_flight() {
        let host = test_host().await;
        host.vms.set_status(stopped_vm());
        host.vms
            .set_verdict("the tenant used its 5 restarts without staying up; shutting the guest down");
        host.state
            .put_record(instance_record(|record| {
                record.on_request = true;
                record.started_at = Some(observed_at());
            }))
            .await;

        instances::refresh_states(host.arc()).await;

        let record = host.state.record(&app_id()).await.unwrap();
        assert_eq!(record.state, InstanceState::Failed);
        assert!(record.message.unwrap().as_str().contains("used its 5 restarts"));
    }

    #[tokio::test]
    async fn one_pass_converges_a_host_onto_a_document_it_has_never_seen() {
        let _serial = ONE_HOST_AT_A_TIME.lock().await;
        let host = test_host().await;

        reconcile(host.arc(), &running_app(), Trigger::Change).await;

        assert!(host.slot_of(&app_id()).await.is_some());
        assert_eq!(host.vms.calls(), vec![VmCall::Boot]);
        let record = host.state.record(&app_id()).await.unwrap();
        assert_eq!(record.state, InstanceState::Starting);
        assert_eq!(record.deployment_id, deployment_id());
        assert!(host.state.snapshot().await.isolated);
        assert_eq!(
            host.router
                .routes()
                .await
                .port_for(app_hostname().hostname.as_str()),
            Some(record.host_port)
        );
        assert_eq!(host.repositories.instances.all().await.unwrap().len(), 1);
        assert_eq!(host.repositories.slots.all().await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn the_port_an_app_is_reached_on_answers_from_the_pass_that_allocated_it() {
        let _serial = ONE_HOST_AT_A_TIME.lock().await;
        let host = test_host().await;
        reconcile(host.arc(), &running_app(), Trigger::Change).await;
        assert_eq!(host.activator.listening_for().await, vec![app_id()]);
    }

    #[tokio::test]
    async fn an_instance_desired_state_stops_naming_is_stopped_then_forgotten() {
        let _serial = ONE_HOST_AT_A_TIME.lock().await;
        let host = test_host().await;
        reconcile(host.arc(), &running_app(), Trigger::Change).await;
        host.vms.set_status(running_vm());

        reconcile(host.arc(), &desired_state(|_| {}), Trigger::Change).await;
        assert!(host.vms.calls().contains(&VmCall::Stop));
        assert_eq!(
            host.state.record(&app_id()).await.unwrap().state,
            InstanceState::Stopped
        );

        host.vms.set_status(stopped_vm());
        reconcile(host.arc(), &desired_state(|_| {}), Trigger::Change).await;
        assert!(host.state.record(&app_id()).await.is_none());
        assert!(host.vms.calls().contains(&VmCall::Discard));
    }

    #[tokio::test]
    async fn and_its_volume_is_let_go_of_with_the_slot_once_the_guest_is_down_but_its_data_kept() {
        let _serial = ONE_HOST_AT_A_TIME.lock().await;
        let host = test_host().await;
        reconcile(host.arc(), &running_app(), Trigger::Change).await;
        host.vms.set_status(running_vm());

        reconcile(host.arc(), &desired_state(|_| {}), Trigger::Change).await;
        assert!(
            host.slot_of(&app_id()).await.is_some(),
            "the guest is still up on it"
        );

        host.vms.set_status(stopped_vm());
        reconcile(host.arc(), &desired_state(|_| {}), Trigger::Change).await;
        assert!(host.slot_of(&app_id()).await.is_none());
        assert_eq!(host.vms.removed_taps(), vec!["nbr0".to_string()]);
        assert!(host.repositories.slots.all().await.unwrap().is_empty());
        let snapshot = host.state.snapshot().await;
        assert_eq!(snapshot.volume_reports[0].state, protocol::VolumeState::Detached);
        assert!(snapshot.deleted_volumes.is_empty());
        assert_eq!(
            host.volumes.observe(&Default::default()).await.len(),
            1,
            "a vanished entry is a detach, never a delete"
        );

        reconcile(host.arc(), &desired_state(|_| {}), Trigger::Tick).await;
        assert!(host.state.snapshot().await.volume_reports.is_empty());
    }

    #[tokio::test]
    async fn an_app_added_to_the_document_already_stopped_is_answered_for_without_being_booted() {
        // What was measured: the volume was provisioned and reported ready, the instance was never
        // reported at all, and its hostname answered that it was not here rather than that it was
        // down. Not a running app that was told to stop: one that arrived stopped.
        let _serial = ONE_HOST_AT_A_TIME.lock().await;
        let host = test_host().await;

        reconcile(host.arc(), &stopped_app(), Trigger::Change).await;

        assert!(host.vms.calls().is_empty(), "{:?}", host.vms.calls());
        let record = host.state.record(&app_id()).await.unwrap();
        assert_eq!(record.state, InstanceState::Stopped);
        assert!(!record.desired_running);
        assert_eq!(record.hostnames, vec![app_hostname()]);
        let slot = host.slot_of(&app_id()).await.expect("held on a slot of its own");
        assert_eq!(record.host_port, slot.host_port);
        assert_eq!(
            host.router
                .routes()
                .await
                .port_for(app_hostname().hostname.as_str()),
            Some(slot.host_port)
        );
        assert_eq!(host.activator.listening_for().await, vec![app_id()]);
        assert_eq!(
            host.state.snapshot().await.volume_reports[0].state,
            protocol::VolumeState::Ready
        );
        assert_eq!(host.repositories.instances.all().await.unwrap().len(), 1);

        // The timer and the status loop find it as it was left.
        reconcile(host.arc(), &stopped_app(), Trigger::Tick).await;
        refresh(host.arc()).await;
        assert!(host.vms.calls().is_empty(), "{:?}", host.vms.calls());
        let held = host.state.record(&app_id()).await.unwrap();
        assert_eq!(held.state, InstanceState::Stopped);
        assert_eq!(held.host_port, slot.host_port);
        assert!(host.state.snapshot().await.converged);

        // And when the document wants it up, it comes up on the port it was answering on.
        reconcile(host.arc(), &running_app(), Trigger::Change).await;
        assert_eq!(host.vms.calls(), vec![VmCall::Boot]);
        let started = host.state.record(&app_id()).await.unwrap();
        assert_eq!(started.state, InstanceState::Starting);
        assert!(started.desired_running);
        assert_eq!(started.host_port, slot.host_port);
    }

    #[tokio::test]
    async fn an_app_stopped_after_running_keeps_the_record_its_stop_left() {
        let _serial = ONE_HOST_AT_A_TIME.lock().await;
        let host = test_host().await;
        reconcile(host.arc(), &running_app(), Trigger::Change).await;
        host.vms.set_status(running_vm());

        reconcile(host.arc(), &stopped_app(), Trigger::Change).await;
        assert_eq!(host.vms.calls(), vec![VmCall::Boot, VmCall::Stop]);
        let stopped = host.state.record(&app_id()).await.unwrap();
        assert_eq!(stopped.state, InstanceState::Stopped);
        assert!(stopped.started_at.is_some(), "it ran");

        host.vms.set_status(stopped_vm());
        reconcile(host.arc(), &stopped_app(), Trigger::Tick).await;

        assert_eq!(host.vms.calls(), vec![VmCall::Boot, VmCall::Stop]);
        assert_eq!(host.state.record(&app_id()).await.unwrap(), stopped);
    }

    #[tokio::test]
    async fn an_instance_whose_volume_is_not_here_does_not_boot() {
        let _serial = ONE_HOST_AT_A_TIME.lock().await;
        let host = test_host().await;
        reconcile(
            host.arc(),
            &desired_state(|state| state.instances = vec![desired_instance(|_| {})]),
            Trigger::Change,
        )
        .await;
        assert!(host.vms.calls().is_empty());
        let record = host.state.record(&app_id()).await.unwrap();
        assert_eq!(record.state, InstanceState::Failed);
        assert!(record.message.unwrap().as_str().contains("does not serve"));
    }

    #[tokio::test]
    async fn a_seed_the_document_names_wrongly_keeps_the_app_down_until_the_document_is_put_right() {
        let _serial = ONE_HOST_AT_A_TIME.lock().await;
        let archive = crate::adapters::volumes::initial_contents::tests::archive();
        let host = test_host_seeding(archive.clone()).await;
        let seeded_with = |contents: protocol::InitialContents| {
            desired_state(|state| {
                state.volumes = vec![desired_volume(|volume| volume.initial_contents = Some(contents))];
                state.instances = vec![desired_instance(|instance| {
                    instance.hostnames = vec![app_hostname()]
                })];
            })
        };
        // The store holds the archive; the document names a digest of something else.
        let wrong = seeded_with(initial_contents(b"not what the store holds", "/app/data"));

        reconcile(host.arc(), &wrong, Trigger::Change).await;
        reconcile(host.arc(), &wrong, Trigger::Change).await;

        assert!(
            host.vms.calls().is_empty(),
            "nothing was booted onto a bare volume"
        );
        let snapshot = host.state.snapshot().await;
        let volume = &snapshot.volume_reports[0];
        assert_eq!(volume.state, protocol::VolumeState::Failed);
        let reason = volume.message.clone().expect("the refusal");
        assert!(reason.as_str().contains("hashes to"), "{}", reason.as_str());
        let record = &snapshot.records[&app_id()];
        assert_eq!(record.state, InstanceState::Failed);
        assert_eq!(
            record.message.as_ref(),
            Some(&reason),
            "the app carries the volume's reason"
        );
        assert_eq!(record.start_attempts, crate::domain::backoff::NO_START_ATTEMPTS);
        assert!(
            !host.commands.executables().contains(&"mke2fs".to_string()),
            "a seed that was refused formats nothing"
        );
        let provisions_failed = |page: &str| {
            page.lines()
                .find(|line| {
                    line.starts_with(
                        "nibrunner_storage_operation_seconds_count{operation=\"volume_provision\",outcome=\"failed\"}",
                    )
                })
                .map(str::to_string)
        };
        let after_two_changes = provisions_failed(&metrics_page(&host).await);
        assert!(
            after_two_changes.as_deref().unwrap_or("").ends_with(" 2"),
            "{after_two_changes:?}"
        );

        // The timer does not fetch the seed again against a document that still names the wrong
        // thing; the volume and the app stay failed for the reason they were.
        reconcile(host.arc(), &wrong, Trigger::Tick).await;

        assert_eq!(provisions_failed(&metrics_page(&host).await), after_two_changes);
        assert!(host.vms.calls().is_empty());
        let snapshot = host.state.snapshot().await;
        assert_eq!(snapshot.volume_reports[0].state, protocol::VolumeState::Failed);
        assert_eq!(snapshot.volume_reports[0].message, Some(reason.clone()));
        assert_eq!(snapshot.records[&app_id()].state, InstanceState::Failed);
        assert_eq!(snapshot.records[&app_id()].message, Some(reason));

        reconcile(
            host.arc(),
            &seeded_with(initial_contents(&archive, "/app/data")),
            Trigger::Change,
        )
        .await;

        assert_eq!(host.vms.calls(), vec![VmCall::Boot]);
        let snapshot = host.state.snapshot().await;
        assert_eq!(snapshot.volume_reports[0].state, protocol::VolumeState::Ready);
        assert_eq!(snapshot.volume_reports[0].message, None);
        let record = &snapshot.records[&app_id()];
        assert_eq!(record.state, InstanceState::Starting);
        assert_eq!(record.message, None);
        let formats: Vec<_> = host
            .commands
            .commands()
            .into_iter()
            .filter(|command| command[0] == "mke2fs")
            .collect();
        assert_eq!(formats.len(), 1);
        assert!(
            formats[0].contains(&"-d".to_string()),
            "the volume was formatted holding the seed: {formats:?}"
        );
    }

    #[tokio::test]
    async fn an_on_request_app_whose_seed_is_put_right_is_started_without_waiting_to_be_asked_for() {
        let _serial = ONE_HOST_AT_A_TIME.lock().await;
        let archive = crate::adapters::volumes::initial_contents::tests::archive();
        let host = test_host_seeding(archive.clone()).await;
        let seeded_with = |contents: protocol::InitialContents| {
            desired_state(|state| {
                state.volumes = vec![desired_volume(|volume| volume.initial_contents = Some(contents))];
                state.instances = vec![desired_instance(|instance| {
                    instance.desired_state = DesiredInstanceState::OnRequest;
                    instance.hostnames = vec![app_hostname()];
                })];
            })
        };
        let wrong = seeded_with(initial_contents(b"not what the store holds", "/app/data"));

        reconcile(host.arc(), &wrong, Trigger::Change).await;
        let refused = host.state.record(&app_id()).await.unwrap();
        assert_eq!(refused.state, InstanceState::Failed);
        let reason = refused.message.clone().expect("the volume's reason");
        assert!(reason.as_str().contains("hashes to"), "{}", reason.as_str());
        let provisions_failed = |page: &str| {
            page.lines()
                .find(|line| {
                    line.starts_with(
                        "nibrunner_storage_operation_seconds_count{operation=\"volume_provision\",outcome=\"failed\"}",
                    )
                })
                .map(str::to_string)
        };
        let once = provisions_failed(&metrics_page(&host).await);
        assert!(once.as_deref().unwrap_or("").ends_with(" 1"), "{once:?}");

        // The ticks neither fetch the seed again nor boot anything, and the app is not read as
        // one asleep and well: it stays failed for the volume's reason.
        reconcile(host.arc(), &wrong, Trigger::Tick).await;
        refresh(host.arc()).await;
        reconcile(host.arc(), &wrong, Trigger::Tick).await;

        assert_eq!(provisions_failed(&metrics_page(&host).await), once);
        assert!(host.vms.calls().is_empty(), "{:?}", host.vms.calls());
        let snapshot = host.state.snapshot().await;
        assert_eq!(snapshot.volume_reports[0].state, protocol::VolumeState::Failed);
        let record = &snapshot.records[&app_id()];
        assert_eq!(record.state, InstanceState::Failed);
        assert_eq!(record.message, Some(reason));
        assert_eq!(record.start_attempts, crate::domain::backoff::NO_START_ATTEMPTS);
        let [_, _, _, volume, _, _, _, _] = host.metrics.health.of(&app_id()).failures;
        assert_eq!(volume, 1, "said once, counted once");

        reconcile(
            host.arc(),
            &seeded_with(initial_contents(&archive, "/app/data")),
            Trigger::Change,
        )
        .await;

        assert_eq!(host.vms.calls(), vec![VmCall::Boot]);
        let snapshot = host.state.snapshot().await;
        assert_eq!(snapshot.volume_reports[0].state, protocol::VolumeState::Ready);
        let record = &snapshot.records[&app_id()];
        assert_eq!(record.state, InstanceState::Starting);
        assert_eq!(record.message, None);
        assert_eq!(
            record.start_attempts.attempts, 1,
            "the refusal cost nothing; this boot is the first attempt"
        );
    }

    #[tokio::test]
    async fn an_on_request_app_that_spent_its_budget_before_coming_up_is_not_started_by_a_pass() {
        let _serial = ONE_HOST_AT_A_TIME.lock().await;
        let host = test_host().await;
        host.volumes.provision(&desired_volume(|_| {})).await.unwrap();
        let said = protocol::StateMessage::new("out of restarts: 6 starts attempted against a budget of 5");
        host.state
            .put_record(instance_record(|record| {
                record.on_request = true;
                record.state = InstanceState::Failed;
                record.started_at = None;
                record.start_attempts = crate::domain::backoff::AttemptWindow {
                    attempts: protocol::DEFAULT_RESTART_POLICY.max_restarts + 1,
                    last_attempt_at_ms: Some(crate::clock::now_ms()),
                };
                record.message = Some(said.clone());
            }))
            .await;
        let on_request = desired_state(|state| {
            state.volumes = vec![desired_volume(|_| {})];
            state.instances = vec![desired_instance(|instance| {
                instance.desired_state = DesiredInstanceState::OnRequest
            })];
        });

        reconcile(host.arc(), &on_request, Trigger::Tick).await;

        assert!(host.vms.calls().is_empty(), "{:?}", host.vms.calls());
        let record = host.state.record(&app_id()).await.unwrap();
        assert_eq!(record.state, InstanceState::Failed);
        assert_eq!(record.message, Some(said));
        assert_eq!(
            record.start_attempts.attempts,
            protocol::DEFAULT_RESTART_POLICY.max_restarts + 1
        );
    }

    #[tokio::test]
    async fn a_deploy_replaces_the_release_rather_than_restarting_it() {
        let _serial = ONE_HOST_AT_A_TIME.lock().await;
        let host = test_host().await;
        reconcile(host.arc(), &running_app(), Trigger::Change).await;
        host.vms.set_status(running_vm());

        let newer = desired_state(|state| {
            state.volumes = vec![desired_volume(|_| {})];
            state.instances = vec![desired_instance(|instance| {
                instance.deployment_id = protocol::DeploymentId::parse("dep-2").unwrap();
            })]
        });
        reconcile(host.arc(), &newer, Trigger::Change).await;

        assert_eq!(
            host.state.record(&app_id()).await.unwrap().deployment_id.as_str(),
            "dep-2"
        );
        let calls = host.vms.calls();
        let stopped = calls.iter().position(|call| *call == VmCall::Stop).unwrap();
        let booted = calls.iter().rposition(|call| *call == VmCall::Boot).unwrap();
        assert!(stopped < booted);
    }

    #[tokio::test]
    async fn a_microvm_this_host_adopted_without_a_record_is_still_observed() {
        let host = test_host().await;
        host.vms.set_adopted(vec![app_id()]);
        host.vms.set_status(running_vm());

        let observed = observe(&host, &desired_state(|_| {})).await;

        assert_eq!(observed.instances.len(), 1);
        assert_eq!(observed.instances[0].app_id, app_id());
        assert_eq!(observed.instances[0].volume_id, None);
        assert_eq!(observed.instances[0].deployment_id, None);
        assert!(observed.instances[0].present);
        assert!(observed.instances[0].running);
        assert!(!observed.instances[0].exited);
    }

    #[tokio::test]
    async fn a_record_with_nothing_loaded_is_still_present_so_the_plan_can_act_on_it() {
        let host = test_host().await;
        host.state.put_record(instance_record(|_| {})).await;

        let observed = observe(&host, &desired_state(|_| {})).await;

        assert_eq!(observed.instances.len(), 1);
        assert!(observed.instances[0].present);
        assert!(!observed.instances[0].running);
        assert_eq!(observed.instances[0].volume_id, Some(volume_id()));
        assert_eq!(observed.instances[0].deployment_id, Some(deployment_id()));
    }

    #[tokio::test]
    async fn a_guest_that_was_asked_to_stop_is_not_read_as_one_that_exited_on_its_own() {
        let host = test_host().await;
        host.vms.set_status(stopped_vm());
        host.state
            .put_record(instance_record(|record| {
                record.started_at = Some(observed_at());
                record.stop_requested = true;
            }))
            .await;
        assert!(!observe(&host, &desired_state(|_| {})).await.instances[0].exited);

        host.state
            .update_record(&app_id(), |record| record.stop_requested = false)
            .await;
        assert!(observe(&host, &desired_state(|_| {})).await.instances[0].exited);
    }

    #[tokio::test]
    async fn one_that_was_never_started_has_not_exited_either() {
        let host = test_host().await;
        host.vms.set_status(stopped_vm());
        host.state
            .put_record(instance_record(|record| record.started_at = None))
            .await;
        assert!(!observe(&host, &desired_state(|_| {})).await.instances[0].exited);
    }

    #[tokio::test]
    async fn a_record_failed_with_no_attempt_spent_on_it_reads_as_refused() {
        let host = test_host().await;
        host.state
            .put_record(instance_record(|record| {
                record.state = InstanceState::Failed;
                record.started_at = None;
                record.message = Some(protocol::StateMessage::new(
                    "the initial contents could not be laid out",
                ));
            }))
            .await;
        assert!(observe(&host, &desired_state(|_| {})).await.instances[0].refused);
    }

    #[tokio::test]
    async fn one_that_spent_its_budget_or_came_up_once_does_not() {
        let host = test_host().await;
        let spent = crate::domain::backoff::AttemptWindow {
            attempts: protocol::DEFAULT_RESTART_POLICY.max_restarts + 1,
            last_attempt_at_ms: Some(crate::clock::now_ms()),
        };
        host.state
            .put_record(instance_record(|record| {
                record.state = InstanceState::Failed;
                record.started_at = None;
                record.start_attempts = spent;
            }))
            .await;
        assert!(
            !observe(&host, &desired_state(|_| {})).await.instances[0].refused,
            "every boot failed on the way up, and each cost an attempt"
        );

        host.state
            .update_record(&app_id(), |record| {
                record.started_at = Some(observed_at());
                record.start_attempts = crate::domain::backoff::NO_START_ATTEMPTS;
            })
            .await;
        assert!(
            !observe(&host, &desired_state(|_| {})).await.instances[0].refused,
            "a guest that came up and earned its window back is one that exited, not one refused"
        );
    }

    #[tokio::test]
    async fn the_document_is_what_an_existing_record_says_it_wants() {
        let host = test_host().await;
        host.state
            .put_record(instance_record(|record| {
                record.hostnames = vec![];
                record.on_request = false;
            }))
            .await;
        let desired = desired_state(|state| {
            state.instances = vec![desired_instance(|instance| {
                instance.desired_state = DesiredInstanceState::OnRequest;
                instance.hostnames = vec![app_hostname()];
            })]
        });

        sync_desired(&host, &desired).await;

        let record = host.state.record(&app_id()).await.unwrap();
        assert!(record.on_request);
        assert!(record.desired_running);
        assert_eq!(record.hostnames, vec![app_hostname()]);
    }

    #[tokio::test]
    async fn a_document_that_stops_an_app_says_so_on_the_record_without_stopping_anything() {
        let host = test_host().await;
        host.state.put_record(instance_record(|_| {})).await;
        let desired = desired_state(|state| {
            state.instances = vec![desired_instance(|instance| {
                instance.desired_state = DesiredInstanceState::Stopped
            })]
        });

        sync_desired(&host, &desired).await;

        let record = host.state.record(&app_id()).await.unwrap();
        assert!(!record.desired_running);
        assert!(!record.on_request);
        assert!(host.vms.calls().is_empty());
    }

    #[tokio::test]
    async fn a_document_naming_an_app_this_host_has_no_record_for_writes_nothing_down() {
        let host = test_host().await;
        sync_desired(&host, &running_app()).await;
        assert!(host.state.record(&app_id()).await.is_none());
    }

    #[tokio::test]
    async fn a_host_whose_isolation_ruleset_did_not_apply_starts_nothing() {
        let host = test_host().await;
        host.volumes.provision(&desired_volume(|_| {})).await.unwrap();

        apply_starts(&host, &starting(desired_instance(|_| {}))).await;

        assert!(host.vms.calls().is_empty());
        assert!(host.state.record(&app_id()).await.is_none());
    }

    #[tokio::test]
    async fn one_that_did_apply_it_starts_what_the_plan_names() {
        let host = test_host().await;
        host.volumes.provision(&desired_volume(|_| {})).await.unwrap();
        host.state.modify(|snapshot| snapshot.isolated = true).await;

        apply_starts(&host, &starting(desired_instance(|_| {}))).await;

        assert_eq!(host.vms.calls(), vec![VmCall::Boot]);
    }

    #[tokio::test]
    async fn a_plan_with_nothing_to_start_asks_nothing_of_the_ruleset() {
        let host = test_host().await;
        apply_starts(&host, &ReconcilePlan::default()).await;
        assert!(host.vms.calls().is_empty());
    }

    fn nth_app(n: usize) -> protocol::AppId {
        protocol::AppId::parse(format!("app-{n}")).unwrap()
    }

    /// A document naming `count` on-request apps this host has never held, each on a volume of
    /// its own, in the order the document names them.
    fn on_request_apps(count: usize) -> protocol::HostDesiredState {
        desired_state(|state| {
            for n in 1..=count {
                let volume_id = protocol::VolumeId::parse(format!("vol-{n}")).unwrap();
                state.volumes.push(desired_volume(|volume| {
                    volume.volume_id = volume_id.clone();
                    volume.app_id = nth_app(n);
                }));
                state.instances.push(desired_instance(|instance| {
                    instance.app_id = nth_app(n);
                    instance.volume_id = volume_id;
                    instance.desired_state = DesiredInstanceState::OnRequest;
                }));
            }
        })
    }

    /// How many apps of the default size the test host has memory for: four.
    fn apps_that_fit(host: &TestHost) -> usize {
        crate::domain::report::capacity::apps_up_at_once(
            host.guest_memory_mib,
            protocol::DEFAULT_INSTANCE_RESOURCES.memory_mib,
        ) as usize
    }

    fn boots(host: &TestHost) -> usize {
        host.vms
            .calls()
            .into_iter()
            .filter(|call| *call == VmCall::Boot)
            .count()
    }

    fn start_refusals_for_room(page: &str) -> String {
        page.lines()
            .find(|line| line.starts_with("nibrunner_start_refusals_total{reason=\"no_room\"}"))
            .map(str::to_string)
            .unwrap_or_default()
    }

    const WAITING_FOR_ONE_MORE: &str =
        "waiting to be started: its host is 256 MiB short of the memory it needs";

    #[tokio::test]
    async fn a_wave_of_starts_boots_what_the_memory_fits_in_document_order_and_leaves_the_rest_waiting() {
        let _serial = ONE_HOST_AT_A_TIME.lock().await;
        let (said, _listening) = Said::listening();
        let host = test_host().await;
        let fits = apps_that_fit(&host);
        let wave = on_request_apps(fits + 2);

        reconcile(host.arc(), &wave, Trigger::Change).await;

        assert_eq!(boots(&host), fits);
        for n in 1..=fits {
            let record = host.state.record(&nth_app(n)).await.unwrap();
            assert_eq!(record.state, InstanceState::Starting, "app-{n}");
        }
        for n in fits + 1..=fits + 2 {
            let record = host.state.record(&nth_app(n)).await.unwrap();
            assert_eq!(record.state, InstanceState::Pending, "app-{n}");
            assert_eq!(record.message.as_ref().unwrap().as_str(), WAITING_FOR_ONE_MORE);
            assert_eq!(
                record.start_attempts, NO_START_ATTEMPTS,
                "nothing was spent waiting"
            );
            assert!(record.started_at.is_none());
            assert_eq!(
                host.metrics.health.of(&nth_app(n)).failures,
                [0; 8],
                "waiting is not failing"
            );
            assert!(
                host.slot_of(&nth_app(n)).await.is_some(),
                "the record answers for its hostnames"
            );
        }
        assert!(
            start_refusals_for_room(&metrics_page(&host).await).ends_with(" 2"),
            "{}",
            metrics_page(&host).await
        );
        let warned: Vec<_> = said
            .lines()
            .into_iter()
            .filter(|line| line.contains("wait for room"))
            .collect();
        assert_eq!(
            warned.len(),
            1,
            "said once for the pass, not once per app: {warned:?}"
        );
        assert!(warned[0].starts_with("WARN "), "{}", warned[0]);
    }

    #[tokio::test]
    async fn and_the_next_pass_starts_one_more_for_each_app_that_went_to_sleep() {
        let _serial = ONE_HOST_AT_A_TIME.lock().await;
        let host = test_host().await;
        let fits = apps_that_fit(&host);
        let wave = on_request_apps(fits + 2);
        reconcile(host.arc(), &wave, Trigger::Change).await;
        assert_eq!(boots(&host), fits);

        // A tick over the same document boots nothing more while nothing has made room.
        reconcile(host.arc(), &wave, Trigger::Tick).await;
        assert_eq!(boots(&host), fits);

        // The first app's idle timeout put it to sleep, as the idle pass would.
        host.state
            .update_record(&nth_app(1), |record| {
                record.state = InstanceState::Idle;
                record.stop_requested = true;
            })
            .await;
        reconcile(host.arc(), &wave, Trigger::Tick).await;

        assert_eq!(boots(&host), fits + 1);
        let started = host.state.record(&nth_app(fits + 1)).await.unwrap();
        assert_eq!(started.state, InstanceState::Starting);
        assert!(started.message.is_none());
        assert_eq!(
            started.start_attempts.attempts, 1,
            "the boot is the first attempt"
        );
        let waiting = host.state.record(&nth_app(fits + 2)).await.unwrap();
        assert_eq!(waiting.state, InstanceState::Pending);
        assert_eq!(waiting.message.as_ref().unwrap().as_str(), WAITING_FOR_ONE_MORE);
        assert!(
            start_refusals_for_room(&metrics_page(&host).await).ends_with(" 5"),
            "two refused, then two, then one: {}",
            metrics_page(&host).await
        );
    }

    #[tokio::test]
    async fn a_replacement_asking_for_more_than_fits_waits_rather_than_booting_beside_the_others() {
        let _serial = ONE_HOST_AT_A_TIME.lock().await;
        let host = test_host().await;
        let fits = apps_that_fit(&host);
        let full = desired_state(|state| {
            let wave = on_request_apps(fits);
            state.volumes = wave.volumes;
            state.instances = wave
                .instances
                .into_iter()
                .map(|mut instance| {
                    instance.desired_state = DesiredInstanceState::Running;
                    instance
                })
                .collect();
        });
        reconcile(host.arc(), &full, Trigger::Change).await;
        assert_eq!(boots(&host), fits);
        host.vms.set_status(running_vm());

        // A new release of the first app, wanting twice the memory: the old guest goes down for
        // it, and what is left beside the others is one app's worth short of what it asks.
        let mut newer = full.clone();
        newer.instances[0].deployment_id = protocol::DeploymentId::parse("dep-2").unwrap();
        newer.instances[0].config.resources.memory_mib = protocol::DEFAULT_INSTANCE_RESOURCES.memory_mib * 2;
        reconcile(host.arc(), &newer, Trigger::Change).await;

        let calls = host.vms.calls();
        assert_eq!(&calls[fits..], &[VmCall::Stop, VmCall::Discard], "{calls:?}");
        let replaced = host.state.record(&nth_app(1)).await.unwrap();
        assert_eq!(replaced.state, InstanceState::Pending);
        assert_eq!(replaced.deployment_id.as_str(), "dep-2");
        assert_eq!(replaced.message.as_ref().unwrap().as_str(), WAITING_FOR_ONE_MORE);
        assert_eq!(replaced.start_attempts, NO_START_ATTEMPTS);
    }

    #[tokio::test]
    async fn a_record_waiting_for_room_is_left_waiting_by_the_status_loop() {
        let host = test_host().await;
        for (app_id, on_request) in [(nth_app(1), false), (nth_app(2), true)] {
            host.state
                .put_record(instance_record(|record| {
                    record.app_id = app_id;
                    record.on_request = on_request;
                    record.state = InstanceState::Pending;
                    record.started_at = None;
                    record.message = Some(protocol::StateMessage::new(WAITING_FOR_ONE_MORE));
                }))
                .await;
        }

        instances::refresh_states(host.arc()).await;

        for n in 1..=2 {
            let record = host.state.record(&nth_app(n)).await.unwrap();
            assert_eq!(record.state, InstanceState::Pending, "app-{n}");
            assert_eq!(record.message.as_ref().unwrap().as_str(), WAITING_FOR_ONE_MORE);
            assert!(
                observe(&host, &desired_state(|_| {})).await.instances[n - 1].refused,
                "app-{n} is started the pass there is room, on request or not"
            );
        }
    }

    #[tokio::test]
    async fn an_instance_the_plan_forgets_is_discarded_and_its_record_dropped() {
        let host = test_host().await;
        host.state.put_record(instance_record(|_| {})).await;
        let plan = ReconcilePlan {
            instances: vec![InstancePlan::Forget { app_id: app_id() }],
            ..Default::default()
        };

        apply_stops(&host, &plan).await;

        assert_eq!(host.vms.calls(), vec![VmCall::Discard]);
        assert!(host.state.record(&app_id()).await.is_none());
    }

    async fn output_kept_for(host: &TestHost) -> std::path::PathBuf {
        use crate::ports::{LogSink, TenantLogBody, TenantLogEvent};
        host.logs
            .publish(vec![TenantLogEvent {
                app_id: app_id(),
                deployment_id: deployment_id(),
                source_id: "source-1".into(),
                sequence: 0,
                observed_at: observed_at(),
                body: TenantLogBody::Data {
                    stream: protocol::TenantLogStream::Stdout,
                    text: "listening".into(),
                },
            }])
            .await;
        let path = host.logs.path_for(&app_id());
        assert!(path.exists());
        path
    }

    async fn stops_applied(host: &TestHost, action: InstancePlan) {
        apply_stops(
            host,
            &ReconcilePlan {
                instances: vec![action],
                ..Default::default()
            },
        )
        .await;
    }

    #[tokio::test]
    async fn an_app_the_document_no_longer_names_loses_the_output_this_host_kept_for_it() {
        let host = test_host().await;
        host.state.put_record(instance_record(|_| {})).await;
        let kept = output_kept_for(&host).await;

        stops_applied(&host, InstancePlan::Forget { app_id: app_id() }).await;

        assert!(!kept.exists());
    }

    #[tokio::test]
    async fn an_app_the_document_still_names_keeps_its_output_through_a_stop_a_replacement_and_a_recovery() {
        for action in [
            InstancePlan::Stop {
                app_id: app_id(),
                reason: InstanceStopReason::DesiredStopped,
            },
            InstancePlan::Replace {
                desired: desired_instance(|_| {}),
            },
            InstancePlan::Recover {
                desired: desired_instance(|_| {}),
            },
        ] {
            let host = test_host().await;
            host.state.put_record(instance_record(|_| {})).await;
            let kept = output_kept_for(&host).await;

            stops_applied(&host, action.clone()).await;

            assert!(kept.exists(), "{action:?}");
        }
    }

    #[tokio::test]
    async fn an_app_asleep_on_this_host_keeps_the_output_it_wrote_before_it_went_to_sleep() {
        let host = test_host().await;
        let plan = ReconcilePlan {
            instances: vec![InstancePlan::Sleep {
                desired: desired_instance(|_| {}),
            }],
            ..Default::default()
        };
        let kept = output_kept_for(&host).await;

        apply_stops(&host, &plan).await;
        apply_sleeps(&host, &plan).await;

        assert!(kept.exists());
    }

    #[tokio::test]
    async fn a_replacement_takes_the_old_microvm_down_before_the_record_is_dropped() {
        let host = test_host().await;
        host.state.put_record(instance_record(|_| {})).await;
        let plan = ReconcilePlan {
            instances: vec![InstancePlan::Replace {
                desired: desired_instance(|_| {}),
            }],
            ..Default::default()
        };

        apply_stops(&host, &plan).await;

        assert_eq!(host.vms.calls(), vec![VmCall::Stop, VmCall::Discard]);
        assert!(host.state.record(&app_id()).await.is_none());
    }

    #[tokio::test]
    async fn a_recovery_takes_the_guest_with_the_dead_disk_down_and_forgets_its_record() {
        let host = test_host().await;
        host.state.put_record(instance_record(|_| {})).await;
        let plan = ReconcilePlan {
            instances: vec![InstancePlan::Recover {
                desired: desired_instance(|_| {}),
            }],
            ..Default::default()
        };

        apply_stops(&host, &plan).await;

        // The record is dropped so the start below is a cold boot, not a wake onto a dead snapshot.
        assert_eq!(host.vms.calls(), vec![VmCall::Stop, VmCall::Discard]);
        assert!(host.state.record(&app_id()).await.is_none());
    }

    #[tokio::test]
    async fn a_recovery_boots_the_guest_back_onto_its_re_attached_disk() {
        let host = test_host().await;
        host.volumes.provision(&desired_volume(|_| {})).await.unwrap();
        host.state.modify(|snapshot| snapshot.isolated = true).await;
        let plan = ReconcilePlan {
            instances: vec![InstancePlan::Recover {
                desired: desired_instance(|_| {}),
            }],
            ..Default::default()
        };

        apply_starts(&host, &plan).await;

        assert_eq!(host.vms.calls(), vec![VmCall::Boot]);
    }

    #[tokio::test]
    async fn a_plan_that_leaves_an_instance_alone_touches_no_microvm() {
        let host = test_host().await;
        host.state.put_record(instance_record(|_| {})).await;
        let plan = ReconcilePlan {
            instances: vec![
                InstancePlan::None { app_id: app_id() },
                InstancePlan::Sleep {
                    desired: desired_instance(|_| {}),
                },
                InstancePlan::Start {
                    desired: desired_instance(|_| {}),
                },
                InstancePlan::Hold {
                    desired: desired_instance(|_| {}),
                },
            ],
            ..Default::default()
        };

        apply_stops(&host, &plan).await;

        assert!(host.vms.calls().is_empty());
        assert!(host.state.record(&app_id()).await.is_some());
    }

    #[tokio::test]
    async fn a_plan_that_holds_an_app_leaves_a_stopped_record_and_boots_nothing() {
        let host = test_host().await;
        let plan = ReconcilePlan {
            instances: vec![InstancePlan::Hold {
                desired: desired_instance(|instance| instance.desired_state = DesiredInstanceState::Stopped),
            }],
            ..Default::default()
        };

        apply_holds(&host, &plan).await;

        let record = host.state.record(&app_id()).await.unwrap();
        assert_eq!(record.state, InstanceState::Stopped);
        assert!(!record.desired_running);
        assert!(host.slot_of(&app_id()).await.is_some());
        assert!(host.vms.calls().is_empty());
    }

    #[tokio::test]
    async fn a_plan_that_puts_an_app_to_sleep_leaves_a_record_waiting_to_be_asked_for() {
        let host = test_host().await;
        let plan = ReconcilePlan {
            instances: vec![InstancePlan::Sleep {
                desired: desired_instance(|instance| {
                    instance.desired_state = DesiredInstanceState::OnRequest
                }),
            }],
            ..Default::default()
        };

        apply_sleeps(&host, &plan).await;

        let record = host.state.record(&app_id()).await.unwrap();
        assert_eq!(record.state, InstanceState::Idle);
        assert!(record.on_request);
        assert!(host.vms.calls().is_empty());
    }

    #[tokio::test]
    async fn a_second_pass_over_a_converged_host_boots_nothing_a_second_time() {
        let _serial = ONE_HOST_AT_A_TIME.lock().await;
        let host = test_host().await;
        reconcile(host.arc(), &running_app(), Trigger::Change).await;
        host.vms.set_status(running_vm());

        reconcile(host.arc(), &running_app(), Trigger::Change).await;

        assert_eq!(host.vms.calls(), vec![VmCall::Boot]);
        let snapshot = host.state.snapshot().await;
        assert!(snapshot.converged);
        assert!(!snapshot.deferred_work);
    }

    #[tokio::test]
    async fn a_removal_this_pass_could_not_finish_is_left_for_the_next_one() {
        let _serial = ONE_HOST_AT_A_TIME.lock().await;
        let host = test_host().await;
        host.state.put_record(instance_record(|_| {})).await;
        host.vms.set_status(running_vm());

        reconcile(
            host.arc(),
            &desired_state(|state| {
                state.volumes = vec![desired_volume(|volume| {
                    volume.desired_state = protocol::DesiredPresence::Absent
                })]
            }),
            Trigger::Change,
        )
        .await;

        let snapshot = host.state.snapshot().await;
        assert!(snapshot.deferred_work);
        assert!(snapshot.converged);
    }

    #[tokio::test]
    async fn a_refresh_publishes_the_routes_for_the_apps_this_host_holds() {
        let host = test_host().await;
        host.slot_for(&app_id()).await.unwrap();
        host.state.put_record(instance_record(|_| {})).await;

        refresh(host.arc()).await;

        let record = host.state.record(&app_id()).await.unwrap();
        assert_eq!(
            host.router
                .routes()
                .await
                .port_for(app_hostname().hostname.as_str()),
            Some(record.host_port)
        );
        assert_eq!(host.repositories.instances.all().await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn a_microvm_the_host_never_started_is_not_asked_what_its_guest_said() {
        let host = test_host().await;
        host.vms.set_verdict("the tenant used its 5 restarts");
        host.vms.set_status(failed_vm());
        host.state.put_record(instance_record(|_| {})).await;

        refresh(host.arc()).await;

        let record = host.state.record(&app_id()).await.unwrap();
        assert_eq!(record.state, InstanceState::Failed);
        assert_eq!(record.message.unwrap().as_str(), "the microVM exited");
    }

    mod a_running_app_whose_guest_exited {
        use super::*;
        use crate::domain::backoff::AttemptWindow;

        const A_MINUTE_AGO_MS: i64 = 60_000;

        fn spent() -> AttemptWindow {
            AttemptWindow {
                attempts: protocol::DEFAULT_RESTART_POLICY.max_restarts + 1,
                last_attempt_at_ms: Some(crate::clock::now_ms()),
            }
        }

        async fn exited_after(host: &TestHost, uptime_ms: i64, window: AttemptWindow) {
            host.vms.set_status(stopped_vm());
            host.state
                .put_record(instance_record(|record| {
                    record.started_at = Some(protocol::Timestamp::from_epoch_ms(
                        crate::clock::now_ms() - uptime_ms,
                    ));
                    record.start_attempts = window;
                }))
                .await;
            refresh(host.arc()).await;
            assert_eq!(
                host.state.record(&app_id()).await.unwrap().state,
                InstanceState::Failed
            );
        }

        #[tokio::test]
        async fn is_booted_again_once_its_backoff_is_done_and_the_boot_counts_as_an_attempt() {
            let _serial = ONE_HOST_AT_A_TIME.lock().await;
            let host = test_host().await;
            reconcile(host.arc(), &running_app(), Trigger::Change).await;
            assert_eq!(host.vms.calls(), vec![VmCall::Boot]);
            // Ten seconds of running, in which the guest restarted its tenant twice, then the
            // guest went away: past the first backoff, short of the reset.
            let booted_at = crate::clock::now_ms() - 10_000;
            host.state
                .update_record(&app_id(), |record| {
                    record.started_at = Some(protocol::Timestamp::from_epoch_ms(booted_at));
                    record.start_attempts.last_attempt_at_ms = Some(booted_at);
                    record.restart_count = 2;
                    record.last_restart = Some(reported_restart(|_| {}));
                })
                .await;
            host.vms.set_status(stopped_vm());
            refresh(host.arc()).await;
            assert_eq!(
                host.state.record(&app_id()).await.unwrap().state,
                InstanceState::Failed
            );

            reconcile(host.arc(), &running_app(), Trigger::Tick).await;

            assert_eq!(host.vms.calls(), vec![VmCall::Boot, VmCall::Boot]);
            let record = host.state.record(&app_id()).await.unwrap();
            assert_eq!(record.state, InstanceState::Starting);
            assert_eq!(
                record.start_attempts.attempts, 2,
                "the host's own boots are the attempts"
            );
            // A cold boot is a fresh guest, and what the last one counted was that guest's.
            assert_eq!(record.restart_count, 0);
            assert_eq!(record.last_restart, None);
            assert_eq!(record.message, None);
        }

        #[tokio::test]
        async fn is_left_down_while_its_backoff_still_has_time_to_run() {
            let _serial = ONE_HOST_AT_A_TIME.lock().await;
            let host = test_host().await;
            host.volumes.provision(&desired_volume(|_| {})).await.unwrap();
            exited_after(
                &host,
                100,
                AttemptWindow {
                    attempts: 3,
                    last_attempt_at_ms: Some(crate::clock::now_ms() - 100),
                },
            )
            .await;

            reconcile(host.arc(), &running_app(), Trigger::Tick).await;

            assert!(host.vms.calls().is_empty(), "{:?}", host.vms.calls());
            let record = host.state.record(&app_id()).await.unwrap();
            assert_eq!(record.state, InstanceState::Failed);
            assert_eq!(record.start_attempts.attempts, 3);
            assert!(
                record.message.unwrap().as_str().contains("the microVM exited"),
                "what happened to it is kept while it waits"
            );
        }

        #[tokio::test]
        async fn that_spent_its_budget_without_staying_up_is_out_of_restarts() {
            let _serial = ONE_HOST_AT_A_TIME.lock().await;
            let host = test_host().await;
            host.volumes.provision(&desired_volume(|_| {})).await.unwrap();
            exited_after(&host, 10_000, spent()).await;

            reconcile(host.arc(), &running_app(), Trigger::Tick).await;
            reconcile(host.arc(), &running_app(), Trigger::Tick).await;

            assert!(host.vms.calls().is_empty(), "{:?}", host.vms.calls());
            let record = host.state.record(&app_id()).await.unwrap();
            assert_eq!(record.state, InstanceState::Failed);
            let said = record.message.unwrap();
            assert!(said.as_str().contains("out of restarts"), "{}", said.as_str());
            assert!(
                said.as_str().contains("until it is deployed afresh"),
                "{}",
                said.as_str()
            );
            // One exit, and one running out of restarts: said once, counted once, however many
            // passes say it again.
            let [_, _, _, _, _, exited, _, out_of_restarts] = host.metrics.health.of(&app_id()).failures;
            assert_eq!((exited, out_of_restarts), (1, 1));
        }

        #[tokio::test]
        async fn that_stayed_up_past_reset_after_ms_starts_a_fresh_window_however_spent_the_old_one() {
            let _serial = ONE_HOST_AT_A_TIME.lock().await;
            let host = test_host().await;
            host.volumes.provision(&desired_volume(|_| {})).await.unwrap();
            let reset_after_ms = protocol::DEFAULT_RESTART_POLICY.reset_after_ms as i64;
            exited_after(&host, reset_after_ms + A_MINUTE_AGO_MS, spent()).await;
            assert_eq!(
                host.state.record(&app_id()).await.unwrap().start_attempts,
                crate::domain::backoff::NO_START_ATTEMPTS,
                "the window was given back the moment the exit was seen"
            );

            reconcile(host.arc(), &running_app(), Trigger::Tick).await;

            assert_eq!(host.vms.calls(), vec![VmCall::Boot]);
            let record = host.state.record(&app_id()).await.unwrap();
            assert_eq!(record.state, InstanceState::Starting);
            assert_eq!(record.start_attempts.attempts, 1);
        }

        #[tokio::test]
        async fn that_was_on_request_is_left_for_the_next_request() {
            let _serial = ONE_HOST_AT_A_TIME.lock().await;
            let host = test_host().await;
            host.volumes.provision(&desired_volume(|_| {})).await.unwrap();
            host.vms.set_status(stopped_vm());
            host.state
                .put_record(instance_record(|record| {
                    record.on_request = true;
                    record.started_at = Some(observed_at());
                }))
                .await;
            refresh(host.arc()).await;
            let on_request = desired_state(|state| {
                state.volumes = vec![desired_volume(|_| {})];
                state.instances = vec![desired_instance(|instance| {
                    instance.desired_state = DesiredInstanceState::OnRequest
                })];
            });

            reconcile(host.arc(), &on_request, Trigger::Tick).await;

            assert!(host.vms.calls().is_empty(), "{:?}", host.vms.calls());
            let record = host.state.record(&app_id()).await.unwrap();
            assert_eq!(record.state, InstanceState::Failed);
            assert!(record.message.unwrap().as_str().contains("the microVM exited"));
        }
    }
}
