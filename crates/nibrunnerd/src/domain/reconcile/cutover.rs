use std::collections::BTreeMap;
use std::sync::Arc;

use protocol::{AppId, DesiredInstance, DesiredInstanceState, InstanceState, StateMessage};

use crate::clock::now_ms;
use crate::domain::health::initial_tracker;
use crate::domain::reconcile::plan::{InstancePlan, ReconcilePlan, StagingProgress};
use crate::domain::report::instance_record::{Cutover, InstanceRecord};
use crate::host::Host;

/// Every name an app id may have is the document's to give, so this one is only ever used while
/// the document names no app by it; `plan` falls back to stopping first when it does.
const STAGING_SUFFIX: &str = "--next";

/// The id a new deployment of `app_id` runs under while it boots beside the one still serving.
/// None when the app's own id leaves no room for the suffix.
pub(crate) fn staging_id(app_id: &AppId) -> Option<AppId> {
    AppId::parse(format!("{app_id}{STAGING_SUFFIX}")).ok()
}

pub(super) fn progress(staged: &InstanceRecord) -> StagingProgress {
    match staged.state {
        InstanceState::Pending => StagingProgress::Waiting,
        InstanceState::Starting => StagingProgress::Starting,
        InstanceState::Running => StagingProgress::Healthy,
        _ => StagingProgress::Failed,
    }
}

/// The new deployment as it is booted beside the old one: under the staging id, kept up whatever
/// the app's activation says until it takes the app over, and expiring never, since it is not yet
/// the app.
fn staged_instance(desired: &DesiredInstance, staging: &AppId) -> DesiredInstance {
    DesiredInstance {
        app_id: staging.clone(),
        desired_state: DesiredInstanceState::Running,
        idle_timeout_ms: None,
        activation: None,
        expiry: None,
        ..desired.clone()
    }
}

async fn waiting(host: &Host, staged: &DesiredInstance, owner: &AppId, message: StateMessage) -> bool {
    let Ok(slot) = host.slot_for(&staged.app_id).await else {
        return false;
    };
    let fields = super::instances::record_fields(staged, &slot);
    let mut record =
        host.state.record(&staged.app_id).await.unwrap_or_else(|| {
            InstanceRecord::new(fields.clone(), InstanceState::Pending, initial_tracker())
        });
    record.adopt(fields);
    record.state = InstanceState::Pending;
    record.staging_for = Some(owner.clone());
    record.message = Some(message);
    host.state.put_record(record).await;
    true
}

/// Takes down what was booted for a cut-over, and everything this host held for it, slot and all.
async fn retire(host: &Host, staging: &AppId, reason: &str) {
    super::instances::stop_instance(host, staging, reason).await;
    let _ = host.vms.discard(staging).await;
    host.state.drop_record(staging).await;
    host.state
        .modify(|snapshot| {
            snapshot.next_probe_at_ms.remove(staging);
            snapshot.deploys.remove(staging);
            snapshot.compute_ticks.remove(staging);
            snapshot.compute_usage.remove(staging);
            snapshot.app_traffic.remove(staging);
        })
        .await;
    super::volumes::give_back_slot(host, staging).await;
}

async fn stage(host: &Arc<Host>, desired: &DesiredInstance, timeout_ms: u64) {
    let owner = &desired.app_id;
    let Some(staging) = staging_id(owner) else {
        return;
    };
    let deadline_ms = now_ms().saturating_add(i64::try_from(timeout_ms).unwrap_or(i64::MAX));
    host.state
        .update_record(owner, |record| {
            if record.cutover.as_ref().map(|cutover| &cutover.deployment_id) != Some(&desired.deployment_id) {
                record.cutover = Some(Cutover {
                    deployment_id: desired.deployment_id.clone(),
                    deadline_ms,
                    abandoned: false,
                });
                record.cutover_note = None;
                record.cutover_outcome = None;
            }
        })
        .await;

    let _transition = host.state.transition(&staging).await;
    if let Some(earlier) = host.state.record(&staging).await {
        if earlier.deployment_id != desired.deployment_id {
            retire(host, &staging, "superseded").await;
        } else if earlier.state != InstanceState::Pending {
            return;
        }
    }
    let staged = staged_instance(desired, &staging);
    if !host.state.snapshot().await.isolated {
        return;
    }
    // Both deployments are held at once until the cut-over, so the new one's memory is
    // reserved as any start's is; a host with no room for it leaves it waiting to its deadline.
    let _reservation = match host
        .reserve_memory(
            &staging,
            staged.config.resources,
            super::pressure::ReclaimPurpose::Deployment,
        )
        .await
    {
        Ok(reservation) => reservation,
        Err(shortfall_mib) => {
            waiting(
                host,
                &staged,
                owner,
                StateMessage::new(format!(
                    "waiting to be started: its host is {shortfall_mib} MiB short of the memory it needs"
                )),
            )
            .await;
            return;
        }
    };
    if !waiting(
        host,
        &staged,
        owner,
        StateMessage::new("being started beside the deployment still serving"),
    )
    .await
    {
        tracing::warn!(app_id = %owner, "no slot is left to boot the new deployment beside the one serving");
        return;
    }
    super::instances::start_instance(host, &staged).await;
    tracing::info!(app_id = %owner, deployment_id = %desired.deployment_id, "new deployment booting beside the one serving");
}

fn exchanged<T>(map: &mut BTreeMap<AppId, T>, first: &AppId, second: &AppId) {
    let one = map.remove(first);
    let two = map.remove(second);
    if let Some(value) = one {
        map.insert(second.clone(), value);
    }
    if let Some(value) = two {
        map.insert(first.clone(), value);
    }
}

/// The new deployment takes the app's id, slot and record, and the old one takes the staging id
/// in their place, in one write the report and the router read whole; then the router is told,
/// and only once requests are going to the new deployment is the old one stopped.
async fn promote(host: &Arc<Host>, desired: &DesiredInstance) {
    let owner = &desired.app_id;
    let Some(staging) = staging_id(owner) else {
        return;
    };
    let _owner = host.state.transition(owner).await;
    let _staging = host.state.transition(&staging).await;
    let snapshot = host.state.snapshot().await;
    let (Some(old), Some(new)) = (snapshot.records.get(owner), snapshot.records.get(&staging)) else {
        return;
    };
    let (old_deployment, new_deployment) = (old.deployment_id.clone(), new.deployment_id.clone());
    let now = now_ms();
    if let Err(error) = host.vms.exchange(owner, &staging).await {
        tracing::error!(app_id = %owner, error = %error.message(), "the new deployment could not take its app over");
        return;
    }
    host.allocator.lock().await.exchange(owner, &staging);
    host.state
        .modify(|snapshot| {
            let (Some(mut new), Some(mut old)) =
                (snapshot.records.remove(&staging), snapshot.records.remove(owner))
            else {
                return;
            };
            new.app_id = owner.clone();
            new.staging_for = None;
            new.cutover = None;
            new.cutover_note = None;
            new.cutover_outcome = None;
            new.message = None;
            new.hostnames = desired.hostnames.clone();
            new.desired_running = desired.desired_state != DesiredInstanceState::Stopped;
            new.on_request = desired.desired_state == DesiredInstanceState::OnRequest;
            new.apply_expiry(desired.expiry.clone(), &desired.deployment_id, now);
            old.app_id = staging.clone();
            old.staging_for = Some(owner.clone());
            old.cutover = None;
            old.cutover_note = None;
            old.cutover_outcome = None;
            snapshot.records.insert(owner.clone(), new);
            snapshot.records.insert(staging.clone(), old);
            exchanged(&mut snapshot.next_probe_at_ms, owner, &staging);
            exchanged(&mut snapshot.deploys, owner, &staging);
            exchanged(&mut snapshot.compute_ticks, owner, &staging);
            exchanged(&mut snapshot.app_traffic, owner, &staging);
        })
        .await;
    super::network::apply_network(host).await;
    super::network::apply_activators(host).await;
    super::network::apply_routes(host).await;
    tracing::info!(app_id = %owner, from = %old_deployment, to = %new_deployment, "deployment cut over");
    retire(host, &staging, "superseded").await;
}

async fn abandon(host: &Host, desired: &DesiredInstance) {
    let owner = &desired.app_id;
    let Some(staging) = staging_id(owner) else {
        return;
    };
    let _staging = host.state.transition(&staging).await;
    let why = host
        .state
        .record(&staging)
        .await
        .and_then(|record| record.message)
        .map(|message| format!(" ({})", message.as_str()))
        .unwrap_or_default();
    retire(host, &staging, "cutover-abandoned").await;
    let serving = host.state.record(owner).await.map(|record| record.deployment_id);
    let note = StateMessage::new(format!(
        "cutover_abandoned: deployment {} did not become healthy within its cutoverTimeoutMs{why}, so it was stopped and deployment {} keeps serving",
        desired.deployment_id,
        serving.map_or_else(|| "the old one".to_string(), |id| id.to_string()),
    ));
    tracing::warn!(app_id = %owner, deployment_id = %desired.deployment_id, note = note.as_str(), "cut-over abandoned");
    host.state
        .update_record(owner, |record| {
            if let Some(cutover) = &mut record.cutover {
                cutover.abandoned = true;
            }
            record.cutover_note = Some(note);
            record.cutover_outcome = Some(protocol::CutoverOutcome::Abandoned {
                deployment_id: desired.deployment_id.clone(),
            });
        })
        .await;
}

pub(super) async fn apply_cutovers(host: &Arc<Host>, plan: &ReconcilePlan) {
    for action in &plan.instances {
        match action {
            InstancePlan::Stage { desired, timeout_ms } => stage(host, desired, *timeout_ms).await,
            InstancePlan::Promote { desired } => promote(host, desired).await,
            InstancePlan::Abandon { desired } => abandon(host, desired).await,
            _ => {}
        }
    }
}

/// A replacement that fell back from a cut-over says so on the instance it started.
pub(super) async fn note_fallbacks(host: &Host, plan: &ReconcilePlan) {
    for action in &plan.instances {
        if let InstancePlan::FallBack { desired, reason } = action {
            host.state
                .update_record(&desired.app_id, |record| {
                    record.cutover_note = Some(super::plan::fallback_note(*reason));
                    record.cutover_outcome = Some(protocol::CutoverOutcome::Fallback { reason: *reason });
                })
                .await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::reconcile::plan::CutoverFallback;
    use crate::domain::report::build_report::to_reported_instance;
    use crate::domain::report::routes::renderable_routes;
    use crate::ports::VmCall;
    use crate::test_support::*;
    use protocol::DeploymentId;

    fn new_deployment() -> DeploymentId {
        DeploymentId::parse("dep-2").unwrap()
    }

    fn staging() -> AppId {
        staging_id(&app_id()).unwrap()
    }

    fn wanted() -> DesiredInstance {
        desired_instance(|instance| {
            instance.deployment_id = new_deployment();
            instance.volume_id = None;
            instance.scratch = Some(protocol::Scratch::Memory {
                mib: std::num::NonZeroU32::new(64).unwrap(),
            });
            instance.hostnames = vec![app_hostname()];
        })
    }

    fn plan_of(action: InstancePlan) -> ReconcilePlan {
        ReconcilePlan {
            instances: vec![action],
            ..Default::default()
        }
    }

    async fn serving(host: &TestHost) {
        host.slot_for(&app_id()).await.unwrap();
        host.state
            .put_record(instance_record(|record| record.volume_id = None))
            .await;
    }

    async fn booted_beside(host: &TestHost, state: InstanceState) -> u16 {
        let slot = host.slot_for(&staging()).await.unwrap();
        host.state
            .put_record(instance_record(|record| {
                record.app_id = staging();
                record.volume_id = None;
                record.deployment_id = new_deployment();
                record.host_port = Some(slot.host_port);
                record.state = state;
                record.staging_for = Some(app_id());
            }))
            .await;
        slot.host_port.get()
    }

    #[test]
    fn an_app_id_with_no_room_for_the_suffix_has_no_staging_id() {
        assert_eq!(staging().as_str(), "app-1--next");
        assert!(staging_id(&AppId::parse("a".repeat(60)).unwrap()).is_none());
    }

    #[tokio::test]
    async fn the_new_deployment_boots_beside_the_old_one_without_being_routed_or_reported() {
        let host = test_host().await;
        host.state.modify(|snapshot| snapshot.isolated = true).await;
        serving(&host).await;

        apply_cutovers(
            host.arc(),
            &plan_of(InstancePlan::Stage {
                desired: wanted(),
                timeout_ms: 600_000,
            }),
        )
        .await;

        assert_eq!(host.vms.calls(), vec![VmCall::Boot]);
        let owner = host.state.record(&app_id()).await.unwrap();
        assert_eq!(owner.deployment_id, deployment_id());
        assert_eq!(owner.cutover.unwrap().deployment_id, new_deployment());
        let staged = host.state.record(&staging()).await.unwrap();
        assert_eq!(staged.staging_for, Some(app_id()));
        assert_eq!(staged.state, InstanceState::Starting);
        let routes = renderable_routes(&host.state.records().await);
        assert_eq!(routes.len(), 1);
        assert_eq!(routes[0].app_id, app_id());
    }

    #[tokio::test]
    async fn a_healthy_new_deployment_takes_the_app_its_hostnames_and_its_slot_and_the_old_one_is_stopped() {
        let host = test_host().await;
        serving(&host).await;
        let staged_port = booted_beside(&host, InstanceState::Running).await;

        apply_cutovers(host.arc(), &plan_of(InstancePlan::Promote { desired: wanted() })).await;

        assert_eq!(
            host.vms.calls(),
            vec![VmCall::Exchange, VmCall::Stop, VmCall::Discard, VmCall::DeleteTap]
        );
        let owner = host.state.record(&app_id()).await.unwrap();
        assert_eq!(owner.deployment_id, new_deployment());
        assert_eq!(owner.staging_for, None);
        assert_eq!(owner.host_port.unwrap().get(), staged_port);
        assert_eq!(
            host.slot_of(&app_id()).await.unwrap().host_port.get(),
            staged_port
        );
        assert!(host.state.record(&staging()).await.is_none());
        assert!(host.slot_of(&staging()).await.is_none());
        let routes = renderable_routes(&host.state.records().await);
        assert_eq!(routes.len(), 1);
        assert_eq!(routes[0].host_port.get(), staged_port);
        assert_eq!(routes[0].hostnames, vec![app_hostname()]);
    }

    #[tokio::test]
    async fn a_new_deployment_never_healthy_in_time_is_stopped_and_the_instance_says_the_old_one_serves() {
        let host = test_host().await;
        serving(&host).await;
        host.state
            .update_record(&app_id(), |record| {
                record.cutover = Some(Cutover {
                    deployment_id: new_deployment(),
                    deadline_ms: 0,
                    abandoned: false,
                });
            })
            .await;
        booted_beside(&host, InstanceState::Failed).await;
        host.state
            .update_record(&staging(), |record| {
                record.message = Some(StateMessage::new("it never answered its health check"));
            })
            .await;

        apply_cutovers(host.arc(), &plan_of(InstancePlan::Abandon { desired: wanted() })).await;

        assert!(host.state.record(&staging()).await.is_none());
        assert!(host.slot_of(&staging()).await.is_none());
        let owner = host.state.record(&app_id()).await.unwrap();
        assert_eq!(owner.deployment_id, deployment_id());
        assert!(owner.cutover.as_ref().unwrap().abandoned);
        assert_eq!(
            to_reported_instance(&owner, None).cutover,
            Some(protocol::CutoverOutcome::Abandoned {
                deployment_id: new_deployment()
            })
        );
        let said = to_reported_instance(&owner, None).message.unwrap();
        assert!(
            said.as_str().starts_with("cutover_abandoned: deployment dep-2"),
            "{said:?}"
        );
        assert!(
            said.as_str().contains("it never answered its health check"),
            "{said:?}"
        );
        assert!(
            said.as_str()
                .contains(&format!("deployment {} keeps serving", deployment_id())),
            "{said:?}"
        );
    }

    #[tokio::test]
    async fn a_replacement_that_could_not_be_a_cut_over_says_why() {
        let host = test_host().await;
        serving(&host).await;

        note_fallbacks(
            &host,
            &plan_of(InstancePlan::FallBack {
                desired: desired_instance(|_| {}),
                reason: CutoverFallback::Volume,
            }),
        )
        .await;

        let reported = to_reported_instance(&host.state.record(&app_id()).await.unwrap(), None);
        assert_eq!(
            reported.cutover,
            Some(protocol::CutoverOutcome::Fallback {
                reason: CutoverFallback::Volume
            })
        );
        let said = reported.message.unwrap();
        assert!(said.as_str().starts_with("cutover_fallback: "), "{said:?}");
        assert!(said.as_str().contains("volume"), "{said:?}");
    }

    fn document(deployment: DeploymentId) -> protocol::HostDesiredState {
        desired_state(|state| {
            state.instances = vec![desired_instance(|instance| {
                *instance = wanted();
                instance.deployment_id = deployment;
                instance.activation = Some(protocol::ActivationPolicy {
                    snapshot_ttl_ms: None,
                    first_boot: protocol::FirstBoot::Eager,
                    sleep_when: protocol::SleepPolicy::Never,
                    cutover_timeout_ms: Some(protocol::CutoverTimeoutMs::try_from(600_000).unwrap()),
                });
            })];
        })
    }

    #[tokio::test]
    async fn passes_keep_the_old_deployment_routed_until_the_new_one_is_healthy_then_route_the_new_one() {
        use crate::domain::metrics::passes::Trigger;
        let _serial = ONE_HOST_AT_A_TIME.lock().await;
        let host = test_host().await;
        let hostname = app_hostname().hostname;
        super::super::reconcile(host.arc(), &document(deployment_id()), Trigger::Change).await;
        host.vms.set_status(crate::adapters::vm::VmStatus {
            frozen: false,
            loaded: true,
            active: true,
            failed: false,
            started_this_boot: true,
            exit: None,
        });
        host.state
            .update_record(&app_id(), |record| record.state = InstanceState::Running)
            .await;
        let old_port = host.router.routes().await.port_for(hostname.as_str()).unwrap();

        super::super::reconcile(host.arc(), &document(new_deployment()), Trigger::Change).await;

        assert!(host.vms.calls().ends_with(&[VmCall::Boot, VmCall::Boot]));
        assert_eq!(
            host.state.record(&app_id()).await.unwrap().deployment_id,
            deployment_id()
        );
        assert_eq!(
            host.router.routes().await.port_for(hostname.as_str()),
            Some(old_port)
        );

        super::super::reconcile(host.arc(), &document(new_deployment()), Trigger::Tick).await;
        assert!(!host.vms.calls().contains(&VmCall::Exchange));

        host.state
            .update_record(&staging(), |record| record.state = InstanceState::Running)
            .await;
        super::super::reconcile(host.arc(), &document(new_deployment()), Trigger::Tick).await;

        let owner = host.state.record(&app_id()).await.unwrap();
        assert_eq!(owner.deployment_id, new_deployment());
        assert!(host.state.record(&staging()).await.is_none());
        let new_port = host.router.routes().await.port_for(hostname.as_str()).unwrap();
        assert_ne!(new_port, old_port);
        assert_eq!(Some(new_port), owner.host_port);
        assert!(host.vms.calls().ends_with(&[
            VmCall::Exchange,
            VmCall::Stop,
            VmCall::Discard,
            VmCall::DeleteTap
        ]));
    }
}
