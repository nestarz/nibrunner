//! Terminal retention is decided beside admission, never from an eventually consistent report.
use crate::host::Host;
use protocol::InstanceState;
use std::sync::Arc;

pub(crate) async fn apply(host: &Arc<Host>, now: i64) {
    let records = host.state.records().await;
    for previous in records {
        if previous.expiry.is_none() {
            continue;
        }
        let id = &previous.app_id;
        let _transition = host.state.transition(id).await;
        // Gate admission and reports until the terminal record has reached durable storage.
        // Every general persistence pass takes this mutex before reading the snapshot too.
        let _writing = host.state.persistence.lock().await;
        let mut snapshot = host.state.locked_snapshot().await;
        if snapshot.snapshotting.contains(id) || host.metrics.proxy.open_requests_for(id) != 0 {
            continue;
        }
        let Some(mut record) = snapshot.records.get(id).cloned() else {
            continue;
        };
        let Some(policy) = record.expiry else {
            continue;
        };
        if record.deployment_id != previous.deployment_id {
            continue;
        }
        let Some(since) = record.expiry_since_ms else {
            continue;
        };
        let last = since.max(snapshot.last_active_at_ms.get(id).copied().unwrap_or(since));
        if record.expired_at_ms.is_none() && now.saturating_sub(last) < policy.idle_ms.get() as i64 {
            continue;
        }
        record.expired_at_ms.get_or_insert(now);
        record.state = InstanceState::Expired;
        record.stop_requested = true;
        let records: Vec<_> = snapshot
            .records
            .values()
            .map(|held| {
                if held.app_id == *id {
                    record.clone()
                } else {
                    held.clone()
                }
            })
            .collect();
        if let Err(error) = host.repositories.instances.replace_all(&records).await {
            tracing::error!(%id, error = %error.message(), "expiry could not be persisted");
            continue;
        }
        snapshot.records.insert(id.clone(), record);
        drop(snapshot);
        drop(_writing);
        if let Err(error) = host.vms.stop(id).await {
            tracing::warn!(%id, error = %error.message(), "expired instance could not stop");
            continue;
        }
        if let Err(error) = host.vms.discard(id).await {
            tracing::warn!(%id, error = %error.message(), "expired snapshot could not be removed");
        }
        host.router.close_connections_to(id).await;
        host.state.signal_report();
    }
    super::network::apply_activators(host).await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ports::VmCall;
    use crate::test_support::*;
    use protocol::{ExpiryIdleMs, ExpiryPolicy};
    const IDLE: i64 = 3_600_000;
    async fn expirable() -> TestHost {
        let host = test_host().await;
        host.state
            .put_record(instance_record(|r| {
                r.on_request = true;
                r.expiry = Some(ExpiryPolicy {
                    idle_ms: ExpiryIdleMs::try_from(IDLE as u64).unwrap(),
                });
                r.expiry_since_ms = Some(0);
            }))
            .await;
        host
    }
    #[tokio::test]
    async fn an_admitted_request_one_millisecond_before_expiry_renews_it() {
        let host = expirable().await;
        let open = host
            .state
            .admit(&app_id(), IDLE - 1, || host.metrics.proxy.open(&app_id()))
            .await
            .unwrap();
        drop(open);
        apply(host.arc(), IDLE).await;
        assert!(host.vms.calls().is_empty());
        apply(host.arc(), 2 * IDLE - 1).await;
        assert_eq!(
            host.state.record(&app_id()).await.unwrap().state,
            InstanceState::Expired
        );
    }
    #[tokio::test]
    async fn an_open_stream_pins_the_revision_and_terminal_expiry_refuses_future_admission() {
        let host = expirable().await;
        let open = host
            .state
            .admit(&app_id(), 0, || host.metrics.proxy.open(&app_id()))
            .await
            .unwrap();
        apply(host.arc(), IDLE + 1).await;
        assert!(host.vms.calls().is_empty());
        drop(open);
        apply(host.arc(), IDLE + 1).await;
        assert_eq!(host.vms.calls(), [VmCall::Stop, VmCall::Discard]);
        assert!(host.state.admit(&app_id(), IDLE + 2, || ()).await.is_none());
        assert_eq!(host.state.snapshot().await.last_active_at_ms[&app_id()], 0);
        let persisted = host.repositories.instances.all().await.unwrap();
        let restarted = crate::state::HostState::shared();
        restarted.put_record(persisted[0].clone()).await;
        assert!(restarted.admit(&app_id(), IDLE + 3, || ()).await.is_none());
        assert_eq!(persisted[0].expired_at_ms, Some(IDLE + 1));
    }
    #[tokio::test]
    async fn no_policy_never_expires() {
        let host = test_host().await;
        host.state.put_record(instance_record(|_| {})).await;
        apply(host.arc(), i64::MAX).await;
        assert!(host.vms.calls().is_empty());
        assert_eq!(
            host.state.record(&app_id()).await.unwrap().state,
            InstanceState::Running
        );
    }
    #[tokio::test]
    async fn a_stale_observation_cannot_revive_a_terminal_instance() {
        let host = expirable().await;
        let stale = host.state.record(&app_id()).await.unwrap();
        apply(host.arc(), IDLE).await;
        host.state.put_record(stale).await;
        host.state
            .update_record(&app_id(), |r| r.state = InstanceState::Running)
            .await;
        assert_eq!(
            host.state.record(&app_id()).await.unwrap().state,
            InstanceState::Expired
        );
    }
    #[tokio::test]
    async fn policy_removal_and_a_new_deployment_revive_and_reset_the_anchor() {
        let host = expirable().await;
        apply(host.arc(), IDLE).await;
        host.state
            .update_record(&app_id(), |r| r.apply_expiry(None, &deployment_id(), IDLE + 1))
            .await;
        let mut r = host.state.record(&app_id()).await.unwrap();
        assert_eq!(r.state, InstanceState::Idle);
        assert_eq!(r.expired_at_ms, None);
        let policy = Some(ExpiryPolicy {
            idle_ms: ExpiryIdleMs::try_from(IDLE as u64).unwrap(),
        });
        r.apply_expiry(policy, &deployment_id(), IDLE + 2);
        assert_eq!(r.expiry_since_ms, Some(IDLE + 2));
        r.apply_expiry(policy, &deployment_id(), IDLE + 3);
        assert_eq!(r.expiry_since_ms, Some(IDLE + 2));
        r.state = InstanceState::Expired;
        r.expired_at_ms = Some(2 * IDLE + 2);
        let mut next = record_fields();
        next.expiry = policy;
        next.deployment_id = protocol::DeploymentId::parse("dep-next").unwrap();
        r.adopt(next);
        assert_eq!(r.expired_at_ms, None);
        assert_eq!(r.state, InstanceState::Idle);
    }
    #[tokio::test]
    async fn a_snapshot_in_progress_cannot_be_expired() {
        let host = expirable().await;
        host.state.mark_snapshotting(&app_id(), true).await;
        apply(host.arc(), IDLE).await;
        assert!(host.vms.calls().is_empty());
        host.state.mark_snapshotting(&app_id(), false).await;
        apply(host.arc(), IDLE).await;
        assert_eq!(host.vms.calls(), [VmCall::Stop, VmCall::Discard]);
    }
    struct DelayedFailure {
        entered: tokio::sync::Notify,
        release: tokio::sync::Notify,
    }
    #[async_trait::async_trait]
    impl crate::repositories::instances_repository::InstanceRepository for DelayedFailure {
        async fn all(
            &self,
        ) -> Result<Vec<crate::domain::report::InstanceRecord>, crate::domain::store::StoreError> {
            Ok(vec![])
        }
        async fn summary(&self) -> Result<Vec<(protocol::AppId, String)>, crate::domain::store::StoreError> {
            Ok(vec![])
        }
        async fn replace_all(
            &self,
            _: &[crate::domain::report::InstanceRecord],
        ) -> Result<(), crate::domain::store::StoreError> {
            self.entered.notify_one();
            self.release.notified().await;
            Err(crate::domain::store::StoreError::Unwritable("disk full".into()))
        }
    }
    #[tokio::test]
    async fn a_report_cannot_observe_expiry_before_durability_and_failed_persistence_leaves_it_live() {
        let mut host = expirable().await;
        let storage = Arc::new(DelayedFailure {
            entered: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
        });
        Arc::get_mut(&mut host.host).unwrap().repositories.instances = storage.clone();
        let task = tokio::spawn({
            let host = host.arc().clone();
            async move {
                apply(&host, IDLE).await;
            }
        });
        storage.entered.notified().await;
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(10), host.state.snapshot())
                .await
                .is_err()
        );
        storage.release.notify_one();
        task.await.unwrap();
        let report = host.state.record(&app_id()).await.unwrap();
        assert_eq!(report.state, InstanceState::Running);
        assert_eq!(report.expired_at_ms, None);
        assert!(host.vms.calls().is_empty());
        assert!(host.state.admit(&app_id(), IDLE, || ()).await.is_some());
    }
    #[tokio::test]
    async fn restart_before_activity_persistence_grants_a_new_observed_window_but_keeps_terminal_records() {
        let host = expirable().await;
        host.persist().await;
        host.state.mark_active(&app_id(), crate::clock::now_ms()).await;
        host.load().await;
        let anchor = host
            .state
            .record(&app_id())
            .await
            .unwrap()
            .expiry_since_ms
            .unwrap();
        apply(host.arc(), anchor + IDLE - 1).await;
        assert_eq!(
            host.state.record(&app_id()).await.unwrap().state,
            InstanceState::Running
        );
        apply(host.arc(), anchor + IDLE).await;
        host.load().await;
        assert_eq!(
            host.state.record(&app_id()).await.unwrap().state,
            InstanceState::Expired
        );
    }
    #[tokio::test]
    async fn expiry_keeps_the_volume_until_the_document_drops_it_then_removes_its_file_and_record() {
        let host = expirable().await;
        host.slot_for(&app_id()).await.unwrap();
        host.volumes.provision(&desired_volume(|_| {})).await.unwrap();
        let owners = std::collections::BTreeMap::from([(volume_id(), app_id())]);
        apply(host.arc(), IDLE).await;
        assert_eq!(host.volumes.observe(&owners).await.len(), 1);
        super::super::reconcile(
            host.arc(),
            &desired_state(|_| {}),
            crate::domain::metrics::passes::Trigger::Change,
        )
        .await;
        assert!(host.volumes.observe(&owners).await.is_empty());
        assert!(host.state.record(&app_id()).await.is_none());
        assert!(host.slot_of(&app_id()).await.is_none());
    }
    #[test]
    fn an_expired_disk_is_not_deleted_while_any_document_or_other_instance_references_it() {
        use super::super::plan::*;
        let held = observed_instance(|i| {
            i.expired = true;
            i.running = false;
        });
        let observed = observed_state(|s| {
            s.instances = vec![held.clone()];
            s.volumes = vec![observed_volume(|_| {})];
        });
        let empty = desired_state(|_| {});
        let planned = plan_reconcile(&empty, &observed);
        assert!(matches!(planned.volumes[0], VolumePlan::Teardown { .. }));
        let kept = desired_state(|d| d.volumes = vec![desired_volume(|_| {})]);
        assert!(!plan_reconcile(&kept, &observed)
            .volumes
            .iter()
            .any(|v| matches!(v, VolumePlan::Teardown { .. })));
        let mut shared = observed.clone();
        shared.instances.push(observed_instance(|i| {
            i.app_id = protocol::AppId::parse("other-app").unwrap();
            i.running = false;
        }));
        assert!(!plan_reconcile(&empty, &shared)
            .volumes
            .iter()
            .any(|v| matches!(v, VolumePlan::Teardown { .. })));
        let mut no_backing = observed;
        no_backing.volumes.clear();
        assert!(matches!(
            plan_reconcile(&empty, &no_backing).volumes[0],
            VolumePlan::Teardown { .. }
        ));
    }
}
