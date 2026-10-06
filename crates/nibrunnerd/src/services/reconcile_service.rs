use std::sync::Arc;

use async_trait::async_trait;
use protocol::HostDesiredState;

use crate::domain::metrics::passes::Trigger;
use crate::domain::reconcile::{reconcile, refresh};
use crate::host::Host;

#[cfg_attr(any(test, feature = "testing"), mockall::automock)]
#[async_trait]
pub trait ReconcileService: Send + Sync {
    async fn reconcile(&self, desired: &HostDesiredState, trigger: Trigger);
    async fn refresh(&self);
}

pub struct HostReconciler {
    host: Arc<Host>,
}

impl HostReconciler {
    pub fn new(host: Arc<Host>) -> Arc<Self> {
        Arc::new(Self { host })
    }
}

#[async_trait]
impl ReconcileService for HostReconciler {
    async fn reconcile(&self, desired: &HostDesiredState, trigger: Trigger) {
        reconcile(&self.host, desired, trigger).await;
    }

    async fn refresh(&self) {
        refresh(&self.host).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapters::vm::{VmExit, VmStatus};
    use crate::ports::VmCall;
    use crate::test_support::*;
    use protocol::InstanceState;
    fn running_app() -> protocol::HostDesiredState {
        desired_state(|state| {
            state.volumes = vec![desired_volume(|_| {})];
            state.instances = vec![desired_instance(|instance| {
                instance.hostnames = vec![app_hostname()]
            })];
        })
    }
    fn stopped_vm() -> VmStatus {
        VmStatus {
            loaded: true,
            active: false,
            frozen: false,
            failed: false,
            started_this_boot: true,
            exit: Some(VmExit::Code(0)),
        }
    }
    #[tokio::test]
    async fn the_service_converges_the_host_it_was_built_on() {
        let _serial = ONE_HOST_AT_A_TIME.lock().await;
        let host = test_host().await;
        let reconciler = HostReconciler::new(host.arc().clone());

        reconciler.reconcile(&running_app(), Trigger::Change).await;

        assert_eq!(host.vms.calls(), vec![VmCall::Boot]);
        assert_eq!(
            host.state.record(&app_id()).await.unwrap().state,
            InstanceState::Starting
        );
        assert!(host.state.snapshot().await.converged);
    }
    #[tokio::test]
    async fn the_refresh_it_runs_is_the_one_that_moves_a_record_onto_what_the_host_sees() {
        let host = test_host().await;
        host.vms.set_status(stopped_vm());
        host.state
            .put_record(instance_record(|record| record.started_at = Some(observed_at())))
            .await;
        let reconciler = HostReconciler::new(host.arc().clone());

        reconciler.refresh().await;

        assert_eq!(
            host.state.record(&app_id()).await.unwrap().state,
            InstanceState::Failed
        );
    }
}
