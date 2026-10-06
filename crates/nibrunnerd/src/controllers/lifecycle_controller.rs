use std::sync::Arc;

use protocol::HostVersions;

use crate::adapters::guest_measurements::VsockMeasurements;
use crate::controllers::activity_controller::ActivityController;
use crate::controllers::converge_controller::ConvergeController;
use crate::controllers::idle_controller::IdleController;
use crate::controllers::measurement_controller::MeasurementController;
use crate::controllers::status_controller::StatusController;
use crate::controllers::Controller;
use crate::host::Host;
use crate::services::idle_service::HostIdle;
use crate::services::reconcile_service::HostReconciler;
use crate::services::report_service::HostReporter;
use crate::services::report_service::ReportService;
use crate::services::usage_service::HostUsage;

pub struct LifecycleController {
    host: Arc<Host>,
    versions: HostVersions,
}

impl LifecycleController {
    pub fn new(host: Arc<Host>, versions: HostVersions) -> Arc<Self> {
        Arc::new(Self { host, versions })
    }

    pub async fn start(&self) {
        self.host.load().await;
        let adopted = self.host.vms.adopted_app_ids().await;
        if !adopted.is_empty() {
            tracing::info!(adopted = adopted.len(), "microVMs from an earlier daemon adopted");
            for app_id in &adopted {
                if let Err(error) = self.host.vms.readopt(app_id).await {
                    tracing::warn!(%app_id, %error, "an adopted microVM could not be fully recovered");
                }
            }
            for (app_id, status) in self.host.vms.statuses(&adopted).await {
                if status.frozen {
                    self.host
                        .state
                        .update_record(&app_id, |record| record.state = protocol::InstanceState::Frozen)
                        .await;
                }
            }
        }
        crate::domain::reconcile::network::reclaim_stranded_taps(&self.host).await;
        crate::domain::reconcile::network::apply_activators(&self.host).await;
        crate::domain::reconcile::network::apply_network(&self.host).await;
        crate::run::serve_proxy(&self.host);
        crate::run::serve_metrics(&self.host);
        crate::run::serve_filesystem(&self.host);
    }

    pub fn controllers(&self) -> Vec<Arc<dyn Controller>> {
        let reconciler = HostReconciler::new(self.host.clone());
        let reports = HostReporter::new(self.host.clone(), self.versions.clone());
        let measurements = VsockMeasurements::new(self.host.clone());
        let idle = HostIdle::new(self.host.clone());

        let held: Vec<Arc<dyn Controller>> = vec![
            ConvergeController::new(self.host.clone(), reconciler.clone()),
            StatusController::new(self.host.clone(), reconciler, reports),
            ActivityController::new(idle.clone()),
            IdleController::new(idle),
            MeasurementController::new(HostUsage::new(self.host.clone(), measurements)),
        ];
        held
    }

    pub async fn stop(&self) {
        self.host.persist().await;
        HostReporter::new(self.host.clone(), self.versions.clone())
            .publish()
            .await;
    }
}
