use std::sync::Arc;

use protocol::HostVersions;

use crate::adapters::guest_measurements::VsockMeasurements;
use crate::controllers::activity_controller::ActivityController;
use crate::controllers::converge_controller::ConvergeController;
use crate::controllers::idle_controller::IdleController;
use crate::controllers::measurement_controller::MeasurementController;
use crate::controllers::status_controller::StatusController;
use crate::controllers::telemetry_controller::TelemetryController;
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
        if let (Some(telemetry), Some(accepted)) = (&self.host.telemetry, self.host.accepted_document().await)
        {
            telemetry.learn(&accepted.desired);
        }
        let adopted = self.host.vms.adopted_app_ids().await;
        if !adopted.is_empty() {
            tracing::info!(adopted = adopted.len(), "microVMs from an earlier daemon adopted");
            for app_id in &adopted {
                if let Err(error) = self.host.vms.readopt(app_id).await {
                    tracing::warn!(%app_id, %error, "an adopted microVM's guest channels could not be re-established");
                }
            }
        }
        crate::domain::reconcile::network::reclaim_stranded_taps(&self.host).await;
        crate::domain::reconcile::network::apply_activators(&self.host).await;
        crate::run::serve_proxy(&self.host);
        crate::run::serve_metrics(&self.host);
        crate::run::serve_filesystem(&self.host);
    }

    pub fn controllers(&self) -> Vec<Arc<dyn Controller>> {
        let reconciler = HostReconciler::new(self.host.clone());
        let reports = HostReporter::new(self.host.clone(), self.versions.clone());
        let measurements = VsockMeasurements::new(self.host.clone());
        let idle = HostIdle::new(self.host.clone());

        let mut held: Vec<Arc<dyn Controller>> = vec![
            crate::controllers::cron_controller::CronController::new(
                self.host.clone(),
                self.host.cron.clone(),
            ),
            ConvergeController::new(self.host.clone(), reconciler.clone()),
            StatusController::new(self.host.clone(), reconciler, reports),
            ActivityController::new(idle.clone()),
            IdleController::new(idle),
            MeasurementController::new(HostUsage::new(self.host.clone(), measurements)),
        ];
        if self.host.telemetry.is_some() {
            held.push(TelemetryController::new(self.host.clone()));
        }
        held
    }

    pub async fn stop(&self) {
        self.host.cron_runs.shutdown().await;
        self.host.persist().await;
        HostReporter::new(self.host.clone(), self.versions.clone())
            .publish()
            .await;
    }
}
