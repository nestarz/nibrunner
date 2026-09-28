use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use protocol::{HostReportedState, HostVersions};
use tokio::sync::Mutex;

use crate::domain::report::writer;
use crate::host::Host;

#[cfg_attr(any(test, feature = "testing"), mockall::automock)]
#[async_trait]
pub trait ReportService: Send + Sync {
    async fn build(&self) -> HostReportedState;
    async fn publish(&self);
}

/// How long a host that has nothing new to say goes before saying it anyway, so that
/// `reportedAt` is a sign of life rather than only of change. The status loop builds a report
/// every second; writing each one renames the file sixty times a minute, and anything watching
/// it — a control plane, an operator's `tail` — is woken by every one of them.
const HEARTBEAT: Duration = Duration::from_secs(15);

const HEARTBEAT_MS: i64 = HEARTBEAT.as_millis() as i64;

struct Written {
    report: HostReportedState,
    at_ms: i64,
}

pub struct HostReporter {
    host: Arc<Host>,
    versions: HostVersions,
    path: PathBuf,
    last: Mutex<Option<Written>>,
}

impl HostReporter {
    pub fn new(host: Arc<Host>, versions: HostVersions) -> Arc<Self> {
        let path = writer::reported_state_file(&host);
        Arc::new(Self {
            host,
            versions,
            path,
            last: Mutex::new(None),
        })
    }
}

#[async_trait]
impl ReportService for HostReporter {
    async fn build(&self) -> HostReportedState {
        writer::build(&self.host, self.versions.clone()).await
    }

    async fn publish(&self) {
        let report = self.build().await;
        let now_ms = crate::clock::now_ms();
        let mut last = self.last.lock().await;
        if let Some(held) = last.as_ref() {
            if writer::says_the_same(&held.report, &report) && now_ms - held.at_ms < HEARTBEAT_MS {
                return;
            }
        }
        if writer::write(&self.path, &report) {
            self.host
                .metrics
                .passes
                .report_written(report.reported_at.epoch_ms());
            *last = Some(Written {
                report,
                at_ms: now_ms,
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::*;
    fn versions() -> HostVersions {
        crate::domain::report::versions::compiled_versions("v1.16.1", "6.1.180-test")
    }
    use protocol::{HostCapacity, HostState, InstanceState};

    #[tokio::test]
    async fn a_reporter_says_what_the_host_holds_and_under_which_versions() {
        let host = test_host().await;
        host.state.put_record(instance_record(|_| {})).await;
        host.state.modify(|snapshot| snapshot.converged = true).await;

        let report = reporter(&host).build().await;
        assert_eq!(report.versions, versions());
        assert_eq!(report.state, HostState::Ready);
        assert_eq!(report.instances.len(), 1);
        assert_eq!(report.instances[0].app_id, app_id());
        assert_eq!(report.instances[0].state, InstanceState::Running);
    }

    #[tokio::test]
    async fn admitted_activity_reaches_the_report_without_a_separate_activity_file() {
        let host = test_host().await;
        host.state.put_record(instance_record(|_| {})).await;
        let reporter = reporter(&host);
        assert!(reporter.build().await.instances[0].last_active_at.is_none());
        for at_ms in [1_000, 2_000] {
            assert_eq!(host.state.admit(&app_id(), at_ms, || ()).await, Some(()));
            reporter.publish().await;
            let report: HostReportedState = crate::json_store::read_json(&writer::reported_state_file(&host))
                .unwrap()
                .unwrap();
            assert_eq!(
                report.instances[0]
                    .last_active_at
                    .as_ref()
                    .map(protocol::Timestamp::epoch_ms),
                Some(at_ms)
            );
            let json = serde_json::to_value(&report.instances[0]).unwrap();
            assert_eq!(
                json["lastActiveAt"],
                protocol::Timestamp::from_epoch_ms(at_ms).as_str()
            );
        }
    }

    #[tokio::test]
    async fn a_host_that_has_not_converged_yet_reports_itself_as_registering() {
        let host = test_host().await;
        assert_eq!(reporter(&host).build().await.state, HostState::Registering);
    }

    #[tokio::test]
    async fn publishing_writes_the_report_to_the_file_the_reporter_named() {
        let host = test_host().await;
        host.state.put_record(instance_record(|_| {})).await;
        let path = writer::reported_state_file(&host);
        assert!(!path.exists());

        reporter(&host).publish().await;

        let written: HostReportedState = crate::json_store::read_json(&path)
            .unwrap()
            .expect("the report was written where the reporter named");
        assert_eq!(written.instances.len(), 1);
        assert_eq!(written.instances[0].app_id, app_id());
        assert!(
            page(&host, &written).contains(&format!(
                "nibrunner_report_written_timestamp_seconds {}\n",
                crate::domain::metrics::as_seconds(written.reported_at.epoch_ms() as u64)
            )),
            "the scrape says when the file was last written"
        );
    }

    #[tokio::test]
    async fn each_publish_replaces_the_last_rather_than_leaving_a_stale_one_behind() {
        let host = test_host().await;
        let reporter = reporter(&host);
        let path = writer::reported_state_file(&host);

        host.state.put_record(instance_record(|_| {})).await;
        reporter.publish().await;
        host.state.drop_record(&app_id()).await;
        reporter.publish().await;

        let written: HostReportedState = crate::json_store::read_json(&path).unwrap().unwrap();
        assert!(written.instances.is_empty());
    }

    #[tokio::test]
    async fn a_report_that_says_what_the_last_one_said_is_not_written_again() {
        let host = test_host().await;
        let reporter = HostReporter::new(host.arc().clone(), versions());
        let path = writer::reported_state_file(&host);

        host.state.put_record(instance_record(|_| {})).await;
        reporter.publish().await;

        std::fs::write(&path, b"what the heartbeat would replace").unwrap();
        reporter.publish().await;
        assert_eq!(
            std::fs::read_to_string(&path).unwrap(),
            "what the heartbeat would replace",
            "nothing moved, so nothing was written"
        );

        host.state.drop_record(&app_id()).await;
        reporter.publish().await;
        let written: HostReportedState = crate::json_store::read_json(&path).unwrap().unwrap();
        assert!(written.instances.is_empty(), "what moved is written at once");
    }

    #[tokio::test]
    async fn a_host_with_nothing_new_to_say_still_says_it_once_a_heartbeat() {
        let host = test_host().await;
        let reporter = HostReporter::new(host.arc().clone(), versions());
        let path = writer::reported_state_file(&host);
        reporter.publish().await;

        std::fs::write(&path, b"what the heartbeat would replace").unwrap();
        reporter
            .last
            .lock()
            .await
            .as_mut()
            .expect("one was written")
            .at_ms -= HEARTBEAT_MS;
        reporter.publish().await;

        assert!(
            crate::json_store::read_json::<HostReportedState>(&path)
                .unwrap()
                .is_some(),
            "a report is a sign of life even when it says what the last one said"
        );
    }

    #[tokio::test]
    async fn a_disk_that_filled_under_this_host_is_not_by_itself_a_report_to_write() {
        let host = test_host().await;
        let report = HostReporter::new(host.arc().clone(), versions()).build().await;
        let roomier = HostReportedState {
            allocatable: HostCapacity {
                cache_bytes: report.allocatable.cache_bytes / 2,
                ..report.allocatable
            },
            ..report.clone()
        };
        assert!(writer::says_the_same(&report, &roomier));

        let converged = HostReportedState {
            state: HostState::Ready,
            ..report.clone()
        };
        assert!(!writer::says_the_same(&report, &converged));
    }

    #[tokio::test]
    async fn a_reporter_that_cannot_write_says_so_rather_than_bringing_the_host_down() {
        let host = test_host().await;
        let path = writer::reported_state_file(&host);
        std::fs::create_dir_all(&path).unwrap();
        reporter(&host).publish().await;
        assert!(path.is_dir());
        let report = reporter(&host).build().await;
        assert!(page(&host, &report).contains("nibrunner_report_written_timestamp_seconds 0.000\n"));
    }

    fn page(host: &TestHost, report: &HostReportedState) -> String {
        crate::domain::metrics::tests::page(report, &host.metrics, &crate::state::HostSnapshot::default(), 0)
    }

    fn reporter(host: &TestHost) -> Arc<dyn ReportService> {
        HostReporter::new(host.arc().clone(), versions())
    }
}
