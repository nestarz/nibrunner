use std::collections::BTreeMap;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::time::Duration;

use protocol::{AppId, DeploymentId, HostReportedState, HostState, InstanceState};

use crate::domain::metrics::converge::{is_converged, Deploy};
use crate::domain::metrics::{as_seconds, Histogram, Kind, Metric, Page};
use crate::state::HostSnapshot;

// A pass over a quiet host is milliseconds; one that boots a fleet after a restart is the better
// part of a minute.
const BUCKET_BOUNDS_SECONDS: [f64; 15] = [
    0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0,
];

const HOST_STATES: [HostState; 4] = [
    HostState::Registering,
    HostState::Ready,
    HostState::Draining,
    HostState::Unreachable,
];

fn host_state_str(state: HostState) -> &'static str {
    match state {
        HostState::Registering => "registering",
        HostState::Ready => "ready",
        HostState::Draining => "draining",
        HostState::Unreachable => "unreachable",
    }
}

/// What set a reconcile pass going: the document moved, the last pass left work it could not
/// do yet, the daemon came up and set about the document it had cached, or the timer went off
/// on a host whose document stood still.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Trigger {
    Change,
    Deferred,
    Restart,
    Tick,
}

impl Trigger {
    pub fn as_str(self) -> &'static str {
        match self {
            Trigger::Change => "change",
            Trigger::Deferred => "deferred",
            Trigger::Restart => "restart",
            Trigger::Tick => "tick",
        }
    }
}

const TRIGGERS: [Trigger; 4] = [
    Trigger::Change,
    Trigger::Deferred,
    Trigger::Restart,
    Trigger::Tick,
];

/// How the loops that run this host are doing: how long each pass takes, when the last one
/// ran, and what has kept one from running.
#[derive(Debug)]
pub struct PassMetrics {
    started_at_ms: i64,
    reconcile: Vec<Histogram>,
    refresh: Histogram,
    last_reconcile_at_ms: AtomicI64,
    desired_unreadable: AtomicU64,
    cron_taken: AtomicU64,
    report_written_at_ms: AtomicI64,
}

impl Default for PassMetrics {
    fn default() -> Self {
        Self {
            started_at_ms: crate::clock::now_ms(),
            reconcile: TRIGGERS
                .iter()
                .map(|_| Histogram::over(&BUCKET_BOUNDS_SECONDS))
                .collect(),
            refresh: Histogram::over(&BUCKET_BOUNDS_SECONDS),
            last_reconcile_at_ms: AtomicI64::new(0),
            desired_unreadable: AtomicU64::new(0),
            cron_taken: AtomicU64::new(0),
            report_written_at_ms: AtomicI64::new(0),
        }
    }
}

impl PassMetrics {
    fn reconcile(&self, trigger: Trigger) -> &Histogram {
        &self.reconcile[TRIGGERS.iter().position(|each| *each == trigger).unwrap_or(0)]
    }

    pub fn reconciled(&self, trigger: Trigger, took: Duration, finished_at_ms: i64) {
        self.reconcile(trigger).observe(took);
        self.last_reconcile_at_ms.store(finished_at_ms, Ordering::Relaxed);
    }

    pub fn refreshed(&self, took: Duration) {
        self.refresh.observe(took);
    }

    pub fn desired_state_unreadable(&self) {
        self.desired_unreadable.fetch_add(1, Ordering::Relaxed);
    }

    pub fn cron_attempt_taken(&self) {
        self.cron_taken.fetch_add(1, Ordering::Relaxed);
    }

    pub fn report_written(&self, at_ms: i64) {
        self.report_written_at_ms.store(at_ms, Ordering::Relaxed);
    }
}

/// Whether every app this host was asked for is what it was asked for. A host nothing was
/// asked of is.
fn every_app_converged(deploys: &BTreeMap<AppId, Deploy>, report: &HostReportedState) -> bool {
    let held: BTreeMap<&AppId, (&DeploymentId, InstanceState)> = report
        .instances
        .iter()
        .map(|instance| (&instance.app_id, (&instance.deployment_id, instance.state)))
        .collect();
    deploys.iter().all(|(app_id, deploy)| {
        is_converged(
            &deploy.deployment_id,
            deploy.desired_state,
            held.get(app_id).copied(),
        )
    })
}

static BUILD_INFO: Metric = Metric {
    name: "nibrunner_build_info",
    help: "What this host runs, as labels on a 1.",
    kind: Kind::Gauge,
    labels: &["agent", "guest_image", "firecracker", "zerofs"],
};

static PROCESS_START_TIME_SECONDS: Metric = Metric {
    name: "nibrunner_process_start_time_seconds",
    help: "When this daemon started, as seconds since the epoch.",
    kind: Kind::Gauge,
    labels: &[],
};

static HOST_STATE: Metric = Metric {
    name: "nibrunner_host_state",
    help: "1 for the state the host reports itself in, 0 for every state it is not.",
    kind: Kind::Gauge,
    labels: &["state"],
};

static HOST_RECONCILED: Metric = Metric {
    name: "nibrunner_host_reconciled",
    help: "1 once a pass over the document has run to the end since this daemon started.",
    kind: Kind::Gauge,
    labels: &[],
};

static HOST_CONVERGED: Metric = Metric {
    name: "nibrunner_host_converged",
    help: "1 while every app is what its document asks for.",
    kind: Kind::Gauge,
    labels: &[],
};

static HOST_DEFERRED_WORK: Metric = Metric {
    name: "nibrunner_host_deferred_work",
    help: "1 while the last pass left work it could not do yet, such as a volume still held by an app on its way down.",
    kind: Kind::Gauge,
    labels: &[],
};

static HOST_ISOLATED: Metric = Metric {
    name: "nibrunner_host_isolated",
    help: "1 while the isolation ruleset is applied. Nothing is started or woken while it is not.",
    kind: Kind::Gauge,
    labels: &[],
};

static RECONCILE_SECONDS: Metric = Metric {
    name: "nibrunner_reconcile_seconds",
    help: "A pass from the document to the host, by what set it going.",
    kind: Kind::Histogram,
    labels: &["trigger"],
};

static RECONCILE_LAST_PASS_TIMESTAMP_SECONDS: Metric = Metric {
    name: "nibrunner_reconcile_last_pass_timestamp_seconds",
    help: "When the last pass over the document finished, as seconds since the epoch. 0 until one has.",
    kind: Kind::Gauge,
    labels: &[],
};

static REFRESH_SECONDS: Metric = Metric {
    name: "nibrunner_refresh_seconds",
    help: "A pass from the host to the record: probing what is up, settling each state, applying the ruleset and the routes, writing down what it found.",
    kind: Kind::Histogram,
    labels: &[],
};

static DESIRED_STATE_UNREADABLE_TOTAL: Metric = Metric {
    name: "nibrunner_desired_state_unreadable_total",
    help: "Times the desired state file was there and could not be read as a document.",
    kind: Kind::Counter,
    labels: &[],
};

static CRON_ATTEMPTS_TAKEN_TOTAL: Metric = Metric {
    name: "nibrunner_cron_attempts_taken_total",
    help: "Declared cron attempts not run here because their record was already there: another host, or this one before a restart, had taken them.",
    kind: Kind::Counter,
    labels: &[],
};

static REPORT_WRITTEN_TIMESTAMP_SECONDS: Metric = Metric {
    name: "nibrunner_report_written_timestamp_seconds",
    help: "When reported.json was last written, as seconds since the epoch. 0 until it has been.",
    kind: Kind::Gauge,
    labels: &[],
};

pub(super) static DECLARED: &[&Metric] = &[
    &BUILD_INFO,
    &PROCESS_START_TIME_SECONDS,
    &HOST_STATE,
    &HOST_RECONCILED,
    &HOST_CONVERGED,
    &HOST_DEFERRED_WORK,
    &HOST_ISOLATED,
    &RECONCILE_SECONDS,
    &RECONCILE_LAST_PASS_TIMESTAMP_SECONDS,
    &REFRESH_SECONDS,
    &DESIRED_STATE_UNREADABLE_TOTAL,
    &CRON_ATTEMPTS_TAKEN_TOTAL,
    &REPORT_WRITTEN_TIMESTAMP_SECONDS,
];

pub(super) fn render(
    page: &mut Page,
    report: &HostReportedState,
    metrics: &PassMetrics,
    snapshot: &HostSnapshot,
) {
    page.declare(&BUILD_INFO);
    page.value(
        &BUILD_INFO,
        &[
            ("agent", &report.versions.agent),
            ("guest_image", &report.versions.guest_image),
            ("firecracker", &report.versions.firecracker),
            ("zerofs", &report.versions.zerofs),
        ],
        1,
    );

    page.declare(&PROCESS_START_TIME_SECONDS);
    page.value(
        &PROCESS_START_TIME_SECONDS,
        &[],
        as_seconds(metrics.started_at_ms.max(0) as u64),
    );

    page.declare(&HOST_STATE);
    for state in HOST_STATES {
        page.value(
            &HOST_STATE,
            &[("state", host_state_str(state))],
            u8::from(report.state == state),
        );
    }

    page.declare(&HOST_RECONCILED);
    page.value(&HOST_RECONCILED, &[], u8::from(snapshot.converged));

    page.declare(&HOST_CONVERGED);
    page.value(
        &HOST_CONVERGED,
        &[],
        u8::from(every_app_converged(&snapshot.deploys, report)),
    );

    page.declare(&HOST_DEFERRED_WORK);
    page.value(&HOST_DEFERRED_WORK, &[], u8::from(snapshot.deferred_work));

    page.declare(&HOST_ISOLATED);
    page.value(&HOST_ISOLATED, &[], u8::from(snapshot.isolated));

    page.declare(&RECONCILE_SECONDS);
    for trigger in TRIGGERS {
        page.histogram(
            &RECONCILE_SECONDS,
            &[("trigger", trigger.as_str())],
            metrics.reconcile(trigger),
        );
    }

    page.declare(&RECONCILE_LAST_PASS_TIMESTAMP_SECONDS);
    page.value(
        &RECONCILE_LAST_PASS_TIMESTAMP_SECONDS,
        &[],
        as_seconds(metrics.last_reconcile_at_ms.load(Ordering::Relaxed).max(0) as u64),
    );

    page.declare(&REFRESH_SECONDS);
    page.histogram(&REFRESH_SECONDS, &[], &metrics.refresh);

    page.declare(&DESIRED_STATE_UNREADABLE_TOTAL);
    page.value(
        &DESIRED_STATE_UNREADABLE_TOTAL,
        &[],
        metrics.desired_unreadable.load(Ordering::Relaxed),
    );

    page.declare(&CRON_ATTEMPTS_TAKEN_TOTAL);
    page.value(
        &CRON_ATTEMPTS_TAKEN_TOTAL,
        &[],
        metrics.cron_taken.load(Ordering::Relaxed),
    );

    page.declare(&REPORT_WRITTEN_TIMESTAMP_SECONDS);
    page.value(
        &REPORT_WRITTEN_TIMESTAMP_SECONDS,
        &[],
        as_seconds(metrics.report_written_at_ms.load(Ordering::Relaxed).max(0) as u64),
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::metrics::converge::Cause;
    use crate::domain::metrics::tests::page;
    use crate::domain::metrics::HostMetrics;
    use crate::test_support::*;
    use protocol::DesiredInstanceState;

    fn lines_for<'a>(page: &'a str, name: &str) -> Vec<&'a str> {
        page.lines()
            .filter(|line| line.starts_with(&format!("{name}{{")) || line.starts_with(&format!("{name} ")))
            .collect()
    }

    fn deploy(app_id: AppId, converged: bool) -> (AppId, Deploy) {
        (
            app_id,
            Deploy {
                deployment_id: deployment_id(),
                desired_state: DesiredInstanceState::Running,
                cause: Cause::Change,
                detected_at_ms: 1_000,
                layers_ready_at_ms: None,
                volume_ready_at_ms: None,
                booted_at_ms: None,
                converged_at_ms: converged.then_some(2_000),
            },
        )
    }

    #[test]
    fn the_page_says_what_runs_here_and_whether_the_loops_have_been_round() {
        let metrics = HostMetrics::default();
        metrics
            .passes
            .reconciled(Trigger::Change, Duration::from_millis(40), 1_700_000_000_250);
        metrics.passes.refreshed(Duration::from_millis(3));
        metrics.passes.desired_state_unreadable();
        metrics.passes.report_written(1_700_000_000_500);
        let report = crate::domain::metrics::tests::report();
        let snapshot = HostSnapshot {
            converged: true,
            deferred_work: true,
            isolated: false,
            ..Default::default()
        };
        let page = page(&report, &metrics, &snapshot, 0);

        assert_eq!(
            lines_for(&page, "nibrunner_build_info"),
            vec!["nibrunner_build_info{agent=\"0.1.0\",guest_image=\"test\",firecracker=\"v1\",zerofs=\"none\"} 1"]
        );
        assert!(page.contains("nibrunner_host_state{state=\"ready\"} 1\n"));
        assert!(page.contains("nibrunner_host_state{state=\"registering\"} 0\n"));
        assert!(page.contains("nibrunner_host_reconciled 1\n"));
        assert!(page.contains("nibrunner_host_deferred_work 1\n"));
        assert!(page.contains("nibrunner_host_isolated 0\n"));
        assert!(page.contains("nibrunner_reconcile_seconds_count{trigger=\"change\"} 1\n"));
        assert!(page.contains("nibrunner_reconcile_seconds_count{trigger=\"restart\"} 0\n"));
        assert!(page.contains("nibrunner_reconcile_seconds_count{trigger=\"tick\"} 0\n"));
        assert!(page.contains("nibrunner_reconcile_last_pass_timestamp_seconds 1700000000.250\n"));
        assert!(page.contains("nibrunner_refresh_seconds_count 1\n"));
        assert!(page.contains("nibrunner_desired_state_unreadable_total 1\n"));
        assert!(page.contains("nibrunner_report_written_timestamp_seconds 1700000000.500\n"));
        let started = lines_for(&page, "nibrunner_process_start_time_seconds")[0];
        assert!(
            started.ends_with(&as_seconds(metrics.passes.started_at_ms as u64)),
            "{started}"
        );
    }

    #[test]
    fn a_host_is_converged_when_every_app_is_and_a_host_nothing_was_asked_of_is_too() {
        let metrics = HostMetrics::default();
        let mut report = crate::domain::metrics::tests::report();
        let other = AppId::parse("app-2").unwrap();
        report.instances = vec![
            reported_instance(|_| {}),
            reported_instance(|instance| {
                instance.app_id = other.clone();
                instance.state = InstanceState::Starting;
            }),
        ];

        let nothing_asked = HostSnapshot::default();
        assert!(page(&report, &metrics, &nothing_asked, 0).contains("nibrunner_host_converged 1\n"));

        let one_on_its_way = HostSnapshot {
            deploys: BTreeMap::from([deploy(app_id(), true), deploy(other.clone(), false)]),
            ..Default::default()
        };
        assert!(page(&report, &metrics, &one_on_its_way, 0).contains("nibrunner_host_converged 0\n"));

        report.instances[1].state = InstanceState::Running;
        assert!(page(&report, &metrics, &one_on_its_way, 0).contains("nibrunner_host_converged 1\n"));
    }
}
