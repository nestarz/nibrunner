use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::Duration;

use protocol::AppId;

use crate::domain::activation::SleepReason;
use crate::domain::metrics::{as_seconds, Histogram, Kind, Metric, Page};
use crate::ports::{WakeOutcome, WakeRefusal};
use crate::state::HostSnapshot;

// A restore from a snapshot answers in tens of milliseconds and a cold boot in seconds; the grace
// period a guest that never answers runs out is thirty of them.
const WAKE_BOUNDS_SECONDS: [f64; 14] = [
    0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 20.0, 30.0, 60.0, 120.0,
];

// A flush and a snapshot are each well under a second on a quiet guest; how late the policy
// fired is the idle pass's interval, and a pass over a big fleet runs late by more.
const SLEEP_BOUNDS_SECONDS: [f64; 13] = [
    0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 7.5, 10.0, 15.0, 30.0, 60.0, 120.0,
];

/// A stretch of a wake as the waker sees it: waiting for what `readyWhen` names once the microVM
/// is back, and the whole of it. Bringing the microVM back is the difference, and the VMM times
/// its own part of that on `nibrunner_vm_restore_duration_seconds`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WakePhase {
    Ready,
    Total,
}

impl WakePhase {
    pub fn as_str(self) -> &'static str {
        match self {
            WakePhase::Ready => "ready",
            WakePhase::Total => "total",
        }
    }
}

/// A stretch of putting an app to sleep: how late the policy was acted on, asking the guest to
/// flush, and the whole of the flush and the snapshot. The snapshot alone is the VMM's, on
/// `nibrunner_vm_snapshot_duration_seconds`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SleepPhase {
    Late,
    Flush,
    Total,
}

impl SleepPhase {
    pub fn as_str(self) -> &'static str {
        match self {
            SleepPhase::Late => "late",
            SleepPhase::Flush => "flush",
            SleepPhase::Total => "total",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SleepOutcome {
    Slept,
    Refused,
    Failed,
}

impl SleepOutcome {
    pub fn as_str(self) -> &'static str {
        match self {
            SleepOutcome::Slept => "slept",
            SleepOutcome::Refused => "refused",
            SleepOutcome::Failed => "failed",
        }
    }
}

/// What the HTTP activator answered a request with. A 503 from here is this host's, where one
/// through the proxy is the app's.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Answer {
    Served,
    ComeBack,
    Down,
    WouldNotStart,
    HostFull,
}

impl Answer {
    pub fn as_str(self) -> &'static str {
        match self {
            Answer::Served => "served",
            Answer::ComeBack => "come_back",
            Answer::Down => "down",
            Answer::WouldNotStart => "would_not_start",
            Answer::HostFull => "host_full",
        }
    }
}

const ANSWERS: [Answer; 5] = [
    Answer::Served,
    Answer::ComeBack,
    Answer::Down,
    Answer::WouldNotStart,
    Answer::HostFull,
];

/// Why a wake was refused, in the word the counter uses for it.
pub fn refusal_str(refusal: &WakeRefusal) -> &'static str {
    match refusal {
        WakeRefusal::NoRoom { .. } => "no_room",
        WakeRefusal::Failed { kind, .. } => kind.as_str(),
    }
}

const WAKE_PHASES: [WakePhase; 2] = [WakePhase::Ready, WakePhase::Total];
const WAKE_OUTCOMES: [WakeOutcome; 4] = [
    WakeOutcome::Restored,
    WakeOutcome::ColdBoot,
    WakeOutcome::AlreadyRunning,
    WakeOutcome::Thawed,
];
const REFUSALS: [&str; 9] = [
    "no_room",
    "host_starting",
    "not_named",
    "not_on_request",
    "not_isolated",
    "volume_unusable",
    "would_not_start",
    "never_answered",
    "abandoned",
];
const SLEEP_PHASES: [SleepPhase; 3] = [SleepPhase::Late, SleepPhase::Flush, SleepPhase::Total];
const SLEEP_REASONS: [SleepReason; 3] = [
    SleepReason::Quiet,
    SleepReason::LivedLongEnough,
    SleepReason::MemoryPressure,
];
const SLEEP_OUTCOMES: [SleepOutcome; 3] = [SleepOutcome::Slept, SleepOutcome::Refused, SleepOutcome::Failed];

/// What one app has been through, kept while the document names it.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct AppSleepWake {
    pub wakes: [u64; WAKE_OUTCOMES.len()],
    pub last_wake_ms: Option<u64>,
    pub sleeps: [u64; SLEEP_OUTCOMES.len()],
    pub last_sleep_ms: Option<u64>,
}

fn position<T: PartialEq>(of: &[T], value: &T) -> usize {
    of.iter().position(|each| each == value).unwrap_or(0)
}

/// What sleeping and waking cost, both ways: from a request finding an app asleep to the app
/// being ready for it, and from a policy saying an app may sleep to its snapshot being on disk.
#[derive(Debug)]
pub struct SleepWakeMetrics {
    // Not a label on the request histogram: this daemon forwards to a loopback port that either
    // the activator or the guest answers, and nothing comes back to say which. The waker knows,
    // and records here, so the two populations stay separable without a side channel.
    wake_wait: Histogram,
    snapshot: Histogram,
    restore: Histogram,
    wake: Vec<Histogram>,
    first_response: Histogram,
    coalesced: AtomicU64,
    refusals: Vec<AtomicU64>,
    answers: [AtomicU64; ANSWERS.len()],
    sleep: Vec<Histogram>,
    sleep_outcomes: Vec<AtomicU64>,
    apps: Mutex<BTreeMap<AppId, AppSleepWake>>,
}

impl Default for SleepWakeMetrics {
    fn default() -> Self {
        Self {
            wake_wait: Histogram::over(&WAKE_BOUNDS_SECONDS),
            snapshot: Histogram::over(&SLEEP_BOUNDS_SECONDS),
            restore: Histogram::over(&WAKE_BOUNDS_SECONDS),
            wake: (0..WAKE_PHASES.len() * WAKE_OUTCOMES.len())
                .map(|_| Histogram::over(&WAKE_BOUNDS_SECONDS))
                .collect(),
            first_response: Histogram::over(&WAKE_BOUNDS_SECONDS),
            coalesced: AtomicU64::new(0),
            refusals: REFUSALS.iter().map(|_| AtomicU64::new(0)).collect(),
            answers: Default::default(),
            sleep: (0..SLEEP_PHASES.len() * SLEEP_REASONS.len())
                .map(|_| Histogram::over(&SLEEP_BOUNDS_SECONDS))
                .collect(),
            sleep_outcomes: (0..SLEEP_REASONS.len() * SLEEP_OUTCOMES.len())
                .map(|_| AtomicU64::new(0))
                .collect(),
            apps: Mutex::new(BTreeMap::new()),
        }
    }
}

impl SleepWakeMetrics {
    fn wake(&self, phase: WakePhase, outcome: WakeOutcome) -> &Histogram {
        &self.wake[position(&WAKE_PHASES, &phase) * WAKE_OUTCOMES.len() + position(&WAKE_OUTCOMES, &outcome)]
    }

    fn sleep(&self, phase: SleepPhase, reason: SleepReason) -> &Histogram {
        &self.sleep[position(&SLEEP_PHASES, &phase) * SLEEP_REASONS.len() + position(&SLEEP_REASONS, &reason)]
    }

    fn sleep_outcome(&self, reason: SleepReason, outcome: SleepOutcome) -> &AtomicU64 {
        &self.sleep_outcomes
            [position(&SLEEP_REASONS, &reason) * SLEEP_OUTCOMES.len() + position(&SLEEP_OUTCOMES, &outcome)]
    }

    fn app(&self, app_id: &AppId, change: impl FnOnce(&mut AppSleepWake)) {
        let mut apps = self.apps.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        change(apps.entry(app_id.clone()).or_default());
    }

    /// One wake, once the app is ready: how long the whole took, and how much of that was
    /// waiting for what readyWhen names after the microVM was back.
    pub fn woken(&self, app_id: &AppId, outcome: WakeOutcome, total: Duration, ready: Duration) {
        self.wake(WakePhase::Ready, outcome).observe(ready);
        self.wake(WakePhase::Total, outcome).observe(total);
        self.app(app_id, |app| {
            app.wakes[position(&WAKE_OUTCOMES, &outcome)] += 1;
            app.last_wake_ms = Some(total.as_millis() as u64);
        });
    }

    pub fn snapshotted(&self, took: Duration) {
        self.snapshot.observe(took);
    }

    pub fn restored(&self, took: Duration) {
        self.restore.observe(took);
    }

    pub fn wake_refused(&self, refusal: &WakeRefusal) {
        self.refusals[position(&REFUSALS, &refusal_str(refusal))].fetch_add(1, Ordering::Relaxed);
    }

    /// A request waited this long to be given its app, whether it set the wake going or found
    /// one under way and joined it.
    pub fn woke(&self, took: Duration, joined: bool) {
        self.wake_wait.observe(took);
        if joined {
            self.coalesced.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn first_response(&self, took: Duration) {
        self.first_response.observe(took);
    }

    pub fn answered(&self, answer: Answer) {
        self.answers[position(&ANSWERS, &answer)].fetch_add(1, Ordering::Relaxed);
    }

    pub fn sleep_due(&self, reason: SleepReason, late: Duration) {
        self.sleep(SleepPhase::Late, reason).observe(late);
    }

    pub fn slept(
        &self,
        app_id: &AppId,
        reason: SleepReason,
        outcome: SleepOutcome,
        flush: Duration,
        snapshot: Duration,
    ) {
        self.sleep_outcome(reason, outcome)
            .fetch_add(1, Ordering::Relaxed);
        if outcome == SleepOutcome::Slept {
            self.sleep(SleepPhase::Flush, reason).observe(flush);
            self.sleep(SleepPhase::Total, reason).observe(flush + snapshot);
        }
        self.app(app_id, |app| {
            app.sleeps[position(&SLEEP_OUTCOMES, &outcome)] += 1;
            if outcome == SleepOutcome::Slept {
                app.last_sleep_ms = Some((flush + snapshot).as_millis() as u64);
            }
        });
    }

    pub fn forget(&self, app_id: &AppId) {
        self.apps
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(app_id);
    }

    pub fn of(&self, app_id: &AppId) -> AppSleepWake {
        self.apps
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(app_id)
            .cloned()
            .unwrap_or_default()
    }
}

static WAKE_DURATION_SECONDS: Metric = Metric {
    name: "nibrunner_wake_duration_seconds",
    help: "Bringing a sleeping app back for the request that asked for it, from the request reaching the waker to the app being ready to be forwarded one, whether it set the wake going or joined one under way. On an on-request host this is the population that makes the tail of nibrunner_proxy_request_duration_seconds.",
    kind: Kind::Histogram,
    labels: &[],
};

static VM_SNAPSHOT_DURATION_SECONDS: Metric = Metric {
    name: "nibrunner_vm_snapshot_duration_seconds",
    help: "Pausing a guest that has gone quiet and writing its memory out. Scales with the memory the app was given, so it is the cost of the sleep rather than of the app.",
    kind: Kind::Histogram,
    labels: &[],
};

static VM_RESTORE_DURATION_SECONDS: Metric = Metric {
    name: "nibrunner_vm_restore_duration_seconds",
    help: "Bringing a guest back from the snapshot it slept in. The other half of the sleep, and cheaper than it by orders of magnitude.",
    kind: Kind::Histogram,
    labels: &[],
};

static WAKE_PHASE_SECONDS: Metric = Metric {
    name: "nibrunner_wake_phase_seconds",
    help: "One wake, by how it was done and the stretch of it: waiting for what readyWhen names once the microVM was back, and the whole. Total less ready is bringing the microVM back; the VMM's own part of that is nibrunner_vm_restore_duration_seconds.",
    kind: Kind::Histogram,
    labels: &["phase", "outcome"],
};

static WAKE_REQUESTS_COALESCED_TOTAL: Metric = Metric {
    name: "nibrunner_wake_requests_coalesced_total",
    help: "Requests that found a wake of their app already under way and waited on it.",
    kind: Kind::Counter,
    labels: &[],
};

static WAKE_FIRST_RESPONSE_SECONDS: Metric = Metric {
    name: "nibrunner_wake_first_response_seconds",
    help: "The request that woke an app, from being handed to the app to the app answering it. What being ready was worth.",
    kind: Kind::Histogram,
    labels: &[],
};

static WAKE_REFUSALS_TOTAL: Metric = Metric {
    name: "nibrunner_wake_refusals_total",
    help: "Wakes this host refused, by why.",
    kind: Kind::Counter,
    labels: &["reason"],
};

static ACTIVATOR_RESPONSES_TOTAL: Metric = Metric {
    name: "nibrunner_activator_responses_total",
    help: "What the HTTP activator answered requests for on-request apps with. The proxy counts every one of these as served, because serving it is what it did.",
    kind: Kind::Counter,
    labels: &["answer"],
};

static SLEEP_PHASE_SECONDS: Metric = Metric {
    name: "nibrunner_sleep_phase_seconds",
    help: "Putting an app to sleep, by why and the stretch of it: how late the policy was acted on past what it allowed, asking the guest to flush, and the whole of the flush and the snapshot. The snapshot alone is nibrunner_vm_snapshot_duration_seconds.",
    kind: Kind::Histogram,
    labels: &["phase", "reason"],
};

static SLEEP_OUTCOMES_TOTAL: Metric = Metric {
    name: "nibrunner_sleep_outcomes_total",
    help: "Times a policy said an app may sleep, by what came of it: it slept, its microVM refused to be snapshotted, or the snapshot failed.",
    kind: Kind::Counter,
    labels: &["reason", "outcome"],
};

static APP_WAKES_TOTAL: Metric = Metric {
    name: "nibrunner_app_wakes_total",
    help: "Times an on-request app was woken by a request, by how.",
    kind: Kind::Counter,
    labels: &["app", "outcome"],
};

static APP_LAST_WAKE_SECONDS: Metric = Metric {
    name: "nibrunner_app_last_wake_seconds",
    help: "What the last wake of an on-request app took. Absent until it has been woken.",
    kind: Kind::Gauge,
    labels: &["app"],
};

static APP_SLEEPS_TOTAL: Metric = Metric {
    name: "nibrunner_app_sleeps_total",
    help: "Times a policy said an on-request app may sleep, by what came of it.",
    kind: Kind::Counter,
    labels: &["app", "outcome"],
};

static APP_LAST_SLEEP_SECONDS: Metric = Metric {
    name: "nibrunner_app_last_sleep_seconds",
    help: "What the last sleep of an on-request app took, flush and snapshot. Absent until it has slept.",
    kind: Kind::Gauge,
    labels: &["app"],
};

pub(super) static DECLARED: &[&Metric] = &[
    &WAKE_DURATION_SECONDS,
    &VM_SNAPSHOT_DURATION_SECONDS,
    &VM_RESTORE_DURATION_SECONDS,
    &WAKE_PHASE_SECONDS,
    &WAKE_REQUESTS_COALESCED_TOTAL,
    &WAKE_FIRST_RESPONSE_SECONDS,
    &WAKE_REFUSALS_TOTAL,
    &ACTIVATOR_RESPONSES_TOTAL,
    &SLEEP_PHASE_SECONDS,
    &SLEEP_OUTCOMES_TOTAL,
    &APP_WAKES_TOTAL,
    &APP_LAST_WAKE_SECONDS,
    &APP_SLEEPS_TOTAL,
    &APP_LAST_SLEEP_SECONDS,
];

pub(super) fn render(page: &mut Page, metrics: &SleepWakeMetrics, snapshot: &HostSnapshot) {
    page.declare(&WAKE_DURATION_SECONDS);
    page.histogram(&WAKE_DURATION_SECONDS, &[], &metrics.wake_wait);

    page.declare(&VM_SNAPSHOT_DURATION_SECONDS);
    page.histogram(&VM_SNAPSHOT_DURATION_SECONDS, &[], &metrics.snapshot);

    page.declare(&VM_RESTORE_DURATION_SECONDS);
    page.histogram(&VM_RESTORE_DURATION_SECONDS, &[], &metrics.restore);

    page.declare(&WAKE_PHASE_SECONDS);
    for phase in WAKE_PHASES {
        for outcome in WAKE_OUTCOMES {
            page.histogram(
                &WAKE_PHASE_SECONDS,
                &[("phase", phase.as_str()), ("outcome", outcome.as_str())],
                metrics.wake(phase, outcome),
            );
        }
    }

    page.declare(&WAKE_REQUESTS_COALESCED_TOTAL);
    page.value(
        &WAKE_REQUESTS_COALESCED_TOTAL,
        &[],
        metrics.coalesced.load(Ordering::Relaxed),
    );

    page.declare(&WAKE_FIRST_RESPONSE_SECONDS);
    page.histogram(&WAKE_FIRST_RESPONSE_SECONDS, &[], &metrics.first_response);

    page.declare(&WAKE_REFUSALS_TOTAL);
    for (index, reason) in REFUSALS.iter().enumerate() {
        page.value(
            &WAKE_REFUSALS_TOTAL,
            &[("reason", reason)],
            metrics.refusals[index].load(Ordering::Relaxed),
        );
    }

    page.declare(&ACTIVATOR_RESPONSES_TOTAL);
    for (index, answer) in ANSWERS.iter().enumerate() {
        page.value(
            &ACTIVATOR_RESPONSES_TOTAL,
            &[("answer", answer.as_str())],
            metrics.answers[index].load(Ordering::Relaxed),
        );
    }

    page.declare(&SLEEP_PHASE_SECONDS);
    for phase in SLEEP_PHASES {
        for reason in SLEEP_REASONS {
            page.histogram(
                &SLEEP_PHASE_SECONDS,
                &[("phase", phase.as_str()), ("reason", reason.as_str())],
                metrics.sleep(phase, reason),
            );
        }
    }

    page.declare(&SLEEP_OUTCOMES_TOTAL);
    for reason in SLEEP_REASONS {
        for outcome in SLEEP_OUTCOMES {
            page.value(
                &SLEEP_OUTCOMES_TOTAL,
                &[("reason", reason.as_str()), ("outcome", outcome.as_str())],
                metrics.sleep_outcome(reason, outcome).load(Ordering::Relaxed),
            );
        }
    }

    let on_request: Vec<&AppId> = snapshot
        .records
        .values()
        .filter(|record| record.on_request)
        .map(|record| &record.app_id)
        .collect();

    page.declare(&APP_WAKES_TOTAL);
    for app_id in &on_request {
        let app = metrics.of(app_id);
        for (index, outcome) in WAKE_OUTCOMES.iter().enumerate() {
            page.value(
                &APP_WAKES_TOTAL,
                &[("app", app_id.as_str()), ("outcome", outcome.as_str())],
                app.wakes[index],
            );
        }
    }

    page.declare(&APP_LAST_WAKE_SECONDS);
    for app_id in &on_request {
        if let Some(ms) = metrics.of(app_id).last_wake_ms {
            page.value(
                &APP_LAST_WAKE_SECONDS,
                &[("app", app_id.as_str())],
                as_seconds(ms),
            );
        }
    }

    page.declare(&APP_SLEEPS_TOTAL);
    for app_id in &on_request {
        let app = metrics.of(app_id);
        for (index, outcome) in SLEEP_OUTCOMES.iter().enumerate() {
            page.value(
                &APP_SLEEPS_TOTAL,
                &[("app", app_id.as_str()), ("outcome", outcome.as_str())],
                app.sleeps[index],
            );
        }
    }

    page.declare(&APP_LAST_SLEEP_SECONDS);
    for app_id in &on_request {
        if let Some(ms) = metrics.of(app_id).last_sleep_ms {
            page.value(
                &APP_LAST_SLEEP_SECONDS,
                &[("app", app_id.as_str())],
                as_seconds(ms),
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::metrics::tests::page;
    use crate::ports::WakeFailure;
    use crate::test_support::*;

    fn lines_for<'a>(page: &'a str, name: &str) -> Vec<&'a str> {
        page.lines()
            .filter(|line| line.starts_with(&format!("{name}{{")) || line.starts_with(&format!("{name} ")))
            .collect()
    }

    #[test]
    fn every_reason_a_wake_is_refused_for_has_a_counter_of_its_own() {
        let metrics = SleepWakeMetrics::default();
        metrics.wake_refused(&WakeRefusal::NoRoom { shortfall_mib: 64 });
        for kind in [
            WakeFailure::HostStarting,
            WakeFailure::NotNamed,
            WakeFailure::NotOnRequest,
            WakeFailure::NotIsolated,
            WakeFailure::VolumeUnusable,
            WakeFailure::WouldNotStart,
            WakeFailure::NeverAnswered,
            WakeFailure::Abandoned,
        ] {
            metrics.wake_refused(&WakeRefusal::Failed {
                kind,
                reason: String::new(),
            });
        }
        assert!(metrics
            .refusals
            .iter()
            .all(|count| count.load(Ordering::Relaxed) == 1));
    }

    #[tokio::test]
    async fn what_an_on_request_app_has_been_through_is_on_its_own_series_and_a_running_apps_is_not() {
        let host = test_host().await;
        let other = AppId::parse("app-2").unwrap();
        host.state
            .put_record(instance_record(|record| record.on_request = true))
            .await;
        host.state
            .put_record(instance_record(|record| record.app_id = other.clone()))
            .await;
        let metrics = &host.metrics.sleep_wake;
        metrics.woken(
            &app_id(),
            WakeOutcome::Restored,
            Duration::from_millis(36),
            Duration::from_millis(6),
        );
        metrics.woken(
            &other,
            WakeOutcome::ColdBoot,
            Duration::from_secs(1),
            Duration::from_secs(1),
        );
        metrics.slept(
            &app_id(),
            SleepReason::Quiet,
            SleepOutcome::Refused,
            Duration::ZERO,
            Duration::ZERO,
        );
        metrics.slept(
            &app_id(),
            SleepReason::Quiet,
            SleepOutcome::Slept,
            Duration::from_millis(200),
            Duration::from_millis(400),
        );
        metrics.sleep_due(SleepReason::LivedLongEnough, Duration::from_secs(3));
        metrics.woke(Duration::from_millis(36), false);
        metrics.woke(Duration::from_millis(20), true);
        metrics.snapshotted(Duration::from_millis(400));
        metrics.restored(Duration::from_millis(8));
        metrics.first_response(Duration::from_millis(9));
        metrics.answered(Answer::ComeBack);

        let page = page(
            &crate::domain::metrics::tests::report(),
            &host.metrics,
            &host.state.snapshot().await,
            0,
        );
        assert_eq!(
            lines_for(&page, "nibrunner_app_wakes_total"),
            vec![
                "nibrunner_app_wakes_total{app=\"app-1\",outcome=\"restored\"} 1",
                "nibrunner_app_wakes_total{app=\"app-1\",outcome=\"cold-boot\"} 0",
                "nibrunner_app_wakes_total{app=\"app-1\",outcome=\"already-running\"} 0",
                "nibrunner_app_wakes_total{app=\"app-1\",outcome=\"thawed\"} 0",
            ]
        );
        assert_eq!(
            lines_for(&page, "nibrunner_app_last_wake_seconds"),
            vec!["nibrunner_app_last_wake_seconds{app=\"app-1\"} 0.036"]
        );
        assert_eq!(
            lines_for(&page, "nibrunner_app_sleeps_total"),
            vec![
                "nibrunner_app_sleeps_total{app=\"app-1\",outcome=\"slept\"} 1",
                "nibrunner_app_sleeps_total{app=\"app-1\",outcome=\"refused\"} 1",
                "nibrunner_app_sleeps_total{app=\"app-1\",outcome=\"failed\"} 0",
            ]
        );
        assert_eq!(
            lines_for(&page, "nibrunner_app_last_sleep_seconds"),
            vec!["nibrunner_app_last_sleep_seconds{app=\"app-1\"} 0.600"]
        );
        assert!(
            page.contains("nibrunner_wake_phase_seconds_count{phase=\"total\",outcome=\"cold-boot\"} 1\n")
        );
        assert!(
            page.contains("nibrunner_wake_phase_seconds_sum{phase=\"ready\",outcome=\"restored\"} 0.006\n")
        );
        assert!(page.contains("nibrunner_wake_duration_seconds_count 2\n"));
        assert!(page.contains("nibrunner_vm_snapshot_duration_seconds_sum 0.4\n"));
        assert!(page.contains("nibrunner_vm_restore_duration_seconds_sum 0.008\n"));
        assert!(page.contains("nibrunner_wake_requests_coalesced_total 1\n"));
        assert!(page.contains("nibrunner_wake_first_response_seconds_count 1\n"));
        assert!(
            page.contains("nibrunner_sleep_phase_seconds_count{phase=\"late\",reason=\"max-lifetime\"} 1\n")
        );
        assert!(page.contains("nibrunner_sleep_phase_seconds_count{phase=\"total\",reason=\"idle\"} 1\n"));
        assert!(page.contains("nibrunner_sleep_phase_seconds_count{phase=\"flush\",reason=\"idle\"} 1\n"));
        assert!(page.contains("nibrunner_sleep_outcomes_total{reason=\"idle\",outcome=\"refused\"} 1\n"));
        assert!(page.contains("nibrunner_wake_refusals_total{reason=\"no_room\"} 0\n"));
        assert!(page.contains("nibrunner_activator_responses_total{answer=\"come_back\"} 1\n"));
        assert!(page.contains("nibrunner_activator_responses_total{answer=\"served\"} 0\n"));
    }

    #[test]
    fn an_app_the_document_dropped_is_forgotten() {
        let metrics = SleepWakeMetrics::default();
        metrics.woken(&app_id(), WakeOutcome::Restored, Duration::ZERO, Duration::ZERO);
        assert_eq!(metrics.of(&app_id()).wakes[0], 1);
        metrics.forget(&app_id());
        assert_eq!(metrics.of(&app_id()), AppSleepWake::default());
    }
}
