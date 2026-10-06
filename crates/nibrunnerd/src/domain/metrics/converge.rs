use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use protocol::{AppId, DeploymentId, DesiredInstanceState, HostReportedState, InstanceState, Timestamp};

use crate::desired::Changes;
use crate::domain::metrics::{as_seconds, Histogram, Kind, Metric, Page};
use crate::host::Host;
use crate::state::HostState;

// From the tenth of a second a warm boot answers in to the minutes a first pull of a large layer
// over a slow link can take.
const BUCKET_BOUNDS_SECONDS: [f64; 13] = [
    0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 20.0, 30.0, 60.0, 120.0, 300.0, 600.0,
];

/// Why this host set out to give an app what its document asks for: the document changed under
/// it, or the daemon came back up and set about what the last document asked.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Cause {
    Change,
    Restart,
}

impl Cause {
    pub fn as_str(self) -> &'static str {
        match self {
            Cause::Change => "change",
            Cause::Restart => "restart",
        }
    }
}

/// A stretch of the work, in the order the pass does it. Each is measured from the end of the
/// one before it, so the ones an ask passed through add up to its total.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Phase {
    Layers,
    Volume,
    Boot,
    Ready,
    Total,
}

impl Phase {
    pub fn as_str(self) -> &'static str {
        match self {
            Phase::Layers => "layers",
            Phase::Volume => "volume",
            Phase::Boot => "boot",
            Phase::Ready => "ready",
            Phase::Total => "total",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    Converged,
    Superseded,
}

impl Outcome {
    pub fn as_str(self) -> &'static str {
        match self {
            Outcome::Converged => "converged",
            Outcome::Superseded => "superseded",
        }
    }
}

const CAUSES: [Cause; 2] = [Cause::Change, Cause::Restart];
const PHASES: [Phase; 5] = [
    Phase::Layers,
    Phase::Volume,
    Phase::Boot,
    Phase::Ready,
    Phase::Total,
];
const DESIRED_STATES: [DesiredInstanceState; 3] = [
    DesiredInstanceState::Running,
    DesiredInstanceState::OnRequest,
    DesiredInstanceState::Stopped,
];
const OUTCOMES: [Outcome; 2] = [Outcome::Converged, Outcome::Superseded];

/// One ask for one app, from the moment this host noticed it to the moment the app was what it
/// asked for. It stays behind as the last ask until the next one replaces it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Deploy {
    pub deployment_id: DeploymentId,
    pub desired_state: DesiredInstanceState,
    pub cause: Cause,
    pub detected_at_ms: i64,
    pub layers_ready_at_ms: Option<i64>,
    pub volume_ready_at_ms: Option<i64>,
    pub booted_at_ms: Option<i64>,
    pub converged_at_ms: Option<i64>,
}

impl Deploy {
    pub fn is_open(&self) -> bool {
        self.converged_at_ms.is_none()
    }

    /// Each stretch the ask passed through, in milliseconds. One whose milestone was never
    /// reached — a stop boots nothing — is left out rather than reported as nothing, and a
    /// milestone from before the ask was noticed belongs to an earlier one.
    pub fn phases(&self) -> Vec<(Phase, u64)> {
        let Some(converged) = self.converged_at_ms else {
            return Vec::new();
        };
        let mut phases = Vec::new();
        let mut reached = self.detected_at_ms;
        let mut milestone = |phase: Phase, at: Option<i64>| -> bool {
            let Some(at) = at.filter(|at| *at >= reached) else {
                return false;
            };
            phases.push((phase, (at - reached) as u64));
            reached = at;
            true
        };
        milestone(Phase::Layers, self.layers_ready_at_ms);
        milestone(Phase::Volume, self.volume_ready_at_ms);
        if milestone(Phase::Boot, self.booted_at_ms) {
            milestone(Phase::Ready, Some(converged));
        }
        phases.push((Phase::Total, (converged - self.detected_at_ms).max(0) as u64));
        phases
    }
}

/// Whether what the host holds for an app is what the document asks for it. What that means is
/// the desired state's to say: a running app answers, an on-request one is there to be asked
/// for, and a stopped one is down — whichever deployment was down before.
pub fn is_converged(
    deployment_id: &DeploymentId,
    desired_state: DesiredInstanceState,
    held: Option<(&DeploymentId, InstanceState)>,
) -> bool {
    match desired_state {
        DesiredInstanceState::Stopped => held.is_none_or(|(_, state)| state == InstanceState::Stopped),
        DesiredInstanceState::Running => {
            held.is_some_and(|(held, state)| held == deployment_id && state == InstanceState::Running)
        }
        DesiredInstanceState::OnRequest => held.is_some_and(|(held, state)| {
            held == deployment_id
                && matches!(
                    state,
                    InstanceState::Idle | InstanceState::Frozen | InstanceState::Running
                )
        }),
    }
}

#[derive(Debug)]
pub struct ConvergeMetrics {
    took: Vec<Histogram>,
    outcomes: Vec<AtomicU64>,
}

impl Default for ConvergeMetrics {
    fn default() -> Self {
        Self {
            took: (0..PHASES.len() * DESIRED_STATES.len() * CAUSES.len())
                .map(|_| Histogram::over(&BUCKET_BOUNDS_SECONDS))
                .collect(),
            outcomes: (0..DESIRED_STATES.len() * CAUSES.len() * OUTCOMES.len())
                .map(|_| AtomicU64::new(0))
                .collect(),
        }
    }
}

fn position<T: PartialEq>(of: &[T], value: &T) -> usize {
    of.iter().position(|each| each == value).unwrap_or(0)
}

impl ConvergeMetrics {
    fn took(&self, phase: Phase, desired_state: DesiredInstanceState, cause: Cause) -> &Histogram {
        let index = (position(&PHASES, &phase) * DESIRED_STATES.len()
            + position(&DESIRED_STATES, &desired_state))
            * CAUSES.len()
            + position(&CAUSES, &cause);
        &self.took[index]
    }

    fn outcome(&self, desired_state: DesiredInstanceState, cause: Cause, outcome: Outcome) -> &AtomicU64 {
        let index = (position(&DESIRED_STATES, &desired_state) * CAUSES.len() + position(&CAUSES, &cause))
            * OUTCOMES.len()
            + position(&OUTCOMES, &outcome);
        &self.outcomes[index]
    }

    pub fn closed(&self, deploy: &Deploy, outcome: Outcome) {
        self.outcome(deploy.desired_state, deploy.cause, outcome)
            .fetch_add(1, Ordering::Relaxed);
        for (phase, ms) in deploy.phases() {
            self.took(phase, deploy.desired_state, deploy.cause)
                .observe(Duration::from_millis(ms));
        }
    }
}

/// The document changed, or came back after a restart: every app whose ask moved starts a new
/// deploy now, and an ask still under way for it is over without having got there.
pub async fn detected(host: &Host, changes: &Changes, cause: Cause, now_ms: i64) {
    host.state
        .modify(|snapshot| {
            for wanted in &changes.wanted {
                let started = Deploy {
                    deployment_id: wanted.deployment_id.clone(),
                    desired_state: wanted.desired_state,
                    cause,
                    detected_at_ms: now_ms,
                    layers_ready_at_ms: None,
                    volume_ready_at_ms: None,
                    booted_at_ms: None,
                    converged_at_ms: None,
                };
                if let Some(open) = snapshot
                    .deploys
                    .insert(wanted.app_id.clone(), started)
                    .filter(Deploy::is_open)
                {
                    host.metrics.converge.closed(&open, Outcome::Superseded);
                }
                // What a restart converges on is what the record already says it reached.
                if cause == Cause::Change {
                    if let Some(record) = snapshot.records.get_mut(&wanted.app_id) {
                        record.converged_at = None;
                    }
                }
            }
            for gone in &changes.gone {
                if let Some(open) = snapshot.deploys.remove(gone).filter(Deploy::is_open) {
                    host.metrics.converge.closed(&open, Outcome::Superseded);
                }
                host.metrics.forget(gone);
            }
        })
        .await;
}

/// A milestone on the way, noted against the ask under way for the app, if there is one. The
/// latest one wins: an app that was started twice on one ask booted when it last did.
pub async fn stamp(state: &HostState, app_id: &AppId, milestone: impl FnOnce(&mut Deploy)) {
    state
        .modify(|snapshot| {
            if let Some(deploy) = snapshot.deploys.get_mut(app_id).filter(|deploy| deploy.is_open()) {
                milestone(deploy);
            }
        })
        .await;
}

/// Closes every ask whose app is now what it asked for. Run after anything that can move an
/// instance's state, so what is measured is when it got there and not when someone looked.
pub async fn observe(host: &Host, now_ms: i64) {
    host.state
        .modify(|snapshot| {
            let records = &mut snapshot.records;
            for (app_id, deploy) in snapshot.deploys.iter_mut().filter(|(_, deploy)| deploy.is_open()) {
                let record = records.get_mut(app_id);
                let held = record
                    .as_ref()
                    .map(|record| (&record.deployment_id, record.state));
                if !is_converged(&deploy.deployment_id, deploy.desired_state, held) {
                    continue;
                }
                deploy.converged_at_ms = Some(now_ms);
                if let Some(record) = record {
                    record
                        .converged_at
                        .get_or_insert(Timestamp::from_epoch_ms(now_ms));
                }
                host.metrics.converge.closed(deploy, Outcome::Converged);
                tracing::info!(
                    %app_id,
                    desired_state = deploy.desired_state.as_str(),
                    cause = deploy.cause.as_str(),
                    took_ms = now_ms - deploy.detected_at_ms,
                    "app is what its document asks for"
                );
            }
        })
        .await;
}

static CONVERGE_SECONDS: Metric = Metric {
    name: "nibrunner_converge_seconds",
    help: "How long giving an app what its document asked for took, from the change being noticed, by the stretch of the work: fetching its layers, readying its volume, booting it, and waiting for it to be ready. Total is the whole of it.",
    kind: Kind::Histogram,
    labels: &["phase", "desired_state", "cause"],
};

static CONVERGE_OUTCOMES_TOTAL: Metric = Metric {
    name: "nibrunner_converge_outcomes_total",
    help:
        "Asks this host set out on, by how they ended: the app got there, or the next ask for it came first.",
    kind: Kind::Counter,
    labels: &["desired_state", "cause", "outcome"],
};

static APP_CONVERGED: Metric = Metric {
    name: "nibrunner_app_converged",
    help: "1 while an app is what its document asks for, 0 while it is not.",
    kind: Kind::Gauge,
    labels: &["app"],
};

static APP_CONVERGING_SECONDS: Metric = Metric {
    name: "nibrunner_app_converging_seconds",
    help: "How long an app has been on its way to what its document asks for. 0 once it got there.",
    kind: Kind::Gauge,
    labels: &["app"],
};

static APP_LAST_CONVERGE_SECONDS: Metric = Metric {
    name: "nibrunner_app_last_converge_seconds",
    help: "What the last ask for an app took, by the stretch of the work. Absent while one is under way.",
    kind: Kind::Gauge,
    labels: &["app", "phase"],
};

pub(super) static DECLARED: &[&Metric] = &[
    &CONVERGE_SECONDS,
    &CONVERGE_OUTCOMES_TOTAL,
    &APP_CONVERGED,
    &APP_CONVERGING_SECONDS,
    &APP_LAST_CONVERGE_SECONDS,
];

pub(super) fn render(
    page: &mut Page,
    report: &HostReportedState,
    metrics: &ConvergeMetrics,
    deploys: &BTreeMap<AppId, Deploy>,
    now_ms: i64,
) {
    page.declare(&CONVERGE_SECONDS);
    for phase in PHASES {
        for desired_state in DESIRED_STATES {
            for cause in CAUSES {
                page.histogram(
                    &CONVERGE_SECONDS,
                    &[
                        ("phase", phase.as_str()),
                        ("desired_state", desired_state.as_str()),
                        ("cause", cause.as_str()),
                    ],
                    metrics.took(phase, desired_state, cause),
                );
            }
        }
    }

    page.declare(&CONVERGE_OUTCOMES_TOTAL);
    for desired_state in DESIRED_STATES {
        for cause in CAUSES {
            for outcome in OUTCOMES {
                page.value(
                    &CONVERGE_OUTCOMES_TOTAL,
                    &[
                        ("desired_state", desired_state.as_str()),
                        ("cause", cause.as_str()),
                        ("outcome", outcome.as_str()),
                    ],
                    metrics
                        .outcome(desired_state, cause, outcome)
                        .load(Ordering::Relaxed),
                );
            }
        }
    }

    let held: BTreeMap<&AppId, (&DeploymentId, InstanceState)> = report
        .instances
        .iter()
        .map(|instance| (&instance.app_id, (&instance.deployment_id, instance.state)))
        .collect();
    page.declare(&APP_CONVERGED);
    for (app_id, deploy) in deploys {
        page.value(
            &APP_CONVERGED,
            &[("app", app_id.as_str())],
            u8::from(is_converged(
                &deploy.deployment_id,
                deploy.desired_state,
                held.get(app_id).copied(),
            )),
        );
    }

    page.declare(&APP_CONVERGING_SECONDS);
    for (app_id, deploy) in deploys {
        let waiting = if deploy.is_open() {
            (now_ms - deploy.detected_at_ms).max(0) as u64
        } else {
            0
        };
        page.value(
            &APP_CONVERGING_SECONDS,
            &[("app", app_id.as_str())],
            as_seconds(waiting),
        );
    }

    page.declare(&APP_LAST_CONVERGE_SECONDS);
    for (app_id, deploy) in deploys {
        for (phase, ms) in deploy.phases() {
            page.value(
                &APP_LAST_CONVERGE_SECONDS,
                &[("app", app_id.as_str()), ("phase", phase.as_str())],
                as_seconds(ms),
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapters::vm::VmStatus;
    use crate::domain::metrics::tests::page;
    use crate::domain::metrics::HostMetrics;
    use crate::state::HostSnapshot;
    use crate::test_support::*;
    use protocol::{ActivationPolicy, HealthCheck, SleepPolicy};

    const DETECTED: i64 = 1_000_000;

    fn deploy(edit: impl FnOnce(&mut Deploy)) -> Deploy {
        let mut value = Deploy {
            deployment_id: deployment_id(),
            desired_state: DesiredInstanceState::Running,
            cause: Cause::Change,
            detected_at_ms: DETECTED,
            layers_ready_at_ms: None,
            volume_ready_at_ms: None,
            booted_at_ms: None,
            converged_at_ms: None,
        };
        edit(&mut value);
        value
    }

    fn other() -> AppId {
        AppId::parse("app-2").unwrap()
    }

    fn changes(wanted: Vec<protocol::DesiredInstance>, gone: Vec<AppId>) -> Changes {
        Changes { wanted, gone }
    }

    fn lines_for<'a>(page: &'a str, name: &str) -> Vec<&'a str> {
        page.lines()
            .filter(|line| line.starts_with(&format!("{name}{{")) || line.starts_with(&format!("{name} ")))
            .collect()
    }

    #[test]
    fn the_phases_an_ask_passed_through_add_up_to_its_total() {
        let whole = deploy(|deploy| {
            deploy.layers_ready_at_ms = Some(DETECTED + 4_000);
            deploy.volume_ready_at_ms = Some(DETECTED + 9_000);
            deploy.booted_at_ms = Some(DETECTED + 9_500);
            deploy.converged_at_ms = Some(DETECTED + 11_000);
        });
        assert_eq!(
            whole.phases(),
            vec![
                (Phase::Layers, 4_000),
                (Phase::Volume, 5_000),
                (Phase::Boot, 500),
                (Phase::Ready, 1_500),
                (Phase::Total, 11_000),
            ]
        );
        let stretches: u64 = whole
            .phases()
            .iter()
            .filter(|(phase, _)| *phase != Phase::Total)
            .map(|(_, ms)| ms)
            .sum();
        assert_eq!(stretches, 11_000);
    }

    #[test]
    fn a_milestone_never_reached_is_left_out_and_one_from_before_the_ask_is_not_this_asks() {
        let stop = deploy(|deploy| {
            deploy.desired_state = DesiredInstanceState::Stopped;
            deploy.converged_at_ms = Some(DETECTED + 300);
        });
        assert_eq!(stop.phases(), vec![(Phase::Total, 300)]);

        let already_up = deploy(|deploy| {
            deploy.booted_at_ms = Some(DETECTED - 60_000);
            deploy.converged_at_ms = Some(DETECTED + 20);
        });
        assert_eq!(already_up.phases(), vec![(Phase::Total, 20)]);

        let no_volume_stamp = deploy(|deploy| {
            deploy.layers_ready_at_ms = Some(DETECTED + 100);
            deploy.booted_at_ms = Some(DETECTED + 600);
            deploy.converged_at_ms = Some(DETECTED + 1_600);
        });
        assert_eq!(
            no_volume_stamp.phases(),
            vec![
                (Phase::Layers, 100),
                (Phase::Boot, 500),
                (Phase::Ready, 1_000),
                (Phase::Total, 1_600)
            ]
        );
        assert!(
            deploy(|_| {}).phases().is_empty(),
            "an open ask has no phases yet"
        );
    }

    #[test]
    fn what_converged_means_is_the_desired_states_to_say() {
        let wanted = deployment_id();
        let older = DeploymentId::parse("dep-0").unwrap();
        let held = |state| Some((&wanted, state));

        assert!(is_converged(
            &wanted,
            DesiredInstanceState::Running,
            held(InstanceState::Running)
        ));
        assert!(!is_converged(
            &wanted,
            DesiredInstanceState::Running,
            held(InstanceState::Starting)
        ));
        assert!(!is_converged(
            &wanted,
            DesiredInstanceState::Running,
            held(InstanceState::Idle)
        ));
        assert!(!is_converged(
            &wanted,
            DesiredInstanceState::Running,
            Some((&older, InstanceState::Running))
        ));
        assert!(!is_converged(&wanted, DesiredInstanceState::Running, None));

        assert!(is_converged(
            &wanted,
            DesiredInstanceState::OnRequest,
            held(InstanceState::Idle)
        ));
        assert!(is_converged(
            &wanted,
            DesiredInstanceState::OnRequest,
            held(InstanceState::Running)
        ));
        assert!(!is_converged(
            &wanted,
            DesiredInstanceState::OnRequest,
            held(InstanceState::Starting)
        ));
        assert!(!is_converged(
            &wanted,
            DesiredInstanceState::OnRequest,
            Some((&older, InstanceState::Idle))
        ));

        assert!(is_converged(&wanted, DesiredInstanceState::Stopped, None));
        assert!(is_converged(
            &wanted,
            DesiredInstanceState::Stopped,
            held(InstanceState::Stopped)
        ));
        assert!(is_converged(
            &wanted,
            DesiredInstanceState::Stopped,
            Some((&older, InstanceState::Stopped))
        ));
        assert!(!is_converged(
            &wanted,
            DesiredInstanceState::Stopped,
            held(InstanceState::Running)
        ));
    }

    #[tokio::test]
    async fn a_change_opens_an_ask_and_the_moment_the_app_is_what_it_asks_for_closes_it() {
        let host = test_host().await;
        host.state
            .put_record(instance_record(|record| {
                record.state = InstanceState::Starting;
                record.converged_at = Some(observed_at());
            }))
            .await;

        detected(
            &host,
            &changes(vec![desired_instance(|_| {})], vec![]),
            Cause::Change,
            DETECTED,
        )
        .await;
        let opened = host.state.snapshot().await.deploys[&app_id()].clone();
        assert!(opened.is_open());
        assert_eq!(opened.detected_at_ms, DETECTED);
        assert_eq!(
            host.state.record(&app_id()).await.unwrap().converged_at,
            None,
            "a new ask is not what the record reached before it"
        );

        observe(&host, DETECTED + 100).await;
        assert!(
            host.state.snapshot().await.deploys[&app_id()].is_open(),
            "still starting"
        );

        host.state
            .update_record(&app_id(), |record| record.state = InstanceState::Running)
            .await;
        observe(&host, DETECTED + 2_500).await;
        let closed = host.state.snapshot().await.deploys[&app_id()].clone();
        assert_eq!(closed.converged_at_ms, Some(DETECTED + 2_500));
        assert_eq!(
            host.state.record(&app_id()).await.unwrap().converged_at,
            Some(Timestamp::from_epoch_ms(DETECTED + 2_500))
        );
        assert_eq!(
            host.metrics
                .converge
                .outcome(DesiredInstanceState::Running, Cause::Change, Outcome::Converged)
                .load(Ordering::Relaxed),
            1
        );
        assert_eq!(
            host.metrics
                .converge
                .took(Phase::Total, DesiredInstanceState::Running, Cause::Change)
                .count(),
            1
        );

        observe(&host, DETECTED + 9_000).await;
        assert_eq!(
            host.state.snapshot().await.deploys[&app_id()].converged_at_ms,
            Some(DETECTED + 2_500),
            "an ask closes once"
        );
    }

    #[tokio::test]
    async fn an_ask_the_next_one_overtakes_is_superseded_and_an_app_the_document_dropped_is_forgotten() {
        let host = test_host().await;
        let first = changes(
            vec![
                desired_instance(|_| {}),
                desired_instance(|instance| instance.app_id = other()),
            ],
            vec![],
        );
        detected(&host, &first, Cause::Change, DETECTED).await;

        let second = changes(
            vec![desired_instance(|instance| {
                instance.deployment_id = DeploymentId::parse("dep-2").unwrap();
            })],
            vec![other()],
        );
        detected(&host, &second, Cause::Change, DETECTED + 1_000).await;

        let deploys = host.state.snapshot().await.deploys;
        assert_eq!(deploys.len(), 1);
        assert_eq!(deploys[&app_id()].deployment_id.as_str(), "dep-2");
        assert_eq!(deploys[&app_id()].detected_at_ms, DETECTED + 1_000);
        assert_eq!(
            host.metrics
                .converge
                .outcome(DesiredInstanceState::Running, Cause::Change, Outcome::Superseded)
                .load(Ordering::Relaxed),
            2
        );
        assert_eq!(
            host.metrics
                .converge
                .took(Phase::Total, DesiredInstanceState::Running, Cause::Change)
                .count(),
            0,
            "an ask that never got there has no time to report"
        );
    }

    #[tokio::test]
    async fn a_restart_is_measured_apart_and_leaves_the_record_the_moment_it_reached_before() {
        let host = test_host().await;
        host.state
            .put_record(instance_record(|record| {
                record.converged_at = Some(observed_at())
            }))
            .await;
        detected(
            &host,
            &changes(vec![desired_instance(|_| {})], vec![]),
            Cause::Restart,
            DETECTED,
        )
        .await;
        assert_eq!(
            host.state.record(&app_id()).await.unwrap().converged_at,
            Some(observed_at())
        );

        observe(&host, DETECTED + 50).await;
        assert_eq!(
            host.state.record(&app_id()).await.unwrap().converged_at,
            Some(observed_at()),
            "what it reached before the restart is when it got there"
        );
        assert_eq!(
            host.metrics
                .converge
                .took(Phase::Total, DesiredInstanceState::Running, Cause::Restart)
                .count(),
            1
        );
        assert_eq!(
            host.metrics
                .converge
                .took(Phase::Total, DesiredInstanceState::Running, Cause::Change)
                .count(),
            0
        );
    }

    #[tokio::test]
    async fn every_stretch_of_a_first_deploy_is_stamped_by_the_pass_that_does_it() {
        let _serial = ONE_HOST_AT_A_TIME.lock().await;
        let host = test_host().await;
        let desired = desired_state(|state| {
            state.volumes = vec![desired_volume(|_| {})];
            state.instances = vec![desired_instance(|instance| {
                instance.activation = Some(ActivationPolicy {
                    sleep_when: SleepPolicy::Never,
                });
                instance.config.health_check = HealthCheck::BootCompleted;
            })];
        });
        host.cache.lock().await.accept(desired.clone());
        let noticed = crate::clock::now_ms();
        detected(
            &host,
            &changes(desired.instances.clone(), vec![]),
            Cause::Change,
            noticed,
        )
        .await;

        crate::domain::reconcile::reconcile(
            host.arc(),
            &desired,
            crate::domain::metrics::passes::Trigger::Change,
        )
        .await;
        let after_pass = host.state.snapshot().await.deploys[&app_id()].clone();
        assert!(after_pass.is_open(), "starting is not there yet");
        assert!(after_pass.layers_ready_at_ms.is_some_and(|at| at >= noticed));
        assert!(after_pass.volume_ready_at_ms >= after_pass.layers_ready_at_ms);
        assert!(after_pass.booted_at_ms >= after_pass.volume_ready_at_ms);

        host.vms.set_status(VmStatus {
            loaded: true,
            active: true,
            frozen: false,
            failed: false,
            started_this_boot: true,
            exit: None,
        });
        // The guest is up and its tenant is listening: the test is the tenant, on the loopback.
        let listener = tokio::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
            .await
            .unwrap();
        let listening = protocol::HttpPort::new(listener.local_addr().unwrap().port()).unwrap();
        tokio::spawn(async move { while listener.accept().await.is_ok() {} });
        host.state
            .update_record(&app_id(), |record| {
                record.guest_ipv4 = crate::domain::health::probe::loopback();
                record.http_port = listening;
            })
            .await;
        crate::domain::reconcile::refresh(host.arc()).await;
        let closed = host.state.snapshot().await.deploys[&app_id()].clone();
        assert!(closed.converged_at_ms >= closed.booted_at_ms, "{closed:?}");
        let phases: Vec<Phase> = closed.phases().into_iter().map(|(phase, _)| phase).collect();
        assert_eq!(
            phases,
            vec![
                Phase::Layers,
                Phase::Volume,
                Phase::Boot,
                Phase::Ready,
                Phase::Total
            ]
        );
        assert!(host.state.record(&app_id()).await.unwrap().converged_at.is_some());
    }

    #[tokio::test]
    async fn the_page_says_which_apps_are_there_and_how_long_the_others_have_been_on_their_way() {
        let metrics = HostMetrics::default();
        let deploys = BTreeMap::from([
            (
                app_id(),
                deploy(|deploy| {
                    deploy.layers_ready_at_ms = Some(DETECTED + 1_000);
                    deploy.booted_at_ms = Some(DETECTED + 1_250);
                    deploy.converged_at_ms = Some(DETECTED + 3_000);
                }),
            ),
            (other(), deploy(|_| {})),
        ]);
        let mut report = crate::domain::metrics::tests::report();
        report.instances = vec![
            reported_instance(|_| {}),
            reported_instance(|instance| {
                instance.app_id = other();
                instance.state = InstanceState::Starting;
            }),
        ];
        let snapshot = HostSnapshot {
            deploys,
            ..Default::default()
        };
        let page = page(&report, &metrics, &snapshot, DETECTED + 10_000);

        let converged = lines_for(&page, "nibrunner_app_converged");
        assert_eq!(
            converged,
            vec![
                "nibrunner_app_converged{app=\"app-1\"} 1",
                "nibrunner_app_converged{app=\"app-2\"} 0",
            ]
        );
        let converging = lines_for(&page, "nibrunner_app_converging_seconds");
        assert_eq!(
            converging,
            vec![
                "nibrunner_app_converging_seconds{app=\"app-1\"} 0.000",
                "nibrunner_app_converging_seconds{app=\"app-2\"} 10.000",
            ]
        );
        let last = lines_for(&page, "nibrunner_app_last_converge_seconds");
        assert_eq!(
            last,
            vec![
                "nibrunner_app_last_converge_seconds{app=\"app-1\",phase=\"layers\"} 1.000",
                "nibrunner_app_last_converge_seconds{app=\"app-1\",phase=\"boot\"} 0.250",
                "nibrunner_app_last_converge_seconds{app=\"app-1\",phase=\"ready\"} 1.750",
                "nibrunner_app_last_converge_seconds{app=\"app-1\",phase=\"total\"} 3.000",
            ]
        );
        assert_eq!(
            lines_for(&page, "nibrunner_converge_seconds_count").len(),
            PHASES.len() * DESIRED_STATES.len() * CAUSES.len(),
            "every stretch is there to be rated from zero"
        );
        assert!(page.contains(
            "nibrunner_converge_outcomes_total{desired_state=\"on-request\",cause=\"restart\",outcome=\"superseded\"} 0"
        ));
    }

    #[tokio::test]
    async fn an_app_that_is_no_longer_what_it_reached_reads_as_not_there_without_a_new_ask() {
        let metrics = HostMetrics::default();
        let deploys = BTreeMap::from([(
            app_id(),
            deploy(|deploy| deploy.converged_at_ms = Some(DETECTED + 1)),
        )]);
        let mut report = crate::domain::metrics::tests::report();
        report.instances = vec![reported_instance(|instance| {
            instance.state = InstanceState::Failed
        })];
        let snapshot = HostSnapshot {
            deploys,
            ..Default::default()
        };
        let page = page(&report, &metrics, &snapshot, DETECTED + 5_000);
        assert_eq!(
            lines_for(&page, "nibrunner_app_converged"),
            vec!["nibrunner_app_converged{app=\"app-1\"} 0"]
        );
        assert_eq!(
            lines_for(&page, "nibrunner_app_converging_seconds"),
            vec!["nibrunner_app_converging_seconds{app=\"app-1\"} 0.000"],
            "it got there once; what it is now is the state series' to say"
        );
    }
}
