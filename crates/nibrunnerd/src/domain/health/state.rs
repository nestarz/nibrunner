use protocol::{HealthCheck, HttpPort, InstanceState, TenantExit, Timestamp};
use serde::{Deserialize, Serialize};

use crate::adapters::vm::VmStatus;
use crate::domain::health::probe::ProbeFailure;

pub const STARTUP_PROBE_INTERVAL_MS: u64 = 250;

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct HealthTracker {
    pub consecutive_successes: u32,
    pub consecutive_failures: u32,
    pub ever_healthy: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_healthy_at: Option<Timestamp>,
    /// How the latest of the failures in a row went; nothing once a probe finds the tenant well.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_failure: Option<ProbeFailure>,
}

pub fn initial_tracker() -> HealthTracker {
    HealthTracker::default()
}

pub fn apply_probe(
    tracker: &HealthTracker,
    outcome: Result<(), ProbeFailure>,
    at: &Timestamp,
    healthy_threshold: u32,
) -> HealthTracker {
    if let Err(failure) = outcome {
        return HealthTracker {
            consecutive_successes: 0,
            consecutive_failures: tracker.consecutive_failures + 1,
            last_failure: Some(failure),
            ..tracker.clone()
        };
    }
    let consecutive_successes = tracker.consecutive_successes + 1;
    HealthTracker {
        consecutive_successes,
        consecutive_failures: 0,
        ever_healthy: tracker.ever_healthy || consecutive_successes >= healthy_threshold,
        last_healthy_at: Some(at.clone()),
        last_failure: None,
    }
}

#[derive(Debug, Clone)]
pub struct GraceInputs<'a> {
    pub health_check: &'a HealthCheck,
    pub started_at_ms: Option<i64>,
    pub now_ms: i64,
}

pub fn is_within_grace_period(grace: &GraceInputs<'_>) -> bool {
    match grace.started_at_ms {
        None => true,
        Some(started_at_ms) => {
            grace.now_ms - started_at_ms < grace.health_check.probe().grace_period_ms as i64
        }
    }
}

pub fn is_on_startup_grid(tracker: &HealthTracker, grace: &GraceInputs<'_>) -> bool {
    !tracker.ever_healthy && is_within_grace_period(grace)
}

pub fn next_probe_delay_ms(tracker: &HealthTracker, grace: &GraceInputs<'_>) -> u64 {
    if is_on_startup_grid(tracker, grace) {
        STARTUP_PROBE_INTERVAL_MS.min(grace.health_check.probe().interval_ms)
    } else {
        grace.health_check.probe().interval_ms
    }
}

/// Whether the next probe asks the guest's port. A check of a port always does; `boot-completed`
/// does until the port has answered once — the tenant is listening — and never after, the port
/// being its start gate and not its health.
pub fn asks_the_port(tracker: &HealthTracker, health_check: &HealthCheck) -> bool {
    health_check.probes_a_port() || !tracker.ever_healthy
}

pub fn describe_instance_failure(
    unit: &VmStatus,
    tracker: &HealthTracker,
    health_check: &HealthCheck,
    http_port: HttpPort,
    guest_verdict: Option<&str>,
) -> String {
    if !unit.active {
        if let Some(verdict) = guest_verdict {
            return format!("the microVM stopped: {verdict}");
        }
        return match unit.exit {
            None => "the microVM exited".to_string(),
            Some(exit) => format!("the microVM {}", exit.describe()),
        };
    }
    format!(
        "nothing answered on port {http_port} inside the guest: {} health probes failed after the {}ms grace period",
        tracker.consecutive_failures, health_check.probe().grace_period_ms
    )
}

/// Why an instance that was well reads as unhealthy: which check, how many probes in a row it
/// failed, and how the last of them did — a wrong path, a pinned event loop and a stalled disk
/// otherwise all look the same.
pub fn describe_unhealthy_instance(tracker: &HealthTracker, health_check: &HealthCheck) -> String {
    let failed = tracker.consecutive_failures;
    let probes = if failed == 1 { "probe" } else { "probes" };
    let of = match health_check {
        HealthCheck::Http { path, .. } => format!(" of {path}"),
        HealthCheck::Tcp { .. } | HealthCheck::BootCompleted => String::new(),
    };
    let sentence = format!("{failed} {} {probes}{of} failed in a row", health_check.kind());
    match &tracker.last_failure {
        Some(failure) => format!("{sentence}; the last {}", failure.describe()),
        None => sentence,
    }
}

#[derive(Debug, Clone)]
pub struct LifecycleInputs<'a> {
    pub unit: &'a VmStatus,
    pub tracker: &'a HealthTracker,
    pub health_check: &'a HealthCheck,
    pub desired_running: bool,
    pub on_request: bool,
    pub stop_requested: bool,
    pub snapshotting: bool,
    pub started_at_ms: Option<i64>,
    pub now_ms: i64,
    pub current: InstanceState,
    /// How the tenant ended, when its guest went down saying it had been run once.
    pub ran_once: Option<TenantExit>,
}

fn evaluate_stopped_state(inputs: &LifecycleInputs<'_>) -> InstanceState {
    let down = if inputs.on_request && inputs.desired_running {
        InstanceState::Idle
    } else {
        InstanceState::Stopped
    };
    if inputs.stop_requested || inputs.snapshotting || !inputs.desired_running {
        return down;
    }
    if inputs.ran_once.is_some() || inputs.current == InstanceState::Exited {
        return InstanceState::Exited;
    }
    if inputs.started_at_ms.is_some() {
        return InstanceState::Failed;
    }
    // A start that failed before the microVM ever came up leaves the record Failed with the reason
    // on it, but no started_at. Collapsing that to Idle would read as an on-request app asleep and
    // well; it stays Failed until a fresh start attempt moves it off.
    if inputs.current == InstanceState::Failed {
        return InstanceState::Failed;
    }
    if inputs.on_request && inputs.current != InstanceState::Pending {
        down
    } else {
        InstanceState::Pending
    }
}

pub fn evaluate_instance_state(inputs: &LifecycleInputs<'_>) -> InstanceState {
    if inputs.unit.failed {
        return InstanceState::Failed;
    }
    if !inputs.unit.active {
        return evaluate_stopped_state(inputs);
    }
    if inputs.stop_requested {
        return InstanceState::Stopping;
    }
    if inputs.unit.frozen {
        return InstanceState::Frozen;
    }
    if inputs.tracker.consecutive_successes >= inputs.health_check.probe().healthy_threshold {
        return InstanceState::Running;
    }
    let within_grace = is_within_grace_period(&GraceInputs {
        health_check: inputs.health_check,
        started_at_ms: inputs.started_at_ms,
        now_ms: inputs.now_ms,
    });
    if inputs.tracker.consecutive_failures >= inputs.health_check.probe().unhealthy_threshold && !within_grace
    {
        return if inputs.tracker.ever_healthy {
            InstanceState::Unhealthy
        } else {
            InstanceState::Failed
        };
    }
    if inputs.tracker.ever_healthy {
        InstanceState::Running
    } else {
        InstanceState::Starting
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapters::vm::{VmExit, UNKNOWN_VM};
    use crate::test_support::TCP_HEALTH_CHECK;
    use protocol::{Probe, DEFAULT_HTTP_PORT};

    const STARTED_AT_MS: i64 = 1_000_000;
    const OBSERVED_AT: &str = "2026-08-03T10:00:00.000Z";

    fn grace_ms() -> i64 {
        TCP_HEALTH_CHECK.probe().grace_period_ms as i64
    }

    fn within_grace() -> i64 {
        STARTED_AT_MS + grace_ms() - 1
    }

    fn past_grace() -> i64 {
        STARTED_AT_MS + grace_ms() + 1
    }

    fn observed_at() -> Timestamp {
        Timestamp::parse(OBSERVED_AT).unwrap()
    }

    fn active() -> VmStatus {
        VmStatus {
            loaded: true,
            active: true,
            frozen: false,
            failed: false,
            started_this_boot: true,
            exit: None,
        }
    }

    fn exited() -> VmStatus {
        VmStatus {
            loaded: true,
            active: false,
            frozen: false,
            failed: false,
            started_this_boot: true,
            exit: Some(VmExit::Code(0)),
        }
    }

    fn crashed() -> VmStatus {
        VmStatus {
            loaded: true,
            active: false,
            frozen: false,
            failed: true,
            started_this_boot: true,
            exit: Some(VmExit::Code(1)),
        }
    }

    fn absent() -> VmStatus {
        UNKNOWN_VM
    }

    fn probe(tracker: &HealthTracker, healthy: bool, healthy_threshold: u32) -> HealthTracker {
        let outcome = if healthy {
            Ok(())
        } else {
            Err(ProbeFailure::Refused)
        };
        apply_probe(tracker, outcome, &observed_at(), healthy_threshold)
    }

    fn failing(count: u32) -> HealthTracker {
        let mut tracker = initial_tracker();
        for _ in 0..count {
            tracker = probe(&tracker, false, 1);
        }
        tracker
    }

    fn healthy_then(failures: u32) -> HealthTracker {
        let mut tracker = probe(&initial_tracker(), true, 1);
        for _ in 0..failures {
            tracker = probe(&tracker, false, 1);
        }
        tracker
    }

    struct Evaluate {
        unit: VmStatus,
        tracker: HealthTracker,
        now_ms: i64,
        health_check: HealthCheck,
        stop_requested: bool,
        desired_running: bool,
        on_request: bool,
        snapshotting: bool,
        started_at_ms: Option<i64>,
        current: InstanceState,
        ran_once: Option<TenantExit>,
    }

    impl Default for Evaluate {
        fn default() -> Self {
            Self {
                unit: active(),
                tracker: initial_tracker(),
                now_ms: within_grace(),
                health_check: TCP_HEALTH_CHECK,
                stop_requested: false,
                desired_running: true,
                on_request: false,
                snapshotting: false,
                started_at_ms: Some(STARTED_AT_MS),
                current: InstanceState::Pending,
                ran_once: None,
            }
        }
    }

    fn evaluate(inputs: Evaluate) -> InstanceState {
        evaluate_instance_state(&LifecycleInputs {
            unit: &inputs.unit,
            tracker: &inputs.tracker,
            health_check: &inputs.health_check,
            desired_running: inputs.desired_running,
            on_request: inputs.on_request,
            stop_requested: inputs.stop_requested,
            snapshotting: inputs.snapshotting,
            started_at_ms: inputs.started_at_ms,
            now_ms: inputs.now_ms,
            current: inputs.current,
            ran_once: inputs.ran_once,
        })
    }

    fn delay(tracker: &HealthTracker, now_ms: i64, health_check: &HealthCheck) -> u64 {
        next_probe_delay_ms(
            tracker,
            &GraceInputs {
                health_check,
                started_at_ms: Some(STARTED_AT_MS),
                now_ms,
            },
        )
    }

    #[test]
    fn how_soon_a_tenant_is_asked_again() {
        assert_eq!(
            delay(&initial_tracker(), within_grace(), &TCP_HEALTH_CHECK),
            STARTUP_PROBE_INTERVAL_MS
        );
        assert_eq!(
            delay(&healthy_then(0), within_grace(), &TCP_HEALTH_CHECK),
            TCP_HEALTH_CHECK.probe().interval_ms
        );
        assert_eq!(
            delay(&initial_tracker(), past_grace(), &TCP_HEALTH_CHECK),
            TCP_HEALTH_CHECK.probe().interval_ms
        );
        let fast = HealthCheck::Tcp {
            probe: Probe {
                interval_ms: STARTUP_PROBE_INTERVAL_MS - 1,
                ..TCP_HEALTH_CHECK.probe().clone()
            },
        };
        assert_eq!(
            delay(&initial_tracker(), within_grace(), &fast),
            fast.probe().interval_ms
        );
    }

    #[test]
    fn probe_accounting_keeps_the_fact_it_was_once_healthy() {
        let tracker = probe(&failing(2), true, 1);
        assert_eq!(tracker.consecutive_successes, 1);
        assert_eq!(tracker.consecutive_failures, 0);
        assert!(tracker.ever_healthy);
        assert_eq!(tracker.last_healthy_at, Some(observed_at()));
        let then_failed = probe(&tracker, false, 1);
        assert_eq!(then_failed.consecutive_successes, 0);
        assert_eq!(then_failed.consecutive_failures, 1);
        assert!(then_failed.ever_healthy);
    }

    #[test]
    fn probe_accounting_keeps_how_the_last_probe_failed_until_one_succeeds() {
        assert_eq!(initial_tracker().last_failure, None);
        let refused = probe(&initial_tracker(), false, 1);
        assert_eq!(refused.last_failure, Some(ProbeFailure::Refused));
        let then_timed_out = apply_probe(
            &refused,
            Err(ProbeFailure::TimedOut { after_ms: 2_000 }),
            &observed_at(),
            1,
        );
        assert_eq!(
            then_timed_out.last_failure,
            Some(ProbeFailure::TimedOut { after_ms: 2_000 }),
            "the latest failure is the one kept"
        );
        assert_eq!(then_timed_out.consecutive_failures, 2);
        let well = probe(&then_timed_out, true, 1);
        assert_eq!(well.last_failure, None);
        assert_eq!(well.last_healthy_at, Some(observed_at()));
    }

    #[test]
    fn a_tracker_written_before_it_kept_the_last_failure_still_reads() {
        let written = serde_json::json!({
            "consecutiveSuccesses": 0,
            "consecutiveFailures": 3,
            "everHealthy": true,
        });
        let tracker: HealthTracker = serde_json::from_value(written).unwrap();
        assert_eq!(tracker.consecutive_failures, 3);
        assert_eq!(tracker.last_failure, None);
        assert_eq!(
            describe_unhealthy_instance(&tracker, &TCP_HEALTH_CHECK),
            "3 tcp probes failed in a row"
        );
    }

    #[test]
    fn an_instance_that_stopped_answering_says_which_check_how_often_and_how() {
        let http = |path: &str| HealthCheck::Http {
            path: path.to_string(),
            probe: TCP_HEALTH_CHECK.probe().clone(),
        };
        let after = |failures: u32, failure: ProbeFailure| HealthTracker {
            consecutive_failures: failures,
            ever_healthy: true,
            last_failure: Some(failure),
            ..initial_tracker()
        };
        assert_eq!(
            describe_unhealthy_instance(&after(3, ProbeFailure::Answered { status: 404 }), &http("/nope")),
            "3 http probes of /nope failed in a row; the last answered 404 Not Found"
        );
        assert_eq!(
            describe_unhealthy_instance(&after(3, ProbeFailure::Refused), &TCP_HEALTH_CHECK),
            "3 tcp probes failed in a row; the last tcp connect refused"
        );
        assert_eq!(
            describe_unhealthy_instance(
                &after(7, ProbeFailure::TimedOut { after_ms: 2_000 }),
                &http("/health")
            ),
            "7 http probes of /health failed in a row; the last timed out after 2000 ms"
        );
        assert_eq!(
            describe_unhealthy_instance(&after(1, ProbeFailure::Refused), &TCP_HEALTH_CHECK),
            "1 tcp probe failed in a row; the last tcp connect refused"
        );
    }

    #[test]
    fn a_tenant_is_only_called_healthy_once_it_has_answered_as_often_as_it_was_asked_to() {
        let mut tracker = initial_tracker();
        for answered in 1..3 {
            tracker = probe(&tracker, true, 3);
            assert_eq!(tracker.consecutive_successes, answered);
            assert!(!tracker.ever_healthy, "{answered} answers is not enough");
        }
        tracker = probe(&tracker, true, 3);
        assert!(tracker.ever_healthy);

        let lapsed = probe(&probe(&tracker, false, 3), true, 3);
        assert_eq!(lapsed.consecutive_successes, 1);
        assert!(lapsed.ever_healthy);
    }

    #[test]
    fn a_tenant_that_has_never_answered_is_asked_on_the_startup_grid_until_its_grace_runs_out() {
        let grace = |tracker: &HealthTracker, now_ms: i64| {
            is_on_startup_grid(
                tracker,
                &GraceInputs {
                    health_check: &TCP_HEALTH_CHECK,
                    started_at_ms: Some(STARTED_AT_MS),
                    now_ms,
                },
            )
        };
        assert!(grace(&initial_tracker(), within_grace()));
        assert!(grace(&failing(3), within_grace()));
        assert!(!grace(&initial_tracker(), past_grace()));
        assert!(!grace(&healthy_then(3), within_grace()));

        assert!(is_on_startup_grid(
            &initial_tracker(),
            &GraceInputs {
                health_check: &TCP_HEALTH_CHECK,
                started_at_ms: None,
                now_ms: past_grace(),
            }
        ));
    }

    #[test]
    fn a_booted_vm_is_not_a_running_app() {
        assert_eq!(evaluate(Evaluate::default()), InstanceState::Starting);
        assert_eq!(
            evaluate(Evaluate {
                tracker: probe(&initial_tracker(), true, 1),
                ..Default::default()
            }),
            InstanceState::Running
        );
        assert_eq!(
            evaluate(Evaluate {
                tracker: failing(10),
                ..Default::default()
            }),
            InstanceState::Starting
        );
        assert_eq!(
            evaluate(Evaluate {
                tracker: failing(TCP_HEALTH_CHECK.probe().unhealthy_threshold),
                now_ms: past_grace(),
                ..Default::default()
            }),
            InstanceState::Failed
        );
        assert_eq!(
            evaluate(Evaluate {
                tracker: healthy_then(TCP_HEALTH_CHECK.probe().unhealthy_threshold),
                now_ms: past_grace(),
                ..Default::default()
            }),
            InstanceState::Unhealthy
        );
        assert_eq!(
            evaluate(Evaluate {
                tracker: healthy_then(1),
                now_ms: past_grace(),
                ..Default::default()
            }),
            InstanceState::Running
        );
    }

    #[test]
    fn a_boot_completed_guest_is_running_once_its_port_has_answered_and_not_before() {
        let booted = || Evaluate {
            health_check: HealthCheck::BootCompleted,
            ..Default::default()
        };
        let grace_ms = HealthCheck::BootCompleted.probe().grace_period_ms as i64;
        let within_grace = STARTED_AT_MS + grace_ms - 1;
        let past_grace = STARTED_AT_MS + grace_ms + 1;

        assert!(asks_the_port(&initial_tracker(), &HealthCheck::BootCompleted));
        assert_eq!(
            evaluate(Evaluate {
                now_ms: within_grace,
                ..booted()
            }),
            InstanceState::Starting,
            "the microVM is up, but nothing has answered on the port yet"
        );
        assert_eq!(
            evaluate(Evaluate {
                tracker: failing(3),
                now_ms: within_grace,
                ..booted()
            }),
            InstanceState::Starting,
            "a refused connect within the grace is the tenant still binding its port"
        );
        assert_eq!(
            delay(&failing(3), within_grace, &HealthCheck::BootCompleted),
            STARTUP_PROBE_INTERVAL_MS,
            "and it is asked again on the settling cadence"
        );

        let listening = probe(&initial_tracker(), true, 1);
        assert_eq!(
            evaluate(Evaluate {
                tracker: listening.clone(),
                now_ms: STARTED_AT_MS + 1,
                ..booted()
            }),
            InstanceState::Running
        );
        assert!(
            !asks_the_port(&listening, &HealthCheck::BootCompleted),
            "the port is asked once, and never again"
        );
        assert!(
            asks_the_port(&listening, &TCP_HEALTH_CHECK),
            "a check of a port goes on asking it"
        );

        assert_eq!(
            evaluate(Evaluate {
                tracker: failing(1),
                now_ms: past_grace,
                ..booted()
            }),
            InstanceState::Failed,
            "a port that never opened is a guest that never answered"
        );
    }

    #[test]
    fn what_the_vm_itself_is_doing() {
        assert_eq!(
            evaluate(Evaluate {
                unit: exited(),
                tracker: healthy_then(0),
                now_ms: past_grace(),
                ..Default::default()
            }),
            InstanceState::Failed
        );
        assert_eq!(
            evaluate(Evaluate {
                unit: crashed(),
                tracker: probe(&initial_tracker(), true, 1),
                ..Default::default()
            }),
            InstanceState::Failed
        );
        assert_eq!(
            evaluate(Evaluate {
                unit: exited(),
                now_ms: past_grace(),
                stop_requested: true,
                ..Default::default()
            }),
            InstanceState::Stopped
        );
        assert_eq!(
            evaluate(Evaluate {
                tracker: probe(&initial_tracker(), true, 1),
                now_ms: past_grace(),
                stop_requested: true,
                ..Default::default()
            }),
            InstanceState::Stopping
        );
        assert_eq!(
            evaluate(Evaluate {
                unit: exited(),
                now_ms: STARTED_AT_MS,
                started_at_ms: None,
                ..Default::default()
            }),
            InstanceState::Pending
        );
        assert_eq!(
            evaluate(Evaluate {
                unit: absent(),
                now_ms: STARTED_AT_MS,
                started_at_ms: None,
                ..Default::default()
            }),
            InstanceState::Pending
        );
        assert_eq!(
            evaluate(Evaluate {
                unit: absent(),
                now_ms: past_grace(),
                desired_running: false,
                ..Default::default()
            }),
            InstanceState::Stopped
        );
    }

    #[test]
    fn a_program_run_once_that_ended_is_exited_rather_than_failed_and_stays_so() {
        for exit in [TenantExit::Code(0), TenantExit::Code(3), TenantExit::Signal(9)] {
            assert_eq!(
                evaluate(Evaluate {
                    unit: exited(),
                    tracker: healthy_then(0),
                    now_ms: past_grace(),
                    current: InstanceState::Running,
                    ran_once: Some(exit),
                    ..Default::default()
                }),
                InstanceState::Exited
            );
        }
        assert_eq!(
            evaluate(Evaluate {
                unit: exited(),
                now_ms: past_grace(),
                current: InstanceState::Exited,
                ..Default::default()
            }),
            InstanceState::Exited,
            "the console is read once, and the state it gave is kept"
        );
        assert_eq!(
            evaluate(Evaluate {
                unit: exited(),
                now_ms: past_grace(),
                stop_requested: true,
                current: InstanceState::Exited,
                ..Default::default()
            }),
            InstanceState::Stopped,
            "a stop the document asked for is a stop"
        );
    }

    #[test]
    fn an_app_that_runs_on_request_is_idle_rather_than_stopped() {
        let base = Evaluate {
            on_request: true,
            started_at_ms: None,
            now_ms: STARTED_AT_MS,
            ..Default::default()
        };
        assert_eq!(
            evaluate(Evaluate {
                unit: absent(),
                current: InstanceState::Idle,
                ..base
            }),
            InstanceState::Idle
        );
        assert_eq!(
            evaluate(Evaluate {
                unit: absent(),
                current: InstanceState::Pending,
                on_request: true,
                started_at_ms: None,
                now_ms: STARTED_AT_MS,
                ..Default::default()
            }),
            InstanceState::Pending
        );
        assert_eq!(
            evaluate(Evaluate {
                unit: exited(),
                on_request: true,
                stop_requested: true,
                now_ms: past_grace(),
                ..Default::default()
            }),
            InstanceState::Idle
        );
        assert_eq!(
            evaluate(Evaluate {
                unit: exited(),
                on_request: true,
                now_ms: past_grace(),
                ..Default::default()
            }),
            InstanceState::Failed
        );
        assert_eq!(
            evaluate(Evaluate {
                unit: exited(),
                on_request: true,
                snapshotting: true,
                now_ms: past_grace(),
                ..Default::default()
            }),
            InstanceState::Idle
        );
        assert_eq!(
            evaluate(Evaluate {
                unit: absent(),
                on_request: true,
                desired_running: false,
                now_ms: past_grace(),
                ..Default::default()
            }),
            InstanceState::Stopped
        );
    }

    #[test]
    fn a_first_start_that_failed_stays_failed_rather_than_reading_as_asleep_or_still_pending() {
        // On-request, never booted (no started_at), nothing asked it to stop: the record is Failed
        // only because its first start failed. It must not collapse to Idle or Pending.
        let after_failed_start = Evaluate {
            unit: absent(),
            on_request: true,
            started_at_ms: None,
            now_ms: STARTED_AT_MS,
            current: InstanceState::Failed,
            ..Default::default()
        };
        assert_eq!(evaluate(after_failed_start), InstanceState::Failed);
        // The same for an always-on app, whose failed first start used to read as Pending.
        assert_eq!(
            evaluate(Evaluate {
                unit: absent(),
                on_request: false,
                started_at_ms: None,
                now_ms: STARTED_AT_MS,
                current: InstanceState::Failed,
                ..Default::default()
            }),
            InstanceState::Failed
        );
        // A document that no longer wants it up still takes it down, Failed or not.
        assert_eq!(
            evaluate(Evaluate {
                unit: absent(),
                on_request: true,
                desired_running: false,
                started_at_ms: None,
                now_ms: STARTED_AT_MS,
                current: InstanceState::Failed,
                ..Default::default()
            }),
            InstanceState::Stopped
        );
    }

    #[test]
    fn thresholds_are_honoured() {
        let two = HealthCheck::Tcp {
            probe: Probe {
                healthy_threshold: 2,
                ..TCP_HEALTH_CHECK.probe().clone()
            },
        };
        let once = probe(&initial_tracker(), true, 2);
        assert_eq!(
            evaluate(Evaluate {
                tracker: once.clone(),
                health_check: two.clone(),
                ..Default::default()
            }),
            InstanceState::Starting
        );
        let twice = probe(&once, true, 2);
        assert_eq!(
            evaluate(Evaluate {
                tracker: twice,
                health_check: two,
                ..Default::default()
            }),
            InstanceState::Running
        );
    }

    #[test]
    fn a_failure_accounts_for_itself() {
        let failure = |unit: &VmStatus, tracker: &HealthTracker, verdict: Option<&str>| {
            describe_instance_failure(unit, tracker, &TCP_HEALTH_CHECK, DEFAULT_HTTP_PORT, verdict)
        };
        assert_eq!(
            failure(&exited(), &initial_tracker(), None),
            "the microVM exited with exit code 0"
        );
        assert_eq!(
            failure(
                &VmStatus {
                    exit: None,
                    ..crashed()
                },
                &initial_tracker(),
                None
            ),
            "the microVM exited"
        );
        assert_eq!(
            failure(
                &VmStatus {
                    exit: Some(VmExit::Signal(9)),
                    ..crashed()
                },
                &initial_tracker(),
                None
            ),
            "the microVM was killed by signal 9 (SIGKILL)"
        );
        // A guest that said why it went down is quoted, whatever its VMM's exit code says.
        assert_eq!(
            failure(
                &exited(),
                &initial_tracker(),
                Some("the tenant used its 5 restarts without staying up; shutting the guest down")
            ),
            "the microVM stopped: the tenant used its 5 restarts without staying up; shutting the guest down"
        );
        assert_eq!(
            failure(
                &crashed(),
                &initial_tracker(),
                Some("Kernel panic - not syncing: Attempted to kill init!")
            ),
            "the microVM stopped: Kernel panic - not syncing: Attempted to kill init!"
        );
        let unreachable = HealthTracker {
            consecutive_failures: TCP_HEALTH_CHECK.probe().unhealthy_threshold,
            ..initial_tracker()
        };
        let expected = format!(
            "nothing answered on port {DEFAULT_HTTP_PORT} inside the guest: {} health probes failed after the {}ms grace period",
            TCP_HEALTH_CHECK.probe().unhealthy_threshold, TCP_HEALTH_CHECK.probe().grace_period_ms
        );
        assert_eq!(
            failure(
                &active(),
                &unreachable,
                Some("the tenant has stopped; shutting the guest down")
            ),
            expected
        );
        assert_eq!(failure(&active(), &unreachable, None), expected);
    }
}
