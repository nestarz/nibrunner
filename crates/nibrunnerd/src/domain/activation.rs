use protocol::{ActivationPolicy, InstanceState, SleepPolicy};

use crate::domain::report::InstanceRecord;

/// States holding a live microVM that can move to a disk snapshot.
const SLEEPABLE_STATES: [InstanceState; 2] = [InstanceState::Running, InstanceState::Frozen];

/// How long past its lifetime an app with a request still open is left to finish answering it.
/// Bounded, because a policy that recycles an app on a clock must not be held off it for ever by
/// a stream that never ends.
pub const MAX_LIFETIME_DRAIN_MS: i64 = 30_000;

/// How old a reading of an app's traffic may be and still stand for what the app is doing. The
/// counters are read on a clock of their own, a few seconds apart; a reading this far behind is
/// this host having lost sight of the app rather than the app having gone quiet, and an app it
/// cannot see is left up.
pub const MAX_ACTIVITY_AGE_MS: i64 = 15_000;

/// Why this host is putting a microVM to sleep, in the word the log and the report use for it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SleepReason {
    Quiet,
    LivedLongEnough,
    MemoryPressure,
}

impl SleepReason {
    pub fn as_str(self) -> &'static str {
        match self {
            SleepReason::Quiet => "idle",
            SleepReason::LivedLongEnough => "max-lifetime",
            SleepReason::MemoryPressure => "memory-pressure",
        }
    }
}

/// What a pass has observed about an instance since it was last started.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ActivitySignals {
    pub last_active_at_ms: Option<i64>,
    /// When this host last read the app's traffic counters, and nothing where it never has.
    pub measured_at_ms: Option<i64>,
    pub started_at_ms: Option<i64>,
    /// Requests the proxy has routed to the app and is still carrying the answer to.
    pub requests_open: u64,
}

impl ActivitySignals {
    /// Whether what this host last read of the app's traffic still stands for what it is doing.
    pub fn measured_lately(&self, now_ms: i64) -> bool {
        self.measured_at_ms.is_some_and(|measured_at| {
            (0..=MAX_ACTIVITY_AGE_MS).contains(&now_ms.saturating_sub(measured_at))
        })
    }
}

/// Whether a policy is one this host can act on without any signal from inside the guest.
pub fn should_sleep(
    policy: &ActivationPolicy,
    record: &InstanceRecord,
    signals: &ActivitySignals,
    now_ms: i64,
) -> Option<SleepReason> {
    if !record.on_request || !record.desired_running || !SLEEPABLE_STATES.contains(&record.state) {
        return None;
    }
    let answering = signals.requests_open > 0;
    match policy.sleep_when {
        SleepPolicy::Never => None,
        SleepPolicy::TrafficIdle { timeout_ms } => {
            // A stretch nobody read the counters over is not a stretch of quiet: an app taking
            // hundreds of requests a second reads exactly like one taking none.
            if answering || !signals.measured_lately(now_ms) {
                return None;
            }
            let quiet_since = signals.last_active_at_ms?;
            (now_ms - quiet_since >= timeout_ms.get() as i64).then_some(SleepReason::Quiet)
        }
        SleepPolicy::MaxLifetime { ttl_ms } => {
            let up_since = signals.started_at_ms?;
            let drain_ms = if answering { MAX_LIFETIME_DRAIN_MS } else { 0 };
            (now_ms - up_since >= ttl_ms.get() as i64 + drain_ms).then_some(SleepReason::LivedLongEnough)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::instance_record;
    use protocol::{IdleTimeoutMs, MaxLifetimeMs, INSTANCE_STATES};

    const NOW_MS: i64 = 10_000_000;
    const TIMEOUT_MS: u64 = 900_000;
    const TTL_MS: u64 = 3_600_000;

    fn traffic_idle() -> ActivationPolicy {
        ActivationPolicy {
            sleep_when: SleepPolicy::TrafficIdle {
                timeout_ms: IdleTimeoutMs::try_from(TIMEOUT_MS).unwrap(),
            },
        }
    }

    fn max_lifetime() -> ActivationPolicy {
        ActivationPolicy {
            sleep_when: SleepPolicy::MaxLifetime {
                ttl_ms: MaxLifetimeMs::try_from(TTL_MS).unwrap(),
            },
        }
    }

    fn never() -> ActivationPolicy {
        ActivationPolicy {
            sleep_when: SleepPolicy::Never,
        }
    }

    fn serving(state: InstanceState) -> InstanceRecord {
        instance_record(|record| {
            record.on_request = true;
            record.desired_running = true;
            record.state = state;
        })
    }

    fn quiet_since(ms: i64) -> ActivitySignals {
        ActivitySignals {
            last_active_at_ms: Some(ms),
            measured_at_ms: Some(NOW_MS),
            started_at_ms: None,
            requests_open: 0,
        }
    }

    fn up_since(ms: i64) -> ActivitySignals {
        ActivitySignals {
            last_active_at_ms: None,
            measured_at_ms: Some(NOW_MS),
            started_at_ms: Some(ms),
            requests_open: 0,
        }
    }

    #[test]
    fn a_quiet_app_sleeps_at_its_timeout_and_not_a_moment_before() {
        let record = serving(InstanceState::Running);
        let at = quiet_since(NOW_MS - TIMEOUT_MS as i64);
        assert_eq!(
            should_sleep(&traffic_idle(), &record, &at, NOW_MS),
            Some(SleepReason::Quiet)
        );
        let a_moment_ago = quiet_since(NOW_MS - TIMEOUT_MS as i64 + 1);
        assert_eq!(
            should_sleep(&traffic_idle(), &record, &a_moment_ago, NOW_MS),
            None
        );
    }

    #[test]
    fn an_app_that_has_lived_its_ttl_sleeps_however_busy_it_still_is() {
        let record = serving(InstanceState::Running);
        let signals = ActivitySignals {
            last_active_at_ms: Some(NOW_MS),
            measured_at_ms: Some(NOW_MS),
            started_at_ms: Some(NOW_MS - TTL_MS as i64),
            requests_open: 0,
        };
        assert_eq!(
            should_sleep(&max_lifetime(), &record, &signals, NOW_MS),
            Some(SleepReason::LivedLongEnough)
        );
        let younger = up_since(NOW_MS - TTL_MS as i64 + 1);
        assert_eq!(should_sleep(&max_lifetime(), &record, &younger, NOW_MS), None);
    }

    #[test]
    fn a_ttl_is_measured_from_the_start_and_a_timeout_from_the_last_caller() {
        let record = serving(InstanceState::Running);
        let busy_but_old = ActivitySignals {
            last_active_at_ms: Some(NOW_MS),
            measured_at_ms: Some(NOW_MS),
            started_at_ms: Some(NOW_MS - TTL_MS as i64),
            requests_open: 0,
        };
        assert_eq!(
            should_sleep(&traffic_idle(), &record, &busy_but_old, NOW_MS),
            None
        );

        let quiet_but_young = ActivitySignals {
            last_active_at_ms: Some(NOW_MS - TIMEOUT_MS as i64),
            measured_at_ms: Some(NOW_MS),
            started_at_ms: Some(NOW_MS),
            requests_open: 0,
        };
        assert_eq!(
            should_sleep(&max_lifetime(), &record, &quiet_but_young, NOW_MS),
            None
        );
    }

    #[test]
    fn a_policy_that_never_sleeps_lets_nothing_go_however_long_it_has_been() {
        let record = serving(InstanceState::Running);
        let signals = ActivitySignals {
            last_active_at_ms: Some(0),
            measured_at_ms: Some(NOW_MS),
            started_at_ms: Some(0),
            requests_open: 0,
        };
        assert_eq!(should_sleep(&never(), &record, &signals, NOW_MS), None);
    }

    #[test]
    fn an_app_with_a_request_open_is_not_quiet_however_long_its_counters_have_stood_still() {
        let record = serving(InstanceState::Running);
        let answering = ActivitySignals {
            requests_open: 1,
            ..quiet_since(0)
        };
        assert_eq!(should_sleep(&traffic_idle(), &record, &answering, NOW_MS), None);
    }

    #[test]
    fn an_app_whose_traffic_has_not_been_read_lately_is_not_quiet_however_still_its_counters_look() {
        let record = serving(InstanceState::Running);
        let long_enough = quiet_since(NOW_MS - TIMEOUT_MS as i64);
        let unread = ActivitySignals {
            measured_at_ms: Some(NOW_MS - MAX_ACTIVITY_AGE_MS - 1),
            ..long_enough
        };
        assert_eq!(should_sleep(&traffic_idle(), &record, &unread, NOW_MS), None);

        let read = ActivitySignals {
            measured_at_ms: Some(NOW_MS - MAX_ACTIVITY_AGE_MS),
            ..long_enough
        };
        assert_eq!(
            should_sleep(&traffic_idle(), &record, &read, NOW_MS),
            Some(SleepReason::Quiet)
        );
    }

    #[test]
    fn a_lifetime_runs_out_on_the_clock_whether_or_not_any_traffic_was_read() {
        let record = serving(InstanceState::Running);
        let unread = ActivitySignals {
            measured_at_ms: None,
            ..up_since(NOW_MS - TTL_MS as i64)
        };
        assert_eq!(
            should_sleep(&max_lifetime(), &record, &unread, NOW_MS),
            Some(SleepReason::LivedLongEnough)
        );
    }

    #[test]
    fn an_app_past_its_lifetime_is_left_to_finish_answering_but_not_for_ever() {
        let record = serving(InstanceState::Running);
        let still_answering = |lived_ms: i64| ActivitySignals {
            requests_open: 1,
            ..up_since(NOW_MS - lived_ms)
        };
        assert_eq!(
            should_sleep(&max_lifetime(), &record, &still_answering(TTL_MS as i64), NOW_MS),
            None
        );
        assert_eq!(
            should_sleep(
                &max_lifetime(),
                &record,
                &still_answering(TTL_MS as i64 + MAX_LIFETIME_DRAIN_MS - 1),
                NOW_MS
            ),
            None
        );
        assert_eq!(
            should_sleep(
                &max_lifetime(),
                &record,
                &still_answering(TTL_MS as i64 + MAX_LIFETIME_DRAIN_MS),
                NOW_MS
            ),
            Some(SleepReason::LivedLongEnough)
        );
    }

    #[test]
    fn a_policy_that_never_sleeps_is_no_different_with_a_request_open() {
        let record = serving(InstanceState::Running);
        let answering = ActivitySignals {
            last_active_at_ms: Some(0),
            measured_at_ms: Some(NOW_MS),
            started_at_ms: Some(0),
            requests_open: 1,
        };
        assert_eq!(should_sleep(&never(), &record, &answering, NOW_MS), None);
    }

    #[test]
    fn an_app_nothing_has_been_observed_about_is_left_alone() {
        let record = serving(InstanceState::Running);
        assert_eq!(
            should_sleep(&traffic_idle(), &record, &ActivitySignals::default(), NOW_MS),
            None
        );
        assert_eq!(
            should_sleep(&max_lifetime(), &record, &ActivitySignals::default(), NOW_MS),
            None
        );
    }

    #[test]
    fn only_live_on_request_apps_this_host_still_wants_up_can_be_snapshotted() {
        let long_ago = quiet_since(0);
        for state in INSTANCE_STATES {
            let expected =
                matches!(state, InstanceState::Running | InstanceState::Frozen).then_some(SleepReason::Quiet);
            assert_eq!(
                should_sleep(&traffic_idle(), &serving(state), &long_ago, NOW_MS),
                expected,
                "{state:?}"
            );
        }

        let always_up = instance_record(|record| {
            record.on_request = false;
            record.desired_running = true;
            record.state = InstanceState::Running;
        });
        assert_eq!(should_sleep(&traffic_idle(), &always_up, &long_ago, NOW_MS), None);

        let suspended = instance_record(|record| {
            record.on_request = true;
            record.desired_running = false;
            record.state = InstanceState::Running;
        });
        assert_eq!(should_sleep(&traffic_idle(), &suspended, &long_ago, NOW_MS), None);
    }

    #[test]
    fn what_each_reason_is_called_is_what_the_report_reads() {
        assert_eq!(SleepReason::Quiet.as_str(), "idle");
        assert_eq!(SleepReason::LivedLongEnough.as_str(), "max-lifetime");
    }
}
