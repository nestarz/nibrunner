use guest_contract::instance_env::InstanceConfig;
use protocol::{StateMessage, TenantExit, TenantRestart};

pub(crate) const SHUTDOWN_GRACE_MS: u32 = 10_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Outcome {
    ShutdownRequested,
    RestartBudgetExhausted,
    SpawnFailed,
}

/// The restarts a tenant has spent of the budget its document gave it. A tenant that stays up
/// for `reset_after_ms` earns the whole budget back.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Budget {
    spent: u32,
}

impl Budget {
    /// The tenant exited after `uptime_ms`, `because` being what the runtime knows about it that
    /// the exit does not say. Hands back the restart to report and wait out, or nothing once the
    /// budget is spent.
    pub(crate) fn exited(
        &mut self,
        config: &InstanceConfig,
        uptime_ms: u64,
        exit: TenantExit,
        because: &str,
    ) -> Option<TenantRestart> {
        if budget_resets(config, uptime_ms) {
            self.spent = 0;
        }
        if self.spent >= config.max_restarts {
            return None;
        }
        let backoff_ms = backoff_ms(config, self.spent);
        self.spent += 1;
        Some(TenantRestart {
            attempt: self.spent,
            budget: config.max_restarts,
            exit,
            reason: StateMessage::new(format!(
                "the tenant exited ({}){because}; restart {} of {} in {backoff_ms}ms",
                exit.status(),
                self.spent,
                config.max_restarts
            )),
            backoff_ms: u64::from(backoff_ms),
        })
    }
}

pub(crate) fn backoff_ms(config: &InstanceConfig, restart_count: u32) -> u32 {
    let mut delay = f64::from(config.initial_backoff_ms);
    for _ in 0..restart_count {
        delay *= config.backoff_factor;
        if delay >= f64::from(config.max_backoff_ms) {
            return config.max_backoff_ms;
        }
    }
    delay.min(f64::from(config.max_backoff_ms)) as u32
}

pub(crate) fn budget_resets(config: &InstanceConfig, uptime_ms: u64) -> bool {
    uptime_ms >= u64::from(config.reset_after_ms)
}

/// The document's defaults, for the tests here and the supervisor's.
#[cfg(test)]
pub(crate) fn config(edit: impl FnOnce(&mut InstanceConfig)) -> InstanceConfig {
    let mut value = InstanceConfig {
        http_port: 3000,
        layers: 1,
        writable: guest_contract::paths::WritableRoot::VolumeDrive,
        program: "/app/server".to_string(),
        working_directory: "/app".to_string(),
        hostname: None,
        max_restarts: 5,
        initial_backoff_ms: 500,
        max_backoff_ms: 30_000,
        backoff_factor: 2.0,
        reset_after_ms: 60_000,
        nameservers: vec![],
        arguments: vec![],
        environment: vec![],
    };
    edit(&mut value);
    value
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_first_restart_waits_rather_than_going_straight_back() {
        assert_eq!(backoff_ms(&config(|_| {}), 0), 500);
    }

    #[test]
    fn each_restart_waits_longer_until_the_cap() {
        let config = config(|_| {});
        assert_eq!(backoff_ms(&config, 1), 1_000);
        assert_eq!(backoff_ms(&config, 2), 2_000);
        assert_eq!(backoff_ms(&config, 3), 4_000);
        assert_eq!(backoff_ms(&config, 20), 30_000, "capped, not overflowed");
    }

    #[test]
    fn the_delay_is_a_function_of_the_count_and_not_of_the_history() {
        let config = config(|_| {});
        for count in 0..10 {
            assert_eq!(backoff_ms(&config, count), backoff_ms(&config, count));
        }
    }

    #[test]
    fn a_factor_that_does_not_grow_is_a_fixed_delay() {
        let config = config(|config| config.backoff_factor = 1.0);
        for count in 0..10 {
            assert_eq!(backoff_ms(&config, count), 500);
        }
    }

    #[test]
    fn a_tenant_that_stayed_up_earns_its_budget_back() {
        let config = config(|_| {});
        assert!(!budget_resets(&config, 59_999));
        assert!(budget_resets(&config, 60_000));
    }

    const OOM: &str = ": the kernel killed it for running out of memory at its ceiling of 198 MiB";

    #[test]
    fn a_restart_carries_what_the_guest_prints_and_the_figures_it_printed_it_from() {
        let config = config(|_| {});
        let mut budget = Budget::default();
        let restart = budget.exited(&config, 1_000, TenantExit::Signal(9), OOM).unwrap();
        assert_eq!(
            restart,
            TenantRestart {
                attempt: 1,
                budget: 5,
                exit: TenantExit::Signal(9),
                reason: StateMessage::new(format!("the tenant exited (137){OOM}; restart 1 of 5 in 500ms")),
                backoff_ms: 500,
            }
        );
        let second = budget.exited(&config, 1_000, TenantExit::Code(1), "").unwrap();
        assert_eq!((second.attempt, second.backoff_ms), (2, 1_000));
        assert_eq!(
            second.reason.as_str(),
            "the tenant exited (1); restart 2 of 5 in 1000ms"
        );
    }

    #[test]
    fn the_exit_that_spends_the_budget_is_not_a_restart() {
        let config = config(|config| config.max_restarts = 2);
        let mut budget = Budget::default();
        assert!(budget.exited(&config, 0, TenantExit::Code(1), "").is_some());
        assert!(budget.exited(&config, 0, TenantExit::Code(1), "").is_some());
        assert_eq!(budget.exited(&config, 0, TenantExit::Code(1), ""), None);
        assert_eq!(
            budget.exited(&config, 0, TenantExit::Code(1), ""),
            None,
            "and stays spent"
        );
    }

    #[test]
    fn a_tenant_that_stayed_up_starts_its_count_over() {
        let config = config(|config| config.max_restarts = 2);
        let mut budget = Budget::default();
        budget.exited(&config, 0, TenantExit::Code(1), "");
        budget.exited(&config, 0, TenantExit::Code(1), "");
        let earned_back = budget.exited(&config, 60_000, TenantExit::Code(1), "").unwrap();
        assert_eq!((earned_back.attempt, earned_back.backoff_ms), (1, 500));
    }
}
