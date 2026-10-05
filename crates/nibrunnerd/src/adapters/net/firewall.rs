use std::collections::BTreeMap;
use std::sync::Arc;

use nft_render::{
    parse_app_traffic, parse_kernel_tables, render_ruleset, AppTraffic, FirewallState, KernelTables,
    NFTABLES_TABLE,
};
use protocol::AppId;
use tokio::sync::Mutex;

use crate::ports::{CommandError, CommandRequest, CommandRunner, CommandRunnerExt};

#[derive(Debug, Clone, PartialEq, Eq)]
struct Applied {
    ruleset: String,
    tables: KernelTables,
}

pub struct HostFirewall {
    commands: Arc<dyn CommandRunner>,
    applied: Mutex<Option<Applied>>,
}

impl HostFirewall {
    pub fn new(commands: Arc<dyn CommandRunner>) -> Self {
        Self {
            commands,
            applied: Mutex::new(None),
        }
    }

    async fn kernel_tables(&self) -> Option<KernelTables> {
        self.commands
            .stdout_of(CommandRequest::new(&["nft", "-j", "list", "tables"]))
            .await
            .ok()
            .map(|json| parse_kernel_tables(&json))
    }

    pub async fn apply(&self, state: &FirewallState) -> Result<(), CommandError> {
        let ruleset = render_ruleset(state);
        let mut applied = self.applied.lock().await;
        if let Some(last) = applied.as_ref() {
            if last.ruleset == ruleset {
                if let Some(current) = self.kernel_tables().await {
                    if current == last.tables {
                        return Ok(());
                    }
                }
            }
        }
        self.commands
            .stdout_of(CommandRequest::new(&["nft", "-f", "-"]).with_stdin(ruleset.clone()))
            .await?;
        let denied: Vec<&str> = state
            .denied_egress_addresses_v4
            .iter()
            .chain(&state.denied_egress_addresses_v6)
            .map(String::as_str)
            .collect();
        if !denied.is_empty() {
            tracing::info!(
                denied = denied.join(", "),
                "egress denies in place, flows already open to them included"
            );
        }
        *applied = self
            .kernel_tables()
            .await
            .map(|tables| Applied { ruleset, tables });
        Ok(())
    }

    pub async fn traffic(&self) -> Result<BTreeMap<AppId, AppTraffic>, CommandError> {
        let json = self
            .commands
            .stdout_of(CommandRequest::new(&[
                "nft",
                "-j",
                "list",
                "counters",
                "table",
                "ip",
                NFTABLES_TABLE,
            ]))
            .await?;
        Ok(parse_app_traffic(&json))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ports::CommandResult;
    use crate::test_support::mocks::{self, CommandLog};
    use nft_render::{app_received_counter_name, app_sent_counter_name, Counted, ForwardedInstance};
    use protocol::{HostPort, Ipv4Address};

    fn holding(handles: (u64, u64)) -> String {
        format!(
            r#"{{"nftables":[{{"table":{{"family":"ip","name":"nibrun","handle":{}}}}},{{"table":{{"family":"ip6","name":"nibrun","handle":{}}}}}]}}"#,
            handles.0, handles.1
        )
    }

    fn instance() -> ForwardedInstance {
        ForwardedInstance {
            app_id: AppId::parse("app-1").unwrap(),
            ports: vec![nft_render::ForwardedPort {
                host_port: HostPort::new(21_000).unwrap(),
                guest_port: protocol::GuestPort::new(3000).unwrap(),
                raw: false,
            }],
            host_ipv4: Ipv4Address::parse("10.201.0.1").unwrap(),
            guest_ipv4: Ipv4Address::parse("10.201.0.2").unwrap(),
        }
    }

    fn listing(answer: String) -> (Arc<crate::ports::MockCommandRunner>, CommandLog) {
        mocks::commands_answering(move |request| {
            if request.command.contains(&"list".to_string()) {
                Ok(CommandResult::with_stdout(answer.clone()))
            } else {
                Ok(CommandResult::succeeded())
            }
        })
    }

    fn writes(log: &CommandLog) -> usize {
        log.calls()
            .iter()
            .filter(|request| request.command.contains(&"-f".to_string()))
            .count()
    }

    #[tokio::test]
    async fn the_ruleset_is_piped_to_nft_whole_and_a_rerun_costs_nothing() {
        let (commands, log) = listing(holding((2, 4)));
        let firewall = HostFirewall::new(commands);
        let state = FirewallState {
            instances: vec![instance()],
            ..Default::default()
        };
        firewall.apply(&state).await.unwrap();
        let written = log
            .calls()
            .into_iter()
            .find(|request| request.command.contains(&"-f".to_string()))
            .unwrap();
        assert_eq!(written.command, vec!["nft", "-f", "-"]);
        assert!(written
            .stdin
            .as_deref()
            .unwrap()
            .starts_with("table ip nibrun\ndelete table ip nibrun"));

        firewall.apply(&state).await.unwrap();
        assert_eq!(
            writes(&log),
            1,
            "an unchanged ruleset the kernel still holds is not rewritten"
        );
    }

    #[tokio::test]
    async fn a_ruleset_something_else_dropped_is_written_again() {
        let flushed = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let (commands, log) = mocks::commands_answering({
            let flushed = flushed.clone();
            move |request| {
                if request.command.contains(&"list".to_string()) {
                    let answer = if flushed.load(std::sync::atomic::Ordering::SeqCst) {
                        r#"{"nftables":[]}"#.to_string()
                    } else {
                        holding((2, 4))
                    };
                    return Ok(CommandResult::with_stdout(answer));
                }
                Ok(CommandResult::succeeded())
            }
        });
        let firewall = HostFirewall::new(commands);
        let state = FirewallState::default();
        firewall.apply(&state).await.unwrap();
        flushed.store(true, std::sync::atomic::Ordering::SeqCst);
        firewall.apply(&state).await.unwrap();
        assert_eq!(writes(&log), 2);
    }

    #[tokio::test]
    async fn a_table_rebuilt_under_this_daemon_is_written_again() {
        let handles = Arc::new(std::sync::atomic::AtomicU64::new(2));
        let (commands, log) = mocks::commands_answering({
            let handles = handles.clone();
            move |request| {
                if request.command.contains(&"list".to_string()) {
                    let handle = handles.load(std::sync::atomic::Ordering::SeqCst);
                    return Ok(CommandResult::with_stdout(holding((handle, handle + 2))));
                }
                Ok(CommandResult::succeeded())
            }
        });
        let firewall = HostFirewall::new(commands);
        firewall.apply(&FirewallState::default()).await.unwrap();
        handles.store(8, std::sync::atomic::Ordering::SeqCst);
        firewall.apply(&FirewallState::default()).await.unwrap();
        assert_eq!(writes(&log), 2);
    }

    #[tokio::test]
    async fn a_kernel_that_would_not_answer_is_not_taken_as_proof_the_rules_are_in_place() {
        let (commands, log) = mocks::commands_answering(|request| {
            if request.command.contains(&"list".to_string()) {
                Err(CommandError::Unstartable {
                    executable: "nft".into(),
                    reason: "not found".into(),
                })
            } else {
                Ok(CommandResult::succeeded())
            }
        });
        let firewall = HostFirewall::new(commands.clone());
        firewall.apply(&FirewallState::default()).await.unwrap();
        firewall.apply(&FirewallState::default()).await.unwrap();
        assert_eq!(writes(&log), 2);
    }

    #[tokio::test]
    async fn what_the_kernel_counted_is_read_back_per_app() {
        let app = AppId::parse("app-1").unwrap();
        let counted = |name: String, bytes: u64| {
            format!(
                r#"{{"counter":{{"family":"ip","name":"{name}","table":"nibrun","handle":2,"packets":3,"bytes":{bytes}}}}}"#
            )
        };
        // One listing carries both ways, so metering each apart costs no second call.
        let counters = format!(
            r#"{{"nftables":[{},{}]}}"#,
            counted(app_received_counter_name(&app), 512),
            counted(app_sent_counter_name(&app), 4096)
        );
        let firewall = HostFirewall::new(listing(counters).0);
        assert_eq!(
            firewall.traffic().await.unwrap().get(&app),
            Some(&AppTraffic {
                received: Counted {
                    packets: 3,
                    bytes: 512
                },
                sent: Counted {
                    packets: 3,
                    bytes: 4096
                },
                activity: Counted::default(),
            })
        );
    }

    #[tokio::test]
    async fn a_ruleset_the_kernel_would_not_take_is_handed_up_and_never_remembered_as_applied() {
        let (commands, log) = mocks::commands_answering(|request| {
            if request.command.contains(&"-f".to_string()) {
                Err(CommandError::Unstartable {
                    executable: "nft".into(),
                    reason: "not found".into(),
                })
            } else {
                Ok(CommandResult::with_stdout(holding((2, 4))))
            }
        });
        let firewall = HostFirewall::new(commands);
        let state = FirewallState {
            instances: vec![instance()],
            ..Default::default()
        };
        let error = firewall.apply(&state).await.unwrap_err();
        assert!(matches!(error, CommandError::Unstartable { .. }), "{error}");
        firewall.apply(&state).await.unwrap_err();
        assert_eq!(writes(&log), 2, "a ruleset that never landed is written again");
    }

    #[tokio::test]
    async fn counters_this_host_cannot_read_are_a_failure_rather_than_an_empty_reading() {
        let (commands, _) = mocks::commands_answering(|_| {
            Err(CommandError::TimedOut {
                executable: "nft".into(),
            })
        });
        let error = HostFirewall::new(commands).traffic().await.unwrap_err();
        assert!(matches!(error, CommandError::TimedOut { .. }), "{error}");
    }

    #[tokio::test]
    async fn a_counter_listing_that_is_not_json_reads_as_no_traffic_rather_than_a_failure() {
        let firewall = HostFirewall::new(listing("not json at all".to_string()).0);
        assert!(firewall.traffic().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn a_ruleset_that_changed_is_written_even_when_the_kernel_still_holds_the_old_one() {
        let (commands, log) = listing(holding((2, 4)));
        let firewall = HostFirewall::new(commands);
        firewall.apply(&FirewallState::default()).await.unwrap();
        firewall
            .apply(&FirewallState {
                instances: vec![instance()],
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(writes(&log), 2);
    }
}
