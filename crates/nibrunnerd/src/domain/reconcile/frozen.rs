use protocol::{ActivationPolicy, AppId, InstanceState, SleepPolicy};

use crate::domain::activation::ActivitySignals;
use crate::domain::report::InstanceRecord;
use crate::host::Host;

const MIN_QUIET_MS: i64 = 30_000;
const COOLDOWN_MS: i64 = 60_000;
const FREEZES_PER_PASS: usize = 4;

fn eligible(
    record: &InstanceRecord,
    policy: &ActivationPolicy,
    signals: &ActivitySignals,
    after_ms: i64,
    now: i64,
) -> bool {
    let SleepPolicy::TrafficIdle { timeout_ms } = policy.sleep_when else {
        return false;
    };
    let deadline = i64::try_from(timeout_ms.get()).unwrap_or(i64::MAX);
    record.on_request
        && record.desired_running
        && record.state == InstanceState::Running
        // Established raw flows bypass proxy request accounting and cannot be safely paused yet.
        && record.ports.is_empty()
        && signals.requests_open == 0
        && after_ms < deadline
        // A reading from the future is a clock that moved, not a reading.
        && signals
            .measured_at_ms
            .is_some_and(|at| at <= now && signals.measured_lately(now))
        && signals
            .started_at_ms
            .is_some_and(|at| now.saturating_sub(at) >= COOLDOWN_MS)
        && signals
            .last_active_at_ms
            .is_some_and(|at| (after_ms..deadline).contains(&now.saturating_sub(at)))
}

async fn freeze_one(host: &Host, app: &AppId, after_ms: i64) -> bool {
    let Some(held) = host.state.try_transition(app) else {
        return false;
    };
    let policy = host.cache.lock().await.latest().and_then(|desired| {
        desired
            .instances
            .iter()
            .find(|instance| &instance.app_id == app)
            .map(|instance| instance.activation())
    });
    let Some(policy) = policy else {
        return false;
    };
    let now = crate::clock::now_ms();
    let mut snapshot = host.state.locked_snapshot().await;
    let Some(record) = snapshot.records.get(app) else {
        return false;
    };
    let signals = super::idle::signals(&snapshot, record, host.metrics.proxy.open_requests_for(app));
    if snapshot.snapshotting.contains(app)
        || snapshot
            .freeze_attempted_at_ms
            .get(app)
            .is_some_and(|at| now.saturating_sub(*at) < COOLDOWN_MS)
        || !eligible(record, &policy, &signals, after_ms, now)
    {
        return false;
    }
    snapshot.freeze_attempted_at_ms.insert(app.clone(), now);
    snapshot.snapshotting.insert(app.clone());
    drop(snapshot);
    freeze_marked(host, app, held).await;
    true
}

/// Freezes an app already marked as moving, so that the ruleset hands its port to the activator
/// before the guest stops answering. The caller holds its transition and has marked it.
pub(super) async fn freeze_marked(host: &Host, app: &AppId, _held: tokio::sync::OwnedMutexGuard<()>) -> bool {
    super::network::apply_network(host).await;
    if !host.state.snapshot().await.isolated {
        tracing::warn!(%app, "freeze waits until incoming traffic can reach the activator");
        host.state.mark_snapshotting(app, false).await;
        return false;
    }
    host.router.close_connections_to(app).await;
    let result = host.vms.freeze(app).await;
    let frozen = result.is_ok()
        || host
            .vms
            .statuses(std::slice::from_ref(app))
            .await
            .get(app)
            .is_some_and(|status| status.frozen);
    if frozen {
        host.state
            .update_record(app, |record| {
                record.state = InstanceState::Frozen;
                record.message = None;
            })
            .await;
        tracing::info!(%app, "quiet workload frozen");
    }
    if let Err(error) = result {
        tracing::warn!(%app, error = %error.message(), "workload freeze was incomplete");
    }
    host.state.mark_snapshotting(app, false).await;
    super::network::apply_network(host).await;
    host.state.signal_report();
    frozen
}

pub(crate) async fn apply(host: &Host) {
    let Some(after_ms) = host
        .config
        .memory_admission
        .as_ref()
        .and_then(|policy| policy.freeze_after_ms)
    else {
        return;
    };
    let after_ms = i64::try_from(after_ms.get())
        .unwrap_or(i64::MAX)
        .max(MIN_QUIET_MS);
    host.state
        .modify(|snapshot| {
            snapshot
                .freeze_attempted_at_ms
                .retain(|app, _| snapshot.records.contains_key(app))
        })
        .await;
    let snapshot = host.state.snapshot().await;
    let mut candidates: Vec<_> = snapshot
        .records
        .values()
        .filter(|record| record.state == InstanceState::Running)
        .map(|record| {
            (
                snapshot.last_active_at_ms.get(&record.app_id).copied(),
                record.app_id.clone(),
            )
        })
        .collect();
    candidates.sort();
    let mut attempted = 0;
    for (_, app) in candidates {
        if freeze_one(host, &app, after_ms).await {
            attempted += 1;
            if attempted >= FREEZES_PER_PASS {
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ports::{VmCall, WakeOutcome};
    use crate::test_support::*;
    use protocol::Timestamp;

    fn on_request() -> protocol::DesiredInstance {
        desired_instance(|instance| instance.desired_state = protocol::DesiredInstanceState::OnRequest)
    }

    async fn quiet_host() -> TestHost {
        let mut host = test_host().await;
        std::sync::Arc::get_mut(&mut host.host)
            .unwrap()
            .config
            .memory_admission = Some(crate::config::MemoryAdmission {
            pool: None,
            mode: crate::config::MemoryAdmissionMode::Observe,
            freeze_after_ms: Some(60_000.try_into().unwrap()),
            reclaim: false,
            headroom_mib: 1024.try_into().unwrap(),
        });
        let now = crate::clock::now_ms();
        host.slot_for(&app_id()).await.unwrap();
        host.volumes.provision(&desired_volume(|_| {})).await.unwrap();
        host.state
            .put_record(instance_record(|record| {
                record.on_request = true;
                record.started_at = Some(Timestamp::from_epoch_ms(now - 120_000));
            }))
            .await;
        host.cache
            .lock()
            .await
            .accept(desired_state(|state| state.instances = vec![on_request()]));
        host.vms.set_status(crate::adapters::vm::VmStatus {
            active: true,
            ..Default::default()
        });
        measured_quiet_since(&host.state, &app_id(), now - 90_000).await;
        host
    }

    #[tokio::test]
    async fn a_quiet_app_freezes_keeps_its_memory_accounted_and_thaws_without_restore() {
        let host = quiet_host().await;
        apply(&host).await;
        let _transition = host.state.transition(&app_id()).await;
        let frozen = host.state.record(&app_id()).await.unwrap();
        assert_eq!(frozen.state, InstanceState::Frozen);
        assert!(crate::domain::report::capacity::holds_something(&frozen));
        assert!(super::super::network::forwarded_instances(&host).await.is_empty());
        host.persist().await;
        assert_eq!(
            host.repositories.instances.all().await.unwrap()[0].state,
            InstanceState::Frozen
        );
        assert_eq!(
            super::super::instances::resume_instance(&host, &on_request())
                .await
                .unwrap(),
            WakeOutcome::Thawed
        );
        assert_eq!(host.vms.calls(), vec![VmCall::Freeze, VmCall::Thaw]);
        assert_eq!(
            host.state.record(&app_id()).await.unwrap().state,
            InstanceState::Starting
        );
    }

    #[tokio::test]
    async fn an_open_request_and_a_recently_arrived_request_each_prevent_freezing() {
        let host = quiet_host().await;
        let request = host.metrics.proxy.open(&app_id());
        apply(&host).await;
        assert!(host.vms.calls().is_empty());
        drop(request);
        host.state.mark_active(&app_id(), crate::clock::now_ms()).await;
        apply(&host).await;
        assert!(host.vms.calls().is_empty());
    }

    #[tokio::test]
    async fn a_frozen_app_still_reaches_its_original_snapshot_deadline() {
        let host = quiet_host().await;
        apply(&host).await;
        drop(host.state.transition(&app_id()).await);
        measured_quiet_since(
            &host.state,
            &app_id(),
            crate::clock::now_ms() - protocol::DEFAULT_IDLE_TIMEOUT_MS as i64 - 1,
        )
        .await;
        host.state
            .modify(|snapshot| {
                snapshot.last_measured_at_ms.remove(&app_id());
            })
            .await;
        super::super::idle::apply_sleep(host.arc()).await;
        assert_eq!(
            host.state.record(&app_id()).await.unwrap().state,
            InstanceState::Idle
        );
        assert_eq!(host.vms.calls(), vec![VmCall::Freeze, VmCall::Sleep]);
    }

    #[tokio::test]
    async fn declared_background_work_is_not_frozen() {
        let host = quiet_host().await;
        host.cache.lock().await.accept(desired_state(|state| {
            state.instances = vec![desired_instance(|instance| {
                instance.desired_state = protocol::DesiredInstanceState::OnRequest;
                instance.activation = Some(ActivationPolicy {
                    cutover_timeout_ms: None,
                    snapshot_ttl_ms: None,
                    first_boot: protocol::FirstBoot::Eager,
                    sleep_when: SleepPolicy::Never,
                });
            })]
        }));
        apply(&host).await;
        assert!(host.vms.calls().is_empty());
    }

    #[test]
    fn freezing_requires_fresh_quiet_observations_and_a_settled_start() {
        let now = 1_000_000;
        let record = instance_record(|record| record.on_request = true);
        let policy = on_request().activation();
        let quiet = ActivitySignals {
            last_active_at_ms: Some(now - 90_000),
            measured_at_ms: Some(now),
            started_at_ms: Some(now - 120_000),
            requests_open: 0,
        };
        assert!(eligible(&record, &policy, &quiet, 60_000, now));
        for signals in [
            ActivitySignals {
                measured_at_ms: None,
                ..quiet
            },
            ActivitySignals {
                measured_at_ms: Some(now - 15_001),
                ..quiet
            },
            ActivitySignals {
                measured_at_ms: Some(now + 1),
                ..quiet
            },
            ActivitySignals {
                last_active_at_ms: None,
                ..quiet
            },
            ActivitySignals {
                last_active_at_ms: Some(now - 59_999),
                ..quiet
            },
            ActivitySignals {
                last_active_at_ms: Some(now + 1),
                ..quiet
            },
            ActivitySignals {
                started_at_ms: Some(now - 59_999),
                ..quiet
            },
            ActivitySignals {
                requests_open: 1,
                ..quiet
            },
        ] {
            assert!(!eligible(&record, &policy, &signals, 60_000, now), "{signals:?}");
        }
        assert!(!eligible(&record, &policy, &quiet, i64::MAX, now));
        let mut raw = record;
        raw.ports
            .push(crate::domain::report::instance_record::RecordPort {
                name: protocol::PortName::parse("ssh").unwrap(),
                host_port: protocol::HostPort::new(21_001).unwrap(),
                guest_port: protocol::GuestPort::new(22).unwrap(),
            });
        assert!(!eligible(&raw, &policy, &quiet, 60_000, now));
    }

    #[tokio::test]
    async fn changing_to_background_work_thaws_the_existing_guest() {
        let host = quiet_host().await;
        apply(&host).await;
        drop(host.state.transition(&app_id()).await);
        let desired = desired_state(|state| {
            state.instances = vec![desired_instance(|instance| {
                instance.desired_state = protocol::DesiredInstanceState::OnRequest;
                instance.activation = Some(ActivationPolicy {
                    cutover_timeout_ms: None,
                    snapshot_ttl_ms: None,
                    first_boot: protocol::FirstBoot::Eager,
                    sleep_when: SleepPolicy::Never,
                });
            })];
        });
        super::super::sync_desired(host.arc(), &desired).await;
        assert_eq!(host.vms.calls(), vec![VmCall::Freeze, VmCall::Thaw]);
        assert_eq!(
            host.state.record(&app_id()).await.unwrap().state,
            InstanceState::Starting
        );
    }

    #[tokio::test]
    async fn a_frozen_guest_that_will_not_thaw_is_let_go_of_and_the_app_booted_afresh() {
        let host = quiet_host().await;
        apply(&host).await;
        let _transition = host.state.transition(&app_id()).await;
        host.vms.refuse_thaw(crate::ports::VmError::WakeStalled {
            reason: "dial did not complete: CONNECT 51001 had no answer within 1000 ms".into(),
        });
        assert_eq!(
            super::super::instances::resume_instance(&host, &on_request())
                .await
                .unwrap(),
            WakeOutcome::ColdBoot
        );
        let [.., thaw_failed] = host.metrics.sleep_wake.of(&app_id()).wakes;
        assert_eq!(thaw_failed, 1, "the failed thaw is counted as its own outcome");
        assert_eq!(
            host.vms.calls(),
            vec![
                VmCall::Freeze,
                VmCall::Thaw,
                VmCall::Stop,
                VmCall::Discard,
                VmCall::Boot
            ],
            "the paused guest is taken down before another is booted"
        );
        let record = host.state.record(&app_id()).await.unwrap();
        assert_eq!(record.state, InstanceState::Starting);
        assert!(!record.stop_requested);
        assert_eq!(
            record.message.as_ref().map(protocol::StateMessage::as_str),
            Some("dial did not complete: CONNECT 51001 had no answer within 1000 ms"),
            "the instance says why the request was not thawed"
        );
    }

    #[tokio::test]
    async fn a_frozen_guest_of_a_deployment_being_replaced_is_left_to_the_pass_when_it_will_not_thaw() {
        let host = quiet_host().await;
        apply(&host).await;
        let _transition = host.state.transition(&app_id()).await;
        host.vms.refuse_thaw(crate::ports::VmError::Host(
            "guest clock synchronization timed out".into(),
        ));
        let redeployed = desired_instance(|instance| {
            instance.desired_state = protocol::DesiredInstanceState::OnRequest;
            instance.deployment_id = protocol::DeploymentId::parse("dep-2").unwrap();
        });
        assert!(super::super::instances::resume_instance(&host, &redeployed)
            .await
            .is_err());
        assert_eq!(host.vms.calls(), vec![VmCall::Freeze, VmCall::Thaw]);
    }

    fn named(hostnames: &[&str]) -> protocol::HostDesiredState {
        desired_state(|state| {
            state.instances = vec![desired_instance(|instance| {
                instance.desired_state = protocol::DesiredInstanceState::OnRequest;
                instance.hostnames = hostnames
                    .iter()
                    .map(|name| protocol::AppHostname {
                        hostname: protocol::Hostname::parse(*name).unwrap(),
                        kind: protocol::AppHostnameKind::Custom,
                    })
                    .chain([app_hostname()])
                    .collect();
            })]
        })
    }

    async fn renamed_while_frozen(host: &TestHost) {
        apply(host).await;
        for added in [&["app-test.example.com"][..], &[]] {
            let desired = named(added);
            host.cache.lock().await.accept(desired.clone());
            super::super::reconcile(
                host.arc(),
                &desired,
                crate::domain::metrics::passes::Trigger::Change,
            )
            .await;
            let routes = host.router.routes().await;
            let port = host.slot_of(&app_id()).await.unwrap().host_port;
            assert_eq!(routes.port_for(app_hostname().hostname.as_str()), Some(port));
            assert_eq!(
                routes.port_for("app-test.example.com"),
                (!added.is_empty()).then_some(port),
                "the route follows the document while the app is frozen"
            );
            assert_eq!(
                host.state.record(&app_id()).await.unwrap().state,
                InstanceState::Frozen
            );
        }
    }

    #[tokio::test]
    async fn a_frozen_app_given_a_hostname_and_then_its_old_ones_is_served_by_the_next_request() {
        let host = quiet_host().await;
        let listener = tokio::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
            .await
            .unwrap();
        let port = protocol::HttpPort::new(listener.local_addr().unwrap().port()).unwrap();
        tokio::spawn(async move { while listener.accept().await.is_ok() {} });
        renamed_while_frozen(&host).await;
        // After the passes, which set the port the document names: the guest answers on ours.
        host.state
            .update_record(&app_id(), |record| {
                record.guest_ipv4 = Some(crate::domain::health::probe::loopback());
                record.http_port = port;
                record.health_check = protocol::HealthCheck::BootCompleted;
            })
            .await;

        let waker = crate::domain::waker::AppWaker::new(host.arc().clone());
        let woke = crate::ports::Waker::wake(waker.as_ref(), &app_id()).await;
        assert!(woke.is_ok(), "{woke:?}");
        assert_eq!(host.vms.calls(), vec![VmCall::Freeze, VmCall::Thaw]);
        assert_eq!(
            host.state.record(&app_id()).await.unwrap().state,
            InstanceState::Starting
        );
    }

    #[tokio::test]
    async fn a_frozen_app_renamed_twice_whose_guest_will_not_thaw_is_booted_by_the_next_request() {
        let host = quiet_host().await;
        renamed_while_frozen(&host).await;
        host.vms.refuse_thaw(crate::ports::VmError::Host(
            "guest clock synchronization timed out".into(),
        ));
        let _transition = host.state.transition(&app_id()).await;
        assert_eq!(
            super::super::instances::resume_instance(&host, &named(&[]).instances[0])
                .await
                .unwrap(),
            WakeOutcome::ColdBoot
        );
        let record = host.state.record(&app_id()).await.unwrap();
        assert_eq!(record.state, InstanceState::Starting);
        assert_eq!(record.hostnames, vec![app_hostname()]);
    }
}
