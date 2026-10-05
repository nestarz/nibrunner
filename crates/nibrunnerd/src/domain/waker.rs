use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use protocol::{AppId, DesiredInstance, DesiredInstanceState, InstanceState};
use tokio::sync::broadcast;

use crate::domain::health::is_within_grace_period;
use crate::domain::health::probe::probe_instance;
use crate::host::Host;
use crate::ports::{WakeFailure, WakeRefusal, Waker};

const PROBE_INTERVAL: Duration = Duration::from_millis(5);

type Outcome = Result<(), WakeRefusal>;
type Coalescing = BTreeMap<AppId, (broadcast::Sender<Outcome>, u64)>;
type InFlight = Mutex<Coalescing>;

fn lock(in_flight: &InFlight) -> std::sync::MutexGuard<'_, Coalescing> {
    in_flight.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

pub struct AppWaker {
    host: Arc<Host>,
    in_flight: InFlight,
}

impl AppWaker {
    pub fn new(host: Arc<Host>) -> Arc<Self> {
        Arc::new(Self {
            host,
            in_flight: Mutex::new(BTreeMap::new()),
        })
    }

    async fn reserve_room(
        &self,
        app_id: &AppId,
        wanted: &protocol::InstanceResources,
    ) -> Result<crate::state::MemoryReservation, WakeRefusal> {
        self.host
            .reserve_memory(app_id, *wanted)
            .await
            .map_err(|shortfall_mib| WakeRefusal::NoRoom { shortfall_mib })
    }

    async fn boot(&self, app_id: &AppId) -> Outcome {
        let started = Instant::now();
        let wanted = {
            let cache = self.host.cache.lock().await;
            cache
                .latest()
                .and_then(|desired| {
                    desired
                        .instances
                        .iter()
                        .find(|instance| &instance.app_id == app_id)
                })
                .cloned()
        };
        let Some(wanted) = wanted else {
            return Err(WakeRefusal::Failed {
                kind: WakeFailure::NotNamed,
                reason: "the control plane no longer names it".into(),
            });
        };
        if wanted.desired_state == DesiredInstanceState::Stopped {
            return Err(WakeRefusal::Failed {
                kind: WakeFailure::NotOnRequest,
                reason: format!("it is {}", wanted.desired_state.as_str()),
            });
        }
        if !self.host.state.snapshot().await.isolated {
            return Err(WakeRefusal::Failed {
                kind: WakeFailure::NotIsolated,
                reason: "the isolation ruleset is not applied".into(),
            });
        }
        if wanted.desired_state == DesiredInstanceState::Running {
            return self.hold(app_id, &wanted).await;
        }
        // A request arriving during sleep waits for the completed snapshot before reserving RAM.
        let transition = self.host.state.transition(app_id).await;
        let reservation = match self.reserve_room(app_id, &wanted.config.resources).await {
            Ok(reservation) => reservation,
            Err(refusal) => {
                if let WakeRefusal::NoRoom { shortfall_mib } = &refusal {
                    let message = format!(
                    "{app_id} could not be woken: its host is {shortfall_mib} MiB short of the memory it needs"
                );
                    self.host
                        .state
                        .update_record(app_id, |record| {
                            record.message = Some(protocol::StateMessage::new(message));
                        })
                        .await;
                }
                return Err(refusal);
            }
        };

        let outcome = crate::domain::reconcile::instances::resume_instance(&self.host, &wanted).await?;
        drop(reservation);
        drop(transition);

        let Some(record) = self.host.state.record(app_id).await else {
            return Err(WakeRefusal::Failed {
                kind: WakeFailure::WouldNotStart,
                reason: "the microVM would not start".into(),
            });
        };
        if record.started_at.is_none() {
            let reason = record.message.as_ref().map_or_else(
                || "the microVM would not start".to_string(),
                |message| message.as_str().to_string(),
            );
            return Err(WakeRefusal::Failed {
                kind: WakeFailure::WouldNotStart,
                reason,
            });
        }
        let restored = started.elapsed();

        // A caller is handed on once the guest is ready for it, and what ready means is the
        // instance's to say: a port that answers its check, or one that accepts a connection at
        // all — either way, a port a request can be forwarded to.
        let deadline = Instant::now() + Duration::from_millis(record.health_check.probe().grace_period_ms);
        loop {
            if probe_instance(&record.guest_ipv4, record.http_port, &record.health_check)
                .await
                .is_ok()
            {
                break;
            }
            if Instant::now() >= deadline {
                return Err(WakeRefusal::Failed {
                    kind: WakeFailure::NeverAnswered,
                    reason: format!("nothing answered on port {} inside the guest", record.http_port),
                });
            }
            tokio::time::sleep(PROBE_INTERVAL).await;
        }
        let ready = started.elapsed() - restored;

        self.host.state.signal_refresh();
        self.host
            .metrics
            .sleep_wake
            .woken(app_id, outcome, started.elapsed(), ready);
        let joined = lock(&self.in_flight).get(app_id).map_or(0, |(_, count)| *count);
        tracing::debug!(
            %app_id,
            outcome = outcome.as_str(),
            waited_ms = started.elapsed().as_millis(),
            restored_ms = restored.as_millis(),
            ready_ms = ready.as_millis(),
            coalesced = joined,
            "app woken by a request"
        );
        Ok(())
    }

    /// A running app is booted by the pass, not by a request, so a request only reaches here
    /// while the pass is between guests: the last one is being stopped for its replacement, or
    /// exited and is being booted again. The request waits for the guest that is coming the way
    /// a wake waits for a restored one, unless the record says none is. The grace period is
    /// counted from the boot, as the status loop counts it, so a slow stop does not eat into it
    /// and a guest that has already had it is not given it again; until there is a boot to count
    /// from, it is counted from the request.
    async fn hold(&self, app_id: &AppId, wanted: &DesiredInstance) -> Outcome {
        let arrived = Instant::now();
        let grace = Duration::from_millis(wanted.config.health_check.probe().grace_period_ms);
        loop {
            let record = self.host.state.record(app_id).await;
            if let Some(failed) = record
                .as_ref()
                .filter(|record| record.state == InstanceState::Failed)
            {
                let reason = failed.message.as_ref().map_or_else(
                    || "the microVM would not start".to_string(),
                    |message| message.as_str().to_string(),
                );
                return Err(WakeRefusal::Failed {
                    kind: WakeFailure::WouldNotStart,
                    reason,
                });
            }
            let booted = record
                .as_ref()
                .filter(|record| record.started_at.is_some() && !record.stop_requested);
            match booted {
                Some(record) => {
                    if probe_instance(&record.guest_ipv4, record.http_port, &record.health_check)
                        .await
                        .is_ok()
                    {
                        let joined = lock(&self.in_flight).get(app_id).map_or(0, |(_, count)| *count);
                        tracing::debug!(
                            %app_id,
                            waited_ms = arrived.elapsed().as_millis(),
                            coalesced = joined,
                            "app answered the request held through its boot"
                        );
                        return Ok(());
                    }
                    if !is_within_grace_period(&record.grace_inputs(crate::clock::now_ms())) {
                        return Err(WakeRefusal::Failed {
                            kind: WakeFailure::NeverAnswered,
                            reason: format!("nothing answered on port {} inside the guest", record.http_port),
                        });
                    }
                }
                None if arrived.elapsed() >= grace => {
                    return Err(WakeRefusal::Failed {
                        kind: WakeFailure::WouldNotStart,
                        reason: "no microVM came up for it in time".into(),
                    });
                }
                None => {}
            }
            tokio::time::sleep(PROBE_INTERVAL).await;
        }
    }
}

enum Role<'a> {
    Follower(broadcast::Receiver<Outcome>),
    Leader(Leader<'a>),
}

/// A leader owns its app's `in_flight` entry until it broadcasts the boot's outcome to the
/// followers. Should the leader's future be dropped before then — its caller gave up mid-boot —
/// the entry is still released and the followers told, so the next wake of this app leads a fresh
/// one instead of subscribing to a sender that would never fire.
struct Leader<'a> {
    in_flight: &'a InFlight,
    app_id: &'a AppId,
    settled: bool,
}

impl<'a> Leader<'a> {
    fn new(in_flight: &'a InFlight, app_id: &'a AppId) -> Self {
        Self {
            in_flight,
            app_id,
            settled: false,
        }
    }

    fn settle(mut self, outcome: Outcome) {
        self.broadcast(outcome);
        self.settled = true;
    }

    fn broadcast(&self, outcome: Outcome) {
        if let Some((sender, _)) = lock(self.in_flight).remove(self.app_id) {
            let _ = sender.send(outcome);
        }
    }
}

impl Drop for Leader<'_> {
    fn drop(&mut self) {
        if !self.settled {
            self.broadcast(Err(WakeRefusal::Failed {
                kind: WakeFailure::Abandoned,
                reason: "the wake was abandoned".into(),
            }));
        }
    }
}

#[async_trait::async_trait]
impl Waker for AppWaker {
    async fn wake(&self, app_id: &AppId) -> Result<(), WakeRefusal> {
        let started = Instant::now();
        let role = {
            let mut in_flight = lock(&self.in_flight);
            match in_flight.get_mut(app_id) {
                Some((sender, count)) => {
                    *count += 1;
                    Role::Follower(sender.subscribe())
                }
                None => {
                    let (sender, _) = broadcast::channel(1);
                    in_flight.insert(app_id.clone(), (sender, 0));
                    Role::Leader(Leader::new(&self.in_flight, app_id))
                }
            }
        };
        let metrics = &self.host.metrics.sleep_wake;
        match role {
            Role::Follower(mut waiting) => {
                let outcome = match waiting.recv().await {
                    Ok(outcome) => outcome,
                    Err(_) => Err(WakeRefusal::Failed {
                        kind: WakeFailure::Abandoned,
                        reason: "the wake was abandoned".into(),
                    }),
                };
                metrics.woke(started.elapsed(), true);
                outcome
            }
            Role::Leader(leader) => {
                let outcome = self.boot(app_id).await;
                if let Err(refusal) = &outcome {
                    metrics.wake_refused(refusal);
                }
                metrics.woke(started.elapsed(), false);
                leader.settle(outcome.clone());
                outcome
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::*;
    use protocol::InstanceState;

    #[tokio::test]
    async fn concurrent_requests_to_one_app_cause_one_wake() {
        let host = test_host().await;
        host.volumes.provision(&desired_volume(|_| {})).await.unwrap();
        host.state.modify(|snapshot| snapshot.isolated = true).await;
        host.state
            .put_record(instance_record(|record| {
                record.on_request = true;
                record.state = InstanceState::Idle;
                record.health_check.probe_mut().unwrap().grace_period_ms = 50;
            }))
            .await;
        let on_request =
            desired_instance(|instance| instance.desired_state = DesiredInstanceState::OnRequest);
        host.cache
            .lock()
            .await
            .accept(desired_state(|state| state.instances = vec![on_request]));

        let waker = AppWaker::new(host.arc().clone());
        let outcomes = futures::future::join_all((0..10).map(|_| {
            let waker = waker.clone();
            let app_id = app_id();
            async move { waker.wake(&app_id).await }
        }))
        .await;

        assert_eq!(outcomes.len(), 10);
        assert_eq!(
            host.vms
                .calls()
                .iter()
                .filter(|call| **call == crate::ports::VmCall::Wake)
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn a_request_that_finds_its_app_mid_snapshot_waits_for_the_snapshot_and_restores_from_it() {
        use crate::ports::VmCall;

        let mut host = test_host().await;
        let (held, spy) = mocks::vmm_holding_sleeps();
        Arc::get_mut(&mut host.host)
            .expect("nothing else holds this host yet")
            .vms = held.clone();
        host.vms = spy;
        host.volumes.provision(&desired_volume(|_| {})).await.unwrap();
        host.state.modify(|snapshot| snapshot.isolated = true).await;
        host.slot_for(&app_id()).await.unwrap();
        let listening = listening().await;
        host.state
            .put_record(instance_record(|record| {
                record.on_request = true;
                record.health_check = protocol::HealthCheck::BootCompleted;
                record.guest_ipv4 = crate::domain::health::probe::loopback();
                record.http_port = listening;
            }))
            .await;
        let on_request =
            desired_instance(|instance| instance.desired_state = DesiredInstanceState::OnRequest);
        host.cache
            .lock()
            .await
            .accept(desired_state(|state| state.instances = vec![on_request]));
        let quiet_since = crate::clock::now_ms() - protocol::DEFAULT_IDLE_TIMEOUT_MS as i64 - 1;
        measured_quiet_since(&host.state, &app_id(), quiet_since).await;

        let sleeping = tokio::spawn({
            let host = host.arc().clone();
            async move { crate::domain::reconcile::idle::apply_sleep(&host).await }
        });
        held.held_up(1).await;

        let waker = AppWaker::new(host.arc().clone());
        let asked_for = app_id();
        let mut woken = std::pin::pin!(waker.wake(&asked_for));
        let went_ahead = tokio::time::timeout(Duration::from_millis(50), &mut woken).await;
        assert!(
            went_ahead.is_err(),
            "the wake went ahead while the snapshot was still being written: {went_ahead:?}"
        );
        assert!(!host.vms.calls().contains(&VmCall::Wake));

        held.let_through(1);
        sleeping.await.unwrap();
        woken
            .await
            .expect("restored from the snapshot once it was on the disk");

        assert_eq!(held.wakes_mid_sleep(), 0);
        assert_eq!(host.vms.calls(), vec![VmCall::Sleep, VmCall::Wake]);
        let record = host.state.record(&app_id()).await.unwrap();
        assert_eq!(record.state, InstanceState::Starting);
        assert!(!record.stop_requested);
    }

    #[tokio::test]
    async fn a_leader_dropped_mid_boot_does_not_wedge_the_next_wake() {
        let host = test_host().await;
        host.volumes.provision(&desired_volume(|_| {})).await.unwrap();
        host.state.modify(|snapshot| snapshot.isolated = true).await;
        host.state
            .put_record(instance_record(|record| {
                record.on_request = true;
                record.state = InstanceState::Idle;
                record.guest_ipv4 = crate::domain::health::probe::loopback();
                record.http_port = protocol::HttpPort::new(1).unwrap();
                let probe = record.health_check.probe_mut().unwrap();
                probe.grace_period_ms = 200;
                probe.timeout_ms = 50;
            }))
            .await;
        let on_request =
            desired_instance(|instance| instance.desired_state = DesiredInstanceState::OnRequest);
        host.cache
            .lock()
            .await
            .accept(desired_state(|state| state.instances = vec![on_request]));

        let waker = AppWaker::new(host.arc().clone());

        // The leader's caller gives up while the boot is still probing an unanswered port.
        let abandoned = tokio::time::timeout(Duration::from_millis(40), waker.wake(&app_id())).await;
        assert!(
            abandoned.is_err(),
            "the leader should still be booting when its caller gives up"
        );

        // The next request must lead its own wake rather than subscribe to the abandoned leader's
        // sender, which would never fire. A regression hangs here until the generous timeout.
        let next = tokio::time::timeout(Duration::from_secs(2), waker.wake(&app_id()))
            .await
            .expect("a dropped leader wedged the next wake");
        assert!(
            matches!(
                next,
                Err(WakeRefusal::Failed {
                    kind: WakeFailure::NeverAnswered,
                    ..
                })
            ),
            "{next:?}"
        );
    }

    #[tokio::test]
    async fn the_requests_that_waited_on_a_wake_are_counted_on_it() {
        use tracing_subscriber::layer::SubscriberExt;

        let counted = CountsCoalesced::default();
        let _guard = crate::test_support::listen(tracing_subscriber::registry().with(counted.clone()));

        let listening = listening().await;
        let host = test_host().await;
        host.volumes.provision(&desired_volume(|_| {})).await.unwrap();
        host.state.modify(|snapshot| snapshot.isolated = true).await;
        host.state
            .put_record(instance_record(|record| {
                record.on_request = true;
                record.state = InstanceState::Idle;
                record.guest_ipv4 = crate::domain::health::probe::loopback();
                record.http_port = listening;
            }))
            .await;
        let on_request =
            desired_instance(|instance| instance.desired_state = DesiredInstanceState::OnRequest);
        host.cache
            .lock()
            .await
            .accept(desired_state(|state| state.instances = vec![on_request]));

        let waker = AppWaker::new(host.arc().clone());
        let outcomes = futures::future::join_all((0..10).map(|_| {
            let waker = waker.clone();
            let app_id = app_id();
            async move { waker.wake(&app_id).await }
        }))
        .await;

        assert!(outcomes.iter().all(Result::is_ok));
        assert_eq!(counted.taken(), vec![9]);

        let page = page(&host).await;
        assert!(
            page.contains("nibrunner_wake_requests_coalesced_total 9\n"),
            "{page}"
        );
        assert!(
            page.contains("nibrunner_wake_duration_seconds_count 10\n"),
            "every request waited"
        );
        assert!(page.contains("nibrunner_wake_phase_seconds_count{phase=\"total\",outcome=\"restored\"} 1\n"));
        assert!(page.contains("nibrunner_wake_phase_seconds_count{phase=\"ready\",outcome=\"restored\"} 1\n"));
        assert!(page.contains("nibrunner_app_wakes_total{app=\"app-1\",outcome=\"restored\"} 1\n"));
        assert!(
            page.contains("nibrunner_app_last_wake_seconds{app=\"app-1\"} 0."),
            "{page}"
        );
    }

    #[derive(Clone, Default)]
    struct CountsCoalesced(Arc<std::sync::Mutex<Vec<u64>>>);

    impl CountsCoalesced {
        fn taken(&self) -> Vec<u64> {
            self.0.lock().unwrap().clone()
        }
    }

    impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for CountsCoalesced {
        fn on_event(&self, event: &tracing::Event<'_>, _: tracing_subscriber::layer::Context<'_, S>) {
            struct Pick(Option<u64>);
            impl tracing::field::Visit for Pick {
                fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
                    if field.name() == "coalesced" {
                        self.0 = Some(value);
                    }
                }
                fn record_debug(&mut self, _: &tracing::field::Field, _: &dyn std::fmt::Debug) {}
            }
            let mut pick = Pick(None);
            event.record(&mut pick);
            if let Some(value) = pick.0 {
                self.0.lock().unwrap().push(value);
            }
        }
    }

    #[tokio::test]
    async fn a_host_with_no_memory_left_refuses_rather_than_evicting_a_neighbour() {
        let host = test_host().await;
        host.state.modify(|snapshot| snapshot.isolated = true).await;
        for index in 0..4 {
            host.state
                .put_record(instance_record(|record| {
                    record.app_id = AppId::parse(format!("neighbour-{index}")).unwrap();
                }))
                .await;
        }
        let on_request =
            desired_instance(|instance| instance.desired_state = DesiredInstanceState::OnRequest);
        host.cache
            .lock()
            .await
            .accept(desired_state(|state| state.instances = vec![on_request]));

        let waker = AppWaker::new(host.arc().clone());
        assert_eq!(
            waker.wake(&app_id()).await.unwrap_err(),
            WakeRefusal::NoRoom {
                shortfall_mib: u64::from(protocol::DEFAULT_INSTANCE_RESOURCES.memory_mib)
            }
        );
    }

    #[tokio::test]
    async fn a_tenant_refused_for_want_of_memory_is_told_how_much_was_missing() {
        let host = test_host().await;
        host.state.modify(|snapshot| snapshot.isolated = true).await;
        host.state.put_record(instance_record(|_| {})).await;
        for index in 0..4 {
            host.state
                .put_record(instance_record(|record| {
                    record.app_id = AppId::parse(format!("neighbour-{index}")).unwrap();
                }))
                .await;
        }
        let on_request =
            desired_instance(|instance| instance.desired_state = DesiredInstanceState::OnRequest);
        host.cache
            .lock()
            .await
            .accept(desired_state(|state| state.instances = vec![on_request]));

        let refusal = AppWaker::new(host.arc().clone())
            .wake(&app_id())
            .await
            .unwrap_err();
        assert!(matches!(refusal, WakeRefusal::NoRoom { .. }), "{refusal:?}");
        let message = host
            .state
            .record(&app_id())
            .await
            .and_then(|record| record.message)
            .expect("a refused tenant is told why");
        assert!(
            message.as_str().contains("MiB short of the memory it needs"),
            "{message:?}"
        );
        assert!(message.as_str().contains(app_id().as_str()), "{message:?}");
        assert!(host.vms.calls().is_empty());
    }

    #[tokio::test]
    async fn a_guest_that_never_answers_is_refused_rather_than_reported_as_woken() {
        let host = test_host().await;
        host.volumes.provision(&desired_volume(|_| {})).await.unwrap();
        host.state.modify(|snapshot| snapshot.isolated = true).await;
        host.state
            .put_record(instance_record(|record| {
                record.on_request = true;
                record.state = InstanceState::Idle;
                record.guest_ipv4 = crate::domain::health::probe::loopback();
                record.http_port = protocol::HttpPort::new(1).unwrap();
                let probe = record.health_check.probe_mut().unwrap();
                probe.grace_period_ms = 20;
                probe.timeout_ms = 50;
            }))
            .await;
        let on_request =
            desired_instance(|instance| instance.desired_state = DesiredInstanceState::OnRequest);
        host.cache
            .lock()
            .await
            .accept(desired_state(|state| state.instances = vec![on_request]));

        let Err(WakeRefusal::Failed { kind, reason }) =
            AppWaker::new(host.arc().clone()).wake(&app_id()).await
        else {
            panic!("a guest that never answered was reported as woken");
        };
        assert!(reason.contains("nothing answered on port 1"), "{reason}");
        assert_eq!(kind, WakeFailure::NeverAnswered);
        assert!(
            page(&host)
                .await
                .contains("nibrunner_wake_refusals_total{reason=\"never_answered\"} 1\n"),
            "the refusal is counted under its reason"
        );
    }

    async fn page(host: &TestHost) -> String {
        crate::domain::metrics::tests::page(
            &crate::domain::metrics::tests::report(),
            &host.metrics,
            &host.state.snapshot().await,
            0,
        )
    }

    /// A port on the loopback that accepts, for as long as the test runs.
    async fn listening() -> protocol::HttpPort {
        let listener = tokio::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
            .await
            .unwrap();
        let port = protocol::HttpPort::new(listener.local_addr().unwrap().port()).unwrap();
        tokio::spawn(async move { while listener.accept().await.is_ok() {} });
        port
    }

    // A request handed on between the boot completing and the tenant binding its port is refused
    // by the guest; it is held until the port accepts once instead.
    #[tokio::test]
    async fn a_boot_completed_guest_is_woken_once_its_port_accepts_and_held_until_then() {
        let host = test_host().await;
        host.volumes.provision(&desired_volume(|_| {})).await.unwrap();
        host.state.modify(|snapshot| snapshot.isolated = true).await;
        host.state
            .put_record(instance_record(|record| {
                record.on_request = true;
                record.state = InstanceState::Idle;
                record.health_check = protocol::HealthCheck::BootCompleted;
                record.guest_ipv4 = crate::domain::health::probe::loopback();
                record.http_port = protocol::HttpPort::new(1).unwrap();
            }))
            .await;
        let on_request =
            desired_instance(|instance| instance.desired_state = DesiredInstanceState::OnRequest);
        host.cache
            .lock()
            .await
            .accept(desired_state(|state| state.instances = vec![on_request]));
        let waker = AppWaker::new(host.arc().clone());

        let held = tokio::time::timeout(Duration::from_millis(50), waker.wake(&app_id())).await;
        assert!(
            held.is_err(),
            "the caller was handed on while nothing listened: {held:?}"
        );
        assert!(host.vms.calls().contains(&crate::ports::VmCall::Wake));

        let listening = listening().await;
        host.state
            .update_record(&app_id(), |record| record.http_port = listening)
            .await;
        waker
            .wake(&app_id())
            .await
            .expect("the tenant is listening, which is all it promised");
    }

    /// A host whose volumes are ZeroFS exports on NBD devices: the kernel's view of the devices
    /// lives under `sysfs`, and `commands` answers for nbd-client. The volume is there and its
    /// app is asleep, waiting to be asked for.
    async fn host_on_nbd(
        commands: Arc<crate::ports::MockCommandRunner>,
        sysfs: &std::path::Path,
    ) -> (TestHost, tempfile::TempDir) {
        use crate::adapters::volumes::nbd::NbdDevices;
        use crate::adapters::volumes::zerofs::{ZerofsFilesystem, ZerofsVolumes};

        let root = tempfile::tempdir().unwrap();
        let filesystem = ZerofsFilesystem {
            storage_prefix: protocol::ObjectKey::parse("volumes").unwrap(),
            mount_path: root.path().join("mnt"),
            nbd_socket_path: "/run/zerofs/nbd.sock".into(),
            checkpoint_runtime_dir: "/run/zerofs-checkpoint".into(),
            binary: "/opt/nibrun/bin/zerofs/zerofs".into(),
            config_file: root.path().join("zerofs.toml"),
        };
        std::fs::create_dir_all(filesystem.nbd_directory()).unwrap();
        std::fs::write(filesystem.device_file_for(&volume_id()), vec![0u8; 4096]).unwrap();

        let mut host = test_host().await;
        let staging = crate::adapters::volumes::initial_contents::tests::staging(
            &root.path().join("staging"),
            crate::adapters::volumes::initial_contents::tests::archive(),
        );
        let volumes = ZerofsVolumes::new(filesystem, host.allocator.clone(), commands.clone(), staging)
            .with_devices(NbdDevices::with_sysfs(sysfs.to_path_buf(), commands));
        Arc::get_mut(&mut host.host)
            .expect("nothing else holds this host yet")
            .volumes = Arc::new(volumes);

        host.state.modify(|snapshot| snapshot.isolated = true).await;
        let listening = listening().await;
        host.state
            .put_record(instance_record(|record| {
                record.on_request = true;
                record.state = InstanceState::Idle;
                record.health_check = protocol::HealthCheck::BootCompleted;
                record.guest_ipv4 = crate::domain::health::probe::loopback();
                record.http_port = listening;
            }))
            .await;
        let on_request =
            desired_instance(|instance| instance.desired_state = DesiredInstanceState::OnRequest);
        host.cache
            .lock()
            .await
            .accept(desired_state(|state| state.instances = vec![on_request]));
        (host, root)
    }

    /// The kernel holds the device but nothing answers on it: what a ZeroFS restart leaves.
    fn dead_device(sysfs: &std::path::Path) -> std::path::PathBuf {
        let directory = sysfs.join("nbd0");
        std::fs::create_dir_all(&directory).unwrap();
        let pid_file = directory.join("pid");
        std::fs::write(&pid_file, "4123\n").unwrap();
        pid_file
    }

    fn detaches(request: &crate::ports::CommandRequest) -> bool {
        request.command.get(1).is_some_and(|word| word == "-d")
    }

    #[tokio::test]
    async fn a_wake_onto_a_device_that_does_not_answer_re_attaches_it_before_the_restore() {
        let sysfs = tempfile::tempdir().unwrap();
        let pid_file = dead_device(sysfs.path());
        let (commands, log) = mocks::commands_answering(move |request| {
            if detaches(request) {
                std::fs::remove_file(&pid_file).unwrap();
            }
            Ok(crate::ports::CommandResult::succeeded())
        });
        let (host, _root) = host_on_nbd(commands, sysfs.path()).await;

        AppWaker::new(host.arc().clone())
            .wake(&app_id())
            .await
            .expect("the device was re-attached and the app restored onto it");

        let asked = log.commands();
        assert_eq!(asked.len(), 2, "{asked:?}");
        assert_eq!(
            asked[0],
            vec!["nbd-client".to_string(), "-d".into(), "/dev/nbd0".into()]
        );
        assert_eq!(asked[1][3..6], ["/dev/nbd0", "-N", "vol-1"]);
        assert_eq!(host.vms.calls(), vec![crate::ports::VmCall::Wake]);
    }

    #[tokio::test]
    async fn a_wake_whose_re_attach_fails_is_refused_rather_than_restored_onto_a_dead_disk() {
        let sysfs = tempfile::tempdir().unwrap();
        let pid_file = dead_device(sysfs.path());
        let (commands, _) = mocks::commands_answering(move |request| {
            if detaches(request) {
                std::fs::remove_file(&pid_file).unwrap();
                return Ok(crate::ports::CommandResult::succeeded());
            }
            Ok(crate::ports::CommandResult {
                code: 1,
                stdout: String::new(),
                stderr: "Error: Failed to setup device, check dmesg\nExiting.\n".into(),
            })
        });
        let (host, _root) = host_on_nbd(commands, sysfs.path()).await;

        // The pause between the two attach attempts is on the clock rather than waited out.
        tokio::time::pause();
        let refusal = AppWaker::new(host.arc().clone())
            .wake(&app_id())
            .await
            .unwrap_err();

        let WakeRefusal::Failed { kind, reason } = refusal else {
            panic!("refused for a reason of its own, not for want of memory: {refusal:?}");
        };
        assert_eq!(kind, WakeFailure::VolumeUnusable);
        assert!(reason.contains("Failed to setup device, check dmesg"), "{reason}");
        assert!(
            host.vms.calls().is_empty(),
            "nothing was restored onto the dead disk: {:?}",
            host.vms.calls()
        );
        let record = host.state.record(&app_id()).await.unwrap();
        assert_eq!(record.state, InstanceState::Idle, "the app is still asleep");
        assert!(
            record
                .message
                .as_ref()
                .unwrap()
                .as_str()
                .contains("Failed to setup device"),
            "the tenant is told why: {:?}",
            record.message
        );
        assert!(
            page(&host)
                .await
                .contains("nibrunner_wake_refusals_total{reason=\"volume_unusable\"} 1\n"),
            "the refusal is counted under its reason"
        );
    }

    #[tokio::test]
    async fn a_wake_is_refused_for_an_app_this_host_has_no_record_of_starting() {
        let host = test_host().await;
        host.volumes.provision(&desired_volume(|_| {})).await.unwrap();
        host.state.modify(|snapshot| snapshot.isolated = true).await;
        let on_request =
            desired_instance(|instance| instance.desired_state = DesiredInstanceState::OnRequest);
        host.cache
            .lock()
            .await
            .accept(desired_state(|state| state.instances = vec![on_request]));

        let refusal = AppWaker::new(host.arc().clone())
            .wake(&app_id())
            .await
            .unwrap_err();
        assert_eq!(
            refusal,
            WakeRefusal::Failed {
                kind: WakeFailure::WouldNotStart,
                reason: "the microVM would not start".into()
            }
        );
    }

    #[tokio::test]
    async fn a_wake_is_refused_for_an_app_the_document_no_longer_names() {
        let host = test_host().await;
        host.state.modify(|snapshot| snapshot.isolated = true).await;
        let waker = AppWaker::new(host.arc().clone());
        let refusal = waker.wake(&app_id()).await.unwrap_err();
        assert_eq!(
            refusal,
            WakeRefusal::Failed {
                kind: WakeFailure::NotNamed,
                reason: "the control plane no longer names it".into()
            }
        );
    }

    #[tokio::test]
    async fn a_wake_is_refused_while_the_isolation_ruleset_is_not_applied() {
        let host = test_host().await;
        let on_request =
            desired_instance(|instance| instance.desired_state = DesiredInstanceState::OnRequest);
        host.cache
            .lock()
            .await
            .accept(desired_state(|state| state.instances = vec![on_request]));
        let waker = AppWaker::new(host.arc().clone());
        let refusal = waker.wake(&app_id()).await.unwrap_err();
        assert_eq!(
            refusal,
            WakeRefusal::Failed {
                kind: WakeFailure::NotIsolated,
                reason: "the isolation ruleset is not applied".into()
            }
        );
    }

    /// A host whose document keeps the app running, with the ruleset that lets it start anything.
    async fn host_keeping_the_app_running() -> TestHost {
        let host = test_host().await;
        host.state.modify(|snapshot| snapshot.isolated = true).await;
        host.cache.lock().await.accept(desired_state(|state| {
            state.instances = vec![desired_instance(|_| {})]
        }));
        host
    }

    /// A port on the loopback nothing is listening on yet.
    async fn free_port() -> protocol::HttpPort {
        let probe = tokio::net::TcpListener::bind(std::net::SocketAddr::from(([127, 0, 0, 1], 0)))
            .await
            .unwrap();
        protocol::HttpPort::new(probe.local_addr().unwrap().port()).unwrap()
    }

    /// The guest comes up on `port`: it accepts every connection from here on.
    async fn guest_answers_on(port: protocol::HttpPort) {
        let listener =
            tokio::net::TcpListener::bind(std::net::SocketAddr::from(([127, 0, 0, 1], port.get())))
                .await
                .unwrap();
        tokio::spawn(async move { while listener.accept().await.is_ok() {} });
    }

    /// A guest booted `ago_ms` ago on the loopback, at `port`, with its health still unproven.
    fn booted(port: protocol::HttpPort, ago_ms: i64) -> crate::domain::report::InstanceRecord {
        instance_record(|record| {
            record.state = InstanceState::Starting;
            record.started_at = Some(protocol::Timestamp::from_epoch_ms(
                crate::clock::now_ms() - ago_ms,
            ));
            record.guest_ipv4 = crate::domain::health::probe::loopback();
            record.http_port = port;
        })
    }

    #[tokio::test]
    async fn a_request_during_a_replacement_boot_is_held_and_served_once_the_guest_answers() {
        let host = host_keeping_the_app_running().await;
        let port = free_port().await;
        // The pass has stopped the old guest for its replacement.
        host.state
            .put_record(instance_record(|record| {
                record.state = InstanceState::Stopping;
                record.stop_requested = true;
                record.started_at = Some(observed_at());
            }))
            .await;

        let waker = AppWaker::new(host.arc().clone());
        let asked_for = app_id();
        let mut held = std::pin::pin!(waker.wake(&asked_for));
        let went_ahead = tokio::time::timeout(Duration::from_millis(50), &mut held).await;
        assert!(
            went_ahead.is_err(),
            "the request was answered while the old guest was still going down: {went_ahead:?}"
        );

        // The pass forgets the old record and boots the replacement, which is not answering yet.
        host.state.drop_record(&app_id()).await;
        host.state.put_record(booted(port, 0)).await;
        let went_ahead = tokio::time::timeout(Duration::from_millis(50), &mut held).await;
        assert!(
            went_ahead.is_err(),
            "the request was answered before the replacement answered its port: {went_ahead:?}"
        );

        guest_answers_on(port).await;
        tokio::time::timeout(Duration::from_secs(2), held)
            .await
            .expect("the request was still held after the replacement answered")
            .expect("served by the replacement");
        assert!(host.vms.calls().is_empty(), "a hold boots nothing of its own");
    }

    #[tokio::test]
    async fn the_requests_that_arrive_during_one_boot_are_held_together_and_released_together() {
        let host = host_keeping_the_app_running().await;
        let port = free_port().await;
        host.state.put_record(booted(port, 0)).await;
        let waker = AppWaker::new(host.arc().clone());

        let held = tokio::spawn(futures::future::join_all((0..10).map(|_| {
            let waker = waker.clone();
            let app_id = app_id();
            async move { waker.wake(&app_id).await }
        })));
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            !held.is_finished(),
            "the requests went ahead before the guest answered"
        );

        guest_answers_on(port).await;
        let outcomes = tokio::time::timeout(Duration::from_secs(2), held)
            .await
            .expect("the requests were still held after the guest answered")
            .unwrap();
        assert!(outcomes.iter().all(Result::is_ok), "{outcomes:?}");
        assert!(
            page(&host)
                .await
                .contains("nibrunner_wake_requests_coalesced_total 9\n"),
            "one request led the hold and the rest joined it"
        );
    }

    #[tokio::test]
    async fn a_request_to_an_app_that_is_out_of_restarts_is_refused_at_once() {
        let host = host_keeping_the_app_running().await;
        host.state
            .put_record(instance_record(|record| {
                record.state = InstanceState::Failed;
                record.started_at = Some(observed_at());
                record.message = Some(protocol::StateMessage::new(
                    "out of restarts: 6 starts attempted against a budget of 5, \
                     and this instance will not be started again until it is deployed afresh",
                ));
            }))
            .await;

        let refusal = tokio::time::timeout(
            Duration::from_secs(1),
            AppWaker::new(host.arc().clone()).wake(&app_id()),
        )
        .await
        .expect("refused at once rather than held for the grace period")
        .unwrap_err();
        let WakeRefusal::Failed { kind, reason } = refusal else {
            panic!("refused for a reason of its own, not for want of memory: {refusal:?}");
        };
        assert_eq!(kind, WakeFailure::WouldNotStart);
        assert!(reason.contains("out of restarts"), "{reason}");
        assert!(host.vms.calls().is_empty());
    }

    #[tokio::test]
    async fn a_request_no_guest_comes_up_for_inside_the_grace_period_is_refused_rather_than_kept() {
        let host = host_keeping_the_app_running().await;
        host.cache.lock().await.accept(desired_state(|state| {
            state.instances = vec![desired_instance(|instance| {
                instance.config.health_check.probe_mut().unwrap().grace_period_ms = 20;
            })]
        }));
        // Booted by nobody yet: the pass wrote the record and is still fetching what it boots from.
        host.state
            .put_record(instance_record(|record| record.state = InstanceState::Pending))
            .await;

        let refusal = AppWaker::new(host.arc().clone())
            .wake(&app_id())
            .await
            .unwrap_err();
        assert_eq!(
            refusal,
            WakeRefusal::Failed {
                kind: WakeFailure::WouldNotStart,
                reason: "no microVM came up for it in time".into()
            }
        );
    }

    #[tokio::test]
    async fn a_guest_that_has_had_its_grace_period_and_still_does_not_answer_is_not_waited_on_again() {
        let host = host_keeping_the_app_running().await;
        let port = free_port().await;
        let grace_ms = TCP_HEALTH_CHECK.probe().grace_period_ms as i64;
        host.state.put_record(booted(port, grace_ms + 1)).await;

        let refusal = tokio::time::timeout(
            Duration::from_secs(1),
            AppWaker::new(host.arc().clone()).wake(&app_id()),
        )
        .await
        .expect("refused at once rather than given a second grace period")
        .unwrap_err();
        let WakeRefusal::Failed { kind, reason } = refusal else {
            panic!("refused for a reason of its own, not for want of memory: {refusal:?}");
        };
        assert_eq!(kind, WakeFailure::NeverAnswered);
        assert!(
            reason.contains(&format!("nothing answered on port {port}")),
            "{reason}"
        );
    }

    #[tokio::test]
    async fn a_wake_is_refused_for_an_app_the_document_says_is_stopped() {
        let host = test_host().await;
        host.state.modify(|snapshot| snapshot.isolated = true).await;
        let stopped = desired_instance(|instance| instance.desired_state = DesiredInstanceState::Stopped);
        host.cache
            .lock()
            .await
            .accept(desired_state(|state| state.instances = vec![stopped]));
        let waker = AppWaker::new(host.arc().clone());
        assert_eq!(
            waker.wake(&app_id()).await.unwrap_err(),
            WakeRefusal::Failed {
                kind: WakeFailure::NotOnRequest,
                reason: "it is stopped".into()
            }
        );
    }
}
