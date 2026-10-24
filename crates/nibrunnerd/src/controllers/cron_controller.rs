use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use protocol::{DesiredInstanceState, InstanceState};
use tokio::sync::{Mutex, Notify};
use tokio::task::JoinSet;

use crate::adapters::cron_execution::{self, CronExecutionError, GuestCronExecution};
use crate::adapters::logs::object_sink::{Attributes, CronScope, OtelStatus, SpanKind, SpanLine, SpanStatus};
use crate::adapters::otlp::trace::{SpanId, TraceBook};
use crate::controllers::Controller;
use crate::domain::cron::execution::{
    self as attempts, AttemptTrace, CronOutcome, CronRecord, CronRunOutput,
};
use crate::domain::cron::jobs::{CronJobs, CronOrigin, CronWhen};
use crate::domain::cron::registry::{CronRegistry, CronRegistryError};
use crate::domain::cron::runs::{CronRunLease, CronRunRefusal};
use crate::domain::cron::scheduler::{CronScheduler, ScheduledRun};
use crate::domain::filesystem::reader::guest_vsock_path;
use crate::domain::metrics::rollup::micros_rfc3339;
use crate::host::Host;
use crate::ports::LogSink;
use crate::state::CronActivity;

const CRON_RETRY_DELAY: Duration = Duration::from_secs(1);

#[derive(Default)]
struct Scheduling {
    scheduler: CronScheduler,
    tasks: JoinSet<()>,
}

struct RunCompleted(Arc<Notify>);

impl Drop for RunCompleted {
    fn drop(&mut self) {
        self.0.notify_one();
    }
}

pub struct CronController {
    host: Arc<Host>,
    registry: Arc<CronRegistry>,
    scheduling: Mutex<Scheduling>,
    completed: Arc<Notify>,
}

impl CronController {
    pub fn new(host: Arc<Host>, registry: Arc<CronRegistry>) -> Arc<Self> {
        Arc::new(Self {
            host,
            registry,
            scheduling: Mutex::new(Scheduling::default()),
            completed: Arc::new(Notify::new()),
        })
    }

    pub async fn cron_once(&self, now: DateTime<Utc>) {
        if let Err(error) = self.try_cron_once(now).await {
            tracing::warn!(%error, "cron registrations could not be synchronized");
        }
    }

    async fn try_cron_once(&self, now: DateTime<Utc>) -> Result<(), CronRegistryError> {
        let tables = self.registry.jobs().await?;
        let desired = self.host.cache.lock().await.latest().cloned();
        let tables: Vec<_> = tables
            .into_iter()
            .filter(|table| {
                desired.as_ref().is_some_and(|desired| {
                    desired.instances.iter().any(|instance| {
                        instance.app_id == table.app_id
                            && instance.deployment_id == table.deployment_id
                            && instance.desired_state != DesiredInstanceState::Stopped
                    })
                })
            })
            .collect();
        let mut scheduling = self.scheduling.lock().await;
        while let Some(task) = scheduling.tasks.try_join_next() {
            if let Err(error) = task {
                tracing::warn!(%error, "a cron execution task did not finish");
            }
        }
        let time_zone = self.host.config.cron.time_zone;
        scheduling.scheduler.synchronize(&tables, now, time_zone);
        for run in scheduling.scheduler.due(now, time_zone) {
            let Some(table) = tables
                .iter()
                .find(|table| table.app_id == run.app_id && table.deployment_id == run.deployment_id)
            else {
                continue;
            };
            // A crontab's edit cancels what its old text had made due; a document's does not.
            let expected = (table.origin != CronOrigin::Document).then(|| table.clone());
            let host = self.host.clone();
            let lease = match host.cron_runs.start(&run.app_id, &run.deployment_id, &run.job.id) {
                Ok(lease) => lease,
                Err(CronRunRefusal::Busy) => {
                    scheduling
                        .tasks
                        .spawn(async move { Self::skipped(&host, &run).await });
                    continue;
                }
                Err(CronRunRefusal::Closed) => continue,
            };
            let registry = self.registry.clone();
            let completed = self.completed.clone();
            scheduling.tasks.spawn(async move {
                let _completed = RunCompleted(completed);
                Self::execute_owned(host, registry, run, lease, expected).await;
            });
        }
        Ok(())
    }

    async fn current(host: &Host, run: &ScheduledRun) -> bool {
        host.cache.lock().await.latest().is_some_and(|desired| {
            desired.instances.iter().any(|instance| {
                instance.app_id == run.app_id
                    && instance.deployment_id == run.deployment_id
                    && instance.desired_state != DesiredInstanceState::Stopped
            })
        })
    }

    async fn registered(registry: &CronRegistry, run: &ScheduledRun, expected: Option<&CronJobs>) -> bool {
        registry.jobs().await.is_ok_and(|tables| {
            tables.iter().any(|table| {
                table.app_id == run.app_id
                    && table.deployment_id == run.deployment_id
                    && table.jobs.contains(&run.job)
                    && expected.is_none_or(|expected| table == expected)
            })
        })
    }

    async fn prepare(
        host: &Host,
        registry: &CronRegistry,
        run: &ScheduledRun,
        expected: Option<&CronJobs>,
    ) -> Option<CronActivity> {
        let (activity, needs_wake) = {
            let _transition = host.state.transition(&run.app_id).await;
            if !Self::current(host, run).await
                || !Self::registered(registry, run, expected).await
                || !host.state.snapshot().await.isolated
            {
                return None;
            }
            let record = host.state.record(&run.app_id).await?;
            if record.deployment_id != run.deployment_id
                || (record.stop_requested
                    && !matches!(record.state, InstanceState::Idle | InstanceState::Frozen))
                || !record.desired_running
                || !matches!(
                    record.state,
                    InstanceState::Idle
                        | InstanceState::Frozen
                        | InstanceState::Running
                        | InstanceState::Starting
                        | InstanceState::Unhealthy
                )
            {
                return None;
            }
            (
                host.state.cron_activity(&run.app_id),
                matches!(record.state, InstanceState::Idle | InstanceState::Frozen),
            )
        };
        if needs_wake {
            if let Err(refusal) = host.waker.wake(&run.app_id).await {
                tracing::warn!(app_id = %run.app_id, refusal = ?refusal, "cron could not wake its app");
                return None;
            }
        }
        {
            let _transition = host.state.transition(&run.app_id).await;
            if !Self::current(host, run).await || !host.state.snapshot().await.isolated {
                return None;
            }
            if !host.state.record(&run.app_id).await.is_some_and(|record| {
                record.deployment_id == run.deployment_id
                    && record.started_at.is_some()
                    && !record.stop_requested
                    && record.desired_running
                    && matches!(
                        record.state,
                        InstanceState::Running | InstanceState::Starting | InstanceState::Unhealthy
                    )
            }) {
                return None;
            }
        }
        Some(activity)
    }

    /// Every attempt of `run`: the first, then each retry its backoff allows, each under a lease
    /// of its own so that the job's next occurrence is never kept waiting behind a backoff.
    async fn execute_owned(
        host: Arc<Host>,
        registry: Arc<CronRegistry>,
        run: ScheduledRun,
        mut lease: CronRunLease,
        expected: Option<CronJobs>,
    ) {
        for attempt in 1.. {
            let activity = tokio::select! {
                biased;
                () = lease.cancelled() => None,
                activity = Self::prepare(&host, &registry, &run, expected.as_ref()) => activity,
            };
            let Some(activity) = activity else {
                return;
            };
            let outcome = Self::attempt(&host, &run, attempt, &mut lease).await;
            host.state.mark_active(&run.app_id, crate::clock::now_ms()).await;
            drop(activity);
            drop(lease);
            let Some(delay) = outcome.and_then(|outcome| {
                attempts::retry_delay(&run, attempt, outcome, Utc::now(), host.config.cron.time_zone)
            }) else {
                return;
            };
            tokio::time::sleep(delay).await;
            let Ok(retry) = host.cron_runs.start(&run.app_id, &run.deployment_id, &run.job.id) else {
                return;
            };
            lease = retry;
        }
    }

    /// One attempt, as one trace between its two log lines. A declared job's attempt is run only
    /// by the host whose record of it is created first; `None` when that is not this one.
    async fn attempt(
        host: &Host,
        run: &ScheduledRun,
        attempt: usize,
        lease: &mut CronRunLease,
    ) -> Option<CronOutcome> {
        let trace = TraceBook::host().mint(crate::clock::now_us());
        let started_us = trace.started_us();
        let trace = AttemptTrace {
            trace: trace.to_string(),
            span: SpanId::random().to_string(),
        };
        let scope = host
            .telemetry
            .as_deref()
            .and_then(|sink| sink.cron_scope(&run.app_id, &run.deployment_id));
        let fence = scope
            .as_ref()
            .filter(|_| run.job.declared.is_some())
            .map(|scope| Fence::new(scope, run, attempt, Some(&trace), started_us));
        if let Some(fence) = &fence {
            if !fence.open(host).await {
                return None;
            }
        }
        let mut output = CronRunOutput::new(run.clone(), attempt, trace);
        host.logs.publish(vec![output.started()]).await;
        let timeout = run.job.declared.as_ref().map(|declared| declared.timeout);
        let result = GuestCronExecution::run(
            &guest_vsock_path(host, &run.app_id),
            &mut output,
            host.logs.as_ref(),
            async {
                tokio::select! {
                    () = lease.cancelled() => CronExecutionError::Cancelled,
                    () = async {
                        match timeout {
                            Some(timeout) => tokio::time::sleep(timeout).await,
                            None => std::future::pending().await,
                        }
                    } => CronExecutionError::TimedOut,
                }
            },
        )
        .await;
        if let Err(error) = &result {
            tracing::warn!(app_id = %run.app_id, deployment_id = %run.deployment_id, job_id = %run.job.id, %error, "cron execution did not complete");
        }
        let outcome = cron_execution::outcome(&result);
        let ended_us = crate::clock::now_us().max(started_us);
        let duration_ms = u64::try_from((ended_us - started_us) / 1000).unwrap_or(0);
        host.logs.publish(vec![output.ended(outcome, duration_ms)]).await;
        if let Some(fence) = fence {
            fence.close(outcome, ended_us, duration_ms).await;
        }
        if let Some(sink) = &host.telemetry {
            let span = root_span(
                run,
                attempt,
                output.trace(),
                scope.as_ref(),
                outcome,
                (started_us, ended_us),
            );
            sink.cron_attempt(&run.app_id, &run.deployment_id, span);
        }
        Some(outcome)
    }

    /// A declared job due while it still runs here: its occurrence is recorded as skipped.
    async fn skipped(host: &Host, run: &ScheduledRun) {
        let Some(scope) = host
            .telemetry
            .as_deref()
            .filter(|_| run.job.declared.is_some())
            .and_then(|sink| sink.cron_scope(&run.app_id, &run.deployment_id))
        else {
            return;
        };
        let now_us = crate::clock::now_us();
        let fence = Fence::new(&scope, run, 1, None, now_us);
        fence
            .create(
                host,
                &fence
                    .record
                    .clone()
                    .ended(CronOutcome::Skipped, micros_rfc3339(now_us), 0),
            )
            .await;
    }

    #[cfg(test)]
    async fn dispatch(host: Arc<Host>, registry: Arc<CronRegistry>, run: ScheduledRun) {
        let Ok(lease) = host.cron_runs.start(&run.app_id, &run.deployment_id, &run.job.id) else {
            return;
        };
        Self::execute_owned(host, registry, run, lease, None).await;
    }
}

/// A declared job's attempt as its record says it: the key whose creation is the right to run it.
struct Fence<'a> {
    scope: &'a CronScope<'a>,
    key: String,
    record: CronRecord,
}

impl<'a> Fence<'a> {
    fn new(
        scope: &'a CronScope<'a>,
        run: &ScheduledRun,
        attempt: usize,
        trace: Option<&AttemptTrace>,
        started_us: i64,
    ) -> Self {
        Self {
            scope,
            key: attempts::record_key(&scope.app, &scope.timeline, run, attempt),
            record: CronRecord {
                host: scope.host.clone(),
                rev: scope.rev.clone(),
                deployment: run.deployment_id.to_string(),
                trace: trace.map(|trace| trace.trace.clone()),
                started: micros_rfc3339(started_us),
                manual: matches!(run.job.when, CronWhen::At(_)),
                ended: None,
                exit: None,
                duration_ms: None,
                status: None,
                reason: None,
            },
        }
    }

    /// Whether this host may run the attempt: it created the record.
    async fn open(&self, host: &Host) -> bool {
        self.create(host, &self.record).await
    }

    async fn close(self, outcome: CronOutcome, ended_us: i64, duration_ms: u64) {
        let record = self
            .record
            .clone()
            .ended(outcome, micros_rfc3339(ended_us), duration_ms);
        if let Err(error) = self.scope.put(&self.key, encoded(&record), false).await {
            tracing::warn!(key = %self.key, %error, "a cron attempt's outcome was not recorded");
        }
    }

    async fn create(&self, host: &Host, record: &CronRecord) -> bool {
        match self.scope.put(&self.key, encoded(record), true).await {
            Ok(true) => true,
            Ok(false) => {
                host.metrics.passes.cron_attempt_taken();
                false
            }
            Err(error) => {
                tracing::warn!(key = %self.key, %error, "a cron attempt could not be recorded, so it is not run");
                false
            }
        }
    }
}

fn encoded(record: &CronRecord) -> Vec<u8> {
    serde_json::to_vec(record).expect("a cron record serializes as JSON")
}

/// The span an attempt is: the root of its trace, which its process's own spans join.
fn root_span(
    run: &ScheduledRun,
    attempt: usize,
    trace: &AttemptTrace,
    scope: Option<&CronScope<'_>>,
    outcome: CronOutcome,
    (started_us, ended_us): (i64, i64),
) -> SpanLine {
    let mut attrs = Attributes::from([
        ("cron.name".into(), run.job.id.clone().into()),
        ("cron.instant".into(), attempts::instant(run).into()),
        ("cron.attempt".into(), attempt.into()),
        (
            "cron.manual".into(),
            matches!(run.job.when, CronWhen::At(_)).into(),
        ),
    ]);
    if let CronWhen::Schedule(schedule) = &run.job.when {
        attrs.insert("cron.schedule".into(), schedule.as_str().into());
    }
    if let Some(scope) = scope {
        attrs.insert("cron.timeline".into(), scope.timeline.clone().into());
    }
    if let Some(exit) = outcome.exit() {
        attrs.insert("process.exit_code".into(), exit.into());
    }
    SpanLine {
        trace: trace.trace.clone(),
        span: trace.span.clone(),
        parent: None,
        kind: SpanKind::Internal,
        name: format!("cron {}", run.job.id),
        t: micros_rfc3339(started_us),
        end: micros_rfc3339(ended_us),
        root: true,
        status: Some(SpanStatus::Otel(if outcome.completed() {
            OtelStatus::Ok
        } else {
            OtelStatus::Error
        })),
        status_msg: outcome.reason(),
        deployment: Some(run.deployment_id.to_string()),
        attrs: Some(attrs),
        ..SpanLine::default()
    }
}

#[async_trait::async_trait]
impl Controller for CronController {
    fn name(&self) -> &'static str {
        "cron"
    }

    async fn run(&self) {
        let mut registrations = self.registry.watch();
        let mut desired = self.host.cache.lock().await.watch();
        loop {
            registrations.borrow_and_update();
            desired.borrow_and_update();
            let retry = if let Err(error) = self.try_cron_once(Utc::now()).await {
                tracing::warn!(%error, "cron registrations could not be synchronized");
                true
            } else {
                false
            };
            let next = self.scheduling.lock().await.scheduler.next_run();
            let timer = async {
                if retry {
                    tokio::time::sleep(CRON_RETRY_DELAY).await;
                } else if let Some(next) = next {
                    let delay = (next - Utc::now()).to_std().unwrap_or_default();
                    tokio::time::sleep(delay).await;
                } else {
                    std::future::pending::<()>().await;
                }
            };
            tokio::select! {
                () = timer => {},
                change = registrations.changed() => { if change.is_err() { return; } },
                change = desired.changed() => { if change.is_err() { return; } },
                () = self.completed.notified() => {},
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::time::Duration;

    use guest_contract::cron_execution::{self as wire, ExecutionFrame};
    use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
    use tokio::net::UnixListener;

    use super::*;
    use crate::domain::store::StoreError;
    use crate::ports::{WakeRefusal, Waker};
    use crate::repositories::cron_repository::MockCronRepository;
    use crate::test_support::*;

    fn at(value: &str) -> DateTime<Utc> {
        value.parse().unwrap()
    }

    async fn named_host() -> TestHost {
        let host = test_host().await;
        host.cache.lock().await.accept(desired_state(|state| {
            state.instances = vec![desired_instance(|_| {})]
        }));
        host.state.modify(|snapshot| snapshot.isolated = true).await;
        host.state
            .put_record(instance_record(|record| {
                record.state = InstanceState::Running;
                record.desired_running = true;
                record.started_at = Some(protocol::Timestamp::now());
            }))
            .await;
        host.cron
            .begin_deployment(&app_id(), &deployment_id())
            .await
            .unwrap();
        host.cron
            .replace(
                &app_id(),
                &deployment_id(),
                "* * * * * echo ok",
                at("2026-01-01T00:00:00Z"),
            )
            .await
            .unwrap();
        host.cron_runs.synchronize(&[(app_id(), deployment_id())]).await;
        host
    }

    fn run() -> ScheduledRun {
        cron_run("echo ok")
    }

    #[tokio::test]
    async fn a_job_due_while_it_runs_is_skipped_beside_another_and_both_disconnect_before_the_owner_is_stopped(
    ) {
        let host = named_host().await;
        host.cron
            .replace(
                &app_id(),
                &deployment_id(),
                "* * * * * echo ok\n*/2 * * * * echo other",
                at("2026-01-01T00:00:00Z"),
            )
            .await
            .unwrap();
        let path = guest_vsock_path(&host, &app_id());
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let listener = UnixListener::bind(&path).unwrap();
        let (ready, mut started) = tokio::sync::mpsc::channel(2);
        let guest = tokio::spawn(async move {
            let mut guests = JoinSet::new();
            for _ in 0..2 {
                let (stream, _) = listener.accept().await.unwrap();
                let ready = ready.clone();
                guests.spawn(async move {
                    let mut stream = BufReader::new(stream);
                    let mut connect = String::new();
                    stream.read_line(&mut connect).await.unwrap();
                    stream.get_mut().write_all(b"OK 1234\n").await.unwrap();
                    let mut header = [0; wire::HEADER_BYTES];
                    stream.read_exact(&mut header).await.unwrap();
                    let header = wire::decode_request_header(&header).unwrap();
                    let mut body = vec![0; header.body_length];
                    stream.read_exact(&mut body).await.unwrap();
                    let command = wire::decode_request(header, &body).unwrap().command;
                    stream
                        .get_mut()
                        .write_all(&wire::encode_reply(&ExecutionFrame::Started).unwrap())
                        .await
                        .unwrap();
                    assert_eq!(stream.read_u8().await.unwrap(), wire::ACK);
                    ready.send(command).await.unwrap();
                    assert!(stream.read_u8().await.is_err());
                });
            }
            while let Some(guest) = guests.join_next().await {
                guest.unwrap();
            }
        });
        let controller = CronController::new(host.arc().clone(), host.cron.clone());
        controller.cron_once(at("2026-01-01T00:00:00Z")).await;
        controller.cron_once(at("2026-01-01T00:01:00Z")).await;
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(5), started.recv())
                .await
                .unwrap()
                .unwrap(),
            "echo ok"
        );
        assert!(host.state.cron_running(&app_id()));
        let transition = tokio::time::timeout(Duration::from_secs(1), host.state.transition(&app_id()))
            .await
            .unwrap();
        drop(transition);
        controller.cron_once(at("2026-01-01T00:02:00Z")).await;
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(5), started.recv())
                .await
                .unwrap()
                .unwrap(),
            "echo other"
        );
        host.cache.lock().await.accept(desired_state(|state| {
            state.instances = vec![desired_instance(|instance| {
                instance.desired_state = DesiredInstanceState::Stopped
            })]
        }));
        host.cron_runs.synchronize(&[]).await;
        tokio::time::timeout(Duration::from_secs(5), guest)
            .await
            .unwrap()
            .unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            while host.state.cron_running(&app_id()) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(host
            .state
            .snapshot()
            .await
            .last_active_at_ms
            .contains_key(&app_id()));
    }

    struct WakeChecksPin {
        state: crate::state::SharedState,
        called: Arc<AtomicBool>,
    }

    #[async_trait::async_trait]
    impl Waker for WakeChecksPin {
        async fn wake(&self, app_id: &protocol::AppId) -> Result<crate::ports::WakeOutcome, WakeRefusal> {
            assert!(self.state.cron_running(app_id));
            let _transition = self.state.transition(app_id).await;
            self.state
                .update_record(app_id, |record| {
                    record.state = InstanceState::Running;
                    record.started_at = Some(protocol::Timestamp::now());
                    record.stop_requested = false;
                })
                .await;
            self.called.store(true, Ordering::SeqCst);
            Ok(crate::ports::WakeOutcome::Restored)
        }
    }

    #[tokio::test]
    async fn cron_activity_is_pinned_before_a_wake_and_released_after_execution_failure() {
        let mut host = named_host().await;
        host.cache.lock().await.accept(desired_state(|state| {
            state.instances = vec![desired_instance(|instance| {
                instance.desired_state = DesiredInstanceState::OnRequest
            })];
        }));
        host.state
            .update_record(&app_id(), |record| {
                record.state = InstanceState::Idle;
                record.started_at = None;
                record.stop_requested = true;
            })
            .await;
        let called = Arc::new(AtomicBool::new(false));
        let waker = WakeChecksPin {
            state: host.state.clone(),
            called: called.clone(),
        };
        Arc::get_mut(&mut host.host).unwrap().waker = Arc::new(waker);
        tokio::time::timeout(
            Duration::from_secs(5),
            CronController::dispatch(host.arc().clone(), host.cron.clone(), run()),
        )
        .await
        .unwrap();
        assert!(called.load(Ordering::SeqCst));
        assert!(!host.state.cron_running(&app_id()));
    }

    #[tokio::test]
    async fn an_unregistered_or_stale_run_does_not_pin_or_wake_its_guest() {
        let host = named_host().await;
        host.cron
            .replace(&app_id(), &deployment_id(), "", at("2026-01-01T00:00:00Z"))
            .await
            .unwrap();
        CronController::dispatch(host.arc().clone(), host.cron.clone(), run()).await;
        assert!(!host.state.cron_running(&app_id()));
        assert!(host.state.snapshot().await.last_active_at_ms.is_empty());
    }

    #[tokio::test]
    async fn editing_or_removing_the_crontab_keeps_a_started_command_running() {
        let host = named_host().await;
        let path = guest_vsock_path(&host, &app_id());
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let listener = UnixListener::bind(&path).unwrap();
        let (ready, started) = tokio::sync::oneshot::channel();
        let (finish, finished) = tokio::sync::oneshot::channel();
        let guest = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut stream = BufReader::new(stream);
            let mut connect = String::new();
            stream.read_line(&mut connect).await.unwrap();
            stream.get_mut().write_all(b"OK 1234\n").await.unwrap();
            let mut header = [0; wire::HEADER_BYTES];
            stream.read_exact(&mut header).await.unwrap();
            let header = wire::decode_request_header(&header).unwrap();
            let mut body = vec![0; header.body_length];
            stream.read_exact(&mut body).await.unwrap();
            stream
                .get_mut()
                .write_all(&wire::encode_reply(&ExecutionFrame::Started).unwrap())
                .await
                .unwrap();
            assert_eq!(stream.read_u8().await.unwrap(), wire::ACK);
            ready.send(()).unwrap();
            tokio::select! {
                _ = finished => {},
                result = stream.read_u8() => panic!("a registry edit interrupted the command: {result:?}"),
            }
            stream
                .get_mut()
                .write_all(&wire::encode_reply(&ExecutionFrame::Exit { code: 0, signal: 0 }).unwrap())
                .await
                .unwrap();
        });
        let dispatch = tokio::spawn(CronController::dispatch(
            host.arc().clone(),
            host.cron.clone(),
            run(),
        ));
        tokio::time::timeout(Duration::from_secs(5), started)
            .await
            .unwrap()
            .unwrap();
        for text in ["* * * * * echo replacement", ""] {
            host.cron
                .replace(&app_id(), &deployment_id(), text, at("2026-01-01T00:01:00Z"))
                .await
                .unwrap();
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(host.state.cron_running(&app_id()));
        assert!(!dispatch.is_finished());
        finish.send(()).unwrap();
        guest.await.unwrap();
        dispatch.await.unwrap();
        assert!(!host.state.cron_running(&app_id()));
    }

    struct WaitingWake {
        ready: tokio::sync::Notify,
    }

    #[async_trait::async_trait]
    impl Waker for WaitingWake {
        async fn wake(&self, _: &protocol::AppId) -> Result<crate::ports::WakeOutcome, WakeRefusal> {
            self.ready.notify_one();
            std::future::pending().await
        }
    }

    #[tokio::test]
    async fn stopping_the_owner_cancels_a_waiting_wake_without_waiting_for_readiness() {
        let mut host = named_host().await;
        host.state
            .update_record(&app_id(), |record| {
                record.state = InstanceState::Idle;
                record.stop_requested = true;
            })
            .await;
        let waker = Arc::new(WaitingWake {
            ready: tokio::sync::Notify::new(),
        });
        Arc::get_mut(&mut host.host).unwrap().waker = waker.clone();
        let dispatch = tokio::spawn(CronController::dispatch(
            host.arc().clone(),
            host.cron.clone(),
            run(),
        ));
        tokio::time::timeout(Duration::from_secs(5), waker.ready.notified())
            .await
            .unwrap();
        assert!(host.state.cron_running(&app_id()));
        tokio::time::timeout(Duration::from_secs(5), host.cron_runs.stop(&app_id()))
            .await
            .unwrap();
        dispatch.await.unwrap();
        assert!(!host.state.cron_running(&app_id()));
    }

    #[tokio::test]
    async fn stopping_the_owner_cancels_a_run_waiting_for_the_transition_lock() {
        let host = named_host().await;
        let transition = host.state.transition(&app_id()).await;
        let lease = host
            .cron_runs
            .start(&app_id(), &deployment_id(), &run().job.id)
            .unwrap();
        let dispatch = tokio::spawn(CronController::execute_owned(
            host.arc().clone(),
            host.cron.clone(),
            run(),
            lease,
            None,
        ));
        tokio::time::timeout(Duration::from_secs(5), host.cron_runs.stop(&app_id()))
            .await
            .unwrap();
        dispatch.await.unwrap();
        assert!(!host.state.cron_running(&app_id()));
        drop(transition);
    }

    #[tokio::test]
    async fn registry_and_desired_notifications_update_timers_without_a_polling_pass() {
        let host = named_host().await;
        host.cron
            .replace(&app_id(), &deployment_id(), "", Utc::now())
            .await
            .unwrap();
        let controller = CronController::new(host.arc().clone(), host.cron.clone());
        let controller_task = tokio::spawn({
            let controller = controller.clone();
            async move { controller.run().await }
        });
        host.cron
            .replace(&app_id(), &deployment_id(), "@yearly echo ok", Utc::now())
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            while controller.scheduling.lock().await.scheduler.next_run().is_none() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        host.cache.lock().await.accept(desired_state(|state| {
            state.instances = vec![desired_instance(|instance| {
                instance.desired_state = DesiredInstanceState::Stopped
            })]
        }));
        tokio::time::timeout(Duration::from_secs(5), async {
            while controller.scheduling.lock().await.scheduler.next_run().is_some() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        controller_task.abort();
        let _ = controller_task.await;
    }

    #[tokio::test]
    async fn a_comment_edit_after_the_timer_fired_prevents_the_old_table_from_launching() {
        let host = named_host().await;
        let table = host.cron.jobs().await.unwrap().remove(0);
        host.cron
            .replace(
                &app_id(),
                &deployment_id(),
                "# edited\n* * * * * echo ok",
                Utc::now(),
            )
            .await
            .unwrap();
        let lease = host
            .cron_runs
            .start(&app_id(), &deployment_id(), &run().job.id)
            .unwrap();
        CronController::execute_owned(host.arc().clone(), host.cron.clone(), run(), lease, Some(table)).await;
        assert!(!host.state.cron_running(&app_id()));
        assert!(host.state.snapshot().await.last_active_at_ms.is_empty());
    }

    async fn wait_for_reads(reads: &AtomicUsize, observed: &Notify, minimum: usize) {
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let changed = observed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                if reads.load(Ordering::SeqCst) >= minimum {
                    break;
                }
                changed.await;
            }
        })
        .await
        .expect("the controller attempts registry synchronization in time");
    }

    #[tokio::test]
    async fn a_failed_initial_registry_read_retries_without_a_notification_and_then_stops_retrying() {
        let host = named_host().await;
        host.cron
            .replace(&app_id(), &deployment_id(), "@yearly echo ok", Utc::now())
            .await
            .unwrap();
        let tables = host.cron.tables().await.unwrap();
        let reads = Arc::new(AtomicUsize::new(0));
        let observed = reads.clone();
        let read = Arc::new(Notify::new());
        let read_observed = read.clone();
        let mut repository = MockCronRepository::new();
        repository.expect_all().returning(move || {
            let attempt = observed.fetch_add(1, Ordering::SeqCst);
            read_observed.notify_one();
            if attempt == 0 {
                Err(StoreError::Unreadable(
                    "the registry is temporarily unavailable".into(),
                ))
            } else {
                Ok(tables.clone())
            }
        });
        let registry = Arc::new(CronRegistry::new(Arc::new(repository), 10, chrono_tz::UTC));
        let controller = CronController::new(host.arc().clone(), registry);
        let running = tokio::spawn({
            let controller = controller.clone();
            async move { controller.run().await }
        });
        wait_for_reads(&reads, &read, 1).await;
        tokio::time::sleep(CRON_RETRY_DELAY / 2).await;
        assert_eq!(reads.load(Ordering::SeqCst), 1);
        wait_for_reads(&reads, &read, 2).await;
        tokio::time::timeout(Duration::from_secs(5), async {
            while controller.scheduling.lock().await.scheduler.next_run().is_none() {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("a recovered registry restores the timers in time");
        assert_eq!(reads.load(Ordering::SeqCst), 2);
        tokio::time::sleep(CRON_RETRY_DELAY + Duration::from_millis(100)).await;
        assert_eq!(reads.load(Ordering::SeqCst), 2);
        running.abort();
        let _ = running.await;
    }

    #[tokio::test]
    async fn a_failed_synchronization_waits_for_the_retry_delay_even_with_an_old_due_timer() {
        let host = named_host().await;
        let jobs = host.cron.jobs().await.unwrap();
        let reads = Arc::new(AtomicUsize::new(0));
        let observed = reads.clone();
        let read = Arc::new(Notify::new());
        let read_observed = read.clone();
        let mut repository = MockCronRepository::new();
        repository.expect_all().returning(move || {
            observed.fetch_add(1, Ordering::SeqCst);
            read_observed.notify_one();
            Err(StoreError::Unreadable("the registry remains unavailable".into()))
        });
        let registry = Arc::new(CronRegistry::new(Arc::new(repository), 10, chrono_tz::UTC));
        let controller = CronController::new(host.arc().clone(), registry);
        controller.scheduling.lock().await.scheduler.synchronize(
            &jobs,
            Utc::now() - chrono::Duration::minutes(2),
            chrono_tz::UTC,
        );
        let running = tokio::spawn({
            let controller = controller.clone();
            async move { controller.run().await }
        });
        wait_for_reads(&reads, &read, 1).await;
        tokio::time::sleep(CRON_RETRY_DELAY / 2).await;
        assert_eq!(reads.load(Ordering::SeqCst), 1);
        wait_for_reads(&reads, &read, 2).await;
        tokio::time::sleep(CRON_RETRY_DELAY / 2).await;
        assert_eq!(reads.load(Ordering::SeqCst), 2);
        running.abort();
        let _ = running.await;
    }

    mod declared {
        use object_store::memory::InMemory;
        use object_store::ObjectStoreExt;

        use super::*;
        use crate::adapters::logs::object_sink::tests::{read_lines, sink};
        use crate::adapters::logs::object_sink::{LogLine, TraceLine};
        use crate::adapters::otlp::trace::TraceId;
        use guest_contract::cron_execution::ExecutionRequest;

        const DUE: &str = "2026-10-24T16:15:00Z";
        const RECORD: &str = "cron/site/production/tick/2026-10-24T16:15:00Z";

        fn tick(edit: impl FnOnce(&mut serde_json::Value)) -> serde_json::Value {
            let mut job = serde_json::json!({
                "name": "tick", "schedule": "* * * * *", "command": "echo tick; exit ${CRON_EXIT:-0}",
                "timeoutS": 300, "backoffMs": []
            });
            edit(&mut job);
            job
        }

        /// A host whose one instance declares `job` and is labelled for a sink that writes to the
        /// store returned beside it.
        async fn declaring(
            job: serde_json::Value,
        ) -> (TestHost, Arc<crate::adapters::logs::ObjectSink>, Arc<InMemory>) {
            let mut host = test_host().await;
            let desired = desired_state(|state| {
                state.instances = vec![desired_instance(|instance| {
                    instance.cron = Some(serde_json::from_value(serde_json::json!([job])).unwrap());
                    instance.labels = serde_json::from_value(serde_json::json!({
                        "app": "site", "rev": "0123456789ab", "context": "production", "timeline": "production"
                    }))
                    .unwrap();
                })]
            });
            let (sink, store) = sink();
            sink.learn(&desired);
            let built = Arc::get_mut(&mut host.host).unwrap();
            built.telemetry = Some(sink.clone());
            built.logs = Arc::new(
                crate::adapters::logs::FileLogSink::new(built.config.logs_dir(), 1 << 20)
                    .with_objects(sink.clone()),
            );
            host.cache.lock().await.accept(desired.clone());
            host.state.modify(|snapshot| snapshot.isolated = true).await;
            host.state
                .put_record(instance_record(|record| {
                    record.state = InstanceState::Running;
                    record.desired_running = true;
                    record.started_at = Some(protocol::Timestamp::now());
                }))
                .await;
            host.cron
                .synchronize(&[(app_id(), deployment_id())])
                .await
                .unwrap();
            host.cron.declare(&desired.instances).await.unwrap();
            host.cron_runs.synchronize(&[(app_id(), deployment_id())]).await;
            (host, sink, store)
        }

        async fn due(host: &TestHost) -> ScheduledRun {
            ScheduledRun {
                app_id: app_id(),
                deployment_id: deployment_id(),
                job: host.cron.jobs().await.unwrap().remove(0).jobs.remove(0),
                scheduled_at: at(DUE),
            }
        }

        async fn record(store: &InMemory, key: &str) -> Option<serde_json::Value> {
            let got = store.get(&format!("fleets/acme/{key}").into()).await.ok()?;
            Some(serde_json::from_slice(&got.bytes().await.unwrap()).unwrap())
        }

        /// A guest that answers `exits.len()` attempts, each printing `tick` and then exiting with
        /// its code, or never exiting when it has none. Each request is handed over as it comes,
        /// with the attempt's record as the guest found it.
        fn guest(
            host: &TestHost,
            store: Arc<InMemory>,
            exits: Vec<Option<u32>>,
        ) -> (
            tokio::task::JoinHandle<()>,
            tokio::sync::mpsc::UnboundedReceiver<(ExecutionRequest, Option<serde_json::Value>)>,
        ) {
            let path = guest_vsock_path(host, &app_id());
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            let listener = UnixListener::bind(&path).unwrap();
            let (requests, received) = tokio::sync::mpsc::unbounded_channel();
            let task = tokio::spawn(async move {
                for (attempt, exit) in exits.into_iter().enumerate() {
                    let (stream, _) = listener.accept().await.unwrap();
                    let mut stream = BufReader::new(stream);
                    let mut connect = String::new();
                    stream.read_line(&mut connect).await.unwrap();
                    stream.get_mut().write_all(b"OK 1234\n").await.unwrap();
                    let mut header = [0; wire::HEADER_BYTES];
                    stream.read_exact(&mut header).await.unwrap();
                    let header = wire::decode_request_header(&header).unwrap();
                    let mut body = vec![0; header.body_length];
                    stream.read_exact(&mut body).await.unwrap();
                    let found = record(&store, &format!("{RECORD}/{}", attempt + 1)).await;
                    requests
                        .send((wire::decode_request(header, &body).unwrap(), found))
                        .unwrap();
                    for frame in [
                        ExecutionFrame::Started,
                        ExecutionFrame::Stdout(b"tick\n".to_vec()),
                    ] {
                        stream
                            .get_mut()
                            .write_all(&wire::encode_reply(&frame).unwrap())
                            .await
                            .unwrap();
                        assert_eq!(stream.read_u8().await.unwrap(), wire::ACK);
                    }
                    match exit {
                        Some(code) => stream
                            .get_mut()
                            .write_all(
                                &wire::encode_reply(&ExecutionFrame::Exit { code, signal: 0 }).unwrap(),
                            )
                            .await
                            .unwrap(),
                        None => assert!(stream.read_u8().await.is_err(), "the host hung up on it"),
                    }
                }
            });
            (task, received)
        }

        fn traceparent(request: &ExecutionRequest) -> (String, String) {
            let value = request
                .environment
                .iter()
                .find_map(|entry| entry.strip_prefix("TRACEPARENT="))
                .expect("the process is given its trace");
            let fields: Vec<_> = value.split('-').collect();
            assert_eq!((fields[0], fields[3]), ("00", "01"));
            (fields[1].to_owned(), fields[2].to_owned())
        }

        #[tokio::test]
        async fn an_attempt_is_recorded_before_it_runs_and_its_outcome_after_as_one_trace() {
            let (host, sink, store) = declaring(tick(|_| {})).await;
            let (guest, mut requests) = guest(&host, store.clone(), vec![Some(0)]);
            let run = due(&host).await;
            CronController::dispatch(host.arc().clone(), host.cron.clone(), run).await;
            guest.await.unwrap();
            let (request, found) = requests.recv().await.unwrap();
            let found = found.expect("the record is there before the command starts");
            assert!(found.get("ended").is_none());
            assert_eq!(found["host"], "host-1");
            assert_eq!(found["rev"], "0123456789ab");
            let (trace, span) = traceparent(&request);
            assert_eq!(found["trace"], trace.as_str());
            assert!(
                !TraceBook::host()
                    .admit(TraceId::parse(&trace).unwrap(), crate::clock::now_us())
                    .foreign
            );

            let ended = record(&store, &format!("{RECORD}/1")).await.unwrap();
            assert_eq!(
                (&ended["status"], &ended["exit"], ended.get("reason")),
                (&serde_json::json!("completed"), &serde_json::json!(0), None)
            );
            assert_eq!(ended["started"], found["started"]);

            sink.ship(crate::clock::now_ms() / 1000).await.unwrap();
            let lines: Vec<LogLine> = read_lines(&store, "logs/site/live-host-1.jsonl.gz").await;
            let said: Vec<_> = lines.iter().map(|line| line.msg.as_deref().unwrap()).collect();
            assert_eq!(
                said[0],
                "cron tick started (scheduled 2026-10-24T16:15:00Z, attempt 1)"
            );
            assert_eq!(said[1], "tick");
            assert!(
                said[2].starts_with("cron tick completed exit 0 after "),
                "{}",
                said[2]
            );
            for line in &lines {
                assert_eq!(line.job.as_deref(), Some("tick"));
                assert_eq!(line.run.as_deref(), Some("2026-10-24T16:15:00Z/1"));
                assert_eq!(
                    (line.trace.as_deref(), line.span.as_deref()),
                    (Some(trace.as_str()), Some(span.as_str()))
                );
            }
            let spans: Vec<TraceLine> = read_lines(&store, "traces/site/live-host-1.jsonl.gz").await;
            let [TraceLine::Span(root)] = spans.as_slice() else {
                panic!("one root span: {spans:?}")
            };
            assert!(root.root && root.parent.is_none());
            assert_eq!(
                (root.trace.as_str(), root.span.as_str()),
                (trace.as_str(), span.as_str())
            );
            assert_eq!(root.name, "cron tick");
            assert_eq!(root.kind, SpanKind::Internal);
            assert_eq!(root.status, Some(SpanStatus::Otel(OtelStatus::Ok)));
            let attrs = root.attrs.as_ref().unwrap();
            for (name, value) in [
                ("cron.name", serde_json::json!("tick")),
                ("cron.schedule", serde_json::json!("* * * * *")),
                ("cron.timeline", serde_json::json!("production")),
                ("cron.instant", serde_json::json!(DUE)),
                ("cron.attempt", serde_json::json!(1)),
                ("cron.manual", serde_json::json!(false)),
                ("process.exit_code", serde_json::json!(0)),
            ] {
                assert_eq!(attrs.get(name), Some(&value), "{name}");
            }
        }

        #[tokio::test]
        async fn a_single_run_is_made_once_and_a_restarted_host_finds_it_taken_and_prints_nothing() {
            let (host, sink, store) = declaring(tick(|job| {
                job.as_object_mut().unwrap().remove("schedule");
                job["at"] = serde_json::json!(DUE);
            }))
            .await;
            let (guest, _requests) = guest(&host, store.clone(), vec![Some(0)]);
            CronController::dispatch(host.arc().clone(), host.cron.clone(), due(&host).await).await;
            guest.await.unwrap();
            let made = record(&store, &format!("{RECORD}/1")).await.unwrap();
            assert_eq!(made["manual"], true);
            sink.ship(crate::clock::now_ms() / 1000).await.unwrap();
            store
                .delete(&"fleets/acme/logs/site/live-host-1.jsonl.gz".into())
                .await
                .unwrap();

            let restarted = CronController::new(host.arc().clone(), host.cron.clone());
            restarted.cron_once(at(DUE) - chrono::Duration::seconds(1)).await;
            restarted.cron_once(at(DUE)).await;
            while let Some(task) = restarted.scheduling.lock().await.tasks.join_next().await {
                task.unwrap();
            }
            assert_eq!(record(&store, &format!("{RECORD}/1")).await.unwrap(), made);
            sink.ship(crate::clock::now_ms() / 1000).await.unwrap();
            assert!(record(&store, "logs/site/live-host-1.jsonl.gz").await.is_none());
        }

        #[tokio::test]
        async fn an_attempt_past_its_timeout_has_its_process_killed_and_is_recorded_as_timed_out() {
            let (host, _sink, store) = declaring(tick(|job| job["timeoutS"] = serde_json::json!(1))).await;
            let (guest, _requests) = guest(&host, store.clone(), vec![None]);
            let started = std::time::Instant::now();
            CronController::dispatch(host.arc().clone(), host.cron.clone(), due(&host).await).await;
            tokio::time::timeout(Duration::from_secs(5), guest)
                .await
                .unwrap()
                .unwrap();
            assert!(started.elapsed() >= Duration::from_secs(1));
            let ended = record(&store, &format!("{RECORD}/1")).await.unwrap();
            assert_eq!(
                (&ended["status"], &ended["exit"], &ended["reason"]),
                (
                    &serde_json::json!("failed"),
                    &serde_json::json!(124),
                    &serde_json::json!("timeout")
                )
            );
        }

        #[tokio::test]
        async fn a_failed_attempt_is_tried_again_after_its_delay_under_a_new_key_and_trace_and_no_more() {
            let (host, _sink, store) = declaring(tick(|job| {
                job["schedule"] = serde_json::json!("@yearly");
                job["backoffMs"] = serde_json::json!([10]);
            }))
            .await;
            let (guest, mut requests) = guest(&host, store.clone(), vec![Some(1), Some(1)]);
            CronController::dispatch(host.arc().clone(), host.cron.clone(), due(&host).await).await;
            guest.await.unwrap();
            let first = traceparent(&requests.recv().await.unwrap().0);
            let second = traceparent(&requests.recv().await.unwrap().0);
            assert_ne!(first.0, second.0);
            for (attempt, trace) in [(1, &first.0), (2, &second.0)] {
                let ended = record(&store, &format!("{RECORD}/{attempt}")).await.unwrap();
                assert_eq!(
                    (&ended["status"], &ended["exit"]),
                    (&serde_json::json!("failed"), &serde_json::json!(1))
                );
                assert_eq!(ended["trace"], trace.as_str());
            }
            assert!(record(&store, &format!("{RECORD}/3")).await.is_none());
        }

        #[tokio::test]
        async fn a_job_due_while_it_still_runs_is_recorded_as_skipped() {
            let (host, _sink, store) = declaring(tick(|_| {})).await;
            let _running = host.cron_runs.start(&app_id(), &deployment_id(), "tick").unwrap();
            let controller = CronController::new(host.arc().clone(), host.cron.clone());
            controller
                .cron_once(at(DUE) - chrono::Duration::seconds(30))
                .await;
            controller.cron_once(at(DUE)).await;
            while let Some(task) = controller.scheduling.lock().await.tasks.join_next().await {
                task.unwrap();
            }
            let skipped = record(&store, &format!("{RECORD}/1")).await.unwrap();
            assert_eq!(
                (&skipped["status"], &skipped["reason"]),
                (
                    &serde_json::json!("failed"),
                    &serde_json::json!("skipped: previous run still running")
                )
            );
            assert!(skipped.get("trace").is_none());
        }
    }
}
