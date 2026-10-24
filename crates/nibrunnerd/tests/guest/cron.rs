use std::sync::Mutex;

use chrono::Utc;
use guest_contract::cron_execution::{self as wire, ExecutionFrame, ExecutionRequest};
use nibrunnerd::adapters::cron_execution::{CronExitStatus, GuestCronExecution};
use nibrunnerd::controllers::cron_controller::CronController;
use nibrunnerd::domain::cron::execution::{AttemptTrace, CronRunOutput};
use nibrunnerd::domain::cron::jobs::CronJobs;
use nibrunnerd::domain::cron::scheduler::ScheduledRun;
use nibrunnerd::domain::filesystem::reader::guest_vsock_path;
use nibrunnerd::ports::{LogSink, TenantLogBody, TenantLogEvent};
use nibrunnerd::test_support::machine::{RunningHost, Tenant};
use protocol::{
    CronCommand, CronJobDefinition, CronSchedule, CronTable, DesiredInstanceState, InstanceState,
};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;

#[derive(Default)]
struct Output(Mutex<Vec<TenantLogEvent>>);

#[async_trait::async_trait]
impl LogSink for Output {
    async fn publish(&self, events: Vec<TenantLogEvent>) {
        self.0.lock().unwrap().extend(events);
    }
}

impl Output {
    fn text(&self) -> String {
        self.0
            .lock()
            .unwrap()
            .iter()
            .filter_map(|event| match &event.body {
                TenantLogBody::Data { text, .. } | TenantLogBody::CronData { text, .. } => {
                    Some(text.as_str())
                }
                _ => None,
            })
            .collect::<Vec<_>>()
            .join("\n")
    }
}

struct GuestRun(BufReader<UnixStream>);

impl GuestRun {
    async fn start(host: &RunningHost, app: &Tenant, command: &str) -> Self {
        Self::start_request(
            host,
            app,
            ExecutionRequest {
                command: command.into(),
                environment: vec![],
            },
        )
        .await
    }

    async fn start_request(host: &RunningHost, app: &Tenant, request: ExecutionRequest) -> Self {
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            let stream = UnixStream::connect(guest_vsock_path(&host.host, &app.app_id))
                .await
                .unwrap();
            let mut run = Self(BufReader::new(stream));
            let port = guest_contract::vsock::CRON_EXECUTION_PORT;
            run.0
                .get_mut()
                .write_all(guest_contract::vsock::connect_request(port).as_bytes())
                .await
                .unwrap();
            let mut connected = String::new();
            run.0.read_line(&mut connected).await.unwrap();
            guest_contract::vsock::read_connect_reply(&connected, port).unwrap();
            let request = wire::encode_request(&request).unwrap();
            run.0.get_mut().write_all(&request).await.unwrap();
            assert_eq!(run.receive().await, ExecutionFrame::Started);
            run.ack().await;
            run
        })
        .await
        .expect("the guest starts an independent cron worker")
    }

    async fn receive(&mut self) -> ExecutionFrame {
        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            let mut header = [0; wire::HEADER_BYTES];
            self.0.read_exact(&mut header).await.unwrap();
            let header = wire::decode_reply_header(&header).unwrap();
            let mut body = vec![0; header.body_length];
            self.0.read_exact(&mut body).await.unwrap();
            wire::decode_reply(header, &body).unwrap()
        })
        .await
        .expect("the guest delivers its next cron frame")
    }

    async fn ack(&mut self) {
        self.0.get_mut().write_all(&[wire::ACK]).await.unwrap();
    }

    async fn pid(&mut self) -> u32 {
        let ExecutionFrame::Stdout(bytes) = self.receive().await else {
            panic!("the command prints its process group leader before waiting");
        };
        self.ack().await;
        std::str::from_utf8(&bytes).unwrap().trim().parse().unwrap()
    }
}

async fn command(host: &RunningHost, app: &Tenant, command: &str, output: &Output) -> CronExitStatus {
    let table = CronTable {
        app_id: app.app_id.clone(),
        deployment_id: app.instance.deployment_id.clone(),
        jobs: vec![CronJobDefinition {
            schedule: CronSchedule::parse("@daily").unwrap(),
            command: CronCommand::parse(command).unwrap(),
            environment: None,
        }]
        .into(),
        crontab: None,
    };
    let run = ScheduledRun {
        app_id: app.app_id.clone(),
        deployment_id: app.instance.deployment_id.clone(),
        job: CronJobs::registered(&table).jobs.remove(0),
        scheduled_at: Utc::now(),
    };
    let trace = AttemptTrace {
        trace: "019a1b2c3d4e0123456789abcdef0123".into(),
        span: "0123456789abcdef".into(),
    };
    tokio::time::timeout(
        std::time::Duration::from_secs(15),
        GuestCronExecution::run(
            &guest_vsock_path(&host.host, &app.app_id),
            &mut CronRunOutput::new(run, 1, trace),
            output,
            std::future::pending(),
        ),
    )
    .await
    .expect("the guest command completes")
    .expect("the guest accepted the command")
}

#[tokio::test(flavor = "multi_thread")]
async fn a_tenant_installs_lists_and_removes_its_crontab_through_the_host() {
    let Some(host) = crate::host().await else {
        return;
    };
    let app = host.tenant(1);
    host.deploy(std::slice::from_ref(&app)).await;
    host.until_state(&app.app_id, InstanceState::Running).await;
    let output = Output::default();
    let status = command(&host, &app, r#"printf '@daily echo registered\n' > jobs; crontab jobs && crontab -l && printf 'uid=%s cwd=%s\n' "$(id -u)" "$PWD""#, &output).await;
    assert_eq!(status, CronExitStatus { code: 0, signal: 0 });
    assert!(
        output.text().contains("@daily echo registered"),
        "{}",
        output.text()
    );
    assert!(output.text().contains("uid=65534 cwd=/app"), "{}", output.text());
    let registered = host
        .host
        .cron
        .list(&app.app_id, &app.instance.deployment_id)
        .await
        .unwrap();
    assert_eq!(registered.expose(), "@daily echo registered\n");
    let status = command(&host, &app, "crontab -r && crontab -l", &Output::default()).await;
    assert_eq!(status, CronExitStatus { code: 0, signal: 0 });
    assert!(host
        .host
        .cron
        .list(&app.app_id, &app.instance.deployment_id)
        .await
        .unwrap()
        .expose()
        .is_empty());
    host.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn an_invalid_crontab_cannot_replace_a_tenants_installed_jobs() {
    let Some(host) = crate::host().await else {
        return;
    };
    let app = host.tenant(1);
    host.deploy(std::slice::from_ref(&app)).await;
    host.until_state(&app.app_id, InstanceState::Running).await;
    let status = command(
        &host,
        &app,
        r"printf '@daily echo retained\n' | crontab -",
        &Output::default(),
    )
    .await;
    assert_eq!(status.code, 0);
    let output = Output::default();
    let status = command(
        &host,
        &app,
        r"printf 'invalid tenant-secret\n' | crontab -",
        &output,
    )
    .await;
    assert_ne!(status.code, 0);
    assert!(!output.text().contains("tenant-secret"));
    assert_eq!(
        host.host
            .cron
            .list(&app.app_id, &app.instance.deployment_id)
            .await
            .unwrap()
            .expose(),
        "@daily echo retained\n"
    );
    host.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn disconnecting_one_overlapping_run_cancels_its_group_and_preserves_the_other() {
    let Some(host) = crate::host().await else {
        return;
    };
    let app = host.tenant(1);
    host.deploy(std::slice::from_ref(&app)).await;
    host.until_state(&app.app_id, InstanceState::Running).await;
    let long_command = r#"printf '%s\n' "$$"; while :; do sleep 1; done"#;
    let mut first = GuestRun::start(&host, &app, long_command).await;
    let first_pid = first.pid().await;
    let mut second = GuestRun::start(&host, &app, long_command).await;
    let second_pid = second.pid().await;
    assert_ne!(first_pid, second_pid);
    let inspect = format!("kill -0 -{first_pid} && kill -0 -{second_pid}");
    assert_eq!(command(&host, &app, &inspect, &Output::default()).await.code, 0);
    drop(first);
    let inspect = format!("if kill -0 -{first_pid} 2>/dev/null; then printf first-alive; else printf first-gone; fi; if kill -0 -{second_pid} 2>/dev/null; then printf second-alive; else printf second-gone; fi");
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        let output = Output::default();
        assert_eq!(command(&host, &app, &inspect, &output).await.code, 0);
        if output.text().contains("first-gonesecond-alive") {
            break;
        }
        assert!(tokio::time::Instant::now() < deadline, "{}", output.text());
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    drop(second);
    let inspect = format!("if kill -0 -{second_pid} 2>/dev/null; then printf alive; else printf gone; fi");
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        let output = Output::default();
        command(&host, &app, &inspect, &output).await;
        if output.text().lines().any(|line| line == "gone") {
            break;
        }
        assert!(tokio::time::Instant::now() < deadline, "{}", output.text());
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    host.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_finished_run_drains_its_output_without_waiting_for_background_children() {
    let Some(host) = crate::host().await else {
        return;
    };
    let app = host.tenant(1);
    host.deploy(std::slice::from_ref(&app)).await;
    host.until_state(&app.app_id, InstanceState::Running).await;
    let output = Output::default();
    let status = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        command(&host, &app, "sleep 30 & printf drained; exit 9", &output),
    )
    .await
    .expect("finished shell descendants cannot keep a cron connection open");
    assert_eq!(status, CronExitStatus { code: 9, signal: 0 });
    assert!(output.text().lines().any(|line| line == "drained"));
    host.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_registered_environment_overrides_app_values_and_selects_its_shell() {
    let Some(host) = crate::host().await else {
        return;
    };
    let app = host.tenant(1).edited(|instance| {
        instance.config.command.environment =
            serde_json::from_value(serde_json::json!({"HOME": "/app/tenant", "TOKEN": "instance-secret"}))
                .unwrap();
    });
    host.deploy(std::slice::from_ref(&app)).await;
    host.until_state(&app.app_id, InstanceState::Running).await;
    let request = ExecutionRequest {
        command: r#"printf '%s|%s|%s|%s|%s|%s|%s' "$HOME" "$TMPDIR" "$PATH" "$TOKEN" "$VALUE" "$PORT" "${BASH_VERSION:+bash}"; printf stderr >&2; exit 7"#.into(),
        environment: vec!["HOME=/app/job".into(), "VALUE=$literal 'quoted' héllo".into(), "SHELL=/bin/bash".into()],
    };
    let mut run = GuestRun::start_request(&host, &app, request).await;
    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    loop {
        match run.receive().await {
            ExecutionFrame::Stdout(bytes) => stdout.extend(bytes),
            ExecutionFrame::Stderr(bytes) => stderr.extend(bytes),
            ExecutionFrame::Exit { code, signal } => {
                assert_eq!((code, signal), (7, 0));
                break;
            }
            other => panic!("unexpected cron frame: {other:?}"),
        }
        run.ack().await;
    }
    assert_eq!(
        std::str::from_utf8(&stdout).unwrap(),
        "/app/job|/tmp|/usr/bin:/bin|instance-secret|$literal 'quoted' héllo|3000|bash"
    );
    assert_eq!(stderr, b"stderr");
    let request = ExecutionRequest {
        command: "true".into(),
        environment: vec!["SHELL=/missing-shell".into()],
    };
    let mut run = GuestRun::start_request(&host, &app, request).await;
    let mut errors = Vec::new();
    loop {
        match run.receive().await {
            ExecutionFrame::Stderr(bytes) => {
                errors.extend(bytes);
                run.ack().await;
            }
            ExecutionFrame::Exit { code, signal } => {
                assert_eq!((code, signal), (126, 0));
                break;
            }
            other => panic!("unexpected failed launch frame: {other:?}"),
        }
    }
    assert!(std::str::from_utf8(&errors)
        .unwrap()
        .contains("could not be started"));
    assert_eq!(
        command(&host, &app, "kill -34 $$", &Output::default()).await,
        CronExitStatus { code: 0, signal: 34 }
    );
    host.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_due_registered_job_wakes_and_pins_its_guest_until_desired_stop_drains_it() {
    let Some(host) = crate::host().await else {
        return;
    };
    let app = host.tenant(1).on_request(60_000);
    host.deploy(std::slice::from_ref(&app)).await;
    host.until_state(&app.app_id, InstanceState::Running).await;
    let installed = command(&host, &app, r"printf '@yearly mkdir -p /app/data; echo $$ > /app/data/cron-started; while :; do sleep 1; done\n' | crontab -", &Output::default()).await;
    assert_eq!(installed, CronExitStatus { code: 0, signal: 0 });
    host.let_sleep(&app).await;
    let restores_before = host.host.metrics.sleep_wake.of(&app.app_id).wakes[0];
    let controller = CronController::new(host.host.clone(), host.host.cron.clone());
    controller
        .cron_once("2030-12-31T23:59:00Z".parse().unwrap())
        .await;
    assert!(!host.host.state.cron_running(&app.app_id));
    assert_eq!(
        host.instance(&app.app_id).await.unwrap().state,
        InstanceState::Idle
    );
    controller
        .cron_once("2031-01-01T00:00:00Z".parse().unwrap())
        .await;
    tokio::time::timeout(std::time::Duration::from_secs(15), async {
        loop {
            if host.host.state.cron_running(&app.app_id)
                && host
                    .instance(&app.app_id)
                    .await
                    .is_some_and(|instance| instance.state == InstanceState::Running)
            {
                let answer = host.get(&app, "/read?path=cron-started").await.unwrap();
                if answer.status == 200 {
                    assert!(answer.body.trim().parse::<u32>().unwrap() > 1);
                    break;
                }
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("a due job wakes its guest and starts the registered command");
    assert_eq!(
        host.host.metrics.sleep_wake.of(&app.app_id).wakes[0],
        restores_before + 1
    );
    nibrunnerd::test_support::measured_quiet_since(
        &host.host.state,
        &app.app_id,
        Utc::now().timestamp_millis() - i64::try_from(protocol::MAX_IDLE_TIMEOUT_MS).unwrap(),
    )
    .await;
    nibrunnerd::domain::reconcile::idle::apply_sleep(&host.host).await;
    assert!(host.host.state.cron_running(&app.app_id));
    assert_eq!(
        host.instance(&app.app_id).await.unwrap().state,
        InstanceState::Running
    );
    let stopped = app
        .clone()
        .edited(|instance| instance.desired_state = DesiredInstanceState::Stopped);
    tokio::time::timeout(std::time::Duration::from_secs(15), async {
        host.deploy(std::slice::from_ref(&stopped)).await;
        host.until_state(&app.app_id, InstanceState::Stopped).await;
        while host.host.state.cron_running(&app.app_id) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("desired stop drains the registered run and stops the guest");
    assert!(host
        .host
        .cron_runs
        .start(&app.app_id, &app.instance.deployment_id, "registered")
        .is_err());
    controller
        .cron_once("2032-01-01T00:00:00Z".parse().unwrap())
        .await;
    assert!(!host.host.state.cron_running(&app.app_id));
    host.stop().await;
}
