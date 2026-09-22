//! What the tenant writes and what becomes of it when it misbehaves: the output path from the
//! guest's stdout to a file on this host, what bounds that file, and what a host does with a
//! program that dies or never listens.

use std::time::{Duration, Instant};

use nibrunnerd::test_support::machine::RunningHost;
use protocol::{HealthCheck, InstanceState, Probe};

const KEEP_BYTES: u64 = 256 * 1024;

/// Short enough that a test about an unhealthy app is not a test about waiting.
const IMPATIENT: HealthCheck = HealthCheck::Tcp {
    probe: Probe {
        interval_ms: 1_000,
        timeout_ms: 500,
        grace_period_ms: 3_000,
        healthy_threshold: 1,
        unhealthy_threshold: 2,
    },
};

fn log_path(host: &RunningHost, app_id: &protocol::AppId) -> std::path::PathBuf {
    host.host.config.logs_dir().join(format!("{app_id}.log"))
}

/// What the tenant wrote, without the header each record carries.
fn lines_written(held: &str) -> Vec<u64> {
    held.lines()
        .filter_map(|record| record.rsplit_once("line "))
        .filter_map(|(_, number)| number.trim().parse().ok())
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn every_line_a_tenant_wrote_reaches_this_host_in_the_order_it_wrote_them() {
    const LINES: u64 = 20_000;

    let Some(host) = crate::host().await else {
        return;
    };
    let app = host.tenant(1);
    host.deploy(std::slice::from_ref(&app)).await;
    host.until_state(&app.app_id, InstanceState::Running).await;

    let answer = host
        .get(&app, &format!("/log?lines={LINES}"))
        .await
        .expect("the proxy answers");
    assert_eq!(answer.body, LINES.to_string(), "{answer:?}");

    let path = log_path(&host, &app.app_id);
    let waiting = Instant::now();
    let numbers = loop {
        let held = std::fs::read_to_string(&path).unwrap_or_default();
        let numbers = lines_written(&held);
        if numbers.last() == Some(&LINES) {
            break numbers;
        }
        assert!(
            waiting.elapsed() < Duration::from_secs(60),
            "{} of {LINES} lines arrived in {:?}",
            numbers.len(),
            waiting.elapsed()
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    };
    println!("{LINES} lines in {:?}", waiting.elapsed());

    let expected: Vec<u64> = (1..=LINES).collect();
    assert_eq!(
        numbers, expected,
        "what arrived is not what the tenant wrote, in the order it wrote it"
    );

    host.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_tenants_output_stops_growing_where_this_host_says_it_keeps_it() {
    const LINES: u64 = 60_000;

    let Some(host) = nibrunnerd::test_support::machine::started_with(|config| {
        config.logs.keep_bytes_per_app = KEEP_BYTES;
    })
    .await
    else {
        return;
    };
    let app = host.tenant(1);
    host.deploy(std::slice::from_ref(&app)).await;
    host.until_state(&app.app_id, InstanceState::Running).await;

    host.get(&app, &format!("/log?lines={LINES}"))
        .await
        .expect("the proxy answers");

    // Far more than is kept, so the file has to have been cut rather than merely not filled.
    let directory = host.host.config.logs_dir();
    let waiting = Instant::now();
    loop {
        let previous = directory.join(format!("{}.log.1", app.app_id));
        if previous.exists() {
            break;
        }
        assert!(
            waiting.elapsed() < Duration::from_secs(60),
            "the output was never cut: {:?}",
            std::fs::read_dir(&directory)
                .map(|held| held.flatten().map(|entry| entry.file_name()).collect::<Vec<_>>())
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    tokio::time::sleep(Duration::from_secs(2)).await;
    let mut kept = 0;
    let mut files = Vec::new();
    for entry in std::fs::read_dir(&directory)
        .expect("the log directory")
        .flatten()
    {
        kept += entry.metadata().map(|held| held.len()).unwrap_or(0);
        files.push(entry.file_name().to_string_lossy().to_string());
    }
    files.sort();
    assert_eq!(
        files,
        vec![format!("{}.log", app.app_id), format!("{}.log.1", app.app_id)],
        "the output was kept in more places than the two this host cuts between"
    );
    assert!(
        kept <= KEEP_BYTES * 3,
        "{kept} bytes kept for a host that says it keeps {KEEP_BYTES}"
    );
    println!("{kept} bytes kept of the {LINES} lines written");

    host.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_tenant_that_exited_is_started_again_and_the_report_says_so() {
    let Some(host) = crate::host().await else {
        return;
    };
    let app = host.tenant(1);
    host.deploy(std::slice::from_ref(&app)).await;
    host.until_state(&app.app_id, InstanceState::Running).await;
    assert_eq!(
        host.instance(&app.app_id).await.map(|held| held.restart_count),
        Some(0)
    );

    host.get(&app, "/exit?code=3").await.expect("the proxy answers");

    host.until("the guest to have started its tenant again", |report| {
        report
            .instances
            .iter()
            .any(|instance| instance.app_id == app.app_id && instance.restart_count > 0)
    })
    .await;

    let waiting = Instant::now();
    loop {
        match host.get(&app, "/").await {
            Ok(answer) if answer.status == 200 => break,
            other => assert!(
                waiting.elapsed() < Duration::from_secs(60),
                "it never came back: {other:?}"
            ),
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    let reported = host.instance(&app.app_id).await.expect("a record");
    assert_eq!(reported.restart_count, 1);
    // `lastExitCode` is the microVM's; a tenant that died inside a guest that kept running is on
    // the restart record instead.
    let restart = reported
        .last_restart
        .expect("the report lost that it was restarted");
    assert_eq!(
        restart.restart.exit,
        protocol::TenantExit::Code(3),
        "the report lost how it died: {restart:?}"
    );

    host.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn an_app_whose_program_never_listens_is_reported_unhealthy_and_says_why() {
    let Some(host) = crate::host().await else {
        return;
    };
    let app = host
        .tenant(1)
        .arguments(&["--never-listen"])
        .edited(|instance| instance.config.health_check = IMPATIENT);
    host.deploy(std::slice::from_ref(&app)).await;

    // Unhealthy first and failed once its probes run out; which of the two is caught depends on
    // how fast the machine is, and either is this host saying the app is not well.
    host.until("app-1 to be reported unwell", |report| {
        report.instances.iter().any(|instance| {
            instance.app_id == app.app_id
                && matches!(instance.state, InstanceState::Unhealthy | InstanceState::Failed)
        })
    })
    .await;
    let reported = host.instance(&app.app_id).await.expect("a record");
    let said = reported
        .message
        .as_ref()
        .expect("an app that is not well and does not say why is one nobody can act on");
    assert!(
        said.as_str().contains(&app.instance.config.http_port.to_string()),
        "it does not name the port nothing answered on: {said}"
    );
    println!("said: {said}");

    host.stop().await;
}
