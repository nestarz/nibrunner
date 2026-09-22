//! The document is the only input: what it names is served, what it stops naming is gone, and
//! what it says about an app is true of the machine.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::Duration;

use protocol::{DesiredInstanceState, DesiredPresence, InstanceState, VolumeState};

#[tokio::test(flavor = "multi_thread")]
async fn an_app_the_document_names_answers_on_its_hostname() {
    let Some(host) = crate::host().await else {
        return;
    };
    let app = host.tenant(1);
    host.deploy(std::slice::from_ref(&app)).await;
    let took = host.until_state(&app.app_id, InstanceState::Running).await;
    println!("running {took:?} after the document named it");

    let answer = host.get(&app, "/").await.expect("the proxy answers");
    assert_eq!(answer.status, 200, "{answer:?}");
    assert_eq!(answer.body, "ok");

    host.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn an_app_the_document_stopped_naming_leaves_nothing_behind() {
    let Some(host) = crate::host().await else {
        return;
    };
    let app = host.tenant(1);
    host.deploy(std::slice::from_ref(&app)).await;
    host.until_state(&app.app_id, InstanceState::Running).await;
    host.get(&app, "/log?lines=3").await.expect("the proxy answers");

    let slot = host
        .host
        .slot_of(&app.app_id)
        .await
        .expect("a running app holds a slot");
    let volume_file = host.host.config.volumes_dir().join(app.volume_id.as_str());
    let log_file = host.host.config.logs_dir().join(format!("{}.log", app.app_id));
    assert!(volume_file.exists(), "the volume was never written");

    // Removing an app is two writes, and the first is what does the deleting: while the document
    // still names a volume it has told the host to let go of, the host goes on reporting it — as
    // one it has deleted, which is the honest answer to a document that asked about it.
    let mut leaving = host.document(&[]);
    leaving.volumes = vec![protocol::DesiredVolume {
        desired_state: DesiredPresence::Absent,
        ..app.volume.clone()
    }];
    host.write(&leaving).await;
    host.until("its volume to be deleted", |report| {
        report.instances.is_empty()
            && report
                .volumes
                .iter()
                .all(|volume| volume.state == VolumeState::Deleted)
    })
    .await;

    host.write(&host.document(&[])).await;
    host.until("nothing of it to be left on this host", |report| {
        report.instances.is_empty() && report.volumes.is_empty()
    })
    .await;

    assert!(
        !host.host.vms.tap_names().await.contains(&slot.tap_name),
        "the tap it was given is still here"
    );
    assert!(!volume_file.exists(), "its volume is still on the disk");
    assert!(!log_file.exists(), "its log is still on the disk");
    assert!(
        !host
            .host
            .firewall
            .traffic()
            .await
            .unwrap_or_default()
            .contains_key(&app.app_id),
        "the ruleset still counts for it"
    );
    assert_ne!(
        host.get(&app, "/").await.map(|answer| answer.status).unwrap_or(0),
        200,
        "its hostname is still being served"
    );

    host.stop().await;
}

// Everything in the report is something a control plane will act on, so every field of it has to
// be a fact about this machine rather than about what the daemon meant to do.
#[tokio::test(flavor = "multi_thread")]
async fn what_the_report_says_about_an_app_is_true_of_the_machine() {
    let Some(host) = crate::host().await else {
        return;
    };
    let app = host.tenant(1);
    host.deploy(std::slice::from_ref(&app)).await;
    host.until_state(&app.app_id, InstanceState::Running).await;

    let reported = host.instance(&app.app_id).await.expect("a record");
    let slot = host.host.slot_of(&app.app_id).await.expect("a slot");

    host.until_routed(&app).await;

    // Twice over: the rule the kernel is holding, and a request that goes through it. The proxy
    // reaches every app this same way, over loopback into the port the slot gave it.
    let port = reported.host_port.expect("a running app is reachable somewhere");
    let forwarded = format!(
        "tcp dport {} dnat to {}:{}",
        port.get(),
        slot.guest_ipv4.as_str(),
        app.instance.config.http_port
    );
    let ruleset = host.ruleset().await;
    assert!(
        ruleset.contains(&forwarded),
        "the report gives out a port the kernel does not forward: looked for `{forwarded}` in\n{ruleset}"
    );

    let answer = host
        .get_at(
            SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port.get()),
            &app.hostname,
            "/",
        )
        .await
        .expect("the port the report names answers");
    assert_eq!(answer.status, 200, "{answer:?}");

    assert_eq!(
        reported.guest_ipv4.as_ref(),
        Some(&slot.guest_ipv4),
        "the report names an address the slot does not hold"
    );
    assert!(
        reported.layer_digests.contains(&host.tenant_digest),
        "the report does not name the layer this app was given"
    );
    assert_eq!(reported.deployment_id, app.instance.deployment_id);

    host.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_document_this_host_refused_changes_nothing_about_the_app_it_is_already_serving() {
    let Some(host) = crate::host().await else {
        return;
    };
    let app = host.tenant(1);
    host.deploy(std::slice::from_ref(&app)).await;
    host.until_state(&app.app_id, InstanceState::Running).await;
    let taken_up = host.host.accepted_document().await;

    // A running instance may not name a sleep policy: nothing would wake it again, and the
    // protocol refuses the whole document rather than the one field.
    let refused = host.document(std::slice::from_ref(&app.clone().edited(|instance| {
        instance.desired_state = DesiredInstanceState::Running;
        instance.activation = Some(protocol::ActivationPolicy {
            sleep_when: protocol::SleepPolicy::TrafficIdle {
                timeout_ms: protocol::DEFAULT_IDLE_TIMEOUT,
            },
        });
    })));
    host.write(&refused).await;

    let watching = std::time::Instant::now();
    while watching.elapsed() < Duration::from_secs(5) {
        assert_eq!(
            host.host.accepted_document().await,
            taken_up,
            "a refused document was taken up"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    let answer = host.get(&app, "/").await.expect("the proxy still answers");
    assert_eq!(answer.status, 200, "{answer:?}");

    host.stop().await;
}

// The window the 2026-09-14 run found three times: the record said running, and a connection that
// arrived in the next hundred milliseconds was refused rather than held.
#[tokio::test(flavor = "multi_thread")]
async fn every_connection_that_arrives_once_an_app_is_reported_running_is_answered() {
    let Some(host) = crate::host().await else {
        return;
    };
    let app = host.tenant(1);
    host.deploy(std::slice::from_ref(&app)).await;

    loop {
        if host.instance(&app.app_id).await.map(|held| held.state) == Some(InstanceState::Running) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    let mut turned_away = Vec::new();
    for attempt in 0..100 {
        match host.get(&app, "/").await {
            Ok(answer) if answer.status == 200 => {}
            Ok(answer) => turned_away.push(format!("{attempt}: {}", answer.status)),
            Err(error) => turned_away.push(format!("{attempt}: {error}")),
        }
    }
    assert!(
        turned_away.is_empty(),
        "{} of 100 connections after `running` were turned away: {}",
        turned_away.len(),
        turned_away.join(", ")
    );

    host.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn an_app_the_document_stopped_has_no_microvm_of_its_own_left_running() {
    let Some(host) = crate::host().await else {
        return;
    };
    let app = host.tenant(1);
    host.deploy(std::slice::from_ref(&app)).await;
    host.until_state(&app.app_id, InstanceState::Running).await;

    let stopped = app.clone().edited(|instance| {
        instance.desired_state = DesiredInstanceState::Stopped;
    });
    host.deploy(std::slice::from_ref(&stopped)).await;
    host.until_state(&app.app_id, InstanceState::Stopped).await;

    let statuses = host.host.vms.statuses(std::slice::from_ref(&app.app_id)).await;
    assert!(
        !statuses.get(&app.app_id).is_some_and(|status| status.active),
        "a stopped app still has a microVM: {statuses:?}"
    );

    host.stop().await;
}
