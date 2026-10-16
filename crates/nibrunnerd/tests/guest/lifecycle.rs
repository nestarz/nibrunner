//! The document is the only input: what it names is served, what it stops naming is gone, and
//! what it says about an app is true of the machine.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::Duration;

use protocol::{DesiredInstanceState, DesiredPresence, InstanceState, VolumeState};

// Docker's official BusyBox 1.37.0-musl index; the digest fixes both the image and its platform selection.
const BUSYBOX_INDEX: &str = "5cec3fc171c87218698e85a52af7087de727372aae264a787b8112901a5b0092";

#[tokio::test(flavor = "multi_thread")]
async fn a_guest_that_initialized_releases_start_capacity_before_its_app_is_healthy() {
    let Some(host) =
        crate::host_with(|config| config.max_concurrent_vm_starts = std::num::NonZeroU16::new(1).unwrap())
            .await
    else {
        return;
    };
    let waiting = host.tenant(1).arguments(&["--never-listen"]).edited(|instance| {
        instance.config.health_check = protocol::HealthCheck::Tcp {
            probe: protocol::Probe {
                interval_ms: 1000,
                timeout_ms: 100,
                grace_period_ms: 120_000,
                healthy_threshold: 1,
                unhealthy_threshold: 3,
            },
        };
    });
    host.deploy(std::slice::from_ref(&waiting)).await;
    host.until_state(&waiting.app_id, InstanceState::Starting).await;
    let answering = host.tenant(2);
    host.deploy(&[waiting.clone(), answering.clone()]).await;
    host.until_state(&answering.app_id, InstanceState::Running).await;
    assert_eq!(
        host.instance(&waiting.app_id).await.unwrap().state,
        InstanceState::Starting
    );
    assert_eq!(host.get(&answering, "/").await.unwrap().status, 200);
    host.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_public_busybox_image_serves_a_cow_through_a_jailed_microvm() {
    let Some(host) = crate::host().await else {
        return;
    };
    let app = host.tenant(1).edited(|instance| {
        instance.layers = vec![protocol::DesiredLayer::Oci { source: protocol::OciSource::Registry {
            repository: protocol::OciRepository::parse("docker.io/library/busybox").unwrap(),
            digest: protocol::Sha256Digest::parse(BUSYBOX_INDEX).unwrap(),
        }, }];
        instance.config.command.program = protocol::GuestPath::parse("/bin/busybox").unwrap();
        instance.config.command.args = vec![
            "sh".to_string(),
            "-c".to_string(),
            r#"printf '%s\n' '<pre>moo from a microVM' '  ^__^' '  (oo)\_______' '  (__)\       )\/\' '      ||----w |' '      ||     ||' "uid=$(/bin/busybox id -u)" '</pre>' > index.html; exec /bin/busybox httpd -f -p 0.0.0.0:3000 -h /app"#.to_string(),
        ].try_into().unwrap();
    });
    host.deploy(std::slice::from_ref(&app)).await;
    host.until_state(&app.app_id, InstanceState::Running).await;
    let answer = host
        .get(&app, "/")
        .await
        .expect("BusyBox answers through the proxy");
    assert_eq!(answer.status, 200, "{answer:?}");
    assert!(answer.body.contains("moo from a microVM"), "{answer:?}");
    assert!(answer.body.contains("(oo)"), "{answer:?}");
    assert!(answer.body.contains("uid=65534"), "{answer:?}");
    assert_eq!(
        host.instance(&app.app_id).await.unwrap().layer_digests,
        vec![protocol::Sha256Digest::parse(BUSYBOX_INDEX).unwrap()]
    );
    let cached = nibrunnerd::adapters::vm::layers::layer_image_path(
        &host.host.config.artifact_cache_dir(),
        &app.instance.layers[0],
    );
    assert!(cached.exists());
    let again = host.host.payloads.prepare(&app.instance.layers).await.unwrap();
    assert_eq!(again.fetched_bytes, 0);
    host.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn an_oci_archive_runs_the_explicit_command_from_its_filesystem_layer() {
    use sha2::{Digest, Sha256};
    let Some(host) = crate::host().await else {
        return;
    };
    let store = std::path::Path::new(&host.host.config.artifact_store_url);
    let program = std::fs::read(store.join("tenant")).expect("the host's static tenant");
    let archive = nibrunnerd::test_support::oci::archive(&program);
    std::fs::write(store.join("tenant-oci"), &archive).unwrap();
    let app = host.tenant(1).edited(|instance| {
        instance.layers = vec![protocol::DesiredLayer::Oci {
            source: protocol::OciSource::Archive(protocol::StoredObject {
                digest: protocol::Sha256Digest::parse(hex::encode(Sha256::digest(&archive))).unwrap(),
                object_key: protocol::ObjectKey::parse("tenant-oci").unwrap(),
            }),
        }];
    });
    host.deploy(std::slice::from_ref(&app)).await;
    host.until_state(&app.app_id, InstanceState::Running).await;
    let answer = host
        .get(&app, "/")
        .await
        .expect("the OCI tenant answers through the proxy");
    assert_eq!(answer.status, 200, "{answer:?}");
    let image = nibrunnerd::adapters::vm::layers::layer_image_path(
        &host.host.config.artifact_cache_dir(),
        &app.instance.layers[0],
    );
    assert!(image.exists());
    host.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_killed_microvm_is_replaced_without_losing_its_disk_or_interrupting_its_neighbour() {
    use nibrunnerd::adapters::vm::process::VmProcesses;
    let Some(host) = crate::host().await else {
        return;
    };
    let app = host.tenant(1);
    let neighbour = host.tenant(2);
    host.deploy(&[app.clone(), neighbour.clone()]).await;
    host.until_state(&app.app_id, InstanceState::Running).await;
    host.until_state(&neighbour.app_id, InstanceState::Running).await;
    assert_eq!(
        host.get(&app, "/write?path=kept&body=durable")
            .await
            .expect("a durable write")
            .status,
        200
    );
    assert_eq!(host.get(&app, "/remember").await.expect("an answer").body, "1");
    assert_eq!(host.get(&app, "/remember").await.expect("an answer").body, "2");
    assert_eq!(
        host.get(&neighbour, "/remember")
            .await
            .expect("the neighbour answers")
            .body,
        "1"
    );
    let processes = VmProcesses::new(host.host.config.runtime_dir.clone());
    let previous = processes
        .read_record(&app.app_id)
        .expect("the live microVM's process record");
    let slot = host.host.slot_of(&app.app_id).await.expect("a slot");
    nix::sys::signal::kill(
        nix::unistd::Pid::from_raw(previous.pid),
        nix::sys::signal::Signal::SIGKILL,
    )
    .expect("the test kills only its own microVM");

    {
        let recovery = host.until("the killed VM to be replaced and healthy", |report| {
            processes
                .read_record(&app.app_id)
                .is_some_and(|record| record.pid != previous.pid)
                && report
                    .instances
                    .iter()
                    .any(|instance| instance.app_id == app.app_id && instance.state == InstanceState::Running)
        });
        tokio::pin!(recovery);
        loop {
            tokio::select! {
                _ = &mut recovery => break,
                answer = host.get(&neighbour, "/") => {
                    let answer = answer.expect("the neighbour stays reachable during recovery");
                    assert_eq!(answer.status, 200, "{answer:?}");
                }
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
    let replacement = processes
        .read_record(&app.app_id)
        .expect("the replacement process");
    assert_ne!(replacement.pid, previous.pid);
    assert_eq!(host.get(&app, "/remember").await.expect("a cold boot").body, "1");
    assert_eq!(
        host.get(&app, "/read?path=kept")
            .await
            .expect("a durable read")
            .body,
        "durable"
    );
    assert_eq!(
        host.get(&neighbour, "/remember")
            .await
            .expect("the neighbour kept its memory")
            .body,
        "2"
    );
    assert_eq!(
        host.host
            .slot_of(&app.app_id)
            .await
            .expect("the same slot")
            .host_port,
        slot.host_port
    );
    host.stop().await;
}

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
            first_boot: protocol::FirstBoot::Eager,
            snapshot_ttl_ms: None,
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

#[tokio::test(flavor = "multi_thread")]
async fn a_lazy_instance_is_routed_without_booting_and_its_first_request_boots_it() {
    let Some(host) = crate::host().await else {
        return;
    };
    let app = host.tenant(1).on_request(60_000).edited(|instance| {
        instance.idle_timeout_ms = None;
        instance.activation = Some(protocol::ActivationPolicy {
            snapshot_ttl_ms: None,
            first_boot: protocol::FirstBoot::Lazy,
            sleep_when: protocol::SleepPolicy::TrafficIdle {
                timeout_ms: protocol::DEFAULT_IDLE_TIMEOUT,
            },
        });
    });
    host.deploy(std::slice::from_ref(&app)).await;
    host.until_state(&app.app_id, InstanceState::Idle).await;
    let processes = nibrunnerd::adapters::vm::process::VmProcesses::new(host.host.config.runtime_dir.clone());
    assert!(processes.read_record(&app.app_id).is_none(), "nothing was booted");

    let answer = host.get(&app, "/").await.expect("the first request is answered");
    assert_eq!(answer.status, 200);
    assert!(
        processes.read_record(&app.app_id).is_some(),
        "the request booted it"
    );
    host.stop().await;
}
