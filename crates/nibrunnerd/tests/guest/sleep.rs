//! Sleep and wake are the host's business, not the caller's: a request is answered whatever the
//! app was doing when it arrived, and what the app was holding survives.

use std::time::{Duration, Instant};

use protocol::InstanceState;

/// Long enough that a wake which booted from cold instead of restoring would still be caught by
/// what the tenant remembers, and short enough that a hung request fails the test rather than the
/// suite's patience.
const ANSWERED_WITHIN: Duration = Duration::from_secs(15);

/// The shortest a document may name. Nothing here waits one out except the test that is about
/// the timer.
const IDLE_TIMEOUT_MS: u64 = 60_000;

#[tokio::test(flavor = "multi_thread")]
async fn concurrent_requests_restore_one_guest_and_every_caller_is_answered() {
    const CALLERS: u64 = 32;

    let Some(host) =
        crate::host_with(|config| config.max_concurrent_vm_starts = std::num::NonZeroU16::new(1).unwrap())
            .await
    else {
        return;
    };
    let app = host.tenant(1).on_request(IDLE_TIMEOUT_MS);
    host.deploy(std::slice::from_ref(&app)).await;
    host.until_state(&app.app_id, InstanceState::Running).await;
    assert_eq!(host.get(&app, "/remember").await.expect("an answer").body, "1");
    host.let_sleep(&app).await;

    let [restores_before, _, cold_boots_before, _] = host.host.metrics.sleep_wake.of(&app.app_id).wakes;
    let barrier = tokio::sync::Barrier::new(CALLERS as usize);
    let requests = (0..CALLERS).map(|_| async {
        barrier.wait().await;
        host.get(&app, "/remember")
            .await
            .expect("each waiting caller is answered")
    });
    let answers = tokio::time::timeout(ANSWERED_WITHIN, futures::future::join_all(requests))
        .await
        .expect("the concurrent wake answers every caller in time");
    let mut remembered: Vec<u64> = answers
        .into_iter()
        .map(|answer| {
            assert_eq!(answer.status, 200, "{answer:?}");
            answer.body.parse().expect("the guest's counter")
        })
        .collect();
    remembered.sort_unstable();
    assert_eq!(
        remembered,
        (2..=CALLERS + 1).collect::<Vec<_>>(),
        "the restored guest lost or duplicated state"
    );
    let [restores_after, _, cold_boots_after, _] = host.host.metrics.sleep_wake.of(&app.app_id).wakes;
    assert_eq!(
        restores_after - restores_before,
        1,
        "more than one restore served the callers"
    );
    assert_eq!(
        cold_boots_after, cold_boots_before,
        "the wake cold-booted instead of restoring"
    );
    host.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_request_that_finds_an_app_asleep_is_answered_rather_than_refused() {
    let Some(host) = crate::host().await else {
        return;
    };
    let app = host.tenant(1).on_request(IDLE_TIMEOUT_MS);
    host.deploy(std::slice::from_ref(&app)).await;
    host.until_state(&app.app_id, InstanceState::Running).await;

    host.let_sleep(&app).await;

    let asking = Instant::now();
    let answer = host.get(&app, "/").await.expect("the proxy answers");
    assert_eq!(answer.status, 200, "{answer:?}");
    println!("woken and answered in {:?}", asking.elapsed());
    assert!(
        asking.elapsed() < ANSWERED_WITHIN,
        "the wake took {:?}",
        asking.elapsed()
    );

    host.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn an_app_that_slept_comes_back_with_what_it_had_in_memory() {
    let Some(host) = crate::host().await else {
        return;
    };
    let app = host.tenant(1).on_request(IDLE_TIMEOUT_MS);
    host.deploy(std::slice::from_ref(&app)).await;
    host.until_state(&app.app_id, InstanceState::Running).await;

    assert_eq!(host.get(&app, "/remember").await.expect("an answer").body, "1");
    assert_eq!(host.get(&app, "/remember").await.expect("an answer").body, "2");

    host.let_sleep(&app).await;

    let after = host.get(&app, "/remember").await.expect("an answer");
    assert_eq!(
        after.body, "3",
        "the microVM was booted afresh rather than restored: the tenant had forgotten"
    );

    host.stop().await;
}

// What the 2026-09-21 run found as 4.6: the proxy keeps an upstream connection for five seconds
// after it is done with it, and a sleep inside that window left the next request riding a
// connection into a guest that was being paused. It hung for the caller's whole timeout.
#[tokio::test(flavor = "multi_thread")]
async fn a_request_is_answered_across_a_sleep_the_proxy_was_holding_a_connection_through() {
    let Some(host) = crate::host().await else {
        return;
    };
    let app = host.tenant(1).on_request(IDLE_TIMEOUT_MS);
    host.deploy(std::slice::from_ref(&app)).await;
    host.until_state(&app.app_id, InstanceState::Running).await;

    let mut slowest = Duration::ZERO;
    for round in 1..=3 {
        // Leaves a pooled connection into the guest, which the sleep has to close behind it.
        let answer = host.get(&app, "/").await.expect("the proxy answers");
        assert_eq!(answer.status, 200, "round {round}: {answer:?}");

        host.let_sleep(&app).await;

        let asking = Instant::now();
        let answer = host.get(&app, "/").await.expect("the proxy answers");
        let took = asking.elapsed();
        slowest = slowest.max(took);
        assert_eq!(answer.status, 200, "round {round}: {answer:?}");
        assert!(
            took < ANSWERED_WITHIN,
            "round {round}: the request across the sleep took {took:?}"
        );
    }
    println!("slowest request across a sleep: {slowest:?}");

    host.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_corrupt_snapshot_cold_boots_the_app_and_still_answers_the_request() {
    use std::io::{Read, Seek, SeekFrom, Write};

    use nibrunnerd::adapters::vm::snapshot::{
        snapshot_paths, SNAPSHOT_MEMORY_FILENAME, SNAPSHOT_STATE_FILENAME,
    };

    let Some(host) = crate::host().await else {
        return;
    };
    for (number, filename) in [(1, SNAPSHOT_STATE_FILENAME), (2, SNAPSHOT_MEMORY_FILENAME)] {
        let app = host.tenant(number).on_request(IDLE_TIMEOUT_MS);
        host.deploy(std::slice::from_ref(&app)).await;
        host.until_state(&app.app_id, InstanceState::Running).await;
        assert_eq!(host.get(&app, "/remember").await.expect("an answer").body, "1");
        assert_eq!(host.get(&app, "/remember").await.expect("an answer").body, "2");
        host.let_sleep(&app).await;
        let paths = snapshot_paths(&host.host.config.snapshot_dir, &app.app_id);
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(paths.directory.join(filename))
            .unwrap();
        let mut byte = [0];
        file.read_exact(&mut byte).unwrap();
        byte[0] ^= 1;
        file.seek(SeekFrom::Start(0)).unwrap();
        file.write_all(&byte).unwrap();
        drop(file);
        let [restored_before, cold_before, _, _] = host.host.metrics.sleep_wake.of(&app.app_id).wakes;

        let answer = host.get(&app, "/remember").await.expect("the cold boot answers");
        assert_eq!(answer.status, 200, "{answer:?}");
        assert_eq!(answer.body, "1", "the corrupted snapshot was restored");
        let [restored_after, cold_after, _, _] = host.host.metrics.sleep_wake.of(&app.app_id).wakes;
        assert_eq!(restored_after, restored_before);
        assert_eq!(cold_after, cold_before + 1);
        assert!(!paths.directory.exists());
    }
    host.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_restored_tenant_answers_with_current_host_time_and_keeps_its_memory() {
    const SNAPSHOT_HOLD: Duration = Duration::from_secs(3);
    const MAX_CLOCK_ERROR: Duration = Duration::from_secs(1);
    let Some(host) = crate::host().await else {
        return;
    };
    let app = host.tenant(1).on_request(IDLE_TIMEOUT_MS);
    host.deploy(std::slice::from_ref(&app)).await;
    host.until_state(&app.app_id, InstanceState::Running).await;
    assert_eq!(host.get(&app, "/remember").await.unwrap().body, "1");
    host.let_sleep(&app).await;
    tokio::time::sleep(SNAPSHOT_HOLD).await;
    let before = nibrunnerd::clock::now_ms();
    let answer = host.get(&app, "/time").await.unwrap();
    let after = nibrunnerd::clock::now_ms();
    assert_eq!(answer.status, 200);
    let guest_time: i64 = answer.body.parse().unwrap();
    let tolerance = MAX_CLOCK_ERROR.as_millis() as i64;
    assert!(
        guest_time >= before - tolerance && guest_time <= after + tolerance,
        "guest time {guest_time}, host interval {before}..{after}"
    );
    assert_eq!(host.get(&app, "/remember").await.unwrap().body, "2");
    host.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn an_abandoned_tenant_freeze_expires_without_restarting_the_app() {
    use guest_contract::{control, vsock};
    use nibrunnerd::domain::guest_line;
    use tokio::io::{AsyncWriteExt, BufReader};
    use tokio::net::UnixStream;

    let Some(host) = crate::host().await else {
        return;
    };
    let app = host.tenant(1).on_request(IDLE_TIMEOUT_MS);
    host.deploy(std::slice::from_ref(&app)).await;
    host.until_state(&app.app_id, InstanceState::Running).await;
    assert_eq!(host.get(&app, "/remember").await.unwrap().body, "1");
    let socket = host
        .host
        .vms
        .working_dir(&app.app_id)
        .join(vsock::GUEST_VSOCK_FILENAME);
    let mut wire = BufReader::new(UnixStream::connect(socket).await.unwrap());
    let timeout = Duration::from_millis(control::TENANT_CONTROL_TIMEOUT_MS);
    wire.get_mut()
        .write_all(vsock::connect_request(vsock::GUEST_CONTROL_VSOCK_PORT).as_bytes())
        .await
        .unwrap();
    let connected = guest_line::read(&mut wire, timeout).await.unwrap();
    vsock::read_connect_reply(&connected, vsock::GUEST_CONTROL_VSOCK_PORT).unwrap();
    wire.get_mut()
        .write_all(format!("{}\n", control::TENANT_FREEZE_REQUEST).as_bytes())
        .await
        .unwrap();
    assert_eq!(
        guest_line::read(&mut wire, timeout).await.unwrap(),
        control::TENANT_FREEZE_READY
    );
    wire.get_mut()
        .write_all(format!("{}\n", control::TENANT_FREEZE_COMMIT).as_bytes())
        .await
        .unwrap();
    assert_eq!(
        guest_line::read(&mut wire, timeout).await.unwrap(),
        control::TENANT_FREEZE_HELD
    );
    drop(wire);
    tokio::time::sleep(Duration::from_millis(control::TENANT_FREEZE_LEASE_MS) + timeout).await;
    assert_eq!(host.get(&app, "/remember").await.unwrap().body, "2");
    host.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_frozen_guest_keeps_its_memory_in_place_and_answers_once_thawed() {
    let Some(host) = crate::host().await else {
        return;
    };
    let app = host.tenant(1);
    host.deploy(std::slice::from_ref(&app)).await;
    host.until_state(&app.app_id, InstanceState::Running).await;
    assert_eq!(host.get(&app, "/allocate?mib=32").await.unwrap().status, 200);
    let pid = |host: &nibrunnerd::test_support::machine::RunningHost| {
        nibrunnerd::adapters::vm::process::VmProcesses::new(host.host.config.runtime_dir.clone())
            .read_record(&app.app_id)
            .unwrap()
            .pid
    };
    let before = pid(&host);

    host.host.vms.freeze(&app.app_id).await.unwrap();
    let statuses = host.host.vms.statuses(std::slice::from_ref(&app.app_id)).await;
    assert!(statuses[&app.app_id].frozen);
    assert!(
        tokio::time::timeout(Duration::from_secs(2), host.get(&app, "/"))
            .await
            .is_err(),
        "a paused guest answers nothing"
    );

    host.host.vms.thaw(&app.app_id).await.unwrap();
    assert!(!host.host.vms.statuses(std::slice::from_ref(&app.app_id)).await[&app.app_id].frozen);
    assert_eq!(pid(&host), before, "thawed in place, not restored");
    assert_eq!(
        host.get(&app, "/allocated").await.unwrap().body,
        format!("{}:1", 32 * 1024 * 1024)
    );
    host.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_unit_frozen_past_the_snapshot_lease_with_production_limits_thaws_in_place_through_the_activator() {
    use guest_contract::control::{TENANT_CONTROL_TIMEOUT_MS, TENANT_FREEZE_LEASE_MS};
    use nibrunnerd::config::{MemoryAdmission, MemoryAdmissionMode};

    let Some(host) = crate::host_with(|config| {
        config.memory_admission = Some(MemoryAdmission {
            mode: MemoryAdmissionMode::Observe,
            pool: None,
            // Under 30 000 is taken as 30 000: the shortest a host freezes after.
            freeze_after_ms: std::num::NonZeroU64::new(1),
            reclaim: false,
            headroom_mib: 256.try_into().unwrap(),
        });
    })
    .await
    else {
        return;
    };
    // What mf writes for a durable unit: swap as large as its memory, a production priority and a
    // fraction of a core.
    let app = host
        .tenant(1)
        .on_request(10 * IDLE_TIMEOUT_MS)
        .edited(|instance| {
            instance.limits = Some(protocol::InstanceLimits {
                concurrent: None,
                cpu_percent: Some(25.try_into().unwrap()),
                memory: Some(protocol::MemoryPolicy {
                    target_mib: instance.config.resources.memory_mib.try_into().unwrap(),
                    swap_mib: instance.config.resources.memory_mib,
                    priority: protocol::MemoryPriority::Production,
                }),
            });
        });
    host.deploy(std::slice::from_ref(&app)).await;
    host.until_state(&app.app_id, InstanceState::Running).await;
    assert_eq!(host.get(&app, "/remember").await.unwrap().body, "1");
    let pid = |host: &crate::RunningHost| {
        nibrunnerd::adapters::vm::process::VmProcesses::new(host.host.config.runtime_dir.clone())
            .read_record(&app.app_id)
            .unwrap()
            .pid
    };
    let before = pid(&host);

    host.until_state(&app.app_id, InstanceState::Frozen).await;
    // Past the lease a snapshot sleep's guest would release its tenant under: a paused guest's
    // clock keeps running, and nothing sets it on the thaw.
    tokio::time::sleep(Duration::from_millis(
        TENANT_FREEZE_LEASE_MS + TENANT_CONTROL_TIMEOUT_MS,
    ))
    .await;
    let [_, cold_boots, _, thawed] = host.host.metrics.sleep_wake.of(&app.app_id).wakes;

    let asked = Instant::now();
    let answer = host.get(&app, "/remember").await.unwrap();
    let took = asked.elapsed();

    assert_eq!(answer.body, "2", "the tenant kept its memory");
    // A12 asked for 300 ms. Under a quarter of a core the guest is throttled while it catches up
    // on a pause of over half a minute: 331 ms on a 2-vCPU VPS, under 300 ms without the cap.
    assert!(took < Duration::from_millis(500), "the thaw took {took:?}");
    assert_eq!(pid(&host), before, "thawed in place, not booted again");
    let [_, cold_boots_after, _, thawed_after] = host.host.metrics.sleep_wake.of(&app.app_id).wakes;
    assert_eq!(thawed_after, thawed + 1, "the wake was a thaw");
    assert_eq!(cold_boots_after, cold_boots, "nothing fell back to a cold boot");
    let (sent, guest_time, answered) = (
        nibrunnerd::clock::now_ms(),
        host.get(&app, "/time")
            .await
            .unwrap()
            .body
            .parse::<i64>()
            .unwrap(),
        nibrunnerd::clock::now_ms(),
    );
    assert!(
        guest_time >= sent - 1_000 && guest_time <= answered + 1_000,
        "the thawed guest reads {guest_time} against the host's {sent}..{answered}, with no clock step"
    );
    host.stop().await;
}
