//! When an app is allowed to go: the one invariant here waits out a real idle timeout, which a
//! document may not set under a minute, so it is kept apart from the rest and given a CI shard of
//! its own rather than made everything else queue behind it.

use std::time::{Duration, Instant};

use protocol::InstanceState;

/// The shortest a document may name.
const IDLE_TIMEOUT_MS: u64 = 60_000;

#[tokio::test(flavor = "multi_thread")]
async fn a_lifetime_expiry_drains_an_open_request_only_until_its_allowance_is_spent() {
    use nibrunnerd::domain::activation::MAX_LIFETIME_DRAIN_MS;
    use protocol::{ActivationPolicy, MaxLifetimeMs, SleepPolicy, Timestamp};
    let Some(host) = crate::host().await else {
        return;
    };
    let app = host.tenant(1).edited(|instance| {
        instance.desired_state = protocol::DesiredInstanceState::OnRequest;
        instance.activation = Some(ActivationPolicy {
            first_boot: protocol::FirstBoot::Eager,
            snapshot_ttl_ms: None,
            cutover_timeout_ms: None,
            sleep_when: SleepPolicy::MaxLifetime {
                ttl_ms: MaxLifetimeMs::try_from(IDLE_TIMEOUT_MS).expect("a lifetime"),
            },
        });
    });
    host.deploy(std::slice::from_ref(&app)).await;
    host.until_state(&app.app_id, InstanceState::Running).await;
    {
        let checking = async {
            while host.host.metrics.proxy.open_requests_for(&app.app_id) == 0 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            let recently_expired =
                Timestamp::from_epoch_ms(nibrunnerd::clock::now_ms() - IDLE_TIMEOUT_MS as i64 - 1_000);
            host.host
                .state
                .update_record(&app.app_id, |record| record.started_at = Some(recently_expired))
                .await;
            nibrunnerd::domain::reconcile::idle::apply_sleep(&host.host).await;
            assert_eq!(
                host.instance(&app.app_id).await.expect("a draining app").state,
                InstanceState::Running
            );
            assert!(host.host.metrics.proxy.open_requests_for(&app.app_id) > 0);
            let allowance_spent = Timestamp::from_epoch_ms(
                nibrunnerd::clock::now_ms() - IDLE_TIMEOUT_MS as i64 - MAX_LIFETIME_DRAIN_MS - 1_000,
            );
            host.host
                .state
                .update_record(&app.app_id, |record| record.started_at = Some(allowance_spent))
                .await;
            nibrunnerd::domain::reconcile::idle::apply_sleep(&host.host).await;
            assert_eq!(
                host.instance(&app.app_id).await.expect("a sleeping app").state,
                InstanceState::Idle
            );
        };
        tokio::select! {
            answer = host.get(&app, "/hang?ms=120000") => panic!("the request ended before its drain was tested: {answer:?}"),
            checked = tokio::time::timeout(Duration::from_secs(15), checking) => checked.expect("the lifetime drain is bounded"),
        }
    }
    let answer = host.get(&app, "/").await.expect("the next request wakes the app");
    assert_eq!(answer.status, 200, "{answer:?}");
    host.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_quiet_open_request_prevents_idle_sleep_until_its_answer_is_finished() {
    let Some(host) = crate::host().await else {
        return;
    };
    let app = host.tenant(1).on_request(IDLE_TIMEOUT_MS);
    host.deploy(std::slice::from_ref(&app)).await;
    host.until_state(&app.app_id, InstanceState::Running).await;

    let watching = async {
        tokio::time::timeout(Duration::from_secs(5), async {
            while host.host.metrics.proxy.open_requests_for(&app.app_id) == 0 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the proxy starts carrying the request");
        for _ in 0..5 {
            assert!(host.host.metrics.proxy.open_requests_for(&app.app_id) > 0);
            nibrunnerd::test_support::measured_quiet_since(
                &host.host.state,
                &app.app_id,
                nibrunnerd::clock::now_ms() - IDLE_TIMEOUT_MS as i64 - 1,
            )
            .await;
            nibrunnerd::domain::reconcile::idle::apply_sleep(&host.host).await;
            assert_eq!(
                host.instance(&app.app_id).await.expect("a record").state,
                InstanceState::Running
            );
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    };
    let (answer, ()) = tokio::join!(host.get(&app, "/hang?ms=8000"), watching);
    let answer = answer.expect("the quiet request completes");
    assert_eq!(answer.status, 200, "{answer:?}");
    assert_eq!(answer.body, "waited");
    assert_eq!(host.host.metrics.proxy.open_requests_for(&app.app_id), 0);
    host.let_sleep(&app).await;
    host.stop().await;
}

// The 2026-09-18 and 2026-09-21 runs both found loaded apps snapshotted under traffic, because
// the activity reading was minutes behind. Two apps, one timeout, one wait: the busy one must
// still be up and the quiet one must be asleep.
#[tokio::test(flavor = "multi_thread")]
async fn an_app_sleeps_because_it_is_quiet_and_not_because_time_passed() {
    let Some(host) = crate::host().await else {
        return;
    };
    let busy = host.tenant(1).on_request(IDLE_TIMEOUT_MS);
    let quiet = host.tenant(2).on_request(IDLE_TIMEOUT_MS);
    host.deploy(&[busy.clone(), quiet.clone()]).await;
    host.until_state(&busy.app_id, InstanceState::Running).await;
    host.until_state(&quiet.app_id, InstanceState::Running).await;

    let watching = Instant::now();
    let mut busy_was_let_go = Vec::new();
    while watching.elapsed() < Duration::from_millis(IDLE_TIMEOUT_MS + 20_000) {
        let answer = host.get(&busy, "/").await.expect("the proxy answers");
        assert_eq!(answer.status, 200, "{answer:?}");
        if let Some(state) = host.instance(&busy.app_id).await.map(|held| held.state) {
            if state != InstanceState::Running {
                busy_was_let_go.push(format!("{:?} at {:?}", state, watching.elapsed()));
            }
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }

    assert!(
        busy_was_let_go.is_empty(),
        "an app under traffic was let go: {}",
        busy_was_let_go.join(", ")
    );
    assert_eq!(
        host.instance(&quiet.app_id).await.map(|held| held.state),
        Some(InstanceState::Idle),
        "an app nobody asked for in {IDLE_TIMEOUT_MS}ms is still up"
    );

    host.stop().await;
}
