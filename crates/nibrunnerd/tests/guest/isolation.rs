//! Isolation, asked of the kernel rather than of the ruleset's text. Every target here is one
//! something is actually listening on, so a guest that got through says "reached" rather than a
//! refusal that would have happened anyway.

use std::net::{Ipv4Addr, SocketAddr, TcpListener};
use std::time::Duration;

use nibrunnerd::test_support::machine::{RunningHost, Tenant};
use protocol::InstanceState;

/// The tenant gives up after two seconds, so anything under this came back because a rule said
/// no rather than because nothing was there.
const REJECTED_WITHIN_MS: u128 = 1_500;

async fn reaching(host: &RunningHost, app: &Tenant, address: &str) -> String {
    host.get(app, &format!("/reach?addr={address}"))
        .await
        .expect("the proxy answers")
        .body
}

fn said_milliseconds(said: &str) -> u128 {
    said.split_once(" in ")
        .and_then(|(_, rest)| rest.split_once("ms"))
        .and_then(|(number, _)| number.parse().ok())
        .unwrap_or(u128::MAX)
}

#[tokio::test(flavor = "multi_thread")]
async fn an_app_cannot_reach_another_app_on_this_host() {
    let Some(host) = crate::host().await else {
        return;
    };
    let one = host.tenant(1);
    let another = host.tenant(2);
    host.deploy(&[one.clone(), another.clone()]).await;
    host.until_state(&one.app_id, InstanceState::Running).await;
    host.until_state(&another.app_id, InstanceState::Running).await;

    // The other guest's tenant is listening on this exact address and port, so nothing but the
    // ruleset can be what stops this.
    let neighbour = host
        .host
        .slot_of(&another.app_id)
        .await
        .expect("a slot")
        .guest_ipv4;
    let said = reaching(
        &host,
        &one,
        &format!("{}:{}", neighbour.as_str(), protocol::DEFAULT_HTTP_PORT),
    )
    .await;
    assert!(said.starts_with("blocked"), "one app reached another: {said}");

    host.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn an_app_cannot_reach_the_host_it_runs_on() {
    let Some(host) = crate::host().await else {
        return;
    };
    let app = host.tenant(1);
    host.deploy(std::slice::from_ref(&app)).await;
    host.until_state(&app.app_id, InstanceState::Running).await;

    let slot = host.host.slot_of(&app.app_id).await.expect("a slot");
    let side: Ipv4Addr = slot
        .host_ipv4
        .as_str()
        .parse()
        .expect("the host's side of the tap");
    let listening = TcpListener::bind(SocketAddr::from((side, 0))).expect("a listener on the tap");
    let address = listening.local_addr().expect("its address");
    std::thread::spawn(move || for _ in listening.incoming() {});

    let said = reaching(&host, &app, &address.to_string()).await;
    assert!(
        said.starts_with("blocked"),
        "an app reached something listening on its host: {said}"
    );

    host.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn an_app_cannot_reach_the_instance_metadata_endpoint() {
    let Some(host) = crate::host().await else {
        return;
    };
    let app = host.tenant(1);
    host.deploy(std::slice::from_ref(&app)).await;
    host.until_state(&app.app_id, InstanceState::Running).await;

    let said = reaching(&host, &app, "169.254.169.254:80").await;
    assert!(
        said.starts_with("blocked"),
        "the metadata endpoint answered: {said}"
    );
    assert!(
        said_milliseconds(&said) < REJECTED_WITHIN_MS,
        "nothing rejected it — it was left to time out, which is not the same rule: {said}"
    );

    host.stop().await;
}

// The one test here that needs the machine to have egress of its own. Without it there is nothing
// a guest is allowed to reach, and so nothing to prove a deny against.
#[tokio::test(flavor = "multi_thread")]
async fn an_address_the_configuration_denies_cannot_be_reached() {
    const TARGET: &str = "1.1.1.1:53";

    let Some(open) = crate::host().await else {
        return;
    };
    let app = open.tenant(1);
    open.deploy(std::slice::from_ref(&app)).await;
    open.until_state(&app.app_id, InstanceState::Running).await;
    let allowed = reaching(&open, &app, TARGET).await;
    open.stop().await;
    if !allowed.starts_with("reached") {
        eprintln!("this machine has no egress a guest could use, so a deny proves nothing: {allowed}");
        return;
    }

    // A second host, the same in every way but the range it refuses.
    let Some(closed) = nibrunnerd::test_support::machine::started_with(|config| {
        config.denied_egress_addresses_v4 = vec!["1.1.1.1/32".to_string()];
    })
    .await
    else {
        return;
    };
    let app = closed.tenant(1);
    closed.deploy(std::slice::from_ref(&app)).await;
    closed.until_state(&app.app_id, InstanceState::Running).await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    let said = reaching(&closed, &app, TARGET).await;
    assert!(
        said.starts_with("blocked"),
        "a denied address was reached: {said}"
    );

    closed.stop().await;
}
