//! What the proxy answers from memory, the guest never hears: a page its app made shareable is
//! served while the app sleeps, without waking it.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use nibrunnerd::config::{Compression, HttpCache, HttpEncode};
use nibrunnerd::test_support::machine::{RunningHost, Tenant};
use protocol::InstanceState;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const IDLE_TIMEOUT_MS: u64 = 60_000;
const THREE_MEGABYTES: &str = "/page?kib=3072";

struct Answered {
    status: u16,
    headers: BTreeMap<String, String>,
    took: Duration,
}

async fn asked(host: &RunningHost, tenant: &Tenant, path: &str, accept_encoding: Option<&str>) -> Answered {
    let accepting = accept_encoding.map_or(String::new(), |accepted| {
        format!("accept-encoding: {accepted}\r\n")
    });
    let request = format!(
        "GET {path} HTTP/1.1\r\nhost: {}\r\n{accepting}connection: close\r\n\r\n",
        tenant.hostname
    );
    let asking = Instant::now();
    let mut stream = tokio::net::TcpStream::connect(host.proxy()).await.unwrap();
    stream.write_all(request.as_bytes()).await.unwrap();
    let mut head = Vec::new();
    while !head.ends_with(b"\r\n\r\n") {
        let mut byte = [0u8; 1];
        stream.read_exact(&mut byte).await.unwrap();
        head.push(byte[0]);
    }
    let took = asking.elapsed();
    stream.read_to_end(&mut Vec::new()).await.unwrap();
    let head = String::from_utf8_lossy(&head).to_string();
    let mut lines = head.lines();
    let status = lines
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|status| status.parse().ok())
        .unwrap();
    let headers = lines
        .filter_map(|line| line.split_once(':'))
        .map(|(name, value)| (name.trim().to_ascii_lowercase(), value.trim().to_string()))
        .collect();
    Answered {
        status,
        headers,
        took,
    }
}

fn wakes(host: &RunningHost, tenant: &Tenant) -> u64 {
    host.host.metrics.sleep_wake.of(&tenant.app_id).wakes.iter().sum()
}

async fn still_asleep(host: &RunningHost, tenant: &Tenant) -> bool {
    host.instance(&tenant.app_id).await.map(|instance| instance.state) == Some(InstanceState::Idle)
}

#[tokio::test(flavor = "multi_thread")]
async fn a_shareable_page_is_answered_from_memory_while_its_app_sleeps() {
    let Some(host) = crate::host_with(|config| {
        let http = config.proxy.http.as_mut().expect("the suite's host serves HTTP");
        http.cache = Some(HttpCache {
            memory_mib: 128.try_into().unwrap(),
            max_entry_kib: 1024.try_into().unwrap(),
        });
        http.encode = Some(HttpEncode {
            encodings: vec![Compression::Br, Compression::Gzip],
            min_kib: 1,
        });
    })
    .await
    else {
        return;
    };
    let app = host.tenant(1).on_request(IDLE_TIMEOUT_MS);
    host.deploy(std::slice::from_ref(&app)).await;
    host.until_state(&app.app_id, InstanceState::Running).await;

    let first = asked(&host, &app, THREE_MEGABYTES, Some("gzip, deflate, br")).await;
    assert_eq!(first.status, 200);
    assert_eq!(first.headers["content-encoding"], "br");
    assert_eq!(first.headers["etag"], "W/\"page-3072\"");
    // The copy is stored on a task of its own once the body has gone to the visitor whole.
    let deadline = Instant::now() + Duration::from_secs(5);
    while host.host.metrics.proxy.cache_bytes() == 0 {
        assert!(Instant::now() < deadline, "the page was never stored");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let stored = host.host.metrics.proxy.cache_bytes();
    assert!(
        stored > 0 && stored < 1024 * 1024,
        "3 MB of html is stored at its encoded size: {stored}"
    );

    host.let_sleep(&app).await;
    let woken = wakes(&host, &app);
    let hit = asked(&host, &app, THREE_MEGABYTES, Some("br")).await;
    assert_eq!(hit.status, 200);
    assert_eq!(hit.headers["content-encoding"], "br");
    assert!(hit.headers.contains_key("age"));
    println!("a hit while the app slept answered in {:?}", hit.took);
    assert!(hit.took < Duration::from_millis(2), "the hit took {:?}", hit.took);
    assert!(still_asleep(&host, &app).await, "a hit woke the app");
    assert_eq!(wakes(&host, &app), woken);

    let etag = hit.headers["etag"].clone();
    let not_modified = {
        let request = format!(
            "GET {THREE_MEGABYTES} HTTP/1.1\r\nhost: {}\r\naccept-encoding: br\r\nif-none-match: {etag}\r\nconnection: close\r\n\r\n",
            app.hostname
        );
        let mut stream = tokio::net::TcpStream::connect(host.proxy()).await.unwrap();
        stream.write_all(request.as_bytes()).await.unwrap();
        let mut answer = Vec::new();
        stream.read_to_end(&mut answer).await.unwrap();
        String::from_utf8_lossy(&answer).to_string()
    };
    assert!(not_modified.starts_with("HTTP/1.1 304 "), "{not_modified}");

    let gzip = asked(&host, &app, THREE_MEGABYTES, Some("gzip")).await;
    assert_eq!(
        gzip.headers["content-encoding"], "gzip",
        "a gzip client gets its own variant"
    );
    host.let_sleep(&app).await;
    let woken = wakes(&host, &app);
    let identity = asked(&host, &app, THREE_MEGABYTES, None).await;
    assert_eq!(identity.status, 200);
    assert!(!identity.headers.contains_key("content-encoding"));
    assert_eq!(
        wakes(&host, &app),
        woken,
        "identity was decoded from the stored gzip"
    );
    assert!(still_asleep(&host, &app).await);

    let never = "/page?kib=8&cache-control=no-store";
    asked(&host, &app, never, Some("br")).await;
    host.let_sleep(&app).await;
    let woken = wakes(&host, &app);
    asked(&host, &app, never, Some("br")).await;
    assert_eq!(wakes(&host, &app), woken + 1, "a no-store page is never cached");

    let unencoded = asked(
        &host,
        &app,
        "/page?kib=8&cache-control=public,max-age=60,no-transform",
        Some("br"),
    )
    .await;
    assert!(!unencoded.headers.contains_key("content-encoding"));
    assert_eq!(unencoded.headers["etag"], "\"page-8\"");

    host.stop().await;
}
