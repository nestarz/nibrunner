use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;

use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioIo;
use protocol::{AppId, HostPort};
use tokio::net::TcpListener;
use tokio::sync::Mutex;

use crate::adapters::proxy::forward::{forward, say, ProxyBody};
use crate::domain::metrics::sleep_wake::Answer;
use crate::domain::metrics::HostMetrics;
use crate::ports::{WakeFailure, WakeRefusal, Waker};
use crate::state::SharedState;

fn app_is_down() -> Response<ProxyBody> {
    say(StatusCode::SERVICE_UNAVAILABLE, "This app is not running.\n")
}

fn app_would_not_start() -> Response<ProxyBody> {
    say(
        StatusCode::SERVICE_UNAVAILABLE,
        "This app could not be started.\n",
    )
}

fn host_is_full() -> Response<ProxyBody> {
    say(
        StatusCode::SERVICE_UNAVAILABLE,
        "This app could not be started: its machine is out of memory.\n",
    )
}

fn come_back() -> Response<ProxyBody> {
    let mut response = say(
        StatusCode::SERVICE_UNAVAILABLE,
        "This app is starting. Please reconnect.\n",
    );
    response
        .headers_mut()
        .insert("retry-after", hyper::header::HeaderValue::from_static("2"));
    response
}

struct Listener {
    host_port: HostPort,
    task: tokio::task::JoinHandle<()>,
}

pub struct AppActivator {
    state: SharedState,
    waker: Arc<dyn Waker>,
    metrics: Arc<HostMetrics>,
    client: Client<HttpConnector, Incoming>,
    listeners: Mutex<BTreeMap<AppId, Listener>>,
}

impl AppActivator {
    pub fn new(state: SharedState, waker: Arc<dyn Waker>, metrics: Arc<HostMetrics>) -> Arc<Self> {
        Arc::new(Self {
            state,
            waker,
            metrics,
            client: crate::adapters::proxy::forward::upstream_client(),
            listeners: Mutex::new(BTreeMap::new()),
        })
    }

    pub async fn serve(self: &Arc<Self>, slots: &[(AppId, HostPort)]) {
        let wanted: BTreeMap<AppId, HostPort> = slots.iter().cloned().collect();
        let mut listeners = self.listeners.lock().await;
        listeners.retain(|app_id, listener| {
            let keep = wanted.get(app_id) == Some(&listener.host_port);
            if !keep {
                listener.task.abort();
            }
            keep
        });
        for (app_id, host_port) in wanted {
            if listeners.contains_key(&app_id) {
                continue;
            }
            let address = SocketAddr::from(([127, 0, 0, 1], host_port.get()));
            match TcpListener::bind(address).await {
                Ok(listener) => {
                    tracing::info!(%app_id, %host_port, "app activator listening");
                    let task = tokio::spawn(accept(listener, self.clone(), app_id.clone()));
                    listeners.insert(app_id, Listener { host_port, task });
                }
                Err(error) => {
                    tracing::warn!(%app_id, %host_port, %error, "app activator bind failed");
                }
            }
        }
    }

    pub async fn listening_for(&self) -> Vec<AppId> {
        self.listeners.lock().await.keys().cloned().collect()
    }

    async fn handle(self: Arc<Self>, app_id: AppId, request: Request<Incoming>) -> Response<ProxyBody> {
        let (response, answer) = self.answer(app_id, request).await;
        self.metrics.sleep_wake.answered(answer);
        response
    }

    async fn answer(&self, app_id: AppId, request: Request<Incoming>) -> (Response<ProxyBody>, Answer) {
        let Some(record) = self.state.record(&app_id).await else {
            return (app_is_down(), Answer::Down);
        };
        if !record.desired_running {
            return (app_is_down(), Answer::Down);
        }
        let Some(open) = self
            .state
            .admit(&app_id, crate::clock::now_ms(), || {
                self.metrics.proxy.open(&app_id)
            })
            .await
        else {
            return (
                say(StatusCode::GONE, "This revision has expired.\n"),
                Answer::Down,
            );
        };

        let started = std::time::Instant::now();
        // An idle guest is asleep and has to be woken. Any other guest that reaches the activator
        // is one the firewall stopped forwarding because it is no longer Running, yet it may well
        // still be serving its port — an app the health tracker moved to Unhealthy is the case that
        // brought this here. Health is report-only, so if such a guest already accepts a connection
        // the request is handed straight to it rather than waking it onto a health check it is
        // currently failing (which would wait out the whole grace period and then refuse). A guest
        // that is genuinely down still takes the wake path, which for an app the document wants
        // running is a wait for the guest the pass is bringing up in its place. A guest being
        // written out is the one exception to answering a port that accepts: it accepts right up
        // to the pause, and a request handed to it then is paused with it, so it goes to the
        // waker, which waits for the snapshot and restores from it.
        let guest = SocketAddr::from((record.guest_ipv4.addr(), record.http_port.get()));
        if record.needs_wake()
            || self.state.is_snapshotting(&app_id).await
            || !accepts_a_connection(guest).await
        {
            if let Err(refusal) = self.waker.wake(&app_id).await {
                match refusal {
                    WakeRefusal::NoRoom { shortfall_mib } => {
                        tracing::warn!(%app_id, shortfall_mib, "a request could not be given an app");
                        return (host_is_full(), Answer::HostFull);
                    }
                    // The guest booted but its health probe stayed red through the grace window;
                    // health is report-only, so forward to it anyway rather than manufacture an
                    // outage. A port still not listening falls through to forward()'s bad gateway.
                    WakeRefusal::Failed {
                        kind: WakeFailure::NeverAnswered,
                        reason,
                    } => {
                        tracing::debug!(%app_id, reason, "forwarding to an app whose health check is unmet");
                    }
                    WakeRefusal::Failed { reason, .. } => {
                        tracing::warn!(%app_id, reason, "a request could not be given an app");
                        return (app_would_not_start(), Answer::WouldNotStart);
                    }
                }
            }
        }
        let woke_ms = started.elapsed().as_millis();

        let Some(woken) = self.state.record(&app_id).await else {
            return (app_would_not_start(), Answer::WouldNotStart);
        };
        if request.headers().get(hyper::header::UPGRADE).is_some() {
            return (come_back(), Answer::ComeBack);
        }
        let served = std::time::Instant::now();
        let response = forward(
            &self.client,
            request,
            woken.guest_ipv4.as_str(),
            woken.http_port.get(),
            false,
            open,
        )
        .await;
        self.metrics.sleep_wake.first_response(served.elapsed());
        tracing::debug!(
            %app_id,
            woke_ms,
            served_ms = served.elapsed().as_millis(),
            "app answered the request that woke it"
        );
        (response, Answer::Served)
    }
}

/// Whether the guest is already listening. The kernel completes this connection even while the
/// app behind the port is unhealthy, and the wait is bounded so an unreachable guest does not hold
/// the request up before it falls through to the wake path.
async fn accepts_a_connection(address: SocketAddr) -> bool {
    matches!(
        tokio::time::timeout(
            std::time::Duration::from_millis(200),
            tokio::net::TcpStream::connect(address),
        )
        .await,
        Ok(Ok(_))
    )
}

async fn accept(listener: TcpListener, activator: Arc<AppActivator>, app_id: AppId) {
    loop {
        let Ok((stream, _)) = listener.accept().await else {
            return;
        };
        let activator = activator.clone();
        let app_id = app_id.clone();
        tokio::spawn(async move {
            let service = service_fn(move |request| {
                let activator = activator.clone();
                let app_id = app_id.clone();
                async move { Ok::<_, std::convert::Infallible>(activator.handle(app_id, request).await) }
            });
            let _ = http1::Builder::new()
                .serve_connection(TokioIo::new(stream), service)
                .await;
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::HostState;
    use crate::test_support::{app_id, instance_record};
    use protocol::{HttpPort, InstanceState, Ipv4Address};
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct CountingWaker {
        woken: AtomicUsize,
        refusal: Option<WakeRefusal>,
    }

    impl CountingWaker {
        fn allowing() -> Arc<Self> {
            Arc::new(Self {
                woken: AtomicUsize::new(0),
                refusal: None,
            })
        }

        fn refusing(refusal: WakeRefusal) -> Arc<Self> {
            Arc::new(Self {
                woken: AtomicUsize::new(0),
                refusal: Some(refusal),
            })
        }

        fn count(&self) -> usize {
            self.woken.load(Ordering::SeqCst)
        }
    }

    #[async_trait::async_trait]
    impl Waker for CountingWaker {
        async fn wake(&self, _app_id: &AppId) -> Result<(), WakeRefusal> {
            self.woken.fetch_add(1, Ordering::SeqCst);
            match &self.refusal {
                None => Ok(()),
                Some(refusal) => Err(refusal.clone()),
            }
        }
    }

    async fn serving(activator: &Arc<AppActivator>, app_id: &AppId) -> HostPort {
        for _ in 0..50 {
            let port = HostPort::new(free_port().await).unwrap();
            activator.serve(&[(app_id.clone(), port)]).await;
            if activator.listening_for().await.contains(app_id) {
                return port;
            }
        }
        panic!("the activator could not be given a port to listen on");
    }

    async fn get(port: HostPort) -> reqwest::Response {
        crate::install_crypto_provider();
        reqwest::Client::builder()
            .build()
            .unwrap()
            .get(format!("http://127.0.0.1:{port}/"))
            .send()
            .await
            .expect("the activator answers")
    }

    /// Answers every request on `listener` with `body`, as a tenant would.
    fn serve(listener: TcpListener, body: &'static str) {
        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    return;
                };
                tokio::spawn(async move {
                    let service = service_fn(move |_request: Request<Incoming>| async move {
                        Ok::<_, std::convert::Infallible>(say(StatusCode::OK, body))
                    });
                    let _ = http1::Builder::new()
                        .serve_connection(TokioIo::new(stream), service)
                        .await;
                });
            }
        });
    }

    /// A port nothing is listening on yet.
    async fn free_port() -> u16 {
        let probe = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
            .await
            .unwrap();
        probe.local_addr().unwrap().port()
    }

    async fn guest(state: &SharedState, in_state: InstanceState, body: &'static str) -> HostPort {
        let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
            .await
            .unwrap();
        let port = listener.local_addr().unwrap().port();
        serve(listener, body);
        state
            .put_record(instance_record(|record| {
                record.on_request = true;
                record.state = in_state;
                record.guest_ipv4 = Ipv4Address::parse("127.0.0.1").unwrap();
                record.http_port = HttpPort::new(port).unwrap();
            }))
            .await;
        HostPort::new(port).unwrap()
    }

    /// Wakes by bringing the guest up on the port the record names, then reports the wake as
    /// `outcome`.
    struct WakingBringsUpAGuest {
        port: HostPort,
        body: &'static str,
        outcome: Result<(), WakeRefusal>,
    }

    #[async_trait::async_trait]
    impl Waker for WakingBringsUpAGuest {
        async fn wake(&self, _app_id: &AppId) -> Result<(), WakeRefusal> {
            let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], self.port.get())))
                .await
                .unwrap();
            serve(listener, self.body);
            self.outcome.clone()
        }
    }

    #[tokio::test]
    async fn a_request_to_a_running_app_between_guests_waits_for_the_next_one_and_is_served_by_it() {
        // The pass stopped the old guest for its replacement and the record is Starting, but
        // nothing answers on the port yet. The request is not turned away for the app not being
        // on-request: it is handed to the waker, which holds it until the guest answers.
        let state = HostState::shared();
        let port = free_port().await;
        state
            .put_record(instance_record(|record| {
                record.on_request = false;
                record.state = InstanceState::Starting;
                record.guest_ipv4 = Ipv4Address::parse("127.0.0.1").unwrap();
                record.http_port = HttpPort::new(port).unwrap();
            }))
            .await;
        let waker = WakingBringsUpAGuest {
            port: HostPort::new(port).unwrap(),
            body: "served by the replacement\n",
            outcome: Ok(()),
        };
        let activator = AppActivator::new(state, Arc::new(waker), Arc::default());
        let host_port = serving(&activator, &app_id()).await;

        let response = get(host_port).await;
        assert_eq!(response.status(), 200);
        assert_eq!(response.text().await.unwrap(), "served by the replacement\n");
    }

    #[tokio::test]
    async fn a_running_app_that_is_out_of_restarts_is_said_to_have_not_started() {
        let state = HostState::shared();
        state
            .put_record(instance_record(|record| {
                record.on_request = false;
                record.state = InstanceState::Failed;
            }))
            .await;
        let activator = AppActivator::new(
            state,
            CountingWaker::refusing(WakeRefusal::Failed {
                kind: WakeFailure::WouldNotStart,
                reason: "out of restarts: 6 starts attempted against a budget of 5".into(),
            }),
            Arc::default(),
        );
        let host_port = serving(&activator, &app_id()).await;
        let response = get(host_port).await;
        assert_eq!(response.status(), 503);
        assert!(response.text().await.unwrap().contains("could not be started"));
    }

    #[tokio::test]
    async fn the_request_waits_for_the_wake_and_is_answered_by_the_guest_that_comes_up() {
        let state = HostState::shared();
        guest(&state, InstanceState::Idle, "served by the tenant\n").await;
        let waker = CountingWaker::allowing();
        let activator = AppActivator::new(state, waker.clone(), Arc::default());
        let host_port = serving(&activator, &app_id()).await;

        let response = get(host_port).await;
        assert_eq!(response.status(), 200);
        assert_eq!(
            response
                .headers()
                .get("connection")
                .map(|value| value.to_str().unwrap()),
            Some("close")
        );
        assert_eq!(response.text().await.unwrap(), "served by the tenant\n");
        assert_eq!(waker.count(), 1);
    }

    #[tokio::test]
    async fn a_wake_refused_for_want_of_memory_says_so_rather_than_blaming_the_app() {
        let state = HostState::shared();
        state
            .put_record(instance_record(|record| {
                record.on_request = true;
                record.state = InstanceState::Idle;
            }))
            .await;
        let activator = AppActivator::new(
            state,
            CountingWaker::refusing(WakeRefusal::NoRoom { shortfall_mib: 256 }),
            Arc::default(),
        );
        let host_port = serving(&activator, &app_id()).await;
        assert!(get(host_port)
            .await
            .text()
            .await
            .unwrap()
            .contains("out of memory"));
    }

    #[tokio::test]
    async fn a_microvm_that_would_not_start_is_said_to_have_not_started() {
        let state = HostState::shared();
        state
            .put_record(instance_record(|record| {
                record.on_request = true;
                record.state = InstanceState::Idle;
            }))
            .await;
        let activator = AppActivator::new(
            state,
            CountingWaker::refusing(WakeRefusal::Failed {
                kind: crate::ports::WakeFailure::WouldNotStart,
                reason: "no slots left".into(),
            }),
            Arc::default(),
        );
        let host_port = serving(&activator, &app_id()).await;
        assert!(get(host_port)
            .await
            .text()
            .await
            .unwrap()
            .contains("could not be started"));
    }

    #[tokio::test]
    async fn an_app_that_is_up_but_unhealthy_is_served_without_being_woken() {
        // A guest the health tracker has marked unhealthy is still listening, but the firewall
        // stops forwarding it, so its traffic reaches the activator. Health is report-only, so the
        // request must be handed to the port that answers rather than wait out a wake that would
        // hang on a health check it never passes.
        let state = HostState::shared();
        guest(&state, InstanceState::Unhealthy, "served though unhealthy\n").await;
        let waker = CountingWaker::allowing();
        let activator = AppActivator::new(state, waker.clone(), Arc::default());
        let host_port = serving(&activator, &app_id()).await;

        let response = get(host_port).await;
        assert_eq!(response.status(), 200);
        assert_eq!(response.text().await.unwrap(), "served though unhealthy\n");
        assert_eq!(waker.count(), 0, "an app already answering its port is not woken");
    }

    /// Holds every wake until the test lets it through, as the waker does while a snapshot of
    /// the app is being written.
    struct HoldingWaker {
        woken: AtomicUsize,
        gate: tokio::sync::Semaphore,
    }

    impl HoldingWaker {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                woken: AtomicUsize::new(0),
                gate: tokio::sync::Semaphore::new(0),
            })
        }

        fn let_through(&self) {
            self.gate.add_permits(1);
        }
    }

    #[async_trait::async_trait]
    impl Waker for HoldingWaker {
        async fn wake(&self, _app_id: &AppId) -> Result<(), WakeRefusal> {
            self.woken.fetch_add(1, Ordering::SeqCst);
            self.gate.acquire().await.unwrap().forget();
            Ok(())
        }
    }

    #[tokio::test]
    async fn a_request_to_a_guest_being_written_out_waits_for_the_wake_rather_than_the_port_it_finds_open() {
        // The sleep hands the port to the activator before it pauses the guest, so for a moment
        // the guest still accepts a connection. A request handed to it now would be paused with
        // it; handed to the waker, it waits for the snapshot and the restore.
        let state = HostState::shared();
        guest(&state, InstanceState::Running, "answered once restored\n").await;
        state.mark_snapshotting(&app_id(), true).await;
        let waker = HoldingWaker::new();
        let activator = AppActivator::new(state.clone(), waker.clone(), Arc::default());
        let host_port = serving(&activator, &app_id()).await;

        let mut response = std::pin::pin!(get(host_port));
        let answered = tokio::time::timeout(std::time::Duration::from_millis(100), &mut response).await;
        assert!(
            answered.is_err(),
            "the request was handed to the guest that is about to be paused"
        );
        assert_eq!(waker.woken.load(Ordering::SeqCst), 1);

        state.mark_snapshotting(&app_id(), false).await;
        waker.let_through();
        let response = response.await;
        assert_eq!(response.status(), 200);
        assert_eq!(response.text().await.unwrap(), "answered once restored\n");
    }

    #[tokio::test]
    async fn a_woken_guest_that_comes_up_unhealthy_is_forwarded_to_rather_than_refused() {
        // Cold path: nothing is listening when the request arrives, so the guest is woken. It comes
        // up serving its port but never passes its health check (NeverAnswered). Health is
        // report-only, so the request is still forwarded to the port that is now up.
        let state = HostState::shared();
        let port = free_port().await;
        state
            .put_record(instance_record(|record| {
                record.on_request = true;
                record.state = InstanceState::Idle;
                record.guest_ipv4 = Ipv4Address::parse("127.0.0.1").unwrap();
                record.http_port = HttpPort::new(port).unwrap();
            }))
            .await;
        let waker = WakingBringsUpAGuest {
            port: HostPort::new(port).unwrap(),
            body: "up but unhealthy\n",
            outcome: Err(WakeRefusal::Failed {
                kind: WakeFailure::NeverAnswered,
                reason: "the health check is unmet".into(),
            }),
        };
        let activator = AppActivator::new(state, Arc::new(waker), Arc::default());
        let host_port = serving(&activator, &app_id()).await;

        let response = get(host_port).await;
        assert_eq!(response.status(), 200);
        assert_eq!(response.text().await.unwrap(), "up but unhealthy\n");
    }

    #[tokio::test]
    async fn a_suspended_app_is_not_woken_by_somebody_finding_its_hostname() {
        let state = HostState::shared();
        state
            .put_record(instance_record(|record| {
                record.on_request = true;
                record.state = InstanceState::Stopped;
                record.desired_running = false;
            }))
            .await;
        let waker = CountingWaker::allowing();
        let activator = AppActivator::new(state, waker.clone(), Arc::default());
        let host_port = serving(&activator, &app_id()).await;
        assert_eq!(get(host_port).await.status(), 503);
        assert_eq!(waker.count(), 0);
    }

    #[tokio::test]
    async fn a_slot_the_host_no_longer_holds_stops_being_answered_for() {
        let state = HostState::shared();
        state
            .put_record(instance_record(|record| record.desired_running = false))
            .await;
        let activator = AppActivator::new(state, CountingWaker::allowing(), Arc::default());
        let host_port = serving(&activator, &app_id()).await;
        activator.serve(&[(app_id(), host_port)]).await;
        assert_eq!(activator.listening_for().await, vec![app_id()]);
        assert_eq!(get(host_port).await.status(), 503);

        activator.serve(&[]).await;
        assert!(activator.listening_for().await.is_empty());
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(tokio::net::TcpStream::connect(("127.0.0.1", host_port.get()))
            .await
            .is_err());
    }

    async fn raw(port: HostPort, request: &str) -> String {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let mut stream = tokio::net::TcpStream::connect(("127.0.0.1", port.get()))
            .await
            .unwrap();
        stream.write_all(request.as_bytes()).await.unwrap();
        let mut answer = String::new();
        tokio::time::timeout(
            std::time::Duration::from_secs(10),
            stream.read_to_string(&mut answer),
        )
        .await
        .expect("the activator closes the connection it answered on")
        .unwrap();
        answer
    }

    #[tokio::test]
    async fn a_connection_that_would_be_upgraded_is_told_to_come_back_once_the_app_is_up() {
        let state = HostState::shared();
        guest(&state, InstanceState::Idle, "served by the tenant\n").await;
        let waker = CountingWaker::allowing();
        let activator = AppActivator::new(state, waker.clone(), Arc::default());
        let host_port = serving(&activator, &app_id()).await;

        let answered = raw(
            host_port,
            "GET /ws HTTP/1.0\r\nHost: app-1.apps.example.com\r\nupgrade: websocket\r\n\r\n",
        )
        .await;
        assert!(answered.starts_with("HTTP/1.0 503 "), "{answered}");
        assert!(answered.contains("retry-after: 2"), "{answered}");
        assert!(
            answered.ends_with("This app is starting. Please reconnect.\n"),
            "{answered}"
        );
        assert_eq!(waker.count(), 1, "the app is still woken for the reconnect");
    }

    #[tokio::test]
    async fn a_request_that_woke_an_app_counts_as_the_app_having_been_used() {
        let state = HostState::shared();
        guest(&state, InstanceState::Idle, "served by the tenant\n").await;
        let before = crate::clock::now_ms();
        let activator = AppActivator::new(state.clone(), CountingWaker::allowing(), Arc::default());
        let host_port = serving(&activator, &app_id()).await;
        assert_eq!(get(host_port).await.status(), 200);
        let stamped = state.snapshot().await.last_active_at_ms.get(&app_id()).copied();
        assert!(stamped.is_some_and(|at| at >= before), "{stamped:?}");
    }

    #[tokio::test]
    async fn an_app_the_document_has_stopped_is_not_marked_as_used_by_a_refusal() {
        let state = HostState::shared();
        state
            .put_record(instance_record(|record| record.desired_running = false))
            .await;
        let activator = AppActivator::new(state.clone(), CountingWaker::allowing(), Arc::default());
        let host_port = serving(&activator, &app_id()).await;
        let response = get(host_port).await;
        assert_eq!(response.status(), 503);
        assert!(response.text().await.unwrap().contains("not running"));
        assert!(state.snapshot().await.last_active_at_ms.is_empty());
    }

    #[tokio::test]
    async fn a_slot_that_moved_to_another_port_stops_being_answered_for_on_the_old_one() {
        let state = HostState::shared();
        state.put_record(instance_record(|_| {})).await;
        let activator = AppActivator::new(state, CountingWaker::allowing(), Arc::default());
        let first = serving(&activator, &app_id()).await;
        let second = serving(&activator, &app_id()).await;
        assert_ne!(first, second);
        assert_eq!(activator.listening_for().await, vec![app_id()]);
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(tokio::net::TcpStream::connect(("127.0.0.1", first.get()))
            .await
            .is_err());
    }

    #[tokio::test]
    async fn a_port_nothing_can_bind_leaves_the_activator_answering_for_nothing() {
        let held = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
            .await
            .unwrap();
        let taken = HostPort::new(held.local_addr().unwrap().port()).unwrap();
        let activator = AppActivator::new(HostState::shared(), CountingWaker::allowing(), Arc::default());
        activator.serve(&[(app_id(), taken)]).await;
        assert!(activator.listening_for().await.is_empty());
    }
}
