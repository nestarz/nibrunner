use std::collections::BTreeMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::{Duration, Instant};

use protocol::{AppId, GuestPort, HostPort, PortName};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;

use crate::domain::metrics::proxy::{Protocol, RawOutcome};
use crate::domain::metrics::HostMetrics;
use crate::ports::{WakeRefusal, Waker};
use crate::state::SharedState;

/// One port a record named, and where it goes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamBinding {
    pub app_id: AppId,
    pub name: PortName,
    pub host_port: HostPort,
    pub guest_port: GuestPort,
}

/// How often a woken guest is asked again whether it is listening yet.
const DIAL_INTERVAL: Duration = Duration::from_millis(50);

struct Listener {
    host_port: HostPort,
    guest_port: GuestPort,
    task: tokio::task::JoinHandle<()>,
}

/// Dial the guest until it answers or the deadline passes.
///
/// The wake returns when the HTTP port answers, because that is the one port the readiness gate
/// knows. A service on a raw port can be a few hundred milliseconds behind it, and a client that
/// has just waited out a snapshot restore should not be closed on at the last step for that. Only
/// a refusal is retried — it is the one error that means "not yet" rather than "not at all".
async fn dial_until(upstream: SocketAddr, deadline: Instant) -> Option<TcpStream> {
    loop {
        match TcpStream::connect(upstream).await {
            Ok(guest) => return Some(guest),
            Err(error)
                if error.kind() == std::io::ErrorKind::ConnectionRefused && Instant::now() < deadline =>
            {
                tokio::time::sleep(DIAL_INTERVAL).await;
            }
            Err(_) => return None,
        }
    }
}

/// The way in for a protocol this host does not read.
///
/// It is the same trade the HTTP activator makes and a smaller one: while an app is up, nftables
/// dnats the host port to the guest before anything reaches this listener, so it only ever sees
/// the connections that arrived while the app was asleep. Those it holds open, wakes the app, and
/// then splices — the first bytes a client sent are still unread, so an ssh client that opened a
/// connection to a sleeping box sees a slow banner rather than a closed socket.
pub struct StreamActivator {
    state: SharedState,
    waker: Arc<dyn Waker>,
    metrics: Arc<HostMetrics>,
    listen_address: IpAddr,
    listeners: Mutex<BTreeMap<(AppId, PortName), Listener>>,
}

impl StreamActivator {
    pub fn new(
        state: SharedState,
        waker: Arc<dyn Waker>,
        metrics: Arc<HostMetrics>,
        listen_address: IpAddr,
    ) -> Arc<Self> {
        Arc::new(Self {
            state,
            waker,
            metrics,
            listen_address,
            listeners: Mutex::new(BTreeMap::new()),
        })
    }

    pub async fn serve(self: &Arc<Self>, bindings: &[StreamBinding]) {
        let wanted: BTreeMap<(AppId, PortName), &StreamBinding> = bindings
            .iter()
            .map(|binding| ((binding.app_id.clone(), binding.name.clone()), binding))
            .collect();

        let mut listeners = self.listeners.lock().await;
        listeners.retain(|key, listener| {
            let keep = wanted.get(key).is_some_and(|binding| {
                binding.host_port == listener.host_port && binding.guest_port == listener.guest_port
            });
            if !keep {
                listener.task.abort();
            }
            keep
        });

        for (key, binding) in wanted {
            if listeners.contains_key(&key) {
                continue;
            }
            let address = SocketAddr::from((self.listen_address, binding.host_port.get()));
            match TcpListener::bind(address).await {
                Ok(listener) => {
                    tracing::info!(
                        app_id = %binding.app_id,
                        port_name = %binding.name,
                        host_port = %binding.host_port,
                        guest_port = %binding.guest_port,
                        "stream activator listening"
                    );
                    let task = tokio::spawn(accept(
                        listener,
                        self.clone(),
                        binding.app_id.clone(),
                        binding.guest_port,
                    ));
                    listeners.insert(
                        key,
                        Listener {
                            host_port: binding.host_port,
                            guest_port: binding.guest_port,
                            task,
                        },
                    );
                }
                Err(error) => {
                    tracing::warn!(
                        app_id = %binding.app_id,
                        host_port = %binding.host_port,
                        %error,
                        "stream activator bind failed"
                    );
                }
            }
        }
    }

    pub async fn listening_for(&self) -> Vec<(AppId, PortName)> {
        self.listeners.lock().await.keys().cloned().collect()
    }

    async fn handle(self: Arc<Self>, app_id: AppId, guest_port: GuestPort, client: TcpStream) {
        let outcome = self.relay(&app_id, guest_port, client).await;
        self.metrics.proxy.raw_session(&app_id, Protocol::Tcp, outcome);
    }

    async fn relay(&self, app_id: &AppId, guest_port: GuestPort, client: TcpStream) -> RawOutcome {
        let Some(record) = self.state.record(app_id).await else {
            return RawOutcome::Down;
        };
        if !record.on_request || !record.desired_running {
            return RawOutcome::Down;
        }
        let Some(_open) = self
            .state
            .admit(app_id, crate::clock::now_ms(), || self.metrics.proxy.open(app_id))
            .await
        else {
            return RawOutcome::Down;
        };

        let started = Instant::now();
        if let Err(refusal) = self.waker.wake(app_id).await {
            let reason = match refusal {
                WakeRefusal::NoRoom { shortfall_mib } => {
                    format!("its machine is {shortfall_mib} MiB short of memory")
                }
                WakeRefusal::Failed { reason, .. } => reason,
            };
            tracing::warn!(%app_id, reason, "a stream could not be given an app");
            return RawOutcome::Refused;
        }
        let woke_ms = started.elapsed().as_millis();

        let Some(woken) = self.state.record(app_id).await else {
            return RawOutcome::Down;
        };
        let upstream = SocketAddr::new(
            match woken.guest_ipv4.as_str().parse() {
                Ok(address) => address,
                Err(_) => return RawOutcome::Unreachable,
            },
            guest_port.get(),
        );
        let deadline = Instant::now() + Duration::from_millis(woken.health_check.probe().grace_period_ms);
        let Some(mut guest) = dial_until(upstream, deadline).await else {
            tracing::warn!(%app_id, %upstream, "a woken app would not take the stream");
            return RawOutcome::Unreachable;
        };
        let _ = guest.set_nodelay(true);
        let mut client = client;
        let _ = client.set_nodelay(true);

        match tokio::io::copy_bidirectional(&mut client, &mut guest).await {
            Ok((from_client, from_guest)) => {
                self.metrics
                    .proxy
                    .raw_bytes(app_id, Protocol::Tcp, from_client, from_guest);
                tracing::info!(
                    %app_id,
                    woke_ms,
                    from_client,
                    from_guest,
                    "app took the stream that woke it"
                );
            }
            Err(error) => tracing::debug!(%app_id, %error, "a spliced stream ended"),
        }
        RawOutcome::Served
    }
}

async fn accept(
    listener: TcpListener,
    activator: Arc<StreamActivator>,
    app_id: AppId,
    guest_port: GuestPort,
) {
    loop {
        let Ok((stream, _)) = listener.accept().await else {
            continue;
        };
        let activator = activator.clone();
        let app_id = app_id.clone();
        tokio::spawn(async move { activator.handle(app_id, guest_port, stream).await });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::*;
    use protocol::InstanceState;

    fn binding(name: &str, host_port: u16, guest_port: u16) -> StreamBinding {
        StreamBinding {
            app_id: app_id(),
            name: PortName::parse(name).unwrap(),
            host_port: HostPort::new(host_port).unwrap(),
            guest_port: GuestPort::new(guest_port).unwrap(),
        }
    }

    struct AlwaysWakes;

    #[async_trait::async_trait]
    impl Waker for AlwaysWakes {
        async fn wake(&self, _app_id: &AppId) -> Result<(), WakeRefusal> {
            Ok(())
        }
    }

    async fn activator(host: &TestHost) -> Arc<StreamActivator> {
        StreamActivator::new(
            host.state.clone(),
            Arc::new(AlwaysWakes),
            host.metrics.clone(),
            std::net::Ipv4Addr::LOCALHOST.into(),
        )
    }

    #[tokio::test]
    async fn every_binding_it_is_given_listens_and_nothing_else_does() {
        let host = test_host().await;
        let activator = activator(&host).await;

        let mut held = Vec::new();
        let mut ports = Vec::new();
        for _ in 0..2 {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            ports.push(listener.local_addr().unwrap().port());
            held.push(listener);
        }
        drop(held);

        activator
            .serve(&[binding("ssh", ports[0], 22), binding("debug", ports[1], 9229)])
            .await;

        let listening = activator.listening_for().await;
        assert_eq!(
            listening.len(),
            2,
            "one listener per binding, and a slot's other reserved ports are not bindings"
        );
        assert!(listening.contains(&(app_id(), PortName::parse("ssh").unwrap())));
        assert!(listening.contains(&(app_id(), PortName::parse("debug").unwrap())));
    }

    #[tokio::test]
    async fn a_binding_that_is_taken_away_stops_listening() {
        let host = test_host().await;
        let activator = activator(&host).await;
        let free = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = free.local_addr().unwrap().port();
        drop(free);

        activator.serve(&[binding("ssh", port, 22)]).await;
        assert_eq!(activator.listening_for().await.len(), 1);

        activator.serve(&[]).await;
        assert!(activator.listening_for().await.is_empty());

        // The accept loop is aborted rather than joined, so the socket goes back when the task is
        // actually dropped rather than when `serve` returns.
        let address = SocketAddr::from((std::net::Ipv4Addr::LOCALHOST, port));
        for attempt in 0..50 {
            if tokio::net::TcpListener::bind(address).await.is_ok() {
                return;
            }
            assert!(attempt < 49, "the port was never given back");
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    }

    #[tokio::test]
    async fn a_stream_to_a_sleeping_app_reaches_the_guest_it_woke() {
        let host = test_host().await;
        host.state
            .put_record(instance_record(|record| {
                record.on_request = true;
                record.desired_running = true;
                record.state = InstanceState::Idle;
                record.guest_ipv4 = crate::domain::health::probe::loopback();
            }))
            .await;

        let guest = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let guest_port = guest.local_addr().unwrap().port();
        tokio::spawn(async move {
            let (mut stream, _) = guest.accept().await.unwrap();
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            let mut greeting = [0u8; 4];
            stream.read_exact(&mut greeting).await.unwrap();
            stream.write_all(b"SSH-2.0").await.unwrap();
            stream.flush().await.unwrap();
        });

        let free = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let host_port = free.local_addr().unwrap().port();
        drop(free);

        let activator = activator(&host).await;
        activator.serve(&[binding("ssh", host_port, guest_port)]).await;

        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let mut client = TcpStream::connect(SocketAddr::from((std::net::Ipv4Addr::LOCALHOST, host_port)))
            .await
            .unwrap();
        client.write_all(b"ping").await.unwrap();
        let mut answer = [0u8; 7];
        client.read_exact(&mut answer).await.unwrap();
        assert_eq!(&answer, b"SSH-2.0");

        // The session's bytes are counted when it ends, so end it.
        drop(client);
        let counted = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let app = host.metrics.proxy.of(&app_id());
                if app.raw_sessions[0][0] == 1 {
                    return app;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the stream was counted as served once it ended");
        assert_eq!((counted.raw_bytes_in[0], counted.raw_bytes_out[0]), (4, 7));
    }

    #[tokio::test]
    async fn a_guest_whose_service_is_a_moment_behind_its_wake_is_waited_for() {
        let host = test_host().await;
        host.state
            .put_record(instance_record(|record| {
                record.on_request = true;
                record.desired_running = true;
                record.state = InstanceState::Idle;
                record.guest_ipv4 = crate::domain::health::probe::loopback();
                record.health_check.probe_mut().unwrap().grace_period_ms = 2_000;
            }))
            .await;

        // Nothing listens on the guest port until well after the client has connected.
        let reserved = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let guest_port = reserved.local_addr().unwrap().port();
        drop(reserved);
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(300)).await;
            let guest =
                tokio::net::TcpListener::bind(SocketAddr::from((std::net::Ipv4Addr::LOCALHOST, guest_port)))
                    .await
                    .unwrap();
            let (mut stream, _) = guest.accept().await.unwrap();
            use tokio::io::AsyncWriteExt;
            stream.write_all(b"SSH-2.0").await.unwrap();
        });

        let free = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let host_port = free.local_addr().unwrap().port();
        drop(free);
        activator(&host)
            .await
            .serve(&[binding("ssh", host_port, guest_port)])
            .await;

        use tokio::io::AsyncReadExt;
        let mut client = TcpStream::connect(SocketAddr::from((std::net::Ipv4Addr::LOCALHOST, host_port)))
            .await
            .unwrap();
        let mut answer = [0u8; 7];
        client
            .read_exact(&mut answer)
            .await
            .expect("the activator kept dialling until sshd was there");
        assert_eq!(&answer, b"SSH-2.0");
    }

    #[tokio::test]
    async fn a_guest_that_never_listens_is_given_up_on_at_the_grace_period_not_before_and_not_never() {
        let host = test_host().await;
        host.state
            .put_record(instance_record(|record| {
                record.on_request = true;
                record.desired_running = true;
                record.state = InstanceState::Idle;
                record.guest_ipv4 = crate::domain::health::probe::loopback();
                record.health_check.probe_mut().unwrap().grace_period_ms = 200;
            }))
            .await;
        let reserved = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let guest_port = reserved.local_addr().unwrap().port();
        drop(reserved);

        let free = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let host_port = free.local_addr().unwrap().port();
        drop(free);
        activator(&host)
            .await
            .serve(&[binding("ssh", host_port, guest_port)])
            .await;

        use tokio::io::AsyncReadExt;
        let started = Instant::now();
        let mut client = TcpStream::connect(SocketAddr::from((std::net::Ipv4Addr::LOCALHOST, host_port)))
            .await
            .unwrap();
        let mut answer = [0u8; 1];
        assert_eq!(client.read(&mut answer).await.unwrap(), 0, "closed, not hung");
        let waited = started.elapsed();
        assert!(waited >= Duration::from_millis(150), "gave up early: {waited:?}");
        assert!(waited < Duration::from_secs(2), "never gave up: {waited:?}");
    }

    #[tokio::test]
    async fn a_stream_to_an_app_that_is_not_asked_for_is_dropped_rather_than_dialled() {
        let host = test_host().await;
        host.state
            .put_record(instance_record(|record| {
                record.on_request = true;
                record.desired_running = false;
                record.state = InstanceState::Stopped;
            }))
            .await;
        let free = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let host_port = free.local_addr().unwrap().port();
        drop(free);

        let activator = activator(&host).await;
        activator.serve(&[binding("ssh", host_port, 22)]).await;

        use tokio::io::AsyncReadExt;
        let mut client = TcpStream::connect(SocketAddr::from((std::net::Ipv4Addr::LOCALHOST, host_port)))
            .await
            .unwrap();
        let mut answer = [0u8; 1];
        assert_eq!(
            client.read(&mut answer).await.unwrap(),
            0,
            "a stopped app is a closed socket, not a hung one"
        );
    }
}
