use std::collections::BTreeMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use protocol::{AppId, GuestPort, PortName};
use tokio::net::UdpSocket;
use tokio::sync::Mutex;

use crate::adapters::proxy::stream_activator::StreamBinding;
use crate::domain::metrics::proxy::{Protocol, RawOutcome};
use crate::domain::metrics::HostMetrics;
use crate::ports::Waker;
use crate::state::SharedState;

/// The largest datagram this relay will carry in one piece.
///
/// Larger than any path MTU, so nothing a sender could reasonably put on the wire is truncated,
/// and small enough that a buffer per waiting datagram costs nothing.
const MAX_DATAGRAM: usize = 65_535;

/// How long a client's reply socket is kept after the last datagram crossed it.
///
/// A datagram carries no close, so there is nothing to wait for: what ends a session here is
/// nobody using it. Only the connections that arrive while an app sleeps ever reach this relay,
/// so a session is short-lived by construction and this is a backstop, not a tuning knob.
const SESSION_IDLE: Duration = Duration::from_secs(60);

struct Listener {
    guest_port: GuestPort,
    task: tokio::task::JoinHandle<()>,
}

/// The way in for a protocol that has no connection to hold.
///
/// The stream activator can accept a connection and leave a client waiting while the app boots.
/// A datagram gives nothing to hold, so this holds the datagram instead: the first one to arrive
/// for a sleeping app is kept, the app is woken, and it is delivered once the guest is there to
/// take it. A sender that gave up first loses that datagram — which is what UDP already promises,
/// and why anything built on it retries.
///
/// Once the app is up, nftables dnats the port straight to the guest and nothing arrives here at
/// all, so what this carries is only ever the wake.
pub struct DatagramActivator {
    state: SharedState,
    waker: Arc<dyn Waker>,
    metrics: Arc<HostMetrics>,
    listen_address: IpAddr,
    listeners: Mutex<BTreeMap<(AppId, PortName), Listener>>,
}

impl DatagramActivator {
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
            let keep = wanted
                .get(key)
                .is_some_and(|binding| binding.guest_port == listener.guest_port);
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
            match UdpSocket::bind(address).await {
                Ok(socket) => {
                    tracing::info!(
                        app_id = %binding.app_id,
                        port_name = %binding.name,
                        host_port = %binding.host_port,
                        guest_port = %binding.guest_port,
                        "datagram activator listening"
                    );
                    let task = tokio::spawn(receive(
                        socket,
                        self.clone(),
                        binding.app_id.clone(),
                        binding.guest_port,
                    ));
                    listeners.insert(
                        key,
                        Listener {
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
                        "datagram activator bind failed"
                    );
                }
            }
        }
    }

    pub async fn listening_for(&self) -> Vec<(AppId, PortName)> {
        self.listeners.lock().await.keys().cloned().collect()
    }

    /// Where a woken app's datagram port is, or why there is nowhere to send to.
    async fn upstream(&self, app_id: &AppId, guest_port: GuestPort) -> Result<SocketAddr, RawOutcome> {
        let Some(record) = self.state.record(app_id).await else {
            return Err(RawOutcome::Down);
        };
        if !record.on_request || !record.desired_running {
            return Err(RawOutcome::Down);
        }
        let Some(_open) = self
            .state
            .admit(app_id, crate::clock::now_ms(), || self.metrics.proxy.open(app_id))
            .await
        else {
            return Err(RawOutcome::Down);
        };

        let started = std::time::Instant::now();
        if let Err(refusal) = self.waker.wake(app_id).await {
            tracing::warn!(%app_id, ?refusal, "a datagram could not be given an app");
            return Err(RawOutcome::Refused);
        }
        let Some(woken) = self.state.record(app_id).await else {
            return Err(RawOutcome::Down);
        };
        let address = woken
            .guest_ipv4
            .as_str()
            .parse()
            .map_err(|_| RawOutcome::Unreachable)?;
        tracing::info!(
            %app_id,
            woke_ms = started.elapsed().as_millis(),
            "app woken by a datagram"
        );
        Ok(SocketAddr::new(address, guest_port.get()))
    }
}

/// One client's side of the relay: everything the guest sends back goes to the address the first
/// datagram came from, until nobody has used it for [`SESSION_IDLE`].
async fn relay_replies(
    guest: Arc<UdpSocket>,
    inbound: Arc<UdpSocket>,
    client: SocketAddr,
    activator: Arc<DatagramActivator>,
    app_id: AppId,
) {
    let mut buffer = vec![0u8; MAX_DATAGRAM];
    loop {
        let read = tokio::time::timeout(SESSION_IDLE, guest.recv(&mut buffer)).await;
        let Ok(Ok(length)) = read else {
            return;
        };
        if inbound.send_to(&buffer[..length], client).await.is_err() {
            return;
        }
        activator
            .metrics
            .proxy
            .raw_bytes(&app_id, Protocol::Udp, 0, length as u64);
    }
}

async fn receive(
    inbound: UdpSocket,
    activator: Arc<DatagramActivator>,
    app_id: AppId,
    guest_port: GuestPort,
) {
    let inbound = Arc::new(inbound);
    let mut sessions: BTreeMap<SocketAddr, Arc<UdpSocket>> = BTreeMap::new();
    let mut buffer = vec![0u8; MAX_DATAGRAM];

    loop {
        let Ok((length, client)) = inbound.recv_from(&mut buffer).await else {
            continue;
        };
        let Some(_open) = activator
            .state
            .admit(&app_id, crate::clock::now_ms(), || {
                activator.metrics.proxy.open(&app_id)
            })
            .await
        else {
            continue;
        };
        let datagram = &buffer[..length];

        if let Some(guest) = sessions.get(&client) {
            if guest.send(datagram).await.is_ok() {
                activator
                    .metrics
                    .proxy
                    .raw_bytes(&app_id, Protocol::Udp, length as u64, 0);
            }
            continue;
        }

        // The datagram is held for exactly as long as the wake takes, and then delivered.
        let upstream = match activator.upstream(&app_id, guest_port).await {
            Ok(upstream) => upstream,
            Err(outcome) => {
                activator
                    .metrics
                    .proxy
                    .raw_session(&app_id, Protocol::Udp, outcome);
                continue;
            }
        };
        let unspecified = match upstream {
            SocketAddr::V4(_) => SocketAddr::from(([0, 0, 0, 0], 0)),
            SocketAddr::V6(_) => SocketAddr::from(([0u16; 8], 0)),
        };
        let Ok(guest) = UdpSocket::bind(unspecified).await else {
            activator
                .metrics
                .proxy
                .raw_session(&app_id, Protocol::Udp, RawOutcome::Unreachable);
            continue;
        };
        if guest.connect(upstream).await.is_err() {
            activator
                .metrics
                .proxy
                .raw_session(&app_id, Protocol::Udp, RawOutcome::Unreachable);
            continue;
        }
        let guest = Arc::new(guest);
        if guest.send(datagram).await.is_err() {
            activator
                .metrics
                .proxy
                .raw_session(&app_id, Protocol::Udp, RawOutcome::Unreachable);
            continue;
        }
        activator
            .metrics
            .proxy
            .raw_session(&app_id, Protocol::Udp, RawOutcome::Served);
        activator
            .metrics
            .proxy
            .raw_bytes(&app_id, Protocol::Udp, length as u64, 0);
        tokio::spawn(relay_replies(
            guest.clone(),
            inbound.clone(),
            client,
            activator.clone(),
            app_id.clone(),
        ));
        sessions.insert(client, guest);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ports::WakeRefusal;
    use crate::test_support::*;
    use protocol::{HostPort, InstanceState};

    struct AlwaysWakes;

    #[async_trait::async_trait]
    impl Waker for AlwaysWakes {
        async fn wake(&self, _app_id: &AppId) -> Result<(), WakeRefusal> {
            Ok(())
        }
    }

    fn binding(name: &str, host_port: u16, guest_port: u16) -> StreamBinding {
        StreamBinding {
            app_id: app_id(),
            name: PortName::parse(name).unwrap(),
            host_port: HostPort::new(host_port).unwrap(),
            guest_port: GuestPort::new(guest_port).unwrap(),
        }
    }

    fn activator(host: &TestHost) -> Arc<DatagramActivator> {
        DatagramActivator::new(
            host.state.clone(),
            Arc::new(AlwaysWakes),
            host.metrics.clone(),
            std::net::Ipv4Addr::LOCALHOST.into(),
        )
    }

    /// A free port, given up immediately so the activator can take it.
    async fn free_port() -> u16 {
        let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        socket.local_addr().unwrap().port()
    }

    async fn sleeping_app(host: &TestHost) {
        host.state
            .put_record(instance_record(|record| {
                record.on_request = true;
                record.desired_running = true;
                record.state = InstanceState::Idle;
                record.guest_ipv4 = crate::domain::health::probe::loopback();
            }))
            .await;
    }

    #[tokio::test]
    async fn every_binding_it_is_given_listens_and_nothing_else_does() {
        let host = test_host().await;
        let activator = activator(&host);

        activator
            .serve(&[
                binding("dns", free_port().await, 53),
                binding("wg", free_port().await, 51820),
            ])
            .await;

        let listening = activator.listening_for().await;
        assert_eq!(listening.len(), 2);
        assert!(listening.contains(&(app_id(), PortName::parse("dns").unwrap())));
        assert!(listening.contains(&(app_id(), PortName::parse("wg").unwrap())));
    }

    #[tokio::test]
    async fn a_binding_that_is_taken_away_stops_listening() {
        let host = test_host().await;
        let activator = activator(&host);
        activator.serve(&[binding("dns", free_port().await, 53)]).await;
        assert_eq!(activator.listening_for().await.len(), 1);

        activator.serve(&[]).await;
        assert!(activator.listening_for().await.is_empty());
    }

    #[tokio::test]
    async fn the_datagram_that_woke_an_app_is_the_one_the_app_receives() {
        let host = test_host().await;
        sleeping_app(&host).await;

        let guest = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let guest_port = guest.local_addr().unwrap().port();
        tokio::spawn(async move {
            let mut buffer = vec![0u8; 64];
            let (length, from) = guest.recv_from(&mut buffer).await.unwrap();
            assert_eq!(&buffer[..length], b"query");
            guest.send_to(b"answer", from).await.unwrap();
        });

        let host_port = free_port().await;
        activator(&host)
            .serve(&[binding("dns", host_port, guest_port)])
            .await;

        let client = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let inbound = SocketAddr::from((std::net::Ipv4Addr::LOCALHOST, host_port));
        client.send_to(b"query", inbound).await.unwrap();

        let mut buffer = vec![0u8; 64];
        let read = tokio::time::timeout(Duration::from_secs(5), client.recv_from(&mut buffer))
            .await
            .expect("the guest answered the datagram that woke it");
        let (length, _) = read.unwrap();
        assert_eq!(
            &buffer[..length],
            b"answer",
            "a reply reaches the client that sent the first datagram"
        );

        let counted = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let app = host.metrics.proxy.of(&app_id());
                if app.raw_bytes_out[1] == 6 {
                    return app;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the reply was counted once it was relayed");
        assert_eq!(counted.raw_sessions[1][0], 1, "one session, served");
        assert_eq!(counted.raw_bytes_in[1], 5);
    }

    #[tokio::test]
    async fn a_datagram_for_an_app_that_is_not_asked_for_is_dropped_rather_than_relayed() {
        let host = test_host().await;
        host.state
            .put_record(instance_record(|record| {
                record.on_request = true;
                record.desired_running = false;
                record.state = InstanceState::Stopped;
            }))
            .await;

        let host_port = free_port().await;
        activator(&host).serve(&[binding("dns", host_port, 53)]).await;

        let client = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        client
            .send_to(
                b"query",
                SocketAddr::from((std::net::Ipv4Addr::LOCALHOST, host_port)),
            )
            .await
            .unwrap();

        let mut buffer = vec![0u8; 64];
        assert!(
            tokio::time::timeout(Duration::from_millis(300), client.recv_from(&mut buffer))
                .await
                .is_err(),
            "a stopped app answers nothing rather than being dialled"
        );
    }
}
