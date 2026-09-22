//! The ports an app names besides its HTTP one: bytes reach the guest, and the first thing that
//! arrives on one finds the app however it left it.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::Duration;

use protocol::{GuestPort, InstancePort, InstanceState, PortName};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const ECHO_TCP: u16 = 4_000;
const ECHO_UDP: u16 = 4_001;
const ANSWERED_WITHIN: Duration = Duration::from_secs(15);

fn at(port: protocol::HostPort) -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port.get())
}

async fn echoed_over_stream(address: SocketAddr, said: &[u8]) -> std::io::Result<Vec<u8>> {
    let mut stream = tokio::time::timeout(ANSWERED_WITHIN, tokio::net::TcpStream::connect(address))
        .await
        .unwrap_or_else(|_| Err(std::io::Error::other("nothing accepted in time")))?;
    stream.write_all(said).await?;
    let mut back = vec![0u8; said.len()];
    tokio::time::timeout(ANSWERED_WITHIN, stream.read_exact(&mut back))
        .await
        .unwrap_or_else(|_| Err(std::io::Error::other("nothing came back in time")))?;
    Ok(back)
}

async fn echoed_over_datagram(address: SocketAddr, said: &[u8]) -> std::io::Result<Vec<u8>> {
    let socket = tokio::net::UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await?;
    socket.send_to(said, address).await?;
    let mut back = vec![0u8; 1024];
    let read = tokio::time::timeout(ANSWERED_WITHIN, socket.recv(&mut back))
        .await
        .unwrap_or_else(|_| Err(std::io::Error::other("nothing came back in time")))?;
    back.truncate(read);
    Ok(back)
}

fn echoing(
    host: &nibrunnerd::test_support::machine::RunningHost,
    number: u32,
) -> nibrunnerd::test_support::machine::Tenant {
    host.tenant(number)
        .on_request(60_000)
        .arguments(&[
            "--raw-tcp",
            &ECHO_TCP.to_string(),
            "--raw-udp",
            &ECHO_UDP.to_string(),
        ])
        .edited(|instance| {
            instance.config.ports = vec![
                InstancePort {
                    name: PortName::parse("echo").expect("a port name"),
                    guest_port: GuestPort::new(ECHO_TCP).expect("a guest port"),
                },
                InstancePort {
                    name: PortName::parse("beat").expect("a port name"),
                    guest_port: GuestPort::new(ECHO_UDP).expect("a guest port"),
                },
            ];
        })
}

#[tokio::test(flavor = "multi_thread")]
async fn a_raw_stream_port_reaches_the_guest_and_the_first_connection_wakes_it() {
    let Some(host) = crate::host().await else {
        return;
    };
    let app = echoing(&host, 1);
    host.deploy(std::slice::from_ref(&app)).await;
    host.until_state(&app.app_id, InstanceState::Running).await;

    let slot = host.host.slot_of(&app.app_id).await.expect("a slot");
    host.until_routed(&app).await;

    let port = at(slot.host_port_at(1).expect("the first named port"));
    let back = echoed_over_stream(port, b"what went in").await.expect("an echo");
    assert_eq!(back, b"what went in", "the raw port did not reach the guest");

    // Asleep, the host port is a socket the activator holds itself rather than a rule forwarding
    // to the guest, and the connection that finds it has to be carried through the wake.
    host.let_sleep(&app).await;
    let woken = std::time::Instant::now();
    let back = echoed_over_stream(port, b"and again").await.expect("an echo");
    assert_eq!(
        back, b"and again",
        "the connection that woke it lost what it carried"
    );
    println!("woken over a raw stream port in {:?}", woken.elapsed());

    host.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_raw_datagram_port_reaches_the_guest_and_the_first_datagram_wakes_it() {
    let Some(host) = crate::host().await else {
        return;
    };
    let app = echoing(&host, 1);
    host.deploy(std::slice::from_ref(&app)).await;
    host.until_state(&app.app_id, InstanceState::Running).await;

    let slot = host.host.slot_of(&app.app_id).await.expect("a slot");
    host.until_routed(&app).await;

    let port = at(slot.host_port_at(2).expect("the second named port"));
    let back = echoed_over_datagram(port, b"a datagram").await.expect("an echo");
    assert_eq!(back, b"a datagram", "the raw port did not reach the guest");

    // A datagram has nothing to hold open, so what is asked of the one that finds the app asleep
    // is that it wakes it; what it carried may or may not survive.
    host.let_sleep(&app).await;
    let _ = echoed_over_datagram(port, b"wake up").await;
    host.until_state(&app.app_id, InstanceState::Running).await;
    let back = echoed_over_datagram(port, b"and again").await.expect("an echo");
    assert_eq!(back, b"and again");

    host.stop().await;
}
