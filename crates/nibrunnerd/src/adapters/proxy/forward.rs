use bytes::Bytes;
use http_body_util::combinators::BoxBody;
use http_body_util::{BodyExt, Full};
use hyper::body::{Body, Frame, Incoming, SizeHint};
use hyper::header::{HeaderName, HeaderValue, CONNECTION, UPGRADE};
use hyper::{Request, Response, StatusCode, Uri};
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::client::legacy::Client;
use hyper_util::rt::{TokioExecutor, TokioTimer};
use protocol::AppId;
use std::collections::{BTreeMap, BTreeSet};
use std::net::IpAddr;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::sync::RwLock;

use crate::domain::metrics::proxy::OpenRequest;

pub type ProxyBody = BoxBody<Bytes, hyper::Error>;

pub(crate) struct ForwardedRequest {
    _metrics: OpenRequest,
    _admission: Option<super::admission::Permit>,
}

impl ForwardedRequest {
    pub(crate) fn new(metrics: OpenRequest, admission: Option<super::admission::Permit>) -> Self {
        Self {
            _metrics: metrics,
            _admission: admission,
        }
    }
}

impl From<OpenRequest> for ForwardedRequest {
    fn from(metrics: OpenRequest) -> Self {
        Self::new(metrics, None)
    }
}

/// An upstream's body carrying the request it answers, so the request stays open until the body
/// has been sent whole or dropped: the proxy hands the response on as soon as its headers are in,
/// and a stream's body is still being written long after that.
struct HeldOpen<B> {
    body: B,
    _request: ForwardedRequest,
}

impl<B: Body + Unpin> Body for HeldOpen<B> {
    type Data = B::Data;
    type Error = B::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        Pin::new(&mut self.get_mut().body).poll_frame(cx)
    }

    fn is_end_stream(&self) -> bool {
        self.body.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        self.body.size_hint()
    }
}

const HOP_BY_HOP: [&str; 8] = [
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
];

fn strip_hop_by_hop(headers: &mut hyper::HeaderMap) {
    for name in HOP_BY_HOP {
        headers.remove(name);
    }
}

// A connection the pool keeps to a guest is one that guest's kernel probes when nothing is on it,
// and this host's answer is counted as traffic addressed to the app — Go servers probe every
// fifteen seconds, so an app written in one never slept. The pool evicts somewhere between once
// and twice this, which is before the first probe and long before an idle window runs out.
const UPSTREAM_IDLE: Duration = Duration::from_secs(5);

pub fn upstream_client() -> Client<HttpConnector, Incoming> {
    upstream_client_dropping_idle_after(UPSTREAM_IDLE)
}

fn upstream_client_dropping_idle_after(idle: Duration) -> Client<HttpConnector, Incoming> {
    let mut connector = HttpConnector::new();
    connector.set_nodelay(true);
    Client::builder(TokioExecutor::new())
        .pool_timer(TokioTimer::new())
        .pool_idle_timeout(idle)
        .build(connector)
}

/// The connections this proxy keeps into the guests, a pool of its own for each app.
///
/// An open connection is pinned to its guest by conntrack, and the ruleset that hands an app's
/// host port to the activator does not touch it: a request reusing one after the guest was paused
/// is carried into the paused machine rather than to the activator, and waits there for a restore
/// nobody has asked for. Hyper's pool cannot be told to forget one address, so an app's
/// connections are held apart from every other app's in order to be droppable at all.
pub struct Upstreams {
    clients: RwLock<BTreeMap<AppId, Client<HttpConnector, Incoming>>>,
    idle: Duration,
}

impl Upstreams {
    fn dropping_idle_after(idle: Duration) -> Self {
        Self {
            clients: RwLock::new(BTreeMap::new()),
            idle,
        }
    }

    pub async fn to(&self, app_id: &AppId) -> Client<HttpConnector, Incoming> {
        if let Some(client) = self.clients.read().await.get(app_id) {
            return client.clone();
        }
        self.clients
            .write()
            .await
            .entry(app_id.clone())
            .or_insert_with(|| upstream_client_dropping_idle_after(self.idle))
            .clone()
    }

    /// What is idle is closed now, and what is still carrying an answer is left to finish and
    /// closed when it has: either way the next request for this app opens a connection of its own.
    pub async fn close_connections_to(&self, app_id: &AppId) {
        self.clients.write().await.remove(app_id);
    }

    pub async fn keep_only(&self, apps: &BTreeSet<AppId>) {
        self.clients
            .write()
            .await
            .retain(|app_id, _| apps.contains(app_id));
    }
}

impl Default for Upstreams {
    fn default() -> Self {
        Self::dropping_idle_after(UPSTREAM_IDLE)
    }
}

fn rewritten(uri: &Uri, host: &str, port: u16) -> Uri {
    let path = uri.path_and_query().map_or("/", |path| path.as_str());
    Uri::builder()
        .scheme("http")
        .authority(format!("{host}:{port}"))
        .path_and_query(path)
        .build()
        .unwrap_or_else(|_| Uri::from_static("http://127.0.0.1/"))
}

fn upgrade_requested<B>(request: &Request<B>) -> bool {
    request.version() == hyper::Version::HTTP_11
        && request.headers().contains_key(UPGRADE)
        && request
            .headers()
            .get_all(CONNECTION)
            .iter()
            .filter_map(|value| value.to_str().ok())
            .flat_map(|value| value.split(','))
            .any(|token| token.trim().eq_ignore_ascii_case("upgrade"))
}

// A websocket is a request that stops being one. The pooled client cannot carry it, because the
// connection leaves HTTP the moment the upstream agrees and can never be handed back to the pool,
// so this leg dials a connection of its own and keeps it for as long as the two sides talk. The
// headers that say what the upgrade is are the ones `strip_hop_by_hop` exists to remove, which is
// why nothing is stripped on the way through here.
async fn forward_upgrade(
    request: Request<Incoming>,
    host: &str,
    port: u16,
    open: ForwardedRequest,
) -> Response<ProxyBody> {
    let (mut parts, body) = request.into_parts();
    parts.uri = rewritten(&parts.uri, host, port);
    let mut forwarded = Request::from_parts(parts, body);
    let downstream = hyper::upgrade::on(&mut forwarded);

    let Ok(stream) = tokio::net::TcpStream::connect((host, port)).await else {
        return could_not_be_reached();
    };
    let Ok((mut sender, connection)) =
        hyper::client::conn::http1::handshake(hyper_util::rt::TokioIo::new(stream)).await
    else {
        return could_not_be_reached();
    };
    tokio::spawn(async move {
        let _ = connection.with_upgrades().await;
    });
    let mut answered = match sender.send_request(forwarded).await {
        Ok(answered) => answered,
        Err(error) => {
            tracing::warn!(%error, host, port, "an upstream would not take an upgrade");
            return could_not_be_reached();
        }
    };
    if answered.status() == StatusCode::SWITCHING_PROTOCOLS {
        let upstream = hyper::upgrade::on(&mut answered);
        tokio::spawn(async move {
            let _open_while_they_talk = open;
            let (Ok(downstream), Ok(upstream)) = (downstream.await, upstream.await) else {
                return;
            };
            let _ = tokio::io::copy_bidirectional(
                &mut hyper_util::rt::TokioIo::new(downstream),
                &mut hyper_util::rt::TokioIo::new(upstream),
            )
            .await;
        });
        let (parts, body) = answered.into_parts();
        return Response::from_parts(parts, body.boxed());
    }
    let (parts, body) = answered.into_parts();
    Response::from_parts(parts, HeldOpen { body, _request: open }.boxed())
}

// The leg to a tenant is HTTP/1.1 whatever the visitor spoke. Carried over as it came, HTTP/2 is a
// version the pooled client refuses outright, and HTTP/1.0 one the guest honours by closing the
// connection once it has answered — a visitor speaking it, as `ab` does, cost a connection to the
// guest per request, and each closed one sat in conntrack for two minutes after. The name the
// visitor asked for has to be carried over too: HTTP/2 keeps it in the URI rather than a host
// header, an HTTP/1 request may name it only in its request line, and the URI is about to be
// rewritten onto loopback, so the tenant would be told it was asked for 127.0.0.1 rather than for
// the name its visitor typed. A request that names it in both was routed by the header, which stays.
fn as_the_upstream_speaks(parts: &mut hyper::http::request::Parts) {
    let named_by_the_uri =
        parts.version == hyper::Version::HTTP_2 || !parts.headers.contains_key(hyper::header::HOST);
    parts.version = hyper::Version::HTTP_11;
    if !named_by_the_uri {
        return;
    }
    if let Some(asked_for) = parts
        .uri
        .authority()
        .and_then(|authority| HeaderValue::from_str(authority.as_str()).ok())
    {
        parts.headers.insert(hyper::header::HOST, asked_for);
    }
}

pub(crate) async fn forward(
    client: &Client<HttpConnector, Incoming>,
    request: Request<Incoming>,
    host: &str,
    port: u16,
    keep_alive: bool,
    open: impl Into<ForwardedRequest>,
) -> Response<ProxyBody> {
    let open = open.into();
    if upgrade_requested(&request) {
        return forward_upgrade(request, host, port, open).await;
    }
    let (mut parts, body) = request.into_parts();
    as_the_upstream_speaks(&mut parts);
    parts.uri = rewritten(&parts.uri, host, port);
    strip_hop_by_hop(&mut parts.headers);
    let forwarded = Request::from_parts(parts, body);

    match client.request(forwarded).await {
        Ok(upstream) => {
            let (mut parts, body) = upstream.into_parts();
            strip_hop_by_hop(&mut parts.headers);
            if !keep_alive {
                parts.headers.insert(
                    HeaderName::from_static("connection"),
                    HeaderValue::from_static("close"),
                );
            }
            Response::from_parts(parts, HeldOpen { body, _request: open }.boxed())
        }
        Err(error) => {
            tracing::warn!(%error, host, port, "an upstream would not answer");
            could_not_be_reached()
        }
    }
}

/// On a response the proxy wrote itself because nothing answered, so that a 502 of its own can
/// be told from one the app sent.
#[derive(Debug, Clone, Copy)]
pub struct Unreachable;

fn could_not_be_reached() -> Response<ProxyBody> {
    let mut response = say(StatusCode::BAD_GATEWAY, "This app could not be reached.\n");
    response.extensions_mut().insert(Unreachable);
    response
}

// What the visitor's own connection looked like, for a tenant that only ever sees a loopback one.
// An edge in front of this host has already written down the leg it terminated, and that account is
// the better one: only the hop this proxy is making gets added, and only what nobody has said yet
// gets filled in.
pub fn note_the_hop(headers: &mut hyper::HeaderMap, peer: IpAddr, secure: bool, hostname: &str) {
    let travelled = match headers
        .get("x-forwarded-for")
        .and_then(|value| value.to_str().ok())
    {
        Some(already) => format!("{already}, {peer}"),
        None => peer.to_string(),
    };
    if let Ok(travelled) = HeaderValue::from_str(&travelled) {
        headers.insert(HeaderName::from_static("x-forwarded-for"), travelled);
    }
    if !headers.contains_key("x-forwarded-proto") {
        headers.insert(
            HeaderName::from_static("x-forwarded-proto"),
            HeaderValue::from_static(if secure { "https" } else { "http" }),
        );
    }
    if !headers.contains_key("x-forwarded-host") {
        if let Ok(hostname) = HeaderValue::from_str(hostname) {
            headers.insert(HeaderName::from_static("x-forwarded-host"), hostname);
        }
    }
}

pub fn say(status: StatusCode, message: &str) -> Response<ProxyBody> {
    Response::builder()
        .status(status)
        .header("content-type", "text/plain; charset=utf-8")
        .header("cache-control", "no-store")
        .header("connection", "close")
        .body(
            Full::new(Bytes::from(message.to_string()))
                .map_err(|never| match never {})
                .boxed(),
        )
        .expect("a constant response is always buildable")
}

pub fn hostname_of(request: &Request<Incoming>) -> Option<String> {
    request
        .headers()
        .get(hyper::header::HOST)
        .and_then(|value| value.to_str().ok())
        .or_else(|| request.uri().host())
        .map(|host| host.split(':').next().unwrap_or(host).to_ascii_lowercase())
}

#[cfg(test)]
mod tests {
    use super::*;
    use http_body_util::BodyExt;

    #[test]
    fn a_uri_is_rewritten_onto_the_upstream_keeping_its_path_and_query() {
        let uri: Uri = "https://app.example.com/a/b?c=1".parse().unwrap();
        assert_eq!(
            rewritten(&uri, "127.0.0.1", 21000).to_string(),
            "http://127.0.0.1:21000/a/b?c=1"
        );
        let bare: Uri = "/".parse().unwrap();
        assert_eq!(
            rewritten(&bare, "10.201.0.2", 3000).to_string(),
            "http://10.201.0.2:3000/"
        );
    }

    #[test]
    fn a_request_that_arrived_over_http2_reaches_the_tenant_as_the_version_it_speaks() {
        let arrived = Request::builder()
            .version(hyper::Version::HTTP_2)
            .uri("https://app-1.apps.example.com/a?b=1")
            .body(())
            .unwrap();
        let (mut parts, ()) = arrived.into_parts();
        as_the_upstream_speaks(&mut parts);
        assert_eq!(parts.version, hyper::Version::HTTP_11);
        assert_eq!(
            parts.headers.get(hyper::header::HOST).unwrap(),
            "app-1.apps.example.com",
            "the name the visitor asked for outlives a URI that is about to name loopback"
        );
        assert_eq!(
            rewritten(&parts.uri, "127.0.0.1", 21000).to_string(),
            "http://127.0.0.1:21000/a?b=1"
        );
    }

    #[test]
    fn a_request_that_arrived_over_http1_1_is_left_the_way_it_came() {
        let arrived = Request::builder()
            .version(hyper::Version::HTTP_11)
            .uri("/a")
            .header("host", "app-1.apps.example.com")
            .body(())
            .unwrap();
        let (mut parts, ()) = arrived.into_parts();
        as_the_upstream_speaks(&mut parts);
        assert_eq!(parts.version, hyper::Version::HTTP_11);
        assert_eq!(
            parts.headers.get(hyper::header::HOST).unwrap(),
            "app-1.apps.example.com"
        );
    }

    #[test]
    fn a_request_that_arrived_over_http1_0_reaches_the_tenant_as_http1_1() {
        let arrived = Request::builder()
            .version(hyper::Version::HTTP_10)
            .uri("/a")
            .header("host", "app-1.apps.example.com")
            .body(())
            .unwrap();
        let (mut parts, ()) = arrived.into_parts();
        as_the_upstream_speaks(&mut parts);
        assert_eq!(parts.version, hyper::Version::HTTP_11);
        assert_eq!(
            parts.headers.get(hyper::header::HOST).unwrap(),
            "app-1.apps.example.com"
        );
    }

    #[test]
    fn a_request_that_names_its_host_only_in_the_line_itself_tells_the_tenant_that_name() {
        let arrived = Request::builder()
            .version(hyper::Version::HTTP_10)
            .uri("http://app-1.apps.example.com/a?b=1")
            .body(())
            .unwrap();
        let (mut parts, ()) = arrived.into_parts();
        as_the_upstream_speaks(&mut parts);
        assert_eq!(parts.version, hyper::Version::HTTP_11);
        assert_eq!(
            parts.headers.get(hyper::header::HOST).unwrap(),
            "app-1.apps.example.com",
            "the name the visitor asked for outlives a URI that is about to name loopback"
        );

        let named_twice = Request::builder()
            .version(hyper::Version::HTTP_11)
            .uri("http://line.example.com/")
            .header("host", "header.example.com")
            .body(())
            .unwrap();
        let (mut parts, ()) = named_twice.into_parts();
        as_the_upstream_speaks(&mut parts);
        assert_eq!(
            parts.headers.get(hyper::header::HOST).unwrap(),
            "header.example.com",
            "the header is what the client asked for, and what it was routed by"
        );
    }

    #[test]
    fn hop_by_hop_headers_do_not_travel() {
        let mut headers = hyper::HeaderMap::new();
        headers.insert("connection", HeaderValue::from_static("keep-alive"));
        headers.insert("upgrade", HeaderValue::from_static("websocket"));
        headers.insert("x-real", HeaderValue::from_static("kept"));
        strip_hop_by_hop(&mut headers);
        assert!(headers.get("connection").is_none());
        assert!(headers.get("upgrade").is_none());
        assert_eq!(headers.get("x-real").unwrap(), "kept");
    }

    #[tokio::test]
    async fn a_refusal_reads_as_a_sentence_and_is_not_reusable() {
        let response = say(StatusCode::SERVICE_UNAVAILABLE, "This app is not running.\n");
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(response.headers().get("connection").unwrap(), "close");
        assert_eq!(
            response.headers().get("cache-control").unwrap(),
            "no-store",
            "a refusal must never be cached against the app"
        );
        let body = response.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(std::str::from_utf8(&body).unwrap(), "This app is not running.\n");
    }

    #[test]
    fn a_uri_that_cannot_be_rebuilt_falls_back_to_somewhere_that_answers_nothing() {
        let uri: Uri = "/a".parse().unwrap();
        assert_eq!(
            rewritten(&uri, "not a host", 3000).to_string(),
            "http://127.0.0.1/"
        );
    }

    async fn reading_hostnames() -> u16 {
        let listener = tokio::net::TcpListener::bind(std::net::SocketAddr::from(([127, 0, 0, 1], 0)))
            .await
            .unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    return;
                };
                tokio::spawn(async move {
                    let service = hyper::service::service_fn(|request: Request<Incoming>| async move {
                        let read = hostname_of(&request).unwrap_or_else(|| "(none)".to_string());
                        Ok::<_, std::convert::Infallible>(say(StatusCode::OK, &read))
                    });
                    let _ = hyper::server::conn::http1::Builder::new()
                        .serve_connection(hyper_util::rt::TokioIo::new(stream), service)
                        .await;
                });
            }
        });
        port
    }

    async fn asked(port: u16, request: &str) -> String {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let mut stream = tokio::net::TcpStream::connect(("127.0.0.1", port)).await.unwrap();
        stream.write_all(request.as_bytes()).await.unwrap();
        let mut answer = String::new();
        tokio::time::timeout(
            std::time::Duration::from_secs(10),
            stream.read_to_string(&mut answer),
        )
        .await
        .expect("the proxy closes the connection it answered on")
        .unwrap();
        answer.rsplit("\r\n\r\n").next().unwrap_or_default().to_string()
    }

    #[tokio::test]
    async fn the_hostname_a_request_names_is_read_from_the_header_and_stripped_of_its_port() {
        let port = reading_hostnames().await;
        assert_eq!(
            asked(port, "GET / HTTP/1.0\r\nHost: app-1.apps.example.com\r\n\r\n").await,
            "app-1.apps.example.com"
        );
        assert_eq!(
            asked(
                port,
                "GET / HTTP/1.0\r\nHost: APP-1.Apps.Example.Com:8443\r\n\r\n"
            )
            .await,
            "app-1.apps.example.com"
        );
    }

    #[tokio::test]
    async fn a_request_that_names_no_host_at_all_names_no_app() {
        let port = reading_hostnames().await;
        assert_eq!(asked(port, "GET /a/b HTTP/1.0\r\n\r\n").await, "(none)");
    }

    #[tokio::test]
    async fn a_request_that_names_its_host_only_in_the_line_itself_is_still_routed() {
        let port = reading_hostnames().await;
        assert_eq!(
            asked(port, "GET http://App-1.Example.Com:80/a HTTP/1.0\r\n\r\n").await,
            "app-1.example.com"
        );
        assert_eq!(
            asked(
                port,
                "GET http://line.example.com/ HTTP/1.0\r\nHost: header.example.com\r\n\r\n"
            )
            .await,
            "header.example.com",
            "the header is what the client asked for"
        );
    }

    async fn proxying(host: &'static str, port: u16, keep_alive: bool) -> u16 {
        proxying_with(upstream_client(), host, port, keep_alive).await
    }

    async fn proxying_with(
        client: Client<HttpConnector, Incoming>,
        host: &'static str,
        port: u16,
        keep_alive: bool,
    ) -> u16 {
        proxying_counted_by(
            std::sync::Arc::new(crate::domain::metrics::ProxyMetrics::default()),
            client,
            host,
            port,
            keep_alive,
        )
        .await
    }

    async fn proxying_counted_by(
        metrics: std::sync::Arc<crate::domain::metrics::ProxyMetrics>,
        client: Client<HttpConnector, Incoming>,
        host: &'static str,
        port: u16,
        keep_alive: bool,
    ) -> u16 {
        let listener = tokio::net::TcpListener::bind(std::net::SocketAddr::from(([127, 0, 0, 1], 0)))
            .await
            .unwrap();
        let listening_on = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            let client = std::sync::Arc::new(client);
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    return;
                };
                let (client, metrics) = (client.clone(), metrics.clone());
                tokio::spawn(async move {
                    let service = hyper::service::service_fn(move |request: Request<Incoming>| {
                        let (client, metrics) = (client.clone(), metrics.clone());
                        async move {
                            let open = metrics.open(&crate::test_support::app_id());
                            Ok::<_, std::convert::Infallible>(
                                forward(&client, request, host, port, keep_alive, open).await,
                            )
                        }
                    });
                    let _ = hyper::server::conn::http1::Builder::new()
                        .serve_connection(hyper_util::rt::TokioIo::new(stream), service)
                        .with_upgrades()
                        .await;
                });
            }
        });
        listening_on
    }

    fn empty() -> ProxyBody {
        Full::new(Bytes::new()).map_err(|never| match never {}).boxed()
    }

    /// Answers one request on the connection and then reports how the connection ended: the
    /// bytes read next, so `0` is the peer closing it.
    async fn answering_once_then_listening_for_the_close() -> (u16, tokio::sync::oneshot::Receiver<usize>) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind(std::net::SocketAddr::from(([127, 0, 0, 1], 0)))
            .await
            .unwrap();
        let port = listener.local_addr().unwrap().port();
        let (ended, ending) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let Ok((mut stream, _)) = listener.accept().await else {
                return;
            };
            let mut heard = [0u8; 1024];
            let _ = stream.read(&mut heard).await;
            let _ = stream
                .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\n\r\n")
                .await;
            let _ = ended.send(stream.read(&mut heard).await.unwrap_or(0));
        });
        (port, ending)
    }

    // The pool evicts on a timer and nothing else: without one, a connection nobody was using
    // lived until the next request for that guest — or, for a guest left alone, until the guest
    // closed it. While it lived, the guest's keepalive probes and this host's answers kept the
    // app's traffic counters moving, and the app never slept.
    #[tokio::test]
    async fn an_upstream_connection_nothing_is_using_is_closed_without_waiting_for_another_request() {
        let (upstream, ending) = answering_once_then_listening_for_the_close().await;
        let proxy = proxying_with(
            upstream_client_dropping_idle_after(Duration::from_millis(100)),
            "127.0.0.1",
            upstream,
            true,
        )
        .await;

        let answer = asked(proxy, "GET / HTTP/1.0\r\nHost: app-1.example.com\r\n\r\n").await;
        assert_eq!(answer, "", "the one answer came back through the proxy");

        let ended = tokio::time::timeout(Duration::from_secs(5), ending)
            .await
            .expect("the pool closed the idle connection on its own")
            .unwrap();
        assert_eq!(ended, 0, "the upstream saw the connection closed, not reused");
    }

    /// Answers every request with the version it arrived as, and counts the connections it took.
    async fn counting_connections() -> (u16, std::sync::Arc<std::sync::atomic::AtomicUsize>) {
        let listener = tokio::net::TcpListener::bind(std::net::SocketAddr::from(([127, 0, 0, 1], 0)))
            .await
            .unwrap();
        let port = listener.local_addr().unwrap().port();
        let connections = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counted = connections.clone();
        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    return;
                };
                counted.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                tokio::spawn(async move {
                    let service = hyper::service::service_fn(|request: Request<Incoming>| async move {
                        Ok::<Response<ProxyBody>, std::convert::Infallible>(Response::new(
                            Full::new(Bytes::from(format!("{:?}", request.version())))
                                .map_err(|never| match never {})
                                .boxed(),
                        ))
                    });
                    let _ = hyper::server::conn::http1::Builder::new()
                        .serve_connection(hyper_util::rt::TokioIo::new(stream), service)
                        .await;
                });
            }
        });
        (port, connections)
    }

    #[tokio::test]
    async fn requests_that_arrived_over_http1_0_share_one_connection_to_the_upstream() {
        let (upstream, connections) = counting_connections().await;
        let proxy = proxying("127.0.0.1", upstream, true).await;
        for _ in 0..3 {
            let answered = asked(proxy, "GET / HTTP/1.0\r\nHost: app-1.example.com\r\n\r\n").await;
            assert_eq!(
                answered, "HTTP/1.1",
                "the tenant was spoken to as the version it speaks"
            );
        }
        assert_eq!(
            connections.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "every request travelled the connection the first one opened"
        );
    }

    async fn echoing_once_it_is_no_longer_http() -> u16 {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind(std::net::SocketAddr::from(([127, 0, 0, 1], 0)))
            .await
            .unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    return;
                };
                tokio::spawn(async move {
                    let service = hyper::service::service_fn(|mut request: Request<Incoming>| async move {
                        let upgraded = hyper::upgrade::on(&mut request);
                        tokio::spawn(async move {
                            let Ok(upgraded) = upgraded.await else {
                                return;
                            };
                            let mut io = hyper_util::rt::TokioIo::new(upgraded);
                            let mut heard = [0u8; 64];
                            let Ok(read) = io.read(&mut heard).await else {
                                return;
                            };
                            let _ = io.write_all(&heard[..read]).await;
                        });
                        Ok::<_, std::convert::Infallible>(
                            Response::builder()
                                .status(StatusCode::SWITCHING_PROTOCOLS)
                                .header("upgrade", "websocket")
                                .header("connection", "Upgrade")
                                .body(empty())
                                .unwrap(),
                        )
                    });
                    let _ = hyper::server::conn::http1::Builder::new()
                        .serve_connection(hyper_util::rt::TokioIo::new(stream), service)
                        .with_upgrades()
                        .await;
                });
            }
        });
        port
    }

    #[tokio::test]
    async fn a_connection_that_asks_to_stop_being_http_is_carried_through_and_keeps_talking() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let patience = std::time::Duration::from_secs(10);
        let upstream = echoing_once_it_is_no_longer_http().await;
        let proxy = proxying("127.0.0.1", upstream, true).await;
        let mut stream = tokio::net::TcpStream::connect(("127.0.0.1", proxy))
            .await
            .unwrap();
        stream
            .write_all(
                b"GET / HTTP/1.1\r\nHost: app-1.example.com\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n\r\n",
            )
            .await
            .unwrap();

        let mut head = Vec::new();
        let mut byte = [0u8; 1];
        while !head.ends_with(b"\r\n\r\n") {
            let read = tokio::time::timeout(patience, stream.read(&mut byte))
                .await
                .expect("the upgrade was answered")
                .unwrap();
            assert_eq!(read, 1, "the proxy closed the connection instead of upgrading it");
            head.push(byte[0]);
        }
        let head = String::from_utf8(head).unwrap();
        assert!(head.starts_with("HTTP/1.1 101 "), "{head}");
        assert!(head.to_ascii_lowercase().contains("upgrade: websocket"), "{head}");

        stream.write_all(b"ping").await.unwrap();
        let mut echoed = [0u8; 4];
        tokio::time::timeout(patience, stream.read_exact(&mut echoed))
            .await
            .expect("the tenant answered past the proxy")
            .unwrap();
        assert_eq!(
            &echoed, b"ping",
            "the two ends are talking through a proxy that stepped aside"
        );
    }

    #[tokio::test]
    async fn a_websocket_is_open_on_its_app_for_as_long_as_the_two_sides_talk() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let patience = std::time::Duration::from_secs(10);
        let metrics = std::sync::Arc::new(crate::domain::metrics::ProxyMetrics::default());
        let open = || metrics.open_requests_for(&crate::test_support::app_id());
        let upstream = echoing_once_it_is_no_longer_http().await;
        let proxy =
            proxying_counted_by(metrics.clone(), upstream_client(), "127.0.0.1", upstream, true).await;
        let mut stream = tokio::net::TcpStream::connect(("127.0.0.1", proxy))
            .await
            .unwrap();
        stream
            .write_all(
                b"GET / HTTP/1.1\r\nHost: app-1.example.com\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n\r\n",
            )
            .await
            .unwrap();
        let mut head = Vec::new();
        let mut byte = [0u8; 1];
        while !head.ends_with(b"\r\n\r\n") {
            tokio::time::timeout(patience, stream.read(&mut byte))
                .await
                .expect("the upgrade was answered")
                .unwrap();
            head.push(byte[0]);
        }
        assert_eq!(open(), 1, "upgraded, and the request is still open");

        stream.write_all(b"ping").await.unwrap();
        let mut echoed = [0u8; 4];
        tokio::time::timeout(patience, stream.read_exact(&mut echoed))
            .await
            .expect("the tenant answered past the proxy")
            .unwrap();
        assert_eq!(open(), 1, "talking, and the request is still open");

        drop(stream);
        let deadline = std::time::Instant::now() + patience;
        while open() > 0 {
            assert!(
                std::time::Instant::now() < deadline,
                "still open after the visitor hung up"
            );
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    }

    #[test]
    fn only_a_request_that_asks_for_an_upgrade_takes_the_leg_that_gives_it_one() {
        let asking = Request::builder()
            .header("connection", "keep-alive, Upgrade")
            .header("upgrade", "websocket")
            .body(())
            .unwrap();
        assert!(upgrade_requested(&asking));

        let ordinary = Request::builder()
            .header("connection", "keep-alive")
            .body(())
            .unwrap();
        assert!(!upgrade_requested(&ordinary));

        let named_but_not_asked_for = Request::builder()
            .header("upgrade", "websocket")
            .body(())
            .unwrap();
        assert!(
            !upgrade_requested(&named_but_not_asked_for),
            "an upgrade nothing in the connection header asks for is not one"
        );
    }

    #[tokio::test]
    async fn an_upstream_that_is_not_listening_is_a_bad_gateway_rather_than_a_hang() {
        let listener = tokio::net::TcpListener::bind(std::net::SocketAddr::from(([127, 0, 0, 1], 0)))
            .await
            .unwrap();
        let nobody = listener.local_addr().unwrap().port();
        drop(listener);
        let proxy = proxying("127.0.0.1", nobody, true).await;
        let answered = asked(proxy, "GET / HTTP/1.0\r\nHost: app-1.example.com\r\n\r\n").await;
        assert_eq!(answered, "This app could not be reached.\n");
    }

    #[tokio::test]
    async fn a_request_reaches_the_upstream_with_its_path_and_without_the_headers_that_do_not_travel() {
        let upstream = reading_hostnames().await;
        let proxy = proxying("127.0.0.1", upstream, true).await;
        let answered = asked(
            proxy,
            "GET /a?b=1 HTTP/1.0\r\nHost: app-1.example.com\r\nupgrade: websocket\r\n\r\n",
        )
        .await;
        assert_eq!(
            answered, "app-1.example.com",
            "the host the client named travels, the hop-by-hop header does not"
        );
    }
}
