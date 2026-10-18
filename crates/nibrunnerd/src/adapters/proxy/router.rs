use std::collections::{BTreeMap, BTreeSet};
use std::net::{IpAddr, SocketAddr};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::{TokioExecutor, TokioIo, TokioTimer};
use hyper_util::server::conn::auto;
use protocol::{AppId, DeploymentId, HostPort, InstanceLabels};
use tokio::net::{TcpListener, TcpStream};

use crate::config::{Compression, HttpCache, HttpEncode};
use crate::domain::metrics::{CacheOutcome, HostMetrics, Outcome};
use tokio::sync::RwLock;

use crate::adapters::proxy::cache::{self, Asked, Origin, ResponseCache};
use crate::adapters::proxy::encode::{self, Encoding, Metered};
use crate::adapters::proxy::forward::{
    forward, hostname_of, note_the_hop, say, ProxyBody, Unreachable, Upstreams,
};
use crate::adapters::proxy::http_log::{HttpRequestLog, HttpRequestRecord, HttpRequestScope};
use crate::adapters::proxy::pem;
use crate::domain::metrics::proxy::Handshake;
use crate::domain::report::routes::RouteTarget;

const LOOPBACK: &str = "127.0.0.1";

// Nothing otherwise bounds how long a caller may take to finish a handshake or send a request
// line, and these listeners are open to the world: a connection opened and left unfinished is
// held for as long as its opener likes. The header alone, and the same ten seconds nibrun's edge
// gives, because a tenant's slow upload and its streaming response are the tenant's to take as
// long over as their own users need.
const GREETING_TIMEOUT: Duration = Duration::from_secs(10);

// The app behind a hostname, not just the port: a request is measured against the tenant it was
// for, and the port alone cannot say which that is.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Route {
    pub app_id: AppId,
    pub deployment_id: DeploymentId,
    pub host_port: HostPort,
    pub labels: InstanceLabels,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RouteTable {
    by_hostname: BTreeMap<String, Route>,
}

impl RouteTable {
    pub fn from_targets(targets: &[RouteTarget]) -> Self {
        Self {
            by_hostname: targets
                .iter()
                .flat_map(|target| {
                    target.hostnames.iter().map(|entry| {
                        (
                            entry.hostname.as_str().to_ascii_lowercase(),
                            Route {
                                app_id: target.app_id.clone(),
                                deployment_id: target.deployment_id.clone(),
                                host_port: target.host_port,
                                labels: target.labels.clone(),
                            },
                        )
                    })
                })
                .collect(),
        }
    }

    pub fn route_for(&self, hostname: &str) -> Option<Route> {
        self.by_hostname.get(hostname).cloned()
    }

    pub fn port_for(&self, hostname: &str) -> Option<HostPort> {
        self.by_hostname.get(hostname).map(|route| route.host_port)
    }

    pub fn hostnames(&self) -> Vec<&str> {
        self.by_hostname.keys().map(String::as_str).collect()
    }

    pub fn apps(&self) -> BTreeSet<AppId> {
        self.by_hostname
            .values()
            .map(|route| route.app_id.clone())
            .collect()
    }

    pub fn is_empty(&self) -> bool {
        self.by_hostname.is_empty()
    }
}

// What a connection was before any request arrived on it: the name its handshake asked for, the
// address it came from, and whether it was encrypted. A tenant is dialled over loopback and can
// read none of it for itself.
#[derive(Debug, Clone)]
pub struct Arrival {
    pub server_name: Option<Arc<str>>,
    pub peer: IpAddr,
    pub secure: bool,
}

pub struct Router {
    routes: RwLock<Arc<RouteTable>>,
    upstreams: Upstreams,
    metrics: Arc<HostMetrics>,
    http_log: Option<Arc<HttpRequestLog>>,
    concurrency: Option<super::concurrency::ConcurrencyLimiter>,
    state: crate::state::SharedState,
    cache: Option<Arc<ResponseCache>>,
    encode: Option<HttpEncode>,
}

/// On the 503 the proxy writes itself when the host or the app has no capacity left.
#[derive(Debug, Clone, Copy)]
struct Busy;

/// On a response the activator held for a wake, how long it held it.
#[derive(Debug, Clone, Copy)]
struct Woken(u64);

fn busy() -> Response<ProxyBody> {
    let mut response = say(
        StatusCode::SERVICE_UNAVAILABLE,
        "This host or app is busy. Please retry.\n",
    );
    response
        .headers_mut()
        .insert("retry-after", hyper::header::HeaderValue::from_static("1"));
    response.extensions_mut().insert(Busy);
    response
}

/// The app behind one route, as the response cache asks it.
struct TheApp {
    router: Arc<Router>,
    route: Route,
}

impl Origin for TheApp {
    fn ask(&self, request: Request<ProxyBody>) -> futures::future::BoxFuture<'static, Response<ProxyBody>> {
        let (router, route) = (self.router.clone(), self.route.clone());
        Box::pin(async move { router.ask_the_app(&route, request).await })
    }

    fn recode(&self, response: Response<ProxyBody>, encoding: Encoding) -> Response<ProxyBody> {
        let (metrics, app_id) = (self.router.metrics.clone(), self.route.app_id.clone());
        let meter = Metered::reporting(move |taken, given| {
            let compression = match encoding {
                Encoding::Br => Compression::Br,
                Encoding::Gzip => Compression::Gzip,
                Encoding::Identity => return,
            };
            metrics.proxy.encoded(&app_id, compression, taken, given);
        });
        encode::recoded(response, encoding, self.router.encode.as_ref(), meter)
    }
}

impl Router {
    pub fn new(metrics: Arc<HostMetrics>, http_log: Option<Arc<HttpRequestLog>>) -> Arc<Self> {
        Self::with_concurrency(metrics, http_log, None)
    }

    pub fn with_concurrency(
        metrics: Arc<HostMetrics>,
        http_log: Option<Arc<HttpRequestLog>>,
        limits: Option<crate::config::HttpConcurrency>,
    ) -> Arc<Self> {
        Self::with_state(metrics, http_log, limits, crate::state::HostState::shared())
    }

    /// A router that refuses an app whose deployment the host's records say has expired, and
    /// counts every request it carries as the app's activity under the same lock that expires it.
    pub(crate) fn with_state(
        metrics: Arc<HostMetrics>,
        http_log: Option<Arc<HttpRequestLog>>,
        limits: Option<crate::config::HttpConcurrency>,
        state: crate::state::SharedState,
    ) -> Arc<Self> {
        Self::with_http(metrics, http_log, limits, None, None, state)
    }

    /// The router `[proxy.http]` describes: admission, the response cache, compression, each
    /// absent when its section is.
    pub fn with_http(
        metrics: Arc<HostMetrics>,
        http_log: Option<Arc<HttpRequestLog>>,
        limits: Option<crate::config::HttpConcurrency>,
        cache: Option<&HttpCache>,
        encode: Option<HttpEncode>,
        state: crate::state::SharedState,
    ) -> Arc<Self> {
        let cache = cache
            .map(|settings| ResponseCache::new(settings, ResponseCache::system_clock(), metrics.clone()));
        Arc::new(Self {
            state,
            concurrency: Some(super::concurrency::ConcurrencyLimiter::new(limits)),
            routes: RwLock::new(Arc::new(RouteTable::default())),
            upstreams: Upstreams::default(),
            metrics,
            http_log,
            cache,
            encode,
        })
    }

    pub async fn discard_http_log(&self, app_id: &AppId) {
        Arc::make_mut(&mut *self.routes.write().await)
            .by_hostname
            .retain(|_, route| &route.app_id != app_id);
        if let Some(http_log) = &self.http_log {
            http_log.discard(app_id).await;
        }
    }

    /// Each instance's `limits.concurrent`, taken by the next request routed to it.
    pub fn set_instance_concurrency(&self, limits: BTreeMap<AppId, std::num::NonZeroU16>) {
        if let Some(concurrency) = &self.concurrency {
            concurrency.set_instance_limits(limits);
        }
    }

    pub async fn apply(&self, table: RouteTable) {
        self.upstreams.keep_only(&table.apps()).await;
        if let Some(concurrency) = &self.concurrency {
            concurrency.keep_only(&table.apps());
        }
        *self.routes.write().await = Arc::new(table);
    }

    pub async fn close_connections_to(&self, app_id: &AppId) {
        self.upstreams.close_connections_to(app_id).await;
    }

    pub async fn routes(&self) -> Arc<RouteTable> {
        self.routes.read().await.clone()
    }

    pub async fn handle(
        self: Arc<Self>,
        request: Request<Incoming>,
        arrival: Arrival,
    ) -> Response<ProxyBody> {
        let http2 = request.version() == hyper::Version::HTTP_2;
        let started = std::time::Instant::now();
        let metrics = self.metrics.clone();
        metrics.proxy.began();
        let (mut response, outcome, route) = self.route(request, arrival).await;
        let elapsed = started.elapsed();
        metrics.proxy.answered(
            outcome,
            response.status(),
            elapsed,
            route.as_ref().map(|route| &route.app_id),
        );
        metrics.proxy.ended();
        if http2 {
            // Illegal over HTTP/2, and hyper logs a warning for each one it has to strip.
            response.headers_mut().remove(hyper::header::CONNECTION);
        }
        response
    }

    async fn route(
        self: Arc<Self>,
        mut request: Request<Incoming>,
        arrival: Arrival,
    ) -> (Response<ProxyBody>, Outcome, Option<Route>) {
        let started = std::time::Instant::now();
        let Some(hostname) = hostname_of(&request) else {
            return (
                say(StatusCode::BAD_REQUEST, "This request names no host.\n"),
                Outcome::Refused,
                None,
            );
        };
        // One certificate covers every app on this host, so the name in the handshake is the only
        // thing stopping a connection opened for one tenant from asking for another tenant's app.
        if arrival
            .server_name
            .as_deref()
            .is_some_and(|name| !name.eq_ignore_ascii_case(&hostname))
        {
            return (
                say(
                    StatusCode::MISDIRECTED_REQUEST,
                    "This connection was opened for a different host.\n",
                ),
                Outcome::WrongHost,
                None,
            );
        }
        let (route, logging) = {
            let routes = self.routes.read().await;
            let Some(route) = routes.route_for(&hostname) else {
                return (
                    say(
                        StatusCode::NOT_FOUND,
                        "No app on this host answers for that hostname.\n",
                    ),
                    Outcome::NoSuchHost,
                    None,
                );
            };
            let logging = if let Some(log) = &self.http_log {
                Some((
                    log.begin(&route.app_id).await,
                    request.method().as_str().to_owned(),
                    request.uri().clone(),
                ))
            } else {
                None
            };
            (route, logging)
        };
        if self
            .state
            .record(&route.app_id)
            .await
            .is_some_and(|record| record.expired_at_ms.is_some())
        {
            return self.logged(expired(), Outcome::Refused, route, logging, started, None, None);
        }
        note_the_hop(request.headers_mut(), arrival.peer, arrival.secure, &hostname);
        let (response, cache, encoding) = self.exchange(request, &route, &hostname).await;
        let outcome = if response.extensions().get::<Busy>().is_some() {
            self.metrics.proxy.app_refused(&route.app_id);
            Outcome::Refused
        } else if response.extensions().get::<Expired>().is_some() {
            Outcome::Refused
        } else {
            let reached = response.extensions().get::<Unreachable>().is_none();
            self.metrics
                .proxy
                .app_answered(&route.app_id, response.status(), reached);
            if reached {
                Outcome::Served
            } else {
                Outcome::Unreachable
            }
        };
        if let Some(cache) = cache {
            self.metrics.proxy.cached(&route.app_id, cache);
        }
        self.logged(response, outcome, route, logging, started, cache, encoding)
    }

    #[allow(clippy::too_many_arguments)]
    fn logged(
        &self,
        response: Response<ProxyBody>,
        outcome: Outcome,
        route: Route,
        logging: Option<(HttpRequestScope, String, hyper::Uri)>,
        started: std::time::Instant,
        cache: Option<CacheOutcome>,
        encoding: Option<Encoding>,
    ) -> (Response<ProxyBody>, Outcome, Option<Route>) {
        let wake_micros = response.extensions().get::<Woken>().map(|woken| woken.0);
        if let (Some(log), Some((scope, method, uri))) = (&self.http_log, logging) {
            log.record(
                scope,
                HttpRequestRecord::new(
                    &method,
                    &uri,
                    response.status().as_u16(),
                    u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX),
                    route.deployment_id.clone(),
                )
                .woken_for(wake_micros)
                .served_as(cache, encoding)
                .labelled(route.labels.clone()),
            );
        }
        (response, outcome, Some(route))
    }

    /// The request answered: from the response cache before the app is asked, which is what lets
    /// a hit be served while the app sleeps without waking it; from the app otherwise, compressed
    /// on the way out. A host with neither section relays the app's answer as it came.
    async fn exchange(
        self: &Arc<Self>,
        mut request: Request<Incoming>,
        route: &Route,
        hostname: &str,
    ) -> (Response<ProxyBody>, Option<CacheOutcome>, Option<Encoding>) {
        if self.cache.is_none() && self.encode.is_none() {
            return (self.ask_the_app(route, request).await, None, None);
        }
        let encoding = self.encode.as_ref().map_or(Encoding::Identity, |settings| {
            encode::negotiate(request.headers(), &settings.encodings)
        });
        encode::as_negotiated(request.headers_mut(), encoding);
        let origin: Arc<dyn Origin> = Arc::new(TheApp {
            router: self.clone(),
            route: route.clone(),
        });
        let (response, outcome) = match &self.cache {
            Some(cache) if cache::answerable(&request) => {
                let (visitor, _) = request.into_parts();
                let asked = Asked::new(visitor, route.deployment_id.clone(), hostname, encoding);
                let answered = cache.answer(asked, origin).await;
                (answered.response, Some(answered.outcome))
            }
            cached => {
                let invalidates = cache::invalidating(request.method());
                let target = request
                    .uri()
                    .path_and_query()
                    .map_or("/", |target| target.as_str())
                    .to_string();
                let response = origin.recode(self.ask_the_app(route, request).await, encoding);
                if let (Some(cache), true) = (cached, invalidates) {
                    cache
                        .invalidate(&route.deployment_id, hostname, &target, &response)
                        .await;
                }
                (response, cached.as_ref().map(|_| CacheOutcome::Bypass))
            }
        };
        let sent = Encoding::of(response.headers());
        (response, outcome, sent)
    }

    async fn ask_the_app<B>(&self, route: &Route, request: Request<B>) -> Response<ProxyBody>
    where
        B: hyper::body::Body<Data = bytes::Bytes, Error = hyper::Error> + Send + Sync + Unpin + 'static,
    {
        let Ok(permit) = self
            .concurrency
            .as_ref()
            .map(|limits| limits.acquire(&route.app_id))
            .transpose()
        else {
            return busy();
        };
        // Admitted under the lock that expires the app, so a request is either counted as its
        // activity or refused, never both.
        let admitted = self
            .state
            .admit(&route.app_id, crate::clock::now_ms(), || {
                super::forward::ForwardedRequest::new(self.metrics.proxy.open(&route.app_id), permit)
            })
            .await;
        let Some(open) = admitted else {
            return expired();
        };
        let upstream = self.upstreams.to(&route.app_id).await;
        let mut response = forward(&upstream, request, LOOPBACK, route.host_port.get(), true, open).await;
        if let Some(micros) = super::http_log::take_wake_micros(response.headers_mut()) {
            response.extensions_mut().insert(Woken(micros));
        }
        response
    }
}

/// On the 410 the proxy writes itself for a revision that has expired.
#[derive(Debug, Clone, Copy)]
struct Expired;

fn expired() -> Response<ProxyBody> {
    let mut response = say(StatusCode::GONE, "This revision has expired.\n");
    response.extensions_mut().insert(Expired);
    response
}

// A reply leaves this proxy as more than one write — the headers, then the body, and under TLS the
// records carrying them. With Nagle still on, the second write waits for an ACK the other end has
// already decided to delay, so a visitor pays about 40ms for an answer the tenant produced in two.
// The connector to the tenant is already nodelay; the leg the visitor is on was not.
fn without_nagle(stream: TcpStream) -> TcpStream {
    let _ = stream.set_nodelay(true);
    stream
}

pub async fn serve_http(router: Arc<Router>, address: SocketAddr) -> std::io::Result<()> {
    let listener = TcpListener::bind(address).await?;
    tracing::info!(%address, "the proxy is listening");
    loop {
        let Ok((stream, peer)) = listener.accept().await else {
            continue;
        };
        let stream = without_nagle(stream);
        let router = router.clone();
        tokio::spawn(async move {
            let arrival = Arrival {
                server_name: None,
                peer: peer.ip(),
                secure: false,
            };
            let service = service_fn(move |request| {
                let (router, arrival) = (router.clone(), arrival.clone());
                async move { Ok::<_, std::convert::Infallible>(router.handle(request, arrival).await) }
            });
            let _ = http1::Builder::new()
                .timer(TokioTimer::new())
                .header_read_timeout(GREETING_TIMEOUT)
                .serve_connection(TokioIo::new(stream), service)
                .with_upgrades()
                .await;
        });
    }
}

/// Where a plain request to `host` and `target` belongs: the same host and path over TLS, on the
/// TLS port unless that is the default one. Refused for a host that could put a second header in
/// the answer or send a visitor to another machine.
pub fn secure_location(host: &str, target: &str, secure_port: u16) -> Option<String> {
    let name = host.split(':').next().unwrap_or(host).to_ascii_lowercase();
    let plain = !name.is_empty()
        && name.len() <= 253
        && name.split('.').all(|label| {
            !label.is_empty()
                && label.len() <= 63
                && label
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
        });
    let path = target.starts_with('/') && target.bytes().all(|b| b.is_ascii_graphic());
    (plain && path).then(|| match secure_port {
        443 => format!("https://{name}{target}"),
        port => format!("https://{name}:{port}{target}"),
    })
}

/// The plain port of a TLS host: it answers every request with a redirect to the encrypted one and
/// forwards nothing, so an app is still never served unencrypted.
pub async fn serve_redirect(address: SocketAddr, secure_port: u16) -> std::io::Result<()> {
    let listener = TcpListener::bind(address).await?;
    tracing::info!(%address, secure_port, "the proxy is redirecting plain HTTP to TLS");
    loop {
        let Ok((stream, _)) = listener.accept().await else {
            continue;
        };
        let stream = without_nagle(stream);
        tokio::spawn(async move {
            let service = service_fn(move |request: Request<Incoming>| async move {
                let target = request.uri().path_and_query().map_or("/", |p| p.as_str());
                let response = match hostname_of(&request)
                    .and_then(|host| secure_location(&host, target, secure_port))
                {
                    Some(location) => {
                        let mut redirect = say(StatusCode::MOVED_PERMANENTLY, "");
                        if let Ok(value) = hyper::header::HeaderValue::from_str(&location) {
                            redirect.headers_mut().insert(hyper::header::LOCATION, value);
                        }
                        redirect
                    }
                    None => say(
                        StatusCode::BAD_REQUEST,
                        "This request names no host to redirect.\n",
                    ),
                };
                Ok::<_, std::convert::Infallible>(response)
            });
            let _ = http1::Builder::new()
                .timer(TokioTimer::new())
                .header_read_timeout(GREETING_TIMEOUT)
                .serve_connection(TokioIo::new(stream), service)
                .await;
        });
    }
}

pub async fn serve_https(
    router: Arc<Router>,
    address: SocketAddr,
    acceptor: tokio_rustls::TlsAcceptor,
    client_ca: Option<&Path>,
) -> std::io::Result<()> {
    let listener = TcpListener::bind(address).await?;
    match client_ca {
        Some(pool) => tracing::info!(
            %address,
            trust_pool = %pool.display(),
            "the proxy is listening for TLS, and refuses a caller that presents no certificate of its own"
        ),
        None => tracing::info!(
            %address,
            "the proxy is listening for TLS, and serves whoever reaches this address"
        ),
    }
    loop {
        let Ok((stream, peer)) = listener.accept().await else {
            continue;
        };
        let stream = without_nagle(stream);
        let router = router.clone();
        let acceptor = acceptor.clone();
        tokio::spawn(async move {
            let stream = match tokio::time::timeout(GREETING_TIMEOUT, acceptor.accept(stream)).await {
                Ok(Ok(stream)) => stream,
                Ok(Err(_)) => {
                    router.metrics.proxy.handshake(Handshake::Failed);
                    return;
                }
                Err(_) => {
                    router.metrics.proxy.handshake(Handshake::TimedOut);
                    return;
                }
            };
            router.metrics.proxy.handshake(Handshake::Completed);
            let arrival = Arrival {
                server_name: stream.get_ref().1.server_name().map(Arc::from),
                peer: peer.ip(),
                secure: true,
            };
            let service = service_fn(move |request| {
                let (router, arrival) = (router.clone(), arrival.clone());
                async move { Ok::<_, std::convert::Infallible>(router.handle(request, arrival).await) }
            });
            let _ = auto::Builder::new(TokioExecutor::new())
                .http1()
                .timer(TokioTimer::new())
                .header_read_timeout(GREETING_TIMEOUT)
                .serve_connection_with_upgrades(TokioIo::new(stream), service)
                .await;
        });
    }
}

fn invalid(error: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, error)
}

// A trust pool that admits fewer than its file holds is refused here rather than at the handshakes
// it would refuse: the file is named to say who may call, and one that is short, empty or not
// X.509 says something else.
fn client_verifier(client_ca: &Path) -> std::io::Result<Arc<dyn rustls::server::danger::ClientCertVerifier>> {
    let anchors = pem::read_certificates(client_ca)?;
    let mut roots = rustls::RootCertStore::empty();
    let (_, unparsable) = roots.add_parsable_certificates(anchors);
    if unparsable > 0 {
        return Err(invalid(format!(
            "{} holds {unparsable} certificate(s) that could not be parsed as X.509",
            client_ca.display()
        )));
    }
    tracing::info!(certificates = roots.len(), pool = %client_ca.display(), "client CA pool loaded");
    rustls::server::WebPkiClientVerifier::builder(Arc::new(roots))
        .build()
        .map_err(invalid)
}

pub fn tls_acceptor(
    certificate: &Path,
    key: &Path,
    client_ca: Option<&Path>,
) -> std::io::Result<tokio_rustls::TlsAcceptor> {
    let resolver = Arc::new(super::certificate::ReloadingCertificate::open(certificate, key)?);
    let builder = rustls::ServerConfig::builder();
    let builder = match client_ca {
        Some(client_ca) => builder.with_client_cert_verifier(client_verifier(client_ca)?),
        None => builder.with_no_client_auth(),
    };
    let mut config = builder.with_cert_resolver(resolver);
    config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
    Ok(tokio_rustls::TlsAcceptor::from(Arc::new(config)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_visitor_never_waits_on_an_ack_the_other_end_is_delaying() {
        let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
            .await
            .unwrap();
        let address = listener.local_addr().unwrap();
        let accepting = tokio::spawn(async move { listener.accept().await.unwrap().0 });
        let _visitor = TcpStream::connect(address).await.unwrap();

        let accepted = accepting.await.unwrap();
        assert!(
            !accepted.nodelay().unwrap(),
            "the kernel still hands it over with Nagle on"
        );
        assert!(without_nagle(accepted).nodelay().unwrap());
    }
    use crate::domain::report::routes::renderable_routes;
    use crate::test_support::{app_hostname, instance_record};
    use http_body_util::BodyExt;
    use protocol::{AppHostname, AppHostnameKind, Hostname};

    #[test]
    fn a_route_is_rendered_for_every_hostname_an_app_holds() {
        let record = instance_record(|record| {
            record.hostnames = vec![
                app_hostname(),
                AppHostname {
                    hostname: Hostname::parse("Www.Example.Com")
                        .unwrap_or_else(|_| Hostname::parse("www.example.com").unwrap()),
                    kind: AppHostnameKind::Custom,
                },
            ];
        });
        let table = RouteTable::from_targets(&renderable_routes(std::slice::from_ref(&record)));
        assert_eq!(table.port_for(app_hostname().hostname.as_str()), record.host_port);
        assert_eq!(table.port_for("www.example.com"), record.host_port);
        assert_eq!(table.port_for("nobody.example.com"), None);
        assert_eq!(table.hostnames().len(), 2);
    }

    #[test]
    fn the_table_is_identical_whether_the_app_is_up_or_down() {
        let up = RouteTable::from_targets(&renderable_routes(&[instance_record(|_| {})]));
        let down = RouteTable::from_targets(&renderable_routes(&[instance_record(|record| {
            record.state = protocol::InstanceState::Stopped;
        })]));
        assert_eq!(up, down);
        assert!(RouteTable::default().is_empty());
    }

    #[tokio::test]
    async fn a_router_that_has_been_told_nothing_answers_for_nothing() {
        let router = Router::new(std::sync::Arc::new(HostMetrics::new()), None);
        assert!(router.routes().await.is_empty());
        assert!(router.routes().await.hostnames().is_empty());
        assert_eq!(router.routes().await.port_for("app-1.example.com"), None);
    }

    #[tokio::test]
    async fn a_new_table_replaces_the_old_one_rather_than_being_added_to_it() {
        let router = Router::new(std::sync::Arc::new(HostMetrics::new()), None);
        router
            .apply(RouteTable::from_targets(&renderable_routes(&[instance_record(
                |_| {},
            )])))
            .await;
        assert_eq!(router.routes().await.hostnames(), vec!["app-1.apps.example.com"]);

        router.apply(RouteTable::default()).await;
        assert!(router.routes().await.is_empty());
    }

    fn arriving(server_name: Option<&'static str>, secure: bool) -> Arrival {
        Arrival {
            server_name: server_name.map(Arc::from),
            peer: IpAddr::from([203, 0, 113, 7]),
            secure,
        }
    }

    async fn serving(router: Arc<Router>) -> u16 {
        serving_as(router, arriving(None, false)).await
    }

    async fn serving_as(router: Arc<Router>, arrival: Arrival) -> u16 {
        let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
            .await
            .unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    return;
                };
                let (router, arrival) = (router.clone(), arrival.clone());
                tokio::spawn(async move {
                    let service = service_fn(move |request| {
                        let (router, arrival) = (router.clone(), arrival.clone());
                        async move { Ok::<_, std::convert::Infallible>(router.handle(request, arrival).await) }
                    });
                    let _ = http1::Builder::new()
                        .serve_connection(TokioIo::new(stream), service)
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
        answer
    }

    #[tokio::test]
    async fn a_request_that_names_no_host_is_refused_before_any_table_is_read() {
        let port = serving(Router::new(std::sync::Arc::new(HostMetrics::new()), None)).await;
        let answered = asked(port, "GET / HTTP/1.0\r\n\r\n").await;
        assert!(answered.starts_with("HTTP/1.0 400 "), "{answered}");
        assert!(answered.ends_with("This request names no host.\n"), "{answered}");
    }

    #[tokio::test]
    async fn a_hostname_no_app_on_this_host_holds_is_a_not_found_rather_than_a_gateway_failure() {
        let router = Router::new(std::sync::Arc::new(HostMetrics::new()), None);
        router
            .apply(RouteTable::from_targets(&renderable_routes(&[instance_record(
                |_| {},
            )])))
            .await;
        let port = serving(router).await;
        let answered = asked(port, "GET / HTTP/1.0\r\nHost: nobody.example.com\r\n\r\n").await;
        assert!(answered.starts_with("HTTP/1.0 404 "), "{answered}");
        assert!(
            answered.ends_with("No app on this host answers for that hostname.\n"),
            "{answered}"
        );
    }

    #[tokio::test]
    async fn a_hostname_that_is_routed_is_carried_to_the_app_it_names() {
        let upstream = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
            .await
            .unwrap();
        let host_port = HostPort::new(upstream.local_addr().unwrap().port()).unwrap();
        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = upstream.accept().await else {
                    return;
                };
                tokio::spawn(async move {
                    let service = service_fn(|_request: Request<Incoming>| async move {
                        Ok::<_, std::convert::Infallible>(say(StatusCode::OK, "served by the tenant\n"))
                    });
                    let _ = http1::Builder::new()
                        .serve_connection(TokioIo::new(stream), service)
                        .await;
                });
            }
        });

        let router = Router::new(std::sync::Arc::new(HostMetrics::new()), None);
        router
            .apply(RouteTable::from_targets(&renderable_routes(&[instance_record(
                |record| record.host_port = Some(host_port),
            )])))
            .await;
        let port = serving(router).await;
        let answered = asked(port, "GET / HTTP/1.0\r\nHost: App-1.Apps.Example.Com\r\n\r\n").await;
        assert!(answered.starts_with("HTTP/1.0 200 "), "{answered}");
        assert!(answered.ends_with("served by the tenant\n"), "{answered}");
    }

    #[tokio::test]
    async fn a_routed_request_writes_an_http_record_without_its_query() {
        let directory = tempfile::tempdir().expect("an HTTP request log directory");
        let http_log =
            Arc::new(HttpRequestLog::new(directory.path().to_path_buf()).expect("HTTP request logs"));
        let router = Router::new(Arc::new(HostMetrics::new()), Some(http_log));
        routed_at(&router, nobody_listening().await).await;
        let port = serving(router).await;
        let answered = asked(
            port,
            "GET /catalog?token=private HTTP/1.0\r\nHost: app-1.apps.example.com\r\n\r\n",
        )
        .await;
        assert!(answered.starts_with("HTTP/1.0 502 "), "{answered}");

        let path = directory.path().join("app-1.http.jsonl");
        let record = tokio::time::timeout(std::time::Duration::from_secs(1), async {
            loop {
                if let Ok(content) = std::fs::read_to_string(&path) {
                    if !content.is_empty() {
                        break content;
                    }
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the HTTP log writer flushes the routed request");
        assert!(record.contains("\"path\":\"/catalog\""), "{record}");
        assert!(record.contains("\"status\":502"), "{record}");
        assert!(record.contains("\"deploymentId\":\"dep-1\""), "{record}");
        assert!(!record.contains("private"), "{record}");
    }

    #[tokio::test]
    async fn a_request_the_activator_held_for_a_wake_is_logged_with_it_and_the_client_never_sees_how() {
        let upstream = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
            .await
            .unwrap();
        let host_port = HostPort::new(upstream.local_addr().unwrap().port()).unwrap();
        tokio::spawn(async move {
            let Ok((stream, _)) = upstream.accept().await else {
                return;
            };
            let service = service_fn(|_request: Request<Incoming>| async move {
                let mut woken = say(StatusCode::OK, "woken\n");
                woken.headers_mut().insert(
                    super::super::http_log::wake_header(),
                    hyper::header::HeaderValue::from(240_000u64),
                );
                Ok::<_, std::convert::Infallible>(woken)
            });
            let _ = http1::Builder::new()
                .serve_connection(TokioIo::new(stream), service)
                .await;
        });
        let directory = tempfile::tempdir().expect("an HTTP request log directory");
        let http_log =
            Arc::new(HttpRequestLog::new(directory.path().to_path_buf()).expect("HTTP request logs"));
        let router = Router::new(Arc::new(HostMetrics::new()), Some(http_log));
        routed_at(&router, host_port).await;
        let port = serving(router).await;

        let answered = asked(port, "GET / HTTP/1.0\r\nHost: app-1.apps.example.com\r\n\r\n").await;

        assert!(answered.starts_with("HTTP/1.0 200 "), "{answered}");
        assert!(!answered.contains("x-nibrunner-wake"), "{answered}");
        let path = directory.path().join("app-1.http.jsonl");
        let record = tokio::time::timeout(std::time::Duration::from_secs(1), async {
            loop {
                if let Ok(content) = std::fs::read_to_string(&path) {
                    if !content.is_empty() {
                        break content;
                    }
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the HTTP log writer flushes the routed request");
        assert!(record.contains("\"wakeMicros\":240000"), "{record}");
    }

    /// A tenant that answers every request and counts the connections it was asked over. Its
    /// answers are keep-alive, unlike `say`'s, so that a second request may travel the first's
    /// connection and a count above one means the proxy opened another.
    async fn counting_its_connections() -> (HostPort, Arc<std::sync::atomic::AtomicUsize>) {
        let upstream = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
            .await
            .unwrap();
        let host_port = HostPort::new(upstream.local_addr().unwrap().port()).unwrap();
        let connections = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counted = connections.clone();
        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = upstream.accept().await else {
                    return;
                };
                counted.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                tokio::spawn(async move {
                    let service = service_fn(|_request: Request<Incoming>| async move {
                        Ok::<Response<ProxyBody>, std::convert::Infallible>(Response::new(
                            http_body_util::Full::new(bytes::Bytes::from_static(b"served\n"))
                                .map_err(|never| match never {})
                                .boxed(),
                        ))
                    });
                    let _ = http1::Builder::new()
                        .serve_connection(TokioIo::new(stream), service)
                        .await;
                });
            }
        });
        (host_port, connections)
    }

    fn opened(connections: &Arc<std::sync::atomic::AtomicUsize>) -> usize {
        connections.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// The pool takes a connection back on a task of its own once the answer has been relayed
    /// whole, which is not always before the caller has finished reading it.
    async fn once_the_pool_has_it_back() {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    async fn asked_for(proxy: u16, hostname: &str) -> String {
        asked(proxy, &format!("GET / HTTP/1.0\r\nHost: {hostname}\r\n\r\n")).await
    }

    fn app_at(name: &str, host_port: HostPort) -> crate::domain::report::InstanceRecord {
        instance_record(|record| {
            record.app_id = AppId::parse(name).unwrap();
            record.host_port = Some(host_port);
            record.hostnames = vec![AppHostname {
                hostname: Hostname::parse(format!("{name}.apps.example.com")).unwrap(),
                kind: AppHostnameKind::Platform,
            }];
        })
    }

    // A request that reused a connection opened before the app was put down was carried into the
    // guest as it was being paused: the activator never saw it, so nothing woke the app, and the
    // caller waited out its own timeout for an answer only a restore could give.
    #[tokio::test]
    async fn a_request_for_an_app_that_was_put_down_opens_a_connection_of_its_own() {
        let (host_port, connections) = counting_its_connections().await;
        let router = Router::new(Arc::new(HostMetrics::new()), None);
        routed_at(&router, host_port).await;
        let proxy = serving(router.clone()).await;

        for _ in 0..2 {
            assert!(asked_for(proxy, "app-1.apps.example.com")
                .await
                .ends_with("served\n"));
            once_the_pool_has_it_back().await;
        }
        assert_eq!(
            opened(&connections),
            1,
            "the second request travelled the connection the first one opened"
        );

        router.close_connections_to(&crate::test_support::app_id()).await;
        assert!(asked_for(proxy, "app-1.apps.example.com")
            .await
            .ends_with("served\n"));
        assert_eq!(
            opened(&connections),
            2,
            "the request after the app was put down opened a connection the firewall reads afresh"
        );
    }

    #[tokio::test]
    async fn putting_one_app_down_leaves_the_connections_into_another_app_alone() {
        let (mine, into_mine) = counting_its_connections().await;
        let (theirs, into_theirs) = counting_its_connections().await;
        let router = Router::new(Arc::new(HostMetrics::new()), None);
        router
            .apply(RouteTable::from_targets(&renderable_routes(&[
                app_at("app-1", mine),
                app_at("app-2", theirs),
            ])))
            .await;
        let proxy = serving(router.clone()).await;

        for _ in 0..2 {
            asked_for(proxy, "app-1.apps.example.com").await;
            asked_for(proxy, "app-2.apps.example.com").await;
            once_the_pool_has_it_back().await;
        }
        assert_eq!((opened(&into_mine), opened(&into_theirs)), (1, 1));

        router.close_connections_to(&AppId::parse("app-1").unwrap()).await;
        asked_for(proxy, "app-1.apps.example.com").await;
        asked_for(proxy, "app-2.apps.example.com").await;
        assert_eq!(
            (opened(&into_mine), opened(&into_theirs)),
            (2, 1),
            "only the app that was put down was opened a new connection"
        );
    }

    #[tokio::test]
    async fn an_app_this_host_no_longer_answers_for_is_not_one_it_keeps_a_connection_into() {
        let (host_port, connections) = counting_its_connections().await;
        let router = Router::new(Arc::new(HostMetrics::new()), None);
        routed_at(&router, host_port).await;
        let proxy = serving(router.clone()).await;
        asked_for(proxy, "app-1.apps.example.com").await;
        once_the_pool_has_it_back().await;

        router.apply(RouteTable::default()).await;
        routed_at(&router, host_port).await;

        asked_for(proxy, "app-1.apps.example.com").await;
        assert_eq!(
            opened(&connections),
            2,
            "the app left and came back, and nothing was kept from before"
        );
    }

    #[tokio::test]
    async fn forgetting_an_app_removes_its_route_and_rejects_a_late_http_record() {
        let directory = tempfile::tempdir().unwrap();
        let logs = Arc::new(HttpRequestLog::new(directory.path().to_path_buf()).unwrap());
        let router = Router::new(Arc::new(HostMetrics::new()), Some(logs.clone()));
        let upstream = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let host_port = HostPort::new(upstream.local_addr().unwrap().port()).unwrap();
        let (seen, received) = tokio::sync::oneshot::channel();
        let (release, released) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            let (mut stream, _) = upstream.accept().await.unwrap();
            let mut request = [0u8; 1024];
            assert!(stream.read(&mut request).await.unwrap() > 0);
            seen.send(()).unwrap();
            released.await.unwrap();
            stream
                .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\n\r\n")
                .await
                .unwrap();
        });
        routed_at(&router, host_port).await;
        let proxy = serving(router.clone()).await;
        let request = tokio::spawn(asked(
            proxy,
            "GET /forgotten HTTP/1.0\r\nHost: app-1.apps.example.com\r\n\r\n",
        ));
        received.await.unwrap();
        router.discard_http_log(&crate::test_support::app_id()).await;
        assert!(router.routes().await.is_empty());
        let missing = asked(proxy, "GET / HTTP/1.0\r\nHost: app-1.apps.example.com\r\n\r\n").await;
        assert!(missing.starts_with("HTTP/1.0 404 "));
        release.send(()).unwrap();
        assert!(request.await.unwrap().starts_with("HTTP/1.0 200 "));
        // Draining the writer makes a late record observable before checking the directory.
        logs.discard(&AppId::parse("writer-barrier").unwrap()).await;
        assert!(!directory.path().join("app-1.http.jsonl").exists());
    }

    /// Answers one request and then reports how that connection ended: the bytes read next, so
    /// `0` is the peer closing it.
    async fn answering_once_then_listening_for_the_close() -> (HostPort, tokio::sync::oneshot::Receiver<usize>)
    {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let upstream = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
            .await
            .unwrap();
        let host_port = HostPort::new(upstream.local_addr().unwrap().port()).unwrap();
        let (ended, ending) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let Ok((mut stream, _)) = upstream.accept().await else {
                return;
            };
            let mut heard = [0u8; 1024];
            let _ = stream.read(&mut heard).await;
            let _ = stream
                .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\n\r\n")
                .await;
            let _ = ended.send(stream.read(&mut heard).await.unwrap_or(0));
        });
        (host_port, ending)
    }

    #[tokio::test]
    async fn the_connection_into_an_app_that_was_put_down_is_closed_rather_than_left_to_its_timer() {
        let (host_port, ending) = answering_once_then_listening_for_the_close().await;
        let router = Router::new(Arc::new(HostMetrics::new()), None);
        routed_at(&router, host_port).await;
        let proxy = serving(router.clone()).await;
        asked_for(proxy, "app-1.apps.example.com").await;
        once_the_pool_has_it_back().await;

        router.close_connections_to(&crate::test_support::app_id()).await;

        // Well inside the idle timer the pool would have closed it on anyway, so that a closed
        // connection means it was put down with the app rather than left to expire.
        let ended = tokio::time::timeout(Duration::from_secs(2), ending)
            .await
            .expect("the connection outlived the app it led into")
            .unwrap();
        assert_eq!(ended, 0, "the tenant saw the connection closed, not held open");
    }

    /// A tenant that answers every request with `status`, as one that is up but failing would.
    async fn answering_with(status: StatusCode) -> HostPort {
        let upstream = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
            .await
            .unwrap();
        let host_port = HostPort::new(upstream.local_addr().unwrap().port()).unwrap();
        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = upstream.accept().await else {
                    return;
                };
                tokio::spawn(async move {
                    let service = service_fn(move |_request: Request<Incoming>| async move {
                        Ok::<_, std::convert::Infallible>(say(status, "the app's own answer\n"))
                    });
                    let _ = http1::Builder::new()
                        .serve_connection(TokioIo::new(stream), service)
                        .await;
                });
            }
        });
        host_port
    }

    async fn nobody_listening() -> HostPort {
        let held = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
            .await
            .unwrap();
        HostPort::new(held.local_addr().unwrap().port()).unwrap()
    }

    async fn routed_at(router: &Router, host_port: HostPort) {
        router
            .apply(RouteTable::from_targets(&renderable_routes(&[instance_record(
                |record| record.host_port = Some(host_port),
            )])))
            .await;
    }

    fn outcomes(metrics: &HostMetrics) -> Vec<String> {
        crate::domain::metrics::tests::page(
            &crate::domain::metrics::tests::report(),
            metrics,
            &crate::state::HostSnapshot::default(),
            0,
        )
        .lines()
        .filter(|line| line.starts_with("nibrunner_proxy_requests_total{"))
        .map(str::to_string)
        .collect()
    }

    // A 502 is the one status the proxy writes itself, and a tenant may write one too. Only the
    // proxy's own says the app was not there; the tenant's was served, whatever it said.
    #[tokio::test]
    async fn a_502_the_proxy_wrote_itself_is_counted_as_unreachable_and_one_the_app_sent_as_served() {
        let metrics = Arc::new(HostMetrics::new());
        let router = Router::new(metrics.clone(), None);
        let port = serving(router.clone()).await;
        let request = "GET / HTTP/1.0\r\nHost: app-1.apps.example.com\r\n\r\n";

        routed_at(&router, nobody_listening().await).await;
        let answered = asked(port, request).await;
        assert!(
            answered.ends_with("This app could not be reached.\n"),
            "{answered}"
        );
        assert!(
            asked(
                port,
                "GET / HTTP/1.1\r\nHost: app-1.apps.example.com\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n\r\n",
            )
            .await
            .ends_with("This app could not be reached.\n"),
            "the upgrade leg writes the same 502 of its own"
        );

        routed_at(&router, answering_with(StatusCode::BAD_GATEWAY).await).await;
        let answered = asked(port, request).await;
        assert!(answered.starts_with("HTTP/1.0 502 "), "{answered}");
        assert!(answered.ends_with("the app's own answer\n"), "{answered}");

        let counted = outcomes(&metrics);
        assert!(
            counted.contains(&"nibrunner_proxy_requests_total{outcome=\"unreachable\"} 2".to_string()),
            "{counted:?}"
        );
        assert!(
            counted.contains(&"nibrunner_proxy_requests_total{outcome=\"served\"} 1".to_string()),
            "{counted:?}"
        );
        assert_eq!(metrics.proxy.of(&crate::test_support::app_id()).unreachable, 2);
    }

    #[tokio::test]
    async fn a_request_carried_to_an_app_is_timed_under_the_class_of_the_status_it_came_back_with() {
        let metrics = Arc::new(HostMetrics::new());
        let router = Router::new(metrics.clone(), None);
        let port = serving(router.clone()).await;
        let request = "GET / HTTP/1.0\r\nHost: app-1.apps.example.com\r\n\r\n";

        routed_at(&router, answering_with(StatusCode::NOT_FOUND).await).await;
        asked(port, request).await;
        asked(port, request).await;
        routed_at(&router, nobody_listening().await).await;
        asked(port, request).await;

        let page = crate::domain::metrics::tests::page(
            &crate::domain::metrics::tests::report(),
            &metrics,
            &crate::state::HostSnapshot {
                records: std::collections::BTreeMap::from([(
                    crate::test_support::app_id(),
                    instance_record(|_| {}),
                )]),
                ..Default::default()
            },
            0,
        );
        for (class, count) in [("2xx", 0), ("4xx", 2), ("5xx", 1)] {
            let line =
                format!("nibrunner_app_request_seconds_count{{app=\"app-1\",class=\"{class}\"}} {count}\n");
            assert!(page.contains(&line), "{line} in {page}");
        }
    }

    /// A tenant that streams its one answer as the test feeds it: a chunk per message, and the
    /// end of the body once the feed is dropped.
    async fn streaming_as_fed() -> (HostPort, futures::channel::mpsc::UnboundedSender<bytes::Bytes>) {
        use futures::StreamExt;

        let (feed, fed) = futures::channel::mpsc::unbounded::<bytes::Bytes>();
        let fed = Arc::new(std::sync::Mutex::new(Some(fed)));
        let upstream = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
            .await
            .unwrap();
        let host_port = HostPort::new(upstream.local_addr().unwrap().port()).unwrap();
        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = upstream.accept().await else {
                    return;
                };
                let fed = fed.clone();
                tokio::spawn(async move {
                    let service = service_fn(move |_request: Request<Incoming>| {
                        let fed = fed.clone();
                        async move {
                            let fed = fed.lock().unwrap().take().expect("the one answer that streams");
                            let body = http_body_util::StreamBody::new(
                                fed.map(|chunk| Ok::<_, hyper::Error>(hyper::body::Frame::data(chunk))),
                            );
                            Ok::<_, std::convert::Infallible>(Response::new(http_body_util::BodyExt::boxed(
                                body,
                            )))
                        }
                    });
                    let _ = http1::Builder::new()
                        .serve_connection(TokioIo::new(stream), service)
                        .await;
                });
            }
        });
        (host_port, feed)
    }

    fn open_on_the_app(metrics: &HostMetrics) -> u64 {
        metrics.proxy.open_requests_for(&crate::test_support::app_id())
    }

    async fn once_open_on_the_app_reads(metrics: &HostMetrics, expected: u64) {
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while open_on_the_app(metrics) != expected {
            assert!(
                std::time::Instant::now() < deadline,
                "{} open, {expected} expected",
                open_on_the_app(metrics)
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    async fn asking_for_a_stream(port: u16) -> TcpStream {
        use tokio::io::AsyncWriteExt;
        let mut visitor = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
        visitor
            .write_all(b"GET / HTTP/1.0\r\nHost: app-1.apps.example.com\r\n\r\n")
            .await
            .unwrap();
        visitor
    }

    fn limited_router(metrics: Arc<HostMetrics>, host: u16, app: u16) -> Arc<Router> {
        Router::with_concurrency(
            metrics,
            None,
            Some(crate::config::HttpConcurrency {
                host_concurrent: host.try_into().unwrap(),
                app_concurrent: app.try_into().unwrap(),
            }),
        )
    }

    async fn answering_before_the_upload_ends() -> (HostPort, tokio::sync::oneshot::Receiver<Vec<u8>>) {
        use http_body_util::BodyExt;
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let port = HostPort::new(listener.local_addr().unwrap().port()).unwrap();
        let (done, uploaded) = tokio::sync::oneshot::channel();
        let done = Arc::new(std::sync::Mutex::new(Some(done)));
        tokio::spawn(async move {
            loop {
                let (stream, _) = listener.accept().await.unwrap();
                let done = done.clone();
                tokio::spawn(async move {
                    let service = service_fn(move |request: Request<Incoming>| {
                        let done = done.clone();
                        async move {
                            if request.method() == hyper::Method::POST {
                                let done = done.lock().unwrap().take().unwrap();
                                tokio::spawn(async move {
                                    if let Ok(body) = request.into_body().collect().await {
                                        let _ = done.send(body.to_bytes().to_vec());
                                    }
                                });
                            }
                            let body: ProxyBody = http_body_util::Full::new(bytes::Bytes::new())
                                .map_err(|never| match never {})
                                .boxed();
                            Ok::<_, std::convert::Infallible>(Response::new(body))
                        }
                    });
                    http1::Builder::new()
                        .serve_connection(TokioIo::new(stream), service)
                        .await
                        .unwrap();
                });
            }
        });
        (port, uploaded)
    }

    async fn upload_answered_before_it_ended(proxy: u16, upgrade: bool) -> TcpStream {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let mut visitor = TcpStream::connect(("127.0.0.1", proxy)).await.unwrap();
        let upgrade = if upgrade {
            "Connection: Upgrade\r\nUpgrade: websocket\r\n"
        } else {
            ""
        };
        visitor.write_all(format!("POST / HTTP/1.1\r\nHost: app-1.apps.example.com\r\nTransfer-Encoding: chunked\r\n{upgrade}\r\n1\r\nx\r\n").as_bytes()).await.unwrap();
        let mut headers = Vec::new();
        while !headers.ends_with(b"\r\n\r\n") {
            let mut byte = [0u8; 1];
            tokio::time::timeout(Duration::from_secs(5), visitor.read_exact(&mut byte))
                .await
                .unwrap()
                .unwrap();
            headers.push(byte[0]);
        }
        assert!(String::from_utf8(headers).unwrap().starts_with("HTTP/1.1 200 "));
        visitor
    }

    #[tokio::test]
    async fn a_finished_response_keeps_host_and_app_capacity_until_its_upload_ends() {
        use tokio::io::AsyncWriteExt;
        for (host_limit, app_limit, upgrade) in [(1, 1, false), (1, 2, false), (2, 1, false), (1, 1, true)] {
            let metrics = Arc::new(HostMetrics::new());
            let router = limited_router(metrics.clone(), host_limit, app_limit);
            let (port, mut uploaded) = answering_before_the_upload_ends().await;
            routed_at(&router, port).await;
            let proxy = serving(router).await;
            let mut visitor = upload_answered_before_it_ended(proxy, upgrade).await;
            assert!(matches!(
                uploaded.try_recv(),
                Err(tokio::sync::oneshot::error::TryRecvError::Empty)
            ));
            let second = asked(proxy, "GET / HTTP/1.0\r\nHost: app-1.apps.example.com\r\n\r\n").await;
            assert!(second.starts_with("HTTP/1.0 503 "), "{second}");
            assert!(second.to_ascii_lowercase().contains("retry-after: 1"), "{second}");
            let counted = metrics.proxy.of(&crate::test_support::app_id());
            assert_eq!(counted.requests.iter().sum::<u64>(), 2);
            assert_eq!(counted.unreachable, 0);
            visitor.write_all(b"1\r\ny\r\n0\r\n\r\n").await.unwrap();
            assert_eq!(
                tokio::time::timeout(Duration::from_secs(5), uploaded)
                    .await
                    .unwrap()
                    .unwrap(),
                b"xy"
            );
            once_open_on_the_app_reads(&metrics, 0).await;
            let third = asked(proxy, "GET / HTTP/1.0\r\nHost: app-1.apps.example.com\r\n\r\n").await;
            assert!(third.starts_with("HTTP/1.0 200 "), "{third}");
        }
    }

    #[tokio::test]
    async fn an_abandoned_upload_releases_capacity_after_an_early_response() {
        let metrics = Arc::new(HostMetrics::new());
        let router = limited_router(metrics.clone(), 1, 1);
        let (port, uploaded) = answering_before_the_upload_ends().await;
        routed_at(&router, port).await;
        let proxy = serving(router).await;
        let visitor = upload_answered_before_it_ended(proxy, false).await;
        assert_eq!(open_on_the_app(&metrics), 1);
        drop(visitor);
        assert!(tokio::time::timeout(Duration::from_secs(5), uploaded)
            .await
            .unwrap()
            .is_err());
        once_open_on_the_app_reads(&metrics, 0).await;
        let next = asked(proxy, "GET / HTTP/1.0\r\nHost: app-1.apps.example.com\r\n\r\n").await;
        assert!(next.starts_with("HTTP/1.0 200 "), "{next}");
    }

    #[tokio::test]
    async fn a_streaming_response_keeps_capacity_until_its_body_ends() {
        use tokio::io::AsyncReadExt;
        let metrics = Arc::new(HostMetrics::new());
        let router = limited_router(metrics.clone(), 1, 1);
        let (host_port, feed) = streaming_as_fed().await;
        routed_at(&router, host_port).await;
        let proxy = serving(router.clone()).await;
        let mut visitor = asking_for_a_stream(proxy).await;
        once_open_on_the_app_reads(&metrics, 1).await;
        let second = asked(proxy, "GET / HTTP/1.0\r\nHost: app-1.apps.example.com\r\n\r\n").await;
        assert!(second.starts_with("HTTP/1.0 503 "), "{second}");
        drop(feed);
        visitor.read_to_end(&mut Vec::new()).await.unwrap();
        once_open_on_the_app_reads(&metrics, 0).await;
        routed_at(&router, answering_with(StatusCode::OK).await).await;
        let third = asked(proxy, "GET / HTTP/1.0\r\nHost: app-1.apps.example.com\r\n\r\n").await;
        assert!(third.starts_with("HTTP/1.0 200 "), "{third}");
    }

    #[tokio::test]
    async fn a_request_is_open_on_its_app_until_the_last_of_its_answer_has_been_sent() {
        use tokio::io::AsyncReadExt;
        let metrics = Arc::new(HostMetrics::new());
        let router = Router::new(metrics.clone(), None);
        let (host_port, feed) = streaming_as_fed().await;
        routed_at(&router, host_port).await;
        let port = serving(router).await;

        let mut visitor = asking_for_a_stream(port).await;
        once_open_on_the_app_reads(&metrics, 1).await;

        feed.unbounded_send(bytes::Bytes::from_static(b"the first of it"))
            .unwrap();
        let mut heard = Vec::new();
        while !heard.ends_with(b"the first of it") {
            let mut byte = [0u8; 1];
            assert_eq!(
                visitor.read(&mut byte).await.unwrap(),
                1,
                "the answer ended early"
            );
            heard.push(byte[0]);
        }
        assert_eq!(
            open_on_the_app(&metrics),
            1,
            "the headers and a chunk are out, and the answer is still being sent"
        );

        drop(feed);
        let mut rest = String::new();
        visitor.read_to_string(&mut rest).await.unwrap();
        once_open_on_the_app_reads(&metrics, 0).await;
    }

    #[tokio::test]
    async fn a_request_whose_visitor_left_before_the_answer_ended_is_no_longer_open() {
        let metrics = Arc::new(HostMetrics::new());
        let router = Router::new(metrics.clone(), None);
        let (host_port, feed) = streaming_as_fed().await;
        routed_at(&router, host_port).await;
        let port = serving(router).await;

        let visitor = asking_for_a_stream(port).await;
        once_open_on_the_app_reads(&metrics, 1).await;
        drop(visitor);

        // The proxy learns the visitor has gone when a write to it fails, so the tenant keeps
        // sending until it does.
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while open_on_the_app(&metrics) > 0 {
            assert!(
                std::time::Instant::now() < deadline,
                "still open after the visitor left"
            );
            let _ = feed.unbounded_send(bytes::Bytes::from_static(b"to nobody"));
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    struct CountingWaker(std::sync::atomic::AtomicUsize);

    impl CountingWaker {
        fn woken(&self) -> usize {
            self.0.load(std::sync::atomic::Ordering::SeqCst)
        }
    }

    #[async_trait::async_trait]
    impl crate::ports::Waker for CountingWaker {
        async fn wake(
            &self,
            _app_id: &AppId,
        ) -> Result<crate::ports::WakeOutcome, crate::ports::WakeRefusal> {
            self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(crate::ports::WakeOutcome::Restored)
        }
    }

    fn three_megabytes_of_html() -> bytes::Bytes {
        bytes::Bytes::from("<p>a page worth keeping, and worth compressing</p>\n".repeat(60_000))
    }

    /// A guest answering every request with a 3 MB page any visitor may share for a minute.
    async fn a_guest_with_a_shareable_page() -> u16 {
        let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
            .await
            .unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    return;
                };
                tokio::spawn(async move {
                    let service = service_fn(|_request: Request<Incoming>| async move {
                        let mut page = Response::new(
                            http_body_util::Full::new(three_megabytes_of_html())
                                .map_err(|never| match never {})
                                .boxed(),
                        );
                        for (name, value) in [
                            ("content-type", "text/html; charset=utf-8"),
                            ("cache-control", "public, max-age=60"),
                            ("etag", "\"v1\""),
                        ] {
                            page.headers_mut()
                                .insert(name, hyper::header::HeaderValue::from_static(value));
                        }
                        Ok::<Response<ProxyBody>, std::convert::Infallible>(page)
                    });
                    let _ = http1::Builder::new()
                        .serve_connection(TokioIo::new(stream), service)
                        .await;
                });
            }
        });
        port
    }

    /// The router of a host with `[proxy.http.cache]` and `[proxy.http.encode]`, in front of a real
    /// activator for one app that is asleep, whose wakes are counted.
    async fn caching_in_front_of_a_sleeping_app() -> (
        u16,
        Arc<CountingWaker>,
        crate::state::SharedState,
        Arc<HostMetrics>,
        tempfile::TempDir,
    ) {
        let guest_port = a_guest_with_a_shareable_page().await;
        let state = crate::state::HostState::shared();
        let waker = Arc::new(CountingWaker(std::sync::atomic::AtomicUsize::new(0)));
        let metrics = Arc::new(HostMetrics::new());
        let activator = crate::adapters::proxy::activator::AppActivator::new(
            state.clone(),
            waker.clone(),
            metrics.clone(),
        );
        let host_port = loop {
            let port = nobody_listening().await;
            activator.serve(&[(crate::test_support::app_id(), port)]).await;
            if !activator.listening_for().await.is_empty() {
                break port;
            }
        };
        let record = instance_record(|record| {
            record.on_request = true;
            record.state = protocol::InstanceState::Idle;
            record.guest_ipv4 = Some(protocol::Ipv4Address::parse("127.0.0.1").unwrap());
            record.http_port = protocol::HttpPort::new(guest_port).unwrap();
            record.host_port = Some(host_port);
        });
        state.put_record(record.clone()).await;
        let directory = tempfile::tempdir().unwrap();
        let http_log = Arc::new(HttpRequestLog::new(directory.path().to_path_buf()).unwrap());
        let router = Router::with_http(
            metrics.clone(),
            Some(http_log),
            None,
            Some(&HttpCache {
                memory_mib: 64.try_into().unwrap(),
                max_entry_kib: 1024.try_into().unwrap(),
            }),
            Some(HttpEncode {
                encodings: vec![Compression::Br, Compression::Gzip],
                min_kib: 1,
            }),
            crate::state::HostState::shared(),
        );
        router
            .apply(RouteTable::from_targets(&renderable_routes(&[record])))
            .await;
        (serving(router).await, waker, state, metrics, directory)
    }

    async fn fetched(proxy: u16, accept_encoding: Option<&str>) -> reqwest::Response {
        crate::install_crypto_provider();
        let mut request = reqwest::Client::new()
            .get(format!("http://127.0.0.1:{proxy}/"))
            .header("host", "app-1.apps.example.com");
        if let Some(accepted) = accept_encoding {
            request = request.header("accept-encoding", accepted);
        }
        request.send().await.expect("the proxy answers")
    }

    async fn once_stored(metrics: &HostMetrics, more_than: u64) {
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while metrics.proxy.cache_bytes() <= more_than {
            assert!(std::time::Instant::now() < deadline, "nothing was stored");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    fn records(directory: &tempfile::TempDir) -> Vec<serde_json::Value> {
        std::fs::read_to_string(directory.path().join("app-1.http.jsonl"))
            .unwrap_or_default()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect()
    }

    async fn once_recorded(directory: &tempfile::TempDir, count: usize) -> Vec<serde_json::Value> {
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            let written = records(directory);
            if written.len() >= count {
                return written;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "{} of {count} records",
                written.len()
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    // In-process, the guest lane's done-when: what the cache answers, the activator never sees,
    // so the app stays asleep and nothing it serves counts as its activity.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_page_the_app_made_shareable_is_served_from_memory_while_the_app_sleeps() {
        let (proxy, waker, state, metrics, directory) = caching_in_front_of_a_sleeping_app().await;
        let app = crate::test_support::app_id();

        let first = fetched(proxy, Some("gzip, deflate, br, zstd")).await;
        assert_eq!(first.status(), 200);
        assert_eq!(first.headers()["content-encoding"], "br");
        assert_eq!(first.headers()["etag"], "W/\"v1\"");
        first.bytes().await.unwrap();
        assert_eq!(
            waker.woken(),
            1,
            "the miss went through the activator, which woke the app"
        );
        once_stored(&metrics, 0).await;
        let stored = metrics.proxy.cache_bytes();
        assert!(
            stored < 1024 * 1024,
            "3 MB of html is stored at its encoded size: {stored}"
        );
        let active_after_the_miss = state.snapshot().await.last_active_at_ms.get(&app).copied();

        for _ in 0..5 {
            let hit = fetched(proxy, Some("br, gzip")).await;
            assert_eq!(hit.status(), 200);
            assert_eq!(hit.headers()["content-encoding"], "br");
            assert!(hit.headers().contains_key("age"));
            let body = hit.bytes().await.unwrap();
            assert_eq!(
                encode::decoded(body, Encoding::Br)
                    .collect()
                    .await
                    .unwrap()
                    .to_bytes(),
                three_megabytes_of_html()
            );
        }
        let revalidating = fetched(proxy, Some("br")).await;
        let held = revalidating.headers()["etag"].clone();
        let not_modified = reqwest::Client::new()
            .get(format!("http://127.0.0.1:{proxy}/"))
            .header("host", "app-1.apps.example.com")
            .header("accept-encoding", "br")
            .header("if-none-match", held)
            .send()
            .await
            .unwrap();
        assert_eq!(not_modified.status(), 304);
        assert_eq!(waker.woken(), 1, "no hit woke the app");
        assert!(
            state.record(&app).await.unwrap().is_idle(),
            "the instance is still idle"
        );
        assert_eq!(
            state.snapshot().await.last_active_at_ms.get(&app).copied(),
            active_after_the_miss,
            "a hit is not the app's activity"
        );
        assert_eq!(metrics.proxy.open_requests_for(&app), 0);

        let gzip = fetched(proxy, Some("gzip")).await;
        assert_eq!(gzip.headers()["content-encoding"], "gzip");
        gzip.bytes().await.unwrap();
        assert_eq!(waker.woken(), 2, "a gzip visitor's variant is its own");
        once_stored(&metrics, stored).await;
        let identity = fetched(proxy, None).await;
        assert!(!identity.headers().contains_key("content-encoding"));
        assert_eq!(identity.bytes().await.unwrap(), three_megabytes_of_html());
        assert_eq!(
            waker.woken(),
            2,
            "identity is decoded from the stored gzip without a wake"
        );

        let written = once_recorded(&directory, 10).await;
        let hits: Vec<&serde_json::Value> =
            written.iter().filter(|record| record["cache"] == "hit").collect();
        assert_eq!(hits.len(), 8, "{written:?}");
        assert_eq!(written[0]["cache"], "miss");
        assert_eq!(written[0]["encoding"], "br");
        assert!(written[0]["wakeMicros"].is_u64(), "{:?}", written[0]);
        assert!(hits.iter().all(|hit| hit.get("wakeMicros").is_none()));
        let fastest = hits
            .iter()
            .filter_map(|hit| hit["durationMicros"].as_u64())
            .min()
            .unwrap();
        assert!(fastest < 2_000, "the fastest hit took {fastest}µs");
        let counted = crate::domain::metrics::tests::page(
            &crate::domain::metrics::tests::report(),
            &metrics,
            &state.snapshot().await,
            0,
        );
        assert!(
            counted.contains("nibrunner_proxy_cache_requests_total{app=\"app-1\",outcome=\"hit\"} 8\n"),
            "{counted}"
        );
    }

    /// A tenant whose every answer may be shared for a minute, counting the requests it is asked.
    async fn sharing_and_counting() -> (HostPort, Arc<std::sync::atomic::AtomicUsize>) {
        let upstream = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
            .await
            .unwrap();
        let host_port = HostPort::new(upstream.local_addr().unwrap().port()).unwrap();
        let asked = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counted = asked.clone();
        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = upstream.accept().await else {
                    return;
                };
                let counted = counted.clone();
                tokio::spawn(async move {
                    let service = service_fn(move |_request: Request<Incoming>| {
                        counted.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        async move {
                            let mut shared = say(StatusCode::OK, "shared\n");
                            shared.headers_mut().insert(
                                "cache-control",
                                hyper::header::HeaderValue::from_static("public, max-age=60"),
                            );
                            Ok::<_, std::convert::Infallible>(shared)
                        }
                    });
                    let _ = http1::Builder::new()
                        .serve_connection(TokioIo::new(stream), service)
                        .await;
                });
            }
        });
        (host_port, asked)
    }

    #[tokio::test]
    async fn a_post_that_succeeds_sends_the_next_get_for_its_target_to_the_app() {
        let (host_port, asked_of_the_app) = sharing_and_counting().await;
        let metrics = Arc::new(HostMetrics::new());
        let router = Router::with_http(
            metrics.clone(),
            None,
            None,
            Some(&HttpCache {
                memory_mib: 8.try_into().unwrap(),
                max_entry_kib: 1024.try_into().unwrap(),
            }),
            None,
            crate::state::HostState::shared(),
        );
        routed_at(&router, host_port).await;
        let proxy = serving(router).await;
        let get = "GET /cart HTTP/1.0\r\nHost: app-1.apps.example.com\r\n\r\n";
        let app_was_asked = || asked_of_the_app.load(std::sync::atomic::Ordering::SeqCst);
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while metrics.proxy.of(&crate::test_support::app_id()).cache[0] == 0 {
            assert!(std::time::Instant::now() < deadline, "nothing was ever a hit");
            assert!(asked(proxy, get).await.ends_with("shared\n"));
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let before = app_was_asked();
        asked(proxy, get).await;
        assert_eq!(app_was_asked(), before, "a hit");

        asked(
            proxy,
            "POST /cart HTTP/1.0\r\nHost: app-1.apps.example.com\r\nContent-Length: 0\r\n\r\n",
        )
        .await;
        asked(proxy, get).await;
        assert_eq!(
            app_was_asked(),
            before + 2,
            "the POST, then the GET it made stale"
        );
        assert_eq!(
            metrics.proxy.of(&crate::test_support::app_id()).cache[3],
            1,
            "a POST bypasses"
        );
    }

    #[test]
    fn a_redirect_keeps_the_host_and_path_and_refuses_what_could_split_the_response() {
        assert_eq!(
            secure_location("Hello.Apps.Example.com", "/a/b?c=1", 443).as_deref(),
            Some("https://hello.apps.example.com/a/b?c=1")
        );
        assert_eq!(
            secure_location("hello.example.com:80", "/", 8443).as_deref(),
            Some("https://hello.example.com:8443/"),
            "the plain port in the Host header is dropped and a non-default TLS port is named"
        );
        for host in [
            "",
            "evil.com/../x",
            "a b",
            "a@b.com",
            "x\r\nSet-Cookie: 1",
            ".",
            "a..b",
        ] {
            assert_eq!(secure_location(host, "/", 443), None, "{host:?}");
        }
        for target in ["", "*", "//x y", "/a\r\nb", "/ünïcode"] {
            assert_eq!(secure_location("ok.example.com", target, 443), None, "{target:?}");
        }
    }

    #[tokio::test]
    async fn the_plain_port_of_a_tls_host_moves_every_visitor_and_serves_no_app() {
        let probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = probe.local_addr().unwrap();
        drop(probe);
        tokio::spawn(serve_redirect(address, 443));
        let mut answered = String::new();
        for _ in 0..50 {
            match std::net::TcpStream::connect(address) {
                Ok(_) => break,
                Err(_) => tokio::time::sleep(Duration::from_millis(20)).await,
            }
        }
        answered.push_str(
            &asked(
                address.port(),
                "GET /hello?q=1 HTTP/1.1\r\nHost: hello.example.com\r\n\r\n",
            )
            .await,
        );
        assert!(answered.starts_with("HTTP/1.1 301 "), "{answered}");
        assert!(
            answered.contains("location: https://hello.example.com/hello?q=1\r\n"),
            "{answered}"
        );
        let unnamed = asked(address.port(), "GET / HTTP/1.0\r\n\r\n").await;
        assert!(
            unnamed.starts_with("HTTP/1.0 400 ") || unnamed.starts_with("HTTP/1.1 400 "),
            "{unnamed}"
        );
        let injected = asked(address.port(), "GET / HTTP/1.1\r\nHost: evil@example.com\r\n\r\n").await;
        assert!(injected.contains(" 400 "), "{injected}");
    }

    #[test]
    fn a_certificate_this_host_does_not_have_is_an_error_rather_than_a_proxy_that_serves_nothing() {
        let directory = tempfile::tempdir().unwrap();
        let refusal = |certificate: &Path, key: &Path| match tls_acceptor(certificate, key, None) {
            Ok(_) => panic!("{} was accepted as TLS material", certificate.display()),
            Err(error) => error,
        };
        let absent = directory.path().join("absent.pem");
        assert_eq!(refusal(&absent, &absent).kind(), std::io::ErrorKind::NotFound);

        let empty = directory.path().join("empty.pem");
        std::fs::write(&empty, b"").unwrap();
        let error = refusal(&empty, &empty);
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("holds no certificate"), "{error}");
    }

    async fn routed_to_one_app(server_name: Option<&'static str>) -> u16 {
        let router = Router::new(std::sync::Arc::new(HostMetrics::new()), None);
        router
            .apply(RouteTable::from_targets(&renderable_routes(&[instance_record(
                |_| {},
            )])))
            .await;
        serving_as(router, arriving(server_name, false)).await
    }

    #[tokio::test]
    async fn a_connection_opened_for_one_hostname_may_not_ask_for_another() {
        let port = routed_to_one_app(Some("somewhere.else.example.com")).await;
        let answered = asked(port, "GET / HTTP/1.0\r\nHost: app-1.apps.example.com\r\n\r\n").await;
        assert!(answered.starts_with("HTTP/1.0 421 "), "{answered}");
        assert!(
            answered.ends_with("This connection was opened for a different host.\n"),
            "{answered}"
        );
    }

    #[tokio::test]
    async fn the_name_in_the_handshake_and_the_name_in_the_header_are_one_name_in_any_case() {
        let port = routed_to_one_app(Some("app-1.apps.example.com")).await;
        let answered = asked(port, "GET / HTTP/1.0\r\nHost: APP-1.Apps.Example.Com\r\n\r\n").await;
        assert!(
            !answered.starts_with("HTTP/1.0 421 "),
            "the request reached its route rather than being read as a misdirected one: {answered}"
        );
    }

    #[tokio::test]
    async fn a_request_that_names_no_host_is_refused_before_the_handshake_name_is_read() {
        let port = routed_to_one_app(Some("app-1.apps.example.com")).await;
        let answered = asked(port, "GET / HTTP/1.0\r\n\r\n").await;
        assert!(answered.starts_with("HTTP/1.0 400 "), "{answered}");
    }

    async fn echoing_the_forwarded_headers(secure: bool) -> u16 {
        let upstream = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
            .await
            .unwrap();
        let host_port = HostPort::new(upstream.local_addr().unwrap().port()).unwrap();
        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = upstream.accept().await else {
                    return;
                };
                tokio::spawn(async move {
                    let service = service_fn(|request: Request<Incoming>| async move {
                        let read = |name: &str| {
                            request
                                .headers()
                                .get(name)
                                .and_then(|value| value.to_str().ok())
                                .unwrap_or("(none)")
                                .to_string()
                        };
                        let heard = format!(
                            "for={} proto={} host={}",
                            read("x-forwarded-for"),
                            read("x-forwarded-proto"),
                            read("x-forwarded-host")
                        );
                        Ok::<_, std::convert::Infallible>(say(StatusCode::OK, &heard))
                    });
                    let _ = http1::Builder::new()
                        .serve_connection(TokioIo::new(stream), service)
                        .await;
                });
            }
        });
        let router = Router::new(std::sync::Arc::new(HostMetrics::new()), None);
        router
            .apply(RouteTable::from_targets(&renderable_routes(&[instance_record(
                |record| record.host_port = Some(host_port),
            )])))
            .await;
        serving_as(router, arriving(None, secure)).await
    }

    #[tokio::test]
    async fn a_tenant_is_told_where_the_request_came_from_and_what_it_arrived_over() {
        let over_tls = echoing_the_forwarded_headers(true).await;
        let answered = asked(over_tls, "GET / HTTP/1.0\r\nHost: app-1.apps.example.com\r\n\r\n").await;
        assert!(
            answered.ends_with("for=203.0.113.7 proto=https host=app-1.apps.example.com"),
            "{answered}"
        );

        let in_the_clear = echoing_the_forwarded_headers(false).await;
        let answered = asked(
            in_the_clear,
            "GET / HTTP/1.0\r\nHost: app-1.apps.example.com\r\n\r\n",
        )
        .await;
        assert!(
            answered.ends_with("proto=http host=app-1.apps.example.com"),
            "{answered}"
        );
    }

    #[tokio::test]
    async fn what_an_edge_already_wrote_down_is_added_to_rather_than_written_over() {
        let port = echoing_the_forwarded_headers(true).await;
        let answered = asked(
            port,
            "GET / HTTP/1.0\r\nHost: app-1.apps.example.com\r\nx-forwarded-for: 198.51.100.9\r\nx-forwarded-proto: http\r\nx-forwarded-host: brought.example.com\r\n\r\n",
        )
        .await;
        assert!(
            answered.ends_with("for=198.51.100.9, 203.0.113.7 proto=http host=brought.example.com"),
            "the visitor's own leg outlives the hop this proxy made: {answered}"
        );
    }

    #[test]
    fn a_trust_pool_that_admits_nobody_is_refused_rather_than_a_listener_that_refuses_everyone() {
        let directory = tempfile::tempdir().unwrap();
        let empty = directory.path().join("ca.pem");
        std::fs::write(&empty, b"").unwrap();
        let error = client_verifier(&empty).expect_err("an empty trust pool was accepted");
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);

        let absent = directory.path().join("absent.pem");
        assert_eq!(
            client_verifier(&absent)
                .expect_err("a trust pool this host does not have was accepted")
                .kind(),
            std::io::ErrorKind::NotFound
        );
    }

    #[test]
    fn two_apps_that_claim_the_same_hostname_do_not_both_get_the_traffic() {
        let mine = instance_record(|_| {});
        let theirs = instance_record(|record| {
            record.app_id = protocol::AppId::parse("app-2").unwrap();
            record.host_port = Some(HostPort::new(mine.host_port.unwrap().get() + 1).unwrap());
        });
        let table = RouteTable::from_targets(&renderable_routes(&[mine, theirs.clone()]));
        assert_eq!(table.hostnames().len(), 1);
        assert_eq!(table.port_for(app_hostname().hostname.as_str()), theirs.host_port);
    }

    // Reading the file whole says how many certificates it holds; the trust store then keeps the
    // ones it can parse and drops the rest without a word, and that would be the same short pool.
    #[test]
    fn a_pool_the_trust_store_would_quietly_thin_is_refused_with_the_count() {
        let directory = tempfile::tempdir().unwrap();
        let pool = directory.path().join("origin-pull-ca.pem");
        std::fs::write(
            &pool,
            "-----BEGIN CERTIFICATE-----\nAAAA\n-----END CERTIFICATE-----\n-----BEGIN CERTIFICATE-----\nAQID\n-----END CERTIFICATE-----\n",
        )
        .unwrap();

        let said = client_verifier(&pool).unwrap_err().to_string();
        assert_eq!(
            said,
            format!(
                "{} holds 2 certificate(s) that could not be parsed as X.509",
                pool.display()
            )
        );
    }
}
