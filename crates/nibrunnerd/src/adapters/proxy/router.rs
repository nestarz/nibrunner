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
use protocol::{AppId, DeploymentId, HostPort};
use tokio::net::{TcpListener, TcpStream};

use crate::domain::metrics::{HostMetrics, Outcome};
use tokio::sync::RwLock;

use crate::adapters::proxy::access::{AccessLog, AccessRecord};
use crate::adapters::proxy::forward::{
    forward, hostname_of, note_the_hop, say, ProxyBody, Unreachable, Upstreams,
};
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
    access: Option<Arc<AccessLog>>,
}

impl Router {
    pub fn new(metrics: Arc<HostMetrics>, access: Option<Arc<AccessLog>>) -> Arc<Self> {
        Arc::new(Self {
            routes: RwLock::new(Arc::new(RouteTable::default())),
            upstreams: Upstreams::default(),
            metrics,
            access,
        })
    }

    pub fn discard_access(&self, app_id: &AppId) {
        if let Some(access) = &self.access {
            access.discard(app_id);
        }
    }

    pub async fn apply(&self, table: RouteTable) {
        self.upstreams.keep_only(&table.apps()).await;
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
        let access = self.access.as_ref().map(|sink| {
            (
                sink.clone(),
                request.method().as_str().to_owned(),
                request.uri().clone(),
            )
        });
        let started = std::time::Instant::now();
        let metrics = self.metrics.clone();
        metrics.proxy.began();
        let (mut response, outcome, route) = self.route(request, arrival).await;
        let elapsed = started.elapsed();
        metrics
            .proxy
            .answered(outcome, elapsed, route.as_ref().map(|route| &route.app_id));
        metrics.proxy.ended();
        if let (Some((sink, method, uri)), Some(route)) = (access, route) {
            sink.record(
                route.app_id,
                AccessRecord::new(
                    &method,
                    &uri,
                    response.status().as_u16(),
                    u64::try_from(elapsed.as_micros()).unwrap_or(u64::MAX),
                    route.deployment_id,
                ),
            );
        }
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
        let Some(route) = self.routes().await.route_for(&hostname) else {
            return (
                say(
                    StatusCode::NOT_FOUND,
                    "No app on this host answers for that hostname.\n",
                ),
                Outcome::NoSuchHost,
                None,
            );
        };
        note_the_hop(request.headers_mut(), arrival.peer, arrival.secure, &hostname);
        let open = self.metrics.proxy.open(&route.app_id);
        let upstream = self.upstreams.to(&route.app_id).await;
        let response = forward(&upstream, request, LOOPBACK, route.host_port.get(), true, open).await;
        let reached = response.extensions().get::<Unreachable>().is_none();
        self.metrics
            .proxy
            .app_answered(&route.app_id, response.status(), reached);
        let outcome = if reached {
            Outcome::Served
        } else {
            Outcome::Unreachable
        };
        (response, outcome, Some(route))
    }
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
    let certificates = pem::read_certificates(certificate)?;
    tracing::info!(
        certificates = certificates.len(),
        chain = %certificate.display(),
        "origin certificate chain loaded"
    );
    let private_key = rustls_pemfile::private_key(&mut std::io::BufReader::new(std::fs::File::open(key)?))?
        .ok_or_else(|| invalid("the key file holds no private key"))?;
    let builder = rustls::ServerConfig::builder();
    let builder = match client_ca {
        Some(client_ca) => builder.with_client_cert_verifier(client_verifier(client_ca)?),
        None => builder.with_no_client_auth(),
    };
    let mut config = builder
        .with_single_cert(certificates, private_key)
        .map_err(invalid)?;
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
        assert_eq!(
            table.port_for(app_hostname().hostname.as_str()),
            Some(record.host_port)
        );
        assert_eq!(table.port_for("www.example.com"), Some(record.host_port));
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
                |record| record.host_port = host_port,
            )])))
            .await;
        let port = serving(router).await;
        let answered = asked(port, "GET / HTTP/1.0\r\nHost: App-1.Apps.Example.Com\r\n\r\n").await;
        assert!(answered.starts_with("HTTP/1.0 200 "), "{answered}");
        assert!(answered.ends_with("served by the tenant\n"), "{answered}");
    }

    #[tokio::test]
    async fn a_routed_request_writes_an_access_record_without_its_query() {
        let directory = tempfile::tempdir().expect("an access log directory");
        let access = Arc::new(AccessLog::new(directory.path().to_path_buf()).expect("access logs"));
        let router = Router::new(Arc::new(HostMetrics::new()), Some(access));
        routed_at(&router, nobody_listening().await).await;
        let port = serving(router).await;
        let answered = asked(
            port,
            "GET /catalog?token=private HTTP/1.0\r\nHost: app-1.apps.example.com\r\n\r\n",
        )
        .await;
        assert!(answered.starts_with("HTTP/1.0 502 "), "{answered}");

        let path = directory.path().join("app-1.access.jsonl");
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
        .expect("the access writer flushes the routed request");
        assert!(record.contains("\"path\":\"/catalog\""), "{record}");
        assert!(record.contains("\"status\":502"), "{record}");
        assert!(record.contains("\"deploymentId\":\"dep-1\""), "{record}");
        assert!(!record.contains("private"), "{record}");
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
            record.host_port = host_port;
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
                |record| record.host_port = host_port,
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
                |record| record.host_port = host_port,
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
            record.host_port = HostPort::new(mine.host_port.get() + 1).unwrap();
        });
        let table = RouteTable::from_targets(&renderable_routes(&[mine, theirs.clone()]));
        assert_eq!(table.hostnames().len(), 1);
        assert_eq!(
            table.port_for(app_hostname().hostname.as_str()),
            Some(theirs.host_port)
        );
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
