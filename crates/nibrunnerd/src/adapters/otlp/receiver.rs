//! The OTLP/HTTP endpoint every guest is given: `POST /v1/traces` and `/v1/logs` in OTLP/JSON,
//! one listener on each tap's host address. The sender is the unit whose tap the connection came
//! over, so a guest can only ever write its own unit's lines.
use std::collections::BTreeMap;
use std::io::Read;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use http_body_util::{BodyExt, Full, Limited};
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::{TokioIo, TokioTimer};
use protocol::{AppId, DeploymentId};
use tokio::net::TcpListener;

use crate::adapters::logs::ObjectSink;
use crate::adapters::otlp::trace::TraceBook;
use crate::adapters::otlp::wire::{ExportLogs, ExportTraces};

const MAX_BODY_BYTES: usize = 4 << 20;
const GREETING_TIMEOUT: Duration = Duration::from_secs(10);

/// A guest that may export, and the unit its lines are filed under.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OtlpSender {
    pub app_id: AppId,
    pub deployment_id: Option<DeploymentId>,
    pub host_ipv4: Ipv4Addr,
    pub guest_ipv4: Ipv4Addr,
}

struct Listener {
    local: SocketAddr,
    task: tokio::task::JoinHandle<()>,
}

/// Who the guest at an address is right now, read from the host's slots and records.
pub type SenderLookup =
    Arc<dyn Fn(Ipv4Addr) -> futures::future::BoxFuture<'static, Option<OtlpSender>> + Send + Sync>;

/// How long an export from an address no unit holds yet waits for one to: a slot is taken and its
/// record written before the guest boots, so this is only ever a pass caught between the two.
const UNKNOWN_SENDER_WAIT: Duration = Duration::from_secs(2);
const UNKNOWN_SENDER_POLL: Duration = Duration::from_millis(50);

pub struct OtlpReceiver {
    sink: Option<Arc<ObjectSink>>,
    lookup: std::sync::OnceLock<SenderLookup>,
    port: u16,
    senders: std::sync::RwLock<Arc<BTreeMap<Ipv4Addr, OtlpSender>>>,
    listeners: tokio::sync::Mutex<BTreeMap<Ipv4Addr, Listener>>,
    /// Addresses whose bind failed and was said so once; every pass tries them again.
    refused: std::sync::Mutex<std::collections::BTreeSet<Ipv4Addr>>,
}

type Answer = Response<Full<Bytes>>;

fn answer(status: StatusCode, body: &'static str) -> Answer {
    let mut response = Response::new(Full::new(Bytes::from_static(body.as_bytes())));
    *response.status_mut() = status;
    if status.is_success() {
        response.headers_mut().insert(
            hyper::header::CONTENT_TYPE,
            hyper::header::HeaderValue::from_static("application/json"),
        );
    }
    response
}

/// A tap's address is the host's only once its tap is up, which may be after the pass that names
/// its slot; a daemon that restarts finds its taps already there.
fn bind(address: SocketAddr) -> std::io::Result<TcpListener> {
    let socket = tokio::net::TcpSocket::new_v4()?;
    socket.set_reuseaddr(true)?;
    #[cfg(target_os = "linux")]
    nix::sys::socket::setsockopt(&socket, nix::sys::socket::sockopt::IpFreebind, &true)
        .map_err(std::io::Error::from)?;
    socket.bind(address)?;
    socket.listen(64)
}

impl OtlpReceiver {
    /// Lines go to `sink`; without one, an export is answered and let go.
    pub fn new(sink: Option<Arc<ObjectSink>>, port: u16) -> Arc<Self> {
        Arc::new(Self {
            sink,
            lookup: std::sync::OnceLock::new(),
            port,
            senders: std::sync::RwLock::default(),
            listeners: tokio::sync::Mutex::default(),
            refused: std::sync::Mutex::default(),
        })
    }

    /// Asks `lookup` who sent each export, so a guest is known from the moment its slot and record
    /// are, rather than from the next pass that hands over the senders.
    pub fn resolve_with(&self, lookup: SenderLookup) {
        let _ = self.lookup.set(lookup);
    }

    /// Listens on `host`, a tap address a guest is about to boot behind, unless it already does.
    pub async fn listen(self: &Arc<Self>, host: Ipv4Addr) {
        let mut listeners = self.listeners.lock().await;
        self.bind_missing(&mut listeners, [host]);
    }

    /// Listens on each sender's tap address, and on no other.
    pub async fn serve(self: &Arc<Self>, senders: Vec<OtlpSender>) {
        let hosts: std::collections::BTreeSet<Ipv4Addr> = senders.iter().map(|s| s.host_ipv4).collect();
        *self
            .senders
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) =
            Arc::new(senders.into_iter().map(|s| (s.guest_ipv4, s)).collect());
        let mut listeners = self.listeners.lock().await;
        listeners.retain(|host, listener| {
            let keep = hosts.contains(host);
            if !keep {
                listener.task.abort();
            }
            keep
        });
        self.bind_missing(&mut listeners, hosts);
    }

    fn bind_missing(
        self: &Arc<Self>,
        listeners: &mut BTreeMap<Ipv4Addr, Listener>,
        hosts: impl IntoIterator<Item = Ipv4Addr>,
    ) {
        for host in hosts {
            if listeners.contains_key(&host) {
                continue;
            }
            match bind(SocketAddr::from((host, self.port))).and_then(|l| Ok((l.local_addr()?, l))) {
                Ok((local, listener)) => {
                    tracing::debug!(%local, "OTLP receiver listening");
                    self.refused
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner())
                        .remove(&host);
                    let task = tokio::spawn(accept(listener, self.clone()));
                    listeners.insert(host, Listener { local, task });
                }
                Err(error) => {
                    let first = self
                        .refused
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner())
                        .insert(host);
                    if first {
                        tracing::warn!(%host, %error, "OTLP receiver bind failed");
                    }
                }
            }
        }
    }

    pub async fn listening_on(&self) -> Vec<SocketAddr> {
        self.listeners.lock().await.values().map(|l| l.local).collect()
    }

    /// The unit of the guest at `peer`, when it reached the address of its own tap: as the host
    /// knows it now, else as the last pass said, waiting briefly for a unit being set up.
    async fn sender(&self, peer: IpAddr, local: IpAddr) -> Option<OtlpSender> {
        let (IpAddr::V4(peer), IpAddr::V4(local)) = (peer, local) else {
            return None;
        };
        let deadline = tokio::time::Instant::now() + UNKNOWN_SENDER_WAIT;
        loop {
            let live = match self.lookup.get() {
                Some(lookup) => lookup(peer).await,
                None => None,
            };
            let known = live.or_else(|| {
                let senders = self
                    .senders
                    .read()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .clone();
                senders.get(&peer).cloned()
            });
            if let Some(sender) = known.filter(|s| s.host_ipv4 == local) {
                return Some(sender);
            }
            if self.lookup.get().is_none() || tokio::time::Instant::now() >= deadline {
                return None;
            }
            tokio::time::sleep(UNKNOWN_SENDER_POLL).await;
        }
    }

    async fn answer(&self, peer: IpAddr, local: Option<IpAddr>, request: Request<Incoming>) -> Answer {
        let sender = match local {
            Some(local) => self.sender(peer, local).await,
            None => None,
        };
        let Some(sender) = sender else {
            return answer(StatusCode::FORBIDDEN, "This address is no guest of this host.\n");
        };
        let signal = match request.uri().path() {
            "/v1/traces" => Signal::Traces,
            "/v1/logs" => Signal::Logs,
            "/v1/metrics" => Signal::Metrics,
            _ => return answer(StatusCode::NOT_FOUND, "OTLP takes /v1/traces and /v1/logs.\n"),
        };
        if request.method() != Method::POST {
            return answer(StatusCode::METHOD_NOT_ALLOWED, "An export is a POST.\n");
        }
        if signal == Signal::Metrics {
            return answer(StatusCode::NO_CONTENT, "");
        }
        let body = match read(request).await {
            Ok(body) => body,
            Err((status, why)) => return answer(status, why),
        };
        let now_us = crate::clock::now_us();
        let (spans, records) = match signal {
            Signal::Traces => match serde_json::from_slice::<ExportTraces>(&body) {
                Ok(export) => (export.lines(TraceBook::host(), now_us), vec![]),
                Err(_) => return answer(StatusCode::BAD_REQUEST, "This is not an OTLP/JSON export.\n"),
            },
            _ => match serde_json::from_slice::<ExportLogs>(&body) {
                Ok(export) => (vec![], export.lines(now_us)),
                Err(_) => return answer(StatusCode::BAD_REQUEST, "This is not an OTLP/JSON export.\n"),
            },
        };
        if let Some(sink) = &self.sink {
            sink.exported(&sender.app_id, sender.deployment_id.as_ref(), spans, records);
        }
        answer(StatusCode::OK, "{}")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Signal {
    Traces,
    Logs,
    Metrics,
}

type Refusal = (StatusCode, &'static str);

fn too_large() -> Refusal {
    (StatusCode::PAYLOAD_TOO_LARGE, "An export is at most 4 MiB.\n")
}

/// The JSON of an export, gunzipped when it came compressed: at most [`MAX_BODY_BYTES`] either way.
async fn read(request: Request<Incoming>) -> Result<Vec<u8>, Refusal> {
    let header = |name| {
        request
            .headers()
            .get(name)
            .and_then(|value| value.to_str().ok())
            .map(str::to_ascii_lowercase)
    };
    let media = header(hyper::header::CONTENT_TYPE).unwrap_or_default();
    if media.split(';').next().map(str::trim) != Some("application/json") {
        return Err((
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "This receiver takes OTLP/JSON: set OTEL_EXPORTER_OTLP_PROTOCOL=http/json.\n",
        ));
    }
    let gzipped = match header(hyper::header::CONTENT_ENCODING).as_deref() {
        None | Some("identity") => false,
        Some("gzip") => true,
        Some(_) => {
            return Err((
                StatusCode::UNSUPPORTED_MEDIA_TYPE,
                "An export is plain or gzipped.\n",
            ))
        }
    };
    let declared = header(hyper::header::CONTENT_LENGTH).and_then(|length| length.parse::<usize>().ok());
    if declared.is_some_and(|length| length > MAX_BODY_BYTES) {
        return Err(too_large());
    }
    let body = match Limited::new(request.into_body(), MAX_BODY_BYTES).collect().await {
        Ok(collected) => collected.to_bytes(),
        Err(error) if error.is::<http_body_util::LengthLimitError>() => return Err(too_large()),
        Err(_) => return Err((StatusCode::BAD_REQUEST, "The export ended early.\n")),
    };
    if !gzipped {
        return Ok(body.to_vec());
    }
    let mut plain = Vec::new();
    let limit = u64::try_from(MAX_BODY_BYTES).unwrap_or(u64::MAX) + 1;
    match flate2::read::GzDecoder::new(&body[..])
        .take(limit)
        .read_to_end(&mut plain)
    {
        Ok(n) if n > MAX_BODY_BYTES => Err(too_large()),
        Ok(_) => Ok(plain),
        Err(_) => Err((StatusCode::BAD_REQUEST, "The export is not gzip.\n")),
    }
}

async fn accept(listener: TcpListener, receiver: Arc<OtlpReceiver>) {
    loop {
        let Ok((stream, peer)) = listener.accept().await else {
            continue;
        };
        let local = stream.local_addr().ok().map(|a| a.ip());
        let receiver = receiver.clone();
        tokio::spawn(async move {
            // Asked on every request: a guest booted beside its app under the staging id is the
            // app itself once the cut-over hands it the slot.
            let service = service_fn(move |request| {
                let receiver = receiver.clone();
                async move { Ok::<_, std::convert::Infallible>(receiver.answer(peer.ip(), local, request).await) }
            });
            let _ = http1::Builder::new()
                .timer(TokioTimer::new())
                .header_read_timeout(GREETING_TIMEOUT)
                .serve_connection(TokioIo::new(stream), service)
                .await;
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapters::logs::object_sink::tests::{read_lines, sink};
    use crate::adapters::logs::object_sink::{LogKind, LogLine, SpanStatus, TraceLine};
    use crate::adapters::otlp::wire::tests::{
        DENO_LOGS_AT_START, DENO_LOGS_OF_A_REQUEST, DENO_METRICS, DENO_TRACE, DENO_TRACES,
    };
    use crate::test_support::{app_id, deployment_id};

    const LOOPBACK: Ipv4Addr = Ipv4Addr::LOCALHOST;

    fn loopback_sender() -> OtlpSender {
        OtlpSender {
            app_id: app_id(),
            deployment_id: Some(deployment_id()),
            host_ipv4: LOOPBACK,
            guest_ipv4: LOOPBACK,
        }
    }

    async fn receiving(sink: Option<Arc<ObjectSink>>, senders: Vec<OtlpSender>) -> SocketAddr {
        let receiver = OtlpReceiver::new(sink, 0);
        receiver.serve(senders).await;
        let listening = receiver.listening_on().await;
        assert_eq!(listening.len(), 1);
        listening[0]
    }

    async fn post(
        at: SocketAddr,
        path: &str,
        content_type: &str,
        body: impl Into<reqwest::Body>,
    ) -> reqwest::Response {
        crate::install_crypto_provider();
        reqwest::Client::new()
            .post(format!("http://{at}{path}"))
            .header("content-type", content_type)
            .body(body)
            .send()
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn a_recorded_deno_export_lands_in_its_units_traces_and_logs_with_its_records_attributed() {
        let (sink, store) = sink();
        // The recording's trace is the one its request was given; replayed now, it is one this
        // proxy has just minted.
        let minted = TraceBook::host().mint(crate::clock::now_us()).to_string();
        let at = receiving(Some(sink.clone()), vec![loopback_sender()]).await;
        for (path, body) in [
            ("/v1/logs", DENO_LOGS_AT_START),
            ("/v1/logs", DENO_LOGS_OF_A_REQUEST),
            ("/v1/traces", DENO_TRACES),
        ] {
            let answered = post(at, path, "application/json", body.replace(DENO_TRACE, &minted)).await;
            assert_eq!(answered.status(), 200, "{path}");
            assert_eq!(answered.text().await.unwrap(), "{}");
        }
        let metrics = post(at, "/v1/metrics", "application/json", DENO_METRICS).await;
        assert_eq!(metrics.status(), 204);
        sink.forget(&app_id()).await.unwrap();

        let traces: Vec<TraceLine> = read_lines(&store, "traces/site/2026102009/2152-ovh-1.jsonl.gz").await;
        assert_eq!(traces.len(), 3);
        for line in &traces {
            let TraceLine::Span(span) = line else {
                panic!("a span line");
            };
            assert_eq!(span.trace, minted);
            assert_eq!(
                (
                    span.host.as_str(),
                    span.unit.as_str(),
                    span.rev.as_str(),
                    span.context.as_str()
                ),
                ("ovh-1", app_id().as_str(), "0123456789ab", "production")
            );
            assert!(!span.foreign && !span.root);
            assert!(matches!(span.status, Some(SpanStatus::Otel(_))));
        }
        let logs: Vec<LogLine> = read_lines(&store, "logs/site/2026102009/2150-ovh-1.jsonl.gz").await;
        assert_eq!(logs.len(), 4);
        assert!(logs.iter().all(|line| line.k == LogKind::Otel));
        let attributed: Vec<_> = logs.iter().filter(|line| line.trace.is_some()).collect();
        assert_eq!(attributed.len(), 2);
        assert!(attributed
            .iter()
            .all(|line| line.trace.as_deref() == Some(minted.as_str())
                && line.span.as_deref() == Some("b0709fad9766112d")));
        assert_eq!(logs[2].msg.as_deref(), Some("handling /hello"));
        let first = serde_json::to_value(&logs[0]).unwrap();
        assert_eq!(first["k"], "otel");
        assert_eq!(first["severity"], "INFO");
        assert!(first.get("trace").is_none());
    }

    #[tokio::test]
    async fn a_guests_first_export_is_filed_under_the_deployment_the_host_knows_it_as_before_any_pass() {
        let (sink, store) = sink();
        sink.learn(&crate::test_support::desired_state(|state| {
            state.host_id = protocol::HostId::parse("ovh-1").unwrap();
            state.instances = vec![crate::test_support::desired_instance(|instance| {
                instance.deployment_id = DeploymentId::parse("dep-2").unwrap();
                instance.labels = serde_json::from_value(serde_json::json!({
                    "app": "site", "rev": "r-2", "context": "production"
                }))
                .unwrap();
            })];
        }));
        let receiver = OtlpReceiver::new(Some(sink.clone()), 0);
        let known_after = tokio::time::Instant::now() + Duration::from_millis(300);
        receiver.resolve_with(Arc::new(move |guest| {
            Box::pin(async move {
                (tokio::time::Instant::now() >= known_after).then(|| OtlpSender {
                    deployment_id: Some(DeploymentId::parse("dep-2").unwrap()),
                    guest_ipv4: guest,
                    ..loopback_sender()
                })
            })
        }));
        receiver.listen(LOOPBACK).await;
        let at = receiver.listening_on().await[0];
        let minted = TraceBook::host().mint(crate::clock::now_us()).to_string();
        let answered = post(
            at,
            "/v1/traces",
            "application/json",
            DENO_TRACES.replace(DENO_TRACE, &minted),
        )
        .await;
        assert_eq!(
            answered.status(),
            200,
            "a unit being set up is waited for, not refused"
        );
        sink.forget(&app_id()).await.unwrap();
        let traces: Vec<TraceLine> = read_lines(&store, "traces/site/2026102009/2152-ovh-1.jsonl.gz").await;
        assert!(traces
            .iter()
            .all(|line| matches!(line, TraceLine::Span(span) if span.rev == "r-2")));
    }

    #[tokio::test]
    async fn an_address_that_is_no_guest_or_reached_another_taps_address_is_refused() {
        let elsewhere = OtlpSender {
            guest_ipv4: Ipv4Addr::new(10, 201, 0, 2),
            ..loopback_sender()
        };
        let at = receiving(None, vec![elsewhere]).await;
        let answered = post(at, "/v1/traces", "application/json", DENO_TRACES).await;
        assert_eq!(answered.status(), 403);

        let receiver = OtlpReceiver::new(None, 0);
        assert!(receiver.sender(LOOPBACK.into(), LOOPBACK.into()).await.is_none());
        receiver
            .serve(vec![OtlpSender {
                host_ipv4: Ipv4Addr::new(10, 201, 0, 1),
                ..loopback_sender()
            }])
            .await;
        assert!(
            receiver.sender(LOOPBACK.into(), LOOPBACK.into()).await.is_none(),
            "a guest reaches only its own tap's address"
        );
        assert!(receiver
            .sender(LOOPBACK.into(), Ipv4Addr::new(10, 201, 0, 1).into())
            .await
            .is_some());
    }

    #[tokio::test]
    async fn protobuf_is_unsupported_a_body_past_4_mib_too_large_and_garbage_a_bad_request() {
        let at = receiving(None, vec![loopback_sender()]).await;
        let protobuf = post(at, "/v1/traces", "application/x-protobuf", vec![0u8; 8]).await;
        assert_eq!(protobuf.status(), 415);
        // Refused on its declared length, before a byte of it is read.
        let large = {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            let mut stream = tokio::net::TcpStream::connect(at).await.unwrap();
            let head = format!(
                "POST /v1/logs HTTP/1.1\r\nHost: x\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n",
                MAX_BODY_BYTES + 1
            );
            stream.write_all(head.as_bytes()).await.unwrap();
            let mut answer = [0; 12];
            stream.read_exact(&mut answer).await.unwrap();
            String::from_utf8_lossy(&answer).into_owned()
        };
        assert_eq!(large, "HTTP/1.1 413");
        let garbage = post(at, "/v1/logs", "application/json; charset=utf-8", "{nope").await;
        assert_eq!(garbage.status(), 400);
        let unknown = post(at, "/v1/profiles", "application/json", "{}").await;
        assert_eq!(unknown.status(), 404);
    }

    #[tokio::test]
    async fn a_gzipped_export_is_read_and_one_that_inflates_past_4_mib_refused() {
        let gzip = |plain: &[u8]| {
            use std::io::Write;
            let mut encoder = flate2::write::GzEncoder::new(vec![], flate2::Compression::default());
            encoder.write_all(plain).unwrap();
            encoder.finish().unwrap()
        };
        let at = receiving(None, vec![loopback_sender()]).await;
        crate::install_crypto_provider();
        let send = |body: Vec<u8>| async move {
            reqwest::Client::new()
                .post(format!("http://{at}/v1/traces"))
                .header("content-type", "application/json")
                .header("content-encoding", "gzip")
                .body(body)
                .send()
                .await
                .unwrap()
                .status()
        };
        assert_eq!(send(gzip(DENO_TRACES.as_bytes())).await, 200);
        let mut bomb = b"{\"resourceSpans\":[]".to_vec();
        bomb.resize(MAX_BODY_BYTES + 10, b' ');
        bomb.push(b'}');
        assert_eq!(send(gzip(&bomb)).await, 413);
    }
}
