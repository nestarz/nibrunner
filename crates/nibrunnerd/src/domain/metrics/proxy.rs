use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use hyper::StatusCode;
use protocol::AppId;

use crate::domain::metrics::{Histogram, Kind, Metric, Page};
use crate::state::HostSnapshot;

// Straddling the millisecond a tenant answers in and the tens of milliseconds a stall costs, since
// telling those apart is the whole reason this histogram exists.
const BUCKET_BOUNDS_SECONDS: [f64; 13] = [
    0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    Served,
    Unreachable,
    NoSuchHost,
    WrongHost,
    Refused,
}

impl Outcome {
    pub fn as_str(self) -> &'static str {
        match self {
            Outcome::Served => "served",
            Outcome::Unreachable => "unreachable",
            Outcome::NoSuchHost => "no_such_host",
            Outcome::WrongHost => "wrong_host",
            Outcome::Refused => "refused",
        }
    }
}

// What this proxy decided, which is all it can honestly claim: a 502 or a 503 travelling back
// through it was the upstream's answer and is counted as served, because serving it is what
// happened. The one 502 that is not is the one the proxy wrote itself because nothing answered.
pub const OUTCOMES: [Outcome; 5] = [
    Outcome::Served,
    Outcome::Unreachable,
    Outcome::NoSuchHost,
    Outcome::WrongHost,
    Outcome::Refused,
];

/// How a TLS handshake ended, in the word the counter uses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Handshake {
    Completed,
    Failed,
    TimedOut,
}

impl Handshake {
    pub fn as_str(self) -> &'static str {
        match self {
            Handshake::Completed => "completed",
            Handshake::Failed => "failed",
            Handshake::TimedOut => "timed_out",
        }
    }
}

const HANDSHAKES: [Handshake; 3] = [Handshake::Completed, Handshake::Failed, Handshake::TimedOut];

/// What came of a stream or datagram session on a raw port.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RawOutcome {
    Served,
    Down,
    Refused,
    Unreachable,
}

impl RawOutcome {
    pub fn as_str(self) -> &'static str {
        match self {
            RawOutcome::Served => "served",
            RawOutcome::Down => "down",
            RawOutcome::Refused => "refused",
            RawOutcome::Unreachable => "unreachable",
        }
    }
}

const RAW_OUTCOMES: [RawOutcome; 4] = [
    RawOutcome::Served,
    RawOutcome::Down,
    RawOutcome::Refused,
    RawOutcome::Unreachable,
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Protocol {
    Tcp,
    Udp,
}

impl Protocol {
    pub fn as_str(self) -> &'static str {
        match self {
            Protocol::Tcp => "tcp",
            Protocol::Udp => "udp",
        }
    }
}

const PROTOCOLS: [Protocol; 2] = [Protocol::Tcp, Protocol::Udp];

// 1xx to 5xx, by the hundreds digit.
const CLASSES: [&str; 5] = ["1xx", "2xx", "3xx", "4xx", "5xx"];

fn class_of(status: StatusCode) -> usize {
    (usize::from(status.as_u16() / 100)).clamp(1, 5) - 1
}

// A 1xx that ends a request here is a websocket's upgrade: waiting for it is a handshake rather
// than an answer, and a class almost always empty would cost every app a histogram of nothing.
const TIMED_CLASSES: [&str; 4] = ["2xx", "3xx", "4xx", "5xx"];

fn timed_class_of(status: StatusCode) -> Option<usize> {
    class_of(status).checked_sub(1)
}

// Prometheus's own defaults, which every dashboard already reads. Coarser than the host-wide
// histogram's, because these are rendered once per class for every app on the page, and a
// tenant's latency matters in the milliseconds a visitor notices, up to a wake.
const APP_BUCKET_BOUNDS_SECONDS: [f64; 11] = [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0];

#[derive(Debug)]
struct AppDurations([Histogram; TIMED_CLASSES.len()]);

impl Default for AppDurations {
    fn default() -> Self {
        Self(std::array::from_fn(|_| {
            Histogram::over(&APP_BUCKET_BOUNDS_SECONDS)
        }))
    }
}

/// What one app's ingress has carried, kept while the document names it. What its requests cost
/// is a sum and a count here; the shape of that cost, by class, is [`ProxyMetrics`]'s to keep.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct AppProxy {
    pub requests: [u64; CLASSES.len()],
    pub request_count: u64,
    pub request_micros: u64,
    pub unreachable: u64,
    pub open: u64,
    pub raw_sessions: [[u64; RAW_OUTCOMES.len()]; PROTOCOLS.len()],
    pub raw_bytes_in: [u64; PROTOCOLS.len()],
    pub raw_bytes_out: [u64; PROTOCOLS.len()],
}

fn position<T: PartialEq>(of: &[T], value: &T) -> usize {
    of.iter().position(|each| each == value).unwrap_or(0)
}

type Apps = Arc<Mutex<BTreeMap<AppId, AppProxy>>>;

/// One request routed to an app, open until this is dropped: with the last byte of the response
/// body, or when a websocket closes. The rx counters see nothing of an answer that is taking its
/// time, so this is what stands between an open stream and a guest paused underneath it.
#[derive(Debug)]
pub struct OpenRequest {
    apps: Apps,
    app_id: AppId,
}

impl Drop for OpenRequest {
    fn drop(&mut self) {
        let mut apps = self.apps.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(app) = apps.get_mut(&self.app_id) {
            app.open = app.open.saturating_sub(1);
        }
    }
}

#[derive(Debug)]
pub struct ProxyMetrics {
    served: [AtomicU64; OUTCOMES.len()],
    answered: Histogram,
    in_flight: AtomicU64,
    handshakes: [AtomicU64; HANDSHAKES.len()],
    apps: Apps,
    durations: Mutex<BTreeMap<AppId, AppDurations>>,
}

impl Default for ProxyMetrics {
    fn default() -> Self {
        Self {
            served: Default::default(),
            answered: Histogram::over(&BUCKET_BOUNDS_SECONDS),
            in_flight: AtomicU64::new(0),
            handshakes: Default::default(),
            apps: Arc::new(Mutex::new(BTreeMap::new())),
            durations: Mutex::new(BTreeMap::new()),
        }
    }
}

impl ProxyMetrics {
    fn app(&self, app_id: &AppId, change: impl FnOnce(&mut AppProxy)) {
        let mut apps = self.apps.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        change(apps.entry(app_id.clone()).or_default());
    }

    pub fn open(&self, app_id: &AppId) -> OpenRequest {
        self.app(app_id, |app| app.open += 1);
        OpenRequest {
            apps: self.apps.clone(),
            app_id: app_id.clone(),
        }
    }

    pub fn open_requests_for(&self, app_id: &AppId) -> u64 {
        self.of(app_id).open
    }

    /// The apps with a request open, and how many each.
    pub fn open_requests(&self) -> BTreeMap<AppId, u64> {
        self.apps
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .iter()
            .filter(|(_, app)| app.open > 0)
            .map(|(app_id, app)| (app_id.clone(), app.open))
            .collect()
    }

    /// A request this proxy finished, with the status it answered and the app it was routed to,
    /// when it got that far.
    pub fn answered(&self, outcome: Outcome, status: StatusCode, took: Duration, app_id: Option<&AppId>) {
        let index = OUTCOMES.iter().position(|each| *each == outcome).unwrap_or(0);
        self.served[index].fetch_add(1, Ordering::Relaxed);
        self.answered.observe(took);
        if let Some(app_id) = app_id {
            self.app(app_id, |app| {
                app.request_count += 1;
                app.request_micros += took.as_micros() as u64;
            });
            if let Some(class) = timed_class_of(status) {
                self.durations
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .entry(app_id.clone())
                    .or_default()
                    .0[class]
                    .observe(took);
            }
        }
    }

    /// A request routed to an app came back with this status, or with none because the app
    /// could not be reached — which the proxy answers with a 502 of its own.
    pub fn app_answered(&self, app_id: &AppId, status: StatusCode, reached: bool) {
        self.app(app_id, |app| {
            app.requests[class_of(status)] += 1;
            if !reached {
                app.unreachable += 1;
            }
        });
    }

    pub fn began(&self) {
        self.in_flight.fetch_add(1, Ordering::Relaxed);
    }

    pub fn ended(&self) {
        self.in_flight.fetch_sub(1, Ordering::Relaxed);
    }

    pub fn handshake(&self, outcome: Handshake) {
        self.handshakes[position(&HANDSHAKES, &outcome)].fetch_add(1, Ordering::Relaxed);
    }

    pub fn raw_session(&self, app_id: &AppId, protocol: Protocol, outcome: RawOutcome) {
        self.app(app_id, |app| {
            app.raw_sessions[position(&PROTOCOLS, &protocol)][position(&RAW_OUTCOMES, &outcome)] += 1;
        });
    }

    pub fn raw_bytes(&self, app_id: &AppId, protocol: Protocol, from_client: u64, from_guest: u64) {
        self.app(app_id, |app| {
            app.raw_bytes_in[position(&PROTOCOLS, &protocol)] += from_client;
            app.raw_bytes_out[position(&PROTOCOLS, &protocol)] += from_guest;
        });
    }

    pub fn forget(&self, app_id: &AppId) {
        self.apps
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(app_id);
        self.durations
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(app_id);
    }

    pub fn of(&self, app_id: &AppId) -> AppProxy {
        self.apps
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(app_id)
            .cloned()
            .unwrap_or_default()
    }
}

static PROXY_REQUESTS_TOTAL: Metric = Metric {
    name: "nibrunner_proxy_requests_total",
    help: "Requests this proxy answered, by what it answered with.",
    kind: Kind::Counter,
    labels: &["outcome"],
};

static PROXY_REQUEST_DURATION_SECONDS: Metric = Metric {
    name: "nibrunner_proxy_request_duration_seconds",
    help: "Deciding a route and getting an answer back from the app. Ends when the response is handed on to be written, so it counts neither the handshake before it nor the write after.",
    kind: Kind::Histogram,
    labels: &[],
};

static PROXY_REQUESTS_IN_FLIGHT: Metric = Metric {
    name: "nibrunner_proxy_requests_in_flight",
    help: "Requests the proxy has taken and not yet answered.",
    kind: Kind::Gauge,
    labels: &[],
};

static PROXY_TLS_HANDSHAKES_TOTAL: Metric = Metric {
    name: "nibrunner_proxy_tls_handshakes_total",
    help: "TLS handshakes at the proxy, by how they ended.",
    kind: Kind::Counter,
    labels: &["outcome"],
};

static APP_REQUEST_DURATION_SECONDS: Metric = Metric {
    name: "nibrunner_app_request_duration_seconds",
    help: "What requests to one app cost this proxy. Divide the rate of the sum by the rate of the count for that app's mean.",
    kind: Kind::Summary,
    labels: &["app"],
};

static APP_REQUEST_SECONDS: Metric = Metric {
    name: "nibrunner_app_request_seconds",
    help: "How long requests to one app took this proxy to answer, by the class of the status it answered with — a 502 or 503 of the proxy's own included, a websocket's 101 not. Measured as the host-wide request duration is.",
    kind: Kind::Histogram,
    labels: &["app", "class"],
};

static APP_REQUESTS_TOTAL: Metric = Metric {
    name: "nibrunner_app_requests_total",
    help: "Requests routed to an app, by the class of the status that came back. A 502 for an app that could not be reached is the proxy's and counted apart below.",
    kind: Kind::Counter,
    labels: &["app", "class"],
};

static APP_REQUESTS_UNREACHABLE_TOTAL: Metric = Metric {
    name: "nibrunner_app_requests_unreachable_total",
    help: "Requests routed to an app that nothing answered, so the proxy answered 502 for it.",
    kind: Kind::Counter,
    labels: &["app"],
};

static APP_REQUESTS_OPEN: Metric = Metric {
    name: "nibrunner_app_requests_open",
    help: "Requests routed to an app that are still being answered: from arrival until the last byte of the response body, or until a websocket closes. An app with one open is not quiet.",
    kind: Kind::Gauge,
    labels: &["app"],
};

static RAW_PORT_SESSIONS_TOTAL: Metric = Metric {
    name: "nibrunner_raw_port_sessions_total",
    help: "Sessions on an app's raw ports — a TCP connection, or a UDP client address — by what came of them: relayed, the app was down, its wake was refused, or it would not take the session. Only for apps with a raw port.",
    kind: Kind::Counter,
    labels: &["app", "protocol", "outcome"],
};

static RAW_PORT_BYTES_TOTAL: Metric = Metric {
    name: "nibrunner_raw_port_bytes_total",
    help: "What the relay carried on an app's raw ports, by which way it went: in is towards the app. A TCP session's bytes are counted when it ends.",
    kind: Kind::Counter,
    labels: &["app", "protocol", "direction"],
};

pub(super) static DECLARED: &[&Metric] = &[
    &PROXY_REQUESTS_TOTAL,
    &PROXY_REQUEST_DURATION_SECONDS,
    &PROXY_REQUESTS_IN_FLIGHT,
    &PROXY_TLS_HANDSHAKES_TOTAL,
    &APP_REQUEST_DURATION_SECONDS,
    &APP_REQUEST_SECONDS,
    &APP_REQUESTS_TOTAL,
    &APP_REQUESTS_UNREACHABLE_TOTAL,
    &APP_REQUESTS_OPEN,
    &RAW_PORT_SESSIONS_TOTAL,
    &RAW_PORT_BYTES_TOTAL,
];

pub(super) fn render(page: &mut Page, metrics: &ProxyMetrics, snapshot: &HostSnapshot) {
    page.declare(&PROXY_REQUESTS_TOTAL);
    for (index, outcome) in OUTCOMES.iter().enumerate() {
        page.value(
            &PROXY_REQUESTS_TOTAL,
            &[("outcome", outcome.as_str())],
            metrics.served[index].load(Ordering::Relaxed),
        );
    }

    page.declare(&PROXY_REQUEST_DURATION_SECONDS);
    page.histogram(&PROXY_REQUEST_DURATION_SECONDS, &[], &metrics.answered);

    page.declare(&PROXY_REQUESTS_IN_FLIGHT);
    page.value(
        &PROXY_REQUESTS_IN_FLIGHT,
        &[],
        metrics.in_flight.load(Ordering::Relaxed),
    );

    page.declare(&PROXY_TLS_HANDSHAKES_TOTAL);
    for (index, handshake) in HANDSHAKES.iter().enumerate() {
        page.value(
            &PROXY_TLS_HANDSHAKES_TOTAL,
            &[("outcome", handshake.as_str())],
            metrics.handshakes[index].load(Ordering::Relaxed),
        );
    }

    let apps: Vec<&AppId> = snapshot.records.keys().collect();

    // A summary with no quantiles, which is a sum and a count: what each tenant cost, without
    // fifteen series apiece for the shape of it.
    page.declare(&APP_REQUEST_DURATION_SECONDS);
    for app_id in &apps {
        let app = metrics.of(app_id);
        page.summary(
            &APP_REQUEST_DURATION_SECONDS,
            &[("app", app_id.as_str())],
            app.request_micros as f64 / 1_000_000.0,
            app.request_count,
        );
    }

    page.declare(&APP_REQUEST_SECONDS);
    let unobserved = AppDurations::default();
    let durations = metrics
        .durations
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    for app_id in &apps {
        let app = durations.get(*app_id).unwrap_or(&unobserved);
        for (histogram, class) in app.0.iter().zip(TIMED_CLASSES) {
            page.histogram(
                &APP_REQUEST_SECONDS,
                &[("app", app_id.as_str()), ("class", class)],
                histogram,
            );
        }
    }
    drop(durations);

    page.declare(&APP_REQUESTS_TOTAL);
    for app_id in &apps {
        let app = metrics.of(app_id);
        for (index, class) in CLASSES.iter().enumerate() {
            page.value(
                &APP_REQUESTS_TOTAL,
                &[("app", app_id.as_str()), ("class", class)],
                app.requests[index],
            );
        }
    }

    page.declare(&APP_REQUESTS_UNREACHABLE_TOTAL);
    for app_id in &apps {
        page.value(
            &APP_REQUESTS_UNREACHABLE_TOTAL,
            &[("app", app_id.as_str())],
            metrics.of(app_id).unreachable,
        );
    }

    page.declare(&APP_REQUESTS_OPEN);
    for app_id in &apps {
        page.value(
            &APP_REQUESTS_OPEN,
            &[("app", app_id.as_str())],
            metrics.of(app_id).open,
        );
    }

    let with_raw_ports: Vec<&AppId> = snapshot
        .records
        .values()
        .filter(|record| !record.ports.is_empty())
        .map(|record| &record.app_id)
        .collect();

    page.declare(&RAW_PORT_SESSIONS_TOTAL);
    for app_id in &with_raw_ports {
        let app = metrics.of(app_id);
        for (protocol_index, protocol) in PROTOCOLS.iter().enumerate() {
            for (outcome_index, outcome) in RAW_OUTCOMES.iter().enumerate() {
                page.value(
                    &RAW_PORT_SESSIONS_TOTAL,
                    &[
                        ("app", app_id.as_str()),
                        ("protocol", protocol.as_str()),
                        ("outcome", outcome.as_str()),
                    ],
                    app.raw_sessions[protocol_index][outcome_index],
                );
            }
        }
    }

    page.declare(&RAW_PORT_BYTES_TOTAL);
    for app_id in &with_raw_ports {
        let app = metrics.of(app_id);
        for (index, protocol) in PROTOCOLS.iter().enumerate() {
            for (direction, bytes) in [("in", app.raw_bytes_in[index]), ("out", app.raw_bytes_out[index])] {
                page.value(
                    &RAW_PORT_BYTES_TOTAL,
                    &[
                        ("app", app_id.as_str()),
                        ("protocol", protocol.as_str()),
                        ("direction", direction),
                    ],
                    bytes,
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::metrics::tests::page;
    use crate::test_support::*;

    fn lines_for<'a>(page: &'a str, name: &str) -> Vec<&'a str> {
        page.lines()
            .filter(|line| line.starts_with(&format!("{name}{{")) || line.starts_with(&format!("{name} ")))
            .collect()
    }

    #[test]
    fn a_status_is_classed_by_its_hundreds_and_nothing_falls_outside_the_five() {
        assert_eq!(class_of(StatusCode::CONTINUE), 0);
        assert_eq!(class_of(StatusCode::OK), 1);
        assert_eq!(class_of(StatusCode::NOT_MODIFIED), 2);
        assert_eq!(class_of(StatusCode::NOT_FOUND), 3);
        assert_eq!(class_of(StatusCode::BAD_GATEWAY), 4);
        assert_eq!(class_of(StatusCode::from_u16(599).unwrap()), 4);
    }

    #[tokio::test]
    async fn what_each_app_was_asked_and_answered_is_on_its_own_series_and_raw_ports_only_where_there_are_any(
    ) {
        let host = test_host().await;
        let other = AppId::parse("app-2").unwrap();
        host.state.put_record(instance_record(|_| {})).await;
        host.state
            .put_record(instance_record(|record| {
                record.app_id = other.clone();
                record.ports = vec![crate::domain::report::instance_record::RecordPort {
                    name: protocol::PortName::parse("game").unwrap(),
                    host_port: protocol::HostPort::new(21001).unwrap(),
                    guest_port: protocol::GuestPort::new(7777).unwrap(),
                }];
            }))
            .await;
        let metrics = &host.metrics.proxy;
        metrics.answered(
            Outcome::Served,
            StatusCode::OK,
            Duration::from_millis(2),
            Some(&app_id()),
        );
        metrics.app_answered(&app_id(), StatusCode::OK, true);
        metrics.answered(
            Outcome::Unreachable,
            StatusCode::BAD_GATEWAY,
            Duration::from_millis(4),
            Some(&app_id()),
        );
        metrics.app_answered(&app_id(), StatusCode::BAD_GATEWAY, false);
        metrics.raw_session(&other, Protocol::Tcp, RawOutcome::Served);
        metrics.raw_bytes(&other, Protocol::Tcp, 100, 2_000);
        metrics.raw_session(&other, Protocol::Udp, RawOutcome::Refused);
        metrics.began();
        metrics.began();
        metrics.ended();
        metrics.handshake(Handshake::Completed);
        metrics.handshake(Handshake::TimedOut);
        let _still_answering = metrics.open(&app_id());

        let page = page(
            &crate::domain::metrics::tests::report(),
            &host.metrics,
            &host.state.snapshot().await,
            0,
        );
        assert!(page.contains("nibrunner_proxy_requests_total{outcome=\"served\"} 1\n"));
        assert!(page.contains("nibrunner_proxy_requests_total{outcome=\"unreachable\"} 1\n"));
        assert!(
            page.contains("nibrunner_proxy_request_duration_seconds_count 2\n"),
            "the proxy answered both, and how long a failed dial took is worth knowing"
        );
        assert!(page.contains("nibrunner_proxy_requests_in_flight 1\n"));
        assert!(page.contains("nibrunner_proxy_tls_handshakes_total{outcome=\"completed\"} 1\n"));
        assert!(page.contains("nibrunner_proxy_tls_handshakes_total{outcome=\"failed\"} 0\n"));
        assert!(page.contains("nibrunner_proxy_tls_handshakes_total{outcome=\"timed_out\"} 1\n"));
        let requests = lines_for(&page, "nibrunner_app_requests_total");
        assert_eq!(
            requests.len(),
            2 * CLASSES.len(),
            "every class for every app: {requests:?}"
        );
        assert!(requests.contains(&"nibrunner_app_requests_total{app=\"app-1\",class=\"2xx\"} 1"));
        assert!(requests.contains(&"nibrunner_app_requests_total{app=\"app-1\",class=\"5xx\"} 1"));
        assert!(requests.contains(&"nibrunner_app_requests_total{app=\"app-2\",class=\"2xx\"} 0"));
        assert!(page.contains("nibrunner_app_requests_unreachable_total{app=\"app-1\"} 1\n"));
        assert!(page.contains("nibrunner_app_requests_open{app=\"app-1\"} 1\n"));
        assert!(page.contains("nibrunner_app_requests_open{app=\"app-2\"} 0\n"));
        assert!(page.contains("nibrunner_app_request_duration_seconds_sum{app=\"app-1\"} 0.006\n"));
        assert!(page.contains("nibrunner_app_request_duration_seconds_count{app=\"app-1\"} 2\n"));

        let sessions = lines_for(&page, "nibrunner_raw_port_sessions_total");
        assert!(
            sessions.iter().all(|line| line.contains("app=\"app-2\"")),
            "{sessions:?}"
        );
        assert_eq!(sessions.len(), PROTOCOLS.len() * RAW_OUTCOMES.len());
        assert!(sessions.contains(
            &"nibrunner_raw_port_sessions_total{app=\"app-2\",protocol=\"tcp\",outcome=\"served\"} 1"
        ));
        assert!(sessions.contains(
            &"nibrunner_raw_port_sessions_total{app=\"app-2\",protocol=\"udp\",outcome=\"refused\"} 1"
        ));
        let bytes = lines_for(&page, "nibrunner_raw_port_bytes_total");
        assert!(bytes.contains(
            &"nibrunner_raw_port_bytes_total{app=\"app-2\",protocol=\"tcp\",direction=\"in\"} 100"
        ));
        assert!(bytes.contains(
            &"nibrunner_raw_port_bytes_total{app=\"app-2\",protocol=\"tcp\",direction=\"out\"} 2000"
        ));
        assert!(bytes
            .contains(&"nibrunner_raw_port_bytes_total{app=\"app-2\",protocol=\"udp\",direction=\"in\"} 0"));
    }

    fn holding_the_app() -> HostSnapshot {
        HostSnapshot {
            records: BTreeMap::from([(app_id(), instance_record(|_| {}))]),
            ..HostSnapshot::default()
        }
    }

    fn rendered_with_the_app(metrics: &ProxyMetrics) -> String {
        let mut page = Page::new();
        render(&mut page, metrics, &holding_the_app());
        page.0
    }

    #[test]
    fn what_each_app_s_requests_took_is_a_distribution_by_the_class_it_was_answered_with() {
        let metrics = ProxyMetrics::default();
        let app = Some(&app_id());
        metrics.answered(Outcome::Served, StatusCode::OK, Duration::from_millis(2), app);
        metrics.answered(Outcome::Served, StatusCode::OK, Duration::from_millis(30), app);
        metrics.answered(
            Outcome::Served,
            StatusCode::NOT_FOUND,
            Duration::from_millis(3),
            app,
        );
        metrics.answered(
            Outcome::Unreachable,
            StatusCode::BAD_GATEWAY,
            Duration::from_secs(20),
            app,
        );
        metrics.answered(
            Outcome::Served,
            StatusCode::SWITCHING_PROTOCOLS,
            Duration::from_millis(1),
            app,
        );
        metrics.answered(
            Outcome::NoSuchHost,
            StatusCode::NOT_FOUND,
            Duration::from_millis(1),
            None,
        );

        let page = rendered_with_the_app(&metrics);
        let series = lines_for(&page, "nibrunner_app_request_seconds_bucket");
        assert_eq!(
            series.len(),
            TIMED_CLASSES.len() * (APP_BUCKET_BOUNDS_SECONDS.len() + 1),
            "every class of every app, answered or not, and no 1xx: {series:?}"
        );
        for expected in [
            r#"nibrunner_app_request_seconds_bucket{app="app-1",class="2xx",le="0.005"} 1"#,
            r#"nibrunner_app_request_seconds_bucket{app="app-1",class="2xx",le="0.025"} 1"#,
            r#"nibrunner_app_request_seconds_bucket{app="app-1",class="2xx",le="0.05"} 2"#,
            r#"nibrunner_app_request_seconds_bucket{app="app-1",class="2xx",le="+Inf"} 2"#,
            r#"nibrunner_app_request_seconds_bucket{app="app-1",class="3xx",le="+Inf"} 0"#,
            r#"nibrunner_app_request_seconds_bucket{app="app-1",class="4xx",le="0.005"} 1"#,
            r#"nibrunner_app_request_seconds_bucket{app="app-1",class="5xx",le="10"} 0"#,
            r#"nibrunner_app_request_seconds_bucket{app="app-1",class="5xx",le="+Inf"} 1"#,
        ] {
            assert!(series.contains(&expected), "{expected} in {series:?}");
        }
        assert!(page.contains("nibrunner_app_request_seconds_sum{app=\"app-1\",class=\"2xx\"} 0.032\n"));
        assert!(page.contains("nibrunner_app_request_seconds_count{app=\"app-1\",class=\"4xx\"} 1\n"));
        assert!(page.contains("nibrunner_app_request_seconds_count{app=\"app-1\",class=\"5xx\"} 1\n"));
    }

    #[test]
    fn an_app_the_document_dropped_takes_its_distribution_with_it() {
        let metrics = ProxyMetrics::default();
        metrics.answered(
            Outcome::Served,
            StatusCode::OK,
            Duration::from_millis(2),
            Some(&app_id()),
        );
        assert!(rendered_with_the_app(&metrics)
            .contains("nibrunner_app_request_seconds_count{app=\"app-1\",class=\"2xx\"} 1\n"));
        metrics.forget(&app_id());

        let page = rendered_with_the_app(&metrics);
        assert!(
            page.contains("nibrunner_app_request_seconds_count{app=\"app-1\",class=\"2xx\"} 0\n"),
            "an app named again starts from nothing: {page}"
        );
    }

    #[test]
    fn an_app_the_document_dropped_is_forgotten() {
        let metrics = ProxyMetrics::default();
        metrics.app_answered(&app_id(), StatusCode::OK, true);
        assert_eq!(metrics.of(&app_id()).requests[1], 1);
        metrics.forget(&app_id());
        assert_eq!(metrics.of(&app_id()), AppProxy::default());
    }

    #[test]
    fn a_request_is_open_on_its_app_from_when_it_is_taken_until_it_is_let_go() {
        let metrics = ProxyMetrics::default();
        let other = AppId::parse("app-2").unwrap();
        assert!(metrics.open_requests().is_empty());

        let first = metrics.open(&app_id());
        let second = metrics.open(&app_id());
        assert_eq!(metrics.open_requests_for(&app_id()), 2);
        assert_eq!(
            metrics.open_requests(),
            BTreeMap::from([(app_id(), 2)]),
            "an app with nothing open is not listed"
        );
        assert_eq!(metrics.open_requests_for(&other), 0);

        drop(first);
        assert_eq!(metrics.open_requests_for(&app_id()), 1);
        drop(second);
        assert_eq!(metrics.open_requests_for(&app_id()), 0);
        assert!(metrics.open_requests().is_empty());
    }

    #[test]
    fn a_request_still_open_on_an_app_the_document_dropped_goes_with_it() {
        let metrics = ProxyMetrics::default();
        let held = metrics.open(&app_id());
        metrics.forget(&app_id());
        assert_eq!(metrics.open_requests_for(&app_id()), 0);
        drop(held);
        assert_eq!(metrics.of(&app_id()), AppProxy::default());
    }
}
