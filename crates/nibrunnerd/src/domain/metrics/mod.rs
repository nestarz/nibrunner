pub mod conntrack;
pub mod converge;
pub mod health;
pub mod passes;
pub mod proxy;
pub mod resources;
pub mod sleep_wake;

pub use conntrack::{Conntrack, ConntrackWatch};
pub use proxy::{Outcome, ProxyMetrics};

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use protocol::{HostReportedState, INSTANCE_STATES};

use crate::domain::metrics::converge::ConvergeMetrics;
use crate::domain::metrics::health::HealthMetrics;
use crate::domain::metrics::passes::PassMetrics;
use crate::domain::metrics::resources::ResourceMetrics;
use crate::domain::metrics::sleep_wake::SleepWakeMetrics;
use crate::state::HostSnapshot;

/// Everything this daemon counts in memory rather than reads off the report. One per host, and
/// a scrape renders it beside the report it is rendered with.
#[derive(Debug, Default)]
pub struct HostMetrics {
    pub proxy: ProxyMetrics,
    pub sleep_wake: SleepWakeMetrics,
    pub converge: ConvergeMetrics,
    pub passes: PassMetrics,
    pub health: HealthMetrics,
    pub resources: ResourceMetrics,
    pub conntrack: ConntrackWatch,
}

/// What a scrape is rendered from besides what the daemon counted: the report as it stands, the
/// snapshot it was built from, and what only the moment of the scrape can say.
pub struct Scrape<'a> {
    pub report: &'a HostReportedState,
    pub snapshot: &'a HostSnapshot,
    pub now_ms: i64,
    pub slots_used: usize,
    pub slots_total: u32,
    pub memory_available_bytes: Option<u64>,
    pub conntrack: Option<Conntrack>,
}

impl HostMetrics {
    pub fn new() -> Self {
        Self::default()
    }

    /// The document no longer names the app, so nothing kept per app is kept for it.
    pub fn forget(&self, app_id: &protocol::AppId) {
        self.sleep_wake.forget(app_id);
        self.health.forget(app_id);
        self.proxy.forget(app_id);
    }
}

/// Counts by bucket, the way Prometheus reads a histogram back: cumulative, with the ones that
/// outran every bound still in the count and the sum.
#[derive(Debug)]
pub struct Histogram {
    bounds: &'static [f64],
    buckets: Vec<AtomicU64>,
    above_every_bucket: AtomicU64,
    total_micros: AtomicU64,
}

impl Histogram {
    pub fn over(bounds: &'static [f64]) -> Self {
        Self {
            bounds,
            buckets: bounds.iter().map(|_| AtomicU64::new(0)).collect(),
            above_every_bucket: AtomicU64::new(0),
            total_micros: AtomicU64::new(0),
        }
    }

    pub fn observe(&self, took: Duration) {
        self.total_micros
            .fetch_add(took.as_micros() as u64, Ordering::Relaxed);
        let seconds = took.as_secs_f64();
        match self.bounds.iter().position(|bound| seconds <= *bound) {
            Some(bucket) => self.buckets[bucket].fetch_add(1, Ordering::Relaxed),
            None => self.above_every_bucket.fetch_add(1, Ordering::Relaxed),
        };
    }

    pub fn count(&self) -> u64 {
        self.buckets
            .iter()
            .map(|count| count.load(Ordering::Relaxed))
            .sum::<u64>()
            + self.above_every_bucket.load(Ordering::Relaxed)
    }

    fn seconds(&self) -> f64 {
        self.total_micros.load(Ordering::Relaxed) as f64 / 1_000_000.0
    }
}

// A label's value is the only place tenant-shaped text reaches a scraper, and the exposition format
// ends a line on a newline and a value on a quote.
fn escaped(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for character in value.chars() {
        match character {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            other => out.push(other),
        }
    }
    out
}

// Prometheus counts time in seconds and this host meters it in milliseconds. Rendered rather than
// rounded, so a series that is read back and multiplied out is the figure the report carries.
pub(crate) fn as_seconds(ms: u64) -> String {
    format!("{}.{:03}", ms / 1_000, ms % 1_000)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    Counter,
    Gauge,
    Histogram,
    Summary,
}

impl Kind {
    pub fn as_str(self) -> &'static str {
        match self {
            Kind::Counter => "counter",
            Kind::Gauge => "gauge",
            Kind::Histogram => "histogram",
            Kind::Summary => "summary",
        }
    }
}

/// A series as a scraper meets it: the name it is read by, the sentence it is published with, the
/// kind it is rated as, and the labels every sample of it carries. Declared once, because a name
/// written where it is described and again where it is emitted can differ by a character and the
/// exposition format will carry both — `# HELP` and `# TYPE` are optional, so a page whose two
/// halves disagree scrapes without complaint and answers nothing.
///
/// A histogram's `labels` are the ones its caller passes; `le` is this page's to add.
#[derive(Debug)]
pub struct Metric {
    pub name: &'static str,
    pub help: &'static str,
    pub kind: Kind,
    pub labels: &'static [&'static str],
}

pub(crate) struct Page(String);

impl Page {
    fn new() -> Self {
        Self(String::new())
    }

    pub(crate) fn declare(&mut self, metric: &Metric) {
        let (name, help, kind) = (metric.name, metric.help, metric.kind.as_str());
        self.0
            .push_str(&format!("# HELP {name} {help}\n# TYPE {name} {kind}\n"));
    }

    pub(crate) fn value(&mut self, metric: &Metric, labels: &[(&str, &str)], value: impl std::fmt::Display) {
        debug_assert!(
            labels
                .iter()
                .map(|(key, _)| *key)
                .eq(metric.labels.iter().copied()),
            "{} is declared with {:?} and was given {:?}",
            metric.name,
            metric.labels,
            labels.iter().map(|(key, _)| *key).collect::<Vec<_>>()
        );
        self.sample(metric.name, labels, value);
    }

    fn sample(&mut self, name: &str, labels: &[(&str, &str)], value: impl std::fmt::Display) {
        if labels.is_empty() {
            self.0.push_str(&format!("{name} {value}\n"));
            return;
        }
        let rendered: Vec<String> = labels
            .iter()
            .map(|(key, value)| format!("{key}=\"{}\"", escaped(value)))
            .collect();
        self.0
            .push_str(&format!("{name}{{{}}} {value}\n", rendered.join(",")));
    }

    pub(crate) fn summary(&mut self, metric: &Metric, labels: &[(&str, &str)], seconds: f64, count: u64) {
        debug_assert!(
            labels
                .iter()
                .map(|(key, _)| *key)
                .eq(metric.labels.iter().copied()),
            "{} is declared with {:?} and was given {:?}",
            metric.name,
            metric.labels,
            labels.iter().map(|(key, _)| *key).collect::<Vec<_>>()
        );
        self.sample(&format!("{}_sum", metric.name), labels, seconds);
        self.sample(&format!("{}_count", metric.name), labels, count);
    }

    pub(crate) fn histogram(&mut self, metric: &Metric, labels: &[(&str, &str)], histogram: &Histogram) {
        debug_assert!(
            labels
                .iter()
                .map(|(key, _)| *key)
                .eq(metric.labels.iter().copied()),
            "{} is declared with {:?} and was given {:?}",
            metric.name,
            metric.labels,
            labels.iter().map(|(key, _)| *key).collect::<Vec<_>>()
        );
        let name = metric.name;
        let bucket = format!("{name}_bucket");
        let mut running = 0u64;
        for (index, bound) in histogram.bounds.iter().enumerate() {
            running += histogram.buckets[index].load(Ordering::Relaxed);
            let mut with_bound = labels.to_vec();
            let bound = bound.to_string();
            with_bound.push(("le", &bound));
            self.sample(&bucket, &with_bound, running);
        }
        let mut with_inf = labels.to_vec();
        with_inf.push(("le", "+Inf"));
        self.sample(&bucket, &with_inf, histogram.count());
        self.sample(&format!("{name}_sum"), labels, histogram.seconds());
        self.sample(&format!("{name}_count"), labels, histogram.count());
    }
}

/// An app this host holds a record of but has not metered yet has used nothing, and nothing is a
/// number rather than an absence.
fn metered(snapshot: &HostSnapshot, app_id: &protocol::AppId) -> protocol::UsageMeters {
    snapshot.meters.get(app_id).copied().unwrap_or_default()
}

static UP: Metric = Metric {
    name: "nibrunner_up",
    help: "Whether this daemon answered the scrape.",
    kind: Kind::Gauge,
    labels: &[],
};

static HOST_CAPACITY: Metric = Metric {
    name: "nibrunner_host_capacity",
    help: "What the host has, by resource.",
    kind: Kind::Gauge,
    labels: &["resource"],
};

static HOST_ALLOCATABLE: Metric = Metric {
    name: "nibrunner_host_allocatable",
    help: "What the host has left to give a new app.",
    kind: Kind::Gauge,
    labels: &["resource"],
};

static APP_STATE: Metric = Metric {
    name: "nibrunner_app_state",
    help: "1 for the state an app is in, 0 for every state it is not.",
    kind: Kind::Gauge,
    labels: &["app", "state"],
};

static APP_RESTARTS_TOTAL: Metric = Metric {
    name: "nibrunner_app_restarts_total",
    help:
        "Times an app's tenant has been restarted inside its guest since the host last booted the app afresh.",
    kind: Kind::Counter,
    labels: &["app"],
};

static APP_MEMORY_USED_BYTES: Metric = Metric {
    name: "nibrunner_app_memory_used_bytes",
    help: "What a guest reported using, when it was last measured.",
    kind: Kind::Gauge,
    labels: &["app"],
};

static APP_CPU_SHARE: Metric = Metric {
    name: "nibrunner_app_cpu_share",
    help: "The share of one vCPU a guest was using, when it was last measured.",
    kind: Kind::Gauge,
    labels: &["app"],
};

static APP_TIME_SECONDS_TOTAL: Metric = Metric {
    name: "nibrunner_app_time_seconds_total",
    help: "How long an app has been held, by what it was holding: memory while it runs, a snapshot on disk while it sleeps.",
    kind: Kind::Counter,
    labels: &["app", "holding"],
};

static APP_CPU_SECONDS_TOTAL: Metric = Metric {
    name: "nibrunner_app_cpu_seconds_total",
    help: "What an app's guest has reported spending, summed across the vCPUs it was given.",
    kind: Kind::Counter,
    labels: &["app"],
};

static APP_NETWORK_BYTES_TOTAL: Metric = Metric {
    name: "nibrunner_app_network_bytes_total",
    help: "What has crossed an app's tap, by which way it went. Only what was let out is counted as sent.",
    kind: Kind::Counter,
    labels: &["app", "direction"],
};

static APP_DISK_MIB_SECONDS_TOTAL: Metric = Metric {
    name: "nibrunner_app_disk_mib_seconds_total",
    help: "What an app has held on disk over time: what was set aside for it, and what its guest reported filling.",
    kind: Kind::Counter,
    labels: &["app", "disk"],
};

pub(super) static DECLARED: &[&Metric] = &[
    &UP,
    &HOST_CAPACITY,
    &HOST_ALLOCATABLE,
    &APP_STATE,
    &APP_RESTARTS_TOTAL,
    &APP_MEMORY_USED_BYTES,
    &APP_CPU_SHARE,
    &APP_TIME_SECONDS_TOTAL,
    &APP_CPU_SECONDS_TOTAL,
    &APP_NETWORK_BYTES_TOTAL,
    &APP_DISK_MIB_SECONDS_TOTAL,
];

/// Every series this page publishes, in the order it renders them. What is declared here is what
/// the reference the docs site renders is written from, and a test holds the page to emitting all
/// of it.
pub fn declared() -> Vec<&'static Metric> {
    [
        DECLARED,
        proxy::DECLARED,
        sleep_wake::DECLARED,
        passes::DECLARED,
        converge::DECLARED,
        health::DECLARED,
        resources::DECLARED,
        conntrack::DECLARED,
    ]
    .concat()
}

/// One sample of every series [`declared`], rendered by the page that renders the real ones.
///
/// What promtool rates a page by is its samples: a series that is only described is not one it can
/// hold to the naming rules, and a page of descriptions alone passes its lint saying nothing. The
/// labels are the declared ones because a label's name is what the rules are about; the values are
/// not, and are placeholders.
pub fn page_of_every_series() -> String {
    let unobserved = Histogram::over(&[]);
    let mut page = Page::new();
    for metric in declared() {
        let labels: Vec<(&str, &str)> = metric.labels.iter().map(|label| (*label, "x")).collect();
        page.declare(metric);
        match metric.kind {
            Kind::Counter | Kind::Gauge => page.value(metric, &labels, 0),
            Kind::Histogram => page.histogram(metric, &labels, &unobserved),
            Kind::Summary => page.summary(metric, &labels, 0.0, 0),
        }
    }
    page.0
}

pub fn render(metrics: &HostMetrics, scrape: &Scrape<'_>) -> String {
    let (report, snapshot, now_ms) = (scrape.report, scrape.snapshot, scrape.now_ms);
    let mut page = Page::new();

    page.declare(&UP);
    page.value(&UP, &[], 1);

    page.declare(&HOST_CAPACITY);
    for (resource, value) in [
        ("vcpu", u64::from(report.capacity.vcpu_count)),
        ("memory_mib", report.capacity.memory_mib),
        ("cache_bytes", report.capacity.cache_bytes),
    ] {
        page.value(&HOST_CAPACITY, &[("resource", resource)], value);
    }

    page.declare(&HOST_ALLOCATABLE);
    for (resource, value) in [
        ("vcpu", u64::from(report.allocatable.vcpu_count)),
        ("memory_mib", report.allocatable.memory_mib),
        ("cache_bytes", report.allocatable.cache_bytes),
    ] {
        page.value(&HOST_ALLOCATABLE, &[("resource", resource)], value);
    }

    // One series per state rather than a number standing for one, so a query reads as the word the
    // report uses and a state nothing is in is a zero rather than a gap.
    page.declare(&APP_STATE);
    for instance in &report.instances {
        for state in INSTANCE_STATES {
            page.value(
                &APP_STATE,
                &[("app", instance.app_id.as_str()), ("state", state.as_str())],
                u8::from(instance.state == state),
            );
        }
    }

    page.declare(&APP_RESTARTS_TOTAL);
    for instance in &report.instances {
        page.value(
            &APP_RESTARTS_TOTAL,
            &[("app", instance.app_id.as_str())],
            instance.restart_count,
        );
    }

    page.declare(&APP_MEMORY_USED_BYTES);
    for instance in &report.instances {
        if let Some(compute) = snapshot.compute_usage.get(&instance.app_id) {
            page.value(
                &APP_MEMORY_USED_BYTES,
                &[("app", instance.app_id.as_str())],
                compute.memory_used_bytes,
            );
        }
    }

    page.declare(&APP_CPU_SHARE);
    for instance in &report.instances {
        if let Some(share) = snapshot
            .compute_usage
            .get(&instance.app_id)
            .and_then(|compute| compute.cpu_share)
        {
            page.value(&APP_CPU_SHARE, &[("app", instance.app_id.as_str())], share);
        }
    }

    // The only place these are published. They are totals rather than rates on purpose: what a
    // period cost is the figure at its end less the figure at its start, so a scrape nobody took
    // is a resolution nobody has rather than usage nobody billed.
    page.declare(&APP_TIME_SECONDS_TOTAL);
    for instance in &report.instances {
        let meters = metered(snapshot, &instance.app_id);
        for (holding, ms) in [("running", meters.running_ms), ("idle", meters.idle_ms)] {
            page.value(
                &APP_TIME_SECONDS_TOTAL,
                &[("app", instance.app_id.as_str()), ("holding", holding)],
                as_seconds(ms),
            );
        }
    }

    page.declare(&APP_CPU_SECONDS_TOTAL);
    for instance in &report.instances {
        page.value(
            &APP_CPU_SECONDS_TOTAL,
            &[("app", instance.app_id.as_str())],
            as_seconds(metered(snapshot, &instance.app_id).cpu_ms),
        );
    }

    page.declare(&APP_NETWORK_BYTES_TOTAL);
    for instance in &report.instances {
        let meters = metered(snapshot, &instance.app_id);
        for (direction, bytes) in [("rx", meters.rx_bytes), ("tx", meters.tx_bytes)] {
            page.value(
                &APP_NETWORK_BYTES_TOTAL,
                &[("app", instance.app_id.as_str()), ("direction", direction)],
                bytes,
            );
        }
    }

    // Mebibyte-seconds rather than bytes, and named for it: what a volume holds is a level, so
    // what accumulates is that level multiplied by how long it was held, and the byte-milliseconds
    // that would be the base-unit form of it outrun a 64-bit counter on a large volume.
    page.declare(&APP_DISK_MIB_SECONDS_TOTAL);
    for instance in &report.instances {
        let meters = metered(snapshot, &instance.app_id);
        for (disk, held) in [
            ("provisioned", meters.disk_provisioned_mib_seconds),
            ("used", meters.disk_used_mib_seconds),
        ] {
            page.value(
                &APP_DISK_MIB_SECONDS_TOTAL,
                &[("app", instance.app_id.as_str()), ("disk", disk)],
                held,
            );
        }
    }

    proxy::render(&mut page, &metrics.proxy, snapshot);
    sleep_wake::render(&mut page, &metrics.sleep_wake, snapshot);
    passes::render(&mut page, report, &metrics.passes, snapshot);
    converge::render(&mut page, report, &metrics.converge, &snapshot.deploys, now_ms);
    health::render(&mut page, report, &metrics.health, snapshot);
    resources::render(&mut page, &metrics.resources, scrape);
    conntrack::render(&mut page, scrape.conntrack);

    page.0
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use hyper::StatusCode;
    use protocol::{
        ComputeUsage, HostCapacity, HostId, HostReportedState, HostState, HostVersions, Timestamp,
    };

    pub(crate) fn report() -> HostReportedState {
        HostReportedState {
            host_id: HostId::parse("host-1").unwrap(),
            reported_at: Timestamp::from_epoch_ms(0),
            state: HostState::Ready,
            capacity: HostCapacity {
                vcpu_count: 12,
                memory_mib: 64_000,
                cache_bytes: 1_000,
            },
            allocatable: HostCapacity {
                vcpu_count: 6,
                memory_mib: 32_000,
                cache_bytes: 500,
            },
            versions: HostVersions {
                agent: "0.1.0".into(),
                guest_image: "test".into(),
                zerofs: "none".into(),
                firecracker: "v1".into(),
            },
            volumes: vec![],
            instances: vec![],
            checkpoints: vec![],
            exports: vec![],
            accepted_digest: None,
            accepted_revision: None,
            message: None,
        }
    }

    /// A page rendered as a scrape would render it, with nothing the scrape alone can say.
    pub(crate) fn page(
        report: &HostReportedState,
        metrics: &HostMetrics,
        snapshot: &HostSnapshot,
        now_ms: i64,
    ) -> String {
        render(
            metrics,
            &Scrape {
                report,
                snapshot,
                now_ms,
                slots_used: 0,
                slots_total: 1000,
                memory_available_bytes: None,
                conntrack: None,
            },
        )
    }

    fn rendered(report: &HostReportedState, metrics: &HostMetrics) -> String {
        page(report, metrics, &HostSnapshot::default(), 0)
    }

    /// A scrape of a host holding one of everything, so that a series rendered only when there
    /// is something to render it for is still rendered.
    fn page_of_a_busy_host() -> String {
        use crate::test_support::*;
        let metrics = HostMetrics::new();
        metrics.proxy.answered(
            Outcome::Served,
            StatusCode::OK,
            Duration::from_millis(2),
            Some(&app_id()),
        );
        metrics.proxy.raw_bytes(&app_id(), proxy::Protocol::Tcp, 1, 2);
        metrics
            .proxy
            .raw_session(&app_id(), proxy::Protocol::Tcp, proxy::RawOutcome::Served);
        metrics.sleep_wake.woke(Duration::from_millis(10), false);
        metrics.sleep_wake.woken(
            &app_id(),
            crate::ports::WakeOutcome::Restored,
            Duration::from_millis(10),
            Duration::from_millis(5),
        );
        metrics.sleep_wake.slept(
            &app_id(),
            crate::domain::activation::SleepReason::Quiet,
            sleep_wake::SleepOutcome::Slept,
            Duration::from_millis(1),
            Duration::from_millis(2),
        );
        metrics.health.probed(&app_id(), true, Duration::from_millis(1));
        metrics.resources.layer_cached();

        let mut report = report();
        report.instances = vec![reported_instance(|_| {})];
        report.volumes = vec![reported_volume(|_| {})];
        report.checkpoints = vec![protocol::ReportedCheckpoint {
            checkpoint_id: checkpoint_id(),
            volume_id: volume_id(),
            state: protocol::CheckpointState::Ready,
            reference: None,
            ready_at: None,
            message: None,
        }];
        report.exports = vec![protocol::ReportedExport {
            export_id: export_id(),
            checkpoint_id: None,
            state: protocol::ExportState::Ready,
            size_bytes: Some(2048),
            ready_at: None,
            message: None,
        }];
        let snapshot = HostSnapshot {
            records: [(
                app_id(),
                instance_record(|record| {
                    record.on_request = true;
                    record.ports = vec![crate::domain::report::instance_record::RecordPort {
                        name: protocol::PortName::parse("ssh").unwrap(),
                        host_port: protocol::HostPort::new(22_000).unwrap(),
                        guest_port: protocol::GuestPort::new(22).unwrap(),
                    }];
                }),
            )]
            .into_iter()
            .collect(),
            meters: [(app_id(), protocol::UsageMeters::default())]
                .into_iter()
                .collect(),
            compute_usage: [(
                app_id(),
                ComputeUsage {
                    memory_total_bytes: 100,
                    memory_used_bytes: 40,
                    cpu_share: Some(0.25),
                    measured_at: Timestamp::from_epoch_ms(0),
                },
            )]
            .into_iter()
            .collect(),
            deploys: [(
                app_id(),
                converge::Deploy {
                    deployment_id: deployment_id(),
                    desired_state: protocol::DesiredInstanceState::OnRequest,
                    cause: converge::Cause::Change,
                    detected_at_ms: 0,
                    layers_ready_at_ms: Some(0),
                    volume_ready_at_ms: Some(0),
                    booted_at_ms: Some(0),
                    converged_at_ms: Some(1),
                },
            )]
            .into_iter()
            .collect(),
            volume_usage: [(
                app_id(),
                protocol::FilesystemUsage {
                    total_bytes: 4096,
                    used_bytes: 512,
                    measured_at: Timestamp::from_epoch_ms(0),
                },
            )]
            .into_iter()
            .collect(),
            ..HostSnapshot::default()
        };
        render(
            &metrics,
            &Scrape {
                report: &report,
                snapshot: &snapshot,
                now_ms: 1,
                slots_used: 1,
                slots_total: 1000,
                memory_available_bytes: Some(1_000_000),
                conntrack: Some(Conntrack {
                    used: 1,
                    max: 262_144,
                }),
            },
        )
    }

    // A name written where a series is described and again where it is emitted can differ by a
    // character, and the exposition format carries both halves without complaint. Declared once
    // and held to here, so a series that is described and never emitted fails rather than reads
    // as a scraper finding nothing.
    #[test]
    fn every_series_this_page_declares_is_one_it_emits() {
        let page = page_of_a_busy_host();
        for metric in declared() {
            let emitted = match metric.kind {
                Kind::Histogram | Kind::Summary => format!("{}_count", metric.name),
                Kind::Counter | Kind::Gauge => metric.name.to_string(),
            };
            assert!(
                page.lines().any(|line| {
                    line.starts_with(&format!("{emitted}{{")) || line.starts_with(&format!("{emitted} "))
                }),
                "{} is declared and never emitted",
                metric.name
            );
        }
    }

    fn lines_for<'a>(page: &'a str, name: &str) -> Vec<&'a str> {
        page.lines()
            .filter(|line| line.starts_with(name) && !line.starts_with('#'))
            .collect()
    }

    #[test]
    fn a_wake_is_counted_apart_from_the_requests_it_is_the_tail_of() {
        let metrics = HostMetrics::new();
        metrics
            .proxy
            .answered(Outcome::Served, StatusCode::OK, Duration::from_millis(2), None);
        metrics
            .proxy
            .answered(Outcome::Served, StatusCode::OK, Duration::from_millis(150), None);
        metrics.sleep_wake.woke(Duration::from_millis(148), false);
        let page = rendered(&report(), &metrics);

        assert!(page.contains("nibrunner_wake_duration_seconds_count 1"));
        assert!(page.contains("nibrunner_proxy_request_duration_seconds_count 2"));
    }

    #[test]
    fn the_two_halves_of_a_sleep_are_measured_apart_because_they_cost_nothing_alike() {
        let metrics = HostMetrics::new();
        metrics.sleep_wake.snapshotted(Duration::from_millis(2563));
        metrics.sleep_wake.restored(Duration::from_millis(8));
        let page = rendered(&report(), &metrics);

        assert!(page.contains("nibrunner_vm_snapshot_duration_seconds_count 1"));
        assert!(page.contains("nibrunner_vm_restore_duration_seconds_count 1"));
        assert!(page.contains("nibrunner_vm_snapshot_duration_seconds_sum 2.563"));
        assert!(page.contains("nibrunner_vm_restore_duration_seconds_sum 0.008"));
    }

    /// A snapshot holding a record for each app named, since every per-app series is keyed by
    /// the records the host holds.
    fn holding(app_ids: &[&str]) -> HostSnapshot {
        HostSnapshot {
            records: app_ids
                .iter()
                .map(|app_id| {
                    let app_id = protocol::AppId::parse(*app_id).unwrap();
                    (
                        app_id.clone(),
                        crate::test_support::instance_record(|record| record.app_id = app_id),
                    )
                })
                .collect(),
            ..Default::default()
        }
    }

    #[test]
    fn what_one_app_cost_is_a_sum_and_a_count_and_its_distribution_is_a_series_apart() {
        let metrics = HostMetrics::new();
        let app = protocol::AppId::parse("app-1").unwrap();
        metrics.proxy.answered(
            Outcome::Served,
            StatusCode::OK,
            Duration::from_millis(10),
            Some(&app),
        );
        metrics.proxy.answered(
            Outcome::Served,
            StatusCode::OK,
            Duration::from_millis(30),
            Some(&app),
        );
        let page = page(&report(), &metrics, &holding(&["app-1"]), 0);

        assert!(page.contains(r#"nibrunner_app_request_duration_seconds_count{app="app-1"} 2"#));
        assert!(page.contains(r#"nibrunner_app_request_duration_seconds_sum{app="app-1"} 0.04"#));
        assert!(
            lines_for(&page, "nibrunner_app_request_duration_seconds_bucket").is_empty(),
            "the summary keeps its shape; the buckets are nibrunner_app_request_seconds'"
        );
        assert!(page.contains(r#"nibrunner_app_request_seconds_count{app="app-1",class="2xx"} 2"#));
    }

    #[test]
    fn an_app_this_host_has_stopped_serving_keeps_no_series_of_its_own() {
        let metrics = HostMetrics::new();
        let gone = protocol::AppId::parse("app-1").unwrap();
        let kept = protocol::AppId::parse("app-2").unwrap();
        metrics.proxy.answered(
            Outcome::Served,
            StatusCode::OK,
            Duration::from_millis(10),
            Some(&gone),
        );
        metrics.proxy.answered(
            Outcome::Served,
            StatusCode::OK,
            Duration::from_millis(10),
            Some(&kept),
        );

        metrics.forget(&gone);
        let page = page(&report(), &metrics, &holding(&["app-2"]), 0);

        assert!(!page.contains(r#"app="app-1""#), "a departed app kept a series");
        assert!(page.contains(r#"nibrunner_app_request_duration_seconds_count{app="app-2"} 1"#));
        assert_eq!(
            metrics.proxy.of(&gone),
            proxy::AppProxy::default(),
            "and nothing of its own is kept"
        );
        // What it cost is still in the host-wide total; only its own line goes.
        assert!(page.contains("nibrunner_proxy_request_duration_seconds_count 2"));
    }

    #[test]
    fn every_series_is_introduced_before_it_is_given_a_value() {
        let page = page_of_a_busy_host();
        for line in page
            .lines()
            .filter(|line| !line.starts_with('#') && !line.is_empty())
        {
            let name = line
                .split(['{', ' '])
                .next()
                .unwrap()
                .trim_end_matches("_bucket")
                .trim_end_matches("_sum")
                .trim_end_matches("_count");
            assert!(
                page.contains(&format!("# TYPE {name} ")),
                "{name} is given a value with no TYPE above it"
            );
            assert!(page.contains(&format!("# HELP {name} ")), "{name} has no HELP");
        }
    }

    // A family introduced twice is a page Prometheus refuses whole, and a sample is one of its
    // family only under its family's name — so a page with everything on it is checked for both.
    #[test]
    fn no_family_is_introduced_twice_and_every_sample_sits_under_its_own_family() {
        let mut report = report();
        report.instances = vec![crate::test_support::reported_instance(|_| {})];
        report.volumes = vec![crate::test_support::reported_volume(|_| {})];
        let snapshot = HostSnapshot {
            records: std::collections::BTreeMap::from([(
                crate::test_support::app_id(),
                crate::test_support::instance_record(|_| {}),
            )]),
            ..Default::default()
        };
        let page = page(&report, &HostMetrics::default(), &snapshot, 0);

        let mut families = Vec::new();
        let mut current: Option<String> = None;
        for line in page.lines().filter(|line| !line.is_empty()) {
            if let Some(rest) = line.strip_prefix("# TYPE ") {
                let name = rest.split(' ').next().unwrap().to_string();
                assert!(!families.contains(&name), "{name} is introduced twice");
                families.push(name.clone());
                current = Some(name);
            } else if !line.starts_with('#') {
                let family = current.as_deref().expect("a sample under a family");
                let name = line.split(['{', ' ']).next().unwrap();
                assert!(
                    name == family
                        || name.strip_suffix("_bucket") == Some(family)
                        || name.strip_suffix("_sum") == Some(family)
                        || name.strip_suffix("_count") == Some(family),
                    "{name} sits under {family}"
                );
            }
        }
        assert!(families.len() > 40, "{}", families.len());
    }

    #[test]
    fn a_histogram_counts_every_request_once_and_its_buckets_only_grow() {
        let proxy = HostMetrics::new();
        for micros in [500, 3_000, 40_000, 90_000_000] {
            proxy.proxy.answered(
                Outcome::Served,
                StatusCode::OK,
                Duration::from_micros(micros),
                None,
            );
        }
        let page = rendered(&report(), &proxy);

        let counts: Vec<u64> = lines_for(&page, "nibrunner_proxy_request_duration_seconds_bucket")
            .iter()
            .map(|line| line.rsplit(' ').next().unwrap().parse().unwrap())
            .collect();
        assert!(
            counts.windows(2).all(|pair| pair[0] <= pair[1]),
            "buckets are cumulative: {counts:?}"
        );
        assert_eq!(counts.last().copied(), Some(4), "+Inf holds every request");

        let count = lines_for(&page, "nibrunner_proxy_request_duration_seconds_count")[0];
        assert!(count.ends_with(" 4"), "{count}");
        // The one that outran every bound is still in the sum and the count, only not in a bucket.
        assert_eq!(counts[counts.len() - 2], 3);
    }

    #[test]
    fn an_app_is_in_one_state_and_reported_as_not_being_in_the_others() {
        let mut state = report();
        state.instances = vec![crate::test_support::reported_instance(|instance| {
            instance.state = protocol::InstanceState::Idle;
            instance.restart_count = 2;
        })];
        let snapshot = HostSnapshot {
            compute_usage: [(
                crate::test_support::app_id(),
                ComputeUsage {
                    memory_total_bytes: 100,
                    memory_used_bytes: 40,
                    cpu_share: Some(0.25),
                    measured_at: Timestamp::from_epoch_ms(0),
                },
            )]
            .into_iter()
            .collect(),
            ..HostSnapshot::default()
        };
        let page = page(&state, &HostMetrics::new(), &snapshot, 0);

        let states = lines_for(&page, "nibrunner_app_state");
        assert_eq!(states.len(), INSTANCE_STATES.len());
        assert_eq!(states.iter().filter(|line| line.ends_with(" 1")).count(), 1);
        assert!(states
            .iter()
            .any(|line| line.contains("state=\"idle\"") && line.ends_with(" 1")));
        assert!(lines_for(&page, "nibrunner_app_restarts_total")[0].ends_with(" 2"));
        assert!(lines_for(&page, "nibrunner_app_cpu_share")[0].ends_with(" 0.25"));
    }

    #[test]
    fn what_an_app_has_used_is_exposed_as_counters_in_the_units_a_scraper_reads() {
        let mut state = report();
        state.instances = vec![crate::test_support::reported_instance(|_| {})];
        let snapshot = HostSnapshot {
            meters: [(
                crate::test_support::app_id(),
                protocol::UsageMeters {
                    running_ms: 3_600_000,
                    idle_ms: 1_500,
                    cpu_ms: 42_150,
                    rx_bytes: 1_073_741_824,
                    tx_bytes: 2_147_483_648,
                    disk_provisioned_mib_seconds: 29_491_200,
                    disk_used_mib_seconds: 5_242_880,
                },
            )]
            .into_iter()
            .collect(),
            ..HostSnapshot::default()
        };
        let page = page(&state, &HostMetrics::new(), &snapshot, 0);

        let time = lines_for(&page, "nibrunner_app_time_seconds_total");
        assert_eq!(time.len(), 2, "memory and disk are counted apart: {time:?}");
        assert!(time
            .iter()
            .any(|line| line.contains("holding=\"running\"") && line.ends_with(" 3600.000")));
        assert!(time
            .iter()
            .any(|line| line.contains("holding=\"idle\"") && line.ends_with(" 1.500")));
        assert!(lines_for(&page, "nibrunner_app_cpu_seconds_total")[0].ends_with(" 42.150"));
        let network = lines_for(&page, "nibrunner_app_network_bytes_total");
        assert_eq!(network.len(), 2, "each way is counted apart: {network:?}");
        assert!(network
            .iter()
            .any(|line| line.contains("direction=\"rx\"") && line.ends_with(" 1073741824")));
        assert!(network
            .iter()
            .any(|line| line.contains("direction=\"tx\"") && line.ends_with(" 2147483648")));
    }

    #[test]
    fn an_app_that_has_used_nothing_is_a_zero_rather_than_a_missing_series() {
        let mut state = report();
        state.instances = vec![crate::test_support::reported_instance(|_| {})];
        let page = rendered(&state, &HostMetrics::new());
        for name in [
            "nibrunner_app_time_seconds_total",
            "nibrunner_app_cpu_seconds_total",
            "nibrunner_app_network_bytes_total",
            "nibrunner_app_disk_mib_seconds_total",
        ] {
            assert!(!lines_for(&page, name).is_empty(), "{name} is missing");
        }
    }

    #[test]
    fn a_label_cannot_end_the_line_it_is_written_on() {
        assert_eq!(escaped(r#"a"b"#), r#"a\"b"#);
        assert_eq!(escaped("a\nb"), "a\\nb");
        assert_eq!(escaped(r"a\b"), r"a\\b");
    }
}
