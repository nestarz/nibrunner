use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use protocol::{CheckpointState, ExportState, VolumeState};

use crate::domain::metrics::{as_seconds, Histogram, Kind, Metric, Page, Scrape};

// A local-file volume is made in milliseconds; one on an object store is made in seconds and
// exported in minutes, and a layer pulled over a slow link in more.
const BUCKET_BOUNDS_SECONDS: [f64; 14] = [
    0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 300.0, 900.0,
];

/// Something done to storage on an app's behalf, each timed apart because each is a different
/// backend call with a different reason to be slow.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Operation {
    VolumeProvision,
    VolumeAttach,
    VolumeTeardown,
    CheckpointCreate,
    CheckpointDelete,
    ExportWrite,
    LayerFetch,
}

impl Operation {
    pub fn as_str(self) -> &'static str {
        match self {
            Operation::VolumeProvision => "volume_provision",
            Operation::VolumeAttach => "volume_attach",
            Operation::VolumeTeardown => "volume_teardown",
            Operation::CheckpointCreate => "checkpoint_create",
            Operation::CheckpointDelete => "checkpoint_delete",
            Operation::ExportWrite => "export_write",
            Operation::LayerFetch => "layer_fetch",
        }
    }
}

const OPERATIONS: [Operation; 7] = [
    Operation::VolumeProvision,
    Operation::VolumeAttach,
    Operation::VolumeTeardown,
    Operation::CheckpointCreate,
    Operation::CheckpointDelete,
    Operation::ExportWrite,
    Operation::LayerFetch,
];

const OUTCOMES: [&str; 2] = ["ok", "failed"];

/// Why a pass refused a start, in the word the counter uses for it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StartRefusal {
    NoRoom,
    NotIsolated,
}

impl StartRefusal {
    pub fn as_str(self) -> &'static str {
        match self {
            StartRefusal::NoRoom => "no_room",
            StartRefusal::NotIsolated => "not_isolated",
        }
    }
}

const START_REFUSALS: [StartRefusal; 2] = [StartRefusal::NoRoom, StartRefusal::NotIsolated];

const VOLUME_STATES: [VolumeState; 5] = [
    VolumeState::Pending,
    VolumeState::Ready,
    VolumeState::Detached,
    VolumeState::Deleted,
    VolumeState::Failed,
];

const CHECKPOINT_STATES: [CheckpointState; 4] = [
    CheckpointState::Pending,
    CheckpointState::Ready,
    CheckpointState::Deleted,
    CheckpointState::Failed,
];

const EXPORT_STATES: [ExportState; 5] = [
    ExportState::Pending,
    ExportState::Preparing,
    ExportState::Ready,
    ExportState::Failed,
    ExportState::Expired,
];

fn volume_state_str(state: VolumeState) -> &'static str {
    match state {
        VolumeState::Pending => "pending",
        VolumeState::Ready => "ready",
        VolumeState::Detached => "detached",
        VolumeState::Deleted => "deleted",
        VolumeState::Failed => "failed",
    }
}

fn checkpoint_state_str(state: CheckpointState) -> &'static str {
    match state {
        CheckpointState::Pending => "pending",
        CheckpointState::Ready => "ready",
        CheckpointState::Deleted => "deleted",
        CheckpointState::Failed => "failed",
    }
}

fn export_state_str(state: ExportState) -> &'static str {
    match state {
        ExportState::Pending => "pending",
        ExportState::Preparing => "preparing",
        ExportState::Ready => "ready",
        ExportState::Failed => "failed",
        ExportState::Expired => "expired",
    }
}

fn position<T: PartialEq>(of: &[T], value: &T) -> usize {
    of.iter().position(|each| each == value).unwrap_or(0)
}

/// What storage and the artifact store cost this host: every operation timed by how it ended,
/// what the layer cache saved, and the starts the host's memory turned away.
#[derive(Debug)]
pub struct ResourceMetrics {
    operations: Vec<Histogram>,
    layers_cached: AtomicU64,
    layer_fetch_bytes: AtomicU64,
    start_refusals: Vec<AtomicU64>,
}

impl Default for ResourceMetrics {
    fn default() -> Self {
        Self {
            operations: (0..OPERATIONS.len() * OUTCOMES.len())
                .map(|_| Histogram::over(&BUCKET_BOUNDS_SECONDS))
                .collect(),
            layers_cached: AtomicU64::new(0),
            layer_fetch_bytes: AtomicU64::new(0),
            start_refusals: START_REFUSALS.iter().map(|_| AtomicU64::new(0)).collect(),
        }
    }
}

impl ResourceMetrics {
    fn operation(&self, operation: Operation, ok: bool) -> &Histogram {
        &self.operations[position(&OPERATIONS, &operation) * OUTCOMES.len() + usize::from(!ok)]
    }

    pub fn done(&self, operation: Operation, ok: bool, took: Duration) {
        self.operation(operation, ok).observe(took);
    }

    pub fn layer_fetched(&self, bytes: u64, took: Duration) {
        self.layer_fetch_bytes.fetch_add(bytes, Ordering::Relaxed);
        self.done(Operation::LayerFetch, true, took);
    }

    pub fn layer_cached(&self) {
        self.layers_cached.fetch_add(1, Ordering::Relaxed);
    }

    pub fn starts_refused(&self, refusal: StartRefusal, count: usize) {
        self.start_refusals[position(&START_REFUSALS, &refusal)].fetch_add(count as u64, Ordering::Relaxed);
    }
}

static STORAGE_OPERATION_SECONDS: Metric = Metric {
    name: "nibrunner_storage_operation_seconds",
    help: "Something done to storage or the artifact store on an app's behalf, by what and how it ended.",
    kind: Kind::Histogram,
    labels: &["operation", "outcome"],
};

static LAYERS_CACHED_TOTAL: Metric = Metric {
    name: "nibrunner_layers_cached_total",
    help: "Layers a pass asked for that were already in the cache, so nothing was fetched.",
    kind: Kind::Counter,
    labels: &[],
};

static LAYER_FETCH_BYTES_TOTAL: Metric = Metric {
    name: "nibrunner_layer_fetch_bytes_total",
    help: "What has been pulled from the artifact store, as the store sized it.",
    kind: Kind::Counter,
    labels: &[],
};

static VOLUME_STATE: Metric = Metric {
    name: "nibrunner_volume_state",
    help: "1 for the state a volume is in, 0 for every state it is not.",
    kind: Kind::Gauge,
    labels: &["volume", "app", "state"],
};

static VOLUME_SIZE_BYTES: Metric = Metric {
    name: "nibrunner_volume_size_bytes",
    help: "What a volume was set aside as.",
    kind: Kind::Gauge,
    labels: &["volume", "app"],
};

static VOLUME_USED_BYTES: Metric = Metric {
    name: "nibrunner_volume_used_bytes",
    help: "What a guest reported filling of its volume, when it was last measured. Absent until it has been.",
    kind: Kind::Gauge,
    labels: &["volume", "app"],
};

static CHECKPOINT_STATE: Metric = Metric {
    name: "nibrunner_checkpoint_state",
    help: "1 for the state a checkpoint is in, 0 for every state it is not.",
    kind: Kind::Gauge,
    labels: &["checkpoint", "volume", "state"],
};

static EXPORT_STATE: Metric = Metric {
    name: "nibrunner_export_state",
    help: "1 for the state an export is in, 0 for every state it is not.",
    kind: Kind::Gauge,
    labels: &["export", "state"],
};

static EXPORT_SIZE_BYTES: Metric = Metric {
    name: "nibrunner_export_size_bytes",
    help: "What an export came to, once written. Absent until it has been.",
    kind: Kind::Gauge,
    labels: &["export"],
};

static SLOTS: Metric = Metric {
    name: "nibrunner_slots",
    help: "Slots on this host: each holds an app's ports, tap and guest address, and an app with none is refused. The total is max_apps in config.toml, or the desired state's maxApps when that is more.",
    kind: Kind::Gauge,
    labels: &["of"],
};

static HOST_MEMORY_AVAILABLE_BYTES: Metric = Metric {
    name: "nibrunner_host_memory_available_bytes",
    help: "What the kernel says it could give out without swapping, as against the promises nibrunner_host_allocatable adds up. Absent where the kernel does not say.",
    kind: Kind::Gauge,
    labels: &[],
};

static START_REFUSALS_TOTAL: Metric = Metric {
    name: "nibrunner_start_refusals_total",
    help: "Starts a pass refused, by why: no room in memory beside what is already up, or the isolation ruleset not applied. One refused for room waits, pending, and is planned again the next pass.",
    kind: Kind::Counter,
    labels: &["reason"],
};

static APP_MEASURED_TIMESTAMP_SECONDS: Metric = Metric {
    name: "nibrunner_app_measured_timestamp_seconds",
    help: "When a guest last reported what it was using, as seconds since the epoch. 0 for one that never has; one that stopped is one this host can no longer hear.",
    kind: Kind::Gauge,
    labels: &["app"],
};

pub(super) static DECLARED: &[&Metric] = &[
    &STORAGE_OPERATION_SECONDS,
    &LAYERS_CACHED_TOTAL,
    &LAYER_FETCH_BYTES_TOTAL,
    &VOLUME_STATE,
    &VOLUME_SIZE_BYTES,
    &VOLUME_USED_BYTES,
    &CHECKPOINT_STATE,
    &EXPORT_STATE,
    &EXPORT_SIZE_BYTES,
    &SLOTS,
    &HOST_MEMORY_AVAILABLE_BYTES,
    &START_REFUSALS_TOTAL,
    &APP_MEASURED_TIMESTAMP_SECONDS,
];

pub(super) fn render(page: &mut Page, metrics: &ResourceMetrics, scrape: &Scrape<'_>) {
    page.declare(&STORAGE_OPERATION_SECONDS);
    for operation in OPERATIONS {
        for (index, outcome) in OUTCOMES.iter().enumerate() {
            page.histogram(
                &STORAGE_OPERATION_SECONDS,
                &[("operation", operation.as_str()), ("outcome", outcome)],
                metrics.operation(operation, index == 0),
            );
        }
    }

    page.declare(&LAYERS_CACHED_TOTAL);
    page.value(
        &LAYERS_CACHED_TOTAL,
        &[],
        metrics.layers_cached.load(Ordering::Relaxed),
    );

    page.declare(&LAYER_FETCH_BYTES_TOTAL);
    page.value(
        &LAYER_FETCH_BYTES_TOTAL,
        &[],
        metrics.layer_fetch_bytes.load(Ordering::Relaxed),
    );

    page.declare(&VOLUME_STATE);
    for volume in &scrape.report.volumes {
        for state in VOLUME_STATES {
            page.value(
                &VOLUME_STATE,
                &[
                    ("volume", volume.volume_id.as_str()),
                    ("app", volume.app_id.as_str()),
                    ("state", volume_state_str(state)),
                ],
                u8::from(volume.state == state),
            );
        }
    }

    page.declare(&VOLUME_SIZE_BYTES);
    for volume in &scrape.report.volumes {
        page.value(
            &VOLUME_SIZE_BYTES,
            &[
                ("volume", volume.volume_id.as_str()),
                ("app", volume.app_id.as_str()),
            ],
            volume.size_bytes,
        );
    }

    page.declare(&VOLUME_USED_BYTES);
    for volume in &scrape.report.volumes {
        if let Some(usage) = scrape.snapshot.volume_usage.get(&volume.app_id) {
            page.value(
                &VOLUME_USED_BYTES,
                &[
                    ("volume", volume.volume_id.as_str()),
                    ("app", volume.app_id.as_str()),
                ],
                usage.used_bytes,
            );
        }
    }

    page.declare(&CHECKPOINT_STATE);
    for checkpoint in &scrape.report.checkpoints {
        for state in CHECKPOINT_STATES {
            page.value(
                &CHECKPOINT_STATE,
                &[
                    ("checkpoint", checkpoint.checkpoint_id.as_str()),
                    ("volume", checkpoint.volume_id.as_str()),
                    ("state", checkpoint_state_str(state)),
                ],
                u8::from(checkpoint.state == state),
            );
        }
    }

    page.declare(&EXPORT_STATE);
    for export in &scrape.report.exports {
        for state in EXPORT_STATES {
            page.value(
                &EXPORT_STATE,
                &[
                    ("export", export.export_id.as_str()),
                    ("state", export_state_str(state)),
                ],
                u8::from(export.state == state),
            );
        }
    }

    page.declare(&EXPORT_SIZE_BYTES);
    for export in &scrape.report.exports {
        if let Some(size_bytes) = export.size_bytes {
            page.value(
                &EXPORT_SIZE_BYTES,
                &[("export", export.export_id.as_str())],
                size_bytes,
            );
        }
    }

    page.declare(&SLOTS);
    page.value(&SLOTS, &[("of", "used")], scrape.slots_used);
    page.value(&SLOTS, &[("of", "total")], scrape.slots_total);

    page.declare(&HOST_MEMORY_AVAILABLE_BYTES);
    if let Some(bytes) = scrape.memory_available_bytes {
        page.value(&HOST_MEMORY_AVAILABLE_BYTES, &[], bytes);
    }

    page.declare(&START_REFUSALS_TOTAL);
    for (index, refusal) in START_REFUSALS.iter().enumerate() {
        page.value(
            &START_REFUSALS_TOTAL,
            &[("reason", refusal.as_str())],
            metrics.start_refusals[index].load(Ordering::Relaxed),
        );
    }

    page.declare(&APP_MEASURED_TIMESTAMP_SECONDS);
    for instance in &scrape.report.instances {
        page.value(
            &APP_MEASURED_TIMESTAMP_SECONDS,
            &[("app", instance.app_id.as_str())],
            as_seconds(
                scrape
                    .snapshot
                    .compute_usage
                    .get(&instance.app_id)
                    .map_or(0, |compute| compute.measured_at.epoch_ms().max(0) as u64),
            ),
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::metrics::{render, HostMetrics};
    use crate::state::HostSnapshot;
    use crate::test_support::*;
    use protocol::{ComputeUsage, FilesystemUsage, ReportedCheckpoint, ReportedExport, Timestamp};

    fn lines_for<'a>(page: &'a str, name: &str) -> Vec<&'a str> {
        page.lines()
            .filter(|line| line.starts_with(&format!("{name}{{")) || line.starts_with(&format!("{name} ")))
            .collect()
    }

    #[test]
    fn what_the_host_holds_beyond_its_guests_is_on_the_page_with_what_each_cost() {
        let metrics = HostMetrics::default();
        metrics
            .resources
            .done(Operation::VolumeProvision, true, Duration::from_secs(3));
        metrics
            .resources
            .done(Operation::ExportWrite, false, Duration::from_secs(40));
        metrics.resources.layer_fetched(1_024, Duration::from_millis(300));
        metrics.resources.layer_cached();
        metrics.resources.layer_cached();
        metrics.resources.starts_refused(StartRefusal::NoRoom, 742);

        let mut report = crate::domain::metrics::tests::report();
        report.volumes = vec![reported_volume(|_| {})];
        report.checkpoints = vec![ReportedCheckpoint {
            checkpoint_id: checkpoint_id(),
            volume_id: volume_id(),
            state: protocol::CheckpointState::Ready,
            reference: None,
            ready_at: None,
            message: None,
        }];
        report.exports = vec![ReportedExport {
            export_id: export_id(),
            checkpoint_id: None,
            state: protocol::ExportState::Failed,
            size_bytes: None,
            ready_at: None,
            message: None,
        }];
        report.instances = vec![reported_instance(|_| {})];
        let snapshot = HostSnapshot {
            volume_usage: [(
                app_id(),
                FilesystemUsage {
                    total_bytes: 4_096,
                    used_bytes: 512,
                    measured_at: observed_at(),
                },
            )]
            .into_iter()
            .collect(),
            compute_usage: [(
                app_id(),
                ComputeUsage {
                    memory_total_bytes: 1,
                    memory_used_bytes: 1,
                    cpu_share: None,
                    measured_at: Timestamp::from_epoch_ms(1_700_000_000_000),
                },
            )]
            .into_iter()
            .collect(),
            ..HostSnapshot::default()
        };
        let page = render(
            &metrics,
            &Scrape {
                report: &report,
                snapshot: &snapshot,
                now_ms: 0,
                slots_used: 7,
                slots_total: 63,
                memory_available_bytes: Some(1_000_000),
                conntrack: Some(crate::domain::metrics::Conntrack {
                    used: 210_000,
                    max: 262_144,
                }),
            },
        );

        assert!(page.contains(
            "nibrunner_storage_operation_seconds_count{operation=\"volume_provision\",outcome=\"ok\"} 1\n"
        ));
        assert!(page.contains(
            "nibrunner_storage_operation_seconds_count{operation=\"export_write\",outcome=\"failed\"} 1\n"
        ));
        assert!(page.contains(
            "nibrunner_storage_operation_seconds_count{operation=\"layer_fetch\",outcome=\"ok\"} 1\n"
        ));
        assert!(page.contains("nibrunner_layers_cached_total 2\n"));
        assert!(page.contains("nibrunner_layer_fetch_bytes_total 1024\n"));
        let states = lines_for(&page, "nibrunner_volume_state");
        assert_eq!(states.len(), VOLUME_STATES.len());
        assert!(states.contains(&"nibrunner_volume_state{volume=\"vol-1\",app=\"app-1\",state=\"ready\"} 1"));
        assert!(page.contains("nibrunner_volume_size_bytes{volume=\"vol-1\",app=\"app-1\"} 4096\n"));
        assert!(page.contains("nibrunner_volume_used_bytes{volume=\"vol-1\",app=\"app-1\"} 512\n"));
        assert!(page.contains(
            "nibrunner_checkpoint_state{checkpoint=\"chk-1\",volume=\"vol-1\",state=\"ready\"} 1\n"
        ));
        assert!(page.contains("nibrunner_export_state{export=\"exp-1\",state=\"failed\"} 1\n"));
        assert!(
            lines_for(&page, "nibrunner_export_size_bytes").is_empty(),
            "not written, no size"
        );
        assert!(page.contains("nibrunner_slots{of=\"used\"} 7\n"));
        assert!(
            page.contains("nibrunner_slots{of=\"total\"} 63\n"),
            "the total is what the host is laid out for"
        );
        assert!(page.contains("nibrunner_host_memory_available_bytes 1000000\n"));
        assert!(page.contains("nibrunner_start_refusals_total{reason=\"no_room\"} 742\n"));
        assert!(page.contains("nibrunner_start_refusals_total{reason=\"not_isolated\"} 0\n"));
        assert!(page.contains("nibrunner_conntrack_entries{of=\"used\"} 210000\n"));
        assert!(page.contains("nibrunner_conntrack_entries{of=\"max\"} 262144\n"));
        assert!(page.contains("nibrunner_app_measured_timestamp_seconds{app=\"app-1\"} 1700000000.000\n"));
    }

    #[test]
    fn what_the_kernel_does_not_say_and_what_was_never_measured_are_left_out_and_zero_in_turn() {
        let metrics = HostMetrics::default();
        let mut report = crate::domain::metrics::tests::report();
        report.volumes = vec![reported_volume(|_| {})];
        report.instances = vec![reported_instance(|_| {})];
        let snapshot = HostSnapshot::default();
        let page = render(
            &metrics,
            &Scrape {
                report: &report,
                snapshot: &snapshot,
                now_ms: 0,
                slots_used: 0,
                slots_total: 1000,
                memory_available_bytes: None,
                conntrack: None,
            },
        );
        assert!(lines_for(&page, "nibrunner_host_memory_available_bytes").is_empty());
        assert!(lines_for(&page, "nibrunner_conntrack_entries").is_empty());
        assert!(lines_for(&page, "nibrunner_volume_used_bytes").is_empty());
        assert!(page.contains("nibrunner_app_measured_timestamp_seconds{app=\"app-1\"} 0.000\n"));
    }
}
