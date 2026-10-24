//! Tenant output and traces, shipped to a control plane's bucket in the objects it reads
//! (`logs/<app>/live-<host>.jsonl.gz`, `logs/<app>/<yyyymmddhh>/<mmss>-<host>.jsonl.gz`, and the
//! same two under `traces/`): gzipped JSON lines, sealed under its fleet key. An instance is
//! shipped only once its document labels it with an `app`; every line carries that label, `rev`
//! and `context`, and its unit (the app id).
use std::collections::{BTreeMap, VecDeque};
use std::io::Write;
use std::sync::{Arc, Mutex};

use chacha20poly1305::aead::{Aead, KeyInit, Payload};
use chacha20poly1305::{XChaCha20Poly1305, XNonce};
use hmac::{Hmac, Mac};
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutOptions, PutPayload};
use protocol::{AppId, DeploymentId, HostDesiredState, InstanceLabels, TenantLogStream};
use serde::{Deserialize, Serialize};
use sha2::Sha256;

use crate::adapters::proxy::encode::Encoding;
use crate::adapters::proxy::http_log::{HttpRequestRecord, WakeKind};
use crate::domain::metrics::rollup::{hour_key, micros_rfc3339, mmss};
use crate::domain::metrics::CacheOutcome;
use crate::ports::{TenantLogBody, TenantLogEvent};

pub const LIVE_WINDOW_S: i64 = 120;
pub const LIVE_MAX_BYTES: usize = 256 << 10;
pub const CHUNK_S: i64 = 300;
pub const PENDING_MAX_BYTES: usize = 4 << 20;
/// Raw bytes tried for a live tail per compressed byte it may weigh: JSON lines compress past this.
const LIVE_RATIO: usize = 8;
/// Later seconds a chunk may be named after when its first second's name is taken.
const NAME_TRIES: i64 = 5;

#[derive(Debug, thiserror::Error)]
pub enum TelemetryError {
    #[error("the telemetry store failed: {0}")]
    Store(#[from] object_store::Error),
    #[error("every name of the chunk of {app} starting at {start} is taken")]
    Taken { app: String, start: i64 },
    #[error("the seal key {path} could not be read as 32 bytes: {reason}")]
    SealKey { path: String, reason: String },
    #[error("the telemetry sink could not be opened: {0}")]
    Open(String),
    #[error("the telemetry sink {0} names no fleets/<fleet> to seal under")]
    NoFleet(String),
}

/// The fleet key, as the control plane wrote it: 32 raw bytes.
#[derive(Clone)]
pub struct SealKey([u8; 32]);

impl SealKey {
    pub fn read(path: &std::path::Path) -> Result<Self, TelemetryError> {
        let refused = |reason: String| TelemetryError::SealKey {
            path: path.display().to_string(),
            reason,
        };
        let bytes = std::fs::read(path).map_err(|error| refused(error.to_string()))?;
        let key = bytes
            .try_into()
            .map_err(|b: Vec<u8>| refused(format!("{} bytes", b.len())))?;
        Ok(Self(key))
    }

    /// The control plane's seal: the nonce is `HMAC-SHA256(key, "nonce" ‖ len(aad) ‖ aad ‖
    /// plaintext)`, so an object sealed again unchanged is the same bytes.
    pub fn seal(&self, aad: &str, plaintext: &[u8]) -> Vec<u8> {
        let mut mac = <Hmac<Sha256> as Mac>::new_from_slice(&self.0).expect("HMAC takes a key of any length");
        mac.update(b"nonce");
        mac.update(&(aad.len() as u64).to_be_bytes());
        mac.update(aad.as_bytes());
        mac.update(plaintext);
        let nonce = mac.finalize().into_bytes();
        let nonce = XNonce::from_slice(&nonce[..24]);
        let payload = Payload {
            msg: plaintext,
            aad: aad.as_bytes(),
        };
        let sealed = XChaCha20Poly1305::new((&self.0).into())
            .encrypt(nonce, payload)
            .expect("XChaCha20-Poly1305 fails only past 256 GiB");
        [nonce.as_slice(), &sealed].concat()
    }
}

/// A control plane's fleet in a bucket: `<url>` ends in `fleets/<fleet>`, every key goes under it
/// and a sealed object authenticates `<fleet>/<key>`.
pub struct Bucket {
    store: Arc<dyn ObjectStore>,
    prefix: Option<String>,
    fleet: String,
}

impl Bucket {
    pub fn open(url: &str) -> Result<Self, TelemetryError> {
        let fleet = fleet_of(url).ok_or_else(|| TelemetryError::NoFleet(url.into()))?;
        let (store, prefix) = crate::adapters::artifact_store::open_store(url)
            .map_err(|error| TelemetryError::Open(error.message()))?;
        Ok(Self { store, prefix, fleet })
    }

    pub fn new(store: Arc<dyn ObjectStore>, prefix: Option<String>, fleet: &str) -> Self {
        Self {
            store,
            prefix,
            fleet: fleet.into(),
        }
    }

    fn path(&self, key: &str) -> object_store::path::Path {
        match &self.prefix {
            Some(prefix) => format!("{prefix}/{key}").into(),
            None => key.into(),
        }
    }

    /// Whether it was written: `create` refuses a key already there.
    pub async fn put(&self, key: &str, body: Vec<u8>, create: bool) -> Result<bool, TelemetryError> {
        let mode = if create {
            PutMode::Create
        } else {
            PutMode::Overwrite
        };
        let options = PutOptions::from(mode);
        match self
            .store
            .put_opts(&self.path(key), PutPayload::from(body), options)
            .await
        {
            Ok(_) => Ok(true),
            Err(object_store::Error::AlreadyExists { .. } | object_store::Error::Precondition { .. }) => {
                Ok(false)
            }
            Err(error) => Err(error.into()),
        }
    }

    pub async fn put_sealed(
        &self,
        key: &str,
        plain: &[u8],
        seal: &SealKey,
        create: bool,
    ) -> Result<bool, TelemetryError> {
        let aad = format!("{}/{key}", self.fleet);
        self.put(key, seal.seal(&aad, plain), create).await
    }

    pub async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, TelemetryError> {
        match self.store.get(&self.path(key)).await {
            Ok(got) => Ok(Some(got.bytes().await?.to_vec())),
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(error) => Err(error.into()),
        }
    }
}

fn fleet_of(url: &str) -> Option<String> {
    let mut segments = url.trim_end_matches('/').rsplit('/');
    let fleet = segments.next().filter(|fleet| !fleet.is_empty())?;
    (segments.next() == Some("fleets")).then(|| fleet.to_string())
}

/// One line as the control plane reads it: field order and omissions are the wire.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct LogLine {
    pub t: String,
    pub host: String,
    pub unit: String,
    pub rev: String,
    pub context: String,
    pub k: LogKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub msg: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub method: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<u16>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dur_us: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wake_us: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dropped: Option<u64>,
    /// The cron job, and its run, that printed the line.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub job: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) cache: Option<CacheOutcome>,
    /// The coding the response left the proxy in.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) encoding: Option<Encoding>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub severity: Option<Severity>,
    /// The trace and span the guest's runtime put on the record, or the cron attempt that printed
    /// the line; never guessed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trace: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub span: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attrs: Option<Attributes>,
}

impl LogLine {
    fn labelled(host: String, unit: String, rev: String, context: String) -> Self {
        Self {
            host,
            unit,
            rev,
            context,
            ..Self::default()
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LogKind {
    #[default]
    Stdout,
    Stderr,
    /// A log record the guest's runtime exported over OTLP.
    Otel,
    /// A request, as hosts before traces wrote it; a reader still meets it in older chunks.
    Req,
    Gap,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum Severity {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
    Fatal,
}

/// Allow-listed span and record attributes: strings, numbers and booleans only.
pub type Attributes = BTreeMap<String, serde_json::Value>;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SpanKind {
    Server,
    Client,
    #[default]
    Internal,
    Producer,
    Consumer,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OtelStatus {
    Unset,
    Ok,
    Error,
}

/// The proxy's root span carries the HTTP status; a guest's span its OpenTelemetry one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum SpanStatus {
    Http(u16),
    Otel(OtelStatus),
}

/// One span as the control plane reads it under `traces/`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct SpanLine {
    pub trace: String,
    pub span: String,
    pub parent: Option<String>,
    pub kind: SpanKind,
    pub name: String,
    pub t: String,
    pub end: String,
    pub host: String,
    pub unit: String,
    pub rev: String,
    pub context: String,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub root: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub method: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<SpanStatus>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status_msg: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deployment: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) wake: Option<WakeKind>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wake_us: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) cache: Option<CacheOutcome>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) encoding: Option<Encoding>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rx: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tx: Option<u64>,
    /// The trace the caller said it was in, which the proxy never continues.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_trace: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attrs: Option<Attributes>,
    /// A trace id this proxy did not mint within the last hour.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub foreign: bool,
}

/// What a bound dropped: the chunk's cap, or, with `trace`, one trace's spans or attributes.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TraceGap {
    pub t: String,
    pub host: String,
    pub unit: String,
    pub rev: String,
    pub context: String,
    pub dropped: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trace: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "k", rename_all = "lowercase")]
pub enum TraceLine {
    Span(Box<SpanLine>),
    Gap(TraceGap),
}

impl TraceLine {
    fn labelled(self, host: &str, unit: &str, labels: &Unit) -> Self {
        let (host, unit, rev, context) = (
            host.into(),
            unit.into(),
            labels.rev.clone(),
            labels.context.clone(),
        );
        match self {
            TraceLine::Span(span) => TraceLine::Span(Box::new(SpanLine {
                host,
                unit,
                rev,
                context,
                ..*span
            })),
            TraceLine::Gap(gap) => TraceLine::Gap(TraceGap {
                host,
                unit,
                rev,
                context,
                ..gap
            }),
        }
    }

    fn ended_micros(&self) -> i64 {
        match self {
            TraceLine::Span(span) => micros_of(&span.end),
            TraceLine::Gap(gap) => micros_of(&gap.t),
        }
    }
}

/// A line of one of the two families: the chunks of each are written and capped alike.
pub(crate) trait Line: Serialize + Clone {
    const FAMILY: &'static str;
    fn t(&self) -> &str;
    /// The line saying `dropped` lines past the cap were let go, labelled as `self` is.
    fn gap(&self, t: String, dropped: u64) -> Self;
    fn shipping(state: &mut Shipping) -> &mut BTreeMap<String, AppLogs<Self>>;
}

impl Line for LogLine {
    const FAMILY: &'static str = "logs";

    fn t(&self) -> &str {
        &self.t
    }

    fn gap(&self, t: String, dropped: u64) -> Self {
        LogLine {
            t,
            k: LogKind::Gap,
            dropped: Some(dropped),
            ..LogLine::labelled(
                self.host.clone(),
                self.unit.clone(),
                self.rev.clone(),
                self.context.clone(),
            )
        }
    }

    fn shipping(state: &mut Shipping) -> &mut BTreeMap<String, AppLogs<Self>> {
        &mut state.apps
    }
}

impl Line for TraceLine {
    const FAMILY: &'static str = "traces";

    fn t(&self) -> &str {
        match self {
            TraceLine::Span(span) => &span.t,
            TraceLine::Gap(gap) => &gap.t,
        }
    }

    fn gap(&self, t: String, dropped: u64) -> Self {
        let (host, unit, rev, context) = match self {
            TraceLine::Span(s) => (&s.host, &s.unit, &s.rev, &s.context),
            TraceLine::Gap(g) => (&g.host, &g.unit, &g.rev, &g.context),
        };
        TraceLine::Gap(TraceGap {
            t,
            host: host.clone(),
            unit: unit.clone(),
            rev: rev.clone(),
            context: context.clone(),
            dropped,
            trace: None,
        })
    }

    fn shipping(state: &mut Shipping) -> &mut BTreeMap<String, AppLogs<Self>> {
        &mut state.traces
    }
}

pub fn encode<L: Serialize>(lines: &[L]) -> Vec<u8> {
    let mut gz = flate2::write::GzEncoder::new(vec![], flate2::Compression::default());
    for line in lines {
        let _ = serde_json::to_writer(&mut gz, line);
        let _ = gz.write_all(b"\n");
    }
    gz.finish().unwrap_or_default()
}

#[derive(Debug, Clone)]
struct Unit {
    app: String,
    rev: String,
    context: String,
    timeline: Option<String>,
}

impl Unit {
    /// The labels' `app`, `rev`, `context` and `timeline`; none without an `app`, which is not
    /// shipped.
    fn of(labels: &InstanceLabels) -> Option<Self> {
        let label = |k: &str| {
            labels
                .iter()
                .find(|(key, _)| key.as_str() == k)
                .map(|(_, v)| v.to_string())
        };
        Some(Self {
            app: label("app")?,
            rev: label("rev").unwrap_or_default(),
            context: label("context").unwrap_or_default(),
            timeline: label("timeline"),
        })
    }
}

/// A deployment being replaced may still be writing while its successor starts.
const DEPLOYMENTS_PER_APP: usize = 2;

pub(crate) struct AppLogs<L> {
    pending: Vec<L>,
    pending_bytes: usize,
    /// A chunk that filled before its five minutes were up, waiting for the next write.
    full: Option<Vec<L>>,
    dropped: u64,
    dropped_label: Option<L>,
    live: VecDeque<(i64, usize, L)>,
    live_bytes: usize,
    printed: bool,
}

impl<L> Default for AppLogs<L> {
    fn default() -> Self {
        Self {
            pending: Vec::new(),
            pending_bytes: 0,
            full: None,
            dropped: 0,
            dropped_label: None,
            live: VecDeque::new(),
            live_bytes: 0,
            printed: false,
        }
    }
}

impl<L: Line> AppLogs<L> {
    fn push(&mut self, micros: i64, line: L) {
        let size = serde_json::to_vec(&line).map_or(0, |b| b.len() + 1);
        if self.pending_bytes + size > PENDING_MAX_BYTES && self.full.is_none() {
            self.full = Some(std::mem::take(&mut self.pending));
            self.pending_bytes = 0;
        }
        self.live.push_back((micros, size, line.clone()));
        self.live_bytes += size;
        while self.live_bytes > PENDING_MAX_BYTES {
            let Some((_, n, _)) = self.live.pop_front() else {
                break;
            };
            self.live_bytes -= n;
        }
        self.printed = true;
        if self.pending_bytes + size > PENDING_MAX_BYTES {
            self.dropped += 1;
            self.dropped_label = Some(line);
        } else {
            self.pending.push(line);
            self.pending_bytes += size;
        }
    }

    fn prune(&mut self, now: i64) {
        let since = (now - LIVE_WINDOW_S) * 1_000_000;
        while self.live.front().is_some_and(|(t, _, _)| *t < since) {
            let Some((_, n, _)) = self.live.pop_front() else {
                break;
            };
            self.live_bytes -= n;
        }
    }

    /// The next chunk to write: a full one first, then (when `pending` is due) the lines gathered
    /// since, ended by one gap line for whatever the cap dropped.
    fn take(&mut self, pending: bool, now: i64) -> Option<Vec<L>> {
        if let Some(full) = self.full.take() {
            return Some(full);
        }
        if !pending {
            return None;
        }
        let mut lines = std::mem::take(&mut self.pending);
        self.pending_bytes = 0;
        if let (n @ 1.., Some(label)) = (std::mem::take(&mut self.dropped), self.dropped_label.take()) {
            lines.push(label.gap(micros_rfc3339(now * 1_000_000), n));
        }
        (!lines.is_empty()).then_some(lines)
    }

    fn put_back(&mut self, mut lines: Vec<L>) {
        self.pending_bytes += lines
            .iter()
            .map(|l| serde_json::to_vec(l).map_or(0, |b| b.len() + 1))
            .sum::<usize>();
        lines.append(&mut self.pending);
        self.pending = lines;
    }

    fn idle(&self) -> bool {
        self.pending.is_empty()
            && self.full.is_none()
            && self.dropped == 0
            && self.live.is_empty()
            && !self.printed
    }
}

#[derive(Default)]
pub(crate) struct Shipping {
    host: String,
    /// Each app id's labelled deployments, newest first.
    units: BTreeMap<AppId, Vec<(DeploymentId, Unit)>>,
    apps: BTreeMap<String, AppLogs<LogLine>>,
    traces: BTreeMap<String, AppLogs<TraceLine>>,
    chunk_slot: Option<i64>,
}

impl Shipping {
    /// The labels `deployment` of `app_id` wrote under, or its newest when that is not said.
    fn unit(&self, app_id: &AppId, deployment: Option<&DeploymentId>) -> Option<Unit> {
        let deployments = self.units.get(app_id)?;
        let wrote = match deployment {
            Some(deployment) => deployments.iter().find(|(d, _)| d == deployment),
            None => deployments.first(),
        };
        wrote.map(|(_, unit)| unit.clone())
    }
}

/// Where a declared cron job of one deployment records its attempts: under its instance's `app`
/// and `timeline` labels, beside its logs.
pub struct CronScope<'a> {
    bucket: &'a Bucket,
    pub host: String,
    pub app: String,
    pub timeline: String,
    pub rev: String,
}

impl CronScope<'_> {
    /// Whether it was written: `create` refuses a record already there.
    pub async fn put(&self, key: &str, body: Vec<u8>, create: bool) -> Result<bool, TelemetryError> {
        self.bucket.put(key, body, create).await
    }
}

pub struct ObjectSink {
    logs: Option<(Bucket, SealKey)>,
    metrics: Option<Bucket>,
    state: Mutex<Shipping>,
}

impl ObjectSink {
    pub fn new(logs: Option<(Bucket, SealKey)>, metrics: Option<Bucket>) -> Arc<Self> {
        Arc::new(Self {
            logs,
            metrics,
            state: Mutex::default(),
        })
    }

    pub fn metrics(&self) -> Option<&Bucket> {
        self.metrics.as_ref()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Shipping> {
        self.state.lock().expect("no panic holds the telemetry lock")
    }

    /// The labels of every instance the document names. One no longer named keeps its labels
    /// until it is forgotten, so its last lines still ship.
    pub fn learn(&self, desired: &HostDesiredState) {
        let mut state = self.lock();
        state.host = desired.host_id.to_string();
        for instance in &desired.instances {
            let Some(unit) = Unit::of(&instance.labels) else {
                continue;
            };
            let deployments = state.units.entry(instance.app_id.clone()).or_default();
            deployments.retain(|(labelled, _)| *labelled != instance.deployment_id);
            deployments.insert(0, (instance.deployment_id.clone(), unit));
            deployments.truncate(DEPLOYMENTS_PER_APP);
        }
    }

    /// The host name and every labelled unit's app.
    pub fn units(&self) -> (String, BTreeMap<String, String>) {
        let state = self.lock();
        let newest = state
            .units
            .iter()
            .filter_map(|(id, d)| Some((id.to_string(), d.first()?.1.app.clone())));
        (state.host.clone(), newest.collect())
    }

    fn push(&self, app_id: &AppId, unit: Option<Unit>, micros: i64, line: impl FnOnce(LogLine) -> LogLine) {
        let (Some(_), Some(unit)) = (&self.logs, unit) else {
            return;
        };
        let mut state = self.lock();
        let labelled = LogLine {
            t: micros_rfc3339(micros),
            ..LogLine::labelled(state.host.clone(), app_id.to_string(), unit.rev, unit.context)
        };
        state
            .apps
            .entry(unit.app)
            .or_default()
            .push(micros, line(labelled));
    }

    pub fn output(&self, event: &TenantLogEvent) {
        let unit = self.lock().unit(&event.app_id, Some(&event.deployment_id));
        let (stream, msg, cron) = match &event.body {
            TenantLogBody::CronData {
                stream,
                text,
                job_id,
                run_id,
                trace,
                span,
            } => (
                *stream,
                text.clone(),
                Some([job_id, run_id, trace, span].map(String::clone)),
            ),
            body => {
                let (stream, text) = super::file_sink::FileLogSink::text(body);
                (stream, text.into_owned(), None)
            }
        };
        let [job, run, trace, span] = cron.map_or([None, None, None, None], |ids| ids.map(Some));
        let k = match stream {
            TenantLogStream::Stdout => LogKind::Stdout,
            TenantLogStream::Stderr => LogKind::Stderr,
        };
        self.push(&event.app_id, unit, event.observed_at.epoch_ms() * 1000, |l| {
            LogLine {
                k,
                msg: Some(msg),
                job,
                run,
                trace,
                span,
                ..l
            }
        });
    }

    /// The proxy's root span of a request, labelled with what the route it took carried, so a
    /// deployment being replaced keeps its own.
    pub fn request(&self, app_id: &AppId, record: &HttpRequestRecord) {
        let Some(trace) = &record.trace else {
            return;
        };
        let span = SpanLine {
            trace: trace.trace.to_string(),
            span: trace.span.to_string(),
            parent: None,
            kind: SpanKind::Server,
            name: format!("{} {}", record.method, record.path),
            t: micros_rfc3339(trace.started_us),
            end: micros_rfc3339(record.ended_us.max(trace.started_us)),
            root: true,
            method: Some(record.method.clone()),
            path: Some(record.path.clone()),
            status: Some(SpanStatus::Http(record.status)),
            deployment: Some(record.deployment_id.to_string()),
            wake: record.wake,
            wake_us: record.wake_micros,
            cache: record.cache,
            encoding: record.encoding,
            rx: Some(record.request_bytes),
            tx: Some(record.response_bytes),
            parent_trace: trace.parent_trace.map(|t| t.to_string()),
            ..SpanLine::default()
        };
        self.traced(
            app_id,
            Unit::of(&record.labels),
            vec![TraceLine::Span(Box::new(span))],
        );
    }

    pub fn cron_scope(&self, app_id: &AppId, deployment_id: &DeploymentId) -> Option<CronScope<'_>> {
        let (bucket, _) = self.logs.as_ref()?;
        let state = self.lock();
        let unit = state.unit(app_id, Some(deployment_id))?;
        Some(CronScope {
            bucket,
            host: state.host.clone(),
            app: unit.app,
            timeline: unit.timeline?,
            rev: unit.rev,
        })
    }

    /// The root span of a cron attempt, labelled as the deployment that ran it.
    pub fn cron_attempt(&self, app_id: &AppId, deployment_id: &DeploymentId, span: SpanLine) {
        let unit = self.lock().unit(app_id, Some(deployment_id));
        self.traced(app_id, unit, vec![TraceLine::Span(Box::new(span))]);
    }

    /// What a guest exported over OTLP, labelled as the deployment that sent it.
    pub(crate) fn exported(
        &self,
        app_id: &AppId,
        deployment: Option<&DeploymentId>,
        spans: Vec<TraceLine>,
        records: Vec<(i64, LogLine)>,
    ) {
        // A deployment the document no longer names still has its app's labels to file under.
        let unit = {
            let state = self.lock();
            state
                .unit(app_id, deployment)
                .or_else(|| state.unit(app_id, None))
        };
        self.traced(app_id, unit.clone(), spans);
        for (micros, record) in records {
            self.push(app_id, unit.clone(), micros, |labelled| LogLine {
                t: labelled.t,
                host: labelled.host,
                unit: labelled.unit,
                rev: labelled.rev,
                context: labelled.context,
                k: LogKind::Otel,
                ..record
            });
        }
    }

    fn traced(&self, app_id: &AppId, unit: Option<Unit>, lines: Vec<TraceLine>) {
        let (Some(_), Some(unit)) = (&self.logs, unit) else {
            return;
        };
        let mut state = self.lock();
        let host = state.host.clone();
        let traces = state.traces.entry(unit.app.clone()).or_default();
        for line in lines {
            let line = line.labelled(&host, app_id.as_str(), &unit);
            traces.push(line.ended_micros(), line);
        }
    }

    /// The live tail of every app that printed since the last; every app's chunk at a 5-minute
    /// boundary, and a chunk that filled early whenever. A write that fails keeps its lines.
    pub async fn ship(&self, now: i64) -> Result<(), TelemetryError> {
        let boundary = {
            let mut state = self.lock();
            let slot = now.div_euclid(CHUNK_S);
            state.chunk_slot.replace(slot).is_some_and(|s| s != slot)
        };
        let logs = self.ship_family::<LogLine>(boundary, now).await;
        let traces = self.ship_family::<TraceLine>(boundary, now).await;
        logs.and(traces)
    }

    async fn ship_family<L: Line>(&self, boundary: bool, now: i64) -> Result<(), TelemetryError> {
        let (printed, apps) = {
            let mut state = self.lock();
            let family = L::shipping(&mut state);
            family.values_mut().for_each(|a| a.prune(now));
            let printed: Vec<String> = family
                .iter()
                .filter(|(_, a)| a.printed)
                .map(|(n, _)| n.clone())
                .collect();
            (printed, family.keys().cloned().collect::<Vec<_>>())
        };
        let mut first = Ok(());
        for app in printed {
            first = first.and(self.write_live::<L>(&app, now).await);
        }
        for app in apps {
            first = first.and(self.flush::<L>(&app, boundary, now).await);
        }
        L::shipping(&mut self.lock()).retain(|_, a| !a.idle());
        first
    }

    /// Writes the last chunk of the app `app_id` is a unit of, before its files and its labels go.
    pub async fn forget(&self, app_id: &AppId) -> Result<(), TelemetryError> {
        let Some(deployments) = self.lock().units.remove(app_id) else {
            return Ok(());
        };
        let apps: std::collections::BTreeSet<String> = deployments.into_iter().map(|(_, u)| u.app).collect();
        let now = crate::clock::now_ms() / 1000;
        for app in apps {
            self.flush::<LogLine>(&app, true, now).await?;
            self.flush::<TraceLine>(&app, true, now).await?;
        }
        Ok(())
    }

    async fn flush<L: Line>(&self, app: &str, pending: bool, now: i64) -> Result<(), TelemetryError> {
        let Some((bucket, seal)) = &self.logs else {
            return Ok(());
        };
        loop {
            let (host, lines) = {
                let mut state = self.lock();
                let host = state.host.clone();
                match L::shipping(&mut state)
                    .get_mut(app)
                    .and_then(|a| a.take(pending, now))
                {
                    Some(lines) => (host, lines),
                    None => return Ok(()),
                }
            };
            if let Err(error) = write_chunk(bucket, seal, app, &host, &lines).await {
                L::shipping(&mut self.lock())
                    .entry(app.into())
                    .or_default()
                    .put_back(lines);
                return Err(error);
            }
        }
    }

    /// `app`'s last [`LIVE_WINDOW_S`], at most [`LIVE_MAX_BYTES`] compressed, oldest lines first out.
    async fn write_live<L: Line>(&self, app: &str, now: i64) -> Result<(), TelemetryError> {
        let Some((bucket, seal)) = &self.logs else {
            return Ok(());
        };
        let (host, mut lines) = {
            let mut state = self.lock();
            let host = state.host.clone();
            let Some(a) = L::shipping(&mut state).get_mut(app) else {
                return Ok(());
            };
            a.prune(now);
            a.printed = false;
            let mut budget = LIVE_MAX_BYTES * LIVE_RATIO;
            let newest = a.live.iter().rev().take_while(|(_, n, _)| {
                budget = budget.saturating_sub(*n);
                budget > 0
            });
            let mut lines: Vec<L> = newest.map(|(_, _, l)| l.clone()).collect();
            lines.reverse();
            (host, lines)
        };
        let mut body = encode(&lines);
        while body.len() > LIVE_MAX_BYTES && !lines.is_empty() {
            lines.drain(..lines.len().div_ceil(4));
            body = encode(&lines);
        }
        let key = format!("{}/{app}/live-{host}.jsonl.gz", L::FAMILY);
        let written = bucket.put_sealed(&key, &body, seal, false).await;
        if written.is_err() {
            if let Some(a) = L::shipping(&mut self.lock()).get_mut(app) {
                a.printed = true;
            }
        }
        written.map(drop)
    }
}

/// Created once, named after its first line's second, or one of the few after it if taken.
async fn write_chunk<L: Line>(
    bucket: &Bucket,
    seal: &SealKey,
    app: &str,
    host: &str,
    lines: &[L],
) -> Result<(), TelemetryError> {
    let start = lines
        .first()
        .map_or(0, |l| micros_of(l.t()).div_euclid(1_000_000));
    let body = encode(lines);
    for t in start..start + NAME_TRIES {
        let key = format!("{}/{app}/{}/{}-{host}.jsonl.gz", L::FAMILY, hour_key(t), mmss(t));
        if bucket.put_sealed(&key, &body, seal, true).await? {
            return Ok(());
        }
    }
    Err(TelemetryError::Taken {
        app: app.into(),
        start,
    })
}

fn micros_of(t: &str) -> i64 {
    chrono::DateTime::parse_from_rfc3339(t).map_or(0, |at| at.timestamp_micros())
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::adapters::otlp::trace::{RequestTrace, TraceBook};
    use crate::adapters::proxy::http_log::{Wake, WakeKind};
    use crate::test_support::{app_id, deployment_id, desired_instance, desired_state};
    use object_store::memory::InMemory;

    const NOW: i64 = 1_792_152_300; // 2026-10-16T12:05:00Z

    fn unseal(seal: &SealKey, aad: &str, sealed: &[u8]) -> Vec<u8> {
        let (nonce, body) = sealed.split_at(24);
        XChaCha20Poly1305::new((&seal.0).into())
            .decrypt(
                XNonce::from_slice(nonce),
                Payload {
                    msg: body,
                    aad: aad.as_bytes(),
                },
            )
            .unwrap()
    }

    fn decode<L: serde::de::DeserializeOwned>(gz: &[u8]) -> Vec<L> {
        let mut text = String::new();
        std::io::Read::read_to_string(&mut flate2::read::GzDecoder::new(gz), &mut text).unwrap();
        text.lines().map(|l| serde_json::from_str(l).unwrap()).collect()
    }

    fn fixture_lines() -> Vec<LogLine> {
        let line = |t: &str, k| LogLine {
            t: t.into(),
            k,
            ..LogLine::labelled(
                "ovh-1".into(),
                "site".into(),
                "0123456789ab".into(),
                "production".into(),
            )
        };
        vec![
            LogLine {
                msg: Some("listening on :8000".into()),
                ..line("2026-10-16T12:05:00.123000Z", LogKind::Stdout)
            },
            LogLine {
                msg: Some("[nibrun] exited with code 1".into()),
                ..line("2026-10-16T12:05:01.000000Z", LogKind::Stderr)
            },
            LogLine {
                msg: Some("tick".into()),
                job: Some("nightly".into()),
                run: Some("r-1".into()),
                ..line("2026-10-16T12:05:01.500000Z", LogKind::Stdout)
            },
            LogLine {
                method: Some("GET".into()),
                path: Some("/checkout".into()),
                status: Some(500),
                dur_us: Some(312_000),
                wake_us: Some(240_000),
                cache: Some(CacheOutcome::Miss),
                encoding: Some(Encoding::Br),
                ..line("2026-10-16T12:05:02.500000Z", LogKind::Req)
            },
            LogLine {
                dropped: Some(3),
                ..line("2026-10-16T12:10:00.000000Z", LogKind::Gap)
            },
        ]
    }

    #[test]
    fn a_chunk_is_the_very_bytes_the_control_plane_seals_and_reads() {
        let seal = SealKey([7; 32]);
        let key = "logs/site/2026101612/0500-ovh-1.jsonl.gz";
        let plain = include_bytes!("../../../tests/fixtures/telemetry/chunk.jsonl.gz");
        let sealed = include_bytes!("../../../tests/fixtures/telemetry/chunk.sealed");
        assert_eq!(
            decode::<LogLine>(&unseal(&seal, &format!("acme/{key}"), sealed)),
            fixture_lines()
        );
        assert_eq!(encode(&fixture_lines()), plain);
        assert_eq!(seal.seal(&format!("acme/{key}"), plain), sealed);
    }

    fn fixture_traces() -> Vec<TraceLine> {
        let labelled = |span: SpanLine| SpanLine {
            host: "ovh-1".into(),
            unit: "site".into(),
            rev: "0123456789ab".into(),
            context: "production".into(),
            ..span
        };
        vec![
            TraceLine::Span(Box::new(labelled(SpanLine {
                trace: "01a14499f26ca1b2c3d4e5f60718293a".into(),
                span: "00f067aa0ba902b7".into(),
                parent: None,
                kind: SpanKind::Server,
                name: "GET /checkout".into(),
                t: "2026-10-16T12:05:02.188000Z".into(),
                end: "2026-10-16T12:05:02.500000Z".into(),
                root: true,
                method: Some("GET".into()),
                path: Some("/checkout".into()),
                status: Some(SpanStatus::Http(500)),
                deployment: Some("dep-1".into()),
                wake: Some(WakeKind::Restored),
                wake_us: Some(240_000),
                cache: Some(CacheOutcome::Miss),
                encoding: Some(Encoding::Br),
                rx: Some(0),
                tx: Some(1_234),
                parent_trace: Some("4bf92f3577b34da6a3ce929d0e0e4736".into()),
                ..SpanLine::default()
            }))),
            TraceLine::Span(Box::new(labelled(SpanLine {
                trace: "01a14499f26ca1b2c3d4e5f60718293a".into(),
                span: "b0709fad9766112d".into(),
                parent: Some("00f067aa0ba902b7".into()),
                kind: SpanKind::Server,
                name: "GET".into(),
                t: "2026-10-16T12:05:02.430000Z".into(),
                end: "2026-10-16T12:05:02.499000Z".into(),
                status: Some(SpanStatus::Otel(OtelStatus::Error)),
                status_msg: Some("upstream refused".into()),
                attrs: Some(Attributes::from([
                    ("http.response.status_code".into(), 500.into()),
                    ("url.path".into(), "/checkout".into()),
                ])),
                ..SpanLine::default()
            }))),
            TraceLine::Span(Box::new(labelled(SpanLine {
                trace: "4bf92f3577b34da6a3ce929d0e0e4736".into(),
                span: "b7ad6b7169203331".into(),
                parent: Some("00f067aa0ba902b7".into()),
                kind: SpanKind::Client,
                name: "pg.query".into(),
                t: "2026-10-16T12:05:03.000000Z".into(),
                end: "2026-10-16T12:05:03.010000Z".into(),
                status: Some(SpanStatus::Otel(OtelStatus::Unset)),
                foreign: true,
                ..SpanLine::default()
            }))),
            TraceLine::Gap(TraceGap {
                t: "2026-10-16T12:05:04.000000Z".into(),
                host: "ovh-1".into(),
                unit: "site".into(),
                rev: "0123456789ab".into(),
                context: "production".into(),
                dropped: 44,
                trace: Some("01a14499f26ca1b2c3d4e5f60718293a".into()),
            }),
        ]
    }

    #[test]
    fn a_trace_chunk_is_the_very_bytes_the_control_plane_seals_and_reads() {
        let seal = SealKey([7; 32]);
        let key = "traces/site/2026101612/0502-ovh-1.jsonl.gz";
        let plain = include_bytes!("../../../tests/fixtures/telemetry/trace-chunk.jsonl.gz");
        let sealed = include_bytes!("../../../tests/fixtures/telemetry/trace-chunk.sealed");
        assert_eq!(
            decode::<TraceLine>(&unseal(&seal, &format!("acme/{key}"), sealed)),
            fixture_traces()
        );
        assert_eq!(encode(&fixture_traces()), plain);
        assert_eq!(seal.seal(&format!("acme/{key}"), plain), sealed);
    }

    #[test]
    fn a_sink_names_its_fleet_by_the_last_two_segments_of_its_url() {
        assert_eq!(fleet_of("s3://bucket/fleets/acme").as_deref(), Some("acme"));
        assert_eq!(fleet_of("/srv/store/fleets/acme/").as_deref(), Some("acme"));
        assert_eq!(fleet_of("s3://bucket/acme"), None);
    }

    fn labels(rev: &str) -> InstanceLabels {
        serde_json::from_value(serde_json::json!({ "app": "site", "rev": rev, "context": "production" }))
            .unwrap()
    }

    pub(crate) fn sink() -> (Arc<ObjectSink>, Arc<InMemory>) {
        let store = Arc::new(InMemory::new());
        let bucket = Bucket::new(store.clone(), Some("fleets/acme".into()), "acme");
        let sink = ObjectSink::new(Some((bucket, SealKey([7; 32]))), None);
        sink.learn(&desired_state(|state| {
            state.host_id = protocol::HostId::parse("ovh-1").unwrap();
            state.instances = vec![desired_instance(|instance| {
                instance.labels = labels("0123456789ab");
            })];
        }));
        (sink, store)
    }

    fn event(at_ms: i64, text: &str) -> TenantLogEvent {
        TenantLogEvent {
            app_id: app_id(),
            deployment_id: deployment_id(),
            source_id: "s".into(),
            sequence: 0,
            observed_at: protocol::Timestamp::from_epoch_ms(at_ms),
            body: TenantLogBody::Data {
                stream: TenantLogStream::Stdout,
                text: text.into(),
            },
        }
    }

    async fn read(store: &InMemory, key: &str) -> Option<Vec<LogLine>> {
        read_family(store, key).await
    }

    async fn read_family<L: serde::de::DeserializeOwned>(store: &InMemory, key: &str) -> Option<Vec<L>> {
        let got = store.get(&format!("fleets/acme/{key}").into()).await.ok()?;
        let sealed = got.bytes().await.unwrap();
        Some(decode(&unseal(
            &SealKey([7; 32]),
            &format!("acme/{key}"),
            &sealed,
        )))
    }

    /// The lines of the sealed object at `key`, which must be there.
    pub(crate) async fn read_lines<L: serde::de::DeserializeOwned>(store: &InMemory, key: &str) -> Vec<L> {
        use futures::StreamExt;
        let found = read_family(store, key).await;
        let Some(lines) = found else {
            let keys: Vec<_> = store
                .list(None)
                .map(|meta| meta.unwrap().location.to_string())
                .collect::<Vec<_>>()
                .await;
            panic!("no {key} among {keys:?}");
        };
        lines
    }

    #[tokio::test]
    async fn lines_ship_live_at_once_and_as_a_chunk_named_after_the_first_at_the_boundary() {
        let (sink, store) = sink();
        sink.ship(NOW).await.unwrap();
        sink.output(&event(NOW * 1000 + 123, "listening"));
        sink.ship(NOW + 5).await.unwrap();
        let live = read(&store, "logs/site/live-ovh-1.jsonl.gz").await.unwrap();
        assert_eq!(live.len(), 1);
        assert_eq!(
            (live[0].unit.as_str(), live[0].t.as_str()),
            (app_id().as_str(), "2026-10-16T12:05:00.123000Z")
        );
        assert_eq!(live[0].msg.as_deref(), Some("listening"));
        assert!(read(&store, "logs/site/2026101612/0500-ovh-1.jsonl.gz")
            .await
            .is_none());

        sink.ship(NOW + CHUNK_S).await.unwrap();
        let chunk = read(&store, "logs/site/2026101612/0500-ovh-1.jsonl.gz")
            .await
            .unwrap();
        assert_eq!(chunk, live);
    }

    #[tokio::test]
    async fn an_unlabelled_instance_ships_nothing_and_a_forgotten_one_ships_its_last_lines() {
        let (sink, store) = sink();
        let stranger = event(NOW * 1000, "who");
        sink.output(&TenantLogEvent {
            app_id: AppId::parse("build-x").unwrap(),
            ..stranger
        });
        sink.output(&event(NOW * 1000, "last words"));
        sink.forget(&app_id()).await.unwrap();
        let chunk = read(&store, "logs/site/2026101612/0500-ovh-1.jsonl.gz")
            .await
            .unwrap();
        assert_eq!(chunk.len(), 1);
        assert!(sink.units().1.is_empty());
    }

    #[tokio::test]
    async fn a_line_carries_the_labels_of_the_deployment_that_wrote_it_and_a_cron_run_its_ids() {
        let (sink, store) = sink();
        let cron = TenantLogEvent {
            body: TenantLogBody::CronData {
                stream: TenantLogStream::Stdout,
                text: "tick".into(),
                job_id: "nightly".into(),
                run_id: "2026-10-16T12:05:00Z/1".into(),
                trace: "019a1b2c3d4e0123456789abcdef0123".into(),
                span: "0123456789abcdef".into(),
            },
            ..event(NOW * 1000, "")
        };
        sink.output(&cron);
        sink.output(&TenantLogEvent {
            deployment_id: protocol::DeploymentId::parse("dep-unlabelled").unwrap(),
            ..event(NOW * 1000, "unlabelled")
        });
        sink.forget(&app_id()).await.unwrap();
        let chunk = read(&store, "logs/site/2026101612/0500-ovh-1.jsonl.gz")
            .await
            .unwrap();
        assert_eq!(chunk.len(), 1);
        assert_eq!(
            (
                chunk[0].msg.as_deref(),
                chunk[0].job.as_deref(),
                chunk[0].run.as_deref(),
                chunk[0].trace.as_deref(),
                chunk[0].span.as_deref(),
            ),
            (
                Some("tick"),
                Some("nightly"),
                Some("2026-10-16T12:05:00Z/1"),
                Some("019a1b2c3d4e0123456789abcdef0123"),
                Some("0123456789abcdef")
            )
        );
    }

    fn checkout(started_us: i64, caller: Option<&str>) -> (HttpRequestRecord, RequestTrace) {
        let mut headers = hyper::HeaderMap::new();
        if let Some(caller) = caller {
            headers.insert("traceparent", caller.parse().unwrap());
        }
        let trace = RequestTrace::begin(&mut headers, &TraceBook::default(), started_us);
        let uri: hyper::Uri = "/checkout?token=private".parse().unwrap();
        let record = HttpRequestRecord::new("POST", &uri, 500, 312_000, deployment_id())
            .labelled(labels("r-2"))
            .woken(Some(Wake {
                micros: 240_000,
                kind: Some(WakeKind::Restored),
            }))
            .served_as(Some(CacheOutcome::Bypass), Some(Encoding::Gzip))
            .traced(trace)
            .finished(1_200, 3_400, started_us + 312_500);
        (record, trace)
    }

    #[tokio::test]
    async fn a_request_is_a_root_span_under_traces_and_no_longer_a_line_under_logs() {
        let (sink, store) = sink();
        let (record, trace) = checkout(
            NOW * 1_000_000 + 7,
            Some("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"),
        );
        sink.request(&app_id(), &record);
        sink.ship(NOW + 1).await.unwrap();
        assert!(read(&store, "logs/site/live-ovh-1.jsonl.gz").await.is_none());
        let live: Vec<TraceLine> = read_lines(&store, "traces/site/live-ovh-1.jsonl.gz").await;
        sink.forget(&app_id()).await.unwrap();
        let chunk: Vec<TraceLine> = read_lines(&store, "traces/site/2026101612/0500-ovh-1.jsonl.gz").await;
        assert_eq!(live, chunk);
        let [TraceLine::Span(root)] = chunk.as_slice() else {
            panic!("one root span: {chunk:?}");
        };
        assert_eq!(
            serde_json::to_value(root).unwrap(),
            serde_json::json!({
                "trace": trace.trace.to_string(), "span": trace.span.to_string(), "parent": null,
                "kind": "server", "name": "POST /checkout",
                "t": "2026-10-16T12:05:00.000000Z", "end": "2026-10-16T12:05:00.312507Z",
                "host": "ovh-1", "unit": app_id().to_string(), "rev": "r-2", "context": "production",
                "root": true, "method": "POST", "path": "/checkout", "status": 500,
                "deployment": deployment_id().to_string(), "wake": "restored", "wake_us": 240_000,
                "cache": "bypass", "encoding": "gzip", "rx": 1_200, "tx": 3_400,
                "parent_trace": "4bf92f3577b34da6a3ce929d0e0e4736",
            })
        );
        let id_ms = i64::from_str_radix(&root.trace[..12], 16).unwrap();
        assert_eq!(
            micros_rfc3339(id_ms * 1000),
            root.t,
            "the id and the root's t name one instant"
        );
        let line = serde_json::to_string(&chunk[0]).unwrap();
        assert!(line.starts_with(r#"{"k":"span","trace":""#), "{line}");
    }

    #[test]
    fn past_the_cap_a_trace_chunk_waits_and_the_rest_is_one_gap_line_of_no_trace() {
        let mut traces = AppLogs::<TraceLine>::default();
        let (record, trace) = checkout(NOW * 1_000_000, None);
        let span = TraceLine::Span(Box::new(SpanLine {
            trace: trace.trace.to_string(),
            name: "x".repeat(1 << 20),
            host: "ovh-1".into(),
            unit: record.path,
            ..SpanLine::default()
        }));
        for _ in 0..9 {
            traces.push(0, span.clone());
        }
        assert_eq!(traces.take(false, NOW).unwrap().len(), 3);
        let rest = traces.take(true, NOW).unwrap();
        let Some(TraceLine::Gap(gap)) = rest.last() else {
            panic!("a gap line");
        };
        assert_eq!(
            (gap.dropped, gap.trace.as_deref(), gap.host.as_str()),
            (3, None, "ovh-1")
        );
    }

    #[test]
    fn past_the_cap_one_chunk_waits_and_the_rest_is_counted_into_a_gap_line() {
        let mut logs = AppLogs::default();
        let line = |n: usize| LogLine {
            msg: Some("x".repeat(n)),
            ..fixture_lines().remove(0)
        };
        for _ in 0..9 {
            logs.push(0, line(1 << 20));
        }
        let full = logs.take(false, NOW).unwrap();
        assert_eq!(full.len(), 3);
        let rest = logs.take(true, NOW).unwrap();
        assert_eq!(rest.len(), 4);
        assert_eq!((rest[3].k, rest[3].dropped), (LogKind::Gap, Some(3)));
        assert!(logs.take(true, NOW).is_none());
    }
}
