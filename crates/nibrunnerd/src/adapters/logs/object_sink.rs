//! Tenant output and requests, shipped to a control plane's bucket in the objects it reads
//! (`logs/<app>/live-<host>.jsonl.gz`, `logs/<app>/<yyyymmddhh>/<mmss>-<host>.jsonl.gz`): gzipped
//! JSON lines, sealed under its fleet key. An instance is shipped only once its document labels it
//! with an `app`; every line carries that label, `rev` and `context`, and its unit (the app id).
use std::collections::{BTreeMap, VecDeque};
use std::io::Write;
use std::sync::{Arc, Mutex};

use chacha20poly1305::aead::{Aead, KeyInit, Payload};
use chacha20poly1305::{XChaCha20Poly1305, XNonce};
use hmac::{Hmac, Mac};
use object_store::{ObjectStore, PutMode, PutOptions, PutPayload};
use protocol::{AppId, DeploymentId, HostDesiredState, InstanceLabels, TenantLogStream};
use serde::{Deserialize, Serialize};
use sha2::Sha256;

use crate::adapters::proxy::http_log::HttpRequestRecord;
use crate::domain::metrics::rollup::{hour_key, micros_rfc3339, mmss};
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
}

fn fleet_of(url: &str) -> Option<String> {
    let mut segments = url.trim_end_matches('/').rsplit('/');
    let fleet = segments.next().filter(|fleet| !fleet.is_empty())?;
    (segments.next() == Some("fleets")).then(|| fleet.to_string())
}

/// One line as the control plane reads it: field order and omissions are the wire.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
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
    Req,
    Gap,
}

pub fn encode(lines: &[LogLine]) -> Vec<u8> {
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
}

impl Unit {
    /// The labels' `app`, `rev` and `context`; none without an `app`, which is not shipped.
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
        })
    }
}

/// A deployment being replaced may still be writing while its successor starts.
const DEPLOYMENTS_PER_APP: usize = 2;

#[derive(Default)]
struct AppLogs {
    pending: Vec<LogLine>,
    pending_bytes: usize,
    /// A chunk that filled before its five minutes were up, waiting for the next write.
    full: Option<Vec<LogLine>>,
    dropped: u64,
    dropped_label: Option<LogLine>,
    live: VecDeque<(i64, usize, LogLine)>,
    live_bytes: usize,
    printed: bool,
}

impl AppLogs {
    fn push(&mut self, micros: i64, line: LogLine) {
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
    fn take(&mut self, pending: bool, now: i64) -> Option<Vec<LogLine>> {
        if let Some(full) = self.full.take() {
            return Some(full);
        }
        if !pending {
            return None;
        }
        let mut lines = std::mem::take(&mut self.pending);
        self.pending_bytes = 0;
        if let (n @ 1.., Some(label)) = (std::mem::take(&mut self.dropped), self.dropped_label.take()) {
            lines.push(LogLine {
                t: micros_rfc3339(now * 1_000_000),
                k: LogKind::Gap,
                dropped: Some(n),
                ..LogLine::labelled(label.host, label.unit, label.rev, label.context)
            });
        }
        (!lines.is_empty()).then_some(lines)
    }

    fn put_back(&mut self, mut lines: Vec<LogLine>) {
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
struct Shipping {
    host: String,
    /// Each app id's labelled deployments, newest first.
    units: BTreeMap<AppId, Vec<(DeploymentId, Unit)>>,
    apps: BTreeMap<String, AppLogs>,
    chunk_slot: Option<i64>,
}

pub struct ObjectSink {
    logs: (Bucket, SealKey),
    state: Mutex<Shipping>,
}

impl ObjectSink {
    pub fn new(logs: (Bucket, SealKey)) -> Arc<Self> {
        Arc::new(Self {
            logs,
            state: Mutex::default(),
        })
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

    fn push(&self, app_id: &AppId, unit: Option<Unit>, micros: i64, line: impl FnOnce(LogLine) -> LogLine) {
        let Some(unit) = unit else {
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
        let unit = self.lock().units.get(&event.app_id).and_then(|deployments| {
            let wrote = deployments.iter().find(|(d, _)| *d == event.deployment_id);
            wrote.map(|(_, unit)| unit.clone())
        });
        let (stream, msg, job, run) = match &event.body {
            TenantLogBody::CronData {
                stream,
                text,
                job_id,
                run_id,
            } => (
                *stream,
                text.clone(),
                Some(job_id.to_string()),
                Some(run_id.to_string()),
            ),
            body => {
                let (stream, text) = super::file_sink::FileLogSink::text(body);
                (stream, text.into_owned(), None, None)
            }
        };
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
                ..l
            }
        });
    }

    /// Labelled with what the route it took carried, so a deployment being replaced keeps its own.
    pub fn request(&self, app_id: &AppId, record: &HttpRequestRecord) {
        self.push(
            app_id,
            Unit::of(&record.labels),
            record.at.epoch_ms() * 1000,
            |l| LogLine {
                k: LogKind::Req,
                method: Some(record.method.clone()),
                path: Some(record.path.clone()),
                status: Some(record.status),
                dur_us: Some(record.duration_micros),
                ..l
            },
        );
    }

    /// The live tail of every app that printed since the last; every app's chunk at a 5-minute
    /// boundary, and a chunk that filled early whenever. A write that fails keeps its lines.
    pub async fn ship(&self, now: i64) -> Result<(), TelemetryError> {
        let (printed, apps, boundary) = {
            let mut state = self.lock();
            state.apps.values_mut().for_each(|a| a.prune(now));
            let slot = now.div_euclid(CHUNK_S);
            let boundary = state.chunk_slot.replace(slot).is_some_and(|s| s != slot);
            let printed: Vec<String> = state
                .apps
                .iter()
                .filter(|(_, a)| a.printed)
                .map(|(n, _)| n.clone())
                .collect();
            (printed, state.apps.keys().cloned().collect::<Vec<_>>(), boundary)
        };
        let mut first = Ok(());
        for app in printed {
            first = first.and(self.write_live(&app, now).await);
        }
        for app in apps {
            first = first.and(self.flush(&app, boundary, now).await);
        }
        self.lock().apps.retain(|_, a| !a.idle());
        first
    }

    /// Writes the last chunk of the app `app_id` is a unit of, before its files and its labels go.
    pub async fn forget(&self, app_id: &AppId) -> Result<(), TelemetryError> {
        let Some(deployments) = self.lock().units.remove(app_id) else {
            return Ok(());
        };
        let apps: std::collections::BTreeSet<String> = deployments.into_iter().map(|(_, u)| u.app).collect();
        for app in apps {
            self.flush(&app, true, crate::clock::now_ms() / 1000).await?;
        }
        Ok(())
    }

    async fn flush(&self, app: &str, pending: bool, now: i64) -> Result<(), TelemetryError> {
        let (bucket, seal) = &self.logs;
        loop {
            let (host, lines) = {
                let mut state = self.lock();
                let host = state.host.clone();
                match state.apps.get_mut(app).and_then(|a| a.take(pending, now)) {
                    Some(lines) => (host, lines),
                    None => return Ok(()),
                }
            };
            if let Err(error) = write_chunk(bucket, seal, app, &host, &lines).await {
                self.lock().apps.entry(app.into()).or_default().put_back(lines);
                return Err(error);
            }
        }
    }

    /// `app`'s last [`LIVE_WINDOW_S`], at most [`LIVE_MAX_BYTES`] compressed, oldest lines first out.
    async fn write_live(&self, app: &str, now: i64) -> Result<(), TelemetryError> {
        let (bucket, seal) = &self.logs;
        let (host, mut lines) = {
            let mut state = self.lock();
            let host = state.host.clone();
            let Some(a) = state.apps.get_mut(app) else {
                return Ok(());
            };
            a.prune(now);
            a.printed = false;
            let mut budget = LIVE_MAX_BYTES * LIVE_RATIO;
            let newest = a.live.iter().rev().take_while(|(_, n, _)| {
                budget = budget.saturating_sub(*n);
                budget > 0
            });
            let mut lines: Vec<LogLine> = newest.map(|(_, _, l)| l.clone()).collect();
            lines.reverse();
            (host, lines)
        };
        let mut body = encode(&lines);
        while body.len() > LIVE_MAX_BYTES && !lines.is_empty() {
            lines.drain(..lines.len().div_ceil(4));
            body = encode(&lines);
        }
        let key = format!("logs/{app}/live-{host}.jsonl.gz");
        let written = bucket.put_sealed(&key, &body, seal, false).await;
        if written.is_err() {
            if let Some(a) = self.lock().apps.get_mut(app) {
                a.printed = true;
            }
        }
        written.map(drop)
    }
}

/// Created once, named after its first line's second, or one of the few after it if taken.
async fn write_chunk(
    bucket: &Bucket,
    seal: &SealKey,
    app: &str,
    host: &str,
    lines: &[LogLine],
) -> Result<(), TelemetryError> {
    let start = lines.first().map_or(0, |l| micros_of(&l.t).div_euclid(1_000_000));
    let body = encode(lines);
    for t in start..start + NAME_TRIES {
        let key = format!("logs/{app}/{}/{}-{host}.jsonl.gz", hour_key(t), mmss(t));
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
mod tests {
    use super::*;
    use crate::test_support::{app_id, deployment_id, desired_instance, desired_state};
    use object_store::memory::InMemory;
    use object_store::ObjectStoreExt;

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

    fn decode(gz: &[u8]) -> Vec<LogLine> {
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
            decode(&unseal(&seal, &format!("acme/{key}"), sealed)),
            fixture_lines()
        );
        assert_eq!(encode(&fixture_lines()), plain);
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

    fn sink() -> (Arc<ObjectSink>, Arc<InMemory>) {
        let store = Arc::new(InMemory::new());
        let bucket = Bucket::new(store.clone(), Some("fleets/acme".into()), "acme");
        let sink = ObjectSink::new((bucket, SealKey([7; 32])));
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
        let got = store.get(&format!("fleets/acme/{key}").into()).await.ok()?;
        let sealed = got.bytes().await.unwrap();
        Some(decode(&unseal(
            &SealKey([7; 32]),
            &format!("acme/{key}"),
            &sealed,
        )))
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
    }

    #[tokio::test]
    async fn a_line_carries_the_labels_of_the_deployment_that_wrote_it_and_a_cron_run_its_ids() {
        let (sink, store) = sink();
        let cron = TenantLogEvent {
            body: TenantLogBody::CronData {
                stream: TenantLogStream::Stdout,
                text: "tick".into(),
                job_id: "nightly".into(),
                run_id: "r-1".into(),
            },
            ..event(NOW * 1000, "")
        };
        sink.output(&cron);
        sink.output(&TenantLogEvent {
            deployment_id: protocol::DeploymentId::parse("dep-unlabelled").unwrap(),
            ..event(NOW * 1000, "unlabelled")
        });
        let uri: hyper::Uri = "/checkout".parse().unwrap();
        let record =
            HttpRequestRecord::new("GET", &uri, 500, 312_000, deployment_id()).labelled(labels("r-2"));
        sink.request(&app_id(), &record);
        sink.forget(&app_id()).await.unwrap();
        let chunk = read(&store, "logs/site/2026101612/0500-ovh-1.jsonl.gz")
            .await
            .unwrap();
        assert_eq!(chunk.len(), 2);
        assert_eq!(
            (
                chunk[0].msg.as_deref(),
                chunk[0].job.as_deref(),
                chunk[0].run.as_deref()
            ),
            (Some("tick"), Some("nightly"), Some("r-1"))
        );
        assert_eq!(
            (chunk[1].k, chunk[1].rev.as_str(), chunk[1].status),
            (LogKind::Req, "r-2", Some(500))
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
