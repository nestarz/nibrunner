//! The OTLP/JSON export requests, as far as a span line or a log line reads them, and the mapping
//! onto those lines. Every exporter is accepted as the OTLP/HTTP specification lets it write: an
//! enum as its number or its name, a 64-bit integer as a number or a decimal string.
use serde::Deserialize;

use crate::adapters::logs::object_sink::{
    Attributes, LogKind, LogLine, OtelStatus, Severity, SpanKind, SpanLine, SpanStatus, TraceGap, TraceLine,
};
use crate::adapters::otlp::trace::{SpanId, TraceBook, TraceId};
use crate::domain::metrics::rollup::micros_rfc3339;

const ATTRIBUTE_PREFIXES: [&str; 8] = [
    "http.",
    "url.",
    "db.",
    "rpc.",
    "exception.",
    "server.",
    "cron.",
    "process.exit_code",
];
/// Headers an SDK was told to capture: cookies and credentials, never kept.
const HEADER_PREFIXES: [&str; 2] = ["http.request.header.", "http.response.header."];
pub(crate) const ATTRIBUTES_KEPT: usize = 32;
const STRING_BYTES: usize = 1024;
const NAME_CHARS: usize = 256;
const MESSAGE_BYTES: usize = 16 * 1024;

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
enum Integer {
    Number(i64),
    Text(String),
}

impl Integer {
    fn value(&self) -> Option<i64> {
        match self {
            Integer::Number(n) => Some(*n),
            Integer::Text(text) => text.trim().parse().ok(),
        }
    }
}

/// An enum field: its number, or its name with or without the prefix the protobuf gives it.
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
enum Named {
    Number(i64),
    Name(String),
}

impl Named {
    fn read(&self, prefix: &str, names: &[&str]) -> Option<i64> {
        match self {
            Named::Number(n) => Some(*n),
            Named::Name(name) => {
                let upper = name.to_ascii_uppercase();
                let bare = upper.strip_prefix(prefix).unwrap_or(&upper);
                names
                    .iter()
                    .position(|n| *n == bare)
                    .and_then(|i| i64::try_from(i).ok())
            }
        }
    }
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct AnyValue {
    string_value: Option<String>,
    bool_value: Option<bool>,
    int_value: Option<Integer>,
    double_value: Option<f64>,
    array_value: Option<ArrayValue>,
    kvlist_value: Option<KeyValueList>,
    bytes_value: Option<String>,
}

#[derive(Debug, Clone, Default, Deserialize)]
struct ArrayValue {
    #[serde(default)]
    values: Vec<AnyValue>,
}

#[derive(Debug, Clone, Default, Deserialize)]
struct KeyValueList {
    #[serde(default)]
    values: Vec<KeyValue>,
}

#[derive(Debug, Clone, Deserialize)]
struct KeyValue {
    key: String,
    #[serde(default)]
    value: AnyValue,
}

impl AnyValue {
    fn json(&self) -> serde_json::Value {
        use serde_json::Value;
        if let Some(text) = self.string_value.as_ref().or(self.bytes_value.as_ref()) {
            Value::String(text.clone())
        } else if let Some(flag) = self.bool_value {
            Value::Bool(flag)
        } else if let Some(n) = self.int_value.as_ref().and_then(Integer::value) {
            Value::from(n)
        } else if let Some(x) = self.double_value {
            Value::from(x)
        } else if let Some(array) = &self.array_value {
            Value::Array(array.values.iter().map(AnyValue::json).collect())
        } else if let Some(list) = &self.kvlist_value {
            Value::Object(
                list.values
                    .iter()
                    .map(|kv| (kv.key.clone(), kv.value.json()))
                    .collect(),
            )
        } else {
            Value::Null
        }
    }

    /// What an attribute may carry: a string (cut to 1 KiB), a number, a boolean.
    fn scalar(&self) -> Option<serde_json::Value> {
        match self.json() {
            serde_json::Value::String(text) => Some(cut(text, STRING_BYTES).into()),
            value @ (serde_json::Value::Number(_) | serde_json::Value::Bool(_)) => Some(value),
            _ => None,
        }
    }
}

fn cut(mut text: String, bytes: usize) -> String {
    if text.len() > bytes {
        let mut end = bytes;
        while !text.is_char_boundary(end) {
            end -= 1;
        }
        text.truncate(end);
    }
    text
}

/// The allow-listed attributes, at most [`ATTRIBUTES_KEPT`], and how many allowed ones were cut.
fn attributes<'a>(given: impl IntoIterator<Item = &'a KeyValue>) -> (Option<Attributes>, u64) {
    let mut kept = Attributes::new();
    let mut cut_off = 0;
    for kv in given {
        let allowed = ATTRIBUTE_PREFIXES.iter().any(|prefix| kv.key.starts_with(prefix))
            && !HEADER_PREFIXES.iter().any(|prefix| kv.key.starts_with(prefix));
        if !allowed {
            continue;
        }
        let Some(value) = kv.value.scalar() else {
            continue;
        };
        if kept.len() < ATTRIBUTES_KEPT || kept.contains_key(&kv.key) {
            kept.insert(cut(kv.key.clone(), STRING_BYTES), value);
        } else {
            cut_off += 1;
        }
    }
    ((!kept.is_empty()).then_some(kept), cut_off)
}

fn nanos_to_micros(nanos: Option<&Integer>) -> Option<i64> {
    nanos
        .and_then(Integer::value)
        .filter(|n| *n > 0)
        .map(|n| n / 1000)
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ExportTraces {
    #[serde(default)]
    resource_spans: Vec<ResourceSpans>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ResourceSpans {
    #[serde(default)]
    scope_spans: Vec<ScopeSpans>,
}

#[derive(Debug, Default, Deserialize)]
struct ScopeSpans {
    #[serde(default)]
    spans: Vec<Span>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Span {
    #[serde(default)]
    trace_id: String,
    #[serde(default)]
    span_id: String,
    #[serde(default)]
    parent_span_id: String,
    #[serde(default)]
    name: String,
    kind: Option<Named>,
    start_time_unix_nano: Option<Integer>,
    end_time_unix_nano: Option<Integer>,
    #[serde(default)]
    attributes: Vec<KeyValue>,
    #[serde(default)]
    events: Vec<Event>,
    status: Option<Status>,
}

#[derive(Debug, Default, Deserialize)]
struct Event {
    #[serde(default)]
    name: String,
    #[serde(default)]
    attributes: Vec<KeyValue>,
}

#[derive(Debug, Default, Deserialize)]
struct Status {
    code: Option<Named>,
    #[serde(default)]
    message: String,
}

const SPAN_KINDS: [&str; 6] = [
    "UNSPECIFIED",
    "INTERNAL",
    "SERVER",
    "CLIENT",
    "PRODUCER",
    "CONSUMER",
];
const STATUS_CODES: [&str; 3] = ["UNSET", "OK", "ERROR"];

fn span_kind(kind: Option<&Named>) -> SpanKind {
    match kind.and_then(|kind| kind.read("SPAN_KIND_", &SPAN_KINDS)) {
        Some(2) => SpanKind::Server,
        Some(3) => SpanKind::Client,
        Some(4) => SpanKind::Producer,
        Some(5) => SpanKind::Consumer,
        _ => SpanKind::Internal,
    }
}

fn status(status: Option<&Status>) -> (OtelStatus, Option<String>) {
    let code = status
        .and_then(|status| status.code.as_ref())
        .and_then(|code| code.read("STATUS_CODE_", &STATUS_CODES));
    let code = match code {
        Some(1) => OtelStatus::Ok,
        Some(2) => OtelStatus::Error,
        _ => OtelStatus::Unset,
    };
    let message = status
        .map(|status| cut(status.message.clone(), STRING_BYTES))
        .filter(|message| !message.is_empty());
    (code, message)
}

impl ExportTraces {
    fn spans(&self) -> impl Iterator<Item = &Span> {
        self.resource_spans
            .iter()
            .flat_map(|resource| &resource.scope_spans)
            .flat_map(|scope| &scope.spans)
    }

    /// Span lines, unlabelled: the receiver knows who sent them. A span with no valid ids is
    /// dropped and counted into a gap line; a trace past its span bound, or a span past its
    /// attribute bound, into a gap line naming that trace.
    pub(crate) fn lines(&self, book: &TraceBook, now_us: i64) -> Vec<TraceLine> {
        let mut lines = Vec::new();
        let mut unreadable = 0;
        let mut cut_by_trace = std::collections::BTreeMap::<TraceId, u64>::new();
        for span in self.spans() {
            let (Some(trace), Some(id)) = (TraceId::parse(&span.trace_id), SpanId::parse(&span.span_id))
            else {
                unreadable += 1;
                continue;
            };
            let admission = book.admit(trace, now_us);
            if !admission.kept {
                *cut_by_trace.entry(trace).or_default() += 1;
                continue;
            }
            let exceptions = span
                .events
                .iter()
                .filter(|event| event.name == "exception")
                .flat_map(|event| &event.attributes);
            let (attrs, cut_off) = attributes(span.attributes.iter().chain(exceptions));
            if cut_off > 0 {
                *cut_by_trace.entry(trace).or_default() += cut_off;
            }
            let start = nanos_to_micros(span.start_time_unix_nano.as_ref()).unwrap_or(now_us);
            let end = nanos_to_micros(span.end_time_unix_nano.as_ref()).unwrap_or(start);
            let (status, status_msg) = status(span.status.as_ref());
            lines.push(TraceLine::Span(Box::new(SpanLine {
                trace: trace.to_string(),
                span: id.to_string(),
                parent: SpanId::parse(&span.parent_span_id).map(|parent| parent.to_string()),
                kind: span_kind(span.kind.as_ref()),
                name: span.name.chars().take(NAME_CHARS).collect(),
                t: micros_rfc3339(start),
                end: micros_rfc3339(end.max(start)),
                status: Some(SpanStatus::Otel(status)),
                status_msg,
                attrs,
                foreign: admission.foreign,
                ..SpanLine::default()
            })));
        }
        let gap = |dropped, trace: Option<TraceId>| {
            TraceLine::Gap(TraceGap {
                t: micros_rfc3339(now_us),
                dropped,
                trace: trace.map(|trace| trace.to_string()),
                ..TraceGap::default()
            })
        };
        lines.extend(
            cut_by_trace
                .into_iter()
                .map(|(trace, dropped)| gap(dropped, Some(trace))),
        );
        if unreadable > 0 {
            lines.push(gap(unreadable, None));
        }
        lines
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ExportLogs {
    #[serde(default)]
    resource_logs: Vec<ResourceLogs>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ResourceLogs {
    #[serde(default)]
    scope_logs: Vec<ScopeLogs>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ScopeLogs {
    #[serde(default)]
    log_records: Vec<LogRecord>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct LogRecord {
    time_unix_nano: Option<Integer>,
    observed_time_unix_nano: Option<Integer>,
    severity_number: Option<Named>,
    #[serde(default)]
    severity_text: String,
    body: Option<AnyValue>,
    #[serde(default)]
    attributes: Vec<KeyValue>,
    #[serde(default)]
    trace_id: String,
    #[serde(default)]
    span_id: String,
}

const SEVERITY_NUMBERS: [&str; 25] = [
    "UNSPECIFIED",
    "TRACE",
    "TRACE2",
    "TRACE3",
    "TRACE4",
    "DEBUG",
    "DEBUG2",
    "DEBUG3",
    "DEBUG4",
    "INFO",
    "INFO2",
    "INFO3",
    "INFO4",
    "WARN",
    "WARN2",
    "WARN3",
    "WARN4",
    "ERROR",
    "ERROR2",
    "ERROR3",
    "ERROR4",
    "FATAL",
    "FATAL2",
    "FATAL3",
    "FATAL4",
];

fn severity(record: &LogRecord) -> Option<Severity> {
    let number = record
        .severity_number
        .as_ref()
        .and_then(|n| n.read("SEVERITY_NUMBER_", &SEVERITY_NUMBERS));
    match number {
        Some(1..=4) => Some(Severity::Trace),
        Some(5..=8) => Some(Severity::Debug),
        Some(9..=12) => Some(Severity::Info),
        Some(13..=16) => Some(Severity::Warn),
        Some(17..=20) => Some(Severity::Error),
        Some(21..=24) => Some(Severity::Fatal),
        _ => match record.severity_text.to_ascii_uppercase().as_str() {
            "TRACE" => Some(Severity::Trace),
            "DEBUG" => Some(Severity::Debug),
            "INFO" => Some(Severity::Info),
            "WARN" | "WARNING" => Some(Severity::Warn),
            "ERROR" => Some(Severity::Error),
            "FATAL" | "CRITICAL" => Some(Severity::Fatal),
            _ => None,
        },
    }
}

fn message(body: Option<&AnyValue>) -> String {
    let text = match body.map(AnyValue::json) {
        Some(serde_json::Value::String(text)) => text,
        None | Some(serde_json::Value::Null) => String::new(),
        Some(value) => value.to_string(),
    };
    let line = text.strip_suffix('\n').unwrap_or(&text);
    cut(line.strip_suffix('\r').unwrap_or(line).to_string(), MESSAGE_BYTES)
}

impl ExportLogs {
    fn records(&self) -> impl Iterator<Item = &LogRecord> {
        self.resource_logs
            .iter()
            .flat_map(|resource| &resource.scope_logs)
            .flat_map(|scope| &scope.log_records)
    }

    /// Log lines, unlabelled, each with the microsecond it was written at.
    pub(crate) fn lines(&self, now_us: i64) -> Vec<(i64, LogLine)> {
        self.records()
            .map(|record| {
                let at = nanos_to_micros(record.time_unix_nano.as_ref())
                    .or_else(|| nanos_to_micros(record.observed_time_unix_nano.as_ref()))
                    .unwrap_or(now_us);
                let line = LogLine {
                    k: LogKind::Otel,
                    msg: Some(message(record.body.as_ref())),
                    severity: severity(record),
                    trace: TraceId::parse(&record.trace_id).map(|trace| trace.to_string()),
                    span: SpanId::parse(&record.span_id).map(|span| span.to_string()),
                    attrs: attributes(&record.attributes).0,
                    ..LogLine::default()
                };
                (at, line)
            })
            .collect()
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    pub(crate) const DENO_TRACES: &str = include_str!("../../../tests/fixtures/otlp/deno-2.9.7-traces.json");
    pub(crate) const DENO_LOGS_AT_START: &str =
        include_str!("../../../tests/fixtures/otlp/deno-2.9.7-logs-1.json");
    pub(crate) const DENO_LOGS_OF_A_REQUEST: &str =
        include_str!("../../../tests/fixtures/otlp/deno-2.9.7-logs-2.json");
    pub(crate) const DENO_METRICS: &str =
        include_str!("../../../tests/fixtures/otlp/deno-2.9.7-metrics.json");
    const NODE_TRACES: &str = include_str!("../../../tests/fixtures/otlp/node-traces.json");
    const NODE_LOGS: &str = include_str!("../../../tests/fixtures/otlp/node-logs.json");

    pub(crate) const DENO_TRACE: &str = "65b0c1d2e3f40123456789abcdef0123";
    const NOW_US: i64 = 1_792_488_115_000_000;

    fn spans(lines: &[TraceLine]) -> Vec<&SpanLine> {
        lines
            .iter()
            .filter_map(|line| match line {
                TraceLine::Span(span) => Some(&**span),
                TraceLine::Gap(_) => None,
            })
            .collect()
    }

    #[test]
    fn a_deno_export_reads_as_its_server_and_fetch_spans_nested_under_the_proxys_root() {
        let book = TraceBook::default();
        let recorded = TraceId::parse(DENO_TRACE).unwrap();
        book.remember(recorded);
        let export: ExportTraces = serde_json::from_str(DENO_TRACES).unwrap();
        let lines = export.lines(&book, recorded.started_us() + 1_000_000);
        let spans = spans(&lines);
        assert_eq!(spans.len(), 3);
        assert_eq!(lines.len(), 3, "no gap");
        let server = spans.iter().find(|s| s.span == "b0709fad9766112d").unwrap();
        assert_eq!(server.trace, DENO_TRACE);
        assert_eq!(server.parent.as_deref(), Some("0123456789abcdef"));
        assert_eq!(server.kind, SpanKind::Server);
        assert_eq!(server.name, "GET");
        assert_eq!(server.t, "2026-10-20T09:21:52.318181Z");
        assert_eq!(server.end, "2026-10-20T09:21:52.327515Z");
        assert_eq!(server.status, Some(SpanStatus::Otel(OtelStatus::Unset)));
        assert_eq!(server.status_msg, None);
        assert!(!server.foreign);
        let attrs = server.attrs.as_ref().unwrap();
        assert_eq!(attrs["url.path"], "/hello");
        assert_eq!(attrs["http.response.status_code"], "200");
        assert_eq!(attrs.len(), 6);
        let fetch = spans.iter().find(|s| s.span == "9f35f16799ba03d9").unwrap();
        assert_eq!(
            (fetch.kind, fetch.parent.as_deref()),
            (SpanKind::Client, Some("b0709fad9766112d"))
        );
    }

    #[test]
    fn a_deno_export_of_a_trace_this_proxy_did_not_mint_is_kept_and_marked_foreign() {
        let export: ExportTraces = serde_json::from_str(DENO_TRACES).unwrap();
        let lines = export.lines(&TraceBook::default(), NOW_US);
        assert!(spans(&lines).iter().all(|span| span.foreign));
        assert_eq!(spans(&lines).len(), 3);
    }

    #[test]
    fn deno_console_records_read_with_their_severity_and_the_span_they_were_printed_in() {
        let at_start: ExportLogs = serde_json::from_str(DENO_LOGS_AT_START).unwrap();
        let lines = at_start.lines(NOW_US);
        assert_eq!(lines.len(), 2);
        assert_eq!(lines[0].0, 1_792_488_110_358_485);
        assert_eq!(
            lines[0].1.msg.as_deref(),
            Some("Listening on http://0.0.0.0:8787/ (http://localhost:8787/)")
        );
        assert_eq!(lines[0].1.severity, Some(Severity::Info));
        assert_eq!((&lines[0].1.trace, &lines[0].1.span), (&None, &None));

        let of_a_request: ExportLogs = serde_json::from_str(DENO_LOGS_OF_A_REQUEST).unwrap();
        let lines = of_a_request.lines(NOW_US);
        assert_eq!(lines[0].1.msg.as_deref(), Some("handling /hello"));
        assert_eq!(lines[0].1.trace.as_deref(), Some(DENO_TRACE));
        assert_eq!(lines[0].1.span.as_deref(), Some("b0709fad9766112d"));
        assert_eq!(lines[1].1.severity, Some(Severity::Error));
        assert_eq!(lines[1].1.k, LogKind::Otel);
        assert!(
            lines.iter().all(|(_, line)| line.attrs.is_none()),
            "log.iostream is not on the list"
        );
    }

    #[test]
    fn a_node_export_with_named_enums_numeric_nanos_an_error_status_and_a_structured_body_reads_the_same() {
        let book = TraceBook::default();
        let export: ExportTraces = serde_json::from_str(NODE_TRACES).unwrap();
        let lines = export.lines(&book, NOW_US);
        let spans = spans(&lines);
        assert_eq!(spans.len(), 2);
        let failed = spans.iter().find(|s| s.span == "00f067aa0ba902b7").unwrap();
        assert_eq!(failed.kind, SpanKind::Server);
        assert_eq!(failed.status, Some(SpanStatus::Otel(OtelStatus::Error)));
        assert_eq!(failed.status_msg.as_deref(), Some("upstream refused"));
        assert_eq!(failed.t, "2026-10-20T09:21:52.000100Z");
        assert_eq!(failed.parent, None);
        let attrs = failed.attrs.as_ref().unwrap();
        assert_eq!(attrs["http.response.status_code"], 502);
        assert_eq!(attrs["exception.type"], "ECONNREFUSED");
        assert!(!attrs.contains_key("process.pid") && !attrs.contains_key("http.request.header.cookie"));
        let query = spans.iter().find(|s| s.span == "b7ad6b7169203331").unwrap();
        assert_eq!(
            (query.kind, query.status),
            (SpanKind::Client, Some(SpanStatus::Otel(OtelStatus::Ok)))
        );
        assert_eq!(query.attrs.as_ref().unwrap()["db.system"], "postgresql");

        let logs: ExportLogs = serde_json::from_str(NODE_LOGS).unwrap();
        let lines = logs.lines(NOW_US);
        assert_eq!(lines.len(), 2);
        assert_eq!(
            lines[0].1.msg.as_deref(),
            Some(r#"{"event":"checkout","items":3,"ok":false}"#)
        );
        assert_eq!(lines[0].1.severity, Some(Severity::Warn));
        assert_eq!(
            lines[0].1.trace.as_deref(),
            Some("4bf92f3577b34da6a3ce929d0e0e4736")
        );
        assert_eq!(lines[0].0, 1_792_488_112_000_200);
        assert_eq!(
            lines[1].0, 1_792_488_112_500_000,
            "observed time when no time is given"
        );
        assert_eq!(lines[1].1.severity, Some(Severity::Debug), "from its text");
    }

    #[test]
    fn past_32_attributes_a_span_keeps_32_and_says_how_many_it_lost() {
        let attributes: Vec<_> = (0..40)
            .map(|n| serde_json::json!({ "key": format!("http.x{n:02}"), "value": { "intValue": n } }))
            .collect();
        let export: ExportTraces = serde_json::from_value(
            serde_json::json!({ "resourceSpans": [{ "scopeSpans": [{ "spans": [{
                "traceId": DENO_TRACE, "spanId": "00f067aa0ba902b7", "name": "x",
                "startTimeUnixNano": 1, "endTimeUnixNano": 2, "attributes": attributes,
            }]}]}]}),
        )
        .unwrap();
        let lines = export.lines(&TraceBook::default(), NOW_US);
        assert_eq!(spans(&lines)[0].attrs.as_ref().unwrap().len(), ATTRIBUTES_KEPT);
        assert!(matches!(&lines[1], TraceLine::Gap(gap)
            if gap.dropped == 8 && gap.trace.as_deref() == Some(DENO_TRACE)));
    }

    #[test]
    fn past_256_spans_a_trace_keeps_256_and_one_gap_line_counts_the_rest() {
        let book = TraceBook::default();
        let trace = book.mint(NOW_US);
        let spans_json: Vec<_> = (1..=300u64)
            .map(|n| serde_json::json!({ "traceId": trace.to_string(), "spanId": format!("{n:016x}"), "name": "s" }))
            .collect();
        let export: ExportTraces = serde_json::from_value(
            serde_json::json!({ "resourceSpans": [{ "scopeSpans": [{ "spans": spans_json }]}]}),
        )
        .unwrap();
        let lines = export.lines(&book, NOW_US);
        assert_eq!(spans(&lines).len(), 256);
        assert!(spans(&lines).iter().all(|span| !span.foreign));
        let TraceLine::Gap(gap) = lines.last().unwrap() else {
            panic!("a gap line");
        };
        assert_eq!((gap.dropped, gap.trace.clone()), (44, Some(trace.to_string())));
    }

    #[test]
    fn a_span_without_ids_it_can_read_is_dropped_into_a_gap_line_of_no_trace() {
        let export: ExportTraces = serde_json::from_value(
            serde_json::json!({ "resourceSpans": [{ "scopeSpans": [{ "spans": [
                { "traceId": "", "spanId": "00f067aa0ba902b7" },
                { "traceId": "S/kvNXezTaajzpKdDg5HNg==", "spanId": "APBnqgupArc=" },
            ]}]}]}),
        )
        .unwrap();
        let lines = export.lines(&TraceBook::default(), NOW_US);
        assert!(matches!(lines.as_slice(), [TraceLine::Gap(gap)] if gap.dropped == 2 && gap.trace.is_none()));
    }

    #[test]
    fn a_metrics_export_parses_as_neither_spans_nor_records() {
        let traces: ExportTraces = serde_json::from_str(DENO_METRICS).unwrap();
        assert!(traces.lines(&TraceBook::default(), NOW_US).is_empty());
        let logs: ExportLogs = serde_json::from_str(DENO_METRICS).unwrap();
        assert!(logs.lines(NOW_US).is_empty());
    }
}
