//! The ids the proxy gives every request it routes, and the book of which ones it gave.
//!
//! A trace id is `hex(t_us >> 4)` in twelve digits followed by eighty random bits: a W3C trace id
//! that also says the hour it began in, which is how a control plane finds a trace's chunks
//! without an index (its wire contract with this host).
use std::collections::{HashMap, VecDeque};
use std::sync::{Mutex, OnceLock};

use hyper::header::{HeaderMap, HeaderName, HeaderValue};

const TRACEPARENT: &str = "traceparent";
const TRACESTATE: &str = "tracestate";

/// How long a minted id is still this proxy's: a reader looks for a trace in its start hour.
const MINTED_FOR_US: i64 = 3_600_000_000;
/// The ids remembered at most, about 18 MiB: past it the oldest are taken for foreign.
const MINTED_KEPT: usize = 1 << 18;
/// Foreign traces counted at most, so that a guest naming new ones cannot push minted ones out.
const FOREIGN_KEPT: usize = 1 << 12;
pub(crate) const SPANS_PER_TRACE: u32 = 256;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(crate) struct TraceId(u128);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SpanId(u64);

/// Bits from the operating system's generator: a v4 UUID's 122 random bits, the version and
/// variant nibbles left out.
fn random_bits() -> u128 {
    let bits = uuid::Uuid::new_v4().as_u128();
    let high = bits >> 80;
    let low = bits & ((1 << 62) - 1);
    (high << 62) | low
}

impl TraceId {
    pub(crate) fn minted_at(t_us: i64) -> Self {
        let time = (u128::try_from(t_us).unwrap_or(0) >> 4) & ((1 << 48) - 1);
        let random = random_bits() & ((1 << 80) - 1);
        Self((time << 80) | random | u128::from(time == 0 && random == 0))
    }

    /// When a minted id says its trace began, to the 16 µs.
    pub(crate) fn started_us(self) -> i64 {
        i64::try_from((self.0 >> 80) << 4).unwrap_or(i64::MAX)
    }

    pub(crate) fn parse(hex: &str) -> Option<Self> {
        let id = parse_hex(hex, 32).and_then(|id| u128::from_str_radix(id, 16).ok())?;
        (id != 0).then_some(Self(id))
    }
}

impl std::fmt::Display for TraceId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:032x}", self.0)
    }
}

impl SpanId {
    pub(crate) fn random() -> Self {
        Self(
            u64::try_from(random_bits() & u128::from(u64::MAX))
                .unwrap_or(1)
                .max(1),
        )
    }

    pub(crate) fn parse(hex: &str) -> Option<Self> {
        let id = parse_hex(hex, 16).and_then(|id| u64::from_str_radix(id, 16).ok())?;
        (id != 0).then_some(Self(id))
    }
}

impl std::fmt::Display for SpanId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:016x}", self.0)
    }
}

fn parse_hex(hex: &str, digits: usize) -> Option<&str> {
    (hex.len() == digits && hex.bytes().all(|b| b.is_ascii_hexdigit())).then_some(hex)
}

/// The trace a routed request starts: the proxy's root span in it, and the caller's own trace,
/// which is recorded and never continued.
#[derive(Debug, Clone, Copy)]
pub(crate) struct RequestTrace {
    pub(crate) trace: TraceId,
    pub(crate) span: SpanId,
    pub(crate) parent_trace: Option<TraceId>,
    pub(crate) started_us: i64,
}

impl RequestTrace {
    /// A new trace for the request whose headers these are: the caller's `traceparent` and
    /// `tracestate` taken off, ours put on for the guest.
    pub(crate) fn begin(headers: &mut HeaderMap, book: &TraceBook, started_us: i64) -> Self {
        let parent_trace = headers
            .get_all(TRACEPARENT)
            .iter()
            .find_map(|value| traceparent_trace(value.to_str().ok()?));
        headers.remove(TRACEPARENT);
        headers.remove(TRACESTATE);
        let trace = Self {
            trace: book.mint(started_us),
            span: SpanId::random(),
            parent_trace,
            started_us,
        };
        trace.answer(headers);
        trace
    }

    pub(crate) fn traceparent(&self) -> HeaderValue {
        HeaderValue::try_from(format!("00-{}-{}-01", self.trace, self.span))
            .expect("hex digits and dashes are a header value")
    }

    pub(crate) fn answer(&self, headers: &mut HeaderMap) {
        headers.insert(HeaderName::from_static(TRACEPARENT), self.traceparent());
    }
}

/// The trace id of a W3C `traceparent`; a version this proxy does not know is read for its first
/// four fields, as the recommendation asks.
fn traceparent_trace(value: &str) -> Option<TraceId> {
    let mut fields = value.trim().split('-');
    let version = fields.next().filter(|v| v.len() == 2 && *v != "ff")?;
    let (trace, span, flags) = (fields.next()?, fields.next()?, fields.next()?);
    let lower = |field: &str| field.bytes().all(|b| !b.is_ascii_uppercase());
    let well_formed = u8::from_str_radix(version, 16).is_ok()
        && lower(trace)
        && SpanId::parse(span).is_some()
        && lower(span)
        && flags.len() == 2
        && u8::from_str_radix(flags, 16).is_ok()
        && (version != "00" || fields.next().is_none());
    well_formed.then(|| TraceId::parse(trace)).flatten()
}

#[derive(Default)]
struct Seen {
    order: VecDeque<(i64, TraceId)>,
    spans: HashMap<TraceId, u32>,
}

impl Seen {
    fn keep(&mut self, now_us: i64, kept: usize) {
        while let Some(&(at, id)) = self.order.front() {
            if at >= now_us - MINTED_FOR_US && self.order.len() <= kept {
                break;
            }
            self.order.pop_front();
            self.spans.remove(&id);
        }
    }
}

/// What the receiver makes of one span: whether this proxy minted its trace, and whether the
/// trace still has room for it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Admission {
    pub(crate) foreign: bool,
    pub(crate) kept: bool,
}

/// The traces this proxy minted in the last hour, and how many spans each has been sent.
#[derive(Default)]
pub(crate) struct TraceBook {
    minted: Mutex<Seen>,
    foreign: Mutex<Seen>,
}

impl TraceBook {
    /// The one this host's proxy and receiver share.
    pub(crate) fn host() -> &'static TraceBook {
        static BOOK: OnceLock<TraceBook> = OnceLock::new();
        BOOK.get_or_init(TraceBook::default)
    }

    fn lock(seen: &Mutex<Seen>) -> std::sync::MutexGuard<'_, Seen> {
        seen.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    pub(crate) fn mint(&self, now_us: i64) -> TraceId {
        let id = TraceId::minted_at(now_us);
        self.remember(id);
        id
    }

    /// A trace minted here: its start is the one its id says.
    pub(crate) fn remember(&self, id: TraceId) {
        let mut minted = Self::lock(&self.minted);
        minted.order.push_back((id.started_us(), id));
        minted.spans.insert(id, 0);
        minted.keep(id.started_us(), MINTED_KEPT);
    }

    pub(crate) fn admit(&self, id: TraceId, now_us: i64) -> Admission {
        let mut minted = Self::lock(&self.minted);
        minted.keep(now_us, MINTED_KEPT);
        if let Some(spans) = minted.spans.get_mut(&id) {
            *spans += 1;
            return Admission {
                foreign: false,
                kept: *spans <= SPANS_PER_TRACE,
            };
        }
        drop(minted);
        let mut foreign = Self::lock(&self.foreign);
        foreign.keep(now_us, FOREIGN_KEPT);
        let spans = match foreign.spans.get_mut(&id) {
            Some(spans) => spans,
            None => {
                foreign.order.push_back((now_us, id));
                foreign.spans.entry(id).or_default()
            }
        };
        *spans += 1;
        Admission {
            foreign: true,
            kept: *spans <= SPANS_PER_TRACE,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const NOW_US: i64 = 1_792_152_300_123_456; // 2026-10-16T12:05:00.123456Z

    #[test]
    fn a_minted_id_is_32_hex_whose_first_twelve_say_when_it_began_to_sixteen_microseconds() {
        let id = TraceId::minted_at(NOW_US);
        let hex = id.to_string();
        assert_eq!(hex.len(), 32);
        assert!(hex
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)));
        assert_eq!(&hex[..12], &format!("{:012x}", NOW_US >> 4));
        let recovered = i64::from_str_radix(&hex[..12], 16).unwrap() << 4;
        assert_eq!(recovered, id.started_us());
        assert!(NOW_US - recovered < 16);
        assert_eq!(
            crate::domain::metrics::rollup::hour_key(recovered / 1_000_000),
            "2026101612"
        );
        assert_eq!(TraceId::parse(&hex), Some(id));
    }

    #[test]
    fn two_ids_minted_in_the_same_microsecond_differ_and_none_is_zero() {
        let ids: std::collections::BTreeSet<_> = (0..1000).map(|_| TraceId::minted_at(NOW_US)).collect();
        assert_eq!(ids.len(), 1000);
        assert!(TraceId::minted_at(0).0 != 0);
        assert!((0..1000).all(|_| SpanId::random().0 != 0));
        assert_eq!(SpanId::random().to_string().len(), 16);
    }

    #[test]
    fn an_all_zero_or_malformed_id_is_no_id() {
        assert_eq!(TraceId::parse(&"0".repeat(32)), None);
        assert_eq!(TraceId::parse("abc"), None);
        assert_eq!(TraceId::parse(&"g".repeat(32)), None);
        assert_eq!(SpanId::parse(&"0".repeat(16)), None);
        assert!(SpanId::parse("00f067aa0ba902b7").is_some());
    }

    #[test]
    fn the_callers_traceparent_and_tracestate_are_replaced_and_its_trace_kept_as_the_parent() {
        let book = TraceBook::default();
        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_static(TRACEPARENT),
            HeaderValue::from_static("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"),
        );
        headers.insert(
            HeaderName::from_static(TRACESTATE),
            HeaderValue::from_static("vendor=secret"),
        );
        let trace = RequestTrace::begin(&mut headers, &book, NOW_US);
        assert_eq!(
            trace.parent_trace.map(|t| t.to_string()).as_deref(),
            Some("4bf92f3577b34da6a3ce929d0e0e4736")
        );
        assert!(headers.get(TRACESTATE).is_none());
        let sent = headers.get_all(TRACEPARENT).iter().collect::<Vec<_>>();
        assert_eq!(sent.len(), 1);
        assert_eq!(
            sent[0].to_str().unwrap(),
            format!("00-{}-{}-01", trace.trace, trace.span)
        );
        assert!(!book.admit(trace.trace, NOW_US).foreign);
    }

    #[test]
    fn a_traceparent_that_is_not_one_is_dropped_without_becoming_a_parent() {
        for value in [
            "00-00000000000000000000000000000000-00f067aa0ba902b7-01",
            "00-4BF92F3577B34DA6A3CE929D0E0E4736-00f067aa0ba902b7-01",
            "00-4bf92f3577b34da6a3ce929d0e0e4736-0000000000000000-01",
            "ff-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01-extra",
            "garbage",
        ] {
            let mut headers = HeaderMap::new();
            headers.insert(
                HeaderName::from_static(TRACEPARENT),
                HeaderValue::from_static(value),
            );
            let trace = RequestTrace::begin(&mut headers, &TraceBook::default(), NOW_US);
            assert_eq!(trace.parent_trace, None, "{value}");
            assert_ne!(headers.get(TRACEPARENT).unwrap().to_str().unwrap(), value);
        }
        let mut later = HeaderMap::new();
        later.insert(
            HeaderName::from_static(TRACEPARENT),
            HeaderValue::from_static("01-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01-more"),
        );
        let trace = RequestTrace::begin(&mut later, &TraceBook::default(), NOW_US);
        assert!(
            trace.parent_trace.is_some(),
            "a later version is read for its first fields"
        );
    }

    #[test]
    fn a_trace_is_foreign_unless_minted_here_within_the_hour() {
        let book = TraceBook::default();
        let minted = book.mint(NOW_US);
        assert_eq!(
            book.admit(minted, NOW_US + 1),
            Admission {
                foreign: false,
                kept: true
            }
        );
        let stranger = TraceId::parse("4bf92f3577b34da6a3ce929d0e0e4736").unwrap();
        assert!(book.admit(stranger, NOW_US).foreign);
        assert!(book.admit(minted, NOW_US + MINTED_FOR_US + 16).foreign);
    }

    #[test]
    fn the_book_forgets_its_oldest_once_full() {
        let book = TraceBook::default();
        let first = book.mint(NOW_US);
        for n in 0..MINTED_KEPT {
            book.mint(NOW_US + i64::try_from(n).unwrap());
        }
        assert!(book.admit(first, NOW_US + 1_000_000).foreign);
        assert!(TraceBook::lock(&book.minted).order.len() <= MINTED_KEPT);
    }

    #[test]
    fn past_256_spans_a_trace_keeps_no_more() {
        let book = TraceBook::default();
        for id in [
            book.mint(NOW_US),
            TraceId::parse("4bf92f3577b34da6a3ce929d0e0e4736").unwrap(),
        ] {
            let kept = (0..300).filter(|_| book.admit(id, NOW_US).kept).count();
            assert_eq!(kept, SPANS_PER_TRACE as usize);
        }
    }
}
