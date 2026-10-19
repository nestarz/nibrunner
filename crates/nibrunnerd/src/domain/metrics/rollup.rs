//! The per-unit hour and day a control plane charts, summed from this page's own `nibrunner_app_`
//! counters and the per-app `nibrunner_proxy_` ones. The shapes are the control plane's `metrics/<host>/<yyyymmddhh>.json` and `<yyyymmdd>.json`,
//! which its readers parse: a field renamed here is a chart gone blank there.
use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// How often the counters are written: at most once a window, and only when one moved.
pub const ROLLUP_EVERY_S: i64 = 300;
/// How often the counters are read into the minute that just ended, in memory.
pub const COUNT_EVERY_S: i64 = 60;

/// One unit's counters over an hour or a day.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct UnitMetrics {
    /// Set on a unit's total, left out of its slots.
    #[serde(skip_serializing_if = "String::is_empty")]
    pub app: String,
    pub requests: u64,
    pub by_status: BTreeMap<String, u64>,
    /// Requests by latency bucket upper bound, one count per bucket rather than cumulative.
    pub latency_hist: BTreeMap<String, u64>,
    pub cpu_s: f64,
    pub mem_peak_mib: u64,
    pub rx_bytes: u64,
    pub tx_bytes: u64,
    pub wakes: u64,
    /// Wakes by how: a frozen microVM resumed in place, a snapshot restored, a cold boot.
    pub wakes_thawed: u64,
    pub wakes_restored: u64,
    pub wakes_booted: u64,
    /// Wakes whose microVM did not come back (its resume, dial or clock sync stalled), each
    /// answered by a cold boot instead: the fallbacks.
    pub wakes_failed: u64,
    pub sleeps: u64,
    pub cache_hit: u64,
    pub cache_stale: u64,
    pub cache_miss: u64,
    pub cache_bypass: u64,
    /// What the proxy's compression took from the app and gave out, every coding together.
    pub encoded_bytes_in: u64,
    pub encoded_bytes_out: u64,
}

impl UnitMetrics {
    fn add(&mut self, other: &Self) {
        for (k, v) in &other.by_status {
            *self.by_status.entry(k.clone()).or_default() += v;
        }
        for (k, v) in &other.latency_hist {
            *self.latency_hist.entry(k.clone()).or_default() += v;
        }
        self.requests += other.requests;
        self.cpu_s += other.cpu_s;
        self.mem_peak_mib = self.mem_peak_mib.max(other.mem_peak_mib);
        self.rx_bytes += other.rx_bytes;
        self.tx_bytes += other.tx_bytes;
        self.wakes += other.wakes;
        self.wakes_thawed += other.wakes_thawed;
        self.wakes_restored += other.wakes_restored;
        self.wakes_booted += other.wakes_booted;
        self.wakes_failed += other.wakes_failed;
        self.sleeps += other.sleeps;
        self.cache_hit += other.cache_hit;
        self.cache_stale += other.cache_stale;
        self.cache_miss += other.cache_miss;
        self.cache_bypass += other.cache_bypass;
        self.encoded_bytes_in += other.encoded_bytes_in;
        self.encoded_bytes_out += other.encoded_bytes_out;
    }
}

/// One unit's counters over the hour (or the day), its total's fields at the top as before, and
/// beside them the same counters for each minute of the hour (`"04"`), or each hour of the day
/// (`"14"`), that counted something. Slots are sparse and add up to the total, so a reader sums
/// whichever a range and a step need and derives percentiles from their buckets.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct UnitSlots {
    #[serde(flatten)]
    pub total: UnitMetrics,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub minutes: BTreeMap<String, UnitMetrics>,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub hours: BTreeMap<String, UnitMetrics>,
}

/// The day file has this shape too, its day key under `hour`: what the readers expect.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct MetricsHour {
    #[serde(alias = "day")]
    pub hour: String,
    pub at: String,
    pub units: BTreeMap<String, UnitSlots>,
}

type Slot = fn(&mut UnitSlots) -> &mut BTreeMap<String, UnitMetrics>;

fn utc(secs: i64) -> chrono::DateTime<chrono::Utc> {
    chrono::DateTime::from_timestamp(secs, 0).unwrap_or_default()
}
pub fn hour_key(secs: i64) -> String {
    utc(secs).format("%Y%m%d%H").to_string()
}
pub fn day_key(secs: i64) -> String {
    utc(secs).format("%Y%m%d").to_string()
}
/// A minute's slot in its hour file.
pub fn minute_slot(secs: i64) -> String {
    utc(secs).format("%M").to_string()
}
/// An hour's slot in its day file.
pub fn hour_slot(secs: i64) -> String {
    utc(secs).format("%H").to_string()
}
pub fn mmss(secs: i64) -> String {
    utc(secs).format("%M%S").to_string()
}
pub fn rfc3339(secs: i64) -> String {
    utc(secs).format("%Y-%m-%dT%H:%M:%SZ").to_string()
}
pub fn micros_rfc3339(micros: i64) -> String {
    let at = chrono::DateTime::from_timestamp_micros(micros).unwrap_or_default();
    at.format("%Y-%m-%dT%H:%M:%S%.6fZ").to_string()
}

/// Series by unit, then by name without `nibrunner_app_` (a proxy series keeps `proxy_`) and the
/// labels other than `app`.
pub type Samples = BTreeMap<String, BTreeMap<String, f64>>;

fn unit_series(line: &str) -> Option<&str> {
    let rest = line.strip_prefix("nibrunner_")?;
    rest.strip_prefix("app_")
        .or_else(|| rest.starts_with("proxy_").then_some(rest))
}

pub fn parse(page: &str) -> Samples {
    let mut out = Samples::new();
    for line in page.lines() {
        let Some(rest) = unit_series(line) else {
            continue;
        };
        let Some(split) = rest.find(['{', ' ']) else {
            continue;
        };
        let (series, rest) = rest.split_at(split);
        let (labels, value) = rest
            .strip_prefix('{')
            .and_then(|r| r.split_once('}'))
            .unwrap_or(("", rest));
        let Some(value) = value
            .split_whitespace()
            .next()
            .and_then(|v| v.parse::<f64>().ok())
        else {
            continue;
        };
        let pairs = labels.split(',').filter_map(|p| p.split_once('='));
        let (apps, others): (Vec<_>, Vec<_>) = pairs
            .map(|(k, v)| (k.trim(), v.trim().trim_matches('"')))
            .partition(|(k, _)| *k == "app");
        let Some((_, unit)) = apps.first() else {
            continue;
        };
        let others: Vec<String> = others.iter().map(|(k, v)| format!("{k}={v}")).collect();
        let key = match others.is_empty() {
            true => series.to_owned(),
            false => format!("{series}{{{}}}", others.join(",")),
        };
        *out.entry((*unit).to_owned()).or_default().entry(key).or_default() += value;
    }
    out
}

fn label<'a>(series: &'a str, k: &str) -> Option<&'a str> {
    let labels = series.split_once('{')?.1.trim_end_matches('}');
    labels
        .split(',')
        .find_map(|p| p.strip_prefix(k)?.strip_prefix('='))
}

fn status_class(class: &str) -> String {
    match class.chars().next() {
        Some(c @ '1'..='5') => format!("{c}xx"),
        _ => class.to_owned(),
    }
}

/// The hour and the day being counted. Held in memory only: a restart counts on top of what the
/// store holds of them, from a new baseline.
pub struct Counting {
    last: Option<Samples>,
    pub hour: String,
    pub day: String,
    hours: BTreeMap<String, UnitSlots>,
    days: BTreeMap<String, UnitSlots>,
    /// A counter moved and the write that says so has not succeeded yet.
    pub dirty: bool,
}

impl Counting {
    pub fn carried(t: i64, hour: Option<MetricsHour>, day: Option<MetricsHour>) -> Self {
        Self {
            last: None,
            hour: hour_key(t),
            day: day_key(t),
            hours: hour.map(|m| m.units).unwrap_or_default(),
            days: day.map(|m| m.units).unwrap_or_default(),
            dirty: false,
        }
    }

    /// Whether `t` falls in another hour or day than the one counted.
    pub fn ends_before(&self, t: i64) -> bool {
        hour_key(t) != self.hour || day_key(t) != self.day
    }

    pub fn start(&mut self, t: i64) {
        if hour_key(t) != self.hour {
            (self.hour, self.hours) = (hour_key(t), BTreeMap::new());
        }
        if day_key(t) != self.day {
            (self.day, self.days) = (day_key(t), BTreeMap::new());
        }
    }

    /// Adds what each counter counted since the last scrape to the totals and to the slots of
    /// `t`, the minute that just ended; whether one moved. A counter lower than it was counts from
    /// zero (the daemon restarted); one first seen after the baseline counts whole. Memory is a
    /// gauge: it rides along with counters and never causes a write, nor a slot.
    pub fn count(&mut self, sampled: Samples, apps: &BTreeMap<String, String>, t: i64) -> bool {
        let mut moved = false;
        for (unit, series) in &sampled {
            let Some(app) = apps.get(unit) else { continue };
            let last = self.last.as_ref().map(|l| l.get(unit));
            let mut m = UnitMetrics::default();
            // What each cumulative bucket counted, summed by `le` across the other labels (the
            // status class): a bucket is one series per class, and only their sum is the unit's.
            let mut buckets: BTreeMap<String, u64> = BTreeMap::new();
            let mut unit_moved = false;
            for (s, v) in series {
                let name = s.split_once('{').map_or(s.as_str(), |(n, _)| n);
                if name == "memory_used_bytes" {
                    m.mem_peak_mib = m.mem_peak_mib.max(*v as u64 >> 20);
                    continue;
                }
                let Some(last) = last else { continue };
                let d = match last.and_then(|l| l.get(s)) {
                    Some(l) if v >= l => v - l,
                    _ => *v,
                };
                let n = d.round() as u64;
                match name {
                    "requests_total" => {
                        m.requests += n;
                        let class = status_class(label(s, "class").unwrap_or("other"));
                        *m.by_status.entry(class).or_default() += n;
                    }
                    "request_seconds_bucket" => {
                        let le = label(s, "le").unwrap_or("+Inf");
                        *buckets.entry(le.to_owned()).or_default() += n;
                    }
                    "cpu_seconds_total" => m.cpu_s += d,
                    "network_bytes_total" => match label(s, "direction") {
                        Some("rx" | "in" | "receive" | "received") => m.rx_bytes += n,
                        Some(_) => m.tx_bytes += n,
                        None => {}
                    },
                    "wakes_total" => match label(s, "outcome") {
                        // The cold boot that answered it is the wake; the thaw is its failure.
                        Some("thaw-failed") => continue,
                        outcome => {
                            m.wakes += n;
                            match outcome {
                                Some("thawed") => m.wakes_thawed += n,
                                Some("restored") => m.wakes_restored += n,
                                Some("cold-boot") => m.wakes_booted += n,
                                _ => {}
                            }
                        }
                    },
                    "wake_failures_total" => match label(s, "phase") {
                        Some("resume" | "dial" | "sync") => m.wakes_failed += n,
                        _ => continue,
                    },
                    "sleeps_total" => m.sleeps += n,
                    "proxy_cache_requests_total" => match label(s, "outcome") {
                        Some("hit") => m.cache_hit += n,
                        Some("stale") => m.cache_stale += n,
                        Some("miss") => m.cache_miss += n,
                        Some("bypass") => m.cache_bypass += n,
                        _ => {}
                    },
                    "proxy_encode_bytes_total" => match label(s, "direction") {
                        Some("in") => m.encoded_bytes_in += n,
                        Some("out") => m.encoded_bytes_out += n,
                        _ => {}
                    },
                    _ => continue,
                }
                unit_moved |= d > 0.0;
            }
            let mut buckets: Vec<(f64, String, u64)> = buckets
                .into_iter()
                .map(|(le, n)| (le.parse().unwrap_or(f64::INFINITY), le, n))
                .collect();
            buckets.sort_by(|a, b| a.0.total_cmp(&b.0));
            let mut below = 0;
            for (_, le, n) in buckets {
                m.latency_hist.insert(le, n.saturating_sub(below));
                below = below.max(n);
            }
            let tiers: [(&mut BTreeMap<String, UnitSlots>, String, Slot); 2] = [
                (&mut self.hours, minute_slot(t), |u| &mut u.minutes),
                (&mut self.days, hour_slot(t), |u| &mut u.hours),
            ];
            for (held, slot, slots) in tiers {
                if unit_moved || held.contains_key(unit) {
                    let h = held.entry(unit.clone()).or_default();
                    h.total.add(&m);
                    h.total.app.clone_from(app);
                    if unit_moved {
                        slots(h).entry(slot).or_default().add(&m);
                    }
                }
            }
            moved |= unit_moved;
        }
        self.last = Some(sampled);
        moved
    }

    /// The hour and the day as they stand, keyed by their key.
    pub fn documents(&self, now: i64) -> [(String, MetricsHour); 2] {
        let document = |key: &str, units: &BTreeMap<String, UnitSlots>| MetricsHour {
            hour: key.to_owned(),
            at: rfc3339(now),
            units: units.clone(),
        };
        [
            (self.hour.clone(), document(&self.hour, &self.hours)),
            (self.day.clone(), document(&self.day, &self.days)),
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const HOUR: i64 = 1_792_152_000; // 2026-10-16T12:00:00Z

    fn page(requests: u64, cpu: f64) -> String {
        format!(
            "# TYPE nibrunner_app_requests_total counter\n\
             nibrunner_app_requests_total{{app=\"site\",class=\"2xx\"}} {requests}\n\
             nibrunner_app_requests_total{{app=\"site\",class=\"5xx\"}} 1\n\
             nibrunner_app_request_seconds_bucket{{app=\"site\",class=\"2xx\",le=\"0.005\"}} {requests}\n\
             nibrunner_app_request_seconds_bucket{{app=\"site\",class=\"2xx\",le=\"+Inf\"}} {requests}\n\
             nibrunner_app_request_seconds_bucket{{app=\"site\",class=\"5xx\",le=\"0.005\"}} 0\n\
             nibrunner_app_request_seconds_bucket{{app=\"site\",class=\"5xx\",le=\"+Inf\"}} 1\n\
             nibrunner_app_cpu_seconds_total{{app=\"site\"}} {cpu}\n\
             nibrunner_app_memory_used_bytes{{app=\"site\"}} 67108864\n\
             nibrunner_app_network_bytes_total{{app=\"site\",direction=\"rx\"}} 100\n\
             nibrunner_app_network_bytes_total{{app=\"site\",direction=\"tx\"}} 200\n\
             nibrunner_app_wakes_total{{app=\"site\",outcome=\"thawed\"}} {requests}\n\
             nibrunner_app_wakes_total{{app=\"site\",outcome=\"cold-boot\"}} 1\n\
             nibrunner_app_wakes_total{{app=\"site\",outcome=\"thaw-failed\"}} 1\n\
             nibrunner_app_wake_failures_total{{app=\"site\",phase=\"dial\"}} {requests}\n\
             nibrunner_app_wake_failures_total{{app=\"site\",phase=\"ready\"}} {requests}\n\
             nibrunner_proxy_requests_total{{app=\"site\",outcome=\"served\"}} {requests}\n\
             nibrunner_proxy_cache_requests_total{{app=\"site\",outcome=\"hit\"}} {requests}\n\
             nibrunner_proxy_cache_requests_total{{app=\"site\",outcome=\"stale\"}} 1\n\
             nibrunner_proxy_cache_requests_total{{app=\"site\",outcome=\"miss\"}} {}\n\
             nibrunner_proxy_cache_requests_total{{app=\"site\",outcome=\"bypass\"}} 0\n\
             nibrunner_proxy_cache_bytes 4096\n\
             nibrunner_proxy_encode_bytes_total{{app=\"site\",encoding=\"br\",direction=\"in\"}} {}\n\
             nibrunner_proxy_encode_bytes_total{{app=\"site\",encoding=\"br\",direction=\"out\"}} {requests}\n\
             nibrunner_proxy_encode_bytes_total{{app=\"site\",encoding=\"gzip\",direction=\"in\"}} {}\n\
             nibrunner_proxy_encode_bytes_total{{app=\"site\",encoding=\"gzip\",direction=\"out\"}} {requests}\n\
             nibrunner_app_requests_total{{app=\"build-x\",class=\"2xx\"}} 9\n",
            requests * 2,
            requests * 10,
            requests * 10,
        )
    }

    fn units() -> BTreeMap<String, String> {
        [("site".to_string(), "site".to_string())].into()
    }

    #[test]
    fn the_first_scrape_is_the_baseline_and_only_what_moved_after_it_is_counted() {
        let mut counting = Counting::carried(HOUR, None, None);
        assert!(!counting.count(parse(&page(10, 4.0)), &units(), HOUR + 240));
        assert!(
            !counting.count(parse(&page(10, 4.0)), &units(), HOUR + 240),
            "nothing moved"
        );
        assert!(counting.count(parse(&page(12, 5.5)), &units(), HOUR + 240));
        let [(hour, document), (day, _)] = counting.documents(HOUR + 600);
        assert_eq!((hour.as_str(), day.as_str()), ("2026101612", "20261016"));
        let site = &document.units["site"].total;
        assert_eq!(site.requests, 2);
        assert_eq!(
            site.by_status,
            [("2xx".to_string(), 2), ("5xx".to_string(), 0)].into()
        );
        assert_eq!(
            site.latency_hist,
            [("0.005".to_string(), 2), ("+Inf".to_string(), 0)].into()
        );
        assert_eq!((site.cpu_s, site.mem_peak_mib), (1.5, 64));
        assert_eq!(
            (
                site.cache_hit,
                site.cache_stale,
                site.cache_miss,
                site.cache_bypass
            ),
            (2, 0, 4, 0)
        );
        assert_eq!((site.encoded_bytes_in, site.encoded_bytes_out), (40, 4));
        assert_eq!(
            (
                site.wakes,
                site.wakes_thawed,
                site.wakes_booted,
                site.wakes_failed
            ),
            (2, 2, 0, 2),
            "a wake by how; a failed thaw is not a wake, and a probe that stalled is no fallback"
        );
        assert!(
            !document.units.contains_key("build-x"),
            "a unit without labels is not charted"
        );
    }

    #[test]
    fn the_latency_buckets_of_every_status_class_are_summed_before_they_are_split() {
        let page = |ok: u64, failed: u64| {
            format!(
                "nibrunner_app_requests_total{{app=\"site\",class=\"2xx\"}} {ok}\n\
                 nibrunner_app_requests_total{{app=\"site\",class=\"5xx\"}} {failed}\n\
                 nibrunner_app_request_seconds_bucket{{app=\"site\",class=\"2xx\",le=\"0.005\"}} {ok}\n\
                 nibrunner_app_request_seconds_bucket{{app=\"site\",class=\"2xx\",le=\"0.1\"}} {ok}\n\
                 nibrunner_app_request_seconds_bucket{{app=\"site\",class=\"2xx\",le=\"+Inf\"}} {ok}\n\
                 nibrunner_app_request_seconds_bucket{{app=\"site\",class=\"5xx\",le=\"0.005\"}} 0\n\
                 nibrunner_app_request_seconds_bucket{{app=\"site\",class=\"5xx\",le=\"0.1\"}} {failed}\n\
                 nibrunner_app_request_seconds_bucket{{app=\"site\",class=\"5xx\",le=\"+Inf\"}} {failed}\n"
            )
        };
        let mut counting = Counting::carried(HOUR, None, None);
        counting.count(parse(&page(0, 0)), &units(), HOUR + 240);
        assert!(counting.count(parse(&page(3, 1)), &units(), HOUR + 240));
        let site = &counting.documents(HOUR)[0].1.units["site"].total;
        assert_eq!(site.requests, 4);
        assert_eq!(
            site.by_status,
            [("2xx".to_string(), 3), ("5xx".to_string(), 1)].into()
        );
        assert_eq!(
            site.latency_hist,
            [
                ("0.005".to_string(), 3),
                ("0.1".to_string(), 1),
                ("+Inf".to_string(), 0)
            ]
            .into(),
            "three 2xx under 5ms and one 5xx between 5ms and 100ms"
        );
    }

    #[test]
    fn a_counter_that_went_down_counts_from_zero_and_a_new_hour_starts_empty() {
        let mut counting = Counting::carried(HOUR, None, None);
        counting.count(parse(&page(10, 4.0)), &units(), HOUR + 240);
        assert!(counting.count(parse(&page(3, 1.0)), &units(), HOUR + 240));
        assert_eq!(counting.documents(HOUR)[0].1.units["site"].total.requests, 3);
        assert!(counting.ends_before(HOUR + 3600));
        counting.start(HOUR + 3600);
        assert!(counting.documents(HOUR + 3600)[0].1.units.is_empty());
        assert_eq!(
            counting.documents(HOUR + 3600)[1].1.units["site"].total.requests,
            3
        );
    }

    #[test]
    fn the_hour_file_is_written_in_the_shape_the_control_plane_reads() {
        let point = UnitMetrics {
            app: String::new(),
            requests: 3,
            by_status: [("2xx".to_string(), 2), ("5xx".to_string(), 1)].into(),
            latency_hist: [("0.005".to_string(), 2), ("+Inf".to_string(), 1)].into(),
            cpu_s: 1.5,
            mem_peak_mib: 64,
            rx_bytes: 100,
            tx_bytes: 200,
            wakes: 1,
            wakes_thawed: 1,
            wakes_restored: 0,
            wakes_booted: 0,
            wakes_failed: 0,
            sleeps: 0,
            cache_hit: 2,
            cache_stale: 0,
            cache_miss: 1,
            cache_bypass: 0,
            encoded_bytes_in: 30_000,
            encoded_bytes_out: 2_000,
        };
        let site = UnitSlots {
            total: UnitMetrics {
                app: "site".into(),
                ..point.clone()
            },
            minutes: [("04".to_string(), point)].into(),
            hours: BTreeMap::new(),
        };
        let document = MetricsHour {
            hour: "2026101612".into(),
            at: rfc3339(HOUR + 600),
            units: [("site".to_string(), site)].into(),
        };
        let expected = include_bytes!("../../../tests/fixtures/telemetry/metrics-hour.json");
        assert_eq!(serde_json::to_vec(&document).unwrap(), expected);
    }

    #[test]
    fn each_minute_and_each_hour_that_counted_has_a_slot_and_the_slots_add_up_to_the_total() {
        let mut counting = Counting::carried(HOUR, None, None);
        counting.count(parse(&page(10, 4.0)), &units(), HOUR + 59);
        assert!(counting.count(parse(&page(12, 4.5)), &units(), HOUR + 119));
        assert!(
            !counting.count(parse(&page(12, 4.5)), &units(), HOUR + 179),
            "a quiet minute"
        );
        assert!(counting.count(parse(&page(15, 5.5)), &units(), HOUR + 239));
        let [(_, hour), (_, day)] = counting.documents(HOUR + 240);
        let site = &hour.units["site"];
        assert_eq!(
            site.minutes.keys().collect::<Vec<_>>(),
            ["01", "03"],
            "only the minutes that counted"
        );
        assert_eq!((site.minutes["01"].requests, site.minutes["03"].requests), (2, 3));
        assert_eq!(site.total.requests, 5);
        let mut summed = UnitMetrics::default();
        site.minutes.values().for_each(|slot| summed.add(slot));
        assert_eq!(
            UnitMetrics {
                app: "site".into(),
                ..summed
            },
            site.total
        );
        assert!(site.hours.is_empty() && site.minutes["01"].app.is_empty());
        let site = &day.units["site"];
        assert_eq!(site.hours.keys().collect::<Vec<_>>(), ["12"]);
        assert_eq!((site.hours["12"].requests, site.total.requests), (5, 5));
        assert!(site.minutes.is_empty());
    }

    #[test]
    fn a_file_written_before_the_slots_reads_as_its_totals() {
        let old = br#"{"hour":"2026101612","at":"2026-10-16T12:10:00Z","units":{"site":{"app":"site","requests":3,"by_status":{"2xx":3}}}}"#;
        let read: MetricsHour = serde_json::from_slice(old).unwrap();
        assert_eq!(read.units["site"].total.requests, 3);
        assert!(read.units["site"].minutes.is_empty());
        let mut counting = Counting::carried(HOUR + 600, Some(read), None);
        counting.count(parse(&page(10, 4.0)), &units(), HOUR + 659);
        counting.count(parse(&page(11, 4.0)), &units(), HOUR + 719);
        let site = &counting.documents(HOUR + 720)[0].1.units["site"];
        assert_eq!((site.total.requests, site.minutes["11"].requests), (4, 1));
    }

    #[test]
    fn keys_and_times_are_spelled_as_the_control_plane_spells_them() {
        assert_eq!(hour_key(HOUR + 305), "2026101612");
        assert_eq!(day_key(HOUR + 305), "20261016");
        assert_eq!(mmss(HOUR + 305), "0505");
        assert_eq!(
            (minute_slot(HOUR + 305), hour_slot(HOUR + 305)),
            ("05".into(), "12".into())
        );
        assert_eq!(rfc3339(HOUR + 5), "2026-10-16T12:00:05Z");
        assert_eq!(
            micros_rfc3339(HOUR * 1_000_000 + 123_000),
            "2026-10-16T12:00:00.123000Z"
        );
    }
}
