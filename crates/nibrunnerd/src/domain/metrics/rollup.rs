//! The per-unit hour and day a control plane charts, summed from this page's own `nibrunner_app_`
//! counters. The shapes are microfleets' `metrics/<host>/<yyyymmddhh>.json` and `<yyyymmdd>.json`,
//! which its readers parse: a field renamed here is a chart gone blank there.
use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

pub const ROLLUP_EVERY_S: i64 = 300;

/// One unit's counters over an hour or a day.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct UnitMetrics {
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
    pub sleeps: u64,
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
        self.sleeps += other.sleeps;
    }
}

/// The day file has this shape too, its day key under `hour`: what the readers expect.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct MetricsHour {
    #[serde(alias = "day")]
    pub hour: String,
    pub at: String,
    pub units: BTreeMap<String, UnitMetrics>,
}

fn utc(secs: i64) -> chrono::DateTime<chrono::Utc> {
    chrono::DateTime::from_timestamp(secs, 0).unwrap_or_default()
}
pub fn hour_key(secs: i64) -> String {
    utc(secs).format("%Y%m%d%H").to_string()
}
pub fn day_key(secs: i64) -> String {
    utc(secs).format("%Y%m%d").to_string()
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

/// Series by unit, then by name without `nibrunner_app_` and the labels other than `app`.
pub type Samples = BTreeMap<String, BTreeMap<String, f64>>;

pub fn parse(page: &str) -> Samples {
    let mut out = Samples::new();
    for line in page.lines() {
        let Some(rest) = line.strip_prefix("nibrunner_app_") else {
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
    hours: BTreeMap<String, UnitMetrics>,
    days: BTreeMap<String, UnitMetrics>,
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

    /// Adds what each counter counted since the last scrape; whether one moved. A counter lower
    /// than it was counts from zero (the daemon restarted); one first seen after the baseline
    /// counts whole. Memory is a gauge: it rides along with counters and never causes a write.
    pub fn count(&mut self, sampled: Samples, apps: &BTreeMap<String, String>) -> bool {
        let mut moved = false;
        for (unit, series) in &sampled {
            let Some(app) = apps.get(unit) else { continue };
            let last = self.last.as_ref().map(|l| l.get(unit));
            let mut m = UnitMetrics::default();
            let mut buckets: Vec<(f64, String, u64)> = vec![];
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
                        buckets.push((le.parse().unwrap_or(f64::INFINITY), le.to_owned(), n));
                    }
                    "cpu_seconds_total" => m.cpu_s += d,
                    "network_bytes_total" => match label(s, "direction") {
                        Some("rx" | "in" | "receive" | "received") => m.rx_bytes += n,
                        Some(_) => m.tx_bytes += n,
                        None => {}
                    },
                    "wakes_total" => m.wakes += n,
                    "sleeps_total" => m.sleeps += n,
                    _ => continue,
                }
                unit_moved |= d > 0.0;
            }
            buckets.sort_by(|a, b| a.0.total_cmp(&b.0));
            let mut below = 0;
            for (_, le, n) in buckets {
                m.latency_hist.insert(le, n.saturating_sub(below));
                below = below.max(n);
            }
            for held in [&mut self.hours, &mut self.days] {
                if unit_moved || held.contains_key(unit) {
                    let h = held.entry(unit.clone()).or_default();
                    h.add(&m);
                    h.app.clone_from(app);
                }
            }
            moved |= unit_moved;
        }
        self.last = Some(sampled);
        moved
    }

    /// The hour and the day as they stand, keyed by their key.
    pub fn documents(&self, now: i64) -> [(String, MetricsHour); 2] {
        let document = |key: &str, units: &BTreeMap<String, UnitMetrics>| MetricsHour {
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
             nibrunner_app_request_seconds_bucket{{app=\"site\",le=\"0.005\"}} {requests}\n\
             nibrunner_app_request_seconds_bucket{{app=\"site\",le=\"+Inf\"}} {}\n\
             nibrunner_app_cpu_seconds_total{{app=\"site\"}} {cpu}\n\
             nibrunner_app_memory_used_bytes{{app=\"site\"}} 67108864\n\
             nibrunner_app_network_bytes_total{{app=\"site\",direction=\"rx\"}} 100\n\
             nibrunner_app_network_bytes_total{{app=\"site\",direction=\"tx\"}} 200\n\
             nibrunner_app_wakes_total{{app=\"site\"}} 1\n\
             nibrunner_app_requests_total{{app=\"build-x\",class=\"2xx\"}} 9\n",
            requests + 1
        )
    }

    fn units() -> BTreeMap<String, String> {
        [("site".to_string(), "site".to_string())].into()
    }

    #[test]
    fn the_first_scrape_is_the_baseline_and_only_what_moved_after_it_is_counted() {
        let mut counting = Counting::carried(HOUR, None, None);
        assert!(!counting.count(parse(&page(10, 4.0)), &units()));
        assert!(!counting.count(parse(&page(10, 4.0)), &units()), "nothing moved");
        assert!(counting.count(parse(&page(12, 5.5)), &units()));
        let [(hour, document), (day, _)] = counting.documents(HOUR + 600);
        assert_eq!((hour.as_str(), day.as_str()), ("2026101612", "20261016"));
        let site = &document.units["site"];
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
        assert!(
            !document.units.contains_key("build-x"),
            "a unit without labels is not charted"
        );
    }

    #[test]
    fn a_counter_that_went_down_counts_from_zero_and_a_new_hour_starts_empty() {
        let mut counting = Counting::carried(HOUR, None, None);
        counting.count(parse(&page(10, 4.0)), &units());
        assert!(counting.count(parse(&page(3, 1.0)), &units()));
        assert_eq!(counting.documents(HOUR)[0].1.units["site"].requests, 3);
        assert!(counting.ends_before(HOUR + 3600));
        counting.start(HOUR + 3600);
        assert!(counting.documents(HOUR + 3600)[0].1.units.is_empty());
        assert_eq!(counting.documents(HOUR + 3600)[1].1.units["site"].requests, 3);
    }

    #[test]
    fn the_hour_file_is_written_in_the_shape_microfleets_reads() {
        let units = [(
            "site".to_string(),
            UnitMetrics {
                app: "site".into(),
                requests: 3,
                by_status: [("2xx".to_string(), 2), ("5xx".to_string(), 1)].into(),
                latency_hist: [("0.005".to_string(), 2), ("+Inf".to_string(), 1)].into(),
                cpu_s: 1.5,
                mem_peak_mib: 64,
                rx_bytes: 100,
                tx_bytes: 200,
                wakes: 1,
                sleeps: 0,
            },
        )];
        let document = MetricsHour {
            hour: "2026101612".into(),
            at: rfc3339(HOUR + 600),
            units: units.into(),
        };
        let expected = include_bytes!("../../../tests/fixtures/telemetry/metrics-hour.json");
        assert_eq!(serde_json::to_vec(&document).unwrap(), expected);
    }

    #[test]
    fn keys_and_times_are_spelled_as_microfleets_spells_them() {
        assert_eq!(hour_key(HOUR + 305), "2026101612");
        assert_eq!(day_key(HOUR + 305), "20261016");
        assert_eq!(mmss(HOUR + 305), "0505");
        assert_eq!(rfc3339(HOUR + 5), "2026-10-16T12:00:05Z");
        assert_eq!(
            micros_rfc3339(HOUR * 1_000_000 + 123_000),
            "2026-10-16T12:00:00.123000Z"
        );
    }
}
