use std::collections::BTreeMap;

use protocol::AppId;

use crate::firewall::{APP_ACTIVITY_COUNTER_PREFIX, APP_RECEIVED_COUNTER_PREFIX, APP_SENT_COUNTER_PREFIX};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Counted {
    pub packets: u64,
    pub bytes: u64,
}

/// Total network usage and the subset initiated towards the guest, excluding replies to its egress.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct AppTraffic {
    pub received: Counted,
    pub sent: Counted,
    pub activity: Counted,
}

pub fn parse_app_traffic(json: &str) -> BTreeMap<AppId, AppTraffic> {
    let mut traffic = BTreeMap::new();
    let Ok(parsed) = serde_json::from_str::<serde_json::Value>(json) else {
        return traffic;
    };
    let Some(entries) = parsed.get("nftables").and_then(|v| v.as_array()) else {
        return traffic;
    };
    for entry in entries {
        let Some(counter) = entry.get("counter").and_then(|v| v.as_object()) else {
            continue;
        };
        let (Some(name), Some(packets), Some(bytes)) = (
            counter.get("name").and_then(|v| v.as_str()),
            counter.get("packets").and_then(|v| v.as_u64()),
            counter.get("bytes").and_then(|v| v.as_u64()),
        ) else {
            continue;
        };
        let counted = Counted { packets, bytes };
        if let Some(app_id) = app_id_after(APP_RECEIVED_COUNTER_PREFIX, name) {
            traffic.entry(app_id).or_default().received = counted;
        } else if let Some(app_id) = app_id_after(APP_SENT_COUNTER_PREFIX, name) {
            traffic.entry(app_id).or_default().sent = counted;
        } else if let Some(app_id) = app_id_after(APP_ACTIVITY_COUNTER_PREFIX, name) {
            traffic.entry(app_id).or_default().activity = counted;
        }
    }
    traffic
}

fn app_id_after(prefix: &str, name: &str) -> Option<AppId> {
    name.strip_prefix(prefix)
        .and_then(|value| AppId::parse(value).ok())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::firewall::{app_received_counter_name, app_sent_counter_name};

    fn counters(entries: &[(&str, u64)]) -> String {
        let mut list = vec![serde_json::json!({ "metainfo": { "version": "1.0.4" } })];
        list.extend(entries.iter().map(|(name, bytes)| {
            serde_json::json!({ "counter": { "family": "ip", "name": name, "table": "nibrun", "handle": 2, "packets": 3, "bytes": bytes } })
        }));
        serde_json::json!({ "nftables": list }).to_string()
    }

    #[test]
    fn a_counter_is_attributed_by_its_name_and_every_app_is_read() {
        let app = AppId::parse("0198f3aa-1c2d-7e4b-9f11-a0b1c2d3e4f5").unwrap();
        let other = AppId::parse("0198f3bb-2d3e-7f5c-8a22-b1c2d3e4f5a6").unwrap();
        let traffic = parse_app_traffic(&counters(&[
            (&app_received_counter_name(&app), 512),
            (&app_received_counter_name(&other), 1024),
        ]));
        assert_eq!(
            traffic.get(&app),
            Some(&AppTraffic {
                received: Counted {
                    packets: 3,
                    bytes: 512
                },
                sent: Counted::default(),
                activity: Counted::default(),
            })
        );
        assert_eq!(traffic.get(&other).map(|t| t.received.bytes), Some(1024));
        assert_eq!(traffic.len(), 2);
    }

    #[test]
    fn which_way_a_counter_faces_is_read_off_its_name_and_both_land_on_one_app() {
        let app = AppId::parse("0198f3aa-1c2d-7e4b-9f11-a0b1c2d3e4f5").unwrap();
        let traffic = parse_app_traffic(&counters(&[
            (&app_sent_counter_name(&app), 4096),
            (&app_received_counter_name(&app), 512),
        ]));
        assert_eq!(traffic.len(), 1, "one app, however many counters it has");
        let held = traffic.get(&app).copied().unwrap_or_default();
        assert_eq!(held.received.bytes, 512);
        assert_eq!(held.sent.bytes, 4096);
    }

    #[test]
    fn an_app_only_one_way_was_read_of_counts_the_other_as_nothing_rather_than_going_missing() {
        let app = AppId::parse("0198f3aa-1c2d-7e4b-9f11-a0b1c2d3e4f5").unwrap();
        let traffic = parse_app_traffic(&counters(&[(&app_sent_counter_name(&app), 4096)]));
        let held = traffic.get(&app).copied().unwrap_or_default();
        assert_eq!(held.sent.bytes, 4096);
        assert_eq!(held.received, Counted::default());
    }

    #[test]
    fn background_replies_are_bandwidth_but_not_activity() {
        let app = AppId::parse("app-1").unwrap();
        let traffic = parse_app_traffic(&counters(&[
            (&app_received_counter_name(&app), 4096),
            (&crate::firewall::app_activity_counter_name(&app), 128),
        ]));
        assert_eq!(traffic[&app].received.bytes, 4096);
        assert_eq!(traffic[&app].activity.bytes, 128);
    }

    #[test]
    fn what_is_not_an_app_counter_is_skipped_rather_than_parsed() {
        assert!(parse_app_traffic(&counters(&[("something_else", 8)])).is_empty());
        assert!(parse_app_traffic(&counters(&[("rx_has.a.dot", 8)])).is_empty());
        assert!(parse_app_traffic(&counters(&[(&format!("rx_{}", "x".repeat(64)), 8)])).is_empty());
        assert!(parse_app_traffic(&counters(&[("app_0198f3aa-1c2d-7e4b-9f11-a0b1c2d3e4f5", 8)])).is_empty());
        assert!(parse_app_traffic("not json").is_empty());
        assert!(parse_app_traffic("{}").is_empty());
        assert!(parse_app_traffic(r#"{"nftables":[{"counter":{"name":5}}]}"#).is_empty());
    }
}
