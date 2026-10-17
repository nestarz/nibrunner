use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;

use crate::adapters::logs::object_sink::{Bucket, TelemetryError};
use crate::controllers::Controller;
use crate::domain::metrics::rollup::{parse, Counting, MetricsHour, ROLLUP_EVERY_S};
use crate::host::Host;

/// How often a live tail of an app that printed is rewritten, and a full chunk written early.
const SHIP_INTERVAL: Duration = Duration::from_secs(5);

pub struct TelemetryController {
    host: Arc<Host>,
    rollup: tokio::sync::Mutex<Rollup>,
}

impl TelemetryController {
    pub fn new(host: Arc<Host>) -> Arc<Self> {
        Arc::new(Self {
            host,
            rollup: tokio::sync::Mutex::default(),
        })
    }

    pub async fn tick(&self, now: i64) {
        let Some(sink) = &self.host.telemetry else { return };
        if let Err(error) = sink.ship(now).await {
            tracing::warn!(%error, "tenant output was kept for the next write to its sink");
        }
        let Some(bucket) = sink.metrics() else { return };
        let mut rollup = self.rollup.lock().await;
        if !rollup.due(now) {
            return;
        }
        let (host, units) = sink.units();
        let page = crate::run::scrape_page(&self.host).await;
        if let Err(error) = rollup.advance(bucket, &host, &units, &page, now).await {
            tracing::warn!(%error, "an hour of counters was kept for the next write to its sink");
        }
    }
}

#[async_trait]
impl Controller for TelemetryController {
    fn name(&self) -> &'static str {
        "telemetry"
    }

    async fn run(&self) {
        loop {
            tokio::time::sleep(SHIP_INTERVAL).await;
            self.tick(crate::clock::now_ms() / 1000).await;
        }
    }
}

/// The hour and day being counted, rewritten at a [`ROLLUP_EVERY_S`] boundary only when a counter
/// moved, and once more at the hour's end when a write failed.
#[derive(Default)]
struct Rollup {
    slot: Option<i64>,
    counting: Option<Counting>,
}

impl Rollup {
    fn due(&mut self, now: i64) -> bool {
        let slot = now.div_euclid(ROLLUP_EVERY_S) * ROLLUP_EVERY_S;
        self.slot.replace(slot) != Some(slot)
    }

    async fn advance(
        &mut self,
        bucket: &Bucket,
        host: &str,
        units: &BTreeMap<String, String>,
        page: &str,
        now: i64,
    ) -> Result<(), TelemetryError> {
        if host.is_empty() {
            return Ok(());
        }
        let slot = now.div_euclid(ROLLUP_EVERY_S) * ROLLUP_EVERY_S;
        // What the last interval counted belongs to the slot that just ended.
        let ended = slot - 1;
        let counting = match &mut self.counting {
            Some(counting) => counting,
            None => self.counting.insert(carried(bucket, host, ended).await),
        };
        roll(bucket, host, counting, ended).await?;
        counting.dirty |= counting.count(parse(page), units);
        if counting.dirty {
            write(bucket, host, counting, now).await?;
        }
        roll(bucket, host, counting, slot).await
    }
}

/// Counting on top of what an earlier run of this host wrote of the hour and the day of `t`.
async fn carried(bucket: &Bucket, host: &str, t: i64) -> Counting {
    let empty = Counting::carried(t, None, None);
    let read = |key: String| async move {
        let held = bucket
            .get(&format!("metrics/{host}/{key}.json"))
            .await
            .ok()
            .flatten();
        held.and_then(|body| serde_json::from_slice::<MetricsHour>(&body).ok())
    };
    Counting::carried(t, read(empty.hour.clone()).await, read(empty.day.clone()).await)
}

async fn roll(bucket: &Bucket, host: &str, counting: &mut Counting, t: i64) -> Result<(), TelemetryError> {
    if !counting.ends_before(t) {
        return Ok(());
    }
    if counting.dirty {
        write(bucket, host, counting, t).await?;
    }
    counting.start(t);
    Ok(())
}

async fn write(bucket: &Bucket, host: &str, counting: &mut Counting, now: i64) -> Result<(), TelemetryError> {
    for (key, document) in counting.documents(now) {
        let body = serde_json::to_vec(&document).expect("counters always serialize");
        bucket
            .put(&format!("metrics/{host}/{key}.json"), body, false)
            .await?;
    }
    counting.dirty = false;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;
    use object_store::ObjectStore;

    const HOUR: i64 = 1_792_152_000; // 2026-10-16T12:00:00Z

    fn page(requests: u64) -> String {
        format!("nibrunner_app_requests_total{{app=\"site\",class=\"2xx\"}} {requests}\n")
    }

    async fn written(store: &InMemory) -> Vec<String> {
        let mut keys: Vec<String> = futures::TryStreamExt::try_collect::<Vec<_>>(store.list(None))
            .await
            .unwrap()
            .into_iter()
            .map(|meta| meta.location.to_string())
            .collect();
        keys.sort();
        keys
    }

    #[tokio::test]
    async fn the_hour_and_day_are_written_only_when_a_counter_moved_and_carried_across_a_restart() {
        let store = Arc::new(InMemory::new());
        let bucket = Bucket::new(store.clone(), None, "acme");
        let units: BTreeMap<String, String> = [("site".to_string(), "site".to_string())].into();
        let mut rollup = Rollup::default();
        for (at, requests) in [(HOUR + 300, 5), (HOUR + 600, 5)] {
            assert!(rollup.due(at));
            rollup
                .advance(&bucket, "ovh-1", &units, &page(requests), at)
                .await
                .unwrap();
        }
        assert!(written(&store).await.is_empty(), "a quiet host writes nothing");
        assert!(!rollup.due(HOUR + 601));
        rollup
            .advance(&bucket, "ovh-1", &units, &page(8), HOUR + 900)
            .await
            .unwrap();
        assert_eq!(
            written(&store).await,
            ["metrics/ovh-1/20261016.json", "metrics/ovh-1/2026101612.json"]
        );

        let mut restarted = Rollup::default();
        for (at, requests) in [(HOUR + 1200, 100), (HOUR + 1500, 102)] {
            restarted
                .advance(&bucket, "ovh-1", &units, &page(requests), at)
                .await
                .unwrap();
        }
        let day = bucket.get("metrics/ovh-1/20261016.json").await.unwrap().unwrap();
        let day: MetricsHour = serde_json::from_slice(&day).unwrap();
        assert_eq!((day.hour.as_str(), day.units["site"].requests), ("20261016", 5));
    }
}
