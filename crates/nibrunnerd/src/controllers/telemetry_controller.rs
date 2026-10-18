use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;

use crate::controllers::Controller;
use crate::host::Host;

/// How often a live tail of an app that printed is rewritten, and a full chunk written early.
const SHIP_INTERVAL: Duration = Duration::from_secs(5);

pub struct TelemetryController {
    host: Arc<Host>,
}

impl TelemetryController {
    pub fn new(host: Arc<Host>) -> Arc<Self> {
        Arc::new(Self { host })
    }

    pub async fn tick(&self, now: i64) {
        let Some(sink) = &self.host.telemetry else { return };
        if let Err(error) = sink.ship(now).await {
            tracing::warn!(%error, "tenant output was kept for the next write to its sink");
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
