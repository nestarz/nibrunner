use std::sync::Arc;

use async_trait::async_trait;
use protocol::AppId;

use crate::domain::filesystem::reader;
use crate::host::Host;
use crate::ports::{GuestMeasurements, GuestReading};

pub struct VsockMeasurements {
    host: Arc<Host>,
}

impl VsockMeasurements {
    pub fn new(host: Arc<Host>) -> Arc<Self> {
        Arc::new(Self { host })
    }
}

#[async_trait]
impl GuestMeasurements for VsockMeasurements {
    async fn measure(&self, app_id: &AppId) -> GuestReading {
        let Some(_transition) = self.host.state.try_transition(app_id) else {
            return GuestReading::default();
        };
        if self
            .host
            .vms
            .statuses(std::slice::from_ref(app_id))
            .await
            .get(app_id)
            .is_some_and(|status| status.frozen)
        {
            return GuestReading::default();
        }
        reader::measure(&self.host, app_id).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::*;

    #[tokio::test]
    async fn a_guest_that_is_not_running_measures_as_nothing_rather_than_as_zero() {
        let host = test_host().await;
        let measurements = VsockMeasurements::new(host.arc().clone());
        assert_eq!(measurements.measure(&app_id()).await, GuestReading::default());
    }
}
