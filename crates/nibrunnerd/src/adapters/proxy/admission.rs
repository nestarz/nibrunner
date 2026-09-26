use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

use protocol::AppId;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::config::HttpAdmission;

pub(crate) struct Admission {
    limits: HttpAdmission,
    host: Arc<Semaphore>,
    apps: Mutex<BTreeMap<AppId, Arc<Semaphore>>>,
}

pub(crate) struct Permit {
    _host: OwnedSemaphorePermit,
    _app: OwnedSemaphorePermit,
}

impl Admission {
    pub(crate) fn new(limits: HttpAdmission) -> Self {
        Self {
            host: Arc::new(Semaphore::new(usize::from(limits.host_concurrent.get()))),
            limits,
            apps: Mutex::new(BTreeMap::new()),
        }
    }

    pub(crate) fn acquire(&self, app: &AppId) -> Result<Permit, ()> {
        let host = self.host.clone().try_acquire_owned().map_err(|_| ())?;
        let mut apps = self.apps.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        let permits = apps.entry(app.clone()).or_insert_with(|| {
            Arc::new(Semaphore::new(usize::from(
                self.limits
                    .apps
                    .get(app)
                    .unwrap_or(&self.limits.app_concurrent)
                    .get(),
            )))
        });
        let app = permits.clone().try_acquire_owned().map_err(|_| ())?;
        Ok(Permit {
            _host: host,
            _app: app,
        })
    }

    pub(crate) fn keep_only(&self, wanted: &BTreeSet<AppId>) {
        self.apps
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            // Removing and re-adding an app must not grant fresh permits over its live streams.
            .retain(|app, permits| wanted.contains(app) || Arc::strong_count(permits) > 1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_overloaded_app_does_not_consume_another_apps_capacity() {
        let one = AppId::parse("app-one").unwrap();
        let two = AppId::parse("app-two").unwrap();
        let gate = Admission::new(HttpAdmission {
            host_concurrent: 3.try_into().unwrap(),
            app_concurrent: 1.try_into().unwrap(),
            apps: BTreeMap::from([(two.clone(), 2.try_into().unwrap())]),
        });
        let first = gate.acquire(&one).unwrap();
        assert!(gate.acquire(&one).is_err());
        let second = gate.acquire(&two).unwrap();
        let third = gate.acquire(&two).unwrap();
        assert!(gate.acquire(&two).is_err());
        gate.keep_only(&BTreeSet::new());
        assert!(gate.acquire(&one).is_err());
        drop(first);
        assert!(gate.acquire(&one).is_ok());
        drop((second, third));
        gate.keep_only(&BTreeSet::new());
        assert!(gate.apps.lock().unwrap().is_empty());
    }
}
