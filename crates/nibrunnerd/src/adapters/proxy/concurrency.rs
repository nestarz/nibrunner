use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use protocol::AppId;

use crate::config::HttpConcurrency;

/// Counts rather than semaphores, so that a limit the document changes takes the next request
/// without closing or re-counting the streams already open.
pub(crate) struct ConcurrencyLimiter {
    host_limit: Option<usize>,
    app_default: Option<usize>,
    host: Arc<AtomicUsize>,
    apps: Mutex<BTreeMap<AppId, Arc<AtomicUsize>>>,
    instances: RwLock<BTreeMap<AppId, usize>>,
}

pub(crate) struct Permit {
    host: Arc<AtomicUsize>,
    app: Arc<AtomicUsize>,
}

impl Drop for Permit {
    fn drop(&mut self) {
        self.host.fetch_sub(1, Ordering::AcqRel);
        self.app.fetch_sub(1, Ordering::AcqRel);
    }
}

fn take(count: &AtomicUsize, limit: Option<usize>) -> bool {
    count
        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |held| {
            limit.is_none_or(|limit| held < limit).then_some(held + 1)
        })
        .is_ok()
}

impl ConcurrencyLimiter {
    /// `None` is a host with no `[proxy.http.concurrency]`: unlimited, until an instance's own
    /// `limits.concurrent` bounds that instance alone.
    pub(crate) fn new(limits: Option<HttpConcurrency>) -> Self {
        Self {
            host_limit: limits
                .as_ref()
                .map(|limits| usize::from(limits.host_concurrent.get())),
            app_default: limits
                .as_ref()
                .map(|limits| usize::from(limits.app_concurrent.get())),
            host: Arc::new(AtomicUsize::new(0)),
            apps: Mutex::new(BTreeMap::new()),
            instances: RwLock::new(BTreeMap::new()),
        }
    }

    /// Each instance's `limits.concurrent`, in place of the host's per-app default.
    pub(crate) fn set_instance_limits(&self, limits: BTreeMap<AppId, std::num::NonZeroU16>) {
        *self
            .instances
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = limits
            .into_iter()
            .map(|(app, limit)| (app, usize::from(limit.get())))
            .collect();
    }

    fn app_limit(&self, app: &AppId) -> Option<usize> {
        self.instances
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(app)
            .copied()
            .or(self.app_default)
    }

    pub(crate) fn acquire(&self, app: &AppId) -> Result<Permit, ()> {
        let app_limit = self.app_limit(app);
        let counted = self
            .apps
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .entry(app.clone())
            .or_default()
            .clone();
        if !take(&self.host, self.host_limit) {
            return Err(());
        }
        if !take(&counted, app_limit) {
            self.host.fetch_sub(1, Ordering::AcqRel);
            return Err(());
        }
        Ok(Permit {
            host: self.host.clone(),
            app: counted,
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
        let gate = ConcurrencyLimiter::new(Some(HttpConcurrency {
            host_concurrent: 3.try_into().unwrap(),
            app_concurrent: 1.try_into().unwrap(),
        }));
        let first = gate.acquire(&one).unwrap();
        assert!(gate.acquire(&one).is_err());
        let second = gate.acquire(&two).unwrap();
        assert!(gate.acquire(&two).is_err());
        gate.keep_only(&BTreeSet::new());
        assert!(gate.acquire(&one).is_err());
        drop(first);
        assert!(gate.acquire(&one).is_ok());
        drop(second);
        gate.keep_only(&BTreeSet::new());
        assert!(gate.apps.lock().unwrap().is_empty());
    }
    #[test]
    fn the_host_limit_covers_distinct_apps_and_recovers_after_a_request_ends() {
        let limiter = ConcurrencyLimiter::new(Some(HttpConcurrency {
            host_concurrent: 1.try_into().unwrap(),
            app_concurrent: 2.try_into().unwrap(),
        }));
        let one = AppId::parse("app-one").unwrap();
        let two = AppId::parse("app-two").unwrap();
        let held = limiter.acquire(&one).unwrap();
        assert!(limiter.acquire(&two).is_err());
        drop(held);
        assert!(limiter.acquire(&two).is_ok());
    }

    #[test]
    fn an_instance_limit_bounds_that_instance_alone_on_a_host_without_concurrency_limits() {
        let limiter = ConcurrencyLimiter::new(None);
        let one = AppId::parse("app-one").unwrap();
        let two = AppId::parse("app-two").unwrap();
        limiter.set_instance_limits(BTreeMap::from([(one.clone(), 1.try_into().unwrap())]));
        let held = limiter.acquire(&one).unwrap();
        assert!(limiter.acquire(&one).is_err());
        let others: Vec<_> = (0..64).map(|_| limiter.acquire(&two).unwrap()).collect();
        drop(others);
        drop(held);
        assert!(limiter.acquire(&one).is_ok());
    }

    #[test]
    fn a_changed_instance_limit_takes_the_next_request_and_leaves_open_ones_counted() {
        let limiter = ConcurrencyLimiter::new(Some(HttpConcurrency {
            host_concurrent: 16.try_into().unwrap(),
            app_concurrent: 4.try_into().unwrap(),
        }));
        let app = AppId::parse("app-one").unwrap();
        let open: Vec<_> = (0..3).map(|_| limiter.acquire(&app).unwrap()).collect();
        limiter.set_instance_limits(BTreeMap::from([(app.clone(), 2.try_into().unwrap())]));
        assert!(
            limiter.acquire(&app).is_err(),
            "three open is already past the new two"
        );
        drop(open);
        let _first = limiter.acquire(&app).unwrap();
        let _second = limiter.acquire(&app).unwrap();
        assert!(limiter.acquire(&app).is_err());
        limiter.set_instance_limits(BTreeMap::new());
        assert!(
            limiter.acquire(&app).is_ok(),
            "back to the host's default of four"
        );
    }

    #[test]
    fn a_refused_app_permit_gives_back_its_host_permit() {
        let limiter = ConcurrencyLimiter::new(Some(HttpConcurrency {
            host_concurrent: 2.try_into().unwrap(),
            app_concurrent: 1.try_into().unwrap(),
        }));
        let one = AppId::parse("app-one").unwrap();
        let two = AppId::parse("app-two").unwrap();
        let _held = limiter.acquire(&one).unwrap();
        for _ in 0..8 {
            assert!(limiter.acquire(&one).is_err());
        }
        assert!(limiter.acquire(&two).is_ok());
    }
}
