use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use crate::runtime_policy::RuntimePolicy;
use protocol::AppId;

pub(crate) struct Admission {
    policy: Arc<RuntimePolicy>,
    active: Arc<Mutex<Active>>,
}

#[derive(Default)]
struct Active {
    host: usize,
    apps: BTreeMap<AppId, usize>,
}

pub(crate) struct Permit {
    active: Arc<Mutex<Active>>,
    app: AppId,
}

impl Drop for Permit {
    fn drop(&mut self) {
        let mut active = self
            .active
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        active.host -= 1;
        if let Some(count) = active.apps.get_mut(&self.app) {
            *count -= 1;
            if *count == 0 {
                active.apps.remove(&self.app);
            }
        }
    }
}

impl Admission {
    pub(crate) fn new(policy: Arc<RuntimePolicy>) -> Self {
        Self {
            policy,
            active: Arc::default(),
        }
    }

    pub(crate) fn acquire(&self, app: &AppId) -> Result<Permit, ()> {
        let mut active = self
            .active
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let app_active = active.apps.get(app).copied().unwrap_or(0);
        if self
            .policy
            .http_limits(app)
            .is_some_and(|(host, app)| active.host >= host || app_active >= app)
        {
            return Err(());
        }
        active.host += 1;
        *active.apps.entry(app.clone()).or_default() += 1;
        Ok(Permit {
            active: self.active.clone(),
            app: app.clone(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::HttpAdmission;

    #[test]
    fn reloading_limits_never_grants_fresh_capacity_over_live_requests() {
        let app = AppId::parse("app-one").unwrap();
        let policy = Arc::new(RuntimePolicy::default());
        let gate = Admission::new(policy.clone());
        let first = gate.acquire(&app).unwrap();
        let second = gate.acquire(&app).unwrap();
        let limits = |count: u16| {
            Some(HttpAdmission {
                host_concurrent: count.try_into().unwrap(),
                app_concurrent: count.try_into().unwrap(),
                apps: BTreeMap::new(),
            })
        };
        policy.replace(limits(1), None);
        assert!(gate.acquire(&app).is_err());
        drop(first);
        assert!(gate.acquire(&app).is_err());
        policy.replace(limits(2), None);
        let third = gate.acquire(&app).unwrap();
        assert!(gate.acquire(&app).is_err());
        policy.replace(None, None);
        let fourth = gate.acquire(&app).unwrap();
        policy.replace(limits(2), None);
        assert!(gate.acquire(&app).is_err());
        drop((second, third, fourth));
        assert!(gate.acquire(&app).is_ok());
        assert!(gate.active.lock().unwrap().apps.is_empty());
    }

    #[test]
    fn an_overloaded_app_does_not_consume_another_apps_capacity() {
        let one = AppId::parse("app-one").unwrap();
        let two = AppId::parse("app-two").unwrap();
        let gate = Admission::new(Arc::new(RuntimePolicy::new(
            Some(HttpAdmission {
                host_concurrent: 3.try_into().unwrap(),
                app_concurrent: 1.try_into().unwrap(),
                apps: BTreeMap::from([(two.clone(), 2.try_into().unwrap())]),
            }),
            None,
        )));
        let first = gate.acquire(&one).unwrap();
        assert!(gate.acquire(&one).is_err());
        let second = gate.acquire(&two).unwrap();
        let third = gate.acquire(&two).unwrap();
        assert!(gate.acquire(&two).is_err());
        assert!(gate.acquire(&one).is_err());
        drop(first);
        let first = gate.acquire(&one).unwrap();
        drop((second, third));
        drop(first);
        assert!(gate.active.lock().unwrap().apps.is_empty());
    }
}
