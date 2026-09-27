use std::sync::RwLock;

use protocol::AppId;

use crate::config::{HttpAdmission, VmBudget, VmBudgets};

#[derive(Default)]
struct Settings {
    admission: Option<HttpAdmission>,
    budgets: Option<VmBudgets>,
}

#[derive(Default)]
pub struct RuntimePolicy {
    settings: RwLock<Settings>,
}

impl RuntimePolicy {
    pub fn new(admission: Option<HttpAdmission>, budgets: Option<VmBudgets>) -> Self {
        Self {
            settings: RwLock::new(Settings { admission, budgets }),
        }
    }

    pub(crate) fn replace(&self, admission: Option<HttpAdmission>, budgets: Option<VmBudgets>) {
        *self
            .settings
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Settings { admission, budgets };
    }

    pub(crate) fn http_limits(&self, app: &AppId) -> Option<(usize, usize)> {
        let settings = self
            .settings
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let limits = settings.admission.as_ref()?;
        Some((
            usize::from(limits.host_concurrent.get()),
            usize::from(limits.apps.get(app).unwrap_or(&limits.app_concurrent).get()),
        ))
    }

    pub(crate) fn vm_budget(&self, app: &AppId) -> Option<VmBudget> {
        let settings = self
            .settings
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let budgets = settings.budgets.as_ref()?;
        Some(budgets.apps.get(app).unwrap_or(&budgets.default).clone())
    }
}
