use std::{collections::BTreeMap, sync::RwLock};

use protocol::{AppId, DesiredInstance, InstanceLimits, InstanceResources};

use crate::config::{HttpAdmission, VmBudget, VmBudgets};

#[derive(Default)]
struct Settings {
    admission: Option<HttpAdmission>,
    budgets: Option<VmBudgets>,
    instances: BTreeMap<AppId, InstanceLimits>,
    resources: BTreeMap<AppId, InstanceResources>,
}

#[derive(Default)]
pub struct RuntimePolicy {
    settings: RwLock<Settings>,
}

impl RuntimePolicy {
    pub fn new(admission: Option<HttpAdmission>, budgets: Option<VmBudgets>) -> Self {
        Self {
            settings: RwLock::new(Settings {
                admission,
                budgets,
                ..Settings::default()
            }),
        }
    }

    pub(crate) fn replace(&self, admission: Option<HttpAdmission>, budgets: Option<VmBudgets>) {
        let mut settings = self
            .settings
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        settings.admission = admission;
        settings.budgets = budgets;
    }

    /// Replace document overrides, preserving the latest live configuration fallback.
    pub(crate) fn replace_instances(&self, instances: &[DesiredInstance]) {
        let mut settings = self
            .settings
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        settings.instances = instances
            .iter()
            .filter_map(|instance| instance.limits.map(|limits| (instance.app_id.clone(), limits)))
            .collect();
        settings.resources = instances
            .iter()
            .map(|instance| (instance.app_id.clone(), instance.config.resources))
            .collect();
    }

    pub(crate) fn http_limits(&self, app: &AppId) -> Option<(usize, usize)> {
        let settings = self
            .settings
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(limits) = settings.instances.get(app) {
            return Some((
                settings.admission.as_ref().map_or(usize::MAX, |configuration| {
                    usize::from(configuration.host_concurrent.get())
                }),
                usize::from(limits.concurrent.get()),
            ));
        }
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
        if let Some(limits) = settings.instances.get(app) {
            return Some(VmBudget {
                cpu_percent: limits.cpu_percent,
                memory_mib: limits.memory_mib,
                memory: limits.memory,
            });
        }
        if let Some(budgets) = &settings.budgets {
            return Some(budgets.apps.get(app).unwrap_or(&budgets.default).clone());
        }
        let resources = settings.resources.get(app)?;
        Some(VmBudget {
            cpu_percent: u16::try_from(resources.vcpu_count.saturating_mul(100))
                .unwrap_or(u16::MAX)
                .try_into()
                .ok()?,
            memory_mib: resources.memory_mib.saturating_add(64).try_into().ok()?,
            memory: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::*;

    #[test]
    fn every_desired_instance_gets_a_host_budget_even_without_an_override() {
        let policy = RuntimePolicy::default();
        let instance = desired_instance(|_| {});
        policy.replace_instances(std::slice::from_ref(&instance));
        let budget = policy.vm_budget(&app_id()).unwrap();
        assert_eq!(
            u32::from(budget.cpu_percent.get()),
            instance.config.resources.vcpu_count * 100
        );
        assert_eq!(budget.memory_mib.get(), instance.config.resources.memory_mib + 64);
        assert_eq!(budget.memory, None);
        policy.replace_instances(&[]);
        assert!(policy.vm_budget(&app_id()).is_none());
    }

    #[test]
    fn document_overrides_survive_reload_and_removal_restores_latest_fallback() {
        let app = app_id();
        let admission = || HttpAdmission {
            host_concurrent: 32.try_into().unwrap(),
            app_concurrent: 8.try_into().unwrap(),
            apps: [(app.clone(), 4.try_into().unwrap())].into(),
        };
        let budget = VmBudget {
            cpu_percent: 100.try_into().unwrap(),
            memory_mib: 512.try_into().unwrap(),
            memory: None,
        };
        let budgets = VmBudgets {
            default: budget.clone(),
            apps: BTreeMap::new(),
        };
        let policy = RuntimePolicy::new(Some(admission()), Some(budgets.clone()));
        let mut instance = desired_instance(|instance| {
            instance.limits = Some(InstanceLimits {
                concurrent: 2.try_into().unwrap(),
                cpu_percent: 200.try_into().unwrap(),
                memory_mib: 768.try_into().unwrap(),
                memory: None,
            })
        });
        policy.replace_instances(std::slice::from_ref(&instance));
        assert_eq!(policy.http_limits(&app), Some((32, 2)));
        assert_eq!(policy.vm_budget(&app).unwrap().memory_mib.get(), 768);
        let mut reloaded = admission();
        reloaded.host_concurrent = 16.try_into().unwrap();
        policy.replace(Some(reloaded), Some(budgets));
        assert_eq!(policy.http_limits(&app), Some((16, 2)));
        assert_eq!(policy.vm_budget(&app).unwrap().cpu_percent.get(), 200);
        instance.limits = None;
        policy.replace_instances(&[instance]);
        assert_eq!(policy.http_limits(&app), Some((16, 4)));
        assert_eq!(policy.vm_budget(&app), Some(budget));
    }

    #[test]
    fn document_limits_work_without_optional_config_and_disappear_with_instance() {
        let policy = RuntimePolicy::default();
        let instance = desired_instance(|instance| {
            instance.limits = Some(InstanceLimits {
                concurrent: 1.try_into().unwrap(),
                cpu_percent: 50.try_into().unwrap(),
                memory_mib: 256.try_into().unwrap(),
                memory: None,
            })
        });
        policy.replace_instances(&[instance]);
        assert_eq!(policy.http_limits(&app_id()), Some((usize::MAX, 1)));
        assert_eq!(policy.vm_budget(&app_id()).unwrap().cpu_percent.get(), 50);
        policy.replace_instances(&[]);
        assert_eq!(policy.http_limits(&app_id()), None);
        assert_eq!(policy.vm_budget(&app_id()), None);
    }
}
