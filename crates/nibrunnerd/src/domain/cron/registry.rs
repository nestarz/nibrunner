use std::collections::BTreeMap;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use chrono_tz::Tz;
use protocol::{
    AppId, CronJobDefinitions, CronTable, Crontab, DeploymentId, DesiredCronJob, DesiredInstance,
    ReportedCronJob,
};
use tokio::sync::{watch, Mutex};

use crate::domain::store::StoreError;
use crate::repositories::cron_repository::CronRepository;

use super::crontab::{parse_crontab, InvalidCrontab};
use super::jobs::CronJobs;

#[derive(Debug, thiserror::Error)]
pub enum CronRegistryError {
    #[error("cron registrations must belong to the current deployment")]
    DeploymentMismatch,
    #[error(transparent)]
    Invalid(#[from] InvalidCrontab),
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error("a stored cron schedule is invalid or has no future occurrence")]
    Restore,
    #[error("the cron registry contains more than one table for an app")]
    Duplicate,
    #[error("this deployment's jobs are declared by its document")]
    Declared,
    #[error("{0} declares more cron jobs than [cron] max_jobs_per_app")]
    TooManyDeclared(AppId),
}

/// The jobs each instance's document declares, under the deployment it declares them for. The
/// document is taken up again on every start, so these are never stored.
type Declarations = BTreeMap<AppId, (DeploymentId, Vec<DesiredCronJob>)>;

pub struct CronRegistry {
    repository: Arc<dyn CronRepository>,
    mutation: Mutex<Declarations>,
    max_jobs_per_app: usize,
    time_zone: Tz,
    revision: watch::Sender<u64>,
}

impl CronRegistry {
    pub fn new(repository: Arc<dyn CronRepository>, max_jobs_per_app: usize, time_zone: Tz) -> Self {
        Self {
            repository,
            mutation: Mutex::new(Declarations::new()),
            max_jobs_per_app,
            time_zone,
            revision: watch::channel(0).0,
        }
    }

    pub fn watch(&self) -> watch::Receiver<u64> {
        self.revision.subscribe()
    }

    async fn persist(&self, tables: Vec<CronTable>) -> Result<(), CronRegistryError> {
        self.repository.replace_all(&tables).await?;
        self.changed();
        Ok(())
    }

    fn changed(&self) {
        self.revision
            .send_modify(|revision| *revision = revision.wrapping_add(1));
    }

    async fn read(&self) -> Result<BTreeMap<AppId, CronTable>, CronRegistryError> {
        let mut tables = BTreeMap::new();
        for table in self.repository.all().await? {
            let app_id = table.app_id.clone();
            if tables.insert(app_id, table).is_some() {
                return Err(CronRegistryError::Duplicate);
            }
        }
        Ok(tables)
    }

    pub async fn tables(&self) -> Result<Vec<CronTable>, CronRegistryError> {
        let _mutation = self.mutation.lock().await;
        Ok(self.read().await?.into_values().collect())
    }

    /// Each current deployment's jobs: the ones its document declares, else its guest's crontab.
    pub async fn jobs(&self) -> Result<Vec<CronJobs>, CronRegistryError> {
        let declarations = self.mutation.lock().await;
        Ok(self
            .read()
            .await?
            .into_values()
            .map(|table| match declarations.get(&table.app_id) {
                Some((deployment_id, jobs)) if *deployment_id == table.deployment_id => {
                    CronJobs::declared(&table.app_id, deployment_id, jobs)
                }
                _ => CronJobs::registered(&table),
            })
            .collect())
    }

    /// Every app's jobs, under the deployment they belong to.
    pub async fn reported(
        &self,
        now: DateTime<Utc>,
    ) -> Result<BTreeMap<AppId, (DeploymentId, Vec<ReportedCronJob>)>, CronRegistryError> {
        Ok(self
            .jobs()
            .await?
            .into_iter()
            .map(|jobs| {
                let reported = super::jobs::reported_jobs(&jobs, now, self.time_zone);
                (jobs.app_id, (jobs.deployment_id, reported))
            })
            .collect())
    }

    /// Takes up what every instance of the document declares, replacing what it declared before.
    pub async fn declare(&self, instances: &[DesiredInstance]) -> Result<(), CronRegistryError> {
        let mut declarations = self.mutation.lock().await;
        let mut declared = Declarations::new();
        for instance in instances {
            let Some(jobs) = &instance.cron else {
                continue;
            };
            if jobs.len() > self.max_jobs_per_app {
                return Err(CronRegistryError::TooManyDeclared(instance.app_id.clone()));
            }
            declared.insert(
                instance.app_id.clone(),
                (instance.deployment_id.clone(), jobs.clone()),
            );
        }
        if *declarations != declared {
            *declarations = declared;
            self.changed();
        }
        Ok(())
    }

    pub async fn validate_restored(&self, after: DateTime<Utc>) -> Result<(), CronRegistryError> {
        for table in self.tables().await? {
            for job in table.jobs.iter() {
                job.schedule
                    .next_after(after, self.time_zone)
                    .map_err(|_| CronRegistryError::Restore)?;
            }
        }
        Ok(())
    }

    pub async fn synchronize(&self, deployments: &[(AppId, DeploymentId)]) -> Result<(), CronRegistryError> {
        let _mutation = self.mutation.lock().await;
        let held = self.read().await?;
        let mut wanted = BTreeMap::new();
        for (app_id, deployment_id) in deployments {
            let table = held
                .get(app_id)
                .filter(|table| table.deployment_id == *deployment_id)
                .cloned()
                .unwrap_or_else(|| CronTable {
                    app_id: app_id.clone(),
                    deployment_id: deployment_id.clone(),
                    jobs: CronJobDefinitions::default(),
                    crontab: None,
                });
            if wanted.insert(app_id.clone(), table).is_some() {
                return Err(CronRegistryError::Duplicate);
            }
        }
        if wanted != held {
            self.persist(wanted.into_values().collect()).await?;
        }
        Ok(())
    }

    pub async fn begin_deployment(
        &self,
        app_id: &AppId,
        deployment_id: &DeploymentId,
    ) -> Result<(), CronRegistryError> {
        let _mutation = self.mutation.lock().await;
        let mut tables = self.read().await?;
        if tables
            .get(app_id)
            .is_some_and(|table| table.deployment_id == *deployment_id)
        {
            return Ok(());
        }
        tables.insert(
            app_id.clone(),
            CronTable {
                app_id: app_id.clone(),
                deployment_id: deployment_id.clone(),
                jobs: CronJobDefinitions::default(),
                crontab: None,
            },
        );
        self.persist(tables.into_values().collect()).await?;
        Ok(())
    }

    pub async fn replace(
        &self,
        app_id: &AppId,
        deployment_id: &DeploymentId,
        text: &str,
        after: DateTime<Utc>,
    ) -> Result<(), CronRegistryError> {
        let declarations = self.mutation.lock().await;
        if declarations
            .get(app_id)
            .is_some_and(|(declared, _)| declared == deployment_id)
        {
            return Err(CronRegistryError::Declared);
        }
        let mut tables = self.read().await?;
        if !tables
            .get(app_id)
            .is_some_and(|table| table.deployment_id == *deployment_id)
        {
            return Err(CronRegistryError::DeploymentMismatch);
        }
        let parsed = parse_crontab(text, self.max_jobs_per_app, self.time_zone, after)?;
        let replacement = CronTable {
            app_id: app_id.clone(),
            deployment_id: deployment_id.clone(),
            jobs: parsed.jobs,
            crontab: Some(parsed.crontab),
        };
        if tables.get(app_id) == Some(&replacement) {
            return Ok(());
        }
        tables.insert(app_id.clone(), replacement);
        self.persist(tables.into_values().collect()).await?;
        Ok(())
    }

    pub async fn list(
        &self,
        app_id: &AppId,
        deployment_id: &DeploymentId,
    ) -> Result<Crontab, CronRegistryError> {
        let declarations = self.mutation.lock().await;
        let tables = self.read().await?;
        let table = tables
            .get(app_id)
            .filter(|table| table.deployment_id == *deployment_id)
            .ok_or(CronRegistryError::DeploymentMismatch)?;
        if let Some((_, jobs)) = declarations
            .get(app_id)
            .filter(|(declared, _)| declared == deployment_id)
        {
            return Crontab::parse(listed(jobs)).map_err(|_| CronRegistryError::Declared);
        }
        Ok(table
            .crontab
            .clone()
            .unwrap_or_else(|| Crontab::parse("").expect("empty text fits the crontab limit")))
    }
}

/// A document's scheduled jobs as `crontab -l` prints them, each under its name.
fn listed(jobs: &[DesiredCronJob]) -> String {
    jobs.iter()
        .filter_map(|job| {
            let schedule = job.schedule.as_ref()?;
            Some(format!(
                "# {}\n{} {}\n",
                job.name,
                schedule.as_str(),
                job.command.expose()
            ))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::store::in_memory;
    use crate::repositories::cron_repository::SqliteCron;

    fn app() -> AppId {
        AppId::parse("app-1").unwrap()
    }
    fn deployment(value: &str) -> DeploymentId {
        DeploymentId::parse(value).unwrap()
    }
    fn after() -> DateTime<Utc> {
        "2026-10-01T00:00:00Z".parse().unwrap()
    }

    async fn registry() -> CronRegistry {
        let registry = CronRegistry::new(Arc::new(SqliteCron::new(in_memory().await)), 10, chrono_tz::UTC);
        registry
            .synchronize(&[(app(), deployment("dep-1"))])
            .await
            .unwrap();
        registry
    }

    #[tokio::test]
    async fn an_invalid_replacement_leaves_the_registered_table_unchanged() {
        let registry = registry().await;
        registry
            .replace(&app(), &deployment("dep-1"), "* * * * * echo original\n", after())
            .await
            .unwrap();
        assert!(registry
            .replace(&app(), &deployment("dep-1"), "invalid tenant-secret", after())
            .await
            .is_err());
        assert_eq!(
            registry
                .list(&app(), &deployment("dep-1"))
                .await
                .unwrap()
                .expose(),
            "* * * * * echo original\n"
        );
    }

    #[tokio::test]
    async fn a_new_deployment_discards_old_jobs_and_old_guest_registrations() {
        let registry = registry().await;
        registry
            .replace(
                &app(),
                &deployment("dep-1"),
                "TOKEN=old\n@daily echo old",
                after(),
            )
            .await
            .unwrap();
        registry
            .synchronize(&[(app(), deployment("dep-2"))])
            .await
            .unwrap();
        let table = registry.tables().await.unwrap().remove(0);
        assert_eq!(table.deployment_id, deployment("dep-2"));
        assert!(table.jobs.is_empty());
        assert!(table.crontab.is_none());
        assert!(matches!(
            registry
                .replace(&app(), &deployment("dep-1"), "@daily echo stale", after())
                .await,
            Err(CronRegistryError::DeploymentMismatch)
        ));
        assert!(registry.list(&app(), &deployment("dep-1")).await.is_err());
        assert!(registry
            .list(&app(), &deployment("dep-2"))
            .await
            .unwrap()
            .expose()
            .is_empty());
        registry
            .replace(&app(), &deployment("dep-2"), "@hourly echo new", after())
            .await
            .unwrap();
        let table = registry.tables().await.unwrap().remove(0);
        let jobs = table.jobs.iter().collect::<Vec<_>>();
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].command.expose(), "echo new");
        assert!(jobs[0].environment.as_ref().unwrap().is_empty());
        assert_eq!(table.crontab.unwrap().expose(), "@hourly echo new");
    }

    #[tokio::test]
    async fn synchronizing_the_same_deployment_preserves_its_table() {
        let registry = registry().await;
        registry
            .replace(&app(), &deployment("dep-1"), "@daily echo retained", after())
            .await
            .unwrap();
        registry
            .synchronize(&[(app(), deployment("dep-1"))])
            .await
            .unwrap();
        assert_eq!(
            registry
                .list(&app(), &deployment("dep-1"))
                .await
                .unwrap()
                .expose(),
            "@daily echo retained"
        );
        registry.synchronize(&[]).await.unwrap();
        assert!(registry.tables().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn removing_a_crontab_keeps_the_deployment_eligible_for_new_registration() {
        let registry = registry().await;
        registry
            .replace(&app(), &deployment("dep-1"), "@daily echo old", after())
            .await
            .unwrap();
        registry
            .replace(&app(), &deployment("dep-1"), "", after())
            .await
            .unwrap();
        assert_eq!(
            registry
                .list(&app(), &deployment("dep-1"))
                .await
                .unwrap()
                .expose(),
            ""
        );
        registry
            .replace(&app(), &deployment("dep-1"), "@daily echo new", after())
            .await
            .unwrap();
        assert_eq!(registry.tables().await.unwrap()[0].jobs.iter().count(), 1);
    }

    #[tokio::test]
    async fn concurrent_registrations_keep_both_apps_tables() {
        let registry = registry().await;
        let second = AppId::parse("app-2").unwrap();
        registry
            .synchronize(&[
                (app(), deployment("dep-1")),
                (second.clone(), deployment("dep-2")),
            ])
            .await
            .unwrap();
        let first_app = app();
        let first_deployment = deployment("dep-1");
        let second_deployment = deployment("dep-2");
        let (first, second) = tokio::join!(
            registry.replace(&first_app, &first_deployment, "@daily echo first", after()),
            registry.replace(&second, &second_deployment, "@hourly echo second", after()),
        );
        first.unwrap();
        second.unwrap();
        assert!(registry
            .tables()
            .await
            .unwrap()
            .iter()
            .all(|table| table.jobs.iter().count() == 1));
    }

    #[tokio::test]
    async fn committed_changes_wake_watchers_and_repeating_the_same_table_does_not() {
        let registry = registry().await;
        let mut changes = registry.watch();
        assert!(!changes.has_changed().unwrap());
        registry
            .replace(&app(), &deployment("dep-1"), "@daily echo retained", after())
            .await
            .unwrap();
        assert!(changes.has_changed().unwrap());
        let revision = *changes.borrow_and_update();
        registry
            .replace(&app(), &deployment("dep-1"), "@daily echo retained", after())
            .await
            .unwrap();
        registry
            .synchronize(&[(app(), deployment("dep-1"))])
            .await
            .unwrap();
        assert!(!changes.has_changed().unwrap());
        assert_eq!(*changes.borrow(), revision);
        registry
            .replace(
                &app(),
                &deployment("dep-1"),
                "# changed original text\n@daily echo retained",
                after(),
            )
            .await
            .unwrap();
        assert!(changes.has_changed().unwrap());
        assert_eq!(*changes.borrow_and_update(), revision + 1);
        assert!(registry
            .replace(&app(), &deployment("dep-1"), "@reboot secret", after())
            .await
            .is_err());
        assert!(!changes.has_changed().unwrap());
    }

    fn declaring(deployment: &str, jobs: serde_json::Value) -> DesiredInstance {
        crate::test_support::desired_instance(|instance| {
            instance.deployment_id = self::deployment(deployment);
            instance.cron = Some(serde_json::from_value(jobs).unwrap());
        })
    }

    fn retry(schedule: &str) -> serde_json::Value {
        serde_json::json!({
            "name": "retry", "schedule": schedule, "command": "echo tick; exit ${CRON_EXIT:-0}",
            "timeoutS": 300, "backoffMs": [10000]
        })
    }

    #[tokio::test]
    async fn a_documents_jobs_are_the_deployments_table_without_a_guest_crontab() {
        let registry = registry().await;
        registry
            .declare(&[declaring("dep-1", serde_json::json!([retry("*/5 * * * *")]))])
            .await
            .unwrap();
        let jobs = registry.jobs().await.unwrap().remove(0);
        assert_eq!(jobs.origin, super::super::jobs::CronOrigin::Document);
        assert_eq!(jobs.jobs.len(), 1);
        assert_eq!(jobs.jobs[0].id, "retry");
        let reported = registry.reported(after()).await.unwrap();
        assert_eq!(reported[&app()].1[0].job_id, "retry");
    }

    #[tokio::test]
    async fn a_guest_crontab_is_refused_under_a_document_and_listing_prints_its_jobs() {
        let registry = registry().await;
        registry
            .declare(&[declaring("dep-1", serde_json::json!([retry("*/5 * * * *")]))])
            .await
            .unwrap();
        for text in ["@daily echo mine", ""] {
            let refused = registry
                .replace(&app(), &deployment("dep-1"), text, after())
                .await
                .unwrap_err();
            assert_eq!(
                refused.to_string(),
                "this deployment's jobs are declared by its document"
            );
        }
        assert_eq!(
            registry
                .list(&app(), &deployment("dep-1"))
                .await
                .unwrap()
                .expose(),
            "# retry\n*/5 * * * * echo tick; exit ${CRON_EXIT:-0}\n"
        );
    }

    #[tokio::test]
    async fn a_changed_declaration_under_one_deployment_replaces_its_table_in_place() {
        let registry = registry().await;
        registry
            .replace(&app(), &deployment("dep-1"), "@daily echo mine", after())
            .await
            .unwrap();
        let mut changes = registry.watch();
        changes.borrow_and_update();
        registry
            .declare(&[declaring("dep-1", serde_json::json!([retry("*/5 * * * *")]))])
            .await
            .unwrap();
        assert!(changes.has_changed().unwrap());
        changes.borrow_and_update();
        registry
            .declare(&[declaring("dep-1", serde_json::json!([retry("*/5 * * * *")]))])
            .await
            .unwrap();
        assert!(!changes.has_changed().unwrap());
        registry
            .declare(&[declaring("dep-1", serde_json::json!([retry("@hourly")]))])
            .await
            .unwrap();
        assert!(changes.has_changed().unwrap());
        let jobs = registry.jobs().await.unwrap().remove(0);
        assert_eq!(
            jobs.jobs[0].when,
            super::super::jobs::CronWhen::Schedule(protocol::CronSchedule::parse("@hourly").unwrap())
        );
        registry
            .declare(&[crate::test_support::desired_instance(|_| {})])
            .await
            .unwrap();
        assert!(matches!(
            registry.jobs().await.unwrap()[0].origin,
            super::super::jobs::CronOrigin::Crontab(Some(_))
        ));
    }

    #[tokio::test]
    async fn a_declaration_for_a_deployment_the_registry_does_not_hold_is_not_scheduled() {
        let registry = registry().await;
        registry
            .declare(&[declaring("dep-2", serde_json::json!([retry("*/5 * * * *")]))])
            .await
            .unwrap();
        let jobs = registry.jobs().await.unwrap().remove(0);
        assert_eq!(jobs.deployment_id, deployment("dep-1"));
        assert!(jobs.jobs.is_empty());
    }

    #[tokio::test]
    async fn a_document_declaring_more_jobs_than_the_host_allows_is_refused() {
        let registry = CronRegistry::new(Arc::new(SqliteCron::new(in_memory().await)), 1, chrono_tz::UTC);
        let mut second = retry("@hourly");
        second["name"] = serde_json::json!("other");
        assert!(matches!(
            registry
                .declare(&[declaring("dep-1", serde_json::json!([retry("@daily"), second]))])
                .await,
            Err(CronRegistryError::TooManyDeclared(_))
        ));
    }

    #[tokio::test]
    async fn a_failed_commit_never_notifies_registry_watchers() {
        let mut repository = crate::repositories::cron_repository::MockCronRepository::new();
        repository.expect_all().returning(|| Ok(Vec::new()));
        repository
            .expect_replace_all()
            .returning(|_| Err(StoreError::Unwritable("disk failure".to_owned())));
        let registry = CronRegistry::new(Arc::new(repository), 10, chrono_tz::UTC);
        let changes = registry.watch();
        assert!(registry
            .synchronize(&[(app(), deployment("dep-1"))])
            .await
            .is_err());
        assert!(!changes.has_changed().unwrap());
    }
}
