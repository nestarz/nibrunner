pub mod accepted_document_repository;
pub mod activity_repository;
pub mod deleted_volumes_repository;
pub mod host_identity_repository;
pub mod instances_repository;
mod last_written;
pub(crate) mod memory_profiles_repository;
pub mod meters_repository;
pub mod slots_repository;

use std::sync::Arc;

use sqlx::SqlitePool;

use crate::repositories::accepted_document_repository::{AcceptedDocumentRepository, SqliteAcceptedDocument};
use crate::repositories::activity_repository::{ActivityRepository, SqliteActivity};
use crate::repositories::deleted_volumes_repository::{DeletedVolumeRepository, SqliteDeletedVolumes};
use crate::repositories::host_identity_repository::{HostIdentityRepository, SqliteHostIdentity};
use crate::repositories::instances_repository::{InstanceRepository, SqliteInstances};
use crate::repositories::meters_repository::{MeterRepository, SqliteMeters};
use crate::repositories::slots_repository::{SlotRepository, SqliteSlots};

pub struct Repositories {
    pub instances: Arc<dyn InstanceRepository>,
    pub(crate) memory_profiles: Arc<dyn memory_profiles_repository::MemoryProfileRepository>,
    pub slots: Arc<dyn SlotRepository>,
    pub activity: Arc<dyn ActivityRepository>,
    pub meters: Arc<dyn MeterRepository>,
    pub deleted_volumes: Arc<dyn DeletedVolumeRepository>,
    pub identity: Arc<dyn HostIdentityRepository>,
    pub accepted_document: Arc<dyn AcceptedDocumentRepository>,
}

impl Repositories {
    pub fn sqlite(pool: SqlitePool) -> Self {
        Self {
            instances: Arc::new(SqliteInstances::new(pool.clone())),
            memory_profiles: Arc::new(memory_profiles_repository::SqliteMemoryProfiles::new(
                pool.clone(),
            )),
            slots: Arc::new(SqliteSlots::new(pool.clone())),
            activity: Arc::new(SqliteActivity::new(pool.clone())),
            meters: Arc::new(SqliteMeters::new(pool.clone())),
            deleted_volumes: Arc::new(SqliteDeletedVolumes::new(pool.clone())),
            identity: Arc::new(SqliteHostIdentity::new(pool.clone())),
            accepted_document: Arc::new(SqliteAcceptedDocument::new(pool)),
        }
    }

    pub async fn holds_nothing(&self) -> bool {
        let slots = self.slots.all().await.map(|held| held.len()).unwrap_or_default();
        let instances = self
            .instances
            .all()
            .await
            .map(|held| held.len())
            .unwrap_or_default();
        slots + instances == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::store::import::import_documents;
    use crate::domain::store::{in_memory, open};

    #[tokio::test]
    async fn a_host_with_no_database_gets_one_with_the_schema_already_in_it() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("state.db");
        let pool = open(&path).await.unwrap();
        assert!(path.exists());

        let tables: Vec<String> =
            sqlx::query_scalar("select name from sqlite_master where type = 'table' order by name")
                .fetch_all(&pool)
                .await
                .unwrap();
        for expected in [
            "accepted_document",
            "activity",
            "deleted_volumes",
            "host_identity",
            "instances",
            "meters",
            "slot_cursor",
            "slots",
        ] {
            assert!(
                tables.contains(&expected.to_string()),
                "{expected} is missing: {tables:?}"
            );
        }
    }

    #[tokio::test]
    async fn opening_a_database_that_already_exists_is_a_restart_and_not_an_error() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("state.db");
        let first = open(&path).await.unwrap();
        sqlx::query("insert into slots (app_id, slot) values ('app-1', 0)")
            .execute(&first)
            .await
            .unwrap();
        first.close().await;

        let second = Repositories::sqlite(open(&path).await.unwrap());
        assert_eq!(second.slots.all().await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn a_host_that_has_written_nothing_holds_nothing() {
        let repositories = Repositories::sqlite(in_memory().await);
        assert!(repositories.holds_nothing().await);
        repositories
            .instances
            .replace_all(&[crate::test_support::instance_record(|_| {})])
            .await
            .unwrap();
        assert!(!repositories.holds_nothing().await);
    }

    #[tokio::test]
    async fn a_slot_an_older_daemon_allocated_is_carried_over_rather_than_reissued() {
        let directory = tempfile::tempdir().unwrap();
        let config = crate::config::HostConfig::under(directory.path());
        crate::json_store::write_json(&config.slots_file(), &serde_json::json!({ "app-1": 3 })).unwrap();
        crate::json_store::write_json(&config.slot_cursor_file(), &serde_json::json!(4)).unwrap();

        let repositories = Repositories::sqlite(in_memory().await);
        import_documents(&repositories, &config).await.unwrap();
        assert_eq!(
            repositories
                .slots
                .all()
                .await
                .unwrap()
                .get(&crate::test_support::app_id()),
            Some(&3)
        );
        assert_eq!(repositories.slots.cursor().await.unwrap(), 4);
    }

    #[tokio::test]
    async fn a_database_that_already_holds_something_is_never_written_over_by_an_import() {
        let directory = tempfile::tempdir().unwrap();
        let config = crate::config::HostConfig::under(directory.path());
        crate::json_store::write_json(&config.slots_file(), &serde_json::json!({ "app-1": 3 })).unwrap();

        let repositories = Repositories::sqlite(in_memory().await);
        repositories
            .slots
            .replace_all(
                &std::collections::BTreeMap::from([(crate::test_support::app_id(), 7)]),
                8,
            )
            .await
            .unwrap();
        import_documents(&repositories, &config).await.unwrap();
        assert_eq!(
            repositories
                .slots
                .all()
                .await
                .unwrap()
                .get(&crate::test_support::app_id()),
            Some(&7)
        );
    }

    #[tokio::test]
    async fn a_host_with_no_documents_to_carry_over_imports_nothing() {
        let directory = tempfile::tempdir().unwrap();
        let config = crate::config::HostConfig::under(directory.path());
        let repositories = Repositories::sqlite(in_memory().await);
        import_documents(&repositories, &config).await.unwrap();
        assert!(repositories.holds_nothing().await);
    }
}
