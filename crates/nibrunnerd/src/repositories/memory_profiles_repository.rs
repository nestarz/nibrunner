use std::collections::BTreeMap;

use async_trait::async_trait;
use sqlx::SqlitePool;

use crate::domain::memory_admission::{MemoryProfile, MemoryProfiles};
use crate::domain::store::StoreError;
use crate::repositories::last_written::LastWritten;

#[cfg_attr(any(test, feature = "testing"), mockall::automock)]
#[async_trait]
pub(crate) trait MemoryProfileRepository: Send + Sync {
    async fn all(&self) -> Result<MemoryProfiles, StoreError>;
    async fn replace_all(&self, profiles: &MemoryProfiles) -> Result<(), StoreError>;
}

pub(crate) struct SqliteMemoryProfiles {
    pool: SqlitePool,
    last_written: LastWritten<BTreeMap<String, String>>,
}

impl SqliteMemoryProfiles {
    pub(crate) fn new(pool: SqlitePool) -> Self {
        Self {
            pool,
            last_written: LastWritten::unknown(),
        }
    }

    async fn stored(&self) -> Result<BTreeMap<String, String>, StoreError> {
        let rows: Vec<(String, String)> = sqlx::query_as("select profile_key, profile from memory_profiles")
            .fetch_all(&self.pool)
            .await
            .map_err(StoreError::read)?;
        Ok(rows.into_iter().collect())
    }
}

#[async_trait]
impl MemoryProfileRepository for SqliteMemoryProfiles {
    async fn all(&self) -> Result<MemoryProfiles, StoreError> {
        Ok(self
            .stored()
            .await?
            .into_values()
            .filter_map(|value| {
                let profile: MemoryProfile = serde_json::from_str(&value).ok()?;
                Some(((profile.app_id.clone(), profile.deployment_id.clone()), profile))
            })
            .collect())
    }

    async fn replace_all(&self, profiles: &MemoryProfiles) -> Result<(), StoreError> {
        let wanted = profiles
            .iter()
            .map(|(key, value)| {
                Ok((
                    serde_json::to_string(key).map_err(|e| StoreError::Unwritable(e.to_string()))?,
                    serde_json::to_string(value).map_err(|e| StoreError::Unwritable(e.to_string()))?,
                ))
            })
            .collect::<Result<_, StoreError>>()?;
        let delta = self.last_written.towards(wanted, self.stored()).await?;
        if !delta.is_empty() {
            let mut tx = self.pool.begin().await.map_err(StoreError::write)?;
            for (key, value) in delta.changed() {
                sqlx::query("insert into memory_profiles (profile_key, profile) values (?, ?) on conflict (profile_key) do update set profile = excluded.profile")
                    .bind(key).bind(value).execute(&mut *tx).await.map_err(StoreError::write)?;
            }
            for key in delta.gone() {
                sqlx::query("delete from memory_profiles where profile_key = ?")
                    .bind(key)
                    .execute(&mut *tx)
                    .await
                    .map_err(StoreError::write)?;
            }
            tx.commit().await.map_err(StoreError::write)?;
        }
        delta.written();
        Ok(())
    }
}
