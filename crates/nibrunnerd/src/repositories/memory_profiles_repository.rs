use async_trait::async_trait;
use protocol::{AppId, DeploymentId, InstanceResources};
use sqlx::SqlitePool;

use crate::domain::memory_admission::{MemoryProfile, MemoryProfiles};
use crate::domain::store::StoreError;

#[cfg_attr(any(test, feature = "testing"), mockall::automock)]
#[async_trait]
pub trait MemoryProfileRepository: Send + Sync {
    async fn all(&self) -> Result<MemoryProfiles, StoreError>;
    /// Leaves the table holding exactly `profiles`.
    async fn replace_all(&self, profiles: &MemoryProfiles) -> Result<(), StoreError>;
}

pub struct SqliteMemoryProfiles {
    pool: SqlitePool,
}

impl SqliteMemoryProfiles {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl MemoryProfileRepository for SqliteMemoryProfiles {
    async fn all(&self) -> Result<MemoryProfiles, StoreError> {
        let rows = sqlx::query!(
            "select app_id, deployment_id, vcpu_count, memory_mib, peak_bytes, observed_at_ms from memory_profiles"
        )
        .fetch_all(&self.pool)
        .await
        .map_err(StoreError::read)?;
        Ok(rows
            .into_iter()
            .filter_map(|row| {
                let profile = MemoryProfile {
                    app_id: AppId::parse(row.app_id).ok()?,
                    deployment_id: DeploymentId::parse(row.deployment_id).ok()?,
                    resources: InstanceResources {
                        vcpu_count: u32::try_from(row.vcpu_count).ok()?,
                        memory_mib: u32::try_from(row.memory_mib).ok()?,
                    },
                    peak_bytes: u64::try_from(row.peak_bytes).ok()?,
                    observed_at_ms: row.observed_at_ms,
                };
                Some(((profile.app_id.clone(), profile.deployment_id.clone()), profile))
            })
            .collect())
    }

    async fn replace_all(&self, profiles: &MemoryProfiles) -> Result<(), StoreError> {
        let mut tx = self.pool.begin().await.map_err(StoreError::write)?;
        sqlx::query!("delete from memory_profiles")
            .execute(&mut *tx)
            .await
            .map_err(StoreError::write)?;
        for profile in profiles.values() {
            let (app_id, deployment_id) = (profile.app_id.as_str(), profile.deployment_id.as_str());
            let (vcpu_count, memory_mib) = (
                i64::from(profile.resources.vcpu_count),
                i64::from(profile.resources.memory_mib),
            );
            let peak_bytes = i64::try_from(profile.peak_bytes).unwrap_or(i64::MAX);
            sqlx::query!(
                "insert into memory_profiles (app_id, deployment_id, vcpu_count, memory_mib, peak_bytes, observed_at_ms)
                 values (?, ?, ?, ?, ?, ?)",
                app_id,
                deployment_id,
                vcpu_count,
                memory_mib,
                peak_bytes,
                profile.observed_at_ms
            )
            .execute(&mut *tx)
            .await
            .map_err(StoreError::write)?;
        }
        tx.commit().await.map_err(StoreError::write)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::store::in_memory;

    fn profile(app: &str, peak_bytes: u64) -> MemoryProfile {
        MemoryProfile {
            app_id: AppId::parse(app).unwrap(),
            deployment_id: DeploymentId::parse("dep-1").unwrap(),
            resources: protocol::DEFAULT_INSTANCE_RESOURCES,
            peak_bytes,
            observed_at_ms: 1_000,
        }
    }

    fn profiles(held: &[MemoryProfile]) -> MemoryProfiles {
        held.iter()
            .map(|profile| {
                (
                    (profile.app_id.clone(), profile.deployment_id.clone()),
                    profile.clone(),
                )
            })
            .collect()
    }

    #[tokio::test]
    async fn the_profiles_written_are_the_ones_read_back_and_nothing_older() {
        let repository = SqliteMemoryProfiles::new(in_memory().await);
        assert!(repository.all().await.unwrap().is_empty());
        repository
            .replace_all(&profiles(&[profile("app-1", 512), profile("app-2", 1024)]))
            .await
            .unwrap();
        repository
            .replace_all(&profiles(&[profile("app-2", 2048)]))
            .await
            .unwrap();
        let read = repository.all().await.unwrap();
        assert_eq!(read.len(), 1);
        assert_eq!(read.values().next().unwrap().peak_bytes, 2048);
    }
}
