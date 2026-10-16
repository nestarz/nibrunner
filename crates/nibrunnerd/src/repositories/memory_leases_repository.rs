use async_trait::async_trait;
use sqlx::SqlitePool;

use crate::adapters::cgroup::MemoryGroup;
use crate::domain::store::StoreError;

/// One lease as it is kept: who holds it, for which unit, and what it was granted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StoredLease {
    pub(crate) id: String,
    pub(crate) boot_id: String,
    pub(crate) unit: String,
    pub(crate) memory_mib: u32,
    /// The bounds a flexible request asked within; absent for a fixed one.
    pub(crate) range: Option<(u32, u32)>,
    pub(crate) owner_pid: i32,
    pub(crate) owner_start_ticks: u64,
    pub(crate) owner_cgroup: String,
    pub(crate) owner_group: Option<MemoryGroup>,
    pub(crate) budget_group: Option<MemoryGroup>,
}

#[cfg_attr(any(test, feature = "testing"), mockall::automock)]
#[async_trait]
pub(crate) trait MemoryLeaseRepository: Send + Sync {
    async fn all(&self) -> Result<Vec<StoredLease>, StoreError>;
    /// Leaves the table holding exactly `leases`.
    async fn replace_all(&self, leases: &[StoredLease]) -> Result<(), StoreError>;
}

pub(crate) struct SqliteMemoryLeases {
    pool: SqlitePool,
}

impl SqliteMemoryLeases {
    pub(crate) fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }
}

// Device and inode numbers are 64 bits unsigned; SQLite integers are signed. The bits are kept.
fn group(membership: Option<String>, device: Option<i64>, inode: Option<i64>) -> Option<MemoryGroup> {
    Some(MemoryGroup {
        membership: membership?,
        device: u64::from_ne_bytes(device?.to_ne_bytes()),
        inode: u64::from_ne_bytes(inode?.to_ne_bytes()),
    })
}

fn bits(value: u64) -> i64 {
    i64::from_ne_bytes(value.to_ne_bytes())
}

#[async_trait]
impl MemoryLeaseRepository for SqliteMemoryLeases {
    async fn all(&self) -> Result<Vec<StoredLease>, StoreError> {
        let rows = sqlx::query!(
            "select lease_id, boot_id, unit, memory_mib, minimum_mib, preferred_mib, owner_pid,
                    owner_start_ticks, owner_cgroup, owner_group_membership, owner_group_device,
                    owner_group_inode, budget_group_membership, budget_group_device, budget_group_inode
             from memory_leases"
        )
        .fetch_all(&self.pool)
        .await
        .map_err(StoreError::read)?;
        rows.into_iter()
            .map(|row| {
                let unreadable =
                    || StoreError::Unreadable(format!("the memory lease {} is malformed", row.lease_id));
                let range = match (row.minimum_mib, row.preferred_mib) {
                    (Some(minimum), Some(preferred)) => Some((
                        u32::try_from(minimum).map_err(|_| unreadable())?,
                        u32::try_from(preferred).map_err(|_| unreadable())?,
                    )),
                    (None, None) => None,
                    _ => return Err(unreadable()),
                };
                Ok(StoredLease {
                    memory_mib: u32::try_from(row.memory_mib).map_err(|_| unreadable())?,
                    range,
                    owner_pid: i32::try_from(row.owner_pid).map_err(|_| unreadable())?,
                    owner_start_ticks: u64::from_ne_bytes(row.owner_start_ticks.to_ne_bytes()),
                    owner_group: group(
                        row.owner_group_membership,
                        row.owner_group_device,
                        row.owner_group_inode,
                    ),
                    budget_group: group(
                        row.budget_group_membership,
                        row.budget_group_device,
                        row.budget_group_inode,
                    ),
                    id: row.lease_id,
                    boot_id: row.boot_id,
                    unit: row.unit,
                    owner_cgroup: row.owner_cgroup,
                })
            })
            .collect()
    }

    async fn replace_all(&self, leases: &[StoredLease]) -> Result<(), StoreError> {
        let mut tx = self.pool.begin().await.map_err(StoreError::write)?;
        sqlx::query!("delete from memory_leases")
            .execute(&mut *tx)
            .await
            .map_err(StoreError::write)?;
        for lease in leases {
            let memory_mib = i64::from(lease.memory_mib);
            let minimum_mib = lease.range.map(|(minimum, _)| i64::from(minimum));
            let preferred_mib = lease.range.map(|(_, preferred)| i64::from(preferred));
            let owner_pid = i64::from(lease.owner_pid);
            let owner_start_ticks = bits(lease.owner_start_ticks);
            let owner_membership = lease.owner_group.as_ref().map(|group| group.membership.clone());
            let owner_device = lease.owner_group.as_ref().map(|group| bits(group.device));
            let owner_inode = lease.owner_group.as_ref().map(|group| bits(group.inode));
            let budget_membership = lease.budget_group.as_ref().map(|group| group.membership.clone());
            let budget_device = lease.budget_group.as_ref().map(|group| bits(group.device));
            let budget_inode = lease.budget_group.as_ref().map(|group| bits(group.inode));
            sqlx::query!(
                "insert into memory_leases (lease_id, boot_id, unit, memory_mib, minimum_mib, preferred_mib,
                     owner_pid, owner_start_ticks, owner_cgroup, owner_group_membership, owner_group_device,
                     owner_group_inode, budget_group_membership, budget_group_device, budget_group_inode)
                 values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                lease.id,
                lease.boot_id,
                lease.unit,
                memory_mib,
                minimum_mib,
                preferred_mib,
                owner_pid,
                owner_start_ticks,
                lease.owner_cgroup,
                owner_membership,
                owner_device,
                owner_inode,
                budget_membership,
                budget_device,
                budget_inode
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

    fn lease(id: &str) -> StoredLease {
        StoredLease {
            id: id.into(),
            boot_id: "boot-1".into(),
            unit: "build-1.service".into(),
            memory_mib: 512,
            range: Some((256, 1024)),
            owner_pid: 42,
            owner_start_ticks: u64::MAX,
            owner_cgroup: "/workloads.slice/build-1.service".into(),
            owner_group: Some(MemoryGroup {
                membership: "/workloads.slice/build-1.service".into(),
                device: u64::MAX - 1,
                inode: 7,
            }),
            budget_group: None,
        }
    }

    #[tokio::test]
    async fn the_leases_written_are_the_ones_read_back_whole() {
        let repository = SqliteMemoryLeases::new(in_memory().await);
        assert!(repository.all().await.unwrap().is_empty());
        repository.replace_all(&[lease("a"), lease("b")]).await.unwrap();
        let mut fixed = lease("c");
        fixed.range = None;
        repository.replace_all(&[fixed.clone()]).await.unwrap();
        assert_eq!(repository.all().await.unwrap(), vec![fixed]);
    }
}
