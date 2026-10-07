pub mod import;

use std::path::Path;
use std::time::Duration;

use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous};
use sqlx::SqlitePool;

const MAX_CONNECTIONS: u32 = 4;

/// How long a writer waits for the write lock before `database is locked`. sqlx's own 5 s was
/// overrun on a host sleeping a thousand apps at once: with their snapshots streaming to the same
/// disk a commit took up to 2.7 s, and the converge and status passes each write five
/// transactions, so a writer can queue behind more than one. Over five times the slowest commit
/// seen; a wait longer than this is a disk that has stopped, not one that is busy.
const BUSY_TIMEOUT: Duration = Duration::from_secs(15);

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("{path} could not be opened: {reason}")]
    Unopenable { path: String, reason: String },
    #[error("the host's own notes could not be read: {0}")]
    Unreadable(String),
    #[error("the host's own notes could not be written: {0}")]
    Unwritable(String),
}

impl StoreError {
    pub fn message(&self) -> String {
        self.to_string()
    }

    pub fn read(error: sqlx::Error) -> Self {
        Self::Unreadable(error.to_string())
    }

    pub fn write(error: sqlx::Error) -> Self {
        Self::Unwritable(error.to_string())
    }
}

pub async fn open(path: &Path) -> Result<SqlitePool, StoreError> {
    if let Some(parent) = path.parent() {
        crate::json_store::make_directory(parent, 0o700).map_err(|error| StoreError::Unopenable {
            path: path.display().to_string(),
            reason: error.to_string(),
        })?;
    }
    let unopenable = |error: sqlx::Error| StoreError::Unopenable {
        path: path.display().to_string(),
        reason: error.to_string(),
    };
    let options = SqliteConnectOptions::new()
        .filename(path)
        .create_if_missing(true)
        .journal_mode(SqliteJournalMode::Wal)
        // NORMAL fsyncs at checkpoints rather than on every commit, and in WAL mode the database
        // is consistent after a crash either way; what a power cut can lose is the last commit
        // or so. What is written here is rebuilt from the document and the running guests when
        // the daemon starts, so a lost last write costs nothing a restart does not already redo,
        // and FULL made every commit wait on a disk the snapshots were saturating.
        .synchronous(SqliteSynchronous::Normal)
        .busy_timeout(BUSY_TIMEOUT)
        .foreign_keys(true);

    let pool = SqlitePoolOptions::new()
        .max_connections(MAX_CONNECTIONS)
        .connect_with(options)
        .await
        .map_err(unopenable)?;
    sqlx::migrate!("./migrations")
        .run(&pool)
        .await
        .map_err(|error| StoreError::Unopenable {
            path: path.display().to_string(),
            reason: error.to_string(),
        })?;
    Ok(pool)
}

pub async fn in_memory() -> SqlitePool {
    use std::str::FromStr;
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT: AtomicU64 = AtomicU64::new(0);
    let name = format!("nibrunner-test-{}", NEXT.fetch_add(1, Ordering::Relaxed));
    let options = SqliteConnectOptions::from_str(&format!("file:{name}?mode=memory&cache=shared"))
        .expect("a shared in-memory url")
        .create_if_missing(true)
        .foreign_keys(true);
    let pool = SqlitePoolOptions::new()
        .min_connections(1)
        .max_connections(MAX_CONNECTIONS)
        .idle_timeout(None)
        .max_lifetime(None)
        .connect_with(options)
        .await
        .expect("an in-memory database opens");
    sqlx::migrate!("./migrations")
        .run(&pool)
        .await
        .expect("the schema applies");
    pool
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn opened() -> (tempfile::TempDir, SqlitePool) {
        let directory = tempfile::tempdir().unwrap();
        let pool = open(&directory.path().join("state.db")).await.unwrap();
        (directory, pool)
    }

    #[tokio::test]
    async fn adding_startup_history_preserves_a_running_hosts_existing_database() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("state.db");
        let pool = SqlitePoolOptions::new()
            .connect_with(
                SqliteConnectOptions::new()
                    .filename(&path)
                    .create_if_missing(true),
            )
            .await
            .unwrap();
        let migrations = tempfile::tempdir().unwrap();
        std::fs::write(
            migrations.path().join("0001_host_state.sql"),
            include_str!("../../../migrations/0001_host_state.sql"),
        )
        .unwrap();
        sqlx::migrate::Migrator::new(migrations.path())
            .await
            .unwrap()
            .run(&pool)
            .await
            .unwrap();
        sqlx::query("insert into slots (app_id, slot) values ('app-1', 7)")
            .execute(&pool)
            .await
            .unwrap();
        pool.close().await;
        let upgraded = open(&path).await.unwrap();
        let slot: i64 = sqlx::query_scalar("select slot from slots where app_id = 'app-1'")
            .fetch_one(&upgraded)
            .await
            .unwrap();
        assert_eq!(slot, 7);
        let profiles: i64 = sqlx::query_scalar("select count(*) from memory_profiles")
            .fetch_one(&upgraded)
            .await
            .unwrap();
        assert_eq!(profiles, 0);
    }

    #[tokio::test]
    async fn a_writer_waits_its_turn_and_a_commit_does_not_wait_for_the_disk() {
        let (_directory, pool) = opened().await;
        let busy_timeout_ms: i64 = sqlx::query_scalar("pragma busy_timeout")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(busy_timeout_ms, i64::try_from(BUSY_TIMEOUT.as_millis()).unwrap());

        let synchronous: i64 = sqlx::query_scalar("pragma synchronous")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(synchronous, 1, "1 is NORMAL, 2 is FULL");
    }

    #[tokio::test]
    async fn a_writer_that_finds_the_lock_held_waits_for_it_rather_than_failing() {
        let (_directory, pool) = opened().await;
        let mut held = pool.begin_with("begin immediate").await.unwrap();
        sqlx::query("insert into slots (app_id, slot) values ('app-1', 0)")
            .execute(&mut *held)
            .await
            .unwrap();
        let release = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            held.commit().await.unwrap();
        });

        let started = std::time::Instant::now();
        sqlx::query("insert into slots (app_id, slot) values ('app-2', 1)")
            .execute(&pool)
            .await
            .unwrap();
        assert!(
            started.elapsed() >= Duration::from_millis(100),
            "the second write went through while the first still held the lock"
        );
        release.await.unwrap();

        let rows: i64 = sqlx::query_scalar("select count(*) from slots")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(rows, 2);
    }
}
