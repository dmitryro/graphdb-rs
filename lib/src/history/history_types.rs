// lib/src/history/history_types.rs
use anyhow::Result;
use serde::{Serialize, Deserialize};
use bincode::{Encode, Decode};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use std::path::{Path, PathBuf};
use tokio::sync::{RwLock, Semaphore, OnceCell};
use std::os::unix::fs::PermissionsExt;
use tokio::fs;
use sled::{Db, IVec, Config};

/// Enum for the execution status of a command.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub enum HistoryStatus {
    /// Command execution was successful.
    Success,
    /// Command execution failed (error occurred).
    Failure,
    /// Command execution was cancelled or timed out.
    Cancelled,
}

impl Default for HistoryStatus {
    fn default() -> Self {
        HistoryStatus::Success
    }
}

/// Metadata record for a single executed command.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Encode, Decode)]
pub struct HistoryMetadata {
    /// Unique sequential ID for this record. Used as the Sled key.
    pub id: u64,
    /// The user who ran the command.
    pub user: String,
    /// The full command string executed.
    pub command: String,
    /// The time the command was started (Unix timestamp in nanoseconds).
    pub start_time_nanos: u64,
    /// The time the command finished (Unix timestamp in nanoseconds).
    pub end_time_nanos: u64,
    /// The resulting status of the command.
    pub status: HistoryStatus,
    /// The service type that ran the command (e.g., 'cli', 'rest', 'daemon').
    pub service_type: String,
    /// Port of the originating service (if applicable).
    pub port: Option<u16>,
    /// Any error message, if status is Failure.
    pub error_message: Option<String>,
}

impl Default for HistoryMetadata {
    fn default() -> Self {
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos() as u64;
        HistoryMetadata {
            id: 0, // Placeholder
            user: "system".to_string(),
            command: "".to_string(),
            start_time_nanos: now,
            end_time_nanos: now,
            status: HistoryStatus::default(),
            service_type: "cli".to_string(),
            port: None,
            error_message: None,
        }
    }
}

/// Parameters used to filter and paginate history results.
#[derive(Debug, Clone, PartialEq)]
pub struct HistoryFilter {
    pub user: Option<String>,
    pub since_nanos: Option<u64>,
    pub until_nanos: Option<u64>,
    pub status: Option<HistoryStatus>,
    pub service_type: Option<String>,
    pub keyword: Option<String>,
    pub limit: usize,
    pub offset: usize,
}

impl Default for HistoryFilter {
    fn default() -> Self {
        HistoryFilter {
            user: None,
            since_nanos: None,
            until_nanos: None,
            status: None,
            service_type: None,
            keyword: None,
            limit: 100,
            offset: 0,
        }
    }
}


#[derive(Clone)]
pub struct ImprovedSledPool {
    db: Arc<Db>,
    _semaphore: Arc<Semaphore>,
}

#[cfg(feature = "with-sled")]
impl ImprovedSledPool {
    pub async fn new(db_path: PathBuf, max_concurrent: usize) -> Result<Self> {
        Self::validate_environment(&db_path).await?;

        let db = tokio::task::spawn_blocking(move || {
            Config::new()
                .path(db_path)
                .cache_capacity(16 * 1024 * 1024)
                .flush_every_ms(Some(1000))
                .use_compression(true)
                .open()
        }).await??;

        Ok(ImprovedSledPool {
            db: Arc::new(db),
            _semaphore: Arc::new(Semaphore::new(max_concurrent)),
        })
    }

    pub async fn validate_environment(db_path: &PathBuf) -> Result<()> {
        if let Some(parent) = db_path.parent() {
            fs::create_dir_all(parent).await?;
            let metadata = fs::metadata(parent).await?;
            let mut perms = metadata.permissions();
            perms.set_mode(0o755);
            fs::set_permissions(parent, perms).await?;
        }

        Ok(())
    }

    pub async fn insert(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let _permit = self._semaphore.acquire().await?;
        let db = self.db.clone();
        let key = key.to_vec();
        let value = value.to_vec();

        tokio::task::spawn_blocking(move || -> Result<()> {
            db.insert(key, value)
                .map(|_| ())
                .map_err(|e| anyhow::anyhow!("Failed to insert into sled: {}", e))
        }).await?
    }

    pub async fn get(&self, key: &[u8]) -> Result<Option<IVec>> {
        let _permit = self._semaphore.acquire().await?;
        let db = self.db.clone();
        let key = key.to_vec();

        tokio::task::spawn_blocking(move || -> Result<Option<IVec>> {
            db.get(key).map_err(|e| anyhow::anyhow!("Failed to get from sled: {}", e))
        }).await?
    }

    pub async fn remove(&self, key: &[u8]) -> Result<Option<IVec>> {
        let _permit = self._semaphore.acquire().await?;
        let db = self.db.clone();
        let key = key.to_vec();

        tokio::task::spawn_blocking(move || -> Result<Option<IVec>> {
            db.remove(key).map_err(|e| anyhow::anyhow!("Failed to remove from sled: {}", e))
        }).await?
    }

    pub async fn iter_all(&self) -> Result<Vec<(IVec, IVec)>> {
        let _permit = self._semaphore.acquire().await?;
        let db = self.db.clone();

        tokio::task::spawn_blocking(move || -> Result<Vec<(IVec, IVec)>> {
            db.iter().map(|result| result.map_err(|e| anyhow::anyhow!("Failed to iterate sled: {}", e))).collect()
        }).await?
    }
}
