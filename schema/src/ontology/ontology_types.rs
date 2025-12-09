use serde::{Serialize, Deserialize};
use bincode::{Encode, Decode, config};
use anyhow::Result;
use sled::{Db, IVec};
use tokio::sync::Semaphore;
use std::sync::Arc;
use std::path::PathBuf;

// --- Placeholder for Ontology Status ---
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, Encode, Decode)]
pub enum OntologyStatus {
    Registered,
    Active,
    Deprecated,
    Deleted,
}

// --- Placeholder for Ontology Metadata (Mirroring HistoryMetadata structure) ---
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Encode, Decode)]
pub struct OntologyMetadata {
    pub id: u64,
    pub name: String,
    pub version: String,
    pub path: PathBuf,
    pub status: OntologyStatus,
    pub registration_time_nanos: u64,
    pub description: String,
}

// --- Placeholder for Ontology Filter (Mirroring HistoryFilter structure) ---
#[derive(Debug, Clone)]
pub struct OntologyFilter {
    pub name: Option<String>,
    pub status: Option<OntologyStatus>,
    pub version: Option<String>,
    pub since_nanos: Option<u64>,
    pub until_nanos: Option<u64>,
    pub limit: usize,
    pub offset: usize,
}

impl Default for OntologyFilter {
    fn default() -> Self {
        OntologyFilter {
            name: None,
            status: None,
            version: None,
            since_nanos: None,
            until_nanos: None,
            limit: 50, // Default limit
            offset: 0,
        }
    }
}


// --- Improved Sled Pool (Handling concurrent Sled I/O) ---
#[derive(Clone)]
pub struct ImprovedSledPool {
    db: Arc<Db>,
    semaphore: Arc<Semaphore>,
}

impl ImprovedSledPool {
    pub async fn new(path: PathBuf, max_concurrent_ops: usize) -> Result<Self> {
        let config = sled::Config::new()
            .path(path)
            .mode(sled::Mode::HighThroughput);
        
        let db = config.open()?;
        
        Ok(ImprovedSledPool {
            db: Arc::new(db),
            semaphore: Arc::new(Semaphore::new(max_concurrent_ops)),
        })
    }

    /// Inserts a key-value pair into Sled, respecting concurrency limits.
    pub async fn insert(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let _permit = self.semaphore.acquire().await;
        self.db.insert(key, value)?;
        Ok(())
    }

    /// Retrieves all key-value pairs from Sled.
    pub async fn iter_all(&self) -> Result<Vec<(IVec, IVec)>> {
        let _permit = self.semaphore.acquire().await;
        let mut results = Vec::new();
        for item in self.db.iter() {
            results.push(item?);
        }
        Ok(results)
    }

    /// Removes a key-value pair from Sled, respecting concurrency limits.
    pub async fn remove(&self, key: &[u8]) -> Result<()> {
        let _permit = self.semaphore.acquire().await;
        self.db.remove(key)?;
        Ok(())
    }
}
