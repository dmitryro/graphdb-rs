// lib/src/history/history_service.rs

use anyhow::Result;
use serde_json;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::{RwLock, Semaphore, OnceCell};
use std::collections::HashMap;
use log::{info, warn, error, debug};
use bincode::{encode_to_vec, decode_from_slice, config, Encode, Decode};
use std::time::Duration;
use tokio::fs;
use sled::{Db, IVec, Config};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::history::history_types::{HistoryMetadata, HistoryFilter, HistoryStatus, ImprovedSledPool,};
// --- History Service Specific Configuration ---

// Placeholder path, similar to DAEMON_REGISTRY_DB_PATH
const HISTORY_REGISTRY_DB_PATH: &str = "history_registry_db"; 
const HISTORY_FALLBACK_FILE: &str = "history_registry_fallback.json";

// --- NonBlockingHistoryRegistry ---

#[derive(Clone)]
pub struct NonBlockingHistoryRegistry {
    // Stores history metadata temporarily and for quick access before persistence
    memory_store: Arc<RwLock<HashMap<u64, HistoryMetadata>>>,
    // Persistent storage, wrapped in an RwLock and Option for non-blocking init
    storage: Arc<RwLock<Option<ImprovedSledPool>>>,
    // Tracks the next available history ID
    next_id: Arc<RwLock<u64>>,
    config: Arc<RegistryConfig>,
    background_tasks: Arc<RwLock<Vec<tokio::task::JoinHandle<()>>>>,
}

#[derive(Debug)]
struct RegistryConfig {
    db_path: PathBuf,
    fallback_file: PathBuf,
    max_concurrent_ops: usize,
}

impl NonBlockingHistoryRegistry {
    pub async fn new() -> Result<Self> {
        let config = Arc::new(RegistryConfig {
            db_path: Self::get_db_path(),
            fallback_file: Self::get_fallback_file_path(),
            max_concurrent_ops: 10,
        });

        let registry = NonBlockingHistoryRegistry {
            memory_store: Arc::new(RwLock::new(HashMap::new())),
            storage: Arc::new(RwLock::new(None)),
            next_id: Arc::new(RwLock::new(0)),
            config: config.clone(),
            background_tasks: Arc::new(RwLock::new(Vec::new())),
        };

        registry.initialize_storage_background().await;
        registry.load_initial_data().await?;
        registry.schedule_storage_sync().await;

        Ok(registry)
    }

    fn get_fallback_file_path() -> PathBuf {
        dirs::home_dir()
            .unwrap_or_else(|| PathBuf::from("/tmp"))
            .join(".graphdb")
            .join(HISTORY_FALLBACK_FILE)
    }

    fn get_db_path() -> PathBuf {
        dirs::home_dir()
            .map(|home| home.join(".graphdb").join(HISTORY_REGISTRY_DB_PATH))
            .unwrap_or_else(|| PathBuf::from("/tmp/graphdb_history_registry"))
    }

    async fn initialize_storage_background(&self) {
        let storage = self.storage.clone();
        let db_path = self.config.db_path.clone();
        let max_concurrent = self.config.max_concurrent_ops;

        let task = tokio::spawn(async move {
            // NOTE: Assuming ImprovedSledPool is available or defined in a common scope
            match ImprovedSledPool::new(db_path.clone(), max_concurrent).await {
                Ok(pool) => {
                    let mut storage_guard = storage.write().await;
                    *storage_guard = Some(pool);
                    info!("History storage backend initialized successfully at {:?}", db_path);
                }
                Err(e) => {
                    warn!("Failed to initialize history storage backend: {}. History will be memory-only until resolved.", e);
                }
            }
        });

        let mut tasks = self.background_tasks.write().await;
        tasks.push(task);
    }

    async fn load_initial_data(&self) -> Result<()> {
        let storage_guard = self.storage.read().await;
        let mut memory = self.memory_store.write().await;
        let mut max_id = 0;
        
        if let Some(pool) = &*storage_guard {
            // Load from Sled first if available
            let all_sled_entries = pool.iter_all().await?;
            for (_, encoded_value) in all_sled_entries {
                let metadata: HistoryMetadata = decode_from_slice(&encoded_value, config::standard())?.0;
                max_id = max_id.max(metadata.id);
                memory.insert(metadata.id, metadata);
            }
            info!("Loaded {} history entries from Sled", memory.len());
        } 
        // Use Self:: and pass the required PathBuf argument
        else if let Ok(data) = Self::load_from_fallback(&self.config.fallback_file).await {
            // Fallback to JSON file if Sled isn't ready
            for metadata in data {
                max_id = max_id.max(metadata.id);
                memory.insert(metadata.id, metadata);
            }
            info!("Loaded {} history entries from fallback file", memory.len());
        }
        
        *self.next_id.write().await = max_id + 1;
        
        Ok(())
    }

    async fn schedule_storage_sync(&self) {
        let storage = self.storage.clone();
        let memory_store = self.memory_store.clone();
        let fallback_file = self.config.fallback_file.clone();

        let task = tokio::spawn(async move {
            loop {
                // Batch changes every 1 second
                tokio::time::sleep(Duration::from_secs(1)).await;

                let memory = memory_store.read().await;
                let all_metadata: Vec<_> = memory.values().cloned().collect();
                drop(memory);

                let storage_guard = storage.read().await;
                if let Some(pool) = &*storage_guard {
                    // This performs a full sync, an optimized version would only sync new/changed items
                    for metadata in &all_metadata {
                        let key = metadata.id.to_be_bytes().to_vec(); // Use ID as key
                        match encode_to_vec(metadata, config::standard()) {
                            Ok(encoded) => {
                                if let Err(e) = pool.insert(&key, &encoded).await {
                                    warn!("Failed to sync history entry {} to sled: {}", metadata.id, e);
                                }
                            },
                            Err(e) => {
                                warn!("Failed to encode history metadata {}: {}", metadata.id, e);
                            }
                        }
                    }
                }
                drop(storage_guard);

                let _ = Self::save_fallback_file(&fallback_file, &all_metadata).await;

                // Sync interval
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        });

        let mut tasks = self.background_tasks.write().await;
        tasks.push(task);
    }

    // --- Public History Methods ---

    /// Registers a new history entry.
    pub async fn register_entry(&self, mut metadata: HistoryMetadata) -> Result<()> {
        let id = {
            let mut next_id = self.next_id.write().await;
            let current_id = *next_id;
            *next_id += 1;
            current_id
        };
        metadata.id = id;
        
        // 1. Update in-memory store
        let mut memory = self.memory_store.write().await;
        memory.insert(id, metadata.clone());
        drop(memory);
        
        // 2. Persist to Sled immediately (synchronously awaited task)
        let storage = self.storage.clone();
        let metadata_clone = metadata.clone();
        
        let task = tokio::spawn(async move {
            let storage_guard = storage.read().await;
            if let Some(pool) = &*storage_guard {
                let key = metadata_clone.id.to_be_bytes().to_vec();
                let encoded = encode_to_vec(&metadata_clone, config::standard())
                    .map_err(|e| anyhow::anyhow!("Failed to encode history metadata {}: {}", metadata_clone.id, e))?;
                if let Err(e) = pool.insert(&key, &encoded).await {
                    warn!("Failed to insert history entry {} into sled: {}", metadata_clone.id, e);
                }
            }
            Ok::<_, anyhow::Error>(())
        });
        task.await??;
        
        // 3. Update fallback file in background (will be picked up by sync task)
        info!("Registered history entry {}", id);
        Ok(())
    }

    /// Retrieves history entries based on the given filter.
    pub async fn get_history(&self, filter: HistoryFilter) -> Result<Vec<HistoryMetadata>> {
        let memory = self.memory_store.read().await;
        let mut results: Vec<HistoryMetadata> = memory.values().cloned().collect();
        drop(memory);

        // Apply filtering logic
        results.retain(|m| {
            // User filter
            if let Some(ref user) = filter.user {
                if m.user != *user { return false; }
            }
            // Status filter
            if let Some(ref status) = filter.status {
                if m.status != *status { return false; }
            }
            // Since filter
            if let Some(since) = filter.since_nanos {
                if m.start_time_nanos < since { return false; }
            }
            // Until filter
            if let Some(until) = filter.until_nanos {
                if m.start_time_nanos > until { return false; }
            }
            // Keyword filter
            if let Some(ref keyword) = filter.keyword {
                if !m.command.contains(keyword) { return false; }
            }
            true
        });
        
        // Sort results (default: by time descending)
        // This is a simple in-memory sort; for large history, this should be done by Sled/DB queries.
        results.sort_by_key(|m| m.start_time_nanos);
        results.reverse(); // Newest first

        // Apply offset and limit (pagination)
        let start = filter.offset.min(results.len());
        let end = (filter.offset + filter.limit).min(results.len());
        
        Ok(results[start..end].to_vec())
    }

    /// Clears history entries based on a filter or entirely.
    pub async fn clear_history(&self, filter: HistoryFilter) -> Result<usize> {
        let mut memory = self.memory_store.write().await;
        let initial_count = memory.len();
        let mut to_remove = Vec::new();

        // 1. Identify IDs to remove (reusing filtering logic)
        for metadata in memory.values() {
            let mut matches = true;
            if let Some(ref user) = filter.user {
                if metadata.user != *user { matches = false; }
            }
            // Add other filter checks here...

            if matches {
                to_remove.push(metadata.id);
            }
        }
        
        // If no filters were specified, clear all
        if filter == HistoryFilter::default() || to_remove.len() == initial_count {
            memory.clear();
            to_remove.clear();
            to_remove.extend(0..initial_count as u64); // Indicate all should be cleared from disk
        } else {
            for id in &to_remove {
                memory.remove(id);
            }
        }
        let removed_count = initial_count - memory.len();
        drop(memory);

        // 2. Remove from Sled in the background
        let storage = self.storage.clone();
        let fallback_file = self.config.fallback_file.clone();
        let memory_store = self.memory_store.clone();
        
        tokio::spawn(async move {
            let storage_guard = storage.read().await;
            if let Some(pool) = &*storage_guard {
                for id in &to_remove {
                    let key = id.to_be_bytes().to_vec();
                    let _ = pool.remove(&key).await;
                }
            }
            drop(storage_guard);
            
            // Re-save fallback file
            let memory = memory_store.read().await;
            let all_metadata: Vec<_> = memory.values().cloned().collect();
            let _ = Self::save_fallback_file(&fallback_file, &all_metadata).await;
        });

        info!("Cleared {} history entries", removed_count);
        Ok(removed_count)
    }

    // --- Utility Methods (Copied/Adapted from DaemonRegistry) ---

    async fn load_from_fallback(file_path: &PathBuf) -> Result<Vec<HistoryMetadata>> {
        if !file_path.exists() {
            return Ok(Vec::new());
        }
        let data = fs::read_to_string(file_path).await?;
        let metadata_list: Vec<HistoryMetadata> = serde_json::from_str(&data)?;
        Ok(metadata_list)
    }
    
    async fn save_fallback_file(file_path: &PathBuf, metadata_list: &[HistoryMetadata]) -> Result<()> {
        if let Some(parent) = file_path.parent() {
            fs::create_dir_all(parent).await?;
        }
        
        let data = serde_json::to_string_pretty(metadata_list)?;
        
        let temp_file = file_path.with_extension("tmp");
        fs::write(&temp_file, data).await?;
        fs::rename(&temp_file, file_path).await?;
        
        Ok(())
    }

    pub async fn close(&self) -> Result<()> {
        let mut tasks = self.background_tasks.write().await;
        for task in tasks.drain(..) {
            task.abort();
        }
        
        let memory = self.memory_store.read().await;
        let all_metadata: Vec<_> = memory.values().cloned().collect();
        drop(memory);
        
        Self::save_fallback_file(&self.config.fallback_file, &all_metadata).await?;
        
        info!("History Registry closed gracefully");
        Ok(())
    }
}

// --- Wrapper and Global Singleton ---

#[derive(Clone)]
pub struct HistoryService {
    inner: Arc<NonBlockingHistoryRegistry>,
}

impl HistoryService {
    pub async fn new() -> Result<Self> {
        Ok(HistoryService {
            inner: Arc::new(NonBlockingHistoryRegistry::new().await?),
        })
    }
    
    pub async fn register_entry(&self, metadata: HistoryMetadata) -> Result<()> {
        self.inner.register_entry(metadata).await
    }

    pub async fn get_history(&self, filter: HistoryFilter) -> Result<Vec<HistoryMetadata>> {
        self.inner.get_history(filter).await
    }

    pub async fn clear_history(&self, filter: HistoryFilter) -> Result<usize> {
        self.inner.clear_history(filter).await
    }
    
    pub async fn close(&self) -> Result<()> {
        self.inner.close().await
    }
}

pub struct HistoryServiceWrapper {
    inner: OnceCell<HistoryService>,
}

impl HistoryServiceWrapper {
    pub const fn new() -> Self {
        HistoryServiceWrapper {
            inner: OnceCell::const_new(),
        }
    }
    
    pub async fn get(&self) -> &HistoryService {
        self.inner
            .get_or_init(|| async {
                HistoryService::new().await.unwrap_or_else(|e| {
                    error!("Failed to initialize History Service: {}", e);
                    panic!("Cannot initialize History Service")
                })
            })
            .await
    }

    // Proxy methods for easy access

    pub async fn register_entry(&self, metadata: HistoryMetadata) -> Result<()> {
        self.get().await.register_entry(metadata).await
    }

    pub async fn get_history(&self, filter: HistoryFilter) -> Result<Vec<HistoryMetadata>> {
        self.get().await.get_history(filter).await
    }

    pub async fn clear_history(&self, filter: HistoryFilter) -> Result<usize> {
        self.get().await.clear_history(filter).await
    }
    
    pub async fn close(&self) -> Result<()> {
        self.get().await.close().await
    }
}

// The Global Singleton Instance
pub static GLOBAL_HISTORY_SERVICE: HistoryServiceWrapper = HistoryServiceWrapper::new();
