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

const HISTORY_REGISTRY_DB_PATH: &str = "history_registry_db";
const HISTORY_FALLBACK_FILE: &str = "history_registry_fallback.json";

// --- NonBlockingHistoryRegistry ---

#[derive(Clone)]
pub struct NonBlockingHistoryRegistry {
    memory_store: Arc<RwLock<HashMap<u64, HistoryMetadata>>>,
    storage: Arc<RwLock<Option<ImprovedSledPool>>>,
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
            let all_sled_entries = pool.iter_all().await?;
            for (_, encoded_value) in all_sled_entries {
                let metadata: HistoryMetadata = decode_from_slice(&encoded_value, config::standard())?.0;
                max_id = max_id.max(metadata.id);
                memory.insert(metadata.id, metadata);
            }
            info!("Loaded {} history entries from Sled", memory.len());
        } else if let Ok(data) = Self::load_from_fallback(&self.config.fallback_file).await {
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
                tokio::time::sleep(Duration::from_secs(1)).await;

                let memory = memory_store.read().await;
                let all_metadata: Vec<_> = memory.values().cloned().collect();
                drop(memory);

                let storage_guard = storage.read().await;
                if let Some(pool) = &*storage_guard {
                    for metadata in &all_metadata {
                        let key = metadata.id.to_be_bytes().to_vec();
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
        
        let mut memory = self.memory_store.write().await;
        memory.insert(id, metadata.clone());
        drop(memory);
        
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
        
        info!("Registered history entry {}", id);
        Ok(())
    }

    /// Retrieves history entries based on the given filter.
    pub async fn get_history(&self, filter: HistoryFilter) -> Result<Vec<HistoryMetadata>> {
        let memory = self.memory_store.read().await;
        let mut results: Vec<HistoryMetadata> = memory.values().cloned().collect();
        drop(memory);

        results.retain(|m| {
            if let Some(ref user) = filter.user {
                if m.user != *user { return false; }
            }
            if let Some(ref status) = filter.status {
                if m.status != *status { return false; }
            }
            if let Some(since) = filter.since_nanos {
                if m.start_time_nanos < since { return false; }
            }
            if let Some(until) = filter.until_nanos {
                if m.start_time_nanos > until { return false; }
            }
            if let Some(ref keyword) = filter.keyword {
                if !m.command.contains(keyword) { return false; }
            }
            true
        });
        
        results.sort_by_key(|m| m.start_time_nanos);
        results.reverse();

        let start = filter.offset.min(results.len());
        let end = (filter.offset + filter.limit).min(results.len());
        
        Ok(results[start..end].to_vec())
    }

    /// Clears history entries based on a filter or entirely.
    /// CRITICAL FIX: Now performs SYNCHRONOUS disk removal to ensure persistence.
    /// If filter.user is None, this clears ALL users' history.
    pub async fn clear_history(&self, filter: HistoryFilter) -> Result<usize> {
        println!("===> CLEAR_HISTORY: Starting clear operation");
        println!("===> CLEAR_HISTORY: filter.user = {:?}", filter.user);
        
        let mut memory = self.memory_store.write().await;
        let mut to_remove_ids = Vec::new();

        // 1. Identify IDs to remove
        for metadata in memory.values() {
            let mut matches = true;
            
            // CRITICAL: User filter - if None, match ALL users
            if let Some(ref user) = filter.user {
                if metadata.user != *user { 
                    matches = false; 
                }
            }
            
            if let Some(ref status) = filter.status {
                if metadata.status != *status { matches = false; }
            }
            if let Some(since) = filter.since_nanos {
                if metadata.start_time_nanos < since { matches = false; }
            }
            if let Some(until) = filter.until_nanos {
                if metadata.start_time_nanos > until { matches = false; }
            }
            if let Some(ref keyword) = filter.keyword {
                if !metadata.command.contains(keyword) { matches = false; }
            }

            if matches {
                to_remove_ids.push(metadata.id);
            }
        }
        
        println!("===> CLEAR_HISTORY: Found {} records to remove from memory", to_remove_ids.len());
        
        // 2. Remove identified IDs from memory
        let removed_count = to_remove_ids.len();
        for id in &to_remove_ids {
            memory.remove(id);
        }
        
        println!("===> CLEAR_HISTORY: Removed {} records from memory", removed_count);
        
        // Get remaining records for fallback save
        let remaining_metadata: Vec<_> = memory.values().cloned().collect();
        println!("===> CLEAR_HISTORY: {} records remaining in memory", remaining_metadata.len());
        
        drop(memory); // Release lock before disk I/O

        // 3. CRITICAL FIX: Remove from Sled SYNCHRONOUSLY (no spawn!)
        let storage = self.storage.clone();
        let ids_for_disk_removal = to_remove_ids.clone();
        
        println!("===> CLEAR_HISTORY: Starting Sled removal");
        let storage_guard = storage.read().await;
        if let Some(pool) = &*storage_guard {
            println!("===> CLEAR_HISTORY: Removing {} records from Sled", ids_for_disk_removal.len());
            let mut sled_remove_count = 0;
            for id in &ids_for_disk_removal {
                let key = id.to_be_bytes().to_vec();
                match pool.remove(&key).await {
                    Ok(_) => {
                        sled_remove_count += 1;
                    },
                    Err(e) => {
                        warn!("Failed to remove history entry {} from Sled: {}", id, e);
                    }
                }
            }
            println!("===> CLEAR_HISTORY: Successfully removed {} records from Sled", sled_remove_count);
        } else {
            println!("===> CLEAR_HISTORY: No Sled pool available, skipping Sled removal");
        }
        drop(storage_guard);
        
        // 4. CRITICAL FIX: Save fallback file SYNCHRONOUSLY (no spawn!)
        println!("===> CLEAR_HISTORY: Saving {} remaining records to fallback file", remaining_metadata.len());
        let fallback_file = self.config.fallback_file.clone();
        match Self::save_fallback_file(&fallback_file, &remaining_metadata).await {
            Ok(_) => {
                println!("===> CLEAR_HISTORY: Fallback file saved successfully at {:?}", fallback_file);
            },
            Err(e) => {
                error!("Failed to save fallback file at {:?}: {}", fallback_file, e);
                return Err(anyhow::anyhow!("Failed to persist clear operation to disk: {}", e));
            }
        }

        info!("Cleared {} history entries (logical AND physical removal complete)", removed_count);
        println!("===> CLEAR_HISTORY: Operation completed successfully");
        Ok(removed_count)
    }

    /// Clears all in-memory history, resets the next ID counter,
    /// and physically deletes the persistent Sled database and fallback file.
    /// This should only be called when explicit `--force` is used for a clean command.
    pub async fn force_cleanup_db(&self) -> Result<()> {
        info!("Initiating forced history cleanup: clearing memory, resetting ID, and deleting persistent store.");
        println!("===> FORCE_CLEANUP_DB: Starting forced cleanup");
        
        // 1. Clear in-memory state
        let mut memory = self.memory_store.write().await;
        let record_count = memory.len();
        memory.clear();
        drop(memory);
        println!("===> FORCE_CLEANUP_DB: Cleared {} records from memory", record_count);

        let mut next_id = self.next_id.write().await;
        *next_id = 0;
        drop(next_id);
        println!("===> FORCE_CLEANUP_DB: Reset next_id to 0");

        // 2. Stop and drop background tasks (sync)
        let mut tasks = self.background_tasks.write().await;
        println!("===> FORCE_CLEANUP_DB: Stopping {} background tasks", tasks.len());
        for task in tasks.drain(..) {
            task.abort();
        }
        drop(tasks);
        
        // 3. Close and drop Sled pool BEFORE deleting files
        {
            let mut storage_guard = self.storage.write().await;
            *storage_guard = None;
            println!("===> FORCE_CLEANUP_DB: Closed Sled pool");
        }
        
        // Give the OS time to release file handles
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        
        // 4. Physically delete the Sled database directory
        let db_path = self.config.db_path.clone();
        if db_path.exists() {
            println!("===> FORCE_CLEANUP_DB: Deleting Sled directory at {:?}", db_path);
            match fs::remove_dir_all(&db_path).await {
                Ok(_) => {
                    info!("Successfully deleted Sled history database directory at {:?}", db_path);
                    println!("===> FORCE_CLEANUP_DB: Sled directory deleted successfully");
                }
                Err(e) => {
                    error!("Failed to delete Sled history database directory at {:?}: {}", db_path, e);
                    println!("===> FORCE_CLEANUP_DB: WARNING - Failed to delete Sled directory: {}", e);
                }
            }
        } else {
            println!("===> FORCE_CLEANUP_DB: Sled directory doesn't exist at {:?}", db_path);
        }

        // 5. Delete the fallback file
        let fallback_file = self.config.fallback_file.clone();
        if fallback_file.exists() {
            println!("===> FORCE_CLEANUP_DB: Deleting fallback file at {:?}", fallback_file);
            match fs::remove_file(&fallback_file).await {
                Ok(_) => {
                    info!("Successfully deleted history fallback file at {:?}", fallback_file);
                    println!("===> FORCE_CLEANUP_DB: Fallback file deleted successfully");
                }
                Err(e) => {
                    error!("Failed to delete history fallback file at {:?}: {}", fallback_file, e);
                    println!("===> FORCE_CLEANUP_DB: WARNING - Failed to delete fallback file: {}", e);
                }
            }
        } else {
            println!("===> FORCE_CLEANUP_DB: Fallback file doesn't exist at {:?}", fallback_file);
        }

        println!("===> FORCE_CLEANUP_DB: Forced cleanup completed");
        Ok(())
    }

    // --- Utility Methods ---

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
    
    pub async fn force_cleanup_db(&self) -> Result<()> {
        self.inner.force_cleanup_db().await
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

    pub async fn register_entry(&self, metadata: HistoryMetadata) -> Result<()> {
        self.get().await.register_entry(metadata).await
    }

    pub async fn get_history(&self, filter: HistoryFilter) -> Result<Vec<HistoryMetadata>> {
        self.get().await.get_history(filter).await
    }

    pub async fn clear_history(&self, filter: HistoryFilter) -> Result<usize> {
        self.get().await.clear_history(filter).await
    }
    
    pub async fn force_cleanup_db(&self) -> Result<()> {
        self.get().await.force_cleanup_db().await
    }
    
    pub async fn close(&self) -> Result<()> {
        self.get().await.close().await
    }
}

pub static GLOBAL_HISTORY_SERVICE: HistoryServiceWrapper = HistoryServiceWrapper::new();