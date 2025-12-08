use anyhow::Result;
use serde_json;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::{RwLock, OnceCell};
use std::collections::HashMap;
use log::{info, warn, error, debug};
use bincode::{encode_to_vec, decode_from_slice, config};
use tokio::time::{ sleep, Duration as TokioDuration };
use tokio::fs;

// Assuming these types are defined in a sibling module or passed from a specific crate
use crate::ontology::ontology_types::{OntologyMetadata, OntologyFilter, ImprovedSledPool};

// --- Ontology Registry Specific Configuration ---

const ONTOLOGY_REGISTRY_DB_PATH: &str = "ontology_registry_db";
const ONTOLOGY_FALLBACK_FILE: &str = "ontology_registry_fallback.json";

// --- NonBlockingOntologyRegistry ---

#[derive(Clone)]
pub struct NonBlockingOntologyRegistry {
    // Stores Ontologies keyed by their unique ID
    memory_store: Arc<RwLock<HashMap<u64, OntologyMetadata>>>,
    // The persistent Sled storage pool
    storage: Arc<RwLock<Option<ImprovedSledPool>>>,
    // Monotonically increasing ID counter
    next_id: Arc<RwLock<u64>>,
    config: Arc<RegistryConfig>,
    // Handles for background sync/initialization tasks
    background_tasks: Arc<RwLock<Vec<tokio::task::JoinHandle<()>>>>,
}

#[derive(Debug)]
struct RegistryConfig {
    db_path: PathBuf,
    fallback_file: PathBuf,
    max_concurrent_ops: usize,
}

impl NonBlockingOntologyRegistry {
    /// Initializes the Ontology Registry, starting the Sled backend initialization
    /// and loading data from disk/fallback file.
    pub async fn new() -> Result<Self> {
        let config = Arc::new(RegistryConfig {
            db_path: Self::get_db_path(),
            fallback_file: Self::get_fallback_file_path(),
            max_concurrent_ops: 10,
        });

        let registry = NonBlockingOntologyRegistry {
            memory_store: Arc::new(RwLock::new(HashMap::new())),
            storage: Arc::new(RwLock::new(None)),
            next_id: Arc::new(RwLock::new(0)),
            config: config.clone(),
            background_tasks: Arc::new(RwLock::new(Vec::new())),
        };

        // 1. Initialize Sled pool in the background (non-blocking)
        registry.initialize_storage_background().await;
        // 2. Load initial data from disk/fallback
        registry.load_initial_data().await?;
        // 3. Schedule periodic sync task
        registry.schedule_storage_sync().await;

        Ok(registry)
    }

    /// Constructs the path for the JSON fallback file.
    fn get_fallback_file_path() -> PathBuf {
        dirs::home_dir()
            .unwrap_or_else(|| PathBuf::from("/tmp"))
            .join(".graphdb")
            .join(ONTOLOGY_FALLBACK_FILE)
    }

    /// Constructs the path for the Sled database directory.
    fn get_db_path() -> PathBuf {
        dirs::home_dir()
            .map(|home| home.join(".graphdb").join(ONTOLOGY_REGISTRY_DB_PATH))
            .unwrap_or_else(|| PathBuf::from("/tmp/graphdb_ontology_registry"))
    }

    /// Spawns a task to initialize the Sled storage backend.
    async fn initialize_storage_background(&self) {
        let storage = self.storage.clone();
        let db_path = self.config.db_path.clone();
        let max_concurrent = self.config.max_concurrent_ops;

        let task = tokio::spawn(async move {
            match ImprovedSledPool::new(db_path.clone(), max_concurrent).await {
                Ok(pool) => {
                    let mut storage_guard = storage.write().await;
                    *storage_guard = Some(pool);
                    info!("Ontology storage backend initialized successfully at {:?}", db_path);
                }
                Err(e) => {
                    warn!("Failed to initialize ontology storage backend: {}. Ontology registry will be memory-only until resolved.", e);
                }
            }
        });

        let mut tasks = self.background_tasks.write().await;
        tasks.push(task);
    }

    /// Loads initial data from Sled or the JSON fallback file.
    async fn load_initial_data(&self) -> Result<()> {
        let storage_guard = self.storage.read().await;
        let mut memory = self.memory_store.write().await;
        let mut max_id = 0;
        
        let mut loaded_from_sled = false;

        if let Some(pool) = &*storage_guard {
            match pool.iter_all().await {
                Ok(all_sled_entries) => {
                    for (_, encoded_value) in all_sled_entries {
                        match decode_from_slice(&encoded_value, config::standard()) {
                            Ok((metadata, _)) => {
                                let metadata: OntologyMetadata = metadata;
                                max_id = max_id.max(metadata.id);
                                memory.insert(metadata.id, metadata);
                            }
                            Err(e) => warn!("Failed to decode Sled ontology entry: {}", e),
                        }
                    }
                    info!("Loaded {} ontology entries from Sled", memory.len());
                    loaded_from_sled = true;
                },
                Err(e) => warn!("Failed to iterate Sled pool during load: {}", e),
            }
        }
        
        // If Sled failed or was not yet initialized, try the fallback file
        if !loaded_from_sled {
            if let Ok(data) = Self::load_from_fallback(&self.config.fallback_file).await {
                for metadata in data {
                    max_id = max_id.max(metadata.id);
                    memory.insert(metadata.id, metadata);
                }
                info!("Loaded {} ontology entries from fallback file", memory.len());
            }
        }
        
        *self.next_id.write().await = max_id + 1;
        
        Ok(())
    }

    /// Schedules a periodic background task to synchronize memory to the disk.
    async fn schedule_storage_sync(&self) {
        let storage = self.storage.clone();
        let memory_store = self.memory_store.clone();
        let fallback_file = self.config.fallback_file.clone();

        let task = tokio::spawn(async move {
            // Wait for Sled initialization
            loop {
                let storage_guard = storage.read().await;
                if storage_guard.is_some() {
                    break;
                }
                sleep(TokioDuration::from_millis(500)).await;
            }
            
            // Start the main sync loop
            loop {
                sleep(TokioDuration::from_secs(1)).await;

                let memory = memory_store.read().await;
                let all_metadata: Vec<_> = memory.values().cloned().collect();
                drop(memory);

                let storage_guard = storage.read().await;
                if let Some(pool) = &*storage_guard {
                    for metadata in &all_metadata {
                        let key = metadata.id.to_be_bytes().to_vec();
                        match encode_to_vec(metadata, config::standard()) {
                            Ok(encoded) => {
                                // Background sync is fire-and-forget for performance
                                if let Err(e) = pool.insert(&key, &encoded).await {
                                    warn!("Failed to sync ontology entry {} to sled: {}", metadata.id, e);
                                }
                            },
                            Err(e) => {
                                warn!("Failed to encode ontology metadata {}: {}", metadata.id, e);
                            }
                        }
                    }
                }
                drop(storage_guard);

                // Save fallback file periodically as a safety measure
                let _ = Self::save_fallback_file(&fallback_file, &all_metadata).await;

                sleep(TokioDuration::from_secs(5)).await;
            }
        });

        let mut tasks = self.background_tasks.write().await;
        tasks.push(task);
    }

    // --- Public Ontology Methods ---

    /// Registers a new ontology entry.
    pub async fn register_ontology(&self, mut metadata: OntologyMetadata) -> Result<()> {
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
        
        // Asynchronously update Sled (fire-and-forget for this operation)
        let storage = self.storage.clone();
        let metadata_clone = metadata.clone();
        
        tokio::spawn(async move {
            let storage_guard = storage.read().await;
            if let Some(pool) = &*storage_guard {
                let key = metadata_clone.id.to_be_bytes().to_vec();
                match encode_to_vec(&metadata_clone, config::standard()) {
                    Ok(encoded) => {
                        if let Err(e) = pool.insert(&key, &encoded).await {
                            warn!("Failed to insert ontology entry {} into sled: {}", metadata_clone.id, e);
                        }
                    },
                    Err(e) => {
                         warn!("Failed to encode ontology metadata {}: {}", metadata_clone.id, e);
                    }
                }
            }
        });
        
        info!("Registered ontology entry {}", id);
        Ok(())
    }

    /// Retrieves ontology entries based on the given filter.
    pub async fn get_ontologies(&self, filter: OntologyFilter) -> Result<Vec<OntologyMetadata>> {
        let memory = self.memory_store.read().await;
        let mut results: Vec<OntologyMetadata> = memory.values().cloned().collect();
        drop(memory);

        results.retain(|m| {
            if let Some(ref name) = filter.name {
                // Case-insensitive containment check
                if !m.name.to_lowercase().contains(&name.to_lowercase()) { return false; }
            }
            if let Some(ref status) = filter.status {
                if m.status != *status { return false; }
            }
            if let Some(ref version) = filter.version {
                if m.version != *version { return false; }
            }
            if let Some(since) = filter.since_nanos {
                if m.registration_time_nanos < since { return false; }
            }
            if let Some(until) = filter.until_nanos {
                if m.registration_time_nanos > until { return false; }
            }
            true
        });
        
        results.sort_by_key(|m| m.registration_time_nanos);
        results.reverse(); // Newest first

        let start = filter.offset.min(results.len());
        let end = (filter.offset + filter.limit).min(results.len());
        
        Ok(results[start..end].to_vec())
    }

    /// Clears ontology entries based on a filter or entirely.
    /// Performs synchronous disk removal to ensure persistence of the clear operation.
    pub async fn clear_ontologies(&self, filter: OntologyFilter) -> Result<usize> {
        debug!("Starting ontology clear operation with filter: {:?}", filter);
        
        let mut memory = self.memory_store.write().await;
        let mut to_remove_ids = Vec::new();

        // 1. Identify IDs to remove
        for metadata in memory.values() {
            let mut matches = true;
            
            if let Some(ref name) = filter.name {
                if !metadata.name.to_lowercase().contains(&name.to_lowercase()) { matches = false; }
            }
            if let Some(ref status) = filter.status {
                if metadata.status != *status { matches = false; }
            }
            if let Some(ref version) = filter.version {
                if metadata.version != *version { matches = false; }
            }
            if let Some(since) = filter.since_nanos {
                if metadata.registration_time_nanos < since { matches = false; }
            }
            if let Some(until) = filter.until_nanos {
                if metadata.registration_time_nanos > until { matches = false; }
            }

            if matches {
                to_remove_ids.push(metadata.id);
            }
        }
        
        // 2. Remove identified IDs from memory
        let removed_count = to_remove_ids.len();
        for id in &to_remove_ids {
            memory.remove(id);
        }
        
        // Get remaining records for fallback save
        let remaining_metadata: Vec<_> = memory.values().cloned().collect();
        
        drop(memory); // Release lock before disk I/O

        // 3. Remove from Sled SYNCHRONOUSLY
        let storage = self.storage.clone();
        let ids_for_disk_removal = to_remove_ids.clone();
        
        let storage_guard = storage.read().await;
        if let Some(pool) = &*storage_guard {
            let mut sled_remove_count = 0;
            for id in &ids_for_disk_removal {
                let key = id.to_be_bytes().to_vec();
                match pool.remove(&key).await {
                    Ok(_) => sled_remove_count += 1,
                    Err(e) => warn!("Failed to remove ontology entry {} from Sled: {}", id, e),
                }
            }
            debug!("Successfully removed {} records from Sled", sled_remove_count);
        } else {
            debug!("No Sled pool available, skipping Sled removal");
        }
        drop(storage_guard);
        
        // 4. Save fallback file SYNCHRONOUSLY
        let fallback_file = self.config.fallback_file.clone();
        match Self::save_fallback_file(&fallback_file, &remaining_metadata).await {
            Ok(_) => {
                debug!("Fallback file saved successfully at {:?}", fallback_file);
            },
            Err(e) => {
                error!("Failed to save fallback file at {:?}: {}", fallback_file, e);
                // Return an error to the caller, as persistence failed
                return Err(anyhow::anyhow!("Failed to persist clear operation to disk: {}", e));
            }
        }

        info!("Cleared {} ontology entries (logical AND physical removal complete)", removed_count);
        Ok(removed_count)
    }

    /// Clears all in-memory ontology, resets the next ID counter,
    /// and physically deletes the persistent Sled database and fallback file.
    pub async fn force_cleanup_db(&self) -> Result<()> {
        info!("Initiating forced ontology cleanup.");
        
        // 1. Clear in-memory state
        let mut memory = self.memory_store.write().await;
        let record_count = memory.len();
        memory.clear();
        drop(memory);
        debug!("Cleared {} records from memory", record_count);

        let mut next_id = self.next_id.write().await;
        *next_id = 0;
        drop(next_id);

        // 2. Stop and drop background tasks (sync)
        let mut tasks = self.background_tasks.write().await;
        debug!("Stopping {} background tasks", tasks.len());
        for task in tasks.drain(..) {
            task.abort();
        }
        drop(tasks);
        
        // 3. Close and drop Sled pool BEFORE deleting files
        {
            let mut storage_guard = self.storage.write().await;
            *storage_guard = None;
            debug!("Closed Sled pool");
        }
        
        // Give the OS time to release file handles
        sleep(TokioDuration::from_millis(100)).await;
        
        // 4. Physically delete the Sled database directory
        let db_path = self.config.db_path.clone();
        if db_path.exists() {
            match fs::remove_dir_all(&db_path).await {
                Ok(_) => info!("Successfully deleted Sled ontology database directory at {:?}", db_path),
                Err(e) => error!("Failed to delete Sled ontology database directory at {:?}: {}", db_path, e),
            }
        }

        // 5. Delete the fallback file
        let fallback_file = self.config.fallback_file.clone();
        if fallback_file.exists() {
            match fs::remove_file(&fallback_file).await {
                Ok(_) => info!("Successfully deleted ontology fallback file at {:?}", fallback_file),
                Err(e) => error!("Failed to delete ontology fallback file at {:?}: {}", fallback_file, e),
            }
        }

        info!("Forced ontology cleanup completed.");
        Ok(())
    }

    // --- Utility Methods ---

    /// Loads ontology metadata from the JSON fallback file.
    async fn load_from_fallback(file_path: &PathBuf) -> Result<Vec<OntologyMetadata>> {
        if !file_path.exists() {
            return Ok(Vec::new());
        }
        let data = fs::read_to_string(file_path).await?;
        let metadata_list: Vec<OntologyMetadata> = serde_json::from_str(&data)?;
        Ok(metadata_list)
    }
    
    /// Saves ontology metadata to the JSON fallback file using atomic rename.
    async fn save_fallback_file(file_path: &PathBuf, metadata_list: &[OntologyMetadata]) -> Result<()> {
        if let Some(parent) = file_path.parent() {
            fs::create_dir_all(parent).await?;
        }
        
        let data = serde_json::to_string_pretty(metadata_list)?;
        
        let temp_file = file_path.with_extension("tmp");
        fs::write(&temp_file, data).await?;
        fs::rename(&temp_file, file_path).await?;
        
        Ok(())
    }

    /// Gracefully closes the registry by stopping background tasks and saving the final state to the fallback file.
    pub async fn close(&self) -> Result<()> {
        let mut tasks = self.background_tasks.write().await;
        for task in tasks.drain(..) {
            task.abort();
        }
        
        let memory = self.memory_store.read().await;
        let all_metadata: Vec<_> = memory.values().cloned().collect();
        drop(memory);
        
        Self::save_fallback_file(&self.config.fallback_file, &all_metadata).await?;
        
        info!("Ontology Registry closed gracefully");
        Ok(())
    }
}

// --- Wrapper and Global Singleton ---

#[derive(Clone)]
pub struct OntologyService {
    inner: Arc<NonBlockingOntologyRegistry>,
}

impl OntologyService {
    pub async fn new() -> Result<Self> {
        Ok(OntologyService {
            inner: Arc::new(NonBlockingOntologyRegistry::new().await?),
        })
    }
    
    pub async fn register_ontology(&self, metadata: OntologyMetadata) -> Result<()> {
        self.inner.register_ontology(metadata).await
    }

    pub async fn get_ontologies(&self, filter: OntologyFilter) -> Result<Vec<OntologyMetadata>> {
        self.inner.get_ontologies(filter).await
    }

    pub async fn clear_ontologies(&self, filter: OntologyFilter) -> Result<usize> {
        self.inner.clear_ontologies(filter).await
    }
    
    pub async fn force_cleanup_db(&self) -> Result<()> {
        self.inner.force_cleanup_db().await
    }
    
    pub async fn close(&self) -> Result<()> {
        self.inner.close().await
    }
}

pub struct OntologyServiceWrapper {
    inner: OnceCell<OntologyService>,
}

impl OntologyServiceWrapper {
    pub const fn new() -> Self {
        OntologyServiceWrapper {
            inner: OnceCell::const_new(),
        }
    }
    
    /// Gets the singleton instance of the Ontology Service, initializing it if necessary.
    pub async fn get(&self) -> &OntologyService {
        self.inner
            .get_or_init(|| async {
                OntologyService::new().await.unwrap_or_else(|e| {
                    error!("Failed to initialize Ontology Service: {}", e);
                    panic!("Cannot initialize Ontology Service")
                })
            })
            .await
    }

    pub async fn register_ontology(&self, metadata: OntologyMetadata) -> Result<()> {
        self.get().await.register_ontology(metadata).await
    }

    pub async fn get_ontologies(&self, filter: OntologyFilter) -> Result<Vec<OntologyMetadata>> {
        self.get().await.get_ontologies(filter).await
    }

    pub async fn clear_ontologies(&self, filter: OntologyFilter) -> Result<usize> {
        self.get().await.clear_ontologies(filter).await
    }
    
    pub async fn force_cleanup_db(&self) -> Result<()> {
        self.get().await.force_cleanup_db().await
    }
    
    pub async fn close(&self) -> Result<()> {
        self.get().await.close().await
    }
}

/// The global, lazy-initialized singleton for the Ontology Registry.
pub static GLOBAL_ONTOLOGY_REGISTRY: OntologyServiceWrapper = OntologyServiceWrapper::new();
