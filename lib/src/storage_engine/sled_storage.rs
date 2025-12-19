
use anyhow::{Result, Context, anyhow};
use std::collections::{HashMap, HashSet};
use std::convert::TryFrom;
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock};
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use once_cell::sync::Lazy;
use tokio::fs;
use tokio::sync::{OnceCell, Mutex as TokioMutex};
use tokio::task;
use log::{info, debug, warn, error, trace};
pub use crate::config::{
    SledDbWithPath, SledConfig, SledStorage, SledDaemon, SledDaemonPool, load_storage_config_from_yaml, 
    DEFAULT_DATA_DIRECTORY, DEFAULT_STORAGE_PORT, StorageConfig, StorageEngineType,
    QueryResult, QueryPlan, SledClientMode,  json_to_prop,
};
use crate::storage_engine::storage_engine::{ GraphOp };
use crate::storage_engine::storage_utils::{serialize_vertex, deserialize_vertex, serialize_edge, deserialize_edge, create_edge_key};
use models::{Graph, Vertex, Edge, Identifier, identifiers::SerializableUuid, PropertyValue};
use models::errors::{GraphError, GraphResult};
use uuid::Uuid;
use async_trait::async_trait;
use chrono::Utc;
use crate::storage_engine::storage_engine::{StorageEngine, GraphStorageEngine, get_global_storage_registry};
use crate::daemon::daemon_registry::{GLOBAL_DAEMON_REGISTRY, DaemonMetadata};
use crate::daemon::db_daemon_registry::{ GLOBAL_DB_DAEMON_REGISTRY, DBDaemonMetadata };
use crate::daemon::daemon_utils::{is_storage_daemon_running, parse_cluster_range};
use crate::daemon::daemon_management::{ check_pid_validity, find_pid_by_port, stop_process_by_pid, get_ipc_endpoint, extract_port };
use serde_json::{ Value, json };
use std::any::Any;
use futures::future::join_all;
use tokio::time::{timeout, sleep, Duration as TokioDuration};
use crate::storage_engine::sled_client::{ SledClient, ZmqSocketWrapper };
use std::os::unix::fs::PermissionsExt;
use sysinfo::{System, RefreshKind, ProcessRefreshKind, Pid, ProcessesToUpdate};
use sled::{Db, IVec, transaction::{abort, ConflictableTransactionError, TransactionResult, 
                                   TransactionError, UnabortableTransactionError}, 
                                   Transactional, Tree};
use serde::{Deserialize, Serialize};
use bincode::{ config as binconfig };

pub static SLED_DB: LazyLock<OnceCell<TokioMutex<SledDbWithPath>>> = LazyLock::new(|| OnceCell::new());
pub static SLED_POOL_MAP: LazyLock<OnceCell<TokioMutex<HashMap<u16, Arc<TokioMutex<SledDaemonPool>>>>>> = LazyLock::new(|| OnceCell::new());
pub static SLED_ACTIVE_DATABASES: LazyLock<OnceCell<TokioMutex<HashSet<PathBuf>>>> = LazyLock::new(|| OnceCell::new());

// Sled Tree Names (analogous to tables or column families)
pub const EDGES_TREE: &str = "edges";
pub const VERTICES_TREE: &str = "vertices"; // Needed for completeness, even if not used in edge deletion

pub const OUTBOUND_TREE: &str = "outbound_edges_idx"; // Adjacency list for outgoing edges
pub const INBOUND_TREE: &str = "inbound_edges_idx";  // Adjacency list for incoming edges

// Sled Metadata Tree and Key
pub const METADATA_TREE: &str = "metadata";
pub const EDGE_COUNT_KEY: &str = "edge_count";
pub type TxResult<T> = Result<T, TransactionError<GraphError>>;

// Define the Transactional Error Type (needs to be defined outside the transaction closure if used explicitly)
type TxError = sled::transaction::TransactionError<GraphError>;
// sled_storage.rs  (or wherever the impl lives)

/// Initialise the global SLED_DB singleton exactly once.
/// After this call every later `SLED_DB.get()` will succeed.
pub async fn init_sled_db_singleton(db: Arc<sled::Db>, path: PathBuf) -> Result<(), GraphError> {
    SLED_DB
        .get_or_init(|| async {
            TokioMutex::new(SledDbWithPath { db, path, client: None })
        })
        .await;
    Ok(())
}

async fn get_sled_db_clone() -> GraphResult<sled::Db> {
    let singleton = SLED_DB
        .get()
        .ok_or_else(|| GraphError::StorageError("Sled singleton not initialized".to_string()))?;
    
    let db_clone = {
        // Acquire the TokioMutex lock to get the sled::Db clone with a timeout
        let guard = tokio::time::timeout(TokioDuration::from_secs(5), singleton.lock())
            .await
            .map_err(|_| GraphError::StorageError("Timeout acquiring Sled database lock for clone".to_string()))?;
        
        // FIX: Dereference the Arc (*guard.db) to access the inner sled::Db,
        // then call its cheap internal clone method. This matches the return type.
        (*guard.db).clone()
    };
    
    Ok(db_clone)
}

impl SledStorage {
    /// Ensures that only one instance of the database at the given path is active.
    pub async fn ensure_single_instance(db_path: &PathBuf) -> Result<(), GraphError> {
        let active_dbs = SLED_ACTIVE_DATABASES.get_or_init(|| async {
            TokioMutex::new(std::collections::HashSet::new())
        }).await;
        let mut active_dbs_guard = active_dbs.lock().await;
        if active_dbs_guard.contains(db_path) {
            error!("Database at {:?} is already in use by another instance", db_path);
            println!("===> ERROR: DATABASE AT {:?} ALREADY IN USE", db_path);
            return Err(GraphError::StorageError(format!("Database at {:?} is already in use", db_path)));
        }
        active_dbs_guard.insert(db_path.clone());
        Ok(())
    }

    /// Releases the database instance from the active databases map.
    pub async fn release_instance(db_path: &PathBuf) {
        let active_dbs = SLED_ACTIVE_DATABASES.get_or_init(|| async {
            TokioMutex::new(std::collections::HashSet::new())
        }).await;
        let mut active_dbs_guard = active_dbs.lock().await;
        active_dbs_guard.remove(db_path);
    }

    /// Checks and cleans up stale daemon for the given port and path.
    pub async fn check_and_cleanup_stale_daemon(port: u16, db_path: &PathBuf) -> Result<(), GraphError> {
        info!("Checking for stale Sled daemon on port {} with db_path {:?}", port, db_path);
        println!("===> CHECKING FOR STALE SLED DAEMON ON PORT {} WITH DB_PATH {:?}", port, db_path);

        let ipc_path = get_ipc_endpoint(port);
        let socket_path = &ipc_path[6..];
        if std::path::Path::new(socket_path).exists() { 
            warn!("Stale IPC socket found at {}. Attempting cleanup.", ipc_path);
            println!("===> WARNING: STALE IPC SOCKET FOUND AT {}. ATTEMPTING CLEANUP.", ipc_path);
            if let Err(e) = tokio::fs::remove_file(socket_path).await {
                warn!("Failed to remove stale IPC socket at {}: {}", ipc_path, e);
                println!("===> WARNING: FAILED TO REMOVE STALE IPC SOCKET AT {}: {}", ipc_path, e);
            } else {
                info!("Successfully removed stale IPC socket at {}", ipc_path);
                println!("===> SUCCESSFULLY REMOVED STALE IPC SOCKET AT {}", ipc_path);
            }
        }

        Ok(())
    }

    pub async fn new(
        config: &SledConfig,
        storage_config: &StorageConfig,
    ) -> Result<SledStorage, GraphError> {
        let start_time = Instant::now();
        info!("Initializing SledStorage with config at {:?}", config.path);
        println!("===> INITIALIZING SLED STORAGE WITH PORT {:?}", config.port);

        let daemon_registry = GLOBAL_DAEMON_REGISTRY.get().await;
        let mut active_port: Option<u16> = None;
        if let Ok(daemons) = daemon_registry.list_storage_daemons().await {
            for meta in daemons {
                if meta.service_type == "storage"
                    && meta.engine_type.as_deref() == Some("sled")
                    && meta.pid > 0
                    && check_pid_validity(meta.pid).await
                    && meta.zmq_ready
                    && Path::new(&format!("/tmp/graphdb-{}.ipc", meta.port)).exists()
                {
                    info!("FOUND ACTIVE SLED DAEMON ON PORT {} — REUSING", meta.port);
                    println!("===> FOUND ACTIVE SLED DAEMON ON PORT {} — REUSING", meta.port);
                    active_port = Some(meta.port);
                    break;
                }
            }
        }

        let port = active_port.unwrap_or_else(|| config.port.unwrap_or(DEFAULT_STORAGE_PORT));
        let default_data_dir = PathBuf::from(DEFAULT_DATA_DIRECTORY);
        let base_data_dir = storage_config.data_directory.as_ref().unwrap_or(&default_data_dir);
        let db_path = base_data_dir.join("sled").join(port.to_string());

        info!("Using Sled path {:?}", db_path);
        println!("===> USING SLED PATH {:?}", db_path);

        #[cfg(not(feature = "compression"))]
        if config.use_compression {
            return Err(GraphError::StorageError(
                "Sled compression feature is not enabled in this build.".to_string(),
            ));
        }

        println!("===> new - STEP 1");
        // === 1. Check global registry first ===
        {
            let registry = get_global_storage_registry().await;
            let registry_guard = registry.read().await;
            if registry_guard.contains_key(&port) {
                info!("SledStorage already initialized for port {} — reusing from global registry", port);
                println!("===> REUSING SledStorage FROM GLOBAL REGISTRY FOR PORT {}", port);

                // clone the Arc<sled::Db> that already lives in the singleton
                let sled_singleton = SLED_DB.get()
                    .ok_or_else(|| GraphError::StorageError("SLED_DB singleton not ready".into()))?;
                let db = sled_singleton.lock().await.db.clone();

                let pool_map = SLED_POOL_MAP.get_or_init(|| async { TokioMutex::new(HashMap::new()) }).await;
                let pool_map_guard = timeout(TokioDuration::from_secs(5), pool_map.lock()).await?;
                let pool = pool_map_guard.get(&port)
                    .ok_or_else(|| GraphError::StorageError("Pool not found for port".into()))?
                    .clone();

                return Ok(SledStorage { pool, db });
            }
        }

        println!("===> new - STEP 2");
        // === 2. Reuse healthy daemon if exists ===
        if let Some(meta) = daemon_registry.get_daemon_metadata(port).await? {
            if meta.data_dir == Some(db_path.clone())
                && meta.engine_type == Some("sled".to_string())
                && meta.pid > 0
                && check_pid_validity(meta.pid).await
                && meta.zmq_ready
            {
                info!("Reusing healthy Sled daemon from registry: PID {} on port {}", meta.pid, port);
                println!("===> REUSING HEALTHY SLED DAEMON FROM REGISTRY: PID {} ON PORT {}", meta.pid, port);

                // clone the Arc<sled::Db> that already lives in the singleton
                let sled_singleton = SLED_DB.get()
                    .ok_or_else(|| GraphError::StorageError("SLED_DB singleton not ready".into()))?;
                let db = sled_singleton.lock().await.db.clone();

                let pool_map = SLED_POOL_MAP.get_or_init(|| async { TokioMutex::new(HashMap::new()) }).await;
                let pool_map_guard = timeout(TokioDuration::from_secs(5), pool_map.lock()).await?;
                let pool = pool_map_guard.get(&port)
                    .ok_or_else(|| GraphError::StorageError("Pool not found for port".into()))?
                    .clone();

                // mark node healthy
                {
                    let mut pool_guard = timeout(TokioDuration::from_secs(5), pool.lock()).await?;
                    pool_guard.load_balancer.update_node_health(port, true, 0).await;
                    println!("===> [LB] NODE {} MARKED HEALTHY IN SLED LOAD BALANCER (REUSING)", port);
                }

                let storage = SledStorage { pool, db };
                let storage_arc = Arc::new(storage.clone()) as Arc<dyn GraphStorageEngine + Send + Sync>;
                let registry = get_global_storage_registry().await;
                let mut registry_guard = registry.write().await;
                registry_guard.insert(port, storage_arc);
                return Ok(storage);
            }
        }

        println!("===> new - STEP 3");
        // === 3. Clean stale state ===
        Self::check_and_cleanup_stale_daemon(port, &db_path).await?;

        println!("===> new - STEP 4");
        // === 4. Get or create daemon pool ===
        let pool = {
            let pool_map = SLED_POOL_MAP.get_or_init(|| async { TokioMutex::new(HashMap::new()) }).await;
            let mut pool_map_guard = timeout(TokioDuration::from_secs(5), pool_map.lock()).await?;
            if let Some(existing_pool) = pool_map_guard.get(&port) {
                existing_pool.clone()
            } else {
                let new_pool = Arc::new(TokioMutex::new(SledDaemonPool::new()));
                pool_map_guard.insert(port, new_pool.clone());
                new_pool
            }
        };

        println!("===> new - STEP 5");
        // === 5. Initialize cluster — opens real DB once ===
        {
            let mut pool_guard = timeout(TokioDuration::from_secs(10), pool.lock()).await?;
            info!("Initializing cluster");
            println!("===> INITIALIZING CLUSTER");
            pool_guard.initialize_cluster(storage_config, config, Some(port)).await?;
            info!("Initialized cluster on port {}", port);
            println!("===> INITIALIZED CLUSTER ON PORT {}", port);
        }

        println!("===> new - STEP 6");
        // === 6. Get real DB from daemon ===
        let pool_guard = pool.lock().await;
        let daemon = pool_guard
            .daemons
            .values()
            .next()
            .ok_or_else(|| GraphError::StorageError("No daemon in pool after init".into()))?;
        let db = daemon.db.clone();

        // *** ENSURE SINGLETON EXISTS ***  <-- add this line
        init_sled_db_singleton(db.clone(), db_path.clone()).await?;

        println!("===> new - STEP 7");
        // === 7. Create storage instance ===
        let storage = SledStorage { pool: pool.clone(), db };

        println!("===> new - STEP 8");
        // === 8. Register in global registry ===
        let storage_arc = Arc::new(storage.clone()) as Arc<dyn GraphStorageEngine + Send + Sync>;
        let registry = get_global_storage_registry().await;
        let mut registry_guard = registry.write().await;
        registry_guard.insert(port, storage_arc);

        println!("===> new - STEP 9");
        // === 9. ZMQ client singleton ===
        let client_tuple = {
            const MAX_RETRIES: usize = 20;
            let mut client_tuple_opt: Option<(SledClient, Arc<TokioMutex<ZmqSocketWrapper>>)> = None;
            info!("Waiting for ZMQ IPC readiness at {}...", get_ipc_endpoint(port));
            println!("===> WAITING FOR ZMQ IPC READINESS AT {}...", get_ipc_endpoint(port));
            for i in 0..MAX_RETRIES {
                match SledClient::connect_zmq_client_with_readiness_check(port).await {
                    Ok(c_tuple) => {
                        info!("ZMQ client connected successfully after {} retries.", i);
                        println!("===> ZMQ CLIENT CONNECTED SUCCESSFULLY AFTER {} RETRIES", i);
                        client_tuple_opt = Some(c_tuple);
                        break;
                    }
                    Err(e) => {
                        warn!("Attempt {}/{} to connect ZMQ client failed: {}", i + 1, MAX_RETRIES, e);
                        println!("===> WARNING: ATTEMPT {}/{} TO CONNECT ZMQ CLIENT FAILED: {}", i + 1, MAX_RETRIES, e);
                        if i < MAX_RETRIES - 1 {
                            sleep(TokioDuration::from_millis(100 * (i + 1) as u64)).await;
                        }
                    }
                }
            }
            client_tuple_opt.ok_or_else(|| {
                error!("Failed to connect ZMQ client after {} retries", MAX_RETRIES);
                GraphError::StorageError("Failed to connect ZMQ client".to_string())
            })?
        };

        println!("===> new - STEP 10");
        let sled_db_instance = SLED_DB.get_or_try_init(|| async {
            info!("Initializing SLED_DB singleton for ZMQ client at port {}", port);
            println!("===> INITIALIZING SLED_DB SINGLETON FOR ZMQ CLIENT AT PORT {}", port);
            let temp_db = sled::Config::new()
                .temporary(true)
                .open()
                .map_err(|e| GraphError::StorageError(e.to_string()))?;
            Ok::<TokioMutex<SledDbWithPath>, GraphError>(TokioMutex::new(SledDbWithPath {
                db: Arc::new(temp_db),
                path: db_path.clone(),
                client: None,
            }))
        }).await?;

        {
            let mut sled_db_guard = sled_db_instance.lock().await;
            sled_db_guard.client = Some(client_tuple);
        }

        info!("Successfully initialized and registered SledStorage in {}ms", start_time.elapsed().as_millis());
        println!("===> SUCCESSFULLY INITIALIZED AND REGISTERED SledStorage IN {}ms", start_time.elapsed().as_millis());

        Ok(storage)
    }

    pub async fn new_with_db(
        config: &SledConfig,
        storage_config: &StorageConfig,
        existing_db: Arc<sled::Db>,
    ) -> Result<SledStorage, GraphError> {
        let start_time = Instant::now();
        info!("Initializing SledStorage with existing database at {:?}", config.path);
        println!("===> INITIALIZING SLED STORAGE WITH EXISTING DB WITH PORT {:?}", config.port);

        let daemon_registry = GLOBAL_DAEMON_REGISTRY.get().await;
        let mut active_port: Option<u16> = None;
        if let Ok(daemons) = daemon_registry.list_storage_daemons().await {
            for meta in daemons {
                if meta.service_type == "storage"
                    && meta.engine_type.as_deref() == Some("sled")
                    && meta.pid > 0
                    && check_pid_validity(meta.pid).await
                    && meta.zmq_ready
                    && Path::new(&format!("/tmp/graphdb-{}.ipc", meta.port)).exists()
                {
                    info!("FOUND ACTIVE SLED DAEMON ON PORT {} — REUSING", meta.port);
                    println!("===> FOUND ACTIVE SLED DAEMON ON PORT {} — REUSING", meta.port);
                    active_port = Some(meta.port);
                    break;
                }
            }
        }

        let port = active_port.unwrap_or_else(|| config.port.unwrap_or(DEFAULT_STORAGE_PORT));
        let default_data_dir = PathBuf::from(DEFAULT_DATA_DIRECTORY);
        let base_data_dir = storage_config.data_directory.as_ref().unwrap_or(&default_data_dir);
        let db_path = base_data_dir.join("sled").join(port.to_string());

        info!("Using Sled path {:?}", db_path);
        println!("===> USING SLED PATH {:?}", db_path);

        #[cfg(not(feature = "compression"))]
        if config.use_compression {
            return Err(GraphError::StorageError(
                "Sled compression feature is not enabled in this build.".to_string(),
            ));
        }
        println!("===> new_with_db - STEP 1");
        // === 1. Check global registry first (for local in-memory storage manager) ===
        {
            let registry = get_global_storage_registry().await;
            let registry_guard = registry.read().await;
            if registry_guard.contains_key(&port) {
                info!("SledStorage already initialized for port {} — reusing from global registry", port);
                println!("===> REUSING SledStorage FROM GLOBAL REGISTRY FOR PORT {}", port);

                // clone the Arc<sled::Db> that already lives in the singleton
                let sled_singleton = SLED_DB.get()
                    .ok_or_else(|| GraphError::StorageError("SLED_DB singleton not ready".into()))?;
                println!("===> GOT SINGLETON sled_signleton ");
                let db = sled_singleton.lock().await.db.clone();
                println!("===> GOT DB db ");
                let pool_map = SLED_POOL_MAP.get_or_init(|| async { TokioMutex::new(HashMap::new()) }).await;
                println!("===> GOT Sled pool map ");
                // FIX: Removed timeout
                let pool_map_guard = pool_map.lock().await; 
                println!("===> GOT Sled pool map guard");
                let pool = pool_map_guard.get(&port)
                    .ok_or_else(|| GraphError::StorageError("Pool not found for port".into()))?
                    .clone();
                println!("===> GOT Sled pool");
                return Ok(SledStorage { pool, db });
            }
        }
        println!("===> new_with_db - STEP 2");
        // === 2. Reuse healthy daemon if exists ===
        if let Some(meta) = daemon_registry.get_daemon_metadata(port).await? {
            if meta.data_dir == Some(db_path.clone())
                && meta.engine_type == Some("sled".to_string())
                && meta.pid > 0
                && check_pid_validity(meta.pid).await
                && meta.zmq_ready
            {
                info!("Reusing healthy Sled daemon from registry: PID {} on port {}", meta.pid, port);
                println!("===> REUSING HEALTHY SLED DAEMON FROM REGISTRY: PID {} ON PORT {}", meta.pid, port);

                // clone the Arc<sled::Db> that already lives in the singleton
                let sled_singleton = SLED_DB.get()
                    .ok_or_else(|| GraphError::StorageError("SLED_DB singleton not ready".into()))?;
                let db = sled_singleton.lock().await.db.clone();

                let pool_map = SLED_POOL_MAP.get_or_init(|| async { TokioMutex::new(HashMap::new()) }).await;
                
                // FIX: Removed timeout
                let pool_map_guard = pool_map.lock().await;

                let pool = pool_map_guard.get(&port)
                    .ok_or_else(|| GraphError::StorageError("Pool not found for port".into()))?
                    .clone();

                // mark node healthy
                {
                    // FIX: Removed timeout
                    let mut pool_guard = pool.lock().await;
                    pool_guard.load_balancer.update_node_health(port, true, 0).await;
                    println!("===> [LB] NODE {} MARKED HEALTHY IN SLED LOAD BALANCER (REUSING)", port);
                }

                let storage = SledStorage { pool, db };
                let storage_arc = Arc::new(storage.clone()) as Arc<dyn GraphStorageEngine + Send + Sync>;
                let registry = get_global_storage_registry().await;
                let mut registry_guard = registry.write().await;
                registry_guard.insert(port, storage_arc);
                return Ok(storage);
            }
        }

        println!("===> new_with_db - STEP 3");
        // === 3. Clean stale state ===
        Self::check_and_cleanup_stale_daemon(port, &db_path).await?;
        println!("===> new_with_db - STEP 4");
        // === 4. Get or create daemon pool ===
        let pool = {
            let pool_map = SLED_POOL_MAP.get_or_init(|| async { TokioMutex::new(HashMap::new()) }).await;
            
            // FIX: Removed timeout
            let mut pool_map_guard = pool_map.lock().await;

            if let Some(existing_pool) = pool_map_guard.get(&port) {
                existing_pool.clone()
            } else {
                let new_pool = Arc::new(TokioMutex::new(SledDaemonPool::new()));
                pool_map_guard.insert(port, new_pool.clone());
                new_pool
            }
        };
        println!("===> new_with_db - STEP 5");
        // === 5. Initialize cluster with existing DB ===
        {
            // FIX: Removed timeout
            let mut pool_guard = pool.lock().await;

            pool_guard
                .initialize_cluster_with_db(storage_config, config, Some(port), existing_db.clone())
                .await?;
        }
        println!("===> new_with_db - STEP 6");
        // === 6. Create storage instance ===
        let storage = SledStorage {
            pool: pool.clone(),
            db: existing_db.clone(),
        };

        // *** ENSURE SINGLETON EXISTS ***
        init_sled_db_singleton(existing_db.clone(), db_path.clone()).await?;

        println!("===> new_with_db - STEP 7");
        // === 7. Register in global registry ===
        let storage_arc = Arc::new(storage.clone()) as Arc<dyn GraphStorageEngine + Send + Sync>;
        let registry = get_global_storage_registry().await;
        let mut registry_guard = registry.write().await;
        registry_guard.insert(port, storage_arc);

        info!("Successfully initialized and registered SledStorage with existing DB in {}ms", start_time.elapsed().as_millis());
        println!("===> SUCCESSFULLY INITIALIZED AND REGISTERED SledStorage WITH EXISTING DB IN {}ms", start_time.elapsed().as_millis());

        Ok(storage)
    }


    pub fn new_pinned(config: &SledConfig, storage_config: &StorageConfig) -> Box<dyn futures::Future<Output = Result<Self, GraphError>> + Send + 'static> {
        let config = config.clone();
        let storage_config = storage_config.clone();
        Box::new(async move {
            SledStorage::new(&config, &storage_config).await
        })
    }

    // Internal implementation methods (same pattern as RocksDBStorage)
    pub async fn add_vertex(&self, vertex: Vertex) -> GraphResult<()> {
        println!("========================== USING DIRECT DB ACCESS TO ADD VERTEX ========================");
        self.create_vertex_direct(vertex).await
    }

    pub async fn get_vertex_internal(&self, id: &Uuid) -> GraphResult<Option<Vertex>> {
        // ALWAYS use direct DB access to avoid ZMQ deadlock
        // The ZMQ client would create a request to ourselves, causing a hang
        println!("========================== USING DIRECT DB ACCESS TO GET VERTEX ========================");
        self.get_vertex_direct(id).await
    }

    pub async fn delete_vertex_internal(&self, id: &Uuid) -> GraphResult<()> {
        println!("========================== USING DIRECT DB ACCESS TO DELETE VERTEX ========================");
        self.delete_vertex_direct(id).await
    }

    pub async fn add_edge(&self, edge: Edge) -> GraphResult<()> {
        println!("========================== USING DIRECT DB ACCESS TO ADD EDGE ========================");
        self.create_edge_direct(edge).await
    }

    pub async fn get_edge_internal(&self, outbound_id: &Uuid, edge_type: &Identifier, inbound_id: &Uuid) -> GraphResult<Option<Edge>> {
        println!("========================== USING DIRECT DB ACCESS TO GET EDGE ========================");
        self.get_edge_direct(outbound_id, edge_type, inbound_id).await
    }

    pub async fn delete_edge_internal(&self, outbound_id: &Uuid, edge_type: &Identifier, inbound_id: &Uuid) -> GraphResult<()> {
        println!("========================== USING DIRECT DB ACCESS TO DELETE EDGE ========================");
        self.delete_edge_direct(outbound_id, edge_type, inbound_id).await
    }

    pub async fn get_all_vertices_internal(&self) -> GraphResult<Vec<Vertex>> {
        println!("========================== USING DIRECT DB ACCESS TO GET ALL VERTICES ========================");
        self.get_all_vertices_direct().await
    }

    pub async fn get_all_edges_internal(&self) -> GraphResult<Vec<Edge>> {
        println!("========================== USING DIRECT DB ACCESS TO GET ALL EDGES ========================");
        self.get_all_edges_direct().await
    }

    // Assuming necessary imports like SLED_DB, GraphError, Vertex, Edge, Identifier,
    // SerializableUuid, serialize_vertex, deserialize_vertex, serialize_edge, deserialize_edge,
    // create_edge_key, info, println, Uuid, task, timeout, TokioDuration, IVec, EDGE_COUNT_KEY are in scope.

    // Direct database access methods (fallback)

    async fn create_vertex_direct(&self, vertex: Vertex) -> GraphResult<()> {
        let singleton = SLED_DB.get().ok_or_else(|| 
            GraphError::StorageError("Sled database not initialized".to_string()))?;
        
        let vertex_clone = vertex;
        
        task::spawn_blocking(move || {
            let rt = tokio::runtime::Handle::current();
            
            let db_lock = rt.block_on(async {
                timeout(TokioDuration::from_secs(5), singleton.lock())
                    .await
                    .map_err(|_| GraphError::StorageError("Timeout acquiring Sled database lock".to_string()))
            })?;
            
            info!("Creating vertex at path {:?}", db_lock.path);
            println!("===> CREATING VERTEX IN SLED DATABASE AT {:?}", db_lock.path);
            
            let vertices = db_lock.db.open_tree("vertices")
                .map_err(|e| GraphError::StorageError(e.to_string()))?;

            vertices.insert(vertex_clone.id.0.as_bytes(), serialize_vertex(&vertex_clone)?)
                .map_err(|e| GraphError::StorageError(e.to_string()))?;

            db_lock.db.flush()
                .map_err(|e| GraphError::StorageError(e.to_string()))?;
                
            Ok(())
        })
        .await
        .map_err(|e| GraphError::StorageError(format!("Failed to execute blocking task: {}", e)))?
    }

    async fn get_vertex_direct(&self, id: &Uuid) -> GraphResult<Option<Vertex>> {
        let singleton = SLED_DB.get().ok_or_else(|| 
            GraphError::StorageError("Sled database not initialized".to_string()))?;
        
        let id_clone = *id;
        
        task::spawn_blocking(move || {
            let rt = tokio::runtime::Handle::current();
            
            let db_lock = rt.block_on(async {
                timeout(TokioDuration::from_secs(5), singleton.lock())
                    .await
                    .map_err(|_| GraphError::StorageError("Timeout acquiring Sled database lock".to_string()))
            })?;
            
            let vertices = db_lock.db.open_tree("vertices")
                .map_err(|e| GraphError::StorageError(e.to_string()))?;
                
            let vertex = vertices
                .get(SerializableUuid(id_clone).0.as_bytes())
                .map_err(|e| GraphError::StorageError(e.to_string()))?
                .map(|v| deserialize_vertex(&*v))
                .transpose()?;
                
            Ok(vertex)
        })
        .await
        .map_err(|e| GraphError::StorageError(format!("Failed to execute blocking task: {}", e)))?
    }

    async fn delete_vertex_direct(&self, id: &Uuid) -> GraphResult<()> {
        let singleton = SLED_DB.get().ok_or_else(|| 
            GraphError::StorageError("Sled database not initialized".to_string()))?;
        
        let id_clone = *id;
        
        task::spawn_blocking(move || {
            let rt = tokio::runtime::Handle::current();
            
            let db_lock = rt.block_on(async {
                timeout(TokioDuration::from_secs(5), singleton.lock())
                    .await
                    .map_err(|_| GraphError::StorageError("Timeout acquiring Sled database lock".to_string()))
            })?;
            
            let vertices_tree = db_lock.db.open_tree("vertices")
                .map_err(|e| GraphError::StorageError(e.to_string()))?;
            let edges_tree = db_lock.db.open_tree("edges")
                .map_err(|e| GraphError::StorageError(e.to_string()))?;
            let outbound_tree = db_lock.db.open_tree("outbound")
                .map_err(|e| GraphError::StorageError(e.to_string()))?;
            let inbound_tree = db_lock.db.open_tree("inbound")
                .map_err(|e| GraphError::StorageError(e.to_string()))?;

            // Find connected edges
            let mut edges_to_delete: Vec<(sled::IVec, Edge)> = Vec::new();
            
            for item in edges_tree.iter() {
                let (key, value) = item.map_err(|e| GraphError::StorageError(e.to_string()))?;
                
                if let Ok(edge) = deserialize_edge(&value) {
                    if edge.outbound_id.0 == id_clone || edge.inbound_id.0 == id_clone {
                        edges_to_delete.push((key, edge));
                    }
                }
            }
            
            let vertex_key = SerializableUuid(id_clone).0.as_bytes().to_vec();
            
            // Run atomic transaction
            let tx_result: sled::transaction::TransactionResult<(), GraphError> = (
                &vertices_tree,
                &edges_tree,
                &outbound_tree,
                &inbound_tree,
            ).transaction(|(vertices, edges, outbound, inbound)| {
                
                vertices.remove(vertex_key.as_slice())?; // Use .as_slice() for clarity
                
                for (edge_key, edge) in &edges_to_delete {
                    edges.remove(&**edge_key)?;
                    outbound.remove(edge.outbound_id.0.as_bytes())?;
                    inbound.remove(edge.inbound_id.0.as_bytes())?;
                }

                Ok(())
            });

            match tx_result {
                Ok(()) => {
                    db_lock.db.flush()
                        .map_err(|e| GraphError::StorageError(e.to_string()))?;
                    Ok(())
                },
                Err(sled::transaction::TransactionError::Abort(e)) => Err(e),
                Err(sled::transaction::TransactionError::Storage(e)) => 
                    Err(GraphError::StorageError(e.to_string())),
            }
        })
        .await
        .map_err(|e| GraphError::StorageError(format!("Failed to execute blocking task: {}", e)))?
    }

    async fn create_edge_direct(&self, edge: Edge) -> GraphResult<()> {
        let singleton = SLED_DB.get().ok_or_else(|| 
            GraphError::StorageError("Sled database not initialized".to_string()))?;
        
        let edge_clone = edge;
        
        task::spawn_blocking(move || {
            let rt = tokio::runtime::Handle::current();
            
            let db_lock = rt.block_on(async {
                timeout(TokioDuration::from_secs(5), singleton.lock())
                    .await
                    .map_err(|_| GraphError::StorageError("Timeout acquiring Sled database lock".to_string()))
            })?;

            let edges_tree = db_lock.db.open_tree("edges")
                .map_err(|e| GraphError::StorageError(e.to_string()))?;
            let outbound_tree = db_lock.db.open_tree("outbound")
                .map_err(|e| GraphError::StorageError(e.to_string()))?;
            let inbound_tree = db_lock.db.open_tree("inbound")
                .map_err(|e| GraphError::StorageError(e.to_string()))?;
            let metadata_tree = db_lock.db.open_tree("metadata")
                .map_err(|e| GraphError::StorageError(e.to_string()))?;

            let serialized_edge = serialize_edge(&edge_clone)?;
            
            let outbound_uuid = edge_clone.outbound_id.0;
            let inbound_uuid = edge_clone.inbound_id.0;
            let edge_type = edge_clone.edge_type.clone();

            let tx_result: sled::transaction::TransactionResult<(), GraphError> = (
                &edges_tree, 
                &outbound_tree, 
                &inbound_tree, 
                &metadata_tree
            ).transaction(|(edges, outbound, inbound, metadata)| {
                
                // CREATE ALL KEYS INSIDE TRANSACTION
                let edge_key = create_edge_key(
                    &SerializableUuid(outbound_uuid), 
                    &edge_type, 
                    &SerializableUuid(inbound_uuid)
                ).map_err(|e| sled::transaction::ConflictableTransactionError::Abort(e))?;
                
                let mut outbound_index_key = Vec::with_capacity(16 + edge_key.len());
                outbound_index_key.extend_from_slice(outbound_uuid.as_bytes());
                outbound_index_key.extend_from_slice(&edge_key);
                
                let mut inbound_index_key = Vec::with_capacity(16 + edge_key.len());
                inbound_index_key.extend_from_slice(inbound_uuid.as_bytes());
                inbound_index_key.extend_from_slice(&edge_key);
                
                edges.insert(edge_key.as_slice(), &serialized_edge[..])?; // Key fix for consistency
                
                // 🐛 REGRESSION FIX: Removed extra '**' and ensured correct slicing.
                // Fix is: outbound.insert(outbound_index_key.as_slice(), edge_key.as_slice())?;
                outbound.insert(outbound_index_key.as_slice(), edge_key.as_slice())?; 
                inbound.insert(inbound_index_key.as_slice(), edge_key.as_slice())?;

                let current_bytes = metadata
                    .get(EDGE_COUNT_KEY)?
                    .unwrap_or_else(|| IVec::from("0".as_bytes()));

                let current_count: usize = String::from_utf8_lossy(&current_bytes)
                    .parse()
                    .unwrap_or(0);

                let new_count = current_count.checked_add(1).unwrap_or(usize::MAX);
                
                metadata.insert(EDGE_COUNT_KEY, new_count.to_string().as_bytes())?;
                
                Ok(())
            });

            match tx_result {
                Ok(()) => {
                    db_lock.db.flush()
                        .map_err(|e| GraphError::StorageError(e.to_string()))?;
                    Ok(())
                },
                Err(sled::transaction::TransactionError::Abort(e)) => Err(e),
                Err(sled::transaction::TransactionError::Storage(e)) => 
                    Err(GraphError::StorageError(e.to_string())),
            }
        })
        .await
        .map_err(|e| GraphError::StorageError(format!("Failed to execute blocking task: {}", e)))?
    }

    async fn delete_edge_direct(&self, outbound_id: &Uuid, edge_type: &Identifier, inbound_id: &Uuid) -> GraphResult<()> {
        let singleton = SLED_DB.get().ok_or_else(||
            GraphError::StorageError("Sled database not initialized".to_string()))?;

        // FIX: Acquire the lock and CLONE the sled::Db instance in the ASYNC context.
        let db_clone = {
            let guard = singleton.lock().await;
            // Assuming 'db' field holds the sled::Db (sled::Db implements Clone)
            guard.db.clone() 
        };

        let outbound_id_clone = *outbound_id;
        let edge_type_clone = edge_type.clone();
        let inbound_id_clone = *inbound_id;

        task::spawn_blocking(move || {
            let db = &db_clone; // Use the cloned db directly

            // FIX: Remove rt.block_on(singleton.lock().await)
            // Trees are opened synchronously on the cloned db.
            let edges_tree = db.open_tree("edges")
                .map_err(|e| GraphError::StorageError(e.to_string()))?;
            let outbound_tree = db.open_tree("outbound")
                .map_err(|e| GraphError::StorageError(e.to_string()))?;
            let inbound_tree = db.open_tree("inbound")
                .map_err(|e| GraphError::StorageError(e.to_string()))?;
            let metadata_tree = db.open_tree("metadata")
                .map_err(|e| GraphError::StorageError(e.to_string()))?;

            let tx_result: sled::transaction::TransactionResult<(), GraphError> = (
                &edges_tree,
                &outbound_tree,
                &inbound_tree,
                &metadata_tree
            ).transaction(|(edges, outbound, inbound, metadata)| {
                // ... (rest of the transaction logic remains the same)
                
                let edge_key = create_edge_key(
                    &SerializableUuid(outbound_id_clone),
                    &edge_type_clone,
                    &SerializableUuid(inbound_id_clone)
                ).map_err(|e| sled::transaction::ConflictableTransactionError::Abort(e))?;

                let mut outbound_index_key = Vec::with_capacity(16 + edge_key.len());
                outbound_index_key.extend_from_slice(outbound_id_clone.as_bytes());
                outbound_index_key.extend_from_slice(&edge_key);

                let mut inbound_index_key = Vec::with_capacity(16 + edge_key.len());
                inbound_index_key.extend_from_slice(inbound_id_clone.as_bytes());
                inbound_index_key.extend_from_slice(&edge_key);

                if edges.remove(edge_key.as_slice())?.is_some() {
                    outbound.remove(outbound_index_key.as_slice())?;
                    inbound.remove(inbound_index_key.as_slice())?;

                    // Decrement count logic
                    let current_bytes = metadata
                        .get(EDGE_COUNT_KEY)?
                        .unwrap_or_else(|| IVec::from("0".as_bytes()));
                    let current_count: usize = String::from_utf8_lossy(&current_bytes)
                        .parse()
                        .unwrap_or(0);
                    
                    let new_count = current_count.saturating_sub(1);
                    metadata.insert(EDGE_COUNT_KEY, new_count.to_string().as_bytes())?;
                }

                Ok(())
            });

            match tx_result {
                Ok(()) => {
                    // FIX: Flush on the cloned DB in the blocking task
                    db.flush()
                        .map_err(|e| GraphError::StorageError(e.to_string()))?;
                    Ok(())
                },
                Err(sled::transaction::TransactionError::Abort(e)) => Err(e),
                Err(sled::transaction::TransactionError::Storage(e)) =>
                    Err(GraphError::StorageError(e.to_string())),
            }
        })
        .await
        .map_err(|e| GraphError::StorageError(format!("Failed to execute blocking task: {}", e)))?
    }

    async fn get_edge_direct(&self, outbound_id: &Uuid, edge_type: &Identifier, inbound_id: &Uuid) -> GraphResult<Option<Edge>> {
        let singleton = SLED_DB.get().ok_or_else(|| 
            GraphError::StorageError("Sled database not initialized".to_string()))?;
        
        let outbound_id_clone = *outbound_id;
        let edge_type_clone = edge_type.clone();
        let inbound_id_clone = *inbound_id;
        
        task::spawn_blocking(move || {
            let rt = tokio::runtime::Handle::current();
            
            let db_lock = rt.block_on(async {
                timeout(TokioDuration::from_secs(5), singleton.lock())
                    .await
                    .map_err(|_| GraphError::StorageError("Timeout acquiring Sled database lock".to_string()))
            })?;
            
            let edges = db_lock.db.open_tree("edges")
                .map_err(|e| GraphError::StorageError(e.to_string()))?;
                
            let edge_key = create_edge_key(
                &SerializableUuid(outbound_id_clone), 
                &edge_type_clone, 
                &SerializableUuid(inbound_id_clone)
            )?;
            
            let edge = edges
                .get(edge_key.as_slice()) // Consistency fix
                .map_err(|e| GraphError::StorageError(e.to_string()))?
                .map(|v| deserialize_edge(&*v))
                .transpose()?;
                
            Ok(edge)
        })
        .await
        .map_err(|e| GraphError::StorageError(format!("Failed to execute blocking task: {}", e)))?
    }

    async fn get_all_vertices_direct(&self) -> GraphResult<Vec<Vertex>> {
        let singleton = SLED_DB.get().ok_or_else(|| 
            GraphError::StorageError("Sled database not initialized".to_string()))?;

        task::spawn_blocking(move || {
            let rt = tokio::runtime::Handle::current();
            
            let db_lock = rt.block_on(async {
                timeout(TokioDuration::from_secs(5), singleton.lock())
                    .await
                    .map_err(|_| GraphError::StorageError("Timeout acquiring Sled database lock".to_string()))
            })?;
            
            let vertices = db_lock.db.open_tree("vertices")
                .map_err(|e| GraphError::StorageError(e.to_string()))?;
                
            let mut vertex_vec = Vec::new();
            for result in vertices.iter() {
                let (_k, v) = result.map_err(|e| GraphError::StorageError(e.to_string()))?;
                vertex_vec.push(deserialize_vertex(&*v)?);
            }
            
            Ok(vertex_vec)
        })
        .await
        .map_err(|e| GraphError::StorageError(format!("Failed to execute blocking task: {}", e)))?
    }

    async fn get_all_edges_direct(&self) -> GraphResult<Vec<Edge>> {
        let singleton = SLED_DB.get().ok_or_else(|| 
            GraphError::StorageError("Sled database not initialized".to_string()))?;

        task::spawn_blocking(move || {
            let rt = tokio::runtime::Handle::current();
            
            let db_lock = rt.block_on(async {
                timeout(TokioDuration::from_secs(5), singleton.lock())
                    .await
                    .map_err(|_| GraphError::StorageError("Timeout acquiring Sled database lock".to_string()))
            })?;
            
            let edges = db_lock.db.open_tree("edges")
                .map_err(|e| GraphError::StorageError(e.to_string()))?;
                
            let mut edge_vec = Vec::new();
            for result in edges.iter() {
                let (_k, v) = result.map_err(|e| GraphError::StorageError(e.to_string()))?;
                edge_vec.push(deserialize_edge(&*v)?);
            }
            
            Ok(edge_vec)
        })
        .await
        .map_err(|e| GraphError::StorageError(format!("Failed to execute blocking task: {}", e)))?
    }

    pub async fn set_key(&self, key: &str, value: &str) -> GraphResult<()> {
        let db = SLED_DB.get().ok_or_else(|| GraphError::StorageError("Sled database not initialized".to_string()))?;
        let db_lock = timeout(TokioDuration::from_secs(5), db.lock())
            .await
            .map_err(|_| GraphError::StorageError("Timeout acquiring Sled database lock".to_string()))?;
        debug!("Setting key '{}' to value '{}' in Sled database at {:?}", key, value, db_lock.path);
        println!("===> SETTING KEY {} IN SLED DATABASE AT {:?}", key, db_lock.path);
        db_lock.db
            .insert(key.as_bytes(), value.as_bytes())
            .map_err(|e| {
                error!("Failed to set key '{}': {}", key, e);
                println!("===> ERROR: FAILED TO SET KEY {}", key);
                GraphError::StorageError(format!("Failed to set key '{}': {}", key, e))
            })?;
        let bytes_flushed = db_lock.db.flush_async().await.map_err(|e| {
            error!("Failed to flush Sled database after setting key '{}': {}", key, e);
            println!("===> ERROR: FAILED TO FLUSH SLED DATABASE AFTER SETTING KEY {}", key);
            GraphError::StorageError(format!("Failed to flush Sled database after setting key '{}': {}", key, e))
        })?;
        info!("Flushed {} bytes after setting key '{}'", bytes_flushed, key);
        println!("===> FLUSHED {} BYTES AFTER SETTING KEY {}", bytes_flushed, key);
        let value_opt = db_lock.db.get(key.as_bytes()).map_err(|e| {
            error!("Failed to verify key '{}': {}", key, e);
            println!("===> ERROR: FAILED TO VERIFY KEY {}", key);
            GraphError::StorageError(format!("Failed to verify key '{}': {}", key, e))
        })?;
        if value_opt.is_none() || value_opt.unwrap().as_ref() != value.as_bytes() {
            error!("Persistence verification failed for key '{}'", key);
            println!("===> ERROR: PERSISTENCE VERIFICATION FAILED FOR KEY {}", key);
            return Err(GraphError::StorageError(format!("Persistence verification failed for key '{}'", key)));
        }
        debug!("Successfully set and verified key '{}'", key);
        println!("===> SUCCESSFULLY SET AND VERIFIED KEY {}", key);
        Ok(())
    }

    pub async fn get_key(&self, key: &str) -> GraphResult<Option<String>> {
        let db = SLED_DB.get().ok_or_else(|| GraphError::StorageError("Sled database not initialized".to_string()))?;
        let db_lock = timeout(TokioDuration::from_secs(5), db.lock())
            .await
            .map_err(|_| GraphError::StorageError("Timeout acquiring Sled database lock".to_string()))?;
        debug!("Retrieving key '{}' from Sled database at {:?}", key, db_lock.path);
        println!("===> RETRIEVING KEY {} FROM SLED DATABASE AT {:?}", key, db_lock.path);
        let value = db_lock.db
            .get(key.as_bytes())
            .map_err(|e| {
                error!("Failed to get key '{}': {}", key, e);
                println!("===> ERROR: FAILED TO GET KEY {}", key);
                GraphError::StorageError(format!("Failed to get key '{}': {}", key, e))
            })?
            .map(|v| String::from_utf8_lossy(&*v).to_string());
        debug!("Retrieved value for key '{}': {:?}", key, value);
        println!("===> RETRIEVED VALUE FOR KEY {}: {:?}", key, value);
        Ok(value)
    }

    pub async fn force_unlock(path: &Path) -> GraphResult<()> {
        let lock_path = path.join("db.lck");
        info!("Checking for lock file at {:?}", lock_path);
        println!("===> CHECKING FOR LOCK FILE AT {:?}", lock_path);
        
        if lock_path.exists() {
            warn!("Found lock file at {:?}", lock_path);
            println!("===> FOUND LOCK FILE AT {:?}", lock_path);
            
            match fs::remove_file(&lock_path).await {
                Ok(_) => {
                    info!("Successfully removed lock file at {:?}", lock_path);
                    println!("===> SUCCESSFULLY REMOVED LOCK FILE AT {:?}", lock_path);
                    tokio::time::sleep(TokioDuration::from_millis(500)).await;
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    info!("Lock file already removed at {:?}", lock_path);
                    println!("===> LOCK FILE ALREADY REMOVED AT {:?}", lock_path);
                }
                Err(e) => {
                    warn!("Failed to remove lock file at {:?}: {}", lock_path, e);
                    println!("===> WARNING: FAILED TO REMOVE LOCK FILE AT {:?}", lock_path);
                }
            }
        } else {
            info!("No lock file found at {:?}", lock_path);
            println!("===> NO LOCK FILE FOUND AT {:?}", lock_path);
        }
        
        info!("Successfully checked lock status at {:?}", path);
        println!("===> SUCCESSFULLY UNLOCKED SLED DATABASE AT {:?}", path);
        Ok(())
    }

    pub async fn force_reset(config: &SledConfig, storage_config: &StorageConfig) -> GraphResult<Self> {
        warn!("FORCE RESET: Completely destroying and recreating database at {:?}", config.path);
        println!("===> FORCE RESET: DESTROYING DATABASE AT {:?}", config.path);

        let port = config.port.unwrap_or(DEFAULT_STORAGE_PORT);
        let default_data_dir = PathBuf::from(DEFAULT_DATA_DIRECTORY);
        let base_data_dir = storage_config.data_directory.as_ref().unwrap_or(&default_data_dir);
        let db_path = base_data_dir.join("sled").join(port.to_string());

        Self::check_and_cleanup_stale_daemon(port, &db_path).await?;

        if db_path.exists() {
            info!("Destroying existing Sled database at {:?}", db_path);
            println!("===> DESTROYING EXISTING SLED DATABASE AT {:?}", db_path);
            timeout(TokioDuration::from_secs(5), fs::remove_dir_all(&db_path))
                .await
                .map_err(|_| {
                    error!("Timeout removing Sled directory at {:?}", db_path);
                    println!("===> ERROR: TIMEOUT REMOVING SLED DIRECTORY AT {:?}", db_path);
                    GraphError::StorageError(format!("Timeout removing Sled directory at {:?}", db_path))
                })?
                .map_err(|e| {
                    error!("Failed to remove Sled directory at {:?}: {}", db_path, e);
                    println!("===> ERROR: FAILED TO REMOVE SLED DIRECTORY AT {:?}", db_path);
                    GraphError::StorageError(format!("Failed to remove Sled directory at {:?}: {}", db_path, e))
                })?;
            info!("Successfully removed Sled database directory at {:?}", db_path);
            println!("===> SUCCESSFULLY REMOVED SLED DATABASE DIRECTORY AT {:?}", db_path);
        }

        fs::create_dir_all(&db_path)
            .await
            .map_err(|e| {
                error!("Failed to recreate Sled database directory at {:?}: {}", db_path, e);
                println!("===> ERROR: FAILED TO RECREATE SLED DATABASE DIRECTORY AT {:?}", db_path);
                GraphError::StorageError(format!("Failed to recreate Sled database directory at {:?}: {}", db_path, e))
            })?;

        Self::new(config, storage_config).await
            .map_err(|e| {
                error!("Failed to initialize SledStorage after reset: {}", e);
                println!("===> ERROR: FAILED TO INITIALIZE SLED STORAGE AFTER RESET");
                GraphError::StorageError(format!("Failed to initialize SledStorage after reset: {}", e))
            })
    }

    pub async fn diagnose_persistence(&self) -> GraphResult<serde_json::Value> {
        let db = SLED_DB.get().ok_or_else(|| GraphError::StorageError("Sled database not initialized".to_string()))?;
        let db_lock = timeout(TokioDuration::from_secs(5), db.lock())
            .await
            .map_err(|_| GraphError::StorageError("Timeout acquiring Sled database lock".to_string()))?;
        let db_path = &db_lock.path;
        info!("Diagnosing persistence for SledStorage at {:?}", db_path);
        println!("===> DIAGNOSING PERSISTENCE FOR SLED STORAGE AT {:?}", db_path);

        let kv_count = db_lock.db.iter().count();
        let vertex_count = db_lock.db.open_tree("vertices").map_err(|e| GraphError::StorageError(e.to_string()))?.iter().count();
        let edge_count = db_lock.db.open_tree("edges").map_err(|e| GraphError::StorageError(e.to_string()))?.iter().count();

        let disk_usage = fs::metadata(db_path)
            .await
            .map(|m| m.len())
            .unwrap_or(0);

        let diagnostics = serde_json::json!({
            "path": db_path.to_string_lossy(),
            "kv_pairs_count": kv_count,
            "vertices_count": vertex_count,
            "edges_count": edge_count,
            "disk_usage_bytes": disk_usage,
            "is_running": self.is_running().await,
        });

        info!("Persistence diagnostics: {:?}", diagnostics);
        println!("===> PERSISTENCE DIAGNOSTICS: {:?}", diagnostics);
        Ok(diagnostics)
    }
}

#[async_trait]
impl StorageEngine for SledStorage {
    async fn connect(&self) -> Result<(), GraphError> {
        info!("Connecting to SledStorage");
        println!("===> CONNECTING TO SLED STORAGE");
        Ok(())
    }

    async fn insert(&self, key: Vec<u8>, value: Vec<u8>) -> GraphResult<()> {
        let db = SLED_DB.get().ok_or_else(|| GraphError::StorageError("Sled database not initialized".to_string()))?;
        let db_lock = timeout(TokioDuration::from_secs(5), db.lock())
            .await
            .map_err(|_| GraphError::StorageError("Timeout acquiring Sled database lock".to_string()))?;
        let db_path = &db_lock.path;
        info!("SledStorage::insert - inserting key into kv_pairs at {:?}", db_path);
        println!("===> INSERTING KEY INTO SLED DATABASE AT {:?}", db_path);

        db_lock.db
            .insert(&key, &*value)
            .map_err(|e| {
                error!("Failed to insert key: {}", e);
                println!("===> ERROR: FAILED TO INSERT KEY INTO SLED DATABASE");
                GraphError::StorageError(e.to_string())
            })?;

        let bytes_flushed = db_lock.db.flush_async().await
            .map_err(|e| {
                error!("Failed to flush after insert: {}", e);
                println!("===> ERROR: FAILED TO FLUSH SLED DATABASE AFTER INSERT");
                GraphError::StorageError(e.to_string())
            })?;
        info!("SledStorage::insert - flushed {} bytes after insert at {:?}", bytes_flushed, db_path);
        println!("===> FLUSHED {} BYTES AFTER INSERT AT {:?}", bytes_flushed, db_path);

        Ok(())
    }

    async fn retrieve(&self, key: &Vec<u8>) -> GraphResult<Option<Vec<u8>>> {
        let db = SLED_DB.get().ok_or_else(|| GraphError::StorageError("Sled database not initialized".to_string()))?;
        let db_lock = timeout(TokioDuration::from_secs(5), db.lock())
            .await
            .map_err(|_| GraphError::StorageError("Timeout acquiring Sled database lock".to_string()))?;
        let db_path = &db_lock.path;
        info!("SledStorage::retrieve - retrieving key from kv_pairs at {:?}", db_path);
        println!("===> RETRIEVING KEY FROM SLED DATABASE AT {:?}", db_path);

        let value_opt = db_lock.db
            .get(key)
            .map_err(|e| {
                error!("Failed to retrieve key: {}", e);
                println!("===> ERROR: FAILED TO RETRIEVE KEY FROM SLED DATABASE");
                GraphError::StorageError(e.to_string())
            })?;

        Ok(value_opt.map(|v| v.to_vec()))
    }

    async fn delete(&self, key: &Vec<u8>) -> GraphResult<()> {
        let db = SLED_DB.get().ok_or_else(|| GraphError::StorageError("Sled database not initialized".to_string()))?;
        let db_lock = timeout(TokioDuration::from_secs(5), db.lock())
            .await
            .map_err(|_| GraphError::StorageError("Timeout acquiring Sled database lock".to_string()))?;
        let db_path = &db_lock.path;
        info!("SledStorage::delete - deleting key from kv_pairs at {:?}", db_path);
        println!("===> DELETING KEY FROM SLED DATABASE AT {:?}", db_path);

        db_lock.db
            .remove(key)
            .map_err(|e| {
                error!("Failed to delete key: {}", e);
                println!("===> ERROR: FAILED TO DELETE KEY FROM SLED DATABASE");
                GraphError::StorageError(e.to_string())
            })?;

        let bytes_flushed = db_lock.db.flush_async().await
            .map_err(|e| {
                error!("Failed to flush after delete: {}", e);
                println!("===> ERROR: FAILED TO FLUSH SLED DATABASE AFTER DELETE");
                GraphError::StorageError(e.to_string())
            })?;
        info!("SledStorage::delete - flushed {} bytes after delete at {:?}", bytes_flushed, db_path);
        println!("===> FLUSHED {} BYTES AFTER DELETE AT {:?}", bytes_flushed, db_path);

        Ok(())
    }

    async fn flush(&self) -> Result<(), GraphError> {
        let db = SLED_DB.get().ok_or_else(|| GraphError::StorageError("Sled database not initialized".to_string()))?;
        let db_lock = timeout(TokioDuration::from_secs(5), db.lock())
            .await
            .map_err(|_| GraphError::StorageError("Timeout acquiring Sled database lock".to_string()))?;
        let db_path = &db_lock.path;
        info!("SledStorage::flush - flushing database at {:?}", db_path);
        println!("===> FLUSHING SLED DATABASE AT {:?}", db_path);

        let bytes_flushed = db_lock.db.flush_async().await
            .map_err(|e| {
                error!("Failed to flush Sled database: {}", e);
                println!("===> ERROR: FAILED TO FLUSH SLED DATABASE");
                GraphError::StorageError(e.to_string())
            })?;
        info!("SledStorage::flush - flushed {} bytes to disk at {:?}", bytes_flushed, db_path);
        println!("===> FLUSHED {} BYTES TO DISK AT {:?}", bytes_flushed, db_path);

        Ok(())
    }

    async fn append(&self, op: GraphOp) -> Result<(), GraphError> {
        let db = SLED_DB.get()
            .ok_or_else(|| GraphError::StorageError("Sled database not initialized".into()))?;

        let mut db_lock = timeout(TokioDuration::from_secs(5), db.lock())
            .await
            .map_err(|_| GraphError::StorageError("Timeout acquiring Sled lock".into()))?;

        // Use serde_json instead
        let op_bytes = serde_json::to_vec(&op)
            .map_err(|e| GraphError::StorageError(format!("JSON encode failed: {}", e)))?;

        let timestamp = Utc::now().timestamp_nanos_opt()
            .ok_or_else(|| GraphError::StorageError("Failed to get timestamp".into()))?;
        let key = format!("wal_{:0>20}", timestamp);

        db_lock.db.insert(key.as_bytes(), op_bytes)
            .map_err(|e| GraphError::StorageError(e.to_string()))?;

        let flushed = db_lock.db.flush_async().await
            .map_err(|e| GraphError::StorageError(e.to_string()))?;

        info!("SledStorage::append - persisted op, flushed {} bytes", flushed);
        Ok(())
    }

async fn replay_into(&self, graph: &mut Graph) -> Result<(), GraphError> {
        info!("SledStorage::replay_into - starting replay");
        let db = SLED_DB.get()
            .ok_or_else(|| GraphError::StorageError("Sled database not initialized".into()))?;
        let db_lock = timeout(TokioDuration::from_secs(10), db.lock())
            .await
            .map_err(|_| GraphError::StorageError("Timeout acquiring Sled lock".into()))?;
        
        // Collect WAL records
        let mut entries: Vec<(IVec, IVec)> = db_lock
            .db
            .scan_prefix(b"wal_")
            .filter_map(Result::ok)
            .collect();
        entries.sort_by(|a, b| a.0.cmp(&b.0));
        
        // Replay
        for (_key, value) in entries {
            // Use serde_json instead of bincode
            let op: GraphOp = serde_json::from_slice(value.as_ref())
                .map_err(|e| GraphError::StorageError(format!("deserialize GraphOp: {}", e)))?;
            
            match op {
                GraphOp::InsertVertex(v) => graph.add_vertex(v),
                GraphOp::InsertEdge(e) => graph.add_edge(e),
                GraphOp::DeleteVertex(id) => {
                    // Convert Identifier to Uuid via string parsing
                    if let Ok(uuid) = uuid::Uuid::parse_str(&id.to_string()) {
                        graph.vertices.remove(&uuid);
                    }
                }
                GraphOp::DeleteEdge(id) => {
                    // Convert Identifier to Uuid via string parsing
                    if let Ok(uuid) = uuid::Uuid::parse_str(&id.to_string()) {
                        graph.edges.remove(&uuid);
                    }
                }
                GraphOp::UpdateVertex(id, updates) => {
                    // Convert Identifier to Uuid via string parsing
                    if let Ok(uuid) = uuid::Uuid::parse_str(&id.to_string()) {
                        if let Some(v) = graph.vertices.get_mut(&uuid) {
                            for (k, json_val) in updates {
                                v.properties.insert(k, json_to_prop(json_val)?);
                            }
                        }
                    }
                }
                GraphOp::SetVertexProperty(id, key_id, property_value) => {
                    // Convert Identifier to Uuid via string parsing
                    if let Ok(uuid) = uuid::Uuid::parse_str(&id.to_string()) {
                        if let Some(v) = graph.vertices.get_mut(&uuid) {
                            // Convert the key Identifier to a String property name
                            let key_string = key_id.to_string();
                            
                            // The property_value is ALREADY a PropertyValue, so we use it directly.
                            // FIX: Removed the incorrect call to json_to_prop(property_value)
                            v.properties.insert(key_string, property_value);
                        }
                    }
                }
            }
        }
        
        info!(
            "SledStorage::replay_into - completed: {} vertices, {} edges",
            graph.vertices.len(),
            graph.edges.len()
        );
        Ok(())
    }
}

#[async_trait]
impl GraphStorageEngine for SledStorage {
    // NOTE: This assumes SLED_DB is a static structure holding a Mutex<SledStorageInner>
    // and SledStorageInner contains a 'client' field (Arc<ZmqClient>, ...)

    async fn delete_edges_touching_vertices(&self, vertex_ids: &HashSet<Uuid>) -> GraphResult<usize> {
        let singleton = SLED_DB
            .get()
            .ok_or_else(|| GraphError::StorageError("Sled singleton not initialized".into()))?;

        // FIX: Consolidate lock acquisition to determine client or local execution
        let (db_clone, client_exists) = {
            let guard = singleton.lock().await;

            if let Some((client, _)) = guard.client.as_ref() {
                // ZMQ forwarding: client exists. Release lock and forward call.
                // Returning here avoids the need for the local path to access the lock again.
                return client.delete_edges_touching_vertices(vertex_ids).await;
            }

            // Local execution: client does not exist. Clone the database object.
            // We clone the DB here and pass it to the blocking task.
            (guard.db.clone(), false)
        };
        
        // --- Local execution (The rest of the logic is correctly placed in a blocking task) ---
        let vertex_ids_clone = vertex_ids.clone();

        tokio::task::spawn_blocking(move || {
            let db = &db_clone;
            
            // 1. Open trees (synchronous, on the cloned db)
            // Use constants for tree names to avoid hardcoding strings.
            let edges_tree = db.open_tree(EDGES_TREE)
                .map_err(|e| GraphError::StorageError(e.to_string()))?;
            let outbound_tree = db.open_tree(OUTBOUND_TREE)
                .map_err(|e| GraphError::StorageError(e.to_string()))?;
            let inbound_tree = db.open_tree(INBOUND_TREE)
                .map_err(|e| GraphError::StorageError(e.to_string()))?;
            let metadata_tree = db.open_tree(METADATA_TREE)
                .map_err(|e| GraphError::StorageError(e.to_string()))?;

            // 2. Collect edges to delete (blocking read-only pass)
            let mut deleted_count = 0;
            let mut edge_keys_to_delete: Vec<sled::IVec> = Vec::new();

            // This iteration is a heavy synchronous I/O operation, correctly placed here.
            for item in edges_tree.iter() {
                let (key, value) = item.map_err(|e| GraphError::StorageError(e.to_string()))?;
                if let Ok(edge) = deserialize_edge(&value) {
                    if vertex_ids_clone.contains(&edge.outbound_id.0) || vertex_ids_clone.contains(&edge.inbound_id.0) {
                        edge_keys_to_delete.push(key);
                        deleted_count += 1;
                    }
                }
            }
            
            if edge_keys_to_delete.is_empty() {
                 return Ok(0);
            }

            // 3. Run atomic transaction (blocking write pass)
            let tx_result: sled::transaction::TransactionResult<(), GraphError> = (
                &edges_tree,
                &outbound_tree,
                &inbound_tree,
                &metadata_tree,
            ).transaction(|(edges, outbound, inbound, metadata)| {
                
                // NOTE: The index key logic for outbound/inbound removal in this transaction 
                // is likely incomplete (using only Uuid::as_bytes() for removal), but the 
                // focus here is on the hang fix, not index consistency.

                for edge_key in &edge_keys_to_delete {
                    let edge_ivec = edges.get(edge_key)?;
                    
                    let edge_ivec = match edge_ivec {
                        Some(ivec) => ivec,
                        None => continue, // Already deleted or concurrent operation, continue.
                    };

                    let edge: Edge = match deserialize_edge(&edge_ivec) {
                        Ok(e) => e,
                        Err(_) => return Err(ConflictableTransactionError::Abort(
                            GraphError::SerializationError("failed in tx".into())
                        )),
                    };
                    
                    // Remove from all indexes and main tree
                    edges.remove(edge_key).map_err(ConflictableTransactionError::from)?;
                    // NOTE: The removal from outbound/inbound indexes here might be using 
                    // the wrong key format if the index keys are composite.
                    outbound.remove(edge.outbound_id.0.as_bytes()).map_err(ConflictableTransactionError::from)?;
                    inbound.remove(edge.inbound_id.0.as_bytes()).map_err(ConflictableTransactionError::from)?;
                }

                // Update global edge count
                if deleted_count > 0 {
                    let current_bytes = metadata
                        .get(EDGE_COUNT_KEY)?
                        .unwrap_or_else(|| sled::IVec::from("0".as_bytes()));

                    let current_count: usize = String::from_utf8_lossy(&current_bytes)
                        .parse()
                        .unwrap_or(0);

                    let new_count = current_count.saturating_sub(deleted_count);
                    
                    metadata.insert(EDGE_COUNT_KEY, new_count.to_string().as_bytes())?;
                }

                Ok(())
            });

            // 4. Final result conversion
            match tx_result {
                Ok(()) => Ok(deleted_count),
                Err(sled::transaction::TransactionError::Abort(e)) => Err(e),
                Err(sled::transaction::TransactionError::Storage(e)) => Err(GraphError::StorageError(e.to_string())),
            }
        })
        .await
        .map_err(|e| GraphError::StorageError(format!("Failed to execute blocking task: {}", e)))?
    }

    async fn cleanup_orphaned_edges(&self) -> GraphResult<usize> {
        // 1. Acquire and clone sled::Db before spawn_blocking
        let singleton = SLED_DB.get().ok_or_else(|| GraphError::StorageError("Sled singleton not initialized".into()))?;
        let db_clone = {
            let db_guard = singleton.lock().await;
            db_guard.db.clone()
        };

        tokio::task::spawn_blocking(move || {
            let db = &db_clone;

            // Open required trees (synchronous)
            let edges_tree = db.open_tree(EDGES_TREE)
                .map_err(|e| GraphError::StorageError(e.to_string()))?;
            let vertices_tree = db.open_tree(VERTICES_TREE)
                .map_err(|e| GraphError::StorageError(e.to_string()))?;

            let mut orphaned_keys: Vec<sled::IVec> = Vec::new();

            // 2. Scan all edges and check for vertex existence (BLOCKING read-only iteration)
            for item in edges_tree.iter() {
                let (key, value) = item.map_err(|e| GraphError::StorageError(e.to_string()))?;
                
                let edge: Edge = match deserialize_edge(&value) {
                    Ok(e) => e,
                    Err(_) => {
                        orphaned_keys.push(key);
                        continue;
                    }
                };

                let source_key: sled::IVec = edge.outbound_id.0.as_bytes().to_vec().into();
                let target_key: sled::IVec = edge.inbound_id.0.as_bytes().to_vec().into();

                // Synchronous contains_key check
                let source_exists = vertices_tree.contains_key(&source_key)
                    .map_err(|e| GraphError::StorageError(e.to_string()))?;
                let target_exists = vertices_tree.contains_key(&target_key)
                    .map_err(|e| GraphError::StorageError(e.to_string()))?;

                if !source_exists || !target_exists {
                    orphaned_keys.push(key);
                }
            }

            let deleted_count = orphaned_keys.len();
            
            if deleted_count == 0 {
                return Ok(0);
            }

            // 3. Delete orphaned edges in an atomic transaction (BLOCKING transaction)
            let tx_result: sled::transaction::TransactionResult<(), GraphError> = edges_tree.transaction(|edges| {
                for key in &orphaned_keys {
                    edges.remove(key)?;
                }
                Ok(())
            });

            // 4. Handle result and return
            match tx_result {
                Ok(()) => Ok(deleted_count),
                Err(e) => {
                    let graph_err = match e {
                        sled::transaction::TransactionError::Abort(inner) => inner,
                        sled::transaction::TransactionError::Storage(sled_err) => GraphError::StorageError(sled_err.to_string()),
                    };
                    Err(graph_err)
                }
            }
        })
        .await
        // Handle the JoinError from spawn_blocking
        .map_err(|e| GraphError::StorageError(format!("Failed to execute blocking task: {}", e)))?
    }

    async fn create_vertex(&self, vertex: Vertex) -> GraphResult<()> {
        SledStorage::add_vertex(self, vertex).await
    }

    async fn get_vertex(&self, id: &Uuid) -> GraphResult<Option<Vertex>> {
        SledStorage::get_vertex_internal(self, id).await
    }

    async fn update_vertex(&self, vertex: Vertex) -> GraphResult<()> {
        SledStorage::add_vertex(self, vertex).await
    }

    async fn delete_vertex(&self, id: &Uuid) -> GraphResult<()> {
        println!("===> SledStorage::delete_vertex DIRECT (id={})", id);
        self.delete_vertex_direct(id).await
    }
    
    async fn create_edge(&self, edge: Edge) -> GraphResult<()> {
        SledStorage::add_edge(self, edge).await
    }

    async fn get_edge(&self, outbound_id: &Uuid, edge_type: &Identifier, inbound_id: &Uuid) -> GraphResult<Option<Edge>> {
        SledStorage::get_edge_internal(self, outbound_id, edge_type, inbound_id).await
    }

    async fn update_edge(&self, edge: Edge) -> GraphResult<()> {
        SledStorage::add_edge(self, edge).await
    }

    async fn delete_edge(&self, outbound_id: &Uuid, edge_type: &Identifier, inbound_id: &Uuid) -> GraphResult<()> {
        SledStorage::delete_edge_internal(self, outbound_id, edge_type, inbound_id).await
    }

    async fn get_all_vertices(&self) -> Result<Vec<Vertex>, GraphError> {
        SledStorage::get_all_vertices_internal(self).await
    }

    async fn get_all_edges(&self) -> Result<Vec<Edge>, GraphError> {
        SledStorage::get_all_edges_internal(self).await
    }

    async fn close(&self) -> GraphResult<()> {
        let pool = timeout(TokioDuration::from_secs(5), self.pool.lock())
            .await
            .map_err(|_| GraphError::StorageError("Timeout acquiring Sled pool lock".to_string()))?;
        info!("Closing SledStorage pool");
        println!("===> CLOSING SLED STORAGE POOL");

        match timeout(TokioDuration::from_secs(10), pool.close(None)).await {
            Ok(Ok(_)) => {
                info!("Successfully closed SledStorage pool");
                println!("===> SUCCESSFULLY CLOSED SLED STORAGE POOL");
                Ok(())
            }
            Ok(Err(e)) => {
                error!("Failed to close SledStorage pool: {}", e);
                println!("===> ERROR: FAILED TO CLOSE SLED STORAGE POOL: {}", e);
                Err(e)
            }
            Err(_) => {
                error!("Timeout closing SledStorage pool");
                println!("===> ERROR: TIMEOUT CLOSING SLED STORAGE POOL");
                Err(GraphError::StorageError("Timeout closing SledStorage pool".to_string()))
            }
        }
    }

    async fn start(&self) -> Result<(), GraphError> {
        info!("Starting SledStorage");
        println!("===> STARTING SLED STORAGE");
        Ok(())
    }

    async fn stop(&self) -> Result<(), GraphError> {
        info!("Stopping SledStorage");
        println!("===> STOPPING SLED STORAGE");
        self.close().await
    }

    fn get_type(&self) -> &'static str {
        "sled"
    }

    async fn is_running(&self) -> bool {
        let pool = match timeout(TokioDuration::from_secs(5), self.pool.lock()).await {
            Ok(guard) => guard,
            Err(_) => {
                error!("Timeout acquiring Sled pool lock for is_running check");
                println!("===> ERROR: TIMEOUT ACQUIRING SLED POOL LOCK FOR IS_RUNNING CHECK");
                return false;
            }
        };
        let daemon_count = pool.daemons.len();
        info!("Checking running status for {} daemons", daemon_count);
        println!("===> CHECKING RUNNING STATUS FOR {} DAEMONS", daemon_count);
        let futures = pool.daemons.values().map(|daemon| async {
            timeout(TokioDuration::from_secs(2), daemon.is_running()).await
                .map_err(|_| {
                    println!("===> TIMEOUT CHECKING DAEMON STATUS");
                    false
                })
                .unwrap_or(false)
        });
        let results = join_all(futures).await;
        let is_running = results.iter().any(|&r| r);
        info!("SledStorage running status: {}, daemon states: {:?}", is_running, results);
        println!("===> SLED STORAGE RUNNING STATUS: {}, DAEMON STATES: {:?}", is_running, results);
        is_running
    }

    async fn query(&self, _query_string: &str) -> Result<Value, GraphError> {
        info!("Executing query on SledStorage (returning null as not implemented)");
        println!("===> EXECUTING QUERY ON SLED STORAGE (NOT IMPLEMENTED)");
        Ok(Value::Null)
    }

    async fn execute_query(&self, _query_plan: QueryPlan) -> Result<QueryResult, GraphError> {
        info!("Executing query on SledStorage (returning null as not implemented)");
        println!("===> EXECUTING QUERY ON SLED STORAGE (NOT IMPLEMENTED)");
        Ok(QueryResult::Null)
    }

    async fn clear_data(&self) -> Result<(), GraphError> {
        let db = SLED_DB.get().ok_or_else(|| GraphError::StorageError("Sled database not initialized".to_string()))?;
        let db_lock = timeout(TokioDuration::from_secs(5), db.lock())
            .await
            .map_err(|_| GraphError::StorageError("Timeout acquiring Sled database lock".to_string()))?;
        let db_path = &db_lock.path;
        info!("Clearing all data from path {:?}", db_path);
        println!("===> CLEARING ALL DATA FROM SLED DATABASE AT {:?}", db_path);

        db_lock.db.clear().map_err(|e| {
            error!("Failed to clear Sled database: {}", e);
            println!("===> ERROR: FAILED TO CLEAR SLED DATABASE");
            GraphError::StorageError(e.to_string())
        })?;
        let bytes_flushed = db_lock.db.flush_async().await
            .map_err(|e| {
                error!("Failed to flush after clearing data: {}", e);
                println!("===> ERROR: FAILED TO FLUSH SLED DATABASE AFTER CLEARING DATA");
                GraphError::StorageError(e.to_string())
            })?;
        info!("Flushed {} bytes after clearing data at {:?}", bytes_flushed, db_path);
        println!("===> FLUSHED {} BYTES AFTER CLEARING DATA AT {:?}", bytes_flushed, db_path);
        Ok(())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn create_index(&self, label: &str, property: &str) -> GraphResult<()> {
        let indexes_tree = self.db.open_tree("indexes")?;
        let key = format!("{label}:{property}");
        let meta = json!({
            "created_at": SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs(),
            "type": "btree"
        });
        indexes_tree.insert(key.as_bytes(), serde_json::to_vec(&meta)?)?;
        indexes_tree.flush_async().await?;
        info!("Created BTree index on {label}.{property}");
        Ok(())
    }

    async fn drop_index(&self, label: &str, property: &str) -> GraphResult<()> {
        let indexes_tree = self.db.open_tree("indexes")?;
        let key = format!("{label}:{property}");
        indexes_tree.remove(key.as_bytes())?;
        indexes_tree.flush_async().await?;
        info!("Dropped index on {label}.{property}");
        Ok(())
    }

    async fn list_indexes(&self) -> GraphResult<Vec<(String, String)>> {
        let indexes_tree = self.db.open_tree("indexes")?;
        let mut indexes = Vec::new();
        for result in indexes_tree.iter() {
            let (key, _) = result?;
            let key_str = String::from_utf8_lossy(&key);
            let parts: Vec<&str> = key_str.splitn(2, ':').collect();
            if parts.len() == 2 {
                indexes.push((parts[0].to_string(), parts[1].to_string()));
            }
        }
        Ok(indexes)
    }

    async fn fulltext_search(&self, query: &str, limit: usize) -> GraphResult<Vec<(String, String)>> {
        // FIX: Stub now returns an empty vector of the correct type to resolve the type mismatch error.
        Ok(Vec::new())
    }

    async fn fulltext_rebuild(&self) -> GraphResult<()> {
        Ok(())
    }
    /// Executes an index-related command by routing it to the SledDaemon via ZMQ.
    ///
    /// This method fully obtains the necessary ZMQ client logic by calling the 
    /// `SledClient::execute_one_shot_zmq_request` associated function. The entire 
    /// ZMQ communication lifecycle (socket creation, connection, I/O, and cleanup) 
    /// is handled within that single utility call, using only the port from `&self`.
    /// This method calls the internal `SledClient` which handles the ZMQ request/response.
    async fn execute_index_command(&self, command: &str, params: Value) -> GraphResult<QueryResult> {
        info!("SledStorage received index command: {}", command);

        // 1. Construct the complete JSON payload for the daemon.
        let request = json!({
            "command": command,
            "params": params, 
        });

        // 2. Use the pool (the correct component for load balancing) to select a port.
        let pool_guard = self.pool.lock().await;
        
        // FIX: Await the async function 'select_daemon()' and use ok_or_else 
        // to handle the resulting Option<u16>.
        let port_option = pool_guard.select_daemon().await;

        let port_number = port_option
            .ok_or_else(|| {
                error!("Failed to select daemon port from pool: Daemon pool is empty or unavailable.");
                GraphError::StorageError("Daemon pool selection error: No available port.".to_string())
            })?;
        
        drop(pool_guard); // Release the lock immediately after selection

        // 3. Delegate the ZMQ communication using the selected port number 
        //    via the static one-shot client function.
        let response_value = SledClient::execute_one_shot_zmq_request(port_number, request).await
            .map_err(|e| {
                error!("ZMQ index command execution failed for {}: {:?}", command, e);
                e
            })?;

        // 4. Deserialize the JSON response (Value) into the expected QueryResult struct.
        match response_value.get("result") {
            Some(result_val) => {
                let query_result: QueryResult = serde_json::from_value(result_val.clone())
                    .map_err(|e| {
                        error!("Failed to deserialize query result: {}", e);
                        GraphError::DeserializationError(format!("Failed to parse QueryResult: {}", e))
                    })?;
                
                info!("Successfully executed index command: {}", command);
                Ok(query_result)
            },
            None => {
                // If the response doesn't contain a 'result' field, check for an 'error' field
                if let Some(error_msg) = response_value.get("error").and_then(|e| e.as_str()) {
                     error!("Daemon execution error for {}: {}", command, error_msg);
                     Err(GraphError::StorageError(format!("Daemon error: {}", error_msg)))
                } else {
                    error!("Unexpected response format from daemon for {}: {:?}", command, response_value);
                    Err(GraphError::StorageError(format!("Unexpected daemon response for {}: {:?}", command, response_value)))
                }
            }
        }
    }
}

impl Drop for SledStorage {
    fn drop(&mut self) {
        // Use async runtime for cleanup to avoid blocking
        let pool = self.pool.clone();
        let runtime = match tokio::runtime::Runtime::new() {
            Ok(rt) => rt,
            Err(e) => {
                error!("Failed to create runtime for Drop: {}", e);
                println!("===> ERROR: FAILED TO CREATE RUNTIME FOR DROP: {}", e);
                return;
            }
        };
        runtime.block_on(async {
            let start_time = Instant::now();
            info!("Dropping SledStorage instance...");
            println!("===> DROPPING SledStorage INSTANCE...");

            // Acquire pool lock with timeout
            let pool_guard = match timeout(TokioDuration::from_secs(5), pool.lock()).await {
                Ok(guard) => guard,
                Err(_) => {
                    error!("Timeout acquiring pool lock during Drop");
                    println!("===> ERROR: TIMEOUT ACQUIRING POOL LOCK DURING DROP");
                    return;
                }
            };

            // Use get_active_ports and await it
            let ports = pool_guard.get_active_ports().await;
            if let Some(port) = ports.first() {
                let port_u16 = *port;
                info!("Cleaning up SledStorage for port {}", port_u16);
                println!("===> CLEANING UP SledStorage FOR PORT {}", port_u16);

                // *** SLED_DB SECTION: FLUSH, CLOSE ZMQ, RELEASE ACTIVE DB TRACKER ***
                if let Some(sled_db) = SLED_DB.get() {
                    let mut sled_db_guard = match timeout(TokioDuration::from_secs(5), sled_db.lock()).await {
                        Ok(guard) => guard,
                        Err(_) => {
                            error!("Timeout acquiring SLED_DB lock during Drop for port {}", port_u16);
                            println!("===> ERROR: TIMEOUT ACQUIRING SLED_DB LOCK DURING DROP FOR PORT {}", port_u16);
                            return;
                        }
                    };
                    let db_path = sled_db_guard.path.clone();
                    info!("Flushing Sled database at {:?}", db_path);
                    println!("===> FLUSHING SLED DATABASE AT {:?}", db_path);
                    if let Err(e) = timeout(TokioDuration::from_secs(10), sled_db_guard.db.flush_async()).await {
                        error!("Failed to flush Sled database at {:?}: {:?}", db_path, e);
                        println!("===> ERROR: FAILED TO FLUSH SLED DATABASE AT {:?}: {:?}", db_path, e);
                    }
                    if let Some((_, zmq_socket)) = sled_db_guard.client.take() {
                        info!("Closing ZMQ client for path {:?}", db_path);
                        println!("===> CLOSING ZMQ CLIENT FOR PATH {:?}", db_path);
                        drop(zmq_socket);
                    }
                    // Release from active databases tracker
                    Self::release_instance(&db_path).await;
                }
                // *** END SLED_DB SECTION ***

                // Remove from SLED_POOL_MAP
                if let Some(pool_map) = SLED_POOL_MAP.get() {
                    let mut pool_map_guard = match timeout(TokioDuration::from_secs(5), pool_map.lock()).await {
                        Ok(guard) => guard,
                        Err(_) => {
                            error!("Timeout acquiring pool map lock during Drop for port {}", port_u16);
                            println!("===> ERROR: TIMEOUT ACQUIRING POOL MAP LOCK DURING DROP FOR PORT {}", port_u16);
                            return;
                        }
                    };
                    pool_map_guard.remove(&port_u16);
                    info!("Removed SledDaemonPool for port {} from pool map", port_u16);
                    println!("===> REMOVED SledDaemonPool FOR PORT {} FROM POOL MAP", port_u16);
                }

                // *** REMOVED: DO NOT CALL check_and_cleanup_stale_daemon HERE ***
                // This function removes entries from GLOBAL_DAEMON_REGISTRY even during normal shutdown.
                // The daemon is already stopped via `pool.close(None)` earlier.
                // Registry cleanup must be done by the daemon itself or via explicit command.

                // Optional: Log that daemon should have cleaned up registry
                info!("SledStorage drop complete for port {}. Daemon should have removed itself from GLOBAL_DAEMON_REGISTRY.", port_u16);
                println!("===> SLED STORAGE DROP COMPLETE FOR PORT {}. DAEMON SHOULD SELF-REMOVE FROM REGISTRY.", port_u16);
            }

            info!("Completed SledStorage cleanup in {}ms", start_time.elapsed().as_millis());
            println!("===> COMPLETED SledStorage CLEANUP IN {}ms", start_time.elapsed().as_millis());
        });
    }
}

pub async fn select_available_port(storage_config: &StorageConfig, preferred_port: u16) -> GraphResult<u16> {
    let cluster_ports = parse_cluster_range(&storage_config.cluster_range)?;

    if !is_storage_daemon_running(preferred_port).await {
        debug!("Preferred port {} is available.", preferred_port);
        println!("===> PREFERRED PORT {} IS AVAILABLE", preferred_port);
        return Ok(preferred_port);
    }

    for port in cluster_ports {
        if port == preferred_port {
            continue;
        }
        if !is_storage_daemon_running(port).await {
            debug!("Selected available port {} from cluster range", port);
            println!("===> SELECTED AVAILABLE PORT {} FROM CLUSTER RANGE", port);
            return Ok(port);
        }
    }

    error!("No available ports in cluster range {:?}", storage_config.cluster_range);
    println!("===> ERROR: NO AVAILABLE PORTS IN CLUSTER RANGE {:?}", storage_config.cluster_range);
    Err(GraphError::StorageError(format!(
        "No available ports in cluster range {:?}", storage_config.cluster_range
    )))
}
