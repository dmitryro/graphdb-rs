// lib/src/storage_engine/hybrid_storage.rs
// Updated: 2025-09-09 - Fixed E0308 and E0560 by correcting StorageConfig initialization:
// - Wrapped PathBuf fields in Some for data_directory and config_root_directory.
// - Removed non-existent connection_string field.
// - Converted log_directory to Option<PathBuf>.
// - Provided a u64 value for max_open_files.

use async_trait::async_trait;
use std::collections::{ HashSet };
use std::any::Any;
use std::path::PathBuf;
use std::sync::Arc;
use serde_json::Value;
use uuid::Uuid;
use tokio::sync::Mutex as TokioMutex;
use chrono::Utc;
use log::{info, debug, error, warn};
use models::errors::{GraphError, GraphResult};
use crate::storage_engine::{GraphStorageEngine, StorageEngine};
use crate::storage_engine::{ GraphOp };
use crate::storage_engine::inmemory_storage::InMemoryStorage;
use crate::config::config::{StorageConfig, StorageEngineType, QueryResult, QueryPlan,};
use models::{ Graph, Edge, Identifier, Vertex};


#[derive(Debug)]
pub struct HybridStorage {
    pub inmemory: Arc<InMemoryStorage>,
    pub persistent: Arc<dyn GraphStorageEngine + Send + Sync>,
    pub running: Arc<TokioMutex<bool>>,
    pub engine_type: StorageEngineType,
}

impl HybridStorage {
    pub fn new(persistent: Arc<dyn GraphStorageEngine + Send + Sync>) -> Self {
        let config = StorageConfig {
            storage_engine_type: StorageEngineType::InMemory,
            data_directory: Some(PathBuf::from("/tmp/graphdb_inmemory")), // Fixed: Wrapped in Some
            config_root_directory: Some(PathBuf::from("/tmp/graphdb_config")), // Fixed: Wrapped in Some
            log_directory: Some(PathBuf::from("/tmp/graphdb_logs")), // Fixed: Converted to PathBuf and wrapped in Some
            default_port: 0,
            cluster_range: String::new(),
            max_disk_space_gb: 100,
            min_disk_space_gb: 1,
            use_raft_for_scale: false,
            engine_specific_config: None,
            max_open_files: 1000, // Fixed: Provided u64 value
        };
        HybridStorage {
            inmemory: Arc::new(InMemoryStorage::new(&config)),
            persistent,
            running: Arc::new(TokioMutex::new(false)),
            engine_type: StorageEngineType::Hybrid,
        }
    }
}

#[async_trait]
impl StorageEngine for HybridStorage {
    async fn insert(&self, key: Vec<u8>, value: Vec<u8>) -> Result<(), GraphError> {
        self.inmemory.insert(key.clone(), value.clone()).await?;
        self.persistent.insert(key, value).await?;
        Ok(())
    }

    async fn retrieve(&self, key: &Vec<u8>) -> Result<Option<Vec<u8>>, GraphError> {
        if let Some(value) = self.inmemory.retrieve(key).await? {
            Ok(Some(value))
        } else {
            let value = self.persistent.retrieve(key).await?;
            if let Some(v) = &value {
                self.inmemory.insert(key.to_vec(), v.clone()).await?;
            }
            Ok(value)
        }
    }

    async fn delete(&self, key: &Vec<u8>) -> Result<(), GraphError> {
        self.inmemory.delete(key).await?;
        self.persistent.delete(key).await?;
        Ok(())
    }

    async fn connect(&self) -> Result<(), GraphError> {
        self.inmemory.connect().await?;
        self.persistent.connect().await?;
        Ok(())
    }

    async fn flush(&self) -> Result<(), GraphError> {
        self.inmemory.flush().await?;
        self.persistent.flush().await?;
        Ok(())
    }
    // ------------------------------------------------------------------ //
    // append: mirror what SledStorage does, but write to **both** layers //
    // ------------------------------------------------------------------ //
    async fn append(&self, op: GraphOp) -> Result<(), GraphError> {
        let op_bytes = serde_json::to_vec(&op)
            .map_err(|e| GraphError::StorageError(format!("JSON encode failed: {}", e)))?;

        let timestamp = Utc::now().timestamp_nanos_opt()
            .ok_or_else(|| GraphError::StorageError("Failed to get timestamp".into()))?;
        let key = format!("wal_{:0>20}", timestamp);

        // 1.  in-memory layer
        self.inmemory.insert(key.as_bytes().to_vec(), op_bytes.clone()).await?;

        // 2.  persistent layer
        self.persistent.insert(key.as_bytes().to_vec(), op_bytes).await?;

        Ok(())
    }

    // ------------------------------------------------------------------ //
    // replay_into: delegate to the **persistent** layer only             //
    // ------------------------------------------------------------------ //
    async fn replay_into(&self, graph: &mut Graph) -> Result<(), GraphError> {
        self.persistent.replay_into(graph).await
    }
}

#[async_trait]
impl GraphStorageEngine for HybridStorage {
    async fn delete_edges_touching_vertices(&self, vertex_ids: &HashSet<Uuid>) -> GraphResult<usize> {
        // TODO: implement it.
        Ok(0)
    }

    async fn cleanup_orphaned_edges(&self) -> GraphResult<usize> {
        // TODO: implement it.
        Ok(0)
    }

    fn get_type(&self) -> &'static str {
        "Hybrid"
    }

    async fn start(&self) -> Result<(), GraphError> {
        self.inmemory.start().await?;
        self.persistent.start().await?;
        let mut running = self.running.lock().await;
        *running = true;
        Ok(())
    }

    async fn stop(&self) -> Result<(), GraphError> {
        self.inmemory.stop().await?;
        self.persistent.stop().await?;
        let mut running = self.running.lock().await;
        *running = false;
        Ok(())
    }

    async fn is_running(&self) -> bool {
        *self.running.lock().await && self.inmemory.is_running().await && self.persistent.is_running().await
    }

    async fn query(&self, query_string: &str) -> Result<Value, GraphError> {
        self.persistent.query(query_string).await
    }

    async fn execute_query(&self, query_plan: QueryPlan) -> Result<QueryResult, GraphError> {
        info!("Executing query on HybridStorage (delegating to persistent layer)");
        self.persistent.execute_query(query_plan).await
    }

    async fn create_vertex(&self, vertex: Vertex) -> Result<(), GraphError> {
        self.inmemory.create_vertex(vertex.clone()).await?;
        self.persistent.create_vertex(vertex).await?;
        Ok(())
    }

    async fn get_vertex(&self, id: &Uuid) -> Result<Option<Vertex>, GraphError> {
        if let Some(vertex) = self.inmemory.get_vertex(id).await? {
            Ok(Some(vertex))
        } else {
            let vertex = self.persistent.get_vertex(id).await?;
            if let Some(v) = &vertex {
                let _ = self.inmemory.create_vertex(v.clone()).await; // Cache miss → warm up
            }
            Ok(vertex)
        }
    }

    async fn update_vertex(&self, vertex: Vertex) -> Result<(), GraphError> {
        self.inmemory.update_vertex(vertex.clone()).await?;
        self.persistent.update_vertex(vertex).await?;
        Ok(())
    }

    async fn delete_vertex(&self, id: &Uuid) -> Result<(), GraphError> {
        self.inmemory.delete_vertex(id).await?;
        self.persistent.delete_vertex(id).await?;
        Ok(())
    }

    async fn get_all_vertices(&self) -> Result<Vec<Vertex>, GraphError> {
        let vertices = self.persistent.get_all_vertices().await?;
        for vertex in &vertices {
            let _ = self.inmemory.create_vertex(vertex.clone()).await; // Warm cache
        }
        Ok(vertices)
    }

    async fn create_edge(&self, edge: Edge) -> Result<(), GraphError> {
        self.inmemory.create_edge(edge.clone()).await?;
        self.persistent.create_edge(edge).await?;
        Ok(())
    }

    async fn get_edge(&self, outbound_id: &Uuid, edge_type: &Identifier, inbound_id: &Uuid) -> Result<Option<Edge>, GraphError> {
        if let Some(edge) = self.inmemory.get_edge(outbound_id, edge_type, inbound_id).await? {
            Ok(Some(edge))
        } else {
            let edge = self.persistent.get_edge(outbound_id, edge_type, inbound_id).await?;
            if let Some(e) = &edge {
                let _ = self.inmemory.create_edge(e.clone()).await;
            }
            Ok(edge)
        }
    }

    async fn update_edge(&self, edge: Edge) -> Result<(), GraphError> {
        self.inmemory.update_edge(edge.clone()).await?;
        self.persistent.update_edge(edge).await?;
        Ok(())
    }

    async fn delete_edge(&self, outbound_id: &Uuid, edge_type: &Identifier, inbound_id: &Uuid) -> Result<(), GraphError> {
        self.inmemory.delete_edge(outbound_id, edge_type, inbound_id).await?;
        self.persistent.delete_edge(outbound_id, edge_type, inbound_id).await?;
        Ok(())
    }

    async fn get_all_edges(&self) -> Result<Vec<Edge>, GraphError> {
        let edges = self.persistent.get_all_edges().await?;
        for edge in &edges {
            let _ = self.inmemory.create_edge(edge.clone()).await;
        }
        Ok(edges)
    }

    async fn clear_data(&self) -> Result<(), GraphError> {
        self.inmemory.clear_data().await?;
        self.persistent.clear_data().await?;
        Ok(())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn close(&self) -> Result<(), GraphError> {
        self.inmemory.close().await?;
        self.persistent.close().await?;
        let mut running = self.running.lock().await;
        *running = false;
        Ok(())
    }

    // === INDEX METHODS (delegate to persistent layer) ===
    async fn create_index(&self, label: &str, property: &str) -> GraphResult<()> {
        self.persistent.create_index(label, property).await
    }

    async fn drop_index(&self, label: &str, property: &str) -> GraphResult<()> {
        self.persistent.drop_index(label, property).await
    }

    async fn list_indexes(&self) -> GraphResult<Vec<(String, String)>> {
        self.persistent.list_indexes().await
    }

    // === FULLTEXT SEARCH (Tantivy via persistent layer) ===
    async fn fulltext_search(&self, query: &str, limit: usize) -> GraphResult<Vec<(String, String)>> {
        self.persistent.fulltext_search(query, limit).await
    }

    async fn fulltext_rebuild(&self) -> GraphResult<()> {
        self.persistent.fulltext_rebuild().await
    }

    // === REQUIRED INDEX COMMAND DELEGATION (Fix for E0046) ===
    async fn execute_index_command(&self, command: &str, params: Value) -> GraphResult<QueryResult> {
        info!("Executing index command '{}' on HybridStorage (delegating to persistent layer)", command);
        self.persistent.execute_index_command(command, params).await
    }
}