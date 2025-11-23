use std::sync::Arc;
use anyhow::{Result, Context, anyhow, Error as AnyhowError};
use serde_json::Value;
use tokio::sync::{Mutex as TokioMutex, OnceCell};
use sled::Db;
use rocksdb::DB;
use std::fmt;
use std::path::PathBuf;

use crate::config::config_structs::StorageEngineType;
use crate::indexing::tantivy_engine::{
    TantivyIndexingEngine,
    TantivyEngineHandles,
};
use crate::indexing::IndexingBackend;
use crate::indexing::backend::{IndexingError, Document};
use models::{Edge, Identifier, Vertex, };
use models::errors::{GraphError, GraphResult};
use models::identifiers::{SerializableUuid};

pub enum EngineHandles {
    Sled(Arc<Db>),
    RocksDB(Arc<DB>),
    TiKV(String),
    Redis(String),
    None,
}

pub type Service = Arc<TokioMutex<IndexingService>>;

static INDEX_SERVICE_SINGLETON: OnceCell<Service> = OnceCell::const_new();

#[derive(Clone)]
pub struct IndexingService {
    backend: Arc<dyn IndexingBackend>,
}

impl fmt::Debug for IndexingService {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IndexingService")
            .field("backend", &"Arc<dyn IndexingBackend>")
            .finish()
    }
}

impl IndexingService {
    pub fn new(backend: Arc<dyn IndexingBackend>) -> Self {
        Self { backend }
    }

    pub async fn initialize(&self) -> Result<Value> {
        self.backend.initialize().await
            .map(|_| Value::String("Indexing backend initialized successfully".to_string()))
            .map_err(|e: IndexingError| anyhow::anyhow!(e))
    }

    pub async fn create_index(&self, label: &str, property: &str) -> Result<Value> {
        self.backend.create_index(label, property).await
            .map(|s| Value::String(s))
            .map_err(|e: IndexingError| anyhow::anyhow!(e))
    }

    pub async fn drop_index(&self, label: &str, property: &str) -> Result<Value> {
        self.backend.drop_index(label, property).await
            .map(|s| Value::String(s))
            .map_err(|e: IndexingError| anyhow::anyhow!(e))
    }

    pub async fn list_indexes(&self) -> Result<Value> {
        self.backend.index_stats().await
            .map(|s| Value::String(s))
            .map_err(|e: IndexingError| anyhow::anyhow!(e))
    }

    pub async fn create_fulltext_index(
        &self,
        name: &str,
        labels: &[&str],
        properties: &[&str],
    ) -> Result<Value> {
        self.backend.create_fulltext_index(name, labels, properties).await
            .map(|s| Value::String(s))
            .map_err(|e: IndexingError| anyhow::anyhow!(e))
    }

    pub async fn drop_fulltext_index(&self, name: &str) -> Result<Value> {
        self.backend.drop_fulltext_index(name).await
            .map(|s| Value::String(s))
            .map_err(|e: IndexingError| anyhow::anyhow!(e))
    }

    pub async fn fulltext_search(&self, query: &str, limit: usize) -> Result<Value> {
        self.backend.fulltext_search(query, limit).await
            .map(|s| Value::String(s))
            .map_err(|e: IndexingError| anyhow::anyhow!(e))
    }

    pub async fn index_document(&self, doc: Document) -> Result<Value> {
        self.backend.index_document(doc).await
            .map(|_| Value::String("Document indexed successfully".to_string()))
            .map_err(|e: IndexingError| anyhow::anyhow!(e))
    }

    pub async fn delete_document(&self, doc_id: &str) -> Result<Value> {
        self.backend.delete_document(doc_id).await
            .map(|_| Value::String(format!("Document {} deleted successfully", doc_id)))
            .map_err(|e: IndexingError| anyhow::anyhow!(e))
    }

    pub async fn search(&self, query: &str) -> Result<Vec<Document>> {
        self.backend.search(query).await
            .map_err(|e: IndexingError| anyhow::anyhow!(e))
    }

    pub async fn rebuild_indexes(&self) -> Result<Value> {
        println!("In rebuild indexes - service.rs");
        self.backend.rebuild_indexes().await
            .map(|s| Value::String(s))
            .map_err(|e: IndexingError| anyhow::anyhow!(e))
    }

    pub async fn rebuild_indexes_with_data(&self, all_vertices: Vec<Vertex>) -> Result<Value> {
        println!("In rebuild indexes with data - service.rs");
        self.backend.rebuild_indexes_with_data(all_vertices).await
            .map(|s| Value::String(s))
            .map_err(|e: IndexingError| anyhow::anyhow!(e))
    }

    pub async fn index_stats(&self) -> Result<Value> {
        self.backend.index_stats().await
            .map(|s| Value::String(s))
            .map_err(|e: IndexingError| anyhow::anyhow!(e))
    }
}

/// Construct the persistent storage path for the indexing engine
/// Format: /opt/graphdb/storage_data/<engine_type>/<port>
fn construct_storage_path(engine_type: StorageEngineType, port: u16) -> PathBuf {
    let engine_name = match engine_type {
        StorageEngineType::Sled => "sled",
        StorageEngineType::RocksDB => "rocksdb",
        StorageEngineType::TiKV => "tikv",
        StorageEngineType::Redis => "redis",
        _ => "unknown",
    };
    
    PathBuf::from("/opt/graphdb/storage_data")
        .join(engine_name)
        .join(port.to_string())
}

pub async fn init_indexing_service(
    engine_type: StorageEngineType,
    handles: EngineHandles,
    port: u16, // Add port parameter
) -> Result<Service> {
    let service = INDEX_SERVICE_SINGLETON.get_or_try_init(|| async move {
        let backend_handles = match handles {
            EngineHandles::Sled(h) => TantivyEngineHandles::Sled(h),
            EngineHandles::RocksDB(h) => TantivyEngineHandles::RocksDB(h),
            EngineHandles::TiKV(s) => TantivyEngineHandles::TiKV(s),
            EngineHandles::Redis(s) => TantivyEngineHandles::Redis(s),
            EngineHandles::None => TantivyEngineHandles::None,
        };

        // Construct persistent storage path
        let storage_path = construct_storage_path(engine_type, port);
        println!("===========> Initializing indexing service with storage path: {:?}", storage_path);

        let backend = TantivyIndexingEngine::new(engine_type, backend_handles, storage_path)
            .context("Failed to initialize TantivyIndexingEngine")?;

        backend.initialize().await
            .context("Failed to perform initial setup on the indexing backend")?;

        Ok::<Service, AnyhowError>(Arc::new(TokioMutex::new(IndexingService::new(Arc::new(backend)))))
    })
    .await
    .cloned()
    .map_err(|e| anyhow!("Indexing service initialization failed: {}", e))?;

    Ok(service)
}

pub fn indexing_service() -> Service {
    INDEX_SERVICE_SINGLETON
        .get()
        .expect("IndexingService not initialised – call init_indexing_service first")
        .clone()
}