
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};
use std::path::{ PathBuf, Path };
use anyhow::{Result, Context, bail};
use serde_json::{json, Value as JsonValue};
use serde::{Deserialize, Serialize};
use async_trait::async_trait;
use tokio::sync::{ Mutex as TokioMutex, OnceCell };
use sled::Db;
use rocksdb::DB;
use log::{info, debug, warn, error, trace};
use regex::Regex;
use uuid::Uuid;

pub type IndexResult<T> = Result<T, crate::indexing::backend::IndexingError>;
use crate::indexing::backend::{IndexingBackend, Document, IndexingError};
use crate::config::config_structs::StorageEngineType;
use crate::daemon::daemon_registry::{GLOBAL_DAEMON_REGISTRY};
use crate::storage_engine::storage_engine::{ GraphStorageEngine, get_global_storage_registry};
use crate::storage_engine::storage_utils::{create_edge_key, deserialize_edge, deserialize_vertex, serialize_edge, serialize_vertex};
use models::{Edge, Identifier, Vertex, };
use models::errors::{GraphError, GraphResult};
use models::identifiers::{SerializableUuid};
use tantivy::collector::TopDocs;
use tantivy::query::QueryParser;
use tantivy::schema::{Field, Schema, INDEXED, STORED, TEXT, Term, Value};
use tantivy::{Index, IndexWriter, Searcher, TantivyDocument};
use tantivy::directory::MmapDirectory;
use zmq::{Context as ZmqContext, Socket, REQ};


// one regex for every operator (with optional spaces)
static FILTER_RE: once_cell::sync::Lazy<Regex> = once_cell::sync::Lazy::new(|| {
    Regex::new(r"(?P<field>\w+)\s*(?P<op>>=|<=|=>|=<|==|===|!=|!==|>|<)\s*(?P<val>-?\d+(?:\.\d+)?)")
        .unwrap()
});

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphDocument {
    pub node_id: i64,
    pub properties: HashMap<String, JsonValue>,
    pub labels: Vec<String>,
}

pub enum TantivyEngineHandles {
    Sled(Arc<sled::Db>),
    RocksDB(Arc<DB>),
    TiKV(String),
    Redis(String),
    None,
}

struct TantivyManager {
    index: Index,
    schema: Schema,
    writer: Arc<TokioMutex<IndexWriter>>,
    searcher: Arc<RwLock<Searcher>>,
    node_id_field: Field,
    fulltext_content_field: Field,
}

impl TantivyManager {
    pub fn new(storage_path: &Path) -> Result<Self> {
        let mut schema_builder = Schema::builder();
        
        let node_id_field = schema_builder.add_i64_field("node_id", INDEXED | STORED);
        let fulltext_content_field = schema_builder.add_text_field("fulltext_content", TEXT | STORED);
        
        let schema = schema_builder.build();
        
        let index_path = storage_path.join("tantivy_index");
        println!("===========> Creating Tantivy index at: {:?}", index_path);
        
        std::fs::create_dir_all(&index_path)
            .context(format!("Failed to create index directory at {:?}", index_path))?;
        
        let directory = MmapDirectory::open(&index_path)
            .context(format!("Failed to open MmapDirectory at {:?}", index_path))?;
        
        let index = Index::open_or_create(directory, schema.clone())
            .context("Failed to create or open Tantivy index")?;
        
        let writer = index.writer(50_000_000)
            .context("Failed to create Tantivy IndexWriter")?;
        
        let reader = index.reader()
            .context("Failed to create Tantivy IndexReader")?;
        let searcher = reader.searcher();
        
        Ok(TantivyManager {
            index,
            schema,
            writer: Arc::new(TokioMutex::new(writer)),
            searcher: Arc::new(RwLock::new(searcher)),
            node_id_field,
            fulltext_content_field,
        })
    }

    pub async fn commit(&self) -> Result<()> {
        let mut writer = self.writer.lock().await;
        writer.commit().context("Tantivy commit failed")?;
        drop(writer);
        self.reload_searcher()
    }

    pub fn reload_searcher(&self) -> Result<()> {
        let reader = self.index.reader()
            .context("Failed to create reader for reload")?;
        *self.searcher.write().unwrap() = reader.searcher();
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
enum IndexType {
    Standard,
    FullText,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct IndexMetadata {
    index_type: IndexType,
    tantivy_index_name: String,
    labels: Vec<String>,
    properties: Vec<String>,
}

pub struct TantivyIndexingEngine {
    engine_type: StorageEngineType,
    manager: Arc<TantivyManager>,
    handles: TantivyEngineHandles,
    metadata_id: String,
}

impl TantivyIndexingEngine {
    pub fn new(engine_type: StorageEngineType, handles: TantivyEngineHandles, storage_path: PathBuf) -> Result<Self> {
        let manager = Arc::new(TantivyManager::new(&storage_path)?);
        let metadata_id = format!("{:?}", engine_type).to_lowercase();
        Ok(Self {
            engine_type,
            manager,
            handles,
            metadata_id,
        })
    }

    fn store_metadata(&self, key: &str, metadata: &IndexMetadata) -> Result<()> {
        println!("===========> Storing metadata for key: {}", key);
        let data = serde_json::to_vec(metadata).context("Failed to serialize metadata")?;
        println!("===========> Serialized metadata: {} bytes", data.len());

        match &self.handles {
            TantivyEngineHandles::Sled(db) => {
                let tree_name = format!("index_metadata_{}", self.metadata_id);
                println!("===========> Opening Sled tree: {}", tree_name);
                let tree = db.open_tree(&tree_name)?;
                tree.insert(key.as_bytes(), data)?;
                tree.flush()?;
                println!("===========> Metadata stored and flushed to Sled for key: {}", key);
            },
            TantivyEngineHandles::RocksDB(db) => {
                let prefixed_key = format!("{}:{}", self.metadata_id, key);
                println!("===========> Storing to RocksDB with key: {}", prefixed_key);
                db.put(prefixed_key.as_bytes(), data).context("RocksDB put failed")?;
                db.flush().context("RocksDB flush failed")?;
                println!("===========> Metadata stored and flushed to RocksDB for key: {}", prefixed_key);
            },
            _ => bail!("Metadata storage not implemented for {:?}", self.engine_type),
        }
        Ok(())
    }

    fn retrieve_metadata(&self, key: &str) -> Result<Option<IndexMetadata>> {
        let result = match &self.handles {
            TantivyEngineHandles::Sled(db) => {
                let tree_name = format!("index_metadata_{}", self.metadata_id);
                db.open_tree(tree_name)?.get(key.as_bytes())?.map(|ivec| ivec.to_vec())
            },
            TantivyEngineHandles::RocksDB(db) => {
                let prefixed_key = format!("{}:{}", self.metadata_id, key);
                db.get(prefixed_key.as_bytes()).context("RocksDB get failed")?
            },
            _ => return Ok(None),
        };

        if let Some(data) = result {
            let metadata: IndexMetadata = serde_json::from_slice(&data).context("Failed to deserialize metadata")?;
            Ok(Some(metadata))
        } else {
            Ok(None)
        }
    }

    fn retrieve_all_metadata(&self, index_type: IndexType) -> Result<Vec<IndexMetadata>> {
        let mut results = Vec::new();
        
        match &self.handles {
            TantivyEngineHandles::Sled(db) => {
                let tree_name = format!("index_metadata_{}", self.metadata_id);
                println!("===========> Retrieving from Sled tree: {}", tree_name);
                
                let tree = db.open_tree(&tree_name)?;
                println!("===========> Sled tree has {} entries", tree.len());
                
                for item in tree.iter() {
                    let (key, data) = item?;
                    println!("===========> Found Sled key: {:?}", String::from_utf8_lossy(&key));
                    let metadata: IndexMetadata = serde_json::from_slice(&data)
                        .context("Failed to deserialize metadata")?;
                    println!("===========> Sled metadata: labels={:?}, props={:?}, type={:?}", 
                        metadata.labels, metadata.properties, metadata.index_type);
                    if metadata.index_type == index_type {
                        results.push(metadata);
                    }
                }
                println!("===========> Total fulltext indexes found in Sled: {}", results.len());
            },
            TantivyEngineHandles::RocksDB(db) => {
                let prefix = format!("{}:", self.metadata_id);
                println!("===========> Retrieving from RocksDB with prefix: {}", prefix);
                
                let iter = db.prefix_iterator(prefix.as_bytes());
                let mut count = 0;
                
                for item in iter {
                    let (key, data) = item?;
                    let key_str = String::from_utf8_lossy(&key);
                    println!("===========> Found RocksDB key: {}", key_str);
                    
                    if !key_str.starts_with(&prefix) {
                        break;
                    }
                    
                    let metadata: IndexMetadata = serde_json::from_slice(&data)
                        .context("Failed to deserialize metadata")?;
                    println!("===========> RocksDB metadata: labels={:?}, props={:?}, type={:?}", 
                        metadata.labels, metadata.properties, metadata.index_type);
                    
                    if metadata.index_type == index_type {
                        results.push(metadata);
                    }
                    count += 1;
                }
                println!("===========> Scanned {} RocksDB entries, found {} fulltext indexes", count, results.len());
            },
            _ => {
                println!("===========> No metadata storage for engine type: {:?}", self.engine_type);
            }
        }
        
        Ok(results)
    }

    fn delete_metadata(&self, key: &str) -> Result<()> {
        match &self.handles {
            TantivyEngineHandles::Sled(db) => {
                let tree_name = format!("index_metadata_{}", self.metadata_id);
                db.open_tree(tree_name)?.remove(key.as_bytes())?;
            },
            TantivyEngineHandles::RocksDB(db) => {
                let prefixed_key = format!("{}:{}", self.metadata_id, key);
                db.delete(prefixed_key.as_bytes()).context("RocksDB delete failed")?;
            },
            _ => {}
        }
        Ok(())
    }

    async fn commit_and_reload_internal(&self) -> Result<()> {
        let mut writer = self.manager.writer.lock().await;
        writer.commit().context("Tantivy commit failed")?;
        drop(writer);
        self.manager.reload_searcher()?;
        Ok(())
    }

    fn convert_document_to_graph_doc(&self, doc: Document) -> Result<GraphDocument, IndexingError> {
        let serialized = serde_json::to_string(&doc)
            .map_err(|e| IndexingError::SerializationError(format!("Failed to serialize Document: {}", e)))?;
        let graph_doc: GraphDocument = serde_json::from_str(&serialized)
            .map_err(|e| IndexingError::SerializationError(format!("Failed to deserialize into GraphDocument: {}", e)))?;
        Ok(graph_doc)
    }
}

#[async_trait]
impl IndexingBackend for TantivyIndexingEngine {
    fn engine_type(&self) -> StorageEngineType {
        self.engine_type
    }

    async fn initialize(&self) -> IndexResult<()> {
        Ok(())
    }

    async fn index_document(&self, doc: Document) -> IndexResult<()> {
        let graph_doc = self.convert_document_to_graph_doc(doc)?;
        let mut writer = self.manager.writer.lock().await;

        let term = Term::from_field_i64(self.manager.node_id_field, graph_doc.node_id);
        writer.delete_term(term);

        let fulltext_indexes = self.retrieve_all_metadata(IndexType::FullText)
            .map_err(|e| IndexingError::Other(e.to_string()))?;
        let mut content = String::new();
        let doc_labels: HashSet<_> = graph_doc.labels.iter().map(|s| s.as_str()).collect();

        for meta in fulltext_indexes {
            let meta_labels: HashSet<&str> = meta.labels.iter().map(|s| s.as_str()).collect();
            let matches = meta.labels.is_empty() || doc_labels.intersection(&meta_labels).next().is_some();
            if matches {
                for prop in &meta.properties {
                    if let Some(val) = graph_doc.properties.get(prop) {
                        if let Some(s) = val.as_str() {
                            content.push_str(s);
                            content.push(' ');
                        }
                    }
                }
            }
        }

        if !content.is_empty() {
            let mut tantivy_doc = TantivyDocument::new();
            tantivy_doc.add_i64(self.manager.node_id_field, graph_doc.node_id);
            tantivy_doc.add_text(self.manager.fulltext_content_field, content.trim());
            writer.add_document(tantivy_doc)?;
        }

       self.manager.commit().await
        .map_err(|e| IndexingError::Other(e.to_string()))?;
        Ok(())
    }

    async fn delete_document(&self, doc_id: &str) -> IndexResult<()> {
        let node_id: i64 = doc_id.parse()
            .map_err(|_| IndexingError::Other(format!("Invalid document ID: {}", doc_id)))?;

        let mut writer = self.manager.writer.lock().await;
        let term = Term::from_field_i64(self.manager.node_id_field, node_id);
        writer.delete_term(term);
        Ok(())
    }

    async fn search(&self, query: &str) -> IndexResult<Vec<Document>> {
        self.manager.commit().await
            .map_err(|e| IndexingError::Other(e.to_string()))?;

        let searcher = self.manager.searcher.read().unwrap();
        let query_parser = QueryParser::for_index(&self.manager.index, vec![self.manager.fulltext_content_field]);
        let tantivy_query = query_parser.parse_query(query)
            .map_err(|e| IndexingError::Other(format!("Query parse error: {}", e)))?;

        let top_docs = searcher.search(&tantivy_query, &TopDocs::with_limit(100))?;

        let results: Vec<Document> = top_docs.into_iter()
            .filter_map(|(_score, doc_address)| {
                let retrieved_doc: TantivyDocument = searcher.doc(doc_address).ok()?;
                let node_id = retrieved_doc.get_first(self.manager.node_id_field)?
                    .as_i64()?;
                Some(Document {
                    id: node_id.to_string(),
                    fields: HashMap::new(),
                })
            })
            .collect();

        Ok(results)
    }

    async fn create_index(&self, label: &str, property: &str) -> IndexResult<String> {
        let key = format!("{}:{}", label, property);
        let meta = IndexMetadata {
            index_type: IndexType::Standard,
            tantivy_index_name: key.clone(),
            labels: vec![label.to_string()],
            properties: vec![property.to_string()],
        };
        self.store_metadata(&key, &meta)
            .map_err(|e| IndexingError::Other(e.to_string()))?;
        Ok(format!("Standard index created: {}:{}", label, property))
    }

    async fn drop_index(&self, label: &str, property: &str) -> IndexResult<String> {
        let key = format!("{}:{}", label, property);
        self.delete_metadata(&key)
            .map_err(|e| IndexingError::Other(e.to_string()))?;
        Ok(format!("Standard index dropped: {}:{}", label, property))
    }

    async fn create_fulltext_index(&self, name: &str, labels: &[&str], properties: &[&str]) -> IndexResult<String> {
        let meta = IndexMetadata {
            index_type: IndexType::FullText,
            tantivy_index_name: name.to_string(),
            labels: labels.iter().map(|s| s.to_string()).collect(),
            properties: properties.iter().map(|s| s.to_string()).collect(),
        };
        self.store_metadata(name, &meta)
            .map_err(|e| IndexingError::Other(e.to_string()))?;
        Ok(format!("Full-text index created: {}", name))
    }

    async fn drop_fulltext_index(&self, name: &str) -> IndexResult<String> {
        self.delete_metadata(name)
            .map_err(|e| IndexingError::Other(e.to_string()))?;
        Ok(format!("Full-text index dropped: {}", name))
    }

    async fn fulltext_search(&self, query: &str, limit: usize) -> IndexResult<String> {
        self.manager.commit().await
            .map_err(|e| IndexingError::Other(e.to_string()))?;

        // ---------- 1. Tantivy search — get matching node_ids ----------
        let top_docs_with_ids: Vec<(f32, i64)> = {
            let searcher = self.manager.searcher.read().unwrap();
            let query_parser = QueryParser::for_index(&self.manager.index, vec![self.manager.fulltext_content_field]);
            let tantivy_query = query_parser.parse_query(query)
                .map_err(|e| IndexingError::Other(format!("Query parse error: {}", e)))?;
            let top_docs = searcher.search(&tantivy_query, &TopDocs::with_limit(limit))?;
            println!("===========> Found {} matching documents", top_docs.len());

            let mut results = Vec::new();
            for (score, doc_address) in top_docs {
                let retrieved_doc: tantivy::TantivyDocument = searcher.doc(doc_address)
                    .map_err(|e| IndexingError::TantivyError(e))?;

                if let Some(node_id) = retrieved_doc.get_first(self.manager.node_id_field)
                    .and_then(|v| v.as_i64()) {
                    println!("===========> Found node_id in index: {}", node_id);
                    results.push((score, node_id));
                }
            }
            results
        };

        // ---------- 2. INSTANT EMPTY RESULT IF NO HITS ----------
        if top_docs_with_ids.is_empty() {
            println!("===========> No indexed matches — returning empty result instantly");
            return Ok(json!({
                "status": "success",
                "results": [],
                "query": query,
                "limit": limit,
            }).to_string());
        }

        // ---------- 3. Fetch ONLY needed vertices using DIRECT DB ACCESS ----------
        println!("===========> Fetching {} vertices using DIRECT DB access to avoid deadlock", top_docs_with_ids.len());
        
        let mut vertex_map: HashMap<i64, JsonValue> = HashMap::new();

        match &self.handles {
            TantivyEngineHandles::Sled(db) => {
                let vertices_tree = db.open_tree("vertices")
                    .map_err(|e| IndexingError::Other(format!("Failed to open vertices tree: {}", e)))?;
                
                println!("===========> Scanning {} total vertices in Sled", vertices_tree.len());
                
                // SCAN ALL VERTICES and hash their UUIDs to match indexed i64 values
                for item in vertices_tree.iter() {
                    if let Ok((key, value)) = item {
                        if let Ok(vertex) = deserialize_vertex(&value) {
                            // Hash the UUID to get the same i64 used during indexing
                            let hashed_id = vertex.id.0.as_u128() as i64;
                            
                            // Check if this vertex matches any of our search hits
                            if top_docs_with_ids.iter().any(|(_, node_id)| *node_id == hashed_id) {
                                println!("===========> Matched vertex UUID {} -> hashed {}", vertex.id.0, hashed_id);
                                if let Ok(json_val) = serde_json::to_value(&vertex) {
                                    vertex_map.insert(hashed_id, json_val);
                                }
                            }
                        }
                    }
                }
            },
            TantivyEngineHandles::RocksDB(db) => {
                let vertices_cf = db.cf_handle("vertices")
                    .ok_or_else(|| IndexingError::Other("vertices column family not found".into()))?;
                
                // SCAN ALL VERTICES
                let iter = db.iterator_cf(&vertices_cf, rocksdb::IteratorMode::Start);
                
                for item in iter {
                    if let Ok((key, value)) = item {
                        if let Ok(vertex) = deserialize_vertex(&value) {
                            // Hash the UUID to get the same i64 used during indexing
                            let hashed_id = vertex.id.0.as_u128() as i64;
                            
                            // Check if this vertex matches any of our search hits
                            if top_docs_with_ids.iter().any(|(_, node_id)| *node_id == hashed_id) {
                                println!("===========> Matched vertex UUID {} -> hashed {}", vertex.id.0, hashed_id);
                                if let Ok(json_val) = serde_json::to_value(&vertex) {
                                    vertex_map.insert(hashed_id, json_val);
                                }
                            }
                        }
                    }
                }
            },
            _ => {
                return Err(IndexingError::Other("Unsupported storage engine for direct access".into()));
            }
        }

        println!("===========> Fetched {} needed vertices using DIRECT DB access", vertex_map.len());

        // ---------- 4. Parse filters from query ----------
        let mut filters: Vec<(String, String, f64)> = Vec::new();

        for m in FILTER_RE.captures_iter(query) {
            let field = m.name("field").unwrap().as_str().to_string();
            let op = m.name("op").unwrap().as_str()
                .replace("=<", "<=").replace("=>", ">=");
            let val: f64 = m.name("val").unwrap().as_str().parse()
                .map_err(|_| IndexingError::Other("invalid numeric filter".into()))?;
            filters.push((field, op, val));
        }

        // ---------- 5. Apply filters and build final results ----------
        let mut results: Vec<JsonValue> = Vec::new();

        for (score, node_id) in top_docs_with_ids {
            if let Some(vertex) = vertex_map.get(&node_id) {
                let mut passes = true;

                if let Some(props) = vertex.get("properties").and_then(|p| p.as_object()) {
                    for (field, op, threshold) in &filters {
                        let num_val = props.get(field)
                            .and_then(|v| v.as_f64().or_else(|| v.as_i64().map(|i| i as f64)));

                        if let Some(val) = num_val {
                            passes &= match op.as_str() {
                                ">"  => val >  *threshold,
                                ">=" => val >= *threshold,
                                "<"  => val <  *threshold,
                                "<=" => val <= *threshold,
                                "==" | "===" => (val - threshold).abs() < f64::EPSILON,
                                "!=" | "!==" => (val - threshold).abs() >= f64::EPSILON,
                                _ => true,
                            };
                        } else {
                            passes = false;
                            break;
                        }
                    }
                }

                if passes {
                    results.push(json!({ "score": score, "vertex": vertex }));
                }
            }
        }

        println!("===========> Returning {} enriched results after filtering", results.len());

        Ok(json!({
            "status": "success",
            "results": results,
            "query": query,
            "limit": limit,
        }).to_string())
    }

    // Add a new method that takes the data directly
    async fn rebuild_indexes_with_data(&self, all_vertices: Vec<Vertex>) -> IndexResult<String> {
        info!("Starting full engine-agnostic rebuild of full-text indexes");
        println!("===========> Starting full engine-agnostic rebuild with {} vertices", all_vertices.len());
        let vertex_count = all_vertices.len();

        // 1.  wipe existing index
        {
            let mut writer = self.manager.writer.lock().await;
            writer.delete_all_documents()
                .map_err(IndexingError::TantivyError)?;
            writer.commit()
                .map_err(IndexingError::TantivyError)?;
        }

        // 2.  load full-text definitions
        let fulltext_indexes = self
            .retrieve_all_metadata(IndexType::FullText)
            .map_err(|e| IndexingError::Other(e.to_string()))?;
        println!("===========> Found {} fulltext indexes", fulltext_indexes.len());

        // 3.  re-index every supplied vertex
        let mut writer = self.manager.writer.lock().await;

        for vertex in all_vertices {
            let node_id        = vertex.id.0.as_u128() as i64;
            let vertex_label   = &vertex.label;
            let mut content    = String::new();

            for meta in &fulltext_indexes {
                let matches = meta.labels.is_empty()
                    || meta.labels.iter().any(|l| AsRef::<str>::as_ref(l) == vertex_label.as_ref());

                if matches {
                    for prop in &meta.properties {
                        if let Some(s) = vertex.properties.get(prop)
                                                  .and_then(|v| v.as_str()) {
                            content.push_str(s);
                            content.push(' ');
                        }
                    }
                }
            }

            if !content.is_empty() {
                let mut doc = TantivyDocument::new();
                doc.add_i64(self.manager.node_id_field, node_id);
                doc.add_text(self.manager.fulltext_content_field, content.trim());
                writer.add_document(doc)
                    .map_err(IndexingError::TantivyError)?;
            }
        }

        writer.commit()
            .map_err(IndexingError::TantivyError)?;
        drop(writer);

        self.manager.reload_searcher()
            .map_err(|e| IndexingError::Other(e.to_string()))?;

        Ok(format!("Full-text index rebuilt: {} documents indexed", vertex_count))
    }

    async fn rebuild_indexes(&self) -> IndexResult<String> {
        info!("Starting full engine-agnostic rebuild of full-text indexes");
        println!("===========> Starting full engine-agnostic rebuild of full-text indexes");

        // 1. Discover the active storage daemon port from the registry
        let registry = GLOBAL_DAEMON_REGISTRY.get().await;
        let all_daemons = registry.get_all_daemon_metadata().await.unwrap_or_default();
        
        let storage_daemon = all_daemons.iter()
            .find(|d| d.service_type == "storage")
            .ok_or_else(|| IndexingError::Other("No active storage daemon found in registry".into()))?;
        
        let port = storage_daemon.port;
        println!("===========> Found storage daemon on port {}", port);

        // 2. Fetch all vertices via DIRECT ZMQ
        println!("===========> Fetching all vertices via direct ZMQ (bypassing storage engine abstraction)");
        
        // This call is now completely independent of the current process's storage engine state
        let response = direct_zmq_request(port, "get_all_vertices").await?;
        
        // 3. Process the response
        let all_vertices_json = response.get("vertices")
            .and_then(|v| v.as_array())
            .ok_or_else(|| IndexingError::Other("Invalid response: 'vertices' array missing".into()))?;

        let vertex_count = all_vertices_json.len();
        println!("===========> Fetched {} vertices", vertex_count);
        info!("Rebuilding full-text index from {} vertices", vertex_count);

        // 4. Clear Index
        {
            let mut writer = self.manager.writer.lock().await;
            writer.delete_all_documents()
                .map_err(|e| IndexingError::TantivyError(e))?;
            writer.commit().map_err(|e| IndexingError::TantivyError(e))?;
        }

        // 5. Retrieve Metadata (what to index)
        let fulltext_indexes = self.retrieve_all_metadata(IndexType::FullText)
            .map_err(|e| IndexingError::Other(e.to_string()))?;

        // 6. Index Vertices
        let mut writer = self.manager.writer.lock().await;
        
        for v_json in all_vertices_json {
            // Extract ID (handling string or number)
            let node_id = if let Some(n) = v_json.get("id").and_then(|v| v.as_i64()) {
                n
            } else if let Some(s) = v_json.get("id").and_then(|v| v.as_str()) {
                // Simple hash of UUID string to i64 if necessary, or use 0. 
                // Ideally schema should match ID type.
                use std::collections::hash_map::DefaultHasher;
                use std::hash::{Hash, Hasher};
                let mut hasher = DefaultHasher::new();
                s.hash(&mut hasher);
                hasher.finish() as i64
            } else {
                0
            };

            let vertex_label = v_json.get("label").and_then(|v| v.as_str()).unwrap_or("");
            let properties = v_json.get("properties").and_then(|v| v.as_object());

            let mut content = String::new();

            for meta in &fulltext_indexes {
                let matches = meta.labels.is_empty() || meta.labels.iter().any(|l| l == vertex_label);
                
                if matches {
                    if let Some(props) = properties {
                        for prop_name in &meta.properties {
                            if let Some(val) = props.get(prop_name) {
                                if let Some(s) = val.as_str() {
                                    content.push_str(s);
                                    content.push(' ');
                                } else if let Some(n) = val.as_number() {
                                    content.push_str(&n.to_string());
                                    content.push(' ');
                                }
                            }
                        }
                    }
                }
            }

            if !content.is_empty() {
                let mut doc = TantivyDocument::new();
                doc.add_i64(self.manager.node_id_field, node_id);
                doc.add_text(self.manager.fulltext_content_field, content.trim());
                writer.add_document(doc).map_err(|e| IndexingError::TantivyError(e))?;
            }
        }

        writer.commit().map_err(|e| IndexingError::TantivyError(e))?;
        drop(writer);

        self.manager.reload_searcher()
            .map_err(|e| IndexingError::Other(e.to_string()))?;  // ✅ Fixed

        Ok(format!("Full-text index rebuilt: {} documents indexed", vertex_count))
    }

    async fn index_stats(&self) -> IndexResult<String> {
        let reader = self.manager.index.reader()?;
        let searcher = reader.searcher();
        let segment_count = searcher.segment_readers().len();
        let total_docs: u64 = searcher.segment_readers().iter().map(|r| r.num_docs() as u64).sum();

        Ok(json!({
            "status": "success",
            "stats": {
                "total_docs": total_docs,
                "segment_count": segment_count,
                "metadata_engine": format!("{:?}", self.engine_type)
            }
        }).to_string())
    }
}


// --- HELPER: Direct ZMQ Request ---
// This function creates a FRESH ZMQ context and socket to talk to the storage daemon.
// It avoids re-using any internal state that might be locked.

// Then the function signature works:
fn do_zmq_request(addr: &str, request_data: &[u8]) -> Result<JsonValue> {
    println!("===========> do_zmq_request: Creating ZMQ context");
    let zmq_context = zmq::Context::new();
    
    println!("===========> do_zmq_request: Creating socket");
    let client = zmq_context
        .socket(zmq::REQ)
        .context("Failed to create ZMQ socket")?;
    
    println!("===========> do_zmq_request: Setting timeouts");
    client.set_rcvtimeo(15000)
        .context("Failed to set receive timeout")?;
    client.set_sndtimeo(10000)
        .context("Failed to set send timeout")?;
    client.set_linger(500)
        .context("Failed to set linger")?;
    
    println!("===========> do_zmq_request: Connecting to {}", addr);
    client.connect(addr)
        .context(format!("Failed to connect to {}", addr))?;
    
    println!("===========> do_zmq_request: Sending request ({} bytes)", request_data.len());
    client.send(request_data, 0)
        .context("Failed to send request")?;
    
    println!("===========> do_zmq_request: Waiting for response");
    let mut msg = zmq::Message::new();
    client.recv(&mut msg, 0)
        .context("Failed to receive response")?;
    
    println!("===========> do_zmq_request: Received {} bytes", msg.len());
    
    client.disconnect(addr).ok();
    
    let response: JsonValue = serde_json::from_slice(msg.as_ref())
        .context("Failed to deserialize response")?;
    
    println!("===========> do_zmq_request: Parsed response successfully");
    Ok(response)
}

async fn direct_zmq_request(port: u16, command: &str) -> Result<JsonValue, IndexingError> {
    let command_string = command.to_string();
    let addr = format!("ipc:///tmp/graphdb-{}.ipc", port);
    
    let request = json!({ 
        "command": command_string, 
        "params": {},
        "request_id": uuid::Uuid::new_v4().to_string()
    });
    
    let request_data = serde_json::to_vec(&request)
        .map_err(|e| IndexingError::Other(format!("Serialization failed: {}", e)))?;
    
    let (tx, rx) = tokio::sync::oneshot::channel();
    
    std::thread::spawn(move || {
        let result = do_zmq_request(&addr, &request_data);
        let _ = tx.send(result);
    });
    
    let response = rx.await
        .map_err(|e| IndexingError::Other(format!("Channel error: {}", e)))?
        .map_err(|e| IndexingError::Other(format!("ZMQ error: {}", e)))?;
    
    Ok(response)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn setup_sled_engine() -> Result<TantivyIndexingEngine> {
        let db = sled::Config::new().temporary(true).open()?;
        let handles = TantivyEngineHandles::Sled(Arc::new(db));
        TantivyIndexingEngine::new(StorageEngineType::Sled, handles)
    }

    fn setup_rocksdb_engine() -> Result<TantivyIndexingEngine> {
        let temp_dir = std::env::temp_dir().join(format!("tantivy_rocksdb_test_{}", std::process::id()));
        std::fs::create_dir_all(&temp_dir)?;
        let db = rocksdb::DB::open_default(&temp_dir)?;
        let handles = TantivyEngineHandles::RocksDB(Arc::new(db));
        TantivyIndexingEngine::new(StorageEngineType::RocksDB, handles)
    }

    fn create_mock_external_document(node_id: i64, properties: HashMap<String, JsonValue>, labels: Vec<String>) -> Document {
        let graph_doc = GraphDocument { node_id, properties, labels };
        let fields: HashMap<String, String> = serde_json::from_value(serde_json::to_value(graph_doc).unwrap()).unwrap();
        Document {
            id: node_id.to_string(),
            fields,
        }
    }

    #[tokio::test]
    async fn test_fulltext_indexing_and_retrieval_sled() -> Result<()> {
        let engine = setup_sled_engine()?;

        let index_name = "dir_name_index_sled";
        engine.create_fulltext_index(index_name, &["Person:Director"], &["name"]).await?;

        let mut props = HashMap::new();
        props.insert("name".to_string(), JsonValue::String("Oliver Stone".to_string()));

        let doc = create_mock_external_document(
            10001,
            props,
            vec!["Person".to_string(), "Director".to_string(), "Person:Director".to_string()]
        );

        engine.index_document(doc).await?;
        engine.rebuild_indexes().await?;

        let query = "Oliver Stone";
        let search_res_str = engine.fulltext_search(query, 10).await?;
        let search_res: JsonValue = serde_json::from_str(&search_res_str)?;

        let results = search_res["results"].as_array().context("Results should be an array")?;
        assert!(!results.is_empty(), "Search for '{}' should return results after indexing.", query);

        let found_id = results[0]["node_id"].as_i64().unwrap_or(0);
        assert_eq!(found_id, 10001, "The retrieved node ID should match the indexed node ID.");
        Ok(())
    }

    #[tokio::test]
    async fn test_fulltext_indexing_and_retrieval_rocksdb() -> Result<()> {
        let engine = setup_rocksdb_engine()?;

        let index_name = "actor_name_index_rocksdb";
        engine.create_fulltext_index(index_name, &["Actor"], &["name"]).await?;

        let mut props = HashMap::new();
        props.insert("name".to_string(), JsonValue::String("Tom Hanks".to_string()));

        let doc = create_mock_external_document(
            10002,
            props,
            vec!["Person".to_string(), "Actor".to_string()]
        );

        engine.index_document(doc).await?;
        engine.rebuild_indexes().await?;

        let query = "Tom Hanks";
        let search_res_str = engine.fulltext_search(query, 10).await?;
        let search_res: JsonValue = serde_json::from_str(&search_res_str)?;

        let results = search_res["results"].as_array().context("Results should be an array")?;
        assert!(!results.is_empty(), "Search for '{}' should return results after indexing.", query);

        let found_id = results[0]["node_id"].as_i64().unwrap_or(0);
        assert_eq!(found_id, 10002, "The retrieved node ID should match the indexed node ID.");
        Ok(())
    }

    #[tokio::test]
    async fn test_document_update_and_delete() -> Result<()> {
        let engine = setup_sled_engine()?;
        let index_name = "test_index";
        engine.create_fulltext_index(index_name, &[], &["content"]).await?;

        let doc1 = create_mock_external_document(
            20001,
            HashMap::from([("content".to_string(), JsonValue::String("Initial content for deletion test".to_string()))]),
            vec![]
        );
        engine.index_document(doc1).await?;
        engine.rebuild_indexes().await?;
        assert!(!serde_json::from_str::<JsonValue>(&engine.fulltext_search("deletion", 10).await?)?["results"].as_array().unwrap().is_empty());

        let doc2_update = create_mock_external_document(
            20001,
            HashMap::from([("content".to_string(), JsonValue::String("Updated text for search verification".to_string()))]),
            vec![]
        );
        engine.index_document(doc2_update).await?;
        engine.rebuild_indexes().await?;

        assert!(serde_json::from_str::<JsonValue>(&engine.fulltext_search("deletion", 10).await?)?["results"].as_array().unwrap().is_empty());
        assert!(!serde_json::from_str::<JsonValue>(&engine.fulltext_search("verification", 10).await?)?["results"].as_array().unwrap().is_empty());

        engine.delete_document("20001").await?;
        engine.rebuild_indexes().await?;
        assert!(serde_json::from_str::<JsonValue>(&engine.fulltext_search("verification", 10).await?)?["results"].as_array().unwrap().is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn test_index_stats() -> Result<()> {
        let engine = setup_sled_engine()?;
        let index_name = "stats_test_index";
        engine.create_fulltext_index(index_name, &[], &["prop"]).await?;

        for i in 30001..30005 {
            let doc = create_mock_external_document(
                i,
                HashMap::from([("prop".to_string(), JsonValue::String(format!("test doc {}", i)))]),
                vec![]
            );
            engine.index_document(doc).await?;
        }

        let stats_pre_str = engine.index_stats().await?;
        let stats_pre: JsonValue = serde_json::from_str(&stats_pre_str)?;
        let pre_docs = stats_pre["stats"]["total_docs"].as_u64().unwrap_or(0);

        engine.rebuild_indexes().await?;
        let stats_post_str = engine.index_stats().await?;
        let stats_post: JsonValue = serde_json::from_str(&stats_post_str)?;
        let post_docs = stats_post["stats"]["total_docs"].as_u64().context("Total docs not found or invalid")?;

        assert_eq!(post_docs, pre_docs + 4, "Post-commit index should contain 4 new documents.");
        Ok(())
    }
}
