use std::collections::{ HashSet, HashMap };
use std::ops::Deref;
use std::str::FromStr;
use std::sync::Arc;
use std::result::Result;
use tokio::sync::{OnceCell, RwLock};
use uuid::Uuid;
use anyhow::Error;
use serde_json::{json, Value};
use chrono::Utc;

use crate::query_exec_engine::query_exec_engine::QueryExecEngine;
use crate::graph_engine::medical::*;
use crate::storage_engine::{ GraphStorageEngine, StorageEngine, GraphOp }; // Ensure this trait is in scope
use models::identifiers::{ Identifier, SerializableUuid, SerializableInternString };
use models::properties::{ PropertyValue, SerializableFloat };
use models::medical::*;
use models::{ Graph, Vertex, Edge };
use models::errors::{GraphError, GraphResult};

/// Defines the event types for all graph mutations.
/// This is the payload sent to observers.
#[derive(Debug, Clone)]
pub enum GraphEvent {
    InsertVertex(Vertex),
    DeleteVertex(Identifier),
    InsertEdge(Edge),
    DeleteEdge(Identifier),
}

/// Type alias for the mutation observer closure.
pub type MutationObserver = Arc<dyn Fn(GraphEvent) + Send + Sync + 'static>;

/// Global singleton for the GraphService, ensuring a single instance.
pub static GRAPH_SERVICE: OnceCell<Arc<GraphService>> = OnceCell::const_new();

#[derive(Clone)]
pub struct GraphService {
    /// The in-memory graph structure, acting as the fast, refreshed view.
    graph: Arc<RwLock<Graph>>,
    /// The StorageEngine for persistence operations.
    storage: Arc<dyn GraphStorageEngine + Send + Sync + 'static>,
    /// Field to hold observers for all graph-level mutations/transactions.
    mutation_observers: Arc<RwLock<Vec<MutationObserver>>>,
}

impl GraphService {
    /// Initialise the in-memory graph view by replaying all operations from the StorageEngine.
    pub async fn global_init(
        storage: Arc<dyn GraphStorageEngine + Send + Sync>,
    ) -> Result<(), GraphError> {
        let mut graph = Graph::new();
        
        // CRITICAL STEP: Load initial state by replaying operations from the StorageEngine.
        storage.replay_into(&mut graph).await
            .map_err(|e| GraphError::StorageError(e.to_string()))?;
        
        QueryExecEngine::global_init(storage.clone()).await?;

        let service = Arc::new(Self {
            graph: Arc::new(RwLock::new(graph)),
            storage,
            mutation_observers: Arc::new(RwLock::new(Vec::new())),
        });

        GRAPH_SERVICE
            .set(service.clone())
            .map_err(|_| GraphError::StorageError("GraphService already initialised".into()))?;
        Ok(())
    }

    /// Retrieves the global singleton GraphService instance.
    pub async fn get() -> Result<Arc<Self>, GraphError> {
        GRAPH_SERVICE.get()
            .cloned()
            .ok_or_else(|| GraphError::InternalError(
                "GraphService not initialized. Ensure initialize_graph_service is called before use.".into()
            ))
    }

    /// Helper to notify all registered observers of a graph mutation event.
    fn notify_mutation(&self, event: GraphEvent) {
        let observers_lock = self.mutation_observers.clone();
        
        tokio::spawn(async move {
            let observers_guard = observers_lock.read().await;
            
            for observer in observers_guard.iter() {
                let event_clone = event.clone();
                let observer_clone = observer.clone();
                
                tokio::spawn(async move {
                    observer_clone(event_clone);
                });
            }
        });
    }

    /// Allows external crates to subscribe to all graph changes.
    pub async fn register_observer(&self, observer: MutationObserver) -> Result<(), GraphError> {
        self.mutation_observers.write().await.push(observer);
        Ok(())
    }
    
    // =========================================================================
    // COMMAND EMISSION LOGIC
    // =========================================================================

    /// Converts a local GraphEvent into a portable GraphOp.
    pub fn event_to_graph_op(event: &GraphEvent) -> GraphOp {
        match event {
            GraphEvent::InsertVertex(v) => GraphOp::InsertVertex(v.clone()),
            GraphEvent::DeleteVertex(id) => GraphOp::DeleteVertex(id.clone()),
            GraphEvent::InsertEdge(e) => GraphOp::InsertEdge(e.clone()),
            GraphEvent::DeleteEdge(id) => GraphOp::DeleteEdge(id.clone()),
        }
    }

    /// Executes a raw, engine-agnostic command (GraphOp).
    pub async fn execute_engine_command(&self, op: GraphOp) -> Result<(), GraphError> {
        match op {
            GraphOp::InsertVertex(v) => self.add_vertex(v).await,
            GraphOp::DeleteVertex(id) => self.delete_vertex(id).await,
            GraphOp::InsertEdge(e) => self.add_edge(e).await,
            GraphOp::DeleteEdge(id) => self.delete_edge(id).await,
            _ => {
                Ok(())
            }
        }
    }

    // =========================================================================
    // PERSISTENCE + IN-MEMORY MUTATION OPERATIONS
    // =========================================================================

    /// Creates a vertex in both storage and memory.
    pub async fn create_vertex(&self, vertex: Vertex) -> Result<(), GraphError> {
        // 1. Persist to storage
        self.storage.create_vertex(vertex.clone()).await
            .map_err(|e| GraphError::StorageError(e.to_string()))?;
        
        // 2. Update in-memory graph and notify observers
        self.add_vertex(vertex).await
    }

    /// Updates a vertex in both storage and memory.
    pub async fn update_vertex(&self, vertex: Vertex) -> Result<(), GraphError> {
        // 1. Persist to storage
        self.storage.update_vertex(vertex.clone()).await
            .map_err(|e| GraphError::StorageError(e.to_string()))?;
        
        // 2. Update in-memory graph (add_vertex will overwrite existing)
        self.add_vertex(vertex).await
    }

    /// Creates an edge in both storage and memory.
    pub async fn create_edge(&self, edge: Edge) -> Result<(), GraphError> {
        // 1. Persist to storage
        self.storage.create_edge(edge.clone()).await
            .map_err(|e| GraphError::StorageError(e.to_string()))?;
        
        // 2. Update in-memory graph and notify observers
        self.add_edge(edge).await
    }

    /// Deletes a vertex by UUID from both storage and memory.
    pub async fn delete_vertex_by_uuid(&self, vertex_id: Uuid) -> Result<(), GraphError> {
        // 1. Delete from persistent storage
        self.storage.delete_vertex(&vertex_id).await
            .map_err(|e| GraphError::StorageError(e.to_string()))?;
        
        // 2. Remove from in-memory graph
        self.delete_vertex_from_memory(vertex_id).await?;
        
        // 3. Notify observers
        let identifier = Identifier::new(vertex_id.to_string())
            .map_err(|e| GraphError::ValidationError(e.to_string()))?;
        self.notify_mutation(GraphEvent::DeleteVertex(identifier));
        
        Ok(())
    }

    /// Deletes an edge from both storage and memory by its components.
    pub async fn delete_edge_by_components(
        &self, 
        outbound_id: &Uuid, 
        edge_type: &Identifier, 
        inbound_id: &Uuid
    ) -> Result<(), GraphError> {
        // 1. Find the edge in memory first
        let edge_opt = {
            let graph = self.graph.read().await;
            graph.edges.values()
                .find(|e| {
                    e.outbound_id.0 == *outbound_id 
                    && e.edge_type == *edge_type 
                    && e.inbound_id.0 == *inbound_id
                })
                .cloned()
        };
        
        if let Some(edge) = edge_opt {
            // 2. Delete from persistent storage
            self.storage.delete_edge(outbound_id, edge_type, inbound_id).await
                .map_err(|e| GraphError::StorageError(e.to_string()))?;
            
            // 3. Remove from in-memory graph
            self.delete_edge_from_memory(&edge).await?;
            
            // 4. Notify observers
            let edge_id = Identifier::new(edge.id.0.to_string())
                .map_err(|e| GraphError::ValidationError(e.to_string()))?;
            self.notify_mutation(GraphEvent::DeleteEdge(edge_id));
        }
        
        Ok(())
    }

    /// Deletes edges touching specific vertices from both storage and memory.
    pub async fn delete_edges_touching_vertices(
        &self, 
        vertex_ids: &HashSet<Uuid>
    ) -> Result<usize, GraphError> {
        // 1. Collect edges to delete from memory first
        let edges_to_delete: Vec<Edge> = {
            let graph = self.graph.read().await;
            graph.edges.values()
                .filter(|e| {
                    vertex_ids.contains(&e.outbound_id.0) || vertex_ids.contains(&e.inbound_id.0)
                })
                .cloned()
                .collect()
        };
        
        let deleted_count = edges_to_delete.len();
        
        // 2. Delete from persistent storage
        self.storage.delete_edges_touching_vertices(vertex_ids).await
            .map_err(|e| GraphError::StorageError(e.to_string()))?;
        
        // 3. Remove from in-memory graph and notify
        for edge in edges_to_delete {
            self.delete_edge_from_memory(&edge).await?;
            
            let edge_id = Identifier::new(edge.id.0.to_string())
                .map_err(|e| GraphError::ValidationError(e.to_string()))?;
            self.notify_mutation(GraphEvent::DeleteEdge(edge_id));
        }
        
        Ok(deleted_count)
    }

pub async fn match_nodes(
        &self,
        label: Option<&str>,
        query_props: &HashMap<String, PropertyValue>,
    ) -> GraphResult<Vec<Vertex>> {
        // ABSTRACTED READ: Assumes self.get_all_vertices() handles memory/storage lookup
        let all_vertices = self.get_all_vertices().await?;

        let filtered = all_vertices.into_iter().filter(|v| {
            // 1. Check Label Match
            let matches_label = if let Some(query_label) = label {
                let vertex_label_str = v.label.as_ref();
                vertex_label_str == query_label || vertex_label_str.starts_with(&format!("{}:", query_label))
            } else {
                true
            };
            
            // 2. Check Property Match (Binding/Filter Logic)
            let matches_props = if query_props.is_empty() {
                true
            } else {
                query_props.iter().all(|(k, expected_val)| {
                    v.properties.get(k).map_or(false, |actual_val| actual_val == expected_val)
                })
            };
            
            matches_label && matches_props
        }).collect::<Vec<_>>();
        
        Ok(filtered)
    }

    /// Executes a MATCH query with multiple, disjoint node patterns (like MATCH (a:L1), (b:L2)).
    pub async fn match_multiple_nodes(
        &self,
        nodes: Vec<(Option<String>, Option<String>, HashMap<String, Value>)>,
    ) -> GraphResult<Vec<Vertex>> {
        // ABSTRACTED READ: Assumes self.get_all_vertices() handles memory/storage lookup
        let all_vertices = self.get_all_vertices().await?;
        let mut result_vertices = Vec::new();
        let mut matched_ids = HashSet::new();

        for (_var, label, properties) in nodes {
            let props: HashMap<String, PropertyValue> = properties
                .into_iter()
                .map(|(k, v)| to_property_value(v.clone()).map(|pv| (k.clone(), pv)))
                .collect::<GraphResult<_>>()?;
            
            for v in &all_vertices {
                let matches_label = label.as_ref().map_or(true, |l| {
                    let vl = v.label.as_ref();
                    vl == l || vl.starts_with(&format!("{}:", l))
                });
                
                let matches_props = props.iter().all(|(k, expected_val)| {
                    v.properties.get(k).map_or(false, |actual_val| actual_val == expected_val)
                });
                
                if matches_label && matches_props && !matched_ids.contains(&v.id) {
                    result_vertices.push(v.clone());
                    matched_ids.insert(v.id);
                }
            }
        }
        
        Ok(result_vertices)
    }

    pub async fn create_node_batch(
        &self,
        nodes: Vec<(Option<String>, HashMap<String, Value>)>,
    ) -> GraphResult<Vec<Vertex>> {
        let mut created_vertices = Vec::new();

        for (label_opt, properties) in nodes {
            let props: GraphResult<HashMap<String, PropertyValue>> = properties
                .into_iter()
                .map(|(k, v)| to_property_value(v).map(|pv| (k, pv)))
                .collect();
            
            let label_str = label_opt.unwrap_or_else(|| "Node".to_string());

            let vertex = Vertex {
                id: SerializableUuid(Uuid::new_v4()),
                label: Identifier::new(label_str)?,
                properties: props?,
                created_at: Utc::now().into(),
                updated_at: Utc::now().into(),
            };
            
            // Centralized persistence and in-memory update handled by create_vertex
            self.create_vertex(vertex.clone()).await?;

            created_vertices.push(vertex);
        }
        
        Ok(created_vertices)
    }

    /// Key-value insert operation.
    pub async fn kv_insert(&self, key: Vec<u8>, value: Vec<u8>) -> Result<(), GraphError> {
        self.storage.insert(key, value).await
            .map_err(|e| GraphError::StorageError(e.to_string()))
    }

    /// Key-value retrieve operation.
    pub async fn kv_retrieve(&self, key: &[u8]) -> Result<Option<Vec<u8>>, GraphError> {
        // FIX 1: Convert &[u8] to &Vec<u8> for the retrieve method
        let owned_key = key.to_vec();
        self.storage.retrieve(&owned_key).await
            .map_err(|e| GraphError::StorageError(e.to_string()))
    }

    /// Key-value delete operation.
    pub async fn kv_delete(&self, key: &[u8]) -> Result<(), GraphError> {
        // FIX 1: Convert &[u8] to &Vec<u8> for the delete method
        let owned_key = key.to_vec();
        self.storage.delete(&owned_key).await
            .map_err(|e| GraphError::StorageError(e.to_string()))
    }

    /// Flush storage to disk.
    pub async fn flush_storage(&self) -> Result<(), GraphError> {
        self.storage.flush().await
            .map_err(|e| GraphError::StorageError(e.to_string()))
    }

    /// Gets all vertices from storage.
    pub async fn get_all_vertices_from_storage(&self) -> Result<Vec<Vertex>, GraphError> {
        // FIX: Revert to idiomatic dot notation to enable Deref Coercion (Arc -> dyn Trait)
        self.storage.get_all_vertices().await
            .map_err(|e| GraphError::StorageError(e.to_string()))
    }

    /// Gets all edges from storage.
    pub async fn get_all_edges_from_storage(&self) -> Result<Vec<Edge>, GraphError> {
        // FIX: Revert to idiomatic dot notation to enable Deref Coercion (Arc -> dyn Trait)
        self.storage.get_all_edges().await
            .map_err(|e| GraphError::StorageError(e.to_string()))
    }

    /// Gets a vertex by UUID from storage.
    pub async fn get_vertex_from_storage(&self, vertex_id: &Uuid) -> Result<Option<Vertex>, GraphError> {
        // FIX: Revert to idiomatic dot notation to enable Deref Coercion (Arc -> dyn Trait)
        self.storage.get_vertex(vertex_id).await
            .map_err(|e| GraphError::StorageError(e.to_string()))
    }

    /// Synonym for 'get_all_vertices_from_storage'.
    /// Gets all vertices from the underlying storage.
    pub async fn get_all_vertices(&self) -> Result<Vec<Vertex>, GraphError> {
        self.get_all_vertices_from_storage().await
    }

    /// Synonym for 'get_all_edges_from_storage'.
    /// Gets all edges from the underlying storage.
    pub async fn get_all_edges(&self) -> Result<Vec<Edge>, GraphError> {
        self.get_all_edges_from_storage().await
    }

    /// Synonym for 'get_vertex_from_storage'.
    /// Gets a vertex by UUID from the underlying storage.
    pub async fn get_stored_vertex(&self, vertex_id: &Uuid) -> Result<Option<Vertex>, GraphError> {
        self.get_vertex_from_storage(vertex_id).await
    }

    // =========================================================================
    // DECLARATIVE QUERY EXECUTION (via QueryExecEngine)
    // =========================================================================

    /// Executes a declarative read-only Cypher query against the graph.
    /// 
    /// NOTE: Assumes QueryExecEngine is a singleton accessible via QueryExecEngine::get().
    pub async fn execute_cypher_read(
        &self,
        query: &str,
        params: Value, // This parameter is currently ignored to match the QueryExecEngine contract
    ) -> Result<Vec<Value>, GraphError> {
        // Retrieve the global Query Execution Engine instance
        let engine = QueryExecEngine::get()
            .await
            .map_err(|e| GraphError::InternalError(format!("QueryExecEngine not initialized: {}", e)))?;

        // FIX 1 (E0061): Remove the `params` argument from the call.
        // FIX 2 (E0308): Use `.map(|value| vec![value])` to convert the single Value into a Vec<Value>.
        println!("===> in execute_cypher_read - query was: {}", query);
        engine.execute_cypher(query) 
            .await
            .map_err(|e| GraphError::QueryExecutionError(format!("Cypher READ failed: {}", e)))
            .map(|value| vec![value]) // Wrap the single result in a vector
    }

    /// Executes a declarative write Cypher query against the graph (e.g., CREATE, MERGE).
    /// 
    /// NOTE: This is necessary for the PatientService::create_patient implementation.
    pub async fn execute_cypher_write(
        &self,
        query: &str,
        params: Value, // This parameter is currently ignored to match the QueryExecEngine contract
    ) -> Result<Vec<Value>, GraphError> {
        // Retrieve the global Query Execution Engine instance
        let engine = QueryExecEngine::get()
            .await
            .map_err(|e| GraphError::InternalError(format!("QueryExecEngine not initialized: {}", e)))?;

        // FIX 1 (E0061): Remove the `params` argument from the call.
        // FIX 2 (E0308): Use `.map(|value| vec![value])` to convert the single Value into a Vec<Value>.
        println!("===> in execute_cypher_write - query was: {}", query);
        engine.execute_cypher(query)
            .await
            .map_err(|e| GraphError::QueryExecutionError(format!("Cypher WRITE failed: {}", e)))
            .map(|value| vec![value]) // Wrap the single result in a vector
    }

    // =========================================================================
    // IN-MEMORY ONLY OPERATIONS (for observer pattern)
    // =========================================================================

    /// Adds vertex to memory only and notifies observers.
    /// Used internally after storage persistence is complete.
    pub async fn add_vertex(&self, vertex: Vertex) -> Result<(), GraphError> {
        // 1. Modify in-memory graph
        self.graph.write().await.add_vertex(vertex.clone()); 
        // 2. Notify observers (Persistence Engine/ZMQ Listener will be here)
        self.notify_mutation(GraphEvent::InsertVertex(vertex)); 
        Ok(())
    }

    /// Adds edge to memory only and notifies observers.
    /// Used internally after storage persistence is complete.
    pub async fn add_edge(&self, edge: Edge) -> Result<(), GraphError> {
        // 1. Modify in-memory graph
        self.graph.write().await.add_edge(edge.clone()); 
        // 2. Notify observers
        self.notify_mutation(GraphEvent::InsertEdge(edge)); 
        Ok(())
    }

    /// Deletes a vertex from memory and fires the mutation hook.
    pub async fn delete_vertex(&self, vertex_id: Identifier) -> Result<(), GraphError> {
        let uuid_id = Uuid::from_str(vertex_id.0.as_ref())
            .map_err(|e| GraphError::InternalError(format!("Invalid UUID format for vertex deletion: {}", e)))?;
        
        // 1. Modify in-memory graph (this handles associated edge cleanup)
        self.delete_vertex_from_memory(uuid_id).await?;

        // 2. Notify observers
        self.notify_mutation(GraphEvent::DeleteVertex(vertex_id));
        Ok(())
    }

    /// Deletes an edge from memory and fires the mutation hook.
    pub async fn delete_edge(&self, edge_id: Identifier) -> Result<(), GraphError> {
        let edge_id_uuid = Uuid::from_str(edge_id.0.as_ref())
            .map_err(|e| GraphError::InternalError(format!("Invalid UUID format for edge deletion: {}", e)))?;
        
        let edge_opt = { self.graph.read().await.edges.get(&edge_id_uuid).cloned() };
        let edge = edge_opt.ok_or_else(|| GraphError::InternalError(format!("Edge {} not found for deletion.", edge_id_uuid)))?;

        // 1. Modify in-memory graph
        self.delete_edge_from_memory(&edge).await?;

        // 2. Notify observers
        self.notify_mutation(GraphEvent::DeleteEdge(edge_id));
        Ok(())
    }

    // =========================================================================
    // READ & TRAVERSAL OPERATIONS
    // =========================================================================

    pub async fn read(&self) -> tokio::sync::RwLockReadGuard<'_, Graph> {
        self.graph.read().await
    }
    
    /// Retrieves a single Vertex by its Uuid from memory.
    pub async fn get_vertex(&self, vertex_id: &Uuid) -> Option<Vertex> {
        let graph = self.read().await;
        graph.get_vertex(vertex_id).cloned()
    }

    /// Retrieves vertices by a specific label and property value from memory.
    pub async fn get_vertices_by_label_and_property(
        &self,
        label: &str,
        property_key: &str,
        property_value: &PropertyValue,
    ) -> Vec<Vertex> {
        let graph = self.read().await;
        graph.vertices.values()
            .filter(|v| {
                v.label.as_ref() == label && v.properties.get(property_key) == Some(property_value)
            })
            .cloned()
            .collect()
    }

    /// Retrieves all edges connected to a given vertex (both inbound and outbound).
    pub async fn get_connected_edges(&self, vertex_id: Uuid) -> Vec<Edge> {
        let graph = self.read().await;
        let mut connected_edges = Vec::new();

        let mut add_edges = |edge_ids: Option<&HashSet<Uuid>>| {
            if let Some(ids) = edge_ids {
                for id in ids {
                    if let Some(edge) = graph.edges.get(id) {
                        connected_edges.push(edge.clone());
                    }
                }
            }
        };

        add_edges(graph.out_edges.get(&vertex_id));
        add_edges(graph.in_edges.get(&vertex_id));

        let unique_edges: HashSet<Edge> = connected_edges.into_iter().collect();
        unique_edges.into_iter().collect()
    }

    /// Retrieves all vertices connected to a given vertex (neighbors).
    pub async fn get_connected_vertices(&self, vertex_id: Uuid) -> Vec<Vertex> {
        let graph = self.read().await;
        let mut neighbor_ids = HashSet::new();

        if let Some(out_edges) = graph.out_edges.get(&vertex_id) {
            for edge_id in out_edges {
                if let Some(edge) = graph.edges.get(edge_id) {
                    neighbor_ids.insert(edge.inbound_id.0);
                }
            }
        }

        if let Some(in_edges) = graph.in_edges.get(&vertex_id) {
            for edge_id in in_edges {
                if let Some(edge) = graph.edges.get(edge_id) {
                    neighbor_ids.insert(edge.outbound_id.0);
                }
            }
        }

        neighbor_ids.into_iter()
            .filter_map(|id| graph.vertices.get(&id).cloned())
            .collect()
    }
    
    /// Retrieves the Patient Vertex based on its i32 ID (patient_id field).
    pub async fn get_patient_vertex_by_id(&self, patient_id: i32) -> Option<Vertex> {
        let graph = self.read().await;
        graph.vertices.values()
            .find(|v| {
                v.label.as_ref() == "Patient" &&
                v.properties.get("id")
                    .and_then(|v| v.as_str())
                    .and_then(|s| s.parse::<i32>().ok()) == Some(patient_id)
            })
            .cloned()
    }

    pub async fn patient_view(&self, patient_vertex_id: Uuid) -> Option<Patient> {
        let graph = self.read().await;
        let vertex = graph.get_vertex(&patient_vertex_id)?;
        Patient::from_vertex(vertex)
    }

    pub async fn patient_by_id(&self, patient_id: i32) -> Option<Patient> {
        let graph = self.read().await;
        graph.vertices.values()
            .find(|v| {
                v.label.as_ref() == "Patient" &&
                v.properties.get("id")
                    .and_then(|v| v.as_str())
                    .and_then(|s| s.parse::<i32>().ok()) == Some(patient_id)
            })
            .and_then(|v| Patient::from_vertex(v))
    }

    pub async fn all_patients(&self) -> Vec<Patient> {
        let graph = self.read().await;
        graph.vertices.values()
            .filter(|v| v.label.as_ref() == "Patient")
            .filter_map(|v| Patient::from_vertex(v))
            .collect()
    }

    pub async fn search_patients(&self, query: &str) -> Vec<Patient> {
        let graph = self.read().await;
        let query = query.to_lowercase();
        graph.vertices.values()
            .filter(|v| v.label.as_ref() == "Patient")
            .filter_map(|v| {
                let first = v.properties.get("first_name")?.as_str()?.to_lowercase();
                let last = v.properties.get("last_name")?.as_str()?.to_lowercase();
                if first.contains(&query) || last.contains(&query) {
                    Patient::from_vertex(v)
                } else {
                    None
                }
            })
            .collect()
    }

    /// Asynchronously removes a single edge from the in-memory graph structure.
    /// This is an internal helper for `delete_edge`.
    pub async fn delete_edge_from_memory(&self, edge: &Edge) -> Result<(), GraphError> {
        let mut graph = self.graph.write().await; 

        if graph.edges.remove(&edge.id.0).is_some() {
            if let Some(outbound_set) = graph.out_edges.get_mut(&edge.outbound_id.0) {
                outbound_set.remove(&edge.id.0);
                if outbound_set.is_empty() {
                    graph.out_edges.remove(&edge.outbound_id.0);
                }
            }

            if let Some(inbound_set) = graph.in_edges.get_mut(&edge.inbound_id.0) {
                inbound_set.remove(&edge.id.0);
                if inbound_set.is_empty() {
                    graph.in_edges.remove(&edge.inbound_id.0);
                }
            }
            graph.edge_count = graph.edge_count.saturating_sub(1);
        }

        Ok(())
    }

    /// Asynchronously removes a single vertex from the in-memory graph structure and cleans up its associated edges.
    /// This is an internal helper for `delete_vertex`.
    pub async fn delete_vertex_from_memory(&self, vertex_id: Uuid) -> Result<(), GraphError> {
        let mut graph = self.graph.write().await;
        
        if graph.vertices.remove(&vertex_id).is_some() {
            let mut edges_to_remove = HashSet::new();

            if let Some(out_edges) = graph.out_edges.remove(&vertex_id) {
                edges_to_remove.extend(out_edges);
            }

            if let Some(in_edges) = graph.in_edges.remove(&vertex_id) {
                edges_to_remove.extend(in_edges);
            }

            for edge_id in edges_to_remove.iter() {
                if graph.edges.remove(edge_id).is_some() {
                    graph.edge_count = graph.edge_count.saturating_sub(1);
                }
            }
            
            graph.vertex_count = graph.vertex_count.saturating_sub(1);
        }

        Ok(())
    }

    // In lib/src/graph_engine/graph_service.rs (inside impl GraphService)

    /// Atomically deletes a single edge by its unique UUID.
    /// This implementation ensures persistent storage, in-memory edge map, and
    /// the in-memory adjacency maps (`out_edges`, `in_edges`) are all cleaned up.
    pub async fn delete_edge_by_uuid(&self, id: Uuid) -> Result<(), GraphError> {
        // 1. Acquire write lock and remove the edge from the in-memory map.
        let edge_components = {
            let mut graph = self.graph.write().await;
            
            if let Some(edge) = graph.edges.remove(&id) {
                // 2. Cleanup adjacency lists on the Graph struct
                // We must remove the edge ID from the source vertex's outgoing list...
                if let Some(out_edges) = graph.out_edges.get_mut(&edge.outbound_id.0) {
                    out_edges.remove(&id);
                    if out_edges.is_empty() {
                        // Optional cleanup: remove the entry if the set is empty
                        graph.out_edges.remove(&edge.outbound_id.0);
                    }
                }
                // ...and the target vertex's incoming list.
                if let Some(in_edges) = graph.in_edges.get_mut(&edge.inbound_id.0) {
                    in_edges.remove(&id);
                    if in_edges.is_empty() {
                        // Optional cleanup: remove the entry if the set is empty
                        graph.in_edges.remove(&edge.inbound_id.0);
                    }
                }
                
                // 3. Decrement edge count
                graph.edge_count = graph.edge_count.saturating_sub(1);
                
                // Return components needed for persistent storage delete
                Some((edge.outbound_id.0, edge.edge_type.clone(), edge.inbound_id.0))
            } else {
                None
            }
        };

        if let Some((out_id, edge_type, in_id)) = edge_components {
            // 4. Delete from persistent storage (using the existing component-based method)
            self.storage.delete_edge(&out_id, &edge_type, &in_id).await?;
        } 
        
        // Edge not found or deletion successful.
        Ok(())
    }

    /// Atomically performs the DETACH DELETE operation for a list of vertex IDs.
    /// Deletes all touching edges first, then the vertices. Returns the count of deleted edges.
    pub async fn detach_delete_vertices(&self, ids: &Vec<Uuid>) -> Result<usize, GraphError> {
        
        // 1. Identify all edges touching the vertices by iterating over ALL edges.
        let edges_to_delete_uuids = {
            let graph = self.graph.read().await;
            let vertex_ids: HashSet<Uuid> = ids.iter().cloned().collect();
            graph.edges.values()
                .filter(|edge| {
                    // An edge is touching if its source or target ID is in the set of vertices to delete.
                    vertex_ids.contains(&edge.outbound_id.0) || vertex_ids.contains(&edge.inbound_id.0)
                })
                .map(|edge| edge.id.0)
                .collect::<HashSet<Uuid>>()
        };

        let mut deleted_edges_count = 0;

        // 2. Delete all touching edges atomically (handles storage and memory cleanup for edges)
        for edge_id in edges_to_delete_uuids.into_iter() {
            // Use the self-contained delete_edge_by_uuid wrapper 
            self.delete_edge_by_uuid(edge_id).await?;
            deleted_edges_count += 1;
        }

        // 3. Delete the vertex records atomically
        for id in ids {
            // Delete from persistent storage
            self.storage.delete_vertex(id).await?;
            
            // Update in-memory graph
            let mut graph = self.graph.write().await;
            // This removes the Vertex object itself
            if graph.vertices.remove(id).is_some() {
                 // 4. Decrement vertex count and cleanup adjacency list maps
                graph.vertex_count = graph.vertex_count.saturating_sub(1);
                // The vertex's adjacency list entries are now empty (since we deleted all edges in step 2), 
                // but we must remove the entry for the vertex itself from the out/in_edges maps 
                // if it still exists (it shouldn't based on step 2's cleanup, but belt and suspenders).
                graph.out_edges.remove(id); 
                graph.in_edges.remove(id);
            }
        }

        Ok(deleted_edges_count)
    }

    // ===== Observer helpers (kept for backward compatibility) =====
    pub async fn add_vertex_observer<F>(&self, f: F) -> Result<(), GraphError>
    where
        F: Fn(&Vertex) + Send + Sync + 'static,
    {
        let observer: MutationObserver = Arc::new(move |event| {
            if let GraphEvent::InsertVertex(v) = event {
                f(&v);
            }
        });
        self.register_observer(observer).await
    }

    pub async fn add_edge_observer<F>(&self, f: F) -> Result<(), GraphError>
    where
        F: Fn(&Edge) + Send + Sync + 'static,
    {
        let observer: MutationObserver = Arc::new(move |event| {
            if let GraphEvent::InsertEdge(e) = event {
                f(&e);
            }
        });
        self.register_observer(observer).await
    }

    // ===== helpers for outside crates =====
    /// Read-only access to the inner `Graph`.
    pub async fn get_graph(&self) -> tokio::sync::RwLockReadGuard<'_, Graph> {
        self.graph.read().await
    }

    /// Mutable access to the inner `Graph`.
    /// 
    /// NOTE: This access should be used carefully by external systems only for bulk
    /// operations or state recovery, as it bypasses the observer notification system.
    pub async fn write_graph(&self) -> tokio::sync::RwLockWriteGuard<'_, Graph> {
        self.graph.write().await
    }
}

// =========================================================================
// HELPER FOR INITIALIZATION/RETRIEVAL
// =========================================================================

/// Helper to ensure the global GraphService is initialized.
pub async fn initialize_graph_service(
    // FIX: Change input type to the specific GraphStorageEngine trait
    storage: Arc<dyn GraphStorageEngine + Send + Sync>,
) -> Result<Arc<GraphService>, GraphError> {
    match GRAPH_SERVICE.get() {
        Some(service) => Ok(service.clone()),
        None => {
            // This call now matches the expected argument type (GraphStorageEngine)
            GraphService::global_init(storage.clone()).await?;
            
            GRAPH_SERVICE.get()
                .cloned()
                .ok_or_else(|| GraphError::StorageError("Failed to retrieve GraphService after initialization".into()))
        }
    }
}

// =========================================================================
// LOCKABLE GRAPH SERVICE (Legacy)
// =========================================================================

pub struct LockableGraphService {
    pub in_memory_edges: HashMap<String, Edge>,
}

impl LockableGraphService {
    pub fn new() -> Self {
        LockableGraphService {
            in_memory_edges: HashMap::new(),
        }
    }

    pub fn create_edge_in_memory(&mut self, edge: Edge) -> GraphResult<()> {
        let edge_id_key = edge.id.0.to_string(); 
        
        if self.in_memory_edges.contains_key(&edge_id_key) {
            eprintln!("Warning: Edge with ID {} already exists. Overwriting.", edge_id_key);
        }
        self.in_memory_edges.insert(edge_id_key, edge);
        Ok(())
    }
}

/// Helper to convert Cypher `Value` → `PropertyValue`
// Assuming GraphResult<T> is defined as Result<T, GraphError>
fn to_property_value(v: Value) -> GraphResult<PropertyValue> {
    match v {
        Value::String(s) => Ok(PropertyValue::String(s)),
        Value::Number(n) if n.is_i64() => Ok(PropertyValue::Integer(n.as_i64().unwrap())),
        Value::Number(n) if n.is_f64() => Ok(PropertyValue::Float(SerializableFloat(n.as_f64().unwrap()))),
        Value::Bool(b) => Ok(PropertyValue::Boolean(b)),
        // The error suggests the return type expects a different Error type (e.g., anyhow::Error).
        // To satisfy the "Must use GraphError" constraint, we must assume the function's signature
        // is `fn to_property_value(v: Value) -> Result<PropertyValue, GraphError>`.
        // If the original `GraphResult` was `Result<T, anyhow::Error>`, then GraphError needs .into()
        // to convert it. However, the requirement is to use GraphError as the error type.
        
        // Sticking to the requirement: the function returns GraphError
        Value::Null => Err(GraphError::InternalError("Null values not supported in properties".into())),
        Value::Array(_) => Err(GraphError::InternalError("Array values not supported in properties".into())),
        Value::Object(_) => Err(GraphError::InternalError("Nested objects not supported in properties".into())),
        _ => Err(GraphError::InternalError("Unsupported property value type".into())),
    }
}