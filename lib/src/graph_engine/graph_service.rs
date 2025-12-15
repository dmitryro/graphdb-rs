use anyhow::{Result, Context, anyhow};
use std::collections::{ HashSet, HashMap };
use internment::Intern;
use std::ops::Deref;
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::{OnceCell, RwLock};
use uuid::Uuid;
use anyhow::Error;
use serde_json::{json, Value};
use chrono::Utc;
use log::{info, error, warn};
use models::identifiers::{ Identifier, SerializableUuid, SerializableInternString };
use models::properties::{ PropertyValue, SerializableFloat };
use models::medical::*;
use models::{ Graph, Vertex, Edge, timestamp::BincodeDateTime };
use models::errors::{GraphError, GraphResult};
use crate::query_exec_engine::query_exec_engine::QueryExecEngine;
use crate::graph_engine::medical::*;
use crate::storage_engine::{ GraphStorageEngine, StorageEngine, GraphOp }; // Ensure this trait is in scope
use crate::graph_engine::parse_patient::{ parse_patient_from_cypher_result };

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
    pub graph: Arc<RwLock<Graph>>,
    /// The StorageEngine for persistence operations.
    pub storage: Arc<dyn GraphStorageEngine + Send + Sync + 'static>,
    /// Field to hold observers for all graph-level mutations/transactions.
    pub mutation_observers: Arc<RwLock<Vec<MutationObserver>>>,
}

/// Helper to convert a Cypher Node (represented as serde_json::Value) into our Rust Vertex model.
/// This assumes the JSON Value contains the 'id', 'labels', and 'properties' fields.
fn try_parse_cypher_node_to_vertex(cypher_node: Value) -> Result<Vertex, GraphError> {
    
    let node_map = cypher_node.as_object()
        .ok_or_else(|| GraphError::DeserializationError("Cypher node result not an object".into()))?;

    // 1. Get ID
    let id_str = node_map.get("id")
        .and_then(Value::as_str)
        .ok_or_else(|| GraphError::DeserializationError("Missing 'id' in Cypher Node result".into()))?;
        
    let id = Uuid::from_str(id_str)
        .map_err(|e| GraphError::DeserializationError(format!("Invalid Uuid format: {}", e)))?;

    // 2. Get Label
    let labels = node_map.get("labels")
        .and_then(Value::as_array)
        .ok_or_else(|| GraphError::DeserializationError("Missing 'labels' in Cypher Node result".into()))?;
        
    let label_str = labels.get(0)
        .and_then(Value::as_str)
        .ok_or_else(|| GraphError::DeserializationError("No primary label found in Cypher Node result".into()))?;
        
    // 3. Get Properties
    let properties_val = node_map.get("properties")
        .ok_or_else(|| GraphError::DeserializationError("Missing 'properties' in Cypher Node result".into()))?;

    let mut properties = HashMap::new();
    
    if let Some(prop_map) = properties_val.as_object() {
        for (key, val) in prop_map {
            let pv = to_property_value(val.clone())
                .map_err(|e| GraphError::DeserializationError(format!("Failed to convert property value for key {}: {}", key, e)))?;

            // Key is String, which matches the previous fix for the Vertex model.
            properties.insert(key.clone(), pv); 
        }
    }

    let now = Utc::now();

    Ok(Vertex {
        id: SerializableUuid(id),
        label: Identifier(
            // Assuming SerializableInternString is an alias for Intern<String>
            // and implements From<Intern<String>>
            SerializableInternString::from(Intern::from(label_str.to_string()))
        ),
        // FIX 2 & 3: Wrap the chrono DateTime in the required BincodeDateTime struct.
        created_at: BincodeDateTime(now),
        updated_at: BincodeDateTime(now),
        
        properties,
    })
}

// Placeholder helper function (must be implemented elsewhere)
// This is necessary to satisfy the `execute_demographic_search` implementation.
fn sanitize_cypher_string(s: &str) -> String {
    // Basic sanitization: escape single quotes and trim whitespace
    s.replace('\'', "\\'")
        .trim()
        .to_string()
}

// Helper function to create the correct Identifier type for GraphError::NotFound
// ASSUMPTION: Intern is available and has a method to convert to Intern<String>
fn create_error_identifier(s: String) -> models::Identifier {
    // 1. Intern the String
    let interned_string = Intern::from(s); // Or Intern::new(s) or similar
    
    // 2. Convert the Interned type to SerializableInternString
    Identifier(SerializableInternString::from(interned_string))
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

    /// Performs a partial update on a Vertex by ID.
    /// Fetches the existing vertex, merges the new properties, and saves the updated vertex.
    pub async fn update_vertex_properties(&self, id: Uuid, props: HashMap<String, PropertyValue>) -> Result<(), GraphError> {
        
        // 1. Read: Fetch the existing vertex (returns Option<Vertex> per E0308 error)
        // FIX for E0308: Use a match structure for Option<Vertex> and manually handle the error return path.
        // FIX for E0308: Borrow '&id' for the method call.
        let mut existing_vertex = match self.get_vertex(&id).await { 
            Some(v) => v, // Success
            None => {
                warn!("Attempted to update properties for non-existent Vertex ID: {}", id);
                // Error path for 'NotFound'
                let err_msg = format!("Vertex with ID {} not found for update.", id);
                return Err(GraphError::NotFound(create_error_identifier(err_msg)));
            }
        };
        
        // NOTE: If get_vertex() can fail with a storage error, you must wrap it in a custom function
        // that returns Result<Option<Vertex>, GraphError> to properly handle that case.
        // For now, we proceed assuming get_vertex handles storage errors internally and returns None on not-found.

        // 2. Modify: Merge new properties
        info!("Merging {} new properties into Vertex ID {}", props.len(), id);
        for (key, value) in props {
            existing_vertex.properties.insert(key, value);
        }
        
        // Update the timestamp to reflect the modification
        existing_vertex.updated_at = BincodeDateTime(Utc::now());
        
        // 3. Write: Save the modified vertex
        match self.update_vertex(existing_vertex).await {
            Ok(_) => {
                info!("Successfully updated Vertex properties for ID {}", id);
                Ok(())
            },
            Err(e) => {
                // GraphError::StorageError expects String
                let err_msg = format!("Failed to save updated vertex {}: {}", id, e);
                Err(GraphError::StorageError(err_msg))
            }
        }
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

    // --- Corrected High-Level Methods ---

    /// **Resolves an external identifier to a canonical PatientId (Golden Record ID).**
    pub async fn find_canonical_id_by_external_id(
        &self,
        external_id_value: &str,
        id_type: &IdType,
    ) -> Result<Option<PatientId>> {
        
        // Sanitize inputs before embedding in the query string
        let safe_id_value = sanitize_cypher_string(external_id_value);
        let safe_id_type = format!("{:?}", id_type); // Use debug format as Display is missing

        let query = format!(
            "MATCH (e:ExternalId {{value: '{safe_id_value}', type: '{safe_id_type}'}})-[:LINKS_TO]->(p:Patient:GoldenRecord) 
             RETURN p.id AS canonical_id",
        );
        
        let raw_results_vec_of_value = self.execute_cypher_read(&query, Value::Null)
            .await
            .context("GraphService failed to execute canonical ID lookup query")?;

        let raw_result_set = raw_results_vec_of_value.into_iter().next()
            .unwrap_or_else(|| Value::Array(Vec::new())); 

        // The result set is expected to be an array of maps ({ "canonical_id": "..." })
        if let Some(row_value) = raw_result_set.as_array().and_then(|arr| arr.get(0)) {
            if let Some(id_str) = row_value.get("canonical_id").and_then(|v| v.as_str()) {
                return Ok(Some(PatientId::from(id_str.to_string())));
            }
        }

        Ok(None)
    }

    /// **Retrieves the complete Patient Golden Record by its canonical ID.**
    ///
    /// NOTE: We must ensure this returns the entire node properties for the parser,
    /// by returning the node `p` itself, which the engine is assumed to serialize into a map.
    pub async fn get_patient_by_id(&self, canonical_id: &str) -> Result<Option<Patient>> {
        
        let safe_id = sanitize_cypher_string(canonical_id);
        
        // Match only the Golden Record using the ID
        let query = format!(
            "MATCH (p:Patient:GoldenRecord {{id: '{safe_id}'}}) 
             RETURN p", // Return the node 'p' itself, assumed to be serialized as a map/Value
        );
        
        let raw_results_vec_of_value = self.execute_cypher_read(&query, Value::Null)
            .await
            .context("GraphService failed to execute patient retrieval query")?;

        let raw_result_set = raw_results_vec_of_value.into_iter().next()
            .unwrap_or_else(|| Value::Array(Vec::new()));

        // The result set is expected to be an array of a single patient node map.
        if let Some(patient_map) = raw_result_set.as_array().and_then(|arr| arr.get(0)) {
            
            // The result is the raw patient node map, pass it directly to the parser
            return Ok(Some(parse_patient_from_cypher_result(patient_map.clone())?));
        }

        Ok(None)
    }

    /// Executes a demographic search by constructing a dynamic Cypher query.
    ///
    /// It builds a MATCH clause targeting only Golden Record nodes (`:Patient:GoldenRecord`)
    /// and applies `WHERE` filters for each provided demographic criteria.
    pub async fn execute_demographic_search(
        &self,
        criteria: HashMap<String, String>,
    ) -> Result<Vec<Patient>> {
        if criteria.is_empty() {
            return Ok(Vec::new()); // No criteria, no search.
        }

        // --- 1. Query Construction (Reintroducing GoldenRecord and logic) ---
        let mut where_clauses = Vec::new();
        
        // 
        
        for (key, value) in criteria.iter() {
            // Sanitize input to prevent Cypher injection attacks
            let sanitized_value = sanitize_cypher_string(value);
            
            // Construct a WHERE clause for each field.
            let clause = match key.as_str() {
                // Fuzzy/Substring search for common fields
                "name" | "first_name" | "last_name" | "address" | "phone" => {
                    // Using CONTAINS for simple substring search on string fields
                    format!("p.{key} CONTAINS '{sanitized_value}'")
                }
                // Exact match fields
                "dob" | "gender" | "patient_status" => {
                    format!("p.{key} = '{sanitized_value}'")
                }
                _ => {
                    // Ignore unrecognized keys
                    continue;
                }
            };
            where_clauses.push(clause);
        }

        // Combine clauses with AND
        let where_clause_str = where_clauses.join(" AND ");

        // Ensure the Cypher query only returns Golden Records.
        let cypher_query = format!(
            "MATCH (p:Patient:GoldenRecord) WHERE {where_clause_str} RETURN p" // Return the node 'p'
        );

        // --- 2. Query Execution ---
        println!("===> Executing demographic search query: {}", cypher_query);

        // Execute the read query using the existing execution method.
        let raw_results_vec_of_value = self.execute_cypher_read(&cypher_query, Value::Null).await
            .map_err(|e| anyhow::anyhow!("Graph search query failed: {}", e))?;
        
        // Extract the actual result set (which is the first/only element of the returned vector)
        let raw_result_set = raw_results_vec_of_value.into_iter().next()
            .context("QueryExecEngine returned empty result vector.")?;

        // --- 3. Result Parsing ---
        // The raw_result_set (Value) should contain an array of patient nodes/maps.
        let results_array = raw_result_set.as_array()
            .context("Expected Cypher result to be an array for patient parsing.")?;

        let mut patients = Vec::new();
        for result_item in results_array {
            // result_item is the 'p' node map returned by the Cypher query
            match parse_patient_from_cypher_result(result_item.clone()) {
                Ok(patient) => patients.push(patient),
                Err(e) => log::warn!("Failed to parse patient from Cypher result: {}", e),
            }
        }

        Ok(patients)
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
        println!("===> in execute_cypher_write - query was: {:?}", query);
        engine.execute_cypher(query)
            .await
            .map_err(|e| GraphError::QueryExecutionError(format!("Cypher WRITE failed: {}", e)))
            .map(|value| vec![value]) // Wrap the single result in a vector
    }

    /// Finds a single Patient vertex by its Medical Record Number (MRN) using a Cypher query.
    pub async fn get_patient_by_mrn(&self, mrn: &str) -> Result<Option<Vertex>, GraphError> {
        
        // Construct the Cypher query
        let query = format!(
            "MATCH (p:Patient {{mrn: '{}'}}) RETURN p",
            // Use basic escaping for the single quote within the MRN value
            mrn.replace('\'', "\\'") 
        );

        // Execute the read query.
        let results_vec = self.execute_cypher_read(&query, json!({})).await?;
        
        // The immediate Vec<Value> (results_vec) returned by execute_cypher_read 
        // should contain the overall Cypher JSON response as its first (and usually only) element.
        let cypher_json_value = results_vec.into_iter().next()
            .ok_or_else(|| GraphError::DeserializationError("Cypher execution returned an empty top-level array.".into()))?;

        // --- Start Robust Parsing of the Query Engine Result Structure ---
        
        // 1. Get the 'results' array from the top-level JSON object.
        let results_array = cypher_json_value.as_object()
            .and_then(|obj| obj.get("results"))
            .and_then(Value::as_array)
            .ok_or_else(|| GraphError::DeserializationError("Cypher response missing top-level 'results' array.".into()))?;

        // 2. Get the first result block (which contains vertices/edges/stats).
        let first_result_block = results_array.get(0)
            .ok_or_else(|| GraphError::DeserializationError("Cypher 'results' array was empty.".into()))?;

        // 3. Extract the 'vertices' array (the list of matched nodes).
        let vertices = first_result_block.as_object()
            .and_then(|obj| obj.get("vertices"))
            .and_then(Value::as_array)
            .ok_or_else(|| GraphError::DeserializationError("Cypher result block missing 'vertices' array.".into()))?;

        // >>> CRITICAL FIX: Gracefully handle the empty match. <<<
        if vertices.is_empty() {
            return Ok(None);
        }
        
        // 4. Check for multiple matches (unexpected for MRN lookup).
        if vertices.len() > 1 {
            warn!("MRN lookup returned {} vertices. Returning the first one.", vertices.len());
        }

        // 5. Extract the first vertex JSON value.
        let vertex_json_value = vertices.get(0)
            .ok_or_else(|| GraphError::DeserializationError("Vertices array was unexpectedly empty.".into()))?;

        // 6. Convert the extracted node structure into our Rust Vertex model.
        // We use serde_json::from_value to safely deserialize.
        match serde_json::from_value(vertex_json_value.clone()) {
            Ok(vertex) => Ok(Some(vertex)),
            Err(e) => {
                error!("Failed to deserialize JSON to Vertex: {}", e);
                Err(GraphError::DeserializationError(format!("Failed to parse vertex JSON: {}", e)))
            }
        }
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
    
    // In graph_service.rs or a storage trait:
    pub async fn get_vertex_by_internal_id(&self, internal_id: i32) -> GraphResult<Vertex> {
        
        // 1. Look up the UUID from the internal_id using the Cypher query approach
        let query = format!("MATCH (p:Patient {{id: {}}}) RETURN p", internal_id);
        
        let result = self.execute_cypher_read(&query, serde_json::Value::Null)
            .await
            .map_err(|e| {
                let id_str = format!("internal_id_{}", internal_id);
                GraphError::NotFound(unsafe { Identifier::new_unchecked(id_str) })
            })?;
        
        // Parse the result to extract the UUID
        if let Some(outer_wrapper) = result.into_iter().next() {
            let vertex_data = outer_wrapper
                .get("results")
                .and_then(|r| r.as_array())
                .and_then(|a| a.get(0))
                .and_then(|r| r.get("vertices"))
                .and_then(|v| v.as_array())
                .and_then(|a| a.get(0));
                
            if let Some(v) = vertex_data {
                let uuid_str = v.get("id")
                    .and_then(|u| u.as_str())
                    .ok_or_else(|| {
                        let id_str = format!("internal_id_{}", internal_id);
                        GraphError::NotFound(unsafe { Identifier::new_unchecked(id_str) })
                    })?;
                
                let uuid = Uuid::parse_str(uuid_str)
                    .map_err(|_| {
                        let id_str = format!("internal_id_{}", internal_id);
                        GraphError::NotFound(unsafe { Identifier::new_unchecked(id_str) })
                    })?;
                
                // 2. Retrieve the vertex using the UUID
                match self.storage.get_vertex(&uuid).await? {
                    Some(vertex) => Ok(vertex),
                    None => {
                        let id_str = format!("internal_id_{}", internal_id);
                        Err(GraphError::NotFound(unsafe { Identifier::new_unchecked(id_str) }))
                    }
                }
            } else {
                let id_str = format!("internal_id_{}", internal_id);
                Err(GraphError::NotFound(unsafe { Identifier::new_unchecked(id_str) }))
            }
        } else {
            let id_str = format!("internal_id_{}", internal_id);
            Err(GraphError::NotFound(unsafe { Identifier::new_unchecked(id_str) }))
        }
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
