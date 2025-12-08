use std::collections::{ HashSet, HashMap };
use std::ops::Deref;
use std::str::FromStr; // Needed for Uuid::from_str
use std::sync::Arc;
use std::result::Result; // Explicitly import standard Result
use tokio::sync::{OnceCell, RwLock};
use uuid::Uuid;
use anyhow::Error; // FIX: Import the standard error type used for GraphResult

// These imports are assumed to exist in the crate structure
use crate::graph_engine::medical::*;
use crate::storage_engine::{StorageEngine, GraphOp};
use models::identifiers::{ Identifier, SerializableInternString };
use models::properties::{ PropertyValue };
use models::medical::*;
use models::graph::Graph;
use models::vertices::Vertex;
use models::edges::Edge;
use models::errors::GraphError; // Retained, but GraphResult uses anyhow::Error

/// Defines the event types for all graph mutations.
/// This is the payload sent to observers.
#[derive(Debug, Clone)]
pub enum GraphEvent {
    InsertVertex(Vertex),
    DeleteVertex(Identifier),
    InsertEdge(Edge),
    DeleteEdge(Identifier),
}

// FIX E0107: Result must take two generic arguments (Success type, Error type).
pub type GraphResult<T> = Result<T, Error>;

/// Type alias for the mutation observer closure.
pub type MutationObserver = Arc<dyn Fn(GraphEvent) + Send + Sync + 'static>;

/// Global singleton for the GraphService, ensuring a single instance.
pub static GRAPH_SERVICE: OnceCell<Arc<GraphService>> = OnceCell::const_new();

#[derive(Clone)]
pub struct GraphService {
    /// The in-memory graph structure, acting as the fast, refreshed view.
    graph: Arc<RwLock<Graph>>,
    /// The StorageEngine is kept only for the initial state replay, not for live mutation persistence.
    storage: Arc<dyn StorageEngine>, 
    /// Field to hold observers for all graph-level mutations/transactions.
    mutation_observers: Arc<RwLock<Vec<MutationObserver>>>,
}

impl GraphService {
    /// Initialise the in-memory graph view by replaying all operations from the StorageEngine.
    pub async fn global_init(
        storage: Arc<dyn StorageEngine>,
    ) -> Result<(), GraphError> {
        let mut graph = Graph::new();
        
        // CRITICAL STEP: Load initial state by replaying operations from the StorageEngine.
        storage.replay_into(&mut graph).await
            .map_err(|e| GraphError::StorageError(e.to_string()))?;

        let service = Arc::new(Self {
            graph: Arc::new(RwLock::new(graph)),
            storage, // Retained for context but not used for mutation persistence
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
        
        // Spawn a task to read the list of observers without blocking the caller.
        tokio::spawn(async move {
            let observers_guard = observers_lock.read().await;
            
            for observer in observers_guard.iter() {
                let event_clone = event.clone();
                let observer_clone = observer.clone();
                
                // Spawn a new task for *each* observer to call its synchronous closure.
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

    /// Converts a local GraphEvent (the result of a successful in-memory mutation)
    /// into a portable GraphOp (the command structure needed for persistence or ZMQ).
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
            // If the command is a mutation, we apply it to memory and notify observers.
            GraphOp::InsertVertex(v) => self.add_vertex(v).await,
            GraphOp::DeleteVertex(id) => self.delete_vertex(id).await,
            GraphOp::InsertEdge(e) => self.add_edge(e).await,
            GraphOp::DeleteEdge(id) => self.delete_edge(id).await,
            
            // Non-mutation commands (like queries, or daemon control ops)
            _ => {
                Ok(())
            }
        }
    }

    // =========================================================================
    // IN-MEMORY MUTATION OPERATIONS (Mutate Memory, Notify)
    // =========================================================================

    pub async fn add_vertex(&self, vertex: Vertex) -> Result<(), GraphError> {
        // 1. Modify in-memory graph
        self.graph.write().await.add_vertex(vertex.clone()); 
        // 2. Notify observers (Persistence Engine/ZMQ Listener will be here)
        self.notify_mutation(GraphEvent::InsertVertex(vertex)); 
        Ok(())
    }

    pub async fn add_edge(&self, edge: Edge) -> Result<(), GraphError> {
        // 1. Modify in-memory graph
        self.graph.write().await.add_edge(edge.clone()); 
        // 2. Notify observers
        self.notify_mutation(GraphEvent::InsertEdge(edge)); 
        Ok(())
    }

    /// Deletes a vertex from memory and fires the mutation hook.
    pub async fn delete_vertex(&self, vertex_id: Identifier) -> Result<(), GraphError> {
        // Identifier.0 is SerializableInternString, which needs to be parsed to Uuid
        let uuid_id = Uuid::from_str(vertex_id.0.as_ref())
            .map_err(|e| GraphError::InternalError(format!("Invalid UUID format for vertex deletion: {}", e)))?;
        
        // 1. Modify in-memory graph (this handles associated edge cleanup)
        self.delete_vertex_from_memory(uuid_id).await?; // Fixed to use uuid_id (Uuid)

        // 2. Notify observers
        self.notify_mutation(GraphEvent::DeleteVertex(vertex_id));
        Ok(())
    }

    /// Deletes an edge from memory and fires the mutation hook.
    pub async fn delete_edge(&self, edge_id: Identifier) -> Result<(), GraphError> {
        // Identifier.0 is SerializableInternString, which needs to be parsed to Uuid
        let edge_id_uuid = Uuid::from_str(edge_id.0.as_ref())
            .map_err(|e| GraphError::InternalError(format!("Invalid UUID format for edge deletion: {}", e)))?;
        
        // Use the parsed Uuid to look up the edge in the graph's internal map
        let edge_opt = { self.graph.read().await.edges.get(&edge_id_uuid).cloned() }; // Fixed lookup key
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
    
    /// Retrieves a single Vertex by its Uuid.
    pub async fn get_vertex(&self, vertex_id: &Uuid) -> Option<Vertex> {
        let graph = self.read().await;
        graph.get_vertex(vertex_id).cloned()
    }

    /// Retrieves vertices by a specific label and property value.
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

        // Closure must be mutable because it mutates `connected_edges`
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

        // Use a temporary HashSet to deduplicate edges if they are both in_ and out_
        let unique_edges: HashSet<Edge> = connected_edges.into_iter().collect();
        unique_edges.into_iter().collect()
    }

    /// Retrieves all vertices connected to a given vertex (neighbors).
    pub async fn get_connected_vertices(&self, vertex_id: Uuid) -> Vec<Vertex> {
        let graph = self.read().await;
        let mut neighbor_ids = HashSet::new();

        // Add outbound neighbors
        if let Some(out_edges) = graph.out_edges.get(&vertex_id) {
            for edge_id in out_edges {
                if let Some(edge) = graph.edges.get(edge_id) {
                    neighbor_ids.insert(edge.inbound_id.0);
                }
            }
        }

        // Add inbound neighbors
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
            .cloned() // Clone the vertex to return it
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

            // Collect all outbound edges
            if let Some(out_edges) = graph.out_edges.remove(&vertex_id) {
                edges_to_remove.extend(out_edges);
            }

            // Collect all inbound edges
            if let Some(in_edges) = graph.in_edges.remove(&vertex_id) {
                edges_to_remove.extend(in_edges);
            }

            // Remove collected edges from the main edge map
            for edge_id in edges_to_remove.iter() {
                if graph.edges.remove(edge_id).is_some() {
                    graph.edge_count = graph.edge_count.saturating_sub(1);
                }
            }
            
            graph.vertex_count = graph.vertex_count.saturating_sub(1);
        }

        Ok(())
    }

    // ===== Observer helpers (kept for backward compatibility) =====
    pub async fn add_vertex_observer<F>(&self, f: F) -> Result<(), GraphError>
    where
        F: Fn(&Vertex) + Send + Sync + 'static,
    {
        // Wrap the old Fn(&Vertex) into the new MutationObserver (Fn(GraphEvent))
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
        // Wrap the old Fn(&Edge) into the new MutationObserver (Fn(GraphEvent))
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
// HELPER FOR INITIALIZATION/RETRIEVAL (Unchanged)
// =========================================================================

/// Helper to ensure the global GraphService is initialized.
pub async fn initialize_graph_service(
    storage: Arc<dyn StorageEngine>,
) -> Result<Arc<GraphService>, GraphError> {
    match GRAPH_SERVICE.get() {
        Some(service) => Ok(service.clone()),
        None => {
            // Service not initialized, attempt one-time initialization.
            GraphService::global_init(storage.clone()).await?;
            
            // Initialization succeeded, retrieve the newly set service
            GRAPH_SERVICE.get()
                .cloned()
                .ok_or_else(|| GraphError::StorageError("Failed to retrieve GraphService after initialization".into()))
        }
    }
}

// This struct holds the mutable state of the graph.
// It should be wrapped in `tokio::sync::Mutex` and then `Arc`
// to enable the thread-safe, mutable access required by your application.
// (i.e., Arc<tokio::sync::Mutex<LockableGraphService>>)
pub struct LockableGraphService {
    // HashMap for fast in-memory edge access by ID
    pub in_memory_edges: HashMap<String, Edge>,
}

impl LockableGraphService {
    /// Initializes a new LockableGraphService instance.
    pub fn new() -> Self {
        LockableGraphService {
            in_memory_edges: HashMap::new(),
        }
    }

    /// Creates a new edge and stores it in the in-memory collection.
    /// This method is called on the mutable reference (`&mut self`) obtained
    /// after successfully locking the Mutex.
    // FIX E0277/E0308: Ensure we convert the Identifier's content (SerializableInternString) 
    // to an owned String to match the HashMap key type, then use a reference to the String 
    // for `contains_key`.
    pub fn create_edge_in_memory(&mut self, edge: Edge) -> GraphResult<()> {
        // Use the inner SerializableInternString and convert it to an owned String key
        let edge_id_key = edge.id.0.to_string(); 
        
        if self.in_memory_edges.contains_key(&edge_id_key) {
            eprintln!("Warning: Edge with ID {} already exists. Overwriting.", edge_id_key);
        }
        // Insert or overwrite the edge
        self.in_memory_edges.insert(edge_id_key, edge);
        Ok(())
    }
}