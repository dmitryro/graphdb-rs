// graph_engine/src/graph_service.rs
//! Global singleton GraphService — fully persistent using lib's StorageEngine + GraphError

use crate::graph_engine::medical::*;
use crate::storage_engine::{StorageEngine, GraphOp};
use models::medical::*;
use models::graph::Graph;
use models::vertices::Vertex;
use models::edges::Edge;
use models::errors::GraphError;
use std::sync::Arc;
use tokio::sync::{OnceCell, RwLock};
use uuid::Uuid;

/// Global singleton
pub static GRAPH_SERVICE: OnceCell<Arc<GraphService>> = OnceCell::const_new();

#[derive(Clone)]
pub struct GraphService {
    graph: Arc<RwLock<Graph>>,
    storage: Arc<dyn StorageEngine>,
}

impl GraphService {
    /// Initialise with real persistent storage
    pub async fn global_init(
        storage: Arc<dyn StorageEngine>,
    ) -> Result<(), GraphError> {
        let mut graph = Graph::new();
        storage.replay_into(&mut graph).await
            .map_err(|e| GraphError::StorageError(e.to_string()))?;

        let service = Arc::new(Self {
            graph: Arc::new(RwLock::new(graph)),
            storage,
        });

        GRAPH_SERVICE
            .set(service.clone())
            .map_err(|_| GraphError::StorageError("GraphService already initialised".into()))?;
        Ok(())
    }

    pub async fn get() -> Arc<Self> {
        GRAPH_SERVICE.get().unwrap().clone()
    }

    // =========================================================================
    // PERSISTENT OPERATIONS
    // =========================================================================

    pub async fn add_vertex(&self, vertex: Vertex) -> Result<(), GraphError> {
        let op = GraphOp::InsertVertex(vertex.clone());
        self.storage.append(op.clone()).await
            .map_err(|e| GraphError::StorageError(e.to_string()))?;
        self.graph.write().await.add_vertex(vertex);
        Ok(())
    }

    pub async fn add_edge(&self, edge: Edge) -> Result<(), GraphError> {
        let op = GraphOp::InsertEdge(edge.clone());
        self.storage.append(op.clone()).await
            .map_err(|e| GraphError::StorageError(e.to_string()))?;
        self.graph.write().await.add_edge(edge);
        Ok(())
    }

    // =========================================================================
    // READ OPERATIONS
    // =========================================================================

    pub async fn read(&self) -> tokio::sync::RwLockReadGuard<'_, Graph> {
        self.graph.read().await
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
                let last  = v.properties.get("last_name")?.as_str()?.to_lowercase();
                if first.contains(&query) || last.contains(&query) {
                    Patient::from_vertex(v)
                } else {
                    None
                }
            })
            .collect()
    }

    // =====  observer helpers  =====
    pub async fn add_vertex_observer<F>(&self, f: F) -> Result<(), GraphError>
    where
        F: Fn(&Vertex) + Send + Sync + 'static,
    {
        self.graph.write().await.on_vertex_added(f).await;
        Ok(())
    }

    pub async fn add_edge_observer<F>(&self, f: F) -> Result<(), GraphError>
    where
        F: Fn(&Edge) + Send + Sync + 'static,
    {
        self.graph.write().await.on_edge_added(f).await;
        Ok(())
    }

    // =====  helpers for outside crates  =====
    /// Read-only access to the inner `Graph` (replaces `.inner()`).
    pub async fn get_graph(&self) -> tokio::sync::RwLockReadGuard<'_, Graph> {
        self.graph.read().await
    }

    /// Mutable access to the inner `Graph` (replaces `.write()` on `Arc`).
    pub async fn write_graph(&self) -> tokio::sync::RwLockWriteGuard<'_, Graph> {
        self.graph.write().await
    }
}

// =========================================================================
// HELPER FOR INITIALIZATION/RETRIEVAL
// =========================================================================

/// Helper to ensure the global GraphService is initialized.
/// If already initialized, returns the existing instance.
/// If not initialized, calls `GraphService::global_init` using the provided storage.
pub async fn initialize_graph_service(
    storage: Arc<dyn StorageEngine>,
) -> Result<Arc<GraphService>, GraphError> {
    match GRAPH_SERVICE.get() {
        Some(service) => Ok(service.clone()),
        None => {
            // Service not initialized, attempt one-time initialization.
            // We clone storage because global_init takes ownership of the Arc (or needs a clone).
            GraphService::global_init(storage.clone()).await?;
            
            // Initialization succeeded, retrieve the newly set service (now unwrap is safe)
            GRAPH_SERVICE.get()
                .cloned()
                .ok_or_else(|| GraphError::StorageError("Failed to retrieve GraphService after initialization".into()))
        }
    }
}