// graph_engine/src/graph_service.rs
//! Global singleton GraphService — the single source of truth for the entire graph
//! Use via GraphService::get().await anywhere in the system

use crate::graph::Graph;
use std::sync::Arc;
use tokio::sync::{OnceCell, RwLock};

/// Global singleton — use via GraphService::get().await
pub static GRAPH_SERVICE: OnceCell<Arc<GraphService>> = OnceCell::const_new();

#[derive(Clone)]
pub struct GraphService {
    graph: Arc<RwLock<Graph>>,
}

impl GraphService {
    /// Initialize the global singleton — call once at startup
    pub async fn global_init(graph: Graph) -> Result<(), &'static str> {
        let service = Arc::new(Self {
            graph: Arc::new(RwLock::new(graph)),
        });

        GRAPH_SERVICE
            .set(service)
            .map_err(|_| "GraphService already initialized")
    }

    /// Get the global singleton instance — this is what you call everywhere
    pub async fn get() -> Arc<Self> {
        GRAPH_SERVICE
            .get_or_init(|| async {
                panic!("GraphService not initialized! Call global_init() first.");
            })
            .await
            .clone()
    }

    /// Read-only access to the graph
    pub async fn read(&self) -> tokio::sync::RwLockReadGuard<'_, Graph> {
        self.graph.read().await
    }

    /// Write access to the graph
    pub async fn write(&self) -> tokio::sync::RwLockWriteGuard<'_, Graph> {
        self.graph.write().await
    }

    /// Direct access to the inner graph (for observer registration)
    pub fn inner(&self) -> Arc<RwLock<Graph>> {
        self.graph.clone()
    }
}
