// src/graph.rs
//! Core in-memory graph with real-time observer callbacks
//! Used by medical_knowledge for instant clinical intelligence

use crate::{Vertex, Edge};
use uuid::Uuid;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::RwLock;

/// Type alias for vertex observers
pub type VertexObserver = Arc<dyn Fn(&Vertex) + Send + Sync>;
/// Type alias for edge observers
pub type EdgeObserver = Arc<dyn Fn(&Edge) + Send + Sync>;

pub struct Graph {
    pub vertices: HashMap<Uuid, Vertex>,
    pub edges: HashMap<Uuid, Edge>,
    pub out_edges: HashMap<Uuid, HashSet<Uuid>>,
    pub in_edges: HashMap<Uuid, HashSet<Uuid>>,

    // Observers — medical_knowledge will register here
    vertex_observers: Arc<RwLock<Vec<VertexObserver>>>,
    edge_observers: Arc<RwLock<Vec<EdgeObserver>>>,
}

impl Graph {
    pub fn new() -> Self {
        Self {
            vertices: HashMap::new(),
            edges: HashMap::new(),
            out_edges: HashMap::new(),
            in_edges: HashMap::new(),
            vertex_observers: Arc::new(RwLock::new(Vec::new())),
            edge_observers: Arc::new(RwLock::new(Vec::new())),
        }
    }

    /// Register a vertex observer (called on every vertex addition)
    pub async fn on_vertex_added<F>(&mut self, callback: F)
    where
        F: Fn(&Vertex) + Send + Sync + 'static,
    {
        self.vertex_observers.write().await.push(Arc::new(callback));
    }

    /// Register an edge observer (called on every edge addition)
    pub async fn on_edge_added<F>(&mut self, callback: F)
    where
        F: Fn(&Edge) + Send + Sync + 'static,
    {
        self.edge_observers.write().await.push(Arc::new(callback));
    }

    pub fn add_vertex(&mut self, vertex: Vertex) {
        let vertex_id = vertex.id.0;
        let vertex_ref = vertex.clone();
        self.vertices.insert(vertex_id, vertex);

        // Notify all vertex observers
        let observers = self.vertex_observers.clone();
        tokio::spawn(async move {
            for observer in observers.read().await.iter() {
                observer(&vertex_ref);
            }
        });
    }

    pub fn add_edge(&mut self, edge: Edge) {
        let edge_id = edge.id.0;
        let from = edge.outbound_id.0;
        let to = edge.inbound_id.0;
        let edge_ref = edge.clone();

        self.edges.insert(edge_id, edge);
        self.out_edges.entry(from).or_default().insert(edge_id);
        self.in_edges.entry(to).or_default().insert(edge_id);

        // Notify all edge observers
        let observers = self.edge_observers.clone();
        tokio::spawn(async move {
            for observer in observers.read().await.iter() {
                observer(&edge_ref);
            }
        });
    }

    pub fn get_vertex(&self, id: &Uuid) -> Option<&Vertex> {
        self.vertices.get(id)
    }

    pub fn outgoing_edges(&self, id: &Uuid) -> impl Iterator<Item = &Edge> {
        self.out_edges.get(id)
            .into_iter()
            .flatten()
            .filter_map(|edge_id| self.edges.get(edge_id))
    }

    pub fn incoming_edges(&self, id: &Uuid) -> impl Iterator<Item = &Edge> {
        self.in_edges.get(id)
            .into_iter()
            .flatten()
            .filter_map(|edge_id| self.edges.get(edge_id))
    }

    /// Get a clone of the graph for read-only use (e.g. in medical_knowledge)
    pub fn clone_for_observation(&self) -> GraphReadOnly {
        GraphReadOnly {
            vertices: self.vertices.clone(),
            edges: self.edges.clone(),
            out_edges: self.out_edges.clone(),
            in_edges: self.in_edges.clone(),
        }
    }
}

/// Read-only snapshot for observers (prevents holding locks)
#[derive(Clone)]
pub struct GraphReadOnly {
    vertices: HashMap<Uuid, Vertex>,
    edges: HashMap<Uuid, Edge>,
    out_edges: HashMap<Uuid, HashSet<Uuid>>,
    in_edges: HashMap<Uuid, HashSet<Uuid>>,
}

impl GraphReadOnly {
    pub fn get_vertex(&self, id: &Uuid) -> Option<&Vertex> {
        self.vertices.get(id)
    }

    pub fn outgoing_edges(&self, id: &Uuid) -> impl Iterator<Item = &Edge> {
        self.out_edges.get(id)
            .into_iter()
            .flatten()
            .filter_map(|edge_id| self.edges.get(edge_id))
    }

    pub fn incoming_edges(&self, id: &Uuid) -> impl Iterator<Item = &Edge> {
        self.in_edges.get(id)
            .into_iter()
            .flatten()
            .filter_map(|edge_id| self.edges.get(edge_id))
    }
}
