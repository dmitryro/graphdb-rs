// medical_knowledge/src/clinical_notes/clinical_notes.rs
//! Clinical Notes — Global singleton, real-time note management using GraphService

use graph_engine::graph_service::GraphService;
use models::medical::*;
use models::edges::Edge;
use models::identifiers::Identifier;
use models::vertices::Vertex;
use models::ToVertex;
use uuid::Uuid;
use chrono::Utc;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{OnceCell, RwLock};

/// Global singleton — use via CLINICAL_NOTE_SERVICE.get().await
pub static CLINICAL_NOTE_SERVICE: OnceCell<Arc<ClinicalNoteService>> = OnceCell::const_new();

#[derive(Clone)]
pub struct ClinicalNoteService {
    patient_notes: Arc<RwLock<HashMap<Uuid, Vec<ClinicalNote>>>>,
}

impl ClinicalNoteService {
    /// Initialize the global singleton — call once at startup
    pub async fn global_init() -> Result<(), &'static str> {
        let service = Arc::new(Self {
            patient_notes: Arc::new(RwLock::new(HashMap::new())),
        });

        // Register real-time observers on the global GraphService
        {
            let service_clone = service.clone();
            let graph_service = GraphService::get().await;

            tokio::spawn(async move {
                let mut graph = graph_service.write_graph().await;
                let service = service_clone;

                // Observe ClinicalNote vertices
                graph.on_vertex_added({
                    let service = service.clone();
                    move |vertex| {
                        let vertex = vertex.clone();
                        let service = service.clone();
                        tokio::spawn(async move {
                            if vertex.label.as_ref() == "ClinicalNote" {
                                if let Some(note) = ClinicalNote::from_vertex(&vertex) {
                                    service.on_note_added(note, vertex.id.0).await;
                                }
                            }
                        });
                    }
                }).await;

                // Observe HAS_CLINICAL_NOTE edges
                graph.on_edge_added({
                    let service = service.clone();
                    move |edge| {
                        let edge = edge.clone();
                        let service = service.clone();
                        tokio::spawn(async move {
                            if edge.edge_type.as_ref() == "HAS_CLINICAL_NOTE" {
                                service.on_has_note_edge(&edge).await;
                            }
                        });
                    }
                }).await;
            });
        }

        CLINICAL_NOTE_SERVICE
            .set(service)
            .map_err(|_| "ClinicalNoteService already initialized")
    }

    /// Get the global singleton instance — use this everywhere
    pub async fn get() -> Arc<Self> {
        CLINICAL_NOTE_SERVICE
            .get_or_init(|| async {
                panic!("ClinicalNoteService not initialized! Call global_init() first.");
            })
            .await
            .clone()
    }

    /// Add a new clinical note
    pub async fn add_note(
        &self,
        encounter_id: Uuid,
        author_id: Uuid,
        note_text: &str,
        _note_type: &str,
    ) -> ClinicalNote {
        let note = ClinicalNote {
            id: rand::random(),
            patient_id: 0,
            doctor_id: 0,
            note_text: note_text.to_string(),
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };

        let vertex = note.to_vertex();
        let graph_service = GraphService::get().await;
        {
            let mut graph = graph_service.write_graph().await;
            graph.add_vertex(vertex.clone());
            graph.add_edge(Edge::new(
                encounter_id,
                Identifier::new("HAS_CLINICAL_NOTE".to_string()).unwrap(),
                vertex.id.0,
            ));
            graph.add_edge(Edge::new(
                author_id,
                Identifier::new("AUTHORED".to_string()).unwrap(),
                vertex.id.0,
            ));
        }

        note
    }

    /// Real-time: when a note vertex is added
    async fn on_note_added(&self, note: ClinicalNote, note_vertex_id: Uuid) {
        let graph_service = GraphService::get().await;
        let graph = graph_service.read().await;

        let patient_id = graph.incoming_edges(&note_vertex_id)
            .find_map(|e| if e.edge_type.as_ref() == "HAS_CLINICAL_NOTE" {
                graph.incoming_edges(&e.outbound_id.0)
                    .find_map(|ee| if ee.edge_type.as_ref() == "HAS_ENCOUNTER" {
                        Some(ee.outbound_id.0)
                    } else { None })
            } else { None })
            .unwrap_or_default();

        let mut notes = self.patient_notes.write().await;
        notes.entry(patient_id).or_default().push(note);
    }

    /// Real-time: when a HAS_CLINICAL_NOTE edge is added
    async fn on_has_note_edge(&self, edge: &Edge) {
        let graph_service = GraphService::get().await;
        let graph = graph_service.read().await;

        if let Some(vertex) = graph.get_vertex(&edge.inbound_id.0) {
            if let Some(note) = ClinicalNote::from_vertex(vertex) {
                self.on_note_added(note, edge.inbound_id.0).await;
            }
        }
    }

    /// Get all notes for a patient (from real-time cache)
    pub async fn notes_for_patient(&self, patient_id: Uuid) -> Vec<ClinicalNote> {
        let notes = self.patient_notes.read().await;
        notes.get(&patient_id).cloned().unwrap_or_default()
    }
}