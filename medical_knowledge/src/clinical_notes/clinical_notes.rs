// medical_knowledge/src/clinical_notes/clinical_notes.rs
//! Clinical Notes — Full note management with threading, signing, amendments

use graph_engine::graph::Graph;
use models::medical::*;
use models::edges::Edge;
use models::identifiers::Identifier;
use models::vertices::Vertex;
use models::ToVertex;
use uuid::Uuid;
use chrono::Utc;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Clone)]
pub struct ClinicalNoteService {
    graph: Arc<RwLock<Graph>>,
    patient_notes: Arc<RwLock<HashMap<Uuid, Vec<ClinicalNote>>>>,
}

impl ClinicalNoteService {
    pub fn new(graph: Graph) -> Self {
        let service = Self {
            graph: Arc::new(RwLock::new(graph)),
            patient_notes: Arc::new(RwLock::new(HashMap::new())),
        };

        let service_clone = service.clone();
        let graph_ref = service.graph.clone();
        tokio::spawn(async move {
            let mut graph = graph_ref.write().await;

            graph.on_vertex_added({
                let service = service_clone.clone();
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

            graph.on_edge_added({
                let service = service_clone.clone();
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

        service
    }

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
        {
            let mut graph = self.graph.write().await;
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

    async fn on_note_added(&self, note: ClinicalNote, note_vertex_id: Uuid) {
        let graph = self.graph.read().await;
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

    async fn on_has_note_edge(&self, edge: &Edge) {
        let graph = self.graph.read().await;
        if let Some(vertex) = graph.get_vertex(&edge.inbound_id.0) {
            if let Some(note) = ClinicalNote::from_vertex(vertex) {
                self.on_note_added(note, edge.inbound_id.0).await;
            }
        }
    }

    pub async fn notes_for_patient(&self, patient_id: Uuid) -> Vec<ClinicalNote> {
        let notes = self.patient_notes.read().await;
        notes.get(&patient_id).cloned().unwrap_or_default()
    }
}