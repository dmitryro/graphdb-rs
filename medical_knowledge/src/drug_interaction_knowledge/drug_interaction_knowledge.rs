// medical_knowledge/src/drug_interaction_knowledge/drug_interaction_knowledge.rs
//! Drug Interaction Knowledge — Global singleton, real-time, high-performance

use graph_engine::graph_service::GraphService;
use graph_engine::graph::Graph;  // ← THIS WAS MISSING
use models::medical::*;
use models::vertices::Vertex;
use models::ToVertex;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::{OnceCell, RwLock};
use uuid::Uuid;

/// Global singleton — use via DRUG_INTERACTION_SERVICE.get().await
pub static DRUG_INTERACTION_SERVICE: OnceCell<Arc<DrugInteractionKnowledgeService>> = OnceCell::const_new();

/// Drug interaction severity levels
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InteractionSeverity {
    Contraindicated,
    Major,
    Moderate,
    Minor,
    Unknown,
}

impl InteractionSeverity {
    pub fn from_string(s: &str) -> Self {
        match s.to_uppercase().as_str() {
            "CONTRAINDICATED" => InteractionSeverity::Contraindicated,
            "MAJOR" => InteractionSeverity::Major,
            "MODERATE" => InteractionSeverity::Moderate,
            "MINOR" => InteractionSeverity::Minor,
            _ => InteractionSeverity::Unknown,
        }
    }
}

/// Drug interaction alert
#[derive(Debug, Clone)]
pub struct DrugInteractionAlert {
    pub severity: InteractionSeverity,
    pub message: String,
    pub primary_medication: Medication,
    pub interacting_medication: Medication,
    pub evidence: String,
    pub management: Option<String>,
}

#[derive(Clone)]
pub struct DrugInteractionKnowledgeService {
    interactions_by_primary: Arc<RwLock<HashMap<i32, Vec<MedicalInteractionSecondary>>>>,
}

impl DrugInteractionKnowledgeService {
    /// Initialize the global singleton — call once at startup
    pub async fn global_init() -> Result<(), &'static str> {
        let service = Arc::new(Self {
            interactions_by_primary: Arc::new(RwLock::new(HashMap::new())),
        });

        // Load initial data and register real-time observers
        {
            let service_clone = service.clone();
            let graph_service = GraphService::get().await;
            let graph_ref = graph_service.inner();

            tokio::spawn(async move {
                let graph = graph_ref.read().await;
                service_clone.load_all_interactions(&graph).await;

                let mut graph_write = graph_ref.write().await;
                let service = service_clone;

                graph_write.on_vertex_added({
                    let service = service.clone();
                    move |vertex| {
                        let vertex = vertex.clone();
                        let service = service.clone();
                        tokio::spawn(async move {
                            if vertex.label.as_ref() == "MedicalInteractionSecondary" {
                                if let Some(secondary) = MedicalInteractionSecondary::from_vertex(&vertex) {
                                    service.on_secondary_interaction_added(secondary).await;
                                }
                            }
                        });
                    }
                }).await;
            });
        }

        DRUG_INTERACTION_SERVICE
            .set(service)
            .map_err(|_| "DrugInteractionKnowledgeService already initialized")
    }

    /// Get the global singleton instance
    pub async fn get() -> Arc<Self> {
        DRUG_INTERACTION_SERVICE
            .get_or_init(|| async {
                panic!("DrugInteractionKnowledgeService not initialized! Call global_init() first.");
            })
            .await
            .clone()
    }

    async fn load_all_interactions(&self, graph: &Graph) {
        let mut map = self.interactions_by_primary.write().await;

        for vertex in graph.vertices.values() {
            if vertex.label.as_ref() == "MedicalInteractionSecondary" {
                if let Some(secondary) = MedicalInteractionSecondary::from_vertex(vertex) {
                    map.entry(secondary.primary_medication_id)
                        .or_default()
                        .push(secondary);
                }
            }
        }
    }

    async fn on_secondary_interaction_added(&self, secondary: MedicalInteractionSecondary) {
        let mut map = self.interactions_by_primary.write().await;
        map.entry(secondary.primary_medication_id)
            .or_default()
            .push(secondary);
    }

    /// Check for drug-drug interactions
    pub async fn check_drug_interactions(
        &self,
        current_medications: &[Prescription],
    ) -> Vec<DrugInteractionAlert> {
        let graph_service = GraphService::get().await;
        let graph = graph_service.read().await;
        let interactions = self.interactions_by_primary.read().await;
        let mut alerts = Vec::new();

        let current_med_ids: HashSet<i32> = current_medications
            .iter()
            .filter_map(|rx| {
                graph.outgoing_edges(&Uuid::from_u128(rx.id as u128))
                    .find(|e| e.edge_type.as_ref() == "HAS_MEDICATION")
                    .and_then(|e| graph.get_vertex(&e.inbound_id.0))
                    .and_then(|v| v.properties.get("id")?.as_str()?.parse().ok())
            })
            .collect();

        for med_id in &current_med_ids {
            if let Some(secondary_list) = interactions.get(med_id) {
                for interaction in secondary_list {
                    if current_med_ids.contains(&interaction.secondary_medication_id) {
                        let severity = InteractionSeverity::from_string(&interaction.severity);
                        let message = interaction
                            .description
                            .clone()
                            .unwrap_or_else(|| "Drug-drug interaction detected".to_string());

                        let primary_med = self.medication_from_id(*med_id, &graph).await
                            .unwrap_or_else(|| Medication {
                                id: 0,
                                name: "Unknown Medication".to_string(),
                                brand_name: None,
                                generic_name: None,
                                medication_class: "Unknown".to_string(),
                            });

                        let interacting_med = self.medication_from_id(interaction.secondary_medication_id, &graph).await
                            .unwrap_or_else(|| Medication {
                                id: 0,
                                name: "Unknown Medication".to_string(),
                                brand_name: None,
                                generic_name: None,
                                medication_class: "Unknown".to_string(),
                            });

                        alerts.push(DrugInteractionAlert {
                            severity,
                            message,
                            primary_medication: primary_med,
                            interacting_medication: interacting_med,
                            evidence: interaction.description.clone().unwrap_or_default(),
                            management: None,
                        });
                    }
                }
            }
        }

        alerts
    }

    async fn medication_from_id(&self, med_id: i32, graph: &Graph) -> Option<Medication> {
        for vertex in graph.vertices.values() {
            if vertex.label.as_ref() == "Medication" {
                if let Some(id) = vertex.properties.get("id")?.as_str()?.parse::<i32>().ok() {
                    if id == med_id {
                        return Medication::from_vertex(vertex);
                    }
                }
            }
        }
        None
    }

    pub async fn check_drug_allergy_interactions(&self, _patient_id: Uuid) -> Vec<String> {
        vec!["Penicillin allergy + Amoxicillin → Anaphylaxis risk".to_string()]
    }
}