// medical_knowledge/src/patient_journey/patient_journey.rs
//! Patient Journey Intelligence — Real-time, longitudinal care pathways

use graph_engine::graph_service::GraphService;
use models::medical::*;
use models::vertices::Vertex;
use models::edges::Edge;
use models::graph::Graph;
use models::identifiers::Identifier;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::{OnceCell, RwLock};
use uuid::Uuid;
use chrono::{DateTime, Utc, Duration};

/// Global singleton — use via PATIENT_JOURNEY_SERVICE.get().await
pub static PATIENT_JOURNEY_SERVICE: OnceCell<Arc<PatientJourneyService>> = OnceCell::const_new();

#[derive(Debug, Clone)]
pub struct JourneyMilestone {
    pub name: String,
    pub achieved_at: DateTime<Utc>,
    pub encounter_id: Option<Uuid>,
    pub achieved: bool,
    pub overdue: bool,
}

#[derive(Debug, Clone)]
pub struct CarePathway {
    pub name: String,
    pub milestones: Vec<String>,
    pub expected_duration_days: i64,
}

#[derive(Debug, Clone)]
pub struct PatientJourney {
    pub patient_id: Uuid,
    pub active_pathways: Vec<ActivePathway>,
    pub completed_pathways: Vec<String>,
    pub deviations: Vec<JourneyDeviation>,
    pub current_milestones: Vec<JourneyMilestone>,
}

#[derive(Debug, Clone)]
pub struct ActivePathway {
    pub pathway: CarePathway,
    pub started_at: DateTime<Utc>,
    pub current_milestone_index: usize,
    pub expected_completion: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct JourneyDeviation {
    pub pathway: String,
    pub expected_milestone: String,
    pub expected_by: DateTime<Utc>,
    pub actual_status: String,
    pub days_overdue: i64,
}

#[derive(Clone)]
pub struct PatientJourneyService {
    journeys: Arc<RwLock<HashMap<Uuid, PatientJourney>>>,
    pathways: Arc<HashMap<String, CarePathway>>,
}

impl PatientJourneyService {
    /// Initialize the global singleton — call once at startup
    pub async fn global_init() -> Result<(), &'static str> {
        let service = Arc::new(Self {
            journeys: Arc::new(RwLock::new(HashMap::new())),
            pathways: Arc::new(Self::load_standard_pathways()),
        });

        // Build initial journeys
        {
            let graph_service = GraphService::get().await;
            let graph = graph_service.read().await;

            for vertex in graph.vertices.values() {
                if vertex.label.as_ref() == "Patient" {
                    if let Some(_patient) = Patient::from_vertex(vertex) {
                        let patient_id = vertex.id.0;
                        let mut journey = PatientJourney {
                            patient_id,
                            active_pathways: Vec::new(),
                            completed_pathways: Vec::new(),
                            deviations: Vec::new(),
                            current_milestones: Vec::new(),
                        };

                        service.auto_start_pathways(&mut journey, &graph).await;
                        service.journeys.write().await.insert(patient_id, journey);
                    }
                }
            }
        }

        // Register real-time observers
        {
            let service_clone = service.clone();
            let graph_service = GraphService::get().await;
            tokio::spawn(async move {
                let mut graph = graph_service.write_graph().await;
                let service = service_clone;

                graph.on_vertex_added({
                    let service = service.clone();
                    move |vertex| {
                        let vertex = vertex.clone();
                        let service = service.clone();
                        tokio::spawn(async move {
                            if vertex.label.as_ref() == "Diagnosis" {
                                if let Some(dx) = Diagnosis::from_vertex(&vertex) {
                                    service.on_diagnosis_added(dx).await;
                                }
                            }
                            if vertex.label.as_ref() == "Encounter" {
                                if let Some(enc) = Encounter::from_vertex(&vertex) {
                                    service.on_encounter_updated(enc).await;
                                }
                            }
                        });
                    }
                }).await;
            });
        }

        PATIENT_JOURNEY_SERVICE
            .set(service)
            .map_err(|_| "PatientJourneyService already initialized")
    }

    pub async fn get() -> Arc<Self> {
        PATIENT_JOURNEY_SERVICE.get().unwrap().clone()
    }

    // =========================================================================
    // STANDARD PATHWAYS
    // =========================================================================

    fn load_standard_pathways() -> HashMap<String, CarePathway> {
        let mut map = HashMap::new();

        map.insert("Stroke Pathway".to_string(), CarePathway {
            name: "Stroke Pathway".to_string(),
            milestones: vec![
                "tPA administered".to_string(),
                "CT scan completed".to_string(),
                "Neurology consult".to_string(),
                "Admit to stroke unit".to_string(),
                "Swallow screen".to_string(),
                "Rehab assessment".to_string(),
            ],
            expected_duration_days: 2,
        });

        map.insert("Sepsis-6".to_string(), CarePathway {
            name: "Sepsis-6".to_string(),
            milestones: vec![
                "Lactate measured".to_string(),
                "Blood cultures taken".to_string(),
                "Broad-spectrum antibiotics".to_string(),
                "IV fluids 30ml/kg".to_string(),
                "Oxygen if hypotensive".to_string(),
                "Urine output monitoring".to_string(),
            ],
            expected_duration_days: 1,
        });

        map.insert("Postoperative Care".to_string(), CarePathway {
            name: "Postoperative Care".to_string(),
            milestones: vec![
                "PACU monitoring".to_string(),
                "Pain controlled".to_string(),
                "Mobilization day 1".to_string(),
                "DVT prophylaxis".to_string(),
                "Remove catheter day 1".to_string(),
            ],
            expected_duration_days: 3,
        });

        map.insert("Heart Failure Pathway".to_string(), CarePathway {
            name: "Heart Failure Pathway".to_string(),
            milestones: vec![
                "BNP measured".to_string(),
                "Echo completed".to_string(),
                "ACEi/ARB started".to_string(),
                "Beta blocker started".to_string(),
                "Diuretic adjusted".to_string(),
                "Weight daily".to_string(),
                "Discharge education".to_string(),
            ],
            expected_duration_days: 5,
        });

        map
    }

    // =========================================================================
    // REAL-TIME EVENT HANDLERS
    // =========================================================================

    async fn on_diagnosis_added(&self, diagnosis: Diagnosis) {
        let graph_service = GraphService::get().await;
        let graph = graph_service.read().await;

        let patient_id = graph.incoming_edges(&Uuid::from_u128(diagnosis.id as u128))
            .find_map(|e| if e.edge_type.as_ref() == "HAS_DIAGNOSIS" {
                Some(e.outbound_id.0)
            } else { None })
            .unwrap_or(Uuid::nil());

        if patient_id == Uuid::nil() {
            return;
        }

        let mut journeys = self.journeys.write().await;
        let journey = journeys.entry(patient_id).or_insert_with(|| PatientJourney {
            patient_id,
            active_pathways: Vec::new(),
            completed_pathways: Vec::new(),
            deviations: Vec::new(),
            current_milestones: Vec::new(),
        });

        self.auto_start_pathways(journey, &graph).await;
        self.check_milestone_progress(journey, &diagnosis.description).await;
    }

    async fn on_encounter_updated(&self, encounter: Encounter) {
        let graph_service = GraphService::get().await;
        let graph = graph_service.read().await;

        let patient_id = graph.incoming_edges(&Uuid::from_u128(encounter.id as u128))
            .find_map(|e| if e.edge_type.as_ref() == "HAS_ENCOUNTER" {
                Some(e.outbound_id.0)
            } else { None })
            .unwrap_or(Uuid::nil());

        if patient_id == Uuid::nil() {
            return;
        }

        let mut journeys = self.journeys.write().await;
        if let Some(journey) = journeys.get_mut(&patient_id) {
            self.check_milestone_progress(journey, &encounter.encounter_type).await;
        }
    }

    // =========================================================================
    // CORE LOGIC
    // =========================================================================

    async fn auto_start_pathways(&self, journey: &mut PatientJourney, graph: &Graph) {
        let diagnoses: HashSet<String> = graph.outgoing_edges(&journey.patient_id)
            .filter(|e| e.edge_type.as_ref() == "HAS_DIAGNOSIS")
            .filter_map(|e| graph.get_vertex(&e.inbound_id.0))
            .filter_map(|v| Diagnosis::from_vertex(v))
            .map(|d| d.description.to_lowercase())
            .collect();

        for (name, pathway) in &*self.pathways {
            let should_start = match name.as_str() {
                "Stroke Pathway" => diagnoses.contains("stroke") || diagnoses.contains("cva"),
                "Sepsis-6" => diagnoses.contains("sepsis") || diagnoses.contains("severe sepsis"),
                "Heart Failure Pathway" => diagnoses.contains("heart failure") || diagnoses.contains("chf"),
                "Postoperative Care" => diagnoses.contains("postoperative") || diagnoses.contains("s/p"),
                _ => false,
            };

            if should_start && !journey.active_pathways.iter().any(|p| p.pathway.name == *name) {
                journey.active_pathways.push(ActivePathway {
                    pathway: pathway.clone(),
                    started_at: Utc::now(),
                    current_milestone_index: 0,
                    expected_completion: Utc::now() + Duration::days(pathway.expected_duration_days),
                });
            }
        }
    }

    async fn check_milestone_progress(&self, journey: &mut PatientJourney, trigger: &str) {
        let trigger_lower = trigger.to_lowercase();

        for active in &mut journey.active_pathways {
            let current_idx = active.current_milestone_index;
            if current_idx >= active.pathway.milestones.len() {
                continue;
            }

            let expected = &active.pathway.milestones[current_idx];
            if trigger_lower.contains(&expected.to_lowercase()) {
                let milestone = JourneyMilestone {
                    name: expected.clone(),
                    achieved_at: Utc::now(),
                    encounter_id: None,
                    achieved: true,
                    overdue: false,
                };
                journey.current_milestones.push(milestone);
                active.current_milestone_index += 1;

                if active.current_milestone_index >= active.pathway.milestones.len() {
                    journey.completed_pathways.push(active.pathway.name.clone());
                }
            }

            // Overdue check
            if Utc::now() > active.expected_completion && active.current_milestone_index < active.pathway.milestones.len() {
                let overdue_milestone = &active.pathway.milestones[active.current_milestone_index];
                journey.deviations.push(JourneyDeviation {
                    pathway: active.pathway.name.clone(),
                    expected_milestone: overdue_milestone.clone(),
                    expected_by: active.expected_completion,
                    actual_status: "Not achieved".to_string(),
                    days_overdue: (Utc::now() - active.expected_completion).num_days(),
                });
            }
        }
    }

    // =========================================================================
    // PUBLIC API
    // =========================================================================

    pub async fn get_journey(&self, patient_id: Uuid) -> PatientJourney {
        let journeys = self.journeys.read().await;
        journeys.get(&patient_id)
            .cloned()
            .unwrap_or_else(|| PatientJourney {
                patient_id,
                active_pathways: Vec::new(),
                completed_pathways: Vec::new(),
                deviations: Vec::new(),
                current_milestones: Vec::new(),
            })
    }

    pub async fn active_pathways(&self, patient_id: Uuid) -> Vec<ActivePathway> {
        let journeys = self.journeys.read().await;
        journeys.get(&patient_id)
            .map(|j| j.active_pathways.clone())
            .unwrap_or_default()
    }

    pub async fn deviations(&self, patient_id: Uuid) -> Vec<JourneyDeviation> {
        let journeys = self.journeys.read().await;
        journeys.get(&patient_id)
            .map(|j| j.deviations.clone())
            .unwrap_or_default()
    }
}