// medical_knowledge/src/clinical_encounters/clinical_encounters.rs
//! Clinical Encounters — Global singleton, real-time clinical intelligence using GraphService

use graph_engine::graph_service::GraphService;
use models::medical::*;
use models::edges::Edge;
use models::identifiers::Identifier;
use models::vertices::Vertex;
use models::ToVertex;
use uuid::Uuid;
use chrono::{DateTime, Utc, Duration};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::{OnceCell, RwLock};

/// Global singleton — use via CLINICAL_ENCOUNTER_SERVICE.get().await
pub static CLINICAL_ENCOUNTER_SERVICE: OnceCell<Arc<ClinicalEncounterService>> = OnceCell::const_new();

#[derive(Debug, Clone, Default)]
pub struct PatientTimeline {
    pub patient_id: Uuid,
    pub entries: Vec<TimelineEntry>,
}

impl PatientTimeline {
    pub fn new(patient_id: Uuid) -> Self {
        Self { patient_id, entries: Vec::new() }
    }
}

#[derive(Debug, Clone)]
pub struct TimelineEntry {
    pub date: DateTime<Utc>,
    pub encounter_type: String,
    pub doctor: Option<Doctor>,
    pub diagnoses: Vec<Diagnosis>,
    pub prescriptions: Vec<Prescription>,
    pub vitals: Vec<Vitals>,
    pub observations: Vec<Observation>,
    pub notes: Option<String>,
}

#[derive(Debug, Clone)]
pub struct DrugAlert {
    pub severity: String,
    pub message: String,
    pub medications: Vec<Prescription>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CareGap {
    ColonCancerScreening,
    BreastCancerScreening,
    DiabetesScreening,
    HypertensionScreening,
    VaccinationDue,
}

#[derive(Clone)]
pub struct ClinicalEncounterService {
    patient_timelines: Arc<RwLock<HashMap<Uuid, PatientTimeline>>>,
    active_problems: Arc<RwLock<HashMap<Uuid, Vec<Diagnosis>>>>,
    current_medications: Arc<RwLock<HashMap<Uuid, Vec<Prescription>>>>,
}

impl ClinicalEncounterService {
    /// Initialize the global singleton — call once at startup
    pub async fn global_init() -> Result<(), &'static str> {
        let service = Arc::new(Self {
            patient_timelines: Arc::new(RwLock::new(HashMap::new())),
            active_problems: Arc::new(RwLock::new(HashMap::new())),
            current_medications: Arc::new(RwLock::new(HashMap::new())),
        });

        // Register real-time observers on the global GraphService
        {
            let service_clone = service.clone();
            let graph_service = GraphService::get().await;
            let graph_ref = graph_service.inner();

            tokio::spawn(async move {
                let mut graph = graph_ref.write().await;
                let service = service_clone;

                // Vertex observers
                graph.on_vertex_added({
                    let service = service.clone();
                    move |vertex| {
                        let vertex = vertex.clone();
                        let service = service.clone();
                        tokio::spawn(async move {
                            if vertex.label.as_ref() == "Encounter" {
                                if let Some(encounter) = Encounter::from_vertex(&vertex) {
                                    service.on_encounter_added(encounter, vertex.id.0).await;
                                }
                            }
                            if vertex.label.as_ref() == "Diagnosis" {
                                if let Some(diagnosis) = Diagnosis::from_vertex(&vertex) {
                                    service.on_diagnosis_added(diagnosis).await;
                                }
                            }
                            if vertex.label.as_ref() == "Prescription" {
                                if let Some(rx) = Prescription::from_vertex(&vertex) {
                                    service.on_prescription_added(rx).await;
                                }
                            }
                        });
                    }
                }).await;

                // Edge observers
                graph.on_edge_added({
                    let service = service.clone();
                    move |edge| {
                        let edge = edge.clone();
                        let service = service.clone();
                        tokio::spawn(async move {
                            if edge.edge_type.as_ref() == "HAS_ENCOUNTER" {
                                service.on_has_encounter_edge(&edge).await;
                            }
                            if edge.edge_type.as_ref() == "HAS_DIAGNOSIS" {
                                service.on_has_diagnosis_edge(&edge).await;
                            }
                            if edge.edge_type.as_ref() == "TAKES_MEDICATION" {
                                service.on_takes_medication_edge(&edge).await;
                            }
                        });
                    }
                }).await;
            });
        }

        CLINICAL_ENCOUNTER_SERVICE
            .set(service)
            .map_err(|_| "ClinicalEncounterService already initialized")
    }

    /// Get the global singleton instance — use this everywhere
    pub async fn get() -> Arc<Self> {
        CLINICAL_ENCOUNTER_SERVICE
            .get_or_init(|| async {
                panic!("ClinicalEncounterService not initialized! Call global_init() first.");
            })
            .await
            .clone()
    }

    // CORE WORKFLOW — uses global GraphService
    pub async fn start_encounter(
        &self,
        patient_id: Uuid,
        doctor_id: Uuid,
        encounter_type: &str,
        location: Option<&str>,
    ) -> Encounter {
        let encounter = Encounter {
            id: rand::random(),
            patient_id: 0,
            doctor_id: 0,
            encounter_type: encounter_type.to_string(),
            date: Utc::now(),
            notes: location.map(|s| s.to_string()),
        };

        let vertex = encounter.to_vertex();
        let graph_service = GraphService::get().await;
        {
            let mut graph = graph_service.write().await;
            graph.add_vertex(vertex.clone());
            graph.add_edge(Edge::new(
                patient_id,
                Identifier::new("HAS_ENCOUNTER".to_string()).unwrap(),
                vertex.id.0,
            ));
            graph.add_edge(Edge::new(
                doctor_id,
                Identifier::new("PROVIDES_CARE_IN".to_string()).unwrap(),
                vertex.id.0,
            ));
        }

        encounter
    }

    pub async fn add_diagnosis(&self, encounter_id: Uuid, description: &str) -> Diagnosis {
        let diagnosis = Diagnosis {
            id: rand::random(),
            patient_id: 0,
            doctor_id: 0,
            code_id: 0,
            description: description.to_string(),
            date: Utc::now().date_naive(),
        };

        let vertex = diagnosis.to_vertex();
        let graph_service = GraphService::get().await;
        {
            let mut graph = graph_service.write().await;
            graph.add_vertex(vertex.clone());
            graph.add_edge(Edge::new(
                encounter_id,
                Identifier::new("HAS_DIAGNOSIS".to_string()).unwrap(),
                vertex.id.0,
            ));
        }

        diagnosis
    }

    pub async fn prescribe_medication(
        &self,
        encounter_id: Uuid,
        medication_name: &str,
        dose: &str,
        frequency: &str,
        duration_days: i64,
    ) -> Prescription {
        let rx = Prescription {
            id: rand::random(),
            patient_id: 0,
            doctor_id: 0,
            medication_name: medication_name.to_string(),
            dose: dose.to_string(),
            frequency: frequency.to_string(),
            start_date: Utc::now(),
            end_date: Some(Utc::now() + Duration::days(duration_days)),
        };

        let vertex = rx.to_vertex();
        let graph_service = GraphService::get().await;
        {
            let mut graph = graph_service.write().await;
            graph.add_vertex(vertex.clone());
            graph.add_edge(Edge::new(
                encounter_id,
                Identifier::new("HAS_PRESCRIPTION".to_string()).unwrap(),
                vertex.id.0,
            ));

            let patient_id = self.patient_id_from_encounter(encounter_id).await;
            graph.add_edge(Edge::new(
                patient_id,
                Identifier::new("TAKES_MEDICATION".to_string()).unwrap(),
                vertex.id.0,
            ));
        }

        rx
    }

    // REAL-TIME EVENT HANDLERS
    async fn on_encounter_added(&self, encounter: Encounter, vertex_id: Uuid) {
        let patient_id = self.patient_id_from_encounter(vertex_id).await;
        let mut timelines = self.patient_timelines.write().await;
        let timeline = timelines.entry(patient_id).or_default();
        timeline.entries.push(TimelineEntry {
            date: encounter.date,
            encounter_type: encounter.encounter_type.clone(),
            doctor: self.doctor_for_encounter(vertex_id).await,
            diagnoses: Vec::new(),
            prescriptions: Vec::new(),
            vitals: Vec::new(),
            observations: Vec::new(),
            notes: encounter.notes.clone(),
        });
        timeline.entries.sort_by_key(|e| e.date);
    }

    async fn on_diagnosis_added(&self, diagnosis: Diagnosis) {
        let graph_service = GraphService::get().await;
        let graph = graph_service.read().await;
        if let Some(edge) = graph.incoming_edges(&Uuid::from_u128(diagnosis.id as u128))
            .find(|e| e.edge_type.as_ref() == "HAS_DIAGNOSIS")
        {
            let mut problems = self.active_problems.write().await;
            problems.entry(edge.outbound_id.0).or_default().push(diagnosis);
        }
    }

    async fn on_prescription_added(&self, rx: Prescription) {
        let graph_service = GraphService::get().await;
        let graph = graph_service.read().await;
        if let Some(edge) = graph.incoming_edges(&Uuid::from_u128(rx.id as u128))
            .find(|e| e.edge_type.as_ref() == "TAKES_MEDICATION")
        {
            let mut meds = self.current_medications.write().await;
            meds.entry(edge.outbound_id.0).or_default().push(rx);
        }
    }

    async fn on_has_encounter_edge(&self, edge: &Edge) {
        let graph_service = GraphService::get().await;
        let graph = graph_service.read().await;
        if let Some(vertex) = graph.get_vertex(&edge.inbound_id.0) {
            if let Some(encounter) = Encounter::from_vertex(vertex) {
                self.on_encounter_added(encounter, edge.inbound_id.0).await;
            }
        }
    }

    async fn on_has_diagnosis_edge(&self, edge: &Edge) {
        let graph_service = GraphService::get().await;
        let graph = graph_service.read().await;
        if let Some(vertex) = graph.get_vertex(&edge.inbound_id.0) {
            if let Some(diagnosis) = Diagnosis::from_vertex(vertex) {
                self.on_diagnosis_added(diagnosis).await;
            }
        }
    }

    async fn on_takes_medication_edge(&self, edge: &Edge) {
        let graph_service = GraphService::get().await;
        let graph = graph_service.read().await;
        if let Some(vertex) = graph.get_vertex(&edge.inbound_id.0) {
            if let Some(rx) = Prescription::from_vertex(vertex) {
                self.on_prescription_added(rx).await;
            }
        }
    }

    // PUBLIC API
    pub async fn patient_timeline(&self, patient_id: Uuid) -> PatientTimeline {
        let timelines = self.patient_timelines.read().await;
        timelines.get(&patient_id)
            .cloned()
            .unwrap_or_else(|| PatientTimeline::new(patient_id))
    }

    pub async fn active_problems(&self, patient_id: Uuid) -> Vec<Diagnosis> {
        let problems = self.active_problems.read().await;
        problems.get(&patient_id).cloned().unwrap_or_default()
    }

    pub async fn current_medications(&self, patient_id: Uuid) -> Vec<Prescription> {
        let meds = self.current_medications.read().await;
        meds.get(&patient_id).cloned().unwrap_or_default()
    }

    // Helper methods
    async fn patient_id_from_encounter(&self, encounter_id: Uuid) -> Uuid {
        let graph_service = GraphService::get().await;
        let graph = graph_service.read().await;
        graph.incoming_edges(&encounter_id)
            .find_map(|e| if e.edge_type.as_ref() == "HAS_ENCOUNTER" { Some(e.outbound_id.0) } else { None })
            .unwrap_or_default()
    }

    async fn doctor_for_encounter(&self, encounter_id: Uuid) -> Option<Doctor> {
        let graph_service = GraphService::get().await;
        let graph = graph_service.read().await;
        graph.incoming_edges(&encounter_id)
            .find_map(|e| if e.edge_type.as_ref() == "PROVIDES_CARE_IN" {
                graph.get_vertex(&e.outbound_id.0).and_then(|v| Doctor::from_vertex(v))
            } else { None })
    }
}