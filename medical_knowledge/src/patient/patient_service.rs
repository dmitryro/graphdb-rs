// medical_knowledge/src/patient_service/patient_service.rs
//! Patient Service — Global singleton for patient management
//! Handles patient creation, viewing, searching, timeline, problems, meds, care gaps, allergies, referrals, drug alerts

use std::collections::{ HashMap };
use graph_engine::graph_service::GraphService;
use models::medical::{Patient, Problem, Prescription, Allergy, Referral};
use lib::commands::{PatientCommand, JourneyFormat, AlertFormat, AlertSeverity};
use std::sync::Arc;
use tokio::sync::{OnceCell, RwLock};
use uuid::Uuid;
use chrono::Utc;
use serde_json::Value;

// Global singleton
pub static PATIENT_SERVICE: OnceCell<Arc<PatientService>> = OnceCell::const_new();

#[derive(Clone)]
pub struct PatientService {
    // Patient cache (simplified, use graph for persistence)
    patients: Arc<RwLock<HashMap<Uuid, Patient>>>,
}

impl PatientService {
    /// Initialise global singleton
    pub async fn global_init() -> Result<(), &'static str> {
        let service = Arc::new(Self {
            patients: Arc::new(RwLock::new(HashMap::new())),
        });

        // 1. preload existing patients
        let graph_service = GraphService::get().await;
        {
            let graph = graph_service.get_graph().await; // ← &Graph
            let mut patients = service.patients.write().await;
            for vertex in graph.vertices.values() {
                if vertex.label.as_ref() == "Patient" {
                    if let Some(patient) = Patient::from_vertex(vertex) {
                        patients.insert(vertex.id.0, patient);
                    }
                }
            }
        }

        // 2. register observer
        let service_clone = service.clone();
        tokio::spawn(async move {
            graph_service
                .add_vertex_observer(move |vertex| {
                    if vertex.label.as_ref() == "Patient" {
                        let vertex = vertex.clone();
                        let svc = service_clone.clone();
                        tokio::spawn(async move {
                            if let Some(patient) = Patient::from_vertex(&vertex) {
                                svc.add_patient(patient).await;
                            }
                        });
                    }
                })
                .await
                .expect("observer registration failed");
        });

        PATIENT_SERVICE.set(service).map_err(|_| "PatientService already initialized")
    }

    pub async fn get() -> Arc<Self> {
        PATIENT_SERVICE.get().unwrap().clone()
    }

    // =========================================================================
    // PATIENT MANAGEMENT
    // =========================================================================

    async fn add_patient(&self, patient: Patient) {
        let mut patients = self.patients.write().await;
        patients.insert(Uuid::from_u128(patient.id as u128), patient);
    }

    pub async fn create_patient(&self, new_patient: Patient) -> Result<String, String> {
        let mut patient = new_patient;
        patient.id = Uuid::new_v4().as_u128() as i32; // Simplified ID generation
        patient.created_at = Utc::now();
        patient.updated_at = Utc::now();
        self.add_patient(patient.clone()).await;
        Ok(format!("Patient created with ID {}", patient.id))
    }

    pub async fn view_patient(&self, patient_id: i32) -> Result<String, String> {
        let patients = self.patients.read().await;
        patients.get(&Uuid::from_u128(patient_id as u128))
            .map(|p| format!("{:?}", p))
            .ok_or("Patient not found".to_string())
    }

    pub async fn search_patients(&self, query: &str) -> Result<String, String> {
        let patients = self.patients.read().await;
        let results = patients.values().filter(|p| p.first_name.contains(query) || p.last_name.contains(query)).collect::<Vec<_>>();
        Ok(format!("Found {} patients: {:?}", results.len(), results))
    }

    pub async fn get_timeline(&self, patient_id: i32) -> Result<String, String> {
        Ok("Timeline placeholder".to_string())
    }

    pub async fn get_problems(&self, patient_id: i32) -> Result<String, String> {
        Ok("Problems placeholder".to_string())
    }

    pub async fn get_meds(&self, patient_id: i32) -> Result<String, String> {
        Ok("Meds placeholder".to_string())
    }

    pub async fn get_care_gaps(&self, patient_id: Option<i32>) -> Result<String, String> {
        Ok("Care gaps placeholder".to_string())
    }

    pub async fn get_allergies(&self, patient_id: i32) -> Result<String, String> {
        Ok("Allergies placeholder".to_string())
    }

    pub async fn get_referrals(&self, patient_id: i32) -> Result<String, String> {
        Ok("Referrals placeholder".to_string())
    }

    pub async fn get_drug_alerts(&self, patient_id: i32, severity: Option<AlertSeverity>, include_resolved: bool, include_overridden: bool, drug_class: Option<String>, format: Option<AlertFormat>, include_inactive: bool, severity_filter: Option<AlertSeverity>) -> Result<String, String> {
        Ok("Drug alerts placeholder".to_string())
    }
}

