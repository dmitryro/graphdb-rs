// medical_knowledge/src/mpi_identity_resolution/mpi_identity_resolution.rs
//! MPI Identity Resolution — Real-time, probabilistic patient matching
//! Global singleton with blocking + scoring + auto-merge

use lib::graph_engine::graph_service::GraphService;
use models::medical::*;
use models::{ Graph, Identifier, Edge, Vertex, ToVertex };  // Add ToVertex here
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::{OnceCell, RwLock};
use uuid::Uuid;
use chrono::Utc;  // Add this
use log::info;     // Add this

/// Global singleton — use via MPI_RESOLUTION_SERVICE.get().await
pub static MPI_RESOLUTION_SERVICE: OnceCell<Arc<MpiIdentityResolutionService>> = OnceCell::const_new();

#[derive(Debug, Clone)]
pub struct PatientCandidate {
    pub patient_vertex_id: Uuid,
    pub master_record_id: Option<i32>,
    pub match_score: f64,
    pub blocking_keys: Vec<String>,
}

#[derive(Clone)]
pub struct MpiIdentityResolutionService {
    // Fast lookup indexes
    ssn_index: Arc<RwLock<HashMap<String, Uuid>>>,
    mrn_index: Arc<RwLock<HashMap<String, Uuid>>>,
    name_dob_index: Arc<RwLock<HashMap<(String, String), Vec<Uuid>>>>, // (normalized_name, dob)
}

impl MpiIdentityResolutionService {
    /// Initialize the global singleton — call once at startup
    pub async fn global_init() -> Result<(), &'static str> {
        let service = Arc::new(Self {
            ssn_index: Arc::new(RwLock::new(HashMap::new())),
            mrn_index: Arc::new(RwLock::new(HashMap::new())),
            name_dob_index: Arc::new(RwLock::new(HashMap::new())),
        });

        // Load existing patients and build indexes
        {
            let graph_service = GraphService::get().await;
            let graph = graph_service.read().await;

            for vertex in graph.vertices.values() {
                if vertex.label.as_ref() == "Patient" {
                    if let Some(patient) = Patient::from_vertex(vertex) {
                        service.index_patient(&patient, vertex.id.0).await;
                    }
                }
                if vertex.label.as_ref() == "MasterPatientIndex" {
                    if let Some(mpi) = MasterPatientIndex::from_vertex(vertex) {
                        service.index_mpi_record(&mpi, vertex.id.0).await;
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
                            if vertex.label.as_ref() == "Patient" {
                                if let Some(patient) = Patient::from_vertex(&vertex) {
                                    service.on_patient_added(patient, vertex.id.0).await;
                                }
                            }
                        });
                    }
                }).await;
            });
        }

        MPI_RESOLUTION_SERVICE
            .set(service)
            .map_err(|_| "MpiIdentityResolutionService already initialized")
    }

    /// Get the global singleton instance
    pub async fn get() -> Arc<Self> {
        MPI_RESOLUTION_SERVICE
            .get_or_init(|| async {
                panic!("MpiIdentityResolutionService not initialized! Call global_init() first.");
            })
            .await
            .clone()
    }

    // =========================================================================
    // INDEXING
    // =========================================================================

    async fn index_patient(&self, patient: &Patient, vertex_id: Uuid) {
        let mut ssn_idx = self.ssn_index.write().await;
        let mut name_dob_idx = self.name_dob_index.write().await;

        // Index SSN (exact) - Patient doesn't have SSN, use mrn instead or skip
        if let Some(mrn) = patient.mrn.as_ref() {
            let mut mrn_idx = self.mrn_index.write().await;
            mrn_idx.insert(mrn.clone(), vertex_id);
        }

        // Index Name + DOB (blocking key)
        let norm_name = format!("{} {}", patient.last_name.to_lowercase(), patient.first_name.to_lowercase());
        let dob = patient.date_of_birth.format("%Y-%m-%d").to_string();
        name_dob_idx.entry((norm_name, dob))
            .or_default()
            .push(vertex_id);
    }

    async fn index_mpi_record(&self, mpi: &MasterPatientIndex, vertex_id: Uuid) {
        // MPI records are golden — override any duplicates
        if let Some(ssn) = mpi.social_security_number.as_ref() {
            let mut ssn_idx = self.ssn_index.write().await;
            ssn_idx.insert(ssn.clone(), vertex_id);
        }
    }

    // =========================================================================
    // REAL-TIME HANDLERS
    // =========================================================================

    async fn on_patient_added(&self, patient: Patient, vertex_id: Uuid) {
        self.index_patient(&patient, vertex_id).await;

        // Run probabilistic matching
        let candidates = self.find_candidates(&patient).await;
        if let Some(best) = candidates.into_iter().max_by(|a, b| a.match_score.partial_cmp(&b.match_score).unwrap()) {
            if best.match_score > 0.95 {
                self.auto_merge(patient, best).await;
            }
        }
    }

    // =========================================================================
    // MATCHING LOGIC
    // =========================================================================

    async fn find_candidates(&self, patient: &Patient) -> Vec<PatientCandidate> {
        let mut candidates = HashSet::new();
        let graph_service = GraphService::get().await;
        let graph = graph_service.read().await;

        // Blocking: MRN exact match (Patient doesn't have SSN)
        if let Some(mrn) = patient.mrn.as_ref() {
            let mrn_idx = self.mrn_index.read().await;
            if let Some(id) = mrn_idx.get(mrn) {
                candidates.insert(*id);
            }
        }

        // Blocking: Name + DOB
        let norm_name = format!("{} {}", patient.last_name.to_lowercase(), patient.first_name.to_lowercase());
        let dob = patient.date_of_birth.format("%Y-%m-%d").to_string();
        let name_dob_idx = self.name_dob_index.read().await;
        if let Some(ids) = name_dob_idx.get(&(norm_name.clone(), dob)) {
            candidates.extend(ids);
        }

        // Score candidates
        let mut scored = Vec::new();
        for &candidate_id in &candidates {
            if let Some(vertex) = graph.get_vertex(&candidate_id) {
                if let Some(existing) = Patient::from_vertex(vertex) {
                    let score = self.calculate_match_score(patient, &existing);
                    scored.push(PatientCandidate {
                        patient_vertex_id: candidate_id,
                        master_record_id: None, // would come from MPI record
                        match_score: score,
                        blocking_keys: vec![],
                    });
                }
            }
        }

        scored
    }

    fn calculate_match_score(&self, a: &Patient, b: &Patient) -> f64 {
        let mut score = 0.0;

        // Exact matches (high weight) - Patient doesn't have SSN, use MRN
        if a.mrn == b.mrn && a.mrn.is_some() { score += 0.4; }
        if a.email.as_ref() == b.email.as_ref() && a.email.is_some() { score += 0.3; }
        if a.phone_mobile.as_ref() == b.phone_mobile.as_ref() && a.phone_mobile.is_some() { score += 0.2; }

        // Name matching (fuzzy)
        let name_sim = strsim::jaro_winkler(
            &format!("{} {}", a.first_name, a.last_name).to_lowercase(),
            &format!("{} {}", b.first_name, b.last_name).to_lowercase(),
        );
        score += name_sim * 0.3;

        // DOB exact
        if a.date_of_birth == b.date_of_birth { score += 0.2; }

        score.clamp(0.0, 1.0)
    }

    // =========================================================================
    // AUTO-MERGE
    // =========================================================================

    async fn auto_merge(&self, new_patient: Patient, candidate: PatientCandidate) {
        let graph_service = GraphService::get().await;
        let mut graph = graph_service.write_graph().await;

        // Create or update MasterPatientIndex
        let mpi = MasterPatientIndex {
            id: rand::random(),
            patient_id: Some(new_patient.id),
            first_name: Some(new_patient.first_name.clone()),
            last_name: Some(new_patient.last_name.clone()),
            date_of_birth: Some(new_patient.date_of_birth),
            gender: Some(new_patient.gender.clone()),
            address: new_patient.address.clone(),
            contact_number: new_patient.phone_mobile.clone(),
            email: new_patient.email.clone(),
            social_security_number: None, // Patient model doesn't have SSN
            match_score: Some(candidate.match_score as f32), // Convert f64 to f32
            match_date: Some(Utc::now()),
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };

        let mpi_vertex = mpi.to_vertex();
        graph.add_vertex(mpi_vertex.clone());

        // Link both patients to MPI record
        graph.add_edge(Edge::new(
            candidate.patient_vertex_id,
            Identifier::new("HAS_MPI_RECORD".to_string()).unwrap(),
            mpi_vertex.id.0,
        ));
        graph.add_edge(Edge::new(
            Uuid::from_u128(new_patient.id as u128),
            Identifier::new("HAS_MPI_RECORD".to_string()).unwrap(),
            mpi_vertex.id.0,
        ));

        info!("Auto-merged patient {} into MPI {}", new_patient.id, mpi.id);
    }
}
