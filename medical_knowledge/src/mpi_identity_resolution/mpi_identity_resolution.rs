//! MPI Identity Resolution — Real-time, probabilistic patient matching
//! Global singleton with blocking + scoring + auto-merge

use lib::graph_engine::graph_service::GraphService;
use models::medical::*;
use models::{Graph, Identifier, Edge, Vertex, ToVertex};
use models::medical::{Patient, MasterPatientIndex};
use models::identifiers::SerializableUuid;
use models::timestamp::BincodeDateTime; // <-- ADD THIS
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::{OnceCell, RwLock};
use uuid::Uuid;
use chrono::Utc;
use log::info;

/// Global singleton — use via MPI_RESOLUTION_SERVICE.get().await
pub static MPI_RESOLUTION_SERVICE: OnceCell<Arc<MpiIdentityResolutionService>> = OnceCell::const_new();

#[derive(Debug, Clone)]
pub struct PatientCandidate {
    pub patient_vertex_id: Uuid,
    pub patient_id: i32,
    pub master_record_id: Option<i32>,
    pub match_score: f64,
    pub blocking_keys: Vec<String>,
}

#[derive(Clone)]
pub struct MpiIdentityResolutionService {
    ssn_index: Arc<RwLock<HashMap<String, Uuid>>>,
    mrn_index: Arc<RwLock<HashMap<String, Uuid>>>,
    name_dob_index: Arc<RwLock<HashMap<(String, String), Vec<Uuid>>>>,
}

impl MpiIdentityResolutionService {
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
                let graph_service_clone = graph_service.clone();
                let graph = graph_service_clone.write_graph().await; // <-- REMOVED `mut` (not needed)
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

    pub async fn get() -> Arc<Self> {
        MPI_RESOLUTION_SERVICE
            .get_or_init(|| async {
                panic!("MpiIdentityResolutionService not initialized! Call global_init() first.");
            })
            .await
            .clone()
    }

    pub async fn run_probabilistic_match(&self, patient: &Patient) -> Result<Vec<PatientCandidate>, String> {
        let candidates = self.find_candidates(patient).await;
        
        if candidates.is_empty() {
            return Err("No potential match candidates found based on blocking keys.".into());
        }

        let mut sorted_candidates = candidates;
        sorted_candidates.sort_by(|a, b| b.match_score.partial_cmp(&a.match_score).unwrap_or(std::cmp::Ordering::Equal));
        
        Ok(sorted_candidates)
    }

    pub async fn link_external_identifier(
        &self, 
        master_id: i32, 
        external_id: String, 
        id_type: String
    ) -> Result<MasterPatientIndex, String> {
        let graph_service = GraphService::get().await;
        
        let patient_vertex = graph_service.get_patient_vertex_by_id(master_id)
            .await
            .ok_or_else(|| format!("Master Patient ID {} not found in the graph.", master_id))?;
        
        let patient_vertex_id = patient_vertex.id.0;

        let medical_id_vertex = Vertex {
            id: SerializableUuid(Uuid::new_v4()),
            label: Identifier::new("MedicalIdentifier".to_string()).unwrap(),
            properties: HashMap::new(),
            created_at: Utc::now().into(),  // <-- FIXED
            updated_at: Utc::now().into(),  // <-- FIXED
        };

        graph_service.add_vertex(medical_id_vertex.clone()).await
            .map_err(|e| format!("Failed to add Identifier vertex: {}", e))?;

        let edge = Edge::new(
            patient_vertex_id,
            Identifier::new("HAS_EXTERNAL_ID".to_string()).unwrap(),
            medical_id_vertex.id.0,
        );
        graph_service.add_edge(edge).await
            .map_err(|e| format!("Failed to add HAS_EXTERNAL_ID edge: {}", e))?;

        info!("Successfully linked external ID {} ({}) to patient {}", external_id, id_type, master_id);
        
        Ok(MasterPatientIndex {
            id: rand::random(),
            patient_id: Some(master_id),
            first_name: None, last_name: None, date_of_birth: None, gender: None,
            address: None, contact_number: None, email: None, social_security_number: None,
            match_score: None, match_date: None,
            created_at: Utc::now(), updated_at: Utc::now(),
        })
    }

    pub async fn manual_merge_records(
        &self, 
        source_id: i32, 
        target_id: i32, 
        _policy: String
    ) -> Result<MasterPatientIndex, String> {
        let graph_service = GraphService::get().await;
        
        let target_vertex = graph_service.get_patient_vertex_by_id(target_id)
            .await
            .ok_or_else(|| format!("Target Patient ID {} not found.", target_id))?;
        let target_vertex_id = target_vertex.id.0;
        
        let source_vertex = graph_service.get_patient_vertex_by_id(source_id)
            .await
            .ok_or_else(|| format!("Source Patient ID {} not found for merge.", source_id))?;
        let source_vertex_id = source_vertex.id.0;

        let audit_vertex = Vertex {
            id: SerializableUuid(Uuid::new_v4()),
            label: Identifier::new("AuditLog".to_string()).unwrap(),
            properties: HashMap::new(),
            created_at: Utc::now().into(),  // <-- FIXED
            updated_at: Utc::now().into(),  // <-- FIXED
        };

        graph_service.add_vertex(audit_vertex.clone()).await
            .map_err(|e| format!("Failed to add audit vertex: {}", e))?;

        let edge = Edge::new(
            target_vertex_id,
            Identifier::new("HAS_IDENTITY_HISTORY".to_string()).unwrap(),
            audit_vertex.id.0,
        );
        graph_service.add_edge(edge).await
            .map_err(|e| format!("Failed to add HAS_IDENTITY_HISTORY edge: {}", e))?;

        let delete_op = lib::storage_engine::GraphOp::DeleteVertex(
            Identifier::new("Patient".to_string()).unwrap()
        );
        graph_service.delete_op(delete_op).await
            .map_err(|e| format!("Failed to delete source vertex: {}", e))?;
        graph_service.delete_vertex_from_memory(source_vertex_id).await
            .map_err(|e| format!("Failed to delete from memory: {}", e))?;

        info!("Manual merge successful: {} -> {}. Source retired.", source_id, target_id);

        Ok(MasterPatientIndex {
            id: rand::random(),
            patient_id: Some(target_id),
            first_name: None, last_name: None, date_of_birth: None, gender: None,
            address: None, contact_number: None, email: None, social_security_number: None,
            match_score: None, match_date: None,
            created_at: Utc::now(), updated_at: Utc::now(),
        })
    }

    pub async fn get_audit_trail(&self, patient_id: i32, _timeframe: Option<String>) -> Result<Vec<String>, String> {
        let graph_service = GraphService::get().await;
        
        let patient_vertex = graph_service.get_patient_vertex_by_id(patient_id)
            .await
            .ok_or_else(|| format!("Patient ID {} not found.", patient_id))?;
        let patient_vertex_id = patient_vertex.id.0;
        
        let graph = graph_service.read().await;
        let mut changes = Vec::new();

        for edge in graph.outgoing_edges(&patient_vertex_id) {
            if edge.label.as_str() == "HAS_IDENTITY_HISTORY" {
                if graph.get_vertex(&edge.inbound_id.0).is_some() {
                    changes.push("Identity change recorded".to_string());
                }
            }
        }

        info!("Retrieved identity audit trail for patient {}", patient_id);
        Ok(changes)
    }

    // =========================================================================
    // INDEXING & REAL-TIME
    // =========================================================================

    async fn index_patient(&self, patient: &Patient, vertex_id: Uuid) {
        let mut mrn_idx = self.mrn_index.write().await;
        let mut name_dob_idx = self.name_dob_index.write().await;

        if let Some(mrn) = patient.mrn.as_ref() {
            mrn_idx.insert(mrn.clone(), vertex_id);
        }

        let norm_name = format!("{} {}", patient.last_name.to_lowercase(), patient.first_name.to_lowercase());
        let dob = patient.date_of_birth.format("%Y-%m-%d").to_string();
        name_dob_idx.entry((norm_name, dob))
            .or_default()
            .push(vertex_id);
    }

    async fn index_mpi_record(&self, mpi: &MasterPatientIndex, vertex_id: Uuid) {
        if let Some(ssn) = mpi.social_security_number.as_ref() {
            let mut ssn_idx = self.ssn_index.write().await;
            ssn_idx.insert(ssn.clone(), vertex_id);
        }
    }

    async fn on_patient_added(&self, patient: Patient, vertex_id: Uuid) {
        self.index_patient(&patient, vertex_id).await;

        let candidates = self.find_candidates(&patient).await;
        if let Some(best) = candidates.into_iter().max_by(|a, b| a.match_score.partial_cmp(&b.match_score).unwrap()) {
            if best.match_score > 0.95 {
                self.auto_merge(vertex_id, patient, best).await;
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

        if let Some(mrn) = patient.mrn.as_ref() {
            let mrn_idx = self.mrn_index.read().await;
            if let Some(id) = mrn_idx.get(mrn) {
                candidates.insert(*id);
            }
        }

        let norm_name = format!("{} {}", patient.last_name.to_lowercase(), patient.first_name.to_lowercase());
        let dob = patient.date_of_birth.format("%Y-%m-%d").to_string();
        let name_dob_idx = self.name_dob_index.read().await;
        if let Some(ids) = name_dob_idx.get(&(norm_name, dob)) {
            candidates.extend(ids);
        }

        let mut scored = Vec::new();
        for &candidate_id in &candidates {
            if let Some(vertex) = graph.get_vertex(&candidate_id) {
                if let Some(existing) = Patient::from_vertex(vertex) {
                    let score = self.calculate_match_score(patient, &existing);
                    scored.push(PatientCandidate {
                        patient_vertex_id: candidate_id,
                        patient_id: existing.id,
                        master_record_id: None,
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

        if a.mrn == b.mrn && a.mrn.is_some() { score += 0.4; }
        if a.email.as_ref() == b.email.as_ref() && a.email.is_some() { score += 0.3; }
        if a.phone_mobile.as_ref() == b.phone_mobile.as_ref() && a.phone_mobile.is_some() { score += 0.2; }

        let name_sim = strsim::jaro_winkler(
            &format!("{} {}", a.first_name, a.last_name).to_lowercase(),
            &format!("{} {}", b.first_name, b.last_name).to_lowercase(),
        );
        score += name_sim * 0.3;

        if a.date_of_birth == b.date_of_birth { score += 0.2; }

        score.clamp(0.0, 1.0)
    }

    // =========================================================================
    // AUTO-MERGE
    // =========================================================================

    async fn auto_merge(&self, new_patient_vertex_id: Uuid, new_patient: Patient, candidate: PatientCandidate) {
        let graph_service = GraphService::get().await;
        let mut graph_lock = graph_service.write_graph().await;

        let target_mpi_vertex_id = Uuid::new_v4();
        let mpi_record_data = MasterPatientIndex {
            id: rand::random(),
            patient_id: Some(new_patient.id),
            first_name: Some(new_patient.first_name.clone()),
            last_name: Some(new_patient.last_name.clone()),
            date_of_birth: Some(new_patient.date_of_birth),
            gender: Some(new_patient.gender.clone()),
            address: new_patient.address.clone(),
            contact_number: new_patient.phone_mobile.clone(),
            email: new_patient.email.clone(),
            social_security_number: None,
            match_score: Some(candidate.match_score as f32),
            match_date: Some(Utc::now()),
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };

        let mpi_vertex = mpi_record_data.to_vertex();
        graph_lock.add_vertex(mpi_vertex);

        let new_edge = Edge::new(
            new_patient_vertex_id,
            Identifier::new("HAS_MPI_RECORD".to_string()).unwrap(),
            target_mpi_vertex_id,
        );
        graph_lock.add_edge(new_edge);

        let candidate_edge = Edge::new(
            candidate.patient_vertex_id,
            Identifier::new("HAS_MPI_RECORD".to_string()).unwrap(),
            target_mpi_vertex_id,
        );
        graph_lock.add_edge(candidate_edge);

        info!("Auto-merge complete: New patient {} and candidate {} linked to MPI {}", 
            new_patient.id, candidate.patient_id, target_mpi_vertex_id);
    }
}