//! MPI Identity Resolution — Real-time, probabilistic patient matching
//! Global singleton with blocking + scoring + auto-merge

use lib::graph_engine::graph_service::{GraphService, initialize_graph_service}; 
use models::medical::*;
use models::{Graph, Identifier, Edge, Vertex, ToVertex};
use models::medical::{Patient, MasterPatientIndex};
use models::identifiers::SerializableUuid;
use models::properties::{ PropertyValue, SerializableFloat };
use models::timestamp::BincodeDateTime;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::{OnceCell, RwLock};
use uuid::Uuid;
use chrono::Utc;
use log::{info, error};

/// Global singleton — use via MPI_RESOLUTION_SERVICE.get().await
pub static MPI_RESOLUTION_SERVICE: OnceCell<Arc<MpiIdentityResolutionService>> = OnceCell::const_new();

// We define the internal ID type based on existing usage in the file.
type PatientIdInternal = i32;

#[derive(Debug, Clone)]
pub struct PatientCandidate {
    pub patient_vertex_id: Uuid,
    pub patient_id: PatientIdInternal, // Using PatientIdInternal type
    pub master_record_id: Option<PatientIdInternal>,
    pub match_score: f64,
    pub blocking_keys: Vec<String>,
}

#[derive(Clone)]
pub struct MpiIdentityResolutionService {
    // ✅ CONSTRUCTOR INJECTION: The dependency is now a required, explicit field.
    graph_service: Arc<GraphService>,
    ssn_index: Arc<RwLock<HashMap<String, Uuid>>>,
    mrn_index: Arc<RwLock<HashMap<String, Uuid>>>,
    // Key is (normalized_last_first_name, date_of_birth_iso_string)
    name_dob_index: Arc<RwLock<HashMap<(String, String), Vec<Uuid>>>>,
}

/// Safely parses the Master Patient Index ID, handling the "MPI" prefix if present.
fn extract_numeric_patient_id(patient_id_str: &str) -> Result<PatientIdInternal, String> {
    let numeric_part = if patient_id_str.len() > 3 && patient_id_str[..3].eq_ignore_ascii_case("MPI") {
        &patient_id_str[3..]
    } else {
        patient_id_str
    };

    numeric_part
        .parse::<PatientIdInternal>()
        .map_err(|_| {
            format!(
                "Invalid Patient ID format: '{}'. Expected a positive integer (i32), optionally prefixed with 'MPI'.", 
                patient_id_str
            )
        })
}


impl MpiIdentityResolutionService {
    /// ✅ CONSTRUCTOR: Requires the GraphService instance, guaranteeing a valid state.
    pub fn new(graph_service: Arc<GraphService>) -> Self {
        Self {
            graph_service,
            ssn_index: Arc::new(RwLock::new(HashMap::new())),
            mrn_index: Arc::new(RwLock::new(HashMap::new())),
            name_dob_index: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Factory method for the global singleton. It handles dependency setup and index loading.
    pub async fn global_init() -> Result<(), &'static str> {
        // 1. Initialize the dependency (GraphService)
        // NOTE: initialize_graph_service now requires a StorageEngine dependency. 
        // We rely on GraphService::get() and handle its Result. (Fixes Error E0061)
        /*
        if let Err(e) = initialize_graph_service().await { 
             error!("CRITICAL: Failed to initialize GraphService, preventing MPI service start: {}", e);
             return Err("Failed to initialize GraphService.");
        }
        */

        // 2. Retrieve the initialized dependency instance, handling the Result.
        // GraphService::get() returns Result<Arc<GraphService>, GraphError>. We use map_err/?.
        let graph_service_instance = GraphService::get().await
            .map_err(|_| "CRITICAL: Failed to retrieve initialized GraphService.")?; // Fixes Errors E0308 and E0599 source

        // 3. Construct the service using the dependency (Constructor Injection)
        // graph_service_instance is now Arc<GraphService>, matching the Self::new signature.
        let service = Arc::new(Self::new(graph_service_instance.clone()));

        // 4. Load existing patients and build indexes
        {
            // Now we use the unwrapped instance retrieved in step 2.
            let gs = graph_service_instance.clone(); 
            let graph = gs.read().await; // gs is Arc<GraphService>, so .read() is valid.

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

        // 5. Register real-time observers
        {
            let service_clone = service.clone();
            // graph_service_instance is Arc<GraphService> now, move a clone into the thread
            let graph_service_for_thread = graph_service_instance;
            
            tokio::spawn(async move {
                // In a real implementation, the observer would be set up here using graph_service_for_thread.
                let gs = graph_service_for_thread;
                let _service = service_clone;

                // Existing observer placeholder logic updated since gs is now Arc<GraphService> not a Result.
                let _graph = gs.read().await; 
                // ... observer registration logic ...
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

    /// Links an external identifier (like an account ID or different MRN) to a master patient record.
    pub async fn link_external_identifier(
        &self, 
        master_id_str: String,
        external_id: String, 
        id_type: String
    ) -> Result<MasterPatientIndex, String> {
        let master_id = extract_numeric_patient_id(&master_id_str)
            .map_err(|e| format!("Invalid Master Patient ID format: {}. {}", master_id_str, e))?;

        // 🎯 Using injected dependency
        let gs = &self.graph_service; 
        
        let patient_vertex = gs.get_patient_vertex_by_id(master_id)
            .await
            .ok_or_else(|| format!("Master Patient ID {} not found in the graph. ", master_id))?;
        
        let patient_vertex_id = patient_vertex.id.0;

        let medical_id_vertex = Vertex {
            id: SerializableUuid(Uuid::new_v4()),
            label: Identifier::new("MedicalIdentifier".to_string()).unwrap(),
            properties: HashMap::new(),
            created_at: Utc::now().into(),
            updated_at: Utc::now().into(),
        };

        gs.add_vertex(medical_id_vertex.clone()).await
            .map_err(|e| format!("Failed to add Identifier vertex: {}", e))?;

        let edge = Edge::new(
            patient_vertex_id,
            Identifier::new("HAS_EXTERNAL_ID".to_string()).unwrap(),
            medical_id_vertex.id.0,
        );
        gs.add_edge(edge).await
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

     /// Manually merges a source patient record into a target patient record.
    pub async fn manual_merge_records(
        &self, 
        source_id_str: String,
        target_id_str: String,
        _policy: String
    ) -> Result<MasterPatientIndex, String> {
        let source_id = extract_numeric_patient_id(&source_id_str)
            .map_err(|e| format!("Invalid Source Patient ID format: {}. {}", source_id_str, e))?;
        let target_id = extract_numeric_patient_id(&target_id_str)
            .map_err(|e| format!("Invalid Target Patient ID format: {}. {}", target_id_str, e))?;
        // 🎯 Using injected dependency
        let gs = &self.graph_service; 
        
        // 1. Validate Target
        let target_vertex = gs.get_patient_vertex_by_id(target_id)
            .await
            .ok_or_else(|| format!("Target Patient ID {} not found.", target_id))?;
        let target_vertex_id = target_vertex.id.0;
        
        // 2. Validate Source
        let source_vertex = gs.get_patient_vertex_by_id(source_id)
            .await
            .ok_or_else(|| format!("Source Patient ID {} not found for merge.", source_id))?;
        let source_vertex_id = source_vertex.id.0;
        
        // 3. Create Audit Log Vertex (Placeholder for actual audit data)
        let audit_vertex = Vertex {
            id: SerializableUuid(Uuid::new_v4()),
            label: Identifier::new("AuditLog".to_string()).unwrap(),
            properties: HashMap::new(),
            created_at: Utc::now().into(),
            updated_at: Utc::now().into(),
        };
        gs.add_vertex(audit_vertex.clone()).await
            .map_err(|e| format!("Failed to add audit vertex: {}", e))?;
        
        // 4. Link Target to Audit Log
        let edge = Edge::new(
            target_vertex_id,
            Identifier::new("HAS_IDENTITY_HISTORY".to_string()).unwrap(),
            audit_vertex.id.0,
        );
        gs.add_edge(edge).await
            .map_err(|e| format!("Failed to add HAS_IDENTITY_HISTORY edge: {}", e))?;
        
        // 5. Delete Source Vertex - Convert Uuid to Identifier
        let source_identifier = Identifier::new(source_vertex_id.to_string())
            .map_err(|e| format!("Failed to create identifier for source vertex: {}", e))?;
        gs.delete_vertex(source_identifier).await
            .map_err(|e| format!("Failed to delete source vertex: {}", e))?;
        
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

    /// Retrieves the identity audit trail for a given patient ID.
    pub async fn get_audit_trail(&self, patient_id_str: String, _timeframe: Option<String>) -> Result<Vec<String>, String> {
        let patient_id = extract_numeric_patient_id(&patient_id_str)
            .map_err(|e| format!("MPI audit failed: An internal error occurred: {}. {}", patient_id_str, e))?;

        // 🎯 Using injected dependency
        let gs = &self.graph_service; 
        
        let patient_vertex = gs.get_patient_vertex_by_id(patient_id)
            .await
            .ok_or_else(|| format!("Patient ID {} not found.", patient_id))?;
        let patient_vertex_id = patient_vertex.id.0;
        
        let graph = gs.read().await;
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
        // 🎯 Using injected dependency
        let gs = &self.graph_service;
        
        let graph = gs.read().await;

        // Blocking on MRN
        if let Some(mrn) = patient.mrn.as_ref() {
            let mrn_idx = self.mrn_index.read().await;
            if let Some(id) = mrn_idx.get(mrn) {
                candidates.insert(*id);
            }
        }
        
        // Blocking on Name + DOB
        let norm_name = format!("{} {}", patient.last_name.to_lowercase(), patient.first_name.to_lowercase());
        let dob = patient.date_of_birth.format("%Y-%m-%d").to_string();
        let name_dob_idx = self.name_dob_index.read().await;
        if let Some(ids) = name_dob_idx.get(&(norm_name, dob)) {
            candidates.extend(ids);
        }

        // Blocking on SSN (via MPI record, not Patient record)
        if let Some(ssn) = patient.ssn.as_ref() {
            let ssn_idx = self.ssn_index.read().await;
            if let Some(_mpi_vertex_id) = ssn_idx.get(ssn) {
                // Skipping complex graph traversal blocking for now.
            }
        }

        let mut scored = Vec::new();
        for &candidate_id in &candidates {
            if candidate_id == patient.to_vertex().id.0 {
                continue;
            }
            
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

        // Weights: Total weight should ideally sum to 1.0 or less if some fields are optional.
        // MRN match is strong (0.4)
        if a.mrn == b.mrn && a.mrn.is_some() { score += 0.4; }
        // Name similarity is important (0.3)
        let name_sim = strsim::jaro_winkler(
            &format!("{} {}", a.first_name, a.last_name).to_lowercase(),
            &format!("{} {}", b.first_name, b.last_name).to_lowercase(),
        );
        score += name_sim * 0.3; 
        // DOB match is strong (0.2)
        if a.date_of_birth == b.date_of_birth { score += 0.2; }
        // SSN match 
        if a.ssn.as_ref() == b.ssn.as_ref() && a.ssn.is_some() { score += 0.1; }
        // Remaining 0.1 for other factors (email, phone, address, etc.)

        if a.email.as_ref() == b.email.as_ref() && a.email.is_some() { score += 0.05; }
        if a.phone_mobile.as_ref() == b.phone_mobile.as_ref() && a.phone_mobile.is_some() { score += 0.05; }


        score.clamp(0.0, 1.0)
    }

    // =========================================================================
    // AUTO-MERGE
    // =========================================================================

    async fn auto_merge(&self, new_patient_vertex_id: Uuid, new_patient: Patient, candidate: PatientCandidate) {
        // 🎯 Using injected dependency
        let gs = &self.graph_service;
        
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
            social_security_number: new_patient.ssn.clone(), // Use new patient's SSN if available
            match_score: Some(candidate.match_score as f32),
            match_date: Some(Utc::now()),
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };

        let mut mpi_vertex = mpi_record_data.to_vertex();
        mpi_vertex.id.0 = target_mpi_vertex_id;

        if let Err(e) = gs.add_vertex(mpi_vertex).await {
            info!("Failed to add MPI vertex during auto-merge: {}", e);
            return;
        }

        // Link the new patient to the MPI record
        let new_edge = Edge::new(
            new_patient_vertex_id,
            Identifier::new("HAS_MPI_RECORD".to_string()).unwrap(),
            target_mpi_vertex_id,
        );
        if let Err(e) = gs.add_edge(new_edge).await {
            info!("Failed to add new patient MPI edge: {}", e);
        }

        // Link the candidate patient to the same MPI record
        let candidate_edge = Edge::new(
            candidate.patient_vertex_id,
            Identifier::new("HAS_MPI_RECORD".to_string()).unwrap(),
            target_mpi_vertex_id,
        );
        if let Err(e) = gs.add_edge(candidate_edge).await {
            info!("Failed to add candidate patient MPI edge: {}", e);
        }

        info!("Auto-merge complete: New patient {} and candidate {} linked to MPI {}", 
            new_patient.id, candidate.patient_id, target_mpi_vertex_id);
    }

    /// Records the user who manually resolves a conflict, typically linking an MPI record to an AuditLog or Conflict vertex.
    /// This uses the RESOLVED_BY relationship structure.
    pub async fn log_conflict_resolution(
        &self,
        conflict_vertex_id: Uuid,
        user_id: String,
    ) -> Result<Uuid, String> {
        let gs = self.graph_service.clone();

        // Create User vertex (ephemeral clinician/analyst)
        let user_vertex_id = Uuid::new_v4();

        let mut properties = HashMap::new();
        properties.insert(
            "user_id".to_string(),
            PropertyValue::String(user_id.clone()),
        );
        properties.insert(
            "timestamp".to_string(),
            PropertyValue::String(Utc::now().to_rfc3339()),
        );
        properties.insert(
            "action".to_string(),
            PropertyValue::String("conflict_resolution".to_string()),
        );

        let user_vertex = Vertex {
            id: user_vertex_id.into(),
            label: Identifier::new("User".to_string()).map_err(|e| e.to_string())?,
            properties,
            created_at: Utc::now().into(),
            updated_at: Utc::now().into(),
        };

        gs.create_vertex(user_vertex)
            .await
            .map_err(|e| format!("Failed to create User vertex: {e}"))?;

        // Create RESOLVED_BY edge
        let edge = Edge::new(
            conflict_vertex_id,
            Identifier::new("RESOLVED_BY".to_string()).map_err(|e| e.to_string())?,
            user_vertex_id,
        );

        gs.create_edge(edge)
            .await
            .map_err(|e| format!("Failed to create RESOLVED_BY edge: {e}"))?;

        info!("Conflict resolution logged for {conflict_vertex_id} by user {user_id}");
        Ok(user_vertex_id)
    }

    /// Creates a SurvivorshipRule vertex and links it to an MPI record.
    pub async fn create_survivorship_rule_link(
        &self,
        mpi_vertex_id: Uuid,
        rule_name: String,
        field: String,
        policy: String,
    ) -> Result<Uuid, String> {
        let gs = self.graph_service.clone();

        let rule_vertex_id = Uuid::new_v4();

        let mut properties = HashMap::new();
        properties.insert("rule_name".to_string(), PropertyValue::String(rule_name));
        properties.insert("field".to_string(), PropertyValue::String(field));
        properties.insert("policy".to_string(), PropertyValue::String(policy));
        properties.insert(
            "applied_at".to_string(),
            PropertyValue::String(Utc::now().to_rfc3339()),
        );

        let rule_vertex = Vertex {
            id: rule_vertex_id.into(),
            label: Identifier::new("SurvivorshipRule".to_string()).map_err(|e| e.to_string())?,
            properties,
            created_at: Utc::now().into(),
            updated_at: Utc::now().into(),
        };

        gs.create_vertex(rule_vertex)
            .await
            .map_err(|e| format!("Failed to create SurvivorshipRule vertex: {e}"))?;

        let edge = Edge::new(
            mpi_vertex_id,
            Identifier::new("HAS_SURVIVORSHIP_RULE".to_string()).map_err(|e| e.to_string())?,
            rule_vertex_id,
        );

        gs.create_edge(edge)
            .await
            .map_err(|e| format!("Failed to create HAS_SURVIVORSHIP_RULE edge: {e}"))?;

        info!("Created SurvivorshipRule link for MPI record {mpi_vertex_id}");
        Ok(rule_vertex_id)
    }

    // =========================================================================
    // CORE MPI ENTITY CREATION
    // =========================================================================

    /// Creates a MatchScore vertex and links it to an MPI record.
    /// This is used after run_probabilistic_match identifies a score.
    pub async fn create_match_score_link(
        &self,
        mpi_vertex_id: Uuid,
        score: f64,
        matching_algo: String,
    ) -> Result<Uuid, String> {
        let gs = self.graph_service.clone();

        let score_vertex_id = Uuid::new_v4();
        let matching_algo_clone = matching_algo.clone();
        let mut properties = HashMap::new();
        properties.insert(
            "score".to_string(),
            PropertyValue::Float(SerializableFloat(score)),
        );
        properties.insert("algorithm".to_string(), PropertyValue::String(matching_algo));
        properties.insert(
            "recorded_at".to_string(),
            PropertyValue::String(Utc::now().to_rfc3339()),
        );

        let score_vertex = Vertex {
            id: score_vertex_id.into(),
            label: Identifier::new("MatchScore".to_string()).map_err(|e| e.to_string())?,
            properties,
            created_at: Utc::now().into(),
            updated_at: Utc::now().into(),
        };

        gs.create_vertex(score_vertex)
            .await
            .map_err(|e| format!("Failed to create MatchScore vertex: {e}"))?;

        let edge = Edge::new(
            mpi_vertex_id,
            Identifier::new("HAS_MATCH_SCORE".to_string()).map_err(|e| e.to_string())?,
            score_vertex_id,
        );

        gs.create_edge(edge)
            .await
            .map_err(|e| format!("Failed to create HAS_MATCH_SCORE edge: {e}"))?;

        info!("Created MatchScore {:?} (algo: {:?}) for MPI {:?}", score, matching_algo_clone, mpi_vertex_id );
        Ok(score_vertex_id)
    }

    // =========================================================================
    // SOFT-LINKING & CONFLICT MANAGEMENT
    // =========================================================================

    /// Creates a soft-link (IS_LINKED_TO) between two Patient records based on probabilistic scoring.
    /// This flags them as potential duplicates requiring manual review.
    pub async fn create_potential_duplicate(
        &self,
        patient_a_vertex_id: Uuid,
        patient_b_vertex_id: Uuid,
        score: f64,
        user: Option<String>,
    ) -> Result<Uuid, String> {
        let gs = self.graph_service.clone();

        let mut edge = Edge::new(
            patient_a_vertex_id,
            Identifier::new("IS_LINKED_TO".to_string()).map_err(|e| e.to_string())?,
            patient_b_vertex_id,
        );

        edge = edge.with_property(
            "match_score",
            PropertyValue::Float(SerializableFloat(score)),
        );

        if let Some(u) = user {
            edge = edge.with_property("flagged_by", PropertyValue::String(u));
        }

        edge = edge.with_property(
            "detected_at",
            PropertyValue::String(Utc::now().to_rfc3339()),
        );

        gs.create_edge(edge)
            .await
            .map_err(|e| format!("Failed to create IS_LINKED_TO edge: {e}"))?;

        info!(
            "Created potential duplicate link between {patient_a_vertex_id} <> {patient_b_vertex_id} (score: {score})"
        );

        Ok(patient_a_vertex_id) // or return edge.id.0 if you add it to Edge
    }

    // =========================================================================
    // IDENTITY SPLIT (Reversing a Merge)
    // =========================================================================

    /// Performs an identity split, reversing an erroneous merge by creating a new Patient record
    /// and linking it back to the original source.
    pub async fn identity_split(
        &self,
        target_patient_id_str: String,
        new_patient_data: Patient,
        reason: String
    ) -> Result<Uuid, String> {
        let gs = &self.graph_service;
        
        let target_id = extract_numeric_patient_id(&target_patient_id_str)
            .map_err(|e| format!("Invalid Target Patient ID format: {}. {}", target_patient_id_str, e))?;

        // 1. Validate Target Patient (the one that needs to be split)
        let target_vertex = gs.get_patient_vertex_by_id(target_id)
            .await
            .ok_or_else(|| format!("Target Patient ID {} not found for split.", target_id))?;
        let target_vertex_id = target_vertex.id.0;
        
        // 2. Create the new Patient record
        let new_patient_vertex = new_patient_data.to_vertex();
        let new_patient_vertex_id = new_patient_vertex.id.0;

        gs.add_vertex(new_patient_vertex).await
            .map_err(|e| format!("Failed to create new Patient vertex for split: {}", e))?;

        // 3. Create Audit Vertex for the split
        let audit_vertex_id = Uuid::new_v4();
        let audit_vertex = Vertex {
            id: SerializableUuid(audit_vertex_id),
            label: Identifier::new("AuditLog".to_string()).unwrap(),
            properties: HashMap::from([
                ("reason".to_string(), models::PropertyValue::String(reason)),
                ("action".to_string(), models::PropertyValue::String("IDENTITY_SPLIT".to_string())),
            ]),
            created_at: Utc::now().into(),
            updated_at: Utc::now().into(),
        };
        gs.add_vertex(audit_vertex).await
            .map_err(|e| format!("Failed to add AuditLog vertex for split: {}", e))?;
            
        // 4. Create IS_SPLIT_FROM edge (NewPatient -> Original/Target Patient)
        let split_edge = Edge::new(
            new_patient_vertex_id,
            Identifier::new("IS_SPLIT_FROM".to_string()).unwrap(),
            target_vertex_id,
        );
        gs.add_edge(split_edge).await
            .map_err(|e| format!("Failed to link IS_SPLIT_FROM edge: {}", e))?;
            
        // 5. Link New Patient to Audit Log
        let audit_edge = Edge::new(
            new_patient_vertex_id,
            Identifier::new("HAS_IDENTITY_HISTORY".to_string()).unwrap(),
            audit_vertex_id,
        );
        gs.add_edge(audit_edge).await
            .map_err(|e| format!("Failed to link AuditLog edge: {}", e))?;

        info!("Identity split successful: New Patient {} created from {}", new_patient_vertex_id, target_vertex_id);
        Ok(new_patient_vertex_id)
    }
}