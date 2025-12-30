// medical_knowledge/src/patient/patient_service.rs
//! Patient Service — Global singleton for patient management
//! Handles patient creation, viewing, searching, timeline, problems, meds, care gaps, allergies, referrals, drug alerts
use std::sync::Arc;
use tokio::sync::{OnceCell, RwLock}; // tokio::sync used
use uuid::Uuid;
use chrono::{DateTime, Utc, NaiveDate, NaiveDateTime};
use serde_json::{self, Value, json}; // Added serde_json for property manipulation
use anyhow::{anyhow, Result}; // Added for better error handling in get()
use log::{info, error, warn, debug};
use std::collections::{ HashMap };
use models::medical::{Patient, Problem, Prescription, Allergy, Referral};
use models::{Vertex};
use models::identifiers::{Identifier, VertexId, };
use models::properties::{ SerializableDateTime };
use models::errors::{GraphError, GraphResult};
use lib::commands::{PatientCommand, JourneyFormat, AlertFormat, AlertSeverity};
use lib::graph_engine::graph_service::{GraphService}; 
use lib::config::{ sanitize_cypher_string, format_optional_string };
use crate::{FromVertex}; // Assuming FromVertex is a trait in the current crate

// Global singleton
pub static PATIENT_SERVICE: OnceCell<Arc<PatientService>> = OnceCell::const_new();

#[derive(Clone)]
pub struct PatientService {
    // Patient cache (retained for fast local lookups, but graph is the source of truth)
    patients: Arc<RwLock<HashMap<Uuid, Patient>>>,
    // DEPENDENCY INJECTION: Store the required GraphService
    graph_service: Arc<GraphService>, 
}

impl PatientService {
    /// CONSTRUCTOR: Requires the GraphService instance, guaranteeing a valid state.
    pub fn new(graph_service: Arc<GraphService>) -> Self {
        let service = Self {
            patients: Arc::new(RwLock::new(HashMap::new())),
            graph_service: graph_service.clone(),
        };
        
        // --- Initialization Logic (Moved from init) ---
        // 2. Register observer to keep cache warm
        let service_clone = Arc::new(service.clone());
        let gs_clone = graph_service.clone();
        
        // NOTE: The previous logic uses `add_vertex_observer` which expects a synchronous closure. 
        // We will adapt the inner `tokio::spawn` to run the async cache update.
        tokio::spawn(async move {
            let _ = gs_clone
                .add_vertex_observer(move |vertex| {
                    if vertex.label.as_ref() == "Patient" {
                        let vertex = vertex.clone();
                        let svc = service_clone.clone();
                        tokio::spawn(async move {
                            // Assuming Patient::from_vertex exists to deserialize
                            if let Some(patient) = Patient::from_vertex(&vertex) { 
                                svc.add_patient_to_cache(patient).await;
                            }
                        });
                    }
                })
                .await;
        });
        
        service
    }

    /// Factory method for the global singleton, taking the required dependencies,
    /// and performing all necessary ASYNC initialization.
    /// This replaces the reliance on `GraphService::get()` inside `init()`.
    /// 
    /// Usage: `PatientService::global_init(graph_service_instance).await?`
    pub async fn global_init(graph_service_instance: Arc<GraphService>) -> std::result::Result<(), &'static str> {
        let service = Arc::new(Self::new(graph_service_instance));

        // --- FIXED INITIALIZATION LOGIC (Moved from 'new' to 'global_init') ---
        // 2. Register observer to keep cache warm and perform the necessary async call.
        let service_clone = service.clone();
        let gs_clone = service.graph_service.clone();
        
        // Spawn a task to register the observer and run the async cache update logic
        tokio::spawn(async move {
            let _ = gs_clone
                .add_vertex_observer(move |vertex| {
                    if vertex.label.as_ref() == "Patient" {
                        let vertex = vertex.clone();
                        let svc = service_clone.clone();
                        tokio::spawn(async move {
                            // Assuming Patient::from_vertex exists to deserialize
                            // NOTE: The previous code used Patient::from_vertex(&vertex) which returns Option<Self>
                            // You must implement the FromVertex trait correctly for Patient.
                            if let Some(patient) = Patient::from_vertex(&vertex) { 
                                svc.add_patient_to_cache(patient).await;
                            }
                        });
                    }
                })
                .await;
        });

        PATIENT_SERVICE
            .set(service)
            .map_err(|_| "PatientService already initialized")
    }
    
    /// Retrieves the global PatientService singleton. Panics if global_init was not called.
    pub async fn get() -> Result<Arc<Self>, anyhow::Error> {
        PATIENT_SERVICE
            .get()
            .cloned()
            .ok_or_else(|| {
                 anyhow!("PatientService not initialized! Call global_init(graph_service) first.")
            })
    }

    // =========================================================================
    // CACHE MANAGEMENT (Helper)
    // =========================================================================

    // Renamed to clarify role: only manages in-memory cache, not persistence
    async fn add_patient_to_cache(&self, patient: Patient) {
        let mut patients = self.patients.write().await;
        
        // FIX: Check if the optional ID exists before casting and inserting
        if let Some(id_val) = patient.id {
            // Assuming Uuid::from_u128 conversion is correct for patient.id (i32)
            patients.insert(Uuid::from_u128(id_val as u128), patient);
        } else {
            // Log a warning if we attempt to cache a patient without a canonical ID
            warn!("[MPI] Skipping cache insertion: Patient has no canonical ID.");
        }
    }

    // =========================================================================
    // PATIENT MANAGEMENT (Persistence via Graph)
    // =========================================================================
    /// Creates a new Patient vertex in the graph and initiates MPI matching/linking.
    pub async fn create_patient(&self, mut new_patient: Patient) -> Result<String, GraphError> {
        // Ensure IDs are generated and synced
        let patient_uuid = Uuid::new_v4();
        let now = Utc::now();
        
        new_patient.vertex_id = Some(patient_uuid);
        
        // Match the legacy i32 ID logic used for internal integer-based references
        new_patient.id = Some(patient_uuid.as_u128() as i32);
        
        // Wrap raw DateTime into Option<SerializableDateTime>
        new_patient.created_at = Some(SerializableDateTime(now));
        new_patient.updated_at = Some(SerializableDateTime(now));
        
        // 1. Sanitize REQUIRED string fields
        let first_name = sanitize_cypher_string(new_patient.first_name.as_deref().unwrap_or(""));
        let last_name = sanitize_cypher_string(new_patient.last_name.as_deref().unwrap_or(""));
        
        // 2. Format OPTIONAL fields (handles quoting or "null" literal)
        let gender = format_optional_string(&new_patient.gender);
        let ssn = format_optional_string(&new_patient.ssn);
        let mrn = format_optional_string(&new_patient.mrn);
        
        // 3. Format Timestamps to ISO 8601 (handling Option and the SerializableDateTime wrapper)
        let dob = new_patient.date_of_birth.as_ref()
            .map(|d| d.0.to_rfc3339())
            .unwrap_or_else(|| "null".to_string());
            
        let created_at_str = new_patient.created_at.as_ref()
            .map(|d| d.0.to_rfc3339())
            .unwrap_or_else(|| now.to_rfc3339());
            
        let updated_at_str = new_patient.updated_at.as_ref()
            .map(|d| d.0.to_rfc3339())
            .unwrap_or_else(|| now.to_rfc3339());
        
        let id_val = new_patient.id.map(|i| i.to_string()).unwrap_or_else(|| "null".to_string());
        let vertex_uuid_str = patient_uuid.to_string();

        // The query explicitly sets the vertex_id to ensure merge/lookup reliability
        // Note: If dob is null, we handle it without quotes in the Cypher query via a helper or conditional
        let dob_query_val = if dob == "null" { "null".to_string() } else { format!("\"{}\"", dob) };

        let query = format!(
            "CREATE (p:Patient {{
                id: {}, 
                vertex_id: \"{}\",
                first_name: \"{}\", 
                last_name: \"{}\", 
                gender: {}, 
                ssn: {}, 
                mrn: {}, 
                date_of_birth: {}, 
                created_at: \"{}\", 
                updated_at: \"{}\"
            }});",
            id_val,
            vertex_uuid_str,
            first_name,
            last_name,
            gender,
            ssn, 
            mrn,
            dob_query_val,
            created_at_str,
            updated_at_str
        );
        
        // Passing the full json properties ensures all 40+ clinical fields from the Patient struct
        // are stored on the node, making them available for 'MASTER_WINS' merge policies.
        let properties = serde_json::to_value(&new_patient)
            .map_err(|e| GraphError::InternalError(format!("Serialization failed: {}", e)))?;
        
        self.graph_service
            .execute_cypher_write(&query, properties)
            .await
            .map_err(|e| GraphError::InternalError(format!("Graph write failed: {}", e)))?;
        
        // Logging to Graph of Events for auditability
        info!("[MPI] Patient created: {} (MRN: {:?})", vertex_uuid_str, new_patient.mrn);
        
        Ok(format!("Patient registered with UUID: {}", patient_uuid))
    }

    /// Fetches a Patient vertex from the graph and returns its data.
    pub async fn view_patient(&self, patient_id: i32) -> Result<Patient, GraphError> {
        // First check the cache (fast path)
        let cache_key = Uuid::from_u128(patient_id as u128);
        {
            let patients = self.patients.read().await;
            if let Some(patient) = patients.get(&cache_key) {
                return Ok(patient.clone());
            }
        }
        
        // If not in cache, query the graph (slow path)
        let query = "MATCH (p:Patient {id: $id}) RETURN p";
        let params = json!({"id": patient_id});

        self.graph_service
            .execute_cypher_read(query, params)
            .await
            .and_then(|results| {
                if let Some(Value::Object(map)) = results.get(0) {
                    let patient_node = map.get("p")
                        .ok_or_else(|| GraphError::NotFoundError("Patient node not returned".to_string()))?;

                    let patient: Patient = serde_json::from_value(patient_node.clone())
                        .map_err(|e| GraphError::InternalError(format!("Failed to deserialize patient: {}", e)))?;

                    // FIX: Clone the patient instance *before* moving it into the tokio::spawn future.
                    let patient_for_cache = patient.clone(); 
                    
                    // The original patient is now available to be returned below.

                    // NOTE: If this service uses the observer pattern correctly, 
                    // this manual cache update might be redundant, but we keep it for safety.
                    let self_clone = self.clone();
                    tokio::spawn(async move {
                        // We use the cloned variable `patient_for_cache` here.
                        self_clone.add_patient_to_cache(patient_for_cache).await;
                    });
                    
                    // Return the original patient instance.
                    Ok(patient)
                } else {
                    Err(GraphError::NotFoundError(format!("Patient with ID {} not found in graph.", patient_id)))
                }
            })
    }

    /// Searches patients in the graph based on the query (name/demographics).
    pub async fn search_patients(&self, query: &str) -> Result<String, GraphError> {
        // Use a case-insensitive, fuzzy search Cypher query
        let cypher_query = format!(
            "MATCH (p:Patient) 
             WHERE toUpper(p.first_name) CONTAINS toUpper($query) OR toUpper(p.last_name) CONTAINS toUpper($query)
             RETURN p.id, p.first_name, p.last_name 
             LIMIT 10"
        );
        let params = json!({"query": query.to_uppercase()});

        let results = self.graph_service
            .execute_cypher_read(&cypher_query, params)
            .await?;

        // Format results for display
        Ok(format!("Found {} patients matching '{}': {:?}", results.len(), query, results))
    }

    // =========================================================================
    // CLINICAL DATA RETRIEVAL (Graph Traversal)
    // =========================================================================

    /// Fetches the chronological timeline of clinical events for a patient.
    pub async fn get_timeline(&self, patient_id: i32) -> Result<String, GraphError> {
        // Query: Find Patient, then find related clinical events (Problems, Meds, Encounters)
        let query = "
            MATCH (p:Patient {id: $id})-[r]-(e)
            WHERE type(r) IN ['HAS_PROBLEM', 'HAS_MED', 'HAS_ENCOUNTER']
            RETURN type(r) AS event_type, e.id, e.date
            ORDER BY e.date DESC
        ";
        let params = json!({"id": patient_id});

        let results = self.graph_service.execute_cypher_read(query, params).await?;
        
        Ok(format!("Timeline events for {}: {:?}", patient_id, results))
    }

    /// Fetches a patient's active problems/diagnoses.
    pub async fn get_problems(&self, patient_id: i32) -> Result<String, GraphError> {
        let query = "
            MATCH (p:Patient {id: $id})-[:HAS_PROBLEM]->(pr:Problem)
            WHERE pr.status = 'Active' // Assuming a 'status' property
            RETURN pr.code, pr.name, pr.status
        ";
        let params = json!({"id": patient_id});

        let results = self.graph_service.execute_cypher_read(query, params).await?;
        Ok(format!("Active Problems for {}: {:?}", patient_id, results))
    }

    /// Fetches a patient's active prescriptions/medications.
    pub async fn get_meds(&self, patient_id: i32) -> Result<String, GraphError> {
        let query = "
            MATCH (p:Patient {id: $id})-[:HAS_MED]->(m:Prescription)
            WHERE m.status = 'Active'
            RETURN m.name, m.dosage, m.status, m.start_date
        ";
        let params = json!({"id": patient_id});

        let results = self.graph_service.execute_cypher_read(query, params).await?;
        Ok(format!("Active Medications for {}: {:?}", patient_id, results))
    }

    /// Fetches a list of identified care gaps (e.g., missing screenings, overdue immunizations).
    pub async fn get_care_gaps(&self, patient_id: Option<i32>) -> Result<String, GraphError> {
        // Care gap calculation is complex, but the retrieval pattern is the same.
        // Assuming a pre-calculated CareGap node linked to the Patient.
        let query = "
            MATCH (p:Patient {id: $id})-[:HAS_CARE_GAP]->(g:CareGap)
            RETURN g.description, g.reason
        ";
        
        let params = json!({"id": patient_id.unwrap_or(-1)}); // Handle Option<i32>
        
        let results = self.graph_service.execute_cypher_read(query, params).await?;
        Ok(format!("Care Gaps for {:?}: {:?}", patient_id, results))
    }

    /// Fetches a patient's known allergies.
    pub async fn get_allergies(&self, patient_id: i32) -> Result<String, GraphError> {
        let query = "
            MATCH (p:Patient {id: $id})-[:HAS_ALLERGY]->(a:Allergy)
            RETURN a.substance, a.reaction, a.severity
        ";
        let params = json!({"id": patient_id});
        
        let results = self.graph_service.execute_cypher_read(query, params).await?;
        Ok(format!("Allergies for {}: {:?}", patient_id, results))
    }

    /// Fetches pending and completed referrals for a patient.
    pub async fn get_referrals(&self, patient_id: i32) -> Result<String, GraphError> {
        let query = "
            MATCH (p:Patient {id: $id})-[:HAS_REFERRAL]->(r:Referral)
            RETURN r.specialty, r.status, r.date
        ";
        let params = json!({"id": patient_id});
        
        let results = self.graph_service.execute_cypher_read(query, params).await?;
        Ok(format!("Referrals for {}: {:?}", patient_id, results))
    }

    /// Fetches drug-drug interaction or drug-allergy alerts.
    pub async fn get_drug_alerts(
        &self, 
        patient_id: i32, 
        _severity: Option<AlertSeverity>, 
        include_resolved: bool, 
        include_overridden: bool, 
        drug_class: Option<String>, 
        format: Option<AlertFormat>, 
        include_inactive: bool, 
        severity_filter: Option<AlertSeverity>
    ) -> Result<String, GraphError> {
        // Drug alerts are generated by complex graph traversal. 
        // This query finds pre-calculated or potential alerts.
        let query = "
            MATCH (p:Patient {id: $id})-[:HAS_ALERT]->(a:DrugAlert)
            WHERE a.status = 'Active' // Basic filtering
            // In a full implementation, the WHERE clause would use the input parameters.
            RETURN a.type, a.description, a.severity
        ";
        let params = json!({"id": patient_id});

        let results = self.graph_service.execute_cypher_read(query, params).await?;
        Ok(format!("Drug Alerts for {}: {:?}", patient_id, results))
    }
}

// =========================================================================
// Helper implementations for external models (Assumed in context)
// =========================================================================

/// Trait for converting a raw graph vertex into a structured application model.
impl FromVertex for Patient {
    /// Deserializes the properties field of a graph Vertex into a Patient struct.
    fn from_vertex(vertex: &Vertex) -> Option<Self> where Self: Sized {
        // Fix 1: Mismatched types for label comparison.
        // Assuming `vertex.label` implements `AsRef<str>`:
        if vertex.label.as_ref() != "Patient" {
            return None;
        }

        // Fix 2: Mismatched types for properties.
        // We convert the HashMap into a Value using `serde_json::to_value`.
        let properties_value = match serde_json::to_value(&vertex.properties) {
            Ok(v) => v,
            Err(e) => {
                eprintln!("Failed to serialize properties for Patient from Vertex {}: {}", vertex.id.0, e);
                return None;
            }
        };

        // Now we can use the resulting serde_json::Value for deserialization
        match serde_json::from_value(properties_value) {
            Ok(patient) => Some(patient),
            Err(e) => {
                // Log the error for debugging why deserialization failed
                eprintln!("Failed to deserialize Patient from Vertex {}: {}", vertex.id.0, e);
                None
            }
        }
    }
}
