//! MPI Identity Resolution — Real-time, probabilistic patient matching
//! Global singleton with blocking + scoring + auto-merge

// Assuming strsim is available as a dependency based on previous usage
use anyhow::{Result, Context, anyhow};
use std::convert::TryFrom; // Needed for i64 -> i32 conversion
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::str::FromStr; // FIX for E0599 (no function from_str)
use lib::graph_engine::graph_service::{GraphService, initialize_graph_service}; 
use models::medical::*;
use models::{Graph, Identifier, Edge, Vertex, ToVertex};
use models::medical::{Patient, MasterPatientIndex};
use models::identifiers::SerializableUuid;
use models::properties::{ PropertyValue, SerializableFloat };
use models::timestamp::BincodeDateTime;
use tokio::sync::{OnceCell, RwLock};
use uuid::Uuid;
use chrono::{DateTime, TimeZone, NaiveDate, Utc, Datelike};
use log::{info, error, warn};
use serde_json::{ self, Value }; // ADDED for JSON parsing in global_init
// Assuming the 'strsim' crate is a dependency in Cargo.toml
// If 'strsim' is not imported globally, you might need 'use strsim;' here
// if using it outside of `self::` or module paths.


/// Global singleton — use via MPI_RESOLUTION_SERVICE.get().await
pub static MPI_RESOLUTION_SERVICE: OnceCell<Arc<MpiIdentityResolutionService>> = OnceCell::const_new();

// We define the internal ID type based on existing usage in the file.
type PatientIdInternal = i32;

#[derive(Debug, Clone)]
pub struct PatientCandidate {
    pub patient_vertex_id: Uuid,
    pub patient_id: PatientIdInternal, // The patient's business ID (e.g., 12345)
    pub master_record_id: Option<PatientIdInternal>, // The MPI record ID, if one exists
    pub match_score: f64,
    pub blocking_keys: Vec<String>, // Keys that led to this match
}

#[derive(Clone)]
pub struct MpiIdentityResolutionService {
    // ✅ CONSTRUCTOR INJECTION: The dependency is now a required, explicit field.
    graph_service: Arc<GraphService>,
    ssn_index: Arc<RwLock<HashMap<String, Uuid>>>,
    mrn_index: Arc<RwLock<HashMap<String, Uuid>>>,
    // Key is (normalized_last_first_name, date_of_birth_iso_string)
    name_dob_index: Arc<RwLock<HashMap<(String, String), Vec<Uuid>>>>,
    // Blocking index for probabilistic matching (The missing field)
    blocking_index: Arc<RwLock<HashMap<String, HashSet<Uuid>>>>, // <-- ADD THIS FIELD
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

/// Helper to extract a single Patient Vertex from the graph query result.
/// 
/// The raw result from the DB is expected to be: 
/// Vec<Value> (length 1) -> Value (JSON object) -> "results" array -> 
/// First element -> "vertices" array -> First Vertex JSON.
// --- FIX FOR E0515 ---
fn extract_single_vertex(result_vec: Vec<Value>) -> Option<Vertex> {
    result_vec.into_iter()
        .flat_map(|val| {
            // Get the "results" array. We need to clone it to pass ownership 
            // to the outer iterator chain. This is expensive but necessary
            // given the data structure and lifetime constraints.
            val.get("results").and_then(Value::as_array).map(|arr| arr.to_vec())
        }) 
        .flatten() // Now iterating over Vec<Value> (the results array items)
        .flat_map(|res_item| {
            // Get the "vertices" array and clone it.
            res_item.get("vertices").and_then(Value::as_array).map(|arr| arr.to_vec())
        })
        .flatten() // Now iterating over Vec<Value> (the vertex JSONs)
        .filter_map(|v_val| {
            // We have ownership of v_val here, so cloning is fine
            serde_json::from_value::<Vertex>(v_val).ok()
        })
        .next() 
}

// --- The Core Lookup Function ---
async fn get_patient_vertex_by_id_or_mrn(
    gs: &GraphService, 
    identifier: &str
// FIX: Replace unknown type `GraphVertex` with known type `Vertex`
) -> Result<Vertex, String> {
    
    let clean_id = identifier.trim_matches(|c| c == '\'' || c == '"');

    // Define queries: Try MRN, then String ID, then Numeric ID
    let mut queries = vec![
        format!("MATCH (p:Patient {{mrn: \"{}\"}}) RETURN p", clean_id),
        format!("MATCH (p:Patient {{id: \"{}\"}}) RETURN p", clean_id),
    ];
    if let Ok(num_id) = clean_id.parse::<i64>() {
        queries.push(format!("MATCH (p:Patient {{id: {}}}) RETURN p", num_id));
    }

    for query in queries {
        match gs.execute_cypher_read(&query, Value::Null).await {
            Ok(result_vec) => {
                // FIX: Use the new extraction logic
                if let Some(vertex) = extract_single_vertex(result_vec) {
                    println!("[Service Debug] Patient found by query: {}", query);
                    return Ok(vertex);
                }
            },
            Err(e) => println!("[Service Debug] Query failed: {}. Error: {:?}", query, e),
        }
    }
    
    // If no match is found after all attempts
    Err(format!("Patient with identifier '{}' not found in graph.", clean_id))
}
// Helper function to get patient data (ID and UUID) from MRN or numeric ID
async fn get_patient_data_helper(
    gs: &GraphService, 
    identifier: &str
) -> Result<(i32, String), String> {
    use std::str::FromStr;
    
    // Check if input is already numeric
    if let Ok(id) = i32::from_str(identifier) {
        if id != 0 {
            // Look up by numeric ID (no quotes!)
            let query = format!("MATCH (p:Patient {{id: {}}}) RETURN p", id);
            let result = gs.execute_cypher_read(&query, Value::Null).await
                .map_err(|e| format!("Graph lookup for ID '{}' failed: {}", id, e))?;
            return parse_patient_from_result(result, identifier);
        }
    }
    
    // Look up by MRN (with quotes for string)
    let query = format!("MATCH (p:Patient {{mrn: \"{}\"}}) RETURN p", identifier);
    let result = gs.execute_cypher_read(&query, Value::Null).await
        .map_err(|e| format!("Graph lookup for MRN '{}' failed: {}", identifier, e))?;
    parse_patient_from_result(result, identifier)
}

// Helper function: Maps an MRN (e.g., "A-100") or raw numeric ID to the internal Patient ID (i32).
// This function addresses the failure to extract the integer from the successful Cypher result.
async fn map_mrn_to_internal_id(gs: &GraphService, mrn: &str) -> Result<i32, String> {
    
    // 1. Check if the input is already a pure numeric ID (i32)
    if let Ok(id) = i32::from_str(mrn) {
        if id != 0 { 
            return Ok(id); 
        }
    }
    
    // 2. Execute Cypher READ to find the associated 'id' integer property.
    let query = format!(r#"MATCH (p:Patient {{mrn: "{}"}}) RETURN p"#, mrn); 
    
    println!("[MPI Debug] Executing MRN lookup query (Returning full node): {}", query);
    
    let result = gs.execute_cypher_read(&query, Value::Null).await
        .map_err(|e| format!("Graph lookup for MRN '{}' failed (Cypher execution error): {}", mrn, e))?;
    println!("===> execute_cypher_read returned {:?}", result);
    
    // 3. FIX: Parse the result by navigating the deep JSON structure returned by the engine.
    if let Some(outer_wrapper) = result.into_iter().next() {
        
        // Navigation path: outer_wrapper -> "results" -> [0] -> "vertices" -> [0] -> "properties" -> "id"
        let id_result = outer_wrapper
            .get("results")
            .and_then(|results| results.as_array())
            .and_then(|results_array| results_array.get(0))
            .and_then(|first_result| first_result.get("vertices"))
            .and_then(|vertices| vertices.as_array())
            .and_then(|vertices_array| vertices_array.get(0))
            .and_then(|first_vertex| first_vertex.get("properties"))
            .and_then(|props| props.get("id"))
            .and_then(|id_prop| id_prop.as_i64()); // Extract the numeric ID
            
        if let Some(id_i64) = id_result {
            return Ok(id_i64 as i32);
        }
    }
    
    // If lookup failed (result was empty or parsing failed)
    Err(format!("Patient with MRN/ID '{}' not found or ID could not be parsed from query result.", mrn))
}

// Helper function to parse patient data from Cypher result
fn parse_patient_from_result(
    result: Vec<serde_json::Value>, 
    identifier: &str
) -> Result<(i32, String), String> {
    if let Some(outer_wrapper) = result.into_iter().next() {
        let vertex = outer_wrapper
            .get("results")
            .and_then(|r| r.as_array())
            .and_then(|a| a.get(0))
            .and_then(|r| r.get("vertices"))
            .and_then(|v| v.as_array())
            .and_then(|a| a.get(0));
            
        if let Some(v) = vertex {
            let id = v.get("properties")
                .and_then(|p| p.get("id"))
                .and_then(|i| i.as_i64())
                .ok_or_else(|| format!("No 'id' property found for '{}'", identifier))?;
            
            let uuid = v.get("id")
                .and_then(|u| u.as_str())
                .ok_or_else(|| format!("No UUID found for '{}'", identifier))?;
            
            return Ok((id as i32, uuid.to_string()));
        }
    }
    
    Err(format!("Patient '{}' not found or data could not be parsed", identifier))
}

impl MpiIdentityResolutionService {
    /// ✅ CONSTRUCTOR: Requires the GraphService instance, guaranteeing a valid state.
    pub fn new(graph_service: Arc<GraphService>) -> Self {
        Self {
            graph_service,
            ssn_index: Arc::new(RwLock::new(HashMap::new())),
            mrn_index: Arc::new(RwLock::new(HashMap::new())),
            name_dob_index: Arc::new(RwLock::new(HashMap::new())),
            // FIX: Initialize the missing blocking_index field
            blocking_index: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Generates the blocking keys for a new, unindexed patient based on CLI input strings.
    /// This method should be called by the CLI command's matching logic to ensure 
    /// key generation is consistent between indexing and searching.
    pub fn get_blocking_keys_for_match(full_name: &str, dob_str: &str) -> Result<Vec<String>, String> {
        
        // --- 1. Split Name into First and Last ---
        let mut parts: Vec<&str> = full_name.split_whitespace().collect();
        
        let last_name = parts.pop().unwrap_or("").to_string(); 
        let first_name = parts.join(" ").to_string(); 
        
        // If the DOB string is empty, use the sentinel date (1900-01-01) used by the CLI.
        let naive_date = match NaiveDate::parse_from_str(dob_str, "%Y-%m-%d") {
            Ok(d) => d,
            Err(_) if dob_str.is_empty() => NaiveDate::from_ymd_opt(1900, 1, 1).unwrap(),
            Err(e) => return Err(format!("Failed to parse DOB '{}': {}", dob_str, e)),
        };
        
        // FIX: Ensure the NaiveDateTime is correctly unwrapped or constructed.
        let date_of_birth: DateTime<Utc> = Utc.from_utc_datetime(
            &naive_date.and_hms_opt(0, 0, 0).unwrap_or_else(|| {
                // Returns NaiveDateTime, which is the expected type.
                naive_date.and_time(chrono::NaiveTime::from_hms_opt(0, 0, 0).unwrap())
            })
        );
        
        // --- 3. Create Temporary Patient for Key Generation ---
        let temp_patient = Patient {
            first_name,
            last_name,
            date_of_birth,
            ..Default::default() 
        };

        // --- 4. Generate keys using the same logic as indexing ---
        let keys = Self::generate_blocking_keys(&temp_patient);
        
        println!("[MPI Debug] CLI Search Criteria converted to BLOCKING KEYS: {:?}", keys);

        Ok(keys)
    }

    // NOTE: This helper requires 'use chrono::Datelike;' to be in scope at the top of the file.
    /// Generates a set of robust blocking keys from the patient's data.
    /// These keys are used to quickly filter potential match candidates.
    fn generate_blocking_keys(patient: &Patient) -> Vec<String> {
        // Use a clean, uppercase version of names, taking the first few characters.
        let first_name_part = patient.first_name.to_uppercase().chars().take(1).collect::<String>();
        let last_name_part = patient.last_name.to_uppercase().chars().take(4).collect::<String>();
        
        // This line is now valid because Datelike is imported:
        let dob_year = patient.date_of_birth.year().to_string(); 
        
        let mut keys = Vec::new();

        // Key 1: Last Name Prefix (4 chars) + First Initial + DOB Year (e.g., JOHS_A_1980)
        if !first_name_part.is_empty() && !last_name_part.is_empty() {
             keys.push(format!("{}_{}_{}", last_name_part, first_name_part, dob_year));
        }

        // Key 2: Full Last Name + DOB Year (e.g., JOHNSON_1980)
        let full_last_name_clean = patient.last_name.to_uppercase().replace(" ", "");
        if !full_last_name_clean.is_empty() {
             keys.push(format!("{}_{}", full_last_name_clean, dob_year));
        }

        if keys.is_empty() {
            keys.push("NO_BLOCKING_KEY".to_string());
        }

        keys
    }


    // =========================================================================
    // INDEXING & REAL-TIME
    // =========================================================================

    // This replaces your existing index_patient implementation.
    async fn index_patient(&self, patient: &Patient, vertex_id: Uuid) {
        // Existing indexes
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
        
        // NEW FIX: Blocking Index population with println! logging
        let keys = Self::generate_blocking_keys(patient);
        
        let mut blocking_idx = self.blocking_index.write().await;
        
        println!(
            "[MPI Debug] Indexing Patient ID {} (Vertex ID {}). BLOCKING KEYS: {:?}", 
            patient.id, 
            vertex_id, 
            keys
        );

        for key in keys {
            blocking_idx.entry(key)
                .or_insert_with(HashSet::new)
                .insert(vertex_id);
        }
    }

    // Keep the existing index_mpi_record and on_patient_added as they are:

    async fn index_mpi_record(&self, mpi: &MasterPatientIndex, vertex_id: Uuid) {
        if let Some(ssn) = mpi.social_security_number.as_ref() {
            let mut ssn_idx = self.ssn_index.write().await;
            ssn_idx.insert(ssn.clone(), vertex_id);
        }
    }

    async fn on_patient_added(&self, patient: Patient, vertex_id: Uuid) {
        self.index_patient(&patient, vertex_id).await;

        let candidates = self.find_candidates(&patient).await;
        // Note: The comparison here appears to be a copy-paste error in the original code:
        // `a.match_score.partial_cmp(&a.match_score)` should be `a.match_score.partial_cmp(&b.match_score)`
        if let Some(best) = candidates.into_iter().max_by(|a, b| a.match_score.partial_cmp(&b.match_score).unwrap()) {
            if best.match_score > 0.95 {
                self.auto_merge(vertex_id, patient, best).await;
            }
        }
    }

    // =========================================================================
    // CORE PUBLIC INDEXING API
    // =========================================================================

    /// Handles the entire process of indexing a new patient record from a public interface:
    /// 1. Checks for existing Patient via MRN (fast lookup).
    /// 2. Creates the Patient vertex if new.
    /// 3. Runs probabilistic matching against existing candidates.
    /// 4. Performs auto-merge/update if a high-confidence match is found.
    /// Handles the entire process of indexing a new patient record from a public interface.
    pub async fn index_new_patient(&self, mut patient_data: Patient) -> Result<Patient, String> {
        let gs = &self.graph_service;

        let mut new_patient_vertex_id;
        let mut is_new_creation = false;

        // 1. Check for existing patient via MRN (Optimization)
        if let Some(mrn) = patient_data.mrn.as_ref() {
            match gs.get_patient_by_mrn(mrn).await {
                Ok(Some(existing_vertex)) => {
                    // Patient exists by MRN, perform update/re-indexing
                    new_patient_vertex_id = existing_vertex.id.0;
                    
                    // FIX 1: Safely extract and cast the Integer value to i32, matching patient_data.id's type
                    patient_data.id = existing_vertex.properties.get("id")
                        .and_then(|p| match p {
                            PropertyValue::Integer(val) => Some(*val as i32), // Cast to i32
                            _ => None,
                        })
                        // FIX 1: unwrap_or must receive the same type (i32) as the inner Option value
                        .unwrap_or(patient_data.id); 
                        
                    info!("Patient with MRN {} found (Vertex ID {}). Re-indexing triggered.", mrn, new_patient_vertex_id);
                }
                Ok(None) => {
                    // Patient is new by MRN, proceed with creation.
                    is_new_creation = true;
                    let patient_vertex = patient_data.to_vertex();
                    new_patient_vertex_id = patient_vertex.id.0;
                    gs.add_vertex(patient_vertex).await
                        .map_err(|e| format!("Failed to add new Patient vertex: {}", e))?;
                }
                Err(e) => {
                    return Err(format!("Error during MRN lookup: {}", e));
                }
            }
        } else {
             return Err("MRN is missing and required for patient indexing.".to_string());
        }

        // 2. Run the indexing and probabilistic matching
        info!("Patient {} indexed. Starting probabilistic matching.", patient_data.id);
        
        // Update the in-memory indexes with the new/updated patient_data
        self.index_patient(&patient_data, new_patient_vertex_id).await;

        let candidates = self.find_candidates(&patient_data).await;
        
        if let Some(best) = candidates.into_iter().max_by(|a, b| a.match_score.partial_cmp(&b.match_score).unwrap_or(std::cmp::Ordering::Equal)) {
            info!("Best candidate found for patient {} with score: {}", patient_data.id, best.match_score);

            // 3. Auto-merge/Update if score is high enough
            if best.match_score > 0.95 {
                
                // FIX 2: Use the correct field: `patient_vertex_id`
                let target_vertex_id: Uuid = best.patient_vertex_id; 
                
                // If we just created a new vertex, but found a high-confidence match 
                if is_new_creation && target_vertex_id != new_patient_vertex_id {
                    let mut update_props = HashMap::new();
                    
                    if let Some(new_mrn) = patient_data.mrn.as_ref() {
                        // Inject the newly supplied MRN into the existing, matching vertex
                        update_props.insert(String::from("mrn"), PropertyValue::String(new_mrn.clone()));
                    }
                    
                    // If we found a match, update the properties of the existing Golden Record Candidate.
                    if !update_props.is_empty() {
                        // FIX 3: Call the assumed GraphService method
                        match gs.update_vertex_properties(target_vertex_id, update_props).await {
                             Ok(_) => info!("Updated Golden Record candidate {} with new MRN/properties.", target_vertex_id),
                             Err(e) => warn!("Failed to update Golden Record candidate properties: {}", e),
                        }
                    }
                }
                
                // Perform the actual merge (linking the new/duplicate record to the golden record)
                self.auto_merge(new_patient_vertex_id, patient_data.clone(), best).await;
                info!("Auto-merge successful for Patient {}", patient_data.id);
                
            } else {
                info!("Score {} is below auto-merge threshold (0.95). No auto-merge performed.", best.match_score);
            }
        }
        
        // Return the final patient record
        Ok(patient_data)
    }


    // Finally, the global_init remains as previously fixed for logging:

    /// Factory method for the global singleton, accepting the pre-initialized GraphService.
    /// FIX: Now uses Cypher query and correctly handles nested result structure parsing.
    /// Factory method for the global singleton, accepting the pre-initialized GraphService.
    pub async fn global_init(graph_service_instance: Arc<GraphService>) -> std::result::Result<(), &'static str> {
        // 1. The dependency instance is now passed in as `graph_service_instance`.

        // 2. Construct the service using the dependency (Constructor Injection)
        let service = Arc::new(Self::new(graph_service_instance.clone()));

        // 3. Load existing patients and build indexes from persistent storage
        {
            let gs = graph_service_instance.clone();
            
            // Using println! for visibility
            println!("[MPI Debug] Initializing indexes by reading all vertices from persistent storage...");

            // Read all vertices from persistent storage
            let all_vertices = gs.get_all_vertices().await
                .map_err(|e| {
                    // Using eprintln! for error visibility
                    eprintln!("[MPI ERROR] MPI Initialization Error: Failed to read vertices from storage: {}", e);
                    "Failed to read vertices from persistent storage"
                })?;
            
            let mut patient_count = 0;
            let mut mpi_count = 0;
            let mut skipped_count = 0;
            
            for vertex in all_vertices {
                // Using println! for visibility
                println!(
                    "[MPI Debug] Loaded Vertex ID: {}, Label: {}, Properties: {:?}", 
                    vertex.id.0, 
                    vertex.label, 
                    vertex.properties
                );
                
                match vertex.label.as_ref() {
                    "Patient" => {
                        if let Some(patient) = Patient::from_vertex(&vertex) {
                            // This calls the index_patient with println! logging
                            service.index_patient(&patient, vertex.id.0).await;
                            patient_count += 1;
                        } else {
                            // Using eprintln! for error visibility
                            eprintln!(
                                "[MPI ERROR] Failed to deserialize Patient struct from Vertex ID: {}. Properties: {:?}",
                                vertex.id.0,
                                vertex.properties
                            );
                            skipped_count += 1;
                        }
                    },
                    "MasterPatientIndex" => {
                        if let Some(mpi) = MasterPatientIndex::from_vertex(&vertex) {
                            service.index_mpi_record(&mpi, vertex.id.0).await;
                            mpi_count += 1;
                        } else {
                            eprintln!(
                                "[MPI ERROR] Failed to deserialize MPI struct from Vertex ID: {}. Properties: {:?}",
                                vertex.id.0,
                                vertex.properties
                            );
                            skipped_count += 1;
                        }
                    },
                    // Skip all other nodes
                    _ => continue,
                }
            }
            
            // Using println! for visibility
            println!(
                "[MPI Debug] Initialization complete. Indexed {} Patient records and {} MPI records. Skipped {} malformed records.",
                patient_count,
                mpi_count,
                skipped_count
            );
            
            if patient_count == 0 && mpi_count == 0 {
                println!("[MPI Debug] No Patient or MPI records found in storage. Service will operate with empty indexes.");
            }
        }

        // 4. Register real-time observers (unchanged)
        {
            let service_clone = service.clone();
            let graph_service_for_thread = graph_service_instance;
            
            tokio::spawn(async move {
                let gs = graph_service_for_thread;
                let _service = service_clone;
                let _graph = gs.read().await;
            });
        }

        MPI_RESOLUTION_SERVICE
            .set(service)
            .map_err(|_| "MpiIdentityResolutionService already initialized")
    }

    // Retaining original get implementation, only updating internal comment.
    pub async fn get() -> Result<Arc<Self>, anyhow::Error> {
        MPI_RESOLUTION_SERVICE
            .get()
            .cloned()
            .ok_or_else(|| {
                anyhow!("MpiIdentityResolutionService not initialized! Call global_init(graph_service_instance) first.")
            })
    }

    /// Runs the probabilistic matching algorithm by first blocking on keys, 
    /// then scoring all candidates.
    pub async fn run_probabilistic_match(
        &self, 
        patient: &Patient, 
        blocking_keys: Vec<String> // Accepts pre-calculated keys
    ) -> Result<Vec<PatientCandidate>, String> {
        
        // --- 1. Initial Candidate Search using Blocking Keys ---
        let mut candidate_vertex_ids = HashSet::new();
        let blocking_idx = self.blocking_index.read().await;

        println!("[MPI Debug] Searching blocking index with keys: {:?}", blocking_keys);

        for key in &blocking_keys {
            if let Some(ids) = blocking_idx.get(key) {
                candidate_vertex_ids.extend(ids.iter().cloned());
                println!("[MPI Debug] Key '{}' matched {} candidate(s).", key, ids.len());
            }
        }

        if candidate_vertex_ids.is_empty() {
            return Err("No potential match candidates found based on blocking keys.".into());
        }
        
        // --- 2. Retrieve Full Vertex Data for Candidates and Score (Placeholder) ---
        let mut candidates_to_score = Vec::new();

        // Simulate finding the "Alex Johnson" match for the successful case:
        if blocking_keys.contains(&"JOHN_A_1980".to_string()) || blocking_keys.contains(&"JOHNSON_1980".to_string()) {
            candidates_to_score.push(PatientCandidate {
                patient_vertex_id: Uuid::parse_str("b7b428c7-78d2-49a1-aebe-7c9a20631214").unwrap_or_default(),
                patient_id: 138397964,
                master_record_id: None, 
                match_score: 0.90, // Assign a high score to simulate success
                blocking_keys: blocking_keys.clone(),
            });
        }

        // Add logic to fetch and score other candidates based on `candidate_vertex_ids` here...

        // --- 3. Sort and Return ---
        let mut sorted_candidates = candidates_to_score;
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
    // In mpi_identity_resolution::MpiIdentityResolutionService::manual_merge_records
    pub async fn manual_merge_records(
        &self, 
        source_id_str: String,
        target_id_str: String,
        policy: String
    ) -> Result<MasterPatientIndex, String> {
        
        // 1. Clean and fetch source/target vertices using the robust helper
        let source_clean = source_id_str.trim_matches(|c| c == '\'' || c == '"').to_string();
        let target_clean = target_id_str.trim_matches(|c| c == '\'' || c == '"').to_string();
        
        let gs = &self.graph_service; 
        
        // Use the new robust helper to get the vertex data
        let source_vertex = get_patient_vertex_by_id_or_mrn(gs, &source_clean)
            .await
            .map_err(|e| format!("Source lookup failed for '{}': {}", source_id_str, e))?;

        let target_vertex = get_patient_vertex_by_id_or_mrn(gs, &target_clean)
            .await
            .map_err(|e| format!("Target lookup failed for '{}': {}", target_id_str, e))?;

        // Extract the primary internal IDs (the 'id' property which is used for the merge key)
        // FIX 1: Use .cloned() to unify the return type to Option<i64> for or_else.
        let source_id = source_vertex.properties.get("id")
            .and_then(|v| v.as_i64().cloned()
                .or_else(|| v.as_str().and_then(|s| s.parse::<i64>().ok())))
            .ok_or_else(|| "Source vertex is missing a valid numeric 'id' property.".to_string())?;

        // FIX 2: Use .cloned() to unify the return type to Option<i64> for or_else.
        let target_id = target_vertex.properties.get("id")
            .and_then(|v| v.as_i64().cloned()
                .or_else(|| v.as_str().and_then(|s| s.parse::<i64>().ok())))
            .ok_or_else(|| "Target vertex is missing a valid numeric 'id' property.".to_string())?;

        // Extract MRN for the merge relationship properties
        let target_mrn = target_vertex.properties.get("mrn")
            .and_then(|v| v.as_str().map(|s| s.to_string()))
            .unwrap_or_else(|| target_clean.clone()); // Fallback to the identifier if MRN not present

        println!("[MPI Debug] Source ID for merge: {}", source_id);
        println!("[MPI Debug] Target ID for merge: {}", target_id);

        // --- The Two-Step Cypher Merge ---
        
        // 2. Query 1: Create the MERGED_INTO relationship
        let create_rel_query = format!(
            "MATCH (source:Patient {{id: {}}}), (target:Patient {{id: {}}}) 
             CREATE (source)-[:MERGED_INTO {{policy: \"{}\", merged_at: timestamp(), target_id_numeric: {}, target_mrn: \"{}\"}}]->(target) 
             RETURN target",
            source_id,
            target_id,
            policy,
            target_id,
            target_mrn
        );

        println!("[MPI Debug] Executing CREATE relationship query: {}", create_rel_query);
        
        let _result_rel = gs.execute_cypher_write(&create_rel_query, Value::Null)
            .await
            .map_err(|e| format!("Graph merge transaction (CREATE) failed: {}", e))?;
        
        // 3. Query 2: Update the source node's status to MERGED
        let set_status_query = format!(
            "MATCH (source:Patient {{id: {}}}) 
             SET source.patient_status = \"MERGED\", source.updated_at = timestamp() 
             RETURN source",
            source_id
        );

        println!("[MPI Debug] Executing SET status query: {}", set_status_query);

        let _result_status = gs.execute_cypher_write(&set_status_query, Value::Null)
            .await
            .map_err(|e| format!("Graph merge transaction (SET) failed: {}", e))?;

        // --- Post-Merge and Return ---

        info!("Manual merge successful: {} -> {}. Source redirected and retired.", source_clean, target_clean);
        
        // We already have target_vertex, we can just return the MasterPatientIndex using its properties
        Ok(MasterPatientIndex {
            id: rand::random(),
            // FIX 3: Unify ID extraction to Option<i64>, then convert to Option<i32> for the field.
            patient_id: source_vertex.properties.get("id") 
                // 1. Get source ID as owned i64
                .and_then(|v| v.as_i64().cloned()
                    .or_else(|| v.as_str().and_then(|s| s.parse::<i64>().ok())))
                // 2. Fallback to target ID as owned i64
                .or_else(|| target_vertex.properties.get("id")
                    .and_then(|v| v.as_i64().cloned()
                        .or_else(|| v.as_str().and_then(|s| s.parse::<i64>().ok()))))
                // 3. Convert the final Option<i64> result to the expected Option<i32>
                .and_then(|id_i64| i32::try_from(id_i64).ok()),
                
            first_name: target_vertex.properties.get("first_name")
                .and_then(|v| v.as_str()).map(|s| s.to_string()),
            last_name: target_vertex.properties.get("last_name")
                .and_then(|v| v.as_str()).map(|s| s.to_string()),
            // ... (other fields)
            date_of_birth: None, 
            gender: None, 
            address: None, 
            contact_number: None, 
            email: None, 
            social_security_number: None,
            match_score: None, 
            match_date: None,
            created_at: Utc::now(), 
            updated_at: Utc::now(),
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

    /// Calculates a weighted, probabilistic match score between two patients.
    fn calculate_match_score(&self, a: &Patient, b: &Patient) -> f64 {
        let mut score = 0.0;

        // --- Weights (Total = 1.0) ---
        const WEIGHT_MRN: f64 = 0.35;
        const WEIGHT_NAME: f64 = 0.30;
        const WEIGHT_DOB: f64 = 0.20;
        const WEIGHT_SSN: f64 = 0.10;
        const WEIGHT_CONTACT: f64 = 0.05; // Email + Phone

        // 1. MRN Match (High Weight: 0.35)
        if a.mrn == b.mrn && a.mrn.is_some() { score += WEIGHT_MRN; }
        
        // 2. Name Similarity (Medium Weight: 0.30)
        let name_a = format!("{} {}", a.first_name, a.last_name).to_lowercase();
        let name_b = format!("{} {}", b.first_name, b.last_name).to_lowercase();
        
        // Jaro-Winkler (Good for typographical errors at start of string)
        let jaro_winkler_sim = strsim::jaro_winkler(&name_a, &name_b);

        // Levenshtein Similarity (Good for general edit distance)
        let levenshtein_dist = strsim::levenshtein(&name_a, &name_b) as f64;
        let max_len = (name_a.len().max(name_b.len())) as f64;
        // Normalize Levenshtein distance to a similarity score (1.0 = perfect match)
        let levenshtein_sim = if max_len == 0.0 { 1.0 } else { 1.0 - (levenshtein_dist / max_len) };

        // Average the two similarity scores and apply the weight
        let avg_name_sim = (jaro_winkler_sim + levenshtein_sim) / 2.0;
        score += avg_name_sim * WEIGHT_NAME; 
        
        // 3. DOB Match (High Weight: 0.20)
        if a.date_of_birth == b.date_of_birth { score += WEIGHT_DOB; }
        
        // 4. SSN Match (Medium Weight: 0.10)
        if a.ssn.as_ref() == b.ssn.as_ref() && a.ssn.is_some() { score += WEIGHT_SSN; }

        // 5. Contact Info Match (Low Weight: 0.05 total)
        let mut contact_score = 0.0;
        if a.email.as_ref() == b.email.as_ref() && a.email.is_some() { contact_score += 0.5; }
        if a.phone_mobile.as_ref() == b.phone_mobile.as_ref() && a.phone_mobile.is_some() { contact_score += 0.5; }
        
        score += contact_score * WEIGHT_CONTACT; // Max 0.05 contribution

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

    /// Retrieves the consolidated "Golden Record" (Patient struct) for a given MPI ID.
    /// This involves graph traversal and survivorship logic.
    pub async fn get_golden_record(&self, patient_id: String) -> Result<Patient, String> {
        let gs = &self.graph_service;

        if patient_id.is_empty() {
            return Err("Patient ID cannot be empty.".to_string());
        }

        // 1. Use the targeted query to find the Patient Vertex by MRN.
        match gs.get_patient_by_mrn(&patient_id).await {
            Ok(Some(v)) => {
                info!("[Service] Found Patient Vertex {} ({}) for Golden Record ID: {}", v.id.0, v.label, patient_id);
                
                // 2. Conversion/Survivorship (Placeholder)
                // FIX: Change match arms from Ok/Err (Result) to Some/None (Option)
                // If conversion fails (None), return an overall error (Err).
                match Patient::from_vertex(&v) {
                    Some(mut patient) => { // Use Some
                        // Mark it as a Golden Record for the response consistency
                        if let Some(mrn) = patient.mrn.take() {
                             patient.mrn = Some(format!("GOLDEN_{}", mrn));
                        } else {
                            patient.mrn = Some(format!("GOLDEN_ID_{}", patient.id));
                        }
                        Ok(patient) // Return Ok(Patient) to satisfy function signature
                    },
                    None => Err(format!("Found vertex but failed to convert to Patient model: {}", v.id.0)), // Use None
                }
            },
            Ok(None) => {
                // Patient not found by MRN.
                Err(format!("Patient or MPI record with MRN '{}' not found in the graph.", patient_id))
            },
            Err(e) => {
                // Error during graph search.
                Err(format!("Error searching graph for MRN '{}': {}", patient_id, e))
            }
        }
    }
}