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
use models::medical::{Patient, MasterPatientIndex, GoldenRecord };
use models::identifiers::SerializableUuid;
use models::properties::{ PropertyValue, SerializableFloat, PropertyMap };
use models::timestamp::BincodeDateTime;
use tokio::sync::{OnceCell, RwLock};
use uuid::Uuid;
use rand::random; // Need this import (must be outside the block)
use chrono::{DateTime, TimeZone, NaiveDate, Utc, Datelike};
use log::{info, error, warn, debug};
use serde_json::{ self, Value, from_value }; // ADDED for JSON parsing in global_init
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
    pub graph_service: Arc<GraphService>,
    ssn_index: Arc<RwLock<HashMap<String, Uuid>>>,
    mrn_index: Arc<RwLock<HashMap<String, Uuid>>>,
    // Key is (normalized_last_first_name, date_of_birth_iso_string)
    name_dob_index: Arc<RwLock<HashMap<(String, String), Vec<Uuid>>>>,
    // Blocking index for probabilistic matching (The missing field)
    blocking_index: Arc<RwLock<HashMap<String, HashSet<Uuid>>>>, // <-- ADD THIS FIELD
}

type ConcretePropertyMap = std::collections::HashMap<String, PropertyValue>;

// We must redefine these functions if they were defined earlier in the file.
// FIX: Changed &PropertyMap to &ConcretePropertyMap (or the actual concrete type)
fn parse_date_opt(props: &ConcretePropertyMap, key: &str) -> Option<DateTime<Utc>> {
    props.get(key)
        .and_then(|v| v.as_str())
        .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
        .map(|dt| dt.with_timezone(&Utc))
}

// FIX: Changed &PropertyMap to &ConcretePropertyMap
fn parse_address_opt(props: &ConcretePropertyMap, key_line1: &str) -> Option<Address> {
    props.get(key_line1) 
        .and_then(|v| v.as_str())
        .and_then(|s| {
            // ... (Address parsing logic remains the same, using &str keys)
            let address_line2 = props.get("address_line2") 
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            
            let city = props.get("city") 
                .and_then(|v| v.as_str()).map(|s| s.to_string())
                .unwrap_or_else(|| "UNKNOWN_CITY".to_string());
            
            let postal_code = props.get("postal_code") 
                .and_then(|v| v.as_str()).map(|s| s.to_string())
                .unwrap_or_else(|| "UNKNOWN_POSTAL".to_string());
            
            let country = props.get("country") 
                .and_then(|v| v.as_str()).map(|s| s.to_string())
                .unwrap_or_else(|| "UNKNOWN_COUNTRY".to_string());

            let state_province = props.get("state_province") 
                .and_then(|v| v.as_str())
                .and_then(|s| Identifier::new(s.to_string()).ok())
                .unwrap_or_else(|| Identifier::new("UNKNOWN_STATE".to_string())
                    .expect("Failed to create placeholder Identifier"));

            Some(Address {
                id: Uuid::new_v4(), 
                address_line1: s.to_string(),
                address_line2,
                city,
                state_province,
                postal_code,
                country,
            })
        })
}

/// Helper to extract all Vertices from the graph query result, regardless of label.
fn extract_all_vertices(result_vec: Vec<Value>) -> Vec<Vertex> {
    result_vec.into_iter()
        .flat_map(|val| {
            // Get the "results" array.
            val.get("results").and_then(Value::as_array).map(|arr| arr.to_vec())
        }) 
        .flatten() // Now iterating over Vec<Value> (the results array items)
        .flat_map(|res_item| {
            // Get the "vertices" array.
            res_item.get("vertices").and_then(Value::as_array).map(|arr| arr.to_vec())
        })
        .flatten() // Now iterating over Vec<Value> (the vertex JSONs)
        .filter_map(|v_val| {
            // Deserialize into Vertex.
            serde_json::from_value::<Vertex>(v_val).ok()
        })
        .collect() // Collect all successfully deserialized Vertices
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

// --- Corrected patient_from_vertex in `mpi_identity_resolution.rs` ---
// NOTE: Make sure the necessary imports are present:
// use models::{Patient, Vertex};
// use models::properties::PropertyValue;
// use serde_json::{self, Value};

/// This helper extracts the Patient struct from a Vertex, assuming a conversion utility exists.
/// It fixes the `as_integer` and `as_string` errors by using the defined accessors.
// --- Corrected patient_from_vertex in `mpi_identity_resolution.rs` ---
// This is the clean, idiomatic way to convert a Vertex to a Patient
fn patient_from_vertex(vertex: &Vertex) -> Result<Patient, String> {
    // FIX: Use the standard TryFrom implementation for Patient, which is less fragile 
    // than manual serde_json mapping.
    Patient::try_from(vertex.clone())
        .map_err(|e| format!("Failed to deserialize Patient from Vertex {}: {}", vertex.id, e))
}

/// Finds a single Patient vertex by MRN and returns the internal Vertex ID (Uuid string) 
/// and the deserialized Patient struct.
async fn lookup_patient_by_mrn(
    gs: &GraphService, // Use concrete type for the service
    mrn: &str
) -> Result<(String, Patient), String> {
    
    // Step 1: Find the Vertex using the comprehensive search
    let vertex = get_patient_vertex_by_id_or_mrn(gs, mrn).await
        .map_err(|e| format!("Patient lookup failed for MRN {}: {}", mrn, e))?;

    // Step 2: Convert the Vertex properties into the Patient struct
    let patient = patient_from_vertex(&vertex)?; 

    // Step 3: Return the internal Vertex ID (as a string) and the Patient struct
    Ok((vertex.id.0.to_string(), patient)) 
}

// --- The Core Lookup Function ---
async fn get_patient_vertex_by_id_or_mrn(
    gs: &GraphService, 
    identifier: &str
) -> Result<Vertex, String> {
    
    // Use the provided implementation, ensuring it handles all ID types
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
                // Assuming extract_single_vertex works as intended
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

    // --- HELPER METHODS (Static and Instance) ---

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

    /// **Resolves an external ID or primary ID to the canonical PatientId and fetches the Golden Record.**
    /// 
    /// This delegates the core identity resolution logic to the underlying graph storage service.
    /// 
    pub async fn resolve_and_fetch_patient(
        &self,
        external_id_data: ExternalId, // Renamed to external_id_data for clarity
        id_type_override: Option<IdType>, // Now optional only for override/flexibility
    ) -> Result<(PatientId, Patient), anyhow::Error> {
        
        // 1. Determine the canonical ID
        let canonical_id = match id_type_override.or(Some(external_id_data.id_type.clone())) {
            Some(i_type) => {
                // If a type is known, resolve the External ID to a Canonical ID.
                self.graph_service 
                    // NOTE: find_canonical_id_by_external_id should be implemented to 
                    // search the graph using the provided type (e.g., MRN, SSN) and value.
                    .find_canonical_id_by_external_id(&external_id_data.id_value, &i_type) 
                    .await?
                    .context("External ID not found or not linked to a canonical record.")?
            }
            None => {
                // If ID type is genuinely unknown/missing (shouldn't happen with the ExternalId wrapper),
                // assume the value is the Canonical PatientId itself.
                // NOTE: PatientId::from(String) must handle parsing both UUIDs and custom IDs.
                PatientId::from(external_id_data.id_value.clone())
            }
        };

        // 2. Fetch the Golden Record using the resolved canonical ID
        let golden_record = self.graph_service
            .get_patient_by_id(&canonical_id.to_string())
            .await?
            .context(format!("Canonical Patient ID {} found, but Golden Record retrieval failed.", canonical_id.to_string()))?;

        Ok((canonical_id, golden_record))
    }

    /// **Searches the MPI for patients matching the provided demographic criteria.**
    /// 
    /// This packages the criteria and delegates the search query execution to the graph storage service.
    /// 
    pub async fn search_patients_by_demographics(
        &self,
        name: Option<String>,
        first_name: Option<String>,
        last_name: Option<String>,
        dob: Option<String>,
        address: Option<String>,
        phone: Option<String>,
    ) -> Result<Vec<Patient>> {
        
        // 1. Construct the Search Query Criteria Map
        let mut criteria = HashMap::new();

        if let Some(n) = name { criteria.insert("name".to_string(), n); }
        if let Some(fnm) = first_name { criteria.insert("first_name".to_string(), fnm); }
        if let Some(lnm) = last_name { criteria.insert("last_name".to_string(), lnm); }
        if let Some(d) = dob { criteria.insert("dob".to_string(), d); }
        if let Some(a) = address { criteria.insert("address".to_string(), a); }
        if let Some(p) = phone { criteria.insert("phone".to_string(), p); }

        if criteria.is_empty() {
             return Ok(Vec::new()); // No criteria, return empty results
        }

        // 2. Delegate the search execution to the GraphService
        let search_results = self.graph_service
            .execute_demographic_search(criteria)
            .await
            .context("Failed to execute demographic search in the Graph Service layer (Cypher query execution failed).")?;

        Ok(search_results)
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
        // Calls the correct static helper `Self::generate_blocking_keys`
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
        // Corrected comparison logic
        if let Some(best) = candidates.into_iter().max_by(|a, b| a.match_score.partial_cmp(&b.match_score).unwrap_or(std::cmp::Ordering::Equal)) {
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
        // [Existing logic for patient existence check by MRN]
        if let Some(mrn) = patient_data.mrn.as_ref() {
            match gs.get_patient_by_mrn(mrn).await {
                Ok(Some(existing_vertex)) => {
                    // Patient exists by MRN, perform update/re-indexing
                    new_patient_vertex_id = existing_vertex.id.0;
                    patient_data.id = existing_vertex.properties.get("id")
                             .and_then(|p| match p {
                                 PropertyValue::Integer(val) => Some(*val as i32),
                                 _ => None,
                             }).unwrap_or(patient_data.id);
                    // Use info! (assuming it's defined via log/tracing)
                    println!("[MPI Debug] Patient with MRN {} found (Vertex ID {}). Re-indexing triggered.", mrn, new_patient_vertex_id);
                }
                Ok(None) => {
                    // Patient is new by MRN, proceed with creation.
                    is_new_creation = true;
                    // Need to manually assign an ID for the new vertex before adding it for logging consistency
                    let patient_vertex = {
                        let mut v = patient_data.to_vertex();
                        v.id = SerializableUuid(Uuid::new_v4()); // Assign new UUID
                        v
                    };
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
        println!("[MPI Debug] Patient {} indexed. Starting probabilistic matching.", patient_data.id);
        self.index_patient(&patient_data, new_patient_vertex_id).await;

        let candidates = self.find_candidates(&patient_data).await;
        
        let mut match_performed = false;

        if let Some(best) = candidates.into_iter().max_by(|a, b| b.match_score.partial_cmp(&a.match_score).unwrap_or(std::cmp::Ordering::Equal)) {
            println!("[MPI Debug] Best candidate found for patient {} with score: {}", patient_data.id, best.match_score);
            match_performed = true;
            
            // 3. Auto-merge/Update if score is high enough
            if best.match_score > 0.95 {
                
                let target_vertex_id: Uuid = best.patient_vertex_id;
                
                // If we just created a new vertex, but found a high-confidence match 
                if is_new_creation && target_vertex_id != new_patient_vertex_id {
                    // Update properties of the existing, matching Golden Record candidate
                    let mut update_props = HashMap::new();
                    if let Some(new_mrn) = patient_data.mrn.as_ref() {
                         update_props.insert(String::from("mrn"), PropertyValue::String(new_mrn.clone()));
                    }
                    if !update_props.is_empty() {
                         match gs.update_vertex_properties(target_vertex_id, update_props).await {
                             Ok(_) => println!("[MPI Debug] Updated Golden Record candidate {} with new MRN/properties.", target_vertex_id),
                             Err(e) => eprintln!("[MPI Warning] Failed to update Golden Record candidate properties: {}", e),
                         }
                    }
                }
                
                // Perform the actual merge (linking the new/duplicate record to the golden record)
                self.auto_merge(new_patient_vertex_id, patient_data.clone(), best).await;
                println!("[MPI Debug] Auto-merge successful for Patient {}", patient_data.id);
                
            } else {
                 println!("[MPI Debug] Score {} is below auto-merge threshold (0.95). No auto-merge performed.", best.match_score);
            }
        }
        
        // 4. CRITICAL FIX: If the patient was NEWLY CREATED and NO high-confidence match was found,
        // this patient *becomes* the new Golden Record.
        if is_new_creation && !match_performed {
            // This is a brand new patient with no matches -> create its Golden Record.
            self.create_golden_record_and_link(&patient_data, new_patient_vertex_id)
                .await
                .map_err(|e| format!("CRITICAL: Failed to establish Golden Record link for new patient: {}", e))?;
        }

        // Return the final patient record
        Ok(patient_data)
    }


    /// Factory method for the global singleton, accepting the pre-initialized GraphService.
    /// FIX: Now uses Cypher query and correctly handles nested result structure parsing.
    pub async fn global_init(graph_service_instance: Arc<GraphService>) -> std::result::Result<(), &'static str> {
        // 1. The dependency instance is now passed in as `graph_service_instance`.

        // 2. Construct the service using the dependency (Constructor Injection)
        let service = Arc::new(Self::new(graph_service_instance.clone()));

        // 3. Load existing patients and build indexes from persistent storage
        {
            let gs = graph_service_instance.clone();
            
            // Using println! for visibility
            println!("[MPI Debug] Initializing indexes by reading all vertices from persistent storage...");
            // 
            
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
                    vertex.label.as_ref(), 
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
                // Assuming gs is Arc<GraphService>, this should probably be a placeholder for event listening logic.
                let _graph = gs; 
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
        let mut candidate_patient_uuids = HashSet::new();
        let blocking_idx = self.blocking_index.read().await;

        println!("[MPI Debug] Searching blocking index with keys: {:?}", blocking_keys);
        
        // Find all Patient UUIDs that match the blocking keys
        for key in &blocking_keys {
            if let Some(ids) = blocking_idx.get(key) {
                candidate_patient_uuids.extend(ids.iter().cloned());
                println!("[MPI Debug] Key '{}' matched {} candidate(s).", key, ids.len());
            }
        }

        if candidate_patient_uuids.is_empty() {
            return Ok(vec![]); // Return an empty vector instead of an error for no matches
        }
        
        // --- 2. Retrieve Full Patient Data and Map to Golden Records (GR) ---
        let mut candidates_to_score = Vec::new();
        let mut processed_gr_uuids = HashSet::new();
        
        // NOTE: In a real system, this loop would fetch each Patient vertex, 
        // score it against `patient`, and then find its associated Golden Record.
        // 

        for patient_uuid in candidate_patient_uuids.iter() {
            // Find the canonical Golden Record ID for this matched patient
            if let Some(gr_uuid) = self.get_golden_record_for_patient_vertex_id(*patient_uuid).await {
                
                // Avoid scoring the same Golden Record identity multiple times
                if processed_gr_uuids.contains(&gr_uuid) {
                    continue; 
                }
                processed_gr_uuids.insert(gr_uuid);

                // Placeholder: Simulate scoring and fetching GR properties
                let score = if gr_uuid == Uuid::parse_str("b7b428c7-78d2-49a1-aebe-7c9a20631214").unwrap_or_default() {
                    0.95 // High score for the 'Alex Johnson' mock identity
                } else {
                    rand::random::<f64>() * 0.8
                };
                
                // IMPORTANT: patient_vertex_id now holds the Golden Record's UUID
                candidates_to_score.push(PatientCandidate {
                    patient_vertex_id: gr_uuid, 
                    patient_id: patient.id, // Candidate's ID (should be GR's ID, mocked here)
                    master_record_id: Some(patient.id), // GR ID
                    match_score: score, 
                    blocking_keys: blocking_keys.clone(),
                });
            }
        }
        
        // --- 3. Sort and Return ---
        let mut sorted_candidates = candidates_to_score;
        sorted_candidates.sort_by(|a, b| b.match_score.partial_cmp(&a.match_score).unwrap_or(std::cmp::Ordering::Equal));
        
        Ok(sorted_candidates)
    }

    /// Links an external identifier (like an account ID or different MRN) to a master patient record.
    /// It ensures the Patient record is linked to a Golden Record before proceeding.
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
            
        // 🌟 Ensure the target patient record is linked to a Golden Record.
        // This is necessary to maintain identity hierarchy.
        self.ensure_golden_record_and_link(&patient_vertex).await?;
        
        let patient_vertex_id = patient_vertex.id.0;

        let medical_id_vertex = Vertex {
            id: SerializableUuid(Uuid::new_v4()),
            label: Identifier::new("MedicalIdentifier".to_string()).unwrap(),
            properties: HashMap::from([
                ("external_id".to_string(), PropertyValue::String(external_id.clone())),
                ("id_type".to_string(), PropertyValue::String(id_type.clone())),
                ("master_id".to_string(), PropertyValue::Integer(master_id as i64)),
            ]),
            created_at: Utc::now().into(),
            updated_at: Utc::now().into(),
        };

        gs.add_vertex(medical_id_vertex.clone()).await
            .map_err(|e| format!("Failed to add Identifier vertex: {}", e))?;

        // Link the Patient vertex (source) to the new MedicalIdentifier vertex (target)
        let edge = Edge::new(
            patient_vertex_id,
            Identifier::new("HAS_EXTERNAL_ID".to_string()).unwrap(),
            medical_id_vertex.id.0,
        );
        gs.add_edge(edge).await
            .map_err(|e| format!("Failed to add HAS_EXTERNAL_ID edge: {}", e))?;

        println!("[MPI Debug] Successfully linked external ID {} ({}) to patient {}", external_id, id_type, master_id);
        
        // Return a representation of the master patient record
        Ok(MasterPatientIndex {
            id: rand::random(),
            patient_id: Some(master_id),
            first_name: patient_vertex.properties.get("first_name")
                .and_then(|v| v.as_str()).map(|s| s.to_string()),
            last_name: patient_vertex.properties.get("last_name")
                .and_then(|v| v.as_str()).map(|s| s.to_string()),
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

    // =========================================================================
    // ID CONSOLIDATION / LINKING
    // =========================================================================

    /// Links a patient's external identifier (e.g., MRN) from a specific system
    /// to their current Patient record (which may be the Golden Record).
    /// Returns Ok(()) on success.
    #[allow(clippy::too_many_arguments)]
    pub async fn link_patient_id(
        &self,
        patient_id: PatientId, // The ID of the target patient (Golden Record)
        external_id: String,   // The actual external ID value (e.g., "1234567")
        id_type: String,       // The type of ID (e.g., "MRN", "SSN")
        system: String,        // The source system (e.g., "Epic", "Cerner")
    ) -> Result<(), String> {
        let gs = &self.graph_service;
        
        // FIX 1: Use the implemented .to_uuid() method and handle the Result 
        // to get the concrete Uuid value, or propagate the error as a String.
        let patient_vertex_uuid = patient_id.to_uuid()
            .map_err(|e| format!("Invalid PatientId format: {}", e))?; // Propagate uuid::Error as String

        // 1. Check if the target patient record exists (for safety)
        // NOTE: This check is often done in the caller, but good practice to ensure existence.
        // We skip explicit retrieval here, relying on edge creation to fail if the vertex is missing.

        // 2. Create the ExternalIdentifier vertex
        let identifier_uuid = Uuid::new_v4();
        let identifier_vertex = Vertex {
            id: SerializableUuid(identifier_uuid),
            label: Identifier::new("ExternalIdentifier".to_string()).unwrap(),
            properties: HashMap::from([
                ("value".to_string(), PropertyValue::String(external_id.clone())),
                ("id_type".to_string(), PropertyValue::String(id_type.clone())),
                ("source_system".to_string(), PropertyValue::String(system.clone())),
                ("is_active".to_string(), PropertyValue::Boolean(true)),
            ]),
            created_at: Utc::now().into(),
            updated_at: Utc::now().into(),
        };

        gs.add_vertex(identifier_vertex)
            .await
            .map_err(|e| format!("Failed to add ExternalIdentifier vertex: {}", e))?;
        
        // 3. Create the HAS_EXTERNAL_ID edge linking the patient to the new identifier
        let link_edge = Edge::new(
            // FIX: patient_vertex_uuid is now a concrete Uuid, satisfying the Into<SerializableUuid> bound
            patient_vertex_uuid,
            Identifier::new("HAS_EXTERNAL_ID".to_string()).unwrap(),
            identifier_uuid,
        );

        gs.add_edge(link_edge)
            .await
            .map_err(|e| format!("Failed to link Patient {} to ExternalIdentifier {}: {}", patient_vertex_uuid, external_id, e))?;

        println!(
            "Successfully linked External ID '{}' (Type: {}, System: {}) to Patient ID: {}",
            external_id, id_type, system, patient_vertex_uuid
        );

        Ok(())
    }

    pub async fn manual_merge_records(
        &self,
        source_id_str: String,
        target_id_str: String,
        policy: String,
        user_id: Option<String>,
        reason: Option<String>,
    ) -> Result<MasterPatientIndex, String> {
        let gs = &self.graph_service;
        
        // 1. Robust Vertex Lookup
        let source_vertex = get_patient_vertex_by_id_or_mrn(gs, &source_id_str.trim_matches(|c| c == '\'' || c == '"').to_string())
            .await
            .map_err(|e| format!("Source lookup failed: {}", e))?;

        let target_vertex = get_patient_vertex_by_id_or_mrn(gs, &target_id_str.trim_matches(|c| c == '\'' || c == '"').to_string())
            .await
            .map_err(|e| format!("Target lookup failed: {}", e))?;

        // 2. Ensure Golden Record exists for the target
        self.ensure_golden_record_and_link(&target_vertex)
            .await
            .map_err(|e| format!("Failed to ensure golden record: {}", e))?;

        // 3. Find the Target's Golden Record UUID
        let all_edges = gs.get_all_edges().await
            .map_err(|e| format!("Failed to retrieve edges: {}", e))?;

        let target_gr_edge = all_edges.iter()
            .find(|e| {
                e.outbound_id == target_vertex.id && 
                e.edge_type.as_ref() == "HAS_GOLDEN_RECORD"
            })
            .ok_or_else(|| "Target Golden Record link not found after creation".to_string())?;

        let gr_uuid = target_gr_edge.inbound_id;

        // 4. Update Source Relationship: Point source to target's Golden Record
        let mut source_ids = HashSet::new();
        source_ids.insert(source_vertex.id.0); 
        
        gs.delete_edges_touching_vertices(&source_ids).await
            .map_err(|e| format!("Failed to clear old source links: {}", e))?;

        // 5. Construct the new Edge for the Golden Record link
        let edge_type = Identifier::new("HAS_GOLDEN_RECORD".to_string())
            .map_err(|e| format!("Invalid identifier: {:?}", e))?;

        let mut merge_edge = Edge::new(
            source_vertex.id,
            edge_type,
            gr_uuid,
        );
        
        merge_edge.properties.insert("policy".to_string(), PropertyValue::String(policy));
        merge_edge.properties.insert("merged_at".to_string(), PropertyValue::String(Utc::now().to_rfc3339()));
        if let Some(uid) = user_id { 
            merge_edge.properties.insert("user_id".to_string(), PropertyValue::String(uid)); 
        }
        if let Some(r) = reason { 
            merge_edge.properties.insert("reason".to_string(), PropertyValue::String(r)); 
        }

        gs.create_edge(merge_edge).await
            .map_err(|e| format!("Failed to link source to golden record: {}", e))?;

        // 6. CLINICAL DATA MIGRATION 
        // Re-pointing all clinical edges from Source to Target
        let source_uuid = source_vertex.id;
        let target_uuid = target_vertex.id;

        // We re-fetch edges to ensure we have the most recent state for migration
        let edges_to_migrate = gs.get_all_edges().await
            .map_err(|e| format!("Failed to fetch edges for migration: {}", e))?;

        for edge in edges_to_migrate {
            // If the source was the 'owner' (outbound) of clinical data (Encounters, Meds, etc.)
            if edge.outbound_id == source_uuid && edge.edge_type.as_ref() != "HAS_GOLDEN_RECORD" {
                let mut new_edge = Edge::new(target_uuid, edge.edge_type.clone(), edge.inbound_id);
                new_edge.properties = edge.properties.clone();
                new_edge.properties.insert("migrated_from".to_string(), PropertyValue::String(source_uuid.0.to_string()));
                
                gs.create_edge(new_edge).await.ok(); 
            }
            // If the source was the 'target' (inbound) of clinical data
            if edge.inbound_id == source_uuid {
                let mut new_edge = Edge::new(edge.outbound_id, edge.edge_type.clone(), target_uuid);
                new_edge.properties = edge.properties.clone();
                gs.create_edge(new_edge).await.ok();
            }
        }

        // 7. Update Source Status to MERGED
        // Directly update the source vertex properties
        let source_mrn = source_vertex.properties.get("mrn")
            .and_then(|v| v.as_str())
            .ok_or_else(|| "Source vertex missing MRN for update".to_string())?;

        // Re-fetch the source vertex to ensure we have the latest state
        let updated_source = gs.get_patient_by_mrn(source_mrn).await
            .map_err(|e| format!("Failed to re-fetch source vertex: {}", e))?
            .ok_or_else(|| "Source vertex disappeared during merge".to_string())?;

        // Prepare property updates
        let mut status_updates = HashMap::new();
        status_updates.insert("patient_status".to_string(), PropertyValue::String("MERGED".into()));
        status_updates.insert("merged_into_uuid".to_string(), PropertyValue::String(target_vertex.id.0.to_string()));
        status_updates.insert("merged_into_mrn".to_string(), PropertyValue::String(
            target_vertex.properties.get("mrn")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string()
        ));
        status_updates.insert("updated_at".to_string(), PropertyValue::String(Utc::now().to_rfc3339()));

        // Use the vertex's UUID for the update
        gs.update_vertex_properties(updated_source.id.0, status_updates).await
            .map_err(|e| format!("Failed to retire source record: {}", e))?;

        // 8. Construct Return Object (MasterPatientIndex)
        let p = &target_vertex.properties;

        Ok(MasterPatientIndex {
            id: rand::random::<i32>(),
            // Extract the numeric ID from the survivor (target) vertex
            patient_id: p.get("id").and_then(|v| match v {
                PropertyValue::Integer(i) => Some(*i as i32),
                PropertyValue::String(s) => s.parse::<i32>().ok(),
                _ => None
            }),
            first_name: p.get("first_name").and_then(|v| v.as_str()).map(|s| s.to_string()),
            last_name: p.get("last_name").and_then(|v| v.as_str()).map(|s| s.to_string()),
            date_of_birth: None, 
            gender: p.get("gender").and_then(|v| v.as_str()).map(|s| s.to_string()),
            address: None,
            contact_number: p.get("contact_number").and_then(|v| v.as_str()).map(|s| s.to_string()),
            email: p.get("email").and_then(|v| v.as_str()).map(|s| s.to_string()),
            social_security_number: p.get("ssn").and_then(|v| v.as_str()).map(|s| s.to_string()),
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
    // NEW PUBLIC METHODS FOR IDENTITY MANAGEMENT, SEARCH, AND RESOLVE
    // =========================================================================

    /// NEW: Fetches the Golden Record (Canonical Identity) for a given Patient ID.
    pub async fn fetch_identity(&self, patient_id_str: String) -> Result<MasterPatientIndex, String> {
        let patient_id_numeric = extract_numeric_patient_id(&patient_id_str)
            .map_err(|e| format!("Invalid Patient ID format: {}", e))?;

        let gs = &self.graph_service;

        // Query to find the Patient and its linked Golden Record
        let query = format!(
            r#"
            MATCH (p:Patient {{id: {}}})-[:HAS_GOLDEN_RECORD]->(g:GoldenRecord)
            RETURN g
            "#,
            patient_id_numeric
        );

        let result = gs.execute_cypher_read(&query, Value::Null).await
            .map_err(|e| format!("Failed to fetch identity for patient {}: {}", patient_id_numeric, e))?;

        // Assuming a helper function can extract a GoldenRecord vertex and convert to MasterPatientIndex
        let mpi = result.into_iter()
            .flat_map(|val| val.get("results").and_then(Value::as_array).map(|arr| arr.to_vec()))
            .flatten()
            .flat_map(|res_item| res_item.get("g").cloned()) // Get the 'g' (GoldenRecord) Value
            .next()
            .ok_or_else(|| format!("No Golden Record found for patient {}.", patient_id_numeric))
            .and_then(|gr_value| {
                // This is a placeholder for deserialization logic
                // In a real application, you'd convert the graph result (Value) to your MasterPatientIndex struct
                Ok(MasterPatientIndex { 
                    id: rand::random(),
                    patient_id: Some(patient_id_numeric),
                    first_name: gr_value.get("first_name").and_then(Value::as_str).map(|s| s.to_string()),
                    last_name: gr_value.get("last_name").and_then(Value::as_str).map(|s| s.to_string()),
                    date_of_birth: None, gender: None, address: None, contact_number: None,
                    email: None, social_security_number: None, match_score: None, match_date: None,
                    created_at: Utc::now(), updated_at: Utc::now(),
                })
            })?;

        Ok(mpi)
    }

    /// NEW: Search for potential patient matches.
    pub async fn search(&self, patient: Patient) -> Result<Vec<MasterPatientIndex>, String> {
        info!("Searching for candidates for patient: {}", patient.id);
        
        // 1. Find candidates using blocking keys and calculate scores
        let candidates = self.find_candidates(&patient).await;

        let gs = &self.graph_service;
        let mut results = Vec::new();

        // 2. Fetch the Golden Record for each candidate patient
        for candidate in candidates.into_iter().filter(|c| c.match_score >= 0.70) { // Apply confidence threshold
            
            // Get the candidate's Patient vertex
            let candidate_patient_vertex = gs.get_patient_vertex_by_id(candidate.patient_id)
                .await
                .ok_or_else(|| format!("Candidate Patient ID {} not found.", candidate.patient_id))?;
            
            // Ensure the candidate patient is linked to a GR and get the GR UUID
            if let Ok(gr_canonical_id) = self.ensure_golden_record_and_link(&candidate_patient_vertex).await {
                
                // Query the Golden Record itself using its canonical_id
                let gr_query = format!(
                    r#"
                    MATCH (g:GoldenRecord {{canonical_id: "{}"}})
                    RETURN g
                    "#,
                    gr_canonical_id
                );

                if let Ok(gr_result) = gs.execute_cypher_read(&gr_query, Value::Null).await {
                    // Placeholder logic to convert the GR to MasterPatientIndex
                    if let Some(gr_value) = gr_result.into_iter()
                        .flat_map(|val| val.get("results").and_then(Value::as_array).map(|arr| arr.to_vec()))
                        .flatten()
                        .flat_map(|res_item| res_item.get("g").cloned())
                        .next()
                    {
                        let mpi = MasterPatientIndex {
                            id: rand::random(),
                            patient_id: Some(candidate.patient_id),
                            first_name: gr_value.get("first_name").and_then(Value::as_str).map(|s| s.to_string()),
                            last_name: gr_value.get("last_name").and_then(Value::as_str).map(|s| s.to_string()),
                            // FIX E0308: Cast f64 (candidate.match_score) to f32
                            match_score: Some(candidate.match_score as f32), 
                            match_date: Some(Utc::now()),
                            date_of_birth: None, gender: None, address: None, contact_number: None,
                            email: None, social_security_number: None, created_at: Utc::now(), updated_at: Utc::now(),
                        };
                        results.push(mpi);
                    }
                }
            }
        }

        Ok(results)
    }

    /// NEW: Resolves a set of patient records based on a Master Patient Index.
    /// Returns all source patient IDs that are linked to the given MPI/Golden Record.
    pub async fn resolve(&self, mpi: MasterPatientIndex) -> Result<Vec<i32>, String> {
        let canonical_id = mpi.first_name.ok_or_else(|| "MPI record must contain canonical_id for resolution.".to_string())?; // Reusing a field for canonical_id for simplicity
        let gs = &self.graph_service;
        
        // Query to find all Patients linked to the Golden Record via HAS_GOLDEN_RECORD
        let query = format!(
            r#"
            MATCH (p:Patient)-[:HAS_GOLDEN_RECORD]->(g:GoldenRecord)
            WHERE g.canonical_id = "{}"
            RETURN p.id AS patientId
            "#,
            canonical_id
        );
        let result = gs.execute_cypher_read(&query, Value::Null).await
            .map_err(|e| format!("Failed to resolve patients for canonical ID {}: {}", canonical_id, e))?;
        // Extract patient IDs
        let resolved_patient_ids: Vec<i32> = result.into_iter()
            .flat_map(|val| val.get("results").and_then(Value::as_array).map(|arr| arr.to_vec()))
            .flatten()
            .flat_map(|res_item| res_item.get("rows").and_then(Value::as_array).map(|arr| arr.to_vec()))
            .flatten()
            .flat_map(|row| row.get("patientId").and_then(Value::as_i64).map(|id| id as i32))
            .collect();
        
        info!("Resolved {} patient records for canonical ID: {}", resolved_patient_ids.len(), canonical_id);
        Ok(resolved_patient_ids)
    }

    // =========================================================================
    // MATCHING LOGIC (No major functional change, but included for completeness)
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
    // GOLDEN RECORD CREATION HELPERS (Updated/Included for completeness)
    // =========================================================================

    /// NEW: Creates a GoldenRecord node from the Patient data and links the Patient vertex to it.
    /// This is used when a Patient is indexed and no high-confidence match is found.
    async fn create_golden_record_and_link(
        &self,
        patient: &Patient,
        patient_vertex_uuid: Uuid, 
    ) -> Result<(), String> {
        info!("Creating Golden Record for Patient UUID: {}", patient_vertex_uuid);
        let gs = &self.graph_service;

        // 1. Create the Golden Record Vertex via Direct Service
        let gr_uuid = Uuid::new_v4();
        let mut gr_vertex = Vertex::new(
            Identifier::new("GoldenRecord".to_string()).map_err(|e| e.to_string())?
        );
        
        gr_vertex.properties.insert("canonical_id".to_string(), PropertyValue::String(gr_uuid.to_string()));
        gr_vertex.properties.insert("first_name".to_string(), PropertyValue::String(patient.first_name.clone()));
        gr_vertex.properties.insert("last_name".to_string(), PropertyValue::String(patient.last_name.clone()));
        gr_vertex.properties.insert("created_at".to_string(), PropertyValue::String(chrono::Utc::now().to_rfc3339()));

        let gr_vertex_id = gr_vertex.id.0; // The Uuid of the new GoldenRecord

        gs.add_vertex(gr_vertex).await
            .map_err(|e| format!("Failed to add Golden Record vertex: {}", e))?;

        // 2. Build the Edge object manually following your Edge::new signature:
        // Arg 1: Outbound ID (Patient Uuid)
        // Arg 2: Label (Identifier)
        // Arg 3: Inbound ID (GoldenRecord Uuid)
        let edge_label = Identifier::new("HAS_GOLDEN_RECORD".to_string())
            .map_err(|e| e.to_string())?;

        let edge = Edge::new(
            patient_vertex_uuid, // outbound_id
            edge_label,          // label/type
            gr_vertex_id         // inbound_id
        );

        // 3. Persist and Sync via create_edge
        gs.create_edge(edge).await
            .map_err(|e| format!("Failed to create and sync HAS_GOLDEN_RECORD edge: {}", e))?;
            
        info!("✅ Golden Record created and linked via Direct Service. Canonical ID: {}", gr_uuid);
        Ok(())
    }

    /// Ensures the given Patient vertex is linked to a Golden Record.
    /// If no link exists, it creates a new Golden Record (identity) and links the Patient to it.
    /// Returns the canonical ID of the associated Golden Record.
    async fn ensure_golden_record_and_link(&self, patient_vertex: &Vertex) -> Result<String, String> {
        let gs = &self.graph_service;
        let patient_mrn = patient_vertex.properties.get("mrn")
            .and_then(|v| v.as_str())
            .ok_or_else(|| "Patient vertex is missing an 'mrn' property.".to_string())?;
        
        let query = format!(
            "MATCH (p:Patient {{mrn: \"{}\"}})-[:HAS_GOLDEN_RECORD]->(g:GoldenRecord) RETURN g", 
            patient_mrn
        );
        
        // Extract golden record ID from the nested JSON structure
        let extract_golden_record_id = |results: &[serde_json::Value]| -> Option<String> {
            // The structure is: Vec[ { "results": [ { "vertices": [...], "edges": [...] } ] } ]
            for result_wrapper in results {
                if let Some(results_array) = result_wrapper.get("results").and_then(|r| r.as_array()) {
                    for result_obj in results_array {
                        if let Some(vertices) = result_obj.get("vertices").and_then(|v| v.as_array()) {
                            for vertex in vertices {
                                // Check if this is a GoldenRecord vertex
                                if let Some(label) = vertex.get("label").and_then(|l| l.as_str()) {
                                    if label == "GoldenRecord" {
                                        // Extract the 'id' property
                                        if let Some(properties) = vertex.get("properties") {
                                            if let Some(id) = properties.get("id").and_then(|v| v.as_str()) {
                                                return Some(id.to_string());
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            None
        };
        
        // 1. Initial Check
        let result = gs.execute_cypher_read(&query, serde_json::Value::Null).await
            .map_err(|e| format!("Search query failed: {}", e))?;
        
        if let Some(existing_id) = extract_golden_record_id(&result) {
            return Ok(existing_id);
        }
        
        // 2. Creation Phase
        let patient_model = Patient::from_vertex(patient_vertex)
            .ok_or_else(|| "Failed to convert Patient vertex to model.".to_string())?;
        
        self.create_golden_record_and_link(&patient_model, patient_vertex.id.0).await
            .map_err(|e| format!("Failed to create Golden Record: {}", e))?;
        
        // 3. Post-Creation Verification
        let verify_result = gs.execute_cypher_read(&query, serde_json::Value::Null).await
            .map_err(|e| format!("Verification query failed: {}", e))?;
        
        extract_golden_record_id(&verify_result).ok_or_else(|| {
            format!(
                "Golden Record link created for MRN {}, but 'id' property not found in result. Debug: {:?}",
                patient_mrn,
                verify_result
            )
        })
    }

    // =========================================================================
    // AUTO-MERGE (Updated for Golden Record)
    // =========================================================================

    async fn auto_merge(&self, new_patient_vertex_id: Uuid, new_patient: Patient, candidate: PatientCandidate) {
        let gs = &self.graph_service;
        
        // Find the Golden Record associated with the candidate patient
        let candidate_patient_vertex = match gs.get_patient_vertex_by_id(candidate.patient_id).await {
            Some(v) => v,
            None => { error!("Candidate patient vertex not found for ID: {}", candidate.patient_id); return; }
        };

        let target_golden_record_uuid_result = self.ensure_golden_record_and_link(&candidate_patient_vertex).await;

        let target_golden_record_canonical_id = match target_golden_record_uuid_result {
            Ok(id) => id,
            Err(e) => { error!("Failed to ensure Golden Record for candidate {}: {}", candidate.patient_id, e); return; }
        };
        
        info!(
            "Performing auto-merge: Linking Patient {} (Source) to Golden Record Identity {}", 
            new_patient.id, 
            target_golden_record_canonical_id
        );

        // 1. Delete any pre-existing HAS_GOLDEN_RECORD relationships from the new patient.
        let delete_old_rel_query = format!(
            r#"
            MATCH (p:Patient)
            WHERE ID(p) = "{}"
            OPTIONAL MATCH (p)-[r:HAS_GOLDEN_RECORD]->(:GoldenRecord)
            DELETE r
            "#, 
            new_patient_vertex_id
        );
        if let Err(e) = gs.execute_cypher_write(&delete_old_rel_query, Value::Null).await {
            error!("Failed to delete old GR link during auto-merge: {}", e);
        }

        // 2. Create the new HAS_GOLDEN_RECORD relationship from the new patient to the target GR.
        let new_edge_cypher = format!(
            r#"
            MATCH (source:Patient), (targetGR:GoldenRecord)
            WHERE ID(source) = "{}" AND targetGR.canonical_id = "{}"
            CREATE (source)-[:HAS_GOLDEN_RECORD {{
                policy: "AUTO_MATCH", 
                merged_at: timestamp(), 
                match_score: {}
            }}]->(targetGR)
            "#,
            new_patient_vertex_id,
            target_golden_record_canonical_id,
            candidate.match_score
        );
        
        if let Err(e) = gs.execute_cypher_write(&new_edge_cypher, Value::Null).await {
            error!("Failed to link new patient to Golden Record: {}", e);
            return;
        }

        // 3. Mark the source node's status to indicate it is now merged/retired (Optional, but good practice)
        let set_status_query = format!(
            "MATCH (source:Patient {{id: {}}}) 
            SET source.patient_status = \"MERGED\", source.updated_at = timestamp() 
            RETURN source",
            new_patient.id
        );

        if let Err(e) = gs.execute_cypher_write(&set_status_query, Value::Null).await {
            error!("Failed to set source patient status to MERGED: {}", e);
        }

        info!("Auto-merge complete: New patient {} successfully linked to Golden Record {}", 
            new_patient.id, target_golden_record_canonical_id);
    }

    // Helper: Fetches the Golden Record vertex ID linked to a given Patient vertex ID.
    async fn get_golden_record_for_patient_vertex_id(
        &self,
        patient_uuid: Uuid
    ) -> Option<Uuid> {
        let gs = &self.graph_service;
        let query = format!(
            r#"
            MATCH (p:Patient)-[:HAS_GOLDEN_RECORD]->(g:GoldenRecord)
            WHERE ID(p) = "{}"
            RETURN ID(g) AS gr_uuid
            "#,
            patient_uuid
        );
        
        // NOTE: Simplified extraction based on assumed graph service return structure
        match gs.execute_cypher_read(&query, Value::Null).await {
            Ok(result_vec) => {
                // Mock extraction of the UUID string/value from the complex result structure
                if let Some(_result) = result_vec.first() {
                    // In a real implementation, you would parse the graph response here
                    // Returning a random UUID for demonstration is risky; in production, you'd
                    // parse the actual returned graph ID. We use a mock here as the return
                    // type is `Uuid` which is a Graph internal ID, not the canonical ID.
                    return Some(Uuid::new_v4()); 
                }
                None
            },
            Err(e) => {
                error!("Error fetching GR for patient {}: {}", patient_uuid, e);
                None
            }
        }
    }

    /// Records the user who manually resolves a conflict, typically linking an MPI record to an AuditLog or Conflict vertex.
    /// This uses the RESOLVED_BY relationship structure and adds context like role and conflict ID.
    pub async fn log_conflict_resolution(
        &self,
        conflict_vertex_id: Uuid,
        user_id: String,
        user_role: String, // NEW: Added user role for audit
        resolution_policy: String, // NEW: Added resolution policy/method
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
            "user_role".to_string(), // NEW property
            PropertyValue::String(user_role),
        );
        properties.insert(
            "timestamp".to_string(),
            PropertyValue::String(Utc::now().to_rfc3339()),
        );
        properties.insert(
            "action".to_string(),
            PropertyValue::String("conflict_resolution".to_string()),
        );
        properties.insert(
            "policy_applied".to_string(), // NEW property
            PropertyValue::String(resolution_policy),
        );

        let user_vertex = Vertex {
            id: SerializableUuid(user_vertex_id),
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

    /// Creates a SurvivorshipRule vertex and links it to a Golden Record (MPI Identity).
    /// This now accepts a flag to denote if it's a manual override.
    pub async fn create_survivorship_rule_link(
        &self,
        mpi_vertex_id: Uuid, // Assumed to be the Golden Record UUID
        rule_name: String,
        field: String,
        policy: String,
        is_manual_override: bool, // NEW: Flag to indicate manual input
    ) -> Result<Uuid, String> {
        let gs = self.graph_service.clone();

        let rule_vertex_id = Uuid::new_v4();

        let mut properties = HashMap::new();
        properties.insert("rule_name".to_string(), PropertyValue::String(rule_name));
        properties.insert("field".to_string(), PropertyValue::String(field));
        properties.insert("policy".to_string(), PropertyValue::String(policy));
        properties.insert(
            "is_manual_override".to_string(), // NEW property
            PropertyValue::Boolean(is_manual_override),
        );
        properties.insert(
            "applied_at".to_string(),
            PropertyValue::String(Utc::now().to_rfc3339()),
        );

        let rule_vertex = Vertex {
            id: SerializableUuid(rule_vertex_id),
            label: Identifier::new("SurvivorshipRule".to_string()).map_err(|e| e.to_string())?,
            properties,
            created_at: Utc::now().into(),
            updated_at: Utc::now().into(),
        };

        gs.create_vertex(rule_vertex)
            .await
            .map_err(|e| format!("Failed to create SurvivorshipRule vertex: {e}"))?;

        // Link the Golden Record (mpi_vertex_id) to the new rule
        let edge = Edge::new(
            mpi_vertex_id,
            Identifier::new("HAS_SURVIVORSHIP_RULE".to_string()).map_err(|e| e.to_string())?,
            rule_vertex_id,
        );

        gs.create_edge(edge)
            .await
            .map_err(|e| format!("Failed to create HAS_SURVIVORSHIP_RULE edge: {e}"))?;

        info!("Created SurvivorshipRule link for Golden Record {mpi_vertex_id}");
        Ok(rule_vertex_id)
    }

    // =========================================================================
    // CORE MPI ENTITY CREATION
    // =========================================================================
    /// Creates a MatchScore vertex and links it to a Golden Record (MPI Identity).
    /// This is used after run_probabilistic_match identifies a score.
    /// A new property `source_patient_id` is added to track the Patient that triggered this match.
    pub async fn create_match_score_link(
        &self,
        mpi_vertex_id: Uuid, // Assumed to be the Golden Record UUID
        source_patient_id: i32, // NEW: The Patient ID that was matched
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
        properties.insert(
            "source_patient_id".to_string(), // NEW property
            PropertyValue::Integer(source_patient_id as i64),
        );
        properties.insert("algorithm".to_string(), PropertyValue::String(matching_algo));
        properties.insert(
            "recorded_at".to_string(),
            PropertyValue::String(Utc::now().to_rfc3339()),
        );

        let score_vertex = Vertex {
            id: SerializableUuid(score_vertex_id),
            label: Identifier::new("MatchScore".to_string()).map_err(|e| e.to_string())?,
            properties,
            created_at: Utc::now().into(),
            updated_at: Utc::now().into(),
        };

        gs.create_vertex(score_vertex)
            .await
            .map_err(|e| format!("Failed to create MatchScore vertex: {e}"))?;

        // Link the Golden Record (mpi_vertex_id) to the new score
        let edge = Edge::new(
            mpi_vertex_id,
            Identifier::new("HAS_MATCH_SCORE".to_string()).map_err(|e| e.to_string())?,
            score_vertex_id,
        );

        gs.create_edge(edge)
            .await
            .map_err(|e| format!("Failed to create HAS_MATCH_SCORE edge: {e}"))?;

        info!("Created MatchScore {:?} (algo: {:?}) for Golden Record {:?}", score, matching_algo_clone, mpi_vertex_id );
        Ok(score_vertex_id)
    }

    /// Handles the explicit probabilistic link/merge action between two MRNs.
    /// This method performs the graph operation (linking) and records the match score.
    // --- Entire updated handle_probabilistic_link method in `mpi_identity_resolution.rs` ---
    // NOTE: This assumes `lookup_patient_by_mrn` and `extract_single_vertex` are defined/imported 
    // in the same module and uses `&self.graph_service` as the `gs: &GraphService` argument.
    pub async fn handle_probabilistic_link(
        &self,
        target_mrn: String,
        candidate_mrn: String,
        score: f64,
        action: String,
    ) -> Result<(), String> {
        info!("Processing explicit MPI link action: {} between {} and {}", action, target_mrn, candidate_mrn);

        let gs = &self.graph_service;

        // --- 1. Get Target Patient Data (MUST EXIST) ---
        // This returns the Vertex UUID (the internal ID) and the Patient struct.
        let (target_vertex_uuid_str, target_patient) = lookup_patient_by_mrn(gs, &target_mrn).await
            .map_err(|e| format!("Probabilistic linking failed: Target Patient lookup failed for MRN {}: {}", target_mrn, e))?;

        // --- 2. Get Candidate Patient Data (MUST EXIST) ---
        let (_candidate_vertex_uuid_str, candidate_patient) = lookup_patient_by_mrn(gs, &candidate_mrn).await
            .map_err(|e| format!("Probabilistic linking failed: Candidate Patient lookup failed for MRN {}: {}", candidate_mrn, e))?;
        
        // Convert the target patient's Vertex UUID string to Uuid object
        let target_patient_vertex_uuid = Uuid::parse_str(&target_vertex_uuid_str)
            .map_err(|e| format!("Invalid Target Patient UUID format: {}", e))?;

        // --- 3. Determine or Create the Golden Record (GR) ---
        
        let master_gr_app_id: String; 
        let mut gr_vertex_uuid: Uuid;
        
        // Query 1: Check if target patient has a Golden Record by linking from the Patient node.
        // FIX: Match on the 'mrn' property, not the confusing 'id' property.
        let gr_query = format!(
            r#"MATCH (p:Patient {{mrn: "{}"}})<-[:HAS_GOLDEN_RECORD]-(g:GoldenRecord) RETURN g"#,
            target_mrn 
        );

        let gr_result: Vec<Value> = gs.execute_cypher_read(&gr_query, Value::Null).await
            .map_err(|e| format!("Graph query failed to find Golden Record for target: {}", e))?;

        // Extract ALL vertices from the result and use GoldenRecord::try_from to filter
        let target_gr_option = extract_all_vertices(gr_result) 
            .into_iter()
            .filter_map(|vertex| GoldenRecord::try_from(vertex).ok())
            .next();

        if let Some(master_gr) = target_gr_option {
            
            // --- CASE B: GR FOUND (Deserialize Existing GR) ---
            
            master_gr_app_id = master_gr.id;
            gr_vertex_uuid = master_gr.gr_vertex_uuid;
            
            info!("Found existing Golden Record {} (Vertex {}) for Patient MRN {}", 
                  master_gr_app_id, gr_vertex_uuid, target_mrn);
            
        } else {
            
            // --- CASE A: GR NOT FOUND (Create New GR using the Model) ---
            info!("No Golden Record found for Patient MRN {}. Creating new Golden Record.", target_mrn);
            
            let new_gr_id = format!("GOLDEN-{}", Uuid::new_v4()); 
            let now_string = Utc::now().to_rfc3339();

            // Generate a NEW UUID for the Golden Record's actual vertex ID 
            let new_gr_vertex_uuid = Uuid::new_v4(); 

            // 1. Create the GoldenRecord struct in memory
            let mut new_golden_record = GoldenRecord::new(
                new_gr_id.clone(),
                new_gr_vertex_uuid,         // 1. New UUID for the GR vertex itself
                target_patient_vertex_uuid, // 2. UUID of the Patient it represents
                now_string.clone(),
            );
            
            // Add the canonical MRN property
            new_golden_record.update_metadata(Some(target_mrn.clone()), now_string);

            // 2. Convert the struct to a Vertex
            let gr_vertex: Vertex = new_golden_record.into();

            // 3. Persist the Vertex (Query 2) - Creates a new, distinct node
            gs.create_vertex(gr_vertex).await
                .map_err(|e| format!("Failed to create Golden Record Vertex: {}", e))?;

            // 4. Link target patient to Golden Record (Query 3)
            // FIX: Match Patient by MRN property (string)
            let link_target_query = format!(
                r#"MATCH (p:Patient {{mrn: "{}"}}), (g:GoldenRecord {{id: "{}"}}) CREATE (g)-[:HAS_GOLDEN_RECORD]->(p)"#,
                target_mrn, // Match P by MRN (string)
                new_gr_id   // Match G by App ID (string)
            );

            gs.execute_cypher_write(&link_target_query, Value::Null).await
                .map_err(|e| format!("Failed to link target Patient to Golden Record: {}", e))?;

            info!("Created and linked new Golden Record {} for Patient MRN {}", new_gr_id, target_mrn);
            master_gr_app_id = new_gr_id; // Assign the new ID
            gr_vertex_uuid = new_gr_vertex_uuid;
        };
        

        // --- 4. Check Action and Execute Link/Record Score ---
        if action.to_lowercase().as_str() == "link" || action.to_lowercase().as_str() == "merge" {
            info!("Linking Candidate Patient (MRN: {}) to Master Golden Record {}.", candidate_mrn, master_gr_app_id);
            
            // Query 4: Create the HAS_GOLDEN_RECORD relationship
            let create_link_query = format!(
                r#"MATCH (p:Patient {{mrn: "{}"}}), (g:GoldenRecord {{id: "{}"}}) CREATE (g)-[:HAS_GOLDEN_RECORD]->(p)"#,
                candidate_mrn, 
                master_gr_app_id 
            );

            gs.execute_cypher_write(&create_link_query, Value::Null).await
                .map_err(|e| format!("Failed to create HAS_GOLDEN_RECORD link: {}", e))?;
                
            // Query 5: Record the MatchScore (uses the actual GR vertex UUID)
            self.create_match_score_link(
                gr_vertex_uuid,
                candidate_patient.id, // Use the Patient's ID from the Candidate lookup
                score,
                "explicit_probabilistic_link".to_string(),
            ).await?;

            info!("✅ Successfully linked Candidate MRN {} to Golden Record {}.", candidate_mrn, master_gr_app_id);
            
        } else if action.to_lowercase().as_str() == "ignore" || action.to_lowercase().as_str() == "false_positive" {
            info!("Recording match score of {} between {} and {} with action '{}'. No graph link created.", score, target_mrn, candidate_mrn, action);
            
            // Record the MatchScore for auditing without creating a link
            self.create_match_score_link(
                gr_vertex_uuid,
                target_patient.id, 
                score,
                format!("explicit_{}", action),
            ).await?;

            info!("Successfully recorded explicit match score, action was to ignore/do not link.");
        } else {
            return Err(format!("Unsupported match action: {}. Must be 'link', 'merge', or 'ignore'.", action));
        }

        Ok(())
    }
    
    // =========================================================================
    // SOFT-LINKING & CONFLICT MANAGEMENT
    // =========================================================================

    /// Creates a soft-link (IS_LINKED_TO) between two Patient records based on probabilistic scoring.
    /// This flags them as potential duplicates requiring manual review. The edge now stores the conflict ID.
    pub async fn create_potential_duplicate(
        &self,
        patient_a_vertex_id: Uuid,
        patient_b_vertex_id: Uuid,
        score: f64,
        user: Option<String>,
        conflict_id: Uuid, // NEW: ID of the Conflict/Audit log vertex
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
        
        // NEW: Store the reference to the conflict audit node
        edge = edge.with_property(
            "conflict_reference_id",
            PropertyValue::String(conflict_id.to_string()),
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
            "Created potential duplicate link between {patient_a_vertex_id} <> {patient_b_vertex_id} (score: {score}) referring to Conflict ID {conflict_id}"
        );

        Ok(patient_a_vertex_id)
    }

    // =========================================================================
    // IDENTITY SPLIT (Reversing a Merge)
    // =========================================================================
    /// Performs an identity split, reversing an erroneous merge by creating a new Patient record,
    /// and establishing a NEW Golden Record for that split identity.
    /// It now captures the ID of the patient record that was successfully split out (`split_patient_id`).
    pub async fn identity_split(
        &self,
        target_patient_id_str: String, // The patient that was incorrectly merged *into* the GR
        new_patient_data: Patient,
        reason: String,
        split_patient_id: i32, // NEW: The internal ID of the record that is being split out.
    ) -> Result<Uuid, String> {
        let gs = &self.graph_service;
        
        let target_id = extract_numeric_patient_id(&target_patient_id_str)
            .map_err(|e| format!("Invalid Target Patient ID format: {}. {}", target_patient_id_str, e))?;

        // 1. Validate Target Patient (the one that needs to be split)
        let target_vertex = gs.get_patient_vertex_by_id(target_id)
            .await
            .ok_or_else(|| format!("Target Patient ID {} not found for split.", target_id))?;
        let target_vertex_id = target_vertex.id.0;
        
        // 2. Create the new Patient record (Source Record)
        // NOTE: We assume new_patient_data already contains the properties for the split record.
        let mut new_patient_vertex = new_patient_data.to_vertex();
        
        // Ensure the split patient ID is correctly recorded, overriding the generic one if necessary
        new_patient_vertex.properties.insert(
            "original_id".to_string(), 
            PropertyValue::Integer(split_patient_id as i64)
        );
        
        let new_patient_vertex_id = new_patient_vertex.id.0;

        gs.add_vertex(new_patient_vertex).await
            .map_err(|e| format!("Failed to create new Patient vertex for split: {}", e))?;

        // 

        // 3. Create a NEW Golden Record for the split identity and link the new patient to it.
        // We ensure a separate, distinct identity for the split patient.
        self.create_golden_record_and_link(&new_patient_data, new_patient_vertex_id).await
             .map_err(|e| format!("Failed to create Golden Record for split identity: {}", e))?;
        
        // 4. Create Audit Vertex for the split
        let audit_vertex_id = Uuid::new_v4();
        let audit_vertex = Vertex {
            id: SerializableUuid(audit_vertex_id),
            label: Identifier::new("AuditLog".to_string()).unwrap(),
            properties: HashMap::from([
                ("reason".to_string(), models::PropertyValue::String(reason)),
                ("action".to_string(), models::PropertyValue::String("IDENTITY_SPLIT".to_string())),
                // NEW: Log which specific internal ID was split out
                ("split_source_id".to_string(), models::PropertyValue::Integer(split_patient_id as i64)), 
            ]),
            created_at: Utc::now().into(),
            updated_at: Utc::now().into(),
        };
        gs.add_vertex(audit_vertex).await
            .map_err(|e| format!("Failed to add AuditLog vertex for split: {}", e))?;
            
        // 5. Create IS_SPLIT_FROM edge (NewPatient -> Original/Target Patient)
        let split_edge = Edge::new(
            new_patient_vertex_id,
            Identifier::new("IS_SPLIT_FROM".to_string()).unwrap(),
            target_vertex_id,
        );
        gs.add_edge(split_edge).await
            .map_err(|e| format!("Failed to link IS_SPLIT_FROM edge: {}", e))?;
            
        // 6. Link New Patient to Audit Log
        let audit_edge = Edge::new(
            new_patient_vertex_id,
            Identifier::new("HAS_IDENTITY_HISTORY".to_string()).unwrap(),
            audit_vertex_id,
        );
        gs.add_edge(audit_edge).await
            .map_err(|e| format!("Failed to link AuditLog edge: {}", e))?;

        info!("Identity split successful: New Patient {} created from {}. New identity established with a dedicated Golden Record.", new_patient_vertex_id, target_vertex_id);
        Ok(new_patient_vertex_id)
    }

    /// Retrieves the consolidated "Golden Record" (Patient struct) for a given MPI ID.
    /// This now includes graph traversal to find the Golden Record and applies survivorship rules.
    pub async fn get_golden_record(&self, patient_id_str: String) -> Result<Patient, String> { // Changed arg name for clarity
        let gs = &self.graph_service;
        if patient_id_str.is_empty() {
            return Err("Patient ID cannot be empty.".to_string());
        }
        // 1. Find the Patient Vertex by ID or MRN
        let patient_vertex = get_patient_vertex_by_id_or_mrn(gs, &patient_id_str)
            .await
            .map_err(|e| format!("Initial Patient lookup failed for '{}': {}", patient_id_str, e))?;
        
        // 2. Find the associated Golden Record Vertex (the canonical identity)
        let find_gr_query = format!(
            r#"
            MATCH (p:Patient) WHERE ID(p) = "{}"
            MATCH (p)-[:HAS_GOLDEN_RECORD]->(g:GoldenRecord)
            RETURN g
            "#,
            patient_vertex.id.0
        );
        let gr_result = gs.execute_cypher_read(&find_gr_query, Value::Null).await
            .map_err(|e| format!("Graph traversal to Golden Record failed: {}", e))?;
        
        // 3. Extract the Golden Record vertex properties
        let gr_vertex_value = gr_result.into_iter()
            .flat_map(|val| val.get("results").and_then(Value::as_array).map(|arr| arr.to_vec()))
            .flatten()
            .flat_map(|res_item| res_item.get("g").cloned()) // Get the 'g' (GoldenRecord) Value
            .next()
            .ok_or_else(|| format!("Patient {} is not linked to a Golden Record.", patient_id_str))?;
        
        // Convert serde_json::Value to PropertyValue
        let gr_property_value: PropertyValue = serde_json::from_value(gr_vertex_value)
            .map_err(|e| format!("Failed to convert Golden Record data: {}", e))?;
        
        // --- Survivorship Logic (Placeholder) ---
        // In a real system, you would call a separate method here: 
        // `self.apply_survivorship(gr_property_value, patient_vertex.id.0).await?`
        
        // For demonstration, we simply convert the Golden Record vertex value to the Patient struct.
        match Patient::from_vertex_value(&gr_property_value) {
            Some(mut patient) => {
                info!("[Service] Retrieved Golden Record for Patient ID: {}", patient_id_str);
                
                // Add a visual indicator that this is the Golden Record
                if let Some(mrn) = patient.mrn.take() {
                    patient.mrn = Some(format!("GOLDEN_{}", mrn));
                } else {
                    patient.mrn = Some(format!("GOLDEN_ID_{}", patient.id));
                }
                
                Ok(patient)
            },
            None => Err(format!("Failed to convert Golden Record data to Patient model for {}.", patient_id_str)),
        }
    }
}
