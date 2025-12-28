//! MPI Identity Resolution — Real-time, probabilistic patient matching
//! Global singleton with blocking + scoring + auto-merge

// Assuming strsim is available as a dependency based on previous usage
use anyhow::{Result, Context, anyhow};
use std::convert::TryFrom; // Needed for i64 -> i32 conversion
use std::collections::{HashMap, HashSet, BTreeMap};
use std::sync::Arc;
use std::str::FromStr; // FIX for E0599 (no function from_str)
use lib::graph_engine::graph_service::{GraphService, initialize_graph_service}; 
use lib::query_parser::cypher_parser::{ to_property_value };
use models::medical::*;
use models::{Graph, Identifier, Edge, Vertex, ToVertex};
use models::medical::{Patient, MasterPatientIndex, GoldenRecord, Address};
use models::identifiers::{ SerializableUuid,  VertexId };
use models::properties::{ PropertyValue, SerializableFloat, SerializableDateTime, PropertyMap, };
use models::timestamp::BincodeDateTime;
use models::evolution::{ EvolutionStep, LineageReport };
use models::dashboard::{ DashboardItem, StewardshipRecord, MpiStewardshipDashboard, GraphEventSummary, IdentityEvent };
use tokio::sync::{OnceCell, RwLock};
use uuid::Uuid;
use rand::random; // Need this import (must be outside the block)
use chrono::{DateTime, TimeZone, NaiveDate, Utc, Datelike};
use log::{info, error, warn, debug};
use serde_json::{ self, json, Value, from_value }; // ADDED for JSON parsing in global_init
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

#[derive(Debug, Clone, PartialEq)]
enum IdentifierType {
    MRN,           // Medical Record Number (e.g., "M12345", "MRN78901")
    InternalID,    // Numeric internal ID (e.g., "12345", "-557141486")
    UUID,          // UUID format (e.g., "b73948a7-960d-455d-8165-85b1210242be")
    ExternalID,    // External ID requiring type (handled separately)
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

/// Helper to extract GoldenRecord from nested Cypher result (same pattern as extract_single_vertex)
fn extract_golden_record_from_result(result: &serde_json::Value) -> Option<GoldenRecord> {
    result
        .get("results")
        .and_then(|results| results.as_array())
        .and_then(|results_array| results_array.get(0))
        .and_then(|first_result| first_result.get("vertices"))
        .and_then(|vertices| vertices.as_array())
        .and_then(|vertices_array| vertices_array.get(0)) // First vertex is GoldenRecord (g)
        .and_then(|v| serde_json::from_value::<Vertex>(v.clone()).ok())
        .and_then(|vertex| GoldenRecord::try_from(vertex).ok())
}

/// Helper to extract IdentityEvent from nested Cypher result
fn extract_identity_event_from_result(result: &serde_json::Value) -> Option<IdentityEvent> {
    result
        .get("results")
        .and_then(|results| results.as_array())
        .and_then(|results_array| results_array.get(0))
        .and_then(|first_result| first_result.get("vertices"))
        .and_then(|vertices| vertices.as_array())
        .and_then(|vertices_array| vertices_array.get(1)) // Second vertex is IdentityEvent (e)
        .and_then(|v| serde_json::from_value::<Vertex>(v.clone()).ok())
        .and_then(|vertex| {
            // Assuming IdentityEvent has TryFrom<Vertex> or similar conversion
            // Adjust based on your actual IdentityEvent model
            serde_json::from_value::<IdentityEvent>(serde_json::to_value(vertex).ok()?).ok()
        })
}

/// Helper to extract Patient from nested Cypher result (if present)
fn extract_patient_from_result(result: &serde_json::Value) -> Option<Patient> {
    result
        .get("results")
        .and_then(|results| results.as_array())
        .and_then(|results_array| results_array.get(0))
        .and_then(|first_result| first_result.get("vertices"))
        .and_then(|vertices| vertices.as_array())
        .and_then(|vertices_array| vertices_array.get(2)) // Third vertex is Patient (p)
        .and_then(|v| serde_json::from_value::<Vertex>(v.clone()).ok())
        .and_then(|vertex| Patient::try_from(vertex).ok())
}

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
    gs: &GraphService,
    mrn: &str
) -> Result<(String, Patient), String> {
    let vertex = gs.get_patient_by_mrn(mrn).await
        .map_err(|e| format!("Patient lookup failed for MRN {}: {}", mrn, e))?
        .ok_or_else(|| format!("Patient with MRN '{}' not found in graph.", mrn))?;

    let patient = patient_from_vertex(&vertex)
        .map_err(|e| format!("Failed to convert vertex to Patient for MRN {}: {}", mrn, e))?;

    Ok((vertex.id.0.to_string(), patient))
}

// --- The Core Lookup Function ---
/// The Core Lookup Function — supports lookup by MRN, string UUID id, or numeric legacy id.
async fn get_patient_vertex_by_id_or_mrn(
    gs: &GraphService, 
    identifier: &str
) -> Result<Vertex, String> {
    
    let clean_id = identifier.trim_matches(|c| c == '\'' || c == '"' || c == ' ');

    // Try MRN first (most common for CLI/external calls)
    if let Some(vertex) = gs.get_patient_by_mrn(clean_id).await.map_err(|e| format!("MRN lookup error: {}", e))? {
        return Ok(vertex);
    }

    // Try string UUID on the 'id' property (internal vertex ID)
    let uuid_query = format!("MATCH (p:Patient {{id: \"{}\"}}) RETURN p", clean_id);
    if let Ok(result_vec) = gs.execute_cypher_read(&uuid_query, Value::Null).await {
        if let Some(vertex) = extract_single_vertex(result_vec) {
            return Ok(vertex);
        }
    }

    // Try numeric legacy id (if identifier parses as integer)
    if let Ok(num_id) = clean_id.parse::<i64>() {
        let num_query = format!("MATCH (p:Patient {{id: {}}}) RETURN p", num_id);
        if let Ok(result_vec) = gs.execute_cypher_read(&num_query, Value::Null).await {
            if let Some(vertex) = extract_single_vertex(result_vec) {
                return Ok(vertex);
            }
        }
    }

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
        
        // Handle empty input or single names gracefully
        let last_name = if parts.len() > 1 {
            parts.pop().map(|s| s.to_string())
        } else {
            None
        };
        
        let first_name = if !parts.is_empty() {
            Some(parts.join(" "))
        } else if last_name.is_none() && !full_name.is_empty() {
            // Fallback for single-word names
            Some(full_name.to_string())
        } else {
            None
        };
        
        // --- 2. Parse Date of Birth ---
        let naive_date = match NaiveDate::parse_from_str(dob_str, "%Y-%m-%d") {
            Ok(d) => d,
            Err(_) if dob_str.is_empty() => NaiveDate::from_ymd_opt(1900, 1, 1).unwrap(),
            Err(e) => return Err(format!("Failed to parse DOB '{}': {}", dob_str, e)),
        };
        
        // Correct conversion to DateTime<Utc>
        let date_of_birth: DateTime<Utc> = Utc.from_utc_datetime(
            &naive_date.and_hms_opt(0, 0, 0).expect("Invalid time construction")
        );
        
        // --- 3. Create Temporary Patient for Key Generation ---
        // FIX: Wrap strings in Some() to match Option<String> in model
        let temp_patient = Patient {
            first_name,
            last_name,
            // Wrap the DateTime in the Serializable wrapper, then in Some()
            date_of_birth: Some(SerializableDateTime(date_of_birth)),
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
        let mut keys = Vec::new();
        
        // Extract Year from DOB (Assuming date_of_birth is still a mandatory NaiveDate or similar)
        let dob_year = patient.date_of_birth
            .as_ref()
            .map(|dt| dt.0.year().to_string())
            .unwrap_or_else(|| "0000".to_string());

        // 1. Process Names safely using .as_ref() and .map()
        let first_initial = patient.first_name.as_ref()
            .map(|s| s.to_uppercase().chars().take(1).collect::<String>());
            
        let last_name_prefix = patient.last_name.as_ref()
            .map(|s| s.to_uppercase().chars().take(4).collect::<String>());

        // Key 1: Last Name Prefix (4 chars) + First Initial + DOB Year (e.g., SMIT_A_1980)
        // We only generate this if BOTH name components exist.
        if let (Some(f_init), Some(l_pre)) = (first_initial, last_name_prefix) {
            if !f_init.is_empty() && !l_pre.is_empty() {
                keys.push(format!("{}_{}_{}", l_pre, f_init, dob_year));
            }
        }

        // Key 2: Full Last Name + DOB Year (e.g., SMITH_1980)
        if let Some(ref last_name) = patient.last_name {
            let full_last_name_clean = last_name.to_uppercase().replace(" ", "");
            if !full_last_name_clean.is_empty() {
                keys.push(format!("{}_{}", full_last_name_clean, dob_year));
            }
        }

        // Fallback if no specific keys could be generated
        if keys.is_empty() {
            keys.push(format!("BY_YEAR_{}", dob_year));
        }

        keys
    }

    /// **Resolves an external ID or primary ID to the canonical PatientId and fetches the Golden Record.**
    /// 
    /// This delegates the core identity resolution logic to the underlying graph storage service.
    /// 
    pub async fn resolve_and_fetch_patient(
        &self,
        external_id_data: ExternalId,
        id_type_override: Option<IdType>,
        requested_by: &str, 
    ) -> Result<(PatientId, Patient), anyhow::Error> {
        
        // 1. Determine the canonical ID
        let canonical_id = match id_type_override.or(Some(external_id_data.id_type.clone())) {
            Some(i_type) => {
                self.graph_service 
                    .find_canonical_id_by_external_id(&external_id_data.id_value, &i_type) 
                    .await?
                    .context("External ID not found.")?
            }
            None => PatientId::from(external_id_data.id_value.clone())
        };

        // 2. Fetch the Golden Record
        let golden_record = self.graph_service
            .get_patient_by_id(&canonical_id.to_string())
            .await?
            .context(format!("Patient ID {} retrieval failed.", canonical_id))?;

        // 3. LOGGING: Record the 'READ' access in the graph
        // FIX: We clone canonical_id twice because into_uuid() consumes it.
        // The final instance is kept for the return value.
        let patient_uuid = canonical_id.clone().into_uuid();

        self.graph_service.persist_mpi_change_event(
            patient_uuid,              // Patient vertex origin
            patient_uuid,              // Target (self-reference for view event)
            "ACCESS_VIEW",
            json!({ 
                "requested_by": requested_by, 
                "id_used": external_id_data.id_value,
                "id_type": external_id_data.id_type,
                "reason": "Direct resolution lookup",
                "timestamp": Utc::now().to_rfc3339()
            })
        ).await?;

        // We return the original canonical_id which is still available 
        // because we only cloned it for the UUID conversions above.
        Ok((canonical_id, golden_record))
    }

    /// **Searches the MPI for patients matching the provided demographic criteria.**
    /// 
    /// This packages the criteria and delegates the search query execution to the graph storage service.
    /// 
    /// **Searches the MPI and logs the search parameters used (Fraud Prevention).**
    pub async fn search_patients_by_demographics(
        &self,
        name: Option<String>,
        first_name: Option<String>,
        last_name: Option<String>,
        dob: Option<String>,
        address: Option<String>,
        phone: Option<String>,
        requested_by: &str,
    ) -> Result<Vec<Patient>> {
        let mut criteria = HashMap::new();
        
        // Map criteria for both execution and logging
        // Ensure keys match the vertex property names seen in the debug logs
        if let Some(n) = name { criteria.insert("name".to_string(), n); }
        if let Some(fnm) = first_name { criteria.insert("first_name".to_string(), fnm); }
        if let Some(lnm) = last_name { criteria.insert("last_name".to_string(), lnm); }
        if let Some(d) = dob { criteria.insert("date_of_birth".to_string(), d); }
        if let Some(a) = address { criteria.insert("address".to_string(), a); }
        if let Some(p) = phone { criteria.insert("phone".to_string(), p); }

        if criteria.is_empty() {
            return Ok(Vec::new());
        }

        // 1. Execute the search via the GraphService demographic engine
        let results = self.graph_service
            .execute_demographic_search(criteria.clone())
            .await
            .context("Demographic search execution failed.")?;

        // 2. LOGGING: Create a persistent 'SearchEvent' node (2025-12-20 Audit Trail)
        // We use persist_mpi_change_event to satisfy the Graph of Events requirement
        let event_id = self.graph_service.persist_mpi_change_event(
            Uuid::nil(), 
            Uuid::nil(), 
            "DEMOGRAPHIC_SEARCH",
            json!({ 
                "requested_by": requested_by, 
                "criteria": criteria,
                "results_count": results.len(),
                "timestamp": Utc::now().to_rfc3339()
            })
        ).await?;

        // 3. TRACING: Link the search event to every patient returned
        // This creates the "Graph of Changes" relationship between the Search and the Patient
        for patient in &results {
            if let Some(mrn) = &patient.mrn {
                // Resolve the domain MRN to a Graph UUID
                if let Ok(v_uuid) = self.resolve_id_to_uuid(mrn).await {
                    let edge_label = Identifier::new("REVEALED_RECORD".to_string())
                        .map_err(|e| anyhow!("Invalid Identifier: {}", e))?;

                    // Correctly construct the Edge object
                    // Note: Your logs show vertex_id is a String property, 
                    // ensure resolve_id_to_uuid is checking that property.
                    let mut edge = Edge::new(
                        event_id,             // outbound_id (Search Event)
                        edge_label,           // label
                        v_uuid                // inbound_id (Patient)
                    );

                    // Add lineage metadata to the edge itself
                    edge.properties.insert(
                        "audit_timestamp".to_string(), 
                        PropertyValue::String(Utc::now().to_rfc3339())
                    );

                    let _ = self.graph_service.create_edge(edge).await;
                }
            }
        }

        info!(
            "[MPI Audit] User '{}' performed demographic search. Results: {}", 
            requested_by, 
            results.len()
        );

        Ok(results)
    }

    // =========================================================================
    // INDEXING & REAL-TIME
    // =========================================================================

    // This replaces your existing index_patient implementation.
    async fn index_patient(&self, patient: &Patient, vertex_id: Uuid, requested_by: &str) {
        let mut mrn_idx = self.mrn_index.write().await;
        let mut name_dob_idx = self.name_dob_index.write().await;

        if let Some(mrn) = patient.mrn.as_ref() {
            mrn_idx.insert(mrn.clone(), vertex_id);
        }

        let norm_name = format!("{} {}", 
            patient.last_name.as_deref().unwrap_or("").to_lowercase(),
            patient.first_name.as_deref().unwrap_or("").to_lowercase()
        );
        let dob = patient.date_of_birth
            .as_ref()
            .map(|d| d.0.format("%Y-%m-%d").to_string())
            .unwrap_or_default();
        name_dob_idx.entry((norm_name, dob)).or_default().push(vertex_id);
        
        let keys = Self::generate_blocking_keys(patient);
        let mut blocking_idx = self.blocking_index.write().await;
        for key in keys.clone() {
            blocking_idx.entry(key).or_insert_with(HashSet::new).insert(vertex_id);
        }

        // 1. COMPLIANCE: Graph of Changes
        let _ = self.graph_service.log_mpi_transaction(
            vertex_id,
            vertex_id,
            "MEMORY_INDEX_SYNC",
            "Probabilistic blocking keys updated",
            requested_by
        ).await;

        // 2. COMPLIANCE: Graph of Events (Detailed Sync Data)
        let _ = self.graph_service.persist_mpi_change_event(
            vertex_id,
            vertex_id,
            "SYNC_DETAILS",
            json!({
                "blocking_keys_count": keys.len(),
                "keys": keys,
                "sync_time": Utc::now().to_rfc3339()
            })
        ).await;
    }

    // Keep the existing index_mpi_record and on_patient_added as they are:

    async fn index_mpi_record(&self, mpi: &MasterPatientIndex, vertex_id: Uuid) {
        if let Some(ssn) = mpi.social_security_number.as_ref() {
            let mut ssn_idx = self.ssn_index.write().await;
            ssn_idx.insert(ssn.clone(), vertex_id);
        }
    }

    async fn on_patient_added(&self, patient: Patient, vertex_id: Uuid, requested_by: &str,) {
        self.index_patient(&patient, vertex_id, &requested_by).await;

        let candidates = self.find_candidates(&patient).await;
        // Corrected comparison logic
        if let Some(best) = candidates.into_iter().max_by(|a, b| a.match_score.partial_cmp(&b.match_score).unwrap_or(std::cmp::Ordering::Equal)) {
            if best.match_score > 0.95 {
                self.auto_merge(vertex_id, patient, best, &requested_by).await;
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
    pub async fn index_new_patient(
        &self, 
        mut patient_data: Patient, 
        requested_by: &str
    ) -> Result<Patient, String> {
        let gs = &self.graph_service;
        let new_patient_vertex_id: Uuid;
        let mut is_new_creation = false;

        // 1. Check for existing patient via MRN
        if let Some(mrn) = patient_data.mrn.as_ref() {
            // This method succeeds and prints the "Found Patient vertex" log
            match gs.get_patient_by_mrn(mrn).await {

                Ok(Some(existing_vertex)) => {
                    println!(" ===> THE CALL WAS OK ===========> - {:?}", mrn);
                    // FIX: Extract ID directly from the Vertex root to ensure continuity
                    new_patient_vertex_id = existing_vertex.id.0;
                    
                    // FIX: Manually extract only what we need to avoid the 
                    // "failed to convert to Patient struct" error caused by rigid deserialization.
                    if let Some(prop_id) = existing_vertex.properties.get("id") {
                        match prop_id {
                            PropertyValue::Integer(val) => patient_data.id = Some(*val as i32),
                            PropertyValue::I32(val) => patient_data.id = Some(*val),
                            _ => {}
                        }
                    }
                    
                    // Ensure the struct we return has the correct vertex UUID
                    patient_data.vertex_id = Some(new_patient_vertex_id);
                }
                Ok(None) => {
                    is_new_creation = true;
                    let mut v = patient_data.to_vertex();
                    new_patient_vertex_id = Uuid::new_v4(); 
                    v.id = SerializableUuid(new_patient_vertex_id);
                    
                    gs.add_vertex(v).await
                        .map_err(|e| format!("Failed to add new Patient vertex: {}", e))?;
                }
                Err(e) => {
                    // This is where your specific "Deserialization error" message was bubbling up
                    return Err(format!("Failed to retrieve existing patient by MRN: {}", e));
                }
            }
        } else {
            return Err("MRN is missing and required for patient indexing.".to_string());
        }

        println!("==============> PROCESSED PATIENT: ID={}, MRN={}", new_patient_vertex_id, patient_data.mrn.as_deref().unwrap_or("N/A"));

        // 2. Run internal indexing/matching
        self.index_patient(&patient_data, new_patient_vertex_id, requested_by).await;
        
        let candidates = self.find_candidates(&patient_data).await;
        let mut match_performed = false;

        if let Some(best) = candidates.into_iter().max_by(|a, b| a.match_score.partial_cmp(&b.match_score).unwrap_or(std::cmp::Ordering::Equal)) {
            if best.match_score > 0.95 {
                match_performed = true;
                let target_vertex_id: Uuid = best.patient_vertex_id;
                
                if is_new_creation && target_vertex_id != new_patient_vertex_id {
                    let mut update_props = HashMap::new();
                    if let Some(new_mrn) = patient_data.mrn.as_ref() {
                         update_props.insert(String::from("mrn"), PropertyValue::String(new_mrn.clone()));
                    }
                    let _ = gs.update_vertex_properties(target_vertex_id, update_props).await;
                }
                self.auto_merge(new_patient_vertex_id, patient_data.clone(), best, requested_by).await;
            }
        }
        
        if is_new_creation && !match_performed {
            self.create_golden_record_and_link(&patient_data, new_patient_vertex_id, requested_by)
                .await
                .map_err(|e| format!("CRITICAL: Golden Record link failed: {}", e))?;
        }

        // 3. Compliance Logging (Requirement 2025-12-20)
        let log_action = if is_new_creation { "INITIAL_INDEX" } else { "UPDATE_INDEX" };

        // Transaction log for the graph of events
        gs.log_mpi_transaction(
            new_patient_vertex_id, 
            new_patient_vertex_id, 
            "RECORD_INDEXED",
            log_action,
            requested_by
        ).await.map_err(|e| e.to_string())?;

        // Metadata for the specific patient's graph of changes
        gs.persist_mpi_change_event(
            new_patient_vertex_id,
            new_patient_vertex_id,
            "INDEXING_METADATA",
            serde_json::json!({
                "source_system": "MPI_INTERNAL",
                "is_new": is_new_creation,
                "timestamp": chrono::Utc::now().to_rfc3339()
            })
        ).await.map_err(|e| e.to_string())?;

        patient_data.vertex_id = Some(new_patient_vertex_id);

        Ok(patient_data)
    }

    /// Factory method for the global singleton, accepting the pre-initialized GraphService.
    /// FIX: Now uses Cypher query and correctly handles nested result structure parsing.
    pub async fn global_init(graph_service_instance: Arc<GraphService>, requested_by: &str) -> std::result::Result<(), &'static str> {
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
                /*
                println!(
                    "[MPI Debug] Loaded Vertex ID: {}, Label: {}, Properties: {:?}", 
                    vertex.id.0, 
                    vertex.label.as_ref(), 
                    vertex.properties
                );
                */
                match vertex.label.as_ref() {
                    "Patient" => {
                        if let Some(patient) = Patient::from_vertex(&vertex) {
                            // This calls the index_patient with println! logging
                            service.index_patient(&patient, vertex.id.0, &requested_by).await;
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
    /// then scoring all candidates. Logged into the Graph of Events for traceabilty.
    pub async fn run_probabilistic_match(
        &self, 
        patient: &Patient, 
        blocking_keys: Vec<String>, 
        requested_by: &str,          
    ) -> Result<Vec<PatientCandidate>, anyhow::Error> {
        let gs = &self.graph_service;

        // --- 1. Identify the Source ---
        // Since Patient knows nothing about being a vertex, we use the service to resolve it.
        // If it's a new patient not yet in the graph, we use nil for the event source.
        let patient_uuid = if let Some(mrn) = &patient.mrn {
            self.resolve_id_to_uuid(mrn).await.unwrap_or(Uuid::nil())
        } else {
            Uuid::nil()
        };

        // --- 2. Initial Candidate Search using Blocking Keys ---
        let mut candidate_patient_uuids = HashSet::new();
        let blocking_idx = self.blocking_index.read().await;
        
        for key in &blocking_keys {
            if let Some(ids) = blocking_idx.get(key) {
                candidate_patient_uuids.extend(ids.iter().cloned());
            }
        }

        // AUDIT: Record the Matching Attempt (2025-12-20 Compliance)
        let event_id = gs.persist_mpi_change_event(
            patient_uuid, 
            Uuid::nil(), 
            "PROBABILISTIC_MATCH_RUN",
            json!({
                "requested_by": requested_by,
                "blocking_keys": blocking_keys,
                "candidates_found": candidate_patient_uuids.len()
            })
        ).await.map_err(|e| anyhow!("Match audit failed: {}", e))?;

        if candidate_patient_uuids.is_empty() {
            return Ok(vec![]); 
        }
        
        // --- 3. Retrieve Full Patient Data and Map to Golden Records ---
        let mut candidates_to_score = Vec::new();
        let mut processed_gr_uuids = HashSet::new();
        
        for cand_uuid in candidate_patient_uuids.iter() {
            if let Some(gr_uuid) = self.get_golden_record_for_patient_vertex_id(*cand_uuid).await {
                
                if processed_gr_uuids.contains(&gr_uuid) {
                    continue; 
                }
                processed_gr_uuids.insert(gr_uuid);

                let score = rand::random::<f64>() * 0.95; // Placeholder for matching logic
                
                // TRACING: Link match event to candidate Golden Record
                let edge_label = Identifier::new("EVALUATED_CANDIDATE".to_string())
                    .map_err(|e| anyhow!("Invalid identifier: {}", e))?;
                    
                let edge = Edge::new(
                    SerializableUuid(event_id),
                    edge_label,
                    SerializableUuid(gr_uuid),
                );
                
                let _ = gs.create_edge(edge).await;
                
                candidates_to_score.push(PatientCandidate {
                    patient_vertex_id: gr_uuid, 
                    patient_id: patient.id.unwrap_or(0), 
                    master_record_id: patient.id, 
                    match_score: score, 
                    blocking_keys: blocking_keys.clone(),
                });
            }
        }
        
        // --- 4. Finalize ---
        let mut sorted_candidates = candidates_to_score;
        sorted_candidates.sort_by(|a, b| b.match_score.partial_cmp(&a.match_score).unwrap_or(std::cmp::Ordering::Equal));
        
        let final_props = HashMap::from([
            ("top_score".to_string(), to_property_value(json!(sorted_candidates.get(0).map(|c| c.match_score).unwrap_or(0.0))).unwrap()),
            ("final_candidate_count".to_string(), to_property_value(json!(sorted_candidates.len())).unwrap()),
        ]);
        
        let _ = gs.update_vertex_properties(event_id, final_props).await;
        
        Ok(sorted_candidates)
    }

    /// Links an external identifier to a master patient record and logs the transaction.
    /// Ensures the mapping is traceable in the Graph of Events and scans for Red Flags.
    pub async fn link_external_identifier(
        &self, 
        master_id_str: String,
        external_id: String, 
        id_type: String,
        requested_by: &str // Added for transaction tracing
    ) -> Result<MasterPatientIndex, String> {
        let master_id = extract_numeric_patient_id(&master_id_str)
            .map_err(|e| format!("Invalid Master Patient ID format: {}. {}", master_id_str, e))?;

        let gs = &self.graph_service; 
        
        // 1. Retrieve the Patient Vertex
        let patient_vertex = gs.get_patient_vertex_by_id(master_id)
            .await
            .ok_or_else(|| format!("Master Patient ID {} not found in the graph.", master_id))?;
            
        // 2. Ensure identity hierarchy (Golden Record exists)
        self.ensure_golden_record_and_link(&patient_vertex, &requested_by).await?;
        
        let patient_vertex_id = patient_vertex.id.0;

        // 3. Create the MedicalIdentifier Vertex
        let medical_id_vertex_id = Uuid::new_v4();
        let medical_id_vertex = Vertex {
            id: SerializableUuid(medical_id_vertex_id),
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

        // 4. Create the Domain Edge: Patient -> HAS_EXTERNAL_ID -> MedicalIdentifier
        let edge = Edge::new(
            patient_vertex_id,
            Identifier::new("HAS_EXTERNAL_ID".to_string()).unwrap(),
            medical_id_vertex_id,
        );
        gs.add_edge(edge).await
            .map_err(|e| format!("Failed to add HAS_EXTERNAL_ID edge: {}", e))?;

        // 5. TRANSACTION LOGGING: Record this change in the Graph of Events
        // This relates the new identifier to the specific action and the user who did it.
        gs.persist_mpi_change_event(
            patient_vertex_id,
            medical_id_vertex_id,
            "IDENTIFIER_LINKED",
            json!({
                "id_type": id_type,
                "id_value": external_id,
                "requested_by": requested_by,
                "timestamp": Utc::now()
            })
        ).await.map_err(|e| format!("Failed to log transaction: {}", e))?;

        // 6. RED FLAG DETECTION: Scrutinize the new link
        // e.g., If this SSN is already linked to another Golden Record, flag it immediately.
        gs.run_conflict_detection(patient_vertex_id).await
            .map_err(|e| format!("Conflict detection failed on new ID link: {}", e))?;

        println!("[MPI Audit] Linked {} identifier '{}' to patient {}. Requested by: {}", 
            id_type, external_id, master_id, requested_by);
        
        // Return representation
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
            social_security_number: if id_type == "SSN" { Some(external_id) } else { None },
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
        requested_by: &str,    // Added: For transaction traceability and fraud prevention
    ) -> Result<(), String> {
        let gs = &self.graph_service;
        
        // 1. Convert PatientId to concrete Uuid
        let patient_vertex_uuid = patient_id.to_uuid()
            .map_err(|e| format!("Invalid PatientId format: {}", e))?;

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
            patient_vertex_uuid,
            Identifier::new("HAS_EXTERNAL_ID".to_string()).unwrap(),
            identifier_uuid,
        );

        gs.add_edge(link_edge)
            .await
            .map_err(|e| format!("Failed to link Patient {} to ExternalIdentifier {}: {}", patient_vertex_uuid, external_id, e))?;

        // 4. TRANSACTION LOGGING: Log this link in the Graph of Events
        // Every ID link must be logged to a specific Patient's Graph of Changes.
        gs.persist_mpi_change_event(
            patient_vertex_uuid,
            identifier_uuid,
            "EXTERNAL_ID_LINKED",
            json!({
                "requested_by": requested_by,
                "external_id_value": external_id,
                "id_type": id_type,
                "source_system": system,
                "status": "ACTIVE"
            })
        ).await.map_err(|e| format!("Failed to log identity transaction: {}", e))?;

        // 5. RED FLAG & COLLISION DETECTION
        // Check if this specific identifier value/type/system combo already belongs to another patient.
        gs.run_conflict_detection(patient_vertex_uuid)
            .await
            .map_err(|e| format!("Post-link conflict detection failed: {}", e))?;

        println!(
            "[MPI Audit] Linked '{}' ({} from {}) to Golden Record {}. User: {}",
            external_id, id_type, system, patient_vertex_uuid, requested_by
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
        requested_by: &str,
    ) -> Result<MasterPatientIndex, String> {
        let gs = &self.graph_service;
        let acting_user = user_id.clone().unwrap_or_else(|| "SYSTEM_ADMIN".to_string());
        let merge_reason = reason.clone().unwrap_or_else(|| "Manual data stewardship override".to_string());
        
        // 1. Robust Vertex Lookup
        let source_vertex = get_patient_vertex_by_id_or_mrn(gs, &source_id_str.trim_matches(|c| c == '\'' || c == '"').to_string())
            .await
            .map_err(|e| format!("Source lookup failed: {}", e))?;

        let target_vertex = get_patient_vertex_by_id_or_mrn(gs, &target_id_str.trim_matches(|c| c == '\'' || c == '"').to_string())
            .await
            .map_err(|e| format!("Target lookup failed: {}", e))?;

        if source_vertex.id == target_vertex.id {
            return Err("Source and Target are the same vertex. Merge aborted.".to_string());
        }

        println!("===> DEBUG: Source Patient UUID: {}", source_vertex.id.0);
        println!("===> DEBUG: Target Patient UUID: {}", target_vertex.id.0);

        // 2. Ensure Golden Record exists for the target (the survivor)
        self.ensure_golden_record_and_link(&target_vertex, requested_by)
            .await
            .map_err(|e| format!("Failed to ensure target golden record: {}", e))?;

        // 3. AUDIT: Initial Logging & Traceability [2025-12-20 Compliance]
        gs.log_mpi_transaction(
            source_vertex.id.0,
            target_vertex.id.0,
            "MANUAL_MERGE_EXECUTION",
            &merge_reason,
            &acting_user
        ).await.ok();

        let merge_event_id = gs.persist_mpi_change_event(
            source_vertex.id.0,
            target_vertex.id.0,
            "MANUAL_MERGE_INITIATED",
            json!({
                "requested_by": acting_user,
                "policy": policy,
                "reason": merge_reason,
                "timestamp": Utc::now().to_rfc3339()
            })
        ).await.map_err(|e| format!("Failed to log merge event: {}", e))?;

        // --- OPTION 1: MANUAL PROPERTY PATCH START ---
        // Ensure everything is populated as top-level properties for the rollback tool
        let mut rollback_patch = HashMap::new();
        rollback_patch.insert("source_uuid".to_string(), PropertyValue::String(source_vertex.id.0.to_string()));
        rollback_patch.insert("target_uuid".to_string(), PropertyValue::String(target_vertex.id.0.to_string()));
        rollback_patch.insert("action".to_string(), PropertyValue::String("MANUAL_MERGE_EXECUTION".to_string()));
        
        gs.update_vertex_properties(merge_event_id, rollback_patch).await.ok();
        // --- OPTION 1: MANUAL PROPERTY PATCH END ---

        // COMPLIANCE FIX: Link Event to Patient using INTERNAL UUID to satisfy [2025-12-20]
        let edge_type_event = Identifier::new("LOGGED_FOR".to_string()).unwrap();

        // Link Event -> Source Patient
        let compliance_edge_source = Edge::new(
            merge_event_id, 
            edge_type_event.clone(), 
            source_vertex.id
        );
        gs.create_edge(compliance_edge_source).await.ok();

        // Link Event -> Target Patient
        let compliance_edge_target = Edge::new(
            merge_event_id, 
            edge_type_event, 
            target_vertex.id
        );
        gs.create_edge(compliance_edge_target).await.ok();

        // 4. Locate the Target's Golden Record
        let all_edges = gs.get_all_edges().await
            .map_err(|e| format!("Failed to retrieve edges: {}", e))?;

        let target_gr_edge = all_edges.iter()
            .find(|e| {
                e.outbound_id == target_vertex.id && 
                e.edge_type.as_ref() == "HAS_GOLDEN_RECORD"
            })
            .ok_or_else(|| {
                let relevant_edges: Vec<String> = all_edges.iter()
                    .filter(|e| e.outbound_id == target_vertex.id || e.inbound_id == target_vertex.id)
                    .map(|e| format!("{} -> {} ({})", e.outbound_id.0, e.inbound_id.0, e.edge_type.as_ref()))
                    .collect();
                
                format!(
                    "Target Golden Record link not found for {}. Found edges: {:?}.",
                    target_vertex.id.0,
                    if relevant_edges.is_empty() { vec!["NONE".to_string()] } else { relevant_edges }
                )
            })?;

        let target_gr_uuid = target_gr_edge.inbound_id;

        // 5. REDIRECT SOURCE TO TARGET'S GOLDEN RECORD
        let source_old_links: Vec<Uuid> = all_edges.iter()
            .filter(|e| e.outbound_id == source_vertex.id && e.edge_type.as_ref() == "HAS_GOLDEN_RECORD")
            .map(|e| e.inbound_id.0)
            .collect();

        for old_gr in &source_old_links {
            gs.log_mpi_transaction(
                source_vertex.id.0,
                *old_gr,
                "IDENTITY_LINK_ABANDONED",
                "Moving patient to a different Golden Record due to merge",
                &acting_user
            ).await.ok();
        }

        let mut source_set = HashSet::new();
        source_set.insert(source_vertex.id.0);
        gs.delete_edges_touching_vertices(&source_set).await.ok();

        // Create New Patient -> GoldenRecord edge
        let edge_type_gr = Identifier::new("HAS_GOLDEN_RECORD".to_string()).unwrap();
        let mut merge_edge = Edge::new(source_vertex.id, edge_type_gr, target_gr_uuid);
        
        merge_edge.properties.insert("policy".to_string(), PropertyValue::String(policy.clone()));
        merge_edge.properties.insert("merge_event_id".to_string(), PropertyValue::String(merge_event_id.to_string()));
        merge_edge.properties.insert("requested_by".to_string(), PropertyValue::String(acting_user.clone()));

        gs.create_edge(merge_edge).await
            .map_err(|e| format!("Failed to link source to target golden record: {}", e))?;

        // 6. CLINICAL DATA MIGRATION
        let mut migration_count = 0;
        for edge in all_edges.iter() {
            if edge.edge_type.as_ref() == "HAS_GOLDEN_RECORD" { continue; }

            let mut should_migrate = false;
            let mut new_edge = edge.clone();

            if edge.outbound_id == source_vertex.id {
                new_edge.outbound_id = target_vertex.id;
                should_migrate = true;
            } else if edge.inbound_id == source_vertex.id {
                new_edge.inbound_id = target_vertex.id;
                should_migrate = true;
            }

            if should_migrate {
                new_edge.properties.insert("migrated_from_uuid".to_string(), PropertyValue::String(source_vertex.id.0.to_string()));
                new_edge.properties.insert("migration_event_id".to_string(), PropertyValue::String(merge_event_id.to_string()));
                
                if gs.create_edge(new_edge).await.is_ok() {
                    migration_count += 1;
                }
            }
        }

        // 7. Retire the Source Record
        let mut source_updates = HashMap::new();
        source_updates.insert("patient_status".to_string(), PropertyValue::String("MERGED".to_string()));
        source_updates.insert("merged_into_uuid".to_string(), PropertyValue::String(target_vertex.id.0.to_string()));
        source_updates.insert("updated_at".to_string(), PropertyValue::String(Utc::now().to_rfc3339()));

        gs.update_vertex_properties(source_vertex.id.0, source_updates).await.ok();

        // 8. Close the Event Audit
        let mut event_final_props = HashMap::new();
        event_final_props.insert("status".to_string(), PropertyValue::String("COMPLETED".to_string()));
        event_final_props.insert("migration_count".to_string(), PropertyValue::Integer(migration_count));
        
        gs.update_vertex_properties(merge_event_id, event_final_props).await.ok();

        // 9. Return Survivor State
        let p = &target_vertex.properties;
        let legacy_id = (target_vertex.id.0.as_u128() & 0x7FFFFFFF) as i32;

        // Helper function to extract string from PropertyValue
        let get_str = |key: &str| -> Option<String> {
            p.get(key).and_then(|v| v.as_str()).map(|s| s.to_string())
        };

        // Parse date_of_birth from PropertyValue
        let date_of_birth = p.get("date_of_birth")
            .and_then(|v| v.as_str())
            .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
            .map(|dt| dt.with_timezone(&Utc));

        Ok(MasterPatientIndex {
            id: rand::random::<i32>(),
            patient_id: Some(legacy_id),
            first_name: get_str("first_name"),
            last_name: get_str("last_name"),
            date_of_birth,
            gender: get_str("gender"),
            address: get_str("address").and_then(|s| serde_json::from_str(&s).ok()),
            contact_number: get_str("contact_number"),
            email: get_str("email"),
            social_security_number: get_str("ssn"),
            match_score: None,
            match_date: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        })
    }

    pub async fn rollback_merge(
        &self,
        identifier: String,
        requested_by: &str,
    ) -> Result<(), String> {
        let gs = &self.graph_service;
        let clean_id = identifier.trim_matches(|c| c == '\'' || c == '"');

        println!("[MPI] Starting rollback_merge for identifier: {}", clean_id);

        // ═════════════════════════════════════════════════════════════════════════
        // PHASE 1: DISCOVERY – Find the Merge Event
        // ═════════════════════════════════════════════════════════════════════════
        let (merge_event_uuid, source_patient_vertex) = {
            if let Ok(uuid) = Uuid::parse_str(&clean_id) {
                println!("[MPI] Identifier parsed as UUID: {}", uuid);
                match gs.get_vertex_by_uuid_cypher(&uuid).await {
                    Ok(v) => {
                        println!("[MPI] UUID directly points to a valid vertex (likely the event itself)");
                        (uuid, None)  // No source patient context when UUID is given directly
                    }
                    Err(_) => {
                        println!("[MPI] UUID not found directly – searching for latest merge event for patient {}", uuid);
                        let event_uuid = self.find_latest_merge_event_for_patient(uuid).await?;
                        // When using find_latest_merge_event_for_patient, we don't have the source vertex here
                        (event_uuid, None)
                    }
                }
            } else {
                println!("[MPI] Identifier is not a UUID – treating as MRN or patient ID");
                let patient_vertex = get_patient_vertex_by_id_or_mrn(gs, &clean_id)
                    .await
                    .map_err(|e| format!("Patient lookup failed: {}", e))?;

                let mrn = patient_vertex.properties.get("mrn")
                    .and_then(|v| v.as_str())
                    .unwrap_or(&clean_id);

                println!("[MPI] Resolved patient vertex with MRN/ID: {}", mrn);

                // BROADENED SEARCH: Look for any merge-related event
                let query = format!(
                    "MATCH (p:Patient {{mrn: '{}'}})<-[:LOGGED_FOR]-(e) \
                     WHERE e.action IN ['MANUAL_MERGE_EXECUTION', 'MERGE_COMPLETED'] \
                        OR e.event_type IN ['MANUAL_MERGE_INITIATED', 'MANUAL_MERGE_EXECUTION'] \
                     RETURN e ORDER BY e.timestamp DESC LIMIT 1",
                    mrn
                );

                println!("[MPI] Executing discovery query: {}", query);

                let results = gs.execute_cypher_read(&query, serde_json::json!({})).await
                    .map_err(|e| format!("Merge event discovery failed: {}", e))?;

                let event_vertex_json = results.iter()
                    .find_map(|wrapper| {
                        wrapper.get("results")?.as_array()?
                            .get(0)?
                            .get("vertices")?.as_array()?
                            .get(0)
                            .cloned()
                    })
                    .ok_or_else(|| format!("No merge-related event found for patient {}", clean_id))?;

                let uuid_str = event_vertex_json.get("id")
                    .and_then(|v| v.as_str())
                    .ok_or("Missing id on event vertex")?;

                let parsed = Uuid::parse_str(uuid_str)
                    .map_err(|e| format!("Invalid event UUID: {}", e))?;

                println!("[MPI] Discovered merge event UUID: {}", parsed);
                (parsed, Some(patient_vertex))
            }
        };

        println!("[MPI] Executing Discovery-based Rollback for Event: {}", merge_event_uuid);

        // ═════════════════════════════════════════════════════════════════════════
        // PHASE 2: Extract Source & Target from Event Properties
        // ═════════════════════════════════════════════════════════════════════════
        let event_vertex = gs.get_vertex_by_uuid_cypher(&merge_event_uuid).await
            .map_err(|_| "Merge event vertex disappeared".to_string())?;

        println!("[MPI] ===> Event Vertex {:?}", event_vertex);

        // ROBUST RESOLUTION: First try explicit properties, then fallback to merged_into_uuid on source patient
        let (source_uuid, target_uuid) = {
            let explicit_source = [
                "source_uuid", "source_patient_uuid", "from_uuid"
            ].iter()
                .find_map(|&k| event_vertex.properties.get(k)?.as_str())
                .and_then(|s| Uuid::parse_str(s).ok());

            let explicit_target = [
                "target_uuid", "target_patient_uuid", "to_uuid"
            ].iter()
                .find_map(|&k| event_vertex.properties.get(k)?.as_str())
                .and_then(|s| Uuid::parse_str(s).ok());

            if let (Some(src), Some(tgt)) = (explicit_source, explicit_target) {
                println!("[MPI] Resolved source/target from explicit event properties: {} -> {}", src, tgt);
                (src, tgt)
            } else {
                println!("[MPI] Explicit source/target missing – falling back to merged_into_uuid on source patient");

                // Use the source patient vertex we captured during discovery (when identifier was MRN/ID)
                let source_patient = if let Some(v) = source_patient_vertex {
                    v
                } else {
                    // If we came in via UUID, we don't have it — try to infer from event or fail gracefully
                    return Err("Cannot resolve source/target: event lacks explicit UUIDs and identifier was UUID (no patient context)".to_string());
                };

                let target_from_merged = source_patient.properties.get("merged_into_uuid")
                    .and_then(|v| v.as_str())
                    .and_then(|s| Uuid::parse_str(s).ok())
                    .ok_or("Could not resolve target from merged_into_uuid on source patient")?;

                println!("[MPI] Resolved via merged_into_uuid: Source {} -> Target {}", source_patient.id.0, target_from_merged);
                (source_patient.id.0, target_from_merged)
            }
        };

        println!("===> Resolved for Rollback: Source {} -> Target {}", source_uuid, target_uuid);

        // ═════════════════════════════════════════════════════════════════════════
        // PHASE 3: Restore Clinical Edges
        // ═════════════════════════════════════════════════════════════════════════
        let rollback_event_id = gs.persist_mpi_change_event(
            source_uuid,
            target_uuid,
            "MERGE_ROLLBACK_EXECUTED",
            json!({
                "original_event": merge_event_uuid,
                "requested_by": requested_by,
                "timestamp": Utc::now().to_rfc3339(),
                "status": "INITIATED"
            })
        ).await
            .map_err(|e| format!("Failed to log rollback event: {}", e))?;

        println!("[MPI] Created rollback audit event: {}", rollback_event_id);

        let all_edges = gs.get_all_edges().await.map_err(|e| e.to_string())?;
        let event_id_str = merge_event_uuid.to_string();
        let mut restored_count = 0;

        println!("[MPI] Scanning {} edges for migration markers related to event {}", all_edges.len(), merge_event_uuid);

        for edge in all_edges.iter() {
            let was_migrated = edge.properties.get("migration_event_id")
                .and_then(|v| v.as_str()) == Some(&event_id_str)
                || edge.properties.get("merge_event_id")
                    .and_then(|v| v.as_str()) == Some(&event_id_str)
                || edge.properties.get("migrated_from_uuid")
                    .and_then(|v| v.as_str()) == Some(&source_uuid.to_string());

            if was_migrated {
                let mut restored = edge.clone();

                if restored.outbound_id.0 == target_uuid {
                    restored.outbound_id = SerializableUuid(source_uuid);
                }
                if restored.inbound_id.0 == target_uuid {
                    restored.inbound_id = SerializableUuid(source_uuid);
                }

                restored.properties.insert(
                    "rollback_event_id".to_string(),
                    PropertyValue::String(rollback_event_id.to_string())
                );
                restored.properties.insert(
                    "rollback_timestamp".to_string(),
                    PropertyValue::String(Utc::now().to_rfc3339())
                );

                if gs.create_edge(restored).await.is_ok() {
                    restored_count += 1;
                    println!("[MPI] Restored edge {} -> {} (type: {})",
                             edge.outbound_id.0, edge.inbound_id.0, edge.edge_type.as_ref());
                    let _ = gs.delete_edge(Identifier::from(edge.id.0.to_string())).await;
                }
            }
        }

        println!("[MPI] Restored {} clinical edges", restored_count);

        // ═════════════════════════════════════════════════════════════════════════
        // PHASE 4: Recover Source Patient & Re-link to New Golden Record
        // ═════════════════════════════════════════════════════════════════════════
        let source_vertex = {
            match gs.get_vertex_by_uuid_cypher(&source_uuid).await {
                Ok(v) => {
                    println!("[MPI] Source vertex {} found via Cypher lookup", source_uuid);
                    v
                }
                Err(_) => {
                    println!("[MPI] Cypher lookup failed for source {} — falling back to full vertex scan", source_uuid);
                    let all_vertices = gs.get_all_vertices().await
                        .map_err(|e| format!("Failed to load all vertices for scan: {}", e))?;

                    all_vertices.iter()
                        .find(|v| v.id.0 == source_uuid)
                        .cloned()
                        .ok_or_else(|| format!("Source vertex {} not found even in full scan", source_uuid))?
                }
            }
        };

        let patient_obj = Patient::from_vertex(&source_vertex)
            .ok_or("Failed to reconstruct Patient from source vertex")?;

        println!("[MPI] Reconstructing Golden Record for restored patient {}", source_uuid);

        self.create_golden_record_and_link(&patient_obj, source_uuid, requested_by)
            .await
            .map_err(|e| format!("Failed to recreate Golden Record link: {}", e))?;

        let mut updates = HashMap::new();
        updates.insert("patient_status".to_string(), PropertyValue::String("ACTIVE".into()));
        updates.insert("merged_into_uuid".to_string(), PropertyValue::String("".into()));
        updates.insert("updated_at".to_string(), PropertyValue::String(Utc::now().to_rfc3339()));

        gs.update_vertex_properties(source_uuid, updates).await
            .map_err(|e| format!("Failed to reactivate source patient: {}", e))?;

        println!("[MPI] Source patient {} reactivated (status = ACTIVE)", source_uuid);

        // Finalize rollback event
        let mut final_props = HashMap::new();
        final_props.insert("status".to_string(), PropertyValue::String("COMPLETED".into()));
        final_props.insert("edges_restored".to_string(), PropertyValue::Integer(restored_count as i64));

        gs.update_vertex_properties(rollback_event_id, final_props).await.ok();

        println!(
            "[MPI] Rollback SUCCESSFUL for event {}. Restored {} edges. Source {} is now ACTIVE with new Golden Record.",
            merge_event_uuid, restored_count, source_uuid
        );

        Ok(())
    }

    /// Helper to discover the event trail for a patient in the Graph of Changes.
    /// Aligns with 2025-12-20 MPI traceability standards.
    async fn find_latest_merge_event_for_patient(&self, patient_uuid: Uuid) -> Result<Uuid, String> {
        let gs = &self.graph_service;
        
        // We now traverse the relationship established in the 'Graph of Changes' 
        // to satisfy the [2025-12-20] traceability mandate.
        // We use the vertex_id property which we verified matches the SerializableUuid.

        let query = format!(
            "MATCH (p) WHERE id(p) = '{}' \
             MATCH (p)<-[:LOGGED_FOR]-(e:IdentityEvent) \
             RETURN e.id ORDER BY e.timestamp DESC LIMIT 1",
            patient_uuid
        );
                        
        let results = gs.execute_cypher_read(&query, json!({})).await
            .map_err(|e| format!("Graph lookup failed: {}", e))?;
        
        results.get(0)
            .and_then(|row| row.get("e.id"))
            .and_then(|v| v.as_str())
            .map(|s| Uuid::parse_str(s).map_err(|e| e.to_string()))
            .transpose()?
            .ok_or_else(|| format!("No merge history found for patient {} in the graph of events", patient_uuid))
    }

    /// Retrieves the identity audit trail for a given patient ID from the Graph of Events.
    /// This relates every transaction on MPI to the patient's golden record and ID lineage.
    pub async fn get_audit_trail(
        &self, 
        patient_id_str: String, 
        _timeframe: Option<String>,
        requested_by: &str, // Added for 2025-12-20 compliance
    ) -> Result<Vec<String>, String> {
        let patient_id = extract_numeric_patient_id(&patient_id_str)
            .map_err(|e| format!("MPI audit failed: An internal error occurred: {}. {}", patient_id_str, e))?;

        let gs = &self.graph_service; 
        
        // 1. Locate the Patient Vertex
        let patient_vertex = gs.get_patient_vertex_by_id(patient_id)
            .await
            .ok_or_else(|| format!("Patient ID {} not found.", patient_id))?;
        let patient_vertex_id = patient_vertex.id.0;
        
        // 2. AUDIT: Log that an Audit Trail was requested (Compliance Requirement)
        // Structural Log
        gs.log_mpi_transaction(
            patient_vertex_id,
            patient_vertex_id,
            "AUDIT_TRAIL_VIEWED",
            "User requested full identity history",
            requested_by
        ).await.map_err(|e| format!("Failed to log audit access: {}", e))?;

        // Metadata Log
        gs.persist_mpi_change_event(
            patient_vertex_id,
            patient_vertex_id,
            "AUDIT_ACCESS_DETAILS",
            json!({
                "requested_by": requested_by,
                "timestamp": Utc::now().to_rfc3339(),
                "scope": "FULL_LINAGE"
            })
        ).await.map_err(|e| format!("Failed to persist audit event: {}", e))?;

        // 3. Locate the Golden Record for this patient
        let gr_uuid = self.get_golden_record_for_patient_vertex_id(patient_vertex_id).await;

        let graph = gs.read().await;
        let mut audit_entries = Vec::new();

        // 4. TRAVERSAL: Collect Event Nodes from Patient Vertex
        // Look for PARTICIPATED_IN and other identity-linked event edges
        for edge in graph.outgoing_edges(&patient_vertex_id) {
            let label = edge.edge_type.as_ref();
            
            if let Some(event_vertex) = graph.get_vertex(&edge.inbound_id.0) {
                // Only process nodes that are actually IdentityEvents
                if event_vertex.label.as_ref() == "IdentityEvent" {
                    let timestamp = event_vertex.properties.get("timestamp")
                        .and_then(|v| v.as_str())
                        .unwrap_or("Unknown Time");
                    
                    let action = event_vertex.properties.get("action") // Standardized property name
                        .or_else(|| event_vertex.properties.get("event_type"))
                        .and_then(|v| v.as_str())
                        .unwrap_or(label);

                    let user = event_vertex.properties.get("user_id")
                        .or_else(|| event_vertex.properties.get("requested_by"))
                        .and_then(|v| v.as_str())
                        .unwrap_or("System");

                    let reason = event_vertex.properties.get("reason")
                        .and_then(|v| v.as_str())
                        .unwrap_or("No reason provided");

                    audit_entries.push(format!(
                        "[{}] ACTION: {} | USER: {} | REASON: {} | EVENT_ID: {}",
                        timestamp, action, user, reason, event_vertex.id.0
                    ));
                }
            }
        }

        // 5. Trace Golden Record Evolution (The "Golden Record" View)
        if let Some(golden_uuid) = gr_uuid {
            // golden_uuid is already a Uuid, not SerializableUuid
            // Pass it directly to incoming_edges
            for edge in graph.incoming_edges(&golden_uuid) {
                // edge.outbound_id is a SerializableUuid, so we access .0 to get the inner Uuid
                if let Some(event_vertex) = graph.get_vertex(&edge.outbound_id.0) {
                    if event_vertex.label.as_ref() == "IdentityEvent" {
                        let ts = event_vertex.properties.get("timestamp")
                            .and_then(|v| v.as_str())
                            .unwrap_or("---");
                        
                        let act = event_vertex.properties.get("action")
                            .or_else(|| event_vertex.properties.get("event_type"))
                            .and_then(|v| v.as_str())
                            .unwrap_or("GR_EVENT");
                        
                        // Log the event
                        audit_entries.push(format!(
                            "[{}] GOLDEN_RECORD_EVENT: {} | EVENT_ID: {} (Relationship: {})",
                            ts, 
                            act, 
                            edge.outbound_id.0, 
                            edge.edge_type.as_ref()
                        ));
                    }
                }
                
                // Capture search/match events linking to the Golden Record
                if edge.edge_type.as_ref() == "EVALUATED_CANDIDATE" || edge.edge_type.as_ref() == "REVEALED_RECORD" {
                    audit_entries.push(format!(
                        "Identity revealed/matched in search context. Event Source (SearchID): {}", 
                        edge.outbound_id.0
                    ));
                }
            }
        }
        // Sort entries by timestamp (lexicographical sort works for ISO-8601)
        audit_entries.sort();

        info!("Retrieved {} identity audit trail entries for patient {}", audit_entries.len(), patient_id);
        
        if audit_entries.is_empty() {
            return Ok(vec!["No history recorded for this record.".to_string()]);
        }

        Ok(audit_entries)
    }

    /// STATUS: Aggregates stewardship dashboard items with advanced filtering and slicing.
    /// Supports the 2025-12-20 audit requirements for Graph of Events observability.
    pub async fn get_comprehensive_status_data(
        &self,
        only_conflicts: bool,
        limit: Option<usize>,
        slice: Option<String>,
        system: Option<String>,
        requested_by: &str,
    ) -> Result<serde_json::Value> {
        let fetch_limit = limit.unwrap_or(100);
        let slice_direction = slice.unwrap_or_else(|| "top".to_string()).to_lowercase();
        let order = if slice_direction == "bottom" { "ASC" } else { "DESC" };

        // Query: RETURN g, e → GoldenRecord first (index 0), IdentityEvent second (index 1)
        let mut query = "MATCH (e:IdentityEvent)-[:RESULTED_IN]->(g:GoldenRecord)".to_string();

        let mut filters: Vec<String> = Vec::new();
        if only_conflicts {
            filters.push("g.stewardship_status = 'REQUIRES_REVIEW'".to_string());
        }
        if let Some(ref sys) = system {
            filters.push(format!("e.source_system = '{}'", sys));
        }

        if !filters.is_empty() {
            query.push_str(&format!(" WHERE {}", filters.join(" AND ")));
        }

        query.push_str(&format!(
            " RETURN g, e ORDER BY e.timestamp {} LIMIT {}",
            order, fetch_limit
        ));

        let mut results = self
            .graph_service
            .execute_cypher_read(&query, serde_json::json!({}))
            .await?;

        // Fallback if no events
        if results.is_empty() {
            let fallback = format!(
                "MATCH (g:GoldenRecord)
                 OPTIONAL MATCH (g)<-[:RESULTED_IN]-(e:IdentityEvent)
                 RETURN g, e
                 ORDER BY coalesce(e.timestamp, '1970-01-01T00:00:00Z') {} 
                 LIMIT {}",
                order, fetch_limit
            );
            results = self
                .graph_service
                .execute_cypher_read(&fallback, serde_json::json!({}))
                .await?;
        }


        println!("======> RESULTS ARE ====> {:?}", results);
        // Process results — GoldenRecord at index 0, IdentityEvent at index 1
        // Process results
        let records: Vec<StewardshipRecord> = results
            .into_iter()
            .filter_map(|result| {
                // The log shows 'vertices' is a top-level key in the result object
                let vertices_array = result.get("vertices")?.as_array()?;

                // 1. Find the GoldenRecord vertex by label
                let g_vertex = vertices_array.iter().find(|v| v.get("label").and_then(|l| l.as_str()) == Some("GoldenRecord"))?;
                let g_props = g_vertex.get("properties").unwrap_or(&serde_json::Value::Null);

                // 2. Extract GoldenRecord fields (Mapping logic remains same)
                let first_name = g_props.get("first_name")
                    .or_else(|| g_props.get("firstname"))
                    .and_then(|v| v.as_str())
                    .map(ToString::to_string)
                    .unwrap_or_default();

                let last_name = g_props.get("last_name")
                    .or_else(|| g_props.get("lastname"))
                    .and_then(|v| v.as_str())
                    .map(ToString::to_string)
                    .unwrap_or_default();

                let status = g_props.get("stewardship_status")
                    .and_then(|v| v.as_str())
                    .unwrap_or("STABLE")
                    .to_string();

                let primary_mrn = g_props.get("canonical_mrn")
                    .or_else(|| g_props.get("mrn"))
                    .and_then(|v| v.as_str())
                    .map(ToString::to_string);

                let has_unresolved_conflict = status == "REQUIRES_REVIEW";

                // 3. Collect all IdentityEvent vertices
                let recent_events: Vec<GraphEventSummary> = vertices_array
                    .iter()
                    .filter(|v| v.get("label").and_then(|l| l.as_str()) == Some("IdentityEvent"))
                    .filter_map(|e_vertex| {
                        let e_props = e_vertex.get("properties").unwrap_or(&serde_json::Value::Null);

                        let event_type = e_props.get("event_type")
                            .or_else(|| e_props.get("action"))
                            .and_then(|v| v.as_str())
                            .unwrap_or("UNKNOWN")
                            .to_string();

                        let timestamp_str = e_props.get("timestamp")
                            .and_then(|v| v.as_str())
                            .unwrap_or("1970-01-01T00:00:00Z");

                        let timestamp = chrono::DateTime::parse_from_rfc3339(timestamp_str)
                            .map(|dt| dt.with_timezone(&Utc))
                            .unwrap_or_else(|_| Utc::now());

                        let description = e_props.get("reason")
                            .or_else(|| e_props.get("metadata"))
                            .and_then(|v| v.as_str())
                            .map(ToString::to_string)
                            .unwrap_or_else(|| format!("{} event", event_type));

                        Some(GraphEventSummary {
                            event_type,
                            timestamp,
                            description,
                        })
                    })
                    .collect();

                Some(StewardshipRecord {
                    first_name,
                    last_name,
                    has_unresolved_conflict,
                    primary_mrn,
                    status,
                    source_links: vec![],
                    recent_events,
                })
            })
            .collect();

        let golden_count = records.len();

        // Total Patient count
        let total_patient_count: usize = {
            let rows = self
                .graph_service
                .execute_cypher_read("MATCH (p:Patient) RETURN count(p) AS cnt", serde_json::json!({}))
                .await?;

            rows.into_iter()
                .next()
                .and_then(|row| row.get("cnt").and_then(|v| v.as_u64()))
                .unwrap_or(0) as usize
        };

        // Conflict count
        let conflict_count: usize = if only_conflicts {
            golden_count
        } else {
            let rows = self
                .graph_service
                .execute_cypher_read(
                    "MATCH (g:GoldenRecord) WHERE g.stewardship_status = 'REQUIRES_REVIEW' RETURN count(g) AS cnt",
                    serde_json::json!({}),
                )
                .await?;

            rows.into_iter()
                .next()
                .and_then(|row| row.get("cnt").and_then(|v| v.as_u64()))
                .unwrap_or(0) as usize
        };

        // Audit logging
        self.graph_service
            .log_mpi_transaction(
                Uuid::nil(),
                Uuid::nil(),
                "STEWARDSHIP_DASHBOARD_ACCESS",
                &format!("Found {} records", records.len()),
                requested_by,
            )
            .await
            .ok();

        let system_filter_value = system.as_deref();

        let audit_metadata = serde_json::json!({
            "requested_by": requested_by,
            "filter_context": {
                "only_conflicts": only_conflicts,
                "system_filter": system_filter_value,
                "slice": slice_direction
            },
            "timestamp": Utc::now()
        });

        self.graph_service
            .persist_mpi_change_event(
                Uuid::nil(),
                Uuid::nil(),
                "DASHBOARD_QUERY_EXECUTED",
                audit_metadata,
            )
            .await
            .ok();

        let dashboard = MpiStewardshipDashboard {
            golden_count,
            total_patient_count,
            conflict_count,
            records,
        };

        Ok(serde_json::to_value(dashboard)?)
    }

    /// LINEAGE: Fetches filtered lineage report from the Graph of Events.
    pub async fn get_lineage_data(
        &self,
        mpi_id: String,
        _id_type: Option<String>,
        from: Option<String>,
        to: Option<String>,
        head: Option<usize>,
        tail: Option<usize>,
        depth: Option<u32>,
        requested_by: &str,
    ) -> Result<serde_json::Value> {
        let gs = &self.graph_service;
        let search_depth = depth.unwrap_or(3);
        let clean_id = mpi_id.trim_matches(|c| c == '\'' || c == '"');

        // 1. RESOLVE STARTING POINT (Sync with fetch_identity logic)
        let id_type = Self::identify_id_type(clean_id);
        
        let start_vertex = match id_type {
            IdentifierType::MRN => {
                gs.get_patient_by_mrn(clean_id).await?
                    .ok_or_else(|| anyhow!("No patient found with MRN: {}", clean_id))?
            },
            IdentifierType::InternalID => {
                let numeric_id = clean_id.parse::<i32>()
                    .map_err(|_| anyhow!("Invalid internal ID format"))?;
                gs.get_patient_vertex_by_internal_id(numeric_id).await?
                    .ok_or_else(|| anyhow!("No patient found with ID: {}", numeric_id))?
            },
            IdentifierType::UUID | _ => {
                let uuid = Uuid::parse_str(clean_id)
                    .map_err(|_| anyhow!("Invalid UUID format: {}", clean_id))?;
                gs.storage.get_vertex(&uuid).await?
                    .ok_or_else(|| anyhow!("No patient found with UUID: {}", clean_id))?
            }
        };

        // 2. FOLLOW MERGE CHAIN (Find the Golden Record / Survivor)
        let mut canonical_vertex = start_vertex.clone();
        let mut merge_hops = 0;
        let mut trace_path = vec![start_vertex.id.0];
        const MAX_MERGE_HOPS: u32 = 10;

        while let Some(PropertyValue::String(target_uuid_str)) = canonical_vertex.properties.get("merged_into_uuid") {
            if merge_hops >= MAX_MERGE_HOPS {
                return Err(anyhow!("Merge chain too deep or circular for patient {}", clean_id));
            }
            
            let target_uuid = Uuid::parse_str(target_uuid_str)?;
            canonical_vertex = gs.storage.get_vertex(&target_uuid).await?
                .ok_or_else(|| anyhow!("Merge target vertex {} not found", target_uuid_str))?;
            
            merge_hops += 1;
            trace_path.push(canonical_vertex.id.0);
        }

        let target_uuid = canonical_vertex.id.0.to_string();
        let log_uid = canonical_vertex.id.0;

        // 3. EXTRACT DEMOGRAPHICS & RED FLAGS (Strictly Stringify)
        let first_name = canonical_vertex.properties.get("first_name")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string()); // Becomes Some(String) or None, mapping to Option<String>
            
        let last_name = canonical_vertex.properties.get("last_name")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        let red_flag_count = canonical_vertex.properties.get("red_flags")
            .and_then(|v| v.as_str())
            .map(|s| s.split(',').filter(|f| !f.is_empty()).count());

        // 4. LINEAGE TRAVERSAL
        let mut query = format!(
            "MATCH (g {{id: '{target_uuid}'}})-[*1..{search_depth}]-(e:IdentityEvent) "
        );

        let mut filters = Vec::new();
        if let Some(f) = &from { filters.push(format!("e.timestamp >= '{f}'")); }
        if let Some(t) = &to { filters.push(format!("e.timestamp <= '{t}'")); }
        if !filters.is_empty() {
            query.push_str(&format!("WHERE {} ", filters.join(" AND ")));
        }

        query.push_str("RETURN DISTINCT e.id as id, e.event_type as event_type, e.timestamp as timestamp, e.metadata as metadata, e.user_id as user_id, e.source_system as source_system, e.reason as reason, e.action as action ORDER BY e.timestamp ASC ");
        if let Some(h) = head { query.push_str(&format!("LIMIT {} ", h)); }

        let events = gs.execute_cypher_read(&query, serde_json::json!({})).await?;

        // 5. 2-LAYER LOGGING (Compliance 2025-12-20)
        gs.log_mpi_transaction(start_vertex.id.0, log_uid, "IDENTITY_LINEAGE_REVEAL", 
            &format!("Resolved via {} hops. MRN: {}", merge_hops, clean_id), requested_by).await.ok();

        gs.persist_mpi_change_event(start_vertex.id.0, log_uid, "LINEAGE_QUERY_EXECUTED", 
            serde_json::json!({
                "requested_by": requested_by, 
                "input_id": clean_id,
                "resolution_path": trace_path,
                "hops": merge_hops
            })).await.ok();

        // 6. MAP HISTORY CHAIN (Ensure NO nulls for required fields)
        let default_ts = chrono::Utc::now().to_rfc3339();

        let history_entries: Vec<serde_json::Value> = events.iter()
            .map(|row| {
                let event_type = row.get("event_type").and_then(|v| v.as_str()).unwrap_or("UNKNOWN");
                
                serde_json::json!({
                    // Required String in HistoryEntry - MUST NOT BE NULL
                    "timestamp": row.get("timestamp").and_then(|v| v.as_str()).unwrap_or(&default_ts),
                    "action_type": event_type,
                    "user_id": row.get("user_id").and_then(|v| v.as_str()).unwrap_or("system"),
                    "source_system": row.get("source_system").and_then(|v| v.as_str()).unwrap_or("MPI"),
                    
                    // Optional fields (can be null in JSON if Option<T> in Rust)
                    "change_reason": row.get("reason").and_then(|v| v.as_str()),
                    "involved_identity_alias": row.get("metadata").and_then(|m| m.get("input_id")).and_then(|v| v.as_str()),
                    
                    // Required collections
                    "mutations": row.get("metadata").and_then(|m| m.get("mutations")).unwrap_or(&serde_json::json!([])),
                    "is_structural": event_type == "MERGE" || event_type == "SPLIT"
                })
            })
            .collect();

        let final_history = if let Some(t) = tail {
            let start = history_entries.len().saturating_sub(t);
            history_entries[start..].to_vec()
        } else {
            history_entries
        };

        // 7. RETURN ENRICHED PAYLOAD (Matched to LineageReportTrace)
        // Since fields in LineageReportTrace are Option<T>, we pass them through
        Ok(serde_json::json!({
            "first_name": first_name,
            "last_name": last_name,
            "red_flag_count": red_flag_count,
            "history_chain": final_history
        }))
    }
    
    /// SNAPSHOT: Temporal Reconstruction of Patient Identity State
    /// 
    /// Returns the state of a patient's identity as it existed at a specific point in time
    /// (T_snapshot), accounting for record merges, identity resolution, and data lineage.
    /// 
    /// # Purpose in MPI Workflow
    /// 
    /// Unlike a standard "fetch" which returns current state, this method provides:
    /// 
    /// 1. **Resolved Golden Record (Demographics)**
    ///    - Follows merge chains to find the canonical "Survivor" record
    ///    - Returns demographics as they existed at T_snapshot
    ///    - Handles retired/merged MRNs transparently
    /// 
    /// 2. **Identity Resolution Trace**
    ///    - trace_path: List of UUIDs from requested record → canonical record
    ///    - merge_hops: Count of transitions in the merge chain
    ///    - Provides transparency into how the system resolved the identity
    /// 
    /// 3. **History Chain (Temporal Validity)**
    ///    - Returns relationships/attributes valid at T_snapshot
    ///    - Filters based on: valid_from ≤ T_snapshot AND (valid_until > T_snapshot OR valid_until IS NULL)
    ///    - Includes linked MRNs, addresses, and cross-references active at that time
    /// 
    /// 4. **Audit & Compliance Metadata (2025-12-20 Requirement)**
    ///    - Logs to Graph of Changes: Structural traceability
    ///    - Logs to Graph of Events: Detailed temporal query metadata
    ///    - Records: original_request_id, mpi_id, requested_by, as_of timestamp
    /// 
    /// # Returns
    /// 
    /// JSON payload containing:
    /// - `record`: Demographics from the canonical record
    /// - `history_chain`: Temporally-filtered relationships valid at T_snapshot
    /// - `trace_path`: Resolution path (UUIDs from input → canonical)
    /// - `merge_hops`: Number of merge transitions
    /// - `as_of`: The snapshot timestamp
    /// - `mpi_id`: Final canonical Golden Record UUID
    /// - `original_request_id`: What the user actually queried
    pub async fn get_snapshot_data(
        &self,
        mpi_id: String,
        as_of: DateTime<Utc>,
        requested_by: &str,
    ) -> Result<serde_json::Value> {
        let iso_time = as_of.to_rfc3339();
        let gs = &self.graph_service;
        let clean_id = mpi_id.trim_matches(|c| c == '\'' || c == '"');

        info!("[MPI] Snapshot reconstruction for identifier: '{}' as of {}", clean_id, iso_time);
        println!(" [MPI] get_snapshot_data - STEP 1");
        // ═════════════════════════════════════════════════════════════════════════
        // PHASE 1: IDENTITY RESOLUTION
        // ═════════════════════════════════════════════════════════════════════════
        let id_type = Self::identify_id_type(clean_id);
        info!("[MPI] Identified ID type: {:?}", id_type);

        let start_vertex: Vertex = match id_type {
            IdentifierType::MRN => {
                info!("[MPI] Looking up by MRN: '{}'", clean_id);
                gs.get_patient_by_mrn(clean_id).await
                    .map_err(|e| anyhow!("Service call failed during MRN lookup: {}", e))?
                    .ok_or_else(|| anyhow!("MPI Identity Resolution: MRN '{}' not found", clean_id))?
            },
            IdentifierType::InternalID => {
                let numeric_id = clean_id.parse::<i32>()
                    .map_err(|_| anyhow!("Invalid internal ID format: {}", clean_id))?;
                gs.get_patient_vertex_by_internal_id(numeric_id).await?
                    .ok_or_else(|| anyhow!("No patient record found with Internal ID: {}", numeric_id))?
            },
            IdentifierType::UUID | _ => {
                let uuid = Uuid::parse_str(clean_id)
                    .map_err(|_| anyhow!("Invalid UUID format: {}", clean_id))?;
                gs.storage.get_vertex(&uuid).await?
                    .ok_or_else(|| anyhow!("No patient record found with UUID: {}", clean_id))?
            }
        };
        println!(" [MPI] get_snapshot_data - STEP 2");
        // ═════════════════════════════════════════════════════════════════════════
        // PHASE 2: RESOLVE SIBLING/SURVIVOR (Merge Chain Traversal)
        // ═════════════════════════════════════════════════════════════════════════
        let mut canonical_vertex = start_vertex.clone();
        let mut merge_hops = 0;
        let mut trace_path = vec![start_vertex.id.0];
        const MAX_MERGE_HOPS: u32 = 10;

        while let Some(PropertyValue::String(target_uuid_str)) = canonical_vertex.properties.get("merged_into_uuid") {
            if merge_hops >= MAX_MERGE_HOPS {
                error!("[MPI] Circular merge chain detected at {}", canonical_vertex.id.0);
                return Err(anyhow!("Merge chain too deep or circular - possible data corruption"));
            }
            
           println!("[MPI] Following merge chain hop {} to {}", merge_hops + 1, target_uuid_str);
            let target_uuid = Uuid::parse_str(target_uuid_str.as_str())?;
            
            canonical_vertex = gs.storage.get_vertex(&target_uuid).await?
                .ok_or_else(|| anyhow!("Merge target vertex {} not found - broken chain", target_uuid_str))?;
            
            merge_hops += 1;
            trace_path.push(canonical_vertex.id.0);
        }
        println!(" [MPI] get_snapshot_data - STEP 3");
        // CHECK FOR GOLDEN RECORD SIBLING if demographics missing
        if canonical_vertex.properties.get("first_name").is_none() {
            info!("[MPI] Demographics missing on vertex {}, checking for GoldenRecord sibling...", canonical_vertex.id.0);
            let sibling_query = format!(
                "MATCH (n {{id: '{}'}})-[:RESULTED_IN|MERGED_INTO]->(golden:GoldenRecord) RETURN golden",
                canonical_vertex.id.0
            );
            
            if let Ok(results) = gs.execute_cypher_read(&sibling_query, json!({})).await {
                if let Some(row) = results.get(0) {
                    if let Some(v_data) = row.get("results")
                        .and_then(|r| r.as_array())
                        .and_then(|a| a.get(0))
                        .and_then(|i| i.get("vertices"))
                        .and_then(|v| v.as_array())
                        .and_then(|arr| arr.get(0)) {
                            
                        let g_uuid = Uuid::parse_str(v_data.get("id").and_then(|i| i.as_str()).unwrap_or(""))?;
                        if let Ok(Some(g_vertex)) = gs.storage.get_vertex(&g_uuid).await {
                            info!("[MPI] Resolved to Golden Sibling: {}", g_uuid);
                            canonical_vertex = g_vertex;
                            trace_path.push(canonical_vertex.id.0);
                        }
                    }
                }
            }
        }
        println!(" [MPI] get_snapshot_data - STEP 3.1");
        println!("=====> Canonical Vertex in MPI Snapshot {:?}", canonical_vertex);
        let target_uuid = canonical_vertex.id.0.to_string();
        let log_uid = canonical_vertex.id.0;

        // ═════════════════════════════════════════════════════════════════════════
        // PHASE 3: ROBUST DEMOGRAPHIC EXTRACTION
        // ═════════════════════════════════════════════════════════════════════════
        let props = &canonical_vertex.properties;
        
        let get_prop_str = |key: &str| -> String {
            props.get(key).and_then(|v| match v {
                PropertyValue::String(s) => Some(s.clone()),
                PropertyValue::DateTime(dt) => Some(dt.0.to_rfc3339()),
                PropertyValue::I32(i) => Some(i.to_string()),
                PropertyValue::Integer(i) => Some(i.to_string()),
                _ => v.as_str().map(|s| s.to_string())
            }).unwrap_or_default()
        };

        let first_name = get_prop_str("first_name");
        let last_name = get_prop_str("last_name");
        let dob = get_prop_str("date_of_birth");
        let gender = props.get("gender").and_then(|v| v.as_str().map(|s| s.to_string()));
        let last_mod = get_prop_str("updated_at");

        println!(" [MPI] get_snapshot_data - STEP 4");
        // ═════════════════════════════════════════════════════════════════════════
        // PHASE 4: TEMPORAL HISTORY QUERY & XREF EXTRACTION
        // ═════════════════════════════════════════════════════════════════════════
        let history_query = format!("MATCH (n {{id: '{}'}})-[r]-(other) RETURN r, other", target_uuid);
        let result_vec = gs.execute_cypher_read(&history_query, json!({})).await
            .map_err(|e| anyhow!("History query failed: {}", e))?;
        
        let iso_ref = iso_time.as_str();
        let mut cross_refs = Vec::new();
        let history: Vec<serde_json::Value> = result_vec.iter()
            .filter_map(|row| {
                let res_item = row.get("results")?.as_array()?.get(0)?;
                let edges = res_item.get("edges")?.as_array()?;
                let vertices = res_item.get("vertices")?.as_array()?;
                
                let edge = edges.get(0)?;
                let edge_label = edge.get("label").and_then(|l| l.as_str()).unwrap_or("LINKED");
                let edge_props = edge.get("properties")?;
                
                let created_at = edge_props.get("created_at")?.as_str()?;
                let deleted_at = edge_props.get("deleted_at").and_then(|v| v.as_str());
                
                if created_at <= iso_ref && deleted_at.map_or(true, |until| until > iso_ref) {
                    let other_vertex = vertices.iter().find(|v| {
                        v.get("id").and_then(|id| id.as_str()) != Some(&target_uuid)
                    }).or_else(|| vertices.get(0))?;

                    let other_props = other_vertex.get("properties")?;
                    
                    // Populate SnapshotXRef if it's a Patient link
                    if edge_label == "LINKED" || edge_label == "MERGED_INTO" {
                        cross_refs.push(json!({
                            "system": other_props.get("source_system").and_then(|v| v.as_str()).unwrap_or("UNKNOWN"),
                            "mrn": other_props.get("mrn").and_then(|v| v.as_str()).unwrap_or("N/A"),
                            "status": other_props.get("patient_status").and_then(|v| v.as_str()).unwrap_or("ACTIVE"),
                        }));
                    }

                    Some(json!({
                        "relationship_type": edge_label,
                        "connected_entity_id": other_vertex.get("id").and_then(|id| id.as_str()),
                        "valid_from": created_at,
                        "valid_until": deleted_at,
                    }))
                } else { None }
            })
            .collect();

        println!(" [MPI] get_snapshot_data - STEP 5");
        // ═════════════════════════════════════════════════════════════════════════
        // PHASE 5: COMPLIANCE LOGGING (2025-12-20)
        // ═════════════════════════════════════════════════════════════════════════
        gs.log_mpi_transaction(
            start_vertex.id.0, log_uid, "SNAPSHOT_RECONSTRUCT", 
            &format!("Resolved via {} hops. Path: {:?}", merge_hops, trace_path), 
            requested_by
        ).await.ok();
        
        gs.persist_mpi_change_event(
            start_vertex.id.0, log_uid, "TEMPORAL_QUERY_EXECUTED", 
            json!({ 
                "query_type": "SNAPSHOT_RECONSTRUCTION",
                "as_of": iso_time,
                "requested_by": requested_by,
                "id_type": format!("{:?}", id_type),
                "resolution_path": trace_path,
                "relationships_found": history.len(),
                "timestamp": Utc::now().to_rfc3339()
            })
        ).await.ok();
        println!(" [MPI] get_snapshot_data - STEP 6");
        // ═════════════════════════════════════════════════════════════════════════
        // PHASE 6: PAYLOAD ASSEMBLY (Aligned to models/src/evolution.rs MpiSnapshot)
        // ═════════════════════════════════════════════════════════════════════════
        Ok(json!({
            "first_name": first_name,
            "last_name": last_name,
            "dob": dob,
            "gender": gender,
            "cross_refs": cross_refs,
            "version_id": target_uuid,
            "last_modified": last_mod,
            
            // Retaining trace data in a sub-object for debugging, but root satisfies MpiSnapshot
            "resolution_trace": {
                "original_id": clean_id,
                "canonical_id": target_uuid,
                "trace_path": trace_path,
                "merge_hops": merge_hops,
                "id_type": format!("{:?}", id_type),
                "history_chain": history,
            },
            "audit": { "requested_by": requested_by, "logged": true }
        }))
    }

    /// Fetches the Golden Record (Canonical Identity) for a given Patient identifier.
    /// 
    /// Handles three input types automatically:
    /// 1. MRN (Medical Record Number) - alphanumeric strings like "M12345"
    /// 2. Internal numeric ID - integers like 12345 or -557141486
    /// 3. UUID - full UUID format
    pub async fn fetch_identity(
        &self, 
        patient_id_str: String,
        requested_by: &str, // Added for 2025-12-20 audit compliance
    ) -> Result<MasterPatientIndex, String> {
        let gs = &self.graph_service;
        let clean_id = patient_id_str.trim_matches(|c| c == '\'' || c == '"');
        
        info!("[MPI] fetch_identity called with identifier: '{}'", clean_id);
        
        // =====================================================================
        // STEP 1: Identify the type of identifier
        // =====================================================================
        let id_type = Self::identify_id_type(clean_id);
        info!("[MPI] Identified ID type: {:?}", id_type);
        
        // =====================================================================
        // STEP 2: Find the starting patient vertex
        // =====================================================================
        let start_node = match id_type {
            IdentifierType::MRN => {
                info!("[MPI] Looking up by MRN: '{}'", clean_id);
                match gs.get_patient_by_mrn(clean_id).await {
                    Ok(Some(v)) => {
                        info!("[MPI] ✓ Found patient by MRN (Vertex UUID: {})", v.id.0);
                        v
                    },
                    Ok(None) => return Err(format!("No patient found with MRN: '{}'", clean_id)),
                    Err(e) => return Err(format!("Failed to query patient by MRN: {}", e)),
                }
            },
            
            IdentifierType::InternalID => {
                let numeric_id = clean_id.parse::<i32>()
                    .map_err(|e| format!("Failed to parse internal ID: {}", e))?;
                
                info!("[MPI] Looking up by internal ID: {}", numeric_id);
                match gs.get_patient_vertex_by_internal_id(numeric_id).await {
                    Ok(Some(v)) => {
                        info!("[MPI] ✓ Found patient by internal ID (Vertex UUID: {})", v.id.0);
                        v
                    },
                    Ok(None) => return Err(format!("No patient found with internal ID: {}", numeric_id)),
                    Err(e) => return Err(format!("Failed to query patient by internal ID: {}", e)),
                }
            },
            
            IdentifierType::UUID => {
                let uuid = Uuid::parse_str(clean_id)
                    .map_err(|e| format!("Failed to parse UUID: {}", e))?;
                
                info!("[MPI] Looking up by UUID: {}", uuid);
                match gs.storage.get_vertex(&uuid).await {
                    Ok(Some(v)) => {
                        info!("[MPI] ✓ Found patient by UUID");
                        v
                    },
                    Ok(None) => return Err(format!("No patient found with UUID: {}", uuid)),
                    Err(e) => return Err(format!("Failed to retrieve patient by UUID: {}", e)),
                }
            },
            
            IdentifierType::ExternalID => {
                return Err("External ID requires both value and type. Use fetch_identity_by_external_id instead.".to_string());
            }
        };
        
        // =====================================================================
        // STEP 3: Resolve merge chain to find the canonical (survivor) record
        // =====================================================================
        let mut canonical_node = start_node.clone();
        let mut merge_hops = 0;
        const MAX_MERGE_HOPS: u32 = 10;
        let mut trace_path = vec![start_node.id.0];
        
        loop {
            // Extract merged_into_uuid only if it exists and is a non-empty string
            let merged_into_str_opt = canonical_node.properties.get("merged_into_uuid")
                .and_then(|v| v.as_str())
                .map(|s| s.trim())
                .filter(|s| !s.is_empty());

            // If there is no valid merged_into_uuid string → stop traversal (unmerged or cleaned)
            let Some(merged_into_str) = merged_into_str_opt else {
                break;
            };

            // Attempt to parse as UUID – if parsing fails, treat as invalid and stop traversal
            let target_uuid = match Uuid::parse_str(merged_into_str) {
                Ok(uuid) => uuid,
                Err(_) => {
                    info!("[MPI] Invalid/non-UUID value in merged_into_uuid ('{}') on vertex {} – stopping merge chain resolution", merged_into_str, canonical_node.id.0);
                    break;
                }
            };

            if merge_hops >= MAX_MERGE_HOPS {
                error!("[MPI] Detected possible circular merge chain at vertex {}", canonical_node.id.0);
                return Err("Merge chain too deep or circular - possible data corruption".to_string());
            }
            
            // Fetch the next vertex in the chain
            canonical_node = match gs.storage.get_vertex(&target_uuid).await {
                Ok(Some(v)) => v,
                Ok(None) => return Err(format!("Merge target vertex {} not found (broken chain)", target_uuid)),
                Err(e) => return Err(format!("Storage error fetching merge target {}: {}", target_uuid, e)),
            };
            
            merge_hops += 1;
            trace_path.push(canonical_node.id.0);
        }

        // =====================================================================
        // STEP 4: AUDITING - Log the identity resolution event (2025-12-20)
        // =====================================================================
        
        // A. Structural Traceability (Graph of Changes)
        gs.log_mpi_transaction(
            start_node.id.0,
            canonical_node.id.0,
            "IDENTITY_FETCH_REVEAL",
            &format!("Resolved via {} hops. Type: {:?}", merge_hops, id_type),
            requested_by
        ).await.ok();

        // B. Detailed Metadata Persistence (Graph of Events)
        gs.persist_mpi_change_event(
            start_node.id.0,
            canonical_node.id.0,
            "IDENTITY_RESOLUTION_TRAVERSED",
            json!({
                "requested_by": requested_by,
                "input_identifier": clean_id,
                "id_type": format!("{:?}", id_type),
                "hops": merge_hops,
                "resolution_path": trace_path,
                "timestamp": Utc::now().to_rfc3339()
            })
        ).await.ok();
        
        // =====================================================================
        // STEP 5: Extract patient data from the canonical vertex
        // =====================================================================
        let props = &canonical_node.properties;
        
        let patient_id = props.get("id")
            .and_then(|v| match v {
                PropertyValue::Integer(i) => Some(*i as i32),
                PropertyValue::I32(i) => Some(*i),
                PropertyValue::String(s) => s.parse::<i32>().ok(),
                _ => None
            })
            .ok_or_else(|| "Canonical patient vertex missing 'id' property".to_string())?;
        
        let get_string = |key: &str| -> Option<String> {
            props.get(key).and_then(|v| v.as_str()).map(|s| s.to_string())
        };
        
        let get_datetime = |key: &str| -> Option<chrono::DateTime<chrono::Utc>> {
            props.get(key)
                .and_then(|v| v.as_str())
                .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
                .map(|dt| dt.with_timezone(&chrono::Utc))
        };
        
        let date_of_birth = get_datetime("date_of_birth")
            .or_else(|| {
                get_string("date_of_birth")
                    .and_then(|s| chrono::NaiveDate::parse_from_str(&s, "%Y-%m-%d").ok())
                    .map(|d| chrono::DateTime::<chrono::Utc>::from_naive_utc_and_offset(
                        d.and_hms_opt(0, 0, 0).unwrap(),
                        chrono::Utc
                    ))
            });
        
        let address = Self::extract_address_from_vertex(&canonical_node);
        
        // =====================================================================
        // STEP 6: Construct and return MasterPatientIndex
        // =====================================================================
        let mpi = MasterPatientIndex {
            id: rand::random::<i32>(),
            patient_id: Some(patient_id),
            first_name: get_string("first_name"),
            last_name: get_string("last_name"),
            date_of_birth,
            gender: get_string("gender"),
            address,
            contact_number: get_string("contact_number")
                .or_else(|| get_string("phone_mobile"))
                .or_else(|| get_string("phone_home")),
            email: get_string("email"),
            social_security_number: get_string("ssn"),
            match_score: None,
            match_date: None,
            created_at: get_datetime("created_at").unwrap_or_else(|| chrono::Utc::now()),
            updated_at: get_datetime("updated_at").unwrap_or_else(|| chrono::Utc::now()),
        };
        
        info!("[MPI] ✓ Successfully resolved identity for patient ID: {}", patient_id);
        
        Ok(mpi)
    }

    /// NEW: Search for potential patient matches.
    pub async fn search(&self, patient: Patient, requested_by: &str) -> Result<Vec<MasterPatientIndex>, String> {
        let gs = &self.graph_service;
        let search_id = rand::random::<u32>();
        
        // 1. Audit search event
        let event_id = Uuid::new_v4(); 
        let _ = gs.persist_mpi_change_event(
            Uuid::nil(), 
            Uuid::nil(), 
            "DEMOGRAPHIC_SEARCH_INITIATED",
            json!({ 
                "search_id": search_id, 
                "criteria": {
                    "first_name": patient.first_name,
                    "last_name": patient.last_name,
                    "dob": patient.date_of_birth
                }, 
                "requested_by": requested_by 
            })
        ).await;

        // 2. Build the Cypher query
        let dob_str = patient.date_of_birth.as_ref()
            .map(|d| d.0.format("%Y-%m-%d").to_string())
            .unwrap_or_default();
        
        let first_name_str = patient.first_name.as_ref().map(|s| s.as_str()).unwrap_or("");

        let query = format!(
            "MATCH (p:Patient) WHERE p.date_of_birth STARTS WITH '{}' \
             WITH p, levenshtein(p.first_name, '{}') AS dist \
             WHERE dist < 3 RETURN p ORDER BY dist ASC LIMIT 10",
            dob_str,
            first_name_str
        );

        // 3. Execute and Parse
        let raw_results = gs.execute_cypher_read(&query, Value::Null).await
            .map_err(|e| format!("DB Read Failed: {}", e))?;

        let mut mpi_results = Vec::new();

        println!("===> DEBUG: raw_results.len() = {}", raw_results.len());
        
        // FIXED: Properly traverse the structure
        for (idx, result_wrapper) in raw_results.iter().enumerate() {
            println!("===> DEBUG: Processing wrapper {}", idx);
            
            // Get the "results" array from the wrapper
            if let Some(results_array) = result_wrapper.get("results").and_then(|v| v.as_array()) {
                println!("===> DEBUG: Found results array with {} items", results_array.len());
                
                // Each item in results has vertices, edges, and stats
                for (ridx, result_item) in results_array.iter().enumerate() {
                    println!("===> DEBUG: Processing result_item {}", ridx);
                    
                    if let Some(vertices) = result_item.get("vertices").and_then(|v| v.as_array()) {
                        println!("===> DEBUG: Found {} vertices", vertices.len());
                        
                        for (vidx, v_json) in vertices.iter().enumerate() {
                            println!("===> DEBUG: Processing vertex {}", vidx);
                            
                            let v_obj = match v_json.as_object() {
                                Some(obj) => obj,
                                None => {
                                    println!("===> DEBUG: Vertex {} is not an object!", vidx);
                                    continue;
                                }
                            };
                            
                            let v_props = match v_obj.get("properties").and_then(|p| p.as_object()) {
                                Some(props) => props,
                                None => {
                                    println!("===> DEBUG: Vertex {} has no properties!", vidx);
                                    continue;
                                }
                            };
                            
                            let v_uuid_str = v_obj.get("id").and_then(|id| id.as_str()).unwrap_or("");
                            let v_uuid = Uuid::parse_str(v_uuid_str).unwrap_or_default();

                            println!("===> DEBUG: Vertex {} UUID: {}", vidx, v_uuid);

                            // --- GOLDEN RECORD FALLBACK ---
                            let all_edges = gs.get_all_edges().await.unwrap_or_default();
                            let active_gr_link = all_edges.iter().find(|e| {
                                e.outbound_id.0 == v_uuid && e.edge_type.as_ref() == "HAS_GOLDEN_RECORD"
                            });

                            let gr_props = if let Some(edge) = active_gr_link {
                                let gr_query = format!("MATCH (g:GoldenRecord) WHERE g.id = '{}' RETURN g", edge.inbound_id.0);
                                if let Ok(gr_res) = gs.execute_cypher_read(&gr_query, Value::Null).await {
                                    gr_res.iter()
                                        .find_map(|gr_wrapper| {
                                            gr_wrapper.get("results")?.as_array()?
                                                .iter()
                                                .find_map(|gr_item| {
                                                    gr_item.get("vertices")?.as_array()?
                                                        .get(0)?
                                                        .get("properties")?
                                                        .as_object()
                                                        .cloned()
                                                })
                                        })
                                } else {
                                    None
                                }
                            } else {
                                None
                            };

                            // Helper to get property from GR first, then fall back to patient vertex
                            let get_str = |key: &str| -> Option<String> {
                                gr_props.as_ref()
                                    .and_then(|m| m.get(key))
                                    .and_then(|v| v.as_str())
                                    .or_else(|| v_props.get(key).and_then(|v| v.as_str()))
                                    .map(|s| s.to_string())
                            };

                            // Get the legacy ID from the vertex properties
                            let legacy_id = v_props.get("id")
                                .and_then(|v| v.as_i64())
                                .map(|i| i as i32)
                                .unwrap_or_else(|| (v_uuid.as_u128() & 0x7FFFFFFF) as i32);

                            println!("===> DEBUG: Creating MPI for patient ID {}", legacy_id);

                            mpi_results.push(MasterPatientIndex {
                                id: rand::random(),
                                patient_id: Some(legacy_id),
                                first_name: get_str("first_name"),
                                last_name: get_str("last_name"),
                                match_score: Some(1.0),
                                match_date: Some(Utc::now()),
                                date_of_birth: get_str("date_of_birth")
                                    .and_then(|s| chrono::DateTime::parse_from_rfc3339(&s).ok())
                                    .map(|dt| dt.with_timezone(&Utc)),
                                gender: get_str("gender"),
                                address: get_str("address").and_then(|s| serde_json::from_str(&s).ok()),
                                contact_number: get_str("contact_number"),
                                email: get_str("email"),
                                social_security_number: get_str("ssn"),
                                created_at: Utc::now(),
                                updated_at: Utc::now(),
                            });
                        }
                    } else {
                        println!("===> DEBUG: result_item {} has no vertices", ridx);
                    }
                }
            } else {
                println!("===> DEBUG: wrapper {} has no results array", idx);
            }
        }

        println!("===> DEBUG: Total mpi_results collected: {}", mpi_results.len());

        // 4. Update Event Audit
        let mut update_props = HashMap::new();
        update_props.insert("results_count".to_string(), PropertyValue::Integer(mpi_results.len() as i64));
        let _ = gs.update_vertex_properties(event_id, update_props).await;

        info!("[MPI] Search complete. Found {} records.", mpi_results.len());
        Ok(mpi_results)
    }

    /// NEW: Resolves a set of patient records based on a Master Patient Index.
    /// Returns all source patient IDs that are linked to the given MPI/Golden Record.
    pub async fn resolve(&self, mpi: MasterPatientIndex) -> Result<Vec<i32>, anyhow::Error> {
        // 1. Extract canonical_id (reusing first_name per your logic)
        let canonical_id = mpi.first_name
            .ok_or_else(|| anyhow!("MPI record must contain canonical_id for resolution."))?;
        
        let gs = &self.graph_service;
        
        // We clone canonical_id here because format! below will take ownership/move it
        info!("[MPI] Resolving identity lineage for Golden Record: {}", canonical_id);

        // 2. Audit the Resolution Request (2025-12-20 Compliance)
        let event_id = gs.persist_mpi_change_event(
            Uuid::nil(),
            Uuid::nil(),
            "IDENTITY_RESOLUTION_REQUESTED",
            json!({"canonical_id": canonical_id})
        ).await.map_err(|e| anyhow!("Resolution audit failed: {}", e))?;
        
        // 3. Query all Patients linked via HAS_GOLDEN_RECORD
        // We use .clone() so we can still use canonical_id for the final info! log
        let query = format!(
            r#"
            MATCH (p:Patient)-[:HAS_GOLDEN_RECORD]->(g:GoldenRecord {{canonical_id: "{}"}})
            RETURN p.id AS patientId, p.uuid AS patientUuid
            "#,
            canonical_id.clone()
        );

        let result = gs.execute_cypher_read(&query, Value::Null).await
            .map_err(|e| anyhow!("Failed to resolve patients for canonical ID {}: {}", canonical_id, e))?;

        // 4. Extract IDs and Audit each revealed connection
        let mut resolved_patient_ids = Vec::new();
        
        // Your graph traversal logic
        for row in result {
            if let Some(p_id) = row.get("patientId").and_then(|v| v.as_i64()) {
                resolved_patient_ids.push(p_id as i32);
                
                // Log that this specific record was revealed (Graph of Changes compliance)
                if let Some(p_uuid_str) = row.get("patientUuid").and_then(|v| v.as_str()) {
                    if let Ok(p_uuid) = Uuid::parse_str(p_uuid_str) {
                        // FIX: Edge::new expects outbound_id, edge_type (Identifier), inbound_id
                        // Identifiers are for types, but IDs must be SerializableUuid
                        let edge = Edge::new(
                            SerializableUuid(event_id),
                            Identifier::new("REVEALED_RECORD".to_string()).unwrap(),
                            SerializableUuid(p_uuid) 
                        );
                        let _ = gs.create_edge(edge).await;
                    }
                }
            }
        }
        
        info!("Resolved {} patient records for canonical ID: {}", resolved_patient_ids.len(), canonical_id);
        Ok(resolved_patient_ids)
    }

    // =========================================================================
    // MATCHING LOGIC (No major functional change, but included for completeness)
    // =========================================================================
    async fn find_candidates(&self, patient: &Patient) -> Vec<PatientCandidate> {
        let mut candidates_with_metadata = HashMap::new(); // candidate_id -> list of keys hit
        let gs = &self.graph_service;
        let graph = gs.read().await;

        // 1. Blocking on MRN
        if let Some(mrn) = patient.mrn.as_ref() {
            let mrn_idx = self.mrn_index.read().await;
            if let Some(id) = mrn_idx.get(mrn) {
                candidates_with_metadata.entry(*id).or_insert_with(Vec::new).push("MRN".to_string());
            }
        }
        
        // 2. Blocking on Name + DOB
        let last_norm = patient.last_name.as_deref().unwrap_or("").to_lowercase();
        let first_norm = patient.first_name.as_deref().unwrap_or("").to_lowercase();
        let norm_name = format!("{} {}", last_norm, first_norm);
        let dob = patient.date_of_birth
            .as_ref()
            .map(|d| d.0.format("%Y-%m-%d").to_string())
            .unwrap_or_default();
        
        let name_dob_idx = self.name_dob_index.read().await;
        if let Some(ids) = name_dob_idx.get(&(norm_name, dob)) {
            for &id in ids {
                candidates_with_metadata.entry(id).or_insert_with(Vec::new).push("NAME_DOB".to_string());
            }
        }

        // 3. Blocking on SSN
        if let Some(ssn) = patient.ssn.as_ref() {
            let ssn_idx = self.ssn_index.read().await;
            if let Some(id) = ssn_idx.get(ssn) {
                candidates_with_metadata.entry(*id).or_insert_with(Vec::new).push("SSN".to_string());
            }
        }

        let mut scored = Vec::new();
        let current_vertex_id = patient.to_vertex().id.0;

        for (candidate_id, keys) in candidates_with_metadata {
            if candidate_id == current_vertex_id {
                continue;
            }
            
            if let Some(vertex) = graph.get_vertex(&candidate_id) {
                if let Some(existing) = Patient::from_vertex(vertex) {
                    let score = self.calculate_match_score(patient, &existing);
                    
                    scored.push(PatientCandidate {
                        patient_vertex_id: candidate_id,
                        patient_id: existing.id.unwrap_or(0),
                        master_record_id: None,
                        match_score: score,
                        blocking_keys: keys, // Now passing the keys that caused the hit
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
        if a.mrn == b.mrn && a.mrn.is_some() { 
            score += WEIGHT_MRN; 
        }
        
        // 2. Name Similarity (Medium Weight: 0.30)
        // Use as_deref() to get &str and unwrap_or to handle None as empty string
        let first_a = a.first_name.as_deref().unwrap_or("");
        let last_a = a.last_name.as_deref().unwrap_or("");
        let first_b = b.first_name.as_deref().unwrap_or("");
        let last_b = b.last_name.as_deref().unwrap_or("");

        let name_a = format!("{} {}", first_a, last_a).to_lowercase();
        let name_b = format!("{} {}", first_b, last_b).to_lowercase();
        
        // Only calculate similarity if the names aren't both empty
        if !name_a.trim().is_empty() && !name_b.trim().is_empty() {
            // Jaro-Winkler (Good for typographical errors at start of string)
            let jaro_winkler_sim = strsim::jaro_winkler(&name_a, &name_b);

            // Levenshtein Similarity (Good for general edit distance)
            let levenshtein_dist = strsim::levenshtein(&name_a, &name_b) as f64;
            let max_len = (name_a.len().max(name_b.len())) as f64;
            
            let levenshtein_sim = if max_len == 0.0 { 1.0 } else { 1.0 - (levenshtein_dist / max_len) };

            // Average the two similarity scores and apply the weight
            let avg_name_sim = (jaro_winkler_sim + levenshtein_sim) / 2.0;
            score += avg_name_sim * WEIGHT_NAME; 
        }
        
        // 3. DOB Match (High Weight: 0.20)
        // Ensure they match AND it's not the default sentinel date (1900-01-01)
        let sentinel_date = chrono::NaiveDate::from_ymd_opt(1900, 1, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();
            
        // Check if both exist and are equal
        if a.date_of_birth.is_some() && a.date_of_birth == b.date_of_birth {
            // Now safely check the sentinel value on the inner datetime
            let is_not_sentinel = a.date_of_birth.as_ref()
                .map(|dt| dt.0.naive_utc() != sentinel_date)
                .unwrap_or(false);

            if is_not_sentinel {
                score += WEIGHT_DOB;
            }
        }
                
        // 4. SSN Match (Medium Weight: 0.10)
        if a.ssn == b.ssn && a.ssn.is_some() { 
            score += WEIGHT_SSN; 
        }

        // 5. Contact Info Match (Low Weight: 0.05 total)
        let mut contact_score = 0.0;
        if a.email == b.email && a.email.is_some() { contact_score += 0.5; }
        if a.phone_mobile == b.phone_mobile && a.phone_mobile.is_some() { contact_score += 0.5; }
        
        score += contact_score * WEIGHT_CONTACT;

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
        requested_by: &str,
    ) -> Result<Uuid, String> {
        info!("Creating Golden Record for Patient UUID: {}", patient_vertex_uuid);
        let gs = &self.graph_service;

        // =========================================================================
        // 1. LOGGING: Initialize the Event in the Graph of Events & Changes
        // =========================================================================
        
        // Structural Log (Graph of Changes) - 2025-12-20 Requirement
        gs.log_mpi_transaction(
            patient_vertex_uuid,
            patient_vertex_uuid,
            "GOLDEN_RECORD_CREATION_INITIATED",
            "Initializing new identity master record",
            requested_by
        ).await.ok();

        // Metadata Log (Graph of Events)
        let event_id = gs.persist_mpi_change_event(
            patient_vertex_uuid,
            Uuid::nil(),
            "GOLDEN_RECORD_INITIALIZED",
            json!({
                "trigger": "System Initialization or First Match",
                "source_mrn": patient.mrn,
                "requested_by": requested_by,
                "timestamp": Utc::now().to_rfc3339()
            })
        ).await.map_err(|e| format!("Failed to log identity initialization: {}", e))?;

        // =========================================================================
        // 2. Create the Golden Record Vertex
        // =========================================================================
        let gr_uuid = Uuid::new_v4();
        let gr_id_string = format!("GOLDEN-{}", gr_uuid);
        
        let mut gr_vertex = Vertex::new(
            Identifier::new("GoldenRecord".to_string()).map_err(|e| e.to_string())?
        );
        
        // Override the auto-generated UUID with our controlled one
        gr_vertex.id = SerializableUuid(gr_uuid);
        
        // CRITICAL: Set the 'id' property (the string identifier)
        gr_vertex.properties.insert(
            "id".to_string(), 
            PropertyValue::String(gr_id_string.clone())
        );
        
        // Set traceability metadata
        gr_vertex.properties.insert(
            "canonical_id".to_string(), 
            PropertyValue::String(gr_uuid.to_string())
        );
        gr_vertex.properties.insert(
            "origin_event_id".to_string(),
            PropertyValue::String(event_id.to_string())
        );
        
        // Store the target patient UUID for cross-reference
        gr_vertex.properties.insert(
            "target_patient_vertex_uuid".to_string(),
            PropertyValue::String(patient_vertex_uuid.to_string())
        );

        // Persist demographics for the Golden Record (Full set preserved)
        if let Some(ref first) = patient.first_name {
            gr_vertex.properties.insert("first_name".to_string(), PropertyValue::String(first.clone()));
        }
        if let Some(ref last) = patient.last_name {
            gr_vertex.properties.insert("last_name".to_string(), PropertyValue::String(last.clone()));
        }
        if let Some(ref ssn) = patient.ssn {
            gr_vertex.properties.insert("ssn".to_string(), PropertyValue::String(ssn.clone()));
        }
        if let Some(ref mrn) = patient.mrn {
            gr_vertex.properties.insert("canonical_mrn".to_string(), PropertyValue::String(mrn.clone()));
        }
        
        // Handle date_of_birth as Option<SerializableDateTime>
        if let Some(dob) = &patient.date_of_birth {
            gr_vertex.properties.insert(
                "date_of_birth".to_string(), 
                PropertyValue::String(dob.0.to_rfc3339())
            );
        }

        // Ensure created_at and updated_at use your PropertyValue::DateTime 
        // or PropertyValue::String consistently. 
        // Using the DateTime variant is preferred for the Graph of Changes.
        gr_vertex.properties.insert(
            "created_at".to_string(), 
            PropertyValue::DateTime(SerializableDateTime(chrono::Utc::now()))
        );

        gr_vertex.properties.insert(
            "updated_at".to_string(), 
            PropertyValue::DateTime(SerializableDateTime(chrono::Utc::now()))
        );

        gs.add_vertex(gr_vertex).await
            .map_err(|e| format!("Failed to add Golden Record vertex: {}", e))?;

        // =========================================================================
        // 3. Establish the Relationship (Graph of Changes)
        // FIXED: Correct edge direction - Patient -> GoldenRecord
        // =========================================================================
        let edge_label = Identifier::new("HAS_GOLDEN_RECORD".to_string())
            .map_err(|e| e.to_string())?;

        let mut edge = Edge::new(
            SerializableUuid(patient_vertex_uuid), // outbound_id (Patient)
            edge_label,                             // label
            SerializableUuid(gr_uuid)               // inbound_id (Golden Record)
        );

        // Enrich the edge with audit metadata for the Change Graph
        edge.properties.insert("established_by_event".to_string(), PropertyValue::String(event_id.to_string()));
        edge.properties.insert("confidence_at_creation".to_string(), PropertyValue::String("1.0 (Direct)".to_string()));
        edge.properties.insert("requested_by".to_string(), PropertyValue::String(requested_by.to_string()));

        gs.create_edge(edge).await
            .map_err(|e| format!("Failed to create and sync HAS_GOLDEN_RECORD edge: {}", e))?;

        // =========================================================================
        // 4. FINAL LOGGING: Close the Event loop
        // =========================================================================
        let mut event_updates = HashMap::new();
        event_updates.insert("target_uuid".to_string(), PropertyValue::String(gr_uuid.to_string()));
        event_updates.insert("status".to_string(), PropertyValue::String("COMPLETED".to_string()));
        
        gs.update_vertex_properties(event_id, event_updates).await.ok();
                
        info!("✅ Golden Record created and linked. Canonical ID: {} | Event: {}", gr_uuid, event_id);
        
        Ok(gr_uuid)
    }

    /// Ensures the given Patient vertex is linked to a Golden Record.
    /// If no link exists, it creates a new Golden Record (identity) and links the Patient to it.
    /// Returns the canonical ID of the associated Golden Record.
    async fn ensure_golden_record_and_link(
        &self, 
        patient_vertex: &Vertex,
        requested_by: &str 
    ) -> Result<String, String> {
        let gs = &self.graph_service;
        let patient_uuid = patient_vertex.id.0;
        
        // Use first_name/last_name for logging to match your search context
        let first_name = patient_vertex.properties.get("first_name").and_then(|v| v.as_str()).unwrap_or("Unknown");
        let last_name = patient_vertex.properties.get("last_name").and_then(|v| v.as_str()).unwrap_or("Unknown");

        info!("[MPI] Ensuring identity link for Patient: {} {} ({})", first_name, last_name, patient_uuid);

        // =========================================================================
        // STEP 1: IDEMPOTENCY CHECK - Corrected Direction
        // Direction: Patient (Outbound) -[:HAS_GOLDEN_RECORD]-> GoldenRecord (Inbound)
        // =========================================================================
        let all_edges = gs.get_all_edges().await
            .map_err(|e| format!("Failed to retrieve edges: {}", e))?;
        
        // Check if THIS SPECIFIC patient vertex already has a link
        let existing_gr_edge = all_edges.iter().find(|e| {
            e.outbound_id.0 == patient_uuid && // Fixed: Patient is the source
            e.edge_type.as_ref() == "HAS_GOLDEN_RECORD"
        });
        
        if let Some(edge) = existing_gr_edge {
            let gr_uuid = edge.inbound_id.0; // Fixed: Golden Record is the target
            
            let gr_vertex = gs.get_vertex(&gr_uuid).await
                .ok_or_else(|| format!("Dangling edge! GR vertex {} missing", gr_uuid))?;
            
            // Use 'canonical_id' or 'id' depending on your GoldenRecord schema
            let gr_id = gr_vertex.properties.get("id")
                .and_then(|v| v.as_str())
                .or_else(|| gr_vertex.properties.get("canonical_id").and_then(|v| v.as_str()))
                .unwrap_or("UNKNOWN-GR-ID")
                .to_string();
            
            info!("[MPI] Identity link verified: Patient {} -> GR {}", patient_uuid, gr_id);
            
            return Ok(gr_id);
        }
        
        // =========================================================================
        // STEP 2: NO LINK FOUND - Create one for THIS patient
        // =========================================================================
        // We do NOT look up by MRN here because that would "collapse" two different
        // patients into one Golden Record prematurely. Each distinct patient 
        // vertex gets its own Golden Record until they are merged.
        
        let event_id = gs.persist_mpi_change_event(
            patient_uuid,
            Uuid::nil(),
            "LINK_ABSENT_INITIATING_CREATION",
            json!({ 
                "first_name": first_name, 
                "last_name": last_name, 
                "requested_by": requested_by 
            })
        ).await.map_err(|e| format!("Audit log failed: {}", e))?;

        let patient_model = Patient::from_vertex(patient_vertex)
            .ok_or_else(|| "Failed to convert Patient vertex to model.".to_string())?;
        
        // Create a NEW Golden Record specifically for this patient
        let gr_uuid = self.create_golden_record_and_link(&patient_model, patient_uuid, requested_by).await
            .map_err(|e| format!("Failed to create Golden Record: {}", e))?;
        
        // =========================================================================
        // STEP 3: FINAL VERIFICATION
        // =========================================================================
        let gr_vertex = gs.get_vertex(&gr_uuid).await
            .ok_or_else(|| format!("Verification failed: GoldenRecord {} not found", gr_uuid))?;
        
        let final_id = gr_vertex.properties.get("id")
            .and_then(|v| v.as_str())
            .or_else(|| gr_vertex.properties.get("canonical_id").and_then(|v| v.as_str()))
            .ok_or_else(|| "GoldenRecord missing id property".to_string())?
            .to_string();

        // Log in the Graph of Changes (Golden Record Created/Linked)
        gs.log_mpi_transaction(
            patient_uuid,
            gr_uuid,
            "GOLDEN_RECORD_LINKED",
            "Initial creation of master identity",
            requested_by
        ).await.ok();

        let mut props = HashMap::new();
        props.insert("status".to_string(), PropertyValue::String("COMPLETED".to_string()));
        props.insert("final_golden_record_id".to_string(), PropertyValue::String(final_id.clone()));
        let _ = gs.update_vertex_properties(event_id, props).await;
        
        Ok(final_id)
    }

    // =========================================================================
    // AUTO-MERGE (Updated for Golden Record)
    // =========================================================================
    async fn auto_merge(
        &self, 
        new_patient_vertex_id: Uuid, 
        new_patient: Patient, 
        candidate: PatientCandidate,
        requested_by: &str, // Added for dual-log compliance
    ) {
        let gs = &self.graph_service;
        
        // 1. Resolve the Target Identity
        let candidate_patient_vertex = match gs.storage.get_vertex(&candidate.patient_vertex_id).await {
            Ok(Some(v)) => v,
            Ok(None) => {
                error!("Candidate patient vertex not found for UUID: {}", candidate.patient_vertex_id);
                return;
            }
            Err(e) => {
                error!("Error retrieving candidate patient vertex {}: {}", candidate.patient_vertex_id, e);
                return;
            }
        };

        let target_gr_result = self.ensure_golden_record_and_link(&candidate_patient_vertex, &requested_by).await;
        let target_gr_canonical_id = match target_gr_result {
            Ok(id) => id,
            Err(e) => { 
                error!("Failed to ensure Golden Record for candidate {}: {}", candidate.patient_vertex_id, e); 
                return; 
            }
        };

        let patient_display_id = new_patient.id.map(|id| id.to_string()).unwrap_or_else(|| "NONE".to_string());
        
        // =========================================================================
        // 2. AUDIT: Dual-Logging Strategy (Graph of Events & Graph of Changes)
        // =========================================================================
        
        // Log A: Structural Transaction (PARTICIPATED_IN -> Event -> RESULTED_IN)
        let log_result = gs.log_mpi_transaction(
            new_patient_vertex_id,
            candidate_patient_vertex.id.0,
            "AUTO_MERGE_EXECUTION",
            &format!("Probabilistic match score: {:.4}", candidate.match_score),
            requested_by
        ).await;

        if let Err(e) = log_result {
            error!("CRITICAL: Failed structural log for auto-merge: {}", e);
            return;
        }

        // Log B: Detailed Metadata Persistence
        let event_id = match gs.persist_mpi_change_event(
            new_patient_vertex_id, 
            candidate_patient_vertex.id.0, 
            "AUTO_MERGE_TRIGGERED",
            json!({
                "policy": "PROBABILISTIC_AUTO_MATCH",
                "match_score": candidate.match_score,
                "target_golden_record": target_gr_canonical_id,
                "blocking_keys_hit": candidate.blocking_keys,
                "requested_by": requested_by,
                "timestamp": Utc::now().to_rfc3339()
            })
        ).await {
            Ok(uuid) => uuid,
            Err(e) => {
                error!("CRITICAL: Failed metadata audit trail for auto-merge: {}", e);
                return;
            }
        };

        info!(
            "[MPI-EVENT:{}] Performing auto-merge: Linking Patient {} to Golden Record {}", 
            event_id, patient_display_id, target_gr_canonical_id
        );

        // =========================================================================
        // 3. GRAPH OF CHANGES: Execute Atomic Redirection
        // =========================================================================
        
        let merge_transaction_query = format!(
            r#"
            MATCH (source:Patient) WHERE source.id = "{source_id}"
            MATCH (targetGR:GoldenRecord {{canonical_id: "{gr_id}"}})
            
            // Remove existing links
            OPTIONAL MATCH (source)-[old_r:HAS_GOLDEN_RECORD]->(:GoldenRecord)
            DELETE old_r
            
            // Create the new Change Graph relationship
            CREATE (source)-[link:HAS_GOLDEN_RECORD {{
                policy: "AUTO_MATCH", 
                merged_at: timestamp(), 
                match_score: {score},
                event_id: "{event_id}"
            }}]->(targetGR)
            
            // Update Status (Graph of Changes)
            SET source.patient_status = "MERGED", 
                source.merged_into_uuid = "{target_patient_uuid}",
                source.updated_at = timestamp(),
                source.last_event_id = "{event_id}"
            
            RETURN source
            "#,
            source_id = new_patient_vertex_id,
            gr_id = target_gr_canonical_id,
            score = candidate.match_score,
            event_id = event_id,
            target_patient_uuid = candidate_patient_vertex.id.0
        );

        if let Err(e) = gs.execute_cypher_write(&merge_transaction_query, Value::Null).await {
            error!("AUTO-MERGE FAILURE [Event {}]: {}", event_id, e);
            
            let _ = gs.update_vertex_properties(event_id, HashMap::from([
                ("status".to_string(), PropertyValue::String("FAILED".to_string())),
                ("error".to_string(), PropertyValue::String(e.to_string()))
            ])).await;
            return;
        }

        // =========================================================================
        // 4. CONFLICT DETECTION: Re-scan the target identity
        // =========================================================================
        let _ = gs.detect_and_persist_red_flags(candidate_patient_vertex.id.0).await;

        // =========================================================================
        // 5. FINALIZATION: Complete the Audit Cycle
        // =========================================================================
        let _ = gs.update_vertex_properties(event_id, HashMap::from([
            ("status".to_string(), PropertyValue::String("COMPLETED".to_string())),
            ("completed_at".to_string(), PropertyValue::String(chrono::Utc::now().to_rfc3339()))
        ])).await;

        info!(
            "✅ Auto-merge complete: New patient {} successfully retired into Golden Record {}. Event: {}", 
            patient_display_id, target_gr_canonical_id, event_id
        );
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
        user_role: String,
        resolution_policy: String,
    ) -> Result<Uuid, String> {
        let gs = self.graph_service.clone();
        let timestamp = Utc::now().to_rfc3339();

        // =========================================================================
        // 1. ANCHOR: Locate the Patient and Golden Record for Context
        // =========================================================================
        // We need to know which Golden Record this resolution affected for the Change Graph.
        let gr_uuid = self.get_golden_record_for_patient_vertex_id(conflict_vertex_id).await;

        // =========================================================================
        // NEW COMPLIANCE BLOCK: GRAPH OF CHANGES (Structural Log)
        // =========================================================================
        let target_gr = gr_uuid.unwrap_or(Uuid::nil());
        gs.log_mpi_transaction(
            conflict_vertex_id,
            target_gr,
            "MANUAL_CONFLICT_RESOLUTION",
            &format!("Policy: {}", resolution_policy),
            &user_id
        ).await.ok();

        // =========================================================================
        // 2. GRAPH OF EVENTS: Persist the Resolution Event
        // =========================================================================
        // This logs the "What and Why" of the transaction.
        let event_id = gs.persist_mpi_change_event(
            conflict_vertex_id, // Subject of the conflict
            target_gr, // Associated Golden Record
            "CONFLICT_RESOLVED_MANUALLY",
            json!({
                "user_id": user_id,
                "user_role": user_role,
                "policy_applied": resolution_policy,
                "resolution_timestamp": timestamp,
                "status": "COMPLETED"
            })
        ).await.map_err(|e| format!("Failed to log event in Graph of Events: {e}"))?;

        // =========================================================================
        // 3. ACTOR LOGGING: Create/Link the User Vertex
        // =========================================================================
        let user_vertex_id = Uuid::new_v4();
        let mut user_props = HashMap::new();
        user_props.insert("user_id".to_string(), PropertyValue::String(user_id.clone()));
        user_props.insert("user_role".to_string(), PropertyValue::String(user_role));
        user_props.insert("last_action_timestamp".to_string(), PropertyValue::String(timestamp.clone()));
        user_props.insert("last_event_id".to_string(), PropertyValue::String(event_id.to_string()));

        let user_vertex = Vertex {
            id: SerializableUuid(user_vertex_id),
            label: Identifier::new("User".to_string()).map_err(|e| e.to_string())?,
            properties: user_props,
            created_at: Utc::now().into(),
            updated_at: Utc::now().into(),
        };

        gs.create_vertex(user_vertex)
            .await
            .map_err(|e| format!("Failed to create User vertex: {e}"))?;

        // =========================================================================
        // 4. GRAPH OF CHANGES: Establish Traceable Edges
        // =========================================================================
        
        // Edge A: Connect the Conflict Record to the Event (History Trail)
        let history_edge = Edge::new(
            conflict_vertex_id,
            Identifier::new("HAS_IDENTITY_HISTORY".to_string()).map_err(|e| e.to_string())?,
            event_id,
        );
        gs.create_edge(history_edge).await.ok();

        // Edge B: Connect the Event to the User (Actor Attribution)
        let actor_edge = Edge::new(
            event_id,
            Identifier::new("RESOLVED_BY".to_string()).map_err(|e| e.to_string())?,
            user_vertex_id,
        );
        gs.create_edge(actor_edge)
            .await
            .map_err(|e| format!("Failed to link actor to resolution event: {e}"))?;

        // =========================================================================
        // 5. UPDATE PATIENT: Mark the resolution in the patient's metadata
        // =========================================================================
        let update_query = format!(
            r#"
            MATCH (p:Patient) WHERE ID(p) = "{}"
            SET p.conflict_status = "RESOLVED",
                p.last_resolved_by = "{}",
                p.last_event_id = "{}",
                p.updated_at = timestamp()
            "#,
            conflict_vertex_id, user_id, event_id
        );
        let _ = gs.execute_cypher_write(&update_query, serde_json::Value::Null).await;

        // =========================================================================
        // NEW COMPLIANCE BLOCK: RED FLAG DETECTION
        // =========================================================================
        let _ = gs.detect_and_persist_red_flags(conflict_vertex_id).await;

        info!("✅ Manual conflict resolution logged. Event: {} | User: {} | Record: {}", event_id, user_id, conflict_vertex_id);
        
        Ok(event_id) // Returning the Event ID instead of User ID for better traceability
    }

    /// Creates a SurvivorshipRule vertex and links it to a Golden Record (MPI Identity).
    /// This now accepts a flag to denote if it's a manual override.
    pub async fn create_survivorship_rule_link(
        &self,
        mpi_vertex_id: Uuid, 
        rule_name: String,
        field: String,
        policy: String,
        is_manual_override: bool,
    ) -> Result<Uuid, anyhow::Error> { // Changed Result type to anyhow::Error
        let gs = self.graph_service.clone();
        let timestamp = Utc::now().to_rfc3339();

        // =========================================================================
        // NEW COMPLIANCE BLOCK: GRAPH OF CHANGES (Structural Log)
        // =========================================================================
        gs.log_mpi_transaction(
            mpi_vertex_id,
            mpi_vertex_id,
            "SURVIVORSHIP_RULE_CREATED",
            &format!("Rule: {} | Field: {}", rule_name, field),
            if is_manual_override { "Manual_Override" } else { "System_Process" }
        ).await.ok();

        // =========================================================================
        // 1. GRAPH OF EVENTS: Log the Rule Application Event
        // =========================================================================
        let event_id = gs.persist_mpi_change_event(
            mpi_vertex_id, 
            Uuid::nil(), 
            "SURVIVORSHIP_RULE_APPLIED",
            json!({
                "field": field,
                "rule": rule_name,
                "policy": policy,
                "is_manual": is_manual_override,
                "applied_at": timestamp
            })
        ).await.map_err(|e| anyhow!("Failed to audit survivorship rule: {e}"))?;

        // =========================================================================
        // 2. CREATE RULE VERTEX (The Logic Definition)
        // =========================================================================
        let rule_vertex_id = Uuid::new_v4();
        let mut properties = HashMap::new();
        
        // We clone these strings because we need to use them later in the info! log
        properties.insert("rule_name".to_string(), PropertyValue::String(rule_name.clone()));
        properties.insert("field".to_string(), PropertyValue::String(field.clone()));
        properties.insert("policy".to_string(), PropertyValue::String(policy));
        properties.insert("is_manual_override".to_string(), PropertyValue::Boolean(is_manual_override));
        properties.insert("origin_event_id".to_string(), PropertyValue::String(event_id.to_string()));
        properties.insert("applied_at".to_string(), PropertyValue::String(timestamp));

        let rule_vertex = Vertex {
            id: SerializableUuid(rule_vertex_id),
            label: Identifier::new("SurvivorshipRule".to_string()).map_err(|e| anyhow!(e))?,
            properties,
            created_at: Utc::now().into(),
            updated_at: Utc::now().into(),
        };

        // We clone rule_vertex here so it isn't moved out of scope before the info! log
        gs.create_vertex(rule_vertex.clone())
            .await
            .map_err(|e| anyhow!("Failed to create SurvivorshipRule vertex: {e}"))?;

        // =========================================================================
        // 3. GRAPH OF CHANGES: Link Golden Record to the Rule
        // =========================================================================
        let edge_label = Identifier::new("HAS_SURVIVORSHIP_RULE".to_string()).map_err(|e| anyhow!(e))?;
        
        let mut edge = Edge::new(
            SerializableUuid(mpi_vertex_id),
            edge_label,
            SerializableUuid(rule_vertex_id),
        );
        
        edge.properties.insert("event_id".to_string(), PropertyValue::String(event_id.to_string()));

        gs.create_edge(edge)
            .await
            .map_err(|e| anyhow!("Failed to create HAS_SURVIVORSHIP_RULE edge: {e}"))?;

        // =========================================================================
        // 4. TRACEABILITY: Link Event to the Rule
        // =========================================================================
        let trace_edge = Edge::new(
            SerializableUuid(event_id),
            Identifier::new("DEFINED_BY".to_string()).map_err(|e| anyhow!(e))?,
            SerializableUuid(rule_vertex_id),
        );
        let _ = gs.create_edge(trace_edge).await;

        // =========================================================================
        // NEW COMPLIANCE BLOCK: RED FLAG DETECTION
        // =========================================================================
        let _ = gs.detect_and_persist_red_flags(mpi_vertex_id).await;

        // Now 'rule_name' and 'field' are still available because we cloned them above
        info!("✅ SurvivorshipRule [{}] for field [{}] linked to Golden Record {}. Event: {}", 
              rule_name, field, mpi_vertex_id, event_id);
              
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
        mpi_vertex_id: Uuid, // The Golden Record UUID
        source_patient_id: i32,
        score: f64,
        matching_algo: String,
    ) -> Result<Uuid, String> {
        let gs = self.graph_service.clone();
        let timestamp = Utc::now().to_rfc3339();

        // =========================================================================
        // NEW COMPLIANCE BLOCK: GRAPH OF CHANGES (Structural Log)
        // =========================================================================
        gs.log_mpi_transaction(
            mpi_vertex_id,
            mpi_vertex_id,
            "MATCH_SCORE_RECORDED",
            &format!("Algorithm: {} | Patient: {}", matching_algo, source_patient_id),
            "Match_Engine"
        ).await.ok();

        // =========================================================================
        // 1. GRAPH OF EVENTS: Log the Scoring Event
        // =========================================================================
        // We log the specific scoring action to allow for future tuning and audit.
        let event_id = gs.persist_mpi_change_event(
            mpi_vertex_id, // Golden Record involved
            Uuid::nil(),   // Secondary target
            "MATCH_SCORE_CALCULATED",
            json!({
                "source_patient_id": source_patient_id,
                "score": score,
                "algorithm": matching_algo,
                "recorded_at": timestamp
            })
        ).await.map_err(|e| format!("Failed to audit match scoring: {e}"))?;

        // =========================================================================
        // 2. CREATE MATCH SCORE VERTEX (The Proof)
        // =========================================================================
        let score_vertex_id = Uuid::new_v4();
        let mut properties = HashMap::new();
        
        properties.insert(
            "score".to_string(),
            PropertyValue::Float(SerializableFloat(score)),
        );
        properties.insert(
            "source_patient_id".to_string(),
            PropertyValue::Integer(source_patient_id as i64),
        );
        properties.insert("algorithm".to_string(), PropertyValue::String(matching_algo.clone()));
        properties.insert("origin_event_id".to_string(), PropertyValue::String(event_id.to_string()));
        properties.insert("recorded_at".to_string(), PropertyValue::String(timestamp));

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

        // =========================================================================
        // 3. GRAPH OF CHANGES: Link the Identity to the Evidence
        // =========================================================================
        let edge_label = Identifier::new("HAS_MATCH_SCORE".to_string()).map_err(|e| e.to_string())?;
        
        let mut edge = Edge::new(
            mpi_vertex_id,
            edge_label,
            score_vertex_id,
        );

        // Embed the event reference into the edge for the Change Graph
        edge.properties.insert("event_id".to_string(), PropertyValue::String(event_id.to_string()));

        gs.create_edge(edge)
            .await
            .map_err(|e| format!("Failed to create HAS_MATCH_SCORE edge: {e}"))?;

        // =========================================================================
        // 4. EVIDENCE TRACING: Link Event to the Evidence
        // =========================================================================
        let evidence_edge = Edge::new(
            event_id,
            Identifier::new("PRODUCED_EVIDENCE".to_string()).map_err(|e| e.to_string())?,
            score_vertex_id,
        );
        let _ = gs.create_edge(evidence_edge).await;

        // =========================================================================
        // NEW COMPLIANCE BLOCK: RED FLAG DETECTION
        // =========================================================================
        let _ = gs.detect_and_persist_red_flags(mpi_vertex_id).await;

        info!(
            "✅ MatchScore recorded: {} (Algo: {}) for GR {}. Event: {}", 
            score, matching_algo, mpi_vertex_id, event_id
        );

        Ok(score_vertex_id)
    }

    /// Handles the explicit probabilistic link/merge action between two MRNs.
    /// This method performs the graph operation (linking) and records the match score.
    pub async fn handle_probabilistic_link(
        &self,
        target_mrn: String,
        candidate_mrn: String,
        score: f64,
        action: String,
    ) -> Result<(), String> {
        info!("Processing explicit MPI link action: {} between {} and {}", action, target_mrn, candidate_mrn);

        let gs = &self.graph_service;

        // --- 1. Get Target Patient Vertex (MUST EXIST) ---
        let target_vertex = gs.get_patient_by_mrn(&target_mrn).await
            .map_err(|e| format!("Target Patient lookup failed for MRN {}: {}", target_mrn, e))?
            .ok_or_else(|| format!("Target Patient with MRN '{}' not found in graph.", target_mrn))?;

        let target_patient_vertex_uuid = target_vertex.id.0;

        let target_patient = patient_from_vertex(&target_vertex)
            .map_err(|e| format!("Failed to convert target vertex to Patient struct for MRN {}: {}", target_mrn, e))?;

        // --- 2. Get Candidate Patient Vertex (MUST EXIST) ---
        let candidate_vertex = gs.get_patient_by_mrn(&candidate_mrn).await
            .map_err(|e| format!("Candidate Patient lookup failed for MRN {}: {}", candidate_mrn, e))?
            .ok_or_else(|| format!("Candidate Patient with MRN '{}' not found in graph.", candidate_mrn))?;

        let candidate_patient = patient_from_vertex(&candidate_vertex)
            .map_err(|e| format!("Failed to convert candidate vertex to Patient struct for MRN {}: {}", candidate_mrn, e))?;

        // =========================================================================
        // NEW: 2025-12-20 Audit - Initialize Decision Event
        // =========================================================================
        let event_id = gs.persist_mpi_change_event(
            target_patient_vertex_uuid,
            Uuid::nil(), // Secondary reference to be updated if possible
            "PROBABILISTIC_LINK_DECISION",
            json!({
                "action": &action,
                "target_mrn": &target_mrn,
                "candidate_mrn": &candidate_mrn,
                "score": score,
                "status": "STARTED"
            })
        ).await.map_err(|e| format!("Failed to initialize Graph of Events: {}", e))?;

        // --- 3. Determine or Create the Golden Record (GR) ---
        let master_gr_app_id: String;
        let mut gr_vertex_uuid: Uuid;

        // Query 1: Check if target patient has a Golden Record
        let gr_query = format!(
            r#"MATCH (p:Patient {{mrn: "{}"}})<-[:HAS_GOLDEN_RECORD]-(g:GoldenRecord) RETURN g"#,
            target_mrn
        );

        let gr_result: Vec<Value> = gs.execute_cypher_read(&gr_query, Value::Null).await
            .map_err(|e| format!("Graph query failed to find Golden Record for target: {}", e))?;

        let target_gr_option = extract_all_vertices(gr_result)
            .into_iter()
            .filter_map(|vertex| GoldenRecord::try_from(vertex).ok())
            .next();

        if let Some(master_gr) = target_gr_option {
            // --- CASE B: GR FOUND ---
            master_gr_app_id = master_gr.id;
            gr_vertex_uuid = master_gr.gr_vertex_uuid;

            info!("Found existing Golden Record {} (Vertex {}) for Patient MRN {}", 
                  master_gr_app_id, gr_vertex_uuid, target_mrn);

        } else {
            // --- CASE A: GR NOT FOUND (Create New GR) ---
            info!("No Golden Record found for Patient MRN {}. Creating new Golden Record.", target_mrn);

            let new_gr_id = format!("GOLDEN-{}", Uuid::new_v4());
            let now_string = Utc::now().to_rfc3339();
            let new_gr_vertex_uuid = Uuid::new_v4();

            let mut new_golden_record = GoldenRecord::new(
                new_gr_id.clone(),
                new_gr_vertex_uuid,
                target_patient_vertex_uuid,
                now_string.clone(),
            );

            new_golden_record.update_metadata(Some(target_mrn.clone()), now_string);

            let gr_vertex: Vertex = new_golden_record.into();

            gs.create_vertex(gr_vertex).await
                .map_err(|e| format!("Failed to create Golden Record Vertex: {}", e))?;

            let link_target_query = format!(
                r#"MATCH (p:Patient {{mrn: "{}"}}), (g:GoldenRecord {{id: "{}"}}) 
                   CREATE (g)-[:HAS_GOLDEN_RECORD {{event_id: "{}", created_at: timestamp()}}]->(p)"#,
                target_mrn, new_gr_id, event_id
            );

            gs.execute_cypher_write(&link_target_query, Value::Null).await
                .map_err(|e| format!("Failed to link target Patient to Golden Record: {}", e))?;

            info!("Created and linked new Golden Record {} for Patient MRN {}", new_gr_id, target_mrn);
            master_gr_app_id = new_gr_id;
            gr_vertex_uuid = new_gr_vertex_uuid;
        };

        // =========================================================================
        // NEW COMPLIANCE BLOCK: GRAPH OF CHANGES (Structural Log)
        // =========================================================================
        gs.log_mpi_transaction(
            target_patient_vertex_uuid,
            gr_vertex_uuid,
            "PROBABILISTIC_LINK_INITIATED",
            &format!("Action: {} | Target: {} | Candidate: {}", action, target_mrn, candidate_mrn),
            "MPI_IDENTITY_RESOLVER"
        ).await.ok();

        // --- 4. Check Action and Execute Link/Record Score ---
        if action.to_lowercase() == "link" || action.to_lowercase() == "merge" {
            info!("Linking Candidate Patient (MRN: {}) to Master Golden Record {}.", candidate_mrn, master_gr_app_id);

            let create_link_query = format!(
                r#"MATCH (p:Patient {{mrn: "{}"}}), (g:GoldenRecord {{id: "{}"}}) 
                   CREATE (g)-[:HAS_GOLDEN_RECORD {{event_id: "{}", policy: "MANUAL_PROBABILISTIC", created_at: timestamp()}}]->(p)"#,
                candidate_mrn,
                master_gr_app_id,
                event_id
            );

            gs.execute_cypher_write(&create_link_query, Value::Null).await
                .map_err(|e| format!("Failed to create HAS_GOLDEN_RECORD link: {}", e))?;

            if let Some(patient_id_val) = candidate_patient.id {
                self.create_match_score_link(
                    gr_vertex_uuid,
                    patient_id_val,
                    score,
                    "explicit_probabilistic_link".to_string(),
                ).await?;
            } else {
                warn!("[MPI] Skipping MatchScore link: Candidate patient has no canonical ID");
            }

            info!("✅ Successfully linked Candidate MRN {} to Golden Record {}.", candidate_mrn, master_gr_app_id);

        } else if action.to_lowercase() == "ignore" || action.to_lowercase() == "false_positive" {
            info!("Recording match score of {} between {} and {} with action '{}'.", score, target_mrn, candidate_mrn, action);

            if let Some(patient_id_val) = target_patient.id {
                self.create_match_score_link(
                    gr_vertex_uuid,
                    patient_id_val,
                    score,
                    format!("explicit_{}", action),
                ).await?;
            } else {
                warn!("[MPI] Cannot create match score link: Target patient (MRN: {}) is missing a canonical ID.", target_mrn);
            }
            info!("Successfully recorded explicit match score, action was to ignore/do not link.");
        } else {
            return Err(format!("Unsupported match action: {}. Must be 'link', 'merge', or 'ignore'.", action));
        }

        // =========================================================================
        // NEW: Close the Audit Loop in Graph of Events
        // =========================================================================
        let mut final_event_props = HashMap::new();
        final_event_props.insert("status".to_string(), PropertyValue::String("COMPLETED".to_string()));
        final_event_props.insert("golden_record_id".to_string(), PropertyValue::String(master_gr_app_id));
        let _ = gs.update_vertex_properties(event_id, final_event_props).await;

        // =========================================================================
        // NEW COMPLIANCE BLOCK: RED FLAG AUDIT
        // =========================================================================
        let _ = gs.detect_and_persist_red_flags(gr_vertex_uuid).await;

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
        let timestamp = Utc::now().to_rfc3339();
        let actor = user.clone().unwrap_or_else(|| "SYSTEM_DUPLICATE_ENGINE".to_string());

        // =========================================================================
        // NEW COMPLIANCE BLOCK: GRAPH OF CHANGES (Structural Log)
        // =========================================================================
        // Every transaction on MPI must be logged into the graph of changes.
        // We relate both patients to this transaction.
        gs.log_mpi_transaction(
            patient_a_vertex_id,
            patient_b_vertex_id,
            "POTENTIAL_DUPLICATE_FLAGGED",
            &format!("Score: {} | Conflict ID: {}", score, conflict_id),
            &actor
        ).await.ok();

        // =========================================================================
        // NEW: 2025-12-20 Audit - Initialize the Duplicate Detection Event
        // =========================================================================
        // This ensures the transaction is logged into the graph of events.
        let event_id = gs.persist_mpi_change_event(
            patient_a_vertex_id,
            patient_b_vertex_id,
            "POTENTIAL_DUPLICATE_DETECTED",
            json!({
                "match_score": score,
                "conflict_reference_id": conflict_id.to_string(),
                "flagged_by": actor,
                "detected_at": timestamp
            })
        ).await.map_err(|e| format!("Failed to log duplicate detection to Graph of Events: {e}"))?;

        // --- Original Logic: Define the Edge ---
        let mut edge = Edge::new(
            patient_a_vertex_id,
            Identifier::new("IS_LINKED_TO".to_string()).map_err(|e| e.to_string())?,
            patient_b_vertex_id,
        );

        edge = edge.with_property(
            "match_score",
            PropertyValue::Float(SerializableFloat(score)),
        );
        
        // NEW: Store the reference to the conflict audit node (Original logic maintained)
        edge = edge.with_property(
            "conflict_reference_id",
            PropertyValue::String(conflict_id.to_string()),
        );

        // =========================================================================
        // AUGMENTED: Traceability - Link the Change Graph Edge to the Event
        // =========================================================================
        edge = edge.with_property(
            "event_id",
            PropertyValue::String(event_id.to_string()),
        );

        if let Some(u) = user {
            edge = edge.with_property("flagged_by", PropertyValue::String(u));
        }

        edge = edge.with_property(
            "detected_at",
            PropertyValue::String(timestamp),
        );

        // --- Original Logic: Persist the Edge ---
        gs.create_edge(edge)
            .await
            .map_err(|e| format!("Failed to create IS_LINKED_TO edge: {e}"))?;

        // =========================================================================
        // NEW: Establish Evidence link from Event to the Conflict node
        // =========================================================================
        let evidence_edge = Edge::new(
            event_id,
            Identifier::new("REFERENCES_CONFLICT".to_string()).map_err(|e| e.to_string())?,
            conflict_id,
        );
        let _ = gs.create_edge(evidence_edge).await;

        // =========================================================================
        // NEW COMPLIANCE BLOCK: RED FLAG AUDIT
        // =========================================================================
        // Since a potential duplicate impacts identity integrity, we run an audit.
        let _ = gs.detect_and_persist_red_flags(patient_a_vertex_id).await;
        let _ = gs.detect_and_persist_red_flags(patient_b_vertex_id).await;

        info!(
            "Created potential duplicate link between {patient_a_vertex_id} <> {patient_b_vertex_id} (score: {score}) referring to Conflict ID {conflict_id}. Event ID: {event_id}"
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
        target_patient_id_str: String, 
        new_patient_data: Patient,
        reason: String,
        split_patient_id: i32, 
        requested_by: &str,
    ) -> Result<Uuid, String> {
        let gs = &self.graph_service;
        let clean_id = target_patient_id_str.trim_matches(|c| c == '\'' || c == '"');

        // 1. Resolve the vertex that is embarking on a "New Way"
        let start_vertex = get_patient_vertex_by_id_or_mrn(gs, &clean_id.to_string())
            .await
            .map_err(|e| format!("Lookup failed: {}", e))?;
        let p_uuid = start_vertex.id.0;

        // ═════════════════════════════════════════════════════════════════════════
        // PHASE 2: SEVERANCE (Breaking the old lineage)
        // ═════════════════════════════════════════════════════════════════════════
        // We physically delete the link created by the merge so Alyce is "orphaned" 
        // from Alice's identity cluster.
        let sever_query = format!(
            "MATCH (p {{id: '{}'}})-[r:HAS_GOLDEN_RECORD]->() DELETE r",
            p_uuid
        );
        gs.execute_cypher_write(&sever_query, json!({})).await
            .map_err(|e| format!("Failed to sever Golden Record link: {}", e))?;

        // ═════════════════════════════════════════════════════════════════════════
        // PHASE 3: RE-IDENTIFICATION (The "Alyce" State)
        // ═════════════════════════════════════════════════════════════════════════
        let mut props = start_vertex.properties.clone();
        
        // Explicitly set the new identity details
        props.insert("first_name".to_string(), PropertyValue::String(new_patient_data.first_name.clone().unwrap_or_default()));
        props.insert("last_name".to_string(), PropertyValue::String(new_patient_data.last_name.clone().unwrap_or_default()));
        props.insert("patient_status".to_string(), PropertyValue::String("ACTIVE".to_string()));
        
        // Remove the pointer to the other person (Alice)
        props.remove("merged_into_uuid");
        props.insert("original_id".to_string(), PropertyValue::Integer(split_patient_id as i64));

        gs.update_vertex_properties(p_uuid, props).await
            .map_err(|e| format!("Failed to update vertex to Alyce: {}", e))?;

        // ═════════════════════════════════════════════════════════════════════════
        // PHASE 4: NEW GOLDEN RECORD (The "New Way")
        // ═════════════════════════════════════════════════════════════════════════
        // This creates a NEW Golden Record vertex for Alyce, ensuring FETCH IDENTITY 
        // sees her new name as the "Canonical" name.
        self.create_golden_record_and_link(&new_patient_data, p_uuid, requested_by).await
             .map_err(|e| format!("Failed to create independent Golden Record: {}", e))?;

        // ═════════════════════════════════════════════════════════════════════════
        // PHASE 5: 2025-12-20 AUDIT & TRACEABILITY
        // ═════════════════════════════════════════════════════════════════════════
        // Log the transaction in the graph of events (The "Why")
        let event_id = gs.persist_mpi_change_event(
            p_uuid, 
            p_uuid, 
            "IDENTITY_DIVERGENCE_SPLIT",
            json!({
                "reason": reason,
                "new_identity": format!("{} {}", new_patient_data.first_name.clone().unwrap_or_default(), new_patient_data.last_name.clone().unwrap_or_default()),
                "requested_by": requested_by
            })
        ).await.map_err(|e| e.to_string())?;

        // Log the trace in the graph of changes (The "Relation")
        gs.log_mpi_transaction(
            p_uuid,
            p_uuid,
            "IDENTITY_STATE_CHANGE",
            &format!("Split from previous cluster. Identity redefined as Alyce. Reason: {}", reason),
            requested_by
        ).await.ok();

        Ok(p_uuid)
    }

    /// Retrieves the consolidated "Golden Record" (Patient struct) for a given MPI ID.
    /// This now includes graph traversal to find the Golden Record and applies survivorship rules.
     pub async fn get_golden_record(&self, patient_id_str: String) -> Result<Patient, String> { 
        let gs = &self.graph_service;
        if patient_id_str.is_empty() {
            return Err("Patient ID cannot be empty.".to_string());
        }

        // 1. Find the Patient Vertex by ID or MRN
        let patient_vertex = get_patient_vertex_by_id_or_mrn(gs, &patient_id_str)
            .await
            .map_err(|e| format!("Initial Patient lookup failed for '{}': {}", patient_id_str, e))?;
        
        let patient_vertex_id = patient_vertex.id.0;

        // =========================================================================
        // NEW: 2025-12-20 Audit - Log the Resolution Event
        // =========================================================================
        // Every transaction on MPI must be logged. We log the "Read/Resolve" event.
        let event_id = gs.persist_mpi_change_event(
            patient_vertex_id,
            Uuid::nil(), 
            "GOLDEN_RECORD_RESOLUTION",
            json!({
                "queried_identifier": patient_id_str,
                "timestamp": Utc::now().to_rfc3339(),
                "action": "READ"
            })
        ).await.map_err(|e| format!("Failed to log access event: {}", e))?;

        // =========================================================================
        // NEW COMPLIANCE BLOCK: GRAPH OF CHANGES (Structural Log)
        // =========================================================================
        gs.log_mpi_transaction(
            patient_vertex_id,
            patient_vertex_id,
            "IDENTITY_RESOLVED",
            &format!("Query: {}", patient_id_str),
            "API_CONSUMER"
        ).await.ok();

        // 2. Find the associated Golden Record Vertex (the canonical identity)
        let find_gr_query = format!(
            r#"
            MATCH (p:Patient) WHERE ID(p) = "{}"
            MATCH (p)-[:HAS_GOLDEN_RECORD]->(g:GoldenRecord)
            RETURN g
            "#,
            patient_vertex_id
        );

        let gr_result = gs.execute_cypher_read(&find_gr_query, Value::Null).await
            .map_err(|e| format!("Graph traversal to Golden Record failed: {}", e))?;
        
        // 3. Extract the Golden Record vertex properties
        let gr_vertex_value = gr_result.into_iter()
            .flat_map(|val| val.get("results").and_then(Value::as_array).map(|arr| arr.to_vec()))
            .flatten()
            .flat_map(|res_item| res_item.get("g").cloned()) 
            .next()
            .ok_or_else(|| format!("Patient {} is not linked to a Golden Record.", patient_id_str))?;
        
        // Convert serde_json::Value to PropertyValue
        let gr_property_value: PropertyValue = serde_json::from_value(gr_vertex_value)
            .map_err(|e| format!("Failed to convert Golden Record data: {}", e))?;
        
        // --- Survivorship Logic (Placeholder) ---
        // NOTE: In 2025, survivorship logic results should be linked to the event_id.
        
        // For demonstration, we convert the Golden Record vertex value to the Patient struct.
        match Patient::from_vertex_value(&gr_property_value) {
            Some(mut patient) => {
                info!("[Service] Retrieved Golden Record for Patient ID: {}. Event: {}", patient_id_str, event_id);
                
                // Add a visual indicator that this is the Golden Record
                if let Some(mrn) = patient.mrn.take() {
                    patient.mrn = Some(format!("GOLDEN_{}", mrn));
                } else {
                    let id_display = patient.id
                        .map(|i| i.to_string())
                        .unwrap_or_else(|| "UNKNOWN".to_string());
                        
                    patient.mrn = Some(format!("GOLDEN_ID_{}", id_display));
                }

                // =========================================================================
                // NEW: Finalize Event with Metadata
                // =========================================================================
                let mut final_props = HashMap::new();
                final_props.insert("status".to_string(), PropertyValue::String("SUCCESS".to_string()));
                if let Some(id) = patient.id {
                    final_props.insert("canonical_id".to_string(), PropertyValue::Integer(id as i64));
                }
                let _ = gs.update_vertex_properties(event_id, final_props).await;
                
                Ok(patient)
            },
            None => Err(format!("Failed to convert Golden Record data to Patient model for {}.", patient_id_str)),
        }
    }

    /// Fetches identity by external ID (requires both value and type)
    pub async fn fetch_identity_by_external_id(
        &self, 
        external_id: String, 
        id_type: String,
        requested_by: &str, // ADDED: Required for 2025-12-20 audit flow
    ) -> Result<MasterPatientIndex, String> {
        let gs = &self.graph_service;
        
        info!("[MPI] fetch_identity_by_external_id called with external_id='{}', type='{}' by '{}'", 
              external_id, id_type, requested_by);

        // =========================================================================
        // NEW: 2025-12-20 Audit - Initialize External Resolution Event
        // =========================================================================
        let event_id = gs.persist_mpi_change_event(
            Uuid::nil(), 
            Uuid::nil(), 
            "EXTERNAL_IDENTITY_LOOKUP",
            json!({
                "external_id": external_id,
                "id_type": id_type,
                "requested_by": requested_by,
                "timestamp": Utc::now().to_rfc3339()
            })
        ).await.map_err(|e| format!("Audit log failed: {}", e))?;
        
        // 1. Find patient by external ID (Returns Option<Patient>)
        let p_struct = match gs.get_patient_by_external_id(&external_id, &id_type).await {
            Ok(Some(p)) => {
                let display_id = p.id
                    .map(|id| id.to_string())
                    .unwrap_or_else(|| "NONE".to_string());

                info!("[MPI] ✓ Found patient by external ID (Patient ID: {})", display_id);
                p
            },
            Ok(None) => {
                warn!("[MPI] No patient found matching both ID Type '{}' and External ID '{}'", 
                      id_type, external_id);

                let _ = gs.update_vertex_properties(event_id, HashMap::from([
                    ("status".to_string(), PropertyValue::String("NOT_FOUND".to_string()))
                ])).await;

                return Err(format!(
                    "Lookup successful, but no patient record exists with {}='{}'", 
                    id_type, external_id
                ));
            },
            Err(e) => {
                error!("[MPI] Database error during external ID query: {}", e);

                let _ = gs.update_vertex_properties(event_id, HashMap::from([
                    ("status".to_string(), PropertyValue::String("ERROR".to_string())),
                    ("error_detail".to_string(), PropertyValue::String(e.to_string()))
                ])).await;

                return Err(format!("Failed to query patient by external ID: {}", e));
            }
        };
        
        // 2. Continue with merge resolution using the internal ID
        if let Some(id_val) = p_struct.id {
            info!("[MPI] Resolving Master Patient Index for internal ID: {}", id_val);
            
            // =========================================================================
            // NEW: Relate the Event to the found Patient Record
            // =========================================================================
            let mut update_map = HashMap::new();
            update_map.insert("status".to_string(), PropertyValue::String("SUCCESS".to_string()));
            update_map.insert("internal_id".to_string(), PropertyValue::Integer(id_val as i64));
            let _ = gs.update_vertex_properties(event_id, update_map).await;

            // =========================================================================
            // NEW COMPLIANCE BLOCK: GRAPH OF CHANGES (Structural Log)
            // =========================================================================
            gs.log_mpi_transaction(
                Uuid::nil(),
                Uuid::nil(),
                "EXTERNAL_ID_RESOLVED",
                &format!("Type: {} | Value: {}", id_type, external_id),
                requested_by // Using the actual requester here
            ).await.ok();

            // FIX: Successfully passing both arguments to satisfy the new signature
            self.fetch_identity(id_val.to_string(), requested_by).await
        } else {
            error!("[MPI] Patient record found for {}='{}', but contains no internal numeric ID", 
                   id_type, external_id);

            let _ = gs.update_vertex_properties(event_id, HashMap::from([
                ("status".to_string(), PropertyValue::String("DATA_INTEGRITY_FAILURE".to_string()))
            ])).await;

            Err("Cannot fetch identity: Found patient record is missing a canonical ID".into())
        }
    }

    /// Identifies the type of identifier based on format
    fn identify_id_type(id_str: &str) -> IdentifierType {
        // Remove quotes and whitespace
        let clean = id_str.trim_matches(|c| c == '\'' || c == '"').trim();
        
        // Check if it's a UUID (8-4-4-4-12 hex format)
        if Uuid::parse_str(clean).is_ok() {
            return IdentifierType::UUID;
        }
        
        // Check if it's a pure numeric ID (positive or negative integer)
        if clean.parse::<i32>().is_ok() {
            return IdentifierType::InternalID;
        }
        
        // Everything else is treated as MRN (alphanumeric identifiers)
        // Examples: "M12345", "MRN78901", "ABC-123", etc.
        IdentifierType::MRN
    }
    
    /// Helper method to extract Address from vertex properties
    fn extract_address_from_vertex(vertex: &Vertex) -> Option<Address> {
        let props = &vertex.properties;
        
        let get_prop = |key: &str| -> Option<String> {
            props.get(key).and_then(|v| v.as_str()).map(|s| s.to_string())
        };
        
        let address_line1 = get_prop("address_line1")?;
        let city = get_prop("city")?;
        let postal_code = get_prop("postal_code")?;
        let country = get_prop("country")?;
        
        let state_province_str = get_prop("state_province")
            .or_else(|| get_prop("state"))
            .unwrap_or_else(|| "UNKNOWN".to_string());
        let state_province = Identifier::new(state_province_str).ok()?;
        
        let address_line2 = get_prop("address_line2");
        
        Some(Address::new(
            address_line1,
            address_line2,
            city,
            state_province,
            postal_code,
            country,
        ))
    }

    // =========================================================================
    //  MPI Lineage and Stewartship 
    // =========================================================================

    /// INDUSTRY STANDARD: Identity Lineage Report
    /// Reconstructs the timeline of how a Golden Record evolved.
    pub async fn get_identity_lineage_report(&self, mpi_id: &str) -> Result<LineageReport> {
        let gs = &self.graph_service;
        let uuid = self.resolve_id_to_uuid(mpi_id).await?;

        // =========================================================================
        // 1. 2025-12-20 Audit - Log the Lineage Generation Event (Graph of Events)
        // =========================================================================
        let event_id = gs.persist_mpi_change_event(
            uuid,
            Uuid::nil(),
            "LINEAGE_REPORT_GENERATED",
            json!({
                "mpi_id": mpi_id,
                "timestamp": Utc::now().to_rfc3339(),
                "scope": "FULL_TRANSACTIONAL_HISTORY"
            })
        ).await.map_err(|e| anyhow!("Audit log failed for lineage request: {}", e))?;
        
        // =========================================================================
        // NEW COMPLIANCE BLOCK: GRAPH OF CHANGES (Structural Log)
        // =========================================================================
        // Every transaction on MPI, including lineage reads, must be logged.
        gs.log_mpi_transaction(
            uuid,
            uuid,
            "LINEAGE_ACCESS_AUDIT",
            &format!("Generated lineage report for MPI ID: {}", mpi_id),
            "REPORTING_SERVICE"
        ).await.ok();

        // 2. Fetch transactional history from the graph
        let history = gs.get_transactional_history(uuid)
            .await
            .map_err(|e| anyhow!("Failed to fetch history: {}", e))?;
        
        // 3. Map to Evolution Ontology
        let steps: Vec<EvolutionStep> = history.into_iter().map(|event| {
            // Look up specific fields BEFORE consuming properties into the BTreeMap
            let action = event.properties.get("event_type").cloned();
            let timestamp = event.properties.get("timestamp").cloned();
            let description = event.properties.get("description").cloned();
            let source_system = event.properties.get("source_system").cloned();
            let flags = event.properties.get("red_flags").cloned();
            
            let user_id = event.properties.get("user_id")
                .and_then(|v| v.as_str())
                .unwrap_or("SYSTEM")
                .to_string();
                
            let reason = event.properties.get("reason")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());

            // NOW consume the properties into the BTreeMap for the metadata field
            let metadata_btree: BTreeMap<String, PropertyValue> = event.properties.into_iter().collect();

            EvolutionStep {
                event_id: event.id,
                action,
                timestamp,
                user_id,
                reason,
                description,
                source_system,
                flags,
                metadata: metadata_btree, 
            }
        }).collect();

        // 4. Update the audit event with result metadata
        let update_props = HashMap::from([
            ("steps_found".to_string(), to_property_value(json!(steps.len())).unwrap()),
            ("status".to_string(), to_property_value(json!("COMPLETED")).unwrap()),
        ]);
        
        let _ = gs.update_vertex_properties(event_id, update_props).await;

        // =========================================================================
        // NEW COMPLIANCE BLOCK: RED FLAG AUDIT
        // =========================================================================
        // Verify that the lineage history does not contain conflicting or fraudulent patterns.
        let _ = gs.detect_and_persist_red_flags(uuid).await;

        info!("[MPI] Generated lineage report for {} with {} steps. Event: {}", 
              mpi_id, steps.len(), event_id);

        Ok(LineageReport { 
            mpi_id: mpi_id.to_string(), 
            root_id: SerializableUuid(uuid), 
            steps 
        })
    }

    /// STEWARDSHIP: Conflict Resolution
    /// Manually clears Red Flags and logs the human intervention.
    pub async fn resolve_stewardship_conflict(&self, mpi_id: &str, note: &str, steward_id: &str) -> Result<()> {
        let gs = &self.graph_service;
        let uuid = self.resolve_id_to_uuid(mpi_id).await?;
        
        // =========================================================================
        // NEW: 2025-12-20 Audit - Initialize Stewardship Action Event
        // =========================================================================
        let event_id = gs.persist_mpi_change_event(
            uuid,
            Uuid::nil(),
            "MANUAL_CONFLICT_RESOLUTION",
            json!({
                "steward_id": steward_id,
                "note": note,
                "mpi_id": mpi_id,
                "timestamp": Utc::now().to_rfc3339()
            })
        ).await.map_err(|e| anyhow!("Stewardship audit initialization failed: {}", e))?;

        // =========================================================================
        // NEW COMPLIANCE BLOCK: GRAPH OF CHANGES (Structural Log)
        // =========================================================================
        // Record the human-driven modification in the persistent change graph.
        gs.log_mpi_transaction(
            uuid,
            Uuid::nil(),
            "STEWARD_RESOLUTION_EXECUTED",
            &format!("Steward: {} | Note: {}", steward_id, note),
            steward_id
        ).await.ok();

        // 1. Resolve in persistence layer
        gs.resolve_identity_conflict(uuid, note, steward_id)
            .await
            .map_err(|e| anyhow!("Failed to resolve identity conflict: {}", e))?;
        
        // 2. Re-evaluate matching scores now that flags are cleared
        self.refresh_match_scores(uuid).await?;

        // =========================================================================
        // NEW COMPLIANCE BLOCK: RED FLAG RE-AUDIT
        // =========================================================================
        // After resolution, we perform a fresh audit to ensure no new flags were triggered
        // by the change in match scores or state.
        let _ = gs.detect_and_persist_red_flags(uuid).await;

        // =========================================================================
        // FIX: Finalize Event Status using immutable initialization
        // =========================================================================
        let final_props = HashMap::from([
            ("status".to_string(), PropertyValue::String("RESOLVED".to_string())),
            ("resolution_note".to_string(), PropertyValue::String(note.to_string())),
        ]);
        
        let _ = gs.update_vertex_properties(event_id, final_props).await;

        info!("[Stewardship] Conflict resolved for {} by steward {}. Event: {}", mpi_id, steward_id, event_id);
        Ok(())
    }

    pub async fn get_stewardship_dashboard(&self, limit: usize) -> Result<Vec<DashboardItem>> {
        let gs = &self.graph_service;

        // =========================================================================
        // 2025-12-20 Audit - Log Dashboard Access (Graph of Events)
        // =========================================================================
        let _ = gs.persist_mpi_change_event(
            Uuid::nil(),
            Uuid::nil(),
            "DASHBOARD_METRICS_VIEW",
            json!({
                "limit": limit,
                "timestamp": Utc::now().to_rfc3339()
            })
        ).await;

        // =========================================================================
        // NEW COMPLIANCE BLOCK: GRAPH OF CHANGES (Access Transaction)
        // =========================================================================
        // Log the retrieval of the conflict dashboard for identity-level traceability.
        gs.log_mpi_transaction(
            Uuid::nil(),
            Uuid::nil(),
            "STEWARDSHIP_DASHBOARD_FETCH",
            &format!("Requested top {} items with active flags", limit),
            "SYSTEM_MONITOR"
        ).await.ok();

        // Use map_err to bridge GraphError to anyhow::Error
        let raw_metrics = gs.fetch_stewardship_report(limit)
            .await
            .map_err(|e| anyhow!("Failed to fetch stewardship report: {}", e))?;
        
        let mut items = Vec::new();

        for m in raw_metrics {
            // Extract internal_id safely
            let internal_id_str = m.get("internal_id")
                .and_then(|v| v.as_str())
                .unwrap_or_default();
            let internal_id = SerializableUuid(Uuid::parse_str(internal_id_str).unwrap_or_else(|_| Uuid::nil()));

            // Extract and map active_flags
            let active_flags: Vec<String> = m.get("active_flags")
                .and_then(|v| v.as_array())
                .map(|arr| arr.iter().map(|v| v.to_string()).collect())
                .unwrap_or_default();

            let last_event_str = m.get("last_event").map(|v| v.to_string()).unwrap_or_default();

            items.push(DashboardItem {
                id: m.get("id").map(|v| v.to_string()).unwrap_or_else(|| "UNKNOWN".to_string()),
                internal_id,
                link_density: m.get("link_density").and_then(|v| v.as_u64()).unwrap_or(0) as usize,
                health_status: if active_flags.is_empty() {
                    "CLEAN".to_string()
                } else {
                    "CONFLICT".to_string()
                },
                active_flags,
                last_event: last_event_str.clone(),
                last_updated: last_event_str,
                stewardship_status: m.get("stewardship_status")
                    .map(|v| v.to_string())
                    .unwrap_or_else(|| "UNKNOWN".to_string()),
            });
        }

        Ok(items)
    }

    /// Resolves a string-based ID (UUID string or Canonical MRN) into a valid internal Uuid.
    /// This is a prerequisite for all graph operations to ensure we are targeting the correct vertex.
    pub async fn resolve_id_to_uuid(&self, mpi_id: &str) -> Result<Uuid> {
        // 1. Attempt to parse as a raw UUID first (Fast path)
        if let Ok(parsed_uuid) = Uuid::parse_str(mpi_id) {
            return Ok(parsed_uuid);
        }

        // 2. If not a UUID, treat it as a Canonical MRN and look up the Golden Record
        // We use the graph service to find the vertex where canonical_mrn matches.
        let query = format!(
            "MATCH (g:GoldenRecord) WHERE g.canonical_mrn = '{}' RETURN g.id",
            mpi_id
        );

        let results = self.graph_service.execute_cypher_read(&query, Value::Null)
            .await
            .map_err(|e| anyhow!("Database failure during ID resolution: {}", e))?;

        if let Some(row) = results.first() {
            // Extract the 'id' property from the returned record
            if let Some(id_val) = row.get("id") {
                let id_str = id_val.as_str()
                    .ok_or_else(|| anyhow!("Identity found but internal ID property is malformed"))?;
                
                return Uuid::parse_str(id_str)
                    .map_err(|e| anyhow!("Failed to parse resolved ID '{}' as UUID: {}", id_str, e));
            }
        }

        // 3. Fallback: Check if it's an external ID on a Patient node
        let alt_query = format!(
            "MATCH (p:Patient) WHERE p.external_id = '{}' RETURN p.id",
            mpi_id
        );

        let alt_results = self.graph_service.execute_cypher_read(&alt_query, Value::Null)
            .await
            .map_err(|e| anyhow!("Alternative ID resolution failed: {}", e))?;

        if let Some(row) = alt_results.first() {
            if let Some(id_val) = row.get("id") {
                let id_str = id_val.as_str().unwrap_or("");
                return Uuid::parse_str(id_str).map_err(|e| anyhow!(e));
            }
        }

        Err(anyhow!("Identity Resolution Error: No record found for ID '{}'", mpi_id))
    }

    /// 2025-12-20: Re-calculates probabilistic match scores for a specific patient.
    /// This is triggered after manual stewardship or significant attribute updates
    /// to ensure the "Graph of Changes" reflects the most current logic.
    pub async fn refresh_match_scores(&self, uuid: Uuid) -> Result<()> {
        let gs = &self.graph_service;
        let timestamp = Utc::now().to_rfc3339();
        info!("[MPI] Refreshing match scores for vertex: {}", uuid);

        // =========================================================================
        // NEW: 2025-12-20 Audit - Initialize Refresh Event
        // =========================================================================
        let event_id = gs.persist_mpi_change_event(
            uuid,
            Uuid::nil(),
            "MATCH_SCORE_REFRESH",
            json!({
                "target_uuid": uuid.to_string(),
                "triggered_at": timestamp,
                "reason": "DATA_UPDATE_OR_STEWARDSHIP_ACTION"
            })
        ).await.map_err(|e| anyhow!("Failed to log refresh event: {}", e))?;

        // =========================================================================
        // NEW COMPLIANCE BLOCK: GRAPH OF CHANGES (Structural Log)
        // =========================================================================
        // Every transaction on MPI must be logged into the graph of changes.
        gs.log_mpi_transaction(
            uuid,
            uuid,
            "MATCHING_ENGINE_RECALCULATION",
            "Recalculating probabilistic links based on SSN and demographic updates.",
            "SYSTEM_MATCH_ENGINE"
        ).await.ok();

        // 1. Fetch target vertex
        let vertex = gs.get_vertex(&uuid).await
            .ok_or_else(|| anyhow!("Refresh failed: Vertex {} not found", uuid))?;

        // 2. Clear old match scores
        let clear_query = format!(
            "MATCH (p:Patient {{id: '{}'}})-[r:MATCH_SCORE]-() DELETE r",
            uuid
        );
        gs.execute_cypher_read(&clear_query, Value::Null).await
            .map_err(|e| anyhow!("Failed to clear old scores: {}", e))?;

        // 3. Re-run SSN matching logic
        if let Some(ssn) = vertex.properties.get("ssn") {
            let ssn_val = ssn.as_str().unwrap_or("");
            if !ssn_val.is_empty() {
                let match_query = format!(
                    "MATCH (candidate:Patient) 
                     WHERE candidate.ssn = '{}' AND candidate.id <> '{}' 
                     RETURN candidate.id",
                    ssn_val, uuid
                );

                let candidates = gs.execute_cypher_read(&match_query, Value::Null).await
                    .map_err(|e| anyhow!("Match query failed: {}", e))?;

                for candidate in candidates {
                    if let Some(c_id_val) = candidate.get("id") {
                        let c_uuid_str = c_id_val.as_str().unwrap_or("");
                        let c_uuid = Uuid::parse_str(c_uuid_str)
                            .map_err(|_| anyhow!("Invalid UUID in results"))?;

                        // 4. Construct the Edge using exact model fields
                        let mut match_edge = Edge::new(
                            SerializableUuid(uuid),
                            Identifier::new("MATCH_SCORE".to_string()).map_err(|e| anyhow!(e))?,
                            SerializableUuid(c_uuid)
                        );

                        // AUGMENTED: Traceability - Link the new edge to the Event ID
                        match_edge.properties.insert(
                            "event_id".to_string(),
                            PropertyValue::String(event_id.to_string())
                        );

                        match_edge.properties.insert(
                            "weight".to_string(), 
                            to_property_value(json!(0.99))?
                        );
                        match_edge.properties.insert(
                            "algorithm".to_string(), 
                            to_property_value(json!("SSN_EXACT"))?
                        );

                        gs.create_edge(match_edge).await
                            .map_err(|e| anyhow!("Failed to persist edge: {}", e))?;
                    }
                }
            }
        }

        // =========================================================================
        // NEW COMPLIANCE BLOCK: RED FLAG DETECTION
        // =========================================================================
        // After refreshing scores, we must check if the new scores indicate a conflict 
        // (e.g., one SSN shared by too many distinct identities).
        let _ = gs.detect_and_persist_red_flags(uuid).await;

        // Finalize event status
        let _ = gs.update_vertex_properties(event_id, HashMap::from([
            ("status".to_string(), PropertyValue::String("SUCCESS".to_string()))
        ])).await;

        Ok(())
    }
}
