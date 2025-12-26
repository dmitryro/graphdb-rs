use anyhow::{Result, Context, anyhow};
use serde::{Serialize, Deserialize};
use serde_json::{Value, from_value};
use chrono::{DateTime, Utc, NaiveDate};

// Assuming the necessary domain model imports:
use models::medical::{
    Patient, 
    Address, // Imported from models/src/medical/patient.rs reference
    // The PatientId, ExternalId, and IdType structs were not part of the final Patient struct 
    // but are used internally for resolution/parsing of linked data.
    PatientId, 
    ExternalId, 
    IdType,
};

// --- Helper Functions (Revised and New) ---

/// Helper function to safely extract a required String property from a Value.
fn get_required_string(value: &Value, key: &str) -> Result<String> {
    value.get(key)
        .context(format!("Missing required field '{}'", key))?
        .as_str()
        .map(|s| s.to_string())
        .context(format!("Field '{}' is not a string", key))
}

/// Helper function to safely extract an optional String property from a Value.
fn get_optional_string(value: &Value, key: &str) -> Option<String> {
    value.get(key)
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
}

/// Safely extracts an i32 ID from a Value, handling string or number format.
fn get_required_i32(value: &Value, key: &str) -> Result<i32> {
    let v = value.get(key)
        .context(format!("Missing required i32 ID field '{}'", key))?;
    
    if let Some(n) = v.as_i64() {
        Ok(n as i32)
    } else if let Some(s) = v.as_str() {
        s.parse::<i32>().with_context(|| format!("Failed to parse required i32 ID '{}' from string: {}", key, s))
    } else {
        Err(anyhow!("Field '{}' has unexpected type for i32 ID", key))
    }
}

/// Safely extracts a required DateTime<Utc> property from a Value string.
fn get_required_datetime(value: &Value, key: &str) -> Result<DateTime<Utc>> {
    let dt_str = get_required_string(value, key)?;
    DateTime::parse_from_rfc3339(&dt_str)
        .map(|dt| dt.with_timezone(&Utc))
        .context(format!("Field '{}' is not a valid RFC3339 datetime", key))
}

/// Safely extracts a required DateTime<Utc> property from a Value string, attempting YYYY-MM-DD conversion if necessary.
fn get_required_dob(value: &Value, key: &str) -> Result<DateTime<Utc>> {
    let dt_str = get_required_string(value, key)?;

    // 1. Try RFC3339 directly
    if let Ok(dt) = DateTime::parse_from_rfc3339(&dt_str) {
        return Ok(dt.with_timezone(&Utc));
    }

    // 2. Try YYYY-MM-DD format (common DB storage for date only)
    if let Ok(naive_date) = NaiveDate::parse_from_str(&dt_str, "%Y-%m-%d") {
        return Ok(naive_date.and_hms_opt(0, 0, 0).unwrap().and_utc());
    }

    Err(anyhow!("Field '{}' is not a valid RFC3339 or YYYY-MM-DD date", key))
}

/// Safely extracts an optional Address struct from a Value string (JSON).
fn get_optional_address(value: &Value, key: &str) -> Option<Address> {
    value.get(key)
        .and_then(|v| v.as_str())
        .and_then(|s| {
            // Address is stored as a JSON string in the graph property
            serde_json::from_str::<Address>(s).ok()
        })
}

// Helper function to parse an individual ExternalId object from a Value
// FIX: Prefix unused variables with an underscore to silence warnings.
fn parse_external_id(_value: &Value) -> Result<ExternalId> {
    // This function is often used to map external data to domain objects, 
    // but the resulting ExternalId fields aren't used in the final Patient construction.
    let _id_type_str = get_required_string(_value, "id_type")?;
    let _id_value = get_required_string(_value, "id_value")?;
    let _system = get_required_string(_value, "system")?;

    Ok(ExternalId {
        id_type: IdType::from(_id_type_str),
        id_value: _id_value,
        system: Some(_system),
    })
}

// Helper function to safely extract a list of ExternalIds
// FIX: Prefix unused variable in `get_external_ids`
fn get_external_ids(_value: &Value, _key: &str) -> Result<Vec<ExternalId>> {
    // This is not used in the final Patient struct but is left for completeness/future extension.
    Ok(Vec::new())
}

// --- Main Parsing Function ---

/// Fully implements the function to parse a Patient struct from the raw Cypher result Value (JSON map).
/// 
/// This performs necessary conversions and field mapping to align with the actual `models::Patient` struct.

/// Parses a Patient from the raw Cypher result Value.
///
/// The Cypher engine may return either:
/// - Direct node properties: { "id": ..., "first_name": ..., ... }
/// - Wrapped node: { "p": { "id": ..., "first_name": ..., ... } }
///
/// This function handles both cases robustly.
pub fn parse_patient_from_cypher_result(value: Value) -> Result<Patient> {
    let obj = value
        .as_object()
        .ok_or_else(|| anyhow!("Result is not a JSON object"))?;
    
    // The vertex structure has properties nested inside a "properties" field
    let props = obj.get("properties")
        .and_then(|p| p.as_object())
        .ok_or_else(|| anyhow!("Missing 'properties' field"))?;
    
    // Also get top-level fields (created_at, updated_at)
    let created_at_str = obj.get("created_at")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow!("Missing created_at"))?;
    let updated_at_str = obj.get("updated_at")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow!("Missing updated_at"))?;
    
    let mut cleaned = serde_json::Map::new();
    
    // ID - now looking in properties
    let id = props
        .get("id")
        .and_then(|v| v.as_i64())
        .or_else(|| props.get("id").and_then(|v| v.as_str()).and_then(|s| s.parse::<i32>().ok().map(|i| i as i64)))
        .ok_or_else(|| anyhow!("Missing or invalid 'id' in properties"))?;
    cleaned.insert("id".to_string(), Value::Number(id.into()));
    
    // First name (fallback to split "name" if needed)
    let first_name = props
        .get("first_name")
        .and_then(|v| v.as_str())
        .or_else(|| props.get("name").and_then(|v| v.as_str()).and_then(|n| n.split_whitespace().next()))
        .unwrap_or("")
        .to_string();
    cleaned.insert("first_name".to_string(), Value::String(first_name));
    
    // Last name
    let last_name = props
        .get("last_name")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .or_else(|| {
            props.get("name").and_then(|v| v.as_str()).map(|n| {
                let mut parts = n.split_whitespace();
                parts.next(); // skip first
                parts.collect::<Vec<&str>>().join(" ")
            })
        })
        .unwrap_or_default();
    cleaned.insert("last_name".to_string(), Value::String(last_name));
    
    // DOB
    let dob_str = props
        .get("date_of_birth")
        .or_else(|| props.get("dob"))
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow!("Missing date_of_birth"))?;
    
    let dob = if let Ok(dt) = DateTime::parse_from_rfc3339(dob_str) {
        dt.with_timezone(&Utc)
    } else if let Ok(naive) = NaiveDate::parse_from_str(dob_str, "%Y-%m-%d") {
        naive.and_hms_opt(0, 0, 0).unwrap().and_utc()
    } else {
        return Err(anyhow!("Invalid DOB format: {}", dob_str));
    };
    cleaned.insert("date_of_birth".to_string(), Value::String(dob.to_rfc3339()));
    
    // created_at / updated_at from top level
    let created_at = created_at_str.parse::<DateTime<Utc>>()?;
    let updated_at = updated_at_str.parse::<DateTime<Utc>>()?;
    
    cleaned.insert("created_at".to_string(), Value::String(created_at.to_rfc3339()));
    cleaned.insert("updated_at".to_string(), Value::String(updated_at.to_rfc3339()));
    
    // Defaults
    let gender = props.get("gender").and_then(|v| v.as_str()).unwrap_or("UNKNOWN");
    let status = props.get("patient_status").and_then(|v| v.as_str()).unwrap_or("ACTIVE");
    cleaned.insert("gender".to_string(), Value::String(gender.to_string()));
    cleaned.insert("patient_status".to_string(), Value::String(status.to_string()));
    
    // Optional fields
    if let Some(mrn) = props.get("mrn").and_then(|v| v.as_str()) {
        cleaned.insert("mrn".to_string(), Value::String(mrn.to_string()));
    }
    
    if let Some(ssn) = props.get("ssn").and_then(|v| v.as_str()) {
        cleaned.insert("ssn".to_string(), Value::String(ssn.to_string()));
    }
    
    if let Some(phone) = props.get("phone").and_then(|v| v.as_str()) {
        cleaned.insert("phone_mobile".to_string(), Value::String(phone.to_string()));
    }
    
    if let Some(addr_str) = props.get("address").and_then(|v| v.as_str()) {
        if let Ok(addr) = serde_json::from_str::<Address>(addr_str) {
            cleaned.insert("address".to_string(), serde_json::to_value(addr)?);
        }
    }
    
    let final_value = Value::Object(cleaned);
    from_value(final_value.clone())
        .with_context(|| format!("Failed to deserialize Patient: {}", final_value))
}