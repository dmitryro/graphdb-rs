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
pub fn parse_patient_from_cypher_result(value: Value) -> Result<Patient> {
    
    let mut map_value = value.as_object()
        .context("Cypher result value is not a JSON object (node properties).")?
        .clone();

    // 1. Correct and Convert Required Fields based on `Patient` struct definition.
    
    // id: i32 (Required)
    let patient_id = get_required_i32(&Value::Object(map_value.clone()), "id")?;
    map_value.insert("id".to_string(), Value::Number(serde_json::Number::from(patient_id)));

    // date_of_birth: DateTime<Utc> (Required)
    let dob_key = if map_value.contains_key("date_of_birth") { "date_of_birth" } else { "dob" };
    let date_of_birth = get_required_dob(&Value::Object(map_value.clone()), dob_key)?;
    map_value.insert("date_of_birth".to_string(), Value::String(date_of_birth.to_rfc3339()));
    map_value.remove("dob"); // Remove temporary/legacy field

    // created_at / updated_at: DateTime<Utc> (Required)
    let created_at = get_required_datetime(&Value::Object(map_value.clone()), "created_at")?;
    let updated_at = get_required_datetime(&Value::Object(map_value.clone()), "updated_at")?;
    map_value.insert("created_at".to_string(), Value::String(created_at.to_rfc3339()));
    map_value.insert("updated_at".to_string(), Value::String(updated_at.to_rfc3339()));
    
    // first_name, last_name, gender, patient_status: String (Required)
    let first_name_str = get_optional_string(&Value::Object(map_value.clone()), "first_name")
        .or_else(|| get_optional_string(&Value::Object(map_value.clone()), "name").map(|n| n.split_whitespace().next().unwrap_or("").to_string()))
        .unwrap_or_else(|| "".to_string());
    map_value.insert("first_name".to_string(), Value::String(first_name_str));

    let last_name_str = get_optional_string(&Value::Object(map_value.clone()), "last_name")
        .or_else(|| get_optional_string(&Value::Object(map_value.clone()), "name").map(|n| n.split_whitespace().skip(1).collect::<Vec<&str>>().join(" ")))
        .unwrap_or_else(|| "".to_string());
    map_value.insert("last_name".to_string(), Value::String(last_name_str));
    map_value.remove("name"); // Remove temporary/legacy field

    if !map_value.contains_key("gender") {
        map_value.insert("gender".to_string(), Value::String("UNKNOWN".to_string()));
    }
    if !map_value.contains_key("patient_status") {
        map_value.insert("patient_status".to_string(), Value::String("ACTIVE".to_string()));
    }

    // 2. Handle Address and Phone Number Aliases
    
    // address: Option<Address>
    let address_struct = get_optional_address(&Value::Object(map_value.clone()), "address");
    map_value.insert("address".to_string(), serde_json::to_value(address_struct).unwrap_or(Value::Null));
    
    // phone_home, phone_mobile, phone_work: Map CLI/temp 'phone' to 'phone_mobile'
    if let Some(temp_phone) = get_optional_string(&Value::Object(map_value.clone()), "phone") {
        if !map_value.contains_key("phone_mobile") {
             map_value.insert("phone_mobile".to_string(), Value::String(temp_phone));
        }
        map_value.remove("phone"); // Remove temporary field
    }
    
    // 3. Final Deserialization using serde_json::from_value
    let final_json_value = Value::Object(map_value); 
    
    // FIX: Clone the value before the move into from_value, and use the clone in the closure.
    let final_json_value_clone = final_json_value.clone(); 

    let patient: Patient = from_value(final_json_value) // final_json_value moved here
        .with_context(|| format!("Final Deserialization of Patient struct failed from: {}", final_json_value_clone))?; // clone used here

    Ok(patient)
}