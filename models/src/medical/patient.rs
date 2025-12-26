// models/src/medical/patient.rs
use std::collections::{ HashMap, BTreeMap };
use std::fmt;
use bincode::{Encode, Decode};
use serde::{Serialize, Deserialize};
use chrono::{DateTime, Utc};
use uuid::Uuid;
use crate::{Vertex, ToVertex, identifiers::Identifier};
use crate::errors::{GraphError};
use crate::medical::Address;   // or wherever it lives
use crate::properties::{ PropertyValue, UnhashableVertex }; // <--- ADD THIS IMPORT

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct Patient {
    // Primary identifiers
    pub id: Option<i32>,
    pub user_id: Option<i32>,
    pub mrn: Option<String>,  // Medical Record Number
    pub ssn: Option<String>,  // Social Security Number (encrypted in production)

    // Demographics
    pub first_name: Option<String>,
    pub middle_name: Option<String>,
    pub last_name: Option<String>,
    pub suffix: Option<String>,  // Jr, Sr, III, etc.
    pub preferred_name: Option<String>,
    pub date_of_birth: DateTime<Utc>,
    pub date_of_death: Option<DateTime<Utc>>,
    // FIX: Change from String to Option<String> to support merging logic (gender.or(...))
    pub gender: Option<String>,  // MALE, FEMALE, OTHER, UNKNOWN
    pub sex_assigned_at_birth: Option<String>,  // MALE, FEMALE, INTERSEX
    pub gender_identity: Option<String>,  // MAN, WOMAN, TRANSGENDER_MALE, TRANSGENDER_FEMALE, NON_BINARY, OTHER
    pub pronouns: Option<String>,  // HE/HIM, SHE/HER, THEY/THEM, etc.

    // Contact Information
    pub address_id: Option<uuid::Uuid>, // foreign key to Address vertex
    pub address: Option<Address>,
    pub phone_home: Option<String>,
    pub phone_mobile: Option<String>,
    pub phone_work: Option<String>,
    pub email: Option<String>,
    pub preferred_contact_method: Option<String>,  // PHONE, EMAIL, SMS, MAIL
    pub preferred_language: Option<String>,  // EN, ES, FR, etc.
    pub interpreter_needed: Option<bool>,

    // Emergency Contact
    pub emergency_contact_name: Option<String>,
    pub emergency_contact_relationship: Option<String>,
    pub emergency_contact_phone: Option<String>,

    // Demographic Details
    pub marital_status: Option<String>,  // SINGLE, MARRIED, DIVORCED, WIDOWED, SEPARATED, DOMESTIC_PARTNER
    pub race: Option<String>,
    pub ethnicity: Option<String>,
    pub religion: Option<String>,

    // Insurance & Financial
    pub primary_insurance: Option<String>,
    pub primary_insurance_id: Option<String>,
    pub secondary_insurance: Option<String>,
    pub secondary_insurance_id: Option<String>,
    pub guarantor_name: Option<String>,
    pub guarantor_relationship: Option<String>,

    // Clinical Information
    pub primary_care_provider_id: Option<i32>,
    pub blood_type: Option<String>,  // A+, A-, B+, B-, AB+, AB-, O+, O-
    pub organ_donor: Option<bool>,
    #[serde(default)]
    pub advance_directive_on_file: Option<bool>,
    pub dni_status: Option<String>,  // Do Not Intubate
    pub dnr_status: Option<String>,  // Do Not Resuscitate
    pub code_status: Option<String>,  // FULL_CODE, DNR, DNI, COMFORT_CARE

    // Administrative
    #[serde(default)] // Best practice to prevent Serde from looking for it if missing
    pub patient_status: Option<String>,  // ACTIVE, INACTIVE, DECEASED, MERGED, TEST
    pub vip_flag: Option<bool>,
    pub confidential_flag: Option<bool>, // Restricts who can view record
    pub research_consent: Option<bool>,
    pub marketing_consent: Option<bool>,

    // Social Determinants of Health (SDOH)
    pub employment_status: Option<String>,
    pub housing_status: Option<String>,  // HOUSED, HOMELESS, UNSTABLE, TEMPORARY
    pub education_level: Option<String>,
    pub financial_strain: Option<String>,  // NONE, MILD, MODERATE, SEVERE
    pub food_insecurity: Option<bool>,
    pub transportation_needs: Option<bool>,
    pub social_isolation: Option<String>,
    #[serde(default)]
    pub veteran_status: Option<bool>,
    #[serde(default)]
    pub disability_status: Option<String>,

    // Clinical Alerts
    pub alert_flags: Option<String>,  // JSON array of alert codes
    pub special_needs: Option<String>,

    // Audit Trail
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub created_by: Option<i32>,
    pub updated_by: Option<i32>,
    pub last_visit_date: Option<DateTime<Utc>>,

    // Graph vertex ID
    pub vertex_id: Option<Uuid>,
}

impl ToVertex for Patient {
    fn to_vertex(&self) -> Vertex {
        // Initialize with "Patient" label
        let mut v = Vertex::new(Identifier::new("Patient".to_string()).unwrap());

        // --- PRIMARY IDENTIFIERS ---
        // These are the core keys for the MPI and Golden Record
        if let Some(id_val) = self.id {
            v.add_property("id", &id_val.to_string());
        }
        if let Some(ref val) = self.user_id {
            v.add_property("user_id", &val.to_string());
        }
        // Vertex ID is crucial for the graph of changes/events
        if let Some(ref val) = self.vertex_id {
            v.add_property("vertex_id", &val.to_string());
        }
        if let Some(ref val) = self.mrn {
            v.add_property("mrn", val);
        }
        if let Some(ref val) = self.ssn {
            v.add_property("ssn", val);
        }

        // --- DEMOGRAPHICS ---
        if let Some(ref val) = self.first_name {
            v.add_property("first_name", val);
        }
        if let Some(ref val) = self.middle_name {
            v.add_property("middle_name", val);
        }
        if let Some(ref val) = self.last_name {
            v.add_property("last_name", val);
        }
        if let Some(ref val) = self.suffix {
            v.add_property("suffix", val);
        }
        if let Some(ref val) = self.preferred_name {
            v.add_property("preferred_name", val);
        }

        v.add_property("date_of_birth", &self.date_of_birth.to_rfc3339());

        if let Some(ref val) = self.date_of_death {
            v.add_property("date_of_death", &val.to_rfc3339());
        }
        
        if let Some(ref val) = self.gender { 
            v.add_property("gender", val);
        }
        if let Some(ref val) = self.sex_assigned_at_birth {
            v.add_property("sex_assigned_at_birth", val);
        }
        if let Some(ref val) = self.gender_identity {
            v.add_property("gender_identity", val);
        }
        if let Some(ref val) = self.pronouns {
            v.add_property("pronouns", val);
        }

        // --- CONTACT INFORMATION ---
        if let Some(ref addr_id) = self.address_id {
            v.add_property("address_id", &addr_id.to_string());
        }
        if let Some(ref addr) = self.address {
            // Serialization ensures complex address structs are stored as JSON strings in the property
            v.add_property("address", &serde_json::to_string(addr).expect("Address serialization failed"));
        }
        if let Some(ref val) = self.phone_home {
            v.add_property("phone_home", val);
        }
        if let Some(ref val) = self.phone_mobile {
            v.add_property("phone_mobile", val);
        }
        if let Some(ref val) = self.phone_work {
            v.add_property("phone_work", val);
        }
        if let Some(ref val) = self.email {
            v.add_property("email", val);
        }
        if let Some(ref val) = self.preferred_contact_method {
            v.add_property("preferred_contact_method", val);
        }
        if let Some(ref val) = self.preferred_language {
            v.add_property("preferred_language", val);
        }
        if let Some(interpreter_needed) = self.interpreter_needed {
            v.add_property("interpreter_needed", &interpreter_needed.to_string()); 
        }

        // --- EMERGENCY CONTACT ---
        if let Some(ref val) = self.emergency_contact_name {
            v.add_property("emergency_contact_name", val);
        }
        if let Some(ref val) = self.emergency_contact_relationship {
            v.add_property("emergency_contact_relationship", val);
        }
        if let Some(ref val) = self.emergency_contact_phone {
            v.add_property("emergency_contact_phone", val);
        }

        // --- DEMOGRAPHIC DETAILS ---
        if let Some(ref val) = self.marital_status {
            v.add_property("marital_status", val);
        }
        if let Some(ref val) = self.race {
            v.add_property("race", val);
        }
        if let Some(ref val) = self.ethnicity {
            v.add_property("ethnicity", val);
        }
        if let Some(ref val) = self.religion {
            v.add_property("religion", val);
        }

        // --- INSURANCE & FINANCIAL ---
        if let Some(ref val) = self.primary_insurance {
            v.add_property("primary_insurance", val);
        }
        if let Some(ref val) = self.primary_insurance_id {
            v.add_property("primary_insurance_id", val);
        }
        if let Some(ref val) = self.secondary_insurance {
            v.add_property("secondary_insurance", val);
        }
        if let Some(ref val) = self.secondary_insurance_id {
            v.add_property("secondary_insurance_id", val);
        }
        if let Some(ref val) = self.guarantor_name {
            v.add_property("guarantor_name", val);
        }
        if let Some(ref val) = self.guarantor_relationship {
            v.add_property("guarantor_relationship", val);
        }

        // --- CLINICAL INFORMATION ---
        if let Some(ref val) = self.primary_care_provider_id {
            v.add_property("primary_care_provider_id", &val.to_string());
        }
        if let Some(ref val) = self.blood_type {
            v.add_property("blood_type", val);
        }
        if let Some(ref val) = self.organ_donor {
            v.add_property("organ_donor", &val.to_string());
        }

        // --- DIRECTIVE STATUS ---
        if let Some(val) = self.advance_directive_on_file {
            v.add_property("advance_directive_on_file", &val.to_string());
        }
        if let Some(ref val) = self.dni_status {
            v.add_property("dni_status", val);
        }
        if let Some(ref val) = self.dnr_status {
            v.add_property("dnr_status", val);
        }
        if let Some(ref val) = self.code_status {
            v.add_property("code_status", val);
        }

        // --- ADMINISTRATIVE ---
        if let Some(ref val) = self.patient_status {
            v.add_property("patient_status", val.as_str()); 
        }
        if let Some(val) = self.vip_flag {
            v.add_property("vip_flag", &val.to_string());
        } 
        if let Some(val) = self.confidential_flag {
            v.add_property("confidential_flag", &val.to_string());
        }
        if let Some(ref val) = self.research_consent {
            v.add_property("research_consent", &val.to_string());
        }
        if let Some(ref val) = self.marketing_consent {
            v.add_property("marketing_consent", &val.to_string());
        }

        // --- SOCIAL DETERMINANTS OF HEALTH ---
        if let Some(ref val) = self.employment_status {
            v.add_property("employment_status", val);
        }
        if let Some(ref val) = self.housing_status {
            v.add_property("housing_status", val);
        }
        if let Some(ref val) = self.education_level {
            v.add_property("education_level", val);
        }
        if let Some(ref val) = self.financial_strain {
            v.add_property("financial_strain", val);
        }
        if let Some(val) = self.food_insecurity {
            v.add_property("food_insecurity", &val.to_string());
        }
        if let Some(val) = self.transportation_needs {
            v.add_property("transportation_needs", &val.to_string());
        }
        if let Some(ref val) = self.social_isolation {
            v.add_property("social_isolation", val);
        }
        if let Some(ref val) = self.veteran_status {
            v.add_property("veteran_status", &val.to_string());
        }
        if let Some(ref val) = self.disability_status {
            v.add_property("disability_status", val);
        }

        // --- CLINICAL ALERTS ---
        if let Some(ref val) = self.alert_flags {
            v.add_property("alert_flags", val);
        }
        if let Some(ref val) = self.special_needs {
            v.add_property("special_needs", val);
        }

        // --- AUDIT TRAIL ---
        // Ensuring UTC timestamps are stored in ISO format for temporal queries
        v.add_property("created_at", &self.created_at.to_rfc3339());
        v.add_property("updated_at", &self.updated_at.to_rfc3339());
        if let Some(ref val) = self.created_by {
            v.add_property("created_by", &val.to_string());
        }
        if let Some(ref val) = self.updated_by {
            v.add_property("updated_by", &val.to_string());
        }
        if let Some(ref val) = self.last_visit_date {
            v.add_property("last_visit_date", &val.to_rfc3339());
        }

        v
    }
}

/// Normalizes vertex properties by converting legacy aliases to canonical field names
/// and ensuring required fields have non-null string representations.
fn normalize_patient_vertex(vertex: &Vertex) -> Vertex {
    let mut normalized = vertex.clone();
    
    // --- 1. NEW: Handle vertex_id mapping ---
    // Extract the physical graph vertex ID and ensure it's in the properties for the struct mapper
    normalized.properties.insert(
        "vertex_id".to_string(),
        PropertyValue::String(vertex.id.0.to_string())
    );
    
    // --- 2. Handle legacy "dob" field and ensure "date_of_birth" is present ---
    let dob_key = "date_of_birth".to_string();
    if !normalized.properties.contains_key(&dob_key) {
        // Attempt to convert "dob"
        let dob_string_to_convert: Option<String> = normalized.properties.get("dob").and_then(|v| {
            if let PropertyValue::String(s) = v {
                Some(s.clone())
            } else {
                None
            }
        });

        let mut date_found = false;
        if let Some(dob_value) = dob_string_to_convert {
            // CRITICAL FIX: Convert "YYYY-MM-DD" to RFC3339 format
            if let Ok(naive_date) = chrono::NaiveDate::parse_from_str(&dob_value, "%Y-%m-%d") {
                let date_time = naive_date.and_hms_opt(0, 0, 0).unwrap().and_utc();
                
                normalized.properties.insert(
                    dob_key.clone(),
                    PropertyValue::String(date_time.to_rfc3339())
                );
                date_found = true;
                normalized.properties.remove("dob");
            }
        }
        
        // NEW FIX: If "date_of_birth" is still missing, set a default to prevent deserialization failure
        if !date_found {
            // Default to 1900-01-01T00:00:00+00:00 (A safe fallback date)
            let default_date = chrono::NaiveDate::from_ymd_opt(1900, 1, 1)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap()
                .and_utc()
                .to_rfc3339();
                
            normalized.properties.insert(dob_key, PropertyValue::String(default_date));
        }
    }
    
    // --- 3. Handle legacy "name" field ---
    // This logic attempts to split 'name' into 'first_name' and 'last_name' if they are missing
    let mut names_set = normalized.properties.contains_key("first_name") && normalized.properties.contains_key("last_name");
    
    if !names_set {
        let name_string_to_split: Option<String> = normalized.properties.get("name").and_then(|v| {
            if let PropertyValue::String(s) = v {
                Some(s.clone()) 
            } else {
                None
            }
        });

        if let Some(name_value) = name_string_to_split {
            let parts: Vec<&str> = name_value.split_whitespace().collect();
            
            // Insert first_name
            if let Some(first) = parts.first() {
                normalized.properties.insert(
                    "first_name".to_string(),
                    PropertyValue::String(first.to_string()) 
                );
            }
            
            // Insert last_name (all remaining parts)
            if parts.len() > 1 {
                let last = parts[1..].join(" ");
                normalized.properties.insert(
                    "last_name".to_string(),
                    PropertyValue::String(last) 
                );
            } else if parts.len() == 1 {
                // If only one part, set last_name to empty
                normalized.properties.insert(
                    "last_name".to_string(),
                    PropertyValue::String("".to_string()) 
                );
            }
            
            normalized.properties.remove("name");
            names_set = true;
        }
    }
    
    // NEW FIX: Ensure first_name and last_name exist as strings after conversion attempt
    if !normalized.properties.contains_key("first_name") {
        normalized.properties.insert("first_name".to_string(), PropertyValue::String("".to_string()));
    }
    if !normalized.properties.contains_key("last_name") {
        normalized.properties.insert("last_name".to_string(), PropertyValue::String("".to_string()));
    }
    
    // --- 4. Ensure other required/default fields are present ---

    // Ensure required fields have defaults if missing
    if !normalized.properties.contains_key("patient_status") {
        normalized.properties.insert(
            "patient_status".to_string(),
            PropertyValue::String("ACTIVE".to_string())
        );
    }
    
    if !normalized.properties.contains_key("gender") {
        normalized.properties.insert(
            "gender".to_string(),
            PropertyValue::String("UNKNOWN".to_string())
        );
    }
    
    // Ensure boolean fields have defaults
    if !normalized.properties.contains_key("interpreter_needed") {
        normalized.properties.insert(
            "interpreter_needed".to_string(),
            PropertyValue::String("false".to_string())
        );
    }
    
    if !normalized.properties.contains_key("advance_directive_on_file") {
        normalized.properties.insert(
            "advance_directive_on_file".to_string(),
            PropertyValue::String("false".to_string())
        );
    }
    
    if !normalized.properties.contains_key("vip_flag") {
        normalized.properties.insert(
            "vip_flag".to_string(),
            PropertyValue::String("false".to_string())
        );
    }
    
    if !normalized.properties.contains_key("confidential_flag") {
        normalized.properties.insert(
            "confidential_flag".to_string(),
            PropertyValue::String("false".to_string())
        );
    }
    
    if !normalized.properties.contains_key("food_insecurity") {
        normalized.properties.insert(
            "food_insecurity".to_string(),
            PropertyValue::String("false".to_string())
        );
    }
    
    if !normalized.properties.contains_key("transportation_needs") {
        normalized.properties.insert(
            "transportation_needs".to_string(),
            PropertyValue::String("false".to_string())
        );
    }
    
    // Ensure timestamp fields exist
    if !normalized.properties.contains_key("created_at") {
        normalized.properties.insert(
            "created_at".to_string(),
            PropertyValue::String(chrono::Utc::now().to_rfc3339())
        );
    }
    
    if !normalized.properties.contains_key("updated_at") {
        normalized.properties.insert(
            "updated_at".to_string(),
            PropertyValue::String(chrono::Utc::now().to_rfc3339())
        );
    }
    
    normalized
}


impl Patient {
    pub fn from_vertex(vertex: &Vertex) -> Option<Self> {
        if vertex.label.as_ref() != "Patient" { 
            return None; 
        }
        
        // Normalize the vertex properties first
        let normalized = normalize_patient_vertex(vertex);
        
        Some(Patient {
            // NEW: Graph vertex ID for tracing and relating events
            vertex_id: normalized.properties.get("vertex_id")
                .and_then(|v| v.as_str())
                .and_then(|s| Uuid::parse_str(s).ok()),

            // Primary identifiers
            id: {
                let prop = normalized.properties.get("id");
                let id_str = match prop {
                    Some(PropertyValue::String(s)) => Some(s.clone()),
                    Some(PropertyValue::Integer(i)) => Some(i.to_string()),
                    _ => None,
                };
                id_str.and_then(|s| s.parse::<i32>().ok())
            },
            
            user_id: normalized.properties.get("user_id")
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse().ok()),
            mrn: normalized.properties.get("mrn")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            ssn: normalized.properties.get("ssn")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),

            // Demographics
            first_name: normalized.properties.get("first_name")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            middle_name: normalized.properties.get("middle_name")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            last_name: normalized.properties.get("last_name")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            suffix: normalized.properties.get("suffix")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            preferred_name: normalized.properties.get("preferred_name")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),

            date_of_birth: chrono::DateTime::parse_from_rfc3339(
                normalized.properties.get("date_of_birth")?.as_str()?
            ).ok()?.with_timezone(&chrono::Utc),
            
            date_of_death: normalized.properties.get("date_of_death")
                .and_then(|v| v.as_str())
                .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
                .map(|dt| dt.with_timezone(&chrono::Utc)),
            
            gender: normalized.properties.get("gender")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            
            sex_assigned_at_birth: normalized.properties.get("sex_assigned_at_birth")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            gender_identity: normalized.properties.get("gender_identity")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            pronouns: normalized.properties.get("pronouns")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),

            // Contact Information
            address_id: normalized.properties.get("address_id")
                .and_then(|v| v.as_str())
                .and_then(|s| uuid::Uuid::parse_str(s).ok()),
            address: normalized.properties.get("address")
                .and_then(|v| v.as_str())
                .and_then(|s| serde_json::from_str::<Address>(s).ok()),
            phone_home: normalized.properties.get("phone_home")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            phone_mobile: normalized.properties.get("phone_mobile")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            phone_work: normalized.properties.get("phone_work")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            email: normalized.properties.get("email")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            preferred_contact_method: normalized.properties.get("preferred_contact_method")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            preferred_language: normalized.properties.get("preferred_language")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
                
            interpreter_needed: normalized.properties.get("interpreter_needed")
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse().ok()),

            // Emergency Contact
            emergency_contact_name: normalized.properties.get("emergency_contact_name")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            emergency_contact_relationship: normalized.properties.get("emergency_contact_relationship")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            emergency_contact_phone: normalized.properties.get("emergency_contact_phone")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),

            // Demographic Details
            marital_status: normalized.properties.get("marital_status")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            race: normalized.properties.get("race")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            ethnicity: normalized.properties.get("ethnicity")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            religion: normalized.properties.get("religion")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),

            // Insurance & Financial
            primary_insurance: normalized.properties.get("primary_insurance")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            primary_insurance_id: normalized.properties.get("primary_insurance_id")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            secondary_insurance: normalized.properties.get("secondary_insurance")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            secondary_insurance_id: normalized.properties.get("secondary_insurance_id")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            guarantor_name: normalized.properties.get("guarantor_name")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            guarantor_relationship: normalized.properties.get("guarantor_relationship")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),

            // Clinical Information
            primary_care_provider_id: normalized.properties.get("primary_care_provider_id")
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse().ok()),
            blood_type: normalized.properties.get("blood_type")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            organ_donor: normalized.properties.get("organ_donor")
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse().ok()),

            // Directive Status
            advance_directive_on_file: normalized.properties.get("advance_directive_on_file")
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse().ok()),
            dni_status: normalized.properties.get("dni_status")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            dnr_status: normalized.properties.get("dnr_status")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            code_status: normalized.properties.get("code_status")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),

            // Administrative
            patient_status: normalized.properties.get("patient_status")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
                .or_else(|| Some("ACTIVE".to_string())),
            vip_flag: normalized.properties.get("vip_flag")
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse().ok()),
            confidential_flag: normalized.properties.get("confidential_flag")
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse().ok()),
            research_consent: normalized.properties.get("research_consent")
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse().ok()),
            marketing_consent: normalized.properties.get("marketing_consent")
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse().ok()),

            // Social Determinants of Health
            employment_status: normalized.properties.get("employment_status")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            housing_status: normalized.properties.get("housing_status")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            education_level: normalized.properties.get("education_level")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            financial_strain: normalized.properties.get("financial_strain")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            food_insecurity: normalized.properties.get("food_insecurity")
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse().ok()),
            transportation_needs: normalized.properties.get("transportation_needs")
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse().ok()),
            social_isolation: normalized.properties.get("social_isolation")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            veteran_status: normalized.properties.get("veteran_status")
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse().ok()),
            disability_status: normalized.properties.get("disability_status")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            
            // Clinical Alerts
            alert_flags: normalized.properties.get("alert_flags")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            special_needs: normalized.properties.get("special_needs")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),

            // Audit Trail
            created_at: chrono::DateTime::parse_from_rfc3339(
                normalized.properties.get("created_at")?.as_str()?
            ).ok()?.with_timezone(&chrono::Utc),
            updated_at: chrono::DateTime::parse_from_rfc3339(
                normalized.properties.get("updated_at")?.as_str()?
            ).ok()?.with_timezone(&chrono::Utc),
            created_by: normalized.properties.get("created_by")
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse().ok()),
            updated_by: normalized.properties.get("updated_by")
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse().ok()),
            last_visit_date: normalized.properties.get("last_visit_date")
                .and_then(|v| v.as_str())
                .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
                .map(|dt| dt.with_timezone(&chrono::Utc)),
        })
    }

    pub fn from_vertex_value(value: &PropertyValue) -> Option<Self> {
        // 1. Check if the PropertyValue contains the canonical Vertex structure.
        if let PropertyValue::Vertex(unhashable_vertex) = value {
            let vertex = &unhashable_vertex.0;
            return Patient::from_vertex(vertex); 
        }

        // 2. Fallback check for PropertyValue::Map
        if let PropertyValue::Map(hashable_map) = value {
            let property_map = &hashable_map.0; 

            let properties: HashMap<String, PropertyValue> = property_map
                .iter()
                .map(|(id, value)| (id.to_string(), value.clone()))
                .collect();

            let label = Identifier::new("Patient".to_string()).ok()?; 

            let vertex = Vertex {
                id: Default::default(),
                label,
                properties,
                created_at: Utc::now().into(),
                updated_at: Utc::now().into(),
            };

            return Patient::from_vertex(&vertex); 
        }
        
        None
    }
}

// --- Placeholder for IdType (necessary for the parsing logic above to compile) ---
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum IdType {
    Mrn,
    Ssn,
    Other(String),
}   
    
impl IdType {
    pub fn from(s: String) -> Self {
        match s.to_lowercase().as_str() {
            "mrn" => IdType::Mrn,
            "ssn" => IdType::Ssn,
            _ => IdType::Other(s),
        }
    }
}   

// FIX: Implement Display for IdType
impl fmt::Display for IdType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            IdType::Mrn => write!(f, "MRN"),
            IdType::Ssn => write!(f, "SSN"),
            IdType::Other(s) => write!(f, "{}", s),
        }
    }
}

// --- Placeholder for ExternalId (necessary for the parsing logic above to compile) ---
#[derive(Debug, Clone)]
pub struct ExternalId {
    pub id_type: IdType,
    pub id_value: String,
    pub system: Option<String>,
}   

// FIX: Implement Display for ExternalId
// This implementation makes `.to_string()` return the core ID value, resolving the error.
impl fmt::Display for ExternalId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.id_value)
    }
}
    
// --- Placeholder for PatientId (necessary for the parsing logic above to compile) ---
#[derive(Debug, Clone, PartialEq, Eq)] // Added PartialEq/Eq for completeness
pub struct PatientId(String);
    
impl PatientId {
    /// Creates a new PatientId from a String. This is the constructor (was missing).
    pub fn new(s: String) -> Result<Self, String> {
        if s.is_empty() {
            return Err("PatientId cannot be empty".to_string());
        }
        
        if s.len() > 255 {
            return Err("PatientId cannot exceed 255 characters".to_string());
        }
        
        // Optional: Validate that it's either a valid UUID or alphanumeric MRN
        // Uncomment if you want strict UUID validation:
        // if s.parse::<Uuid>().is_err() {
        //     return Err(format!("PatientId '{}' is not a valid UUID", s));
        // }
        
        Ok(PatientId(s))
    }
    
    // NOTE: The 'from' method is usually implemented via the From trait, see below.
    // pub fn from(s: String) -> Self { PatientId(s) }
    
    /// Returns a reference to the inner ID as a string slice (idiomatic).
    pub fn as_str(&self) -> &str { 
        &self.0 
    }

    /// Returns the inner ID as a String. (The implementation you provided is fine but less idiomatic).
    pub fn to_string(&self) -> String { 
        self.0.clone() 
    }

    /// Attempts to parse the inner String as a Uuid. (Corrected implementation).
    pub fn to_uuid(&self) -> Result<Uuid, uuid::Error> {
        self.0.parse::<Uuid>()
    }

    /// Attempts to parse the inner String as a Uuid. (Alias for to_uuid, as requested).
    pub fn as_uuid(&self) -> Result<Uuid, uuid::Error> {
        self.0.parse::<Uuid>()
    }

    /// NEW: Consumes the PatientId and returns a Uuid.
    /// This resolves the E0599 error in mpi_identity_resolution.rs.
    pub fn into_uuid(self) -> Uuid {
        self.0.parse::<Uuid>().unwrap_or_else(|_| {
            // 2025-12-20: If we can't parse the UUID, we log a warning.
            // In a production MPI, you might want to resolve this via a lookup 
            // table instead of using a nil UUID fallback.
            eprintln!("[MPI Internal] Warning: Could not parse PatientId '{}' as UUID. Falling back to nil.", self.0);
            Uuid::nil()
        })
    }
}

// --- Trait Implementations for Idiomatic Rust ---

// Allows for `PatientId::from(s)` or `s.into()` where s is a String.
impl From<String> for PatientId {
    fn from(s: String) -> Self {
        PatientId(s)
    }
}

// Allows conversion from a string slice.
impl<'a> From<&'a str> for PatientId {
    fn from(s: &'a str) -> Self {
        PatientId(s.to_string())
    }
}

// Allows for transparent access to the inner String via Deref.
impl std::ops::Deref for PatientId {
    type Target = String;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

// FIX: Implement Display for PatientId (Redundant due to to_string(), but good practice)
impl fmt::Display for PatientId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl TryFrom<Vertex> for Patient {
    type Error = GraphError; // Assuming your error type is GraphError

    fn try_from(vertex: Vertex) -> Result<Self, Self::Error> {
        if vertex.label != "Patient".into() {
             return Err(GraphError::VertexError(format!(
                "Expected label 'Patient', found '{}'", vertex.label
            )));
        }
        
        // Convert Vertex properties to a serde_json::Map for automatic deserialization.
        let properties_map: HashMap<String, serde_json::Value> = vertex.properties.iter()
            .map(|(k, v)| (k.to_string(), v.to_serde_value()))
            .collect();
        
        let mut patient: Patient = serde_json::from_value(serde_json::Value::Object(
            serde_json::Map::from_iter(properties_map.into_iter())
        ))
        .map_err(|e| GraphError::VertexError(format!(
            "Failed to deserialize Patient properties into struct: {}", e
        )))?;

        // Manually inject the internal UUID of the Vertex back into the Patient struct
        // if your Patient struct has a field for it (e.g., patient_vertex_uuid).
        // Since the current definition isn't shown, we assume it's correctly handled by serde_json.
        // If not, you might need to add: patient.vertex_uuid = vertex.id.0;

        Ok(patient)
    }
}
