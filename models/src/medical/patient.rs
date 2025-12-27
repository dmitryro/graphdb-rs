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
use crate::properties::{ PropertyValue, UnhashableVertex, SerializableDateTime }; // <--- ADD THIS IMPORT

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
        let label_str = "Patient";
        let identifier = Identifier::new(label_str.to_string()).unwrap_or_else(|_| {
            println!("[ToVertex] CRITICAL: Failed to create identifier for 'Patient'");
            panic!("Invalid Label");
        });
        
        let mut v = Vertex::new(identifier);

        println!("[ToVertex] Starting conversion for MRN: {:?}", self.mrn);

        // --- PRIMARY IDENTIFIERS ---
        if let Some(id_val) = self.id {
            v.properties.insert("id".to_string(), PropertyValue::Integer(id_val as i64));
        }
        
        if let Some(ref val) = self.user_id {
            v.properties.insert("user_id".to_string(), PropertyValue::String(val.to_string()));
        }
        
        // Vertex ID is crucial for the graph of changes/events (MPI requirement)
        if let Some(ref val) = self.vertex_id {
            v.properties.insert("vertex_id".to_string(), PropertyValue::String(val.to_string()));
        }
        
        if let Some(ref val) = self.mrn {
            v.properties.insert("mrn".to_string(), PropertyValue::String(val.clone()));
        }
        
        if let Some(ref val) = self.ssn {
            v.properties.insert("ssn".to_string(), PropertyValue::String(val.clone()));
        }

        // --- DEMOGRAPHICS ---
        if let Some(ref val) = self.first_name {
            v.properties.insert("first_name".to_string(), PropertyValue::String(val.clone()));
        }
        if let Some(ref val) = self.middle_name {
            v.properties.insert("middle_name".to_string(), PropertyValue::String(val.clone()));
        }
        if let Some(ref val) = self.last_name {
            v.properties.insert("last_name".to_string(), PropertyValue::String(val.clone()));
        }
        if let Some(ref val) = self.suffix {
            v.properties.insert("suffix".to_string(), PropertyValue::String(val.clone()));
        }
        if let Some(ref val) = self.preferred_name {
            v.properties.insert("preferred_name".to_string(), PropertyValue::String(val.clone()));
        }

        // --- DATE FIELDS (Using Explicit SerializableDateTime Wrapper) ---
        v.properties.insert(
            "date_of_birth".to_string(), 
            PropertyValue::DateTime(SerializableDateTime(self.date_of_birth))
        );

        if let Some(ref val) = self.date_of_death {
            v.properties.insert(
                "date_of_death".to_string(), 
                PropertyValue::DateTime(SerializableDateTime(*val))
            );
        }
        
        if let Some(ref val) = self.gender { 
            v.properties.insert("gender".to_string(), PropertyValue::String(val.clone()));
        }
        if let Some(ref val) = self.sex_assigned_at_birth {
            v.properties.insert("sex_assigned_at_birth".to_string(), PropertyValue::String(val.clone()));
        }
        if let Some(ref val) = self.gender_identity {
            v.properties.insert("gender_identity".to_string(), PropertyValue::String(val.clone()));
        }
        if let Some(ref val) = self.pronouns {
            v.properties.insert("pronouns".to_string(), PropertyValue::String(val.clone()));
        }

        // --- CONTACT INFORMATION ---
        if let Some(ref addr_id) = self.address_id {
            v.properties.insert("address_id".to_string(), PropertyValue::String(addr_id.to_string()));
        }
        if let Some(ref addr) = self.address {
            match serde_json::to_string(addr) {
                Ok(s) => { v.properties.insert("address".to_string(), PropertyValue::String(s)); }
                Err(e) => { println!("[ToVertex] Error serializing address: {}", e); }
            }
        }
        if let Some(ref val) = self.phone_home {
            v.properties.insert("phone_home".to_string(), PropertyValue::String(val.clone()));
        }
        if let Some(ref val) = self.phone_mobile {
            v.properties.insert("phone_mobile".to_string(), PropertyValue::String(val.clone()));
        }
        if let Some(ref val) = self.phone_work {
            v.properties.insert("phone_work".to_string(), PropertyValue::String(val.clone()));
        }
        if let Some(ref val) = self.email {
            v.properties.insert("email".to_string(), PropertyValue::String(val.clone()));
        }
        if let Some(ref val) = self.preferred_contact_method {
            v.properties.insert("preferred_contact_method".to_string(), PropertyValue::String(val.clone()));
        }
        if let Some(ref val) = self.preferred_language {
            v.properties.insert("preferred_language".to_string(), PropertyValue::String(val.clone()));
        }
        if let Some(interpreter_needed) = self.interpreter_needed {
            v.properties.insert("interpreter_needed".to_string(), PropertyValue::Boolean(interpreter_needed)); 
        }

        // --- EMERGENCY CONTACT ---
        if let Some(ref val) = self.emergency_contact_name {
            v.properties.insert("emergency_contact_name".to_string(), PropertyValue::String(val.clone()));
        }
        if let Some(ref val) = self.emergency_contact_relationship {
            v.properties.insert("emergency_contact_relationship".to_string(), PropertyValue::String(val.clone()));
        }
        if let Some(ref val) = self.emergency_contact_phone {
            v.properties.insert("emergency_contact_phone".to_string(), PropertyValue::String(val.clone()));
        }

        // --- DEMOGRAPHIC DETAILS ---
        if let Some(ref val) = self.marital_status {
            v.properties.insert("marital_status".to_string(), PropertyValue::String(val.clone()));
        }
        if let Some(ref val) = self.race {
            v.properties.insert("race".to_string(), PropertyValue::String(val.clone()));
        }
        if let Some(ref val) = self.ethnicity {
            v.properties.insert("ethnicity".to_string(), PropertyValue::String(val.clone()));
        }
        if let Some(ref val) = self.religion {
            v.properties.insert("religion".to_string(), PropertyValue::String(val.clone()));
        }

        // --- INSURANCE & FINANCIAL ---
        if let Some(ref val) = self.primary_insurance {
            v.properties.insert("primary_insurance".to_string(), PropertyValue::String(val.clone()));
        }
        if let Some(ref val) = self.primary_insurance_id {
            v.properties.insert("primary_insurance_id".to_string(), PropertyValue::String(val.clone()));
        }
        if let Some(ref val) = self.secondary_insurance {
            v.properties.insert("secondary_insurance".to_string(), PropertyValue::String(val.clone()));
        }
        if let Some(ref val) = self.secondary_insurance_id {
            v.properties.insert("secondary_insurance_id".to_string(), PropertyValue::String(val.clone()));
        }
        if let Some(ref val) = self.guarantor_name {
            v.properties.insert("guarantor_name".to_string(), PropertyValue::String(val.clone()));
        }
        if let Some(ref val) = self.guarantor_relationship {
            v.properties.insert("guarantor_relationship".to_string(), PropertyValue::String(val.clone()));
        }

        // --- CLINICAL INFORMATION ---
        if let Some(ref val) = self.primary_care_provider_id {
            v.properties.insert("primary_care_provider_id".to_string(), PropertyValue::String(val.to_string()));
        }
        if let Some(ref val) = self.blood_type {
            v.properties.insert("blood_type".to_string(), PropertyValue::String(val.clone()));
        }
        if let Some(ref val) = self.organ_donor {
            v.properties.insert("organ_donor".to_string(), PropertyValue::Boolean(*val));
        }

        // --- DIRECTIVE STATUS ---
        if let Some(val) = self.advance_directive_on_file {
            v.properties.insert("advance_directive_on_file".to_string(), PropertyValue::Boolean(val));
        }
        if let Some(ref val) = self.dni_status {
            v.properties.insert("dni_status".to_string(), PropertyValue::String(val.clone()));
        }
        if let Some(ref val) = self.dnr_status {
            v.properties.insert("dnr_status".to_string(), PropertyValue::String(val.clone()));
        }
        if let Some(ref val) = self.code_status {
            v.properties.insert("code_status".to_string(), PropertyValue::String(val.clone()));
        }

        // --- ADMINISTRATIVE ---
        if let Some(ref val) = self.patient_status {
            v.properties.insert("patient_status".to_string(), PropertyValue::String(val.clone())); 
        }
        if let Some(val) = self.vip_flag {
            v.properties.insert("vip_flag".to_string(), PropertyValue::Boolean(val));
        } 
        if let Some(val) = self.confidential_flag {
            v.properties.insert("confidential_flag".to_string(), PropertyValue::Boolean(val));
        }
        if let Some(ref val) = self.research_consent {
            v.properties.insert("research_consent".to_string(), PropertyValue::Boolean(*val));
        }
        if let Some(ref val) = self.marketing_consent {
            v.properties.insert("marketing_consent".to_string(), PropertyValue::Boolean(*val));
        }

        // --- SOCIAL DETERMINANTS OF HEALTH ---
        if let Some(ref val) = self.employment_status {
            v.properties.insert("employment_status".to_string(), PropertyValue::String(val.clone()));
        }
        if let Some(ref val) = self.housing_status {
            v.properties.insert("housing_status".to_string(), PropertyValue::String(val.clone()));
        }
        if let Some(ref val) = self.education_level {
            v.properties.insert("education_level".to_string(), PropertyValue::String(val.clone()));
        }
        if let Some(ref val) = self.financial_strain {
            v.properties.insert("financial_strain".to_string(), PropertyValue::String(val.clone()));
        }
        if let Some(val) = self.food_insecurity {
            v.properties.insert("food_insecurity".to_string(), PropertyValue::Boolean(val));
        }
        if let Some(val) = self.transportation_needs {
            v.properties.insert("transportation_needs".to_string(), PropertyValue::Boolean(val));
        }
        if let Some(ref val) = self.social_isolation {
            v.properties.insert("social_isolation".to_string(), PropertyValue::String(val.clone()));
        }
        if let Some(ref val) = self.veteran_status {
            v.properties.insert("veteran_status".to_string(), PropertyValue::Boolean(*val));
        }
        if let Some(ref val) = self.disability_status {
            v.properties.insert("disability_status".to_string(), PropertyValue::String(val.clone()));
        }

        // --- CLINICAL ALERTS ---
        if let Some(ref val) = self.alert_flags {
            v.properties.insert("alert_flags".to_string(), PropertyValue::String(val.clone()));
        }
        if let Some(ref val) = self.special_needs {
            v.properties.insert("special_needs".to_string(), PropertyValue::String(val.clone()));
        }

        // --- AUDIT TRAIL (Native DateTime variants using SerializableDateTime) ---
        v.properties.insert(
            "created_at".to_string(), 
            PropertyValue::DateTime(SerializableDateTime(self.created_at))
        );
        v.properties.insert(
            "updated_at".to_string(), 
            PropertyValue::DateTime(SerializableDateTime(self.updated_at))
        );
        
        if let Some(ref val) = self.created_by {
            v.properties.insert("created_by".to_string(), PropertyValue::String(val.to_string()));
        }
        if let Some(ref val) = self.updated_by {
            v.properties.insert("updated_by".to_string(), PropertyValue::String(val.to_string()));
        }
        if let Some(ref val) = self.last_visit_date {
            v.properties.insert(
                "last_visit_date".to_string(), 
                PropertyValue::DateTime(SerializableDateTime(*val))
            );
        }

        v
    }
}

fn normalize_patient_vertex(vertex: &Vertex) -> Vertex {
    let mut normalized = vertex.clone();
    
    // --- 1. Vertex ID mapping ---
    normalized.properties.insert(
        "vertex_id".to_string(),
        PropertyValue::String(vertex.id.0.to_string())
    );
    
    // --- 2. Handle numeric ID types to prevent conversion errors ---
    if let Some(id_val) = normalized.properties.get("id").cloned() {
        if let PropertyValue::String(s) = id_val {
            if let Ok(parsed) = s.parse::<i64>() {
                normalized.properties.insert("id".to_string(), PropertyValue::Integer(parsed));
            }
        }
    }

    // --- 3. Handle legacy "dob" field and ensure "date_of_birth" is a DateTime variant ---
    let dob_key = "date_of_birth".to_string();
    
    // Helper to attempt conversion of existing value to DateTime variant
    let try_to_datetime = |val: &PropertyValue| -> Option<PropertyValue> {
        match val {
            PropertyValue::DateTime(_) => Some(val.clone()),
            PropertyValue::String(s) => {
                // Try RFC3339 first
                if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(s) {
                    return Some(PropertyValue::DateTime(dt.with_timezone(&chrono::Utc).into()));
                }
                // Try Naive date yyyy-mm-dd
                if let Ok(nd) = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d") {
                    let dt = nd.and_hms_opt(0, 0, 0).unwrap().and_utc();
                    return Some(PropertyValue::DateTime(dt.into()));
                }
                None
            }
            _ => None,
        }
    };

    // If date_of_birth is missing but legacy 'dob' exists, move it
    if !normalized.properties.contains_key(&dob_key) {
        if let Some(legacy_dob) = normalized.properties.get("dob") {
            if let Some(dt_val) = try_to_datetime(legacy_dob) {
                normalized.properties.insert(dob_key.clone(), dt_val);
                normalized.properties.remove("dob");
            }
        }
    }

    // Ensure date_of_birth is now a DateTime variant if it was a String
    if let Some(current_dob) = normalized.properties.get(&dob_key).cloned() {
        if let Some(dt_val) = try_to_datetime(&current_dob) {
            normalized.properties.insert(dob_key.clone(), dt_val);
        }
    } else {
        // Ultimate fallback for missing DOB
        let default_dt = chrono::NaiveDate::from_ymd_opt(1900, 1, 1).unwrap().and_hms_opt(0, 0, 0).unwrap().and_utc();
        normalized.properties.insert(dob_key, PropertyValue::DateTime(default_dt.into()));
    }
    
    // --- 4. Handle legacy "name" field ---
    let mut names_set = normalized.properties.contains_key("first_name") && normalized.properties.contains_key("last_name");
    
    if !names_set {
        let name_string_to_split: Option<String> = normalized.properties.get("name").and_then(|v| {
            if let PropertyValue::String(s) = v { Some(s.clone()) } else { None }
        });

        if let Some(name_value) = name_string_to_split {
            let parts: Vec<&str> = name_value.split_whitespace().collect();
            if let Some(first) = parts.first() {
                normalized.properties.insert("first_name".to_string(), PropertyValue::String(first.to_string()));
            }
            if parts.len() > 1 {
                let last = parts[1..].join(" ");
                normalized.properties.insert("last_name".to_string(), PropertyValue::String(last));
            } else {
                normalized.properties.insert("last_name".to_string(), PropertyValue::String("".to_string()));
            }
            normalized.properties.remove("name");
            names_set = true;
        }
    }
    
    if !normalized.properties.contains_key("first_name") {
        normalized.properties.insert("first_name".to_string(), PropertyValue::String("".to_string()));
    }
    if !normalized.properties.contains_key("last_name") {
        normalized.properties.insert("last_name".to_string(), PropertyValue::String("".to_string()));
    }
    
    // --- 5. Fix Boolean fields from Strings ---
    let bool_fields = [
        "interpreter_needed", "advance_directive_on_file", "vip_flag", 
        "confidential_flag", "food_insecurity", "transportation_needs",
        "organ_donor", "research_consent", "marketing_consent", "veteran_status"
    ];

    for field in bool_fields {
        if let Some(PropertyValue::String(s)) = normalized.properties.get(field) {
            let b_val = s.to_lowercase() == "true";
            normalized.properties.insert(field.to_string(), PropertyValue::Boolean(b_val));
        }
    }

    // --- 6. Defaults and Audit Timestamps (ensure DateTime variants) ---
    if !normalized.properties.contains_key("patient_status") {
        normalized.properties.insert("patient_status".to_string(), PropertyValue::String("ACTIVE".to_string()));
    }
    
    let now = chrono::Utc::now();
    for audit_field in ["created_at", "updated_at"] {
        let existing = normalized.properties.get(audit_field).cloned();
        if let Some(val) = existing {
            if let Some(dt_val) = try_to_datetime(&val) {
                normalized.properties.insert(audit_field.to_string(), dt_val);
            }
        } else {
            normalized.properties.insert(audit_field.to_string(), PropertyValue::DateTime(now.into()));
        }
    }
    
    normalized
}

impl Patient {
    pub fn from_vertex(vertex: &Vertex) -> Option<Self> {
        if vertex.label.as_ref() != "Patient" { 
            println!("[Patient::from_vertex] Failed: Label is not Patient, found: {}", vertex.label.as_ref());
            return None; 
        }
        
        let normalized = normalize_patient_vertex(vertex);
        
        // --- DEFENSIVE DATE EXTRACTION HELPER ---
        // Handles both String (from manual insert) and DateTime (from DB casting/native storage)
        let extract_date = |key: &str| -> Option<chrono::DateTime<chrono::Utc>> {
            let val = normalized.properties.get(key);
            match val {
                Some(PropertyValue::DateTime(dt)) => Some(dt.0.with_timezone(&chrono::Utc)),
                Some(PropertyValue::String(s)) => {
                    chrono::DateTime::parse_from_rfc3339(s)
                        .ok()
                        .map(|dt| dt.with_timezone(&chrono::Utc))
                },
                _ => None,
            }
        };

        // --- CRITICAL FIELD: DATE OF BIRTH ---
        let dob = extract_date("date_of_birth");
        if dob.is_none() {
            println!(
                "[Patient::from_vertex] Error: date_of_birth missing or unparseable. Raw value: {:?}", 
                normalized.properties.get("date_of_birth")
            );
        }

        // --- AUDIT FIELDS (CRITICAL FOR MPI LOGGING) ---
        // Fallback to vertex root audit if property map doesn't contain them
        let created_at = extract_date("created_at").unwrap_or(vertex.created_at.0);
        let updated_at = extract_date("updated_at").unwrap_or(vertex.updated_at.0);

        let patient = Patient {
            vertex_id: normalized.properties.get("vertex_id")
                .and_then(|v| v.as_str())
                .and_then(|s| Uuid::parse_str(s).ok())
                .or_else(|| Some(vertex.id.0)),

            id: match normalized.properties.get("id") {
                Some(PropertyValue::Integer(i)) => Some(*i as i32),
                Some(PropertyValue::String(s)) => {
                    let p = s.parse::<i32>().ok();
                    if p.is_none() { println!("[Patient::from_vertex] Warn: id string '{}' failed to parse to i32", s); }
                    p
                },
                _ => None,
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

            // Extract the DOB or return None for the whole record
            date_of_birth: dob?,
            
            date_of_death: extract_date("date_of_death"),
            
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
                
            interpreter_needed: match normalized.properties.get("interpreter_needed") {
                Some(PropertyValue::Boolean(b)) => Some(*b),
                Some(PropertyValue::String(s)) => s.parse().ok(),
                _ => None,
            },

            emergency_contact_name: normalized.properties.get("emergency_contact_name")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            emergency_contact_relationship: normalized.properties.get("emergency_contact_relationship")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            emergency_contact_phone: normalized.properties.get("emergency_contact_phone")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),

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

            primary_care_provider_id: normalized.properties.get("primary_care_provider_id")
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse().ok()),
            blood_type: normalized.properties.get("blood_type")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            organ_donor: match normalized.properties.get("organ_donor") {
                Some(PropertyValue::Boolean(b)) => Some(*b),
                Some(PropertyValue::String(s)) => s.parse().ok(),
                _ => None,
            },

            advance_directive_on_file: match normalized.properties.get("advance_directive_on_file") {
                Some(PropertyValue::Boolean(b)) => Some(*b),
                Some(PropertyValue::String(s)) => s.parse().ok(),
                _ => None,
            },
            dni_status: normalized.properties.get("dni_status")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            dnr_status: normalized.properties.get("dnr_status")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            code_status: normalized.properties.get("code_status")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),

            patient_status: normalized.properties.get("patient_status")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
                .or_else(|| Some("ACTIVE".to_string())),
            vip_flag: match normalized.properties.get("vip_flag") {
                Some(PropertyValue::Boolean(b)) => Some(*b),
                Some(PropertyValue::String(s)) => s.parse().ok(),
                _ => None,
            },
            confidential_flag: match normalized.properties.get("confidential_flag") {
                Some(PropertyValue::Boolean(b)) => Some(*b),
                Some(PropertyValue::String(s)) => s.parse().ok(),
                _ => None,
            },
            research_consent: match normalized.properties.get("research_consent") {
                Some(PropertyValue::Boolean(b)) => Some(*b),
                Some(PropertyValue::String(s)) => s.parse().ok(),
                _ => None,
            },
            marketing_consent: match normalized.properties.get("marketing_consent") {
                Some(PropertyValue::Boolean(b)) => Some(*b),
                Some(PropertyValue::String(s)) => s.parse().ok(),
                _ => None,
            },

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
            food_insecurity: match normalized.properties.get("food_insecurity") {
                Some(PropertyValue::Boolean(b)) => Some(*b),
                Some(PropertyValue::String(s)) => s.parse().ok(),
                _ => None,
            },
            transportation_needs: match normalized.properties.get("transportation_needs") {
                Some(PropertyValue::Boolean(b)) => Some(*b),
                Some(PropertyValue::String(s)) => s.parse().ok(),
                _ => None,
            },
            social_isolation: normalized.properties.get("social_isolation")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            veteran_status: match normalized.properties.get("veteran_status") {
                Some(PropertyValue::Boolean(b)) => Some(*b),
                Some(PropertyValue::String(s)) => s.parse().ok(),
                _ => None,
            },
            disability_status: normalized.properties.get("disability_status")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            
            alert_flags: normalized.properties.get("alert_flags")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            special_needs: normalized.properties.get("special_needs")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),

            created_at,
            updated_at,
            
            created_by: normalized.properties.get("created_by")
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse().ok()),
            updated_by: normalized.properties.get("updated_by")
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse().ok()),
            last_visit_date: extract_date("last_visit_date"),
        };

        // Final sanity check for MPI logging consistency
        if patient.mrn.is_none() {
            println!("[Patient::from_vertex] Warn: Successfully parsed Patient but MRN is missing.");
        }

        Some(patient)
    }

    pub fn from_vertex_value(value: &PropertyValue) -> Option<Self> {
        if let PropertyValue::Vertex(unhashable_vertex) = value {
            return Patient::from_vertex(&unhashable_vertex.0); 
        }

        if let PropertyValue::Map(hashable_map) = value {
            let properties: HashMap<String, PropertyValue> = hashable_map.0
                .iter()
                .map(|(id, value)| (id.to_string(), value.clone()))
                .collect();

            let label = Identifier::new("Patient".to_string()).ok();
            if label.is_none() { println!("[Patient::from_vertex_value] Error: Failed to create Label identifier"); return None; }

            let vertex = Vertex {
                id: Default::default(),
                label: label.unwrap(),
                properties,
                created_at: Utc::now().into(),
                updated_at: Utc::now().into(),
            };
            return Patient::from_vertex(&vertex); 
        }
        
        println!("[Patient::from_vertex_value] Error: PropertyValue is neither Vertex nor Map. Found: {:?}", value);
        None
    }
}

fn extract_date(val: Option<&PropertyValue>) -> Option<chrono::DateTime<chrono::Utc>> {
    match val {
        Some(PropertyValue::DateTime(dt)) => Some(dt.0.with_timezone(&chrono::Utc)),
        Some(PropertyValue::String(s)) => {
            chrono::DateTime::parse_from_rfc3339(s)
                .ok()
                .map(|dt| dt.with_timezone(&chrono::Utc))
        },
        _ => None,
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
