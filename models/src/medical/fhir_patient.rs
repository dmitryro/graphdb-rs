use serde::{Serialize, Deserialize};
use uuid::Uuid;
use chrono::{DateTime, Utc};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FHIR_Patient {
    pub id: Uuid,
    pub fhir_id: String,              // FHIR resource ID
    pub name: Vec<FHIR_Name>,         // Structured names
    pub birth_date: Option<DateTime<Utc>>,
    pub gender: Option<String>,       // "male", "female", etc.
    pub address: Vec<FHIR_Address>,
    // More FHIR fields: telecom, identifier, etc.
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FHIR_Name {
    pub family: String,
    pub given: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FHIR_Address {
    pub line: Vec<String>,
    pub city: String,
    pub state: String,
    pub postal_code: String,
}
