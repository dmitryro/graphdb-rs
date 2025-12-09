use serde::{Serialize, Deserialize};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SNOMED_Concept {
    pub id: Uuid,
    pub code: String,      // SNOMED code (e.g., "1234567890")
    pub display: String,   // Human-readable term
    pub system: String,    // "SNOMED_CT"
    pub version: Option<String>,
    // Hierarchy links handled via edges (e.g., IS_A)
}

