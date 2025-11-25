// models/src/medical/disposition.rs
use chrono::{DateTime, Utc};
// FIX: Changed `crate::models::{...}` to `crate::{...}`
use crate::{Vertex, ToVertex, identifiers::Identifier};

#[derive(Debug, Clone)]
pub struct Disposition {
    pub id: i32,
    pub encounter_id: i32, // Links to an existing Encounter
    pub patient_id: i32, // Links to an existing Patient
    pub disposition_type: String, // e.g., "Discharged Home", "Admitted to Inpatient", "Transfer to Other Facility", "Left Against Medical Advice", "Expired"
    pub admitting_service: Option<String>, // e.g., "Internal Medicine", "Cardiology" (if admitted)
    pub admitting_doctor_id: Option<i32>, // Links to an existing Doctor (if admitted)
    pub transfer_facility_id: Option<i32>, // Links to a Hospital or Partner model (if transferred)
    pub discharge_instructions: Option<String>,
    pub disposed_at: DateTime<Utc>,
}

impl ToVertex for Disposition {
    fn to_vertex(&self) -> Vertex {
        let mut v = Vertex::new(Identifier::new("Disposition".to_string()).unwrap());
        v.add_property("id", &self.id.to_string());
        v.add_property("encounter_id", &self.encounter_id.to_string());
        v.add_property("patient_id", &self.patient_id.to_string());
        v.add_property("disposition_type", &self.disposition_type);
        if let Some(ref val) = self.admitting_service {
            v.add_property("admitting_service", val);
        }
        if let Some(ref val) = self.admitting_doctor_id {
            v.add_property("admitting_doctor_id", &val.to_string());
        }
        if let Some(ref val) = self.transfer_facility_id {
            v.add_property("transfer_facility_id", &val.to_string());
        }
        if let Some(ref val) = self.discharge_instructions {
            v.add_property("discharge_instructions", val);
        }
        v.add_property("disposed_at", &self.disposed_at.to_rfc3339());
        v
    }
}

impl Disposition {
    pub fn from_vertex(vertex: &Vertex) -> Option<Self> {
        if vertex.label.as_ref() != "Disposition" {
            return None;
        }

        Some(Disposition {
            id: vertex.properties.get("id")?.as_str()?.parse().ok()?,
            encounter_id: vertex.properties.get("encounter_id")?.as_str()?.parse().ok()?,
            patient_id: vertex.properties.get("patient_id")?.as_str()?.parse().ok()?,
            disposition_type: vertex.properties.get("disposition_type")?.as_str()?.to_string(),
            admitting_service: vertex.properties.get("admitting_service").and_then(|v| v.as_str()).map(|s| s.to_string()),
            admitting_doctor_id: vertex.properties.get("admitting_doctor_id").and_then(|v| v.as_str()).and_then(|s| s.parse().ok()),
            transfer_facility_id: vertex.properties.get("transfer_facility_id").and_then(|v| v.as_str()).and_then(|s| s.parse().ok()),
            discharge_instructions: vertex.properties.get("discharge_instructions").and_then(|v| v.as_str()).map(|s| s.to_string()),
            disposed_at: chrono::DateTime::parse_from_rfc3339(
                vertex.properties.get("disposed_at")?.as_str()?
            ).ok()?.with_timezone(&chrono::Utc),
        })
    }
}
