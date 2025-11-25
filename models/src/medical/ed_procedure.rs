// models/src/medical/ed_procedure.rs
use chrono::{DateTime, Utc};
// FIX: Changed `crate::models::{...}` to `crate::{...}`
use crate::{Vertex, ToVertex, identifiers::Identifier};

#[derive(Debug, Clone)]
pub struct EdProcedure {
    pub id: i32,
    pub encounter_id: i32, // Links to an existing Encounter
    pub patient_id: i32, // Links to an existing Patient
    pub procedure_code_id: i32, // Links to an existing MedicalCode (e.g., CPT/HCPCS for billing/standardization)
    pub procedure_name: String,
    pub performed_by_doctor_id: i32, // Links to an existing Doctor
    pub assist_nurse_id: Option<i32>, // Links to an existing Nurse
    pub start_time: DateTime<Utc>,
    pub end_time: Option<DateTime<Utc>>,
    pub outcome: Option<String>, // e.g., "Successful", "Complicated", "Aborted"
    pub notes: Option<String>,
}

impl ToVertex for EdProcedure {
    fn to_vertex(&self) -> Vertex {
        let mut v = Vertex::new(Identifier::new("EdProcedure".to_string()).unwrap());
        v.add_property("id", &self.id.to_string());
        v.add_property("encounter_id", &self.encounter_id.to_string());
        v.add_property("patient_id", &self.patient_id.to_string());
        v.add_property("procedure_code_id", &self.procedure_code_id.to_string());
        v.add_property("procedure_name", &self.procedure_name);
        v.add_property("performed_by_doctor_id", &self.performed_by_doctor_id.to_string());
        if let Some(ref val) = self.assist_nurse_id {
            v.add_property("assist_nurse_id", &val.to_string());
        }
        v.add_property("start_time", &self.start_time.to_rfc3339());
        if let Some(ref val) = self.end_time {
            v.add_property("end_time", &val.to_rfc3339());
        }
        if let Some(ref val) = self.outcome {
            v.add_property("outcome", val);
        }
        if let Some(ref val) = self.notes {
            v.add_property("notes", val);
        }
        v
    }
}

impl EdProcedure {
    pub fn from_vertex(vertex: &Vertex) -> Option<Self> {
        if vertex.label.as_ref() != "EdProcedure" { return None; }
        Some(EdProcedure {
            id: vertex.properties.get("id")?.as_str()?.parse().ok()?,
            encounter_id: vertex.properties.get("encounter_id")?.as_str()?.parse().ok()?,
            patient_id: vertex.properties.get("patient_id")?.as_str()?.parse().ok()?,
            procedure_code_id: vertex.properties.get("procedure_code_id")?.as_str()?.parse().ok()?,
            procedure_name: vertex.properties.get("procedure_name")?.as_str()?.to_string(),
            performed_by_doctor_id: vertex.properties.get("performed_by_doctor_id")?.as_str()?.parse().ok()?,
            assist_nurse_id: vertex.properties.get("assist_nurse_id").and_then(|v| v.as_str()).and_then(|s| s.parse().ok()),
            start_time: chrono::DateTime::parse_from_rfc3339(vertex.properties.get("start_time")?.as_str()?)
                .ok()?.with_timezone(&chrono::Utc),
            end_time: vertex.properties.get("end_time")
                .and_then(|v| v.as_str())
                .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
                .map(|dt| dt.with_timezone(&chrono::Utc)),
            outcome: vertex.properties.get("outcome").and_then(|v| v.as_str()).map(|s| s.to_string()),
            notes: vertex.properties.get("notes").and_then(|v| v.as_str()).map(|s| s.to_string()),
        })
    }
}
