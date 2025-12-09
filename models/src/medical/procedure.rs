// models/src/medical/procedure.rs
use chrono::{DateTime, Utc};
use crate::{Vertex, ToVertex, identifiers::Identifier};

#[derive(Debug, Clone)]
pub struct Procedure {
    pub id: i32,
    pub patient_id: i32,
    pub encounter_id: Option<i32>,
    pub procedure_code_id: i32,
    pub procedure_name: String,
    pub performed_by_doctor_id: i32,
    pub performed_at: DateTime<Utc>,
    pub status: String,
    pub notes: Option<String>,
}

impl ToVertex for Procedure {
    fn to_vertex(&self) -> Vertex {
        let mut v = Vertex::new(Identifier::new("Procedure".to_string()).unwrap());
        v.add_property("id", &self.id.to_string());
        v.add_property("patient_id", &self.patient_id.to_string());
        if let Some(ref val) = self.encounter_id {
            v.add_property("encounter_id", &val.to_string());
        }
        v.add_property("procedure_code_id", &self.procedure_code_id.to_string());
        v.add_property("procedure_name", &self.procedure_name);
        v.add_property("performed_by_doctor_id", &self.performed_by_doctor_id.to_string());
        v.add_property("performed_at", &self.performed_at.to_rfc3339());
        v.add_property("status", &self.status);
        if let Some(ref val) = self.notes {
            v.add_property("notes", val);
        }
        v
    }
}

impl Procedure {
    pub fn from_vertex(vertex: &Vertex) -> Option<Self> {
        if vertex.label.as_ref() != "Procedure" { return None; }
        Some(Procedure {
            id: vertex.properties.get("id")?.as_str()?.parse().ok()?,
            patient_id: vertex.properties.get("patient_id")?.as_str()?.parse().ok()?,
            encounter_id: vertex.properties.get("encounter_id").and_then(|v| v.as_str()).and_then(|s| s.parse().ok()),
            procedure_code_id: vertex.properties.get("procedure_code_id")?.as_str()?.parse().ok()?,
            procedure_name: vertex.properties.get("procedure_name")?.as_str()?.to_string(),
            performed_by_doctor_id: vertex.properties.get("performed_by_doctor_id")?.as_str()?.parse().ok()?,
            performed_at: chrono::DateTime::parse_from_rfc3339(
                vertex.properties.get("performed_at")?.as_str()?
            ).ok()?.with_timezone(&chrono::Utc),
            status: vertex.properties.get("status")?.as_str()?.to_string(),
            notes: vertex.properties.get("notes").and_then(|v| v.as_str()).map(|s| s.to_string()),
        })
    }
}
