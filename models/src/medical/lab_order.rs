// models/src/medical/lab_order.rs
use chrono::{DateTime, Utc};
use crate::{Vertex, ToVertex, identifiers::Identifier};

#[derive(Debug, Clone)]
pub struct LabOrder {
    pub id: i32,
    pub patient_id: i32,
    pub encounter_id: Option<i32>,
    pub ordered_by_doctor_id: i32,
    pub test_requested: String,
    pub priority: String, // e.g., "Routine", "STAT"
    pub ordered_at: DateTime<Utc>,
    pub status: String,
    pub notes: Option<String>,
}

impl ToVertex for LabOrder {
    fn to_vertex(&self) -> Vertex {
        let mut v = Vertex::new(Identifier::new("LabOrder".to_string()).unwrap());
        v.add_property("id", &self.id.to_string());
        v.add_property("patient_id", &self.patient_id.to_string());
        if let Some(ref val) = self.encounter_id {
            v.add_property("encounter_id", &val.to_string());
        }
        v.add_property("ordered_by_doctor_id", &self.ordered_by_doctor_id.to_string());
        v.add_property("test_requested", &self.test_requested);
        v.add_property("priority", &self.priority);
        v.add_property("ordered_at", &self.ordered_at.to_rfc3339());
        v.add_property("status", &self.status);
        if let Some(ref val) = self.notes {
            v.add_property("notes", val);
        }
        v
    }
}

impl LabOrder {
    pub fn from_vertex(vertex: &Vertex) -> Option<Self> {
        if vertex.label.as_ref() != "LabOrder" { return None; }
        Some(LabOrder {
            id: vertex.properties.get("id")?.as_str()?.parse().ok()?,
            patient_id: vertex.properties.get("patient_id")?.as_str()?.parse().ok()?,
            encounter_id: vertex.properties.get("encounter_id").and_then(|v| v.as_str()).and_then(|s| s.parse().ok()),
            ordered_by_doctor_id: vertex.properties.get("ordered_by_doctor_id")?.as_str()?.parse().ok()?,
            test_requested: vertex.properties.get("test_requested")?.as_str()?.to_string(),
            priority: vertex.properties.get("priority")?.as_str()?.to_string(),
            ordered_at: chrono::DateTime::parse_from_rfc3339(
                vertex.properties.get("ordered_at")?.as_str()?
            ).ok()?.with_timezone(&chrono::Utc),
            status: vertex.properties.get("status")?.as_str()?.to_string(),
            notes: vertex.properties.get("notes").and_then(|v| v.as_str()).map(|s| s.to_string()),
        })
    }
}
