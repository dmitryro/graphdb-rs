use chrono::{DateTime, Utc};
use crate::{Vertex, ToVertex, identifiers::Identifier};

#[derive(Debug, Clone)]
pub struct MedicalRecord {
    pub id: i32,
    pub patient_id: i32,
    pub doctor_id: i32,
    pub record_type: Option<String>,
    pub record_data: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl ToVertex for MedicalRecord {
    fn to_vertex(&self) -> Vertex {
        let mut v = Vertex::new(Identifier::new("MedicalRecord".to_string()).unwrap());
        v.add_property("id", &self.id.to_string());
        v.add_property("patient_id", &self.patient_id.to_string());
        v.add_property("doctor_id", &self.doctor_id.to_string());
        if let Some(ref val) = self.record_type {
            v.add_property("record_type", val);
        }
        if let Some(ref val) = self.record_data {
            v.add_property("record_data", val);
        }
        v.add_property("created_at", &self.created_at.to_rfc3339());
        v.add_property("updated_at", &self.updated_at.to_rfc3339());
        v
    }
}

impl MedicalRecord {
    pub fn from_vertex(vertex: &Vertex) -> Option<Self> {
        if vertex.label.as_ref() != "MedicalRecord" { return None; }
        Some(MedicalRecord {
            id: vertex.properties.get("id")?.as_str()?.parse().ok()?,
            patient_id: vertex.properties.get("patient_id")?.as_str()?.parse().ok()?,
            doctor_id: vertex.properties.get("doctor_id")?.as_str()?.parse().ok()?,
            record_type: vertex.properties.get("record_type").and_then(|v| v.as_str()).map(|s| s.to_string()),
            record_data: vertex.properties.get("record_data").and_then(|v| v.as_str()).map(|s| s.to_string()),
            created_at: chrono::DateTime::parse_from_rfc3339(
                vertex.properties.get("created_at")?.as_str()?
            ).ok()?.with_timezone(&chrono::Utc),
            updated_at: chrono::DateTime::parse_from_rfc3339(
                vertex.properties.get("updated_at")?.as_str()?
            ).ok()?.with_timezone(&chrono::Utc),
        })
    }
}
