use chrono::{DateTime, Utc};
use crate::{Vertex, ToVertex, identifiers::Identifier};

#[derive(Debug, Clone)]
pub struct ClinicalNote {
    pub id: i32,
    pub patient_id: i32,
    pub doctor_id: i32,
    pub note_text: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl ToVertex for ClinicalNote {
    fn to_vertex(&self) -> Vertex {
        let id_type = Identifier::new("ClinicalNote".to_string()).expect("Invalid Identifier");
        let mut vertex = Vertex::new(id_type);

        vertex.add_property("id", &self.id.to_string());
        vertex.add_property("patient_id", &self.patient_id.to_string());
        vertex.add_property("doctor_id", &self.doctor_id.to_string());
        vertex.add_property("note_text", &self.note_text);
        vertex.add_property("created_at", &self.created_at.to_rfc3339());
        vertex.add_property("updated_at", &self.updated_at.to_rfc3339());

        vertex
    }
}

impl ClinicalNote {
    pub fn from_vertex(vertex: &Vertex) -> Option<Self> {
        if vertex.label.as_ref() != "ClinicalNote" { return None; }
        Some(ClinicalNote {
            id: vertex.properties.get("id")?.as_str()?.parse().ok()?,
            patient_id: vertex.properties.get("patient_id")?.as_str()?.parse().ok()?,
            doctor_id: vertex.properties.get("doctor_id")?.as_str()?.parse().ok()?,
            note_text: vertex.properties.get("note_text")?.as_str()?.to_string(),
            created_at: chrono::DateTime::parse_from_rfc3339(
                vertex.properties.get("created_at")?.as_str()?
            ).ok()?.with_timezone(&chrono::Utc),
            updated_at: chrono::DateTime::parse_from_rfc3339(
                vertex.properties.get("updated_at")?.as_str()?
            ).ok()?.with_timezone(&chrono::Utc),
        })
    }
}
