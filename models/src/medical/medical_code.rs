// medical_code.rs
use chrono::{DateTime, Utc};
use crate::{Vertex, ToVertex, identifiers::Identifier};

#[derive(Debug, Clone)]
pub struct MedicalCode {
    pub id: i32,
    pub code: String,
    pub description: String,
    pub code_type: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl ToVertex for MedicalCode {
    fn to_vertex(&self) -> Vertex {
        let mut v = Vertex::new(Identifier::new("MedicalCode".to_string()).unwrap());
        v.add_property("id", &self.id.to_string());
        v.add_property("code", &self.code);
        v.add_property("description", &self.description);
        v.add_property("code_type", &self.code_type);
        v.add_property("created_at", &self.created_at.to_rfc3339());
        v.add_property("updated_at", &self.updated_at.to_rfc3339());
        v
    }
}

impl MedicalCode {
    pub fn from_vertex(vertex: &Vertex) -> Option<Self> {
        if vertex.label.as_ref() != "MedicalCode" { return None; }
        Some(MedicalCode {
            id: vertex.properties.get("id")?.as_str()?.parse().ok()?,
            code: vertex.properties.get("code")?.as_str()?.to_string(),
            description: vertex.properties.get("description")?.as_str()?.to_string(),
            code_type: vertex.properties.get("code_type")?.as_str()?.to_string(),
            created_at: chrono::DateTime::parse_from_rfc3339(
                vertex.properties.get("created_at")?.as_str()?
            ).ok()?.with_timezone(&chrono::Utc),
            updated_at: chrono::DateTime::parse_from_rfc3339(
                vertex.properties.get("updated_at")?.as_str()?
            ).ok()?.with_timezone(&chrono::Utc),
        })
    }
}
