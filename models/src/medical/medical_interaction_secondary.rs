use crate::{Vertex, ToVertex, identifiers::Identifier};
use chrono::{DateTime, Utc};

#[derive(Debug, Clone)]
pub struct MedicalInteractionSecondary {
    pub id: i32,
    pub primary_medication_id: i32,
    pub secondary_medication_id: i32,
    pub severity: String,
    pub description: Option<String>,
}

impl ToVertex for MedicalInteractionSecondary {
    fn to_vertex(&self) -> Vertex {
        let id_type = Identifier::new("MedicalInteractionSecondary".to_string()).expect("Invalid Identifier");
        let mut vertex = Vertex::new(id_type);

        vertex.add_property("id", &self.id.to_string());
        vertex.add_property("primary_medication_id", &self.primary_medication_id.to_string());
        vertex.add_property("secondary_medication_id", &self.secondary_medication_id.to_string());
        vertex.add_property("severity", &self.severity);
        if let Some(ref desc) = self.description {
            vertex.add_property("description", desc);
        }

        vertex
    }
}

impl MedicalInteractionSecondary {
    pub fn from_vertex(vertex: &Vertex) -> Option<Self> {
        if vertex.label.as_ref() != "MedicalInteractionSecondary" { return None; }
        Some(MedicalInteractionSecondary {
            id: vertex.properties.get("id")?.as_str()?.parse().ok()?,
            primary_medication_id: vertex.properties.get("primary_medication_id")?.as_str()?.parse().ok()?,
            secondary_medication_id: vertex.properties.get("secondary_medication_id")?.as_str()?.parse().ok()?,
            severity: vertex.properties.get("severity")?.as_str()?.to_string(),
            description: vertex.properties.get("description").and_then(|v| v.as_str()).map(|s| s.to_string()),
        })
    }
}
