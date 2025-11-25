use crate::{Vertex, ToVertex, identifiers::Identifier};
use chrono::{DateTime, Utc};

#[derive(Debug, Clone)]
pub struct MedicalInteractionPrimary {
    pub id: i32,
    pub medication_id: i32,
    pub interaction_name: String,
    pub interaction_class: String,
    pub description: Option<String>,
}

impl ToVertex for MedicalInteractionPrimary {
    fn to_vertex(&self) -> Vertex {
        let id_type = Identifier::new("MedicalInteractionPrimary".to_string()).expect("Invalid Identifier");
        let mut vertex = Vertex::new(id_type);

        vertex.add_property("id", &self.id.to_string());
        vertex.add_property("medication_id", &self.medication_id.to_string());
        vertex.add_property("interaction_name", &self.interaction_name);
        vertex.add_property("interaction_class", &self.interaction_class);
        if let Some(ref desc) = self.description {
            vertex.add_property("description", desc);
        }

        vertex
    }
}

impl MedicalInteractionPrimary {
    pub fn from_vertex(vertex: &Vertex) -> Option<Self> {
        if vertex.label.as_ref() != "MedicalInteractionPrimary" { return None; }
        Some(MedicalInteractionPrimary {
            id: vertex.properties.get("id")?.as_str()?.parse().ok()?,
            medication_id: vertex.properties.get("medication_id")?.as_str()?.parse().ok()?,
            interaction_name: vertex.properties.get("interaction_name")?.as_str()?.to_string(),
            interaction_class: vertex.properties.get("interaction_class")?.as_str()?.to_string(),
            description: vertex.properties.get("description").and_then(|v| v.as_str()).map(|s| s.to_string()),
        })
    }
}
