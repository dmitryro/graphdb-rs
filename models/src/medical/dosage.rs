use chrono::{DateTime, Utc};
use crate::{Vertex, ToVertex, identifiers::Identifier};

#[derive(Debug, Clone)]
pub struct Dosage {
    pub id: i32,
    pub medication_id: i32,
    pub dosage_amount: String,
    pub dosage_frequency: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: Option<DateTime<Utc>>,
}

impl ToVertex for Dosage {
    fn to_vertex(&self) -> Vertex {
        let mut vertex = Vertex::new(Identifier::new("Dosage".to_string()).unwrap());
        vertex.add_property("id", &self.id.to_string());
        vertex.add_property("medication_id", &self.medication_id.to_string());
        vertex.add_property("dosage_amount", &self.dosage_amount);
        vertex.add_property("dosage_frequency", &self.dosage_frequency);
        vertex.add_property("created_at", &self.created_at.to_rfc3339());

        if let Some(ref updated) = self.updated_at {
            vertex.add_property("updated_at", &updated.to_rfc3339());
        }

        vertex
    }
}

impl Dosage {
    pub fn from_vertex(vertex: &Vertex) -> Option<Self> {
        if vertex.label.as_ref() != "Dosage" { return None; }
        Some(Dosage {
            id: vertex.properties.get("id")?.as_str()?.parse().ok()?,
            medication_id: vertex.properties.get("medication_id")?.as_str()?.parse().ok()?,
            dosage_amount: vertex.properties.get("dosage_amount")?.as_str()?.to_string(),
            dosage_frequency: vertex.properties.get("dosage_frequency")?.as_str()?.to_string(),
            created_at: chrono::DateTime::parse_from_rfc3339(vertex.properties.get("created_at")?.as_str()?)
                .ok()?.with_timezone(&chrono::Utc),
            updated_at: vertex.properties.get("updated_at")
                .and_then(|v| v.as_str())
                .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
                .map(|dt| dt.with_timezone(&chrono::Utc)),
        })
    }
}
