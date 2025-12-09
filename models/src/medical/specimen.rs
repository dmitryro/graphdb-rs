// models/src/medical/specimen.rs
use chrono::{DateTime, Utc};
use crate::{Vertex, ToVertex, identifiers::Identifier};

#[derive(Debug, Clone)]
pub struct Specimen {
    pub id: i32,
    pub lab_result_id: i32,
    pub type_: String, // e.g., "Blood", "Urine"
    pub collection_method: Option<String>,
    pub collected_at: DateTime<Utc>,
    pub collection_site: Option<String>, // e.g., "Left Arm"
}

impl ToVertex for Specimen {
    fn to_vertex(&self) -> Vertex {
        let mut v = Vertex::new(Identifier::new("Specimen".to_string()).unwrap());
        v.add_property("id", &self.id.to_string());
        v.add_property("lab_result_id", &self.lab_result_id.to_string());
        v.add_property("type", &self.type_);
        if let Some(ref val) = self.collection_method {
            v.add_property("collection_method", val);
        }
        v.add_property("collected_at", &self.collected_at.to_rfc3339());
        if let Some(ref val) = self.collection_site {
            v.add_property("collection_site", val);
        }
        v
    }
}

impl Specimen {
    pub fn from_vertex(vertex: &Vertex) -> Option<Self> {
        if vertex.label.as_ref() != "Specimen" { return None; }
        Some(Specimen {
            id: vertex.properties.get("id")?.as_str()?.parse().ok()?,
            lab_result_id: vertex.properties.get("lab_result_id")?.as_str()?.parse().ok()?,
            type_: vertex.properties.get("type")?.as_str()?.to_string(),
            collection_method: vertex.properties.get("collection_method").and_then(|v| v.as_str()).map(|s| s.to_string()),
            collected_at: chrono::DateTime::parse_from_rfc3339(
                vertex.properties.get("collected_at")?.as_str()?
            ).ok()?.with_timezone(&chrono::Utc),
            collection_site: vertex.properties.get("collection_site").and_then(|v| v.as_str()).map(|s| s.to_string()),
        })
    }
}
