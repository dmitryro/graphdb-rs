// models/src/medical/microbiology_culture.rs
use chrono::{DateTime, Utc};
use crate::{Vertex, ToVertex, identifiers::Identifier};

#[derive(Debug, Clone)]
pub struct MicrobiologyCulture {
    pub id: i32,
    pub lab_result_id: i32,
    pub culture_result: String, // e.g., "Positive", "Negative"
    pub organism_identified: Option<String>,
    pub sensitivity: Option<String>, // JSON or string for antibiogram
    pub resulted_at: DateTime<Utc>,
}

impl ToVertex for MicrobiologyCulture {
    fn to_vertex(&self) -> Vertex {
        let mut v = Vertex::new(Identifier::new("MicrobiologyCulture".to_string()).unwrap());
        v.add_property("id", &self.id.to_string());
        v.add_property("lab_result_id", &self.lab_result_id.to_string());
        v.add_property("culture_result", &self.culture_result);
        if let Some(ref val) = self.organism_identified {
            v.add_property("organism_identified", val);
        }
        if let Some(ref val) = self.sensitivity {
            v.add_property("sensitivity", val);
        }
        v.add_property("resulted_at", &self.resulted_at.to_rfc3339());
        v
    }
}

impl MicrobiologyCulture {
    pub fn from_vertex(vertex: &Vertex) -> Option<Self> {
        if vertex.label.as_ref() != "MicrobiologyCulture" { return None; }
        Some(MicrobiologyCulture {
            id: vertex.properties.get("id")?.as_str()?.parse().ok()?,
            lab_result_id: vertex.properties.get("lab_result_id")?.as_str()?.parse().ok()?,
            culture_result: vertex.properties.get("culture_result")?.as_str()?.to_string(),
            organism_identified: vertex.properties.get("organism_identified").and_then(|v| v.as_str()).map(|s| s.to_string()),
            sensitivity: vertex.properties.get("sensitivity").and_then(|v| v.as_str()).map(|s| s.to_string()),
            resulted_at: chrono::DateTime::parse_from_rfc3339(
                vertex.properties.get("resulted_at")?.as_str()?
            ).ok()?.with_timezone(&chrono::Utc),
        })
    }
}
