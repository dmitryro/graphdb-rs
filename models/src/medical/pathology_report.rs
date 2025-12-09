// models/src/medical/pathology_report.rs
use chrono::{DateTime, Utc};
use crate::{Vertex, ToVertex, identifiers::Identifier};

#[derive(Debug, Clone)]
pub struct PathologyReport {
    pub id: i32,
    pub patient_id: i32,
    pub gross_description: Option<String>,
    pub microscopic_description: Option<String>,
    pub immunohistochemistry: Option<String>, // e.g., JSON for stains
    pub molecular_test: Option<String>, // e.g., JSON for results
    pub reported_at: DateTime<Utc>,
    pub pathologist_id: i32,
}

impl ToVertex for PathologyReport {
    fn to_vertex(&self) -> Vertex {
        let mut v = Vertex::new(Identifier::new("PathologyReport".to_string()).unwrap());
        v.add_property("id", &self.id.to_string());
        v.add_property("patient_id", &self.patient_id.to_string());
        if let Some(ref val) = self.gross_description {
            v.add_property("gross_description", val);
        }
        if let Some(ref val) = self.microscopic_description {
            v.add_property("microscopic_description", val);
        }
        if let Some(ref val) = self.immunohistochemistry {
            v.add_property("immunohistochemistry", val);
        }
        if let Some(ref val) = self.molecular_test {
            v.add_property("molecular_test", val);
        }
        v.add_property("reported_at", &self.reported_at.to_rfc3339());
        v.add_property("pathologist_id", &self.pathologist_id.to_string());
        v
    }
}

impl PathologyReport {
    pub fn from_vertex(vertex: &Vertex) -> Option<Self> {
        if vertex.label.as_ref() != "PathologyReport" { return None; }
        Some(PathologyReport {
            id: vertex.properties.get("id")?.as_str()?.parse().ok()?,
            patient_id: vertex.properties.get("patient_id")?.as_str()?.parse().ok()?,
            gross_description: vertex.properties.get("gross_description").and_then(|v| v.as_str()).map(|s| s.to_string()),
            microscopic_description: vertex.properties.get("microscopic_description").and_then(|v| v.as_str()).map(|s| s.to_string()),
            immunohistochemistry: vertex.properties.get("immunohistochemistry").and_then(|v| v.as_str()).map(|s| s.to_string()),
            molecular_test: vertex.properties.get("molecular_test").and_then(|v| v.as_str()).map(|s| s.to_string()),
            reported_at: chrono::DateTime::parse_from_rfc3339(
                vertex.properties.get("reported_at")?.as_str()?
            ).ok()?.with_timezone(&chrono::Utc),
            pathologist_id: vertex.properties.get("pathologist_id")?.as_str()?.parse().ok()?,
        })
    }
}
