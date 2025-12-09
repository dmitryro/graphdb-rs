// models/src/medical/radiology_report.rs
use chrono::{DateTime, Utc};
use crate::{Vertex, ToVertex, identifiers::Identifier};

#[derive(Debug, Clone)]
pub struct RadiologyReport {
    pub id: i32,
    pub imaging_study_id: i32,
    pub findings: String,
    pub impression: String,
    pub recommendation: Option<String>,
    pub reported_at: DateTime<Utc>,
    pub radiologist_id: i32,
}

impl ToVertex for RadiologyReport {
    fn to_vertex(&self) -> Vertex {
        let mut v = Vertex::new(Identifier::new("RadiologyReport".to_string()).unwrap());
        v.add_property("id", &self.id.to_string());
        v.add_property("imaging_study_id", &self.imaging_study_id.to_string());
        v.add_property("findings", &self.findings);
        v.add_property("impression", &self.impression);
        if let Some(ref val) = self.recommendation {
            v.add_property("recommendation", val);
        }
        v.add_property("reported_at", &self.reported_at.to_rfc3339());
        v.add_property("radiologist_id", &self.radiologist_id.to_string());
        v
    }
}

impl RadiologyReport {
    pub fn from_vertex(vertex: &Vertex) -> Option<Self> {
        if vertex.label.as_ref() != "RadiologyReport" { return None; }
        Some(RadiologyReport {
            id: vertex.properties.get("id")?.as_str()?.parse().ok()?,
            imaging_study_id: vertex.properties.get("imaging_study_id")?.as_str()?.parse().ok()?,
            findings: vertex.properties.get("findings")?.as_str()?.to_string(),
            impression: vertex.properties.get("impression")?.as_str()?.to_string(),
            recommendation: vertex.properties.get("recommendation").and_then(|v| v.as_str()).map(|s| s.to_string()),
            reported_at: chrono::DateTime::parse_from_rfc3339(
                vertex.properties.get("reported_at")?.as_str()?
            ).ok()?.with_timezone(&chrono::Utc),
            radiologist_id: vertex.properties.get("radiologist_id")?.as_str()?.parse().ok()?,
        })
    }
}
