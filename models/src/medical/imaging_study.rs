// models/src/medical/imaging_study.rs
use chrono::{DateTime, Utc};
use crate::{Vertex, ToVertex, identifiers::Identifier};

#[derive(Debug, Clone)]
pub struct ImagingStudy {
    pub id: i32,
    pub patient_id: i32,
    pub imaging_order_id: Option<i32>,
    pub modality: String,
    pub protocol: Option<String>,
    pub body_part: String,
    pub contrast: Option<String>, // e.g., "With IV Contrast"
    pub performed_at: DateTime<Utc>,
    pub status: String,
}

impl ToVertex for ImagingStudy {
    fn to_vertex(&self) -> Vertex {
        let mut v = Vertex::new(Identifier::new("ImagingStudy".to_string()).unwrap());
        v.add_property("id", &self.id.to_string());
        v.add_property("patient_id", &self.patient_id.to_string());
        if let Some(ref val) = self.imaging_order_id {
            v.add_property("imaging_order_id", &val.to_string());
        }
        v.add_property("modality", &self.modality);
        if let Some(ref val) = self.protocol {
            v.add_property("protocol", val);
        }
        v.add_property("body_part", &self.body_part);
        if let Some(ref val) = self.contrast {
            v.add_property("contrast", val);
        }
        v.add_property("performed_at", &self.performed_at.to_rfc3339());
        v.add_property("status", &self.status);
        v
    }
}

impl ImagingStudy {
    pub fn from_vertex(vertex: &Vertex) -> Option<Self> {
        if vertex.label.as_ref() != "ImagingStudy" { return None; }
        Some(ImagingStudy {
            id: vertex.properties.get("id")?.as_str()?.parse().ok()?,
            patient_id: vertex.properties.get("patient_id")?.as_str()?.parse().ok()?,
            imaging_order_id: vertex.properties.get("imaging_order_id").and_then(|v| v.as_str()).and_then(|s| s.parse().ok()),
            modality: vertex.properties.get("modality")?.as_str()?.to_string(),
            protocol: vertex.properties.get("protocol").and_then(|v| v.as_str()).map(|s| s.to_string()),
            body_part: vertex.properties.get("body_part")?.as_str()?.to_string(),
            contrast: vertex.properties.get("contrast").and_then(|v| v.as_str()).map(|s| s.to_string()),
            performed_at: chrono::DateTime::parse_from_rfc3339(
                vertex.properties.get("performed_at")?.as_str()?
            ).ok()?.with_timezone(&chrono::Utc),
            status: vertex.properties.get("status")?.as_str()?.to_string(),
        })
    }
}
