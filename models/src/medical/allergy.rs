// models/src/medical/allergy.rs
use chrono::{DateTime, Utc};
use crate::{Vertex, ToVertex, identifiers::Identifier};

#[derive(Debug, Clone)]
pub struct Allergy {
    pub id: i32,
    pub patient_id: i32,
    pub allergen: String,
    pub reaction: Option<String>,
    pub severity: String,  // MILD, MODERATE, SEVERE, LIFE_THREATENING
    pub verified_by: Option<String>,
    pub verified_date: Option<DateTime<Utc>>,
    pub status: String,  // ACTIVE, INACTIVE, RESOLVED
    pub notes: Option<String>,
    pub created_at: DateTime<Utc>,
}

impl ToVertex for Allergy {
    fn to_vertex(&self) -> Vertex {
        let mut v = Vertex::new(Identifier::new("Allergy".to_string()).unwrap());
        v.add_property("id", &self.id.to_string());
        v.add_property("patient_id", &self.patient_id.to_string());
        v.add_property("allergen", &self.allergen);
        if let Some(ref val) = self.reaction {
            v.add_property("reaction", val);
        }
        v.add_property("severity", &self.severity);
        if let Some(ref val) = self.verified_by {
            v.add_property("verified_by", val);
        }
        if let Some(ref val) = self.verified_date {
            v.add_property("verified_date", &val.to_rfc3339());
        }
        v.add_property("status", &self.status);
        if let Some(ref val) = self.notes {
            v.add_property("notes", val);
        }
        v.add_property("created_at", &self.created_at.to_rfc3339());
        v
    }
}

impl Allergy {
    pub fn from_vertex(vertex: &Vertex) -> Option<Self> {
        if vertex.label.as_ref() != "Allergy" { return None; }
        Some(Allergy {
            id: vertex.properties.get("id")?.as_str()?.parse().ok()?,
            patient_id: vertex.properties.get("patient_id")?.as_str()?.parse().ok()?,
            allergen: vertex.properties.get("allergen")?.as_str()?.to_string(),
            reaction: vertex.properties.get("reaction").and_then(|v| v.as_str()).map(|s| s.to_string()),
            severity: vertex.properties.get("severity")?.as_str()?.to_string(),
            verified_by: vertex.properties.get("verified_by").and_then(|v| v.as_str()).map(|s| s.to_string()),
            verified_date: vertex.properties.get("verified_date")
                .and_then(|v| v.as_str())
                .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
                .map(|dt| dt.with_timezone(&chrono::Utc)),
            status: vertex.properties.get("status")?.as_str()?.to_string(),
            notes: vertex.properties.get("notes").and_then(|v| v.as_str()).map(|s| s.to_string()),
            created_at: chrono::DateTime::parse_from_rfc3339(
                vertex.properties.get("created_at")?.as_str()?
            ).ok()?.with_timezone(&chrono::Utc),
        })
    }
}
