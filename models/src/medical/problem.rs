// models/src/medical/problem.rs
use chrono::{DateTime, Utc};
use crate::{Vertex, ToVertex, identifiers::Identifier};

#[derive(Debug, Clone)]
pub struct Problem {
    pub id: i32,
    pub patient_id: i32,
    pub problem: String,
    pub icd10_code: Option<String>,
    pub status: String,  // ACTIVE, RESOLVED, IMPROVING, WORSENING, STABLE, CHRONIC
    pub onset_date: Option<DateTime<Utc>>,
    pub resolved_date: Option<DateTime<Utc>>,
    pub severity: Option<String>,  // MILD, MODERATE, SEVERE
    pub notes: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl ToVertex for Problem {
    fn to_vertex(&self) -> Vertex {
        let mut v = Vertex::new(Identifier::new("Problem".to_string()).unwrap());
        v.add_property("id", &self.id.to_string());
        v.add_property("patient_id", &self.patient_id.to_string());
        v.add_property("problem", &self.problem);
        if let Some(ref val) = self.icd10_code {
            v.add_property("icd10_code", val);
        }
        v.add_property("status", &self.status);
        if let Some(ref val) = self.onset_date {
            v.add_property("onset_date", &val.to_rfc3339());
        }
        if let Some(ref val) = self.resolved_date {
            v.add_property("resolved_date", &val.to_rfc3339());
        }
        if let Some(ref val) = self.severity {
            v.add_property("severity", val);
        }
        if let Some(ref val) = self.notes {
            v.add_property("notes", val);
        }
        v.add_property("created_at", &self.created_at.to_rfc3339());
        v.add_property("updated_at", &self.updated_at.to_rfc3339());
        v
    }
}

impl Problem {
    pub fn from_vertex(vertex: &Vertex) -> Option<Self> {
        if vertex.label.as_ref() != "Problem" { return None; }
        Some(Problem {
            id: vertex.properties.get("id")?.as_str()?.parse().ok()?,
            patient_id: vertex.properties.get("patient_id")?.as_str()?.parse().ok()?,
            problem: vertex.properties.get("problem")?.as_str()?.to_string(),
            icd10_code: vertex.properties.get("icd10_code").and_then(|v| v.as_str()).map(|s| s.to_string()),
            status: vertex.properties.get("status")?.as_str()?.to_string(),
            onset_date: vertex.properties.get("onset_date")
                .and_then(|v| v.as_str())
                .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
                .map(|dt| dt.with_timezone(&chrono::Utc)),
            resolved_date: vertex.properties.get("resolved_date")
                .and_then(|v| v.as_str())
                .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
                .map(|dt| dt.with_timezone(&chrono::Utc)),
            severity: vertex.properties.get("severity").and_then(|v| v.as_str()).map(|s| s.to_string()),
            notes: vertex.properties.get("notes").and_then(|v| v.as_str()).map(|s| s.to_string()),
            created_at: chrono::DateTime::parse_from_rfc3339(
                vertex.properties.get("created_at")?.as_str()?
            ).ok()?.with_timezone(&chrono::Utc),
            updated_at: chrono::DateTime::parse_from_rfc3339(
                vertex.properties.get("updated_at")?.as_str()?
            ).ok()?.with_timezone(&chrono::Utc),
        })
    }
}
