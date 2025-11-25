// models/src/medical/triage.rs
use chrono::{DateTime, Utc};
// FIX: Changed `crate::models::{...}` to `crate::{...}`
use crate::{Vertex, ToVertex, identifiers::Identifier};

#[derive(Debug, Clone)]
pub struct Triage {
    pub id: i32,
    pub encounter_id: i32, // Links to an existing Encounter
    pub patient_id: i32, // Redundant if always linked to Encounter, but useful for direct patient queries
    pub triage_nurse_id: i32, // Links to a Nurse
    pub triage_level: String, // e.g., "ESI 3", "Urgent", "Emergent"
    pub chief_complaint: String,
    pub presenting_symptoms: Option<String>, // Consider a more structured type (e.g., Vec<String>) for future
    pub pain_score: Option<i32>, // e.g., 0-10
    pub triage_notes: Option<String>,
    pub assessed_at: DateTime<Utc>,
}

impl ToVertex for Triage {
    fn to_vertex(&self) -> Vertex {
        let mut v = Vertex::new(Identifier::new("Triage".to_string()).unwrap());
        v.add_property("id", &self.id.to_string());
        v.add_property("encounter_id", &self.encounter_id.to_string());
        v.add_property("patient_id", &self.patient_id.to_string());
        v.add_property("triage_nurse_id", &self.triage_nurse_id.to_string());
        v.add_property("triage_level", &self.triage_level);
        v.add_property("chief_complaint", &self.chief_complaint);
        if let Some(ref val) = self.presenting_symptoms {
            v.add_property("presenting_symptoms", val);
        }
        if let Some(ref val) = self.pain_score {
            v.add_property("pain_score", &val.to_string());
        }
        if let Some(ref val) = self.triage_notes {
            v.add_property("triage_notes", val);
        }
        v.add_property("assessed_at", &self.assessed_at.to_rfc3339());
        v
    }
}

impl Triage {
    pub fn from_vertex(vertex: &Vertex) -> Option<Self> {
        if vertex.label.as_ref() != "Triage" {
            return None;
        }

        Some(Triage {
            id: vertex.properties.get("id")?.as_str()?.parse().ok()?,
            encounter_id: vertex.properties.get("encounter_id")?.as_str()?.parse().ok()?,
            patient_id: vertex.properties.get("patient_id")?.as_str()?.parse().ok()?,
            triage_nurse_id: vertex.properties.get("triage_nurse_id")?.as_str()?.parse().ok()?,
            triage_level: vertex.properties.get("triage_level")?.as_str()?.to_string(),
            chief_complaint: vertex.properties.get("chief_complaint")?.as_str()?.to_string(),
            presenting_symptoms: vertex.properties.get("presenting_symptoms").and_then(|v| v.as_str()).map(|s| s.to_string()),
            pain_score: vertex.properties.get("pain_score").and_then(|v| v.as_str()).and_then(|s| s.parse().ok()),
            triage_notes: vertex.properties.get("triage_notes").and_then(|v| v.as_str()).map(|s| s.to_string()),
            assessed_at: chrono::DateTime::parse_from_rfc3339(
                vertex.properties.get("assessed_at")?.as_str()?
            ).ok()?.with_timezone(&chrono::Utc),
        })
    }
}
