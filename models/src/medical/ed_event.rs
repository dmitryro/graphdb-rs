// models/src/medical/ed_event.rs
// Rationale: This model specializes the generic Event for the emergency department,
// allowing more granular tracking of ED-specific occurrences linked directly to an
// Encounter and potentially other entities
use chrono::{DateTime, Utc};
// FIX: Changed `crate::models::{...}` to `crate::{...}`
use crate::{Vertex, ToVertex, identifiers::Identifier};

#[derive(Debug, Clone)]
pub struct EdEvent {
    pub id: i32,
    pub encounter_id: i32, // Links to an existing Encounter (specific to this ED visit)
    pub event_type: String, // e.g., "Medication Administered", "Procedure Performed", "Consult Requested", "Imaging Ordered", "Critical Lab Result", "Patient Deterioration"
    pub event_description: Option<String>, // More specific details than type
    pub associated_entity_id: Option<i32>, // Generic ID for linking (e.g., Medication.id, EdProcedure.id, LabResult.id)
    pub occurred_at: DateTime<Utc>,
    pub recorded_by_user_id: i32, // Links to a User (could be Doctor, Nurse, etc.)
}

impl ToVertex for EdEvent {
    fn to_vertex(&self) -> Vertex {
        let mut v = Vertex::new(Identifier::new("EdEvent".to_string()).unwrap());
        v.add_property("id", &self.id.to_string());
        v.add_property("encounter_id", &self.encounter_id.to_string());
        v.add_property("event_type", &self.event_type);
        if let Some(ref val) = self.event_description {
            v.add_property("event_description", val);
        }
        if let Some(ref val) = self.associated_entity_id {
            v.add_property("associated_entity_id", &val.to_string());
        }
        v.add_property("occurred_at", &self.occurred_at.to_rfc3339());
        v.add_property("recorded_by_user_id", &self.recorded_by_user_id.to_string());
        v
    }
}

impl EdEvent {
    pub fn from_vertex(vertex: &Vertex) -> Option<Self> {
        if vertex.label.as_ref() != "EdEvent" { return None; }
        Some(EdEvent {
            id: vertex.properties.get("id")?.as_str()?.parse().ok()?,
            encounter_id: vertex.properties.get("encounter_id")?.as_str()?.parse().ok()?,
            event_type: vertex.properties.get("event_type")?.as_str()?.to_string(),
            event_description: vertex.properties.get("event_description").and_then(|v| v.as_str()).map(|s| s.to_string()),
            associated_entity_id: vertex.properties.get("associated_entity_id").and_then(|v| v.as_str()).and_then(|s| s.parse().ok()),
            occurred_at: chrono::DateTime::parse_from_rfc3339(vertex.properties.get("occurred_at")?.as_str()?)
                .ok()?.with_timezone(&chrono::Utc),
            recorded_by_user_id: vertex.properties.get("recorded_by_user_id")?.as_str()?.parse().ok()?,
        })
    }
}
