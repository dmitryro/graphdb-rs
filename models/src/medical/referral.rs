// models/src/medical/referral.rs
use crate::{Vertex, ToVertex, identifiers::Identifier};
use chrono::{DateTime, Utc};

#[derive(Debug, Clone)]
pub struct Referral {
    pub id: i32,
    pub encounter_id: i32,
    pub patient_id: i32,
    pub referring_doctor_id: i32,
    pub referred_doctor_id: i32,
    pub specialty: String,
    pub reason: String,
    pub status: String, // "PENDING", "ACCEPTED", "DECLINED", "COMPLETED"
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl ToVertex for Referral {
    fn to_vertex(&self) -> Vertex {
        let mut vertex = Vertex::new(Identifier::new("Referral".to_string()).unwrap());
        vertex.add_property("id", &self.id.to_string());
        vertex.add_property("encounter_id", &self.encounter_id.to_string());
        vertex.add_property("patient_id", &self.patient_id.to_string());
        vertex.add_property("referring_doctor_id", &self.referring_doctor_id.to_string());
        vertex.add_property("referred_doctor_id", &self.referred_doctor_id.to_string());
        vertex.add_property("specialty", &self.specialty);
        vertex.add_property("reason", &self.reason);
        vertex.add_property("status", &self.status);
        vertex.add_property("created_at", &self.created_at.to_rfc3339());
        vertex.add_property("updated_at", &self.updated_at.to_rfc3339());
        vertex
    }
}

impl Referral {
    pub fn from_vertex(vertex: &Vertex) -> Option<Self> {
        if vertex.label.as_ref() != "Referral" {
            return None;
        }

        Some(Referral {
            id: vertex.properties.get("id")?.as_str()?.parse().ok()?,
            encounter_id: vertex.properties.get("encounter_id")?.as_str()?.parse().ok()?,
            patient_id: vertex.properties.get("patient_id")?.as_str()?.parse().ok()?,
            referring_doctor_id: vertex.properties.get("referring_doctor_id")?.as_str()?.parse().ok()?,
            referred_doctor_id: vertex.properties.get("referred_doctor_id")?.as_str()?.parse().ok()?,
            specialty: vertex.properties.get("specialty")?.as_str()?.to_string(),
            reason: vertex.properties.get("reason")?.as_str()?.to_string(),
            status: vertex.properties.get("status")?.as_str()?.to_string(),
            created_at: chrono::DateTime::parse_from_rfc3339(
                vertex.properties.get("created_at")?.as_str()?
            ).ok()?.with_timezone(&chrono::Utc),
            updated_at: chrono::DateTime::parse_from_rfc3339(
                vertex.properties.get("updated_at")?.as_str()?
            ).ok()?.with_timezone(&chrono::Utc),
        })
    }
}
