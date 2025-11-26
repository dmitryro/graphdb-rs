// models/src/medical/appointment.rs
use chrono::{DateTime, Utc};
use crate::{Vertex, ToVertex, identifiers::Identifier};

#[derive(Debug, Clone)]
pub struct Appointment {
    pub id: i32,
    pub patient_id: i32,
    pub provider: String,
    pub appointment_type: String,  // FOLLOW_UP, NEW_PATIENT, URGENT, PROCEDURE, etc.
    pub scheduled_time: DateTime<Utc>,
    pub duration_minutes: i32,
    pub reason: Option<String>,
    pub status: String,  // SCHEDULED, CONFIRMED, CHECKED_IN, COMPLETED, CANCELLED, NO_SHOW
    pub location: Option<String>,
    pub notes: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl ToVertex for Appointment {
    fn to_vertex(&self) -> Vertex {
        let mut v = Vertex::new(Identifier::new("Appointment".to_string()).unwrap());
        v.add_property("id", &self.id.to_string());
        v.add_property("patient_id", &self.patient_id.to_string());
        v.add_property("provider", &self.provider);
        v.add_property("appointment_type", &self.appointment_type);
        v.add_property("scheduled_time", &self.scheduled_time.to_rfc3339());
        v.add_property("duration_minutes", &self.duration_minutes.to_string());
        if let Some(ref val) = self.reason {
            v.add_property("reason", val);
        }
        v.add_property("status", &self.status);
        if let Some(ref val) = self.location {
            v.add_property("location", val);
        }
        if let Some(ref val) = self.notes {
            v.add_property("notes", val);
        }
        v.add_property("created_at", &self.created_at.to_rfc3339());
        v.add_property("updated_at", &self.updated_at.to_rfc3339());
        v
    }
}

impl Appointment {
    pub fn from_vertex(vertex: &Vertex) -> Option<Self> {
        if vertex.label.as_ref() != "Appointment" { return None; }
        Some(Appointment {
            id: vertex.properties.get("id")?.as_str()?.parse().ok()?,
            patient_id: vertex.properties.get("patient_id")?.as_str()?.parse().ok()?,
            provider: vertex.properties.get("provider")?.as_str()?.to_string(),
            appointment_type: vertex.properties.get("appointment_type")?.as_str()?.to_string(),
            scheduled_time: chrono::DateTime::parse_from_rfc3339(
                vertex.properties.get("scheduled_time")?.as_str()?
            ).ok()?.with_timezone(&chrono::Utc),
            duration_minutes: vertex.properties.get("duration_minutes")?.as_str()?.parse().ok()?,
            reason: vertex.properties.get("reason").and_then(|v| v.as_str()).map(|s| s.to_string()),
            status: vertex.properties.get("status")?.as_str()?.to_string(),
            location: vertex.properties.get("location").and_then(|v| v.as_str()).map(|s| s.to_string()),
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
