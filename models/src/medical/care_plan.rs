// models/src/medical/care_plan.rs
use chrono::{DateTime, Utc};
use crate::{Vertex, ToVertex, identifiers::Identifier};

#[derive(Debug, Clone)]
pub struct CarePlan {
    pub id: i32,
    pub patient_id: i32,
    pub plan_name: String,
    pub start_date: DateTime<Utc>,
    pub end_date: Option<DateTime<Utc>>,
    pub status: String, // e.g., "Active", "Completed"
    pub goals: Option<String>, // JSON array of goals
    pub interventions: Option<String>, // JSON array of interventions
    pub created_by_doctor_id: i32,
}

impl ToVertex for CarePlan {
    fn to_vertex(&self) -> Vertex {
        let mut v = Vertex::new(Identifier::new("CarePlan".to_string()).unwrap());
        v.add_property("id", &self.id.to_string());
        v.add_property("patient_id", &self.patient_id.to_string());
        v.add_property("plan_name", &self.plan_name);
        v.add_property("start_date", &self.start_date.to_rfc3339());
        if let Some(ref val) = self.end_date {
            v.add_property("end_date", &val.to_rfc3339());
        }
        v.add_property("status", &self.status);
        if let Some(ref val) = self.goals {
            v.add_property("goals", val);
        }
        if let Some(ref val) = self.interventions {
            v.add_property("interventions", val);
        }
        v.add_property("created_by_doctor_id", &self.created_by_doctor_id.to_string());
        v
    }
}

impl CarePlan {
    pub fn from_vertex(vertex: &Vertex) -> Option<Self> {
        if vertex.label.as_ref() != "CarePlan" { return None; }
        Some(CarePlan {
            id: vertex.properties.get("id")?.as_str()?.parse().ok()?,
            patient_id: vertex.properties.get("patient_id")?.as_str()?.parse().ok()?,
            plan_name: vertex.properties.get("plan_name")?.as_str()?.to_string(),
            start_date: chrono::DateTime::parse_from_rfc3339(
                vertex.properties.get("start_date")?.as_str()?
            ).ok()?.with_timezone(&chrono::Utc),
            end_date: vertex.properties.get("end_date")
                .and_then(|v| v.as_str())
                .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
                .map(|dt| dt.with_timezone(&chrono::Utc)),
            status: vertex.properties.get("status")?.as_str()?.to_string(),
            goals: vertex.properties.get("goals").and_then(|v| v.as_str()).map(|s| s.to_string()),
            interventions: vertex.properties.get("interventions").and_then(|v| v.as_str()).map(|s| s.to_string()),
            created_by_doctor_id: vertex.properties.get("created_by_doctor_id")?.as_str()?.parse().ok()?,
        })
    }
}
