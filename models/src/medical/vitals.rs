use chrono::{DateTime, Utc};
use crate::{Vertex, ToVertex, identifiers::Identifier};

#[derive(Debug, Clone)]
pub struct Vitals {
    pub id: i32,
    pub patient_id: i32,
    pub weight: Option<f32>,
    pub height: Option<f32>,
    pub blood_pressure_systolic: Option<i32>,
    pub blood_pressure_diastolic: Option<i32>,
    pub temperature: Option<f32>,
    pub heart_rate: Option<i32>,
    pub created_at: DateTime<Utc>,
}

impl ToVertex for Vitals {
    fn to_vertex(&self) -> Vertex {
        let mut vertex = Vertex::new(Identifier::new("Vitals".to_string()).unwrap());
        vertex.add_property("id", &self.id.to_string());
        vertex.add_property("patient_id", &self.patient_id.to_string());
        if let Some(weight) = self.weight {
            vertex.add_property("weight", &weight.to_string());
        }
        if let Some(height) = self.height {
            vertex.add_property("height", &height.to_string());
        }
        if let Some(systolic) = self.blood_pressure_systolic {
            vertex.add_property("blood_pressure_systolic", &systolic.to_string());
        }
        if let Some(diastolic) = self.blood_pressure_diastolic {
            vertex.add_property("blood_pressure_diastolic", &diastolic.to_string());
        }
        if let Some(temp) = self.temperature {
            vertex.add_property("temperature", &temp.to_string());
        }
        if let Some(hr) = self.heart_rate {
            vertex.add_property("heart_rate", &hr.to_string());
        }
        vertex.add_property("created_at", &self.created_at.to_rfc3339());
        vertex
    }
}

impl Vitals {
    pub fn from_vertex(vertex: &Vertex) -> Option<Self> {
        if vertex.label.as_ref() != "Vitals" { return None; }
        Some(Vitals {
            id: vertex.properties.get("id")?.as_str()?.parse().ok()?,
            patient_id: vertex.properties.get("patient_id")?.as_str()?.parse().ok()?,
            weight: vertex.properties.get("weight").and_then(|v| v.as_str()).and_then(|s| s.parse().ok()),
            height: vertex.properties.get("height").and_then(|v| v.as_str()).and_then(|s| s.parse().ok()),
            blood_pressure_systolic: vertex.properties.get("blood_pressure_systolic").and_then(|v| v.as_str()).and_then(|s| s.parse().ok()),
            blood_pressure_diastolic: vertex.properties.get("blood_pressure_diastolic").and_then(|v| v.as_str()).and_then(|s| s.parse().ok()),
            temperature: vertex.properties.get("temperature").and_then(|v| v.as_str()).and_then(|s| s.parse().ok()),
            heart_rate: vertex.properties.get("heart_rate").and_then(|v| v.as_str()).and_then(|s| s.parse().ok()),
            created_at: chrono::DateTime::parse_from_rfc3339(
                vertex.properties.get("created_at")?.as_str()?
            ).ok()?.with_timezone(&chrono::Utc),
        })
    }
}
