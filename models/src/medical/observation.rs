use chrono::{DateTime, Utc};
use crate::{Vertex, ToVertex, identifiers::Identifier};

#[derive(Debug, Clone)]
pub struct Observation {
    pub id: i32,
    pub encounter_id: i32,
    pub patient_id: i32,
    pub observation_type: String,
    pub value: String,
    pub unit: Option<String>,
    pub observed_at: DateTime<Utc>,
    pub observed_by_user_id: i32,
}

impl ToVertex for Observation {
    fn to_vertex(&self) -> Vertex {
        let mut v = Vertex::new(Identifier::new("Observation".to_string()).unwrap());
        v.add_property("id", &self.id.to_string());
        v.add_property("encounter_id", &self.encounter_id.to_string());
        v.add_property("patient_id", &self.patient_id.to_string());
        v.add_property("observation_type", &self.observation_type);
        v.add_property("value", &self.value);
        if let Some(ref val) = self.unit {
            v.add_property("unit", val);
        }
        v.add_property("observed_at", &self.observed_at.to_rfc3339());
        v.add_property("observed_by_user_id", &self.observed_by_user_id.to_string());
        v
    }
}

impl Observation {
    pub fn from_vertex(vertex: &Vertex) -> Option<Self> {
        if vertex.label.as_ref() != "Observation" { return None; }
        Some(Observation {
            id: vertex.properties.get("id")?.as_str()?.parse().ok()?,
            encounter_id: vertex.properties.get("encounter_id")?.as_str()?.parse().ok()?,
            patient_id: vertex.properties.get("patient_id")?.as_str()?.parse().ok()?,
            observation_type: vertex.properties.get("observation_type")?.as_str()?.to_string(),
            value: vertex.properties.get("value")?.as_str()?.to_string(),
            unit: vertex.properties.get("unit").and_then(|v| v.as_str()).map(|s| s.to_string()),
            observed_at: chrono::DateTime::parse_from_rfc3339(
                vertex.properties.get("observed_at")?.as_str()?
            ).ok()?.with_timezone(&chrono::Utc),
            observed_by_user_id: vertex.properties.get("observed_by_user_id")?.as_str()?.parse().ok()?,
        })
    }
}
