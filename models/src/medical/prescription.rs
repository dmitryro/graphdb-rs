use chrono::{DateTime, Utc};
use crate::{Vertex, ToVertex, identifiers::Identifier};

#[derive(Debug, Clone)]
pub struct Prescription {
    pub id: i32,
    pub patient_id: i32,
    pub doctor_id: i32,
    pub medication_name: String,
    pub dose: String,
    pub frequency: String,
    pub start_date: DateTime<Utc>,
    pub end_date: Option<DateTime<Utc>>,
}

impl ToVertex for Prescription {
    fn to_vertex(&self) -> Vertex {
        let mut vertex = Vertex::new(Identifier::new("Prescription".to_string()).unwrap());
        vertex.add_property("id", &self.id.to_string());
        vertex.add_property("patient_id", &self.patient_id.to_string());
        vertex.add_property("doctor_id", &self.doctor_id.to_string());
        vertex.add_property("medication_name", &self.medication_name);
        vertex.add_property("dose", &self.dose);
        vertex.add_property("frequency", &self.frequency);
        vertex.add_property("start_date", &self.start_date.to_rfc3339());
        if let Some(ref v) = self.end_date {
            vertex.add_property("end_date", &v.to_rfc3339());
        }
        vertex
    }
}

impl Prescription {
    pub fn from_vertex(vertex: &Vertex) -> Option<Self> {
        if vertex.label.as_ref() != "Prescription" { return None; }
        Some(Prescription {
            id: vertex.properties.get("id")?.as_str()?.parse().ok()?,
            patient_id: vertex.properties.get("patient_id")?.as_str()?.parse().ok()?,
            doctor_id: vertex.properties.get("doctor_id")?.as_str()?.parse().ok()?,
            medication_name: vertex.properties.get("medication_name")?.as_str()?.to_string(),
            dose: vertex.properties.get("dose")?.as_str()?.to_string(),
            frequency: vertex.properties.get("frequency")?.as_str()?.to_string(),
            start_date: chrono::DateTime::parse_from_rfc3339(vertex.properties.get("start_date")?.as_str()?)
                .ok()?.with_timezone(&chrono::Utc),
            end_date: vertex.properties.get("end_date")
                .and_then(|v| v.as_str())
                .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
                .map(|dt| dt.with_timezone(&chrono::Utc)),
        })
    }
}
