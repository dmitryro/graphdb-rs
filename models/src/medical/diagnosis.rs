use chrono::NaiveDate;
use crate::{Vertex, ToVertex, identifiers::Identifier};

#[derive(Debug, Clone)]
pub struct Diagnosis {
    pub id: i32,
    pub patient_id: i32,
    pub doctor_id: i32,
    pub code_id: i32,
    pub description: String,
    pub date: NaiveDate,
}

impl ToVertex for Diagnosis {
    fn to_vertex(&self) -> Vertex {
        let mut vertex = Vertex::new(Identifier::new("Diagnosis".to_string()).unwrap());
        vertex.add_property("id", &self.id.to_string());
        vertex.add_property("patient_id", &self.patient_id.to_string());
        vertex.add_property("doctor_id", &self.doctor_id.to_string());
        vertex.add_property("code_id", &self.code_id.to_string());
        vertex.add_property("description", &self.description);
        vertex.add_property("date", &self.date.to_string());
        vertex
    }
}

impl Diagnosis {
    pub fn from_vertex(vertex: &Vertex) -> Option<Self> {
        if vertex.label.as_ref() != "Diagnosis" { return None; }
        Some(Diagnosis {
            id: vertex.properties.get("id")?.as_str()?.parse().ok()?,
            patient_id: vertex.properties.get("patient_id")?.as_str()?.parse().ok()?,
            doctor_id: vertex.properties.get("doctor_id")?.as_str()?.parse().ok()?,
            code_id: vertex.properties.get("code_id")?.as_str()?.parse().ok()?,
            description: vertex.properties.get("description")?.as_str()?.to_string(),
            date: chrono::NaiveDate::parse_from_str(
                vertex.properties.get("date")?.as_str()?,
                "%Y-%m-%d"
            ).ok()?,
        })
    }
}
