use chrono::{DateTime, Utc};
use crate::{Vertex, ToVertex, identifiers::Identifier};

#[derive(Debug, Clone)]
pub struct Immunization {
    pub id: i32,
    pub patient_id: i32,
    pub vaccine_name: String,
    pub administration_date: DateTime<Utc>,
    pub administered_by: Option<i32>,
    pub notes: Option<String>,
}

impl ToVertex for Immunization {
    fn to_vertex(&self) -> Vertex {
        let mut vertex = Vertex::new(Identifier::new("Immunization".to_string()).unwrap());

        vertex.add_property("id", &self.id.to_string());
        vertex.add_property("patient_id", &self.patient_id.to_string());
        vertex.add_property("vaccine_name", &self.vaccine_name);
        vertex.add_property("administration_date", &self.administration_date.to_rfc3339());
        if let Some(ref adm) = self.administered_by {
            vertex.add_property("administered_by", &adm.to_string());
        }
        if let Some(ref n) = self.notes {
            vertex.add_property("notes", n);
        }

        vertex
    }
}

impl Immunization {
    pub fn from_vertex(vertex: &Vertex) -> Option<Self> {
        if vertex.label.as_ref() != "Immunization" { return None; }
        Some(Immunization {
            id: vertex.properties.get("id")?.as_str()?.parse().ok()?,
            patient_id: vertex.properties.get("patient_id")?.as_str()?.parse().ok()?,
            vaccine_name: vertex.properties.get("vaccine_name")?.as_str()?.to_string(),
            administration_date: chrono::DateTime::parse_from_rfc3339(vertex.properties.get("administration_date")?.as_str()?)
                .ok()?.with_timezone(&chrono::Utc),
            administered_by: vertex.properties.get("administered_by").and_then(|v| v.as_str()).and_then(|s| s.parse().ok()),
            notes: vertex.properties.get("notes").and_then(|v| v.as_str()).map(|s| s.to_string()),
        })
    }
}
