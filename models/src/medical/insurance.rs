// insurance.rs
use chrono::{DateTime, Utc};
use crate::{Vertex, ToVertex, identifiers::Identifier};

#[derive(Debug, Clone)]
pub struct Insurance {
    pub id: i32,
    pub name: String,
    pub contact_info: String,
    pub coverage_details: Option<String>,
    pub claims_integration_status: String,
}

impl ToVertex for Insurance {
    fn to_vertex(&self) -> Vertex {
        let mut v = Vertex::new(Identifier::new("Insurance".to_string()).unwrap());
        v.add_property("id", &self.id.to_string());
        v.add_property("name", &self.name);
        v.add_property("contact_info", &self.contact_info);
        if let Some(ref val) = self.coverage_details {
            v.add_property("coverage_details", val);
        }
        v.add_property("claims_integration_status", &self.claims_integration_status);
        v
    }
}

impl Insurance {
    pub fn from_vertex(vertex: &Vertex) -> Option<Self> {
        if vertex.label.as_ref() != "Insurance" { return None; }
        Some(Insurance {
            id: vertex.properties.get("id")?.as_str()?.parse().ok()?,
            name: vertex.properties.get("name")?.as_str()?.to_string(),
            contact_info: vertex.properties.get("contact_info")?.as_str()?.to_string(),
            coverage_details: vertex.properties.get("coverage_details").and_then(|v| v.as_str()).map(|s| s.to_string()),
            claims_integration_status: vertex.properties.get("claims_integration_status")?.as_str()?.to_string(),
        })
    }
}
