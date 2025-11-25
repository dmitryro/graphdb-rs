use crate::{Vertex, ToVertex, identifiers::Identifier};

#[derive(Debug, Clone)]
pub struct Medication {
    pub id: i32,
    pub name: String,
    pub brand_name: Option<String>,
    pub generic_name: Option<String>,
    pub medication_class: String,
}

impl ToVertex for Medication {
    fn to_vertex(&self) -> Vertex {
        let mut v = Vertex::new(Identifier::new("Medication".to_string()).unwrap());
        v.add_property("id", &self.id.to_string());
        v.add_property("name", &self.name);
        if let Some(ref val) = self.brand_name {
            v.add_property("brand_name", val);
        }
        if let Some(ref val) = self.generic_name {
            v.add_property("generic_name", val);
        }
        v.add_property("medication_class", &self.medication_class);
        v
    }
}

impl Medication {
    pub fn from_vertex(vertex: &Vertex) -> Option<Self> {
        if vertex.label.as_ref() != "Medication" { return None; }
        Some(Medication {
            id: vertex.properties.get("id")?.as_str()?.parse().ok()?,
            name: vertex.properties.get("name")?.as_str()?.to_string(),
            brand_name: vertex.properties.get("brand_name").and_then(|v| v.as_str()).map(|s| s.to_string()),
            generic_name: vertex.properties.get("generic_name").and_then(|v| v.as_str()).map(|s| s.to_string()),
            medication_class: vertex.properties.get("medication_class")?.as_str()?.to_string(),
        })
    }
}
