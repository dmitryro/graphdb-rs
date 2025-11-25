// models/src/medical/doctor.rs
use crate::{Vertex, ToVertex, identifiers::Identifier};

#[derive(Debug, Clone)]
pub struct Doctor {
    pub id: i32,
    pub first_name: String,
    pub last_name: String,
    pub phone: String,
    pub email: String,
    pub specialization: String,
    pub license_number: String,
}

impl ToVertex for Doctor {
    fn to_vertex(&self) -> Vertex {
        let mut vertex = Vertex::new(Identifier::new("Doctor".to_string()).unwrap());
        vertex.add_property("id", &self.id.to_string());
        vertex.add_property("first_name", &self.first_name);
        vertex.add_property("last_name", &self.last_name);
        vertex.add_property("phone", &self.phone);
        vertex.add_property("email", &self.email);
        vertex.add_property("specialization", &self.specialization);
        vertex.add_property("license_number", &self.license_number);
        vertex
    }
}

impl Doctor {
    /// Convert a Vertex back into a Doctor
    /// Returns None if the vertex is not a Doctor or data is malformed
    pub fn from_vertex(vertex: &Vertex) -> Option<Self> {
        // Check label
        if vertex.label.as_ref() != "Doctor" {
            return None;
        }

        // Extract required fields with safe parsing
        let id = vertex.properties.get("id")?.as_str()?.parse().ok()?;
        let first_name = vertex.properties.get("first_name")?.as_str()?.to_string();
        let last_name = vertex.properties.get("last_name")?.as_str()?.to_string();
        let phone = vertex.properties.get("phone")?.as_str()?.to_string();
        let email = vertex.properties.get("email")?.as_str()?.to_string();
        let specialization = vertex.properties.get("specialization")?.as_str()?.to_string();
        let license_number = vertex.properties.get("license_number")?.as_str()?.to_string();

        Some(Doctor {
            id,
            first_name,
            last_name,
            phone,
            email,
            specialization,
            license_number,
        })
    }
}
