use chrono::{DateTime, Utc};
use crate::{Vertex, ToVertex, identifiers::Identifier};

#[derive(Debug, Clone)]
pub struct Patient {
    pub id: i32,
    pub user_id: Option<i32>,
    pub first_name: String,
    pub last_name: String,
    pub date_of_birth: DateTime<Utc>,
    pub gender: String,
    pub address: Option<String>,
    pub phone: Option<String>,
    pub email: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl ToVertex for Patient {
    fn to_vertex(&self) -> Vertex {
        let mut v = Vertex::new(Identifier::new("Patient".to_string()).unwrap());
        v.add_property("id", &self.id.to_string());
        if let Some(ref val) = self.user_id {
            v.add_property("user_id", &val.to_string());
        }
        v.add_property("first_name", &self.first_name);
        v.add_property("last_name", &self.last_name);
        v.add_property("date_of_birth", &self.date_of_birth.to_rfc3339());
        v.add_property("gender", &self.gender);
        if let Some(ref val) = self.address {
            v.add_property("address", val);
        }
        if let Some(ref val) = self.phone {
            v.add_property("phone", val);
        }
        if let Some(ref val) = self.email {
            v.add_property("email", val);
        }
        v.add_property("created_at", &self.created_at.to_rfc3339());
        v.add_property("updated_at", &self.updated_at.to_rfc3339());
        v
    }
}

impl Patient {
    pub fn from_vertex(vertex: &Vertex) -> Option<Self> {
        if vertex.label.as_ref() != "Patient" { return None; }
        Some(Patient {
            id: vertex.properties.get("id")?.as_str()?.parse().ok()?,
            user_id: vertex.properties.get("user_id").and_then(|v| v.as_str()).and_then(|s| s.parse().ok()),
            first_name: vertex.properties.get("first_name")?.as_str()?.to_string(),
            last_name: vertex.properties.get("last_name")?.as_str()?.to_string(),
            date_of_birth: chrono::DateTime::parse_from_rfc3339(
                vertex.properties.get("date_of_birth")?.as_str()?
            ).ok()?.with_timezone(&chrono::Utc),
            gender: vertex.properties.get("gender")?.as_str()?.to_string(),
            address: vertex.properties.get("address").and_then(|v| v.as_str()).map(|s| s.to_string()),
            phone: vertex.properties.get("phone").and_then(|v| v.as_str()).map(|s| s.to_string()),
            email: vertex.properties.get("email").and_then(|v| v.as_str()).map(|s| s.to_string()),
            created_at: chrono::DateTime::parse_from_rfc3339(
                vertex.properties.get("created_at")?.as_str()?
            ).ok()?.with_timezone(&chrono::Utc),
            updated_at: chrono::DateTime::parse_from_rfc3339(
                vertex.properties.get("updated_at")?.as_str()?
            ).ok()?.with_timezone(&chrono::Utc),
        })
    }
}
