// models/src/medical/lab_panel.rs
use chrono::{DateTime, Utc};
use crate::{Vertex, ToVertex, identifiers::Identifier};

#[derive(Debug, Clone)]
pub struct LabPanel {
    pub id: i32,
    pub lab_order_id: i32,
    pub panel_name: String, // e.g., "CBC", "CMP"
    pub resulted_at: DateTime<Utc>,
    pub status: String, // e.g., "Preliminary", "Final"
}

impl ToVertex for LabPanel {
    fn to_vertex(&self) -> Vertex {
        let mut v = Vertex::new(Identifier::new("LabPanel".to_string()).unwrap());
        v.add_property("id", &self.id.to_string());
        v.add_property("lab_order_id", &self.lab_order_id.to_string());
        v.add_property("panel_name", &self.panel_name);
        v.add_property("resulted_at", &self.resulted_at.to_rfc3339());
        v.add_property("status", &self.status);
        v
    }
}

impl LabPanel {
    pub fn from_vertex(vertex: &Vertex) -> Option<Self> {
        if vertex.label.as_ref() != "LabPanel" { return None; }
        Some(LabPanel {
            id: vertex.properties.get("id")?.as_str()?.parse().ok()?,
            lab_order_id: vertex.properties.get("lab_order_id")?.as_str()?.parse().ok()?,
            panel_name: vertex.properties.get("panel_name")?.as_str()?.to_string(),
            resulted_at: chrono::DateTime::parse_from_rfc3339(
                vertex.properties.get("resulted_at")?.as_str()?
            ).ok()?.with_timezone(&chrono::Utc),
            status: vertex.properties.get("status")?.as_str()?.to_string(),
        })
    }
}
