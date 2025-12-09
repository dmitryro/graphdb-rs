// models/src/medical/lab_result.rs
use chrono::{DateTime, Utc};
use crate::{Vertex, ToVertex, identifiers::Identifier};

#[derive(Debug, Clone)]
pub struct LabResult {
    pub id: i32,
    pub patient_id: i32,
    pub lab_order_id: Option<i32>,
    pub test_name: String,
    pub value: String,
    pub unit: Option<String>,
    pub reference_range: Option<String>,
    pub abnormal_flag: Option<String>, // e.g., "H", "L", "Normal"
    pub resulted_at: DateTime<Utc>,
    pub notes: Option<String>,
}

impl ToVertex for LabResult {
    fn to_vertex(&self) -> Vertex {
        let mut v = Vertex::new(Identifier::new("LabResult".to_string()).unwrap());
        v.add_property("id", &self.id.to_string());
        v.add_property("patient_id", &self.patient_id.to_string());
        if let Some(ref val) = self.lab_order_id {
            v.add_property("lab_order_id", &val.to_string());
        }
        v.add_property("test_name", &self.test_name);
        v.add_property("value", &self.value);
        if let Some(ref val) = self.unit {
            v.add_property("unit", val);
        }
        if let Some(ref val) = self.reference_range {
            v.add_property("reference_range", val);
        }
        if let Some(ref val) = self.abnormal_flag {
            v.add_property("abnormal_flag", val);
        }
        v.add_property("resulted_at", &self.resulted_at.to_rfc3339());
        if let Some(ref val) = self.notes {
            v.add_property("notes", val);
        }
        v
    }
}

impl LabResult {
    pub fn from_vertex(vertex: &Vertex) -> Option<Self> {
        if vertex.label.as_ref() != "LabResult" { return None; }
        Some(LabResult {
            id: vertex.properties.get("id")?.as_str()?.parse().ok()?,
            patient_id: vertex.properties.get("patient_id")?.as_str()?.parse().ok()?,
            lab_order_id: vertex.properties.get("lab_order_id").and_then(|v| v.as_str()).and_then(|s| s.parse().ok()),
            test_name: vertex.properties.get("test_name")?.as_str()?.to_string(),
            value: vertex.properties.get("value")?.as_str()?.to_string(),
            unit: vertex.properties.get("unit").and_then(|v| v.as_str()).map(|s| s.to_string()),
            reference_range: vertex.properties.get("reference_range").and_then(|v| v.as_str()).map(|s| s.to_string()),
            abnormal_flag: vertex.properties.get("abnormal_flag").and_then(|v| v.as_str()).map(|s| s.to_string()),
            resulted_at: chrono::DateTime::parse_from_rfc3339(
                vertex.properties.get("resulted_at")?.as_str()?
            ).ok()?.with_timezone(&chrono::Utc),
            notes: vertex.properties.get("notes").and_then(|v| v.as_str()).map(|s| s.to_string()),
        })
    }
}
