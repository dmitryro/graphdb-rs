// models/src/medical/order.rs
use chrono::{DateTime, Utc};
use uuid::Uuid;
use crate::{Vertex, ToVertex, identifiers::Identifier};

#[derive(Debug, Clone)]
pub struct Order {
    pub id: i32,
    pub encounter_id: Uuid,
    pub patient_id: i32,
    pub order_type: String,  // ADMIT, DIET, ACTIVITY, VITALS, IV_FLUIDS, MEDICATION, LAB, IMAGING, CONSULT
    pub order_details: String,
    pub diet: Option<String>,
    pub activity: Option<String>,
    pub vitals_frequency: Option<String>,
    pub iv_fluids: Option<String>,
    pub medications: Option<String>,
    pub labs: Option<String>,
    pub imaging: Option<String>,
    pub status: String,  // ORDERED, IN_PROGRESS, COMPLETED, CANCELLED, DISCONTINUED
    pub ordered_by: i32,  // provider_id
    pub ordered_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
    pub notes: Option<String>,
}

impl ToVertex for Order {
    fn to_vertex(&self) -> Vertex {
        let mut v = Vertex::new(Identifier::new("Order".to_string()).unwrap());
        v.add_property("id", &self.id.to_string());
        v.add_property("encounter_id", &self.encounter_id.to_string());
        v.add_property("patient_id", &self.patient_id.to_string());
        v.add_property("order_type", &self.order_type);
        v.add_property("order_details", &self.order_details);
        if let Some(ref val) = self.diet {
            v.add_property("diet", val);
        }
        if let Some(ref val) = self.activity {
            v.add_property("activity", val);
        }
        if let Some(ref val) = self.vitals_frequency {
            v.add_property("vitals_frequency", val);
        }
        if let Some(ref val) = self.iv_fluids {
            v.add_property("iv_fluids", val);
        }
        if let Some(ref val) = self.medications {
            v.add_property("medications", val);
        }
        if let Some(ref val) = self.labs {
            v.add_property("labs", val);
        }
        if let Some(ref val) = self.imaging {
            v.add_property("imaging", val);
        }
        v.add_property("status", &self.status);
        v.add_property("ordered_by", &self.ordered_by.to_string());
        v.add_property("ordered_at", &self.ordered_at.to_rfc3339());
        if let Some(ref val) = self.completed_at {
            v.add_property("completed_at", &val.to_rfc3339());
        }
        if let Some(ref val) = self.notes {
            v.add_property("notes", val);
        }
        v
    }
}

impl Order {
    pub fn from_vertex(vertex: &Vertex) -> Option<Self> {
        if vertex.label.as_ref() != "Order" { return None; }
        Some(Order {
            id: vertex.properties.get("id")?.as_str()?.parse().ok()?,
            encounter_id: Uuid::parse_str(vertex.properties.get("encounter_id")?.as_str()?).ok()?,
            patient_id: vertex.properties.get("patient_id")?.as_str()?.parse().ok()?,
            order_type: vertex.properties.get("order_type")?.as_str()?.to_string(),
            order_details: vertex.properties.get("order_details")?.as_str()?.to_string(),
            diet: vertex.properties.get("diet").and_then(|v| v.as_str()).map(|s| s.to_string()),
            activity: vertex.properties.get("activity").and_then(|v| v.as_str()).map(|s| s.to_string()),
            vitals_frequency: vertex.properties.get("vitals_frequency").and_then(|v| v.as_str()).map(|s| s.to_string()),
            iv_fluids: vertex.properties.get("iv_fluids").and_then(|v| v.as_str()).map(|s| s.to_string()),
            medications: vertex.properties.get("medications").and_then(|v| v.as_str()).map(|s| s.to_string()),
            labs: vertex.properties.get("labs").and_then(|v| v.as_str()).map(|s| s.to_string()),
            imaging: vertex.properties.get("imaging").and_then(|v| v.as_str()).map(|s| s.to_string()),
            status: vertex.properties.get("status")?.as_str()?.to_string(),
            ordered_by: vertex.properties.get("ordered_by")?.as_str()?.parse().ok()?,
            ordered_at: chrono::DateTime::parse_from_rfc3339(
                vertex.properties.get("ordered_at")?.as_str()?
            ).ok()?.with_timezone(&chrono::Utc),
            completed_at: vertex.properties.get("completed_at")
                .and_then(|v| v.as_str())
                .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
                .map(|dt| dt.with_timezone(&chrono::Utc)),
            notes: vertex.properties.get("notes").and_then(|v| v.as_str()).map(|s| s.to_string()),
        })
    }
}
