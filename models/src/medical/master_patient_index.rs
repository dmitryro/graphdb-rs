// models/src/medical/master_patient_index.rs
use serde::{Deserialize, Serialize};
use std::{cmp::Ordering, fmt, str::FromStr};
use core::{hash::Hash, ops::Deref};
use bincode::{Encode, Decode, BorrowDecode};
use chrono::{DateTime, Utc};
use crate::{Vertex, ToVertex, identifiers::Identifier};
use crate::medical::Address;

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct MasterPatientIndex {
    pub id: i32,
    pub patient_id: Option<i32>,
    pub first_name: Option<String>,
    pub last_name: Option<String>,
    pub date_of_birth: Option<DateTime<Utc>>,
    pub gender: Option<String>,
    pub address: Option<Address>,
    pub contact_number: Option<String>,
    pub email: Option<String>,
    pub social_security_number: Option<String>,
    pub match_score: Option<f32>,
    pub match_date: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl ToVertex for MasterPatientIndex {
    fn to_vertex(&self) -> Vertex {
        let id_type = Identifier::new("MasterPatientIndex".to_string()).expect("Invalid Identifier");
        let mut v = Vertex::new(id_type);

        v.add_property("id", &self.id.to_string());

        if let Some(ref val) = self.patient_id {
            v.add_property("patient_id", &val.to_string());
        }
        if let Some(ref val) = self.first_name {
            v.add_property("first_name", val);
        }
        if let Some(ref val) = self.last_name {
            v.add_property("last_name", val);
        }
        if let Some(ref val) = self.date_of_birth {
            v.add_property("date_of_birth", &val.to_rfc3339());
        }
        if let Some(ref val) = self.gender {
            v.add_property("gender", val);
        }
        if let Some(ref val) = self.address {
            v.add_property("address", &serde_json::to_string(val).expect("Address serialize"));
        }
        if let Some(ref val) = self.contact_number {
            v.add_property("contact_number", val);
        }
        if let Some(ref val) = self.email {
            v.add_property("email", val);
        }
        if let Some(ref val) = self.social_security_number {
            v.add_property("ssn", val);
        }
        if let Some(ref val) = self.match_score {
            v.add_property("match_score", &val.to_string());
        }
        if let Some(ref val) = self.match_date {
            v.add_property("match_date", &val.to_rfc3339());
        }

        v.add_property("created_at", &self.created_at.to_rfc3339());
        v.add_property("updated_at", &self.updated_at.to_rfc3339());

        v
    }
}

impl MasterPatientIndex {
    /// Build a struct from a Vertex (mirror of ClinicalNote pattern)
    pub fn from_vertex(vertex: &Vertex) -> Option<Self> {
        if vertex.label.as_ref() != "MasterPatientIndex" {
            return None;
        }
        Some(MasterPatientIndex {
            id: vertex.properties.get("id")?.as_str()?.parse().ok()?,
            patient_id: vertex
                .properties
                .get("patient_id")
                .and_then(|v| v.as_str()?.parse().ok()),
            first_name: vertex
                .properties
                .get("first_name")
                .and_then(|v| v.as_str().map(String::from)),
            last_name: vertex
                .properties
                .get("last_name")
                .and_then(|v| v.as_str().map(String::from)),
            date_of_birth: vertex
                .properties
                .get("date_of_birth")
                .and_then(|v| DateTime::parse_from_rfc3339(v.as_str()?).ok())
                .map(|dt| dt.with_timezone(&Utc)),
            gender: vertex
                .properties
                .get("gender")
                .and_then(|v| v.as_str().map(String::from)),
            // NEW: deserialise Address from JSON string
            address: vertex.properties.get("address")
                .and_then(|v| v.as_str())
                .and_then(|s| serde_json::from_str::<Address>(s).ok()),
            contact_number: vertex
                .properties
                .get("contact_number")
                .and_then(|v| v.as_str().map(String::from)),
            email: vertex
                .properties
                .get("email")
                .and_then(|v| v.as_str().map(String::from)),
            social_security_number: vertex
                .properties
                .get("ssn")
                .and_then(|v| v.as_str().map(String::from)),
            match_score: vertex
                .properties
                .get("match_score")
                .and_then(|v| v.as_str()?.parse().ok()),
            match_date: vertex
                .properties
                .get("match_date")
                .and_then(|v| DateTime::parse_from_rfc3339(v.as_str()?).ok())
                .map(|dt| dt.with_timezone(&Utc)),
            created_at: DateTime::parse_from_rfc3339(
                vertex.properties.get("created_at")?.as_str()?
            )
            .ok()?
            .with_timezone(&Utc),
            updated_at: DateTime::parse_from_rfc3339(
                vertex.properties.get("updated_at")?.as_str()?
            )
            .ok()?
            .with_timezone(&Utc),
        })
    }
}
