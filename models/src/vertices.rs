// models/src/vertices.rs
use std::collections::HashMap;
use serde::{Deserialize, Serialize};
use bincode::{Encode, Decode};
use chrono::{DateTime, Utc};

use crate::{
    identifiers::{Identifier, SerializableUuid},
    properties::PropertyValue,
    timestamp::BincodeDateTime, // <-- ADD THIS LINE
};

// FIX 1: Added 'Default' to the derive list.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize, Encode, Decode)]
pub struct Vertex {
    pub id: SerializableUuid,
    pub label: Identifier,
    pub properties: HashMap<String, PropertyValue>,

    pub created_at: BincodeDateTime,
    pub updated_at: BincodeDateTime,
}

impl Vertex {
    pub fn new(label: Identifier) -> Self {
        // Note: The conversion from Utc::now() to BincodeDateTime uses .into()
        let now = Utc::now().into();
        Vertex {
            id: SerializableUuid(uuid::Uuid::new_v4()),
            label,
            properties: HashMap::new(),
            created_at: now,
            updated_at: now,
        }
    }

    pub fn new_with_id(id: impl Into<SerializableUuid>, label: Identifier) -> Self {
        let now = Utc::now().into();
        Vertex {
            id: id.into(),
            label,
            properties: HashMap::new(),
            created_at: now,
            updated_at: now,
        }
    }

    /// Creates a new Vertex using a specific ID and Label.
    /// 
    /// NOTE: This method is functionally identical to `new_with_id`.
    /// It is added to satisfy the calling convention in `golden_record.rs` 
    /// and to offer a clearer name when both ID and Label are provided.
    pub fn new_with_label(id: impl Into<SerializableUuid>, label: Identifier) -> Self {
        let now = Utc::now().into();
        Vertex {
            id: id.into(),
            label,
            properties: HashMap::new(),
            created_at: now,
            updated_at: now,
        }
    }

    pub fn label(&self) -> &Identifier {
        &self.label
    }

    pub fn id(&self) -> &SerializableUuid {
        &self.id
    }

    pub fn add_property(&mut self, key: &str, value: &str) {
        self.properties.insert(key.to_string(), PropertyValue::String(value.to_string()));
    }

    pub fn get_property(&self, key: &str) -> Option<&str> {
        self.properties.get(key)
            .and_then(|prop_val| {
                match prop_val {
                    PropertyValue::String(s) => Some(s.as_str()),
                    _ => None,
                }
            })
    }
}
