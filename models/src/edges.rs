// models/src/edges.rs
use crate::identifiers::{Identifier, SerializableUuid};
use crate::properties::PropertyValue;
use serde::{Deserialize, Serialize};
use bincode::{Encode, Decode};
use std::collections::BTreeMap;

// Removed: unused import
// use uuid::Uuid;

/// A directed, typed edge connecting two vertices.
#[derive(Clone, Debug, Default, Eq, PartialEq, Hash, Ord, PartialOrd, Serialize, Deserialize, Encode, Decode)]
pub struct Edge {
    /// Auto-generated unique ID for the edge.
    pub id: SerializableUuid,

    /// Source vertex.
    pub outbound_id: SerializableUuid,

    /// Edge type (e.g., "FOLLOWS", "HAS_DIAGNOSIS").
    pub edge_type: Identifier,

    /// Target vertex.
    pub inbound_id: SerializableUuid,

    /// Human-readable label (optional, defaults to `t` as string).
    pub label: String,

    /// Edge properties (ordered for deterministic serialization).
    pub properties: BTreeMap<String, PropertyValue>,
}

impl Edge {
    /// Create a new edge with an auto-generated `id`.
    pub fn new(
        outbound_id: impl Into<SerializableUuid>,
        edge_type: Identifier,
        inbound_id: impl Into<SerializableUuid>,
    ) -> Self {
        let outbound_id = outbound_id.into();
        let inbound_id = inbound_id.into();
        let label = edge_type.to_string();

        Self {
            id: SerializableUuid(uuid::Uuid::new_v4()),
            outbound_id,
            edge_type,
            inbound_id,
            label,
            properties: BTreeMap::new(),
        }
    }

    pub fn with_property(mut self, key: impl Into<String>, value: PropertyValue) -> Self {
        self.properties.insert(key.into(), value);
        self
    }

    pub fn reversed(&self) -> Self {
        Self {
            id: self.id,
            outbound_id: self.inbound_id,
            edge_type: self.edge_type.clone(),
            inbound_id: self.outbound_id,
            label: self.label.clone(),
            properties: self.properties.clone(),
        }
    }
}
