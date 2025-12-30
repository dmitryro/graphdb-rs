// models/src/properties.rs
use chrono::{DateTime, NaiveDateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;
use bincode::{Encode, Decode };
use bincode::error::{EncodeError, DecodeError};
use std::hash::{Hash, Hasher};
use std::cmp::Ordering;
use serde_json::{json, Value };
use crate::{edges::Edge, identifiers::Identifier, json::Json, vertices::Vertex};

/// --- NEW STRUCT FOR F64 WRAPPER ---
/// f64 does not implement `Eq` or `Hash` directly.
/// We need a newtype wrapper to implement these traits manually.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, Encode, Decode)]
#[serde(transparent)]
pub struct SerializableFloat(pub f64);

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct SerializableDateTime(pub DateTime<Utc>);

impl bincode::Encode for SerializableDateTime {
    fn encode<E: bincode::enc::Encoder>(&self, encoder: &mut E) -> Result<(), bincode::error::EncodeError> {
        self.0.timestamp().encode(encoder)?;
        (self.0.timestamp_nanos() % 1_000_000_000i64).encode(encoder)?;
        Ok(())
    }
}

impl<C> bincode::Decode<C> for SerializableDateTime {
    fn decode<D>(decoder: &mut D) -> Result<Self, bincode::error::DecodeError>
    where
        D: bincode::de::Decoder<Context = C>,
    {
        let secs: i64 = bincode::Decode::decode(decoder)?;
        let nanos: i64 = bincode::Decode::decode(decoder)?;
        let nanos_u32 = (nanos as u32).min(999_999_999);

        let naive = NaiveDateTime::from_timestamp_opt(secs, nanos_u32)
            .ok_or_else(|| bincode::error::DecodeError::OtherString("Invalid timestamp".to_string()))?;

        let dt = DateTime::from_utc(naive, Utc);
        Ok(SerializableDateTime(dt))
    }
}

impl<'de, C> bincode::BorrowDecode<'de, C> for SerializableDateTime {
    fn borrow_decode<D>(decoder: &mut D) -> Result<Self, bincode::error::DecodeError>
    where
        D: bincode::de::BorrowDecoder<'de, Context = C>,
    {
        Self::decode(decoder)
    }
}

// Assuming SerializableDateTime is defined as: 
// pub struct SerializableDateTime(pub DateTime<Utc>);

impl From<DateTime<Utc>> for SerializableDateTime {
    fn from(dt: DateTime<Utc>) -> Self {
        SerializableDateTime(dt)
    }
}

// Optional but recommended: Implement the reverse for easy extraction
impl From<SerializableDateTime> for DateTime<Utc> {
    fn from(sdt: SerializableDateTime) -> Self {
        sdt.0
    }
}

impl PartialEq for SerializableFloat {
    fn eq(&self, other: &Self) -> bool {
        self.0.to_bits() == other.0.to_bits()
    }
}
impl Eq for SerializableFloat {}

impl PartialOrd for SerializableFloat {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for SerializableFloat {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.to_bits().cmp(&other.0.to_bits())
    }
}
impl std::hash::Hash for SerializableFloat {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.0.to_bits().hash(state);
    }
}
/// --- END NEW STRUCT ---

/// A map of property names to their values.
pub type PropertyMap = HashMap<Identifier, PropertyValue>;

// =========================================================================
// --- WRAPPERS FOR TRAIT COMPLIANCE ---

/// This wrapper allows Vertex to be contained in PropertyValue without breaking derives.
/// It implements Hash, PartialOrd, and Ord based on the inner Vertex's ID for stability.
#[derive(Clone, Debug, Serialize, Deserialize, Encode, Decode)]
pub struct UnhashableVertex(pub Vertex);

impl PartialEq for UnhashableVertex {
    fn eq(&self, other: &Self) -> bool {
        self.0.eq(&other.0)
    }
}
impl Eq for UnhashableVertex {}

impl Hash for UnhashableVertex {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.id.hash(state); 
    }
}

impl PartialOrd for UnhashableVertex {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.0.id.partial_cmp(&other.0.id)
    }
}

impl Ord for UnhashableVertex {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.id.cmp(&other.0.id)
    }
}


/// Wraps PropertyMap to implement Hash, PartialOrd, and Ord deterministically,
/// which is necessary because HashMap does not implement these traits.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Encode, Decode)]
pub struct HashablePropertyMap(pub PropertyMap);

impl Hash for HashablePropertyMap {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // To hash deterministically, we must sort the key-value pairs by key.
        let mut sorted_pairs: Vec<(&Identifier, &PropertyValue)> = self.0.iter().collect();
        sorted_pairs.sort_by(|(k1, _), (k2, _)| k1.cmp(k2));

        for (key, value) in sorted_pairs {
            key.hash(state);
            value.hash(state);
        }
    }
}

impl PartialOrd for HashablePropertyMap {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HashablePropertyMap {
    fn cmp(&self, other: &Self) -> Ordering {
        // 1. Compare lengths.
        let len_cmp = self.0.len().cmp(&other.0.len());
        if len_cmp != Ordering::Equal {
            return len_cmp;
        }

        // 2. Sort key-value pairs and compare element by element.
        let mut self_pairs: Vec<(&Identifier, &PropertyValue)> = self.0.iter().collect();
        let mut other_pairs: Vec<(&Identifier, &PropertyValue)> = other.0.iter().collect();
        
        self_pairs.sort_by(|(k1, _), (k2, _)| k1.cmp(k2));
        other_pairs.sort_by(|(k1, _), (k2, _)| k1.cmp(k2));

        for ((k1, v1), (k2, v2)) in self_pairs.iter().zip(other_pairs.iter()) {
            let key_cmp = k1.cmp(k2);
            if key_cmp != Ordering::Equal {
                return key_cmp;
            }
            let value_cmp = v1.cmp(v2);
            if value_cmp != Ordering::Equal {
                return value_cmp;
            }
        }
        
        Ordering::Equal
    }
}

// =========================================================================

/// Represents a generic property value.
///
/// NOTE: Only types supported by bincode derive Encode/Decode.
/// `Uuid` is supported via SerializableUuid.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize, Encode, Decode)]
#[serde(untagged)]
pub enum PropertyValue {
    Null, // Added to fix E0599
    Boolean(bool),
    Integer(i64),
    I32(i32),
    Float(SerializableFloat),
   /// DateTime is placed above String so ISO-8601 strings 
    /// are correctly typed for temporal graph queries.
    /// Use the wrapper to satisfy Encode/Decode trait bounds for Sled storage
    DateTime(SerializableDateTime),
    String(String),
    Uuid(crate::identifiers::SerializableUuid),
    Byte(u8),
    Map(HashablePropertyMap),
    Vertex(UnhashableVertex),
    List(Vec<PropertyValue>), // Replaces 'Array' concept
}

impl PropertyValue {
    pub fn as_str(&self) -> Option<&str> {
        match self {
            PropertyValue::String(s) => Some(s),
            _ => None,
        }
    }

    pub fn as_i32(&self) -> Option<&i32> {
        match self {
            PropertyValue::I32(v) => Some(v),
            _ => None,
        }
    }

    pub fn as_i64(&self) -> Option<&i64> {
        match self {
            PropertyValue::Integer(v) => Some(v),
            _ => None,
        }
    }

    /// Helper to access the internal list if the variant is a List
    pub fn as_list(&self) -> Option<&Vec<PropertyValue>> {
        match self {
            PropertyValue::List(l) => Some(l),
            _ => None,
        }
    }

    pub fn to_serde_value(&self) -> serde_json::Value {
        use serde_json::Value;
        match self {
            PropertyValue::Null => Value::Null,

            PropertyValue::Boolean(b) => Value::Bool(*b),
            PropertyValue::Integer(i) => Value::Number((*i).into()),
            PropertyValue::I32(i) => Value::Number((*i).into()),

            PropertyValue::Float(f) => Value::from(f.0),

            PropertyValue::String(s) => Value::String(s.clone()),
            PropertyValue::Byte(b) => Value::Number((*b as i64).into()),
            PropertyValue::Uuid(u) => Value::String(u.0.to_string()),

            PropertyValue::DateTime(dt) => Value::String(dt.0.to_rfc3339()),

            PropertyValue::List(items) => {
                let vec = items
                    .iter()
                    .map(|item| item.to_serde_value())
                    .collect::<Vec<Value>>();
                Value::Array(vec)
            }

            PropertyValue::Map(m) => {
                let map = m
                    .0
                    .iter()
                    .map(|(k, v)| (k.to_string(), v.to_serde_value()))
                    .collect::<serde_json::Map<String, Value>>();
                Value::Object(map)
            }

            PropertyValue::Vertex(v) => {
                serde_json::to_value(&v.0).unwrap_or(Value::Null)
            }
        }
    }
}

impl From<String> for PropertyValue {
    fn from(s: String) -> Self { PropertyValue::String(s) }
}
impl From<&str> for PropertyValue {
    fn from(s: &str) -> Self { PropertyValue::String(s.to_string()) }
}
impl From<i64> for PropertyValue {
    fn from(i: i64) -> Self { PropertyValue::Integer(i) }
}
impl From<f64> for PropertyValue {
    fn from(f: f64) -> Self { PropertyValue::Float(SerializableFloat(f)) }
}
impl From<bool> for PropertyValue {
    fn from(b: bool) -> Self { PropertyValue::Boolean(b) }
}
impl From<Uuid> for PropertyValue {
    fn from(u: Uuid) -> Self { PropertyValue::Uuid(u.into()) }
}
impl From<crate::identifiers::SerializableUuid> for PropertyValue {
    fn from(u: crate::identifiers::SerializableUuid) -> Self { PropertyValue::Uuid(u) }
}


/// Represents a vertex property.
/// NOT bincode-Encode/Decode as Json (serde_json::Value) is not bincode compatible.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct VertexProperty {
    pub id: Uuid,
    pub value: Json,
}
impl VertexProperty {
    pub fn new(id: Uuid, value: Json) -> Self { Self { id, value } }
}

/// A named property, for both vertices and edges.
/// NOT bincode-Encode/Decode due to Json.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct NamedProperty {
    pub name: Identifier,
    pub value: Json,
}
impl NamedProperty {
    pub fn new(name: Identifier, value: Json) -> Self { Self { name, value } }
}

/// A vertex with properties.
/// NOT bincode-Encode/Decode due to Json in props.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct VertexProperties {
    pub vertex: Vertex,
    pub props: Vec<NamedProperty>,
}
impl VertexProperties {
    pub fn new(vertex: Vertex, props: Vec<NamedProperty>) -> Self { VertexProperties { vertex, props } }
}

/// An edge with properties.
/// NOT bincode-Encode/Decode due to Json in props.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct EdgeProperties {
    pub edge: Edge,
    pub props: Vec<NamedProperty>,
}
impl EdgeProperties {
    pub fn new(edge: Edge, props: Vec<NamedProperty>) -> Self { EdgeProperties { edge, props } }
}

/// Represents an edge property.
/// NOT bincode-Encode/Decode due to Json.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct EdgeProperty {
    pub edge: Edge,
    pub value: Json,
}
impl EdgeProperty {
    pub fn new(edge: Edge, value: Json) -> Self { Self { edge, value } }
}

/// Represents a vertex property update event.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct VertexPropertyUpdate {
    pub id: Uuid,
    pub property: Identifier,
    pub value: Option<Json>,
}
impl VertexPropertyUpdate {
    pub fn new(id: Uuid, property: Identifier, value: Option<Json>) -> Self {
        Self { id, property, value }
    }
}

/// Represents an edge property update event.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct EdgePropertyUpdate {
    pub edge: Edge,
    pub property: Identifier,
    pub value: Option<Json>,
}
impl EdgePropertyUpdate {
    pub fn new(edge: Edge, property: Identifier, value: Option<Json>) -> Self {
        Self { edge, property, value }
    }
}
