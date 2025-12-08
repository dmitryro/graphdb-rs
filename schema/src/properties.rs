use serde_json::Value;
use crate::constraints::PropertyConstraint; 
/// Defines the supported data types within the graph schema.
// Renamed to DataType to match usage in patient.rs error trace.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DataType {
    Boolean,
    Integer,
    Float,
    String,
    Uuid,
    Byte,
    Date,
    DateTime,
    Timestamp, // Added to support `date_of_birth` type
    Json,      // For complex, unstructured data
}

/// Defines the fields for linking a property to an external ontology or standard.
/// Now includes the fields you were trying to assign.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OntologyReference {
    pub ontology_system_id: String,
    pub reference_uri: String,
}

/// Defines a specific property allowed on a Vertex, Edge, or Event.
/// Fields updated to match your attempted usage in patient.rs, allowing compilation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PropertyDefinition {
    /// The unique name of the property.
    pub property_name: String,
    /// The expected data type of the property.
    pub data_type: DataType,
    /// Whether this property must be present (renamed from `is_required`).
    pub required: bool,
    /// Whether this property must be unique across all vertices of this type.
    pub is_unique: bool, 
    /// Default value for the property, using `Value` to accommodate `json!()`.
    pub default_value: Option<Value>,
    /// Optional reference to an external ontology.
    pub ontology_ref: Option<OntologyReference>,
    /// Optional additional constraints. (Currently a marker)
    pub constraints: Option<PropertyConstraint>, 
}

// Re-export the corrected DataType enum
pub use DataType as SchemaDataType;
