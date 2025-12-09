use serde_json::Value;
use crate::constraints::{ PropertyConstraint, DataType };

/// Defines the fields for linking a property to an external ontology or standard.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OntologyReference {
    /// A unique name/identifier for this ontology reference
    pub name: String,
    /// The ID of the ontology system (e.g., "FHIR_AdministrativeGender")
    pub ontology_system_id: String,
    /// The URI reference to the ontology (e.g., "http://hl7.org/fhir/ValueSet/administrative-gender")
    pub uri: Option<String>, // Made optional for flexibility
    /// Alternative name for URI (kept for backward compatibility and is now optional)
    pub reference_uri: Option<String>, // REQUIRED FIX: Made optional to align with struct usage (Some(...))
    /// Optional description of this ontology reference
    pub description: Option<String>,
}

/// Defines a specific property allowed on a Vertex, Edge, or Event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PropertyDefinition {
    /// The unique name of the property.
    pub property_name: String,
    /// The expected data type of the property.
    pub data_type: DataType,
    /// Whether this property must be present.
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
