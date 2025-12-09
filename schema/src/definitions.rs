use crate::properties::{ PropertyDefinition, OntologyReference };
// Assuming these are needed for the VertexSchema trait definitions
use crate::lifecycle::{ LifecycleRule, MessagingSchema }; 
use crate::constraints::{ PropertyConstraint };
/// Defines the structure for a Vertex (Node) type in the graph.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VertexDefinition {
    /// The unique name of the vertex type (e.g., "Patient", "Product").
    pub name: String,
    /// A description of what the vertex represents.
    pub description: String,
    /// The properties allowed on this vertex type.
    pub properties: Vec<PropertyDefinition>,
}

/// Defines the structure for an Edge (Relationship) type in the graph.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EdgeDefinition {
    /// The unique name of the edge type (e.g., "CARES_FOR", "PURCHASED").
    pub name: String,
    /// A description of what the edge signifies.
    pub description: String,
    /// The properties allowed on this edge type.
    pub properties: Vec<PropertyDefinition>,
    /// Specifies which vertex types can be the starting point of this edge (e.g., [Patient, Doctor]).
    pub source_types: Vec<String>,
    /// Specifies which vertex types can be the ending point of this edge.
    pub target_types: Vec<String>,
}

/// Defines the structure for an Event type in the graph's time-series data layer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventDefinition {
    /// The unique name of the event type (e.g., "LoginAttempt", "SensorReading").
    pub name: String,
    /// A description of the event.
    pub description: String,
    /// The properties associated with the event.
    pub properties: Vec<PropertyDefinition>,
}

// NOTE: EventDefinition is currently defined in schema/src/events/mod.rs, but we
// will keep the overarching GraphSchema trait here since it references all three.

/// A trait for defining the specific schema, constraints, and behavior
/// for an individual Vertex type.
pub trait VertexSchema {
    /// The unique name of the schema element (e.g., "Patient").
    fn schema_name() -> &'static str;
    
    /// The list of constraints for properties on this element type.
    fn property_constraints() -> Vec<PropertyConstraint>;
    
    /// The list of lifecycle rules governing state transitions and actions.
    fn lifecycle_rules() -> Vec<LifecycleRule>;
    
    /// References to external ontology systems for properties.
    fn ontology_references() -> Vec<OntologyReference>;
    
    /// Defines the messaging configuration for this element type.
    fn messaging_schema() -> MessagingSchema;
}

/// The central trait for defining a graph's structure. All specific schema versions
/// (e.g., MedicalV1, FinanceV2) must implement this.
pub trait GraphSchema {
    /// The unique identifier for this schema (e.g., "MedicalV1").
    const VERSION: &'static str;
    
    /// The unique name of the schema element (e.g., "Patient", "MedicalGraph").
    fn schema_name() -> &'static str;
    
    /// Returns a list of all Node/Vertex definitions in the schema.
    fn vertex_definitions(&self) -> Vec<VertexDefinition>;
    
    /// Returns a list of all Edge definitions in the schema.
    fn edge_definitions(&self) -> Vec<EdgeDefinition>;
    
    /// Returns a list of all Event definitions in the schema.
    fn event_definitions(&self) -> Vec<EventDefinition>; 
}
