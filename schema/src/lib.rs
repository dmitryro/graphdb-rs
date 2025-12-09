// schema/src/lib.rs

use serde::{Deserialize, Serialize};
use crate::constraints::{Constraint, PropertyConstraint};
use crate::definitions::{ EventDefinition };
use crate::lifecycle::{LifecycleRule, MessagingSchema};
use crate::service::SchemaService; // Import the service

// --- CORE TRAITS ---

/// A trait that all graph elements (Vertex, Edge) must implement to declare their schema.
pub trait GraphSchema: Send + Sync {
    /// The unique type name of the element (e.g., "Patient", "HAS_ALLERGY").
    fn schema_name() -> &'static str;

    /// The list of required and optional properties, and their constraints.
    fn property_constraints() -> Vec<PropertyConstraint>;

    /// The lifecycle rules that govern state transitions, events, and actions.
    fn lifecycle_rules() -> Vec<LifecycleRule>;

    /// Ontology or terminology references (e.g., for medical coding).
    fn ontology_references() -> Vec<OntologyReference>;

    /// Messaging configuration (queues, topics) related to this element's creation/update/deletion.
    fn messaging_schema() -> MessagingSchema;
}

/// A trait for schema elements that can be dynamically extended or modified.
pub trait ExtendableSchema {
    /// Adds a new property constraint dynamically.
    fn add_property_constraint(constraint: PropertyConstraint) -> Result<(), SchemaError>;

    /// Modifies an existing lifecycle rule.
    fn modify_lifecycle_rule(rule: LifecycleRule) -> Result<(), SchemaError>;
}

// --- CORE STRUCTS ---

/// Represents a single external ontology or terminology reference used by a schema property.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OntologyReference {
    pub property_name: String,
    pub system_id: String, // e.g., "SNOMED_CT", "ICD_10"
    pub reference_uri: String, // URI for the specific code system
    pub required: bool,
}

/// A centralized error type for schema validation and operations.
#[derive(Debug)]
pub enum SchemaError {
    InvalidSchemaName,
    ConstraintViolation(String),
    LifecycleRuleError(String),
    ExtensionError(String),
    // ... other error types
}

impl std::fmt::Display for SchemaError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SchemaError::InvalidSchemaName => write!(f, "Invalid schema element name."),
            SchemaError::ConstraintViolation(msg) => write!(f, "Constraint violation: {}", msg),
            SchemaError::LifecycleRuleError(msg) => write!(f, "Lifecycle rule error: {}", msg),
            SchemaError::ExtensionError(msg) => write!(f, "Schema extension error: {}", msg),
        }
    }
}

// Re-export the modules
pub mod constraints;
pub mod lifecycle;
pub mod ontology;
pub mod extension;
pub mod events;
pub mod rules;
pub mod service; // <-- EXPOSED MODULE
pub mod migration;
pub mod vertices;
pub mod edges;
pub mod properties;
pub mod definitions;
pub mod errors;

// Re-export SchemaService initialization functions
pub use service::{init_schema_service, get_schema_service};
pub use vertices::*;
pub use edges::*;
pub use rules::*;
pub use events::*;
pub use migration::*;
pub use lifecycle::*;
pub use ontology::*;
pub use extension::*;
pub use constraints::*;
pub use properties::*;
pub use errors::*;   
pub use definitions::*;
