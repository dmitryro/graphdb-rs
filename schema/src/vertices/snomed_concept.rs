// schema/src/vertices/snomed_concept.rs
use crate::definitions::VertexSchema;
use crate::constraints::{PropertyConstraint, Constraint};
use crate::lifecycle::{
    LifecycleRule, StateTransition, MessagingSchema, SchemaAction
};
use crate::properties::OntologyReference;

/// Implementation of the VertexSchema for the SNOMED_Concept vertex type.
///
/// This schema defines constraints for SNOMED CT concepts, including the code, display term,
/// system identifier, and optional version. Hierarchy and attributes are handled via edges.
pub struct SNOMED_Concept;

impl VertexSchema for SNOMED_Concept {
    fn schema_name() -> &'static str {
        "SNOMED_Concept"
    }

    /// Returns the list of property constraints for the SNOMED_Concept vertex type.
    fn property_constraints() -> Vec<PropertyConstraint> {
        vec![
            // --- Core Fields from the Model ---
            PropertyConstraint::new("id", true)
                .with_description("Primary internal ID (Uuid). Required."),
            PropertyConstraint::new("code", true)
                .with_description("SNOMED CT code (e.g., '1234567890'). Required."),
            PropertyConstraint::new("display", true)
                .with_description("Human-readable term/description. Required."),
            PropertyConstraint::new("system", true)
                .with_description("Ontology system (e.g., 'SNOMED_CT'). Required."),
            PropertyConstraint::new("version", false)
                .with_description("Specific SNOMED CT version (e.g., '2023-07-31'). Optional."),
        ]
    }

    /// Defines lifecycle rules (empty for simplicity, as concepts are static).
    fn lifecycle_rules() -> Vec<LifecycleRule> {
        vec![]
    }

    /// References to the SNOMED CT ontology itself (self-referential).
    fn ontology_references() -> Vec<OntologyReference> {
        vec![
            OntologyReference {
                name: "SNOMED_CT_Core".to_string(),
                ontology_system_id: "SNOMED_CT".to_string(),
                uri: Some("http://snomed.info/sct".to_string()),
                reference_uri: Some("http://snomed.info/sct".to_string()),
                description: Some("Core SNOMED CT terminology for clinical concepts.".to_string()),
            },
        ]
    }

    /// Defines the messaging topics for SNOMED_Concept CRUD operations (minimal, as concepts are imported).
    fn messaging_schema() -> MessagingSchema {
        MessagingSchema {
            creation_topic: Some("snomed_concept.created".to_string()),
            update_topic: Some("snomed_concept.updated".to_string()),
            deletion_topic: Some("snomed_concept.deleted".to_string()),
            error_queue: Some("snomed_concept.errors".to_string()),
        }
    }
}
