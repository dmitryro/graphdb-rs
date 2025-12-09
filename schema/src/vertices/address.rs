use crate::definitions::VertexSchema;
use crate::constraints::{PropertyConstraint, EnumValues, DataType, Constraint};
use crate::lifecycle::{LifecycleRule, MessagingSchema};
use crate::properties::OntologyReference;
use serde_json::Value as JsonValue;

/// Defines the schema for a standard physical address vertex.
/// This is typically used for shipping or location tracking.
pub struct Address;

impl VertexSchema for Address {
    fn schema_name() -> &'static str {
        "Address"
    }

    fn property_constraints() -> Vec<PropertyConstraint> {
        vec![
            PropertyConstraint::new("id", true)
                .with_description("Globally Unique Identifier (UUID) for the address.")
                .with_data_type(DataType::Uuid)
                .with_constraints(vec![Constraint::Required, Constraint::Unique, Constraint::Immutable]),

            PropertyConstraint::new("street1", true)
                .with_description("Primary street address line.")
                .with_data_type(DataType::String)
                .with_constraints(vec![Constraint::Required, Constraint::Mutable]),

            PropertyConstraint::new("street2", false)
                .with_description("Secondary street address line (apartment, suite, etc.).")
                .with_data_type(DataType::String)
                .with_constraints(vec![Constraint::Optional, Constraint::Mutable]),

            PropertyConstraint::new("city", true)
                .with_description("City or locality name.")
                .with_data_type(DataType::String)
                .with_constraints(vec![Constraint::Required, Constraint::Mutable]),

            PropertyConstraint::new("state_province", true)
                .with_description("State, province, or region code/name.")
                .with_data_type(DataType::String)
                .with_constraints(vec![Constraint::Required, Constraint::Mutable]),

            PropertyConstraint::new("postal_code", true)
                .with_description("Postal or ZIP code.")
                .with_data_type(DataType::String)
                .with_constraints(vec![Constraint::Required, Constraint::Mutable, Constraint::Indexable]),

            // FIX: Changed Constraint::Pattern to Constraint::Format
            PropertyConstraint::new("country", true)
                .with_description("The two or three-letter ISO country code (e.g., US, GBR).")
                .with_data_type(DataType::String)
                .with_constraints(vec![
                    Constraint::Required,
                    Constraint::Format(r"^[A-Z]{2,3}$".to_string()), // ISO country codes format check
                ]),

            PropertyConstraint::new("is_primary", true)
                .with_description("Flag indicating if this is the primary address for the linked entity.")
                .with_data_type(DataType::Boolean)
                .with_constraints(vec![Constraint::Required, Constraint::Mutable])
                .with_default_value(JsonValue::Bool(false)),
        ]
    }

    fn lifecycle_rules() -> Vec<LifecycleRule> {
        // Simple lifecycle for data entity
        vec![]
    }

    fn ontology_references() -> Vec<OntologyReference> {
        // References to geographic standards if needed
        vec![]
    }

    fn messaging_schema() -> MessagingSchema {
        MessagingSchema {
            creation_topic: Some("address.created".to_string()),
            update_topic: Some("address.updated".to_string()),
            deletion_topic: Some("address.deleted".to_string()),
            error_queue: Some("data.integrity_errors".to_string()),
        }
    }
}
