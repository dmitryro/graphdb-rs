use crate::definitions::VertexSchema;
use crate::constraints::{PropertyConstraint, EnumValues};
use crate::lifecycle::{LifecycleRule, MessagingSchema};
use crate::properties::OntologyReference;

/// Implementation of the VertexSchema for the Allergy vertex type.
///
/// This schema defines constraints for recording a patient's allergy or intolerance,
/// including the specific allergen, reaction details, severity, and verification status.
pub struct Allergy;

impl VertexSchema for Allergy {
    fn schema_name() -> &'static str {
        "Allergy"
    }

    /// Returns the list of property constraints for the Allergy vertex type.
    fn property_constraints() -> Vec<PropertyConstraint> {
        vec![
            // --- Core Fields from the Model ---
            PropertyConstraint::new("id", true)
                .with_description("Primary internal ID (i32). Required."),
            PropertyConstraint::new("patient_id", true)
                .with_description("Foreign key to the Patient vertex (i32). Required."),
            PropertyConstraint::new("allergen", true)
                .with_description("The substance causing the allergic reaction (String). Required."),

            PropertyConstraint::new("reaction", false)
                .with_description("Description of the physical reaction (String). Optional."),

            // Severity is constrained to common clinical values.
            // FIX: Removed the non-existent `with_enum_values` method call (E0599).
            PropertyConstraint::new("severity", true)
                .with_description("The clinical severity of the allergy (String). Must be one of: MILD, MODERATE, SEVERE, LIFE_THREATENING. Required."),

            PropertyConstraint::new("verified_by", false)
                .with_description("Identifier or name of the clinician who verified the allergy (String). Optional."),

            // The model uses DateTime<Utc>, which is a timestamp.
            PropertyConstraint::new("verified_date", false)
                .with_description("The date the allergy was clinically verified (Timestamp). Optional."),

            // Status is constrained to common clinical values.
            // FIX: Removed the non-existent `with_enum_values` method call (E0599).
            PropertyConstraint::new("status", true)
                .with_description("The current clinical status of the allergy (String). Must be one of: ACTIVE, INACTIVE, RESOLVED. Required."),

            PropertyConstraint::new("notes", false)
                .with_description("Additional clinical notes regarding the allergy (String). Optional."),

            // --- Audit Fields ---
            PropertyConstraint::new("created_at", true)
                .with_description("Creation timestamp. Required."),
            PropertyConstraint::new("updated_at", false)
                .with_description("Last update timestamp. Optional/System-managed."),
        ]
    }

    /// Defines lifecycle rules (empty for simplicity).
    fn lifecycle_rules() -> Vec<LifecycleRule> {
        vec![]
    }

    /// References to standard terminologies used for allergens and reactions.
    fn ontology_references() -> Vec<OntologyReference> {
        vec![
            OntologyReference {
                ontology_system_id: "RXNORM".to_string(),
                name: "RxNorm".to_string(),
                // FIX: description, uri, and reference_uri must be wrapped in Some() and reference_uri added.
                description: Some("Standardized naming system for generic and branded drugs. Useful for drug allergies.".to_string()),
                uri: Some("http://www.nlm.nih.gov/research/umls/rxnorm".to_string()),
                reference_uri: Some("http://www.nlm.nih.gov/research/umls/rxnorm".to_string()),
            },
            OntologyReference {
                ontology_system_id: "SNOMED_CT".to_string(),
                name: "Systematized Nomenclature of Medicine—Clinical Terms".to_string(),
                // FIX: description, uri, and reference_uri must be wrapped in Some() and reference_uri added.
                description: Some("Comprehensive terminology for clinical findings, symptoms, and diagnoses, including allergies.".to_string()),
                uri: Some("http://snomed.info/sct".to_string()),
                reference_uri: Some("http://snomed.info/sct".to_string()),
            },
        ]
    }

    /// Defines the messaging topics for Allergy CRUD operations.
    fn messaging_schema() -> MessagingSchema {
        MessagingSchema {
            creation_topic: Some("allergy.created".to_string()),
            update_topic: Some("allergy.updated".to_string()),
            deletion_topic: Some("allergy.deleted".to_string()),
            error_queue: Some("allergy.errors".to_string()),
        }
    }
}