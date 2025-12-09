use serde_json::json;

use crate::definitions::VertexSchema;
use crate::properties::OntologyReference;
use crate::constraints::{PropertyConstraint, EnumValues}; // Added EnumValues
use crate::lifecycle::{
    LifecycleRule, StateTransition, MessagingSchema, SchemaAction
};

/// Implementation of the VertexSchema for the Patient vertex type.
/// The schema defines the required properties, lifecycle rules, and messaging topics
/// for the Patient vertex, reflecting the comprehensive fields in the underlying model struct.
pub struct Patient;

impl VertexSchema for Patient {
    fn schema_name() -> &'static str {
        "Patient"
    }

    /// Returns the list of property constraints for the Patient vertex type.
    fn property_constraints() -> Vec<PropertyConstraint> {
        vec![
            // --- Primary Identifiers ---
            PropertyConstraint::new("id", true)
                .with_description("Primary internal ID (i32). Required."),
            PropertyConstraint::new("user_id", false)
                .with_description("Foreign key to user account (Optional i32)."),
            PropertyConstraint::new("mrn", false)
                .with_description("Medical Record Number (Optional String). Highly recommended and often unique."),
            PropertyConstraint::new("ssn", false)
                .with_description("Social Security Number (Optional String)."),

            // --- Demographics (first_name, last_name, date_of_birth, gender are required) ---
            PropertyConstraint::new("first_name", true)
                .with_description("Patient's first name. Required."),
            PropertyConstraint::new("middle_name", false)
                .with_description("Patient's middle name (Optional String)."),
            PropertyConstraint::new("last_name", true)
                .with_description("Patient's last name. Required."),
            PropertyConstraint::new("suffix", false)
                .with_description("Name suffix (Optional: Jr, Sr, III, etc.)."),
            PropertyConstraint::new("preferred_name", false)
                .with_description("Patient's preferred name or alias (Optional String)."),
            PropertyConstraint::new("date_of_birth", true)
                .with_description("Patient's date of birth (Required Timestamp)."),
            PropertyConstraint::new("date_of_death", false)
                .with_description("Patient's date of death (Optional Timestamp)."),
            PropertyConstraint::new("gender", true)
                .with_description("Gender (Required: MALE, FEMALE, OTHER, UNKNOWN)."),
            PropertyConstraint::new("sex_assigned_at_birth", false)
                .with_description("Sex assigned at birth (Optional: MALE, FEMALE, INTERSEX)."),
            PropertyConstraint::new("gender_identity", false)
                .with_description("Gender identity (Optional String)."),
            PropertyConstraint::new("pronouns", false)
                .with_description("Preferred pronouns (Optional: HE/HIM, SHE/HER, THEY/THEM, etc.)."),

            // --- Contact Information (interpreter_needed is required) ---
            PropertyConstraint::new("address_id", false)
                .with_description("Foreign key to Address vertex (Optional Uuid)."),
            PropertyConstraint::new("address", false)
                .with_description("Embedded Address object (Optional, stored as JSON)."),
            PropertyConstraint::new("phone_home", false)
                .with_description("Home phone number (Optional String)."),
            PropertyConstraint::new("phone_mobile", false)
                .with_description("Mobile phone number (Optional String)."),
            PropertyConstraint::new("phone_work", false)
                .with_description("Work phone number (Optional String)."),
            PropertyConstraint::new("email", false)
                .with_description("Email address (Optional String)."),
            PropertyConstraint::new("preferred_contact_method", false)
                .with_description("Preferred contact method (Optional: PHONE, EMAIL, SMS, MAIL)."),
            PropertyConstraint::new("preferred_language", false)
                .with_description("The patient's preferred communication language (Optional: EN, ES, FR)."),
            PropertyConstraint::new("interpreter_needed", true)
                .with_description("Boolean flag indicating if interpreter services are required. Required."),

            // --- Emergency Contact ---
            PropertyConstraint::new("emergency_contact_name", false)
                .with_description("Emergency contact's name (Optional String)."),
            PropertyConstraint::new("emergency_contact_relationship", false)
                .with_description("Emergency contact's relationship (Optional String)."),
            PropertyConstraint::new("emergency_contact_phone", false)
                .with_description("Emergency contact's phone number (Optional String)."),

            // --- Demographic Details ---
            PropertyConstraint::new("marital_status", false)
                .with_description("Marital status (Optional: SINGLE, MARRIED, DIVORCED, etc.)."),
            PropertyConstraint::new("race", false)
                .with_description("Patient's race detail (Optional String)."),
            PropertyConstraint::new("ethnicity", false)
                .with_description("Patient's ethnicity detail (Optional String)."),
            PropertyConstraint::new("religion", false)
                .with_description("Religious affiliation (Optional String)."),

            // --- Insurance & Financial ---
            PropertyConstraint::new("primary_insurance", false)
                .with_description("Primary insurance carrier name (Optional String)."),
            PropertyConstraint::new("primary_insurance_id", false)
                .with_description("Primary insurance member ID (Optional String)."),
            PropertyConstraint::new("secondary_insurance", false)
                .with_description("Secondary insurance carrier name (Optional String)."),
            PropertyConstraint::new("secondary_insurance_id", false)
                .with_description("Secondary insurance member ID (Optional String)."),
            PropertyConstraint::new("guarantor_name", false)
                .with_description("Guarantor's name (Optional String)."),
            PropertyConstraint::new("guarantor_relationship", false)
                .with_description("Guarantor's relationship to patient (Optional String)."),

            // --- Clinical Information (advance_directive_on_file is required) ---
            PropertyConstraint::new("primary_care_provider_id", false)
                .with_description("Foreign key to Primary Care Provider vertex (Optional i32)."),
            PropertyConstraint::new("blood_type", false)
                .with_description("Blood type (Optional: A+, B-, O+ etc.)."),
            PropertyConstraint::new("organ_donor", false)
                .with_description("Organ donor status (Optional Boolean)."),
            PropertyConstraint::new("advance_directive_on_file", true)
                .with_description("Boolean flag for advance directive status. Required."),
            PropertyConstraint::new("dni_status", false)
                .with_description("Do Not Intubate status (Optional String)."),
            PropertyConstraint::new("dnr_status", false)
                .with_description("Do Not Resuscitate status (Optional String)."),
            PropertyConstraint::new("code_status", false)
                .with_description("Code status (Optional: FULL_CODE, DNR, DNI, COMFORT_CARE)."),

            // --- Administrative (patient_status, vip_flag, confidential_flag are required) ---
            PropertyConstraint::new("patient_status", true)
                .with_description("Administrative status (Required: ACTIVE, INACTIVE, DECEASED, MERGED, TEST)."),
            PropertyConstraint::new("vip_flag", true)
                .with_description("VIP flag. Required."),
            PropertyConstraint::new("confidential_flag", true)
                .with_description("Confidentiality restriction flag. Required."),
            PropertyConstraint::new("research_consent", false)
                .with_description("Research consent status (Optional Boolean)."),
            PropertyConstraint::new("marketing_consent", false)
                .with_description("Marketing consent status (Optional Boolean)."),

            // --- Social Determinants of Health (food_insecurity, transportation_needs are required) ---
            PropertyConstraint::new("employment_status", false)
                .with_description("Employment status (Optional String)."),
            PropertyConstraint::new("housing_status", false)
                .with_description("Housing status (Optional: HOUSED, HOMELESS, UNSTABLE, TEMPORARY)."),
            PropertyConstraint::new("education_level", false)
                .with_description("Education level (Optional String)."),
            PropertyConstraint::new("financial_strain", false)
                .with_description("Financial strain level (Optional: NONE, MILD, MODERATE, SEVERE)."),
            PropertyConstraint::new("food_insecurity", true)
                .with_description("Boolean flag for food insecurity. Required."),
            PropertyConstraint::new("transportation_needs", true)
                .with_description("Boolean flag for transportation needs. Required."),
            PropertyConstraint::new("social_isolation", false)
                .with_description("Social isolation score/level (Optional String)."),
            PropertyConstraint::new("veteran_status", false)
                .with_description("Veteran status (Optional Boolean)."),
            PropertyConstraint::new("disability_status", false)
                .with_description("Disability status (Optional String)."),

            // --- Clinical Alerts ---
            PropertyConstraint::new("alert_flags", false)
                .with_description("JSON array of clinical alert codes (Optional, stored as string)."),
            PropertyConstraint::new("special_needs", false)
                .with_description("Special needs description (Optional String)."),

            // --- Audit Trail (created_at, updated_at are required) ---
            PropertyConstraint::new("created_at", true)
                .with_description("Creation timestamp. Required."),
            PropertyConstraint::new("updated_at", true)
                .with_description("Last update timestamp. Required."),
            PropertyConstraint::new("created_by", false)
                .with_description("Creator user ID (Optional i32)."),
            PropertyConstraint::new("updated_by", false)
                .with_description("Updater user ID (Optional i32)."),
            PropertyConstraint::new("last_visit_date", false)
                .with_description("Last visit timestamp (Optional Timestamp)."),
        ]
    }

    fn lifecycle_rules() -> Vec<LifecycleRule> {
        vec![
            LifecycleRule {
                // The element name is correctly set to "patient_status"
                element: "patient_status".to_string(),
                initial_state: Some("Pre-Registered".to_string()),
                transitions: vec![
                    StateTransition {
                        from_state: "Pre-Registered".to_string(),
                        to_state: "Admitted".to_string(),
                        required_rules: vec!["Has_Billing_Address".to_string()],
                        triggers_events: vec!["PATIENT_ADMITTED".to_string()],
                    },
                    StateTransition {
                        from_state: "Admitted".to_string(),
                        to_state: "Discharged".to_string(),
                        required_rules: vec!["Has_Discharge_Summary".to_string()],
                        triggers_events: vec!["PATIENT_DISCHARGED".to_string()],
                    },
                    StateTransition {
                        from_state: "Discharged".to_string(),
                        to_state: "Archived".to_string(),
                        required_rules: vec!["Years_After_Discharge_Min_7".to_string()],
                        triggers_events: vec![],
                    },
                ],
                pre_action_checks: vec![],
                post_action_actions: vec![
                    SchemaAction::PublishMessage {
                        topic: "patient_status_changes".to_string(),
                        payload_template: "{id} transitioned from {old_status} to {new_status}".to_string()
                    }
                ],
            }
        ]
    }

    fn ontology_references() -> Vec<OntologyReference> {
        // References to standardized healthcare terminologies
        vec![
            OntologyReference {
                name: "FHIR_Gender_Codes".to_string(),
                ontology_system_id: "FHIR_AdministrativeGender".to_string(),
                // FIX: Wrap uri and reference_uri in Some()
                uri: Some("http://hl7.org/fhir/ValueSet/administrative-gender".to_string()),
                reference_uri: Some("http://hl7.org/fhir/ValueSet/administrative-gender".to_string()),
                description: Some("FHIR codes for administrative gender.".to_string()),
            },
            OntologyReference {
                name: "SDOH_Codes".to_string(),
                ontology_system_id: "LOINC_SDOH".to_string(),
                // FIX: Wrap uri and reference_uri in Some()
                uri: Some("http://loinc.org/sdoh".to_string()),
                reference_uri: Some("http://loinc.org/sdoh".to_string()),
                description: Some("LOINC codes for Social Determinants of Health.".to_string()),
            },
        ]
    }

    fn messaging_schema() -> MessagingSchema {
        // Standard messaging topics for CRUD and error handling
        MessagingSchema {
            creation_topic: Some("patient.created".to_string()),
            update_topic: Some("patient.updated".to_string()),
            deletion_topic: Some("patient.deleted".to_string()),
            error_queue: Some("patient.errors".to_string()),
        }
    }
}