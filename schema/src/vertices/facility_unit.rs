use crate::definitions::VertexSchema;
use crate::constraints::{PropertyConstraint, EnumValues, DataType, Constraint};
use crate::lifecycle::{
    LifecycleRule, StateTransition, MessagingSchema
};
use crate::rules::{SchemaRule, EnforcementLevel}; 
use crate::properties::OntologyReference;
use serde_json::Value as JsonValue;

/// Implementation of the VertexSchema for the FacilityUnit vertex type.
///
/// Description: Represents a physical or functional unit within a department (e.g.,
/// specific room, bed cluster, lab area). Crucial for bed management and resource tracking.
pub struct FacilityUnit;

impl FacilityUnit {
    /// Provides the possible types of facility units.
    fn unit_type_values() -> Vec<String> {
        vec![
            "ED_Triage".to_string(),
            "ED_Resuscitation".to_string(),
            "Operating_Room".to_string(),
            "ICU_Adult".to_string(),
            "General_Medicine_Ward".to_string(),
            "Laboratory".to_string(),
            "Radiology_Suite".to_string(),
            "Outpatient_Clinic".to_string(),
        ]
    }

    /// Provides the possible operational status states for a unit.
    fn status_values() -> Vec<String> {
        vec![
            "Operational".to_string(),      // Ready for use
            "Under_Maintenance".to_string(),// Temporarily unavailable for repair/cleaning
            "Out_of_Service".to_string(),   // Long-term unavailability
            "Decommissioned".to_string(),   // Permanently removed from service
        ]
    }
}

impl VertexSchema for FacilityUnit {
    fn schema_name() -> &'static str {
        "FacilityUnit"
    }

    /// Returns the list of property constraints for the FacilityUnit vertex type.
    fn property_constraints() -> Vec<PropertyConstraint> {
        vec![
            // --- Core Identifier and Structural Fields ---
            PropertyConstraint::new("id", true)
                .with_description("Primary internal ID (i32). Required, Unique, Immutable.")
                .with_data_type(DataType::Integer)
                .with_constraints(vec![Constraint::Required, Constraint::Unique, Constraint::Immutable]),

            PropertyConstraint::new("department_id", true)
                .with_description("ID of the Department vertex this unit belongs to. Required, Immutable.")
                .with_data_type(DataType::Integer)
                .with_constraints(vec![Constraint::Required, Constraint::Immutable]),

            PropertyConstraint::new("name", true)
                .with_description("The human-readable name of the unit (e.g., 'Trauma Bay 1', 'ICU North'). Required, Immutable.")
                .with_data_type(DataType::String)
                .with_constraints(vec![Constraint::Required, Constraint::Immutable]),
            
            PropertyConstraint::new("unit_type", true)
                .with_description("The functional type of the unit (e.g., ED_Triage, Operating_Room). Required, Immutable.")
                .with_data_type(DataType::String)
                .with_enum_values(EnumValues::new(FacilityUnit::unit_type_values()))
                .with_constraints(vec![Constraint::Required, Constraint::Immutable]),
            
            // --- Operational Fields ---
            PropertyConstraint::new("total_beds", false)
                .with_description("Maximum physical capacity of the unit.")
                .with_data_type(DataType::Integer)
                .with_constraints(vec![]),

            PropertyConstraint::new("current_occupancy", false)
                .with_description("Real-time count of occupied beds/slots. Should be >= 0 and <= total_beds.")
                .with_data_type(DataType::Integer)
                // FIX: Changed JsonValue::from(0) to 0i64 to match Constraint::Min(i64)
                .with_constraints(vec![Constraint::Min(0i64)]), 

            PropertyConstraint::new("phone", false)
                .with_description("Direct phone number for the unit.")
                .with_data_type(DataType::String)
                .with_constraints(vec![]),
            
            PropertyConstraint::new("status", true)
                .with_description("The current operational status (Operational, Under_Maintenance, Out_of_Service).")
                .with_data_type(DataType::String)
                .with_enum_values(EnumValues::new(FacilityUnit::status_values()))
                .with_default_value(JsonValue::String("Operational".to_string()))
                .with_constraints(vec![Constraint::Required]),

            // --- Metadata Fields ---
            PropertyConstraint::new("created_at", true)
                .with_description("Timestamp of record creation. Required, Immutable.")
                .with_data_type(DataType::DateTime)
                .with_constraints(vec![Constraint::Required, Constraint::Immutable]),

            PropertyConstraint::new("updated_at", true)
                .with_description("Timestamp of last modification. Required, Managed by lifecycle.")
                .with_data_type(DataType::DateTime)
                .with_constraints(vec![Constraint::Required]),
        ]
    }

    /// Lifecycle manages the operational status of the unit.
    fn lifecycle_rules() -> Vec<LifecycleRule> {
        vec![
            LifecycleRule {
                element: "status".to_string(),
                initial_state: Some("Operational".to_string()),
                transitions: vec![
                    // 1. Transition to Unavailable (Maintenance)
                    StateTransition {
                        from_state: "Operational".to_string(),
                        to_state: "Under_Maintenance".to_string(),
                        // Requires occupancy to be 0 before maintenance can start
                        required_rules: vec!["require_zero_occupancy".to_string()],
                        triggers_events: vec!["unit.maintenance_start".to_string()],
                    },
                    // 2. Transition back to Operational
                    StateTransition {
                        from_state: "Under_Maintenance".to_string(),
                        to_state: "Operational".to_string(),
                        required_rules: vec![],
                        triggers_events: vec!["unit.operational_return".to_string()],
                    },
                    // 3. Transition to Decommissioned (Final state)
                    StateTransition {
                        from_state: "*".to_string(), // Can be decommissioned from any state
                        to_state: "Decommissioned".to_string(),
                        required_rules: vec!["require_zero_occupancy".to_string()],
                        triggers_events: vec!["unit.decommissioned".to_string()],
                    },
                ],
                // Any status change (other than current_occupancy) must update 'updated_at'
                pre_action_checks: vec![
                    SchemaRule {
                        name: "update_timestamp_on_change".to_string(),
                        description: "Ensures updated_at is refreshed on any property modification except occupancy.".to_string(),
                        // We use a simplified check here, assuming the system handles the timestamp update automatically
                        // based on other property changes managed by the update_at lifecycle.
                        condition_expression: "ACTION_TYPE == 'UPDATE'".to_string(), 
                        enforcement_level: EnforcementLevel::HardStop,
                    }
                ],
                post_action_actions: vec![],
            }
        ]
    }

    /// References to standard ontologies for facility and location types.
    fn ontology_references() -> Vec<OntologyReference> {
        vec![
            OntologyReference {
                name: "HL7_LocationType".to_string(),
                ontology_system_id: "HL7".to_string(),
                uri: Some("http://terminology.hl7.org/CodeSystem/location-physical-type".to_string()),
                reference_uri: None,
                description: Some("HL7 standard codes for physical types of locations/units.".to_string()),
            },
            OntologyReference {
                name: "HL7_ServiceDeliveryLocation".to_string(),
                ontology_system_id: "HL7".to_string(),
                uri: Some("http://terminology.hl7.org/CodeSystem/service-delivery-location-role".to_string()),
                reference_uri: None,
                description: Some("HL7 standard for the role of a service delivery location.".to_string()),
            },
        ]
    }

    /// Defines the messaging topics for FacilityUnit CRUD operations.
    fn messaging_schema() -> MessagingSchema {
        MessagingSchema {
            creation_topic: Some("facility_unit.created".to_string()),
            update_topic: Some("facility_unit.updated".to_string()),
            deletion_topic: Some("facility_unit.deleted".to_string()),
            error_queue: Some("facility_unit.errors".to_string()),
        }
    }
}
