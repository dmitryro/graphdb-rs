// schema/src/rules.rs

use serde::{Deserialize, Serialize};

/// Represents a reusable, named business rule to enforce compliance or logic.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaRule {
    /// Unique name of the rule (e.g., "Patient_Age_Must_Be_Over_18").
    pub name: String,
    /// A human-readable description.
    pub description: String,
    /// The condition expressed in a simplified query language (e.g., a mini-Cypher/SQL subset).
    /// This is used for complex, cross-property or cross-element validation.
    pub condition_expression: String, // e.g., "age > 18 AND status == 'Active'"
    pub enforcement_level: EnforcementLevel,
}

impl SchemaRule {
    /// Simplified constructor used for referencing rules/events by name in the schema.
    /// The actual definition is assumed to be provided elsewhere in the system.
    pub fn new(name: &str) -> Self {
        SchemaRule {
            name: name.to_string(),
            description: format!("Reference to rule/event '{}'. Full description pending.", name),
            condition_expression: "N/A - See full rule definition".to_string(),
            enforcement_level: EnforcementLevel::HardStop,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum EnforcementLevel {
    /// Transaction fails if rule is violated (Default).
    HardStop,
    /// A warning is logged, but the transaction proceeds.
    Warning,
    /// An event is triggered, but the transaction proceeds.
    EventOnly,
}
