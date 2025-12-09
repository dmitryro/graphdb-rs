// schema/src/lifecycle.rs

use serde::{Deserialize, Serialize};
use crate::rules::SchemaRule;
use crate::definitions::EventDefinition;

/// Defines the allowable states and transitions for a Vertex or Edge.
/// This is vital for processes like patient admission/discharge or claim status changes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateTransition {
    /// The starting state (e.g., "Draft", "Active", "Admitted").
    pub from_state: String,
    /// The target state (e.g., "Final", "Discharged").
    pub to_state: String,
    /// The conditions that must be met to perform the transition.
    pub required_rules: Vec<String>, // References to named rules in rules.rs
    /// Events to be triggered upon successful transition.
    pub triggers_events: Vec<String>, // References to named events in events.rs
}

/// The definition of a single lifecycle rule for a schema element.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LifecycleRule {
    /// The name of the property or element this rule applies to.
    pub element: String, // e.g., "status" property, or the element itself.
    /// The default starting state.
    pub initial_state: Option<String>,
    /// List of all possible transitions.
    pub transitions: Vec<StateTransition>,
    /// Rules that must be checked before any database action (CREATE, UPDATE, DELETE).
    pub pre_action_checks: Vec<SchemaRule>,
    /// Actions (like sending a message) to be executed after a database action.
    pub post_action_actions: Vec<SchemaAction>,
}

/// Represents an action to be performed (e.g., sending a message, calling an external service).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SchemaAction {
    /// Publish a message to a defined topic/queue.
    PublishMessage { topic: String, payload_template: String },
    /// Call an external HTTP endpoint.
    CallExternalService { service_name: String, endpoint: String, method: String },
    /// Create or update a related graph element.
    GraphMutation { mutation_type: String, target_schema: String },
}

/// Defines the queues and topics related to this schema element.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessagingSchema {
    pub creation_topic: Option<String>,
    pub update_topic: Option<String>,
    pub deletion_topic: Option<String>,
    pub error_queue: Option<String>,
}
