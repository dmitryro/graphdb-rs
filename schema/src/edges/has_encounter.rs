use serde::{Deserialize, Serialize};
use chrono::{DateTime, Utc};
use std::collections::HashMap;

/// Represents the relationship type for a patient's encounter with a healthcare entity.
/// This edge connects a `Patient` Node (Source) to an `Encounter` Node (Target).
/// It carries temporal data about the encounter duration.
/// 
/// Relationship Type: HAS_ENCOUNTER
/// Source Node Label: Patient
/// Target Node Label: Encounter
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HasEncounter {
    /// The unique identifier for this specific edge instance.
    pub id: String,
    
    /// The date and time the relationship (encounter) began. 
    /// This may correspond to the 'date' property on the Encounter node, 
    /// or be used as the definitive start time for graph analysis.
    pub start_time: DateTime<Utc>,

    /// The date and time the relationship (encounter) ended. If null, the encounter is ongoing.
    pub end_time: Option<DateTime<Utc>>,

    /// The duration of the relationship/encounter in minutes. Calculated from start and end times.
    /// Stored as an Option to handle ongoing encounters gracefully.
    pub duration_minutes: Option<i64>,

    /// A brief description of the primary reason for the encounter, stored on the relationship 
    /// to qualify the patient's motivation for the visit.
    pub reason_for_encounter: String,
    
    /// A map for any additional, non-standard properties required for this relationship.
    #[serde(default)]
    pub additional_properties: HashMap<String, String>,
}

impl HasEncounter {
    /// The canonical, uppercase name for the relationship type as stored in the graph.
    pub const RELATIONSHIP_TYPE: &'static str = "HAS_ENCOUNTER";

    /// Creates a new, minimal HasEncounter instance.
    pub fn new(
        id: String,
        start_time: DateTime<Utc>,
        reason: String,
    ) -> Self {
        Self {
            id,
            start_time,
            end_time: None,
            duration_minutes: None,
            reason_for_encounter: reason,
            additional_properties: HashMap::new(),
        }
    }

    /// Sets the end time for the encounter and calculates the duration.
    /// Returns an error if the end time is before the start time.
    pub fn end_encounter(&mut self, end_time: DateTime<Utc>) -> Result<(), String> {
        if end_time < self.start_time {
            return Err("End time cannot be before start time.".to_string());
        }
        
        let duration = end_time.signed_duration_since(self.start_time);
        self.duration_minutes = Some(duration.num_minutes());
        self.end_time = Some(end_time);
        Ok(())
    }
}
