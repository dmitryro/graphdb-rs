// schema/src/edges/mod.rs

pub mod has_allergy;
pub mod has_diagnosis;
pub mod has_encounter;
pub mod has_medication;
pub mod relationship;
pub mod relationship_types;

// Export the generic handler components
pub use relationship::*;

// Export the types list (if needed elsewhere)
pub use relationship_types::*;
