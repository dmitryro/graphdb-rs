// schema/src/edges/mod.rs

pub mod authored_by;
pub mod cosigned_by;
pub mod has_allergy;
pub mod has_blocking_key;
pub mod has_clinical_note;
pub mod has_diagnosis;
pub mod has_encounter;
pub mod has_match_score;
pub mod has_medication;
pub mod has_mpi_record;
pub mod has_potential_duplicate;
pub mod has_section;
pub mod has_subsection;
pub mod has_survivorship_rule;
pub mod is_linked_to;
pub mod mentions_concept;
pub mod relationship;
pub mod relationship_types;
pub mod resolved_by;
pub mod is_merged_into;
pub mod is_split_from;

// Export the generic handler components
pub use relationship::*;

// Export the types list (if needed elsewhere)
pub use authored_by::*;
pub use cosigned_by::*;
pub use relationship_types::*;
pub use has_allergy::*;
pub use has_blocking_key::*;
pub use has_clinical_note::*;
pub use has_diagnosis::*;
pub use has_encounter::*;
pub use has_match_score::*;
pub use has_medication::*;
pub use has_mpi_record::*;
pub use has_potential_duplicate::*;
pub use has_section::*;
pub use has_subsection::*;
pub use has_survivorship_rule::*;
pub use is_linked_to::*;
pub use is_merged_into::*;
pub use is_split_from::*;
pub use mentions_concept::*;
pub use resolved_by::*;
