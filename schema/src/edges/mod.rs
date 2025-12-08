// schema/src/edges/mod.rs

pub mod has_medication;
pub mod has_diagnosis;
pub mod has_encounter;
pub mod has_allergy;

pub use has_medication::*;
pub use has_diagnosis::*;
pub use has_encounter::*;
pub use has_allergy::*;
