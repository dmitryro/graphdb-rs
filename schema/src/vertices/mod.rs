// schema/src/vertices/mod.rs

pub mod allergy;
pub mod doctor;
pub mod diagnosis;
pub mod encounter;
pub mod fhir_patient;
pub mod imaging_study;
pub mod lab_result;
pub mod medication;
pub mod patient;
pub mod snomed_concept;

pub use allergy::*;
pub use doctor::*;
pub use diagnosis::*;
pub use encounter::*;
pub use fhir_patient::*;
pub use imaging_study::*;
pub use lab_result::*;
pub use medication::*;
pub use patient::*;
pub use snomed_concept::*;
