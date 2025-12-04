// medical_knowledge/src/clinical_encounters/mod.rs
//! Clinical Encounters — the complete clinical intelligence layer
//!
//! This module provides the full clinical workflow API used by:
//! • Doctors (via UI)
//! • AI agents (CDS, risk scoring)
//! • Analytics & population health
//! • Developers building healthcare applications
//!
//! It is built on top of `lib::graph_engine` and `models` — pure domain logic, no storage.

// medical_knowledge/src/clinical_encounters/mod.rs
pub mod clinical_encounters;

pub use clinical_encounters::{
    ClinicalEncounterService,
    PatientTimeline,
    TimelineEntry,
    DrugAlert,
    CareGap,
    CLINICAL_ENCOUNTER_SERVICE,
};
// Re-export key domain types for convenience
pub use models::medical::{
    Encounter, Patient, Doctor, Diagnosis, Prescription,
    ClinicalNote, Observation, Triage, Disposition,
};
