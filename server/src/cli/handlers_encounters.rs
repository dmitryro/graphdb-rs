//  server/src/cli/handlers_encounters.rs
use  medical_knowledge::clinical_encounters::{
    ClinicalEncounterService,
    PatientTimeline,
    TimelineEntry,
    DrugAlert,
    CareGap,
    CLINICAL_ENCOUNTER_SERVICE,
    Encounter, Patient, Doctor, Diagnosis, Prescription,
    ClinicalNote, Observation, Triage, Disposition,
};