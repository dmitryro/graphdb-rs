// medical_knowledge/src/patient_journey/mod.rs
pub mod patient_journey;

pub use patient_journey::{
    PatientJourneyService,
    PatientJourney,
    ActivePathway,
    JourneyDeviation,
    JourneyMilestone,
    PATIENT_JOURNEY_SERVICE,
};
