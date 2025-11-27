// medical_knowledge/src/fhir/mod.rs

pub mod fhir;

// medical_knowledge/src/fhir/mod.rs

pub use fhir::{
    FhirService,
    FHIR_SERVICE,
};
// Re-export key domain types for convenience
pub use models::{
    Vertex, Edge, Graph,
};                                              