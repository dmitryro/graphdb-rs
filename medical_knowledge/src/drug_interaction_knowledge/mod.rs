// medical_knowledge/src/drug_interaction_knowledge/mod.rs
pub mod drug_interaction_knowledge;

pub use drug_interaction_knowledge::{
    DrugInteractionKnowledgeService,
    DrugInteractionAlert,
    InteractionSeverity,
    DRUG_INTERACTION_SERVICE,
};