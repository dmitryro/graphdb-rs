// medical_knowledge/src/snomed_knowledge/mod.rs
pub mod snomed_knowledge;

pub use snomed_knowledge::{
    SNOMEDKnowledgeService,
    ClinicalCode,
    CodeValidationIssue,
    CodeIssueType,
    IssueSeverity,
    SNOMED_SERVICE,
};
