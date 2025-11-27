// server/src/cli/handlers_snomed_codes.rs
use medical_knowledge::snomed_knowledge::{
    SNOMEDKnowledgeService,
    ClinicalCode,
    CodeValidationIssue,
    CodeIssueType,
    IssueSeverity,
    SNOMED_SERVICE,
};