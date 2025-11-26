// medical_knowledge/src/snomed_knowledge/snomed_knowledge.rs
//! SNOMED-CT + ICD-10 + CPT Knowledge Service
//! Real-time code validation, hierarchy, mapping, and clinical intelligence

use graph_engine::graph_service::GraphService;
use models::medical::*;
use models::vertices::Vertex;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::{OnceCell, RwLock};
use uuid::Uuid;

/// Global singleton
pub static SNOMED_SERVICE: OnceCell<Arc<SNOMEDKnowledgeService>> = OnceCell::const_new();

/// Clinical code with full metadata
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ClinicalCode {
    pub medical_code_id: i32,
    pub code: String,
    pub description: String,
    pub code_type: String,
    pub active: bool,
    pub is_preferred: bool,
    pub semantic_tags: Vec<String>,
    pub parent_codes: Vec<String>,
    pub child_codes: Vec<String>,
    pub synonyms: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct CodeValidationIssue {
    pub patient_id: Uuid,
    pub encounter_id: Option<Uuid>,
    pub code_id: i32,
    pub code: String,
    pub issue_type: CodeIssueType,
    pub severity: IssueSeverity,
    pub recommendation: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CodeIssueType {
    InactiveCode,
    NonSpecificCode,
    DeprecatedCode,
    WrongCodeSystem,
    MissingMapping,
    AmbiguousTerm,
}

#[derive(Debug, Clone, PartialEq, Eq, Ord, PartialOrd)]
pub enum IssueSeverity {
    Critical,
    High,
    Medium,
    Low,
}

#[derive(Clone)]
pub struct SNOMEDKnowledgeService {
    by_id: Arc<RwLock<HashMap<i32, ClinicalCode>>>,
    by_code: Arc<RwLock<HashMap<String, ClinicalCode>>>,
    by_term: Arc<RwLock<HashMap<String, Vec<i32>>>>,
    parent_index: Arc<RwLock<HashMap<i32, Vec<i32>>>>,
    child_index: Arc<RwLock<HashMap<i32, Vec<i32>>>>,
    tag_index: Arc<RwLock<HashMap<String, Vec<i32>>>>,
}

impl SNOMEDKnowledgeService {
    pub async fn global_init() -> Result<(), &'static str> {
        let service = Arc::new(Self {
            by_id: Arc::new(RwLock::new(HashMap::new())),
            by_code: Arc::new(RwLock::new(HashMap::new())),
            by_term: Arc::new(RwLock::new(HashMap::new())),
            parent_index: Arc::new(RwLock::new(HashMap::new())),
            child_index: Arc::new(RwLock::new(HashMap::new())),
            tag_index: Arc::new(RwLock::new(HashMap::new())),
        });

        // Load all MedicalCode vertices
        {
            let graph_service = GraphService::get().await;
            let graph = graph_service.read().await;

            for vertex in graph.vertices.values() {
                if vertex.label.as_ref() == "MedicalCode" {
                    if let Some(code) = MedicalCode::from_vertex(vertex) {
                        service.index_code(&code).await;
                    }
                }
            }
        }

        // Real-time validation observer
        {
            let service_clone = service.clone();
            let graph_service = GraphService::get().await;
            tokio::spawn(async move {
                let mut graph = graph_service.write_graph().await;
                let service = service_clone;

                graph.on_vertex_added({
                    let service = service.clone();
                    move |vertex| {
                        let vertex = vertex.clone();
                        let service = service.clone();
                        tokio::spawn(async move {
                            if vertex.label.as_ref() == "MedicalCode" {
                                if let Some(code) = MedicalCode::from_vertex(&vertex) {
                                    service.index_code(&code).await;
                                }
                            }
                            if vertex.label.as_ref() == "Diagnosis" || vertex.label.as_ref() == "EdProcedure" {
                                service.validate_clinical_code_usage(&vertex).await;
                            }
                        });
                    }
                }).await;
            });
        }

        SNOMED_SERVICE
            .set(service)
            .map_err(|_| "SNOMEDKnowledgeService already initialized")
    }

    pub async fn get() -> Arc<Self> {
        SNOMED_SERVICE.get().unwrap().clone()
    }

    // INDEXING
    async fn index_code(&self, code: &MedicalCode) {
        let clinical_code = ClinicalCode {
            medical_code_id: code.id,
            code: code.code.clone(),
            description: code.description.clone(),
            code_type: code.code_type.clone(),
            active: true,
            is_preferred: true,
            semantic_tags: self.extract_semantic_tags(&code.description),
            parent_codes: Vec::new(),
            child_codes: Vec::new(),
            synonyms: self.generate_synonyms(&code.description),
        };

        let mut by_id = self.by_id.write().await;
        let mut by_code = self.by_code.write().await;
        let mut by_term = self.by_term.write().await;

        by_id.insert(code.id, clinical_code.clone());
        by_code.insert(code.code.clone(), clinical_code.clone());

        let terms = vec![&code.description, &code.code]
            .into_iter()
            .chain(clinical_code.synonyms.iter())
            .map(|s| s.to_lowercase().replace(" ", ""));

        for term in terms {
            by_term.entry(term).or_default().push(code.id);
        }
    }

    fn extract_semantic_tags(&self, description: &str) -> Vec<String> {
        let desc = description.to_lowercase();
        let mut tags = Vec::new();

        if desc.contains("disorder") || desc.contains("disease") {
            tags.push("disorder".to_string());
        }
        if desc.contains("finding") {
            tags.push("finding".to_string());
        }
        if desc.contains("procedure") {
            tags.push("procedure".to_string());
        }
        if desc.contains("regime") || desc.contains("therapy") {
            tags.push("regime/therapy".to_string());
        }

        tags
    }

    fn generate_synonyms(&self, description: &str) -> Vec<String> {
        let mut synonyms = Vec::new();
        let lower = description.to_lowercase();

        if lower.contains("myocardial infarction") {
            synonyms.push("heart attack".to_string());
            synonyms.push("MI".to_string());
        }
        if lower.contains("diabetes mellitus") {
            synonyms.push("diabetes".to_string());
            synonyms.push("DM".to_string());
        }

        synonyms
    }

    // REAL-TIME VALIDATION
    async fn validate_clinical_code_usage(&self, vertex: &Vertex) {
        let by_id = self.by_id.read().await;

        let code_id = if vertex.label.as_ref() == "Diagnosis" {
            vertex.properties.get("code_id")
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse::<i32>().ok())
        } else if vertex.label.as_ref() == "EdProcedure" {
            vertex.properties.get("procedure_code_id")
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse::<i32>().ok())
        } else {
            None
        };

        if let Some(id) = code_id {
            if let Some(code) = by_id.get(&id) {
                let mut issues = Vec::new();

                if !code.active {
                    issues.push(CodeValidationIssue {
                        patient_id: Uuid::nil(),
                        encounter_id: Some(vertex.id.0),
                        code_id: id,
                        code: code.code.clone(),
                        issue_type: CodeIssueType::InactiveCode,
                        severity: IssueSeverity::High,
                        recommendation: "Replace with active equivalent".to_string(),
                    });
                }

                if code.semantic_tags.contains(&"disorder".to_string()) && vertex.label.as_ref() == "EdProcedure" {
                    issues.push(CodeValidationIssue {
                        patient_id: Uuid::nil(),
                        encounter_id: Some(vertex.id.0),
                        code_id: id,
                        code: code.code.clone(),
                        issue_type: CodeIssueType::WrongCodeSystem,
                        severity: IssueSeverity::Critical,
                        recommendation: "Use procedure code (CPT/HCPCS), not diagnosis code".to_string(),
                    });
                }

                for issue in issues {
                    log::warn!("SNOMED/Clinical Code Issue: {:?}", issue);
                }
            }
        }
    }

    // PUBLIC API
    pub async fn search_codes(&self, query: &str) -> Vec<ClinicalCode> {
        let query = query.to_lowercase().replace(" ", "");
        let by_term = self.by_term.read().await;
        let by_id = self.by_id.read().await;

        let mut results = HashSet::new();
        if let Some(ids) = by_term.get(&query) {
            for id in ids {
                if let Some(code) = by_id.get(id) {
                    results.insert(code.clone());
                }
            }
        }

        results.into_iter().collect()
    }

    pub async fn get_hierarchy(&self, code_id: i32) -> Vec<ClinicalCode> {
        let by_id = self.by_id.read().await;
        let mut path = Vec::new();

        if let Some(code) = by_id.get(&code_id) {
            path.push(code.clone());
        }

        path
    }

    pub async fn codes_by_tag(&self, tag: &str) -> Vec<ClinicalCode> {
        let tag_index = self.tag_index.read().await;
        let by_id = self.by_id.read().await;

        tag_index.get(tag)
            .map(|ids| ids.iter()
                .filter_map(|id| by_id.get(id).cloned())
                .collect())
            .unwrap_or_default()
    }
}