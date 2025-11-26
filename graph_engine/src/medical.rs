// graph_engine/src/medical.rs
// Clinical domain API — works 100% with current models crate (no from_vertex)

use models::{ Graph, Vertex, Edge };
use models::medical::*;
use models::properties::PropertyValue;
use uuid::Uuid;
use std::collections::HashSet;
use std::str::FromStr;

/// High-level patient view — the main entry point for clinical logic
pub struct PatientView<'g> {
    graph: &'g Graph,
    patient_vertex: &'g Vertex,
    patient_id: Uuid,
}

impl<'g> PatientView<'g> {
    pub fn new(graph: &'g Graph, patient_vertex: &'g Vertex) -> Option<Self> {
        if patient_vertex.label.as_ref() != "Patient" {
            return None;
        }
        let patient_id = patient_vertex.id.0;
        Some(Self { graph, patient_vertex, patient_id })
    }

    pub fn id(&self) -> i32 {
        self.patient_vertex
            .properties
            .get("id")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse().ok())
            .unwrap_or(0)
    }

    pub fn first_name(&self) -> Option<&str> {
        self.patient_vertex.properties.get("first_name")?.as_str()
    }

    pub fn last_name(&self) -> Option<&str> {
        self.patient_vertex.properties.get("last_name")?.as_str()
    }

    pub fn date_of_birth(&self) -> Option<chrono::DateTime<chrono::Utc>> {
        let dob_str = self.patient_vertex.properties.get("date_of_birth")?.as_str()?;
        chrono::DateTime::parse_from_rfc3339(dob_str)
            .ok()
            .map(|dt| dt.with_timezone(&chrono::Utc))
    }

    pub fn age(&self) -> Option<u32> {
        let dob = self.date_of_birth()?;
        let now = chrono::Utc::now();
        let age = now.years_since(dob)?;
        Some(age)
    }

    pub fn active_diagnoses(&self) -> Vec<DiagnosisInfo> {
        self.graph
            .outgoing_edges(&self.patient_id)
            .filter_map(|edge| {
                if edge.edge_type.as_ref() == "HAS_DIAGNOSIS" {
                    let dx_vertex = self.graph.get_vertex(&edge.inbound_id.0)?;
                    Some(DiagnosisInfo::from_vertex(dx_vertex))
                } else {
                    None
                }
            })
            .collect()
    }

    pub fn current_prescriptions(&self) -> Vec<PrescriptionInfo> {
        self.graph
            .outgoing_edges(&self.patient_id)
            .filter_map(|edge| {
                if edge.edge_type.as_ref() == "HAS_PRESCRIPTION" {
                    let rx_vertex = self.graph.get_vertex(&edge.inbound_id.0)?;
                    Some(PrescriptionInfo::from_vertex(rx_vertex))
                } else {
                    None
                }
            })
            .collect()
    }

    pub fn recent_encounters(&self, limit: usize) -> Vec<EncounterInfo> {
        self.graph
            .outgoing_edges(&self.patient_id)
            .filter_map(|edge| {
                if edge.edge_type.as_ref() == "HAS_ENCOUNTER" {
                    let enc_vertex = self.graph.get_vertex(&edge.inbound_id.0)?;
                    Some(EncounterInfo::from_vertex(enc_vertex))
                } else {
                    None
                }
            })
            .take(limit)
            .collect()
    }

    pub fn care_gaps(&self) -> Vec<String> {
        let mut gaps = Vec::new();
        let age = self.age();

        if let Some(age) = age {
            if age >= 50 && age <= 74 {
                let has_colonoscopy = self.recent_encounters(100)
                    .iter()
                    .any(|e| e.encounter_type == "COLONOSCOPY");
                if !has_colonoscopy {
                    gaps.push("Colon cancer screening missing".to_string());
                }
            }
        }
        gaps
    }
}

#[derive(Debug, Clone)]
pub struct DiagnosisInfo {
    pub id: i32,
    pub code_id: i32,
    pub description: String,
    pub date: chrono::NaiveDate,
}

impl DiagnosisInfo {
    pub fn from_vertex(vertex: &Vertex) -> Self {
        let id = vertex.properties.get("id").and_then(|v| v.as_str()).and_then(|s| s.parse().ok()).unwrap_or(0);
        let code_id = vertex.properties.get("code_id").and_then(|v| v.as_str()).and_then(|s| s.parse().ok()).unwrap_or(0);
        let description = vertex.properties.get("description").and_then(|v| v.as_str()).unwrap_or("").to_string();
        let date_str = vertex.properties.get("date").and_then(|v| v.as_str()).unwrap_or("1970-01-01");
        let date = chrono::NaiveDate::parse_from_str(date_str, "%Y-%m-%d")
            .unwrap_or(chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap());

        Self { id, code_id, description, date }
    }
}

#[derive(Debug, Clone)]
pub struct PrescriptionInfo {
    pub id: i32,
    pub medication_name: String,
    pub dose: String,
    pub frequency: String,
}

impl PrescriptionInfo {
    pub fn from_vertex(vertex: &Vertex) -> Self {
        let id = vertex.properties.get("id").and_then(|v| v.as_str()).and_then(|s| s.parse().ok()).unwrap_or(0);
        let medication_name = vertex.properties.get("medication_name").and_then(|v| v.as_str()).unwrap_or("").to_string();
        let dose = vertex.properties.get("dose").and_then(|v| v.as_str()).unwrap_or("").to_string();
        let frequency = vertex.properties.get("frequency").and_then(|v| v.as_str()).unwrap_or("").to_string();

        Self { id, medication_name, dose, frequency }
    }
}

#[derive(Debug, Clone)]
pub struct EncounterInfo {
    pub id: i32,
    pub encounter_type: String,
    pub date: chrono::DateTime<chrono::Utc>,
}

impl EncounterInfo {
    pub fn from_vertex(vertex: &Vertex) -> Self {
        let id = vertex.properties.get("id").and_then(|v| v.as_str()).and_then(|s| s.parse().ok()).unwrap_or(0);
        let encounter_type = vertex.properties.get("encounter_type").and_then(|v| v.as_str()).unwrap_or("").to_string();
        let date_str = vertex.properties.get("date").and_then(|v| v.as_str()).unwrap_or("1970-01-01T00:00:00Z");
        let date = chrono::DateTime::parse_from_rfc3339(date_str)
            .map(|dt| dt.with_timezone(&chrono::Utc))
            .unwrap_or(chrono::Utc::now());

        Self { id, encounter_type, date }
    }
}
