// handlers_medical.rs
use crate::graph_engine::graph_service::GraphService;
use crate::medical_knowledge::patient_service::PatientService;
use crate::models::medical::{Patient, Problem, Prescription, Allergy, Referral};
use crate::commands::{PatientCommand, JourneyFormat, AlertFormat, AlertSeverity};
use chrono::prelude::*;
use serde_json::json;
use std::collections::HashMap;

pub async fn handle_patient_command(action: PatientCommand) -> String {
    let graph = GraphService::get().await;
    let patient_service = PatientService::get().await;

    match action {
        PatientCommand::Create { first_name, last_name, dob, gender, ssn, mrn } => {
            let dob_parsed = NaiveDate::parse_from_str(&dob, "%Y-%m-%d").unwrap_or(NaiveDate::from_ymd_opt(1900, 1, 1).unwrap());
            let new_patient = Patient {
                id: 0, // Will be generated
                user_id: None,
                mrn,
                ssn,
                first_name,
                middle_name: None,
                last_name,
                suffix: None,
                preferred_name: None,
                date_of_birth: dob_parsed.and_hms_opt(0, 0, 0).unwrap().and_utc(),
                date_of_death: None,
                gender,
                sex_assigned_at_birth: None,
                gender_identity: None,
                pronouns: None,
                address_id: None,
                address: None,
                phone_home: None,
                phone_mobile: None,
                phone_work: None,
                email: None,
                preferred_contact_method: None,
                preferred_language: None,
                interpreter_needed: false,
                emergency_contact_name: None,
                emergency_contact_relationship: None,
                emergency_contact_phone: None,
                marital_status: None,
                race: None,
                ethnicity: None,
                religion: None,
                primary_insurance: None,
                primary_insurance_id: None,
                secondary_insurance: None,
                secondary_insurance_id: None,
                guarantor_name: None,
                guarantor_relationship: None,
                primary_care_provider_id: None,
                blood_type: None,
                organ_donor: None,
                advance_directive_on_file: false,
                dni_status: None,
                dnr_status: None,
                code_status: None,
                patient_status: "ACTIVE".to_string(),
                vip_flag: false,
                confidential_flag: false,
                research_consent: None,
                marketing_consent: None,
                employment_status: None,
                housing_status: None,
                education_level: None,
                financial_strain: None,
                food_insecurity: false,
                transportation_needs: false,
                social_isolation: None,
                veteran_status: None,
                disability_status: None,
                alert_flags: None,
                special_needs: None,
                created_at: Utc::now(),
                updated_at: Utc::now(),
                created_by: None,
                updated_by: None,
                last_visit_date: None,
            };
            patient_service.create_patient(&new_patient).await.unwrap_or_else(|e| format!("Error: {}", e))
        }
        PatientCommand::View { patient_id } => {
            patient_service.view_patient(patient_id).await.unwrap_or_else(|e| format!("Error: {}", e))
        }
        PatientCommand::Search { query } => {
            patient_service.search_patients(&query).await.unwrap_or_else(|e| format!("Error: {}", e))
        }
        PatientCommand::Timeline { patient_id } => {
            patient_service.get_timeline(patient_id).await.unwrap_or_else(|e| format!("Error: {}", e))
        }
        PatientCommand::Problems { patient_id } => {
            patient_service.get_problems(patient_id).await.unwrap_or_else(|e| format!("Error: {}", e))
        }
        PatientCommand::Meds { patient_id } => {
            patient_service.get_meds(patient_id).await.unwrap_or_else(|e| format!("Error: {}", e))
        }
        PatientCommand::CareGaps { patient_id } => {
            patient_service.get_care_gaps(patient_id).await.unwrap_or_else(|e| format!("Error: {}", e))
        }
        PatientCommand::Allergies { patient_id } => {
            patient_service.get_allergies(patient_id).await.unwrap_or_else(|e| format!("Error: {}", e))
        }
        PatientCommand::Referrals { patient_id } => {
            patient_service.get_referrals(patient_id).await.unwrap_or_else(|e| format!("Error: {}", e))
        }
        PatientCommand::Journey { patient_id, pathway, show_completed, show_deviations_only, format } => {
            patient_service.get_journey(patient_id, pathway, show_completed, show_deviations_only, format).await.unwrap_or_else(|e| format!("Error: {}", e))
        }
        PatientCommand::DrugAlerts { patient_id, severity, include_resolved, include_overridden, drug_class, format, include_inactive, severity_filter } => {
            patient_service.get_drug_alerts(patient_id, severity, include_resolved, include_overridden, drug_class, format, include_inactive, severity_filter).await.unwrap_or_else(|e| format!("Error: {}", e))
        }
    }
}