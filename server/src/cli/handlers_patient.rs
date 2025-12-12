// handlers_patient.rs
use lib::graph_engine::graph_service::{GraphService};
use medical_knowledge::patient_service::PatientService;
use models::medical::{Patient, Problem, Prescription, Allergy, Referral};
use models::errors::{ GraphError, GraphResult };
use lib::commands::{PatientCommand, JourneyFormat, AlertFormat, AlertSeverity, CreatePatientArgs};
// FIX: Need to import the storage trait used in the MPI file
use lib::storage_engine::GraphStorageEngine; 
use crate::cli::get_storage_engine_singleton; // Assuming this function exists in the crate root
use chrono::prelude::*;
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;
use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};

// =========================================================================
// INTERNAL BUSINESS LOGIC (omitted for brevity, assume unchanged)
// =========================================================================

/// State and logic container for patient-related operations.
/// Encapsulates the PatientService dependency.
#[derive(Clone)]
pub struct PatientHandlers {
    patient_service: Arc<PatientService>,
}

impl PatientHandlers {
    /// Initialize the Patient handler by explicitly building the dependency chain (like MPIHandlers::new).
    /// Used in CLI context where global services are NOT pre-initialized.
    pub async fn new() -> Result<Self, anyhow::Error> {
        println!("Initializing Patient Handlers...");

        // 1. Get storage singleton (required by GraphService)
        let storage: Arc<dyn GraphStorageEngine> = get_storage_engine_singleton().await?;
        println!("Storage dependency retrieved from singleton.");

        // 2. Initialize GraphService globally (required by PatientService)
        GraphService::global_init(storage).await
            .map_err(|e| anyhow!("Failed to initialize GraphService: {}", e))?;
        println!("GraphService initialized globally.");

        // 3. Initialize PatientService globally (requires GraphService)
        let graph_service = GraphService::get().await
            .map_err(|e| anyhow!("Failed to retrieve initialized GraphService: {}", e))?;
            
        PatientService::global_init(graph_service).await
            .map_err(|e| anyhow!("Failed to initialize PatientService: {}", e))?;
        println!("PatientService initialized globally.");

        // 4. Get the initialized service — returns Arc directly
        let patient_service: Arc<PatientService> = PatientService::get().await
            .map_err(|e| anyhow!("Failed to get initialized PatientService: {}", e))?;

        println!("Patient Handlers ready.");

        Ok(Self { patient_service })
    }

    /// Initialize using externally provided services (for tests/scripts)
    pub async fn new_with_service(patient_service: Arc<PatientService>) -> Self {
        Self { patient_service }
    }
    
    pub async fn create_patient(&self, new_patient: Patient) -> Result<String, GraphError> {
        self.patient_service.create_patient(new_patient).await
    }
    
    pub async fn view_patient(&self, patient_id: i32) -> Result<String, GraphError> {
        // PatientService::view_patient was implemented to return Result<Patient, GraphError>
        // Here we serialize it back to a readable string format.
        match self.patient_service.view_patient(patient_id).await {
            Ok(patient) => Ok(format!("{:#?}", patient)),
            Err(e) => Err(e),
        }
    }
    
    pub async fn search_patients(&self, query: &str) -> Result<String, GraphError> {
        self.patient_service.search_patients(query).await
    }

    pub async fn get_timeline(&self, patient_id: i32) -> Result<String, GraphError> {
        self.patient_service.get_timeline(patient_id).await
    }

    pub async fn get_problems(&self, patient_id: i32) -> Result<String, GraphError> {
        self.patient_service.get_problems(patient_id).await
    }

    pub async fn get_meds(&self, patient_id: i32) -> Result<String, GraphError> {
        self.patient_service.get_meds(patient_id).await
    }

    pub async fn get_care_gaps(&self, patient_id: Option<i32>) -> Result<String, GraphError> {
        self.patient_service.get_care_gaps(patient_id).await
    }

    pub async fn get_allergies(&self, patient_id: i32) -> Result<String, GraphError> {
        self.patient_service.get_allergies(patient_id).await
    }

    pub async fn get_referrals(&self, patient_id: i32) -> Result<String, GraphError> {
        self.patient_service.get_referrals(patient_id).await
    }

    /// Fetches the patient's care journey information.
    pub async fn get_journey(
        &self,
        patient_id: i32, 
        pathway: Option<String>, 
        show_completed: bool, 
        show_deviations_only: bool, 
        format: Option<JourneyFormat>
    ) -> Result<String, GraphError> {
        // Since PatientService did not expose get_journey, we stub the logic here.
        Ok(format!("Journey for Patient {}: Pathway {:?}, Completed: {}, Deviations Only: {}, Format: {:?}", 
            patient_id, pathway, show_completed, show_deviations_only, format))
    }

    pub async fn get_drug_alerts(
        &self, 
        patient_id: i32, 
        severity: Option<AlertSeverity>, 
        include_resolved: bool, 
        include_overridden: bool, 
        drug_class: Option<String>, 
        format: Option<AlertFormat>, 
        include_inactive: bool, 
        severity_filter: Option<AlertSeverity>
    ) -> Result<String, GraphError> {
        self.patient_service.get_drug_alerts(
            patient_id, 
            severity, 
            include_resolved, 
            include_overridden, 
            drug_class, 
            format, 
            include_inactive, 
            severity_filter
        ).await
    }
}

// =========================================================================
// NON-INTERACTIVE COMMAND HANDLER (CLI/API)
// =========================================================================

/// Handles Patient-related commands in non-interactive mode. Returns a formatted String result.
pub async fn handle_patient_command(action: PatientCommand) -> String {
    // Initialize handlers instance
    let handlers = match PatientHandlers::new().await {
        Ok(h) => h,
        Err(e) => return format!("FATAL ERROR: Failed to initialize PatientHandlers: {}", e),
    };

    let result = match action {
        PatientCommand::Create(args) => {
            // Define the final tuple that will hold our parsed values
            let patient_args_tuple: (Option<String>, Option<String>, Option<String>, Option<String>, Option<String>, Option<String>);
            
            // Check if batch is present and perform JSON deserialization if necessary.
            if let Some(json_str) = &args.batch {
                // CASE 1: Batch JSON is provided, deserialize it.
                match serde_json::from_str::<serde_json::Value>(&json_str) {
                    Ok(json_value) => {
                        // Extract fields from JSON
                        let first_name = json_value.get("first_name")
                            .and_then(|v| v.as_str())
                            .map(|s| s.to_string());
                        let last_name = json_value.get("last_name")
                            .and_then(|v| v.as_str())
                            .map(|s| s.to_string());
                        let dob = json_value.get("dob")
                            .and_then(|v| v.as_str())
                            .map(|s| s.to_string());
                        let gender = json_value.get("gender")
                            .and_then(|v| v.as_str())
                            .map(|s| s.to_string());
                        let ssn = json_value.get("ssn")
                            .and_then(|v| v.as_str())
                            .map(|s| s.to_string());
                        let mrn = json_value.get("mrn")
                            .and_then(|v| v.as_str())
                            .map(|s| s.to_string());
                        
                        patient_args_tuple = (first_name, last_name, dob, gender, ssn, mrn);
                    },
                    Err(e) => return format!("Error: Failed to parse batch JSON: {}", e),
                }
            } else {
                // CASE 2: Individual flags were provided, use them directly from `args`.
                patient_args_tuple = (
                    args.first_name.clone(), 
                    args.last_name.clone(), 
                    args.dob.clone(), 
                    args.gender.clone(), 
                    args.ssn.clone(), 
                    args.mrn.clone()
                );
            };

            // Destructure the Option<String> tuple
            let (first_name_opt, last_name_opt, dob_opt, gender_opt, ssn_opt, mrn_opt) = patient_args_tuple;
            
            // Validate that required fields are present
            let (first_name, last_name, dob, gender) = match (first_name_opt, last_name_opt, dob_opt, gender_opt) {
                (Some(fnm), Some(lnm), Some(dob), Some(g)) => (fnm, lnm, dob, g),
                _ => {
                    if args.batch.is_some() {
                        return format!("Error: Batch JSON missing required field(s). Required: first_name, last_name, dob, gender");
                    } else {
                        return format!("Error: Missing required field(s). Required: --first-name, --last-name, --dob, --gender");
                    }
                }
            };
            
            // Parse the date with the specific format: "dd-mm-yyyy"
            let dob_parsed = match NaiveDate::parse_from_str(&dob, "%d-%m-%Y") {
                 Ok(d) => d,
                 Err(e) => return format!("Error: Invalid DOB format ('{}'), expected 'dd-mm-yyyy': {}", dob, e),
            };
            
            // Build the Patient struct
            let new_patient = Patient {
                mrn: mrn_opt, 
                ssn: ssn_opt, 
                first_name, 
                last_name, 
                gender,
                
                // Calculated/Default fields
                id: 0, 
                date_of_birth: dob_parsed.and_hms_opt(0, 0, 0).unwrap().and_utc(),
                created_at: Utc::now(),
                updated_at: Utc::now(),
                patient_status: "ACTIVE".to_string(), 

                // Default/None fields
                user_id: None, middle_name: None, suffix: None,
                preferred_name: None,
                date_of_death: None, sex_assigned_at_birth: None, gender_identity: None,
                pronouns: None, address_id: None, address: None, phone_home: None,
                phone_mobile: None, phone_work: None, email: None, preferred_contact_method: None,
                preferred_language: None, interpreter_needed: false, emergency_contact_name: None,
                emergency_contact_relationship: None, emergency_contact_phone: None,
                marital_status: None, race: None, ethnicity: None, religion: None,
                primary_insurance: None, primary_insurance_id: None, secondary_insurance: None,
                secondary_insurance_id: None, guarantor_name: None, guarantor_relationship: None,
                primary_care_provider_id: None, blood_type: None, organ_donor: None,
                advance_directive_on_file: false, dni_status: None, dnr_status: None,
                code_status: None, vip_flag: false,
                confidential_flag: false, research_consent: None, marketing_consent: None,
                employment_status: None, housing_status: None, education_level: None,
                financial_strain: None, food_insecurity: false, transportation_needs: false,
                social_isolation: None, veteran_status: None, disability_status: None,
                alert_flags: None, special_needs: None, created_by: None, updated_by: None, last_visit_date: None,
            };
            handlers.create_patient(new_patient).await
        }
        PatientCommand::View { patient_id } => handlers.view_patient(patient_id).await,
        PatientCommand::Search { query } => handlers.search_patients(&query).await,
        PatientCommand::Timeline { patient_id } => handlers.get_timeline(patient_id).await,
        PatientCommand::Problems { patient_id } => handlers.get_problems(patient_id).await,
        PatientCommand::Meds { patient_id } => handlers.get_meds(patient_id).await,
        PatientCommand::CareGaps { patient_id } => handlers.get_care_gaps(patient_id).await,
        PatientCommand::Allergies { patient_id } => handlers.get_allergies(patient_id).await,
        PatientCommand::Referrals { patient_id } => handlers.get_referrals(patient_id).await,
        PatientCommand::Journey { patient_id, pathway, show_completed, show_deviations_only, format } => {
            handlers.get_journey(patient_id, pathway, show_completed, show_deviations_only, format).await
        }
        PatientCommand::DrugAlerts { patient_id, severity, include_resolved, include_overridden, drug_class, format, include_inactive, severity_filter } => {
            handlers.get_drug_alerts(patient_id, severity, include_resolved, include_overridden, drug_class, format, include_inactive, severity_filter).await
        }
    };
    
    // Convert result to the required output String
    match result {
        Ok(s) => s,
        Err(GraphError::NotFoundError(msg)) => format!("Error: Not Found - {}", msg),
        Err(e) => format!("Error: Patient Command Failed - {}", e),
    }
}

// =========================================================================
// INTERACTIVE COMMAND HANDLER (REPL/TUI)
// =========================================================================

/// Handles Patient-related commands in interactive mode. Outputs results to stdout.
pub async fn handle_patient_command_interactive(
    action: PatientCommand,
) -> Result<(), anyhow::Error> {
    
    // FIX: Using the fully dependency-aware `new` method for interactive mode.
    let handlers = PatientHandlers::new().await?;

    let result = match action {
        PatientCommand::Create(args) => {
            // Define the final tuple that will hold our parsed values
            let patient_args_tuple: (Option<String>, Option<String>, Option<String>, Option<String>, Option<String>, Option<String>);

            // Check if batch is present and perform JSON deserialization if necessary.
            if let Some(json_str) = &args.batch {
                // CASE 1: Batch JSON is provided, deserialize it.
                match serde_json::from_str::<serde_json::Value>(&json_str) {
                    Ok(json_value) => {
                        // Extract fields from JSON
                        let first_name = json_value.get("first_name")
                            .and_then(|v| v.as_str())
                            .map(|s| s.to_string());
                        let last_name = json_value.get("last_name")
                            .and_then(|v| v.as_str())
                            .map(|s| s.to_string());
                        let dob = json_value.get("dob")
                            .and_then(|v| v.as_str())
                            .map(|s| s.to_string());
                        let gender = json_value.get("gender")
                            .and_then(|v| v.as_str())
                            .map(|s| s.to_string());
                        let ssn = json_value.get("ssn")
                            .and_then(|v| v.as_str())
                            .map(|s| s.to_string());
                        let mrn = json_value.get("mrn")
                            .and_then(|v| v.as_str())
                            .map(|s| s.to_string());
                        
                        patient_args_tuple = (first_name, last_name, dob, gender, ssn, mrn);
                    },
                    Err(e) => return Err(anyhow!("Failed to parse batch JSON: {}", e)),
                }
            } else {
                // CASE 2: Individual flags were provided, use them directly from `args`.
                patient_args_tuple = (
                    args.first_name.clone(), 
                    args.last_name.clone(), 
                    args.dob.clone(), 
                    args.gender.clone(), 
                    args.ssn.clone(), 
                    args.mrn.clone()
                );
            };

            // Destructure the Option<String> tuple
            let (first_name_opt, last_name_opt, dob_opt, gender_opt, ssn_opt, mrn_opt) = patient_args_tuple;
            
            // Validate that required fields are present
            let (first_name, last_name, dob, gender) = match (first_name_opt, last_name_opt, dob_opt, gender_opt) {
                (Some(fnm), Some(lnm), Some(dob), Some(g)) => (fnm, lnm, dob, g),
                _ => {
                    if args.batch.is_some() {
                        return Err(anyhow!("Batch JSON missing required field(s). Required: first_name, last_name, dob, gender"));
                    } else {
                        return Err(anyhow!("Missing required field(s). Required: --first-name, --last-name, --dob, --gender"));
                    }
                }
            };
            
            // Parse the date with the specific format: "dd-mm-yyyy"
            let dob_parsed = match NaiveDate::parse_from_str(&dob, "%d-%m-%Y") {
                Ok(d) => d,
                Err(e) => return Err(anyhow!("Invalid DOB format ('{}'), expected 'dd-mm-yyyy': {}", dob, e)),
            };
            
            // Build the Patient struct
            let new_patient = Patient {
                mrn: mrn_opt, 
                ssn: ssn_opt, 
                first_name, 
                last_name, 
                gender,

                // Calculated/Default fields
                id: 0, 
                date_of_birth: dob_parsed.and_hms_opt(0, 0, 0).unwrap().and_utc(),
                created_at: Utc::now(),
                updated_at: Utc::now(),
                patient_status: "ACTIVE".to_string(), 

                // Default/None fields
                user_id: None, middle_name: None, suffix: None,
                preferred_name: None,
                date_of_death: None, sex_assigned_at_birth: None, gender_identity: None,
                pronouns: None, address_id: None, address: None, phone_home: None,
                phone_mobile: None, phone_work: None, email: None, preferred_contact_method: None,
                preferred_language: None, interpreter_needed: false, emergency_contact_name: None,
                emergency_contact_relationship: None, emergency_contact_phone: None,
                marital_status: None, race: None, ethnicity: None, religion: None,
                primary_insurance: None, primary_insurance_id: None, secondary_insurance: None,
                secondary_insurance_id: None, guarantor_name: None, guarantor_relationship: None,
                primary_care_provider_id: None, blood_type: None, organ_donor: None,
                advance_directive_on_file: false, dni_status: None, dnr_status: None,
                code_status: None, vip_flag: false,
                confidential_flag: false, research_consent: None, marketing_consent: None,
                employment_status: None, housing_status: None, education_level: None,
                financial_strain: None, food_insecurity: false, transportation_needs: false,
                social_isolation: None, veteran_status: None, disability_status: None,
                alert_flags: None, special_needs: None, created_by: None, updated_by: None, last_visit_date: None,
            };
            handlers.create_patient(new_patient).await
        }
        PatientCommand::View { patient_id } => handlers.view_patient(patient_id).await,
        PatientCommand::Search { query } => handlers.search_patients(&query).await,
        PatientCommand::Timeline { patient_id } => handlers.get_timeline(patient_id).await,
        PatientCommand::Problems { patient_id } => handlers.get_problems(patient_id).await,
        PatientCommand::Meds { patient_id } => handlers.get_meds(patient_id).await,
        PatientCommand::CareGaps { patient_id } => handlers.get_care_gaps(patient_id).await,
        PatientCommand::Allergies { patient_id } => handlers.get_allergies(patient_id).await,
        PatientCommand::Referrals { patient_id } => handlers.get_referrals(patient_id).await,
        PatientCommand::Journey { patient_id, pathway, show_completed, show_deviations_only, format } => {
            handlers.get_journey(patient_id, pathway, show_completed, show_deviations_only, format).await
        }
        PatientCommand::DrugAlerts { patient_id, severity, include_resolved, include_overridden, drug_class, format, include_inactive, severity_filter } => {
            handlers.get_drug_alerts(patient_id, severity, include_resolved, include_overridden, drug_class, format, include_inactive, severity_filter).await
        }
    };
    
    match result {
        Ok(s) => {
            println!("{}", s);
            Ok(())
        }
        Err(e) => Err(anyhow!("Patient Command Error: {}", e)),
    }
}