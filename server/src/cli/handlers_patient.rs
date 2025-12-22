use lib::graph_engine::graph_service::{GraphService};
use medical_knowledge::patient_service::PatientService;
use models::medical::{Patient, Problem, Prescription, Allergy, Referral, Address};
use models::identifiers::{ Identifier };
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
// INTERNAL BUSINESS LOGIC
// =========================================================================

/// Helper function to parse Date of Birth, trying multiple common formats.
/// Formats supported: "dd-mm-yyyy" and "yyyy-mm-dd".
fn parse_dob(dob_str: &str) -> Result<NaiveDate, anyhow::Error> {
    // Attempt 1: dd-mm-yyyy 
    if let Ok(d) = NaiveDate::parse_from_str(dob_str, "%d-%m-%Y") {
        return Ok(d);
    }
    // Attempt 2: yyyy-mm-dd (ISO 8601 format, commonly used in JSON/APIs)
    if let Ok(d) = NaiveDate::parse_from_str(dob_str, "%Y-%m-%d") {
        return Ok(d);
    }
    
    // Failure case: If neither format worked
    Err(anyhow!(
        "Invalid DOB format ('{}'). Expected formats: 'dd-mm-yyyy' OR 'yyyy-mm-dd'.", 
        dob_str
    ))
}


/// Parses a comma-separated address string into an Address struct.
/// Format assumed (minimum): "line1,city". 
/// Format assumed (full): "line1,city,state_code,postal_code,country"
/// FIX: Made address parsing more flexible to handle fewer than 5 parts, 
/// and to handle state/zip being in the same part.
fn parse_address(raw_address: String) -> Result<Address, anyhow::Error> {
    // Trim the raw input and split. Max splits = 4 to handle the issue where state/zip/country are combined.
    let parts: Vec<String> = raw_address.split(',')
        .map(|s| s.trim().to_string())
        .collect();

    if parts.len() < 2 {
        return Err(anyhow!("Invalid address format: Expected at least 2 comma-separated parts (line1, city), got {}: '{}'", parts.len(), raw_address));
    }

    // Required parts
    let address_line1 = parts[0].clone();
    let city = parts[1].clone();
    
    // Optional parts, initialized to defaults/None
    let mut state_province_str = "UNSPECIFIED".to_string();
    let mut postal_code = "UNSPECIFIED".to_string();
    let mut country = "US".to_string(); // Default to "US" if not specified

    match parts.len() {
        2 => {
            // Case: "line1,city" -> Use defaults for state, zip, country
        }
        3 => {
            // Case: "line1,city,state_zip_country_combined" e.g., "123 Main St, New York, NY 10001"
            let state_zip_str = parts[2].clone();
            let state_zip_parts: Vec<&str> = state_zip_str.split_whitespace().collect();
            
            // Try to separate state and zip (e.g., "NY 10001")
            if state_zip_parts.len() >= 2 {
                state_province_str = state_zip_parts[0].to_string();
                postal_code = state_zip_parts[1].to_string();
            } else if state_zip_parts.len() == 1 {
                // If it's just one item, assume it's the postal code or state and default the other
                if state_zip_str.len() > 5 && state_zip_str.chars().any(|c| c.is_alphabetic()) {
                    // Looks more like a place/country name
                    country = state_zip_str;
                } else {
                    // Assume it's a zip code
                    postal_code = state_zip_str;
                }
            } else {
                // Fall back to defaults
            }
        }
        4 => {
            // Case: "line1,city,state_code,postal_code_or_country"
            state_province_str = parts[2].clone();
            postal_code = parts[3].clone(); // Assuming this is postal code

            // This is ambiguous, we'll keep postal_code and use default country
        }
        _ => { 
            // Case: 5+ parts, assuming full "line1,city,state_code,postal_code,country"
            state_province_str = parts[2].clone();
            postal_code = parts[3].clone();
            country = parts[4].clone();
        }
    }
    
    // --- Final Construction ---
    // StateProvince must be converted to an Identifier
    let state_province = Identifier::new(state_province_str)
        .map_err(|e| anyhow!("Failed to create Identifier for state/province: {}", e))?;

    Ok(Address::new(
        address_line1,
        None, // address_line2 is set to None for simple parsing
        city,
        state_province,
        postal_code,
        country
    ))
}


/// State and logic container for patient-related operations.
/// Encapsulates the PatientService dependency.
#[derive(Clone)]
pub struct PatientHandlers {
    patient_service: Arc<PatientService>,
}

impl PatientHandlers {
    // ... (new, new_with_service, and other handler methods remain unchanged)
    
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
            // --- 1. Determine argument source (Batch vs. Individual Flags) ---
            
            let mut parsed_first_name: Option<String> = None;
            let mut parsed_last_name: Option<String> = None;
            let mut dob_opt: Option<String> = None;
            let mut gender_opt: Option<String> = None;
            let mut ssn_opt: Option<String> = None;
            let mut mrn_opt: Option<String> = None;
            let mut address_raw_opt: Option<String> = None;
            let mut phone_opt: Option<String> = None;

            if let Some(json_str) = &args.batch {
                // CASE 1: Batch JSON is provided (Parsing remains the same)
                match serde_json::from_str::<serde_json::Value>(&json_str) {
                    Ok(json_value) => {
                        parsed_first_name = json_value.get("first_name").and_then(|v| v.as_str()).map(|s| s.to_string());
                        parsed_last_name = json_value.get("last_name").and_then(|v| v.as_str()).map(|s| s.to_string());
                        dob_opt = json_value.get("dob").and_then(|v| v.as_str()).map(|s| s.to_string());
                        gender_opt = json_value.get("gender").and_then(|v| v.as_str()).map(|s| s.to_string());
                        ssn_opt = json_value.get("ssn").and_then(|v| v.as_str()).map(|s| s.to_string());
                        mrn_opt = json_value.get("mrn").and_then(|v| v.as_str()).map(|s| s.to_string());
                        address_raw_opt = json_value.get("address").and_then(|v| v.as_str()).map(|s| s.to_string());
                        phone_opt = json_value.get("phone").and_then(|v| v.as_str()).map(|s| s.to_string());
                    },
                    Err(e) => return format!("Error: Failed to parse batch JSON: {}", e),
                }
            } else {
                // CASE 2: Individual flags were provided (Non-Batch).
                
                // Set non-name fields
                dob_opt = args.dob.clone();
                gender_opt = args.gender.clone(); 
                ssn_opt = args.ssn.clone();
                mrn_opt = args.mrn.clone();
                address_raw_opt = args.address.clone();
                phone_opt = args.phone.clone();

                // Handle Name Parsing Logic (Uses the priority established in CLI parser)
                if args.first_name.is_some() || args.last_name.is_some() {
                    // Highest Priority: Explicit first and last names are provided (even if --name was also passed).
                    parsed_first_name = args.first_name.clone();
                    parsed_last_name = args.last_name.clone();
                } else if let Some(full_name) = &args.name {
                    // Second Priority: Single combined --name flag is provided. Split it.
                    let name_parts: Vec<&str> = full_name.splitn(2, ' ').collect();
                    
                    if name_parts.len() == 2 {
                        // Assume first word is first name, the rest is last name
                        parsed_first_name = Some(name_parts[0].trim().to_string());
                        parsed_last_name = Some(name_parts[1].trim().to_string());
                    } else if name_parts.len() == 1 {
                        // Only one name provided. Use it as first name.
                        parsed_first_name = Some(name_parts[0].trim().to_string());
                        parsed_last_name = None;
                    } 
                } 
            };
            
            // --- 2. Validation and Extraction ---
            
            // Validate that required fields (first name and DOB) are present.
            // Other fields (last_name, gender) are resolved to defaults if missing.
            let (first_name, last_name, dob, gender) = match (parsed_first_name, dob_opt) {
                (Some(fnm), Some(dob_str)) => {
                    // If last name is missing (e.g., from one word in --name), use placeholder.
                    let lnm = parsed_last_name.unwrap_or_else(|| "UNSPECIFIED".to_string());
                    
                    // If gender is missing, use placeholder.
                    let g = gender_opt.unwrap_or_else(|| "UNSPECIFIED".to_string());
                    
                    (fnm, lnm, dob_str, g)
                },
                _ => {
                    // This block catches missing first_name OR missing dob.
                    if args.batch.is_some() {
                        return format!("Error: Batch JSON missing required field(s). Required: first_name, dob");
                    } else {
                        return format!("Error: Missing required field(s). Required: (--name OR --first-name), --dob");
                    }
                }
            };
            
            // Use the new helper function to parse date with multiple formats
            let dob_parsed = match parse_dob(&dob) {
                Ok(d) => d,
                Err(e) => return format!("Error: DOB Parsing Failed - {}", e),
            };

            // Parse raw address string into an Address struct
            let address_opt = match address_raw_opt {
                Some(raw_addr) => match parse_address(raw_addr) {
                    Ok(addr) => Some(addr),
                    Err(e) => return format!("Error: Address Parsing Failed - {}", e),
                },
                None => None,
            };

            // FIX: Replace None/null values for optional string fields with ""
            let ssn_fixed = ssn_opt.unwrap_or_else(|| "".to_string());
            let mrn_fixed = mrn_opt.unwrap_or_else(|| "".to_string());
            let phone_fixed = phone_opt.unwrap_or_else(|| "".to_string());
            
            // --- 3. Build Patient Struct and Call Service ---
            
            let new_patient = Patient {
                // FIX: Use the fixed (non-null) strings
                mrn: Some(mrn_fixed), // Patient struct requires Option<String> but we wrap the fixed string
                ssn: Some(ssn_fixed), 
                
                first_name: Some(first_name), // Guaranteed Some
                last_name: Some(last_name),  // Guaranteed Some (or "UNSPECIFIED")
                // FIX: Correctly assign the wrapped value to the `gender` field.
                gender: Some(gender),
                address: address_opt, 
                // FIX: Use the fixed (non-null) strings
                phone_mobile: Some(phone_fixed),

                // Calculated/Default fields
                id: Some(0), 
                date_of_birth: dob_parsed.and_hms_opt(0, 0, 0).unwrap().and_utc(),
                created_at: Utc::now(),
                updated_at: Utc::now(),
                patient_status: Some("ACTIVE".to_string()), 

                // Default/None fields (These must also be fixed if they are String properties in the DB, 
                // but since we only fixed the command line ones, we'll assume the model fields are 
                // adjusted elsewhere to handle these remaining Nones, or the graph layer is tolerant of them).
                // However, since the error was on ssn/mrn, we must treat them as mandatory strings 
                // (even if empty) for the Cypher query builder.
                vertex_id: None, user_id: None, middle_name: None, suffix: None,
                preferred_name: None,
                date_of_death: None, sex_assigned_at_birth: None, gender_identity: None,
                pronouns: None, address_id: None, phone_home: None,
                phone_work: None, email: None, preferred_contact_method: None,
                preferred_language: None, interpreter_needed: Some(false), emergency_contact_name: None,
                emergency_contact_relationship: None, emergency_contact_phone: None,
                marital_status: None, race: None, ethnicity: None, religion: None,
                primary_insurance: None, primary_insurance_id: None, secondary_insurance: None,
                secondary_insurance_id: None, guarantor_name: None, guarantor_relationship: None,
                primary_care_provider_id: None, blood_type: None, organ_donor: None,
                advance_directive_on_file: Some(false), dni_status: None, dnr_status: None,
                code_status: None, vip_flag: Some(false),
                confidential_flag: Some(false), research_consent: None, marketing_consent: None,
                employment_status: None, housing_status: None, education_level: None,
                financial_strain: None, food_insecurity: Some(false), transportation_needs: Some(false),
                social_isolation: None, veteran_status: None, disability_status: None,
                alert_flags: None, special_needs: None, created_by: None, updated_by: None, last_visit_date: None,
            };
            handlers.create_patient(new_patient).await
        }
        // ... (other PatientCommand variants remain the same)
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
    
    let handlers = PatientHandlers::new().await?;

    let result = match action {
        PatientCommand::Create(args) => {
            // --- 1. Determine argument source (Batch vs. Individual Flags) ---
            
            let mut parsed_first_name: Option<String> = None;
            let mut parsed_last_name: Option<String> = None;
            let mut dob_opt: Option<String> = None;
            let mut gender_opt: Option<String> = None;
            let mut ssn_opt: Option<String> = None;
            let mut mrn_opt: Option<String> = None;
            let mut address_raw_opt: Option<String> = None;
            let mut phone_opt: Option<String> = None;

            if let Some(json_str) = &args.batch {
                // CASE 1: Batch JSON is provided (Parsing remains the same)
                match serde_json::from_str::<serde_json::Value>(&json_str) {
                    Ok(json_value) => {
                        parsed_first_name = json_value.get("first_name").and_then(|v| v.as_str()).map(|s| s.to_string());
                        parsed_last_name = json_value.get("last_name").and_then(|v| v.as_str()).map(|s| s.to_string());
                        dob_opt = json_value.get("dob").and_then(|v| v.as_str()).map(|s| s.to_string());
                        gender_opt = json_value.get("gender").and_then(|v| v.as_str()).map(|s| s.to_string());
                        ssn_opt = json_value.get("ssn").and_then(|v| v.as_str()).map(|s| s.to_string());
                        mrn_opt = json_value.get("mrn").and_then(|v| v.as_str()).map(|s| s.to_string());
                        address_raw_opt = json_value.get("address").and_then(|v| v.as_str()).map(|s| s.to_string());
                        phone_opt = json_value.get("phone").and_then(|v| v.as_str()).map(|s| s.to_string());
                    },
                    Err(e) => return Err(anyhow!("Failed to parse batch JSON: {}", e)),
                }
            } else {
                // CASE 2: Individual flags were provided (Non-Batch).
                
                // Set non-name fields
                dob_opt = args.dob.clone();
                gender_opt = args.gender.clone(); 
                ssn_opt = args.ssn.clone();
                mrn_opt = args.mrn.clone();
                address_raw_opt = args.address.clone();
                phone_opt = args.phone.clone();
                
                // Handle Name Parsing Logic (Uses the priority established in CLI parser)
                if args.first_name.is_some() || args.last_name.is_some() {
                    // Highest Priority: Explicit first and last names are provided (even if --name was also passed).
                    parsed_first_name = args.first_name.clone();
                    parsed_last_name = args.last_name.clone();
                } else if let Some(full_name) = &args.name {
                    // Second Priority: Single combined --name flag is provided. Split it.
                    let name_parts: Vec<&str> = full_name.splitn(2, ' ').collect();
                    
                    if name_parts.len() == 2 {
                        // Assume first word is first name, the rest is last name
                        parsed_first_name = Some(name_parts[0].trim().to_string());
                        parsed_last_name = Some(name_parts[1].trim().to_string());
                    } else if name_parts.len() == 1 {
                        // Only one name provided. Use it as first name.
                        parsed_first_name = Some(name_parts[0].trim().to_string());
                        parsed_last_name = None;
                    } 
                } 
            };
            
            // --- 2. Validation and Extraction ---
            
            // Validate that required fields (first name and DOB) are present.
            // Other fields (last_name, gender) are resolved to defaults if missing.
            let (first_name, last_name, dob, gender) = match (parsed_first_name, dob_opt) {
                (Some(fnm), Some(dob_str)) => {
                    // If last name is missing, use placeholder.
                    let lnm = parsed_last_name.unwrap_or_else(|| "UNSPECIFIED".to_string());
                    
                    // If gender is missing, use placeholder. 
                    let g = gender_opt.unwrap_or_else(|| "UNSPECIFIED".to_string());
                    
                    (fnm, lnm, dob_str, g)
                },
                _ => {
                    // This block catches missing first_name OR missing dob.
                    if args.batch.is_some() {
                        return Err(anyhow!("Batch JSON missing required field(s). Required: first_name, dob"));
                    } else {
                        return Err(anyhow!("Missing required field(s). Required: (--name OR --first-name), --dob"));
                    }
                }
            };
            
            // Use the new helper function to parse date with multiple formats
            let dob_parsed = match parse_dob(&dob) {
                Ok(d) => d,
                Err(e) => return Err(anyhow!("DOB Parsing Failed - {}", e)),
            };
            
            // Parse raw address string into an Address struct
            let address_opt = match address_raw_opt {
                Some(raw_addr) => match parse_address(raw_addr) {
                    Ok(addr) => Some(addr),
                    Err(e) => return Err(anyhow!("Address Parsing Failed - {}", e)),
                },
                None => None,
            };

            // FIX: Replace None/null values for optional string fields with ""
            let ssn_fixed = ssn_opt.unwrap_or_else(|| "".to_string());
            let mrn_fixed = mrn_opt.unwrap_or_else(|| "".to_string());
            let phone_fixed = phone_opt.unwrap_or_else(|| "".to_string());
            
            // --- 3. Build Patient Struct and Call Service ---
            
            let new_patient = Patient {
                // FIX: Use the fixed (non-null) strings
                mrn: Some(mrn_fixed), 
                ssn: Some(ssn_fixed), 
                
                first_name: Some(first_name), // Guaranteed Some
                last_name: Some(last_name),  // Guaranteed Some (or "UNSPECIFIED")
                // FIX: Correctly assign the wrapped value to the `gender` field.
                gender: Some(gender),
                
                address: address_opt, 
                // FIX: Use the fixed (non-null) strings
                phone_mobile: Some(phone_fixed),

                id: Some(0), 
                date_of_birth: dob_parsed.and_hms_opt(0, 0, 0).unwrap().and_utc(),
                created_at: Utc::now(),
                updated_at: Utc::now(),
                patient_status: Some("ACTIVE".to_string()), 

                vertex_id: None, user_id: None, middle_name: None, suffix: None,
                preferred_name: None,
                date_of_death: None, sex_assigned_at_birth: None, gender_identity: None,
                pronouns: None, address_id: None, phone_home: None,
                phone_work: None, email: None, preferred_contact_method: None,
                preferred_language: None, interpreter_needed: Some(false), emergency_contact_name: None,
                emergency_contact_relationship: None, emergency_contact_phone: None,
                marital_status: None, race: None, ethnicity: None, religion: None,
                primary_insurance: None, primary_insurance_id: None, secondary_insurance: None,
                secondary_insurance_id: None, guarantor_name: None, guarantor_relationship: None,
                primary_care_provider_id: None, blood_type: None, organ_donor: None,
                advance_directive_on_file: Some(false), dni_status: None, dnr_status: None,
                code_status: None, vip_flag: Some(false),
                confidential_flag: Some(false), research_consent: None, marketing_consent: None,
                employment_status: None, housing_status: None, education_level: None,
                financial_strain: None, food_insecurity: Some(false), transportation_needs: Some(false),
                social_isolation: None, veteran_status: None, disability_status: None,
                alert_flags: None, special_needs: None, created_by: None, updated_by: None, last_visit_date: None,
            };
            handlers.create_patient(new_patient).await
        }
        // ... (other PatientCommand variants remain the same)
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
