//! Master Patient Index (MPI) Command Handlers
//!
//! This module implements a comprehensive MPI system following healthcare industry standards.
//! The MPI serves as the authoritative source for patient identity data, ensuring accurate
//! patient identification and linking across disparate healthcare systems.
//!
//! Core MPI Functions:
//! - Patient Matching: Probabilistic and deterministic matching algorithms
//! - Identity Linking: Cross-reference external identifiers (MRN, SSN, etc.)
//! - Record Merging: Consolidate duplicate patient records with conflict resolution
//! - Audit Trail: Complete history of identity changes and merge operations
//! - Golden Record: Maintain single source of truth for each patient
use async_trait::async_trait;
use tokio::sync::{Mutex as TokioMutex};
use chrono::{DateTime, Datelike, NaiveDate, Utc};
use std::sync::Arc;
// Removed: tokio::sync::Mutex as TokioMutex (no longer used in local storage init)
use lib::commands::{ MPICommand, NameMatchAlgorithm };
// NOTE: Assuming GraphService is accessible via the lib crate structure based on panic trace
use lib::graph_engine::GraphService;
use lib::storage_engine::{ GraphStorageEngine, GLOBAL_STORAGE_ENGINE };
use models::medical::{MasterPatientIndex, Patient, Address};
use medical_knowledge::mpi_identity_resolution::{MpiIdentityResolutionService, PatientCandidate};
use models::errors::GraphError;
use models::identifiers::Identifier;
use crate::cli::{ get_storage_engine_singleton };

// Placeholder types needed for function signatures
type PatientId = String;
type Timeframe = String; 

// =========================================================================
// MPI Handler Implementation
// =========================================================================
#[derive(Clone)]
pub struct MPIHandlers {
    mpi_service: Arc<MpiIdentityResolutionService>,
}

// NOTE: The `get_configured_storage_engine` helper function and all related
// storage engine initialization logic have been removed as storage configuration
// is handled by the main CLI entry point (cli.rs).


/// Retrieves and initializes the MpiIdentityResolutionService singleton.
/// This function is now deprecated in favor of explicit initialization in `MPIHandlers::new`.
pub async fn get_mpi_service_singleton() -> Result<Arc<MpiIdentityResolutionService>, GraphError> {
    // Rely on the service being initialized by the CLI's main logic
    let mpi_service = MpiIdentityResolutionService::get().await;

    // This block is left mostly as-is from the user's snippet, but noted for deprecation
    // in favor of the full explicit dependency chain in MPIHandlers::new/new_with_storage.
    // In a real application, this function should be removed or refactored.
    Ok(mpi_service)
}

impl MPIHandlers {
    /// Initialize the MPI handler by explicitly building the dependency chain
    /// Used in CLI context where global services are NOT pre-initialized.
    pub async fn new() -> Result<Self, GraphError> {
        println!("Initializing Master Patient Index (MPI) service...");

        // 1. Get storage singleton
        let storage: Arc<dyn GraphStorageEngine> = get_storage_engine_singleton().await?;
        println!("Storage dependency retrieved from singleton.");

        // 2. Initialize GraphService globally
        GraphService::global_init(storage.clone()).await?;
        println!("GraphService initialized globally.");

        // 3. Initialize MPI service globally (requires the GraphService instance)
        // FIX: Use '?' to unwrap the Result returned by GraphService::get().await
        let graph_service = GraphService::get().await?;
        MpiIdentityResolutionService::global_init(graph_service).await
            .map_err(|e| GraphError::InternalError(format!("Failed to initialize MPI service: {}", e)))?;
        println!("MpiIdentityResolutionService initialized globally.");

        // 4. Get the initialized service — returns Arc directly, no Option
        let mpi_service: Arc<MpiIdentityResolutionService> = MpiIdentityResolutionService::get().await;

        println!("Master Patient Index (MPI) service ready.");

        Ok(Self { mpi_service })
    }

    /// Initialize using externally provided storage (for tests/scripts)
    pub async fn new_with_storage(storage: Arc<dyn GraphStorageEngine>) -> Result<Self, GraphError> {
        println!("Initializing MPI service with injected storage...");

        println!("Storage dependency injected.");

        // Initialize the global services using the provided storage
        GraphService::global_init(storage).await?;
        println!("GraphService initialized globally.");

        // Initialize MPI service globally (requires the GraphService instance)
        // FIX: Use '?' to unwrap the Result returned by GraphService::get().await
        let graph_service = GraphService::get().await?;
        MpiIdentityResolutionService::global_init(graph_service).await
            .map_err(|e| GraphError::InternalError(format!("Failed to initialize MPI service: {}", e)))?;
        println!("MpiIdentityResolutionService initialized globally.");

        // Safe: we just called global_init(), so get() will succeed
        let mpi_service: Arc<MpiIdentityResolutionService> = MpiIdentityResolutionService::get().await;

        println!("Master Patient Index (MPI) service ready.");

        Ok(Self { mpi_service })
    }

    /// Main command router - dispatches to appropriate handler
    pub async fn handle(&self, command: MPICommand) -> Result<(), GraphError> {
        match command {
            MPICommand::Match {
                name,
                dob,
                address,
                phone,
                name_algo, // NEW: Added name_algo
            } => self.handle_match(name, dob, address, phone, name_algo).await, // NEW: Updated call signature
            MPICommand::Link {
                master_id,
                external_id,
                id_type,
            } => self.handle_link(master_id, external_id, id_type).await,
            MPICommand::Merge {
                source_id,
                target_id,
                resolution_policy,
            } => self.handle_merge(source_id, target_id, resolution_policy).await,
            MPICommand::Audit { mpi_id, timeframe } => self.handle_audit(mpi_id, timeframe).await,

            // Handle Split command
            MPICommand::Split {
                merged_id,
                new_patient_data_json,
                reason,
            } => {
                // Deserialize the JSON string into the Patient struct
                let new_patient_data: Patient = serde_json::from_str(&new_patient_data_json)
                    // Convert serde_json error into GraphError
                    .map_err(|e| GraphError::InternalError(format!("Failed to parse new patient data JSON: {}", e)))?;

                // Pass the deserialized Patient to the handler
                self.handle_split(merged_id, new_patient_data, reason).await
            }
            
            // Handle GetGoldenRecord command
            MPICommand::GetGoldenRecord { patient_id } => {
                // Check for error and discard the returned Patient object on success
                self.handle_get_golden_record(patient_id).await?;
                Ok(())
            }
        }
    }

    // =========================================================================
    // Core MPI Operations
    // =========================================================================
    /// Match - Find potential duplicate records using probabilistic matching
    ///
    /// This implements the core patient matching logic that:
    /// 1. Accepts patient demographics
    /// 2. Runs probabilistic matching algorithm
    /// 3. Returns scored candidates with match confidence
    ///
    /// Match scores typically use:
    /// - Exact matches on key identifiers (SSN, MRN)
    /// - Fuzzy matching on names (Soundex, Levenshtein distance)
    /// - Date of birth comparison (exact or close matches)
    /// - Address similarity
    /// - Phone number matching
    /// Match - Find potential duplicate records using probabilistic matching
    // =========================================================================
    // Core MPI Operations
    // =========================================================================
    /// Match - Find potential duplicate records using probabilistic matching
    async fn handle_match(
        &self,
        name: String,
        dob: Option<String>,
        address: Option<String>,
        phone: Option<String>,
        name_algo: NameMatchAlgorithm,
    ) -> Result<(), GraphError> {
        println!("=== MPI Patient Matching ===");
        println!("Searching for potential matches...\n");

        // --- 1. Generate Blocking Keys from raw input strings ---
        // Extract DOB as a string slice for the key generation function
        let dob_str = dob.as_ref().map(|s| s.as_str()).unwrap_or("");
        
        // **CRITICAL FIX: Fix the syntax here.**
        // 1. Remove the line `let search_keys = self`
        // 2. Call the static method directly on the type and assign the result.
        let search_keys = MpiIdentityResolutionService::get_blocking_keys_for_match(&name, dob_str) 
            .map_err(|e| GraphError::InternalError(e))?;

        // --- 2. Build Patient struct for full scoring (demographics) ---
        let patient = self.build_search_patient(name, dob, address, phone)?;

        // ... (Printing of Search Criteria remains the same)
        println!("Search Criteria:");
        println!(" Name: {} {}", patient.first_name, patient.last_name);
        
        if patient.date_of_birth.naive_utc().date().year() > 1900 {
            println!(" DOB: {}", patient.date_of_birth.format("%Y-%m-%d"));
        } else {
            println!(" DOB: Not Provided");
        }
        
        if let Some(ref addr) = patient.address {
            println!(" Address: {}", addr.address_line1);
        }
        if let Some(ref p) = patient.phone_mobile {
            println!(" Phone: {}", p);
        }

        println!(" Name Match Algorithm: {:?}", name_algo);
        // This is now fixed as search_keys is correctly Vec<String>
        println!(" Blocking Keys: {:?}", search_keys); 

        println!();

        // --- 3. Execute Match with Keys ---
        let candidates = self
            .mpi_service
            // search_keys is now correctly passed as Vec<String>
            .run_probabilistic_match(&patient, search_keys) 
            .await
            .map_err(|e| GraphError::InternalError(e.to_string()))?;

        self.display_match_results(candidates);
        Ok(())
    }

    /// Link - Associate external identifiers with MPI master record
    async fn handle_link(
        &self,
        master_id: String,
        external_id: String,
        id_type: String,
    ) -> Result<(), GraphError> {
        println!("=== MPI Identity Linking ===");
        println!("Linking external identifier to master record...\n");

        let master_mpi_id = master_id;

        println!("Master Patient ID: {}", master_mpi_id);
        println!("External ID: {} (Type: {})", external_id, id_type);
        println!();
        
        self.validate_identifier_type(&id_type)?;
        
        let result = self
            .mpi_service
            .link_external_identifier(master_mpi_id.clone(), external_id.clone(), id_type.clone())
            .await;
        match result {
            Ok(record) => {
                println!("✓ Link successful");
                println!(" MPI Record ID: {}", record.id);
                println!(
                    " External ID: {} → Master ID: {}",
                    external_id, master_mpi_id
                );
                println!(" Identifier Type: {}", id_type);
                println!("\nCross-reference established. Patient can now be found using this identifier.");
            }
            Err(e) => {
                eprintln!("✗ Link failed: {}", e);
                eprintln!("\nPossible causes:");
                eprintln!(" - Master patient ID does not exist");
                eprintln!(" - External ID already linked to different patient");
                eprintln!(" - Invalid identifier type");
                return Err(GraphError::InternalError(e.to_string()));
            }
        }
        Ok(())
    }
    
    /// Merge - Consolidate duplicate patient records into golden record
    async fn handle_merge(
        &self,
        source_id: String,
        target_id: String,
        resolution_policy: String,
    ) -> Result<(), GraphError> {
        println!("=== MPI Record Merge ===");
        println!("Consolidating duplicate patient records...\n");
        
        let source_mpi_id = source_id;
        let target_mpi_id = target_id;
        
        self.validate_resolution_policy(&resolution_policy)?;

        println!("Source Record (will be merged): {}", source_mpi_id);
        println!("Target Record (survivor): {}", target_mpi_id);
        println!("Conflict Resolution Policy: {}", resolution_policy);
        println!();

        println!("⚠ WARNING: This operation will:");
        println!(" 1. Transfer all clinical data from source to target");
        println!(" 2. Redirect all external identifiers to target");
        println!(" 3. Mark source record as MERGED (inactive)");
        println!(" 4. Create permanent audit trail");
        println!();
        
        let result = self
            .mpi_service
            .manual_merge_records(
                source_mpi_id.clone(),
                target_mpi_id.clone(),
                resolution_policy.clone(),
            )
            .await;
        match result {
            Ok(_golden_record) => {
                println!("✓ Merge completed successfully");
                println!(
                    " Source ID {} → Target ID {}",
                    source_mpi_id, target_mpi_id
                );
                println!(" Policy Applied: {}", resolution_policy);
                println!(" Golden Record Created: Yes");
                println!("\nAll future queries for source ID will return target record.");
                println!("Merge can be audited but cannot be automatically reversed.");
            }
            Err(e) => {
                eprintln!("✗ Merge failed: {}", e);
                eprintln!("\nPossible causes:");
                eprintln!(" - One or both patient IDs do not exist");
                eprintln!(" - Records are already merged");
                eprintln!(" - Source and target are the same record");
                eprintln!(" - Business rules prevent this merge");
                return Err(GraphError::InternalError(e.to_string()));
            }
        }
        Ok(())
    }
    
    /// Audit - Retrieve complete history of identity changes
    async fn handle_audit(
        &self,
        mpi_id: String,
        timeframe: Option<String>,
    ) -> Result<(), GraphError> {
        println!("=== MPI Audit Trail ===");
        println!("Retrieving identity change history...\n");

        let patient_mpi_id = mpi_id;
        
        println!("Patient MPI ID: {}", patient_mpi_id);
        if let Some(ref tf) = timeframe {
            println!("Timeframe Filter: {}", tf);
        } else {
            println!("Timeframe: All history");
        }
        println!();
        
        let changes = self
            .mpi_service
            .get_audit_trail(patient_mpi_id, timeframe)
            .await
            .map_err(|e| GraphError::InternalError(e.to_string()))?;
        if changes.is_empty() {
            println!("No audit records found.");
            println!("\nPossible reasons:");
            println!(" - Patient ID does not exist");
            println!(" - No identity changes have occurred");
            println!(" - Timeframe filter excludes all events");
        } else {
            println!("Found {} audit event(s):\n", changes.len());
            println!(
                "{:<20} | {:<15} | {:<30} | {}",
                "Timestamp", "Event Type", "Description", "User"
            );
            println!("{}", "-".repeat(90));

            let num_changes = changes.len();
            for change in changes {
                // NOTE: Assuming the Display trait is implemented for the audit change type
                println!("{}", change); 
            }

            println!("\n{} total events", num_changes);
        }
        Ok(())
    }

    // =========================================================================
    // Helper Functions
    // =========================================================================

    /// Build a minimal Patient struct for searching/matching
    fn build_search_patient(
        &self,
        name: String,
        // UPDATED: Now accepts optional demographics
        dob: Option<String>,
        address_str: Option<String>, 
        phone: Option<String>,
    ) -> Result<Patient, GraphError> {
        use chrono::{NaiveDate, Utc};

        // Parse name components
        let parts: Vec<&str> = name.split_whitespace().collect();
        if parts.is_empty() {
            return Err(GraphError::InvalidRequest("Name cannot be empty".into()));
        }

        let first_name = parts[0].to_string();
        let middle_name = if parts.len() > 2 {
            Some(parts[1..parts.len() - 1].join(" "))
        } else {
            None
        };
        let last_name = parts.last().unwrap().to_string();

        // Parse and convert date of birth to DateTime<Utc>
        let date_of_birth = dob
            .and_then(|s| NaiveDate::parse_from_str(&s, "%Y-%m-%d").ok())
            // Use a sentinel date if DOB is missing or invalid (1900-01-01)
            .unwrap_or_else(|| NaiveDate::from_ymd_opt(1900, 1, 1).unwrap()) 
            .and_hms_opt(0, 0, 0)
            .ok_or_else(|| GraphError::InvalidRequest("Invalid date".into()))?
            .and_utc();

        // Create Address struct (only if address_str is provided)
        let address_tuple = address_str.map(|address_line1| {
            // NOTE: Using placeholder values for city, state, etc., as before.
            Address::new(
                address_line1,
                None,
                "Unknown".to_string(),
                Identifier::new("US".to_string()).unwrap(),
                "00000".to_string(),
                "US".to_string(),
            )
        });

        let now = Utc::now();
        
        // Return minimal Patient for matching
        Ok(Patient {
            // Primary identifiers
            id: rand::random(),
            user_id: None,
            mrn: None,
            ssn: None,

            // Demographics - critical for matching
            first_name,
            middle_name,
            last_name,
            suffix: None,
            preferred_name: None,
            date_of_birth,
            date_of_death: None,
            gender: "Unknown".to_string(),
            
            // Contact - used for matching
            address_id: address_tuple.as_ref().map(|a| a.id),
            address: address_tuple,
            phone_mobile: phone,
            
            // Administrative
            patient_status: "ACTIVE".to_string(),
            vip_flag: false,
            confidential_flag: false,
            
            // Audit Trail
            created_at: now,
            updated_at: now,
            created_by: None,
            updated_by: None,
            last_visit_date: None,
            
            // FIX: Use struct update syntax to fill in all missing fields with defaults.
            // This requires 'Patient' to implement #[derive(Default)].
            ..Default::default() 
        })
    }

    /// Display formatted match results with confidence scores (no changes)
    fn display_match_results(&self, candidates: Vec<PatientCandidate>) {
        // ... (function body remains the same) ...
        if candidates.is_empty() {
            println!("No matches found.");
            println!("\nThis could mean:");
            println!(" - Patient is not in the system (new patient)");
            println!(" - Search criteria are too specific");
            println!(" - Patient data has changed significantly");
            return;
        }
        println!("Found {} potential match(es):\n", candidates.len());
        println!(
            "{:<12} | {:<10} | {:<40} | {}",
            "Patient ID", "Score", "Vertex ID", "Confidence"
        );
        println!("{}", "-".repeat(90));
        for candidate in candidates {
            let confidence = match candidate.match_score {
                s if s >= 0.95 => "Exact Match",
                s if s >= 0.85 => "High Confidence",
                s if s >= 0.70 => "Probable Match",
                s if s >= 0.50 => "Possible Match",
                _ => "Low Confidence",
            };
            println!(
                "{:<12} | {:<10.3} | {:<40} | {}",
                candidate.patient_id,
                candidate.match_score,
                candidate.patient_vertex_id,
                confidence
            );
        }
        println!("\nMatch Score Interpretation:");
        println!(" ≥ 0.95 - Exact match, safe to auto-link");
        println!(" ≥ 0.85 - High confidence, recommend review");
        println!(" ≥ 0.70 - Probable match, requires verification");
        println!(" ≥ 0.50 - Possible match, manual review required");
        println!(" < 0.50 - Low confidence, likely different patient");
    }

    /// Validate identifier type is recognized by the system (no changes)
    fn validate_identifier_type(&self, id_type: &str) -> Result<(), GraphError> {
        let valid_types = vec![
            "MRN",      // Medical Record Number
            "SSN",      // Social Security Number
            "DL",       // Driver's License
            "PASSPORT", // Passport Number
            "INS",      // Insurance Member ID
            "MILITARY", // Military Service Number
            "MEDICAID", // Medicaid ID
            "MEDICARE", // Medicare ID
            "NPI",      // National Provider Identifier
            "OTHER",    // Other identifier types
        ];
        if !valid_types.contains(&id_type) {
            return Err(GraphError::InvalidRequest(format!(
                "Invalid identifier type '{}'. Valid types: {}",
                id_type,
                valid_types.join(", ")
            )));
        }
        Ok(())
    }

    /// Validate merge resolution policy (no changes)
    fn validate_resolution_policy(&self, policy: &str) -> Result<(), GraphError> {
        let valid_policies = vec![
            "MOST_RECENT",      // Use most recently updated values
            "MOST_COMPLETE",    // Prefer non-null values
            "SOURCE_PRIORITY",  // Always take source values
            "TARGET_PRIORITY",  // Always keep target values
            "MANUAL",           // Require human review
        ];
        if !valid_policies.contains(&policy) {
            return Err(GraphError::InvalidRequest(format!(
                "Invalid resolution policy '{}'. Valid policies: {}",
                policy,
                valid_policies.join(", ")
            )));
        }
        Ok(())
    }

    // =========================================================================
    // Identity Management Operations
    // =========================================================================

    /// Split - Reverse a previous merge operation (Unmerge)
    async fn handle_split(
        &self,
        merged_id: String,
        new_patient_data: Patient,
        reason: String,
    ) -> Result<(), GraphError> {
        println!("=== MPI Identity Split (Unmerge) ===");
        println!("Reversing a previous merge operation...\n");
        
        let target_mpi_id = merged_id;
        
        println!("Merged Record to Split: {}", target_mpi_id);
        println!("New Patient Data ID: {}", new_patient_data.id);
        println!("Reason for Split: {}", reason);
        println!();

        println!("⚠ WARNING: This operation will:");
        println!(" 1. Create a new Patient record with new ID.");
        println!(" 2. Link the new record back to the merged record.");
        println!(" 3. **NOTE**: Clinical data is NOT automatically migrated on split (manual step).");
        println!(" 4. Create permanent audit trail.");
        println!();

        let result = self
            .mpi_service
            .identity_split(
                target_mpi_id.clone(),
                new_patient_data,
                reason,
            )
            .await;

        match result {
            Ok(new_vertex_id) => {
                println!("✓ Identity Split completed successfully");
                println!(
                    " Original Merged ID: {} → New Patient Vertex ID: {}",
                    target_mpi_id, new_vertex_id
                );
                println!(" New Patient record is now ACTIVE.");
            }
            Err(e) => {
                eprintln!("✗ Identity Split failed: {}", e);
                eprintln!("\nPossible causes:");
                eprintln!(" - Merged patient ID does not exist");
                return Err(GraphError::InternalError(e.to_string()));
            }
        }
        Ok(())
    }

    /// Get Golden Record - Retrieve the authoritative patient record
    ///
    /// The return type is updated to reflect the need to return `Result<(), GraphError>`
    /// from the top-level `handle` function, rather than the `Patient` struct itself.
    /// Get Golden Record - Retrieve the authoritative patient record
    async fn handle_get_golden_record(
        &self,
        patient_id: String,
    ) -> Result<(), GraphError> {
        println!("=== MPI Golden Record Retrieval ===");
        println!("Retrieving consolidated record for patient ID: {}\n", patient_id);
        
        // Call the new service method
        let result = self.mpi_service.get_golden_record(patient_id.clone()).await; 

        match result {
            Ok(golden_patient) => {
                println!("✓ Golden Record successfully retrieved.");
                println!(" Full Name: {} {}", golden_patient.first_name, golden_patient.last_name);
                println!(" Date of Birth: {}", golden_patient.date_of_birth.format("%Y-%m-%d"));
                println!(" MRN: {}", golden_patient.mrn.as_deref().unwrap_or("N/A"));
                
                // Further logic to print address, phone, etc., would go here.
                
                Ok(())
            }
            Err(e) => {
                eprintln!("✗ Failed to retrieve Golden Record for {}: {}", patient_id, e);
                Err(GraphError::NotFoundError(format!("Golden Record not found or retrieval failed: {}", e)))
            }
        }
    }
}

// =========================================================================
// Additional MPI Operations (Future Enhancement)
// =========================================================================
impl MPIHandlers {
    /// Search - Find patients by various criteria
    /// Future: Implement search by MRN, SSN, phone, email, etc.
    #[allow(dead_code)]
    async fn search_by_identifier(
        &self,
        identifier: String,
        id_type: String,
    ) -> Result<Option<Patient>, GraphError> {
        // TODO: Implement identifier-based search
        todo!("Search by identifier not yet implemented")
    }
    /// Unmerge - Split previously merged records (rare operation)
    /// Requires special permissions and creates audit trail
    #[allow(dead_code)]
    async fn unmerge_records(
        &self,
        merged_id: i32,
        reason: String,
    ) -> Result<(), GraphError> {
        // TODO: Implement unmerge functionality
        // This is a sensitive operation requiring special authorization
        todo!("Unmerge operation not yet implemented")
    }
    /// Get Golden Record - Retrieve the authoritative patient record
    /// This returns the consolidated "single source of truth" for a patient
    #[allow(dead_code)]
    async fn get_golden_record(
        &self,
        patient_id: i32,
    ) -> Result<Patient, GraphError> {
        // TODO: Implement golden record retrieval
        todo!("Golden record retrieval not yet implemented")
    }
    /// Batch Match - Process multiple patients for matching
    /// Used for initial data loads or nightly batch processing
    #[allow(dead_code)]
    async fn batch_match(
        &self,
        patients: Vec<Patient>,
    ) -> Result<Vec<(Patient, Vec<PatientCandidate>)>, GraphError> {
        // TODO: Implement batch matching
        todo!("Batch matching not yet implemented")
    }
    /// Data Quality Report - Analyze MPI for duplicate rates
    /// Generates statistics on match quality and duplicate records
    #[allow(dead_code)]
    async fn generate_quality_report(&self) -> Result<String, GraphError> {
        // TODO: Implement quality reporting
        // Should include:
        // - Total patients in MPI
        // - Estimated duplicate rate
        // - Orphaned external identifiers
        // - Records needing review
        todo!("Quality reporting not yet implemented")
    }
}
// =========================================================================
// Interactive Mode Handlers (standalone functions, outside impl)
// =========================================================================
/// Handle MPI Match command in interactive mode
pub async fn handle_mpi_match_interactive(
    name: String,
    // FIX: Change String to Option<String> to match handle_match
    dob: Option<String>,
    // FIX: Change String to Option<String> to match handle_match
    address: Option<String>,
    phone: Option<String>,
    name_algo: NameMatchAlgorithm,
    mpi_handlers: Arc<TokioMutex<Option<MPIHandlers>>>,
) -> Result<(), GraphError> {
    let handlers_guard = mpi_handlers.lock().await;
    
    match handlers_guard.as_ref() {
        Some(handlers) => {
            // This call now matches the expected signature
            handlers.handle_match(name, dob, address, phone, name_algo).await
        }
        None => {
            eprintln!("Error: MPI handlers not initialized");
            eprintln!("Please ensure the MPI service is running");
            Err(GraphError::InternalError("MPI handlers not available".into()))
        }
    }
}

/// Handle MPI Link command in interactive mode
pub async fn handle_mpi_link_interactive(
    master_id: String,
    external_id: String,
    id_type: String,
    mpi_handlers: Arc<TokioMutex<Option<MPIHandlers>>>,
) -> Result<(), GraphError> {
    let handlers_guard = mpi_handlers.lock().await;
    
    match handlers_guard.as_ref() {
        Some(handlers) => {
            handlers.handle_link(master_id, external_id, id_type).await
        }
        None => {
            eprintln!("Error: MPI handlers not initialized");
            eprintln!("Please ensure the MPI service is running");
            Err(GraphError::InternalError("MPI handlers not available".into()))
        }
    }
}
/// Handle MPI Merge command in interactive mode
pub async fn handle_mpi_merge_interactive(
    source_id: String,
    target_id: String,
    resolution_policy: String,
    mpi_handlers: Arc<TokioMutex<Option<MPIHandlers>>>,
) -> Result<(), GraphError> {
    let handlers_guard = mpi_handlers.lock().await;
    
    match handlers_guard.as_ref() {
        Some(handlers) => {
            // Display confirmation prompt for merge operation
            println!("\n⚠️ WARNING: Merge is a destructive operation!");
            println!("This will consolidate patient {} into patient {}", source_id, target_id);
            println!("Type 'CONFIRM' to proceed, or anything else to cancel:");
            
            let mut input = String::new();
            if let Err(e) = std::io::stdin().read_line(&mut input) {
                eprintln!("Failed to read confirmation: {}", e);
                return Err(GraphError::InternalError("Failed to read input".into()));
            }
            
            if input.trim() != "CONFIRM" {
                println!("Merge operation cancelled.");
                return Ok(());
            }
            
            handlers.handle_merge(source_id, target_id, resolution_policy).await
        }
        None => {
            eprintln!("Error: MPI handlers not initialized");
            eprintln!("Please ensure the MPI service is running");
            Err(GraphError::InternalError("MPI handlers not available".into()))
        }
    }
}
/// Handle MPI Audit command in interactive mode
pub async fn handle_mpi_audit_interactive(
    mpi_id: String,
    timeframe: Option<String>,
    mpi_handlers: Arc<TokioMutex<Option<MPIHandlers>>>,
) -> Result<(), GraphError> {
    let handlers_guard = mpi_handlers.lock().await;
    
    match handlers_guard.as_ref() {
        Some(handlers) => {
            handlers.handle_audit(mpi_id, timeframe).await
        }
        None => {
            eprintln!("Error: MPI handlers not initialized");
            eprintln!("Please ensure the MPI service is running");
            Err(GraphError::InternalError("MPI handlers not available".into()))
        }
    }
}

/// Handle MPI Split command in interactive mode (New addition)
pub async fn handle_mpi_split_interactive(
    merged_id: String,
    new_patient_data: Patient,
    reason: String,
    mpi_handlers: Arc<TokioMutex<Option<MPIHandlers>>>,
) -> Result<(), GraphError> {
    let handlers_guard = mpi_handlers.lock().await;
    
    match handlers_guard.as_ref() {
        Some(handlers) => {
            // Display confirmation prompt for split operation
            println!("\n⚠️ WARNING: Split is a sensitive operation!");
            println!("This will create a new patient record to reverse the merge of {}", merged_id);
            println!("Type 'CONFIRM' to proceed, or anything else to cancel:");
            
            let mut input = String::new();
            if let Err(e) = std::io::stdin().read_line(&mut input) {
                eprintln!("Failed to read confirmation: {}", e);
                return Err(GraphError::InternalError("Failed to read input".into()));
            }
            
            if input.trim() != "CONFIRM" {
                println!("Split operation cancelled.");
                return Ok(());
            }
            
            handlers.handle_split(merged_id, new_patient_data, reason).await
        }
        None => {
            eprintln!("Error: MPI handlers not initialized");
            eprintln!("Please ensure the MPI service is running");
            Err(GraphError::InternalError("MPI handlers not available".into()))
        }
    }
}

/// Handle MPI Get Golden Record command in interactive mode
pub async fn handle_get_golden_record_interactive(
    patient_id: PatientId, // String
    mpi_handlers: Arc<TokioMutex<Option<MPIHandlers>>>,
) -> Result<(), GraphError> {
    let handlers_guard = mpi_handlers.lock().await;

    match handlers_guard.as_ref() {
        Some(handlers) => {
            // Call the implementation method within MPIHandlers
            let _record = handlers.handle_get_golden_record(patient_id.clone()).await?;
            println!("Golden Record for Patient {} retrieved successfully.", patient_id);
            Ok(())
        }
        None => {
            eprintln!("Error: MPI handlers not initialized");
            eprintln!("Please ensure the MPI service is running");
            Err(GraphError::InternalError("MPI handlers not available".into()))
        }
    }
}

// =========================================================================
// Non-Interactive Mode Handlers (standalone functions)
// =========================================================================

/// Handle MPI Split command in non-interactive mode (scripts, tests, API, etc.)
pub async fn handle_mpi_split(
    storage: Arc<dyn GraphStorageEngine>,
    merged_id: String,
    new_patient_data: Patient,
    reason: String,
) -> Result<(), GraphError> {
    let handlers = MPIHandlers::new_with_storage(storage).await?;
    handlers.handle_split(merged_id, new_patient_data, reason).await
}

/// Handle MPI Get Golden Record command in non-interactive mode
pub async fn handle_get_golden_record(
    storage: Arc<dyn GraphStorageEngine>,
    patient_id: PatientId, // String
) -> Result<(), GraphError> {
    let handlers = MPIHandlers::new_with_storage(storage).await?;
    let _record = handlers.handle_get_golden_record(patient_id).await?;
    println!("Golden Record retrieved successfully.");
    Ok(())
}

// /// Handle MPI Match command in non-interactive mode (scripts, tests, API, etc.)
// pub async fn handle_mpi_match(
//     storage: Arc<dyn GraphStorageEngine>,  // Take by value (ownership)
//     name: String,
//     dob: String, // <- FIX: Changed to Option<String>
//     address: String, // <- FIX: Changed to Option<String>
//     phone: Option<String>,
// ) -> Result<(), GraphError> { // <- The original function only took 5 arguments and required dob/address
/// Handle MPI Match command in non-interactive mode (scripts, tests, API, etc.)
pub async fn handle_mpi_match(
    storage: Arc<dyn GraphStorageEngine>,  // Take by value (ownership)
    name: String,
    // FIX: Change String to Option<String>
    dob: Option<String>,
    // FIX: Change String to Option<String>
    address: Option<String>,
    phone: Option<String>,
    // FIX: Add NameMatchAlgorithm parameter
    name_algo: NameMatchAlgorithm,
) -> Result<(), GraphError> {
    let handlers = MPIHandlers::new_with_storage(storage).await?;
    // FIX: Pass all 5 arguments to the internal handler
    handlers.handle_match(name, dob, address, phone, name_algo).await
}


/// Handle MPI Link command in non-interactive mode
pub async fn handle_mpi_link(
    storage: Arc<dyn GraphStorageEngine>,
    master_id: String,
    external_id: String,
    id_type: String,
) -> Result<(), GraphError> {
    let handlers = MPIHandlers::new_with_storage(storage).await?;
    handlers.handle_link(master_id, external_id, id_type).await
}

/// Handle MPI Merge command in non-interactive mode
pub async fn handle_mpi_merge(
    storage: Arc<dyn GraphStorageEngine>,
    source_id: String,
    target_id: String,
    resolution_policy: String,
) -> Result<(), GraphError> {
    let handlers = MPIHandlers::new_with_storage(storage).await?;
    handlers.handle_merge(source_id, target_id, resolution_policy).await
}

/// Handle MPI Audit command in non-interactive mode
pub async fn handle_mpi_audit(
    storage: Arc<dyn GraphStorageEngine>,
    mpi_id: String,
    timeframe: Option<String>,
) -> Result<(), GraphError> {
    let handlers = MPIHandlers::new_with_storage(storage).await?;
    handlers.handle_audit(mpi_id, timeframe).await
}