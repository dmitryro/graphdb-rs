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
use chrono::{DateTime, NaiveDate, Utc};
use std::sync::Arc;
use tokio::sync::Mutex as TokioMutex;

use lib::commands::MPICommand; 
use models::medical::{MasterPatientIndex, Patient, Address};
use medical_knowledge::mpi_identity_resolution::{MpiIdentityResolutionService, PatientCandidate};
use models::errors::GraphError;
use models::identifiers::Identifier;

// =========================================================================
// MPI Handler Implementation
// =========================================================================

#[derive(Clone)]
pub struct MPIHandlers {
    mpi_service: Arc<MpiIdentityResolutionService>,
}

impl MPIHandlers {
    /// Initialize the MPI handler with the identity resolution service
    pub async fn new() -> Result<Self, GraphError> {
        let mpi_service = MpiIdentityResolutionService::get().await;
        Ok(Self { mpi_service })
    }

    /// Main command router - dispatches to appropriate handler
    pub async fn handle(&self, command: MPICommand) -> Result<(), GraphError> {
        match command {
            MPICommand::Match { name, dob, address, phone } => {
                self.handle_match(name, dob, address, phone).await
            }
            MPICommand::Link { master_id, external_id, id_type } => {
                self.handle_link(master_id, external_id, id_type).await
            }
            MPICommand::Merge { source_id, target_id, resolution_policy } => {
                self.handle_merge(source_id, target_id, resolution_policy).await
            }
            MPICommand::Audit { mpi_id, timeframe } => {
                self.handle_audit(mpi_id, timeframe).await
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
    async fn handle_match(
        &self,
        name: String,
        dob: String,
        address: String,
        phone: Option<String>,
    ) -> Result<(), GraphError> {
        println!("=== MPI Patient Matching ===");
        println!("Searching for potential matches...\n");

        let patient = self.build_search_patient(name, dob, address, phone)?;
        
        println!("Search Criteria:");
        println!("  Name: {} {}", patient.first_name, patient.last_name);
        println!("  DOB: {}", patient.date_of_birth.format("%Y-%m-%d"));
        if let Some(ref addr) = patient.address {
            println!("  Address: {}", addr.address_line1);
        }
        if let Some(ref p) = patient.phone_mobile {
            println!("  Phone: {}", p);
        }
        println!();

        // Run probabilistic matching algorithm
        let candidates = self.mpi_service
            .run_probabilistic_match(&patient)
            .await
            .map_err(|e| GraphError::InternalError(e.to_string()))?;

        self.display_match_results(candidates);
        Ok(())
    }

    /// Link - Associate external identifiers with MPI master record
    /// 
    /// External identifiers include:
    /// - Medical Record Numbers (MRN) from different facilities
    /// - Social Security Numbers (SSN)
    /// - Insurance member IDs
    /// - Driver's license numbers
    /// - Passport numbers
    /// - Previous patient IDs from merged systems
    /// 
    /// This enables cross-referencing across disparate systems
    async fn handle_link(
        &self,
        master_id: String,
        external_id: String,
        id_type: String,
    ) -> Result<(), GraphError> {
        println!("=== MPI Identity Linking ===");
        println!("Linking external identifier to master record...\n");

        let master_id: i32 = master_id.parse()
            .map_err(|_| GraphError::InvalidRequest("master_id must be integer".into()))?;

        println!("Master Patient ID: {}", master_id);
        println!("External ID: {} (Type: {})", external_id, id_type);
        println!();

        // Validate the identifier type
        self.validate_identifier_type(&id_type)?;

        // Link the external identifier to the master record
        let result = self.mpi_service
            .link_external_identifier(master_id, external_id.clone(), id_type.clone())
            .await;

        match result {
            Ok(record) => {
                println!("✓ Link successful");
                println!("  MPI Record ID: {}", record.id);
                println!("  External ID: {} → Master ID: {}", external_id, master_id);
                println!("  Identifier Type: {}", id_type);
                println!("\nCross-reference established. Patient can now be found using this identifier.");
            }
            Err(e) => {
                eprintln!("✗ Link failed: {}", e);
                eprintln!("\nPossible causes:");
                eprintln!("  - Master patient ID does not exist");
                eprintln!("  - External ID already linked to different patient");
                eprintln!("  - Invalid identifier type");
                return Err(GraphError::InternalError(e.to_string()));
            }
        }
        Ok(())
    }

    /// Merge - Consolidate duplicate patient records into golden record
    /// 
    /// The merge operation:
    /// 1. Identifies source (duplicate) and target (survivor) records
    /// 2. Applies conflict resolution policy for conflicting data
    /// 3. Transfers all encounters, orders, results to survivor record
    /// 4. Marks source record as merged/inactive
    /// 5. Creates audit trail of merge operation
    /// 
    /// Resolution policies:
    /// - MOST_RECENT: Use most recently updated values
    /// - MOST_COMPLETE: Prefer non-null values
    /// - SOURCE_PRIORITY: Always take source values
    /// - TARGET_PRIORITY: Always keep target values
    /// - MANUAL: Require human review for conflicts
    async fn handle_merge(
        &self,
        source_id: String,
        target_id: String,
        resolution_policy: String,
    ) -> Result<(), GraphError> {
        println!("=== MPI Record Merge ===");
        println!("Consolidating duplicate patient records...\n");

        let source: i32 = source_id.parse()
            .map_err(|_| GraphError::InvalidRequest("source_id must be integer".into()))?;
        let target: i32 = target_id.parse()
            .map_err(|_| GraphError::InvalidRequest("target_id must be integer".into()))?;

        // Validate the resolution policy
        self.validate_resolution_policy(&resolution_policy)?;

        println!("Source Record (will be merged): {}", source);
        println!("Target Record (survivor): {}", target);
        println!("Conflict Resolution Policy: {}", resolution_policy);
        println!();
        
        // Display warning about merge consequences
        println!("⚠ WARNING: This operation will:");
        println!("  1. Transfer all clinical data from source to target");
        println!("  2. Redirect all external identifiers to target");
        println!("  3. Mark source record as MERGED (inactive)");
        println!("  4. Create permanent audit trail");
        println!();

        // Execute the merge
        let result = self.mpi_service
            .manual_merge_records(source, target, resolution_policy.clone())
            .await;

        match result {
            Ok(_golden_record) => {
                println!("✓ Merge completed successfully");
                println!("  Source ID {} → Target ID {}", source, target);
                println!("  Policy Applied: {}", resolution_policy);
                println!("  Golden Record Created: Yes");
                println!("\nAll future queries for source ID will return target record.");
                println!("Merge can be audited but cannot be automatically reversed.");
            }
            Err(e) => {
                eprintln!("✗ Merge failed: {}", e);
                eprintln!("\nPossible causes:");
                eprintln!("  - One or both patient IDs do not exist");
                eprintln!("  - Records are already merged");
                eprintln!("  - Source and target are the same record");
                eprintln!("  - Business rules prevent this merge");
                return Err(GraphError::InternalError(e.to_string()));
            }
        }
        Ok(())
    }

    /// Audit - Retrieve complete history of identity changes
    /// 
    /// Audit trail includes:
    /// - All merge operations (source, target, timestamp, user)
    /// - Identity links added/removed
    /// - Demographic updates
    /// - Split operations (unmerge)
    /// - Access logs for sensitive records
    /// 
    /// Required for:
    /// - Regulatory compliance (HIPAA, GDPR)
    /// - Quality assurance
    /// - Troubleshooting identity issues
    /// - Legal discovery requests
    async fn handle_audit(
        &self,
        mpi_id: String,
        timeframe: Option<String>,
    ) -> Result<(), GraphError> {
        println!("=== MPI Audit Trail ===");
        println!("Retrieving identity change history...\n");

        let patient_id: i32 = mpi_id.parse()
            .map_err(|_| GraphError::InvalidRequest("mpi_id must be integer".into()))?;

        println!("Patient MPI ID: {}", patient_id);
        if let Some(ref tf) = timeframe {
            println!("Timeframe Filter: {}", tf);
        } else {
            println!("Timeframe: All history");
        }
        println!();

        // Retrieve audit trail
        let changes = self.mpi_service
            .get_audit_trail(patient_id, timeframe)
            .await
            .map_err(|e| GraphError::InternalError(e.to_string()))?;

        if changes.is_empty() {
            println!("No audit records found.");
            println!("\nPossible reasons:");
            println!("  - Patient ID does not exist");
            println!("  - No identity changes have occurred");
            println!("  - Timeframe filter excludes all events");
        } else {
            println!("Found {} audit event(s):\n", changes.len());
            println!("{:<20} | {:<15} | {:<30} | {}", 
                     "Timestamp", "Event Type", "Description", "User");
            println!("{}", "-".repeat(90));
            
            let num_changes = changes.len();
            for change in changes {
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
    /// 
    /// This creates a search template with only the fields needed for matching.
    /// Not all 60+ Patient fields are required for identity resolution.
    fn build_search_patient(
        &self,
        name: String,
        dob: String,
        address_str: String,
        phone: Option<String>,
    ) -> Result<Patient, GraphError> {
        // Parse name components
        let parts: Vec<&str> = name.split_whitespace().collect();
        if parts.len() < 2 {
            return Err(GraphError::InvalidRequest(
                "Name must include at least first and last name".into()
            ));
        }
        
        let first_name = parts[0].to_string();
        let middle_name = if parts.len() > 2 {
            Some(parts[1..parts.len()-1].join(" "))
        } else {
            None
        };
        let last_name = parts.last().unwrap().to_string();
        
        // Parse and convert date of birth to DateTime<Utc>
        let naive_date = NaiveDate::parse_from_str(&dob, "%Y-%m-%d")
            .map_err(|_| GraphError::InvalidRequest(
                "Invalid date format. Use YYYY-MM-DD".into()
            ))?;
        let date_of_birth = naive_date
            .and_hms_opt(0, 0, 0)
            .ok_or_else(|| GraphError::InvalidRequest("Invalid date".into()))?
            .and_utc();

        // Create Address struct
        let address = Address::new(
            address_str,
            None,
            "Unknown".to_string(),
            Identifier::new("US".to_string())
                .map_err(|_| GraphError::InvalidRequest("Invalid identifier".into()))?,
            "00000".to_string(),
            "US".to_string(),
        );

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
            sex_assigned_at_birth: None,
            gender_identity: None,
            pronouns: None,
            
            // Contact - used for matching
            address_id: Some(address.id),
            address: Some(address),
            phone_home: None,
            phone_mobile: phone,
            phone_work: None,
            email: None,
            preferred_contact_method: None,
            preferred_language: None,
            interpreter_needed: false,
            
            // Emergency Contact - not used for matching
            emergency_contact_name: None,
            emergency_contact_relationship: None,
            emergency_contact_phone: None,
            
            // Demographic Details - not critical for matching
            marital_status: None,
            race: None,
            ethnicity: None,
            religion: None,
            
            // Insurance - not used for matching
            primary_insurance: None,
            primary_insurance_id: None,
            secondary_insurance: None,
            secondary_insurance_id: None,
            guarantor_name: None,
            guarantor_relationship: None,
            
            // Clinical - not used for matching
            primary_care_provider_id: None,
            blood_type: None,
            organ_donor: None,
            advance_directive_on_file: false,
            dni_status: None,
            dnr_status: None,
            code_status: None,
            
            // Administrative
            patient_status: "ACTIVE".to_string(),
            vip_flag: false,
            confidential_flag: false,
            research_consent: None,
            marketing_consent: None,
            
            // Social Determinants - not used for matching
            employment_status: None,
            housing_status: None,
            education_level: None,
            financial_strain: None,
            food_insecurity: false,
            transportation_needs: false,
            social_isolation: None,
            veteran_status: None,
            disability_status: None,
            
            // Clinical Alerts - not used for matching
            alert_flags: None,
            special_needs: None,
            
            // Audit Trail
            created_at: now,
            updated_at: now,
            created_by: None,
            updated_by: None,
            last_visit_date: None,
        })
    }

    /// Display formatted match results with confidence scores
    fn display_match_results(&self, candidates: Vec<PatientCandidate>) {
        if candidates.is_empty() {
            println!("No matches found.");
            println!("\nThis could mean:");
            println!("  - Patient is not in the system (new patient)");
            println!("  - Search criteria are too specific");
            println!("  - Patient data has changed significantly");
            return;
        }

        println!("Found {} potential match(es):\n", candidates.len());
        println!("{:<12} | {:<10} | {:<40} | {}", 
                 "Patient ID", "Score", "Vertex ID", "Confidence");
        println!("{}", "-".repeat(90));

        for candidate in candidates {
            let confidence = match candidate.match_score {
                s if s >= 0.95 => "Exact Match",
                s if s >= 0.85 => "High Confidence",
                s if s >= 0.70 => "Probable Match",
                s if s >= 0.50 => "Possible Match",
                _ => "Low Confidence",
            };

            println!("{:<12} | {:<10.3} | {:<40} | {}", 
                     candidate.patient_id, 
                     candidate.match_score, 
                     candidate.patient_vertex_id,
                     confidence);
        }

        println!("\nMatch Score Interpretation:");
        println!("  ≥ 0.95 - Exact match, safe to auto-link");
        println!("  ≥ 0.85 - High confidence, recommend review");
        println!("  ≥ 0.70 - Probable match, requires verification");
        println!("  ≥ 0.50 - Possible match, manual review required");
        println!("  < 0.50 - Low confidence, likely different patient");
    }

    /// Validate identifier type is recognized by the system
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
            return Err(GraphError::InvalidRequest(
                format!("Invalid identifier type '{}'. Valid types: {}", 
                        id_type, 
                        valid_types.join(", "))
            ));
        }

        Ok(())
    }

    /// Validate merge resolution policy
    fn validate_resolution_policy(&self, policy: &str) -> Result<(), GraphError> {
        let valid_policies = vec![
            "MOST_RECENT",      // Use most recently updated values
            "MOST_COMPLETE",    // Prefer non-null values
            "SOURCE_PRIORITY",  // Always take source values
            "TARGET_PRIORITY",  // Always keep target values
            "MANUAL",           // Require human review
        ];

        if !valid_policies.contains(&policy) {
            return Err(GraphError::InvalidRequest(
                format!("Invalid resolution policy '{}'. Valid policies: {}", 
                        policy, 
                        valid_policies.join(", "))
            ));
        }

        Ok(())
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
    dob: String,
    address: String,
    phone: Option<String>,
    mpi_handlers: Arc<TokioMutex<Option<MPIHandlers>>>,
) -> Result<(), GraphError> {
    let handlers_guard = mpi_handlers.lock().await;
    
    match handlers_guard.as_ref() {
        Some(handlers) => {
            handlers.handle_match(name, dob, address, phone).await
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
            println!("\n⚠️  WARNING: Merge is a destructive operation!");
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

// =========================================================================
// Non-Interactive Mode Handlers (standalone functions)
// =========================================================================

/// Handle MPI Match command in non-interactive mode
pub async fn handle_mpi_match(
    name: String,
    dob: String,
    address: String,
    phone: Option<String>,
) -> Result<(), GraphError> {
    let handlers = MPIHandlers::new().await?;
    handlers.handle_match(name, dob, address, phone).await
}

/// Handle MPI Link command in non-interactive mode
pub async fn handle_mpi_link(
    master_id: String,
    external_id: String,
    id_type: String,
) -> Result<(), GraphError> {
    let handlers = MPIHandlers::new().await?;
    handlers.handle_link(master_id, external_id, id_type).await
}

/// Handle MPI Merge command in non-interactive mode
pub async fn handle_mpi_merge(
    source_id: String,
    target_id: String,
    resolution_policy: String,
) -> Result<(), GraphError> {
    let handlers = MPIHandlers::new().await?;
    handlers.handle_merge(source_id, target_id, resolution_policy).await
}

/// Handle MPI Audit command in non-interactive mode
pub async fn handle_mpi_audit(
    mpi_id: String,
    timeframe: Option<String>,
) -> Result<(), GraphError> {
    let handlers = MPIHandlers::new().await?;
    handlers.handle_audit(mpi_id, timeframe).await
}