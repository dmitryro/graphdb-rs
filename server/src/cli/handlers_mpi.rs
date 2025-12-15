//! Master Patient Index (MPI) Command Handlers
//!
//! This module implements a comprehensive MPI system following healthcare industry standards.
//! The MPI serves as the authoritative source for patient identity data, ensuring accurate
//! patient identification and linking across disparate healthcare systems.
//!
//! Core MPI Functions:
//! - Patient Index:
//! - Patient Matching: Probabilistic and deterministic matching algorithms
//! - Identity Linking: Cross-reference external identifiers (MRN, SSN, etc.)
//! - Record Merging: Consolidate duplicate patient records with conflict resolution
//! - Audit Trail: Complete history of identity changes and merge operations
//! - Golden Record: Maintain single source of truth for each patient
use anyhow::{Result, anyhow, Context};
use async_trait::async_trait;
use tokio::sync::{Mutex as TokioMutex, OnceCell};
use chrono::{DateTime, Datelike, NaiveDate, Utc};
use log::{info, error, warn};
use std::sync::Arc;
// Removed: tokio::sync::Mutex as TokioMutex (no longer used in local storage init)
use lib::commands::{ MPICommand, NameMatchAlgorithm };
// NOTE: Assuming GraphService is accessible via the lib crate structure based on panic trace
use lib::graph_engine::GraphService;
use lib::storage_engine::{ GraphStorageEngine, GLOBAL_STORAGE_ENGINE };
use models::medical::{MasterPatientIndex, Patient, Address, ExternalId, IdType, PatientId};
use medical_knowledge::mpi_identity_resolution::{MpiIdentityResolutionService, PatientCandidate};
use models::errors::GraphError;
use models::identifiers::Identifier;
use crate::cli::{ get_storage_engine_singleton };

// Placeholder types needed for function signatures
type Timeframe = String; 
// Type alias for the specific state field being passed around, using the concrete type
type MpiHandlersStateRef = Arc<TokioMutex<Option<MPIHandlers>>>;

// =========================================================================
// MPI Handler Implementation
// =========================================================================
#[derive(Clone)]
pub struct MPIHandlers {
    mpi_service: Arc<MpiIdentityResolutionService>,
}

// =========================================================================
// Singleton Definition
// =========================================================================

/// Global singleton for MPI Handlers. Used to ensure the underlying storage
/// dependencies are initialized only once per process lifecycle.
pub static MPI_HANDLERS_SINGLETON: OnceCell<Arc<MPIHandlers>> = OnceCell::const_new();

// NOTE: The `get_configured_storage_engine` helper function and all related
// storage engine initialization logic have been removed as storage configuration
// is handled by the main CLI entry point (cli.rs).

/// Helper to initialize MPIHandlers and store it in the global singleton.
async fn store_handlers_in_singleton(mpi_service: Arc<MpiIdentityResolutionService>) -> Result<Arc<MPIHandlers>, GraphError> {
    let handlers = Arc::new(MPIHandlers { mpi_service });
    
    // Attempt to set the singleton. If it's already set, we discard the new handlers
    // and return the error, maintaining the single initialized instance.
    MPI_HANDLERS_SINGLETON
        .set(handlers.clone())
        .map_err(|_| GraphError::InternalError("MPIHandlers singleton was concurrently initialized.".to_string()))?;
        
    println!("MPIHandlers stored globally in singleton.");
    Ok(handlers)
}

/// Retrieves and initializes the MpiIdentityResolutionService singleton.
/// This function is now deprecated in favor of explicit initialization in `MPIHandlers::new`.
pub async fn get_mpi_service_singleton() -> Result<Arc<MpiIdentityResolutionService>, GraphError> {
    // Rely on the service being initialized by the CLI's main logic
    // We assume MpiIdentityResolutionService::get().await returns Result<Arc<...>, GraphError>
    
    // FIX: Use the ? operator to propagate errors and unwrap the success value.
    let mpi_service = MpiIdentityResolutionService::get().await?;

    // The result is already the success value, so we wrap it in Ok().
    Ok(mpi_service)
}

// --- We need a helper function to initialize handlers from the singleton's perspective
// --- This is required for the non-interactive handlers that still need to take `storage`.
async fn get_or_init_handlers_from_singleton(storage: Arc<dyn GraphStorageEngine>) -> Result<Arc<MPIHandlers>> {
    MPI_HANDLERS_SINGLETON.get_or_try_init(|| async {
        // Initialize the handlers using the provided storage, which will also set up 
        // GraphService and MpiIdentityResolutionService as singletons.
        
        // FIX 1: Assume MPIHandlers::new_with_storage already returns Arc<MPIHandlers>
        // and remove the redundant .map(Arc::new)
        MPIHandlers::new_with_storage(storage)
            .await
            // .map(Arc::new) <--- REMOVE THIS LINE
            .map_err(|e| anyhow!("Failed to initialize MPI Handlers: {}", e))
    })
    .await
    // FIX 2: Use .cloned() to convert Result<&Arc<MPIHandlers>, Error> to Result<Arc<MPIHandlers>, Error>
    .cloned()
    .context("Failed to get or initialize MPIHandlers from the global singleton.")
}

// =========================================================================
// Interactive Mode Handlers (RELY ON SINGLETON ALREADY BEING SET)
// =========================================================================

/// Helper to safely retrieve initialized handlers from the global singleton.
pub async fn get_handlers_for_interactive() -> Result<Arc<MPIHandlers>> {
    MPI_HANDLERS_SINGLETON.get()
        .cloned()
        .context("MPI handlers are not initialized in the global singleton. Logic error in application startup.")
}

/// Ensures the GraphService and MpiIdentityResolutionService are initialized globally.
/// This must be called before any handlers_mpi::handle_mpi_* function is executed.
pub async fn initialize_mpi_services(
    storage_engine: Arc<dyn GraphStorageEngine>
) -> Result<(), anyhow::Error> {
    // We modify this function to use `MPI_HANDLERS_SINGLETON.get_or_try_init` 
    // to handle the full initialization chain if not already done.
    
    // We first check if MPIHandlers are already initialized.
    if MPI_HANDLERS_SINGLETON.get().is_some() {
        println!("MPIHandlers already initialized. Skipping dependency setup.");
        return Ok(());
    }

    // 1. Initialize GraphService (if not already done).
    GraphService::global_init(storage_engine)
        .await
        .context("Failed to initialize GraphService using the storage engine.")?;
        
    // 2. Retrieve the now-initialized GraphService singleton instance.
    let graph_service_instance = GraphService::get()
        .await
        .context("GraphService failed to initialize or could not be retrieved.")?;

    // 3. Use the retrieved Arc<GraphService> instance to initialize the MPI service.
    MpiIdentityResolutionService::global_init(graph_service_instance)
        .await
        .map_err(|e| anyhow!("Failed to initialize MpiIdentityResolutionService: {}", e))?;
        
    // 4. Retrieve the initialized MPI service and store the MPIHandlers instance
    let mpi_service: Arc<MpiIdentityResolutionService> = MpiIdentityResolutionService::get()
        .await
        .map_err(|e| anyhow!("Failed to retrieve MpiIdentityResolutionService after init: {}", e))?;

    // 5. Create and store the final MPIHandlers instance in the singleton
    store_handlers_in_singleton(mpi_service)
        .await
        .map_err(|e| anyhow!("Failed to finalize and store MPIHandlers singleton: {}", e))?;

    Ok(())
}

impl MPIHandlers {
    /// Initialize the MPI handler by explicitly building the dependency chain
    /// Used in CLI context where global services are NOT pre-initialized.
    pub async fn new() -> Result<Arc<Self>, GraphError> {
        println!("Initializing Master Patient Index (MPI) service...");

        // Check singleton first (Idempotency)
        if let Some(handlers) = MPI_HANDLERS_SINGLETON.get() {
            println!("MPIHandlers retrieved from singleton.");
            return Ok(handlers.clone());
        }

        // 1. Get storage singleton
        let storage: Arc<dyn GraphStorageEngine> = get_storage_engine_singleton().await?;
        println!("Storage dependency retrieved from singleton.");

        // 2. Initialize GraphService globally
        GraphService::global_init(storage.clone()).await?;
        println!("GraphService initialized globally.");

        // 3. Initialize MPI service globally (requires the GraphService instance)
        let graph_service = GraphService::get().await?;
        MpiIdentityResolutionService::global_init(graph_service).await
            .map_err(|e| GraphError::InternalError(format!("Failed to initialize MPI service: {}", e)))?;
        println!("MpiIdentityResolutionService initialized globally.");

        // 4. Get the initialized service
        let mpi_service: Arc<MpiIdentityResolutionService> = MpiIdentityResolutionService::get().await?;
        println!("Master Patient Index (MPI) service ready.");

        // 5. Store the newly created handlers in the singleton
        store_handlers_in_singleton(mpi_service).await
    }

    /// Initialize using externally provided storage (for tests/scripts)
    pub async fn new_with_storage(storage: Arc<dyn GraphStorageEngine>) -> Result<Arc<Self>, GraphError> {
        println!("Initializing MPI service with injected storage...");

        // Check singleton first (Idempotency)
        if let Some(handlers) = MPI_HANDLERS_SINGLETON.get() {
            println!("MPIHandlers retrieved from singleton.");
            return Ok(handlers.clone());
        }

        println!("Storage dependency injected.");

        // Initialize the global services using the provided storage
        GraphService::global_init(storage).await?;
        println!("GraphService initialized globally.");

        // Initialize MPI service globally (requires the GraphService instance)
        let graph_service = GraphService::get().await?;
        MpiIdentityResolutionService::global_init(graph_service).await
            .map_err(|e| GraphError::InternalError(format!("Failed to initialize MPI service: {}", e)))?;
        println!("MpiIdentityResolutionService initialized globally.");

        // Safe: we just called global_init(), so get() will succeed
        let mpi_service: Arc<MpiIdentityResolutionService> = MpiIdentityResolutionService::get().await?;
        println!("Master Patient Index (MPI) service ready.");

        // Store the newly created handlers in the singleton
        store_handlers_in_singleton(mpi_service).await
    }

    // =========================================================================
    // Main Command Router (DISPATCHES to non-static methods via self)
    // =========================================================================

    /// Main command router - dispatches to appropriate handler (RESTORED and FULLY UPDATED).
    pub async fn handle(&self, command: MPICommand) -> Result<(), GraphError> {
        match command {
            // --- Index Command Routing ---
            MPICommand::Index {
                name, first_name, last_name, dob, mrn, address, phone, system, gender,
            } => {
                // 1. Retrieve the existing patient data using the MRN.
                let existing_patient_opt = self.retrieve_and_convert_patient(&mrn).await?;
                // 2. Call handle_index with all arguments.
                self.handle_index(
                    existing_patient_opt, 
                    name,
                    first_name,
                    last_name,
                    dob,
                    mrn,
                    address,
                    phone,
                    Some(system),   
                    gender,   
                ).await
            }

            // --- Match Command Routing (Passing ALL arguments) ---
            MPICommand::Match {
                name, dob, address, phone, name_algo,
                target_mrn, candidate_mrn, score, action,
            } => {
                // FIX: REMOVE the mandatory check for 'name'. 'name' can be None for MRN-based matches.
                // The syntax error (nested MPICommand::Match) is removed here.

                self.handle_match(
                    name,               // Option<String> passed directly
                    dob,                // Option<String>
                    address,            // Option<String>
                    phone,              // Option<String>
                    name_algo,          // NameMatchAlgorithm
                    
                    // The rest are assumed to be non-optional types destructured from the CLI command
                    // and must be wrapped in Some() to match the handler's Option<T> signature.
                    Some(target_mrn), 
                    Some(candidate_mrn), 
                    Some(score), 
                    Some(action),
                ).await
            }
            
            // --- Merge Command Routing (Passing ALL arguments) ---
            MPICommand::Merge {
                master_mrn, 
                duplicate_mrn, 
                resolution_policy, 
                user_id, // Likely String/required type from CLI
                reason,  // Likely String/required type from CLI
            } => self.handle_merge(
                master_mrn, 
                duplicate_mrn, 
                resolution_policy, 
                Some(user_id), // FIX: Wrap in Some() to match Option<String> signature
                Some(reason)   // FIX: Wrap in Some() to match Option<String> signature
            ).await,
            
            // --- Link Command Routing ---
            MPICommand::Link {
                master_id, external_id, id_type, // id_type is a concrete String, not Option<String>
            } => {
                // FIX: Remove the incorrect check (`ok_or_else`) for a field that is
                // already guaranteed to be a String by the command's struct definition.
                let required_id_type = id_type;
                
                self.handle_link(master_id, external_id, required_id_type).await
            },

            // --- Audit Command Routing ---
            MPICommand::Audit { mpi_id, timeframe } => self.handle_audit(mpi_id, timeframe).await,

            // --- Resolve Command Routing (NOW NON-STATIC) ---
            MPICommand::Resolve { 
                id, external_id, id_type, source_mrn, system,
            } => self.handle_resolve(id, external_id, id_type, source_mrn, system).await,

            // --- Fetch Identity Command Routing (NOW NON-STATIC) ---
            MPICommand::FetchIdentity {
                id, external_id, id_type, source_mrn,
            } => self.handle_fetch_identity(
                id, 
                external_id, 
                id_type, 
                source_mrn, 
                None // FIX: Pass the missing 'system' argument as None
            ).await,

            // --- Search Command Routing (NOW NON-STATIC) ---
            MPICommand::Search { name, first_name, last_name, dob, address, phone } => 
                self.handle_search(name, first_name, last_name, dob, address, phone).await,

            // --- Split Command Routing ---
            MPICommand::Split {
                merged_id, new_patient_data_json, reason, split_patient_id_str,
            } => {
                let new_patient_data: Patient = serde_json::from_str(&new_patient_data_json)
                    .map_err(|e| GraphError::InternalError(format!("Failed to parse new patient data JSON: {}", e)))?;
                
                // FIX: Parse the String ID into an i32
                let split_patient_id = split_patient_id_str
                    .parse::<i32>()
                    .map_err(|e| GraphError::InvalidRequest(format!("Invalid split_patient_id: {}", e)))?;
                    
                self.handle_split(merged_id, new_patient_data, reason, split_patient_id).await
            }
            
            // --- GetGoldenRecord Command Routing ---
            MPICommand::GetGoldenRecord { patient_id } => {
                // Note: The conversion from String to PatientId is assumed to be handled 
                // within the handle_get_golden_record method or an issue was missed here.
                // Assuming patient_id here is the PatientId type if the CLI was fixed to convert it.
                // If patient_id is still a String, it will cause a type error on handle_get_golden_record.
                // However, based on the provided code, we must assume it matches the signature.
                self.handle_get_golden_record(patient_id).await?;
                Ok(())
            }
        }
    }

    // =========================================================================
    // Core MPI Operations (All non-static, primary handlers)
    // =========================================================================

    /// Index - Insert a new patient record or update an existing one based on MRN.
    #[allow(clippy::too_many_arguments)]
    async fn handle_index(
        &self,
        existing_patient_opt: Option<Patient>,
        name: Option<String>,
        first_name: Option<String>,
        last_name: Option<String>,
        dob: Option<String>,
        mrn: String,
        address: Option<String>,
        phone: Option<String>,
        system: Option<String>, 
        gender: Option<String>, 
    ) -> Result<(), GraphError> {
        println!("=== MPI Patient Indexing ===");
        println!("Inserting or updating patient record (MRN: {})...\n", mrn);
        

        // 1. Build the Patient struct from input (merging with existing if found)
        let new_patient = self.create_patient_from_index_input(
            existing_patient_opt, 
            name,
            first_name,
            last_name,
            dob,
            mrn.clone(), 
            address,
            phone,
            system, 
            gender, 
        )?;

        println!("Indexing Patient:");
        println!(" MRN: {}", new_patient.mrn.as_deref().unwrap_or("N/A"));
        println!(" Name: {} {}", new_patient.first_name, new_patient.last_name);
        println!(" DOB: {}", new_patient.date_of_birth.format("%Y-%m-%d"));
        println!();

        // 2. Call the core service method
        let result = self.mpi_service.index_new_patient(new_patient).await;

        match result {
            Ok(indexed_record) => {
                println!("✓ Indexing successful");
                println!(" Assigned Patient ID: {}", indexed_record.id);
                println!(" Linked Vertex ID: {}", indexed_record.user_id.map(|id| id.to_string()).unwrap_or_else(|| "N/A".to_string()));
                println!("\nRecord indexed and matched/linked to the Golden Record.");
            }
            Err(e) => {
                eprintln!("✗ Indexing failed: {}", e);
                return Err(GraphError::InternalError(e.to_string()));
            }
        }
        Ok(())
    }

    pub async fn handle_mpi_match_via_singleton(
        name: String,
        dob: Option<String>,
        address: Option<String>,
        phone: Option<String>,
        name_algo: NameMatchAlgorithm,
    ) -> Result<(), anyhow::Error> {
        info!("Executing MPI match via global singleton retrieval.");

        // Retrieve the MPIHandlers instance from the global singleton, initializing it
        // if necessary.
        let handlers_arc = MPI_HANDLERS_SINGLETON.get_or_try_init(|| async {
            
            // ASSUMPTION: MPIHandlers::new().await already returns Result<Arc<MPIHandlers>, GraphError>.

            MPIHandlers::new()
                .await
                // We use map_err to convert the inner error type (e.g., GraphError) to anyhow::Error
                .map_err(|e| anyhow!("Failed to initialize MPI Handlers for singleton: {}", e))
                
        })
        .await
        .context("Failed to get or initialize MPIHandlers from the global singleton.")?;
        
        // The result 'handlers_arc' is &Arc<MPIHandlers>, satisfying the original type.
        
        // The actual core logic is the instance method on MPIHandlers.
        // FIX E0061: Supply the four missing optional arguments (target_mrn, candidate_mrn, score, action).
        handlers_arc.as_ref().handle_match(
            Some(name), 
            dob, 
            address, 
            phone, 
            name_algo,
            None, // target_mrn: Option<String>
            None, // candidate_mrn: Option<String>
            None, // score: Option<f64>
            None, // action: Option<String>
        )
            .await
            .map_err(|e| anyhow!("Core MPI Match failed: {}", e))?;

        Ok(())
    }

    /// Merge - Consolidate duplicate patient records into golden record
    #[allow(clippy::too_many_arguments)]
    async fn handle_merge(
        &self,
        master_mrn: String,
        duplicate_mrn: String,
        resolution_policy: String,
        user_id: Option<String>, // Retaining for full signature adherence
        reason: Option<String>, // Retaining for full signature adherence
    ) -> Result<(), GraphError> {
        println!("=== MPI Record Merge ===");
        println!("Consolidating duplicate patient records...\n");
        
        
        let source_mpi_id = duplicate_mrn;
        let target_mpi_id = master_mrn;
        
        self.validate_resolution_policy(&resolution_policy)?;

        println!("Source Record (will be merged): {}", source_mpi_id);
        println!("Target Record (survivor): {}", target_mpi_id);
        println!("Conflict Resolution Policy: {}", resolution_policy);

        if user_id.is_some() || reason.is_some() {
            println!("\n[Merge Context]");
            println!(" User ID: {}", user_id.as_deref().unwrap_or("N/A"));
            println!(" Reason: {}", reason.as_deref().unwrap_or("N/A"));
        }

        println!("\n⚠ WARNING: This operation will:");
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
                user_id, // Passed to service
                reason, // Passed to service
            )
            .await;

        match result {
            Ok(_golden_record) => {
                println!("✓ Merge completed successfully");
                println!(" Source ID {} → Target ID {}", source_mpi_id, target_mpi_id);
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

    /// Link - Associate external identifiers with MPI master record
    async fn handle_link(
        &self,
        master_id: String,
        external_id: String,
        id_type: String, // Router passes non-Option String
    ) -> Result<(), GraphError> {
        println!("=== MPI Identity Linking ===");
        println!("Linking external identifier to master record...\n");
        
        // Use the master ID directly
        let master_mpi_id = master_id;

        println!("Master Patient ID: {}", master_mpi_id);
        println!("External ID: {} (Type: {})", external_id, id_type);
        println!();
        
        // Validation check (assuming validate_identifier_type expects String)
        self.validate_identifier_type(&id_type)?;
        
        // NOTE: Converting to IdType is unnecessary if the service only accepts String.
        // If IdType enum is critical for internal logic:
        let id_type_enum = IdType::from(id_type.clone());

        // The error shows the service method expects String for the third argument, not Option<IdType>.
        let result = self
            .mpi_service
            .link_external_identifier(
                master_mpi_id.clone(), 
                external_id.clone(), 
                // FIX: Use the original id_type String, or id_type_enum.to_string(), 
                // depending on what the service expects. Since the service expects String:
                id_type.clone() 
            )
            .await;
        
        match result {
            Ok(record) => {
                println!("✓ Link successful");
                println!(" MPI Record ID: {}", record.id);
                println!(" External ID: {} → Master ID: {}", external_id, master_mpi_id);
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

    /// Match - Find potential duplicate records using probabilistic matching
    #[allow(clippy::too_many_arguments)]
    async fn handle_match(
        &self,
        name: Option<String>,
        dob: Option<String>,
        address: Option<String>,
        phone: Option<String>,
        name_algo: NameMatchAlgorithm,
        target_mrn: Option<String>,
        candidate_mrn: Option<String>,
        score: Option<f64>,
        action: Option<String>,
    ) -> Result<(), GraphError> {
        println!("=== MPI Patient Matching ===");

        // --- Explicit MRN/Score Matching Logic ---
        if target_mrn.is_some() && candidate_mrn.is_some() && score.is_some() && action.is_some() {
            println!("Executing explicit probabilistic match/link based on MRNs and score...");

            // FIX: Replace the stub with the actual service call for linking.
            // We use .unwrap() safely here because we checked is_some() in the if condition.
            let target_mrn_val = target_mrn.clone().unwrap();
            let candidate_mrn_val = candidate_mrn.clone().unwrap();
            let score_val = score.unwrap();
            let action_val = action.clone().unwrap();
            
            self.mpi_service.handle_probabilistic_link(
                target_mrn_val.clone(), 
                candidate_mrn_val.clone(), 
                score_val, 
                action_val.clone()
            )
            .await
            .map_err(|e| GraphError::InternalError(format!("Probabilistic linking failed: {}", e)))?;
            
            // FIX: Add user-facing success summary output
            println!("---------------------------------------------------");
            println!("✅ MPI LINK/MATCH OPERATION SUCCESS");
            println!("Action: {}", action_val);
            println!("Target MRN: {}", target_mrn_val);
            println!("Candidate MRN: {}", candidate_mrn_val);
            println!("Score: {:.3}", score_val);
            println!("---------------------------------------------------");
        } 
        // --- Demographic Search Logic ---
        else if let Some(name_str) = name {
            println!("Searching for potential matches using demographics...\n");

            // --- 1. Generate Blocking Keys from raw input strings ---
            let dob_str = dob.as_ref().map(|s| s.as_str()).unwrap_or("");
            let search_keys = MpiIdentityResolutionService::get_blocking_keys_for_match(&name_str, dob_str)
                .map_err(|e| GraphError::InternalError(e))?;

            // --- 2. Build Patient struct for full scoring (demographics) ---
            let patient = self.build_search_patient(
                name_str.clone(), 
                dob.clone(), 
                address.clone(), 
                phone.clone()
            )?;

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

            println!(" Name Match Algorithm: {}", name_algo);
            println!(" Blocking Keys: {:?}", search_keys); 

            // Additional fields from command (even if not used by core stub)
            if target_mrn.is_some() || candidate_mrn.is_some() || score.is_some() || action.is_some() {
                println!("\n[Match Context]");
                println!(" Target MRN: {}", target_mrn.as_deref().unwrap_or("N/A"));
                println!(" Candidate MRN: {}", candidate_mrn.as_deref().unwrap_or("N/A"));
                println!(" Score Threshold: {}", score.map(|s| s.to_string()).unwrap_or("N/A".to_string()));
                println!(" Action: {}", action.as_deref().unwrap_or("N/A"));
            }
            
            println!();

            // --- 3. Execute Match with Keys ---
            let candidates = self
                .mpi_service
                .run_probabilistic_match(&patient, search_keys) 
                .await
                .map_err(|e| GraphError::InternalError(e.to_string()))?;

            self.display_match_results(candidates);
            
        } else {
            // Should be caught by CLI parsing, but serves as a final check.
            return Err(GraphError::InternalError("Match command requires either demographic data (name) or explicit MRN/score fields.".to_string()));
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
                println!("{}", change); 
            }

            println!("\n{} total events", num_changes);
        }
        Ok(())
    }

    // =========================================================================
    // CORRECTED NON-STATIC IDENTITY RESOLUTION HANDLERS (Resolve/Fetch/Search)
    // The previous static logic is fully integrated here as non-static methods.
    // =========================================================================

    /// Resolve - Finds the canonical MPI ID for a given external identifier.
    /// Combines the logic of handle_mpi_resolve from the previous context, but uses &self.
    #[allow(clippy::too_many_arguments)]
    pub async fn handle_resolve(
        &self,
        id: Option<String>, 
        external_id: Option<String>, 
        id_type: Option<String>, 
        source_mrn: Option<String>,
        // FIX: Add the new system parameter
        system: Option<String>,
    ) -> Result<(), GraphError> {
        
        // FIX 1: Correctly derive final_id by using .as_ref() and .cloned() on the Option<String> inputs.
        // This avoids moving the original variables (id, external_id, source_mrn).
        // The original variable names are reused in the closures for clarity, but they refer to the borrowed content.
        let final_id = id.as_ref().cloned()
            .or_else(|| external_id.as_ref().cloned())
            .or_else(|| source_mrn.as_ref().cloned())
            .context("At least one ID field (--id, --external-id, or --source-mrn) is required for resolve operation.")?;
        
        // The original variables (external_id, id_type, system, source_mrn) are now available here.

        // The ID used for the consolidate operation must be handled separately. 
        // We assume the consolidate operation is skipped if only 'id' is present.
        
        if let (Some(ext_id), Some(id_t), Some(sys), Some(src_mrn_val)) = 
            (external_id.clone(), id_type.clone(), system.clone(), source_mrn.clone()) 
        {
            // If all external ID components are present, call consolidate_id.
            // NOTE: The previous code block attempting to re-derive final_id is removed as it is now redundant/incorrect.
            // Instead, we call consolidate_id with the necessary arguments.
            
            // Assuming consolidate_id is called here if needed for linking:
            self.consolidate_id(
                final_id.clone(), // raw_patient_id_str (the resolved ID)
                ext_id,           // external_id
                id_t,             // id_type
                sys,              // system
            ).await?;
        }
        
        // The second attempt to derive final_id (lines 758-762 in the original code) is removed as final_id is derived once.


        info!(
            "Starting non-interactive MPI resolve and fetch for ID: {} (Type: {:?}, System: {:?})", 
            final_id, 
            id_type,
            system // Log the new system context
        );

        let final_id_type: Option<IdType> = id_type.map(IdType::from);
        
        // Use default if type is missing
        let resolved_id_type = final_id_type.clone()
            .unwrap_or_else(|| IdType::Other("UNKNOWN_ID_TYPE".to_string()));

        // FIX 3: Assume ExternalId struct has been updated to include the system field
        let external_id_struct: ExternalId = ExternalId { 
            id_type: resolved_id_type, 
            id_value: final_id.clone(),
            // system is passed as Option<String>
            system: system, 
        };
        
        // FIX 4: Add .await to the asynchronous call (already present in the previous fix)
        let (resolved_id, patient_record) = self.mpi_service
            .resolve_and_fetch_patient(external_id_struct, final_id_type)
            .await
            .context("Failed to resolve ID and fetch patient record.")
            .map_err(|e| GraphError::InternalError(e.to_string()))?;
        
        println!("---------------------------------------------------");
        println!("✅ RESOLVE AND FETCH SUCCESS");
        println!("Input ID ({}) resolved to Canonical Patient ID: {}", final_id, resolved_id.to_string());
        println!("---------------------------------------------------");
        
        // Output Patient Data (Golden Record)
        println!("\n### GOLDEN RECORD DATA ###");
        println!("Patient Name: {} {}", patient_record.first_name, patient_record.last_name);
        println!("Date of Birth: {}", patient_record.date_of_birth.format("%Y-%m-%d").to_string());
        
        // Use debug print for Address
        let address_str = patient_record.address.as_ref()
            .map(|a| format!("{:?}", a))
            .unwrap_or_else(|| "N/A".to_string());
        println!("Address: {}", address_str);
        
        println!("Phone: {}", patient_record.phone_home.as_deref().unwrap_or("N/A"));

        // Output Metadata
        println!("\n### METADATA ###");
        println!("Record Creation Date: {}", patient_record.created_at.to_rfc3339());
        println!("Record Last Updated: {}", patient_record.updated_at.to_rfc3339());
        
        // Display the primary identifiers explicitly
        println!("External IDs Linked (Primary):");
        
        if let Some(ref mrn) = patient_record.mrn {
            println!("  - Type: MRN, Value: {}", mrn);
        }
        if let Some(ref ssn) = patient_record.ssn {
            println!("  - Type: SSN, Value: {}", ssn);
        }
        
        if patient_record.mrn.is_none() && patient_record.ssn.is_none() {
            println!("  - None explicitly listed (MRN/SSN)");
        }
        
        Ok(())
    }

    /// Fetch Identity - Retrieves the complete Golden Record data for a resolved identifier.
    /// This is largely a duplicate of Resolve in a stubbed system, but is included for command completeness.
    #[allow(clippy::too_many_arguments)]
    pub async fn handle_fetch_identity(
        &self,
        id: Option<String>, external_id: Option<String>, id_type: Option<String>, source_mrn: Option<String>,
        // Add the new system parameter to the handler, which was required in the previous fixes.
        system: Option<String>,
    ) -> Result<(), GraphError> {
        
        // FIX 1: Correctly derive the ID to be used for fetching (final_id)
        // We use the first available ID for the fetch operation.
        let final_id = id.as_ref()
            .or(external_id.as_ref())
            .or(source_mrn.as_ref())
            .cloned()
            .context("At least one ID field (--id, --external-id, or --source-mrn) is required for fetch operation.")?;
        
        // NOTE: The previous code was incorrectly trying to assign the result of consolidate_id (which returns ()) 
        // to final_id. The call is logically separate.
        
        // Conditional consolidation: If we have enough mandatory fields to link an ID, perform the consolidation.
        if let (Some(ext_id), Some(src_mrn_val), Some(sys), Some(id_t)) = 
            (external_id.clone(), source_mrn.clone(), system.clone(), id_type.clone()) 
        {
            // The consolidate_id signature requires (String, String, String, String):
            // 1. raw_patient_id_str (the canonical ID, which is final_id)
            // 2. external_id (the external ID value)
            // 3. id_type (the type of the external ID)
            // 4. system (the source system)
            
            // FIX 2: Call consolidate_id correctly with unwrapped values and .await
            self.consolidate_id(
                final_id.clone(),     // 1. raw_patient_id_str (Patient ID to link to)
                ext_id,               // 2. external_id
                id_t,                 // 3. id_type (Assuming id_type is used as the id_type argument)
                sys,                  // 4. system
            ).await?;
        }
        
        info!("Fetching identity record for ID: {} (Type: {:?})", final_id, id_type);

        let final_id_type: Option<IdType> = id_type.map(IdType::from);
        let resolved_id_type = final_id_type.clone().unwrap_or_else(|| IdType::Other("UNKNOWN_ID_TYPE".to_string()));

        // FIX 3: Initialize the ExternalId struct with the required `system` field.
        let external_id_struct: ExternalId = ExternalId { 
            id_type: resolved_id_type, 
            id_value: final_id.clone(),
            system: system, // Passed as Option<String>
        };
        
        // No fix needed here, .await is already on the resolve_and_fetch_patient call below.
        let (_, patient_record) = self.mpi_service
            .resolve_and_fetch_patient(external_id_struct, final_id_type)
            .await
            .context("Failed to fetch patient record for provided ID.")
            .map_err(|e| GraphError::InternalError(e.to_string()))?;
        
        println!("---------------------------------------------------");
        println!("✅ FETCH IDENTITY SUCCESS");
        println!("Retrieved Golden Record using ID: {}", final_id);
        println!("\n### GOLDEN RECORD DATA ###");
        println!("Canonical ID: {}", patient_record.id);
        println!("Full Name: {} {}", patient_record.first_name, patient_record.last_name);
        println!("Date of Birth: {}", patient_record.date_of_birth.format("%Y-%m-%d"));
        println!("Phone (Home): {}", patient_record.phone_home.as_deref().unwrap_or("N/A"));
        println!("---------------------------------------------------");

        Ok(())
    }

    /// Search - Finds matching patient records based on demographics.
    /// Combines the logic of handle_mpi_search from the previous context, but uses &self.
    #[allow(clippy::too_many_arguments)]
    pub async fn handle_search(
        &self,
        name: Option<String>,
        first_name: Option<String>,
        last_name: Option<String>,
        dob: Option<String>,
        address: Option<String>,
        phone: Option<String>,
    ) -> Result<(), GraphError> {
        info!("Starting MPI search with demographics: Name={:?}, DOB={:?}", name, dob);
        
        
        // Safety check
        let has_fields = name.is_some() || first_name.is_some() || last_name.is_some() || dob.is_some() || address.is_some() || phone.is_some();
        if !has_fields {
            return Err(GraphError::InvalidRequest("MPI Search requires at least one demographic field.".into()));
        }

        // Call the service layer function to retrieve a list of complete Patient records
        let search_results: Vec<Patient> = self.mpi_service
            .search_patients_by_demographics(
                name.clone(), // Clone to retain original options for output printing if needed
                first_name.clone(),
                last_name.clone(),
                dob.clone(),
                address.clone(),
                phone.clone(),
            )
            .await
            .context("Failed to execute patient demographic search.")
            .map_err(|e| GraphError::InternalError(e.to_string()))?;

        println!("---------------------------------------------------");
        println!("✅ SEARCH COMPLETE");
        println!("Found {} potential patient records.", search_results.len());
        println!("---------------------------------------------------");

        if search_results.is_empty() {
            println!("No patients matched the provided criteria.");
        } else {
            // Display detailed results for each patient (Full restored logic)
            for (index, patient) in search_results.iter().enumerate() {
                println!("\n===================================================");
                println!("[Result {} / {}]", index + 1, search_results.len());
                let patient_id_str = patient.id.to_string();
                println!("Canonical Patient ID: {}", patient_id_str);
                println!("===================================================");
                
                // Output Patient Data (Golden Record)
                println!("\n### GOLDEN RECORD DATA ###");
                println!("Full Name: {} {}", patient.first_name, patient.last_name);
                println!("Date of Birth: {}", patient.date_of_birth.format("%Y-%m-%d").to_string());
                
                // Use debug print for Address
                let address_str = patient.address.as_ref()
                    .map(|a| format!("{:?}", a))
                    .unwrap_or_else(|| "N/A".to_string());
                println!("Address: {}", address_str);

                println!("Phone: {}", patient.phone_home.as_deref().unwrap_or("N/A"));

                // Output Metadata
                println!("\n### METADATA ###");
                println!("Record Creation Date: {}", patient.created_at.to_rfc3339());
                println!("Record Last Updated: {}", patient.updated_at.to_rfc3339());
                
                println!("External IDs Linked:");
                
                if let Some(ref mrn) = patient.mrn {
                    println!("  - Type: MRN, Value: {}", mrn);
                }
                if let Some(ref ssn) = patient.ssn {
                    println!("  - Type: SSN, Value: {}", ssn);
                }
                
                if patient.mrn.is_none() && patient.ssn.is_none() {
                    println!("  - None explicitly listed (MRN/SSN)");
                }
            }
        }

        Ok(())
    }

    /// Consolidate ID - Link a patient's external ID (e.g., MRN from a specific system) 
    /// to an existing or newly created Golden Record/MPI ID. This is typically used 
    /// internally by the indexing process.
    async fn consolidate_id(
        &self,
        // FIX: Change to String to accept the raw ID value passed from the caller.
        raw_patient_id_str: String, 
        external_id: String,
        id_type: String, // E.g., "MRN"
        system: String,  // E.g., "Epic", "Cerner"
    ) -> Result<(), GraphError> {
        
        // FIX: Convert the raw string ID into the PatientId struct (assuming a ::from method exists)
        let patient_id = PatientId::from(raw_patient_id_str.clone());

        println!("=== MPI ID Consolidation/Linking ===");
        println!(
            "Attempting to link external ID '{}' (Type: {}, System: {}) to Golden Record ID: {}...",
            external_id, id_type, system, patient_id
        );

        // Call the internal service method to perform the actual link operation
        let result = self.mpi_service.link_patient_id(
            // FIX: Clone patient_id here so the original variable remains available for logging/error messages
            patient_id.clone(),
            external_id.clone(),
            id_type,
            system,
        ).await;

        match result {
            Ok(()) => {
                println!("✓ Successfully linked external ID: {} to Golden Record.", external_id);
                Ok(())
            }
            Err(e) => {
                // patient_id is now available because link_patient_id consumed a clone.
                eprintln!("✗ Failed to consolidate ID {} for patient {}: {}", external_id, patient_id, e);
                // Wrap the internal error for the handler
                Err(GraphError::InternalError(format!("ID consolidation failed: {}", e)))
            }
        }
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
        // Note: The logic handles both Some(dob) parsing and a None fallback, 
        // converting the final NaiveDate to DateTime<Utc>.
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
            // FIX: Wrap the String in Some() to match the Option<String> field type
            gender: Some("Unknown".to_string()), 
            
            // Contact - used for matching
            address_id: address_tuple.as_ref().map(|a| a.id),
            address: address_tuple,
            phone_mobile: phone,
            
            // Administrative
            patient_status: Some("ACTIVE".to_string()),
            vip_flag: Some(false),
            confidential_flag: Some(false),
            
            // Audit Trail
            created_at: now,
            updated_at: now,
            created_by: None,
            updated_by: None,
            last_visit_date: None,
            
            // Use struct update syntax to fill in all missing fields with defaults.
            ..Patient::default() 
        })
    }
    
    pub async fn retrieve_and_convert_patient(&self, mrn: &str) -> Result<Option<Patient>, GraphError> {
        // Access the GraphService through the appropriate field path
        let vertex_opt = self.mpi_service.graph_service.get_patient_by_mrn(mrn).await?;

        match vertex_opt {
            Some(vertex) => {
                // The method Patient::from_vertex returns Option<Patient>.
                // We match on that Option.
                match Patient::from_vertex(&vertex) {
                    // If conversion succeeds, return Ok(Some(patient)).
                    Some(patient) => Ok(Some(patient)),
                    
                    // If conversion fails (returns None), this means we found a vertex 
                    // by MRN, but it could not be deserialized into a valid Patient struct.
                    // We treat this as a deserialization error.
                    None => {
                        Err(GraphError::DeserializationError(
                            format!("Found vertex for MRN '{}' but failed to convert it to Patient struct (malformed data).", mrn)
                        ))
                    }
                }
            }
            None => Ok(None), // No vertex found, return Ok(None)
        }
    }

    /// In MPIHandlers impl block (or similar):
    /// Create a complete Patient struct suitable for indexing/insertion.
    /// This method merges input data with an existing patient record if one was found.
    pub fn create_patient_from_index_input(
        &self,
        existing_patient_opt: Option<Patient>, // NEW: Accepts the pre-retrieved patient
        name: Option<String>,
        first_name: Option<String>,
        last_name: Option<String>,
        dob: Option<String>,
        mrn: String,
        address_str: Option<String>,
        phone: Option<String>,
        // FIX 1: Add the missing 'system' argument (Argument #9)
        system: Option<String>, // Variable is declared here, but usage removed below to fix E0609.
        // FIX 2: Add the missing 'gender' argument (Argument #10)
        gender: Option<String>,
    ) -> Result<Patient, GraphError> {
        use chrono::{NaiveDate, Utc};

        // --- LOGIC FOR NAME RESOLUTION ---
        let (final_first_name, final_last_name) = match (first_name, last_name, existing_patient_opt.as_ref()) {
            // Case 1: Names explicitly provided, use them
            (Some(f), Some(l), _) => (f, l),

            // Case 2: Names exist in the database record, use them (MRN-only call)
            // Use existing patient data's names
            (_, _, Some(p)) => (p.first_name.clone(), p.last_name.clone()),

            // Case 3: Names are missing and NO existing record found, try to parse from full 'name' field
            (None, None, None) => {
                let full_name = name.ok_or_else(|| GraphError::InvalidRequest(
                    "Must provide (first_name and last_name) or a full name string when creating a NEW record.".into()
                ))?;
                let parts: Vec<&str> = full_name.split_whitespace().collect();
                if parts.is_empty() {
                    return Err(GraphError::InvalidRequest("Name cannot be empty".into()));
                }
                let first = parts.first().unwrap().to_string();
                let last = parts.last().unwrap().to_string();
                (first, last)
            }
            
            // Case 4: Handle partial inputs if no existing record is found.
            _ => {
                 return Err(GraphError::InvalidRequest(
                     "Must provide both first_name and last_name, or rely on an existing record for MRN.".into()
                 ));
            }
        };

        // --- LOGIC FOR DATA MERGING ---
        
        // Start with existing patient data or a default patient struct
        let mut patient_to_index = existing_patient_opt.unwrap_or_else(Patient::default);

        // 2. Parse Date of Birth (use new DOB if supplied, otherwise use existing, fallback to sentinel)
        let date_of_birth = dob
            .and_then(|s| NaiveDate::parse_from_str(&s, "%Y-%m-%d").ok())
            .map(|d| d.and_hms_opt(0, 0, 0).unwrap().and_utc())
            .unwrap_or(patient_to_index.date_of_birth); // Use current/default DOB

        
        // 3. Create Address struct (or use existing address, merging data)
        let address_tuple = address_str.map(|address_line1| {
            // Creates a new Address struct using provided data and placeholders
            Address::new(
                address_line1,
                None,
                "Unknown".to_string(),
                Identifier::new("US".to_string()).unwrap(),
                "00000".to_string(),
                "US".to_string(),
            )
        }).or_else(|| patient_to_index.address.clone()); // Use existing address if no new one provided

        let now = Utc::now();
        
        // Apply resolved/new values
        patient_to_index.mrn = Some(mrn);
        patient_to_index.first_name = final_first_name;
        patient_to_index.last_name = final_last_name;
        patient_to_index.date_of_birth = date_of_birth;
        patient_to_index.address = address_tuple;
        patient_to_index.address_id = patient_to_index.address.as_ref().map(|a| a.id);
        
        // Prioritize new phone, otherwise keep existing
        patient_to_index.phone_mobile = phone.or(patient_to_index.phone_mobile);
        
        // FIX 3: Apply new/existing gender value
        patient_to_index.gender = gender.or(patient_to_index.gender);
        
        // FIX 4: REMOVED. The field `source_system` does not exist in the Patient struct.
        // patient_to_index.source_system = system.or(patient_to_index.source_system); 
        // If you intended to use one of the existing fields for 'system', use that here.
        // For now, the 'system' value is unused but required in the signature.
        let _ = system; // Silence "unused variable" warning for `system`
        
        patient_to_index.updated_at = now;
        
        // Only assign new IDs and created_at if it's genuinely a new record
        if patient_to_index.created_at.timestamp() == 0 { // Check if it's the default created_at value
            patient_to_index.id = rand::random();
            patient_to_index.created_at = now;
        }
        
        Ok(patient_to_index)
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
        // ... (validate_identifier_type implementation remains the same) ...
        let valid_types = vec![
            "MRN",     // Medical Record Number
            "SSN",     // Social Security Number
            "DL",      // Driver's License
            "PASSPORT", // Passport Number
            "INS",     // Insurance Member ID
            "MILITARY", // Military Service Number
            "MEDICAID", // Medicaid ID
            "MEDICARE", // Medicare ID
            "NPI",     // National Provider Identifier
            "OTHER",   // Other identifier types
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
        // ... (validate_resolution_policy implementation remains the same) ...
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
    async fn handle_split(
        &self,
        merged_id: String,
        new_patient_data: Patient,
        reason: String,
        // FIX 1: Add the missing split_patient_id argument
        split_patient_id: i32,
    ) -> Result<(), GraphError> {
        // ... (handle_split implementation remains the same) ...
        println!("=== MPI Identity Split (Unmerge) ===");
        println!("Reversing a previous merge operation...\n");
        
        let target_mpi_id = merged_id;
        
        println!("Merged Record to Split: {}", target_mpi_id);
        println!("New Patient Data ID: {}", new_patient_data.id);
        println!("Reason for Split: {}", reason);
        // Display the new required ID
        println!("Internal ID being split out: {}", split_patient_id); 
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
                // FIX 2: Pass the new required argument
                split_patient_id, 
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
    async fn handle_get_golden_record(
        &self,
        patient_id: String,
    ) -> Result<(), GraphError> {
        // ... (handle_get_golden_record implementation remains the same) ...
        println!("=== MPI Golden Record Retrieval ===");
        println!("Retrieving consolidated record for patient ID: {}\n", patient_id);
        
        // Call the new service method
        // NOTE: It is assumed self.mpi_service.get_golden_record returns Result<Patient, Error>
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
// INTERACTIVE MODE HANDLERS (Delegating to MPIHandlers INSTANCE methods)
// =========================================================================

/// Delegates the interactive MPI Index command to the internal handler (UPDATED).
#[allow(clippy::too_many_arguments)]
pub async fn handle_mpi_index_interactive(
    name: Option<String>,
    first_name: Option<String>,
    last_name: Option<String>,
    dob: Option<String>,
    mrn: String,
    address: Option<String>,
    phone: Option<String>,
    system: Option<String>,
    gender: Option<String>,
    mpi_handlers_state: MpiHandlersStateRef,
) -> Result<()> {
    // 1. Lock the state and get the handler instance.
    let handlers_lock = mpi_handlers_state.lock().await;
    // handlers_arc is &Arc<MPIHandlers>
    let handlers_arc = handlers_lock.as_ref().context("MPIHandlers not initialized in state mutex.")?;
    // FIX: Remove redundant .as_ref() call. Method calls handle the dereferencing.
    let handlers = handlers_arc;

    // 2. Retrieve existing patient data by MRN (Helper function assumed to be non-static).
    let existing_patient_opt = handlers.retrieve_and_convert_patient(&mrn).await
        .map_err(|e| anyhow!("Failed to retrieve existing patient by MRN: {}", e))?;
    
    // 3. Call the non-static instance method.
    // 
    handlers.handle_index(
        existing_patient_opt,
        name,
        first_name,
        last_name,
        dob,
        mrn,
        address,
        phone,
        system,
        gender,
    ).await
    .map_err(|e| anyhow!("MPI index operation failed: {}", e))
}

/// Delegates the interactive MPI Match command to the internal handler (UPDATED).
#[allow(clippy::too_many_arguments)]
pub async fn handle_mpi_match_interactive(
    name: Option<String>,
    dob: Option<String>,
    address: Option<String>,
    phone: Option<String>,
    name_algo: Option<String>, // Option<String> from CLI args
    target_mrn: Option<String>,
    candidate_mrn: Option<String>,
    score: Option<f64>,
    action: Option<String>,
    mpi_handlers_state: MpiHandlersStateRef,
) -> Result<()> {
    // 1. Lock the state and get the handler instance.
    let handlers_lock = mpi_handlers_state.lock().await;
    let handlers_arc = handlers_lock.as_ref().context("MPIHandlers not initialized in state mutex.")?;
    let handlers = handlers_arc;

    // FIX 1: Convert Option<String> name_algo to the required NameMatchAlgorithm enum.
    // Assuming NameMatchAlgorithm::from_str is implemented or similar utility exists.
    // We'll default to JaroWinkler, as seen in the command definition.
    let resolved_name_algo = name_algo
        .as_deref() // Get Option<&str>
        .unwrap_or("jaro-winkler") // Default to string representation of the default algorithm
        .parse::<NameMatchAlgorithm>() // Parse the string into the enum
        .map_err(|e| anyhow!("Invalid value for name-algo: {}", e))?;
    
    // 2. Call the non-static instance method.
    handlers.handle_match(
        name,
        dob,
        address,
        phone,
        resolved_name_algo, // FIX 2: Pass the concrete enum type
        target_mrn,
        candidate_mrn,
        score,
        action,
    ).await
    .map_err(|e| anyhow!("MPI match operation failed: {}", e))
}

/// Delegates the interactive MPI Merge command to the internal handler (UPDATED).
#[allow(clippy::too_many_arguments)]
pub async fn handle_mpi_merge_interactive(
    master_mrn: String,
    duplicate_mrn: String,
    user_id: Option<String>,
    reason: Option<String>,
    resolution_policy: Option<String>, // FIX: Needs to be unwrapped
    mpi_handlers_state: MpiHandlersStateRef,
) -> Result<()> {
    // 1. Lock the state and get the handler instance.
    let handlers_lock = mpi_handlers_state.lock().await;
    let handlers_arc = handlers_lock.as_ref().context("MPIHandlers not initialized in state mutex.")?;
    let handlers = handlers_arc;

    // FIX 1: Unwrap the mandatory resolution_policy field before calling handle_merge
    let required_policy = resolution_policy
        .context("MPI Merge requires a --resolution-policy.")?;

    // 2. Call the non-static instance method, ensuring correct argument order and types.
    handlers.handle_merge(
        master_mrn,
        duplicate_mrn,
        required_policy, // FIX 2: Pass unwrapped String
        user_id,         // FIX 3: Pass Optional arguments in the correct position
        reason,          // FIX 3: Pass Optional arguments in the correct position
    ).await
    .map_err(|e| anyhow!("MPI merge operation failed: {}", e))
}

/// Delegates the interactive MPI Resolve command to the internal handler (UPDATED).
#[allow(clippy::too_many_arguments)]
pub async fn handle_mpi_resolve_interactive(
    id: Option<String>,
    external_id: Option<String>,
    id_type: Option<String>,
    source_mrn: Option<String>,
    // FIX 1: Add the missing 'system' argument to match the caller and the internal handler
    system: Option<String>, 
    mpi_handlers_state: MpiHandlersStateRef,
) -> Result<()> {
    // 1. Lock the state and get the handler instance.
    let handlers_lock = mpi_handlers_state.lock().await;
    let handlers_arc = handlers_lock.as_ref().context("MPIHandlers not initialized in state mutex.")?;
    // The previous FIX note 'Remove redundant .as_ref() call' is misleading here, 
    // as it's necessary to get a reference from the Option (if MpiHandlersStateRef is a mutex wrapping an Option).
    let handlers = handlers_arc;
    
    // 2. Call the non-static instance method.
    handlers.handle_resolve(
        id,
        external_id,
        id_type,
        source_mrn,
        // FIX 2: Pass the new 'system' argument to the internal handler
        system, 
    ).await
    .map_err(|e| anyhow!("MPI resolve operation failed: {}", e))
}

/// Delegates the interactive MPI Fetch Identity command to the internal handler (UPDATED).
#[allow(clippy::too_many_arguments)]
pub async fn handle_mpi_fetch_identity_interactive(
    id: Option<String>,
    external_id: Option<String>,
    id_type: Option<String>,
    source_mrn: Option<String>,
    mpi_handlers_state: MpiHandlersStateRef,
) -> Result<()> {
    // 1. Lock the state and get the handler instance.
    let handlers_lock = mpi_handlers_state.lock().await;
    // handlers_arc is &Arc<MPIHandlers>
    let handlers_arc = handlers_lock.as_ref().context("MPIHandlers not initialized in state mutex.")?;
    // FIX: Remove redundant .as_ref() call.
    let handlers = handlers_arc;
    
    // 2. Call the non-static instance method.
    handlers.handle_fetch_identity(
        id,
        external_id,
        id_type,
        source_mrn,
        None, // FIX: Pass the missing 'system' argument as None
    ).await
    .map_err(|e| anyhow!("MPI fetch identity operation failed: {}", e))
}

/// Delegates the interactive MPI Link command to the internal handler (UPDATED).
pub async fn handle_mpi_link_interactive(
    master_id: String,
    external_id: String,
    id_type: Option<String>,
    mpi_handlers_state: MpiHandlersStateRef,
) -> Result<()> {
    // 1. Lock the state and get the handler instance.
    let handlers_lock = mpi_handlers_state.lock().await;
    // handlers_arc is &Arc<MPIHandlers>
    let handlers_arc = handlers_lock.as_ref().context("MPIHandlers not initialized in state mutex.")?;
    // FIX: Remove redundant .as_ref() call.
    let handlers = handlers_arc;

    // 2. Call the non-static instance method.
    // 
    handlers.handle_link(
        master_id,
        external_id,
        id_type.unwrap(),
    ).await
    .map_err(|e| anyhow!("MPI link operation failed: {}", e))
}

/// Delegates the interactive MPI Search command to the internal handler (UPDATED).
#[allow(clippy::too_many_arguments)]
pub async fn handle_mpi_search_interactive(
    name: Option<String>,
    first_name: Option<String>,
    last_name: Option<String>,
    dob: Option<String>,
    address: Option<String>,
    phone: Option<String>,
    mpi_handlers_state: MpiHandlersStateRef,
) -> Result<()> {
    // 1. Lock the state and get the handler instance.
    let handlers_lock = mpi_handlers_state.lock().await;
    // handlers_arc is &Arc<MPIHandlers>
    let handlers_arc = handlers_lock.as_ref().context("MPIHandlers not initialized in state mutex.")?;
    // FIX: Remove redundant .as_ref() call.
    let handlers = handlers_arc;

    // 2. Call the non-static instance method.
    // 
    handlers.handle_search(
        name,
        first_name,
        last_name,
        dob,
        address,
        phone,
    ).await
    .map_err(|e| anyhow!("MPI search operation failed: {}", e))
}

/// Delegates the interactive MPI Audit command to the internal handler (UPDATED).
pub async fn handle_mpi_audit_interactive(
    mpi_id: String,
    timeframe: Option<String>,
    mpi_handlers_state: MpiHandlersStateRef,
) -> Result<()> {
    // 1. Lock the state and get the handler instance.
    let handlers_lock = mpi_handlers_state.lock().await;
    // handlers_arc is &Arc<MPIHandlers>
    let handlers_arc = handlers_lock.as_ref().context("MPIHandlers not initialized in state mutex.")?;
    // FIX: Remove redundant .as_ref() call.
    let handlers = handlers_arc;

    // 2. Call the non-static instance method.
    handlers.handle_audit(
        mpi_id,
        timeframe,
    ).await
    .map_err(|e| anyhow!("MPI audit operation failed: {}", e))
}

/// Delegates the interactive MPI Split command to the internal handler (UPDATED).
pub async fn handle_mpi_split_interactive(
    merged_id: String,
    new_patient_data: Patient,
    user_id: String, 
    reason: Option<String>,
    // FIX 1: Add the required split patient ID from the CLI context (usually a string)
    split_patient_id: String, 
    mpi_handlers_state: MpiHandlersStateRef,
) -> Result<()> {
    // 1. Lock the state and get the handler instance.
    let handlers_lock = mpi_handlers_state.lock().await;
    let handlers_arc = handlers_lock.as_ref().context("MPIHandlers not initialized in state mutex.")?;
    let handlers = handlers_arc;

    // FIX 2: Unwrap reason from Option<String> to String, as required by handle_split.
    let required_reason = reason.context("MPI Split command requires a 'reason' for the audit trail.")?;

    // FIX 3: Parse the split_patient_id (String) to i32, as required by handle_split.
    let required_split_id = split_patient_id.parse::<i32>()
        .map_err(|e| anyhow!("Invalid format for split_patient_id: {}", e))?;

    // 2. Call the non-static instance method with all arguments in the correct order.
    handlers.handle_split(
        merged_id,
        new_patient_data,
        required_reason, // Note: Reason is the 3rd argument in handle_split signature
        required_split_id, // Note: split_patient_id is the 4th argument in handle_split signature
    ).await
    .map_err(|e| anyhow!("MPI split operation failed: {}", e))
}

/// Delegates the interactive MPI Get Golden Record command to the internal handler (UPDATED).
pub async fn handle_get_golden_record_interactive(
    patient_id: String,
    mpi_handlers_state: MpiHandlersStateRef,
) -> Result<()> {
    // 1. Lock the state and get the handler instance.
    let handlers_lock = mpi_handlers_state.lock().await;
    // handlers_arc is &Arc<MPIHandlers>
    let handlers_arc = handlers_lock.as_ref().context("MPIHandlers not initialized in state mutex.")?;
    // FIX: Remove redundant .as_ref() call.
    let handlers = handlers_arc;

    // 2. Call the non-static instance method.
    handlers.handle_get_golden_record(
        patient_id
    ).await
    .map_err(|e| anyhow!("Get Golden Record operation failed: {}", e))
}

// =========================================================================
// EXTERNAL CLI HANDLERS (Calling non-static methods via the instance)
// =========================================================================

/// Handles the 'mpi index' command from the non-interactive CLI (UPDATED).
#[allow(clippy::too_many_arguments)]
pub async fn handle_mpi_index(
    storage: Arc<dyn GraphStorageEngine>,
    name: Option<String>,
    first_name: Option<String>,
    last_name: Option<String>,
    dob: Option<String>,
    mrn: String,
    address: Option<String>,
    phone: Option<String>,
    system: Option<String>, 
    gender: Option<String>, 
) -> Result<()> {
    // 1. Get or Initialize MPI Handlers from the Singleton using the provided helper
    let handlers_arc = get_or_init_handlers_from_singleton(storage).await?;
    // FIX: Remove redundant .as_ref() call. handlers_arc is Arc<MPIHandlers> here.
    let handlers = handlers_arc.as_ref();

    // 2. Retrieve existing patient data by MRN.
    let existing_patient_opt = handlers.retrieve_and_convert_patient(&mrn).await
        .map_err(|e| anyhow!("Failed to retrieve existing patient by MRN: {}", e))?;

    // 3. Call the core handler logic (non-static instance method).
    // 
    handlers.handle_index(
        existing_patient_opt,
        name,
        first_name,
        last_name,
        dob,
        mrn,
        address,
        phone,
        system, 
        gender, 
    )
    .await
    .map_err(|e| anyhow!("MPI index operation failed: {}", e))?;

    Ok(())
}

/// Handle MPI Match command in non-interactive mode (UPDATED to call non-static).
#[allow(clippy::too_many_arguments)]
pub async fn handle_mpi_match(
    storage: Arc<dyn GraphStorageEngine>,
    name: Option<String>, 
    dob: Option<String>,
    address: Option<String>,
    phone: Option<String>,
    name_algo: NameMatchAlgorithm,
    target_mrn: Option<String>,  
    candidate_mrn: Option<String>, 
    score: Option<f64>,  
    action: Option<String>,  
) -> Result<(), GraphError> {
    let handlers_arc = get_or_init_handlers_from_singleton(storage)
        .await
        .map_err(|e| GraphError::InternalError(e.to_string()))?;

    // FIX: REMOVE the mandatory check for 'name'.
    // The name field is optional for MRN-based match/link commands.
    // let required_name = name.context("Match command requires a 'name' field.")
    //     .map_err(|e| GraphError::InvalidRequest(e.to_string()))?;

    // Call the core handler logic (non-static instance method).
    handlers_arc.as_ref().handle_match(
        name, // The original Option<String> is now passed directly
        dob,
        address,
        phone,
        name_algo,
        target_mrn,
        candidate_mrn,
        score,
        action,
    ).await
}


/// Handle MPI Merge command in non-interactive mode (UPDATED to call non-static).
#[allow(clippy::too_many_arguments)]
pub async fn handle_mpi_merge(
    storage: Arc<dyn GraphStorageEngine>,
    master_mrn: String,    
    duplicate_mrn: String,    
    user_id: Option<String>, 
    reason: Option<String>, 
    resolution_policy: String,
) -> Result<(), GraphError> {
    let handlers_arc = get_or_init_handlers_from_singleton(storage)
        .await
        .map_err(|e| GraphError::InternalError(e.to_string()))?;

    // Call the core handler logic (non-static instance method).
    // 
    handlers_arc.as_ref().handle_merge(
        master_mrn,
        duplicate_mrn,
        resolution_policy,
        user_id,
        reason,
    ).await
}

/// Handles the 'mpi resolve' command from the non-interactive CLI (UPDATED to call NON-STATIC).
#[allow(clippy::too_many_arguments)]
pub async fn handle_mpi_resolve(
    storage: Arc<dyn GraphStorageEngine>,
    id: Option<String>,
    external_id: Option<String>,
    id_type: Option<String>,
    source_mrn: Option<String>, 
    // FIX 1: Add the new 'system' parameter to the function signature
    system: Option<String>,
) -> Result<()> {
    // 1. Get or Initialize MPI Handlers from the Singleton
    let handlers_arc = get_or_init_handlers_from_singleton(storage).await?;
    let handlers = handlers_arc.as_ref();

    // 2. Delegate to the internal NON-STATIC instance method.
    handlers.handle_resolve(
        id,
        external_id,
        id_type,
        source_mrn,
        // FIX 2: Pass the new 'system' parameter to the non-static handler
        system,
    )
    .await
    .map_err(|e| anyhow!("MPI resolve operation failed: {}", e))
}

/// Handles the 'mpi fetch-identity' command from the non-interactive CLI (UPDATED to call NON-STATIC).
#[allow(clippy::too_many_arguments)]
pub async fn handle_mpi_fetch_identity(
    storage: Arc<dyn GraphStorageEngine>,
    id: Option<String>,
    external_id: Option<String>,
    id_type: Option<String>,
    source_mrn: Option<String>,
    // The CLI command does not provide a 'system', so the function signature remains with 4 arguments here
) -> Result<()> {
    // 1. Get or Initialize MPI Handlers from the Singleton
    let handlers_arc = get_or_init_handlers_from_singleton(storage).await?;
    let handlers = handlers_arc.as_ref();

    // 2. Delegate to the internal NON-STATIC instance method.
    // FIX: Pass None as the fifth argument (system) to satisfy the new handler signature.
    handlers.handle_fetch_identity(
        id,
        external_id,
        id_type,
        source_mrn,
        None, // <- FIX: Missing 'system' argument supplied as None
    )
    .await
    .map_err(|e| anyhow!("MPI fetch identity operation failed: {}", e))
}

/// Handle MPI Link command in non-interactive mode (UPDATED to call non-static, required Option check).
pub async fn handle_mpi_link(
    storage: Arc<dyn GraphStorageEngine>,
    master_id: String,
    external_id: String,
    id_type: Option<String>, 
) -> Result<(), GraphError> {
    let handlers_arc = get_or_init_handlers_from_singleton(storage)
        .await
        .map_err(|e| GraphError::InternalError(e.to_string()))?;

    let required_id_type = id_type.context("Link command requires an 'id_type' field.")
        .map_err(|e| GraphError::InvalidRequest(e.to_string()))?;

    // Call the core handler logic (non-static instance method).
    // 
    handlers_arc.as_ref().handle_link(master_id, external_id, required_id_type).await
}

/// Handle MPI Audit command in non-interactive mode (UPDATED to call non-static).
pub async fn handle_mpi_audit(
    storage: Arc<dyn GraphStorageEngine>,
    mpi_id: String,
    timeframe: Option<String>,
) -> Result<(), GraphError> {
    let handlers_arc = get_or_init_handlers_from_singleton(storage)
        .await
        .map_err(|e| GraphError::InternalError(e.to_string()))?;

    // Call the core handler logic (non-static instance method).
    handlers_arc.as_ref().handle_audit(mpi_id, timeframe).await
}

/// Handle MPI Split command in non-interactive mode (UPDATED to call non-static).
pub async fn handle_mpi_split(
    storage: Arc<dyn GraphStorageEngine>,
    merged_id: String,
    new_patient_data: Patient,
    reason: Option<String>, 
    // FIX 1: Add the new required argument (assuming String from CLI)
    split_patient_id_str: String, 
) -> Result<(), GraphError> {
    let handlers_arc = get_or_init_handlers_from_singleton(storage)
        .await
        .map_err(|e| GraphError::InternalError(e.to_string()))?;

    // FIX 2: Unwrap reason from Option<String> to String, providing a descriptive error.
    let required_reason = reason
        .context("MPI Split command requires a 'reason' for the audit trail.")
        .map_err(|e| GraphError::InvalidRequest(e.to_string()))?;

    // FIX 3: Parse the split_patient_id (String) to i32.
    let required_split_id = split_patient_id_str.parse::<i32>()
        .map_err(|e| GraphError::InvalidRequest(format!("Invalid format for split_patient_id: {}", e)))?;

    // Call the core handler logic (non-static instance method).
    handlers_arc.as_ref().handle_split(
        merged_id, 
        new_patient_data, 
        required_reason,       // Now required String
        required_split_id,     // Now required i32
    ).await
    .map_err(|e| anyhow!("MPI split operation failed: {}", e))
    .map_err(|e| GraphError::InternalError(e.to_string()))
}

/// Handle MPI Get Golden Record command in non-interactive mode (UPDATED to call non-static).
pub async fn handle_get_golden_record(
    storage: Arc<dyn GraphStorageEngine>,
    patient_id: PatientId, // This is the structured PatientId type
) -> Result<(), GraphError> {
    let handlers_arc = get_or_init_handlers_from_singleton(storage)
        .await
        .map_err(|e| GraphError::InternalError(e.to_string()))?;

    // Call the core handler logic (non-static instance method).
    // FIX: Convert the PatientId struct to a String using .to_string() before passing it.
    let _record = handlers_arc.as_ref().handle_get_golden_record(patient_id.to_string()).await?;
    
    println!("Golden Record retrieved successfully.");
    Ok(())
}
/// Delegates the non-interactive MPI Search command to the internal handler (UPDATED to call NON-STATIC).
#[allow(clippy::too_many_arguments)]
pub async fn handle_mpi_search(
    storage: Arc<dyn GraphStorageEngine>,
    name: Option<String>,
    first_name: Option<String>,
    last_name: Option<String>,
    dob: Option<String>,
    address: Option<String>,
    phone: Option<String>,
) -> Result<()> {
    // 1. Get or Initialize MPI Handlers from the Singleton
    let handlers_arc = get_or_init_handlers_from_singleton(storage).await?;
    let handlers = handlers_arc.as_ref();

    // 2. Delegate to the internal NON-STATIC instance method.
    // 
    handlers.handle_search(
        name,
        first_name,
        last_name,
        dob,
        address,
        phone,
    )
    .await
    .map_err(|e| anyhow!("MPI search operation failed: {}", e))
}
 
pub async fn handle_mpi_match_via_singleton(
    name: String,
    dob: Option<String>,
    address: Option<String>,
    phone: Option<String>,
    name_algo: NameMatchAlgorithm,
) -> Result<(), anyhow::Error> {
    info!("Executing MPI match via global singleton retrieval.");

    // Retrieve the MPIHandlers instance from the global singleton, initializing it
    // if necessary.
    let handlers_arc = MPI_HANDLERS_SINGLETON.get_or_try_init(|| async {
        
        // ASSUMPTION: MPIHandlers::new().await already returns Result<Arc<MPIHandlers>, GraphError>.

        MPIHandlers::new()
            .await
            // We use map_err to convert the inner error type (e.g., GraphError) to anyhow::Error
            .map_err(|e| anyhow!("Failed to initialize MPI Handlers for singleton: {}", e))
            
    })
    .await
    .context("Failed to get or initialize MPIHandlers from the global singleton.")?;
    
    // The result 'handlers_arc' is &Arc<MPIHandlers>. We use handlers_arc.as_ref() to get &MPIHandlers.
    // The previous error trace shows the call on the dereferenced type, which is correct here.
    let handlers = handlers_arc.as_ref();
    
    // The actual core logic is the instance method on MPIHandlers.
    handlers.handle_match(
        // FIX 1: Wrap 'name' in Some() because handle_match expects Option<String>
        Some(name),
        dob,
        address,
        phone,
        name_algo,
        // FIX 2: Supply the four missing arguments as None
        None, // target_mrn: Option<String>
        None, // candidate_mrn: Option<String>
        None, // score: Option<f64>
        None, // action: Option<String>
    )
    .await
    .map_err(|e| anyhow!("Core MPI Match failed: {}", e))?;

    Ok(())
}
