// server/src/cli/commands.rs
// ADDED: 2025-08-08 - Added `pub use crate::cli::config::StorageEngineType` to re-export `StorageEngineType` publicly, resolving `error[E0603]` in `interactive.rs`.
// UPDATED: 2025-08-08 - Changed import from `lib::storage_engine::config::StorageEngineType` to `crate::cli::config::StorageEngineType` to align with project structure.
// UPDATED: 2025-08-08 - Ensured `StorageEngineType` supports `Sled`, `RocksDB`, `InMemory`, `Redis`, `PostgreSQL`, `MySQL` as per updated enum definition.
// NOTE: Kept `std::path::PathBuf` as it's standard for CLI args and compatible with `fs2` in `config.rs`.
// FIXED: 2025-08-08 - Added `permanent` boolean field to `UseAction::Storage` and `CommandType::UseStorage` to resolve `E0026` error in `config.rs`.
// ADDED: 2025-08-09 - Added `Save` command with `Storage` and `Config` subcommands to save storage engine and configuration changes.
// FIXED: 2025-08-09 - Added `ShowArgs` wrapper struct to fix `E0277` trait bound error for `ShowAction`.
// UPDATED: 2025-11-06 - Unified `Exec`, `Query`, `-q`, `-c` into single `Query` command with optional `--language`.
//                     Bare strings in interactive mode are now treated as queries with inference.
use serde::{Deserialize, Serialize};
use clap::{Parser, Subcommand, Arg, Args, ArgAction, ValueEnum}; 
use std::path::PathBuf;
use uuid::Uuid;
use chrono::{DateTime, Utc};
use std::fmt;
use std::str::FromStr;
// Re-export StorageEngineType to make it accessible to `interactive.rs`
pub use crate::config::StorageEngineType;
pub use models::medical::{ Patient };

// Helper structs for variants that need Args implementation
#[derive(Args, Debug, PartialEq, Clone)]
pub struct PortArgs {
    /// Port number
    #[arg(short, long)]
    pub port: Option<u16>,
}

#[derive(Debug, PartialEq, Clone, Args)]
pub struct HelpArgs {
    pub filter_command: Option<String>,
    pub command_path: Vec<String>,
}

/// Custom parser for storage engine to accept many aliases (rocksdb, rocks-db, postgres, postgresql, postgre-sql, mysql, my-sql, inmemory, in-memory).
pub fn parse_storage_engine(engine: &str) -> Result<StorageEngineType, String> {
    match engine.to_lowercase().as_str() {
        "sled" => Ok(StorageEngineType::Sled),
        "tikv" => Ok(StorageEngineType::TiKV),
        "rocksdb" | "rocks-db" => Ok(StorageEngineType::RocksDB),
        "inmemory" | "in-memory" | "in_memory" => Ok(StorageEngineType::InMemory),
        "redis" => Ok(StorageEngineType::Redis),
        "postgres" | "postgresql" | "postgre-sql" | "postgres-sql" => Ok(StorageEngineType::PostgreSQL),
        "mysql" | "my-sql" | "my_sql" => Ok(StorageEngineType::MySQL),
        other => Err(format!(
            "Invalid storage engine: '{}'. Supported values (examples): sled, rocksdb, rocks-db, tikv, inmemory, in-memory, redis, postgres, postgresql, postgre-sql, mysql, my-sql",
            other
        )),
    }
}

/// Custom parser for KV operation to accept `get`, `set`, or `delete`.
pub fn parse_kv_operation(operation: &str) -> Result<String, String> {
    match operation.to_lowercase().as_str() {
        "get" | "set" | "delete" => Ok(operation.to_string()),
        other => Err(format!(
            "Invalid KV operation: '{}'. Supported operations: get, set, delete",
            other
        )),
    }
}

/// A unified query that carries the raw string and an optional language hint.
#[derive(Debug, Clone)]
pub struct UnifiedQuery {
    pub query: String,
    pub language: Option<String>, // None → infer
}

/// Enum representing the parsed command type in interactive mode.
#[derive(Debug, PartialEq, Clone)]
pub enum CommandType {
    // Daemon Commands
    Daemon(DaemonCliCommand),
    // Rest Commands
    Rest(RestCliCommand),
    // Storage Commands
    Storage(StorageAction),
    // Use Commands
    UseStorage { engine: StorageEngineType, permanent: bool, migrate: bool },
    UsePlugin { enable: bool },
    // Save Commands
    SaveStorage,
    SaveConfig,
    // Top-level Start command variants (can also be subcommands of 'start')
    StartRest { port: Option<u16>, cluster: Option<String>, rest_port: Option<u16>, rest_cluster: Option<String> },
    StartStorage { port: Option<u16>, config_file: Option<PathBuf>, cluster: Option<String>, storage_port: Option<u16>, storage_cluster: Option<String> },
    StartDaemon { port: Option<u16>, cluster: Option<String>, daemon_port: Option<u16>, daemon_cluster: Option<String> },
    StartAll {
        port: Option<u16>,
        cluster: Option<String>,
        daemon_port: Option<u16>,
        daemon_cluster: Option<String>,
        listen_port: Option<u16>,
        rest_port: Option<u16>,
        rest_cluster: Option<String>,
        storage_port: Option<u16>,
        storage_cluster: Option<String>,
        storage_config_file: Option<PathBuf>,
    },
    // Top-level Stop commands (can also be subcommands of 'stop')
    StopAll,
    StopRest(Option<u16>),
    StopDaemon(Option<u16>),
    StopStorage(Option<u16>),
    // Top-level Status commands (can also be subcommands of 'status')
    StatusSummary,
    StatusDaemon(Option<u16>),
    StatusStorage(Option<u16>),
    StatusCluster,
    StatusRaft(Option<u16>),
    // Authentication and User Management
    Auth { username: String, password: String },
    Authenticate { username: String, password: String },
    RegisterUser { username: String, password: String },
    // General Information
    Version,
    Health,
    // Reload Commands
    ReloadAll,
    ReloadRest,
    ReloadStorage,
    ReloadDaemon(Option<u16>),
    ReloadCluster,
    // Restart Commands
    RestartAll {
        port: Option<u16>,
        cluster: Option<String>,
        listen_port: Option<u16>,
        storage_port: Option<u16>,
        storage_config_file: Option<PathBuf>,
        daemon_cluster: Option<String>,
        daemon_port: Option<u16>,
        rest_cluster: Option<String>,
        rest_port: Option<u16>,
        storage_cluster: Option<String>,
    },
    RestartRest { port: Option<u16>, cluster: Option<String>, rest_port: Option<u16>, rest_cluster: Option<String> },
    RestartStorage { port: Option<u16>, config_file: Option<PathBuf>, cluster: Option<String>, storage_port: Option<u16>, storage_cluster: Option<String> },
    RestartDaemon { port: Option<u16>, cluster: Option<String>, daemon_port: Option<u16>, daemon_cluster: Option<String> },
    RestartCluster,
    Show(ShowAction),
    // Utility Commands
    Clear,
    Cleanup(CleanupCommand), // <--- ADDED
    Help(HelpArgs),
    Exit,
    Unknown,
    // Key-Value and Query Commands
    Kv { action: KvAction },
    Migrate(MigrateAction),
    /// Unified query command (used by `query`, `exec`, `-q`, `-c`, and bare strings)
    Query { query: String, language: Option<String> },
    /// Unified query command (used by `visualize`, `-q`, `-c`, and bare strings)
    Visualize { query: String, language: Option<String> },
    // Graph and Index commands - plain variants, no attributes
    Graph(GraphAction),
    Index(IndexAction),
    
    // =========================================================================
    // COMMAND HISTORY
    // =========================================================================
    History {
        action: Option<HistoryCommand>,
    },

    // =========================================================================
    // PATIENT MANAGEMENT
    // =========================================================================
    Patient(PatientCommand),

    // =========================================================================
    // CLINICAL WORKFLOW
    // =========================================================================
    Encounter(EncounterCommand),
    Diagnosis(DiagnosisCommand),
    Prescription(PrescriptionCommand),
    Note(NoteCommand),
    Referral(ReferralCommand),
    Triage(TriageCommand),
    Disposition(DispositionCommand),
    Allergy(AllergyCommand),
    Appointment(AppointmentCommand),
    Problem(ProblemCommand),
    Order(OrderCommand),
    Discharge(DischargeCommand),
    Procedure(ProcedureCommand),
    // =========================================================================
    // DOSING
    // =========================================================================
    Dosing(DosingCommand),

    // =========================================================================
    // ALERT & NOTIFICATION SYSTEM
    // =========================================================================
    Alert(AlertCommand),

    // =========================================================================
    // PATHOLOGY
    // =========================================================================
    Pathology(PathologyCommand),

    // =========================================================================
    // MICROBIOLOGY
    // =========================================================================
    Microbiology(MicrobiologyCommand),

    // =========================================================================
    // VITAL & OBSERVATION
    // =========================================================================
    Vitals(VitalsCommand),
    Observation(ObservationCommand),

    // =========================================================================
    // LAB & IMAGING
    // =========================================================================
    Lab(LabCommand),
    Imaging(ImagingCommand),

    // =========================================================================
    // SPECIALTY CARE
    // =========================================================================
    Chemo(ChemoCommand),
    Radiation(RadiationCommand),
    Surgery(SurgeryCommand),

    // =========================================================================
    // DRUG SAFETY & INTERACTIONS
    // =========================================================================
    Drug(DrugCommand),

    // =========================================================================
    // POPULATION HEALTH & ANALYTICS
    // =========================================================================
    Population(PopulationCommand),
    Analytics(AnalyticsCommand),
    Metrics(MetricsCommand),

    // =========================================================================
    // COMPLIANCE & AUDIT
    // =========================================================================
    Audit(AuditCommand),
    Export(ExportCommand),

    // =========================================================================
    // FACILITY & ADMIN
    // =========================================================================
    Facility(FacilityCommand),
    Access(AccessCommand),
    Financial(FinancialCommand),

    // =========================================================================
    // QUALITY & COMPLIANCE
    // =========================================================================
    Quality(QualityCommand),
    Incident(IncidentCommand),
    Compliance(ComplianceCommand),

    // =========================================================================
    // RESEARCH & AI
    // =========================================================================
    Research(ResearchCommand),
    Ml(MlCommand),
    ClinicalTrial(ClinicalTrialCommand),
    Model(ModelCommand),

    // =========================================================================
    // NURSING & CARE COORDINATION
    // =========================================================================
    Nursing(NursingCommand),
    Education(EducationCommand),
    DischargePlanning(DischargePlanningCommand),

    // =========================================================================
    // EMERGENCY, TIMING, SCHEDULING, RESUS
    // =========================================================================
    Emerg(EmergCommand),
    Timing(TimingCommand),
    Schedule(ScheduleCommand),
    Resus(ResusCommand),

    // =========================================================================
    // Master Patient Index
    // =========================================================================
    Mpi(MPICommand), 
}

#[derive(Subcommand, Debug, PartialEq, Clone)]
pub enum CleanupCommand {
    /// Cleans up old log files based on a retention policy.
    Logs {
        /// Number of days of logs to keep. Defaults to 7.
        #[clap(long, default_value = "7")]
        days_retention: i32,
        /// Force cleanup without confirmation.
        #[clap(long)]
        force: bool,
    },
    /// Removes temporary and cache files from the application's working directory.
    TemporaryFiles,
    /// Clears data in a specific storage engine (DANGEROUS and non-recoverable).
    Storage {
        /// Storage engine whose data directory should be cleared.
        #[arg(value_parser = parse_storage_engine)]
        engine: StorageEngineType,
        /// Force cleanup without confirmation.
        #[clap(long)]
        force: bool,
    },
    /// Executes all standard cleanup operations (logs and temporary files).
    All {
        /// Number of days of logs to keep. Defaults to 7.
        #[clap(long, default_value = "7")]
        days_retention: i32,
        /// Force cleanup without confirmation.
        #[clap(long)]
        force: bool,
    },
    Graph, 
}

// =========================================================================
// History Command
// =========================================================================
/// Command history and auditing for executed commands and queries.
// =========================================================================
// History Command
// =========================================================================
/// Command history and auditing for executed commands and queries.
#[derive(Subcommand, Debug, PartialEq, Clone)]
pub enum HistoryCommand {
    /// Lists command history (default action, equivalent to 'list').
    #[command(name = "list")]
    List(HistoryListArgs),

    /// Shows the N most recent commands (Top/Head).
    #[clap(alias = "head")]
    Top(HistoryTopTailArgs),

    /// Shows the N oldest commands (Bottom/Tail).
    #[clap(alias = "bottom")]
    Tail(HistoryTopTailArgs),

    /// Shows commands executed within a specific time window.
    Window(HistoryWindowArgs),

    /// Filters history based on keywords in the command content.
    Search {
        /// The keyword or command fragment to search for.
        #[arg(value_name = "KEYWORD")]
        keyword: String,
        
        /// Inherits all HistoryFilterArgs.
        #[clap(flatten)]
        filters: HistoryFilterArgs,
    },

    /// Clears command history for a user or all history (DANGEROUS).
    Clear {
        /// Optional: Clear history only for this specific user.
        /// If not provided, clears history for ALL users.
        #[arg(long, short = 'u')]
        user: Option<String>,
        
        /// Required: Force clear without confirmation
        #[arg(long, short = 'f')]
        force: bool,
    },

    /// Displays summary statistics about command execution.
    Stats {
        /// Group and count statistics by a specific field (e.g., 'user', 'type', 'status').
        #[clap(long, short = 'b', value_name = "FIELD", default_value = "user")]
        by: String,
        
        /// Show stats for a specific user.
        #[clap(long, short = 'u', value_name = "USERNAME")]
        user: Option<String>,
    },
}

// --- Argument Structs for Reusability and Flags (Same as before) ---

/// Filters for the status of a command execution.
#[derive(Debug, Clone, PartialEq, Eq, clap::ValueEnum)]
pub enum HistoryStatusFilter {
    /// Command execution was successful.
    Success,
    /// Command execution failed (error occurred).
    Fail,
    /// Command execution timed out.
    Timeout,
}

/// Fields to sort the history results by.
#[derive(Debug, Clone, PartialEq, Eq, clap::ValueEnum)]
pub enum HistorySortField {
    /// Sort by execution time (most recent first).
    Time,
    /// Sort by the user who ran the command.
    User,
    /// Sort by the duration of the command execution.
    Duration,
    Id,
    Status,
}

/// Output format for the history results.
#[derive(Debug, Clone, PartialEq, Eq, clap::ValueEnum)]
pub enum HistoryOutputFormat {
    /// Plain text table (default for CLI).
    Table,
    /// JSON output format.
    Json,
    /// Comma-separated values format.
    Csv,
}

/// Group of arguments for filtering history records.
#[derive(Args, Debug, PartialEq, Clone)]
pub struct HistoryFilterArgs {
    /// Filter by a specific user. Use 'ALL' to see history for all users.
    #[clap(long, short = 'u', value_name = "USERNAME")]
    pub user: Option<String>,
    
    /// Filter by start time (e.g., '2025-12-01T10:00:00Z' or '3h ago').
    #[clap(long, value_name = "TIMESTAMP")]
    pub since: Option<String>,
    
    /// Filter by end time.
    #[clap(long, value_name = "TIMESTAMP")]
    pub until: Option<String>,
    
    /// Filter by command type (e.g., 'query', 'daemon', 'config', 'graph').
    #[clap(long, short = 't', value_name = "TYPE")]
    pub command_type: Option<String>,
    
    /// Filter by command execution result.
    #[clap(long, short = 's', value_enum)]
    pub status: Option<HistoryStatusFilter>,

    /// Limit the number of results returned (Default: 100).
    #[clap(long, short = 'n', default_value = "100")]
    pub limit: usize,
    
    /// Inherits display flags.
    #[clap(flatten)]
    pub display: HistoryDisplayArgs,
}

/// Group of arguments for controlling history output display.
#[derive(Args, Debug, PartialEq, Clone)]
pub struct HistoryDisplayArgs {
    /// Show the entire command/query instead of truncating it.
    #[clap(long, short = 'f')]
    pub full_command: bool,
    
    /// Include extra metadata (duration, client IP, etc.).
    #[clap(long, short = 'v')]
    pub verbose: bool,
    
    /// Sort the results by a specific field (e.g., 'time', 'user', 'duration').
    #[clap(long, value_enum, default_value = "time")]
    pub sort_by: HistorySortField,
    
    /// Output format ('table', 'json', 'csv').
    #[clap(long, short = 'o', value_enum, default_value = "table")]
    pub format: HistoryOutputFormat,
    
    /// Display the raw command string without sanitization.
    #[clap(long)]
    pub raw: bool,
}

#[derive(Args, Debug, PartialEq, Clone)]
pub struct HistoryListArgs {
    /// Inherits all filtering and display flags.
    #[clap(flatten)]
    pub filters: HistoryFilterArgs,
}

#[derive(Args, Debug, PartialEq, Clone)]
pub struct HistoryTopTailArgs {
    /// The number of records to show (N). Defaults to 10.
    #[arg(value_name = "N", default_value = "10")]
    pub n: usize,
    
    /// Filter to show only commands that failed (for 'top').
    #[clap(long, conflicts_with = "successful_only")]
    pub fail_only: bool,
    
    /// Filter to show only commands that succeeded (for 'tail').
    #[clap(long, conflicts_with = "fail_only")]
    pub successful_only: bool,
    
    /// Inherits general filtering and display flags.
    #[clap(flatten)]
    pub filters: HistoryFilterArgs,
}

#[derive(Args, Debug, PartialEq, Clone)]
pub struct HistoryWindowArgs {
    /// The duration window relative to the present (e.g., '1h', '30m', '7d').
    #[arg(value_name = "DURATION")]
    pub duration: String,
    
    /// Look backward from a specific point in time (e.g., '3d' starts the window 3 days ago).
    #[clap(long, value_name = "BACK_DURATION")]
    pub before: Option<String>,
    
    /// Groups results and shows execution metrics per time interval (e.g., '1h').
    #[clap(long, short = 'i', value_name = "INTERVAL_DURATION")]
    pub interval: Option<String>,
    
    /// Inherits general filtering and display flags.
    #[clap(flatten)]
    pub filters: HistoryFilterArgs,
}

// =========================================================================
// CLINICAL WORKFLOW
// =========================================================================
#[derive(Subcommand, Debug, PartialEq, Clone)]
pub enum DischargeCommand {
    /// Initiate discharge planning
    Plan {
        patient_id: i32,
        #[clap(long)] encounter_id: Option<Uuid>,
        #[clap(long)] estimated_date: Option<String>,
        #[clap(long)] target_disposition: Option<DispositionTarget>,
        #[clap(long)] barriers: Vec<String>,
        #[clap(long)] primary_diagnosis: Option<String>,
    },

    /// Document discharge readiness assessment
    Readiness {
        patient_id: i32,
        #[clap(long)] medically_stable: bool,
        #[clap(long)] pain_controlled: bool,
        #[clap(long)] mobility_safe: bool,
        #[clap(long)] barriers_resolved: Vec<String>,
        #[clap(long)] pending_items: Vec<String>,
        #[clap(long)] assessed_by: i32,
    },

    /// Generate discharge summary
    Summary {
        patient_id: i32,
        #[clap(long)] template: Option<SummaryTemplate>,
        #[clap(long)] include_reconciliation: bool,
        #[clap(long)] include_followup: bool,
        #[clap(long)] format: Option<OutputFormat>,
    },

    /// Medication reconciliation at discharge
    MedRec {
        patient_id: i32,
        #[clap(value_enum)]
        action: MedRecAction,
        #[clap(long)]
        medication: String,
        #[clap(long)]
        new_dose: Option<String>,     // for Modify
        #[clap(long)]
        new_frequency: Option<String>, // for Modify
        #[clap(long)]
        reason: Option<String>,
        #[clap(long)]
        reconciled_by: i32,
    },

    /// Create discharge orders
    Orders {
        patient_id: i32,
        #[clap(long)] diet: Option<String>,
        #[clap(long)] activity: Option<String>,
        #[clap(long)] wound_care: Option<String>,
        #[clap(long)] monitoring: Vec<String>,
        #[clap(long)] restrictions: Vec<String>,
    },

    /// Schedule follow-up appointments
    FollowUp {
        patient_id: i32,
        provider_type: String, // "PCP", "Cardiology", "Oncology"
        #[clap(long)] days_out: Option<i64>,
        #[clap(long)] priority: Option<FollowUpPriority>,
        #[clap(long)] reason: Option<String>,
        #[clap(long)] telehealth: bool,
    },

    /// Patient education & instructions
    Education {
        patient_id: i32,
        topics: Vec<String>, // "medications", "diet", "warning-signs"
        #[clap(long)] method: Option<EducationMethod>,
        #[clap(long)] language: Option<String>,
        #[clap(long)] literacy_level: Option<String>,
        #[clap(long)] teach_back_verified: bool,
    },

    /// Finalize discharge
    Finalize {
        patient_id: i32,
        #[clap(long)] actual_date: Option<String>,
        #[clap(long)] disposition: DispositionTarget,
        #[clap(long)] transportation: Option<String>,
        #[clap(long)] accompanying_person: Option<String>,
        #[clap(long)] final_diagnosis: Vec<String>,
        #[clap(long)] completed_by: i32,
    },

    /// Discharge dashboard & analytics
    Dashboard {
        #[clap(long)] unit: Option<String>,
        #[clap(long)] provider_id: Option<i32>,
        #[clap(long)] timeframe: Option<String>,
        #[clap(long)] show_pending: bool,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, ValueEnum)]
pub enum DispositionTarget {
    Home,
    HomeHealth,
    SkilledNursing,
    Rehab,
    LTACH,
    Hospice,
    AgainstMedicalAdvice,
    Expired,
    Transfer,
}

impl fmt::Display for DispositionTarget {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DispositionTarget::Home               => write!(f, "Home"),
            DispositionTarget::HomeHealth         => write!(f, "HomeHealth"),
            DispositionTarget::SkilledNursing     => write!(f, "SkilledNursing"),
            DispositionTarget::Rehab              => write!(f, "Rehab"),
            DispositionTarget::LTACH              => write!(f, "LTACH"),
            DispositionTarget::Hospice            => write!(f, "Hospice"),
            DispositionTarget::AgainstMedicalAdvice => write!(f, "AgainstMedicalAdvice"),
            DispositionTarget::Expired            => write!(f, "Expired"),
            DispositionTarget::Transfer           => write!(f, "Transfer"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, ValueEnum)]
pub enum SummaryTemplate {
    Standard,
    ComplexChronic,
    Surgical,
    Stroke,
    MI,
    Sepsis,
    Oncology,
}

#[derive(Debug, Clone, PartialEq, Eq, ValueEnum)]
pub enum OutputFormat {
    Text,
    Html,
    Pdf,
    Fhir,
    Hl7,
}

#[derive(Subcommand, Debug, Clone, PartialEq, Eq)]
pub enum MedRecActionCmd {
    Continue,
    Hold,
    Modify { #[clap(long)] new_dose: String },
    Discontinue { #[clap(long)] reason: String },
    New {
        #[clap(long)] medication: String,
        #[clap(long)] dose: String,
        #[clap(long)] frequency: String,
    },
}

// lib/src/commands.rs — Add this enum

#[derive(Debug, Clone, PartialEq, Eq, clap::ValueEnum)]
pub enum MedRecAction {
    /// Continue medication as prescribed
    Continue,

    /// Modify dose/frequency/route
    Modify,

    /// Hold medication temporarily
    Hold,

    /// Permanently discontinue
    Discontinue,

    /// Add new medication at discharge
    New,

    /// Medication was not prescribed on admission but patient reports taking
    Resume,

    /// Medication reconciliation completed — no changes
    NoChange,

    /// Medication not reconciled (e.g., patient unable to provide info)
    UnableToReconcile,
}

#[derive(Debug, Clone, PartialEq, Eq, ValueEnum)]
pub enum FollowUpPriority {
    Urgent,    // <7 days
    Routine,   // 7-14 days
    Standard,  // 14-30 days
    Extended,  // >30 days
}

#[derive(Debug, Clone, PartialEq, Eq, ValueEnum)]
pub enum EducationMethod {
    Verbal,
    Written,
    Video,
    TeachBack,
    Interpreter,
}

#[derive(Subcommand, Debug, PartialEq, Clone)]
pub enum ProcedureCommand {
    /// Create new procedure order
    Order {
        encounter_id: Uuid,
        procedure: String,
        #[clap(long)] cpt_code: Option<String>,
        #[clap(long)] laterality: Option<Laterality>,
        #[clap(long)] priority: Option<ProcedurePriority>,
        #[clap(long)] indication: Option<String>,
        #[clap(long)] ordering_provider: Option<i32>,
        #[clap(long)] scheduled_date: Option<String>,
        #[clap(long)] location: Option<String>,
    },

    /// Document procedure performance
    Perform {
        procedure_id: Uuid,
        #[clap(long)] performing_provider: i32,
        #[clap(long)] start_time: Option<String>,
        #[clap(long)] end_time: Option<String>,
        #[clap(long)] anesthesia_type: Option<AnesthesiaType>,
        #[clap(long)] assistants: Vec<i32>,
        #[clap(long)] specimens: Vec<String>,
        #[clap(long)] implants: Vec<String>,
        #[clap(long)] ebl_ml: Option<i32>,
        #[clap(long)] complications: Vec<String>,
    },

    /// Record procedure result/report
    Result {
        procedure_id: Uuid,
        #[clap(long)] status: ProcedureStatus,
        #[clap(long)] findings: Option<String>,
        #[clap(long)] impression: Option<String>,
        #[clap(long)] pathology_sent: bool,
        #[clap(long)] report_by: i32,
        #[clap(long)] report_date: Option<String>,
    },

    /// Cancel procedure
    Cancel {
        procedure_id: Uuid,
        reason: String,
        #[clap(long)] cancelled_by: i32,
    },

    /// List procedures
    List {
        #[clap(long)] patient_id: Option<i32>,
        #[clap(long)] encounter_id: Option<Uuid>,
        #[clap(long)] status: Option<ProcedureStatus>,
        #[clap(long)] provider_id: Option<i32>,
        #[clap(long)] date_range: Option<String>,
        #[clap(long)] limit: Option<usize>,
    },

    /// Procedure timeline for patient
    Timeline {
        patient_id: i32,
        #[clap(long)] procedure_type: Option<String>,
        #[clap(long)] years: Option<i32>,
    },

    /// Quality & utilization analytics
    Analytics {
        #[clap(subcommand)]
        analytics_type: ProcedureAnalyticsType,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, ValueEnum)]
pub enum Laterality {
    Left,
    Right,
    Bilateral,
    Midline,
    None,
}

#[derive(Debug, Clone, PartialEq, Eq, ValueEnum)]
pub enum ProcedurePriority {
    Routine,
    Urgent,
    Stat,
    Elective,
    Emergent,
}

#[derive(Debug, Clone, PartialEq, Eq, ValueEnum)]
pub enum AnesthesiaType {
    General,
    Regional,
    Local,
    Monitored,
    Sedation,
    None,
}

#[derive(Debug, Clone, PartialEq, Eq, ValueEnum)]
pub enum ProcedureStatus {
    Ordered,
    Scheduled,
    InProgress,
    Completed,
    Cancelled,
    NotDone,
    Preliminary,
    Final,
}

#[derive(Debug, Clone, PartialEq, Eq, Subcommand)]
pub enum ProcedureAnalyticsType {
    /// Volume by procedure type
    Volume {
        #[clap(long)] procedure_type: Option<String>,
        #[clap(long)] provider: Option<i32>,
        #[clap(long)] timeframe: Option<String>,
    },

    /// Complication rates
    Complications {
        procedure: String,
        #[clap(long)] risk_adjusted: bool,
    },

    /// Turnaround time (order to completion)
    Turnaround {
        procedure: String,
        #[clap(long)] target_hours: Option<i64>,
    },

    /// Cancellation/no-show rate
    Cancellations,

    /// Provider procedure volume
    ProviderVolume {
        provider_id: i32,
        #[clap(long)] timeframe: Option<String>,
    },

    /// Procedure-specific outcomes
    Outcomes {
        procedure: String,
        #[clap(long)] outcome_metric: Option<String>,
    },
}
// AppointmentCommand
#[derive(Subcommand, Debug, PartialEq, Clone)]
pub enum AppointmentCommand {
    /// Schedule a new appointment
    Schedule {
        patient_id: i32,
        #[arg(long)] appointment_type: String,  // FOLLOW_UP, NEW_PATIENT, URGENT, PROCEDURE
        #[arg(long)] provider: String,
        #[arg(long)] days_out: Option<i32>,
        #[arg(long)] date_time: Option<DateTime<Utc>>,
        #[arg(long)] duration: Option<i32>,  // minutes
        #[arg(long)] reason: Option<String>,
        #[arg(long)] location: Option<String>,
    },
    /// List appointments for a patient
    List {
        patient_id: i32,
        #[arg(long)] status: Option<String>,
        #[arg(long)] from_date: Option<DateTime<Utc>>,
        #[arg(long)] to_date: Option<DateTime<Utc>>,
    },
    /// Update appointment status
    UpdateStatus {
        appointment_id: i32,
        status: String,  // CONFIRMED, CHECKED_IN, COMPLETED, CANCELLED, NO_SHOW
        #[arg(long)] notes: Option<String>,
    },
    /// Cancel appointment
    Cancel {
        appointment_id: i32,
        #[arg(long)] reason: Option<String>,
    },
    /// Reschedule appointment
    Reschedule {
        appointment_id: i32,
        #[arg(long)] new_date_time: DateTime<Utc>,
        #[arg(long)] reason: Option<String>,
    },
}

// ProblemCommand
#[derive(Subcommand, Debug, PartialEq, Clone)]
pub enum ProblemCommand {
    /// Add a new problem to patient's problem list
    Add {
        patient_id: i32,
        problem: String,
        #[arg(long)] icd10: Option<String>,
        #[arg(long)] status: Option<String>,  // ACTIVE, CHRONIC
        #[arg(long)] onset_date: Option<DateTime<Utc>>,
        #[arg(long)] severity: Option<String>,  // MILD, MODERATE, SEVERE
        #[arg(long)] notes: Option<String>,
    },
    /// Update problem status
    Update {
        patient_id: i32,
        problem: String,
        #[arg(long)] status: String,  // ACTIVE, RESOLVED, IMPROVING, WORSENING, STABLE, CHRONIC
        #[arg(long)] notes: Option<String>,
    },
    /// List all problems for a patient
    List {
        patient_id: i32,
        #[arg(long)] status: Option<String>,  // filter by status
        #[arg(long)] active_only: bool,
    },
    /// Resolve a problem
    Resolve {
        problem_id: i32,
        #[arg(long)] resolved_date: Option<DateTime<Utc>>,
        #[arg(long)] notes: Option<String>,
    },
}

// OrderCommand
#[derive(Subcommand, Debug, PartialEq, Clone)]
pub enum OrderCommand {
    /// Place admission orders
    Admit {
        encounter_id: Uuid,
        #[arg(long)] diet: Option<String>,
        #[arg(long)] activity: Option<String>,
        #[arg(long)] vitals: Option<String>,
        #[arg(long)] iv_fluids: Option<String>,
        #[arg(long)] medications: Option<String>,
        #[arg(long)] labs: Option<String>,
        #[arg(long)] imaging: Option<String>,
        #[arg(long)] notes: Option<String>,
    },
    /// Place a lab order
    Lab {
        encounter_id: Uuid,
        tests: String,
        #[arg(long)] priority: Option<String>,  // ROUTINE, URGENT, STAT
        #[arg(long)] notes: Option<String>,
    },
    /// Place an imaging order
    Imaging {
        encounter_id: Uuid,
        study: String,
        #[arg(long)] priority: Option<String>,
        #[arg(long)] indication: Option<String>,
        #[arg(long)] notes: Option<String>,
    },
    /// Place a medication order
    Medication {
        encounter_id: Uuid,
        medication: String,
        dose: String,
        frequency: String,
        #[arg(long)] route: Option<String>,
        #[arg(long)] duration: Option<String>,
        #[arg(long)] notes: Option<String>,
    },
    /// List orders for an encounter
    List {
        encounter_id: Uuid,
        #[arg(long)] order_type: Option<String>,
        #[arg(long)] status: Option<String>,
    },
    /// Update order status
    UpdateStatus {
        order_id: i32,
        status: String,  // IN_PROGRESS, COMPLETED, CANCELLED, DISCONTINUED
        #[arg(long)] notes: Option<String>,
    },
    /// Discontinue an order
    Discontinue {
        order_id: i32,
        #[arg(long)] reason: String,
    },
}

// Define the argument struct outside the enum
#[derive(Args, Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct CreatePatientArgs {
    /// Patient's first name
    #[arg(long)]
    pub first_name: Option<String>,
    
    /// Patient's last name
    #[arg(long)]
    pub last_name: Option<String>,
    
    /// Patient's full name (interprets as first and last name if provided) // <--- ADDED FIELD
    #[arg(long)]
    pub name: Option<String>, // <--- ADDED FIELD

    /// Patient's date of birth (dd-mm-yyyy format, e.g., 12-01-1975)
    #[arg(long)]
    pub dob: Option<String>,
    
    /// Patient's gender
    #[arg(long)]
    pub gender: Option<String>,
    
    // NEW: Patient's primary address (optional)
    #[arg(long)]
    pub address: Option<String>,
    
    // NEW: Patient's mobile phone number (optional)
    #[arg(long)]
    pub phone: Option<String>,
    
    /// Patient's social security number (optional)
    #[arg(long)] 
    pub ssn: Option<String>,
    
    /// Patient's medical record number (optional)
    #[arg(long)] 
    pub mrn: Option<String>,
    
    /// Batch create using JSON string with all patient fields
    #[arg(long)]
    pub batch: Option<String>,
}

#[derive(Subcommand, Debug, PartialEq, Clone)]
pub enum PatientCommand {
    // FIX: Using the separate argument struct here
    Create(CreatePatientArgs), 

    // Single-field variants correctly use positional argument by default
    View { patient_id: i32 },
    Search { query: String },
    Timeline { patient_id: i32 },
    Problems { patient_id: i32 },
    Meds { patient_id: i32 },
    CareGaps { patient_id: Option<i32> }, // None = all
    Allergies { patient_id: i32 },
    Referrals { patient_id: i32 },
    
    /// View full patient clinical journey with pathways, milestones, deviations
    Journey {
        patient_id: i32,
        // Consistency: Using #[arg(long)]
        #[arg(long)] pathway: Option<String>, // Filter by pathway name
        #[arg(long)] show_completed: bool, // Include completed pathways
        #[arg(long)] show_deviations_only: bool, // Show only deviations
        #[arg(long)] format: Option<JourneyFormat>, // text, json, timeline
    },

    /// View all active drug alerts and interactions for patient
    DrugAlerts {
        patient_id: i32,
        // Consistency: Using #[arg(long)]
        #[arg(long)] severity: Option<AlertSeverity>, // Critical, High, Medium, Low
        #[arg(long)] include_resolved: bool, // Show resolved alerts
        #[arg(long)] include_overridden: bool, // Show overridden alerts
        #[arg(long)] drug_class: Option<String>, // Filter by drug class
        #[arg(long)] format: Option<AlertFormat>, // text, json, summary
        #[arg(long)] include_inactive: bool,
        #[arg(long)] severity_filter: Option<AlertSeverity>, 
    },
}

#[derive(Debug, Clone, PartialEq, Eq, ValueEnum)]
pub enum JourneyFormat {
    Text,
    Json,
    Timeline,
    Detailed,
}

#[derive(Debug, Clone, PartialEq, Eq, ValueEnum)]
pub enum AlertFormat {
    Text,
    Json,
    Summary,
    Detailed,
}

#[derive(Subcommand, Debug, PartialEq, Clone)]
pub enum EncounterCommand {
    Start {
        patient_id: i32,
        doctor_id: i32,
        encounter_type: String, // ED, INPATIENT, OUTPATIENT, TELEHEALTH
        #[arg(long)] location: Option<String>,
    },
    Close {
        encounter_id: Uuid,
        disposition: String,
        #[arg(long)] instructions: Option<String>,
    },
}

#[derive(Subcommand, Debug, PartialEq, Clone)]
pub enum DiagnosisCommand {
    Add {
        encounter_id: Uuid,
        description: String,
        #[arg(long)] icd10: Option<String>,
    },
}

#[derive(Subcommand, Debug, PartialEq, Clone)]
pub enum PrescriptionCommand {
    Add {
        encounter_id: Uuid,
        medication_name: String,
        dose: String,
        frequency: String,
        days: i64,
        #[arg(long)] refills: Option<i32>,
    },
    CheckInteractions {
        patient_id: i32,
    },
}

#[derive(Subcommand, Debug, PartialEq, Clone)]
pub enum NoteCommand {
    Add {
        patient_id: i32,
        author_id: i32,
        text: String,
        #[arg(long)] note_type: Option<String>, // H&P, PROGRESS, CONSULT
    },
    List {
        patient_id: i32,
        #[arg(long)] since: Option<DateTime<Utc>>,
        #[arg(long)] limit: Option<usize>,
    },
}

// Referrals
#[derive(Subcommand, Debug, Clone, PartialEq)]
pub enum ReferralCommand {
    /// Create a new referral
    Create {
        #[arg(long, value_name = "UUID")]
        encounter_id: Uuid,
        #[arg(long, value_name = "DOCTOR_ID")]
        to_doctor_id: i32,
        specialty: String,                 // Cardiology, Oncology, Nephrology ...
        reason: String,
        #[arg(long, value_enum, default_value = "ROUTINE")]
        urgency: String, // ROUTINE, URGENT, STAT
    },
    /// List referrals that are still pending
    Pending {
        #[arg(long, value_name = "PATIENT")]
        patient_id: Option<i32>,
        #[arg(long, value_name = "DOCTOR")]
        doctor_id: Option<i32>,
    },
}

// =========================================================================
// LABORATORY
// =========================================================================
#[derive(Subcommand, Debug, PartialEq, Clone)]
pub enum LabCommand {
    /// Order lab tests
    Order {
        encounter_id: Uuid,
        tests: Vec<String>, // "CBC", "BMP", "Troponin", "Blood Culture"
        #[clap(long)] priority: Option<LabPriority>,
        #[clap(long)] clinical_indication: Option<String>,
        #[clap(long)] collect_time: Option<String>,
    },

    /// Receive and record lab result
    Result {
        order_id: Uuid,
        test_name: String,
        value: String,
        #[clap(long)] unit: Option<String>,
        #[clap(long)] reference_range: Option<String>,
        #[clap(long)] flag: Option<LabFlag>,
        #[clap(long)] critical: bool,
    },

    /// View lab trending
    Trend {
        patient_id: i32,
        test_name: String,
        #[clap(long)] days: Option<i64>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, ValueEnum)]
pub enum LabPriority {
    Routine,
    Urgent,
    Stat,
}

#[derive(Debug, Clone, PartialEq, Eq, ValueEnum)]
pub enum LabFlag {
    Low,
    High,
    CriticalLow,
    CriticalHigh,
    Abnormal,
    Normal,
}

#[derive(ValueEnum, Debug, PartialEq, Clone)]
/// Specifies the name matching algorithm to use in probabilistic matching.
pub enum NameMatchAlgorithm {
    #[value(name = "jaro-winkler")]
    JaroWinkler,
    #[value(name = "levenshtein")]
    Levenshtein,
    #[value(name = "both")]
    Both,
}

// FIX: Implement the Display trait to satisfy the default_value_t requirement.
impl fmt::Display for NameMatchAlgorithm {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            NameMatchAlgorithm::JaroWinkler => write!(f, "jaro-winkler"),
            NameMatchAlgorithm::Levenshtein => write!(f, "levenshtein"),
            NameMatchAlgorithm::Both => write!(f, "both"),
        }
    }
}

// Add this FromStr implementation
impl FromStr for NameMatchAlgorithm {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "jaro-winkler" => Ok(NameMatchAlgorithm::JaroWinkler),
            "levenshtein" => Ok(NameMatchAlgorithm::Levenshtein),
            "both" => Ok(NameMatchAlgorithm::Both),
            _ => Err(format!(
                "Invalid name matching algorithm: '{}'. Valid options are: jaro-winkler, levenshtein, both",
                s
            )),
        }
    }
}

// --- MASTER PATIENT INDEX COMMANDS ---
#[derive(Subcommand, Debug, PartialEq, Clone,)]
/// Commands for Master Patient Index (MPI) operations, including matching, linking, and merging patient identities.
pub enum MPICommand {
    /// Indexes a new patient record into the MPI for future matching and retrieval.
    /// Requires either --name OR the combination of --first-name and --last-name.
    Index {
        /// Patient's full name (e.g., "Alex Johnson"). Conflicts with --first-name and --last_name.
        #[arg(long, conflicts_with_all = ["first_name", "last_name"])]
        name: Option<String>,

        /// Patient's first name. Requires --last-name if --name is not provided.
        #[arg(long)]
        first_name: Option<String>,

        /// Patient's last name. Requires --first-name if --name is not provided.
        #[arg(long)]
        last_name: Option<String>,

        /// Optional: Patient's date of birth (YYYY-MM-DD). Accepts --dob or --date_of_birth interchangeably.
        #[arg(long, aliases = ["date_of_birth"])]
        dob: Option<String>,

        /// Mandatory: Patient's Medical Record Number (MRN). Used as the source identifier.
        #[arg(long, aliases = ["source-mrn"])]
        mrn: String,
        
        /// Optional: Patient's address (street/city/zip).
        #[arg(long)]
        address: Option<String>,
        
        /// Optional: Patient's phone number.
        #[arg(long)]
        phone: Option<String>,

        // --- MISSING FLAGS ADDED HERE ---
        /// Mandatory: The system submitting this patient record (e.g., "SourceSystemA").
        #[arg(long)]
        system: String,

        /// Optional: Patient's gender (M, F, O, U).
        #[arg(long)]
        gender: Option<String>,
        // --- END ADDED FLAGS ---
    },
    
    /// Runs probabilistic matching logic to find potential duplicate patient records.
    ///
    /// The accuracy of the match improves with more provided input fields (DOB, Address, Phone).
    Match {
        /// Patient's full name (MANDATORY: required for any probabilistic match)
        #[arg(long)]
        name: Option<String>, // Changed to Option<String> for flexibility, though logic may enforce it.

        // --- FLAGS UPDATED/ADDED HERE ---
        /// Mandatory: The MRN of the master patient identity to match against (Target).
        #[arg(long)]
        target_mrn: String,

        /// Mandatory: The MRN of the patient candidate being evaluated (Candidate).
        #[arg(long)]
        candidate_mrn: String,

        /// Mandatory: The probabilistic match score calculated between the two identities (0.0 to 1.0).
        #[arg(long)]
        score: f64,

        /// Mandatory: The action to take based on the match (e.g., "link", "no-link").
        #[arg(long)]
        action: String,
        // --- END ADDED/UPDATED FLAGS ---

        /// Optional: Patient's date of birth (YYYY-MM-DD). If provided, significantly increases score weight.
        #[arg(long)]
        dob: Option<String>,
        
        /// Optional: Patient's address (street/city/zip). Used for geographic blocking and scoring.
        #[arg(long)]
        address: Option<String>,
        
        /// Optional: Patient's phone number. Used for high match certainty blocking.
        #[arg(long)]
        phone: Option<String>,

        /// Optional: Specify the primary name matching algorithm to use for scoring.
        /// Defaults to 'jaro-winkler', which is generally better for names with minor typos.
        #[arg(long, default_value_t = NameMatchAlgorithm::JaroWinkler)]
        name_algo: NameMatchAlgorithm,
    },

    /// Explicitly links an external identifier (e.g., foreign MRN) to an existing master MPI record.
    Link {
        /// The canonical MPI ID to link to
        #[arg(long)]
        master_id: String,
        /// The external ID value (e.g., "98765")
        #[arg(long)]
        external_id: String,
        /// The type of external ID (e.g., "ForeignMRN", "SSN", "Passport")
        #[arg(long)]
        id_type: String,
    },

    /// Merges a duplicate patient record (source-id) into the master record (target-id).
    Merge {
        // --- FLAGS UPDATED/ADDED HERE ---
        /// The MRN of the master record (the identity that remains active).
        #[arg(long)]
        master_mrn: String,
        /// The MRN of the duplicate record to be consolidated and retired.
        #[arg(long)]
        duplicate_mrn: String,
        /// The ID of the user performing the manual merge operation (required for audit).
        #[arg(long)]
        user_id: String,
        /// The reason for the manual merge (required for audit).
        #[arg(long)]
        reason: String,
        // The previous source_id/target_id flags are replaced by the MRNs for clarity.
        // --- END ADDED FLAGS ---
        
        /// Policy for data resolution (e.g., "Latest", "SourceWins", "TargetWins")
        #[arg(long)]
        resolution_policy: String,
    },

    /// Retrieves the immutable audit log for all identity changes (links, merges, demographics) for an MPI record.
    Audit {
        /// The MPI ID to audit
        #[arg(long)]
        mpi_id: String,
        /// Optional: Timeframe to filter the audit log (e.g., "30d", "2024-01-01..2024-06-30")
        #[arg(long)]
        timeframe: Option<String>,
    },

    /// Reverses an erroneous merge operation, creating a new patient record and maintaining an audit trail.
    Split {
        /// The ID of the target record that was previously merged into (the one to split from).
        #[arg(long)]
        merged_id: String,
        
        /// The complete data for the new Patient record being split out (provided as a JSON string).
        #[arg(long)]
        new_patient_data_json: String,
        
        /// The reason for the identity split (required for audit).
        #[arg(long)]
        reason: String,
        
        // FIX: Add the new mandatory internal ID of the record being split out.
        /// Mandatory: The internal ID (i32) of the patient record that is being split out.
        #[arg(long)]
        split_patient_id_str: String, 
    },

    /// Retrieves the consolidated "Golden Record" (single source of truth) for a given patient.
    GetGoldenRecord {
        /// The canonical MPI ID of the patient to retrieve.
        #[arg(long)]
        patient_id: String,
    },
    
    // --- ADDED COMMAND: FETCH IDENTITY (structurally identical to Resolve) ---
    /// Alias for Resolve: Retrieves the Golden Record using a known source identifier (MRN, SSN, etc.).
    FetchIdentity {
        /// The internal MPI ID or the primary external ID (MRN, SSN) to resolve.
        #[arg(long, conflicts_with = "external_id")]
        id: Option<String>,
        
        /// An external ID value (e.g., "98765") that is not the primary ID. Must be used with --id_type.
        #[arg(long, conflicts_with = "id")]
        external_id: Option<String>,
        
        /// The type of ID being provided (e.g., "MRN", "SSN", "InternalID", "ForeignMRN").
        /// Required if using --external-id.
        #[arg(long)]
        id_type: Option<String>,

        // --- MISSING FLAG ADDED HERE ---
        /// Mandatory: The primary source MRN for the identity to fetch.
        #[arg(long, aliases = ["source-mrn"], conflicts_with_all = ["id", "external_id", "id_type"])]
        source_mrn: Option<String>,
        // --- END ADDED FLAG ---
    },
    
    // --- UPDATED COMMAND: RESOLVE (adding the source_mrn and system flags for completeness) ---
    /// Resolves any patient identifier (MRN, old ID, SSN, etc.) to the canonical, active MPI record.
    Resolve {
        /// The internal MPI ID or the primary external ID (MRN, SSN) to resolve.
        #[arg(long, conflicts_with = "external_id")]
        id: Option<String>,
        
        /// An external ID value (e.g., "98765") that is not the primary ID. Must be used with --id_type.
        #[arg(long, conflicts_with = "id")]
        external_id: Option<String>,
        
        /// The type of ID being provided (e.g., "MRN", "SSN", "InternalID", "ForeignMRN").
        /// Required if using --external-id.
        #[arg(long)]
        id_type: Option<String>,

        // --- SOURCE_MRN FLAG (Logic for conflicts_with_all removed for flexibility) ---
        /// The primary source MRN for the identity to resolve, if different from --id/--external-id.
        #[arg(long, aliases = ["source-mrn"])]
        source_mrn: Option<String>,
        // --- END SOURCE_MRN FLAG ---

        // --- MISSING SYSTEM FLAG ADDED HERE (To satisfy handle_mpi_resolve 5th argument) ---
        /// The system or source that the external ID originates from (e.g., "Epic", "Cerner").
        /// Required if using --external-id.
        #[arg(long)]
        system: Option<String>,
        // --- END MISSING FLAG ---
    },

    /// Performs an indexed, fast search for patient records based on key demographics.
    Search {
        /// Optional: Patient's full name (e.g., "Alex Johnson"). Conflicts with --first-name and --last-name.
        #[arg(long, conflicts_with_all = ["first_name", "last_name"])]
        name: Option<String>,

        /// Optional: Patient's first name.
        #[arg(long)]
        first_name: Option<String>,

        /// Optional: Patient's last name.
        #[arg(long)]
        last_name: Option<String>,

        /// Optional: Patient's date of birth (YYYY-MM-DD).
        #[arg(long)]
        dob: Option<String>,

        /// Optional: Patient's address (street/city/zip).
        #[arg(long)]
        address: Option<String>,
        
        /// Optional: Patient's phone number.
        #[arg(long)]
        phone: Option<String>,
    },
    /// Industry-Standard Lineage: Visualizes the evolution and survivorship of a Golden Record.
    /// Shows how various IDs merged, linked, and contributed to the current state.
    /// Industry-Standard Lineage: Visualizes the evolution and survivorship of a Golden Record.
    Lineage {
        /// Filter by Golden ID, MRN, or External ID.
        #[arg(long)]
        id: String,
        #[arg(long)]
        id_type: Option<String>,
        /// Time window filtering for the evolution chain.
        #[arg(long)]
        from: Option<String>, 
        #[arg(long)]
        to: Option<String>,
        /// Slice the evolution history.
        #[arg(long)]
        head: Option<usize>,
        #[arg(long)]
        tail: Option<usize>,
        #[arg(long, default_value_t = 3)]
        depth: u32,
        #[arg(long)]
        include_flags: bool,
    },

    /// MPI Snapshot: Fetch a complete point-in-time state of a record or the entire index.
    Snapshot {
        #[arg(long)]
        id: Option<String>,
        /// Export as 'json' or 'fhir' (Industry standard for patient data exchange).
        #[arg(long, default_value = "json")]
        format: String,
        /// Optional: specific timestamp to see what the record looked like then.
        #[arg(long)]
        as_of: Option<String>,
    },

    /// Comprehensive Dashboard/Status
    Status {
        #[arg(long)]
        only_conflicts: bool,
        #[arg(long)]
        limit: Option<usize>,
        /// "top" (newest) or "bottom" (oldest).
        #[arg(long, default_value = "top")]
        slice: String,
        #[arg(long)]
        system: Option<String>,
    },
    /// Reverses an erroneous merge operation by discovering the merge event via Patient ID, MRN, or Event UUID.
    /// This restores clinical edges to the source and re-establishes an independent Golden Record.
    Rollback {
        /// Discovery: The UUID of the specific MANUAL_MERGE_INITIATED event to roll back.
        #[arg(long, group = "discovery")]
        event_uuid: Option<String>,

        /// Discovery: The UUID of the patient who was the SOURCE of a merge. 
        /// The system will find the latest merge event for this patient.
        #[arg(long, group = "discovery")]
        id: Option<String>,

        /// Discovery: The MRN of the patient who was the SOURCE of a merge.
        /// The system will find the patient and then their latest merge event.
        #[arg(long, group = "discovery")]
        mrn: Option<String>,

        /// Mandatory: The user or system requesting the rollback (required for 2025-12-20 audit compliance).
        #[arg(long)]
        requested_by: String,

        /// Optional: A detailed reason for the rollback to be logged in the graph of changes.
        #[arg(long)]
        reason: Option<String>,
    },
}

// =========================================================================
// IMAGING
// =========================================================================
#[derive(Subcommand, Debug, PartialEq, Clone)]
pub enum ImagingCommand {
    /// Order imaging study
    Order {
        encounter_id: Uuid,
        study: String, // "CT Chest with Contrast", "MRI Brain"
        #[clap(long)] priority: Option<ImagingPriority>,
        #[clap(long)] indication: Option<String>,
        #[clap(long)] contrast: Option<bool>,
        #[clap(long)] modality: Option<String>,
    },

    /// Record preliminary read
    Preliminary {
        study_id: Uuid,
        finding: String,
        #[clap(long)] impression: Option<String>,
        #[clap(long)] radiologist: Option<i32>,
    },

    /// Record final report
    Final {
        study_id: Uuid,
        report: String,
        #[clap(long)] critical_finding: Option<bool>,
        #[clap(long)] radiologist: Option<i32>,
    },

    /// Compare with prior
    Compare {
        patient_id: i32,
        study_type: String,
        #[clap(long)] prior_date: Option<String>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, ValueEnum)]
pub enum ImagingPriority {
    Routine,
    Urgent,
    Stat,
}

// =========================================================================
// CHEMOTHERAPY
// =========================================================================
#[derive(Subcommand, Debug, PartialEq, Clone)]
pub enum ChemoCommand {
    /// Create chemotherapy regimen
    Regimen {
        patient_id: i32,
        regimen_name: String, // "FOLFOX", "R-CHOP"
        #[clap(long)] diagnosis: String,
        #[clap(long)] intent: Option<ChemoIntent>,
        #[clap(long)] start_date: Option<String>,
    },

    /// Add cycle
    CycleAdd {
        regimen_id: Uuid,
        cycle_number: i32,
        #[clap(long)] planned_date: String,
        #[clap(long)] pre_meds: Vec<String>,
    },

    /// Verify pre-chemo labs
    LabsVerify {
        regimen_id: Uuid,
        cycle_number: i32,
        #[clap(long)] required_labs: Vec<String>,
    },

    /// Modify cycle (hold, dose reduce)
    CycleModify {
        cycle_id: Uuid,
        #[clap(long)] action: ChemoAction,
        #[clap(long)] reason: Option<String>,
        #[clap(long)] dose_reduction_pct: Option<i32>,
    },

    /// Document toxicity
    Toxicity {
        patient_id: i32,
        cycle_id: Option<Uuid>,
        term: String,
        grade: i32,
        #[clap(long)] management: Option<String>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, ValueEnum)]
pub enum ChemoIntent {
    Curative,
    Adjuvant,
    Neoadjuvant,
    Palliative,
}

#[derive(Debug, Clone, PartialEq, Eq, ValueEnum)]
pub enum ChemoAction {
    Hold,
    Delay,
    Reduce,
    Discontinue,
}

// =========================================================================
// RADIATION THERAPY
// =========================================================================
#[derive(Subcommand, Debug, PartialEq, Clone)]
pub enum RadiationCommand {
    /// Create radiation plan
    Plan {
        patient_id: i32,
        site: String,
        technique: String, // "IMRT", "SBRT", "3D-CRT"
        total_dose_gy: f32,
        fractions: i32,
        #[clap(long)] start_date: Option<String>,
    },

    /// Daily treatment
    Treatment {
        plan_id: Uuid,
        fraction: i32,
        dose_delivered: f32,
        #[clap(long)] image_guidance: Option<String>,
        #[clap(long)] toxicity: Option<String>,
    },

    /// Assess cumulative toxicity
    Toxicity {
        patient_id: i32,
        #[clap(long)] grade: i32,
        #[clap(long)] organ: String,
        #[clap(long)] management: Option<String>,
    },
}

// =========================================================================
// SURGERY
// =========================================================================
#[derive(Subcommand, Debug, PartialEq, Clone)]
pub enum SurgeryCommand {
    /// Create surgical case
    CaseCreate {
        patient_id: i32,
        procedure: String,
        surgeon_id: i32,
        #[clap(long)] date: String,
        #[clap(long)] time: Option<String>,
        #[clap(long)] location: Option<String>,
    },

    /// Pre-operative checklist
    PreopChecklist {
        case_id: Uuid,
        #[clap(long)] consent: bool,
        #[clap(long)] h_and_p: bool,
        #[clap(long)] npo: bool,
        #[clap(long)] antibiotics_given: Option<String>,
    },

    /// Start case
    CaseStart {
        case_id: Uuid,
        #[clap(long)] anesthesia_start: Option<String>,
        #[clap(long)] incision_time: Option<String>,
    },

    /// Document intraoperative event
    EventAdd {
        case_id: Uuid,
        time: String,
        event: String,
    },

    /// Close case
    CaseClose {
        case_id: Uuid,
        #[clap(long)] ebl_ml: Option<i32>,
        #[clap(long)] complications: Option<String>,
        #[clap(long)] specimens: Option<String>,
    },
}

// =========================================================================
// OBSERVATION & VITALS
// =========================================================================
#[derive(Subcommand, Debug, PartialEq, Clone)]
pub enum ObservationCommand {
    /// Record a clinical observation (pain, consciousness, etc.)
    Add {
        encounter_id: Uuid,
        observation_type: String, // "PAIN", "CONSCIOUSNESS", "MOOD"
        value: String,
        #[clap(long)] unit: Option<String>,
        #[clap(long)] observed_by: Option<i32>,
        #[clap(long)] notes: Option<String>,
    },

    /// List observations for encounter
    List {
        encounter_id: Uuid,
        #[clap(long)] type_filter: Option<String>,
        #[clap(long)] limit: Option<usize>,
    },
}

#[derive(Subcommand, Debug, PartialEq, Clone)]
pub enum VitalsCommand {
    /// Add vital signs for an encounter
    Add {
        encounter_id: Uuid,
        #[arg(long)] bp: Option<String>,           // "120/80" format
        #[arg(long)] hr: Option<i32>,              // heart rate
        #[arg(long)] rr: Option<i32>,              // respiratory rate
        #[arg(long)] temp: Option<f32>,            // temperature
        #[arg(long)] spo2: Option<i32>,            // oxygen saturation
        #[arg(long)] pain_score: Option<i32>,      // 0-10 pain scale
        #[arg(long)] weight: Option<f32>,          // weight in kg
        #[arg(long)] height: Option<f32>,          // height in cm
    },
    /// View vital signs for a patient
    View {
        patient_id: i32,
        #[arg(long)] limit: Option<usize>,
        #[arg(long)] since: Option<DateTime<Utc>>,
    },
    /// Get trending data for specific vital
    Trending {
        patient_id: i32,
        vital_type: String,  // bp, hr, temp, spo2, etc.
        #[arg(long)] days_back: Option<i32>,
    },
}

#[derive(Subcommand, Debug, PartialEq, Clone)]
pub enum TriageCommand {
    Assess {
        encounter_id: Uuid,
        nurse_id: i32,
        level: String, // ESI 1-5
        chief_complaint: String,
        #[arg(long)] symptoms: Option<String>,
        #[arg(long)] pain_score: Option<i32>,
        #[arg(long)] notes: Option<String>,
    },
}

#[derive(Subcommand, Debug, PartialEq, Clone)]
pub enum DispositionCommand {
    Set {
        encounter_id: Uuid,
        disposition_type: String, // DISCHARGED, ADMITTED, TRANSFER, EXPIRED
        #[arg(long)] admitting_service: Option<String>,
        #[arg(long)] admitting_doctor: Option<i32>,
        #[arg(long)] transfer_facility: Option<i32>,
        #[arg(long)] instructions: Option<String>,
    },
}

#[derive(Subcommand, Debug, PartialEq, Clone)]
pub enum DrugCommand {
    Check {
        patient_id: i32,
    },
    AllergyCheck {
        patient_id: i32,
    },
}

#[derive(Subcommand, Debug, PartialEq, Clone)]
pub enum AllergyCommand {
    /// Add a new allergy for a patient
    Add {
        patient_id: i32,
        allergen: String,
        #[arg(long)] reaction: Option<String>,
        #[arg(long)] severity: String,  // MILD, MODERATE, SEVERE, LIFE_THREATENING
        #[arg(long)] verified_by: Option<String>,
        #[arg(long)] notes: Option<String>,
    },
    /// List all allergies for a patient
    List {
        patient_id: i32,
        #[arg(long)] status: Option<String>,  // ACTIVE, INACTIVE, RESOLVED
    },
    /// Update allergy status
    UpdateStatus {
        allergy_id: i32,
        status: String,  // ACTIVE, INACTIVE, RESOLVED
        #[arg(long)] notes: Option<String>,
    },
    /// Remove/inactivate an allergy
    Remove {
        allergy_id: i32,
        #[arg(long)] reason: Option<String>,
    },
    /// Check for allergies before prescribing
    Check {
        patient_id: i32,
        #[arg(long)] medication: Option<String>,
    },
}

// Population Health
#[derive(Subcommand, Debug, Clone, PartialEq)]
pub enum PopulationCommand {
    /// Patients overdue for a screening test
    ScreeningDue {
        screening_type: String, // COLONOSCOPY, MAMMOGRAM, A1C, CERVICAL, LDL
        #[arg(long, value_name = "AGE")]
        age_min: Option<u32>,
        #[arg(long, value_name = "AGE")]
        age_max: Option<u32>,
        #[arg(long, value_name = "MONTHS")]
        months_overdue: Option<u32>,
    },
    /// Patients on high-risk medication combinations
    HighRiskMeds,
    /// Patients with uncontrolled chronic conditions
    ChronicConditions {
        condition: String, // diabetes, hypertension, copd, asthma, ckd
        #[arg(long, value_name = "THRESHOLD")]
        uncontrolled_threshold: Option<f64>,
    },
    /// Social-determinants screening
    SocialDeterminants {
        #[arg(long, value_name = "DOMAIN", value_enum)]
        domains: Vec<String>, // housing, food, transport, safety
    },
    /// Quality-measure performance
    QualityMeasures {
        measure_type: String, // MIPS_2025, HEDIS_2025, CMS_STAR
        #[arg(long, value_name = "PERIOD")]
        time_period: Option<String>,
    },
    /// Population risk stratification
    RiskStratification {
        #[arg(long, value_name = "FACTOR")]
        risk_factors: Vec<String>, // readmission, fall, sepsis, stroke
        #[arg(long, value_name = "LEVEL")]
        risk_level: Option<String>, // LOW, MEDIUM, HIGH
    },
    /// Patients at high risk for 30-day readmission
    ReadmissionRisk {
        #[arg(long, value_name = "DAYS")]
        days: Option<i32>, // default 30
        #[arg(long)]
        preventable_only: bool,
        #[arg(long)]
        high_risk_only: bool,
    },
    /// Today's active drug-drug interaction and allergy alerts
    DrugAlertsToday,
    /// One-row summary for executives
    CareGapsSummary,
}

#[derive(Subcommand, Debug, Clone, PartialEq)]
pub enum AnalyticsCommand {
    /// Population health & epidemiology
    #[clap(subcommand)]
    Population(AnalyticsPopulationCommand),

    /// Clinical quality & outcomes
    #[clap(subcommand)]
    Quality(AnalyticsQualityCommand),

    /// Utilization & efficiency
    #[clap(subcommand)]
    Utilization(AnalyticsUtilizationCommand),

    /// Risk stratification & predictive
    #[clap(subcommand)]
    Risk(AnalyticsRiskCommand),

    /// Care pathway & protocol adherence
    #[clap(subcommand)]
    Pathway(AnalyticsPathwayCommand),

    /// Social determinants & equity
    #[clap(subcommand)]
    Equity(AnalyticsEquityCommand),

    /// Custom cohort analytics
    Cohort {
        query: String,
        #[clap(long)] name: Option<String>,
        #[clap(long)] save: bool,
    },
}

// =========================================================================
// 1. POPULATION HEALTH & EPIDEMIOLOGY
// =========================================================================
#[derive(Subcommand, Debug, Clone, PartialEq)]
pub enum AnalyticsPopulationCommand {
    /// Disease prevalence by demographics
    Prevalence {
        diagnosis: String,
        #[clap(long)] age_group: Option<String>,
        #[clap(long)] gender: Option<String>,
        #[clap(long)] race: Option<String>,
    },

    /// Incidence rate (new cases)
    Incidence {
        diagnosis: String,
        #[clap(long)] timeframe: Option<String>, // 30d, 90d, 1y
    },

    /// Comorbidity analysis
    Comorbidity {
        primary_diagnosis: String,
        #[clap(long)] top_n: Option<usize>,
    },

    /// Seasonal patterns
    Seasonal {
        diagnosis: String,
        #[clap(long)] years: Option<usize>,
    },

    /// Outbreak detection
    Outbreak {
        #[clap(long)] pathogen: Option<String>,
        #[clap(long)] threshold: Option<f64>,
    },
}

// =========================================================================
// 2. CLINICAL QUALITY & OUTCOMES
// =========================================================================
#[derive(Subcommand, Debug, Clone, PartialEq)]
pub enum AnalyticsQualityCommand {
    /// Mortality index (observed vs expected)
    MortalityIndex {
        #[clap(long)] service_line: Option<String>,
    },

    /// Length of stay analysis
    Los {
        #[clap(long)] diagnosis: Option<String>,
        #[clap(long)] benchmark: Option<String>, // national, peer
    },

    /// Readmission analysis
    Readmission {
        #[clap(long)] days: Option<i32>, // 7, 30, 90
        #[clap(long)] preventable_only: bool,
    },

    /// Complication rates
    Complications {
        procedure: String,
        #[clap(long)] risk_adjusted: bool,
    },

    /// Patient experience scores
    Experience {
        #[clap(long)] domain: Option<String>, // communication, pain, quietness
    },
}

// =========================================================================
// 3. UTILIZATION & EFFICIENCY
// =========================================================================
#[derive(Subcommand, Debug, Clone, PartialEq)]
pub enum AnalyticsUtilizationCommand {
    /// ED utilization patterns
    EdUtilization {
        #[clap(long)] chief_complaint: Option<String>,
        #[clap(long)] time_of_day: bool,
    },

    /// Bed turnover rate
    BedTurnover {
        #[clap(long)] unit: Option<String>,
    },

    /// OR case volume & efficiency
    OrEfficiency {
        #[clap(long)] surgeon: Option<i32>,
        #[clap(long)] procedure_type: Option<String>,
    },

    /// Diagnostic test utilization
    TestUtilization {
        test_name: String,
        #[clap(long)] ordering_provider: Option<i32>,
    },

    /// Medication utilization
    MedicationUtilization {
        medication: String,
        #[clap(long)] therapeutic_class: Option<String>,
    },
}

// =========================================================================
// 4. RISK STRATIFICATION & PREDICTIVE
// =========================================================================
#[derive(Subcommand, Debug, Clone, PartialEq)]
pub enum AnalyticsRiskCommand {
    /// LACE+ readmission risk distribution
    LacePlus,

    /// Hospital-acquired condition risk
    HacRisk {
        #[clap(long)] condition: Option<String>, // fall, pressure ulcer, etc.
    },

    /// Deterioration index (modified early warning score)
    DeteriorationIndex {
        #[clap(long)] unit: Option<String>,
    },

    /// Sepsis risk trajectory
    SepsisTrajectory {
        patient_id: i32,
        #[clap(long)] hours: Option<i32>,
    },

    /// Frailty index
    Frailty {
        #[clap(long)] age_min: Option<u32>,
    },
}

// =========================================================================
// 5. CARE PATHWAY & PROTOCOL ADHERENCE
// =========================================================================
#[derive(Subcommand, Debug, Clone, PartialEq)]
pub enum AnalyticsPathwayCommand {
    /// Pathway compliance rate
    Compliance {
        pathway: String, // "Stroke", "Sepsis-6", "Postoperative"
        #[clap(long)] milestone: Option<String>,
    },

    /// Time to milestone achievement
    MilestoneTiming {
        pathway: String,
        milestone: String,
    },

    /// Deviation analysis
    Deviations {
        pathway: String,
        #[clap(long)] reason_filter: Option<String>,
    },

    /// Pathway outcomes comparison
    Outcomes {
        pathway: String,
        #[clap(long)] variant: Option<String>,
    },
}

// =========================================================================
// 6. SOCIAL DETERMINANTS & HEALTH EQUITY
// =========================================================================
#[derive(Subcommand, Debug, Clone, PartialEq)]
pub enum AnalyticsEquityCommand {
    /// SDOH prevalence
    SdohPrevalence {
        #[clap(long)] domain: Option<String>, // housing, food, transportation
    },

    /// Outcomes by SDOH status
    OutcomesBySdoh {
        diagnosis: String,
        sdoh_factor: String,
    },

    /// Access disparities
    AccessDisparities {
        #[clap(long)] metric: Option<String>, // ED visits, preventive screening
        #[clap(long)] demographic: Option<String>,
    },

    /// Language barrier impact
    LanguageImpact {
        #[clap(long)] preferred_language: Option<String>,
    },
}

#[derive(Subcommand, Debug, Clone, PartialEq)]
pub enum MetricsCommand {
    /// Real-time clinical quality metrics
    #[clap(subcommand)]
    Clinical(MetricsClinicalCommand),

    /// Operational & throughput metrics
    #[clap(subcommand)]
    Operational(MetricsOperationalCommand),

    /// Financial & revenue cycle metrics
    #[clap(subcommand)]
    Financial(MetricsFinancialCommand),

    /// Safety & adverse event metrics
    #[clap(subcommand)]
    Safety(MetricsSafetyCommand),

    /// AI model performance metrics
    #[clap(subcommand)]
    Ml(MetricsMlCommand),

    /// Custom dashboard — combine any metrics
    Dashboard {
        #[clap(long)] name: Option<String>,
        #[clap(long)] save: bool,
    },
}

// =========================================================================
// 1. CLINICAL QUALITY METRICS
// =========================================================================
#[derive(Subcommand, Debug, Clone, PartialEq)]
pub enum MetricsClinicalCommand {
    /// Sepsis bundle compliance (SEP-1)
    SepsisBundle {
        #[clap(long)] facility: Option<String>,
        #[clap(long)] timeframe: Option<String>, // today, 7d, 30d
    },

    /// Door-to-balloon time for STEMI
    DoorToBalloon {
        #[clap(long)] target_minutes: Option<i32>, // default 90
    },

    /// Stroke pathway compliance (tPA <60min)
    StrokeTpa {
        #[clap(long)] target_minutes: Option<i32>, // default 60
    },

    /// Antibiotic stewardship — time to appropriate therapy
    AntibioticTiming,

    /// Glycemic control — severe hypo/hyperglycemia rates
    GlycemicControl,

    /// VTE prophylaxis compliance
    VteProphylaxis,

    /// Pain reassessment within 1 hour
    PainReassessment,

    /// Fall rate per 1000 patient days
    FallRate,

    /// Pressure injury prevalence
    PressureInjury,

    /// 30-day readmission rate
    ReadmissionRate {
        #[clap(long)] condition: Option<String>,
    },
}

// =========================================================================
// 2. OPERATIONAL & THROUGHPUT METRICS
// =========================================================================
#[derive(Subcommand, Debug, Clone, PartialEq)]
pub enum MetricsOperationalCommand {
    /// ED throughput metrics
    EdThroughput {
        #[clap(long)] metric: Option<EdMetric>,
    },

    /// OR utilization and efficiency
    OrUtilization,

    /// Bed occupancy and turnover
    BedOccupancy {
        #[clap(long)] unit: Option<String>,
        #[clap(long)] predict_hours: Option<i32>,
    },

    /// Patient flow — LWBS, AMA, elopement
    PatientFlow,

    /// Staff productivity
    StaffProductivity {
        #[clap(long)] role: Option<String>, // doctor, nurse, tech
    },

    /// Appointment no-show rate
    NoShowRate,

    /// Telehealth utilization
    TelehealthUtilization,
}

#[derive(Debug, Clone, PartialEq, Eq, ValueEnum)]
pub enum EdMetric {
    DoorToProvider,
    DoorToDisposition,
    Los, // Length of stay
    Lwbs, // Left without being seen
    BoardingTime,
}

// =========================================================================
// 3. FINANCIAL METRICS
// =========================================================================
#[derive(Subcommand, Debug, Clone, PartialEq)]
pub enum MetricsFinancialCommand {
    /// Revenue per encounter by service line
    RevenuePerEncounter {
        #[clap(long)] service: Option<String>,
    },

    /// Cost per case
    CostPerCase,

    /// Denial rate and reasons
    DenialRate,

    /// Payer mix
    PayerMix,

    /// Point-of-service collections
    PosCollections,

    /// Charge lag days
    ChargeLag,

    /// AR days outstanding
    ArDays,
}

// =========================================================================
// 4. SAFETY & ADVERSE EVENT METRICS
// =========================================================================
#[derive(Subcommand, Debug, Clone, PartialEq)]
pub enum MetricsSafetyCommand {
    /// Medication errors
    MedicationErrors {
        #[clap(long)] type_filter: Option<String>, // wrong-dose, wrong-drug
    },

    /// Adverse drug events
    AdverseDrugEvents,

    /// Hospital-acquired infections
    Hai {
        #[clap(long)] infection_type: Option<String>, // CLABSI, CAUTI, SSI
    },

    /// Sentinel events
    SentinelEvents,

    /// Near misses
    NearMisses,

    /// Patient satisfaction (HCAHPS)
    PatientSatisfaction,
}

// =========================================================================
// 5. AI/ML MODEL METRICS
// =========================================================================
#[derive(Subcommand, Debug, Clone, PartialEq)]
pub enum MetricsMlCommand {
    /// Model performance dashboard
    Performance {
        model_name: String,
    },

    /// Alert fatigue analysis
    AlertFatigue {
        #[clap(long)] provider_id: Option<i32>,
    },

    /// Override rate by model
    OverrideRate,

    /// Positive predictive value over time
    PpvTrend {
        model_name: String,
    },

    /// Model drift detection
    Drift {
        model_name: String,
    },
}

#[derive(Subcommand, Debug, Clone, PartialEq)]
pub enum AuditCommand {
    Patient {
        patient_id: i32,
        #[arg(long)] from: DateTime<Utc>,
        #[arg(long)] to: Option<DateTime<Utc>>,
    },
    ControlledSubstances {
        #[arg(long)] from: DateTime<Utc>,
        #[arg(long)] to: Option<DateTime<Utc>>,
    },
}

#[derive(Subcommand, Debug, PartialEq, Clone)]
pub enum ExportCommand {
    Patient {
        patient_id: i32,
        #[arg(long)] format: String, // json, fhir, hl7, cda
        #[arg(long)] deidentify: bool,
    },
    Cohort {
        query: String,
        #[arg(long)] format: String,
    },
}

// =========================================================================
// RESEARCH & AI
// =========================================================================
#[derive(Subcommand, Debug, PartialEq, Clone)]
pub enum ResearchCommand {
    Cohort { query: String },
    Export { cohort_id: String, #[clap(long)] format: String, #[clap(long)] deidentify: bool },
}

#[derive(Subcommand, Debug, PartialEq, Clone)]
pub enum MlCommand {
    Predict { model: String, patient_id: i32 },
    Train { name: String, dataset: String },
}

#[derive(Subcommand, Debug, PartialEq, Clone)]
pub enum ClinicalTrialCommand {
    /// List all active trials
    List {
        #[clap(long)] status: Option<TrialStatus>,
        #[clap(long)] sponsor: Option<String>,
        #[clap(long)] phase: Option<TrialPhase>,
    },

    /// Show detailed trial information
    Show {
        trial_id: String, // NCT ID or internal ID
        #[clap(long)] include_sites: bool,
        #[clap(long)] include_arms: bool,
    },

    /// Screen patients for trial eligibility
    Screen {
        trial_id: String,
        #[clap(long)] patient_id: Option<i32>,
        #[clap(long)] cohort_query: Option<String>,
        #[clap(long)] dry_run: bool,
    },

    /// Enroll patient in trial
    Enroll {
        trial_id: String,
        patient_id: i32,
        #[clap(long)] consent_date: Option<String>,
        #[clap(long)] arm: Option<String>,
        #[clap(long)] site_id: Option<String>,
    },

    /// Randomize patient (when applicable)
    Randomize {
        trial_id: String,
        patient_id: i32,
        #[clap(long)] stratification_factors: Vec<String>,
    },

    /// Record trial visit
    Visit {
        trial_id: String,
        patient_id: i32,
        visit_name: String,
        #[clap(long)] scheduled_date: Option<String>,
        #[clap(long)] actual_date: Option<String>,
        #[clap(long)] status: Option<VisitStatus>,
    },

    /// Record adverse event
    AdverseEvent {
        trial_id: String,
        patient_id: i32,
        #[clap(long)] term: String,
        #[clap(long)] grade: Option<i32>,
        #[clap(long)] seriousness: Option<Seriousness>,
        #[clap(long)] expected: bool,
        #[clap(long)] onset_date: String,
    },

    /// Record protocol deviation
    Deviation {
        trial_id: String,
        patient_id: i32,
        description: String,
        #[clap(long)] category: Option<DeviationCategory>,
        #[clap(long)] impact: Option<String>,
    },

    /// End trial participation
    EndParticipation {
        trial_id: String,
        patient_id: i32,
        #[clap(long)] reason: String,
        #[clap(long)] date: Option<String>,
    },

    /// Generate trial reports
    Report {
        trial_id: String,
        #[clap(subcommand)]
        report_type: TrialReportType,
    },

    /// Export trial data
    Export {
        trial_id: String,
        #[clap(long)] format: ExportFormat,
        #[clap(long)] include_sae: bool,
        #[clap(long)] deidentify: bool,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, ValueEnum)] 
pub enum TrialStatus {
    Recruiting,
    Active,
    Completed,
    Terminated,
    Withdrawn,
}

#[derive(Debug, Clone, PartialEq, Eq, ValueEnum)] 
pub enum TrialPhase {
    Phase1,
    Phase2,
    Phase3,
    Phase4,
}

#[derive(Subcommand, Debug, PartialEq, Clone)]
pub enum TrialReportType {
    Enrollment,
    Safety,
    Demographics,
    Efficacy,
    Compliance,
    DataQuality,
}

#[derive(Debug, Clone, PartialEq, Eq, ValueEnum)]
pub enum VisitStatus {
    Scheduled,
    Completed,
    Missed,
    Cancelled,
}

#[derive(Debug, Clone, PartialEq, Eq, ValueEnum)]
pub enum Seriousness {
    Serious,
    NonSerious,
}

#[derive(Debug, Clone, PartialEq, Eq, ValueEnum)]
pub enum DeviationCategory {
    Major,
    Minor,
    Critical,
}

#[derive(Debug, Clone, PartialEq, Eq, ValueEnum)]
pub enum ExportFormat {
    Json,
    Fhir,
    Csv,
    Sdtm,
    Redcap,
}

#[derive(Subcommand, Debug, PartialEq, Clone)]
pub enum ModelCommand {
    /// List all deployed models
    List {
        #[clap(long)] status: Option<ModelStatus>,
        #[clap(long)] domain: Option<String>, // sepsis, readmission, etc.
    },

    /// Show detailed model information
    Show {
        model_name: String,
        #[clap(long)] include_metrics: bool,
        #[clap(long)] include_features: bool,
        #[clap(long)] include_version_history: bool,
    },

    /// Deploy new model version
    Deploy {
        model_name: String,
        #[clap(long)] version: String,
        #[clap(long)] path: std::path::PathBuf,
        #[clap(long)] format: ModelFormat,
        #[clap(long)] description: Option<String>,
        #[clap(long)] force: bool,
    },

    /// Run real-time prediction
    Predict {
        model_name: String,
        patient_id: i32,
        #[clap(long)] encounter_id: Option<Uuid>,
        #[clap(long)] output_format: Option<PredictionFormat>,
    },

    /// Evaluate model performance
    Evaluate {
        model_name: String,
        #[clap(long)] test_cohort: Option<String>,
        #[clap(long)] start_date: Option<String>,
        #[clap(long)] end_date: Option<String>,
    },

    /// Retrain model with new data
    Retrain {
        model_name: String,
        #[clap(long)] cohort_query: Option<String>,
        #[clap(long)] outcome_variable: String,
        #[clap(long)] time_window_hours: Option<i64>,
    },

    /// Model monitoring dashboard
    Monitor {
        model_name: Option<String>,
        #[clap(long)] timeframe: Option<String>, // 1h, 24h, 7d
        #[clap(long)] alert_threshold: Option<f64>,
    },

    /// Explain prediction (XAI)
    Explain {
        model_name: String,
        patient_id: i32,
        #[clap(long)] top_features: Option<usize>,
        #[clap(long)] format: Option<ExplainFormat>,
    },

    /// Model drift detection
    Drift {
        model_name: String,
        #[clap(long)] baseline_date: Option<String>,
    },

    /// A/B test models
    AbTest {
        model_a: String,
        model_b: String,
        #[clap(long)] traffic_split: Option<String>, // "50-50", "10-90"
        #[clap(long)] duration_days: Option<i64>,
    },

    /// Rollback to previous version
    Rollback {
        model_name: String,
        #[clap(long)] version: Option<String>, // latest safe if omitted
    },
}

#[derive(Debug, Clone, PartialEq, Eq, ValueEnum)]
pub enum ModelStatus {
    Development,
    Staging,
    Production,
    Retired,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq, ValueEnum)]
pub enum ModelFormat {
    Pytorch,
    Tensorflow,
    Onnx,
    Pkl,
    Json,
}

#[derive(Debug, Clone, PartialEq, Eq, ValueEnum)]
pub enum PredictionFormat {
    Json,
    FhirObservation,
    ClinicalNote,
    Alert,
}

#[derive(Debug, Clone, PartialEq, Eq, ValueEnum)]
pub enum ExplainFormat {
    Text,
    Json,
    Html,
    Lime,
    Shap,
}

// =========================================================================
// FACILITY & FINANCIAL
// =========================================================================
#[derive(Debug, Clone, PartialEq, Subcommand)]
pub enum FacilityCommand {
    Beds { #[clap(long)] unit: Option<String> },
    Capacity,
}

#[derive(Debug, Clone, PartialEq, Subcommand)]
pub enum FinancialCommand {
    ServiceLine { service: String },
    PayerMix,
}

// =========================================================================
// COMPLIANCE & ACCESS
// =========================================================================
#[derive(Debug, Clone, PartialEq, Subcommand)]
pub enum ComplianceCommand {
    AuditPatient { patient_id: i32, #[clap(long)] from: DateTime<Utc> },
    AuditControlledSubstances { #[clap(long)] from: DateTime<Utc> },
}

#[derive(Debug, Clone, PartialEq, Subcommand)]
pub enum AccessCommand {
    Login { username: String },
    Whoami,
    Logout,
}

// AFTER — this is the ONLY change you need:
#[derive(Debug, Clone, PartialEq, Subcommand)]
pub enum GraphAction {
    #[command(about = "Insert a new Person node")]
    InsertPerson {
        #[arg(long, help = "Name of the person")]
        name: Option<String>,

        #[arg(long, help = "Age of the person")]
        age: Option<i32>,

        #[arg(long, help = "City of residence")]
        city: Option<String>,
    },

    #[command(about = "Create a medical record (patient + diagnosis)")]
    MedicalRecord {
        #[arg(long, help = "Patient name")]
        patient_name: Option<String>,

        #[arg(long, help = "Patient age")]
        patient_age: Option<i32>,

        #[arg(long, help = "ICD diagnosis code")]
        diagnosis_code: Option<String>,
    },

    #[command(about = "Delete node by ID")]
    DeleteNode {
        #[arg(help = "Node ID (UUID)")]
        id: String,
    },

    #[command(about = "Bulk load data from JSON/CSV")]
    LoadData {
        #[arg(help = "Path to data file")]
        path: std::path::PathBuf,
    },
}


// =========================================================================
// ALERT & NOTIFICATION SYSTEM
// =========================================================================
#[derive(Debug, Clone, PartialEq, Subcommand)]
pub enum AlertCommand {
    /// List active alerts
    List {
        #[clap(long)] patient_id: Option<i32>,
        #[clap(long)] severity: Option<AlertSeverity>,
        #[clap(long)] type_filter: Option<String>,
        #[clap(long)] unresolved_only: bool,
    },

    /// Create new clinical alert
    Create {
        patient_id: i32,
        alert_type: String, // "CRITICAL_LAB", "DDI_HIGH", "SEPSIS_RISK"
        message: String,
        #[clap(long)] severity: Option<AlertSeverity>,
        #[clap(long)] trigger_source: Option<String>,
        #[clap(long)] expires_in_hours: Option<i64>,
    },

    /// Acknowledge/alert resolution
    Ack {
        alert_id: Uuid,
        #[clap(long)] resolved_by: i32,
        #[clap(long)] resolution_note: Option<String>,
    },

    /// Escalate alert
    Escalate {
        alert_id: Uuid,
        #[clap(long)] to_role: Option<String>, // "rapid-response", "ICU-consult"
        #[clap(long)] reason: Option<String>,
    },

    /// Alert dashboard
    Dashboard {
        #[clap(long)] provider_id: Option<i32>,
        #[clap(long)] unit: Option<String>,
        #[clap(long)] timeframe: Option<String>, // today, shift, 24h
    },
}

#[derive(Debug, Clone, PartialEq, Eq, ValueEnum)]
pub enum AlertSeverity {
    Critical,
    High,
    Medium,
    Low,
    Info,
} 

// =========================================================================
// DOSING
// =========================================================================
#[derive(Debug, Clone, PartialEq, Subcommand)]
pub enum DosingCommand {
    /// Real-time dose calculation with adjustments
    Calculate {
        patient_id: i32,
        medication: String,
        #[clap(long)] indication: Option<String>,
        #[clap(long)] weight_kg: Option<f64>,
        #[clap(long)] height_cm: Option<f64>,
        #[clap(long)] creatinine: Option<f64>,
        #[clap(long)] age: Option<i32>,
        #[clap(long)] hepatic_function: Option<HepaticFunction>,
        #[clap(long)] dialysis: bool,
    },

    /// Verify current dose appropriateness
    Verify {
        patient_id: i32,
        #[clap(long)] prescription_id: Option<Uuid>,
        #[clap(long)] medication: Option<String>,
        #[clap(long)] include_therapeutic_duplicates: bool,
    },

    /// Vancomycin pharmacokinetic dosing
    Vancomycin {
        patient_id: i32,
        #[clap(long)] trough_level: Option<f64>,
        #[clap(long)] target_auc: Option<f64>, // default 400-600
        #[clap(long)] loading_dose: bool,
    },

    /// Warfarin dosing with INR feedback
    Warfarin {
        patient_id: i32,
        current_inr: f64,
        target_inr_range: Option<String>, // "2.0-3.0"
        #[clap(long)] weekly_dose_mg: Option<f64>,
        #[clap(long)] cyp2c9: Option<String>,
        #[clap(long)] vkorc1: Option<String>,
    },

    /// Aminoglycoside (Gentamicin/Tobramycin) extended interval
    Aminoglycoside {
        patient_id: i32,
        medication: String, // "Gentamicin", "Tobramycin"
        #[clap(long)] level: Option<f64>,
        #[clap(long)] level_timing_hours: Option<f64>,
    },

    /// Antiepileptic drug (AED) level interpretation
    Aed {
        patient_id: i32,
        medication: String, // "Phenytoin", "Valproate", "Carbamazepine"
        level: f64,
        #[clap(long)] albumin: Option<f64>,
        #[clap(long)] free_level: Option<bool>,
    },

    /// Immunosuppressant (Tacrolimus, Cyclosporine) monitoring
    Immunosuppressant {
        patient_id: i32,
        medication: String,
        trough_level: f64,
        #[clap(long)] transplant_type: Option<String>,
        #[clap(long)] post_transplant_days: Option<i32>,
    },

    /// Chemotherapy dose calculation & adjustment
    Chemo {
        patient_id: i32,
        regimen: String,
        cycle: i32,
        #[clap(long)] bsa_m2: Option<f64>,
        #[clap(long)] auc_target: Option<f64>, // for carboplatin
        #[clap(long)] dose_reduction_pct: Option<i32>,
    },

    /// Pediatric weight-based dosing
    Pediatric {
        patient_id: i32,
        medication: String,
        #[clap(long)] weight_kg: Option<f64>,
        #[clap(long)] age_months: Option<i32>,
        #[clap(long)] max_dose: Option<bool>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, ValueEnum)]
pub enum HepaticFunction {
    Normal,
    Mild,
    Moderate,
    Severe,
    CirrhosisChildPughA,
    CirrhosisChildPughB,
    CirrhosisChildPughC,
}

// =========================================================================
// PATHOLOGY
// =========================================================================
#[derive(Debug, Clone, PartialEq, Subcommand)]
pub enum PathologyCommand {
    /// Register new specimen
    Specimen {
        patient_id: i32,
        encounter_id: Option<Uuid>,
        specimen_type: String, // "Biopsy", "Surgical resection"
        collection_site: String,
        #[clap(long)] collection_date: Option<String>,
        #[clap(long)] collector: Option<i32>,
    },

    /// Record pathology result
    Result {
        specimen_id: Uuid,
        diagnosis: String,
        #[clap(long)] malignancy: Option<bool>,
        #[clap(long)] grade: Option<i32>,
        #[clap(long)] stage: Option<String>,
        #[clap(long)] margins: Option<String>,
        #[clap(long)] biomarkers: Vec<String>,
        #[clap(long)] pathologist: i32,
        #[clap(long)] report_date: Option<String>,
    },

    /// Add molecular/genomic findings
    Molecular {
        specimen_id: Uuid,
        test_type: String, // "NGS", "IHC", "FISH"
        gene: String,
        variant: String,
        #[clap(long)] interpretation: Option<String>,
        #[clap(long)] therapeutic_implication: Option<String>,
    },

    /// Search pathology reports
    Search {
        #[clap(long)] patient_id: Option<i32>,
        #[clap(long)] diagnosis_contains: Option<String>,
        #[clap(long)] biomarker: Option<String>,
        #[clap(long)] malignancy: Option<bool>,
    },
}

// =========================================================================
// MICROBIOLOGY
// =========================================================================
#[derive(Debug, Clone, PartialEq, Subcommand)]
pub enum MicrobiologyCommand {
    /// Register culture specimen
    Culture {
        patient_id: i32,
        encounter_id: Option<Uuid>,
        source: String, // "Blood", "Urine", "Wound"
        #[clap(long)] collection_date: Option<String>,
        #[clap(long)] collected_by: Option<i32>,
    },

    /// Preliminary culture result
    Preliminary {
        culture_id: Uuid,
        finding: String, // "Gram positive cocci in clusters"
        #[clap(long)] organism_quantity: Option<String>,
    },

    /// Final identification & sensitivities
    Final {
        culture_id: Uuid,
        organism: String, // "MRSA", "Pseudomonas aeruginosa"
        #[clap(long)] sensitivities: Vec<String>, // "Vancomycin:S", "Ceftazidime:R"
        #[clap(long)] mic_values: Vec<String>,
        #[clap(long)] microbiologist: i32,
    },

    /// Antibiotic resistance trends
    Resistance {
        organism: String,
        #[clap(long)] antibiotic: Option<String>,
        #[clap(long)] timeframe: Option<String>,
    },

    /// Alert on resistant organisms
    Alert {
        organism: String, // "VRE", "CRE", "MDR-Pseudomonas"
        #[clap(long)] auto_notify: bool,
    },

    /// Culture turnaround time
    Turnaround {
        #[clap(long)] source: Option<String>,
        #[clap(long)] target_hours: Option<i64>,
    },
}

// =========================================================================
// NURSING & EDUCATION
// =========================================================================
#[derive(Subcommand, Debug, PartialEq, Clone)]
pub enum NursingCommand {
    Assessment { patient_id: i32, #[clap(long)] shift: Option<String>, #[clap(long)] pain_level: Option<i32>, #[clap(long)] mobility: Option<String>, #[clap(long)] notes: Option<String> },
    SafetyCheck { patient_id: i32, #[clap(long)] fall_risk: Option<String>, #[clap(long)] pressure_ulcer_risk: Option<String>, #[clap(long)] notes: Option<String> },
    IntakeOutput { patient_id: i32, #[clap(long)] intake_ml: Option<i64>, #[clap(long)] output_ml: Option<i64>, #[clap(long)] notes: Option<String> },
    #[clap(subcommand)]
    MedAdministration(MedAdministrationCommand),
    #[clap(subcommand)]
    CarePlan(CarePlanCommand),
    PatientEducation { patient_id: i32, topic: String, #[clap(long)] method: Option<EducationMethod> },
}

#[derive(Subcommand, Debug, PartialEq, Clone)]
pub enum MedAdministrationCommand {
    Verify { order_id: Uuid, patient_id: i32, #[clap(long)] barcode_scan: bool },
    Give { order_id: Uuid, #[clap(long)] time_given: Option<String>, #[clap(long)] route: Option<String> },
    Refuse { order_id: Uuid, reason: String },
}

#[derive(Subcommand, Debug, PartialEq, Clone)]
pub enum CarePlanCommand {
    Add { patient_id: i32, goal: String, interventions: String },
    Update { patient_id: i32, goal_id: Uuid, status: String },
}

#[derive(Subcommand, Debug, PartialEq, Clone)]
pub enum EducationCommand {
    Document { patient_id: i32, 
               topic: String, 
               #[clap(long)] 
               teach_back_verified: bool,
               language: Option<String>,
               literacy_level: Option<String>,
               method: Option<EducationMethod> 
    },
}

// =========================================================================
// Schedule Command
// =========================================================================
#[derive(Subcommand, Debug, PartialEq, Clone)]
pub enum ScheduleCommand {
    /// Books a new appointment, test, or resource (e.g., room, equipment).
    Book {
        patient_id: String,
        appointment_type: String,
        provider_id: Option<String>,
        facility_id: Option<String>,
        time: String,
        duration_minutes: u32,
        notes: Option<String>,
    },
    /// Cancels an existing scheduled item.
    Cancel {
        schedule_id: String,
        reason: String,
    },
    /// Moves an existing scheduled item.
    Reschedule {
        schedule_id: String,
        new_time: String,
        new_provider_id: Option<String>,
        notes: Option<String>,
    },
    /// Queries scheduled items based on criteria.
    Query {
        patient_id: Option<String>,
        provider_id: Option<String>,
        facility_id: Option<String>,
        from_time: Option<String>,
        to_time: Option<String>,
    },
    /// Finds available time slots for a resource.
    Available {
        provider_id: String,
        appointment_type: Option<String>,
        start_time: Option<String>,
        lookahead_days: u32,
    },
}

// =========================================================================
// Timing
// =========================================================================
#[derive(Subcommand, Debug, PartialEq, Clone)]
pub enum TimingCommand {
    /// Starts a clinical timer for a procedure or infusion.
    Start {
        encounter_id: String,
        event_name: String,
        start_time: Option<String>,
    },
    /// Stops a previously started timer.
    Stop {
        timer_id: String,
        stop_time: Option<String>,
        result: String, // e.g., "Completed", "Interrupted", "Adverse Event"
    },
    /// Marks a specific time point during an encounter.
    Mark {
        encounter_id: String,
        milestone_name: String,
        time: Option<String>,
    },
    /// Sets or updates a due time for an ordered task.
    Due {
        order_id: String,
        due_time: String,
        notes: Option<String>,
    },
}

// =========================================================================
// Emergencies
// =========================================================================
#[derive(Subcommand, Debug, PartialEq, Clone)]
pub enum EmergCommand {
    /// Initiates a clinical emergency protocol (e.g., Sepsis, Trauma, Stroke).
    Alert {
        patient_id: String,
        protocol_name: String,
        location: String,
        severity: String,
        notes: Option<String>,
    },
    /// Records a specific step completed in an ongoing emergency protocol.
    Protocol {
        alert_id: String,
        step_name: String,
        status: String, // e.g., "Initiated", "Completed", "Failed"
        time: Option<String>,
    },
    /// Updates the overall status of the emergency.
    Status {
        alert_id: String,
        status: String,
        severity: Option<String>,
    },
    /// Assigns a role to a user during an emergency response.
    Team {
        alert_id: String,
        user_id: String,
        role: String,
    },
}

// =========================================================================
// RESUS
// =========================================================================
#[derive(Subcommand, Debug, PartialEq, Clone)]
pub enum ResusCommand {
    /// Initiates a resuscitation (Code Blue) event and starts the timer.
    Code {
        patient_id: String,
        location: String,
        initial_rhythm: String,
        time: Option<String>,
    },
    /// Records administration of a code medication.
    Drug {
        code_id: String,
        drug_name: String,
        dose: String,
        route: String,
        time: Option<String>,
    },
    /// Records a defibrillation or cardioversion event.
    Shock {
        code_id: String,
        energy_joules: u32,
        rhythm_pre: String,
        rhythm_post: String,
        time: Option<String>,
    },
    /// Records critical non-drug/non-shock intervention (e.g., intubation).
    Intervention {
        code_id: String,
        action_name: String,
        success: bool,
        notes: Option<String>,
        time: Option<String>,
    },
    /// Records the termination of the resuscitation event.
    Stop {
        code_id: String,
        outcome: String, // e.g., "ROSC Achieved", "Pronounced Dead", "Transferred"
        time: Option<String>,
    },
}

// =========================================================================
// DISCHARGE PLANNING
// =========================================================================
#[derive(Subcommand, Debug, PartialEq, Clone)]
pub enum DischargePlanningCommand {
    Assess { patient_id: i32, #[clap(long)] barriers: Vec<String> },
    DmeOrder { patient_id: i32, equipment: String },
    FollowUp { patient_id: i32, provider_id: i32, days_out: i64 },
}

// =========================================================================
// QUALITY & INCIDENT
// =========================================================================
#[derive(Subcommand, Debug, PartialEq, Clone)]
pub enum QualityCommand {
    Measure { name: String },
    GapReport { #[clap(long)] payer: Option<String> },
}

#[derive(Subcommand, Debug, PartialEq, Clone)]
pub enum IncidentCommand {
    Report { patient_id: i32, incident_type: String, description: String },
    Investigate { incident_id: Uuid },
}

#[derive(Debug, Clone, PartialEq, Subcommand)]
pub enum IndexAction {
    #[command(about = "Create a standard B-Tree index or a single-field FULLTEXT index")]
    Create {
        // This will capture either "FULLTEXT" (in 3-arg usage) or the Label (in 2-arg usage)
        #[arg(help = "The index type (e.g., FULLTEXT) or the Label (e.g., Person)")]
        arg1: String,
        
        // This will capture either the Label (in 3-arg usage) or the Property (in 2-arg usage)
        #[arg(help = "The Label (e.g., Person) or the Property (e.g., name)")]
        arg2: String,
        
        // This makes the third argument optional, allowing both `create <A> <B>` and `create <A> <B> <C>`
        #[arg(help = "The Property (e.g., name). Required if index type is provided.")]
        arg3_property: Option<String>,
    },

    #[command(about = "Search the index")]
    Search {
        #[arg(help = "The term to search for (e.g., \"Oliver Stone\")")]
        term: String,
        
        #[clap(subcommand)]
        order: Option<SearchOrder>,
    },

    #[command(about = "Rebuild all existing indexes")]
    Rebuild,

    #[command(about = "List all existing indexes")]
    List,

    #[command(about = "Show statistics about all indexes")]
    Stats,

    #[command(about = "Drop a standard index")]
    Drop {
        #[arg(help = "The Label (e.g., Person)")]
        label: String,
        #[arg(help = "The Property (e.g., name)")]
        property: String,
    },

    #[command(name = "create-fulltext-index", about = "Create a new fulltext index (for multi-field/multi-label indices)")]
    CreateFulltext {
        #[arg(help = "The name for the new index (e.g., people_fulltext_index)")]
        index_name: String,

        // FIX: Added `long` to allow the --labels flag.
        // Use `value_delimiter = ','` to allow comma separation when using the flag.
        // We must rely on the flags to unambiguously separate the two lists.
        #[arg(required = true, long, help = "Comma-separated list of Labels (e.g., Person,Movie)", value_delimiter = ',', num_args = 1..)]
        labels: Vec<String>,

        // FIX: Added `long` to allow the --properties flag.
        // Use `value_delimiter = ','` to allow comma separation when using the flag.
        #[arg(required = true, long, help = "Comma-separated list of Properties (e.g., name,title,summary)", value_delimiter = ',', num_args = 1..)]
        properties: Vec<String>
    },

    #[command(name = "drop-fulltext-index", about = "Drop a fulltext index")]
    DropFulltext {
        #[arg(help = "The name of the index to drop")]
        index_name: String
    }
}


#[derive(Debug, Clone, PartialEq, Subcommand)]
pub enum SearchOrder {
    #[command(about = "Get top N results (highest scores first)")]
    Top {
        #[arg(help = "Number of results to return", value_name = "COUNT")]
        count: usize,
    },
    
    #[command(about = "Get top N results (alias for 'top')")]
    Head {
        #[arg(help = "Number of results to return", value_name = "COUNT")]
        count: usize,
    },
    
    #[command(about = "Get bottom N results (lowest scores first)")]
    Bottom {
        #[arg(help = "Number of results to return", value_name = "COUNT")]
        count: usize,
    },
    
    #[command(about = "Get bottom N results (alias for 'bottom')")]
    Tail {
        #[arg(help = "Number of results to return", value_name = "COUNT")]
        count: usize,
    },
}

/// Arguments for the unified query command.
#[derive(Args, Debug, PartialEq, Clone)]
pub struct QueryArgs {
    /// The query string to execute.
    #[clap(value_name = "QUERY", help = "The query to run (Cypher, SQL, KV, etc.)")]
    pub query_string: String,

    /// Optional flag to explicitly specify the query language (e.g., cypher, sql, graphql, kv).
    #[clap(long = "language", short = 'l', value_name = "LANG", help = "Force a language (cypher|sql|kv|graphql). If omitted, language is inferred.")]
    pub language: Option<String>,
}

/// GraphDB Command Line Interface
#[derive(Parser, Debug)]
#[clap(author, version, about = "GraphDB Command Line Interface", long_about = None)]
#[clap(propagate_version = true)]
pub struct CliArgs {
    #[clap(subcommand)]
    pub command: Option<Commands>,

    /// Run CLI in interactive mode
    #[clap(long, short = 'c', action = ArgAction::SetTrue)]
    pub cli: bool,

    /// Enable experimental plugins
    #[clap(long)]
    pub enable_plugins: bool,

    /// Execute a direct query string
    #[clap(long, short = 'q', value_name = "QUERY", conflicts_with = "command")]
    pub query: Option<String>,

    // Internal flags for daemonized processes (hidden from help)
    #[clap(long, hide = true)]
    pub internal_rest_api_run: bool,
    #[clap(long, hide = true)]
    pub internal_storage_daemon_run: bool,
    #[clap(long, hide = true)]
    pub internal_daemon_run: bool,
    #[clap(long, hide = true)]
    pub internal_port: Option<u16>,
    #[clap(long, hide = true)]
    pub internal_storage_config_path: Option<PathBuf>,
    #[clap(long, hide = true)]
    pub internal_storage_engine: Option<StorageEngineType>,
    #[clap(long, hide = true)]
    pub internal_data_directory: Option<PathBuf>,
    #[clap(long, hide = true)]
    pub internal_cluster_range: Option<String>,
}

#[derive(Subcommand, Debug, PartialEq, Clone)]
pub enum Commands {
    /// Start GraphDB components (daemon, rest, storage, or all)
    Start {
        #[arg(long, value_parser = clap::value_parser!(u16), help = "Port for the daemon. Conflicts with --daemon-port if both specified.")]
        port: Option<u16>,
        #[arg(long, value_parser = clap::value_parser!(String), help = "Cluster range for the daemon. Conflicts with --daemon-cluster if both specified.")]
        cluster: Option<String>,
        #[arg(long, value_parser = clap::value_parser!(u16), help = "Port for the daemon (synonym for --port).")]
        daemon_port: Option<u16>,
        #[arg(long, value_parser = clap::value_parser!(String), help = "Cluster range for the daemon (synonym for --cluster).")]
        daemon_cluster: Option<String>,
        #[arg(long, value_parser = clap::value_parser!(u16), help = "Listen port for the REST API.")]
        listen_port: Option<u16>,
        #[arg(long, value_parser = clap::value_parser!(u16), help = "Port for the REST API. Conflicts with --listen-port if both specified.")]
        rest_port: Option<u16>,
        #[arg(long, value_parser = clap::value_parser!(String), help = "Cluster name for the REST API.")]
        rest_cluster: Option<String>,
        #[arg(long, value_parser = clap::value_parser!(u16), help = "Port for the Storage Daemon. Synonym for --port in `start storage`.")]
        storage_port: Option<u16>,
        #[arg(long, value_parser = clap::value_parser!(String), help = "Cluster name for the Storage Daemon. Synonym for --cluster in `start storage`.")]
        storage_cluster: Option<String>,
        #[arg(long, value_parser = clap::value_parser!(PathBuf), help = "Path to the Storage Daemon configuration file.")]
        storage_config: Option<PathBuf>,
        #[clap(subcommand)]
        action: Option<StartAction>,
    },
    /// Stop GraphDB components (daemon, rest, storage, or all)
    Stop(StopArgs),
    /// Get status of GraphDB components (daemon, rest, storage, cluster, or all)
    Status(StatusArgs),
    /// Manage GraphDB daemon instances
    #[clap(subcommand)]
    Daemon(DaemonCliCommand),
    /// Manage REST API server
    #[clap(subcommand)]
    Rest(RestCliCommand),
    /// Manage standalone Storage daemon
    #[clap(subcommand)]
    Storage(StorageAction),
    /// Configure components (storage engine or plugins)
    #[clap(subcommand)]
    Use(UseAction),
    /// Save changes to storage or configuration
    #[clap(subcommand)]
    Save(SaveAction),
    /// Reload GraphDB components (all, rest, storage, daemon, or cluster)
    Reload(ReloadArgs),
    /// Restart GraphDB components (all, rest, storage, daemon, or cluster)
    Restart(RestartArgs),
    /// Run CLI in interactive mode
    Interactive,
    /// Authenticate a user and get a token
    Auth {
        username: String,
        password: String,
    },
    /// Authenticate a user and get a token (alias for 'auth')
    Authenticate {
        username: String,
        password: String,
    },
    /// Register a new user
    Register {
        username: String,
        password: String,
    },
    /// Get the version of the REST API server
    Version,
    /// Perform a health check on the REST API server
    Health,
    /// Display help message
    Help(HelpArgs),
    /// Clear the terminal screen
    Clear,
    /// Exit the CLI
    Exit,
    /// Quit the CLI (alias for 'exit')
    Quit,
    /// Show information about system components
    Show(ShowArgs),
    /// Execute a query against the running GraphDB REST API.
    #[clap(alias = "q", alias = "e")]
    Query(QueryArgs),
    /// Interact with the key-value store.
    #[clap(alias = "k")]
    Kv {
        #[clap(subcommand)]
        action: KvAction,
    },
    /// Migrates one store to another.
    #[clap(alias = "m")]
    Migrate(MigrateAction),

    /// Graph engine domain-specific operations (insert, medical, delete, load)
    #[clap(subcommand)]
    Graph(GraphAction),

    /// Full-text and indexing operations
    #[clap(subcommand)]
    Index(IndexAction),

    // THE HISTORY COMMAND H
    History {
        #[clap(subcommand)]
        action: Option<HistoryCommand>,
    },

    #[clap(subcommand)]
    Cleanup(CleanupCommand), // <--- ADDED
    // =========================================================================
    // PATIENT MANAGEMENT
    // =========================================================================
    #[clap(subcommand)]
    Patient(PatientCommand),

    // =========================================================================
    // CLINICAL WORKFLOW
    // =========================================================================
    #[clap(subcommand)]
    Encounter(EncounterCommand),
    #[clap(subcommand)]
    Diagnosis(DiagnosisCommand),
    #[clap(subcommand)]
    Prescription(PrescriptionCommand),
    #[clap(subcommand)]
    Note(NoteCommand),
    #[clap(subcommand)]
    Referral(ReferralCommand),
    #[clap(subcommand)]
    Triage(TriageCommand),
    #[clap(subcommand)]
    Disposition(DispositionCommand),
    #[clap(subcommand)]
    Allergy(AllergyCommand),
    #[clap(subcommand)]
    Appointment(AppointmentCommand),
    #[clap(subcommand)]
    Problem(ProblemCommand),
    #[clap(subcommand)]
    Order(OrderCommand),
    #[clap(subcommand)]
    Discharge(DischargeCommand),
    #[clap(subcommand)]
    Procedure(ProcedureCommand),
    // =========================================================================
    // DOSING
    // =========================================================================
    #[clap(subcommand)]
    Dosing(DosingCommand),

    // =========================================================================
    // ALERT & NOTIFICATION SYSTEM
    // =========================================================================
    #[clap(subcommand)]
    Alert(AlertCommand),

    // =========================================================================
    // PATHOLOGY
    // =========================================================================
    #[clap(subcommand)]
    Pathology(PathologyCommand),

    // =========================================================================
    // MICROBIOLOGY
    // =========================================================================
    #[clap(subcommand)]
    Microbiology(MicrobiologyCommand),

    // =========================================================================
    // VITAL & OBSERVATION
    // =========================================================================
    #[clap(subcommand)]
    Vitals(VitalsCommand),
    #[clap(subcommand)]
    Observation(ObservationCommand),

    // =========================================================================
    // LAB & IMAGING
    // =========================================================================
    #[clap(subcommand)]
    Lab(LabCommand),
    #[clap(subcommand)]
    Imaging(ImagingCommand),

    // =========================================================================
    // SPECIALTY CARE
    // =========================================================================
    #[clap(subcommand)]
    Chemo(ChemoCommand),
    #[clap(subcommand)]
    Radiation(RadiationCommand),
    #[clap(subcommand)]
    Surgery(SurgeryCommand),

    // =========================================================================
    // DRUG SAFETY & INTERACTIONS
    // =========================================================================
    #[clap(subcommand)]
    Drug(DrugCommand),

    // =========================================================================
    // POPULATION HEALTH & ANALYTICS
    // =========================================================================
    #[clap(subcommand)]
    Population(PopulationCommand),
    #[clap(subcommand)]
    Analytics(AnalyticsCommand),
    #[clap(subcommand)]
    Metrics(MetricsCommand),

    // =========================================================================
    // COMPLIANCE & AUDIT
    // =========================================================================
    #[clap(subcommand)]
    Audit(AuditCommand),
    #[clap(subcommand)]
    Export(ExportCommand),

    // =========================================================================
    // FACILITY & ADMIN
    // =========================================================================
    #[clap(subcommand)]
    Facility(FacilityCommand),
    #[clap(subcommand)]
    Access(AccessCommand),
    #[clap(subcommand)]
    Financial(FinancialCommand),

    // =========================================================================
    // QUALITY & COMPLIANCE
    // =========================================================================
    #[clap(subcommand)]
    Quality(QualityCommand),
    #[clap(subcommand)]
    Incident(IncidentCommand),
    #[clap(subcommand)]
    Compliance(ComplianceCommand),

    // =========================================================================
    // RESEARCH & AI
    // =========================================================================
    #[clap(subcommand)]
    Research(ResearchCommand),
    #[clap(subcommand)]
    Ml(MlCommand),
    #[clap(subcommand)]
    ClinicalTrial(ClinicalTrialCommand),
    #[clap(subcommand)]
    Model(ModelCommand),

    // =========================================================================
    // NURSING & CARE COORDINATION
    // =========================================================================
    #[clap(subcommand)]
    Nursing(NursingCommand),
    #[clap(subcommand)]
    Education(EducationCommand),
    #[clap(subcommand)]
    DischargePlanning(DischargePlanningCommand),

    // =========================================================================
    // EMERGENCY, TIMING, SCHEDULING, RESUS
    // =========================================================================
    #[clap(subcommand)]
    Emerg(EmergCommand),
    #[clap(subcommand)]
    Timing(TimingCommand),
    #[clap(subcommand)]
    Shcedule(ScheduleCommand),
    #[clap(subcommand)]
    Resus(ResusCommand),

    // =========================================================================
    // Master Patient Index
    // =========================================================================
    #[clap(subcommand)]
    Mpi(MPICommand), 
}

#[derive(Subcommand, Debug, PartialEq, Clone)]
pub enum KvAction {
    /// Get a value by its key.
    Get {
        #[clap(name = "KEY", help = "The key to retrieve.")]
        key: String,
    },
    /// Set a key-value pair.
    Set {
        #[clap(name = "KEY", help = "The key to set.")]
        key: String,
        #[clap(name = "VALUE", help = "The value to associate with the key.")]
        value: String,
    },
    /// Delete a key-value pair.
    Delete {
        #[clap(name = "KEY", help = "The key to delete.")]
        key: String,
    },
}

#[derive(Args, Debug, PartialEq, Clone)]
pub struct MigrateAction {
    #[clap(long, short = 'f', group = "from_engine")]
    pub from: Option<StorageEngineType>,
    #[clap(long, short = 't', group = "to_engine")]
    pub to: Option<StorageEngineType>,
    #[clap(long, group = "source_engine", conflicts_with = "from")]
    pub source: Option<StorageEngineType>,
    #[clap(long, group = "dest_engine", conflicts_with = "to")]
    pub dest: Option<StorageEngineType>,
    #[clap(value_name = "FROM_ENGINE", value_parser = parse_storage_engine, required_unless_present_any = ["from", "source"])]
    pub from_engine_pos: Option<StorageEngineType>,
    #[clap(value_name = "TO_ENGINE", value_parser = parse_storage_engine, required_unless_present_any = ["to", "dest"])]
    pub to_engine_pos: Option<StorageEngineType>,
    #[clap(long)]
    pub port: Option<u16>,
    #[clap(long)]
    pub cluster: Option<String>,
}

#[derive(Args, Debug, PartialEq, Clone)]
pub struct ShowArgs {
    #[clap(subcommand)]
    pub action: ShowAction,
}

#[derive(Subcommand, Debug, PartialEq, Clone)]
pub enum SaveAction {
    /// Save changes to the storage engine configuration
    Storage,
    /// Save changes to the general configuration (alias: save config)
    #[clap(name = "config")]
    Configuration,
}

#[derive(Subcommand, Debug, PartialEq, Clone)]
pub enum ShowAction {
    /// Show the current storage engine.
    Storage,
    /// Show configuration information.
    Config {
        #[clap(subcommand)]
        config_type: ConfigAction,
    },
    /// Show the status of experimental plugins.
    Plugins,
}

#[derive(Subcommand, Debug, PartialEq, Clone)]
pub enum ConfigAction {
    /// Show all configurations.
    All,
    /// Show REST API configuration.
    Rest,
    /// Show Storage configuration.
    Storage,
    /// Show main daemon configuration.
    Main,
}

#[derive(Subcommand, Debug, PartialEq, Clone)]
pub enum StartAction {
    /// Start all components (daemon, rest, storage)
    All {
        /// Port for the main daemon
        #[arg(long, value_parser = clap::value_parser!(u16), help = "Port for the main daemon.")]
        port: Option<u16>,
        /// Cluster range for the main daemon (e.g., "9001-9005")
        #[arg(long, value_parser = clap::value_parser!(String), help = "Cluster range for the main daemon.")]
        cluster: Option<String>,
        /// Port for the main daemon (synonym for --port)
        #[arg(long, value_parser = clap::value_parser!(u16), help = "Port for the main daemon (synonym for --port).", conflicts_with = "port")]
        daemon_port: Option<u16>,
        /// Cluster range for the main daemon (synonym for --cluster)
        #[arg(long, value_parser = clap::value_parser!(String), help = "Cluster range for the main daemon (synonym for --cluster).", conflicts_with = "cluster")]
        daemon_cluster: Option<String>,
        /// Listen port for the REST API
        #[arg(long, value_parser = clap::value_parser!(u16), help = "Listen port for the REST API.")]
        listen_port: Option<u16>,
        /// Port for the REST API (synonym for --listen-port)
        #[arg(long, value_parser = clap::value_parser!(u16), help = "Port for the REST API (synonym for --listen-port).", conflicts_with = "listen_port")]
        rest_port: Option<u16>,
        /// Cluster range for the REST API (e.g., "8080-8085")
        #[arg(long, value_parser = clap::value_parser!(String), help = "Cluster range for the REST API.")]
        rest_cluster: Option<String>,
        /// Port for the Storage Daemon
        #[arg(long, value_parser = clap::value_parser!(u16), help = "Port for the Storage Daemon.")]
        storage_port: Option<u16>,
        /// Cluster range for the Storage Daemon (e.g., "8080-8085")
        #[arg(long, value_parser = clap::value_parser!(String), help = "Cluster range for the Storage Daemon.")]
        storage_cluster: Option<String>,
        /// Path to the Storage Daemon configuration file
        #[arg(long, value_parser = clap::value_parser!(PathBuf), help = "Path to the Storage Daemon configuration file.")]
        storage_config: Option<PathBuf>,
    },
    /// Start the GraphDB daemon.
    Daemon {
        /// Port for the daemon.
        #[clap(long, short = 'p')]
        port: Option<u16>,
        /// Cluster range for daemon (e.g., "9001-9005").
        #[clap(long)]
        cluster: Option<String>,
        /// Port for the daemon (synonym for --port).
        #[clap(long = "daemon-port", conflicts_with = "port")]
        daemon_port: Option<u16>,
        /// Cluster range for daemon (synonym for --cluster).
        #[clap(long = "daemon-cluster", conflicts_with = "cluster")]
        daemon_cluster: Option<String>,
    },
    /// Start the REST API server.
    #[clap(name = "rest")]
    Rest {
        /// Port for the REST API.
        #[clap(long, short = 'p', name = "listen-port")]
        port: Option<u16>,
        /// Cluster range for REST (e.g., "8080-8085").
        #[clap(long)]
        cluster: Option<String>,
        /// Port for the REST API (synonym for --listen-port).
        #[clap(long = "rest-port", conflicts_with = "port")]
        rest_port: Option<u16>,
        /// Cluster range for REST (synonym for --cluster).
        #[clap(long = "rest-cluster", conflicts_with = "cluster")]
        rest_cluster: Option<String>,
    },
    /// Start the standalone Storage daemon.
    #[clap(name = "storage")]
    Storage {
        /// Port for the Storage daemon.
        #[clap(long, short = 'p', name = "storage-port")]
        port: Option<u16>,
        /// Path to the storage configuration file.
        #[clap(long)]
        config_file: Option<PathBuf>,
        /// Cluster range for Storage (e.g., "8080-8085").
        #[clap(long)]
        cluster: Option<String>,
        /// Port for the Storage daemon (synonym for --storage-port).
        #[clap(long = "storage-port")]
        storage_port: Option<u16>,
        /// Cluster range for Storage (synonym for --cluster).
        #[clap(long = "storage-cluster", conflicts_with = "cluster")]
        storage_cluster: Option<String>,
    },
}

#[derive(Args, Debug, PartialEq, Clone)]
pub struct StopArgs {
    #[clap(subcommand)]
    pub action: Option<StopAction>,
}

#[derive(Subcommand, Debug, PartialEq, Clone)]
pub enum StopAction {
    /// Stop all running components.
    All,
    /// Stop the REST API server.
    Rest {
        /// Port of the daemon to stop.
        #[clap(long, short = 'p')]
        port: Option<u16>,
    },
    /// Stop a specific GraphDB daemon instance by port.
    Daemon {
        /// Port of the daemon to stop.
        #[clap(long, short = 'p')]
        port: Option<u16>,
    },
    /// Stop the standalone Storage daemon by port.
    Storage {
        /// Port of the storage daemon to stop.
        #[clap(long, short = 'p')]
        port: Option<u16>,
    },
}

#[derive(Args, Debug, PartialEq, Clone)]
pub struct StatusArgs {
    #[clap(subcommand)]
    pub action: Option<StatusAction>,
}

#[derive(Subcommand, Debug, PartialEq, Clone)]
pub enum StatusAction {
    /// Get a summary status of all running components.
    All,
    Summary,
    /// Get the status of the REST API server.
    Rest {
        /// Port of the daemon to check.
        #[clap(long, short = 'p')]
        port: Option<u16>,
        #[clap(long)]
        cluster: Option<String>,
    },
    /// Get the status of a specific GraphDB daemon instance by port.
    Daemon {
        /// Port of the daemon to check.
        #[clap(long, short = 'p')]
        port: Option<u16>,
        #[clap(long)]
        cluster: Option<String>,
    },
    /// Get the status of the standalone Storage daemon by port.
    Storage {
        /// Port of the storage daemon to check.
        #[clap(long, short = 'p')]
        port: Option<u16>,
        #[clap(long)]
        cluster: Option<String>,
    },
    /// Get the status of the entire cluster.
    Cluster,
    /// Get the status of Raft nodes (Storage daemons).
    Raft {
        /// Port of the storage daemon to check.
        #[clap(long, short = 'p')]
        port: Option<u16>,
    },
}

#[derive(Subcommand, Debug, PartialEq, Clone)]
pub enum DaemonCliCommand {
    /// Start the GraphDB daemon.
    Start {
        /// Port for the daemon.
        #[clap(long, short = 'p')]
        port: Option<u16>,
        /// Cluster range for daemon (e.g., "9001-9005").
        #[clap(long)]
        cluster: Option<String>,
        /// Port for the daemon (synonym for --port).
        #[clap(long = "daemon-port", conflicts_with = "port")]
        daemon_port: Option<u16>,
        /// Cluster range for daemon (synonym for --cluster).
        #[clap(long = "daemon-cluster", conflicts_with = "cluster")]
        daemon_cluster: Option<String>,
    },
    /// Stop a specific GraphDB daemon instance.
    Stop {
        /// Port of the daemon to stop.
        #[clap(long, short = 'p')]
        port: Option<u16>,
    },
    /// Get the status of a specific GraphDB daemon instance.
    Status {
        /// Port of the daemon to check.
        #[clap(long, short = 'p')]
        port: Option<u16>,
        #[clap(long)]
        cluster: Option<String>,
    },
    /// List all running GraphDB daemon instances.
    List,
    /// Clear all daemon-related processes and state.
    ClearAll,
}

#[derive(Subcommand, Debug, PartialEq, Clone)]
pub enum RestCliCommand {
    /// Start the REST API server.
    Start {
        /// Port for the REST API.
        #[clap(long, short = 'p', name = "listen-port")]
        port: Option<u16>,
        /// Cluster range for REST (e.g., "8080-8085").
        #[clap(long)]
        cluster: Option<String>,
        /// Port for the REST API (synonym for --listen-port).
        #[clap(long = "rest-port", conflicts_with = "port")]
        rest_port: Option<u16>,
        /// Cluster range for REST (synonym for --cluster).
        #[clap(long = "rest-cluster", conflicts_with = "cluster")]
        rest_cluster: Option<String>,
    },
    /// Stop the REST API server.
    Stop {
        #[clap(long)]
        port: Option<u16>,
    },
    /// Get the status of the REST API server.
    Status {
        #[clap(long)]
        cluster: Option<String>,
        #[clap(long, short = 'p')]
        port: Option<u16>,
    },
    /// Perform a health check on the REST API server.
    Health,
    /// Get the version of the REST API server.
    Version,
    /// Register a new user with the REST API.
    RegisterUser {
        username: String,
        password: String,
    },
    /// Authenticate a user with the REST API.
    Authenticate {
        username: String,
        password: String,
    },
    /// Execute a graph query via the REST API.
    GraphQuery {
        query_string: String,
        #[clap(long)]
        persist: Option<bool>,
    },
    /// Execute a storage query via the REST API.
    StorageQuery,
}

#[derive(Subcommand, Debug, PartialEq, Clone)]
pub enum StorageAction {
    /// Start the standalone Storage daemon.
    Start {
        /// Port for the Storage daemon.
        #[clap(long, short = 'p', name = "storage-port")]
        port: Option<u16>,
        /// Path to the storage configuration file.
        #[clap(long)]
        config_file: Option<PathBuf>,
        /// Cluster range for Storage (e.g., "8080-8085").
        #[clap(long)]
        cluster: Option<String>,
        /// Port for the Storage daemon (synonym for --storage-port).
        #[clap(long = "storage-port")]
        storage_port: Option<u16>,
        /// Cluster range for Storage (synonym for --cluster).
        #[clap(long = "storage-cluster", conflicts_with = "cluster")]
        storage_cluster: Option<String>,
    },
    /// Stop the standalone Storage daemon.
    Stop {
        #[clap(long, short = 'p')]
        port: Option<u16>,
    },
    /// Get the status of the standalone Storage daemon.
    Status {
        #[clap(long)]
        cluster: Option<String>,
        #[clap(long, short = 'p')]
        port: Option<u16>,
    },
    StorageQuery,
    /// Perform a health check on the REST API server.
    Health,
    /// Get the version of the REST API server.
    Version,
    List,
    Show, // New variant for 'show storage'
}

#[derive(Args, Debug, PartialEq, Clone)]
pub struct ReloadArgs {
    #[clap(subcommand)]
    pub action: Option<ReloadAction>,
}

#[derive(Subcommand, Debug, PartialEq, Clone)]
pub enum ReloadAction {
    /// Reload all components.
    All,
    /// Reload the REST API server.
    Rest,
    /// Reload the standalone Storage daemon.
    Storage,
    /// Reload a specific GraphDB daemon.
    Daemon {
        /// Port for the daemon to reload.
        #[clap(long, short = 'p')]
        port: Option<u16>,
    },
    /// Reload the cluster configuration.
    Cluster,
}

#[derive(Args, Debug, PartialEq, Clone)]
pub struct RestartArgs {
    #[clap(subcommand)]
    pub action: RestartAction,
}

#[derive(Subcommand, Debug, PartialEq, Clone)]
pub enum RestartAction {
    /// Restart all components.
    All {
        #[arg(long, value_parser = clap::value_parser!(u16))]
        port: Option<u16>,
        #[arg(long, value_parser = clap::value_parser!(String))]
        cluster: Option<String>,
        #[arg(long, value_parser = clap::value_parser!(u16))]
        listen_port: Option<u16>,
        #[arg(long, value_parser = clap::value_parser!(u16))]
        storage_port: Option<u16>,
        #[arg(long, value_parser = clap::value_parser!(PathBuf))]
        storage_config_file: Option<PathBuf>,
        #[arg(long, value_parser = clap::value_parser!(String))]
        daemon_cluster: Option<String>,
        #[arg(long, value_parser = clap::value_parser!(u16))]
        daemon_port: Option<u16>,
        #[arg(long, value_parser = clap::value_parser!(String))]
        rest_cluster: Option<String>,
        #[arg(long, value_parser = clap::value_parser!(u16))]
        rest_port: Option<u16>,
        #[arg(long, value_parser = clap::value_parser!(String))]
        storage_cluster: Option<String>,
    },
    /// Restart the REST API server.
    Rest {
        /// Port for the REST API.
        #[clap(long, short = 'p')]
        port: Option<u16>,
        /// Cluster range for REST.
        #[clap(long)]
        cluster: Option<String>,
        /// Port for the REST API (synonym for --port).
        #[clap(long = "rest-port", conflicts_with = "port")]
        rest_port: Option<u16>,
        /// Cluster range for REST (synonym for --cluster).
        #[clap(long = "rest-cluster", conflicts_with = "cluster")]
        rest_cluster: Option<String>,
    },
    /// Restart the standalone Storage daemon.
    Storage {
        /// Port for the Storage daemon.
        #[clap(long, short = 'p')]
        port: Option<u16>,
        /// Path to the storage configuration file.
        #[clap(long)]
        config_file: Option<PathBuf>,
        /// Cluster range for Storage.
        #[clap(long)]
        cluster: Option<String>,
        /// Port for the Storage daemon (synonym for --port).
        #[clap(long = "storage-port", conflicts_with = "port")]
        storage_port: Option<u16>,
        /// Cluster range for Storage (synonym for --cluster).
        #[clap(long = "storage-cluster", conflicts_with = "cluster")]
        storage_cluster: Option<String>,
    },
    /// Restart a specific GraphDB daemon.
    Daemon {
        /// Port for the daemon.
        #[clap(long, short = 'p')]
        port: Option<u16>,
        /// Cluster range for daemon.
        #[clap(long)]
        cluster: Option<String>,
        /// Port for the daemon (synonym for --port).
        #[clap(long = "daemon-port", conflicts_with = "port")]
        daemon_port: Option<u16>,
        /// Cluster range for daemon (synonym for --cluster).
        #[clap(long = "daemon-cluster", conflicts_with = "cluster")]
        daemon_cluster: Option<String>,
    },
    /// Restart the cluster configuration.
    Cluster,
}

#[derive(Subcommand, Debug, PartialEq, Clone)]
pub enum UseAction {
    /// Configure the storage engine (e.g., sled, rocksdb, tikv, inmemory, redis, postgresql, mysql).
    Storage {
        /// Storage engine to use.
        #[arg(value_parser = parse_storage_engine)]
        engine: StorageEngineType,
        /// Persist the storage engine choice across sessions.
        #[clap(long, default_value = "false")]
        permanent: bool,
        /// Persist the storage engine choice across sessions.
        #[clap(long, default_value = "false")]
        migrate: bool,
    },
    /// Enable or disable experimental plugins.
    Plugin {
        /// Enable or disable plugins (true to enable, false to disable).
        #[clap(long, default_value = "true")]
        enable: bool,
    },
}

pub fn get_command_string(command: &CommandType) -> String {
    match command {
        // Daemon, Rest, Storage Commands
        CommandType::Daemon(_) => "daemon".to_string(),
        CommandType::Rest(_) => "rest".to_string(),
        CommandType::Storage(_) => "storage".to_string(),
        
        // Use Commands
        CommandType::UseStorage { .. } => "use storage".to_string(),
        CommandType::UsePlugin { .. } => "use plugin".to_string(),
        
        // Save Commands
        CommandType::SaveStorage => "save storage".to_string(),
        CommandType::SaveConfig => "save config".to_string(),
        
        // Start Commands
        CommandType::StartRest { .. } => "start rest".to_string(),
        CommandType::StartStorage { .. } => "start storage".to_string(),
        CommandType::StartDaemon { .. } => "start daemon".to_string(),
        CommandType::StartAll { .. } => "start all".to_string(),
        
        // Stop Commands
        CommandType::StopAll => "stop all".to_string(),
        CommandType::StopRest(_) => "stop rest".to_string(),
        CommandType::StopDaemon(_) => "stop daemon".to_string(),
        CommandType::StopStorage(_) => "stop storage".to_string(),
        
        // Status Commands
        CommandType::StatusSummary => "status summary".to_string(),
        CommandType::StatusDaemon(_) => "status daemon".to_string(),
        CommandType::StatusStorage(_) => "status storage".to_string(),
        CommandType::StatusCluster => "status cluster".to_string(),
        CommandType::StatusRaft(_) => "status raft".to_string(),
        
        // Authentication Commands
        CommandType::Auth { .. } => "auth".to_string(),
        CommandType::Authenticate { .. } => "authenticate".to_string(),
        CommandType::RegisterUser { .. } => "register user".to_string(),
        
        // General Info / Utility Commands
        CommandType::Version => "version".to_string(),
        CommandType::Health => "health".to_string(),
        CommandType::Clear => "clear".to_string(),
        CommandType::Cleanup(_) => "cleanup".to_string(),
        CommandType::Help(_) => "help".to_string(),
        CommandType::Exit => "exit".to_string(),
        CommandType::Unknown => "unknown".to_string(),

        // Reload Commands
        CommandType::ReloadAll => "reload all".to_string(),
        CommandType::ReloadRest => "reload rest".to_string(),
        CommandType::ReloadStorage => "reload storage".to_string(),
        CommandType::ReloadDaemon(_) => "reload daemon".to_string(),
        CommandType::ReloadCluster => "reload cluster".to_string(),
        
        // Restart Commands
        CommandType::RestartAll { .. } => "restart all".to_string(),
        CommandType::RestartRest { .. } => "restart rest".to_string(),
        CommandType::RestartStorage { .. } => "restart storage".to_string(),
        CommandType::RestartDaemon { .. } => "restart daemon".to_string(),
        CommandType::RestartCluster => "restart cluster".to_string(),
        
        // Other Core Commands
        CommandType::Show(_) => "show".to_string(),
        CommandType::Kv { .. } => "kv".to_string(),
        CommandType::Migrate(_) => "migrate".to_string(),
        CommandType::Query { .. } => "query".to_string(),
        CommandType::Visualize { .. } => "visualize".to_string(),
        CommandType::Graph(_) => "graph".to_string(),
        CommandType::Index(_) => "index".to_string(),
        CommandType::History { .. } => "history".to_string(),
        
        // Medical Commands (Alphabetical by root command)
        CommandType::Access(_) => "access".to_string(),
        CommandType::Alert(_) => "alert".to_string(),
        CommandType::Allergy(_) => "allergy".to_string(),
        CommandType::Analytics(_) => "analytics".to_string(),
        CommandType::Appointment(_) => "appointment".to_string(),
        CommandType::Audit(_) => "audit".to_string(),
        CommandType::Chemo(_) => "chemo".to_string(),
        CommandType::ClinicalTrial(_) => "clinical trial".to_string(),
        CommandType::Compliance(_) => "compliance".to_string(),
        CommandType::Diagnosis(_) => "diagnosis".to_string(),
        CommandType::Discharge(_) => "discharge".to_string(),
        CommandType::DischargePlanning(_) => "discharge planning".to_string(),
        CommandType::Disposition(_) => "disposition".to_string(),
        CommandType::Dosing(_) => "dosing".to_string(),
        CommandType::Drug(_) => "drug".to_string(),
        CommandType::Emerg(_) => "emerg".to_string(), 
        CommandType::Encounter(_) => "encounter".to_string(),
        CommandType::Education(_) => "education".to_string(),
        CommandType::Export(_) => "export".to_string(),
        CommandType::Facility(_) => "facility".to_string(),
        CommandType::Financial(_) => "financial".to_string(),
        CommandType::Imaging(_) => "imaging".to_string(),
        CommandType::Incident(_) => "incident".to_string(),
        CommandType::Lab(_) => "lab".to_string(),
        CommandType::Microbiology(_) => "microbiology".to_string(),
        CommandType::Metrics(_) => "metrics".to_string(),
        CommandType::Ml(_) => "ml".to_string(),
        CommandType::Model(_) => "model".to_string(),
        CommandType::Note(_) => "note".to_string(),
        CommandType::Nursing(_) => "nursing".to_string(),
        CommandType::Observation(_) => "observation".to_string(),
        CommandType::Order(_) => "order".to_string(),
        CommandType::Pathology(_) => "pathology".to_string(),
        CommandType::Patient(_) => "patient".to_string(),
        CommandType::Population(_) => "population".to_string(),
        CommandType::Prescription(_) => "prescription".to_string(),
        CommandType::Problem(_) => "problem".to_string(),
        CommandType::Procedure(_) => "procedure".to_string(),
        CommandType::Quality(_) => "quality".to_string(),
        CommandType::Radiation(_) => "radiation".to_string(),
        CommandType::Referral(_) => "referral".to_string(),
        CommandType::Research(_) => "research".to_string(),
        CommandType::Resus(_) => "resus".to_string(), 
        CommandType::Schedule(_) => "schedule".to_string(),
        CommandType::Surgery(_) => "surgery".to_string(),
        CommandType::Timing(_) => "timing".to_string(),
        CommandType::Triage(_) => "triage".to_string(),
        CommandType::Vitals(_) => "vitals".to_string(),
        CommandType::Mpi(_) => "mpi".to_string(),
    }
}
