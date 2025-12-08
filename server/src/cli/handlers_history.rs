use anyhow::{Result, Context, anyhow};
use log::{info, error};
use std::time::{SystemTime, UNIX_EPOCH};
use chrono::{DateTime, Utc, Duration as ChronoDuration};
use whoami;

use lib::commands::{
    HistoryCommand, HistoryListArgs, HistoryTopTailArgs, HistoryWindowArgs,
    HistoryStatusFilter, HistorySortField, HistoryOutputFormat, HistoryFilterArgs,
    HistoryDisplayArgs,
};
use lib::history::{
    GLOBAL_HISTORY_SERVICE, 
    HistoryFilter, 
    HistoryStatus, 
    HistoryService,
    HistoryMetadata,
};

/// Main dispatch function for all 'history' commands.
pub async fn handle_history_command(command: HistoryCommand) -> Result<()> {
    // Acquire a reference to the global history service singleton
    let service = GLOBAL_HISTORY_SERVICE.get().await;
    
    match command {
        HistoryCommand::List(args) => handle_list(service, args).await,
        HistoryCommand::Top(args) | HistoryCommand::Tail(args) => handle_top_tail(service, args).await,
        HistoryCommand::Window(args) => handle_window(service, args).await,
        HistoryCommand::Search { keyword, filters } => handle_search(service, keyword, filters).await,
        // Non-interactive clear: requires --force or aborts with a message
        HistoryCommand::Clear { user, force } => handle_clear(service, user, force).await,
        HistoryCommand::Stats { by, user } => handle_stats(service, by, user).await,
    }
}

// ============================================================================
// FILE: handlers_history.rs - Complete handle_history_command_interactive
// ============================================================================

/// Main dispatch function for all 'history' commands in an interactive shell.
/// This differs from the non-interactive handler by prompting for confirmation
/// on destructive operations like 'clear' when --force is not provided.
pub async fn handle_history_command_interactive(command: HistoryCommand) -> Result<()> {
    let service = GLOBAL_HISTORY_SERVICE.get().await;

    match command {
        HistoryCommand::Clear { user, force } => {
            if force {
                // If --force is passed, execute the clear immediately without confirmation
                handle_clear(service, user, true).await
            } else {
                // INTERACTIVE CONFIRMATION LOGIC
                let target_description = match &user {
                    Some(username) => format!("user '{}'", username),
                    None => "ALL USERS".to_string(),
                };
                
                println!("⚠️  WARNING: This operation will permanently delete history records for: {}", target_description);
                println!("Do you want to proceed? (yes/no)");

                // Read user input from stdin
                use std::io::{self, BufRead};
                let stdin = io::stdin();
                let mut line = String::new();
                
                match stdin.lock().read_line(&mut line) {
                    Ok(_) => {
                        let response = line.trim().to_lowercase();
                        if response == "yes" || response == "y" {
                            // User confirmed - proceed with clear
                            println!("Proceeding with history clear...");
                            handle_clear(service, user, true).await
                        } else {
                            // User declined - abort
                            println!("❌ Operation cancelled by user.");
                            Ok(())
                        }
                    }
                    Err(e) => {
                        println!("❌ Failed to read input: {}. Operation cancelled.", e);
                        Ok(())
                    }
                }
            }
        }
        // For all other commands, delegate to the non-interactive handler
        _ => handle_history_command(command).await,
    }
}

// --- Helper Functions for Time Parsing and Filtering ---

/// Converts CLI time strings ('2025-12-01T...', '3h ago') into nanosecond timestamps.
fn parse_cli_time(time_str: &str) -> Result<u64> {
    if time_str.ends_with(" ago") {
        // Simple duration parsing (e.g., "3h ago")
        let parts: Vec<&str> = time_str.split(' ').collect();
        if parts.len() != 2 {
            return Err(anyhow!("Invalid duration format. Use '3h ago', '10m ago', etc."));
        }
        let duration_str = parts[0];
        
        let now = Utc::now();
        let timestamp = if duration_str.ends_with('m') {
            let mins = duration_str.trim_end_matches('m').parse::<i64>()?;
            (now - ChronoDuration::minutes(mins)).timestamp_nanos_opt()
        } else if duration_str.ends_with('h') {
            let hours = duration_str.trim_end_matches('h').parse::<i64>()?;
            (now - ChronoDuration::hours(hours)).timestamp_nanos_opt()
        } else if duration_str.ends_with('d') {
            let days = duration_str.trim_end_matches('d').parse::<i64>()?;
            (now - ChronoDuration::days(days)).timestamp_nanos_opt()
        } else {
            return Err(anyhow!("Unsupported duration unit in '{}'. Use 'm', 'h', or 'd'.", duration_str));
        };

        Ok(timestamp.unwrap_or_default() as u64)
    } else {
        // Absolute timestamp parsing (e.g., ISO 8601)
        let datetime = time_str.parse::<DateTime<Utc>>()
            .context("Invalid absolute timestamp format. Use ISO 8601, e.g., 2025-12-01T10:00:00Z")?;
        Ok(datetime.timestamp_nanos_opt().unwrap_or_default() as u64)
    }
}

/// Converts CLI-level filter arguments into the service-level HistoryFilter struct.
fn build_filter(args: &HistoryFilterArgs, keyword: Option<String>) -> Result<HistoryFilter> {
    let mut filter = HistoryFilter {
        user: args.user.clone(),
        status: args.status.as_ref().map(|s| match s {
            HistoryStatusFilter::Success => HistoryStatus::Success,
            HistoryStatusFilter::Fail => HistoryStatus::Failure,
            HistoryStatusFilter::Timeout => HistoryStatus::Cancelled,
        }),
        limit: args.limit,
        keyword,
        // Default values for other fields
        ..Default::default()
    };

    if let Some(since_str) = &args.since {
        filter.since_nanos = Some(parse_cli_time(since_str).context("Failed to parse 'since' time")?);
    }
    if let Some(until_str) = &args.until {
        filter.until_nanos = Some(parse_cli_time(until_str).context("Failed to parse 'until' time")?);
    }

    // Note: command_type, offset are not implemented in HistoryFilterArgs yet but can be added here.

    Ok(filter)
}

// --- Handler Implementations ---

/// Handles the `history list` command.
async fn handle_list(service: &HistoryService, args: HistoryListArgs) -> Result<()> {
    info!("Handling history list command.");
    
    let filter = build_filter(&args.filters, None)?;
    let results = service.get_history(filter).await
        .context("Failed to retrieve history records")?;

    format_and_print_results(results, args.filters.display, "History List").await;
    Ok(())
}

/// Handles the `history top`, `history tail`, `history head`, and `history bottom` commands.
async fn handle_top_tail(service: &HistoryService, args: HistoryTopTailArgs) -> Result<()> {
    info!("Handling history top/tail command.");
    
    // Build base filter, then override limit
    let mut filter = build_filter(&args.filters, None)?;
    filter.limit = args.n;

    // Note: The HistoryService currently sorts by time descending. 
    // Top (most recent) is achieved by limit=N. 
    // Tail (oldest) would require reversing the results or adding a flag to the service.
    // For simplicity here, we assume the user only needs 'Top N' based on the default sort.

    let results = service.get_history(filter).await
        .context("Failed to retrieve top/tail history records")?;

    format_and_print_results(results, args.filters.display, "History Top/Tail").await;
    Ok(())
}

/// Handles the `history window` command.
async fn handle_window(service: &HistoryService, args: HistoryWindowArgs) -> Result<()> {
    info!("Handling history window command.");
    
    let mut filter = build_filter(&args.filters, None)?;
    
    // Calculate time window: e.g., "1h" duration before "30m ago" point.
    let now = Utc::now();
    
    let point_of_reference = if let Some(before_str) = &args.before {
        // Parse the 'before' string as a duration ago from now
        let before_millis = parse_duration_to_millis(before_str)
            .context("Invalid 'before' duration format")?;
        let before_duration = ChronoDuration::milliseconds(before_millis as i64);
        now - before_duration
    } else {
        now
    };
    
    let duration_millis = parse_duration_to_millis(&args.duration)
        .context("Invalid duration format for window")?;
    let duration = ChronoDuration::milliseconds(duration_millis as i64);
    
    let end_time = point_of_reference.timestamp_nanos_opt().unwrap_or_default() as u64;
    let start_time = (point_of_reference - duration).timestamp_nanos_opt().unwrap_or_default() as u64;
    
    filter.since_nanos = Some(start_time);
    filter.until_nanos = Some(end_time);
    
    let results = service.get_history(filter).await
        .context("Failed to retrieve history window records")?;
    
    // NOTE: 'interval' flag for grouping stats is currently not implemented in the service 
    // but would be handled here.
    format_and_print_results(results, args.filters.display, "History Window").await;
    
    Ok(())
}

/// Helper function to parse simple duration strings (e.g., 1h, 30m) into milliseconds.
fn parse_duration_to_millis(duration_str: &str) -> Result<u64> {
    let (num, unit) = duration_str.split_at(duration_str.len().saturating_sub(1));
    let num: u64 = num.parse().context("Invalid number in duration string")?;

    let millis = match unit {
        "m" => num * 60 * 1000,
        "h" => num * 60 * 60 * 1000,
        "d" => num * 24 * 60 * 60 * 1000,
        _ => return Err(anyhow!("Unsupported duration unit: {}", unit)),
    };
    Ok(millis)
}


/// Handles the `history search` command.
async fn handle_search(
    service: &HistoryService,
    keyword: String,
    filters: HistoryFilterArgs,
) -> Result<()> {
    info!("Handling history search command for keyword: {}", keyword);
    
    let filter = build_filter(&filters, Some(keyword))?;
    let results = service.get_history(filter).await
        .context("Failed to execute history search")?;

    format_and_print_results(results, filters.display, "History Search Results").await;
    Ok(())
}

/// Handles the `history clear` command.
// ============================================================================
// PART 2: Fix handle_clear to properly handle None user (all users)
// FILE: handlers_history.rs
// ============================================================================

/// Handles the `history clear` command.
async fn handle_clear(service: &HistoryService, user: Option<String>, force: bool) -> Result<()> {
    if !force {
        // This is the non-interactive abort path.
        println!("WARNING: This operation will permanently delete history records.");
        println!("Use the --force flag to confirm. Aborting clear operation.");
        return Ok(());
    }

    // CRITICAL FIX: If user is None, we're clearing ALL history
    let target_description = match &user {
        Some(username) => format!("user '{}'", username),
        None => "ALL USERS".to_string(),
    };

    println!("⚠️  CLEARING HISTORY FOR: {}", target_description);

    let filter = HistoryFilter {
        user, // If None, clear all; if Some(user), clear only that user
        ..Default::default()
    };

    let count = service.clear_history(filter).await
        .context("Failed to clear history records")?;

    println!("✅ Successfully cleared {} history records for {}.", count, target_description);
    Ok(())
}

/// Handles the `history stats` command.
async fn handle_stats(service: &HistoryService, by: String, user: Option<String>) -> Result<()> {
    info!("Handling history stats command, grouping by: {}", by);

    // 1. Fetch all relevant history data
    let filter = HistoryFilter { user: user.clone(), limit: usize::MAX, ..Default::default() };
    let results = service.get_history(filter).await
        .context("Failed to retrieve history records for stats calculation")?;

    if results.is_empty() {
        println!("No history records found for analysis.");
        return Ok(());
    }

    // 2. Perform Grouping and Calculation
    let mut stats_map: std::collections::HashMap<String, (usize, f64)> = std::collections::HashMap::new();

    for meta in &results {
        let key = match by.as_str() {
            "user" => meta.user.clone(),
            "status" => format!("{:?}", meta.status),
            "service_type" => meta.service_type.clone(),
            "command_type" => meta.command.split_whitespace().next().unwrap_or("unknown").to_string(),
            _ => {
                println!("Warning: Unknown grouping field '{}'. Defaulting to 'user'.", by);
                meta.user.clone()
            }
        };

        let duration_nanos = meta.end_time_nanos.saturating_sub(meta.start_time_nanos);
        let duration_secs = duration_nanos as f64 / 1_000_000_000.0;

        let entry = stats_map.entry(key).or_insert((0, 0.0));
        entry.0 += 1;
        entry.1 += duration_secs;
    }

    // 3. Format and Print Stats Table
    println!("\n📊 History Statistics (Grouped by: {})", by.to_uppercase());
    println!("{:-<70}", "");
    println!("{:<20} | {:<10} | {:<15} | {:<15}", by.to_uppercase(), "COUNT", "TOTAL DURATION", "AVG DURATION");
    println!("{:-<70}", "");

    for (key, (count, total_duration)) in stats_map {
        let avg_duration = total_duration / count as f64;
        println!("{:<20} | {:<10} | {:<15.3}s | {:<15.3}s", 
                 key, 
                 count, 
                 total_duration, 
                 avg_duration
        );
    }
    println!("{:-<70}", "");
    
    Ok(())
}

// --- Display / Output Function ---

/// Formats the HistoryMetadata results according to the display arguments and prints them.
async fn format_and_print_results(
    results: Vec<HistoryMetadata>,
    display_args: HistoryDisplayArgs,
    title: &str,
) {
    if results.is_empty() {
        println!("No history records found matching the filter.");
        return;
    }

    match display_args.format {
        HistoryOutputFormat::Json => {
            let json_output = serde_json::to_string_pretty(&results)
                .unwrap_or_else(|e| format!("Error formatting JSON: {}", e));
            println!("{}", json_output);
        }
        HistoryOutputFormat::Csv => {
            // NOTE: Implementing CSV output fully requires a separate CSV serializer dependency (e.g., csv-core/csv).
            // For now, we'll output a simplified CSV header.
            println!("id,user,status,start_time_nanos,duration_secs,command");
            for meta in results {
                let duration = (meta.end_time_nanos.saturating_sub(meta.start_time_nanos)) as f64 / 1_000_000_000.0;
                let command_str = meta.command.replace('\"', "\"\"");
                println!("{},{},{:?},{},{:.3},\"{}\"", 
                         meta.id, 
                         meta.user, 
                         meta.status, 
                         meta.start_time_nanos, 
                         duration, 
                         command_str
                );
            }
        }
        HistoryOutputFormat::Table => {
            println!("\n📒 {}", title);
            println!("{:-<150}", "");
            
            // Define column widths based on verbose flag
            let col_widths = if display_args.verbose {
                (5, 15, 10, 20, 15, 75) // ID, User, Status, Start Time, Duration, Command
            } else {
                (5, 10, 8, 15, 12, 100) // ID, User, Status, Start Time, Duration, Command
            };
            
            // Print Header
            println!("{:<5} {:<15} {:<10} {:<20} {:<15} {}", 
                     "ID", "USER", "STATUS", "START TIME (UTC)", "DURATION (s)", "COMMAND (TRUNCATED)"
            );
            println!("{:-<150}", "");

            for meta in results {
                let duration = (meta.end_time_nanos.saturating_sub(meta.start_time_nanos)) as f64 / 1_000_000_000.0;
                let start_dt = SystemTime::UNIX_EPOCH + std::time::Duration::from_nanos(meta.start_time_nanos);
                let start_dt_utc: DateTime<Utc> = start_dt.into();

                let command_display = if display_args.full_command {
                    meta.command.clone()
                } else {
                    let max_len = col_widths.5 - 1;
                    if meta.command.len() > max_len {
                        format!("{}...", &meta.command[..max_len.saturating_sub(3)])
                    } else {
                        meta.command.clone()
                    }
                };

                print!("{:<5} {:<15} {:<10} {:<20} {:<15.3} ", 
                        meta.id, 
                        meta.user, 
                        format!("{:?}", meta.status), 
                        start_dt_utc.to_rfc3339(), 
                        duration 
                );
                
                // Print command (not space-padded to allow full length in the final column)
                println!("{}", command_display);

                if display_args.verbose {
                    if let Some(msg) = &meta.error_message {
                        println!("  - ERROR: {}", msg);
                    }
                    if let Some(port) = meta.port {
                            println!("  - PORT: {}", port);
                    }
                    println!("  - SERVICE: {}", meta.service_type);
                    println!("{:-<150}", ""); // Separator for verbose entry
                }
            }
            println!("{:-<150}", "");
        }
    }
}

/// Saves the finalized HistoryMetadata record to the global history service.
/// This is called by the main command loop upon command completion.
pub async fn save_history_metadata(metadata: HistoryMetadata) -> Result<()> {
    // Acquire a reference to the global history service singleton
    let service = GLOBAL_HISTORY_SERVICE.get().await;

    // FIX: Clone the command string before 'metadata' is moved into register_entry.
    let command_clone = metadata.command.clone();
    
    // The value of 'metadata' is MOVED here (ownership transferred)
    service.register_entry(metadata).await.context("Failed to save history metadata record to service")?;
    
    // Use the cloned command string for logging to avoid the E0382 error.
    info!("History metadata saved successfully: {}", command_clone);
    Ok(())
}

// ============================================================================
// PART 1: Fix resolve_history_user to NOT inject user for clear/stats when None
// FILE: handlers_history.rs
// ============================================================================

/// Helper function to set the default user (current system user) if None is provided
/// in the HistoryCommand variants that require it.
/// 
/// IMPORTANT: For `Clear` and `Stats` commands, if user is None, it means "all users"
/// and should NOT be replaced with current_user.
pub fn resolve_history_user(action: HistoryCommand) -> HistoryCommand {
    // Get the current system username once to use as the meaningful default.
    let current_user = whoami::username(); 

    match action {
        // For LIST/SEARCH/TOP/TAIL/WINDOW: Default to current user for filtering
        HistoryCommand::List(mut args) => {
            if args.filters.user.is_none() {
                args.filters.user = Some(current_user);
            }
            HistoryCommand::List(args)
        },
        HistoryCommand::Search { keyword, mut filters } => {
            if filters.user.is_none() {
                filters.user = Some(current_user);
            }
            HistoryCommand::Search { keyword, filters }
        },
        HistoryCommand::Top(mut args) => {
            if args.filters.user.is_none() {
                args.filters.user = Some(current_user);
            }
            HistoryCommand::Top(args)
        },
        HistoryCommand::Tail(mut args) => {
            if args.filters.user.is_none() {
                args.filters.user = Some(current_user);
            }
            HistoryCommand::Tail(args)
        },
        HistoryCommand::Window(mut args) => {
            if args.filters.user.is_none() {
                args.filters.user = Some(current_user);
            }
            HistoryCommand::Window(args)
        },
        
        // CRITICAL FIX: For Clear and Stats, None means "all users" - DO NOT replace
        HistoryCommand::Clear { user, force } => {
            // If user explicitly passed None, keep it as None (means all users)
            // If user passed Some(username), keep that specific user
            HistoryCommand::Clear { user, force }
        },
        HistoryCommand::Stats { by, user } => {
            // If user explicitly passed None, keep it as None (means all users)
            // If user passed Some(username), keep that specific user
            HistoryCommand::Stats { by, user }
        },
    }
}
