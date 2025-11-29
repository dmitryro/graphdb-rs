// server/src/cli/interactive.rs
use anyhow::{Result, Context, anyhow};
use rustyline::error::ReadlineError;
use rustyline::{DefaultEditor, history::DefaultHistory};
use std::sync::Arc;
use tokio::sync::{oneshot, Mutex as TokioMutex};
use std::collections::HashMap;
use tokio::task::JoinHandle;
use std::process;
use std::path::PathBuf;
use clap::CommandFactory;
use std::collections::HashSet;
use shlex;
use log::{info, error, warn, debug};
use uuid::Uuid;
use chrono::{DateTime, Utc};
use crate::cli::cli::{CliArgs, get_query_engine_singleton};
use lib::commands::*;
use crate::cli::handlers;
use crate::cli::help_display::{
    print_interactive_help, print_interactive_filtered_help, collect_all_cli_elements_for_suggestions,
    print_help_clap_generated, print_filtered_help_clap_generated
};
use crate::cli::handlers_utils::{parse_show_command};
pub use lib::config::{StorageEngineType};
use lib::query_exec_engine::query_exec_engine::QueryExecEngine;

struct SharedState {
    daemon_handles: Arc<TokioMutex<HashMap<u16, (JoinHandle<()>, oneshot::Sender<()>)>>>,
    rest_api_shutdown_tx_opt: Arc<TokioMutex<Option<oneshot::Sender<()>>>>,
    rest_api_port_arc: Arc<TokioMutex<Option<u16>>>,
    rest_api_handle: Arc<TokioMutex<Option<JoinHandle<()>>>>,
    storage_daemon_shutdown_tx_opt: Arc<TokioMutex<Option<oneshot::Sender<()>>>>,
    storage_daemon_handle: Arc<TokioMutex<Option<JoinHandle<()>>>>,
    storage_daemon_port_arc: Arc<TokioMutex<Option<u16>>>,
    query_engine: Arc<TokioMutex<Option<Arc<QueryExecEngine>>>>,
}

// === Levenshtein distance for fuzzy matching ===
fn levenshtein_distance(s1: &str, s2: &str) -> usize {
    let s1_chars: Vec<char> = s1.chars().collect();
    let s2_chars: Vec<char> = s2.chars().collect();
    let m = s1_chars.len();
    let n = s2_chars.len();
    if m == 0 { return n; }
    if n == 0 { return m; }
    let mut dp = vec![vec![0; n + 1]; m + 1];
    for i in 0..=m { dp[i][0] = i; }
    for j in 0..=n { dp[0][j] = j; }
    for i in 1..=m {
        for j in 1..=n {
            let cost = if s1_chars[i - 1] == s2_chars[j - 1] { 0 } else { 1 };
            dp[i][j] = (dp[i - 1][j] + 1)
                .min(dp[i][j - 1] + 1)
                .min(dp[i - 1][j - 1] + cost);
        }
    }
    dp[m][n]
}

// === Helper: Strip surrounding double quotes ===
fn strip_surrounding_quotes(s: &str) -> String {
    let trimmed = s.trim();
    if trimmed.len() >= 2 && trimmed.starts_with('"') && trimmed.ends_with('"') {
        trimmed[1..trimmed.len() - 1].to_owned()
    } else {
        trimmed.to_owned()
    }
}

// === RAW QUERY AUTO-DETECTION (SQL / Cypher / GraphQL) ===
fn detect_raw_query_language(line: &str) -> Option<(&'static str, String)> {
    let trimmed = line.trim();
    if trimmed.is_empty() {
        return None;
    }
    let upper = trimmed.to_ascii_uppercase();
    let lower = trimmed.to_ascii_lowercase();

    // ---- SQL ----
    // Only match full SQL patterns: SELECT, INSERT, UPDATE, WITH, or DELETE FROM
    if upper.starts_with("SELECT ")
        || upper.starts_with("INSERT ")
        || upper.starts_with("UPDATE ")
        || upper.starts_with("WITH ")
        || upper.starts_with("CREATE ")
        || upper.starts_with("DROP ")
        || upper.starts_with("ALTER ")
        || upper.contains(" DELETE FROM ")
        || upper.starts_with("DELETE FROM ")
    {
        return Some(("sql", trimmed.to_string()));
    }

    // ---- Cypher ----
    if lower.starts_with("match ")
        || lower.starts_with("create ")
        || lower.starts_with("merge ")
        || lower.starts_with("return ")
        || lower.starts_with("optional match ")
        || lower.starts_with("detach delete ")
        || lower.starts_with("remove ")
        || lower.starts_with("where ")
        || lower.starts_with("order by ")
        || lower.starts_with("limit ")
    {
        return Some(("cypher", trimmed.to_string()));
    }

    // ---- GraphQL ----
    if trimmed.starts_with('{')
        || lower.starts_with("query ")
        || lower.starts_with("mutation ")
    {
        return Some(("graphql", trimmed.to_string()));
    }

    None
}

// === Main command parser (raw queries are detected BEFORE parsing) ===
pub fn parse_command(parts: &[String]) -> (CommandType, Vec<String>) {
    if parts.is_empty() {
        return (CommandType::Unknown, Vec::new());
    }

    // Join back the parts – we need the original line for raw-query detection
    let full_line = parts.join(" ");

    // ---- RAW QUERY SHORT-CIRCUIT ----
    if let Some((lang, query)) = detect_raw_query_language(&full_line) {
        return (
            CommandType::Query {
                query,
                language: Some(lang.to_string()),
            },
            Vec::new(),
        );
    }

    // Normal CLI command parsing
    let command_str = parts[0].to_lowercase();
    let remaining_args = parts[1..].to_vec();

    let top_level_commands = vec![
        "start", "stop", "status", "auth", "authenticate", "register", "version", "health",
        "reload", "restart", "clear", "help", "exit", "daemon", "rest", "storage", "use",
        "quit", "q", "clean", "save", "show", "kv", "query", "exec", "migrate", "visualize",
        // NEW: Add graph and index for tab-completion & fuzzy help
        "graph", "index",
        // === NEW CLINICAL COMMANDS ===
        "patient", "encounter", "diagnosis", "prescription", "note", "procedure",
        "triage", "disposition", "referral", "vitals", "observation", "lab", "imaging",
        "chemo", "radiation", "surgery", "nursing", "education", "discharge-planning",
        "discharge", "quality", "incident", "compliance", "population", "analytics",
        "metrics", "research", "ml", "clinical-trial", "facility", "access", "financial",
        "alert", "pathology", "microbiology", "dosing", "model",
    ];
    const FUZZY_MATCH_THRESHOLD: usize = 2;

    let cmd_type = match command_str.as_str() {
        "exit" | "quit" | "q" => CommandType::Exit,
        "clear" | "clean" => CommandType::Clear,
        "version" => CommandType::Version,
        "health" => CommandType::Health,
        "save" => {
            if remaining_args.is_empty() {
                eprintln!("Usage: save [storage|config|configuration]");
                CommandType::Unknown
            } else {
                match remaining_args[0].to_lowercase().as_str() {
                    "storage" => CommandType::SaveStorage,
                    "config" | "configuration" => CommandType::SaveConfig,
                    _ => {
                        eprintln!("Usage: save [storage|config|configuration]");
                        CommandType::Unknown
                    }
                }
            }
        },
        "status" => {
            if remaining_args.is_empty() {
                CommandType::StatusSummary
            } else {
                match remaining_args[0].to_lowercase().as_str() {
                    "summary" | "all" => CommandType::StatusSummary,
                    "rest" => {
                        let mut port = None;
                        let mut cluster = None;
                        let mut i = 1;
                        while i < remaining_args.len() {
                            match remaining_args[i].to_lowercase().as_str() {
                                "--port" | "-p" => {
                                    if i + 1 < remaining_args.len() {
                                        port = remaining_args[i + 1].parse::<u16>().ok();
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", remaining_args[i]);
                                        i += 1;
                                    }
                                }
                                "--cluster" => {
                                    if i + 1 < remaining_args.len() {
                                        cluster = Some(remaining_args[i + 1].clone());
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", remaining_args[i]);
                                        i += 1;
                                    }
                                }
                                _ => { i += 1; }
                            }
                        }
                        CommandType::Rest(RestCliCommand::Status { port, cluster })
                    },
                    "daemon" => {
                        let mut port = None;
                        let mut cluster = None;
                        let mut i = 1;
                        while i < remaining_args.len() {
                            match remaining_args[i].to_lowercase().as_str() {
                                "--port" | "-p" => {
                                    if i + 1 < remaining_args.len() {
                                        port = remaining_args[i + 1].parse::<u16>().ok();
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", remaining_args[i]);
                                        i += 1;
                                    }
                                }
                                "--cluster" => {
                                    if i + 1 < remaining_args.len() {
                                        cluster = Some(remaining_args[i + 1].clone());
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", remaining_args[i]);
                                        i += 1;
                                    }
                                }
                                _ => { i += 1; }
                            }
                        }
                        CommandType::Daemon(DaemonCliCommand::Status { port, cluster })
                    },
                    "storage" => {
                        let mut port = None;
                        let mut cluster = None;
                        let mut i = 1;
                        while i < remaining_args.len() {
                            match remaining_args[i].to_lowercase().as_str() {
                                "--port" | "-p" => {
                                    if i + 1 < remaining_args.len() {
                                        port = remaining_args[i + 1].parse::<u16>().ok();
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", remaining_args[i]);
                                        i += 1;
                                    }
                                }
                                "--cluster" => {
                                    if i + 1 < remaining_args.len() {
                                        cluster = Some(remaining_args[i + 1].clone());
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", remaining_args[i]);
                                        i += 1;
                                    }
                                }
                                _ => { i += 1; }
                            }
                        }
                        CommandType::Storage(StorageAction::Status { port, cluster })
                    },
                    "cluster" => CommandType::StatusCluster,
                    "raft" => {
                        let mut port = None;
                        let mut i = 1;
                        while i < remaining_args.len() {
                            match remaining_args[i].to_lowercase().as_str() {
                                "--port" | "-p" => {
                                    if i + 1 < remaining_args.len() {
                                        port = remaining_args[i + 1].parse::<u16>().ok();
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", remaining_args[i]);
                                        i += 1;
                                    }
                                }
                                _ => { i += 1; }
                            }
                        }
                        CommandType::StatusRaft(port)
                    },
                    _ => CommandType::Unknown,
                }
            }
        },
        "start" => {
            let mut port: Option<u16> = None;
            let mut cluster: Option<String> = None;
            let mut daemon_port: Option<u16> = None;
            let mut daemon_cluster: Option<String> = None;
            let mut listen_port: Option<u16> = None;
            let mut rest_port: Option<u16> = None;
            let mut rest_cluster: Option<String> = None;
            let mut storage_port: Option<u16> = None;
            let mut storage_cluster: Option<String> = None;
            let mut storage_config_file: Option<PathBuf> = None;
            let mut current_subcommand_index = 0;
            let mut explicit_subcommand: Option<String> = None;
            if !remaining_args.is_empty() {
                match remaining_args[0].to_lowercase().as_str() {
                    "all" | "daemon" | "rest" | "storage" => {
                        explicit_subcommand = Some(remaining_args[0].to_lowercase());
                        current_subcommand_index = 1;
                    }
                    _ => { current_subcommand_index = 0; }
                }
            }
            let mut i = current_subcommand_index;
            while i < remaining_args.len() {
                match remaining_args[i].to_lowercase().as_str() {
                    "--port" | "-p" => {
                        if i + 1 < remaining_args.len() {
                            port = remaining_args[i + 1].parse::<u16>().ok();
                            i += 2;
                        } else {
                            eprintln!("Warning: Flag '{}' requires a value.", remaining_args[i]);
                            i += 1;
                        }
                    }
                    "--cluster" => {
                        if i + 1 < remaining_args.len() {
                            cluster = Some(remaining_args[i + 1].clone());
                            i += 2;
                        } else {
                            eprintln!("Warning: Flag '{}' requires a value.", remaining_args[i]);
                            i += 1;
                        }
                    }
                    "--daemon-port" => {
                        if i + 1 < remaining_args.len() {
                            daemon_port = remaining_args[i + 1].parse::<u16>().ok();
                            i += 2;
                        } else {
                            eprintln!("Warning: Flag '{}' requires a value.", remaining_args[i]);
                            i += 1;
                        }
                    }
                    "--daemon-cluster" => {
                        if i + 1 < remaining_args.len() {
                            daemon_cluster = Some(remaining_args[i + 1].clone());
                            i += 2;
                        } else {
                            eprintln!("Warning: Flag '{}' requires a value.", remaining_args[i]);
                            i += 1;
                        }
                    }
                    "--listen-port" => {
                        if i + 1 < remaining_args.len() {
                            listen_port = remaining_args[i + 1].parse::<u16>().ok();
                            i += 2;
                        } else {
                            eprintln!("Warning: Flag '{}' requires a value.", remaining_args[i]);
                            i += 1;
                        }
                    }
                    "--rest-port" => {
                        if i + 1 < remaining_args.len() {
                            rest_port = remaining_args[i + 1].parse::<u16>().ok();
                            i += 2;
                        } else {
                            eprintln!("Warning: Flag '{}' requires a value.", remaining_args[i]);
                            i += 1;
                        }
                    }
                    "--rest-cluster" => {
                        if i + 1 < remaining_args.len() {
                            rest_cluster = Some(remaining_args[i + 1].clone());
                            i += 2;
                        } else {
                            eprintln!("Warning: Flag '{}' requires a value.", remaining_args[i]);
                            i += 1;
                        }
                    }
                    "--storage-port" => {
                        if i + 1 < remaining_args.len() {
                            storage_port = remaining_args[i + 1].parse::<u16>().ok();
                            i += 2;
                        } else {
                            eprintln!("Warning: Flag '{}' requires a value.", remaining_args[i]);
                            i += 1;
                        }
                    }
                    "--storage-cluster" => {
                        if i + 1 < remaining_args.len() {
                            storage_cluster = Some(remaining_args[i + 1].clone());
                            i += 2;
                        } else {
                            eprintln!("Warning: Flag '{}' requires a value.", remaining_args[i]);
                            i += 1;
                        }
                    }
                    "--storage-config" | "--config-file" => {
                        if i + 1 < remaining_args.len() {
                            storage_config_file = Some(PathBuf::from(remaining_args[i + 1].clone()));
                            i += 2;
                        } else {
                            eprintln!("Warning: Flag '{}' requires a value.", remaining_args[i]);
                            i += 1;
                        }
                    }
                    _ => {
                        eprintln!("Warning: Unknown argument for 'start': {}", remaining_args[i]);
                        i += 1;
                    }
                }
            }
            match explicit_subcommand.as_deref() {
                Some("all") => CommandType::StartAll {
                    port, cluster, daemon_port, daemon_cluster, listen_port,
                    rest_port, rest_cluster, storage_port, storage_cluster, storage_config_file,
                },
                Some("daemon") => CommandType::StartDaemon {
                    port: Some(daemon_port.unwrap_or(port.unwrap_or_default())),
                    cluster: daemon_cluster.clone().or(cluster.clone()),
                    daemon_port, daemon_cluster,
                },
                Some("rest") => CommandType::StartRest {
                    port: Some(rest_port.unwrap_or(listen_port.unwrap_or(port.unwrap_or_default()))),
                    cluster: rest_cluster.clone().or(cluster.clone()),
                    rest_port, rest_cluster,
                },
                Some("storage") => CommandType::StartStorage {
                    port: Some(storage_port.unwrap_or(port.unwrap_or_default())),
                    config_file: storage_config_file,
                    cluster: storage_cluster.clone().or(cluster.clone()),
                    storage_port, storage_cluster,
                },
                None => CommandType::StartAll {
                    port, cluster, daemon_port, daemon_cluster, listen_port,
                    rest_port, rest_cluster, storage_port, storage_cluster, storage_config_file,
                },
                _ => CommandType::Unknown,
            }
        },
        "stop" => {
            if remaining_args.is_empty() || remaining_args[0].to_lowercase() == "all" {
                CommandType::StopAll
            } else {
                match remaining_args[0].to_lowercase().as_str() {
                    "rest" => {
                        let mut port = None;
                        let mut i = 1;
                        while i < remaining_args.len() {
                            match remaining_args[i].to_lowercase().as_str() {
                                "--port" | "-p" => {
                                    if i + 1 < remaining_args.len() {
                                        port = remaining_args[i + 1].parse::<u16>().ok();
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", remaining_args[i]);
                                        i += 1;
                                    }
                                }
                                _ => {
                                    eprintln!("Warning: Unknown argument for 'stop rest': {}", remaining_args[i]);
                                    i += 1;
                                }
                            }
                        }
                        CommandType::StopRest(port)
                    }
                    "daemon" => {
                        let mut port = None;
                        let mut i = 1;
                        while i < remaining_args.len() {
                            match remaining_args[i].to_lowercase().as_str() {
                                "--port" | "-p" => {
                                    if i + 1 < remaining_args.len() {
                                        port = remaining_args[i + 1].parse::<u16>().ok();
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", remaining_args[i]);
                                        i += 1;
                                    }
                                }
                                _ => { i += 1; }
                            }
                        }
                        CommandType::StopDaemon(port)
                    }
                    "storage" => {
                        let mut port = None;
                        let mut i = 1;
                        while i < remaining_args.len() {
                            match remaining_args[i].to_lowercase().as_str() {
                                "--port" | "-p" => {
                                    if i + 1 < remaining_args.len() {
                                        port = remaining_args[i + 1].parse::<u16>().ok();
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", remaining_args[i]);
                                        i += 1;
                                    }
                                }
                                _ => { i += 1; }
                            }
                        }
                        CommandType::StopStorage(port)
                    }
                    "all" => CommandType::StopAll,
                    _ => CommandType::Unknown,
                }
            }
        },
        "reload" => {
            if remaining_args.is_empty() {
                eprintln!("Usage: reload [all|rest|storage|daemon|cluster] [--port ]");
                CommandType::Unknown
            } else {
                match remaining_args[0].to_lowercase().as_str() {
                    "all" => CommandType::ReloadAll,
                    "rest" => CommandType::ReloadRest,
                    "storage" => CommandType::ReloadStorage,
                    "daemon" => {
                        let mut port = None;
                        let mut i = 1;
                        while i < remaining_args.len() {
                            match remaining_args[i].to_lowercase().as_str() {
                                "--port" | "-p" => {
                                    if i + 1 < remaining_args.len() {
                                        port = remaining_args[i + 1].parse::<u16>().ok();
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", remaining_args[i]);
                                        i += 1;
                                    }
                                }
                                _ => { i += 1; }
                            }
                        }
                        CommandType::ReloadDaemon(port)
                    },
                    "cluster" => CommandType::ReloadCluster,
                    _ => CommandType::Unknown,
                }
            }
        },
        "restart" => {
            if remaining_args.is_empty() {
                eprintln!("Usage: restart [all|rest|storage|daemon|cluster] ...");
                CommandType::Unknown
            } else {
                match remaining_args[0].to_lowercase().as_str() {
                    "all" => {
                        let mut port = None;
                        let mut cluster = None;
                        let mut listen_port = None;
                        let mut storage_port = None;
                        let mut storage_config_file = None;
                        let mut daemon_cluster = None;
                        let mut daemon_port = None;
                        let mut rest_cluster = None;
                        let mut rest_port = None;
                        let mut storage_cluster = None;
                        let mut i = 1;
                        while i < remaining_args.len() {
                            match remaining_args[i].to_lowercase().as_str() {
                                "--port" | "-p" => {
                                    if i + 1 < remaining_args.len() {
                                        port = remaining_args[i + 1].parse::<u16>().ok();
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", remaining_args[i]);
                                        i += 1;
                                    }
                                }
                                "--cluster" => {
                                    if i + 1 < remaining_args.len() {
                                        cluster = Some(remaining_args[i + 1].clone());
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", remaining_args[i]);
                                        i += 1;
                                    }
                                }
                                "--listen-port" => {
                                    if i + 1 < remaining_args.len() {
                                        listen_port = remaining_args[i + 1].parse::<u16>().ok();
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", remaining_args[i]);
                                        i += 1;
                                    }
                                }
                                "--storage-port" => {
                                    if i + 1 < remaining_args.len() {
                                        storage_port = remaining_args[i + 1].parse::<u16>().ok();
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", remaining_args[i]);
                                        i += 1;
                                    }
                                }
                                "--storage-config" => {
                                    if i + 1 < remaining_args.len() {
                                        storage_config_file = Some(PathBuf::from(&remaining_args[i + 1]));
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", remaining_args[i]);
                                        i += 1;
                                    }
                                }
                                "--daemon-port" => {
                                    if i + 1 < remaining_args.len() {
                                        daemon_port = remaining_args[i + 1].parse::<u16>().ok();
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", remaining_args[i]);
                                        i += 1;
                                    }
                                }
                                "--daemon-cluster" => {
                                    if i + 1 < remaining_args.len() {
                                        daemon_cluster = Some(remaining_args[i + 1].clone());
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", remaining_args[i]);
                                        i += 1;
                                    }
                                }
                                "--rest-port" => {
                                    if i + 1 < remaining_args.len() {
                                        rest_port = remaining_args[i + 1].parse::<u16>().ok();
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", remaining_args[i]);
                                        i += 1;
                                    }
                                }
                                "--rest-cluster" => {
                                    if i + 1 < remaining_args.len() {
                                        rest_cluster = Some(remaining_args[i + 1].clone());
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", remaining_args[i]);
                                        i += 1;
                                    }
                                }
                                "--storage-cluster" => {
                                    if i + 1 < remaining_args.len() {
                                        storage_cluster = Some(remaining_args[i + 1].clone());
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", remaining_args[i]);
                                        i += 1;
                                    }
                                }
                                _ => {
                                    eprintln!("Warning: Unknown argument for 'restart all': {}", remaining_args[i]);
                                    i += 1;
                                }
                            }
                        }
                        CommandType::RestartAll {
                            port, cluster, listen_port, storage_port, storage_config_file,
                            daemon_cluster, daemon_port, rest_cluster, rest_port, storage_cluster,
                        }
                    },
                    "rest" => {
                        let mut port = None;
                        let mut cluster = None;
                        let mut rest_port = None;
                        let mut rest_cluster = None;
                        let mut i = 1;
                        while i < remaining_args.len() {
                            match remaining_args[i].to_lowercase().as_str() {
                                "--port" | "-p" | "--listen-port" => {
                                    if i + 1 < remaining_args.len() {
                                        port = remaining_args[i + 1].parse::<u16>().ok();
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", remaining_args[i]);
                                        i += 1;
                                    }
                                }
                                "--cluster" => {
                                    if i + 1 < remaining_args.len() {
                                        cluster = Some(remaining_args[i + 1].clone());
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", remaining_args[i]);
                                        i += 1;
                                    }
                                }
                                "--rest-port" => {
                                    if i + 1 < remaining_args.len() {
                                        rest_port = remaining_args[i + 1].parse::<u16>().ok();
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", remaining_args[i]);
                                        i += 1;
                                    }
                                }
                                "--rest-cluster" => {
                                    if i + 1 < remaining_args.len() {
                                        rest_cluster = Some(remaining_args[i + 1].clone());
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", remaining_args[i]);
                                        i += 1;
                                    }
                                }
                                _ => {
                                    eprintln!("Warning: Unknown argument for 'restart rest': {}", remaining_args[i]);
                                    i += 1;
                                }
                            }
                        }
                        CommandType::RestartRest {
                            port: Some(rest_port.unwrap_or(port.unwrap_or_default())),
                            cluster: rest_cluster.clone().or(cluster.clone()),
                            rest_port, rest_cluster
                        }
                    },
                    "storage" => {
                        let mut port = None;
                        let mut config_file = None;
                        let mut cluster = None;
                        let mut storage_port = None;
                        let mut storage_cluster = None;
                        let mut i = 1;
                        while i < remaining_args.len() {
                            match remaining_args[i].to_lowercase().as_str() {
                                "--port" | "-p" => {
                                    if i + 1 < remaining_args.len() {
                                        port = remaining_args[i + 1].parse::<u16>().ok();
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", remaining_args[i]);
                                        i += 1;
                                    }
                                }
                                "--config-file" => {
                                    if i + 1 < remaining_args.len() {
                                        config_file = Some(PathBuf::from(&remaining_args[i + 1]));
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", remaining_args[i]);
                                        i += 1;
                                    }
                                }
                                "--cluster" => {
                                    if i + 1 < remaining_args.len() {
                                        cluster = Some(remaining_args[i + 1].clone());
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", remaining_args[i]);
                                        i += 1;
                                    }
                                }
                                "--storage-port" => {
                                    if i + 1 < remaining_args.len() {
                                        storage_port = remaining_args[i + 1].parse::<u16>().ok();
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", remaining_args[i]);
                                        i += 1;
                                    }
                                }
                                "--storage-cluster" => {
                                    if i + 1 < remaining_args.len() {
                                        storage_cluster = Some(remaining_args[i + 1].clone());
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", remaining_args[i]);
                                        i += 1;
                                    }
                                }
                                _ => {
                                    eprintln!("Warning: Unknown argument for 'restart storage': {}", remaining_args[i]);
                                    i += 1;
                                }
                            }
                        }
                        CommandType::RestartStorage {
                            port: Some(storage_port.unwrap_or(port.unwrap_or_default())),
                            config_file,
                            cluster: storage_cluster.clone().or(cluster.clone()),
                            storage_port,
                            storage_cluster
                        }
                    },
                    "daemon" => {
                        let mut port = None;
                        let mut cluster = None;
                        let mut daemon_port = None;
                        let mut daemon_cluster = None;
                        let mut i = 1;
                        while i < remaining_args.len() {
                            match remaining_args[i].to_lowercase().as_str() {
                                "--port" | "-p" => {
                                    if i + 1 < remaining_args.len() {
                                        port = remaining_args[i + 1].parse::<u16>().ok();
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", remaining_args[i]);
                                        i += 1;
                                    }
                                }
                                "--cluster" => {
                                    if i + 1 < remaining_args.len() {
                                        cluster = Some(remaining_args[i + 1].clone());
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", remaining_args[i]);
                                        i += 1;
                                    }
                                }
                                "--daemon-port" => {
                                    if i + 1 < remaining_args.len() {
                                        daemon_port = remaining_args[i + 1].parse::<u16>().ok();
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", remaining_args[i]);
                                        i += 1;
                                    }
                                }
                                "--daemon-cluster" => {
                                    if i + 1 < remaining_args.len() {
                                        daemon_cluster = Some(remaining_args[i + 1].clone());
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '{}' requires a value.", remaining_args[i]);
                                        i += 1;
                                    }
                                }
                                _ => {
                                    eprintln!("Warning: Unknown argument for 'restart daemon': {}", remaining_args[i]);
                                    i += 1;
                                }
                            }
                        }
                        CommandType::RestartDaemon {
                            port: Some(daemon_port.unwrap_or(port.unwrap_or_default())),
                            cluster: daemon_cluster.clone().or(cluster.clone()),
                            daemon_port,
                            daemon_cluster
                        }
                    },
                    "cluster" => CommandType::RestartCluster,
                    _ => CommandType::Unknown,
                }
            }
        },
        "auth" | "authenticate" => {
            if remaining_args.len() >= 2 {
                CommandType::Authenticate {
                    username: remaining_args[0].clone(),
                    password: remaining_args[1].clone()
                }
            } else {
                eprintln!("Usage: auth/authenticate <username> <password>");
                CommandType::Unknown
            }
        },
        "register" => {
            if remaining_args.len() >= 2 {
                CommandType::RegisterUser {
                    username: remaining_args[0].clone(),
                    password: remaining_args[1].clone()
                }
            } else {
                eprintln!("Usage: register <username> <password>");
                CommandType::Unknown
            }
        },
        "use" => {
            if remaining_args.is_empty() {
                eprintln!("Usage: use [storage [--permanent] [--migrate]|plugin [--enable <bool>]]");
                CommandType::Unknown
            } else {
                match remaining_args[0].to_lowercase().as_str() {
                    "storage" => {
                        if remaining_args.len() < 2 {
                            eprintln!("Usage: use storage <engine> [--permanent] [--migrate]");
                            CommandType::Unknown
                        } else {
                            let engine = match remaining_args[1].to_lowercase().as_str() {
                                "sled" => StorageEngineType::Sled,
                                "rocksdb" | "rocks-db" => StorageEngineType::RocksDB,
                                "inmemory" | "in-memory" => StorageEngineType::InMemory,
                                "redis" => StorageEngineType::Redis,
                                "tikv" => StorageEngineType::TiKV,
                                "postgres" | "postgresql" | "postgre-sql" => StorageEngineType::PostgreSQL,
                                "mysql" | "my-sql" => StorageEngineType::MySQL,
                                _ => {
                                    eprintln!(
                                        "Unknown storage engine: {}. Supported: sled, rocksdb, rocks-db, inmemory, in-memory, redis, postgres, postgresql, postgre-sql, mysql, my-sql",
                                        remaining_args[1]
                                    );
                                    return (CommandType::Unknown, remaining_args.clone());
                                }
                            };
                            let mut permanent = false;
                            let mut migrate = false;
                            let mut i = 2;
                            while i < remaining_args.len() {
                                match remaining_args[i].to_lowercase().as_str() {
                                    "--permanent" => { permanent = true; i += 1; }
                                    "--migrate" => { migrate = true; i += 1; }
                                    _ => {
                                        eprintln!("Warning: Unknown argument for 'use storage': {}", remaining_args[i]);
                                        i += 1;
                                    }
                                }
                            }
                            CommandType::UseStorage { engine, permanent, migrate }
                        }
                    },
                    "plugin" => {
                        let mut enable = None;
                        let mut i = 1;
                        while i < remaining_args.len() {
                            match remaining_args[i].to_lowercase().as_str() {
                                "--enable" => {
                                    if i + 1 < remaining_args.len() {
                                        enable = remaining_args[i + 1].parse::<bool>().ok();
                                        i += 2;
                                    } else {
                                        eprintln!("Warning: Flag '--enable' requires a boolean value.");
                                        i += 1;
                                    }
                                }
                                _ => {
                                    eprintln!("Warning: Unknown argument for 'use plugin': {}", remaining_args[i]);
                                    i += 1;
                                }
                            }
                        }
                        CommandType::UsePlugin { enable: enable.unwrap_or(true) }
                    },
                    _ => {
                        eprintln!("Usage: use [storage <engine> [--permanent]|plugin [--enable <bool>]]");
                        CommandType::Unknown
                    }
                }
            }
        },
        "help" => {
            let mut filter_command: Option<String> = None;
            let mut command_path: Vec<String> = Vec::new();
            let mut i = 0;
            while i < remaining_args.len() {
                match remaining_args[i].to_lowercase().as_str() {
                    "--filter" | "-f" | "--command" | "-c" => {
                        if i + 1 < remaining_args.len() {
                            filter_command = Some(remaining_args[i + 1].clone());
                            i += 2;
                        } else {
                            eprintln!("Warning: Flag '{}' requires a value.", remaining_args[i]);
                            i += 1;
                        }
                    },
                    _ => {
                        command_path.push(remaining_args[i].clone());
                        i += 1;
                    }
                }
            }
            let help_args = HelpArgs { filter_command, command_path };
            CommandType::Help(help_args)
        },
        "daemon" => {
            if remaining_args.first().map_or(false, |s| s.to_lowercase() == "list") {
                CommandType::Daemon(DaemonCliCommand::List)
            } else if remaining_args.first().map_or(false, |s| s.to_lowercase() == "clear-all") {
                CommandType::Daemon(DaemonCliCommand::ClearAll)
            } else if remaining_args.first().map_or(false, |s| s.to_lowercase() == "start") {
                let mut port = None;
                let mut cluster = None;
                let mut daemon_port = None;
                let mut daemon_cluster = None;
                let mut i = 1;
                while i < remaining_args.len() {
                    match remaining_args[i].to_lowercase().as_str() {
                        "--port" | "-p" => {
                            if i + 1 < remaining_args.len() {
                                port = remaining_args[i + 1].parse::<u16>().ok();
                                i += 2;
                            } else {
                                eprintln!("Warning: Flag '{}' requires a value.", remaining_args[i]);
                                i += 1;
                            }
                        }
                        "--cluster" => {
                            if i + 1 < remaining_args.len() {
                                cluster = Some(remaining_args[i + 1].clone());
                                i += 2;
                            } else {
                                eprintln!("Warning: Flag '{}' requires a value.", remaining_args[i]);
                                i += 1;
                            }
                        }
                        "--daemon-port" => {
                            if i + 1 < remaining_args.len() {
                                daemon_port = remaining_args[i + 1].parse::<u16>().ok();
                                i += 2;
                            } else {
                                eprintln!("Warning: Flag '{}' requires a value.", remaining_args[i]);
                                i += 1;
                            }
                        }
                        "--daemon-cluster" => {
                            if i + 1 < remaining_args.len() {
                                daemon_cluster = Some(remaining_args[i + 1].clone());
                                i += 2;
                            } else {
                                eprintln!("Warning: Flag '{}' requires a value.", remaining_args[i]);
                                i += 1;
                            }
                        }
                        _ => { i += 1; }
                    }
                }
                CommandType::Daemon(DaemonCliCommand::Start {
                    port: Some(daemon_port.unwrap_or(port.unwrap_or_default())),
                    cluster: daemon_cluster.clone().or(cluster.clone()),
                    daemon_port,
                    daemon_cluster
                })
            } else if remaining_args.first().map_or(false, |s| s.to_lowercase() == "stop") {
                let mut port = None;
                let mut i = 1;
                while i < remaining_args.len() {
                    match remaining_args[i].to_lowercase().as_str() {
                        "--port" | "-p" => {
                            if i + 1 < remaining_args.len() {
                                port = remaining_args[i + 1].parse::<u16>().ok();
                                i += 2;
                            } else {
                                eprintln!("Warning: Flag '{}' requires a value.", remaining_args[i]);
                                i += 1;
                            }
                        }
                        _ => { i += 1; }
                    }
                }
                CommandType::Daemon(DaemonCliCommand::Stop { port })
            } else if remaining_args.first().map_or(false, |s| s.to_lowercase() == "status") {
                let mut port = None;
                let mut cluster = None;
                let mut i = 1;
                while i < remaining_args.len() {
                    match remaining_args[i].to_lowercase().as_str() {
                        "--port" | "-p" => {
                            if i + 1 < remaining_args.len() {
                                port = remaining_args[i + 1].parse::<u16>().ok();
                                i += 2;
                            } else {
                                eprintln!("Warning: Flag '{}' requires a value.", remaining_args[i]);
                                i += 1;
                            }
                        }
                        "--cluster" => {
                            if i + 1 < remaining_args.len() {
                                cluster = Some(remaining_args[i + 1].clone());
                                i += 2;
                            } else {
                                eprintln!("Warning: Flag '{}' requires a value.", remaining_args[i]);
                                i += 1;
                            }
                        }
                        _ => { i += 1; }
                    }
                }
                CommandType::Daemon(DaemonCliCommand::Status { port, cluster })
            } else {
                CommandType::Unknown
            }
        },
        "rest" => {
            if remaining_args.is_empty() {
                eprintln!("Usage: rest <subcommand>");
                CommandType::Unknown
            } else {
                let rest_subcommand = remaining_args[0].to_lowercase();
                let mut port: Option<u16> = None;
                let mut cluster: Option<String> = None;
                let mut rest_port: Option<u16> = None;
                let mut rest_cluster: Option<String> = None;
                let mut query_string: Option<String> = None;
                let mut persist: Option<bool> = None;
                let mut username: Option<String> = None;
                let mut password: Option<String> = None;
                let mut i = 1;
                while i < remaining_args.len() {
                    match remaining_args[i].to_lowercase().as_str() {
                        "--port" | "-p" | "--listen-port" => {
                            if i + 1 < remaining_args.len() {
                                port = remaining_args[i + 1].parse::<u16>().ok();
                                i += 2;
                            } else {
                                eprintln!("Warning: Flag '{}' requires a value.", remaining_args[i]);
                                i += 1;
                            }
                        }
                        "--cluster" => {
                            if i + 1 < remaining_args.len() {
                                cluster = Some(remaining_args[i + 1].clone());
                                i += 2;
                            } else {
                                eprintln!("Warning: Flag '{}' requires a value.", remaining_args[i]);
                                i += 1;
                            }
                        }
                        "--rest-port" => {
                            if i + 1 < remaining_args.len() {
                                rest_port = remaining_args[i + 1].parse::<u16>().ok();
                                i += 2;
                            } else {
                                eprintln!("Warning: Flag '{}' requires a value.", remaining_args[i]);
                                i += 1;
                            }
                        }
                        "--rest-cluster" => {
                            if i + 1 < remaining_args.len() {
                                rest_cluster = Some(remaining_args[i + 1].clone());
                                i += 2;
                            } else {
                                eprintln!("Warning: Flag '{}' requires a value.", remaining_args[i]);
                                i += 1;
                            }
                        }
                        "--persist" => {
                            persist = Some(true);
                            i += 1;
                        }
                        _ => {
                            if rest_subcommand == "register-user" || rest_subcommand == "authenticate" {
                                if username.is_none() {
                                    username = Some(remaining_args[i].clone());
                                } else if password.is_none() {
                                    password = Some(remaining_args[i].clone());
                                }
                            } else if rest_subcommand == "graph-query" {
                                if query_string.is_none() {
                                    query_string = Some(remaining_args[i].clone());
                                }
                            }
                            i += 1;
                        }
                    }
                }
                match rest_subcommand.as_str() {
                    "start" => CommandType::Rest(RestCliCommand::Start {
                        port: Some(rest_port.unwrap_or(port.unwrap_or_default())),
                        cluster: rest_cluster.clone().or(cluster.clone()),
                        rest_port,
                        rest_cluster
                    }),
                    "stop" => CommandType::Rest(RestCliCommand::Stop { port }),
                    "status" => CommandType::Rest(RestCliCommand::Status { port, cluster }),
                    "health" => CommandType::Rest(RestCliCommand::Health),
                    "version" => CommandType::Rest(RestCliCommand::Version),
                    "register-user" => {
                        if let (Some(u), Some(p)) = (username, password) {
                            CommandType::Rest(RestCliCommand::RegisterUser { username: u, password: p })
                        } else {
                            eprintln!("Usage: rest register-user <username> <password>");
                            CommandType::Unknown
                        }
                    }
                    "authenticate" => {
                        if let (Some(u), Some(p)) = (username, password) {
                            CommandType::Rest(RestCliCommand::Authenticate { username: u, password: p })
                        } else {
                            eprintln!("Usage: rest authenticate <username> <password>");
                            CommandType::Unknown
                        }
                    }
                    "graph-query" => {
                        if let Some(q) = query_string {
                            CommandType::Rest(RestCliCommand::GraphQuery { query_string: q, persist })
                        } else {
                            eprintln!("Usage: rest graph-query <query> [--persist]");
                            CommandType::Unknown
                        }
                    }
                    "storage-query" => CommandType::Rest(RestCliCommand::StorageQuery),
                    _ => CommandType::Unknown,
                }
            }
        },
        "show" => {
            match parse_show_command(parts) {
                Ok(command) => command,
                Err(e) => {
                    eprintln!("Error parsing show command: {}", e);
                    return (CommandType::Unknown, vec![]);
                }
            }
        },
        "storage" => {
            if remaining_args.is_empty() {
                eprintln!("Usage: storage <subcommand>");
                CommandType::Unknown
            } else {
                let storage_subcommand = remaining_args[0].to_lowercase();
                let mut port = None;
                let mut config_file = None;
                let mut cluster = None;
                let mut storage_port = None;
                let mut storage_cluster = None;
                let mut i = 1;
                while i < remaining_args.len() {
                    match remaining_args[i].to_lowercase().as_str() {
                        "--port" | "-p" => {
                            if i + 1 < remaining_args.len() {
                                port = remaining_args[i + 1].parse::<u16>().ok();
                                i += 2;
                            } else {
                                eprintln!("Warning: Flag '{}' requires a value.", remaining_args[i]);
                                i += 1;
                            }
                        }
                        "--config-file" => {
                            if i + 1 < remaining_args.len() {
                                config_file = Some(PathBuf::from(remaining_args[i + 1].clone()));
                                i += 2;
                            } else {
                                eprintln!("Warning: Flag '{}' requires a value.", remaining_args[i]);
                                i += 1;
                            }
                        }
                        "--cluster" => {
                            if i + 1 < remaining_args.len() {
                                cluster = Some(remaining_args[i + 1].clone());
                                i += 2;
                            } else {
                                eprintln!("Warning: Flag '{}' requires a value.", remaining_args[i]);
                                i += 1;
                            }
                        }
                        "--storage-port" => {
                            if i + 1 < remaining_args.len() {
                                storage_port = remaining_args[i + 1].parse::<u16>().ok();
                                i += 2;
                            } else {
                                eprintln!("Warning: Flag '{}' requires a value.", remaining_args[i]);
                                i += 1;
                            }
                        }
                        "--storage-cluster" => {
                            if i + 1 < remaining_args.len() {
                                storage_cluster = Some(remaining_args[i + 1].clone());
                                i += 2;
                            } else {
                                eprintln!("Warning: Flag '{}' requires a value.", remaining_args[i]);
                                i += 1;
                            }
                        }
                        _ => { i += 1; }
                    }
                }
                match storage_subcommand.as_str() {
                    "start" => CommandType::Storage(StorageAction::Start {
                        port: Some(storage_port.unwrap_or(port.unwrap_or_default())),
                        config_file,
                        cluster: storage_cluster.clone().or(cluster.clone()),
                        storage_port,
                        storage_cluster
                    }),
                    "stop" => CommandType::Storage(StorageAction::Stop { port }),
                    "status" => CommandType::Storage(StorageAction::Status { port, cluster }),
                    "health" => CommandType::Storage(StorageAction::Health),
                    "version" => CommandType::Storage(StorageAction::Version),
                    "storage-query" => CommandType::Storage(StorageAction::StorageQuery),
                    _ => CommandType::Unknown,
                }
            }
        },
        "kv" => {
            let mut key = None;
            let mut value = None;
            let mut operation = None;
            let mut i = 0;
            if let Some(op_str) = remaining_args.get(0) {
                if !op_str.starts_with('-') {
                    operation = Some(op_str.clone());
                    i += 1;
                }
            }
            while i < remaining_args.len() {
                match remaining_args[i].as_str() {
                    "--key" => {
                        if i + 1 < remaining_args.len() {
                            key = Some(remaining_args[i + 1].clone());
                            i += 2;
                        } else {
                            eprintln!("Warning: Flag '--key' requires a value.");
                            return (CommandType::Unknown, remaining_args.clone());
                        }
                    }
                    "--value" => {
                        if i + 1 < remaining_args.len() {
                            value = Some(remaining_args[i + 1].clone());
                            i += 2;
                        } else {
                            eprintln!("Warning: Flag '--value' requires a value.");
                            return (CommandType::Unknown, remaining_args.clone());
                        }
                    }
                    _ => {
                        if operation.is_some() {
                            if key.is_none() {
                                key = Some(remaining_args[i].clone());
                                i += 1;
                            } else if value.is_none() && operation.as_deref() == Some("set") {
                                value = Some(remaining_args[i].clone());
                                i += 1;
                            } else {
                                eprintln!("Warning: Unrecognized argument: {}", remaining_args[i]);
                                return (CommandType::Unknown, remaining_args.clone());
                            }
                        } else {
                            eprintln!("Warning: Unrecognized argument: {}", remaining_args[i]);
                            return (CommandType::Unknown, remaining_args.clone());
                        }
                    }
                }
            }
            match operation {
                Some(op) => match parse_kv_operation(&op) {
                    Ok(op) => match op.as_str() {
                        "get" => {
                            if let Some(key) = key {
                                CommandType::Kv { action: KvAction::Get { key } }
                            } else {
                                eprintln!("Usage: kv get [--key <key>]");
                                CommandType::Unknown
                            }
                        }
                        "set" => {
                            if let (Some(key), Some(value)) = (key, value) {
                                CommandType::Kv { action: KvAction::Set { key, value } }
                            } else {
                                eprintln!("Usage: kv set [--key <key>] [--value <value>]");
                                CommandType::Unknown
                            }
                        }
                        "delete" => {
                            if let Some(key) = key {
                                CommandType::Kv { action: KvAction::Delete { key } }
                            } else {
                                eprintln!("Usage: kv delete [--key <key>]");
                                CommandType::Unknown
                            }
                        }
                        _ => {
                            eprintln!("Invalid KV operation: '{}'. Supported operations: get, set, delete", op);
                            CommandType::Unknown
                        }
                    },
                    Err(e) => {
                        eprintln!("{}", e);
                        CommandType::Unknown
                    }
                },
                None => {
                    eprintln!("Usage: kv [get|set|delete] [--key <key>] [--value <value>]");
                    CommandType::Unknown
                }
            }
        },
        "set" => {
            if remaining_args.len() >= 2 {
                CommandType::Kv {
                    action: KvAction::Set {
                        key: remaining_args[0].clone(),
                        value: remaining_args[1].clone()
                    }
                }
            } else {
                eprintln!("Usage: set <key> <value>");
                CommandType::Unknown
            }
        },
        "get" => {
            if remaining_args.len() >= 1 {
                CommandType::Kv {
                    action: KvAction::Get {
                        key: remaining_args[0].clone()
                    }
                }
            } else {
                eprintln!("Usage: get <key>");
                CommandType::Unknown
            }
        },
        "delete" => {
            if remaining_args.len() >= 1 {
                CommandType::Kv {
                    action: KvAction::Delete {
                        key: remaining_args[0].clone()
                    }
                }
            } else {
                eprintln!("Usage: delete <key>");
                CommandType::Unknown
            }
        },
        // === NEW: visualize ===
        "visualize" => {
            if remaining_args.is_empty() {
                eprintln!("Usage: visualize <query> | visualize \"<query>\" | visualize --query <query> [--language <lang>]");
                CommandType::Unknown
            } else {
                let mut query: Option<String> = None;
                let mut language: Option<String> = None;
                let mut i = 0;
                // 1. Positional argument (may be quoted)
                if i < remaining_args.len() && !remaining_args[i].starts_with('-') {
                    query = Some(strip_surrounding_quotes(&remaining_args[i]));
                    i = remaining_args.len();
                }
                // 2. Flag parsing
                while i < remaining_args.len() {
                    match remaining_args[i].to_lowercase().as_str() {
                        "--query" => {
                            if i + 1 < remaining_args.len() {
                                query = Some(strip_surrounding_quotes(&remaining_args[i + 1]));
                                i += 2;
                            } else {
                                eprintln!("Warning: '--query' requires a value.");
                                i += 1;
                            }
                        }
                        "--language" => {
                            if i + 1 < remaining_args.len() {
                                language = Some(remaining_args[i + 1].clone());
                                i += 2;
                            } else {
                                eprintln!("Warning: '--language' requires a value.");
                                i += 1;
                            }
                        }
                        _ => {
                            eprintln!("Warning: Unknown argument for 'visualize': {}", remaining_args[i]);
                            i += 1;
                        }
                    }
                }
                if let Some(q) = query {
                    CommandType::Visualize { query: q, language }
                } else {
                    eprintln!("Missing query for 'visualize' command.");
                    CommandType::Unknown
                }
            }
        },

        // === query / exec ===
        "query" | "exec" => {
            if remaining_args.is_empty() {
                eprintln!(
                    "Usage: {} <query> | {} \"<query>\" | {} --query <query> [--language <lang>]",
                    command_str, command_str, command_str
                );
                CommandType::Unknown
            } else {
                let mut query: Option<String> = None;
                let mut language: Option<String> = None;
                let mut i = 0;
                // 1. Positional argument (may be quoted)
                if i < remaining_args.len() && !remaining_args[i].starts_with('-') {
                    query = Some(strip_surrounding_quotes(&remaining_args[i]));
                    i = remaining_args.len();
                }
                // 2. Flag parsing
                while i < remaining_args.len() {
                    match remaining_args[i].to_lowercase().as_str() {
                        "--query" => {
                            if i + 1 < remaining_args.len() {
                                query = Some(strip_surrounding_quotes(&remaining_args[i + 1]));
                                i += 2;
                            } else {
                                eprintln!("Warning: '--query' requires a value.");
                                i += 1;
                            }
                        }
                        "--language" => {
                            if i + 1 < remaining_args.len() {
                                language = Some(remaining_args[i + 1].clone());
                                i += 2;
                            } else {
                                eprintln!("Warning: '--language' requires a value.");
                                i += 1;
                            }
                        }
                        _ => {
                            eprintln!("Warning: Unknown argument for '{}': {}", command_str, remaining_args[i]);
                            i += 1;
                        }
                    }
                }
                if let Some(q) = query {
                    CommandType::Query { query: q, language }
                } else {
                    eprintln!("Missing query for '{}' command.", command_str);
                    CommandType::Unknown
                }
            }
        },
        "migrate" => {
            if remaining_args.len() < 2 && !remaining_args.iter().any(|arg| arg == "--from" || arg == "--source" || arg == "--to" || arg == "--dest") {
                eprintln!("Usage: migrate [--port <port>] [--cluster <cluster>] [--from <engine>] [--to <engine>] [--source <engine>] [--dest <engine>]");
                CommandType::Unknown
            } else {
                let mut from_engine = None;
                let mut to_engine = None;
                let mut from = None;
                let mut to = None;
                let mut source = None;
                let mut dest = None;
                let mut port = None;
                let mut cluster = None;
                let mut i = 0;
                if i < remaining_args.len() && !remaining_args[i].starts_with("--") {
                    from_engine = parse_storage_engine(&remaining_args[i]).ok();
                    i += 1;
                }
                if i < remaining_args.len() && !remaining_args[i].starts_with("--") {
                    to_engine = parse_storage_engine(&remaining_args[i]).ok();
                    i += 1;
                }
                while i < remaining_args.len() {
                    match remaining_args[i].to_lowercase().as_str() {
                        "--from" => {
                            if i + 1 < remaining_args.len() {
                                from = parse_storage_engine(&remaining_args[i + 1]).ok();
                                i += 2;
                            } else {
                                eprintln!("Warning: Flag '--from' requires a value.");
                                return (CommandType::Unknown, remaining_args.clone());
                            }
                        }
                        "--to" => {
                            if i + 1 < remaining_args.len() {
                                to = parse_storage_engine(&remaining_args[i + 1]).ok();
                                i += 2;
                            } else {
                                eprintln!("Warning: Flag '--to' requires a value.");
                                return (CommandType::Unknown, remaining_args.clone());
                            }
                        }
                        "--source" => {
                            if i + 1 < remaining_args.len() {
                                source = parse_storage_engine(&remaining_args[i + 1]).ok();
                                i += 2;
                            } else {
                                eprintln!("Warning: Flag '--source' requires a value.");
                                return (CommandType::Unknown, remaining_args.clone());
                            }
                        }
                        "--dest" => {
                            if i + 1 < remaining_args.len() {
                                dest = parse_storage_engine(&remaining_args[i + 1]).ok();
                                i += 2;
                            } else {
                                eprintln!("Warning: Flag '--dest' requires a value.");
                                return (CommandType::Unknown, remaining_args.clone());
                            }
                        }
                        "--port" => {
                            if i + 1 < remaining_args.len() {
                                port = remaining_args[i + 1].parse::<u16>().ok();
                                i += 2;
                            } else {
                                eprintln!("Warning: Flag '--port' requires a value.");
                                return (CommandType::Unknown, remaining_args.clone());
                            }
                        }
                        "--cluster" => {
                            if i + 1 < remaining_args.len() {
                                cluster = Some(remaining_args[i + 1].clone());
                                i += 2;
                            } else {
                                eprintln!("Warning: Flag '--cluster' requires a value.");
                                return (CommandType::Unknown, remaining_args.clone());
                            }
                        }
                        _ => {
                            eprintln!("Warning: Unknown argument for 'migrate': {}", remaining_args[i]);
                            i += 1;
                        }
                    }
                }
                let from_engine_final = from_engine.or(from).or(source);
                let to_engine_final = to_engine.or(to).or(dest);
                CommandType::Migrate(MigrateAction {
                    from, to, source, dest, from_engine_pos: from_engine_final, to_engine_pos: to_engine_final, port, cluster,
                })
            }
        }
        // === NEW: Graph Domain Commands ===
        "graph" => {
            if remaining_args.is_empty() {
                eprintln!("Usage: graph <insert-person|medical|delete|load> [args...]");
                CommandType::Unknown
            } else {
                match remaining_args[0].to_lowercase().as_str() {
                    "insert-person" => {
                        let mut name = None;
                        let mut age = None;
                        let mut city = None;
                        let mut i = 1;
                        while i < remaining_args.len() {
                            match remaining_args[i].as_str() {
                                "name" | "name=" => {
                                    if let Some(val) = remaining_args.get(i + 1) {
                                        name = Some(val.clone());
                                        i += 2;
                                    } else { i += 1; }
                                }
                                "age" | "age=" => {
                                    if let Some(val) = remaining_args.get(i + 1) {
                                        age = val.parse::<i32>().ok();
                                        i += 2;
                                    } else { i += 1; }
                                }
                                "city" | "city=" => {
                                    if let Some(val) = remaining_args.get(i + 1) {
                                        city = Some(val.clone());
                                        i += 2;
                                    } else { i += 1; }
                                }
                                _ => i += 1,
                            }
                        }
                        CommandType::Graph(GraphAction::InsertPerson { name, age, city })
                    }
                    "medical" => {
                        let mut patient_name = None;
                        let mut patient_age = None;
                        let mut diagnosis_code = None;
                        let mut i = 1;
                        while i < remaining_args.len() {
                            match remaining_args[i].as_str() {
                                "patient_name" | "patient_name=" => {
                                    if let Some(val) = remaining_args.get(i + 1) {
                                        patient_name = Some(val.clone());
                                        i += 2;
                                    } else { i += 1; }
                                }
                                "patient_age" | "patient_age=" => {
                                    if let Some(val) = remaining_args.get(i + 1) {
                                        patient_age = val.parse::<i32>().ok();
                                        i += 2;
                                    } else { i += 1; }
                                }
                                "diagnosis_code" | "diagnosis_code=" => {
                                    if let Some(val) = remaining_args.get(i + 1) {
                                        diagnosis_code = Some(val.clone());
                                        i += 2;
                                    } else { i += 1; }
                                }
                                _ => i += 1,
                            }
                        }
                        CommandType::Graph(GraphAction::MedicalRecord { patient_name, patient_age, diagnosis_code })
                    }
                    "delete" => {
                        if remaining_args.len() > 1 {
                            CommandType::Graph(GraphAction::DeleteNode { id: remaining_args[1].clone() })
                        } else {
                            eprintln!("Usage: graph delete <node_id>");
                            CommandType::Unknown
                        }
                    }
                    "load" => {
                        if remaining_args.len() > 1 {
                            CommandType::Graph(GraphAction::LoadData { path: remaining_args[1].clone().into() })
                        } else {
                            eprintln!("Usage: graph load <path/to/data.json>");
                            CommandType::Unknown
                        }
                    }
                    _ => {
                        eprintln!("Unknown graph action: {}. Use insert-person, medical, delete, load", remaining_args[0]);
                        CommandType::Unknown
                    }
                }
            }
        }
        // === NEW: Index & Full-Text Search Commands ===
        "index" => {
            if remaining_args.is_empty() {
                eprintln!("Usage: index <create|drop|search|rebuild|list|stats|create-fulltext-index|drop-fulltext> [args...]");
                CommandType::Unknown
            } else {
                match remaining_args[0].to_lowercase().as_str() {

                    // 1. Standard Index Creation: index create <L> <P> OR index create FULLTEXT <L> <P>
                    "create" => {
                        // Needs at least two arguments: <arg1> <arg2>
                        if remaining_args.len() < 3 {
                            eprintln!("Usage: index create <Label> <property> (Standard B-Tree)");
                            eprintln!("Usage: index create FULLTEXT <Label> <property> (Single-field Fulltext)");
                            CommandType::Unknown
                        } else {
                            // Map the arguments directly to the flexible IndexAction::Create variant
                            CommandType::Index(IndexAction::Create {
                                arg1: remaining_args[1].clone(),
                                arg2: remaining_args[2].clone(),
                                arg3_property: remaining_args.get(3).cloned(),
                            })
                        }
                    }

                    // 2. Dedicated Multi-field Fulltext Creation: index createfulltext <name> --labels <L1,L2> --properties <P1,P2>
                    "createfulltext" | "create-fulltext-index" => { // Support both styles
                        // Check for index name presence
                        let index_name = remaining_args.get(1).cloned();

                        // Use the original flag parsing logic for labels/properties
                        let mut labels = Vec::new();
                        let mut properties = Vec::new();

                        let mut i = 2; // Start after "index createfulltext <name>"
                        while i < remaining_args.len() {
                            let current_arg_lower = remaining_args[i].to_lowercase();

                            if current_arg_lower == "--labels" && i + 1 < remaining_args.len() {
                                labels = remaining_args[i + 1]
                                    .split(',')
                                    .map(|s| s.trim().to_string())
                                    .filter(|s| !s.is_empty())
                                    .collect();
                                i += 2;
                            // --- FIX: Change --props to --properties ---
                            } else if current_arg_lower == "--properties" && i + 1 < remaining_args.len() {
                                properties = remaining_args[i + 1]
                                    .split(',')
                                    .map(|s| s.trim().to_string())
                                    .filter(|s| !s.is_empty())
                                    .collect();
                                i += 2;
                            // ------------------------------------------
                            } else {
                                i += 1;
                            }
                        }

                        if let Some(name) = index_name {
                            if labels.is_empty() || properties.is_empty() {
                                eprintln!("Usage: index createfulltext <name> --labels <L1,L2> --properties <P1,P2>");
                                CommandType::Unknown
                            } else {
                                // --- FIX: Use the correct new variant name ---
                                CommandType::Index(IndexAction::CreateFulltext {
                                    index_name: name,
                                    labels,
                                    properties,
                                })
                                // --------------------------------------------
                            }
                        } else {
                            eprintln!("Usage: index createfulltext <name> --labels <L1,L2> --properties <P1,P2>");
                            CommandType::Unknown
                        }
                    }

                    // 3. Drop Index Logic: index drop <L> <P> OR index drop fulltext <N>
                    "drop" => {
                        if remaining_args.len() < 3 {
                            eprintln!("Usage: index drop <Label> <property> OR index drop fulltext <name>");
                            CommandType::Unknown
                        } else {
                            let arg1 = remaining_args[1].to_lowercase();
                            if arg1 == "fulltext" {
                                // Fulltext drop: index drop fulltext <name>
                                CommandType::Index(IndexAction::DropFulltext {
                                    index_name: remaining_args[2].clone(),
                                })
                            } else {
                                // Standard drop: index drop <Label> <property>
                                CommandType::Index(IndexAction::Drop {
                                    label: remaining_args[1].clone(),
                                    property: remaining_args[2].clone(),
                                })
                            }
                        }
                    }
                    
                    // 4. Dedicated Drop Fulltext: index dropfulltext <name>
                    "dropfulltext" | "drop-fulltext" => {
                        if remaining_args.len() < 2 {
                            eprintln!("Usage: index dropfulltext <name>");
                            CommandType::Unknown
                        } else {
                            CommandType::Index(IndexAction::DropFulltext {
                                index_name: remaining_args[1].clone(),
                            })
                        }
                    }

                    // 5. Search Logic: index search "query" [top|bottom N]
                    "search" => {
                        if remaining_args.len() < 2 {
                            eprintln!("Usage: index search \"query\" [top|bottom|head|tail <N>]");
                            return (CommandType::Unknown, Vec::new());
                        }

                        let term = remaining_args[1].clone();
                        let mut order = None;

                        // Check for order specification: <order_type> <N> (4 args total)
                        if remaining_args.len() >= 4 {
                            let order_type = remaining_args[2].to_lowercase();
                            if let Ok(n) = remaining_args[3].parse::<usize>() {
                                match order_type.as_str() {
                                    "top" | "--top" | "head" | "--head" => order = Some(SearchOrder::Top { count: n }),
                                    "bottom" | "--bottom" | "tail" | "--tail" => order = Some(SearchOrder::Bottom { count: n }),
                                    _ => {
                                        eprintln!("Invalid order type: {}. Use: top, bottom, head, or tail.", remaining_args[2]);
                                        return (CommandType::Unknown, Vec::new());
                                    }
                                }
                            } else {
                                eprintln!("Invalid count provided for search order.");
                                return (CommandType::Unknown, Vec::new());
                            }
                        } else if remaining_args.len() == 3 {
                            // Check for single-arg flag style like "--top=5"
                            let arg = remaining_args[2].to_lowercase();
                            if let Some(stripped) = arg.strip_prefix("top=").or_else(|| arg.strip_prefix("--top=")) {
                                order = stripped.parse::<usize>().ok().map(|n| SearchOrder::Top { count: n });
                            } else if let Some(stripped) = arg.strip_prefix("head=").or_else(|| arg.strip_prefix("--head=")) {
                                order = stripped.parse::<usize>().ok().map(|n| SearchOrder::Head { count: n });
                            } else if let Some(stripped) = arg.strip_prefix("bottom=").or_else(|| arg.strip_prefix("--bottom=")) {
                                order = stripped.parse::<usize>().ok().map(|n| SearchOrder::Bottom { count: n });
                            } else if let Some(stripped) = arg.strip_prefix("tail=").or_else(|| arg.strip_prefix("--tail=")) {
                                order = stripped.parse::<usize>().ok().map(|n| SearchOrder::Tail { count: n });
                            }
                        }

                        CommandType::Index(IndexAction::Search { term, order })
                    }
                    
                    // 6. Simple Actions
                    "rebuild" => CommandType::Index(IndexAction::Rebuild),
                    "list" => CommandType::Index(IndexAction::List),
                    "stats" => CommandType::Index(IndexAction::Stats),
                    
                    _ => {
                        eprintln!("Unknown index action. Use: create, drop, search, rebuild, list, stats, createfulltext, dropfulltext");
                        CommandType::Unknown
                    }
                }
            }
        }

        // === ALL CLINICAL COMMANDS — FULLY IMPLEMENTED ===
        "patient" => {
            if remaining_args.is_empty() {
                eprintln!("Usage: patient <view|timeline|journey|care-gaps|problems|meds|alerts|allergies|search> [args...]");
                return (CommandType::Unknown, remaining_args);
            }
            match remaining_args[0].to_lowercase().as_str() {
                "view" => {
                    let patient_id = remaining_args.get(1).and_then(|s| s.parse::<i32>().ok()).unwrap_or(0);
                    CommandType::Patient(PatientCommand::View { patient_id })
                }
                "timeline" => {
                    let patient_id = remaining_args.get(1).and_then(|s| s.parse::<i32>().ok()).unwrap_or(0);
                    CommandType::Patient(PatientCommand::Timeline { patient_id })
                }
                "journey" => {
                    let patient_id = remaining_args.get(1).and_then(|s| s.parse::<i32>().ok()).unwrap_or(0);
                    let mut show_completed = false;
                    let mut show_deviations_only = false;
                    let mut pathway = None;
                    let mut format = None;
                    let mut i = 2;
                    while i < remaining_args.len() {
                        match remaining_args[i].to_lowercase().as_str() {
                            "--show-completed" => {
                                show_completed = true;
                                i += 1;
                            }
                            "--show-deviations-only" => {
                                show_deviations_only = true;
                                i += 1;
                            }
                            "--pathway" => {
                                if i + 1 < remaining_args.len() {
                                    pathway = Some(remaining_args[i + 1].clone());
                                    i += 2;
                                } else {
                                    i += 1;
                                }
                            }
                            "--format" => {
                                if i + 1 < remaining_args.len() {
                                    format = match remaining_args[i + 1].to_lowercase().as_str() {
                                        "text" => Some(JourneyFormat::Text),
                                        "json" => Some(JourneyFormat::Json),
                                        "timeline" => Some(JourneyFormat::Timeline),
                                        "detailed" => Some(JourneyFormat::Detailed),
                                        _ => Some(JourneyFormat::Text),
                                    };
                                    i += 2;
                                } else {
                                    i += 1;
                                }
                            }
                            _ => i += 1,
                        }
                    }
                    CommandType::Patient(PatientCommand::Journey {
                        patient_id,
                        show_completed,
                        show_deviations_only,
                        pathway,
                        format,
                    })
                }
                "care-gaps" | "gaps" => {
                    let patient_id = if remaining_args.len() > 1 {
                        remaining_args[1].parse::<i32>().ok()
                    } else {
                        None
                    };
                    CommandType::Patient(PatientCommand::CareGaps { patient_id })
                }
                "problems" => {
                    let patient_id = remaining_args.get(1).and_then(|s| s.parse::<i32>().ok()).unwrap_or(0);
                    CommandType::Patient(PatientCommand::Problems { patient_id })
                }
                "meds" => {
                    let patient_id = remaining_args.get(1).and_then(|s| s.parse::<i32>().ok()).unwrap_or(0);
                    CommandType::Patient(PatientCommand::Meds { patient_id })
                }
                "alerts" => {
                    let patient_id = remaining_args
                        .get(1)
                        .and_then(|s| s.parse::<i32>().ok())
                        .unwrap_or(0);

                    // ---- defaults for the new required fields ----
                    CommandType::Patient(PatientCommand::DrugAlerts {
                        patient_id,
                        drug_class: None,            // Option<String>
                        format: Default::default(),  // whatever your Format enum default is
                        include_overridden: false,   // bool
                        include_inactive: false,     // bool
                        severity_filter: None,       // Option<Severity>
                        include_resolved: false,
                        severity: None,
                    })
                }
                "allergies" => {
                    let patient_id = remaining_args.get(1).and_then(|s| s.parse::<i32>().ok()).unwrap_or(0);
                    CommandType::Patient(PatientCommand::Allergies { patient_id })
                }
                "search" => {
                    let query = remaining_args[1..].join(" ");
                    CommandType::Patient(PatientCommand::Search { query })
                }
                _ => CommandType::Unknown,
            }
        },
        "encounter" => {
            if remaining_args.is_empty() {
                eprintln!("Usage: encounter <start|close> [args...]");
                return (CommandType::Unknown, remaining_args);
            }

            match remaining_args[0].to_lowercase().as_str() {
                "start" => {
                    let mut patient_id = None;
                    let mut doctor_id = None;
                    let mut encounter_type = None;
                    let mut location = None;
                    let mut i = 1;
                    while i < remaining_args.len() {
                        match remaining_args[i].to_lowercase().as_str() {
                            "--patient-id" | "-p" => {
                                if i + 1 < remaining_args.len() {
                                    patient_id = remaining_args[i + 1].parse::<i32>().ok();
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--doctor-id" | "-d" => {
                                if i + 1 < remaining_args.len() {
                                    doctor_id = remaining_args[i + 1].parse::<i32>().ok();
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--type" | "-t" => {
                                if i + 1 < remaining_args.len() {
                                    encounter_type = Some(remaining_args[i + 1].clone());
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--location" | "-l" => {
                                if i + 1 < remaining_args.len() {
                                    location = Some(remaining_args[i + 1].clone());
                                    i += 2;
                                } else { i += 1; }
                            }
                            _ => i += 1,
                        }
                    }
                    CommandType::Encounter(EncounterCommand::Start {
                        patient_id: patient_id.unwrap_or(0),
                        doctor_id: doctor_id.unwrap_or(0),
                        encounter_type: encounter_type.unwrap_or_default(),
                        location,
                    })
                }
                "close" => {
                    if remaining_args.len() < 2 {
                        eprintln!("Usage: encounter close <encounter_id> <disposition>");
                        return (CommandType::Unknown, remaining_args);
                    }
                    let encounter_id = match Uuid::parse_str(&remaining_args[1]) {
                        Ok(id) => id,
                        Err(_) => {
                            eprintln!("Invalid encounter ID");
                            return (CommandType::Unknown, remaining_args);
                        }
                    };
                    let disposition = remaining_args.get(2).cloned().unwrap_or_default();
                    CommandType::Encounter(EncounterCommand::Close { encounter_id, disposition, instructions: None })
                }
                _ => CommandType::Unknown,
            }
        },

        "diagnosis" => {
            if remaining_args.is_empty() || remaining_args[0].to_lowercase() != "add" {
                eprintln!("Usage: diagnosis add --encounter <uuid> <description> [--icd10 <code>]");
                return (CommandType::Unknown, remaining_args);
            }
            let mut encounter_id = None;
            let mut description = None;
            let mut icd10 = None;
            let mut i = 1;
            while i < remaining_args.len() {
                match remaining_args[i].to_lowercase().as_str() {
                    "--encounter" => {
                        if i + 1 < remaining_args.len() {
                            encounter_id = Uuid::parse_str(&remaining_args[i + 1]).ok();
                            i += 2;
                        } else { i += 1; }
                    }
                    "--icd10" => {
                        if i + 1 < remaining_args.len() {
                            icd10 = Some(remaining_args[i + 1].clone());
                            i += 2;
                        } else { i += 1; }
                    }
                    _ => {
                        if description.is_none() {
                            description = Some(remaining_args[i].clone());
                        }
                        i += 1;
                    }
                }
            }
            CommandType::Diagnosis(DiagnosisCommand::Add {
                encounter_id: encounter_id.unwrap_or_default(),
                description: description.unwrap_or_default(),
                icd10,
            })
        },

        "prescription" | "rx" => {
            if remaining_args.is_empty() {
                eprintln!("Usage: prescription <add|check> [args...]");
                return (CommandType::Unknown, remaining_args);
            }
            match remaining_args[0].to_lowercase().as_str() {
                "add" => {
                    let mut encounter_id = None;
                    let mut medication = None;
                    let mut dose = None;
                    let mut frequency = None;
                    let mut days = None;
                    let mut refills = None;
                    let mut i = 1;
                    while i < remaining_args.len() {
                        match remaining_args[i].to_lowercase().as_str() {
                            "--encounter" | "-e" => {
                                if i + 1 < remaining_args.len() {
                                    encounter_id = Uuid::parse_str(&remaining_args[i + 1]).ok();
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--med" => {
                                if i + 1 < remaining_args.len() {
                                    medication = Some(remaining_args[i + 1].clone());
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--dose" => {
                                if i + 1 < remaining_args.len() {
                                    dose = Some(remaining_args[i + 1].clone());
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--freq" => {
                                if i + 1 < remaining_args.len() {
                                    frequency = Some(remaining_args[i + 1].clone());
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--days" => {
                                if i + 1 < remaining_args.len() {
                                    days = remaining_args[i + 1].parse::<i64>().ok();
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--refills" => {
                                if i + 1 < remaining_args.len() {
                                    refills = remaining_args[i + 1].parse::<i32>().ok();
                                    i += 2;
                                } else { i += 1; }
                            }
                            _ => i += 1,
                        }
                    }
                    CommandType::Prescription(PrescriptionCommand::Add {
                        encounter_id: encounter_id.unwrap_or_default(),
                        medication_name: medication.unwrap_or_default(),
                        dose: dose.unwrap_or_default(),
                        frequency: frequency.unwrap_or_default(),
                        days: days.unwrap_or(30),
                        refills,
                    })
                }
                "check" => {
                    let patient_id = remaining_args.get(1).and_then(|s| s.parse::<i32>().ok()).unwrap_or(0);
                    CommandType::Prescription(PrescriptionCommand::CheckInteractions { patient_id })
                }
                _ => CommandType::Unknown,
            }
        },

        "note" => {
            if remaining_args.is_empty() || remaining_args[0].to_lowercase() != "add" {
                eprintln!("Usage: note add --patient <id> --text <text> [--author <id>] [--type <type>]");
                return (CommandType::Unknown, remaining_args);
            }
            let mut patient_id = None;
            let mut author_id = None;
            let mut text = None;
            let mut note_type = None;
            let mut i = 1;
            while i < remaining_args.len() {
                match remaining_args[i].to_lowercase().as_str() {
                    "--patient" => {
                        if i + 1 < remaining_args.len() {
                            patient_id = remaining_args[i + 1].parse::<i32>().ok();
                            i += 2;
                        } else { i += 1; }
                    }
                    "--author" => {
                        if i + 1 < remaining_args.len() {
                            author_id = remaining_args[i + 1].parse::<i32>().ok();
                            i += 2;
                        } else { i += 1; }
                    }
                    "--type" => {
                        if i + 1 < remaining_args.len() {
                            note_type = Some(remaining_args[i + 1].clone());
                            i += 2;
                        } else { i += 1; }
                    }
                    "--text" => {
                        if i + 1 < remaining_args.len() {
                            text = Some(remaining_args[i + 1].clone());
                            i += 2;
                        } else { i += 1; }
                    }
                    _ => {
                        if text.is_none() {
                            text = Some(remaining_args[i].clone());
                        }
                        i += 1;
                    }
                }
            }
            CommandType::Note(NoteCommand::Add {
                patient_id: patient_id.unwrap_or(0),
                author_id: author_id.unwrap_or(0),
                text: text.unwrap_or_default(),
                note_type,
            })
        },

        "procedure" => {
            if remaining_args.is_empty() {
                eprintln!("Usage: procedure <add|perform|result|cancel|list|timeline|analytics> [args...]");
                return (CommandType::Unknown, remaining_args);
            }
            match remaining_args[0].to_lowercase().as_str() {
                "add" | "order" => {
                    let mut encounter_id = None;
                    let mut procedure = None;
                    let mut cpt_code = None;
                    let mut laterality = None;
                    let mut priority = None;
                    let mut indication = None;
                    let mut ordering_provider = None;
                    let mut scheduled_date = None;
                    let mut location = None;
                    let mut i = 1;
                    while i < remaining_args.len() {
                        match remaining_args[i].to_lowercase().as_str() {
                            "--encounter" | "-e" => {
                                if i + 1 < remaining_args.len() {
                                    encounter_id = Uuid::parse_str(&remaining_args[i + 1]).ok();
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--procedure" => {
                                if i + 1 < remaining_args.len() {
                                    procedure = Some(remaining_args[i + 1].clone());
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--cpt" | "--cpt-code" => {
                                if i + 1 < remaining_args.len() {
                                    cpt_code = Some(remaining_args[i + 1].clone());
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--laterality" => {
                                if i + 1 < remaining_args.len() {
                                    laterality = match remaining_args[i + 1].to_lowercase().as_str() {
                                        "left" => Some(Laterality::Left),
                                        "right" => Some(Laterality::Right),
                                        "bilateral" => Some(Laterality::Bilateral),
                                        "midline" => Some(Laterality::Midline),
                                        _ => Some(Laterality::None),
                                    };
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--priority" => {
                                if i + 1 < remaining_args.len() {
                                    priority = match remaining_args[i + 1].to_lowercase().as_str() {
                                        "stat" => Some(ProcedurePriority::Stat),
                                        "urgent" => Some(ProcedurePriority::Urgent),
                                        "routine" => Some(ProcedurePriority::Routine),
                                        "elective" => Some(ProcedurePriority::Elective),
                                        "emergent" => Some(ProcedurePriority::Emergent),
                                        _ => Some(ProcedurePriority::Routine),
                                    };
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--indication" => {
                                if i + 1 < remaining_args.len() {
                                    indication = Some(remaining_args[i + 1].clone());
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--ordering-provider" => {
                                if i + 1 < remaining_args.len() {
                                    ordering_provider = remaining_args[i + 1].parse::<i32>().ok();
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--scheduled-date" => {
                                if i + 1 < remaining_args.len() {
                                    scheduled_date = Some(remaining_args[i + 1].clone());
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--location" => {
                                if i + 1 < remaining_args.len() {
                                    location = Some(remaining_args[i + 1].clone());
                                    i += 2;
                                } else { i += 1; }
                            }
                            _ => {
                                if procedure.is_none() {
                                    procedure = Some(remaining_args[i].clone());
                                }
                                i += 1;
                            }
                        }
                    }
                    CommandType::Procedure(ProcedureCommand::Order {
                        encounter_id: encounter_id.unwrap_or_default(),
                        procedure: procedure.unwrap_or_default(),
                        cpt_code,
                        laterality,
                        priority,
                        indication,
                        ordering_provider,
                        scheduled_date,
                        location,
                    })
                }
                "perform" => {
                    let mut procedure_id = None;
                    let mut performing_provider = None;
                    let mut start_time = None;
                    let mut end_time = None;
                    let mut anesthesia_type = None;
                    let mut assistants = Vec::new();
                    let mut specimens = Vec::new();
                    let mut implants = Vec::new();
                    let mut ebl_ml = None;
                    let mut complications = Vec::new();
                    let mut i = 1;
                    while i < remaining_args.len() {
                        match remaining_args[i].to_lowercase().as_str() {
                            "--procedure-id" => {
                                if i + 1 < remaining_args.len() {
                                    procedure_id = Uuid::parse_str(&remaining_args[i + 1]).ok();
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--performing-provider" => {
                                if i + 1 < remaining_args.len() {
                                    performing_provider = remaining_args[i + 1].parse::<i32>().ok();
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--start-time" => {
                                if i + 1 < remaining_args.len() {
                                    start_time = Some(remaining_args[i + 1].clone());
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--end-time" => {
                                if i + 1 < remaining_args.len() {
                                    end_time = Some(remaining_args[i + 1].clone());
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--anesthesia" => {
                                if i + 1 < remaining_args.len() {
                                    anesthesia_type = match remaining_args[i + 1].to_lowercase().as_str() {
                                        "general" => Some(AnesthesiaType::General),
                                        "regional" => Some(AnesthesiaType::Regional),
                                        "local" => Some(AnesthesiaType::Local),
                                        "monitored" => Some(AnesthesiaType::Monitored),
                                        "sedation" => Some(AnesthesiaType::Sedation),
                                        _ => Some(AnesthesiaType::None),
                                    };
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--assistant" => {
                                if i + 1 < remaining_args.len() {
                                    assistants.push(remaining_args[i + 1].parse::<i32>().unwrap_or(0));
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--specimen" => {
                                if i + 1 < remaining_args.len() {
                                    specimens.push(remaining_args[i + 1].clone());
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--implant" => {
                                if i + 1 < remaining_args.len() {
                                    implants.push(remaining_args[i + 1].clone());
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--ebl" => {
                                if i + 1 < remaining_args.len() {
                                    ebl_ml = remaining_args[i + 1].parse::<i32>().ok();
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--complication" => {
                                if i + 1 < remaining_args.len() {
                                    complications.push(remaining_args[i + 1].clone());
                                    i += 2;
                                } else { i += 1; }
                            }
                            _ => i += 1,
                        }
                    }
                    CommandType::Procedure(ProcedureCommand::Perform {
                        procedure_id: procedure_id.unwrap_or_default(),
                        performing_provider: performing_provider.unwrap_or(0),
                        start_time,
                        end_time,
                        anesthesia_type,
                        assistants,
                        specimens,
                        implants,
                        ebl_ml,
                        complications,
                    })
                }
                "result" => {
                    let mut procedure_id = None;
                    let mut status = None;
                    let mut findings = None;
                    let mut impression = None;
                    let mut pathology_sent = false;
                    let mut report_by = None;
                    let mut report_date = None;
                    let mut i = 1;
                    while i < remaining_args.len() {
                        match remaining_args[i].to_lowercase().as_str() {
                            "--procedure-id" => {
                                if i + 1 < remaining_args.len() {
                                    procedure_id = Uuid::parse_str(&remaining_args[i + 1]).ok();
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--status" => {
                                if i + 1 < remaining_args.len() {
                                    status = match remaining_args[i + 1].to_lowercase().as_str() {
                                        "completed" => Some(ProcedureStatus::Completed),
                                        "preliminary" => Some(ProcedureStatus::Preliminary),
                                        "final" => Some(ProcedureStatus::Final),
                                        "cancelled" => Some(ProcedureStatus::Cancelled),
                                        _ => Some(ProcedureStatus::Completed),
                                    };
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--findings" => {
                                if i + 1 < remaining_args.len() {
                                    findings = Some(remaining_args[i + 1].clone());
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--impression" => {
                                if i + 1 < remaining_args.len() {
                                    impression = Some(remaining_args[i + 1].clone());
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--pathology" => {
                                pathology_sent = true;
                                i += 1;
                            }
                            "--report-by" => {
                                if i + 1 < remaining_args.len() {
                                    report_by = remaining_args[i + 1].parse::<i32>().ok();
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--report-date" => {
                                if i + 1 < remaining_args.len() {
                                    report_date = Some(remaining_args[i + 1].clone());
                                    i += 2;
                                } else { i += 1; }
                            }
                            _ => i += 1,
                        }
                    }
                    CommandType::Procedure(ProcedureCommand::Result {
                        procedure_id: procedure_id.unwrap_or_default(),
                        status: status.unwrap_or(ProcedureStatus::Completed),
                        findings,
                        impression,
                        pathology_sent,
                        report_by: report_by.unwrap_or(0),
                        report_date,
                    })
                }
                "cancel" => {
                    let mut procedure_id = None;
                    let mut reason = None;
                    let mut cancelled_by = None;
                    let mut i = 1;
                    while i < remaining_args.len() {
                        match remaining_args[i].to_lowercase().as_str() {
                            "--procedure-id" => {
                                if i + 1 < remaining_args.len() {
                                    procedure_id = Uuid::parse_str(&remaining_args[i + 1]).ok();
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--reason" => {
                                if i + 1 < remaining_args.len() {
                                    reason = Some(remaining_args[i + 1].clone());
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--cancelled-by" => {
                                if i + 1 < remaining_args.len() {
                                    cancelled_by = remaining_args[i + 1].parse::<i32>().ok();
                                    i += 2;
                                } else { i += 1; }
                            }
                            _ => i += 1,
                        }
                    }
                    CommandType::Procedure(ProcedureCommand::Cancel {
                        procedure_id: procedure_id.unwrap_or_default(),
                        reason: reason.unwrap_or_default(),
                        cancelled_by: cancelled_by.unwrap_or(0),
                    })
                }
                "list" => {
                    let mut patient_id = None;
                    let mut encounter_id = None;
                    let mut status = None;
                    let mut provider_id = None;
                    let mut date_range = None;
                    let mut limit = None;
                    let mut i = 1;
                    while i < remaining_args.len() {
                        match remaining_args[i].to_lowercase().as_str() {
                            "--patient-id" => {
                                if i + 1 < remaining_args.len() {
                                    patient_id = remaining_args[i + 1].parse::<i32>().ok();
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--encounter-id" => {
                                if i + 1 < remaining_args.len() {
                                    encounter_id = Uuid::parse_str(&remaining_args[i + 1]).ok();
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--status" => {
                                if i + 1 < remaining_args.len() {
                                    status = match remaining_args[i + 1].to_lowercase().as_str() {
                                        "ordered" => Some(ProcedureStatus::Ordered),
                                        "completed" => Some(ProcedureStatus::Completed),
                                        "cancelled" => Some(ProcedureStatus::Cancelled),
                                        _ => None,
                                    };
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--provider-id" => {
                                if i + 1 < remaining_args.len() {
                                    provider_id = remaining_args[i + 1].parse::<i32>().ok();
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--date-range" => {
                                if i + 1 < remaining_args.len() {
                                    date_range = Some(remaining_args[i + 1].clone());
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--limit" => {
                                if i + 1 < remaining_args.len() {
                                    limit = remaining_args[i + 1].parse::<usize>().ok();
                                    i += 2;
                                } else { i += 1; }
                            }
                            _ => i += 1,
                        }
                    }
                    CommandType::Procedure(ProcedureCommand::List {
                        patient_id,
                        encounter_id,
                        status,
                        provider_id,
                        date_range,
                        limit,
                    })
                }
                "timeline" => {
                    let patient_id = remaining_args.get(1).and_then(|s| s.parse::<i32>().ok()).unwrap_or(0);
                    let mut procedure_type = None;
                    let mut years = None;
                    let mut i = 2;
                    while i < remaining_args.len() {
                        match remaining_args[i].to_lowercase().as_str() {
                            "--type" => {
                                if i + 1 < remaining_args.len() {
                                    procedure_type = Some(remaining_args[i + 1].clone());
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--years" => {
                                if i + 1 < remaining_args.len() {
                                    years = remaining_args[i + 1].parse::<i32>().ok();
                                    i += 2;
                                } else { i += 1; }
                            }
                            _ => i += 1,
                        }
                    }
                    CommandType::Procedure(ProcedureCommand::Timeline {
                        patient_id,
                        procedure_type,
                        years,
                    })
                }
                "analytics" => {
                    if remaining_args.len() < 2 {
                        eprintln!("Usage: procedure analytics <volume|complications|turnaround|cancellations|provider-volume|outcomes>");
                        return (CommandType::Unknown, remaining_args);
                    }
                    match remaining_args[1].to_lowercase().as_str() {
                        "volume" => {
                            let mut procedure_type = None;
                            let mut provider = None;
                            let mut timeframe = None;
                            let mut i = 2;
                            while i < remaining_args.len() {
                                match remaining_args[i].to_lowercase().as_str() {
                                    "--procedure-type" => {
                                        if i + 1 < remaining_args.len() {
                                            procedure_type = Some(remaining_args[i + 1].clone());
                                            i += 2;
                                        } else { i += 1; }
                                    }
                                    "--provider" => {
                                        if i + 1 < remaining_args.len() {
                                            provider = remaining_args[i + 1].parse::<i32>().ok();
                                            i += 2;
                                        } else { i += 1; }
                                    }
                                    "--timeframe" => {
                                        if i + 1 < remaining_args.len() {
                                            timeframe = Some(remaining_args[i + 1].clone());
                                            i += 2;
                                        } else { i += 1; }
                                    }
                                    _ => i += 1,
                                }
                            }
                            CommandType::Procedure(ProcedureCommand::Analytics {
                                analytics_type: ProcedureAnalyticsType::Volume {
                                    procedure_type,
                                    provider,
                                    timeframe,
                                },
                            })
                        }
                        "complications" => {
                            let mut procedure = None;
                            let mut risk_adjusted = false;
                            let mut i = 2;
                            while i < remaining_args.len() {
                                match remaining_args[i].to_lowercase().as_str() {
                                    "--procedure" => {
                                        if i + 1 < remaining_args.len() {
                                            procedure = Some(remaining_args[i + 1].clone());
                                            i += 2;
                                        } else { i += 1; }
                                    }
                                    "--risk-adjusted" => {
                                        risk_adjusted = true;
                                        i += 1;
                                    }
                                    _ => i += 1,
                                }
                            }
                            CommandType::Procedure(ProcedureCommand::Analytics {
                                analytics_type: ProcedureAnalyticsType::Complications {
                                    procedure: procedure.unwrap_or_default(),
                                    risk_adjusted,
                                },
                            })
                        }
                        "turnaround" => {
                            let mut procedure = None;
                            let mut target_hours = None;
                            let mut i = 2;
                            while i < remaining_args.len() {
                                match remaining_args[i].to_lowercase().as_str() {
                                    "--procedure" => {
                                        if i + 1 < remaining_args.len() {
                                            procedure = Some(remaining_args[i + 1].clone());
                                            i += 2;
                                        } else { i += 1; }
                                    }
                                    "--target-hours" => {
                                        if i + 1 < remaining_args.len() {
                                            target_hours = remaining_args[i + 1].parse::<i64>().ok();
                                            i += 2;
                                        } else { i += 1; }
                                    }
                                    _ => i += 1,
                                }
                            }
                            CommandType::Procedure(ProcedureCommand::Analytics {
                                analytics_type: ProcedureAnalyticsType::Turnaround {
                                    procedure: procedure.unwrap_or_default(),
                                    target_hours,
                                },
                            })
                        }
                        "cancellations" => CommandType::Procedure(ProcedureCommand::Analytics {
                            analytics_type: ProcedureAnalyticsType::Cancellations,
                        }),
                        "provider-volume" => {
                            let provider_id = remaining_args.get(2).and_then(|s| s.parse::<i32>().ok()).unwrap_or(0);
                            let mut timeframe = None;
                            let mut i = 3;
                            while i < remaining_args.len() {
                                match remaining_args[i].to_lowercase().as_str() {
                                    "--timeframe" => {
                                        if i + 1 < remaining_args.len() {
                                            timeframe = Some(remaining_args[i + 1].clone());
                                            i += 2;
                                        } else { i += 1; }
                                    }
                                    _ => i += 1,
                                }
                            }
                            CommandType::Procedure(ProcedureCommand::Analytics {
                                analytics_type: ProcedureAnalyticsType::ProviderVolume {
                                    provider_id,
                                    timeframe,
                                },
                            })
                        }
                        "outcomes" => {
                            let mut procedure = None;
                            let mut outcome_metric = None;
                            let mut i = 2;
                            while i < remaining_args.len() {
                                match remaining_args[i].to_lowercase().as_str() {
                                    "--procedure" => {
                                        if i + 1 < remaining_args.len() {
                                            procedure = Some(remaining_args[i + 1].clone());
                                            i += 2;
                                        } else { i += 1; }
                                    }
                                    "--outcome-metric" => {
                                        if i + 1 < remaining_args.len() {
                                            outcome_metric = Some(remaining_args[i + 1].clone());
                                            i += 2;
                                        } else { i += 1; }
                                    }
                                    _ => i += 1,
                                }
                            }
                            CommandType::Procedure(ProcedureCommand::Analytics {
                                analytics_type: ProcedureAnalyticsType::Outcomes {
                                    procedure: procedure.unwrap_or_default(),
                                    outcome_metric,
                                },
                            })
                        }
                        _ => CommandType::Unknown,
                    }
                }
                _ => CommandType::Unknown,
            }
        },

        "triage" => {
            if remaining_args.is_empty() || remaining_args[0].to_lowercase() != "assess" {
                eprintln!("Usage: triage assess --encounter <uuid> --level <ESI1-5> --complaint <text> [--symptoms] [--pain-score] [--notes]");
                return (CommandType::Unknown, remaining_args);
            }

            let mut encounter_id = None;
            let mut nurse_id = None;
            let mut level = None;
            let mut chief_complaint = None;
            let mut symptoms = None;
            let mut pain_score = None;
            let mut notes = None;
            let mut i = 1;
            while i < remaining_args.len() {
                match remaining_args[i].to_lowercase().as_str() {
                    "--encounter" => {
                        if i + 1 < remaining_args.len() {
                            encounter_id = Uuid::parse_str(&remaining_args[i + 1]).ok();
                            i += 2;
                        } else { i += 1; }
                    }
                    "--nurse-id" => {
                        if i + 1 < remaining_args.len() {
                            nurse_id = remaining_args[i + 1].parse::<i32>().ok();
                            i += 2;
                        } else { i += 1; }
                    }
                    "--level" => {
                        if i + 1 < remaining_args.len() {
                            level = Some(remaining_args[i + 1].clone());
                            i += 2;
                        } else { i += 1; }
                    }
                    "--complaint" => {
                        if i + 1 < remaining_args.len() {
                            chief_complaint = Some(remaining_args[i + 1].clone());
                            i += 2;
                        } else { i += 1; }
                    }
                    "--symptoms" => {
                        if i + 1 < remaining_args.len() {
                            symptoms = Some(remaining_args[i + 1].clone());
                            i += 2;
                        } else { i += 1; }
                    }
                    "--pain-score" | "--pain" => {
                        if i + 1 < remaining_args.len() {
                            pain_score = remaining_args[i + 1].parse::<i32>().ok();
                            i += 2;
                        } else { i += 1; }
                    }
                    "--notes" => {
                        if i + 1 < remaining_args.len() {
                            notes = Some(remaining_args[i + 1].clone());
                            i += 2;
                        } else { i += 1; }
                    }
                    _ => {
                        if chief_complaint.is_none() {
                            chief_complaint = Some(remaining_args[i].clone());
                        }
                        i += 1;
                    }
                }
            }

            CommandType::Triage(TriageCommand::Assess {
                encounter_id: encounter_id.unwrap_or_default(),
                nurse_id: nurse_id.unwrap_or(0),
                level: level.unwrap_or_default(),
                chief_complaint: chief_complaint.unwrap_or_default(),
                symptoms,
                pain_score,
                notes,
            })
        },
        "disposition" => {
            if remaining_args.is_empty() || remaining_args[0].to_lowercase() != "set" {
                eprintln!("Usage: disposition set --encounter <uuid> <disposition> [--admitting-service] [--admitting-doctor] [--transfer-facility] [--instructions]");
                return (CommandType::Unknown, remaining_args);
            }
            let mut encounter_id = None;
            let mut disposition_type = DispositionTarget::Home;
            let mut admitting_service = None;
            let mut admitting_doctor = None;
            let mut transfer_facility = None;
            let mut instructions = None;
            let mut i = 1;
            while i < remaining_args.len() {
                match remaining_args[i].to_lowercase().as_str() {
                    "--encounter" => {
                        if i + 1 < remaining_args.len() {
                            encounter_id = Uuid::parse_str(&remaining_args[i + 1]).ok();
                            i += 2;
                        } else { i += 1; }
                    }
                    "--admitting-service" => {
                        if i + 1 < remaining_args.len() {
                            admitting_service = Some(remaining_args[i + 1].clone());
                            i += 2;
                        } else { i += 1; }
                    }
                    "--admitting-doctor" => {
                        if i + 1 < remaining_args.len() {
                            admitting_doctor = remaining_args[i + 1].parse::<i32>().ok();
                            i += 2;
                        } else { i += 1; }
                    }
                    "--transfer-facility" => {
                        if i + 1 < remaining_args.len() {
                            transfer_facility = remaining_args[i + 1].parse::<i32>().ok();
                            i += 2;
                        } else { i += 1; }
                    }
                    "--instructions" => {
                        if i + 1 < remaining_args.len() {
                            instructions = Some(remaining_args[i + 1].clone());
                            i += 2;
                        } else { i += 1; }
                    }
                    _ => {
                        disposition_type = match remaining_args[i].to_lowercase().as_str() {
                            "home" => DispositionTarget::Home,
                            "homehealth" => DispositionTarget::HomeHealth,
                            "snf" => DispositionTarget::SkilledNursing,
                            "rehab" => DispositionTarget::Rehab,
                            "ltach" => DispositionTarget::LTACH,
                            "hospice" => DispositionTarget::Hospice,
                            "ama" => DispositionTarget::AgainstMedicalAdvice,
                            "expired" => DispositionTarget::Expired,
                            "transfer" => DispositionTarget::Transfer,
                            _ => DispositionTarget::Home,
                        };
                        i += 1;
                    }
                }
            }
             CommandType::Disposition(DispositionCommand::Set {
                encounter_id: encounter_id.unwrap_or_default(),
                disposition_type: disposition_type.to_string(), // <-- 1. String
                admitting_service,
                admitting_doctor,
                transfer_facility,
                instructions,
            })
        },

        "referral" => {
            if remaining_args.is_empty() || remaining_args[0].to_lowercase() != "create" {
                eprintln!("Usage: referral create --encounter <uuid> --to-doctor <id> <specialty> <reason> [--urgency]");
                return (CommandType::Unknown, remaining_args);
            }
            let mut encounter_id = None;
            let mut to_doctor_id = None;
            let mut specialty = None;
            let mut reason = None;
            let mut urgency = None;
            let mut i = 1;
            while i < remaining_args.len() {
                match remaining_args[i].to_lowercase().as_str() {
                    "--encounter" => {
                        if i + 1 < remaining_args.len() {
                            encounter_id = Uuid::parse_str(&remaining_args[i + 1]).ok();
                            i += 2;
                        } else { i += 1; }
                    }
                    "--to-doctor" => {
                        if i + 1 < remaining_args.len() {
                            to_doctor_id = remaining_args[i + 1].parse::<i32>().ok();
                            i += 2;
                        } else { i += 1; }
                    }
                    "--specialty" => {
                        if i + 1 < remaining_args.len() {
                            specialty = Some(remaining_args[i + 1].clone());
                            i += 2;
                        } else { i += 1; }
                    }
                    "--reason" => {
                        if i + 1 < remaining_args.len() {
                            reason = Some(remaining_args[i + 1].clone());
                            i += 2;
                        } else { i += 1; }
                    }
                    "--urgency" => {
                        if i + 1 < remaining_args.len() {
                            urgency = Some(match remaining_args[i + 1].to_lowercase().as_str() {
                                "stat" | "emergent" => "STAT".to_string(),
                                "urgent" => "URGENT".to_string(),
                                _ => "ROUTINE".to_string(),
                            });
                            i += 2;
                        } else { i += 1; }
                    }
                    _ => {
                        if reason.is_none() {
                            reason = Some(remaining_args[i].clone());
                        }
                        i += 1;
                    }
                }
            }
            CommandType::Referral(ReferralCommand::Create {
                encounter_id: encounter_id.unwrap_or_default(),
                to_doctor_id: to_doctor_id.unwrap_or(0),
                specialty: specialty.unwrap_or_default(),
                reason: reason.unwrap_or_default(),
                urgency: urgency.unwrap_or_default(), // <- now a plain String
            })
        },

        "vitals" => {
            if remaining_args.is_empty() || remaining_args[0].to_lowercase() != "add" {
                eprintln!("Usage: vitals add --encounter <uuid> --bp <value> [--hr] [--temp] [--rr] [--spo2] [--pain-score] [--height] [--weight]");
                return (CommandType::Unknown, remaining_args);
            }
            let mut encounter_id = None;
            let mut bp = None;
            let mut hr = None;
            let mut temp = None;
            let mut rr = None;
            let mut spo2 = None;
            let mut pain_score = None;
            let mut height = None;
            let mut weight = None;
            let mut i = 1;
            while i < remaining_args.len() {
                match remaining_args[i].to_lowercase().as_str() {
                    "--encounter" => {
                        if i + 1 < remaining_args.len() {
                            encounter_id = Uuid::parse_str(&remaining_args[i + 1]).ok();
                            i += 2;
                        } else { i += 1; }
                    }
                    "--bp" => {
                        if i + 1 < remaining_args.len() {
                            bp = Some(remaining_args[i + 1].clone());
                            i += 2;
                        } else { i += 1; }
                    }
                    "--hr" => {
                        if i + 1 < remaining_args.len() {
                            hr = remaining_args[i + 1].parse::<i32>().ok();
                            i += 2;
                        } else { i += 1; }
                    }
                    "--temp" => {
                        if i + 1 < remaining_args.len() {
                            temp = remaining_args[i + 1].parse::<f32>().ok();
                            i += 2;
                        } else { i += 1; }
                    }
                    "--rr" => {
                        if i + 1 < remaining_args.len() {
                            rr = remaining_args[i + 1].parse::<i32>().ok();
                            i += 2;
                        } else { i += 1; }
                    }
                    "--spo2" => {
                        if i + 1 < remaining_args.len() {
                            spo2 = remaining_args[i + 1].parse::<i32>().ok();
                            i += 2;
                        } else { i += 1; }
                    }
                    "--pain-score" | "--pain" => {
                        if i + 1 < remaining_args.len() {
                            pain_score = remaining_args[i + 1].parse::<i32>().ok();
                            i += 2;
                        } else { i += 1; }
                    }
                    "--height" => {
                        if i + 1 < remaining_args.len() {
                            height = remaining_args[i + 1].parse::<f32>().ok();
                            i += 2;
                        } else { i += 1; }
                    }
                    "--weight" => {
                        if i + 1 < remaining_args.len() {
                            weight = remaining_args[i + 1].parse::<f32>().ok();
                            i += 2;
                        } else { i += 1; }
                    }
                    _ => i += 1,
                }
            }
            CommandType::Vitals(VitalsCommand::Add {
                encounter_id: encounter_id.unwrap_or_default(),
                bp,
                hr,
                temp,
                rr,
                spo2,
                pain_score,
                height,
                weight,
            })
        },

        "observation" => {
                    if remaining_args.is_empty() {
                        eprintln!("Usage: observation <add|list> [args...]");
                        return (CommandType::Unknown, remaining_args);
                    }
                    match remaining_args[0].to_lowercase().as_str() {
                        "add" => {
                            let mut encounter_id = None;
                            let mut observation_type = None;
                            let mut value = None;
                            let mut unit = None;
                            let mut observed_by = None;
                            let mut notes = None;
                            let mut i = 1;
                            while i < remaining_args.len() {
                                match remaining_args[i].to_lowercase().as_str() {
                                    "--encounter" => {
                                        if i + 1 < remaining_args.len() {
                                            encounter_id = Uuid::parse_str(&remaining_args[i + 1]).ok();
                                            i += 2;
                                        } else { i += 1; }
                                    }
                                    "--type" => {
                                        if i + 1 < remaining_args.len() {
                                            observation_type = Some(remaining_args[i + 1].clone());
                                            i += 2;
                                        } else { i += 1; }
                                    }
                                    "--value" => {
                                        if i + 1 < remaining_args.len() {
                                            value = Some(remaining_args[i + 1].clone());
                                            i += 2;
                                        } else { i += 1; }
                                    }
                                    "--unit" => {
                                        if i + 1 < remaining_args.len() {
                                            unit = Some(remaining_args[i + 1].clone());
                                            i += 2;
                                        } else { i += 1; }
                                    }
                                    "--observed-by" => {
                                        if i + 1 < remaining_args.len() {
                                            observed_by = remaining_args[i + 1].parse::<i32>().ok();
                                            i += 2;
                                        } else { i += 1; }
                                    }
                                    "--notes" => {
                                        if i + 1 < remaining_args.len() {
                                            notes = Some(remaining_args[i + 1].clone());
                                            i += 2;
                                        } else { i += 1; }
                                    }
                                    _ => {
                                        if observation_type.is_none() {
                                            observation_type = Some(remaining_args[i].clone());
                                        } else if value.is_none() {
                                            value = Some(remaining_args[i].clone());
                                        }
                                        i += 1;
                                    }
                                }
                            }
                            CommandType::Observation(ObservationCommand::Add {
                                encounter_id: encounter_id.unwrap_or_default(),
                                observation_type: observation_type.unwrap_or_default(),
                                value: value.unwrap_or_default(),
                                unit,
                                observed_by,
                                notes,
                            })
                        }
                        "list" => {
                            let mut encounter_id = None;
                            let mut type_filter = None;
                            let mut limit = None;
                            let mut i = 1;
                            while i < remaining_args.len() {
                                match remaining_args[i].to_lowercase().as_str() {
                                    "--encounter" => {
                                        if i + 1 < remaining_args.len() {
                                            encounter_id = Uuid::parse_str(&remaining_args[i + 1]).ok();
                                            i += 2;
                                        } else { i += 1; }
                                    }
                                    "--type-filter" => {
                                        if i + 1 < remaining_args.len() {
                                            type_filter = Some(remaining_args[i + 1].clone());
                                            i += 2;
                                        } else { i += 1; }
                                    }
                                    "--limit" => {
                                        if i + 1 < remaining_args.len() {
                                            limit = remaining_args[i + 1].parse::<usize>().ok();
                                            i += 2;
                                        } else { i += 1; }
                                    }
                                    _ => i += 1,
                                }
                            }
                            CommandType::Observation(ObservationCommand::List {
                                encounter_id: encounter_id.unwrap_or_default(),
                                type_filter,
                                limit,
                            })
                        }
                        _ => CommandType::Unknown,
                    }
                },
        "lab" => {
            if remaining_args.is_empty() {
                eprintln!("Usage: lab <order|result|trend> [args...]");
                return (CommandType::Unknown, remaining_args);
            }
            match remaining_args[0].to_lowercase().as_str() {
                "order" => {
                    let mut encounter_id = None;
                    let mut tests = Vec::new();
                    let mut priority = None;
                    let mut clinical_indication = None;
                    let mut collect_time = None;
                    let mut i = 1;
                    while i < remaining_args.len() {
                        match remaining_args[i].to_lowercase().as_str() {
                            "--encounter" => {
                                if i + 1 < remaining_args.len() {
                                    encounter_id = Uuid::parse_str(&remaining_args[i + 1]).ok();
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--priority" => {
                                if i + 1 < remaining_args.len() {
                                    priority = match remaining_args[i + 1].to_lowercase().as_str() {
                                        "routine" => Some(LabPriority::Routine),
                                        "urgent" => Some(LabPriority::Urgent),
                                        "stat" => Some(LabPriority::Stat),
                                        _ => None,
                                    };
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--clinical-indication" => {
                                if i + 1 < remaining_args.len() {
                                    clinical_indication = Some(remaining_args[i + 1].clone());
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--collect-time" => {
                                if i + 1 < remaining_args.len() {
                                    collect_time = Some(remaining_args[i + 1].clone());
                                    i += 2;
                                } else { i += 1; }
                            }
                            _ => {
                                tests.push(remaining_args[i].clone());
                                i += 1;
                            }
                        }
                    }
                    CommandType::Lab(LabCommand::Order {
                        encounter_id: encounter_id.unwrap_or_default(),
                        tests,
                        priority,
                        clinical_indication,
                        collect_time,
                    })
                }
                "result" => {
                    let mut order_id = None;
                    let mut test_name = None;
                    let mut value = None;
                    let mut unit = None;
                    let mut reference_range = None;
                    let mut flag = None;
                    let mut critical = false;
                    let mut i = 1;
                    while i < remaining_args.len() {
                        match remaining_args[i].to_lowercase().as_str() {
                            "--order-id" => {
                                if i + 1 < remaining_args.len() {
                                    order_id = Uuid::parse_str(&remaining_args[i + 1]).ok();
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--test-name" => {
                                if i + 1 < remaining_args.len() {
                                    test_name = Some(remaining_args[i + 1].clone());
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--value" => {
                                if i + 1 < remaining_args.len() {
                                    value = Some(remaining_args[i + 1].clone());
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--unit" => {
                                if i + 1 < remaining_args.len() {
                                    unit = Some(remaining_args[i + 1].clone());
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--reference-range" => {
                                if i + 1 < remaining_args.len() {
                                    reference_range = Some(remaining_args[i + 1].clone());
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--flag" => {
                                if i + 1 < remaining_args.len() {
                                    flag = match remaining_args[i + 1].to_lowercase().as_str() {
                                        "low" => Some(LabFlag::Low),
                                        "high" => Some(LabFlag::High),
                                        "critical-low" => Some(LabFlag::CriticalLow),
                                        "critical-high" => Some(LabFlag::CriticalHigh),
                                        "abnormal" => Some(LabFlag::Abnormal),
                                        "normal" => Some(LabFlag::Normal),
                                        _ => None,
                                    };
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--critical" => {
                                critical = true;
                                i += 1;
                            }
                            _ => i += 1,
                        }
                    }
                    CommandType::Lab(LabCommand::Result {
                        order_id: order_id.unwrap_or_default(),
                        test_name: test_name.unwrap_or_default(),
                        value: value.unwrap_or_default(),
                        unit,
                        reference_range,
                        flag,
                        critical,
                    })
                }
                "trend" => {
                    let mut patient_id = None;
                    let mut test_name = None;
                    let mut days = None;
                    let mut i = 1;
                    while i < remaining_args.len() {
                        match remaining_args[i].to_lowercase().as_str() {
                            "--patient-id" => {
                                if i + 1 < remaining_args.len() {
                                    patient_id = remaining_args[i + 1].parse::<i32>().ok();
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--test-name" => {
                                if i + 1 < remaining_args.len() {
                                    test_name = Some(remaining_args[i + 1].clone());
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--days" => {
                                if i + 1 < remaining_args.len() {
                                    days = remaining_args[i + 1].parse::<i64>().ok();
                                    i += 2;
                                } else { i += 1; }
                            }
                            _ => i += 1,
                        }
                    }
                    CommandType::Lab(LabCommand::Trend {
                        patient_id: patient_id.unwrap_or(0),
                        test_name: test_name.unwrap_or_default(),
                        days,
                    })
                }
                _ => CommandType::Unknown,
            }
        },
        "imaging" => {
            if remaining_args.is_empty() {
                eprintln!("Usage: imaging <order|preliminary|final|compare> [args...]");
                return (CommandType::Unknown, remaining_args);
            }
            match remaining_args[0].to_lowercase().as_str() {
                "order" => {
                    let mut encounter_id = None;
                    let mut study = None;
                    let mut priority = None;
                    let mut indication = None;
                    let mut contrast = None;
                    let mut modality = None;
                    let mut i = 1;
                    while i < remaining_args.len() {
                        match remaining_args[i].to_lowercase().as_str() {
                            "--encounter" => {
                                if i + 1 < remaining_args.len() {
                                    encounter_id = Uuid::parse_str(&remaining_args[i + 1]).ok();
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--study" => {
                                if i + 1 < remaining_args.len() {
                                    study = Some(remaining_args[i + 1].clone());
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--priority" => {
                                if i + 1 < remaining_args.len() {
                                    priority = match remaining_args[i + 1].to_lowercase().as_str() {
                                        "routine" => Some(ImagingPriority::Routine),
                                        "urgent" => Some(ImagingPriority::Urgent),
                                        "stat" => Some(ImagingPriority::Stat),
                                        _ => None,
                                    };
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--indication" => {
                                if i + 1 < remaining_args.len() {
                                    indication = Some(remaining_args[i + 1].clone());
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--contrast" => {
                                contrast = Some(true);
                                i += 1;
                            }
                            "--modality" => {
                                if i + 1 < remaining_args.len() {
                                    modality = Some(remaining_args[i + 1].clone());
                                    i += 2;
                                } else { i += 1; }
                            }
                            _ => {
                                if study.is_none() {
                                    study = Some(remaining_args[i].clone());
                                }
                                i += 1;
                            }
                        }
                    }
                    CommandType::Imaging(ImagingCommand::Order {
                        encounter_id: encounter_id.unwrap_or_default(),
                        study: study.unwrap_or_default(),
                        priority,
                        indication,
                        contrast,
                        modality,
                    })
                }
                "preliminary" => {
                    let mut study_id = None;
                    let mut finding = None;
                    let mut impression = None;
                    let mut radiologist = None;
                    let mut i = 1;
                    while i < remaining_args.len() {
                        match remaining_args[i].to_lowercase().as_str() {
                            "--study-id" => {
                                if i + 1 < remaining_args.len() {
                                    study_id = Uuid::parse_str(&remaining_args[i + 1]).ok();
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--finding" => {
                                if i + 1 < remaining_args.len() {
                                    finding = Some(remaining_args[i + 1].clone());
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--impression" => {
                                if i + 1 < remaining_args.len() {
                                    impression = Some(remaining_args[i + 1].clone());
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--radiologist" => {
                                if i + 1 < remaining_args.len() {
                                    radiologist = remaining_args[i + 1].parse::<i32>().ok();
                                    i += 2;
                                } else { i += 1; }
                            }
                            _ => {
                                if finding.is_none() {
                                    finding = Some(remaining_args[i].clone());
                                }
                                i += 1;
                            }
                        }
                    }
                    CommandType::Imaging(ImagingCommand::Preliminary {
                        study_id: study_id.unwrap_or_default(),
                        finding: finding.unwrap_or_default(),
                        impression,
                        radiologist,
                    })
                }
                "final" => {
                    let mut study_id = None;
                    let mut report = None;
                    let mut critical_finding = None;
                    let mut radiologist = None;
                    let mut i = 1;
                    while i < remaining_args.len() {
                        match remaining_args[i].to_lowercase().as_str() {
                            "--study-id" => {
                                if i + 1 < remaining_args.len() {
                                    study_id = Uuid::parse_str(&remaining_args[i + 1]).ok();
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--report" => {
                                if i + 1 < remaining_args.len() {
                                    report = Some(remaining_args[i + 1].clone());
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--critical-finding" => {
                                critical_finding = Some(true);
                                i += 1;
                            }
                            "--radiologist" => {
                                if i + 1 < remaining_args.len() {
                                    radiologist = remaining_args[i + 1].parse::<i32>().ok();
                                    i += 2;
                                } else { i += 1; }
                            }
                            _ => {
                                if report.is_none() {
                                    report = Some(remaining_args[i].clone());
                                }
                                i += 1;
                            }
                        }
                    }
                    CommandType::Imaging(ImagingCommand::Final {
                        study_id: study_id.unwrap_or_default(),
                        report: report.unwrap_or_default(),
                        critical_finding,
                        radiologist,
                    })
                }
                "compare" => {
                    let mut patient_id = None;
                    let mut study_type = None;
                    let mut prior_date = None;
                    let mut i = 1;
                    while i < remaining_args.len() {
                        match remaining_args[i].to_lowercase().as_str() {
                            "--patient-id" => {
                                if i + 1 < remaining_args.len() {
                                    patient_id = remaining_args[i + 1].parse::<i32>().ok();
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--study-type" => {
                                if i + 1 < remaining_args.len() {
                                    study_type = Some(remaining_args[i + 1].clone());
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--prior-date" => {
                                if i + 1 < remaining_args.len() {
                                    prior_date = Some(remaining_args[i + 1].clone());
                                    i += 2;
                                } else { i += 1; }
                            }
                            _ => {
                                if study_type.is_none() {
                                    study_type = Some(remaining_args[i].clone());
                                }
                                i += 1;
                            }
                        }
                    }
                    CommandType::Imaging(ImagingCommand::Compare {
                        patient_id: patient_id.unwrap_or(0),
                        study_type: study_type.unwrap_or_default(),
                        prior_date,
                    })
                }
                _ => CommandType::Unknown,
            }
        },
        "chemo" => {
            if remaining_args.is_empty() {
                eprintln!("Usage: chemo <regimen|cycle-add|labs-verify|cycle-modify|toxicity> [args...]");
                return (CommandType::Unknown, remaining_args);
            }
            match remaining_args[0].to_lowercase().as_str() {
                "regimen" => {
                    let mut patient_id = None;
                    let mut regimen_name = None;
                    let mut diagnosis = None;
                    let mut intent = None;
                    let mut start_date = None;
                    let mut i = 1;
                    while i < remaining_args.len() {
                        match remaining_args[i].to_lowercase().as_str() {
                            "--patient-id" => {
                                if i + 1 < remaining_args.len() {
                                    patient_id = remaining_args[i + 1].parse::<i32>().ok();
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--diagnosis" => {
                                if i + 1 < remaining_args.len() {
                                    diagnosis = Some(remaining_args[i + 1].clone());
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--intent" => {
                                if i + 1 < remaining_args.len() {
                                    intent = match remaining_args[i + 1].to_lowercase().as_str() {
                                        "curative" => Some(ChemoIntent::Curative),
                                        "adjuvant" => Some(ChemoIntent::Adjuvant),
                                        "neoadjuvant" => Some(ChemoIntent::Neoadjuvant),
                                        "palliative" => Some(ChemoIntent::Palliative),
                                        _ => None,
                                    };
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--start-date" => {
                                if i + 1 < remaining_args.len() {
                                    start_date = Some(remaining_args[i + 1].clone());
                                    i += 2;
                                } else { i += 1; }
                            }
                            _ => {
                                if regimen_name.is_none() {
                                    regimen_name = Some(remaining_args[i].clone());
                                }
                                i += 1;
                            }
                        }
                    }
                    CommandType::Chemo(ChemoCommand::Regimen {
                        patient_id: patient_id.unwrap_or(0),
                        regimen_name: regimen_name.unwrap_or_default(),
                        diagnosis: diagnosis.unwrap_or_default(),
                        intent,
                        start_date,
                    })
                }
                "cycle-add" => {
                    let mut regimen_id = None;
                    let mut cycle_number = None;
                    let mut planned_date = None;
                    let mut pre_meds = Vec::new();
                    let mut i = 1;
                    while i < remaining_args.len() {
                        match remaining_args[i].to_lowercase().as_str() {
                            "--regimen-id" => {
                                if i + 1 < remaining_args.len() {
                                    regimen_id = Uuid::parse_str(&remaining_args[i + 1]).ok();
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--cycle-number" => {
                                if i + 1 < remaining_args.len() {
                                    cycle_number = remaining_args[i + 1].parse::<i32>().ok();
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--planned-date" => {
                                if i + 1 < remaining_args.len() {
                                    planned_date = Some(remaining_args[i + 1].clone());
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--pre-med" => {
                                if i + 1 < remaining_args.len() {
                                    pre_meds.push(remaining_args[i + 1].clone());
                                    i += 2;
                                } else { i += 1; }
                            }
                            _ => i += 1,
                        }
                    }
                    CommandType::Chemo(ChemoCommand::CycleAdd {
                        regimen_id: regimen_id.unwrap_or_default(),
                        cycle_number: cycle_number.unwrap_or(0),
                        planned_date: planned_date.unwrap_or_default(),
                        pre_meds,
                    })
                }
                "labs-verify" => {
                    let mut regimen_id = None;
                    let mut cycle_number = None;
                    let mut required_labs = Vec::new();
                    let mut i = 1;
                    while i < remaining_args.len() {
                        match remaining_args[i].to_lowercase().as_str() {
                            "--regimen-id" => {
                                if i + 1 < remaining_args.len() {
                                    regimen_id = Uuid::parse_str(&remaining_args[i + 1]).ok();
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--cycle-number" => {
                                if i + 1 < remaining_args.len() {
                                    cycle_number = remaining_args[i + 1].parse::<i32>().ok();
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--required-labs" => {
                                if i + 1 < remaining_args.len() {
                                    required_labs.push(remaining_args[i + 1].clone());
                                    i += 2;
                                } else { i += 1; }
                            }
                            _ => i += 1,
                        }
                    }
                    CommandType::Chemo(ChemoCommand::LabsVerify {
                        regimen_id: regimen_id.unwrap_or_default(),
                        cycle_number: cycle_number.unwrap_or(0),
                        required_labs,
                    })
                }
                "cycle-modify" => {
                    let mut cycle_id = None;
                    let mut action = None;
                    let mut reason = None;
                    let mut dose_reduction_pct = None;
                    let mut i = 1;
                    while i < remaining_args.len() {
                        match remaining_args[i].to_lowercase().as_str() {
                            "--cycle-id" => {
                                if i + 1 < remaining_args.len() {
                                    cycle_id = Uuid::parse_str(&remaining_args[i + 1]).ok();
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--action" => {
                                if i + 1 < remaining_args.len() {
                                    action = match remaining_args[i + 1].to_lowercase().as_str() {
                                        "hold" => Some(ChemoAction::Hold),
                                        "delay" => Some(ChemoAction::Delay),
                                        "reduce" => Some(ChemoAction::Reduce),
                                        "discontinue" => Some(ChemoAction::Discontinue),
                                        _ => None,
                                    };
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--reason" => {
                                if i + 1 < remaining_args.len() {
                                    reason = Some(remaining_args[i + 1].clone());
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--dose-reduction-pct" => {
                                if i + 1 < remaining_args.len() {
                                    dose_reduction_pct = remaining_args[i + 1].parse::<i32>().ok();
                                    i += 2;
                                } else { i += 1; }
                            }
                            _ => i += 1,
                        }
                    }
                    CommandType::Chemo(ChemoCommand::CycleModify {
                        cycle_id: cycle_id.unwrap_or_default(),
                        action: action.unwrap_or(ChemoAction::Hold),
                        reason,
                        dose_reduction_pct,
                    })
                }
                "toxicity" => {
                    let mut patient_id = None;
                    let mut cycle_id = None;
                    let mut term = None;
                    let mut grade = None;
                    let mut management = None;
                    let mut i = 1;
                    while i < remaining_args.len() {
                        match remaining_args[i].to_lowercase().as_str() {
                            "--patient-id" => {
                                if i + 1 < remaining_args.len() {
                                    patient_id = remaining_args[i + 1].parse::<i32>().ok();
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--cycle-id" => {
                                if i + 1 < remaining_args.len() {
                                    cycle_id = Uuid::parse_str(&remaining_args[i + 1]).ok();
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--term" => {
                                if i + 1 < remaining_args.len() {
                                    term = Some(remaining_args[i + 1].clone());
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--grade" => {
                                if i + 1 < remaining_args.len() {
                                    grade = remaining_args[i + 1].parse::<i32>().ok();
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--management" => {
                                if i + 1 < remaining_args.len() {
                                    management = Some(remaining_args[i + 1].clone());
                                    i += 2;
                                } else { i += 1; }
                            }
                            _ => i += 1,
                        }
                    }
                    CommandType::Chemo(ChemoCommand::Toxicity {
                        patient_id: patient_id.unwrap_or(0),
                        cycle_id,
                        term: term.unwrap_or_default(),
                        grade: grade.unwrap_or(0),
                        management,
                    })
                }
                _ => CommandType::Unknown,
            }
        },
        "radiation" => {
            if remaining_args.is_empty() {
                eprintln!("Usage: radiation <plan|treatment|toxicity> [args...]");
                return (CommandType::Unknown, remaining_args);
            }
            match remaining_args[0].to_lowercase().as_str() {
                "plan" => {
                    let mut patient_id = None;
                    let mut site = None;
                    let mut technique = None;
                    let mut total_dose_gy = None;
                    let mut fractions = None;
                    let mut start_date = None;
                    let mut i = 1;
                    while i < remaining_args.len() {
                        match remaining_args[i].to_lowercase().as_str() {
                            "--patient-id" => {
                                if i + 1 < remaining_args.len() {
                                    patient_id = remaining_args[i + 1].parse::<i32>().ok();
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--site" => {
                                if i + 1 < remaining_args.len() {
                                    site = Some(remaining_args[i + 1].clone());
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--technique" => {
                                if i + 1 < remaining_args.len() {
                                    technique = Some(remaining_args[i + 1].clone());
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--total-dose-gy" => {
                                if i + 1 < remaining_args.len() {
                                    total_dose_gy = remaining_args[i + 1].parse::<f32>().ok();
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--fractions" => {
                                if i + 1 < remaining_args.len() {
                                    fractions = remaining_args[i + 1].parse::<i32>().ok();
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--start-date" => {
                                if i + 1 < remaining_args.len() {
                                    start_date = Some(remaining_args[i + 1].clone());
                                    i += 2;
                                } else { i += 1; }
                            }
                            _ => i += 1,
                        }
                    }
                    CommandType::Radiation(RadiationCommand::Plan {
                        patient_id: patient_id.unwrap_or(0),
                        site: site.unwrap_or_default(),
                        technique: technique.unwrap_or_default(),
                        total_dose_gy: total_dose_gy.unwrap_or(0.0),
                        fractions: fractions.unwrap_or(0),
                        start_date,
                    })
                }
                "treatment" => {
                    let mut plan_id = None;
                    let mut fraction = None;
                    let mut dose_delivered = None;
                    let mut image_guidance = None;
                    let mut toxicity = None;
                    let mut i = 1;
                    while i < remaining_args.len() {
                        match remaining_args[i].to_lowercase().as_str() {
                            "--plan-id" => {
                                if i + 1 < remaining_args.len() {
                                    plan_id = Uuid::parse_str(&remaining_args[i + 1]).ok();
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--fraction" => {
                                if i + 1 < remaining_args.len() {
                                    fraction = remaining_args[i + 1].parse::<i32>().ok();
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--dose-delivered" => {
                                if i + 1 < remaining_args.len() {
                                    dose_delivered = remaining_args[i + 1].parse::<f32>().ok();
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--image-guidance" => {
                                if i + 1 < remaining_args.len() {
                                    image_guidance = Some(remaining_args[i + 1].clone());
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--toxicity" => {
                                if i + 1 < remaining_args.len() {
                                    toxicity = Some(remaining_args[i + 1].clone());
                                    i += 2;
                                } else { i += 1; }
                            }
                            _ => i += 1,
                        }
                    }
                    CommandType::Radiation(RadiationCommand::Treatment {
                        plan_id: plan_id.unwrap_or_default(),
                        fraction: fraction.unwrap_or(0),
                        dose_delivered: dose_delivered.unwrap_or(0.0),
                        image_guidance,
                        toxicity,
                    })
                }
                "toxicity" => {
                    let mut patient_id = None;
                    let mut grade = None;
                    let mut organ = None;
                    let mut management = None;
                    let mut i = 1;
                    while i < remaining_args.len() {
                        match remaining_args[i].to_lowercase().as_str() {
                            "--patient-id" => {
                                if i + 1 < remaining_args.len() {
                                    patient_id = remaining_args[i + 1].parse::<i32>().ok();
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--grade" => {
                                if i + 1 < remaining_args.len() {
                                    grade = remaining_args[i + 1].parse::<i32>().ok();
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--organ" => {
                                if i + 1 < remaining_args.len() {
                                    organ = Some(remaining_args[i + 1].clone());
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--management" => {
                                if i + 1 < remaining_args.len() {
                                    management = Some(remaining_args[i + 1].clone());
                                    i += 2;
                                } else { i += 1; }
                            }
                            _ => i += 1,
                        }
                    }
                    CommandType::Radiation(RadiationCommand::Toxicity {
                        patient_id: patient_id.unwrap_or(0),
                        grade: grade.unwrap_or(0),
                        organ: organ.unwrap_or_default(),
                        management,
                    })
                }
                _ => CommandType::Unknown,
            }
        },
        "surgery" => {
            if remaining_args.is_empty() {
                eprintln!("Usage: surgery <case-create|preop-checklist|case-start|event-add|case-close> [args...]");
                return (CommandType::Unknown, remaining_args);
            }
            match remaining_args[0].to_lowercase().as_str() {
                "case-create" => {
                    let mut patient_id = None;
                    let mut procedure = None;
                    let mut surgeon_id = None;
                    let mut date = None;
                    let mut time = None;
                    let mut location = None;
                    let mut i = 1;
                    while i < remaining_args.len() {
                        match remaining_args[i].to_lowercase().as_str() {
                            "--patient-id" => {
                                if i + 1 < remaining_args.len() {
                                    patient_id = remaining_args[i + 1].parse::<i32>().ok();
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--surgeon-id" => {
                                if i + 1 < remaining_args.len() {
                                    surgeon_id = remaining_args[i + 1].parse::<i32>().ok();
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--date" => {
                                if i + 1 < remaining_args.len() {
                                    date = Some(remaining_args[i + 1].clone());
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--time" => {
                                if i + 1 < remaining_args.len() {
                                    time = Some(remaining_args[i + 1].clone());
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--location" => {
                                if i + 1 < remaining_args.len() {
                                    location = Some(remaining_args[i + 1].clone());
                                    i += 2;
                                } else { i += 1; }
                            }
                            _ => {
                                if procedure.is_none() {
                                    procedure = Some(remaining_args[i].clone());
                                }
                                i += 1;
                            }
                        }
                    }
                    CommandType::Surgery(SurgeryCommand::CaseCreate {
                        patient_id: patient_id.unwrap_or(0),
                        procedure: procedure.unwrap_or_default(),
                        surgeon_id: surgeon_id.unwrap_or(0),
                        date: date.unwrap_or_default(),
                        time,
                        location,
                    })
                }
                "preop-checklist" => {
                    let mut case_id = None;
                    let mut consent = false;
                    let mut h_and_p = false;
                    let mut npo = false;
                    let mut antibiotics_given = None;
                    let mut i = 1;
                    while i < remaining_args.len() {
                        match remaining_args[i].to_lowercase().as_str() {
                            "--case-id" => {
                                if i + 1 < remaining_args.len() {
                                    case_id = Uuid::parse_str(&remaining_args[i + 1]).ok();
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--consent" => {
                                consent = true;
                                i += 1;
                            }
                            "--h-and-p" => {
                                h_and_p = true;
                                i += 1;
                            }
                            "--npo" => {
                                npo = true;
                                i += 1;
                            }
                            "--antibiotics-given" => {
                                if i + 1 < remaining_args.len() {
                                    antibiotics_given = Some(remaining_args[i + 1].clone());
                                    i += 2;
                                } else { i += 1; }
                            }
                            _ => i += 1,
                        }
                    }
                    CommandType::Surgery(SurgeryCommand::PreopChecklist {
                        case_id: case_id.unwrap_or_default(),
                        consent,
                        h_and_p,
                        npo,
                        antibiotics_given,
                    })
                }
                "case-start" => {
                    let mut case_id = None;
                    let mut anesthesia_start = None;
                    let mut incision_time = None;
                    let mut i = 1;
                    while i < remaining_args.len() {
                        match remaining_args[i].to_lowercase().as_str() {
                            "--case-id" => {
                                if i + 1 < remaining_args.len() {
                                    case_id = Uuid::parse_str(&remaining_args[i + 1]).ok();
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--anesthesia-start" => {
                                if i + 1 < remaining_args.len() {
                                    anesthesia_start = Some(remaining_args[i + 1].clone());
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--incision-time" => {
                                if i + 1 < remaining_args.len() {
                                    incision_time = Some(remaining_args[i + 1].clone());
                                    i += 2;
                                } else { i += 1; }
                            }
                            _ => i += 1,
                        }
                    }
                    CommandType::Surgery(SurgeryCommand::CaseStart {
                        case_id: case_id.unwrap_or_default(),
                        anesthesia_start,
                        incision_time,
                    })
                }
                "event-add" => {
                    let mut case_id = None;
                    let mut time = None;
                    let mut event = None;
                    let mut i = 1;
                    while i < remaining_args.len() {
                        match remaining_args[i].to_lowercase().as_str() {
                            "--case-id" => {
                                if i + 1 < remaining_args.len() {
                                    case_id = Uuid::parse_str(&remaining_args[i + 1]).ok();
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--time" => {
                                if i + 1 < remaining_args.len() {
                                    time = Some(remaining_args[i + 1].clone());
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--event" => {
                                if i + 1 < remaining_args.len() {
                                    event = Some(remaining_args[i + 1].clone());
                                    i += 2;
                                } else { i += 1; }
                            }
                            _ => {
                                if event.is_none() {
                                    event = Some(remaining_args[i].clone());
                                }
                                i += 1;
                            }
                        }
                    }
                    CommandType::Surgery(SurgeryCommand::EventAdd {
                        case_id: case_id.unwrap_or_default(),
                        time: time.unwrap_or_default(),
                        event: event.unwrap_or_default(),
                    })
                }
                "case-close" => {
                    let mut case_id = None;
                    let mut ebl_ml = None;
                    let mut complications = None;
                    let mut specimens = None;
                    let mut i = 1;
                    while i < remaining_args.len() {
                        match remaining_args[i].to_lowercase().as_str() {
                            "--case-id" => {
                                if i + 1 < remaining_args.len() {
                                    case_id = Uuid::parse_str(&remaining_args[i + 1]).ok();
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--ebl-ml" => {
                                if i + 1 < remaining_args.len() {
                                    ebl_ml = remaining_args[i + 1].parse::<i32>().ok();
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--complications" => {
                                if i + 1 < remaining_args.len() {
                                    complications = Some(remaining_args[i + 1].clone());
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--specimens" => {
                                if i + 1 < remaining_args.len() {
                                    specimens = Some(remaining_args[i + 1].clone());
                                    i += 2;
                                } else { i += 1; }
                            }
                            _ => i += 1,
                        }
                    }
                    CommandType::Surgery(SurgeryCommand::CaseClose {
                        case_id: case_id.unwrap_or_default(),
                        ebl_ml,
                        complications,
                        specimens,
                    })
                }
                _ => CommandType::Unknown,
            }
        },
        "nursing" => {
            if remaining_args.is_empty() {
                eprintln!("Usage: nursing <assessment|safety-check|intake-output|med-administration|care-plan|patient-education> [args...]");
                return (CommandType::Unknown, remaining_args);
            }
            match remaining_args[0].to_lowercase().as_str() {
                "assessment" => {
                    let mut patient_id = None;
                    let mut shift = None;
                    let mut pain_level = None;
                    let mut mobility = None;
                    let mut notes = None;
                    let mut i = 1;
                    while i < remaining_args.len() {
                        match remaining_args[i].to_lowercase().as_str() {
                            "--patient-id" => {
                                if i + 1 < remaining_args.len() {
                                    patient_id = remaining_args[i + 1].parse::<i32>().ok();
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--shift" => {
                                if i + 1 < remaining_args.len() {
                                    shift = Some(remaining_args[i + 1].clone());
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--pain-level" => {
                                if i + 1 < remaining_args.len() {
                                    pain_level = remaining_args[i + 1].parse::<i32>().ok();
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--mobility" => {
                                if i + 1 < remaining_args.len() {
                                    mobility = Some(remaining_args[i + 1].clone());
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--notes" => {
                                if i + 1 < remaining_args.len() {
                                    notes = Some(remaining_args[i + 1].clone());
                                    i += 2;
                                } else { i += 1; }
                            }
                            _ => i += 1,
                        }
                    }
                    CommandType::Nursing(NursingCommand::Assessment {
                        patient_id: patient_id.unwrap_or(0),
                        shift,
                        pain_level,
                        mobility,
                        notes,
                    })
                }
                "safety-check" => {
                    let mut patient_id = None;
                    let mut fall_risk = None;
                    let mut pressure_ulcer_risk = None;
                    let mut notes = None;
                    let mut i = 1;
                    while i < remaining_args.len() {
                        match remaining_args[i].to_lowercase().as_str() {
                            "--patient-id" => {
                                if i + 1 < remaining_args.len() {
                                    patient_id = remaining_args[i + 1].parse::<i32>().ok();
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--fall-risk" => {
                                if i + 1 < remaining_args.len() {
                                    fall_risk = Some(remaining_args[i + 1].clone());
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--pressure-ulcer-risk" => {
                                if i + 1 < remaining_args.len() {
                                    pressure_ulcer_risk = Some(remaining_args[i + 1].clone());
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--notes" => {
                                if i + 1 < remaining_args.len() {
                                    notes = Some(remaining_args[i + 1].clone());
                                    i += 2;
                                } else { i += 1; }
                            }
                            _ => i += 1,
                        }
                    }
                    CommandType::Nursing(NursingCommand::SafetyCheck {
                        patient_id: patient_id.unwrap_or(0),
                        fall_risk,
                        pressure_ulcer_risk,
                        notes,
                    })
                }
                "intake-output" => {
                    let mut patient_id = None;
                    let mut intake_ml = None;
                    let mut output_ml = None;
                    let mut notes = None;
                    let mut i = 1;
                    while i < remaining_args.len() {
                        match remaining_args[i].to_lowercase().as_str() {
                            "--patient-id" => {
                                if i + 1 < remaining_args.len() {
                                    patient_id = remaining_args[i + 1].parse::<i32>().ok();
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--intake-ml" => {
                                if i + 1 < remaining_args.len() {
                                    intake_ml = remaining_args[i + 1].parse::<i64>().ok();
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--output-ml" => {
                                if i + 1 < remaining_args.len() {
                                    output_ml = remaining_args[i + 1].parse::<i64>().ok();
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--notes" => {
                                if i + 1 < remaining_args.len() {
                                    notes = Some(remaining_args[i + 1].clone());
                                    i += 2;
                                } else { i += 1; }
                            }
                            _ => i += 1,
                        }
                    }
                    CommandType::Nursing(NursingCommand::IntakeOutput {
                        patient_id: patient_id.unwrap_or(0),
                        intake_ml,
                        output_ml,
                        notes,
                    })
                }
                "med-administration" => {
                    if remaining_args.len() < 2 {
                        eprintln!("Usage: nursing med-administration <verify|give|refuse> [args...]");
                        return (CommandType::Unknown, remaining_args);
                    }
                    match remaining_args[1].to_lowercase().as_str() {
                        "verify" => {
                            let mut order_id = None;
                            let mut patient_id = None;
                            let mut barcode_scan = false;
                            let mut i = 2;
                            while i < remaining_args.len() {
                                match remaining_args[i].to_lowercase().as_str() {
                                    "--order-id" => {
                                        if i + 1 < remaining_args.len() {
                                            order_id = Uuid::parse_str(&remaining_args[i + 1]).ok();
                                            i += 2;
                                        } else { i += 1; }
                                    }
                                    "--patient-id" => {
                                        if i + 1 < remaining_args.len() {
                                            patient_id = remaining_args[i + 1].parse::<i32>().ok();
                                            i += 2;
                                        } else { i += 1; }
                                    }
                                    "--barcode-scan" => {
                                        barcode_scan = true;
                                        i += 1;
                                    }
                                    _ => i += 1,
                                }
                            }
                            CommandType::Nursing(NursingCommand::MedAdministration(MedAdministrationCommand::Verify {
                                order_id: order_id.unwrap_or_default(),
                                patient_id: patient_id.unwrap_or(0),
                                barcode_scan,
                            }))
                        }
                        "give" => {
                            let mut order_id = None;
                            let mut time_given = None;
                            let mut route = None;
                            let mut i = 2;
                            while i < remaining_args.len() {
                                match remaining_args[i].to_lowercase().as_str() {
                                    "--order-id" => {
                                        if i + 1 < remaining_args.len() {
                                            order_id = Uuid::parse_str(&remaining_args[i + 1]).ok();
                                            i += 2;
                                        } else { i += 1; }
                                    }
                                    "--time-given" => {
                                        if i + 1 < remaining_args.len() {
                                            time_given = Some(remaining_args[i + 1].clone());
                                            i += 2;
                                        } else { i += 1; }
                                    }
                                    "--route" => {
                                        if i + 1 < remaining_args.len() {
                                            route = Some(remaining_args[i + 1].clone());
                                            i += 2;
                                        } else { i += 1; }
                                    }
                                    _ => i += 1,
                                }
                            }
                            CommandType::Nursing(NursingCommand::MedAdministration(MedAdministrationCommand::Give {
                                order_id: order_id.unwrap_or_default(),
                                time_given,
                                route,
                            }))
                        }
                        "refuse" => {
                            let mut order_id = None;
                            let mut reason = None;
                            let mut i = 2;
                            while i < remaining_args.len() {
                                match remaining_args[i].to_lowercase().as_str() {
                                    "--order-id" => {
                                        if i + 1 < remaining_args.len() {
                                            order_id = Uuid::parse_str(&remaining_args[i + 1]).ok();
                                            i += 2;
                                        } else { i += 1; }
                                    }
                                    "--reason" => {
                                        if i + 1 < remaining_args.len() {
                                            reason = Some(remaining_args[i + 1].clone());
                                            i += 2;
                                        } else { i += 1; }
                                    }
                                    _ => i += 1,
                                }
                            }
                            CommandType::Nursing(NursingCommand::MedAdministration(MedAdministrationCommand::Refuse {
                                order_id: order_id.unwrap_or_default(),
                                reason: reason.unwrap_or_default(),
                            }))
                        }
                        _ => CommandType::Unknown,
                    }
                }
                "care-plan" => {
                    if remaining_args.len() < 2 {
                        eprintln!("Usage: nursing care-plan <add|update> [args...]");
                        return (CommandType::Unknown, remaining_args);
                    }
                    match remaining_args[1].to_lowercase().as_str() {
                        "add" => {
                            let mut patient_id = None;
                            let mut goal = None;
                            let mut interventions = None;
                            let mut i = 2;
                            while i < remaining_args.len() {
                                match remaining_args[i].to_lowercase().as_str() {
                                    "--patient-id" => {
                                        if i + 1 < remaining_args.len() {
                                            patient_id = remaining_args[i + 1].parse::<i32>().ok();
                                            i += 2;
                                        } else { i += 1; }
                                    }
                                    "--goal" => {
                                        if i + 1 < remaining_args.len() {
                                            goal = Some(remaining_args[i + 1].clone());
                                            i += 2;
                                        } else { i += 1; }
                                    }
                                    "--interventions" => {
                                        if i + 1 < remaining_args.len() {
                                            interventions = Some(remaining_args[i + 1].clone());
                                            i += 2;
                                        } else { i += 1; }
                                    }
                                    _ => i += 1,
                                }
                            }
                            CommandType::Nursing(NursingCommand::CarePlan(CarePlanCommand::Add {
                                patient_id: patient_id.unwrap_or(0),
                                goal: goal.unwrap_or_default(),
                                interventions: interventions.unwrap_or_default(),
                            }))
                        }
                        "update" => {
                            let mut patient_id = None;
                            let mut goal_id = None;
                            let mut status = None;
                            let mut i = 2;
                            while i < remaining_args.len() {
                                match remaining_args[i].to_lowercase().as_str() {
                                    "--patient-id" => {
                                        if i + 1 < remaining_args.len() {
                                            patient_id = remaining_args[i + 1].parse::<i32>().ok();
                                            i += 2;
                                        } else { i += 1; }
                                    }
                                    "--goal-id" => {
                                        if i + 1 < remaining_args.len() {
                                            goal_id = Uuid::parse_str(&remaining_args[i + 1]).ok();
                                            i += 2;
                                        } else { i += 1; }
                                    }
                                    "--status" => {
                                        if i + 1 < remaining_args.len() {
                                            status = Some(remaining_args[i + 1].clone());
                                            i += 2;
                                        } else { i += 1; }
                                    }
                                    _ => i += 1,
                                }
                            }
                            CommandType::Nursing(NursingCommand::CarePlan(CarePlanCommand::Update {
                                patient_id: patient_id.unwrap_or(0),
                                goal_id: goal_id.unwrap_or_default(),
                                status: status.unwrap_or_default(),
                            }))
                        }
                        _ => CommandType::Unknown,
                    }
                }
                "patient-education" => {
                    let mut patient_id = None;
                    let mut topic = None;
                    let mut method = None;
                    let mut i = 1;
                    while i < remaining_args.len() {
                        match remaining_args[i].to_lowercase().as_str() {
                            "--patient-id" => {
                                if i + 1 < remaining_args.len() {
                                    patient_id = remaining_args[i + 1].parse::<i32>().ok();
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--topic" => {
                                if i + 1 < remaining_args.len() {
                                    topic = Some(remaining_args[i + 1].clone());
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--method" => {
                                if i + 1 < remaining_args.len() {
                                    method = match remaining_args[i + 1].to_lowercase().as_str() {
                                        "verbal" => Some(EducationMethod::Verbal),
                                        "written" => Some(EducationMethod::Written),
                                        "video" => Some(EducationMethod::Video),
                                        "teachback" => Some(EducationMethod::TeachBack),
                                        "interpreter" => Some(EducationMethod::Interpreter),
                                        _ => Some(EducationMethod::Verbal),
                                    };
                                    i += 2;
                                } else { i += 1; }
                            }
                            _ => {
                                if topic.is_none() {
                                    topic = Some(remaining_args[i].clone());
                                }
                                i += 1;
                            }
                        }
                    }
                    CommandType::Nursing(NursingCommand::PatientEducation {
                        patient_id: patient_id.unwrap_or(0),
                        topic: topic.unwrap_or_default(),
                        method,
                    })
                }
                _ => CommandType::Unknown,
            }
        },
        "education" => {
            if remaining_args.is_empty() || remaining_args[0].to_lowercase() != "document" {
                eprintln!("Usage: education document --patient-id <id> <topic> [--method] [--language] [--literacy-level] [--teach-back-verified]");
                return (CommandType::Unknown, remaining_args);
            }
            let mut patient_id = None;
            let mut topic = None;
            let mut method = None;
            let mut language = None;
            let mut literacy_level = None;
            let mut teach_back_verified = false;
            let mut i = 1;
            while i < remaining_args.len() {
                match remaining_args[i].to_lowercase().as_str() {
                    "--patient-id" => {
                        if i + 1 < remaining_args.len() {
                            patient_id = remaining_args[i + 1].parse::<i32>().ok();
                            i += 2;
                        } else { i += 1; }
                    }
                    "--method" => {
                        if i + 1 < remaining_args.len() {
                            method = match remaining_args[i + 1].to_lowercase().as_str() {
                                "verbal" => Some(EducationMethod::Verbal),
                                "written" => Some(EducationMethod::Written),
                                "video" => Some(EducationMethod::Video),
                                "teachback" => Some(EducationMethod::TeachBack),
                                "interpreter" => Some(EducationMethod::Interpreter),
                                _ => Some(EducationMethod::Verbal),
                            };
                            i += 2;
                        } else { i += 1; }
                    }
                    "--language" => {
                        if i + 1 < remaining_args.len() {
                            language = Some(remaining_args[i + 1].clone());
                            i += 2;
                        } else { i += 1; }
                    }
                    "--literacy-level" => {
                        if i + 1 < remaining_args.len() {
                            literacy_level = Some(remaining_args[i + 1].clone());
                            i += 2;
                        } else { i += 1; }
                    }
                    "--teach-back-verified" => {
                        teach_back_verified = true;
                        i += 1;
                    }
                    _ => {
                        if topic.is_none() {
                            topic = Some(remaining_args[i].clone());
                        }
                        i += 1;
                    }
                }
            }
            CommandType::Education(EducationCommand::Document {
                patient_id: patient_id.unwrap_or(0),
                topic: topic.unwrap_or_default(),
                method,
                language,
                literacy_level,
                teach_back_verified,
            })
        },
        "discharge-planning" => {
            if remaining_args.is_empty() {
                eprintln!("Usage: discharge-planning <assess|dme-order|follow-up> [args...]");
                return (CommandType::Unknown, remaining_args);
            }
            match remaining_args[0].to_lowercase().as_str() {
                "assess" => {
                    let mut patient_id = None;
                    let mut barriers = Vec::new();
                    let mut i = 1;
                    while i < remaining_args.len() {
                        match remaining_args[i].to_lowercase().as_str() {
                            "--patient-id" => {
                                if i + 1 < remaining_args.len() {
                                    patient_id = remaining_args[i + 1].parse::<i32>().ok();
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--barrier" => {
                                if i + 1 < remaining_args.len() {
                                    barriers.push(remaining_args[i + 1].clone());
                                    i += 2;
                                } else { i += 1; }
                            }
                            _ => i += 1,
                        }
                    }
                    CommandType::DischargePlanning(DischargePlanningCommand::Assess {
                        patient_id: patient_id.unwrap_or(0),
                        barriers,
                    })
                }
                "dme-order" => {
                    let mut patient_id = None;
                    let mut equipment = None;
                    let mut i = 1;
                    while i < remaining_args.len() {
                        match remaining_args[i].to_lowercase().as_str() {
                            "--patient-id" => {
                                if i + 1 < remaining_args.len() {
                                    patient_id = remaining_args[i + 1].parse::<i32>().ok();
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--equipment" => {
                                if i + 1 < remaining_args.len() {
                                    equipment = Some(remaining_args[i + 1].clone());
                                    i += 2;
                                } else { i += 1; }
                            }
                            _ => {
                                if equipment.is_none() {
                                    equipment = Some(remaining_args[i].clone());
                                }
                                i += 1;
                            }
                        }
                    }
                    CommandType::DischargePlanning(DischargePlanningCommand::DmeOrder {
                        patient_id: patient_id.unwrap_or(0),
                        equipment: equipment.unwrap_or_default(),
                    })
                }
                "follow-up" => {
                    let mut patient_id = None;
                    let mut provider_id = None;
                    let mut days_out = None;
                    let mut i = 1;
                    while i < remaining_args.len() {
                        match remaining_args[i].to_lowercase().as_str() {
                            "--patient-id" => {
                                if i + 1 < remaining_args.len() {
                                    patient_id = remaining_args[i + 1].parse::<i32>().ok();
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--provider-id" => {
                                if i + 1 < remaining_args.len() {
                                    provider_id = remaining_args[i + 1].parse::<i32>().ok();
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--days-out" => {
                                if i + 1 < remaining_args.len() {
                                    days_out = remaining_args[i + 1].parse::<i64>().ok();
                                    i += 2;
                                } else { i += 1; }
                            }
                            _ => i += 1,
                        }
                    }
                    CommandType::DischargePlanning(DischargePlanningCommand::FollowUp {
                        patient_id: patient_id.unwrap_or(0),
                        provider_id: provider_id.unwrap_or(0),
                        days_out: days_out.unwrap_or(7),
                    })
                }
                _ => CommandType::Unknown,
            }
        },
        "discharge" => {
            if remaining_args.is_empty() {
                eprintln!("Usage: discharge <plan|readiness|summary|med-rec|orders|follow-up|education|finalize|dashboard> [args...]");
                return (CommandType::Unknown, remaining_args);
            }
            match remaining_args[0].to_lowercase().as_str() {
                "plan" => {
                    let mut patient_id = None;
                    let mut encounter_id = None;
                    let mut estimated_date = None;
                    let mut target_disposition = None;
                    let mut barriers = Vec::new();
                    let mut primary_diagnosis = None;
                    let mut i = 1;
                    while i < remaining_args.len() {
                        match remaining_args[i].to_lowercase().as_str() {
                            "--patient-id" => {
                                if i + 1 < remaining_args.len() {
                                    patient_id = remaining_args[i + 1].parse::<i32>().ok();
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--encounter-id" => {
                                if i + 1 < remaining_args.len() {
                                    encounter_id = Uuid::parse_str(&remaining_args[i + 1]).ok();
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--estimated-date" => {
                                if i + 1 < remaining_args.len() {
                                    estimated_date = Some(remaining_args[i + 1].clone());
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--target-disposition" => {
                                if i + 1 < remaining_args.len() {
                                    target_disposition = match remaining_args[i + 1].to_lowercase().as_str() {
                                        "home" => Some(DispositionTarget::Home),
                                        "homehealth" => Some(DispositionTarget::HomeHealth),
                                        "snf" => Some(DispositionTarget::SkilledNursing),
                                        "rehab" => Some(DispositionTarget::Rehab),
                                        "ltach" => Some(DispositionTarget::LTACH),
                                        "hospice" => Some(DispositionTarget::Hospice),
                                        "ama" => Some(DispositionTarget::AgainstMedicalAdvice),
                                        "expired" => Some(DispositionTarget::Expired),
                                        "transfer" => Some(DispositionTarget::Transfer),
                                        _ => Some(DispositionTarget::Home),
                                    };
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--barrier" => {
                                if i + 1 < remaining_args.len() {
                                    barriers.push(remaining_args[i + 1].clone());
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--primary-diagnosis" => {
                                if i + 1 < remaining_args.len() {
                                    primary_diagnosis = Some(remaining_args[i + 1].clone());
                                    i += 2;
                                } else { i += 1; }
                            }
                            _ => i += 1,
                        }
                    }
                    CommandType::Discharge(DischargeCommand::Plan {
                        patient_id: patient_id.unwrap_or(0),
                        encounter_id,
                        estimated_date,
                        target_disposition,
                        barriers,
                        primary_diagnosis,
                    })
                }
                "readiness" => {
                    let mut patient_id = None;
                    let mut medically_stable = false;
                    let mut pain_controlled = false;
                    let mut mobility_safe = false;
                    let mut barriers_resolved = Vec::new();
                    let mut pending_items = Vec::new();
                    let mut assessed_by = None;
                    let mut i = 1;
                    while i < remaining_args.len() {
                        match remaining_args[i].to_lowercase().as_str() {
                            "--patient-id" => {
                                if i + 1 < remaining_args.len() {
                                    patient_id = remaining_args[i + 1].parse::<i32>().ok();
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--medically-stable" => {
                                medically_stable = true;
                                i += 1;
                            }
                            "--pain-controlled" => {
                                pain_controlled = true;
                                i += 1;
                            }
                            "--mobility-safe" => {
                                mobility_safe = true;
                                i += 1;
                            }
                            "--barrier-resolved" => {
                                if i + 1 < remaining_args.len() {
                                    barriers_resolved.push(remaining_args[i + 1].clone());
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--pending-item" => {
                                if i + 1 < remaining_args.len() {
                                    pending_items.push(remaining_args[i + 1].clone());
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--assessed-by" => {
                                if i + 1 < remaining_args.len() {
                                    assessed_by = remaining_args[i + 1].parse::<i32>().ok();
                                    i += 2;
                                } else { i += 1; }
                            }
                            _ => i += 1,
                        }
                    }
                    CommandType::Discharge(DischargeCommand::Readiness {
                        patient_id: patient_id.unwrap_or(0),
                        medically_stable,
                        pain_controlled,
                        mobility_safe,
                        barriers_resolved,
                        pending_items,
                        assessed_by: assessed_by.unwrap_or(0),
                    })
                }
                "summary" => {
                    let mut patient_id = None;
                    let mut template = None;
                    let mut include_reconciliation = false;
                    let mut include_followup = false;
                    let mut format = None;
                    let mut i = 1;
                    while i < remaining_args.len() {
                        match remaining_args[i].to_lowercase().as_str() {
                            "--patient-id" => {
                                if i + 1 < remaining_args.len() {
                                    patient_id = remaining_args[i + 1].parse::<i32>().ok();
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--template" => {
                                if i + 1 < remaining_args.len() {
                                    template = match remaining_args[i + 1].to_lowercase().as_str() {
                                        "standard" => Some(SummaryTemplate::Standard),
                                        "complexchronic" => Some(SummaryTemplate::ComplexChronic),
                                        "surgical" => Some(SummaryTemplate::Surgical),
                                        "stroke" => Some(SummaryTemplate::Stroke),
                                        "mi" => Some(SummaryTemplate::MI),
                                        "sepsis" => Some(SummaryTemplate::Sepsis),
                                        "oncology" => Some(SummaryTemplate::Oncology),
                                        _ => Some(SummaryTemplate::Standard),
                                    };
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--include-reconciliation" => {
                                include_reconciliation = true;
                                i += 1;
                            }
                            "--include-followup" => {
                                include_followup = true;
                                i += 1;
                            }
                            "--format" => {
                                if i + 1 < remaining_args.len() {
                                    format = match remaining_args[i + 1].to_lowercase().as_str() {
                                        "text" => Some(OutputFormat::Text),
                                        "html" => Some(OutputFormat::Html),
                                        "pdf" => Some(OutputFormat::Pdf),
                                        "fhir" => Some(OutputFormat::Fhir),
                                        "hl7" => Some(OutputFormat::Hl7),
                                        _ => Some(OutputFormat::Text),
                                    };
                                    i += 2;
                                } else { i += 1; }
                            }
                            _ => i += 1,
                        }
                    }
                    CommandType::Discharge(DischargeCommand::Summary {
                        patient_id: patient_id.unwrap_or(0),
                        template,
                        include_reconciliation,
                        include_followup,
                        format,
                    })
                }
                "med-rec" => {
                    let mut patient_id = None;
                    let mut action = None;
                    let mut medication = None;
                    let mut new_dose = None;
                    let mut new_frequency = None;
                    let mut reason = None;
                    let mut reconciled_by = None;
                    let mut i = 1;
                    while i < remaining_args.len() {
                        match remaining_args[i].to_lowercase().as_str() {
                            "--patient-id" => {
                                if i + 1 < remaining_args.len() {
                                    patient_id = remaining_args[i + 1].parse::<i32>().ok();
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--action" => {
                                if i + 1 < remaining_args.len() {
                                    action = match remaining_args[i + 1].to_lowercase().as_str() {
                                        "continue" => Some(MedRecAction::Continue),
                                        "modify" => Some(MedRecAction::Modify),
                                        "hold" => Some(MedRecAction::Hold),
                                        "discontinue" => Some(MedRecAction::Discontinue),
                                        "new" => Some(MedRecAction::New),
                                        "resume" => Some(MedRecAction::Resume),
                                        "no-change" => Some(MedRecAction::NoChange),
                                        "unable-to-reconcile" => Some(MedRecAction::UnableToReconcile),
                                        _ => Some(MedRecAction::Continue),
                                    };
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--medication" => {
                                if i + 1 < remaining_args.len() {
                                    medication = Some(remaining_args[i + 1].clone());
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--new-dose" => {
                                if i + 1 < remaining_args.len() {
                                    new_dose = Some(remaining_args[i + 1].clone());
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--new-frequency" => {
                                if i + 1 < remaining_args.len() {
                                    new_frequency = Some(remaining_args[i + 1].clone());
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--reason" => {
                                if i + 1 < remaining_args.len() {
                                    reason = Some(remaining_args[i + 1].clone());
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--reconciled-by" => {
                                if i + 1 < remaining_args.len() {
                                    reconciled_by = remaining_args[i + 1].parse::<i32>().ok();
                                    i += 2;
                                } else { i += 1; }
                            }
                            _ => i += 1,
                        }
                    }
                    CommandType::Discharge(DischargeCommand::MedRec {
                        patient_id: patient_id.unwrap_or(0),
                        action: action.unwrap_or(MedRecAction::Continue),
                        medication: medication.unwrap_or_default(),
                        new_dose,
                        new_frequency,
                        reason,
                        reconciled_by: reconciled_by.unwrap_or(0),
                    })
                }
                "orders" => {
                    let mut patient_id = None;
                    let mut diet = None;
                    let mut activity = None;
                    let mut wound_care = None;
                    let mut monitoring = Vec::new();
                    let mut restrictions = Vec::new();
                    let mut i = 1;
                    while i < remaining_args.len() {
                        match remaining_args[i].to_lowercase().as_str() {
                            "--patient-id" => {
                                if i + 1 < remaining_args.len() {
                                    patient_id = remaining_args[i + 1].parse::<i32>().ok();
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--diet" => {
                                if i + 1 < remaining_args.len() {
                                    diet = Some(remaining_args[i + 1].clone());
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--activity" => {
                                if i + 1 < remaining_args.len() {
                                    activity = Some(remaining_args[i + 1].clone());
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--wound-care" => {
                                if i + 1 < remaining_args.len() {
                                    wound_care = Some(remaining_args[i + 1].clone());
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--monitoring" => {
                                if i + 1 < remaining_args.len() {
                                    monitoring.push(remaining_args[i + 1].clone());
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--restriction" => {
                                if i + 1 < remaining_args.len() {
                                    restrictions.push(remaining_args[i + 1].clone());
                                    i += 2;
                                } else { i += 1; }
                            }
                            _ => i += 1,
                        }
                    }
                    CommandType::Discharge(DischargeCommand::Orders {
                        patient_id: patient_id.unwrap_or(0),
                        diet,
                        activity,
                        wound_care,
                        monitoring,
                        restrictions,
                    })
                }
                "follow-up" => {
                    let mut patient_id = None;
                    let mut provider_type = None;
                    let mut days_out = None;
                    let mut priority = None;
                    let mut reason = None;
                    let mut telehealth = false;
                    let mut i = 1;
                    while i < remaining_args.len() {
                        match remaining_args[i].to_lowercase().as_str() {
                            "--patient-id" => {
                                if i + 1 < remaining_args.len() {
                                    patient_id = remaining_args[i + 1].parse::<i32>().ok();
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--provider-type" => {
                                if i + 1 < remaining_args.len() {
                                    provider_type = Some(remaining_args[i + 1].clone());
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--days-out" => {
                                if i + 1 < remaining_args.len() {
                                    days_out = remaining_args[i + 1].parse::<i64>().ok();
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--priority" => {
                                if i + 1 < remaining_args.len() {
                                    priority = match remaining_args[i + 1].to_lowercase().as_str() {
                                        "urgent" => Some(FollowUpPriority::Urgent),
                                        "routine" => Some(FollowUpPriority::Routine),
                                        "standard" => Some(FollowUpPriority::Standard),
                                        "extended" => Some(FollowUpPriority::Extended),
                                        _ => Some(FollowUpPriority::Routine),
                                    };
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--reason" => {
                                if i + 1 < remaining_args.len() {
                                    reason = Some(remaining_args[i + 1].clone());
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--telehealth" => {
                                telehealth = true;
                                i += 1;
                            }
                            _ => i += 1,
                        }
                    }
                    CommandType::Discharge(DischargeCommand::FollowUp {
                        patient_id: patient_id.unwrap_or(0),
                        provider_type: provider_type.unwrap_or_default(),
                        days_out,
                        priority,
                        reason,
                        telehealth,
                    })
                }
                "education" => {
                    let mut patient_id = None;
                    let mut topics = Vec::new();
                    let mut method = None;
                    let mut language = None;
                    let mut literacy_level = None;
                    let mut teach_back_verified = false;
                    let mut i = 1;
                    while i < remaining_args.len() {
                        match remaining_args[i].to_lowercase().as_str() {
                            "--patient-id" => {
                                if i + 1 < remaining_args.len() {
                                    patient_id = remaining_args[i + 1].parse::<i32>().ok();
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--topic" => {
                                if i + 1 < remaining_args.len() {
                                    topics.push(remaining_args[i + 1].clone());
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--method" => {
                                if i + 1 < remaining_args.len() {
                                    method = match remaining_args[i + 1].to_lowercase().as_str() {
                                        "verbal" => Some(EducationMethod::Verbal),
                                        "written" => Some(EducationMethod::Written),
                                        "video" => Some(EducationMethod::Video),
                                        "teachback" => Some(EducationMethod::TeachBack),
                                        "interpreter" => Some(EducationMethod::Interpreter),
                                        _ => Some(EducationMethod::Verbal),
                                    };
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--language" => {
                                if i + 1 < remaining_args.len() {
                                    language = Some(remaining_args[i + 1].clone());
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--literacy-level" => {
                                if i + 1 < remaining_args.len() {
                                    literacy_level = Some(remaining_args[i + 1].clone());
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--teach-back-verified" => {
                                teach_back_verified = true;
                                i += 1;
                            }
                            _ => {
                                topics.push(remaining_args[i].clone());
                                i += 1;
                            }
                        }
                    }
                    CommandType::Discharge(DischargeCommand::Education {
                        patient_id: patient_id.unwrap_or(0),
                        topics,
                        method,
                        language,
                        literacy_level,
                        teach_back_verified,
                    })
                }
                "finalize" => {
                    let mut patient_id = None;
                    let mut actual_date = None;
                    let mut disposition = None;
                    let mut transportation = None;
                    let mut accompanying_person = None;
                    let mut final_diagnosis = Vec::new();
                    let mut completed_by = None;
                    let mut i = 1;
                    while i < remaining_args.len() {
                        match remaining_args[i].to_lowercase().as_str() {
                            "--patient-id" => {
                                if i + 1 < remaining_args.len() {
                                    patient_id = remaining_args[i + 1].parse::<i32>().ok();
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--actual-date" => {
                                if i + 1 < remaining_args.len() {
                                    actual_date = Some(remaining_args[i + 1].clone());
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--disposition" => {
                                if i + 1 < remaining_args.len() {
                                    disposition = match remaining_args[i + 1].to_lowercase().as_str() {
                                        "home" => Some(DispositionTarget::Home),
                                        "homehealth" => Some(DispositionTarget::HomeHealth),
                                        "snf" => Some(DispositionTarget::SkilledNursing),
                                        "rehab" => Some(DispositionTarget::Rehab),
                                        "ltach" => Some(DispositionTarget::LTACH),
                                        "hospice" => Some(DispositionTarget::Hospice),
                                        "ama" => Some(DispositionTarget::AgainstMedicalAdvice),
                                        "expired" => Some(DispositionTarget::Expired),
                                        "transfer" => Some(DispositionTarget::Transfer),
                                        _ => Some(DispositionTarget::Home),
                                    };
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--transportation" => {
                                if i + 1 < remaining_args.len() {
                                    transportation = Some(remaining_args[i + 1].clone());
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--accompanying-person" => {
                                if i + 1 < remaining_args.len() {
                                    accompanying_person = Some(remaining_args[i + 1].clone());
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--final-diagnosis" => {
                                if i + 1 < remaining_args.len() {
                                    final_diagnosis.push(remaining_args[i + 1].clone());
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--completed-by" => {
                                if i + 1 < remaining_args.len() {
                                    completed_by = remaining_args[i + 1].parse::<i32>().ok();
                                    i += 2;
                                } else { i += 1; }
                            }
                            _ => i += 1,
                        }
                    }
                    CommandType::Discharge(DischargeCommand::Finalize {
                        patient_id: patient_id.unwrap_or(0),
                        actual_date,
                        disposition: disposition.unwrap_or(DispositionTarget::Home),
                        transportation,
                        accompanying_person,
                        final_diagnosis,
                        completed_by: completed_by.unwrap_or(0),
                    })
                }
                "dashboard" => {
                    let mut unit = None;
                    let mut provider_id = None;
                    let mut timeframe = None;
                    let mut show_pending = false;
                    let mut i = 1;
                    while i < remaining_args.len() {
                        match remaining_args[i].to_lowercase().as_str() {
                            "--unit" => {
                                if i + 1 < remaining_args.len() {
                                    unit = Some(remaining_args[i + 1].clone());
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--provider-id" => {
                                if i + 1 < remaining_args.len() {
                                    provider_id = remaining_args[i + 1].parse::<i32>().ok();
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--timeframe" => {
                                if i + 1 < remaining_args.len() {
                                    timeframe = Some(remaining_args[i + 1].clone());
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--show-pending" => {
                                show_pending = true;
                                i += 1;
                            }
                            _ => i += 1,
                        }
                    }
                    CommandType::Discharge(DischargeCommand::Dashboard {
                        unit,
                        provider_id,
                        timeframe,
                        show_pending,
                    })
                }
                _ => CommandType::Unknown,
            }
        },
        "quality" => {
            if remaining_args.is_empty() {
                eprintln!("Usage: quality <measure|gap-report> [args...]");
                return (CommandType::Unknown, remaining_args);
            }
            match remaining_args[0].to_lowercase().as_str() {
                "measure" => {
                    let mut name = None;
                    let mut i = 1;
                    while i < remaining_args.len() {
                        match remaining_args[i].to_lowercase().as_str() {
                            "--name" => {
                                if i + 1 < remaining_args.len() {
                                    name = Some(remaining_args[i + 1].clone());
                                    i += 2;
                                } else { i += 1; }
                            }
                            _ => {
                                if name.is_none() {
                                    name = Some(remaining_args[i].clone());
                                }
                                i += 1;
                            }
                        }
                    }
                    CommandType::Quality(QualityCommand::Measure { name: name.unwrap_or_default() })
                }
                "gap-report" => {
                    let mut payer = None;
                    let mut i = 1;
                    while i < remaining_args.len() {
                        match remaining_args[i].to_lowercase().as_str() {
                            "--payer" => {
                                if i + 1 < remaining_args.len() {
                                    payer = Some(remaining_args[i + 1].clone());
                                    i += 2;
                                } else { i += 1; }
                            }
                            _ => i += 1,
                        }
                    }
                    CommandType::Quality(QualityCommand::GapReport { payer })
                }
                _ => CommandType::Unknown,
            }
        },
        "incident" => {
            if remaining_args.is_empty() {
                eprintln!("Usage: incident <report|investigate> [args...]");
                return (CommandType::Unknown, remaining_args);
            }
            match remaining_args[0].to_lowercase().as_str() {
                "report" => {
                    let mut patient_id = None;
                    let mut incident_type = None;
                    let mut description = None;
                    let mut i = 1;
                    while i < remaining_args.len() {
                        match remaining_args[i].to_lowercase().as_str() {
                            "--patient-id" => {
                                if i + 1 < remaining_args.len() {
                                    patient_id = remaining_args[i + 1].parse::<i32>().ok();
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--incident-type" => {
                                if i + 1 < remaining_args.len() {
                                    incident_type = Some(remaining_args[i + 1].clone());
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--description" => {
                                if i + 1 < remaining_args.len() {
                                    description = Some(remaining_args[i + 1].clone());
                                    i += 2;
                                } else { i += 1; }
                            }
                            _ => {
                                if description.is_none() {
                                    description = Some(remaining_args[i].clone());
                                }
                                i += 1;
                            }
                        }
                    }
                    CommandType::Incident(IncidentCommand::Report {
                        patient_id: patient_id.unwrap_or(0),
                        incident_type: incident_type.unwrap_or_default(),
                        description: description.unwrap_or_default(),
                    })
                }
                "investigate" => {
                    let mut incident_id = None;
                    let mut i = 1;
                    while i < remaining_args.len() {
                        match remaining_args[i].to_lowercase().as_str() {
                            "--incident-id" => {
                                if i + 1 < remaining_args.len() {
                                    incident_id = Uuid::parse_str(&remaining_args[i + 1]).ok();
                                    i += 2;
                                } else { i += 1; }
                            }
                            _ => i += 1,
                        }
                    }
                    CommandType::Incident(IncidentCommand::Investigate { incident_id: incident_id.unwrap_or_default() })
                }
                _ => CommandType::Unknown,
            }
        },
        "compliance" => {
            if remaining_args.is_empty() {
                eprintln!("Usage: compliance <audit-patient|audit-controlled-substances> [args...]");
                return (CommandType::Unknown, remaining_args);
            }
            match remaining_args[0].to_lowercase().as_str() {
                "audit-patient" => {
                    let mut patient_id = None;
                    let mut from = None;
                    let mut i = 1;
                    while i < remaining_args.len() {
                        match remaining_args[i].to_lowercase().as_str() {
                            "--patient-id" => {
                                if i + 1 < remaining_args.len() {
                                    patient_id = remaining_args[i + 1].parse::<i32>().ok();
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--from" => {
                                if i + 1 < remaining_args.len() {
                                    from = DateTime::parse_from_rfc3339(&remaining_args[i + 1]).ok().map(|dt| dt.with_timezone(&Utc));
                                    i += 2;
                                } else { i += 1; }
                            }
                            _ => i += 1,
                        }
                    }
                    CommandType::Compliance(ComplianceCommand::AuditPatient {
                        patient_id: patient_id.unwrap_or(0),
                        from: from.unwrap_or_default(),
                    })
                }
                "audit-controlled-substances" => {
                    let mut from = None;
                    let mut i = 1;
                    while i < remaining_args.len() {
                        match remaining_args[i].to_lowercase().as_str() {
                            "--from" => {
                                if i + 1 < remaining_args.len() {
                                    from = DateTime::parse_from_rfc3339(&remaining_args[i + 1]).ok().map(|dt| dt.with_timezone(&Utc));
                                    i += 2;
                                } else { i += 1; }
                            }
                            _ => i += 1,
                        }
                    }
                    CommandType::Compliance(ComplianceCommand::AuditControlledSubstances {
                        from: from.unwrap_or_default(),
                    })
                }
                _ => CommandType::Unknown,
            }
        },
        "population" => {
            if remaining_args.is_empty() {
                eprintln!("Usage: population <screening-due|high-risk-meds|chronic-conditions|readmission-risk> [args...]");
                return (CommandType::Unknown, remaining_args);
            }
            match remaining_args[0].to_lowercase().as_str() {
                "screening-due" => {
                    let mut screening_type = None;
                    let mut age_min = None;
                    let mut age_max = None;
                    let mut months_overdue = None;
                    let mut i = 1;
                    while i < remaining_args.len() {
                        match remaining_args[i].to_lowercase().as_str() {
                            "--age-min" => {
                                if i + 1 < remaining_args.len() {
                                    age_min = remaining_args[i + 1].parse::<u32>().ok();
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--age-max" => {
                                if i + 1 < remaining_args.len() {
                                    age_max = remaining_args[i + 1].parse::<u32>().ok();
                                    i += 2;
                                } else { i += 1; }
                            }
                            _ => {
                                if screening_type.is_none() {
                                    screening_type = Some(remaining_args[i].to_uppercase());
                                }
                                i += 1;
                            }
                        }
                    }
                    CommandType::Population(PopulationCommand::ScreeningDue {
                        screening_type: screening_type.unwrap_or_default(),
                        age_min,
                        age_max,
                        months_overdue,
                    })
                }
                "high-risk-meds" => CommandType::Population(PopulationCommand::HighRiskMeds),
                "chronic-conditions" => {
                    let condition = remaining_args.get(1).cloned().unwrap_or_default();
                    CommandType::Population(PopulationCommand::ChronicConditions {
                        condition,
                        uncontrolled_threshold: None, // <-- was missing
                    })
                },
                "readmission-risk" => CommandType::Population(
                    PopulationCommand::ReadmissionRisk {
                        days: None,
                        preventable_only: false,
                        high_risk_only: false,
                    }
                ),
                _ => CommandType::Unknown,
            }
        },

        "analytics" => {
            if remaining_args.is_empty() {
                eprintln!("Usage: analytics <population|quality|utilization|risk|pathway|equity> [subcommand]");
                return (CommandType::Unknown, remaining_args);
            }
            match remaining_args[0].to_lowercase().as_str() {
                "population" => {
                    if remaining_args.len() < 2 {
                        eprintln!("Usage: analytics population <prevalence|incidence|comorbidity|seasonal|outbreak>");
                        return (CommandType::Unknown, remaining_args);
                    }
                    match remaining_args[1].to_lowercase().as_str() {
                        "prevalence" => {
                            let diagnosis = remaining_args.get(2).cloned().unwrap_or_default();
                            CommandType::Analytics(AnalyticsCommand::Population(AnalyticsPopulationCommand::Prevalence {
                                diagnosis,
                                age_group: None,
                                gender: None,
                                race: None,
                            }))
                        }
                        "incidence" => {
                            let diagnosis = remaining_args.get(2).cloned().unwrap_or_default();
                            CommandType::Analytics(AnalyticsCommand::Population(AnalyticsPopulationCommand::Incidence {
                                diagnosis,
                                timeframe: None,
                            }))
                        }
                        "comorbidity" => {
                            let primary_diagnosis = remaining_args.get(2).cloned().unwrap_or_default();
                            CommandType::Analytics(AnalyticsCommand::Population(AnalyticsPopulationCommand::Comorbidity {
                                primary_diagnosis,
                                top_n: Some(10),
                            }))
                        }
                        "seasonal" => {
                            let diagnosis = remaining_args.get(2).cloned().unwrap_or_default();
                            CommandType::Analytics(AnalyticsCommand::Population(AnalyticsPopulationCommand::Seasonal {
                                diagnosis,
                                years: Some(3),
                            }))
                        }
                        "outbreak" => CommandType::Analytics(AnalyticsCommand::Population(AnalyticsPopulationCommand::Outbreak {
                            pathogen: None,
                            threshold: None,
                        })),
                        _ => CommandType::Unknown,
                    }
                }
                "quality" => {
                    if remaining_args.len() < 2 {
                        eprintln!("Usage: analytics quality <mortality|los|readmission|complications|experience>");
                        return (CommandType::Unknown, remaining_args);
                    }
                    match remaining_args[1].to_lowercase().as_str() {
                        "mortality" => CommandType::Analytics(AnalyticsCommand::Quality(AnalyticsQualityCommand::MortalityIndex {
                            service_line: None,
                        })),
                        "los" => CommandType::Analytics(AnalyticsCommand::Quality(AnalyticsQualityCommand::Los {
                            diagnosis: None,
                            benchmark: None,
                        })),
                        "readmission" => CommandType::Analytics(AnalyticsCommand::Quality(AnalyticsQualityCommand::Readmission {
                            days: Some(30),
                            preventable_only: false,
                        })),
                        "complications" => CommandType::Analytics(AnalyticsCommand::Quality(AnalyticsQualityCommand::Complications {
                            procedure: remaining_args.get(2).cloned().unwrap_or_default(),
                            risk_adjusted: true,
                        })),
                        "experience" => CommandType::Analytics(AnalyticsCommand::Quality(AnalyticsQualityCommand::Experience {
                            domain: None,
                        })),
                        _ => CommandType::Unknown,
                    }
                }
                _ => CommandType::Unknown,
            }
        },

        "metrics" => {
            if remaining_args.is_empty() {
                eprintln!("Usage: metrics <clinical|operational|financial|safety|ml> [subcommand]");
                return (CommandType::Unknown, remaining_args);
            }
            match remaining_args[0].to_lowercase().as_str() {
                "clinical" => {
                    if remaining_args.len() < 2 {
                        eprintln!("Usage: metrics clinical <sepsis|stroke|vtp|pain|fall|readmission>");
                        return (CommandType::Unknown, remaining_args);
                    }
                    match remaining_args[1].to_lowercase().as_str() {
                        "sepsis" => CommandType::Metrics(MetricsCommand::Clinical(MetricsClinicalCommand::SepsisBundle {
                            facility: None,
                            timeframe: None,
                        })),
                        "stroke" => CommandType::Metrics(MetricsCommand::Clinical(MetricsClinicalCommand::StrokeTpa {
                            target_minutes: Some(60),
                        })),
                        "vtp" => CommandType::Metrics(MetricsCommand::Clinical(MetricsClinicalCommand::VteProphylaxis)),
                        "pain" => CommandType::Metrics(MetricsCommand::Clinical(MetricsClinicalCommand::PainReassessment)),
                        "fall" => CommandType::Metrics(MetricsCommand::Clinical(MetricsClinicalCommand::FallRate)),
                        "readmission" => CommandType::Metrics(MetricsCommand::Clinical(MetricsClinicalCommand::ReadmissionRate {
                            condition: None,
                        })),
                        _ => CommandType::Unknown,
                    }
                }
                "operational" => {
                    if remaining_args.len() < 2 {
                        eprintln!("Usage: metrics operational <ed|or|bed|flow|staff>");
                        return (CommandType::Unknown, remaining_args);
                    }
                    match remaining_args[1].to_lowercase().as_str() {
                        "ed" => CommandType::Metrics(MetricsCommand::Operational(MetricsOperationalCommand::EdThroughput {
                            metric: None,
                        })),
                        "or" => CommandType::Metrics(MetricsCommand::Operational(MetricsOperationalCommand::OrUtilization)),
                        "bed" => CommandType::Metrics(MetricsCommand::Operational(MetricsOperationalCommand::BedOccupancy {
                            unit: None,
                            predict_hours: None,
                        })),
                        "flow" => CommandType::Metrics(MetricsCommand::Operational(MetricsOperationalCommand::PatientFlow)),
                        "staff" => CommandType::Metrics(MetricsCommand::Operational(MetricsOperationalCommand::StaffProductivity {
                            role: None,
                        })),
                        _ => CommandType::Unknown,
                    }
                }
                _ => CommandType::Unknown,
            }
        },

        "research" => {
            if remaining_args.is_empty() {
                eprintln!("Usage: research <cohort|export> [args...]");
                return (CommandType::Unknown, remaining_args);
            }
            match remaining_args[0].to_lowercase().as_str() {
                "cohort" => {
                    let query = remaining_args[1..].join(" ");
                    CommandType::Research(ResearchCommand::Cohort { query })
                }
                "export" => {
                    let cohort_id = remaining_args.get(1).cloned().unwrap_or_default();
                    let mut format = None;
                    let mut deidentify = false;
                    let mut i = 2;
                    while i < remaining_args.len() {
                        match remaining_args[i].to_lowercase().as_str() {
                            "--format" => {
                                if i + 1 < remaining_args.len() {
                                    format = Some(remaining_args[i + 1].clone());
                                    i += 2;
                                } else {
                                    i += 1;
                                }
                            }
                            "--deidentify" => {
                                deidentify = true;
                                i += 1;
                            }
                            _ => i += 1,
                        }
                    }
                    CommandType::Research(ResearchCommand::Export {
                        cohort_id,
                        format: format.unwrap_or_else(|| "csv".to_string()), // <- String now
                        deidentify,
                    })
                }
                _ => CommandType::Unknown,
            }
        },

        "model" | "ml" => {
            if remaining_args.is_empty() {
                eprintln!("Usage: model <list|show|deploy|predict|evaluate|retrain|monitor|explain|drift|abtest|rollback> [args...]");
                return (CommandType::Unknown, remaining_args);
            }

            match remaining_args[0].to_lowercase().as_str() {
                "list" => {
                    let mut status = None;
                    let mut domain = None;
                    let mut i = 1;
                    while i < remaining_args.len() {
                        match remaining_args[i].to_lowercase().as_str() {
                            "--status" => {
                                if i + 1 < remaining_args.len() {
                                    status = match remaining_args[i + 1].to_lowercase().as_str() {
                                        "development" => Some(ModelStatus::Development),
                                        "staging" => Some(ModelStatus::Staging),
                                        "production" => Some(ModelStatus::Production),
                                        "retired" => Some(ModelStatus::Retired),
                                        "failed" => Some(ModelStatus::Failed),
                                        _ => None,
                                    };
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--domain" => {
                                if i + 1 < remaining_args.len() {
                                    domain = Some(remaining_args[i + 1].clone());
                                    i += 2;
                                } else { i += 1; }
                            }
                            _ => i += 1,
                        }
                    }
                    CommandType::Model(ModelCommand::List { status, domain })
                }

                "show" => {
                    if remaining_args.len() < 2 {
                        eprintln!("Usage: model show <model_name> [--include-metrics] [--include-features] [--include-version-history]");
                        return (CommandType::Unknown, remaining_args);
                    }
                    let model_name = remaining_args[1].clone();
                    let mut include_metrics = false;
                    let mut include_features = false;
                    let mut include_version_history = false;
                    let mut i = 2;
                    while i < remaining_args.len() {
                        match remaining_args[i].to_lowercase().as_str() {
                            "--include-metrics" => { include_metrics = true; i += 1; }
                            "--include-features" => { include_features = true; i += 1; }
                            "--include-version-history" => { include_version_history = true; i += 1; }
                            _ => i += 1,
                        }
                    }
                    CommandType::Model(ModelCommand::Show {
                        model_name,
                        include_metrics,
                        include_features,
                        include_version_history,
                    })
                }

                "deploy" => {
                    if remaining_args.len() < 2 {
                        eprintln!("Usage: model deploy <model_name> --version <v> --path <file> [--format] [--description] [--force]");
                        return (CommandType::Unknown, remaining_args);
                    }
                    let model_name = remaining_args[1].clone();
                    let mut version = None;
                    let mut path = None;
                    let mut format = None;
                    let mut description = None;
                    let mut force = false;
                    let mut i = 2;
                    while i < remaining_args.len() {
                        match remaining_args[i].to_lowercase().as_str() {
                            "--version" => {
                                if i + 1 < remaining_args.len() {
                                    version = Some(remaining_args[i + 1].clone());
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--path" => {
                                if i + 1 < remaining_args.len() {
                                    path = Some(PathBuf::from(&remaining_args[i + 1]));
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--format" => {
                                if i + 1 < remaining_args.len() {
                                    format = match remaining_args[i + 1].to_lowercase().as_str() {
                                        "pytorch" => Some(ModelFormat::Pytorch),
                                        "tensorflow" => Some(ModelFormat::Tensorflow),
                                        "onnx" => Some(ModelFormat::Onnx),
                                        "pkl" => Some(ModelFormat::Pkl),
                                        "json" => Some(ModelFormat::Json),
                                        _ => Some(ModelFormat::Pkl),
                                    };
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--description" => {
                                if i + 1 < remaining_args.len() {
                                    description = Some(remaining_args[i + 1].clone());
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--force" => { force = true; i += 1; }
                            _ => i += 1,
                        }
                    }
                    CommandType::Model(ModelCommand::Deploy {
                        model_name,
                        version: version.unwrap_or_default(),
                        path: path.unwrap_or_else(|| PathBuf::from("model.pkl")),
                        format: format.unwrap_or(ModelFormat::Pkl),
                        description,
                        force,
                    })
                }

                "predict" => {
                    if remaining_args.len() < 3 {
                        eprintln!("Usage: model predict <model_name> <patient_id> [--encounter-id] [--output-format]");
                        return (CommandType::Unknown, remaining_args);
                    }
                    let model_name = remaining_args[1].clone();
                    let patient_id = remaining_args[2].parse::<i32>().unwrap_or(0);
                    let mut encounter_id = None;
                    let mut output_format = None;
                    let mut i = 3;
                    while i < remaining_args.len() {
                        match remaining_args[i].to_lowercase().as_str() {
                            "--encounter-id" => {
                                if i + 1 < remaining_args.len() {
                                    encounter_id = Uuid::parse_str(&remaining_args[i + 1]).ok();
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--output-format" => {
                                if i + 1 < remaining_args.len() {
                                    output_format = match remaining_args[i + 1].to_lowercase().as_str() {
                                        "json" => Some(PredictionFormat::Json),
                                        "fhir" => Some(PredictionFormat::FhirObservation),
                                        "note" => Some(PredictionFormat::ClinicalNote),
                                        "alert" => Some(PredictionFormat::Alert),
                                        _ => Some(PredictionFormat::Json),
                                    };
                                    i += 2;
                                } else { i += 1; }
                            }
                            _ => i += 1,
                        }
                    }
                    CommandType::Model(ModelCommand::Predict {
                        model_name,
                        patient_id,
                        encounter_id,
                        output_format,
                    })
                }

                "evaluate" => {
                    if remaining_args.len() < 2 {
                        eprintln!("Usage: model evaluate <model_name> [--test-cohort] [--start-date] [--end-date]");
                        return (CommandType::Unknown, remaining_args);
                    }
                    let model_name = remaining_args[1].clone();
                    let mut test_cohort = None;
                    let mut start_date = None;
                    let mut end_date = None;
                    let mut i = 2;
                    while i < remaining_args.len() {
                        match remaining_args[i].to_lowercase().as_str() {
                            "--test-cohort" => {
                                if i + 1 < remaining_args.len() {
                                    test_cohort = Some(remaining_args[i + 1].clone());
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--start-date" => {
                                if i + 1 < remaining_args.len() {
                                    start_date = Some(remaining_args[i + 1].clone());
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--end-date" => {
                                if i + 1 < remaining_args.len() {
                                    end_date = Some(remaining_args[i + 1].clone());
                                    i += 2;
                                } else { i += 1; }
                            }
                            _ => i += 1,
                        }
                    }
                    CommandType::Model(ModelCommand::Evaluate {
                        model_name,
                        test_cohort,
                        start_date,
                        end_date,
                    })
                }

                "retrain" => {
                    if remaining_args.len() < 2 {
                        eprintln!("Usage: model retrain <model_name> --outcome-variable <var> [--cohort-query] [--time-window-hours]");
                        return (CommandType::Unknown, remaining_args);
                    }
                    let model_name = remaining_args[1].clone();
                    let mut cohort_query = None;
                    let mut outcome_variable = None;
                    let mut time_window_hours = None;
                    let mut i = 2;
                    while i < remaining_args.len() {
                        match remaining_args[i].to_lowercase().as_str() {
                            "--cohort-query" => {
                                if i + 1 < remaining_args.len() {
                                    cohort_query = Some(remaining_args[i + 1].clone());
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--outcome-variable" => {
                                if i + 1 < remaining_args.len() {
                                    outcome_variable = Some(remaining_args[i + 1].clone());
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--time-window-hours" => {
                                if i + 1 < remaining_args.len() {
                                    time_window_hours = remaining_args[i + 1].parse::<i64>().ok();
                                    i += 2;
                                } else { i += 1; }
                            }
                            _ => i += 1,
                        }
                    }
                    CommandType::Model(ModelCommand::Retrain {
                        model_name,
                        cohort_query,
                        outcome_variable: outcome_variable.unwrap_or_default(),
                        time_window_hours,
                    })
                }

                "monitor" => {
                    let mut model_name = None;
                    let mut timeframe = None;
                    let mut alert_threshold = None;
                    let mut i = 1;
                    while i < remaining_args.len() {
                        match remaining_args[i].to_lowercase().as_str() {
                            "--timeframe" => {
                                if i + 1 < remaining_args.len() {
                                    timeframe = Some(remaining_args[i +1].clone());
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--alert-threshold" => {
                                if i + 1 < remaining_args.len() {
                                    alert_threshold = remaining_args[i + 1].parse::<f64>().ok();
                                    i += 2;
                                } else { i += 1; }
                            }
                            _ => {
                                if model_name.is_none() && !remaining_args[i].starts_with('-') {
                                    model_name = Some(remaining_args[i].clone());
                                }
                                i += 1;
                            }
                        }
                    }
                    CommandType::Model(ModelCommand::Monitor {
                        model_name,
                        timeframe,
                        alert_threshold,
                    })
                }

                "explain" => {
                    if remaining_args.len() < 3 {
                        eprintln!("Usage: model explain <model_name> <patient_id> [--top-features] [--format]");
                        return (CommandType::Unknown, remaining_args);
                    }
                    let model_name = remaining_args[1].clone();
                    let patient_id = remaining_args[2].parse::<i32>().unwrap_or(0);
                    let mut top_features = None;
                    let mut format = None;
                    let mut i = 3;
                    while i < remaining_args.len() {
                        match remaining_args[i].to_lowercase().as_str() {
                            "--top-features" => {
                                if i + 1 < remaining_args.len() {
                                    top_features = remaining_args[i + 1].parse::<usize>().ok();
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--format" => {
                                if i + 1 < remaining_args.len() {
                                    format = match remaining_args[i + 1].to_lowercase().as_str() {
                                        "text" => Some(ExplainFormat::Text),
                                        "json" => Some(ExplainFormat::Json),
                                        "html" => Some(ExplainFormat::Html),
                                        "lime" => Some(ExplainFormat::Lime),
                                        "shap" => Some(ExplainFormat::Shap),
                                        _ => Some(ExplainFormat::Text),
                                    };
                                    i += 2;
                                } else { i += 1; }
                            }
                            _ => i += 1,
                        }
                    }
                    CommandType::Model(ModelCommand::Explain {
                        model_name,
                        patient_id,
                        top_features,
                        format,
                    })
                }

                "drift" => {
                    if remaining_args.len() < 2 {
                        eprintln!("Usage: model drift <model_name> [--baseline-date]");
                        return (CommandType::Unknown, remaining_args);
                    }
                    let model_name = remaining_args[1].clone();
                    let mut baseline_date = None;
                    let mut i = 2;
                    while i < remaining_args.len() {
                        match remaining_args[i].to_lowercase().as_str() {
                            "--baseline-date" => {
                                if i + 1 < remaining_args.len() {
                                    baseline_date = Some(remaining_args[i + 1].clone());
                                    i += 2;
                                } else { i += 1; }
                            }
                            _ => i += 1,
                        }
                    }
                    CommandType::Model(ModelCommand::Drift {
                        model_name,
                        baseline_date,
                    })
                }

                "abtest" | "ab-test" => {
                    if remaining_args.len() < 3 {
                        eprintln!("Usage: model abtest <model_a> <model_b> [--traffic-split] [--duration-days]");
                        return (CommandType::Unknown, remaining_args);
                    }
                    let model_a = remaining_args[1].clone();
                    let model_b = remaining_args[2].clone();
                    let mut traffic_split = None;
                    let mut duration_days = None;
                    let mut i = 3;
                    while i < remaining_args.len() {
                        match remaining_args[i].to_lowercase().as_str() {
                            "--traffic-split" => {
                                if i + 1 < remaining_args.len() {
                                    traffic_split = Some(remaining_args[i + 1].clone());
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--duration-days" => {
                                if i + 1 < remaining_args.len() {
                                    duration_days = remaining_args[i + 1].parse::<i64>().ok();
                                    i += 2;
                                } else { i += 1; }
                            }
                            _ => i += 1,
                        }
                    }
                    CommandType::Model(ModelCommand::AbTest {
                        model_a,
                        model_b,
                        traffic_split,
                        duration_days,
                    })
                }

                "rollback" => {
                    if remaining_args.len() < 2 {
                        eprintln!("Usage: model rollback <model_name> [--version]");
                        return (CommandType::Unknown, remaining_args);
                    }
                    let model_name = remaining_args[1].clone();
                    let mut version = None;
                    let mut i = 2;
                    while i < remaining_args.len() {
                        match remaining_args[i].to_lowercase().as_str() {
                            "--version" => {
                                if i + 1 < remaining_args.len() {
                                    version = Some(remaining_args[i + 1].clone());
                                    i += 2;
                                } else { i += 1; }
                            }
                            _ => i += 1,
                        }
                    }
                    CommandType::Model(ModelCommand::Rollback {
                        model_name,
                        version,
                    })
                }

                _ => CommandType::Unknown,
            }
        },

        "clinical-trial" | "trial" => {
            if remaining_args.is_empty() {
                eprintln!("Usage: clinical-trial <list|screen|enroll|visit|ae|report|export> [args...]");
                return (CommandType::Unknown, remaining_args);
            }
            match remaining_args[0].to_lowercase().as_str() {
                "list" => CommandType::ClinicalTrial(ClinicalTrialCommand::List {
                    status: None,
                    sponsor: None,
                    phase: None,
                }),
                "screen" => {
                    let trial_id = remaining_args.get(1).cloned().unwrap_or_default();
                    CommandType::ClinicalTrial(ClinicalTrialCommand::Screen {
                        trial_id,
                        patient_id: None,
                        cohort_query: None,
                        dry_run: false,
                    })
                }
                "enroll" => {
                    let trial_id = remaining_args.get(1).cloned().unwrap_or_default();
                    let patient_id = remaining_args.get(2).and_then(|s| s.parse::<i32>().ok()).unwrap_or(0);
                    CommandType::ClinicalTrial(ClinicalTrialCommand::Enroll {
                        trial_id,
                        patient_id,
                        consent_date: None,
                        arm: None,
                        site_id: None,
                    })
                }
                _ => CommandType::Unknown,
            }
        },

        "facility" => {
            if remaining_args.is_empty() {
                eprintln!("Usage: facility <beds|capacity> [args...]");
                return (CommandType::Unknown, remaining_args);
            }
            match remaining_args[0].to_lowercase().as_str() {
                "beds" => {
                    let mut unit = None;
                    let mut i = 1;
                    while i < remaining_args.len() {
                        match remaining_args[i].to_lowercase().as_str() {
                            "--unit" => {
                                if i + 1 < remaining_args.len() {
                                    unit = Some(remaining_args[i + 1].clone());
                                    i += 2;
                                } else { i += 1; }
                            }
                            _ => i += 1,
                        }
                    }
                    CommandType::Facility(FacilityCommand::Beds { unit })
                }
                "capacity" => CommandType::Facility(FacilityCommand::Capacity),
                _ => CommandType::Unknown,
            }
        },

        "access" => {
            if remaining_args.is_empty() {
                eprintln!("Usage: access <login|whoami|logout>");
                return (CommandType::Unknown, remaining_args);
            }
            match remaining_args[0].to_lowercase().as_str() {
                "login" => CommandType::Access(AccessCommand::Login {
                    username: remaining_args.get(1).cloned().unwrap_or_default(),
                }),
                "whoami" => CommandType::Access(AccessCommand::Whoami),
                "logout" => CommandType::Access(AccessCommand::Logout),
                _ => CommandType::Unknown,
            }
        },

        "financial" => {
            if remaining_args.is_empty() {
                eprintln!("Usage: financial <service-line|payer-mix|denials>");
                return (CommandType::Unknown, remaining_args);
            }
            match remaining_args[0].to_lowercase().as_str() {
                "service-line" => {
                    let service = remaining_args.get(1).cloned().unwrap_or_default();
                    CommandType::Financial(FinancialCommand::ServiceLine { service })
                }
                "payer-mix" => CommandType::Financial(FinancialCommand::PayerMix),
                _ => CommandType::Unknown,
            }
        },

        "alert" => {
            if remaining_args.is_empty() {
                eprintln!("Usage: alert <list|create|ack|dashboard> [args...]");
                return (CommandType::Unknown, remaining_args);
            }
            match remaining_args[0].to_lowercase().as_str() {
                "list" => {
                    let mut patient_id = None;
                    let mut severity = None;
                    let mut i = 1;
                    while i < remaining_args.len() {
                        match remaining_args[i].to_lowercase().as_str() {
                            "--patient-id" => {
                                if i + 1 < remaining_args.len() {
                                    patient_id = remaining_args[i + 1].parse::<i32>().ok();
                                    i += 2;
                                } else { i += 1; }
                            }
                            "--severity" => {
                                if i + 1 < remaining_args.len() {
                                    severity = match remaining_args[i + 1].to_lowercase().as_str() {
                                        "critical" => Some(AlertSeverity::Critical),
                                        "high" => Some(AlertSeverity::High),
                                        "medium" => Some(AlertSeverity::Medium),
                                        "low" => Some(AlertSeverity::Low),
                                        _ => None,
                                    };
                                    i += 2;
                                } else { i += 1; }
                            }
                            _ => i += 1,
                        }
                    }
                    CommandType::Alert(AlertCommand::List {
                        patient_id,
                        severity,
                        type_filter: None,
                        unresolved_only: true,
                    })
                }
                "create" => {
                    let patient_id = remaining_args.get(1).and_then(|s| s.parse::<i32>().ok()).unwrap_or(0);
                    let alert_type = remaining_args.get(2).cloned().unwrap_or_default();
                    let message = remaining_args.get(3).cloned().unwrap_or_default();
                    CommandType::Alert(AlertCommand::Create {
                        patient_id,
                        alert_type,
                        message,
                        severity: None,
                        trigger_source: None,
                        expires_in_hours: None,
                    })
                }
                _ => CommandType::Unknown,
            }
        },

        "pathology" => {
            if remaining_args.is_empty() {
                eprintln!("Usage: pathology <specimen|result|molecular|search>");
                return (CommandType::Unknown, remaining_args);
            }
            match remaining_args[0].to_lowercase().as_str() {
                "result" => {
                    let specimen_id = Uuid::parse_str(&remaining_args[1]).ok().unwrap_or_default();
                    let diagnosis = remaining_args.get(2).cloned().unwrap_or_default();
                    CommandType::Pathology(PathologyCommand::Result {
                        specimen_id,
                        diagnosis,
                        malignancy: None,
                        grade: None,
                        stage: None,
                        margins: None,
                        biomarkers: vec![],
                        pathologist: 0,
                        report_date: None,
                    })
                }
                _ => CommandType::Unknown,
            }
        },

        "microbiology" | "micro" => {
            if remaining_args.is_empty() {
                eprintln!("Usage: microbiology <culture|preliminary|final|resistance>");
                return (CommandType::Unknown, remaining_args);
            }
            match remaining_args[0].to_lowercase().as_str() {
                "final" => {
                    let culture_id = Uuid::parse_str(&remaining_args[1]).ok().unwrap_or_default();
                    let organism = remaining_args.get(2).cloned().unwrap_or_default();
                    CommandType::Microbiology(MicrobiologyCommand::Final {
                        culture_id,
                        organism,
                        sensitivities: vec![],
                        mic_values: vec![],
                        microbiologist: 0,
                    })
                }
                _ => CommandType::Unknown,
            }
        },

        "dosing" => {
            if remaining_args.is_empty() {
                eprintln!("Usage: dosing <calculate|vancomycin|warfarin|chemo> [args...]");
                return (CommandType::Unknown, remaining_args);
            }
            match remaining_args[0].to_lowercase().as_str() {
                "vancomycin" => {
                    let patient_id = remaining_args.get(1).and_then(|s| s.parse::<i32>().ok()).unwrap_or(0);
                    CommandType::Dosing(DosingCommand::Vancomycin {
                        patient_id,
                        trough_level: None,
                        target_auc: None,
                        loading_dose: false,
                    })
                }
                "warfarin" => {
                    let patient_id = remaining_args.get(1).and_then(|s| s.parse::<i32>().ok()).unwrap_or(0);
                    let current_inr = remaining_args.get(2).and_then(|s| s.parse::<f64>().ok()).unwrap_or(0.0);
                    CommandType::Dosing(DosingCommand::Warfarin {
                        patient_id,
                        current_inr,
                        target_inr_range: None,
                        weekly_dose_mg: None,
                        cyp2c9: None,
                        vkorc1: None,
                    })
                }
                _ => CommandType::Unknown,
            }
        },
        _ => CommandType::Unknown,
    };

    // === Fuzzy suggestion ===
    if cmd_type == CommandType::Unknown && !parts.is_empty() {
        let mut best_match: Option<String> = None;
        let mut min_distance = usize::MAX;
        for cmd in &top_level_commands {
            let dist = levenshtein_distance(&command_str, cmd);
            if dist < min_distance {
                min_distance = dist;
                best_match = Some(cmd.to_string());
            }
        }
        if min_distance <= FUZZY_MATCH_THRESHOLD {
            if let Some(suggestion) = best_match {
                eprintln!("Unknown command '{}'. Did you mean '{}'?", command_str, suggestion);
            }
        }
    }
    (cmd_type, remaining_args)
}

// === Command handler ===
#[allow(clippy::too_many_arguments)]
pub async fn handle_interactive_command(
    command: CommandType,
    state: &SharedState,
) -> Result<()> {
    async fn ensure_query_engine(state: &SharedState) -> Result<Arc<QueryExecEngine>> {
        let mut guard = state.query_engine.lock().await;
        if let Some(engine) = guard.as_ref() {
            Ok(Arc::clone(engine))
        } else {
            let engine = get_query_engine_singleton().await?;
            *guard = Some(Arc::clone(&engine));
            Ok(engine)
        }
    }
    // === All other CLI commands (unchanged) ===
    match command {
        CommandType::Daemon(daemon_cmd) => {
            handlers::handle_daemon_command_interactive(daemon_cmd, state.daemon_handles.clone()).await
        }
        CommandType::Rest(rest_cmd) => {
            match rest_cmd {
                RestCliCommand::Status { port, cluster: _ } => {
                    handlers::display_rest_api_status(port, state.rest_api_port_arc.clone()).await;
                    Ok(())
                },
                _ => handlers::handle_rest_command_interactive(
                    rest_cmd,
                    state.rest_api_shutdown_tx_opt.clone(),
                    state.rest_api_handle.clone(),
                    state.rest_api_port_arc.clone(),
                ).await,
            }
        }
        CommandType::Storage(storage_action) => {
            match storage_action {
                StorageAction::Status { port, cluster: _ } => {
                    handlers::display_storage_daemon_status(port, state.storage_daemon_port_arc.clone()).await;
                    Ok(())
                },
                _ => handlers::handle_storage_command_interactive(
                    storage_action,
                    state.storage_daemon_shutdown_tx_opt.clone(),
                    state.storage_daemon_handle.clone(),
                    state.storage_daemon_port_arc.clone(),
                ).await,
            }
        }
        CommandType::StartRest { port, cluster, .. } => {
            handlers::start_rest_api_interactive(
                port,
                cluster,
                state.rest_api_shutdown_tx_opt.clone(),
                state.rest_api_port_arc.clone(),
                state.rest_api_handle.clone(),
            ).await
        }
        CommandType::StartStorage { port, config_file, cluster, .. } => {
            handlers::start_storage_interactive(
                port,
                config_file,
                None,
                cluster,
                state.storage_daemon_shutdown_tx_opt.clone(),
                state.storage_daemon_handle.clone(),
                state.storage_daemon_port_arc.clone(),
            ).await
        }
        CommandType::StartDaemon { port, cluster, .. } => {
            handlers::start_daemon_instance_interactive(port, cluster, state.daemon_handles.clone()).await
        }
        CommandType::StartAll {
            port, cluster, daemon_port, daemon_cluster, listen_port,
            rest_port, rest_cluster, storage_port, storage_cluster, storage_config_file,
        } => {
            handlers::handle_start_all_interactive(
                daemon_port.or(port), daemon_cluster.or(cluster),
                rest_port.or(listen_port), rest_cluster,
                storage_port, storage_cluster, storage_config_file,
                state.daemon_handles.clone(),
                state.rest_api_shutdown_tx_opt.clone(),
                state.rest_api_port_arc.clone(),
                state.rest_api_handle.clone(),
                state.storage_daemon_shutdown_tx_opt.clone(),
                state.storage_daemon_handle.clone(),
                state.storage_daemon_port_arc.clone(),
            ).await
        }
        CommandType::StopAll => {
            handlers::stop_all_interactive(
                state.daemon_handles.clone(),
                state.rest_api_shutdown_tx_opt.clone(),
                state.rest_api_port_arc.clone(),
                state.rest_api_handle.clone(),
                state.storage_daemon_shutdown_tx_opt.clone(),
                state.storage_daemon_handle.clone(),
                state.storage_daemon_port_arc.clone(),
            ).await
        }
        CommandType::StopRest(port) => {
            handlers::stop_rest_api_interactive(
                port,
                state.rest_api_shutdown_tx_opt.clone(),
                state.rest_api_port_arc.clone(),
                state.rest_api_handle.clone(),
            ).await
        }
        CommandType::StopDaemon(port) => {
            handlers::stop_daemon_instance_interactive(port, state.daemon_handles.clone()).await
        }
        CommandType::StopStorage(port) => {
            handlers::stop_storage_interactive(
                port,
                state.storage_daemon_shutdown_tx_opt.clone(),
                state.storage_daemon_handle.clone(),
                state.storage_daemon_port_arc.clone(),
            ).await
        }
        CommandType::StatusSummary => {
            handlers::display_full_status_summary(
                state.rest_api_port_arc.clone(),
                state.storage_daemon_port_arc.clone(),
            ).await;
            Ok(())
        }
        CommandType::StatusDaemon(port) => {
            handlers::display_daemon_status(port).await;
            Ok(())
        }
        CommandType::StatusStorage(port) => {
            handlers::display_storage_daemon_status(port, state.storage_daemon_port_arc.clone()).await;
            Ok(())
        }
        CommandType::StatusCluster => {
            handlers::display_cluster_status().await;
            Ok(())
        }
        CommandType::StatusRaft(port) => {
            handlers::display_raft_status(port).await;
            Ok(())
        }
        CommandType::Auth { username, password } => {
            handlers::authenticate_user(username, password).await;
            Ok(())
        }
        CommandType::Authenticate { username, password } => {
            handlers::authenticate_user(username, password).await;
            Ok(())
        }
        CommandType::RegisterUser { username, password } => {
            handlers::register_user(username, password).await;
            Ok(())
        }
        CommandType::UseStorage { engine, permanent, migrate } => {
            handlers::handle_use_storage_interactive(
                engine,
                permanent,
                migrate,
                state.storage_daemon_shutdown_tx_opt.clone(),
                state.storage_daemon_handle.clone(),
                state.storage_daemon_port_arc.clone(),
            ).await?;
            Ok(())
        }
        CommandType::UsePlugin { enable } => {
            handlers::use_plugin(enable).await;
            Ok(())
        }
        CommandType::Show(action) => {
            match action {
                ShowAction::Storage => { handlers::handle_show_storage_command().await?; }
                ShowAction::Plugins => { handlers::handle_show_plugins_command().await?; }
                ShowAction::Config { config_type } => {
                    match config_type {
                        ConfigAction::All => { handlers::handle_show_all_config_command().await?; }
                        ConfigAction::Rest => { handlers::handle_show_rest_config_command().await?; }
                        ConfigAction::Storage => { handlers::handle_show_storage_config_command().await?; }
                        ConfigAction::Main => { handlers::handle_show_main_config_command().await?; }
                    }
                }
            }
            Ok(())
        }
        CommandType::Version => {
            handlers::display_rest_api_version().await;
            Ok(())
        }
        CommandType::Health => {
            handlers::display_rest_api_health().await;
            Ok(())
        }
        CommandType::ReloadAll => {
            handlers::reload_all_interactive(
                state.daemon_handles.clone(),
                state.rest_api_shutdown_tx_opt.clone(),
                state.rest_api_port_arc.clone(),
                state.rest_api_handle.clone(),
                state.storage_daemon_shutdown_tx_opt.clone(),
                state.storage_daemon_handle.clone(),
                state.storage_daemon_port_arc.clone(),
            ).await
        }
        CommandType::ReloadRest => {
            handlers::reload_rest_interactive(
                state.rest_api_shutdown_tx_opt.clone(),
                state.rest_api_port_arc.clone(),
                state.rest_api_handle.clone(),
            ).await
        }
        CommandType::ReloadStorage => {
            handlers::reload_storage_interactive(
                state.storage_daemon_shutdown_tx_opt.clone(),
                state.storage_daemon_handle.clone(),
                state.storage_daemon_port_arc.clone(),
            ).await
        }
        CommandType::ReloadDaemon(port) => {
            handlers::reload_daemon_interactive(port).await
        }
        CommandType::ReloadCluster => {
            handlers::reload_cluster_interactive().await
        }
        CommandType::RestartAll {
            port, cluster, listen_port, storage_port, storage_config_file,
            daemon_cluster, daemon_port, rest_cluster, rest_port, storage_cluster,
        } => {
            let restart_args = RestartArgs {
                action: RestartAction::All {
                    port, cluster, listen_port, storage_port, storage_config_file,
                    daemon_cluster, daemon_port, rest_cluster, rest_port, storage_cluster,
                },
            };
            handlers::handle_restart_command_interactive(
                restart_args,
                state.daemon_handles.clone(),
                state.rest_api_shutdown_tx_opt.clone(),
                state.rest_api_port_arc.clone(),
                state.rest_api_handle.clone(),
                state.storage_daemon_shutdown_tx_opt.clone(),
                state.storage_daemon_handle.clone(),
                state.storage_daemon_port_arc.clone(),
            ).await
        }
        CommandType::RestartRest { port, cluster, rest_port, rest_cluster } => {
            let restart_args = RestartArgs {
                action: RestartAction::Rest {
                    port: Some(rest_port.unwrap_or(port.unwrap_or_default())),
                    cluster: rest_cluster.clone().or(cluster.clone()),
                    rest_port,
                    rest_cluster
                },
            };
            handlers::handle_restart_command_interactive(
                restart_args,
                state.daemon_handles.clone(),
                state.rest_api_shutdown_tx_opt.clone(),
                state.rest_api_port_arc.clone(),
                state.rest_api_handle.clone(),
                state.storage_daemon_shutdown_tx_opt.clone(),
                state.storage_daemon_handle.clone(),
                state.storage_daemon_port_arc.clone(),
            ).await
        }
        CommandType::RestartStorage { port, config_file, cluster, storage_port, storage_cluster } => {
            let restart_args = RestartArgs {
                action: RestartAction::Storage {
                    port: Some(storage_port.unwrap_or(port.unwrap_or_default())),
                    config_file,
                    cluster: storage_cluster.clone().or(cluster.clone()),
                    storage_port,
                    storage_cluster,
                },
            };
            handlers::handle_restart_command_interactive(
                restart_args,
                state.daemon_handles.clone(),
                state.rest_api_shutdown_tx_opt.clone(),
                state.rest_api_port_arc.clone(),
                state.rest_api_handle.clone(),
                state.storage_daemon_shutdown_tx_opt.clone(),
                state.storage_daemon_handle.clone(),
                state.storage_daemon_port_arc.clone(),
            ).await
        }
        CommandType::RestartDaemon { port, cluster, daemon_port, daemon_cluster } => {
            let restart_args = RestartArgs {
                action: RestartAction::Daemon {
                    port: Some(daemon_port.unwrap_or(port.unwrap_or_default())),
                    cluster: daemon_cluster.clone().or(cluster.clone()),
                    daemon_port,
                    daemon_cluster
                },
            };
            handlers::handle_restart_command_interactive(
                restart_args,
                state.daemon_handles.clone(),
                state.rest_api_shutdown_tx_opt.clone(),
                state.rest_api_port_arc.clone(),
                state.rest_api_handle.clone(),
                state.storage_daemon_shutdown_tx_opt.clone(),
                state.storage_daemon_handle.clone(),
                state.storage_daemon_port_arc.clone(),
            ).await
        }
        CommandType::RestartCluster => {
            let restart_args = RestartArgs {
                action: RestartAction::Cluster,
            };
            handlers::handle_restart_command_interactive(
                restart_args,
                state.daemon_handles.clone(),
                state.rest_api_shutdown_tx_opt.clone(),
                state.rest_api_port_arc.clone(),
                state.rest_api_handle.clone(),
                state.storage_daemon_shutdown_tx_opt.clone(),
                state.storage_daemon_handle.clone(),
                state.storage_daemon_port_arc.clone(),
            ).await
        }
        CommandType::Clear => {
            handlers::clear_terminal_screen().await?;
            handlers::print_welcome_screen();
            Ok(())
        }
        CommandType::Help(help_args) => {
            let mut cmd = CliArgs::command();
            if let Some(command_filter) = help_args.filter_command {
                crate::cli::help_display::print_filtered_help_clap_generated(&mut cmd, &command_filter);
            } else if !help_args.command_path.is_empty() {
                let command_filter = help_args.command_path.join(" ");
                crate::cli::help_display::print_filtered_help_clap_generated(&mut cmd, &command_filter);
            } else {
                crate::cli::help_display::print_help_clap_generated();
            }
            Ok(())
        }
        CommandType::SaveStorage => {
            handlers::handle_save_storage().await;
            Ok(())
        }
        CommandType::SaveConfig => {
            handlers::handle_save_config().await;
            Ok(())
        }
        CommandType::Exit => {
            Ok(())
        }
        CommandType::Kv { action } => {
            let query_engine = ensure_query_engine(state).await?;
            let (operation, key, value) = match action {
                KvAction::Get { key } => ("get".to_string(), key, None),
                KvAction::Set { key, value } => ("set".to_string(), key, Some(value)),
                KvAction::Delete { key } => ("delete".to_string(), key, None),
            };
            handlers::handle_kv_command(query_engine, operation, key, value).await?;
            Ok(())
        }
        CommandType::Query { query, language } => {
            let query_engine = ensure_query_engine(state).await?;
            // === Language Detection ===
            let lang = language.as_deref().unwrap_or("").to_ascii_lowercase();
            let trimmed = query.trim_start();
            let detected_lang = if lang == "cypher"
                || trimmed.starts_with("MATCH ")
                || trimmed.starts_with("CREATE ")
                || trimmed.starts_with("MERGE ")
            {
                "cypher"
            } else if lang == "sql"
                || trimmed.to_ascii_uppercase().starts_with("SELECT ")
                || trimmed.to_ascii_uppercase().starts_with("INSERT ")
            {
                "sql"
            } else if lang == "graphql"
                || trimmed.starts_with('{')
                || trimmed.starts_with("query ")
                || trimmed.starts_with("mutation ")
            {
                "graphql"
            } else {
                // Auto-detect fallback
                if trimmed.to_ascii_lowercase().starts_with("match ")
                    || trimmed.to_ascii_lowercase().starts_with("create ")
                    || trimmed.to_ascii_lowercase().starts_with("merge ")
                {
                    "cypher"
                } else if trimmed
                    .to_ascii_uppercase()
                    .starts_with("SELECT ")
                    || trimmed.to_ascii_uppercase().starts_with("INSERT ")
                    || trimmed.to_ascii_uppercase().starts_with("UPDATE ")
                    || trimmed.to_ascii_uppercase().starts_with("DELETE ")
                {
                    "sql"
                } else if trimmed.starts_with('{')
                    || trimmed.starts_with("query ")
                    || trimmed.starts_with("mutation ")
                {
                    "graphql"
                } else {
                    eprintln!("Warning: Could not detect query language. Use --language cypher|sql|graphql");
                    return Ok(());
                }
            };
            // === Execution ===
            match detected_lang {
                "cypher" => {
                    println!("[Cypher] {}", query);
                    match query_engine.execute_cypher(&query).await {
                        Ok(res) => println!("{}", res),
                        Err(e) => eprintln!("Cypher Error: {}", e),
                    }
                }
                "sql" => {
                    println!("[SQL] {}", query);
                    match query_engine.execute_sql(&query).await {
                        Ok(res) => println!("{}", res),
                        Err(e) => eprintln!("SQL Error: {}", e),
                    }
                }
                "graphql" => {
                    println!("[GraphQL]\n{}", query);
                    match query_engine.execute_graphql(&query).await {
                        Ok(res) => println!("{}", res),
                        Err(e) => eprintln!("GraphQL Error: {}", e),
                    }
                }
                _ => unreachable!(),
            }
            Ok(())
        }
        CommandType::Visualize { query, language } => {
            let query_engine = ensure_query_engine(state).await?;
            // === Language Detection ===
            let lang = language.as_deref().unwrap_or("").to_ascii_lowercase();
            let trimmed = query.trim_start();
            let detected_lang = if lang == "cypher"
                || trimmed.starts_with("MATCH ")
                || trimmed.starts_with("CREATE ")
                || trimmed.starts_with("MERGE ")
            {
                "cypher"
            } else if lang == "sql"
                || trimmed.to_ascii_uppercase().starts_with("SELECT ")
                || trimmed.to_ascii_uppercase().starts_with("INSERT ")
            {
                "sql"
            } else if lang == "graphql"
                || trimmed.starts_with('{')
                || trimmed.starts_with("query ")
                || trimmed.starts_with("mutation ")
            {
                "graphql"
            } else {
                // Auto-detect fallback
                if trimmed.to_ascii_lowercase().starts_with("match ")
                    || trimmed.to_ascii_lowercase().starts_with("create ")
                    || trimmed.to_ascii_lowercase().starts_with("merge ")
                {
                    "cypher"
                } else if trimmed
                    .to_ascii_uppercase()
                    .starts_with("SELECT ")
                    || trimmed.to_ascii_uppercase().starts_with("INSERT ")
                    || trimmed.to_ascii_uppercase().starts_with("UPDATE ")
                    || trimmed.to_ascii_uppercase().starts_with("DELETE ")
                {
                    "sql"
                } else if trimmed.starts_with('{')
                    || trimmed.starts_with("query ")
                    || trimmed.starts_with("mutation ")
                {
                    "graphql"
                } else {
                    eprintln!("Warning: Could not detect query language. Use --language cypher|sql|graphql");
                    return Ok(());
                }
            };
            // === VISUALIZATION (NOT PRINTING) ===
            match detected_lang {
                "cypher" => {
                    println!("[Cypher] {}", query);
                    handlers::handle_cypher_query_visualizing(query_engine, query.to_string()).await?
                }
                "sql" => {
                    println!("[SQL] {}", query);
                    handlers::handle_sql_query_visualizing(query_engine, query.to_string()).await?
                }
                "graphql" => {
                    println!("[GraphQL]\n{}", query);
                    handlers::handle_graphql_query_visualizing(query_engine, query.to_string()).await?
                }
                _ => unreachable!(),
            }
            Ok(())
        }
        CommandType::Unknown => {
            println!("Unknown command. Type 'help' for a list of commands.");
            Ok(())
        }
        CommandType::Migrate(action) => {
            let from_engine = action.from_engine_pos.or(action.from).or(action.source).ok_or_else(|| {
                anyhow::anyhow!("No source engine specified. Use positional argument or --from/--source.")
            })?;
            let to_engine = action.to_engine_pos.or(action.to).or(action.dest).ok_or_else(|| {
                anyhow::anyhow!("No destination engine specified. Use positional argument or --to/--dest.")
            })?;
            handlers::handle_migrate_interactive(
                from_engine,
                to_engine,
                state.storage_daemon_shutdown_tx_opt.clone(),
                state.storage_daemon_handle.clone(),
                state.storage_daemon_port_arc.clone(),
            ).await?;
            Ok(())
        }
        CommandType::Graph(action) => {
            let engine = ensure_query_engine(state).await?;
            crate::cli::handlers_graph::handle_graph_command(engine, action).await?;
            Ok(())
        }
        CommandType::Index(action) => {
            let engine = ensure_query_engine(state).await?;
            crate::cli::handlers_index::handle_index_command(action).await?;
            Ok(())
        }
        CommandType::Patient(command) => {
            Ok(())
        }
        CommandType::Encounter(command) => {
            Ok(())
        }
        CommandType::Diagnosis(command) => {
            Ok(())
        }
        CommandType::Prescription(command) => {
            Ok(())
        }
        CommandType::Note(command) => {
            Ok(())
        }
        CommandType::Referral(command) => {
            Ok(())
        }
        CommandType::Triage(command) => {
            Ok(())
        }
        CommandType::Disposition(command) => {
            Ok(())
        }
        CommandType::Drug(command) => {
            Ok(())
        }
        CommandType::Population(command) => {
            Ok(())
        }
        CommandType::Audit(command) => {
            Ok(())
        }
        CommandType::Export(command) => {
            Ok(())
        }
        CommandType::Vitals(command) => {
            Ok(())
        }
        CommandType::Allergy(command) => {
            Ok(())
        }
        CommandType::Appointment(command) => {
            Ok(())
        }
        CommandType::Order(command) => {
            Ok(())
        }
        CommandType::Problem(command) => {
            Ok(())
        }
        CommandType::Procedure(command) => {
            Ok(())
        }
        CommandType::Discharge(command) => {
            Ok(())
        }
        CommandType::Dosing(command) => {
            Ok(())
        }
        CommandType::Alert(command) => {
            Ok(())
        }
        CommandType::Pathology(command) => {
            Ok(())
        }
        CommandType::Microbiology(command) => {
            Ok(())
        }
        CommandType::Observation(command) => {
            Ok(())
        }
        CommandType::Lab(command) => {
            Ok(())
        }
        CommandType::Imaging(command) => {
            Ok(())
        }
        CommandType::Chemo(command) => {
            Ok(())
        }
        CommandType::Radiation(command) => {
            Ok(())
        }
        CommandType::Surgery(command) => {
            Ok(())
        }
        CommandType::Analytics(command) => {
            Ok(())
        }
        CommandType::Metrics(command) => {
            Ok(())
        }
        CommandType::Facility(command) => {
            Ok(())
        }
        CommandType::Access(command) => {
            Ok(())
        }
        CommandType::Financial(command) => {
            Ok(())
        }
        CommandType::Quality(command) => {
            Ok(())
        }
        CommandType::Incident(command) => {
            Ok(())
        }
        CommandType::Compliance(command) => {
            Ok(())
        }
        CommandType::Research(command) => {
            Ok(())
        }
        CommandType::Ml(command) => {
            Ok(())
        }
        CommandType::ClinicalTrial(command) => {
            Ok(())
        }
        CommandType::Model(command) => {
            Ok(())
        }
        CommandType::Nursing(command) => {
            Ok(())
        }
        CommandType::Education(command) => {
            Ok(())
        }
        CommandType::DischargePlanning(command) => {
            Ok(())
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub async fn run_cli_interactive(
    daemon_handles: Arc<TokioMutex<HashMap<u16, (JoinHandle<()>, oneshot::Sender<()>)>>>,
    rest_api_shutdown_tx_opt: Arc<TokioMutex<Option<oneshot::Sender<()>>>>,
    rest_api_port_arc: Arc<TokioMutex<Option<u16>>>,
    rest_api_handle: Arc<TokioMutex<Option<JoinHandle<()>>>>,
    storage_daemon_shutdown_tx_opt: Arc<TokioMutex<Option<oneshot::Sender<()>>>>,
    storage_daemon_handle: Arc<TokioMutex<Option<JoinHandle<()>>>>,
    storage_daemon_port_arc: Arc<TokioMutex<Option<u16>>>,
) -> Result<()> {
    let mut rl = DefaultEditor::new()?;
    let history_path = "graphdb_cli_history.txt";
    let _ = rl.load_history(history_path);
    handlers::print_welcome_screen();

    let state = SharedState {
        daemon_handles,
        rest_api_shutdown_tx_opt,
        rest_api_port_arc,
        rest_api_handle,
        storage_daemon_shutdown_tx_opt,
        storage_daemon_handle,
        storage_daemon_port_arc,
        query_engine: Arc::new(TokioMutex::new(None)),
    };

    loop {
        let readline = rl.readline("GraphDB> ");
        match readline {
            Ok(line) => {
                let line_trim = line.trim();
                if line_trim.is_empty() {
                    continue;
                }
                rl.add_history_entry(line_trim).ok();

                // === RAW QUERY DETECTION (before shlex) ===
                if let Some((language, query)) = detect_raw_query_language(line_trim) {
                    let cmd = CommandType::Query {
                        query,                       // String
                        language: Some(language.into()), // &str → String
                    };
                    if let Err(e) = handle_interactive_command(cmd, &state).await {
                        eprintln!("Query error: {}", e);
                    }
                    continue;
                }

                // === NORMAL COMMAND PATH ===
                let args = match shlex::split(line_trim) {
                    Some(a) => a,
                    None => {
                        eprintln!("Error: Malformed input. Please check quoting.");
                        continue;
                    }
                };
                if args.is_empty() {
                    continue;
                }

                let (command, _parsed_args) = parse_command(&args);
                debug!("Parsed command: {:?}", command);

                if command == CommandType::Exit {
                    handle_interactive_command(command, &state).await?;
                    break;
                }

                if let Err(e) = handle_interactive_command(command, &state).await {
                    eprintln!("Error: {}", e);
                    debug!("Full error: {:#}", e);
                }
            }

            Err(ReadlineError::Interrupted) => {
                println!("Ctrl-C received. Type 'exit' to quit.");
            }
            Err(ReadlineError::Eof) => {
                println!("Ctrl-D received. Goodbye!");
                break;
            }
            Err(err) => {
                eprintln!("Readline error: {:?}", err);
                break;
            }
        }
    }

    rl.save_history(&history_path)
        .context("Failed to save history")?;
    Ok(())
}
