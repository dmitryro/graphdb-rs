// server/src/cli/mod.rs

// This module contains the command-line interface (CLI) logic for the GraphDB server.
// It includes argument parsing, command handling, and interactive mode.

pub mod cli;
pub mod daemon_management;
pub mod handlers;
pub mod help_display;
pub mod interactive;
pub mod handlers_user;
pub mod handlers_utils;
pub mod handlers_main;
pub mod handlers_storage;
pub mod handlers_rest;
pub mod handlers_all;
pub mod handlers_queries;
pub mod handlers_history;
pub mod handlers_index;
pub mod handlers_graph;
pub mod handlers_visualizing;
pub mod query_classifier;
pub mod handlers_medical;
pub mod handlers_connectors;
pub mod handlers_hl7;
pub mod handlers_fhir;
pub mod handlers_notes;
pub mod handlers_snomed_codes;
pub mod handlers_patient_journey;
pub mod handlers_encounters;
pub mod handlers_drug_interactions;
pub mod handlers_mpi;
pub mod handlers_patient;

// Re-export the main CLI entry point from cli.rs
pub use cli::{start_cli, CliArgs, Commands, get_storage_engine_singleton }; // Corrected: Changed run_cli to start_cli
pub use lib::daemon::*;
pub use lib::config::config_structs::*;
pub use lib::config::config_defaults::*;
pub use lib::config::config_constants::*;
pub use lib::config::config_helpers::*;
pub use lib::config::config_serializers::*;
pub use lib::config::{
    load_cli_config,
    load_storage_config_str,
    load_storage_config_from_yaml,
    get_default_rest_port_from_config,
    get_default_storage_port_from_config_or_cli_default,
    get_storage_cluster_range,
    get_default_daemon_port,
    get_daemon_cluster_range,
    get_default_rest_port,
    get_rest_cluster_range,
    CliConfig,
    CliConfigToml,
    StorageConfig,
    ServerConfig,
    RestApiConfig,
    MainDaemonConfig,
    DaemonYamlConfig,
    StorageEngineType,
    CLI_ASSUMED_DEFAULT_STORAGE_PORT_FOR_STATUS,
    DAEMON_NAME_STORAGE_DAEMON,
};
// Re-export specific types/functions from other modules if they are part of the public CLI API
pub use lib::commands::{
    DaemonCliCommand,
    RestCliCommand,
    StorageAction,
    StatusArgs,
    StopArgs,
    ReloadArgs,
    RestartArgs,
    StartAction,
    StopAction,
    ReloadAction,
    RestartAction,
    StatusAction,
};
pub use handlers_utils::{
    get_current_exe_path,
    format_engine_config,
    write_registry_fallback,
    read_registry_fallback,
    print_welcome_screen,
    clear_terminal_screen,
    ensure_daemon_registry_paths_exist,
    execute_storage_query,
    storage_engine_type_to_str,
    parse_show_command,
    parse_storage_engine,
    StartStorageFn,
    StopStorageFn,
    START_STORAGE_FN_SINGLETON,
    STOP_STORAGE_FN_SINGLETON,
    get_start_storage_fn,
    get_stop_storage_fn,
    convert_hashmap_to_selected_config,
    adapt_start_storage,
    adapt_stop_storage,
};


pub use handlers_queries::{
    handle_interactive_query,
    handle_kv_command,
    handle_unified_query,
    handle_exec_command,
    handle_query_command,
    handle_cypher_query,
    handle_sql_query,
    handle_graphql_query,
    handle_cleanup_command_interactive,
    handle_cleanup_command,
};

pub use handlers_index::{
    handle_index_command,
    initialize_storage_for_index,
};

pub use handlers_history::{
    handle_history_command,
    handle_history_command_interactive,
    save_history_metadata,
};

pub use handlers_user::*;

pub use handlers_graph::{
    handle_graph_command,
};

pub use handlers_visualizing:: {
    handle_cypher_query_visualizing,
    handle_sql_query_visualizing,
    handle_graphql_query_visualizing,
    handle_unified_query_visualizing,
    handle_interactive_query_visualizing,
};

pub use handlers_medical::*;
pub use handlers_connectors::*;
pub use handlers_hl7::*;
pub use handlers_fhir::*;
pub use handlers_notes::*;
pub use handlers_snomed_codes::*;
pub use handlers_patient_journey::*;
pub use handlers_encounters::*;
pub use handlers_drug_interactions::*;
pub use handlers_mpi::*;
pub use handlers_patient::*;

pub use handlers_main::{
    DaemonArgs,
    display_daemon_status,
    handle_daemon_command,
    handle_daemon_command_interactive,
    start_daemon_instance_interactive, 
    stop_main_interactive,
    stop_daemon_instance_interactive,
    reload_daemon_interactive,
    handle_show_main_config_command,
}; 

pub use handlers_storage::{
    storage,
    show_storage,
    display_storage_daemon_status,
    handle_storage_command,
    handle_storage_command_interactive,
    start_storage_interactive,
    stop_storage,
    stop_storage_interactive,
    use_storage_engine,
    handle_save_storage,
    reload_storage_interactive,
    handle_migrate_command,
    handle_migrate_interactive,
    handle_use_storage_interactive,
    handle_use_storage_command,
    handle_show_storage_command,
    handle_show_storage_command_interactive,
    handle_show_storage_config_command,
};

pub use handlers_rest::{
    RestArgs,
    rest,
    register_user,
    authenticate_user,
    display_rest_api_status,
    handle_rest_command,
    handle_rest_command_interactive,
    start_rest_api_interactive,
    stop_rest_api_interactive,
    display_rest_api_health,
    display_rest_api_version,
    execute_graph_query,
    reload_rest_interactive,
    handle_show_rest_config_command
};

pub use handlers_all::{
    stop_all_interactive,
    reload_all_interactive,
    handle_start_all_interactive,
    display_full_status_summary,
    handle_show_all_config_command,
};
  
pub use handlers::{
    handle_status_command,
    handle_stop_command,
    handle_start_command,
    handle_reload_command,
    handle_restart_command_interactive, // FIX: Changed to handle_restart_command_interactive
    handle_start_command_interactive,
    handle_show_plugins_command,
    handle_show_plugins_command_interactive,
};

pub use interactive::{
    run_cli_interactive,
};

pub use help_display::{
    print_help_clap_generated,
    print_filtered_help_clap_generated,
    collect_all_cli_elements_for_suggestions,
    print_interactive_help,
    print_interactive_filtered_help,
};

pub use daemon_management::{
    start_daemon_process,
    stop_daemon_api_call,
    find_running_storage_daemon_port,
    clear_all_daemon_processes,
};
