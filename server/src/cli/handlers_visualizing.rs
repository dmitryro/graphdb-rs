// server/src/cli/handlers_visualize.rs
use anyhow::{anyhow, Context, Result};
use chrono::Utc;
use log::{debug, error, info, warn};
use serde_json::{json, Value};
use std::path::{PathBuf, Path};
use std::pin::Pin;
use std::sync::Arc;
use std::os::unix::fs::PermissionsExt;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::task::{self, JoinHandle};
use tokio::sync::{oneshot, Mutex as TokioMutex};
use tokio::time::{self, timeout, Duration as TokioDuration};
use lib::daemon::daemon_management::{
    check_pid_validity, is_port_free, is_storage_daemon_running, find_pid_by_port,
    check_daemon_health, restart_daemon_process, stop_storage_daemon_by_port,
    check_pid_validity_sync, get_or_create_daemon_with_ipc, check_ipc_socket_exists,
    verify_and_recover_ipc, is_zmq_reachable,
};
use lib::daemon::daemon_registry::{GLOBAL_DAEMON_REGISTRY, DaemonMetadata};
use lib::query_exec_engine::query_exec_engine::QueryExecEngine;
use lib::query_parser::{parse_query_from_string, QueryType};
use lib::config::{
    StorageEngineType, SledConfig, RocksDBConfig, DEFAULT_STORAGE_CONFIG_PATH_RELATIVE,
    DEFAULT_DATA_DIRECTORY, default_data_directory, default_log_directory,
    daemon_api_storage_engine_type_to_string, load_cli_config,
};
use lib::storage_engine::storage_engine::{
    StorageEngine, GraphStorageEngine, AsyncStorageEngineManager, StorageEngineManager,
    GLOBAL_STORAGE_ENGINE_MANAGER,
};
use lib::commands::parse_kv_operation;
use graph_engine::graph::Graph;
use lib::storage_engine::rocksdb_storage::{ROCKSDB_DB, ROCKSDB_POOL_MAP};
use lib::storage_engine::sled_storage::{SLED_DB, SLED_POOL_MAP};
use lib::config::{StorageConfig, MAX_SHUTDOWN_RETRIES, SHUTDOWN_RETRY_DELAY_MS, load_storage_config_from_yaml, 
                  QueryPlan, QueryResult, SledDbWithPath, SledDaemon };
use lib::database::Database;
use lib::query_parser::{cypher_parser, sql_parser, graphql_parser};
use crate::cli::handlers_storage::{start_storage_interactive};
use crate::cli::handlers_utils::{ StartStorageFn, StopStorageFn };
use crate::cli::handlers_queries::{ format_json_result,  execute_query, verify_and_wait_for_ipc,
                                    find_or_create_working_daemon, initialize_storage_for_query,
                                    handle_query_command, do_zmq_request, execute_and_print, 
                                  };
use models::errors::{GraphError, GraphResult};
use daemon_api::{start_daemon, stop_port_daemon};
use lib::storage_engine::sled_client::SledClient;
use lib::storage_engine::rocksdb_client::RocksDBClient;
use zmq::{Context as ZmqContext, Message};
use base64::Engine;

pub async fn handle_cypher_query_visualizing(
    engine: Arc<QueryExecEngine>,
    query: String,
) -> Result<()> {
    let trimmed_query = query.trim();
    
    if trimmed_query.is_empty() {
        return Err(anyhow!("Cypher query cannot be empty"));
    }

    info!("Executing Cypher query: {}", trimmed_query);

    let result = engine
        .execute_cypher(trimmed_query)
        .await
        .map_err(|e| anyhow!("Cypher execution failed: {e}"))?;

    let pretty_result = serde_json::to_string_pretty(&result)
        .map_err(|e| anyhow!("Failed to serialize query result: {e}"))?;

    println!("Cypher Query Result:\n{pretty_result}");
    
    Ok(())
}

pub async fn handle_sql_query_visualizing(
    engine: Arc<QueryExecEngine>,
    query: String,
) -> Result<()> {
    info!("Executing SQL query: {}", query);
    println!("Executing SQL query: {}", query);

    if query.trim().is_empty() {
        return Err(anyhow!("SQL query cannot be empty"));
    }

    let result = engine
        .execute_sql(&query)
        .await
        .map_err(|e| anyhow!("Failed to execute SQL query: {}", e))?;

    println!("SQL Query Result:\n{}", serde_json::to_string_pretty(&result)?);
    Ok(())
}

pub async fn handle_graphql_query_visualizing(
    engine: Arc<QueryExecEngine>,
    query: String,
) -> Result<()> {
    info!("Executing GraphQL query: {}", query);
    println!("Executing GraphQL query: {}", query);

    if query.trim().is_empty() {
        return Err(anyhow!("GraphQL query cannot be empty"));
    }

    let result = engine
        .execute_graphql(&query)
        .await
        .map_err(|e| anyhow!("Failed to execute GraphQL query: {}", e))?;

    println!("GraphQL Query Result:\n{}", serde_json::to_string_pretty(&result)?);
    Ok(())
}

pub async fn handle_unified_query_visualizing(
    engine: Arc<QueryExecEngine>,
    query_string: String,
    language: Option<String>,
) -> Result<()> {
    info!("Executing unified query: {}", query_string);
    let query = query_string.trim().to_string();

    if query.is_empty() {
        return Err(anyhow!("Query cannot be empty"));
    }

    let effective_lang = language.as_deref().unwrap_or("").trim().to_lowercase();

    match effective_lang.as_str() {
        "cypher" | "" => handle_cypher_query_visualizing(engine, query).await,
        "sql" => handle_sql_query_visualizing(engine, query).await,
        "graphql" => handle_graphql_query_visualizing(engine, query).await,
        _ => Err(anyhow!(
            "Unsupported language: '{}'. Use cypher, sql, or graphql",
            effective_lang
        )),
    }
}

pub async fn handle_interactive_query_visualizing(engine: Arc<QueryExecEngine>, query_string: String) -> Result<()> {
    let normalized_query = query_string.trim().to_uppercase();
    info!("Attempting to identify interactive query: '{}'", normalized_query);
    if normalized_query == "EXIT" || normalized_query == "QUIT" {
        return Ok(());
    }
    handle_unified_query_visualizing(engine, query_string, None).await
}
