// server/src/cli/handlers_visualize.rs
use anyhow::{anyhow, Context, Result};
use chrono::Utc;
use log::{debug, error, info, warn};
use serde_json::{json, Value, Map};
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
use models::{ Graph, Edge, Vertex };
use models::errors::{GraphError, GraphResult};
use daemon_api::{start_daemon, stop_port_daemon};
use lib::storage_engine::sled_client::SledClient;
use lib::storage_engine::rocksdb_client::RocksDBClient;
use zmq::{Context as ZmqContext, Message};
use base64::Engine;
use graph_visualizing::visualizing::{GraphJson, JsonVertex, JsonEdge};

/// Converts a Cypher query result into GraphJson format for visualization
fn convert_cypher_result_to_graph_json(result: &Value) -> Result<GraphJson> {
    let mut vertices = Vec::new();
    let mut edges = Vec::new();
    let mut vertex_ids = std::collections::HashSet::new();
    let mut edge_ids = std::collections::HashSet::new();

    // Handle array of records
    let records = if let Value::Array(arr) = result {
        arr
    } else if let Value::Object(obj) = result {
        // Single record
        return convert_single_record_to_graph_json(obj);
    } else {
        return Err(anyhow!("Unexpected query result format"));
    };

    for record in records {
        if let Value::Object(record_map) = record {
            // Extract all vertices from the record
            for (key, value) in record_map {
                if key.starts_with('r') && key.len() > 1 && key.chars().nth(1).unwrap_or('x').is_ascii_digit() {
                    // This is an edge (r1, r2, etc.)
                    if let Some(edge) = try_extract_edge_from_flat_record(value, key)? {
                        if edge_ids.insert(edge.id.clone()) {
                            edges.push(edge);
                        }
                    }
                } else {
                    // This is a vertex (p, e, d, etc.)
                    if let Some(vertex) = try_extract_vertex_from_flat_record(value, key)? {
                        if vertex_ids.insert(vertex.id.clone()) {
                            vertices.push(vertex);
                        }
                    }
                }
            }
        }
    }

    // Ensure all referenced vertices exist
    let vertex_id_set: std::collections::HashSet<String> = vertices.iter().map(|v| v.id.clone()).collect();
    edges.retain(|e| vertex_id_set.contains(&e.source) && vertex_id_set.contains(&e.target));

    Ok(GraphJson { vertices, edges })
}

/// Tries to extract a vertex from a JSON value
fn try_extract_vertex(value: &Value, _alias: &str) -> Result<Option<JsonVertex>> {
    if let Value::Object(obj) = value {
        // Check for vertex markers
        if obj.get("resourceType").is_some() || obj.get("label").is_some() || obj.get("vertex_id").is_some() {
            // Extract ID
            let id = if let Some(id_val) = obj.get("id").or_else(|| obj.get("vertex_id")) {
                id_val.as_str().unwrap_or("unknown").to_string()
            } else if let Some(uuid_val) = obj.get("uuid") {
                uuid_val.as_str().unwrap_or("unknown").to_string()
            } else {
                // Generate ID from properties hash if needed
                "unknown".to_string()
            };

            // Extract label
            let label = if let Some(label_val) = obj.get("label").or_else(|| obj.get("resourceType")) {
                label_val.as_str().unwrap_or("Vertex").to_string()
            } else {
                "Vertex".to_string()
            };

            // Use the entire object as properties
            let properties = value.clone();

            return Ok(Some(JsonVertex {
                id,
                label,
                properties,
            }));
        }
    }
    Ok(None)
}

/// Tries to extract an edge from a JSON value
fn try_extract_edge(value: &Value, _alias: &str) -> Result<Option<JsonEdge>> {
    if let Value::Object(obj) = value {
        // Check for edge markers
        if obj.get("type").is_some() || obj.get("relationship_type").is_some() || obj.get("edge_id").is_some() {
            // Extract ID
            let id = if let Some(id_val) = obj.get("id").or_else(|| obj.get("edge_id")) {
                id_val.as_str().unwrap_or("unknown").to_string()
            } else {
                "unknown".to_string()
            };

            // Extract source and target
            let source = if let Some(start_val) = obj.get("start").or_else(|| obj.get("source")) {
                start_val.as_str().unwrap_or("unknown").to_string()
            } else if let Some(nodes) = obj.get("nodes").and_then(|n| n.as_array()) {
                nodes.first().and_then(|n| n.as_str()).unwrap_or("unknown").to_string()
            } else {
                "unknown".to_string()
            };

            let target = if let Some(end_val) = obj.get("end").or_else(|| obj.get("target")) {
                end_val.as_str().unwrap_or("unknown").to_string()
            } else if let Some(nodes) = obj.get("nodes").and_then(|n| n.as_array()) {
                nodes.get(1).and_then(|n| n.as_str()).unwrap_or("unknown").to_string()
            } else {
                "unknown".to_string()
            };

            // Extract label/type
            let label = if let Some(type_val) = obj.get("type").or_else(|| obj.get("relationship_type")) {
                type_val.as_str().unwrap_or("RELATED_TO").to_string()
            } else {
                "RELATED_TO".to_string()
            };

            // Use the entire object as properties
            let properties = value.clone();

            return Ok(Some(JsonEdge {
                id,
                source,
                target,
                label,
                properties,
            }));
        }
    }
    Ok(None)
}

/// Handles single record (object) results
fn convert_single_record_to_graph_json(record: &Map<String, Value>) -> Result<GraphJson> {
    let mut vertices = Vec::new();
    let mut edges = Vec::new();
    let mut vertex_ids = std::collections::HashSet::new();
    let mut edge_ids = std::collections::HashSet::new();

    for (key, value) in record {
        if key.starts_with('r') && key.len() > 1 && key.chars().nth(1).unwrap_or('x').is_ascii_digit() {
            if let Some(edge) = try_extract_edge_from_flat_record(value, key)? {
                if edge_ids.insert(edge.id.clone()) {
                    edges.push(edge);
                }
            }
        } else {
            if let Some(vertex) = try_extract_vertex_from_flat_record(value, key)? {
                if vertex_ids.insert(vertex.id.clone()) {
                    vertices.push(vertex);
                }
            }
        }
    }

    let vertex_id_set: std::collections::HashSet<String> = vertices.iter().map(|v| v.id.clone()).collect();
    edges.retain(|e| vertex_id_set.contains(&e.source) && vertex_id_set.contains(&e.target));

    Ok(GraphJson { vertices, edges })
}

fn try_extract_edge_from_flat_record(value: &Value, alias: &str) -> Result<Option<JsonEdge>> {
    if let Value::Object(obj) = value {
        // Extract ID
        let id = if let Some(id_val) = obj.get("id").or_else(|| obj.get("edge_id")) {
            id_val.as_str().unwrap_or(alias).to_string()
        } else {
            alias.to_string()
        };

        // Extract source and target (from start/end or from relationship context)
        let (source, target) = if let (Some(start), Some(end)) = (obj.get("start"), obj.get("end")) {
            (
                start.as_str().unwrap_or("unknown").to_string(),
                end.as_str().unwrap_or("unknown").to_string(),
            )
        } else {
            // Fallback: use aliases from the query pattern
            // For r1 between p and e: source=p, target=e
            // This is approximate but works for simple patterns
            let mut keys: Vec<&String> = obj.keys().collect();
            keys.sort();
            if keys.len() >= 2 {
                ("unknown".to_string(), "unknown".to_string())
            } else {
                ("unknown".to_string(), "unknown".to_string())
            }
        };

        // Extract label/type
        let label = if let Some(type_val) = obj.get("label").or_else(|| obj.get("type")).or_else(|| obj.get("relationship_type")) {
            type_val.as_str().unwrap_or("RELATED_TO").to_string()
        } else {
            "RELATED_TO".to_string()
        };

        Ok(Some(JsonEdge {
            id,
            source,
            target,
            label,
            properties: value.clone(),
        }))
    } else {
        Ok(None)
    }
}

fn try_extract_vertex_from_flat_record(value: &Value, alias: &str) -> Result<Option<JsonVertex>> {
    if let Value::Object(obj) = value {
        // Extract ID (look for id, vertex_id, uuid)
        let id = if let Some(id_val) = obj.get("id").or_else(|| obj.get("vertex_id")) {
            id_val.as_str().unwrap_or(alias).to_string()
        } else if let Some(uuid_val) = obj.get("uuid") {
            uuid_val.as_str().unwrap_or(alias).to_string()
        } else {
            alias.to_string() // Use alias as ID fallback
        };

        // Extract label (look for label, resourceType)
        let label = if let Some(label_val) = obj.get("label").or_else(|| obj.get("resourceType")) {
            label_val.as_str().unwrap_or("Vertex").to_string()
        } else {
            "Vertex".to_string()
        };

        Ok(Some(JsonVertex {
            id,
            label,
            properties: value.clone(),
        }))
    } else {
        Ok(None)
    }
}
/// Extracts vertex from Neo4j-style path node
fn try_extract_vertex_from_path_node(node: &Value) -> Result<Option<JsonVertex>> {
    if let Value::String(node_str) = node {
        let id = node_str.trim_matches(|c| c == '(' || c == ')').split(':').next().unwrap_or("unknown").to_string();
        let label = if node_str.contains(':') {
            node_str.split(':').nth(1).unwrap_or("Vertex").split_whitespace().next().unwrap_or("Vertex").to_string()
        } else {
            "Vertex".to_string()
        };

        Ok(Some(JsonVertex {
            id,
            label,
            properties: json!({}),
        }))
    } else if let Value::Object(obj) = node {
        // Wrap `obj` back into a `Value`
        try_extract_vertex(&Value::Object(obj.clone()), "node")
    } else {
        Ok(None)
    }
}

/// Extracts edge from Neo4j-style path relationship
fn try_extract_edge_from_path_rel(rel: &Value) -> Result<Option<JsonEdge>> {
    if let Value::String(rel_str) = rel {
        let id = rel_str.trim_matches(|c| c == '[' || c == ']').split(':').next().unwrap_or("unknown").to_string();
        let label = if rel_str.contains(':') {
            rel_str.split(':').nth(1).unwrap_or("RELATED_TO").split_whitespace().next().unwrap_or("RELATED_TO").to_string()
        } else {
            "RELATED_TO".to_string()
        };

        Ok(Some(JsonEdge {
            id,
            source: "unknown".to_string(),
            target: "unknown".to_string(),
            label,
            properties: json!({}),
        }))
    } else if let Value::Object(obj) = rel {
        // Wrap `obj` back into a `Value`
        try_extract_edge(&Value::Object(obj.clone()), "relationship")
    } else {
        Ok(None)
    }
}

pub async fn handle_cypher_query_visualizing(
    engine: Arc<QueryExecEngine>,
    query: String,
) -> Result<()> {
    eprintln!("[DEBUG] visualize handler STARTED");
    println!("==========================> HEY  visualize handler STARTED");
    let trimmed_query = query.trim();
    if trimmed_query.is_empty() {
        eprintln!("[DEBUG] empty query");
        return Err(anyhow!("Cypher query cannot be empty"));
    }

    let result = engine
        .execute_cypher(trimmed_query)
        .await
        .map_err(|e| anyhow!("Cypher execution failed: {e}"))?;

    eprintln!("[DEBUG] Raw result: {}", serde_json::to_string_pretty(&result).unwrap());

    let graph_json: GraphJson = serde_json::from_value(result)
        .map_err(|e| {
            eprintln!("[DEBUG] Failed to parse as GraphJson: {}", e);
            anyhow!("Result is not a valid graph structure: {e}")
        })?;

    eprintln!("[DEBUG] Parsed {} vertices", graph_json.vertices.len());

    if graph_json.vertices.is_empty() {
        println!("No vertices to visualize.");
        return Ok(());
    }

    let json_str = serde_json::to_string(&graph_json).unwrap();
    eprintln!("[DEBUG] About to call visualize_graph_from_json");

    graph_visualizing::visualizing::visualize_graph_from_json(&json_str)
        .map_err(|e| anyhow!("Graph visualization failed: {}", e))?;

    eprintln!("[DEBUG] Visualization completed");
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