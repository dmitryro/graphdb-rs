// lib/src/query_parser/parser_zmq.rs

use anyhow::{anyhow, Result, Context}; // Added Context
use tokio::task;
use log::{error, info, debug, warn};
use std::path::PathBuf;
use tokio::time::{timeout, sleep, Duration as TokioDuration};
use serde_json::{json, Value}; 
// --- Assuming a suitable ZMQ library is available and linked (e.g., 'zmq' or 'rust-zmq') ---

// --- CORRECT IMPORTS ---
use crate::config::{StorageConfig, StorageEngineType};
use crate::daemon::{ daemon_registry::GLOBAL_DAEMON_REGISTRY, check_pid_validity_sync };
use crate::storage_engine::{StorageEngineManager, GLOBAL_STORAGE_ENGINE_MANAGER};
// ---------------------------

const DEFAULT_STORAGE_CONFIG_PATH_RELATIVE: &str = "./storage_daemon_server/storage_config.yaml";

// --- ZMQ Client Implementation (Synchronous part) ---
// This function runs on the dedicated blocking thread pool.
fn _do_cleanup_request_sync(addr: &str, request_data: &[u8], timeout_ms: i32) -> Result<Value> {
    // NOTE: This assumes the 'zmq' crate is imported/available in the project scope.
    let context = zmq::Context::new();
    // Use REQ/REP pattern for client-server communication
    let socket = context.socket(zmq::REQ).context("Failed to create ZMQ socket")?;

    // CRITICAL FIX: Set timeouts on the synchronous socket operations
    // This prevents the socket operations from hanging indefinitely.
    socket.set_rcvtimeo(timeout_ms).context("Failed to set ZMQ RCVTIMEO")?;
    socket.set_sndtimeo(timeout_ms).context("Failed to set ZMQ SNDTIMEO")?;

    socket.connect(addr).context(format!("Failed to connect to ZMQ endpoint: {}", addr))?;

    // Send request
    match socket.send(request_data, 0) {
        Ok(_) => debug!("ZMQ request sent successfully."),
        Err(zmq::Error::EAGAIN) => return Err(anyhow!("ZMQ Send Timeout.")),
        Err(e) => return Err(anyhow!("ZMQ Send Error: {}", e)),
    }

    // Receive response
    let mut msg = zmq::Message::new();
    match socket.recv(&mut msg, 0) {
        Ok(_) => {
            debug!("ZMQ response received successfully.");
        },
        Err(zmq::Error::EAGAIN) => return Err(anyhow!("ZMQ Receive Timeout: The daemon did not respond within the set timeout.")),
        Err(e) => return Err(anyhow!("ZMQ Receive Error: {}", e)),
    }
    
    // Deserialize response
    serde_json::from_slice(&msg)
        .context("Failed to deserialize ZMQ response")
}
// -------------------------------------------------

/// Ensures the storage daemon is running and initializes the GLOBAL_STORAGE_ENGINE_MANAGER
/// if it hasn't been already.
async fn initialize_storage_for_cleanup() -> Result<()> {
    // 1. Determine config path
    let config_path = PathBuf::from(DEFAULT_STORAGE_CONFIG_PATH_RELATIVE);
    
    // 2. Load config using the correct StorageConfig::load method
    let storage_config = if config_path.exists() {
        match StorageConfig::load(&config_path).await {
            Ok(config) => config,
            Err(e) => {
                warn!("Failed to load storage config from {:?}: {}. Using default.", config_path, e);
                StorageConfig::default() 
            }
        }
    } else {
        warn!("Storage configuration file not found at {:?}. Using default.", config_path);
        StorageConfig::default() 
    };

    info!("Ensuring storage daemon is running to initialize manager...");

    // 3. Use the port directly, as it is a plain u16.
    let daemon_port = storage_config.default_port;

    // Call the function with only the required port: u16 argument.
    StorageEngineManager::ensure_storage_daemon_running(daemon_port)
        .await
        .map_err(|e| anyhow!("Failed to ensure storage daemon is running for cleanup: {}", e))?;

    info!("Storage daemon check complete. GLOBAL_STORAGE_ENGINE_MANAGER should be initialized.");
    Ok(())
}

/// Executes the ZMQ request to clean up orphaned edges on the specified daemon.
pub async fn cleanup_orphaned_edges_zmq(engine_type: StorageEngineType) -> Result<()> {
    // --- Configuration Constants ---
    const CONNECT_TIMEOUT_SECS: u64 = 3;
    const REQUEST_TIMEOUT_SECS: u64 = 10;
    const RECEIVE_TIMEOUT_SECS: u64 = 15;
    const MAX_RETRIES: u32 = 3;
    const BASE_RETRY_DELAY_MS: u64 = 500;
    
    // Calculate total timeout for the async wrapper (u64 for Tokio Duration)
    const TOTAL_TIMEOUT_SECS: u64 = CONNECT_TIMEOUT_SECS + REQUEST_TIMEOUT_SECS + RECEIVE_TIMEOUT_SECS;
    // Calculate timeout for the synchronous ZMQ socket (i32 for ZMQ options)
    const SOCKET_TIMEOUT_MS: i32 = (TOTAL_TIMEOUT_SECS * 1000) as i32;

    let engine_name = engine_type.to_string();
    info!("Starting ZMQ cleanup_orphaned_edges operation for engine: {}", engine_name);

    // -----------------------------------------------------------------
    // 1. Find a daemon with verified IPC socket
    // -----------------------------------------------------------------
    let registry = GLOBAL_DAEMON_REGISTRY.get().await;
    let engine_type_lower = engine_type.to_string().to_lowercase();
    
    let mut daemons = registry
        .get_all_daemon_metadata().await.map_err(|e| anyhow!("Failed to retrieve daemon metadata: {}", e))?
        .into_iter()
        .filter(|m| m.service_type == "storage" 
             && m.engine_type.as_deref().map(|e| e.to_lowercase()) == Some(engine_type_lower.clone()) 
             && m.pid > 0 && check_pid_validity_sync(m.pid))
        .collect::<Vec<_>>();
    
    if daemons.is_empty() {
        return Err(anyhow!("No running {} daemon found for graph cleanup.", engine_name));
    }
    daemons.sort_by_key(|m| std::cmp::Reverse(m.port));
    
    let daemon = daemons.into_iter().next().ok_or_else(|| {
        anyhow!("Could not select a ready {} daemon for graph cleanup.", engine_name)
    })?; 
    
    info!("Selected {} daemon on port: {}", engine_name, daemon.port);
    
    let socket_path = format!("/tmp/graphdb-{}.ipc", daemon.port);
    let addr = format!("ipc://{}", socket_path);
    
    if tokio::fs::metadata(&socket_path).await.is_err() {
        return Err(anyhow!("IPC socket file {} does not exist. Daemon port: {}.", socket_path, daemon.port));
    }

    // -----------------------------------------------------------------
    // 2. Build the request (Command: cleanup_orphaned_edges)
    // -----------------------------------------------------------------
    // Include the total timeout in the request payload so the daemon knows its time limit.
    let request = json!({ 
        "command": "cleanup_orphaned_edges",
        "timeout_ms": SOCKET_TIMEOUT_MS
    });
    
    let request_data = serde_json::to_vec(&request)
        .map_err(|e| anyhow!("Failed to serialize request: {}", e))?;

    // -----------------------------------------------------------------
    // 3. Send the request with retries
    // -----------------------------------------------------------------
    let mut last_error: Option<anyhow::Error> = None;
    
    for attempt in 1..=MAX_RETRIES {
        debug!("Attempt {}/{} to send ZMQ cleanup_orphaned_edges request", attempt, MAX_RETRIES);
        
        let addr_clone = addr.clone();
        let request_data_clone = request_data.clone();
        
        let response_result = timeout(
            // Use the total timeout for the async wrapper
            TokioDuration::from_secs(TOTAL_TIMEOUT_SECS),
            // Call the new synchronous helper within spawn_blocking
            tokio::task::spawn_blocking(move || {
                _do_cleanup_request_sync(&addr_clone, &request_data_clone, SOCKET_TIMEOUT_MS)
            })
        )
        .await;
        
        match response_result {
            // Outer Ok: Tokio timeout did not fire. Inner Ok: spawn_blocking succeeded.
            Ok(Ok(sync_result)) => {
                // sync_result is Result<Value, anyhow::Error> from the synchronous call
                match sync_result {
                    Ok(response_value) => {
                        // response_value is now a plain serde_json::Value (FIX APPLIED HERE)
                        match response_value.get("status").and_then(|s| s.as_str()) {
                            Some("success") => {
                                let deleted_edges = response_value.get("deleted_edges").and_then(|v| v.as_u64()).unwrap_or(0);
                                let deleted_orphans = response_value.get("deleted_orphans").and_then(|v| v.as_u64()).unwrap_or(0);
                                info!("Graph Cleanup successful for {}: {} edges deleted ({} orphaned).", engine_name, deleted_edges, deleted_orphans);
                                return Ok(());
                            }
                            Some("error") => {
                                let msg = response_value.get("message").and_then(|m| m.as_str()).unwrap_or("Unknown error");
                                error!("Daemon error during graph cleanup: {}", msg);
                                last_error = Some(anyhow!("Daemon error: {}", msg));
                            }
                            _ => {
                                error!("Invalid response: {:?}", response_value);
                                last_error = Some(anyhow!("Invalid response from {}: {:?}", addr, response_value));
                            }
                        }
                    }
                    Err(e) => {
                        // Synchronous ZMQ client error (e.g., connection, send, receive timeout)
                        last_error = Some(e);
                        warn!("Attempt {}/{} failed: {}", attempt, MAX_RETRIES, last_error.as_ref().unwrap());
                    }
                }
            }
            // Inner Err: tokio::task::JoinError (e.g., panic in blocking task)
            Ok(Err(e)) => {
                last_error = Some(e.into());
                warn!("Attempt {}/{} failed: {}", attempt, MAX_RETRIES, last_error.as_ref().unwrap());
            }
            // Outer Err: tokio::time::error::Elapsed (Tokio Timeout)
            Err(_) => {
                last_error = Some(anyhow!("ZMQ operation timed out after {} seconds (Tokio Timeout)", TOTAL_TIMEOUT_SECS));
                warn!("Attempt {}/{} timed out", attempt, MAX_RETRIES);
            }
        }
        
        if attempt < MAX_RETRIES {
            let delay = BASE_RETRY_DELAY_MS * 2u64.pow(attempt - 1);
            sleep(TokioDuration::from_millis(delay)).await;
        }
    }
    
    Err(last_error.unwrap_or_else(|| {
        anyhow!("Failed to complete ZMQ cleanup_orphaned_edges operation after {} attempts", MAX_RETRIES)
    }))
}


/// Triggers the automatic cleanup of orphaned graph edges via an asynchronous ZMQ call.
pub fn trigger_async_graph_cleanup() {
    // Spawn a detached task for cleanup, which includes initialization.
    task::spawn(async move {
        info!("Starting asynchronous graph cleanup process...");

        // 1. Initialize storage/ensure daemon is running.
        if let Err(e) = initialize_storage_for_cleanup().await {
            error!("Skipping automatic graph cleanup: Storage initialization failed: {}", e);
            return;
        }

        // 2. Attempt to get the StorageEngineManager instance (now it should be initialized).
        let manager = match GLOBAL_STORAGE_ENGINE_MANAGER.get() {
            Some(m) => m,
            None => {
                error!("Skipping automatic graph cleanup: StorageEngineManager not initialized after daemon check.");
                return;
            }
        };

        // 3. Get the current engine type.
        let engine_type = manager.current_engine_type().await;
        
        info!("Starting ZMQ graph cleanup for engine: {:?}", engine_type);
        
        // 4. Execute the ZMQ cleanup.
        match cleanup_orphaned_edges_zmq(engine_type).await {
            Ok(_) => {
                debug!("Asynchronous ZMQ graph cleanup task finished successfully.");
            }
            Err(e) => {
                // Log a warning/error since this is a background maintenance task
                error!("Asynchronous ZMQ graph cleanup task failed: {}", e);
            }
        }
    });
}