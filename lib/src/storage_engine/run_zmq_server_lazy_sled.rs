    async fn run_zmq_server_lazy(
        port: u16,
        _config: SledConfig,
        running: Arc<TokioMutex<bool>>,
        zmq_socket: Arc<TokioMutex<ZmqSocket>>,
        endpoint: String,
        db: Arc<sled::Db>,
        kv_pairs: Arc<Tree>,
        vertices: Arc<Tree>,
        edges: Arc<Tree>,
        wal_manager: Arc<SledWalManager>,
        db_path: PathBuf,
        indexing_service: Arc<TokioMutex<IndexingService>>, 
    ) -> GraphResult<()> {
        info!("===> STARTING ZMQ SERVER FOR PORT {} (DB ALREADY OPEN)", port);
        println!("===> STARTING ZMQ SERVER FOR PORT {} (DB ALREADY OPEN)", port);

        // Configure socket once
        {
            let mut socket = zmq_socket.lock().await;
            socket.set_linger(0)?;
            socket.set_rcvtimeo(100)?; // Short – we poll + DONTWAIT
            socket.set_sndtimeo(1000)?;
            socket.set_maxmsgsize(MAX_MESSAGE_SIZE as i64)?;
            socket.set_sndhwm(10000)?;
            socket.set_rcvhwm(10000)?;
            socket.set_immediate(true)?;
        }
        info!("ZeroMQ server configured for port {}", port);
        println!("===> ZEROMQ SERVER CONFIGURED FOR PORT {}", port);

        let mut consecutive_errors = 0;
        let poll_timeout_ms = 10;
        let raw_fd: RawFd = {
            let s = zmq_socket.lock().await;
            (&*s).as_raw_fd()
        };
        let mut poll_items = [PollItem::from_fd(raw_fd, zmq::POLLIN)];

        while *running.lock().await {
            let mut s = zmq_socket.lock().await;
            
            // Blocking receive with 100ms timeout (set via rcvtimeo)
            let msg = match s.recv_bytes(0) {
                Ok(bytes) => bytes,
                Err(zmq::Error::EAGAIN) => {
                    drop(s);
                    tokio::task::yield_now().await;
                    continue;
                }
                Err(e) => {
                    error!("ZMQ recv error: {}", e);
                    drop(s);
                    tokio::time::sleep(TokioDuration::from_millis(100)).await;
                    continue;
                }
            };
            
            drop(s); // Release lock before processing
            
            if msg.is_empty() {
                continue;
            }
            
            // Parse and process message...
            let request: Value = match serde_json::from_slice(&msg) {
                Ok(r) => r,
                Err(e) => {
                    // ... error handling
                    continue;
                }
            };

            let request_id = request.get("request_id")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
                .unwrap_or_else(|| Uuid::new_v4().to_string());

            {
                let mut processed = PROCESSED_REQUESTS.lock().await;
                if processed.contains(&request_id) {
                    debug!("Duplicate request_id {} on port {} – skipping", request_id, port);
                    let resp = json!({"status": "success", "note": "idempotent", "request_id": request_id});
                    let _ = Self::send_zmq_response_static(&*zmq_socket.lock().await, &resp, port).await;
                    continue;
                }
                processed.insert(request_id.clone());
            }

            let command = request.get("command").and_then(|c| c.as_str());

            let response = if let Some(cypher) = request.get("query").and_then(|q| q.as_str()) {
                info!("Executing Cypher query via ZMQ: {}", cypher);
                // Create SledStorage and pass it directly to execute_cypher_from_string
                let storage = Arc::new(SledStorage::new_with_db(
                    &_config,
                    &StorageConfig::default(),
                    db.clone(),
                ).await.map_err(|e| {
                    error!("Failed to create SledStorage for Cypher execution: {}", e);
                    GraphError::StorageError(e.to_string())
                })?);
                
                match crate::query_parser::cypher_parser::execute_cypher_from_string(cypher, storage).await {
                    Ok(result) => json!({"status": "success", "data": result, "request_id": request_id}),
                    Err(e) => json!({"status": "error", "message": e.to_string(), "request_id": request_id}),
                }
            } else {
                match command {
                    Some("initialize") => json!({
                        "status": "success",
                        "message": "ZMQ server is bound and DB is open.",
                        "port": port,
                        "ipc_path": &endpoint,
                        "request_id": request_id
                    }),
                    Some("status") => json!({"status":"success","port":port,"db_open":true,"request_id": request_id}),
                    Some("ping") => json!({
                        "status": "pong",
                        "message": "ZMQ server is bound and DB is open.",
                        "port": port,
                        "ipc_path": &endpoint,
                        "db_open":true,
                        "request_id": request_id
                    }),
                    Some("force_unlock") => match Self::force_unlock_static(&db_path).await {
                        Ok(_) => json!({"status":"success","request_id": request_id}),
                        Err(e) => json!({"status":"error","message":e.to_string(),"request_id": request_id}),
                    },
                    // ---------- indexing commands ----------
                    Some(idx_cmd @ ("index_create" | "index_init" | "index_drop" | "index_create_fulltext" | "index_drop_fulltext" | "index_list" | "index_search" | "index_rebuild" | "index_stats")) => {
    
                        let mut guard = indexing_service.lock().await;  // TokioMutex
                        let result: anyhow::Result<Value> = match idx_cmd {
                            "index_init" => {
                                // The act of retrieving the singleton (indexing_service()) and 
                                // acquiring the lock ensures that the IndexingService is initialized
                                // (via init_indexing_service) before this point. 
                                // We just return a success message.
                                Ok(json!({"message": "IndexingService initialization confirmed."}))
                            }
                            "index_create" => {
                                let label   = request["params"]["label"].as_str().ok_or_else(|| anyhow!("missing label"))?;
                                let prop    = request["params"]["property"].as_str().ok_or_else(|| anyhow!("missing property"))?;
                                guard.create_index(label, prop).await.context("create_index")
                            }
                            "index_drop" => {
                                let label   = request["params"]["label"].as_str().ok_or_else(|| anyhow!("missing label"))?;
                                let prop    = request["params"]["property"].as_str().ok_or_else(|| anyhow!("missing property"))?;
                                guard.drop_index(label, prop).await.context("drop_index")
                            }
                            "index_create_fulltext" => {
                                let name    = request["params"]["name"].as_str().ok_or_else(|| anyhow!("missing name"))?;
                                let labels  = request["params"]["labels"].as_array().ok_or_else(|| anyhow!("missing labels array"))?
                                    .iter().filter_map(|v| v.as_str()).collect::<Vec<_>>();
                                let props   = request["params"]["properties"].as_array().ok_or_else(|| anyhow!("missing properties array"))?
                                    .iter().filter_map(|v| v.as_str()).collect::<Vec<_>>();
                                guard.create_fulltext_index(name, &labels, &props).await.context("create_fulltext_index")
                            }
                            "index_drop_fulltext" => {
                                let name = request["params"]["name"].as_str().ok_or_else(|| anyhow!("missing name"))?;
                                guard.drop_fulltext_index(name).await.context("drop_fulltext_index")
                            }
                            "index_list" => guard.list_indexes().await.context("list_indexes"),
                            "index_search" => {
                                let query = request["params"]["query"]
                                    .as_str()
                                    .ok_or_else(|| anyhow!("missing query"))?;
                                let limit = request["params"]["limit"]
                                    .as_u64()
                                    .unwrap_or(10) as usize;

                                // IndexingService::fulltext_search returns Result<Value>
                                // Just call it and forward the Value directly
                                let response: Value = guard
                                    .fulltext_search(query, limit)
                                    .await
                                    .context("fulltext_search failed")?;

                                Ok(response)
                            }
                            // In your ZMQ server handler (document 10):
                            "index_rebuild" => {
                                info!("Executing full-index rebuild");

                                // 1.  Get a *snapshot* of every vertex
                                let all_vertices: anyhow::Result<Vec<Vertex>> = (|| {
                                    let mut vec = Vec::new();
                                    for item in vertices.iter() {
                                        let (_, value) = item.context("iter vertex")?;
                                        vec.push(deserialize_vertex(&value)?);
                                    }
                                    Ok(vec)
                                })();

                                // 2.  Rebuild
                                let reply = match all_vertices {
                                    Ok(vv) => guard
                                        .rebuild_indexes_with_data(vv)
                                        .await
                                        .context("rebuild_indexes_with_data"),
                                    Err(e) => Err(e),
                                };

                                // 3.  Convert to JSON value
                                reply.map(|s| json!({ "message": s }))
                            }
                            "index_stats" => guard.index_stats().await.context("index_stats"),
                            _ => unreachable!(),
                        };
                        match result {
                            Ok(v) => json!({"status":"success","result":v,"request_id":request_id}),
                            Err(e) => json!({"status":"error","message":format!("Indexing error: {}", e),"request_id":request_id}),
                        }
                    }                    // === MUTATING COMMANDS (WAL + LEADER) — YOUR ORIGINAL LOGIC 100% PRESERVED ===
                    Some("delete_edges_touching_vertices") => {
                        // Parse requested vertex IDs (for DETACH)
                        let target_vertex_ids: HashSet<Uuid> = match request.get("vertex_ids") {
                            Some(value) => {
                                let ids: Vec<String> = serde_json::from_value(value.clone())
                                    .map_err(|e| GraphError::StorageError(format!("Invalid vertex_ids format: {}", e)))?;
                                ids.iter()
                                    .map(|s| Uuid::parse_str(s).map_err(|_| GraphError::StorageError("Invalid UUID in vertex_ids".into())))
                                    .collect::<GraphResult<_>>()?
                            }
                            None => HashSet::new(),
                        };

                        let vertices_tree = db.open_tree("vertices")
                            .map_err(|e| GraphError::StorageError(format!("Failed to open vertices tree: {}", e)))?;

                        let edges_tree = db.open_tree("edges")
                            .map_err(|e| GraphError::StorageError(format!("Failed to open edges tree: {}", e)))?;

                        // Load all existing vertex IDs once — O(V) but required for correctness
                        let mut existing_vertex_ids = HashSet::new();
                        for item in vertices_tree.iter() {
                            let (key, _) = item
                                .map_err(|e| GraphError::StorageError(format!("Vertex iteration failed: {}", e)))?;
                            if key.len() == 16 {
                                let uuid = Uuid::from_slice(&key)
                                    .map_err(|_| GraphError::StorageError("Corrupted vertex key".into()))?;
                                existing_vertex_ids.insert(uuid);
                            }
                        }

                        let mut keys_to_remove = Vec::new();
                        let mut deleted_by_detach = 0;
                        let mut deleted_orphans = 0;

                        // Scan all edges — delete both targeted and orphaned
                        for item in edges_tree.iter() {
                            let (key, value) = item
                                .map_err(|e| GraphError::StorageError(format!("Edge iteration failed: {}", e)))?;

                            let edge: Edge = deserialize_edge(&value)
                                .map_err(|e| GraphError::StorageError(format!("Failed to deserialize edge: {}", e)))?;

                            let out_exists = existing_vertex_ids.contains(&edge.outbound_id.0);
                            let in_exists = existing_vertex_ids.contains(&edge.inbound_id.0);
                            let is_orphan = !out_exists || !in_exists;
                            let is_targeted = target_vertex_ids.contains(&edge.outbound_id.0) 
                                           || target_vertex_ids.contains(&edge.inbound_id.0);

                            if is_orphan || is_targeted {
                                keys_to_remove.push(key);

                                if is_targeted {
                                    deleted_by_detach += 1;
                                }
                                if is_orphan {
                                    deleted_orphans += 1;
                                }
                            }
                        }

                        // Remove all doomed edges in one go
                        for key in keys_to_remove {
                            edges_tree.remove(&key)
                                .map_err(|e| GraphError::StorageError(format!("Failed to remove edge: {}", e)))?;
                        }

                        // Ensure durability
                        db.flush_async()
                            .await
                            .map_err(|e| GraphError::StorageError(format!("Flush failed after edge cleanup: {}", e)))?;

                        let total_deleted = deleted_by_detach + deleted_orphans;

                        info!(
                            "Sled DETACH+ORPHAN cleanup: {} edges deleted ({} by DETACH, {} orphaned)",
                            total_deleted, deleted_by_detach, deleted_orphans
                         );

                        json!({
                            "status": "success",
                            "deleted_edges": total_deleted,
                            "deleted_by_detach": deleted_by_detach,
                            "deleted_orphans": deleted_orphans,
                            "message": "DETACH DELETE + orphan cleanup completed"
                        })
                    }
                    Some("cleanup_orphaned_edges") | Some("cleanup_storage") | Some("cleanup_storage_force") => {
                        // This operation has no input parameters, so we use an empty set
                        let target_vertex_ids: HashSet<Uuid> = HashSet::new();
                        
                        let vertices_tree = db.open_tree("vertices")
                            .map_err(|e| GraphError::StorageError(format!("Failed to open vertices tree: {}", e)))?;

                        let edges_tree = db.open_tree("edges")
                            .map_err(|e| GraphError::StorageError(format!("Failed to open edges tree: {}", e)))?;

                        // Load all existing vertex IDs once — O(V) but required for correctness
                        let mut existing_vertex_ids = HashSet::new();
                        for item in vertices_tree.iter() {
                            let (key, _) = item
                                .map_err(|e| GraphError::StorageError(format!("Vertex iteration failed: {}", e)))?;
                            if key.len() == 16 {
                                let uuid = Uuid::from_slice(&key)
                                    .map_err(|_| GraphError::StorageError("Corrupted vertex key".into()))?;
                                existing_vertex_ids.insert(uuid);
                            }
                        }

                        let mut keys_to_remove = Vec::new();
                        let mut deleted_by_detach = 0; // Will be 0 since target_vertex_ids is empty
                        let mut deleted_orphans = 0;

                        // Scan all edges — only orphaned edges will be deleted here
                        for item in edges_tree.iter() {
                            let (key, value) = item
                                .map_err(|e| GraphError::StorageError(format!("Edge iteration failed: {}", e)))?;

                            let edge: Edge = deserialize_edge(&value)
                                .map_err(|e| GraphError::StorageError(format!("Failed to deserialize edge: {}", e)))?;

                            let out_exists = existing_vertex_ids.contains(&edge.outbound_id.0);
                            let in_exists = existing_vertex_ids.contains(&edge.inbound_id.0);
                            let is_orphan = !out_exists || !in_exists;
                            
                            // is_targeted will be false since target_vertex_ids is empty
                            // let is_targeted = target_vertex_ids.contains(&edge.outbound_id.0) 
                            //                 || target_vertex_ids.contains(&edge.inbound_id.0);

                            if is_orphan /* || is_targeted */ { 
                                keys_to_remove.push(key);

                                // if is_targeted { deleted_by_detach += 1; } // Skipped: is_targeted is always false
                                if is_orphan {
                                    deleted_orphans += 1;
                                }
                            }
                        }

                        // Remove all doomed edges
                        for key in keys_to_remove {
                            edges_tree.remove(&key)
                                .map_err(|e| GraphError::StorageError(format!("Failed to remove edge: {}", e)))?;
                        }

                        // Ensure durability
                        db.flush_async()
                            .await
                            .map_err(|e| GraphError::StorageError(format!("Flush failed after orphan cleanup: {}", e)))?;

                        let total_deleted = deleted_orphans;

                        info!(
                            "Sled ORPHAN cleanup: {} edges deleted ({} orphaned)",
                            total_deleted, deleted_orphans
                        );

                        json!({
                            "status": "success",
                            "deleted_edges": total_deleted,
                            "deleted_orphans": deleted_orphans,
                            "message": "Orphan cleanup completed"
                        })
                    }
                    Some(cmd) if [
                        "set_key", "delete_key",
                        "create_vertex", "update_vertex", "delete_vertex",
                        "create_edge", "update_edge", "delete_edge",
                        "flush"
                    ].contains(&cmd) => {
                        let canonical = db_path.parent().unwrap().to_path_buf();
                        let is_leader = match become_wal_leader(&canonical, port).await {
                            Ok(l) => l,
                            Err(e) => {
                                let resp = json!({"status":"error","message":format!("Leader election failed: {}", e),"request_id": request_id});
                                let _ = Self::send_zmq_response_static(&*zmq_socket.lock().await, &resp, port).await;
                                continue;
                            }
                        };
                        println!("====> IN run_zmq_server_lazy - command {:?}, is_leader: {}", cmd, is_leader);
                        let response = if is_leader {
                            let op = match cmd {
                                "set_key" => {
                                    let tree = request["cf"].as_str().unwrap_or("kv_pairs").to_string();
                                    let key = request["key"].as_str().ok_or_else(|| GraphError::StorageError("missing key".into()))?.as_bytes().to_vec();
                                    let value = request["value"].as_str().ok_or_else(|| GraphError::StorageError("missing value".into()))?.as_bytes().to_vec();
                                    SledWalOperation::Put { tree, key, value }
                                }
                                "delete_key" => {
                                    let tree = request["cf"].as_str().unwrap_or("kv_pairs").to_string();
                                    let key = request["key"].as_str().ok_or_else(|| GraphError::StorageError("missing key".into()))?.as_bytes().to_vec();
                                    SledWalOperation::Delete { tree, key }
                                }
                                "create_vertex" | "update_vertex" => {
                                    let vertex: Vertex = serde_json::from_value(request["vertex"].clone())
                                        .map_err(|e| GraphError::StorageError(format!("Invalid vertex: {}", e)))?;
                                    let key = vertex.id.as_bytes().to_vec();
                                    let value = serialize_vertex(&vertex)?;
                                    SledWalOperation::Put { tree: "vertices".to_string(), key, value }
                                }
                                "delete_vertex" => {
                                    let id: Identifier = serde_json::from_value(request["id"].clone())
                                        .map_err(|e| GraphError::StorageError(format!("Invalid id: {}", e)))?;
                                    let uuid = SerializableUuid::from_str(id.as_ref())
                                        .map_err(|_| GraphError::StorageError("Invalid UUID in vertex".into()))?;
                                    SledWalOperation::Delete { tree: "vertices".to_string(), key: uuid.as_bytes().to_vec() }
                                }
                                "create_edge" | "update_edge" => {
                                    let edge: Edge = serde_json::from_value(request["edge"].clone())
                                        .map_err(|e| GraphError::StorageError(format!("Invalid edge: {}", e)))?;
                                    let key = create_edge_key(&edge.outbound_id, &edge.edge_type, &edge.inbound_id)?;
                                    let value = serialize_edge(&edge)?;
                                    SledWalOperation::Put { tree: "edges".to_string(), key, value }
                                }
                                "delete_edge" => {
                                    let from: Identifier = serde_json::from_value(request["from"].clone())
                                        .map_err(|e| GraphError::StorageError(format!("Invalid from: {}", e)))?;
                                    let to: Identifier = serde_json::from_value(request["to"].clone())
                                        .map_err(|e| GraphError::StorageError(format!("Invalid to: {}", e)))?;
                                    let t: Identifier = serde_json::from_value(request["type"].clone())
                                        .map_err(|e| GraphError::StorageError(format!("Invalid type: {}", e)))?;
                                    let from_uuid = SerializableUuid::from_str(from.as_ref())
                                        .map_err(|_| GraphError::StorageError("Invalid UUID from".into()))?;
                                    let to_uuid = SerializableUuid::from_str(to.as_ref())
                                        .map_err(|_| GraphError::StorageError("Invalid UUID to".into()))?;
                                    let key = create_edge_key(&from_uuid, &t, &to_uuid)?;
                                    SledWalOperation::Delete { tree: "edges".to_string(), key }
                                }
                                "flush" => {
                                    let tree = request["cf"].as_str().unwrap_or("default").to_string();
                                    SledWalOperation::Flush { tree }
                                }
                                _ => unreachable!(),
                            };
                            println!("====> LEADER: About to append to WAL");
                            let lsn = match wal_manager.append(&op).await {
                                Ok(lsn) => {
                                    println!("====> LEADER: WAL append successful, lsn: {}", lsn);
                                    lsn
                                }
                                Err(e) => {
                                    println!("====> LEADER: WAL append failed: {}", e);
                                    let resp = json!({"status":"error","message":e.to_string(),"request_id": request_id});
                                    let _ = Self::send_zmq_response_static(&*zmq_socket.lock().await, &resp, port).await;
                                    continue;
                                }
                            };
                            println!("====> LEADER: About to apply operation locally");
                            if let Err(e) = SledDaemon::apply_op_locally(&db, &op).await {
                                println!("====> LEADER: Apply operation failed: {}", e);
                                let resp = json!({"status":"error","message":e.to_string(),"request_id": request_id});
                                let _ = Self::send_zmq_response_static(&*zmq_socket.lock().await, &resp, port).await;
                                continue;
                            }
                            println!("====> LEADER: Operation applied successfully");
                            let offset_key = format!("__wal_offset_port_{}", port);
                            if let Err(e) = db.insert(offset_key.as_bytes(), lsn.to_string().as_bytes()) {
                                warn!("Failed to update WAL offset for port {}: {}", port, e);
                            }
                            tokio::spawn({
                                let db_clone = db.clone();
                                async move { let _ = db_clone.flush_async().await; }
                            });
                            println!("====> LEADER: Returning success response");
                            json!({"status":"success", "offset": lsn, "leader": true, "request_id": request_id})
                        } else {
                            // === FOLLOWER: FORWARD TO LEADER — YOUR ORIGINAL LOGIC 100% PRESERVED ===
                            println!("====> FOLLOWER: Attempting to get leader port");
                            let leader_port = match get_leader_port(&canonical).await {
                                Ok(Some(p)) => {
                                    println!("====> FOLLOWER: Leader port is {}", p);
                                    p
                                }
                                Ok(None) => {
                                    println!("====> FOLLOWER: No leader available");
                                    let resp = json!({"status":"error","message":"No leader available","request_id": request_id});
                                    let _ = Self::send_zmq_response_static(&*zmq_socket.lock().await, &resp, port).await;
                                    continue;
                                }
                                Err(e) => {
                                    println!("====> FOLLOWER: Leader lookup failed: {}", e);
                                    let resp = json!({"status":"error","message":format!("Leader lookup failed: {}", e),"request_id": request_id});
                                    let _ = Self::send_zmq_response_static(&*zmq_socket.lock().await, &resp, port).await;
                                    continue;
                                }
                            };
                            if leader_port == 0 {
                                println!("====> FOLLOWER: Leader port is 0, election pending");
                                let resp = json!({"status":"error","message":"Leader election pending","request_id": request_id});
                                let _ = Self::send_zmq_response_static(&*zmq_socket.lock().await, &resp, port).await;
                                continue;
                            }
                            let leader_ipc_path = format!("/tmp/graphdb-{}.ipc", leader_port);
                            if tokio::fs::metadata(&leader_ipc_path).await.is_err() {
                                println!("====> FOLLOWER: Leader IPC socket doesn't exist at {}", leader_ipc_path);
                                error!("Leader IPC socket missing at {} - leader might not be running", leader_ipc_path);
                                let resp = json!({
                                    "status":"error",
                                    "message":format!("Leader on port {} not reachable (IPC socket missing)", leader_port),
                                    "request_id": request_id
                                });
                                let _ = Self::send_zmq_response_static(&*zmq_socket.lock().await, &resp, port).await;
                                continue;
                            }
                            println!("====> FOLLOWER: Leader IPC socket exists at {}", leader_ipc_path);
                            println!("====> FOLLOWER: Creating new ZMQ context and socket");
                            let context = ZmqContext::new();
                            let client = match context.socket(zmq::REQ) {
                                Ok(c) => {
                                    println!("====> FOLLOWER: Socket created successfully");
                                    c
                                }
                                Err(e) => {
                                    println!("====> FOLLOWER: Socket creation failed: {}", e);
                                    let resp = json!({"status":"error","message":format!("ZMQ socket error: {}", e),"request_id": request_id});
                                    let _ = Self::send_zmq_response_static(&*zmq_socket.lock().await, &resp, port).await;
                                    continue;
                                }
                            };
                            let leader_endpoint = format!("ipc:///tmp/graphdb-{}.ipc", leader_port);
                            println!("====> FOLLOWER: Connecting to leader at {}", leader_endpoint);
                            if let Err(e) = client.connect(&leader_endpoint) {
                                println!("====> FOLLOWER: Connection failed: {}", e);
                                let resp = json!({"status":"error","message":format!("Failed to connect to leader {}: {}", leader_port, e),"request_id": request_id});
                                let _ = Self::send_zmq_response_static(&*zmq_socket.lock().await, &resp, port).await;
                                continue;
                            }
                            println!("====> FOLLOWER: Connected to leader");
                            client.set_sndtimeo(2000).ok();
                            client.set_rcvtimeo(3000).ok();
                            let mut forward_req = request.clone();
                            forward_req["forwarded_from"] = json!(port);
                            forward_req["command"] = json!(cmd);
                            forward_req["request_id"] = json!(request_id);
                            let payload = match serde_json::to_vec(&forward_req) {
                                Ok(p) => p,
                                Err(e) => {
                                    println!("====> FOLLOWER: Serialization failed: {}", e);
                                    let resp = json!({"status":"error","message":format!("JSON serialize error: {}", e),"request_id": request_id});
                                    let _ = Self::send_zmq_response_static(&*zmq_socket.lock().await, &resp, port).await;
                                    continue;
                                }
                            };
                            println!("====> FOLLOWER: Sending request to leader ({} bytes)", payload.len());
                            if let Err(e) = client.send(&payload, 0) {
                                println!("====> FOLLOWER: Send to leader failed: {}", e);
                                let resp = json!({"status":"error","message":format!("ZMQ send failed: {}", e),"request_id": request_id});
                                let _ = Self::send_zmq_response_static(&*zmq_socket.lock().await, &resp, port).await;
                                continue;
                            }
                            println!("====> FOLLOWER: Request sent, waiting for leader response...");
                            let resp_msg = match client.recv_bytes(0) {
                                Ok(m) => {
                                    println!("====> FOLLOWER: Received response from leader ({} bytes)", m.len());
                                    m
                                }
                                Err(e) => {
                                    println!("====> FOLLOWER: Recv from leader failed: {}", e);
                                    let resp = json!({"status":"error","message":format!("ZMQ recv error: {}", e),"request_id": request_id});
                                    let _ = Self::send_zmq_response_static(&*zmq_socket.lock().await, &resp, port).await;
                                    continue;
                                }
                            };
                            println!("====> FOLLOWER: Parsing leader response");
                            match serde_json::from_slice(&resp_msg) {
                                Ok(resp) => resp,
                                Err(e) => {
                                    let resp = json!({"status":"error","message":format!("Leader response parse error: {}", e),"request_id": request_id});
                                    let _ = Self::send_zmq_response_static(&*zmq_socket.lock().await, &resp, port).await;
                                    continue;
                                }
                            }
                        };
                        response
                    }
                    // === READ COMMANDS — YOUR ORIGINAL LOGIC
                    Some("get_all_edges") => {
                        let mut vec = Vec::new();
                        let len = edges.len();
                        info!("Edges tree has {} entries", len);
                        println!("===> Number of entries in edges tree: {:?}", len);
                        for item in edges.iter() {
                            let (_, value) = item?;
                            match deserialize_edge(&value) {
                                Ok(e) => vec.push(e),
                                Err(e) => warn!("Failed to deserialize edge: {}", e),
                            }
                        }
                        json!({"status": "success", "edges": vec})   // <- no Ok()
                    }
                    // Other cases...
                    // === READ COMMANDS — YOUR ORIGINAL LOGIC
                    Some(cmd) if [
                        "get_key", "get_vertex", "get_edge",
                        "get_all_vertices", "get_all_edges",
                        "get_all_vertices_by_type", "get_all_edges_by_type"
                    ].contains(&cmd) => {
                        match Self::execute_db_command(cmd, &request, &db, &kv_pairs, &vertices, &edges, port, &db_path, &endpoint).await {
                            Ok(mut r) => {
                                r["request_id"] = json!(request_id);
                                r
                            }
                            Err(e) => json!({"status":"error","message":e.to_string(),"request_id": request_id}),
                        }
                    }
                    Some("clear_data") | Some("force_reset") => {
                        match Self::execute_db_command(command.unwrap(), &request, &db, &kv_pairs, &vertices, &edges, port, &db_path, &endpoint).await {
                            Ok(mut r) => {
                                r["request_id"] = json!(request_id);
                                r
                            }
                            Err(e) => json!({"status":"error","message":e.to_string(),"request_id": request_id}),
                        }
                    }
                    Some("force_ipc_init") => {
                        let ipc_path = format!("/tmp/graphdb-{}.ipc", port);
                        match tokio::fs::metadata(&ipc_path).await {
                            Ok(_) => json!({"status": "success", "message": "IPC socket already exists", "ipc_path": &ipc_path, "request_id": request_id}),
                            Err(_) => json!({"status": "error", "message": "IPC socket missing - server restart required", "ipc_path": &ipc_path, "request_id": request_id}),
                        }
                    }
                    Some(cmd) => json!({"status":"error","message":format!("Unsupported command: {}", cmd),"request_id": request_id}),
                    None => json!({"status":"error","message":"No command specified","request_id": request_id}),
                }
            };

            let s = zmq_socket.lock().await;
            if let Err(e) = Self::send_zmq_response_static(&*s, &response, port).await {
                error!("Failed to send ZMQ response on port {}: {}", port, e);
            }
        }

        info!("ZMQ server shutting down for port {}", port);
        let s = zmq_socket.lock().await;
        let _ = s.disconnect(&endpoint);
        Ok(())
    }