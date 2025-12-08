pub async fn execute_cypher(
    query: CypherQuery,
    _db: &Database,
    storage: Arc<dyn GraphStorageEngine + Send + Sync>,
) -> GraphResult<Value> {
    match query {
        // ----------  NEW  ----------
        CypherQuery::Batch(stmts) => {
             use futures::future::try_join_all;

             let tasks = stmts.into_iter().map(|stmt| {
                 let db  = _db;      // copy the references you need
                 let stg = storage.clone();
                 async move { execute_cypher(stmt, db, stg).await }
             });

             let results = try_join_all(tasks).await?;
             Ok(json!({ "results": results }))
        }
        // --- UPDATED MATCH PATTERN ---
        // Handles pure MATCH ... RETURN, relying on a full pattern matcher.
        CypherQuery::MatchPattern { patterns } => {
            info!("===> EXECUTING MatchPattern with {} patterns", patterns.len());
            
            // This helper function must implement the core graph traversal logic (e.g., BFS/DFS).
            let (final_vertices, final_edges) = exec_cypher_pattern(patterns, &storage).await?;

            Ok(json!({
                "vertices": final_vertices,
                "edges": final_edges,
                "stats": {
                    "vertices_matched": final_vertices.len(),
                    "edges_matched": final_edges.len()
                }
            }))
        }
        // --- SIGNIFICANTLY UPDATED MATCH/CREATE (MERGE-like) ---
        // 1. MATCH nodes to resolve their IDs (populates var_to_id).
        // 2. CREATE nodes whose variables are not in var_to_id (populates var_to_id and created_vertices).
        // 3. CREATE relationships between the now-resolved/created nodes.
        CypherQuery::MatchCreate { match_patterns, create_patterns } => {
            println!("===> MATCH patterns: {}", match_patterns.len());
            for (i, pat) in match_patterns.iter().enumerate() {
                println!("===>   Match pattern {}: {} nodes, {} rels", i, pat.1.len(), pat.2.len());
            }
            
            println!("===> CREATE patterns: {}", create_patterns.len());
            for (i, pat) in create_patterns.iter().enumerate() {
                println!("===>   Create pattern {}: {} nodes, {} rels", i, pat.1.len(), pat.2.len());
                for (j, node) in pat.1.iter().enumerate() {
                    println!("===>     Node {}: var={:?}, label={:?}", j, node.0, node.1);
                }
                for (j, rel) in pat.2.iter().enumerate() {
                    println!("===>     Rel {}: var={:?}, label={:?}", j, rel.0, rel.1);
                }
            }
            
            info!("===> EXECUTING MatchCreate: {} match patterns, {} create patterns", 
                   match_patterns.len(), create_patterns.len());
            
            let mut var_to_id: HashMap<String, SerializableUuid> = HashMap::new();
            let mut created_vertices = Vec::new();
            let mut created_edges = Vec::new();

            // 1. Resolve (MATCH) nodes
            println!("===> BEFORE resolve_match_patterns call");
            var_to_id.extend(
                resolve_match_patterns(&storage, match_patterns).await?
            );
            
            // DIAGNOSTIC LOGGING
            println!("===> AFTER resolve_match_patterns: var_to_id has {} entries", var_to_id.len());
            for (var, id) in &var_to_id {
                println!("===>   Bound: {} -> {}", var, id.0);
            }
            println!("===> ABOUT TO PROCESS {} CREATE PATTERNS", create_patterns.len());

            // 2. Process CREATE patterns
            for (pat_idx, pat) in create_patterns.iter().enumerate() {
                println!("===> PROCESSING CREATE PATTERN {}: {} nodes, {} rels", 
                         pat_idx, pat.1.len(), pat.2.len());
                
                // a. Create/Resolve Nodes in the CREATE pattern
                for (node_idx, (var_opt, label_opt, properties)) in pat.1.iter().enumerate() {
                    println!("===>   CREATE pattern node {}: var={:?}, label={:?}", 
                             node_idx, var_opt, label_opt);
                    
                    if let Some(v) = var_opt.as_ref() {
                        // If the variable is not bound (from MATCH), it's a NEW node to be created.
                        if !var_to_id.contains_key(v) {
                            println!("===>     Variable '{}' not bound, creating new vertex", v);
                            
                            let props: GraphResult<HashMap<String, PropertyValue>> = properties
                                .iter()
                                .map(|(k, val)| to_property_value(val.clone()).map(|pv| (k.clone(), pv)))
                                .collect();
                            
                            let final_label = label_opt.as_ref().cloned().unwrap_or_else(|| "Node".to_string());
                            let new_id = SerializableUuid(Uuid::new_v4());
                            
                            let vertex = Vertex {
                                id: new_id,
                                label: Identifier::new(final_label)?,
                                properties: props?,
                                created_at: Utc::now().into(),  
                                updated_at: Utc::now().into(),   
                            };
                            storage.create_vertex(vertex.clone()).await?;
                            var_to_id.insert(v.clone(), new_id);
                            created_vertices.push(vertex);
                            
                            println!("===>     Created new vertex with id {}", new_id.0);
                        } else {
                            println!("===>     Variable '{}' already bound to {}", v, var_to_id[v].0);
                        }
                    } else {
                        if !properties.is_empty() {
                            warn!("CREATE pattern contains a node with properties but no variable/label: {:?}", properties);
                        }
                    }
                }

                // b. Create Edges in the CREATE pattern (sequential relationships)
                println!("===>   Checking edge creation: {} nodes, {} relationships", pat.1.len(), pat.2.len());
                
                if pat.1.len().saturating_sub(1) != pat.2.len() {
                    return Err(GraphError::ValidationError(format!(
                        "Mismatched number of nodes ({}) and relationships ({}) in CREATE pattern path.", 
                        pat.1.len(), pat.2.len()
                    )));
                }

                println!("===>   About to create {} edges", pat.2.len());
                
                for (i, rel_tuple) in pat.2.iter().enumerate() {
                    println!("===>   Processing edge {}", i);
                    
                    // Get node variables from the sequence
                    let from_var_opt = pat.1.get(i).and_then(|node_pattern| node_pattern.0.as_ref());
                    let to_var_opt = pat.1.get(i + 1).and_then(|node_pattern| node_pattern.0.as_ref());

                    println!("===>     from_var_opt: {:?}, to_var_opt: {:?}", from_var_opt, to_var_opt);

                    let from_var = from_var_opt.ok_or_else(|| {
                        GraphError::ValidationError(format!(
                            "Node at index {} in CREATE pattern has no variable for edge creation", i
                        ))
                    })?;
                    let to_var = to_var_opt.ok_or_else(|| {
                        GraphError::ValidationError(format!(
                            "Node at index {} in CREATE pattern has no variable for edge creation", i + 1
                        ))
                    })?;

                    println!("===>     from_var: '{}', to_var: '{}'", from_var, to_var);

                    // Resolve IDs from the bound map (matched or newly created)
                    let from_id = *var_to_id.get(from_var).ok_or_else(|| {
                        println!("===>     ERROR: Unbound source variable '{}'", from_var);
                        println!("===>     Available variables: {:?}", var_to_id.keys().collect::<Vec<_>>());
                        GraphError::ValidationError(format!("Unbound source var for edge: {}", from_var))
                    })?;
                    
                    let to_id = *var_to_id.get(to_var).ok_or_else(|| {
                        println!("===>     ERROR: Unbound target variable '{}'", to_var);
                        println!("===>     Available variables: {:?}", var_to_id.keys().collect::<Vec<_>>());
                        GraphError::ValidationError(format!("Unbound target var for edge: {}", to_var))
                    })?;

                    println!("===>     Resolved IDs: from={}, to={}", from_id.0, to_id.0);

                    // rel_tuple is (_rel_var, label, len_range, properties, direction)
                    let (_rel_var, label_opt, _len_range, properties, direction_opt) = rel_tuple;

                    // Handle direction: Some(false) is inbound.
                    let (outbound_id, inbound_id) = match direction_opt {
                        Some(false) => (to_id, from_id), // (B)<-[R]-(A) where A is from, B is to
                        _ => (from_id, to_id),           // (A)-[R]->(B)
                    };

                    let edge_type_str = label_opt.as_ref().cloned().unwrap_or("RELATED".to_string());
                    
                    println!("===>     Creating edge: {} -[:{}]-> {}", outbound_id.0, edge_type_str, inbound_id.0);
                    
                    let props: GraphResult<BTreeMap<String, PropertyValue>> = properties.iter()
                        .map(|(k, v)| to_property_value(v.clone()).map(|pv| (k.clone(), pv)))
                        .collect();
                    
                    let edge = Edge {
                        id: SerializableUuid(Uuid::new_v4()),
                        outbound_id,
                        inbound_id,
                        edge_type: Identifier::new(edge_type_str.clone())?,
                        label: edge_type_str.clone(),
                        properties: props?,
                    };
                    
                    println!("===>     Calling storage.create_edge()");
                    storage.create_edge(edge.clone()).await?;
                    println!("===>     Edge created successfully with id {}", edge.id.0);
                    
                    created_edges.push(edge);
                }
            }

            println!("===> MatchCreate COMPLETE: {} vertices, {} edges created", 
                     created_vertices.len(), created_edges.len());

            Ok(json!({
                "status": "success",
                "created_vertices": created_vertices,
                "created_edges": created_edges,
                "stats": {
                    "vertices_created": created_vertices.len(),
                    "relationships_created": created_edges.len()
                }
            }))
        }

        CypherQuery::CreateNode { label, properties } => {
            let props: HashMap<_, _> = properties.into_iter()
                .map(|(k, v)| Ok((k, to_property_value(v)?)))
                .collect::<GraphResult<_>>()?;

            let v = Vertex {
                id: SerializableUuid(Uuid::new_v4()),
                label: Identifier::new(label)?,
                properties: props,
                created_at: Utc::now().into(),   
                updated_at: Utc::now().into(), 
            };
            storage.create_vertex(v.clone()).await?;
            Ok(json!({ "vertex": v }))
        }
        
        CypherQuery::CreateNodes { nodes } => {
            let mut created_vertices = Vec::new();
            for (label, properties) in nodes {
                let props: GraphResult<HashMap<String, PropertyValue>> = properties
                    .into_iter()
                    .map(|(k, v)| to_property_value(v).map(|pv| (k, pv)))
                    .collect();
                let vertex = Vertex {
                    id: SerializableUuid(Uuid::new_v4()),
                    label: Identifier::new(label)?,
                    properties: props?,
                    created_at: Utc::now().into(),   
                    updated_at: Utc::now().into(),  
                };
                storage.create_vertex(vertex.clone()).await?;
                created_vertices.push(vertex);
            }
            Ok(json!({ "vertices": created_vertices }))
        }
        
        CypherQuery::MatchNode { label, properties } => {
            let vertices = storage.get_all_vertices().await?;
            let query_props: GraphResult<HashMap<String, PropertyValue>> = properties
                .into_iter()
                .map(|(k, v)| to_property_value(v).map(|pv| (k, pv)))
                .collect();
            let query_props = query_props?;
            
            let filtered = vertices.into_iter().filter(|v| {
                let matches_label = if let Some(query_label) = &label {
                    let vertex_label_str = v.label.as_ref();
                    vertex_label_str == query_label ||
                        vertex_label_str.starts_with(&format!("{}:", query_label))
                } else {
                    true
                };
                
                let matches_props = if query_props.is_empty() {
                    true
                } else {
                    query_props.iter().all(|(k, expected_val)| {
                        v.properties.get(k).map_or(false, |actual_val| actual_val == expected_val)
                    })
                };
                
                matches_label && matches_props
            }).collect::<Vec<_>>();
            
            Ok(json!({ "vertices": filtered }))
        }
        
        CypherQuery::MatchMultipleNodes { nodes } => {
            let all_vertices = storage.get_all_vertices().await?;
            let mut result_vertices = Vec::new();
            let mut matched_ids = HashSet::new();

            for (_var, label, properties) in nodes {
                let props: HashMap<String, PropertyValue> = properties
                    .iter()
                    .map(|(k, v)| to_property_value(v.clone()).map(|pv| (k.clone(), pv)))
                    .collect::<GraphResult<_>>()?;
                
                for v in &all_vertices {
                    let matches_label = label.as_ref().map_or(true, |l| {
                        let vl = v.label.as_ref();
                        vl == l || vl.starts_with(&format!("{}:", l))
                    });
                    
                    let matches_props = props.iter().all(|(k, expected_val)| {
                        v.properties.get(k).map_or(false, |actual_val| actual_val == expected_val)
                    });
                    
                    if matches_label && matches_props && !matched_ids.contains(&v.id) {
                        result_vertices.push(v.clone());
                        matched_ids.insert(v.id);
                    }
                }
            }
            
            Ok(json!({ "vertices": result_vertices, "count": result_vertices.len() }))
        }
        // --- UPDATED CREATE COMPLEX PATTERN (Pure CREATE of a Path) ---
        // This is structurally similar to the CREATE part of MatchCreate, 
        // but all nodes *must* be new, so they are unconditionally created.
        CypherQuery::CreateComplexPattern { nodes, relationships } => {
            info!("===> EXECUTING CreateComplexPattern: {} nodes, {} relationships", nodes.len(), relationships.len());
            
            let mut created_vertices = Vec::new();
            // Map to link the variables in the pattern to the newly created UUIDs.
            let mut var_to_id: HashMap<String, SerializableUuid> = HashMap::new(); 
            let mut created_edges = Vec::new();

            // 1. Create all vertices and populate var_to_id map
            if nodes.is_empty() {
                return Err(GraphError::ValidationError("CREATE pattern must contain at least one node.".into()));
            }

            for (var_opt, label_opt, properties) in nodes.iter() {
                let props: GraphResult<HashMap<String, PropertyValue>> = properties
                    .iter()
                    .map(|(k, v)| to_property_value(v.clone()).map(|pv| (k.clone(), pv)))
                    .collect();
                
                let new_id = SerializableUuid(Uuid::new_v4());
                let final_label = label_opt.as_ref().cloned().unwrap_or_else(|| "Node".to_string());
                
                let vertex = Vertex {
                    id: new_id,
                    label: Identifier::new(final_label)?,
                    properties: props?,
                    created_at: Utc::now().into(),
                    updated_at: Utc::now().into(),  
                };
                storage.create_vertex(vertex.clone()).await?;
                
                if let Some(v) = var_opt.as_ref() {
                    var_to_id.insert(v.clone(), new_id);
                }
                created_vertices.push(vertex);
            }
            
            // 2. Create edges based on the relationships list and sequential nodes.
            if relationships.len() != nodes.len().saturating_sub(1) {
                return Err(GraphError::ValidationError("Mismatched number of nodes and relationships in complex CREATE pattern.".into()));
            }

            // Iterate over the relationships
            for (i, rel_tuple) in relationships.into_iter().enumerate() {
                
                // Get the variables of the connected nodes (from the nodes vector)
                // NodePattern is a 3-tuple: (var, label, properties)
                let from_var_opt = nodes[i].0.as_ref();
                let to_var_opt = nodes[i + 1].0.as_ref();

                let from_var = from_var_opt.ok_or(GraphError::ValidationError(format!("Node at index {} in CREATE pattern has no variable", i)))?;
                let to_var = to_var_opt.ok_or(GraphError::ValidationError(format!("Node at index {} in CREATE pattern has no variable", i + 1)))?;

                // Resolve IDs using the map of newly created nodes
                let from_id = *var_to_id.get(from_var).ok_or(GraphError::ValidationError(format!("Unbound from var: {}", from_var)))?;
                let to_id = *var_to_id.get(to_var).ok_or(GraphError::ValidationError(format!("Unbound to var: {}", to_var)))?;

                // Destructure the 5-tuple: (rel_var, label, len_range, properties, direction)
                let (
                    _rel_var,      
                    label_opt,     // Index 1: Relationship Label
                    _len_range,    
                    properties,    // Index 3: Properties Map
                    direction_opt, // Index 4: Direction (Option<bool>)
                ) = rel_tuple;

                // Handle direction: Some(false) is inbound.
                let (outbound_id, inbound_id) = match direction_opt {
                    Some(false) => (to_id, from_id), // (from_id)<-[R]-(to_id)
                    _ => (from_id, to_id),           // (from_id)-[R]->(to_id) or (from_id)-[R]-(to_id)
                };

                let edge_type_str = label_opt.clone().unwrap_or("RELATED".to_string());
                
                // Properties
                let props: GraphResult<BTreeMap<String, PropertyValue>> = properties.iter()
                    .map(|(k, v)| to_property_value(v.clone()).map(|pv| (k.clone(), pv)))
                    .collect();
                
                let edge = Edge {
                    id: SerializableUuid(Uuid::new_v4()),
                    outbound_id,
                    inbound_id,
                    edge_type: Identifier::new(edge_type_str.clone())?,
                    label: edge_type_str,
                    properties: props?,
                };
                
                storage.create_edge(edge.clone()).await?;
                created_edges.push(edge);
            }

            Ok(json!({
                "vertices": created_vertices,
                "edges": created_edges,
                "stats": {
                    "vertices_created": created_vertices.len(),
                    "relationships_created": created_edges.len(),
                }
            }))
        }
        // New match arm in the query execution logic (e.g., in execute_cypher_query)
        CypherQuery::CreateStatement { patterns, return_items } => {
            info!("===> EXECUTING CreateStatement: {} patterns, returning: {:?}", patterns.len(), return_items);

            let mut created_vertices = Vec::new();
            let mut created_edges = Vec::new();
            let mut var_to_id: HashMap<String, SerializableUuid> = HashMap::new();
            
            // --- Phase 1: Create all vertices and populate var_to_id map ---
            
            // Gather all unique NodePatterns from all patterns
            let mut unique_nodes = HashMap::new();
            for (_, nodes, _) in patterns.iter() {
                for node_pattern in nodes {
                    if let Some(var) = node_pattern.0.as_ref() {
                        // Use the variable as the key to ensure we only create each node once
                        unique_nodes.entry(var.clone()).or_insert(node_pattern);
                    }
                }
            }
            
            for (var, node_pattern) in unique_nodes {
                let (var_opt, label_opt, properties) = node_pattern;
                
                let props: GraphResult<HashMap<String, PropertyValue>> = properties
                    .iter()
                    .map(|(k, v)| to_property_value(v.clone()).map(|pv| (k.clone(), pv)))
                    .collect();
                
                let new_id = SerializableUuid(Uuid::new_v4());
                let final_label = label_opt.as_ref().cloned().unwrap_or_else(|| "Node".to_string());
                
                let vertex = Vertex {
                    id: new_id,
                    label: Identifier::new(final_label)?,
                    properties: props?,
                    created_at: Utc::now().into(),  
                    updated_at: Utc::now().into(),  
                };
                storage.create_vertex(vertex.clone()).await?;
                
                var_to_id.insert(var.clone(), new_id);
                created_vertices.push(vertex);
            }
            
            // --- Phase 2: Create all edges from all patterns ---
            // (This logic correctly iterates through the patterns and uses var_to_id)

            for (_, nodes, relationships) in patterns.into_iter() {
                if relationships.is_empty() {
                    continue;
                }

                // Iterate over (from_node, rel, to_node) segments
                for i in 0..relationships.len() {
                    // FIX: Borrow the relationship tuple reference instead of moving it
                    let rel_tuple = &relationships[i]; 
                    
                    // Nodes are stored sequentially in the Pattern tuple, matching rel[i] with node[i] -> node[i+1]
                    let from_var_opt = nodes[i].0.as_ref();
                    let to_var_opt = nodes[i + 1].0.as_ref();

                    let from_var = from_var_opt.ok_or(GraphError::ValidationError(format!("Relationship source node at index {} has no variable.", i)))?;
                    let to_var = to_var_opt.ok_or(GraphError::ValidationError(format!("Relationship target node at index {} has no variable.", i + 1)))?;

                    // Resolve IDs using the map of newly created nodes (var_to_id)
                    let from_id = *var_to_id.get(from_var).ok_or(GraphError::ValidationError(format!("Unbound source variable: {}", from_var)))?;
                    let to_id = *var_to_id.get(to_var).ok_or(GraphError::ValidationError(format!("Unbound target variable: {}", to_var)))?;

                    // Destructure the BORROWED 5-tuple, cloning the owned parts (Option<String>, HashMap)
                    let (
                        _rel_var,     
                        label_opt,      // Index 1: Relationship Label
                        _len_range,     
                        properties,     // Index 3: Properties Map
                        direction_opt,  // Index 4: Direction (Option<bool>)
                    ) = rel_tuple;

                    // Handle direction: Some(false) is inbound.
                    let (outbound_id, inbound_id) = match direction_opt {
                        Some(false) => (to_id, from_id), // (from_id)<-[R]-(to_id)
                        _ => (from_id, to_id),           // (from_id)-[R]->(to_id) or (from_id)-[R]-(to_id)
                    };

                    // FIX: Clone the label_opt and properties for ownership
                    let edge_type_str = label_opt.clone().unwrap_or("RELATED".to_string());
                    
                    // Properties
                    let props: GraphResult<BTreeMap<String, PropertyValue>> = properties.iter()
                        .map(|(k, v)| to_property_value(v.clone()).map(|pv| (k.clone(), pv)))
                        .collect();
                    
                    let edge = Edge {
                        id: SerializableUuid(Uuid::new_v4()),
                        outbound_id,
                        inbound_id,
                        edge_type: Identifier::new(edge_type_str.clone())?,
                        label: edge_type_str,
                        properties: props?,
                    };
                    
                    storage.create_edge(edge.clone()).await?;
                    created_edges.push(edge);
                }
            }
            
            // --- Phase 3: Shape the result based on return_items ---
            let all_return_variables: HashSet<String> = return_items.into_iter().collect();
            let created_vertex_map: HashMap<SerializableUuid, &Vertex> = created_vertices.iter().map(|v| (v.id, v)).collect();

            // The final result should be a single JSON object containing the returned variables as keys
            let returned_data: HashMap<String, Value> = var_to_id.iter()
                .filter(|(var, _)| all_return_variables.contains(*var))
                .filter_map(|(var, id)| {
                    created_vertex_map.get(id)
                        .and_then(|v| serde_json::to_value(v).ok())
                        .map(|v_json| (var.clone(), v_json))
                })
                .collect();

            Ok(serde_json::to_value(returned_data)?)
        }
        CypherQuery::CreateEdge { from_id, edge_type, to_id } => {
            let edge = Edge {
                id: SerializableUuid(Uuid::new_v4()),
                outbound_id: from_id,
                edge_type: Identifier::new(edge_type.clone())?,
                inbound_id: to_id,
                label: edge_type,
                properties: BTreeMap::new(),
            };
            storage.create_edge(edge.clone()).await?;
            Ok(json!({ "edge": edge }))
        }
        
        CypherQuery::SetNode { id, properties } => {
            let mut vertex = storage.get_vertex(&id.0).await?.ok_or_else(|| {
                GraphError::StorageError(format!("Vertex not found: {:?}", id))
            })?;
            let props: GraphResult<HashMap<String, PropertyValue>> = properties
                .into_iter()
                .map(|(k, v)| to_property_value(v).map(|pv| (k, pv)))
                .collect();
            vertex.properties.extend(props?);
            storage.update_vertex(vertex.clone()).await?;
            Ok(json!({ "vertex": vertex }))
        }
        
        CypherQuery::DeleteNode { id } => {
            // Node deletion without DETACH DELETE, which typically relies on the storage 
            // engine to check for and error on existing edges, but if it succeeds, 
            // it may leave behind orphaned edges.
            storage.delete_vertex(&id.0).await?;
            
            // 🌟 FIX: Trigger asynchronous cleanup after successful node deletion.
            trigger_async_graph_cleanup();
            
            Ok(json!({ "deleted": id }))
        }
        
        CypherQuery::SetKeyValue { key, value } => {
            let kv_key = key.clone().into_bytes();
            storage.insert(kv_key, value.as_bytes().to_vec()).await?;
            storage.flush().await?;
            Ok(json!({ "key": key, "value": value }))
        }
        
        CypherQuery::GetKeyValue { key } => {
            let kv_key = key.clone().into_bytes();
            let value = storage.retrieve(&kv_key).await?;
            Ok(json!({ "key": key, "value": value.map(|v| String::from_utf8_lossy(&v).to_string()) }))
        }
        
        CypherQuery::DeleteKeyValue { key } => {
            let kv_key = key.clone().into_bytes();
            let existed = storage.retrieve(&kv_key).await?.is_some();
            if existed {
                storage.delete(&kv_key).await?;
                storage.flush().await?;
            }
            Ok(json!({ "key": key, "deleted": existed }))
        }
        
        CypherQuery::CreateIndex { label, properties } => {
            warn!("CREATE INDEX command is not implemented yet. Index for label '{:?}' on properties {:?} ignored.", label, &properties);
            Ok(json!({ "status": "success", "message": "Index creation not implemented", "label": label, "properties": properties }))
        }
        
        CypherQuery::CreateEdgeBetweenExisting { source_var, rel_type, properties, target_var } => {
            let properties_clone = properties.clone();
            let from_id = Uuid::parse_str(&source_var)
                .map_err(|_| GraphError::ValidationError(format!("Invalid source vertex ID: {}", source_var)))?;
            let to_id = Uuid::parse_str(&target_var)
                .map_err(|_| GraphError::ValidationError(format!("Invalid target vertex ID: {}", target_var)))?;
            let props: GraphResult<BTreeMap<String, PropertyValue>> = properties
                .into_iter()
                .map(|(k, v)| to_property_value(v).map(|pv| (k, pv)))
                .collect();
            let label = properties_clone
                .get("label")
                .and_then(|v| v.as_str().map(|s| s.to_string()))
                .unwrap_or_else(|| rel_type.clone());
            let edge = Edge {
                id: SerializableUuid(Uuid::new_v4()),
                outbound_id: SerializableUuid(from_id),
                inbound_id: SerializableUuid(to_id),
                edge_type: Identifier::new(rel_type)?,
                label,
                properties: props?,
            };
            storage.create_edge(edge.clone()).await?;
            Ok(json!({ "edge": edge }))
        }
        
        CypherQuery::DeleteEdges {
            edge_variable,
            pattern,
            where_clause,
        } => {
            let graph_service = initialize_graph_service(storage.clone()).await?;

            // --- STEP 1: MATCH (READ) ---
            // ... (Matching logic remains unchanged, as provided in the original code snippet) ...
            let is_variable_length = pattern.relationships.len() == 1 
                && pattern.nodes.len() == 2 
                && pattern.relationships[0].2.map_or(false, |(min, max)| {
                    min.map_or(1, |m| m) != 1 || max.map_or(1, |m| m) != 1
                });

            let edges_to_delete: Vec<Edge> = if is_variable_length {
                // ... (variable length path matching logic) ...
                let graph = graph_service.get_graph().await; 
                
                let rel_pat = &pattern.relationships[0];
                let start_node_pat = &pattern.nodes[0];
                let end_node_pat = &pattern.nodes[1];

                let start_vertices: Vec<&Vertex> = graph.vertices.values()
                    .filter(|v| node_matches_constraints(v, &start_node_pat.1, &start_node_pat.2))
                    .collect();

                let mut all_matched_edge_ids: HashSet<Uuid> = HashSet::new();

                for start_v in start_vertices {
                    let (_, matched_e_ids) = graph.match_variable_length_path(
                        start_v.id.0, 
                        rel_pat, 
                        end_node_pat
                    );
                    all_matched_edge_ids.extend(matched_e_ids);
                }
                
                graph.edges.values()
                    .filter(|e| all_matched_edge_ids.contains(&e.id.0))
                    .cloned()
                    .collect()
                    
            } else {
                let (_, edges) = exec_cypher_pattern(
                    vec![(None, pattern.nodes.clone(), pattern.relationships.clone())],
                    &storage
                ).await?;
                edges
            };
            
            // --- STEP 2: DELETE (WRITE) ---
            let mut deleted = 0usize;

            for edge in edges_to_delete {
                let mut variables = HashMap::new();
                variables.insert(edge_variable.clone(), CypherValue::Edge(edge.clone()));

                let ctx = EvaluationContext {
                    variables,
                    parameters: HashMap::new(),
                };

                // Evaluate WHERE clause
                let should_delete = match where_clause.as_ref() {
                    Some(where_clause) => where_clause.evaluate(&ctx)?,
                    None => true,
                };

                if should_delete {
                    // 1. Delete edge from persistent storage
                    // FIX: Reverting to the existing method `delete_edge`
                    storage
                        .delete_edge(&edge.outbound_id.0, &edge.edge_type, &edge.inbound_id.0)
                        .await?;
                    
                    // 2. Remove edge from the in-memory graph copy immediately
                    // FIX: Using the existing method `delete_edge_from_memory` which takes &Edge
                    graph_service.delete_edge_from_memory(&edge).await?; 
                    
                    deleted += 1;
                }
            }

            storage.flush().await?;
            trigger_async_graph_cleanup(); 
            
            Ok(json!({
                "status": "success",
                "deleted_edges": deleted,
                "message": format!("Successfully deleted {deleted} edge(s)")
            }))
        } 
        // ----------------------------------------------------------------------------------------------------------------------
        CypherQuery::DetachDeleteNodes { node_variable: _, label } => {
            let graph_service = initialize_graph_service(storage.clone()).await?;

            // 1. Fetch all vertices matching the filter.
            let all_vertices = storage.get_all_vertices().await?;
            
            let nodes_to_delete: Vec<Vertex> = all_vertices
                .into_iter()
                .filter(|v| {
                    label.as_ref().map_or(true, |l| v.label.as_ref() == l.as_str())
                })
                .collect();
            
            if nodes_to_delete.is_empty() {
                return Ok(json!({"deleted_vertices": 0, "deleted_edges": 0}));
            }
            
            let vertex_ids: HashSet<Uuid> = nodes_to_delete.iter().map(|v| v.id.0).collect();

            // 2. Delete edges associated with these nodes from persistent storage.
            let deleted_edges = storage.delete_edges_touching_vertices(&vertex_ids).await?;
            
            // 3. Delete the vertex records and update in-memory graph.
            for vertex in nodes_to_delete.iter() {
                storage.delete_vertex(&vertex.id.0).await?;
                
                // FIX: Using the newly implemented `delete_vertex_from_memory`
                // This keeps the in-memory graph consistent after the persistent delete.
                graph_service.delete_vertex_from_memory(vertex.id.0).await?;
            }
            
            storage.flush().await?;
            trigger_async_graph_cleanup(); 
            
            Ok(json!({
                "deleted_vertices": nodes_to_delete.len(),
                "deleted_edges": deleted_edges
            }))
        }

        CypherQuery::MatchPath { path_var: _, left_node, right_node, return_clause: _ } => {
            let left_pat = parse_node_pattern(&left_node)?;
            let right_pat = parse_node_pattern(&right_node)?;

            let all_vertices = storage.get_all_vertices().await?;
            let all_edges = storage.get_all_edges().await?;

            let mut left_ids = HashSet::new();
            let mut right_ids = HashSet::new();

            let matches = |v: &Vertex, (label, props): &(Option<String>, HashMap<String, Value>)| {
                let label_ok = label.as_ref().map_or(true, |l| {
                    let vl = v.label.as_ref();
                    vl == l || vl.starts_with(&format!("{}:", l))
                });
                let props_ok = props.iter().all(|(k, expected)| {
                    v.properties.get(k).map_or(false, |actual| {
                        to_property_value(expected.clone()).ok().map_or(false, |pv| actual == &pv)
                    })
                });
                label_ok && props_ok
            };

            for v in &all_vertices {
                if matches(v, &left_pat) { left_ids.insert(v.id); }
                if matches(v, &right_pat) { right_ids.insert(v.id); }
            }

            let mut matched_edge_ids = HashSet::new();
            let mut matched_vertex_ids = left_ids.union(&right_ids).copied().collect::<HashSet<_>>();

            let mut queue: Vec<(SerializableUuid, u32)> = matched_vertex_ids.iter().map(|&id| (id, 0)).collect();
            let mut visited_edges = HashSet::new();

            while let Some((current_id, hop)) = queue.pop() {
                if hop >= 2 { continue; }
                for edge in &all_edges {
                    if visited_edges.contains(&edge.id) { continue; }
                    let (from, to) = (edge.outbound_id, edge.inbound_id);
                    let next_id = if from == current_id {
                        Some(to)
                    } else if to == current_id {
                        Some(from)
                    } else {
                        None
                    };
                    if let Some(next) = next_id {
                        visited_edges.insert(edge.id);
                        matched_edge_ids.insert(edge.id);
                        matched_vertex_ids.insert(next);
                        if hop < 1 { queue.push((next, hop + 1)); }
                    }
                }
            }

            let vertices: Vec<Vertex> = all_vertices.into_iter()
                .filter(|v| matched_vertex_ids.contains(&v.id))
                .collect();
            let edges: Vec<Edge> = all_edges.into_iter()
                .filter(|e| matched_edge_ids.contains(&e.id))
                .collect();

            Ok(json!({ "vertices": vertices, "edges": edges }))
        }
    }
}
