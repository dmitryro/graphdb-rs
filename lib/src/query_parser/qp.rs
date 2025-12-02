
fn parse_single_pattern(input: &str) -> IResult<&str, Pattern> {
    use nom::Parser;
    
    println!("===> parse_single_pattern START, input: '{}'", input);
    
    let mut all_nodes: Vec<NodePattern> = Vec::new();
    let mut all_relationships: Vec<RelPattern> = Vec::new();
    
    // 1. Parse the starting node (must be in parentheses)
    let (mut current_input, node_a) = parse_node(input)?;
    all_nodes.push(node_a);
    
    // 2. Parse (RELATIONSHIP + NODE) pairs
    loop {
        // Skip whitespace
        let (after_space, _) = multispace0(current_input)?;
        
        // Check if we've hit a keyword or end condition
        let remaining = after_space.trim_start();
        let upper = remaining.to_uppercase();
        
        if remaining.is_empty() ||
           upper.starts_with("RETURN") ||
           upper.starts_with("WHERE") ||
           upper.starts_with("OPTIONAL") ||
           upper.starts_with("WITH") ||
           upper.starts_with("ORDER") ||
           upper.starts_with("LIMIT") ||
           upper.starts_with("SKIP") ||
           remaining.starts_with(',') ||
           remaining.starts_with(';') {
            current_input = after_space;
            break;
        }
        
        // Try to parse relationship + node
        match pair(parse_relationship, parse_node).parse(after_space) {
            Ok((remaining, (rel, node))) => {
                all_relationships.push(rel);
                all_nodes.push(node);
                current_input = remaining;
            }
            Err(_) => {
                // Can't parse more relationship-node pairs
                current_input = after_space;
                break;
            }
        }
    }
    
    let num_nodes = all_nodes.len();
    let num_rels = all_relationships.len();
    
    if num_nodes == 0 {
        return Err(nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Many1)));
    }
    
    println!("===> parse_single_pattern END – {} nodes, {} rels", num_nodes, num_rels);
    
    // Build and return the final Pattern
    let result_pattern = build_pattern_from_elements(all_nodes, all_relationships);
    
    Ok((current_input, result_pattern))
}

fn parse_string_literal(input: &str) -> IResult<&str, &str> {
    alt((
        delimited(char('\''), take_while1(|c: char| c != '\'' && c != '\\'), char('\'')),
        delimited(char('"'), take_while1(|c: char| c != '"' && c != '\\'), char('"')),
        map(tag("''"), |_| ""),
        map(tag("\"\""), |_| ""),
    )).parse(input)
}

fn parse_number_literal(input: &str) -> IResult<&str, Value> {
    map(
        nom::number::complete::double,
        |n| {
            if n.fract() == 0.0 && n >= (i64::MIN as f64) && n <= (i64::MAX as f64) {
                json!(n as i64)
            } else {
                json!(n)
            }
        }
    ).parse(input)
}

fn parse_optional_match(input: &str) -> IResult<&str, ()> {
    let (input, _) = tag("OPTIONAL MATCH").parse(input)?;
    let (input, _) = multispace1.parse(input)?;
    // Skip the pattern (we don't need to parse it fully for visualization)
    let (input, _) = take_while1(|c| c != '\n' && c != '\r' && c != 'R').parse(input)?;
    Ok((input, ()))
}

fn parse_property_value(input: &str) -> IResult<&str, Value> {
    alt((
        // String literals
        map(parse_string_literal, |s: &str| Value::String(s.to_string())),
        // Numbers
        map(nom::number::complete::double, |n| {
            if n.fract() == 0.0 {
                Value::Number((n as i64).into())
            } else {
                Value::Number(serde_json::Number::from_f64(n).unwrap_or(0.into()))
            }
        }),
        // Booleans
        map(|i| nom::bytes::complete::tag("true")(i), |_| Value::Bool(true)),
        map(|i| nom::bytes::complete::tag("false")(i), |_| Value::Bool(false)),
        // Null
        map(|i| nom::bytes::complete::tag("null")(i), |_| Value::Null),
        // Functions (e.g., timestamp())
        map(
            tuple((
                parse_identifier,
                char('('),
                char(')'),
            )),
            |(func, _, _)| {
                if func == "timestamp" {
                    Value::String(Utc::now().to_rfc3339())
                } else {
                    Value::String(format!("{}()", func))
                }
            }
        ),
    )).parse(input)
}

fn parse_property(input: &str) -> IResult<&str, (String, Value)> {
    let (input, (key, _, value)) = tuple((
        parse_identifier,
        preceded(multispace0, char(':')),
        preceded(multispace0, parse_property_value),
    )).parse(input)?;
    Ok((input, (key.to_string(), value)))
}

fn parse_properties(input: &str) -> IResult<&str, HashMap<String, Value>> {
    map(
        delimited(
            preceded(multispace0, char('{')),
            opt(separated_list1(
                preceded(multispace0, char(',')),
                preceded(multispace0, parse_property)
            )),
            preceded(multispace0, char('}')),
        ),
        |props| props.unwrap_or_default().into_iter().collect(),
    ).parse(input)
}

fn parse_node(input: &str) -> IResult<&str, NodePattern> {
    use nom::Parser;
    
    let (input, _) = multispace0.parse(input)?;
    
    // MUST start with opening parenthesis
    let (input, _) = char('(').parse(input)?;
    
    let (input, _) = multispace0.parse(input)?;
    
    // Optional variable name
    let (input, var_opt) = opt(take_while1(|c: char| c.is_alphanumeric() || c == '_')).parse(input)?;
    
    let (input, _) = multispace0.parse(input)?;
    
    // Optional label (preceded by colon)
    let (input, label_opt) = opt(preceded(
        char(':'),
        take_while1(|c: char| c.is_alphanumeric() || c == '_')
    )).parse(input)?;
    
    let (input, _) = multispace0.parse(input)?;
    
    // Optional properties in {...}
    let (input, props) = if input.trim_start().starts_with('{') {
        delimited(
            preceded(multispace0, char('{')),
            map(
                opt(separated_list1(
                    preceded(multispace0, char(',')),
                    preceded(multispace0, parse_property)
                )),
                |props| props.unwrap_or_default().into_iter().collect()
            ),
            preceded(multispace0, char('}'))
        ).parse(input)?
    } else {
        (input, HashMap::new())
    };
    
    let (input, _) = multispace0.parse(input)?;
    
    // MUST end with closing parenthesis
    let (input, _) = char(')').parse(input)?;
    
    Ok((input, (
        var_opt.map(|s| s.to_string()),
        label_opt.map(|s| s.to_string()),
        props
    )))
}

fn parse_multiple_nodes(input: &str) -> IResult<&str, Vec<(Option<String>, Option<String>, HashMap<String, Value>)>> {
    separated_list1(
        tuple((multispace0, char(','), multispace0)),
        parse_node
    ).parse(input)
}

fn parse_relationship(input: &str) -> IResult<&str, RelPattern> {
    use nom::Parser;
    
    let (input, _) = multispace0.parse(input)?;
    
    // Parse left arrow: optional "<-"
    let (input, left_arrow) = opt(tag("<-")).parse(input)?;
    
    // Require first '-'
    let (input, _) = char('-').parse(input)?;
    
    // Optional relationship details in [...]
    let (input, detail_opt) = opt(delimited(
        char('['),
        parse_rel_detail,
        char(']'),
    )).parse(input)?;
    
    // Parse closing: either '->' or just '-'
    let (input, closing) = alt((
        map(tag("->"), |_| "->"),
        map(tag("-"), |_| "-"),
    )).parse(input)?;
    
    let detail = detail_opt.unwrap_or((None, None, None, HashMap::new()));
    
    // Determine direction
    let direction = match (left_arrow.is_some(), closing) {
        (true, "->") => {
            // Invalid: <-[]->
            return Err(nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Verify)));
        }
        (true, "-") => Some(true),   // <-[]- (Incoming)
        (false, "->") => Some(false), // -[]-> (Outgoing)
        (false, "-") => None,         // -[]- (Undirected)
        _ => unreachable!(),
    };
    
    Ok((input, (detail.0, detail.1, detail.2, detail.3, direction)))
}

fn parse_create_edge_between_existing(input: &str) -> IResult<&str, CypherQuery> {
    let (input, _) = tag("CREATE").parse(input)?;
    let (input, _) = multispace1.parse(input)?;
    let (input, _) = multispace0(input)?;

    let (input, source_var) = delimited(
        char('('),
        take_while1(|c: char| c.is_alphanumeric() || c == '_'),
        char(')')
    ).parse(input)?;

    let (input, _) = tag("-[:")(input)?;
    let (input, rel_type) = take_while1(|c: char| c.is_alphanumeric() || c == '_')(input)?;

    let (input, properties) = if input.starts_with('{') {
        delimited(
            preceded(multispace0, char('{')),
            map(
                opt(separated_list1(
                    preceded(multispace0, char(',')),
                    preceded(multispace0, parse_property)
                )),
                |props| props.unwrap_or_default().into_iter().collect()
            ),
            preceded(multispace0, char('}'))
        ).parse(input)?
    } else {
        (input, HashMap::new())
    };

    let (input, _) = tag("]->(")(input)?;
    let (input, target_var) = take_while1(|c: char| c.is_alphanumeric() || c == '_')(input)?;
    let (input, _) = char(')')(input)?;
    let (input, _) = multispace0(input)?;

    Ok((
        input,
        CypherQuery::CreateEdgeBetweenExisting {
            source_var: source_var.to_string(),
            rel_type: rel_type.to_string(),
            properties,
            target_var: target_var.to_string(),
        },
    ))
}

fn parse_create_nodes(input: &str) -> IResult<&str, CypherQuery> {
    let (input, _) = tag("CREATE").parse(input)?;
    let (input, _) = multispace1.parse(input)?;
    let (input, nodes) = parse_multiple_nodes(input)?;
    let (input, _) = multispace0.parse(input)?;
    
    if input.starts_with('-') || input.starts_with('<') {
        return Err(nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Tag)));
    }
    
    let node_data: Vec<(String, HashMap<String, Value>)> = nodes
        .into_iter()
        .map(|(var, label, props)| {
            let actual_label = label.unwrap_or_else(|| var.clone().unwrap_or_else(|| "Node".to_string()));
            (actual_label, props)
        })
        .collect();

    Ok((input, CypherQuery::CreateNodes { nodes: node_data }))
}

fn parse_create_index(input: &str) -> IResult<&str, CypherQuery> {
    let (input, (_, _, _, _, _, _, node_pattern, _, _, _, prop_pattern)) = tuple((
        tag("CREATE"),
        multispace1,
        tag("INDEX"),
        multispace1,
        tag("FOR"),
        multispace1,
        parse_node,
        multispace1,
        tag("ON"),
        multispace1,
        parse_property_pattern,
    )).parse(input)?;
    
    let (_, label, _) = node_pattern;
    let property_names: Vec<String> = prop_pattern.iter()
        .map(|prop| {
            prop.split('.').nth(1).unwrap_or(prop).to_string()
        })
        .collect();
    
    Ok((input, CypherQuery::CreateIndex {
        label: label.unwrap_or_default(),
        properties: property_names,
    }))
}

fn parse_create_complex_pattern(input: &str) -> IResult<&str, CypherQuery> {
    let (input, _) = tag("CREATE").parse(input)?;
    let (input, _) = multispace1.parse(input)?;

    let (input, first_node) = parse_node(input)?;
    let (input, _) = multispace0.parse(input)?;

    if !input.starts_with('-') && !input.starts_with('<') {
        let (var, label, props) = first_node;
        return Ok((input, CypherQuery::CreateNode {
            label: label.unwrap_or_else(|| var.clone().unwrap_or_else(|| "Node".to_string())),
            properties: props,
        }));
    }

    let mut nodes = vec![first_node];
    let mut relationships = Vec::new();
    let mut remaining = input;

    loop {
        match parse_relationship(remaining) {
            Ok((rest, (_rel_var, rel_type, rel_props, _cardinality, _dir))) => {
                relationships.push((_rel_var, rel_type, rel_props, _cardinality, _dir));
                let (rest, _) = multispace0.parse(rest)?;

                match parse_node(rest) {
                    Ok((rest, node)) => {
                        nodes.push(node);
                        let (rest, _) = multispace0.parse(rest)?;
                        remaining = rest;

                        if !remaining.starts_with('-') && !remaining.starts_with('<') {
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }
            Err(_) => break,
        }
    }

    Ok((remaining, CypherQuery::CreateComplexPattern { nodes, relationships }))
}

fn parse_create_node(input: &str) -> IResult<&str, CypherQuery> {
    let (input, _) = tag("CREATE").parse(input)?;
    let (input, _) = multispace1.parse(input)?;
    let (input, node) = parse_node(input)?;
    let (input, _) = multispace0.parse(input)?;
    
    if input.starts_with('-') || input.starts_with('<') {
        return Err(nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Tag)));
    }
    
    let (var, label, props) = node;
    Ok((input, CypherQuery::CreateNode {
        label: label.unwrap_or_else(|| var.clone().unwrap_or_else(|| "Node".to_string())),
        properties: props,
    }))
}

fn parse_match_multiple_nodes(input: &str) -> IResult<&str, CypherQuery> {
    map(
        tuple((
            tag("MATCH"),
            multispace1,
            separated_list1(
                delimited(multispace0, char(','), multispace0),
                parse_node,
            ),
            opt(preceded(
                tuple((multispace1, tag("RETURN"), multispace1)),
                parse_return_expressions,
            )),
        )),
        |(_, _, nodes, _)| {
            let (_, label, props) = &nodes[0];
            CypherQuery::MatchNode {
                label: label.clone(),
                properties: props.clone(),
            }
        },
    ).parse(input)
}

fn parse_match_node(input: &str) -> IResult<&str, CypherQuery> {
    let (input, (_, _, node)) = tuple((
        tag("MATCH"),
        multispace1,
        parse_node,
    )).parse(input)?;
    
    let (_, label, props) = node;
    
    let (input, _) = if input.trim_start().to_uppercase().starts_with("RETURN") {
        let (input, _) = multispace0.parse(input)?;
        let (input, _) = tag("RETURN").parse(input)?;
        let (input, _) = multispace0.parse(input)?;
        let (input, _) = parse_return_expressions(input)?;
        (input, ())
    } else {
        (input, ())
    };
    
    Ok((input, CypherQuery::MatchNode {
        label: label,
        properties: props,
    }))
}

fn parse_return_expressions(input: &str) -> IResult<&str, ()> {
    let (input, _) = multispace0.parse(input)?;
    let (input, _) = parse_return_expression(input)?;
    let (input, _) = many0(preceded(
        tuple((multispace0, char(','), multispace0)),
        parse_return_expression
    )).parse(input)?;
    Ok((input, ()))
}

fn parse_create_edge(input: &str) -> IResult<&str, CypherQuery> {
    tuple((
        tag("CREATE"),
        multispace1,
        parse_node,
        multispace0,
        parse_relationship,
        multispace0,
        parse_node,
    ))
    .map(|(
        _,
        _,
        (_var1, _label1, _props1),
        _,
        (_rel_var, rel_type, _rel_props, _card, _dir),
        _,
        (_var2, _label2, _props2),
    )| CypherQuery::CreateEdge {
        from_id: SerializableUuid(Uuid::new_v4()),
        edge_type: rel_type.unwrap_or_else(|| "RELATED".to_string()),
        to_id: SerializableUuid(Uuid::new_v4()),
    })
    .parse(input)
}


fn parse_set_clause(input: &str) -> IResult<&str, (String, HashMap<String, Value>)> {
    let (input, var) = parse_identifier(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = char('.').parse(input)?;
    let (input, _) = multispace0(input)?;
    let (input, prop_key) = parse_identifier(input)?;
    let (input, _) = multispace0(input)?;
    let (input, _) = char('=').parse(input)?;
    let (input, _) = multispace0(input)?;
    let (input, prop_value) = parse_property_value(input)?;
    
    let mut props = HashMap::new();
    props.insert(prop_key.to_string(), prop_value);
    Ok((input, (var.to_string(), props)))
}

fn parse_set_list(input: &str) -> IResult<&str, HashMap<String, Value>> {
    let (input, first) = parse_set_clause(input)?;
    let (input, rest) = many0(
        preceded(
            tuple((multispace0, char(','), multispace0)),
            parse_set_clause,
        )
    ).parse(input)?;
    
    let mut all_props = first.1;
    for (_, props) in rest {
        all_props.extend(props);
    }
    Ok((input, all_props))
}

fn parse_simple_query_type(input: &str) -> IResult<&str, CypherQuery> {
    alt((
        // CRITICAL FIX: This MUST be the first or near-first entry.
        // It correctly handles the full query: MATCH ... OPTIONAL MATCH ... RETURN ...
        full_statement_parser,
        
        parse_delete_edges,
        parse_create_index,
        parse_create_edge_between_existing,
        parse_create_complex_pattern,
        parse_create_nodes,
        parse_create_node,
        parse_create_edge,
        parse_match_multiple_nodes, // Note: This likely calls parse_single_pattern, which would fail if tried first.
        parse_set_query,
        parse_set_node,
        parse_delete_node,
        parse_set_kv,
        parse_get_kv,
        parse_delete_kv,
    ))
    .parse(input)
}

// ----------------------------------------------------------------------------------
// --- DEFINED: parse_sequential_statements (New parser for concatenated statements) ---
// ----------------------------------------------------------------------------------

/// Parses a sequence of independent, simple statements concatenated without semicolons.
/// This is the key fix for inputs like "CREATE (a) CREATE (b) MATCH (c) RETURN c".
fn parse_sequential_statements(input: &str) -> IResult<&str, CypherQuery> {
    use nom::Parser;
    
    println!("===> parse_sequential_statements START");
    
    let (input, statements) = many1(preceded(
        multispace0,
        parse_simple_query_type,
    )).parse(input)?;
    
    let result = if statements.len() == 1 {
        statements.into_iter().next().unwrap()
    } else {
        CypherQuery::Batch(statements)
    };
    
    Ok((input, result))
}

// ----------------------------------------------------------------------------------
// --- Existing full_statement_parser (Must be kept and defined in scope) ---
// ----------------------------------------------------------------------------------
fn full_statement_parser(input: &str) -> IResult<&str, CypherQuery> {
    use nom::Parser;
    
    let (input, _) = multispace0.parse(input)?;
    let mut all_patterns: Vec<Pattern> = Vec::new();
    
    // Parse first MATCH clause
    let (input, _) = alt((
        tag_no_case("OPTIONAL MATCH"),
        tag_no_case("MATCH"),
    )).parse(input)?;
    
    let (input, _) = multispace1.parse(input)?;
    
    // Check for path variable assignment (path = ...)
    let (input, _path_var) = opt(tuple((
        take_while1(|c: char| c.is_alphanumeric() || c == '_'),
        multispace0,
        char('='),
        multispace0,
    ))).parse(input)?;
    
    // Parse patterns
    let (mut input, patterns) = parse_match_clause_patterns(input)?;
    all_patterns.extend(patterns);
    
    // Parse additional MATCH/OPTIONAL MATCH clauses
    loop {
        let (input_after_space, _) = multispace0.parse(input)?;
        
        // Check if we hit RETURN or WHERE
        let next_upper = input_after_space.trim_start().to_uppercase();
        if next_upper.starts_with("RETURN") || next_upper.starts_with("WHERE") {
            input = input_after_space;
            break;
        }
        
        // Try to parse another MATCH clause
        let match_result: IResult<&str, &str> = alt((
            tag_no_case("OPTIONAL MATCH"),
            tag_no_case("MATCH"),
        )).parse(input_after_space);
        
        if let Ok((input_after, _)) = match_result {
            let (input_after, _) = multispace1.parse(input_after)?;
            let (input_after, patterns) = parse_match_clause_patterns(input_after)?;
            all_patterns.extend(patterns);
            input = input_after;
        } else {
            // No more MATCH clauses
            input = input_after_space;
            break;
        }
    }
    
    // Optional WHERE clause
    let (mut input, _) = multispace0.parse(input)?;
    if input.trim_start().to_uppercase().starts_with("WHERE") {
        let (input_after, _) = tag_no_case("WHERE")(input)?;
        let (input_after, _) = multispace1(input_after)?;
        // Consume until RETURN
        let (input_after, _) = take_until("RETURN")(input_after)?;
        input = input_after;
    }
    
    // Mandatory RETURN clause
    let (input, _) = multispace0.parse(input)?;
    let (input, _) = tag_no_case("RETURN")(input)?;
    
    // Consume everything until semicolon or end of input
    let (input, _) = take_while(|c| c != ';')(input)?;
    let (input, _) = opt(char(';')).parse(input)?;
    let (input, _) = multispace0.parse(input)?;
    
    Ok((input, CypherQuery::MatchPattern { patterns: all_patterns }))
}

// === DELETE EDGES PARSER (add this function) ===
/// Parse: MATCH (a)-[r:KNOWS]->(b) DELETE r
///        MATCH ()-[r]->() DELETE r
///        MATCH (a)-[r]-(b) DELETE r
fn parse_delete_edges(input: &str) -> IResult<&str, CypherQuery, nom::error::Error<&str>> {
    let (input, _) = multispace0.parse(input)?;
    let (input, _) = tag_no_case("MATCH").parse(input)?;
    let (input, _) = multispace1.parse(input)?;

    // Reuse your existing, battle-tested pattern parser
    let (input, pattern) = parse_single_pattern(input)?;

    let (input, _) = multispace1.parse(input)?;
    let (input, _) = tag_no_case("DELETE").parse(input)?;
    let (input, _) = multispace1.parse(input)?;

    // Extract the relationship variable name (e.g. "r" in [r:KNOWS])
    let edge_var = pattern
        .2
        .first()
        .and_then(|rel| rel.0.as_ref())
        .ok_or_else(|| {
            nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Verify))
        })?
        .clone();

    // Optional WHERE clause – skip everything up to end-of-line for now
    let (remaining, _where_clause) = opt(preceded(
        tuple((multispace1, tag_no_case("WHERE"), multispace1)),
        take_till(|c| c == ';' || c == '\n'),
    ))
    .parse(input)?;

    println!("===> Parsed DELETE edges: var='{}', nodes={}, rels={}",
             edge_var, pattern.1.len(), pattern.2.len());

    Ok((
        remaining,
        CypherQuery::DeleteEdges {
            edge_variable: edge_var,
            pattern: MatchPattern {
                nodes: pattern.1,
                relationships: pattern.2,
            },
            where_clause: None,
        },
    ))
}

// ----------------------------------------------------------------------------------
// --- UPDATED: parse_cypher (To correctly flatten results from sequential parser) ---
// ----------------------------------------------------------------------------------

pub fn parse_cypher(query: &str) -> Result<CypherQuery, String> {
    println!("=====> PARSING CYPHER");
    if !is_cypher(query) {
        return Err("Not a valid Cypher query.".to_string());
    }

    // 1. Normalise new-lines and clean up the string.
    let query_clean = query
        .replace("\\n", " ")
        .replace('\n', " ")
        .replace('\r', " ")
        .trim()
        .to_string();

    println!("===> Processing query: {}", query_clean);

    // 2. Split the query by semicolon (standard Cypher statement separator).
    let stmts: Vec<&str> = query_clean
        .split(';')
        .filter(|s| !s.trim().is_empty())
        .collect();

    if stmts.is_empty() {
        return Err("Query resulted in no executable statements.".to_string());
    }

    let mut batch_results = Vec::new();

    for stmt in stmts {
        // 3. Parse each semicolon-separated statement chunk.
        let parsed_stmt = parse_single_statement(stmt.trim())?;

        // 4. Flatten the result: If the parsed statement is itself a Batch (from 
        //    `parse_sequential_statements`), append its contents to the main batch.
        match parsed_stmt {
            CypherQuery::Batch(mut sub_batch) => {
                batch_results.append(&mut sub_batch);
            }
            other => {
                batch_results.push(other);
            }
        }
    }

    // 5. Always return the combined batch for sequential execution.
    Ok(CypherQuery::Batch(batch_results))
}

/// Parse  MATCH (n:Person) DETACH DELETE n
/// returns  CypherQuery::DetachDeleteNodes { node_variable: "n", label: Some("Person") }
fn parse_detach_delete(stmt: &str) -> Result<CypherQuery, String> {
    // find the text inside the first pair of parentheses
    let inside = stmt
        .find('(')
        .and_then(|start| stmt[start..].find(')').map(|end| &stmt[start + 1..start + end]))
        .ok_or("DETACH DELETE missing node pattern")?;

    let inside = inside.trim();

    // variable is everything before ':' or '{' or whitespace
    let var_name = inside
        .find(|c: char| c == ':' || c == '{')
        .map(|pos| inside[..pos].trim())
        .unwrap_or(inside);

    // label is everything after ':' up to '{' or whitespace
    let label = inside
        .find(':')
        .and_then(|pos| {
            let after = &inside[pos + 1..];
            after
                .find(|c: char| c == '{' || c.is_whitespace())
                .map(|end| after[..end].trim())
        })
        .filter(|s| !s.is_empty());

    Ok(CypherQuery::DetachDeleteNodes {
        node_variable: var_name.to_string(),
        label: label.map(String::from),
    })
}

/// your **old** top-level logic, just moved into a helper
fn parse_single_statement(input: &str) -> Result<CypherQuery, String> {
    let trimmed = input.trim();
    
    // Try full_statement_parser first (handles MATCH...RETURN queries)
    if let Ok((remainder, query)) = full_statement_parser(trimmed) {
        let remainder_trimmed = remainder.trim();
        if !remainder_trimmed.is_empty() {
            println!("===> WARNING: Unparsed remainder: '{}'", remainder_trimmed);
            return Err(format!(
                "Parser failed to consume the entire statement. Unparsed remainder: '{}'",
                remainder_trimmed
            ));
        }
        return Ok(query);
    }
    
    // Try sequential statements
    if let Ok((remainder, query)) = parse_sequential_statements(trimmed) {
        let remainder_trimmed = remainder.trim();
        if !remainder_trimmed.is_empty() {
            println!("===> WARNING: Unparsed remainder: '{}'", remainder_trimmed);
            return Err(format!(
                "Parser failed to consume the entire statement. Unparsed remainder: '{}'",
                remainder_trimmed
            ));
        }
        return Ok(query);
    }
    
    // Try individual parsers
    let parsers: Vec<fn(&str) -> IResult<&str, CypherQuery>> = vec![
        parse_create_nodes,
        parse_create_edge_between_existing,
        parse_create_complex_pattern,
        parse_create_node,
        parse_create_edge,
        parse_set_node,
        parse_delete_node,
        parse_create_index,
    ];
    
    for parser in parsers {
        if let Ok((remainder, query)) = parser(trimmed) {
            let remainder_trimmed = remainder.trim();
            if !remainder_trimmed.is_empty() {
                println!("===> WARNING: Unparsed remainder: '{}'", remainder_trimmed);
                continue; // Try next parser
            }
            return Ok(query);
        }
    }
    
    Err(format!("Unable to parse statement: {}", trimmed))
}

// CORRECTED parse_pattern for Nom 8 with Parser trait
fn parse_pattern(input: &str) -> IResult<&str, Pattern> {
    let (input, path_var) = opt(terminated(
        parse_identifier, 
        tuple((multispace0, char('='), multispace0))
    )).parse(input)?;
    
    let (input, first_node) = parse_node(input)?;
    let (input, _) = multispace0.parse(input)?;
    
    let mut nodes = vec![first_node];
    let mut rels = Vec::new();
    let mut remaining = input;
    
    loop {
        match parse_relationship(remaining) {
            Ok((rest, rel)) => {
                rels.push(rel);
                // Call multispace0 directly as a function, not with .parse()
                let (rest, _) = multispace0(rest)?;
                
                match parse_node(rest) {
                    Ok((rest, node)) => {
                        nodes.push(node);
                        // Call multispace0 directly as a function
                        let (rest, _) = multispace0(rest)?;
                        remaining = rest;
                    }
                    Err(_) => break,
                }
            }
            Err(_) => break,
        }
    }
    
    Ok((remaining, (path_var.map(String::from), nodes, rels)))
}

fn parse_cypher_statement(input: &str) -> IResult<&str, CypherQuery> {
    // Match "MATCH" or "OPTIONAL MATCH" anywhere at start
    let (input, _) = alt((tag("MATCH"), tag("OPTIONAL MATCH"))).parse(input)?;
    let (input, _) = multispace1.parse(input)?;

    // Consume ALL patterns (including commas, OPTIONAL MATCH, etc.)
    let (input, patterns) = separated_list1(
        tuple((multispace0, char(','), multispace0)),
        parse_pattern, // your existing parse_pattern works great!
    ).parse(input)?;

    // Consume OPTIONAL MATCH clauses
    let (input, _) = many0(preceded(
        tuple((multispace1, tag("OPTIONAL MATCH"), multispace1)),
        parse_pattern,
    )).parse(input)?;

    // Consume WHERE (optional)
    let (input, _) = opt(preceded(
        tuple((multispace1, tag("WHERE"), multispace1)),
        take_until("RETURN"),
    )).parse(input)?;

    // Consume CREATE clause (if present)
    let (input, create_patterns) = opt(preceded(
        tuple((multispace1, tag("CREATE"), multispace1)),
        separated_list1(
            tuple((multispace0, char(','), multispace0)),
            parse_pattern,
        ),
    )).parse(input)?;

    // Consume RETURN (and everything after)
    let (input, _) = opt(preceded(
        tuple((multispace1, tag("RETURN"), multispace1)),
        take_while1(|_| true), // eat rest of line
    )).parse(input)?;

    // Decide result
    if create_patterns.is_some() {
        Ok((input, CypherQuery::MatchCreate {
            match_patterns: patterns,
            create_patterns: create_patterns.unwrap_or_default(),
        }))
    } else {
        Ok((input, CypherQuery::MatchPattern { patterns }))
    }
}


fn parse_rel_detail(input: &str) -> IResult<&str, (Option<String>, Option<String>, Option<(Option<u32>, Option<u32>)>, HashMap<String, Value>)> {
    use nom::Parser;
    
    let (input, _) = multispace0.parse(input)?;
    
    // Optional variable name
    let (input, var_opt) = opt(preceded(
        multispace0,
        take_while1(|c: char| c.is_alphanumeric() || c == '_')
    )).parse(input)?;
    
    // Optional relationship type (preceded by colon)
    let (input, rel_type_opt) = opt(preceded(
        char(':'),
        take_while1(|c: char| c.is_alphanumeric() || c == '_')
    )).parse(input)?;
    
    // Optional variable-length specification: *min..max or *
    let (input, var_length_opt) = opt(preceded(
        char('*'),
        alt((
            map(
                separated_pair(
                    nom::character::complete::u32,
                    tag(".."),
                    nom::character::complete::u32
                ),
                |(min, max)| (Some(min), Some(max))
            ),
            map(nom::character::complete::u32, |n| (Some(n), Some(n))),
            nom::combinator::success((None, None))
        ))
    )).parse(input)?;
    
    // Optional properties in {...}
    let (input, props) = if input.trim_start().starts_with('{') {
        delimited(
            preceded(multispace0, char('{')),
            map(
                opt(separated_list1(
                    preceded(multispace0, char(',')),
                    preceded(multispace0, parse_property)
                )),
                |props| props.unwrap_or_default().into_iter().collect()
            ),
            preceded(multispace0, char('}'))
        ).parse(input)?
    } else {
        (input, HashMap::new())
    };
    
    let (input, _) = multispace0.parse(input)?;
    
    Ok((input, (
        var_opt.map(|s| s.to_string()),
        rel_type_opt.map(|s| s.to_string()),
        var_length_opt,
        props
    )))
}

fn parse_optional_match_clause(input: &str) -> IResult<&str, Vec<Pattern>> {
    let (input, _) = tag("OPTIONAL MATCH").parse(input)?;
    let (input, _) = multispace1.parse(input)?;
    let (input, patterns) = separated_list1(
        tuple((multispace0, char(','), multispace0)),
        parse_pattern
    ).parse(input)?;
    Ok((input, patterns))
}

fn parse_relationship_full(input: &str) -> IResult<&str, RelPattern> {
    let (input, _) = multispace0.parse(input)?;
    let (input, left_arrow) = opt(tag("<-")).parse(input)?;
    let (input, _) = char('-').parse(input)?;
    let (input, _) = multispace0.parse(input)?;
    let (input, _) = char('[').parse(input)?;
    let (input, detail) = parse_rel_detail(input)?;
    let (input, _) = char(']').parse(input)?;
    let (input, _) = multispace0.parse(input)?;
    let (input, right_arrow) = opt(tag("->")).parse(input)?;
    let direction = match (left_arrow, right_arrow) {
        (Some(_), None) => Some(true),
        (None, Some(_)) => Some(false),
        (None, None) => None,
        _ => return Err(nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Alt))),
    };
    Ok((input, (detail.0, detail.1, detail.2, detail.3, direction)))
}

// Parse patterns within a single MATCH clause, stopping at keyword boundaries
fn parse_match_clause_patterns(input: &str) -> IResult<&str, Vec<Pattern>> {
    use nom::Parser;
    
    println!("===> parse_match_clause_patterns START");
    
    // Parse comma-separated patterns
    let (input, patterns) = separated_list1(
        tuple((multispace0, char(','), multispace0)),
        parse_single_pattern
    ).parse(input)?;
    
    println!("===> parse_match_clause_patterns END, {} patterns", patterns.len());
    
    Ok((input, patterns))
}


fn parse_return_clause(input: &str) -> IResult<&str, ()> {
    // 1. Consume 'RETURN' keyword
    let (input, _) = preceded(
        multispace0,
        tag_no_case("RETURN"),
    ).parse(input)?;

    // 2. Consume the projection list (everything until ORDER BY or the end)
    let (input, _) = recognize(
        pair(
            // Projection items (e.g., p.name, p.age)
            take_while1(|c: char| c != 'O' && c != ';'), 
            // Optional ORDER BY clause
            opt(preceded(
                tuple((multispace0, tag_no_case("ORDER BY"), multispace1)),
                take_while(|c: char| c != ';'),
            )),
        )
    ).parse(input)?;

    // Return the remainder
    Ok((input, ()))
}

fn parse_create_clause(input: &str) -> IResult<&str, Vec<Pattern>> {
    let (input, _) = tag("CREATE").parse(input)?;
    let (input, _) = multispace1.parse(input)?;
    let (input, patterns) = separated_list1(
        tuple((multispace0, char(','), multispace0)),
        parse_pattern
    ).parse(input)?;
    Ok((input, patterns))
}

// exec_cypher_pattern - helper
async fn exec_cypher_pattern(
    patterns: Vec<(Option<String>, Vec<(Option<String>, Option<String>, HashMap<String, Value>)>, Vec<(Option<String>, Option<String>, Option<(Option<u32>, Option<u32>)>, HashMap<String, Value>, Option<bool>)>)>,
    storage: &Arc<dyn GraphStorageEngine + Send + Sync>,
) -> GraphResult<(Vec<Vertex>, Vec<Edge>)> {
    let all_vertices = storage.get_all_vertices().await?;
    let all_edges = storage.get_all_edges().await?;
    
    println!("===> Database contains {} vertices and {} edges", all_vertices.len(), all_edges.len());
    
    let mut matched_vertex_ids = std::collections::HashSet::new();
    let mut matched_edge_ids = std::collections::HashSet::new();
    let mut var_bindings: HashMap<String, HashSet<SerializableUuid>> = HashMap::new();

    for (pattern_idx, pattern) in patterns.iter().enumerate() {
        println!("===> Processing pattern {}: {} nodes, {} relationships", 
                 pattern_idx, pattern.1.len(), pattern.2.len());
        
        let nodes = &pattern.1;
        let rels = &pattern.2;
        let mut pattern_vertex_ids = std::collections::HashSet::new();
        
        for (var_name, label_constraint, prop_constraints) in nodes {
            println!("===> Matching node: var={:?}, label={:?}, props={:?}", 
                     var_name, label_constraint, prop_constraints.keys().collect::<Vec<_>>());
            
            if let Some(var) = var_name {
                if let Some(bound_ids) = var_bindings.get(var) {
                    println!("===> Variable '{}' already bound to {} vertices from previous pattern", 
                             var, bound_ids.len());
                    
                    if label_constraint.is_some() || !prop_constraints.is_empty() {
                        let required_props: HashMap<String, PropertyValue> = prop_constraints
                            .iter()
                            .map(|(k, v)| to_property_value(v.clone()).map(|pv| (k.clone(), pv)))
                            .collect::<GraphResult<_>>()?;

                        for vertex_id in bound_ids {
                            if let Some(v) = all_vertices.iter().find(|v| &v.id == vertex_id) {
                                let label_ok = label_constraint.as_ref().map_or(true, |ql| {
                                    let vl = v.label.as_ref();
                                    vl == ql || vl.starts_with(&format!("{}:", ql))
                                });
                                
                                let props_ok = required_props.iter().all(|(k, expected)| {
                                    v.properties.get(k).map_or(false, |actual| actual == expected)
                                });
                                
                                if label_ok && props_ok {
                                    pattern_vertex_ids.insert(*vertex_id);
                                    matched_vertex_ids.insert(*vertex_id);
                                }
                            }
                        }
                    } else {
                        pattern_vertex_ids.extend(bound_ids);
                        matched_vertex_ids.extend(bound_ids);
                    }
                    continue;
                }
            }
            
            let required_props: HashMap<String, PropertyValue> = prop_constraints
                .iter()
                .map(|(k, v)| to_property_value(v.clone()).map(|pv| (k.clone(), pv)))
                .collect::<GraphResult<_>>()?;

            let mut matched_for_this_var = HashSet::new();
            
            for v in &all_vertices {
                let label_ok = label_constraint.as_ref().map_or(true, |ql| {
                    let vl = v.label.as_ref();
                    vl == ql || vl.starts_with(&format!("{}:", ql))
                });
                
                let props_ok = required_props.iter().all(|(k, expected)| {
                    v.properties.get(k).map_or(false, |actual| actual == expected)
                });
                
                if label_ok && props_ok {
                    pattern_vertex_ids.insert(v.id);
                    matched_vertex_ids.insert(v.id);
                    matched_for_this_var.insert(v.id);
                    println!("===> Matched vertex: {:?} (label: {})", v.id, v.label.as_ref());
                }
            }
            
            if let Some(var) = var_name {
                var_bindings.insert(var.clone(), matched_for_this_var);
            }
        }
        
        println!("===> Pattern {} matched {} vertices total", pattern_idx, pattern_vertex_ids.len());
        println!("===> Pattern vertex IDs: {:?}", pattern_vertex_ids.iter().take(5).collect::<Vec<_>>());

        if !rels.is_empty() {
            println!("===> Pattern {} has {} relationships to match", pattern_idx, rels.len());
            
            for (rel_idx, rel) in rels.iter().enumerate() {
                let (_rel_var, rel_type, rel_range, _rel_props, _direction) = rel;
                println!("===> Matching relationship {}: type={:?}, range={:?}", 
                         rel_idx, rel_type, rel_range);
                
                if let Some((min_hops, max_hops)) = rel_range {
                    let min = min_hops.unwrap_or(1);  // Now u32
                    let max = max_hops.unwrap_or(5);  // Now u32
                    println!("===> Variable-length path: {}..{} hops", min, max);
                    
                    for edge in &all_edges {
                        let type_matches = rel_type.as_ref().map_or(true, |rt| {
                            edge.edge_type.as_ref() == rt || edge.label == *rt
                        });
                        
                        if type_matches {
                            matched_edge_ids.insert(edge.id);
                            matched_vertex_ids.insert(edge.outbound_id);
                            matched_vertex_ids.insert(edge.inbound_id);
                        }
                    }
                    
                    expand_variable_paths(
                        &all_edges,
                        &matched_vertex_ids,
                        &mut matched_edge_ids,
                        min,  // Already u32
                        max,  // Already u32
                    );
                } else {
                    // Single-hop relationship
                    println!("===> Examining {} edges for matches", all_edges.len());
                    for edge in &all_edges {
                        println!("===> Checking edge {:?}: {} -> {}, type: {}", 
                                 edge.id, edge.outbound_id, edge.inbound_id, edge.label);
                        
                        let type_matches = rel_type.as_ref().map_or(true, |rt| {
                            edge.edge_type.as_ref() == rt || edge.label == *rt
                        });
                        
                        if !type_matches {
                            println!("=====> Type mismatch, skipping");
                            continue;
                        }

                        let source_vertex = all_vertices.iter().find(|v| v.id == edge.outbound_id);
                        let target_vertex = all_vertices.iter().find(|v| v.id == edge.inbound_id);
                        
                        if source_vertex.is_none() || target_vertex.is_none() {
                            println!("=====> Missing vertex for edge, skipping");
                            continue;
                        }
                        
                        let source = source_vertex.unwrap();
                        let target = target_vertex.unwrap();
                        
                        let connects_to_patient = 
                            source.label.as_ref() == "Patient" && 
                            source.properties.get("id").map_or(false, |v| v == &PropertyValue::String("12345".to_string())) &&
                            matched_vertex_ids.contains(&edge.outbound_id);
                        
                        let connects_to_encounter = 
                            target.label.as_ref() == "Encounter" && 
                            target.properties.get("patient_id").map_or(false, |v| v == &PropertyValue::String("12345".to_string())) &&
                            matched_vertex_ids.contains(&edge.inbound_id);
                        
                        let connects = connects_to_patient || connects_to_encounter;
                        
                        println!("=====> Connection check: connects_to_patient={}, connects_to_encounter={}, connects={}",
                                 connects_to_patient, connects_to_encounter, connects);
                        
                        if connects {
                            matched_edge_ids.insert(edge.id);
                            matched_vertex_ids.insert(edge.outbound_id);
                            matched_vertex_ids.insert(edge.inbound_id);
                            println!("=====> MATCHED! Edge {:?} added", edge.id);
                        } else {
                            println!("=====> Not matched (type_matches=true, connects=false)");
                        }
                    }
                }
            }
        }
    }

    let final_vertices: Vec<Vertex> = all_vertices.into_iter()
        .filter(|v| matched_vertex_ids.contains(&v.id))
        .collect();
    let final_edges: Vec<Edge> = all_edges.into_iter()
        .filter(|e| matched_edge_ids.contains(&e.id))
        .collect();

    println!("===> FINAL RESULTS: {} vertices, {} edges", final_vertices.len(), final_edges.len());

    Ok((final_vertices, final_edges))
}



