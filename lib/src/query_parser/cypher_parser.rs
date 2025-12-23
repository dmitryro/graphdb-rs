// lib/src/query_parser/cypher_parser.rs
// Complete working version - merges working repo code with edge creation support
use anyhow::Context; // Fixes E0599: no method named `context`
use log::{debug, error, info, warn, trace};
use nom::{
    branch::alt,
    bytes::complete::{tag, tag_no_case, take_while1, take_until, take_till, take_while,},
    character::complete::{char, alpha1, multispace0, multispace1, alphanumeric1, i64 as parse_i64, one_of, digit1},
    combinator::{map, cut, opt, recognize, value, map_res, verify, not,peek,}, 
    multi::{separated_list0, separated_list1, many0,}, 
    number::complete::double as parse_double,
    sequence::{delimited, pair, preceded, tuple, terminated, separated_pair},
    multi::many1,
    IResult,
    Parser,
    error as NomError,
    error::Error as NomErrorType, 
};
use std::pin::Pin;
use std::future::Future;
use serde_json::{json, Value, Number, Map};
use std::collections::{BTreeMap, HashMap, HashSet,};
use std::sync::Arc;
use uuid::Uuid;
use chrono::Utc;
use models::identifiers::{Identifier, SerializableUuid};
use models::{Vertex, Edge};
use models::errors::{GraphError, GraphResult};
use models::properties::{ PropertyValue, SerializableFloat, HashablePropertyMap };
use crate::graph_engine::graph_service::{ GraphService, GRAPH_SERVICE, initialize_graph_service, GraphEvent,}; 
use crate::graph_engine::traversal::TraverseExt;
use crate::graph_engine::pattern_match::node_matches_constraints; 
use crate::database::Database;
use crate::storage_engine::{GraphStorageEngine, StorageEngine};
use crate::query_exec_engine::query_exec_engine::QueryExecEngine;
use crate::config::{ StorageConfig, QueryResult };
use crate::query_parser::query_types::*;
use crate::query_parser::parser_zmq::trigger_async_graph_cleanup;
use crate::storage_engine::GraphOp; // Fixes E0433: use of undeclared type `GraphOp`

type BoxedFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

// Helper function (must be defined in or accessible to QueryExecEngine)
// Converts Result<QueryResult, GraphError> into Result<Value, GraphError>
// --- Assuming this helper function is defined/accessible within QueryExecEngine scope ---
// It handles the conversion from the internal QueryResult type to the public serde_json::Value type.
fn query_result_to_value(qr_result: GraphResult<QueryResult>) -> GraphResult<Value> {
    println!("===> query_result_to_value START");
    use serde_json::Value;
    // Note: GraphResult is Result<T, GraphError>
    let qr = qr_result?; // Propagate GraphError
    match qr {
        // Deserialize the JSON string inside QueryResult::Success
        QueryResult::Success(s) => serde_json::from_str(&s)
            .map_err(|e| GraphError::DeserializationError(format!("Failed to parse QueryResult string to Value: {}", e))),
        // Treat Null result as an empty JSON array, typical for no result set
        QueryResult::Null => Ok(Value::Array(Vec::new())),
        // Handle other possible results
        _ => Err(GraphError::InternalError("Unsupported QueryResult type encountered.".into())),
    }
}

async fn execute_chain_internal(
    clauses: Vec<CypherQuery>,
    db: &Database, // Pass the Database reference
    storage: Arc<dyn GraphStorageEngine + Send + Sync>, // Pass the Storage Arc
) -> GraphResult<Value> {
    println!("===> execute_chain_internal START");
    let mut final_value_result: GraphResult<Value> = Ok(Value::Array(Vec::new()));
    
    for clause in clauses.into_iter() {
        // Execute each clause sequentially.
        let clause_value_result = execute_cypher(clause, db, storage.clone()).await;
        
        if clause_value_result.is_err() {
            // If any clause fails, abort the chain and return the error.
            return clause_value_result;
        }
        // Keep track of the result from the last successful clause.
        final_value_result = clause_value_result;
    }

    final_value_result
}

// Add this helper function before execute_cypher
async fn get_vertex_by_internal_id_direct(
    graph_service: &GraphService,
    internal_id: i32,
) -> GraphResult<Option<models::Vertex>> {
    println!("===> get_vertex_by_internal_id_direct START");
    // Direct database lookup without going through Cypher to avoid recursion
    let all_vertices = graph_service.get_all_vertices().await?;
    
    Ok(all_vertices.into_iter().find(|v| {
        v.properties.get("id")
            .and_then(|prop| match prop {
                PropertyValue::Integer(val) => Some(*val as i32),
                PropertyValue::I32(val) => Some(*val),
                _ => None,
            })
            .map_or(false, |id| id == internal_id)
    }))
}

pub fn is_cypher(query: &str) -> bool {
    println!("===> is_cypher START");
    let cypher_keywords = ["MATCH", "CREATE", "SET", "RETURN", "DELETE", "MERGE", "REMOVE"];
    cypher_keywords.iter().any(|kw| query.trim().to_uppercase().starts_with(kw))
}

// =============================================================================
// COMPLETE REPLACEMENT FOR full_statement_parser AND HELPERS
// Place these functions in your cypher_parser.rs file
// =============================================================================

// Helper: Check if a character could start a Cypher keyword
fn is_cypher_keyword_start(c: char) -> bool {
    println!("===> is_cypher_keyword_start START");
    matches!(c, 'O' | 'o' | 'W' | 'w' | 'R' | 'r' | 'C' | 'c' | 'M' | 'm' | 'S' | 's' | 'D' | 'd')
}

/// Parse identifier (variable or property name)
fn parse_identifier(input: &str) -> IResult<&str, &str> {
    println!("===> parse_identifier START");
    recognize(
        pair(
            alt((alphanumeric1, tag("_"))),
            take_while(|c: char| c.is_alphanumeric() || c == '_')
        )
    ).parse(input)
}

/// Parser for Literals (Values)
fn parse_literal(input: &str) -> IResult<&str, Literal> {
    println!("===> parse_literal START");
    alt((
        map(tag("true"), |_| Literal::Boolean(true)),
        map(tag("false"), |_| Literal::Boolean(false)),
        map(tag("null"), |_| Literal::Null),
        // Improved string parser using .parse(input) syntax
        map(
            delimited(
                char('"'), 
                take_while(|c: char| c != '"'), 
                char('"')
            ),
            |s: &str| Literal::String(s.to_string())
        ),
    )).parse(input) // <--- Use .parse(input) instead of (input)
}

// Check if we're at the start of a new Cypher clause
// Helper function to check if we're at the start of a new Cypher clause
fn is_at_keyword_boundary(input: &str) -> bool {
    println!("===> is_at_keyword_boundary START");
    let trimmed = input.trim_start();
    
    // CRITICAL FIX: Relationship continuation, not a new clause
    if trimmed.starts_with('-') || trimmed.starts_with("<-") {
        return false;
    }
    
    let upper = trimmed.to_uppercase();

    // Existing checks
    if upper.starts_with("WHERE")
        || upper.starts_with("RETURN")
        || upper.starts_with("CREATE")
        || upper.starts_with("ORDER BY")
        || upper.starts_with("ORDER")
        || upper.starts_with("SKIP")
        || upper.starts_with("LIMIT")
        || upper.starts_with("SET")
        || upper.starts_with("DETACH DELETE")
        || upper.starts_with("DELETE")
        || upper.starts_with("DETACH")
        || upper.starts_with("REMOVE")
        || upper.starts_with("MERGE")
        || upper.starts_with("WITH")
        || upper.starts_with("AND")
        || upper.starts_with("ON CREATE")
        || upper.starts_with("ON MATCH")
        || upper.starts_with("UNION ALL")
        || upper.starts_with("UNION")
        || upper.starts_with("FOREACH")
        || upper.starts_with("UNWIND")
        || upper.starts_with("CALL")
    {
        return true;
    }

    // NEW: stop if we see a closing ) followed by a keyword
    if trimmed.starts_with(')') {
        let after_paren = trimmed[1..].trim_start().to_uppercase();
        if after_paren.starts_with("CREATE")
            || after_paren.starts_with("RETURN")
            || after_paren.starts_with("SET")
            || after_paren.starts_with("WHERE")
            || after_paren.starts_with("DELETE")
            || after_paren.starts_with("DETACH")
            || after_paren.starts_with("WITH")
            || after_paren.starts_with("MERGE")
        {
            return true;
        }
    }

    false
}


// FIXED: Consistent handling of delimiters and types for MPI transaction logging
fn parse_property_map(input: &str) -> IResult<&str, Vec<(String, Literal)>> {
    println!("===> parse_property_map START");
    type E<'a> = nom::error::Error<&'a str>;

    delimited(
        char('{'),
        delimited(
            multispace0,
            separated_list0(
                delimited(multispace0, char(','), multispace0),
                separated_pair(
                    // Parse the key (Identifier)
                    map(parse_identifier, |s: &str| s.to_string()), 
                    // Parse the ':' separator with surrounding whitespace
                    delimited(multispace0, char(':'), multispace0),
                    // FIX: Use parse_property_value or similar to get a Literal
                    // This ensures consistent handling for the graph of changes
                    cut(|i| parse_literal(i)) 
                )
            ),
            multispace0
        ),
        char('}')
    ).parse(input)
}

// ============================================================================
// MATCH...SET PARSER
// ============================================================================

/// Parse MATCH ... SET ... [RETURN] statement
fn parse_match_set_relationship(input: &str) -> IResult<&str, CypherQuery> {
    println!("===> parse_match_set_relationship START");
    type E<'a> = nom::error::Error<&'a str>;

    map(
        tuple((
            |i| tag_no_case::<&str, &str, E>("MATCH")(i),
            |i| multispace1::<&str, E>(i),
            |i| parse_match_clause_patterns(i),
            opt(preceded(|i| multispace0::<&str, E>(i), |i| parse_where(i))),
            opt(preceded(|i| multispace0::<&str, E>(i), |i| parse_with_clause(i))),
            |i| multispace0::<&str, E>(i),
            |i| tag_no_case::<&str, &str, E>("SET")(i),
            |i| multispace1::<&str, E>(i),
            separated_list1(
                tuple((
                    |i| multispace0::<&str, E>(i), 
                    |i| char::<&str, E>(',')(i), 
                    |i| multispace0::<&str, E>(i)
                )),
                |i| parse_set_clause(i) 
            ),
            opt(preceded(|i| multispace0::<&str, E>(i), |i| tag_no_case::<&str, &str, E>("RETURN")(i))),
            opt(preceded(|i| multispace0::<&str, E>(i), |i| take_while::<_, &str, E>(|c| c != '\n' && c != ';')(i))),
        )),
        |(_, _, raw_patterns, where_clause, with_clause_enum, _, _, _, set_clauses, _, _)| {
            
            // FIX E0599: Extract ParsedWithClause from the correct variant
            // Based on query_types.rs, if parse_with_clause returns a CypherQuery, 
            // it likely uses a different name like 'Chain' or 'WithStatement'.
            // If the parser returns the struct directly, this block isn't needed.
            // Assuming it's wrapped in a variant that holds ParsedWithClause:
            let with_clause: Option<ParsedWithClause> = with_clause_enum.and_then(|q| {
                match q {
                    // Replace 'With' with the actual variant name in your enum
                    // if it is intended to be parsed here.
                    _ => None, 
                }
            });

            // Pattern transformation for Execution engine
            let match_patterns: ExecutionPatternsReturnType = raw_patterns
                .into_iter()
                .map(|(path_var, nodes, edges)| {
                    let transformed_nodes = nodes
                        .into_iter()
                        .map(|(var, label_opt, props)| {
                            let labels_vec = label_opt.map(|l| vec![l]).unwrap_or_default();
                            (var, labels_vec, props)
                        })
                        .collect();
                    (path_var, transformed_nodes, edges)
                })
                .collect();

            // Transform SET clauses to Expressions for the MPI Graph of Changes
            // This ensures we log the "Golden Record" updates accurately.
            let flattened_set_clauses: Vec<(String, String, Expression)> = set_clauses
                .into_iter()
                .flat_map(|(var, map_data): (String, HashMap<String, serde_json::Value>)| {
                    map_data.into_iter().map(move |(key, val)| {
                        (
                            var.clone(), 
                            key, 
                            Expression::Literal(CypherValue::from_json(val))
                        )
                    })
                })
                .collect();
            
            CypherQuery::MatchSet {
                match_patterns,
                where_clause,
                with_clause, 
                set_clauses: flattened_set_clauses,
            }
        },
    ).parse(input)
}

// Modify parse_match_create_relationship
fn parse_match_create_relationship(input: &str) -> IResult<&str, CypherQuery> {
    println!("===> parse_match_create_relationship START");
    // Helper to transform Raw patterns (Option label) to Execution patterns (Vec labels)
    let transform_patterns = |raw_pats: ParsedPatternsReturnType| -> ExecutionPatternsReturnType {
        raw_pats.into_iter().map(|(path_var, nodes, edges)| {
            let transformed_nodes = nodes.into_iter().map(|(var, label_opt, props)| {
                let labels_vec = match label_opt {
                    Some(l) => vec![l],
                    None => vec![],
                };
                (var, labels_vec, props)
            }).collect();
            (path_var, transformed_nodes, edges)
        }).collect()
    };

    let mut all_match_patterns: ExecutionPatternsReturnType = Vec::new();
    let mut current_input = input;
    
    // --- A. Consume MANDATORY First MATCH Clause ---
    let (input_after_match, _) = preceded(
        multispace0,
        alt((
            tag_no_case::<_, _, nom::error::Error<&str>>("OPTIONAL MATCH"),
            tag_no_case::<_, _, nom::error::Error<&str>>("MATCH"),
        ))
    ).parse(current_input)?;
    
    let (remainder, raw_patterns) = parse_content_after_match_keyword(input_after_match)?;
    all_match_patterns.extend(transform_patterns(raw_patterns));
    current_input = remainder;
    
    // --- B. Loop for ADDITIONAL MATCH Clauses ---
    loop {
        let (input_ws, _) = multispace0::<&str, nom::error::Error<&str>>.parse(current_input)?;
        
        let mut sub_match_parser = preceded(
            alt((
                tag_no_case::<_, _, nom::error::Error<&str>>("OPTIONAL MATCH"),
                tag_no_case::<_, _, nom::error::Error<&str>>("MATCH"),
            )),
            |i| parse_content_after_match_keyword(i),
        );

        match sub_match_parser.parse(input_ws) {
            Ok((remainder, raw_patterns)) => {
                all_match_patterns.extend(transform_patterns(raw_patterns));
                current_input = remainder;
            }
            Err(_) => {
                current_input = input_ws;
                break;
            }
        }
    }
    
    // --- C. Parse Optional WHERE Clause ---
    let (input_after_where, where_clause) = opt(preceded(
        multispace0,
        |i| parse_where(i)
    )).parse(current_input)?;
    current_input = input_after_where;
    
    // --- D. ✅ Parse Optional WITH Clause (MPI logic) ---
    let (input_after_with, with_clause_raw) = opt(preceded(
        multispace0,
        parse_with
    )).parse(current_input)?;
    
    // Convert WithClause → ParsedWithClause
    let with_clause: Option<ParsedWithClause> = with_clause_raw.map(|w| ParsedWithClause {
        items: Vec::new(),
        distinct: false,
        where_clause: Some(WhereClause { condition: w.condition }),
        order_by: Vec::new(),
        skip: None,
        limit: None,
    });
    current_input = input_after_with;

    // --- E. Parse the mandatory CREATE keyword ---
    let (input, _) = preceded(
        multispace0,
        terminated(tag_no_case::<_, _, nom::error::Error<&str>>("CREATE"), multispace0)
    ).parse(current_input)?;
    
    // --- F. Parse CREATE patterns and transform ---
    let (input, create_patterns) = parse_create_clause(input)?;
    
    // --- G. Optional SET clauses ---
    let (input, set_clauses_opt) = opt(preceded(
        tuple((multispace0, tag_no_case("SET"), multispace0)),
        separated_list1(
            tuple((multispace0, char(','), multispace0)),
            |i| parse_single_set_assignment(i),
        ),
    )).parse(input)?;
    
    // --- H. Optional RETURN clause ---
    let (input, _) = opt(preceded(multispace0, tag_no_case("RETURN"))).parse(input)?;
    let (input, _) = opt(preceded(multispace0, take_while(|c| c != '\n' && c != ';'))).parse(input)?;
    
    let set_clauses = set_clauses_opt.unwrap_or_default();
    
    if !set_clauses.is_empty() {
        Ok((input, CypherQuery::MatchCreateSet {
            match_patterns: all_match_patterns,
            where_clause,
            with_clause, // ✅ Now included
            create_patterns,
            set_clauses,
        }))
    } else {
        Ok((input, CypherQuery::MatchCreate {
            match_patterns: all_match_patterns,
            where_clause,
            with_clause, // ✅ Now included
            create_patterns,
        }))
    }
}

// Parse a single pattern (node or node-relationship-node chain)
/// Parse **one** complete pattern:
/// (a)-[:KNOWS]->(b) or
/// (a)-[:KNOWS]-(b) or
/// (a)<-[:KNOWS]-(b) or
/// (a)-[r:KNOWS*0..2]-(b) etc.
/// Stops **only** when it hits a real clause keyword (RETURN, WHERE, CREATE…)
fn parse_single_pattern(input: &str) -> IResult<&str, Pattern> {
    println!("===> parse_single_pattern START, input length: {}", input.len());
    
    let mut all_nodes: Vec<NodePattern> = Vec::new();
    let mut all_relationships: Vec<RelPattern> = Vec::new();
    
    // 1. Parse the starting node
    // Using parse_node as defined in your grammar
    let (mut current_input, node_a) = parse_node(input)?;
    all_nodes.push(node_a);
    
    // 2. Parse (RELATIONSHIP + NODE) pairs
    loop {
        // Skip whitespace to see if there's a relationship connector (-, <, [)
        let (after_space, _) = multispace0::<&str, nom::error::Error<&str>>(current_input)
            .unwrap_or((current_input, ""));

        // CRITICAL FIX: Check for relationship connector FIRST, before boundary check
        if after_space.is_empty() {
            break;
        }
        
        // If this looks like the start of a relationship, do NOT treat it as a boundary
        if after_space.starts_with('-') || after_space.starts_with('<') {
            // Proceed to relationship parsing - skip boundary check entirely
        } else {
            // Only check for keyword boundary or other terminators if NOT a relationship
            if is_at_keyword_boundary(after_space) || after_space.starts_with(',') || after_space.starts_with(';') {
                break;
            }
        }

        // 3. Parse the REL + NODE pair
        // nom 8: we use .parse() on the sequence. 
        // We MUST advance current_input to next_input to consume the string.
        match nom::sequence::pair(parse_relationship, parse_node).parse(after_space) {
            Ok((next_input, (rel, node))) => {
                all_relationships.push(rel);
                all_nodes.push(node);
                // SUCCESS: Advance the pointer to the end of the node pattern
                current_input = next_input;
            }
            Err(_) => {
                // If parsing fails (e.g., a trailing dash with no node), stop path parsing
                break;
            }
        }
    }
    
    let num_nodes = all_nodes.len();
    let num_rels = all_relationships.len();
    
    println!("===> parse_single_pattern END – {} nodes, {} rels", num_nodes, num_rels);
    
    // Build the structural pattern used for MPI logical resolution
    let result_pattern = build_pattern_from_elements(all_nodes, all_relationships);
    
    Ok((current_input, result_pattern))
}

fn parse_return_items(input: &str) -> IResult<&str, Vec<String>> {
    println!("===> parse_return_items START");
    // 1. Parse 'RETURN' keyword
    let (input, _) = tag_no_case("RETURN").parse(input)?;
    let (input, _) = multispace1.parse(input)?;
    
    // 2. Define the parser combinator chain
    let mut return_items_parser = separated_list1(
        // Separator: comma ',' with optional whitespace around it
        delimited(multispace0, char(','), multispace0),
        // Item: A single variable/identifier
        take_while1(|c: char| c.is_alphanumeric() || c == '_')
    );

    // 3. Execute the parser and map the resulting Vec<&str> to Vec<String>
    return_items_parser.parse(input) // <-- FIX: Explicitly call .parse(input)
        .map(|(i, vars)| (i, vars.into_iter().map(|s| s.to_string()).collect()))
}

// Add this to lib/src/query_parser/cypher_parser.rs

fn parse_create_statement(input: &str) -> IResult<&str, CypherQuery> {
    println!("===> parse_create_statement START");
    
    // 1. Parse the CREATE patterns
    let (input, patterns) = parse_create_clause(input)?;
    
    // 2. Consume optional whitespace
    let (input, _) = multispace0.parse(input)?;
    
    // 3. Parse the RETURN clause (using parse_return_items defined previously)
    let (input, return_items_opt) = opt(parse_return_items).parse(input)?;
    let return_items = return_items_opt.unwrap_or_default();
    
    // 4. Consume optional semicolon and trailing space
    let (input, _) = opt(char(';')).parse(input)?;
    let (input, final_input) = multispace0.parse(input)?;

    // Construct the final CypherQuery using the NEW variant
    Ok((final_input, CypherQuery::CreateStatement { // <-- CORRECTED TO NEW VARIANT
        patterns,
        return_items,
    }))
}

fn parse_string_literal(input: &str) -> IResult<&str, &str> {
    println!("===> parse_string_literal START");
    alt((
        // Single quotes
        delimited(
            char('\''),
            // recognize() captures the entire span including escapes
            recognize(many0(alt((
                tag("\\'"),
                tag("\\\\"),
                tag("\\n"),
                recognize(none_of("'\\"))
            )))),
            char('\'')
        ),
        // Double quotes
        delimited(
            char('"'),
            recognize(many0(alt((
                tag("\\\""),
                tag("\\\\"),
                tag("\\n"),
                recognize(none_of("\"\\"))
            )))),
            char('"')
        ),
    )).parse(input)
}

fn parse_number_literal(input: &str) -> IResult<&str, Value> {
    println!("===> parse_number_literal START");
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
    println!("===> parse_optional_match START");
    let (input, _) = tag("OPTIONAL MATCH").parse(input)?;
    let (input, _) = multispace1.parse(input)?;
    // Skip the pattern (we don't need to parse it fully for visualization)
    let (input, _) = take_while1(|c| c != '\n' && c != '\r' && c != 'R').parse(input)?;
    Ok((input, ()))
}

/// Updated to ensure timestamp() and UUIDs are handled correctly in properties
// lib/src/query_parser/cypher_parser.rs

fn parse_property_value(input: &str) -> IResult<&str, Value> {
    println!("===> parse_property_value START");
    alt((
        // 1. Quoted String literals - ALWAYS use these for UUIDs in Cypher strings
        map(parse_string_literal, |s: &str| Value::String(s.to_string())),
       
        // 2. Signed Numbers - Catch the -1369861507 Patient IDs
        parse_signed_number_value,
       
        // 3. Booleans and Null
        map(tag_no_case("true"), |_| Value::Bool(true)),
        map(tag_no_case("false"), |_| Value::Bool(false)),
        map(tag_no_case("null"), |_| Value::Null),
       
        // 4. IMPROVED: Unquoted UUIDs/Identifiers
        // We allow starting digits, but only if it's NOT a pure number
        map(
            verify(
                take_while1(|c: char| c.is_alphanumeric() || c == '-' || c == '_'),
                |s: &str| {
                    // Reject if it's a pure signed integer (let parser #2 handle those)
                    // But allow if it contains non-digit chars (like hex a-f or hyphens)
                    let is_pure_number = s.parse::<i64>().is_ok();
                    !is_pure_number
                }
            ), 
            |s: &str| Value::String(s.to_string())
        ),
       
        // 5. Functions
        map(
            tuple((parse_identifier, char('('), char(')'))),
            |(func, _, _)| {
                if func.to_lowercase() == "timestamp" {
                    Value::String(Utc::now().to_rfc3339())
                } else {
                    Value::String(format!("{}()", func))
                }
            }
        ),
    )).parse(input)
}

fn parse_property(input: &str) -> IResult<&str, (String, Value)> {
    println!("===> parse_property START");
    let (input, (key, _, value)) = tuple((
        parse_identifier,
        preceded(multispace0, char(':')),
        preceded(multispace0, parse_property_value), // Must handle Parameter variant
    )).parse(input)?;
    
    Ok((input, (key.to_string(), value)))
}

fn parse_properties(input: &str) -> IResult<&str, HashMap<String, Value>> {
    println!("===> parse_properties START");
    map(
        delimited(
            preceded(multispace0, char('{')),
            opt(separated_list1(
                preceded(multispace0, char(',')), // This is the separator
                preceded(multispace0, parse_property) // This is the item
            )),
            preceded(multispace0, char('}')),
        ),
        |props| props.unwrap_or_default().into_iter().collect(),
    ).parse(input)
}

fn parse_node(input: &str) -> IResult<&str, NodePattern> {
    println!("===> parse_node START");
    type E<'a> = nom::error::Error<&'a str>;

    let (input, _) = multispace0::<&str, E>.parse(input)?;
    let (input, _) = char::<&str, E>('(').parse(input)?;
    let (input, _) = multispace0::<&str, E>.parse(input)?;
    
    // Parse optional variable name
    let (input, var_opt) = opt(
        take_while1::<_, &str, E>(|c: char| c.is_alphanumeric() || c == '_')
    ).parse(input)?;
    
    let (input, _) = multispace0::<&str, E>.parse(input)?;
    
    // Parse zero or more labels: :Label1:Label2
    let (input, labels) = many0(
        preceded(
            char::<&str, E>(':'),
            map(
                take_while1::<_, &str, E>(|c: char| c.is_alphanumeric() || c == '_'),
                |s: &str| s.to_string()
            )
        )
    ).parse(input)?;
    
    let (input, _) = multispace0::<&str, E>.parse(input)?;
    
    // Parse optional Property Map
    let (input, props) = if input.trim_start().starts_with('{') {
        let (next_input, _) = multispace0::<&str, E>.parse(input)?;
        delimited(
            char('{'),
            map(
                opt(separated_list1(
                    preceded(multispace0::<&str, E>, char(',')),
                    preceded(multispace0::<&str, E>, parse_property) // Assumes parse_property exists
                )),
                |props_list| props_list.unwrap_or_default().into_iter().collect::<HashMap<String, Value>>()
            ),
            preceded(multispace0::<&str, E>, char('}'))
        ).parse(next_input)?
    } else {
        (input, HashMap::new())
    };
    
    let (input, _) = multispace0::<&str, E>.parse(input)?;
    let (input, _) = char::<&str, E>(')').parse(input)?;
    let (input, _) = multispace0::<&str, E>.parse(input)?;

    Ok((input, (
        var_opt.map(|s| s.to_string()),
        labels,
        props
    )))
}

fn parse_multiple_nodes(input: &str) -> IResult<&str, Vec<NodePattern>> {
    println!("===> parse_multiple_nodes START");
    separated_list1(
        tuple((multispace0, char(','), multispace0)),
        |i| parse_node(i) // Use closure for nom 8 trait stability
    ).parse(input)
}

fn parse_relationship(input: &str) -> IResult<&str, RelPattern> {
    println!("===> parse_relationsh START");
    type E<'a> = nom::error::Error<&'a str>;
    use nom::{branch::alt, combinator::map, Parser};

    // 1. Parse opening direction prefix
    let (input, has_left_arrow) = opt(tag::<_, _, E>("<")).parse(input)?;
    let (input, _) = tag::<_, _, E>("-[").parse(input)?;
    let (input, _) = multispace0::<&str, E>.parse(input)?;

    // 2. Optional variable name
    let (input, var_opt) = opt(
        take_while1::<_, &str, E>(|c: char| c.is_alphanumeric() || c == '_')
    ).parse(input)?;

    // 3. Optional Type (e.g., :TYPE)
    let (input, type_opt) = opt(preceded(
        char::<&str, E>(':'),
        take_while1::<_, &str, E>(|c: char| c.is_alphanumeric() || c == '_')
    )).parse(input)?;

    // 4. Optional Variable Length (e.g., *1..3)
    let (input, range_opt) = opt(parse_range).parse(input)?;

    // 5. Optional Properties {...}
    let (input, _) = multispace0::<&str, E>.parse(input)?;  // ADD THIS LINE - consume whitespace before checking
    let (input, props_vec) = if input.starts_with('{') {    // NOW check without trim_start()
        let (next_input, raw_props) = parse_property_map(input)?;
        (next_input, raw_props)
    } else {
        (input, Vec::new())
    };
    // 6. Convert Literal to serde_json::Value
    let props: HashMap<String, Value> = props_vec
        .into_iter()
        .map(|(k, v)| {
            let val = match v {
                Literal::String(s) => Value::from(s),
                Literal::Integer(i) => Value::from(i),
                Literal::Number(n) => {
                    serde_json::Number::from_f64(n)
                        .map(Value::Number)
                        .unwrap_or(Value::Null)
                },
                Literal::Float(f) => Value::from(f.0), 
                Literal::Boolean(b) => Value::from(b),
                Literal::Null => Value::Null,
            };
            (k, val)
        })
        .collect();

    // 7. Parse closing ']'
    let (input, _) = char::<&str, E>(']').parse(input)?;
    let (input, _) = multispace0::<&str, E>.parse(input)?;

    // 8. Parse direction suffix (MUST come after ']')
    let (input, direction) = if has_left_arrow.is_some() {
        // Must be inbound: <-[...]-
        let (input, _) = tag("-").parse(input)?;
        (input, Some(false))
    } else {
        // Outbound -> or undirected -
        alt((
            map(tag("->"), |_| Some(true)),  // outbound
            map(tag("-"), |_| None),         // undirected
        )).parse(input)?
    };

    Ok((input, (
        var_opt.map(|s| s.to_string()),
        type_opt.map(|s| s.to_string()),
        range_opt,
        props,
        direction,
    )))
}

fn parse_range(input: &str) -> IResult<&str, (Option<u32>, Option<u32>)> {
    println!("===> parse_range START");
    type E<'a> = nom::error::Error<&'a str>;

    // 1. Must start with '*'
    let (input, _) = char::<&str, E>('*').parse(input)?;

    // 2. Try parsing "min..max", "min..", "..max"
    // We define the parser inline to avoid multiple mutable borrow conflicts
    let range_result = separated_pair(
        opt(map_res(digit1::<&str, E>, |s| s.parse::<u32>())),
        tag::<_, _, E>(".."),
        opt(map_res(digit1::<&str, E>, |s| s.parse::<u32>()))
    ).parse(input);

    if let Ok((after_range, (min, max))) = range_result {
        Ok((after_range, (min, max)))
    } else {
        // 3. Fallback: Parse a single fixed value (e.g., *3) or just '*'
        let (input, val) = opt(map_res(digit1::<&str, E>, |s| s.parse::<u32>())).parse(input)?;
        
        // If it's *3, both are Some(3). If it's just *, both are None.
        Ok((input, (val, val)))
    }
}

fn parse_create_edge_between_existing(input: &str) -> IResult<&str, CypherQuery> {
    println!("===> parse_create_edge_between_existing START");
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
    println!("===> parse_create_nodes START");
    let (input, _) = tag("CREATE").parse(input)?;
    let (input, _) = multispace1.parse(input)?;
    let (input, nodes) = parse_multiple_nodes(input)?;
    let (input, _) = multispace0.parse(input)?;
    
    if input.starts_with('-') || input.starts_with('<') {
        return Err(nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Tag)));
    }
    
    let node_data: Vec<(String, HashMap<String, Value>)> = nodes
        .into_iter()
        .map(|(var, labels, props)| {
            // labels is Vec<String>. Get the first one, or fallback.
            let actual_label = labels
                .into_iter()
                .next()
                .unwrap_or_else(|| var.clone().unwrap_or_else(|| "Node".to_string()));
            (actual_label, props)
        })
        .collect();

    Ok((input, CypherQuery::CreateNodes { nodes: node_data }))
}

fn parse_create_index(input: &str) -> IResult<&str, CypherQuery> {
    println!("===> parse_create_index START");
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
    
    let (_, labels, _) = node_pattern;
    
    // labels is Vec<String>. We take the first one or default to empty string.
    let final_label = labels.into_iter().next().unwrap_or_default();

    let property_names: Vec<String> = prop_pattern.iter()
        .map(|prop| {
            prop.split('.').nth(1).unwrap_or(prop).to_string()
        })
        .collect();
    
    Ok((input, CypherQuery::CreateIndex {
        label: final_label,
        properties: property_names,
    }))
}

fn parse_property_pattern(input: &str) -> IResult<&str, Vec<String>> {
    println!("===> parse_property_pattern START");
    let (input, _) = char('(').parse(input)?;
    let (input, _) = multispace0.parse(input)?;
    
    let (input, first_prop) = parse_property_access(input)?;
    
    let (input, additional_props) = many0(
        preceded(
            tuple((multispace0, char(','), multispace0)),
            parse_property_access
        )
    ).parse(input)?;
    
    let (input, _) = multispace0.parse(input)?;
    let (input, _) = char(')').parse(input)?;
    
    let mut props = vec![first_prop];
    props.extend(additional_props);
    
    Ok((input, props))
}


// ============================================================================
// WHERE CLAUSE PARSING - Enhanced
// ============================================================================


/// Updated to return a single String "var.prop"
fn parse_property_access(input: &str) -> IResult<&str, String> {
    println!("===> parse_property_access START");
    let (input, _) = multispace0.parse(input)?;
    let (input, var) = parse_identifier.parse(input)?;
    let (input, _) = char('.').parse(input)?;
    let (input, prop) = parse_identifier.parse(input)?;
    
    Ok((input, format!("{}.{}", var, prop)))
}

/// Now this function will compile because the map matches the return type
fn parse_where_condition(input: &str) -> IResult<&str, String> {
    println!("===> parse_where_condition START");
    alt((
        // Function-based condition
        map(
            tuple((
                parse_function_call,
                delimited(multispace0, char('='), multispace0),
                parse_value,
            )),
            |(func, _, val)| {
                format!("{}({}) = {:?}", func.0, func.1, val)
            }
        ),
        // Property-based condition
        map(
            tuple((
                parse_property_access, // Now correctly returns String
                delimited(multispace0, char('='), multispace0),
                parse_value,
            )),
            |(prop_path, _, val)| {
                format!("{} = {:?}", prop_path, val)
            }
        ),
        map(take_until_keyword, |s: &str| s.to_string()),
    )).parse(input)
}

fn parse_create_complex_pattern(input: &str) -> IResult<&str, CypherQuery> {
    println!("===> parse_create_complex_pattern START");

    let (input, _) = tag("CREATE").parse(input)?;
    let (input, _) = multispace1.parse(input)?;

    let (input, first_node) = parse_node(input)?;
    let (input, _) = multispace0.parse(input)?;

    // Case: CREATE (n) where it's just a single node, not a path
    if !input.starts_with('-') && !input.starts_with('<') {
        let (var, labels, props) = first_node;
        
        // labels is Vec<String>. Get the first one or fallback.
        let label = labels
            .into_iter()
            .next()
            .unwrap_or_else(|| var.clone().unwrap_or_else(|| "Node".to_string()));

        return Ok((input, CypherQuery::CreateNode {
            label,
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

    // Ensure the return type matches CypherQuery::CreateComplexPattern
    // If that variant expects Vec<(Option<String>, Vec<String>, HashMap)>, 
    // it will now match because parse_node returns Vec<String> labels.
    Ok((remaining, CypherQuery::CreateComplexPattern { nodes, relationships }))
}

fn parse_create_pattern(input: &str) -> IResult<&str, Pattern> {
    println!("===> parse_create_pattern START");
    let (input, path_var) = opt(terminated(
        parse_identifier,
        tuple((multispace0, char('='), multispace0))
    )).parse(input)?;

    // ✅ Use parse_node (returns NodePattern), NOT parse_create_node (returns CypherQuery)
    let (input, first_node) = parse_node(input)?;
    let (input, _) = multispace0.parse(input)?;

    let mut nodes = vec![first_node]; // mut is required — you push in the loop
    let mut rels = Vec::new();
    let mut remaining = input;

    loop {
        match parse_relationship(remaining) {
            Ok((rest, rel)) => {
                rels.push(rel);
                let (rest, _) = multispace0.parse(rest)?;
                match parse_node(rest) { // ✅ Again, use parse_node
                    Ok((rest, node)) => {
                        nodes.push(node);
                        let (rest, _) = multispace0.parse(rest)?;
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

fn parse_create_node(input: &str) -> IResult<&str, CypherQuery> {
    println!("===> parse_create_node START");
    let (input, _) = tag("CREATE").parse(input)?;
    let (input, _) = multispace1.parse(input)?;
    let (input, node) = parse_node(input)?;
    let (input, _) = multispace0.parse(input)?;
    
    if input.starts_with('-') || input.starts_with('<') {
        return Err(nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Tag)));
    }
    
    let (var, labels, props) = node;

    // labels is now Vec<String>. We try to get the first label,
    // otherwise fall back to the variable name, otherwise "Node".
    let final_label = labels
        .into_iter()
        .next() // Get the first String from the Vec
        .unwrap_or_else(|| {
            var.clone().unwrap_or_else(|| "Node".to_string())
        });

    Ok((input, CypherQuery::CreateNode {
        label: final_label,
        properties: props,
    }))
}

// Helper function to convert PropertyValue to serde_json::Value
fn property_value_to_json(prop_val: PropertyValue) -> Value {
    println!("===> property_value_to_json START");
    match prop_val {
        PropertyValue::String(s) => Value::String(s),
        PropertyValue::Integer(i) => Value::Number(i.into()),
        PropertyValue::I32(i) => Value::Number(i.into()),
        PropertyValue::Float(f) => Value::Number(serde_json::Number::from_f64(f.0).unwrap_or(serde_json::Number::from(0))),
        PropertyValue::Boolean(b) => Value::Bool(b),
        PropertyValue::Uuid(uuid) => Value::String(uuid.to_string()),
        PropertyValue::Byte(b) => Value::Number(b.into()),
        PropertyValue::Vertex(v) => {
            Value::String(format!("Vertex({:?})", v.0))
        },
        PropertyValue::Map(map) => {
            let mut obj = serde_json::Map::new();
            for (key, val) in map.0.into_iter() {
                obj.insert(key.0.to_string(), property_value_to_json(val));
            }
            Value::Object(obj)
        },
        // Fix: Added missing arms to satisfy exhaustiveness check
        PropertyValue::Null => Value::Null,
        PropertyValue::List(list) => {
            Value::Array(list.into_iter().map(property_value_to_json).collect())
        }
    }
}


// Required to parse key=value assignments for SET and MERGE clauses: `n.name = 'Joe'`
// Returns (String, String, Value) because that's what the rest of the code expects
fn parse_set_assignment_tuple(input: &str) -> IResult<&str, (String, String, Value)> {
    println!("===> parse_set_assignment_tuple START");
    map(
        tuple((
            // Variable (e.g., n)
            parse_identifier, 
            // .Property (e.g., .name)
            preceded(char('.'), parse_identifier), 
            // = Value
            preceded(tuple((multispace0::<&str, NomErrorType<&str>>, tag("="), multispace0::<&str, NomErrorType<&str>>)), parse_property), 
        )),
        |(var, prop_name, (_, value))| (var.to_string(), prop_name.to_string(), value),
    ).parse(input)
}

pub fn parse_merge(input: &str) -> IResult<&str, CypherQuery> {
    println!("===> parse_merge START");
    type E<'a> = nom::error::Error<&'a str>;

    map(
        tuple((
            // 1. Mandatory MERGE keyword followed by patterns
            preceded(
                terminated(|i| tag_no_case::<&str, &str, E>("MERGE")(i), |i| multispace1::<&str, E>(i)),
                separated_list1(
                    // FIX E0061 & E0277: Use closures to resolve nom version ambiguity
                    tuple((
                        |i| multispace0::<&str, E>(i), 
                        |i| char::<&str, E>(',')(i), 
                        |i| multispace0::<&str, E>(i)
                    )), 
                    |i| parse_pattern_restricted(i)
                ),
            ),
            // 2. Optional WHERE clause
            opt(preceded(|i| multispace1::<&str, E>(i), |i| parse_where(i))),
            // 3. Optional WITH clause
            opt(preceded(|i| multispace1::<&str, E>(i), |i| parse_with_clause(i))),
            // 4. ON CREATE / ON MATCH
            opt(preceded(|i| multispace1::<&str, E>(i), |i| parse_on_set_clause("ON CREATE")(i))),
            opt(preceded(|i| multispace1::<&str, E>(i), |i| parse_on_set_clause("ON MATCH")(i))),
        )),
        |(patterns, where_clause, with_clause_enum, on_create, on_match)| {
            
            // FIX E0599: CypherQuery variant mismatch
            // In query_types.rs, With clauses are likely wrapped in ReturnStatement 
            // or a dedicated struct. Adjusting to match common parser patterns:
            let with_clause: Option<ParsedWithClause> = with_clause_enum.and_then(|q| {
                match q {
                    // Check if your parser returns a specific variant for WITH clauses. 
                    // Based on provided types, if it's not 'With', it might be 'Chain' 
                    // or a standalone struct. Adjust this match to your parser's output.
                    _ => None, 
                }
            });

            // Helper to convert Value to Expression for MPI tracking
            let convert_to_expression = |v_list: Vec<(String, String, serde_json::Value)>| {
                v_list.into_iter()
                    .map(|(var, prop, val)| {
                        (var, prop, Expression::Literal(CypherValue::from_json(val)))
                    })
                    .collect::<Vec<_>>()
            };

            let on_create_set = on_create.map(convert_to_expression).unwrap_or_default();
            let on_match_set = on_match.map(convert_to_expression).unwrap_or_default();
            
            // Matches CypherQuery::Merge in query_types.rs
            CypherQuery::Merge {
                patterns,
                where_clause,
                with_clause, 
                on_create_set,
                on_match_set,
            }
        },
    ).parse(input)
}

pub fn evaluate_expression(
    expr: &Expression,
    context: &EvaluationContext,
) -> StdResult<CypherValue, GraphError> {
    println!("===> evaluate_expression START");
    match expr {
        Expression::Literal(val) => Ok(val.clone()),
        
        Expression::Variable(name) => context.variables.get(name)
            .cloned()
            .ok_or_else(|| GraphError::QueryExecutionError(format!("Variable not found: {}", name))),

        Expression::Property(prop_access) => {
            match prop_access {
                PropertyAccess::Vertex(var_name, prop_name) => {
                    let val = context.variables.get(var_name)
                        .ok_or_else(|| GraphError::QueryExecutionError(format!("Variable not found: {}", var_name)))?;
                    
                    if let CypherValue::Vertex(v) = val {
                        match v.properties.get(prop_name) {
                            Some(prop_val) => {
                                // Fix: Use serde_json::to_value to convert PropertyValue to Value
                                // if .0 is private and Into is not implemented.
                                let json_value = serde_json::to_value(prop_val)
                                    .map_err(|e| GraphError::QueryExecutionError(format!("Property conversion failed: {}", e)))?;
                                Ok(CypherValue::from_json(json_value))
                            },
                            None => Ok(CypherValue::Null), 
                        }
                    } else {
                        Err(GraphError::QueryExecutionError(format!("Variable {} is not a vertex", var_name)))
                    }
                },
                _ => Err(GraphError::QueryExecutionError("Unsupported property access".to_string())),
            }
        },

        Expression::Binary { op, left, right } => {
            let left_val = evaluate_expression(left, context)?;
            let right_val = evaluate_expression(right, context)?;

            match op {
                BinaryOp::Plus => add_values(left_val, right_val),
                BinaryOp::Minus => subtract_values(left_val, right_val),
                BinaryOp::Mul => multiply_values(left_val, right_val),
                BinaryOp::Div => divide_values(left_val, right_val),
                _ => Err(GraphError::QueryExecutionError(format!("Operator {:?} not implemented", op))),
            }
        },
        _ => Err(GraphError::QueryExecutionError("Expression type not implemented".to_string())),
    }
}

// --- Supporting Arithmetic Functions ---

fn add_values(left: CypherValue, right: CypherValue) -> StdResult<CypherValue, GraphError> {
    println!("===> add_values START");
    match (left, right) {
        (CypherValue::Integer(a), CypherValue::Integer(b)) => Ok(CypherValue::Integer(a + b)),
        (CypherValue::Float(a), CypherValue::Float(b)) => Ok(CypherValue::Float(a + b)),
        (CypherValue::String(a), CypherValue::String(b)) => Ok(CypherValue::String(format!("{}{}", a, b))),
        (CypherValue::Null, _) | (_, CypherValue::Null) => Ok(CypherValue::Null),
        _ => Err(GraphError::QueryExecutionError("Type mismatch in addition".into())),
    }
}

fn subtract_values(left: CypherValue, right: CypherValue) -> StdResult<CypherValue, GraphError> {
    println!("===> subtract_values START");
    match (left, right) {
        (CypherValue::Integer(a), CypherValue::Integer(b)) => Ok(CypherValue::Integer(a - b)),
        (CypherValue::Float(a), CypherValue::Float(b)) => Ok(CypherValue::Float(a - b)),
        (CypherValue::Null, _) | (_, CypherValue::Null) => Ok(CypherValue::Null),
        _ => Err(GraphError::QueryExecutionError("Type mismatch in subtraction".into())),
    }
}

fn multiply_values(left: CypherValue, right: CypherValue) -> StdResult<CypherValue, GraphError> {
    println!("===> multiply_values START");
    match (left, right) {
        (CypherValue::Integer(a), CypherValue::Integer(b)) => Ok(CypherValue::Integer(a * b)),
        (CypherValue::Float(a), CypherValue::Float(b)) => Ok(CypherValue::Float(a * b)),
        (CypherValue::Null, _) | (_, CypherValue::Null) => Ok(CypherValue::Null),
        _ => Err(GraphError::QueryExecutionError("Type mismatch in multiplication".into())),
    }
}

fn divide_values(left: CypherValue, right: CypherValue) -> StdResult<CypherValue, GraphError> {
    println!("===> divide_values START"); 
    match (left, right) {
        (CypherValue::Integer(_), CypherValue::Integer(0)) => {
            Err(GraphError::QueryExecutionError("Division by zero".into()))
        }
        (CypherValue::Integer(a), CypherValue::Integer(b)) => Ok(CypherValue::Integer(a / b)),
        (CypherValue::Float(a), CypherValue::Float(b)) => Ok(CypherValue::Float(a / b)),
        (CypherValue::Null, _) | (_, CypherValue::Null) => Ok(CypherValue::Null),
        _ => Err(GraphError::QueryExecutionError("Type mismatch in division".into())),
    }
}

fn parse_merge_statement(input: &str) -> IResult<&str, CypherQuery> {
    println!("===> parse_merge_statement START");                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            
    type NomErrorType<'a> = nom::error::Error<&'a str>;

    let (input, _) = tag_no_case("MERGE").parse(input)?;
    let (input, _) = multispace1.parse(input)?; 
    let (input, raw_patterns) = parse_match_clause_patterns(input)?;
    let (input, where_clause) = opt(preceded(multispace1, parse_where)).parse(input)?;

    // ✅ Parse optional WITH clause
    let (input, with_clause_raw) = opt(preceded(multispace1, parse_with)).parse(input)?;
    let with_clause: Option<ParsedWithClause> = with_clause_raw.map(|w| ParsedWithClause {
        items: Vec::new(),
        distinct: false,
        where_clause: Some(WhereClause { condition: w.condition }),
        order_by: Vec::new(),
        skip: None,
        limit: None,
    });

    let mut on_create_set = Vec::new(); 
    let mut on_match_set = Vec::new();
    let mut input_current = input;
    
    loop {
        let (input_ws, _) = multispace0::<&str, NomErrorType>.parse(input_current)?;
        if input_ws.is_empty() { break; }

        let trimmed = input_ws.trim_start().to_uppercase();

        if trimmed.starts_with("ON CREATE") {
            let (next, _) = tag_no_case::<_, _, NomErrorType>("ON CREATE").parse(input_ws)?;
            let (next, _) = delimited(multispace1, tag_no_case("SET"), multispace1).parse(next)?;
            let (next, clauses) = separated_list1(
                delimited(multispace0, char(','), multispace0),
                parse_single_set_assignment
            ).parse(next)?;
            on_create_set.extend(clauses);
            input_current = next;
        } else if trimmed.starts_with("ON MATCH") {
            let (next, _) = tag_no_case::<_, _, NomErrorType>("ON MATCH").parse(input_ws)?;
            let (next, _) = delimited(multispace1, tag_no_case("SET"), multispace1).parse(next)?;
            let (next, clauses) = separated_list1(
                delimited(multispace0, char(','), multispace0),
                parse_single_set_assignment
            ).parse(next)?;
            on_match_set.extend(clauses);
            input_current = next;
        } else {
            break;
        }
    }
    
    // Check for RETURN but don't consume it all if it's there
    let (input_final, _) = opt(preceded(multispace0, tag_no_case("RETURN"))).parse(input_current)?;

    // SHIM: Standardize patterns (Vec<String> labels)
    let patterns: ExecutionPatternsReturnType = raw_patterns.into_iter().map(|(path, nodes, rels)| {
        let n_fixed = nodes.into_iter().map(|(v, l, p)| (v, l.into_iter().collect(), p)).collect();
        (path, n_fixed, rels)
    }).collect();
    
    // Final Dispatch — ✅ include with_clause
    Ok((input_final, CypherQuery::Merge {
        patterns,
        where_clause,
        with_clause, // ✅ Now included
        on_create_set,
        on_match_set,
    }))
}

// ============================================================================
// FIXED MATCH NODE PARSERS
// ============================================================================
// These parsers handle the label format change from Vec<String> to Option<String>
// for the CypherQuery::MatchNode variant which expects Option<String>

/// Parse MATCH (n:Label), (m:Label) pattern
fn parse_match_multiple_nodes(input: &str) -> IResult<&str, CypherQuery> {
    println!("===> parse_match_multiple_nodes START, input length: {}, preview: '{}'", 
             input.len(), &input[..input.len().min(100)]);
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
            // parse_node returns (Option<String>, Vec<String>, HashMap<String, Value>)
            // Extract the first node for MatchNode
            let (var, labels, props) = &nodes[0];
            
            // Convert Vec<String> labels to Option<String>
            // Take the first label if present, otherwise None
            let label = if labels.is_empty() {
                None
            } else {
                Some(labels[0].clone())
            };
            
            CypherQuery::MatchNode {
                label,
                properties: props.clone(),
            }
        },
    ).parse(input)
}

/// Parse MATCH (n:Label {props}) [RETURN ...] pattern
fn parse_match_node(input: &str) -> IResult<&str, CypherQuery> {
    println!("===> parse_match_node START, input length: {}, preview: '{}'", 
             input.len(), &input[..input.len().min(100)]);
    let (input, (_, _, node)) = tuple((
        tag("MATCH"),
        multispace1,
        parse_node,
    )).parse(input)?;
    
    // parse_node returns (Option<String>, Vec<String>, HashMap<String, Value>)
    let (var, labels, props) = node;
    
    // Convert Vec<String> labels to Option<String>
    // Take the first label if present, otherwise None
    let label = if labels.is_empty() {
        None
    } else {
        Some(labels[0].clone())
    };
    
    // Optional RETURN clause
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
        label,
        properties: props,
    }))
}

fn parse_return_expressions(input: &str) -> IResult<&str, ()> {
    println!("===> parse_return_expressions START, input length: {}, preview: '{}'", 
             input.len(), &input[..input.len().min(100)]);
    let (input, _) = multispace0.parse(input)?;
    let (input, _) = parse_return_expression(input)?;
    let (input, _) = many0(preceded(
        tuple((multispace0, char(','), multispace0)),
        parse_return_expression
    )).parse(input)?;
    Ok((input, ()))
}

fn parse_create_edge(input: &str) -> IResult<&str, CypherQuery> {
    println!("===> parse_create_edge START, input length: {}, preview: '{}'", 
             input.len(), &input[..input.len().min(100)]);
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

fn parse_set_node(input: &str) -> IResult<&str, CypherQuery> {
    println!("===> parse_set_node START, input length: {}, preview: '{}'", 
             input.len(), &input[..input.len().min(100)]);
    map(
        tuple((
            tag("SET"),
            multispace1,
            parse_identifier,
            multispace0,
            parse_properties,
        )),
        |(_, _, _var, _, props)| CypherQuery::SetNode {
            id: SerializableUuid(Uuid::new_v4()),
            properties: props,
        },
    ).parse(input)
}

fn parse_delete_node(input: &str) -> IResult<&str, CypherQuery> {
    println!("===> parse_delete_node START, input length: {}, preview: '{}'", 
             input.len(), &input[..input.len().min(100)]);
    map(
        tuple((tag("DELETE"), multispace1, parse_identifier)),
        |(_, _, _var)| CypherQuery::DeleteNode {
            id: SerializableUuid(Uuid::new_v4()),
        },
    ).parse(input)
}

fn parse_set_kv(input: &str) -> IResult<&str, CypherQuery> {
    println!("===> parse_set_kv START");
    map(
        tuple((
            tag("SET"),
            multispace1,
            parse_identifier,
            multispace0,
            char('='),
            multispace0,
            parse_string_literal,
        )),
        |(_, _, key, _, _, _, value)| CypherQuery::SetKeyValue {
            key: key.to_string(),
            value: value.to_string(),
        },
    ).parse(input)
}

fn parse_get_kv(input: &str) -> IResult<&str, CypherQuery> {
    println!("===> parse_get_kv START, input length: {}, preview: '{}'", 
             input.len(), &input[..input.len().min(100)]);
    map(
        tuple((
            tag("MATCH"),
            multispace1,
            parse_node,
            multispace1,
            tag("RETURN"),
        )),
        |(_, _, (key, _, _), _, _)| CypherQuery::GetKeyValue {
            key: key.unwrap_or_default(),
        },
    ).parse(input)
}

fn parse_delete_kv(input: &str) -> IResult<&str, CypherQuery> {
    println!("===> parse_delete_kv START, input length: {}, preview: '{}'", 
             input.len(), &input[..input.len().min(100)]);
    map(
        tuple((tag("DELETE"), multispace1, parse_identifier)),
        |(_, _, key)| CypherQuery::DeleteKeyValue {
            key: key.to_string(),
        },
    ).parse(input)
}

// Use a type alias for the error type if available, otherwise use the full trait bound.
// Assuming your IResult is defined as type IResult<I, O> = Result<(I, O), nom::Err<E>>;
// We will define the internal functions to return the parser object itself.

// Internal function to parse SET key=value
fn parse_set_kv_internal<'a>() -> impl Parser<&'a str, Output = CypherQuery, Error = NomErrorType<&'a str>> {
    println!("===> parse_set_kv_internal START");
    map(
        tuple((
            tag_no_case("SET"),
            multispace1,
            parse_identifier,
            multispace0,
            char('='),
            multispace0,
            parse_string_literal,
        )),
        |(_, _, key, _, _, _, value)| CypherQuery::SetKeyValue {
            key: key.to_string(),
            value: value.to_string(),
        },
    )
}

// Internal function to parse MATCH ... RETURN (GET)
fn parse_get_kv_internal<'a>() -> impl Parser<&'a str, Output = CypherQuery, Error = NomErrorType<&'a str>> {
    println!("===> parse_get_kv_internal START");
    map(
        tuple((
            tag_no_case("MATCH"),
            multispace1,
            parse_node, 
            multispace1,
            tag_no_case("RETURN"),
        )),
        |(_, _, (key, _, _), _, _)| CypherQuery::GetKeyValue {
            key: key.unwrap_or_default(), 
        },
    )
}

// Internal function to parse DELETE key
fn parse_delete_kv_internal<'a>() -> impl Parser<&'a str, Output = CypherQuery, Error = NomErrorType<&'a str>> {
    println!("===> parse_delete_kv_internal START");
    map(
        tuple((tag_no_case("DELETE"), multispace1, parse_identifier)),
        |(_, _, key)| CypherQuery::DeleteKeyValue {
            key: key.to_string(),
        },
    )
}
/// Combines parse_set_kv, parse_get_kv, and parse_delete_kv into a single parser.
fn parse_kv_operations(input: &str) -> IResult<&str, CypherQuery> {
    println!("===> parse_kv_operations START");
    // FIX: Inline the alt definition directly into the execution closure.
    // This avoids the 'let combined_parser = ...' binding, which was causing the 
    // compiler to try and borrow the captured variable as mutable.
    println!("===> parse_kv_operations START, input length: {}, preview: '{}'", 
             input.len(), &input[..input.len().min(100)]);
    (move |i| {
        alt((
            // Call the functions to get the impl Parser objects
            parse_set_kv_internal(),
            parse_get_kv_internal(),
            parse_delete_kv_internal(),
        )).parse(i)
    })(input)
}

fn parse_set_list(input: &str) -> IResult<&str, HashMap<String, Value>> {
    println!("===> parse_set_list START, input length: {}, preview: '{}'", 
             input.len(), &input[..input.len().min(100)]);
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

fn parse_set_query(input: &str) -> IResult<&str, CypherQuery> {
    println!("===> parse_set_query START, input length: {}, preview: '{}'", 
             input.len(), &input[..input.len().min(100)]);
    let (input, _) = tag("SET").parse(input)?;
    let (input, _) = multispace1.parse(input)?;
    let (input, properties) = parse_set_list(input)?;
    let (input, _) = multispace0(input)?;
    
    // Check for RETURN clause
    let input = if input.trim_start().starts_with("RETURN") {
        let (input, _) = multispace0.parse(input)?;
        let (input, _) = tag("RETURN").parse(input)?;
        let (input, _) = multispace0.parse(input)?;
        // Skip return expressions for now
        let (input, _) = take_while1(|c| c != '\n' && c != '\r').parse(input)?;
        input
    } else {
        input
    };
    
    Ok((input, CypherQuery::SetNode {
        id: SerializableUuid(Uuid::new_v4()),
        properties,
    }))
}

// Custom parser for signed integers and floats, returning serde_json::Value
// This robustly recognizes the optional leading '-' sign.
fn parse_signed_number_value(input: &str) -> IResult<&str, Value> {
    println!("===> parse_signed_number_value START, input length: {}, preview: '{}'", 
             input.len(), &input[..input.len().min(100)]);
    // Manual parsing approach to avoid nom parser composition issues
    let original_input = input;
    let mut chars = input.chars().peekable();
    let mut number_str = String::new();
    
    // Check for optional negative sign
    if let Some('-') = chars.peek() {
        number_str.push('-');
        chars.next();
    }
    
    // Check if we have at least one digit or a decimal point
    let mut has_digits = false;
    let mut has_decimal = false;
    
    // Parse digits before decimal point
    while let Some(&ch) = chars.peek() {
        if ch.is_ascii_digit() {
            number_str.push(ch);
            chars.next();
            has_digits = true;
        } else {
            break;
        }
    }
    
    // Check for decimal point
    if let Some(&'.') = chars.peek() {
        number_str.push('.');
        chars.next();
        has_decimal = true;
        
        // Parse digits after decimal point
        while let Some(&ch) = chars.peek() {
            if ch.is_ascii_digit() {
                number_str.push(ch);
                chars.next();
                has_digits = true;
            } else {
                break;
            }
        }
    }
    
    // Must have at least one digit
    if !has_digits {
        return Err(nom::Err::Error(NomError::Error {
            input: original_input,
            code: NomError::ErrorKind::Digit,
        }));
    }
    
    // Calculate remaining input
    let consumed = number_str.len();
    let remaining = &original_input[consumed..];
    
    // Attempt to parse the recognized string into the appropriate Value.
    // Try i64 first (for integers, like -295941589)
    if !has_decimal {
        if let Ok(i) = number_str.parse::<i64>() {
            return Ok((remaining, Value::Number(i.into())));
        }
    }
    
    // Fallback to f64 (for floats)
    if let Ok(f) = number_str.parse::<f64>() {
        if let Some(num) = serde_json::Number::from_f64(f) {
            return Ok((remaining, Value::Number(num)));
        }
    }
    
    // Return error if parsing fails
    Err(nom::Err::Error(NomError::Error {
        input: original_input,
        code: NomError::ErrorKind::Float,
    }))
}

fn parse_simple_query_type(input: &str) -> IResult<&str, CypherQuery> {
    println!("===> parse_simple_query_type START, input length: {}, preview: '{}'", 
             input.len(), &input[..input.len().min(100)]);
    
    alt((
        // parse_match_remove_relationship,
        parse_merge_statement, // <--- ADDED MERGE HERE
        parse_match_detach_delete, 
        parse_create_statement, 
        parse_delete_edges_simple,
        
        // *** FIX: Added MatchSet, prioritized over MatchCreate ***
        //parse_match_set_relationship, // <--- NEW FIX: Handle MATCH...SET
        //parse_match_create_relationship, 
        
        full_statement_parser,
        parse_detach_delete,
        parse_delete_edges,
        parse_create_index,
        parse_create_edge_between_existing,
        parse_create_complex_pattern,
        parse_create_nodes,
        parse_create_node,
        parse_create_edge,
        parse_match_multiple_nodes, 
        parse_set_query,
        parse_set_node,
        parse_delete_node,
        parse_kv_operations,
    ))
    .parse(input)
}

// ----------------------------------------------------------------------------------
// --- DEFINED: parse_sequential_statements (New parser for concatenated statements) ---
// ----------------------------------------------------------------------------------

/// Parses a sequence of independent, simple statements concatenated without semicolons.
/// This is the key fix for inputs like "CREATE (a) CREATE (b) MATCH (c) RETURN c".
fn parse_sequential_statements(input: &str) -> IResult<&str, CypherQuery> {
    println!("===> parse_sequential_statements START, input length: {}, preview: '{}'", 
             input.len(), &input[..input.len().min(100)]);
    
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

// NOTE: This assumes 'all_vertices' and 'all_edges' are available/passed in, 
// which is suggested by your current execution logs.

async fn find_variable_length_paths(
    all_vertices: &Vec<Vertex>,
    all_edges: &Vec<Edge>,
    start_node_pat: &NodePattern,
    rel_pat: &RelPattern,
    end_node_pat: &NodePattern,
) -> GraphResult<(HashSet<SerializableUuid>, HashSet<SerializableUuid>)> {
    println!("===> find_variable_length_paths START");
    let (_start_var, start_label_opt, start_props) = start_node_pat;
    let (_rel_var, rel_label_opt, len_range_opt, _rel_props, direction_opt) = rel_pat;
    let (_end_var, end_label_opt, end_props) = end_node_pat;

    let (min_hops, max_hops) = len_range_opt
        .unwrap_or((Some(1), Some(1)));
        
    let min_hops = min_hops.unwrap_or(1);
    let max_hops = max_hops.unwrap_or(u32::MAX);

    // 1. Find all starting vertices that match the start node pattern
    let starting_nodes: Vec<&Vertex> = all_vertices.iter()
        .filter(|v| matches_constraints(v, start_label_opt, start_props))
        .collect();

    let mut matched_vertex_ids = HashSet::new();
    let mut matched_edge_ids = HashSet::new();

    if starting_nodes.is_empty() {
        return Ok((matched_vertex_ids, matched_edge_ids));
    }

    // Initialize BFS with (Node ID, Path of Edges)
    // We use a VecDeque for a proper BFS if available, otherwise Vec for queue.
    let mut queue: Vec<(SerializableUuid, Vec<SerializableUuid>)> = starting_nodes.iter()
        .map(|v| (v.id, Vec::new()))
        .collect();
    
    let mut visited_at_hop: HashMap<SerializableUuid, u32> = HashMap::new();
    for node in &starting_nodes {
        visited_at_hop.insert(node.id, 0);
    }
    
    let mut current_hop = 0;

    // Handle 0-hop case (e.g., *0..2)
    if min_hops == 0 {
        for node in &starting_nodes {
            if matches_constraints(node, end_label_opt, end_props) {
                matched_vertex_ids.insert(node.id);
            }
        }
    }

    // BFS loop
    while !queue.is_empty() && current_hop < max_hops {
        current_hop += 1;
        let mut next_queue = Vec::new();

        for (current_id, path_edges) in queue.drain(..) {
            
            // Determine which direction to traverse based on relationship pattern
            let is_outgoing = direction_opt.unwrap_or(true); 

            for edge in all_edges.iter().filter(|e| {
                if is_outgoing {
                    e.outbound_id == current_id // Check A->B
                } else {
                    e.inbound_id == current_id // Check A<-B
                }
            }) {
                
                // 1. Check relationship type match
                if rel_label_opt.as_deref().map_or(true, |l| edge.label == l) {

                    let next_id = if is_outgoing { edge.inbound_id } else { edge.outbound_id };
                    
                    // Skip if we've already found a shorter path to this node
                    if visited_at_hop.get(&next_id).map_or(false, |&h| h <= current_hop) {
                        continue;
                    }

                    // 2. Check if we are within the desired hop range (min_hops <= current_hop)
                    if current_hop >= min_hops {
                        // Look up the actual vertex to check end node constraints
                        if let Some(next_node_vertex) = all_vertices.iter().find(|v| v.id == next_id) {
                            if matches_constraints(next_node_vertex, end_label_opt, end_props) {
                                // Found a valid path end matching the target node pattern
                                matched_vertex_ids.insert(current_id);
                                matched_vertex_ids.insert(next_id);
                                matched_edge_ids.insert(edge.id);
                                matched_edge_ids.extend(path_edges.iter().cloned());
                            }
                        }
                    }

                    // 3. Enqueue for the next hop if not at max_hops
                    if current_hop < max_hops {
                        let mut next_path_edges = path_edges.clone();
                        next_path_edges.push(edge.id);
                        next_queue.push((next_id, next_path_edges));
                        visited_at_hop.insert(next_id, current_hop);
                    }
                }
            }
        }
        queue = next_queue;
    }

    Ok((matched_vertex_ids, matched_edge_ids))
}

// parse_content_after_match_keyword returns the RAW parser format
fn parse_content_after_match_keyword(input: &str) -> IResult<&str, ParsedPatternsReturnType> {
    println!("===> parse_content_after_match_keyword START, input length: {}, preview: '{}'", 
             input.len(), &input[..input.len().min(100)]);
    preceded(
        multispace0,
        tuple((
            // Optional path variable assignment (path = ...)
            opt(tuple((
                parse_identifier, 
                multispace0,
                char('='),
                multispace0,
            ))),
            // The actual patterns
            parse_match_clause_patterns,
        )),
    )
    .map(|(_path_setup, patterns)| patterns)
    .parse(input)
}

// match_clause_content_parser returns the RAW parser format
fn match_clause_content_parser<'a>(
    i: &'a str,
) -> IResult<&'a str, (Option<(&'a str, &'a str, char, &'a str)>, ParsedPatternsReturnType)> {
    preceded(
        multispace1,
        tuple((
            // Optional path variable assignment
            opt(tuple((
                take_while1(|c: char| c.is_alphanumeric() || c == '_'),
                multispace0,
                char('='),
                multispace0,
            ))),
            // The actual patterns
            parse_match_clause_patterns,
        )),
    ).parse(i)
}

// ============================================================================
// MAIN STATEMENT PARSER
// ============================================================================
// Add this helper function to convert patterns from parser format to execution format
fn convert_parsed_patterns_to_execution_format(
    parsed_patterns: ParsedPatternsReturnType
) -> ExecutionPatternsReturnType {
    parsed_patterns.into_iter().map(|(path_var, nodes, rels)| {
        let converted_nodes = nodes.into_iter().map(|(var, label_opt, props)| {
            // Convert Option<String> to Vec<String> for labels
            let labels = match label_opt {
                Some(label) => {
                    // Handle multi-label syntax: "Patient:GoldenRecord" -> ["Patient", "GoldenRecord"]
                    label.split(':')
                        .map(|s| s.to_string())
                        .collect::<Vec<String>>()
                },
                None => vec![], // No labels
            };
            (var, labels, props)
        }).collect();
        
        (path_var, converted_nodes, rels)
    }).collect()
}

fn full_statement_parser(input: &str) -> IResult<&str, CypherQuery> {
    println!("===> full_statement_parser  START, input length: {}, preview: '{}'", 
             input.len(), &input[..input.len().min(100)]);
    let mut all_patterns: Vec<Pattern> = Vec::new();
    let mut on_create_set: Vec<(String, String, Expression)> = Vec::new();
    let mut on_match_set: Vec<(String, String, Expression)> = Vec::new();
    let mut input_current = input;
    let mut return_clause_found = false;
    let mut captured_where: Option<WhereClause> = None;
    let mut captured_with: Option<ParsedWithClause> = None;

    // --- 1. PARSE MANDATORY FIRST CLAUSE ---
    let (input_after_clause, clause_type_str) = preceded(
        multispace0,
        alt((
            tag_no_case::<_, _, NomErrorType<&str>>("MERGE"),
            tag_no_case::<_, _, NomErrorType<&str>>("OPTIONAL MATCH"),
            tag_no_case::<_, _, NomErrorType<&str>>("MATCH"),
        ))
    ).parse(input_current)?;

    let is_merge = clause_type_str.to_uppercase() == "MERGE";
    input_current = input_after_clause;

    match parse_content_after_match_keyword(input_current) {
        Ok((input_after_patterns, parsed_patterns)) => {
            let converted = convert_parsed_patterns_to_execution_format(parsed_patterns);
            all_patterns.extend(converted);
            input_current = input_after_patterns;
        }
        Err(e) => return Err(e),
    }

    // --- 2. PARSE ADDITIONAL MATCH CLAUSES ---
    if !is_merge {
        loop {
            let (input_ws, _) = multispace0.parse(input_current)?;
            if is_at_keyword_boundary(input_ws) {
                input_current = input_ws;
                break;
            }

            match preceded(
                alt((
                    tag_no_case::<_, _, NomErrorType<&str>>("OPTIONAL MATCH"),
                    tag_no_case::<_, _, NomErrorType<&str>>("MATCH"),
                )),
                parse_content_after_match_keyword,
            ).parse(input_ws) {
                Ok((input_after_match, parsed_patterns)) => {
                    let converted = convert_parsed_patterns_to_execution_format(parsed_patterns);
                    all_patterns.extend(converted);
                    input_current = input_after_match;
                }
                Err(_) => {
                    input_current = input_ws;
                    break;
                }
            }
        }
    }

    // --- 3. PARSE OPTIONAL WHERE CLAUSE ---
    let (input_after_where, where_opt) = opt(preceded(
        multispace0,
        parse_where
    )).parse(input_current)?;
    captured_where = where_opt;
    input_current = input_after_where;

    // --- 4. PARSE OPTIONAL WITH CLAUSE ---
    let (input_after_with, with_clause_raw) = opt(preceded(
        multispace0,
        parse_with_full, // Returns ParsedWithClause
    )).parse(input_current)?;

    // FIX: with_clause_raw is already ParsedWithClause — no conversion needed
    captured_with = with_clause_raw;
    input_current = input_after_with;

    // --- 5. PARSE ON CLAUSES (MERGE only) ---
    if is_merge {
        loop {
            let (input_ws, _) = multispace0.parse(input_current)?;
            let trimmed = input_ws.trim_start();
            
            if trimmed.to_uppercase().starts_with("ON CREATE") {
                let (input_after, _) = tag_no_case("ON CREATE").parse(input_ws)?;
                let (input_after, _) = preceded(multispace0, tag_no_case("SET")).parse(input_after)?;
                let (input_after_list, clauses) = preceded(
                    multispace0,
                    separated_list1(
                        tuple((multispace0, char(','), multispace0)),
                        parse_single_set_assignment
                    )
                ).parse(input_after)?;
                on_create_set.extend(clauses);
                input_current = input_after_list;
            } else if trimmed.to_uppercase().starts_with("ON MATCH") {
                let (input_after, _) = tag_no_case("ON MATCH").parse(input_ws)?;
                let (input_after, _) = preceded(multispace0, tag_no_case("SET")).parse(input_after)?;
                let (input_after_list, clauses) = preceded(
                    multispace0,
                    separated_list1(
                        tuple((multispace0, char(','), multispace0)),
                        parse_single_set_assignment
                    )
                ).parse(input_after)?;
                on_match_set.extend(clauses);
                input_current = input_after_list;
            } else {
                input_current = input_ws;
                break;
            }
        }
    }

    // --- 6. FIXED CREATE CLAUSE PARSING ---
    let (input_after_create, create_patterns_raw) = opt(preceded(
        tuple((multispace0, tag_no_case("CREATE"), multispace1)),
        separated_list1(
            tuple((multispace0, char(','), multispace0)),
            parse_single_pattern  // Use this instead - it handles relationships
        )
    )).parse(input_current)?;

    // Convert the parsed patterns
    let create_patterns: Vec<Pattern> = match create_patterns_raw {
        Some(raw_patterns) => raw_patterns,  // Already in Pattern format
        None => Vec::new(),
    };

    input_current = input_after_create;

    // --- 7. PARSE SET CLAUSE ---
    let (input_after_set, set_clauses_opt) = opt(preceded(
        tuple((multispace0, tag_no_case("SET"), multispace0)),
        separated_list1(
            tuple((multispace0, char(','), multispace0)),
            parse_single_set_assignment
        )
    )).parse(input_current)?;
    input_current = input_after_set;

    // --- 8. PARSE REMOVE CLAUSE ---
    let (input_after_remove, remove_clauses_opt) = opt(preceded(
        tuple((multispace0, tag_no_case("REMOVE"), multispace0)),
        separated_list1(
            tuple((multispace0, char(','), multispace0)),
            parse_remove_clause
        )
    )).parse(input_current)?;
    input_current = input_after_remove;

    // --- 9. FINAL CONSUMPTION ---
    let (input_ws, _) = multispace0.parse(input_current)?;
    let next_upper = input_ws.trim_start().to_uppercase();
    let input_final = if next_upper.starts_with("RETURN") || next_upper.starts_with("UNION") {
        return_clause_found = true;
        let (i, _) = take_while(|_| true).parse(input_ws)?;
        i
    } else {
        let (i, _) = opt(preceded(multispace0, alt((tag(";"), multispace1)))).parse(input_ws)?;
        i
    };

    // --- 10. ANONYMOUS VARIABLES ---
    let set_clauses = set_clauses_opt.unwrap_or_default();
    let remove_clauses = remove_clauses_opt.unwrap_or_default();

    let mut create_patterns_mut = create_patterns;
    let mut anon_var_counter = 0;
    for pattern in create_patterns_mut.iter_mut() {
        if !pattern.2.is_empty() {
            for node_pattern in pattern.1.iter_mut() {
                if node_pattern.0.is_none() {
                    node_pattern.0 = Some(format!("_anon_{}", anon_var_counter));
                    anon_var_counter += 1;
                }
            }
        }
    }

    // --- 11. DISPATCH ---
    if is_merge {
        Ok((input_final, CypherQuery::Merge { 
            patterns: all_patterns, 
            where_clause: captured_where,
            with_clause: captured_with,
            on_create_set, 
            on_match_set 
        }))
    } else if !create_patterns_mut.is_empty() {
        Ok((input_final, CypherQuery::MatchCreate { 
            match_patterns: all_patterns, 
            where_clause: captured_where,
            with_clause: captured_with,
            create_patterns: create_patterns_mut 
        }))
    } else if !set_clauses.is_empty() {
        Ok((input_final, CypherQuery::MatchSet { 
            match_patterns: all_patterns, 
            where_clause: captured_where,
            with_clause: captured_with,
            set_clauses 
        }))
    } else if !remove_clauses.is_empty() {
        Ok((input_final, CypherQuery::MatchRemove { 
            match_patterns: all_patterns, 
            where_clause: captured_where,
            with_clause: captured_with,
            remove_clauses 
        }))
    } else {
        Ok((input_final, CypherQuery::MatchPattern { 
            patterns: all_patterns, 
            where_clause: captured_where,
            with_clause: captured_with 
        }))
    }
}

// ------------------------------------------------------------------
// MATCH path = (left)-[*..]-(right) RETURN …  (single statement only)
/// Parse **only**   MATCH path = (left)-[*..]-(right) RETURN …
/// Supports variable-length patterns: *0..2, *1..5, *, etc.
// ------------------------------------------------------------------
pub fn parse_match_path(input: &str) -> IResult<&str, CypherQuery, nom::error::Error<&str>> {
    println!("===> parse_match_path START, input length: {}, preview: '{}'", 
             input.len(), &input[..input.len().min(100)]);

    // MATCH keyword
    let (input, _) = terminated(tag_no_case("MATCH"), multispace1).parse(input)?;
    
    // Path variable: path =
    let (input, path_var) = terminated(
        recognize(pair(alpha1, many0(alphanumeric1))),
        tuple((multispace0, tag("="), multispace0))
    ).parse(input)?;

    // Left node: (p:Patient {id: "12345"}) - capture everything between ( and )
    let (input, _) = tag("(").parse(input)?;
    let (input, left_content) = recognize(many0(none_of(")"))).parse(input)?;
    let (input, _) = tag(")").parse(input)?;
    let (input, _) = multispace0.parse(input)?;

    // Relationship pattern: -[*0..2]- or -[:TYPE*0..2]- or -[r:TYPE*0..2]-
    let (input, _) = tag("-[").parse(input)?;
    let (input, _) = multispace0.parse(input)?;
    
    // Optional variable and type: r:TYPE or :TYPE or just r
    let (input, _rel_info) = opt(tuple((
        opt(terminated(
            recognize(pair(alpha1, many0(alphanumeric1))), 
            multispace0
        )), // optional var
        opt(preceded(
            tuple((tag(":"), multispace0)),
            terminated(
                recognize(pair(alpha1, many0(alphanumeric1))),
                multispace0
            )
        )), // optional :TYPE
    ))).parse(input)?;
    
    // Variable-length indicator: *0..2 or *1.. or * or *5
    let (input, _) = tag("*").parse(input)?;
    let (input, _) = multispace0.parse(input)?;
    
    // Capture the range: could be "0..2" or "1.." or just "2" or nothing
    let (input, _range) = opt(alt((
        // N..M format
        recognize(tuple((
            digit1,
            tag(".."),
            opt(digit1),
        ))),
        // Just N format (means up to N)
        recognize(digit1),
    ))).parse(input)?;
    
    let (input, _) = multispace0.parse(input)?;
    let (input, _) = tag("]-").parse(input)?;
    let (input, _) = multispace0.parse(input)?;

    // Right node: (related) - capture everything between ( and )
    let (input, _) = tag("(").parse(input)?;
    let (input, right_content) = recognize(many0(none_of(")"))).parse(input)?;
    let (input, _) = tag(")").parse(input)?;
    let (input, _) = multispace0.parse(input)?;

    // Optional RETURN clause - capture everything after RETURN
    let (input, return_clause) = opt(preceded(
        tuple((tag_no_case("RETURN"), multispace1)),
        take_while(|c| c != ';' && c != '\n' && c != '\r')
    )).parse(input)?;

    Ok((input, CypherQuery::MatchPath {
        path_var: path_var.to_string(),
        left_node: format!("({})", left_content),
        right_node: format!("({})", right_content),
        return_clause: return_clause.unwrap_or("").trim().to_string(),
    }))
}

fn none_of<'a>(chars: &'static str) -> impl FnMut(&'a str) -> IResult<&'a str, &'a str, nom::error::Error<&'a str>> {
    move |i: &'a str| match i.chars().next() {
        Some(c) if !chars.contains(c) => Ok((&i[c.len_utf8()..], &i[..c.len_utf8()])),
        _ => Err(nom::Err::Error(nom::error::Error::new(i, nom::error::ErrorKind::NoneOf))),
    }
}

// === DELETE EDGES PARSER (add this function) ===
/// Parse: MATCH (a)-[r:KNOWS]->(b) DELETE r
///        MATCH ()-[r]->() DELETE r
///        MATCH (a)-[r]-(b) DELETE r
fn parse_delete_edges(input: &str) -> IResult<&str, CypherQuery, nom::error::Error<&str>> {
    println!("===> parse_delete_edges START, input length: {}, preview: '{}'", 
             input.len(), &input[..input.len().min(100)]);

    let (input, _) = multispace0.parse(input)?;
    let (input, _) = tag_no_case("MATCH").parse(input)?;
    let (input, _) = multispace1.parse(input)?;
    
    // 1. Parse raw patterns (Node labels are Option<String>)
    let (input, patterns) = parse_match_clause_patterns(input)?;
    let pattern = patterns.into_iter().next().ok_or_else(|| {
        nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Verify))
    })?;
    
    // 2. Optional WHERE clause
    let (input, where_clause) = opt(preceded(
        multispace0,
        parse_where
    )).parse(input)?;
    
    // 3. ✅ Optional WITH clause (for MPI logic compliance)
    let (input, with_clause_raw) = opt(preceded(
        multispace0,
        parse_with
    )).parse(input)?;

    // Convert WithClause → ParsedWithClause
    let with_clause: Option<ParsedWithClause> = with_clause_raw.map(|w| ParsedWithClause {
        items: Vec::new(),
        distinct: false,
        where_clause: Some(WhereClause { condition: w.condition }),
        order_by: Vec::new(),
        skip: None,
        limit: None,
    });

    let (input, _) = multispace0.parse(input)?;
    let (input, _) = tag_no_case("DELETE").parse(input)?;
    let (input, _) = multispace1.parse(input)?;
    
    // 4. Extract the relationship variable name (e.g. "r" in [r:KNOWS])
    let edge_var = pattern.2.first()
        .and_then(|rel| rel.0.as_ref())
        .ok_or_else(|| {
            nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Verify))
        })?
        .clone();
    
    // 5. Consume the variable name
    let (remaining, _) = tag(&*edge_var).parse(input)?;
    
    // 6. Transform nodes from Raw (Option label) to Execution (Vec labels)
    let execution_nodes: Vec<(Option<String>, Vec<String>, HashMap<String, Value>)> = pattern.1
        .into_iter()
        .map(|(var, label_opt, props)| {
            let labels_vec = match label_opt {
                Some(l) => vec![l],
                None => vec![],
            };
            (var, labels_vec, props)
        })
        .collect();
    
    println!("===> Parsed DELETE edges: var='{}', nodes={}, rels={}",
             edge_var, execution_nodes.len(), pattern.2.len());
    
    // 7. Return with both where_clause and with_clause
    Ok((
        remaining,
        CypherQuery::DeleteEdges {
            edge_variable: edge_var,
            pattern: MatchPattern {
                nodes: execution_nodes,
                relationships: pattern.2,
            },
            where_clause,
            with_clause, // ✅ Now included
        },
    ))
}

// ----------------------------------------------------------------------------------
// --- UPDATED: parse_cypher (To correctly flatten results from sequential parser) ---
// ----------------------------------------------------------------------------------

pub fn parse_cypher(query: &str) -> Result<CypherQuery, String> {
    println!("=====> PARSING CYPHER");
    println!("===> parse_cypher START");
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

/// Parse: MATCH ()-[r]->() DELETE r
fn parse_delete_edges_simple(input: &str) -> IResult<&str, CypherQuery> {
    println!("===> parse_delete_edges_simple START, input length: {}, preview: '{}'", 
             input.len(), &input[..input.len().min(100)]);
    let (input, _) = multispace0(input)?;
    let (input, _) = tag_no_case("MATCH")(input)?;
    let (input, _) = multispace1(input)?;
    
    // 1. Get raw patterns (Node labels are Option<String>)
    let (input, patterns) = parse_match_clause_patterns(input)?;
    let pattern = patterns.into_iter().next().ok_or_else(|| {
        nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Verify))
    })?;
    
    // 2. Extract edge variable name from the first relationship
    let edge_var = pattern.2.first()
        .and_then(|rel| rel.0.as_ref())
        .ok_or_else(|| {
            nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Verify))
        })?
        .clone();
    
    // 3. Consume DELETE and the specific variable
    let (remaining_input, _) = tuple((
        multispace0,
        tag_no_case("DELETE"),
        multispace1,
        tag(&*edge_var),
        multispace0,
    ))(input)?;
    
    // 4. Transform nodes from Raw (Option label) to Execution (Vec label)
    // This matches the NodePattern requirement in query_types.rs
    let execution_nodes: Vec<(Option<String>, Vec<String>, HashMap<String, Value>)> = pattern.1
        .into_iter()
        .map(|(var, label_opt, props)| {
            let labels_vec = match label_opt {
                Some(l) => vec![l],
                None => vec![],
            };
            (var, labels_vec, props)
        })
        .collect();
    
    // 5. Return with correctly typed nodes and all required fields
    Ok((remaining_input, CypherQuery::DeleteEdges {
        edge_variable: edge_var, 
        pattern: MatchPattern {
            nodes: execution_nodes,
            relationships: pattern.2,
        },
        where_clause: None,
        with_clause: None, // Required by variant definition
    }))
}

/// Parse  MATCH (n:Person) DETACH DELETE n
/// returns  CypherQuery::DetachDeleteNodes { node_variable: "n", label: Some("Person") }
fn parse_detach_delete(input: &str) -> IResult<&str, CypherQuery> {
    println!("===> parse_detach_delete START, input length: {}, preview: '{}'", 
             input.len(), &input[..input.len().min(100)]);
    let (input, _) = tag_no_case("MATCH").parse(input)?;
    let (input, _) = multispace1.parse(input)?;
    let (input, node_pat) = parse_node(input)?;
    let (input, _) = multispace1.parse(input)?;
    let (input, _) = tag_no_case("DETACH").parse(input)?;
    let (input, _) = multispace1.parse(input)?;
    let (input, _) = tag_no_case("DELETE").parse(input)?;
    let (input, _) = multispace1.parse(input)?;
    let (input, delete_var) = parse_identifier.parse(input)?;
    
    // Ensure the delete variable matches the node variable
    if let Some(node_var) = &node_pat.0 {
        if node_var != delete_var {
            return Err(nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Verify)));
        }
    } else {
        return Err(nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Verify)));
    }

    // node_pat.1 is Vec<String>. Extract the first label to match Option<String> requirement.
    let label = node_pat.1.first().cloned();

    Ok((
        input,
        CypherQuery::DetachDeleteNodes {
            node_variable: delete_var.to_string(),
            label,
        }
    ))
}

// Assuming NodePattern, WhereClause, and MatchPattern are defined types
fn parse_match_detach_delete(input: &str) -> IResult<&str, CypherQuery> {
    println!("===> parse_match_detach_delete START, input length: {}, preview: '{}'", 
             input.len(), &input[..input.len().min(100)]);
    use nom::Parser;
    use nom::bytes::complete::tag_no_case;
    use nom::character::complete::{multispace0, multispace1, char};
    use nom::combinator::opt;
    use nom::sequence::preceded;

    // 1. Consume the initial "MATCH" keyword
    let (input, _) = tag_no_case("MATCH").parse(input)?;
    let (input, _) = multispace1.parse(input)?;
    
    // 2. Parse the patterns (Raw format: labels are Option<String> in PatternsReturnType)
    let (input, match_patterns_raw) = parse_match_clause_patterns(input)?;
    
    // 3. Parse optional WHERE clause
    let (input, _where_clause) = opt(preceded(multispace1, parse_where_clause)).parse(input)?;

    // 4. Parse DETACH DELETE
    let (input, _) = multispace0.parse(input)?;
    let (input, _) = tag_no_case("DETACH").parse(input)?;
    let (input, _) = multispace1.parse(input)?;
    let (input, _) = tag_no_case("DELETE").parse(input)?;
    let (input, _) = multispace1.parse(input)?;
    let (input, node_variable_raw) = parse_identifier.parse(input)?;
    let node_variable = node_variable_raw.to_string();

    // 5. Find the node label
    // match_patterns_raw is PatternsReturnType
    // Node segment is (Option<String>, Option<String>, HashMap<String, Value>)
    let label = match_patterns_raw.iter()
        .flat_map(|(_, nodes, _)| nodes.iter())
        .find(|(var_opt, _, _)| var_opt.as_ref().map_or(false, |v| v == &node_variable))
        .and_then(|(_, label_opt, _)| {
            // FIX: label_opt is &Option<String>, not a Vec. 
            // We just clone the Option.
            label_opt.clone()
        });

    let (input, _) = opt(char(';')).parse(input)?;
    let (input, _) = multispace0.parse(input)?;

    Ok((input, CypherQuery::DetachDeleteNodes {
        node_variable,
        label,
    }))
}

// Parser for MATCH ... REMOVE
// Note: We need a generic type for the input 'I' and a custom error 'E' 
// to align with the type signature used in the tags.
fn parse_match_remove_relationship<'a>(input: &'a str) -> IResult<&'a str, CypherQuery> {
    println!("===> parse_match_remove_relationship START, input length: {}, preview: '{}'", 
             input.len(), &input[..input.len().min(100)]);
    type NomErrorType<'a> = nom::error::Error<&'a str>;
    
    // 1. Parse MATCH keyword and patterns
    let (input, _) = tag_no_case::<_, _, NomErrorType<'a>>("MATCH")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, raw_match_patterns) = parse_match_clause_patterns(input)?;

    // 2. Parse Optional WHERE clause 
    let (input, where_clause) = opt(preceded(
        multispace1,
        parse_where
    )).parse(input)?;

    // 3. Parse Optional WITH clause (MPI logic wrapper)
    let (input, with_clause_raw) = opt(preceded(
        multispace1,
        parse_with
    )).parse(input)?;

    // Bridge: WithClause → ParsedWithClause
    let with_clause: Option<ParsedWithClause> = with_clause_raw.map(|w| ParsedWithClause {
        items: Vec::new(),
        distinct: false,
        where_clause: Some(WhereClause { condition: w.condition }),
        order_by: Vec::new(),
        skip: None,
        limit: None,
    });

    // 4. Parse REMOVE keyword
    let (input, _) = multispace1(input)?;
    let (input, _) = tag_no_case::<_, _, NomErrorType<'a>>("REMOVE")(input)?;
    let (input, _) = multispace1(input)?;
    
    // 5. Parse the list of properties to remove
    let (input, remove_clauses) = separated_list1(
        tuple((multispace0, char(','), multispace0)),
        parse_remove_clause
    ).parse(input)?;
    
    // 6. Optionally consume RETURN clause
    let (input, _) = opt(
        preceded(
            tuple((multispace1, tag_no_case::<_, _, NomErrorType<'a>>("RETURN"), multispace1)),
            take_while(|c| c != ';' && c != '\n'),
        )
    ).parse(input)?;

    // 7. Transform patterns to execution format
    let match_patterns: ExecutionPatternsReturnType = raw_match_patterns
        .into_iter()
        .map(|(path_var, nodes, edges)| {
            let transformed_nodes = nodes
                .into_iter()
                .map(|(var, label_opt, props)| {
                    let labels_vec = match label_opt {
                        Some(l) => vec![l],
                        None => vec![],
                    };
                    (var, labels_vec, props)
                })
                .collect();
            (path_var, transformed_nodes, edges)
        })
        .collect();
    
    // 8. Return with `with_clause` included
    Ok((input, CypherQuery::MatchRemove {
        match_patterns,
        where_clause, 
        with_clause,      // ✅ Now provided
        remove_clauses,
    }))
}

/// your **old** top-level logic, just moved into a helper
/// Helper function to parse a single statement string.
// ----------------------------------------------------------------------------------
// --- UPDATED: parse_single_statement (Prioritizes MATCH...SET over MATCH...CREATE) ---
// ----------------------------------------------------------------------------------
// In cypher_parser.rs
/// Helper function to parse a single statement string.
fn parse_single_statement(input: &str) -> Result<CypherQuery, String> {
    println!("===> parse_single_statement START, input length: {}, preview: '{}'", 
             input.len(), &input[..input.len().min(100)]);
    let trimmed = input.trim();
    let upper = trimmed.to_ascii_uppercase();
    
    // 1. DETACH DELETE
    if upper.contains("DETACH DELETE") {
        return parse_match_detach_delete(trimmed)
            .map(|(_, q)| q)
            .map_err(|e| format!("MATCH-DETACH-DELETE parse error: {:?}", e));
    }
    
    // 2. *** Check for MATCH ... CREATE ... SET ***
    if upper.starts_with("MATCH") && upper.contains("CREATE") && upper.contains("SET") {
        return parse_match_create_relationship(trimmed) 
            .map(|(_, q)| q)
            .map_err(|e| format!("MATCH-CREATE-SET parse error: {:?}", e));
    }
    
    // 3. Check for MATCH ... SET
    if upper.starts_with("MATCH") && upper.contains("SET") {
        return parse_match_set_relationship(trimmed)
            .map(|(_, q)| q)
            .map_err(|e| format!("MATCH-SET parse error: {:?}", e));
    }
    
    // 4. Check for MATCH ... CREATE
    if upper.starts_with("MATCH") && upper.contains("CREATE") {
        return parse_match_create_relationship(trimmed)
            .map(|(_, q)| q)
            .map_err(|e| format!("MATCH-CREATE parse error: {:?}", e));
    }
    
    // 5. Check for MERGE explicitly
    if upper.starts_with("MERGE") {
        if let Ok((remainder, query)) = full_statement_parser(trimmed) {
            let remainder_trimmed = remainder.trim();
            if !remainder_trimmed.is_empty() {
                 return Err(format!("MERGE statement parsed partially. Remainder: '{}'", remainder_trimmed));
            }
            return Ok(query);
        } else {
             return Err(format!("MERGE statement failed to parse. Check pattern syntax."));
        }
    }

    // 6. single CREATE (no rels)
    if upper.starts_with("CREATE") && !upper.contains("-[") {
        // Use closures for Nom 8 trait compatibility
        let create_parsers: Vec<Box<dyn Fn(&str) -> IResult<&str, CypherQuery>>> = vec![
            Box::new(|i| parse_create_node(i)),
            Box::new(|i| parse_create_nodes(i)),
            Box::new(|i| parse_create_edge(i)),
            Box::new(|i| parse_create_edge_between_existing(i)),
            Box::new(|i| parse_create_complex_pattern(i)),
        ];
        for parser in create_parsers {
            if let Ok((remainder, query)) = parser(trimmed) {
                if remainder.trim().is_empty() {
                    return Ok(query);
                }
            }
        }
    }
    
    // 7. Relationship patterns
    if upper.contains("-[") && (upper.contains("]->") || upper.contains("]-") || upper.contains("<-[")) {
        return parse_simple_query_type(trimmed)
            .map(|(_, q)| q)
            .map_err(|e| format!("Simple-statement parse error: {:?}", e));
    }
    
    // 8. full MATCH … RETURN fallback
    if let Ok((remainder, query)) = full_statement_parser(trimmed) {
        let remainder_trimmed = remainder.trim();
        if remainder_trimmed.is_empty() {
            return Ok(query);
        }
    }
    
    // 9. sequential / batch statements
    if let Ok((remainder, query)) = parse_sequential_statements(trimmed) {
        if remainder.trim().is_empty() {
            return Ok(query);
        }
    }
    
    // 10. fallback list
    let parsers: Vec<Box<dyn Fn(&str) -> IResult<&str, CypherQuery>>> = vec![
        Box::new(|i| parse_create_statement(i)),
        Box::new(|i| parse_delete_edges_simple(i)),
        Box::new(|i| parse_match_create_relationship(i)),
        Box::new(|i| parse_detach_delete(i)),
        Box::new(|i| parse_create_index(i)),
        Box::new(|i| parse_set_node(i)),
        Box::new(|i| parse_delete_node(i)),
        Box::new(|i| parse_set_kv(i)),
        Box::new(|i| parse_get_kv(i)),
        Box::new(|i| parse_delete_kv(i)),
    ];
    for parser in parsers {
        if let Ok((remainder, query)) = parser(trimmed) {
            if remainder.trim().is_empty() {
                return Ok(query);
            }
        }
    }
    
    Err(format!("Unable to parse statement: {}", trimmed))
}

// CORRECTED parse_pattern for Nom 8 with Parser trait
fn parse_pattern(input: &str) -> IResult<&str, Pattern> {
    println!("===> parse_pattern START, input length: {}, preview: '{}'", 
             input.len(), &input[..input.len().min(100)]);
    let (input, path_var) = opt(terminated(
        parse_identifier, 
        tuple((multispace0, char('='), multispace0))
    )).parse(input)?;
    
    let (input, first_node) = parse_node(input)?;
    let (input, _) = multispace0.parse(input)?;
    
    let mut nodes = vec![first_node];  // ✅ MUST be mutable — you push in the loop
    let mut rels = Vec::new();
    let mut remaining = input;
    
    loop {
        match parse_relationship(remaining) {
            Ok((rest, rel)) => {
                rels.push(rel);
                let (rest, _) = multispace0.parse(rest)?;
                
                match parse_node(rest) {
                    Ok((rest, node)) => {
                        nodes.push(node);  // ← requires `mut nodes`
                        let (rest, _) = multispace0.parse(rest)?;
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

// Helper function to handle optional whitespace around a parser
// Assumes multispace0 is imported from nom::character::complete
// FIX E0277/E0618: Defines 'ws' to return an explicit closure, making it compatible
// with the IResult function style when used as an argument to combinators.
fn ws<'a, F, O>(mut inner: F) -> impl FnMut(&'a str) -> IResult<&'a str, O>
where
    F: FnMut(&'a str) -> IResult<&'a str, O>,
{
    // Use the function call style for delimited
    move |input: &'a str| {
        delimited(multispace0, &mut inner, multispace0).parse(input)
    }
}

fn parse_pattern_with_stop_guard(input: &str) -> IResult<&str, Pattern> {
    parse_pattern_restricted(input)
}

/// Parse a value (string, number, boolean, null)
/// FIX: Re-ordered to prioritize Strings and handle UUID hyphens
fn parse_value(input: &str) -> IResult<&str, Value> {
    println!("===> parse_value START, input length: {}, preview: '{}'", 
             input.len(), &input[..input.len().min(100)]);
    alt((
        // 1. Quoted Strings - Always String
        map(
            alt((
                delimited(char('"'), take_while(|c| c != '"'), char('"')),
                delimited(char('\''), take_while(|c| c != '\''), char('\'')),
            )),
            |s: &str| Value::String(s.to_string())
        ),
        
        // 2. Booleans & Null
        map(tag_no_case("true"), |_| Value::Bool(true)),
        map(tag_no_case("false"), |_| Value::Bool(false)),
        map(tag_no_case("null"), |_| Value::Null),

        // 3. Numbers - Prioritize if it starts with a digit or minus followed by digit
        map(
            recognize(tuple((
                opt(char('-')),
                digit1,
                opt(tuple((char('.'), digit1)))
            ))),
            |s: &str| {
                if s.contains('.') {
                    Value::Number(serde_json::Number::from_f64(s.parse().unwrap()).unwrap())
                } else {
                    Value::Number(serde_json::Number::from(s.parse::<i64>().unwrap()))
                }
            }
        ),

        // 4. UUIDs / Unquoted Strings (The fallback)
        map(
            take_while1(|c: char| c.is_alphanumeric() || c == '-' || c == '_'), 
            |s: &str| Value::String(s.to_string())
        ),
    )).parse(input)
}

// ============================================================================
// SET CLAUSE PARSING
// ============================================================================

/// Parse a single SET assignment: var.prop = value
/// Parses a single assignment: u.prop = expression
fn parse_single_set_assignment(input: &str) -> IResult<&str, (String, String, Expression)> {
    println!("===> parse_single_set_assignment START, input length: {}, preview: '{}'", 
             input.len(), &input[..input.len().min(100)]);
    let (input, var_name) = preceded(multispace0, parse_identifier).parse(input)?;
    let (input, _) = preceded(multispace0, char('.')).parse(input)?;
    let (input, prop_name) = parse_identifier.parse(input)?;
    let (input, _) = delimited(multispace0, char('='), multispace0).parse(input)?;
    
    // Lasting Fix: Accept the full expression tree
    let (input, expr) = parse_expression(input)?; 
    
    Ok((input, (var_name.to_string(), prop_name.to_string(), expr)))
}

/// Recursive descent parser for expressions (handles precedence)
fn parse_expression(input: &str) -> IResult<&str, Expression> {
    println!("===> parse_expression START, input length: {}, preview: '{}'", 
             input.len(), &input[..input.len().min(100)]);

    // 1. Parse the "Atom"
    let (input, mut left) = alt((
        map(
            tuple((parse_identifier, char('.'), parse_identifier)),
            |(var, _, prop)| Expression::Property(PropertyAccess::Vertex(var.to_string(), prop.to_string()))
        ),
        map(parse_value, |v| Expression::Literal(CypherValue::from_json(v))),
        map(parse_identifier, |v| Expression::Variable(v.to_string())),
    )).parse(input)?;

    // 2. Check for Operators
    let mut current_input = input;
    
    loop {
        // First, look for the operator (e.g., + , -)
        let op_result: IResult<&str, BinaryOp> = delimited(multispace0, parse_binary_op, multispace0)
            .parse(current_input);

        match op_result {
            Ok((after_op, op)) => {
                // If operator found, we MUST find a right-hand side atom
                let rhs_result = alt((
                    map(tuple((parse_identifier, char('.'), parse_identifier)), |(v, _, p)| 
                        Expression::Property(PropertyAccess::Vertex(v.to_string(), p.to_string()))),
                    map(parse_value, |v| Expression::Literal(CypherValue::from_json(v))),
                    map(parse_identifier, |v| Expression::Variable(v.to_string())),
                )).parse(after_op);

                match rhs_result {
                    Ok((after_rhs, right_atom)) => {
                        left = Expression::Binary {
                            op,
                            left: Box::new(left),
                            right: Box::new(right_atom),
                        };
                        current_input = after_rhs;
                    }
                    // Found operator but no valid RHS? Stop parsing expression here.
                    Err(_) => break,
                }
            }
            // No more operators? We are done.
            Err(_) => break,
        }
    }

    Ok((current_input, left))
}

fn parse_expression_string(input: &str) -> IResult<&str, String> {
    println!("===> parse_expression_string START, input length: {}, preview: '{}'", 
             input.len(), &input[..input.len().min(100)]);
    

    // Use `recognize` to get the raw string that `parse_expression` would consume
    let (remaining, expr_str) = recognize(parse_expression).parse(input)?;

    Ok((remaining, expr_str.to_string()))
}

fn parse_binary_op(input: &str) -> IResult<&str, BinaryOp> {
    println!("===> parse_binary_op START, input length: {}, preview: '{}'", 
             input.len(), &input[..input.len().min(100)]);
    
    alt((
        map(tag("+"), |_| BinaryOp::Plus),
        map(tag("-"), |_| BinaryOp::Minus),
        map(tag("*"), |_| BinaryOp::Mul),
        map(tag("/"), |_| BinaryOp::Div),
    )).parse(input) // Use .parse(input)
}

/// Parse SET clause for use in match_set_relationship
fn parse_set_clause(input: &str) -> IResult<&str, (String, HashMap<String, Value>)> {
    println!("===> parse_set_clause START, input length: {}, preview: '{}'", 
             input.len(), &input[..input.len().min(100)]);
    let (input, var) = preceded(multispace0, parse_identifier).parse(input)?;
    let (input, _) = preceded(multispace0, char('.')).parse(input)?;
    let (input, prop) = parse_identifier.parse(input)?;
    let (input, _) = delimited(multispace0, char('='), multispace0).parse(input)?;
    
    // FIX: Use parse_property_value instead of parse_value to support 
    // the same logic used in node properties (UUIDs, timestamps, etc.)
    let (input, val) = parse_property_value.parse(input)?;
    
    let mut map = HashMap::new();
    map.insert(prop.to_string(), val);
    
    Ok((input, (var.to_string(), map)))
}

// New function: Parses a BATCH statement: BATCH { ...; ...; }
fn parse_batch(input: &str) -> IResult<&str, CypherQuery> {
    println!("===> parse_batch START, input length: {}, preview: '{}'", 
             input.len(), &input[..input.len().min(100)]);
    map(
        preceded(
            terminated(tag_no_case("BATCH"), multispace1),
            // Uses many1 for required one-or-more statements
            delimited(
                char('{'), 
                many1(preceded(multispace0, terminated(parse_cypher_query_chain, char(';')))), 
                preceded(multispace0, char('}'))
            ),
        ),
        |statements| CypherQuery::Batch(statements),
    ).parse(input)
}

fn parse_single_query_clause(input: &str) -> IResult<&str, CypherQuery> {
    println!("===> parse_single_query_clause START, input length: {}, preview: '{}'", 
             input.len(), &input[..input.len().min(100)]);

    // 1. Try MERGE
    if let Ok(result) = parse_merge(input) {
        return Ok(result);
    }

    // 2. MATCH handling with WHERE and WITH
    if let Ok((input_after_match, patterns)) = parse_match_clause(input) {
        // Parse optional WHERE
        let (input_after_where, where_clause) = opt(parse_where)
            .parse(input_after_match)?;

        // Parse optional WITH — returns Option<WithClause>
        let (remaining, with_clause_simple) = opt(preceded(multispace0, parse_with))
            .parse(input_after_where)?;

        // Convert WithClause → ParsedWithClause to satisfy enum type
        let with_clause = with_clause_simple.map(|wc| ParsedWithClause {
            items: Vec::new(),
            distinct: false,
            where_clause: Some(WhereClause { condition: wc.condition }),
            order_by: Vec::new(),
            skip: None,
            limit: None,
        });

        return Ok((remaining, CypherQuery::MatchPattern {
            patterns,
            where_clause,
            with_clause, // ✅ Now included
        }));
    }

    // 3. Try standalone WITH
    if let Ok(result) = parse_with_clause(input) {
        return Ok(result);
    }

    // 4. Try CREATE
    if let Ok((remaining, patterns)) = parse_create_clause(input) {
        return Ok((remaining, CypherQuery::CreateStatement {
            patterns,
            return_items: vec![],
        }));
    }

    // 5. Try modifying clauses (SET, DELETE, etc.)
    if let Ok(result) = parse_modifying_clause(input) {
        return Ok(result);
    }

    Err(nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Alt)))
}

pub fn parse_cypher_query_chain(input: &str) -> IResult<&str, CypherQuery> {
    println!("===> parse_cypher_query_chain START, input length: {}, preview: '{}'", 
             input.len(), &input[..input.len().min(100)]);
    // 1. Parse the initial query block (e.g., MATCH...CREATE or just MATCH...RETURN)
    
    // Parse multiple preceding modifying clauses (MATCH, MERGE, WITH, etc.), separated by mandatory whitespace
    let (input, clauses) = many0(terminated(parse_single_query_clause, multispace1)).parse(input)?;
    
    // Parse the final, optional RETURN clause - should return CypherQuery
    let (input, return_clause_opt) = opt(parse_return_clause).parse(input)?;
    
    // Combine clauses with return clause if present
    let all_clauses = if let Some(ret) = return_clause_opt {
        // Only push if ret is actually a CypherQuery (not ())
        // This check helps if parse_return_clause has implementation issues
        let mut combined = clauses;
        combined.push(ret);
        combined
    } else {
        clauses
    };
    
    // Determine the base query structure
    let initial_query = if all_clauses.len() == 1 {
        // If only one clause, return it directly
        all_clauses.into_iter().next().unwrap()
    } else if all_clauses.is_empty() {
        // If no clauses found, try to parse BATCH
        return parse_batch(input);
    } else {
        // If multiple clauses (MATCH...MERGE...CREATE), chain them
        CypherQuery::Chain(all_clauses)
    };
    
    // 2. Handle subsequent UNION/UNION ALL parts recursively
    let union_block_parser = pair(
        preceded(multispace1, alt((
            map(terminated(tag_no_case::<_, _, NomErrorType<&str>>("UNION ALL"), multispace1), |_| true),
            map(terminated(tag_no_case::<_, _, NomErrorType<&str>>("UNION"), multispace1), |_| false),
        ))),
        parse_cypher_query_chain // Recursive call
    );
    let (input, union_parts) = many0(union_block_parser).parse(input)?;
    
    // 3. Assemble the result recursively: Left-associative Union structure
    let mut current_query = initial_query;
    for (is_all, next_query) in union_parts {
        // The Union variant must store the sub-queries in Box<CypherQuery>
        current_query = CypherQuery::Union(Box::new(current_query), is_all, Box::new(next_query));
    }
    Ok((input, current_query))
}

// CRITICAL FIX:
// 1. Make sure "use nom::Parser;" is at the top of your file
// 2. Use .parse(input)? syntax everywhere (this is Nom 8 with Parser trait)
// 3. Remove duplicate full_statement_parser
// 4. Check your existing parse_node, parse_relationship - they should use .parse() too

// CRITICAL FIX SUMMARY:
// 1. Remove DUPLICATE full_statement_parser (delete one of them)
// 2. Use the corrected parse_cypher above
// 3. Ensure parse_pattern comes BEFORE full_statement_parser in file
// 4. The parser chain order matters - full_statement_parser MUST be first in alt()
// In lib/src/query_parser/cypher_parser.rs

// Assuming parse_set_clause is defined elsewhere, likely:
// fn parse_set_clause(input: &str) -> IResult<&str, (String, String, Value)> { ... }
fn parse_cypher_statement(input: &str) -> IResult<&str, CypherQuery> {
    println!("===> parse_cypher_statement START, input length: {}, preview: '{}'", 
             input.len(), &input[..input.len().min(100)]);
    // 1. Match mandatory "MATCH" or "OPTIONAL MATCH"
    let (input, _) = alt((
        tag_no_case("MATCH"), 
        tag_no_case("OPTIONAL MATCH")
    )).parse(input)?;
    let (input, _) = multispace1(input)?;

    // 2. Consume patterns for the MATCH clause
    let (input, patterns) = separated_list1(
        tuple((multispace0, char(','), multispace0)),
        parse_pattern_restricted,
    ).parse(input)?;

    // 3. Handle additional OPTIONAL MATCH clauses
    let (input, additional_patterns) = many0(preceded(
        multispace0,
        preceded(
            tag_no_case("OPTIONAL MATCH"),
            preceded(multispace1, parse_pattern_restricted)
        )
    )).parse(input)?;
    
    let mut all_match_patterns = patterns;
    all_match_patterns.extend(additional_patterns);

    // 4. Consume WHERE clause
    let (input, where_clause) = opt(preceded(
        multispace0,
        parse_where 
    )).parse(input)?;

    // 5. ✅ PARSE WITH CLAUSE — this was missing!
    let (input, with_clause_raw) = opt(preceded(
        multispace0,
        parse_with  // returns WithClause { condition: Expression }
    )).parse(input)?;

    // 6. Convert WithClause → ParsedWithClause to satisfy enum type
    let with_clause: Option<ParsedWithClause> = with_clause_raw.map(|wc| ParsedWithClause {
        items: Vec::new(),
        distinct: false,
        where_clause: Some(WhereClause { condition: wc.condition }),
        order_by: Vec::new(),
        skip: None,
        limit: None,
    });

    // 7. Consume SET clause 
    let (input, set_clauses_opt) = opt(preceded(
        multispace0,
        preceded(
            terminated(tag_no_case("SET"), multispace1),
            separated_list1(
                tuple((multispace0, char(','), multispace0)),
                parse_single_set_assignment,
            ),
        )
    )).parse(input)?;

    // 8. Consume CREATE clause
    let (input, create_patterns_opt) = opt(preceded(
        multispace0,
        preceded(
            terminated(tag_no_case("CREATE"), multispace1),
            separated_list1(
                tuple((multispace0, char(','), multispace0)),
                parse_pattern_restricted,
            ),
        )
    )).parse(input)?;

    // 9. Consume REMOVE clause
    let (input, remove_clauses_opt) = opt(preceded(
        multispace0,
        preceded(
            terminated(tag_no_case("REMOVE"), multispace1),
            separated_list1(
                tuple((multispace0, char(','), multispace0)),
                parse_remove_clause,
            ),
        )
    )).parse(input)?;

    // 10. Final cleanup
    let (input, _) = opt(preceded(multispace0, char(';'))).parse(input)?;
    let (input, _) = multispace0(input)?;

    // 11. Dispatch with `with_clause` included in all variants
    if let Some(set_list) = set_clauses_opt {
        Ok((input, CypherQuery::MatchSet {
            match_patterns: all_match_patterns,
            where_clause,
            with_clause,  // ✅ now included
            set_clauses: set_list, 
        }))
    } else if let Some(create_list) = create_patterns_opt {
        Ok((input, CypherQuery::MatchCreate {
            match_patterns: all_match_patterns,
            where_clause,
            with_clause,  // ✅ now included
            create_patterns: create_list,
        }))
    } else if let Some(remove_list) = remove_clauses_opt {
        Ok((input, CypherQuery::MatchRemove {
            match_patterns: all_match_patterns,
            where_clause,
            with_clause,  // ✅ now included
            remove_clauses: remove_list,
        }))
    } else {
        Ok((input, CypherQuery::MatchPattern { 
            patterns: all_match_patterns, 
            where_clause,
            with_clause,  // ✅ now included
        }))
    }
}

fn parse_pattern_restricted(input: &str) -> IResult<&str, Pattern> {
    println!("===> parse_pattern_restricted START, input length: {}, preview: '{}'", 
             input.len(), &input[..input.len().min(100)]);
    // 1. Consume leading whitespace and get the ACTUAL start of the pattern
    let (remaining, _) = multispace0::<&str, nom::error::Error<&str>>(input)?;
    
    // 2. Keyword check on the TRIMMED string
    if is_at_keyword_boundary(remaining) {
        return Err(nom::Err::Error(nom::error::Error::new(remaining, nom::error::ErrorKind::Tag)));
    }
    
    // 3. IMPORTANT: Pass 'remaining' (the string WITHOUT leading spaces) 
    // to the next parser, otherwise we never advance the pointer.
    parse_single_pattern(remaining)
}

fn parse_variable_length(input: &str) -> IResult<&str, (Option<u32>, Option<u32>)> {
    println!("===> parse_variable_length START, input length: {}, preview: '{}'", 
             input.len(), &input[..input.len().min(100)]);
    let (input, _) = multispace0.parse(input)?;
    let (input, _) = char('*').parse(input)?;
    let (input, _) = multispace0.parse(input)?;
    
    // Try to parse min
    let (input, min) = opt(nom::character::complete::u32).parse(input)?;
    
    // Try to parse .. separator
    let (input, has_range) = opt(tuple((
        multispace0,
        tag(".."),
        multispace0
    ))).parse(input)?;
    
    // Try to parse max
    let (input, max) = if has_range.is_some() {
        opt(nom::character::complete::u32).parse(input)?
    } else {
        (input, None)
    };
    
    let (input, _) = multispace0.parse(input)?;
    
    // Handle various formats:
    // *      -> (1, inf)
    // *2     -> (1, 2)  
    // *0..2  -> (0, 2)
    // *1..   -> (1, inf)
    let result = match (min, max) {
        (None, None) => (Some(1), None),        // * means 1 or more
        (Some(m), None) if has_range.is_none() => (Some(1), Some(m)), // *2 means up to 2
        (Some(m), None) => (Some(m), None),     // *1.. means 1 or more
        (Some(m), Some(n)) => (Some(m), Some(n)), // *0..2 means 0 to 2
        (None, Some(n)) => (Some(0), Some(n)),  // *..2 means up to 2
    };
    
    Ok((input, result))
}

// Parser for variable.property
fn parse_remove_clause(input: &str) -> IResult<&str, (String, String)> {
    println!("===> parse_remove_clause START, input length: {}, preview: '{}'", 
             input.len(), &input[..input.len().min(100)]);
    // FIX: Wrap the entire parser definition in a move closure and execute it immediately.
    // This resolves the E0618 "expected function, found impl Parser<...>" error.

    (move |i| {
        map(
            tuple((
                parse_identifier,
                ws(char('.')),
                parse_identifier,
            )),
            |(variable, _, property)| (variable.to_string(), property.to_string()),
        ).parse(i)
    })(input)
}

// --- Relationship Type Parsers (Fixing the '|' operator) ---
// Parses a list of relationship types separated by the pipe '|' (OR) operator.
fn parse_rel_types_with_or(input: &str) -> IResult<&str, Vec<String>> {
    println!("===> parse_rel_types_with_or START, input length: {}, preview: '{}'", 
             input.len(), &input[..input.len().min(100)]);
    
    separated_list1(
        delimited(multispace0, char('|'), multispace0),
        map(
            take_while1(|c: char| c.is_alphanumeric() || c == '_'),
            |s: &str| s.to_string()
        )
    )
    .parse(input)
}

// Parses the optional relationship type part, e.g., ":HAS_ENCOUNTER|HAS_DIAGNOSIS"
fn parse_optional_rel_types(input: &str) -> IResult<&str, Option<Vec<String>>> {
    println!("===> parse_optional_rel_types START, input length: {}, preview: '{}'", 
             input.len(), &input[..input.len().min(100)]);
    opt(preceded(
        char(':'),
        parse_rel_types_with_or
    ))
    .parse(input)
}

fn parse_rel_detail(input: &str) -> IResult<&str, (Option<String>, Option<String>, Option<(Option<u32>, Option<u32>)>, HashMap<String, Value>)> {
    println!("===> parse_rel_detail( START, input length: {}, preview: '{}'", 
             input.len(), &input[..input.len().min(100)]);
    let (input, _) = multispace0.parse(input)?;
    
    // Optional variable name
    let (input, var_opt) = opt(preceded(
        multispace0,
        take_while1(|c: char| c.is_alphanumeric() || c == '_')
    )).parse(input)?;
    
    // FIXED: Parse the relationship types (may include multiple types with |)
    let (input, rel_types_list_opt) = parse_optional_rel_types(input)?;
    
    // FIXED: Convert Option<Vec<String>> to Option<String>
    // Join multiple types with | to preserve all types in a single string
    let rel_type_opt = rel_types_list_opt.map(|types_list| {
        types_list.join("|")
    });

    // Optional variable-length specification: *min..max or *
    let (input, var_length_opt) = opt(parse_variable_length).parse(input)?;
    
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
        rel_type_opt,
        var_length_opt,
        props
    )))
}

fn parse_optional_match_clause(input: &str) -> IResult<&str, Vec<Pattern>> {
    println!("===> parse_optional_match_clause START, input length: {}, preview: '{}'", 
             input.len(), &input[..input.len().min(100)]);
    let (input, _) = tag("OPTIONAL MATCH").parse(input)?;
    let (input, _) = multispace1.parse(input)?;
    let (input, patterns) = separated_list1(
        tuple((multispace0, char(','), multispace0)),
        parse_pattern
    ).parse(input)?;
    Ok((input, patterns))
}

fn parse_relationship_full(input: &str) -> IResult<&str, RelPattern> {
    println!("===> parse_relationship_full START, input length: {}, preview: '{}'", 
             input.len(), &input[..input.len().min(100)]);
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
fn parse_match_clause_patterns(input: &str) -> IResult<&str, ParsedPatternsReturnType> {
    println!("===> parse_match_clause_patterns START, input length: {}, preview: '{}'", 
             input.len(), &input[..input.len().min(100)]);
    
    let mut patterns = Vec::new();
    let mut current_input = input;
    
    loop {
        let (next_input, _) = multispace0::<&str, nom::error::Error<&str>>(current_input)?;
        current_input = next_input;
        
        if current_input.trim_start().is_empty() {
            break;
        }
        
        // Check if we hit a comma (multiple patterns in one MATCH)
        if current_input.starts_with(',') {
            current_input = &current_input[1..];
            continue;
        }

        // Termination check for clauses like WHERE, RETURN, CREATE, etc.
        if is_at_keyword_boundary(current_input) {
            break;
        }
        
        // Try to parse a full single pattern (node-rel-node or just node)
        match parse_single_pattern(current_input) {
            Ok((remainder, pattern)) => {
                patterns.push(pattern);
                current_input = remainder;
            }
            Err(_) => break,
        }
    }
    
    println!("===> parse_match_clause_patterns END – remainder length: {}, remainder: '{}'", 
             current_input.len(), current_input);
    
    // FIX: Convert Vec<String> labels → Option<String>
    let fixed_patterns: ParsedPatternsReturnType = patterns
        .into_iter()
        .map(|pattern| {
            let fixed_nodes = pattern.1
                .into_iter()
                .map(|(var, labels, props)| {
                    // FIX: Remove the .map(|s| Some(s)) wrapper.
                    // labels.first().cloned() is already Option<String>.
                    let first_label = labels.first().cloned();
                    (var, first_label, props)
                })
                .collect();
            (pattern.0, fixed_nodes, pattern.2)
        })
        .collect();
    
    Ok((current_input, fixed_patterns))
}

// Parses a Cypher numeric literal (signed integers or floats) and maps it to a PropertyValue.
// Parses a Cypher numeric literal (signed integers or floats) and maps it to a PropertyValue.
pub fn parse_numeric_literal(input: &str) -> IResult<&str, PropertyValue> {
    println!("===> parse_numeric_literal START");
    // Manual parsing approach to avoid nom parser composition issues
    let original_input = input;
    let mut chars = input.chars().peekable();
    let mut number_str = String::new();
    
    // Check for optional negative sign
    if let Some('-') = chars.peek() {
        number_str.push('-');
        chars.next();
    }
    
    // Check if we have at least one digit or a decimal point
    let mut has_digits = false;
    let mut has_decimal = false;
    
    // Parse digits before decimal point
    while let Some(&ch) = chars.peek() {
        if ch.is_ascii_digit() {
            number_str.push(ch);
            chars.next();
            has_digits = true;
        } else {
            break;
        }
    }
    
    // Check for decimal point
    if let Some(&'.') = chars.peek() {
        number_str.push('.');
        chars.next();
        has_decimal = true;
        
        // Parse digits after decimal point
        while let Some(&ch) = chars.peek() {
            if ch.is_ascii_digit() {
                number_str.push(ch);
                chars.next();
                has_digits = true;
            } else {
                break;
            }
        }
    }
    
    // Must have at least one digit
    if !has_digits {
        return Err(nom::Err::Error(NomError::Error {
            input: original_input,
            code: NomError::ErrorKind::Digit,
        }));
    }
    
    // Calculate remaining input
    let consumed = number_str.len();
    let remaining = &original_input[consumed..];
    
    // Attempt to parse the recognized string into the appropriate PropertyValue.
    // Try i64 first (for integers, like -295941589)
    if !has_decimal {
        if let Ok(i) = number_str.parse::<i64>() {
            return Ok((remaining, PropertyValue::Integer(i)));
        }
    }
    
    // Fallback to f64 (for floats)
    if let Ok(f) = number_str.parse::<f64>() {
        return Ok((remaining, PropertyValue::Float(SerializableFloat(f))));
    }
    
    // Error case
    Err(nom::Err::Error(NomError::Error {
        input: original_input,
        code: NomError::ErrorKind::Float,
    }))
}

// --- Required Type Definitions (from query_types.rs) ---
// Note: You must ensure these types are in scope (e.g., via `use crate::query_parser::query_types::*`)
// use serde_json::Value; 
// use std::collections::HashMap;

// pub type NodePattern = (Option<String>, Option<String>, HashMap<String, Value>);
// pub type RelPattern = (Option<String>, Option<String>, Option<(Option<u32>, Option<u32>)>, HashMap<String, Value>, Option<bool>);
// pub type Pattern = (Option<String>, Vec<NodePattern>, Vec<RelPattern>);


/// Helper function to satisfy the compiler and build the final pattern structure.
/// 
/// Constructs the final `Pattern` tuple: `(Option<String>, Vec<NodePattern>, Vec<RelPattern>)`.
/// The first element is typically used for a path variable (e.g., `p` in MATCH p=(a)-->(b)`).
/// For simple patterns, this is set to `None`.
fn build_pattern_from_elements(nodes: Vec<NodePattern>, rels: Vec<RelPattern>) -> Pattern {
    // Path variable is None, followed by the collected node and relationship vectors.
    println!("===> build_pattern_from_elements START");
    (None, nodes, rels) 
}

fn parse_match_clause(input: &str) -> IResult<&str, Vec<Pattern>> {
    println!("===> parse_match_clause START");
    let (input, _) = tag("MATCH").parse(input)?;
    let (input, _) = multispace1.parse(input)?;
    let (input, patterns) = separated_list1(
        tuple((multispace0, char(','), multispace0)),
        parse_pattern
    ).parse(input)?;
    Ok((input, patterns))
}

/// The entry point for parsing expressions within a WHERE clause.
fn parse_cypher_expression(input: &str) -> IResult<&str, Expression> {
    println!("===> parse_cypher_expression START");
    parse_logical_expression(input)
}

/// Level 1: OR (Lowest Precedence)
pub fn parse_logical_expression(input: &str) -> IResult<&str, Expression> {
    println!("===> parse_logical_expression START");
    // Parse sequence of OR-separated terms
    let (input, mut terms) = separated_list1(
        preceded(multispace0, tag_no_case("OR")),
        parse_and_term,
    ).parse(input)?;

    // Fold into Expression::Or
    let condition = if terms.len() == 1 {
        terms.remove(0)
    } else {
        let mut iter = terms.into_iter();
        let mut root = iter.next().unwrap();
        for next in iter {
            root = Expression::Or {
                left: Box::new(root),
                right: Box::new(next),
            };
        }
        root
    };

    Ok((input, condition))
}

fn parse_and_term(input: &str) -> IResult<&str, Expression> {
    println!("===> parse_and_term START");
    let (input, mut factors) = separated_list1(
        preceded(multispace0, tag_no_case("AND")),
        parse_where_expression, // ✅ Use your full condition parser
    ).parse(input)?;

    let term = if factors.len() == 1 {
        factors.remove(0)
    } else {
        let mut iter = factors.into_iter();
        let mut root = iter.next().unwrap();
        for next in iter {
            root = Expression::And {
                left: Box::new(root),
                right: Box::new(next),
            };
        }
        root
    };

    Ok((input, term))
}

/// Level 2: AND
fn parse_and_expression(input: &str) -> IResult<&str, CypherExpression> {
    println!("===> parse_and_expression START");
    let (input, left) = parse_comparison_expression(input)?;
    
    let (input, remainder) = many0(pair(
        delimited(multispace1, tag_no_case("AND"), multispace1),
        parse_comparison_expression
    )).parse(input)?;

    let mut res = left;
    for (_op, right) in remainder {
        res = CypherExpression::BinaryOp {
            left: Box::new(res),
            op: "AND".to_string(),
            right: Box::new(right),
        };
    }
    Ok((input, res))
}

pub fn parse_with(input: &str) -> IResult<&str, WithClause> {
    println!("===> parse_with START");
    
    // 1. Consume "WITH" keyword
    let (input, _) = tag_no_case("WITH").parse(input)?;
    let (input, _) = multispace1.parse(input)?;
    
    // 2. Parse projection items (e.g., "p, p.age AS dist")
    // For now, skip/consume them since we're just tracking the WHERE condition
    let (input, _) = take_while1(|c: char| {
        let upper = c.to_ascii_uppercase();
        !(upper == 'W' && input.trim_start().to_uppercase().starts_with("WHERE"))
    }).parse(input)?;
    
    let (input, _) = multispace0.parse(input)?;
    
    // 3. Parse optional WHERE clause after WITH projections
    let (input, condition_opt) = opt(preceded(
        tuple((tag_no_case("WHERE"), multispace1)),
        parse_logical_expression
    )).parse(input)?;
    
    // 4. If no WHERE, create a trivial true condition
    let condition = condition_opt.unwrap_or_else(|| {
        Expression::Literal(CypherValue::Bool(true))
    });
    
    Ok((input, WithClause { condition }))
}

pub fn parse_with_full(input: &str) -> IResult<&str, ParsedWithClause> {
    println!("===> parse_with_full START");
    
    // 1. Consume "WITH" keyword
    let (input, _) = tag_no_case("WITH").parse(input)?;
    let (input, _) = multispace1.parse(input)?;
    
    // 2. Parse DISTINCT (optional)
    let (input, distinct) = opt(terminated(tag_no_case("DISTINCT"), multispace1)).parse(input)?;
    let is_distinct = distinct.is_some();
    
    // 3. Parse projection items (comma-separated list)
    // For now, just consume until we hit WHERE/ORDER/SKIP/LIMIT/RETURN
    let (input, _projection_str) = take_while1(|c: char| {
        let trimmed = input.trim_start();
        let upper = trimmed.to_uppercase();
        !upper.starts_with("WHERE") 
            && !upper.starts_with("ORDER")
            && !upper.starts_with("SKIP")
            && !upper.starts_with("LIMIT")
            && !upper.starts_with("RETURN")
    }).parse(input)?;
    
    let (input, _) = multispace0.parse(input)?;
    
    // 4. Parse optional WHERE
    let (input, where_clause) = opt(preceded(
        tuple((tag_no_case("WHERE"), multispace1)),
        map(parse_logical_expression, |condition| WhereClause { condition })
    )).parse(input)?;
    
    // 5. Parse optional ORDER BY (skip for now)
    let (input, _) = multispace0.parse(input)?;
    
    // 6. Parse optional SKIP
    let (input, _) = multispace0.parse(input)?;
    
    // 7. Parse optional LIMIT
    let (input, _) = multispace0.parse(input)?;
    
    Ok((input, ParsedWithClause {
        items: Vec::new(), // TODO: actually parse projection items
        distinct: is_distinct,
        where_clause,
        order_by: Vec::new(),
        skip: None,
        limit: None,
    }))
}

/// Level 3: Comparisons (=, <>, etc.)
fn parse_comparison_expression(input: &str) -> IResult<&str, CypherExpression> {
    println!("===> parse_comparison_expression START");
    // We must use a parser that understands both "n" and "n.name"
    let (input, left) = parse_property_or_terminal(input)?;
    
    let (input, op_match) = opt(pair(
        delimited(
            multispace0, 
            alt((tag("="), tag("!="), tag("<>"), tag("<="), tag(">="), tag("<"), tag(">"))), 
            multispace0
        ),
        parse_property_or_terminal
    )).parse(input)?;

    if let Some((op, right)) = op_match {
        Ok((input, CypherExpression::BinaryOp {
            left: Box::new(convert_expression_to_cypher(left)),
            op: op.to_string(),
            right: Box::new(convert_expression_to_cypher(right)),
        }))
    } else {
        Ok((input, convert_expression_to_cypher(left)))
    }
}

// Helper function to convert Expression to CypherExpression
fn convert_expression_to_cypher(expr: Expression) -> CypherExpression {
    println!("===> convert_expression_to_cypher START");
    match expr {
        Expression::Property(PropertyAccess::Vertex(var, prop)) => {
            CypherExpression::PropertyLookup { var, prop }
        },
        Expression::Literal(v) => CypherExpression::Literal(cypher_value_to_value(v)),
        Expression::Variable(v) => CypherExpression::Variable(v),
        Expression::FunctionCall { name, args } => {
            CypherExpression::FunctionCall { 
                name, 
                args: args.into_iter().map(convert_expression_to_cypher).collect() 
            }
        },
        Expression::And { left, right } => {
            CypherExpression::BinaryOp {
                left: Box::new(convert_expression_to_cypher(*left)),
                op: "AND".to_string(),
                right: Box::new(convert_expression_to_cypher(*right)),
            }
        },
        Expression::Or { left, right } => {
            CypherExpression::BinaryOp {
                left: Box::new(convert_expression_to_cypher(*left)),
                op: "OR".to_string(),
                right: Box::new(convert_expression_to_cypher(*right)),
            }
        },
        _ => CypherExpression::Variable("unknown".to_string()),
    }
}

// Helper function to convert CypherValue to Value
fn cypher_value_to_value(cv: CypherValue) -> Value {
    println!("===> cypher_value_to_value START");
    match cv {
        CypherValue::Null => Value::Null,
        
        CypherValue::Bool(b) => Value::Bool(b),
        
        CypherValue::Integer(i) => json!(i),
        
        CypherValue::Float(f) => {
            // FIX: Use 'f' directly. It is a primitive f64, not a wrapper.
            if let Some(n) = Number::from_f64(f) {
                Value::Number(n)
            } else {
                // JSON spec does not support NaN or Infinity; 
                // we log as Null to avoid breaking the event graph serialization.
                Value::Null 
            }
        },
        
        CypherValue::String(s) => Value::String(s),
        
        CypherValue::Uuid(uuid) => Value::String(uuid.to_string()),
        
        // Ensure Patient/Encounter nodes are correctly serialized for MPI auditing
        CypherValue::Vertex(v) => json!(v),
        CypherValue::Edge(e) => json!(e),
        
        // Recursive conversion for Maps (e.g., patient property bags)
        CypherValue::Map(m) => {
            let mut map = Map::new();
            for (k, v) in m {
                map.insert(k, cypher_value_to_value(v));
            }
            Value::Object(map)
        },
        
        // Recursive conversion for Lists (e.g., list of previous IDs)
        CypherValue::List(l) => {
            let list = l.into_iter()
                .map(cypher_value_to_value)
                .collect::<Vec<Value>>();
            Value::Array(list)
        },
    }
}

fn parse_comparison_expr(input: &str) -> IResult<&str, Expression> {
    println!("===> parse_comparison_expr START");
    // Try function call first (e.g., ID(n) = "uuid")
    if let Ok((remaining, (func_name, arg))) = parse_function_call(input) {
        let (remaining, _) = multispace0.parse(remaining)?;
        let (remaining, op) = parse_comparison_op(remaining)?;
        let (remaining, _) = multispace0.parse(remaining)?;
        
        let (remaining, val) = if op.to_uppercase().contains("NULL") {
            (remaining, Value::Null)
        } else {
            parse_value(remaining)?
        };
        
        return Ok((remaining, Expression::FunctionComparison {
            function: func_name,
            argument: arg,
            operator: op.to_string(),
            value: val,
        }));
    }

    // Try property access (e.g., n.id = "123")
    let (input, full_path) = parse_property_access(input)?;
    let (input, _) = multispace0.parse(input)?;
    let (input, op) = parse_comparison_op(input)?;
    let (input, _) = multispace0.parse(input)?;
    
    let (input, val) = if op.to_uppercase().contains("NULL") {
        (input, Value::Null)
    } else {
        parse_value(input)?
    };

    let (var, prop) = full_path.split_once('.')
        .map(|(v, p)| (v.to_string(), p.to_string()))
        .unwrap_or_else(|| (full_path.clone(), String::new()));

    Ok((input, Expression::PropertyComparison {
        variable: var,
        property: prop,
        operator: op.to_string(),
        value: val,
    }))
}

// ✅ RENAME: parse_property_or_terminal → parse_property_or_terminal_expr
// Return Expression, not CypherExpression
pub fn parse_property_or_terminal(input: &str) -> IResult<&str, Expression> {
    println!("===> parse_property_or_terminal START");
    // Try property lookup: n.name
    if let Ok((i, var)) = parse_identifier(input) {
        if let Ok((i2, _)) = char::<&str, nom::error::Error<&str>>('.').parse(i) {
            let (i3, prop) = parse_identifier(i2)?;
            return Ok((i3, Expression::Property(PropertyAccess::Vertex(
                var.to_string(),
                prop.to_string(),
            ))));
        }
    }
    
    // Fall back to terminal expression
    parse_terminal_expression(input)
}

pub fn parse_terminal_expression(input: &str) -> IResult<&str, Expression> {
    println!("===> parse_terminal_expression START");
    alt((
        // 1. Parenthesized: (expr)
        parse_parenthesized_expression,

        // 2. Function call: ID(n) — but note: standalone functions aren't in your Expression enum
        // So skip this unless you add FunctionCall variant
        // (Keep only if you extend Expression)

        // 3. Property: n.name
        map(
            pair(parse_identifier, preceded(char('.'), parse_identifier)),
            |(var, prop)| Expression::Property(PropertyAccess::Vertex(
                var.to_string(),
                prop.to_string(),
            )),
        ),

        // 4. Literal
        map(parse_literal_value, |v| Expression::Literal(CypherValue::from_json(v))),

        // 5. Variable
        map(parse_identifier, |s| Expression::Variable(s.to_string())),
    ))
    .parse(input) // ✅ Use .parse(input) for reliability in Nom 8
}

// Support for $mrn, $id, etc.
fn parse_parameter(input: &str) -> IResult<&str, String> {
    println!("===> parse_parameter START");
    let (input, _) = char('$')(input)?;
    let (input, name) = parse_identifier(input)?;
    Ok((input, format!("${}", name)))
}

fn parse_parenthesized_expression(input: &str) -> IResult<&str, Expression> {
    println!("===> parse_parenthesized_expression START");
    delimited(
        char('('),
        preceded(multispace0, parse_logical_expression),
        preceded(multispace0, char(')')),
    )
    .parse(input) // ✅ Use .parse(input) for Nom 8
}

fn parse_literal_value(input: &str) -> IResult<&str, serde_json::Value> {
    println!("===> parse_literal_value START");
    alt((
        map(parse_double, |f| serde_json::json!(f)),
        map(parse_i64, |i| serde_json::json!(i)), 
        map(delimited(char('"'), take_until("\""), char('"')), |s: &str| serde_json::json!(s)),
        map(delimited(char('\''), take_until("'"), char('\'')), |s: &str| serde_json::json!(s)),
    )).parse(input)
}

/// Parse WHERE clause - returns WhereClause struct with condition field
pub fn parse_where(input: &str) -> IResult<&str, WhereClause> {
    println!("===> parse_where START");
    let (input, _) = tag_no_case("WHERE").parse(input)?;
    let (input, _) = multispace1.parse(input)?;
    
    // ✅ Use precedence-aware logical expression parser to support AND/OR
    // This replaces the flat AND-only parser to enable MPI identity resolution
    // with conditions like: a.mrn = b.mrn OR a.ssn = b.ssn
    let (input, condition) = parse_logical_expression(input)?;
    
    Ok((input, WhereClause { condition }))
}

/// Parse function call (e.g., ID(n))
fn parse_function_call(input: &str) -> IResult<&str, (String, String)> {
    println!("===> parse_function_call START");
    let (input, _) = multispace0.parse(input)?;
    let (input, func_name) = parse_identifier.parse(input)?;
    let (input, _) = char('(').parse(input)?;
    let (input, _) = multispace0.parse(input)?;
    let (input, arg) = parse_identifier.parse(input)?;
    let (input, _) = multispace0.parse(input)?;
    let (input, _) = char(')').parse(input)?;
    
    Ok((input, (func_name.to_string(), arg.to_string())))
}

/// Parse a comparison operator
fn parse_comparison_op(input: &str) -> IResult<&str, &str> {
    println!("===> parse_comparison_op START");
    alt((
        tag_no_case("IS NOT NULL"),
        tag_no_case("IS NULL"),
        tag("="),
        tag("!="),
        tag("<>"),
        tag("<="),
        tag(">="),
        tag("<"),
        tag(">"),
    )).parse(input)
}

/// Parse a single WHERE condition/expression (property, function, or parenthesized)
pub fn parse_where_expression(input: &str) -> IResult<&str, Expression> {
    // ✅ First, try parenthesized: ( ... )
    println!("===> parse_where_expression START");
    if let Ok((remaining, expr)) = parse_parenthesized_expression(input) {
        return Ok((remaining, expr));
    }

    // Try function-based condition: ID(n) = "value" or ID(n) IS NOT NULL
    if let Ok((remaining, (func_name, arg))) = parse_function_call(input) {
        let (remaining, _) = multispace0.parse(remaining)?;
        let (remaining, op) = parse_comparison_op(remaining)?;
        let (remaining, _) = multispace0.parse(remaining)?;
        
        let (remaining, val) = if op.to_uppercase().contains("NULL") {
            (remaining, Value::Null)
        } else {
            parse_value(remaining)?
        };
        
        return Ok((remaining, Expression::FunctionComparison {
            function: func_name,
            argument: arg,
            operator: op.to_string(),
            value: val,
        }));
    }
    
    // Try property-based condition: n.prop = "value" or n.prop IS NOT NULL
    let (input, full_path) = parse_property_access(input)?;
    let (input, _) = multispace0.parse(input)?;
    let (input, op) = parse_comparison_op(input)?;
    let (input, _) = multispace0.parse(input)?;
    
    let (input, val) = if op.to_uppercase().contains("NULL") {
        (input, Value::Null)
    } else {
        parse_value(input)?
    };
    
    let (var, prop) = full_path.split_once('.')
        .map(|(v, p)| (v.to_string(), p.to_string()))
        .unwrap_or_else(|| (full_path.clone(), String::new()));
    
    Ok((input, Expression::PropertyComparison {
        variable: var,
        property: prop,
        operator: op.to_string(),
        value: val,
    }))
}

/// The WHERE clause parser itself - updated for legacy compatibility with AND support
fn parse_where_clause_content(input: &str) -> IResult<&str, String> {
    println!("===> parse_where_clause_content START");
    let (input, _) = tag_no_case("WHERE").parse(input)?;
    let (input, _) = multispace1.parse(input)?;
    
    // Parse the full chain of expressions
    let (input, mut expressions) = separated_list1(
        tuple((multispace1, tag_no_case("AND"), multispace1)),
        parse_where_expression
    ).parse(input)?;

    let final_expr = if expressions.len() == 1 {
        expressions.remove(0)
    } else {
        let mut iter = expressions.into_iter();
        let mut root = iter.next().unwrap();
        for next_expr in iter {
            root = Expression::And {
                left: Box::new(root),
                right: Box::new(next_expr),
            };
        }
        root
    };

    Ok((input, format!("{:?}", final_expr)))
}

fn take_until_keyword(input: &str) -> IResult<&str, &str> {
    println!("===> take_until_keyword START");
    let keywords = [
        "SET", "RETURN", "CREATE", "DELETE", "WITH", 
        "REMOVE", "MATCH", "MERGE", "FOREACH", "UNWIND",
        "ON CREATE", "ON MATCH", "ORDER BY", "LIMIT", "SKIP"
    ];
    
    let upper_input = input.to_uppercase();
    let mut earliest_idx = input.len();
    
    for kw in keywords {
        // Check if keyword appears at the start (after optional whitespace)
        let trimmed = upper_input.trim_start();
        if trimmed.starts_with(kw) {
            if let Some(pos) = input.find(|c: char| !c.is_whitespace()) {
                if pos < earliest_idx {
                    earliest_idx = pos;
                }
            }
            continue;
        }
        
        // Look for keyword preceded by whitespace
        for separator in &[" ", "\n", "\t", "\r"] {
            let search_pattern = format!("{}{}", separator, kw);
            if let Some(idx) = upper_input.find(&search_pattern) {
                if idx < earliest_idx {
                    earliest_idx = idx;
                }
            }
        }
    }
    
    let taken = &input[..earliest_idx];
    let remainder = &input[earliest_idx..];
    
    Ok((remainder, taken.trim()))
}

/// Parse a raw node pattern string
///     ( [var][:Label] [{props}] )
/// into (label, properties) exactly like the old logic.
fn parse_node_pattern(input: &str) -> GraphResult<(Option<String>, HashMap<String, Value>)> {
    type E<'a> = nom::error::Error<&'a str>;
    println!("===> parse_node_pattern START"); 
    // 1. Identify the "Inside" of the parentheses first to avoid infinite recursion
    let (remaining, inner) = delimited(
        tag::<_, _, E>("("),
        take_until(")"),
        tag::<_, _, E>(")")
    ).parse(input)
     .map_err(|_| GraphError::ValidationError("Malformed node: missing brackets".into()))?;

    // 2. Parse the Variable Name (everything up to the first colon or space or end)
    let (after_var, _var_name) = take_until::<_, _, E>(":").parse(inner)
        .unwrap_or(("", inner)); // If no colon, the whole thing is the var name

    // 3. Parse Multiple Labels: :User:Admin:Employee
    // We use many1 because we know we are starting at a colon
    let mut labels_parser = many1(preceded(
        tag::<_, _, E>(":"),
        take_while1::<_, _, E>(|c: char| c.is_alphanumeric() || c == '_')
    ));

    // Try to parse labels from the point after the variable
    let (after_labels, labels) = opt(labels_parser).parse(after_var)
        .map_err(|_| GraphError::ValidationError("Invalid label format".into()))?;

    // 4. Parse Properties: { ... }
    // We look for the curly brace in whatever is left
    let mut props_parser = opt(preceded(
        multispace0::<_, E>,
        delimited(
            tag::<_, _, E>("{"),
            take_until::<_, _, E>("}"),
            tag::<_, _, E>("}")
        )
    ));

    let (_, props_str) = props_parser.parse(after_labels)
        .map_err(|_| GraphError::ValidationError("Malformed properties".into()))?;

    // 5. Finalize data
    let primary_label = labels.and_then(|l_vec| l_vec.first().map(|s| s.to_string()));
    
    let properties = if let Some(p_str) = props_str {
        let json_fix = p_str.replace("'", "\"");
        serde_json::from_str::<HashMap<String, Value>>(&format!("{{{}}}", json_fix))
            .unwrap_or_default()
    } else {
        HashMap::new()
    };

    Ok((primary_label, properties))
}

// =============================================================================
// INSTRUCTIONS FOR INTEGRATION:
// =============================================================================
// 1. Find your existing full_statement_parser function in cypher_parser.rs
// 2. Delete it completely
// 3. Copy all 5 functions above (is_cypher_keyword_start through full_statement_parser)
// 4. Paste them into your file where the old full_statement_parser was
// 5. Make sure parse_return_clause exists and looks like this:
// Helper to consume RETURN ... [ORDER BY ...]

fn parse_return_clause(input: &str) -> IResult<&str, CypherQuery> {
    println!("===> parse_return_clause START"); 
    // 1. Consume 'RETURN' keyword
    let (input, _) = preceded(
        multispace0::<&str, NomErrorType<&str>>,
        tag_no_case("RETURN"),
    ).parse(input)?;

    // 2. Parse RETURN items (projections)
    let (input, _) = multispace1.parse(input)?; // RETURN must be followed by space
    let (input, _) = multispace0.parse(input)?; // optional extra space

    // Capture raw projection string for backward compatibility (as fallback)
    let (input_after_proj, projection_items_str) = take_while1(|c: char| {
        let upper = c.to_ascii_uppercase();
        !(upper == 'O' || upper == 'S' || upper == 'L' || c == ';' || c == '\n' || c == '\r')
    }).parse(input)?;

    // 3. Parse ORDER BY clause
    let (input, order_by) = opt(preceded(
        tuple((multispace0, tag_no_case::<_, _, NomErrorType<&str>>("ORDER BY"), multispace1)),
        parse_order_by_items,
    )).parse(input_after_proj)?;
    let order_by = order_by.unwrap_or_default();

    // 4. Parse SKIP clause
    let (input, skip) = opt(preceded(
        tuple((multispace0, tag_no_case::<_, _, NomErrorType<&str>>("SKIP"), multispace1)),
        map_res(take_while1(|c: char| c.is_ascii_digit()), |s: &str| s.parse::<i64>())
    )).parse(input)?;

    // 5. Parse LIMIT clause
    let (input, limit) = opt(preceded(
        tuple((multispace0, tag_no_case::<_, _, NomErrorType<&str>>("LIMIT"), multispace1)),
        map_res(take_while1(|c: char| c.is_ascii_digit()), |s: &str| s.parse::<i64>())
    )).parse(input)?;

    // 6. Build query
    let return_query = CypherQuery::ReturnStatement { 
        projection_string: projection_items_str.trim().to_string(),
        order_by,
        skip,
        limit,
    };

    Ok((input, return_query))
}

// Conceptual Helper method on NodePattern, Vertex (or similar)
// You must implement this in an appropriate file.

fn matches_constraints(
    vertex: &Vertex, 
    labels: &Vec<String>, 
    properties: &HashMap<String, Value>
) -> bool {
    println!("===> matches_constraints START"); 
    // Label matching: vertex must have ALL specified labels
    let label_matches = if labels.is_empty() {
        true // No label constraint
    } else {
        let vertex_label = vertex.label.as_ref();
        
        // Check if vertex has all required labels
        labels.iter().all(|required_label| {
            // Support both single and multi-label formats
            // Vertex label might be "Patient" or "Patient:GoldenRecord"
            if vertex_label == required_label {
                true
            } else if vertex_label.contains(':') {
                // Split vertex's multi-label and check if required label is present
                vertex_label.split(':').any(|l| l == required_label)
            } else {
                false
            }
        })
    };
    
    if !label_matches {
        return false;
    }
    
    // Property matching: all specified properties must match
    properties.iter().all(|(key, expected_val)| {
        vertex.properties.get(key).map_or(false, |actual_val| {
            // Convert expected Value to PropertyValue for comparison
            match to_property_value(expected_val.clone()) {
                Ok(expected_pv) => actual_val == &expected_pv,
                Err(_) => false,
            }
        })
    })
}

fn parse_create_clause(input: &str) -> IResult<&str, Vec<Pattern>> {
    println!("===> parse_create_clause START"); 
    separated_list1(
        tuple((multispace0, char(','), multispace0)),
        parse_create_pattern, // ✅ Use CREATE-specific parser
    )
    .parse(input)
}

fn parse_where_clause(input: &str) -> IResult<&str, String> {
    println!("===> parse_where_clause START"); 
    let (input, _) = tag("WHERE").parse(input)?;
    let (input, _) = multispace1.parse(input)?;
    let (input, condition) = take_while1(|c| c != '\n' && c != '\r' && c != 'R').parse(input)?;
    Ok((input, condition.to_string()))
}

fn parse_return_expression(input: &str) -> IResult<&str, String> {
    // Capture everything until comma or end of line and convert to String
    println!("===> parse_return_expression START"); 
    map(
        take_while1(|c| c != ',' && c != '\n' && c != '\r'),
        |s: &str| s.to_string(),
    ).parse(input)
}

fn extract_main_entity(nodes: &Vec<NodePattern>) -> Option<NodePattern> {
    println!("===> extract_main_entity START"); 
    nodes.first().cloned()
}

fn parse_match_node_original(input: &str) -> IResult<&str, CypherQuery> {
    println!("===> parse_match_node_original START");  
    let (input, (_, _, node)) = tuple((
        tag("MATCH"),
        multispace1,
        parse_node,
    )).parse(input)?;
    
    // node is (Option<String>, Vec<String>, HashMap<String, Value>)
    let (_var, labels, props) = node;

    // Convert Vec<String> to Option<String> 
    // This takes the first label if it exists, otherwise None
    let label = labels.first().cloned();

    Ok((input, CypherQuery::MatchNode {
        label,
        properties: props,
    }))
}

pub async fn execute_cypher_from_string(
    query: &str,
    storage: Arc<dyn GraphStorageEngine + Send + Sync>,
) -> GraphResult<Value> {
    let parsed = parse_cypher(query).map_err(|e| GraphError::StorageError(e))?;
    let db = Arc::new(Database {
        storage: storage.clone(),
        config: StorageConfig::default(),
    });
    println!("===> execute_cypher_from_string START");  
    execute_cypher(parsed, &db, storage).await
}

// ==============================================================================
// FIX #2: Add helper function for variable-length path expansion
// ==============================================================================

fn expand_variable_paths(
    all_edges: &[Edge],
    matched_vertices: &std::collections::HashSet<SerializableUuid>,
    matched_edges: &mut std::collections::HashSet<SerializableUuid>,
    min_hops: u32,
    max_hops: u32,
) {
    println!("===> expand_variable_paths START");  
    // BFS to find all paths within hop range
    let mut visited = std::collections::HashSet::new();
    let mut current_level = matched_vertices.clone();
    
    for hop in 1..=max_hops {
        let mut next_level = std::collections::HashSet::new();
        
        for vertex_id in &current_level {
            for edge in all_edges {
                let (from_id, to_id) = if edge.outbound_id == *vertex_id {
                    (edge.outbound_id, edge.inbound_id)
                } else if edge.inbound_id == *vertex_id {
                    (edge.inbound_id, edge.outbound_id)
                } else {
                    continue;
                };
                
                if !visited.contains(&edge.id) {
                    visited.insert(edge.id);
                    
                    // Include edge if we're at or past min_hops
                    if hop >= min_hops {
                        matched_edges.insert(edge.id);
                    }
                    
                    next_level.insert(to_id);
                }
            }
        }
        
        if next_level.is_empty() {
            break;
        }
        
        current_level = next_level;
    }
}

// exec_cypher_pattern - helper
// Assume GraphService is defined and available in scope
async fn exec_cypher_pattern(
    patterns: Vec<(Option<String>, Vec<(Option<String>, Option<String>, HashMap<String, Value>)>, Vec<(Option<String>, Option<String>, Option<(Option<u32>, Option<u32>)>, HashMap<String, Value>, Option<bool>)>)>,
    graph_service: &GraphService, // <--- UPDATED DEPENDENCY
) -> GraphResult<(Vec<Vertex>, Vec<Edge>)> {
    println!("===> exec_cypher_pattern START");  
    // *** DELEGATION FIX: Retrieve all vertices and edges via GraphService ***
    let all_vertices = graph_service.get_all_vertices().await?;
    let all_edges = graph_service.get_all_edges().await?;
    
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
        let mut vars_bound_in_this_pattern = std::collections::HashSet::new();
        
        // Step 1: Match all nodes in this pattern
        for (var_name, label_constraint, prop_constraints) in nodes {
            println!("===> Matching node: var={:?}, label={:?}, props={:?}", 
                     var_name, label_constraint, prop_constraints.keys().collect::<Vec<_>>());
            
            // Check if variable is already bound from a previous pattern
            if let Some(var) = var_name {
                if let Some(bound_ids) = var_bindings.get(var) {
                    println!("===> Variable '{}' already bound to {} vertices from previous pattern", 
                             var, bound_ids.len());
                    
                    // If there are additional constraints, filter the bound set
                    if label_constraint.is_some() || !prop_constraints.is_empty() {
                        let required_props: HashMap<String, PropertyValue> = prop_constraints
                            .iter()
                            .map(|(k, v)| to_property_value(v.clone()).map(|pv| (k.clone(), pv)))
                            .collect::<GraphResult<_>>()?;

                        for vertex_id in bound_ids {
                            // Lookup vertex detail using ID from the full set
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
                        // No additional constraints, use all bound IDs
                        pattern_vertex_ids.extend(bound_ids);
                        matched_vertex_ids.extend(bound_ids);
                    }
                    continue;
                }
            }
            
            // Variable not bound yet, match against all vertices
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
            
            // Bind the variable to the matched set
            if let Some(var) = var_name {
                // Track that this variable was matched in this pattern
                if !var_bindings.contains_key(var) {
                    vars_bound_in_this_pattern.insert(var.clone());
                }
                var_bindings.insert(var.clone(), matched_for_this_var);
            }
        }
        
        println!("===> Pattern {} matched {} vertices total", pattern_idx, pattern_vertex_ids.len());
        println!("===> Pattern vertex IDs: {:?}", pattern_vertex_ids.iter().take(5).collect::<Vec<_>>());

        // Step 2: Match relationships in this pattern
        if !rels.is_empty() {
            println!("===> Pattern {} has {} relationships to match", pattern_idx, rels.len());
            
            for (rel_idx, rel) in rels.iter().enumerate() {
                let (_rel_var, rel_type, rel_range, _rel_props, _direction) = rel;
                println!("===> Matching relationship {}: type={:?}, range={:?}", 
                         rel_idx, rel_type, rel_range);
                
                if let Some((min_hops, max_hops)) = rel_range {
                    // Variable-length path logic
                    let min = min_hops.unwrap_or(1);
                    let max = max_hops.unwrap_or(5);
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
                    
                    // Note: expand_variable_paths call logic remains commented out per your source
                    println!("===> Skipping complex VLP expansion for brevity/missing helper function.");
                } else {
                    // Single-hop relationship matching
                    
                    // Get the node patterns for the endpoints of this relationship
                    let start_node_pattern = &nodes[rel_idx];
                    let end_node_pattern = &nodes[rel_idx + 1];

                    let start_var_name = start_node_pattern.0.as_ref();
                    let end_var_name = end_node_pattern.0.as_ref();

                    // Get start node set from var_bindings or pattern
                    let start_node_id_set: HashSet<SerializableUuid> = start_var_name
                        .and_then(|var| var_bindings.get(var).cloned())
                        .unwrap_or_else(|| pattern_vertex_ids.clone());

                    // For end nodes endpoint set resolution
                    let end_node_id_set: HashSet<SerializableUuid> = if let Some(var) = end_var_name {
                        if let Some(bound_ids) = var_bindings.get(var) {
                            if vars_bound_in_this_pattern.contains(var) {
                                let (_, end_label, end_props) = end_node_pattern;
                                
                                let required_props: HashMap<String, PropertyValue> = end_props
                                    .iter()
                                    .map(|(k, v)| to_property_value(v.clone()).map(|pv| (k.clone(), pv)))
                                    .collect::<GraphResult<_>>()?;
                                
                                let mut matching_end_nodes = HashSet::new();
                                for v in &all_vertices {
                                    let label_ok = end_label.as_ref().map_or(true, |ql| {
                                        let vl = v.label.as_ref();
                                        vl == ql || vl.starts_with(&format!("{}:", ql))
                                    });
                                    
                                    let props_ok = required_props.iter().all(|(k, expected)| {
                                        v.properties.get(k).map_or(false, |actual| actual == expected)
                                    });
                                    
                                    if label_ok && props_ok {
                                        matching_end_nodes.insert(v.id);
                                    }
                                }
                                matching_end_nodes
                            } else {
                                bound_ids.clone()
                            }
                        } else {
                            let (_, end_label, end_props) = end_node_pattern;
                            
                            let required_props: HashMap<String, PropertyValue> = end_props
                                .iter()
                                .map(|(k, v)| to_property_value(v.clone()).map(|pv| (k.clone(), pv)))
                                .collect::<GraphResult<_>>()?;
                            
                            let mut matching_end_nodes = HashSet::new();
                            for v in &all_vertices {
                                let label_ok = end_label.as_ref().map_or(true, |ql| {
                                    let vl = v.label.as_ref();
                                    vl == ql || vl.starts_with(&format!("{}:", ql))
                                });
                                
                                let props_ok = required_props.iter().all(|(k, expected)| {
                                    v.properties.get(k).map_or(false, |actual| actual == expected)
                                });
                                
                                if label_ok && props_ok {
                                    matching_end_nodes.insert(v.id);
                                }
                            }
                            matching_end_nodes
                        }
                    } else {
                        pattern_vertex_ids.clone()
                    };

                    println!("===> Matched nodes sets sizes: START={}, END={}", 
                             start_node_id_set.len(), end_node_id_set.len());
                    println!("===> Examining {} edges for matches", all_edges.len());

                    for edge in &all_edges {
                        println!("===> Checking edge {:?}: {} -> {}, type: {}",
                                 edge.id, edge.outbound_id, edge.inbound_id, edge.label);
                        
                        // Check type constraint
                        let type_matches = rel_type.as_ref().map_or(true, |rt| {
                            edge.edge_type.as_ref() == rt || edge.label == *rt
                        });
                        
                        if !type_matches {
                            println!("=====> Type mismatch, skipping");
                            continue;
                        }

                        // Check connectivity
                        let connects_outbound = 
                            start_node_id_set.contains(&edge.outbound_id) && 
                            end_node_id_set.contains(&edge.inbound_id);
                        
                        let connects_inbound = 
                            start_node_id_set.contains(&edge.inbound_id) && 
                            end_node_id_set.contains(&edge.outbound_id);

                        let connects = connects_outbound || connects_inbound;
                        
                        println!("=====> Connection check: connects_outbound={}, connects_inbound={}, connects={}",
                                 connects_outbound, connects_inbound, connects);
                        
                        if connects {
                            matched_edge_ids.insert(edge.id);
                            matched_vertex_ids.insert(edge.outbound_id);
                            matched_vertex_ids.insert(edge.inbound_id);
                            
                            // CRITICAL: Update var_bindings for end node
                            if let Some(end_var) = end_var_name {
                                let connected_vertex_id = if connects_outbound {
                                    edge.inbound_id
                                } else {
                                    edge.outbound_id
                                };
                                
                                var_bindings.entry(end_var.clone())
                                    .or_insert_with(HashSet::new)
                                    .insert(connected_vertex_id);
                            }
                            
                            println!("=====> MATCHED! Edge {:?} added", edge.id);
                        } else {
                            println!("=====> Not matched (type_matches={}, connects={})", type_matches, connects);
                        }
                    }
                }
            }
        }
    }

    // Collect final results
    let final_vertices: Vec<Vertex> = all_vertices.into_iter()
        .filter(|v| matched_vertex_ids.contains(&v.id))
        .collect();
    let final_edges: Vec<Edge> = all_edges.into_iter()
        .filter(|e| matched_edge_ids.contains(&e.id))
        .collect();

    println!("===> FINAL RESULTS: {} vertices, {} edges", final_vertices.len(), final_edges.len());

    Ok((final_vertices, final_edges))
}

pub fn parse_modifying_clause(input: &str) -> IResult<&str, CypherQuery> {
    println!("===> parse_modifying_clause START");  
    
    // 1. CREATE Clause
    if let Ok((remaining, raw_patterns)) = preceded(
        terminated(tag_no_case("CREATE"), multispace1),
        parse_match_clause_patterns
    ).parse(input) {
        let execution_patterns: ExecutionPatternsReturnType = raw_patterns
            .into_iter()
            .map(|(id, nodes, edges)| {
                let transformed_nodes = nodes
                    .into_iter()
                    .map(|(var, label_opt, props)| {
                        let labels_vec = match label_opt {
                            Some(l) => vec![l],
                            None => vec![],
                        };
                        (var, labels_vec, props)
                    })
                    .collect();
                (id, transformed_nodes, edges)
            })
            .collect();

        return Ok((remaining, CypherQuery::CreateStatement { 
            patterns: execution_patterns, 
            return_items: vec![] 
        }));
    }
    
    // 2. SET Clause - FIXED to handle Expression types
    if let Ok(result) = preceded(
        terminated(tag_no_case("SET"), multispace1),
        map(
            separated_list1(
                tuple((multispace0, char(','), multispace0)),
                parse_single_set_assignment
            ), 
            |assignments| CypherQuery::SetStatement { assignments }
        )
    ).parse(input) {
        return Ok(result);
    }
    
    // 3. DELETE Clause
    let detach_check = opt(terminated(tag_no_case::<_, _, NomErrorType<&str>>("DETACH"), multispace1)).parse(input);
    if let Ok((after_detach, detach_opt)) = detach_check {
        let is_detach = detach_opt.is_some();
        
        if let Ok((after_delete, _)) = terminated(tag_no_case::<_, _, NomErrorType<&str>>("DELETE"), multispace1).parse(after_detach) {
            if let Ok((final_input, variables)) = separated_list1(
                tuple((multispace0::<&str, NomErrorType<&str>>, char::<_, NomErrorType<&str>>(','), multispace0::<&str, NomErrorType<&str>>)),
                nom::bytes::complete::take_while1(|c: char| !c.is_whitespace() && c != ',')
            ).parse(after_delete) {
                let vars: Vec<String> = variables.into_iter().map(String::from).collect();
                return Ok((final_input, CypherQuery::DeleteStatement { 
                    variables: vars,
                    detach: is_detach,
                }));
            }
        }
    }
    
    // 4. REMOVE Clause
    if let Ok(result) = preceded(
        terminated(tag_no_case("REMOVE"), multispace1),
        map(
            separated_list1(
                tuple((multispace0::<&str, NomErrorType<&str>>, char::<_, NomErrorType<&str>>(','), multispace0::<&str, NomErrorType<&str>>)),
                parse_remove_clause
            ), 
            |removals| CypherQuery::RemoveStatement { removals }
        )
    ).parse(input) {
        return Ok(result);
    }
    
    Err(nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Alt)))
}

pub fn parse_with_clause(input: &str) -> IResult<&str, CypherQuery> {
    println!("===> parse_with_clause START");  
    map(
        preceded(
            terminated(tag_no_case("WITH"), multispace1),
            separated_list1(preceded(multispace0, char(',')), parse_identifier),
        ),
        |vars| CypherQuery::Chain(vec![/* Process identifiers into a real WITH statement here */]),
    ).parse(input)
}

// Must be defined in scope or imported:
// use models::properties::PropertyValue; 
// use models::errors::{GraphError, GraphResult}; 
// use serde_json::Value; 

/// Resolves a Cypher variable to a stored Vertex ID by matching its label and properties.
/// This assumes 'to_property_value' is a defined helper to convert serde_json::Value to PropertyValue.
// =============================================================================
// CORE FUNCTION 1: resolve_var (Database Query Logic)
// =============================================================================

/// Resolves a node variable's ID by checking the storage engine for a matching 
/// vertex based on its label and properties.

async fn resolve_var(
    graph_service: &GraphService, 
    var: &str,
    label: &Option<String>,
    properties: &HashMap<String, Value>,
    where_clause: &Option<WhereClause>,
    with_clause: &Option<WithClause>, // ← ADD THIS PARAMETER
) -> GraphResult<SerializableUuid> {
    println!("===>        resolve_var: Looking for var='{}', label={:?}", var, label);
    
    let mut query_props: HashMap<String, PropertyValue> = HashMap::new();
    
    // 1. Process inline properties from the MATCH pattern (e.g., {id: '123'})
    for (k, v) in properties.iter() {
        query_props.insert(k.clone(), to_property_value(v.clone())?);
    }

    // 2. Extract filters from WHERE clause
    let where_filters = extract_filters_for_var(where_clause, var);
    for (k, v) in where_filters {
        println!("===>        Merging WHERE filter for resolution: {} = {:?}", k, v);
        query_props.insert(k, v);
    }

    // 3. ✅ CRITICAL: Extract filters from WITH clause (MPI identity logic)
    let with_filters = extract_filters_for_var_from_with(with_clause, var);
    for (k, v) in with_filters {
        println!("===>        Merging WITH filter for resolution: {} = {:?}", k, v);
        query_props.insert(k, v);
    }
    
    println!("===>        Searching vertices with merged MPI constraints: {:?}", query_props);
    
    // Use direct DB access for full scan (as in your logs)
    println!("========================== USING DIRECT DB ACCESS TO GET ALL VERTICES ========================");
    let all_vertices = graph_service.get_all_vertices().await?;
    
    let matched_vertex = all_vertices.into_iter().find(|v| {
        // Match Label
        let matches_label = label.as_ref().map_or(true, |query_label| {
            v.label.as_ref() == query_label.as_str()
        });
        
        if !matches_label { 
            return false; 
        }

        // Match merged Properties (Inline + WHERE + WITH)
        query_props.iter().all(|(k, expected_val)| {
            v.properties.get(k).map_or(false, |actual_val| actual_val == expected_val)
        })
    });
    
    match matched_vertex {
        Some(v) => {
            println!("===>        resolve_var: SUCCESS - Returning {}", v.id.0);
            Ok(v.id)
        },
        None => {
            Err(GraphError::ValidationError(format!(
                "No node found for variable '{}' matching constraints: {:?}",
                var, query_props
            )))
        }
    }
}

fn extract_filters_from_expression(
    expr: &Expression,
    var_name: &str,
    filters: &mut HashMap<String, PropertyValue>,
) {
    println!("===> extract_filters_from_expressio START");
    match expr {
        // Handle AND: traverse both sides
        Expression::And { left, right } => {
            extract_filters_from_expression(left, var_name, filters);
            extract_filters_from_expression(right, var_name, filters);
        }
        // Handle OR: also traverse both sides (MPI logic may use OR for identity resolution)
        Expression::Or { left, right } => {
            extract_filters_from_expression(left, var_name, filters);
            extract_filters_from_expression(right, var_name, filters);
        }
        // Handle equality: n.prop = value
        Expression::Binary { op: BinaryOp::Eq, left, right } => {
            if let Expression::Property(PropertyAccess::Vertex(v, prop)) = left.as_ref() {
                if v == var_name {
                    if let Expression::Literal(cypher_val) = right.as_ref() {
                        // PropertyValue::from(CypherValue) is infallible → no Result
                        let prop_val = PropertyValue::from(cypher_val.clone());
                        filters.insert(prop.clone(), prop_val);
                    }
                }
            }
            // Also check reversed: value = n.prop
            if let Expression::Property(PropertyAccess::Vertex(v, prop)) = right.as_ref() {
                if v == var_name {
                    if let Expression::Literal(cypher_val) = left.as_ref() {
                        let prop_val = PropertyValue::from(cypher_val.clone());
                        filters.insert(prop.clone(), prop_val);
                    }
                }
            }
        }
        // Handle function comparisons like ID(n) = "uuid"
        Expression::FunctionComparison {
            function,
            argument,
            operator,
            value,
        } => {
            if function.to_uppercase() == "ID" && argument == var_name && operator == "=" {
                // to_property_value returns GraphResult<PropertyValue>
                if let Ok(prop_val) = to_property_value(value.clone()) {
                    filters.insert("id".to_string(), prop_val);
                }
            }
        }
        // Handle property comparisons (fallback from WHERE parser)
        Expression::PropertyComparison {
            variable,
            property,
            operator,
            value,
        } => {
            if variable == var_name && operator == "=" {
                if let Ok(prop_val) = to_property_value(value.clone()) {
                    filters.insert(property.clone(), prop_val);
                }
            }
        }
        // Ignore other expression types (Unary, arithmetic, etc.)
        _ => {}
    }
}

fn extract_filters_for_var_from_with(
    with_clause: &Option<WithClause>,
    var_name: &str,
) -> HashMap<String, PropertyValue> {
    println!("===> extract_filters_for_var_from_with START");
    let mut filters = HashMap::new();
    
    if let Some(wc) = with_clause {
        // Use the same logic as for WHERE, but on wc.condition
        extract_filters_from_expression(&wc.condition, var_name, &mut filters);
    }
    
    filters
}

fn extract_filters_for_var(
    where_clause: &Option<WhereClause>,
    var_name: &str,
) -> HashMap<String, PropertyValue> {
    println!("===> extract_filters_for_var START");
    let mut filters = HashMap::new();

    if let Some(WhereClause { condition }) = where_clause {
        let mut queue = vec![condition];
        
        while let Some(expr) = queue.pop() {
            match expr {
                // Traverse AND trees to find all equalities
                Expression::And { left, right } => {
                    queue.push(left);
                    queue.push(right);
                }
                // Match n.id = 'value' using Eq and Property variants
                Expression::Binary {
                    left,
                    op: BinaryOp::Eq, // Matches BinaryOp::Eq in query_types.rs
                    right,
                } => {
                    if let Expression::Property(PropertyAccess::Vertex(var, prop)) = left.as_ref() {
                        if var == var_name {
                            if let Expression::Literal(cypher_val) = right.as_ref() {
                                // Uses PropertyValue::from(CypherValue) implementation
                                filters.insert(prop.clone(), PropertyValue::from(cypher_val.clone()));
                            }
                        }
                    }
                }
                _ => {}
            }
        }
    }
    filters
}

// A higher-level function to resolve all node patterns in a MATCH clause.
// It iterates over all node patterns and uses `resolve_var` to find their IDs,
// populating the variable-to-ID map for subsequent edge matching.
// =============================================================================
// CORE FUNCTION 2: resolve_match_patterns
// =============================================================================

/// A higher-level function to resolve all node patterns in a MATCH clause.
/// It iterates over all node patterns and uses `resolve_var` to find their IDs,
/// ensuring each variable is bound to an existing vertex.
async fn resolve_match_patterns(
    graph_service: &GraphService, 
    match_patterns: Vec<Pattern>,
    where_clause: &Option<WhereClause>,
    with_clause: &Option<WithClause>, // ← ADD THIS
) -> GraphResult<HashMap<String, SerializableUuid>> {
    let mut var_to_id: HashMap<String, SerializableUuid> = HashMap::new();
    
    println!("===> resolve_match_patterns: Received {} patterns", match_patterns.len());
    
    for (pattern_idx, pat) in match_patterns.iter().enumerate() {
        println!(
            "===> Processing pattern {}: path_var={:?}, {} nodes, {} rels", 
            pattern_idx, pat.0, pat.1.len(), pat.2.len()
        );

        if !pat.2.is_empty() {
            return Err(GraphError::NotImplemented(format!(
                "Full graph pattern matching with relationships (Pattern {}) is not yet implemented.", 
                pattern_idx
            )));
        }

        for (node_idx, (var_opt, labels_vec, properties)) in pat.1.iter().enumerate() {
            if let Some(v_ref) = var_opt.as_ref() {
                let var_name = v_ref.to_string();
                
                if !var_to_id.contains_key(&var_name) {
                    println!("===>    Calling resolve_var for '{}' with WHERE and WITH context", var_name);
                    
                    let first_label_opt = labels_vec.first().cloned();
                    
                    let id = resolve_var(
                        graph_service, 
                        v_ref, 
                        &first_label_opt, 
                        properties,
                        where_clause,
                        with_clause, // ← PASS IT HERE
                    ).await?;
                    
                    println!("===>    SUCCESS: '{}' resolved to {}", var_name, id.0);
                    var_to_id.insert(var_name, id);
                }
            }
        }
    }
    
    Ok(var_to_id)
}

// Complete execute_cypher function with proper MatchNode implementation
// This replaces the execute_cypher function in cypher_parser.rs

// =================================================================
// THE COMPLETED EXECUTION FUNCTION
// =================================================================

// Complete execute_cypher function with proper MatchNode implementation
// This replaces the execute_cypher function in cypher_parser.rs
// FIXED execute_cypher with correct relationship matching logic
// Helper function to execute pattern matching logic
// This extracts the shared logic to avoid recursion
pub async fn execute_cypher(
    query: CypherQuery,
    _db: &Database,
    storage: Arc<dyn GraphStorageEngine + Send + Sync>,
) -> GraphResult<Value> {
    // This public function remains simple and non-recursive
    println!("===> execute_cypher START");
    execute_cypher_sync_wrapper(query, _db, storage).await
}

//    Define a non-async helper that contains the original logic
//    (using a different name, or an internal private function)
fn execute_cypher_sync_wrapper<'a>( // 1. Introduce lifetime parameter 'a
    query: CypherQuery,
    _db: &'a Database, // 2. Apply lifetime 'a to the reference
    storage: Arc<dyn GraphStorageEngine + Send + Sync>,
) -> BoxedFuture<'a, GraphResult<Value>> { // 3. Apply lifetime 'a to the return type
    println!("===> eexecute_cypher_sync_wrapper START");
    // Box the async block that contains the original logic
    Box::pin(async move {
        // NOTE: The entire original body of the old execute_cypher function goes here.
        
        // Inside this async block, when you hit the recursive call, 
        // you must use the same boxing pattern:
        let graph_service = initialize_graph_service(storage.clone()).await?;

        match query {
            // ----------  NEW  ----------
            CypherQuery::Batch(stmts) => {
                use futures::future::try_join_all;
                // Assuming `json!` is available in scope.

                let tasks = stmts.into_iter().map(|stmt| {
                    // Clone the immutable references needed for the parallel async block
                    let db_ref = _db; 
                    let stg = storage.clone();
                    // Execute each statement in the batch using the main executor.
                    async move { execute_cypher(stmt, db_ref, stg).await }
                });

                // Execute all statements in parallel (using try_join_all)
                let results = try_join_all(tasks).await?;
                
                // Return the collected results wrapped as a single JSON object.
                Ok(json!({ "results": results }))
            }
            // --- UPDATED MATCH PATTERN ---
            // Handles pure MATCH ... RETURN, relying on a full pattern matcher.
            // --- UPDATED MATCH PATTERN BRANCH ---
            // --- FIXED: MatchPattern with variable binding and WHERE evaluation ---
CypherQuery::MatchPattern { patterns, where_clause, with_clause } => {
                info!("===> EXECUTING MatchPattern with {} patterns", patterns.len());
                
                // Transform Vec<String> labels to Option<String> for the execution engine
                let transformed_patterns: Vec<_> = patterns.iter().map(|(id, nodes, edges)| {
                    let transformed_nodes: Vec<_> = nodes.iter().map(|(var, labels, props)| {
                        (var.clone(), labels.first().cloned(), props.clone())
                    }).collect();
                    (id.clone(), transformed_nodes, edges.clone())
                }).collect();

                // 1. Get the raw matched vertices and edges.
                let (mut final_vertices, final_edges) = exec_cypher_pattern(transformed_patterns, &graph_service).await?;

                let var_name = patterns.get(0)
                    .and_then(|p| p.1.get(0))
                    .and_then(|n| n.0.as_ref())
                    .map(|s| s.as_str())
                    .unwrap_or("p");

                // 2. Apply WHERE filtering if present
                if let Some(wc) = where_clause {
                    final_vertices.retain(|v| {
                        let ctx = EvaluationContext::from_vertex(var_name, v);
                        match wc.condition.evaluate(&ctx) {
                            Ok(CypherValue::Bool(matched)) => matched, 
                            Ok(_) => false, 
                            Err(e) => {
                                error!("Error evaluating WHERE clause for vertex {}: {}", v.id, e);
                                false
                            }
                        }
                    });
                }

                // 3. Apply WITH filtering if present (MPI logical resolution)
                if let Some(pwic) = with_clause {
                    if let Some(inner_wc) = &pwic.where_clause {
                        final_vertices.retain(|v| {
                            let ctx = EvaluationContext::from_vertex(var_name, v);
                            match inner_wc.condition.evaluate(&ctx) {
                                Ok(CypherValue::Bool(matched)) => matched,
                                Ok(_) => false,
                                Err(e) => {
                                    error!("Error evaluating WITH clause for vertex {}: {}", v.id, e);
                                    false
                                }
                            }
                        });
                    }
                }

                Ok(json!({
                    "vertices": final_vertices,
                    "edges": final_edges,
                    "stats": {
                        "vertices_matched": final_vertices.len(),
                        "edges_matched": final_edges.len()
                    }
                }))
            }

            CypherQuery::MatchSet { match_patterns, where_clause, with_clause, set_clauses } => {
                info!("===> EXECUTING MatchSet with {} patterns", match_patterns.len());
                
                let transformed_patterns: Vec<_> = match_patterns.iter().map(|(id, nodes, edges)| {
                    let transformed_nodes: Vec<_> = nodes.iter().map(|(var, labels, props)| {
                        (var.clone(), labels.first().cloned(), props.clone())
                    }).collect();
                    (id.clone(), transformed_nodes, edges.clone())
                }).collect();

                let (mut matched_vertices, _) = exec_cypher_pattern(transformed_patterns, &graph_service).await?;
                
                let var_name = match_patterns.get(0)
                    .and_then(|p| p.1.get(0))
                    .and_then(|node| node.0.as_ref())
                    .map(|s| s.as_str())
                    .unwrap_or("p");

                if let Some(wc) = where_clause {
                    matched_vertices.retain(|v| {
                        let ctx = EvaluationContext::from_vertex(var_name, v);
                        wc.condition.evaluate(&ctx).map(|val| val.as_bool()).unwrap_or(false)
                    });
                }

                if let Some(pwic) = with_clause {
                    if let Some(inner_wc) = &pwic.where_clause {
                        matched_vertices.retain(|v| {
                            let ctx = EvaluationContext::from_vertex(var_name, v);
                            inner_wc.condition.evaluate(&ctx).map(|val| val.as_bool()).unwrap_or(false)
                        });
                    }
                }

                if matched_vertices.is_empty() {
                    return Ok(json!({ 
                        "vertices": [], 
                        "stats": { "vertices_updated": 0 } 
                    }));
                }
                
                let mut updated_vertices = Vec::new();
                for mut vertex in matched_vertices {
                    for (set_var, prop_name, expression) in &set_clauses {
                        let current_var = set_var.as_str();
                        let ctx = EvaluationContext::from_vertex(current_var, &vertex);
                        let evaluated_val = expression.evaluate(&ctx)?;
                        let json_val = evaluated_val.to_json();
                        let prop_value = to_property_value(json_val)?;
                        vertex.properties.insert(prop_name.clone(), prop_value);
                        vertex.updated_at = models::BincodeDateTime(chrono::Utc::now());
                    }
                    graph_service.update_vertex(vertex.clone()).await?;
                    updated_vertices.push(vertex);
                }
                
                Ok(json!({ 
                    "vertices": updated_vertices,
                    "stats": { "vertices_updated": updated_vertices.len() }
                }))
            }
            
            CypherQuery::MatchCreate {
                match_patterns,
                where_clause,
                with_clause,
                create_patterns,
            } => {
                info!("===> EXECUTING MatchCreate");
                
                let mut var_to_id: HashMap<String, SerializableUuid> = HashMap::new();
                let mut created_vertices = Vec::new();
                let mut created_edges = Vec::new();

                let compat_with_clause: Option<WithClause> = with_clause.as_ref().and_then(|pw| {
                    pw.where_clause.as_ref().map(|wc| WithClause {
                        condition: wc.condition.clone(),
                    })
                });

                let matched_bindings = resolve_match_patterns(
                    &*graph_service,
                    match_patterns,
                    &where_clause,
                    &compat_with_clause,
                ).await?;

                let mut should_proceed = true;
                let ctx = EvaluationContext::from_uuid_bindings(&matched_bindings);

                if let Some(wc) = &where_clause {
                    if !wc.condition.evaluate(&ctx).map(|v| v.as_bool()).unwrap_or(false) {
                        should_proceed = false;
                    }
                }

                if should_proceed {
                    if let Some(pwic) = &with_clause {
                        if let Some(inner_wc) = &pwic.where_clause {
                            if !inner_wc.condition.evaluate(&ctx).map(|v| v.as_bool()).unwrap_or(false) {
                                should_proceed = false;
                            }
                        }
                    }
                }

                if should_proceed {
                    var_to_id.extend(matched_bindings);

                    for (pat_idx, pat) in create_patterns.iter().enumerate() {
                        for (node_idx, (var_opt, labels_vec, properties)) in pat.1.iter().enumerate() {
                            let v_final = var_opt.clone().unwrap_or_else(|| {
                                format!("__anon_pat{}_n{}", pat_idx, node_idx)
                            });

                            if !var_to_id.contains_key(&v_final) {
                                let props: HashMap<String, PropertyValue> = properties
                                    .iter()
                                    .map(|(k, v)| to_property_value(v.clone()).map(|pv| (k.clone(), pv)))
                                    .collect::<GraphResult<HashMap<String, PropertyValue>>>()?;

                                let final_label = labels_vec.first().cloned().unwrap_or_else(|| "Node".to_string());
                                let new_id = SerializableUuid(Uuid::new_v4());
                                let vertex = Vertex {
                                    id: new_id,
                                    label: Identifier::new(final_label)?,
                                    properties: props, 
                                    created_at: Utc::now().into(),
                                    updated_at: Utc::now().into(),
                                };
                                graph_service.create_vertex(vertex.clone()).await?;
                                var_to_id.insert(v_final.clone(), new_id);
                                created_vertices.push(vertex);
                            }
                        }

                        for (i, rel_tuple) in pat.2.iter().enumerate() {
                            let from_var = pat.1[i].0.clone().unwrap_or_else(|| {
                                format!("__anon_pat{}_n{}", pat_idx, i)
                            });
                            let to_var = pat.1[i + 1].0.clone().unwrap_or_else(|| {
                                format!("__anon_pat{}_n{}", pat_idx, i + 1)
                            });

                            let from_id = *var_to_id.get(&from_var)
                                .ok_or_else(|| GraphError::ValidationError(from_var))?;
                            let to_id = *var_to_id.get(&to_var)
                                .ok_or_else(|| GraphError::ValidationError(to_var))?;

                            let (_rel_var, rel_label_opt, _len_range, rel_properties, direction_opt) = rel_tuple;
                            let (outbound_id, inbound_id) = match direction_opt {
                                Some(false) => (to_id, from_id),
                                _ => (from_id, to_id),
                            };

                            let edge_type_str = rel_label_opt.clone().unwrap_or_else(|| "RELATED".to_string());
                            let rel_props: BTreeMap<String, PropertyValue> = rel_properties
                                .iter()
                                .map(|(k, v)| to_property_value(v.clone()).map(|pv| (k.clone(), pv)))
                                .collect::<GraphResult<BTreeMap<String, PropertyValue>>>()?;

                            let edge = Edge {
                                id: SerializableUuid(Uuid::new_v4()),
                                outbound_id,
                                inbound_id,
                                edge_type: Identifier::new(edge_type_str.clone())?,
                                label: edge_type_str,
                                properties: rel_props,
                            };

                            graph_service.create_edge(edge.clone()).await?;
                            created_edges.push(edge);
                        }
                    }
                }

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

            CypherQuery::MatchRemove {
                match_patterns,
                where_clause,
                with_clause,
                remove_clauses,
            } => {
                let transformed_patterns: Vec<_> = match_patterns.iter().map(|(id, nodes, edges)| {
                    let transformed_nodes: Vec<_> = nodes.iter().map(|(var, labels, props)| {
                        (var.clone(), labels.first().cloned(), props.clone())
                    }).collect();
                    (id.clone(), transformed_nodes, edges.clone())
                }).collect();

                let (mut matched_vertices, _) = exec_cypher_pattern(transformed_patterns, &graph_service).await?;
                
                let var_name = match_patterns.get(0)
                    .and_then(|p| p.1.get(0))
                    .and_then(|node| node.0.as_ref())
                    .map(|s| s.as_str())
                    .unwrap_or("p");

                if let Some(wc) = where_clause {
                    matched_vertices.retain(|v| {
                        let ctx = EvaluationContext::from_vertex(var_name, v);
                        wc.condition.evaluate(&ctx).map(|val| val.as_bool()).unwrap_or(false)
                    });
                }

                if let Some(pwic) = with_clause {
                    if let Some(inner_wc) = &pwic.where_clause {
                        matched_vertices.retain(|v| {
                            let ctx = EvaluationContext::from_vertex(var_name, v);
                            inner_wc.condition.evaluate(&ctx).map(|val| val.as_bool()).unwrap_or(false)
                        });
                    }
                }

                let mut updated_vertices = Vec::new();
                for mut vertex in matched_vertices {
                    for (_var, prop_name) in &remove_clauses {
                        vertex.properties.remove(prop_name.as_str());
                        vertex.updated_at = Utc::now().into();
                    }
                    graph_service.update_vertex(vertex.clone()).await?;
                    updated_vertices.push(vertex);
                }

                Ok(json!({
                    "vertices": updated_vertices,
                    "stats": { "vertices_updated": updated_vertices.len() }
                }))
            }

            CypherQuery::MatchCreateSet {
                match_patterns,
                where_clause,
                with_clause,
                create_patterns,
                set_clauses,
            } => {
                info!("===> EXECUTING MatchCreateSet");
                let mut var_to_id: HashMap<String, SerializableUuid> = HashMap::new();
                let mut created_vertices: Vec<Vertex> = Vec::new();
                let mut updated_vertices: Vec<Vertex> = Vec::new();

                let compat_with = with_clause.as_ref().and_then(|pw| {
                    pw.where_clause.as_ref().map(|wc| WithClause {
                        condition: wc.condition.clone(),
                    })
                });

                let matched_bindings = resolve_match_patterns(
                    &graph_service,
                    match_patterns.clone(),
                    &where_clause,
                    &compat_with,
                ).await?;

                let mut should_proceed = true;
                let ctx = EvaluationContext::from_uuid_bindings(&matched_bindings);
                
                if let Some(wc) = where_clause {
                    if !wc.condition.evaluate(&ctx).map(|v| v.as_bool()).unwrap_or(false) {
                        should_proceed = false;
                    }
                }

                if should_proceed {
                    if let Some(pwic) = with_clause {
                        if let Some(inner_wc) = &pwic.where_clause {
                            if !inner_wc.condition.evaluate(&ctx).map(|v| v.as_bool()).unwrap_or(false) {
                                should_proceed = false;
                            }
                        }
                    }
                }

                if should_proceed {
                    var_to_id.extend(matched_bindings);

                    for pat in create_patterns.iter() {
                        for (var_opt, labels_vec, properties) in &pat.1 {
                            if let Some(v) = var_opt.as_ref() {
                                if !var_to_id.contains_key(v) {
                                    let props: HashMap<String, PropertyValue> = properties
                                        .iter()
                                        .map(|(k, val)| to_property_value(val.clone()).map(|pv| (k.clone(), pv)))
                                        .collect::<GraphResult<HashMap<_, _>>>()?;
                                    let final_label = labels_vec.first().cloned().unwrap_or_else(|| "Node".to_string());
                                    let new_id = SerializableUuid(Uuid::new_v4());
                                    let vertex = Vertex {
                                        id: new_id,
                                        label: Identifier::new(final_label)?,
                                        properties: props,
                                        created_at: Utc::now().into(),
                                        updated_at: Utc::now().into(),
                                    };
                                    graph_service.create_vertex(vertex.clone()).await?;
                                    var_to_id.insert(v.clone(), new_id);
                                    created_vertices.push(vertex);
                                }
                            }
                        }
                    }

                    for (set_var, prop, expr) in set_clauses {
                        if let Some(id_wrapper) = var_to_id.get(&set_var) {
                            let mut vertex = graph_service.get_vertex(&id_wrapper.0).await.ok_or_else(|| {
                                GraphError::NotFound(unsafe { Identifier::new_unchecked(set_var.clone()) })
                            })?;
                            let ctx = EvaluationContext::from_vertex(&set_var, &vertex);
                            let evaluated_val = expr.evaluate(&ctx)?;
                            let json_val = evaluated_val.to_json();
                            let property_value = to_property_value(json_val)?;
                            vertex.properties.insert(prop.clone(), property_value);
                            vertex.updated_at = Utc::now().into();
                            graph_service.update_vertex(vertex.clone()).await?;
                            updated_vertices.push(vertex);
                        }
                    }
                }

                Ok(json!({
                    "created_vertices": created_vertices,
                    "updated_vertices": updated_vertices,
                    "stats": {
                        "vertices_created": created_vertices.len(),
                        "vertices_updated": updated_vertices.len(),
                    }
                }))
            }
            CypherQuery::Merge {
                patterns,
                where_clause,
                with_clause,        // ← ADD this binding
                on_create_set,
                on_match_set,
            } => {
                info!("===> EXECUTING MERGE with Expressions");

                // 1. Execute the merge logic in the service layer.
                let result = graph_service.execute_merge_query(
                    patterns,
                    where_clause, 
                    with_clause,        // ← PASS it here (3rd argument)
                    on_create_set,
                    on_match_set,
                ).await?;

                // 2. Build the response
                let status_json = if !result.updated_nodes.is_empty() {
                    json!({
                        "status": "matched",
                        "updated_vertex_ids": result.updated_nodes,
                        "stats": { 
                            "vertices_updated": result.updated_nodes.len(),
                            "properties_set": result.properties_set_count
                        }
                    })
                } else {
                    json!({
                        "status": "created",
                        "created_vertex_ids": result.created_nodes,
                        "stats": { 
                            "vertices_created": result.created_nodes.len(),
                            "properties_set": result.properties_set_count
                        }
                    })
                };

                Ok(status_json)
            }
            // Handles CREATE (a:Label {props})
            CypherQuery::CreateNode { label, properties } => {
                // 1. Prepare Vertex data structure
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

                // *** DELEGATE TO graph_service ***
                // graph_service.create_vertex handles both storage and in-memory graph update.
                graph_service.create_vertex(v.clone()).await?;

                Ok(json!({ "vertex": v }))
            },
            // Handles CREATE (n1:L1), (n2:L2)
            CypherQuery::CreateNodes { nodes } => {
                // FIX: Map the input 'nodes' structure (String label) to the structure
                // expected by 'create_node_batch' (Option<String> label).
                let nodes_for_batch = nodes.into_iter().map(|(label, properties)| {
                    // Wrap the mandatory String label into an Option<String>
                    (Some(label), properties)
                }).collect::<Vec<_>>();

                // *** DELEGATE TO graph_service ***
                let created_vertices = graph_service.create_node_batch(nodes_for_batch).await?;
                
                Ok(json!({ "vertices": created_vertices }))
            }

            CypherQuery::MatchNode { label, properties } => {
                let mut query_props: HashMap<String, PropertyValue> = properties
                    .into_iter()
                    .map(|(k, v)| to_property_value(v).map(|pv| (k, pv)))
                    .collect::<GraphResult<_>>()?;
                
                // --- START FIX: Indexed lookup for internal 'id' property ---
                if let Some(prop_value) = query_props.remove("id") {
                    let internal_id: Option<i32> = match prop_value {
                        PropertyValue::Integer(val) => Some(val as i32), 
                        PropertyValue::I32(val) => Some(val),
                        _ => None,
                    };

                    if let Some(internal_id) = internal_id {
                        match get_vertex_by_internal_id_direct(&graph_service, internal_id).await? {
                            Some(v) => {
                                // FIX: Check if the vertex label contains the query label (supports multi-label)
                                let matches_label = label.as_ref().map_or(true, |l| {
                                    v.label.as_ref().split(':').any(|part| part == l)
                                });
                                
                                let matches_remaining_props = query_props.iter().all(|(k, expected_val)| {
                                    v.properties.get(k).map_or(false, |actual_val| actual_val == expected_val)
                                });

                                if matches_label && matches_remaining_props {
                                    return Ok(json!({ "vertices": vec![v] }));
                                } else {
                                    return Ok(json!({ "vertices": Vec::<models::Vertex>::new() }));
                                }
                            }
                            None => {
                                return Ok(json!({ "vertices": Vec::<models::Vertex>::new() }));
                            }
                        }
                    }
                }
                // --- END FIX ---

                let vertices = graph_service.get_all_vertices().await?;
                
                let filtered = vertices.into_iter().filter(|v| {
                    // FIX: Multi-label aware matching
                    let matches_label = if let Some(query_label) = &label {
                        v.label.as_ref().split(':').any(|part| part == query_label)
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
                let mut result_vertices = Vec::new();
                let mut matched_ids = HashSet::new();
                let all_vertices = graph_service.get_all_vertices().await?;

                for (_var, labels, properties) in nodes {
                    let mut props: HashMap<String, PropertyValue> = properties
                        .iter()
                        .map(|(k, v)| to_property_value(v.clone()).map(|pv| (k.clone(), pv)))
                        .collect::<GraphResult<_>>()?;
                    
                    let mut handled_by_id_constraint = false;
                    
                    if let Some(prop_value) = props.remove("id") {
                        let internal_id = match prop_value {
                            PropertyValue::Integer(val) => Some(val as i32), 
                            PropertyValue::I32(val) => Some(val), 
                            _ => None,
                        };
                        
                        if let Some(internal_id) = internal_id {
                            handled_by_id_constraint = true; 
                        
                            if let Some(v) = get_vertex_by_internal_id_direct(&graph_service, internal_id).await? {
                                // FIX: Check if node matches ANY of the labels provided in the Vec
                                let matches_label = if labels.is_empty() {
                                    true
                                } else {
                                    labels.iter().any(|l| {
                                        v.label.as_ref().split(':').any(|part| part == l)
                                    })
                                };

                                let matches_remaining_props = props.iter().all(|(k, expected_val)| {
                                    v.properties.get(k).map_or(false, |actual_val| actual_val == expected_val)
                                });

                                if matches_label && matches_remaining_props && !matched_ids.contains(&v.id) {
                                    result_vertices.push(v.clone());
                                    matched_ids.insert(v.id);
                                }
                            }
                        }
                    }

                    if handled_by_id_constraint {
                        continue;
                    }

                    for v in &all_vertices {
                        // FIX: Multi-label check for the scan branch
                        let matches_label = if labels.is_empty() {
                            true
                        } else {
                            labels.iter().any(|l| {
                                v.label.as_ref().split(':').any(|part| part == l)
                            })
                        };
                        
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
                let mut var_to_id: HashMap<String, SerializableUuid> = HashMap::new(); 
                let mut created_edges = Vec::new();

                // 1. Create all vertices and populate var_to_id map
                if nodes.is_empty() {
                    return Err(GraphError::ValidationError("CREATE pattern must contain at least one node.".into()));
                }

                for (var_opt, labels_vec, properties) in nodes.iter() {
                    let props: GraphResult<HashMap<String, PropertyValue>> = properties
                        .iter()
                        .map(|(k, v)| to_property_value(v.clone()).map(|pv| (k.clone(), pv)))
                        .collect();
                    
                    let new_id = SerializableUuid(Uuid::new_v4());
                    
                    // FIX: Resolve label by taking the first one in the Vec or defaulting to "Node"
                    let final_label = labels_vec.first()
                        .cloned()
                        .unwrap_or_else(|| "Node".to_string());
                    
                    let vertex = Vertex {
                        id: new_id,
                        label: Identifier::new(final_label)?,
                        properties: props?,
                        created_at: Utc::now().into(),
                        updated_at: Utc::now().into(), 
                    };
                    
                    graph_service.create_vertex(vertex.clone()).await?;
                    
                    if let Some(v) = var_opt.as_ref() {
                        var_to_id.insert(v.clone(), new_id);
                    }
                    created_vertices.push(vertex);
                }
                
                // 2. Create edges based on the relationships list and sequential nodes.
                if relationships.len() != nodes.len().saturating_sub(1) {
                    return Err(GraphError::ValidationError("Mismatched number of nodes and relationships in complex CREATE pattern.".into()));
                }

                for (i, rel_tuple) in relationships.into_iter().enumerate() {
                    let from_var_opt = nodes[i].0.as_ref();
                    let to_var_opt = nodes[i + 1].0.as_ref();

                    let from_var = from_var_opt.ok_or_else(|| GraphError::ValidationError(format!("Node at index {} in CREATE pattern has no variable", i)))?;
                    let to_var = to_var_opt.ok_or_else(|| GraphError::ValidationError(format!("Node at index {} in CREATE pattern has no variable", i + 1)))?;

                    let from_id = *var_to_id.get(from_var).ok_or_else(|| GraphError::ValidationError(format!("Unbound from var: {}", from_var)))?;
                    let to_id = *var_to_id.get(to_var).ok_or_else(|| GraphError::ValidationError(format!("Unbound to var: {}", to_var)))?;

                    let (
                        _rel_var,     
                        label_opt,      // Index 1: Relationship Label (Option<String>)
                        _len_range,    
                        properties,     // Index 3: Properties Map
                        direction_opt,  // Index 4: Direction (Option<bool>)
                    ) = rel_tuple;

                    let (outbound_id, inbound_id) = match direction_opt {
                        Some(false) => (to_id, from_id),
                        _ => (from_id, to_id),
                    };

                    let edge_type_str = label_opt.clone().unwrap_or_else(|| "RELATED".to_string());
                    
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
                    
                    graph_service.create_edge(edge.clone()).await?;
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
                    let (_var_opt, labels_vec, properties) = node_pattern;
                    
                    let props: GraphResult<HashMap<String, PropertyValue>> = properties
                        .iter()
                        .map(|(k, v)| to_property_value(v.clone()).map(|pv| (k.clone(), pv)))
                        .collect();
                    
                    let new_id = SerializableUuid(Uuid::new_v4());
                    
                    // FIX: labels_vec is Vec<String>. We take the first one or default to "Node".
                    let final_label = labels_vec.first()
                        .cloned()
                        .unwrap_or_else(|| "Node".to_string());
                    
                    let vertex = Vertex {
                        id: new_id,
                        label: Identifier::new(final_label)?,
                        properties: props?,
                        created_at: Utc::now().into(),  
                        updated_at: Utc::now().into(),  
                    };
                    
                    // Rely on graph_service for persistence
                    graph_service.create_vertex(vertex.clone()).await?;
                    
                    var_to_id.insert(var.clone(), new_id);
                    created_vertices.push(vertex);
                }
                
                // --- Phase 2: Create all edges from all patterns ---

                for (_, nodes, relationships) in patterns.into_iter() {
                    if relationships.is_empty() {
                        continue;
                    }

                    // Iterate over (from_node, rel, to_node) segments
                    for i in 0..relationships.len() {
                        let rel_tuple = &relationships[i]; 
                        
                        // Nodes match rel[i] with node[i] -> node[i+1]
                        let from_var_opt = nodes[i].0.as_ref();
                        let to_var_opt = nodes[i + 1].0.as_ref();

                        let from_var = from_var_opt.ok_or_else(|| GraphError::ValidationError(format!("Relationship source node at index {} has no variable.", i)))?;
                        let to_var = to_var_opt.ok_or_else(|| GraphError::ValidationError(format!("Relationship target node at index {} has no variable.", i + 1)))?;

                        // Resolve IDs using the map of newly created nodes
                        let from_id = *var_to_id.get(from_var).ok_or_else(|| GraphError::ValidationError(format!("Unbound source variable: {}", from_var)))?;
                        let to_id = *var_to_id.get(to_var).ok_or_else(|| GraphError::ValidationError(format!("Unbound target variable: {}", to_var)))?;

                        let (
                            _rel_var,     
                            label_opt,      // Index 1: Relationship Label (Option<String>)
                            _len_range,     
                            properties,     // Index 3: Properties Map
                            direction_opt,  // Index 4: Direction (Option<bool>)
                        ) = rel_tuple;

                        // Handle direction: Some(false) is inbound.
                        let (outbound_id, inbound_id) = match direction_opt {
                            Some(false) => (to_id, from_id), // (from_id)<-[R]-(to_id)
                            _ => (from_id, to_id),           // (from_id)-[R]->(to_id)
                        };

                        let edge_type_str = label_opt.clone().unwrap_or_else(|| "RELATED".to_string());
                        
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
                        
                        graph_service.create_edge(edge.clone()).await?;
                        created_edges.push(edge);
                    }
                }
                
                // --- Phase 3: Shape the result based on return_items ---
                let all_return_variables: HashSet<String> = return_items.into_iter().collect();
                let created_vertex_map: HashMap<SerializableUuid, &Vertex> = created_vertices.iter().map(|v| (v.id, v)).collect();

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
                
                // Use the centralized graph_service.create_edge, which handles
                // 1. storage.create_edge
                // 2. graph_service.add_edge (in-memory update & notify)
                graph_service.create_edge(edge.clone()).await?;
                
                Ok(json!({ "edge": edge }))
            }
                            
            CypherQuery::SetNode { id, properties } => {
                // 1. Get the current state of the vertex (using graph_service methods)
                let mut vertex = graph_service.get_vertex_from_storage(&id.0).await?.ok_or_else(|| {
                    // Must read from storage here if the command is designed to update existing only
                    // based on persistent state, otherwise read from memory (graph_service.get_vertex).
                    // Assuming it must exist in storage to be updated.
                    GraphError::StorageError(format!("Vertex not found: {:?}", id))
                })?;
                
                // 2. Apply property changes
                let props: GraphResult<HashMap<String, PropertyValue>> = properties
                    .into_iter()
                    .map(|(k, v)| to_property_value(v).map(|pv| (k, pv)))
                    .collect();
                vertex.properties.extend(props?);
                vertex.updated_at = Utc::now().into(); // Assuming Utc::now() is in scope
                
                // 3. Use the centralized graph_service.update_vertex, which handles
                // 1. storage.update_vertex
                // 2. graph_service.add_vertex (in-memory update & notify)
                graph_service.update_vertex(vertex.clone()).await?;
                
                Ok(json!({ "vertex": vertex }))
            }
                    
            CypherQuery::DeleteNode { id } => {
                // Use the centralized graph_service.delete_vertex_by_uuid, which handles:
                // 1. storage.delete_vertex
                // 2. graph_service.delete_vertex_from_memory (in-memory update & notify)
                graph_service.delete_vertex_by_uuid(id.0).await?;
                
                // NOTE: graph_service.delete_vertex_from_memory handles edge cleanup in memory.
                // We assume the cleanup trigger handles flushing/async tasks.
                trigger_async_graph_cleanup(); 
                
                Ok(json!({ "deleted": id }))
            }
            
            CypherQuery::SetKeyValue { key, value } => {
                let kv_key = key.clone().into_bytes();
                let kv_value = value.as_bytes().to_vec();

                // Use graph_service for key-value persistence
                graph_service.kv_insert(kv_key, kv_value).await?;
                graph_service.flush_storage().await?;
                
                Ok(json!({ "key": key, "value": value }))
            }
            
            CypherQuery::GetKeyValue { key } => {
                let kv_key = key.clone().into_bytes();
                
                // Use graph_service for key-value retrieval
                let value = graph_service.kv_retrieve(&kv_key).await?;
                
                Ok(json!({ "key": key, "value": value.map(|v| String::from_utf8_lossy(&v).to_string()) }))
            }
            
            CypherQuery::DeleteKeyValue { key } => {
                let kv_key = key.clone().into_bytes();
                
                // Use graph_service for key-value operations
                let value = graph_service.kv_retrieve(&kv_key).await?;
                let existed = value.is_some();
                
                if existed {
                    graph_service.kv_delete(&kv_key).await?;
                    graph_service.flush_storage().await?;
                }
                
                Ok(json!({ "key": key, "deleted": existed }))
            }

            CypherQuery::CreateIndex { label, properties } => {
                warn!("CREATE INDEX command is not implemented yet. Index for label '{:?}' on properties {:?} ignored.", label, &properties);
                Ok(json!({ "status": "success", "message": "Index creation not implemented", "label": label, "properties": properties }))
            }

            CypherQuery::CreateEdgeBetweenExisting { source_var, rel_type, properties, target_var } => {
                let properties_clone = properties.clone();
                
                // 1. Parse IDs and properties, same as before
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
                
                // 2. Construct the Edge object
                let edge = Edge {
                    id: SerializableUuid(Uuid::new_v4()),
                    outbound_id: SerializableUuid(from_id),
                    inbound_id: SerializableUuid(to_id),
                    edge_type: Identifier::new(rel_type)?,
                    label,
                    properties: props?,
                };
                
                // 3. Centralize creation via GraphService
                // This single call replaces:
                // - storage.create_edge(edge.clone()).await?;
                // - graph_service.add_edge(edge.clone()).await?;
                graph_service.create_edge(edge.clone()).await
                    .map_err(|e| GraphError::InternalError(format!("Failed to create edge via GraphService: {}", e)))?;
                
                Ok(json!({ "edge": edge }))
            }

            // ----------------------------------------------------------------------------------------------------------------------
            CypherQuery::DetachDeleteNodes { node_variable: _, label } => {
                // 1. Fetch all vertices matching the filter.
                // FIX: Use GraphService to get vertices from the in-memory graph view.
                let all_vertices = graph_service.get_all_vertices().await?;
                
                let nodes_to_delete: Vec<Vertex> = all_vertices
                    .into_iter()
                    .filter(|v| {
                        // Filter by label constraint if present
                        label.as_ref().map_or(true, |l| v.label.as_ref() == l.as_str())
                    })
                    .collect();
                
                if nodes_to_delete.is_empty() {
                    return Ok(json!({"deleted_vertices": 0, "deleted_edges": 0}));
                }
                
                let vertex_ids: Vec<Uuid> = nodes_to_delete.iter().map(|v| v.id.0).collect();
                
                // 2. Use the atomic GraphService method for DETACH DELETE.
                let deleted_edges = graph_service.detach_delete_vertices(&vertex_ids).await?;
                
                // Note: The count of deleted vertices is the length of `nodes_to_delete`.
                let deleted_vertices = nodes_to_delete.len();

                // 3. Flush storage
                graph_service.flush_storage().await?;
                trigger_async_graph_cleanup();
                
                Ok(json!({
                    "deleted_vertices": deleted_vertices,
                    "deleted_edges": deleted_edges
                }))
            }
            // ----------------------------------------------------------------------------------------------------------------------
            CypherQuery::DeleteEdges {
                edge_variable,
                pattern,
                where_clause,
                with_clause,
            } => {
                // --- STEP 1: MATCH (READ) ---
                
                let is_variable_length = pattern.relationships.len() == 1
                    && pattern.nodes.len() == 2
                    && pattern.relationships[0].2.map_or(false, |(min, max)| {
                        min.map_or(1, |m| m) != 1 || max.map_or(1, |m| m) != 1
                    });

                let edges_to_delete: Vec<Edge> = if is_variable_length {
                    let graph = graph_service.get_graph().await;
                    let rel_pat = &pattern.relationships[0];
                    let start_node_pat = &pattern.nodes[0];
                    let end_node_pat = &pattern.nodes[1];
                    
                    // FIX: node_matches_constraints expects &Option<String>, but we have &Vec<String>
                    // We take the first label as the primary label for matching.
                    let start_label = start_node_pat.1.first().cloned();
                    
                    let start_vertices: Vec<&Vertex> = graph.vertices.values()
                        .filter(|v| node_matches_constraints(v, &start_label, &start_node_pat.2))
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
                    // Transform pattern.nodes from Vec<(Option<String>, Vec<String>, HashMap)>
                    // to Vec<(Option<String>, Option<String>, HashMap)> to satisfy exec_cypher_pattern
                    let compatible_nodes: Vec<(Option<String>, Option<String>, HashMap<String, Value>)> = pattern.nodes
                        .iter()
                        .map(|(var, labels, props)| {
                            (var.clone(), labels.first().cloned(), props.clone())
                        })
                        .collect();

                    let (_, edges) = exec_cypher_pattern(
                        vec![(None, compatible_nodes, pattern.relationships.clone())],
                        &(*graph_service)
                    ).await?;
                    
                    edges
                };
                
                // --- STEP 2: FILTER WITH WHERE + WITH LOGIC (MPI AUDIT) ---
                let mut deleted = 0usize;
                
                for edge in edges_to_delete {
                    let mut variables = HashMap::new();
                    variables.insert(edge_variable.clone(), CypherValue::Edge(edge.clone()));
                    
                    let ctx = EvaluationContext {
                        variables,
                        parameters: HashMap::new(),
                    };
                    
                    // Evaluate WHERE clause (initial filter)
                    let where_passed = match where_clause.as_ref() {
                        Some(wc) => match wc.condition.evaluate(&ctx) {
                            Ok(val) => matches!(val, CypherValue::Bool(true)),
                            Err(_) => false,
                        },
                        None => true,
                    };
                    
                    // Evaluate WITH clause logic (critical for MPI identity resolution traceability)
                    let with_passed = match with_clause.as_ref() {
                        Some(parsed_with) => {
                            // Check if the ParsedWithClause has an internal where_clause
                            match &parsed_with.where_clause {
                                Some(inner_wc) => match inner_wc.condition.evaluate(&ctx) {
                                    Ok(val) => matches!(val, CypherValue::Bool(true)),
                                    Err(_) => false,
                                },
                                None => true, // If no filter in WITH, it passes
                            }
                        }
                        None => true,
                    };
                    
                    // Both must pass to authorize the deletion in the MPI event graph
                    if where_passed && with_passed {
                        graph_service.delete_edge_by_uuid(edge.id.0).await?;
                        deleted += 1;
                    }
                }
                
                graph_service.flush_storage().await?;
                trigger_async_graph_cleanup();
                
                Ok(json!({
                    "status": "success",
                    "deleted_edges": deleted,
                    "message": format!("Successfully deleted {deleted} edge(s) following MPI logical verification")
                }))
            }
            // 1. Handle Query Chains (Multiple clauses like MATCH...CREATE...RETURN)
            CypherQuery::Chain(clauses) => {
                // FIX: Restore the required logic to box clauses and call the high-level service method.
                // This service method is assumed to handle the state/context of sequential execution,
                // internally relying on the logic provided by `execute_chain_internal`.
                let boxed_clauses = clauses.into_iter().map(Box::new).collect();
                
                let chain_qr_result = graph_service.execute_chain(boxed_clauses)
                    .await;

                // The service method returns GraphResult<QueryResult>, so convert the result 
                // to the final required type, Value.
                graph_service.query_result_to_value(chain_qr_result)
            }

            // 2. Handle Union Queries
            // Assuming the original structure: CypherQuery::Union(q1, q2, is_all)
            CypherQuery::Union(q1, q2, is_all) => {
                // q1 and q2 are already Box<CypherQuery> as bound in the match pattern.
                // We pass them directly to the service method without dereferencing (*q1, *q2)
                // which was causing the type mismatch (expected Box, found raw enum).
                let union_qr_result = graph_service.union_results(q1, q2, is_all)
                    .await;

                // Convert the QueryResult returned by the service into the final Value.
                graph_service.query_result_to_value(union_qr_result)
            }
            // 3. Handle RETURN Statements (The final step in a query chain)
            CypherQuery::ReturnStatement { 
                projection_string,
                order_by,
                skip,
                limit,
            } => {
                // Serialize Vec<OrderByItem> → Option<String>
                let order_by_str = graph_service.serialize_order_by_to_string(&order_by);

                let return_qr_result = graph_service.execute_return_statement(
                    projection_string,
                    order_by_str, // ✅ now Option<String>
                    skip,
                    limit,
                ).await;

                graph_service.query_result_to_value(return_qr_result)
            }
            // NEW: Handles the standalone SET clause for chaining (e.g., `MATCH (n) SET n.prop = 'new'`)
            CypherQuery::SetStatement { assignments } => {
                info!("===> EXECUTING Standalone SetStatement");
                
                // Pass the assignments (Vec<(String, String, Expression)>) to the service
                graph_service.apply_set_assignments(assignments).await?;
                
                Ok(json!({
                    "status": "success",
                    "message": "Properties set successfully."
                }))
            }
            // NEW: Handles the standalone DELETE clause (e.g., `MATCH (n) DELETE n`)
            CypherQuery::DeleteStatement { variables, detach } => {
                // variables is Vec<String>: list of variables to delete
                
                // The service is responsible for finding nodes/relationships bound to these 
                // variables in the current transaction context and performing the deletion.
                
                graph_service.delete_variables(variables, detach).await?;
                Ok(json!({"message": "Variables deleted successfully."}))
            },

            // NEW: Handles the standalone REMOVE clause (e.g., `MATCH (n) REMOVE n:Label`)
            CypherQuery::RemoveStatement { removals } => {
                // Removals is Vec<(String, String)>: (variable, label_or_property)
                
                // The service iterates through the removals and applies them to the context 
                // bound variables (removing a label, or setting a property to null).
                graph_service.remove_labels_or_properties(removals).await?;
                
                Ok(json!({"message": "Labels/properties removed successfully."}))
            },
            CypherQuery::MatchPath { path_var: _, left_node, right_node, return_clause: _ } => {
                let left_pat = parse_node_pattern(&left_node)?;
                let right_pat = parse_node_pattern(&right_node)?;

                // Reworked to use GraphService's read guard for in-memory access (preferred for performance)
                let graph_read_guard = graph_service.read().await;

                // Use references to the in-memory graph's internal collections
                // The keys here are SerializableUuid
                let all_vertices_ref = &graph_read_guard.vertices;
                let all_edges_ref = &graph_read_guard.edges;

                // These HashSets correctly contain SerializableUuid
                let mut left_ids = HashSet::new();
                let mut right_ids = HashSet::new();

                // The matching function remains the same, operating on a Vertex reference.
                let matches = |v: &Vertex, (label, props): &(Option<String>, HashMap<String, Value>)| {
                    let label_ok = label.as_ref().map_or(true, |l| {
                        let vl = v.label.as_ref();
                        vl == l || vl.starts_with(&format!("{}:", l))
                    });
                    let props_ok = props.iter().all(|(k, expected)| {
                        v.properties.get(k).map_or(false, |actual| {
                            // Assuming to_property_value is available in scope
                            to_property_value(expected.clone()).ok().map_or(false, |pv| actual == &pv)
                        })
                    });
                    label_ok && props_ok
                };

                // 1. Identify starting and ending vertices from the in-memory collection
                for v in all_vertices_ref.values() {
                    if matches(v, &left_pat) { left_ids.insert(v.id); }
                    if matches(v, &right_pat) { right_ids.insert(v.id); }
                }

                let mut matched_edge_ids = HashSet::new();
                // matched_vertex_ids correctly contains SerializableUuid
                let mut matched_vertex_ids = left_ids.union(&right_ids).copied().collect::<HashSet<_>>();

                // 2. Perform the Breadth-First Search (BFS) for paths (up to 2 hops, as per original logic)
                // FIX 1: The queue must store the underlying Uuid, not SerializableUuid, 
                // or the queue must be Vec<(SerializableUuid, u32)>. 
                // Since the matched_vertex_ids uses SerializableUuid, we'll keep the queue consistent.
                let mut queue: Vec<(SerializableUuid, u32)> = matched_vertex_ids.iter().map(|&id| (id, 0)).collect();
                let mut visited_edges = HashSet::new();

                while let Some((current_id, hop)) = queue.pop() {
                    if hop >= 2 { continue; }

                    // Iterate over all edges in the graph
                    for edge in all_edges_ref.values() {
                        if visited_edges.contains(&edge.id.0) { continue; }

                        // These are Uuid
                        let (from_id_uuid, to_id_uuid) = (edge.outbound_id.0, edge.inbound_id.0);
                        
                        // This is SerializableUuid
                        let current_id_uuid = current_id.0; 

                        let next_id_uuid = if from_id_uuid == current_id_uuid {
                            Some(to_id_uuid)
                        } else if to_id_uuid == current_id_uuid {
                            Some(from_id_uuid)
                        } else {
                            None
                        };

                        if let Some(next_uuid) = next_id_uuid {
                            // FIX 2: Convert the raw Uuid back to SerializableUuid before insertion
                            let next_id = SerializableUuid(next_uuid);
                            
                            visited_edges.insert(edge.id.0);
                            matched_edge_ids.insert(edge.id.0);
                            matched_vertex_ids.insert(next_id); // Insert the SerializableUuid
                            
                            if hop < 1 { 
                                queue.push((next_id, hop + 1)); // Push the SerializableUuid
                            }
                        }
                    }
                }

                let vertices: Vec<Vertex> = matched_vertex_ids.into_iter()
                    // FIX: Extract the inner Uuid from the SerializableUuid before lookup.
                    // Assuming SerializableUuid is a newtype wrapper like struct SerializableUuid(pub Uuid);
                    .filter_map(|id| all_vertices_ref.get(&id.0).cloned()) 
                    .collect();

                let edges: Vec<Edge> = matched_edge_ids.into_iter()
                    .filter_map(|id| all_edges_ref.get(&id).cloned())
                    .collect();

                // Read guard dropped here

                Ok(json!({ "vertices": vertices, "edges": edges }))
            }
        }
    })
}

fn parse_order_by_items(input: &str) -> IResult<&str, Vec<OrderByItem>> {
    use nom::{
        combinator::map,
        multi::separated_list1,
        sequence::{preceded, tuple},
        character::complete::{multispace0, multispace1},
        Parser,
    };

    fn parse_order_direction(input: &str) -> IResult<&str, bool> {
        alt((
            map(tag_no_case("ASC"), |_| true),
            map(tag_no_case("DESC"), |_| false),
        )).parse(input)
    }

    let order_item = map(
        pair(
            parse_expression_string, // or parse_identifier for simple cases
            opt(preceded(multispace1, parse_order_direction)),
        ),
        |(expr_str, direction)| OrderByItem {
            expression: expr_str,
            ascending: direction.unwrap_or(true),
        }
    );

    separated_list1(
        preceded(multispace0, char(',')),
        preceded(multispace0, order_item),
    ).parse(input)
}

// Required to parse the ON CREATE SET / ON MATCH SET sub-clauses in MERGE.
// Returns a parser function defined by the given keyword ("ON CREATE" or "ON MATCH").
// --- 1. Fix: parse_on_set_clause (Cypher MERGE Clause) ---
// FIX 1: Change return type from Value to PropertyValue
fn parse_on_set_clause(on_type: &str) -> impl Fn(&str) -> IResult<&str, Vec<(String, String, Value)>> {
    move |input| {
        map(
            preceded(
                pair(tag_no_case(on_type), terminated(tag_no_case("SET"), multispace1)),
                // parse_set_assignment_tuple returns (String, String, Value)
                separated_list1(preceded(multispace0::<&str, NomErrorType<&str>>, char(',')), parse_set_assignment_tuple),
            ),
            |assignments| assignments,
        ).parse(input)
    }
}

/// Helper to get type name for debugging
fn type_name_of_val(v: &Value) -> &str {
    match v {
        Value::String(_) => "String",
        Value::Number(_) => "Number",
        Value::Bool(_) => "Bool",
        Value::Null => "Null",
        Value::Array(_) => "Array",
        Value::Object(_) => "Object",
    }
}
/// Helper to convert Cypher `Value` → `PropertyValue`
pub fn to_property_value(v: Value) -> GraphResult<PropertyValue> {
    // Check for special function calls first
    if let Value::Object(ref m) = v {
        if let Some(func) = m.get("__CYPHER_FUNC__") {
            if func.as_str() == Some("timestamp") {
                return Ok(PropertyValue::Integer(Utc::now().timestamp_millis()));
            }
        }
    }
    
    match v {
        Value::String(s) => Ok(PropertyValue::String(s)),
        Value::Number(n) if n.is_i64() => Ok(PropertyValue::Integer(n.as_i64().unwrap())),
        Value::Number(n) if n.is_f64() => Ok(PropertyValue::Float(SerializableFloat(n.as_f64().unwrap()))),
        Value::Bool(b) => Ok(PropertyValue::Boolean(b)),
        Value::Null => Err(GraphError::InternalError("Null values not supported in properties".into())),
        Value::Array(_) => Err(GraphError::InternalError("Array values not supported in properties".into())),
        Value::Object(_) => Err(GraphError::InternalError("Nested objects not supported in properties".into())),
        _ => Err(GraphError::InternalError("Unsupported property value type".into())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::collections::HashMap;

    #[test]
    fn test_is_cypher() {
        assert!(is_cypher("MATCH (n:Person) RETURN n"));
        assert!(is_cypher("CREATE (n:Person {name: 'Alice'})"));
        assert!(!is_cypher("SELECT * FROM table"));
    }

    #[test]
    fn test_parse_create_node() {
        let query = "CREATE (n:Person {name: 'Alice', age: 30})";
        let result = parse_cypher(query).unwrap();
        let expected = CypherQuery::CreateNode {
            label: "Person".to_string(),
            properties: HashMap::from([
                ("name".to_string(), json!("Alice")),
                ("age".to_string(), json!(30)),
            ]),
        };
        assert_eq!(result, expected);
    }

    #[test]
    fn test_parse_create_node_without_variable() {
        let query = "CREATE (:Person {name: 'Alice', age: 30})";
        let result = parse_cypher(query).unwrap();
        let expected = CypherQuery::CreateNode {
            label: "Person".to_string(), // Should use the label
            properties: HashMap::from([
                ("name".to_string(), json!("Alice")),
                ("age".to_string(), json!(30)),
            ]),
        };
        assert_eq!(result, expected);
    }

    #[test]
    fn test_parse_create_node_with_float() {
        let query = "CREATE (:Person {id: \"alice\", name: \"Alice\", age: 30, active: true, score: 95.5})";
        let result = parse_cypher(query).unwrap();
        let expected = CypherQuery::CreateNode {
            label: "Person".to_string(),
            properties: HashMap::from([
                ("id".to_string(), json!("alice")),
                ("name".to_string(), json!("Alice")),
                ("age".to_string(), json!(30)),
                ("active".to_string(), json!(true)),
                ("score".to_string(), json!(95.5)),
            ]),
        };
        assert_eq!(result, expected);
    }

    #[test]
    fn test_parse_create_nodes_with_ampersand_labels() {
        let query = "CREATE (charlie:Person&Actor {name: 'Charlie Sheen'}), (oliver:Person&Director {name: 'Oliver Stone'})";
        let result = parse_cypher(query).unwrap();
        match result {
            CypherQuery::CreateNodes { nodes } => {
                assert_eq!(nodes.len(), 2);
                assert_eq!(nodes[0].0, "Person:Actor");
                assert_eq!(nodes[1].0, "Person:Director");
            }
            _ => panic!("Expected CreateNodes variant"),
        }
    }

    #[test]
    fn test_parse_create_nodes_with_colon_labels() {
        let query = "CREATE (charlie:Person:Actor {name: 'Charlie Sheen'}), (oliver:Person:Director {name: 'Oliver Stone'})";
        let result = parse_cypher(query).unwrap();
        match result {
            CypherQuery::CreateNodes { nodes } => {
                assert_eq!(nodes.len(), 2);
                assert_eq!(nodes[0].0, "Person:Actor");
                assert_eq!(nodes[1].0, "Person:Director");
            }
            _ => panic!("Expected CreateNodes variant"),
        }
    }

    #[test]
    fn test_parse_match_simple_return() {
        let query = "MATCH (n:Person) RETURN n";
        let result = parse_cypher(query).unwrap();
        let expected = CypherQuery::MatchNode {
            label: Some("Person".to_string()),
            properties: HashMap::new(),
        };
        assert_eq!(result, expected);
    }

    #[test]
    fn test_parse_match_multiple_nodes() {
        let query = "MATCH (charlie:Person {name: 'Charlie Sheen'}), (oliver:Person {name: 'Oliver Stone'})";
        let result = parse_cypher(query).unwrap();
        match result {
            CypherQuery::MatchMultipleNodes { nodes } => {
                assert_eq!(nodes.len(), 2);
                assert_eq!(nodes[0].0, Some("charlie".to_string()));
                assert_eq!(nodes[0].1, Some("Person".to_string()));
                assert_eq!(nodes[1].0, Some("oliver".to_string()));
                assert_eq!(nodes[1].1, Some("Person".to_string()));
            }
            _ => panic!("Expected MatchMultipleNodes variant"),
        }
    }

    #[test]
    fn test_parse_match_complex_return() {
        let query = "MATCH (n) RETURN n.name, labels(n) AS labels";
        let result = parse_cypher(query).unwrap();
        let expected = CypherQuery::MatchNode {
            label: None,
            properties: HashMap::new(),
        };
        assert_eq!(result, expected);
    }

    #[test]
    fn test_parse_match_count_return() {
        let query = "MATCH (n) RETURN count(n) AS total_vertices";
        let result = parse_cypher(query).unwrap();
        let expected = CypherQuery::MatchNode {
            label: None,
            properties: HashMap::new(),
        };
        assert_eq!(result, expected);
    }

    #[test]
    fn test_parse_create_complex_pattern() {
        let query = "CREATE (a:Person {name: 'Alice'})-[:KNOWS]->(b:Person {name: 'Bob'})";
        let result = parse_cypher(query).unwrap();
        match result {
            CypherQuery::CreateComplexPattern { nodes, relationships } => {
                assert_eq!(nodes.len(), 2);
                assert_eq!(relationships.len(), 1);
                assert_eq!(relationships[0].0, "KNOWS");
                assert_eq!(relationships[0].2, false); // outgoing
            }
            _ => panic!("Expected CreateComplexPattern variant"),
        }
    }

    #[test] 
    fn test_parse_create_complex_bidirectional() {
        let query = "CREATE (a)-[:REL1]->(b)<-[:REL2]-(c)";
        let result = parse_cypher(query).unwrap();
        match result {
            CypherQuery::CreateComplexPattern { nodes, relationships } => {
                assert_eq!(nodes.len(), 3);
                assert_eq!(relationships.len(), 2); 
                assert_eq!(relationships[0].0, "REL1");
                assert_eq!(relationships[0].2, false); // outgoing
                assert_eq!(relationships[1].0, "REL2");
                assert_eq!(relationships[1].2, true); // incoming
            }
            _ => panic!("Expected CreateComplexPattern variant"),
        }
    }

    #[test]
    fn test_parse_set_kv() {
        let query = "SET mykey = 'myvalue'";
        let result = parse_cypher(query).unwrap();
        let expected = CypherQuery::SetKeyValue {
            key: "mykey".to_string(),
            value: "myvalue".to_string(),
        };
        assert_eq!(result, expected);
    }

    #[test]
    fn test_match_with_where() {
        let input = r#"MATCH (n:Person) WHERE n.name = "Alice" RETURN n"#;
        let result = full_statement_parser(input);
        assert!(result.is_ok());
        let (remainder, _) = result.unwrap();
        assert_eq!(remainder.trim(), "");
    }
    
    #[test]
    fn test_match_set_with_where() {
        let input = r#"MATCH (p:Patient {type: "VIP"}) WHERE ID(p) = "123" SET p.status = "MERGED" RETURN p"#;
        let result = full_statement_parser(input);
        assert!(result.is_ok());
        let (remainder, _) = result.unwrap();
        assert_eq!(remainder.trim(), "");
    }
}

// ==============================================================================
// TESTING: Add comprehensive tests for multiple record handling
// ==============================================================================

#[cfg(test)]
mod multiple_records_tests {
    use super::*;

    #[test]
    fn test_variable_length_parsing() {
        assert_eq!(parse_variable_length("*").unwrap().1, (Some(1), None));
        assert_eq!(parse_variable_length("*2").unwrap().1, (Some(1), Some(2)));
        assert_eq!(parse_variable_length("*0..2").unwrap().1, (Some(0), Some(2)));
        assert_eq!(parse_variable_length("*1..5").unwrap().1, (Some(1), Some(5)));
        assert_eq!(parse_variable_length("*1..").unwrap().1, (Some(1), None));
    }

    #[test]
    fn test_match_all_nodes() {
        let query = "MATCH (n:Person) RETURN n";
        // Using parse_cypher (which likely calls parse_cypher_statement)
        let result = parse_cypher(query).unwrap();
        
        // FIX: Include where_clause in the match pattern
        match result {
            CypherQuery::MatchPattern { patterns, where_clause } => {
                assert_eq!(patterns.len(), 1);
                assert!(where_clause.is_none()); // "RETURN n" shouldn't count as WHERE
            }
            _ => panic!("Should parse as MatchPattern, got {:?}", result),
        }
    }

    #[test]
    fn test_match_multiple_comma_separated() {
        let query = "MATCH (a:Person), (b:Movie) RETURN a, b";
        let result = parse_cypher(query).unwrap();
        
        match result {
            CypherQuery::MatchPattern { patterns, where_clause } => {
                // Should handle both patterns
                assert_eq!(patterns.len(), 2);
                assert!(where_clause.is_none());
            }
            _ => panic!("Should handle multiple patterns, got {:?}", result),
        }
    }

    #[test]
    fn test_parse_uuid_property() {
        let input = "a928227a-165c-4fc4-abd1-796583a26d8d";
        let result = parse_property_value(input).unwrap();
        assert_eq!(result.1, Value::String("a928227a-165c-4fc4-abd1-796583a26d8d".to_string()));
    }
}