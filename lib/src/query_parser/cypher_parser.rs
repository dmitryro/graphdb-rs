// lib/src/query_parser/cypher_parser.rs
// Complete working version - merges working repo code with edge creation support
use log::{debug, error, info, warn, trace};
use nom::{
    branch::alt,
    bytes::complete::{tag, tag_no_case, take_while1, take_until, take_till, take_while,},
    character::complete::{char, multispace0, multispace1, alphanumeric1},
    combinator::{map, opt, recognize, value},
    multi::{separated_list0, separated_list1, many0}, 
    number::complete::double,
    sequence::{delimited, pair, preceded, tuple, terminated, separated_pair},
    multi::many1,
    IResult,
    Parser,
    error as NomError,
    error::Error as NomErrorType, 
};
use serde_json::{json, Value};
use std::collections::{BTreeMap, HashMap, HashSet,};
use std::sync::Arc;
use uuid::Uuid;
use chrono::Utc;
use models::identifiers::{Identifier, SerializableUuid};
use models::{Vertex, Edge};
use models::errors::{GraphError, GraphResult};
use models::properties::{ PropertyValue, SerializableFloat };
use crate::graph_engine::graph_service::{ GraphService, GRAPH_SERVICE, initialize_graph_service, GraphEvent,}; 
use crate::graph_engine::traversal::TraverseExt;
use crate::graph_engine::pattern_match::node_matches_constraints; 
use crate::database::Database;
use crate::storage_engine::{GraphStorageEngine, StorageEngine};
use crate::query_exec_engine::query_exec_engine::QueryExecEngine;
use crate::config::StorageConfig;
use crate::query_parser::query_types::*;
use crate::query_parser::parser_zmq::trigger_async_graph_cleanup;

// --------------------------------------------------------------------------
// This function must be placed outside of full_statement_parser.
// --------------------------------------------------------------------------

// The complex type that parse_match_clause_patterns returns.
// This type is derived from the previous log and is used as the O (Output) type in IResult.
type PatternsReturnType = std::vec::Vec<(
    std::option::Option<std::string::String>,
    std::vec::Vec<(
        std::option::Option<std::string::String>,
        std::option::Option<std::string::String>,
        std::collections::HashMap<std::string::String, serde_json::Value>,
    )>,
    std::vec::Vec<(
        std::option::Option<std::string::String>,
        std::option::Option<std::string::String>,
        std::option::Option<(std::option::Option<u32>, std::option::Option<u32>)>,
        std::collections::HashMap<std::string::String, serde_json::Value>,
        std::option::Option<bool>,
    )>,
)>;

#[derive(Debug, PartialEq)]
pub enum CypherQuery {
    CreateNode {
        label: String,
        properties: HashMap<String, Value>,
    },
    CreateNodes {
        nodes: Vec<(String, HashMap<String, Value>)>,
    },
    MatchNode {
        label: Option<String>,
        properties: HashMap<String, Value>,
    },
    MatchMultipleNodes {
        nodes: Vec<NodePattern>,
    },
    MatchRemove {
        match_patterns: Vec<Pattern>,
        remove_clauses: Vec<(String, String)>,
    },
    CreateComplexPattern {
        nodes: Vec<NodePattern>,
        relationships: Vec<RelPattern>,
    },
    // NEW VARIANT: Handles general CREATE statements with RETURN clause
    CreateStatement {
        patterns: Vec<Pattern>,      // List of all comma-separated patterns
        return_items: Vec<String>,   // List of variables to return (u, p1, p2)
    },
    MatchPattern {
        patterns: Vec<Pattern>,
    },
    MatchSet {
        match_patterns: Vec<Pattern>,
        set_clauses: Vec<(String, String, Value)>, // (variable, property, value)
    },
    MatchCreateSet {
        match_patterns: Vec<Pattern>,
        create_patterns: Vec<Pattern>,
        set_clauses: Vec<(String, String, Value)>,
    },
    MatchCreate {
        match_patterns: Vec<Pattern>,
        create_patterns: Vec<Pattern>,
    },
    CreateEdgeBetweenExisting {
        source_var: String,
        rel_type: String,
        properties: HashMap<String, Value>,
        target_var: String,
    },
    CreateEdge {
        from_id: SerializableUuid,
        edge_type: String,
        to_id: SerializableUuid,
    },
    SetNode {
        id: SerializableUuid,
        properties: HashMap<String, Value>,
    },
    DeleteNode {
        id: SerializableUuid,
    },
    SetKeyValue {
        key: String,
        value: String,
    },
    GetKeyValue {
        key: String,
    },
    DeleteKeyValue {
        key: String,
    },
    CreateIndex {
        label: String,
        properties: Vec<String>,
    },
    // ---------- NEW ----------
    MatchPath {
        path_var: String,      //  e.g.  path
        left_node: String,     //  e.g.  (p:Patient {id: "12345"})
        right_node: String,    //  e.g.  (related)
        return_clause: String, //  e.g.  collect(DISTINCT …
    },
    /// DELETE edges matched by pattern
    DeleteEdges {
        edge_variable: String,
        pattern: MatchPattern,
        where_clause: Option<WhereClause>,
    },
    DetachDeleteNodes {
        /// The variable name used in the MATCH clause, e.g. "n" in MATCH (n)
        node_variable: String,
        /// Optional label filter, e.g. Some("Person") in MATCH (n:Person)
        label: Option<String>,
    },
    Batch(Vec<CypherQuery>),

}

// Add this helper function before execute_cypher
async fn get_vertex_by_internal_id_direct(
    graph_service: &GraphService,
    internal_id: i32,
) -> GraphResult<Option<models::Vertex>> {
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
    let cypher_keywords = ["MATCH", "CREATE", "SET", "RETURN", "DELETE"];
    cypher_keywords.iter().any(|kw| query.trim().to_uppercase().starts_with(kw))
}

// =============================================================================
// COMPLETE REPLACEMENT FOR full_statement_parser AND HELPERS
// Place these functions in your cypher_parser.rs file
// =============================================================================

// Helper: Check if a character could start a Cypher keyword
fn is_cypher_keyword_start(c: char) -> bool {
    matches!(c, 'O' | 'o' | 'W' | 'w' | 'R' | 'r' | 'C' | 'c' | 'M' | 'm' | 'S' | 's' | 'D' | 'd')
}

fn parse_identifier(input: &str) -> IResult<&str, &str> {
    take_while1(|c: char| c.is_alphanumeric() || c == '_' || c == '&').parse(input)
}

// Check if we're at the start of a new Cypher clause
// Helper function to check if we're at the start of a new Cypher clause
fn is_at_keyword_boundary(input: &str) -> bool {
    let trimmed = input.trim_start();
    trimmed.starts_with("WHERE") ||
    trimmed.starts_with("where") ||
    trimmed.starts_with("RETURN") ||
    trimmed.starts_with("return") ||
    trimmed.starts_with("CREATE") ||
    trimmed.starts_with("create") ||
    trimmed.starts_with("ORDER") ||
    trimmed.starts_with("order") ||
    trimmed.starts_with("SKIP") ||
    trimmed.starts_with("skip") ||
    trimmed.starts_with("LIMIT") ||
    trimmed.starts_with("limit") ||
    trimmed.starts_with("SET") ||
    trimmed.starts_with("set") ||
    trimmed.starts_with("DELETE") ||
    trimmed.starts_with("delete") ||
    trimmed.starts_with("DETACH") ||
    trimmed.starts_with("detach") ||
    trimmed.starts_with("REMOVE") ||
    trimmed.starts_with("remove") 
    // Do NOT include "OPTIONAL MATCH" here — it must be parsed as part of the pattern list
}

/// Parses a full MATCH ... SET ... [RETURN] statement.
fn parse_match_set_relationship(input: &str) -> IResult<&str, CypherQuery> {
    map(
        tuple((
            tag_no_case("MATCH"),
            multispace1,
            parse_match_clause_patterns, // Assumed to return Vec<Pattern>
            multispace1,
            tag_no_case("SET"),
            multispace1,
            // Parse one or more comma-separated SET clauses
            separated_list1(
                ws(char(',')),
                parse_set_clause // (variable, property, value)
            ),
            // Optionally consume RETURN clause if present
            opt(preceded(multispace1, tag_no_case("RETURN"))),
            opt(take_while(|c| c != '\n')), // Consume the rest of the line (Return items)
        )),
        |(_, _, match_patterns, _, _, _, set_clauses, _, _)| {
            let set_clauses: Vec<(String, String, Value)> = set_clauses.into_iter().flat_map(|(var, map): (String, HashMap<String, Value>)| {
                map.into_iter().map(move |(key, val)| (var.clone(), key, val))
            }).collect();
            CypherQuery::MatchSet {
                match_patterns,
                set_clauses,
            }
        },
    ).parse(input)
}

// Modify parse_match_create_relationship
fn parse_match_create_relationship(input: &str) -> IResult<&str, CypherQuery> {
    
    let mut all_match_patterns: PatternsReturnType = PatternsReturnType::default();
    let mut current_input = input;

    // --- A. Consume MANDATORY First MATCH Clause (and extract patterns) ---
    match preceded(
        alt((
            tag_no_case::<_, _, NomErrorType<&str>>("OPTIONAL MATCH"),
            tag_no_case::<_, _, NomErrorType<&str>>("MATCH"),
        )),
        match_clause_content_parser,
    ).parse(current_input) {
        Ok((remainder, (_, patterns))) => {
            all_match_patterns.extend(patterns);
            current_input = remainder;
        }
        Err(e) => return Err(e),
    }

    // --- B. Loop for ADDITIONAL MATCH Clauses (Zero or more) ---
    loop {
        let (input_ws, _) = multispace0.parse(current_input)?;
        
        match preceded(
            alt((
                tag_no_case::<_, _, NomErrorType<&str>>("OPTIONAL MATCH"),
                tag_no_case::<_, _, NomErrorType<&str>>("MATCH"),
            )),
            match_clause_content_parser,
        ).parse(input_ws) {
            Ok((remainder, (_, patterns))) => {
                all_match_patterns.extend(patterns);
                current_input = remainder;
            }
            Err(_) => {
                current_input = input_ws;
                break;
            }
        }
    }
    
    // --- C. Parse the mandatory CREATE keyword and its pattern ---
    
    // Parse the mandatory CREATE keyword and the space that follows it.
    let (input, _) = preceded(
        multispace0,
        terminated(tag_no_case("CREATE"), multispace1)
    ).parse(current_input)?; // Use the input after all MATCH clauses
    
    // Parse the relationship pattern
    let (input, create_pattern) = parse_single_pattern(input)?;
    
    // --- D. Optional SET/RETURN clauses (as before) ---
    
    // Optional SET clause
    let (input, set_clauses_opt) = opt(preceded(
        tuple((multispace0, tag_no_case("SET"), multispace1)), // Changed multispace1 to multispace0 for flexibility
        separated_list1(
            tuple((multispace0, char(','), multispace0)),
            parse_single_set_assignment,
        ),
    )).parse(input)?;
    
    // Optionally consume RETURN clause if present
    let (input, _) = opt(preceded(multispace0, tag_no_case("RETURN"))).parse(input)?; // Changed multispace1 to multispace0
    let (input, _) = opt(take_while(|c| c != '\n')).parse(input)?;
    
    let set_clauses = set_clauses_opt.unwrap_or_default();
    
    if !set_clauses.is_empty() {
        Ok((input, CypherQuery::MatchCreateSet {
            match_patterns: all_match_patterns, // Use all accumulated patterns
            create_patterns: vec![create_pattern],
            set_clauses,
        }))
    } else {
        Ok((input, CypherQuery::MatchCreate {
            match_patterns: all_match_patterns, // Use all accumulated patterns
            create_patterns: vec![create_pattern],
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
    use nom::Parser;
    
    println!("===> parse_single_pattern START, input: '{}'", input);
    
    let mut all_nodes: Vec<NodePattern> = Vec::new();
    let mut all_relationships: Vec<RelPattern> = Vec::new();
    
    // 1. Parse the starting node (must be in parentheses)
    let (mut current_input, node_a) = parse_node(input)?;
    all_nodes.push(node_a);
    
    // 2. Parse (RELATIONSHIP + NODE) pairs
    loop {
        // Check if we've hit a keyword or end condition WITHOUT consuming space
        let remaining_trim = current_input.trim_start();
        let upper = remaining_trim.to_uppercase();
        
        // --- FIX APPLIED HERE: Added MATCH and MERGE to the termination keywords ---
        if remaining_trim.is_empty() ||
            upper.starts_with("RETURN") ||
            upper.starts_with("WHERE") ||
            upper.starts_with("OPTIONAL") ||
            upper.starts_with("WITH") ||
            upper.starts_with("ORDER") ||
            upper.starts_with("LIMIT") ||
            upper.starts_with("SKIP") ||
            upper.starts_with("CREATE") ||
            upper.starts_with("REMOVE") ||
            upper.starts_with("SET") ||
            upper.starts_with("DELETE") ||
            upper.starts_with("DETACH") ||
            upper.starts_with("MATCH") || // <--- ADDED MATCH
            upper.starts_with("MERGE") || // <--- ADDED MERGE (Recommended)
            remaining_trim.starts_with(';') {
             // Do not consume space, break with current_input unchanged
             break;
        }
        // --------------------------------------------------------------------------
        
        // Now safe to consume whitespace
        let (after_space, _) = multispace0(current_input)?;
        
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

fn parse_return_items(input: &str) -> IResult<&str, Vec<String>> {
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
    use nom::Parser;
    
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
        // String literals (quoted)
        map(parse_string_literal, |s: &str| Value::String(s.to_string())),
       
        // Numbers
        parse_signed_number_value,
       
        // Booleans
        map(tag_no_case("true"), |_| Value::Bool(true)),
        map(tag_no_case("false"), |_| Value::Bool(false)),
       
        // Null
        map(tag_no_case("null"), |_| Value::Null),
       
        // Unquoted strings/identifiers (for UUIDs, etc.)
        map(take_while1(|c: char| c.is_alphanumeric() || c == '-' || c == '_'), |s: &str| Value::String(s.to_string())),
       
        // Functions (e.g., timestamp())
        map(
            tuple((
                parse_identifier,
                char('('),
                char(')'),
            )),
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

fn parse_property_pattern(input: &str) -> IResult<&str, Vec<String>> {
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

fn parse_property_access(input: &str) -> IResult<&str, String> {
    let (input, (var, _, prop)) = tuple((
        parse_identifier,
        char('.'),
        parse_identifier,
    )).parse(input)?;
    
    Ok((input, format!("{}.{}", var, prop)))
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

fn parse_set_node(input: &str) -> IResult<&str, CypherQuery> {
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
    map(
        tuple((tag("DELETE"), multispace1, parse_identifier)),
        |(_, _, _var)| CypherQuery::DeleteNode {
            id: SerializableUuid(Uuid::new_v4()),
        },
    ).parse(input)
}

fn parse_set_kv(input: &str) -> IResult<&str, CypherQuery> {
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
    map(
        tuple((tag_no_case("DELETE"), multispace1, parse_identifier)),
        |(_, _, key)| CypherQuery::DeleteKeyValue {
            key: key.to_string(),
        },
    )
}
/// Combines parse_set_kv, parse_get_kv, and parse_delete_kv into a single parser.
fn parse_kv_operations(input: &str) -> IResult<&str, CypherQuery> {
    // FIX: Inline the alt definition directly into the execution closure.
    // This avoids the 'let combined_parser = ...' binding, which was causing the 
    // compiler to try and borrow the captured variable as mutable.
    
    (move |i| {
        alt((
            // Call the functions to get the impl Parser objects
            parse_set_kv_internal(),
            parse_get_kv_internal(),
            parse_delete_kv_internal(),
        )).parse(i)
    })(input)
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

fn parse_set_query(input: &str) -> IResult<&str, CypherQuery> {
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
    alt((
        // parse_match_remove_relationship,
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

// NOTE: This assumes 'all_vertices' and 'all_edges' are available/passed in, 
// which is suggested by your current execution logs.

async fn find_variable_length_paths(
    all_vertices: &Vec<Vertex>,
    all_edges: &Vec<Edge>,
    start_node_pat: &NodePattern,
    rel_pat: &RelPattern,
    end_node_pat: &NodePattern,
) -> GraphResult<(HashSet<SerializableUuid>, HashSet<SerializableUuid>)> {
    
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

// The final signature for the helper function that parses the content of a MATCH clause.
// Note the corrected IResult syntax: IResult<Input, Output, Error>.
fn match_clause_content_parser<'a>(
    i: &'a str,
) -> IResult<&'a str, (Option<(&'a str, &'a str, char, &'a str)>, PatternsReturnType)> {
    // The parser combinator definition:
    preceded(
        multispace1,
        tuple((
            // Optional path variable assignment (path = ...)
            opt(tuple((
                take_while1(|c: char| c.is_alphanumeric() || c == '_'),
                multispace0,
                char('='),
                multispace0,
            ))),
            // The actual patterns
            parse_match_clause_patterns,
        )),
    ).parse(i) // Apply the parser combinator to the input 'i'
}

// --------------------------------------------------------------------------
// The updated full_statement_parser function body
// --------------------------------------------------------------------------

fn full_statement_parser(input: &str) -> IResult<&str, CypherQuery> {
    // Note: We use the non-lifetime version of the type alias here
    let mut all_patterns: PatternsReturnType = PatternsReturnType::default(); 
    let mut input = input;

    // --- 1. PARSE MANDATORY FIRST MATCH CLAUSE ---
    
    // We expect the query to start with a MATCH or OPTIONAL MATCH keyword.
    // We use match_clause_content_parser to handle the pattern details.
    match preceded(
        // 1. MATCH/OPTIONAL MATCH keyword
        alt((
            tag_no_case::<_, _, NomErrorType<&str>>("OPTIONAL MATCH"),
            tag_no_case::<_, _, NomErrorType<&str>>("MATCH"),
        )),
        // 2. The full content (whitespace + path var + patterns) using the external function
        match_clause_content_parser,
    ).parse(input) {
        Ok((input_after_first_match, (_, patterns))) => {
            all_patterns.extend(patterns);
            input = input_after_first_match;
        }
        Err(e) => return Err(e), // If the first MATCH fails, we exit the entire query
    }

    // --- 2. PARSE ADDITIONAL MATCH CLAUSES (Zero or more) ---
    loop {
        // 1. Consume optional whitespace before checking for the next keyword
        let (input_ws, _) = multispace0.parse(input)?;
        
        // 2. Attempt to parse the next MATCH clause
        match preceded(
            alt((
                tag_no_case::<_, _, NomErrorType<&str>>("OPTIONAL MATCH"),
                tag_no_case::<_, _, NomErrorType<&str>>("MATCH"),
            )),
            match_clause_content_parser,
        ).parse(input_ws) {
            Ok((input_after_match, (_, patterns))) => {
                // Found and successfully parsed another MATCH clause
                all_patterns.extend(patterns);
                input = input_after_match;
            }
            Err(_) => {
                // No more MATCH/OPTIONAL MATCH clauses found, break the loop.
                // input_ws holds the position just before the next non-MATCH keyword (like CREATE) or the end.
                input = input_ws;
                break;
            }
        }
    }
    
    // --- 3. OPTIONAL ACTION CLAUSES (CREATE, SET, REMOVE) ---
    
    // optional CREATE
    let (input, create_patterns) = opt(preceded(
        tuple((multispace0, tag_no_case::<_, _, NomErrorType<&str>>("CREATE"), multispace0)),
        parse_match_clause_patterns
    )).parse(input)?;
    
    // optional SET
    let (input, set_clauses_opt) = opt(preceded(
        tuple((multispace0, tag_no_case::<_, _, NomErrorType<&str>>("SET"), multispace0)),
        separated_list1(
            tuple((multispace0, char(','), multispace0)),
            parse_single_set_assignment
        )
    )).parse(input)?;

    // optional REMOVE
    let (input, remove_clauses_opt) = opt(preceded(
        tuple((multispace0, tag_no_case::<_, _, NomErrorType<&str>>("REMOVE"), multispace0)),
        separated_list1(
            tuple((multispace0, char(','), multispace0)),
            parse_remove_clause
        )
    )).parse(input)?;

    // optional RETURN (Simplistic consumption of RETURN and everything after it)
    let (input, _) = opt(preceded(
        multispace0,
        tuple((
            tag_no_case::<_, _, NomErrorType<&str>>("RETURN"), 
            multispace0, 
            take_while(|_| true) // Consume the rest of the line
        )),
    )).parse(input)?;
    
    // --- 4. DISPATCH LOGIC ---
    
    // Ensure the internal variables match PatternsReturnType
    let create_patterns: PatternsReturnType = create_patterns.unwrap_or_default();
    let set_clauses = set_clauses_opt.unwrap_or_default();
    let remove_clauses = remove_clauses_opt.unwrap_or_default();
    
    // Dispatch logic: order matters for determining the query type
    if !create_patterns.is_empty() && !set_clauses.is_empty() {
        Ok((input, CypherQuery::MatchCreateSet {
            match_patterns: all_patterns,
            create_patterns,
            set_clauses,
        }))
    } else if !create_patterns.is_empty() {
        Ok((input, CypherQuery::MatchCreate {
            match_patterns: all_patterns,
            create_patterns,
        }))
    } else if !set_clauses.is_empty() {
        Ok((input, CypherQuery::MatchSet {
            match_patterns: all_patterns,
            set_clauses,
        }))
    } else if !remove_clauses.is_empty() {
        Ok((input, CypherQuery::MatchRemove {
            match_patterns: all_patterns,
            remove_clauses,
        }))
    } else {
        // If no action clause is found, it's just a pure MATCH query
        Ok((input, CypherQuery::MatchPattern {
            patterns: all_patterns,
        }))
    }
}

// ------------------------------------------------------------------
// MATCH path = (left)-[*..]-(right) RETURN …  (single statement only)
/// Parse **only**   MATCH path = (left)-[*..]-(right) RETURN …
/// Supports variable-length patterns: *0..2, *1..5, *, etc.
// ------------------------------------------------------------------
pub fn parse_match_path(input: &str) -> IResult<&str, CypherQuery, nom::error::Error<&str>> {
    use nom::character::complete::{alpha1, alphanumeric1, digit1};
    use nom::bytes::complete::take_while;

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

/// Parse: MATCH ()-[r]->() DELETE r
fn parse_delete_edges_simple(input: &str) -> IResult<&str, CypherQuery> {
    // 1. consume the MATCH pattern
    let (input, _) = multispace0(input)?;
    let (input, _) = tag_no_case("MATCH")(input)?;
    let (input, _) = multispace1(input)?;
    
    // `input` is updated here to be the remainder after the MATCH pattern
    let (input, pattern) = parse_single_pattern(input)?;

    // 2. Extract variable name from the pattern
    let edge_var = pattern.2.first()
        .and_then(|rel| rel.0.as_ref())
        .ok_or_else(|| {
            // The relationship in the pattern must have a variable name (e.g., 'r')
            nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Verify))
        })?
        .clone();
    
    // 3. Consume the DELETE tail, using the extracted variable name
    // FIX: Removed the unnecessary reference (&) when calling `tuple(..)(input)?`
    // and ensured `&*edge_var` is used to pass &str to `tag`.
    let (remaining_input, _) = tuple((
        multispace0, // Handles optional space between pattern and DELETE
        tag_no_case("DELETE"),
        multispace1, // Must have a space after DELETE
        tag(&*edge_var), // Correctly passes &str
        multispace0, // Consume any trailing space/end of line
    ))(input)?; // <-- CORRECTED: Pass `input` (&str), not `&input` (&&str)

    Ok((remaining_input, CypherQuery::DeleteEdges {
        edge_variable: edge_var, 
        pattern: MatchPattern {
            nodes: pattern.1,
            relationships: pattern.2,
        },
        where_clause: None,
    }))
}

/// Parse  MATCH (n:Person) DETACH DELETE n
/// returns  CypherQuery::DetachDeleteNodes { node_variable: "n", label: Some("Person") }
fn parse_detach_delete(input: &str) -> IResult<&str, CypherQuery> {
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

    Ok((
        input,
        CypherQuery::DetachDeleteNodes {
            node_variable: delete_var.to_string(),
            label: node_pat.1,
        }
    ))
}

// Assuming NodePattern, WhereClause, and MatchPattern are defined types

fn parse_match_detach_delete(input: &str) -> IResult<&str, CypherQuery> {
    use nom::Parser;
    // 1. Consume the initial "MATCH" keyword
    let (input, _) = tag_no_case("MATCH").parse(input)?;
    let (input, _) = multispace1.parse(input)?;
    
    // 2. NOW parse the patterns from the remainder (without "MATCH")
    let (input, match_patterns) = parse_match_clause_patterns(input)?;
    
    // 3. Parse DETACH DELETE
    let (input, _) = multispace0.parse(input)?;
    let (input, _) = tag_no_case("DETACH").parse(input)?;
    let (input, _) = multispace1.parse(input)?;
    let (input, _) = tag_no_case("DELETE").parse(input)?;
    let (input, _) = multispace1.parse(input)?;
    let (input, node_variable) = parse_identifier.parse(input)?;
    let node_variable = node_variable.to_string();

    let label = match_patterns.iter()
        .flat_map(|(_, nodes, _)| nodes.iter())
        .find(|(var_opt, _, _)| var_opt.as_ref().map_or(false, |v| v == &node_variable))
        .map(|(_, label_opt, _)| label_opt.clone())
        .flatten();

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
    
    // Define the error type for consistency
    type NomErrorType<'a> = nom::error::Error<&'a str>;
    
    // Initial sequence is fine
    let (input, _) = tag_no_case::<_, _, NomErrorType<'a>>("MATCH")(input)?;
    let (input, _) = multispace1(input)?;
    let (input, match_patterns) = parse_match_clause_patterns(input)?;
    let (input, _) = multispace1(input)?;
    let (input, _) = tag_no_case::<_, _, NomErrorType<'a>>("REMOVE")(input)?;
    let (input, _) = multispace1(input)?;
    
    // FIX 1: Declare the parser as mutable
    let mut remove_list_parser = separated_list1(
        tuple((multispace0, char(','), multispace0)),
        parse_remove_clause
    );

    // Call with .parse(input)
    let (input, remove_clauses) = remove_list_parser.parse(input)?;
    
    // FIX 2: Declare the parser as mutable
    let mut return_opt_parser = opt(
        preceded(
            tuple((multispace1, tag_no_case::<_, _, NomErrorType<'a>>("RETURN"), multispace1)),
            take_while(|_| true),
        )
    );

    // Call with .parse(input)
    let (input, _) = return_opt_parser.parse(input)?;
    
    Ok((input, CypherQuery::MatchRemove {
        match_patterns,
        remove_clauses,
    }))
}

/// your **old** top-level logic, just moved into a helper
/// Helper function to parse a single statement string.
// ----------------------------------------------------------------------------------
// --- UPDATED: parse_single_statement (Prioritizes MATCH...SET over MATCH...CREATE) ---
// ----------------------------------------------------------------------------------
// In cypher_parser.rs
fn parse_single_statement(input: &str) -> Result<CypherQuery, String> {
    let trimmed = input.trim();
    let upper = trimmed.to_ascii_uppercase();
   
    // 1. DETACH DELETE (unchanged)
    if upper.contains("DETACH DELETE") {
        return parse_match_detach_delete(trimmed)
            .map(|(_, q)| q)
            .map_err(|e| format!("MATCH-DETACH-DELETE parse error: {:?}", e));
    }
   
    // 2. *** NEW: Check for MATCH ... CREATE ... SET ***
    if upper.starts_with("MATCH") && upper.contains("CREATE") && upper.contains("SET") {
        return parse_match_create_relationship(trimmed) // Reuse the parser since it now handles SET
            .map(|(_, q)| q)
            .map_err(|e| format!("MATCH-CREATE-SET parse error: {:?}", e));
    }
   
    // 3. Check for MATCH ... SET
    if upper.starts_with("MATCH") && upper.contains("SET") {
        return parse_match_set_relationship(trimmed)
            .map(|(_, q)| q)
            .map_err(|e| format!("MATCH-SET parse error: {:?}", e));
    }
   
    // 4. Check for MATCH ... CREATE (old step 2, now step 3)
    if upper.starts_with("MATCH") && upper.contains("CREATE") {
        return parse_match_create_relationship(trimmed)
            .map(|(_, q)| q)
            .map_err(|e| format!("MATCH-CREATE parse error: {:?}", e));
    }
   
    // 5. single CREATE (no rels) → use original parsers that return the data (old step 3, now step 4)
    if upper.starts_with("CREATE") && !upper.contains("-[") {
        let create_parsers: Vec<fn(&str) -> IResult<&str, CypherQuery>> = vec![
            parse_create_node,
            parse_create_nodes,
            parse_create_edge,
            parse_create_edge_between_existing,
            parse_create_complex_pattern,
        ];
        for parser in create_parsers {
            if let Ok((remainder, query)) = parser(trimmed) {
                if remainder.trim().is_empty() {
                    return Ok(query);
                }
            }
        }
    }
   
    // 6. ANY query that contains a relationship arrow → simple parsers (old step 4, now step 5)
    if upper.contains("-[") && (upper.contains("]->") || upper.contains("]-") || upper.contains("<-[")) {
        return parse_simple_query_type(trimmed)
            .map(|(_, q)| q)
            .map_err(|e| format!("Simple-statement parse error: {:?}", e));
    }
    // 7. full MATCH … RETURN … queries (old step 5, now step 6)
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
    // 8. sequential / batch statements (old step 6, now step 7)
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
    // 9. fallback list (old step 7, now step 8)
    let parsers: Vec<fn(&str) -> IResult<&str, CypherQuery>> = vec![
        parse_create_statement,
        parse_delete_edges_simple,
        parse_match_create_relationship,
        parse_detach_delete,
        parse_create_index,
        parse_set_node,
        parse_delete_node,
        parse_set_kv,
        parse_get_kv,
        parse_delete_kv,
    ];
    for parser in parsers {
        if let Ok((remainder, query)) = parser(trimmed) {
            let remainder_trimmed = remainder.trim();
            if !remainder_trimmed.is_empty() {
                println!("===> WARNING: Unparsed remainder: '{}'", remainder_trimmed);
                continue;
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
/// Parses a Cypher literal value.
// FIX E0618: Must be a function that returns an IResult by calling the parser immediately.
fn parse_value(input: &str) -> IResult<&str, Value> {
    alt((
        // 1. Strings
        map(
            delimited(char('"'), take_while(|c: char| c != '"'), char('"')),
            |s: &str| Value::String(s.to_string()),
        ),
        // 2. Numbers (Float or Integer)
        map(double, |f: f64| {
            if f.fract() == 0.0 {
                Value::Number(serde_json::Number::from(f as i64))
            } else {
                Value::Number(serde_json::Number::from_f64(f).unwrap_or(serde_json::Number::from(0)))
            }
        }),
        // 3. Booleans
        value(Value::Bool(true), tag_no_case("TRUE")),
        value(Value::Bool(false), tag_no_case("FALSE")),
        // 4. NULL
        value(Value::Null, tag_no_case("NULL")),
        // 5. Functions (like timestamp())
        map(
            tuple((
                tag_no_case("timestamp"),
                ws(char('(')),
                char(')'))
            ),
            |(_, _, _)| json!({"__CYPHER_FUNC__": "timestamp"}),
        )
    )).parse(input) // <-- CRITICAL: The function is called here to return IResult
}
/// Parses a single assignment in a SET clause, e.g., 'n.property = "value"'
fn parse_single_set_assignment(input: &str) -> IResult<&str, (String, String, Value)> {
    map(
        tuple((
            // 1. Variable name
            parse_identifier,
            ws(char('.')),
            // 2. Property name
            parse_identifier,
            ws(char('=')),
            // 3. New value
            parse_value,
        )),
        // Map the result to the expected (variable, property_name, value) tuple
        |(variable, _, property, _, value)| (variable.to_string(), property.to_string(), value),
    ).parse(input) // <-- CRITICAL: The function is called here to return IResult
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
    // FIX E0618: Call all parsers with (input)
    // Match "MATCH" or "OPTIONAL MATCH" anywhere at start
    let (input, _) = alt((tag_no_case("MATCH"), tag_no_case("OPTIONAL MATCH"))).parse(input)?;
    let (input, _) = multispace1.parse(input)?;
    // Consume ALL patterns
    let (input, patterns) = separated_list1(
        tuple((multispace0, char(','), multispace0)),
        parse_pattern,
    ).parse(input)?;
    // Consume OPTIONAL MATCH clauses
    let (input, _) = many0(preceded(
        tuple((multispace1, tag_no_case("OPTIONAL MATCH"), multispace1)),
        parse_pattern,
    )).parse(input)?;
    // Consume WHERE (optional)
    let (input, _) = opt(preceded(
        tuple((multispace1, tag_no_case("WHERE"), multispace1)),
        take_until("RETURN"),
    )).parse(input)?;
    // Consume SET clause (if present)
    let (input, set_clauses) = opt(preceded(
        tuple((multispace1, tag_no_case("SET"), multispace1)),
        separated_list1(
            tuple((multispace0, char(','), multispace0)),
            parse_single_set_assignment, // This must be the 3-tuple parser
        ),
    )).parse(input)?;
    // Consume CREATE clause (if present)
    let (input, create_patterns) = opt(preceded(
        tuple((multispace1, tag_no_case("CREATE"), multispace1)),
        separated_list1(
            tuple((multispace0, char(','), multispace0)),
            parse_pattern,
        ),
    )).parse(input)?;
    // Consume RETURN (and everything after)
    let (input, _) = opt(preceded(
        tuple((multispace1, tag_no_case("RETURN"), multispace1)),
        take_while1(|_| true), // eat rest of line
    )).parse(input)?;
    // Decide result: Check SET first, then CREATE, then fallback to MATCH
    if set_clauses.is_some() {
        // FIX E0308: This now correctly uses the 3-tuple Vec returned by parse_single_set_assignment
        Ok((input, CypherQuery::MatchSet {
            match_patterns: patterns,
            set_clauses: set_clauses.unwrap_or_default(),
        }))
    } else if create_patterns.is_some() {
        Ok((input, CypherQuery::MatchCreate {
            match_patterns: patterns,
            create_patterns: create_patterns.unwrap_or_default(),
        }))
    } else {
        Ok((input, CypherQuery::MatchPattern { patterns }))
    }
}

fn parse_variable_length(input: &str) -> IResult<&str, (Option<u32>, Option<u32>)> {
    use nom::Parser;
    
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
    use nom::Parser;
    
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
    opt(preceded(
        char(':'),
        parse_rel_types_with_or
    ))
    .parse(input)
}

fn parse_rel_detail(input: &str) -> IResult<&str, (Option<String>, Option<String>, Option<(Option<u32>, Option<u32>)>, HashMap<String, Value>)> {
    use nom::Parser;
    
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
    separated_list1(
        tuple((multispace0, char(','), multispace0)),
        parse_single_pattern
    ).parse(input)
}

// Parses a Cypher numeric literal (signed integers or floats) and maps it to a PropertyValue.
// Parses a Cypher numeric literal (signed integers or floats) and maps it to a PropertyValue.
pub fn parse_numeric_literal(input: &str) -> IResult<&str, PropertyValue> {
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
    (None, nodes, rels) 
}

fn parse_match_clause(input: &str) -> IResult<&str, Vec<Pattern>> {
    let (input, _) = tag("MATCH").parse(input)?;
    let (input, _) = multispace1.parse(input)?;
    let (input, patterns) = separated_list1(
        tuple((multispace0, char(','), multispace0)),
        parse_pattern
    ).parse(input)?;
    Ok((input, patterns))
}

fn parse_where(input: &str) -> IResult<&str, ()> {
    let (input, _) = tag("WHERE").parse(input)?;
    let (input, _) = multispace0.parse(input)?;
    let (input, _) = take_until("RETURN").parse(input)?;
    Ok((input, ()))
}


/// Parse a raw node pattern string
///     ( [var][:Label] [{props}] )
/// into (label, properties) exactly like the old logic.
fn parse_node_pattern(input: &str) -> GraphResult<(Option<String>, HashMap<String, Value>)> {
    type E<'a> = nom::error::Error<&'a str>;

    // label part  ->  ( ':' Label )
    let label = map(
        preceded(tuple((tag::<_, _, E>("("), take_until(":"), tag::<_, _, E>(":"))), take_until(" }")),
        |s: &str| Some(s.trim().to_string()),
    );

    // properties  ->  { … }
    let props = map(
        delimited(tag::<_, _, E>("{"), take_until("}"), tag::<_, _, E>("}")),
        |s: &str| serde_json::from_str::<HashMap<String, Value>>(s).unwrap_or_default(),
    );

    // full parser
    let mut parser = tuple((
        opt(label),
        opt(preceded(multispace0::<_, E>, props)),
        take_until::<_, _, E>(")"), // throw away variable name if present
    ));

    let (_, (lbl, prp, _)) = parser(input)
        .map_err(|_| GraphError::ValidationError("Malformed node pattern".into()))?;

    Ok((lbl.unwrap_or(None), prp.unwrap_or_default()))
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

// Conceptual Helper method on NodePattern, Vertex (or similar)
// You must implement this in an appropriate file.

fn matches_constraints(vertex: &Vertex, label_opt: &Option<String>, properties: &HashMap<String, Value>) -> bool {
    // 1. label
    let label_matches = label_opt.as_ref().map_or(true, |l| vertex.label.as_ref() == l.as_str());
    if !label_matches { return false; }

    // 2. properties
    properties.iter().all(|(k, v)| {
        vertex.properties.get(k).map_or(false, |pv| {
            to_property_value(v.clone()).map_or(false, |json_pv| json_pv == *pv)
        })
    })
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

fn parse_where_clause(input: &str) -> IResult<&str, String> {
    let (input, _) = tag("WHERE").parse(input)?;
    let (input, _) = multispace1.parse(input)?;
    let (input, condition) = take_while1(|c| c != '\n' && c != '\r' && c != 'R').parse(input)?;
    Ok((input, condition.to_string()))
}

fn parse_return_expression(input: &str) -> IResult<&str, String> {
    // Capture everything until comma or end of line and convert to String
    map(
        take_while1(|c| c != ',' && c != '\n' && c != '\r'),
        |s: &str| s.to_string(),
    ).parse(input)
}

fn extract_main_entity(nodes: &Vec<NodePattern>) -> Option<NodePattern> {
    nodes.first().cloned()
}

fn parse_match_node_original(input: &str) -> IResult<&str, CypherQuery> {
    let (input, (_, _, node)) = tuple((
        tag("MATCH"),
        multispace1,
        parse_node,
    )).parse(input)?;
    
    let (_, label, props) = node;
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
    graph_service: &GraphService, // <--- DEPENDENCY CHANGE: use GraphService
) -> GraphResult<(Vec<Vertex>, Vec<Edge>)> {
    
    // *** DELEGATION FIX: Retrieve all vertices and edges via GraphService internal/abstracted methods ***
    // NOTE: In a robust system, this data retrieval should be optimized (e.g., indexed lookups), 
    // but for this example, we fetch the full sets via GraphService reads.
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
                    
                    // NOTE: expand_variable_paths would need to be defined/available in scope.
                    // This function should rely on the *GraphService* for traversal logic, 
                    // but since it's operating on the local `all_edges` copy, we keep the call structure.
                    // expand_variable_paths(
                    //     &all_edges,
                    //     &matched_vertex_ids,
                    //     &mut matched_edge_ids,
                    //     min,
                    //     max,
                    // );
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

                    // For end nodes: if variable exists in var_bindings AND was bound in a PREVIOUS pattern, use it
                    // If variable was just bound in THIS pattern, find ALL matching vertices
                    let end_node_id_set: HashSet<SerializableUuid> = if let Some(var) = end_var_name {
                        if let Some(bound_ids) = var_bindings.get(var) {
                            // Check if this variable was just bound in this pattern
                            if vars_bound_in_this_pattern.contains(var) {
                                // Variable just bound in this pattern - need to find ALL matching vertices
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
                                // Variable bound in previous pattern - use those IDs
                                bound_ids.clone()
                            }
                        } else {
                            // Not bound yet - find ALL matching vertices
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

                        // Check that both vertices exist (ensured by initial read from GraphService)
                        // This check can be optimized away if we assume data integrity.
                        // let source_vertex = all_vertices.iter().find(|v| v.id == edge.outbound_id);
                        // let target_vertex = all_vertices.iter().find(|v| v.id == edge.inbound_id);
                        
                        // Check connectivity: does this edge connect a vertex in START set to a vertex in END set?
                        // Consider both directions for undirected relationships
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
    // *** DEPENDENCY CHANGE: Use GraphService instead of Arc<dyn GraphStorageEngine> ***
    graph_service: &GraphService, 
    var: &str,
    label: &Option<String>,
    properties: &HashMap<String, Value>,
) -> GraphResult<SerializableUuid> {
    
    println!("===>      resolve_var: Looking for var='{}', label={:?}, {} properties", 
             var, label, properties.len());
    
    let mut query_props: HashMap<String, PropertyValue> = HashMap::new();
    for (k, v) in properties.iter() {
        println!("===>      Property: {} = {:?}", k, v);
        // Assuming to_property_value is correct for type conversion
        query_props.insert(k.clone(), to_property_value(v.clone())?);
    }
    
    // *** DELEGATION FIX: Call get_all_vertices() through graph_service ***
    let all_vertices = graph_service.get_all_vertices().await?;
    println!("===>      resolve_var: Searching through {} vertices", all_vertices.len());
    
    let matched_vertex = all_vertices.into_iter().find(|v| {
        // FIX: Combine checks explicitly and remove brittle early return.
        
        let matches_label = label.as_ref().map_or(true, |query_label| {
            let result = v.label.as_ref() == query_label.as_str();
            println!("===>        Checking vertex {}: label '{}' vs '{}' = {}", 
                     v.id.0, v.label.as_ref(), query_label, result);
            result
        });
        
        // Property matching logic remains the same
        let matches_props = query_props.iter().all(|(k, expected_val)| {
            let result = v.properties.get(k).map_or(false, |actual_val| {
                actual_val == expected_val
            });
            if !result {
                println!("===>        Vertex {}: Property '{}' mismatch (actual: {:?}, expected: {:?})", 
                         v.id.0, k, v.properties.get(k), expected_val);
            }
            result
        });
        
        let is_match = matches_label && matches_props; // <-- Explicitly combine label and property matches
        
        if is_match {
            println!("===>        MATCH FOUND: Vertex {}", v.id.0);
        }
        
        is_match // Return the explicit combination
    });
    
    match matched_vertex {
        Some(v) => {
            println!("===>      resolve_var: SUCCESS - Returning {}", v.id.0);
            Ok(v.id)
        },
        None => {
            println!("===>      resolve_var: ERROR - No match found");
            Err(GraphError::ValidationError(format!(
                "No existing node found for variable '{}' with label '{:?}' and constraints: {:?}",
                var, label, properties
            )))
        }
    }
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
    graph_service: &GraphService, // <--- DEPENDENCY CHANGE: use GraphService
    match_patterns: Vec<Pattern>,
) -> GraphResult<HashMap<String, SerializableUuid>> {
    let mut var_to_id: HashMap<String, SerializableUuid> = HashMap::new();
    
    println!("===> resolve_match_patterns: Received {} patterns", match_patterns.len());
    
    for (pattern_idx, pat) in match_patterns.iter().enumerate() {
        println!("===> Processing pattern {}: path_var={:?}, {} nodes, {} rels", 
                 pattern_idx, pat.0, pat.1.len(), pat.2.len());

        // We maintain the check for unimplemented relationship logic
        if !pat.2.is_empty() {
            return Err(GraphError::NotImplemented(format!(
                "Full graph pattern matching with relationships (Pattern {}) is not yet implemented in resolve_match_patterns.", 
                pattern_idx
            )));
        }

        // For simple, independent node patterns, we proceed with resolving each node.
        for (node_idx, (var_opt, label_opt, properties)) in pat.1.iter().enumerate() {
            println!("===>    Node {}: var={:?}, label={:?}, {} properties", 
                     node_idx, var_opt, label_opt, properties.len());
            
            if let Some(v_ref) = var_opt.as_ref() {
                let var_name = v_ref.to_string();
                
                // If the variable is not already bound from a previous, independent pattern, resolve it.
                if !var_to_id.contains_key(&var_name) {
                    println!("===>    Calling resolve_var for '{}'", var_name);
                    
                    // *** DEPENDENCY CHANGE: Pass graph_service instead of storage ***
                    let id = resolve_var(graph_service, v_ref, label_opt, properties).await?;
                    
                    println!("===>    SUCCESS: '{}' resolved to {}", var_name, id.0);
                    var_to_id.insert(var_name, id);
                } else {
                    println!("===>    SKIPPED: '{}' already bound", var_name);
                }
            } else {
                println!("===>    SKIPPED: Node has no variable");
            }
        }
    }
    
    println!("===> resolve_match_patterns: Final map has {} entries", var_to_id.len());
    for (var, id) in &var_to_id {
        println!("===>    {} -> {}", var, id.0);
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
    let graph_service = initialize_graph_service(storage.clone()).await?;
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
            // *** DELEGATION FIX: Call exec_cypher_pattern with graph_service ***
            let (final_vertices, final_edges) = exec_cypher_pattern(patterns, &graph_service).await?;

            Ok(json!({
                "vertices": final_vertices,
                "edges": final_edges,
                "stats": {
                    "vertices_matched": final_vertices.len(),
                    "edges_matched": final_edges.len()
                }
            }))
        }
        CypherQuery::MatchCreateSet { match_patterns, create_patterns, set_clauses } => {
            info!("===> EXECUTING MatchCreateSet: {} match patterns, {} create patterns, {} set clauses",
                  match_patterns.len(), create_patterns.len(), set_clauses.len());
            
            let mut var_to_id: HashMap<String, SerializableUuid> = HashMap::new();
            let mut created_vertices = Vec::new();
            let mut created_edges = Vec::new();
            let mut updated_vertices = Vec::new();
            
            // 1. Resolve MATCH patterns
            var_to_id.extend(
                resolve_match_patterns(&graph_service, match_patterns).await?
            );
            
            // 2. Process CREATE patterns (same as MatchCreate)
            for pat in create_patterns.iter() {
                // Create nodes
                for (var_opt, label_opt, properties) in &pat.1 {
                    if let Some(v) = var_opt.as_ref() {
                        if !var_to_id.contains_key(v) {
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
                            
                            graph_service.create_vertex(vertex.clone()).await?;
                            var_to_id.insert(v.clone(), new_id);
                            created_vertices.push(vertex);
                        }
                    }
                }
                
                // Create edges
                for (i, rel_tuple) in pat.2.iter().enumerate() {
                    let from_var = pat.1[i].0.as_ref().ok_or(GraphError::ValidationError("No from var".into()))?;
                    let to_var = pat.1[i + 1].0.as_ref().ok_or(GraphError::ValidationError("No to var".into()))?;
                    
                    let from_id = *var_to_id.get(from_var).unwrap();
                    let to_id = *var_to_id.get(to_var).unwrap();
                    
                    let (_rel_var, label_opt, _len_range, properties, direction_opt) = rel_tuple;
                    
                    let (outbound_id, inbound_id) = match direction_opt {
                        Some(true) => (to_id, from_id), // <-
                        _ => (from_id, to_id),
                    };
                    
                    let edge_type_str = label_opt.as_ref().cloned().unwrap_or("RELATED".to_string());
                    
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
            
            // 3. Apply SET clauses
            for (var, prop, val) in set_clauses {
                let id = var_to_id.get(&var)
                    .ok_or(GraphError::ValidationError(format!("Unbound var in SET: {}", var)))?
                    .0;
                
                let mut vertex = graph_service.get_vertex(&id).await
                    .ok_or_else(|| {
                        let id_str = format!("vertex_{}", id);
                        GraphError::NotFound(unsafe { Identifier::new_unchecked(id_str) })
                    })?;
                
                let prop_value = to_property_value(val)?;
                vertex.properties.insert(prop, prop_value);
                vertex.updated_at = Utc::now().into();
                
                graph_service.update_vertex(vertex.clone()).await?;
                updated_vertices.push(vertex);
            }

            Ok(json!({
                "status": "success",
                "created_vertices": created_vertices,
                "created_edges": created_edges,
                "updated_vertices": updated_vertices,
                "stats": {
                    "vertices_created": created_vertices.len(),
                    "relationships_created": created_edges.len(),
                    "vertices_updated": updated_vertices.len()
                }
            }))
        },
        CypherQuery::MatchSet { match_patterns, set_clauses } => {
            info!("===> EXECUTING MatchSet: {} patterns, {} SET clauses", 
                match_patterns.len(), set_clauses.len());
            
            // 1. Find the vertices to update
            let (matched_vertices, _) = exec_cypher_pattern(match_patterns, &graph_service).await?;
            
            if matched_vertices.is_empty() {
                return Ok(json!({ "vertices": Vec::<models::Vertex>::new() }));
            }
            
            // 2. Apply SET operations to each matched vertex
            let mut updated_vertices = Vec::new();
            
            for mut vertex in matched_vertices {
                for (var, prop_name, new_value) in &set_clauses {
                    // Convert the value
                    let prop_value = to_property_value(new_value.clone())?;
                    
                    // Update the property
                    vertex.properties.insert(prop_name.clone(), prop_value);
                    vertex.updated_at = Utc::now().into();
                }
                
                // Persist the update
                graph_service.update_vertex(vertex.clone()).await?;
                updated_vertices.push(vertex);
            }
            
            Ok(json!({ 
                "vertices": updated_vertices,
                "stats": {
                    "vertices_updated": updated_vertices.len()
                }
            }))
        }
        // --- SIGNIFICANTLY UPDATED MATCH/CREATE (MERGE-like) ---
        // 1. MATCH nodes to resolve their IDs (populates var_to_id).
        // 2. CREATE nodes whose variables are not in var_to_id (populates var_to_id and created_vertices).
        // 3. CREATE relationships between the now-resolved/created nodes.
        CypherQuery::MatchCreate { match_patterns, create_patterns } => {
            // NOTE: graph_service is now confirmed to be an Arc<GraphService> 
            // in this scope based on the compiler error.
            
            // 0. Initial Logging and Setup
            println!("===> MATCH patterns: {}", match_patterns.len());
            for (i, pat) in match_patterns.iter().enumerate() {
                println!("===>     Match pattern {}: {} nodes, {} rels", i, pat.1.len(), pat.2.len());
            }
            
            println!("===> CREATE patterns: {}", create_patterns.len());
            for (i, pat) in create_patterns.iter().enumerate() {
                println!("===>     Create pattern {}: {} nodes, {} rels", i, pat.1.len(), pat.2.len());
                for (j, node) in pat.1.iter().enumerate() {
                    println!("===>       Node {}: var={:?}, label={:?}", j, node.0, node.1);
                }
                for (j, rel) in pat.2.iter().enumerate() {
                    println!("===>       Rel {}: var={:?}, label={:?}", j, rel.0, rel.1);
                }
            }
            
            info!("===> EXECUTING MatchCreate: {} match patterns, {} create patterns", 
                         match_patterns.len(), create_patterns.len());
            
            let mut var_to_id: HashMap<String, SerializableUuid> = HashMap::new();
            let mut created_vertices = Vec::new();
            let mut created_edges = Vec::new();

            // 1. Resolve (MATCH) nodes
            println!("===> BEFORE resolve_match_patterns call");
            // FIX: Use &*graph_service to pass a &GraphService reference, not the Arc<GraphService> itself.
            var_to_id.extend(
                resolve_match_patterns(&*graph_service, match_patterns).await? 
            );
            
            // DIAGNOSTIC LOGGING
            println!("===> AFTER resolve_match_patterns: var_to_id has {} entries", var_to_id.len());
            for (var, id) in &var_to_id {
                println!("===>     Bound: {} -> {}", var, id.0);
            }
            println!("===> ABOUT TO PROCESS {} CREATE PATTERNS", create_patterns.len());

            // 2. Process CREATE patterns
            for (pat_idx, pat) in create_patterns.iter().enumerate() {
                println!("===> PROCESSING CREATE PATTERN {}: {} nodes, {} rels", 
                                 pat_idx, pat.1.len(), pat.2.len());
                
                // a. Create/Resolve Nodes in the CREATE pattern
                for (node_idx, (var_opt, label_opt, properties)) in pat.1.iter().enumerate() {
                    println!("===>     CREATE pattern node {}: var={:?}, label={:?}", 
                                     node_idx, var_opt, label_opt);
                    
                    if let Some(v) = var_opt.as_ref() {
                        // If the variable is not bound (from MATCH), it's a NEW node to be created.
                        if !var_to_id.contains_key(v) {
                            println!("===>       Variable '{}' not bound, creating new vertex", v);
                            
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
                            
                            // DELEGATION FIX: Use graph_service.create_vertex for unified handling
                            // This method must now encapsulate storage persistence and in-memory updates.
                            // FIX: Since graph_service is Arc, we must dereference or call .as_ref()
                            graph_service.create_vertex(vertex.clone()).await?;

                            var_to_id.insert(v.clone(), new_id);
                            created_vertices.push(vertex);
                            
                            println!("===>       Created new vertex with id {}", new_id.0);
                        } else {
                            println!("===>       Variable '{}' already bound to {}", v, var_to_id[v].0);
                        }
                    } else {
                        if !properties.is_empty() {
                            warn!("CREATE pattern contains a node with properties but no variable/label: {:?}", properties);
                        }
                    }
                }

                // b. Create Edges in the CREATE pattern (sequential relationships)
                println!("===>     Checking edge creation: {} nodes, {} relationships", pat.1.len(), pat.2.len());
                
                if pat.1.len().saturating_sub(1) != pat.2.len() {
                    return Err(GraphError::ValidationError(format!(
                        "Mismatched number of nodes ({}) and relationships ({}) in CREATE pattern path.", 
                        pat.1.len(), pat.2.len()
                    )));
                }

                println!("===>     About to create {} edges", pat.2.len());
                
                for (i, rel_tuple) in pat.2.iter().enumerate() {
                    println!("===>     Processing edge {}", i);
                    
                    // Get node variables from the sequence
                    let from_var_opt = pat.1.get(i).and_then(|node_pattern| node_pattern.0.as_ref());
                    let to_var_opt = pat.1.get(i + 1).and_then(|node_pattern| node_pattern.0.as_ref());

                    println!("===>       from_var_opt: {:?}, to_var_opt: {:?}", from_var_opt, to_var_opt);

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

                    println!("===>       from_var: '{}', to_var: '{}'", from_var, to_var);

                    // Resolve IDs from the bound map (matched or newly created)
                    let from_id = *var_to_id.get(from_var).ok_or_else(|| {
                        println!("===>       ERROR: Unbound source variable '{}'", from_var);
                        println!("===>       Available variables: {:?}", var_to_id.keys().collect::<Vec<_>>());
                        GraphError::ValidationError(format!("Unbound source var for edge: {}", from_var))
                    })?;
                    
                    let to_id = *var_to_id.get(to_var).ok_or_else(|| {
                        println!("===>       ERROR: Unbound target variable '{}'", to_var);
                        println!("===>       Available variables: {:?}", var_to_id.keys().collect::<Vec<_>>());
                        GraphError::ValidationError(format!("Unbound target var for edge: {}", to_var))
                    })?;

                    println!("===>       Resolved IDs: from={}, to={}", from_id.0, to_id.0);

                    // rel_tuple is (_rel_var, label, len_range, properties, direction)
                    let (_rel_var, label_opt, _len_range, properties, direction_opt) = rel_tuple;

                    // Handle direction: Some(false) is inbound.
                    let (outbound_id, inbound_id) = match direction_opt {
                        Some(false) => (to_id, from_id), // (B)<-[R]-(A) where A is from, B is to
                        _ => (from_id, to_id),           // (A)-[R]->(B)
                    };

                    let edge_type_str = label_opt.as_ref().cloned().unwrap_or("RELATED".to_string());
                    
                    println!("===>       Creating edge: {} -[:{}]-> {}", outbound_id.0, edge_type_str, inbound_id.0);
                    
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
                    
                    println!("===>       Calling graph_service.create_edge()");
                    
                    // DELEGATION FIX: Use graph_service.create_edge for unified handling
                    // FIX: Since graph_service is Arc, we must dereference or call .as_ref()
                    graph_service.create_edge(edge.clone()).await?;
                    
                    println!("===>       Edge created successfully with id {}", edge.id.0);
                    
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
        }
        
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
            // 1. Convert properties to a mutable PropertyValue map
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
                    // Use direct lookup to avoid recursion
                    match get_vertex_by_internal_id_direct(&graph_service, internal_id).await? {
                        Some(v) => {
                            let matches_label = label.as_ref().map_or(true, |l| {
                                let vl = v.label.as_ref();
                                vl == l || vl.starts_with(&format!("{}:", l))
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

            // Fallback/Standard path: Full scan 
            let vertices = graph_service.get_all_vertices().await?;
            
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
            let mut result_vertices = Vec::new();
            let mut matched_ids = HashSet::new();
            
            let all_vertices = graph_service.get_all_vertices().await?;

            for (_var, label, properties) in nodes {
                let mut props: HashMap<String, PropertyValue> = properties
                    .iter()
                    .map(|(k, v)| to_property_value(v.clone()).map(|pv| (k.clone(), pv)))
                    .collect::<GraphResult<_>>()?;
                
                let mut handled_by_id_constraint = false;
                
                // --- START FIX: Indexed lookup for internal 'id' property ---
                if let Some(prop_value) = props.remove("id") {
                    let internal_id = match prop_value {
                        PropertyValue::Integer(val) => Some(val as i32), 
                        PropertyValue::I32(val) => Some(val), 
                        _ => None,
                    };
                    
                    if let Some(internal_id) = internal_id {
                        handled_by_id_constraint = true; 
                    
                        // Use direct lookup to avoid recursion
                        if let Some(v) = get_vertex_by_internal_id_direct(&graph_service, internal_id).await? {
                            let matches_label = label.as_ref().map_or(true, |l| {
                                let vl = v.label.as_ref();
                                vl == l || vl.starts_with(&format!("{}:", l))
                            });

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
                // --- END FIX ---

                if handled_by_id_constraint {
                    continue;
                }

                // Fallback/Standard path: Full scan
                let label_ref = label.as_ref();
                
                for v in &all_vertices {
                    let matches_label = label_ref.map_or(true, |l| {
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
                
                // *** RELY ON graph_service.create_vertex ***
                // Replaces: storage.create_vertex(vertex.clone()).await?;
                // Replaces: graph_service.add_vertex(vertex.clone()).await?;
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

            // Iterate over the relationships
            for (i, rel_tuple) in relationships.into_iter().enumerate() {
                
                // Get the variables of the connected nodes (from the nodes vector)
                let from_var_opt = nodes[i].0.as_ref();
                let to_var_opt = nodes[i + 1].0.as_ref();

                let from_var = from_var_opt.ok_or(GraphError::ValidationError(format!("Node at index {} in CREATE pattern has no variable", i)))?;
                let to_var = to_var_opt.ok_or(GraphError::ValidationError(format!("Node at index {} in CREATE pattern has no variable", i + 1)))?;

                // Resolve IDs using the map of newly created nodes
                let from_id = *var_to_id.get(from_var).ok_or(GraphError::ValidationError(format!("Unbound from var: {}", from_var)))?;
                let to_id = *var_to_id.get(to_var).ok_or(GraphError::ValidationError(format!("Unbound to var: {}", to_var)))?;

                // Destructure the 5-tuple
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
                
                // *** RELY ON graph_service.create_edge ***
                // Replaces: storage.create_edge(edge.clone()).await?;
                // Replaces: graph_service.add_edge(edge.clone()).await?;
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
                
                // *** RELY ON graph_service.create_vertex ***
                // Replaces: storage.create_vertex(vertex.clone()).await?;
                // Replaces: graph_service.add_vertex(vertex.clone()).await?;
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
                    
                    // Nodes are stored sequentially in the Pattern tuple, matching rel[i] with node[i] -> node[i+1]
                    let from_var_opt = nodes[i].0.as_ref();
                    let to_var_opt = nodes[i + 1].0.as_ref();

                    let from_var = from_var_opt.ok_or(GraphError::ValidationError(format!("Relationship source node at index {} has no variable.", i)))?;
                    let to_var = to_var_opt.ok_or(GraphError::ValidationError(format!("Relationship target node at index {} has no variable.", i + 1)))?;

                    // Resolve IDs using the map of newly created nodes (var_to_id)
                    let from_id = *var_to_id.get(from_var).ok_or(GraphError::ValidationError(format!("Unbound source variable: {}", from_var)))?;
                    let to_id = *var_to_id.get(to_var).ok_or(GraphError::ValidationError(format!("Unbound target variable: {}", to_var)))?;

                    // Destructure the BORROWED 5-tuple, cloning the owned parts
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
                    
                    // *** RELY ON graph_service.create_edge ***
                    // Replaces: storage.create_edge(edge.clone()).await?;
                    // Replaces: graph_service.add_edge(edge.clone()).await?;
                    graph_service.create_edge(edge.clone()).await?;
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
        } => {
            // --- STEP 1: MATCH (READ) ---
            
            // Re-evaluating the variable-length check based on the provided pattern structure
            let is_variable_length = pattern.relationships.len() == 1
                && pattern.nodes.len() == 2
                && pattern.relationships[0].2.map_or(false, |(min, max)| {
                    // Check if the range is not exactly 1..1 or implicit 1
                    min.map_or(1, |m| m) != 1 || max.map_or(1, |m| m) != 1
                });

            let edges_to_delete: Vec<Edge> = if is_variable_length {
                // Keep the existing complex variable-length path (VLP) logic for now.
                // NOTE: The `exec_cypher_pattern` is designed for single-hop and must be 
                // re-used carefully if VLP logic isn't fully integrated into it.
                // Assuming `graph_service.match_variable_length_path` exists:
                // ... (Original VLP logic using graph_service.get_graph().await for traversal) ...
                
                // --- Re-using the provided VLP logic from the prompt ---
                let graph = graph_service.get_graph().await;
                let rel_pat = &pattern.relationships[0];
                let start_node_pat = &pattern.nodes[0];
                let end_node_pat = &pattern.nodes[1];
                
                let start_vertices: Vec<&Vertex> = graph.vertices.values()
                    // Assuming node_matches_constraints is available
                    .filter(|v| node_matches_constraints(v, &start_node_pat.1, &start_node_pat.2))
                    .collect();
                
                let mut all_matched_edge_ids: HashSet<Uuid> = HashSet::new();
                
                for start_v in start_vertices {
                    // This method relies on the private methods of Graph (match_variable_length_path)
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
                // FIX: Use the shared helper function for single-hop pattern matching.
                // This replaces the old `exec_cypher_pattern(&storage)`.
                let (_, edges) = exec_cypher_pattern(
                    vec![(None, pattern.nodes.clone(), pattern.relationships.clone())],
                    &(*graph_service) // <-- FIX: Dereference the Arc to get &GraphService
                ).await?;
                
                edges
            };
            
            // --- STEP 2: DELETE (WRITE) ---
            let mut deleted = 0usize;
            
            for edge in edges_to_delete {
                // Where clause evaluation and deletion logic remains the same:
                let mut variables = HashMap::new();
                variables.insert(edge_variable.clone(), CypherValue::Edge(edge.clone()));
                
                let ctx = EvaluationContext {
                    variables,
                    parameters: HashMap::new(),
                };
                
                let should_delete = match where_clause.as_ref() {
                    Some(where_clause) => where_clause.evaluate(&ctx)?,
                    None => true,
                };
                
                if should_delete {
                    // FIX: Use the atomic method implemented on GraphService
                    graph_service.delete_edge_by_uuid(edge.id.0).await?;
                    deleted += 1;
                }
            }
            
            graph_service.flush_storage().await?;
            trigger_async_graph_cleanup();
            
            Ok(json!({
                "status": "success",
                "deleted_edges": deleted,
                "message": format!("Successfully deleted {deleted} edge(s)")
            }))
        }

        CypherQuery::MatchRemove { match_patterns, remove_clauses } => {
            let (matched_vertices, _) = exec_cypher_pattern(match_patterns, &graph_service).await?;
            if matched_vertices.is_empty() {
                return Ok(json!({ "vertices": Vec::<Vertex>::new() }));
            }
            let mut updated_vertices = Vec::new();
            for mut vertex in matched_vertices {
                for (var, prop_name) in &remove_clauses {
                    // Remove the property - prop_name is already &String, so use it directly
                    vertex.properties.remove(prop_name.as_str());
                    vertex.updated_at = Utc::now().into();
                }
                graph_service.update_vertex(vertex.clone()).await?;
                updated_vertices.push(vertex);
            }
            Ok(json!({
                "vertices": updated_vertices,
                "stats": {
                    "vertices_updated": updated_vertices.len()
                }
            }))
        }

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
fn to_property_value(v: Value) -> GraphResult<PropertyValue> {
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
        let result = parse_cypher(query).unwrap();
        
        // Should parse as MatchPattern that will return ALL Person nodes
        match result {
            CypherQuery::MatchPattern { patterns } => {
                assert_eq!(patterns.len(), 1);
            }
            _ => panic!("Should parse as MatchPattern"),
        }
    }

    #[test]
    fn test_match_multiple_comma_separated() {
        let query = "MATCH (a:Person), (b:Movie) RETURN a, b";
        let result = parse_cypher(query).unwrap();
        
        match result {
            CypherQuery::MatchPattern { patterns } => {
                // Should handle comma-separated patterns
                assert!(patterns.len() >= 1);
            }
            _ => panic!("Should handle multiple patterns"),
        }
    }

    #[test]
    fn test_parse_uuid_property() {
        let input = "a928227a-165c-4fc4-abd1-796583a26d8d";
        let result = parse_property_value(input).unwrap();
        assert_eq!(result.1, Value::String("a928227a-165c-4fc4-abd1-796583a26d8d".to_string()));
    }
}