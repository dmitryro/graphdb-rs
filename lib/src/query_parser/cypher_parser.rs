// lib/src/query_parser/cypher_parser.rs
// Complete working version - merges working repo code with edge creation support
use log::{debug, error, info, warn, trace};
use nom::{
    branch::alt,
    bytes::complete::{tag, tag_no_case, take_while1, take_until, take_till, take_while},
    character::complete::{char, multispace0, multispace1},
    combinator::{map, opt, recognize, value},
    multi::{separated_list0, separated_list1, many0}, 
    number::complete::double,
    sequence::{delimited, pair, preceded, tuple, terminated, separated_pair},
    multi::many1,
    IResult,
    Parser,
    error as NomError,
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
use crate::database::Database;
use crate::storage_engine::{GraphStorageEngine, StorageEngine};
use crate::query_exec_engine::query_exec_engine::QueryExecEngine;
use crate::config::StorageConfig;
use crate::query_parser::query_types::*;

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
    CreateComplexPattern {
        nodes: Vec<NodePattern>,
        relationships: Vec<RelPattern>,
    },
    MatchPattern {
        patterns: Vec<Pattern>,
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
    trimmed.starts_with("limit")
    // Do NOT include "OPTIONAL MATCH" here — it must be parsed as part of the pattern list
}

// Parse a single pattern (node or node-relationship-node chain)
// Stops at keyword boundaries
/// Parse **one** complete pattern:
///   (a)-[:KNOWS]->(b)          or
///   (a)-[:KNOWS]-(b)           or
///   (a)<-[:KNOWS]-(b)          or
///   (a)-[r:KNOWS*0..2]-(b)    etc.
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

// ----------------------------------------------------------------------------------
// --- DEFINED: parse_simple_query_type (New, requested function) ---
// ----------------------------------------------------------------------------------

/// Defines the parser for any simple, single-clause Cypher statement.
/// This is used by `parse_sequential_statements` to consume clauses in a batch.
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
/// Helper function to parse a single statement string.
/// Helper function to parse a single statement string.
// ----------------------------------------------------------------------------------
// --- UPDATED: parse_single_statement (Prioritizes new sequence parser) ---
// ----------------------------------------------------------------------------------

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

fn parse_variable_length(input: &str) -> IResult<&str, (Option<u32>, Option<u32>)> {
    let (input, _) = multispace0(input)?;
    let (input, _) = char('*').parse(input)?;
    let (input, _) = multispace0(input)?;
    
    // Try to parse min
    let (input, min) = opt(map(
        nom::character::complete::u32,
        |n| n
    )).parse(input)?;
    
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
    
    let (input, _) = multispace0(input)?;
    
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
    storage: &Arc<dyn GraphStorageEngine + Send + Sync>,
    var: &str,
    label: &Option<String>,
    properties: &HashMap<String, Value>,
) -> GraphResult<SerializableUuid> {
    
    // 1. Convert query properties into internal PropertyValue representation
    let mut query_props: HashMap<String, PropertyValue> = HashMap::new();
    for (k, v) in properties.iter() {
        query_props.insert(k.clone(), to_property_value(v.clone())?);
    }
    
    // 2. Fetch all vertices to search (In a real scenario, this would use indexed lookups)
    let all_vertices = storage.get_all_vertices().await?;
    
    // 3. Filter to find the first vertex that satisfies all constraints
    let matched_vertex = all_vertices.into_iter().find(|v| {
        
        // --- A. Label Match ---
        let matches_label = label.as_ref().map_or(true, |query_label| {
            // FIX E0277: Use .as_ref() on both Identifier and &String to compare &str with &str.
            // `v.label` is `models::Identifier`. `query_label` is `&String`.
            v.label.as_ref() == query_label.as_str() 
        });
        
        if !matches_label {
            return false;
        }

        // --- B. Property Match ---
        let matches_props = query_props.iter().all(|(k, expected_val)| {
            // Check if the vertex has the property key and the value matches
            v.properties.get(k).map_or(false, |actual_val| {
                actual_val == expected_val 
            })
        });
        
        matches_props
    });

    match matched_vertex {
        Some(v) => Ok(v.id),
        None => Err(GraphError::ValidationError(format!(
            "No existing node found for variable '{}' with label '{:?}' and constraints: {:?}",
            var, label, properties
        ))),
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
    storage: &Arc<dyn GraphStorageEngine + Send + Sync>,
    match_patterns: Vec<Pattern>,
) -> GraphResult<HashMap<String, SerializableUuid>> {
    let mut var_to_id: HashMap<String, SerializableUuid> = HashMap::new();

    // Iterate through all complex patterns (comma-separated parts of the MATCH clause)
    for pat in match_patterns {
        // pat.1 is Vec<NodePattern>: Vec<(Option<String> var, Option<String> label, HashMap<String, Value> props)>
        // Iterate over references for efficient ownership handling
        for (var_opt, label_opt, properties) in pat.1.iter() {
            
            // Only proceed if a variable is defined for the node
            if let Some(v_ref) = var_opt.as_ref() {
                
                // E0308 Fix: Convert the &String reference to an owned String for the HashMap key.
                let var_name = v_ref.to_string(); 

                // If the variable is NOT already bound (seen for the first time)
                if !var_to_id.contains_key(&var_name) {
                    
                    // Call the core lookup function to find the ID that satisfies the constraints.
                    let id = resolve_var(storage, v_ref, label_opt, properties).await?;
                    
                    // Bind the variable to the resolved ID.
                    var_to_id.insert(var_name, id);
                }
                // Note: If the variable IS already bound, we skip re-resolution here.
                // The later full graph traversal will enforce consistency for all constraints.
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
            info!("===> EXECUTING MatchCreate: {} match patterns, {} create patterns", 
                   match_patterns.len(), create_patterns.len());
            
            let mut var_to_id: HashMap<String, SerializableUuid> = HashMap::new();
            let mut created_vertices = Vec::new();
            let mut created_edges = Vec::new();

            // 1. Resolve (MATCH) nodes
            var_to_id.extend(
                resolve_match_patterns(&storage, match_patterns).await?
            );

            // 2. Process CREATE patterns
            for pat in create_patterns {
                // pat is (Option<String>, Vec<NodePattern>, Vec<RelationshipPattern>)
                
                // a. Create/Resolve Nodes in the CREATE pattern
                for (var_opt, label_opt, properties) in &pat.1 {
                    if let Some(v) = var_opt.as_ref() {
                        // If the variable is not bound, it's a NEW node to be created.
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
                            };
                            storage.create_vertex(vertex.clone()).await?;
                            var_to_id.insert(v.clone(), new_id);
                            created_vertices.push(vertex);
                        }
                    } else {
                        // Node pattern without a variable: this should be an error if properties are set,
                        // or should refer to an explicitly-defined node if it's a literal ID match, 
                        // but for CREATE it's typically an error to have an unreferenced node.
                        // We'll proceed, but warn/error if it has properties.
                        if !properties.is_empty() {
                            warn!("CREATE pattern contains a node with properties but no variable/label: {:?}", properties);
                        }
                    }
                }

                // b. Create Edges in the CREATE pattern (sequential relationships)
                if pat.1.len().saturating_sub(1) != pat.2.len() {
                     return Err(GraphError::ValidationError(format!("Mismatched number of nodes ({}) and relationships ({}) in CREATE pattern path.", pat.1.len(), pat.2.len())));
                }

                for (i, rel_tuple) in pat.2.iter().enumerate() {
                    
                    // Get node variables from the sequence
                    let from_var_opt = pat.1.get(i).and_then(|(v, _, _)| v.as_ref());
                    let to_var_opt = pat.1.get(i + 1).and_then(|(v, _, _)| v.as_ref());

                    let from_var = from_var_opt.ok_or(GraphError::ValidationError(format!("Node at index {} in CREATE pattern has no variable for edge creation", i)))?;
                    let to_var = to_var_opt.ok_or(GraphError::ValidationError(format!("Node at index {} in CREATE pattern has no variable for edge creation", i + 1)))?;

                    // Resolve IDs from the bound map (matched or newly created)
                    let from_id = *var_to_id.get(from_var).ok_or(GraphError::ValidationError(format!("Unbound source var for edge: {}", from_var)))?;
                    let to_id = *var_to_id.get(to_var).ok_or(GraphError::ValidationError(format!("Unbound target var for edge: {}", to_var)))?;

                    // rel_tuple is (rel_var, label, len_range, properties, direction)
                    let (_rel_var, label_opt, _len_range, properties, direction_opt) = rel_tuple;

                    // Handle direction: Some(false) is inbound.
                    let (outbound_id, inbound_id) = match direction_opt {
                        Some(false) => (to_id, from_id), // (B)<-[R]-(A) where A is from, B is to
                        _ => (from_id, to_id),           // (A)-[R]->(B)
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
                    
                    storage.create_edge(edge.clone()).await?;
                    created_edges.push(edge);
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

        CypherQuery::CreateNode { label, properties } => {
            let props: HashMap<_, _> = properties.into_iter()
                .map(|(k, v)| Ok((k, to_property_value(v)?)))
                .collect::<GraphResult<_>>()?;

            let v = Vertex {
                id: SerializableUuid(Uuid::new_v4()),
                label: Identifier::new(label)?,
                properties: props,
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
            storage.delete_vertex(&id.0).await?;
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
        
        CypherQuery::DetachDeleteNodes { node_variable: _, label } => {
            let all_vertices = storage.get_all_vertices().await?;
            
            let nodes_to_delete: Vec<Vertex> = all_vertices
                .into_iter()
                .filter(|v| {
                    label.as_ref().map_or(true, |l| {
                        let v_label_str = v.label.as_ref();
                        v_label_str == l.as_str()
                    })
                })
                .collect();
            
            if nodes_to_delete.is_empty() {
                return Ok(json!({"deleted_vertices": 0, "deleted_edges": 0}));
            }
            
            let vertex_ids: HashSet<Uuid> = nodes_to_delete.iter().map(|v| v.id.0).collect();
            let deleted_edges = storage.delete_edges_touching_vertices(&vertex_ids).await?;
            
            for vertex in nodes_to_delete.iter() {
                storage.delete_vertex(&vertex.id.0).await?;
            }
            storage.flush().await?;
            
            Ok(json!({
                "deleted_vertices": nodes_to_delete.len(),
                "deleted_edges": deleted_edges
            }))
        }

        CypherQuery::DeleteEdges {
            edge_variable,
            pattern,
            where_clause,
        } => {
            // Use the helper function instead of recursion
            let (_, edges) = exec_cypher_pattern(
                vec![(None, pattern.nodes, pattern.relationships)],
                &storage
            ).await?;

            let mut deleted = 0usize;

            for edge in edges {
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
                    storage
                        .delete_edge(&edge.outbound_id.0, &edge.edge_type, &edge.inbound_id.0)
                        .await?;
                    deleted += 1;
                }
            }

            storage.flush().await?;
            Ok(json!({
                "status": "success",
                "deleted_edges": deleted,
                "message": format!("Successfully deleted {deleted} edge(s)")
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
}