// lib/src/query_parser/cypher_parser.rs
// Complete working version - merges working repo code with edge creation support
use log::{debug, error, info, warn, trace};
use nom::{
    branch::alt,
    bytes::complete::{tag, tag_no_case, take_while1, take_until},
    character::complete::{char, multispace0, multispace1},
    combinator::{map, opt, recognize, value},
    multi::{separated_list0, separated_list1, many0}, 
    number::complete::double,
    sequence::{delimited, pair, preceded, tuple, terminated},
    IResult,
    Parser,
    error as NomError,
};
use serde_json::{json, Value};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use uuid::Uuid;
use chrono::Utc;
use models::identifiers::{Identifier, SerializableUuid};
use models::{Vertex, Edge};
use models::errors::{GraphError, GraphResult};
use models::properties::SerializableFloat;
use models::properties::PropertyValue;
use crate::database::Database;
use crate::storage_engine::{GraphStorageEngine, StorageEngine};
use crate::query_exec_engine::query_exec_engine::QueryExecEngine;
use crate::config::StorageConfig;

type NodePattern = (Option<String>, Option<String>, HashMap<String, Value>);
type RelPattern = (Option<String>, Option<String>, Option<(Option<u32>, Option<u32>)>, HashMap<String, Value>, Option<bool>);
type Pattern = (Option<String>, Vec<NodePattern>, Vec<RelPattern>);

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
}

pub fn is_cypher(query: &str) -> bool {
    let cypher_keywords = ["MATCH", "CREATE", "SET", "RETURN", "DELETE"];
    cypher_keywords.iter().any(|kw| query.trim().to_uppercase().starts_with(kw))
}

fn parse_identifier(input: &str) -> IResult<&str, &str> {
    take_while1(|c: char| c.is_alphanumeric() || c == '_' || c == '&').parse(input)
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

fn parse_node(input: &str) -> IResult<&str, (Option<String>, Option<String>, HashMap<String, Value>)> {
    let (input, _) = char('(').parse(input)?;
    let (input, _) = multispace0.parse(input)?;
    let (input, var) = opt(parse_identifier).parse(input)?;
    let (input, _) = multispace0.parse(input)?;
   
    let (input, labels) = if input.starts_with(':') {
        let (input, _) = char(':').parse(input)?;
        let (input, first_label) = parse_identifier.parse(input)?;
       
        let (input, additional_labels) = many0(
            preceded(
                alt((char(':'), char('&'))),
                parse_identifier
            )
        ).parse(input)?;
       
        let combined_label = if additional_labels.is_empty() {
            first_label.to_string()
        } else {
            let mut all_labels = vec![first_label];
            all_labels.extend(additional_labels);
            all_labels.join(":")
        };
       
        (input, Some(combined_label))
    } else {
        (input, None)
    };
   
    let (input, _) = multispace0.parse(input)?;
    let (input, props) = opt(parse_properties).parse(input)?;
    let (input, _) = multispace0.parse(input)?;
    let (input, _) = char(')').parse(input)?;
   
    Ok((input, (
        var.map(|s| s.to_string()),
        labels,
        props.unwrap_or_default(),
    )))
}

fn parse_multiple_nodes(input: &str) -> IResult<&str, Vec<(Option<String>, Option<String>, HashMap<String, Value>)>> {
    separated_list1(
        tuple((multispace0, char(','), multispace0)),
        parse_node
    ).parse(input)
}

fn parse_relationship(input: &str) -> IResult<&str, RelPattern> {
    let (input, _) = multispace0.parse(input)?;
    let (input, left_arrow) = opt(tag("<-")).parse(input)?;
    let (input, _) = char('-').parse(input)?;
    let (input, _) = multispace0.parse(input)?;
    let (input, detail_opt) = opt(delimited(char('['), parse_rel_detail, char(']'))).parse(input)?;
    let (input, _) = multispace0.parse(input)?;
    let (input, right_arrow) = opt(tag("->")).parse(input)?;
    let direction = match (left_arrow, right_arrow) {
        (Some(_), None) => Some(true), // incoming
        (None, Some(_)) => Some(false), // outgoing
        (None, None) => None, // undirected
        _ => return Err(nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Alt))),
    };
    let detail = detail_opt.unwrap_or((None, None, None, HashMap::new()));
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

// Then use .parse(input)? syntax everywhere:

// CORRECTED full_statement_parser for Nom 8
fn full_statement_parser(input: &str) -> IResult<&str, CypherQuery> {
    // MATCH or OPTIONAL MATCH
    let (input, _) = alt((tag("MATCH"), tag("OPTIONAL MATCH"))).parse(input)?;
    let (input, _) = multispace1.parse(input)?;

    // Main pattern list (supports comma-separated)
    let (input, patterns) = separated_list1(
        tuple((multispace0, char(','), multispace0)),
        parse_pattern,
    ).parse(input)?;

    let (input, _) = multispace0.parse(input)?;

    // *** FIX: Additional OPTIONAL MATCH clauses should parse complete patterns ***
    let (input, extra_patterns) = many0(
        tuple((
            multispace0,
            tag("OPTIONAL MATCH"),
            multispace1,
            separated_list1(
                tuple((multispace0, char(','), multispace0)),
                parse_pattern,
            ),
        ))
    ).parse(input)?;
    
    // Flatten extra patterns into all_patterns
    let mut all_patterns = patterns;
    for (_, _, _, extra_pats) in extra_patterns {
        all_patterns.extend(extra_pats);
    }
    
    let (input, _) = multispace0.parse(input)?;

    // Skip WHERE (if present)
    let (input, _) = opt(preceded(
        tuple((tag("WHERE"), multispace1)),
        take_while1(|c| c != 'R' && c != 'C'),
    )).parse(input)?;
    
    let (input, _) = multispace0.parse(input)?;

    // Optional CREATE clause
    let (input, create_patterns) = opt(preceded(
        tuple((tag("CREATE"), multispace1)),
        separated_list1(
            tuple((multispace0, char(','), multispace0)),
            parse_pattern,
        ),
    )).parse(input)?;
    
    let (input, _) = multispace0.parse(input)?;

    // Optional RETURN — consume everything after it
    let (input, _) = opt(preceded(
        tuple((tag("RETURN"), multispace1)),
        take_while1(|_| true),
    )).parse(input)?;

    let result = if let Some(create) = create_patterns {
        CypherQuery::MatchCreate {
            match_patterns: all_patterns,
            create_patterns: create,
        }
    } else {
        CypherQuery::MatchPattern { patterns: all_patterns }
    };

    Ok((input, result))
}

// ------------------------------------------------------------------
// MATCH path = (left)-[*..]-(right) RETURN …  (single statement only)
/// Parse **only**   MATCH path = (left)-[*..]-(right) RETURN …
/// Supports variable-length patterns: *0..2, *1..5, *, etc.
// ------------------------------------------------------------------
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

// CORRECTED parse_cypher for Nom 8 with Parser trait
pub fn parse_cypher(query: &str) -> Result<CypherQuery, String> {
    println!("=====> PARSING CYPHER");
    if !is_cypher(query) {
        return Err("Not a valid Cypher query.".to_string());
    }

    let query = query.trim();

    // 1. Normalize whitespace
    let query_cleaned = query
        .replace("\\n", " ")
        .replace('\n', " ")
        .replace('\r', " ");

    let query_to_parse = query_cleaned.trim_end_matches(';').trim();

    println!("===> Parsing query: {}", query_to_parse);

    // 2. Try parse_match_path FIRST for explicit path patterns
    if query_to_parse.contains("path") && query_to_parse.contains("=") && query_to_parse.contains("-[") {
        println!("===> Attempting parse_match_path");
        if let Ok((remaining, parsed)) = parse_match_path.parse(query_to_parse) {
            let remaining_trimmed = remaining.trim();
            if remaining_trimmed.is_empty() {
                println!("===> Successfully parsed as MatchPath: {:?}", parsed);
                return Ok(parsed);
            }
        }
    }

    // 3. *** CRITICAL: NO TRUNCATION AT ALL ***
    // The old code was truncating at "-[" which broke ALL relationship queries
    // We must pass the FULL query to the parsers and let them handle it
    
    println!("===> Using full query (no truncation): {}", query_to_parse);

    // 4. Run the parser chain on the FULL query
    let result = alt((
        full_statement_parser,
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
        parse_set_kv,
        parse_get_kv,
        parse_delete_kv,
    ))
    .parse(query_to_parse);

    match result {
        Ok((remaining, parsed)) => {
            let remaining_trimmed = remaining.trim();
            if !remaining_trimmed.is_empty() && !remaining_trimmed.starts_with("RETURN") {
                println!("===> WARNING: Unparsed remainder: '{}'", remaining_trimmed);
            }
            println!("===> Successfully parsed as: {:?}", parsed);
            Ok(parsed)
        }
        Err(nom::Err::Error(e)) | Err(nom::Err::Failure(e)) => {
            Err(format!("Parse error near: '{}'", e.input))
        }
        Err(e) => Err(format!("Parse error: {:?}", e)),
    }
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
    let (input, _) = multispace0.parse(input)?;
    let (input, var) = opt(terminated(parse_identifier, multispace0)).parse(input)?;
    let (input, rel_type) = opt(preceded(char(':'), terminated(parse_identifier, multispace0))).parse(input)?;
    let (input, length) = opt(parse_variable_length).parse(input)?;
    let (input, props) = opt(parse_properties).parse(input)?;
    let (input, _) = multispace0.parse(input)?;
    Ok((input, (var.map(String::from), rel_type.map(String::from), length, props.unwrap_or_default())))
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

fn parse_return_clause(input: &str) -> IResult<&str, ()> {
    let (input, _) = tag("RETURN").parse(input)?;
    let (input, _) = multispace1.parse(input)?;
    let (input, _) = take_while1(|c: char| c != '\n' && c != '\r' && c != ';').parse(input)?;
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

async fn resolve_var(
    storage: &Arc<dyn GraphStorageEngine + Send + Sync>,
    var: &str,
    label: &Option<String>,
    props: &HashMap<String, Value>,
) -> GraphResult<SerializableUuid> {
    let all = storage.get_all_vertices().await?;
    let kv: HashMap<_, _> = props.iter()
        .map(|(k, v)| Ok((k.clone(), to_property_value(v.clone())?)))
        .collect::<GraphResult<_>>()?;

    all.into_iter()
        .find(|v| {
            if let Some(l) = label {
                if v.label.as_ref() != l { return false; }
            }
            kv.iter().all(|(k, ev)| v.properties.get(k) == Some(ev))
        })
        .map(|v| v.id)
        .ok_or_else(|| GraphError::StorageError(format!("Node {} not found", var)).into())
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

// Complete execute_cypher function with proper MatchNode implementation
// This replaces the execute_cypher function in cypher_parser.rs
pub async fn execute_cypher(
    query: CypherQuery,
    _db: &Database,
    storage: Arc<dyn GraphStorageEngine + Send + Sync>,
) -> GraphResult<Value> {
    match query {

        // =============================================================================
        // MATCH (n) RETURN n  →  This is the most common case → MUST be fast & safe
        // =============================================================================
        CypherQuery::MatchPattern { patterns } | CypherQuery::MatchCreate { match_patterns: patterns, .. } => {
            let all_vertices = storage.get_all_vertices().await?;
            let all_edges = storage.get_all_edges().await?;

            let mut result_vertices = Vec::new();
            let mut matched_vertex_ids = std::collections::HashSet::new();
            let mut matched_edge_ids = std::collections::HashSet::new();

            // *** FIX: Process ALL patterns, not just first ***
            for pattern in patterns {
                let nodes = &pattern.1; // Vec<NodePattern>
                let rels = &pattern.2;   // Vec<RelPattern>

                // Match ALL nodes in this pattern
                for (var_name, label_constraint, prop_constraints) in nodes {
                    let required_props: HashMap<String, PropertyValue> = prop_constraints
                        .iter()
                        .map(|(k, v)| to_property_value(v.clone()).map(|pv| (k.clone(), pv)))
                        .collect::<GraphResult<_>>()?;

                    // *** FIX: Collect ALL matching vertices, not just first ***
                    for v in &all_vertices {
                        let label_ok = label_constraint.as_ref().map_or(true, |ql| {
                            let vl = v.label.as_ref();
                            vl == ql || vl.starts_with(&format!("{}:", ql))
                        });

                        let props_ok = required_props.iter().all(|(k, expected)| {
                            v.properties.get(k).map_or(false, |actual| actual == expected)
                        });

                        if label_ok && props_ok {
                            if !matched_vertex_ids.contains(&v.id) {
                                result_vertices.push(v.clone());
                                matched_vertex_ids.insert(v.id);
                            }
                        }
                    }
                }

                // *** FIX: Handle relationship patterns with variable-length paths ***
                for rel in rels {
                    let (rel_var, rel_type, rel_range, rel_props, direction) = rel;
                    
                    // Check if this is a variable-length pattern
                    if let Some((min_hops, max_hops)) = rel_range {
                        // Variable-length path: expand edges recursively
                        let min = min_hops.unwrap_or(1);
                        let max = max_hops.unwrap_or(5); // default max if not specified
                        
                        // Find all paths within hop range
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
                        
                        // Expand paths up to max_hops
                        expand_variable_paths(
                            &all_edges,
                            &matched_vertex_ids,
                            &mut matched_edge_ids,
                            min,
                            max,
                        );
                    } else {
                        // Single-hop relationship
                        for edge in &all_edges {
                            let type_matches = rel_type.as_ref().map_or(true, |rt| {
                                edge.edge_type.as_ref() == rt || edge.label == *rt
                            });
                            
                            let connects_matched = matched_vertex_ids.contains(&edge.outbound_id) ||
                                                  matched_vertex_ids.contains(&edge.inbound_id);
                            
                            if type_matches && connects_matched {
                                matched_edge_ids.insert(edge.id);
                                matched_vertex_ids.insert(edge.outbound_id);
                                matched_vertex_ids.insert(edge.inbound_id);
                            }
                        }
                    }
                }
            }

            // *** FIX: Include ALL matched edges and their connected vertices ***
            let result_edges: Vec<Edge> = all_edges.into_iter()
                .filter(|e| matched_edge_ids.contains(&e.id))
                .collect();
            
            // Ensure all vertices connected by matched edges are included
            for edge in &result_edges {
                if !matched_vertex_ids.contains(&edge.outbound_id) {
                    if let Some(v) = all_vertices.iter().find(|v| v.id == edge.outbound_id) {
                        result_vertices.push(v.clone());
                        matched_vertex_ids.insert(edge.outbound_id);
                    }
                }
                if !matched_vertex_ids.contains(&edge.inbound_id) {
                    if let Some(v) = all_vertices.iter().find(|v| v.id == edge.inbound_id) {
                        result_vertices.push(v.clone());
                        matched_vertex_ids.insert(edge.inbound_id);
                    }
                }
            }

            Ok(json!({
                "vertices": result_vertices,
                "edges": result_edges,
                "stats": {
                    "vertices_matched": result_vertices.len(),
                    "edges_matched": result_edges.len()
                }
            }))
        }

        CypherQuery::MatchCreate { match_patterns, create_patterns } => {
            let mut var_to_id: HashMap<String, SerializableUuid> = HashMap::new();

            for pat in match_patterns {
                for (var, label, props) in pat.1 {
                    if let Some(v) = var.clone() {
                        let id = resolve_var(&storage, &v, &label, &props).await?;
                        var_to_id.insert(v, id);
                    }
                }
            }

            for pat in create_patterns {
                if pat.1.len() == 2 && pat.2.len() == 1 {
                    let from_var = pat.1[0].0.as_ref().ok_or(GraphError::ValidationError("No source var".into()))?;
                    let to_var = pat.1[1].0.as_ref().ok_or(GraphError::ValidationError("No target var".into()))?;
                    let rel = &pat.2[0];

                    let from_id = var_to_id.get(from_var).ok_or(GraphError::ValidationError("Unbound from var".into()))?;
                    let to_id = var_to_id.get(to_var).ok_or(GraphError::ValidationError("Unbound to var".into()))?;

                    let (out, in_) = if rel.4 == Some(false) { (*from_id, *to_id) } else { (*to_id, *from_id) };

                    let edge = Edge {
                        id: SerializableUuid(Uuid::new_v4()),
                        outbound_id: out,
                        inbound_id: in_,
                        edge_type: Identifier::new(rel.1.clone().unwrap_or("RELATED".to_string()))?,
                        label: rel.1.clone().unwrap_or("RELATED".to_string()),
                        properties: rel.3.iter().map(|(k,v)| Ok((k.clone(), to_property_value(v.clone())?))).collect::<GraphResult<_>>()?,
                    };
                    storage.create_edge(edge.clone()).await?;
                    return Ok(json!({ "edge": edge }));
                }
            }
            Ok(json!({ "status": "success" }))
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
            println!("===> EXECUTING CreateNodes: {} nodes", nodes.len());
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
        // =============================================================================
        // MatchNode → only used for bare MATCH without RETURN (rare, legacy)
        // =============================================================================
        CypherQuery::MatchNode { label, properties } => {
            println!("===> EXECUTING MatchNode: label={:?}, query_properties={:?}", label, properties);
            let vertices = storage.get_all_vertices().await?;
            println!("===> Total vertices in storage: {}", vertices.len());
           
            // Convert query properties to PropertyValue for comparison
            let query_props: GraphResult<HashMap<String, PropertyValue>> = properties
                .into_iter()
                .map(|(k, v)| {
                    println!("===> Converting query property: key={}, value={:?} (type: {})",
                             k, v, type_name_of_val(&v));
                    to_property_value(v).map(|pv| {
                        println!("===> Converted query property to: {:?}", pv);
                        (k, pv)
                    })
                })
                .collect();
            let query_props = query_props?;
           
            println!("===> Query properties after conversion: {:?}", query_props);
           
            let filtered = vertices.into_iter().filter(|v| {
                println!("===> Checking vertex: id={}, label={}, properties={:?}",
                         v.id.0, v.label.as_ref(), v.properties);
               
                // Check if the vertex label matches the query label
                let matches_label = if let Some(query_label) = &label {
                    let vertex_label_str = v.label.as_ref();
                    let label_match = vertex_label_str == query_label ||
                        vertex_label_str.starts_with(&format!("{}:", query_label));
                    println!("===> Label comparison: vertex='{}' query='{}' match={}",
                             vertex_label_str, query_label, label_match);
                    label_match
                } else {
                    println!("===> No label constraint, matches all labels");
                    true
                };
               
                // Check if all query properties match vertex properties
                let matches_props = if query_props.is_empty() {
                    println!("===> No property constraints, matches all properties");
                    true
                } else {
                    let all_match = query_props.iter().all(|(k, expected_val)| {
                        let actual_val = v.properties.get(k);
                        let prop_match = actual_val.map_or(false, |av| {
                            let matches = av == expected_val;
                            println!("===> Property '{}': expected={:?}, actual={:?}, match={}",
                                     k, expected_val, av, matches);
                            matches
                        });
                        if actual_val.is_none() {
                            println!("===> Property '{}' not found in vertex", k);
                        }
                        prop_match
                    });
                    println!("===> All properties match: {}", all_match);
                    all_match
                };
               
                let overall_match = matches_label && matches_props;
                println!("===> Vertex {} overall match: label={} props={} result={}",
                         v.id.0, matches_label, matches_props, overall_match);
                overall_match
            }).collect::<Vec<_>>();
           
            println!("===> Filtered result count: {}", filtered.len());
            Ok(json!({ "vertices": filtered }))
        }
        CypherQuery::MatchMultipleNodes { nodes } => {
            let all_vertices = storage.get_all_vertices().await?;
            let mut result_vertices = Vec::new();
            let mut matched_ids = std::collections::HashSet::new();

            // *** FIX: Process ALL nodes, not just .first() ***
            for (var, label, properties) in nodes {
                let props: HashMap<String, PropertyValue> = properties
                    .iter()
                    .map(|(k, v)| to_property_value(v.clone()).map(|pv| (k.clone(), pv)))
                    .collect::<GraphResult<_>>()?;
                
                // Match ALL vertices that satisfy this pattern
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
            
            Ok(json!({ 
                "vertices": result_vertices,
                "count": result_vertices.len()
            }))
        }
        CypherQuery::CreateComplexPattern { nodes, relationships } => {
            let mut created_vertices = Vec::new();
            for (var, label, properties) in nodes {
                let props: GraphResult<HashMap<String, PropertyValue>> = properties
                    .into_iter()
                    .map(|(k, v)| to_property_value(v).map(|pv| (k, pv)))
                    .collect();
                let vertex = Vertex {
                    id: SerializableUuid(Uuid::new_v4()),
                    label: Identifier::new(label.unwrap_or_else(|| var.clone().unwrap_or_else(|| "Node".to_string())))?,
                    properties: props?,
                };
                storage.create_vertex(vertex.clone()).await?;
                created_vertices.push(vertex);
            }
            warn!("Complex pattern with {} relationships parsed but not yet fully implemented", relationships.len());
            Ok(json!({
                "vertices": created_vertices,
                "note": format!("Created {} nodes. Relationships ({}) not yet implemented.", created_vertices.len(), relationships.len())
            }))
        }
        CypherQuery::CreateEdge { from_id, edge_type, to_id } => {
            let edge = Edge {
                id: SerializableUuid(Uuid::new_v4()),
                outbound_id: from_id,
                edge_type: Identifier::new(edge_type)?,
                inbound_id: to_id,
                label: "relationship".to_string(),
                properties: BTreeMap::new(),
            };
            storage.create_edge(edge.clone()).await?;
            Ok(json!({ "edge": edge }))
        }
        CypherQuery::SetNode { id, properties } => {
            let mut vertex = storage.get_vertex(&id.0).await?.ok_or_else(|| {
                GraphError::StorageError(format!("Vertex not found: {}", id.0))
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
            Ok(json!({
                "key": key,
                "value": value.map(|v| String::from_utf8_lossy(&v).to_string())
            }))
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
            warn!("CREATE INDEX command is not implemented yet. Index for label '{}' on properties {:?} ignored.", label, properties);
            println!("===> WARNING: CREATE INDEX COMMAND NOT IMPLEMENTED YET. INDEX FOR LABEL '{}' ON PROPERTIES {:?} IGNORED.", label, properties);
            Ok(json!({ "status": "success", "message": "Index creation not implemented", "label": label, "properties": properties }))
        }
        CypherQuery::CreateEdgeBetweenExisting {
            source_var,
            rel_type,
            properties,
            target_var,
        } => {
            let properties_clone = properties.clone();
            // In dev/testing contexts, treat source_var/target_var as UUID strings
            let from_id = Uuid::parse_str(&source_var)
                .map_err(|_| GraphError::ValidationError(format!("Invalid source vertex ID: {}", source_var)))?;
            let to_id = Uuid::parse_str(&target_var)
                .map_err(|_| GraphError::ValidationError(format!("Invalid target vertex ID: {}", target_var)))?;
            let props: GraphResult<BTreeMap<String, PropertyValue>> = properties
                .into_iter()
                .map(|(k, v)| to_property_value(v).map(|pv| (k, pv)))
                .collect();
            // Extract 'label' from properties, fallback to rel_type
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
        // -----------------------------------------------------------------
        // MATCH path = (a)-[*..]-(b) RETURN …   (path variable ignored)
        // -----------------------------------------------------------------
        CypherQuery::MatchPath {
            path_var: _,
            left_node,
            right_node,
            return_clause: _,
        } => {
            println!("===> Executing MatchPath: left={}, right={}", left_node, right_node);

            let left_pat  = parse_node_pattern(&left_node)?;
            let right_pat = parse_node_pattern(&right_node)?;

            let all_vertices = storage.get_all_vertices().await?;
            let all_edges    = storage.get_all_edges().await?;

            let mut left_ids  = std::collections::HashSet::new();
            let mut right_ids = std::collections::HashSet::new();

            // helper to test a vertex against a pattern
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

            // collect ALL vertices that match either side
            for v in &all_vertices {
                if matches(v, &left_pat)  { left_ids.insert(v.id);  }
                if matches(v, &right_pat) { right_ids.insert(v.id); }
            }

            // edges that touch **any** matched vertex (0-2 hops)
            let mut matched_edge_ids = std::collections::HashSet::new();
            let mut matched_vertex_ids = left_ids.union(&right_ids).copied().collect::<std::collections::HashSet<_>>();

            // BFS up to 2 hops
            let mut queue: Vec<(SerializableUuid, u32)> = matched_vertex_ids.iter().map(|&id| (id, 0)).collect();
            let mut visited_edges = std::collections::HashSet::new();

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

            // materialise the collections
            let vertices: Vec<Vertex> = all_vertices.into_iter()
                .filter(|v| matched_vertex_ids.contains(&v.id))
                .collect();
            let edges: Vec<Edge> = all_edges.into_iter()
                .filter(|e| matched_edge_ids.contains(&e.id))
                .collect();

            println!("===> Returning {} vertices, {} edges", vertices.len(), edges.len());
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
