// lib/src/engine/pattern_match.rs
use serde_json::Value;
use std::collections::HashMap;
use models::vertices::Vertex;
use models::edges::Edge; // Corrected import for Edge from models
use models::identifiers::Identifier;
use models::properties::{ PropertyValue, SerializableFloat }; 

pub enum Pattern {
    VertexType(String),
    EdgeType(String),
    PropertyEquals(String, String), // (property_name, property_value_as_string)
    And(Box<Pattern>, Box<Pattern>),
    Or(Box<Pattern>, Box<Pattern>),
    Not(Box<Pattern>),
}

impl Pattern {
    pub fn matches_vertex(&self, vertex: &Vertex) -> bool {
        match self {
            Pattern::VertexType(t) => vertex.label.to_string() == *t,
            Pattern::PropertyEquals(k, v) => {
                vertex.properties.get(k).map_or(false, |prop_val| {
                    match prop_val {
                        PropertyValue::String(s) => s == v,
                        _ => false,
                    }
                })
            }
            Pattern::And(left, right) => {
                left.matches_vertex(vertex) && right.matches_vertex(vertex)
            }
            Pattern::Or(left, right) => {
                left.matches_vertex(vertex) || right.matches_vertex(vertex)
            }
            Pattern::Not(inner) => !inner.matches_vertex(vertex),
            // FIX: Add a match arm for Pattern::EdgeType.
            // An EdgeType pattern cannot apply to a Vertex, so it always returns false.
            Pattern::EdgeType(_) => false,
        }
    }

    pub fn matches_edge(&self, edge: &Edge) -> bool {
        match self {
            Pattern::EdgeType(t) => edge.edge_type.to_string() == *t,
            // WARNING: The models::Edge struct does NOT have a 'properties' field.
            // This 'PropertyEquals' pattern cannot be matched against an Edge.
            // It will always return false here.
            Pattern::PropertyEquals(_k, _v) => {
                false // Edges currently do not have properties
            }
            Pattern::And(left, right) => {
                left.matches_edge(edge) && right.matches_edge(edge)
            }
            Pattern::Or(left, right) => {
                left.matches_edge(edge) || right.matches_edge(edge)
            }
            Pattern::Not(inner) => !inner.matches_edge(edge),
            // FIX: Add a match arm for Pattern::VertexType.
            // A VertexType pattern cannot apply to an Edge, so it always returns false.
            Pattern::VertexType(_) => false,
        }
    }
}

// Helper function to match a Vertex against a NodePattern (label, properties)
// Note: This assumes NodePattern is (Option<String> var, Option<String> label, HashMap<String, Value> properties)
pub fn node_matches_constraints(
    vertex: &Vertex,
    label_opt: &Option<String>,
    properties: &HashMap<String, Value>,
) -> bool {
    // 1. Check Label Match
    let label_matches = label_opt.as_ref().map_or(true, |l| {
        vertex.label.as_ref() == l.as_str()
    });
    
    if !label_matches { return false; }

    // 2. Check Property Match
    properties.iter().all(|(k, v)| {
        vertex.properties.get(k).map_or(false, |prop_val| {
            // Compare the JSON Value from the query against the PropertyValue from the Vertex
            match (v, prop_val) {
                // Handle String comparison (e.g., id: "12345")
                (Value::String(val_str), PropertyValue::String(prop_str)) => val_str == prop_str,
                // Handle Number comparison (assuming float/double)
                (Value::Number(val_num), PropertyValue::Float(SerializableFloat(prop_f))) => {
                    val_num.as_f64().map_or(false, |val_f| val_f == *prop_f)
                },
                // Add Integer/Boolean handling here if necessary
                _ => false,
            }
        })
    })
}
