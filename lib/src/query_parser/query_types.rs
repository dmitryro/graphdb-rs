// src/query_parser/query_types.rs
// Complete — includes full WHERE evaluation, no external file needed

use serde_json::{json, Value};
use std::collections::{HashSet, HashMap};
use uuid::Uuid;
use serde::{Serialize, Deserialize};
use crate::config::QueryResult; 
use models::{Vertex, Edge};
use models::errors::{GraphError, GraphResult};
use models::properties::PropertyValue;
use models::identifiers::SerializableUuid;

/// (variable_name_opt, label_opt, properties_map)
pub type NodePattern = (Option<String>, Option<String>, HashMap<String, Value>);

/// (variable_name_opt, label_opt, length_range_opt, properties_map, direction_opt)
/// direction_opt: true for ->, false for <-, None for --
pub type RelPattern = (
    Option<String>,
    Option<String>,
    Option<(Option<u32>, Option<u32>)>,
    HashMap<String, Value>,
    Option<bool>,
);

/// Represents the possible outcomes of executing a Cypher query.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CypherResponse {
    /// Indicates successful completion of a mutation (CREATE, SET, DELETE, etc.)
    /// The inner Value typically holds a status object (e.g., { "message": "Success" }).
    Success(Value),

    /// Represents the structured data returned by a read query (MATCH ... RETURN).
    /// The inner Value is typically an array of result rows.
    ResultSet(Value),

    /// Returns a list of the created entities (Nodes/Edges).
    /// Used for CREATE statements where no explicit RETURN is specified.
    CreatedEntities(Value), 

    /// General error wrapper (often preferred to let the external function handle the error,
    /// but useful if you want to explicitly wrap errors that are not GraphError).
    Error(String),
}

/// An internal wrapper to parse and manipulate the opaque String data 
/// inside the QueryResult enum, treating it as a JSON array of result rows.
pub struct InternalQueryResult {
    pub data: Value, 
}

impl InternalQueryResult {
    /// Initializes an empty result set for the start of a CHAIN operation.
    fn empty() -> Self {
        InternalQueryResult { data: Value::Array(vec![]) }
    }
    
    /// Parses the opaque QueryResult::Success(String) into a manipulable JSON Value.
    fn from_query_result(qr: QueryResult) -> GraphResult<Self> {
        match qr {
            QueryResult::Success(s) => serde_json::from_str(&s)
                .map(|data| InternalQueryResult { data })
                .map_err(|e| GraphError::DeserializationError(
                    format!("Failed to parse query result string as JSON: {}", e)
                )),
            QueryResult::Null => Ok(Self::empty()),
        }
    }
    
    /// Serializes the JSON Value back into QueryResult::Success(String) or Null.
    fn to_query_result(self) -> GraphResult<QueryResult> {
        if self.data.is_array() && self.data.as_array().map_or(true, |a| a.is_empty()) {
            Ok(QueryResult::Null)
        } else {
            serde_json::to_string(&self.data)
                .map(QueryResult::Success)
                .map_err(|e| GraphError::SerializationError(
                    format!("Failed to serialize result back to string: {}", e)
                ))
        }
    }
    
    /// Checks for minimal compatibility (i.e., both results are JSON arrays).
    fn check_compatibility(&self, other: &Self) -> GraphResult<()> {
        if !self.data.is_array() || !other.data.is_array() {
            return Err(GraphError::QueryExecutionError(
                "UNION operands must produce structured, compatible data (JSON array expected).".to_string(),
            ));
        }
        // In a real database, detailed schema comparison (column names/types) would happen here.
        Ok(())
    }
    
    /// Logic for UNION ALL: combines results and retains duplicates.
    fn union_all(self, other: Self) -> Self {
        let mut combined_array = Vec::new();
        if let Some(arr) = self.data.as_array() { combined_array.extend_from_slice(arr); }
        if let Some(arr) = other.data.as_array() { combined_array.extend_from_slice(arr); }
        InternalQueryResult { data: Value::Array(combined_array) }
    }
    
    /// Logic for UNION (DISTINCT): combines results and removes duplicates.
    fn union_distinct(self, other: Self) -> Self {
        let mut distinct_set: HashSet<Value> = HashSet::new();
        let mut combined_array = Vec::new();

        let process_array = |data: Value, set: &mut HashSet<Value>, vec: &mut Vec<Value>| {
            if let Some(arr) = data.as_array() {
                for item in arr {
                    // Assumes the result rows (JSON objects) are hashable
                    if set.insert(item.clone()) {
                        vec.push(item.clone());
                    }
                }
            }
        };

        process_array(self.data, &mut distinct_set, &mut combined_array);
        process_array(other.data, &mut distinct_set, &mut combined_array);

        InternalQueryResult { data: Value::Array(combined_array) }
    }
}

/// (path_variable_name_opt, nodes_vec, relationships_vec)
/// Represents a complex path structure, likely used for MATCH.
pub type Pattern = (Option<String>, Vec<NodePattern>, Vec<RelPattern>);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MatchPattern {
    pub nodes: Vec<NodePattern>,
    pub relationships: Vec<RelPattern>,
}

// The complex type that parse_match_clause_patterns returns.
// This type is derived from the previous log and is used as the O (Output) type in IResult.
pub type PatternsReturnType = std::vec::Vec<(
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

#[derive(Debug, Clone, PartialEq)]
pub enum CypherValue {
    Null,
    Bool(bool),
    Integer(i64),
    Float(f64),
    String(String),
    Vertex(Vertex),
    Edge(Edge),
    List(Vec<CypherValue>),
    Map(HashMap<String, CypherValue>),
}

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
    CreateStatement {
        patterns: Vec<Pattern>,
        return_items: Vec<String>,
    },
    MatchPattern {
        patterns: Vec<Pattern>,
    },
    MatchSet {
        match_patterns: Vec<Pattern>,
        set_clauses: Vec<(String, String, Value)>,
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
    MatchPath {
        path_var: String,
        left_node: String,
        right_node: String,
        return_clause: String,
    },
    DeleteEdges {
        edge_variable: String,
        pattern: MatchPattern,
        where_clause: Option<WhereClause>,
    },
    DetachDeleteNodes {
        node_variable: String,
        label: Option<String>,
    },
    Merge {
        patterns: Vec<Pattern>,
        on_create_set: Vec<(String, String, Value)>,
        on_match_set: Vec<(String, String, Value)>,
    },
    ReturnStatement {
        projection_string: String,
        order_by: Option<String>, // Placeholder for ORDER BY expressions
        skip: Option<i64>,        // Parsed value for SKIP
        limit: Option<i64>,       // Parsed value for LIMIT
    },
    // NEW: Standalone SET clause for chaining
    SetStatement { 
        assignments: Vec<(String, String, Value)>, 
    },
    
    // NEW: Standalone DELETE/DETACH DELETE clause
    DeleteStatement { 
        variables: Vec<String>, // List of variable names to delete
        detach: bool,
    },
    
    // NEW: Standalone REMOVE clause
    RemoveStatement { 
        removals: Vec<(String, String)>, // e.g., ("n", "label") or ("n", "property")
    },
    Batch(Vec<CypherQuery>),
    Chain(Vec<CypherQuery>),
    Union(Box<CypherQuery>, bool, Box<CypherQuery>),
}

impl From<&Value> for CypherValue {
    fn from(v: &Value) -> Self {
        match v {
            Value::Null => CypherValue::Null,
            Value::Bool(b) => CypherValue::Bool(*b),
            Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    CypherValue::Integer(i)
                } else if let Some(f) = n.as_f64() {
                    CypherValue::Float(f)
                } else {
                    CypherValue::Null
                }
            }
            Value::String(s) => CypherValue::String(s.clone()),
            Value::Array(arr) => CypherValue::List(arr.iter().map(CypherValue::from).collect()),
            Value::Object(obj) => CypherValue::Map(
                obj.iter()
                    .map(|(k, v)| (k.clone(), CypherValue::from(v)))
                    .collect(),
            ),
        }
    }
}

impl From<PropertyValue> for CypherValue {
    fn from(pv: PropertyValue) -> Self {
        match pv {
            PropertyValue::String(s) => CypherValue::String(s),
            PropertyValue::Integer(i) => CypherValue::Integer(i),
            PropertyValue::Float(f) => CypherValue::Float(f.0),
            PropertyValue::Boolean(b) => CypherValue::Bool(b),
            PropertyValue::Uuid(u) => CypherValue::String(u.0.to_string()),
            PropertyValue::Byte(b) => CypherValue::Integer(b as i64),
            _ => CypherValue::Null,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum PropertyAccess {
    Vertex(String, String),
    Edge(String, String),
    Parameter(String),
}

#[derive(Debug, Clone, PartialEq)]
pub enum Expression {
    Literal(CypherValue),
    Property(PropertyAccess),
    Variable(String),
    Binary {
        op: BinaryOp,
        left: Box<Expression>,
        right: Box<Expression>,
    },
    Unary {
        op: UnaryOp,
        expr: Box<Expression>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum BinaryOp {
    Eq,
    Neq,
    Lt,
    Lte,
    Gt,
    Gte,
    And,
    Or,
    Xor,
    Plus,
    Minus,
    Mul,
    Div,
    Mod,
    In,
    Contains,
    StartsWith,
    EndsWith,
    Regex,
}

#[derive(Debug, Clone, PartialEq)]
pub enum UnaryOp {
    Not,
    Neg,
}

#[derive(Debug, Clone, PartialEq)]
pub struct WhereClause {
    pub condition: Expression,
}

#[derive(Debug, Clone)]
pub struct EvaluationContext {
    pub variables: HashMap<String, CypherValue>,
    pub parameters: HashMap<String, CypherValue>,
}

impl EvaluationContext {
    pub fn from_match(graph_match: &GraphMatch) -> Self {
        let mut variables = HashMap::new();
        for (var_name, vertex) in &graph_match.vertices {
            variables.insert(var_name.clone(), CypherValue::Vertex(vertex.clone()));
        }
        for (var_name, edge) in &graph_match.edges {
            variables.insert(var_name.clone(), CypherValue::Edge(edge.clone()));
        }
        Self {
            variables,
            parameters: HashMap::new(),
        }
    }

    pub fn with_parameters(mut self, params: HashMap<String, Value>) -> Self {
        self.parameters = params
            .into_iter()
            .map(|(k, v)| (k, CypherValue::from(&v)))
            .collect();
        self
    }

    pub fn get(&self, name: &str) -> Option<&CypherValue> {
        if name.starts_with('$') {
            self.parameters.get(&name[1..])
        } else {
            self.variables.get(name)
        }
    }
}

#[derive(Debug, Clone)]
pub struct GraphMatch {
    pub vertices: HashMap<String, Vertex>,
    pub edges: HashMap<String, Edge>,
}

impl GraphMatch {
    pub fn new() -> Self {
        Self {
            vertices: HashMap::new(),
            edges: HashMap::new(),
        }
    }
}

/// Represents the final result of a write operation (CREATE, MERGE, SET, DELETE).
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct ExecutionResult {
    pub created_nodes: HashSet<Uuid>,
    pub updated_nodes: HashSet<Uuid>,
    pub deleted_nodes: HashSet<Uuid>,
    pub created_edges: HashSet<Uuid>,
    pub updated_edges: HashSet<Uuid>,
    pub deleted_edges: HashSet<Uuid>,
    pub updated_kv_keys: HashSet<String>,
}

impl ExecutionResult {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_created_node(&mut self, id: Uuid) {
        self.created_nodes.insert(id);
    }

    pub fn add_updated_node(&mut self, id: Uuid) {
        self.updated_nodes.insert(id);
    }

    pub fn add_deleted_node(&mut self, id: Uuid) {
        self.deleted_nodes.insert(id);
    }

    pub fn add_created_edge(&mut self, id: Uuid) {
        self.created_edges.insert(id);
    }

    pub fn add_updated_edge(&mut self, id: Uuid) {
        self.updated_edges.insert(id);
    }

    pub fn add_deleted_edge(&mut self, id: Uuid) {
        self.deleted_edges.insert(id);
    }

    pub fn add_updated_kv_key(&mut self, key: String) {
        self.updated_kv_keys.insert(key);
    }

    pub fn has_mutations(&self) -> bool {
        !(self.created_nodes.is_empty()
            && self.updated_nodes.is_empty()
            && self.deleted_nodes.is_empty()
            && self.created_edges.is_empty()
            && self.updated_edges.is_empty()
            && self.deleted_edges.is_empty()
            && self.updated_kv_keys.is_empty())
    }

    pub fn created_count(&self) -> usize {
        self.created_nodes.len() + self.created_edges.len()
    }

    pub fn updated_count(&self) -> usize {
        self.updated_nodes.len() + self.updated_edges.len()
    }

    pub fn extend(&mut self, other: ExecutionResult) {
        self.created_nodes.extend(other.created_nodes);
        self.updated_nodes.extend(other.updated_nodes);
        self.deleted_nodes.extend(other.deleted_nodes);
        self.created_edges.extend(other.created_edges);
        self.updated_edges.extend(other.updated_edges);
        self.deleted_edges.extend(other.deleted_edges);
        self.updated_kv_keys.extend(other.updated_kv_keys);
    }
}

/// Represents a single change operation that needs to be persisted to the database.
#[derive(Debug, Clone, PartialEq)]
pub enum Mutation {
    CreateNode(Uuid),
    UpdateNode(Uuid),
    DeleteNode(Uuid),
    CreateEdge(Uuid),
    UpdateEdge(Uuid),
    DeleteEdge(Uuid),
    SetKeyValue(String),
    DeleteKeyValue(String),
    CreateIndex(String),
    DropIndex(String),
}

impl WhereClause {
    pub fn evaluate(&self, ctx: &EvaluationContext) -> GraphResult<bool> {
        let result = self.condition.evaluate(ctx)?;
        match result {
            CypherValue::Bool(b) => Ok(b),
            CypherValue::Null => Ok(false),
            _ => Err(GraphError::EvaluationError(
                "WHERE clause must evaluate to a boolean value".into(),
            )),
        }
    }
}

impl Expression {
    pub fn evaluate(&self, ctx: &EvaluationContext) -> GraphResult<CypherValue> {
        match self {
            Expression::Literal(val) => Ok(val.clone()),
            Expression::Variable(name) => ctx
                .variables
                .get(name)
                .cloned()
                .ok_or_else(|| GraphError::EvaluationError(format!("Variable '{name}' not found"))),
            Expression::Property(access) => match access {
                PropertyAccess::Vertex(var, prop) => {
                    if let Some(CypherValue::Vertex(v)) = ctx.variables.get(var) {
                        Ok(v.properties
                            .get(prop)
                            .map(property_value_to_cypher)
                            .unwrap_or(CypherValue::Null))
                    } else {
                        Err(GraphError::EvaluationError(format!("Vertex variable '{var}' not found")))
                    }
                }
                PropertyAccess::Edge(var, prop) => {
                    if let Some(CypherValue::Edge(e)) = ctx.variables.get(var) {
                        Ok(e.properties
                            .get(prop)
                            .map(property_value_to_cypher)
                            .unwrap_or(CypherValue::Null))
                    } else {
                        Err(GraphError::EvaluationError(format!("Edge variable '{var}' not found")))
                    }
                }
                PropertyAccess::Parameter(name) => ctx
                    .parameters
                    .get(name)
                    .cloned()
                    .ok_or_else(|| GraphError::EvaluationError(format!("Parameter '${name}' not provided"))),
            },
            Expression::Binary { op, left, right } => {
                let l = left.evaluate(ctx)?;
                let r = right.evaluate(ctx)?;
                op.apply(&l, &r)
            }
            Expression::Unary { op, expr } => op.apply(&expr.evaluate(ctx)?),
        }
    }
}

impl BinaryOp {
    pub fn apply(&self, left: &CypherValue, right: &CypherValue) -> GraphResult<CypherValue> {
        match self {
            BinaryOp::Eq => Ok(CypherValue::Bool(left == right)),
            BinaryOp::Neq => Ok(CypherValue::Bool(left != right)),
            BinaryOp::Lt => compare(left, right, |a, b| a < b),
            BinaryOp::Lte => compare(left, right, |a, b| a <= b),
            BinaryOp::Gt => compare(left, right, |a, b| a > b),
            BinaryOp::Gte => compare(left, right, |a, b| a >= b),
            BinaryOp::And => Ok(CypherValue::Bool(to_bool(left)? && to_bool(right)?)),
            BinaryOp::Or => Ok(CypherValue::Bool(to_bool(left)? || to_bool(right)?)),
            BinaryOp::Plus => add(left, right),
            BinaryOp::Minus => subtract(left, right),
            BinaryOp::Mul => multiply(left, right),
            BinaryOp::Div => divide(left, right),
            _ => Err(GraphError::EvaluationError(format!("Operator {self:?} not supported"))),
        }
    }
}

impl UnaryOp {
    pub fn apply(&self, val: &CypherValue) -> GraphResult<CypherValue> {
        match self {
            UnaryOp::Not => Ok(CypherValue::Bool(!to_bool(val)?)),
            UnaryOp::Neg => match val {
                CypherValue::Integer(i) => Ok(CypherValue::Integer(-i)),
                CypherValue::Float(f) => Ok(CypherValue::Float(-f)),
                _ => Err(GraphError::EvaluationError("Cannot negate non-numeric value".into())),
            },
        }
    }
}

fn to_bool(v: &CypherValue) -> GraphResult<bool> {
    match v {
        CypherValue::Bool(b) => Ok(*b),
        CypherValue::Null => Ok(false),
        _ => Err(GraphError::EvaluationError("Expected boolean".into())),
    }
}

fn compare<F>(a: &CypherValue, b: &CypherValue, op: F) -> GraphResult<CypherValue>
where
    F: Fn(f64, f64) -> bool,
{
    let a_val = to_f64(a)?;
    let b_val = to_f64(b)?;
    Ok(CypherValue::Bool(op(a_val, b_val)))
}

fn to_f64(v: &CypherValue) -> GraphResult<f64> {
    match v {
        CypherValue::Integer(i) => Ok(*i as f64),
        CypherValue::Float(f) => Ok(*f),
        CypherValue::String(s) => s.parse::<f64>().map_err(|_| {
            GraphError::EvaluationError("Cannot convert string to number".into())
        }),
        _ => Err(GraphError::EvaluationError("Cannot convert to number".into())),
    }
}

fn add(a: &CypherValue, b: &CypherValue) -> GraphResult<CypherValue> {
    match (a, b) {
        (CypherValue::Integer(x), CypherValue::Integer(y)) => Ok(CypherValue::Integer(x + y)),
        (CypherValue::Float(x), CypherValue::Float(y)) => Ok(CypherValue::Float(x + y)),
        (CypherValue::Integer(x), CypherValue::Float(y)) => Ok(CypherValue::Float(*x as f64 + y)),
        (CypherValue::Float(x), CypherValue::Integer(y)) => Ok(CypherValue::Float(x + *y as f64)),
        (CypherValue::String(x), CypherValue::String(y)) => Ok(CypherValue::String(format!("{x}{y}"))),
        _ => Err(GraphError::EvaluationError("Unsupported operands for +".into())),
    }
}

fn subtract(a: &CypherValue, b: &CypherValue) -> GraphResult<CypherValue> {
    Ok(CypherValue::Float(to_f64(a)? - to_f64(b)?))
}

fn multiply(a: &CypherValue, b: &CypherValue) -> GraphResult<CypherValue> {
    Ok(CypherValue::Float(to_f64(a)? * to_f64(b)?))
}

fn divide(a: &CypherValue, b: &CypherValue) -> GraphResult<CypherValue> {
    let divisor = to_f64(b)?;
    if divisor == 0.0 {
        Err(GraphError::EvaluationError("Division by zero".into()))
    } else {
        Ok(CypherValue::Float(to_f64(a)? / divisor))
    }
}

fn property_value_to_cypher(pv: &PropertyValue) -> CypherValue {
    match pv {
        PropertyValue::String(s) => CypherValue::String(s.clone()),
        PropertyValue::Integer(i) => CypherValue::Integer(*i),
        PropertyValue::Float(f) => CypherValue::Float(f.0),
        PropertyValue::Boolean(b) => CypherValue::Bool(*b),
        PropertyValue::Uuid(u) => CypherValue::String(u.0.to_string()),
        PropertyValue::Byte(b) => CypherValue::Integer(*b as i64),
        _ => CypherValue::Null,
    }
}
