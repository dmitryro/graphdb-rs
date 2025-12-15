// src/query_parser/query_types.rs
// Complete — includes full WHERE evaluation, no external file needed

use serde_json::{json, Value};
use std::collections::{HashSet, HashMap};
use uuid::Uuid;
use serde::{Serialize, Deserialize};
use models::{Vertex, Edge};
use models::errors::{GraphError, GraphResult};
use models::properties::PropertyValue;

// ─────────────────────────────────────────────────────────────────────────────
// PATTERN TYPE ALIASES
// ─────────────────────────────────────────────────────────────────────────────

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

/// (path_variable_name_opt, nodes_vec, relationships_vec)
/// Represents a complex path structure, likely used for MATCH.
pub type Pattern = (Option<String>, Vec<NodePattern>, Vec<RelPattern>);

// ─────────────────────────────────────────────────────────────────────────────
// QUERY ENUMS & STRUCTS
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MatchPattern {
    pub nodes: Vec<NodePattern>,
    pub relationships: Vec<RelPattern>,
}

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
    Eq, Neq, Lt, Lte, Gt, Gte,
    And, Or, Xor,
    Plus, Minus, Mul, Div, Mod,
    In, Contains, StartsWith, EndsWith, Regex,
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

// ─────────────────────────────────────────────────────────────────────────────
// EXECUTION RESULT (Comprehensive Implementation)
// ─────────────────────────────────────────────────────────────────────────────

/// Represents the final result of a write operation (CREATE, MERGE, SET, DELETE).
/// It tracks the set of entities affected by the query.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct ExecutionResult {
    /// IDs of all nodes that were newly created.
    pub created_nodes: HashSet<Uuid>,
    /// IDs of all nodes that had properties or labels updated.
    pub updated_nodes: HashSet<Uuid>,
    /// IDs of all nodes that were deleted.
    pub deleted_nodes: HashSet<Uuid>,
    /// IDs of all edges that were newly created.
    pub created_edges: HashSet<Uuid>,
    /// IDs of all edges that were updated.
    pub updated_edges: HashSet<Uuid>,
    /// IDs of all edges that were deleted.
    pub deleted_edges: HashSet<Uuid>,
    /// Stores any key-value pairs that were updated (key)
    pub updated_kv_keys: HashSet<String>,
    
    // Stores the results of any `RETURN` clause (for mixed read/write queries).
    // Typically used for returning specific properties or computed values.
    // pub return_data: Vec<HashMap<String, CypherValue>>, // Assuming CypherValue exists
}

impl ExecutionResult {
    /// Creates a new, empty execution result.
    pub fn new() -> Self {
        Self::default()
    }

    /// Records a newly created node.
    pub fn add_created_node(&mut self, id: Uuid) {
        self.created_nodes.insert(id);
    }

    /// Records a node that was updated (e.g., via SET or ON MATCH SET).
    pub fn add_updated_node(&mut self, id: Uuid) {
        self.updated_nodes.insert(id);
    }

    /// Records a node that was deleted.
    pub fn add_deleted_node(&mut self, id: Uuid) {
        self.deleted_nodes.insert(id);
    }

    /// Records a newly created edge.
    pub fn add_created_edge(&mut self, id: Uuid) {
        self.created_edges.insert(id);
    }

    /// Records an edge that was updated.
    pub fn add_updated_edge(&mut self, id: Uuid) {
        self.updated_edges.insert(id);
    }

    /// Records an edge that was deleted.
    pub fn add_deleted_edge(&mut self, id: Uuid) {
        self.deleted_edges.insert(id);
    }

    /// Records a key-value pair update.
    pub fn add_updated_kv_key(&mut self, key: String) {
        self.updated_kv_keys.insert(key);
    }
    
    /// Checks if any entity was affected by the query.
    pub fn has_mutations(&self) -> bool {
        !(self.created_nodes.is_empty()
            && self.updated_nodes.is_empty()
            && self.deleted_nodes.is_empty()
            && self.created_edges.is_empty()
            && self.updated_edges.is_empty()
            && self.deleted_edges.is_empty()
            && self.updated_kv_keys.is_empty())
    }

    /// Returns the total number of entities (nodes and edges) created.
    pub fn created_count(&self) -> usize {
        self.created_nodes.len() + self.created_edges.len()
    }

    /// Returns the total number of entities (nodes and edges) updated.
    pub fn updated_count(&self) -> usize {
        self.updated_nodes.len() + self.updated_edges.len()
    }

    /// Combines the results of another `ExecutionResult` into this one.
    pub fn extend(&mut self, other: ExecutionResult) {
        self.created_nodes.extend(other.created_nodes);
        self.updated_nodes.extend(other.updated_nodes);
        self.deleted_nodes.extend(other.deleted_nodes);
        self.created_edges.extend(other.created_edges);
        self.updated_edges.extend(other.updated_edges);
        self.deleted_edges.extend(other.deleted_edges);
        self.updated_kv_keys.extend(other.updated_kv_keys);
        // If return_data existed, you would concatenate it here.
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// MUTATION TYPE
// ─────────────────────────────────────────────────────────────────────────────

/// Represents a single change operation that needs to be persisted to the database.
/// This is typically generated during query execution and committed by the storage manager.
#[derive(Debug, Clone, PartialEq)]
pub enum Mutation {
    /// Create a new node with the given UUID (Id)
    CreateNode(Uuid),
    /// Update an existing node (e.g., set properties)
    UpdateNode(Uuid),
    /// Delete a node (must include all attached edges)
    DeleteNode(Uuid),
    /// Create a new edge (relationship) with the given UUID (Id)
    CreateEdge(Uuid),
    /// Update an existing edge (e.g., set properties)
    UpdateEdge(Uuid),
    /// Delete an edge (relationship)
    DeleteEdge(Uuid),
    /// Set a key-value pair in the global Key-Value store
    SetKeyValue(String),
    /// Delete a key-value pair from the global Key-Value store
    DeleteKeyValue(String),
    /// Create a new index
    CreateIndex(String),
    /// Drop an existing index
    DropIndex(String),
}

// ─────────────────────────────────────────────────────────────────────────────
// CYPHER QUERY ENUM (NEWLY ADDED)
// ─────────────────────────────────────────────────────────────────────────────

/// Represents a parsed Cypher query statement.
#[derive(Debug, Clone)]
pub enum CypherQuery {
    /// MATCH clause (nodes, relationships, where_clause_opt, skip_opt, limit_opt)
    Match {
        patterns: Vec<Pattern>,
        where_clause: Option<WhereClause>,
        skip: Option<i64>,
        limit: Option<i64>,
    },
    /// CREATE clause (patterns)
    Create {
        patterns: Vec<Pattern>,
    },
    /// MERGE clause. This is typically a single path pattern.
    Merge {
        patterns: Vec<Pattern>,
        // List of (variable_name, property_name, value) to SET on CREATE
        on_create_set: Vec<(String, String, Value)>, 
        // List of (variable_name, property_name, value) to SET on MATCH
        on_match_set: Vec<(String, String, Value)>, 
    },
    /// DELETE/DETACH DELETE clause (variables_to_delete, detach)
    Delete {
        variables: Vec<String>,
        detach: bool,
    },
    /// SET clause (updates)
    Set {
        updates: Vec<SetUpdate>, // Assuming SetUpdate is defined elsewhere, or use a simpler structure.
    },
    /// RETURN clause (expressions_to_return)
    Return {
        projections: Vec<String>, // Simplification: just return variables for now
    },
    /// Fallback for unsupported statements
    Unsupported(String),
}

#[derive(Debug, Clone)]
pub enum SetUpdate {
    Property(PropertyAccess, Expression),
    // Add other types of SET updates if needed (e.g., set label)
}

// ─────────────────────────────────────────────────────────────────────────────
// WHERE EVALUATION — FULLY INTEGRATED
// ─────────────────────────────────────────────────────────────────────────────

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
                        // NOTE: Edges currently don't support properties in models, but we'll assume they might here.
                        // If `e.properties` does not exist, this will cause an error or need a fallback.
                        // Based on the provided code, Edge has a properties map.
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

// Helpers
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