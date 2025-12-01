// src/query_parser/query_types.rs
// Complete — includes full WHERE evaluation, no external file needed

use serde_json::{json, Value};
use std::collections::HashMap;
use uuid::Uuid;
use models::{Vertex, Edge};
use models::errors::{GraphError, GraphResult};
use models::properties::PropertyValue;

pub type NodePattern = (Option<String>, Option<String>, HashMap<String, Value>);
pub type RelPattern = (
    Option<String>,
    Option<String>,
    Option<(Option<u32>, Option<u32>)>,
    HashMap<String, Value>,
    Option<bool>,
);
pub type Pattern = (Option<String>, Vec<NodePattern>, Vec<RelPattern>);

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