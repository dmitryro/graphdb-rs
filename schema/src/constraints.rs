// schema/src/constraints.rs
use serde::{Deserialize, Serialize};

/// Defines the data types allowed for a property.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum DataType {
    String,
    Integer,
    Float,
    Boolean,
    Timestamp,
    Json,
    List(Box<DataType>), // For Array types
    Relationship, // Used specifically for enforcing edge types on a vertex property
}

/// Defines a specific structural constraint (e.g., uniqueness, required).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum Constraint {
    /// Must be present and non-null.
    Required,
    /// Must be unique across all elements of this type.
    Unique,
    /// Must match a regex pattern (e.g., for FHIR IDs, patient MRNs).
    Pattern(String),
    /// Minimum length for strings, or minimum value for numbers.
    /// For decimals, multiply by 100 and store as integer (e.g., 10.5 -> 1050)
    Min(i64),
    /// Maximum length for strings, or maximum value for numbers.
    /// For decimals, multiply by 100 and store as integer (e.g., 10.5 -> 1050)
    Max(i64),
    /// Only allows values from a predefined set.
    Enum(Vec<String>),
    /// A custom validation function name defined elsewhere (e.g., "validate_medical_record_number").
    CustomValidator(String),
}

/// The core definition for a property on a Vertex or Edge.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PropertyConstraint {
    /// The name of the property (e.g., "first_name", "date_of_birth").
    pub name: String,
    /// The data type this property must hold.
    pub data_type: DataType,
    /// The list of constraints that must be enforced.
    pub constraints: Vec<Constraint>,
    /// Default value if not provided on creation.
    pub default_value: Option<serde_json::Value>,
}

impl PropertyConstraint {
    /// Validates a given value against all defined constraints.
    pub fn validate(&self, value: &serde_json::Value) -> Result<(), String> {
        // 1. Check for Required constraint
        let is_required = self.constraints.contains(&Constraint::Required);
        if is_required && value.is_null() {
            return Err(format!("Property '{}' is required but missing/null.", self.name));
        }
        
        if value.is_null() {
            return Ok(()); // Skip further checks if optional and null
        }
        
        // 2. Check Data Type
        if !self.check_data_type(value) {
            return Err(format!("Property '{}' failed type check for {:?}", self.name, self.data_type));
        }
        
        // 3. Check other Constraints (Pattern, Min/Max, Enum, etc.)
        for constraint in &self.constraints {
            match constraint {
                Constraint::Unique => {
                    // This check must be done at the database service layer (runtime enforcement)
                    // but the constraint must be declared here.
                },
                Constraint::Pattern(pattern) => {
                    // Runtime check for string pattern
                    if let Some(s) = value.as_str() {
                        if !regex::Regex::new(pattern).unwrap().is_match(s) {
                            return Err(format!("Property '{}' does not match pattern: {}", self.name, pattern));
                        }
                    }
                },
                // ... implementation for Min, Max, Enum, CustomValidator
                _ => {}
            }
        }
        
        Ok(())
    }
    
    // Helper to check if the value matches the defined DataType
    fn check_data_type(&self, value: &serde_json::Value) -> bool {
        match self.data_type {
            DataType::String => value.is_string(),
            DataType::Integer => value.is_i64(),
            DataType::Float => value.is_f64(),
            DataType::Boolean => value.is_boolean(),
            DataType::Timestamp => value.as_i64().is_some() || value.as_str().is_some(), // Allow epoch or string
            DataType::Json => value.is_object() || value.is_array(),
            DataType::List(_) => value.is_array(),
            DataType::Relationship => value.is_string() || value.is_object(), // Expecting a ID or Vertex/Edge stub
        }
    }
}
