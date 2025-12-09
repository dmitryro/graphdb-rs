use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::{self, Value};
use chrono::{DateTime, Utc};

/// Defines the set of allowed string values for a property, effectively
/// creating an internal enum constraint for the graph schema.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EnumValues {
    /// The list of allowed string values for the property.
    pub allowed_values: Vec<String>,
}

impl EnumValues {
    /// Creates a new EnumValues instance with the specified list of allowed values.
    pub fn new(allowed_values: Vec<String>) -> Self {
        EnumValues {
            allowed_values,
        }
    }
}

/// Defines the data types allowed for a property.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum DataType {
    String,
    Integer,
    UnsignedInteger,
    Float,
    Boolean,
    Timestamp,
    DateTime,
    Json,
    List(Box<DataType>),
    Relationship,
    Uuid,
}

/// Defines a specific structural constraint (e.g., uniqueness, required, mutability).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum Constraint {
    /// Must be present and non-null.
    Required,
    /// The property is not mandatory, allowing null or missing values.
    Optional,
    /// Must be unique across all elements of this type.
    Unique,
    /// The property cannot be modified after the vertex is created.
    Immutable,
    /// The property is allowed and expected to be modified after creation.
    Mutable,
    /// The property must be stored using encryption/hashing techniques (e.g., SSN, password hashes).
    Encrypted,

    /// The property contains sensitive data that needs restricted access and logging (e.g., raw password, PII).
    Sensitive,

    /// The property is used for writing data (e.g., password hash on creation) but is not readable via general query.
    WriteOnly, // FIX: Added missing variant

    /// Must be a valid email format.
    ValidateEmail,

    /// The property must be indexed by the database for efficient querying.
    Indexable,

    /// Minimum length for strings.
    MinLength(i64),

    /// Must match a generic regex pattern (e.g., for FHIR IDs, patient MRNs, or specific string formats).
    Format(String), // FIX: Renamed from Pattern(String) to Format(String)

    /// Minimum value for numbers (used for Integer and Decimal types).
    Min(i64),
    /// Maximum value for numbers (used for Integer and Decimal types).
    Max(i64),

    /// Time constraint: the DateTime property must represent a time in the future.
    FutureTime,

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
    /// Whether the property is required (true) or optional (false).
    pub required: bool,
    /// An optional human-readable description of the property's purpose.
    pub description: Option<String>,
    /// The data type this property must hold.
    pub data_type: DataType,
    /// The list of constraints that must be enforced.
    pub constraints: Vec<Constraint>,
    /// Default value if not provided on creation.
    pub default_value: Option<serde_json::Value>,
}

// --- Implementation of Constructors/Methods ---
impl PropertyConstraint {
    /// Creates a new PropertyConstraint instance with default type (String) and constraints.
    /// Automatically adds the Required constraint if `required` is true.
    ///
    /// # Arguments
    /// * `name` - The name of the property.
    /// * `required` - If the property is mandatory.
    pub fn new(name: &str, required: bool) -> Self {
        PropertyConstraint {
            name: name.to_string(), // Convert &str to owned String
            required,
            description: None,
            data_type: DataType::String, // Default to String
            // Initialize constraints, adding Required if necessary
            constraints: if required { vec![Constraint::Required] } else { vec![Constraint::Optional] },
            default_value: None,
        }
    }

    /// Adds a descriptive comment to the constraint and returns the modified instance.
    pub fn with_description(mut self, description: &str) -> Self {
        self.description = Some(description.to_string()); // Convert &str to owned String
        self
    }

    /// Sets the data type for the constraint.
    pub fn with_data_type(mut self, data_type: DataType) -> Self {
        self.data_type = data_type;
        self
    }

    /// Sets the list of constraints, replacing any existing ones (including the auto-added Required/Optional constraint).
    pub fn with_constraints(mut self, constraints: Vec<Constraint>) -> Self {
        self.constraints = constraints;
        // Ensure the required flag matches the presence of the constraint
        self.required = self.constraints.contains(&Constraint::Required);
        // If Required is not present, ensure Optional is present to match expected behavior
        if !self.required && !self.constraints.contains(&Constraint::Optional) {
            self.constraints.push(Constraint::Optional);
        }
        self
    }

    /// Sets the default value.
    pub fn with_default_value(mut self, value: serde_json::Value) -> Self {
        self.default_value = Some(value);
        self
    }

    /// Adds an Enum constraint using the helper struct, replacing any existing Enum constraints.
    pub fn with_enum_values(mut self, enum_values: EnumValues) -> Self {
        // 1. Filter out any existing Enum constraint
        self.constraints.retain(|c| !matches!(c, Constraint::Enum(_)));

        // 2. Add the new Enum constraint
        self.constraints.push(Constraint::Enum(enum_values.allowed_values));
        self
    }

    /// Validates a given value against all defined constraints.
    pub fn validate(&self, value: &Value) -> Result<(), String> {
        // 1. Check for Required constraint (using the dedicated bool field for simplicity)
        if self.required && value.is_null() {
            return Err(format!("Property '{}' is required but missing/null.", self.name));
        }

        if value.is_null() {
            return Ok(()); // Skip further checks if optional and null
        }

        // 2. Check Data Type
        if !self.check_data_type(value) {
            return Err(format!("Property '{}' failed type check for {:?}", self.name, self.data_type));
        }

        // 3. Check other Constraints (Format, Min/Max, Enum, etc.)
        for constraint in &self.constraints {
            match constraint {
                // These constraints are policy flags enforced at the database/storage layer
                Constraint::Unique | Constraint::Immutable | Constraint::Mutable | Constraint::Encrypted | Constraint::Optional | Constraint::Indexable | Constraint::Sensitive | Constraint::WriteOnly => { // FIX: Added WriteOnly
                    // No runtime validation needed for these flags; they govern persistence/access control.
                },
                Constraint::Required => {
                    // Handled by the dedicated `self.required` check at the start.
                }
                Constraint::MinLength(min_len) => {
                    if let Some(s) = value.as_str() {
                        if s.len() < *min_len as usize {
                            return Err(format!(
                                "Property '{}' string length ({}) is below the minimum allowed length of {}.",
                                self.name,
                                s.len(),
                                min_len
                            ));
                        }
                    }
                },
                Constraint::Format(pattern) => { // FIX: Changed from Pattern(pattern) to Format(pattern)
                    // Runtime check for string pattern
                    if let Some(s) = value.as_str() {
                        let re = match Regex::new(pattern) {
                            Ok(r) => r,
                            // Fatal schema definition error
                            Err(_) => return Err(format!("Internal Schema Error: Invalid regex pattern defined for property '{}'", self.name))
                        };

                        if !re.is_match(s) {
                            return Err(format!("Property '{}' does not match format pattern: {}", self.name, pattern));
                        }
                    }
                },
                Constraint::ValidateEmail => {
                    if let Some(s) = value.as_str() {
                        // Simple check for local@domain.tld structure
                        let re = match Regex::new(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$") {
                            Ok(r) => r,
                            Err(_) => return Err(format!("Internal Schema Error: Invalid email regex pattern defined for property '{}'", self.name))
                        };

                        if !re.is_match(s) {
                            return Err(format!("Property '{}' value '{}' is not a valid email format.", self.name, s));
                        }
                    }
                }
                Constraint::Min(min_val) => {
                    if let Some(num) = value.as_i64() {
                        if num < *min_val {
                            return Err(format!("Property '{}' value {} is below the minimum allowed value of {}", self.name, num, min_val));
                        }
                    }
                },
                Constraint::Max(max_val) => {
                    if let Some(num) = value.as_i64() {
                        if num > *max_val {
                            return Err(format!("Property '{}' value {} is above the maximum allowed value of {}", self.name, num, max_val));
                        }
                    }
                },
                Constraint::FutureTime => {
                    // Validation for DateTime types: must be in the future.
                    if let Some(s) = value.as_str() {
                        match s.parse::<DateTime<Utc>>() {
                            Ok(dt) => {
                                let now = Utc::now();
                                if dt <= now {
                                    return Err(format!(
                                        "Property '{}' time '{}' must be in the future (current time: {}).",
                                        self.name,
                                        dt.to_rfc3339(),
                                        now.to_rfc3339()
                                    ));
                                }
                            },
                            Err(_) => {
                                // If parsing fails, it's either not a string or not a valid ISO 8601 date.
                                return Err(format!(
                                    "Property '{}' failed to parse as valid DateTime (required for FutureTime constraint).",
                                    self.name
                                ));
                            }
                        }
                    }
                },
                Constraint::Enum(allowed_values) => {
                    if let Some(s) = value.as_str() {
                        if !allowed_values.contains(&s.to_string()) {
                            return Err(format!("Property '{}' value '{}' is not one of the allowed values: {:?}", self.name, s, allowed_values));
                        }
                    }
                }
                Constraint::CustomValidator(_) => {
                    // Custom validators are typically called by the upstream validation service
                }
            }
        }

        Ok(())
    }

    /// Helper to check if the value matches the defined DataType
    fn check_data_type(&self, value: &serde_json::Value) -> bool {
        match self.data_type {
            DataType::String => value.is_string(),
            DataType::Integer => value.is_i64(),
            DataType::UnsignedInteger => value.is_u64(), // Check if JSON number can be represented as u64
            DataType::Float => value.is_f64(),
            DataType::Boolean => value.is_boolean(),
            DataType::Timestamp => value.as_i64().is_some() || value.as_str().is_some(), // Allow epoch or string
            DataType::DateTime => value.as_i64().is_some() || value.as_str().is_some(), // Allow epoch or ISO8601 string
            DataType::Json => value.is_object() || value.is_array(),
            DataType::List(_) => value.is_array(),
            DataType::Relationship => value.is_string() || value.is_object(), // Expecting a ID or Vertex/Edge stub
            DataType::Uuid => value.is_string(), // Added: UUIDs are represented as JSON strings
        }
    }
}
