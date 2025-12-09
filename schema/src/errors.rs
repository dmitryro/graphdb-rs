use thiserror::Error;

/// Error type for schema validation and operations.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum SchemaError {
    /// The specified schema element name (e.g., Vertex, Edge) is invalid or not found.
    #[error("Invalid schema element name.")]
    InvalidSchemaName,

    /// A required constraint was violated during a schema operation (e.g., property validation failed).
    #[error("Constraint violation: {0}")]
    ConstraintViolation(String),

    /// An error related to modifying or enforcing lifecycle rules.
    #[error("Lifecycle rule error: {0}")]
    LifecycleRuleError(String),

    /// An error specific to schema extensions or patching.
    #[error("Schema extension error: {0}")]
    ExtensionError(String),
}
