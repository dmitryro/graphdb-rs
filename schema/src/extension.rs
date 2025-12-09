// schema/src/extension.rs

use serde::{Deserialize, Serialize};
use crate::constraints::PropertyConstraint;
use crate::lifecycle::LifecycleRule;
use crate::errors::SchemaError;
use crate::definitions::{ GraphSchema };

/// Defines a patch that can be applied to an existing schema element.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaExtensionPatch {
    /// The target schema type (e.g., "Patient", "HAS_APPOINTMENT").
    pub target_schema: String,
    /// New properties to add.
    pub add_properties: Vec<PropertyConstraint>,
    /// Existing properties to modify (e.g., change from optional to required).
    pub modify_properties: Vec<PropertyConstraint>,
    /// New lifecycle rules or state transitions to add.
    pub add_lifecycle_rules: Vec<LifecycleRule>,
    /// Flag to allow or disallow elements of this type to be deleted.
    pub allow_deletion: Option<bool>,
}

/// Service responsible for applying and storing dynamic schema extensions.
pub struct ExtensionService {
    // In a real database, this would interact with a dedicated storage layer
    // to persist the extension definitions alongside the core schema.
    // This allows for modification without recompilation.
}

impl ExtensionService {
    /// Applies a patch to a schema. This would typically be called at runtime.
    pub fn apply_patch<T: GraphSchema>(&self, patch: SchemaExtensionPatch) -> Result<(), SchemaError> {
        if T::schema_name() != patch.target_schema {
            return Err(SchemaError::ExtensionError(format!("Patch target mismatch: expected {}, got {}", T::schema_name(), patch.target_schema)));
        }

        // Logic to apply changes... e.g.:
        // 1. Load the current schema definition for 'T'.
        // 2. Merge 'patch.add_properties' into the current properties.
        // 3. Overwrite/merge 'patch.modify_properties'.
        // 4. Save the new effective schema definition.
        
        println!("Successfully applied patch to schema: {}", patch.target_schema);
        Ok(())
    }
}
