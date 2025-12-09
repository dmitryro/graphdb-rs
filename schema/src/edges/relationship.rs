use crate::constraints::PropertyConstraint;
use crate::relationship_types::{MedicalRelationship, ALL_MEDICAL_RELATIONSHIPS};

/// A simplified trait for Edge/Relationship definitions, now replaced by the
/// declarative list in `relationship_types.rs`. This is kept for consistency
/// with the `VertexSchema` pattern, although for now, all definitions are static.
pub trait EdgeSchema {
    /// The unique label for the relationship (e.g., "HAS_APPOINTMENT").
    fn edge_label() -> &'static str;

    /// Defines the property constraints (optional) that must be met by the edge itself.
    fn property_constraints() -> Vec<PropertyConstraint> {
        // By default, edges defined in the static list have no properties.
        Vec::new()
    }
}

/// Utility struct to hold the relationship metadata and its property constraints.
#[derive(Debug, Clone)]
pub struct EdgeDefinition {
    /// The static metadata from the central list.
    pub metadata: MedicalRelationship,
    /// The property constraints specific to this edge type (if any).
    // Note: The property constraints must be explicitly defined for the edge type,
    // as they are not currently a field on the `MedicalRelationship` metadata struct.
    pub properties: Vec<PropertyConstraint>, 
}

/// Loads all relationship definitions from the static list in `relationship_types.rs`
/// and returns them as a vector of concrete `EdgeDefinition` structs.
pub fn load_all_relationship_definitions() -> Vec<EdgeDefinition> {
    ALL_MEDICAL_RELATIONSHIPS.iter()
        .map(|rel| {
            // Use the data from the static list to construct the full definition
            EdgeDefinition {
                metadata: rel.clone(), // Clone the static metadata
                // FIX: The compiler indicates `MedicalRelationship` (referred to here as `rel`)
                // does not have a field named `property_constraints`. 
                // We are initializing `properties` to an empty vector to fix the compile error.
                properties: Vec::new(), 
            }
        })
        .collect()
}
