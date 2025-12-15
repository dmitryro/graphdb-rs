use std::convert::{TryFrom, TryInto};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

// Correct Imports based on your project structure:
use crate::vertices::Vertex; 
use crate::properties::PropertyValue; 
use crate::errors::GraphError; 
use crate::identifiers::Identifier; 

/// The GoldenRecord represents the canonical, master patient record derived 
/// from one or more contributing source Patient records.
#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct GoldenRecord {
    /// The unique identifier of the GoldenRecord itself (e.g., GOLDEN-xxxx). 
    pub id: String, 
    
    /// The actual Vertex UUID of the GoldenRecord node in the graph.
    /// Renamed from `patient_vertex_uuid` to reflect its use in Into<Vertex> and TryFrom<Vertex>.
    #[serde(skip)] // Skip during JSON serialization, handled by Vertex::new
    pub gr_vertex_uuid: Uuid, 

    /// The UUID of the first source Patient Vertex associated with this GoldenRecord.
    /// This property MUST be explicitly written to the Vertex properties.
    pub target_patient_vertex_uuid: Uuid,

    /// Timestamp when this GoldenRecord was created. (MANDATORY)
    pub created_at: String,

    /// Timestamp when this GoldenRecord was last updated. (OPTIONAL)
    pub updated_at: Option<String>, 
    
    /// The MRN of the patient who contributed to this GoldenRecord (OPTIONAL)
    pub canonical_mrn: Option<String>,
}

// =====================================================================
// IMPLEMENTATION BLOCK
// =====================================================================

impl GoldenRecord {
    /// Creates a new GoldenRecord instance.
    /// 
    /// # Arguments
    /// * `id` - The unique, canonical ID for this record (e.g., "GOLDEN-12345").
    /// * `gr_vertex_uuid` - The UUID that the GR Vertex itself will use.
    /// * `target_patient_vertex_uuid` - The UUID of the source Patient vertex being linked.
    /// * `created_at` - The creation timestamp (e.g., ISO 8601 string).
    pub fn new(id: String, gr_vertex_uuid: Uuid, target_patient_vertex_uuid: Uuid, created_at: String) -> Self {
        GoldenRecord {
            id,
            gr_vertex_uuid,
            target_patient_vertex_uuid, // New field
            created_at,
            updated_at: None, // Initial update time is None
            canonical_mrn: None,
        }
    }

    /// Updates the record with a new MRN (if present) and updates the timestamp.
    pub fn update_metadata(&mut self, new_mrn: Option<String>, updated_at: String) {
        self.canonical_mrn = new_mrn;
        self.updated_at = Some(updated_at);
    }
}


// =====================================================================
// TRAIT IMPLEMENTATIONS (CONVERSIONS)
// =====================================================================

// Conversion from GoldenRecord struct into a graph Vertex (Serialization)
impl Into<Vertex> for GoldenRecord {
    fn into(self) -> Vertex {
        // FIX: Use the Golden Record's own UUID for the Vertex ID.
        let mut v = Vertex::new_with_label(self.gr_vertex_uuid, "GoldenRecord".into());

        // Required fields
        v.add_property("id", &self.id);
        v.add_property("created_at", &self.created_at);
        
        // NEW: Store the source Patient's UUID as a property
        v.add_property("target_patient_vertex_uuid", self.target_patient_vertex_uuid.to_string().as_str());
        
        // Optional fields (only write if Some)
        if let Some(ref val) = self.updated_at {
            v.add_property("updated_at", val.as_str());
        }

        if let Some(ref val) = self.canonical_mrn {
            v.add_property("canonical_mrn", val.as_str());
        }

        v
    }
}


// Conversion from graph Vertex into a GoldenRecord struct (Deserialization)
impl TryFrom<Vertex> for GoldenRecord {
    type Error = GraphError;

    fn try_from(vertex: Vertex) -> Result<Self, Self::Error> {
        // 1. Validate the Label
        if vertex.label != "GoldenRecord".into() {
            return Err(GraphError::VertexError(format!(
                "Expected label 'GoldenRecord', found '{}'", vertex.label
            )));
        }

        // 2. Helper for required String properties (Improved error message)
        let get_required_string = |key: &str| -> Result<String, GraphError> {
            vertex.properties.get(key)
                // Check if the property exists and if its value is a String.
                .and_then(|v| v.as_str()) // Assuming PropertyValue::as_str() exists and returns Option<&str>
                .map(|s| s.to_string())
                .ok_or_else(|| {
                    // This error is the one you are seeing, indicating the key isn't there, 
                    // or the type is wrong. We'll be specific.
                    let prop_exists = vertex.properties.contains_key(key);
                    
                    if !prop_exists {
                        GraphError::VertexError(format!(
                            "Missing required property '{}' on GoldenRecord vertex", key
                        ))
                    } else {
                         // The key exists, but the type check failed (as_str returned None)
                         GraphError::VertexError(format!(
                            "Required property '{}' has wrong type on GoldenRecord vertex", key
                         ))
                    }
                })
        };
        
        // NEW: Helper for required UUID property
        let get_required_uuid = |key: &str| -> Result<Uuid, GraphError> {
             get_required_string(key)?
                .parse::<Uuid>()
                .map_err(|e| GraphError::VertexError(format!("Failed to parse UUID for '{}': {}", key, e)))
        };


        // 3. Helper for optional String properties (remains the same)
        let get_optional_string = |key: &str| -> Option<String> {
            vertex.properties.get(key)
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
        };

        // 4. Extract Required Fields 
        let id = get_required_string("id")
            // Here we wrap the generic VertexError into the specific error seen in the traceback
            .map_err(|e| GraphError::InternalError(format!("Probabilistic linking failed: {}", e.to_string())))?; 
        
        let created_at = get_required_string("created_at")
            .map_err(|e| GraphError::InternalError(format!("Probabilistic linking failed: {}", e.to_string())))?;
            
        // NEW: Extract the Patient UUID that was stored as a property
        let target_patient_vertex_uuid = get_required_uuid("target_patient_vertex_uuid")
             .map_err(|e| GraphError::InternalError(format!("Probabilistic linking failed: {}", e.to_string())))?;


        // 5. Extract Optional Fields
        let updated_at = get_optional_string("updated_at");
        let canonical_mrn = get_optional_string("canonical_mrn");

        // 6. Construct and return the GoldenRecord
        Ok(GoldenRecord {
            id,
            gr_vertex_uuid: vertex.id.into(), // Now correctly stores the GR's UUID
            target_patient_vertex_uuid, // NEW FIELD
            created_at,
            updated_at,
            canonical_mrn,
        })
    }
}

// Provides a quick way to get the canonical ID as a string.
impl Into<String> for GoldenRecord {
    fn into(self) -> String {
        self.id
    }
}