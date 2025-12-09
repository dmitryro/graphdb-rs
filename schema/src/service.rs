use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::OnceCell;
use anyhow::Result;
use log::info;
use serde_json; // Needed for the enforce_contract method

// Importing schema component definitions
use crate::constraints::{ PropertyConstraint }; 
use crate::errors::{ SchemaError };
use crate::definitions::{ VertexSchema }; 
use crate::vertices::patient::Patient;
// NEW IMPORT: Bring in the generic edge loading function
use crate::edges::relationship::{ load_all_relationship_definitions };
// NEW IMPORT: Bring in EdgeSchema trait definition
use crate::edges::relationship::EdgeSchema;

/// The structure holding the entire, compiled graph schema definition.
/// This is the central source of truth for all graph element contracts.
#[derive(Debug, Clone)]
pub struct SchemaDefinition {
    // Storing PropertyConstraint to match the presumed return type of the VertexSchema trait
    pub vertices: HashMap<&'static str, Vec<PropertyConstraint>>,
    pub edges: HashMap<&'static str, Vec<PropertyConstraint>>,
    // Add fields for lifecycle rules, ontology references, etc., as needed.
}

/// The singleton service responsible for managing and enforcing the graph schema.
pub struct SchemaService {
    schema: SchemaDefinition,
}

impl SchemaService {
    /// Private constructor: Loads and compiles the declarative schema definitions.
    fn new() -> Result<Self> {
        info!("Initializing SchemaService: Compiling declarative schema definitions...");

        let mut vertices = HashMap::new();
        let mut edges = HashMap::new();
        
        // 1. Load all Vertex Schemas
        Self::load_vertex_schema::<Patient>(&mut vertices);

        // 2. Load all Edge Schemas (NOW GENERICALLY LOADED)
        // This iterates through ALL_MEDICAL_RELATIONSHIPS and loads all definitions.
        let edge_definitions = load_all_relationship_definitions();
        for def in edge_definitions {
            // Use the correct field: `edge_label` instead of the old `label`
            let label = def.metadata.edge_label;
            let constraints = def.properties; 
            // The edge label is a String, we convert it to &'static str for the HashMap key
            // NOTE: In a production environment, this might require a different HashMap structure (e.g., HashMap<String, ...>)
            // to store the owned string, but we stick to the required signature for now by using .leak().
            edges.insert(label, constraints); 
        }

        let schema = SchemaDefinition {
            vertices,
            edges,
        };

        // Updated log message to reflect both vertices and edges
        info!("SchemaService initialized successfully with {} vertices and {} edges.", 
            schema.vertices.len(), 
            schema.edges.len()
        );
        
        Ok(SchemaService { schema })
    }

    /// Helper function to load schema properties from a concrete type implementing `VertexSchema`.
    fn load_vertex_schema<T: VertexSchema>(map: &mut HashMap<&'static str, Vec<PropertyConstraint>>) {
        map.insert(T::schema_name(), T::property_constraints());
    }

    /// Retrieves the property constraints for a specific graph element type (Vertex or Edge).
    pub fn get_constraints(&self, element_name: &str) -> Option<&Vec<PropertyConstraint>> {
        if let Some(constraints) = self.schema.vertices.get(element_name) {
            Some(constraints)
        } else {
            self.schema.edges.get(element_name)
        }
    }

    /// Enforces the contract for a given element and its properties.
    pub fn enforce_contract(&self, element_name: &str, properties: &serde_json::Value) -> Result<(), SchemaError> {
        let constraints = self.get_constraints(element_name)
            .ok_or_else(|| SchemaError::InvalidSchemaName)?;

        // Iterate through properties and apply constraints
        for constraint in constraints {
            // Use .as_str() to get a &str reference for the JSON key lookup.
            let value = properties.get(constraint.name.as_str()).unwrap_or(&serde_json::Value::Null);
            
            // We capture and return the detailed String error `e`.
            if let Err(e) = constraint.validate(value) { 
                return Err(SchemaError::ConstraintViolation(e));
            }
        }

        // Additional checks for missing required properties not caught above
        // ...

        Ok(())
    }
}

// --- SINGLETON SETUP ---

/// Global OnceCell for lazy, safe, asynchronous initialization of the SchemaService.
/// This ensures the expensive schema compilation happens exactly once.
static SCHEMA_SERVICE_SINGLETON: OnceCell<Arc<SchemaService>> = OnceCell::const_new();

/// Initializes the singleton SchemaService if it hasn't been initialized yet.
pub async fn init_schema_service() -> Result<Arc<SchemaService>> {
    // Use the OnceCell to lazily initialize the service.
    // The inner closure will only be run once.
    let service_arc = SCHEMA_SERVICE_SINGLETON.get_or_try_init(|| async {
        let service = SchemaService::new()?;
        // Explicitly specifying the error type (anyhow::Error) for the inner Result
        // to resolve the earlier E0282/E0283 type inference ambiguity.
        Ok::<Arc<SchemaService>, anyhow::Error>(Arc::new(service))
    }).await?;

    Ok(service_arc.clone())
}

/// Retrieves a reference to the initialized SchemaService.
/// Panics if called before initialization.
pub fn get_schema_service() -> Arc<SchemaService> {
    // We use expect() because init_schema_service must be called at startup.
    SCHEMA_SERVICE_SINGLETON.get().expect("SchemaService must be initialized before use").clone()
}
