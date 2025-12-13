use chrono::{DateTime, Utc};
use uuid::Uuid;
use crate::constraints::PropertyConstraint;
use models::{Edge, ToEdge, FromEdge, Identifier, SerializableUuid, PropertyValue};
use crate::edges::relationship::EdgeSchema;

/// Represents a configuration link defining a survivorship rule applied by the Master Patient Index.
/// Survivorship rules determine which source record's field value "wins" when merging two records.
///
/// Edge Label: HAS_SURVIVORSHIP_RULE
/// Source Vertex: MasterPatientIndex (Configuration/Settings Vertex)
/// Target Vertex: SurvivorshipRule (Specific Rule Definition Vertex)
#[derive(Debug, Clone)]
pub struct HasSurvivorshipRule {
    /// The unique identifier for this specific edge instance (String).
    pub id: String,
    
    /// ID of the source vertex (e.g., the MPI configuration entity)
    pub source_id: Uuid,
    /// ID of the target vertex (the specific SurvivorshipRule entity)
    pub target_id: Uuid,

    /// The name of the rule, e.g., "Most Recent MRN" or "Longest Name".
    pub rule_name: String,

    /// The priority level of this rule. Lower numbers typically indicate higher priority/earlier evaluation.
    pub priority: i32,

    /// The patient field this rule specifically governs (e.g., "name", "address", "mrn").
    pub field_affected: String,

    /// The date and time when this rule association was created or configured.
    pub created_at: DateTime<Utc>,
}

// --- EdgeSchema Implementation for declarative schema definition ---
impl EdgeSchema for HasSurvivorshipRule {
    fn edge_label() -> &'static str {
        "HAS_SURVIVORSHIP_RULE"
    }

    fn property_constraints() -> Vec<PropertyConstraint> {
        vec![
            PropertyConstraint::new("id", true)
                .with_description("Unique edge instance ID (String)."),
            PropertyConstraint::new("rule_name", true)
                .with_description("The human-readable name of the survivorship rule."),
            PropertyConstraint::new("priority", true)
                .with_description("The evaluation order priority (i32)."),
            PropertyConstraint::new("field_affected", true)
                .with_description("The patient field (e.g., 'address') this rule governs."),
            PropertyConstraint::new("created_at", true)
                .with_description("Timestamp when the rule association was created (RFC3339)."),
        ]
    }
}
// --- END EdgeSchema ---


// --- ToEdge Implementation: Struct -> Generic Edge (Serialization) ---
impl ToEdge for HasSurvivorshipRule {
    /// Converts the HasSurvivorshipRule struct into a generic Edge structure.
    fn to_edge(&self) -> Edge {
        let outbound_id: SerializableUuid = self.source_id.into();
        let inbound_id: SerializableUuid = self.target_id.into();
        let edge_label_identifier = Identifier::new(Self::edge_label().to_string())
            .expect("Invalid edge label identifier");

        let mut e = Edge::new(outbound_id, edge_label_identifier, inbound_id);

        // Add relationship-specific properties
        e = e.with_property(
            "id".to_string(), 
            PropertyValue::String(self.id.clone())
        );
        e = e.with_property(
            "rule_name".to_string(), 
            PropertyValue::String(self.rule_name.clone())
        );
        // Cast i32 to i64 for PropertyValue::Int
        e = e.with_property(
            "priority".to_string(), 
            PropertyValue::Integer(self.priority as i64)
        );
        e = e.with_property(
            "field_affected".to_string(), 
            PropertyValue::String(self.field_affected.clone())
        );
        e = e.with_property(
            "created_at".to_string(), 
            PropertyValue::String(self.created_at.to_rfc3339()) // DateTime to RFC3339 String
        );

        e
    }
}


// --- FromEdge Implementation: Generic Edge -> Struct (Deserialization) ---
impl FromEdge for HasSurvivorshipRule {
    fn from_edge(edge: &Edge) -> Option<Self> {
        if edge.label.as_str() != Self::edge_label() { return None; }

        // Helper to parse RFC3339 string property into DateTime<Utc>
        let parse_datetime = |prop_name: &str, edge: &Edge| -> Option<DateTime<Utc>> {
            edge.properties.get(prop_name)
                .and_then(|v| v.as_str())
                .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
                .map(|dt| dt.with_timezone(&chrono::Utc))
        };
        
        // 1. Convert IDs from SerializableUuid back to Uuid
        let source_id = Uuid::parse_str(&edge.outbound_id.to_string()).ok()?; 
        let target_id = Uuid::parse_str(&edge.inbound_id.to_string()).ok()?;

        // 2. Safely extract mandatory properties
        let priority = match edge.properties.get("priority")? {
            PropertyValue::Integer(val) => *val as i32,
            _ => return None,
        };

        Some(HasSurvivorshipRule {
            id: edge.properties.get("id")?.as_str()?.to_string(),
            source_id, 
            target_id,
            rule_name: edge.properties.get("rule_name")?.as_str()?.to_string(),
            priority,
            field_affected: edge.properties.get("field_affected")?.as_str()?.to_string(),
            created_at: parse_datetime("created_at", edge)?,
        })
    }
}
