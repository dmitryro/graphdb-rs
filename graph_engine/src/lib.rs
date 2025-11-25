// graph_engine/src/lib.rs
pub mod graph_service;
pub mod traversal;
pub mod medical;
pub mod pattern_match;
pub mod durability;

pub use graph_service::GraphService;
pub use medical::*;
pub use durability::*;
pub use models::vertices::Vertex;
pub use models::edges::Edge;
pub use models::properties::PropertyValue;
pub use models::identifiers::Identifier;
