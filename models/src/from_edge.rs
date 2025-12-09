// lib/src/models/to_vertex.rs

use super::edges::Edge;

// The trait must be defined with 'Sized' because it returns 'Self'.
pub trait FromEdge: Sized {
    /// Attempts to convert an Edge reference into the implementing type (Self).
    /// 
    /// - It takes 'edge: &Edge' (a reference) because it doesn't need to consume the Edge.
    /// - It returns 'Option<Self>' because the conversion might fail (e.g., if the label is wrong).
    /// - It does NOT take '&self' because it's a static conversion (like a constructor).
    fn from_edge(edge: &Edge) -> Option<Self>;
}
