// lib/src/models/to_vertex.rs

use models::vertices::Vertex;

// The trait must be defined with 'Sized' because it returns 'Self'.
pub trait FromVertex: Sized {
    /// Attempts to convert an Vertex reference into the implementing type (Self).
    /// 
    /// - It takes 'vertex: &Vertex' (a reference) because it doesn't need to consume the Vertex.
    /// - It returns 'Option<Self>' because the conversion might fail (e.g., if the label is wrong).
    /// - It does NOT take '&self' because it's a static conversion (like a constructor).
    fn from_vertex(vertex: &Vertex) -> Option<Self>;
}

