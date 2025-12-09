// lib/src/models/to_vertex.rs

use super::edges::Edge;

pub trait ToEdge {
    fn to_edge(&self) -> Edge;
}
