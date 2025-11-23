use crate::graph_engine::Graph;
use crate::graph_engine::Vertex;
use crate::graph_engine::Edge;
use uuid::Uuid;
use std::collections::{HashSet, VecDeque};
use std::str::FromStr; 

impl Graph {
    /// Breadth-first traversal from a starting vertex id.
    pub fn bfs(&self, start: Uuid, max_depth: usize) -> Vec<&Vertex> {
        let mut visited = HashSet::new();
        let mut queue = VecDeque::new();
        let mut results = Vec::new();

        // Check if the start vertex exists
        if self.vertices.get(&start).is_none() {
            return results;
        }

        queue.push_back((start, 0));
        visited.insert(start);

        while let Some((current_id, depth)) = queue.pop_front() {
            if depth > max_depth {
                break;
            }

            // The current vertex is definitely in the map because we check `start`
            // and only queue up known vertex IDs.
            if let Some(vertex) = self.get_vertex(&current_id) {
                results.push(vertex);
            }

            if let Some(edge_ids) = self.get_edges_from(&current_id) {
                for edge_id in edge_ids {
                    if let Some(edge) = self.edges.get(edge_id) {
                        
                        // FIX: Use the inbound_id.0 (the destination vertex) for the next hop.
                        // We do not want to parse edge.edge_type.
                        let next_id = edge.inbound_id.0; // Uuid is retrieved directly
                        
                        if !visited.contains(&next_id) {
                            visited.insert(next_id);
                            queue.push_back((next_id, depth + 1));
                        }
                    }
                }
            }
        }

        results
    }

    // Additional traversal or pattern matching methods here...
}
