// graph_engine/src/traversal.rs
// Graph traversal algorithms — BFS, DFS, Dijkstra, A*, shortest path

use models::{Vertex, Graph, Edge};
use uuid::Uuid;
use std::collections::{BinaryHeap, HashMap, HashSet, VecDeque};
use std::cmp::Ordering;
use crate::graph_engine::pattern_match::{ node_matches_constraints };
use crate::query_parser::query_types::{ NodePattern, RelPattern };
// State for Dijkstra/A* — use i64 for cost to satisfy Eq
#[derive(Copy, Clone, Eq, PartialEq)]
struct State {
    cost: i64,
    vertex_id: Uuid,
}

impl Ord for State {
    fn cmp(&self, other: &Self) -> Ordering {
        other.cost.cmp(&self.cost)
            .then_with(|| self.vertex_id.cmp(&other.vertex_id))
    }
}

impl PartialOrd for State {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Extension trait for all traversal algorithms.
pub trait TraverseExt {
    fn bfs(&self, start: Uuid, max_depth: Option<usize>) -> Vec<&Vertex>;
    fn dfs(&self, start: Uuid) -> Vec<&Vertex>;
    fn dijkstra(&self, start: Uuid) -> HashMap<Uuid, (i64, Option<Uuid>)>;
    fn a_star(&self, start: Uuid, goal: Uuid) -> Option<(i64, Vec<Uuid>)>;
    fn reachable(&self, start: Uuid) -> HashSet<Uuid>;
    fn shortest_path(&self, start: Uuid, goal: Uuid) -> Option<Vec<Uuid>>;
    /// Finds all vertices and edges participating in paths that match the variable-length pattern.
    /// Returns (matched_vertex_ids, matched_edge_ids).
    fn match_variable_length_path(
        &self, 
        start_id: Uuid, 
        rel_pat: &RelPattern, 
        end_node_pat: &NodePattern
    ) -> (HashSet<Uuid>, HashSet<Uuid>);
}

impl TraverseExt for Graph {
    fn bfs(&self, start: Uuid, max_depth: Option<usize>) -> Vec<&Vertex> {
        let mut visited = HashSet::new();
        let mut queue = VecDeque::new();
        let mut results = Vec::new();

        queue.push_back((start, 0));
        visited.insert(start);

        while let Some((current_id, depth)) = queue.pop_front() {
            if let Some(max_d) = max_depth {
                if depth > max_d { continue; }
            }
            if let Some(vertex) = self.get_vertex(&current_id) {
                results.push(vertex);
            }
            for edge in self.outgoing_edges(&current_id) {
                let next_id = edge.inbound_id.0;
                if !visited.contains(&next_id) {
                    visited.insert(next_id);
                    queue.push_back((next_id, depth + 1));
                }
            }
        }
        results
    }

    fn dfs(&self, start: Uuid) -> Vec<&Vertex> {
        let mut visited = HashSet::new();
        let mut stack = vec![start];
        let mut results = Vec::new();

        while let Some(current_id) = stack.pop() {
            if visited.contains(&current_id) { continue; }
            visited.insert(current_id);
            if let Some(vertex) = self.get_vertex(&current_id) {
                results.push(vertex);
            }
            for edge in self.outgoing_edges(&current_id) {
                let next_id = edge.inbound_id.0;
                if !visited.contains(&next_id) {
                    stack.push(next_id);
                }
            }
        }
        results
    }

    fn dijkstra(&self, start: Uuid) -> HashMap<Uuid, (i64, Option<Uuid>)> {
        let mut dist: HashMap<Uuid, i64> = HashMap::new();
        let mut prev: HashMap<Uuid, Uuid> = HashMap::new();
        let mut heap = BinaryHeap::new();

        dist.insert(start, 0);
        heap.push(State { cost: 0, vertex_id: start });

        while let Some(State { cost, vertex_id }) = heap.pop() {
            if cost > dist.get(&vertex_id).copied().unwrap_or(i64::MAX) { continue; }
            for edge in self.outgoing_edges(&vertex_id) {
                let w = edge.properties
                    .get("weight")
                    .and_then(|v| v.as_str())
                    .and_then(|s| s.parse::<i64>().ok())
                    .unwrap_or(1);
                let next = edge.inbound_id.0;
                let new_cost = cost + w;
                if new_cost < dist.get(&next).copied().unwrap_or(i64::MAX) {
                    dist.insert(next, new_cost);
                    prev.insert(next, vertex_id);
                    heap.push(State { cost: new_cost, vertex_id: next });
                }
            }
        }
        let mut out = HashMap::new();
        for (v, p) in prev {
            out.insert(v, (dist[&v], Some(p)));
        }
        out.entry(start).or_insert((0, None));
        out
    }

    fn a_star(&self, start: Uuid, goal: Uuid) -> Option<(i64, Vec<Uuid>)> {
        let mut dist: HashMap<Uuid, i64> = HashMap::new();
        let mut prev: HashMap<Uuid, Uuid> = HashMap::new();
        let mut heap = BinaryHeap::new();

        dist.insert(start, 0);
        heap.push(State { cost: 0, vertex_id: start });

        while let Some(State { cost, vertex_id }) = heap.pop() {
            if vertex_id == goal {
                let mut path = vec![vertex_id];
                let mut cur = vertex_id;
                while let Some(&p) = prev.get(&cur) {
                    path.push(p);
                    cur = p;
                }
                path.reverse();
                return Some((cost, path));
            }
            for edge in self.outgoing_edges(&vertex_id) {
                let w = edge.properties
                    .get("weight")
                    .and_then(|v| v.as_str())
                    .and_then(|s| s.parse::<i64>().ok())
                    .unwrap_or(1);
                let next = edge.inbound_id.0;
                let new_cost = dist[&vertex_id] + w;
                if new_cost < dist.get(&next).copied().unwrap_or(i64::MAX) {
                    dist.insert(next, new_cost);
                    prev.insert(next, vertex_id);
                    let h = 0i64; // 0 == Dijkstra
                    heap.push(State { cost: new_cost + h, vertex_id: next });
                }
            }
        }
        None
    }

    fn reachable(&self, start: Uuid) -> HashSet<Uuid> {
        let mut vis = HashSet::new();
        let mut stack = vec![start];
        while let Some(cur) = stack.pop() {
            if vis.insert(cur) {
                for e in self.outgoing_edges(&cur) {
                    stack.push(e.inbound_id.0);
                }
            }
        }
        vis
    }

    fn shortest_path(&self, start: Uuid, goal: Uuid) -> Option<Vec<Uuid>> {
        let mut prev: HashMap<Uuid, Uuid> = HashMap::new();
        let mut q = VecDeque::new();
        q.push_back(start);
        while let Some(cur) = q.pop_front() {
            if cur == goal {
                let mut path = vec![cur];
                let mut node = cur;
                while let Some(&p) = prev.get(&node) {
                    path.push(p);
                    node = p;
                }
                path.reverse();
                return Some(path);
            }
            for e in self.outgoing_edges(&cur) {
                let n = e.inbound_id.0;
                if !prev.contains_key(&n) {
                    prev.insert(n, cur);
                    q.push_back(n);
                }
            }
        }
        None
    }
    
    fn match_variable_length_path(
        &self, 
        start_id: Uuid, 
        rel_pat: &RelPattern, 
        end_node_pat: &NodePattern
    ) -> (HashSet<Uuid>, HashSet<Uuid>) {
        
        let (_end_var, end_labels, end_props) = end_node_pat;
        let (_rel_var, rel_label_opt, len_range_opt, _rel_props, direction_opt) = rel_pat;

        // Helper to convert Vec<String> to Option<String> for the current node_matches_constraints
        // This takes the first label if it exists, matching the old behavior.
        let end_label_primary = end_labels.first();

        let (min_hops_opt, max_hops_opt): (Option<u32>, Option<u32>) = len_range_opt
            .unwrap_or((Some(1), Some(1)));
            
        let min_hops = min_hops_opt.unwrap_or(1);
        let max_hops = max_hops_opt.unwrap_or(u32::MAX);

        let mut queue: VecDeque<(Uuid, Vec<Uuid>, Vec<Uuid>)> = VecDeque::new(); 
        let mut best_depth: HashMap<Uuid, u32> = HashMap::from([(start_id, 0)]);

        let mut all_matched_path_vertices = HashSet::new();
        let mut all_matched_path_edges = HashSet::new();

        // 0-hop case
        if min_hops == 0 {
            if let Some(start_vertex) = self.get_vertex(&start_id) {
                // FIX: Pass &end_label_primary (Option<String>) to satisfy the checker
                if node_matches_constraints(start_vertex, &end_label_primary.cloned(), end_props) {
                    all_matched_path_vertices.insert(start_id);
                }
            }
        }
        
        queue.push_back((start_id, vec![start_id], Vec::new()));
        
        while let Some((current_id, current_vertices, current_edges)) = queue.pop_front() {
            let depth = current_edges.len() as u32;

            if depth >= max_hops { continue; }

            let is_outgoing_pattern: bool = direction_opt.unwrap_or(true);

            let all_incident_edges: Vec<&Edge> = self.outgoing_edges(&current_id)
                .into_iter()
                .chain(self.incoming_edges(&current_id).into_iter())
                .collect();
            
            for edge in all_incident_edges {
                
                // 1. Check relationship type match
                if !rel_label_opt.as_deref().map_or(true, |l| edge.label.as_str() == l) {
                    continue;
                }

                // 2. Determine direction match
                let is_outbound_from_current = edge.outbound_id.0 == current_id;
                let is_inbound_to_current = edge.inbound_id.0 == current_id;
                
                let (matched_direction, next_id) = if is_outbound_from_current {
                    if is_outgoing_pattern || direction_opt.is_none() {
                        (true, edge.inbound_id.0) 
                    } else {
                        (false, Uuid::nil()) 
                    }
                } else if is_inbound_to_current {
                    if !is_outgoing_pattern || direction_opt.is_none() {
                        (true, edge.outbound_id.0) 
                    } else {
                        (false, Uuid::nil()) 
                    }
                } else {
                    (false, Uuid::nil())
                };

                if !matched_direction || next_id.is_nil() { continue; }
                
                // 3. Skip cycles
                if current_vertices.contains(&next_id) { continue; }

                let next_depth = depth + 1;

                // 4. Optimization: shortest path only
                if *best_depth.get(&next_id).unwrap_or(&u32::MAX) <= next_depth {
                    continue;
                }
                
                best_depth.insert(next_id, next_depth); 

                let mut next_vertices = current_vertices.clone();
                next_vertices.push(next_id);
                
                let mut next_edges = current_edges.clone();
                next_edges.push(edge.id.0);

                // 5. Check end node constraints
                if next_depth >= min_hops {
                    if let Some(next_vertex) = self.get_vertex(&next_id) {
                        // FIX: Pass the primary label as an Option<String>
                        if node_matches_constraints(next_vertex, &end_label_primary.cloned(), end_props) {
                            all_matched_path_vertices.extend(next_vertices.iter().cloned());
                            all_matched_path_edges.extend(next_edges.iter().cloned());
                        }
                    }
                }
                
                // 6. Continue search
                if next_depth < max_hops {
                    queue.push_back((next_id, next_vertices, next_edges));
                }
            }
        }

        (all_matched_path_vertices, all_matched_path_edges)
    }
}