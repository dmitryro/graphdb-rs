// graph_engine/src/traversal.rs
// Graph traversal algorithms — BFS, DFS, Dijkstra, A*, shortest path

use crate::graph::Graph;
use models::Vertex;
use models::edges::Edge;
use uuid::Uuid;
use std::collections::{BinaryHeap, HashMap, HashSet, VecDeque};
use std::cmp::Ordering;

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

impl Graph {
    /// Breadth-first search
    pub fn bfs(&self, start: Uuid, max_depth: Option<usize>) -> Vec<&Vertex> {
        let mut visited = HashSet::new();
        let mut queue = VecDeque::new();
        let mut results = Vec::new();

        queue.push_back((start, 0));
        visited.insert(start);

        while let Some((current_id, depth)) = queue.pop_front() {
            if let Some(max_d) = max_depth {
                if depth > max_d {
                    continue;
                }
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

    /// Depth-first search
    pub fn dfs(&self, start: Uuid) -> Vec<&Vertex> {
        let mut visited = HashSet::new();
        let mut stack = vec![start];
        let mut results = Vec::new();

        while let Some(current_id) = stack.pop() {
            if visited.contains(&current_id) {
                continue;
            }
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

    /// Dijkstra's shortest path (weighted)
    /// Returns map: vertex_id → (cost, previous_vertex_id)
    pub fn dijkstra(&self, start: Uuid) -> HashMap<Uuid, (i64, Option<Uuid>)> {
        let mut distances: HashMap<Uuid, i64> = HashMap::new();
        let mut previous: HashMap<Uuid, Uuid> = HashMap::new();
        let mut heap = BinaryHeap::new();

        distances.insert(start, 0);
        heap.push(State { cost: 0, vertex_id: start });

        while let Some(State { cost, vertex_id }) = heap.pop() {
            if cost > distances.get(&vertex_id).copied().unwrap_or(i64::MAX) {
                continue;
            }

            for edge in self.outgoing_edges(&vertex_id) {
                let weight = edge.properties
                    .get("weight")
                    .and_then(|v| v.as_str())
                    .and_then(|s| s.parse::<i64>().ok())
                    .unwrap_or(1);

                let next_id = edge.inbound_id.0;
                let next_cost = cost + weight;

                if next_cost < distances.get(&next_id).copied().unwrap_or(i64::MAX) {
                    distances.insert(next_id, next_cost);
                    previous.insert(next_id, vertex_id);
                    heap.push(State { cost: next_cost, vertex_id: next_id });
                }
            }
        }

        // Convert previous to include cost
        let mut result = HashMap::new();
        for (vertex_id, prev) in previous {
            let cost = distances.get(&vertex_id).copied().unwrap_or(i64::MAX);
            result.insert(vertex_id, (cost, Some(prev)));
        }
        // Include start vertex
        result.entry(start).or_insert((0, None));

        result
    }

    /// A* search with heuristic (simple: 0 = Dijkstra)
    pub fn a_star(&self, start: Uuid, goal: Uuid) -> Option<(i64, Vec<Uuid>)> {
        let mut distances: HashMap<Uuid, i64> = HashMap::new();
        let mut previous: HashMap<Uuid, Uuid> = HashMap::new();
        let mut heap = BinaryHeap::new();

        distances.insert(start, 0);
        heap.push(State { cost: 0, vertex_id: start });

        while let Some(State { cost, vertex_id }) = heap.pop() {
            if vertex_id == goal {
                let mut path = vec![vertex_id];
                let mut current = vertex_id;
                while let Some(prev) = previous.get(&current) {
                    path.push(*prev);
                    current = *prev;
                }
                path.reverse();
                return Some((cost, path));
            }

            for edge in self.outgoing_edges(&vertex_id) {
                let weight = edge.properties
                    .get("weight")
                    .and_then(|v| v.as_str())
                    .and_then(|s| s.parse::<i64>().ok())
                    .unwrap_or(1);

                let next_id = edge.inbound_id.0;
                let tentative_cost = cost + weight;

                if tentative_cost < distances.get(&next_id).copied().unwrap_or(i64::MAX) {
                    distances.insert(next_id, tentative_cost);
                    previous.insert(next_id, vertex_id);

                    let h = 0; // Simple heuristic
                    let f = tentative_cost + h;
                    heap.push(State { cost: f, vertex_id: next_id });
                }
            }
        }

        None
    }

    /// Find all reachable vertices
    pub fn reachable(&self, start: Uuid) -> HashSet<Uuid> {
        let mut visited = HashSet::new();
        let mut stack = vec![start];

        while let Some(current) = stack.pop() {
            if visited.insert(current) {
                for edge in self.outgoing_edges(&current) {
                    stack.push(edge.inbound_id.0);
                }
            }
        }

        visited
    }

    /// Shortest path using BFS (unweighted)
    pub fn shortest_path(&self, start: Uuid, goal: Uuid) -> Option<Vec<Uuid>> {
        let mut previous: HashMap<Uuid, Uuid> = HashMap::new();
        let mut queue = VecDeque::new();
        queue.push_back(start);

        while let Some(current) = queue.pop_front() {
            if current == goal {
                let mut path = vec![current];
                let mut node = current;
                while let Some(&prev) = previous.get(&node) {
                    path.push(prev);
                    node = prev;
                }
                path.reverse();
                return Some(path);
            }

            for edge in self.outgoing_edges(&current) {
                let next = edge.inbound_id.0;
                if !previous.contains_key(&next) {
                    previous.insert(next, current);
                    queue.push_back(next);
                }
            }
        }

        None
    }
}