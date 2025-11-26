// graph_engine/src/traversal.rs
// Graph traversal algorithms — BFS, DFS, Dijkstra, A*, shortest path

use models::{Vertex, Graph, Edge};
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

/// Extension trait for all traversal algorithms.
pub trait TraverseExt {
    fn bfs(&self, start: Uuid, max_depth: Option<usize>) -> Vec<&Vertex>;
    fn dfs(&self, start: Uuid) -> Vec<&Vertex>;
    fn dijkstra(&self, start: Uuid) -> HashMap<Uuid, (i64, Option<Uuid>)>;
    fn a_star(&self, start: Uuid, goal: Uuid) -> Option<(i64, Vec<Uuid>)>;
    fn reachable(&self, start: Uuid) -> HashSet<Uuid>;
    fn shortest_path(&self, start: Uuid, goal: Uuid) -> Option<Vec<Uuid>>;
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
}