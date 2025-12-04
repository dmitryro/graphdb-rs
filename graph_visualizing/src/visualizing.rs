use std::collections::{HashMap, HashSet};
use std::io;
use std::time::Duration;
use std::collections::BTreeMap;

use crossterm::{
    event::{self, Event, KeyCode, KeyEventKind},
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
    ExecutableCommand,
};
use ratatui::{
    prelude::*,
    widgets::{Block, Borders, Paragraph},
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use models::{Graph, Edge, Vertex, identifiers::SerializableUuid, Identifier, PropertyValue};
use models::properties::SerializableFloat;

// --- PropertyValue to String helper ---
fn property_value_to_string(pv: &PropertyValue) -> String {
    match pv {
        PropertyValue::Boolean(b) => b.to_string(),
        PropertyValue::Integer(i) => i.to_string(),
        PropertyValue::Float(f) => f.0.to_string(),
        PropertyValue::String(s) => s.clone(),
        PropertyValue::Uuid(u) => u.0.to_string(),
        PropertyValue::Byte(b) => b.to_string(),
    }
}

/// JSON-compatible representation of a graph for import
#[derive(Serialize, Deserialize, Debug)]
pub struct GraphJson {
    pub vertices: Vec<JsonVertex>,
    #[serde(default)]
    pub edges: Vec<JsonEdge>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct JsonVertex {
    pub id: String,
    pub label: String,
    pub properties: serde_json::Value,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct JsonEdge {
    pub id: String,
    #[serde(alias = "outbound_id")]
    pub source: String,
    #[serde(alias = "inbound_id")]
    pub target: String,
    pub label: String,
    pub properties: serde_json::Value,
}

/// Top-level wrapper for Cypher query results
#[derive(Serialize, Deserialize, Debug)]
pub struct QueryResultWrapper {
    pub results: Vec<QueryResultRow>,
}

/// Structure for an individual row in the Cypher query results
#[derive(Serialize, Deserialize, Debug)]
pub struct QueryResultRow {
    pub vertices: Vec<JsonVertex>,
    #[serde(default)]
    pub edges: Vec<JsonEdge>,
    #[serde(default)]
    pub stats: serde_json::Value,
}

impl TryFrom<GraphJson> for Graph {
    type Error = String;

    fn try_from(json: GraphJson) -> Result<Self, Self::Error> {
        let mut graph = Graph::new();
        let mut id_map: HashMap<String, Uuid> = HashMap::new();

        for v in json.vertices {
            let uuid = Uuid::parse_str(&v.id).map_err(|e| format!("Invalid vertex ID: {}", e))?;
            id_map.insert(v.id.clone(), uuid);

            let mut props = HashMap::new();
            if let serde_json::Value::Object(obj) = v.properties {
                for (k, val) in obj {
                    let prop_value = PropertyValue::String(val.to_string().trim_matches('"').to_string());
                    props.insert(k, prop_value);
                }
            }

            let label = Identifier::new(v.label).map_err(|e| format!("Invalid vertex label: {:?}", e))?;
            let vertex = Vertex {
                id: SerializableUuid(uuid),
                label,
                properties: props,
            };
            graph.add_vertex(vertex);
        }

        for e in json.edges {
            let edge_uuid = Uuid::parse_str(&e.id).map_err(|e| format!("Invalid edge ID: {}", e))?;
            let source_uuid = id_map.get(&e.source).ok_or_else(|| format!("Edge source {} not found", e.source))?;
            let target_uuid = id_map.get(&e.target).ok_or_else(|| format!("Edge target {} not found", e.target))?;

            let mut edge_props = BTreeMap::new();
            if let serde_json::Value::Object(obj) = e.properties {
                for (k, val) in obj {
                    let prop_value = PropertyValue::String(val.to_string().trim_matches('"').to_string());
                    edge_props.insert(k, prop_value);
                }
            }

            let edge_type = Identifier::new(e.label.clone()).map_err(|e| format!("Invalid edge type: {:?}", e))?;
            let edge = Edge {
                id: SerializableUuid(edge_uuid),
                outbound_id: SerializableUuid(*source_uuid),
                inbound_id: SerializableUuid(*target_uuid),
                label: e.label,
                edge_type,
                properties: edge_props,
            };
            graph.add_edge(edge);
        }

        Ok(graph)
    }
}

#[derive(Debug, Clone)]
struct PositionedVertex {
    id: Uuid,
    label: String,
    x: u16,
    y: u16,
}

/// Improved layout algorithm using layered approach
fn generate_layout(graph: &Graph) -> Vec<PositionedVertex> {
    let mut positions = Vec::new();
    let all_vertices: Vec<&Vertex> = graph.vertices.values().collect();
    
    if all_vertices.is_empty() {
        return positions;
    }

    // Build adjacency info
    let mut out_degree: HashMap<Uuid, usize> = HashMap::new();
    let mut in_degree: HashMap<Uuid, usize> = HashMap::new();
    
    for v in &all_vertices {
        out_degree.insert(v.id.0, graph.outgoing_edges(&v.id.0).count());
        in_degree.insert(v.id.0, graph.incoming_edges(&v.id.0).count());
    }

    // Find root nodes (nodes with no incoming edges or high out-degree)
    let mut roots: Vec<&Vertex> = all_vertices.iter()
        .filter(|v| *in_degree.get(&v.id.0).unwrap_or(&0) == 0)
        .copied()
        .collect();

    if roots.is_empty() {
        // If no clear root, pick the vertex with highest out-degree
        roots = vec![all_vertices.iter()
            .max_by_key(|v| out_degree.get(&v.id.0).unwrap_or(&0))
            .copied()
            .unwrap()];
    }

    // Layer-based layout (BFS from roots)
    let mut layers: Vec<Vec<Uuid>> = Vec::new();
    let mut visited = HashSet::new();
    let mut current_layer: Vec<Uuid> = roots.iter().map(|v| v.id.0).collect();
    
    while !current_layer.is_empty() {
        layers.push(current_layer.clone());
        for id in &current_layer {
            visited.insert(*id);
        }
        
        let mut next_layer = Vec::new();
        for id in &current_layer {
            if let Some(v) = graph.vertices.get(id) {
                for edge in graph.outgoing_edges(&v.id.0) {
                    let target_id = edge.inbound_id.0;
                    if !visited.contains(&target_id) && !next_layer.contains(&target_id) {
                        next_layer.push(target_id);
                    }
                }
            }
        }
        current_layer = next_layer;
    }

    // Add any remaining unvisited vertices
    for v in &all_vertices {
        if !visited.contains(&v.id.0) {
            layers.push(vec![v.id.0]);
            visited.insert(v.id.0);
        }
    }

    // Calculate positions with proper spacing
    let vertical_spacing = 8u16;
    let horizontal_spacing = 25u16;
    let start_y = 3u16;
    
    for (layer_idx, layer) in layers.iter().enumerate() {
        let y = start_y + (layer_idx as u16 * vertical_spacing);
        let layer_width = (layer.len() as u16).saturating_sub(1) * horizontal_spacing;
        let start_x = if layer_width > 0 { 10u16 } else { 40u16 };
        
        for (node_idx, node_id) in layer.iter().enumerate() {
            if let Some(v) = graph.vertices.get(node_id) {
                let x = start_x + (node_idx as u16 * horizontal_spacing);
                
                let display_label = if let Some(name_prop) = v.properties.get("name") {
                    match name_prop {
                        PropertyValue::String(s) => s.clone(),
                        _ => v.label.to_string(),
                    }
                } else {
                    v.label.to_string()
                };
                
                positions.push(PositionedVertex {
                    id: *node_id,
                    label: display_label,
                    x,
                    y,
                });
            }
        }
    }

    positions
}

pub fn visualize_graph_from_graph_json(graph_json: GraphJson) -> Result<(), Box<dyn std::error::Error>> {
    let graph = Graph::try_from(graph_json)?;
    visualize_graph(&graph)
}

pub fn visualize_graph_from_json(json_str: &str) -> Result<(), Box<dyn std::error::Error>> {
    let wrapper: QueryResultWrapper = serde_json::from_str(json_str)
        .map_err(|e| format!("Result is not a valid query structure: {}", e))?;

    if let Some(result_row) = wrapper.results.into_iter().next() {
        let graph_json = GraphJson {
            vertices: result_row.vertices,
            edges: result_row.edges,
        };
        visualize_graph_from_graph_json(graph_json)
    } else {
        println!("Cypher query returned no graph structure to visualize.");
        Ok(())
    }
}

pub fn visualize_graph(graph: &Graph) -> Result<(), Box<dyn std::error::Error>> {
    if graph.vertices.is_empty() {
        println!("Graph is empty — nothing to visualize.");
        return Ok(());
    }

    // Terminal setup
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    stdout.execute(EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    // Initial layout and state
    let positions = generate_layout(graph);
    let mut offset_x = 0i16;
    let mut offset_y = 0i16;
    let mut running = true;

    while running {
        terminal.draw(|frame| {
            let area = frame.area();
            if area.width == 0 || area.height == 0 {
                return;
            }

            // Use almost full terminal size
            let max_width = area.width.saturating_sub(4);
            let max_height = area.height.saturating_sub(4);
            let draw_area = Rect {
                x: 2,
                y: 2,
                width: max_width,
                height: max_height,
            };

            // Main visualization border
            frame.render_widget(
                Paragraph::new("")
                    .block(Block::default().borders(Borders::ALL).title("Graph Visualization (WASD: move, Q: quit)")),
                draw_area,
            );

            let draw_x = draw_area.x as i16;
            let draw_y = draw_area.y as i16;

            // --- Draw Edges ---
            for edge in graph.edges.values() {
                let source_pos = positions.iter()
                    .find(|p| p.id == edge.outbound_id.0)
                    .map(|p| (p.x as i16 + offset_x, p.y as i16 + offset_y));
                let target_pos = positions.iter()
                    .find(|p| p.id == edge.inbound_id.0)
                    .map(|p| (p.x as i16 + offset_x, p.y as i16 + offset_y));

                if let (Some((sx, sy)), Some((tx, ty))) = (source_pos, target_pos) {
                    let screen_sx = (sx + draw_x) as u16;
                    let screen_sy = (sy + draw_y) as u16;
                    let screen_tx = (tx + draw_x) as u16;
                    let screen_ty = (ty + draw_y) as u16;

                    // Draw edge line between nodes
                    if screen_sy < screen_ty {
                        // Draw vertical connection
                        for y in screen_sy + 1..screen_ty {
                            if y > draw_area.y && y < draw_area.y + draw_area.height &&
                               screen_sx > draw_area.x && screen_sx < draw_area.x + draw_area.width {
                                frame.render_widget(
                                    Paragraph::new("│").style(Style::default().fg(Color::Yellow)),
                                    Rect::new(screen_sx, y, 1, 1),
                                );
                            }
                        }
                        // Draw edge label
                        let label_y = (screen_sy + screen_ty) / 2;
                        if label_y > draw_area.y && label_y < draw_area.y + draw_area.height {
                            let edge_label = format!("─{}→", edge.label);
                            frame.render_widget(
                                Paragraph::new(edge_label.clone())
                                    .style(Style::default().fg(Color::Cyan)),
                                Rect::new(screen_sx + 2, label_y, edge_label.len() as u16, 1),
                            );
                        }
                    } else {
                        // Draw horizontal connection
                        let min_x = screen_sx.min(screen_tx);
                        let max_x = screen_sx.max(screen_tx);
                        if screen_sy > draw_area.y && screen_sy < draw_area.y + draw_area.height {
                            let edge_label = format!("─{}→", edge.label);
                            let width = (max_x - min_x).max(edge_label.len() as u16);
                            frame.render_widget(
                                Paragraph::new(edge_label)
                                    .style(Style::default().fg(Color::Cyan)),
                                Rect::new(min_x, screen_sy, width, 1),
                            );
                        }
                    }
                }
            }

            // --- Draw Vertices ---
            for pos in &positions {
                let screen_x = (pos.x as i16 + offset_x + draw_x) as u16;
                let screen_y = (pos.y as i16 + offset_y + draw_y) as u16;
                
                if screen_x > draw_area.x && screen_x < draw_area.x + draw_area.width &&
                   screen_y > draw_area.y && screen_y < draw_area.y + draw_area.height {
                    
                    let vertex_label = format!("┌─{}─┐", pos.label);
                    let vertex_id = format!("│{}│", pos.id.simple());
                    let vertex_bottom = "└─────┘";
                    
                    let width = vertex_label.len().max(vertex_id.len()).max(vertex_bottom.len()) as u16;
                    
                    frame.render_widget(
                        Paragraph::new(vertex_label)
                            .style(Style::default().fg(Color::Green).add_modifier(Modifier::BOLD)),
                        Rect::new(screen_x, screen_y, width, 1),
                    );
                    frame.render_widget(
                        Paragraph::new(vertex_id)
                            .style(Style::default().fg(Color::Green)),
                        Rect::new(screen_x, screen_y + 1, width, 1),
                    );
                    frame.render_widget(
                        Paragraph::new(vertex_bottom)
                            .style(Style::default().fg(Color::Green)),
                        Rect::new(screen_x, screen_y + 2, width, 1),
                    );
                }
            }
        })?;

        // --- Event Handling ---
        if event::poll(Duration::from_millis(50))? {
            if let Event::Key(key) = event::read()? {
                if key.kind == KeyEventKind::Press {
                    match key.code {
                        KeyCode::Char('q') | KeyCode::Esc => running = false,
                        KeyCode::Char('w') => offset_y = offset_y.saturating_add(3),
                        KeyCode::Char('s') => offset_y = offset_y.saturating_sub(3),
                        KeyCode::Char('a') => offset_x = offset_x.saturating_add(3),
                        KeyCode::Char('d') => offset_x = offset_x.saturating_sub(3),
                        _ => {}
                    }
                }
            }
        }
    }

    // Terminal cleanup
    disable_raw_mode()?;
    io::stdout().execute(LeaveAlternateScreen)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sample_json() {
        let json = r#"
        {
            "vertices": [
                {"id": "550e8400-e29b-41d4-a716-446655440000", "label": "Patient", "properties": {"name": "John Doe", "age": "45"}},
                {"id": "550e8400-e29b-41d4-a716-446655440001", "label": "Encounter", "properties": {"type": "ED", "location": "Room 5"}}
            ],
            "edges": [
                {
                    "id": "550e8400-e29b-41d4-a716-446655440002",
                    "source": "550e8400-e29b-41d4-a716-446655440000",
                    "target": "550e8400-e29b-41d4-a716-446655440001",
                    "label": "HAS_ENCOUNTER",
                    "properties": {}
                }
            ]
        }
        "#;

        let graph_json: GraphJson = serde_json::from_str(json).unwrap();
        let graph = Graph::try_from(graph_json).unwrap();
        assert_eq!(graph.vertices.len(), 2);
        assert_eq!(graph.edges.len(), 1);
    }
    
    #[test]
    fn test_cypher_result_wrapper_parsing() {
        let json = r#"{
          "results": [
            {
              "vertices": [
                {
                  "id": "5de26827-3516-4676-9c48-96dbbf01bdf0",
                  "label": "Patient",
                  "properties": {
                    "id": "12345",
                    "name": "John Doe"
                  }
                },
                {
                  "id": "57c03bd2-043b-49c2-8c93-21431173bf5e",
                  "label": "Encounter",
                  "properties": {
                    "type": "ED_TRIAGE"
                  }
                }
              ],
              "edges": [
                {
                  "id": "721f8fc0-6466-4f97-baaf-4b7690c99a3a",
                  "outbound_id": "5de26827-3516-4676-9c48-96dbbf01bdf0",
                  "inbound_id": "57c03bd2-043b-49c2-8c93-21431173bf5e",
                  "label": "HAS_ENCOUNTER",
                  "properties": {}
                }
              ],
              "stats": {}
            }
          ]
        }"#;

        let wrapper: QueryResultWrapper = serde_json::from_str(json).unwrap();
        assert_eq!(wrapper.results.len(), 1);

        let result_row = wrapper.results.get(0).unwrap();
        assert_eq!(result_row.vertices.len(), 2);
        assert_eq!(result_row.edges.len(), 1);

        let graph_json = GraphJson {
            vertices: result_row.vertices.clone(),
            edges: result_row.edges.clone(),
        };

        let graph = Graph::try_from(graph_json).unwrap();
        assert_eq!(graph.vertices.len(), 2);
        assert_eq!(graph.edges.len(), 1);
    }
}