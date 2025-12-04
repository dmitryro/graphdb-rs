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
    display_name: String,
    x: u16,
    y: u16,
    width: u16,
}

/// Get a human-readable display name for a vertex
fn get_display_name(vertex: &Vertex) -> String {
    if let Some(name) = vertex.properties.get("name") {
        if let PropertyValue::String(s) = name {
            return s.clone();
        }
    }
    vertex.label.to_string()
}

/// Multi-level hierarchical layout centered on screen
fn generate_layout(graph: &Graph) -> Vec<PositionedVertex> {
    let mut positions = Vec::new();
    let all_vertices: Vec<&Vertex> = graph.vertices.values().collect();
    
    if all_vertices.is_empty() {
        return positions;
    }

    // Find the Patient node as root
    let root = all_vertices.iter()
        .find(|v| v.label.to_string() == "Patient")
        .or_else(|| {
            all_vertices.iter()
                .max_by_key(|v| graph.incoming_edges(&v.id.0).count())
        })
        .copied()
        .unwrap_or(all_vertices[0]);

    let mut levels: Vec<Vec<&Vertex>> = Vec::new();
    let mut visited = HashSet::new();
    
    // Level 0: Root
    levels.push(vec![root]);
    visited.insert(root.id.0);

    // BFS to build levels
    let mut current_level = vec![root];
    while !current_level.is_empty() {
        let mut next_level = Vec::new();
        
        for vertex in &current_level {
            // Add children (outgoing edges)
            for edge in graph.outgoing_edges(&vertex.id.0) {
                if let Some(target) = graph.vertices.get(&edge.inbound_id.0) {
                    if visited.insert(target.id.0) {
                        next_level.push(target);
                    }
                }
            }
            
            // Also check incoming edges to capture all connections
            for edge in graph.incoming_edges(&vertex.id.0) {
                if let Some(source) = graph.vertices.get(&edge.outbound_id.0) {
                    if visited.insert(source.id.0) {
                        next_level.push(source);
                    }
                }
            }
        }
        
        if !next_level.is_empty() {
            levels.push(next_level.clone());
            current_level = next_level;
        } else {
            break;
        }
    }

    // Position nodes level by level, centered horizontally
    let y_spacing = 10u16; // Vertical space between levels
    let x_spacing = 30u16; // Horizontal space between nodes
    let center_x = 70u16;  // Screen center X
    let start_y = 8u16;    // Start higher up for better centering

    for (level_idx, level) in levels.iter().enumerate() {
        let y = start_y + (level_idx as u16 * y_spacing);
        
        // Calculate positions to center this level
        let num_nodes = level.len() as u16;
        let level_width = if num_nodes > 1 {
            (num_nodes - 1) * x_spacing
        } else {
            0
        };
        
        let start_x = center_x.saturating_sub(level_width / 2);
        
        for (node_idx, vertex) in level.iter().enumerate() {
            let display = get_display_name(vertex);
            let width = display.len().max(vertex.label.to_string().len()).max(12) as u16 + 4;
            
            positions.push(PositionedVertex {
                id: vertex.id.0,
                label: vertex.label.to_string(),
                display_name: display,
                x: start_x + (node_idx as u16 * x_spacing),
                y,
                width,
            });
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

    enable_raw_mode()?;
    let mut stdout = io::stdout();
    stdout.execute(EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

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

            frame.render_widget(
                Block::default()
                    .borders(Borders::ALL)
                    .title("Medical Graph - WASD: pan | Q: quit"),
                Rect { x: 0, y: 0, width: area.width, height: area.height },
            );

            let inner = Rect {
                x: 1,
                y: 1,
                width: area.width.saturating_sub(2),
                height: area.height.saturating_sub(2),
            };

            // Draw edges with solid lines
            for edge in graph.edges.values() {
                if let Some(src_pos) = positions.iter().find(|p| p.id == edge.outbound_id.0) {
                    if let Some(tgt_pos) = positions.iter().find(|p| p.id == edge.inbound_id.0) {
                        // Calculate connection points (center bottom of source, center top of target)
                        let src_x = ((src_pos.x + src_pos.width / 2) as i16 + offset_x) as u16;
                        let src_y = ((src_pos.y + 5) as i16 + offset_y) as u16; // Bottom of source box
                        let tgt_x = ((tgt_pos.x + tgt_pos.width / 2) as i16 + offset_x) as u16;
                        let tgt_y = (tgt_pos.y as i16 + offset_y - 1) as u16; // Just above target box

                        // Calculate midpoint
                        let mid_y = (src_y + tgt_y) / 2;

                        // Draw vertical line from source down to midpoint
                        for y in src_y..=mid_y {
                            if y >= inner.y && y < inner.y + inner.height && 
                               src_x >= inner.x && src_x < inner.x + inner.width {
                                frame.render_widget(
                                    Paragraph::new("│").style(Style::default().fg(Color::Blue)),
                                    Rect::new(src_x, y, 1, 1),
                                );
                            }
                        }

                        // Draw horizontal line at midpoint (if nodes are not vertically aligned)
                        if src_x != tgt_x {
                            let min_x = src_x.min(tgt_x);
                            let max_x = src_x.max(tgt_x);
                            for x in min_x..=max_x {
                                if mid_y >= inner.y && mid_y < inner.y + inner.height &&
                                   x >= inner.x && x < inner.x + inner.width {
                                    frame.render_widget(
                                        Paragraph::new("─").style(Style::default().fg(Color::Blue)),
                                        Rect::new(x, mid_y, 1, 1),
                                    );
                                }
                            }
                        }

                        // Draw vertical line from midpoint down to target
                        for y in mid_y..=tgt_y {
                            if y >= inner.y && y < inner.y + inner.height &&
                               tgt_x >= inner.x && tgt_x < inner.x + inner.width {
                                frame.render_widget(
                                    Paragraph::new("│").style(Style::default().fg(Color::Blue)),
                                    Rect::new(tgt_x, y, 1, 1),
                                );
                            }
                        }

                        // Draw arrow pointing down at target
                        if tgt_y >= inner.y && tgt_y < inner.y + inner.height &&
                           tgt_x >= inner.x && tgt_x < inner.x + inner.width {
                            frame.render_widget(
                                Paragraph::new("▼").style(Style::default().fg(Color::Blue)),
                                Rect::new(tgt_x, tgt_y, 1, 1),
                            );
                        }

                        // Draw edge label with clear background above midpoint
                        let label = format!(" {} ", edge.label);
                        let label_len = label.len() as u16;
                        let label_x = src_x.saturating_sub(label_len / 2).max(inner.x + 2);
                        let label_y = mid_y.saturating_sub(1);
                        
                        if label_y >= inner.y && label_y < inner.y + inner.height &&
                           label_x >= inner.x && (label_x + label_len) < (inner.x + inner.width) {
                            // Clear background for label
                            frame.render_widget(
                                Paragraph::new(" ".repeat(label_len as usize))
                                    .style(Style::default().bg(Color::Black)),
                                Rect::new(label_x, label_y, label_len, 1),
                            );
                            // Draw label on top
                            frame.render_widget(
                                Paragraph::new(label)
                                    .style(Style::default()
                                        .fg(Color::Yellow)
                                        .bg(Color::Black)
                                        .add_modifier(Modifier::BOLD)),
                                Rect::new(label_x, label_y, label_len, 1),
                            );
                        }
                    }
                }
            }

            // Draw vertices on top of edges
            for pos in &positions {
                let x = (pos.x as i16 + offset_x) as u16;
                let y = (pos.y as i16 + offset_y) as u16;
                
                // Skip if completely out of bounds
                if x + pos.width < inner.x || x >= inner.x + inner.width ||
                   y + 5 < inner.y || y >= inner.y + inner.height {
                    continue;
                }

                let color = match pos.label.as_str() {
                    "Patient" => Color::Cyan,
                    "Encounter" => Color::Green,
                    "Medication" => Color::Magenta,
                    "Diagnosis" => Color::Red,
                    "Allergy" => Color::Yellow,
                    _ => Color::White,
                };

                let padding = pos.width.saturating_sub(2) as usize;
                let box_top = format!("╔{}╗", "═".repeat(padding));
                let box_name = format!("║{:^width$}║", pos.display_name, width = padding);
                let box_sep = format!("╟{}╢", "─".repeat(padding));
                let box_label = format!("║{:^width$}║", pos.label, width = padding);
                let box_bot = format!("╚{}╝", "═".repeat(padding));

                let style = Style::default().fg(color).add_modifier(Modifier::BOLD);

                if y >= inner.y && y < inner.y + inner.height {
                    frame.render_widget(
                        Paragraph::new(box_top).style(style),
                        Rect::new(x, y, pos.width, 1),
                    );
                }
                if y + 1 < inner.y + inner.height {
                    frame.render_widget(
                        Paragraph::new(box_name).style(style),
                        Rect::new(x, y + 1, pos.width, 1),
                    );
                }
                if y + 2 < inner.y + inner.height {
                    frame.render_widget(
                        Paragraph::new(box_sep).style(Style::default().fg(color)),
                        Rect::new(x, y + 2, pos.width, 1),
                    );
                }
                if y + 3 < inner.y + inner.height {
                    frame.render_widget(
                        Paragraph::new(box_label).style(Style::default().fg(color)),
                        Rect::new(x, y + 3, pos.width, 1),
                    );
                }
                if y + 4 < inner.y + inner.height {
                    frame.render_widget(
                        Paragraph::new(box_bot).style(style),
                        Rect::new(x, y + 4, pos.width, 1),
                    );
                }
            }
        })?;

        if event::poll(Duration::from_millis(50))? {
            if let Event::Key(key) = event::read()? {
                if key.kind == KeyEventKind::Press {
                    match key.code {
                        KeyCode::Char('q') | KeyCode::Esc => running = false,
                        KeyCode::Char('w') => offset_y = offset_y.saturating_add(2),
                        KeyCode::Char('s') => offset_y = offset_y.saturating_sub(2),
                        KeyCode::Char('a') => offset_x = offset_x.saturating_add(2),
                        KeyCode::Char('d') => offset_x = offset_x.saturating_sub(2),
                        _ => {}
                    }
                }
            }
        }
    }

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
}