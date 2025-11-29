use std::collections::{HashMap, HashSet};
use std::io;
use std::time::Duration;

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
        PropertyValue::Float(f) => f.0.to_string(), // Extract f64 from SerializableFloat
        PropertyValue::String(s) => s.clone(),
        PropertyValue::Uuid(u) => u.0.to_string(), // Extract Uuid from SerializableUuid
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
    pub source: String,
    pub target: String,
    pub label: String,
    pub properties: serde_json::Value,
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
                    let prop_value = PropertyValue::String(val.to_string());
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

            let mut edge_props = std::collections::BTreeMap::new();
            if let serde_json::Value::Object(obj) = e.properties {
                for (k, val) in obj {
                    edge_props.insert(k, PropertyValue::String(val.to_string()));
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

fn generate_layout(graph: &Graph) -> Vec<PositionedVertex> {
    let mut positions = Vec::new();
    let mut visited = HashSet::new();
    let all_vertices: Vec<&Vertex> = graph.vertices.values().collect();
    if all_vertices.is_empty() {
        return positions;
    }

    let mut root_candidates = Vec::new();
    for v in &all_vertices {
        if graph.incoming_edges(&v.id.0).next().is_none() {
            root_candidates.push(*v); // Dereference to &Vertex
        }
    }

    let roots: Vec<&Vertex> = if root_candidates.is_empty() {
        vec![all_vertices[0]]
    } else {
        root_candidates
    };

    let mut x_offset = 2u16;
    for (root_idx, root) in roots.iter().enumerate() {
        if visited.contains(&root.id.0) {
            continue;
        }
        let y_start = (root_idx as u16) * 6 + 1;
        let mut stack = vec![(*root, x_offset, y_start)];

        while let Some((v, x, y)) = stack.pop() {
            if visited.contains(&v.id.0) {
                continue;
            }
            visited.insert(v.id.0);
            let display_label = if let Some(name_prop) = v.properties.get("name") {
                match name_prop {
                    PropertyValue::String(s) => s.clone(),
                    _ => v.label.to_string(),
                }
            } else {
                v.label.to_string()
            };
            positions.push(PositionedVertex {
                id: v.id.0,
                label: display_label.to_string(),
                x,
                y,
            });

            let out_edges: Vec<&Edge> = graph.outgoing_edges(&v.id.0).collect();
            if out_edges.is_empty() {
                continue;
            }

            let spacing = 15u16;
            let total_width = (out_edges.len() as u16).saturating_sub(1) * spacing;
            let start_x = x.saturating_sub(total_width / 2);

            for (i, edge) in out_edges.iter().enumerate() {
                let target_id = edge.inbound_id.0;
                if let Some(target_v) = graph.get_vertex(&target_id) {
                    if !visited.contains(&target_id) {
                        let child_x = start_x + (i as u16) * spacing;
                        let child_y = y + 4;
                        stack.push((target_v, child_x, child_y)); // ← Remove * (no dereference)
                    }
                }
            }
            x_offset = x + 20;
        }
    }
    positions
}

pub fn visualize_graph_from_json(json_str: &str) -> Result<(), Box<dyn std::error::Error>> {
    let graph_json: GraphJson = serde_json::from_str(json_str)?;
    let graph = Graph::try_from(graph_json)?;
    visualize_graph(&graph)
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

            let max_width = 100u16.min(area.width);
            let max_height = 30u16.min(area.height);
            let draw_area = Rect {
                x: (area.width.saturating_sub(max_width)) / 2,
                y: (area.height.saturating_sub(max_height)) / 2,
                width: max_width,
                height: max_height,
            };

            frame.render_widget(
                Paragraph::new("")
                    .block(Block::default().borders(Borders::ALL).title("Graph Visualization")),
                draw_area,
            );

            let draw_x = draw_area.x as i16;
            let draw_y = draw_area.y as i16;

            for edge in graph.edges.values() {
                let source_pos = positions
                    .iter()
                    .find(|p| p.id == edge.outbound_id.0)
                    .map(|p| (p.x as i16 + offset_x, p.y as i16 + offset_y));
                let target_pos = positions
                    .iter()
                    .find(|p| p.id == edge.inbound_id.0)
                    .map(|p| (p.x as i16 + offset_x, p.y as i16 + offset_y));

                if let (Some((sx, sy)), Some((tx, ty))) = (source_pos, target_pos) {
                    let screen_sx = sx + draw_x;
                    let screen_sy = sy + draw_y;
                    let screen_tx = tx + draw_x;
                    let screen_ty = ty + draw_y;

                    if screen_sx >= draw_x && screen_sx < (draw_x + draw_area.width as i16) &&
                       screen_sy >= draw_y && screen_sy < (draw_y + draw_area.height as i16) &&
                       screen_tx >= draw_x && screen_tx < (draw_x + draw_area.width as i16) &&
                       screen_ty >= draw_y && screen_ty < (draw_y + draw_area.height as i16) {
                        
                        let width = (screen_tx - screen_sx).abs().max(1) as u16;
                        let x = screen_sx.min(screen_tx) as u16;
                        let y = screen_sy as u16;
                        
                        if y < draw_area.y + draw_area.height && x < draw_area.x + draw_area.width {
                            let edge_label = format!("── {}", edge.label);
                            frame.render_widget(
                                Paragraph::new(edge_label)
                                    .style(Style::default().fg(Color::Yellow)),
                                Rect::new(x, y, width.min(draw_area.width - (x - draw_area.x)), 1),
                            );
                        }
                    }
                }
            }

            for pos in &positions {
                let screen_x = (pos.x as i16 + offset_x + draw_x) as u16;
                let screen_y = (pos.y as i16 + offset_y + draw_y) as u16;
                
                if screen_x >= draw_area.x && screen_x < draw_area.x + draw_area.width &&
                   screen_y >= draw_area.y && screen_y < draw_area.y + draw_area.height {
                    
                    let vertex_label = format!("[{}: {}]", pos.label, pos.id.simple());
                    let truncated = if vertex_label.len() > 18 {
                        format!("{}..", &vertex_label[..16])
                    } else {
                        vertex_label
                    };
                    
                    let width = truncated.len() as u16;
                    frame.render_widget(
                        Paragraph::new(truncated)
                            .style(Style::default().fg(Color::Green)),
                        Rect::new(screen_x, screen_y, width, 1),
                    );
                }
            }

            let help = "(WASD: move, Q: quit)";
            frame.render_widget(
                Paragraph::new(help)
                    .style(Style::default().fg(Color::Blue)),
                Rect {
                    x: draw_area.x,
                    y: draw_area.y + draw_area.height.saturating_sub(1),
                    width: draw_area.width,
                    height: 1,
                },
            );
        })?;

        if event::poll(Duration::from_millis(50))? {
            if let Event::Key(key) = event::read()? {
                if key.kind == KeyEventKind::Press {
                    match key.code {
                        KeyCode::Char('q') | KeyCode::Esc => running = false,
                        KeyCode::Char('w') => offset_y = offset_y.saturating_sub(2),
                        KeyCode::Char('s') => offset_y = offset_y.saturating_add(2),
                        KeyCode::Char('a') => offset_x = offset_x.saturating_sub(2),
                        KeyCode::Char('d') => offset_x = offset_x.saturating_add(2),
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