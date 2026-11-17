//! Drawing a diagram that elkrs has laid out, in the shapes, sizes and colors that Lingua Franca
//! gives its reactors, reactions, ports and actions.

use std::fmt::Write;
use std::iter::once;

use serde_json::Value;

use super::{Diagram, DiagramNode, PADDING, PortKind, TITLE_HEIGHT};

/// The font size of the name of a node.
pub(super) const TITLE_SIZE: f64 = 10.0;
/// The font size of every other text.
pub(super) const LABEL_SIZE: f64 = 8.0;
/// The width and the height of a port's triangle.
pub(super) const PORT: f64 = 8.0;
pub(super) const TASK_HEIGHT: f64 = 22.0;
/// The width and the height of an action's triangle.
pub(super) const ACTION: f64 = 18.0;
/// The length of the point at the right of a task's box, and of the notch at its left.
const POINT: f64 = 6.0;
/// The drawing is shown this much larger than its coordinates, which keeps its text readable.
const SCALE: f64 = 1.5;
const BORDER: &str = "#bebebe";
const NODE_FILL: &str = "#f2f2f2";
const TASK_FILL: &str = "#a6a6a6";
const TASK_BORDER: &str = "#737373";

/// Read a coordinate, to a hundredth: elkrs returns some with the noise of a shorter float.
fn number(value: &Value, key: &str) -> f64 {
    round(value[key].as_f64().unwrap())
}

fn round(coordinate: f64) -> f64 {
    (coordinate * 100.0).round() / 100.0
}

/// Write a text whose vertical middle is at `middle`; `anchor` says which part of it is at `x`.
fn text(svg: &mut String, x: f64, middle: f64, size: f64, anchor: &str, content: &str) {
    let content = content
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;");
    // The baseline of a text is about a third of its size below its middle.
    let (x, baseline) = (round(x), round(middle + 0.35 * size));
    writeln!(
        svg,
        r#"<text x="{x}" y="{baseline}" font-size="{size}" text-anchor="{anchor}">{content}</text>"#
    )
    .unwrap();
}

fn points(points: &[(f64, f64)]) -> String {
    let points = points
        .iter()
        .map(|(x, y)| format!("{},{}", round(*x), round(*y)));
    points.collect::<Vec<_>>().join(" ")
}

/// Draw a node: its box with its id in the corner and its name, its actions and tasks in a
/// column, and its ports with their names beside them.
fn draw_node(svg: &mut String, node_id: usize, node: &DiagramNode, placed: &Value) {
    let (x, y) = (number(placed, "x"), number(placed, "y"));
    let (width, height) = (number(placed, "width"), number(placed, "height"));
    writeln!(
        svg,
        r#"<rect class="node" x="{x}" y="{y}" width="{width}" height="{height}" rx="8" fill="{NODE_FILL}" stroke="{BORDER}"/>"#
    )
    .unwrap();
    let title = y + TITLE_HEIGHT / 2.0;
    writeln!(svg, r#"<g fill="{TASK_BORDER}">"#).unwrap();
    let id = node_id.to_string();
    text(svg, x + PADDING, title, LABEL_SIZE, "start", &id);
    svg.push_str("</g>\n");
    text(
        svg,
        x + width / 2.0,
        title,
        TITLE_SIZE,
        "middle",
        &node.name,
    );

    // The column is centered in what the names of the ports leave of the body.
    let (inputs, outputs) = (PortKind::Input, PortKind::Output);
    let (left, right) = (node.names_width(inputs), node.names_width(outputs));
    let center = x + (width + left - right) / 2.0;
    let mut top = y + TITLE_HEIGHT + (node.body_height() - node.column_height()) / 2.0;
    for (_, name) in node.ports_of(PortKind::Action) {
        let left = center - DiagramNode::action_width(name) / 2.0;
        let (right, bottom) = (left + ACTION, top + ACTION);
        let triangle = points(&[(left + ACTION / 2.0, top), (right, bottom), (left, bottom)]);
        writeln!(
            svg,
            r#"<polygon class="action" points="{triangle}" fill="white" stroke="black"/>"#
        )
        .unwrap();
        // The letter marks the action as physical: its events come from outside the node.
        let letter = top + 0.65 * ACTION;
        text(svg, left + ACTION / 2.0, letter, LABEL_SIZE, "middle", "P");
        text(
            svg,
            right + PADDING / 2.0,
            letter,
            LABEL_SIZE,
            "start",
            name,
        );
        top = bottom + PADDING;
    }
    let left = center - node.task_width() / 2.0;
    let right = left + node.task_width();
    for task in &node.tasks {
        let (middle, bottom) = (top + TASK_HEIGHT / 2.0, top + TASK_HEIGHT);
        let outline = points(&[
            (left, top),
            (right - POINT, top),
            (right, middle),
            (right - POINT, bottom),
            (left, bottom),
            (left + POINT, middle),
        ]);
        writeln!(
            svg,
            r#"<polygon class="task" points="{outline}" fill="{TASK_FILL}" stroke="{TASK_BORDER}"/>"#
        )
        .unwrap();
        text(svg, center, middle, LABEL_SIZE, "middle", task);
        top = bottom + PADDING;
    }

    for port in placed["ports"].as_array().into_iter().flatten() {
        let (left, top) = (x + number(port, "x"), y + number(port, "y"));
        let triangle = points(&[
            (left, top),
            (left + PORT, top + PORT / 2.0),
            (left, top + PORT),
        ]);
        writeln!(svg, r#"<polygon class="port" points="{triangle}"/>"#).unwrap();
        // The port's id ends with its position, which finds its name.
        let position = port["id"].as_str().unwrap().rsplit_once(".p").unwrap().1;
        let (name, kind) = &node.ports[position.parse::<usize>().unwrap()];
        let middle = top + PORT / 2.0;
        match kind {
            PortKind::Input => text(svg, x + PADDING, middle, LABEL_SIZE, "start", name),
            _ => text(svg, x + width - PADDING, middle, LABEL_SIZE, "end", name),
        }
    }
}

/// Draw a connection: its line, which is dashed if the connection is physical, a dot where it
/// leaves a line that it shares, and its delay.
fn draw_connection(svg: &mut String, edge: &Value, physical: bool) {
    let section = &edge["sections"][0];
    let bends = section["bendPoints"].as_array().into_iter().flatten();
    let line = once(&section["startPoint"])
        .chain(bends)
        .chain(once(&section["endPoint"]));
    let line: Vec<_> = line
        .map(|point| (number(point, "x"), number(point, "y")))
        .collect();
    let dashes = match physical {
        true => r#" stroke-dasharray="4 3""#,
        false => "",
    };
    writeln!(
        svg,
        r#"<polyline class="connection" points="{}" fill="none" stroke="black"{dashes}/>"#,
        points(&line)
    )
    .unwrap();
    for junction in edge["junctionPoints"].as_array().into_iter().flatten() {
        let (x, y) = (number(junction, "x"), number(junction, "y"));
        writeln!(svg, r#"<circle cx="{x}" cy="{y}" r="2"/>"#).unwrap();
    }
    for delay in edge["labels"].as_array().into_iter().flatten() {
        let (left, top) = (number(delay, "x"), number(delay, "y"));
        let (right, bottom) = (left + number(delay, "width"), top + number(delay, "height"));
        // The delay interrupts its line between two slanted strokes.
        let (slant, inner) = (2.0, PADDING / 2.0);
        let gap = points(&[
            (left + inner - slant, bottom),
            (left + inner + slant, top),
            (right - inner + slant, top),
            (right - inner - slant, bottom),
        ]);
        writeln!(svg, r#"<polygon points="{gap}" fill="white"/>"#).unwrap();
        for x in [left + inner, right - inner] {
            let stroke = points(&[(x - slant, bottom), (x + slant, top)]);
            writeln!(svg, r#"<polyline points="{stroke}" stroke="black"/>"#).unwrap();
        }
        let (center, middle) = ((left + right) / 2.0, (top + bottom) / 2.0);
        text(
            svg,
            center,
            middle,
            LABEL_SIZE,
            "middle",
            delay["text"].as_str().unwrap(),
        );
    }
}

/// Draw the diagram on a white frame.
pub(super) fn draw(diagram: &Diagram, laid_out: &Value) -> String {
    let (width, height) = (number(laid_out, "width"), number(laid_out, "height"));
    let mut svg = String::new();
    writeln!(
        svg,
        r#"<svg xmlns="http://www.w3.org/2000/svg" width="{}" height="{}" viewBox="0 0 {width} {height}" font-family="Helvetica, Arial, sans-serif">"#,
        width * SCALE,
        height * SCALE
    )
    .unwrap();
    writeln!(
        svg,
        r#"<rect x="0.5" y="0.5" width="{}" height="{}" rx="8" fill="white" stroke="{BORDER}"/>"#,
        width - 1.0,
        height - 1.0
    )
    .unwrap();
    // elkrs leaves out a list that is empty.
    for edge in laid_out["edges"].as_array().into_iter().flatten() {
        // The edge's id ends with the position of its connection.
        let position: usize = edge["id"].as_str().unwrap()[1..].parse().unwrap();
        draw_connection(&mut svg, edge, diagram.connections[position].physical);
    }
    let placed = laid_out["children"].as_array().into_iter().flatten();
    for (node_id, (node, placed)) in diagram.nodes.iter().zip(placed).enumerate() {
        draw_node(&mut svg, node_id, node, placed);
    }
    svg.push_str("</svg>\n");
    svg
}
