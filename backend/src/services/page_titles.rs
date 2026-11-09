//! Bulk admitted title projection for composite page DTOs. Never use a raw
//! semantic witness to label a currently readable but unrelated legacy atom.
use serde_json::Value;
use sqlx::{Postgres, Transaction};
use std::collections::{BTreeSet, HashMap};
use uuid::Uuid;
fn references(v: &Value, default_graph: Uuid, allowed: &[Uuid], out: &mut BTreeSet<(Uuid, Uuid)>) {
    match v {
        Value::Object(m) => {
            let graph = m
                .get("graph_id")
                .and_then(|g| g.as_str())
                .and_then(|g| g.parse().ok())
                .unwrap_or(default_graph);
            if let Some(node) = m
                .get("node_id")
                .and_then(|n| n.as_str())
                .and_then(|n| n.parse().ok())
            {
                if m.contains_key("title") && allowed.contains(&graph) {
                    out.insert((graph, node));
                }
            }
            for v in m.values() {
                references(v, graph, allowed, out)
            }
        }
        Value::Array(a) => {
            for v in a {
                references(v, default_graph, allowed, out)
            }
        }
        _ => {}
    }
}
fn apply(v: &mut Value, default_graph: Uuid, titles: &HashMap<(Uuid, Uuid), String>) {
    match v {
        Value::Object(m) => {
            let graph = m
                .get("graph_id")
                .and_then(|g| g.as_str())
                .and_then(|g| g.parse().ok())
                .unwrap_or(default_graph);
            if let Some(node) = m
                .get("node_id")
                .and_then(|n| n.as_str())
                .and_then(|n| n.parse().ok())
            {
                if m.contains_key("title") {
                    m.insert(
                        "title".into(),
                        Value::String(titles.get(&(graph, node)).cloned().unwrap_or_default()),
                    );
                }
            }
            for v in m.values_mut() {
                apply(v, graph, titles)
            }
        }
        Value::Array(a) => {
            for v in a {
                apply(v, default_graph, titles)
            }
        }
        _ => {}
    }
}
pub async fn project(
    conn: &mut Transaction<'_, Postgres>,
    graphs: &[Uuid],
    graph: Uuid,
    value: &mut Value,
) -> Result<(), sqlx::Error> {
    let mut refs = BTreeSet::new();
    references(value, graph, graphs, &mut refs);
    let (gs, ns): (Vec<_>, Vec<_>) = refs.into_iter().unzip();
    let titles = crate::services::journal_activities::project_node_titles(conn, &gs, &ns).await?;
    apply(value, graph, &titles);
    Ok(())
}
