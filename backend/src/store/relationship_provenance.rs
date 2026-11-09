//! Merge reasserted relationship support without rewriting a human decision.
//!
//! Callers must hold the database write/row lock from reading the existing edge
//! through writing this result and its sync op. This is shared by both stores.

use std::collections::HashSet;

use serde_json::Value;

use crate::relationships::EdgeStatus;

const MACHINE_REVIEW_KEYS: &[&str] = &[
    "auto_confirmed",
    "auto_resolved",
    "auto_resolution",
    "auto_resolved_at",
];
const HUMAN_REVIEW_KEYS: &[&str] = &["reviewed_by", "reviewed_at", "via"];

pub(super) fn is_durable_status(status: EdgeStatus, provenance: &Value) -> bool {
    (status == EdgeStatus::Rejected && provenance["lifecycle"].as_str()!=Some("unsupported"))
        || (status == EdgeStatus::Confirmed
            && provenance["auto_confirmed"] != Value::Bool(true)
            && provenance["auto_confirmed"].as_str() != Some("true"))
}

/// Atom support accumulates; replaying a support is idempotent. Other supplied
/// extraction metadata replaces the same key and absent independent fields are
/// retained. A rule and its source_edge_ids form one derivation: replacing
/// either replaces the pair, never unions dependencies from different rules.
/// Existing review attribution is reserved for the explicit review setter.
///
/// These are recorded supporting atom IDs, not a claim that all those atoms
/// remain accessible/current. Retraction and derivation invalidation are separate
/// lifecycle operations; this merge does not reconstruct previously overwritten
/// IDs from historical op_log snapshots.
pub(super) fn merge(existing: &Value, incoming: Value, status: EdgeStatus) -> Value {
    let old = existing.as_object().cloned().unwrap_or_default();
    let new = incoming.as_object().cloned().unwrap_or_default();
    let mut merged = old.clone();
    // Absence of auto_confirmed in a fresh evaluation means it was NOT
    // auto-confirmed. Retaining an old flag would break future status decisions.
    for key in MACHINE_REVIEW_KEYS {
        merged.remove(*key);
    }
    if new.contains_key("rule") || new.contains_key("source_edge_ids") {
        merged.remove("rule");
        merged.remove("source_edge_ids");
    }
    merged.extend(new.clone());
    if ["atom_ids", "semantic_atom_ids"]
        .iter()
        .any(|key| new.get(*key).and_then(Value::as_array).is_some_and(|ids| !ids.is_empty()))
    {
        // Fresh evidence can reopen an unsupported machine invalidation;
        // a deliberate human rejection remains durable via the status gate.
        merged.remove("lifecycle");
    }

    // Chunk supports (legacy writers, extraction v2's bridge) and semantic
    // supports (extraction v2) both accumulate across reassertions.
    for key in ["atom_ids", "semantic_atom_ids"] {
        if !old.contains_key(key) && !new.contains_key(key) {
            continue;
        }
        let mut seen = HashSet::new();
        let mut atoms = Vec::new();
        for source in [&old, &new] {
            // Older/imported records may have a scalar id or a null field.
            // Treat a string as one support, and null as no recorded supports.
            let ids = match source.get(key) {
                Some(Value::Array(ids)) => ids.clone(),
                Some(id @ Value::String(_)) => vec![id.clone()],
                _ => Vec::new(),
            };
            for id in ids {
                if seen.insert(id.to_string()) {
                    atoms.push(id);
                }
            }
        }
        merged.insert(key.into(), Value::Array(atoms));
    }
    // Extraction v2's per-statement validity history: one entry per semantic
    // atom, the newest statement of the same atom winning, the most recent
    // MAX_EDGE_ASSERTIONS kept. Supersession (plan PR 9) reads it.
    if old.contains_key("assertions") || new.contains_key("assertions") {
        let mut assertions: Vec<Value> = Vec::new();
        for source in [&old, &new] {
            for entry in source.get("assertions").and_then(Value::as_array).into_iter().flatten() {
                let id = entry.get("semantic_atom_id");
                assertions.retain(|kept| kept.get("semantic_atom_id") != id);
                assertions.push(entry.clone());
            }
        }
        let max = crate::workers::source_atoms::v2::MAX_EDGE_ASSERTIONS;
        if assertions.len() > max {
            assertions.drain(..assertions.len() - max);
        }
        merged.insert("assertions".into(), Value::Array(assertions));
    }
    for key in HUMAN_REVIEW_KEYS {
        if let Some(value) = old.get(*key) {
            merged.insert((*key).into(), value.clone());
        }
    }
    if is_durable_status(status, existing) {
        // Status stays with the durable decision, so its machine/human
        // attribution must stay with it too. A fresh inference must not turn a
        // human-confirmed edge into an auto-confirmed (therefore revisable) one.
        for key in MACHINE_REVIEW_KEYS {
            merged.remove(*key);
            if let Some(value) = old.get(*key) {
                merged.insert((*key).into(), value.clone());
            }
        }
    }
    Value::Object(merged)
}

/// Insert first; on collision lock the persisted row before merging. The insert
/// waits for an in-flight insert of the same natural key, and SELECT FOR UPDATE
/// observes its committed value under Postgres READ COMMITTED. The caller's
/// transaction also owns the final update and op_log append. Concurrent replay,
/// extraction and review therefore cannot race a stale read/overwrite.
#[allow(clippy::too_many_arguments)]
pub(super) async fn upsert_pg(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    graph_id: uuid::Uuid,
    src_node_id: uuid::Uuid,
    predicate: &str,
    dst_node_id: uuid::Uuid,
    origin: crate::relationships::EdgeOrigin,
    status: EdgeStatus,
    confidence: f32,
    provenance: Value,
) -> Result<crate::relationships::RelationshipEdge, super::StoreError> {
    use super::{relationship_edge_from_row, RelationshipEdgeRow};

    let provenance = merge(&Value::Null, provenance, EdgeStatus::Suggested);
    let inserted: Option<RelationshipEdgeRow> = sqlx::query_as(
        r#"
        INSERT INTO relationship_edge
            (graph_id, src_node_id, predicate, dst_node_id,
             origin, status, confidence, provenance, queued_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8,
                CASE WHEN $6 = 'suggested' THEN now() END)
        ON CONFLICT (graph_id, src_node_id, predicate, dst_node_id) DO NOTHING
        RETURNING edge_id, graph_id, src_node_id, predicate, dst_node_id,
                  origin, status, confidence, provenance,
                  created_at, updated_at, version
        "#,
    )
    .bind(graph_id)
    .bind(src_node_id)
    .bind(predicate)
    .bind(dst_node_id)
    .bind(origin.as_str())
    .bind(status.as_str())
    .bind(confidence)
    .bind(&provenance)
    .fetch_optional(&mut **tx)
    .await?;
    if let Some(row) = inserted {
        return Ok(relationship_edge_from_row(row));
    }

    let existing: RelationshipEdgeRow = sqlx::query_as(
        r#"
        SELECT edge_id, graph_id, src_node_id, predicate, dst_node_id,
               origin, status, confidence, provenance,
               created_at, updated_at, version
          FROM relationship_edge
         WHERE graph_id = $1 AND src_node_id = $2 AND predicate = $3 AND dst_node_id = $4
         FOR UPDATE
        "#,
    )
    .bind(graph_id)
    .bind(src_node_id)
    .bind(predicate)
    .bind(dst_node_id)
    .fetch_one(&mut **tx)
    .await?;
    let existing = relationship_edge_from_row(existing);
    let merged_status = if is_durable_status(existing.status, &existing.provenance) {
        existing.status
    } else {
        status
    };
    let merged = merge(&existing.provenance, provenance, existing.status);
    let row: RelationshipEdgeRow = sqlx::query_as(
        r#"
        UPDATE relationship_edge
           SET origin = $2, status = $3, confidence = $4, provenance = $5,
               -- Preserve the original queue clock except when a machine-
               -- confirmed edge is revised back into the review queue (0127).
               queued_at = CASE WHEN $3 = 'suggested' AND status = 'confirmed'
                                      AND provenance->>'auto_confirmed' = 'true'
                                THEN now() ELSE queued_at END,
               updated_at = now(), version = version + 1
         WHERE edge_id = $1
        RETURNING edge_id, graph_id, src_node_id, predicate, dst_node_id,
                  origin, status, confidence, provenance,
                  created_at, updated_at, version
        "#,
    )
    .bind(existing.edge_id)
    .bind(origin.as_str())
    .bind(merged_status.as_str())
    .bind(confidence)
    .bind(merged)
    .fetch_one(&mut **tx)
    .await?;
    Ok(relationship_edge_from_row(row))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn machine_reevaluation_refreshes_flags_and_derivation_as_a_pair() {
        let merged = merge(
            &json!({"auto_confirmed": true, "rule": "old", "source_edge_ids": ["old-edge"], "atom_ids": ["a"], "independent": 4}),
            json!({"rule": "new", "atom_ids": ["b", "a"]}),
            EdgeStatus::Confirmed,
        );
        assert!(merged.get("auto_confirmed").is_none());
        assert!(merged.get("source_edge_ids").is_none());
        assert_eq!(merged["rule"], "new");
        assert_eq!(merged["atom_ids"], json!(["a", "b"]));
        assert_eq!(merged["independent"], 4);
    }

    #[test]
    fn semantic_supports_and_assertions_accumulate_across_reassertion() {
        let first = merge(
            &Value::Null,
            json!({"semantic_atom_ids": ["s1"], "atom_ids": ["c1"],
                   "assertions": [{"semantic_atom_id": "s1", "valid_until": null}]}),
            EdgeStatus::Confirmed,
        );
        let second = merge(
            &first,
            json!({"semantic_atom_ids": ["s2"], "atom_ids": ["c1", "c2"],
                   "assertions": [{"semantic_atom_id": "s2", "valid_until": "2026-12-31T00:00:00Z"}]}),
            EdgeStatus::Confirmed,
        );
        assert_eq!(second["semantic_atom_ids"], json!(["s1", "s2"]));
        assert_eq!(second["atom_ids"], json!(["c1", "c2"]));
        assert_eq!(second["assertions"].as_array().unwrap().len(), 2);
        // Replaying the same statement is idempotent; a re-extracted atom id replaces its entry.
        let replay = merge(&second, json!({"semantic_atom_ids": ["s2"],
            "assertions": [{"semantic_atom_id": "s2", "valid_until": "2027-01-31T00:00:00Z"}]}), EdgeStatus::Confirmed);
        assert_eq!(replay["semantic_atom_ids"], json!(["s1", "s2"]));
        assert_eq!(replay["assertions"], json!([{"semantic_atom_id": "s1", "valid_until": null},
            {"semantic_atom_id": "s2", "valid_until": "2027-01-31T00:00:00Z"}]));
        // Fresh semantic support reopens a machine invalidation, like chunk support.
        let reopened = merge(&json!({"lifecycle": "unsupported"}), json!({"semantic_atom_ids": ["s3"]}), EdgeStatus::Suggested);
        assert!(reopened.get("lifecycle").is_none());
        let many: Vec<Value> = (0..40).map(|i| json!({"semantic_atom_id": format!("s{i}")})).collect();
        let capped = merge(&Value::Null, json!({"assertions": many}), EdgeStatus::Confirmed);
        assert_eq!(capped["assertions"].as_array().unwrap().len(), crate::workers::source_atoms::v2::MAX_EDGE_ASSERTIONS);
        assert_eq!(capped["assertions"][0]["semantic_atom_id"], "s8");
    }

    #[test]
    fn review_nulls_and_legacy_atom_shapes_survive_reassertion() {
        let existing =
            json!({"atom_ids": "a", "reviewed_by": null, "reviewed_at": "then", "via": "chat"});
        let result = merge(
            &existing,
            json!({"atom_ids": ["a", "b"], "reviewed_by": "machine", "auto_confirmed": true}),
            EdgeStatus::Confirmed,
        );
        assert_eq!(result["atom_ids"], json!(["a", "b"]));
        assert_eq!(result["reviewed_by"], Value::Null);
        assert_eq!(result["reviewed_at"], "then");
        assert!(result.get("auto_confirmed").is_none());
        assert_eq!(
            merge(
                &Value::Null,
                json!({"atom_ids": ["a"]}),
                EdgeStatus::Suggested
            )["atom_ids"],
            json!(["a"])
        );
    }
}
