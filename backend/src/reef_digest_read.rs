//! The attributed read path for reef digests (`nodes.body.digest`).
//!
//! #1225 fenced every stored digest out of the generic node reads
//! (`get_node_digest_scoped`, `node_digest_context_scoped`) because a digest
//! is derived text with no evidence receipt: it was written once, by a
//! system-scoped worker, from up to 30 corals, and nothing re-checked that a
//! later reader may still see every one of them. A coral whose source was
//! revoked or lost, retracted, or otherwise not permitted to the reader would
//! keep leaking through the summary and its per-coral excerpts.
//!
//! Since #1352 every digest names the `managed_enrichment` receipt that
//! published it (`body.digest.managed_receipt`), and that receipt's
//! `managed_enrichment_inputs` rows bind every input the writer's prompt
//! read: each coral `(atom, id, content_revision, sha256)`, each established
//! edge line `(digest_edge, edge_id, 0, sha256(line))`, and the reef marker
//! `(digest_node, node_id, 0, sha256(title))`. This module serves a digest
//! only when, inside the READER's own scoped transaction:
//!
//! 1. the reef is Library-visible to the reader (`library_node_title`);
//! 2. the receipt exists, is this graph's `managed_enrichment` receipt and
//!    was billed (the call was actually served), and every one of its inputs
//!    is `written` (none `invalidated` by a deletion or retraction);
//! 3. every bound coral is still in the reef's graph, still linked to the
//!    reef, at the same `content_revision`, and returned by
//!    `library_permitted_atoms` for this reader;
//! 4. the reef title and every bound edge line are byte-identical to what
//!    the reader sees now (so a related reef whose title is now hidden, or a
//!    rejected edge, withholds the digest);
//! 5. every excerpt names a bound coral.
//!
//! Anything else is `unavailable`, and the caller may queue one deduped
//! regeneration. Served digests always carry their attribution: receipt id,
//! model, generation time and the number of corals read.
//!
//! Ask does NOT read this: stored digests stay out of model evidence
//! (`docs/implementation/scoped-ask-api.md`).

use std::collections::{HashMap, HashSet};

use chrono::{DateTime, Utc};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use sqlx::{Postgres, Transaction};
use uuid::Uuid;

use crate::workers::digest::{current_edge_lines, INPUT_ATOM, INPUT_EDGE, INPUT_NODE};

/// Who generated a served digest, and from how much.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Attribution {
    pub receipt_id: Uuid,
    pub model: String,
    pub generated_at: DateTime<Utc>,
    /// Corals the writer read (every one re-verified for this reader).
    pub input_count: usize,
}

/// A digest this reader may be shown.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct ServedDigest {
    pub summary: Option<String>,
    pub excerpts: Value,
    pub attribution: Attribution,
}

impl ServedDigest {
    /// The one JSON shape both surfaces return (MCP `get_reef.digest`, and
    /// `GET /v1/nodes/{id}/digest` plus its `cached: true`).
    pub(crate) fn to_json(&self) -> Value {
        json!({
            "summary": self.summary,
            "excerpts": self.excerpts,
            "attribution": {
                "receipt_id": self.attribution.receipt_id,
                "model": self.attribution.model,
                "generated_at": self.attribution.generated_at,
                "input_count": self.attribution.input_count,
                "purpose": "managed_enrichment",
            },
        })
    }
}

/// Why a stored digest was not served. Internal only: responses say
/// `unavailable`, never which coral or why, so a withheld digest does not
/// reveal what the reader cannot see.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Withheld {
    /// No digest is stored.
    NotGenerated,
    /// The stored digest names no receipt, or one that does not attribute it
    /// (legacy pre-#1352 digests, foreign or unbilled receipts, unknown input
    /// kinds, excerpts outside the inputs).
    Unattributed,
    /// An input changed since generation (edited or unlinked coral, renamed
    /// reef, changed edge) or was invalidated by a deletion or retraction.
    Stale,
    /// An input coral, edge or related reef title is not permitted to this
    /// reader.
    NotPermitted,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum DigestOutcome {
    Served(ServedDigest),
    Unavailable(Withheld),
}

/// One reader's view of a reef's digest.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct ReaderDigest {
    pub graph_id: Uuid,
    pub node_type: String,
    /// Corals linked to the reef that the Library permits to this reader.
    pub coral_count: i64,
    pub outcome: DigestOutcome,
}

impl ReaderDigest {
    /// Whether a miss should queue a background regeneration
    /// (`workers::digest::queue_regeneration`): the reef is eligible and
    /// digests are switched on. Deduped on `digest:{node}` and
    /// bounded per graph per day by the worker, so a repeated read costs no
    /// more than one pass.
    pub(crate) fn should_regenerate(&self) -> bool {
        matches!(self.outcome, DigestOutcome::Unavailable(_))
            && crate::workers::digest::digest_eligible(&self.node_type, self.coral_count)
            && crate::workers::digest::digests_claim_allowed()
    }

    /// The MCP `get_reef.digest` value: the served digest, else `null`.
    pub(crate) fn mcp_value(&self) -> Value {
        match &self.outcome {
            DigestOutcome::Served(d) => d.to_json(),
            DigestOutcome::Unavailable(_) => Value::Null,
        }
    }

    /// The `GET /v1/nodes/{id}/digest` body. `regenerating` is whether a
    /// regeneration was queued for this miss.
    pub(crate) fn http_body(&self, regenerating: bool) -> Value {
        match &self.outcome {
            DigestOutcome::Served(d) => {
                let mut body = d.to_json();
                body["cached"] = Value::Bool(true);
                body
            }
            DigestOutcome::Unavailable(Withheld::NotGenerated) if self.coral_count == 0 => {
                json!({"summary": Value::Null, "excerpts": []})
            }
            DigestOutcome::Unavailable(_) => json!({
                "summary": Value::Null,
                "excerpts": [],
                "unavailable": true,
                "regenerating": regenerating,
            }),
        }
    }
}

/// Read `node_id`'s digest for the reader `tx` is scoped to. `None` when the
/// reef is not Library-visible to the reader (callers answer not-found).
/// Writes nothing.
pub(crate) async fn read_for_reader(
    tx: &mut Transaction<'_, Postgres>,
    node_id: Uuid,
) -> Result<Option<ReaderDigest>, sqlx::Error> {
    let row: Option<(Uuid, String, Option<String>, Option<Value>, i64)> = sqlx::query_as(
        "SELECT n.graph_id, n.type, library_node_title(n.graph_id,n.node_id), n.body->'digest', \
                (SELECT count(*) FROM library_permitted_atoms(n.graph_id, \
                   ARRAY(SELECT l.atom_id FROM node_atoms l WHERE l.node_id=n.node_id))) \
           FROM nodes n WHERE n.node_id=$1",
    )
    .bind(node_id)
    .fetch_optional(&mut **tx)
    .await?;
    let Some((graph_id, node_type, Some(title), digest, coral_count)) = row else {
        return Ok(None);
    };
    let outcome = match digest.filter(|d| d.is_object()) {
        None => DigestOutcome::Unavailable(Withheld::NotGenerated),
        Some(digest) => verify(tx, graph_id, node_id, &title, &digest).await?,
    };
    Ok(Some(ReaderDigest { graph_id, node_type, coral_count, outcome }))
}

type InputRow = (String, Uuid, i64, Vec<u8>, String);

async fn verify(
    tx: &mut Transaction<'_, Postgres>,
    graph_id: Uuid,
    node_id: Uuid,
    title: &str,
    digest: &Value,
) -> Result<DigestOutcome, sqlx::Error> {
    use DigestOutcome::Unavailable;
    let Some(receipt_id) = digest
        .get("managed_receipt")
        .and_then(Value::as_str)
        .and_then(|s| s.parse::<Uuid>().ok())
    else {
        return Ok(Unavailable(Withheld::Unattributed));
    };
    let receipt: Option<(Uuid, Option<String>, Vec<String>, DateTime<Utc>, String)> = sqlx::query_as(
        "SELECT graph_id, served_model, candidate_models, COALESCE(settled_at, created_at), state \
           FROM managed_attempt_receipts WHERE receipt_id=$1 AND purpose='managed_enrichment'",
    )
    .bind(receipt_id)
    .fetch_optional(&mut **tx)
    .await?;
    let Some((receipt_graph, served_model, candidates, generated_at, state)) = receipt else {
        return Ok(Unavailable(Withheld::Unattributed));
    };
    if receipt_graph != graph_id || !matches!(state.as_str(), "billed" | "overrun" | "ambiguous") {
        return Ok(Unavailable(Withheld::Unattributed));
    }
    let inputs: Vec<InputRow> = sqlx::query_as(
        "SELECT input_kind, input_id, content_revision, content_sha256, outcome \
           FROM managed_enrichment_inputs WHERE receipt_id=$1 AND graph_id=$2 ORDER BY ordinal",
    )
    .bind(receipt_id)
    .bind(graph_id)
    .fetch_all(&mut **tx)
    .await?;

    let mut atoms: Vec<(Uuid, i64)> = Vec::new();
    let mut edges: Vec<(Uuid, Vec<u8>)> = Vec::new();
    let mut marker: Option<Vec<u8>> = None;
    for (kind, id, revision, sha, _) in &inputs {
        match kind.as_str() {
            INPUT_ATOM => atoms.push((*id, *revision)),
            INPUT_EDGE => edges.push((*id, sha.clone())),
            INPUT_NODE if *id == node_id && marker.is_none() => marker = Some(sha.clone()),
            _ => return Ok(Unavailable(Withheld::Unattributed)),
        }
    }
    let Some(marker) = marker else {
        return Ok(Unavailable(Withheld::Unattributed));
    };
    if atoms.is_empty() {
        return Ok(Unavailable(Withheld::Unattributed));
    }
    let atom_ids: Vec<Uuid> = atoms.iter().map(|(id, _)| *id).collect();
    let bound: HashSet<Uuid> = atom_ids.iter().copied().collect();
    let excerpts = digest.get("excerpts").cloned().unwrap_or_else(|| json!([]));
    let Some(excerpt_list) = excerpts.as_array() else {
        return Ok(Unavailable(Withheld::Unattributed));
    };
    let excerpts_bound = excerpt_list.iter().all(|e| {
        e.get("atom_id")
            .and_then(Value::as_str)
            .and_then(|s| s.parse::<Uuid>().ok())
            .is_some_and(|id| bound.contains(&id))
    });
    if !excerpts_bound {
        return Ok(Unavailable(Withheld::Unattributed));
    }

    // Permission first: a coral the reader may not see withholds the digest
    // whatever else holds.
    let visible: HashMap<Uuid, (Uuid, i64)> = sqlx::query_as::<_, (Uuid, Uuid, i64)>(
        "SELECT atom_id, graph_id, content_revision FROM atoms WHERE atom_id = ANY($1)",
    )
    .bind(&atom_ids)
    .fetch_all(&mut **tx)
    .await?
    .into_iter()
    .map(|(id, g, rev)| (id, (g, rev)))
    .collect();
    let permitted: HashSet<Uuid> =
        sqlx::query_scalar::<_, Uuid>("SELECT atom_id FROM library_permitted_atoms($1, $2)")
            .bind(graph_id)
            .bind(&atom_ids)
            .fetch_all(&mut **tx)
            .await?
            .into_iter()
            .collect();
    for id in &atom_ids {
        // Absent is either deleted (whose deletion trigger normally strips the
        // digest first) or invisible to this reader: withheld either way.
        let in_graph = visible.get(id).is_some_and(|(g, _)| *g == graph_id);
        if !in_graph || !permitted.contains(id) {
            return Ok(Unavailable(Withheld::NotPermitted));
        }
    }
    let edge_ids: Vec<Uuid> = edges.iter().map(|(id, _)| *id).collect();
    let edges_now = current_edge_lines(&mut **tx, graph_id, node_id, title, &edge_ids).await?;
    for (id, sha) in &edges {
        match edges_now.get(id) {
            None => return Ok(Unavailable(Withheld::NotPermitted)),
            Some(line) if Sha256::digest(line.as_bytes()).as_slice() != sha.as_slice() => {
                return Ok(Unavailable(Withheld::Stale))
            }
            Some(_) => {}
        }
    }

    // Staleness: every input as generated.
    if inputs.iter().any(|row| row.4 != "written") {
        return Ok(Unavailable(Withheld::Stale));
    }
    if Sha256::digest(title.as_bytes()).as_slice() != marker.as_slice() {
        return Ok(Unavailable(Withheld::Stale));
    }
    if atoms.iter().any(|(id, rev)| visible.get(id).map(|(_, now)| (*now).max(0)) != Some(*rev)) {
        return Ok(Unavailable(Withheld::Stale));
    }
    let linked: i64 = sqlx::query_scalar(
        "SELECT count(DISTINCT atom_id) FROM node_atoms WHERE node_id=$1 AND atom_id = ANY($2)",
    )
    .bind(node_id)
    .bind(&atom_ids)
    .fetch_one(&mut **tx)
    .await?;
    if linked != bound.len() as i64 {
        return Ok(Unavailable(Withheld::Stale));
    }

    let model = served_model
        .filter(|m| !m.is_empty())
        .or_else(|| candidates.into_iter().next())
        .unwrap_or_default();
    Ok(DigestOutcome::Served(ServedDigest {
        summary: digest.get("summary").and_then(Value::as_str).map(str::to_string),
        excerpts,
        attribution: Attribution { receipt_id, model, generated_at, input_count: bound.len() },
    }))
}
