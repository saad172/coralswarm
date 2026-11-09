//! `op_log` payload constructors (ADR-0005).
//!
//! Every content mutation appends an op whose payload is built here: JSON with
//! string-encoded UUIDs / RFC-3339 timestamps, and embeddings as base64
//! little-endian `f32`. `op_log` delete ops are the 30-day tombstones
//! `Store::purge_expired_tombstones` sweeps.

use super::{NewOp, OpRecord, OpType, OP_ENTITY_ATOM, OP_ENTITY_NODE};
use super::{
    OP_ENTITY_NODE_ATOM, OP_ENTITY_NON_SUPERSESSION_PAIR, OP_ENTITY_RELATIONSHIP,
    OP_ENTITY_SUPERSESSION,
};
use base64::{engine::general_purpose::STANDARD, Engine};
use chrono::{DateTime, Utc};
use serde_json::{json, Value};
use uuid::Uuid;

// ── Embedding codec ─────────────────────────────────────────────────────────

/// Encode an embedding as base64 of its little-endian `f32` bytes (~1.5 KB for a
/// 384-d vector).
pub(crate) fn encode_embedding(v: &[f32]) -> String {
    let mut bytes = Vec::with_capacity(v.len() * 4);
    for x in v {
        bytes.extend_from_slice(&x.to_le_bytes());
    }
    STANDARD.encode(bytes)
}

/// True iff every component of `v` is exactly `0.0` — the signature of a
/// zero/stub embedder rather than a real (L2-normalised, so never all-zero
/// unless the input norm was already zero) model output. `Store::set_atom_embeddings`
/// rejects vectors matching this as defense in depth: a stub-embedder fallback
/// previously persisted zero vectors into production silently, corrupting
/// cosine similarity search for every atom it touched.
pub(crate) fn is_zero_vector(v: &[f32]) -> bool {
    v.iter().all(|x| *x == 0.0)
}

// ── Payload field names ───────────────────────────────────────────────────────

pub(crate) mod field {
    pub const ATOM_ID: &str = "atom_id";
    pub const GRAPH_ID: &str = "graph_id";
    pub const SOURCE_TYPE: &str = "source_type";
    pub const SOURCE_ID: &str = "source_id";
    pub const CONTENT_BLOB_KEY: &str = "content_blob_key";
    pub const CONTENT_INLINE: &str = "content_inline";
    pub const EMBEDDING: &str = "embedding";
    /// Embedding-model identifier that produced `EMBEDDING` (mirrors Postgres
    /// Migration 0063's `atoms.embed_model`); see `coralswarm_core::embed::EMBED_MODEL_NAME`.
    pub const EMBED_MODEL: &str = "embed_model";
    /// Chunker version that produced the atom being embedded (mirrors
    /// `atoms.chunker_version`); see `coralswarm_core::chunker::CHUNKER_VERSION`.
    pub const CHUNKER_VERSION: &str = "chunker_version";
    pub const PARTICIPANTS: &str = "participants";
    pub const TIMESTAMP: &str = "timestamp";
    pub const INGESTED_BY: &str = "ingested_by";
    pub const SCOPE: &str = "scope";
    pub const FTS_LANGUAGE: &str = "fts_language";
    pub const CREATED_AT: &str = "created_at";

    pub const NODE_ID: &str = "node_id";
    pub const TYPE: &str = "type";
    pub const TITLE: &str = "title";
    pub const BODY: &str = "body";
    pub const OWNER_ID: &str = "owner_id";
    pub const UPDATED_AT: &str = "updated_at";
    pub const VERSION: &str = "version";

    pub const SUPERSEDER_ID: &str = "superseder_id";
    pub const SUPERSEDED_ID: &str = "superseded_id";
    pub const EDGE_SOURCE: &str = "source";

    // Non-supersession pairs (D3/§3.7).
    pub const NODE_A_ID: &str = "node_a_id";
    pub const NODE_B_ID: &str = "node_b_id";
    pub const MARKED_BY: &str = "marked_by";
    pub const MARKED_AT: &str = "marked_at";

    // Relationship edges (ADR-0008). `GRAPH_ID`/`CREATED_AT`/`UPDATED_AT`/
    // `VERSION` above are reused.
    pub const EDGE_ID: &str = "edge_id";
    pub const SRC_NODE_ID: &str = "src_node_id";
    pub const PREDICATE: &str = "predicate";
    pub const DST_NODE_ID: &str = "dst_node_id";
    pub const ORIGIN: &str = "origin";
    pub const STATUS: &str = "status";
    pub const CONFIDENCE: &str = "confidence";
    pub const PROVENANCE: &str = "provenance";
}

// ── Payload getters (used by `apply_remote_op`) ───────────────────────────────

// ── Payload builders (used when recording ops) ────────────────────────────────

/// Build a full atom-row snapshot payload (the shared shape used by both the
/// create op and the delete tombstone). `content_inline`/`embedding` are
/// included only when present.
///
/// `embed_model`/`chunker_version` are the attribution for `embedding` —
/// callers MUST pass them together with `embedding` (or both `None`) so a
/// restore/apply never resurrects a vector without knowing which model
/// produced it (that combination gets silently excluded by
/// `vector_search`'s model filter — see migration 0063) or, worse, stamps
/// attribution on a row that has no vector at all.
#[allow(clippy::too_many_arguments)]
pub(crate) fn atom_snapshot_payload(
    atom_id: Uuid,
    graph_id: Uuid,
    source_type: &str,
    source_id: &str,
    content_blob_key: &str,
    content_inline: Option<&str>,
    embedding: Option<&[f32]>,
    embed_model: Option<&str>,
    chunker_version: Option<i16>,
    participants: &[String],
    timestamp: DateTime<Utc>,
    ingested_by: Option<Uuid>,
    scope: &str,
    fts_language: &str,
    created_at: DateTime<Utc>,
) -> Value {
    json!({
        field::ATOM_ID: atom_id.to_string(),
        field::GRAPH_ID: graph_id.to_string(),
        field::SOURCE_TYPE: source_type,
        field::SOURCE_ID: source_id,
        field::CONTENT_BLOB_KEY: content_blob_key,
        field::CONTENT_INLINE: content_inline,
        field::EMBEDDING: embedding.map(encode_embedding),
        field::EMBED_MODEL: embed_model,
        field::CHUNKER_VERSION: chunker_version,
        field::PARTICIPANTS: participants,
        field::TIMESTAMP: timestamp.to_rfc3339(),
        field::INGESTED_BY: ingested_by.map(|u| u.to_string()),
        field::SCOPE: scope,
        field::FTS_LANGUAGE: fts_language,
        field::CREATED_AT: created_at.to_rfc3339(),
    })
}

/// Build the `NewOp` for a freshly-created atom (coral). Embedding is filled in
/// later by [`atom_embedding_op`]; content_inline may be `None` at creation.
#[allow(clippy::too_many_arguments)]
pub(crate) fn atom_create_op(
    op_id: Uuid,
    atom_id: Uuid,
    graph_id: Uuid,
    source_type: &str,
    source_id: &str,
    content_blob_key: &str,
    participants: &[String],
    timestamp: DateTime<Utc>,
    ingested_by: Option<Uuid>,
    scope: &str,
    fts_language: &str,
    created_at: DateTime<Utc>,
    device_id: &str,
) -> NewOp {
    let payload = atom_snapshot_payload(
        atom_id,
        graph_id,
        source_type,
        source_id,
        content_blob_key,
        None,
        None,
        None,
        None,
        participants,
        timestamp,
        ingested_by,
        scope,
        fts_language,
        created_at,
    );
    NewOp {
        op_id,
        entity: OP_ENTITY_ATOM.into(),
        entity_id: atom_id.to_string(),
        op_type: OpType::Upsert,
        payload,
        version: 0,
        updated_at: created_at,
        device_id: device_id.into(),
        graph_id: Some(graph_id),
    }
}

/// Build an enrichment op carrying just an atom's embedding, plus which
/// model/chunker version produced it.
#[allow(clippy::too_many_arguments)]
pub(crate) fn atom_embedding_op(
    op_id: Uuid,
    atom_id: Uuid,
    graph_id: Uuid,
    embedding: &[f32],
    embed_model: &str,
    chunker_version: i16,
    updated_at: DateTime<Utc>,
    device_id: &str,
) -> NewOp {
    let payload = json!({
        field::ATOM_ID: atom_id.to_string(),
        field::GRAPH_ID: graph_id.to_string(),
        field::EMBEDDING: encode_embedding(embedding),
        field::EMBED_MODEL: embed_model,
        field::CHUNKER_VERSION: chunker_version,
    });
    NewOp {
        op_id,
        entity: OP_ENTITY_ATOM.into(),
        entity_id: atom_id.to_string(),
        op_type: OpType::Upsert,
        payload,
        version: 0,
        updated_at,
        device_id: device_id.into(),
        graph_id: Some(graph_id),
    }
}

/// Build an enrichment op carrying just an atom's inline content (merged on apply).
pub(crate) fn atom_content_op(
    op_id: Uuid,
    atom_id: Uuid,
    graph_id: Uuid,
    content: &str,
    updated_at: DateTime<Utc>,
    device_id: &str,
) -> NewOp {
    let payload = json!({
        field::ATOM_ID: atom_id.to_string(),
        field::GRAPH_ID: graph_id.to_string(),
        field::CONTENT_INLINE: content,
    });
    NewOp {
        op_id,
        entity: OP_ENTITY_ATOM.into(),
        entity_id: atom_id.to_string(),
        op_type: OpType::Upsert,
        payload,
        version: 0,
        updated_at,
        device_id: device_id.into(),
        graph_id: Some(graph_id),
    }
}

/// Build a tombstone op for a deleted atom, carrying its last-known snapshot so
/// the cloud's 30-day trash can restore it.
pub(crate) fn atom_delete_op(
    op_id: Uuid,
    snapshot: Value,
    atom_id: Uuid,
    graph_id: Uuid,
    updated_at: DateTime<Utc>,
    device_id: &str,
) -> NewOp {
    NewOp {
        op_id,
        entity: OP_ENTITY_ATOM.into(),
        entity_id: atom_id.to_string(),
        op_type: OpType::Delete,
        payload: snapshot,
        version: 0,
        updated_at,
        device_id: device_id.into(),
        graph_id: Some(graph_id),
    }
}

/// Build an upsert op for a reef (node), carrying the full row.
#[allow(clippy::too_many_arguments)]
pub(crate) fn node_upsert_op(
    op_id: Uuid,
    node_id: Uuid,
    graph_id: Uuid,
    node_type: &str,
    title: &str,
    body: &str,
    scope: &str,
    owner_id: Option<Uuid>,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
    version: i64,
    device_id: &str,
) -> NewOp {
    let payload = json!({
        field::NODE_ID: node_id.to_string(),
        field::GRAPH_ID: graph_id.to_string(),
        field::TYPE: node_type,
        field::TITLE: title,
        field::BODY: body,
        field::SCOPE: scope,
        field::OWNER_ID: owner_id.map(|u| u.to_string()),
        field::CREATED_AT: created_at.to_rfc3339(),
        field::UPDATED_AT: updated_at.to_rfc3339(),
        field::VERSION: version,
    });
    NewOp {
        op_id,
        entity: OP_ENTITY_NODE.into(),
        entity_id: node_id.to_string(),
        op_type: OpType::Upsert,
        payload,
        version,
        updated_at,
        device_id: device_id.into(),
        graph_id: Some(graph_id),
    }
}

/// Build a tombstone op for a deleted reef (node), carrying its last-known
/// snapshot (same shape as `node_upsert_op`'s payload) so the trash can
/// restore it. Used by the merge primitive,
/// which deletes the duplicate node.
#[allow(clippy::too_many_arguments)]
pub(crate) fn node_delete_op(
    op_id: Uuid,
    node_id: Uuid,
    graph_id: Uuid,
    node_type: &str,
    title: &str,
    body: &str,
    scope: &str,
    owner_id: Option<Uuid>,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
    version: i64,
    device_id: &str,
) -> NewOp {
    let payload = json!({
        field::NODE_ID: node_id.to_string(),
        field::GRAPH_ID: graph_id.to_string(),
        field::TYPE: node_type,
        field::TITLE: title,
        field::BODY: body,
        field::SCOPE: scope,
        field::OWNER_ID: owner_id.map(|u| u.to_string()),
        field::CREATED_AT: created_at.to_rfc3339(),
        field::UPDATED_AT: updated_at.to_rfc3339(),
        field::VERSION: version,
    });
    NewOp {
        op_id,
        entity: OP_ENTITY_NODE.into(),
        entity_id: node_id.to_string(),
        op_type: OpType::Delete,
        payload,
        version,
        updated_at,
        device_id: device_id.into(),
        graph_id: Some(graph_id),
    }
}

/// Build an op linking (or, for `Delete`, unlinking) an atom to a node. Used
/// by callers that own the node/atom-link write path — the cloud extract
/// worker (through `Store::link_node_atom`).
pub(crate) fn node_atom_op(
    op_id: Uuid,
    node_id: Uuid,
    atom_id: Uuid,
    graph_id: Uuid,
    op_type: OpType,
    updated_at: DateTime<Utc>,
    device_id: &str,
) -> NewOp {
    let payload = json!({
        field::NODE_ID: node_id.to_string(),
        field::ATOM_ID: atom_id.to_string(),
        field::GRAPH_ID: graph_id.to_string(),
    });
    NewOp {
        op_id,
        entity: OP_ENTITY_NODE_ATOM.into(),
        entity_id: super::compose_id(node_id, atom_id),
        op_type,
        payload,
        version: 0,
        updated_at,
        device_id: device_id.into(),
        graph_id: Some(graph_id),
    }
}

/// Build an upsert op that restores an entity from a tombstone (a delete op
/// whose `payload` carries the last-known snapshot). The restore carries a
/// fresh `op_id` + `updated_at` (so it strictly wins LWW against the
/// tombstone) and a `version` one beyond the tombstone's, while preserving
/// the entity identity. Returns `None` when `tombstone` is not a delete.
///
/// Used by the cloud's premium "Restore from trash" flow within the
/// 30-day retention window (`TOMBSTONE_RETENTION_DAYS`).
// Unused until the "Restore from trash" route ships — see doc comment above.
#[allow(dead_code)]
pub fn restore_op_from_tombstone(tombstone: &OpRecord, device_id: &str) -> Option<NewOp> {
    if tombstone.op_type != OpType::Delete {
        return None;
    }
    Some(NewOp {
        op_id: Uuid::new_v4(),
        entity: tombstone.entity.clone(),
        entity_id: tombstone.entity_id.clone(),
        op_type: OpType::Upsert,
        payload: tombstone.payload.clone(),
        version: tombstone.version.saturating_add(1),
        updated_at: Utc::now(),
        device_id: device_id.into(),
        graph_id: tombstone.graph_id,
    })
}

/// Build an op for a supersession edge between two reefs.
pub(crate) fn supersession_op(
    op_id: Uuid,
    superseder_id: Uuid,
    superseded_id: Uuid,
    graph_id: Uuid,
    edge_source: &str,
    op_type: OpType,
    updated_at: DateTime<Utc>,
    device_id: &str,
) -> NewOp {
    let payload = json!({
        field::SUPERSEDER_ID: superseder_id.to_string(),
        field::SUPERSEDED_ID: superseded_id.to_string(),
        field::GRAPH_ID: graph_id.to_string(),
        field::EDGE_SOURCE: edge_source,
    });
    NewOp {
        op_id,
        entity: OP_ENTITY_SUPERSESSION.into(),
        entity_id: super::compose_id(superseder_id, superseded_id),
        op_type,
        payload,
        version: 0,
        updated_at,
        device_id: device_id.into(),
        graph_id: Some(graph_id),
    }
}

/// Build the (always-`Upsert`) op for a rejected supersession pair (D3/§3.7).
/// `entity_id` is order-INSENSITIVE — [`super::compose_id`] is called with the
/// two ids in canonical `(LEAST, GREATEST)` order, matching the table's own
/// `CHECK (node_a_id < node_b_id)`, so `{a, b}` and `{b, a}` always produce the
/// same entity id and a duplicate rejection from either direction converges on
/// one row rather than two. There is no delete variant: a rejection is a
/// permanent negative marker, never un-rejected by this system today.
pub(crate) fn non_supersession_pair_op(
    op_id: Uuid,
    node_a: Uuid,
    node_b: Uuid,
    graph_id: Uuid,
    marked_by: Option<Uuid>,
    marked_at: DateTime<Utc>,
    device_id: &str,
) -> NewOp {
    let (a, b) = if node_a < node_b {
        (node_a, node_b)
    } else {
        (node_b, node_a)
    };
    let payload = json!({
        field::NODE_A_ID: a.to_string(),
        field::NODE_B_ID: b.to_string(),
        field::GRAPH_ID: graph_id.to_string(),
        field::MARKED_BY: marked_by.map(|u| u.to_string()),
        field::MARKED_AT: marked_at.to_rfc3339(),
    });
    NewOp {
        op_id,
        entity: OP_ENTITY_NON_SUPERSESSION_PAIR.into(),
        entity_id: super::compose_id(a, b),
        op_type: OpType::Upsert,
        payload,
        version: 0,
        updated_at: marked_at,
        device_id: device_id.into(),
        graph_id: Some(graph_id),
    }
}

/// Build an upsert op for a typed relationship edge (ADR-0008), carrying the full
/// row so a remote apply can reconstruct it. `provenance` is carried as its raw
/// JSON `Value` (the same JSONB the row stores); `origin`/`status` as their
/// lower-case discriminants; `confidence` as a JSON number.
#[allow(clippy::too_many_arguments)]
pub(crate) fn relationship_edge_op(
    op_id: Uuid,
    edge_id: Uuid,
    graph_id: Uuid,
    src_node_id: Uuid,
    predicate: &str,
    dst_node_id: Uuid,
    origin: &str,
    status: &str,
    confidence: f32,
    provenance: &Value,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
    version: i64,
    device_id: &str,
) -> NewOp {
    let payload = json!({
        field::EDGE_ID: edge_id.to_string(),
        field::GRAPH_ID: graph_id.to_string(),
        field::SRC_NODE_ID: src_node_id.to_string(),
        field::PREDICATE: predicate,
        field::DST_NODE_ID: dst_node_id.to_string(),
        field::ORIGIN: origin,
        field::STATUS: status,
        field::CONFIDENCE: confidence,
        field::PROVENANCE: provenance,
        field::CREATED_AT: created_at.to_rfc3339(),
        field::UPDATED_AT: updated_at.to_rfc3339(),
        field::VERSION: version,
    });
    NewOp {
        op_id,
        entity: OP_ENTITY_RELATIONSHIP.into(),
        entity_id: edge_id.to_string(),
        op_type: OpType::Upsert,
        payload,
        version,
        updated_at,
        device_id: device_id.into(),
        graph_id: Some(graph_id),
    }
}

/// Build a delete op for a typed relationship edge (ADR-0008). The apply path
/// keys deletes off `edge_id` alone; `graph_id` rides along so the op carries
/// its ocean scope like every other delete tombstone. Used by `merge_nodes`
/// (when a re-pointed edge would self-loop or collide with one `keep` already
/// has) and by `unmerge_nodes` (to revert a re-pointed edge back to the dup).
pub(crate) fn relationship_edge_delete_op(
    op_id: Uuid,
    edge_id: Uuid,
    graph_id: Uuid,
    updated_at: DateTime<Utc>,
    device_id: &str,
) -> NewOp {
    let payload = json!({
        field::EDGE_ID: edge_id.to_string(),
        field::GRAPH_ID: graph_id.to_string(),
    });
    NewOp {
        op_id,
        entity: OP_ENTITY_RELATIONSHIP.into(),
        entity_id: edge_id.to_string(),
        op_type: OpType::Delete,
        payload,
        version: 0,
        updated_at,
        device_id: device_id.into(),
        graph_id: Some(graph_id),
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────
