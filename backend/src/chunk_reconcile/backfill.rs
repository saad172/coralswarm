//! Content-verified backfill of `source_version_chunks` for versions that
//! positional, append-only ingest left chunkless or partial (step C of the
//! adopt-or-replace fix; the live path is [`super::reconcile_tx`]).
//!
//! For each candidate version (fewer chunk rows than the source has
//! positional atoms), reef-linked versions first (a `node_semantic_atoms`
//! link through one of its semantic atoms), then current versions, then the
//! rest:
//!
//! 1. Load the version's retained text (`raw_inline`, or the `raw_blob_key`
//!    object decrypted with the graph account's DEK).
//! 2. Re-chunk it with the current chunker and the strategy its writer used
//!    ([`strategy_for`]).
//! 3. For each position, hash the existing atom's canonical text (stored
//!    `chunk_sha256`, else its content minus `header_len`) and link the atom
//!    ONLY when that equals the hash of the re-chunked body.
//! 4. A mismatch on the CURRENT version is replaced through the live
//!    replace path only with `replace_current`; otherwise, and always on a
//!    non-current version, it is reported and left unlinked. Never guessed.
//!
//! Writers that prepend a one-line header without recording `header_len`
//! (Slack and Google Chat messages: `"{author} in {place} · {when}\n\n"`)
//! are compared on the text after that first line: the body must still be
//! byte-identical. No other relaxation exists.
//!
//! Items whose `access_state` is not `permitted` are skipped: binding their
//! legacy atoms would change Library visibility, which a backfill must not.
//!
//! Each version runs in its own short transaction (`BEGIN READ ONLY` for a
//! dry run; under the capture's source lock with `FOR UPDATE` row locks for
//! `--execute`), so a live capture of the same source serializes with it.
//! Idempotent: linked positions are skipped, and links are
//! `ON CONFLICT DO NOTHING`.
//!
//! After linking, run `backfill_node_atoms_bridge` for the same database so
//! reefs linked to the newly chunked versions gain their `node_atoms` rows.

use std::collections::{HashMap, HashSet};

use coralswarm_core::chunker::{chunk, ChunkStrategy};
use coralswarm_core::r2::R2Client;
use sqlx::{PgPool, Postgres, Transaction};
use uuid::Uuid;

use super::{chunk_sha256, chunk_source_id, load_rows, replace_row, stored_text, ChunkTarget, PositionalChunk};
use crate::tenancy::TenantId;

/// Default page of candidate versions per progress line.
pub const DEFAULT_BATCH_SIZE: i64 = 100;

/// Tables the backfill reads or writes; the role must bypass RLS on all.
pub const TABLES: &[&str] = &[
    "atoms",
    "reef_identity_corrections",
    "source_items",
    "source_versions",
    "source_version_chunks",
    "semantic_atoms",
    "node_semantic_atoms",
];

#[derive(Debug, Clone)]
pub struct Options {
    pub graph_id: Option<Uuid>,
    pub batch_size: i64,
    pub max_batches: Option<u64>,
    /// Write links (and hash stamps). `false` = dry run, nothing written.
    pub execute: bool,
    /// Replace mismatched positions of CURRENT versions in place.
    pub replace_current: bool,
}

/// Counts. In a dry run `linked`/`replaced` are what `--execute` would do.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Report {
    pub batches: u64,
    pub versions_examined: u64,
    /// Of those, reef-linked / current versions.
    pub versions_reef_linked: u64,
    pub versions_current: u64,
    /// Version text could not be loaded (no R2 client or DEK, object error).
    pub versions_text_unavailable: u64,
    /// Every position of the version is linked once this run is done.
    pub versions_complete_after: u64,
    pub positions_examined: u64,
    pub already_linked: u64,
    /// Positions linked by exact content equality.
    pub linked: u64,
    /// Mismatched positions replaced in place (`replace_current`).
    pub replaced: u64,
    pub mismatched_current: u64,
    pub mismatched_noncurrent: u64,
    /// Existing atom text could not be read (transient object error).
    pub unverifiable: u64,
    /// No atom at the position.
    pub missing_atoms: u64,
    /// Mismatched atom another source owns, a derived copy, or retracted.
    pub conflicts: u64,
    /// `atoms.chunk_sha256` filled from content read during verification.
    pub hashes_stamped: u64,
    /// Active `legacy` reef-identity corrections citing an atom this run
    /// links. Binding a legacy atom makes such support invalid (legacy
    /// support requires an unbound atom), as any first link always has.
    pub legacy_identity_supports: u64,
}

impl Report {
    fn add(&mut self, other: &Report) {
        self.versions_examined += other.versions_examined;
        self.versions_reef_linked += other.versions_reef_linked;
        self.versions_current += other.versions_current;
        self.versions_text_unavailable += other.versions_text_unavailable;
        self.versions_complete_after += other.versions_complete_after;
        self.positions_examined += other.positions_examined;
        self.already_linked += other.already_linked;
        self.linked += other.linked;
        self.replaced += other.replaced;
        self.mismatched_current += other.mismatched_current;
        self.mismatched_noncurrent += other.mismatched_noncurrent;
        self.unverifiable += other.unverifiable;
        self.missing_atoms += other.missing_atoms;
        self.conflicts += other.conflicts;
        self.hashes_stamped += other.hashes_stamped;
        self.legacy_identity_supports += other.legacy_identity_supports;
    }
}

#[derive(sqlx::FromRow, Debug)]
struct Candidate {
    version_id: Uuid,
    graph_id: Uuid,
    source_item_id: Uuid,
    source_id: String,
    source_type: String,
    provider_namespace: String,
    representation: String,
    raw_inline: Option<String>,
    raw_blob_key: Option<String>,
    code_path: Option<String>,
    account_id: Uuid,
    is_current: bool,
    tier: i32,
}

/// Versions with fewer chunk rows than their source has positional atoms,
/// permitted items only, ordered reef-linked (0), current (1), other (2).
const CANDIDATES_SQL: &str = "
WITH cand AS (
 SELECT v.version_id, v.graph_id, v.source_item_id, v.source_id, v.source_type,
        v.provider_namespace, v.representation, v.raw_inline, v.raw_blob_key,
        v.source_metadata->>'code_path' AS code_path, g.account_id,
        (s.current_version_id = v.version_id) AS is_current,
        CASE WHEN EXISTS(SELECT 1 FROM semantic_atoms sa
                           JOIN node_semantic_atoms l ON l.graph_id=sa.graph_id AND l.semantic_atom_id=sa.semantic_atom_id
                          WHERE sa.graph_id=v.graph_id AND sa.source_version_id=v.version_id) THEN 0
             WHEN s.current_version_id = v.version_id THEN 1 ELSE 2 END AS tier
   FROM source_versions v
   JOIN source_items s ON s.graph_id=v.graph_id AND s.source_item_id=v.source_item_id
   JOIN graphs g ON g.id=v.graph_id
  WHERE s.access_state='permitted' AND v.extraction_version<>'selected-passages-v1'
    AND ($1::uuid IS NULL OR v.graph_id=$1)
    AND (SELECT count(*) FROM source_version_chunks c
          WHERE c.graph_id=v.graph_id AND c.source_version_id=v.version_id)
      < (SELECT count(*) FROM atoms a
          WHERE a.graph_id=v.graph_id AND a.source_type=v.source_type
            AND a.source_id ~>=~ (v.source_id || ':chunk:') AND a.source_id ~<~ (v.source_id || ':chunk;')
            AND substring(a.source_id FROM char_length(v.source_id)+8) ~ '^[0-9]+$')
)
SELECT * FROM cand
 WHERE ($2::int IS NULL OR (tier, version_id) > ($2::int, $3::uuid))
 ORDER BY tier, version_id
 LIMIT $4";

/// The chunk strategy the writer of this source used. A wrong guess here can
/// only produce mismatches (verification is by exact content), never a link.
pub fn strategy_for(
    source_type: &str,
    provider_namespace: &str,
    representation: &str,
    code_path: Option<&str>,
) -> ChunkStrategy {
    match (source_type, code_path) {
        ("github_code", Some(path)) if !path.is_empty() => ChunkStrategy::code_for_path(path),
        // `workers::huddle` chunks Slack huddle notes with Generic, not the
        // Transcript strategy its `transcript` source type would select.
        ("transcript", _)
            if provider_namespace.starts_with("slack:workspace:")
                && representation == "provider_summary" =>
        {
            ChunkStrategy::Generic
        }
        // A Composio item was chunked by its recipe's hint.
        _ => crate::ingest::capture::chunk_strategy(
            source_type,
            None,
            crate::connectors::composio::recipe::chunk_hint_for(source_type, provider_namespace)
                .map(|h| h.strategy())
                .as_ref(),
        ),
    }
}

/// Source types whose writers store `"{one line}\n\n{body}"` with no
/// `header_len`.
fn unrecorded_header(source_type: &str) -> bool {
    matches!(source_type, "slack" | "google_chat")
}

/// The one-line prefix (without the blank line) of an unrecorded header.
fn split_unrecorded_header(stored: &str) -> Option<(&str, &str)> {
    let (head, rest) = stored.split_once("\n\n")?;
    (!head.contains('\n')).then_some((head, rest))
}

async fn tenant_of(pool: &PgPool) -> Result<TenantId, String> {
    let has_meta: bool = sqlx::query_scalar("SELECT to_regclass('public.tenant_meta') IS NOT NULL")
        .fetch_one(pool)
        .await
        .map_err(|e| e.to_string())?;
    if !has_meta {
        return Ok(TenantId::Consumer);
    }
    let org: Option<String> = sqlx::query_scalar("SELECT org_id FROM tenant_meta WHERE id = true")
        .fetch_optional(pool)
        .await
        .map_err(|e| e.to_string())?;
    Ok(org.map(TenantId::Org).unwrap_or(TenantId::Consumer))
}

struct Ctx<'a> {
    pool: &'a PgPool,
    r2: Option<&'a R2Client>,
    tenant: TenantId,
    deks: HashMap<Uuid, Option<[u8; 32]>>,
    opts: &'a Options,
}

impl Ctx<'_> {
    async fn dek(&mut self, account: Uuid) -> Option<[u8; 32]> {
        if let Some(d) = self.deks.get(&account) {
            return *d;
        }
        let d = crate::crypto::resolve_dek_for_tenant(&self.tenant, self.pool, account)
            .await
            .ok()
            .flatten();
        self.deks.insert(account, d);
        d
    }
}

/// Walk every candidate. `on_batch` receives each page's report.
pub async fn run(
    pool: &PgPool,
    r2: Option<&R2Client>,
    opts: &Options,
    mut on_batch: impl FnMut(&Report),
) -> Result<Report, String> {
    let mut ctx = Ctx { pool, r2, tenant: tenant_of(pool).await?, deks: HashMap::new(), opts };
    let mut total = Report::default();
    let mut cursor: Option<(i32, Uuid)> = None;
    loop {
        if opts.max_batches.is_some_and(|max| total.batches >= max) {
            break;
        }
        let page: Vec<Candidate> = {
            let mut tx = pool.begin_with("BEGIN READ ONLY").await.map_err(|e| e.to_string())?;
            let page = sqlx::query_as(CANDIDATES_SQL)
                .bind(opts.graph_id)
                .bind(cursor.map(|c| c.0))
                .bind(cursor.map(|c| c.1))
                .bind(opts.batch_size.max(1))
                .fetch_all(&mut *tx)
                .await
                .map_err(|e| format!("candidate query: {e}"))?;
            tx.rollback().await.map_err(|e| e.to_string())?;
            page
        };
        let Some(last) = page.last() else { break };
        cursor = Some((last.tier, last.version_id));
        let mut batch = Report { batches: 1, ..Report::default() };
        for cand in &page {
            let one = version(&mut ctx, cand).await?;
            batch.add(&one);
        }
        total.batches += 1;
        total.add(&batch);
        on_batch(&batch);
    }
    Ok(total)
}

async fn version_text(ctx: &mut Ctx<'_>, cand: &Candidate) -> Option<String> {
    if let Some(text) = &cand.raw_inline {
        return Some(text.clone());
    }
    let (key, r2) = (cand.raw_blob_key.as_deref()?, ctx.r2?);
    let org = match &ctx.tenant {
        TenantId::Consumer => None,
        TenantId::Org(o) => Some(o.as_str()),
    };
    if !crate::source_evidence::artifact_locator_admitted(cand.graph_id, key, org) {
        return None;
    }
    let dek = ctx.dek(cand.account_id).await?;
    String::from_utf8(r2.get_atom_blob(key, &dek).await.ok()?).ok()
}

async fn version(ctx: &mut Ctx<'_>, cand: &Candidate) -> Result<Report, String> {
    let mut rep = Report { versions_examined: 1, ..Report::default() };
    match cand.tier {
        0 => rep.versions_reef_linked = 1,
        _ if cand.is_current => rep.versions_current = 1,
        _ => {}
    }
    let Some(text) = version_text(ctx, cand).await else {
        rep.versions_text_unavailable = 1;
        return Ok(rep);
    };
    let strategy = strategy_for(&cand.source_type, &cand.provider_namespace,
        &cand.representation, cand.code_path.as_deref());
    let chunks = chunk(&text, strategy);
    if chunks.is_empty() {
        return Ok(rep);
    }
    let bodies: Vec<(String, String)> = chunks
        .iter()
        .map(|c| (chunk_source_id(&cand.source_id, c.position), c.text_with_speaker()))
        .collect();
    let dek = ctx.dek(cand.account_id).await;
    let execute = ctx.opts.execute;
    let mut tx: Transaction<'_, Postgres> = if execute {
        ctx.pool.begin().await.map_err(|e| e.to_string())?
    } else {
        ctx.pool.begin_with("BEGIN READ ONLY").await.map_err(|e| e.to_string())?
    };
    if execute {
        crate::source_evidence::lock_identity_tx(&mut tx, cand.graph_id, &cand.source_id)
            .await
            .map_err(|e| e.to_string())?;
    }
    let sids: Vec<&str> = bodies.iter().map(|b| b.0.as_str()).collect();
    let rows = load_rows(&mut tx, cand.graph_id, &cand.source_type, &sids, cand.source_item_id, execute).await?;
    let linked: HashSet<Uuid> = sqlx::query_scalar(
        "SELECT atom_id FROM source_version_chunks WHERE graph_id=$1 AND source_version_id=$2",
    )
    .bind(cand.graph_id)
    .bind(cand.version_id)
    .fetch_all(&mut *tx)
    .await
    .map_err(|e| e.to_string())?
    .into_iter()
    .collect();
    let org = match &ctx.tenant {
        TenantId::Consumer => None,
        TenantId::Org(o) => Some(o.as_str()),
    };
    let zero = [0u8; 32];
    let target = ChunkTarget {
        graph_id: cand.graph_id,
        account_id: cand.account_id,
        source_item_id: cand.source_item_id,
        version_id: cand.version_id,
        source_type: &cand.source_type,
        base_source_id: &cand.source_id,
        content_hash: None,
        participants: None,
        org_id: org,
        dek: dek.as_ref().unwrap_or(&zero),
        r2: if dek.is_some() { ctx.r2 } else { None },
        device_id: crate::store::CLOUD_DEVICE_ID,
    };
    let mut to_link: Vec<Uuid> = Vec::new();
    let mut stamps: Vec<(Uuid, String)> = Vec::new();
    let mut uploads: Vec<String> = Vec::new();
    let mut complete = true;
    for (sid, body) in &bodies {
        rep.positions_examined += 1;
        let Some(row) = rows.get(sid) else {
            rep.missing_atoms += 1;
            complete = false;
            continue;
        };
        if linked.contains(&row.atom_id) {
            rep.already_linked += 1;
            continue;
        }
        let want = chunk_sha256(body, None);
        // Stored text is needed when there is no hash, and for the
        // unrecorded-header comparison and header-preserving replacement.
        let needs_text = row.chunk_sha256.is_none()
            || unrecorded_header(&cand.source_type)
            || ctx.opts.replace_current;
        let stored: Result<Option<String>, String> = if !needs_text {
            Ok(None)
        } else if row.content_inline.is_some() {
            Ok(row.content_inline.clone())
        } else if let (Some(r2), Some(d)) = (ctx.r2, dek.as_ref()) {
            stored_text(Some(r2), d, &row.content_blob_key, None).await.map_err(String::from)
        } else {
            Err("no blob reader (R2 client or DEK unavailable)".into())
        };
        let stored = match stored {
            Ok(s) => s,
            Err(_) => {
                rep.unverifiable += 1;
                complete = false;
                continue;
            }
        };
        let have = row.chunk_sha256.clone()
            .or_else(|| stored.as_deref().map(|t| chunk_sha256(t, row.header_len)));
        let alt_match = unrecorded_header(&cand.source_type)
            && row.header_len.unwrap_or(0) <= 0
            && stored.as_deref().and_then(split_unrecorded_header).is_some_and(|(_, rest)| rest == body);
        if row.derived_copy_bound || row.evidence_retracted {
            rep.conflicts += 1;
            complete = false;
            continue;
        }
        if have.as_deref() == Some(want.as_str()) || alt_match {
            if row.chunk_sha256.is_none() {
                if let (Some(h), Some(_)) = (&have, &stored) {
                    stamps.push((row.atom_id, h.clone()));
                }
            }
            rep.linked += 1;
            to_link.push(row.atom_id);
            continue;
        }
        if !cand.is_current {
            rep.mismatched_noncurrent += 1;
            complete = false;
            continue;
        }
        rep.mismatched_current += 1;
        if !ctx.opts.replace_current || row.foreign_bound {
            if row.foreign_bound {
                rep.conflicts += 1;
            }
            complete = false;
            continue;
        }
        // Keep the atom's own header and swap the body.
        let replacement = match (stored.as_deref(), row.header_len) {
            (Some(s), Some(h)) if h > 0 && (h as usize) <= s.len() && s.is_char_boundary(h as usize) => {
                PositionalChunk::new(sid.clone(), format!("{}{body}", &s[..h as usize]), Some(h))
            }
            (Some(s), _) if unrecorded_header(&cand.source_type) => match split_unrecorded_header(s) {
                Some((head, _)) => PositionalChunk::new(sid.clone(), format!("{head}\n\n{body}"), None),
                None => PositionalChunk::new(sid.clone(), body.clone(), None),
            },
            _ => PositionalChunk::new(sid.clone(), body.clone(), row.header_len),
        };
        // Never publish a blob-backed row's text inline: without an object
        // store and DEK the replacement is counted, not written.
        if target.r2.is_none() && row.content_inline.is_none() {
            rep.unverifiable += 1;
            complete = false;
            continue;
        }
        if !execute {
            rep.replaced += 1;
            continue;
        }
        replace_row(&mut tx, &target, row, &replacement, &mut uploads).await?;
        rep.replaced += 1;
        to_link.push(row.atom_id);
        crate::jobs::queue::enqueue_tx(&mut tx, &crate::jobs::JobKind::EmbedAtoms { atom_ids: vec![row.atom_id] },
            Some(cand.account_id), None, 0).await.map_err(|e| e.to_string())?;
    }
    if complete {
        rep.versions_complete_after = 1;
    }
    if !to_link.is_empty() {
        let supports: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM reef_identity_corrections
              WHERE graph_id=$1 AND support_atom_id=ANY($2) AND support_kind='legacy' AND state='active'",
        )
        .bind(cand.graph_id)
        .bind(&to_link)
        .fetch_one(&mut *tx)
        .await
        .map_err(|e| e.to_string())?;
        rep.legacy_identity_supports = supports as u64;
    }
    if !execute {
        tx.rollback().await.map_err(|e| e.to_string())?;
        rep.hashes_stamped = stamps.len() as u64;
        return Ok(rep);
    }
    let result: Result<(), String> = async {
        if !stamps.is_empty() {
            let ids: Vec<Uuid> = stamps.iter().map(|s| s.0).collect();
            let hashes: Vec<String> = stamps.iter().map(|s| s.1.clone()).collect();
            sqlx::query(
                "UPDATE atoms a SET chunk_sha256=v.h FROM unnest($2::uuid[],$3::text[]) AS v(id,h)
                  WHERE a.graph_id=$1 AND a.atom_id=v.id AND a.chunk_sha256 IS NULL",
            )
            .bind(cand.graph_id)
            .bind(&ids)
            .bind(&hashes)
            .execute(&mut *tx)
            .await
            .map_err(|e| e.to_string())?;
        }
        crate::source_evidence::link_verified_chunks_tx(&mut tx, cand.graph_id, cand.version_id, &to_link)
            .await
            .map_err(|e| e.to_string())?;
        Ok(())
    }
    .await;
    match result {
        Ok(()) => {
            tx.commit().await.map_err(|e| e.to_string())?;
            rep.hashes_stamped = stamps.len() as u64;
            Ok(rep)
        }
        Err(e) => {
            let _ = tx.rollback().await;
            crate::services::blob_store::cleanup_uncommitted_uploads(ctx.r2, &uploads).await;
            Err(format!("version {}: {e}", cand.version_id))
        }
    }
}

// ---------------------------------------------------------------------------
// Re-chunking atoms the embedder refused as too long
// ---------------------------------------------------------------------------

/// Counts for [`rechunk_oversized`]. A dry run fills the atom, version and
/// position counts; the per-position outcomes (`inserted` onwards) are only
/// known once `--execute` reconciles.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RechunkReport {
    /// Atoms stamped `blocked:input_too_long`.
    pub atoms_stamped: u64,
    /// Of those, atoms with no link to their item's current, permitted
    /// version: nothing to re-chunk from. Left as they are.
    pub atoms_unbound: u64,
    /// Current versions holding a stamped atom.
    pub versions: u64,
    pub versions_text_unavailable: u64,
    /// Linked positions before, positions produced by today's chunker after.
    pub positions_before: u64,
    pub positions_after: u64,
    pub inserted: u64,
    pub replaced: u64,
    pub adopted: u64,
    pub retired: u64,
    pub conflicts: u64,
    pub unverified: u64,
    /// A stamped atom today's chunker still produces unchanged (adopted).
    pub still_stamped: u64,
    pub versions_failed: u64,
}

#[derive(sqlx::FromRow, Debug)]
struct Stamped {
    atom_id: Uuid,
    version_id: Option<Uuid>,
}

/// Row-level columns of a stamped atom: every chunk of one capture shares
/// them (`ingest::capture::AtomFields`), so new positions copy them.
#[derive(sqlx::FromRow, Debug)]
struct Template {
    source_type: String,
    participants: Vec<String>,
    timestamp: chrono::DateTime<chrono::Utc>,
    ingested_by: Option<Uuid>,
    scope: String,
    fts_language: String,
    content_hash: Option<String>,
    channel_id: Option<String>,
    channel_name: Option<String>,
    author_id: Option<String>,
    author_name: Option<String>,
    workspace_id: Option<String>,
    project: Option<String>,
    mcp_client_name: Option<String>,
    header_len: Option<i32>,
    title: Option<String>,
    title_kind: Option<String>,
    content_blob_key: String,
    content_inline: Option<String>,
}

/// Re-chunk the current version of every source holding an atom the managed
/// embedder refused as over the provider's input limit
/// ([`crate::workers::embed::BLOCKED_INPUT_TOO_LONG`]), with today's
/// chunker, which bounds every chunk
/// (`coralswarm_core::chunker::MAX_CHUNK_BYTES`). Through
/// `ingest::rechunk_existing_version_tx`: the oversized position is replaced
/// in place with its first piece, the rest land at new or replaced positions,
/// all linked to the same version and queued for embedding. No text is
/// dropped. One transaction per version, under the capture's source lock.
/// Dry run by default: nothing is written.
pub async fn rechunk_oversized(
    pool: &PgPool,
    r2: Option<&R2Client>,
    graph_id: Option<Uuid>,
    execute: bool,
) -> Result<RechunkReport, String> {
    let mut ctx = Ctx {
        pool,
        r2,
        tenant: tenant_of(pool).await?,
        deks: HashMap::new(),
        opts: &Options { graph_id, batch_size: DEFAULT_BATCH_SIZE, max_batches: None, execute, replace_current: false },
    };
    let stamped: Vec<Stamped> = sqlx::query_as(
        "SELECT a.atom_id,
                (SELECT v.version_id FROM source_version_chunks c
                   JOIN source_versions v ON v.graph_id=c.graph_id AND v.version_id=c.source_version_id
                   JOIN source_items s ON s.graph_id=v.graph_id AND s.source_item_id=v.source_item_id
                  WHERE c.graph_id=a.graph_id AND c.atom_id=a.atom_id
                    AND s.current_version_id=v.version_id AND s.access_state='permitted'
                  LIMIT 1) AS version_id
           FROM atoms a
          WHERE a.embed_model_next=$1 AND NOT a.evidence_retracted
            AND ($2::uuid IS NULL OR a.graph_id=$2)
          ORDER BY a.atom_id",
    )
    .bind(crate::workers::embed::BLOCKED_INPUT_TOO_LONG)
    .bind(graph_id)
    .fetch_all(pool)
    .await
    .map_err(|e| e.to_string())?;
    let mut rep = RechunkReport { atoms_stamped: stamped.len() as u64, ..RechunkReport::default() };
    let mut versions: Vec<(Uuid, Uuid)> = Vec::new();
    for s in &stamped {
        match s.version_id {
            None => rep.atoms_unbound += 1,
            Some(v) if !versions.iter().any(|(seen, _)| *seen == v) => versions.push((v, s.atom_id)),
            Some(_) => {}
        }
    }
    let stamped_ids: HashSet<Uuid> = stamped.iter().map(|s| s.atom_id).collect();
    for (version_id, atom_id) in versions {
        rep.versions += 1;
        match rechunk_version(&mut ctx, version_id, atom_id, &stamped_ids, &mut rep).await {
            Ok(()) => {}
            Err(e) => {
                tracing::warn!(error = %e, %version_id, "rechunk_oversized: version failed; re-run retries it");
                rep.versions_failed += 1;
            }
        }
    }
    Ok(rep)
}

async fn rechunk_version(
    ctx: &mut Ctx<'_>,
    version_id: Uuid,
    atom_id: Uuid,
    stamped: &HashSet<Uuid>,
    rep: &mut RechunkReport,
) -> Result<(), String> {
    let cand: Candidate = sqlx::query_as(
        "SELECT v.version_id, v.graph_id, v.source_item_id, v.source_id, v.source_type,
                v.provider_namespace, v.representation, v.raw_inline, v.raw_blob_key,
                v.source_metadata->>'code_path' AS code_path, g.account_id,
                true AS is_current, 1 AS tier
           FROM source_versions v JOIN graphs g ON g.id=v.graph_id
          WHERE v.version_id=$1",
    )
    .bind(version_id)
    .fetch_one(ctx.pool)
    .await
    .map_err(|e| e.to_string())?;
    let template: Template = sqlx::query_as(
        "SELECT source_type, participants, timestamp, ingested_by, scope,
                COALESCE(fts_language,'english') AS fts_language, content_hash, channel_id,
                channel_name, author_id, author_name, workspace_id, project, mcp_client_name,
                header_len, title, title_kind, content_blob_key, content_inline
           FROM atoms WHERE graph_id=$1 AND atom_id=$2",
    )
    .bind(cand.graph_id)
    .bind(atom_id)
    .fetch_one(ctx.pool)
    .await
    .map_err(|e| e.to_string())?;
    let Some(text) = version_text(ctx, &cand).await else {
        rep.versions_text_unavailable += 1;
        return Ok(());
    };
    let before: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM source_version_chunks WHERE graph_id=$1 AND source_version_id=$2",
    )
    .bind(cand.graph_id)
    .bind(version_id)
    .fetch_one(ctx.pool)
    .await
    .map_err(|e| e.to_string())?;
    let strategy = strategy_for(&cand.source_type, &cand.provider_namespace,
        &cand.representation, cand.code_path.as_deref());
    let chunks = chunk(&text, strategy);
    rep.positions_before += before as u64;
    rep.positions_after += chunks.len() as u64;
    let dek = ctx.dek(cand.account_id).await;
    if ctx.r2.is_some() && dek.is_none() {
        rep.versions_text_unavailable += 1;
        return Ok(());
    }
    // The writer's contextual header, read back from the stamped atom: a
    // recorded header is its first `header_len` bytes; Slack and Google Chat
    // prepend one unrecorded line.
    let needs_stored = template.header_len.unwrap_or(0) > 0 || unrecorded_header(&cand.source_type);
    let stored = if needs_stored {
        let zero = [0u8; 32];
        stored_text(ctx.r2, dek.as_ref().unwrap_or(&zero), &template.content_blob_key,
            template.content_inline.as_deref()).await?
    } else {
        None
    };
    let header = match (template.header_len, stored.as_deref()) {
        (Some(h), Some(s)) if h > 2 && (h as usize) <= s.len() && s.is_char_boundary(h as usize) => {
            crate::ingest::capture::Header::Recorded(s[..h as usize].trim_end_matches("

").to_owned())
        }
        (_, Some(s)) if unrecorded_header(&cand.source_type) => match split_unrecorded_header(s) {
            Some((head, _)) => crate::ingest::capture::Header::Inline(head.to_owned()),
            None => crate::ingest::capture::Header::None,
        },
        _ => crate::ingest::capture::Header::None,
    };
    let hinted = template.title.as_deref().filter(|_| template.title_kind.as_deref() == Some("source"));
    let prepared = crate::ingest::capture::Prepared::new(
        &cand.source_id, &cand.source_type, &text, &chunks, &header, hinted);
    if !ctx.opts.execute {
        return Ok(());
    }
    let org = match &ctx.tenant {
        TenantId::Consumer => None,
        TenantId::Org(o) => Some(o.as_str()),
    };
    let zero = [0u8; 32];
    let version = crate::ingest::ExistingVersion {
        graph_id: cand.graph_id,
        account_id: cand.account_id,
        org_id: org,
        dek: dek.as_ref().unwrap_or(&zero),
        r2: if dek.is_some() { ctx.r2 } else { None },
        source_item_id: cand.source_item_id,
        version_id,
        source_id: &cand.source_id,
    };
    let atoms = crate::ingest::capture::AtomFields {
        source_type: &template.source_type,
        participants: &template.participants,
        timestamp: template.timestamp,
        ingested_by: template.ingested_by,
        scope: &template.scope,
        fts_language: &template.fts_language,
        content_hash: template.content_hash.as_deref(),
        channel_id: template.channel_id.as_deref(),
        channel_name: template.channel_name.as_deref(),
        author_id: template.author_id.as_deref(),
        author_name: template.author_name.as_deref(),
        workspace_id: template.workspace_id.as_deref(),
        project: template.project.as_deref(),
        mcp_client_name: template.mcp_client_name.as_deref(),
    };
    let mut uploads: Vec<String> = Vec::new();
    let mut tx = ctx.pool.begin().await.map_err(|e| e.to_string())?;
    let result = async {
        crate::source_evidence::lock_identity_tx(&mut tx, cand.graph_id, &cand.source_id)
            .await
            .map_err(|e| e.to_string())?;
        let current: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM source_items WHERE graph_id=$1 AND source_item_id=$2
                              AND current_version_id=$3 AND access_state='permitted')",
        )
        .bind(cand.graph_id)
        .bind(cand.source_item_id)
        .bind(version_id)
        .fetch_one(&mut *tx)
        .await
        .map_err(|e| e.to_string())?;
        if !current {
            return Err("version is no longer current and permitted".to_owned());
        }
        crate::ingest::rechunk_existing_version_tx(&mut tx, version, &prepared, &atoms, &mut uploads)
            .await
            .map_err(|e| e.to_string())
    }
    .await;
    match result {
        Ok(done) => {
            tx.commit().await.map_err(|e| e.to_string())?;
            rep.inserted += done.inserted.len() as u64;
            rep.replaced += done.replaced.len() as u64;
            rep.adopted += done.adopted.len() as u64;
            rep.retired += done.retired.len() as u64;
            rep.conflicts += done.conflicts.len() as u64;
            rep.unverified += done.unverified.len() as u64;
            rep.still_stamped += done.adopted.iter().filter(|id| stamped.contains(id)).count() as u64;
            Ok(())
        }
        Err(e) => {
            let _ = tx.rollback().await;
            crate::services::blob_store::cleanup_uncommitted_uploads(ctx.r2, &uploads).await;
            Err(format!("version {version_id}: {e}"))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn huddle_notes_use_generic_and_code_uses_its_path() {
        assert!(matches!(
            strategy_for("transcript", "slack:workspace:T1", "provider_summary", None),
            ChunkStrategy::Generic
        ));
        assert!(matches!(
            strategy_for("transcript", "declared:user:x", "normalized_text", None),
            ChunkStrategy::Transcript
        ));
    }

    #[test]
    fn unrecorded_header_is_exactly_one_line() {
        assert_eq!(split_unrecorded_header("Ada in #c · t\n\nbody"), Some(("Ada in #c · t", "body")));
        assert_eq!(split_unrecorded_header("two\nlines\n\nbody"), None);
        assert_eq!(split_unrecorded_header("no header"), None);
    }
}
