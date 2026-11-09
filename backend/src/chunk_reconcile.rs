//! Content-verified positional chunks: adopt or replace, never append-only.
//!
//! Every capture path stores retrieval chunks as atoms keyed
//! `(graph_id, source_type, "{source_id}:chunk:{position}")`. The insert is
//! `ON CONFLICT DO NOTHING`, so before this module a re-capture with edited
//! text kept the OLD text at every existing position and linked none of those
//! positions to the new source version. [`reconcile_tx`] runs after a
//! caller's insert, in the caller's capture transaction, and settles every
//! position of the new chunk list:
//!
//! * **inserted** by this capture: its text is the incoming text. Stamp its
//!   [`chunk_sha256`] and link it.
//! * **adopted**: an existing atom whose canonical text hashes equal to the
//!   incoming chunk. No content write. Link it (the version-chunk insert is
//!   the only write, which sets `source_evidence_bound`).
//! * **replaced**: an existing atom of THIS source item whose text differs.
//!   Publish the new text in place through
//!   [`content_replacement::publish_tx`]: the revision CAS bumps
//!   `content_revision`, parks the old blob in `superseded_blob_keys`, and
//!   clears the managed embedding. The caller re-embeds it with the inserted
//!   atoms ([`Reconciled::written`]). Link it.
//! * **conflict**: an existing atom this source may not rewrite: bound to a
//!   version of ANOTHER source item (same native id in another provider
//!   namespace), a derived (promoted/connected) copy, or a retracted atom.
//!   Left untouched and unlinked, exactly as before.
//! * **unverified**: an existing atom whose stored text could not be read
//!   (object store error). Left untouched and unlinked; the next capture
//!   retries. A missing object or non-UTF-8 body is not "unverified": the
//!   content is gone, so the position is replaced (the old huddle repair).
//!
//! Positions past the new chunk count (the source shrank) are retired with
//! [`deletion::retire_positional_chunks_tx`] inside a savepoint, so a refusal
//! there leaves the capture intact and the next capture retries.
//!
//! Then ALL settled positions are linked to the new version through
//! [`source_evidence::link_verified_chunks_tx`]. Every id passed is
//! content-verified (inserted with, adopted by hash equality with, or
//! replaced by the incoming text), which is what that function requires.
//!
//! # Canonical text and `atoms.chunk_sha256`
//!
//! The hash covers the persisted text after its first `header_len` bytes
//! ([`crate::store::strip_header_prefix`]): the contextual header (meeting
//! participants, provenance timestamp, project) is metadata, and a header-only
//! difference must not churn content revisions, embeddings and reef identity.
//! Writers that prepend a header without recording `header_len` (Slack and
//! Google Chat messages) hash the full text, header included, which is still
//! exactly what they stored.
//!
//! `chunk_sha256` is a cache, not a source of truth: NULL (every atom written
//! before the column, and any atom whose content changed through another
//! writer; a trigger clears it) means "hash the stored content", which reads
//! the inline text or the decrypted blob with the caller's DEK.
//!
//! # Triggers this relies on
//!
//! * `library_chunk_bound`: inserting a version-chunk row marks the atom
//!   `source_evidence_bound`. Linking is insert-only.
//! * `aa_reef_identity_mapping_loss`: fires on UPDATE/DELETE of
//!   `source_version_chunks`. This module never updates or deletes a chunk
//!   row of an existing version; only retiring a surplus atom cascades its
//!   rows, which is the same erase-support rule every atom deletion has.
//! * `aa_reef_identity_atom_*`: a replacement changes the atom fingerprint,
//!   so reef-identity support citing the old text is erased. Adopting and
//!   hash stamping do not change the fingerprint.

use std::collections::{HashMap, HashSet};

use chrono::{DateTime, Utc};
use sha2::{Digest, Sha256};
use sqlx::{Postgres, Transaction};
use uuid::Uuid;

use crate::services::content_replacement::{self, ContentSnapshot, DerivedColumns};
use crate::source_evidence::CaptureFailure;

/// Separator between a source id and a chunk position.
pub const CHUNK_INFIX: &str = ":chunk:";

pub mod backfill;

/// Concurrent object-store reads while verifying legacy (unhashed) chunks.
const VERIFY_READ_CONCURRENCY: usize = 8;

/// `{base}:chunk:{position}`, the positional atom identity every writer uses.
pub fn chunk_source_id(base: &str, position: impl std::fmt::Display) -> String {
    format!("{base}{CHUNK_INFIX}{position}")
}

/// Lowercase hex SHA-256 of a chunk's canonical text: `text` minus its first
/// `header_len` bytes (`None`/non-positive/out-of-range = no header).
pub fn chunk_sha256(text: &str, header_len: Option<i32>) -> String {
    let body = crate::store::strip_header_prefix(text, header_len.unwrap_or(0));
    hex::encode(Sha256::digest(body.as_bytes()))
}

/// Write-time columns of one chunk, used when its position is replaced.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ChunkColumns {
    pub excerpt: Option<String>,
    pub title: Option<String>,
    pub title_kind: Option<String>,
    pub symbols: Option<String>,
}

/// One position of the incoming chunk list.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PositionalChunk {
    /// `{base}:chunk:{position}`.
    pub source_id: String,
    /// Full persisted text (header included).
    pub text: String,
    /// Header bytes at the start of `text`, as the writer records them.
    pub header_len: Option<i32>,
    /// Columns for a replacement. `None` derives the excerpt and fallback
    /// title from `text` (symbols stay NULL), so a replaced chunk never keeps
    /// a title or excerpt of its old text.
    pub columns: Option<ChunkColumns>,
}

impl PositionalChunk {
    pub fn new(source_id: String, text: String, header_len: Option<i32>) -> Self {
        Self { source_id, text, header_len, columns: None }
    }

    fn columns_for(&self, source_type: &str) -> ChunkColumns {
        if let Some(columns) = &self.columns {
            return columns.clone();
        }
        let stripped =
            crate::services::atom_title::strip_provenance_header(&self.text, self.header_len);
        let title = crate::services::atom_title::fallback_title_for_source(source_type, stripped);
        ChunkColumns {
            excerpt: crate::store::derive_journal_excerpt(Some(&self.text), self.header_len),
            title_kind: title.as_ref().map(|_| "fallback".to_string()),
            title,
            symbols: None,
        }
    }
}

/// Where the chunks belong and how to write their content.
pub struct ChunkTarget<'a> {
    pub graph_id: Uuid,
    /// Owner account of `graph_id` (the DEK's account).
    pub account_id: Uuid,
    /// The captured source item; atoms bound to any OTHER item are conflicts.
    pub source_item_id: Uuid,
    /// The version every settled position is linked to.
    pub version_id: Uuid,
    pub source_type: &'a str,
    /// Source id before `:chunk:N`.
    pub base_source_id: &'a str,
    /// Whole-request hash written to `atoms.content_hash` on replacement.
    pub content_hash: Option<&'a str>,
    /// Written to `atoms.participants` on replacement; `None` keeps them.
    pub participants: Option<&'a [String]>,
    /// Blob-key org segment (`None` on the consumer plane).
    pub org_id: Option<&'a str>,
    pub dek: &'a [u8; 32],
    /// `None` = dev/inline mode: replacements write `content_inline`.
    pub r2: Option<&'a coralswarm_core::r2::R2Client>,
    pub device_id: &'a str,
}

/// What [`reconcile_tx`] did, each list in chunk-position order.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Reconciled {
    /// Every id linked to the version (inserted, adopted and replaced).
    pub linked: Vec<Uuid>,
    pub inserted: Vec<Uuid>,
    pub adopted: Vec<Uuid>,
    pub replaced: Vec<Uuid>,
    /// Surplus positions deleted (the source shrank).
    pub retired: Vec<Uuid>,
    /// Existing atoms this source may not rewrite; left unlinked.
    pub conflicts: Vec<Uuid>,
    /// Existing atoms whose stored text could not be read; left unlinked.
    pub unverified: Vec<Uuid>,
    /// Positions with no atom row at all (for example a failed upload's row
    /// was removed by the caller).
    pub missing: usize,
    /// Surplus retirement was refused and rolled back to its savepoint.
    pub retire_failed: bool,
}

impl Reconciled {
    /// Atoms whose content this capture wrote: they need (re-)embedding.
    pub fn written(&self) -> Vec<Uuid> {
        self.inserted.iter().chain(self.replaced.iter()).copied().collect()
    }
}

#[derive(sqlx::FromRow)]
pub(crate) struct ExistingRow {
    atom_id: Uuid,
    source_id: String,
    content_blob_key: String,
    content_inline: Option<String>,
    header_len: Option<i32>,
    content_revision: i64,
    timestamp: DateTime<Utc>,
    chunk_sha256: Option<String>,
    derived_copy_bound: bool,
    evidence_retracted: bool,
    foreign_bound: bool,
}

/// Read an atom's stored text. `Ok(None)` = the content is gone (no object,
/// no inline text, invalid locator, or not UTF-8). `Err` = could not tell;
/// [`CaptureFailure::Storage`] when the object store refused the read.
async fn stored_text(
    r2: Option<&coralswarm_core::r2::R2Client>,
    dek: &[u8; 32],
    key: &str,
    inline: Option<&str>,
) -> Result<Option<String>, CaptureFailure> {
    if let Some(text) = inline {
        return Ok(Some(text.to_owned()));
    }
    let Some(r2) = r2 else { return Ok(None) };
    match r2.get_atom_blob(key, dek).await {
        Ok(bytes) => Ok(String::from_utf8(bytes).ok()),
        Err(coralswarm_core::r2::R2Error::NotFound(_) | coralswarm_core::r2::R2Error::InvalidKey) => Ok(None),
        Err(e) => Err(e.into()),
    }
}

/// Settle every position of `chunks` against the atoms already stored, then
/// link all settled positions to `target.version_id`. `inserted` = ids this
/// capture's own insert created (and whose content it already persisted).
/// Uploaded replacement objects are appended to `uploads`, so the caller's
/// rollback cleanup covers them. An object store that refuses a replacement
/// body fails the reconcile with [`CaptureFailure::Storage`], so the capture
/// reports a retryable storage outage, not an internal error. See the module
/// doc.
pub(crate) async fn reconcile_tx(
    tx: &mut Transaction<'_, Postgres>,
    target: &ChunkTarget<'_>,
    chunks: &[PositionalChunk],
    inserted: &[Uuid],
    uploads: &mut Vec<String>,
) -> Result<Reconciled, CaptureFailure> {
    let mut out = Reconciled::default();
    if chunks.is_empty() {
        return Ok(out);
    }
    let source_ids: Vec<&str> = chunks.iter().map(|c| c.source_id.as_str()).collect();
    let rows = load_rows(tx, target.graph_id, target.source_type, &source_ids, target.source_item_id, true).await?;
    let inserted: HashSet<Uuid> = inserted.iter().copied().collect();

    // Verify unhashed legacy positions by reading their stored text, a few
    // objects at a time. Only positions that could still be adopted or
    // replaced need it.
    let needs_read: Vec<(Uuid, String, Option<String>, Option<i32>)> = chunks
        .iter()
        .filter_map(|c| rows.get(&c.source_id))
        .filter(|r| {
            !inserted.contains(&r.atom_id)
                && r.chunk_sha256.is_none()
                && !r.derived_copy_bound
                && !r.evidence_retracted
        })
        .map(|r| (r.atom_id, r.content_blob_key.clone(), r.content_inline.clone(), r.header_len))
        .collect();
    let mut read: HashMap<Uuid, Result<Option<String>, CaptureFailure>> = HashMap::new();
    let (r2, dek) = (target.r2, *target.dek);
    for batch in needs_read.chunks(VERIFY_READ_CONCURRENCY) {
        let results = futures::future::join_all(batch.iter().map(|(id, key, inline, header_len)| async move {
            let text = stored_text(r2, &dek, key, inline.as_deref()).await;
            (*id, text.map(|t| t.map(|t| chunk_sha256(&t, *header_len))))
        }))
        .await;
        read.extend(results);
    }

    let mut stamps: Vec<(Uuid, String)> = Vec::new();
    for chunk in chunks {
        let Some(row) = rows.get(&chunk.source_id) else {
            out.missing += 1;
            continue;
        };
        if inserted.contains(&row.atom_id) {
            stamps.push((row.atom_id, chunk_sha256(&chunk.text, row.header_len)));
            out.inserted.push(row.atom_id);
            out.linked.push(row.atom_id);
            continue;
        }
        if row.derived_copy_bound || row.evidence_retracted {
            out.conflicts.push(row.atom_id);
            continue;
        }
        let incoming = chunk_sha256(&chunk.text, chunk.header_len);
        let stored = match &row.chunk_sha256 {
            Some(hash) => Some(hash.clone()),
            None => match read.get(&row.atom_id) {
                Some(Ok(hash)) => hash.clone(),
                Some(Err(e)) => {
                    tracing::warn!(error = %e, atom_id = %row.atom_id,
                        "chunk verification could not read stored text; position left unlinked");
                    out.unverified.push(row.atom_id);
                    continue;
                }
                None => None,
            },
        };
        if stored.as_deref() == Some(incoming.as_str()) {
            if row.chunk_sha256.is_none() {
                stamps.push((row.atom_id, incoming));
            }
            out.adopted.push(row.atom_id);
            out.linked.push(row.atom_id);
            continue;
        }
        if row.foreign_bound {
            // Same native id in another provider namespace: never rewrite
            // another source's evidence on the strength of the position.
            out.conflicts.push(row.atom_id);
            continue;
        }
        replace_row(tx, target, row, chunk, uploads).await?;
        out.replaced.push(row.atom_id);
        out.linked.push(row.atom_id);
    }

    if !stamps.is_empty() {
        let ids: Vec<Uuid> = stamps.iter().map(|s| s.0).collect();
        let hashes: Vec<String> = stamps.into_iter().map(|s| s.1).collect();
        sqlx::query(
            "UPDATE atoms a SET chunk_sha256=v.h FROM unnest($2::uuid[],$3::text[]) AS v(id,h)
              WHERE a.graph_id=$1 AND a.atom_id=v.id AND a.chunk_sha256 IS DISTINCT FROM v.h",
        )
        .bind(target.graph_id)
        .bind(&ids)
        .bind(&hashes)
        .execute(&mut **tx)
        .await
        .map_err(|e| format!("stamping chunk hashes: {e}"))?;
    }

    retire_surplus(tx, target, &source_ids, &mut out).await?;

    crate::source_evidence::link_verified_chunks_tx(tx, target.graph_id, target.version_id, &out.linked)
        .await
        .map_err(|e| format!("linking source chunks: {e}"))?;
    Ok(out)
}

/// Existing atoms at `source_ids`, optionally locked `FOR UPDATE` (a
/// read-only transaction cannot lock). `foreign_bound` = linked to a version
/// of any source item other than `source_item_id`.
pub(crate) async fn load_rows(
    tx: &mut Transaction<'_, Postgres>,
    graph_id: Uuid,
    source_type: &str,
    source_ids: &[&str],
    source_item_id: Uuid,
    lock: bool,
) -> Result<HashMap<String, ExistingRow>, String> {
    let sql = format!(
        "SELECT a.atom_id,a.source_id,a.content_blob_key,a.content_inline,a.header_len,
                a.content_revision,a.timestamp,a.chunk_sha256,a.derived_copy_bound,a.evidence_retracted,
                EXISTS(SELECT 1 FROM source_version_chunks c
                        JOIN source_versions v ON v.graph_id=c.graph_id AND v.version_id=c.source_version_id
                       WHERE c.graph_id=a.graph_id AND c.atom_id=a.atom_id
                         AND v.source_item_id<>$4) AS foreign_bound
           FROM atoms a
          WHERE a.graph_id=$1 AND a.source_type=$2 AND a.source_id=ANY($3)
          ORDER BY a.atom_id{}",
        if lock { " FOR UPDATE OF a" } else { "" }
    );
    let rows: Vec<ExistingRow> = sqlx::query_as(&sql)
        .bind(graph_id)
        .bind(source_type)
        .bind(source_ids)
        .bind(source_item_id)
        .fetch_all(&mut **tx)
        .await
        .map_err(|e| format!("loading positional chunks: {e}"))?;
    Ok(rows.into_iter().map(|r| (r.source_id.clone(), r)).collect())
}

/// Publish `chunk` over `row` in place (the content-revision CAS). With an
/// object store the text goes to a fresh object (and inline too if the row
/// kept inline text); without one (dev/inline mode) it is written inline,
/// which also repairs a row a crashed capture left without a body. Always
/// `Ok(true)` unless it errs; the row must be locked by the caller. A refused
/// upload is a [`CaptureFailure::Storage`].
pub(crate) async fn replace_row(
    tx: &mut Transaction<'_, Postgres>,
    target: &ChunkTarget<'_>,
    row: &ExistingRow,
    chunk: &PositionalChunk,
    uploads: &mut Vec<String>,
) -> Result<bool, CaptureFailure> {
    let key = coralswarm_core::r2::blob_key_for(target.org_id, target.graph_id, Uuid::new_v4())
        .map_err(|e| format!("replacement blob key: {e}"))?;
    if let Some(r2) = target.r2 {
        // A fresh key: rolling back must never delete an object an older
        // committed row still references.
        r2.put_atom_blob_at(&key, chunk.text.as_bytes(), target.dek)
            .await
            .map_err(|e| CaptureFailure::from(e).context("replacement upload"))?;
        uploads.push(key.clone());
    }
    let columns = chunk.columns_for(target.source_type);
    let snapshot = ContentSnapshot {
        graph_id: target.graph_id,
        atom_id: row.atom_id,
        revision: row.content_revision,
        blob_key: row.content_blob_key.clone(),
        inline: row.content_inline.clone(),
        header_len: row.header_len,
        timestamp: row.timestamp,
    };
    let derived = DerivedColumns {
        excerpt: columns.excerpt,
        title: columns.title,
        title_kind: columns.title_kind,
        symbols: columns.symbols,
        content_hash: target.content_hash.map(str::to_owned),
        chunk_sha256: chunk_sha256(&chunk.text, chunk.header_len),
        participants: target.participants.map(<[String]>::to_vec),
        write_inline: target.r2.is_none(),
    };
    let published = content_replacement::publish_tx(
        tx, &snapshot, &chunk.text, chunk.header_len, &key, target.device_id, Some(&derived),
    )
    .await
    .map_err(|e| format!("replacing chunk {}: {e}", chunk.source_id))?;
    if !published {
        // The row is locked FOR UPDATE by the caller; a lost CAS is a bug.
        return Err(format!("chunk {} changed during its own capture", chunk.source_id).into());
    }
    Ok(true)
}

/// Delete this source's positional atoms that the incoming list no longer
/// has, in a savepoint. Atoms bound to another source item or derived copies
/// are never selected.
async fn retire_surplus(
    tx: &mut Transaction<'_, Postgres>,
    target: &ChunkTarget<'_>,
    current: &[&str],
    out: &mut Reconciled,
) -> Result<(), String> {
    // `~>=~`/`~<~` with the `:chunk:`/`:chunk;` bounds is a byte-order prefix
    // range served by `idx_atoms_graph_source_id_pattern` (text_pattern_ops);
    // the regex keeps only `{base}:chunk:{digits}` itself.
    let surplus: Vec<Uuid> = sqlx::query_scalar(
        "SELECT a.atom_id FROM atoms a
          WHERE a.graph_id=$1 AND a.source_type=$2
            AND a.source_id ~>=~ ($3 || ':chunk:') AND a.source_id ~<~ ($3 || ':chunk;')
            AND substring(a.source_id FROM char_length($3)+8) ~ '^[0-9]+$'
            AND a.source_id <> ALL($4)
            AND NOT a.derived_copy_bound
            AND NOT EXISTS(SELECT 1 FROM source_version_chunks c
                            JOIN source_versions v ON v.graph_id=c.graph_id AND v.version_id=c.source_version_id
                           WHERE c.graph_id=a.graph_id AND c.atom_id=a.atom_id AND v.source_item_id<>$5)
          ORDER BY a.atom_id",
    )
    .bind(target.graph_id)
    .bind(target.source_type)
    .bind(target.base_source_id)
    .bind(current)
    .bind(target.source_item_id)
    .fetch_all(&mut **tx)
    .await
    .map_err(|e| format!("finding surplus chunks: {e}"))?;
    if surplus.is_empty() {
        return Ok(());
    }
    sqlx::query("SAVEPOINT chunk_reconcile_retire")
        .execute(&mut **tx)
        .await
        .map_err(|e| format!("retire savepoint: {e}"))?;
    match crate::services::deletion::retire_positional_chunks_tx(
        tx, target.account_id, target.device_id, target.graph_id, &surplus,
    )
    .await
    {
        Ok(deleted) => {
            sqlx::query("RELEASE SAVEPOINT chunk_reconcile_retire")
                .execute(&mut **tx)
                .await
                .map_err(|e| format!("retire release: {e}"))?;
            out.retired = deleted;
        }
        Err(e) => {
            tracing::warn!(error = %e, graph_id = %target.graph_id, source_id = %target.base_source_id,
                surplus = surplus.len(), "surplus chunk retirement refused; kept for the next capture");
            sqlx::query("ROLLBACK TO SAVEPOINT chunk_reconcile_retire")
                .execute(&mut **tx)
                .await
                .map_err(|e| format!("retire rollback: {e}"))?;
            out.retire_failed = true;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hash_strips_exactly_the_recorded_header() {
        let body = "the body";
        let text = format!("Header line\n\n{body}");
        let header_len = Some(("Header line".len() + 2) as i32);
        assert_eq!(chunk_sha256(&text, header_len), chunk_sha256(body, None));
        assert_ne!(chunk_sha256(&text, None), chunk_sha256(body, None));
        // Out-of-range or non-boundary header lengths hash the whole text.
        assert_eq!(chunk_sha256(body, Some(999)), chunk_sha256(body, None));
        assert_eq!(chunk_sha256(body, Some(-1)), chunk_sha256(body, None));
        assert_eq!(chunk_sha256(body, None).len(), 64);
    }

    #[test]
    fn chunk_source_id_matches_the_positional_shape() {
        assert_eq!(chunk_source_id("msg-1", 3), "msg-1:chunk:3");
    }

    #[test]
    fn default_columns_follow_the_replacement_text() {
        let chunk = PositionalChunk::new("n:chunk:0".into(), "Ship it Friday".into(), None);
        let columns = chunk.columns_for("note");
        assert_eq!(columns.title_kind.as_deref(), columns.title.as_ref().map(|_| "fallback"));
        assert!(columns.symbols.is_none());
    }
}
