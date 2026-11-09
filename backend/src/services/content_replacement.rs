use chrono::{DateTime, Utc};
use sqlx::{PgPool, Postgres, Transaction};
use uuid::Uuid;

use crate::{
    jobs::{queue::enqueue_tx, JobKind},
    store::{append_op_tx, sync, StoreError},
    tenancy::scope::{begin_account_scoped, begin_tenant_system_scoped_without_account},
};

use super::blob_store::{cleanup_uncommitted_blob_uploads, PromotionBlobStore};

pub(crate) enum ContentScope<'a> {
    Account { pool: &'a PgPool, account_id: Uuid },
    Org { pool: &'a PgPool, account_id: Uuid },
}

impl ContentScope<'_> {
    fn account_id(&self) -> Uuid {
        match self {
            Self::Account { account_id, .. } | Self::Org { account_id, .. } => *account_id,
        }
    }

    async fn begin(&self) -> Result<Transaction<'static, Postgres>, StoreError> {
        match self {
            Self::Account { pool, account_id } => begin_account_scoped(pool, *account_id).await,
            Self::Org { pool, .. } => begin_tenant_system_scoped_without_account(pool).await,
        }
    }
}

pub(crate) struct ContentSnapshot {
    pub(crate) graph_id: Uuid,
    pub(crate) atom_id: Uuid,
    pub(crate) revision: i64,
    pub(crate) blob_key: String,
    pub(crate) inline: Option<String>,
    pub(crate) header_len: Option<i32>,
    pub(crate) timestamp: DateTime<Utc>,
}

pub(crate) async fn current_text(
    blob_key: &str,
    inline: Option<&str>,
    blobs: Option<&dyn PromotionBlobStore>,
    dek: Option<&[u8; 32]>,
) -> Result<String, String> {
    let Some(blobs) = blobs else {
        return inline
            .map(str::to_owned)
            .ok_or_else(|| "no inline content or blob store".into());
    };
    let dek = dek.ok_or_else(|| "DEK required to resolve authoritative content".to_string())?;
    let result = blobs.get_blob(blob_key, dek).await;
    if let Ok(bytes) = &result {
        std::str::from_utf8(bytes).map_err(|e| format!("content is not UTF-8: {e}"))?;
    }
    crate::workers::embed::atom_text_from_r2_or_inline(result, inline)
}

fn fresh_blob_key(snapshot: &ContentSnapshot, inline_only: bool) -> Result<String, String> {
    if let Err(error) = coralswarm_core::r2::validate_blob_key(&snapshot.blob_key) {
        if inline_only && snapshot.inline.is_some() {
            return Ok(format!("{}/{}", snapshot.graph_id, Uuid::new_v4()));
        }
        return Err(error.to_string());
    }
    let (prefix, _) = snapshot.blob_key.rsplit_once('/').unwrap();
    if prefix
        .rsplit('/')
        .next()
        .and_then(|graph| Uuid::parse_str(graph).ok())
        != Some(snapshot.graph_id)
    {
        return Err("content blob graph does not match atom graph".into());
    }
    Ok(format!("{prefix}/{}", Uuid::new_v4()))
}

pub(crate) async fn replace(
    scope: ContentScope<'_>,
    snapshot: &ContentSnapshot,
    content: &str,
    header_len: Option<i32>,
    device_id: &str,
    blobs: Option<&dyn PromotionBlobStore>,
    dek: Option<&[u8; 32]>,
) -> Result<bool, String> {
    let key = fresh_blob_key(snapshot, blobs.is_none())?;
    replace_with_key(
        scope, snapshot, content, header_len, device_id, blobs, dek, key,
    )
    .await
}

pub(crate) async fn repair_missing_blob(
    scope: ContentScope<'_>,
    snapshot: &ContentSnapshot,
    content: &str,
    org_id: Option<&str>,
    blobs: &dyn PromotionBlobStore,
    dek: &[u8; 32],
) -> Result<bool, String> {
    let key = coralswarm_core::r2::blob_key_for(org_id, snapshot.graph_id, Uuid::new_v4())
        .map_err(|error| error.to_string())?;
    replace_with_key(
        scope,
        snapshot,
        content,
        snapshot.header_len,
        "promoted-blob-backfill",
        Some(blobs),
        Some(dek),
        key,
    )
    .await
}

async fn replace_with_key(
    scope: ContentScope<'_>,
    snapshot: &ContentSnapshot,
    content: &str,
    header_len: Option<i32>,
    device_id: &str,
    blobs: Option<&dyn PromotionBlobStore>,
    dek: Option<&[u8; 32]>,
    key: String,
) -> Result<bool, String> {
    let uploaded = if let Some(blobs) = blobs {
        let dek = dek.ok_or_else(|| "DEK required for content replacement".to_string())?;
        blobs
            .put_blob(&key, content.as_bytes(), dek)
            .await
            .map_err(|e| e.to_string())?;
        Some(key.clone())
    } else {
        if snapshot.inline.is_none() {
            return Err("blob store required for content replacement without inline content".into());
        }
        None
    };

    let mut tx = match scope.begin().await {
        Ok(tx) => tx,
        Err(error) => {
            cleanup_uncommitted_blob_uploads(blobs, uploaded.as_slice()).await;
            return Err(error.to_string());
        }
    };
    let result = replace_tx(
        &mut tx,
        scope.account_id(),
        snapshot,
        content,
        header_len,
        &key,
        device_id,
    )
    .await;
    match result {
        Ok(true) => {
            // A failed COMMIT response can follow a committed pointer publication.
            tx.commit().await.map_err(|e| e.to_string())?;
            Ok(true)
        }
        Ok(false) => {
            tx.rollback().await.map_err(|e| e.to_string())?;
            cleanup_uncommitted_blob_uploads(blobs, uploaded.as_slice()).await;
            Ok(false)
        }
        Err(error) => {
            if tx.rollback().await.is_ok() {
                cleanup_uncommitted_blob_uploads(blobs, uploaded.as_slice()).await;
            }
            Err(error.to_string())
        }
    }
}

async fn replace_tx(
    tx: &mut Transaction<'_, Postgres>,
    account_id: Uuid,
    snapshot: &ContentSnapshot,
    content: &str,
    header_len: Option<i32>,
    published_key: &str,
    device_id: &str,
) -> Result<bool, StoreError> {
    let graph: Option<Uuid> =
        sqlx::query_scalar("SELECT id FROM graphs WHERE id=$1 AND account_id=$2 FOR SHARE")
            .bind(snapshot.graph_id)
            .bind(account_id)
            .fetch_optional(&mut **tx)
            .await?;
    if graph.is_none() {
        return Ok(false);
    }
    if !publish_tx(tx, snapshot, content, header_len, published_key, device_id, None).await? {
        return Ok(false);
    }
    enqueue_tx(
        tx,
        &JobKind::EmbedAtoms {
            atom_ids: vec![snapshot.atom_id],
        },
        Some(account_id),
        None,
        0,
    )
    .await?;
    Ok(true)
}

/// Write-time columns derived from a replacement chunk's text. Supplied by
/// [`crate::chunk_reconcile`] so a positional chunk replaced in place carries
/// the same excerpt/title/symbols/hash a fresh insert of that text would.
#[derive(Debug, Clone, Default)]
pub(crate) struct DerivedColumns {
    pub(crate) excerpt: Option<String>,
    pub(crate) title: Option<String>,
    pub(crate) title_kind: Option<String>,
    pub(crate) symbols: Option<String>,
    /// Whole-request hash (`atoms.content_hash`); `None` writes NULL.
    pub(crate) content_hash: Option<String>,
    /// `atoms.chunk_sha256` of `content` under `header_len`.
    pub(crate) chunk_sha256: String,
    /// `None` leaves `participants` unchanged.
    pub(crate) participants: Option<Vec<String>>,
    /// Write `content_inline` even when the row had none (no object store:
    /// dev/inline mode, where inline text is the only copy).
    pub(crate) write_inline: bool,
}

/// The revision CAS every in-place content replacement goes through, in the
/// caller's transaction. Publishes `content` at `published_key` only if the
/// row still matches `snapshot` exactly (revision, locator, inline text,
/// header length, timestamp). The old locator is parked in
/// `superseded_blob_keys`, the managed embedding is cleared so the next
/// `EmbedAtoms` run re-embeds it (with a fresh attempts count: the old
/// count was spent on the old text), and one sync content op is appended. The
/// triggers bump `content_revision`. Inline text is written only when the
/// row already stored inline text, or `derived.write_inline` asks for it.
/// The caller enqueues embedding.
///
/// `derived: None` keeps the historical statement (the
/// `atoms_chunk_sha256_invalidate` trigger clears the chunk hash, because the
/// content changed and no new hash was supplied).
pub(crate) async fn publish_tx(
    tx: &mut Transaction<'_, Postgres>,
    snapshot: &ContentSnapshot,
    content: &str,
    header_len: Option<i32>,
    published_key: &str,
    device_id: &str,
    derived: Option<&DerivedColumns>,
) -> Result<bool, StoreError> {
    const CAS: &str = "WHERE atom_id=$1 AND graph_id=$2 AND content_revision=$6
             AND content_blob_key=$7 AND content_inline IS NOT DISTINCT FROM $8
             AND header_len IS NOT DISTINCT FROM $9 AND timestamp=$10";
    const SET: &str = "UPDATE atoms SET content_inline=$3,content_blob_key=$4,header_len=$5,
             superseded_blob_keys=CASE WHEN content_blob_key<>'' AND NOT
                 (content_blob_key=ANY(superseded_blob_keys))
                 THEN array_append(superseded_blob_keys,content_blob_key)
                 ELSE superseded_blob_keys END,
             embedding_next=NULL,embed_attempts=0,embed_model_next=CASE WHEN embed_model_next IN
                 ('skipped:local-only','blocked:managed_purpose_unsupported')
                 THEN embed_model_next ELSE NULL END";
    let sql = match derived {
        None => format!("{SET}\n         {CAS}"),
        Some(_) => format!(
            "{SET},excerpt=$11,title=$12,title_kind=$13,symbols=$14,content_hash=$15,
             chunk_sha256=$16,participants=COALESCE($17,participants)\n         {CAS}"
        ),
    };
    let inline = match derived {
        Some(d) if d.write_inline => Some(content),
        _ => snapshot.inline.as_ref().map(|_| content),
    };
    let mut query = sqlx::query(&sql)
        .bind(snapshot.atom_id)
        .bind(snapshot.graph_id)
        .bind(inline)
        .bind(published_key)
        .bind(header_len)
        .bind(snapshot.revision)
        .bind(&snapshot.blob_key)
        .bind(&snapshot.inline)
        .bind(snapshot.header_len)
        .bind(snapshot.timestamp);
    if let Some(d) = derived {
        query = query
            .bind(&d.excerpt)
            .bind(&d.title)
            .bind(&d.title_kind)
            .bind(&d.symbols)
            .bind(&d.content_hash)
            .bind(&d.chunk_sha256)
            .bind(&d.participants);
    }
    let changed = query.execute(&mut **tx).await?.rows_affected();
    if changed == 0 {
        return Ok(false);
    }
    append_op_tx(
        tx,
        &sync::atom_content_op(
            Uuid::new_v4(),
            snapshot.atom_id,
            snapshot.graph_id,
            content,
            Utc::now(),
            device_id,
        ),
    )
    .await?;
    Ok(true)
}

#[cfg(test)]
mod pg_tests;
