//! Helpers left behind by the retired per-atom entity extraction worker.
//!
//! `ExtractEntities` and `ExtractRelationships` were retired with source-agnostic
//! pipeline PR 11 (managed extraction, `workers::source_atoms`, replaced them).
//! What remains here is shared by live code: the chunk-suffix strip used by
//! name corrections and re-embedding, and the R2-only atom fetch.

use coralswarm_core::r2::R2Client;
use sqlx::PgPool;
use uuid::Uuid;

use crate::tenancy::TenantId;

/// Fetch and decrypt an R2-only atom's text. `Ok(None)` when the blob decrypts
/// to empty or whitespace-only text: retrying cannot change that, and sending
/// it on would pay for a model call that can only return nothing.
pub(crate) async fn fetch_r2_only_content(
    tenant: &TenantId,
    pool: &PgPool,
    r2: Option<&R2Client>,
    account_id: Uuid,
    blob_key: &str,
) -> Result<Option<String>, String> {
    let r2_client =
        r2.ok_or_else(|| "R2 not configured — cannot fetch R2-only atom content".to_string())?;
    let dek = crate::crypto::resolve_dek_for_tenant(tenant, pool, account_id)
        .await
        .map_err(|e| format!("DEK resolution failed: {e}"))?
        .ok_or_else(|| "account has no DEK — run key rotation first".to_string())?;
    let bytes = r2_client
        .get_atom_blob(blob_key, &dek)
        .await
        .map_err(|e| format!("R2 fetch failed: {e}"))?;
    let text = String::from_utf8_lossy(&bytes).into_owned();
    if text.trim().is_empty() {
        return Ok(None);
    }
    Ok(Some(text))
}

/// [`fetch_r2_only_content`] for `backend/tests/*.rs`, which link this crate
/// as an ordinary dependency and cannot reach `pub(crate)` items.
#[cfg(feature = "test-support")]
pub async fn test_support_fetch_r2_only_content(
    tenant: &TenantId,
    pool: &PgPool,
    r2: Option<&R2Client>,
    account_id: Uuid,
    blob_key: &str,
) -> Result<Option<String>, String> {
    fetch_r2_only_content(tenant, pool, r2, account_id, blob_key).await
}

/// Strip a trailing `":chunk:<N>"` suffix from a chunk atom's `source_id`,
/// returning the base source id (`routes::ingest` names chunks
/// `"<base>:chunk:<position>"`). Ids without a numeric chunk suffix are
/// returned unchanged.
pub(crate) fn base_source_id(source_id: &str) -> String {
    if let Some(idx) = source_id.rfind(":chunk:") {
        let suffix = &source_id[idx + ":chunk:".len()..];
        if !suffix.is_empty() && suffix.bytes().all(|b| b.is_ascii_digit()) {
            return source_id[..idx].to_string();
        }
    }
    source_id.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn base_source_id_strips_chunk_suffix() {
        assert_eq!(base_source_id("meeting-123:chunk:0"), "meeting-123");
        assert_eq!(base_source_id("meeting-123:chunk:42"), "meeting-123");
        assert_eq!(base_source_id("teams:chunk:notes:chunk:3"), "teams:chunk:notes");
    }

    #[test]
    fn base_source_id_leaves_non_chunk_ids_unchanged() {
        assert_eq!(base_source_id("meeting-123"), "meeting-123");
        assert_eq!(base_source_id("meeting-123:chunk:abc"), "meeting-123:chunk:abc");
        assert_eq!(base_source_id("meeting-123:chunk:"), "meeting-123:chunk:");
    }
}
