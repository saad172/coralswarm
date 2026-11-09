//! [`PromotionBlobStore`] — the narrow blob-transport abstraction shared by
//! `services::deletion` and `services::retraction` (and their `routes/`
//! callers) for the R2 operations those two services need: read a coral's
//! content for cascade/copy bookkeeping, and delete it on cascade-delete or
//! unpublish/retraction cleanup.
//!
//! Split out of the now-deleted `services::promotion` (org-tenancy PR #414,
//! "promotion never crosses a database") — the trait itself was never
//! promotion-specific (it is a thin `get`/`put`/`delete` wrapper over
//! [`R2Client`]), only its *former* neighbor in that module was. Keeping the
//! name rather than renaming on the way out: it is `pub` API referenced from
//! several call sites and a rename here is pure churn with no behavior
//! change.
use async_trait::async_trait;

use coralswarm_core::r2::{R2Client, R2Error};

#[async_trait]
pub trait PromotionBlobStore: Send + Sync {
    /// Download + decrypt the blob at `blob_key` with `dek`.
    async fn get_blob(&self, blob_key: &str, dek: &[u8; 32]) -> Result<Vec<u8>, R2Error>;
    /// Encrypt `content` with `dek` and upload at `blob_key`.
    async fn put_blob(&self, blob_key: &str, content: &[u8], dek: &[u8; 32])
        -> Result<(), R2Error>;
    /// Delete the object at `blob_key` (rollback cleanup; idempotent).
    async fn delete_blob(&self, blob_key: &str) -> Result<(), R2Error>;
}

#[async_trait]
impl PromotionBlobStore for R2Client {
    async fn get_blob(&self, blob_key: &str, dek: &[u8; 32]) -> Result<Vec<u8>, R2Error> {
        self.get_atom_blob(blob_key, dek).await
    }
    async fn put_blob(
        &self,
        blob_key: &str,
        content: &[u8],
        dek: &[u8; 32],
    ) -> Result<(), R2Error> {
        self.put_atom_blob_at(blob_key, content, dek).await
    }
    async fn delete_blob(&self, blob_key: &str) -> Result<(), R2Error> {
        self.delete_atom_blob(blob_key).await
    }
}

/// Best-effort cleanup only after the caller knows its transaction did not
/// commit. Never call this after an ambiguous COMMIT/network response.
/// `true` when every key is gone.
pub(crate) async fn cleanup_uncommitted_uploads(
    r2: Option<&coralswarm_core::r2::R2Client>, keys: &[String],
) -> bool {
    cleanup_uncommitted_blob_uploads(r2.map(|r2| r2 as &dyn PromotionBlobStore), keys).await
}

/// [`cleanup_uncommitted_uploads`] through any blob store. `true` when every
/// key is gone.
pub(crate) async fn cleanup_uncommitted_blob_uploads(
    blobs: Option<&dyn PromotionBlobStore>, keys: &[String],
) -> bool {
    let Some(blobs)=blobs else { return keys.is_empty() };
    let mut all_gone=true;
    for key in keys {
        if let Err(error)=blobs.delete_blob(key).await {
            tracing::warn!(%error,"uncommitted source upload cleanup failed");
            all_gone=false;
        }
    }
    all_gone
}
