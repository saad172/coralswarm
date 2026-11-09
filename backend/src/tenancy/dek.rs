//! "Provision the org DEK if absent" — the first-provision half of
//! `routes::admin::rotate_dek`'s org-plane arm, extracted so the self-serve
//! provisioning job (PR A1b) and the internal endpoint share one definition
//! (audit row 4f).
//!
//! This function only ever MINTS a key into an empty slot. It never rewraps
//! and never rotates: an existing `account_keys` row is left byte-identical
//! and reported as [`DekProvision::AlreadyPresent`]. Wrong-key encryption is
//! unrecoverable (org-tenancy plan 1.3), which is why the destructive paths
//! stay behind the operator endpoint's explicit `?force=true` and nowhere
//! else.
//!
//! `pool` MUST be the org's own tenant pool — the fingerprint-verified one
//! `TenantRegistry::get_or_build` returns. `account_keys` is off
//! `0009_tenant_rls`'s policy roster, so the insert needs no scoped
//! transaction (same reasoning as the endpoint's own comment).

use coralswarm_core::kek::{generate_dek, wrap_dek, KekError};
use sqlx::PgPool;
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DekProvision {
    /// A fresh DEK was generated, wrapped under the current KEK and stored.
    Provisioned,
    /// `account_keys` already held a wrapped DEK; nothing was written.
    AlreadyPresent,
}

#[derive(Debug, thiserror::Error)]
pub enum DekProvisionError {
    #[error("KEK: {0}")]
    Kek(#[from] KekError),
    #[error("account_keys write: {0}")]
    Db(#[from] sqlx::Error),
}

/// Insert a wrapped DEK for `account_id` into this tenant's `account_keys`
/// unless one exists. `ON CONFLICT DO NOTHING` makes the decision and the
/// write one statement, so two concurrent first-provision calls can never
/// both mint (the loser's row is simply not written).
pub async fn provision_org_dek_if_absent(
    pool: &PgPool,
    account_id: Uuid,
    kek: &[u8; 32],
) -> Result<DekProvision, DekProvisionError> {
    let dek = generate_dek();
    let wrapped = wrap_dek(kek, &dek)?;
    let result = sqlx::query(
        "INSERT INTO account_keys (account_id, dek_wrapped, rotated_at) \
         VALUES ($1, $2, now()) \
         ON CONFLICT (account_id) DO NOTHING",
    )
    .bind(account_id)
    .bind(&wrapped)
    .execute(pool)
    .await?;
    Ok(if result.rows_affected() == 1 {
        DekProvision::Provisioned
    } else {
        DekProvision::AlreadyPresent
    })
}
