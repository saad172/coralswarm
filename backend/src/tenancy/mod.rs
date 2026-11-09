//! Database-per-org tenancy.
//!
//! The seam between the shared control/consumer plane (identity, routing, all
//! personal-user content) and the per-org tenant databases (all of an org's
//! content):
//!
//! * [`routing`] — control-plane accessors for `tenant_databases` (migration
//!   0078), the org → tenant-DB routing authority.
//! * [`secrets`] — resolve a secret *reference* (the name in
//!   `tenant_databases.db_secret_ref`) to a tenant DSN. Raw DSNs never live in
//!   the routing table and are never logged. Environment first, then the
//!   KEK-encrypted `tenant_db_credentials` table ([`credentials`]) — the
//!   self-serve successor to one Fly secret per tenant.
//! * [`dek`] — provision an org's DEK if absent; shared by the internal
//!   `rotate-dek` endpoint and the self-serve provisioning job.
//! * [`provisioning`] / [`provisioner`] — the self-serve tenant provisioning
//!   job (a durable step state machine in `tenant_provisioning_jobs`) and the
//!   Neon / local database provisioners it drives. Driven by
//!   `workers::tenant_provisioning` behind `ORG_SELFSERVE_SIGNUP_ENABLED`.
//! * [`registry`] — the [`registry::TenantRegistry`]: lazy, single-flight,
//!   idle-evicted per-tenant connection pools, each fingerprint-verified against
//!   the tenant DB's own `tenant_meta.org_id` before it is ever published.
//! * [`reachability`] — the [`reachability::ReachabilityResolver`] (PR 1.7):
//!   the one place `list_reachable_graphs`/`graph_reachable_by_user`'s D9
//!   union splits into a consumer-plane joined query vs. an org-tenant-plane
//!   two-step, once `account_members` (control) and `graphs`/`teams` (tenant)
//!   no longer share a database.
//! * [`scope`] — the consumer-plane RLS backstop's `SET LOCAL app.account_id`
//!   plumbing (PR 3.4, migration 0083_consumer_rls — see `scope`'s own doc
//!   for why it isn't a method on `PostgresStore`). Live enforcement since
//!   OPS-4 flipped the app's connection role off `BYPASSRLS` (verified in
//!   production 2026-08-04).
//! * [`member_ocean`] — shared provisioning helper for member-private-oceans'
//!   request-adjacent triggers (the Clerk webhook and request-path
//!   first-sight; the reconciler sweep is `workers::member_ocean_reconciler`)
//!   — inert unless `MEMBER_OCEANS_ENABLED` is set.
//!
//! The request binding gate (PR 1.2) wires these into `AppState` and resolves a
//! `TenantContext` per request.

pub mod credentials;
pub mod dek;
pub mod extractor;
pub mod gate;
pub(crate) mod member_ocean;
pub(crate) mod personal_org;
pub mod migrate;
pub mod org_selfserve_cleanup;
pub mod provisioner;
pub mod provisioning;
pub mod reachability;
pub mod read_only_poison;
pub mod registry;
pub mod routing;
pub mod runtime;
pub mod scope;
pub mod secrets;

// ── TenantId ──────────────────────────────────────────────────────────────────

/// Which plane a DEK, a `DekCache` entry, or a database pool belongs to.
///
/// This is the key dimension threaded through [`crate::dek_cache::DekCache`]
/// and the DEK-resolution helpers in [`crate::crypto`] (org-tenancy PR 1.3):
/// `Consumer` is the shared control/consumer DB, where personal accounts'
/// `accounts.dek_wrapped` lives; `Org(clerk_org_id)` is a specific tenant
/// database, where that org's `account_keys.dek_wrapped` lives (relocated
/// there so a control-plane compromise alone yields zero content-decryption
/// keys).
///
/// Wrong-key encryption is unrecoverable, so every DEK use site pairs a
/// `TenantId` with the pool it rode in on (both live together on
/// [`runtime::TenantContext`]) and asserts they match rather than ever
/// falling back across planes. `clerk_org_id` is a Clerk organization id, not
/// secret, and safe to log (already logged elsewhere on this path, e.g.
/// `registry::TenantPoolError`).
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum TenantId {
    /// The shared control/consumer plane (personal accounts).
    Consumer,
    /// A specific org's tenant database, identified by its Clerk org id.
    Org(String),
}

impl TenantId {
    /// The `TenantId` a resolved [`runtime::TenancyDecision`] represents.
    pub fn from_decision(decision: &runtime::TenancyDecision) -> Self {
        match decision {
            runtime::TenancyDecision::Personal => TenantId::Consumer,
            runtime::TenancyDecision::Org(ctx) => TenantId::Org(ctx.org_id.clone()),
        }
    }
}

#[cfg(test)]
mod tenant_id_tests {
    use super::*;
    use runtime::{TenancyDecision, TenantContext};
    use uuid::Uuid;

    #[test]
    fn from_decision_maps_personal_to_consumer() {
        assert_eq!(
            TenantId::from_decision(&TenancyDecision::Personal),
            TenantId::Consumer
        );
    }

    // `connect_lazy` still requires a Tokio context to construct (even though
    // it defers the actual connection), so this needs an async test runtime.
    #[tokio::test]
    async fn from_decision_maps_org_to_org_with_its_id() {
        let ctx = TenantContext {
            org_id: "org_a".to_string(),
            account_id: Uuid::new_v4(),
            user_id: Uuid::new_v4(),
            // A lazy pool never connects unless a query is executed against
            // it, so this is safe to construct in a unit test with no DB.
            pool: sqlx::postgres::PgPoolOptions::new()
                .connect_lazy("postgres://localhost/unused")
                .expect("lazy pool construction never connects"),
            page_pool: None,
        };
        assert_eq!(
            TenantId::from_decision(&TenancyDecision::Org(ctx)),
            TenantId::Org("org_a".to_string())
        );
    }

    #[test]
    fn distinct_orgs_are_distinct_cache_keys() {
        assert_ne!(TenantId::Org("org_a".into()), TenantId::Org("org_b".into()));
        assert_ne!(TenantId::Consumer, TenantId::Org("org_a".into()));
    }
}
