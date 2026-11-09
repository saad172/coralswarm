//! Control-plane accessors for `tenant_databases`, the org → tenant-DB routing
//! table (migration 0078). The tenant pool registry (PR 1.1) and the request
//! binding gate (PR 1.2) consult these to resolve which per-org Postgres serves
//! a given organization or subdomain.

use chrono::{DateTime, Utc};
use sqlx::PgPool;
use uuid::Uuid;

/// Lifecycle status of a tenant database. Only [`TenantStatus::Active`] is
/// servable by the binding gate; every other state must reject at the routing
/// layer (provisioning = mid-onboarding, suspended = offboarding write-freeze,
/// offboarded = kept for audit after teardown).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TenantStatus {
    Provisioning,
    Active,
    Suspended,
    Offboarded,
}

impl TenantStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            TenantStatus::Provisioning => "provisioning",
            TenantStatus::Active => "active",
            TenantStatus::Suspended => "suspended",
            TenantStatus::Offboarded => "offboarded",
        }
    }

    /// Parse the stored text. An unrecognized value maps to `None` so callers
    /// fail closed (an unknown status is never treated as servable).
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "provisioning" => Some(TenantStatus::Provisioning),
            "active" => Some(TenantStatus::Active),
            "suspended" => Some(TenantStatus::Suspended),
            "offboarded" => Some(TenantStatus::Offboarded),
            _ => None,
        }
    }

    /// Whether the binding gate may build a pool and serve requests for a tenant
    /// in this status. Only `Active`.
    pub fn is_servable(self) -> bool {
        matches!(self, TenantStatus::Active)
    }
}

/// One row of `tenant_databases`. `db_secret_ref` is the NAME of a secret
/// holding the tenant DSN — never the DSN or any credential itself.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct TenantDatabase {
    pub clerk_org_id: String,
    pub account_id: Uuid,
    pub slug: String,
    pub db_secret_ref: String,
    pub region: Option<String>,
    pub substrate: String,
    pub schema_version: Option<i64>,
    pub status: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl TenantDatabase {
    /// Parsed status; `None` if the stored value is unrecognized (fail closed).
    pub fn status(&self) -> Option<TenantStatus> {
        TenantStatus::parse(&self.status)
    }

    /// Whether the gate may serve this tenant (status == active).
    pub fn is_servable(&self) -> bool {
        self.status().map(TenantStatus::is_servable).unwrap_or(false)
    }
}

// Column list kept in one place so every accessor selects the same shape the
// `FromRow` derive expects. No user input is ever interpolated into SQL text —
// only this constant — and values are always bound as parameters.
const SELECT_COLS: &str = "clerk_org_id, account_id, slug, db_secret_ref, region, \
     substrate, schema_version, status, created_at, updated_at";

/// Resolve a routing row by subdomain slug (the binding gate's hot path — the
/// slug is parsed from the request `Host`). `None` if no org owns that subdomain.
pub async fn lookup_by_slug(
    pool: &PgPool,
    slug: &str,
) -> Result<Option<TenantDatabase>, sqlx::Error> {
    let q = format!("SELECT {SELECT_COLS} FROM tenant_databases WHERE slug = $1");
    sqlx::query_as::<_, TenantDatabase>(&q)
        .bind(slug)
        .fetch_optional(crate::tenancy::scope::request_read(pool))
        .await
}

/// Resolve a routing row by Clerk org id (the token-claim path).
pub async fn lookup_by_org(
    pool: &PgPool,
    clerk_org_id: &str,
) -> Result<Option<TenantDatabase>, sqlx::Error> {
    let q = format!("SELECT {SELECT_COLS} FROM tenant_databases WHERE clerk_org_id = $1");
    sqlx::query_as::<_, TenantDatabase>(&q)
        .bind(clerk_org_id)
        .fetch_optional(crate::tenancy::scope::request_read(pool))
        .await
}

/// org again" fact. Added for member-private-oceans PR 7 (§D "Org-revocation
/// interaction"): `routes::clerk_webhook::provision_member_ocean_best_effort`
/// (trigger 1) is not otherwise gated against a revoked org the way
/// `require_auth`'s first-sight path is (that path never reaches
/// `TenantRuntime::finish_binding` at all for a revoked org, because
/// `middleware::auth::live_org_membership`'s `account_members` re-check
/// already 403s upstream — see that function's own doc comment). A Clerk
/// webhook delivery carries no such upstream fence, so this is the direct
/// check for that one caller.
///
/// Also used by PR 8 (purge; plan §2.6 "Never purge for an org in
/// `org_revocations`") — `workers::member_ocean_reconciler::purge_arm` calls
/// this before touching anything for a given org, in addition to (not
/// instead of) its per-graph routing-status re-checks, since a revoked org's
/// tenant DB may already be mid- or post-teardown by the time a purge tick
/// runs. PR 8 originally carried its own byte-identical copy of this
/// function; deduped into this one definition on rebase once PR 7 merged
/// first.
///
/// Fresh on every call, never cached: a revocation is never cleared, so
/// there is no staleness risk, only the cost of one indexed lookup.
pub async fn org_is_revoked(pool: &PgPool, clerk_org_id: &str) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM org_revocations WHERE clerk_org_id = $1)")
        .bind(clerk_org_id)
        .fetch_one(pool)
        .await
}

/// All routing rows with status `active` whose org has not been revoked —
/// the worker tenant loop enumerates these (only active tenants serve
/// traffic).
///
/// #1396: a Clerk `organization.deleted` writes `org_revocations` (through
/// `clerk_provision::revoke_org_access`) but cannot move
/// `tenant_databases.status`: that column is writable by the table owner and
/// the provisioner's definers only (controls 0083/0381, guarded by
/// `scripts/check_provisioning_privileges.sh`). So the revocation itself is
/// the stop signal: a revoked org drops out of this list on the harness's
/// next refresh, every tenant-loop worker stops scheduling it, and its pool
/// ages out of the registry. A revocation is never cleared (Clerk never
/// reuses an org id), so this cannot un-stop a tenant by accident. The row's
/// status is left for the offboarding job (#1395) to move.
///
/// One worker still needs the revoked orgs: the member-ocean reconciler's
/// archive arm carries out the archive intents `revoke_org_access` writes
/// (stamping `archived_at` and revoking the share tokens). It opts in through
/// `HarnessConfig::including_revoked_orgs`, which adds
/// [`list_active_revoked`] to this list for that harness only.
pub async fn list_active(pool: &PgPool) -> Result<Vec<TenantDatabase>, sqlx::Error> {
    let q = format!(
        "SELECT {SELECT_COLS} FROM tenant_databases t \
         WHERE t.status = 'active' \
           AND NOT EXISTS (SELECT 1 FROM org_revocations r WHERE r.clerk_org_id = t.clerk_org_id) \
         ORDER BY t.created_at"
    );
    sqlx::query_as::<_, TenantDatabase>(&q).fetch_all(pool).await
}

/// The complement of [`list_active`] within `status = 'active'`: routing
/// rows whose org HAS been revoked (Clerk `organization.deleted`) but whose
/// tenant database is still up. Only the member-ocean reconciler reads these,
/// so its archive arm can stamp `archived_at` and revoke the share tokens of
/// a deleted org's members (#1396 review). Its restore, provision and purge
/// arms already refuse a revoked org.
pub async fn list_active_revoked(pool: &PgPool) -> Result<Vec<TenantDatabase>, sqlx::Error> {
    let q = format!(
        "SELECT {SELECT_COLS} FROM tenant_databases t \
         WHERE t.status = 'active' \
           AND EXISTS (SELECT 1 FROM org_revocations r WHERE r.clerk_org_id = t.clerk_org_id) \
         ORDER BY t.created_at"
    );
    sqlx::query_as::<_, TenantDatabase>(&q).fetch_all(pool).await
}

/// Rows whose schema the migrate_tenants orchestrator should keep current:
/// `active` (live) and `provisioning` (mid-onboarding, being brought up).
/// Suspended/offboarded tenants are intentionally excluded — their schema is
/// frozen.
///
/// Also excluded: a `provisioning` row whose self-serve job
/// (`tenant_provisioning_jobs`, migration 0381) is not `ready`. That job owns
/// the tenant's migration until it activates the row (its own migrate step
/// runs the same orchestrator in-process and asserts `schema_version` before
/// activating), and its database may not even accept connections yet. A
/// release that tried it could abort the deploy for a tenant nobody is
/// serving — the "abandoned onboarding bricks releases" failure (plan OPS-5,
/// audit 4b) that `TENANT_MIGRATE_RELEASE=warn` exists to work around.
/// Concierge rows (no job) are selected exactly as before.
pub async fn list_migratable(pool: &PgPool) -> Result<Vec<TenantDatabase>, sqlx::Error> {
    let q = format!(
        "SELECT {SELECT_COLS} FROM tenant_databases t \
         WHERE t.status IN ('active', 'provisioning') \
           AND NOT EXISTS (SELECT 1 FROM tenant_provisioning_jobs j \
                            WHERE j.clerk_org_id = t.clerk_org_id AND j.status <> 'ready') \
         ORDER BY t.created_at"
    );
    sqlx::query_as::<_, TenantDatabase>(&q).fetch_all(pool).await
}

/// What [`reconcile_slug`] did (or deliberately declined to do) with a Clerk
/// `organization.updated` event. Every variant except [`Self::Renamed`] leaves
/// `tenant_databases` byte-identical; the caller logs and, for `Renamed`, drops
/// the affected routing-cache keys.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SlugReconcile {
    /// This org has no routing row — it was never onboarded onto a tenant DB.
    /// A pure no-op, NOT an error: most Clerk orgs in the account are consumer
    /// orgs that will never have a tenant database, and every one of their
    /// `organization.updated` events lands here.
    NotOnboarded,
    /// The routing row already carried this slug; only the ordering watermark
    /// advanced. Advancing it even on a no-change event is load-bearing — see
    /// [`reconcile_slug`]'s doc for the rename-and-rename-back case.
    Unchanged { slug: String },
    /// Reconciled: the routing row now points at `new`. The caller MUST
    /// invalidate the routing cache for `old`, `new`, and the org id.
    Renamed { old: String, new: String },
    /// The event is not newer than the last one applied to this row, so it was
    /// ignored — an out-of-order or replayed delivery. `current` is the slug
    /// that stays in force.
    Stale {
        current: String,
        applied_at: DateTime<Utc>,
    },
    /// The proposed slug is not a legal tenant slug (reserved apex label, or
    /// outside the charset/length the `tenant_databases` CHECKs and
    /// `gate::is_valid_slug` both enforce). Routing is left on the old slug.
    Invalid { slug: String },
    /// The proposed slug is already owned by a DIFFERENT tenant (the `slug`
    /// UNIQUE constraint refused the update). Routing is left on the old slug
    /// and the watermark is NOT advanced, so a later, legal rename still
    /// applies.
    Conflict { current: String, wanted: String },
    /// The proposed slug has the self-serve routing key's shape
    /// (`o-` + 16 lowercase hex, [`is_selfserve_slug_shape`]). Those keys
    /// are derived server-side from an account id and belong to self-serve
    /// orgs only; a concierge org taking one could claim a pending self-serve
    /// org's key (its `insert_routing` would then refuse). Refused; routing
    /// and the watermark are left as they were.
    ReservedSelfServeShape { slug: String },
    /// The org is self-serve (it has a provisioning job, or its routing slug
    /// is the derived shape). Its routing key is opaque and never follows
    /// the Clerk slug (decision D1), so the event is ignored: no write, no
    /// watermark.
    SelfServe { current: String },
}

/// `^o-[0-9a-f]{16}$`: the self-serve routing key
/// (`provisioning::opaque_slug_for_account`, pinned by control 0381's CHECKs).
pub fn is_selfserve_slug_shape(slug: &str) -> bool {
    let b = slug.as_bytes();
    b.len() == 18
        && b.starts_with(b"o-")
        && b[2..].iter().all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(c))
}

/// Reconcile `tenant_databases.slug` for `clerk_org_id` to `new_slug`, as
/// carried by a Clerk `organization.updated` event stamped `clerk_updated_at`
/// (the event's own `data.updated_at`, i.e. CLERK's clock, never ours).
///
/// This exists because `tenant_databases.slug == the Clerk org slug` is a hard
/// correctness requirement, not a convention: the web app builds the tenant
/// host from `useOrganization().organization.slug` and has no other source
/// (`/v1/me` is itself behind the binding gate), while the gate resolves the
/// routing row from that Host. Onboarding asserts parity once
/// (`orgt_verify_clerk_slug`); a rename afterwards silently re-breaks a LIVE
/// tenant — every org user 404s — with no operator in the loop. So we
/// reconcile rather than merely alert.
///
/// **Ordering.** Clerk does not guarantee webhook ordering and Svix
/// redelivers, so arrival order is not authority order. An event applies only
/// when `clerk_updated_at` is STRICTLY newer than the row's
/// `clerk_slug_updated_at` watermark (migration 0088); otherwise it is
/// [`SlugReconcile::Stale`] and changes nothing. Two consequences worth
/// spelling out:
///
///   * a redelivery of an already-applied event (equal timestamp) is a no-op,
///     so this is idempotent; and
///   * the watermark advances even when the slug did not change
///     ([`SlugReconcile::Unchanged`]). That is what makes rename-and-rename-back
///     safe: given `t=100: acme→beta` and `t=200: beta→acme` delivered in
///     reverse, the `t=200` event is a no-change event on a row still reading
///     `acme` — if it did not stamp the watermark, the delayed `t=100` event
///     would then "win" and leave routing on `beta`, which Clerk has not said
///     since t=100.
///
/// **Safety.** The row is `SELECT ... FOR UPDATE`-locked for the whole
/// decision, so concurrent deliveries for one org cannot interleave a
/// read-then-write. A slug that is reserved/malformed is refused before any
/// write; a slug already owned by another tenant is refused BY the `slug`
/// UNIQUE constraint inside the transaction, which then rolls back — a
/// collision can never leave routing half-updated or two tenants sharing a
/// slug. In both refusal cases the watermark is left untouched so the next
/// legal rename is still applied.
///
/// Never creates a row: an org with no routing row is
/// [`SlugReconcile::NotOnboarded`], a no-op.
pub async fn reconcile_slug(
    pool: &PgPool,
    clerk_org_id: &str,
    new_slug: &str,
    clerk_updated_at: DateTime<Utc>,
) -> Result<SlugReconcile, sqlx::Error> {
    let mut tx = pool.begin().await?;

    // Lock the row for the whole read-decide-write, so two concurrent
    // deliveries for the same org serialize instead of racing the watermark.
    let existing: Option<(String, Option<DateTime<Utc>>)> = sqlx::query_as(
        "SELECT slug, clerk_slug_updated_at FROM tenant_databases \
         WHERE clerk_org_id = $1 FOR UPDATE",
    )
    .bind(clerk_org_id)
    .fetch_optional(&mut *tx)
    .await?;

    // Not onboarded → never write, never error. Checked BEFORE slug validity so
    // a consumer org with an unroutable slug is a quiet no-op rather than a
    // loud (and useless) rejection about a tenant that does not exist.
    let Some((current, watermark)) = existing else {
        tx.rollback().await?;
        return Ok(SlugReconcile::NotOnboarded);
    };

    // A self-serve org's routing key is derived, not the Clerk slug: never
    // rename it. Self-serve means a provisioning job exists for the org
    // (any status), or the row already carries the derived shape.
    let has_job: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM tenant_provisioning_jobs WHERE clerk_org_id = $1)",
    )
    .bind(clerk_org_id)
    .fetch_one(&mut *tx)
    .await?;
    if has_job || is_selfserve_slug_shape(&current) {
        tx.rollback().await?;
        return Ok(SlugReconcile::SelfServe { current });
    }

    // Nor may a concierge org take the derived shape.
    if is_selfserve_slug_shape(new_slug) {
        tx.rollback().await?;
        return Ok(SlugReconcile::ReservedSelfServeShape {
            slug: new_slug.to_string(),
        });
    }

    // Defense in depth with the `tenant_databases` CHECKs (migrations 0078 +
    // 0079): refuse a reserved apex label or an out-of-charset slug here, so
    // the common case never relies on catching a constraint violation.
    if !super::gate::is_valid_slug(new_slug) {
        tx.rollback().await?;
        return Ok(SlugReconcile::Invalid {
            slug: new_slug.to_string(),
        });
    }

    if let Some(applied_at) = watermark {
        if clerk_updated_at <= applied_at {
            tx.rollback().await?;
            return Ok(SlugReconcile::Stale {
                current,
                applied_at,
            });
        }
    }

    let updated = sqlx::query(
        "UPDATE tenant_databases \
            SET slug = $1, clerk_slug_updated_at = $2, updated_at = now() \
          WHERE clerk_org_id = $3",
    )
    .bind(new_slug)
    .bind(clerk_updated_at)
    .bind(clerk_org_id)
    .execute(&mut *tx)
    .await;

    match updated {
        Ok(_) => {
            tx.commit().await?;
            if current == new_slug {
                Ok(SlugReconcile::Unchanged {
                    slug: new_slug.to_string(),
                })
            } else {
                Ok(SlugReconcile::Renamed {
                    old: current,
                    new: new_slug.to_string(),
                })
            }
        }
        Err(e) => {
            // The transaction is already aborted; roll it back explicitly so
            // nothing about this event survives, then classify.
            let _ = tx.rollback().await;
            match e.as_database_error().and_then(|d| d.code()).as_deref() {
                // 23505 unique_violation — another tenant already owns the slug.
                Some("23505") => Ok(SlugReconcile::Conflict {
                    current,
                    wanted: new_slug.to_string(),
                }),
                // 23514 check_violation — reserved/malformed slug. Unreachable
                // behind `is_valid_slug` above unless the two drift; if they
                // ever do, the database is the backstop and we still refuse
                // cleanly instead of 500ing.
                Some("23514") => Ok(SlugReconcile::Invalid {
                    slug: new_slug.to_string(),
                }),
                _ => Err(e),
            }
        }
    }
}

/// Record the tenant tree version last applied to a tenant's database.
///
/// The direct UPDATE the release's owner-role `migrate_tenants` has always
/// used. Any other role is denied it (`42501`: migration 0083 revokes UPDATE
/// on `tenant_databases` from `coralswarm_app`, and `coralswarm_provisioner`
/// never had it) and gets that error back, never a silent success. The
/// self-serve provisioning job records its version through
/// [`record_schema_version_via_definer`] instead, under its lease. A missing
/// routing row is an error, never `Ok`.
pub async fn set_schema_version(
    pool: &PgPool,
    clerk_org_id: &str,
    version: i64,
) -> Result<(), sqlx::Error> {
    let r = sqlx::query(
        "UPDATE tenant_databases SET schema_version = $1, updated_at = now() \
         WHERE clerk_org_id = $2",
    )
    .bind(version)
    .bind(clerk_org_id)
    .execute(pool)
    .await?;
    if r.rows_affected() == 1 {
        Ok(())
    } else {
        Err(sqlx::Error::RowNotFound)
    }
}

/// Highest `schema_version` the 0381 definer accepts: tenant migration files
/// carry four-digit numbers (`scripts/check_migrations.sh`).
pub const MAX_TENANT_SCHEMA_VERSION: i64 = 9999;

/// Record `schema_version` through migration 0381's definer function, as the
/// lease holder (`lease_owner`, renewed to `lease_ttl`) of the org's live
/// provisioning job on `migrate`, and read back what landed. Only
/// `coralswarm_provisioner` may call it. Errors (rather than returning) when
/// the stored value is not `version` — the whole point is that a version
/// write which did not land is never reported as success.
pub async fn record_schema_version_via_definer(
    pool: &PgPool,
    clerk_org_id: &str,
    lease_owner: &str,
    lease_ttl: std::time::Duration,
    version: i64,
) -> Result<(), sqlx::Error> {
    let stored: i64 =
        sqlx::query_scalar("SELECT tenant_provisioning_record_schema_version($1, $2, $3, $4)")
            .bind(clerk_org_id)
            .bind(lease_owner)
            .bind(lease_ttl)
            .bind(version)
            .fetch_one(pool)
            .await?;
    if stored != version {
        return Err(sqlx::Error::Protocol(format!(
            "schema_version for {clerk_org_id}: wrote {version}, read back {stored}"
        )));
    }
    Ok(())
}

// ── Self-serve provisioning writes (migration 0381 definer functions) ─────────
//
// No role but the release's owner has table write on `tenant_databases`
// (0083); these are the narrow writes a provisioning job is allowed, each
// checked by the function itself and granted to `coralswarm_provisioner`
// only (the worker's own pool). See the migration for the invariants.

/// Insert the routing row as `provisioning`. `Ok(true)` inserted, `Ok(false)`
/// an identical `provisioning` row already existed (a retry converging).
pub async fn provisioning_insert_routing(
    pool: &PgPool,
    clerk_org_id: &str,
    account_id: Uuid,
    slug: &str,
    secret_ref: &str,
    region: Option<&str>,
) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar("SELECT tenant_provisioning_insert_routing($1, $2, $3, $4, $5)")
        .bind(clerk_org_id)
        .bind(account_id)
        .bind(slug)
        .bind(secret_ref)
        .bind(region)
        .fetch_one(pool)
        .await
}

/// Flip `provisioning → active` and the job to `ready` atomically, provided
/// `schema_version == expected_version`. `Ok(false)` when already active.
pub async fn provisioning_activate(
    pool: &PgPool,
    clerk_org_id: &str,
    expected_version: i64,
) -> Result<bool, sqlx::Error> {
    sqlx::query_scalar("SELECT tenant_provisioning_activate($1, $2)")
        .bind(clerk_org_id)
        .bind(expected_version)
        .fetch_one(pool)
        .await
}

/// Delete a `provisioning` routing row during cleanup. Returns rows deleted
/// (0 when already gone). Refused by the function for any other status.
pub async fn provisioning_delete_routing(
    pool: &PgPool,
    clerk_org_id: &str,
) -> Result<i32, sqlx::Error> {
    sqlx::query_scalar("SELECT tenant_provisioning_delete_routing($1)")
        .bind(clerk_org_id)
        .fetch_one(pool)
        .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_roundtrips_and_only_active_is_servable() {
        for s in [
            TenantStatus::Provisioning,
            TenantStatus::Active,
            TenantStatus::Suspended,
            TenantStatus::Offboarded,
        ] {
            assert_eq!(TenantStatus::parse(s.as_str()), Some(s));
        }
        assert!(TenantStatus::Active.is_servable());
        assert!(!TenantStatus::Provisioning.is_servable());
        assert!(!TenantStatus::Suspended.is_servable());
        assert!(!TenantStatus::Offboarded.is_servable());
    }

    #[test]
    fn unknown_status_fails_closed() {
        assert_eq!(TenantStatus::parse("bogus"), None);
    }
}
