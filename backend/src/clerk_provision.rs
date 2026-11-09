//! Shared Clerk → internal tenancy provisioning (ADR-0007 D2-impl/D3).
//!
//! These helpers map Clerk identities onto the internal user/account model and
//! are idempotent, so they are safe to call from both paths that provision:
//! `require_auth` (lazy, on a verified session JWT) and the Clerk webhook
//! (eager, on `organization*` events). Model B: a per-user personal
//! account-of-one (clerk_org_id NULL) holds the private Primary ocean; an org
//! account (clerk_org_id set) holds the org + team oceans.

use sqlx::PgPool;
use uuid::Uuid;

use crate::AppState;

#[derive(Debug, thiserror::Error)]
pub enum ProvisionError {
    #[error("database error: {0}")]
    Db(#[from] sqlx::Error),
    #[error("store error: {0}")]
    Store(String),
    /// Org-creation gate (org-tenancy PR 3.3): `ORG_SELFSERVE_DISABLED` is on
    /// and this org has no `accounts` row and no `tenant_databases` routing
    /// row yet — see [`ensure_org_account`]'s doc comment for the exact
    /// behavior. A clean, permanent rejection (never transient), so callers
    /// must NOT treat it like [`ProvisionError::Db`]/[`ProvisionError::Store`]
    /// (which the Clerk webhook retries on) — the webhook handler
    /// (`routes::clerk_webhook`) acks it as a no-op instead.
    #[error("organization not provisioned: self-serve org creation is disabled")]
    SelfServeDisabled,
    /// Legacy-token fence (org-tenancy PR 4.2): this org has been wholesale-
    /// revoked (`organization.deleted`, migration 0082's `org_revocations`)
    /// and has no pre-existing control `accounts` row — see
    /// [`ensure_org_account`]'s doc comment for the exact residual this
    /// closes. A clean, permanent rejection like [`ProvisionError::
    /// SelfServeDisabled`]: callers must not retry it.
    #[error("organization not provisioned: organization has been revoked")]
    OrgRevoked,
    /// Settings launch 2.4: this Clerk user's account deletion exists (running
    /// or completed). Permanent, like [`ProvisionError::OrgRevoked`]: a stale
    /// token or a late webhook for a deleted Clerk user must not provision a
    /// fresh user for the same Clerk id.
    #[error("user not provisioned: the account has been deleted")]
    AccountDeleted,
    /// Billing self-serve PR F (seat lifecycle): [`ensure_org_membership`]/
    /// [`remove_org_membership`] propagate a seat acquire/release failure as
    /// this variant — the Clerk webhook handler turns it into a 5xx exactly
    /// like [`ProvisionError::Db`], so Clerk's redelivery is the retry
    /// mechanism (see those functions' own doc comments, "Seat acquisition"/
    /// "Seat release").
    #[error("seat lifecycle error: {0}")]
    SeatLifecycle(#[from] crate::billing::seat_lifecycle::SeatLifecycleError),
    /// [`ensure_org_membership`]'s removal fence (settings PR 2.9): the
    /// member was tombstoned after the caller read their membership from
    /// Clerk. Transient on purpose — the webhook 5xxs, Svix redelivers, and
    /// the next attempt re-reads Clerk.
    #[error("membership removed after the Clerk lookup; retry against current Clerk state")]
    RemovedSinceObserved,
}

/// Best-effort: auto-grant the `free` plan and enable billing enforcement for
/// a newly-provisioned PERSONAL account (enforcement-rollout PR), so a real
/// (if small) free tier exists before enforcement can ever block anyone.
///
/// Mirrors [`provision_account_dek`]'s posture exactly: a failure here must
/// not fail account creation — a user without an account is a worse outcome
/// than a user whose free grant is delayed a beat and gets picked up by a
/// later reconciliation pass.
///
/// The two writes are deliberately ORDERED, not merely sequential:
/// `accounts.billing_enforcement_enabled` is set ONLY after
/// [`crate::billing::issuance::issue_grant`] has actually succeeded (or the
/// account already held a grant from a prior pass), so an account can never
/// end up enforced with nothing to spend — that would recreate, for a
/// brand-new user, the exact "blocked on the very first request, before any
/// real free-tier experience" failure the enforcement rollout exists to
/// avoid. See `billing::issuance`'s own doc comment for why a grant seeds a
/// real, unconsumed `member_allowance_periods` row rather than only a
/// `grants` row.
///
/// Caller contract: `account_members` for `account_id` must already hold the
/// account's current roster (this function does not create memberships) —
/// `issue_grant` rolls the first allowance period from that roster inside
/// its own transaction, so calling this before the owner's `account_members`
/// row exists would seed a period for zero members and grant nobody
/// anything usable. Returns `true` iff the account holds an active `free`
/// grant afterward (freshly issued or already present) — the signal
/// [`enable_billing_enforcement`] must gate on.
async fn auto_grant_free_plan(pool: &PgPool, account_id: Uuid) -> bool {
    let plan = match crate::billing::issuance::resolve_active_plan(pool, "free").await {
        Ok(Some(p)) => p,
        Ok(None) => {
            tracing::error!(
                account_id = %account_id,
                "billing: no active 'free' plan found (see migration 0153_billing_plans_seed.sql) \
                 — skipping auto-grant"
            );
            return false;
        }
        Err(e) => {
            tracing::warn!(
                error = %e, account_id = %account_id,
                "billing: free-plan lookup failed — skipping auto-grant"
            );
            return false;
        }
    };
    match crate::billing::issuance::issue_grant(
        pool,
        account_id,
        &plan,
        None,
        None,
        Some("auto-granted at signup"),
    )
    .await
    {
        Ok(_) => true,
        Err(e) => {
            tracing::warn!(
                error = %e, account_id = %account_id,
                "billing: auto-grant of the free plan failed"
            );
            false
        }
    }
}

/// Flip `accounts.billing_enforcement_enabled` on. Callers MUST only call
/// this after confirming the account holds a real, usable grant (a `true`
/// from [`auto_grant_free_plan`]) — an account enforced with nothing to
/// spend would be blocked on its very first request, before any real
/// free-tier experience, which is the exact failure this rollout exists to
/// avoid. Best-effort, same posture as every other write in this file.
async fn enable_billing_enforcement(pool: &PgPool, account_id: Uuid) {
    if let Err(e) =
        sqlx::query("UPDATE accounts SET billing_enforcement_enabled = true WHERE id = $1")
            .bind(account_id)
            .execute(pool)
            .await
    {
        tracing::warn!(
            error = %e, account_id = %account_id,
            "billing: failed to enable enforcement for this account"
        );
    }
}

/// Grant the `free` plan and enable billing enforcement together, in order —
/// [`auto_grant_free_plan`] then [`enable_billing_enforcement`] (see each for
/// the full rationale and the ordering contract between them). Called for
/// BOTH personal accounts ([`ensure_personal_account`]) and org accounts
/// ([`ensure_org_account`]) — enforcement-completion PR (plan §1.9/§2.A:
/// "enforcement goes universal before money arrives").
///
/// **Now reused as-is for org accounts — here is why that is safe.** A
/// personal account already holds the owner's `account_members` row by the
/// time this runs, so the grant's first allowance period seeds correctly and
/// flipping enforcement is trivially sound, unchanged from before. An org
/// account has NO members at all at the moment [`ensure_org_account`]
/// creates it — membership arrives later via [`ensure_org_membership`] — so
/// this same call still seeds a period for zero members (harmless, mirrors
/// `bin/grant_plan`'s own documented "no members yet" case): nobody has an
/// `account_members` row yet, so nobody can call
/// `billing::enforcement::is_blocked` for this account either, and there is
/// therefore nothing for the newly-enabled flag to block between org
/// creation and the first member joining.
///
/// The gap that used to make flipping unsafe here — a member joining an
/// enforced-but-empty org would find no `member_allowance_periods` row for
/// themselves, fall through to the account-level pool check
/// (`enforcement::org_pool_exhausted`), and find it empty (no purchase yet),
/// which reads as blocked once a real balance check is added on top — is
/// closed by EVERY code path that can write a member's first
/// `account_members` row for an org: the webhook-authoritative
/// [`ensure_org_membership`] on every membership upsert, and
/// [`refresh_org_membership_from_claims`] on first-sight request-path
/// provisioning (the ONLY path in a webhook-less environment, and the one
/// that can race the webhook). Both now call
/// [`crate::billing::renewal::seed_member_period`], which seeds THAT
/// member's own current-period allowance mid-period (the FULL, unprorated
/// allowance — matching the existing mid-period-seat-add precedent, plan
/// §1.7) as part of the SAME event that makes them a member. The join that
/// would otherwise first expose an unseeded member to enforcement is also
/// the join that seeds their allowance, so there is no window in between
/// where a member exists but has nothing to draw on.
///
/// `pub(crate)` (rather than private) solely so
/// `billing::renewal::pg_tests` can exercise the org-flip invariant directly
/// against a scaffolded org account without standing up the full
/// `require_auth`/webhook router harness that [`ensure_org_account`] itself
/// needs — see that test module for the exact scaffold.
pub(crate) async fn provision_billing(pool: &PgPool, account_id: Uuid) {
    if auto_grant_free_plan(pool, account_id).await {
        enable_billing_enforcement(pool, account_id).await;
    }
}

/// Best-effort: give a freshly-created account a server-custodied DEK so cloud
/// ingest/synthesis works immediately (the interim server-trust cloud model,
/// ADR-0004; the browser-E2E server-blind private ocean of D5 is the in-browser
/// engine phase). Idempotent — only sets `dek_wrapped` when absent — and a no-op
/// when `MASTER_KEK_HEX` is unset (dev), in which case the account can still be
/// provisioned later via the internal rotate-dek endpoint.
async fn provision_account_dek(pool: &PgPool, account_id: Uuid) -> Result<(), ProvisionError> {
    use coralswarm_core::kek::{generate_dek, load_kek, wrap_dek};
    let kek = match load_kek() {
        Ok(k) => k,
        Err(e) => {
            tracing::warn!(error = %e, account_id = %account_id, "MASTER_KEK unavailable — skipping DEK auto-provision");
            return Ok(());
        }
    };
    let wrapped =
        wrap_dek(&kek, &generate_dek()).map_err(|e| ProvisionError::Store(e.to_string()))?;
    sqlx::query("UPDATE accounts SET dek_wrapped = $1 WHERE id = $2 AND dek_wrapped IS NULL")
        .bind(&wrapped)
        .bind(account_id)
        .execute(pool)
        .await?;
    Ok(())
}

/// Create a brand-new account's first ocean under an RLS-scoped transaction
/// (org-tenancy OPS-4 follow-up — the F2 provisioning break).
///
/// `graphs` is one of migration `0083_consumer_rls`'s 15 direct-`account_id`
/// tables, and its `account_isolation` policy is `USING`-only — with no
/// `FOR` clause that is `FOR ALL`, and Postgres reuses a missing `WITH CHECK`
/// as the INSERT check — so once OPS-4 pointed the app at the non-`BYPASSRLS`
/// `coralswarm_app` role, the previous unscoped `Store::create_graph`
/// (`INSERT INTO graphs ... .fetch_one(&self.pool)`) started raising
/// `unrecognized configuration parameter "app.account_id"` on EVERY
/// first-sight provision. That is the whole of `require_auth`'s
/// `resolve_clerk_context` → `ensure_personal_account` path, so a brand-new
/// user's very first authenticated request 500'd and they never got an
/// account. This routes it through `create_graph_scoped` instead.
///
/// **Ordering (the reason this is safe to scope to an account that was
/// created moments ago):** both callers have already COMMITTED the
/// `accounts` row before reaching here — the `INSERT INTO accounts ...
/// RETURNING id` runs as its own autocommit statement on `pool`, not inside
/// any open transaction — so the account exists on disk by the time this
/// opens its scoped transaction. It does not actually need to: 0083's
/// `graphs` policy compares the NEW ROW's own `account_id` column against
/// the `app.account_id` GUC and never joins `accounts`/`account_members`, so
/// scoping to the just-created id is valid regardless of what is or is not
/// visible in the control tables. (`graphs.account_id`'s FK to `accounts` is
/// checked by the referential-integrity machinery, which bypasses RLS
/// anyway.) There is therefore no chicken-and-egg here and nothing to
/// re-order: the account id is known, committed, and is exactly the scope
/// the INSERT must satisfy. The `is_primary` sub-select inside
/// `create_graph_scoped` (`NOT EXISTS (SELECT 1 FROM graphs WHERE
/// account_id = $1 AND is_primary)`) reads under the SAME GUC, so it still
/// sees this account's own oceans and the first personal ocean still becomes
/// primary — unchanged behavior.
///
/// The transaction wraps only the graph INSERT, deliberately: the surrounding
/// provisioning steps (`accounts`, `account_members`, `account_keys` via
/// `provision_account_dek`) are on CONTROL-plane tables with no 0083 policy,
/// and they were already three independent autocommit statements before this
/// change. Widening the transaction to cover them would change the crash-
/// atomicity of a hot auth path, which is not this fix's job.
///
/// `pool` is always the shared consumer/control pool here (`AppState::db`,
/// threaded down from `require_auth`/the Clerk webhook) and `state.store` is
/// always the `PostgresStore` built over that same pool.
/// `owner_user_id` (identity-resolution-at-the-door §10.3): the account's
/// owner, used ONLY when `kind == "personal"` to bind a self reef on the
/// freshly-created ocean — see the `kind == "personal"` branch below for why
/// the org commons path (`kind == "org"`) deliberately skips this. Callers
/// already have this value in hand (`ensure_personal_account`'s own
/// `user_id`, `ensure_org_account`'s own `owner_user_id` parameter), so
/// threading it through costs nothing extra.
async fn create_first_ocean_scoped(
    state: &AppState,
    pool: &PgPool,
    account_id: Uuid,
    owner_user_id: Uuid,
    name: &str,
    kind: &str,
) -> Result<(), ProvisionError> {
    let Some(store) = &state.store else {
        return Ok(());
    };
    let mut tx = crate::tenancy::scope::begin_account_scoped(pool, account_id)
        .await
        .map_err(|e| ProvisionError::Store(e.to_string()))?;
    let graph = store
        .create_graph_scoped(&mut tx, account_id, name, kind)
        .await
        .map_err(|e| ProvisionError::Store(e.to_string()))?;

    // Self reef on every PERSONAL ocean (design doc §10.3 — "ensure_self_node
    // is called from ocean provisioning: primary ocean creation ..."). NOT
    // for `kind == "org"`: the org commons ocean is shared by every member
    // and has no single owner in the self-reef sense (that's what
    // `tenancy::member_ocean::provision_one`'s per-member private oceans are
    // for — see that call site's own comment). `users` is reachable on this
    // SAME `pool`/`tx` (the personal-account provisioning path is always the
    // shared consumer/control plane — see `create_first_ocean_scoped`'s own
    // header doc), so this needs no cross-plane fetch, unlike the org-tenant
    // member-ocean path.
    if kind == "personal" {
        let user: Option<(Option<String>, String)> =
            sqlx::query_as("SELECT name, email FROM users WHERE id = $1")
                .bind(owner_user_id)
                .fetch_optional(&mut *tx)
                .await
                .map_err(ProvisionError::from)?;
        if let Some((name_opt, email)) = user {
            let (title, needs_name) = crate::store::resolve_principal_title_scoped(
                &mut tx,
                graph.id,
                name_opt.as_deref(),
                &email,
            )
            .await;
            match store
                .ensure_self_node_scoped(&mut tx, graph.id, owner_user_id, &title)
                .await
            {
                Ok((node_id, _self_title)) => {
                    if needs_name {
                        crate::store::door_stamp_needs_name(&mut tx, node_id)
                            .await
                            .map_err(|e| ProvisionError::Store(e.to_string()))?;
                    }
                    crate::store::bind_principal_identifiers(&mut tx, graph.id, node_id, owner_user_id)
                        .await
                        .map_err(|e| ProvisionError::Store(e.to_string()))?;
                }
                Err(e) => {
                    // Best-effort, matching this whole function's existing
                    // posture toward the ocean's OWN write (see the account-
                    // scoping doc above) — a self-reef failure must never
                    // strand a brand-new account without its Primary ocean.
                    tracing::warn!(
                        error = %e,
                        %account_id,
                        graph_id = %graph.id,
                        "create_first_ocean_scoped: self-reef binding failed (non-fatal)"
                    );
                }
            }
        }
    }

    tx.commit()
        .await
        .map_err(|e| ProvisionError::Store(e.to_string()))?;
    Ok(())
}

/// Map a Clerk org role (`"org:admin"`, `"org:member"`, …) to our RBAC set.
pub fn map_org_role(org_role: Option<&str>) -> &'static str {
    match org_role {
        // Exact match only: a custom Clerk role that merely CONTAINS "admin"
        // (e.g. `org:billing_admin`) must not become a full org admin.
        Some("org:admin") | Some("admin") => "admin",
        _ => "member",
    }
}

/// Map a Clerk user id to an internal `users.id`, linking an existing email-code
/// user with the same email or inserting a new row. `email_hint` (e.g. from a
/// webhook payload) avoids a Backend-API round-trip; without it we fetch the
/// primary email, falling back to a synthetic address so a transient failure
/// doesn't block provisioning. `name_hint` follows the same shape for
/// `users.name` (nullable — unlike email there is no synthetic fallback; "no
/// name available" is a legitimate, permanent outcome for some users). Only
/// consulted on FIRST sight of a `clerk_user_id` (the early-return branch
/// below never re-checks it) — `resolve_clerk_context` is what keeps an
/// EXISTING user's name fresh on every subsequent auth (see its own doc
/// comment), so this function's Clerk-API name fallback only ever fires
/// once per identity, same as the existing email fallback.
pub async fn ensure_user(
    state: &AppState,
    pool: &PgPool,
    clerk_user_id: &str,
    email_hint: Option<&str>,
    name_hint: Option<&str>,
) -> Result<Uuid, ProvisionError> {
    // Settings launch 2.4: a Clerk id whose account deletion exists (pending,
    // running or done) is never provisioned or resolved here. Checked BEFORE
    // the existing-user lookup so a late webhook for a user mid-deletion is
    // refused too, and the anonymized `users` row (which no longer carries the
    // id) can never be followed by a fresh insert for the same Clerk id.
    if sqlx::query_scalar::<_, Option<String>>("SELECT account_deletion_state_for_clerk_user($1)")
        .bind(clerk_user_id)
        .fetch_one(crate::tenancy::scope::request_read(pool))
        .await?
        .is_some()
    {
        return Err(ProvisionError::AccountDeleted);
    }
    if let Some(id) = sqlx::query_scalar::<_, Uuid>("SELECT id FROM users WHERE clerk_user_id = $1")
        .bind(clerk_user_id)
        .fetch_optional(crate::tenancy::scope::request_read(pool))
        .await?
    {
        return Ok(id);
    }
    let email = match email_hint {
        Some(e) if !e.is_empty() => e.to_string(),
        _ => crate::clerk::fetch_primary_email(clerk_user_id, &state.http_client)
            .await
            .unwrap_or_else(|| format!("{clerk_user_id}@users.clerk.local")),
    };
    let name: Option<String> = match name_hint {
        Some(n) if !n.trim().is_empty() => Some(n.trim().to_string()),
        _ => crate::clerk::fetch_display_name(clerk_user_id, &state.http_client).await,
    };
    // Link to an existing row with this email ONLY when it is unclaimed
    // (clerk_user_id IS NULL — a legacy email-code user signing in via Clerk for
    // the first time). The `WHERE` guard means we never hijack a row already
    // bound to a different Clerk identity (account-takeover guard). `name` is
    // COALESCE'd on the conflict branch so re-linking a legacy row that
    // already had a name some other way (there is currently no other way,
    // but this keeps the merge non-destructive) never clobbers it with NULL.
    let linked = sqlx::query_scalar::<_, Uuid>(
        "INSERT INTO users (email, clerk_user_id, name) VALUES ($1, $2, $3) \
         ON CONFLICT (email) DO UPDATE \
            SET clerk_user_id = EXCLUDED.clerk_user_id, \
                name          = COALESCE(EXCLUDED.name, users.name), \
                updated_at    = now() \
            WHERE users.clerk_user_id IS NULL \
         RETURNING id",
    )
    .bind(&email)
    .bind(clerk_user_id)
    .bind(&name)
    .fetch_optional(pool)
    .await?;
    if let Some(id) = linked {
        return Ok(id);
    }
    // The email is taken by a different identity (DO UPDATE matched nothing). If
    // a concurrent provision just linked our clerk_user_id, use it; otherwise
    // refuse rather than take over someone else's account.
    if let Some(id) = sqlx::query_scalar::<_, Uuid>("SELECT id FROM users WHERE clerk_user_id = $1")
        .bind(clerk_user_id)
        .fetch_optional(crate::tenancy::scope::request_read(pool))
        .await?
    {
        return Ok(id);
    }
    Err(ProvisionError::Store(format!(
        "email already linked to a different identity for clerk_user {clerk_user_id}"
    )))
}

/// Keep an EXISTING user's `users.name` fresh from the verified token's
/// `name` claim on every authenticated request (`resolve_clerk_context`).
/// Deliberately does NOT fall back to a live Clerk API lookup here (unlike
/// `ensure_user`'s one-time provisioning fetch) — that would put a Clerk
/// Backend API round-trip on the hot path of every single authenticated
/// request for any user whose token doesn't carry `name` (which is most
/// session-JWT users; Clerk **session** tokens generally don't carry OIDC
/// profile claims). `name_hint` is free — it comes from the token
/// verification that already happened — so refreshing from it is cheap; a
/// no-op `UPDATE` (`IS DISTINCT FROM`) when nothing changed. Best-effort: a
/// DB error here is logged and swallowed, never fails the request.
///
/// Callers should dispatch this off the request's own critical path (see
/// `resolve_clerk_context`'s `tokio::spawn` + bounded timeout around this
/// call) — nothing downstream in the SAME request depends on the refresh
/// having completed (the next read of `users.name`, e.g.
/// `mcp::tools::write::caller_display_name`, only ever happens on a LATER
/// request), so awaiting it inline would stall every authenticated request
/// on a non-critical `UPDATE` for no benefit.
pub async fn sync_user_display_name(pool: &PgPool, user_id: Uuid, name_hint: Option<&str>) {
    let Some(name) = name_hint.map(str::trim).filter(|n| !n.is_empty()) else {
        return;
    };
    if let Err(e) = sqlx::query(
        "UPDATE users SET name = $2, updated_at = now() \
         WHERE id = $1 AND name IS DISTINCT FROM $2",
    )
    .bind(user_id)
    .bind(name)
    .execute(pool)
    .await
    {
        tracing::warn!(error = %e, user_id = %user_id, "failed to refresh users.name from Clerk claims");
    }
}

/// The user's personal account-of-one, creating it plus a private Primary ocean
/// on first sight. Since 2026-09-08 that account is EITHER a row with no
/// `clerk_org_id` or a row whose `clerk_org_id` is the caller's own personal
/// org, so the lookup is shared with [`crate::personal_account::personal_account_id`]
/// rather than spelled out twice.
pub async fn ensure_personal_account(
    state: &AppState,
    pool: &PgPool,
    user_id: Uuid,
) -> Result<Uuid, ProvisionError> {
    // A caller's personal home is EITHER an account with no `clerk_org_id`
    // (the original shape) OR the account carrying their own personal
    // organization (control migration 0208 + `adopt_personal_org_account`).
    //
    // Both must resolve here, and the personal-org one has to be tried FIRST.
    // After the adopt tool runs, a user has NO `clerk_org_id IS NULL` account
    // at all — so a lookup that only knew the old shape would fall through to
    // the INSERT below and hand them a BRAND-NEW EMPTY account with a fresh
    // "Personal" ocean, while their real content sat under the org-labelled
    // account they can no longer reach by this path.
    //
    // That is not hypothetical: MCP tokens carry no `org_id` while
    // `user:org:read` is unadvertised, so EVERY connector call takes this path.
    //
    // This calls `personal_account::personal_account_id` rather than keeping its
    // own copy of the same two arms: that function IS this find step, so the
    // two cannot drift apart — there is structurally only one query left to
    // disagree with.
    if let Some(id) = crate::personal_account::personal_account_id(pool, user_id).await? {
        return Ok(id);
    }
    // Race-safe create: a concurrent first request might insert the same
    // personal account. ON CONFLICT on the partial unique index makes the loser
    // return None; it then reads the winner's account (which already has its
    // Primary ocean) instead of 500ing or creating a duplicate.
    let inserted = sqlx::query_scalar::<_, Uuid>(
        "INSERT INTO accounts (owner_user_id) VALUES ($1) \
         ON CONFLICT (owner_user_id) WHERE clerk_org_id IS NULL DO NOTHING \
         RETURNING id",
    )
    .bind(user_id)
    .fetch_optional(pool)
    .await?;
    let account_id = match inserted {
        Some(id) => id,
        None => {
            return sqlx::query_scalar::<_, Uuid>(
                "SELECT id FROM accounts WHERE owner_user_id = $1 AND clerk_org_id IS NULL LIMIT 1",
            )
            .bind(user_id)
            .fetch_one(crate::tenancy::scope::request_read(pool))
            .await
            .map_err(ProvisionError::from);
        }
    };
    // `seat_tier_id` explicit `NULL` (billing self-serve PR F, seat
    // lifecycle): a personal account is never seat-priced —
    // `account_members_seat_tier_iff_org` (0169) forbids anything else for a
    // non-org member — and 0176's trigger hardening means an org-member
    // insert with no explicit tier now hard-errors, so every writer states
    // its NULL/tier choice explicitly rather than leaning on the trigger's
    // (now removed) silent default.
    sqlx::query(
        "INSERT INTO account_members (account_id, user_id, role, seat_tier_id) \
         VALUES ($1, $2, 'owner', NULL) \
         ON CONFLICT (account_id, user_id) DO NOTHING",
    )
    .bind(account_id)
    .bind(user_id)
    .execute(pool)
    .await?;
    // Auto-grant `free` + enable enforcement (enforcement-rollout PR). Must
    // run AFTER the `account_members` insert just above — see
    // `provision_billing`'s "Caller contract".
    provision_billing(pool, account_id).await;
    // RLS-scoped (org-tenancy OPS-4 follow-up, F2): `graphs` is RLS-covered,
    // so this INSERT must run inside `begin_account_scoped` on the account
    // committed just above — see `create_first_ocean_scoped` for the full
    // ordering argument.
    create_first_ocean_scoped(state, pool, account_id, user_id, "Personal", "personal").await?;
    provision_account_dek(pool, account_id).await?;
    Ok(account_id)
}

/// The org account mirroring a Clerk Organization, creating it (plus the org
/// commons ocean) on first sight. `owner_user_id` seeds `accounts.owner_user_id`
/// when creating; membership is managed separately via [`ensure_org_membership`].
pub async fn ensure_org_account(
    state: &AppState,
    pool: &PgPool,
    clerk_org_id: &str,
    owner_user_id: Uuid,
    name: Option<&str>,
) -> Result<Uuid, ProvisionError> {
    if let Some(id) =
        sqlx::query_scalar::<_, Uuid>("SELECT id FROM accounts WHERE clerk_org_id = $1")
            .bind(clerk_org_id)
            .fetch_optional(crate::tenancy::scope::request_read(pool))
            .await?
    {
        return Ok(id);
    }

    // Legacy-token fence (org-tenancy PR 4.2), residual named in the PR 2.2
    // review: refuse to create a BRAND-NEW control `accounts` row for an org
    // that has already been wholesale-revoked (`organization.deleted`,
    // migration 0082's `org_revocations`). Without this, a stale/still-valid
    // org token for an org whose deletion webhook landed before ANY member of
    // it ever signed into this backend — so no `accounts` row exists yet to
    // gate this function's INSERT on below — would still create an empty
    // shell `accounts` row here, even though the live membership re-check
    // downstream (`middleware::auth::live_org_membership`, called by both
    // `OrgMembershipPolicy` branches in `resolve_clerk_context`) rejects the
    // request either way — this is a data-hygiene residual (an orphaned
    // shell row), not an access leak. `org_revocations` carries the Clerk
    // org id as plain TEXT (no FK to `accounts`), so the guard works whether
    // or not a row exists yet.
    //
    // The `NOT EXISTS` guard is folded directly into the INSERT statement
    // below (`WHERE NOT EXISTS (...)`) rather than checked as a separate
    // preceding `SELECT` — a prior version of this fence used a standalone
    // `SELECT EXISTS` here, which left a TOCTOU window: a concurrent
    // `organization.deleted` webhook could commit its `org_revocations`
    // insert AFTER this check read "not revoked" but BEFORE the `accounts`
    // INSERT below committed, still creating the shell row (CodeRabbit review
    // on PR #280). Folding the guard into the INSERT's own `WHERE` clause
    // means the revocation predicate and the row creation are decided by ONE
    // statement against one consistent snapshot, closing that window. Note
    // the 0082 constraint trigger does NOT already cover this: it only fires
    // on `account_members` inserts, never `accounts` — this table has no
    // trigger-level backstop, so the atomicity has to come from the
    // statement shape itself.
    //
    // Org-creation gate (org-tenancy PR 3.3). Reconciles with the
    // shadow-provisioning fence just below (which stops a first-sight org
    // JWT/webhook from creating a graph+DEK on the shared plane): THIS gate
    // stops it from creating the control `accounts` row in the first place,
    // so that once self-serve is disabled, `onboard_tenant.sh` (concierge
    // onboarding) is the only path that brings a new org into existence here.
    //
    // Detection: we already know (just above) that no `accounts` row exists
    // for this org. By construction it can therefore have no
    // `tenant_databases` routing row either —
    // `tenant_databases.account_id NOT NULL REFERENCES accounts(id)`
    // (migration 0078) makes a routing row referencing a nonexistent account
    // impossible. The `lookup_by_org` check below is kept anyway as an
    // explicit, testable statement of that invariant rather than an implicit
    // assumption: if a routing row is ever found here, this org is already
    // further along than "brand-new" and is let through unchanged.
    //
    // Exact behavior for a genuinely un-onboarded org id under the flag: NO
    // rows are written at all — no `accounts`, no graph, no DEK (fail
    // closed) — a clean, permanent rejection
    // (`ProvisionError::SelfServeDisabled`), never a partial/shadow account.
    //
    // Reconciliation note (named cutover dependency, not solved in this PR):
    // because of the FK above, once the flag is on, the webhook/JWT path can
    // no longer create the FIRST `accounts` row for a genuinely new org
    // either — `onboard_tenant.sh`'s own step 1 already depends on that row
    // existing before it runs (created today by an admin's first sign-in).
    // Accepted sequencing until a dedicated internal provisioning endpoint
    // exists: for each new partner, let the org admin sign in once (flag
    // still off, or before the org exists at all under the flag) so the
    // `accounts` row is created, THEN flip `ORG_SELFSERVE_DISABLED=true` (or
    // rely on it already being on with the account pre-seeded), THEN run
    // `onboard_tenant.sh`. This flag's job is the backstop for STRAY/leftover
    // org claims against orgs nobody is actively onboarding, not the primary
    // control against self-serve creation — that's the Clerk instance
    // setting (`createOrganizationEnabled=false`), which stops an end user
    // from ever causing this function to run for a brand-new org id at all.
    if let Some(rt) = state.tenant_runtime.as_ref() {
        if rt.org_selfserve_disabled {
            let routed = crate::tenancy::routing::lookup_by_org(pool, clerk_org_id)
                .await?
                .is_some();
            if !routed {
                return Err(ProvisionError::SelfServeDisabled);
            }
        }
    }

    // Race-safe: a webhook and the creator's first JWT can provision the same
    // org concurrently. ON CONFLICT on the partial unique index lets the loser
    // return None and read the winner's account (with its org ocean). The
    // `SELECT ... WHERE NOT EXISTS (org_revocations)` (rather than a plain
    // `VALUES (...)`) is the atomic legacy-token-fence guard described above
    // — one statement, one snapshot, no separate check-then-insert window.
    let inserted = sqlx::query_scalar::<_, Uuid>(
        "INSERT INTO accounts (owner_user_id, clerk_org_id) \
         SELECT $1, $2 \
         WHERE NOT EXISTS (SELECT 1 FROM org_revocations WHERE clerk_org_id = $2) \
         ON CONFLICT (clerk_org_id) WHERE clerk_org_id IS NOT NULL DO NOTHING \
         RETURNING id",
    )
    .bind(owner_user_id)
    .bind(clerk_org_id)
    .fetch_optional(pool)
    .await?;
    let id = match inserted {
        Some(id) => id,
        None => {
            // Zero rows affected is ambiguous on its own — it means EITHER
            // (a) a concurrent creator/webhook already inserted this org (the
            // pre-existing `ON CONFLICT` race, unrelated to revocation), or
            // (b) the `NOT EXISTS org_revocations` guard blocked the insert
            // (this org is wholesale-revoked and had no prior row). Re-read
            // by clerk_org_id to disambiguate: a row now existing is (a) —
            // return the winner's account, same as before this fence
            // existed; no row is (b) — reject, never silently fall through.
            match sqlx::query_scalar::<_, Uuid>("SELECT id FROM accounts WHERE clerk_org_id = $1")
                .bind(clerk_org_id)
                .fetch_optional(crate::tenancy::scope::request_read(pool))
                .await?
            {
                Some(id) => id,
                None => return Err(ProvisionError::OrgRevoked),
            }
        }
    };
    // Self-serve signup PR A2: an org `POST /v1/orgs` created gets no `free`
    // grant and no shared-plane ocean, whichever path writes its `accounts`
    // row first (the route itself, Clerk's `organization.created` webhook,
    // or the creator's first org JWT). Its plan arrives with its Checkout
    // (the trial's `team` grant) and its ocean with its tenant database. The
    // route records the Clerk org id on its intent row in the same
    // transaction that creates the row via [`ensure_selfserve_org_account`],
    // so a webhook that loses the insert race reads it here. (A webhook that
    // lands before Clerk's create response reaches the route can still take
    // this path first; under `ORG_SELFSERVE_DISABLED` it is refused above,
    // and otherwise the `free` grant it leaves is unreachable: the org has
    // no routing row, so the binding gate refuses every request to it, and
    // the Checkout's `team` grant supersedes it.)
    if is_selfserve_creation(pool, clerk_org_id).await? {
        return Ok(id);
    }
    // Grant `free` AND enable enforcement (enforcement-completion PR, plan
    // §1.9/§2.A — "enforcement goes universal before money arrives"). This
    // org has no `account_members` row yet (membership arrives later via
    // `ensure_org_membership`), so the grant's first allowance period seeds
    // for zero members exactly as before — what changed is that
    // `ensure_org_membership` now seeds each JOINING member's own allowance
    // period the moment they join (`billing::renewal::seed_member_period`),
    // so flipping enforcement here is safe: see `provision_billing`'s own
    // doc comment for the full invariant this rests on.
    provision_billing(pool, id).await;
    // Shadow-provisioning fence: under database-per-org, an org's ocean and DEK
    // live in its TENANT database (provisioned by onboard_tenant.sh via
    // `routes::admin::rotate_dek?tenant=<org>`, which upserts the tenant's
    // `account_keys` row — org-tenancy PR 1.3), NOT on the shared
    // control/consumer plane. Creating them here on first org login would
    // race onboarding and leave a duplicate graph + a competing DEK on the shared
    // DB. So when the gate is enabled we create the control `accounts` row only.
    //
    // The ocean's OWN eventual home (org-tenancy OPS-4 follow-up):
    // `tenancy::registry::TenantRegistry::build` — called from every place
    // that ever resolves this org's tenant pool (the request path, `/mcp`,
    // the tenant-harness worker, and onboarding's own step-7 rotate-dek call)
    // — idempotently guarantees the org's tenant database has its commons
    // ocean, regardless of whether the tenant DB existed yet at THIS exact
    // moment. See that function's doc comment for the full story: this fence
    // correctly stops the ocean from being created on the WRONG (shared)
    // plane here, but for a long time nothing ever created it on the RIGHT
    // (tenant) plane either — `TenantRegistry::ensure_org_ocean` is that
    // missing piece, not a change to this fence.
    let multitenant = state
        .tenant_runtime
        .as_ref()
        .map(|r| r.enabled)
        .unwrap_or(false);
    if !multitenant {
        // RLS-scoped (org-tenancy OPS-4 follow-up, F2), same as the personal
        // path: this branch is the LIVE one in today's prod config (the flag
        // is off), and `graphs` is RLS-covered. See `create_first_ocean_scoped`.
        create_first_ocean_scoped(state, pool, id, owner_user_id, name.unwrap_or("Organization"), "org").await?;
        provision_account_dek(pool, id).await?;
    }
    Ok(id)
}

/// Whether `POST /v1/orgs` (self-serve signup PR A2) created this org: its
/// intent row in `org_selfserve_creations` (control 0384) names it.
pub(crate) async fn is_selfserve_creation(pool: &PgPool, clerk_org_id: &str) -> Result<bool, ProvisionError> {
    Ok(sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM org_selfserve_creations WHERE clerk_org_id = $1)",
    )
    .bind(clerk_org_id)
    .fetch_one(pool)
    .await?)
}

/// The `accounts` row for an org `POST /v1/orgs` (self-serve signup PR A2)
/// has just created in Clerk, written inside the caller's transaction, the
/// one that records the Clerk org id on the intent row. Differs from
/// [`ensure_org_account`] in three ways:
///
/// * no `free` grant and no billing enforcement change: the org's plan is
///   the trial's `team` grant, issued when its Checkout completes (founder
///   decision 2026-10-27: no credits before the card);
/// * no shared-plane ocean or DEK: the org's ocean lives in its tenant
///   database, built by the provisioning job after payment;
/// * the `ORG_SELFSERVE_DISABLED` backstop is not consulted. That flag stops
///   a STRAY org claim (a webhook or first-sight JWT for an org nobody is
///   onboarding) from minting a row; this org was minted a moment ago by
///   this backend under `ORG_SELFSERVE_SIGNUP_ENABLED`, against a persisted
///   intent row.
///
/// The revocation fence is kept: an org already deleted in Clerk never gets
/// a row. Idempotent: an existing row (a retry, or a webhook that won the
/// race) is returned as is.
pub(crate) async fn ensure_selfserve_org_account(
    conn: &mut sqlx::PgConnection,
    clerk_org_id: &str,
    owner_user_id: Uuid,
) -> Result<Uuid, ProvisionError> {
    if let Some(id) = sqlx::query_scalar::<_, Uuid>("SELECT id FROM accounts WHERE clerk_org_id = $1")
        .bind(clerk_org_id)
        .fetch_optional(&mut *conn)
        .await?
    {
        return Ok(id);
    }
    let inserted = sqlx::query_scalar::<_, Uuid>(
        "INSERT INTO accounts (owner_user_id, clerk_org_id) \
         SELECT $1, $2 \
         WHERE NOT EXISTS (SELECT 1 FROM org_revocations WHERE clerk_org_id = $2) \
         ON CONFLICT (clerk_org_id) WHERE clerk_org_id IS NOT NULL DO NOTHING \
         RETURNING id",
    )
    .bind(owner_user_id)
    .bind(clerk_org_id)
    .fetch_optional(&mut *conn)
    .await?;
    match inserted {
        Some(id) => Ok(id),
        None => sqlx::query_scalar::<_, Uuid>("SELECT id FROM accounts WHERE clerk_org_id = $1")
            .bind(clerk_org_id)
            .fetch_optional(&mut *conn)
            .await?
            .ok_or(ProvisionError::OrgRevoked),
    }
}

/// Resolves the active `standard` `seat_tiers.id` (`code = 'standard' AND
/// is_active = true`), for the two org-member writers ([`ensure_org_membership`],
/// [`refresh_org_membership_from_claims`]) that must now pass a `seat_tier_id`
/// explicitly (billing self-serve PR F, sunsetting 0169's silent
/// default-to-standard trigger arm — see migration `0176`'s own header).
///
/// `fetch_one`, not `fetch_optional`: a missing active `standard` row is a
/// catalog-provisioning bug, and this must fail LOUDLY (an org-member insert
/// that cannot resolve a tier must not silently proceed with a NULL tier —
/// `account_members_seat_tier_iff_org` would reject that anyway, with a far
/// less specific error). `sqlx::Error::RowNotFound` propagates through
/// [`ProvisionError::Db`] exactly like any other DB failure on these paths —
/// the webhook caller 5xxs and Clerk redelivers; the request-path caller
/// surfaces an error and the next request retries.
// Billing self-serve PR F review round 2, trivial finding: takes any executor
// (any `PgExecutor` is accepted) so
// `ensure_org_membership`'s call site can pass its already-open transaction
// instead of checking out a SECOND pooled connection for the duration of the
// lookup — under webhook bursts against a small pool, every in-flight
// `ensure_org_membership` would otherwise hold one connection and wait for
// another, which can stall until the acquire timeout, and the read would run
// outside the transaction's own snapshot. `refresh_org_membership_from_claims`
// deliberately stays in autocommit (see that function's own cost note), so
// its call site keeps passing `pool` — this is purely additive for it.
pub(crate) async fn resolve_standard_seat_tier_id<'e, E>(executor: E) -> Result<Uuid, ProvisionError>
where
    E: sqlx::PgExecutor<'e>,
{
    let id: Uuid = sqlx::query_scalar(
        "SELECT id FROM seat_tiers WHERE code = 'standard' AND is_active = true",
    )
    .fetch_one(executor)
    .await?;
    Ok(id)
}

/// Upsert a user's membership in an org account (by Clerk org id), setting
/// role — the AUTHORITATIVE writer, callable ONLY from Clerk webhook handlers
/// (`organizationMembership.created`/`.updated`, `routes::clerk_webhook`).
///
/// Clears any offboarding tombstone (migration 0081) in the same transaction
/// as the upsert, so a genuine re-invite through Clerk restores access. This
/// is deliberately the ONLY code path that can clear a tombstone or resurrect
/// a removed membership: request-path token claims go through
/// [`refresh_org_membership_from_claims`] instead, which a tombstone blocks —
/// otherwise a still-valid long-TTL OAuth access token replayed against any
/// `require_auth` route would re-create the very row the offboarding webhook
/// deleted (org-tenancy PR 2.2 P0).
///
/// # Seat acquisition (billing self-serve PR F, seat lifecycle)
///
/// Once the membership transaction below commits, this calls
/// [`crate::billing::seat_lifecycle::acquire_seat`] — "join (webhook) =
/// purchase" (`.scratch/billing-self-serve/pr-f-notes.md`'s reframe of plan
/// §2.F against the Clerk reality: there is no in-app invite action to fail
/// visibly, so the Stripe push happens here, and a failure propagates as an
/// `Err` out of this function, which the caller (the Clerk webhook handler)
/// turns into a 5xx — Clerk's own redelivery loop is what "add fails
/// visibly" means for a webhook-driven join). Deliberately OUTSIDE the
/// membership transaction (same reasoning as the `seed_member_period` call
/// below: `acquire_seat` makes its own outbound Stripe call, which must never
/// run inside an open DB transaction) and deliberately AFTER it (so a Stripe
/// failure never leaves a committed membership row un-billed for — it leaves
/// one un-billed-for AND retried, via Clerk redelivery, until it succeeds).
///
/// # CI enforcement (D3, org-onboarding plan §3 item 31, CS-434)
///
/// `scripts/check_ensure_org_membership_callers.sh` fails CI if any request
/// path outside this file (its own `#[cfg(test)]` calls) and the two
/// trusted callers — `routes::clerk_webhook` and `routes::org_invites`
/// (which creates the real Clerk membership itself, in the same request,
/// before calling this) — calls `ensure_org_membership`. A caller anywhere
/// else writes a local membership row with no matching Clerk membership: a
/// phantom member, billed for a seat via the `acquire_seat` call above, who
/// can never enter the org because org context is derived from the Clerk
/// JWT `org_id` claim, not from this table.
///
/// # Seat tier (CS-442, item 40, D8)
///
/// `seat_tier_id: None` resolves the active `standard` catalog row exactly
/// as before this PR (via [`resolve_standard_seat_tier_id`]) — every
/// existing caller (`routes::clerk_webhook::handle_membership_upsert`,
/// `routes::org_invites::create_invite`) passes `None` and is unaffected.
/// `Some(id)` is used **only on the fresh-INSERT path below** — the admin's
/// chosen tier for a join-request approval
/// (`routes::org_join_requests::approve`, via
/// `routes::org_invites::mirror_local_membership`). It is NEVER applied to
/// an EXISTING member: the `Some((_, true))` arm (a membership row already
/// exists) is a pure role UPDATE that never touches `seat_tier_id`, and the
/// `ON CONFLICT DO UPDATE` arm on the INSERT below sets `role =
/// EXCLUDED.role` only, exactly as before — moving an already-member's tier
/// is [`crate::routes::billing_seats::change_seat_tier`]'s job, a real
/// Stripe MOVE with proration semantics, never this function's silent
/// side effect. Passing `Some(id)` for a `code` this caller never validated
/// is the caller's own bug: this function trusts the id, the same way it
/// already trusted `role`.
///
/// # Removal fence (settings PR 2.9)
///
/// The webhook asks Clerk "is this person a member?" and only then calls this
/// function. An admin removal can land in between: Clerk says "member", the
/// admin removes them (tombstone written), and without a fence this function
/// would clear that tombstone and re-insert the row. If the `.deleted` event
/// was already processed, nothing would ever remove them again.
/// `clerk_observed_at` is the DB time captured before the Clerk read; a
/// tombstone at or after it means the Clerk answer is older than the removal,
/// so this returns [`ProvisionError::RemovedSinceObserved`] without touching
/// anything. That is a retryable error: Svix redelivers, the webhook asks
/// Clerk again, and the fresh answer decides (gone: skip; present again, a
/// genuine re-invite: apply). A tombstone older than the Clerk read is an
/// ordinary re-invite and is cleared as before.
#[allow(clippy::too_many_arguments)]
pub async fn ensure_org_membership(
    state: &AppState,
    pool: &PgPool,
    clerk_org_id: &str,
    user_id: Uuid,
    role: &str,
    is_created_event: bool,
    seat_tier_id: Option<Uuid>,
    clerk_observed_at: Option<chrono::DateTime<chrono::Utc>>,
) -> Result<(), ProvisionError> {
    ensure_org_membership_with_seat_policy(
        state,
        pool,
        clerk_org_id,
        user_id,
        role,
        is_created_event,
        seat_tier_id,
        clerk_observed_at,
        crate::billing::seat_lifecycle::SeatPurchasePolicy::AllowPurchase,
    )
    .await
}

/// [`ensure_org_membership`] with an explicit seat purchase policy for the
/// trailing [`crate::billing::seat_lifecycle::acquire_seat_with_policy`]
/// call. `PoolOnly` (`POST /v1/orgs/invites` with `allow_purchase: false`)
/// surfaces an empty pool as `ProvisionError::SeatLifecycle(NoFreeSeat)`.
/// The membership row is already committed by then, exactly as for a Stripe
/// failure; the caller unwinds the Clerk membership the same way.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn ensure_org_membership_with_seat_policy(
    state: &AppState,
    pool: &PgPool,
    clerk_org_id: &str,
    user_id: Uuid,
    role: &str,
    // Billing self-serve PR F review, MAJOR finding: whether the Clerk event
    // driving THIS call is `organizationMembership.created` (`true`) or
    // `.updated` (`false`) — see the acquire-seat gate below, "Retry gate",
    // for why this is needed in addition to `newly_inserted`.
    is_created_event: bool,
    // CS-442 (item 40, D8): the tier a FRESH insert is assigned —
    // `None` ⇒ resolve `standard` (unchanged default); `Some(id)` ⇒ that
    // `seat_tiers.id`, trusted from the caller (see this function's own
    // "Seat tier" doc section above). Never applied to an existing member.
    seat_tier_id: Option<Uuid>,
    // Removal fence (settings PR 2.9): the control DB's `now()` captured by
    // the caller BEFORE it read the membership from Clerk. `Some(t)` refuses
    // the upsert when a tombstone for (org, user) was written at or after
    // `t`; `None` (`routes::org_invites`, which created the Clerk membership
    // itself in this request) skips the fence. See "Removal fence" above.
    clerk_observed_at: Option<chrono::DateTime<chrono::Utc>>,
    seat_policy: crate::billing::seat_lifecycle::SeatPurchasePolicy,
) -> Result<(), ProvisionError> {
    let mut tx = pool.begin().await?;
    if let Some(observed_at) = clerk_observed_at {
        // Same per-org advisory lock `routes::org_members` holds across its
        // Clerk delete AND the local offboarding commit, so an admin removal
        // that is mid-flight is either fully visible below or has not started
        // its Clerk delete yet. (The `.deleted` webhook does not take this
        // lock; migration 0082's deferred trigger is the backstop there.)
        sqlx::query(
            "SELECT pg_advisory_xact_lock(hashtextextended('org_members:' || a.id::text, 0)) \
               FROM accounts a WHERE a.clerk_org_id = $1",
        )
        .bind(clerk_org_id)
        .execute(&mut *tx)
        .await?;
        let removed_at: Option<chrono::DateTime<chrono::Utc>> = sqlx::query_scalar(
            "SELECT removed_at FROM org_membership_tombstones \
              WHERE clerk_org_id = $1 AND user_id = $2 AND removed_at >= $3",
        )
        .bind(clerk_org_id)
        .bind(user_id)
        .bind(observed_at)
        .fetch_optional(&mut *tx)
        .await?;
        if let Some(removed_at) = removed_at {
            tx.rollback().await?;
            tracing::warn!(
                clerk_org_id, %user_id, %observed_at, %removed_at,
                "ensure_org_membership: member was removed after Clerk reported them present; \
                 refusing to clear the tombstone and re-insert (the webhook retries and re-asks Clerk)"
            );
            return Err(ProvisionError::RemovedSinceObserved);
        }
    }
    sqlx::query("DELETE FROM org_membership_tombstones WHERE clerk_org_id = $1 AND user_id = $2")
        .bind(clerk_org_id)
        .bind(user_id)
        .execute(&mut *tx)
        .await?;

    // Billing self-serve PR F review, trivial finding: resolve the active
    // `standard` tier LAZILY — only when an INSERT will actually attempt
    // one, not unconditionally on every delivery including a role-only
    // `.updated` redelivery for an already-existing member. An earlier
    // version resolved it unconditionally, before this presence check
    // existed: harmless in the common case, but [`resolve_standard_seat_tier_id`]'s
    // own `fetch_one` fails LOUDLY (`RowNotFound`) if the `standard` catalog
    // row is ever deactivated — turning a PURE ROLE CHANGE for a member
    // already on some OTHER tier into a 5xx retry loop that has nothing to
    // do with `standard` at all. Mirrors `refresh_org_membership_from_claims`'s
    // own presence-check shape (see that function's own doc comment for the
    // full cost rationale, including why a stale/raced read here is
    // harmless); the difference here is this webhook's authoritative
    // "always sync role on conflict" contract, which still needs a real
    // write on the existing-member path, just not a `seat_tiers` lookup for
    // it — so that path is a plain role UPDATE, not the INSERT ... ON
    // CONFLICT this function used to run unconditionally.
    let existing_member: Option<(Uuid, bool)> = sqlx::query_as(
        "SELECT a.id, EXISTS (SELECT 1 FROM account_members am \
                                WHERE am.account_id = a.id AND am.user_id = $2) \
           FROM accounts a WHERE a.clerk_org_id = $1",
    )
    .bind(clerk_org_id)
    .bind(user_id)
    .fetch_optional(&mut *tx)
    .await?;

    // `NOT EXISTS org_revocations`: an org-LEVEL revocation
    // (`organization.deleted`, migration 0082) is never cleared — Clerk never
    // reuses an org id — so even the authoritative webhook upsert refuses to
    // add members to a revoked org (out-of-order/replayed member events after
    // the deletion event would otherwise 500 against the 0082 DB trigger; the
    // guard makes them a clean no-op instead).
    // `RETURNING account_id, inserted, seat_purchased_at` (rather than plain
    // `.execute()`) so the best-effort billing seed below
    // (`billing::renewal::seed_member_period`) knows which account to scope
    // to without a second round trip — and, just as importantly, `account_id`
    // is `None` in EXACTLY the cases this statement wrote no row at all: no
    // `accounts` row for this org, or the `NOT EXISTS org_revocations` guard
    // fired. Either way there is no membership to seed an allowance for.
    //
    // **`inserted` (billing self-serve PR F review, finding 1) — whether a
    // genuinely NEW membership row was written by THIS call.** For the
    // INSERT branch below, `xmax` is Postgres's own per-row system column
    // recording the transaction id that would delete/update this row; a row
    // that branch's INSERT just created has never been touched by an
    // UPDATE, so `xmax` reads `0` for it — a row the `ON CONFLICT DO UPDATE`
    // arm instead touched carries a real (non-zero) `xmax`. For the plain
    // role-UPDATE branch (existing member, no tier resolve needed),
    // `inserted` is unconditionally `false` — that branch never creates a
    // row. This is what gates [`crate::billing::seat_lifecycle::acquire_seat`]
    // below on "a genuinely NEW membership row", not "this statement matched
    // a row" — without it, a role-only `organizationMembership.updated`
    // redelivery for an ALREADY-EXISTING, never-purchased member would
    // silently purchase their seat, violating the documented "members who
    // existed before the org subscribed are never auto-purchased" invariant
    // (`billing::seat_lifecycle`'s own module doc, FOUNDER-CONFIRM).
    let membership: Option<(Uuid, bool, Option<chrono::DateTime<chrono::Utc>>)> = match existing_member {
        Some((_, true)) => {
            // A membership row already exists — pure role sync, no
            // `seat_tiers` lookup needed at all.
            sqlx::query_as(
                "UPDATE account_members SET role = $3 \
                   FROM accounts a \
                  WHERE a.clerk_org_id = $1 AND account_members.account_id = a.id \
                    AND account_members.user_id = $2 \
                    AND NOT EXISTS (SELECT 1 FROM org_revocations r WHERE r.clerk_org_id = $1) \
                RETURNING account_members.account_id, false, account_members.seat_purchased_at",
            )
            .bind(clerk_org_id)
            .bind(user_id)
            .bind(role)
            .fetch_optional(&mut *tx)
            .await?
        }
        Some((_, false)) | None => {
            // Genuinely new, or the presence check above raced with a
            // concurrent webhook and is stale — the `ON CONFLICT DO UPDATE`
            // arm below is still correct either way (the same tolerance
            // `refresh_org_membership_from_claims`'s own doc comment
            // documents for its identical presence check). `$4`
            // (`resolved_tier_id`) is passed on this INSERT attempt only —
            // if it instead hits the `ON CONFLICT` arm (the stale-read
            // race), that arm sets `role = EXCLUDED.role` ONLY, never
            // `seat_tier_id`, so an existing member's tier (possibly
            // changed since, via the tier-change route) is never clobbered
            // back to the caller's chosen (or `standard`) tier.
            // ADR-0007 D2 / PR 829 review finding 1(b): a personal org
            // (`personal_orgs`, control migration 0208) is designed to carry
            // exactly one member (`personal_orgs.clerk_user_id UNIQUE`), but
            // this codebase does not control a personal org's Clerk-side
            // membership — nothing stops a second person from being added to
            // one, and `resolve_reachable_graphs` would then return every
            // `kind='personal'` ocean in that account to BOTH members.
            //
            // The added `AND NOT (... personal_orgs ... AND ... account_
            // members ...)` clause refuses this INSERT only when the target
            // account is BOTH a personal org's own account AND already has
            // at least one `account_members` row — i.e. it blocks a SECOND
            // member, never the first. It is deliberately NOT the simpler
            // "refuse whenever this is a personal org" shape: `personal_orgs`
            // is recorded (`bin/create_personal_orgs.rs` step 5) as a plain
            // local INSERT immediately after the Clerk org-creation API call
            // returns, while the owner's OWN first `organizationMembership.
            // created` webhook for that brand-new org is dispatched
            // asynchronously by Clerk (Svix) — `create_personal_orgs.rs`'s
            // own "ordering hazard" doc comment documents exactly this kind
            // of race elsewhere. In practice the local INSERT usually wins,
            // so a naive personal-org-only guard would routinely refuse the
            // OWNER'S OWN first membership row on the temporary org account
            // — the very row `bin/adopt_personal_org_account.rs` reads to
            // copy the owner's seat tier onto the personal account it
            // relabels (that tool SKIPS, never adopting, a user whose org
            // account carries no seat tier to copy). The two-condition form
            // instead keys on "already has a member", which is false for
            // that very first INSERT regardless of ordering, and true for
            // every INSERT after the account has its one legitimate member
            // — exactly the state a genuine unauthorized-second-member
            // attempt would find it in, before OR after adoption.
            //
            // CS-442 (item 40, D8): `seat_tier_id` (the caller's `Some(id)`)
            // wins over the resolved `standard` row when the caller passed
            // one — see this function's own "Seat tier" doc section. The
            // `resolve_standard_seat_tier_id` call stays LAZY either way
            // (only reached when an INSERT will actually be attempted,
            // finding from the trivial review this comment already
            // documents above), so a join-request approval's already-known
            // tier never pays for a `seat_tiers` round trip it does not
            // need.
            let resolved_tier_id = match seat_tier_id {
                Some(id) => id,
                None => resolve_standard_seat_tier_id(&mut *tx).await?,
            };
            sqlx::query_as(
                "INSERT INTO account_members (account_id, user_id, role, seat_tier_id) \
                 SELECT a.id, $2, $3, $4 FROM accounts a WHERE a.clerk_org_id = $1 \
                   AND NOT EXISTS (SELECT 1 FROM org_revocations r WHERE r.clerk_org_id = $1) \
                   AND NOT ( \
                       EXISTS (SELECT 1 FROM personal_orgs p WHERE p.clerk_org_id = a.clerk_org_id) \
                       AND EXISTS (SELECT 1 FROM account_members am WHERE am.account_id = a.id) \
                   ) \
                 ON CONFLICT (account_id, user_id) DO UPDATE SET role = EXCLUDED.role \
                 RETURNING account_id, (xmax = 0) AS inserted, seat_purchased_at",
            )
            .bind(clerk_org_id)
            .bind(user_id)
            .bind(role)
            .bind(resolved_tier_id)
            .fetch_optional(&mut *tx)
            .await?
        }
    };
    let account_id: Option<Uuid> = membership.map(|(account_id, ..)| account_id);
    let newly_inserted = membership.as_ref().is_some_and(|(_, inserted, _)| *inserted);
    let seat_purchased_at = membership.and_then(|(_, _, seat_purchased_at)| seat_purchased_at);
    // Member-ocean lifecycle (member-private-oceans PR 7, plan §2.6
    // "Restore"): this is the ONLY code path that resurrects a membership
    // (see this function's own doc comment), so it is also the restore
    // trigger's desired-state writer. `offboarded_at` is deliberately left
    // untouched — it is the historical "when did they leave" marker and
    // stays for ops visibility even after a rejoin; only the tenant-side
    // `graphs.archived_at` predicate the reconciler reads is the actual
    // purge/restore clock (plan §2.6: "the purge decision reads archived_at
    // only, never Clerk state").
    //
    // Deliberately UNCONDITIONAL, not gated on `MEMBER_OCEANS_ENABLED`: with
    // the flag off this is dead data with zero observable effect (nothing
    // reads this table unless the reconciler's flag check passes), but it is
    // the ONLY record of "this member is currently present" for a member who
    // left and rejoined entirely while the flag was off. Unlike provisioning
    // (which can always re-derive its roster from `account_members`),
    // `account_members` alone cannot answer "was this member ever archived
    // and has since rejoined" — that state lives nowhere else. Gating this
    // write would silently lose that history the moment the flag flips on.
    //
    // Runs on every `organizationMembership.created|.updated` delivery, not
    // just a genuine post-leave rejoin — that is fine: setting
    // `state = 'present', restored_at = now()` on an ordinary ongoing
    // membership is a harmless no-op from the reconciler's point of view,
    // since its restore arm's actual gate is the tenant-side `archived_at`
    // predicate, not this row's `restored_at` timestamp (§2.6's boundary is
    // "a SQL predicate, not worker timing" — this table is bookkeeping for
    // operator visibility, not the mechanism).
    // SAME `NOT EXISTS org_revocations` FENCE as the `account_members` insert
    // above, and it is load-bearing here for a different reason than there.
    //
    // Without it: `organization.deleted` runs `revoke_org_access`, setting every
    // member to `state='archived'`. Clerk then re-delivers, or delivers
    // out-of-order, an `organizationMembership.created|.updated` for that same
    // org. The membership insert above is fenced so `account_members` correctly
    // stays empty — but this upsert would run unconditionally and flip the row
    // back to `state='present'`.
    //
    // `archive_arm` selects work ONLY from `state='archived'`, so that member's
    // oceans would never be stamped `archived_at` and `revoke_shares_for_graphs`
    // would never run against them. Because `org_revocations` rows are never
    // cleared (Clerk does not reuse org ids), nothing downstream ever repairs
    // it: the archive simply never happens, and that member's live
    // `coding_session_shares` / `meeting_shares` tokens OUTLIVE THE REVOCATION
    // indefinitely. A replayed webhook must not be able to cancel a teardown.
    sqlx::query(
        "INSERT INTO member_ocean_desired_state \
             (clerk_org_id, user_id, state, restored_at, updated_at) \
         SELECT $1, $2, 'present', now(), now() \
         WHERE NOT EXISTS (SELECT 1 FROM org_revocations r WHERE r.clerk_org_id = $1) \
         ON CONFLICT (clerk_org_id, user_id) DO UPDATE \
           SET state = 'present', \
               restored_at = now(), \
               updated_at = now()",
    )
    .bind(clerk_org_id)
    .bind(user_id)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;

    // Member-join allowance seeding (enforcement-completion PR, plan
    // §1.9/§2.A) — best-effort, deliberately OUTSIDE the transaction just
    // committed above. `seed_member_period` opens its OWN account-scoped
    // transaction (`tenancy::scope::begin_account_scoped`, the same
    // primitive `billing::renewal`/`billing::issuance` use throughout,
    // required because `member_allowance_periods` and `grants` are both
    // RLS-covered) — nesting that inside the `pool.begin()` transaction
    // above would require a savepoint for no benefit, since this write must
    // never be able to roll back the membership it depends on anyway.
    // Matches `provision_billing`'s posture: a member is provisioned whether or not their
    // allowance seeded successfully — a user without a working membership is
    // a worse outcome than one whose free allowance is delayed a beat.
    //
    // A missed seed here is NOT stranded (finding 6, review round 2 — this
    // comment previously overclaimed "a later backfill/retry pass can always
    // catch a missed seed" before every join path actually seeded, which was
    // not yet true). Two concrete recovery paths exist as of this PR: (1) ANY
    // subsequent membership upsert for this same `(clerk_org_id, user_id)`
    // pair — a re-invite, a role change, or simply this same webhook
    // redelivered — re-runs this exact call, and `seed_member_period` is
    // idempotent, so the retry needs no special handling; and (2)
    // `bin/backfill_billing_enforcement` is re-runnable at any time as an
    // operator-driven repair pass, not just a one-time migration aid — see
    // its own module doc for why it stays idempotent and safe to re-run
    // indefinitely. Absent either of those two things actually happening, a
    // missed seed has no OTHER repair path and stays missed.
    //
    // `account_id` is `None` exactly when the membership INSERT above wrote
    // no row (see that query's own comment) — a revoked org or a
    // still-unprovisioned `accounts` row — in which case there is no
    // membership to seed an allowance for at all.
    if let Some(account_id) = account_id {
        if let Err(e) = crate::billing::renewal::seed_member_period(pool, account_id, user_id).await
        {
            tracing::warn!(
                error = %e, account_id = %account_id, user_id = %user_id,
                clerk_org_id = %clerk_org_id,
                "billing: failed to seed this member's allowance period on join — membership \
                 itself still succeeded; a later backfill pass can repair this"
            );
        }

        // Billing self-serve PR F: acquire this member's seat, AFTER the
        // membership row above is committed and AFTER the best-effort
        // allowance seed just above (ordering between the two does not
        // matter for correctness — they touch disjoint state — but seeding
        // the allowance first means a brand-new member is never left able to
        // spend before they have anything to spend against, however briefly,
        // if this call is slow). Unlike the seed call, this is NOT
        // best-effort — see this function's own doc comment, "Seat
        // acquisition", for why an error here must propagate.
        //
        // **Retry gate (billing self-serve PR F review, MAJOR finding,
        // supersedes finding 1's `newly_inserted`-only version).**
        // `newly_inserted` alone left Clerk's own documented retry
        // mechanism unable to actually retry: the membership transaction
        // commits (just above) BEFORE `acquire_seat` is ever called, so if
        // THIS call's `acquire_seat` fails and this function propagates the
        // error (5xx, Clerk redelivers), the row already exists by the time
        // the redelivery arrives — `newly_inserted` reads `false` on retry,
        // and the gate below used to skip `acquire_seat` forever, leaving
        // the member permanently unpurchased with no retry path at all
        // (this module's own doc comment claims "Clerk's own redelivery
        // loop IS the visible failure surface" — that claim was false for
        // exactly this case).
        //
        // Fixed by also retrying whenever `is_created_event` is `true` AND
        // the seat is still unpurchased: Clerk redelivers an event as
        // ITSELF — a `.created` redelivery arrives as `.created` again, not
        // `.updated` — so `is_created_event` stays `true` across every
        // retry of the SAME join, while a genuine role change for a
        // DIFFERENT, pre-existing unpurchased member always arrives as
        // `.updated` (`is_created_event = false`) and is still never
        // gated in — preserving finding 1's own invariant ("members who
        // existed before the org subscribed are never auto-purchased",
        // `billing::seat_lifecycle`'s own module doc, FOUNDER-CONFIRM).
        // `acquire_seat`'s own `seat_purchased_at` guard, re-checked under
        // its advisory lock, is what makes calling it again on an
        // already-purchased member's redelivery a clean no-op — this gate
        // only needs to decide whether calling it AT ALL is safe.
        if newly_inserted || (is_created_event && seat_purchased_at.is_none()) {
            crate::billing::seat_lifecycle::acquire_seat_with_policy(
                pool,
                state.stripe.as_deref(),
                account_id,
                user_id,
                seat_policy,
            )
            .await?;
            // Self-serve signup A2 (S4): a seat bought for a new member moves
            // the purchased count; Clerk's member cap follows it (a no-op for
            // an org `POST /v1/orgs` did not create).
            crate::routes::orgs_create::spawn_clerk_membership_cap_sync(pool.clone(), account_id);
        }
    }

    Ok(())
}

/// Request-path org-membership provisioning from VERIFIED token claims
/// (`middleware::auth::resolve_clerk_context`, `ProvisionFromClaims` policy).
/// Two hard restrictions distinguish it from the webhook-only
/// [`ensure_org_membership`] (org-tenancy PR 2.2 P0 fix):
///
/// 1. **Tombstone- AND org-revocation-guarded INSERT**: it can create a
///    membership only for a (org, user) pair the offboarding webhook never
///    removed AND an org that was never revoked wholesale
///    (`organization.deleted`, migration 0082 — the org-level guard is what
///    stops a token holder who was in the Clerk org but NEVER signed in here,
///    and so has no per-member tombstone, from joining a deleted org).
///    First-sight provisioning (a brand-new member signs in before the
///    `organizationMembership.created` webhook lands, or local dev with no
///    webhook receiver) still works, but a claim on a still-valid token can
///    never resurrect a deleted membership. Clerk webhooks are the sole
///    writer able to re-create one (they clear the tombstone first), and the
///    0082 deferred constraint trigger enforces both guards at the DB level
///    even against writers that skip this function.
/// 2. **Insert-only (`DO NOTHING`)**: an existing row's role is never
///    touched. Role changes are the webhook's job too
///    (`organizationMembership.updated`) — otherwise a stale token minted
///    before a demotion could replay its old `org_role` claim and re-escalate
///    the row for the token's whole TTL, the same resurrection class as the
///    membership itself.
///
/// Race note: the `NOT EXISTS` guards here run under READ COMMITTED, so an
/// insert racing a concurrent offboarding transaction could in principle
/// evaluate against a pre-offboarding snapshot. The 0082 DEFERRED constraint
/// trigger closes that window — it re-checks tombstones/revocations at COMMIT
/// with a fresh snapshot and aborts this statement's transaction if the
/// offboarding won the race.
///
/// Dev note: in a webhook-less environment (local dev without a Clerk webhook
/// receiver) a tombstone can never be cleared through the product, so a
/// locally-offboarded member stays locked out of that org until the tombstone
/// row is deleted by hand. Accepted: prod Clerk webhooks are authoritative,
/// and local org flows are already webhook-dependent for provisioning parity.
pub async fn refresh_org_membership_from_claims(
    pool: &PgPool,
    clerk_org_id: &str,
    user_id: Uuid,
    role: &str,
) -> Result<(), ProvisionError> {
    // **Billing self-serve PR F review, minor finding: resolve the active
    // `standard` tier LAZILY — only when an insert will actually be
    // attempted.** This function runs on the `require_auth` request path for
    // EVERY org-context request, and the overwhelmingly common case is an
    // already-provisioned member's Nth request, where the INSERT below is
    // going to be a pure `DO NOTHING` no-op regardless of what `$4` is bound
    // to. An earlier version resolved `standard_tier_id` (a full extra round
    // trip) unconditionally, before knowing that. This cheap presence check —
    // ONE query, reading only `accounts`/`account_members` (neither RLS-
    // covered on the control plane, same posture this file's other unscoped
    // reads document) — answers "does a membership row already exist for
    // this (org, user) pair" without touching `seat_tiers` at all; the tier
    // lookup only runs on the genuinely-new-member branch, where a real
    // INSERT (and therefore a real round trip) was always going to happen
    // anyway. This does NOT weaken the explicit-tier guarantee 0176's own
    // header describes (no silent NULL/trigger-default `seat_tier_id`) — it
    // only changes WHEN the tier is resolved, never lets the INSERT below
    // proceed without one.
    //
    // The presence check is a fast-path OPTIMIZATION, not an authority: the
    // INSERT below re-derives everything from scratch (the tombstone/
    // revocation guards, `ON CONFLICT DO NOTHING`) and is correct on its own
    // even if this check's result went stale between here and there (a
    // concurrent webhook could have inserted the row in the meantime) — it
    // would just mean the resolved `standard_tier_id` below goes unused,
    // exactly as harmless as the pre-existing "resolve then INSERT" ordering
    // already was for that same race.
    let existing: Option<(Uuid, bool)> = sqlx::query_as(
        "SELECT a.id, EXISTS (SELECT 1 FROM account_members am \
                                WHERE am.account_id = a.id AND am.user_id = $2) \
           FROM accounts a WHERE a.clerk_org_id = $1",
    )
    .bind(clerk_org_id)
    .bind(user_id)
    .fetch_optional(pool)
    .await?;

    // `RETURNING account_id` (rather than plain `.execute()`) so the
    // best-effort billing seed below knows which account to scope to. Unlike
    // `ensure_org_membership`'s `ON CONFLICT ... DO UPDATE ... RETURNING`
    // (which always returns a row, even on an existing membership, because an
    // UPDATE always affects one), this INSERT is `DO NOTHING` — see this
    // function's own doc comment for why it must stay insert-only — so
    // `RETURNING` is silent on the conflict path and `account_id` is `Some`
    // ONLY when this statement just created a BRAND-NEW membership row. That
    // is exactly the gate the seed call below wants: see its own comment.
    let account_id: Option<Uuid> = match existing {
        Some((_, true)) | None => {
            // Already a member (common case), or no `accounts` row for this
            // org at all — either way, no insert to attempt, so no tier to
            // resolve for one.
            None
        }
        Some((_account_id, false)) => {
            // Billing self-serve PR F: `$4` (`standard_tier_id`) resolved
            // BEFORE this INSERT, same as `ensure_org_membership` — see that
            // function's own comment for why `standard` and why this cannot
            // be left to a trigger default any more. Still `DO NOTHING`
            // (this function's own "insert-only" contract above), and still
            // NO Stripe call on this hot auth path — a brand-new member's
            // seat purchase completes idempotently when the
            // `organizationMembership.created` webhook
            // (`ensure_org_membership`) lands, whether before or after this
            // request.
            let standard_tier_id = resolve_standard_seat_tier_id(pool).await?;
            sqlx::query_scalar(
                "INSERT INTO account_members (account_id, user_id, role, seat_tier_id) \
                 SELECT a.id, $2, $3, $4 FROM accounts a \
                 WHERE a.clerk_org_id = $1 \
                   AND NOT EXISTS (SELECT 1 FROM org_membership_tombstones t \
                                    WHERE t.clerk_org_id = $1 AND t.user_id = $2) \
                   AND NOT EXISTS (SELECT 1 FROM org_revocations r \
                                    WHERE r.clerk_org_id = $1) \
                 ON CONFLICT (account_id, user_id) DO NOTHING \
                 RETURNING account_id",
            )
            .bind(clerk_org_id)
            .bind(user_id)
            .bind(role)
            .bind(standard_tier_id)
            .fetch_optional(pool)
            .await?
        }
    };
    // Member-join allowance seeding (finding 2, review round 2) — best-effort,
    // same posture and same function (`billing::renewal::seed_member_period`)
    // as `ensure_org_membership`'s own call; see that call site's comment for
    // the full rationale. This is the FIRST-SIGHT provisioning path — the
    // ONLY one in a webhook-less environment (local dev with no Clerk webhook
    // receiver) — so without this, a member who joins entirely through a
    // verified session JWT would never get seeded at all until some later
    // membership upsert or an operator backfill pass happened to run.
    //
    // Deliberately gated on `account_id.is_some()` — "the INSERT above just
    // created a brand-new row" — rather than an unconditional call after
    // every request: this function runs on the hot `require_auth` path for
    // EVERY org-context request, and seeding is a full account-scoped
    // transaction, so paying that cost on the overwhelmingly common case (an
    // already-provisioned member's Nth request, where `account_id` is `None`
    // because the INSERT's own `ON CONFLICT DO NOTHING` matched) would be
    // pure waste — the member was already seeded, either by THIS function on
    // their first-ever request, or by the authoritative
    // `ensure_org_membership` webhook path, whichever ran first. Seeding
    // itself is idempotent either way, so this gate is purely a cost
    // decision, never a correctness one.
    if let Some(account_id) = account_id {
        if let Err(e) = crate::billing::renewal::seed_member_period(pool, account_id, user_id).await
        {
            tracing::warn!(
                error = %e, account_id = %account_id, user_id = %user_id,
                clerk_org_id = %clerk_org_id,
                "billing: failed to seed this member's allowance period on first-sight \
                 provisioning — membership itself still succeeded; a later membership upsert \
                 (webhook) or an operator backfill pass can repair this"
            );
        }
    }

    Ok(())
}

/// Remove a user's membership from an org account (by Clerk org + user ids) —
/// the `organizationMembership.deleted` webhook path. One transaction, two
/// effects (org-tenancy PR 2.2 P0 fix):
///
/// 1. Tombstone (migration 0081) FIRST, so the request-path claims refresher
///    can never re-create the row afterward, whatever the interleaving.
/// 2. Delete the `account_members` row (what this function always did).
///
/// Note: nothing here touches MCP write state. MCP write is no longer a
/// per-member grant (settings launch plan, decision 6); the gate is the server
/// write ceiling plus the token's `coralswarm:write` scope, so offboarding
/// needs no write-grant cleanup. Membership removal alone cuts the member off
/// (`live_org_membership` rejects the next request).
///
/// # Seat release — "leave = the slot goes back in the pool" (CS-444)
///
/// [`crate::billing::seat_lifecycle::release_seat`] is called BEFORE the
/// tombstone+delete transaction below — see that function's own doc comment
/// for why the ordering still matters (its guarded stamp clear needs a row
/// to affect, and that guard is what makes a Clerk redelivery safe).
///
/// **"Leave (webhook) = release" no longer means "leave = refund" (founder
/// decision, 2026-09-22 — supersedes billing self-serve PR F's
/// `.scratch/billing-self-serve/pr-f-notes.md` framing).** A release now
/// clears the departing member's `account_members.seat_purchased_at` and
/// nothing else: no Stripe quantity decrement, no `grant_seat_tiers` write.
/// The org keeps the slot it paid for — `purchased` holds steady, `used`
/// drops, `available` rises by one — and the next person to join consumes
/// it for free through `acquire_seat`'s consume-before-buy branch. The pool
/// changes only when an admin explicitly buys or reduces slots
/// (`routes::org_slots`).
///
/// **A release failure still does NOT block the delete (finding 6's posture,
/// kept).** Access revocation always wins: this function's job is removing a
/// departed member's ACCESS, and Clerk itself already considers them gone. A
/// `release_seat` error is logged LOUDLY and swallowed; the membership
/// delete transaction below runs UNCONDITIONALLY. What a failure COSTS is
/// now much smaller than it was: the only thing a release writes is that one
/// member's stamp, and the `DELETE FROM account_members` below removes the
/// whole row moments later — so `billing::slot_pool`'s `used` (a COUNT over
/// surviving member rows) drops either way, and the slot is freed whether or
/// not the release itself succeeded. The error can no longer strand a billed
/// seat, because no billing moves here at all. Note that this also means
/// `release_seat`'s own idempotency guard protects only the window BEFORE
/// this delete commits; afterwards there is no row left to double-free.
pub async fn remove_org_membership(
    state: &AppState,
    pool: &PgPool,
    clerk_org_id: &str,
    clerk_user_id: &str,
) -> Result<(), ProvisionError> {
    // CS-449 (item 38's drain): `state` IS read again as of this story,
    // reversing CS-444's own `let _ = state;` — the slot `release_seat`
    // below frees may be exactly what an org's `pool_full` backlog was
    // waiting on, so a genuine release triggers `routes::
    // org_join_requests::drain_org` before this function returns — but only
    // AFTER this function's own revocation has committed. See the drain
    // call itself, below (placed after the tombstone+delete transaction,
    // not before it — CS-449 review round 2's blocker 3 fix), for the full
    // reasoning; `state.stripe` is still unused (CS-444's own point: a
    // release makes no Stripe call).
    //
    // # Blocker fix, CS-449 review round 2 — the drain used to sit BEFORE
    // # revocation, not after
    //
    // This function's own next section states the principle: *"access
    // revocation must not wait on billing state."* The drain trigger used
    // to invert it. It was awaited right here, in the `release_seat` `Ok`
    // arm — BEFORE the tombstone INSERT and the `DELETE FROM
    // account_members` a few dozen lines down — so a removed member's
    // access stayed live for as long as the drain took, which can be N real
    // Clerk membership-creation round trips against a `pool_full` backlog.
    // A slow drain, or a process death mid-drain, meant Clerk had to
    // redeliver the webhook before this member's access was ever actually
    // cut.
    //
    // Fixed by capturing WHETHER to drain here (`should_drain`, from
    // [`crate::billing::seat_lifecycle::release_seat`]'s own return value —
    // item 4's fix: `true` only on a GENUINE free, never on a no-op) and
    // acting on it only after the tombstone+delete transaction has
    // COMMITTED, several dozen lines below. Revocation persists first;
    // the (possibly slow, possibly failing) drain runs after, exactly like
    // every other best-effort step this function already runs post-commit
    // (the Google Chat re-election, the connector-health note) — same
    // posture, same reasoning: a redelivered webhook re-runs idempotent
    // work (the tombstone upsert, the now-no-op delete), so revoking FIRST
    // costs nothing on a retry and closes the access-lag window for good.
    // AWAITED here, not spawned: this function already awaits several
    // best-effort steps after the same commit, or comparable per this
    // handler's overall reasoning applies; a spawn would need its own error
    // handling and a cloned pool/state for no latency benefit THIS caller
    // needs (a webhook handler, not a synchronous admin UI action — compare
    // `routes::org_slots::purchase_slots`'s own item-6 fix, which DOES
    // spawn, because an admin's browser is waiting on that one).
    let mut should_drain = false;
    let mut drain_account_id: Option<Uuid> = None;

    // No internal user row => nothing to remove and no principal to tombstone
    // (a user who has never touched this backend has no membership here).
    //
    // CS-430 (D12/item 33): also read `email` here so the tombstone below can
    // record the member's CURRENT verified address, not just their user_id —
    // see the tombstone INSERT's own comment for why.
    let Some((user_id, user_email)) =
        sqlx::query_as::<_, (Uuid, String)>("SELECT id, email FROM users WHERE clerk_user_id = $1")
            .bind(clerk_user_id)
            .fetch_optional(pool)
            .await?
    else {
        return Ok(());
    };

    // Resolve the account BEFORE releasing — `release_seat` needs the
    // account id, not the Clerk org id, and must run before this function's
    // own delete transaction touches `account_members` at all (see this
    // function's own doc comment, "Seat release"). No row here means no
    // `accounts` row for this org yet (unreachable in practice — a
    // membership to remove implies the org was already provisioned — but
    // handled the same "nothing to release" way as any other unresolvable
    // billing state in this crate).
    if let Some(account_id) =
        sqlx::query_scalar::<_, Uuid>("SELECT id FROM accounts WHERE clerk_org_id = $1")
            .bind(clerk_org_id)
            .fetch_optional(pool)
            .await?
    {
        // Finding 6: logged and swallowed, NEVER propagated — see this
        // function's own doc comment, "Seat release", for why access
        // revocation must not wait on billing state. CS-444: `release_seat`
        // takes no Stripe client any more (it makes no Stripe call), so the
        // only error reachable here is a DB one.
        match crate::billing::seat_lifecycle::release_seat(pool, account_id, user_id).await {
            Ok(freed) => {
                // CS-449 (item 38's drain, item 4's fix): `release_seat`
                // already cleared `seat_purchased_at` (its own guarded
                // UPDATE, inside its own committed transaction) BEFORE this
                // line runs, so `billing::slot_pool`'s `used` count — and
                // therefore `available()` — reflects the freed slot right
                // now, even though the `account_members` row itself is not
                // deleted until later in this function. `freed` (item 4) is
                // `true` only on a GENUINE free — never on the two no-op
                // cases (no membership row; never held a purchased slot in
                // the first place) or a redelivered `.deleted` webhook whose
                // FIRST delivery already cleared the stamp. Re-gating a
                // `pool_full` backlog for a removal that freed nothing would
                // be a wasted pass every time: two account-scoped
                // transactions plus two queries per candidate, for a pool
                // that did not change. The drain itself does not run HERE —
                // see the blocker-3 fix below, after this function's own
                // revocation has committed — this only records that it
                // should.
                should_drain = freed;
                drain_account_id = Some(account_id);
            }
            Err(e) => {
                tracing::error!(
                    error = %e, %account_id, %user_id, clerk_org_id,
                    "billing: seat release failed during offboarding — proceeding with the membership \
                     delete anyway (access revocation must not wait on billing state). No money moves \
                     in a release (CS-444): the org keeps the slot it purchased either way, and the \
                     DELETE below drops this member out of slot_pool's `used` count regardless, so \
                     the slot is freed even though this call failed."
                );
            }
        }
    }

    // CS-430 (D12/item 33): record the member's CURRENT verified address
    // alongside the user_id — normalized the same way EVERY OTHER
    // `users.email` comparison in this codebase is (`lower(trim(...))`,
    // exact match, no provider canonicalisation; see
    // `connectors::slack::normalize_email`'s own doc comment for why a
    // plus-alias/dot-stripping normaliser would be wrong here). Stored
    // pre-normalized so `is_org_membership_tombstoned` below need only
    // normalize its OWN input the same way, never the column.
    //
    // `users.email` is `NOT NULL` today, so this is populated in every real
    // removal this function handles — but the column itself stays nullable
    // (migration 0226) and this write goes through `Option<&str>` rather than
    // a bare bind, on the same "never fabricate" principle the lookup below
    // documents: a hypothetical future caller with no address in hand must
    // leave the column null, not invent a placeholder the invite-time lookup
    // would then trust as a real, verified match.
    let normalized_email: Option<String> = Some(normalize_user_email(&user_email));

    let mut tx = pool.begin().await?;
    sqlx::query(
        "INSERT INTO org_membership_tombstones (clerk_org_id, user_id, email) VALUES ($1, $2, $3) \
         ON CONFLICT (clerk_org_id, user_id) DO UPDATE SET removed_at = now(), email = EXCLUDED.email",
    )
    .bind(clerk_org_id)
    .bind(user_id)
    .bind(&normalized_email)
    .execute(&mut *tx)
    .await?;
    sqlx::query(
        "DELETE FROM account_members am USING accounts a \
         WHERE am.account_id = a.id AND a.clerk_org_id = $1 AND am.user_id = $2",
    )
    .bind(clerk_org_id)
    .bind(user_id)
    .execute(&mut *tx)
    .await?;
    // Member-ocean lifecycle (member-private-oceans PR 7, plan §2.6
    // "Archive"): intent-only write here — this function is control-plane
    // and may run while the departed member's tenant DB is unreachable,
    // which is exactly why intent (this row) and materialization (the
    // reconciler stamping tenant-side `graphs.archived_at`) are split. The
    // reconciler's archive arm reads every `state = 'archived'` row for this
    // org, since a departed member no longer has an `account_members` row to
    // re-derive that fact from (unlike provisioning's roster).
    //
    // The replay guard is load-bearing: a re-delivered or duplicated
    // `organizationMembership.deleted` webhook must not push `offboarded_at`
    // forward, or the 60-day retention clock would appear to restart on
    // every replay. This is deliberately NOT the same pattern as
    // `org_membership_tombstones`'s upsert just above (`SET removed_at =
    // now()` unconditionally) — plan §2.6 explicitly rejects reusing that
    // column as the clock for exactly this reason.
    //
    // A plain `COALESCE(offboarded_at, now())` is NOT enough, though
    // (CodeRabbit review): consider leave (offboarded_at=T0) -> rejoin
    // (`ensure_org_membership` sets restored_at=T1, deliberately leaves
    // offboarded_at=T0) -> leave again at T2. A bare `COALESCE` would keep T0
    // forever, so this row would report an offboarding date from BEFORE the
    // member's actual, current departure, with no trace of the rejoin ever
    // having happened. The `CASE` below gates on `restored_at IS NOT NULL`
    // (evaluated BEFORE it is cleared in this same `SET` list) — a non-NULL
    // `restored_at` means a rejoin genuinely happened since the last time
    // this row was archived, so THIS archive is a new event and
    // `offboarded_at` must advance to `now()`; `restored_at IS NULL` means no
    // rejoin intervened (a straightforward re-delivered/duplicated `.deleted`
    // webhook for the same departure), so the original `offboarded_at` is
    // preserved exactly as the plain `COALESCE` already did. `restored_at` is
    // cleared on every archive either way, so a stale restore timestamp from
    // a PRIOR leave/rejoin cycle can never be misread as "restored after
    // THIS archive" by an operator reading this table.
    sqlx::query(
        "INSERT INTO member_ocean_desired_state \
             (clerk_org_id, user_id, state, offboarded_at, updated_at) \
         VALUES ($1, $2, 'archived', now(), now()) \
         ON CONFLICT (clerk_org_id, user_id) DO UPDATE \
           SET state = 'archived', \
               offboarded_at = CASE \
                   WHEN member_ocean_desired_state.restored_at IS NOT NULL THEN now() \
                   ELSE COALESCE(member_ocean_desired_state.offboarded_at, now()) \
                 END, \
               restored_at = NULL, \
               updated_at = now()",
    )
    .bind(clerk_org_id)
    .bind(user_id)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;

    // CS-449 (item 38's drain) — blocker 3 fix, CS-449 review round 2.
    // Revocation is durable as of the `tx.commit()` immediately above: the
    // tombstone is written and `account_members` no longer carries this
    // member. ONLY NOW does the drain run, best-effort, exactly like the
    // Google Chat re-election and the connector-health note a few lines
    // below — access revocation must not wait on an add, and by the time
    // this call starts, it no longer does. See `should_drain`'s own
    // assignment above for why this may be a clean no-op (nothing was
    // genuinely freed) rather than a real drain pass.
    if let Some(account_id) = drain_account_id.filter(|_| should_drain) {
        drain_org_on_slot_freed(state, pool, account_id, clerk_org_id).await;
    }

    // ── Google Chat: re-elect any space this member was the READER of ─────
    // gsuite Train G4 PR 4, security review H1 (reproduced).
    //
    // This function deletes the `account_members` row and deliberately does
    // NOT touch `oauth_tokens` — a departed member's stored Google Chat
    // token therefore survives offboarding. `connectors::google_chat::
    // elect_space_reader` now gates on `account_members`, so it will no
    // longer PICK them; this call is the other half, evicting them from every
    // space that already names them. Without it a space keeps a departed
    // member as its designated reader until something else happens to
    // re-elect, and the sweep would go on refreshing and re-persisting their
    // token, reading with their credential, and re-creating a Workspace
    // Events subscription in their name.
    //
    // Runs AFTER the commit, best-effort, and never fails the offboarding:
    // access revocation must not wait on a connector (the same posture the
    // seat-release call above takes for Stripe). If it fails, the gate in
    // `elect_space_reader` still refuses to hand this member any NEW space,
    // and — the part that actually makes this best-effort safe rather than
    // merely convenient (review round 2) — every read of a STORED
    // `reader_user_id` is itself gated on
    // `google_chat::is_current_account_member`, so a stale name is inert:
    // the ex-member's credential is not loaded, not refreshed, and not
    // re-persisted, and the sweep elects a current member in their place.
    if let Some(account_id) = sqlx::query_scalar::<_, Uuid>("SELECT id FROM accounts WHERE clerk_org_id = $1")
        .bind(clerk_org_id)
        .fetch_optional(pool)
        .await?
    {
        // ── Capability-backing registry (CS-456 / Phase G S6) ─────────────
        // One call replaces the prior pair (Chat space-reader re-election +
        // D16 directory `note_member_departure`). `on_backer_lost` runs
        // re-election where the kind allows it and opens a health row only
        // when nobody can take over. `content_pool = pool` here: on the
        // control plane offboarding path the same pool holds both
        // `account_members` and (for personal) Chat watched spaces; org
        // tenants' Chat spaces live on the tenant pool, which this path
        // does not have — Chat then skips election rather than inventing a
        // row from incomplete evidence (see `on_backer_lost`'s own doc).
        //
        // Best-effort and never fails the offboarding — access revocation
        // must not wait on bookkeeping.
        // Doorbell Graph alignment while this member's Microsoft token row
        // still exists (departure does not delete `oauth_tokens`). A successor
        // PATCHes the existing subscription id; only a container with no
        // replacement is deleted upstream. Best-effort: offboarding must not
        // wait on Graph. `on_backer_lost` below still re-elects in the database
        // when this call cannot.
        if let Ok(_plane) = crate::crypto::DekPlane::resolve_for_account(state, pool, account_id).await
        {
            let graph = crate::connectors::teams_doorbell::HttpDoorbell { http: &state.http_client };
            let tokens = crate::connectors::teams_doorbell::ResolvedWatcherTokens {
                state,
                control: pool,
                http: &state.http_client,
            };
            use crate::connectors::teams_doorbell::WatcherTokens as _;
            let departing = tokens.access_token(account_id, user_id, true).await;
            if let Err(e) = crate::connectors::teams_doorbell::prepare_owner_departure(
                pool,
                &graph,
                &tokens,
                account_id,
                user_id,
                departing.as_deref(),
            )
            .await
            {
                tracing::warn!(
                    error = %e, %account_id, %user_id,
                    "teams doorbell: departure handoff failed — database re-election still runs"
                );
            }
        }
        match crate::services::connector_health::on_backer_lost(
            pool,
            Some(pool),
            account_id,
            Some(user_id),
            "", // every kind that applies to this departure
            crate::services::connector_health::BackerLostCause::Departure,
            Some(&user_email),
            None,
        )
        .await
        {
            Ok(report) => {
                if let Some(crate::services::connector_health::DepartureOutcome::Recorded {
                    newly_opened,
                }) = report.directory
                {
                    tracing::info!(
                        %account_id, %user_id, clerk_org_id, newly_opened,
                        "connector health: this org's Google Workspace directory (D16 rung 1) was \
                         backed by the departing member's grant and by nobody else — recorded for the \
                         admin connectors page; auto-add now decides on rung 2"
                    );
                }
                if report.chat_spaces_reelected > 0 || report.chat_spaces_orphaned > 0 {
                    tracing::info!(
                        %account_id, %user_id, clerk_org_id,
                        reelected = report.chat_spaces_reelected,
                        orphaned = report.chat_spaces_orphaned,
                        "google chat: re-elected the designated reader on spaces an offboarded \
                         member was reading"
                    );
                }
            }
            Err(e) => tracing::warn!(
                error = %e, %account_id, %user_id, clerk_org_id, connector_health = true,
                "connector health: on_backer_lost failed during offboarding — the offboarding \
                 itself is unaffected, but an admin may not be told that a shared capability \
                 lost its last eligible backer"
            ),
        }

        // ── Connector revocation (settings launch PR 2.8) ─────────────────
        // Last, because the two steps above still need the departing
        // member's Microsoft token row (the doorbell handoff) and their
        // watcher rows (re-election). Deletes THIS org account's per-member
        // `oauth_tokens`/`connector_installs`/Slack member channels, stops
        // their watches and subscriptions upstream best-effort, and purges
        // the watch state — see `connectors::member_offboarding`'s module
        // doc. Their personal account's connectors are a different
        // `account_id` and are never matched. Idempotent, so a redelivered
        // webhook re-runs it harmlessly; never fails the offboarding (the
        // membership delete above has already committed, and every capture
        // path's live-membership gate refuses this member regardless).
        let plane = match crate::crypto::DekPlane::resolve_for_account(state, pool, account_id).await {
            Ok(plane) => Some(plane),
            Err(e) => {
                tracing::warn!(
                    error = %e, %account_id, %user_id,
                    "member offboarding: DEK plane unresolvable — revoking credentials without \
                     upstream calls or tenant-side purges"
                );
                None
            }
        };
        let upstream = crate::connectors::member_offboarding::HttpUpstream { http: &state.http_client };
        match crate::connectors::member_offboarding::revoke_departed_member_capture(
            &upstream,
            &state.http_client,
            pool,
            plane.as_ref(),
            account_id,
            user_id,
        )
        .await
        {
            Ok(outcome) => tracing::info!(
                %account_id, %user_id, clerk_org_id, ?outcome,
                "member offboarding: departed member's connector capture revoked"
            ),
            Err(e) => tracing::error!(
                error = %e, %account_id, %user_id, clerk_org_id, connector_health = true,
                "member offboarding: revoking the departed member's connectors FAILED — their \
                 org-scoped tokens may remain until the next capture event (whose membership \
                 gate re-runs this) or a webhook redelivery"
            ),
        }
    }

    Ok(())
}

/// CS-449 (item 38's drain) — best-effort trigger, called by
/// [`remove_org_membership`] right after a member's purchased slot is
/// actually freed. See `routes::org_join_requests::drain_org`'s own doc
/// comment for the gates it re-checks and why this never over-admits; this
/// wrapper's only job is the "never fails the caller" posture every other
/// best-effort step in `remove_org_membership` already takes (the Chat
/// re-election and the connector-health note, just above).
async fn drain_org_on_slot_freed(state: &AppState, pool: &PgPool, account_id: Uuid, clerk_org_id: &str) {
    let report = crate::routes::org_join_requests::drain_org(state, pool, account_id, clerk_org_id).await;
    if !report.admitted.is_empty() {
        tracing::info!(
            %account_id, clerk_org_id, admitted = report.admitted.len(),
            "auto_add: a member removal freed a slot and the drain admitted from the pool_full backlog"
        );
    }
}

/// The SAME normalisation every other `users.email` comparison in this
/// codebase uses — trim, then lowercase, and NOTHING else (see
/// `connectors::slack::normalize_email`'s own doc comment for the worked
/// example of why a plus-alias or a dot-stripping "provider canonicalisation"
/// would silently merge two distinct addresses). Kept as its own function
/// rather than reusing `connectors::slack::normalize_email` directly: that
/// function is `pub(crate)` to `connectors::slack` and this module has no
/// connector dependency — duplicating a one-line rule beats adding one.
fn normalize_user_email(raw: &str) -> String {
    raw.trim().to_lowercase()
}

/// CS-430 (org-onboarding-from-the-invite plan, D12, item 33). **This is the
/// one function CS-428's invite path and item 9's auto-add gate must call**
/// to ask "has this org already removed this person" — never a bare
/// `SELECT ... FROM org_membership_tombstones` against only one of the two
/// keys. (No living `docs/` page describes `org_membership_tombstones` as of
/// this story — checked `docs/connectors/*.md` and the rest of `docs/`
/// directly; the only hits are historical `docs/plans/*.md` write-ups of past
/// decisions, which this codebase's convention treats as a record of what
/// was decided, not a living reference to keep in sync. This doc comment is
/// therefore the authoritative description CS-428/item 9 should link back to
/// — if a living `docs/` page is ever added for org membership/offboarding,
/// point it here rather than duplicating the contract.)
///
/// Answers true if EITHER key matches a tombstone row for this
/// `clerk_org_id`:
///
///   - `user_id` — the original key (0081/0082): a still-valid token or a
///     roster read naming an account this org has already removed.
///   - `email` — added by migration 0226: the invite path holds only an
///     address, not a `user_id`, until the recipient has an account at all.
///     Closes D12's "Build note" residue for the case the founder's review
///     asked about: someone removed, then deletes and recreates their
///     CoralSwarm account, gets a brand-new `user_id` but the SAME address.
///
/// Callers pass whichever key(s) they hold — an invite email has only an
/// address; an auto-add gate reached from a roster match may have both.
/// Passing neither is a caller bug, not a database question, and answers
/// `false` without a query (`OR` of nothing is nothing to match).
///
/// Per-org, deliberately: this is scoped to `clerk_org_id` on BOTH arms, same
/// as every other tombstone read in this file, and never crosses into
/// another org's removal history — a person removed from Acme is a stranger
/// to Beta's tombstone, and must be invitable there on the strength of Beta's
/// own rules alone.
///
/// **Documented residue this does NOT close** (D12's own text, restated):
/// a member removed with no address on file (a null `email` column — not
/// reachable through `remove_org_membership`/`revoke_org_access` today, since
/// both read `users.email` and that column is `NOT NULL`, but the column
/// itself stays nullable rather than fabricate one) who is later re-invited
/// under a DIFFERENT address is not caught by the email arm, because there
/// is nothing in the row to compare against. Nor is a removed person who
/// returns under any address the org never saw them use before — "it does
/// not close a removed person returning under a different address, which
/// nothing can" (D12). Both are accepted limits, not defects: closing them
/// would require treating an unverified guess as a match, which is exactly
/// the fabrication this function's own `email` write-side refuses to do.
pub async fn is_org_membership_tombstoned(
    pool: &PgPool,
    clerk_org_id: &str,
    user_id: Option<Uuid>,
    email: Option<&str>,
) -> Result<bool, ProvisionError> {
    if user_id.is_none() && email.is_none() {
        return Ok(false);
    }
    let normalized_email = email.map(normalize_user_email);
    let tombstoned = sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS(SELECT 1 FROM org_membership_tombstones \
         WHERE clerk_org_id = $1 \
           AND ((\
                $2::uuid IS NOT NULL AND user_id = $2\
           ) OR (\
                $3::text IS NOT NULL AND email = $3\
           )))",
    )
    .bind(clerk_org_id)
    .bind(user_id)
    .bind(&normalized_email)
    .fetch_one(pool)
    .await?;
    Ok(tombstoned)
}

/// CS-428 (`docs/plans/org-onboarding-from-the-invite.md` §3 item 3, D12).
/// **This is the seam item 9's auto-add gate (not built yet) is meant to
/// call, and the one CS-428's own Slack invite pass
/// (`workers::slack_unmatched_email::process_job`) already does call** — a
/// thin, plane-aware wrapper over [`is_org_membership_tombstoned`] that
/// additionally knows a PERSONAL account can never have an org tombstone at
/// all, and answers `false` WITHOUT a query rather than passing a
/// synthetic/absent org id down into a `WHERE clerk_org_id = $1` that could
/// never match anyway.
///
/// `clerk_org_id: None` is the personal-plane short-circuit — `accounts.
/// clerk_org_id IS NULL`, the same column and the same "`None` means
/// personal" reading `workers::slack_unmatched_email::resolve_web_base`
/// already uses to choose between an org's own web base and the personal
/// one. A caller resolves it ONCE per unit of work (one channel walk, one
/// auto-add gate's own account lookup) — same "once per walk, not once per
/// member" discipline `connectors::slack::resolve_anchor_domain` documents
/// for itself — not once per recipient, even though this function itself is
/// cheap to call per-recipient.
///
/// Every other parameter and every other behavior is [`is_org_membership_
/// tombstoned`]'s own two-key (`user_id`/`email`) contract, forwarded
/// verbatim — see that function's own doc comment for the full "why EITHER
/// key", "per-org, deliberately", and documented-residue reasoning. This
/// wrapper adds NOTHING to that contract beyond the personal-plane
/// short-circuit.
pub async fn invite_suppressed_by_tombstone(
    pool: &PgPool,
    clerk_org_id: Option<&str>,
    user_id: Option<Uuid>,
    email: Option<&str>,
) -> Result<bool, ProvisionError> {
    let Some(clerk_org_id) = clerk_org_id else {
        return Ok(false);
    };
    is_org_membership_tombstoned(pool, clerk_org_id, user_id, email).await
}

#[cfg(test)]
mod invite_suppressed_by_tombstone_tests {
    //! DB-free proof of the personal-plane short-circuit — CS-428 (D12, item
    //! 3): "the check must short-circuit false on the personal plane without
    //! a query." No `ORGT_TEST_BASE_URL` gate needed: the `PgPool` below is
    //! built with `connect_lazy` against a host that does not exist and is
    //! never actually dialed unless a query is attempted, so this test
    //! doubles as the "no query" proof the story asks for (rather than
    //! timing) — any real query issued against it would hang/error against
    //! DNS resolution failure, not return `Ok(false)`.

    use super::invite_suppressed_by_tombstone;
    use uuid::Uuid;

    #[tokio::test]
    async fn personal_plane_short_circuits_without_ever_touching_the_pool() {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgresql://nobody@coralswarm-cs428-unreachable.invalid/db")
            .expect("connect_lazy must not itself dial anything");

        let suppressed = invite_suppressed_by_tombstone(&pool, None, Some(Uuid::new_v4()), Some("person@example.com"))
            .await
            .expect("the personal-plane branch must return before any query can fail");

        assert!(
            !suppressed,
            "a personal account has no org tombstones — this must be `false`, and it must be \
             reached without ever dialing the (unreachable) pool above"
        );
    }
}

/// Revoke EVERY member's access to an org at once — the `organization.deleted`
/// webhook path (org-tenancy PR 2.2 P0 fix, whole-org variant). One
/// transaction, in order:
///
/// 1. **Org-LEVEL revocation row** (`org_revocations`, migration 0082) — the
///    piece per-member tombstones cannot express (re-review P1): "nobody may
///    ever join this org again". A Clerk-side member who NEVER signed into
///    this backend has no `account_members` row to enumerate here, so without
///    this row their still-valid org token would later pass the claims-path
///    tombstone check (no tombstone for THEM) and INSERT a fresh membership
///    on the deleted org. Checked by `refresh_org_membership_from_claims`,
///    `ensure_org_membership`, /mcp's RequireExisting lookup, and the 0082 DB
///    trigger. Never cleared — Clerk org ids are never reused.
/// 2. Per-member tombstones for all CURRENT members (kept for uniformity with
///    single-member offboarding; also covers any window where the revocation
///    row alone hasn't been consulted yet).
/// 3. Member-ocean `member_ocean_desired_state` archive intents for all
///    CURRENT members (member-private-oceans PR 7) — see the call site's own
///    comment for why archiving still proceeds even though this org can
///    never be un-revoked.
/// 4. Delete all memberships on the org account.
///
/// The `accounts` row itself is deliberately KEPT (same audit posture as
/// `tenant_databases`' 'offboarded' rows) — and that is also load-bearing for
/// the tombstones/revocations: they carry the Clerk org id as plain TEXT
/// rather than an accounts FK precisely so that nothing about
/// deleting/recreating the account row can make them vanish.
pub async fn revoke_org_access(pool: &PgPool, clerk_org_id: &str) -> Result<(), ProvisionError> {
    let mut tx = pool.begin().await?;
    sqlx::query(
        "INSERT INTO org_revocations (clerk_org_id) VALUES ($1) \
         ON CONFLICT (clerk_org_id) DO UPDATE SET revoked_at = now()",
    )
    .bind(clerk_org_id)
    .execute(&mut *tx)
    .await?;
    // CS-430 (D12/item 33): same address capture as `remove_org_membership`,
    // fanned out to every current member in one INSERT ... SELECT — see that
    // function's own comment for the "why normalize at write time, why
    // nullable" reasoning. Normalizing in SQL here (`lower(trim(u.email))`,
    // the exact `connectors::slack`/`connectors::microsoft` convention) is
    // equivalent to `normalize_user_email` and avoids a round trip per member
    // for what is otherwise a genuinely set-based write.
    sqlx::query(
        "INSERT INTO org_membership_tombstones (clerk_org_id, user_id, email) \
         SELECT $1, am.user_id, lower(trim(u.email)) FROM account_members am \
         JOIN accounts a ON a.id = am.account_id \
         JOIN users u ON u.id = am.user_id \
         WHERE a.clerk_org_id = $1 \
         ON CONFLICT (clerk_org_id, user_id) DO UPDATE SET removed_at = now(), email = EXCLUDED.email",
    )
    .bind(clerk_org_id)
    .execute(&mut *tx)
    .await?;
    // Member-ocean lifecycle (member-private-oceans PR 7, plan §2.6
    // "Archive"): the whole-org variant of the intent-write in
    // `remove_org_membership` above — same replay-safe clock (the `CASE` on
    // `restored_at`, not a bare `COALESCE` — see that function's own doc
    // comment for the leave→rejoin→leave staleness bug this closes), just
    // fanned out to every CURRENT member in one INSERT ... SELECT (reads
    // `account_members` here, BEFORE it is deleted below, same as the
    // tombstones insert just above). Runs even though this org may never be
    // un-revoked (see `org_revocations`'s own doc): the reconciler's archive
    // arm still materializes `archived_at` for these rows so an admin export
    // remains possible in the window before the org's own teardown
    // physically deletes the tenant database (plan §2.6: "the org's clock
    // wins" governs TEARDOWN timing, not whether archiving itself still
    // happens) — restoring/provisioning into a revoked org is what stays
    // permanently blocked (plan §2.6/§D — see
    // `workers::member_ocean_reconciler::tick`'s `org_revocations` check).
    sqlx::query(
        "INSERT INTO member_ocean_desired_state \
             (clerk_org_id, user_id, state, offboarded_at, updated_at) \
         SELECT $1, am.user_id, 'archived', now(), now() \
         FROM account_members am \
         JOIN accounts a ON a.id = am.account_id \
         WHERE a.clerk_org_id = $1 \
         ON CONFLICT (clerk_org_id, user_id) DO UPDATE \
           SET state = 'archived', \
               offboarded_at = CASE \
                   WHEN member_ocean_desired_state.restored_at IS NOT NULL THEN now() \
                   ELSE COALESCE(member_ocean_desired_state.offboarded_at, now()) \
                 END, \
               restored_at = NULL, \
               updated_at = now()",
    )
    .bind(clerk_org_id)
    .execute(&mut *tx)
    .await?;
    sqlx::query(
        "DELETE FROM account_members am USING accounts a \
         WHERE am.account_id = a.id AND a.clerk_org_id = $1",
    )
    .bind(clerk_org_id)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(())
}

// ── Live-Postgres tests (billing self-serve PR F review, findings 1 & 6) ────
//
// `ensure_org_membership`/`remove_org_membership` are the two Clerk-webhook
// writers `billing::seat_lifecycle::acquire_seat`/`release_seat` are pushed
// through from — same discipline as those modules' own `mod pg_tests`:
// `RELINFER_TEST_DATABASE_URL`-gated, `--test-threads=1` in CI, a mock Stripe
// standing in for the real API (the same local-axum-server substitute
// `routes::checkout::pg_tests` established).
#[cfg(test)]
mod pg_tests {

    async fn execute_financial_fixture<'q>(
        pool: &sqlx::PgPool,
        account_id: Uuid,
        query: sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments>,
    ) -> Result<sqlx::postgres::PgQueryResult, sqlx::Error> {
        let mut tx = crate::tenancy::scope::begin_account_scoped(pool, account_id)
            .await
            .map_err(|e| sqlx::Error::Protocol(e.to_string()))?;
        crate::billing::renewal::lock_period_rotation(&mut tx, account_id).await?;
        match query.execute(&mut *tx).await {
            Ok(result) => {
                tx.commit().await?;
                Ok(result)
            }
            Err(error) => {
                tx.rollback().await?;
                Err(error)
            }
        }
    }

    async fn fetch_financial_fixture_id<'q>(
        pool: &sqlx::PgPool,
        account_id: Uuid,
        query: sqlx::query::QueryScalar<'q, sqlx::Postgres, Uuid, sqlx::postgres::PgArguments>,
    ) -> Result<Uuid, sqlx::Error> {
        let mut tx = crate::tenancy::scope::begin_account_scoped(pool, account_id)
            .await
            .map_err(|e| sqlx::Error::Protocol(e.to_string()))?;
        crate::billing::renewal::lock_period_rotation(&mut tx, account_id).await?;
        match query.fetch_one(&mut *tx).await {
            Ok(result) => {
                tx.commit().await?;
                Ok(result)
            }
            Err(error) => {
                tx.rollback().await?;
                Err(error)
            }
        }
    }

    use super::*;
    use crate::billing::stripe_client::StripeClient;
    use crate::pg_catalog_test_support::{set_seat_tier_price, CATALOG_PRICE_LOCK};

    async fn pg_test_pool(test_name: &str) -> Option<PgPool> {
        let url = match std::env::var("RELINFER_TEST_DATABASE_URL") {
            Ok(u) => u,
            Err(_) => {
                eprintln!("skipping {test_name}: set RELINFER_TEST_DATABASE_URL to run it");
                return None;
            }
        };
        Some(
            sqlx::postgres::PgPoolOptions::new()
                .max_connections(4)
                .connect(&url)
                .await
                .expect("connect to test Postgres"),
        )
    }

    /// Snapshot an account's pool the way production reads it —
    /// `billing::slot_pool::read_slot_pool` inside an account-scoped
    /// transaction. CS-444: the offboarding test below is about the three
    /// numbers (`purchased`/`used`/`available`), not about any one table.
    async fn slot_pool_snapshot(
        pool: &PgPool,
        account_id: Uuid,
    ) -> crate::billing::slot_pool::SlotPool {
        let mut tx = crate::tenancy::scope::begin_account_scoped(pool, account_id)
            .await
            .expect("scope the account");
        let snapshot = crate::billing::slot_pool::read_slot_pool(&mut tx, account_id)
            .await
            .expect("read the slot pool");
        tx.commit().await.expect("commit the read");
        snapshot
    }

    /// A mock that answers 500 to EVERY request — used to prove "no Stripe
    /// call happened" (finding 1) via propagation of an error the test would
    /// otherwise see, and to prove offboarding neither calls Stripe nor lets
    /// a billing error block the membership delete.
    ///
    /// **The hit counter now proves the opposite of what it was added for.**
    /// It arrived (billing self-serve PR F review) because "still deleted" +
    /// "ledger unchanged" could not distinguish "`release_seat` properly
    /// attempted a Stripe push, then failed gracefully" from "`release_seat`
    /// silently skipped Stripe entirely", so finding 6's test asserted
    /// `> 0`. CS-444 made the silent-skip reading the CORRECT one — a
    /// release makes no Stripe call by design — so
    /// [`offboarding_frees_the_slot_without_calling_stripe`] asserts `== 0`
    /// instead. Same instrument, inverted claim; keeping it is what makes a
    /// reintroduced push during offboarding visible.
    async fn spawn_failing_stripe_mock() -> (StripeClient, std::sync::Arc<std::sync::atomic::AtomicUsize>) {
        let hits = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let hits_for_handler = hits.clone();
        let app = axum::Router::new().fallback(axum::routing::any(move || {
            let hits = hits_for_handler.clone();
            async move {
                hits.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                axum::http::StatusCode::INTERNAL_SERVER_ERROR
            }
        }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind mock");
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.ok();
        });
        (
            StripeClient::new(reqwest::Client::new(), "sk_test_mock").with_base_url(format!("http://{addr}")),
            hits,
        )
    }

    /// A mock that answers 200 to every request with ONE fixed subscription
    /// item at `price_id`/`quantity` — used by the retry-gate regression
    /// test below to prove a REDELIVERED `.created` event can actually
    /// complete a seat purchase that an earlier delivery failed at. Same
    /// shape as `billing::seat_lifecycle::pg_tests::spawn_seat_mock` (that
    /// module's own doc comment: a near-identical copy rather than a
    /// cross-module dependency on another file's test internals). Returns a
    /// hit counter (CS-442 fixup) so a caller can assert Stripe was
    /// ACTUALLY reached, not merely that the call succeeded — a silently
    /// broken consume-before-buy short-circuit would also leave the seat
    /// purchased, with no Stripe call at all.
    async fn spawn_seat_mock(price_id: &str, quantity: i64) -> (StripeClient, std::sync::Arc<std::sync::atomic::AtomicUsize>) {
        let hits = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let hits_for_handler = hits.clone();
        let body = serde_json::json!({
            "id": "sub_mock_1",
            "object": "subscription",
            "status": "active",
            "items": {
                "object": "list",
                "data": [{
                    "id": "si_mock_1",
                    "quantity": quantity,
                    "current_period_start": 1_700_000_000,
                    "current_period_end": 1_702_678_400,
                    "price": {"id": price_id},
                }],
            },
        });
        let app = axum::Router::new().fallback(axum::routing::any(move || {
            let body = body.clone();
            let hits = hits_for_handler.clone();
            async move {
                hits.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                (
                    axum::http::StatusCode::OK,
                    [("content-type", "application/json")],
                    body.to_string(),
                )
            }
        }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind mock");
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.ok();
        });
        (
            StripeClient::new(reqwest::Client::new(), "sk_test_mock").with_base_url(format!("http://{addr}")),
            hits,
        )
    }

    /// A `team`-plan org account with ONE member at `tier_code`, a live
    /// `active` linked Stripe subscription, and (when `member_purchased`) a
    /// `grant_seat_tiers` ledger row — a real subscription and grant exist so
    /// that, if `acquire_seat` were WRONGLY invoked, it would actually
    /// attempt a Stripe push (hitting whatever mock the test wired up)
    /// rather than silently no-op via its own "no live subscription" guard.
    ///
    /// CS-444: that argument no longer extends to `release_seat`, which
    /// makes no Stripe call for any subscription state — the live
    /// subscription is still seeded for the offboarding test, but there it
    /// proves the stronger thing (Stripe was reachable and STILL never
    /// called), not the weaker "a wrong call would have been visible".
    /// Returns `(account_id, owner_user_id, member_user_id, clerk_org_id,
    /// grant_id)`.
    async fn seed_team_org_with_member(
        pool: &PgPool,
        tier_code: &str,
        member_purchased: bool,
    ) -> (Uuid, Uuid, Uuid, String, Uuid) {
        let owner = Uuid::new_v4();
        sqlx::query("INSERT INTO users (id, email) VALUES ($1, $2)")
            .bind(owner)
            .bind(format!("clerk-provision-owner-{}@test.example", owner.simple()))
            .execute(pool)
            .await
            .expect("insert owner user");
        let member = Uuid::new_v4();
        sqlx::query("INSERT INTO users (id, email, clerk_user_id) VALUES ($1, $2, $3)")
            .bind(member)
            .bind(format!("clerk-provision-member-{}@test.example", member.simple()))
            .bind(format!("clerk_user_{}", member.simple()))
            .execute(pool)
            .await
            .expect("insert member user");

        let clerk_org_id = format!("org_clerk_provision_{}", Uuid::new_v4().simple());
        let account_id: Uuid = sqlx::query_scalar(
            "INSERT INTO accounts (owner_user_id, clerk_org_id) VALUES ($1, $2) RETURNING id",
        )
        .bind(owner)
        .bind(&clerk_org_id)
        .fetch_one(pool)
        .await
        .expect("insert org account");

        let tier_id: Uuid = sqlx::query_scalar("SELECT id FROM seat_tiers WHERE code = $1 AND is_active = true")
            .bind(tier_code)
            .fetch_one(pool)
            .await
            .unwrap_or_else(|e| panic!("seat_tiers {tier_code:?} must be seeded (0165): {e}"));
        sqlx::query(
            "INSERT INTO account_members (account_id, user_id, role, seat_tier_id) VALUES ($1, $2, 'owner', $3)",
        )
        .bind(account_id)
        .bind(owner)
        .bind(tier_id)
        .execute(pool)
        .await
        .expect("insert owner membership");
        sqlx::query(
            "INSERT INTO account_members (account_id, user_id, role, seat_tier_id, seat_purchased_at) \
             VALUES ($1, $2, 'member', $3, CASE WHEN $4 THEN now() ELSE NULL END)",
        )
        .bind(account_id)
        .bind(member)
        .bind(tier_id)
        .bind(member_purchased)
        .execute(pool)
        .await
        .expect("insert member membership");

        let plan_id: Uuid = sqlx::query_scalar("SELECT id FROM plans WHERE code = $1")
            .bind(crate::billing::MIXED_TIER_PLAN_CODE)
            .fetch_one(pool)
            .await
            .expect("team plan must be seeded (0167)");
        let grant_id: Uuid = fetch_financial_fixture_id(
            pool,
            account_id,
            sqlx::query_scalar(
            "INSERT INTO grants (account_id, plan_id, seats, plan_is_seat_priced) \
             VALUES ($1, $2, 1, true) RETURNING id",
        )
        .bind(account_id)
        .bind(plan_id),
        )
        .await
        .expect("insert team grant");
        if member_purchased {
            execute_financial_fixture(pool, account_id, sqlx::query("INSERT INTO grant_seat_tiers (grant_id, seat_tier_id, seats) VALUES ($1, $2, 1)")
                .bind(grant_id)
                .bind(tier_id))
                .await
                .expect("seed the purchased ledger");
        }

        execute_financial_fixture(pool, account_id, sqlx::query(
            "INSERT INTO stripe_subscriptions (account_id, stripe_customer_id, stripe_subscription_id, status) \
             VALUES ($1, $2, $3, 'active')",
        )
        .bind(account_id)
        .bind(format!("cus_{}", Uuid::new_v4().simple()))
        .bind(format!("sub_{}", Uuid::new_v4().simple())))
        .await
        .expect("link stripe subscription");

        (account_id, owner, member, clerk_org_id, grant_id)
    }

    async fn cleanup(pool: &PgPool, account_id: Uuid, users: &[Uuid]) {
        let _ = execute_financial_fixture(
            pool,
            account_id,
            sqlx::query("DELETE FROM accounts WHERE id = $1").bind(account_id),
        ).await;
        for u in users {
            let _ = sqlx::query("DELETE FROM users WHERE id = $1").bind(u).execute(pool).await;
        }
    }

    // Billing self-serve PR F review round 2, minor finding: the one test
    // below that mutates the shared `seat_tiers.standard` row (a global
    // catalog row every other billing pg-test suite in this build also
    // resolves) follows the SAME `CATALOG_PRICE_LOCK` +
    // capture-then-restore-then-assert discipline as
    // `routes::billing_seats::pg_tests` / `routes::checkout::pg_tests` — see
    // `crate::pg_catalog_test_support`'s own doc comment on
    // `CATALOG_PRICE_LOCK`/`set_seat_tier_price` (CodeRabbit round 3
    // consolidated all four modules' independent locks/helpers into that one
    // shared location) — restoring the row BEFORE any assertion below can
    // panic, not only on the all-assertions-passed path.

    fn test_state(stripe: StripeClient) -> AppState {
        let mut state = crate::test_app_state_owned(None);
        state.stripe = Some(std::sync::Arc::new(stripe));
        state
    }

    /// **Finding 1.** An `organizationMembership.updated`-shaped call (a
    /// role-only change against an ALREADY-EXISTING, never-purchased member —
    /// `ensure_org_membership` cannot itself distinguish `.created` from
    /// `.updated`, only whether its own upsert INSERTED or UPDATED) must
    /// perform NO Stripe call and NO ledger change. The mock 500s on ANY
    /// request, so a wrongly-invoked `acquire_seat` would surface as a
    /// propagated `Err` here — this test's `.expect("...")` on `Ok(())` is
    /// what actually proves the gate held, not merely that the ledger looks
    /// unchanged (which a silently-swallowed error could also produce).
    #[tokio::test]
    async fn an_updated_redelivery_for_an_existing_unpurchased_member_acquires_nothing() {
        let Some(pool) =
            pg_test_pool("an_updated_redelivery_for_an_existing_unpurchased_member_acquires_nothing").await
        else {
            return;
        };
        let (account_id, owner, member, clerk_org_id, grant_id) =
            seed_team_org_with_member(&pool, "standard", false).await;
        let (stripe, hits) = spawn_failing_stripe_mock().await;
        let state = test_state(stripe);

        // A role-only `.updated` redelivery for the SAME, already-existing
        // member — `is_created_event = false` (this is `handle_membership_upsert`'s
        // OWN `organizationMembership.updated` dispatch arm), which routes
        // to `ensure_org_membership`'s plain role-UPDATE branch.
        ensure_org_membership(&state, &pool, &clerk_org_id, member, "admin", false, None, None)
            .await
            .expect("a role-only update for an existing member must not touch Stripe at all");
        assert_eq!(
            hits.load(std::sync::atomic::Ordering::SeqCst),
            0,
            "a role-only update for an existing member must not even ATTEMPT a Stripe call"
        );

        let (role, seat_purchased_at): (String, Option<chrono::DateTime<chrono::Utc>>) = sqlx::query_as(
            "SELECT role, seat_purchased_at FROM account_members WHERE account_id = $1 AND user_id = $2",
        )
        .bind(account_id)
        .bind(member)
        .fetch_one(&pool)
        .await
        .expect("read member row");
        assert_eq!(role, "admin", "the role change itself must still take effect");
        assert_eq!(
            seat_purchased_at, None,
            "an existing, never-purchased member's seat must stay unpurchased across a role-only update"
        );

        let ledger: Option<i32> = sqlx::query_scalar(
            "SELECT gst.seats FROM grant_seat_tiers gst \
             JOIN seat_tiers st ON st.id = gst.seat_tier_id \
             WHERE gst.grant_id = $1 AND st.code = 'standard'",
        )
        .bind(grant_id)
        .fetch_optional(&pool)
        .await
        .expect("read grant_seat_tiers");
        assert_eq!(ledger, None, "no ledger row must have been created for this never-purchased member");

        cleanup(&pool, account_id, &[owner, member]).await;
    }

    /// **CS-444 — negative test 1 (offboarding path).** *Removing a member
    /// leaves `purchased` unchanged and Stripe un-called, while `available`
    /// rises by one.*
    ///
    /// **This test asserted a different thing until CS-444.** It was
    /// `a_failed_stripe_release_still_deletes_the_membership` (finding 6) and
    /// its load-bearing assertion was `hit_count > 0` — that the release had
    /// genuinely ATTEMPTED a Stripe mutation before failing, since "still
    /// deleted" + "ledger unchanged" alone could not tell a proper attempt
    /// apart from a silent skip. That assertion is inverted here to
    /// `hit_count == 0`: a release makes NO Stripe call at all any more, so
    /// the same mock that used to prove "it tried and failed" now proves "it
    /// never tried". Finding 6's own property — a release failure must not
    /// block the delete — is kept and still asserted; it just costs nothing
    /// now, because no billing moves during a release.
    ///
    /// The Stripe mock is kept (rather than passing no client) precisely
    /// because THIS caller still holds one: `remove_org_membership` takes
    /// `&AppState`, so a future edit that reintroduced a Stripe call during
    /// offboarding would have something to call, and `hit_count == 0` is what
    /// would catch it.
    ///
    /// # Why the stamp clear is checked MID-FLIGHT (mutation review, #1030)
    ///
    /// The end-state pool assertion below (`(1,1,0)` -> `(1,0,1)`) **cannot
    /// detect a broken stamp clear on this path**, and it would be dishonest
    /// to let it read as though it could. `billing::slot_pool`'s `used`
    /// counts `account_members` rows carrying a stamp, and
    /// `remove_org_membership` DELETES the row moments later — so `used`
    /// falls to 0 whether or not `release_seat` ever cleared anything. A
    /// mutation that turned the clear into a no-op write (`SET
    /// seat_purchased_at = seat_purchased_at`, which still matches the guard
    /// and still reports `rows_affected() == 1`) left this test green.
    ///
    /// So the clear is observed while it is still observable. The offboarding
    /// transaction's FIRST statement is the `org_membership_tombstones`
    /// upsert, and the `DELETE FROM account_members` comes after it in the
    /// SAME transaction — so holding an uncommitted, conflicting tombstone
    /// row parks that transaction before it can delete anything.
    /// `release_seat` has already committed by then (it runs BEFORE that
    /// transaction opens — the caller contract in its own doc comment), so
    /// the member row is still present and its `seat_purchased_at` is
    /// readable. That read is the assertion the no-op-write mutation fails.
    ///
    /// It doubles as the only test of the ORDERING contract: release first,
    /// delete second. A future edit that moved `release_seat` after the
    /// delete would find no row to clear, and this would catch it.
    ///
    /// The poll is bounded and cannot flake red: the parked transaction
    /// cannot delete the row while the blocker is uncommitted, so the only
    /// two outcomes are "cleared" (pass) and "still stamped after 3s"
    /// (fail, correctly).
    #[tokio::test]
    async fn offboarding_frees_the_slot_without_calling_stripe() {
        let Some(pool) = pg_test_pool("offboarding_frees_the_slot_without_calling_stripe").await
        else {
            return;
        };
        let _catalog_guard = CATALOG_PRICE_LOCK.lock().await;
        let (account_id, owner, member, clerk_org_id, grant_id) =
            seed_team_org_with_member(&pool, "standard", true).await;
        // A real, non-NULL price on the tier: the pre-CS-444 code path
        // resolved it before reaching Stripe, and leaving it NULL would make
        // "no Stripe call" true for the wrong reason (an early internal
        // error rather than the absence of any push). Keeping it set means
        // the only thing standing between this test and a Stripe call is the
        // change under test.
        let original_standard =
            set_seat_tier_price(&pool, "standard", Some("price_clerk_provision_release_frees")).await;
        let clerk_user_id: Option<String> =
            sqlx::query_scalar("SELECT clerk_user_id FROM users WHERE id = $1")
                .bind(member)
                .fetch_one(&pool)
                .await
                .expect("read the member's clerk_user_id");
        let clerk_user_id = clerk_user_id.expect("test fixture always sets clerk_user_id");
        let (stripe, hits) = spawn_failing_stripe_mock().await;
        let state = test_state(stripe);

        let pool_before = slot_pool_snapshot(&pool, account_id).await;

        // Park the offboarding transaction at its tombstone upsert by holding
        // a conflicting row uncommitted — see this test's own doc comment.
        // `ON CONFLICT (clerk_org_id, user_id) DO UPDATE` must wait on this
        // row's lock, and the `DELETE FROM account_members` that follows it
        // in the same transaction therefore cannot run yet.
        let mut delete_blocker = pool.begin().await.expect("begin the offboarding blocker");
        sqlx::query(
            "INSERT INTO org_membership_tombstones (clerk_org_id, user_id, email) VALUES ($1, $2, NULL)",
        )
        .bind(&clerk_org_id)
        .bind(member)
        .execute(&mut *delete_blocker)
        .await
        .expect("hold a conflicting tombstone row, uncommitted");

        let task_pool = pool.clone();
        let task_org = clerk_org_id.clone();
        let task_user = clerk_user_id.clone();
        let handle = tokio::spawn(async move {
            remove_org_membership(&state, &task_pool, &task_org, &task_user).await
        });

        // Poll until `release_seat` has committed its clear. `RowGone` is
        // unreachable while the blocker is held (that is the point of
        // holding it) but is distinguished from `StillStamped` anyway, so a
        // failure names which of the two actually happened instead of
        // collapsing them into one ambiguous `None`.
        #[derive(Debug, PartialEq, Eq)]
        enum MidFlight {
            Cleared,
            StillStamped,
            RowGone,
        }
        let mut mid_flight = MidFlight::StillStamped;
        for _ in 0..120 {
            let row: Option<Option<chrono::DateTime<chrono::Utc>>> = sqlx::query_scalar(
                "SELECT seat_purchased_at FROM account_members WHERE account_id = $1 AND user_id = $2",
            )
            .bind(account_id)
            .bind(member)
            .fetch_optional(&pool)
            .await
            .expect("read the member's stamp mid-flight");
            match row {
                Some(None) => {
                    mid_flight = MidFlight::Cleared;
                    break;
                }
                None => {
                    mid_flight = MidFlight::RowGone;
                    break;
                }
                Some(Some(_)) => tokio::time::sleep(std::time::Duration::from_millis(25)).await,
            }
        }

        delete_blocker.commit().await.expect("release the offboarding blocker");
        let result = handle.await.expect("the offboarding task must not panic");
        let hit_count = hits.load(std::sync::atomic::Ordering::SeqCst);

        let still_present: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM account_members WHERE account_id = $1 AND user_id = $2)",
        )
        .bind(account_id)
        .bind(member)
        .fetch_one(&pool)
        .await
        .expect("check membership presence");

        let ledger: Option<i32> = sqlx::query_scalar(
            "SELECT gst.seats FROM grant_seat_tiers gst \
             JOIN seat_tiers st ON st.id = gst.seat_tier_id \
             WHERE gst.grant_id = $1 AND st.code = 'standard'",
        )
        .bind(grant_id)
        .fetch_optional(&pool)
        .await
        .expect("read grant_seat_tiers");
        let pool_after = slot_pool_snapshot(&pool, account_id).await;

        set_seat_tier_price(&pool, "standard", original_standard.as_deref()).await;
        cleanup(&pool, account_id, &[owner, member]).await;

        result.expect("offboarding must not propagate a billing error");
        assert_eq!(
            mid_flight,
            MidFlight::Cleared,
            "release_seat must actually CLEAR seat_purchased_at, and must do it BEFORE the \
             membership row is deleted. This is the only assertion in this test that a broken \
             stamp clear can fail — the end-state pool assertion below is carried by the row \
             DELETE and stays green either way (see this test's own doc comment)."
        );
        assert_eq!(
            hit_count, 0,
            "a release must not touch Stripe at all — this mock answers 500 to every request and \
             counts the hits, so any push during offboarding lands here"
        );
        assert!(!still_present, "the membership row must be deleted");
        assert_eq!(
            ledger,
            Some(1),
            "the org keeps the slot it purchased — removing a member frees the slot, it does not \
             un-buy it"
        );
        assert_eq!(
            (pool_before.purchased, pool_before.used, pool_before.available()),
            (1, 1, 0),
            "the fixture starts full: one slot bought, one member on it"
        );
        assert_eq!(
            (pool_after.purchased, pool_after.used, pool_after.available()),
            (1, 0, 1),
            "after the removal the same slot is still purchased, now free for the next joiner. \
             What this proves and what it does not: the `purchased` half IS pinned here (a \
             reinstated grant_seat_tiers decrement would read 0), but the `used` half is carried \
             by the row DELETE, not by the stamp clear — the mid-flight assertion above is what \
             pins the clear on this path, and \
             billing::seat_lifecycle::pg_tests::release_seat_frees_the_slot_and_leaves_purchased_untouched \
             pins it where no delete is involved at all."
        );
    }

    /// Guards `CLERK_API_BASE_URL`/`CLERK_SECRET_KEY` — process-global env
    /// vars — for this module's own tests. Same LOCAL-lock convention
    /// `routes::org_invites::pg_tests`'s own `ENV_LOCK` and `routes::
    /// org_join_requests::pg_tests`'s own `ENV_LOCK` each already use (see
    /// either's doc comment): this file had no Clerk-calling test before
    /// the drain blocker-3 regression test below, so it gets its own.
    static ENV_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
    #[allow(dead_code)]
    struct ClerkEnvGuard(tokio::sync::MutexGuard<'static, ()>);
    impl ClerkEnvGuard {
        async fn acquire(clerk_base: &str) -> Self {
            let guard = ENV_LOCK.lock().await;
            // SAFETY: serialized by `guard`, held for this struct's whole
            // lifetime (including on panic/unwind, via `Drop` below).
            unsafe {
                std::env::set_var("CLERK_API_BASE_URL", clerk_base);
                std::env::set_var("CLERK_SECRET_KEY", "sk_test_mock_clerk_provision_drain_order");
            }
            Self(guard)
        }
    }
    impl Drop for ClerkEnvGuard {
        fn drop(&mut self) {
            // SAFETY: see `acquire`.
            unsafe {
                std::env::remove_var("CLERK_API_BASE_URL");
                std::env::remove_var("CLERK_SECRET_KEY");
            }
        }
    }

    /// **Blocker fix, CS-449 review round 2 (blocker 3) — access revocation
    /// must commit BEFORE the drain's Clerk round trips, not wait on them.**
    ///
    /// Seeds a departing member holding the org's one purchased slot, and a
    /// SEPARATE candidate already queued `pool_full` whose evidence passes
    /// every gate (a real admit, not a refusal) — so removing the departing
    /// member frees the exact slot the drain will spend on that candidate,
    /// and the drain's own Clerk membership-CREATE call genuinely fires.
    ///
    /// The Clerk mock's create-membership handler is the instrument: on
    /// every invocation it queries THIS SAME POOL, live, for whether the
    /// departing member's `account_members` row is already gone and their
    /// `org_membership_tombstones` row already written — the two things
    /// `remove_org_membership`'s own revocation is. If the drain's Clerk
    /// call fires before revocation has committed (the pre-fix ordering:
    /// the drain awaited BEFORE the tombstone+delete transaction), this
    /// observes the row still present and the tombstone still missing, and
    /// records that. `create_calls` proves the observation is meaningful at
    /// all — a mock never hit would make `revoked_before_drain_call`'s
    /// default vacuously true.
    #[tokio::test]
    async fn remove_org_membership_revokes_access_before_the_drain_calls_clerk() {
        let Some(pool) = pg_test_pool("remove_org_membership_revokes_access_before_the_drain_calls_clerk").await
        else {
            return;
        };
        let (account_id, owner, member, clerk_org_id, _grant_id) =
            seed_team_org_with_member(&pool, "standard", true).await;
        let clerk_user_id: Option<String> = sqlx::query_scalar("SELECT clerk_user_id FROM users WHERE id = $1")
            .bind(member)
            .fetch_one(&pool)
            .await
            .expect("read the departing member's clerk_user_id");
        let clerk_user_id = clerk_user_id.expect("test fixture always sets clerk_user_id");

        // A DNS-verified domain, auto-add turned on, and one CLEAN
        // `pool_full` candidate whose stored ladder already passes every
        // gate — the shape `routes::org_join_requests::drain_org`'s own
        // candidate set is filtered to.
        let domain = format!("clerkprov-order-{}.example", Uuid::new_v4().simple());
        sqlx::query(
            "INSERT INTO org_domains (account_id, domain, verification_token, verified_at, method) \
             VALUES ($1, $2, $3, now(), 'dns_txt')",
        )
        .bind(account_id)
        .bind(&domain)
        .bind(format!("tok_{}", Uuid::new_v4().simple()))
        .execute(&pool)
        .await
        .expect("seed a verified domain");
        crate::services::org_auto_add::upsert_settings(&pool, account_id, true, None, owner)
            .await
            .expect("enable auto-add");

        let candidate_id = Uuid::new_v4();
        let candidate_email = format!("clerkprov-order-cand-{}@{domain}", candidate_id.simple());
        sqlx::query("INSERT INTO users (id, email, name, clerk_user_id) VALUES ($1, $2, $3, $4)")
            .bind(candidate_id)
            .bind(&candidate_email)
            .bind("Drain Order Candidate")
            .bind(format!("clerk_cand_{}", candidate_id.simple()))
            .execute(&pool)
            .await
            .expect("insert candidate user");
        let now = chrono::Utc::now();
        let ladder = crate::services::assertion_ladder::evaluate(crate::services::assertion_ladder::LadderSignals {
            directory: crate::services::assertion_ladder::DirectorySignal::Unavailable("test".to_string()),
            workspace: crate::services::assertion_ladder::WorkspaceSignal::FullMember,
            domain: crate::services::assertion_ladder::DomainSignal::Match(domain.clone()),
        });
        let evidence = serde_json::json!({
            "email": candidate_email,
            "invited_at": now.to_rfc3339(),
            "ladder": ladder.evidence(),
        });
        let candidate_request_id: Uuid = sqlx::query_scalar(
            "INSERT INTO org_join_requests (account_id, user_id, source, evidence, requested_at) \
             VALUES ($1, $2, 'pool_full', $3, $4) RETURNING id",
        )
        .bind(account_id)
        .bind(candidate_id)
        .bind(evidence)
        .bind(now - chrono::Duration::minutes(30))
        .fetch_one(&pool)
        .await
        .expect("seed the pool_full candidate");

        // The instrument: observes revocation state at the moment the
        // drain's own Clerk call fires.
        let revoked_before_drain_call = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true));
        let create_calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let observer_pool = pool.clone();
        let observer_org = clerk_org_id.clone();
        let revoked_flag = revoked_before_drain_call.clone();
        let calls_flag = create_calls.clone();
        let app = axum::Router::new()
            .route(
                "/v1/organizations/{org_id}/memberships",
                axum::routing::post(move || {
                    let pool = observer_pool.clone();
                    let clerk_org_id = observer_org.clone();
                    let revoked_flag = revoked_flag.clone();
                    let calls_flag = calls_flag.clone();
                    async move {
                        calls_flag.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        let member_row_gone: bool = sqlx::query_scalar(
                            "SELECT NOT EXISTS(SELECT 1 FROM account_members WHERE account_id = $1 AND user_id = $2)",
                        )
                        .bind(account_id)
                        .bind(member)
                        .fetch_one(&pool)
                        .await
                        .unwrap_or(false);
                        let tombstoned: bool = sqlx::query_scalar(
                            "SELECT EXISTS(SELECT 1 FROM org_membership_tombstones WHERE clerk_org_id = $1 AND user_id = $2)",
                        )
                        .bind(&clerk_org_id)
                        .bind(member)
                        .fetch_one(&pool)
                        .await
                        .unwrap_or(false);
                        if !(member_row_gone && tombstoned) {
                            revoked_flag.store(false, std::sync::atomic::Ordering::SeqCst);
                        }
                        (axum::http::StatusCode::OK, axum::Json(serde_json::json!({"id": "orgmem_mock"})))
                    }
                }),
            )
            .route(
                "/v1/organizations/{org_id}/memberships/{user_id}",
                axum::routing::delete(|| async {
                    (axum::http::StatusCode::OK, axum::Json(serde_json::json!({"id": "orgmem_mock"})))
                }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind clerk mock");
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.ok();
        });
        let clerk_base = format!("http://{addr}");
        let _env = ClerkEnvGuard::acquire(&clerk_base).await;

        let (stripe, stripe_hits) = spawn_failing_stripe_mock().await;
        let state = test_state(stripe);

        let result = remove_org_membership(&state, &pool, &clerk_org_id, &clerk_user_id).await;

        let member_still_present: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM account_members WHERE account_id = $1 AND user_id = $2)",
        )
        .bind(account_id)
        .bind(member)
        .fetch_one(&pool)
        .await
        .unwrap();
        let (candidate_source, candidate_decision): (String, Option<String>) =
            sqlx::query_as("SELECT source, decision FROM org_join_requests WHERE id = $1")
                .bind(candidate_request_id)
                .fetch_one(&pool)
                .await
                .unwrap();

        cleanup(&pool, account_id, &[owner, member, candidate_id]).await;

        result.expect("offboarding must succeed");
        assert_eq!(
            stripe_hits.load(std::sync::atomic::Ordering::SeqCst),
            0,
            "neither the release nor the drain call Stripe"
        );
        assert!(
            create_calls.load(std::sync::atomic::Ordering::SeqCst) >= 1,
            "the drain must actually have attempted the candidate's admit — otherwise the ordering \
             observation below is vacuous"
        );
        assert!(
            revoked_before_drain_call.load(std::sync::atomic::Ordering::SeqCst),
            "blocker 3: the departed member's `account_members` row must already be gone and their \
             tombstone already written BEFORE the drain's own Clerk membership-create call fires — \
             this failing means revocation is still waiting on the add, exactly the defect this fix \
             closes"
        );
        assert!(!member_still_present, "the departed member's own access is revoked");
        assert_eq!(
            candidate_decision.as_deref(),
            Some("approved"),
            "end to end: the drain actually admitted the candidate from the freed slot"
        );
        assert_eq!(candidate_source, "pool_full");
    }

    /// **Billing self-serve PR F review, MAJOR finding — retry gate.** A
    /// `.created` delivery whose `acquire_seat` call fails at Stripe must be
    /// RETRIABLE by Clerk's own redelivery, not permanently stuck: the
    /// membership row already committed on the FIRST (failing) attempt
    /// (before `acquire_seat` is ever called — this function's own doc
    /// comment), so `newly_inserted` reads `false` on the SECOND (retried)
    /// attempt — only `is_created_event` staying `true` across the
    /// redelivery (Clerk redelivers an event as ITSELF: a `.created`
    /// redelivery arrives as `.created` again, never `.updated`) is what
    /// makes the second call retry `acquire_seat` at all. Before this fix,
    /// `newly_inserted` alone gated `acquire_seat` off on the second call,
    /// leaving the member permanently unpurchased with no retry path.
    ///
    /// # Fixture fix (CS-442 fixup, D1 consume-before-buy, CS-435/CS-442)
    ///
    /// This test's own fixture used to seed a purchased-but-UNCONSUMED
    /// standard slot (`grant_seat_tiers.seats = 1`, the owner's own
    /// `account_members` row left `seat_purchased_at IS NULL`) — a real free
    /// slot. That predates CS-435's consume-before-buy change to
    /// `acquire_seat`; once that shipped, this fixture's free slot meant the
    /// FIRST delivery below actually consumed it locally, with ZERO Stripe
    /// calls, and never reached the failing mock this test's own `first_is_err`
    /// assertion expects — the retry-gate behaviour this test exists to
    /// prove was never actually exercised, silently, on `main`, since CS-435
    /// (#1017) shipped. The fix: also stamp the OWNER's row
    /// `seat_purchased_at = now()`, so `purchased == used == 1` (no free
    /// slot) and the new member's own attempt has nothing to consume,
    /// genuinely reaching Stripe both times. See the new sibling test below,
    /// [`a_created_redelivery_consumes_a_free_slot_without_stripe`], for the
    /// free-slot case this fixture was accidentally covering instead.
    #[tokio::test]
    async fn a_created_redelivery_retries_a_previously_failed_seat_acquisition() {
        let Some(pool) =
            pg_test_pool("a_created_redelivery_retries_a_previously_failed_seat_acquisition").await
        else {
            return;
        };
        let owner = Uuid::new_v4();
        sqlx::query("INSERT INTO users (id, email) VALUES ($1, $2)")
            .bind(owner)
            .bind(format!("clerk-provision-owner-{}@test.example", owner.simple()))
            .execute(&pool)
            .await
            .expect("insert owner user");
        let clerk_org_id = format!("org_clerk_provision_{}", Uuid::new_v4().simple());
        let account_id: Uuid = sqlx::query_scalar(
            "INSERT INTO accounts (owner_user_id, clerk_org_id) VALUES ($1, $2) RETURNING id",
        )
        .bind(owner)
        .bind(&clerk_org_id)
        .fetch_one(&pool)
        .await
        .expect("insert org account");
        let standard_id: Uuid =
            sqlx::query_scalar("SELECT id FROM seat_tiers WHERE code = 'standard' AND is_active = true")
                .fetch_one(&pool)
                .await
                .expect("standard tier must be seeded (0165)");
        // CS-442 fixup (see this test's own "Fixture fix" doc section): the
        // owner's OWN slot is both purchased AND consumed (`seat_purchased_at`
        // set here) so `purchased == used == 1` below — no free slot for the
        // new member's own attempts to silently consume instead of reaching
        // Stripe.
        sqlx::query(
            "INSERT INTO account_members (account_id, user_id, role, seat_tier_id, seat_purchased_at) \
             VALUES ($1, $2, 'owner', $3, now())",
        )
        .bind(account_id)
        .bind(owner)
        .bind(standard_id)
        .execute(&pool)
        .await
        .expect("insert owner membership");
        let plan_id: Uuid = sqlx::query_scalar("SELECT id FROM plans WHERE code = $1")
            .bind(crate::billing::MIXED_TIER_PLAN_CODE)
            .fetch_one(&pool)
            .await
            .expect("team plan must be seeded (0167)");
        let grant_id: Uuid = fetch_financial_fixture_id(
            &pool,
            account_id,
            sqlx::query_scalar(
            "INSERT INTO grants (account_id, plan_id, seats, plan_is_seat_priced) \
             VALUES ($1, $2, 1, true) RETURNING id",
        )
        .bind(account_id)
        .bind(plan_id),
        )
        .await
        .expect("insert team grant");
        execute_financial_fixture(&pool, account_id, sqlx::query("INSERT INTO grant_seat_tiers (grant_id, seat_tier_id, seats) VALUES ($1, $2, 1)")
            .bind(grant_id)
            .bind(standard_id))
            .await
            .expect("seed the owner's own purchased AND consumed ledger row — purchased == used, no free slot");
        let _catalog_guard = CATALOG_PRICE_LOCK.lock().await;
        let price_id = format!("price_clerk_provision_retry_{}", Uuid::new_v4().simple());
        let original_standard = set_seat_tier_price(&pool, "standard", Some(&price_id)).await;
        execute_financial_fixture(&pool, account_id, sqlx::query(
            "INSERT INTO stripe_subscriptions (account_id, stripe_customer_id, stripe_subscription_id, status) \
             VALUES ($1, $2, $3, 'active')",
        )
        .bind(account_id)
        .bind(format!("cus_{}", Uuid::new_v4().simple()))
        .bind(format!("sub_{}", Uuid::new_v4().simple())))
        .await
        .expect("link stripe subscription");

        let new_member = Uuid::new_v4();
        sqlx::query("INSERT INTO users (id, email, clerk_user_id) VALUES ($1, $2, $3)")
            .bind(new_member)
            .bind(format!("clerk-provision-new-{}@test.example", new_member.simple()))
            .bind(format!("clerk_user_{}", new_member.simple()))
            .execute(&pool)
            .await
            .expect("insert new member user");

        // First delivery: `organizationMembership.created`, but Stripe
        // fails — the membership row still commits (it happens before
        // `acquire_seat` is ever called), and the error must propagate
        // (this function's own doc comment: Clerk's redelivery loop is the
        // retry mechanism for a webhook-driven join).
        let (failing_stripe, _hits) = spawn_failing_stripe_mock().await;
        let state = test_state(failing_stripe);
        let first = ensure_org_membership(&state, &pool, &clerk_org_id, new_member, "member", true, None, None).await;
        let first_is_err = first.is_err();

        let seat_purchased_at_after_first: Option<chrono::DateTime<chrono::Utc>> = sqlx::query_scalar(
            "SELECT seat_purchased_at FROM account_members WHERE account_id = $1 AND user_id = $2",
        )
        .bind(account_id)
        .bind(new_member)
        .fetch_one(&pool)
        .await
        .expect("read member row after the failed first attempt");

        // Second delivery: Clerk redelivers the SAME `organizationMembership.created`
        // event — `is_created_event` stays `true` — against a working Stripe
        // mock. `newly_inserted` is `false` this time (the row already
        // exists); `is_created_event && seat_purchased_at.is_none()` is what
        // must carry the retry. `hits` (CS-442 fixup) proves Stripe was
        // ACTUALLY reached this time, not merely that the call returned
        // `Ok` — with no free slot (see this test's own "Fixture fix" doc
        // section), `Ok` with zero hits would mean consume-before-buy wrongly
        // fired instead of retrying the purchase.
        let (working_stripe, hits) = spawn_seat_mock(&price_id, 1).await;
        let state = test_state(working_stripe);
        let second = ensure_org_membership(&state, &pool, &clerk_org_id, new_member, "member", true, None, None).await;
        let second_is_ok = second.is_ok();
        let stripe_hits = hits.load(std::sync::atomic::Ordering::SeqCst);

        let seat_purchased_at_after_second: Option<chrono::DateTime<chrono::Utc>> = sqlx::query_scalar(
            "SELECT seat_purchased_at FROM account_members WHERE account_id = $1 AND user_id = $2",
        )
        .bind(account_id)
        .bind(new_member)
        .fetch_one(&pool)
        .await
        .expect("read member row after the redelivered attempt");

        // Capture → restore → cleanup → assert (billing self-serve PR F
        // review round 2, minor finding): every value the assertions below
        // need is already captured in a local above, so the shared
        // `seat_tiers` row and this test's own account rows are restored /
        // deleted BEFORE any assertion — including a failed one's `panic!`
        // — can skip teardown.
        set_seat_tier_price(&pool, "standard", original_standard.as_deref()).await;
        cleanup(&pool, account_id, &[owner, new_member]).await;

        assert!(first_is_err, "the first attempt's Stripe failure must propagate (5xx -> Clerk redelivers)");
        assert_eq!(
            seat_purchased_at_after_first, None,
            "the failed first attempt must not have purchased the seat"
        );
        assert!(second_is_ok, "the redelivered .created event must retry and succeed at purchasing the seat");
        assert!(
            seat_purchased_at_after_second.is_some(),
            "the redelivered .created event must have purchased the seat this time"
        );
        assert!(
            stripe_hits > 0,
            "the redelivery must have ACTUALLY reached Stripe (no free slot to silently consume instead) — \
             got {stripe_hits} hits"
        );
    }

    /// The free-slot case [`a_created_redelivery_retries_a_previously_failed_seat_acquisition`]'s
    /// OLD fixture was accidentally exercising instead of the retry-gate
    /// behaviour it was named for (see that test's own "Fixture fix" doc
    /// section) — restored here as its own, correctly-named, correctly-
    /// asserted test: D1/CS-435's consume-before-buy means a FRESH `.created`
    /// delivery with a real free slot already sitting in the pool
    /// (`purchased == 1, used == 0`) stamps `seat_purchased_at` straight out
    /// of that slot, with ZERO Stripe calls — `billing::seat_lifecycle::
    /// acquire_seat`'s own consume-before-buy branch, reached through
    /// `ensure_org_membership`'s webhook path exactly as CS-442's own
    /// `billing::slot_pool` module doc frames it. Uses
    /// [`spawn_failing_stripe_mock`] (not merely "no mock configured") so a
    /// wrongly-reached Stripe call would fail loudly rather than this test
    /// silently passing for the wrong reason.
    #[tokio::test]
    async fn a_created_redelivery_consumes_a_free_slot_without_stripe() {
        let Some(pool) = pg_test_pool("a_created_redelivery_consumes_a_free_slot_without_stripe").await else {
            return;
        };
        let owner = Uuid::new_v4();
        sqlx::query("INSERT INTO users (id, email) VALUES ($1, $2)")
            .bind(owner)
            .bind(format!("clerk-provision-freeslot-owner-{}@test.example", owner.simple()))
            .execute(&pool)
            .await
            .expect("insert owner user");
        let clerk_org_id = format!("org_clerk_provision_freeslot_{}", Uuid::new_v4().simple());
        let account_id: Uuid = sqlx::query_scalar(
            "INSERT INTO accounts (owner_user_id, clerk_org_id) VALUES ($1, $2) RETURNING id",
        )
        .bind(owner)
        .bind(&clerk_org_id)
        .fetch_one(&pool)
        .await
        .expect("insert org account");
        let standard_id: Uuid =
            sqlx::query_scalar("SELECT id FROM seat_tiers WHERE code = 'standard' AND is_active = true")
                .fetch_one(&pool)
                .await
                .expect("standard tier must be seeded (0165)");
        // The owner holds a role but NO purchased seat — `used = 0`.
        sqlx::query(
            "INSERT INTO account_members (account_id, user_id, role, seat_tier_id) VALUES ($1, $2, 'owner', $3)",
        )
        .bind(account_id)
        .bind(owner)
        .bind(standard_id)
        .execute(&pool)
        .await
        .expect("insert owner membership");
        let plan_id: Uuid = sqlx::query_scalar("SELECT id FROM plans WHERE code = $1")
            .bind(crate::billing::MIXED_TIER_PLAN_CODE)
            .fetch_one(&pool)
            .await
            .expect("team plan must be seeded (0167)");
        let grant_id: Uuid = fetch_financial_fixture_id(
            &pool,
            account_id,
            sqlx::query_scalar(
            "INSERT INTO grants (account_id, plan_id, seats, plan_is_seat_priced) \
             VALUES ($1, $2, 1, true) RETURNING id",
        )
        .bind(account_id)
        .bind(plan_id),
        )
        .await
        .expect("insert team grant");
        // ONE purchased slot, nobody consuming it yet — `purchased = 1`.
        execute_financial_fixture(
            &pool,
            account_id,
            sqlx::query("INSERT INTO grant_seat_tiers (grant_id, seat_tier_id, seats) VALUES ($1, $2, 1)")
            .bind(grant_id)
            .bind(standard_id),
        )
            .await
            .expect("seed one purchased, unconsumed slot");
        execute_financial_fixture(&pool, account_id, sqlx::query(
            "INSERT INTO stripe_subscriptions (account_id, stripe_customer_id, stripe_subscription_id, status) \
             VALUES ($1, $2, $3, 'active')",
        )
        .bind(account_id)
        .bind(format!("cus_{}", Uuid::new_v4().simple()))
        .bind(format!("sub_{}", Uuid::new_v4().simple())))
        .await
        .expect("link stripe subscription");

        let new_member = Uuid::new_v4();
        sqlx::query("INSERT INTO users (id, email, clerk_user_id) VALUES ($1, $2, $3)")
            .bind(new_member)
            .bind(format!("clerk-provision-freeslot-new-{}@test.example", new_member.simple()))
            .bind(format!("clerk_user_{}", new_member.simple()))
            .execute(&pool)
            .await
            .expect("insert new member user");

        // A single `organizationMembership.created` delivery — no prior
        // failure, no redelivery needed: the free slot is consumed on the
        // very first attempt.
        let (failing_stripe, hits) = spawn_failing_stripe_mock().await;
        let state = test_state(failing_stripe);
        let result = ensure_org_membership(&state, &pool, &clerk_org_id, new_member, "member", true, None, None).await;
        let is_ok = result.is_ok();
        let stripe_hits = hits.load(std::sync::atomic::Ordering::SeqCst);

        let seat_purchased_at: Option<chrono::DateTime<chrono::Utc>> = sqlx::query_scalar(
            "SELECT seat_purchased_at FROM account_members WHERE account_id = $1 AND user_id = $2",
        )
        .bind(account_id)
        .bind(new_member)
        .fetch_one(&pool)
        .await
        .expect("read member row");

        cleanup(&pool, account_id, &[owner, new_member]).await;

        assert!(is_ok, "consuming a free slot must succeed even though the only configured Stripe mock fails everything");
        assert!(seat_purchased_at.is_some(), "the free slot must have been stamped as purchased");
        assert_eq!(stripe_hits, 0, "consuming a free slot must never call Stripe at all — got {stripe_hits} hits");
    }

    // ── PR 829 review, finding 1(b) — `ensure_org_membership` and personal
    // orgs ─────────────────────────────────────────────────────────────────

    /// A personal org's own account (control migration 0208 `personal_orgs`)
    /// — `clerk_org_id` set, mapped in `personal_orgs`, no `account_members`
    /// row yet. Returns `(account_id, owner_user_id, clerk_org_id)`.
    async fn seed_personal_org_account(pool: &PgPool) -> (Uuid, Uuid, String) {
        let owner = Uuid::new_v4();
        let clerk_user_id = format!("clerk_user_{}", owner.simple());
        sqlx::query("INSERT INTO users (id, email, clerk_user_id) VALUES ($1, $2, $3)")
            .bind(owner)
            .bind(format!("clerk-provision-personal-org-owner-{}@test.example", owner.simple()))
            .bind(&clerk_user_id)
            .execute(pool)
            .await
            .expect("insert owner user");
        let clerk_org_id = format!("org_clerk_provision_personal_{}", Uuid::new_v4().simple());
        let account_id: Uuid = sqlx::query_scalar(
            "INSERT INTO accounts (owner_user_id, clerk_org_id) VALUES ($1, $2) RETURNING id",
        )
        .bind(owner)
        .bind(&clerk_org_id)
        .fetch_one(pool)
        .await
        .expect("insert personal-org account");
        sqlx::query("INSERT INTO personal_orgs (clerk_org_id, clerk_user_id) VALUES ($1, $2)")
            .bind(&clerk_org_id)
            .bind(&clerk_user_id)
            .execute(pool)
            .await
            .expect("insert personal_orgs row");
        (account_id, owner, clerk_org_id)
    }

    async fn cleanup_personal_org(pool: &PgPool, account_id: Uuid, users: &[Uuid], clerk_org_id: &str) {
        cleanup(pool, account_id, users).await;
        let _ = sqlx::query("DELETE FROM personal_orgs WHERE clerk_org_id = $1")
            .bind(clerk_org_id)
            .execute(pool)
            .await;
    }

    /// **The bootstrap case finding 1(b)'s fix must NOT break.** A brand-new
    /// personal org's account has NO `account_members` row yet — the exact
    /// shape `bin/create_personal_orgs.rs`'s own "ordering hazard" doc
    /// comment documents: its step 5 (`INSERT INTO personal_orgs`) is a
    /// plain local write that typically completes BEFORE Clerk's Svix
    /// delivery of the owner's own `organizationMembership.created` webhook
    /// for the brand-new org. If the fix refused an INSERT for `clerk_org_id
    /// IN personal_orgs` unconditionally, this — the OWNER'S OWN first
    /// membership — would be refused too, and
    /// `bin/adopt_personal_org_account.rs` would find no seat tier to copy
    /// and skip that user forever.
    #[tokio::test]
    async fn ensure_org_membership_still_inserts_the_owners_own_first_membership_on_a_brand_new_personal_org(
    ) {
        let Some(pool) = pg_test_pool(
            "ensure_org_membership_still_inserts_the_owners_own_first_membership_on_a_brand_new_personal_org",
        )
        .await
        else {
            return;
        };
        let (account_id, owner, clerk_org_id) = seed_personal_org_account(&pool).await;
        let (stripe, _hits) = spawn_failing_stripe_mock().await;
        let state = test_state(stripe);

        let result = ensure_org_membership(&state, &pool, &clerk_org_id, owner, "owner", true, None, None).await;

        let row: Option<(String,)> =
            sqlx::query_as("SELECT role FROM account_members WHERE account_id = $1 AND user_id = $2")
                .bind(account_id)
                .bind(owner)
                .fetch_optional(&pool)
                .await
                .expect("read back the membership row");

        cleanup_personal_org(&pool, account_id, &[owner], &clerk_org_id).await;

        result.expect("the personal org owner's own first membership must still be inserted");
        assert_eq!(
            row.map(|(r,)| r),
            Some("owner".to_string()),
            "the owner's account_members row must exist with the role this call supplied"
        );
    }

    /// **The fix itself.** A personal org's account that ALREADY has its one
    /// legitimate member (the owner, mirroring the post-adoption steady
    /// state) must never gain a second `account_members` row through this
    /// function — `resolve_reachable_graphs` would return every
    /// `kind='personal'` ocean in that account to BOTH rows, with no further
    /// filter (ADR-0007 D2).
    #[tokio::test]
    async fn ensure_org_membership_never_attaches_a_second_member_to_a_personal_org() {
        let Some(pool) =
            pg_test_pool("ensure_org_membership_never_attaches_a_second_member_to_a_personal_org").await
        else {
            return;
        };
        let (account_id, owner, clerk_org_id) = seed_personal_org_account(&pool).await;
        let standard_tier_id: Uuid = sqlx::query_scalar("SELECT id FROM seat_tiers WHERE code = 'standard' AND is_active = true")
            .fetch_one(&pool)
            .await
            .expect("standard seat tier must be seeded (0165)");
        sqlx::query(
            "INSERT INTO account_members (account_id, user_id, role, seat_tier_id) VALUES ($1, $2, 'owner', $3)",
        )
        .bind(account_id)
        .bind(owner)
        .bind(standard_tier_id)
        .execute(&pool)
        .await
        .expect("insert the owner's own account_members row (post-adoption steady state)");

        let second_member = Uuid::new_v4();
        sqlx::query("INSERT INTO users (id, email) VALUES ($1, $2)")
            .bind(second_member)
            .bind(format!("clerk-provision-second-member-{}@test.example", second_member.simple()))
            .execute(&pool)
            .await
            .expect("insert second member user");
        let (stripe, hits) = spawn_failing_stripe_mock().await;
        let state = test_state(stripe);

        let result =
            ensure_org_membership(&state, &pool, &clerk_org_id, second_member, "member", true, None, None).await;

        let second_member_row_exists: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM account_members WHERE account_id = $1 AND user_id = $2)",
        )
        .bind(account_id)
        .bind(second_member)
        .fetch_one(&pool)
        .await
        .expect("check for the second member's row");
        let member_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM account_members WHERE account_id = $1")
            .bind(account_id)
            .fetch_one(&pool)
            .await
            .expect("count account_members rows");

        cleanup_personal_org(&pool, account_id, &[owner, second_member], &clerk_org_id).await;

        result.expect("a refused insert must still return Ok — a webhook must never 5xx over this");
        assert!(
            !second_member_row_exists,
            "a personal org must never gain a second account_members row through ensure_org_membership"
        );
        assert_eq!(member_count, 1, "the personal org's account must still carry exactly one member");
        assert_eq!(
            hits.load(std::sync::atomic::Ordering::SeqCst),
            0,
            "a refused membership insert must not attempt a Stripe call either"
        );
    }

    /// CS-442 (item 40, D8): the `seat_tier_id` parameter itself — a fresh
    /// insert lands on the PASSED tier when one is given, still resolves
    /// `standard` when `None` (the webhook's own unaffected default,
    /// `routes::clerk_webhook`/`routes::org_invites::create_invite`'s own
    /// shape), and an explicit `Some(id)` passed for an ALREADY-EXISTING
    /// member is silently ignored — that member's tier is untouched (see
    /// this function's own "Seat tier" doc section: moving an existing
    /// member's tier is `routes::billing_seats::change_seat_tier`'s job,
    /// never a side effect of a role sync). Ignores every `Result` these
    /// calls return — `acquire_seat`'s own downstream Stripe/price
    /// resolution is exercised elsewhere in this module; this test's ONLY
    /// concern is the `account_members.seat_tier_id` COLUMN, which the
    /// membership transaction commits before `acquire_seat` ever runs
    /// (this function's own doc comment).
    #[tokio::test]
    async fn ensure_org_membership_seat_tier_id_only_applies_to_a_fresh_insert() {
        let Some(pool) =
            pg_test_pool("ensure_org_membership_seat_tier_id_only_applies_to_a_fresh_insert").await
        else {
            return;
        };
        let (account_id, owner, existing_member, clerk_org_id, _grant_id) =
            seed_team_org_with_member(&pool, "standard", false).await;
        let standard_id: Uuid =
            sqlx::query_scalar("SELECT id FROM seat_tiers WHERE code = 'standard' AND is_active = true")
                .fetch_one(&pool)
                .await
                .expect("standard seat tier must be seeded (0165)");
        let premium_id: Uuid =
            sqlx::query_scalar("SELECT id FROM seat_tiers WHERE code = 'premium' AND is_active = true")
                .fetch_one(&pool)
                .await
                .expect("premium seat tier must be seeded (0165)");

        let fresh_with_tier = Uuid::new_v4();
        sqlx::query("INSERT INTO users (id, email, clerk_user_id) VALUES ($1, $2, $3)")
            .bind(fresh_with_tier)
            .bind(format!("clerk-provision-fresh-tier-{}@test.example", fresh_with_tier.simple()))
            .bind(format!("clerk_user_{}", fresh_with_tier.simple()))
            .execute(&pool)
            .await
            .expect("insert fresh user (chosen tier)");
        let fresh_default = Uuid::new_v4();
        sqlx::query("INSERT INTO users (id, email, clerk_user_id) VALUES ($1, $2, $3)")
            .bind(fresh_default)
            .bind(format!("clerk-provision-fresh-default-{}@test.example", fresh_default.simple()))
            .bind(format!("clerk_user_{}", fresh_default.simple()))
            .execute(&pool)
            .await
            .expect("insert fresh user (default tier)");

        let (stripe, _hits) = spawn_failing_stripe_mock().await;
        let state = test_state(stripe);

        // Fresh insert, an explicit chosen tier — `routes::org_join_requests::
        // approve`'s own shape (via `mirror_local_membership`).
        let _ =
            ensure_org_membership(&state, &pool, &clerk_org_id, fresh_with_tier, "member", true, Some(premium_id), None)
                .await;
        // Fresh insert, `None` — the webhook's/`create_invite`'s own
        // unaffected default.
        let _ = ensure_org_membership(&state, &pool, &clerk_org_id, fresh_default, "member", true, None, None).await;
        // An EXISTING member (role-only sync, `is_created_event = false`,
        // matching a genuine `.updated` delivery — this also keeps the
        // retry gate from calling `acquire_seat` at all, since
        // `newly_inserted` is false and `is_created_event` is false): an
        // explicit chosen tier must be IGNORED.
        let _ = ensure_org_membership(&state, &pool, &clerk_org_id, existing_member, "member", false, Some(premium_id), None)
            .await;

        let fresh_with_tier_id: Uuid = sqlx::query_scalar(
            "SELECT seat_tier_id FROM account_members WHERE account_id = $1 AND user_id = $2",
        )
        .bind(account_id)
        .bind(fresh_with_tier)
        .fetch_one(&pool)
        .await
        .expect("fresh_with_tier row must exist");
        let fresh_default_id: Uuid = sqlx::query_scalar(
            "SELECT seat_tier_id FROM account_members WHERE account_id = $1 AND user_id = $2",
        )
        .bind(account_id)
        .bind(fresh_default)
        .fetch_one(&pool)
        .await
        .expect("fresh_default row must exist");
        let existing_member_id: Uuid = sqlx::query_scalar(
            "SELECT seat_tier_id FROM account_members WHERE account_id = $1 AND user_id = $2",
        )
        .bind(account_id)
        .bind(existing_member)
        .fetch_one(&pool)
        .await
        .expect("existing_member row must exist");

        cleanup(&pool, account_id, &[owner, existing_member, fresh_with_tier, fresh_default]).await;

        assert_eq!(fresh_with_tier_id, premium_id, "a fresh insert must land on the PASSED tier");
        assert_eq!(fresh_default_id, standard_id, "None must still resolve standard, unaffected by this PR");
        assert_eq!(
            existing_member_id, standard_id,
            "an EXISTING member's tier must never be touched by a passed seat_tier_id"
        );
    }
}

// ── Live-Postgres tests (CS-430, org-onboarding-from-the-invite plan, D12,
// item 33) — the tombstone's `email` column and `is_org_membership_
// tombstoned`'s two-key lookup ──────────────────────────────────────────────
//
// A SEPARATE `ORGT_TEST_BASE_URL`-gated module (a scratch database per test,
// recreated) rather than an addition to `pg_tests` above: this file's own
// `pg_tests` predates the `ORGT_TEST_BASE_URL` convention and uses
// `RELINFER_TEST_DATABASE_URL` against a persistent, pre-seeded shared
// database instead. CS-430's suite needs neither Stripe nor a shared
// `seat_tiers` catalog beyond what a fresh `migrations_control` run already
// seeds, so a disposable per-test database is the simpler, fully
// self-contained and parallel-safe choice — the same convention
// `routes::org_invites::pg_tests` and `seats::history_from_grant_population_
// pg_tests` both use, and the one this story's own build note names.
//
// ## Running locally
//   ORGT_TEST_BASE_URL=postgresql://alex@localhost:5432/postgres \
//     cargo test -p coralswarm-backend --lib "clerk_provision::tombstone_email_pg_tests::" -- --test-threads=1 --nocapture
#[cfg(test)]
mod tombstone_email_pg_tests {
    use super::*;
    use sqlx::postgres::PgPoolOptions;
    use sqlx::{Connection, Executor, PgConnection, PgPool};

    fn base_url() -> Option<String> {
        std::env::var("ORGT_TEST_BASE_URL").ok()
    }
    fn with_db(base: &str, db: &str) -> String {
        let (prefix, rest) = base.split_at(base.rfind('/').expect("url has a path /"));
        let params = rest.find('?').map(|i| &rest[i..]).unwrap_or("");
        format!("{prefix}/{db}{params}")
    }
    async fn admin_conn(base: &str) -> PgConnection {
        PgConnection::connect(base).await.expect("connect admin")
    }
    async fn recreate(conn: &mut PgConnection, db: &str) {
        conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .unwrap();
        conn.execute(format!("CREATE DATABASE {db}").as_str())
            .await
            .unwrap();
    }
    async fn control_pool(base: &str, db: &str) -> PgPool {
        let pool = PgPoolOptions::new()
            .max_connections(4)
            .connect(&with_db(base, db))
            .await
            .unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();
        pool
    }
    async fn teardown(conn: &mut PgConnection, pool: PgPool, db: &str) {
        pool.close().await;
        conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .ok();
    }

    /// A `standard`-tier org account with ONE admin/owner (never removed —
    /// exists only to satisfy `accounts.owner_user_id NOT NULL` and give the
    /// account a non-empty roster) and no other members. Returns `account_id`.
    /// `account_members.seat_tier_id` resolved inline the same way
    /// `routes::org_invites::pg_tests::seed_org` does — 0165 seeds `standard`
    /// on every fresh `migrations_control` run, so this needs no separate
    /// lookup step.
    async fn seed_org_account(pool: &PgPool, clerk_org_id: &str) -> Uuid {
        let owner = Uuid::new_v4();
        sqlx::query("INSERT INTO users (id, email) VALUES ($1, $2)")
            .bind(owner)
            .bind(format!("owner-{}@tombstone-pg.test", owner.simple()))
            .execute(pool)
            .await
            .expect("insert owner user");
        let account_id: Uuid =
            sqlx::query_scalar("INSERT INTO accounts (owner_user_id, clerk_org_id) VALUES ($1, $2) RETURNING id")
                .bind(owner)
                .bind(clerk_org_id)
                .fetch_one(pool)
                .await
                .expect("insert org account");
        sqlx::query(
            "INSERT INTO account_members (account_id, user_id, role, seat_tier_id) \
             VALUES ($1, $2, 'owner', (SELECT id FROM seat_tiers WHERE code = 'standard' AND is_active = true))",
        )
        .bind(account_id)
        .bind(owner)
        .execute(pool)
        .await
        .expect("insert owner membership");
        account_id
    }

    /// A member of `account_id` with a real `clerk_user_id` (so
    /// `remove_org_membership` — which looks the internal user up BY
    /// `clerk_user_id`, exactly as the Clerk webhook does — can find it) and
    /// the given (UN-normalized, on purpose — callers pass whatever case/
    /// whitespace they want to prove gets normalized away) email. Returns
    /// `(user_id, clerk_user_id)`.
    async fn seed_member(pool: &PgPool, account_id: Uuid, email: &str) -> (Uuid, String) {
        let user_id = Uuid::new_v4();
        let clerk_user_id = format!("clerk_user_{}", user_id.simple());
        sqlx::query("INSERT INTO users (id, email, clerk_user_id) VALUES ($1, $2, $3)")
            .bind(user_id)
            .bind(email)
            .bind(&clerk_user_id)
            .execute(pool)
            .await
            .expect("insert member user");
        sqlx::query(
            "INSERT INTO account_members (account_id, user_id, role, seat_tier_id) \
             VALUES ($1, $2, 'member', (SELECT id FROM seat_tiers WHERE code = 'standard' AND is_active = true))",
        )
        .bind(account_id)
        .bind(user_id)
        .execute(pool)
        .await
        .expect("insert member membership");
        (user_id, clerk_user_id)
    }

    async fn tombstone_email(pool: &PgPool, clerk_org_id: &str, user_id: Uuid) -> Option<String> {
        sqlx::query_scalar("SELECT email FROM org_membership_tombstones WHERE clerk_org_id = $1 AND user_id = $2")
            .bind(clerk_org_id)
            .bind(user_id)
            .fetch_one(pool)
            .await
            .expect("tombstone row must exist after removal")
    }

    fn test_state() -> AppState {
        // `stripe: None` is fine for every test in this module — twice over
        // as of CS-444. Every member seeded here has `seat_purchased_at =
        // NULL` (never purchased), which `billing::seat_lifecycle::
        // release_seat` short-circuits on its own early return; and a
        // release makes no Stripe call for ANY member or subscription state
        // now, since it no longer takes a `StripeClient` at all.
        crate::test_app_state_owned(None)
    }

    /// **Removal writes the address.** `remove_org_membership` reads
    /// `users.email` and writes it to the tombstone NORMALIZED (trim +
    /// lowercase) — proven here with an input that is neither trimmed nor
    /// lowercased, so a naive "copy the column verbatim" implementation
    /// would fail this assertion.
    #[tokio::test]
    async fn removal_writes_the_normalized_address_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping removal_writes_the_normalized_address_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "orgt_cs430_write_normalized";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mut conn = admin_conn(&base).await;
        recreate(&mut conn, db).await;
        let pool = control_pool(&base, db).await;

        let clerk_org_id = format!("org_cs430_{}", Uuid::new_v4().simple());
        let account_id = seed_org_account(&pool, &clerk_org_id).await;
        let (member, clerk_user_id) = seed_member(&pool, account_id, "  Removed.Person@Example.COM  ").await;
        let state = test_state();

        remove_org_membership(&state, &pool, &clerk_org_id, &clerk_user_id)
            .await
            .expect("removal must succeed");

        let stored = tombstone_email(&pool, &clerk_org_id, member).await;
        let _ = account_id;
        teardown(&mut conn, pool, db).await;

        assert_eq!(
            stored.as_deref(),
            Some("removed.person@example.com"),
            "the stored address must be trimmed and lowercased, matching every other users.email \
             comparison in this codebase (connectors::slack::normalize_email's rule)"
        );
    }

    /// **The lookup answers true by `user_id`** — the original 0081/0082 key,
    /// unchanged by this migration.
    #[tokio::test]
    async fn lookup_matches_by_user_id_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping lookup_matches_by_user_id_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "orgt_cs430_by_user_id";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mut conn = admin_conn(&base).await;
        recreate(&mut conn, db).await;
        let pool = control_pool(&base, db).await;

        let clerk_org_id = format!("org_cs430_{}", Uuid::new_v4().simple());
        let account_id = seed_org_account(&pool, &clerk_org_id).await;
        let (member, clerk_user_id) = seed_member(&pool, account_id, "person@example.com").await;
        let state = test_state();
        remove_org_membership(&state, &pool, &clerk_org_id, &clerk_user_id)
            .await
            .expect("removal must succeed");

        let by_user_id = is_org_membership_tombstoned(&pool, &clerk_org_id, Some(member), None)
            .await
            .expect("lookup must not error");
        let stranger_by_user_id =
            is_org_membership_tombstoned(&pool, &clerk_org_id, Some(Uuid::new_v4()), None)
                .await
                .expect("lookup must not error");

        teardown(&mut conn, pool, db).await;

        assert!(by_user_id, "the removed member's own user_id must match");
        assert!(!stranger_by_user_id, "an unrelated user_id must not match");
    }

    /// **The lookup answers true by email, case/whitespace-normalised.** The
    /// caller passes a MESSY address (different case, leading/trailing
    /// whitespace) and it must still match the tombstone's normalized column
    /// — proving the reader normalizes its OWN input rather than requiring
    /// callers to pre-normalize (D12/item 33's whole point: an invite path
    /// holds a raw address, not a pre-cleaned one).
    #[tokio::test]
    async fn lookup_matches_by_normalized_email_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping lookup_matches_by_normalized_email_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "orgt_cs430_by_email";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mut conn = admin_conn(&base).await;
        recreate(&mut conn, db).await;
        let pool = control_pool(&base, db).await;

        let clerk_org_id = format!("org_cs430_{}", Uuid::new_v4().simple());
        let account_id = seed_org_account(&pool, &clerk_org_id).await;
        let (_member, clerk_user_id) = seed_member(&pool, account_id, "Dana.Departed@Acme.com").await;
        let state = test_state();
        remove_org_membership(&state, &pool, &clerk_org_id, &clerk_user_id)
            .await
            .expect("removal must succeed");

        let messy_match = is_org_membership_tombstoned(
            &pool,
            &clerk_org_id,
            None,
            Some("  DANA.DEPARTED@ACME.COM  "),
        )
        .await
        .expect("lookup must not error");
        let different_address =
            is_org_membership_tombstoned(&pool, &clerk_org_id, None, Some("someone.else@acme.com"))
                .await
                .expect("lookup must not error");

        teardown(&mut conn, pool, db).await;

        assert!(
            messy_match,
            "a case/whitespace-different rendering of the SAME address must still match — the \
             reader normalizes its own input"
        );
        assert!(!different_address, "a genuinely different address must not match");
    }

    /// **False for a different org — tombstones are per org, never
    /// cross-org.** The SAME person (same `user_id`, same address) removed
    /// from org A must be a stranger to org B's tombstone.
    #[tokio::test]
    async fn lookup_is_false_for_a_different_org_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping lookup_is_false_for_a_different_org_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "orgt_cs430_cross_org";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mut conn = admin_conn(&base).await;
        recreate(&mut conn, db).await;
        let pool = control_pool(&base, db).await;

        let org_a = format!("org_cs430_a_{}", Uuid::new_v4().simple());
        let org_b = format!("org_cs430_b_{}", Uuid::new_v4().simple());
        let account_a = seed_org_account(&pool, &org_a).await;
        // `org_b` needs its own roster too, so the member being checked
        // against it is a genuine stranger rather than an org with no rows
        // at all (a weaker proof — an empty table always answers false).
        let _account_b = seed_org_account(&pool, &org_b).await;
        let (member, clerk_user_id) = seed_member(&pool, account_a, "cross.org@example.com").await;
        let state = test_state();
        remove_org_membership(&state, &pool, &org_a, &clerk_user_id)
            .await
            .expect("removal from org A must succeed");

        let tombstoned_in_a =
            is_org_membership_tombstoned(&pool, &org_a, Some(member), Some("cross.org@example.com"))
                .await
                .expect("lookup must not error");
        let tombstoned_in_b =
            is_org_membership_tombstoned(&pool, &org_b, Some(member), Some("cross.org@example.com"))
                .await
                .expect("lookup must not error");

        teardown(&mut conn, pool, db).await;

        assert!(tombstoned_in_a, "org A, which did the removing, must see the tombstone");
        assert!(
            !tombstoned_in_b,
            "org B must NOT see org A's removal — a person removed from Acme is a stranger to \
             Beta's tombstone on both keys"
        );
    }

    /// **A recreated account (new `user_id`, same email) is caught by the
    /// email key.** This is D12's own "Build note" residue, closed by item
    /// 33: Dana is removed, deletes her CoralSwarm account, signs up again
    /// with the SAME address and gets a BRAND NEW `user_id`. The user_id arm
    /// alone (the pre-CS-430 behaviour) would miss her entirely; the email
    /// arm catches her.
    #[tokio::test]
    async fn lookup_catches_a_recreated_account_by_email_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping lookup_catches_a_recreated_account_by_email_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "orgt_cs430_recreated";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mut conn = admin_conn(&base).await;
        recreate(&mut conn, db).await;
        let pool = control_pool(&base, db).await;

        let clerk_org_id = format!("org_cs430_{}", Uuid::new_v4().simple());
        let account_id = seed_org_account(&pool, &clerk_org_id).await;
        let (_original_user, clerk_user_id) =
            seed_member(&pool, account_id, "resurrected@example.com").await;
        let state = test_state();
        remove_org_membership(&state, &pool, &clerk_org_id, &clerk_user_id)
            .await
            .expect("removal must succeed");

        // The delete-and-recreate: a BRAND NEW user_id, never a member of
        // this org, never itself in `account_members` — the invite path's
        // exact situation (an address with, at most, a fresh signup behind
        // it).
        let new_user_id = Uuid::new_v4();

        let by_new_user_id_alone = is_org_membership_tombstoned(&pool, &clerk_org_id, Some(new_user_id), None)
            .await
            .expect("lookup must not error");
        let by_new_user_id_and_email = is_org_membership_tombstoned(
            &pool,
            &clerk_org_id,
            Some(new_user_id),
            Some("resurrected@example.com"),
        )
        .await
        .expect("lookup must not error");

        teardown(&mut conn, pool, db).await;

        assert!(
            !by_new_user_id_alone,
            "the NEW user_id alone must not match — this is genuinely a different account, and \
             this assertion is what proves the email arm (not a user_id coincidence) is doing the work below"
        );
        assert!(
            by_new_user_id_and_email,
            "the SAME address on a brand-new user_id must still be caught by the email key"
        );
    }

    /// **Documented residue, asserted as a limit, not read as coverage.** A
    /// member removed with NO address on file (an `email IS NULL` tombstone
    /// row — not reachable through `remove_org_membership`/
    /// `revoke_org_access` today, since `users.email` is `NOT NULL`, but
    /// simulated directly here the way a pre-migration-0226 row or some
    /// future email-less writer would produce one) who is later re-invited
    /// under ANY address is NOT caught by the email arm, because there is
    /// nothing in the row to compare against. D12's own text: "it does not
    /// close a removed person returning under a different address, which
    /// nothing can." This test exists to keep that sentence true and
    /// visible, not to demonstrate a passing feature.
    #[tokio::test]
    async fn lookup_documented_limit_no_address_on_file_is_not_caught_by_email_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping lookup_documented_limit_no_address_on_file_is_not_caught_by_email_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "orgt_cs430_residue";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mut conn = admin_conn(&base).await;
        recreate(&mut conn, db).await;
        let pool = control_pool(&base, db).await;

        let clerk_org_id = format!("org_cs430_{}", Uuid::new_v4().simple());
        let account_id = seed_org_account(&pool, &clerk_org_id).await;
        // A user who, at the moment of removal, had no address on file —
        // simulated by inserting the tombstone directly with `email = NULL`
        // rather than through `remove_org_membership` (which cannot produce
        // this today, `users.email` being `NOT NULL`).
        let removed_user = Uuid::new_v4();
        sqlx::query("INSERT INTO users (id, email) VALUES ($1, $2)")
            .bind(removed_user)
            .bind(format!("placeholder-{}@example.com", removed_user.simple()))
            .execute(&pool)
            .await
            .expect("insert placeholder user row (FK target for the tombstone)");
        sqlx::query(
            "INSERT INTO org_membership_tombstones (clerk_org_id, user_id, email) VALUES ($1, $2, NULL)",
        )
        .bind(&clerk_org_id)
        .bind(removed_user)
        .execute(&pool)
        .await
        .expect("insert a no-address tombstone row directly");

        // Re-invited later under some address never associated with the
        // removal — the exact case the email key cannot ever close.
        let caught = is_org_membership_tombstoned(
            &pool,
            &clerk_org_id,
            None,
            Some("a.new.address.nobody.tombstoned@example.com"),
        )
        .await
        .expect("lookup must not error");
        // The user_id key still works — this residue is specific to the
        // email arm, not a regression of the original key.
        let still_caught_by_user_id = is_org_membership_tombstoned(&pool, &clerk_org_id, Some(removed_user), None)
            .await
            .expect("lookup must not error");

        let _ = account_id;
        teardown(&mut conn, pool, db).await;

        assert!(
            !caught,
            "DOCUMENTED LIMIT (D12): a tombstone with no address on file cannot be matched by any \
             email an org is later asked to invite — this assertion proves the limit exists, it is \
             not evidence of coverage"
        );
        assert!(
            still_caught_by_user_id,
            "the user_id key must be unaffected by the missing address — the residue is specific \
             to the email arm"
        );
    }

    /// The whole-org path (`revoke_org_access`, `organization.deleted`) fans
    /// the SAME address capture out to every current member in one
    /// `INSERT ... SELECT` — proven separately from `remove_org_membership`
    /// because it is a different SQL statement (a JOIN to `users`, not a
    /// Rust-side read), with its own normalization to get right.
    #[tokio::test]
    async fn revoke_org_access_writes_addresses_for_every_member_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping revoke_org_access_writes_addresses_for_every_member_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "orgt_cs430_revoke_all";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mut conn = admin_conn(&base).await;
        recreate(&mut conn, db).await;
        let pool = control_pool(&base, db).await;

        let clerk_org_id = format!("org_cs430_{}", Uuid::new_v4().simple());
        let account_id = seed_org_account(&pool, &clerk_org_id).await;
        let (member_one, _) = seed_member(&pool, account_id, "  First.Member@Example.COM").await;
        let (member_two, _) = seed_member(&pool, account_id, "second.member@example.com").await;

        revoke_org_access(&pool, &clerk_org_id)
            .await
            .expect("whole-org revocation must succeed");

        let email_one = tombstone_email(&pool, &clerk_org_id, member_one).await;
        let email_two = tombstone_email(&pool, &clerk_org_id, member_two).await;
        let by_email_one = is_org_membership_tombstoned(&pool, &clerk_org_id, None, Some("first.member@example.com"))
            .await
            .expect("lookup must not error");

        teardown(&mut conn, pool, db).await;

        assert_eq!(email_one.as_deref(), Some("first.member@example.com"));
        assert_eq!(email_two.as_deref(), Some("second.member@example.com"));
        assert!(by_email_one, "the bulk-revocation write must be normalized the same way the lookup expects");
    }

    /// CS-428 (item 3): `invite_suppressed_by_tombstone` forwards to
    /// [`is_org_membership_tombstoned`] byte-for-byte — true for the
    /// removing org, false for a DIFFERENT org that never removed this
    /// person. This is the org-scoping property CS-428's own invite-pass
    /// call site (`workers::slack_unmatched_email::process_job`) leans on
    /// to let a sibling org's own job still invite the same address.
    #[tokio::test]
    async fn invite_suppressed_by_tombstone_is_scoped_to_the_removing_org_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping invite_suppressed_by_tombstone_is_scoped_to_the_removing_org_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "orgt_cs428_seam_org_scope";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mut conn = admin_conn(&base).await;
        recreate(&mut conn, db).await;
        let pool = control_pool(&base, db).await;

        let removing_org = format!("org_cs428_removing_{}", Uuid::new_v4().simple());
        let other_org = format!("org_cs428_other_{}", Uuid::new_v4().simple());
        let removing_account = seed_org_account(&pool, &removing_org).await;
        let _other_account = seed_org_account(&pool, &other_org).await;
        let (_member, clerk_user_id) = seed_member(&pool, removing_account, "dana@example.com").await;
        let state = test_state();
        remove_org_membership(&state, &pool, &removing_org, &clerk_user_id)
            .await
            .expect("removal must succeed");

        let suppressed_by_removing_org =
            invite_suppressed_by_tombstone(&pool, Some(removing_org.as_str()), None, Some("dana@example.com"))
                .await
                .expect("lookup must not error");
        let suppressed_by_other_org =
            invite_suppressed_by_tombstone(&pool, Some(other_org.as_str()), None, Some("dana@example.com"))
                .await
                .expect("lookup must not error");

        teardown(&mut conn, pool, db).await;

        assert!(suppressed_by_removing_org, "the org that removed Dana must suppress her invite");
        assert!(
            !suppressed_by_other_org,
            "a stranger org — one that never removed Dana — must NOT suppress her invite on the \
             strength of a DIFFERENT org's removal history"
        );
    }

    /// CS-428 (item 3): the invite pass's own call shape never holds a
    /// `user_id` (D12's build note: "at invite time we hold an email
    /// address, not a user_id") — so a member who is removed, then deletes
    /// and recreates their CoralSwarm account (a brand-new `user_id`, same
    /// address), must still be suppressed through the EMAIL key alone, with
    /// no `user_id` in the call at all. This exercises the seam with
    /// EXACTLY the argument shape `process_job` uses (`None` for `user_id`),
    /// not a redundant re-proof of `is_org_membership_tombstoned` itself
    /// (already covered by this module's own `lookup_catches_a_recreated_
    /// account_by_email_pg`, above).
    #[tokio::test]
    async fn invite_suppressed_by_tombstone_catches_a_recreated_account_by_email_alone_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping invite_suppressed_by_tombstone_catches_a_recreated_account_by_email_alone_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "orgt_cs428_seam_recreated";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mut conn = admin_conn(&base).await;
        recreate(&mut conn, db).await;
        let pool = control_pool(&base, db).await;

        let clerk_org_id = format!("org_cs428_{}", Uuid::new_v4().simple());
        let account_id = seed_org_account(&pool, &clerk_org_id).await;
        let (_original_user, clerk_user_id) = seed_member(&pool, account_id, "resurrected@example.com").await;
        let state = test_state();
        remove_org_membership(&state, &pool, &clerk_org_id, &clerk_user_id)
            .await
            .expect("removal must succeed");

        // The invite pass never has a `user_id` for this call — only the
        // address the recreated account happens to share with the removed
        // one.
        let suppressed =
            invite_suppressed_by_tombstone(&pool, Some(clerk_org_id.as_str()), None, Some("resurrected@example.com"))
                .await
                .expect("lookup must not error");

        teardown(&mut conn, pool, db).await;

        assert!(
            suppressed,
            "the invite pass's own email-only call shape must still catch a recreated account at \
             the SAME address, via the email key alone"
        );
    }

    /// CS-428 (item 3): the documented residue, restated at THIS seam's own
    /// call shape (email-only, `user_id: None`) — a tombstone with no
    /// address on file cannot suppress ANY later invite by email, because
    /// the invite pass never supplies a `user_id` for this residue's
    /// `user_id`-only row to match against. Asserted as the limit, not as
    /// coverage — same discipline this module's own `lookup_documented_
    /// limit_no_address_on_file_is_not_caught_by_email_pg` states.
    #[tokio::test]
    async fn invite_suppressed_by_tombstone_does_not_catch_the_no_address_residue_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping invite_suppressed_by_tombstone_does_not_catch_the_no_address_residue_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "orgt_cs428_seam_residue";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mut conn = admin_conn(&base).await;
        recreate(&mut conn, db).await;
        let pool = control_pool(&base, db).await;

        let clerk_org_id = format!("org_cs428_{}", Uuid::new_v4().simple());
        let removed_user = Uuid::new_v4();
        sqlx::query("INSERT INTO users (id, email) VALUES ($1, $2)")
            .bind(removed_user)
            .bind(format!("placeholder-{}@example.com", removed_user.simple()))
            .execute(&pool)
            .await
            .expect("insert placeholder user row (FK target for the tombstone)");
        sqlx::query(
            "INSERT INTO org_membership_tombstones (clerk_org_id, user_id, email) VALUES ($1, $2, NULL)",
        )
        .bind(&clerk_org_id)
        .bind(removed_user)
        .execute(&pool)
        .await
        .expect("insert a no-address tombstone row directly");

        let suppressed = invite_suppressed_by_tombstone(
            &pool,
            Some(clerk_org_id.as_str()),
            None,
            Some("a.new.address.nobody.tombstoned@example.com"),
        )
        .await
        .expect("lookup must not error");

        teardown(&mut conn, pool, db).await;

        assert!(
            !suppressed,
            "DOCUMENTED LIMIT (D12): a no-address tombstone cannot suppress an invite arriving by \
             email alone — this is the residue, not a defect"
        );
    }
}
