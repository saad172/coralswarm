//! Admin and internal route handlers.
//!
//! No public (Bearer-authed) admin endpoints live in this file. The former
//! `GET /v1/admin/users` — a `501 NOT_IMPLEMENTED` stub sitting on the
//! ordinary authenticated `/v1` router with **no role check at all** — was
//! deleted by the platform-admin-metrics feature (PR 6 of 7): admin/operator
//! surfaces now live exclusively on the internal-only router, behind
//! `X-Internal-Token`, never on a router a plain authenticated user can reach.
//!
//! # Internal endpoints (X-Internal-Token shared secret, NOT session auth)
//! - `GET /internal/admin/overview` — platform-wide user/org growth,
//!   7-day active-user counts, and waitlist-conversion funnel (platform-
//!   admin-metrics PR 6 of 7). `users`/`orgs` cumulative-by-month series are
//!   derived on demand from `users.created_at` / `accounts.created_at` — NOT
//!   from a snapshot table — so history is exact and retroactive. The
//!   `conversion` block is **nullable**: a query failure against
//!   `waitlist_snapshot` (migration `0096`, sibling Unit E) emits `null`
//!   ("not computable right now"), NEVER an all-zero shape — a truly empty
//!   `waitlist_snapshot` legitimately produces real zeros, and conflating
//!   "genuinely zero" with "we don't know" is exactly the bug review round 2
//!   of this PR caught (a schema mismatch silently became a confident-looking
//!   "0% conversion"). Every other headline count in this handler
//!   (`users.total`, `orgs.total`, `orgs.by_status`, `active_7d.*`) fails
//!   loud (`500`) on error instead, via the [`fail_loud!`] macro — see its
//!   doc comment for the full "swallowed error" discipline this file now
//!   follows.
//! - `GET /internal/admin/where-from` — client/country/channel breakdowns.
//!   `clients`/`countries` come from the live `user_access_log`; `channels`
//!   comes from `waitlist_snapshot.utm_source`. These are breakdown/list
//!   fields (like [`crate::store::PostgresStore::ocean_stats`]'s per-
//!   dimension breakdowns) — an empty array is an honest, low-stakes degrade,
//!   not a fabricated scalar fact, so they keep `.unwrap_or_default()`.
//! - `GET /internal/admin/spend?month=YYYY-MM` — per-account LLM token/cost
//!   report. Personal-plane sums come from `llm_usage` (RLS-covered —
//!   see the loop-per-account note on [`admin_spend`]); org-plane sums come
//!   from `platform_metrics_daily` (the sibling Unit E rollup worker's
//!   output), never from a tenant pool directly (a single request must not
//!   span planes). `cost_usd` is a rough estimate from the control-plane
//!   `model_rates` table (usage-metering PR1 — see
//!   [`crate::billing::rates`]'s module doc) — never a billed
//!   amount. Every query backing `total` (the account roster, each
//!   account's `llm_usage` read, the org rollup read) fails loud rather than
//!   silently excluding an account from the sum — a partial total is exactly
//!   as dangerous as a fabricated zero.
//! - `GET /internal/admin/credits` — current `credit_balances` + recent
//!   `enforcement_events`, across the whole fleet (usage-metering PR13).
//!   `credit_ledger`/`credit_balances`/`grants`/`enforcement_events`
//!   (migration `0150`) are RLS-covered (`account_isolation`), unlike every
//!   other table this file reads — there is no cross-account bypass for that
//!   policy on the control plane, so this handler loops one
//!   `begin_account_scoped` transaction per account (see [`admin_credits`]'s
//!   own doc comment). **Observation only**: `would_have_blocked` reflects
//!   what the always-observe-only `billing::enforcement` flag would have
//!   done, never an actual block — nothing in this train enforces anything.
//!   An account with neither a balance row nor an enforcement-event row is
//!   simply absent from both lists, never a fabricated zero.
//! - `GET /internal/admin/fleet` — one row per `tenant_databases` entry,
//!   `expected_schema_version` derived live from the embedded tenant
//!   migrator ([`crate::tenancy::migrate::head_version`]) rather than
//!   hardcoded, so it tracks the tenant tree without a code change here.
//!   Fails loud on query error rather than reporting an empty fleet.
//! - `GET /internal/admin/waitlist?limit=50` — raw waitlist rows
//!   (`email_raw` displayed as `email`, `signed_up_at`) with an exact
//!   normalized-email `converted` flag against `users.email`. `total` fails
//!   loud on error rather than reporting zero.
//! - `GET /internal/admin/chain-health` — the "did anything downstream of
//!   ingestion actually run" detector for the 2026-08-04 `ExtractRelationships`
//!   incident (three days of silent `status='done'`-but-empty graph inference
//!   while atoms kept flowing and every dashboard stayed green). Two
//!   invariants, deliberately a small and threshold-light set — chain-level
//!   conservation (trailing 24h: atoms created > 20 AND `relationship_edge`
//!   created == 0 is a violation), per-worker outcome distribution (a
//!   `jobs.outcome` `NothingToDo` ratio of 100% over a full window of
//!   consecutive completed jobs, for a worker that has previously reported
//!   `DidWork` — "stopped", not merely "idle"), and structural dead-letters (any
//!   `status='dead' AND failure_class='structural'` row in the trailing 24h —
//!   a worker's own declaration that it cannot process its own input, whose
//!   correct count is zero), and read-only poison (COR-82: any SQLSTATE 25006
//!   the worker harness saw on a tenant pool in the trailing 30 minutes, with
//!   the per-tenant counters from `TenantActivity::read_only` alongside).
//!   Per-worker liveness and per-worker failure
//!   COUNTS were both considered and explicitly rejected — see
//!   [`admin_chain_health`]'s own doc comment for the full incident writeup,
//!   why liveness doesn't catch this failure mode, and the
//!   `feat/typed-job-outcomes` merge-order dependency this handler has on its
//!   `jobs.outcome` column. Every query fails loud, same as every other
//!   handler in this file.
//! - `POST /internal/accounts/:account_id/rotate-dek` — provision or rewrap an
//!   account's wrapped DEK. See "DEK rotation semantics" below — by default
//!   this does **not** mint a new DEK for an account that already has one.
//! - `POST /internal/admin/connector-flags` — upsert a per-org connector flag
//!   override (Foundation train PR 1.3): body `{clerk_org_id, provider,
//!   enabled}`. See [`set_connector_flag`] and the `connector_flags` module
//!   doc for the resolution/caching contract this writes into.
//!
//! ## DEK rotation semantics (v2, org-tenancy PR 1.3 security review)
//! **First provision** (no existing wrapped-DEK row): generates a brand-new
//! DEK and wraps it with the current master KEK. This is the only case where
//! a new key is minted — safe, because there is no existing content encrypted
//! under anything else yet.
//!
//! **Rewrap** (a wrapped-DEK row already exists, the default when one does):
//! recovers the *existing* DEK bytes (rotation-aware: tries `MASTER_KEK_HEX_NEXT`
//! then `MASTER_KEK_HEX`, exactly like every other unwrap site) and re-wraps
//! **the same DEK** under the current KEK. This is the correct per-account step
//! of a master-KEK rotation (see `coralswarm_core::kek`'s module doc) — content
//! encrypted under the old DEK stays decryptable, because the DEK itself never
//! changes, only its envelope.
//!
//! **Force-rotate** (`?force=true` with an existing row): the v1 behavior —
//! mints a brand-new DEK and overwrites the old one. **This orphans every atom
//! already encrypted under the previous DEK** (re-encrypting existing content
//! is still a future task). Only ever pass `force=true` deliberately, e.g. a
//! confirmed key-compromise response where losing old content is accepted.
//!
//! Every path replaces the wrapped-DEK column in a single atomic UPDATE/UPSERT,
//! so the account is never left in a half-written state.
//!
//! ## Tenant selector (org-tenancy PR 1.3)
//! An optional `?tenant=<clerk_org_id>` query parameter selects which plane is
//! targeted. Absent (the default, and the only case pre-org-tenancy) targets
//! the control-plane `accounts.dek_wrapped` row — byte-identical to before
//! this parameter existed. Present, it resolves the org's tenant pool via the
//! routing table + `TenantRegistry` (the same fingerprint-verified path the
//! binding gate uses) and targets that tenant DB's `account_keys.dek_wrapped`
//! instead — this is also how a brand-new org's first DEK is provisioned,
//! matching `onboard_tenant.sh`'s "DEK generated/wrapped server-side … into
//! tenant `account_keys`" step. Without this selector, org DEK
//! provisioning/rotation would have no path at all: the ambient `state.db`
//! pool this handler used to assume is the WRONG database for an org tenant's
//! key.
//!
//! A tenant is writable in status `provisioning` OR `active` — **not** just
//! `active` — because `onboard_tenant.sh` calls this endpoint at step 7, while
//! the routing row is still `provisioning` (it only flips to `active` at step
//! 9); requiring `active` here would make first-provisioning during the real
//! onboarding sequence permanently fail. Only `suspended`/`offboarded` (the
//! write-freeze states) are rejected.
//!
//! ## Consumer-plane RLS scoping (org-tenancy OPS-4 Phase B.4)
//! Every consumer-plane query in this file that touches an RLS-covered
//! content table (migration `0083_consumer_rls`'s 36-policy roster — here,
//! `atoms` + the `graphs` subquery anchor) runs inside a
//! [`crate::tenancy::scope::begin_account_scoped`] transaction, so it keeps
//! working once OPS-4 switches the app's connection role to the
//! non-`BYPASSRLS` `coralswarm_app`. [`backfill_atom_metadata`] — previously
//! ONE deliberate cross-account sweep with no account filter, which 0083's
//! fail-LOUD strict predicate would hard-error post-OPS-4 — is converted to a
//! loop-per-account (see its doc comment for the flagged-decision rationale).
//! Control/identity tables (`accounts`, `tenant_databases`) are deliberately
//! OUTSIDE the RLS roster and stay on plain pool reads.
//!
//! Org-tenant pools are untouched by this file's content queries — every one
//! of them runs against `state.db`, the control/consumer pool. That, and only
//! that, is why no tenant scope appears here. It is NOT because a tenant
//! database carries no policies: whole-database isolation is real, but it
//! covers the CROSS-ORG boundary only, and `migrations_tenant/0009_tenant_rls`
//! puts member-ocean policies inside every tenant DB. A future handler here
//! that does reach a tenant pool needs a `tenancy::scope` tenant primitive,
//! not this sentence.

use axum::{
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    Json,
};
use chrono::{DateTime, NaiveDate, Utc};
use coralswarm_core::kek::{generate_dek, load_kek, load_kek_next, unwrap_dek_with_rotation, wrap_dek};
use futures::StreamExt;
use serde::Deserialize;
use serde_json::{json, Map, Value};
use std::sync::Arc;
use std::time::Duration;
use subtle::ConstantTimeEq;
use uuid::Uuid;

use crate::billing::rates::{PriceSheet, UNATTRIBUTED_SENTINEL};
use crate::tenancy::routing::TenantStatus;
use crate::tenancy::scope::begin_account_scoped;
use crate::tenancy::TenantId;
use crate::AppState;

// ── Internal token guard ───────────────────────────────────────────────────────

/// Check the `X-Internal-Token` header against the `INTERNAL_TOKEN` env var.
///
/// Returns `true` when the token is present, non-empty, and matches.
fn check_internal_token(headers: &HeaderMap) -> bool {
    let expected = match std::env::var("INTERNAL_TOKEN") {
        Ok(t) if !t.is_empty() => t,
        _ => {
            tracing::warn!("INTERNAL_TOKEN env var not set — internal endpoints always reject");
            return false;
        }
    };

    headers
        .get("x-internal-token")
        .and_then(|v| v.to_str().ok())
        .map(|v| bool::from(v.as_bytes().ct_eq(expected.as_bytes())))
        .unwrap_or(false)
}

// ── POST /internal/accounts/:account_id/rotate-dek ───────────────────────────

/// Optional tenant + force selectors for [`rotate_dek`]. See the module doc
/// for the full first-provision / rewrap / force-rotate semantics.
#[derive(Debug, Deserialize)]
pub struct RotateDekQuery {
    /// A Clerk org id (`tenant_databases.clerk_org_id`). When present, targets
    /// that org's tenant-DB DEK instead of the control-plane one.
    pub tenant: Option<String>,
    /// **Destructive.** When `true` AND a wrapped DEK already exists, mints a
    /// brand-new DEK instead of rewrapping the existing one — orphaning all
    /// content encrypted under the old DEK. Ignored on first provision (there
    /// is nothing to orphan yet). Defaults to `false`.
    #[serde(default)]
    pub force: bool,
}

/// Provision or rewrap the data encryption key for an account.
///
/// This is an **internal** endpoint — it requires an `X-Internal-Token` header
/// matching `INTERNAL_TOKEN` env var.  It does NOT use session-based auth.
///
/// See the module doc for the full DEK rotation semantics (first-provision vs
/// rewrap vs `?force=true`) and the `?tenant=` selector, both from org-tenancy
/// PR 1.3.
pub async fn rotate_dek(
    State(state): State<Arc<AppState>>,
    Path(account_id): Path<Uuid>,
    Query(query): Query<RotateDekQuery>,
    headers: HeaderMap,
) -> (StatusCode, Json<Value>) {
    // ── Auth: internal token ──────────────────────────────────────────────────
    if !check_internal_token(&headers) {
        return (
            StatusCode::UNAUTHORIZED,
            Json(json!({"error": "missing or invalid X-Internal-Token"})),
        );
    }

    // ── Control pool (always needed: existence check, and org routing lookup) ─
    let control_pool = match &state.db {
        Some(p) => p.clone(),
        None => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({"error": "service unavailable"})),
            )
        }
    };

    // ── Resolve which plane this rotation targets ─────────────────────────────
    // NOTE (P2, PR 1.3 security review, not fixed here): unlike the `Some`
    // branch below, this arm does NOT check whether `account_id` belongs to
    // an ORG account (`accounts.clerk_org_id IS NOT NULL`). Calling this
    // endpoint with no `?tenant=` for an org's `account_id` would write a
    // control-plane `accounts.dek_wrapped` row for that org — exactly the
    // shared-plane DEK `clerk_provision.rs`'s shadow-provisioning fence exists
    // to prevent — and, combined with `teams.rs`'s ambient-consumer-pool DEK
    // reads (also not tenant-aware yet), could cause cross-plane key
    // confusion once `MULTITENANT_ENABLED` is on. No dedicated follow-up PR
    // filed yet; track before this endpoint is exposed to less-careful
    // operator tooling.
    let (tenant, target_pool) = match &query.tenant {
        None => (TenantId::Consumer, control_pool.clone()),
        Some(clerk_org_id) => {
            let Some(runtime) = state.tenant_runtime.as_ref() else {
                tracing::warn!(
                    org = %clerk_org_id,
                    "rotate-dek: tenant selector given but multitenancy is not configured"
                );
                return (
                    StatusCode::SERVICE_UNAVAILABLE,
                    Json(json!({"error": "multitenancy not configured"})),
                );
            };
            let row = match crate::tenancy::routing::lookup_by_org(&control_pool, clerk_org_id).await {
                Ok(Some(r)) => r,
                Ok(None) => {
                    return (
                        StatusCode::NOT_FOUND,
                        Json(json!({"error": "unknown tenant"})),
                    )
                }
                Err(e) => {
                    tracing::error!(error = %e, org = %clerk_org_id, "rotate-dek: routing lookup failed");
                    return (
                        StatusCode::SERVICE_UNAVAILABLE,
                        Json(json!({"error": "service unavailable"})),
                    );
                }
            };
            // Reject only the write-freeze states (suspended = offboarding in
            // progress, offboarded = torn down) plus any unrecognized status
            // (fail closed). Deliberately NOT `row.is_servable()` (Active-only)
            // — `onboard_tenant.sh` calls this endpoint at step 7 while the
            // routing row is still `provisioning` (flipped to `active` only at
            // step 9), so requiring Active here would make first-provisioning
            // during the real onboarding sequence permanently fail (P0 found
            // in the PR 1.3 security review: the earlier `is_servable()` gate
            // made the feature dead on arrival in the deployed order).
            if matches!(
                row.status(),
                Some(TenantStatus::Suspended) | Some(TenantStatus::Offboarded) | None
            ) {
                return (
                    StatusCode::CONFLICT,
                    Json(json!({"error": "tenant DEK writes are frozen (suspended/offboarded)", "status": row.status})),
                );
            }
            // `{account_id}` in the path and `?tenant=` in the query are two
            // independent inputs to this handler — nothing else stops an
            // operator (or a scripting mistake) from asking to rotate org A's
            // pool under org B's `account_id`. `tenant_databases.account_id`
            // is the routing table's own record of which account owns this
            // org, so cross-checking it here is the same "assert the DEK's
            // tenant matches the pool's tenant" discipline applied to this
            // call site: reject the mismatch rather than ever writing a DEK
            // for the wrong account into a tenant's `account_keys`.
            if account_id != row.account_id {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(json!({"error": "account_id does not belong to this tenant"})),
                );
            }
            // Registry::get_or_build asserts the tenant DB's own tenant_meta
            // fingerprint before ever returning this pool — the swapped-DSN
            // defense — so a successful build here already guarantees the
            // pool's tenant matches `clerk_org_id`. It's ALSO where the org's
            // commons ocean gets guaranteed to exist (`TenantRegistry::
            // ensure_org_ocean`) — which is why `onboard_tenant.sh` calling
            // this endpoint at step 7 (see the status-gate comment just above)
            // is exactly the earliest point in a fresh onboarding that the org
            // ocean can be created, well before step 9 flips the tenant
            // active. `row.account_id` is used (not the path's `account_id`)
            // for consistency with the equality check just above, though the
            // two are asserted equal by that point.
            match runtime
                .registry
                .get_or_build(clerk_org_id, row.account_id, &row.db_secret_ref)
                .await
            {
                Ok(pool) => (TenantId::Org(clerk_org_id.clone()), pool),
                Err(e) => {
                    tracing::error!(error = %e, org = %clerk_org_id, "rotate-dek: tenant pool build failed");
                    return (
                        StatusCode::SERVICE_UNAVAILABLE,
                        Json(json!({"error": "service unavailable"})),
                    );
                }
            }
        }
    };

    // ── Verify the account exists (control plane is the identity source of
    // truth for both planes — an org's account_id is minted there too) ───────
    let exists: Option<Uuid> = sqlx::query_scalar("SELECT id FROM accounts WHERE id = $1")
        .bind(account_id)
        .fetch_optional(&control_pool)
        .await
        .unwrap_or(None);
    if exists.is_none() {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({"error": "account not found"})),
        );
    }

    // ── Load KEK(s). `next` is only needed for the rewrap path (recovering an
    // existing DEK that may still be wrapped under the outgoing key mid-
    // rotation), but load it unconditionally so a misconfigured
    // MASTER_KEK_HEX_NEXT fails loudly here too, not silently later. ─────────
    let kek = match load_kek() {
        Ok(k) => k,
        Err(e) => {
            tracing::error!(error = %e, "failed to load KEK for DEK rotation");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "crypto configuration error"})),
            );
        }
    };
    let next_kek = match load_kek_next() {
        Ok(n) => n,
        Err(e) => {
            tracing::error!(error = %e, "failed to load MASTER_KEK_HEX_NEXT for DEK rotation");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "crypto configuration error"})),
            );
        }
    };

    // ── Consumer-plane scoped transaction (org-tenancy OPS-4 Phase B.4) ──────
    // `accounts` itself is a control/identity table outside 0083's RLS roster,
    // so the read + write below would work unscoped even as `coralswarm_app` —
    // but every single-account consumer-plane read/write in this file now goes
    // through `begin_account_scoped` anyway (one consistent discipline, and it
    // makes the read-decide-write pair below atomic instead of a TOCTOU pair
    // against a concurrent rotation).
    //
    // `consumer_tx` is `None` on the org plane because `begin_account_scoped`
    // is the CONSUMER-plane primitive (it sets `app.account_id`) and the org
    // arm's only tenant-pool statement is the `account_keys` upsert below —
    // and `account_keys` is one of the ten tables
    // `migrations_tenant/0009_tenant_rls` §3f deliberately leaves off its
    // policy roster. That is the reason, not whole-database isolation: the
    // isolation is real but covers the CROSS-ORG boundary only, and a tenant
    // DB does carry member-ocean policies. Any statement added to this arm
    // that touches a table 0009 DOES cover needs a `tenancy::scope` tenant
    // primitive first.
    let mut consumer_tx: Option<sqlx::Transaction<'static, sqlx::Postgres>> = match &tenant {
        TenantId::Consumer => match begin_account_scoped(&control_pool, account_id).await {
            Ok(tx) => Some(tx),
            Err(e) => {
                tracing::error!(error = %e, account_id = %account_id, "rotate-dek: failed to open account-scoped transaction");
                return (
                    StatusCode::SERVICE_UNAVAILABLE,
                    Json(json!({"error": "service unavailable"})),
                );
            }
        },
        TenantId::Org(_) => None,
    };

    // ── Decide first-provision vs rewrap vs force-rotate (module doc has the
    // full semantics) — this is the P1 fix from the PR 1.3 security review:
    // minting a brand-new DEK unconditionally silently orphans all content
    // already encrypted under the existing one. ──────────────────────────────
    let existing_wrapped: Option<Vec<u8>> = match &mut consumer_tx {
        // Consumer plane: same `accounts.dek_wrapped` read
        // `crate::crypto::fetch_wrapped_dek` performs, executed on the scoped
        // transaction so the read and the UPDATE below share one snapshot.
        Some(tx) => {
            let row: Result<Option<(Option<Vec<u8>>,)>, sqlx::Error> =
                sqlx::query_as("SELECT dek_wrapped FROM accounts WHERE id = $1")
                    .bind(account_id)
                    .fetch_optional(&mut **tx)
                    .await;
            match row {
                Ok(r) => r.and_then(|(wrapped,)| wrapped),
                Err(e) => {
                    tracing::error!(error = %e, account_id = %account_id, "failed to read existing wrapped DEK");
                    return (
                        StatusCode::SERVICE_UNAVAILABLE,
                        Json(json!({"error": "service unavailable"})),
                    );
                }
            }
        }
        // Org plane: unchanged tenant-pool read.
        None => match crate::crypto::fetch_wrapped_dek(&tenant, &target_pool, account_id).await {
            Ok(w) => w,
            Err(e) => {
                tracing::error!(error = %e, account_id = %account_id, "failed to read existing wrapped DEK");
                return (
                    StatusCode::SERVICE_UNAVAILABLE,
                    Json(json!({"error": "service unavailable"})),
                );
            }
        },
    };

    // ── Org-plane FIRST PROVISION goes through the shared helper
    // (`tenancy::dek::provision_org_dek_if_absent`) — the same function the
    // self-serve provisioning job calls, so "mint a DEK into an empty
    // `account_keys` slot" has exactly one definition (audit 4f). Rewrap and
    // force-rotate stay here: the job never rotates. ──────────────────────
    if let (TenantId::Org(_), None) = (&tenant, &existing_wrapped) {
        use crate::tenancy::dek::{provision_org_dek_if_absent, DekProvision};
        return match provision_org_dek_if_absent(&target_pool, account_id, &kek).await {
            Ok(DekProvision::Provisioned) => {
                state.dek_cache.invalidate(&tenant, account_id);
                tracing::info!(
                    account_id = %account_id,
                    tenant = ?tenant,
                    action = "provisioned",
                    "DEK rotate-dek call completed"
                );
                (
                    StatusCode::OK,
                    Json(json!({
                        "message": "DEK updated",
                        "action": "provisioned",
                        "account_id": account_id,
                        "tenant": query.tenant,
                    })),
                )
            }
            // Lost a race with a concurrent first provision: nothing was
            // written. Loud rather than a fabricated "provisioned".
            Ok(DekProvision::AlreadyPresent) => (
                StatusCode::CONFLICT,
                Json(json!({"error": "DEK was provisioned concurrently; retry to rewrap"})),
            ),
            Err(e) => {
                tracing::error!(error = %e, account_id = %account_id, "org DEK first provision failed");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({"error": "internal error"})),
                )
            }
        };
    }

    let action: &'static str;
    let new_wrapped: Vec<u8> = match existing_wrapped {
        None => {
            // First provision: nothing exists yet to orphan.
            action = "provisioned";
            let new_dek = generate_dek();
            match wrap_dek(&kek, &new_dek) {
                Ok(w) => w,
                Err(e) => {
                    tracing::error!(error = %e, "DEK wrap failed during provisioning");
                    return (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(json!({"error": "DEK wrap failed"})),
                    );
                }
            }
        }
        Some(wrapped) if !query.force => {
            // Rewrap: recover the SAME DEK (rotation-aware unwrap — the
            // existing bytes may still be under the outgoing key) and
            // re-wrap it under the current KEK. Content stays decryptable.
            action = "rewrapped";
            let dek = match unwrap_dek_with_rotation(&kek, next_kek.as_ref(), &wrapped) {
                Ok(d) => d,
                Err(e) => {
                    tracing::error!(
                        error = %e,
                        account_id = %account_id,
                        "failed to unwrap existing DEK for rewrap (pass ?force=true only if you accept orphaning existing content)"
                    );
                    return (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(json!({"error": "failed to unwrap existing DEK for rewrap"})),
                    );
                }
            };
            match wrap_dek(&kek, &dek) {
                Ok(w) => w,
                Err(e) => {
                    tracing::error!(error = %e, "DEK wrap failed during rewrap");
                    return (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(json!({"error": "DEK wrap failed"})),
                    );
                }
            }
        }
        Some(_) => {
            // force=true: explicit, destructive new-DEK rotation (the v1
            // behavior). Every atom encrypted under the old DEK is orphaned.
            action = "force-rotated (previous DEK discarded — existing content encrypted under it is now unrecoverable)";
            tracing::warn!(
                account_id = %account_id,
                tenant = ?tenant,
                "rotate-dek called with force=true: minting a new DEK, orphaning content under the old one"
            );
            let new_dek = generate_dek();
            match wrap_dek(&kek, &new_dek) {
                Ok(w) => w,
                Err(e) => {
                    tracing::error!(error = %e, "DEK wrap failed during force-rotation");
                    return (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(json!({"error": "DEK wrap failed"})),
                    );
                }
            }
        }
    };

    // ── Atomically replace the wrapped DEK (single UPDATE/UPSERT = atomic, no
    // half-written state) on whichever plane was resolved above.
    // `consumer_tx` is `Some` exactly when `tenant` is `TenantId::Consumer`
    // (see its construction above), so matching on it IS the plane dispatch. ─
    let write_result = match consumer_tx {
        Some(mut tx) => {
            let res = sqlx::query("UPDATE accounts SET dek_wrapped = $1 WHERE id = $2")
                .bind(&new_wrapped)
                .bind(account_id)
                .execute(&mut *tx)
                .await;
            // Only a committed transaction counts as a write — a commit
            // failure must surface as the error path, never a false OK.
            match res {
                Ok(r) => tx.commit().await.map(|()| r),
                Err(e) => Err(e),
            }
        }
        None => {
            // ON CONFLICT DOES double duty as first-provision (onboarding's
            // "DEK generated/wrapped server-side … into tenant account_keys"
            // step) and later rewrap/rotation, both as one atomic upsert.
            sqlx::query(
                "INSERT INTO account_keys (account_id, dek_wrapped, rotated_at) \
                 VALUES ($1, $2, now()) \
                 ON CONFLICT (account_id) DO UPDATE \
                    SET dek_wrapped = EXCLUDED.dek_wrapped, rotated_at = now()",
            )
            .bind(account_id)
            .bind(&new_wrapped)
            .execute(&target_pool)
            .await
        }
    };

    match write_result {
        Ok(result) if result.rows_affected() >= 1 => {
            // NOTE (P2, PR 1.3 security review, not fixed here): this
            // invalidate runs AFTER the write, so a concurrent reader that
            // squeezes in between the UPDATE/UPSERT commit and this call could
            // cache the now-stale DEK for a full `DekCache` TTL (5 minutes).
            // Bounded blast radius (one TTL window, and only for a request
            // racing an operator-triggered rotation), but a real fix needs
            // either invalidate-before-write + best-effort invalidate-after,
            // or a version/epoch check in `DekCache` itself. No dedicated
            // follow-up PR filed yet — track before this endpoint is used
            // under tighter rotation SLAs.
            state.dek_cache.invalidate(&tenant, account_id);
            tracing::info!(
                account_id = %account_id,
                tenant = ?tenant,
                action,
                "DEK rotate-dek call completed"
            );
            (
                StatusCode::OK,
                Json(json!({
                    "message": "DEK updated",
                    "action": action,
                    "account_id": account_id,
                    "tenant": query.tenant,
                })),
            )
        }
        Ok(_) => (
            StatusCode::NOT_FOUND,
            Json(json!({"error": "account not found"})),
        ),
        Err(e) => {
            tracing::error!(error = %e, account_id = %account_id, "failed to update wrapped DEK");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            )
        }
    }
}

// ── POST /internal/backfill-atom-metadata ────────────────────────────────────

/// Parse the bare channel name from a coral's
/// `"<author> in #<channel> · <when>"` body header. Returns the channel without
/// the leading `#` (e.g. `founders-keepers-demo`), or `None` when the line
/// doesn't carry the header.
fn parse_channel_from_header(content: &str) -> Option<String> {
    let first = content.lines().next()?;
    let idx = first.find(" in #")?;
    let rest = &first[idx + " in #".len()..];
    let chan = rest.split(" · ").next()?.trim();
    (!chan.is_empty()).then(|| chan.to_string())
}

/// Per-invocation, PER-ACCOUNT cap on pass-2 content fetches. (This was a
/// single global cap back when the sweep was one cross-account query; the
/// endpoint stays idempotent, so re-run it until `channel_name_remaining`
/// reaches 0 either way.)
const BACKFILL_PASS2_BATCH: i64 = 5000;

/// Bounded concurrency for pass 2's R2 blob fetches — same role as
/// `fetch_atom_content`'s semaphore, sized down for a maintenance sweep that
/// competes with live traffic for R2 bandwidth.
const BACKFILL_R2_CONCURRENCY: usize = 8;

/// Backfill structured provenance (migration 0034) onto pre-existing Slack
/// corals. Two passes:
///   1. Pure-SQL: `channel_id` from `source_id` (`<channel>/<ts>...`) and
///      `author_name` from `participants[1]` — no decryption needed.
///   2. `channel_name` parsed from each coral's decrypted body header
///      (handles dev inline content and prod R2-encrypted blobs).
///
/// `author_id` is left NULL: the provider-native sender id was never persisted
/// for historical atoms. Idempotent (filters on `… IS NULL`) and safe to re-run.
///
/// ## Loop-per-account under consumer RLS (org-tenancy OPS-4 Phase B.4)
/// This handler used to run ONE cross-account `UPDATE atoms … WHERE … IS
/// NULL` sweep with no account filter. Post-OPS-4 (the app connecting as the
/// non-`BYPASSRLS` `coralswarm_app`), that query either hard-errors (no
/// `app.account_id` GUC set — 0083's deliberate fail-LOUD strict predicate)
/// or, if a scope leaked in from an earlier caller on the same connection,
/// silently sweeps only that one account's rows. Neither is acceptable for a
/// maintenance endpoint, so it now:
///
///   1. Enumerates account ids from `accounts` — a control/identity table
///      deliberately OUTSIDE 0083's RLS roster (`coralswarm_app` holds a
///      plain SELECT grant on it), so the enumeration itself needs no scope
///      (and no SECURITY-DEFINER bootstrap function) and behaves identically
///      pre- and post-OPS-4.
///   2. Runs `begin_account_scoped` transactions per account, executing the
///      same idempotent statements WITH an explicit per-account anchor
///      (`graph_id IN (SELECT id FROM graphs WHERE account_id = $n)`): the
///      app-layer WHERE clause stays the primary scoping mechanism and keeps
///      row attribution per-account deterministic even pre-OPS-4 (where the
///      owner role's BYPASSRLS would otherwise let iteration #1 sweep every
///      account); RLS is the backstop, exactly per 0083's design.
///
/// **Flagged design decision, resolved as loop-per-account:** the alternative
/// — a separate elevated (BYPASSRLS) ops connection reserved for
/// cross-account sweeps — was rejected to avoid widening the
/// elevated-credential surface. Looping is slower (one transaction pair per
/// account instead of one sweep) but keeps every consumer-plane content query
/// in the codebase inside the same RLS contract, with no escape hatch to
/// audit.
///
/// R2 blob fetches happen BETWEEN the two per-account transactions, never
/// inside one — network I/O must not pin a scoped pool connection. The DEK
/// read backing them (`accounts.dek_wrapped` via `resolve_dek_cached`) is a
/// control-table read, fine unscoped.
///
/// Internal endpoint — requires `X-Internal-Token`, not session auth.
pub async fn backfill_atom_metadata(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> (StatusCode, Json<Value>) {
    if !check_internal_token(&headers) {
        return (
            StatusCode::UNAUTHORIZED,
            Json(json!({"error": "missing or invalid X-Internal-Token"})),
        );
    }

    let Some(pool) = &state.db else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error": "service unavailable"})),
        );
    };

    // Account roster — control-table read, needs no scope (see handler doc).
    // ORDER BY makes progress observable/deterministic across re-runs.
    let account_ids: Vec<Uuid> = match sqlx::query_scalar("SELECT id FROM accounts ORDER BY id")
        .fetch_all(pool)
        .await
    {
        Ok(ids) => ids,
        Err(e) => {
            tracing::error!(error = %e, "backfill: account enumeration failed");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "backfill account enumeration failed"})),
            );
        }
    };

    let mut pass1_rows = 0u64;
    let mut channel_updates = 0u64;
    let mut remaining = 0i64;

    for account_id in &account_ids {
        match backfill_account_atom_metadata(&state, pool, *account_id).await {
            Ok((p1, upd, rem)) => {
                pass1_rows += p1;
                channel_updates += upd;
                remaining += rem;
            }
            Err(msg) => {
                // Fail the run loudly at the first broken account (the
                // endpoint is idempotent — a re-run resumes safely), reporting
                // WHICH account so the operator can investigate.
                // The detailed cause stays in the log line above; the response
                // body is generic so DB internals (table/column/constraint
                // names, query fragments) never reach the caller.
                tracing::error!(account_id = %account_id, error = %msg, "backfill: per-account pass failed");
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({"error": "backfill failed", "account_id": account_id})),
                );
            }
        }
    }

    tracing::info!(
        accounts_processed = account_ids.len(),
        pass1_rows,
        channel_updates,
        remaining,
        "atom metadata backfill run complete"
    );
    (
        StatusCode::OK,
        Json(json!({
            "accounts_processed": account_ids.len(),
            "channel_id_author_rows": pass1_rows,
            "channel_name_updated": channel_updates,
            "channel_name_remaining": remaining,
        })),
    )
}

/// One account's slice of [`backfill_atom_metadata`]: pass 1 + pass-2
/// candidate select in a first `begin_account_scoped` transaction, content
/// resolution (inline or R2) outside any transaction, then the
/// `channel_name` update + remaining-count in a second scoped transaction.
/// Returns `(pass1_rows, channel_name_updates, channel_name_remaining)`.
async fn backfill_account_atom_metadata(
    state: &AppState,
    pool: &sqlx::PgPool,
    account_id: Uuid,
) -> Result<(u64, u64, i64), String> {
    // ── Scoped tx #1: pass 1 UPDATE + pass 2 candidate SELECT ────────────────
    let mut tx = begin_account_scoped(pool, account_id)
        .await
        .map_err(|e| format!("backfill: begin_account_scoped failed: {e}"))?;

    let pass1_rows = sqlx::query(
        "UPDATE atoms SET \
            channel_id  = COALESCE(channel_id, NULLIF(split_part(source_id, '/', 1), '')), \
            author_name = COALESCE(author_name, NULLIF(participants[1], '')) \
         WHERE source_type = 'slack' \
           AND (channel_id IS NULL OR author_name IS NULL) \
           AND graph_id IN (SELECT id FROM graphs WHERE account_id = $1)",
    )
    .bind(account_id)
    .execute(&mut *tx)
    .await
    .map_err(|e| format!("backfill pass 1 failed: {e}"))?
    .rows_affected();

    // Pass-2 candidates: everything needed to resolve content on either the
    // dev-inline or the prod-R2 path, fetched in one go so no second
    // RLS-covered read is needed outside a scoped transaction.
    let candidates: Vec<(Uuid, Option<String>, String, i32)> = sqlx::query_as(
        "SELECT atom_id, content_inline, content_blob_key, COALESCE(header_len, 0) \
         FROM atoms \
         WHERE source_type = 'slack' AND channel_name IS NULL \
           AND graph_id IN (SELECT id FROM graphs WHERE account_id = $1) \
         LIMIT $2",
    )
    .bind(account_id)
    .bind(BACKFILL_PASS2_BATCH)
    .fetch_all(&mut *tx)
    .await
    .map_err(|e| format!("backfill pass 2 candidate select failed: {e}"))?;

    tx.commit()
        .await
        .map_err(|e| format!("backfill: transaction commit failed: {e}"))?;

    // ── Content resolution, outside any transaction (see handler doc) ────────
    let mut upd_ids: Vec<Uuid> = Vec::new();
    let mut upd_names: Vec<String> = Vec::new();

    if !candidates.is_empty() {
        match &state.r2 {
            // Dev mode: no R2, content lives in the inline column.
            None => {
                for (atom_id, content_inline, _blob_key, header_len) in &candidates {
                    let Some(content) = content_inline else {
                        continue;
                    };
                    // Strip the contextual chunk-header (cs-phase0-chunk-headers)
                    // before parsing — it precedes the "<author> in #<channel>"
                    // body header, exactly as in `fetch_atom_content`.
                    let body = crate::store::strip_header_prefix(content, *header_len);
                    if let Some(name) = parse_channel_from_header(body) {
                        upd_ids.push(*atom_id);
                        upd_names.push(name);
                    }
                }
            }
            // Prod: decrypt each R2 blob with this account's DEK.
            Some(r2) => {
                match crate::crypto::resolve_dek_cached(pool, account_id, &state.dek_cache).await {
                    Err(e) => return Err(format!("backfill: DEK resolution failed: {e}")),
                    Ok(None) => {
                        // No DEK provisioned — nothing of this account's is
                        // decryptable; its atoms stay counted in `remaining`.
                        tracing::warn!(
                            account_id = %account_id,
                            "backfill: no DEK for account — skipping channel_name pass"
                        );
                    }
                    Ok(Some(dek)) => {
                        // Owned job tuples (not borrows of `candidates`):
                        // borrowing closures inside a handler's async fn trip
                        // rustc's "implementation of `FnOnce` is not general
                        // enough" HRTB limitation.
                        let jobs: Vec<(Uuid, String, i32)> = candidates
                            .iter()
                            .map(|(atom_id, _inline, blob_key, header_len)| {
                                (*atom_id, blob_key.clone(), *header_len)
                            })
                            .collect();
                        let mut fetches = futures::stream::iter(jobs.into_iter().map(
                            |(atom_id, blob_key, header_len)| {
                                let r2 = Arc::clone(r2);
                                async move {
                                    let bytes = r2.get_atom_blob(&blob_key, &dek).await;
                                    (atom_id, header_len, bytes)
                                }
                            },
                        ))
                        .buffer_unordered(BACKFILL_R2_CONCURRENCY);

                        while let Some((atom_id, header_len, bytes)) = fetches.next().await {
                            match bytes {
                                Ok(bytes) => {
                                    let raw = String::from_utf8_lossy(&bytes).into_owned();
                                    let body = crate::store::strip_header_prefix(&raw, header_len);
                                    if let Some(name) = parse_channel_from_header(body) {
                                        upd_ids.push(atom_id);
                                        upd_names.push(name);
                                    }
                                }
                                Err(e) => {
                                    // One bad blob must not fail the whole
                                    // account: skip it (it stays in
                                    // `remaining`) and keep going.
                                    tracing::warn!(
                                        atom_id = %atom_id,
                                        error = %e,
                                        "backfill: blob fetch failed — skipping atom"
                                    );
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    // ── Scoped tx #2: channel_name update + this account's remaining count ───
    let mut tx = begin_account_scoped(pool, account_id)
        .await
        .map_err(|e| format!("backfill: begin_account_scoped failed: {e}"))?;

    let mut channel_updates = 0u64;
    if !upd_ids.is_empty() {
        channel_updates = sqlx::query(
            "UPDATE atoms AS a SET channel_name = v.cn \
             FROM (SELECT unnest($1::uuid[]) AS id, unnest($2::text[]) AS cn) v \
             WHERE a.atom_id = v.id AND a.channel_name IS NULL \
               AND a.graph_id IN (SELECT id FROM graphs WHERE account_id = $3)",
        )
        .bind(&upd_ids)
        .bind(&upd_names)
        .bind(account_id)
        .execute(&mut *tx)
        .await
        .map_err(|e| format!("backfill pass 2 update failed: {e}"))?
        .rows_affected();
    }

    // How many of THIS account's slack corals still lack a channel_name after
    // this run (summed across accounts by the caller).
    let remaining: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM atoms \
         WHERE source_type = 'slack' AND channel_name IS NULL \
           AND graph_id IN (SELECT id FROM graphs WHERE account_id = $1)",
    )
    .bind(account_id)
    .fetch_one(&mut *tx)
    .await
    .map_err(|e| format!("backfill remaining count failed: {e}"))?;

    tx.commit()
        .await
        .map_err(|e| format!("backfill: transaction commit failed: {e}"))?;

    Ok((pass1_rows, channel_updates, remaining))
}

// ── Internal platform-metrics endpoints (platform-admin-metrics PR 6 of 7) ───
//
// All five below are read-only, cross-account platform-operator surfaces.
// They live ONLY on `internal_app()` (see `lib.rs`), behind
// `check_internal_token`, exactly like `rotate_dek`/`backfill_atom_metadata`
// above — never on the public `/v1` router. They never query a tenant pool
// directly (org content numbers come from `platform_metrics_daily`, the
// sibling Unit E rollup worker's output) — a single request must not span
// planes (`tenancy::extractor`/`tenancy::reachability`'s invariant).
//
// `users`/`accounts`/`account_members`/`tenant_databases` are control/
// identity tables outside migration `0083_consumer_rls`'s roster (same as
// `rotate_dek`/`backfill_atom_metadata` above), so they're read directly off
// `state.db` with no scoping. `user_access_log` / `platform_metrics_daily` /
// `platform_active_users` (migration `0095`) and `waitlist_snapshot`
// (migration `0096`, sibling Unit E) are likewise NOT RLS-covered — this
// migration deliberately keeps all platform-metrics tables outside that
// roster (see `0095_platform_metrics.sql`'s header). The ONE exception is
// `llm_usage`, which IS RLS-covered (`0083`) — [`admin_spend`] loops it
// per-account through `begin_account_scoped`, the same shape
// `backfill_atom_metadata` uses, for the same reason (an unscoped read
// hard-errors post-OPS-4 under the non-`BYPASSRLS` `coralswarm_app` role).

/// Parse a strict `YYYY-MM` month string into a `[start, end)` half-open UTC
/// range covering that calendar month. Deliberately stricter than
/// `NaiveDate`'s own parser (which would accept a day-of-month, or a
/// single-digit month): exactly 4 digits, a literal `-`, exactly 2 digits,
/// month `01`-`12`. Returns `None` for anything else — callers reject with
/// `400 Bad Request`.
fn parse_month_strict(month: &str) -> Option<(DateTime<Utc>, DateTime<Utc>)> {
    let bytes = month.as_bytes();
    if bytes.len() != 7 || bytes[4] != b'-' {
        return None;
    }
    if !month[0..4].bytes().all(|b| b.is_ascii_digit()) || !month[5..7].bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let year: i32 = month[0..4].parse().ok()?;
    let mon: u32 = month[5..7].parse().ok()?;
    if !(1..=12).contains(&mon) {
        return None;
    }
    let start = NaiveDate::from_ymd_opt(year, mon, 1)?;
    let end = if mon == 12 {
        NaiveDate::from_ymd_opt(year + 1, 1, 1)?
    } else {
        NaiveDate::from_ymd_opt(year, mon + 1, 1)?
    };
    let start_dt = DateTime::<Utc>::from_naive_utc_and_offset(start.and_hms_opt(0, 0, 0)?, Utc);
    let end_dt = DateTime::<Utc>::from_naive_utc_and_offset(end.and_hms_opt(0, 0, 0)?, Utc);
    Some((start_dt, end_dt))
}

/// Evaluate a `Result<T, sqlx::Error>` from a HEADLINE query — one whose
/// value a human reads as a confident fact (a total count, a fleet row
/// list, a dollar total) — and, on error, log it and bail out of the
/// enclosing handler with `500` rather than letting the caller silently
/// substitute a default.
///
/// This is the fix for a real bug (platform-admin-metrics PR 6 of 7, review
/// round 2): `admin_overview`'s original conversion-funnel query used
/// `.unwrap_or((0, 0, 0, 0))`, so a genuine query failure (a schema
/// mismatch, in the actual incident) silently became a confident-looking
/// "0% conversion" instead of a 500 — far worse than an error, because
/// nothing downstream could tell the difference between "truly zero" and
/// "we don't know." The test this macro encodes: **would a swallowed error
/// here produce a number a human would read as fact?** If yes, fail loud
/// (this macro) or degrade to an explicit `null`/absent value (as
/// `admin_overview`'s `conversion` block and every `freshness.at` field do) —
/// never to a plausible-looking default.
///
/// NOT for breakdown/list-shaped fields (a growth series, a status
/// breakdown, a client/country tally) — an empty list there is an honest,
/// low-stakes degrade (same resilience pattern as `Store::ocean_stats`'s
/// per-dimension breakdowns, `store/mod.rs`'s `breakdown_futs`), not a
/// fabricated scalar fact, so those call sites keep `.unwrap_or_default()`.
macro_rules! fail_loud {
    ($result:expr, $what:expr) => {
        match $result {
            Ok(v) => v,
            Err(e) => {
                tracing::error!(error = %e, query = $what, "admin metrics: headline query failed, refusing to fabricate a value");
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({"error": "internal error"})),
                );
            }
        }
    };
}

/// Cumulative-by-month growth series: `[{ "month": "YYYY-MM", "cum": N }, …]`.
/// `table`/`extra_where` are fixed call-site constants (never user input, so
/// safe to interpolate — same convention as `tenancy::routing::SELECT_COLS`),
/// used by both `users.series` and `orgs.series` in [`admin_overview`].
/// Derived on demand from `created_at`, NOT a snapshot table — deliberate,
/// per the migration's header, so history is exact and retroactive.
async fn cumulative_monthly_series(
    pool: &sqlx::PgPool,
    table: &str,
    extra_where: &str,
) -> Result<Vec<Value>, sqlx::Error> {
    let sql = format!(
        "WITH monthly AS ( \
            SELECT date_trunc('month', created_at) AS month, COUNT(*) AS c \
            FROM {table} {extra_where} \
            GROUP BY 1 \
         ) \
         SELECT to_char(month, 'YYYY-MM') AS month, SUM(c) OVER (ORDER BY month)::bigint AS cum \
         FROM monthly ORDER BY month"
    );
    let rows: Vec<(String, i64)> = sqlx::query_as(&sql).fetch_all(pool).await?;
    Ok(rows
        .into_iter()
        .map(|(month, cum)| json!({"month": month, "cum": cum}))
        .collect())
}

// ── GET /internal/admin/overview ─────────────────────────────────────────────

/// Platform-wide user/org growth, 7-day active-user counts, and waitlist-
/// conversion funnel. See the module doc above for the resilience/degrade
/// conventions this follows.
pub async fn admin_overview(State(state): State<Arc<AppState>>, headers: HeaderMap) -> (StatusCode, Json<Value>) {
    if !check_internal_token(&headers) {
        return (
            StatusCode::UNAUTHORIZED,
            Json(json!({"error": "missing or invalid X-Internal-Token"})),
        );
    }
    let Some(pool) = &state.db else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error": "service unavailable"})),
        );
    };

    // ── Headline counts: FAIL LOUD on error, never silently default ─────────
    // Each is a single confident scalar a human reads as fact — see
    // `fail_loud!`'s doc comment for why `.unwrap_or(0)` is wrong here (it IS
    // right, further down, for the `series`/`by_status` BREAKDOWNS — an empty
    // list there is an honest degrade, not a fabricated scalar).
    let users_total: i64 = fail_loud!(
        sqlx::query_scalar("SELECT COUNT(*) FROM users").fetch_one(pool).await,
        "users_total"
    );
    let users_new_30d: i64 = fail_loud!(
        sqlx::query_scalar("SELECT COUNT(*) FROM users WHERE created_at > now() - interval '30 days'")
            .fetch_one(pool)
            .await,
        "users_new_30d"
    );
    let users_series = cumulative_monthly_series(pool, "users", "").await.unwrap_or_default();

    // Orgs = accounts with a non-NULL clerk_org_id, EXCLUDING a personal org
    // (control migration 0208 `personal_orgs`): a personal org's account is a
    // relabelled one-person account-of-one, not an organization, and it has
    // no `tenant_databases` row to ever resolve a real status against — left
    // in, it inflates `orgs_total` and stacks every one of its rows into the
    // `by_status` breakdown's `'unknown'` bucket below, unrelated to (and
    // indistinguishable from) an org whose provisioning is actually stuck or
    // broken. Its data lives on the personal plane and is reported there
    // instead (see `admin_spend`'s `personal_accounts` roster). Personal
    // accounts-of-one (plain `clerk_org_id IS NULL` ones) have always been
    // excluded and must never be counted here either.
    let orgs_total: i64 = fail_loud!(
        sqlx::query_scalar(
            "SELECT COUNT(*) FROM accounts \
             WHERE clerk_org_id IS NOT NULL \
               AND clerk_org_id NOT IN (SELECT clerk_org_id FROM personal_orgs)",
        )
        .fetch_one(pool)
        .await,
        "orgs_total"
    );
    let orgs_series = cumulative_monthly_series(
        pool,
        "accounts",
        "WHERE clerk_org_id IS NOT NULL AND clerk_org_id NOT IN (SELECT clerk_org_id FROM personal_orgs)",
    )
    .await
    .unwrap_or_default();
    // `by_status` is a partition of `orgs_total`, not an independent
    // breakdown-of-convenience — an empty result here while `orgs_total > 0`
    // would visibly contradict it, so this fails loud too rather than
    // silently showing a partition that doesn't sum to the total above it.
    let orgs_by_status: Vec<(String, i64)> = fail_loud!(
        sqlx::query_as(
            "SELECT COALESCE(td.status, 'unknown') AS status, COUNT(*) \
             FROM accounts a LEFT JOIN tenant_databases td ON td.clerk_org_id = a.clerk_org_id \
             WHERE a.clerk_org_id IS NOT NULL \
               AND a.clerk_org_id NOT IN (SELECT clerk_org_id FROM personal_orgs) \
             GROUP BY 1",
        )
        .fetch_all(pool)
        .await,
        "orgs_by_status"
    );

    // did_work = COUNT(DISTINCT user_id) across ALL tenants — a user active in
    // two orgs on the same day counts once (the whole reason
    // `platform_active_users`'s grain includes `tenant`, per 0095's header).
    let signed_in_7d: i64 = fail_loud!(
        sqlx::query_scalar("SELECT COUNT(DISTINCT user_id) FROM user_access_log WHERE created_at > now() - interval '7 days'")
            .fetch_one(pool)
            .await,
        "active_7d.signed_in"
    );
    let did_work_7d: i64 = fail_loud!(
        sqlx::query_scalar("SELECT COUNT(DISTINCT user_id) FROM platform_active_users WHERE day > current_date - 7")
            .fetch_one(pool)
            .await,
        "active_7d.did_work"
    );

    // ── Conversion funnel: exact normalized-email match, NULLABLE on error ──
    // `waitlist_snapshot.email_normalized` is ALREADY normalized at write
    // time (`workers::waitlist_sync`) and is that table's PRIMARY KEY —
    // compare `lower(trim(users.email))` against it DIRECTLY. Wrapping the
    // snapshot side in `lower(trim(...))` too would be redundant AND defeat
    // the PK index (Postgres can't use an index on `email_normalized` through
    // a function call on that same column), forcing a full scan. Normalize
    // only the `users.email` side, never `w.email_normalized`.
    //
    // `conversion` is NULLABLE, and that nullability is load-bearing: review
    // round 2 caught a real bug where this query's failure (a schema
    // mismatch against a table this branch didn't have yet) was swallowed by
    // `.unwrap_or((0, 0, 0, 0))` into a confident-looking "0% conversion"
    // headline — indistinguishable from a genuinely empty waitlist, but
    // fabricated. `waitlist_total: 0` from a truly empty `waitlist_snapshot`
    // is a real, honest answer; `null` from a failed query is a DIFFERENT,
    // equally real answer ("not computable right now") — the two must never
    // be conflated, so a query error here maps to `null`, never to zeros.
    let conversion_result: Result<(i64, i64, i64, i64), sqlx::Error> = sqlx::query_as(
        "SELECT \
            (SELECT COUNT(*) FROM waitlist_snapshot), \
            (SELECT COUNT(*) FROM waitlist_snapshot w JOIN users u ON lower(trim(u.email)) = w.email_normalized), \
            (SELECT COUNT(*) FROM waitlist_snapshot w WHERE NOT EXISTS (SELECT 1 FROM users u WHERE lower(trim(u.email)) = w.email_normalized)), \
            (SELECT COUNT(*) FROM users u WHERE NOT EXISTS (SELECT 1 FROM waitlist_snapshot w WHERE w.email_normalized = lower(trim(u.email))))",
    )
    .fetch_one(pool)
    .await;
    let conversion: Value = match conversion_result {
        Ok((waitlist_total, matched, waitlisted_unconverted, users_never_waitlisted)) => json!({
            "waitlist_total": waitlist_total,
            "matched": matched,
            "waitlisted_unconverted": waitlisted_unconverted,
            "users_never_waitlisted": users_never_waitlisted,
        }),
        Err(e) => {
            tracing::error!(
                error = %e,
                "admin_overview: conversion-funnel query failed — emitting null, never a fabricated zero"
            );
            Value::Null
        }
    };

    // `rollup_at` reflects the platform-wide nightly rollup worker
    // (`platform_metrics_daily.computed_at`, sibling Unit E) — the general
    // "how fresh are the platform_metrics_daily-derived numbers on this
    // dashboard" signal (also backs `active_7d.did_work`'s
    // `platform_active_users` freshness). A freshness TIMESTAMP degrading to
    // `null` on error is fine (absence, correctly expressed) — unlike the
    // scalar counts above, nothing reads a null timestamp as a fabricated
    // fact.
    let rollup_at: Option<DateTime<Utc>> = sqlx::query_scalar("SELECT MAX(computed_at) FROM platform_metrics_daily")
        .fetch_one(pool)
        .await
        .unwrap_or(None);

    (
        StatusCode::OK,
        Json(json!({
            "users": {
                "total": users_total,
                "new_30d": users_new_30d,
                "series": users_series,
            },
            "orgs": {
                "total": orgs_total,
                "by_status": orgs_by_status.into_iter().map(|(status, count)| json!({"status": status, "count": count})).collect::<Vec<_>>(),
                "series": orgs_series,
            },
            "active_7d": {
                "signed_in": signed_in_7d,
                "did_work": did_work_7d,
            },
            "conversion": conversion,
            "freshness": {
                "control_at": Utc::now().to_rfc3339(),
                "rollup_at": rollup_at.map(|t| t.to_rfc3339()),
            },
        })),
    )
}

// ── GET /internal/admin/where-from ───────────────────────────────────────────

/// Client / country / channel breakdowns. `clients`/`countries` are live
/// (`user_access_log`); `channels` is nightly (`waitlist_snapshot.utm_source`).
pub async fn admin_where_from(State(state): State<Arc<AppState>>, headers: HeaderMap) -> (StatusCode, Json<Value>) {
    if !check_internal_token(&headers) {
        return (
            StatusCode::UNAUTHORIZED,
            Json(json!({"error": "missing or invalid X-Internal-Token"})),
        );
    }
    let Some(pool) = &state.db else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error": "service unavailable"})),
        );
    };

    let clients: Vec<(Option<String>, i64)> =
        sqlx::query_as("SELECT client, COUNT(*) FROM user_access_log GROUP BY client ORDER BY COUNT(*) DESC")
            .fetch_all(pool)
            .await
            .unwrap_or_default();
    let countries: Vec<(Option<String>, i64)> =
        sqlx::query_as("SELECT country, COUNT(*) FROM user_access_log GROUP BY country ORDER BY COUNT(*) DESC")
            .fetch_all(pool)
            .await
            .unwrap_or_default();

    let channels: Vec<(Option<String>, i64)> =
        sqlx::query_as("SELECT utm_source, COUNT(*) FROM waitlist_snapshot GROUP BY utm_source ORDER BY COUNT(*) DESC")
            .fetch_all(pool)
            .await
            .unwrap_or_default();

    let access_log_bounds: (Option<DateTime<Utc>>, Option<DateTime<Utc>>) =
        sqlx::query_as("SELECT MIN(created_at), MAX(created_at) FROM user_access_log")
            .fetch_one(pool)
            .await
            .unwrap_or((None, None));

    // A single combined `freshness` covers both a live source (access log)
    // and a nightly one (waitlist channels) — when only the nightly source
    // has data (access log still empty), reported as `'nightly'` rather than
    // fabricating a `'live'` timestamp with nothing behind it.
    let has_live = !clients.is_empty() || !countries.is_empty();
    let has_any = has_live || !channels.is_empty();
    let (kind, at): (&str, Option<DateTime<Utc>>) = if !has_any {
        ("collecting", None)
    } else if has_live {
        ("live", access_log_bounds.1)
    } else {
        ("nightly", None)
    };

    let to_pairs = |rows: Vec<(Option<String>, i64)>| -> Vec<Value> {
        rows.into_iter()
            .map(|(key, count)| json!({"key": key.unwrap_or_else(|| "unknown".to_string()), "count": count}))
            .collect()
    };

    (
        StatusCode::OK,
        Json(json!({
            "clients": to_pairs(clients),
            "countries": to_pairs(countries),
            "channels": to_pairs(channels),
            "collecting_since": access_log_bounds.0.map(|t| t.to_rfc3339()),
            "freshness": { "kind": kind, "at": at.map(|t| t.to_rfc3339()) },
        })),
    )
}

// ── GET /internal/admin/spend?month=YYYY-MM ──────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct SpendQuery {
    pub month: String,
}

/// Top-N highest-cost accounts (personal + org combined, ranked by
/// `cost_usd`) reported individually; the remainder collapses into ONE
/// `other_accounts` row. `usize` cap, not a hard product requirement — tune
/// freely.
const SPEND_TOP_N: usize = 20;

/// Hard bound on the ENTIRE personal-plane `llm_usage` loop in [`admin_spend`]
/// (every account's `begin_account_scoped` transaction + query, combined) —
/// not per-account. `admin_spend` cannot collapse that loop into one
/// grouped-by-account query the way an ordinary aggregate report would:
/// migration `0083_consumer_rls`'s `account_isolation` policy on `llm_usage`
/// is `account_id = current_setting('app.account_id')::uuid`, a single
/// session-GUC value per transaction (set by
/// [`crate::tenancy::scope::begin_account_scoped`]), so one query cannot
/// legitimately return rows for more than one account's `app.account_id` at
/// once post-OPS-4 (once the app stops connecting as a `BYPASSRLS` role) —
/// batching would mean dropping the per-account scope entirely, which is not
/// an acceptable trade against RLS-parity for a request-latency win (CodeRabbit
/// review, PR #361). So the loop-per-account shape stays, and this timeout is
/// the sanctioned mitigation instead: it bounds the worst case (a growing
/// account roster tying up this admin request indefinitely) without touching
/// the scoping. Same `tokio::time::timeout`-around-a-loop pattern as
/// `workers::platform_rollup::QUERY_TIMEOUT`/`ACCOUNT_QUERY_TIMEOUT`, sized
/// generously since this is an on-demand internal report, not a tight
/// user-facing path.
const SPEND_PERSONAL_LOOP_TIMEOUT: Duration = Duration::from_secs(45);

/// LIST rates in USD per 1,000,000 tokens — **not billed amounts**. Anthropic
/// volume discounts, committed-use pricing, and credits are invisible to
/// this; `cost_usd` in `admin_spend`'s response is a rough, internally-
/// consistent estimate for RELATIVE comparison across accounts, never a
/// figure to reconcile against an actual invoice.
///
/// Usage-metering PR1: the rate sheet itself (previously the hardcoded
/// `MODEL_RATES_PER_MILLION` / `SONNET_5_*` / `DEFAULT_RATE_PER_MILLION`
/// constants that lived here) moved to the control-plane `model_rates`
/// table — see `backend/migrations_control/0148_model_rates.sql`'s header
/// for the full seed data and [`crate::billing::rates`]'s module doc for the
/// ordered-substring-match / `as_of` contract this function and
/// [`estimate_cost_usd`] below now just thread a [`PriceSheet`] through.
///
/// `as_of` is the reported month's start (`admin_spend`'s `start`), not
/// `Utc::now()` — usage is grouped by calendar month before this is called,
/// so a report for August 2026 must price `claude-sonnet-5` at its
/// introductory rate even if run in September, and a report for September
/// onward must price it at the standard rate even if run in August. This
/// keeps a given month's `cost_usd` stable no matter when the report is
/// pulled. `sheet` must be snapshotted ONCE per report (`admin_spend` loads
/// it a single time near the top of the handler) and passed BY REFERENCE —
/// see [`crate::billing::rates`]'s module doc for why a per-call reload
/// would be both a latency regression and would force Postgres onto these
/// pricing unit tests.
///
/// **OpenRouter provider port, PR1 — provider is hardcoded to `"anthropic"`
/// here, deliberately, not yet threaded through.** [`PriceSheet::rate`] now
/// filters on `(provider, model)`, but the `llm_usage` queries feeding this
/// handler (`admin_spend`'s per-account loop, below) `GROUP BY model` only —
/// they don't select `provider` per bucket at all, a pre-existing shape this
/// PR does not touch (rewiring 2+ aggregation queries in a money-reporting
/// endpoint is its own, separately-reviewable change, not a drive-by here).
/// Hardcoding `"anthropic"` is a NO-OP for every row this endpoint has ever
/// priced (100% Anthropic to date), and is the fail-safe direction: an
/// OpenRouter row would simply miss its specific `model_rates` row and fall
/// through to the blend rate (a rougher estimate, logged via `PriceSheet::
/// rate`'s own `alert = true` warn) rather than silently mis-price under an
/// unrelated provider's pattern.
fn model_rate(model: &str, as_of: DateTime<Utc>, sheet: &PriceSheet) -> (f64, f64) {
    let resolved = sheet.rate("anthropic", model, as_of);
    (resolved.input_per_million, resolved.output_per_million)
}

/// `cache_creation_tokens`/`cache_read_tokens` are priced off the SAME
/// per-model base input rate [`model_rate`] already returns, scaled by that
/// same row's own `cache_write_multiplier`/`cache_read_multiplier` — see
/// `0148_model_rates.sql`'s header for the Anthropic prompt-cache economics
/// these encode. Deliberately not a second rate table: Anthropic defines
/// both as a multiplier on the model's own input rate, not an independent
/// price point. Omitting these (the pre-fix behavior) undercounts real
/// spend whenever prompt caching is in play: verified against production
/// `llm_usage` rows for 2026-08-03, ignoring cache tokens alone accounted
/// for an ~11% undercount that day.
///
/// Resolves `sheet.rate(model, as_of)` twice ([`model_rate`] internally,
/// then again here for the cache multipliers) rather than once — a
/// deliberate trade for keeping [`model_rate`] itself independently
/// callable/testable with its pre-PR1 `(f64, f64)` shape. `PriceSheet::rate`
/// is a pure in-memory scan over ~16 rows (no I/O), so the duplicate lookup
/// costs nothing that matters at `admin_spend`'s call volume.
fn estimate_cost_usd(
    input_tokens: i64,
    output_tokens: i64,
    cache_creation_tokens: i64,
    cache_read_tokens: i64,
    model: &str,
    as_of: DateTime<Utc>,
    sheet: &PriceSheet,
) -> f64 {
    let (rate_in, rate_out) = model_rate(model, as_of, sheet);
    let resolved = sheet.rate("anthropic", model, as_of);
    (input_tokens as f64 / 1_000_000.0) * rate_in
        + (output_tokens as f64 / 1_000_000.0) * rate_out
        + (cache_creation_tokens as f64 / 1_000_000.0) * rate_in * resolved.cache_write_multiplier
        + (cache_read_tokens as f64 / 1_000_000.0) * rate_in * resolved.cache_read_multiplier
}

fn round_cents(v: f64) -> f64 {
    (v * 100.0).round() / 100.0
}

struct SpendRow {
    account_id: Option<Uuid>,
    name: String,
    /// `Some("personal" | "org")` for a real account row; `None` for the
    /// collapsed `other_accounts` row — that row's tail can span both
    /// planes, and there is no single truthful plane to report for it.
    /// `null` on the wire is the correct "not applicable / mixed" signal
    /// here, never inferred toward one plane or the other.
    plane: Option<&'static str>,
    input_tokens: i64,
    output_tokens: i64,
    cost_usd: f64,
    model_mix: Value,
}

impl SpendRow {
    fn to_json(&self, row_kind: &str, accounts_rolled_up: Option<i64>) -> Value {
        json!({
            "row_kind": row_kind,
            "account_id": self.account_id,
            "name": self.name,
            "plane": self.plane,
            "input_tokens": self.input_tokens,
            "output_tokens": self.output_tokens,
            "cost_usd": round_cents(self.cost_usd),
            "model_mix": self.model_mix,
            "accounts_rolled_up": accounts_rolled_up,
        })
    }
}

/// `admin_spend`'s `freshness` block. Pulled out as its own pure function so
/// the one regression that matters — the "nightly" case must NEVER
/// synthesize a timestamp — is directly unit-testable without a live
/// Postgres (see `spend_freshness_nightly_never_run_is_null` below), rather
/// than only reachable through a real `platform_metrics_daily` row whose
/// `computed_at` (`NOT NULL DEFAULT now()`, migration `0095`) makes `None`
/// hard to seed honestly end-to-end.
///
/// `freshness.at` is `string | null` in the contract (`Freshness`,
/// admin/lib/api-contract.ts) specifically so the "nightly" case can say
/// "never yet computed" honestly. Falling back to `Utc::now()` here when
/// `rollup_at` is `None` — the previous behavior — is the same bug class
/// review already caught once on this PR for `admin_overview`'s `conversion`
/// block ("emitting null, never a fabricated zero", see that comment above):
/// an org whose nightly rollup has NEVER RUN would report as "fresh right
/// now", indistinguishable from an org whose rollup ran seconds ago. That is
/// precisely the failure this freshness badge exists to let an operator
/// catch. So: pass `rollup_at` straight through, `None` stays `None` — never
/// synthesize a timestamp for the nightly case. The `live`/`collecting`
/// cases are unaffected: they aren't backed by a rollup at all, so
/// `Utc::now()` there is an honest "computed live, right now", not a
/// fabrication.
fn spend_freshness(org_count: usize, personal_count: usize, rollup_at: Option<DateTime<Utc>>) -> (&'static str, Option<String>) {
    if org_count > 0 {
        ("nightly", rollup_at.map(|t| t.to_rfc3339()))
    } else if personal_count > 0 {
        ("live", Some(Utc::now().to_rfc3339()))
    } else {
        ("collecting", Some(Utc::now().to_rfc3339()))
    }
}

/// Per-account LLM token/cost report for a calendar month.
///
/// Personal-plane sums come from `llm_usage`, looped per-account through
/// `begin_account_scoped` — `llm_usage` IS RLS-covered (migration `0083`), so
/// an unscoped cross-account read hard-errors post-OPS-4 under the non-
/// `BYPASSRLS` `coralswarm_app` role (identical shape/rationale to
/// `backfill_atom_metadata` above). Org-plane sums come from
/// `platform_metrics_daily` (the sibling Unit E rollup worker's output),
/// never a tenant pool directly — a single request must not span planes.
///
/// The long tail is collapsed into exactly ONE `other_accounts` row, whose
/// members may span both planes — `plane` is `null` for this row (never
/// inferred toward one plane), because a plausible-but-wrong plane label
/// would be worse than reporting the genuine absence of a single answer.
/// `account_id` is also null; `accounts_rolled_up` carries the true count,
/// and the row's `input_tokens`/`output_tokens`/`cost_usd` still cover the
/// ENTIRE tail (both planes combined) — `total` reconciles against the sum
/// of every row including this one.
pub async fn admin_spend(
    State(state): State<Arc<AppState>>,
    Query(query): Query<SpendQuery>,
    headers: HeaderMap,
) -> (StatusCode, Json<Value>) {
    if !check_internal_token(&headers) {
        return (
            StatusCode::UNAUTHORIZED,
            Json(json!({"error": "missing or invalid X-Internal-Token"})),
        );
    }
    let Some((start, end)) = parse_month_strict(&query.month) else {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "month must be a valid YYYY-MM"})),
        );
    };
    let pool = match &state.db {
        Some(p) => p.clone(),
        None => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({"error": "service unavailable"})),
            )
        }
    };

    // ── Rate sheet: snapshot ONCE, pass by reference into every cost call
    // below (usage-metering PR1's "critical shape" — see
    // `crate::billing::rates`'s module doc). A per-account or per-row reload
    // here would both regress the 45s-budgeted personal-account loop further
    // down and defeat the whole point of `estimate_cost_usd` staying a pure,
    // no-I/O function. Fails loud like every other headline query in this
    // handler: a `model_rates` read failure must 500, never silently price
    // every row off some earlier/default sheet.
    let price_sheet: PriceSheet = fail_loud!(PriceSheet::load(&pool).await, "spend.price_sheet");

    // ── Personal-plane per-account sums (loop-per-account under RLS) ────────
    // Fails loud on the account roster itself — an empty `personal_accounts`
    // due to a query error would silently make `total` look like "no personal
    // spend this month" rather than "we couldn't compute this."
    //
    // Includes a personal org's own account (control migration 0208
    // `personal_orgs`) alongside plain `clerk_org_id IS NULL` ones: adoption
    // (`bin/adopt_personal_org_account.rs`) relabels a user's ORDINARY
    // personal account with an org id, so `clerk_org_id IS NULL` ALONE would
    // silently drop every adopted account from this roster. Its `llm_usage`
    // rows never move planes — they stay on THIS same account id, on the
    // shared consumer database `begin_account_scoped` below already reads —
    // so it belongs in this loop, not the org-plane `tenant_databases` one
    // further down (a personal org has no row there at all, by design; see
    // `0208_personal_orgs.sql`'s own doc comment).
    let personal_accounts: Vec<(Uuid, Option<String>, String)> = fail_loud!(
        sqlx::query_as(
            "SELECT a.id, u.name, u.email \
             FROM accounts a JOIN users u ON u.id = a.owner_user_id \
             WHERE a.clerk_org_id IS NULL \
                OR a.clerk_org_id IN (SELECT clerk_org_id FROM personal_orgs) \
             ORDER BY a.id",
        )
        .fetch_all(&pool)
        .await,
        "spend.personal_accounts"
    );

    // The whole loop below (every account's `begin_account_scoped` transaction
    // + `llm_usage` query, combined) runs under ONE hard timeout rather than
    // per-account timeouts — see [`SPEND_PERSONAL_LOOP_TIMEOUT`]'s doc comment
    // for why this loop cannot instead be collapsed into a single grouped
    // query (RLS's one-GUC-value-per-transaction scoping). This is the
    // sanctioned mitigation for the loop tying up the admin request
    // indefinitely as the personal-account roster grows (CodeRabbit review,
    // PR #361): bound the total wall-clock cost, don't weaken the scoping.
    let personal_loop = async {
        let mut all_rows: Vec<SpendRow> = Vec::with_capacity(personal_accounts.len());
        for (account_id, name, email) in &personal_accounts {
            // Fails the WHOLE request loudly on the first broken account — the
            // same discipline `backfill_atom_metadata` above already uses for
            // this exact per-account-loop shape ("Fail the run loudly at the
            // first broken account… the endpoint is idempotent"). Silently
            // `continue`-ing past a broken account (the old behavior here) would
            // silently UNDERCOUNT `total`: a partial sum is exactly as dangerous
            // as a fabricated zero, since nothing in the response would say an
            // account's spend is missing from it.
            let mut tx = match begin_account_scoped(&pool, *account_id).await {
                Ok(tx) => tx,
                Err(e) => {
                    tracing::error!(error = %e, account_id = %account_id, "admin_spend: begin_account_scoped failed");
                    return Err(());
                }
            };
            // `Ok(rows)` where `rows` is empty means this account genuinely spent
            // nothing this month — a real, honest result. `Err(...)` means the
            // query itself failed (schema mismatch, connection issue) and must
            // NEVER be treated the same way as "genuinely nothing" — that
            // conflation is exactly the bug class review round 2 caught in
            // `admin_overview`'s conversion funnel.
            let usage: Vec<(String, i64, i64, i64, i64)> = match sqlx::query_as(
                // `cache_creation_input_tokens`/`cache_read_input_tokens` are
                // BIGINT columns (migration `0073`) — `SUM(bigint)` returns
                // NUMERIC in Postgres (overflow-safety default), which sqlx
                // won't decode into `i64` without an explicit cast back down.
                // Same pitfall the org_usage query below already casts
                // around; missing it here 500s the whole endpoint at runtime
                // (a decode error, not a SQL syntax error, so `cargo build`
                // and `--no-run` compiles don't catch it — only an actual
                // query against real Postgres does).
                "SELECT model, COALESCE(SUM(input_tokens), 0), COALESCE(SUM(output_tokens), 0), \
                        COALESCE(SUM(cache_creation_input_tokens), 0)::bigint, \
                        COALESCE(SUM(cache_read_input_tokens), 0)::bigint \
                 FROM llm_usage \
                 WHERE account_id = $1 AND created_at >= $2 AND created_at < $3 \
                 GROUP BY model",
            )
            .bind(account_id)
            .bind(start)
            .bind(end)
            .fetch_all(&mut *tx)
            .await
            {
                Ok(rows) => rows,
                Err(e) => {
                    tracing::error!(error = %e, account_id = %account_id, "admin_spend: llm_usage query failed");
                    return Err(());
                }
            };
            let _ = tx.commit().await;

            if usage.is_empty() {
                continue;
            }
            let mut input_total = 0i64;
            let mut output_total = 0i64;
            let mut cost_total = 0f64;
            let mut model_mix = Map::new();
            for (model, input_tokens, output_tokens, cache_creation_tokens, cache_read_tokens) in &usage {
                input_total += input_tokens;
                output_total += output_tokens;
                cost_total += estimate_cost_usd(
                    *input_tokens,
                    *output_tokens,
                    *cache_creation_tokens,
                    *cache_read_tokens,
                    model,
                    start,
                    &price_sheet,
                );
                model_mix.insert(model.clone(), json!(input_tokens + output_tokens));
            }
            all_rows.push(SpendRow {
                account_id: Some(*account_id),
                name: name.clone().unwrap_or_else(|| email.clone()),
                plane: Some("personal"),
                input_tokens: input_total,
                output_tokens: output_total,
                cost_usd: cost_total,
                model_mix: Value::Object(model_mix),
            });
        }
        Ok(all_rows)
    };

    let mut all_rows: Vec<SpendRow> = match tokio::time::timeout(SPEND_PERSONAL_LOOP_TIMEOUT, personal_loop).await {
        Ok(Ok(rows)) => rows,
        Ok(Err(())) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            );
        }
        Err(_) => {
            tracing::error!(
                personal_accounts = personal_accounts.len(),
                timeout_secs = SPEND_PERSONAL_LOOP_TIMEOUT.as_secs(),
                "admin_spend: personal-account llm_usage loop timed out"
            );
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            );
        }
    };
    let personal_count = all_rows.len();

    // ── Org-plane sums, from the rollup table only ───────────────────────────
    // No per-model breakdown exists at this grain, so `model_mix` stays empty
    // and cost uses `price_sheet`'s blended `is_blend_fallback` row (routed
    // to via the `UNATTRIBUTED_SENTINEL` model string — see that constant's
    // doc comment).
    // Fails loud — same reasoning as `personal_accounts` above: a silently
    // empty `org_usage` due to a query error would look identical to "no org
    // spent anything this month" in the final `total`.
    // `llm_cache_creation_tokens`/`llm_cache_read_tokens` (migration
    // `0123_platform_metrics_daily_cache_tokens.sql`) are nullable: a row is
    // `NULL` there iff `workers::platform_rollup` has not (re)written it
    // since that migration shipped, `0` iff the cache-aware rollup ran and
    // there really was no cache activity that day. Plain `SUM(...)` already
    // ignores NULLs per standard SQL, so the outer `COALESCE(..., 0)::bigint`
    // gives the BEST-KNOWN total for whatever rows this tenant DOES have
    // cache data for — deliberately not tainting the whole sum to zero just
    // because one row is still unknown, the same "don't let one unknown
    // erase what IS known" reasoning `admin_spend`'s per-account loop above
    // already applies. That best-known sum can still silently UNDERSTATE
    // real spend if some of this tenant's rows this month are pre-cache-aware
    // NULLs with real (nonzero) token usage — the `COUNT(*) FILTER` below is
    // exactly the signal that lets the response say so out loud instead of
    // just presenting a plausible-looking (but incomplete) number.
    let org_usage: Vec<(Uuid, String, i64, i64, i64, i64, i64)> = fail_loud!(
        sqlx::query_as(
            // `llm_input_tokens`/`llm_output_tokens`/`llm_cache_creation_
            // tokens`/`llm_cache_read_tokens` are all BIGINT — `SUM(bigint)`
            // returns NUMERIC in Postgres (overflow-safety default), which
            // sqlx won't decode into `i64` without an explicit cast back
            // down.
            "SELECT td.account_id, td.slug, \
                    COALESCE(SUM(pmd.llm_input_tokens), 0)::bigint, \
                    COALESCE(SUM(pmd.llm_output_tokens), 0)::bigint, \
                    COALESCE(SUM(pmd.llm_cache_creation_tokens), 0)::bigint, \
                    COALESCE(SUM(pmd.llm_cache_read_tokens), 0)::bigint, \
                    COUNT(*) FILTER ( \
                        WHERE pmd.llm_cache_creation_tokens IS NULL \
                          AND (COALESCE(pmd.llm_input_tokens, 0) > 0 OR COALESCE(pmd.llm_output_tokens, 0) > 0) \
                    )::bigint \
             FROM tenant_databases td \
             JOIN platform_metrics_daily pmd ON pmd.tenant = td.clerk_org_id AND pmd.day >= $1 AND pmd.day < $2 \
             GROUP BY td.account_id, td.slug",
        )
        .bind(start.date_naive())
        .bind(end.date_naive())
        .fetch_all(&pool)
        .await,
        "spend.org_usage"
    );
    let org_count = org_usage.len();

    // True as soon as ANY org tenant has at least one pre-cache-aware-rollup
    // `(day, tenant)` row (`llm_cache_creation_tokens IS NULL`) with real
    // token usage this month — see `cache_pricing_is_complete`'s doc comment
    // for the full completeness contract this feeds.
    let mut any_org_row_missing_cache_data = false;

    for (account_id, slug, input_tokens, output_tokens, cache_creation_tokens, cache_read_tokens, incomplete_days) in
        org_usage
    {
        if incomplete_days > 0 {
            any_org_row_missing_cache_data = true;
        }
        all_rows.push(SpendRow {
            account_id: Some(account_id),
            name: slug,
            plane: Some("org"),
            input_tokens,
            output_tokens,
            // Real cache sums now, wired straight from `platform_metrics_
            // daily` (migration `0123`) — see this branch's own comment
            // above for how NULL rows are handled (best-known partial sum,
            // flagged via `any_org_row_missing_cache_data` rather than
            // silently ignored).
            cost_usd: estimate_cost_usd(
                input_tokens,
                output_tokens,
                cache_creation_tokens,
                cache_read_tokens,
                UNATTRIBUTED_SENTINEL,
                start,
                &price_sheet,
            ),
            model_mix: json!({}),
        });
    }

    // ── Rank, split top-N / tail, collapse the tail ─────────────────────────
    all_rows.sort_by(|a, b| b.cost_usd.partial_cmp(&a.cost_usd).unwrap_or(std::cmp::Ordering::Equal));

    let total_input: i64 = all_rows.iter().map(|r| r.input_tokens).sum();
    let total_output: i64 = all_rows.iter().map(|r| r.output_tokens).sum();
    let total_cost: f64 = all_rows.iter().map(|r| r.cost_usd).sum();
    let total_accounts = all_rows.len() as i64;

    let tail: Vec<SpendRow> = if all_rows.len() > SPEND_TOP_N {
        all_rows.split_off(SPEND_TOP_N)
    } else {
        Vec::new()
    };
    let top = all_rows;

    let mut rows_json: Vec<Value> = top.iter().map(|r| r.to_json("account", None)).collect();

    if !tail.is_empty() {
        // Covers the ENTIRE tail — both planes combined — so `total` still
        // reconciles against the sum of every row including this one.
        let tail_input: i64 = tail.iter().map(|r| r.input_tokens).sum();
        let tail_output: i64 = tail.iter().map(|r| r.output_tokens).sum();
        let tail_cost: f64 = tail.iter().map(|r| r.cost_usd).sum();

        let other = SpendRow {
            account_id: None,
            name: "Other accounts".to_string(),
            // `null`, never inferred — the tail can span both planes, and a
            // plausible-but-wrong plane label is worse than reporting the
            // genuine absence of a single answer.
            plane: None,
            input_tokens: tail_input,
            output_tokens: tail_output,
            cost_usd: tail_cost,
            model_mix: json!({}),
        };
        rows_json.push(other.to_json("other_accounts", Some(tail.len() as i64)));
    }

    let rollup_at: Option<DateTime<Utc>> = if org_count > 0 {
        sqlx::query_scalar(
            "SELECT MAX(computed_at) FROM platform_metrics_daily WHERE day >= $1 AND day < $2",
        )
        .bind(start.date_naive())
        .bind(end.date_naive())
        .fetch_one(&pool)
        .await
        .unwrap_or(None)
    } else {
        None
    };
    let (freshness_kind, freshness_at) = spend_freshness(org_count, personal_count, rollup_at);

    (
        StatusCode::OK,
        Json(json!({
            "rows": rows_json,
            "total": {
                "input_tokens": total_input,
                "output_tokens": total_output,
                "cost_usd": round_cents(total_cost),
                "accounts": total_accounts,
            },
            "freshness": { "kind": freshness_kind, "at": freshness_at },
            // Personal-plane rows are always fully cache-aware (`llm_usage`
            // has no nullable cache columns — migration `0073`). Org-plane
            // rows are cache-aware too as of migration `0123`, but only for
            // `(day, tenant)` rows `workers::platform_rollup` has (re)written
            // since that migration shipped; a row from before that is a
            // genuine `NULL`, priced as "0 cache tokens" in the best-known
            // sum above even though the truth is "unknown", not "verified
            // zero". `cache_pricing_is_complete` is the precise signal for
            // whether that gap actually affects this month's numbers.
            "cache_pricing": {
                "complete": cache_pricing_is_complete(org_count, any_org_row_missing_cache_data),
                "note": cache_pricing_note(org_count, any_org_row_missing_cache_data),
            },
        })),
    )
}

/// `cache_pricing.complete` — see `admin_spend`'s doc comment for the two
/// planes' different cache-completeness stories. `org_count == 0` covers the
/// pre-`0123` behavior (no org accounts at all this month means nothing to
/// be incomplete about); `any_org_row_missing_cache_data` is the NEW signal
/// this migration adds — true when at least one org tenant's month includes
/// a `(day, tenant)` row with `llm_cache_creation_tokens IS NULL` (predates
/// the cache-aware rollup) that ALSO had nonzero token usage that day (a
/// NULL row with genuinely zero everything doesn't change the total either
/// way, so it must not taint completeness). Pulled out as its own pure
/// function so this exact boolean logic is unit-testable without a live
/// Postgres, mirroring `spend_freshness` above.
fn cache_pricing_is_complete(org_count: usize, any_org_row_missing_cache_data: bool) -> bool {
    org_count == 0 || !any_org_row_missing_cache_data
}

/// Companion to [`cache_pricing_is_complete`] — names both possible reasons
/// `complete` can end up `false` for org-plane data: this endpoint's
/// deployment predates the cache-aware rollup entirely (impossible in
/// practice once `0123` has shipped everywhere, but named for completeness),
/// or `0123` has shipped and the rollup simply hasn't recomputed some
/// already-closed historical day yet (the expected, common case — see
/// `platform_metrics_daily`'s own migration doc for why that's an accepted
/// gap, not a bug).
fn cache_pricing_note(org_count: usize, any_org_row_missing_cache_data: bool) -> &'static str {
    if org_count == 0 {
        "no org accounts this month; cache pricing is fully accurate."
    } else if any_org_row_missing_cache_data {
        "at least one org tenant has a day this month that predates the cache-aware platform_metrics_daily rollup \
         (migration 0123) and had real token usage that day — its cache tokens are counted as 0 in cost_usd below \
         (not verified zero, genuinely unknown), so cost_usd for that tenant, and any total that includes it, may \
         underestimate. This resolves itself as workers::platform_rollup recomputes each affected day; already-closed \
         historical days are not backfilled by this rollup and will remain unknown until a separate backfill runs."
    } else {
        "every org tenant's rows this month were computed by the cache-aware platform_metrics_daily rollup (migration \
         0123); cost_usd includes real Anthropic prompt-cache pricing for both planes."
    }
}

// ── GET /internal/admin/credits ───────────────────────────────────────────────

/// One row per `accounts` entry: `(id, clerk_org_id, user_name, user_email,
/// org_slug)`. `accounts`/`users`/`tenant_databases` carry no RLS policy
/// (module doc, "Control/identity tables … deliberately OUTSIDE the RLS
/// roster"), so this roster read is a plain unscoped query — only the
/// per-account `credit_balances`/`enforcement_events` reads below need
/// [`begin_account_scoped`].
type CreditAccountRosterRow = (Uuid, Option<String>, Option<String>, Option<String>, Option<String>);

/// Same budget/shape as [`SPEND_PERSONAL_LOOP_TIMEOUT`] — this endpoint's
/// loop is structurally identical (one `begin_account_scoped` transaction
/// per account), just against a different pair of tables.
const CREDITS_LOOP_TIMEOUT: Duration = Duration::from_secs(45);
/// Per-account cap while looping, so one noisy account can't dominate the
/// per-account round trip.
const CREDITS_EVENTS_PER_ACCOUNT: i64 = 5;
/// Final cross-fleet cap on the merged, newest-first event feed — same
/// "recent activity, not the full history" framing as
/// `admin_waitlist`'s `WAITLIST_DEFAULT_LIMIT`.
const CREDITS_EVENTS_TOTAL: usize = 50;

/// One account's current balance for the admin credits view.
struct CreditBalanceRow {
    account_id: Uuid,
    name: String,
    plane: &'static str,
    balance: f64,
    updated_at: DateTime<Utc>,
}

impl CreditBalanceRow {
    fn to_json(&self) -> Value {
        json!({
            "account_id": self.account_id,
            "name": self.name,
            "plane": self.plane,
            "balance": self.balance,
            "updated_at": self.updated_at.to_rfc3339(),
        })
    }
}

/// One `enforcement_events` row for the admin credits view. **Observational
/// only** — see [`admin_credits`]'s doc comment. `would_have_blocked` is
/// what the (currently never-enforced) flag would have done, not a record of
/// anything actually being blocked.
struct CreditEnforcementEventRow {
    account_id: Uuid,
    name: String,
    would_have_blocked: bool,
    reason: String,
    created_at: DateTime<Utc>,
}

impl CreditEnforcementEventRow {
    fn to_json(&self) -> Value {
        json!({
            "account_id": self.account_id,
            "name": self.name,
            "would_have_blocked": self.would_have_blocked,
            "reason": self.reason,
            "created_at": self.created_at.to_rfc3339(),
        })
    }
}

/// Resolves an account's admin-display name + plane — personal: `users.name`
/// falling back to email; org: `tenant_databases.slug` falling back to the
/// raw Clerk org id. Same resolution `admin_spend`/`admin_fleet` already use,
/// pulled out as its own pure function so it's unit-testable without
/// Postgres (same discipline as `cache_pricing_is_complete`/`spend_freshness`
/// above).
fn credit_account_display(
    clerk_org_id: Option<&str>,
    user_name: Option<&str>,
    user_email: Option<&str>,
    org_slug: Option<&str>,
) -> (&'static str, String) {
    match clerk_org_id {
        Some(org_id) => ("org", org_slug.map(str::to_string).unwrap_or_else(|| org_id.to_string())),
        None => ("personal", user_name.or(user_email).unwrap_or("").to_string()),
    }
}

/// One account's `credit_balances` row (if any) + its most recent
/// `enforcement_events` rows (if any), read inside a single
/// `begin_account_scoped` transaction — `credit_balances`/`enforcement_events`
/// are RLS-covered as of migration `0150` (`account_isolation`, bare `USING
/// (account_id = current_setting('app.account_id')::uuid)`), so there is no
/// cross-account way to read either table; this is the one-account-at-a-time
/// unit [`admin_credits`]'s loop calls once per roster row.
///
/// `Ok((None, vec![]))` is the honest empty state for an account that has
/// never been debited, granted, or evaluated for enforcement — never a
/// fabricated `$0.00` row (mirrors `billing::enforcement::check_and_record`'s
/// own `Ok(None)` branch on the write side). `Err(())` means a QUERY failed
/// (connection issue, schema drift) and must never be treated the same way —
/// the caller fails the whole request loudly rather than silently
/// undercounting the fleet.
async fn credit_row_for_account(
    pool: &sqlx::PgPool,
    account_id: Uuid,
    plane: &'static str,
    name: &str,
) -> Result<(Option<CreditBalanceRow>, Vec<CreditEnforcementEventRow>), ()> {
    let mut tx = match begin_account_scoped(pool, account_id).await {
        Ok(tx) => tx,
        Err(e) => {
            tracing::error!(error = %e, account_id = %account_id, "admin_credits: begin_account_scoped failed");
            return Err(());
        }
    };

    // NUMERIC -> float8 cast IN SQL — the presentation edge. Never decode a
    // bare NUMERIC into f64 in Rust (a runtime `query_as` into `Option<f64>`
    // hard-errors); this response is JSON, where a float is the honest wire
    // type anyway. Same shape as `routes::usage::get_usage_summary`'s
    // `balance::float8` read (usage-metering PR12).
    let balance_row: Option<(f64, DateTime<Utc>)> = match sqlx::query_as(
        "SELECT balance::float8, updated_at FROM credit_balances WHERE account_id = $1",
    )
    .bind(account_id)
    .fetch_optional(&mut *tx)
    .await
    {
        Ok(r) => r,
        Err(e) => {
            tracing::error!(error = %e, account_id = %account_id, "admin_credits: credit_balances query failed");
            return Err(());
        }
    };

    let event_rows: Vec<(bool, String, DateTime<Utc>)> = match sqlx::query_as(
        "SELECT would_have_blocked, reason, created_at FROM enforcement_events \
         WHERE account_id = $1 ORDER BY created_at DESC LIMIT $2",
    )
    .bind(account_id)
    .bind(CREDITS_EVENTS_PER_ACCOUNT)
    .fetch_all(&mut *tx)
    .await
    {
        Ok(r) => r,
        Err(e) => {
            tracing::error!(error = %e, account_id = %account_id, "admin_credits: enforcement_events query failed");
            return Err(());
        }
    };

    // Read-only transaction — commit rather than let it implicitly roll back
    // on drop, matching `routes::usage::get_usage_summary`'s same-shaped
    // scoped reads.
    let _ = tx.commit().await;

    let balance = balance_row.map(|(balance, updated_at)| CreditBalanceRow {
        account_id,
        name: name.to_string(),
        plane,
        balance,
        updated_at,
    });
    let events = event_rows
        .into_iter()
        .map(|(would_have_blocked, reason, created_at)| CreditEnforcementEventRow {
            account_id,
            name: name.to_string(),
            would_have_blocked,
            reason,
            created_at,
        })
        .collect();
    Ok((balance, events))
}

/// Current `credit_balances` + recent `enforcement_events`, across the whole
/// fleet — the admin surface for usage-metering PR8/PR10's ledger (plan §1
/// PR13: "Show: current credit balances, and would-have-blocked enforcement
/// events").
///
/// **Observation only — this endpoint does not mean anything was blocked.**
/// `billing::enforcement`'s flag is observe-only by construction: no PR in
/// this 13-PR train gates enrichment or reads on it (see that module's own
/// doc comment, "This module never blocks anything"). `would_have_blocked`
/// below records what enforcement WOULD have done had it been live, not a
/// record of an actual block or deferral — the admin app's section
/// component labels this explicitly so it cannot be read as the opposite.
///
/// `credit_ledger`/`credit_balances`/`grants`/`enforcement_events` are all
/// RLS-covered as of migration `0150` (`account_isolation`, bare `USING
/// (account_id = current_setting('app.account_id')::uuid)`) — unlike every
/// other table this file reads, which are deliberately outside the RLS
/// roster (module doc, "Control/identity tables"). There is no cross-account
/// bypass for that policy on the control plane (unlike the tenant plane's
/// `begin_tenant_system_scoped`), so a fleet-wide view is only reachable by
/// looping one `begin_account_scoped` transaction per account — see
/// [`credit_row_for_account`] — under the same bounded-timeout, fail-the-
/// whole-request-loudly discipline `admin_spend`'s personal-plane loop
/// already established (CodeRabbit review, PR #361).
///
/// An account with no `credit_balances` row and no `enforcement_events` row
/// is simply absent from both lists — never a fabricated zero-balance line.
pub async fn admin_credits(State(state): State<Arc<AppState>>, headers: HeaderMap) -> (StatusCode, Json<Value>) {
    if !check_internal_token(&headers) {
        return (
            StatusCode::UNAUTHORIZED,
            Json(json!({"error": "missing or invalid X-Internal-Token"})),
        );
    }
    let pool = match &state.db {
        Some(p) => p.clone(),
        None => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({"error": "service unavailable"})),
            )
        }
    };

    // Fails loud rather than reporting an empty fleet — same discipline as
    // `admin_spend`'s `personal_accounts` roster read.
    let accounts: Vec<CreditAccountRosterRow> = fail_loud!(
        sqlx::query_as(
            "SELECT a.id, a.clerk_org_id, u.name, u.email, td.slug \
             FROM accounts a \
             LEFT JOIN users u ON u.id = a.owner_user_id \
             LEFT JOIN tenant_databases td ON td.clerk_org_id = a.clerk_org_id \
             ORDER BY a.id",
        )
        .fetch_all(&pool)
        .await,
        "credits.accounts"
    );

    let loop_fut = async {
        let mut balances: Vec<CreditBalanceRow> = Vec::new();
        let mut events: Vec<CreditEnforcementEventRow> = Vec::new();
        for (account_id, clerk_org_id, user_name, user_email, org_slug) in &accounts {
            let (plane, name) = credit_account_display(
                clerk_org_id.as_deref(),
                user_name.as_deref(),
                user_email.as_deref(),
                org_slug.as_deref(),
            );
            let (balance, mut acct_events) =
                credit_row_for_account(&pool, *account_id, plane, &name).await?;
            if let Some(b) = balance {
                balances.push(b);
            }
            events.append(&mut acct_events);
        }
        Ok::<_, ()>((balances, events))
    };

    let (mut balances, mut events) = match tokio::time::timeout(CREDITS_LOOP_TIMEOUT, loop_fut).await {
        Ok(Ok(v)) => v,
        Ok(Err(())) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            );
        }
        Err(_) => {
            tracing::error!(
                accounts = accounts.len(),
                timeout_secs = CREDITS_LOOP_TIMEOUT.as_secs(),
                "admin_credits: per-account loop timed out"
            );
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            );
        }
    };

    // Lowest balance first — an ops watchlist, not a leaderboard: the
    // accounts closest to (or already past) zero are the ones worth a human
    // looking at first.
    balances.sort_by(|a, b| a.balance.partial_cmp(&b.balance).unwrap_or(std::cmp::Ordering::Equal));
    events.sort_by(|a, b| b.created_at.cmp(&a.created_at));
    events.truncate(CREDITS_EVENTS_TOTAL);

    (
        StatusCode::OK,
        Json(json!({
            "balances": balances.iter().map(CreditBalanceRow::to_json).collect::<Vec<_>>(),
            "enforcement_events": events.iter().map(CreditEnforcementEventRow::to_json).collect::<Vec<_>>(),
            "freshness": { "kind": "live", "at": Utc::now().to_rfc3339() },
        })),
    )
}

// ── GET /internal/admin/fleet ─────────────────────────────────────────────────

/// One row per `tenant_databases` entry, with a live member count
/// (`account_members`) and `expected_schema_version` derived from the
/// embedded tenant migrator (`crate::tenancy::migrate::head_version`) rather
/// than hardcoded — the tenant tree moves (0009 today) and this must track it
/// without a code change here. A row with `schema_version <
/// expected_schema_version` is the known 503 failure mode (the schema-version
/// gate); this endpoint just reports both numbers accurately, the caller
/// (admin app, PR #360) flags the mismatch.
pub async fn admin_fleet(State(state): State<Arc<AppState>>, headers: HeaderMap) -> (StatusCode, Json<Value>) {
    if !check_internal_token(&headers) {
        return (
            StatusCode::UNAUTHORIZED,
            Json(json!({"error": "missing or invalid X-Internal-Token"})),
        );
    }
    let Some(pool) = &state.db else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error": "service unavailable"})),
        );
    };

    /// `(clerk_org_id, slug, status, schema_version, region, members, created_at)`.
    type FleetRow = (String, String, String, Option<i64>, Option<String>, i64, DateTime<Utc>);
    // Fails loud rather than reporting an empty fleet — an ops "list of
    // orgs" dashboard reading `rows: []` as fact ("no orgs exist") when the
    // query actually errored is exactly the swallowed-error bug class this
    // file no longer allows (see `fail_loud!`'s doc comment).
    let rows: Vec<FleetRow> = fail_loud!(
        sqlx::query_as(
            "SELECT td.clerk_org_id, td.slug, td.status, td.schema_version, td.region, \
                    COUNT(am.user_id) AS members, td.created_at \
             FROM tenant_databases td \
             LEFT JOIN account_members am ON am.account_id = td.account_id \
             GROUP BY td.clerk_org_id, td.slug, td.status, td.schema_version, td.region, td.created_at \
             ORDER BY td.created_at",
        )
        .fetch_all(pool)
        .await,
        "fleet.rows"
    );

    let expected_schema_version = crate::tenancy::migrate::head_version();

    let rows_json: Vec<Value> = rows
        .into_iter()
        .map(|(clerk_org_id, slug, status, schema_version, region, members, created_at)| {
            json!({
                "slug": slug,
                "clerk_org_id": clerk_org_id,
                "status": status,
                "schema_version": schema_version,
                "expected_schema_version": expected_schema_version,
                "region": region,
                "members": members,
                "created_at": created_at.to_rfc3339(),
            })
        })
        .collect();

    (
        StatusCode::OK,
        Json(json!({
            "rows": rows_json,
            "freshness": { "kind": "live", "at": Utc::now().to_rfc3339() },
        })),
    )
}

// ── GET /internal/admin/waitlist?limit=50 ────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct WaitlistQuery {
    pub limit: Option<i64>,
}

const WAITLIST_DEFAULT_LIMIT: i64 = 50;
const WAITLIST_MAX_LIMIT: i64 = 200;

/// Raw waitlist rows with an exact normalized-email `converted` flag.
///
/// Column notes (fixed in review round 2 — the actual `waitlist_snapshot`
/// schema, migration `0096`, has NO `email`/`created_at` columns at all):
/// `email_raw` is displayed as the contract's `email` field (an operator
/// wants the address as D1 has it typed, not the normalized join key);
/// `signed_up_at` is the contract's `created_at`. The join/`converted` check
/// compares `lower(trim(users.email))` against `w.email_normalized`
/// DIRECTLY — that column is already normalized at write time and is the
/// table's PRIMARY KEY, so wrapping it in `lower(trim(...))` too would defeat
/// the PK index. `total` fails loud on error (see [`fail_loud!`]) rather than
/// silently reporting zero signups.
pub async fn admin_waitlist(
    State(state): State<Arc<AppState>>,
    Query(query): Query<WaitlistQuery>,
    headers: HeaderMap,
) -> (StatusCode, Json<Value>) {
    if !check_internal_token(&headers) {
        return (
            StatusCode::UNAUTHORIZED,
            Json(json!({"error": "missing or invalid X-Internal-Token"})),
        );
    }
    let Some(pool) = &state.db else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error": "service unavailable"})),
        );
    };
    let limit = query.limit.unwrap_or(WAITLIST_DEFAULT_LIMIT).clamp(1, WAITLIST_MAX_LIMIT);

    /// `(email_raw, signed_up_at, converted, utm_source, country)`.
    /// `signed_up_at` is genuinely nullable in `waitlist_snapshot` (a
    /// defensively-parsed sync that failed to parse D1's `created_at` stores
    /// NULL rather than dropping the row) — reported as JSON `null`, never
    /// coerced to a fabricated timestamp.
    type WaitlistRow = (String, Option<DateTime<Utc>>, bool, Option<String>, Option<String>);
    let rows: Vec<WaitlistRow> = match sqlx::query_as(
        "SELECT w.email_raw, w.signed_up_at, \
                EXISTS(SELECT 1 FROM users u WHERE lower(trim(u.email)) = w.email_normalized) AS converted, \
                w.utm_source, w.country \
         FROM waitlist_snapshot w \
         ORDER BY w.signed_up_at DESC NULLS LAST \
         LIMIT $1",
    )
    .bind(limit)
    .fetch_all(pool)
    .await
    {
        Ok(r) => r,
        Err(e) => {
            tracing::error!(error = %e, "admin_waitlist: query failed");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            );
        }
    };

    let total: i64 = fail_loud!(
        sqlx::query_scalar("SELECT COUNT(*) FROM waitlist_snapshot").fetch_one(pool).await,
        "waitlist.total"
    );

    let rows_json: Vec<Value> = rows
        .into_iter()
        .map(|(email, signed_up_at, converted, utm_source, country)| {
            json!({
                "email": email,
                "created_at": signed_up_at.map(|t| t.to_rfc3339()),
                "converted": converted,
                "utm_source": utm_source,
                "country": country,
            })
        })
        .collect();

    (
        StatusCode::OK,
        Json(json!({
            "rows": rows_json,
            "total": total,
            "freshness": { "kind": "nightly", "at": Utc::now().to_rfc3339() },
        })),
    )
}

// ── GET /internal/admin/chain-health ─────────────────────────────────────────

/// Number of most-recent COMPLETED jobs, per `kind`, the outcome-distribution
/// check requires before it will render a verdict for that kind. Small enough
/// that even a moderate-volume worker accumulates a full window within a
/// single poll cycle or two; large enough that a handful of genuinely-empty
/// jobs in a row (a quiet ocean overnight, a burst of dedup no-ops) can't
/// read as "the worker is broken" on their own. See [`admin_chain_health`]'s
/// doc comment for the full rationale.
const NOTHING_TO_DO_WINDOW: i64 = 20;

/// Trailing-24h atom-creation floor the conservation check requires before it
/// will call a zero-edge count a violation at all — see [`admin_chain_health`].
const CONSERVATION_ATOM_FLOOR: i64 = 20;

/// Trailing window, in hours, for the structural-dead-letter check. Matches
/// the conservation check's 24h deliberately, so this endpoint has ONE window
/// concept rather than two independently-tunable ones — and so a violation
/// CLEARS once the cause is fixed, instead of pinning the endpoint red (and
/// the dead-man's switch silent) forever after a single dead job.
const STRUCTURAL_DEAD_WINDOW_HOURS: i64 = 24;

/// Trailing window for the read-only-poison check (COR-82). A SQLSTATE 25006
/// seen by the worker harness inside this window flips `healthy` to false; the
/// per-tenant counters stay visible for the process lifetime either way. The
/// window is what lets the alarm clear after the harness has healed the pool,
/// instead of pinning the endpoint red until the next deploy.
const READ_ONLY_ALARM_WINDOW: chrono::Duration = chrono::Duration::minutes(30);

/// The judgement graph's own kill trigger, as the rollout plan wrote it:
/// "more than 40% of the first 50 adjudications come back UNSURE" means the
/// three-band policy is mis-tuned and the feature is manufacturing review
/// work instead of absorbing it. [`UNSURE_RATE_WINDOW`] is that "first 50";
/// [`UNSURE_RATE_MAX_UNSURE`] is 40% of it, and the predicate is a STRICT `>`
/// so exactly 20/50 is the boundary the plan draws, not a violation.
///
/// Fixed at the FIRST 50 rather than a trailing window on purpose: this is a
/// one-shot verdict on the launch tuning, and a trailing window would let a
/// bad launch age out of view instead of staying flagged until someone
/// retunes.
const UNSURE_RATE_WINDOW: i64 = 50;
const UNSURE_RATE_MAX_UNSURE: i64 = 20;

/// One account's contribution to the kill trigger's window: its oldest
/// [`UNSURE_RATE_WINDOW`] LLM adjudications as `(created_at, judgement_id,
/// verdict)`. Binds `$1 = account_id`, `$2 = limit`.
///
/// A named const rather than an inline literal so
/// `backend/tests/judgement_graph_worker_pg.rs` can bind the REAL query text
/// against a real `reef_judgement` table. This repo's sqlx is
/// runtime-checked, so a typo'd column here compiles fine and fails only in
/// production — where [`fail_loud!`] correctly turns it into a `500`, which
/// is a detector that has itself gone dark. Re-exported at the crate root
/// with the route handlers for exactly that test.
pub const CHAIN_HEALTH_LLM_ADJUDICATION_WINDOW_SQL: &str =
    "SELECT created_at, judgement_id, verdict FROM reef_judgement \
     WHERE source = 'llm' \
       AND graph_id IN (SELECT id FROM graphs WHERE account_id = $1) \
     ORDER BY created_at ASC, judgement_id ASC \
     LIMIT $2";

/// The chain-level "did anything downstream of ingestion actually run" health
/// verdict — the detector for a real incident, not a hypothetical one.
///
/// # The incident (2026-08-04)
/// `ExtractRelationships` structurally could not read its own input, treated
/// that as "nothing to do" for every atom it was ever handed, and completed
/// every one of those jobs with `status = 'done'` — no error, no retry, no
/// dead-letter. All graph inference silently stopped for **three days**
/// while atoms kept being created normally and every dashboard this team
/// watches stayed green: `status = 'done'` looks byte-identical whether a
/// worker did real work or quietly did nothing at all. This endpoint is the
/// detector that would have caught it within the hour.
///
/// Live production check while building this endpoint (2026-08-07, read-only,
/// via the same `flyctl proxy` recipe `.github/workflows/backup-control-
/// plane.yml` uses) confirmed the incident is STILL PRESENT TENSE at the time
/// of writing: trailing 24h, 182-184 atoms created (small drift between two
/// back-to-back checks — atoms keep arriving), 0 `relationship_edge` created;
/// last edge ever created 2026-08-04 05:01 UTC; `ExtractRelationships` job
/// states 5,943 `done`, 2 `dead`. A **negative control** against a healthy
/// window before the outage (2026-08-01) returned 36 atoms / 29 edges — both
/// well clear of a violation. This endpoint's two queries were run directly
/// (not yet deployed) to produce both numbers — see this PR's description
/// for the full recipe.
///
/// # Four invariants, deliberately a small and assumption-free set
///
/// **Primary — chain-level conservation.** Trailing 24h: if `atoms` created
/// exceeds [`CONSERVATION_ATOM_FLOOR`] AND `relationship_edge` created is
/// exactly zero, the chain from ingestion to graph inference is broken
/// SOMEWHERE — exactly the invariant the 2026-08-04 incident violated for 3
/// days straight. It says nothing about WHICH worker; it only needs to know
/// content keeps arriving while nothing structural comes out the other end,
/// which is exactly the shape a `status='done'`-but-silently-empty worker
/// produces.
///
/// **Secondary — outcome distribution (cheap).** Alarms when a worker that has
/// previously reported `DidWork` shows a `NothingToDo` ratio of 100% over
/// [`NOTHING_TO_DO_WINDOW`] consecutive completed jobs. Reads `jobs.outcome`
/// (`'did_work'` | `'nothing_to_do'`, added by the parallel
/// `feat/typed-job-outcomes` migration — see "Schema/merge-order dependency"
/// below). Where conservation says *the chain is broken*, this says *which
/// worker* — and it has to key on `outcome`, not `status` or dead-letter/error
/// counts: the production numbers above show the broken worker completing
/// perfectly cleanly (5,943 `done` against only 2 unrelated `dead`), so
/// `jobs.status` alone can never catch this shape.
///
/// The "has previously reported `DidWork`" clause is not a softening — it is
/// what makes this invariant hold on a healthy system. A worker can be
/// correctly idle forever: `InferRelationships` reports `NothingToDo` on every
/// run in production because `ExtractRelationships` already materialises the
/// inferred edges it would otherwise close over. Alarming on "is idle" rather
/// than "**stopped**" would have shipped this endpoint permanently red, and a
/// permanently-red detector is indistinguishable from a broken one.
///
/// `ever_did_work` is not enough for every idle-by-design worker.
/// `InferExpertise` HAS reported `DidWork` (it wrote `expert_in` edges before
/// the 2026-08-13 assert-or-drop gate, and still does when a topic clears
/// both gates), so the regression guard treats a full window of `NothingToDo`
/// as a stall. That is the wrong reading. `ExtractRelationships` re-enqueues
/// `InferExpertise` after every edge write; the incrementality gate then
/// skips unchanged topics, and assert-or-drop writes zero edges for
/// everything under the bar. 100% `NothingToDo` has been the production
/// shape since 2026-09-10, with conservation healthy. See
/// [`outcome_idle_by_design`] and [`outcome_distribution_violations`].
///
/// **Tertiary — structural dead-letters (cheap).** Alarms when ANY job
/// reached `status = 'dead'` with `failure_class = 'structural'` in the
/// trailing [`STRUCTURAL_DEAD_WINDOW_HOURS`], grouped by `kind`. That pair is
/// a worker's own written declaration that it *cannot process its own input
/// by design* — `queue::could_not_do(.., FailureClass::Structural, ..)` is
/// the only thing that ever writes it, and it dead-letters on the FIRST
/// attempt rather than climbing the retry ladder (see `jobs::queue`). The
/// steady state is exactly zero, so this needs no threshold to tune: one
/// occurrence is the signal.
///
/// It exists because the other two invariants provably do not cover this
/// shape. `feat/typed-job-outcomes` reclassified the huddle worker's
/// `CaptureError::Terminal` (revoked Slack app, missing account DEK) from a
/// quiet `done` to a structural dead-letter — correct, and strictly better
/// than the silent completion it replaced, but it means a revoked Slack app
/// now accumulates one `dead` job per huddle. Conservation cannot see that
/// (atoms and edges both keep flowing), and outcome distribution cannot
/// either (it reads only `status = 'done'` rows, and these never reach
/// `done`). Without this check, a change made specifically to stop a worker
/// failing silently would have produced a NEW silent accumulation — the same
/// defect wearing a different status.
///
/// **Per-worker liveness was considered and explicitly REJECTED.** A "has
/// worker X polled/completed a job in the last N minutes" check sounds
/// appealing but adds a THIRD invariant, N more magic thresholds (one per
/// worker, each independently tunable and independently capable of being
/// wrong), and does not actually detect this incident's failure mode: the
/// worker in the 2026-08-04 incident polled and completed jobs constantly for
/// three days straight — it was never once "not alive". Liveness answers a
/// different question ("is the process running") than the one this incident
/// actually asks ("is the process's output real"). Keeping the invariant set
/// to these two is deliberate, not an oversight — a third invariant here does
/// not make the detector stronger, it makes it slower to reason about and
/// easier to get subtly wrong.
///
/// A per-worker dead-letter COUNT invariant (e.g. "alarm when
/// `RegenerateDigest` exceeds N dead jobs") was considered for the same
/// reason and still rejected: it needs a per-worker magic number, every one
/// of them independently tunable and independently capable of being wrong,
/// which is precisely the per-worker-threshold shape this endpoint refuses
/// to grow into. The structural check above is deliberately NOT that. It
/// keys on a worker's own explicit `FailureClass::Structural` declaration
/// rather than on a volume of ordinary failures, so its correct threshold is
/// zero for every kind, forever, with nothing to tune. Two kinds of dead row
/// leave `failure_class` NULL and are invisible to it: an ordinary `fail()`
/// that exhausts its attempts ladder, and a managed hold that cannot clear
/// (`jobs::queue::hold`, `error_message` `held: <code>`), which dead-letters
/// on its fourth hold in a row. Held `RegenerateDigest` jobs used to do
/// neither: each hold completed the job and enqueued a fresh one every 15
/// minutes, forever, so they showed up as ~43,000 `done` rows a day rather
/// than as dead ones (COR-114).
///
/// **Quaternary — judgement-graph UNSURE rate (the rollout kill trigger).**
/// The judgement-graph rollout plan named one number that would end the
/// experiment: more than 40% of the first 50 LLM adjudications coming back
/// UNSURE means the three-band policy is mis-tuned and the feature is
/// manufacturing human review work rather than absorbing it. That number was
/// prose in a plan document, i.e. a thing somebody has to remember to go
/// measure. Here it is an invariant on the same hourly poller / dead-man's
/// switch (#571) as the other three.
///
/// The window is the FIRST [`UNSURE_RATE_WINDOW`] `reef_judgement` rows with
/// `source = 'llm'`, ordered `(created_at ASC, judgement_id ASC)`, and it
/// **includes rows that were later annulled**: the adjudication is the event
/// being measured, and annulment is lifecycle that happens to it afterwards
/// (supersession by a human ruling, expiry, an unmerge). Excluding annulled
/// rows would make the trigger quieter precisely as humans worked through the
/// queue — the metric would improve because the problem was being paid for.
///
/// SILENT below a full window: with fewer than [`UNSURE_RATE_WINDOW`] LLM
/// adjudications on record there is no verdict to render, and the check
/// reports healthy — the same "judge only a full sample" discipline the
/// outcome-distribution query's `HAVING COUNT(*) = $1` enforces.
///
/// Human-, identifier- and backfill-sourced rows are out of the window by
/// construction (`source = 'llm'` only): the trigger is about how well the
/// MODEL adjudicates, and a human writing NEGATIVE from the dismiss route is
/// not evidence about that.
///
/// Same control-plane scope caveat as the rest of this endpoint (see the
/// "Scope note" below): org tenant pools have their own `reef_judgement`
/// tables and are not counted here.
///
/// Alone among this endpoint's four invariants, this one does NOT
/// [`fail_loud!`]. It is the newest, it is the only one that reads
/// `reef_judgement`, and a 500 here would take the three incident-detecting
/// invariants down with it — see its read site for the full argument. When
/// its window cannot be read it reports `available: false` and logs at
/// `error!`, and `healthy` is decided by the other three alone.
///
/// # Schema/merge-order dependency
/// `jobs.outcome` is added by the parallel `feat/typed-job-outcomes` PR, in
/// both migration trees — this endpoint adds NO migration of its own and
/// MUST merge AFTER that PR. This repo's `sqlx::query`/`query_as` are
/// runtime-checked (no compile-time `query!` macro), so this file compiles
/// fine before that column exists; the outcome-distribution query would only
/// ever fail at RUNTIME pre-merge-order, which [`fail_loud!`] turns into a
/// `500` rather than a false "healthy".
///
/// # Consumer-plane RLS scoping
/// `atoms` and `relationship_edge` are both on migration `0083_consumer_rls`'s
/// per-account policy roster (empirically confirmed against production
/// 2026-08-07: an unscoped `SELECT count(*) FROM atoms` against the live
/// `coralswarm_app` role hard-errors with `unrecognized configuration
/// parameter "app.account_id"` rather than silently returning every
/// account's rows — RLS is genuinely enforcing, not a no-op). The
/// conservation check therefore loops per-account through
/// `begin_account_scoped`, exactly like [`backfill_atom_metadata`]/
/// `workers::platform_rollup`'s consumer branch (this file's module doc,
/// "Consumer-plane RLS scoping"), with an explicit app-layer `graph_id IN
/// (SELECT id FROM graphs WHERE account_id = $1)` filter as the PRIMARY
/// scoping mechanism and RLS as the backstop — the same "app-layer WHERE
/// clause first, RLS second" convention [`backfill_atom_metadata`]'s own doc
/// comment explains and justifies. `jobs` (the outcome-distribution check) is
/// a control table OUTSIDE the RLS roster, so that half runs as one plain
/// unscoped query.
///
/// `reef_judgement` (the UNSURE-rate check) is ALSO RLS-covered — migration
/// `0139_reef_judgement`'s own `account_isolation` policy, anchored directly
/// on `graph_id` — so it rides the SAME per-account loop rather than adding a
/// second one. Its window is global, not per-account, which the loop cannot
/// express in SQL: each account contributes its own oldest
/// [`UNSURE_RATE_WINDOW`] rows and [`first_llm_adjudications`] merges them in
/// Rust. That is exact, not an approximation — the globally-oldest N rows are
/// always a subset of the union of each account's own oldest N.
///
/// **Scope note:** this covers the CONTROL-PLANE / consumer content plane
/// only, not org-tenant pools (each org tenant is a wholly separate database
/// — `tenancy::scope`'s module doc, "Both planes, two different boundaries").
/// `platform_metrics_daily`, the existing cross-tenant rollup, tracks atom/
/// reef/session counts only — no relationship-edge counts, no job outcomes —
/// so there is no sanctioned cross-tenant source for this specific pair of
/// facts without a new rollup column, which is out of scope here, and a
/// single request must not span planes (see `admin_spend`'s own note on
/// this). Extending coverage to org tenants is a flagged follow-up, not a
/// silent gap this endpoint pretends not to have.
///
/// **Capture admission (COR-71).** Every plane's latest
/// [`crate::capture_admission_health::PlaneReport`], read from the control
/// table `workers::capture_admission_health` writes, so this request still
/// never touches a tenant pool. Violated when a capture has waited past
/// [`crate::capture_admission_health::ALERT_AFTER`] with no decision behind
/// the wait, or when the consumer plane or an active org has no fresh report.
/// Only a stall (nothing moving a capture) or an unmeasured plane sets
/// `page`; a capture SLO breach makes `healthy` false without it. See
/// [`crate::capture_admission_health`]'s module doc for the two tiers.
///
/// Internal endpoint — requires `X-Internal-Token`, not session auth. Every
/// query here fails loud (`500`, via [`fail_loud!`]) rather than reporting
/// `"healthy": true` because a query it depends on silently returned nothing
/// — a health check that reports healthy because its OWN query failed is
/// precisely the bug this endpoint exists to kill.
pub async fn admin_chain_health(State(state): State<Arc<AppState>>, headers: HeaderMap) -> (StatusCode, Json<Value>) {
    if !check_internal_token(&headers) {
        return (
            StatusCode::UNAUTHORIZED,
            Json(json!({"error": "missing or invalid X-Internal-Token"})),
        );
    }
    let Some(pool) = &state.db else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error": "service unavailable"})),
        );
    };

    // ── Primary: chain-level conservation, trailing 24h ──────────────────────
    // Account roster — control-table read, outside the RLS roster, needs no
    // scope (same reasoning as `backfill_atom_metadata`'s identical read).
    let account_ids: Vec<Uuid> = fail_loud!(
        sqlx::query_scalar("SELECT id FROM accounts ORDER BY id")
            .fetch_all(pool)
            .await,
        "chain_health.accounts"
    );

    let mut atoms_24h: i64 = 0;
    let mut relationship_edges_24h: i64 = 0;
    // Per-account contributions to the GLOBAL first-N LLM-adjudication window
    // (quaternary invariant) — merged and truncated by
    // `first_llm_adjudications` after the loop. See the doc's RLS section for
    // why the window can't be a single query.
    // `None` once ANY account's window read fails — see the read site below.
    let mut llm_adjudications: Option<Vec<(DateTime<Utc>, Uuid, bool)>> = Some(Vec::new());
    for account_id in &account_ids {
        let mut tx = fail_loud!(
            begin_account_scoped(pool, *account_id).await,
            "chain_health.scope"
        );
        let atoms: i64 = fail_loud!(
            sqlx::query_scalar(
                "SELECT COUNT(*) FROM atoms \
                 WHERE created_at > now() - interval '24 hours' \
                   AND graph_id IN (SELECT id FROM graphs WHERE account_id = $1)",
            )
            .bind(account_id)
            .fetch_one(&mut *tx)
            .await,
            "chain_health.atoms"
        );
        let edges: i64 = fail_loud!(
            sqlx::query_scalar(
                "SELECT COUNT(*) FROM relationship_edge \
                 WHERE created_at > now() - interval '24 hours' \
                   AND graph_id IN (SELECT id FROM graphs WHERE account_id = $1)",
            )
            .bind(account_id)
            .fetch_one(&mut *tx)
            .await,
            "chain_health.relationship_edges"
        );
        // This account's oldest `UNSURE_RATE_WINDOW` LLM adjudications,
        // annulled rows INCLUDED (the doc's quaternary invariant explains
        // why). `ORDER BY created_at, judgement_id` matches the merge order
        // `first_llm_adjudications` applies, so the two can't disagree about
        // which rows are "first".
        //
        // Deliberately NOT `fail_loud!`, unlike every other query in this
        // handler (review round 1 of PR4): this is the NEWEST and least
        // load-bearing invariant, and it reads a table the other three do not
        // need. A `reef_judgement` that is unqueryable in ANY single account
        // context — a pre-0139 restore, a half-stamped dev database — would
        // otherwise 500 the whole endpoint and silence conservation,
        // outcome-distribution and structural-dead-letters with it. The
        // poller `curl -fsS`s this endpoint, so a 500 costs it the specific
        // violation of a REAL incident and hands it an opaque transport
        // failure instead. The kill trigger degrades alone, loudly, and the
        // other three still render and still drive `healthy`.
        //
        // One account's failure poisons the WHOLE window, not just its own
        // share: the window is the globally-oldest N rows, so a missing
        // account may be exactly the account holding them. Reporting a
        // verdict computed from what happened to succeed would be a confident
        // wrong answer, which is the failure mode this endpoint exists to
        // kill.
        match sqlx::query_as::<_, (DateTime<Utc>, Uuid, String)>(
            CHAIN_HEALTH_LLM_ADJUDICATION_WINDOW_SQL,
        )
        .bind(account_id)
        .bind(UNSURE_RATE_WINDOW)
        .fetch_all(&mut *tx)
        .await
        {
            Ok(judgements) => {
                if let Some(rows) = llm_adjudications.as_mut() {
                    rows.extend(judgements.into_iter().map(|(at, id, v)| (at, id, v == "unsure")));
                }
            }
            Err(e) => {
                tracing::error!(
                    error = %e, %account_id,
                    "chain health check: UNSURE-rate window unavailable — the judgement-graph \
                     kill trigger is not being evaluated (the other three invariants still are)"
                );
                llm_adjudications = None;
            }
        }
        // Read-only — no COMMIT needed; dropping `tx` rolls back, which is
        // exactly right for a transaction that never wrote anything.
        atoms_24h += atoms;
        relationship_edges_24h += edges;
    }

    let conservation_violated = chain_conservation_violated(atoms_24h, relationship_edges_24h);
    // `None` = at least one account's window read failed, so there is no
    // trustworthy verdict to render. Never a violation (an unevaluated
    // invariant must not masquerade as a passing one, and must not flip
    // `healthy` on the strength of an outage in a different subsystem) —
    // `available: false` in the response plus the `error!` above is how it
    // stays visible.
    let unsure_window = llm_adjudications.map(first_llm_adjudications);
    let unsure_rate_violated = unsure_window
        .is_some_and(|(total, unsure)| unsure_adjudication_rate_violated(total, unsure));

    // ── Secondary: outcome distribution, per worker kind ──────────────────────
    // `jobs` is a control table (outside the RLS roster) — one plain query,
    // not a per-account loop. `HAVING COUNT(*) = $1` means a kind only shows
    // up here once it actually has a FULL window of completed jobs to judge —
    // a worker with fewer than `NOTHING_TO_DO_WINDOW` completions ever (or
    // recently) is silently excluded rather than judged on a partial sample.
    // `ever_did_work` is the REGRESSION guard, and it is load-bearing: a full
    // window of `NothingToDo` is only evidence of a stall for a worker that
    // does work at all. `InferRelationships` (`workers::inference_closure`)
    // reports `DidWork` only when `run_closure` materialises a new edge, and in
    // production it has returned `did_work` ZERO times ever — because
    // `ExtractRelationships` already materialises the inferred edges itself
    // (measured 2026-08-07: 12 `origin='inferred'` rows written in the two
    // seconds before an `ExtractRelationships` job completed `did_work`, while
    // the nearest `InferRelationships` run 80s earlier reported
    // `nothing_to_do`). That worker is idle BY DESIGN, and without this guard
    // it pins this endpoint to `healthy=false` forever — which would also stop
    // the dead-man's switch pinging, so the one alert that survives a total
    // outage would already be crying wolf on day one.
    //
    // Computed over ALL history, not the window, and that is what makes it a
    // regression test rather than a coincidence: a row only reaches the
    // violation predicate with ZERO `did_work` inside the window, so an
    // `ever_did_work = true` necessarily means the work happened BEFORE it.
    //
    // KNOWN GAP, self-healing: `jobs.outcome` is only populated from the
    // `feat/typed-job-outcomes` deploy onward (2026-08-07 ~19:05 UTC), so a
    // worker that stalled BEFORE that has no `did_work` on record and is
    // exempted — a false negative. It closes as history accumulates. The
    // 2026-08-04 incident's own worker is unaffected: `ExtractRelationships`
    // already carries `did_work` rows.
    //
    // SECOND known gap, NOT self-healing: `InferExpertise` did work, then
    // product policy made `DidWork` rare (assert-or-drop + incrementality +
    // fan-out enqueue). `ever_did_work` cannot see that as idle-by-design.
    // [`outcome_idle_by_design`] names that kind so this detector stops
    // pinning `healthy=false` (and silencing the dead-man's switch) on a
    // follow-up whose success is typically zero writes. The 2026-08-04
    // worker is still fully covered: `ExtractRelationships` is not on that list.
    let outcome_rows: Vec<(String, i64, i64, bool)> = fail_loud!(
        sqlx::query_as(
            "WITH completed AS ( \
                SELECT kind, outcome, finished_at \
                FROM jobs \
                WHERE status = 'done' AND outcome IS NOT NULL \
             ), \
             recent AS ( \
                SELECT kind, outcome, \
                       ROW_NUMBER() OVER (PARTITION BY kind ORDER BY finished_at DESC) AS rn \
                FROM completed \
             ), \
             ever AS ( \
                SELECT kind, bool_or(outcome = 'did_work') AS ever_did_work \
                FROM completed \
                GROUP BY kind \
             ) \
             SELECT r.kind, COUNT(*) AS sample_size, \
                    COUNT(*) FILTER (WHERE r.outcome = 'nothing_to_do') AS nothing_to_do_count, \
                    e.ever_did_work \
             FROM recent r \
             JOIN ever e ON e.kind = r.kind \
             WHERE r.rn <= $1 \
             GROUP BY r.kind, e.ever_did_work \
             HAVING COUNT(*) = $1",
        )
        .bind(NOTHING_TO_DO_WINDOW)
        .fetch_all(pool)
        .await,
        "chain_health.outcome_distribution"
    );
    let outcome_violations = outcome_distribution_violations(&outcome_rows);

    // ── Tertiary: structural dead-letters, trailing window ────────────────────
    // `jobs` again — control table, one plain query, no per-account loop. No
    // `HAVING` and no threshold: `failure_class = 'structural'` is written
    // ONLY by `queue::could_not_do(.., FailureClass::Structural, ..)`, which
    // is a worker declaring it cannot process its own input by design, so the
    // correct count is zero and a single row is the signal. `finished_at` is
    // stamped by `queue::dead_letter` in the same statement that sets the
    // status, so it is never NULL on a row this predicate matches.
    let structural_dead_rows: Vec<(String, i64)> = fail_loud!(
        sqlx::query_as(
            "SELECT kind, COUNT(*) AS dead_count \
             FROM jobs \
             WHERE status = 'dead' \
               AND failure_class = 'structural' \
               AND finished_at > now() - make_interval(hours => $1::int) \
             GROUP BY kind \
             ORDER BY dead_count DESC, kind",
        )
        .bind(STRUCTURAL_DEAD_WINDOW_HOURS as i32)
        .fetch_all(pool)
        .await,
        "chain_health.structural_dead_letters"
    );
    let structural_dead_violations = structural_dead_letter_violations(&structural_dead_rows);

    // ── Read-only poison (COR-82): in-process, no query ──────────────────────
    let read_only_rows = state.worker_activity.read_only.snapshot();
    let read_only_violations = read_only_violations(&read_only_rows, Utc::now());
    // ── Sustained 5xx on a route: in-process, no query, this machine only ───
    let http_5xx_violations =
        crate::request_timing::LEDGER.violations(crate::request_timing::now_secs());
    let capture_reports: Vec<(serde_json::Value,)> = fail_loud!(
        sqlx::query_as("SELECT report FROM capture_admission_health")
            .fetch_all(pool)
            .await,
        "chain_health.capture_admission"
    );
    let capture_reports: Vec<crate::capture_admission_health::PlaneReport> = fail_loud!(
        capture_reports
            .into_iter()
            .map(|(report,)| serde_json::from_value(report))
            .collect::<Result<Vec<_>, _>>(),
        "chain_health.capture_admission_decode"
    );
    let active_tenants = fail_loud!(
        crate::tenancy::routing::list_active(pool).await,
        "chain_health.capture_admission_tenants"
    );
    let expected_planes: Vec<String> =
        std::iter::once(crate::capture_admission_health::CONSUMER_TENANT.to_string())
            .chain(active_tenants.into_iter().map(|t| t.clerk_org_id))
            .collect();
    let capture_verdict =
        crate::capture_admission_health::verdict(&capture_reports, &expected_planes, Utc::now());
    let capture_admission_violated = capture_verdict.violated();
    let capture_admission_page = capture_verdict.page();

    // `page`: an invariant is broken right now and every check should say
    // so. `healthy` additionally needs the capture SLO met; an SLO breach
    // alone is a draining backlog, which the workflow reports on change.
    let page = conservation_violated
        || !outcome_violations.is_empty()
        || !structural_dead_violations.is_empty()
        || unsure_rate_violated
        || !read_only_violations.is_empty()
        || !http_5xx_violations.is_empty()
        || capture_admission_page;
    let healthy = !page && !capture_admission_violated;

    if !healthy {
        tracing::error!(
            atoms_24h,
            relationship_edges_24h,
            conservation_violated,
            outcome_violation_count = outcome_violations.len(),
            structural_dead_violation_count = structural_dead_violations.len(),
            read_only_violation_count = read_only_violations.len(),
            http_5xx_violation_count = http_5xx_violations.len(),
            unsure_rate_violated,
            adjudications_in_window = unsure_window.map(|(total, _)| total),
            unsure_in_window = unsure_window.map(|(_, unsure)| unsure),
            capture_admission_violated,
            capture_admission_page,
            page,
            "chain health check: VIOLATION"
        );
    }

    (
        StatusCode::OK,
        Json(json!({
            "healthy": healthy,
            "page": page,
            "checked_at": Utc::now().to_rfc3339(),
            "conservation": {
                "window_hours": 24,
                "atom_floor": CONSERVATION_ATOM_FLOOR,
                "atoms_created": atoms_24h,
                "relationship_edges_created": relationship_edges_24h,
                "violated": conservation_violated,
                "accounts_checked": account_ids.len(),
            },
            "outcome_distribution": {
                "window_jobs": NOTHING_TO_DO_WINDOW,
                "kinds_with_a_full_window": outcome_rows.len(),
                "violations": outcome_violations,
            },
            "structural_dead_letters": {
                "window_hours": STRUCTURAL_DEAD_WINDOW_HOURS,
                "violations": structural_dead_violations,
            },
            "read_only_rejections": {
                "window_minutes": READ_ONLY_ALARM_WINDOW.num_minutes(),
                "violations": read_only_violations,
                "tenants": read_only_rows
                    .iter()
                    .map(|(tenant, record)| json!({ "tenant": tenant, "record": record }))
                    .collect::<Vec<_>>(),
            },
            "http_5xx": {
                "window_minutes": crate::request_timing::WINDOW_MINUTES,
                "min_errors": crate::request_timing::MIN_ERRORS,
                "min_share": format!("1/{}", crate::request_timing::MIN_SHARE_DENOMINATOR),
                // Process memory of the machine `flyctl proxy` reached.
                "scope": "this_machine",
                "violations": http_5xx_violations,
            },
            "unsure_adjudication_rate": {
                // `available: false` = the window read failed for at least one
                // account and this invariant rendered NO verdict this cycle.
                // It does not move `healthy` (see the read site's comment), so
                // this field and the `error!` log are the only signal that the
                // kill trigger has gone dark — do not assume `violated: false`
                // means "checked and fine".
                "available": unsure_window.is_some(),
                "window": UNSURE_RATE_WINDOW,
                "max_unsure": UNSURE_RATE_MAX_UNSURE,
                "adjudications_seen": unsure_window.map(|(total, _)| total),
                "unsure_count": unsure_window.map(|(_, unsure)| unsure),
                "violated": unsure_rate_violated,
            },
            "capture_admission": {
                "alert_after_minutes": crate::capture_admission_health::ALERT_AFTER.num_minutes(),
                "stale_after_minutes": crate::capture_admission_health::STALE_AFTER.num_minutes(),
                "adoption_floor": crate::capture_admission_health::adoption_floor(),
                "stall_after_minutes": crate::capture_admission_health::STALL_AFTER.num_minutes(),
                "stuck_after_minutes": crate::capture_admission_health::STUCK_AFTER.num_minutes(),
                "violated": capture_admission_violated,
                "page": capture_admission_page,
                "slo_breached": capture_verdict.slo_breached(),
                "stalled": capture_verdict.stalled.iter().map(|(tenant, w)| json!({"tenant": tenant, "captures": w})).collect::<Vec<_>>(),
                "stale_planes": capture_verdict.stale,
                "missing_planes": capture_verdict.missing,
                "unknown_clock": capture_verdict.unknown_clock.iter().map(|(tenant,row)|json!({"tenant":tenant,"captures":row})).collect::<Vec<_>>(),
                "unmeasured_clock_planes":capture_verdict.unmeasured_clock,
                "lag_violations":capture_verdict.lag_violations.iter().map(|(tenant,row)|json!({"tenant":tenant,"lag":row})).collect::<Vec<_>>(),
                "queued_lag_violations":capture_verdict.queued_lag_violations.iter().map(|(tenant,row)|json!({"tenant":tenant,"captures":row})).collect::<Vec<_>>(),
                "planes": capture_reports,
            },
        })),
    )
}

#[derive(Debug, Deserialize, Default)]
pub struct PipelineStageHealthQuery {
    /// `text` renders the operator table; anything else is JSON.
    pub format: Option<String>,
    /// One plane: `__consumer__` or a Clerk org id.
    pub tenant: Option<String>,
    /// The window the table shows (default: the long window, 24h).
    pub hours: Option<i64>,
}

/// `GET /internal/admin/page-read-health`: the page-read ledger
/// (`request_timing::PAGE_READ_LEDGER`), for the hourly
/// `page-read-health-check.yml` poll (`docs/plans/page-load-standard/design.md`
/// §9.2). Process memory, no database: the last 60 minutes of `page_read`
/// request durations as a fixed latency histogram, p50/p95 as bin upper
/// edges, and counts of statement timeouts and page-lane acquire timeouts.
/// One machine's view, like `chain-health`.
pub async fn admin_page_read_health(headers: HeaderMap) -> (StatusCode, Json<Value>) {
    if !check_internal_token(&headers) {
        return (
            StatusCode::UNAUTHORIZED,
            Json(json!({"error": "missing or invalid X-Internal-Token"})),
        );
    }
    let health =
        crate::request_timing::PAGE_READ_LEDGER.snapshot(crate::request_timing::now_secs());
    match serde_json::to_value(&health) {
        Ok(v) => (StatusCode::OK, Json(v)),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": e.to_string()})),
        ),
    }
}


/// GET `/internal/admin/pipeline-stage-health` — every plane's latest
/// [`crate::pipeline_stage_health::StageReport`], re-evaluated against the
/// current `STAGE_HEALTH_*` thresholds. Reads only the control table
/// `workers::pipeline_stage_health` writes, so it never touches a tenant pool.
/// A report older than three worker intervals is stale; an active plane with
/// no report is missing. Both make `healthy` false: an unmeasured plane is
/// not a healthy one.
///
/// `?format=text` prints the per-stage, per-source-type table for each plane
/// (`?hours=6` picks another stored window, `?tenant=org_x` one plane).

pub async fn admin_pipeline_stage_health(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(query): Query<PipelineStageHealthQuery>,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    use crate::pipeline_stage_health::{evaluate, render_table, StageReport, Thresholds};
    if !check_internal_token(&headers) {
        return (StatusCode::UNAUTHORIZED, Json(json!({"error": "missing or invalid X-Internal-Token"}))).into_response();
    }
    let Some(pool) = &state.db else {
        return (StatusCode::SERVICE_UNAVAILABLE, Json(json!({"error": "service unavailable"}))).into_response();
    };
    let rows: Vec<(Value,)> = match sqlx::query_as("SELECT report FROM pipeline_stage_health ORDER BY tenant")
        .fetch_all(pool)
        .await
    {
        Ok(rows) => rows,
        Err(e) => {
            tracing::error!(error = %e, "pipeline stage health: report read failed");
            return (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": "pipeline_stage_health.reports"}))).into_response();
        }
    };
    let mut reports: Vec<StageReport> = Vec::with_capacity(rows.len());
    for (report,) in rows {
        match serde_json::from_value(report) {
            Ok(r) => reports.push(r),
            Err(e) => {
                tracing::error!(error = %e, "pipeline stage health: report decode failed");
                return (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": "pipeline_stage_health.decode"}))).into_response();
            }
        }
    }
    let active = match crate::tenancy::routing::list_active(pool).await {
        Ok(active) => active,
        Err(e) => {
            tracing::error!(error = %e, "pipeline stage health: tenant roster read failed");
            return (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": "pipeline_stage_health.tenants"}))).into_response();
        }
    };
    let expected: Vec<String> = std::iter::once(crate::capture_admission_health::CONSUMER_TENANT.to_string())
        .chain(active.into_iter().map(|t| t.clerk_org_id))
        .collect();
    let th = Thresholds::from_env();
    let now = Utc::now();
    let stale_after = chrono::Duration::from_std(crate::workers::pipeline_stage_health::INTERVAL * 3)
        .unwrap_or_else(|_| chrono::Duration::minutes(45));
    let missing: Vec<&String> = expected.iter().filter(|t| !reports.iter().any(|r| &r.tenant == *t)).collect();
    let hours = query.hours.unwrap_or(th.long_hours);
    let selected: Vec<&StageReport> = reports
        .iter()
        .filter(|r| query.tenant.as_deref().is_none_or(|t| t == r.tenant))
        .collect();
    let mut healthy = missing.is_empty();
    let mut planes = Vec::new();
    let mut text = String::new();
    for r in &selected {
        let findings = evaluate(r, &th);
        let stale = now - r.measured_at > stale_after;
        healthy &= findings.is_empty() && !stale;
        if stale {
            text.push_str(&format!("STALE: last measured {}\n", r.measured_at.to_rfc3339()));
        }
        text.push_str(&render_table(r, hours, &findings));
        text.push('\n');
        planes.push(json!({"tenant": r.tenant, "measured_at": r.measured_at, "stale": stale, "findings": findings, "report": r}));
    }
    if query.format.as_deref() == Some("text") {
        for m in &missing {
            text.push_str(&format!("MISSING: no report for active plane {m}\n"));
        }
        return (StatusCode::OK, [(axum::http::header::CONTENT_TYPE, "text/plain; charset=utf-8")], text).into_response();
    }
    (
        StatusCode::OK,
        Json(json!({
            "healthy": healthy,
            "checked_at": now,
            "stale_after_minutes": stale_after.num_minutes(),
            "thresholds": th,
            "missing_planes": missing,
            "planes": planes,
        })),
    )
        .into_response()
}

/// Pure predicate for [`admin_chain_health`]'s primary invariant — see that
/// function's doc comment for the full incident/rationale. Deliberately a
/// strict `>` on the atom floor (not `>=`): an exactly-at-the-floor count is
/// the boundary the plan's spec draws, not a violation.
fn chain_conservation_violated(atoms_24h: i64, relationship_edges_24h: i64) -> bool {
    atoms_24h > CONSERVATION_ATOM_FLOOR && relationship_edges_24h == 0
}

/// Pure windowing half of [`admin_chain_health`]'s quaternary invariant.
/// `rows` are `(created_at, judgement_id, is_unsure)` — each account's own
/// oldest [`UNSURE_RATE_WINDOW`] LLM adjudications, concatenated in whatever
/// order the account loop produced them. Returns
/// `(adjudications_in_window, unsure_in_window)`.
///
/// The sort is what makes the union exact: the globally-oldest N rows are
/// always a subset of the union of each account's own oldest N, so sorting
/// the union by the SAME `(created_at, judgement_id)` key the per-account
/// queries used and truncating to N reproduces the global window exactly.
/// `judgement_id` is not decorative — rows written inside one ER sweep's
/// transaction share a `created_at`, and without the tie-break the window
/// boundary would depend on incidental row order.
fn first_llm_adjudications(mut rows: Vec<(DateTime<Utc>, Uuid, bool)>) -> (i64, i64) {
    rows.sort_unstable_by(|a, b| (a.0, a.1).cmp(&(b.0, b.1)));
    rows.truncate(UNSURE_RATE_WINDOW as usize);
    let unsure = rows.iter().filter(|(_, _, is_unsure)| *is_unsure).count() as i64;
    (rows.len() as i64, unsure)
}

/// Pure predicate for [`admin_chain_health`]'s quaternary invariant — the
/// judgement-graph rollout's kill trigger. See that function's doc comment
/// for the window's definition and for why annulled rows count.
///
/// SILENT (never a violation) below a full [`UNSURE_RATE_WINDOW`]: a partial
/// sample renders no verdict, the same discipline the outcome-distribution
/// query's `HAVING COUNT(*) = $1` enforces in SQL. Above it, a strict `>` on
/// [`UNSURE_RATE_MAX_UNSURE`] — "more than 40% of 50" is 21, not 20, and the
/// boundary the plan drew is not itself a violation (same strict-`>`
/// convention as [`chain_conservation_violated`]'s atom floor).
fn unsure_adjudication_rate_violated(total_in_window: i64, unsure_in_window: i64) -> bool {
    total_in_window >= UNSURE_RATE_WINDOW && unsure_in_window > UNSURE_RATE_MAX_UNSURE
}

/// Pure filter for [`admin_chain_health`]'s secondary invariant. `rows` are
/// `(kind, sample_size, nothing_to_do_count, ever_did_work)` — already
/// restricted to a FULL `NOTHING_TO_DO_WINDOW`-sized sample by the query's own
/// `HAVING` clause, so a 100% ratio here is exactly
/// `sample_size == nothing_to_do_count` (and `sample_size > 0` is a defensive
/// no-op given that `HAVING`, kept because a pure function should not trust an
/// invariant it cannot itself see).
///
/// `ever_did_work` narrows this from "is idle" to "**stopped** doing work",
/// which is the only shape that is evidence of a stall. A worker that has never
/// reported `DidWork` is idle by design — `InferRelationships` is exactly that
/// in production — and flagging it would pin the endpoint to `healthy=false`
/// permanently. See the query's comment for the measurement and the known
/// false-negative window while `jobs.outcome` history is still short.
///
/// [`outcome_idle_by_design`] is the second exemption, for a worker that DID
/// work and then became idle by policy. Without it, `InferExpertise` pins
/// this endpoint red forever on a healthy ocean (production 2026-09-10
/// onward: 20/20 `nothing_to_do`, conservation not violated).
fn outcome_distribution_violations(rows: &[(String, i64, i64, bool)]) -> Vec<Value> {
    rows.iter()
        .filter(|(kind, sample_size, nothing_to_do_count, ever_did_work)| {
            !outcome_idle_by_design(kind)
                && *ever_did_work
                && *sample_size > 0
                && sample_size == nothing_to_do_count
        })
        .map(|(kind, sample_size, nothing_to_do_count, _)| {
            json!({
                "kind": kind,
                "sample_size": sample_size,
                "nothing_to_do_count": nothing_to_do_count,
            })
        })
        .collect()
}

/// Kinds whose successful steady state is a full window of `NothingToDo`
/// even after they have reported `DidWork` in the past.
///
/// The founder rejected a generic kind-allowlist for #571 in favor of
/// `ever_did_work` — that guard is the right rule for a worker that has
/// *never* done work (`InferRelationships`). It cannot express the
/// `InferExpertise` shape: that worker *did* write `expert_in` edges, then
/// three product decisions made zero writes the common successful outcome:
///
/// 1. `ExtractRelationships` re-enqueues `InferExpertise` after every edge
///    write (debounced, but still a job per extract burst).
/// 2. The incrementality gate skips topics whose evidence has not moved
///    since the last review / blank-evaluation ledger row.
/// 3. The 2026-08-13 assert-or-drop gate (`#604`) writes an `expert_in`
///    edge only when a claim clears both auto-confirm confidence and
///    topic substance; everything else is a blank ledger row, not an
///    edge. Measured 2026-09-05/06: 226 LLM calls, 0 edges.
///
/// 100% `NothingToDo` over [`NOTHING_TO_DO_WINDOW`] is therefore the healthy
/// production shape (chain-health red continuously from 2026-09-10
/// through at least 2026-09-14, conservation healthy). Naming this kind
/// is the smallest fix that does not disable stall detection for
/// `ExtractRelationships`, which is the 2026-08-04 incident worker and is
/// not on this list.
///
/// GAP this accepts: a real `InferExpertise` stall (the worker structurally
/// cannot write `expert_in` when it should) is invisible to outcome
/// distribution. Conservation cannot see it either — `ExtractRelationships`
/// still writes other edges. That is the same coverage `InferRelationships`
/// already has; catching it needs an expert_in-vs-topical conservation
/// check, which is a new invariant this endpoint has explicitly refused to
/// grow into.
/// `OceanHealth` reports `DidWork` when it records paging alarms and
/// `NothingToDo` when evaluation succeeds without alarms. A full idle window
/// after an alarm is recovery, not a processing stall. Evaluation/recording
/// errors still fail the job; this exemption does not alter other health checks.
fn outcome_idle_by_design(kind: &str) -> bool {
    matches!(kind, "InferExpertise" | "OceanHealth")
}

/// Pure predicate for [`admin_chain_health`]'s read-only-poison check
/// (COR-82). A tenant is a violation when the harness saw a SQLSTATE 25006 on
/// its pool inside [`READ_ONLY_ALARM_WINDOW`] of `now`. Nothing in this
/// codebase runs a read-only transaction, so there is no threshold: one is a
/// poisoned pooled connection. `discarded_backends` tells the reader whether
/// the heal found anything to kill.
fn read_only_violations(
    rows: &[(String, crate::tenancy::read_only_poison::ReadOnlyRecord)],
    now: DateTime<Utc>,
) -> Vec<Value> {
    rows.iter()
        .filter(|(_, r)| now.signed_duration_since(r.last_rejection_at) <= READ_ONLY_ALARM_WINDOW)
        .map(|(tenant, r)| {
            json!({
                "tenant": tenant,
                "rejections": r.rejections,
                "discarded_backends": r.discarded_backends,
                "last_rejection_at": r.last_rejection_at.to_rfc3339(),
            })
        })
        .collect()
}

/// Pure filter for [`admin_chain_health`]'s tertiary invariant. `rows` are
/// `(kind, dead_count)`, already restricted by the query to `status='dead'`
/// AND `failure_class='structural'` inside the window — so every row that
/// arrives here IS a violation and this is a shape-map, not a threshold.
///
/// It is still a function rather than an inline `.map`, for the same reason
/// [`outcome_distribution_violations`] is: the `dead_count > 0` guard keeps
/// the predicate visible and testable in one place instead of living only in
/// a `GROUP BY`'s implicit contract. A `COUNT(*)` under `GROUP BY` can never
/// be zero, which is exactly why the guard costs nothing and why a future
/// query edit that breaks that assumption gets caught here.
fn structural_dead_letter_violations(rows: &[(String, i64)]) -> Vec<Value> {
    rows.iter()
        .filter(|(_, dead_count)| *dead_count > 0)
        .map(|(kind, dead_count)| {
            json!({
                "kind": kind,
                "dead_count": dead_count,
            })
        })
        .collect()
}

// ── POST /internal/admin/connector-flags ─────────────────────────────────────
// Foundation train PR 1.3: the operator-facing setter for the per-org
// connector flag registry (`org_connector_flags`, migration
// `0178_org_connector_flags`). See `connector_flags` module doc for the full
// resolution/caching contract this upserts into.

/// Body for [`set_connector_flag`]. `provider` is free text (see the
/// migration's own header for why this table does not use an enum) — today's
/// three meaningful values are `"slack"` (`ORG_SLACK_OAUTH_ENABLED`'s per-org
/// override), `"teams"` (`ORG_TEAMS_ENABLED`'s — CoralSwarm's own CS-094
/// named-teams feature, unrelated to Microsoft despite the name — see
/// `routes::teams_ingest::org_microsoft_teams_enabled_for`'s own doc comment
/// for that collision history), and `"microsoft"` (`ORG_MICROSOFT_TEAMS_
/// ENABLED`'s — the actual Microsoft Teams CONNECTOR gate, m365 Train 2 PR
/// 2.5), but nothing here hardcodes that trio.
#[derive(Debug, Deserialize)]
pub struct SetConnectorFlagRequest {
    /// A Clerk org id (`org_connector_flags.clerk_org_id`) — the same id
    /// `tenant.org_context().org_id` resolves to at every enforcement point
    /// this registry feeds.
    pub clerk_org_id: String,
    pub provider: String,
    pub enabled: bool,
}

/// Pure validation for [`set_connector_flag`]'s request body, factored out so
/// both rules — non-empty identifiers, and the `"teams"`/`MEMBER_OCEANS_ENABLED`
/// coupling guard — are unit-testable without a database. `Err` carries the
/// exact message the handler returns in its `error` field.
fn validate_connector_flag_write(
    clerk_org_id: &str,
    provider: &str,
    enabled: bool,
    member_oceans_enabled: bool,
) -> Result<(), &'static str> {
    if clerk_org_id.is_empty() || provider.is_empty() {
        return Err("clerk_org_id and provider must both be non-empty");
    }
    if provider == "teams" && enabled && !member_oceans_enabled {
        return Err(
            "cannot enable the teams flag for an org while MEMBER_OCEANS_ENABLED is off \
             fleet-wide — the org-offboard team cascade lives inside the member-ocean \
             reconciler's archive arm and would not run for this org's departed members. Set \
             MEMBER_OCEANS_ENABLED first.",
        );
    }
    Ok(())
}

/// Upsert a per-org connector flag override.
///
/// This is an **internal** endpoint — it requires an `X-Internal-Token`
/// header matching `INTERNAL_TOKEN`, same as every other handler in this
/// file. It does NOT use session-based auth: there is no org-admin role
/// concept for "which orgs get piloted on which connector first" — that is
/// an operator/founder decision, not something an org's own admin can grant
/// themselves (see the M365 plan's decision #10 — "prerequisite for piloting
/// M365 on one org").
///
/// Invalidates the resolved-flag cache for `(clerk_org_id, provider)`
/// immediately on success, so the change is visible on the very next
/// request rather than waiting out the registry's TTL.
///
/// **Guards the `ORG_TEAMS_ENABLED` → `MEMBER_OCEANS_ENABLED` coupling for
/// the per-org case too** (see `routes::teams::org_teams_active_for`'s own
/// doc comment for the full argument): refuses to write an ENABLING
/// `"teams"` row while `MEMBER_OCEANS_ENABLED` is off fleet-wide, since that
/// combination would recreate — for one org — the exact "no removal path for
/// a departed org member" hazard `lib.rs::run()`'s startup assert exists to
/// catch for the fleet-wide case. This is belt-and-suspenders, not the
/// primary guard: `org_teams_active_for` re-checks `member_oceans_enabled()`
/// on every request regardless of how a `"teams"` row got written (a row
/// written before this guard existed, a direct DB edit, etc), so it fails
/// closed on its own either way.
pub async fn set_connector_flag(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(req): Json<SetConnectorFlagRequest>,
) -> (StatusCode, Json<Value>) {
    if !check_internal_token(&headers) {
        return (
            StatusCode::UNAUTHORIZED,
            Json(json!({"error": "missing or invalid X-Internal-Token"})),
        );
    }

    let clerk_org_id = req.clerk_org_id.trim();
    let provider = req.provider.trim();
    if let Err(msg) = validate_connector_flag_write(
        clerk_org_id,
        provider,
        req.enabled,
        crate::workers::member_ocean_reconciler::member_oceans_enabled(),
    ) {
        return (
            StatusCode::UNPROCESSABLE_ENTITY,
            Json(json!({"error": msg})),
        );
    }

    let Some(pool) = state.control_db() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error": "service unavailable"})),
        );
    };

    if let Err(e) = crate::connector_flags::set_override(
        pool,
        &state.connector_flag_cache,
        clerk_org_id,
        provider,
        req.enabled,
    )
    .await
    {
        tracing::error!(
            error = %e,
            org_id = %clerk_org_id,
            provider,
            "failed to upsert org_connector_flags row"
        );
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": "internal error"})),
        );
    }

    let account: Option<Uuid> = match sqlx::query_scalar("SELECT id FROM accounts WHERE clerk_org_id=$1")
        .bind(clerk_org_id).fetch_optional(pool).await {
            Ok(account)=>account,
            Err(_)=>return (StatusCode::SERVICE_UNAVAILABLE,Json(json!({"error":"connector invalidation unavailable"}))),
        };
    if let Some(account)=account {
        if crate::read_models::dirty::account_changed(&state,account,None,&crate::read_models::dirty::models_for(crate::read_models::dirty::DirtyEvent::ConnectorGrantChanged)).await.is_err() {
            return (StatusCode::SERVICE_UNAVAILABLE,Json(json!({"error":"connector invalidation unavailable"})));
        }
    }
    (
        StatusCode::OK,
        Json(json!({
            "clerk_org_id": clerk_org_id,
            "provider": provider,
            "enabled": req.enabled,
        })),
    )
}

// ── Unit tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;

    // ── validate_connector_flag_write ───────────────────────────────────────

    #[test]
    fn rejects_empty_clerk_org_id() {
        assert!(validate_connector_flag_write("", "slack", true, true).is_err());
    }

    #[test]
    fn rejects_empty_provider() {
        assert!(validate_connector_flag_write("org_a", "", true, true).is_err());
    }

    #[test]
    fn allows_enabling_slack_regardless_of_member_oceans() {
        assert!(validate_connector_flag_write("org_a", "slack", true, false).is_ok());
    }

    #[test]
    fn allows_disabling_teams_regardless_of_member_oceans() {
        assert!(validate_connector_flag_write("org_a", "teams", false, false).is_ok());
    }

    #[test]
    fn allows_enabling_teams_when_member_oceans_enabled() {
        assert!(validate_connector_flag_write("org_a", "teams", true, true).is_ok());
    }

    #[test]
    fn rejects_enabling_teams_when_member_oceans_disabled() {
        assert!(validate_connector_flag_write("org_a", "teams", true, false).is_err());
    }
    use std::sync::Mutex;

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    fn make_headers_with_token(token: &str) -> HeaderMap {
        let mut h = HeaderMap::new();
        h.insert(
            "x-internal-token",
            HeaderValue::from_str(token).unwrap(),
        );
        h
    }

    #[test]
    fn check_internal_token_all_cases() {
        let _guard = ENV_LOCK.lock().unwrap();

        unsafe { std::env::remove_var("INTERNAL_TOKEN") };
        assert!(
            !check_internal_token(&make_headers_with_token("anything")),
            "missing env var should reject"
        );

        unsafe { std::env::set_var("INTERNAL_TOKEN", "correct-secret") };
        assert!(
            !check_internal_token(&make_headers_with_token("wrong-secret")),
            "wrong token should reject"
        );

        unsafe { std::env::set_var("INTERNAL_TOKEN", "my-secret") };
        assert!(
            check_internal_token(&make_headers_with_token("my-secret")),
            "correct token should accept"
        );

        assert!(
            !check_internal_token(&make_headers_with_token("")),
            "empty header value should reject"
        );

        assert!(
            !check_internal_token(&HeaderMap::new()),
            "missing header should reject"
        );

        unsafe { std::env::remove_var("INTERNAL_TOKEN") };
    }

    // ── admin_credits: credit_account_display ────────────────────────────

    #[test]
    fn credit_account_display_personal_prefers_name_over_email() {
        assert_eq!(
            credit_account_display(None, Some("Jamie Lee"), Some("jamie@example.com"), None),
            ("personal", "Jamie Lee".to_string())
        );
    }

    #[test]
    fn credit_account_display_personal_falls_back_to_email_when_name_is_null() {
        assert_eq!(
            credit_account_display(None, None, Some("jamie@example.com"), None),
            ("personal", "jamie@example.com".to_string())
        );
    }

    /// A `users` row with neither a name nor (in practice never happens, but
    /// a pure function must not panic on it) an email still returns a real
    /// string, never a fabricated placeholder like "Unknown".
    #[test]
    fn credit_account_display_personal_with_neither_name_nor_email_is_an_empty_string_not_a_panic() {
        assert_eq!(credit_account_display(None, None, None, None), ("personal", String::new()));
    }

    #[test]
    fn credit_account_display_org_prefers_slug_over_raw_clerk_org_id() {
        assert_eq!(
            credit_account_display(Some("org_abc123"), None, None, Some("mock-testorg")),
            ("org", "mock-testorg".to_string())
        );
    }

    /// `tenant_databases` can lag `accounts` (an org row created before its
    /// tenant provisioning finished) — the raw Clerk org id is still a real,
    /// honest identifier, never a placeholder.
    #[test]
    fn credit_account_display_org_falls_back_to_raw_clerk_org_id_when_slug_is_null() {
        assert_eq!(
            credit_account_display(Some("org_abc123"), None, None, None),
            ("org", "org_abc123".to_string())
        );
    }

    // ── admin_spend: spend_freshness ──────────────────────────────────────

    /// The regression this PR exists to prevent: a nightly rollup that has
    /// NEVER RUN (`rollup_at: None`) must report `freshness.at: null`, never
    /// a fabricated `Utc::now()` masquerading as "fresh right now".
    #[test]
    fn spend_freshness_nightly_never_run_is_null() {
        let (kind, at) = spend_freshness(1, 0, None);
        assert_eq!(kind, "nightly");
        assert_eq!(at, None, "a rollup that never ran must never synthesize a timestamp");
    }

    #[test]
    fn spend_freshness_nightly_with_a_real_rollup_passes_it_through() {
        let ts = Utc::now();
        let (kind, at) = spend_freshness(1, 0, Some(ts));
        assert_eq!(kind, "nightly");
        assert_eq!(at, Some(ts.to_rfc3339()));
    }

    fn month_start(year: i32, month: u32) -> DateTime<Utc> {
        let dt = NaiveDate::from_ymd_opt(year, month, 1)
            .and_then(|d| d.and_hms_opt(0, 0, 0))
            .expect("valid test date");
        DateTime::<Utc>::from_naive_utc_and_offset(dt, Utc)
    }

    /// `claude-sonnet-5` bills at the introductory rate for report months
    /// through August 2026 and at the standard rate from September 2026 —
    /// keyed off the reported month, not the wall clock.
    ///
    /// Usage-metering PR1: `model_rate`/`estimate_cost_usd` are now thin
    /// callers over a [`PriceSheet`] rather than owning the rate data
    /// themselves — every test below builds one via
    /// `crate::billing::rates::tests::seeded_for_tests()`, a hand-mirrored
    /// copy of `0148_model_rates.sql`'s seed rows kept specifically so these
    /// stay plain `#[test]`s with no live Postgres (see that function's own
    /// doc comment). The assertions and expected numbers are unchanged from
    /// pre-PR1 — only the plumbing to reach them moved.
    #[test]
    fn sonnet_5_rate_flips_at_the_september_2026_report_month() {
        let sheet = crate::billing::rates::tests::seeded_for_tests();
        assert_eq!(
            model_rate("claude-sonnet-5", month_start(2026, 8), &sheet),
            (2.0, 10.0),
            "an August 2026 report month prices at the introductory rate"
        );
        assert_eq!(
            model_rate("claude-sonnet-5", month_start(2026, 9), &sheet),
            (3.0, 15.0),
            "a September 2026 report month prices at the standard rate"
        );
        // Suffixed identifiers (dated snapshots, provider variants) take the
        // same time-dependent path via the substring match.
        assert_eq!(
            model_rate("anthropic.claude-sonnet-5-v1", month_start(2026, 8), &sheet),
            (2.0, 10.0)
        );
        assert_eq!(
            model_rate("anthropic.claude-sonnet-5-v1", month_start(2026, 9), &sheet),
            (3.0, 15.0)
        );
    }

    #[test]
    fn non_sonnet_5_rates_ignore_the_report_month() {
        let sheet = crate::billing::rates::tests::seeded_for_tests();
        for as_of in [month_start(2026, 8), month_start(2026, 9)] {
            assert_eq!(model_rate("claude-haiku-4-5", as_of, &sheet), (1.0, 5.0));
            assert_eq!(model_rate("claude-opus-4-8", as_of, &sheet), (5.0, 25.0));
            assert_eq!(
                model_rate("some-unknown-model", as_of, &sheet),
                (5.7, 27.5),
                "an unrecognized model falls back to the blend rate"
            );
        }
    }

    /// Regression for the bug this fix addresses: `estimate_cost_usd` used to
    /// ignore cache tokens entirely, undercounting real Anthropic-billed
    /// spend whenever prompt caching was in play. Numbers here are the
    /// production `claude-sonnet-4-6` figures verified 2026-08-03 (see this
    /// fix's PR description): 119,561 cache-creation tokens and 78,832
    /// cache-read tokens on top of the plain input/output tokens that
    /// produced the pre-fix $4.14 figure — the fixed formula must land on
    /// $4.61 (rounded to cents, matching `round_cents`/`SpendRow::to_json`).
    #[test]
    fn estimate_cost_usd_prices_cache_creation_and_cache_read_tokens() {
        let sheet = crate::billing::rates::tests::seeded_for_tests();
        let as_of = month_start(2026, 8);
        let (rate_in, rate_out) = model_rate("claude-sonnet-4-6", as_of, &sheet);
        assert_eq!((rate_in, rate_out), (3.0, 15.0));

        let cache_creation_tokens = 119_561i64;
        let cache_read_tokens = 78_832i64;

        // Isolate the cache-only contribution first: zero plain input/output
        // tokens means the whole result is exactly the cache pricing.
        let cache_only = estimate_cost_usd(
            0,
            0,
            cache_creation_tokens,
            cache_read_tokens,
            "claude-sonnet-4-6",
            as_of,
            &sheet,
        );
        let expected_cache_only = (cache_creation_tokens as f64 / 1_000_000.0) * rate_in * 1.25
            + (cache_read_tokens as f64 / 1_000_000.0) * rate_in * 0.1;
        assert!(
            (cache_only - expected_cache_only).abs() < 1e-9,
            "cache-creation tokens must price at 1.25x input rate, cache-read at 0.1x input rate"
        );

        // Full reconciliation: pre-fix plain input/output tokens rounded to
        // $4.14 (`round_cents`); adding the cache tokens above must land on
        // the true Anthropic-billed $4.61.
        let plain_input_tokens = 1_380_000i64; // pre-fix input tokens for the day's ingestion calls
        let plain_output_tokens = 0i64;
        let pre_fix_cost = estimate_cost_usd(plain_input_tokens, plain_output_tokens, 0, 0, "claude-sonnet-4-6", as_of, &sheet);
        assert_eq!(round_cents(pre_fix_cost), 4.14, "sanity-check the pre-fix baseline this test reconciles against");

        let fixed_cost = estimate_cost_usd(
            plain_input_tokens,
            plain_output_tokens,
            cache_creation_tokens,
            cache_read_tokens,
            "claude-sonnet-4-6",
            as_of,
            &sheet,
        );
        assert_eq!(round_cents(fixed_cost), 4.61, "cache-aware total must match the verified production figure");
    }

    #[test]
    fn estimate_cost_usd_zero_cache_tokens_matches_plain_input_output_pricing() {
        let sheet = crate::billing::rates::tests::seeded_for_tests();
        let as_of = month_start(2026, 8);
        assert_eq!(
            estimate_cost_usd(1_000_000, 1_000_000, 0, 0, "claude-haiku-4-5", as_of, &sheet),
            1.0 + 5.0,
            "zero cache tokens must not change the plain input/output result"
        );
    }

    /// New in usage-metering PR1: every model id this backend actually
    /// dispatches to (the full `llm::model_config` roster — extraction,
    /// synthesis, aux, expertise, summary map/reduce, conflict-detection,
    /// journal-compile, and the Fast/Smart/Max answer-prose tiers) must
    /// resolve to a SPECIFIC `model_rates` row, not fall through to the
    /// blend. A model silently pricing off the blend average is exactly the
    /// "looks like it's being tracked, isn't really" failure this PR exists
    /// to make visible (see `PriceSheet::rate`'s `tracing::warn!(alert =
    /// true, ...)` for the production-side half of this same guard).
    ///
    /// Negative control (manual run, documented in this PR's description
    /// and in `crate::billing::rates::tests::seeded_for_tests`'s doc
    /// comment, not a committed test): removing BOTH the `claude-opus-5`
    /// row AND its family-generic `claude-opus` row from
    /// `seeded_for_tests()` makes this test fail. A single row is not
    /// enough — every model below also matches a broader family-generic
    /// pattern (`"claude-haiku"`/`"claude-sonnet"`/`"claude-opus"`) that's
    /// still present after removing just the specific one, so it falls
    /// through to a DIFFERENT real row, not the blend. See that function's
    /// doc comment for the full account of what was actually run.
    #[test]
    fn every_model_config_model_resolves_to_a_real_rate_not_the_blend() {
        let sheet = crate::billing::rates::tests::seeded_for_tests();
        let as_of = month_start(2026, 8);

        let models: &[&str] = &[
            crate::llm::model_config::DEFAULT_EXTRACT_MODEL,
            crate::llm::model_config::DEFAULT_RESOLVE_MODEL,
            crate::llm::model_config::DEFAULT_SYNTHESIS_MODEL,
            crate::llm::model_config::DEFAULT_AUX_MODEL,
            crate::llm::model_config::DEFAULT_EXPERTISE_MODEL,
            crate::llm::model_config::DEFAULT_SUMMARY_MAP_MODEL,
            crate::llm::model_config::DEFAULT_SUMMARY_REDUCE_MODEL,
            crate::llm::model_config::DEFAULT_CONFLICT_MODEL,
            crate::llm::model_config::MODEL_FAST,
            crate::llm::model_config::MODEL_SMART,
            crate::llm::model_config::MODEL_MAX,
            crate::llm::model_config::DEFAULT_JOURNAL_COMPILE_MODEL,
        ];

        for model in models {
            let resolved = sheet.rate("anthropic", model, as_of);
            assert!(
                resolved.matched,
                "{model:?} resolved to the blend fallback instead of a specific model_rates row"
            );
        }
    }

    /// **Old-vs-new pricing equivalence proof.** Plan §4 lists nine ways a
    /// bug in the usage-metering train corrupts billing data; item 5 is this
    /// PR's own: "using `now()` instead of the reported month's start
    /// silently reprices history." But `as_of` semantics being correct is
    /// not the same as the PORT being correct — a typo in a seeded rate, an
    /// off-by-one on the 2026-09-01 cutover, or a dropped cache multiplier
    /// would silently change `/internal/admin/spend`'s numbers with every
    /// existing test still green, because nothing before this test ever
    /// diffed the new path against its pre-PR1 self. This test IS that
    /// diff.
    ///
    /// OLD is recovered VERBATIM from `git show
    /// main:backend/src/routes/admin.rs` (main @ `0669a8c6`, this branch's
    /// own base commit, i.e. this file's content immediately before this
    /// PR's changes) — deliberately re-typed here rather than referenced,
    /// since the entire point is an independent implementation to diff
    /// against, not a shared code path that could hide a divergence from
    /// itself.
    ///
    /// NEW reuses `crate::billing::rates::tests::seeded_for_tests()` — the
    /// SAME hand-mirrored copy of `0148_model_rates.sql`'s seed rows every
    /// other test in this module already uses — rather than a third
    /// hand-typed rate list, which would itself be a place for OLD and NEW
    /// to drift apart without this test noticing (see that function's own
    /// doc comment for how ITS fidelity to the real migration was checked:
    /// a manual negative control plus a live load against a real Postgres
    /// 17 instance with the actual migration applied).
    ///
    /// **Negative control for THIS test** (manual, not committed): shifting
    /// the seeded blend row's `input_per_million` from `5.7` to `5.71` in
    /// `seeded_for_tests()` made this test fail immediately, with a
    /// diagnostic naming the unknown-model/unattributed-sentinel
    /// combinations (the only ones that hit the blend row) and the exact
    /// old/new values that disagreed. Reverted immediately after. Recorded
    /// in this PR's description, not re-run automatically here, for the
    /// same reason `every_model_config_model_resolves_to_a_real_rate_not_
    /// the_blend`'s negative control above isn't either: it requires
    /// hand-editing `seeded_for_tests()`, which would defeat the "shared
    /// helper, one source of truth" property this test's whole design rests
    /// on if done programmatically inside the test itself.
    #[test]
    fn new_pricing_path_is_arithmetically_identical_to_the_pre_pr1_constants() {
        // ── OLD: verbatim pre-PR1 constants + lookup logic ──────────────
        const OLD_MODEL_RATES_PER_MILLION: &[(&str, f64, f64)] = &[
            ("claude-haiku-4-5", 1.0, 5.0),
            ("claude-sonnet-4-6", 3.0, 15.0),
            ("claude-opus-4-8", 5.0, 25.0),
            ("claude-opus-5", 5.0, 25.0),
            ("claude-fable-5", 10.0, 50.0),
            ("claude-3-5-sonnet", 3.0, 15.0),
            ("claude-3-7-sonnet", 3.0, 15.0),
            ("claude-sonnet", 3.0, 15.0),
            ("claude-3-5-haiku", 0.8, 4.0),
            ("claude-3-haiku", 0.25, 1.25),
            ("claude-haiku", 0.8, 4.0),
            ("claude-3-opus", 15.0, 75.0),
            ("claude-opus", 15.0, 75.0),
        ];
        const OLD_SONNET_5_INTRO_RATE_PER_MILLION: (f64, f64) = (2.0, 10.0);
        const OLD_SONNET_5_STANDARD_RATE_PER_MILLION: (f64, f64) = (3.0, 15.0);
        const OLD_DEFAULT_RATE_PER_MILLION: (f64, f64) = (5.7, 27.5);
        const OLD_CACHE_WRITE_RATE_MULTIPLIER: f64 = 1.25;
        const OLD_CACHE_READ_RATE_MULTIPLIER: f64 = 0.1;

        fn old_model_rate(model: &str, as_of: DateTime<Utc>) -> (f64, f64) {
            if model.contains("claude-sonnet-5") {
                let standard_takes_effect = NaiveDate::from_ymd_opt(2026, 9, 1)
                    .and_then(|d| d.and_hms_opt(0, 0, 0))
                    .map(|dt| DateTime::<Utc>::from_naive_utc_and_offset(dt, Utc))
                    .expect("2026-09-01 00:00:00 is a valid timestamp");
                return if as_of < standard_takes_effect {
                    OLD_SONNET_5_INTRO_RATE_PER_MILLION
                } else {
                    OLD_SONNET_5_STANDARD_RATE_PER_MILLION
                };
            }
            OLD_MODEL_RATES_PER_MILLION
                .iter()
                .find(|(needle, _, _)| model.contains(needle))
                .map(|(_, input, output)| (*input, *output))
                .unwrap_or(OLD_DEFAULT_RATE_PER_MILLION)
        }

        fn old_estimate_cost_usd(
            input_tokens: i64,
            output_tokens: i64,
            cache_creation_tokens: i64,
            cache_read_tokens: i64,
            model: &str,
            as_of: DateTime<Utc>,
        ) -> f64 {
            let (rate_in, rate_out) = old_model_rate(model, as_of);
            (input_tokens as f64 / 1_000_000.0) * rate_in
                + (output_tokens as f64 / 1_000_000.0) * rate_out
                + (cache_creation_tokens as f64 / 1_000_000.0) * rate_in * OLD_CACHE_WRITE_RATE_MULTIPLIER
                + (cache_read_tokens as f64 / 1_000_000.0) * rate_in * OLD_CACHE_READ_RATE_MULTIPLIER
        }

        // ── NEW: the PriceSheet-backed path, same seed helper as every
        // other test in this file. ──────────────────────────────────────
        let sheet = crate::billing::rates::tests::seeded_for_tests();

        // ── The cross product ────────────────────────────────────────────
        // Claude-era ids the frozen OLD table prices. DEFAULT_* are OpenRouter
        // DeepSeek/GLM now; feeding them here would compare the unknown-model
        // blend against live OpenRouter rates and fail for the wrong reason.
        let models: &[&str] = &[
            "claude-haiku-4-5",
            "claude-sonnet-4-6",
            "claude-sonnet-5",
            "claude-opus-5",
            UNATTRIBUTED_SENTINEL,
            "some-genuinely-unknown-model-xyz",
        ];

        let token_combos: &[(i64, i64, i64, i64)] = &[
            (0, 0, 0, 0),
            (1, 1, 1, 1),
            (1_000_000, 1_000_000, 0, 0),
            // The production-verified combo from
            // `estimate_cost_usd_prices_cache_creation_and_cache_read_tokens`
            // above ($4.61 reconciled against real `llm_usage` rows).
            (1_380_000, 0, 119_561, 78_832),
            // Large-value stress — exposes any integer/float width or
            // rounding divergence between the two independently-typed
            // formulas.
            (999_999_999_999, 999_999_999_999, 999_999_999_999, 999_999_999_999),
        ];

        let cutover = DateTime::<Utc>::from_naive_utc_and_offset(
            NaiveDate::from_ymd_opt(2026, 9, 1)
                .and_then(|d| d.and_hms_opt(0, 0, 0))
                .expect("valid cutover date"),
            Utc,
        );
        // Five points spanning the Sonnet-5 intro/standard boundary — the
        // ONLY place OLD's `<` comparison and NEW's `effective_from <=
        // as_of AND (effective_to IS NULL OR effective_to > as_of)` window
        // could conceivably disagree, and the entire reason the split
        // exists at all.
        let as_of_values: &[DateTime<Utc>] = &[
            month_start(2026, 1),                     // far before
            cutover - chrono::Duration::seconds(1),    // strictly before
            cutover,                                   // exactly at
            cutover + chrono::Duration::seconds(1),    // strictly after
            month_start(2027, 1),                     // far after
        ];

        let mut combinations = 0usize;
        for &model in models {
            for &(input_tokens, output_tokens, cache_write, cache_read) in token_combos {
                for &as_of in as_of_values {
                    combinations += 1;
                    let old = old_estimate_cost_usd(input_tokens, output_tokens, cache_write, cache_read, model, as_of);
                    let new = estimate_cost_usd(input_tokens, output_tokens, cache_write, cache_read, model, as_of, &sheet);
                    assert!(
                        (old - new).abs() < 1e-9,
                        "old/new pricing diverged for model={model:?} \
                         tokens=(in={input_tokens}, out={output_tokens}, cache_w={cache_write}, cache_r={cache_read}) \
                         as_of={as_of}: old={old}, new={new}"
                    );
                }
            }
        }
        assert_eq!(
            combinations,
            models.len() * token_combos.len() * as_of_values.len(),
            "sanity-check the cross product size this test claims to cover"
        );
    }

    /// Companion to the equivalence test above: proves `as_of` itself is
    /// wired correctly, not just that the pricing MATH agrees once given
    /// the same `as_of` — a test could pass the equivalence proof above and
    /// STILL silently reprice history if `admin_spend` fed `Utc::now()`
    /// into `estimate_cost_usd` instead of the reported month's `start`
    /// (plan §4 item 5, verbatim). `admin_spend` cannot run in a pure
    /// `#[test]` (it needs a live `AppState.db`), so this is a source-level
    /// regression guard — a "grep-strength check" in the same honest sense
    /// `scripts/check_rls_coverage.sh`'s own header uses that phrase: it
    /// proves the CURRENT source has the right shape, not that no future
    /// refactor could ever defeat a text scan. If the anchors below stop
    /// matching (admin_spend moved, or its neighbor renamed), this test
    /// fails LOUD with an explicit message to update it — it does not
    /// silently pass on a stale anchor.
    #[test]
    fn admin_spend_prices_off_the_reported_month_start_never_the_wall_clock() {
        let source = include_str!("admin.rs");

        let start_anchor = "pub async fn admin_spend(";
        let fn_start = source.find(start_anchor).expect(
            "admin_spend definition not found in this file — this test's source-scan anchor \
             is stale, update start_anchor",
        );
        // `cache_pricing_is_complete` is the next top-level item after
        // `admin_spend` today — see the ── section banner comment between
        // them in the source. If that ever changes, this `.expect` fires
        // and names exactly what to fix.
        let end_anchor = "\nfn cache_pricing_is_complete(";
        let fn_end = source[fn_start..]
            .find(end_anchor)
            .map(|rel| fn_start + rel)
            .expect(
                "cache_pricing_is_complete not found after admin_spend — this test's \
                 source-scan end_anchor is stale (admin_spend's neighbor was renamed or \
                 reordered); update end_anchor to whatever now immediately follows admin_spend",
            );
        let body = &source[fn_start..fn_end];

        assert!(
            !body.contains("Utc::now()"),
            "admin_spend must NEVER read the wall clock while pricing — as_of has to come \
             from parse_month_strict's `start`, never Utc::now() (plan §4 item 5: this is the \
             exact bug class that silently reprices history the moment a report for a past \
             month is re-run on a later day)"
        );
        assert!(
            body.contains("parse_month_strict(&query.month)"),
            "admin_spend must derive its reporting window (and therefore as_of) from the \
             requested month via parse_month_strict, not any other source"
        );

        // Extract each call's full argument list by matching parens (not a
        // fixed byte window — a fixed window can land mid-character on this
        // file's non-ASCII `─` section-banner comments, and can't be made
        // safe just by rounding to a char boundary without risking cutting
        // the args short). None of these call sites' arguments contain a
        // string literal with its own parens, so naive depth-counting is
        // exact here.
        fn call_args_after(body: &str, open_paren_byte: usize) -> &str {
            let mut depth = 1i32;
            for (offset, ch) in body[open_paren_byte..].char_indices().skip(1) {
                match ch {
                    '(' => depth += 1,
                    ')' => {
                        depth -= 1;
                        if depth == 0 {
                            return &body[open_paren_byte..open_paren_byte + offset];
                        }
                    }
                    _ => {}
                }
            }
            panic!("estimate_cost_usd( call starting at byte {open_paren_byte} has no matching close paren in admin_spend's body");
        }

        let call_sites: Vec<&str> = body
            .match_indices("estimate_cost_usd(")
            .map(|(i, m)| call_args_after(body, i + m.len() - 1))
            .collect();
        assert!(
            call_sites.len() >= 2,
            "expected admin_spend to still price both the personal- and org-plane rows \
             (found {} estimate_cost_usd call sites, expected at least 2)",
            call_sites.len()
        );
        for args in &call_sites {
            assert!(
                args.split(',').any(|arg| arg.trim() == "start"),
                "an estimate_cost_usd call site in admin_spend does not appear to pass \
                 `start` (parse_month_strict's own return value) as its as_of argument: \
                 args={args:?}"
            );
            assert!(
                !args.contains("now()"),
                "an estimate_cost_usd call site in admin_spend appears to use a wall-clock \
                 timestamp instead of `start`: args={args:?}"
            );
        }
    }

    #[test]
    fn spend_freshness_live_and_collecting_always_carry_a_timestamp() {
        let (kind, at) = spend_freshness(0, 1, None);
        assert_eq!(kind, "live");
        assert!(at.is_some(), "live is never backed by a rollup, so Utc::now() here is honest, not fabricated");

        let (kind, at) = spend_freshness(0, 0, None);
        assert_eq!(kind, "collecting");
        assert!(at.is_some());
    }

    /// Regression for this fix: `cache_pricing.complete` used to be a bare
    /// `org_count == 0` — now that `platform_metrics_daily` (migration
    /// `0123`) can carry real (or genuinely-unknown-NULL) cache sums,
    /// completeness must also account for whether any org tenant's month
    /// still has an unrecomputed pre-cache-aware row with real usage.
    #[test]
    fn cache_pricing_is_complete_true_when_no_org_accounts() {
        // No org accounts at all this month: nothing to be incomplete about,
        // regardless of the (irrelevant, should never happen alongside
        // org_count == 0 in practice) missing-data flag.
        assert!(cache_pricing_is_complete(0, false));
        assert!(cache_pricing_is_complete(0, true));
    }

    #[test]
    fn cache_pricing_is_complete_true_when_every_org_row_has_real_cache_data() {
        assert!(
            cache_pricing_is_complete(3, false),
            "org accounts present, but every (day, tenant) row this month was computed by the cache-aware rollup"
        );
    }

    #[test]
    fn cache_pricing_is_complete_false_when_any_org_row_predates_the_cache_aware_rollup() {
        assert!(
            !cache_pricing_is_complete(1, true),
            "even one org tenant with an unrecomputed NULL-cache row (and real usage that day) must flag incomplete"
        );
        assert!(
            !cache_pricing_is_complete(5, true),
            "one flagged tenant among several org accounts still taints the whole month's completeness signal"
        );
    }

    #[test]
    fn cache_pricing_note_names_the_reason_matching_each_completeness_state() {
        assert!(cache_pricing_note(0, false).contains("no org accounts"));
        assert!(
            cache_pricing_note(2, true).contains("0123"),
            "the incomplete-data note must name the migration/rollup gap explicitly: {}",
            cache_pricing_note(2, true)
        );
        assert!(
            cache_pricing_note(2, false).contains("cache-aware"),
            "the fully-complete-with-orgs note must say cache pricing IS accurate, not just silent: {}",
            cache_pricing_note(2, false)
        );
    }

    // ── admin_chain_health ───────────────────────────────────────────────────

    /// Positive control: the exact production shape confirmed live 2026-08-07
    /// (see [`admin_chain_health`]'s doc comment) — atoms flowing well past
    /// the floor, zero relationship edges, during the real 2026-08-04
    /// incident window. This is the case the whole endpoint exists for.
    #[test]
    fn chain_conservation_violated_matches_the_confirmed_production_incident() {
        assert!(
            chain_conservation_violated(184, 0),
            "184 atoms / 0 edges is the exact live-production violation this endpoint is built to catch"
        );
        assert!(chain_conservation_violated(182, 0), "the earlier of the two back-to-back live reads");
    }

    /// Negative control: a real pre-incident 24h window (2026-08-01, well
    /// before the 2026-08-04 05:01 UTC last edge) confirmed live 2026-08-07 —
    /// atoms AND edges both flowing normally must never read as a violation.
    #[test]
    fn chain_conservation_violated_false_on_the_confirmed_healthy_window() {
        assert!(
            !chain_conservation_violated(36, 29),
            "36 atoms / 29 edges is the real pre-incident shape — must be healthy"
        );
    }

    #[test]
    fn chain_conservation_violated_requires_both_conditions() {
        // Edges present at all — even the same incident's atom volume — is
        // not a violation. Conservation is checking for a total flatline,
        // not a slowdown.
        assert!(!chain_conservation_violated(184, 1));
        // Zero edges alone, below the atom floor, is not a violation either —
        // a quiet ocean with nothing to extract relationships FROM is
        // expected to produce zero edges, not evidence of a broken worker.
        assert!(!chain_conservation_violated(5, 0));
        assert!(!chain_conservation_violated(0, 0));
    }

    #[test]
    fn chain_conservation_violated_floor_is_strict_greater_than() {
        // Exactly at the floor is the documented non-violation boundary.
        assert!(!chain_conservation_violated(CONSERVATION_ATOM_FLOOR, 0));
        assert!(chain_conservation_violated(CONSERVATION_ATOM_FLOOR + 1, 0));
    }

    /// Positive control for the secondary invariant, matching the same
    /// confirmed incident: `ExtractRelationships` with a full window of
    /// `NothingToDo` outcomes must trip a violation.
    #[test]
    fn outcome_distribution_violations_flags_a_full_window_of_nothing_to_do() {
        let rows = vec![
            ("ExtractRelationships".to_string(), NOTHING_TO_DO_WINDOW, NOTHING_TO_DO_WINDOW, true),
            ("EmbedAtoms".to_string(), NOTHING_TO_DO_WINDOW, 0, true),
        ];
        let violations = outcome_distribution_violations(&rows);
        assert_eq!(violations.len(), 1, "only the 100%-NothingToDo kind should trip");
        assert_eq!(violations[0]["kind"], "ExtractRelationships");
        assert_eq!(violations[0]["sample_size"], NOTHING_TO_DO_WINDOW);
        assert_eq!(violations[0]["nothing_to_do_count"], NOTHING_TO_DO_WINDOW);
    }

    #[test]
    fn outcome_distribution_violations_ignores_a_partial_ratio() {
        // 19 of 20 NothingToDo is still a healthy worker doing SOME real
        // work — only a true 100% ratio is a violation.
        let rows =
            vec![("ExtractRelationships".to_string(), NOTHING_TO_DO_WINDOW, NOTHING_TO_DO_WINDOW - 1, true)];
        assert!(outcome_distribution_violations(&rows).is_empty());
    }

    #[test]
    fn outcome_distribution_violations_empty_rows_is_healthy() {
        // No kind has accumulated a full window yet — nothing to judge, not
        // evidence of a problem. This is the `HAVING COUNT(*) = $1` case.
        assert!(outcome_distribution_violations(&[]).is_empty());
    }

    #[test]
    fn outcome_distribution_violations_zero_sample_size_never_trips() {
        // Defensive: a hypothetical zero-sample row (should be excluded by
        // the query's own HAVING clause already) must never read as a false
        // 100% via a 0 == 0 comparison.
        let rows = vec![("SomeKind".to_string(), 0, 0, true)];
        assert!(outcome_distribution_violations(&rows).is_empty());
    }

    /// The production shape that forced the `ever_did_work` guard.
    /// `InferRelationships` sat at 20/20 `nothing_to_do` on 2026-08-07 while
    /// inferred edges were being written normally — by `ExtractRelationships`,
    /// not by it. Without the guard this pins `healthy=false` forever and takes
    /// the dead-man's switch down with it.
    #[test]
    fn outcome_distribution_violations_exempts_a_worker_that_never_did_work() {
        let rows = vec![(
            "InferRelationships".to_string(),
            NOTHING_TO_DO_WINDOW,
            NOTHING_TO_DO_WINDOW,
            false, // has never reported DidWork — idle by design, not stalled
        )];
        assert!(
            outcome_distribution_violations(&rows).is_empty(),
            "a worker with no DidWork history is idle by design, not regressed"
        );
    }

    /// Negative control for the test above: the guard must not swallow the real
    /// incident. Same 100% window, but this worker HAS done work before — that
    /// is a regression and must still trip. Without this pair, a guard that
    /// simply returned `vec![]` would pass the exemption test.
    #[test]
    fn outcome_distribution_violations_still_flags_a_worker_that_stopped_doing_work() {
        let rows = vec![
            ("InferRelationships".to_string(), NOTHING_TO_DO_WINDOW, NOTHING_TO_DO_WINDOW, false),
            ("ExtractRelationships".to_string(), NOTHING_TO_DO_WINDOW, NOTHING_TO_DO_WINDOW, true),
        ];
        let violations = outcome_distribution_violations(&rows);
        assert_eq!(violations.len(), 1, "only the regressed worker should trip");
        assert_eq!(violations[0]["kind"], "ExtractRelationships");
    }

    /// Production shape 2026-09-10 through 2026-09-14: `InferExpertise` at
    /// 20/20 `nothing_to_do` with `ever_did_work = true`, conservation healthy.
    /// The regression guard alone cannot exempt this — it DID write edges
    /// before assert-or-drop made zero writes the common success. Must not
    /// trip, and must not swallow a simultaneous `ExtractRelationships`
    /// stall (same pairing discipline as the InferRelationships tests).
    #[test]
    fn outcome_distribution_violations_exempts_infer_expertise_idle_after_did_work() {
        let rows = vec![
            ("InferExpertise".to_string(), NOTHING_TO_DO_WINDOW, NOTHING_TO_DO_WINDOW, true),
            ("ExtractRelationships".to_string(), NOTHING_TO_DO_WINDOW, NOTHING_TO_DO_WINDOW, true),
        ];
        let violations = outcome_distribution_violations(&rows);
        assert_eq!(violations.len(), 1, "InferExpertise must not hide the 2026-08-04 worker");
        assert_eq!(violations[0]["kind"], "ExtractRelationships");
    }

    #[test]
    fn outcome_distribution_violations_exempts_alarm_free_ocean_health() {
        let rows = vec![
            ("OceanHealth".to_string(), NOTHING_TO_DO_WINDOW, NOTHING_TO_DO_WINDOW, true),
            ("ExtractRelationships".to_string(), NOTHING_TO_DO_WINDOW, NOTHING_TO_DO_WINDOW, true),
        ];
        let violations = outcome_distribution_violations(&rows);
        assert_eq!(violations.len(), 1, "alarm-free health checks must not hide extraction stalls");
        assert_eq!(violations[0]["kind"], "ExtractRelationships");
    }

    #[test]
    fn outcome_idle_by_design_is_limited_to_known_successful_idle_kinds() {
        assert!(outcome_idle_by_design("OceanHealth"));
        assert!(!outcome_idle_by_design("UnknownWorker"));
        assert!(outcome_idle_by_design("InferExpertise"));
        assert!(
            !outcome_idle_by_design("ExtractRelationships"),
            "the 2026-08-04 incident worker must stay fully covered"
        );
        assert!(
            !outcome_idle_by_design("InferRelationships"),
            "that kind is exempted by ever_did_work=false, not this list — \
             if it ever starts doing work, a later stall must still trip"
        );
        assert!(!outcome_idle_by_design("EmbedAtoms"));
    }

    #[test]
    fn read_only_violations_flag_only_rejections_inside_the_window() {
        use crate::tenancy::read_only_poison::ReadOnlyRecord;
        let now = Utc::now();
        let rows = vec![
            (
                "org_fresh".to_string(),
                ReadOnlyRecord {
                    rejections: 3,
                    discarded_backends: 1,
                    last_rejection_at: now - chrono::Duration::minutes(5),
                    last_discard_at: Some(now - chrono::Duration::minutes(5)),
                },
            ),
            (
                "org_stale".to_string(),
                ReadOnlyRecord {
                    rejections: 40,
                    discarded_backends: 2,
                    last_rejection_at: now - READ_ONLY_ALARM_WINDOW - chrono::Duration::seconds(1),
                    last_discard_at: None,
                },
            ),
        ];
        let violations = read_only_violations(&rows, now);
        assert_eq!(violations.len(), 1, "a rejection older than the window has cleared");
        assert_eq!(violations[0]["tenant"], "org_fresh");
        assert_eq!(violations[0]["rejections"], 3);
        assert_eq!(violations[0]["discarded_backends"], 1);
        assert!(read_only_violations(&[], now).is_empty());
    }

    /// Positive control for the tertiary invariant, on the concrete case that
    /// motivated it: `feat/typed-job-outcomes` turns a revoked Slack app into
    /// one structural dead-letter per huddle. Conservation cannot see it
    /// (atoms and edges keep flowing) and outcome distribution cannot either
    /// (it reads `status='done'` rows, and these never reach `done`) — so if
    /// this check does not trip, nothing does.
    #[test]
    fn structural_dead_letter_violations_flags_the_huddle_terminal_accumulation() {
        let rows = vec![("SlackHuddleCapture".to_string(), 7), ("ExtractEntities".to_string(), 1)];
        let violations = structural_dead_letter_violations(&rows);
        assert_eq!(violations.len(), 2, "every structural dead-letter kind is a violation");
        assert_eq!(violations[0]["kind"], "SlackHuddleCapture");
        assert_eq!(violations[0]["dead_count"], 7);
        assert_eq!(violations[1]["dead_count"], 1, "a SINGLE structural dead-letter still trips");
    }

    /// Negative control: the steady state. Zero rows means no worker declared
    /// itself structurally unable to process its input in the window — which
    /// is the only healthy value there is for this invariant.
    #[test]
    fn structural_dead_letter_violations_empty_is_healthy() {
        assert!(structural_dead_letter_violations(&[]).is_empty());
    }

    /// The scope boundary that keeps this from becoming the per-worker
    /// failure-count invariant the endpoint rejected: this predicate only
    /// ever sees rows the query already filtered to
    /// `failure_class = 'structural'`. An ordinary `fail()` that exhausts its
    /// attempts ladder leaves `failure_class` NULL and never reaches here —
    /// including the 152 `RegenerateDigest` dead jobs, which are a real but
    /// DIFFERENT problem. A zero count (defensively — `COUNT(*)` under
    /// `GROUP BY` cannot produce one) must not trip.
    #[test]
    fn structural_dead_letter_violations_zero_count_never_trips() {
        let rows = vec![("RegenerateDigest".to_string(), 0)];
        assert!(structural_dead_letter_violations(&rows).is_empty());
    }

    // ── Quaternary: judgement-graph UNSURE rate (the rollout kill trigger) ────

    /// The boundary the rollout plan drew, both sides of it. "More than 40% of
    /// the first 50" is 21 out of 50, so exactly 20 is healthy — a `>=` here
    /// would fire the kill trigger on the tuning target itself.
    #[test]
    fn unsure_rate_boundary_is_strictly_above_forty_percent() {
        assert!(
            !unsure_adjudication_rate_violated(UNSURE_RATE_WINDOW, UNSURE_RATE_MAX_UNSURE),
            "20/50 is exactly 40% — the plan's target, not a violation"
        );
        assert!(
            unsure_adjudication_rate_violated(UNSURE_RATE_WINDOW, UNSURE_RATE_MAX_UNSURE + 1),
            "21/50 is the first count above 40% and must trip the kill trigger"
        );
    }

    /// Silent below a full window. A brand-new rollout with 49 adjudications —
    /// even ALL of them UNSURE — renders no verdict: a partial sample is not
    /// evidence, and a detector that screams on day one is a detector nobody
    /// reads. Same discipline as the outcome-distribution query's `HAVING`.
    #[test]
    fn unsure_rate_is_silent_below_a_full_window() {
        assert!(!unsure_adjudication_rate_violated(
            UNSURE_RATE_WINDOW - 1,
            UNSURE_RATE_WINDOW - 1
        ));
        assert!(!unsure_adjudication_rate_violated(0, 0));
        assert!(
            !unsure_adjudication_rate_violated(UNSURE_RATE_MAX_UNSURE + 1, UNSURE_RATE_MAX_UNSURE + 1),
            "21 UNSURE out of 21 adjudications is 100% — and still not a full window"
        );
    }

    /// The healthy steady state this invariant is supposed to tolerate: a full
    /// window with the UNSURE band doing its job.
    #[test]
    fn unsure_rate_healthy_on_a_well_tuned_window() {
        assert!(!unsure_adjudication_rate_violated(UNSURE_RATE_WINDOW, 3));
        assert!(!unsure_adjudication_rate_violated(UNSURE_RATE_WINDOW, 0));
    }

    /// The union-of-per-account-pages merge is exact: the globally-oldest 50
    /// rows are always a subset of the union of each account's own oldest 50,
    /// so sorting the concatenation on the SAME key and truncating reproduces
    /// the global window. Here account B's rows are older than account A's and
    /// arrive second — the naive "take the first 50 as concatenated" would
    /// count A's rows and miss B's entirely.
    #[test]
    fn first_llm_adjudications_merges_accounts_by_age_not_by_arrival() {
        let base = Utc::now();
        let mut rows: Vec<(DateTime<Utc>, Uuid, bool)> = Vec::new();
        // Account A: 50 NON-unsure rows, all recent.
        for i in 0..UNSURE_RATE_WINDOW {
            rows.push((base + chrono::Duration::seconds(1000 + i), Uuid::new_v4(), false));
        }
        // Account B: 50 UNSURE rows, all older — the real first window.
        for i in 0..UNSURE_RATE_WINDOW {
            rows.push((base + chrono::Duration::seconds(i), Uuid::new_v4(), true));
        }
        let (total, unsure) = first_llm_adjudications(rows);
        assert_eq!(total, UNSURE_RATE_WINDOW);
        assert_eq!(unsure, UNSURE_RATE_WINDOW, "the OLDEST 50 are account B's, all UNSURE");
        assert!(unsure_adjudication_rate_violated(total, unsure));
    }

    /// Ties on `created_at` are broken by `judgement_id`, deterministically —
    /// rows written inside one ER sweep's transaction share a timestamp, and
    /// without the secondary key the window's contents would depend on
    /// whatever order the per-account queries happened to return.
    #[test]
    fn first_llm_adjudications_breaks_created_at_ties_on_judgement_id() {
        let at = Utc::now();
        let low = Uuid::from_u128(1);
        let high = Uuid::from_u128(2);
        // Both rows fit inside a 50-wide window, so the counts alone can't
        // show the ordering — build a window of exactly `UNSURE_RATE_WINDOW`
        // rows at the SAME instant where only the id decides which fall
        // inside it. `high` ids are UNSURE and must all be pushed out.
        let mut rows: Vec<(DateTime<Utc>, Uuid, bool)> = Vec::new();
        for i in 0..UNSURE_RATE_WINDOW as u128 {
            rows.push((at, Uuid::from_u128(1_000 + i), true)); // higher ids, UNSURE
            rows.push((at, Uuid::from_u128(i), false)); // lower ids, not UNSURE
        }
        let (total, unsure) = first_llm_adjudications(rows);
        assert_eq!(total, UNSURE_RATE_WINDOW);
        assert_eq!(
            unsure, 0,
            "at an identical instant the LOWER judgement_ids are the window, whatever order they arrived in"
        );
        assert!(low < high, "sanity: Uuid ordering is what the tie-break relies on");
    }

    /// A short window stays short: fewer rows than the window means
    /// `adjudications_seen` reports what actually exists, which is what keeps
    /// the response honest about WHY it is silent.
    #[test]
    fn first_llm_adjudications_reports_a_partial_window_as_partial() {
        let at = Utc::now();
        let rows = vec![(at, Uuid::new_v4(), true), (at + chrono::Duration::seconds(1), Uuid::new_v4(), false)];
        assert_eq!(first_llm_adjudications(rows), (2, 1));
        assert_eq!(first_llm_adjudications(Vec::new()), (0, 0));
    }
}

// ── Live-Postgres tests: GET /internal/admin/credits ────────────────────────
//
// Gated on `RELINFER_TEST_DATABASE_URL`, same convention as every other
// `pg_tests` module in this crate (`workers::billing_derivation::pg_tests`,
// `billing::enforcement::pg_tests`, `routes::usage::pg_tests`). `cargo test
// --lib` with no env var set SKIPS these with an `eprintln!` marker rather
// than failing. Every test mints FRESH `Uuid::new_v4()` accounts, so tests
// never collide with each other OR with whatever earlier steps in the same
// CI job already left in `accounts`/`credit_balances`/`enforcement_events` —
// `admin_credits` loops over the WHOLE `accounts` table, so these tests
// assert PRESENCE/ABSENCE of their own account ids in the response rather
// than the response's overall length (the same "before/after, never an
// absolute count against a shared aggregate" discipline this train's own
// lessons call for).
#[cfg(test)]
mod pg_tests {
    use super::*;
    use axum::http::HeaderValue;
    use std::sync::Mutex;

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    async fn pg_test_pool(test_name: &str) -> Option<sqlx::PgPool> {
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

    /// A real `users` + `accounts` row pair — `credit_balances`/
    /// `enforcement_events` both FK to `accounts(id)`, so every test needs
    /// one. Personal-plane (no `clerk_org_id`) unless the test says
    /// otherwise.
    async fn seed_account(pool: &sqlx::PgPool, name: &str) -> Uuid {
        let user_id = Uuid::new_v4();
        let account_id = Uuid::new_v4();
        sqlx::query("INSERT INTO users (id, email, name) VALUES ($1, $2, $3)")
            .bind(user_id)
            .bind(format!("{user_id}@admin-credits-pr13.test"))
            .bind(name)
            .execute(pool)
            .await
            .expect("insert user");
        sqlx::query("INSERT INTO accounts (id, owner_user_id) VALUES ($1, $2)")
            .bind(account_id)
            .bind(user_id)
            .execute(pool)
            .await
            .expect("insert account");
        account_id
    }

    // ── credit_row_for_account ──────────────────────────────────────────

    /// **The required negative control.** A freshly-seeded account with NO
    /// `credit_balances` row and NO `enforcement_events` row must come back
    /// as `(None, vec![])` — an honest empty state, never a fabricated
    /// `$0.00` balance or a phantom event.
    #[tokio::test]
    async fn credit_row_for_account_is_an_honest_empty_state_for_a_never_metered_account() {
        let Some(pool) = pg_test_pool("credit_row_for_account_is_an_honest_empty_state_for_a_never_metered_account").await
        else {
            return;
        };
        let account_id = seed_account(&pool, "Never Metered").await;

        let (balance, events) = credit_row_for_account(&pool, account_id, "personal", "Never Metered")
            .await
            .expect("query must succeed even with nothing to find");

        assert!(balance.is_none(), "no credit_balances row exists -> honest None, not a fabricated zero");
        assert!(events.is_empty(), "no enforcement_events row exists -> empty, not a phantom event");
    }

    /// Positive control alongside the negative one above: a real
    /// `credit_balances` row and two real `enforcement_events` rows come
    /// back as real data, newest-event-first, capped at
    /// `CREDITS_EVENTS_PER_ACCOUNT`.
    #[tokio::test]
    async fn credit_row_for_account_reflects_real_balance_and_events() {
        let Some(pool) = pg_test_pool("credit_row_for_account_reflects_real_balance_and_events").await else {
            return;
        };
        let account_id = seed_account(&pool, "Metered Account").await;

        // A balance seed is a guarded financial write, even for this read fixture.
        // Enter the real account/period transaction; leave all read oracles intact.
        let mut tx = crate::tenancy::scope::begin_account_scoped(&pool, account_id)
            .await.expect("scope balance fixture account");
        crate::billing::renewal::lock_period_rotation(&mut tx, account_id)
            .await.expect("enter compatible financial transaction");
        sqlx::query("INSERT INTO credit_balances (account_id, balance) VALUES ($1, -2.5)")
            .bind(account_id)
            .execute(&mut *tx)
            .await
            .expect("insert balance");
        tx.commit().await.expect("commit balance fixture");
        sqlx::query(
            "INSERT INTO enforcement_events (account_id, would_have_blocked, reason, created_at) \
             VALUES ($1, false, 'older event', now() - interval '1 hour')",
        )
        .bind(account_id)
        .execute(&pool)
        .await
        .expect("insert older event");
        sqlx::query(
            "INSERT INTO enforcement_events (account_id, would_have_blocked, reason, created_at) \
             VALUES ($1, true, 'newer event', now())",
        )
        .bind(account_id)
        .execute(&pool)
        .await
        .expect("insert newer event");

        let (balance, events) = credit_row_for_account(&pool, account_id, "personal", "Metered Account")
            .await
            .expect("query must succeed");

        let balance = balance.expect("a credit_balances row was inserted above");
        assert_eq!(balance.account_id, account_id);
        assert_eq!(balance.name, "Metered Account");
        assert_eq!(balance.plane, "personal");
        assert!((balance.balance - (-2.5)).abs() < 1e-9, "balance must round-trip exactly, got {}", balance.balance);

        assert_eq!(events.len(), 2);
        assert_eq!(events[0].reason, "newer event", "newest event must sort first");
        assert!(events[0].would_have_blocked);
        assert_eq!(events[1].reason, "older event");
        assert!(!events[1].would_have_blocked);
    }

    // ── admin_credits (full handler) ────────────────────────────────────

    fn headers_with_token(token: &str) -> HeaderMap {
        let mut h = HeaderMap::new();
        h.insert("x-internal-token", HeaderValue::from_str(token).unwrap());
        h
    }

    async fn call_admin_credits(state: Arc<AppState>) -> Value {
        let _guard = ENV_LOCK.lock().unwrap();
        unsafe { std::env::set_var("INTERNAL_TOKEN", "admin-credits-pr13-test-token") };
        let (status, Json(body)) = admin_credits(State(state), headers_with_token("admin-credits-pr13-test-token")).await;
        unsafe { std::env::remove_var("INTERNAL_TOKEN") };
        assert_eq!(status, StatusCode::OK, "expected 200, got {body:?}");
        body
    }

    /// **The required negative control, at the wire level.** Two accounts:
    /// one freshly seeded with nothing, one with a real balance + event. The
    /// response must contain the funded account's real row and must NOT
    /// contain any row — fabricated or otherwise — for the empty account.
    /// Asserted by PRESENCE/ABSENCE of each account's id, never by the
    /// response's overall array length (see this module's own header
    /// comment for why: `admin_credits` loops over every account in the
    /// shared test database, not just the two this test seeds).
    #[tokio::test]
    async fn admin_credits_is_an_honest_empty_state_for_a_never_metered_account_alongside_a_real_one() {
        let Some(pool) =
            pg_test_pool("admin_credits_is_an_honest_empty_state_for_a_never_metered_account_alongside_a_real_one").await
        else {
            return;
        };
        let empty_account = seed_account(&pool, "Empty Watcher").await;
        let funded_account = seed_account(&pool, "Funded Watcher").await;

        // A balance seed is a guarded financial write, even for this read fixture.
        // Enter the real account/period transaction; leave all read oracles intact.
        let mut tx = crate::tenancy::scope::begin_account_scoped(&pool, funded_account)
            .await.expect("scope balance fixture account");
        crate::billing::renewal::lock_period_rotation(&mut tx, funded_account)
            .await.expect("enter compatible financial transaction");
        sqlx::query("INSERT INTO credit_balances (account_id, balance) VALUES ($1, 7.25)")
            .bind(funded_account)
            .execute(&mut *tx)
            .await
            .expect("insert balance");
        tx.commit().await.expect("commit balance fixture");
        sqlx::query(
            "INSERT INTO enforcement_events (account_id, would_have_blocked, reason) VALUES ($1, false, 'balance <= 0')",
        )
        .bind(funded_account)
        .execute(&pool)
        .await
        .expect("insert event");

        let state = Arc::new(crate::test_app_state_owned_with_db(pool.clone(), None));
        let body = call_admin_credits(state).await;

        let balances = body["balances"].as_array().expect("balances must be an array");
        let events = body["enforcement_events"].as_array().expect("enforcement_events must be an array");

        assert!(
            balances.iter().any(|b| b["account_id"] == json!(funded_account) && b["balance"] == json!(7.25)),
            "the funded account's real balance must be present: {balances:?}"
        );
        assert!(
            !balances.iter().any(|b| b["account_id"] == json!(empty_account)),
            "an account with no credit_balances row must be ABSENT, never a fabricated $0.00 row: {balances:?}"
        );
        assert!(
            events.iter().any(|e| e["account_id"] == json!(funded_account) && e["reason"] == json!("balance <= 0")),
            "the funded account's real event must be present: {events:?}"
        );
        assert!(
            !events.iter().any(|e| e["account_id"] == json!(empty_account)),
            "an account with no enforcement_events row must be ABSENT, never a phantom event: {events:?}"
        );
    }

    /// The token guard actually gates this handler, same as every other
    /// `/internal/admin/*` endpoint.
    #[tokio::test]
    async fn admin_credits_rejects_a_missing_or_wrong_internal_token() {
        let Some(pool) = pg_test_pool("admin_credits_rejects_a_missing_or_wrong_internal_token").await else {
            return;
        };
        let _guard = ENV_LOCK.lock().unwrap();
        unsafe { std::env::set_var("INTERNAL_TOKEN", "the-real-token") };
        let state = Arc::new(crate::test_app_state_owned_with_db(pool, None));

        let (status, _) = admin_credits(State(state.clone()), headers_with_token("wrong-token")).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);

        let (status, _) = admin_credits(State(state), HeaderMap::new()).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);

        unsafe { std::env::remove_var("INTERNAL_TOKEN") };
    }

    // ── admin_spend / admin_overview: a personal org is reported on the
    // personal plane, not counted as an organization ─────────────────────

    /// A personal org's own account (control migration 0208 `personal_orgs`)
    /// — the end state `bin/adopt_personal_org_account.rs` leaves a user in.
    /// No `account_members` row needed: neither handler under test reads it.
    async fn seed_personal_org_account(pool: &sqlx::PgPool, name: &str) -> (Uuid, String) {
        let clerk_user_id = format!("user_{}", Uuid::new_v4().simple());
        let clerk_org_id = format!("org_{}", Uuid::new_v4().simple());
        let user_id = Uuid::new_v4();
        sqlx::query("INSERT INTO users (id, email, name, clerk_user_id) VALUES ($1, $2, $3, $4)")
            .bind(user_id)
            .bind(format!("{user_id}@admin-personal-org-prtest.test"))
            .bind(name)
            .bind(&clerk_user_id)
            .execute(pool)
            .await
            .expect("insert user");
        let account_id: Uuid = sqlx::query_scalar(
            "INSERT INTO accounts (id, owner_user_id, clerk_org_id) VALUES ($1, $2, $3) RETURNING id",
        )
        .bind(Uuid::new_v4())
        .bind(user_id)
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
        (account_id, clerk_org_id)
    }

    /// A REAL org account — `clerk_org_id` set, no `personal_orgs` row, and
    /// (deliberately, for this test) no `tenant_databases` row either — the
    /// point is proving it is still COUNTED as an org (in `orgs.total`),
    /// landing in the honest `'unknown'` status bucket because nothing has
    /// provisioned a tenant database for it, same as before this PR.
    async fn seed_real_org_account(pool: &sqlx::PgPool) -> Uuid {
        let user_id = Uuid::new_v4();
        sqlx::query("INSERT INTO users (id, email) VALUES ($1, $2)")
            .bind(user_id)
            .bind(format!("{user_id}@admin-personal-org-prtest.test"))
            .execute(pool)
            .await
            .expect("insert user");
        sqlx::query_scalar(
            "INSERT INTO accounts (id, owner_user_id, clerk_org_id) VALUES ($1, $2, $3) RETURNING id",
        )
        .bind(Uuid::new_v4())
        .bind(user_id)
        .bind(format!("org_{}", Uuid::new_v4().simple()))
        .fetch_one(pool)
        .await
        .expect("insert real org account")
    }

    async fn cleanup_personal_org_account(pool: &sqlx::PgPool, account_id: Uuid, clerk_org_id: &str) {
        sqlx::query("DELETE FROM llm_usage WHERE account_id = $1").bind(account_id).execute(pool).await.ok();
        let owner: Option<Uuid> =
            sqlx::query_scalar("SELECT owner_user_id FROM accounts WHERE id = $1").bind(account_id).fetch_optional(pool).await.ok().flatten();
        sqlx::query("DELETE FROM accounts WHERE id = $1").bind(account_id).execute(pool).await.ok();
        if let Some(owner) = owner {
            sqlx::query("DELETE FROM users WHERE id = $1").bind(owner).execute(pool).await.ok();
        }
        sqlx::query("DELETE FROM personal_orgs WHERE clerk_org_id = $1")
            .bind(clerk_org_id)
            .execute(pool)
            .await
            .ok();
    }

    async fn cleanup_plain_account(pool: &sqlx::PgPool, account_id: Uuid) {
        let owner: Option<Uuid> =
            sqlx::query_scalar("SELECT owner_user_id FROM accounts WHERE id = $1").bind(account_id).fetch_optional(pool).await.ok().flatten();
        sqlx::query("DELETE FROM accounts WHERE id = $1").bind(account_id).execute(pool).await.ok();
        if let Some(owner) = owner {
            sqlx::query("DELETE FROM users WHERE id = $1").bind(owner).execute(pool).await.ok();
        }
    }

    async fn call_admin_spend(state: Arc<AppState>, month: &str) -> Value {
        let _guard = ENV_LOCK.lock().unwrap();
        unsafe { std::env::set_var("INTERNAL_TOKEN", "admin-spend-prtest-token") };
        let (status, Json(body)) = admin_spend(
            State(state),
            Query(SpendQuery { month: month.to_string() }),
            headers_with_token("admin-spend-prtest-token"),
        )
        .await;
        unsafe { std::env::remove_var("INTERNAL_TOKEN") };
        assert_eq!(status, StatusCode::OK, "expected 200, got {body:?}");
        body
    }

    async fn call_admin_overview(state: Arc<AppState>) -> Value {
        let _guard = ENV_LOCK.lock().unwrap();
        unsafe { std::env::set_var("INTERNAL_TOKEN", "admin-overview-prtest-token") };
        let (status, Json(body)) =
            admin_overview(State(state), headers_with_token("admin-overview-prtest-token")).await;
        unsafe { std::env::remove_var("INTERNAL_TOKEN") };
        assert_eq!(status, StatusCode::OK, "expected 200, got {body:?}");
        body
    }

    /// **The required positive proof, at the wire level.** A personal org's
    /// own account — `clerk_org_id` set, but mapped in `personal_orgs` — must
    /// appear in `admin_spend`'s response with `plane: "personal"`, carrying
    /// its real `llm_usage` sum. Before this PR the roster query's `WHERE
    /// a.clerk_org_id IS NULL` dropped this account entirely, and the
    /// org-plane loop never picked it up either (no `tenant_databases` row —
    /// by design, `0208_personal_orgs.sql`'s own doc comment): it was
    /// reported nowhere, its real spend silently missing from `total`.
    ///
    /// Also the negative control (PR 829 review, finding 6e): a REAL org
    /// account — `clerk_org_id` set, no `personal_orgs` row — must never be
    /// reported on the personal plane. The roster query's `OR a.clerk_org_id
    /// IN (SELECT clerk_org_id FROM personal_orgs)` clause is additive; this
    /// proves it did not also widen to include every org account.
    #[tokio::test]
    async fn admin_spend_reports_a_personal_orgs_account_on_the_personal_plane() {
        let Some(pool) =
            pg_test_pool("admin_spend_reports_a_personal_orgs_account_on_the_personal_plane").await
        else {
            return;
        };
        let (account_id, clerk_org_id) = seed_personal_org_account(&pool, "Personal Org Owner").await;
        let real_org_account = seed_real_org_account(&pool).await;
        // A deliberately huge, unmistakable sum — guarantees this row ranks
        // inside `SPEND_TOP_N` regardless of whatever else is in the shared
        // test database.
        sqlx::query(
            "INSERT INTO llm_usage (account_id, task, model, input_tokens, output_tokens, created_at) \
             VALUES ($1, 'test', 'claude-sonnet-5', 900000000, 900000000, '2020-06-15T00:00:00Z')",
        )
        .bind(account_id)
        .execute(&pool)
        .await
        .expect("insert llm_usage row");

        let state = Arc::new(crate::test_app_state_owned_with_db(pool.clone(), None));
        let body = call_admin_spend(state, "2020-06").await;

        // Captured before cleanup runs, so a failed `find` below still tears
        // down the fixture (PR 829 review, finding 6d — the panic used to
        // fire BEFORE `cleanup_personal_org_account`, leaking this row into
        // the shared test database on every failure).
        let rows = body["rows"].as_array().expect("rows must be an array").clone();
        let row = rows.iter().find(|r| r["account_id"] == json!(account_id)).cloned();

        cleanup_personal_org_account(&pool, account_id, &clerk_org_id).await;
        cleanup_plain_account(&pool, real_org_account).await;

        let row = row
            .unwrap_or_else(|| panic!("personal org's account must appear in admin_spend's rows: {rows:?}"));
        assert_eq!(
            row["plane"],
            json!("personal"),
            "a personal org's data lives on the personal plane, and must be reported there"
        );
        assert_eq!(row["input_tokens"], json!(900000000));

        assert!(
            !rows
                .iter()
                .any(|r| r["account_id"] == json!(real_org_account) && r["plane"] == json!("personal")),
            "a real org account must never be reported on the personal plane: {rows:?}"
        );
    }

    /// **The required negative control, delta-based.** `admin_overview`
    /// aggregates the WHOLE `accounts` table (shared with every other test in
    /// this database), so this asserts the CHANGE in `orgs.total` and the
    /// `'unknown'` status bucket across one real org + one personal org, never
    /// an absolute count.
    #[tokio::test]
    async fn admin_overview_counts_a_real_org_but_not_a_personal_org() {
        let Some(pool) =
            pg_test_pool("admin_overview_counts_a_real_org_but_not_a_personal_org").await
        else {
            return;
        };

        let before = call_admin_overview(Arc::new(crate::test_app_state_owned_with_db(pool.clone(), None))).await;
        let orgs_total_before = before["orgs"]["total"].as_i64().expect("orgs.total must be an integer");
        let unknown_before = before["orgs"]["by_status"]
            .as_array()
            .expect("by_status must be an array")
            .iter()
            .find(|s| s["status"] == json!("unknown"))
            .and_then(|s| s["count"].as_i64())
            .unwrap_or(0);

        let real_org_account = seed_real_org_account(&pool).await;
        let (personal_org_account, clerk_org_id) =
            seed_personal_org_account(&pool, "Overview Personal Org Owner").await;

        let after = call_admin_overview(Arc::new(crate::test_app_state_owned_with_db(pool.clone(), None))).await;
        let orgs_total_after = after["orgs"]["total"].as_i64().expect("orgs.total must be an integer");
        let unknown_after = after["orgs"]["by_status"]
            .as_array()
            .expect("by_status must be an array")
            .iter()
            .find(|s| s["status"] == json!("unknown"))
            .and_then(|s| s["count"].as_i64())
            .unwrap_or(0);

        cleanup_personal_org_account(&pool, personal_org_account, &clerk_org_id).await;
        cleanup_plain_account(&pool, real_org_account).await;

        assert_eq!(
            orgs_total_after - orgs_total_before,
            1,
            "orgs.total must count the real org but NOT the personal org (delta must be exactly 1, \
             not 2): before={orgs_total_before} after={orgs_total_after}"
        );
        assert_eq!(
            unknown_after - unknown_before,
            1,
            "the 'unknown' status bucket must gain exactly ONE row (the real org, which genuinely \
             has no tenant_databases row) — not two, which would mean the personal org leaked into \
             it: before={unknown_before} after={unknown_after}"
        );
    }
}
