//! Google Calendar catch-up sync for BOTH planes — the Google twin of
//! `workers::microsoft_calendar_sync`, plus the personal-plane sweep that
//! sibling does not need.
//!
//! # What this is
//!
//! The push channel (`routes::google_calendar_push`) is the low-latency path;
//! this worker is the backstop that guarantees every Calendar connection is
//! synced — and, through `connectors::google_calendar::fetch_pending`'s own
//! `ensure_watch` call, that every connection HAS a push channel — whether or
//! not its connect-time bootstrap succeeded. Two connection shapes:
//!
//! * An org member who connects Google Calendar gets a per-member
//!   `oauth_tokens` row (`provider = 'google'`, `owner_user_id = <member>`,
//!   `connectors::google::Surface::calendar_token_owner`), synced by
//!   [`sync_org_member`] into the member's private ocean on the org TENANT
//!   pool.
//! * A personal account gets the account-wide row (`owner_user_id IS NULL`),
//!   synced by [`sync_personal_account`] into the account's primary ocean on
//!   the consumer plane.
//!
//! [`tick`] rides `workers::tenant_harness` ([`POLL_INTERVAL`] = 30 min, same
//! cadence as the Microsoft sibling). The consumer entry discovers personal
//! connections from the TOKEN table via `resolve_google_calendar_personal_
//! accounts` (bounded by [`PERSONAL_SWEEP_LIMIT`], least-recently-ATTEMPTED
//! first — see "Rotation" below) — never from `google_watch_channels`, because
//! a connection whose `events.watch` never succeeded has no row there and
//! would otherwise be invisible to every retry path (prod 2026-10-07: one such
//! personal account sat with a Calendar-scoped token for a day, no channel,
//! no backfill, token never loaded again). The org entry is gated per org on
//! `routes::oauth::org_google_calendar_enabled_for` (default OFF), so a
//! flag-off org is never touched.
//!
//! # Rotation and dead credentials
//!
//! The sweep stamps `google_calendar_sync_attempts.last_attempt_at` for each
//! account BEFORE its pass runs, success or failure, and the resolver orders
//! by that stamp (never-attempted first). A connection that fails every pass
//! (no owner, Google unreachable) therefore moves to the back of the queue
//! each tick instead of pinning the head of a bounded sweep; a connection
//! that cannot fail better — a revoked grant — is taken out of the queue
//! entirely: `GoogleConnector::refresh_if_needed_for` classifies a `400
//! invalid_grant`, `connectors::google_calendar::fetch_pending` a `401` on a
//! just-refreshed token, both as `ConnectorError::CredentialRevoked`, which
//! each pass records through `credential_health::record_if_revoked`
//! (`oauth_tokens.credential_invalid_at`) and the resolver excludes. A
//! reconnect goes through `routes::oauth::store_token_bundle_tx`, whose
//! upsert clears the mark, so the connection is swept again from its next
//! tick.
//!
//! # One pass per connection at a time
//!
//! A sweep pass and a push pass for the SAME connection share one cursor row
//! and one ocean; run concurrently, the slower one's commit could retract a
//! coral the faster one already re-created from a newer page. Both passes
//! take a `pg_try_advisory_xact_lock` keyed on the connection (account, or
//! account + member) for their whole duration and SKIP when it is held —
//! same try-and-skip primitive as `routes::google_push::
//! run_gmail_sync_for_email`; nothing waits because the next push or tick
//! re-triggers a fresh pass anyway.
//!
//! Each pass — personal or org — is shared with the push webhook and with
//! the manual `POST /v1/connectors/google/sync` route
//! (`routes::connectors::trigger_sync`): load + refresh the token, fetch
//! the next page(s) through `connectors::google_calendar` (cursor +
//! `events.watch` channel on the connection's OWN `google_watch_channels`
//! row), ingest, and commit the cursor only if every event landed
//! (`routes::google_calendar_push::ingest_and_commit`). The route used to
//! run `GoogleConnector::sync`, which took no lock and committed the cursor
//! before the route's own ingest loop; it now reports [`PassOutcome`] the
//! way the Microsoft route arm does (`Busy` → `409`). Request-initiated
//! passes (push, route) additionally go through
//! `crate::calendar_pass_gate` so a burst cannot pin the pool; the sweep
//! does not, see that module.
//!
//! # Planes
//!
//! * `oauth_tokens` + `google_watch_channels` — CONTROL pool on both planes
//!   (watch rows are control-plane for org members too, so the push webhook's
//!   control-only channel resolver can route a notification to the org; see
//!   `connectors::google_calendar`'s module doc "Org lift").
//! * Atoms, graphs, deletion propagation — the org TENANT pool (`t.pool`) for
//!   a member; the control pool for a personal account.
//! * The account DEK — the org's tenant plane
//!   (`crypto::DekPlane::resolve_for_org`); control for a personal account.
//!   A personal account is one with NO `tenant_databases` row — plain or
//!   adopted personal-org (`clerk_org_id` set, see `personal_orgs`) — exactly
//!   the accounts `DekPlane::resolve_for_org` puts on the consumer plane.
//!
//! # Isolation
//!
//! Member A's events can only ever resolve to member A's graph:
//! [`sync_org_member`] resolves the destination with
//! `resolve_primary_graph(.., AccountPlane::Org, IngestCaller::Attributable(A))`
//! (which never auto-creates; an unprovisioned member is skipped, not failed),
//! and reads A's token with `TokenOwner::Member(A)` exactly (no
//! `IS NULL OR` fallback).

use std::sync::Arc;
use std::time::Duration;

use sqlx::PgPool;
use uuid::Uuid;

use crate::connectors::google_calendar::{self, CalendarTarget};
use crate::connectors::{credential_health, ConnectorCredentials, ConnectorError, GoogleConnector, TokenOwner};
use crate::routes::connectors::load_creds_bg;
use crate::routes::oauth::store_token_bundle;
use crate::routes::slack::{resolve_primary_graph, AccountPlane, IngestCaller};
use crate::store::{PostgresStore, Store};
use crate::tenancy::extractor::Tenant;
use crate::tenancy::runtime::{TenancyDecision, TenantContext};
use crate::tenancy::scope::begin_account_scoped;
use crate::tenancy::TenantId;
use crate::workers::tenant_harness::{Tick, WorkerTenant};
use crate::AppState;

/// How often to sweep org members for a calendar catch-up. Mirrors
/// `workers::microsoft_calendar_sync::POLL_INTERVAL`; the push channel is the
/// low-latency path, this is the backstop for a missed/expired channel.
pub const POLL_INTERVAL: Duration = Duration::from_secs(30 * 60);

/// Personal connections one consumer tick may sync. Each costs at least one
/// Google round trip, run sequentially; the remainder is picked up next tick.
/// Discovery is least-recently-ATTEMPTED first (every pass stamps
/// `google_calendar_sync_attempts`, success or failure) and skips revoked
/// credentials, so a connection that fails every tick rotates to the back
/// rather than holding a fresh one behind the bound.
pub const PERSONAL_SWEEP_LIMIT: i32 = 200;

/// What one pass ([`sync_personal_account`] / [`sync_org_member`]) did —
/// the same three answers as `workers::microsoft_calendar_sync::PassOutcome`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PassOutcome {
    /// The pass ran and `ingested` events landed (a pure-deletion or empty
    /// page is `Synced { ingested: 0 }` — the cursor still advanced).
    Synced { ingested: usize },
    /// Another pass holds this connection's lock — nothing was touched.
    Busy,
    /// Nothing to do, and not an error: a departed member, an unprovisioned
    /// member ocean, a token that vanished between discovery and here.
    Skipped(&'static str),
}

/// The single account an org tenant database belongs to (same two-line query
/// every connector worker keeps its own copy of — see
/// `workers::microsoft_calendar_sync::resolve_tenant_account`).
async fn resolve_tenant_account(control_pool: &PgPool, org_id: &str) -> Result<Uuid, String> {
    sqlx::query_scalar::<_, Uuid>("SELECT account_id FROM tenant_databases WHERE clerk_org_id = $1")
        .bind(org_id)
        .fetch_optional(control_pool)
        .await
        .map_err(|e| e.to_string())?
        .ok_or_else(|| format!("no tenant_databases row for org {org_id}"))
}

/// Every CURRENT member of `account_id` holding a member-owned Google
/// Calendar token (`provider = 'google'`, `owner_user_id IS NOT NULL` — the
/// org-plane shape only; a personal account's row is account-wide and never
/// matches). Read on the CONTROL pool inside a single-account scope. The
/// `account_members` `EXISTS` keeps a departed member's leftover token from
/// being swept into an ocean the reconciler has already archived (same
/// reasoning as `workers::microsoft_calendar_sync::member_microsoft_owners`).
/// A row marked revoked (`credential_invalid_at`) is skipped until a
/// reconnect clears it, like the personal resolver's: the mark is written
/// once, when a pass first sees the death, not retried every tick.
pub(crate) async fn member_calendar_owners(
    control_pool: &PgPool,
    account_id: Uuid,
) -> Result<Vec<Uuid>, ConnectorError> {
    let mut tx = begin_account_scoped(control_pool, account_id)
        .await
        .map_err(|e| ConnectorError::Internal(format!("member_calendar_owners scoped tx: {e}")))?;
    let rows: Vec<(Uuid,)> = sqlx::query_as(
        r#"
        SELECT DISTINCT ot.owner_user_id
        FROM oauth_tokens ot
        WHERE ot.account_id = $1 AND ot.provider = 'google' AND ot.owner_user_id IS NOT NULL
          AND ot.credential_invalid_at IS NULL
          AND EXISTS (
              SELECT 1 FROM account_members am
              WHERE am.account_id = ot.account_id AND am.user_id = ot.owner_user_id
          )
        "#,
    )
    .bind(account_id)
    .fetch_all(&mut *tx)
    .await
    .map_err(|e| ConnectorError::Internal(format!("member_calendar_owners query: {e}")))?;
    tx.rollback().await.ok();
    Ok(rows.into_iter().map(|(u,)| u).collect())
}

/// One backstop pass for one tenant. Driven by
/// `workers::tenant_harness::run_multi_tenant` as a sweep worker.
pub async fn tick(
    t: &WorkerTenant,
    control_pool: &PgPool,
    state: &Arc<AppState>,
) -> Result<Tick, String> {
    let org_id = match &t.tenant {
        TenantId::Consumer => {
            let (state, pool) = (state.clone(), control_pool.clone());
            return sweep_personal(control_pool, PERSONAL_SWEEP_LIMIT, move |account_id| {
                let (state, pool) = (state.clone(), pool.clone());
                async move { sync_personal_account(&state, &pool, account_id).await }
            })
            .await;
        }
        TenantId::Org(org_id) => org_id.clone(),
    };
    // Per-org flag, resolved BEFORE any account/token work so a flag-off org
    // costs one cached registry lookup and touches nothing.
    if !crate::routes::oauth::org_google_calendar_enabled_for(state, &org_id).await {
        return Ok(Tick::idle());
    }
    let account_id = resolve_tenant_account(control_pool, &org_id).await?;
    let owners = member_calendar_owners(control_pool, account_id)
        .await
        .map_err(|e| e.to_string())?;
    if owners.is_empty() {
        return Ok(Tick::idle());
    }

    let mut processed = 0usize;
    for owner_user_id in owners {
        match sync_org_member(state, control_pool, &t.pool, account_id, &org_id, owner_user_id).await {
            Ok(PassOutcome::Synced { .. }) => processed += 1,
            Ok(PassOutcome::Busy | PassOutcome::Skipped(_)) => {}
            Err(e) => {
                tracing::warn!(
                    %account_id, %owner_user_id, error = %e,
                    "google_calendar_sync: member sync failed"
                );
            }
        }
    }
    Ok(if processed > 0 { Tick::worked(processed) } else { Tick::idle() })
}

/// Personal accounts holding a live account-wide Google Calendar connection
/// (`provider = 'google'`, `owner_user_id IS NULL`, `credential_invalid_at IS
/// NULL`, no `tenant_databases` row, and no `clerk_org_id` unless it is a
/// `personal_orgs` one), least-recently-attempted first, at most `limit`.
/// Goes through the `SECURITY DEFINER` resolver
/// `resolve_google_calendar_personal_accounts` because this is a
/// cross-account read on the RLS-bound consumer pool — a bare `oauth_tokens`
/// query there returns zero rows and succeeds. Ids only; the token itself is
/// read inside the account's own scope by `load_creds_bg`.
pub(crate) async fn personal_calendar_accounts(
    control_pool: &PgPool,
    limit: i32,
) -> Result<Vec<Uuid>, ConnectorError> {
    sqlx::query_scalar("SELECT * FROM resolve_google_calendar_personal_accounts($1)")
        .bind(limit)
        .fetch_all(control_pool)
        .await
        .map_err(|e| ConnectorError::Internal(format!("resolve_google_calendar_personal_accounts: {e}")))
}

/// Stamp `google_calendar_sync_attempts` for `account_id` — the resolver's
/// rotation clock — in its own committed transaction BEFORE the pass runs,
/// so a pass that fails, panics or outlives the process still counts as
/// attempted. Best-effort: a failed stamp is logged and the pass still runs
/// (a rotation hiccup must never cost a sync).
async fn note_personal_attempt(control_pool: &PgPool, account_id: Uuid) {
    let stamped = async {
        let mut tx = begin_account_scoped(control_pool, account_id)
            .await
            .map_err(|e| e.to_string())?;
        sqlx::query(
            "INSERT INTO google_calendar_sync_attempts (account_id, last_attempt_at) VALUES ($1, now()) \
             ON CONFLICT (account_id) DO UPDATE SET last_attempt_at = now()",
        )
        .bind(account_id)
        .execute(&mut *tx)
        .await
        .map_err(|e| e.to_string())?;
        tx.commit().await.map_err(|e| e.to_string())
    }
    .await;
    if let Err(e) = stamped {
        tracing::warn!(%account_id, error = %e, "google_calendar_sync: failed to stamp personal sweep attempt");
    }
}

/// The consumer entry: one `sync_one` per discovered personal connection, at
/// most `limit` per call, each stamped as attempted first and run in its own
/// task so a panic inside one account's pass is that account's failure, not
/// the end of the tick for every account behind it. Sequential — one Google
/// connection at a time, as before. Generic over the pass so the sweep's
/// discovery, rotation and accounting are testable without a Google round
/// trip; [`tick`] passes [`sync_personal_account`] and
/// [`PERSONAL_SWEEP_LIMIT`].
async fn sweep_personal<F, Fut>(control_pool: &PgPool, limit: i32, sync_one: F) -> Result<Tick, String>
where
    F: Fn(Uuid) -> Fut,
    Fut: std::future::Future<Output = Result<PassOutcome, ConnectorError>> + Send + 'static,
{
    let accounts = personal_calendar_accounts(control_pool, limit)
        .await
        .map_err(|e| e.to_string())?;
    if accounts.is_empty() {
        return Ok(Tick::idle());
    }
    let mut processed = 0usize;
    for account_id in accounts {
        note_personal_attempt(control_pool, account_id).await;
        match tokio::spawn(sync_one(account_id)).await {
            Ok(Ok(PassOutcome::Synced { .. })) => processed += 1,
            Ok(Ok(PassOutcome::Busy)) => {
                tracing::info!(%account_id, "google_calendar_sync: a pass is already in flight for this account — skipping");
            }
            Ok(Ok(PassOutcome::Skipped(reason))) => {
                tracing::debug!(%account_id, reason, "google_calendar_sync: personal connection skipped");
            }
            Ok(Err(e)) => {
                tracing::warn!(%account_id, error = %e, "google_calendar_sync: personal sync failed");
            }
            Err(join) => {
                tracing::error!(%account_id, error = %join, "google_calendar_sync: personal sync panicked");
            }
        }
    }
    Ok(if processed > 0 { Tick::worked(processed) } else { Tick::idle() })
}

/// Take the per-connection pass lock (see the module doc, "One pass per
/// connection at a time"). `Some(tx)` holds `pg_try_advisory_xact_lock` on
/// `key` until `tx` is rolled back or dropped — never committed, it carries
/// no writes; `None` means another pass holds it and the caller skips.
async fn try_lock_pass(
    control_pool: &PgPool,
    key: &str,
) -> Result<Option<sqlx::Transaction<'static, sqlx::Postgres>>, ConnectorError> {
    let mut lock_tx = control_pool
        .begin()
        .await
        .map_err(|e| ConnectorError::Internal(format!("calendar pass lock: begin failed: {e}")))?;
    let acquired: bool = sqlx::query_scalar("SELECT pg_try_advisory_xact_lock(hashtext($1))")
        .bind(key)
        .fetch_one(&mut *lock_tx)
        .await
        .map_err(|e| ConnectorError::Internal(format!("calendar pass lock: acquire failed: {e}")))?;
    if !acquired {
        lock_tx.rollback().await.ok();
        return Ok(None);
    }
    Ok(Some(lock_tx))
}

/// Sync one personal account's account-wide Google Calendar connection into
/// its primary ocean on the consumer plane — the personal twin of
/// [`sync_org_member`], shared with the push webhook
/// (`routes::google_calendar_push`, the owner-less channel arm) and the
/// manual route. `Busy` when another pass for this account holds the lock
/// (module doc, "One pass per connection at a time").
///
/// Destination first (cheap, no Google call), then token load + refresh,
/// then the fetch — whose own `ensure_watch` is what gives a connection with
/// no channel yet (connect-time bootstrap failed) its `events.watch` on the
/// first pass that can. The cursor advances only after every event ingested
/// — see `routes::google_calendar_push::ingest_and_commit`. A
/// `CredentialRevoked` from the refresh or the fetch is recorded on the
/// account-wide `google` row before it is returned, guarded by the
/// credential's `row_updated_at` witness so a reconnect that landed while
/// this pass was failing on the old token is not marked
/// (`credential_health::record_if_revoked_since`).
pub(crate) async fn sync_personal_account(
    state: &Arc<AppState>,
    control_pool: &PgPool,
    account_id: Uuid,
) -> Result<PassOutcome, ConnectorError> {
    let Some(lock_tx) = try_lock_pass(control_pool, &format!("gcal_sync:{account_id}")).await? else {
        return Ok(PassOutcome::Busy);
    };
    let mut creds = None;
    let result = personal_pass(state, control_pool, account_id, &mut creds).await;
    lock_tx.rollback().await.ok();
    if let Err(e) = &result {
        let witness = creds.as_ref().and_then(|c: &ConnectorCredentials| c.row_updated_at);
        credential_health::record_if_revoked_since(control_pool, account_id, TokenOwner::AccountWide, e, witness).await;
    }
    result
}

async fn personal_pass(
    state: &Arc<AppState>,
    control_pool: &PgPool,
    account_id: Uuid,
    creds: &mut Option<ConnectorCredentials>,
) -> Result<PassOutcome, ConnectorError> {
    let owner_user_id = google_calendar::account_owner_user(control_pool, account_id)
        .await?
        .ok_or_else(|| ConnectorError::Internal("account has no owner_user_id".into()))?;
    // The member's pause or off switch for the Calendar source, before any Google
    // call or write (the fetch applies deletions too). A resumed source diffs
    // from its stored cursor.
    if crate::connectors::catalog::pause::is_paused(
        control_pool, account_id, owner_user_id, crate::connectors::catalog::pause::GOOGLE_CALENDAR,
    ).await {
        return Ok(PassOutcome::Skipped("source paused"));
    }
    let graph_id = resolve_primary_graph(
        control_pool,
        account_id,
        AccountPlane::Personal,
        IngestCaller::Attributable(owner_user_id),
    )
    .await
    .map_err(|e| ConnectorError::Internal(format!("resolve_primary_graph: {e}")))?;

    // No request `Tenant`/`TenancyDecision` to read a plane off, so resolve
    // it from the account itself through THE decision table — one indexed
    // `accounts` lookup per pass.
    let dek_plane = crate::crypto::DekPlane::resolve_for_account(state, control_pool, account_id)
        .await
        .map_err(|e| ConnectorError::Internal(format!("resolve DEK plane: {e}")))?;
    let owner = TokenOwner::AccountWide;
    let creds = creds.insert(
        load_creds_bg(control_pool, &dek_plane, account_id, "google", owner)
            .await?
            .ok_or_else(|| ConnectorError::NotConfigured("google calendar not connected".into()))?,
    );
    refresh_and_persist(state, control_pool, &dek_plane, account_id, owner, creds).await?;

    let target = CalendarTarget::personal(control_pool, account_id);
    // Same key as the manual "sync now" route (`"{provider}:{account_id}"`),
    // so a sweep pass, a push pass and a manual pass share one limiter bucket.
    let key = format!("google:{account_id}");
    let access_token = creds.access_token.clone();
    let pending = crate::graph_rate_limit::scoped(
        key,
        google_calendar::fetch_pending(&state.http_client, &target, &access_token, creds.refreshed.is_some()),
    )
    .await?;

    if pending.items.is_empty() {
        // Nothing to ingest (also a pure-deletion pass, already applied by
        // `fetch_pending`) — advance the cursor.
        google_calendar::commit_pending(&target, pending.commit).await?;
        return Ok(PassOutcome::Synced { ingested: 0 });
    }

    // `Tenant::legacy` is the personal plane's background-ingest convention
    // here — see `routes::google_calendar_push`'s module doc.
    let ingested = crate::routes::google_calendar_push::ingest_and_commit(
        state,
        &target,
        graph_id,
        owner_user_id,
        Tenant::legacy(state),
        pending,
    )
    .await?;
    Ok(PassOutcome::Synced { ingested })
}

/// Refresh + PERSIST the token BEFORE the fetch, so a fetch failure cannot
/// discard a newer token (same discipline as the Microsoft sibling). The
/// Calendar-aware `refresh_if_needed_for` names the `google` row, so an
/// `invalid_grant` surfaces as `CredentialRevoked`; a successful persist
/// moves the credential's `row_updated_at` witness to the row as written.
async fn refresh_and_persist(
    state: &Arc<AppState>,
    control_pool: &PgPool,
    dek_plane: &crate::crypto::DekPlane,
    account_id: Uuid,
    owner: TokenOwner,
    creds: &mut ConnectorCredentials,
) -> Result<(), ConnectorError> {
    GoogleConnector.refresh_if_needed_for(&state.http_client, creds, "google").await?;
    if let Some(refreshed) = creds.refreshed.as_ref() {
        match store_token_bundle(control_pool, dek_plane, account_id, "google", owner, refreshed).await {
            Ok(updated_at) => creds.row_updated_at = Some(updated_at),
            Err(resp) => tracing::warn!(
                %account_id, status = ?resp.status(),
                "google_calendar_sync: failed to persist refreshed token (non-fatal)"
            ),
        }
    }
    Ok(())
}

/// Whether `user_id` is a CURRENT member of `account_id` (live
/// `account_members` row). Read on the CONTROL pool in a single-account scope.
pub(crate) async fn is_current_member(
    control_pool: &PgPool,
    account_id: Uuid,
    user_id: Uuid,
) -> Result<bool, ConnectorError> {
    let mut tx = begin_account_scoped(control_pool, account_id)
        .await
        .map_err(|e| ConnectorError::Internal(format!("is_current_member scoped tx: {e}")))?;
    let v = crate::routes::slack::membership::is_current_member(&mut *tx, account_id, user_id)
        .await
        .map_err(|e| ConnectorError::Internal(format!("is_current_member: {e}")))?;
    tx.rollback().await.ok();
    Ok(v)
}

/// Sync one org member's Google Calendar into their member-private ocean.
/// `Skipped` covers the benign cases (no member ocean provisioned yet, the
/// token vanished mid-tick, a departed member), `Busy` another pass for
/// this member holding the lock (module doc, "One pass per connection at a
/// time").
///
/// `content_pool` is the org's TENANT pool; `control_pool` owns the token and
/// the member's watch/cursor row. The cursor advances only after every event
/// ingested (or was permanently rejected as invalid) — see
/// `routes::google_calendar_push::ingest_and_commit`. A `CredentialRevoked`
/// from the refresh or the fetch is recorded on the member's `google` row
/// before it is returned, under the same `row_updated_at` guard as
/// [`sync_personal_account`].
pub(crate) async fn sync_org_member(
    state: &Arc<AppState>,
    control_pool: &PgPool,
    content_pool: &PgPool,
    account_id: Uuid,
    org_id: &str,
    owner_user_id: Uuid,
) -> Result<PassOutcome, ConnectorError> {
    let Some(lock_tx) = try_lock_pass(control_pool, &format!("gcal_sync:{account_id}:{owner_user_id}")).await? else {
        return Ok(PassOutcome::Busy);
    };
    let mut creds = None;
    let result = org_member_pass(state, control_pool, content_pool, account_id, org_id, owner_user_id, &mut creds).await;
    lock_tx.rollback().await.ok();
    if let Err(e) = &result {
        let witness = creds.as_ref().and_then(|c: &ConnectorCredentials| c.row_updated_at);
        credential_health::record_if_revoked_since(control_pool, account_id, TokenOwner::Member(owner_user_id), e, witness).await;
    }
    result
}

async fn org_member_pass(
    state: &Arc<AppState>,
    control_pool: &PgPool,
    content_pool: &PgPool,
    account_id: Uuid,
    org_id: &str,
    owner_user_id: Uuid,
    creds: &mut Option<ConnectorCredentials>,
) -> Result<PassOutcome, ConnectorError> {
    // Current membership, checked HERE (not only in `tick`'s discovery) so the
    // push webhook -- which calls this directly with the `owner_user_id` on the
    // watch row -- can never sync a departed member's calendar into the org.
    // Their token row and channel survive offboarding; this is the gate.
    let is_member = is_current_member(control_pool, account_id, owner_user_id).await?;
    if !is_member {
        tracing::info!(%account_id, %owner_user_id, "google_calendar_sync: owner is no longer a member — skipping");
        return Ok(PassOutcome::Skipped("owner is no longer a member"));
    }
    if crate::connectors::catalog::pause::is_paused(
        control_pool, account_id, owner_user_id, crate::connectors::catalog::pause::GOOGLE_CALENDAR,
    ).await {
        return Ok(PassOutcome::Skipped("source paused"));
    }
    // Destination first (cheap, no Google call). Never auto-creates: a member
    // not yet provisioned by `workers::member_ocean_reconciler` is skipped and
    // retried next tick/push, with the cursor untouched.
    let graph_id = match resolve_primary_graph(
        content_pool,
        account_id,
        AccountPlane::Org,
        IngestCaller::Attributable(owner_user_id),
    )
    .await
    {
        Ok(g) => g,
        Err(e) => {
            tracing::debug!(
                %account_id, %owner_user_id, error = %e,
                "google_calendar_sync: member ocean not resolvable yet — skipping"
            );
            return Ok(PassOutcome::Skipped("member ocean not provisioned"));
        }
    };

    let dek_plane =
        crate::crypto::DekPlane::resolve_for_org(state, control_pool, account_id, Some(org_id))
            .await
            .map_err(|e| ConnectorError::Internal(format!("resolve DEK plane: {e}")))?;

    let owner = TokenOwner::Member(owner_user_id);
    let Some(loaded) = load_creds_bg(control_pool, &dek_plane, account_id, "google", owner).await? else {
        // Disconnected between discovery and here.
        return Ok(PassOutcome::Skipped("token row gone"));
    };
    let creds = creds.insert(loaded);
    refresh_and_persist(state, control_pool, &dek_plane, account_id, owner, creds).await?;

    let target = CalendarTarget::org_member(control_pool, content_pool, account_id, owner_user_id);
    let key = format!("google:{account_id}:{owner_user_id}");
    let access_token = creds.access_token.clone();
    let pending = crate::graph_rate_limit::scoped(
        key,
        google_calendar::fetch_pending(&state.http_client, &target, &access_token, creds.refreshed.is_some()),
    )
    .await?;

    if pending.items.is_empty() {
        // Nothing to ingest (also a pure-deletion pass, already applied by
        // `fetch_pending`) — advance the cursor.
        google_calendar::commit_pending(&target, pending.commit).await?;
        return Ok(PassOutcome::Synced { ingested: 0 });
    }

    let store: Arc<dyn Store> = Arc::new(PostgresStore::new(content_pool.clone()));
    let tenant = org_tenant(store, content_pool, org_id, account_id, owner_user_id);
    let ingested = crate::routes::google_calendar_push::ingest_and_commit(
        state,
        &target,
        graph_id,
        owner_user_id,
        tenant,
        pending,
    )
    .await?;
    Ok(PassOutcome::Synced { ingested })
}

/// A real `TenancyDecision::Org` tenant for a background ingest into
/// `pool` — never `Tenant::legacy` with a swapped pool (see
/// `workers::microsoft_calendar_sync::sync_member` / `tenancy::extractor`).
pub(crate) fn org_tenant(
    store: Arc<dyn Store>,
    pool: &PgPool,
    org_id: &str,
    account_id: Uuid,
    owner_user_id: Uuid,
) -> Tenant {
    Tenant {
        store: Some(store),
        pool: Some(pool.clone()),
        decision: Some(TenancyDecision::Org(TenantContext {
            org_id: org_id.to_string(),
            account_id,
            user_id: owner_user_id,
            pool: pool.clone(),
            page_pool: None,
        })),
        class: crate::route_class::RouteClass::Other,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------
//
// Real Postgres (`ORGT_TEST_BASE_URL` unset skips — and, like every pg test in
// this tree, passes vacuously; the PR description states whether these really
// ran). Two REAL databases per test — a control DB (`migrations_control`) and
// an org TENANT DB (`migrations_tenant`) — the production plane split for an
// ordinary org:
//
//   ORGT_TEST_BASE_URL=postgres://alex@localhost:5432/postgres \
//     cargo test -p coralswarm-backend --lib \
//     workers::google_calendar_sync::google_calendar_org_pg_tests -- --test-threads=1
//
// No test here makes an outbound Google call. The Google HTTP legs
// (`events.list`, `events.watch`) have no mock-HTTP infrastructure in this
// crate (see `backend/tests/fixtures/google_calendar/README.md`), so the
// fetch is replaced by a hand-built `PendingSync` handed to the REAL
// `ingest_and_commit` — everything downstream of the HTTP response (ingest
// into the member ocean on the tenant DB, per-member cursor commit, channel
// routing, deletion propagation, purge) runs for real.

#[cfg(test)]
mod google_calendar_org_pg_tests {
    use super::*;
    use chrono::Utc;
    use coralswarm_core::kek::{generate_dek, load_kek, wrap_dek};
    use sqlx::postgres::PgPoolOptions;
    use sqlx::{Connection, Executor, PgConnection};

    use crate::connectors::google_calendar::{PendingCommit, PendingSync};
    use crate::connectors::{NormalizedItem, TokenBundle};
    use crate::routes::oauth::store_token_bundle_tx;

    fn base_url() -> Option<String> {
        std::env::var("ORGT_TEST_BASE_URL").ok()
    }
    fn with_db(base: &str, db: &str) -> String {
        let (prefix, rest) = base.split_at(base.rfind('/').expect("url has a path /"));
        let params = rest.find('?').map(|i| &rest[i..]).unwrap_or("");
        format!("{prefix}/{db}{params}")
    }
    async fn fresh_db(base: &str, db: &str) {
        let mut conn = PgConnection::connect(base).await.expect("connect admin");
        conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.unwrap();
        conn.execute(format!("CREATE DATABASE {db}").as_str()).await.unwrap();
        conn.close().await.ok();
    }
    async fn drop_db(base: &str, db: &str) {
        let mut conn = PgConnection::connect(base).await.unwrap();
        conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
    }
    async fn control(base: &str, db: &str) -> PgPool {
        fresh_db(base, db).await;
        let pool = PgPoolOptions::new().connect(&with_db(base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();
        pool
    }
    async fn tenant(base: &str, db: &str, org_id: &str) -> PgPool {
        fresh_db(base, db).await;
        let pool = PgPoolOptions::new().connect(&with_db(base, db)).await.unwrap();
        sqlx::migrate!("./migrations_tenant").run(&pool).await.unwrap();
        sqlx::query("INSERT INTO tenant_meta (id, org_id) VALUES (true, $1)")
            .bind(org_id)
            .execute(&pool)
            .await
            .unwrap();
        pool
    }
    fn set_test_kek() {
        // SAFETY: every test sets the same constant, so concurrent writes are benign.
        unsafe { std::env::set_var("MASTER_KEK_HEX", "cc".repeat(32)) };
    }
    async fn seed_user(pool: &PgPool) -> Uuid {
        sqlx::query_scalar("INSERT INTO users(id, email) VALUES (gen_random_uuid(), $1) RETURNING id")
            .bind(format!("user-{}@test.local", Uuid::new_v4()))
            .fetch_one(pool)
            .await
            .unwrap()
    }
    /// An ORG account on the control plane plus the org's own DEK on the
    /// TENANT plane (`account_keys`) where org ingest reads it from. No
    /// `account_members` rows are seeded here: departed-member tests need
    /// genuine absence. Tests needing current membership add it explicitly
    /// on CONTROL, including the required active seat tier for org members.
    async fn seed_org_account(control: &PgPool, tenant: &PgPool, owner: Uuid, org_id: &str) -> Uuid {
        let kek = load_kek().expect("test KEK loads");
        let wrapped = wrap_dek(&kek, &generate_dek()).expect("wrap org dek");
        let account_id: Uuid = sqlx::query_scalar(
            "INSERT INTO accounts(owner_user_id, clerk_org_id, dek_wrapped) VALUES ($1, $2, $3) RETURNING id",
        )
        .bind(owner)
        .bind(org_id)
        .bind(&wrapped)
        .fetch_one(control)
        .await
        .unwrap();
        sqlx::query("INSERT INTO account_keys (account_id, dek_wrapped) VALUES ($1, $2)")
            .bind(account_id)
            .bind(&wrapped)
            .execute(tenant)
            .await
            .unwrap();
        account_id
    }
    async fn seed_personal_account(control: &PgPool, owner: Uuid) -> Uuid {
        let kek = load_kek().expect("test KEK loads");
        let wrapped = wrap_dek(&kek, &generate_dek()).expect("wrap dek");
        let id: Uuid = sqlx::query_scalar("INSERT INTO accounts(owner_user_id, dek_wrapped) VALUES ($1, $2) RETURNING id")
            .bind(owner)
            .bind(&wrapped)
            .fetch_one(control)
            .await
            .unwrap();
        sqlx::query("INSERT INTO account_members(account_id, user_id, role) VALUES ($1, $2, 'owner')")
            .bind(id)
            .bind(owner)
            .execute(control)
            .await
            .unwrap();
        id
    }
    async fn seed_member_ocean(tenant: &PgPool, account_id: Uuid, user: Uuid) -> Uuid {
        sqlx::query_scalar(
            "INSERT INTO graphs(account_id, name, kind, is_primary, owner_user_id) \
             VALUES ($1, 'member ocean', 'member', true, $2) RETURNING id",
        )
        .bind(account_id)
        .bind(user)
        .fetch_one(tenant)
        .await
        .unwrap()
    }
    /// A bare `accounts` row (with a wrapped DEK) under `clerk_org_id` — an
    /// org account when paired with [`seed_tenant_row`], or the adopted
    /// personal-org shape (`clerk_org_id` set, no routing row) on its own.
    async fn seed_account_row(control: &PgPool, owner: Uuid, clerk_org_id: &str) -> Uuid {
        let kek = load_kek().expect("test KEK loads");
        let wrapped = wrap_dek(&kek, &generate_dek()).expect("wrap dek");
        sqlx::query_scalar(
            "INSERT INTO accounts(owner_user_id, clerk_org_id, dek_wrapped) VALUES ($1, $2, $3) RETURNING id",
        )
        .bind(owner)
        .bind(clerk_org_id)
        .bind(&wrapped)
        .fetch_one(control)
        .await
        .unwrap()
    }
    /// The `personal_orgs` row that makes a `clerk_org_id` an adopted
    /// personal org (one person's own ocean) rather than a real org.
    async fn seed_personal_org(control: &PgPool, org_id: &str, clerk_user_id: &str) {
        sqlx::query("INSERT INTO personal_orgs (clerk_org_id, clerk_user_id) VALUES ($1, $2)")
            .bind(org_id)
            .bind(clerk_user_id)
            .execute(control)
            .await
            .unwrap();
    }
    /// Records which accounts a `sweep_personal` pass was asked to sync, in
    /// order; `recording(outcome)` is a pass closure that logs the visit and
    /// returns `outcome(account_id)`.
    #[derive(Clone, Default)]
    struct Visited(Arc<std::sync::Mutex<Vec<Uuid>>>);
    impl Visited {
        fn recording<O>(
            &self,
            outcome: O,
        ) -> impl Fn(Uuid) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<PassOutcome, ConnectorError>> + Send>>
        where
            O: Fn(Uuid) -> Result<PassOutcome, ConnectorError> + Send + Sync + Clone + 'static,
        {
            let seen = self.0.clone();
            move |account_id| {
                let (seen, outcome) = (seen.clone(), outcome.clone());
                Box::pin(async move {
                    seen.lock().unwrap().push(account_id);
                    outcome(account_id)
                })
            }
        }
        fn take(&self) -> Vec<Uuid> {
            std::mem::take(&mut *self.0.lock().unwrap())
        }
    }
    /// Hold the personal pass lock for `account_id` on a connection of its
    /// own — what a concurrent push pass looks like to the sweep. Released
    /// when the returned transaction is dropped/rolled back.
    async fn hold_personal_pass_lock(control: &PgPool, account_id: Uuid) -> sqlx::Transaction<'static, sqlx::Postgres> {
        let mut tx = control.begin().await.unwrap();
        sqlx::query("SELECT pg_advisory_xact_lock(hashtext('gcal_sync:' || $1::text))")
            .bind(account_id.to_string())
            .execute(&mut *tx)
            .await
            .unwrap();
        tx
    }
    /// An ACTIVE `tenant_databases` routing row — what makes an org account
    /// the org entry's territory rather than the consumer entry's.
    async fn seed_tenant_row(control: &PgPool, account_id: Uuid, org_id: &str) {
        // Enter the real account/period financial transaction for this guarded fixture write.
        let mut fixture_financial_tx = crate::tenancy::scope::begin_account_scoped(control, account_id)
            .await.unwrap();
        crate::billing::renewal::lock_period_rotation(&mut fixture_financial_tx, account_id)
            .await.unwrap();
        sqlx::query(
            "INSERT INTO tenant_databases (clerk_org_id, account_id, slug, db_secret_ref, status) \
             VALUES ($1, $2, $3, $4, 'active')",
        )
        .bind(org_id)
        .bind(account_id)
        .bind(org_id.replace('_', "-"))
        .bind(format!("secret:{org_id}"))
        .execute(&mut *fixture_financial_tx)
        .await
        .unwrap();
        fixture_financial_tx.commit().await.unwrap();
    }
    /// Real `AppState` on `ctl` whose HTTP client is routed to a closed
    /// local port: a pass that reaches its Google call fails fast without a
    /// byte leaving the machine, so a test can drive [`tick`] end to end up
    /// to that point.
    fn offline_state(ctl: &PgPool) -> Arc<AppState> {
        let mut owned = crate::test_app_state_owned_with_db_and_store(ctl.clone(), None);
        owned.http_client = reqwest::Client::builder()
            .proxy(reqwest::Proxy::all("http://127.0.0.1:9").expect("proxy url"))
            .build()
            .expect("offline client");
        Arc::new(owned)
    }
    async fn primary_graphs(pool: &PgPool, account_id: Uuid) -> i64 {
        sqlx::query_scalar("SELECT count(*) FROM graphs WHERE account_id = $1 AND is_primary = true")
            .bind(account_id)
            .fetch_one(pool)
            .await
            .unwrap()
    }
    /// Store a `provider='google'` token the way the callback does:
    /// `Some(member)` = the org-plane per-member row, `None` = the
    /// personal-plane account-wide row.
    async fn connect_calendar(pool: &PgPool, account_id: Uuid, owner: Option<Uuid>, sub: &str) {
        connect_google(pool, account_id, "google", owner, sub).await
    }
    /// [`connect_calendar`] for any Google surface's `provider` literal.
    async fn connect_google(pool: &PgPool, account_id: Uuid, provider: &str, owner: Option<Uuid>, sub: &str) {
        let bundle = TokenBundle {
            access_token: format!("access-{sub}"),
            refresh_token: Some(format!("refresh-{sub}")),
            user_token: None,
            scope: "openid email https://www.googleapis.com/auth/calendar.readonly".into(),
            expires_at: Some(Utc::now() + chrono::Duration::hours(1)),
            provider_workspace_id: format!("{sub}@example.com"),
            provider_workspace_name: Some(sub.to_string()),
            provider_user_id: sub.to_string(),
        };
        let mut tx = begin_account_scoped(pool, account_id).await.expect("scoped tx");
        store_token_bundle_tx(
            &mut tx,
            &crate::crypto::DekPlane::consumer(pool.clone()),
            account_id,
            provider,
            owner.map(TokenOwner::Member).unwrap_or(TokenOwner::AccountWide),
            &bundle,
        )
        .await
        .expect("store_token_bundle_tx");
        tx.commit().await.expect("commit");
    }
    fn item(source_id: &str, title: &str) -> NormalizedItem {
        NormalizedItem {
            source_type: google_calendar::SOURCE_TYPE.to_string(),
            source_id: source_id.to_string(),
            content: format!("{title}\n\nWhen: tomorrow"),
            participants: vec![],
            timestamp: Utc::now(),
            scope: None,
            thread_id: None,
            channel_id: None,
            author_id: None,
            author_name: None,
        }
    }
    fn worker_tenant(pool: &PgPool, org_id: &str) -> WorkerTenant {
        WorkerTenant {
            tenant: TenantId::Org(org_id.to_string()),
            pool: pool.clone(),
            store: Arc::new(PostgresStore::new(pool.clone())),
        }
    }
    async fn atoms_in(pool: &PgPool, graph_id: Uuid) -> Vec<String> {
        // The generic chunker suffixes `:chunk:N` onto the event's source id.
        sqlx::query_scalar(
            "SELECT DISTINCT split_part(source_id, ':chunk:', 1) AS sid FROM atoms \
             WHERE graph_id = $1 AND source_type = 'google_calendar' ORDER BY sid",
        )
            .bind(graph_id)
            .fetch_all(pool)
            .await
            .unwrap()
    }
    async fn calendar_atom_count(pool: &PgPool) -> i64 {
        sqlx::query_scalar("SELECT count(*) FROM atoms WHERE source_type = 'google_calendar'")
            .fetch_one(pool)
            .await
            .unwrap()
    }

    /// Discovery: ONLY this account's CURRENT members holding a MEMBER-owned
    /// `google` token. Not the account-wide personal-shape row, not another
    /// surface's row, not another account's member, not an offboarded member
    /// (token row survives but no `account_members` row).
    #[tokio::test]
    async fn discovery_finds_only_current_members_own_google_tokens_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping discovery_finds_only_current_members_own_google_tokens_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        set_test_kek();
        let ctl = control(&base, "gcal_org_disc_ctl").await;
        let founder = seed_user(&ctl).await;
        // A plain (non-org) account so `account_members` rows need no seat
        // fixture; discovery never reads `clerk_org_id`.
        let account = seed_personal_account(&ctl, founder).await;
        let a = seed_user(&ctl).await;
        let b = seed_user(&ctl).await;
        let departed = seed_user(&ctl).await;
        let gmail_only = seed_user(&ctl).await;
        for u in [a, b, gmail_only] {
            sqlx::query("INSERT INTO account_members(account_id, user_id, role) VALUES ($1, $2, 'member')")
                .bind(account)
                .bind(u)
                .execute(&ctl)
                .await
                .unwrap();
        }
        connect_calendar(&ctl, account, Some(a), "sub-a").await;
        connect_calendar(&ctl, account, Some(b), "sub-b").await;
        connect_calendar(&ctl, account, Some(departed), "sub-departed").await;
        // The personal-shape account-wide row coexisting on the account.
        connect_calendar(&ctl, account, None, "sub-wide").await;
        // A different surface's member row.
        let bundle_scope = "gmail.readonly";
        let mut tx = begin_account_scoped(&ctl, account).await.unwrap();
        store_token_bundle_tx(
            &mut tx,
            &crate::crypto::DekPlane::consumer(ctl.clone()),
            account,
            "google_gmail",
            TokenOwner::Member(gmail_only),
            &TokenBundle {
                access_token: "x".into(),
                refresh_token: Some("y".into()),
                user_token: None,
                scope: bundle_scope.into(),
                expires_at: Some(Utc::now() + chrono::Duration::hours(1)),
                provider_workspace_id: "g@example.com".into(),
                provider_workspace_name: None,
                provider_user_id: "sub-g".into(),
            },
        )
        .await
        .unwrap();
        tx.commit().await.unwrap();
        // Another account's member.
        let other_owner = seed_user(&ctl).await;
        let other = seed_personal_account(&ctl, other_owner).await;
        connect_calendar(&ctl, other, Some(other_owner), "sub-other").await;

        let mut got = member_calendar_owners(&ctl, account).await.unwrap();
        got.sort();
        let mut want = vec![a, b];
        want.sort();
        assert_eq!(got, want, "only current members' own google Member tokens");

        ctl.close().await;
        drop_db(&base, "gcal_org_disc_ctl").await;
    }

    /// A member whose Calendar credential a pass marked revoked leaves the
    /// org sweep until a reconnect clears the mark — the org twin of the
    /// personal resolver's exclusion. Without it every 30-minute tick
    /// retried the dead token against Google and re-marked the row.
    #[tokio::test]
    async fn discovery_skips_a_revoked_member_until_a_reconnect_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping discovery_skips_a_revoked_member_until_a_reconnect_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        set_test_kek();
        let ctl = control(&base, "gcal_org_disc_revoked_ctl").await;
        let founder = seed_user(&ctl).await;
        let account = seed_personal_account(&ctl, founder).await;
        let a = seed_user(&ctl).await;
        let b = seed_user(&ctl).await;
        for u in [a, b] {
            sqlx::query("INSERT INTO account_members(account_id, user_id, role) VALUES ($1, $2, 'member')")
                .bind(account)
                .bind(u)
                .execute(&ctl)
                .await
                .unwrap();
            connect_calendar(&ctl, account, Some(u), &format!("sub-{u}")).await;
        }
        credential_health::mark_credential_invalid(
            &ctl,
            account,
            TokenOwner::Member(a),
            "google",
            crate::connectors::CredentialDeathReason::GoogleInvalidGrant,
            None,
        )
        .await
        .unwrap();
        assert_eq!(member_calendar_owners(&ctl, account).await.unwrap(), vec![b], "the revoked member is not swept");

        connect_calendar(&ctl, account, Some(a), "sub-a-again").await;
        let mut got = member_calendar_owners(&ctl, account).await.unwrap();
        got.sort();
        let mut want = vec![a, b];
        want.sort();
        assert_eq!(got, want, "a reconnect puts the member back in the sweep");

        ctl.close().await;
        drop_db(&base, "gcal_org_disc_revoked_ctl").await;
    }

    /// Consumer entry is idle; the org entry is a flag-gated no-op that
    /// touches nothing while the org's `google_calendar` flag is off, and
    /// proceeds (to account resolution, which this fixture has no routing row
    /// for) once the registry override turns it on.
    #[tokio::test]
    async fn tick_is_idle_for_consumer_and_a_noop_while_the_org_flag_is_off_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping tick_is_idle_for_consumer_and_a_noop_while_the_org_flag_is_off_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        set_test_kek();
        let ctl = control(&base, "gcal_org_tick_ctl").await;
        let ten = tenant(&base, "gcal_org_tick_ten", "org_gcal_tick").await;
        let state = Arc::new(crate::test_app_state_owned_with_db_and_store(ctl.clone(), None));

        let consumer = WorkerTenant {
            tenant: TenantId::Consumer,
            pool: ctl.clone(),
            store: Arc::new(PostgresStore::new(ctl.clone())),
        };
        assert_eq!(tick(&consumer, &ctl, &state).await.unwrap().processed, 0);

        let wt = worker_tenant(&ten, "org_gcal_tick");
        // Hold the shared env lock so no concurrent test flips the env default.
        let _guard = crate::routes::oauth::GOOGLE_SURFACE_ENV_LOCK.lock().await;
        if std::env::var("ORG_GOOGLE_CALENDAR_ENABLED").is_ok() {
            eprintln!("skipping flag assertions: ORG_GOOGLE_CALENDAR_ENABLED is set in this environment");
        } else {
            // Flag off (no registry row, no env) => Ok(idle) and it never even
            // resolves the tenant account (which would error in this fixture).
            assert_eq!(tick(&wt, &ctl, &state).await.expect("flag off is Ok(idle)").processed, 0);

            // Explicit override row = off also no-ops.
            crate::connector_flags::set_override(&ctl, &state.connector_flag_cache, "org_gcal_tick", "google_calendar", false)
                .await
                .unwrap();
            assert_eq!(tick(&wt, &ctl, &state).await.unwrap().processed, 0);

            // Flip on: now past the gate (fails at account resolution — this
            // fixture has no `tenant_databases` routing row).
            crate::connector_flags::set_override(&ctl, &state.connector_flag_cache, "org_gcal_tick", "google_calendar", true)
                .await
                .unwrap();
            let err = tick(&wt, &ctl, &state).await.expect_err("flag on proceeds to account resolution");
            assert!(err.contains("no tenant_databases row"), "got: {err}");

            // The same registry row drives the connect/refusal gates.
            assert!(crate::routes::oauth::org_google_calendar_enabled_for(&state, "org_gcal_tick").await);
            crate::connector_flags::set_override(&ctl, &state.connector_flag_cache, "org_gcal_tick", "google_calendar", false)
                .await
                .unwrap();
            assert!(!crate::routes::oauth::org_google_calendar_enabled_for(&state, "org_gcal_tick").await);
            // Per-org: another org with no row stays off.
            assert!(!crate::routes::oauth::org_google_calendar_enabled_for(&state, "org_someone_else").await);
        }
        drop(_guard);

        ctl.close().await;
        ten.close().await;
        drop_db(&base, "gcal_org_tick_ten").await;
        drop_db(&base, "gcal_org_tick_ctl").await;
    }

    /// The consumer entry sweeps every personal account-wide Calendar
    /// connection. Prod 2026-10-07: a personal account connected Calendar,
    /// its connect-time `ensure_watch` failed, and nothing ever came back
    /// for it — no channel, no backfill, token never touched again. Here
    /// the pass is observable up to its Google call (refused offline by
    /// [`offline_state`]): the destination ocean is resolved — auto-created
    /// on the personal plane — before the fetch. A Gmail-only personal
    /// account and an org account with an active routing row (the org
    /// entry's territory, even when it also holds an account-wide row) are
    /// never touched.
    #[tokio::test]
    async fn consumer_tick_sweeps_personal_account_wide_calendar_connections_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping consumer_tick_sweeps_personal_account_wide_calendar_connections_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        set_test_kek();
        let ctl = control(&base, "gcal_personal_tick_ctl").await;
        let u1 = seed_user(&ctl).await;
        let u2 = seed_user(&ctl).await;
        let u3 = seed_user(&ctl).await;
        let personal = seed_personal_account(&ctl, u1).await;
        connect_calendar(&ctl, personal, None, "p1").await;
        let gmail_only = seed_personal_account(&ctl, u2).await;
        connect_google(&ctl, gmail_only, "google_gmail", Some(u2), "p2").await;
        let org = seed_account_row(&ctl, u3, "org_gcal_sweep").await;
        seed_tenant_row(&ctl, org, "org_gcal_sweep").await;
        connect_calendar(&ctl, org, Some(u3), "o-member").await;
        connect_calendar(&ctl, org, None, "o-wide").await;
        for a in [personal, gmail_only, org] {
            assert_eq!(primary_graphs(&ctl, a).await, 0, "fixture starts with no ocean");
        }

        let state = offline_state(&ctl);
        let consumer = WorkerTenant {
            tenant: TenantId::Consumer,
            pool: ctl.clone(),
            store: Arc::new(PostgresStore::new(ctl.clone())),
        };
        tick(&consumer, &ctl, &state).await.expect("consumer tick is Ok even when a pass fails");

        assert_eq!(
            primary_graphs(&ctl, personal).await,
            1,
            "the personal account-wide Calendar connection was swept: its pass resolved the destination ocean"
        );
        assert_eq!(primary_graphs(&ctl, gmail_only).await, 0, "a Gmail-only personal account is not a Calendar connection");
        assert_eq!(primary_graphs(&ctl, org).await, 0, "an org account with a routing row belongs to the org entry");

        ctl.close().await;
        drop_db(&base, "gcal_personal_tick_ctl").await;
    }

    /// Discovery for the consumer entry, through the real `SECURITY DEFINER`
    /// resolver: a plain personal account and an adopted personal-org
    /// account (`clerk_org_id` set, `personal_orgs` row, no routing row)
    /// holding the account-wide `google` row are each visited exactly once;
    /// a Gmail-only personal account, a personal account whose Calendar was
    /// disconnected (token row gone), an org account with an active routing
    /// row (member-owned AND account-wide rows), and a REAL org that was
    /// never tenant-onboarded (`clerk_org_id` set, no routing row, no
    /// `personal_orgs` row — its account-wide row must not backfill one
    /// member's calendar into the org-visible primary ocean) are not. The
    /// bound is honored.
    #[tokio::test]
    async fn personal_sweep_visits_only_consumer_plane_account_wide_calendar_connections_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping personal_sweep_visits_only_consumer_plane_account_wide_calendar_connections_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        set_test_kek();
        let ctl = control(&base, "gcal_personal_disc_ctl").await;
        let u1 = seed_user(&ctl).await;
        let u2 = seed_user(&ctl).await;
        let u3 = seed_user(&ctl).await;
        let u4 = seed_user(&ctl).await;
        let u5 = seed_user(&ctl).await;
        let u6 = seed_user(&ctl).await;
        let personal = seed_personal_account(&ctl, u1).await;
        connect_calendar(&ctl, personal, None, "p1").await;
        let adopted = seed_account_row(&ctl, u4, "org_personal_u4").await;
        seed_personal_org(&ctl, "org_personal_u4", "user_u4").await;
        connect_calendar(&ctl, adopted, None, "p4").await;
        let never_onboarded = seed_account_row(&ctl, u6, "org_real_never_onboarded").await;
        connect_calendar(&ctl, never_onboarded, None, "p6").await;
        let gmail_only = seed_personal_account(&ctl, u2).await;
        connect_google(&ctl, gmail_only, "google_gmail", Some(u2), "p2").await;
        let disconnected = seed_personal_account(&ctl, u5).await;
        connect_calendar(&ctl, disconnected, None, "p5").await;
        sqlx::query("DELETE FROM oauth_tokens WHERE account_id = $1")
            .bind(disconnected)
            .execute(&ctl)
            .await
            .unwrap();
        let org = seed_account_row(&ctl, u3, "org_gcal_disc").await;
        seed_tenant_row(&ctl, org, "org_gcal_disc").await;
        connect_calendar(&ctl, org, Some(u3), "o-member").await;
        connect_calendar(&ctl, org, None, "o-wide").await;

        let visited = Visited::default();
        let tick = sweep_personal(&ctl, PERSONAL_SWEEP_LIMIT, visited.recording(|_| Ok(PassOutcome::Synced { ingested: 0 })))
            .await
            .unwrap();
        let mut got = visited.take();
        got.sort();
        let mut want = vec![personal, adopted];
        want.sort();
        assert_eq!(got, want, "exactly the consumer-plane account-wide Calendar connections, once each");
        assert_eq!(tick.processed, 2);

        assert_eq!(personal_calendar_accounts(&ctl, 1).await.unwrap().len(), 1, "the bound is honored");

        // Nothing to find => idle, and the resolver was still consulted
        // (a missing function would be an Err, not idle).
        sqlx::query("DELETE FROM oauth_tokens WHERE owner_user_id IS NULL AND account_id <> $1")
            .bind(org)
            .execute(&ctl)
            .await
            .unwrap();
        let tick = sweep_personal(&ctl, PERSONAL_SWEEP_LIMIT, |_| async { Ok(PassOutcome::Synced { ingested: 0 }) }).await.unwrap();
        assert_eq!(tick.processed, 0);

        ctl.close().await;
        drop_db(&base, "gcal_personal_disc_ctl").await;
    }

    /// Rotation: with the sweep bound at 1 and a pass that always fails, the
    /// second tick must reach the OTHER never-synced connection (the failing
    /// one was stamped as attempted and sorts behind it) and the third comes
    /// back to the first — a connection that never succeeds cannot pin the
    /// head of the queue. A pass that PANICS is that account's failure only:
    /// the tick is `Ok`, the account behind it still runs. A connection whose
    /// credential is marked revoked (`credential_invalid_at`) leaves the
    /// queue altogether.
    #[tokio::test]
    async fn personal_sweep_rotates_past_a_failing_connection_and_skips_revoked_ones_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping personal_sweep_rotates_past_a_failing_connection_and_skips_revoked_ones_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        set_test_kek();
        let ctl = control(&base, "gcal_personal_rotate_ctl").await;
        let u1 = seed_user(&ctl).await;
        let u2 = seed_user(&ctl).await;
        let a = seed_personal_account(&ctl, u1).await;
        let b = seed_personal_account(&ctl, u2).await;
        connect_calendar(&ctl, a, None, "ra").await;
        connect_calendar(&ctl, b, None, "rb").await;

        let visited = Visited::default();
        let always_fails = visited.recording(|_| Err(ConnectorError::Internal("refresh token revoked upstream".into())));
        let tick = sweep_personal(&ctl, 1, &always_fails).await.unwrap();
        assert_eq!(tick.processed, 0, "a failed pass is not worked");
        let first = visited.take();
        assert_eq!(first.len(), 1, "bound of 1");
        let first = first[0];
        let other = if first == a { b } else { a };

        sweep_personal(&ctl, 1, &always_fails).await.unwrap();
        assert_eq!(
            visited.take(),
            vec![other],
            "the connection that just failed sorts behind the one never attempted"
        );
        sweep_personal(&ctl, 1, &always_fails).await.unwrap();
        assert_eq!(visited.take(), vec![first], "both attempted: least-recently-attempted first");
        let stamped: i64 = sqlx::query_scalar("SELECT count(*) FROM google_calendar_sync_attempts WHERE account_id = ANY($1)")
            .bind(vec![a, b])
            .fetch_one(&ctl)
            .await
            .unwrap();
        assert_eq!(stamped, 2, "every attempt is stamped, success or failure");

        // One account's pass panics; the tick survives and the other runs.
        let panics_on_first = visited.recording(move |id| {
            if id == first {
                panic!("simulated pass panic for {id}");
            }
            Ok(PassOutcome::Synced { ingested: 0 })
        });
        let tick = sweep_personal(&ctl, 2, panics_on_first).await.expect("a panicking pass does not fail the tick");
        let mut got = visited.take();
        got.sort();
        let mut want = vec![a, b];
        want.sort();
        assert_eq!(got, want, "the account behind the panicking one still ran");
        assert_eq!(tick.processed, 1);

        // A revoked credential is out of the queue until a reconnect clears it.
        sqlx::query("UPDATE oauth_tokens SET credential_invalid_at = now(), credential_invalid_reason = 'google_invalid_grant' WHERE account_id = $1")
            .bind(first)
            .execute(&ctl)
            .await
            .unwrap();
        sweep_personal(&ctl, 2, visited.recording(|_| Ok(PassOutcome::Synced { ingested: 0 }))).await.unwrap();
        assert_eq!(visited.take(), vec![other], "a revoked connection is not swept");
        // The reconnect write path clears the mark — the same upsert the
        // callback runs — and the connection is swept again.
        connect_calendar(&ctl, first, None, if first == a { "ra" } else { "rb" }).await;
        sweep_personal(&ctl, 2, visited.recording(|_| Ok(PassOutcome::Synced { ingested: 0 }))).await.unwrap();
        let mut got = visited.take();
        got.sort();
        assert_eq!(got, want, "a reconnect puts the connection back in the queue");

        ctl.close().await;
        drop_db(&base, "gcal_personal_rotate_ctl").await;
    }

    /// Mutual exclusion: while another pass holds this account's lock (a
    /// push pass, here a bare transaction on its own connection), the
    /// personal pass is `Busy` without touching anything — not even
    /// the destination ocean it otherwise resolves first. Once the lock is
    /// released the same call proceeds (and, offline, fails at the Google
    /// leg after resolving the ocean).
    #[tokio::test]
    async fn personal_pass_skips_while_another_pass_holds_the_account_lock_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping personal_pass_skips_while_another_pass_holds_the_account_lock_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        set_test_kek();
        let ctl = control(&base, "gcal_personal_lock_ctl").await;
        let u1 = seed_user(&ctl).await;
        let personal = seed_personal_account(&ctl, u1).await;
        connect_calendar(&ctl, personal, None, "lk").await;
        let state = offline_state(&ctl);

        let holder = hold_personal_pass_lock(&ctl, personal).await;
        let outcome = sync_personal_account(&state, &ctl, personal)
            .await
            .expect("a held lock is a skip, not an error");
        assert_eq!(outcome, PassOutcome::Busy);
        assert_eq!(primary_graphs(&ctl, personal).await, 0, "a skipped pass resolves nothing");

        holder.rollback().await.unwrap();
        let err = sync_personal_account(&state, &ctl, personal)
            .await
            .expect_err("lock released: the pass runs and fails offline at its Google call");
        assert!(!matches!(err, ConnectorError::CredentialRevoked { .. }), "{err}");
        assert_eq!(primary_graphs(&ctl, personal).await, 1, "the pass ran up to its Google call");

        ctl.close().await;
        drop_db(&base, "gcal_personal_lock_ctl").await;
    }

    fn owner_ctx(user_id: Uuid, account_id: Uuid) -> crate::middleware::auth::UserContext {
        crate::middleware::auth::UserContext {
            user_id,
            account_id,
            role: "owner".to_string(),
            device_id: "test-device".to_string(),
        }
    }

    /// The manual "sync now" route runs the SAME locked pass as the sweep
    /// and the push: while another pass holds the connection's lock the
    /// real `trigger_sync` handler answers `409` and touches nothing. The
    /// former route arm (`GoogleConnector::sync`) took no lock, so it ran a
    /// second, concurrent pass and committed its cursor before its own
    /// ingest loop. Released, the same request runs the pass (offline, it
    /// fails at its Google call after resolving the ocean).
    #[tokio::test]
    async fn manual_google_sync_is_409_while_a_pass_holds_the_connection_lock_pg() {
        use axum::extract::{Path, State};
        use axum::http::StatusCode;
        use axum::Extension;
        let Some(base) = base_url() else {
            eprintln!("skipping manual_google_sync_is_409_while_a_pass_holds_the_connection_lock_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        set_test_kek();
        let ctl = control(&base, "gcal_route_lock_ctl").await;
        let u1 = seed_user(&ctl).await;
        let personal = seed_personal_account(&ctl, u1).await;
        connect_calendar(&ctl, personal, None, "rt").await;
        let state = offline_state(&ctl);
        let sync_now = || {
            crate::routes::connectors::trigger_sync(
                State(state.clone()),
                Tenant::legacy(&state),
                Extension(owner_ctx(u1, personal)),
                Path("google".to_string()),
            )
        };

        let holder = hold_personal_pass_lock(&ctl, personal).await;
        let resp = sync_now().await;
        assert_eq!(resp.status(), StatusCode::CONFLICT);
        assert_eq!(primary_graphs(&ctl, personal).await, 0, "a busy connection is not touched");

        holder.rollback().await.unwrap();
        let resp = sync_now().await;
        assert_ne!(resp.status(), StatusCode::CONFLICT);
        assert!(!resp.status().is_success(), "offline: the pass fails at its Google call");
        assert_eq!(primary_graphs(&ctl, personal).await, 1, "the locked pass ran up to its Google call");

        ctl.close().await;
        drop_db(&base, "gcal_route_lock_ctl").await;
    }

    /// The pool-pinning bound: with every calendar-pass permit taken, a
    /// push-initiated pass is skipped (`None`) and a manual sync answers
    /// `429` + `Retry-After` — neither opens a lock transaction or resolves
    /// anything. A freed permit lets the same push run its pass.
    #[tokio::test]
    async fn calendar_passes_over_the_gate_bound_are_skipped_or_429_pg() {
        use crate::calendar_pass_gate::{CalendarPassGate, ROUTE_RETRY_AFTER_SECS};
        use axum::http::StatusCode;
        let Some(base) = base_url() else {
            eprintln!("skipping calendar_passes_over_the_gate_bound_are_skipped_or_429_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        set_test_kek();
        let ctl = control(&base, "gcal_gate_ctl").await;
        let u1 = seed_user(&ctl).await;
        let personal = seed_personal_account(&ctl, u1).await;
        connect_calendar(&ctl, personal, None, "gt").await;
        let state = offline_state(&ctl);
        let gate = CalendarPassGate::new(1);
        let held = gate.acquire(Duration::ZERO).await.expect("the only permit");

        let skipped = crate::routes::google_calendar_push::dispatch_push_pass(
            &gate,
            Duration::from_millis(50),
            &state,
            &ctl,
            personal,
            None,
        )
        .await;
        assert!(skipped.is_none(), "a push over the bound is skipped: {skipped:?}");

        let creds = ConnectorCredentials {
            access_token: "unused".into(),
            refresh_token: None,
            expires_at: None,
            provider_workspace_id: String::new(),
            refreshed: None,
            row_updated_at: None,
        };
        let resp = crate::routes::connectors::calendar_sync_response(
            &gate,
            &state,
            &Tenant::legacy(&state),
            &ctl,
            &owner_ctx(u1, personal),
            "google",
            TokenOwner::AccountWide,
            creds,
        )
        .await;
        assert_eq!(resp.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(
            resp.headers().get(axum::http::header::RETRY_AFTER).and_then(|v| v.to_str().ok()),
            Some(ROUTE_RETRY_AFTER_SECS.to_string().as_str())
        );
        assert_eq!(primary_graphs(&ctl, personal).await, 0, "nothing over the bound ran");

        drop(held);
        let ran = crate::routes::google_calendar_push::dispatch_push_pass(
            &gate,
            Duration::from_millis(50),
            &state,
            &ctl,
            personal,
            None,
        )
        .await;
        assert!(matches!(ran, Some(Err(_))), "a free permit runs the pass (offline, it fails at Google): {ran:?}");
        assert_eq!(primary_graphs(&ctl, personal).await, 1);
        assert_eq!(gate.available(), 1, "the permit is released when the pass ends");

        ctl.close().await;
        drop_db(&base, "gcal_gate_ctl").await;
    }

    #[tokio::test]
    async fn a_push_for_a_revoked_connection_runs_no_pass_until_a_reconnect_pg() {
        use crate::calendar_pass_gate::CalendarPassGate;
        let Some(base) = base_url() else {
            eprintln!("skipping a_push_for_a_revoked_connection_runs_no_pass_until_a_reconnect_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        set_test_kek();
        let ctl = control(&base, "gcal_push_revoked_ctl").await;
        let u1 = seed_user(&ctl).await;
        let personal = seed_personal_account(&ctl, u1).await;
        connect_calendar(&ctl, personal, None, "rv").await;
        sqlx::query(
            "UPDATE oauth_tokens SET credential_invalid_at = now(), credential_invalid_reason = 'google_invalid_grant' \
             WHERE account_id = $1",
        )
        .bind(personal)
        .execute(&ctl)
        .await
        .unwrap();
        let state = offline_state(&ctl);
        let gate = CalendarPassGate::new(1);

        let outcome = crate::routes::google_calendar_push::dispatch_push_pass(
            &gate,
            Duration::from_millis(50),
            &state,
            &ctl,
            personal,
            None,
        )
        .await;
        assert!(
            matches!(outcome, Some(Ok(PassOutcome::Skipped(_)))),
            "a revoked connection is not synced on push: {outcome:?}"
        );
        assert_eq!(primary_graphs(&ctl, personal).await, 0, "the skipped push resolved nothing");
        assert_eq!(gate.available(), 1, "and took no permit");

        connect_calendar(&ctl, personal, None, "rv").await;
        let outcome = crate::routes::google_calendar_push::dispatch_push_pass(
            &gate,
            Duration::from_millis(50),
            &state,
            &ctl,
            personal,
            None,
        )
        .await;
        assert!(matches!(outcome, Some(Err(_))), "reconnected: the pass runs (offline, fails at Google): {outcome:?}");
        assert_eq!(primary_graphs(&ctl, personal).await, 1);

        sqlx::query("UPDATE oauth_tokens SET credential_invalid_at = now() WHERE account_id = $1")
            .bind(personal).execute(&ctl).await.unwrap();
        connect_calendar(&ctl, personal, None, "rv-second").await;
        let outcome = crate::routes::google_calendar_push::dispatch_push_pass(
            &gate, Duration::from_millis(50), &state, &ctl, personal, None,
        ).await;
        assert!(matches!(outcome, Some(Err(_))), "a newer live row wins over an older revoked row: {outcome:?}");

        let org = seed_account_row(&ctl, u1, "org_push_revoked").await;
        connect_calendar(&ctl, org, Some(u1), "rv-member").await;
        connect_calendar(&ctl, org, None, "rv-wide").await;
        sqlx::query("UPDATE oauth_tokens SET credential_invalid_at = now() WHERE account_id = $1 AND owner_user_id = $2")
            .bind(org).bind(u1).execute(&ctl).await.unwrap();
        let outcome = crate::routes::google_calendar_push::dispatch_push_pass(
            &gate, Duration::from_millis(50), &state, &ctl, org, Some(u1),
        ).await;
        assert!(matches!(outcome, Some(Ok(PassOutcome::Skipped("credential unavailable or revoked")))),
            "a revoked member row is skipped before org resolution: {outcome:?}");
        assert_eq!(primary_graphs(&ctl, org).await, 0);
        assert_eq!(gate.available(), 1);

        connect_calendar(&ctl, org, Some(u1), "rv-member-new").await;
        assert_eq!(credential_health::is_credential_live(&ctl, org, TokenOwner::Member(u1), "google").await.unwrap(), Some(true));
        let creds = crate::routes::connectors::load_creds_bg(
            &ctl, &crate::crypto::DekPlane::consumer(ctl.clone()), org, "google", TokenOwner::Member(u1),
        ).await.unwrap().unwrap();
        assert_eq!(creds.access_token, "access-rv-member-new");

        ctl.close().await;
        drop_db(&base, "gcal_push_revoked_ctl").await;
    }

    #[tokio::test]
    async fn a_reconnect_begun_before_a_pass_refresh_still_outdates_its_witness_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping a_reconnect_begun_before_a_pass_refresh_still_outdates_its_witness_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        set_test_kek();
        let ctl = control(&base, "gcal_guard_c_window_ctl").await;
        let u1 = seed_user(&ctl).await;
        let personal = seed_personal_account(&ctl, u1).await;
        connect_calendar(&ctl, personal, None, "gc").await;
        let bundle = |access: &str| TokenBundle {
            access_token: access.to_string(),
            refresh_token: Some("refresh-gc".into()),
            user_token: None,
            scope: "openid email https://www.googleapis.com/auth/calendar.readonly".into(),
            expires_at: Some(Utc::now() + chrono::Duration::hours(1)),
            provider_workspace_id: "gc@example.com".into(),
            provider_workspace_name: Some("gc".into()),
            provider_user_id: "gc".into(),
        };
        let plane = crate::crypto::DekPlane::consumer(ctl.clone());

        for owner in [TokenOwner::AccountWide, TokenOwner::Member(u1)] {
            let mut reconnect = begin_account_scoped(&ctl, personal).await.unwrap();
            sqlx::query("SELECT 1").execute(&mut *reconnect).await.unwrap();
            tokio::time::sleep(Duration::from_millis(20)).await;

            let mut refresh = begin_account_scoped(&ctl, personal).await.unwrap();
            let witness = store_token_bundle_tx(&mut refresh, &plane, personal, "google", owner, &bundle("pass-refreshed"))
                .await
                .expect("pass refresh persist");
            refresh.commit().await.unwrap();
            tokio::time::sleep(Duration::from_millis(20)).await;

            let reconnected =
                store_token_bundle_tx(&mut reconnect, &plane, personal, "google", owner, &bundle("reconnected"))
                    .await
                    .expect("reconnect upsert");
            reconnect.commit().await.unwrap();
            assert!(reconnected > witness, "the reconnect is the later write: {reconnected} vs witness {witness}");

            let marked = credential_health::mark_credential_invalid(
                &ctl,
                personal,
                owner,
                "google",
                crate::connectors::CredentialDeathReason::GoogleInvalidGrant,
                Some(witness),
            )
            .await
            .unwrap();
            assert_eq!(marked, 0, "the stale pass's mark is refused");
            let live = credential_health::is_credential_live(&ctl, personal, owner, "google").await.unwrap();
            assert_eq!(live, Some(true), "the reconnected token stays live");
        }

        ctl.close().await;
        drop_db(&base, "gcal_guard_c_window_ctl").await;
    }

    /// The headline test. Two org members each get their OWN calendar events
    /// ingested into their OWN member-private ocean on the org TENANT
    /// database, each with their OWN per-member cursor row on the control
    /// plane; nothing lands in the other member's ocean, in the control DB,
    /// or in a personal account's data, and each channel routes back to its
    /// own member.
    #[tokio::test]
    async fn org_members_events_land_in_their_own_ocean_on_the_tenant_db_with_per_member_cursors_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping org_members_events_land_in_their_own_ocean_on_the_tenant_db_with_per_member_cursors_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        set_test_kek();
        let ctl = control(&base, "gcal_org_e2e_ctl").await;
        let ten = tenant(&base, "gcal_org_e2e_ten", "org_gcal_e2e").await;
        let owner = seed_user(&ctl).await;
        let org = seed_org_account(&ctl, &ten, owner, "org_gcal_e2e").await;
        let alice = seed_user(&ctl).await;
        let bob = seed_user(&ctl).await;
        let g_alice = seed_member_ocean(&ten, org, alice).await;
        let g_bob = seed_member_ocean(&ten, org, bob).await;
        // Each captured actor needs a real CONTROL membership, with an explicit org tier.
        let memberships = sqlx::query(
            "INSERT INTO account_members(account_id,user_id,role,seat_tier_id) \
             SELECT a.id,u.id,'member',t.id FROM accounts a CROSS JOIN users u CROSS JOIN seat_tiers t \
             WHERE a.id=$1 AND u.id=ANY($2::uuid[]) AND a.clerk_org_id=$3 AND a.is_org_account \
               AND t.code='standard' AND t.is_active",
        )
        .bind(org)
        .bind(&[alice, bob][..])
        .bind("org_gcal_e2e")
        .execute(&ctl)
        .await
        .expect("seed both captured calendar actors' CONTROL memberships with explicit tiers");
        assert_eq!(memberships.rows_affected(), 2, "exactly Alice and Bob must be admitted as members");

        // A personal account with its own account-wide calendar channel —
        // must be wholly unaffected.
        let carol = seed_user(&ctl).await;
        let personal = seed_personal_account(&ctl, carol).await;
        let personal_target = CalendarTarget::personal(&ctl, personal);
        let far = Utc::now() + chrono::Duration::days(7);
        google_calendar::upsert_channel_for_test(&personal_target, "chan-personal", "res-p", "tok-personal", far)
            .await
            .unwrap();

        let state = Arc::new(crate::test_app_state_owned_with_db_and_store(ctl.clone(), None));

        // ── Alice's pass ──
        let t_alice = CalendarTarget::org_member(&ctl, &ten, org, alice);
        let store: Arc<dyn Store> = Arc::new(PostgresStore::new(ten.clone()));
        let pending_a = PendingSync {
            items: vec![item("evt-1", "Alice standup"), item("evt-2", "Alice 1:1")],
            commit: PendingCommit::Complete("sync-token-alice".into()),
        };
        let n = crate::routes::google_calendar_push::ingest_and_commit(
            &state,
            &t_alice,
            g_alice,
            alice,
            org_tenant(store.clone(), &ten, "org_gcal_e2e", org, alice),
            pending_a,
        )
        .await
        .expect("org member ingest");
        assert_eq!(n, 2, "both of alice's events ingested");

        // ── Bob's pass (same source ids as alice's — different people can
        // share an event id; they must still be separate atoms/oceans) ──
        let t_bob = CalendarTarget::org_member(&ctl, &ten, org, bob);
        let pending_b = PendingSync {
            items: vec![item("evt-1", "Bob dentist")],
            commit: PendingCommit::Complete("sync-token-bob".into()),
        };
        let n = crate::routes::google_calendar_push::ingest_and_commit(
            &state,
            &t_bob,
            g_bob,
            bob,
            org_tenant(store, &ten, "org_gcal_e2e", org, bob),
            pending_b,
        )
        .await
        .expect("org member ingest");
        assert_eq!(n, 1);

        // Events landed in each member's OWN ocean on the TENANT DB.
        assert_eq!(atoms_in(&ten, g_alice).await, vec!["evt-1", "evt-2"]);
        assert_eq!(atoms_in(&ten, g_bob).await, vec!["evt-1"], "bob's ocean holds only bob's event");
        // Cross-plane isolation: nothing calendar-shaped on the control DB.
        assert_eq!(calendar_atom_count(&ctl).await, 0, "an org pass must never write an atom to the control/personal plane");

        // Per-member cursor rows on the CONTROL plane (watch rows are
        // control-plane for org members, so push routing can find them),
        // each owned by and keyed to its member; none on the tenant DB.
        let rows: Vec<(String, Option<Uuid>, String, bool)> = sqlx::query_as(
            "SELECT resource_id, owner_user_id, sync_token, backfill_complete FROM google_watch_channels \
             WHERE account_id = $1 AND surface = 'calendar' ORDER BY resource_id",
        )
        .bind(org)
        .fetch_all(&ctl)
        .await
        .unwrap();
        let mut want = vec![
            (format!("primary:{alice}"), Some(alice), "sync-token-alice".to_string(), true),
            (format!("primary:{bob}"), Some(bob), "sync-token-bob".to_string(), true),
        ];
        want.sort();
        let mut got = rows.clone();
        got.sort();
        assert_eq!(got, want, "one cursor row per member, each committed with its own sync token");
        let tenant_rows: i64 = sqlx::query_scalar("SELECT count(*) FROM google_watch_channels WHERE account_id = $1")
            .bind(org)
            .fetch_one(&ten)
            .await
            .unwrap();
        assert_eq!(tenant_rows, 0, "org-member watch rows live on the control plane only");

        // The personal account's account-wide channel is untouched.
        let personal_rows: Vec<(String, Option<Uuid>)> = sqlx::query_as(
            "SELECT resource_id, owner_user_id FROM google_watch_channels WHERE account_id = $1 AND surface = 'calendar'",
        )
        .bind(personal)
        .fetch_all(&ctl)
        .await
        .unwrap();
        assert_eq!(personal_rows, vec![("primary".to_string(), None)]);

        // Channel routing: each member's channel resolves back to THAT member.
        google_calendar::upsert_channel_for_test(&t_alice, "chan-alice", "res-a", "tok-alice", far).await.unwrap();
        google_calendar::upsert_channel_for_test(&t_bob, "chan-bob", "res-b", "tok-bob", far).await.unwrap();
        assert_eq!(
            google_calendar::resolve_channel_for_push(&ctl, "chan-alice").await.unwrap(),
            Some((org, Some(alice), "tok-alice".to_string()))
        );
        assert_eq!(
            google_calendar::resolve_channel_for_push(&ctl, "chan-bob").await.unwrap(),
            Some((org, Some(bob), "tok-bob".to_string()))
        );
        assert_eq!(
            google_calendar::resolve_channel_for_push(&ctl, "chan-personal").await.unwrap(),
            Some((personal, None, "tok-personal".to_string())),
            "the personal channel still routes to the account-wide shape"
        );

        ctl.close().await;
        ten.close().await;
        drop_db(&base, "gcal_org_e2e_ten").await;
        drop_db(&base, "gcal_org_e2e_ctl").await;
    }

    /// A member with no provisioned ocean is skipped (never a hard failure,
    /// cursor untouched) — `sync_org_member` resolves the destination before
    /// any token/Google work, so this runs without credentials.
    #[tokio::test]
    async fn member_without_an_ocean_is_skipped_and_nothing_is_written_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping member_without_an_ocean_is_skipped_and_nothing_is_written_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        set_test_kek();
        let ctl = control(&base, "gcal_org_noocean_ctl").await;
        let ten = tenant(&base, "gcal_org_noocean_ten", "org_gcal_noocean").await;
        let owner = seed_user(&ctl).await;
        // A plain account (the org seat-tier trigger forbids bare member rows
        // on an org account); the pass only needs a live `account_members` row.
        let org = seed_personal_account(&ctl, owner).await;
        let dave = seed_user(&ctl).await;
        sqlx::query("INSERT INTO account_members(account_id, user_id, role) VALUES ($1, $2, 'member')")
            .bind(org)
            .bind(dave)
            .execute(&ctl)
            .await
            .unwrap();
        assert!(is_current_member(&ctl, org, dave).await.unwrap());
        let state = Arc::new(crate::test_app_state_owned_with_db_and_store(ctl.clone(), None));

        let outcome = sync_org_member(&state, &ctl, &ten, org, "org_gcal_noocean", dave)
            .await
            .expect("an unprovisioned member is skipped, not failed");
        assert_eq!(outcome, PassOutcome::Skipped("member ocean not provisioned"));
        let rows: i64 = sqlx::query_scalar("SELECT count(*) FROM google_watch_channels WHERE account_id = $1")
            .bind(org)
            .fetch_one(&ctl)
            .await
            .unwrap();
        assert_eq!(rows, 0, "a skipped member leaves no cursor row");

        ctl.close().await;
        ten.close().await;
        drop_db(&base, "gcal_org_noocean_ten").await;
        drop_db(&base, "gcal_org_noocean_ctl").await;
    }

    /// A departed member (token row + channel survive offboarding, but no
    /// `account_members` row) is never synced, on either entry path: the
    /// membership gate lives in `sync_org_member` itself, ahead of any
    /// destination/token/Google work, even though they own a live ocean.
    #[tokio::test]
    async fn departed_member_is_never_synced_even_with_an_ocean_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping departed_member_is_never_synced_even_with_an_ocean_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        set_test_kek();
        let ctl = control(&base, "gcal_org_departed_ctl").await;
        let ten = tenant(&base, "gcal_org_departed_ten", "org_gcal_departed").await;
        let owner = seed_user(&ctl).await;
        let org = seed_org_account(&ctl, &ten, owner, "org_gcal_departed").await;
        let gone = seed_user(&ctl).await;
        let _ocean = seed_member_ocean(&ten, org, gone).await;
        connect_calendar(&ctl, org, Some(gone), "sub-gone").await;
        assert!(!is_current_member(&ctl, org, gone).await.unwrap());

        let state = Arc::new(crate::test_app_state_owned_with_db_and_store(ctl.clone(), None));
        assert_eq!(
            sync_org_member(&state, &ctl, &ten, org, "org_gcal_departed", gone).await.unwrap(),
            PassOutcome::Skipped("owner is no longer a member")
        );
        let rows: i64 = sqlx::query_scalar("SELECT count(*) FROM google_watch_channels WHERE account_id = $1")
            .bind(org)
            .fetch_one(&ctl)
            .await
            .unwrap();
        assert_eq!(rows, 0, "a departed member leaves no cursor row");

        ctl.close().await;
        ten.close().await;
        drop_db(&base, "gcal_org_departed_ten").await;
        drop_db(&base, "gcal_org_departed_ctl").await;
    }

    /// Disconnect purge: member A's disconnect removes ONLY A's channel/cursor
    /// row — never B's (same account), never the personal account's.
    #[tokio::test]
    async fn disconnect_purge_removes_only_that_members_calendar_channel_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping disconnect_purge_removes_only_that_members_calendar_channel_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        set_test_kek();
        let ctl = control(&base, "gcal_org_purge_ctl").await;
        let ten = tenant(&base, "gcal_org_purge_ten", "org_gcal_purge").await;
        let owner = seed_user(&ctl).await;
        let org = seed_org_account(&ctl, &ten, owner, "org_gcal_purge").await;
        let alice = seed_user(&ctl).await;
        let bob = seed_user(&ctl).await;
        let carol = seed_user(&ctl).await;
        let personal = seed_personal_account(&ctl, carol).await;
        let far = Utc::now() + chrono::Duration::days(7);
        google_calendar::upsert_channel_for_test(&CalendarTarget::org_member(&ctl, &ten, org, alice), "chan-a", "r", "t-a", far).await.unwrap();
        google_calendar::upsert_channel_for_test(&CalendarTarget::org_member(&ctl, &ten, org, bob), "chan-b", "r", "t-b", far).await.unwrap();
        google_calendar::upsert_channel_for_test(&CalendarTarget::personal(&ctl, personal), "chan-p", "r", "t-p", far).await.unwrap();

        let ctl_now: chrono::DateTime<Utc> = sqlx::query_scalar("SELECT now()").fetch_one(&ctl).await.unwrap();
        let ten_now: chrono::DateTime<Utc> = sqlx::query_scalar("SELECT now()").fetch_one(&ten).await.unwrap();
        let outcome = crate::connectors::google_disconnect::purge_google_connection(
            &ctl,
            &ten,
            org,
            crate::connectors::google::Surface::Calendar,
            TokenOwner::Member(alice),
            ctl_now,
            ten_now,
        )
        .await;
        assert!(!outcome.purge_had_a_failure(), "{outcome:?}");

        let left: Vec<Option<Uuid>> = sqlx::query_scalar(
            "SELECT owner_user_id FROM google_watch_channels WHERE surface = 'calendar' ORDER BY channel_id",
        )
        .fetch_all(&ctl)
        .await
        .unwrap();
        assert_eq!(left, vec![Some(bob), None], "alice's row is gone; bob's and the personal account's remain");

        ctl.close().await;
        ten.close().await;
        drop_db(&base, "gcal_org_purge_ten").await;
        drop_db(&base, "gcal_org_purge_ctl").await;
    }
}
