//! Microsoft Calendar sync worker for BOTH planes (m365 enterprise train PR
//! 3.8 for org members; the personal sweep since cs-mscal-personal-sweep).
//!
//! Per amended decision #1 (`.scratch/m365-enterprise/plan.md`): the
//! calendar pull (`connectors::microsoft_calendar::fetch_pending`,
//! `source_type = "microsoft_calendar"`) is exactly the shape `docs/
//! connectors/CONNECTOR_CONTRACT.md` §4 already permits for self-owned,
//! non-sensitive data — no carve-out needed. This worker is what CALLS it
//! on a schedule; the user-triggered `POST /v1/connectors/microsoft/sync`
//! route (`routes::connectors::trigger_sync`) runs the SAME two pass
//! functions, [`sync_member`] and [`sync_personal_account`], so a manual
//! sync and a scheduled one are one code path on either plane: same lock,
//! same commit-after-ingest, same bookkeeping.
//!
//! # Planes
//!
//! Tenant loop (rides `workers::tenant_harness`, the same harness `workers::
//! graph_subscriptions`/`workers::google_watch_renewal` already ride).
//!
//! * **Org entry** — `resolve_tenant_account` (same "resolve the org's
//!   account_id from control before working on tenant" pattern those
//!   workers use; kept as its own copy per their precedent) resolves the
//!   tenant's singleton account (ADR-0007), and [`member_microsoft_owners`]
//!   reads `oauth_tokens` on the CONTROL pool, scoped to that one account,
//!   for every DISTINCT current member holding a `TokenOwner::Member`
//!   Microsoft token. Each member's events land in THEIR OWN private ocean
//!   on the org TENANT pool (`t.pool`).
//! * **Consumer entry** — every personal account holding an account-wide
//!   Microsoft connection (`owner_user_id IS NULL`; personal-plane
//!   connections are never `Member`, by `routes::oauth::
//!   resolve_microsoft_token_target`'s design), discovered from the TOKEN
//!   table via the `SECURITY DEFINER` resolver
//!   `resolve_microsoft_calendar_personal_accounts` (control `0278` — a
//!   cross-account read on the RLS-bound consumer pool returns zero rows
//!   and succeeds without one), bounded by [`PERSONAL_SWEEP_LIMIT`],
//!   least-recently-ATTEMPTED first. Events land in the account's primary
//!   ocean on the consumer plane. Until cs-mscal-personal-sweep this entry
//!   was a deliberate no-op and a personal calendar synced only on "sync
//!   now" — no scheduled deletions, edits or history backfill, which org
//!   members had every 30 minutes (founder rule 2026-10-06: nothing is
//!   org-only or personal-only).
//!
//! A personal account is one with NO `tenant_databases` row and either no
//! `clerk_org_id` or a `personal_orgs` one — "no routing row" alone would
//! also match a real org never tenant-onboarded.
//!
//! # Rotation and dead credentials (consumer entry)
//!
//! The sweep stamps `microsoft_calendar_sync_attempts.last_attempt_at` for
//! each account BEFORE its pass runs, success or failure, and the resolver
//! orders by that stamp (never-attempted first). A connection that fails
//! every pass (no owner, Graph unreachable) therefore moves to the back of
//! the queue each tick instead of pinning the head of a bounded sweep — a
//! last-SUCCESS clock would let it starve every connection behind it. A
//! connection that cannot fail better — a revoked grant, `invalid_grant`
//! from the token endpoint (`connectors::microsoft::
//! classify_microsoft_token_error` → `ConnectorError::CredentialRevoked`) —
//! is recorded by each pass through `credential_health::record_if_revoked`
//! (`oauth_tokens.credential_invalid_at`) and the resolver excludes it. A
//! reconnect goes through `routes::oauth::store_token_bundle_tx`, whose
//! upsert clears the mark, so the connection is swept again from its next
//! tick. Each account's pass runs in its own task ([`sweep_personal`]), so
//! a panic inside one is that account's failure, not the tick's.
//!
//! # One pass per connection at a time
//!
//! A sweep pass and a manual pass for the SAME connection share one cursor
//! row and one ocean; run concurrently, the slower one's commit could
//! retract a coral the faster one already re-created from a newer page.
//! Both passes, on both planes, take a `pg_try_advisory_xact_lock` keyed on
//! the connection (`account_id` + `owner_user_id`) on the connection's
//! CONTENT pool for their whole duration ([`try_lock_pass`]) and report
//! [`PassOutcome::Busy`] when it is held — the sweep skips to the next
//! tick, the route answers `409`. Nothing waits: the next tick or click
//! re-triggers a fresh pass anyway.
//!
//! # Default-on + per-connection off switch
//!
//! `microsoft_calendar_sync_state` (migration `0189`/tenant `0067`,
//! widened to the personal row by `0272`) is the per-(account, owner) row
//! the off switch lives on. It is lazily upserted — NOT seeded at connect
//! time — the first time a pass resolves BOTH a token AND the connection's
//! ocean (`routes::slack::resolve_primary_graph`: the member's private
//! ocean on the org plane, never auto-created there — provisioning is
//! `workers::member_ocean_reconciler`'s job, an unprovisioned member is
//! skipped and retried next tick — and the auto-created primary ocean on
//! the personal plane). `sync_enabled` defaults `TRUE` on insert; once a
//! row exists, flipping it `FALSE` skips the connection on both the sweep
//! (the resolver excludes it) and the manual route. No UI ships to flip
//! it; the row and the passes' respect for it are the load-bearing part.
//!
//! # Ingest
//!
//! The atom write follows `workers::graph_ingest::process_job`'s pattern
//! for writing from a tick with no HTTP request in flight: on the org plane
//! a genuine `tenancy::extractor::Tenant` carrying a real
//! `TenancyDecision::Org(TenantContext)` (never `Tenant::legacy` with a
//! swapped pool — see that module's doc for why `ingest::submit`'s
//! reachability check needs the real decision shape),
//! on the personal plane `Tenant::legacy` (the consumer-plane background
//! ingest convention `routes::google_calendar_push` documents); in both a
//! synthetic `middleware::auth::UserContext` stamped with the connection
//! owner's `user_id` (bookkeeping provenance, not a verified caller), then
//! `routes::ingest::ingest_atom` directly. Cross-member isolation falls out
//! of `resolve_primary_graph`'s own resolution, not anything here.
//!
//! # Cadence
//!
//! [`POLL_INTERVAL`] (30 minutes) mirrors `workers::graph_subscriptions::
//! POLL_INTERVAL`. There is no push channel to miss between polls
//! (`docs/connectors/outlook.md` C7: Calendar is pull-only), so this IS the
//! only scheduled pass on either plane.
//!
//! # Sync cursor: `calendarView/delta` (fix/cal-deletions)
//!
//! Each pass runs `connectors::microsoft_calendar::fetch_pending` against
//! the connection's cursor (`microsoft_calendar_sync_state.delta_link`/
//! `delta_resume_link`/window, `0272`/`0273`): created/updated events come
//! back as items for [`ingest_and_commit`]; deleted (`@removed`) and
//! `isCancelled` events are verified against Graph and retracted from the
//! connection's own ocean — and only there — before the items are seen;
//! the cursor is committed (`commit_pending`) ONLY after every calendar
//! event in the batch ingested, so a transient ingest failure, or a manual
//! request dropped mid-batch, can never advance past a change the delta
//! reports exactly once. See that module's doc for the window, the
//! `410`/`syncStateNotFound` restart, how recurring series stay one coral,
//! and the reconcile sweep.
//!
//! # Edited events
//!
//! `ingest_atom`'s dedup is `DO NOTHING`, so on its own it would swallow an
//! EDIT (a Graph event keeps its id when its subject, time, attendees or
//! body change; the Google twin was verified in prod 2026-10-06, PR #1155).
//! [`ingest_and_commit`] therefore runs `services::source_reingest::
//! retract_if_changed_item` on each calendar event before `ingest_atom`:
//! unchanged → no-op; changed → the stale coral is retracted from THIS
//! connection's graph only and the ingest lands a fresh one.

use std::sync::Arc;
use std::time::Duration;

use sqlx::PgPool;
use uuid::Uuid;

use crate::connectors::microsoft_calendar::{self, CalendarTarget, PendingSync};
use crate::connectors::{credential_health, ConnectorCredentials, ConnectorError, MicrosoftConnector, TokenOwner};
use crate::middleware::auth::UserContext;
use crate::routes::connectors::load_creds_bg;
use crate::ingest::atom::IngestAtomRequest;
use crate::routes::oauth::store_token_bundle;
use crate::routes::slack::{resolve_primary_graph, AccountPlane, IngestCaller};
use crate::store::{PostgresStore, Store};
use crate::tenancy::extractor::Tenant;
use crate::tenancy::runtime::{TenancyDecision, TenantContext};
use crate::tenancy::scope::begin_account_scoped;
use crate::tenancy::TenantId;
use crate::workers::tenant_harness::{Tick, WorkerTenant};
use crate::AppState;

/// How often to sweep for connections due a calendar refresh. See this
/// module's doc for why this mirrors `graph_subscriptions::POLL_INTERVAL`
/// rather than getting its own tuned value.
pub const POLL_INTERVAL: Duration = Duration::from_secs(30 * 60);

/// Personal connections one consumer tick may sync. Each costs at least one
/// Graph round trip, run sequentially; the remainder is picked up next tick
/// (discovery is least-recently-attempted first, so nothing starves).
pub const PERSONAL_SWEEP_LIMIT: i32 = 200;

/// What one pass ([`sync_member`] / [`sync_personal_account`]) did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PassOutcome {
    /// The pass ran: `fetched` items came back from the delta (calendar
    /// events, plus any transcripts riding the batch), `ingested` landed.
    /// A partial batch is still `Synced` — it is recorded via
    /// [`mark_failed`], never silently cleared (see [`batch_ingest_outcome`]).
    Synced { fetched: usize, ingested: usize },
    /// Another pass holds this connection's lock — nothing was touched.
    Busy,
    /// Nothing to do, and not an error: the off switch, an unprovisioned
    /// member ocean, a token that vanished between discovery and here.
    Skipped(&'static str),
}

/// A credential the caller already loaded (and may have refreshed) under
/// `owner` — the manual route's shape, which must not load and refresh the
/// same token a second time inside the pass.
pub(crate) type LoadedCreds = (TokenOwner, ConnectorCredentials);

// ── Account resolution (org tenant) ───────────────────────────────────────────

/// The single account an org tenant database belongs to — identical
/// resolution to `workers::graph_subscriptions::resolve_tenant_account` (kept
/// as its own copy per that function's own doc comment on why: duplicating
/// one two-line query is cheaper than a shared dependency between
/// independent connector workers).
async fn resolve_tenant_account(control_pool: &PgPool, org_id: &str) -> Result<Uuid, String> {
    sqlx::query_scalar::<_, Uuid>("SELECT account_id FROM tenant_databases WHERE clerk_org_id = $1")
        .bind(org_id)
        .fetch_optional(control_pool)
        .await
        .map_err(|e| e.to_string())?
        .ok_or_else(|| format!("no tenant_databases row for org {org_id}"))
}

// ── Member discovery (control plane) ──────────────────────────────────────────

/// Every distinct member holding a `TokenOwner::Member` Microsoft token for
/// `account_id`, read on the CONTROL pool inside a single-account-scoped
/// transaction (a known account, so no cross-account `SECURITY DEFINER`
/// bootstrap is needed here — unlike the consumer entry's
/// [`personal_calendar_accounts`]).
///
/// **CURRENT members only (CodeRabbit round 1 on PR #692).** An
/// `oauth_tokens` row survives a member's offboarding — `clerk_provision::
/// remove_org_membership` deletes the member's `account_members` row and
/// tombstones them on the control plane, but nothing in that path revokes
/// the Microsoft OAuth grant or deletes `oauth_tokens` — so without this
/// filter a departed member's calendar would keep getting swept, and their
/// events keep landing in a private ocean `workers::member_ocean_reconciler`
/// has already archived out from under them. Same "`account_members` is the
/// live source of truth for who is actually still in the org right now"
/// principle that module's own doc comment states for the identical problem
/// on the archive arm — an `EXISTS` join against it here, rather than a
/// separate query, keeps the whole discovery inside the one scoped
/// transaction/round-trip. A row marked revoked (`credential_invalid_at`)
/// is skipped until a reconnect clears it, like the personal resolver's.
async fn member_microsoft_owners(
    control_pool: &PgPool,
    account_id: Uuid,
) -> Result<Vec<Uuid>, ConnectorError> {
    let mut tx = begin_account_scoped(control_pool, account_id)
        .await
        .map_err(|e| ConnectorError::Internal(format!("member_microsoft_owners scoped tx: {e}")))?;
    let rows: Vec<(Uuid,)> = sqlx::query_as(
        r#"
        SELECT DISTINCT ot.owner_user_id
        FROM oauth_tokens ot
        WHERE ot.account_id = $1 AND ot.provider = 'microsoft' AND ot.owner_user_id IS NOT NULL
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
    .map_err(|e| ConnectorError::Internal(format!("member_microsoft_owners query: {e}")))?;
    tx.rollback().await.ok();
    Ok(rows.into_iter().map(|(u,)| u).collect())
}

// ── Personal discovery (consumer plane) ───────────────────────────────────────

/// Personal accounts holding a live account-wide Microsoft connection
/// (`provider = 'microsoft'`, `owner_user_id IS NULL`, `credential_invalid_at
/// IS NULL`, `sync_enabled` not false, no `tenant_databases` row, and no
/// `clerk_org_id` unless it is a `personal_orgs` one), least-recently-
/// attempted first, at most `limit`. Through the `SECURITY DEFINER` resolver
/// because this is a cross-account read on the RLS-bound consumer pool. Ids
/// only; the token itself is read inside the account's own scope by
/// `load_creds_bg`.
pub(crate) async fn personal_calendar_accounts(
    control_pool: &PgPool,
    limit: i32,
) -> Result<Vec<Uuid>, ConnectorError> {
    sqlx::query_scalar("SELECT * FROM resolve_microsoft_calendar_personal_accounts($1)")
        .bind(limit)
        .fetch_all(control_pool)
        .await
        .map_err(|e| ConnectorError::Internal(format!("resolve_microsoft_calendar_personal_accounts: {e}")))
}

/// Stamp `microsoft_calendar_sync_attempts` for `account_id` — the
/// resolver's rotation clock — in its own committed transaction BEFORE the
/// pass runs, so a pass that fails, panics or outlives the process still
/// counts as attempted. Best-effort: a failed stamp is logged and the pass
/// still runs (a rotation hiccup must never cost a sync).
async fn note_personal_attempt(control_pool: &PgPool, account_id: Uuid) {
    let stamped = async {
        let mut tx = begin_account_scoped(control_pool, account_id)
            .await
            .map_err(|e| e.to_string())?;
        sqlx::query(
            "INSERT INTO microsoft_calendar_sync_attempts (account_id, last_attempt_at) VALUES ($1, now()) \
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
        tracing::warn!(%account_id, error = %e, "microsoft_calendar_sync: failed to stamp personal sweep attempt");
    }
}

/// The consumer entry: one `sync_one` per discovered personal connection, at
/// most `limit` per call, each stamped as attempted first and run in its own
/// task so a panic inside one account's pass is that account's failure, not
/// the end of the tick for every account behind it. Sequential — one Graph
/// connection at a time. Generic over the pass so discovery, rotation and
/// accounting are testable without a Graph round trip; [`tick`] passes
/// [`sync_personal_account`] and [`PERSONAL_SWEEP_LIMIT`].
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
                tracing::info!(%account_id, "microsoft_calendar_sync: a pass is already in flight for this account — skipping");
            }
            Ok(Ok(PassOutcome::Skipped(reason))) => {
                tracing::debug!(%account_id, reason, "microsoft_calendar_sync: personal connection skipped");
            }
            Ok(Err(e)) => {
                tracing::warn!(%account_id, error = %e, "microsoft_calendar_sync: personal sync failed");
            }
            Err(join) => {
                tracing::error!(%account_id, error = %join, "microsoft_calendar_sync: personal sync panicked");
            }
        }
    }
    Ok(if processed > 0 { Tick::worked(processed) } else { Tick::idle() })
}

// ── Per-connection pass lock ──────────────────────────────────────────────────

fn pass_lock_key(account_id: Uuid, owner_user_id: Uuid) -> String {
    format!("mscal_sync:{account_id}:{owner_user_id}")
}

/// Take the per-connection pass lock (module doc, "One pass per connection
/// at a time") on `pool` — the connection's CONTENT pool, which is the one
/// database both the sweep and the manual route reach the connection
/// through. `Some(tx)` holds `pg_try_advisory_xact_lock` on `key` until
/// `tx` is rolled back or dropped — never committed, it carries no writes;
/// `None` means another pass holds it and the caller reports
/// [`PassOutcome::Busy`].
async fn try_lock_pass(
    pool: &PgPool,
    key: &str,
) -> Result<Option<sqlx::Transaction<'static, sqlx::Postgres>>, ConnectorError> {
    let mut lock_tx = pool
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

// ── Sync-state row (content plane) ────────────────────────────────────────────

/// `microsoft_calendar_sync_state.sync_enabled` for `(account_id,
/// owner_user_id)`, lazily inserting a fresh `sync_enabled = TRUE` row (the
/// column default IS the default-on behavior — decision #1) the first time
/// this pair is seen with a resolved `graph_id`. `graph_id` is `NOT NULL` on
/// this table by design (see migration `0189`'s header): a row is written
/// only once the connection's ocean is known, never before, so the tenant
/// twin's `member_ocean_isolation` policy is always satisfiable for a row
/// that exists at all.
async fn sync_enabled(
    pool: &PgPool,
    account_id: Uuid,
    owner_user_id: Uuid,
    graph_id: Uuid,
) -> Result<bool, ConnectorError> {
    let mut tx = crate::tenancy::scope::begin_tenant_system_scoped(pool, account_id)
        .await
        .map_err(|e| ConnectorError::Internal(format!("sync_enabled scoped tx: {e}")))?;
    let row: (bool,) = sqlx::query_as(
        r#"
        INSERT INTO microsoft_calendar_sync_state (account_id, owner_user_id, graph_id)
        VALUES ($1, $2, $3)
        ON CONFLICT (account_id, owner_user_id) DO UPDATE
            -- Keep `graph_id` current (a primary ocean can change identity
            -- across a reconcile — cheap to refresh on every pass, and this
            -- UPDATE is otherwise a no-op that still returns the existing
            -- `sync_enabled`, unlike `DO NOTHING` which returns no row at
            -- all on a conflict).
            SET graph_id = EXCLUDED.graph_id
        RETURNING sync_enabled
        "#,
    )
    .bind(account_id)
    .bind(owner_user_id)
    .bind(graph_id)
    .fetch_one(&mut *tx)
    .await
    .map_err(|e| ConnectorError::Internal(format!("sync_enabled upsert: {e}")))?;
    tx.commit()
        .await
        .map_err(|e| ConnectorError::Internal(format!("sync_enabled commit: {e}")))?;
    Ok(row.0)
}

/// Record a successful sync (clears any prior error).
async fn mark_synced(pool: &PgPool, account_id: Uuid, owner_user_id: Uuid) {
    let Ok(mut tx) = crate::tenancy::scope::begin_tenant_system_scoped(pool, account_id).await
    else {
        return;
    };
    let _ = sqlx::query(
        r#"
        UPDATE microsoft_calendar_sync_state
        SET last_synced_at = now(), last_error = NULL, last_error_at = NULL, updated_at = now()
        WHERE account_id = $1 AND owner_user_id = $2
        "#,
    )
    .bind(account_id)
    .bind(owner_user_id)
    .execute(&mut *tx)
    .await;
    let _ = tx.commit().await;
}

/// Record a failed sync attempt. Best-effort: the row may not exist yet if
/// the failure happened before [`sync_enabled`] ever wrote one (e.g. the
/// connection's ocean itself failed to resolve) — a no-op `UPDATE` in that
/// case, which is fine, since there is nothing yet to attach the error to.
async fn mark_failed(pool: &PgPool, account_id: Uuid, owner_user_id: Uuid, error: &str) {
    let Ok(mut tx) = crate::tenancy::scope::begin_tenant_system_scoped(pool, account_id).await
    else {
        return;
    };
    let _ = sqlx::query(
        r#"
        UPDATE microsoft_calendar_sync_state
        SET last_error = $3, last_error_at = now(), updated_at = now()
        WHERE account_id = $1 AND owner_user_id = $2
        "#,
    )
    .bind(account_id)
    .bind(owner_user_id)
    .bind(error.chars().take(500).collect::<String>())
    .execute(&mut *tx)
    .await;
    let _ = tx.commit().await;
}

// ── Tick ─────────────────────────────────────────────────────────────────────

/// One calendar-sync pass. Driven by `workers::tenant_harness::
/// run_multi_tenant` as a sweep worker (`HarnessConfig::sweep_worker(_,
/// POLL_INTERVAL)`).
pub async fn tick(
    t: &WorkerTenant,
    control_pool: &PgPool,
    state: &Arc<AppState>,
) -> Result<Tick, String> {
    let (account_id, org_id) = match &t.tenant {
        TenantId::Consumer => {
            let (state, pool) = (state.clone(), control_pool.clone());
            return sweep_personal(control_pool, PERSONAL_SWEEP_LIMIT, move |account_id| {
                let (state, pool) = (state.clone(), pool.clone());
                // Same limiter key as the manual route (`"{provider}:
                // {account_id}"`), so a sweep pass and a click share one
                // bucket.
                crate::graph_rate_limit::scoped(format!("microsoft:{account_id}"), async move {
                    sync_personal_account(&state, &pool, account_id, None).await
                })
            })
            .await;
        }
        TenantId::Org(org_id) => {
            let account_id = resolve_tenant_account(control_pool, org_id).await?;
            (account_id, org_id.clone())
        }
    };

    let owners = member_microsoft_owners(control_pool, account_id)
        .await
        .map_err(|e| e.to_string())?;
    if owners.is_empty() {
        return Ok(Tick::idle());
    }

    let mut processed = 0usize;
    for owner_user_id in owners {
        let key = format!("microsoft:{account_id}:{owner_user_id}");
        let outcome = crate::graph_rate_limit::scoped(
            key,
            sync_member(
                &t.pool,
                control_pool,
                state,
                account_id,
                &org_id,
                owner_user_id,
                None,
                true,
            ),
        )
        .await;
        match outcome {
            Ok(PassOutcome::Synced { .. }) => processed += 1,
            Ok(PassOutcome::Busy) => {
                tracing::info!(
                    %account_id, %owner_user_id,
                    "microsoft_calendar_sync: a pass is already in flight for this member — skipping"
                );
            }
            Ok(PassOutcome::Skipped(_)) => {
                // Disabled (off switch) or ocean not yet provisioned — neither
                // is an error; just nothing to count as work this tick.
            }
            Err(e) => {
                tracing::warn!(
                    %account_id, %owner_user_id, error = %e,
                    "microsoft_calendar_sync: member sync failed"
                );
                mark_failed(&t.pool, account_id, owner_user_id, &e.to_string()).await;
            }
        }
    }
    Ok(if processed > 0 {
        Tick::worked(processed)
    } else {
        Tick::idle()
    })
}

// ── The two passes ────────────────────────────────────────────────────────────

/// Sync one org member's calendar into their private ocean on the org
/// TENANT pool (`pool`). `creds`: `None` loads and refreshes the member's
/// own `TokenOwner::Member` token here (the sweep); `Some` is the manual
/// route's already-loaded credential. `include_transcripts` adds item 51's
/// attended-meeting transcript pull to the batch (the sweep; a manual sync
/// stays calendar-only, as it always was). Holds the connection's pass
/// lock for the duration; `Err` = a real failure BEFORE any per-event
/// ingest was attempted (token/Graph error), which the caller records
/// (`tick` via [`mark_failed`]) — a `CredentialRevoked` is additionally
/// recorded on the token row here, guarded by the credential's
/// `row_updated_at` witness so a reconnect that landed mid-pass is not
/// marked (`credential_health::record_if_revoked_since`). A batch with
/// per-event failures is
/// `Synced` and recorded via [`mark_failed`], never [`mark_synced`], so a
/// partial failure is never silently cleared.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn sync_member(
    pool: &PgPool,
    control_pool: &PgPool,
    state: &Arc<AppState>,
    account_id: Uuid,
    org_id: &str,
    owner_user_id: Uuid,
    creds: Option<LoadedCreds>,
    include_transcripts: bool,
) -> Result<PassOutcome, ConnectorError> {
    // Resolve the member's own private ocean FIRST — cheap (a plain SELECT,
    // no Graph call), and both the off-switch row and the eventual ingest
    // need it. Does not auto-create (member-private-oceans PR 5's own
    // invariant): a member not yet provisioned by `workers::
    // member_ocean_reconciler` is skipped, not failed.
    let graph_id = match resolve_primary_graph(
        pool,
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
                "microsoft_calendar_sync: member ocean not resolvable yet — skipping this pass"
            );
            return Ok(PassOutcome::Skipped("member ocean not provisioned"));
        }
    };
    let Some(lock_tx) = try_lock_pass(pool, &pass_lock_key(account_id, owner_user_id)).await? else {
        return Ok(PassOutcome::Busy);
    };
    let mut creds = creds;
    let result = member_pass(pool, control_pool, state, account_id, org_id, owner_user_id, graph_id, &mut creds, include_transcripts).await;
    lock_tx.rollback().await.ok();
    if let Err(e) = &result {
        let owner = creds.as_ref().map_or(TokenOwner::Member(owner_user_id), |(o, _)| *o);
        let witness = creds.as_ref().and_then(|(_, c)| c.row_updated_at);
        credential_health::record_if_revoked_since(control_pool, account_id, owner, e, witness).await;
    }
    result
}

#[allow(clippy::too_many_arguments)]
async fn member_pass(
    pool: &PgPool,
    control_pool: &PgPool,
    state: &Arc<AppState>,
    account_id: Uuid,
    org_id: &str,
    owner_user_id: Uuid,
    graph_id: Uuid,
    creds: &mut Option<LoadedCreds>,
    include_transcripts: bool,
) -> Result<PassOutcome, ConnectorError> {
    if !sync_enabled(pool, account_id, owner_user_id, graph_id).await? {
        return Ok(PassOutcome::Skipped("sync disabled"));
    }
    // The member's pause or off switch for the Calendar source: no Graph call,
    // no atom. The next pass after a resume diffs from the stored cursor.
    if crate::connectors::catalog::pause::is_paused(
        control_pool, account_id, owner_user_id, crate::connectors::catalog::pause::MS_CALENDAR,
    ).await {
        return Ok(PassOutcome::Skipped("source paused"));
    }

    // The account's DEK plane, from the `org_id` this pass ALREADY resolved —
    // `resolve_for_org` is THE decision table (`crypto::DekPlane`), and
    // passing the known `org_id` means zero extra `accounts` round trips on
    // this per-owner path.
    let dek_plane =
        crate::crypto::DekPlane::resolve_for_org(state, control_pool, account_id, Some(org_id))
            .await
            .map_err(|e| ConnectorError::Internal(format!("resolve DEK plane: {e}")))?;
    if creds.is_none() {
        let owner = TokenOwner::Member(owner_user_id);
        match load_creds_bg(control_pool, &dek_plane, account_id, "microsoft", owner).await? {
            Some(c) => *creds = Some((owner, c)),
            // Token row vanished between discovery and here (disconnected
            // mid-tick) — not an error, just nothing left to sync.
            None => return Ok(PassOutcome::Skipped("token row gone")),
        }
    }
    // Stays in the caller's slot so its `row_updated_at` witness outlives
    // this pass — see `sync_member`.
    let (owner, creds) = creds.as_mut().map(|(o, c)| (*o, c)).expect("loaded above");
    refresh_and_persist(state, control_pool, &dek_plane, account_id, owner, creds).await?;

    let blobs = state.r2.as_deref().map(|r| r as &dyn crate::services::blob_store::PromotionBlobStore);
    let target = CalendarTarget::org_member_resolved(pool, account_id, owner_user_id, graph_id);
    let mut pending = microsoft_calendar::fetch_pending(&state.http_client, &target, &creds.access_token, blobs).await?;
    if pending.retracted > 0 {
        tracing::info!(
            %account_id, %owner_user_id, atoms_retracted = pending.retracted,
            "microsoft_calendar_sync: deleted/cancelled events retracted from the member's ocean"
        );
    }
    // Item 51: organiser meetings get a per-user transcript subscription;
    // attended-only meetings are pulled on this sweep with the same member
    // token. A tenant kill switch skips this member and does not fail the
    // calendar batch (the outer tick would otherwise mark them failed).
    if include_transcripts {
        match crate::connectors::teams_transcripts::plan_member_transcripts(
            state,
            &state.http_client,
            control_pool,
            pool,
            control_pool,
            account_id,
            owner_user_id,
            true,
            Some(graph_id),
            &creds.access_token,
            true,
        )
        .await
        {
            Ok(crate::connectors::teams_transcripts::CaptureOutcome::ScheduledSweep { items: pulled }) => {
                pending.items.extend(pulled);
            }
            Ok(_) => {}
            Err(e) => {
                tracing::warn!(
                    %account_id, %owner_user_id, error = %e,
                    "microsoft_calendar_sync: transcript plan failed — calendar batch continues"
                );
            }
        }
    }

    let store: Arc<dyn Store> = Arc::new(PostgresStore::new(pool.clone()));
    let tenant = Tenant {
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
    };
    let user_ctx = UserContext {
        user_id: owner_user_id,
        account_id,
        role: "member".into(),
        device_id: String::new(),
    };
    let batch = ingest_and_commit(state, &target, tenant, user_ctx, pending).await?;
    match batch_ingest_outcome(batch.fetched, &batch.failed_ids) {
        None => mark_synced(pool, account_id, owner_user_id).await,
        Some(message) => mark_failed(pool, account_id, owner_user_id, &message).await,
    }
    Ok(PassOutcome::Synced { fetched: batch.fetched, ingested: batch.ingested })
}

/// Sync one personal account's account-wide Microsoft connection into its
/// primary ocean on the consumer plane (`control_pool` owns the account's
/// ocean, atoms and cursor row there) — the personal twin of
/// [`sync_member`], run by the consumer sweep and by the manual route.
/// `creds`: `None` loads and refreshes the `TokenOwner::AccountWide` token
/// here; `Some` is the route's already-loaded credential. Holds the
/// connection's pass lock for the duration; a `CredentialRevoked` is
/// recorded on the token row before it is returned.
pub(crate) async fn sync_personal_account(
    state: &Arc<AppState>,
    control_pool: &PgPool,
    account_id: Uuid,
    creds: Option<LoadedCreds>,
) -> Result<PassOutcome, ConnectorError> {
    let owner_user_id = crate::connectors::microsoft::account_owner_user(control_pool, account_id)
        .await?
        .ok_or_else(|| ConnectorError::Internal(format!("microsoft calendar: account {account_id} has no owner_user_id")))?;
    let Some(lock_tx) = try_lock_pass(control_pool, &pass_lock_key(account_id, owner_user_id)).await? else {
        return Ok(PassOutcome::Busy);
    };
    let mut creds = creds;
    let result = personal_pass(state, control_pool, account_id, &mut creds).await;
    lock_tx.rollback().await.ok();
    if let Err(e) = &result {
        let owner = creds.as_ref().map_or(TokenOwner::AccountWide, |(o, _)| *o);
        let witness = creds.as_ref().and_then(|(_, c)| c.row_updated_at);
        credential_health::record_if_revoked_since(control_pool, account_id, owner, e, witness).await;
        mark_failed(control_pool, account_id, owner_user_id, &e.to_string()).await;
    }
    result
}

async fn personal_pass(
    state: &Arc<AppState>,
    control_pool: &PgPool,
    account_id: Uuid,
    creds: &mut Option<LoadedCreds>,
) -> Result<PassOutcome, ConnectorError> {
    // Destination first (cheap, no Graph call): the account's primary ocean,
    // auto-created on this plane like every personal connector's.
    let target = CalendarTarget::personal(control_pool, account_id).await?;
    let owner_user_id = target.owner_user_id;
    if !sync_enabled(control_pool, account_id, owner_user_id, target.graph_id).await? {
        return Ok(PassOutcome::Skipped("sync disabled"));
    }
    if crate::connectors::catalog::pause::is_paused(
        control_pool, account_id, owner_user_id, crate::connectors::catalog::pause::MS_CALENDAR,
    ).await {
        return Ok(PassOutcome::Skipped("source paused"));
    }

    // No request `Tenant`/`TenancyDecision` to read a plane off, so resolve
    // it from the account itself through THE decision table — one indexed
    // `accounts` lookup per pass.
    let dek_plane = crate::crypto::DekPlane::resolve_for_account(state, control_pool, account_id)
        .await
        .map_err(|e| ConnectorError::Internal(format!("resolve DEK plane: {e}")))?;
    if creds.is_none() {
        let owner = TokenOwner::AccountWide;
        match load_creds_bg(control_pool, &dek_plane, account_id, "microsoft", owner).await? {
            Some(c) => *creds = Some((owner, c)),
            None => return Ok(PassOutcome::Skipped("token row gone")),
        }
    }
    let (owner, creds) = creds.as_mut().map(|(o, c)| (*o, c)).expect("loaded above");
    refresh_and_persist(state, control_pool, &dek_plane, account_id, owner, creds).await?;

    let blobs = state.r2.as_deref().map(|r| r as &dyn crate::services::blob_store::PromotionBlobStore);
    let pending = microsoft_calendar::fetch_pending(&state.http_client, &target, &creds.access_token, blobs).await?;
    if pending.retracted > 0 {
        tracing::info!(
            %account_id, atoms_retracted = pending.retracted,
            "microsoft_calendar_sync: deleted/cancelled events retracted from the account's ocean"
        );
    }

    // `Tenant::legacy` is the personal plane's background-ingest convention
    // — see `routes::google_calendar_push`'s module doc.
    let user_ctx = UserContext {
        user_id: owner_user_id,
        account_id,
        role: "owner".into(),
        device_id: String::new(),
    };
    let batch = ingest_and_commit(state, &target, Tenant::legacy(state), user_ctx, pending).await?;
    match batch_ingest_outcome(batch.fetched, &batch.failed_ids) {
        None => mark_synced(control_pool, account_id, owner_user_id).await,
        Some(message) => mark_failed(control_pool, account_id, owner_user_id, &message).await,
    }
    Ok(PassOutcome::Synced { fetched: batch.fetched, ingested: batch.ingested })
}

/// Refresh + PERSIST the token BEFORE the fetch (PR 3.4's CodeRabbit
/// round): a fetch failure must not discard a newer token — the stored one
/// could then expire before any later successful pass persisted a
/// replacement. `refresh_if_needed` is idempotent, so a credential the
/// route already refreshed is a no-op here. A revoked grant surfaces from
/// the refresh as `CredentialRevoked` (`classify_microsoft_token_error`).
async fn refresh_and_persist(
    state: &Arc<AppState>,
    control_pool: &PgPool,
    dek_plane: &crate::crypto::DekPlane,
    account_id: Uuid,
    owner: TokenOwner,
    creds: &mut ConnectorCredentials,
) -> Result<(), ConnectorError> {
    MicrosoftConnector.refresh_if_needed(&state.http_client, creds).await?;
    if let Some(refreshed) = creds.refreshed.as_ref() {
        match store_token_bundle(control_pool, dek_plane, account_id, "microsoft", owner, refreshed).await {
            Ok(updated_at) => creds.row_updated_at = Some(updated_at),
            Err(e) => tracing::warn!(
                %account_id, status = ?e.status(),
                "microsoft_calendar_sync: failed to persist refreshed token"
            ),
        }
    }
    Ok(())
}

/// What [`ingest_and_commit`] did with one batch.
struct BatchOutcome {
    fetched: usize,
    ingested: usize,
    failed_ids: Vec<String>,
}

/// Ingest every item of `pending` into `target.graph_id`, then commit the
/// delta cursor ONLY if every CALENDAR event landed: a failed one is
/// re-reported by the same (unadvanced) delta next pass. A transcript
/// riding a member's batch does not hold the calendar cursor back — it is
/// re-planned independently by `plan_member_transcripts`.
async fn ingest_and_commit(
    state: &Arc<AppState>,
    target: &CalendarTarget<'_>,
    tenant: Tenant,
    user_ctx: UserContext,
    pending: PendingSync,
) -> Result<BatchOutcome, ConnectorError> {
    let PendingSync { items, commit, .. } = pending;
    let (account_id, owner_user_id, graph_id) = (target.account_id, target.owner_user_id, target.graph_id);
    let blobs = state.r2.as_deref().map(|r| r as &dyn crate::services::blob_store::PromotionBlobStore);
    let fetched = items.len();
    let mut ingested = 0usize;
    let mut failed_ids: Vec<String> = Vec::new();
    let mut calendar_failed = false;
    for item in items {
        // Captured before the move into `req` so a failure can NAME the
        // event: a poison event re-fails every pass (the cursor does not
        // advance past it), and "1 of 20 failed" alone gives an operator
        // nothing to find it by (CodeRabbit round 2).
        let event_source_id = item.source_id.clone();
        let is_calendar_event = crate::services::source_reingest::is_calendar_event(&item.source_type);
        // Event EDITS (module doc): retract the stale coral first — confined
        // to THIS connection's graph — so the insert below lands the new
        // content. Calendar events only: `teams_transcript` items return
        // `None` here. A failure counts as a failed ingest for this event
        // (named in `last_error`, retried next pass).
        match crate::services::source_reingest::retract_if_changed_item(
            target.pool, blobs, account_id, graph_id, &item,
        )
        .await
        {
            Ok(Some(crate::services::source_reingest::ReingestOutcome::Replaced { atoms_deleted, reefs_deleted })) => {
                tracing::info!(
                    %account_id, %owner_user_id, source_id = %event_source_id, atoms_deleted, reefs_deleted,
                    "microsoft_calendar_sync: event changed upstream — stale coral retracted, re-ingesting"
                );
            }
            Ok(_) => {}
            Err(e) => {
                tracing::warn!(
                    %account_id, %owner_user_id, error = %e, source_id = %event_source_id,
                    "microsoft_calendar_sync: could not check/retract the stored coral for a changed event — skipping it this pass"
                );
                failed_ids.push(event_source_id);
                calendar_failed |= is_calendar_event;
                continue;
            }
        }
        let req = IngestAtomRequest {
            graph_id,
            source_type: item.source_type,
            source_id: item.source_id,
            content: item.content,
            participants: item.participants,
            timestamp: item.timestamp,
            scope: item.scope,
            fts_language: None,
            channel_id: item.channel_id,
            channel_name: None,
            author_id: item.author_id,
            author_name: item.author_name,
            project: None,
            mcp_client_name: None,
            raw_content: None,
            duration_ms: None,
            segments: None,
            session_id: None,
            session_kind: None,
            session_title: None,
            repo_path: None,
            remote: None,
            hostname: None,
            harness_version: None,
            model: None,
            branch: None,
            agent_name: None,
            platform: None,
            task: None,
            parent_session_id: None,
            trusted_user_agent: None,
            client_info: None,
            code_path: None,
        };
        let source_namespace = format!("microsoft:connection:{account_id}:{owner_user_id}");
        let origin = crate::source_origin::SourceOrigin::native("microsoft_calendar","event","microsoft_calendar_sync").expect("static origin identifiers");
        let resp = crate::ingest::submit(crate::ingest::IngestRequest {
            origin: Some(&origin),
            ..crate::ingest::IngestRequest::private(crate::ingest::Member {
                provider_namespace: Some(&source_namespace),
                ..crate::ingest::Member::new(state, &tenant, &user_ctx, req)
            })
        }).await;
        if let Err(refused) = resp {
            tracing::warn!(
                %account_id, %owner_user_id, error = %refused, source_id = %event_source_id,
                "microsoft_calendar_sync: ingest_atom failed for a calendar event"
            );
            failed_ids.push(event_source_id);
            calendar_failed |= is_calendar_event;
        } else {
            ingested += 1;
        }
    }

    if calendar_failed {
        tracing::warn!(
            %account_id, %owner_user_id,
            "microsoft_calendar_sync: one or more calendar events failed to ingest — delta cursor left unchanged"
        );
    } else {
        microsoft_calendar::commit_pending(target, commit).await?;
    }
    Ok(BatchOutcome { fetched, ingested, failed_ids })
}

/// Pure decision logic for [`ingest_and_commit`]'s callers: given how many
/// events this pass attempted (`total`) and how many of those `ingest_atom`
/// calls did not return success (`failed`), decide whether the batch counts
/// as cleanly synced. `None` = mark synced (nothing to ingest, or every
/// event ingested); `Some(message)` = mark failed instead, with a
/// human-readable "N of M events failed to ingest" message, and the caller
/// must NOT also call `mark_synced` for this batch (see the call site's own
/// comment for why clearing error state on a partial failure is the bug this
/// exists to prevent).
///
/// Extracted as a pure function — rather than tested only via a pass
/// end-to-end — because forcing a REAL `ingest_atom` failure requires a live
/// Microsoft Graph credential this test tree does not have (same posture
/// this module's own pg-test module doc states for why it does not exercise
/// `tick`/`sync_member` end-to-end either); the decision this function makes
/// is exactly the part of that logic that does not need Postgres or Graph to
/// verify.
fn batch_ingest_outcome(total: usize, failed_ids: &[String]) -> Option<String> {
    if failed_ids.is_empty() {
        return None;
    }
    let failed = failed_ids.len();
    // Name up to 5 culprits so a poison event is findable from the stored
    // error alone, without truncating the row's `last_error` unboundedly.
    let sample = failed_ids
        .iter()
        .take(5)
        .cloned()
        .collect::<Vec<_>>()
        .join(", ");
    let ellipsis = if failed > 5 { ", …" } else { "" };
    Some(format!(
        "{failed} of {total} events failed to ingest (source_ids: {sample}{ellipsis})"
    ))
}

#[cfg(test)]
mod batch_ingest_outcome_tests {
    use super::batch_ingest_outcome;

    fn ids(names: &[&str]) -> Vec<String> {
        names.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn clean_batch_marks_synced() {
        assert_eq!(batch_ingest_outcome(5, &[]), None);
    }

    #[test]
    fn empty_batch_marks_synced() {
        assert_eq!(batch_ingest_outcome(0, &[]), None);
    }

    #[test]
    fn partial_failure_marks_failed_naming_the_culprits() {
        assert_eq!(
            batch_ingest_outcome(20, &ids(&["ev-a", "ev-b", "ev-c"])),
            Some("3 of 20 events failed to ingest (source_ids: ev-a, ev-b, ev-c)".to_string())
        );
    }

    #[test]
    fn total_failure_marks_failed_naming_the_culprits() {
        assert_eq!(
            batch_ingest_outcome(4, &ids(&["e1", "e2", "e3", "e4"])),
            Some("4 of 4 events failed to ingest (source_ids: e1, e2, e3, e4)".to_string())
        );
    }

    #[test]
    fn more_than_five_failures_sample_the_first_five_with_an_ellipsis() {
        let many = ids(&["a", "b", "c", "d", "e", "f", "g"]);
        assert_eq!(
            batch_ingest_outcome(20, &many),
            Some("7 of 20 events failed to ingest (source_ids: a, b, c, d, e, …)".to_string())
        );
    }
}

/// pg-gated integration tests: proves the two plane-specific reads/writes
/// this worker's tick is built from, against a REAL Postgres database. Split
/// into two groups matching the two pools [`sync_member`] actually touches —
/// [`member_microsoft_owners`] (control-plane `oauth_tokens`) and
/// [`sync_enabled`]/[`mark_synced`]/[`mark_failed`] (tenant-plane
/// `microsoft_calendar_sync_state`) — rather than one fixture spanning both,
/// since each function only ever opens one pool at a time (same reasoning
/// `microsoft_disconnect_pg_tests`'s own module doc gives for its
/// tenant-plane-only fixture: "no genuine control/tenant behavior divergence
/// to prove" by combining them here). Deliberately does NOT exercise
/// [`tick`]/[`sync_member`] end-to-end — that would require a live Microsoft
/// Graph credential, which no suite in this tree has; `graph_subscriptions_
/// worker_tenant_loop_pg.rs`'s own module doc documents the identical
/// "processing itself fails closed, discovery/isolation is what this proves"
/// posture for the same reason.
///
///   ORGT_TEST_BASE_URL=postgres://postgres:postgres@localhost:15544/postgres \
///     cargo test -p coralswarm-backend --lib "workers::microsoft_calendar_sync::microsoft_calendar_sync_pg_tests" \
///     -- --test-threads=1
#[cfg(test)]
mod microsoft_calendar_sync_pg_tests {
    use super::*;
    use sqlx::postgres::PgPoolOptions;
    use sqlx::{Connection, Executor, PgConnection};

    fn base_url() -> Option<String> {
        std::env::var("ORGT_TEST_BASE_URL").ok()
    }
    fn with_db(base: &str, db: &str) -> String {
        let (prefix, rest) = base.split_at(base.rfind('/').expect("url has a path /"));
        let params = rest.find('?').map(|i| &rest[i..]).unwrap_or("");
        format!("{prefix}/{db}{params}")
    }
    async fn recreate(base: &str, db: &str) -> PgConnection {
        let mut conn = PgConnection::connect(base).await.expect("connect admin");
        conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .unwrap();
        conn.execute(format!("CREATE DATABASE {db}").as_str())
            .await
            .unwrap();
        conn
    }
    async fn teardown(base: &str, db: &str, pool: PgPool) {
        pool.close().await;
        let mut admin_conn = PgConnection::connect(base).await.unwrap();
        admin_conn
            .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .ok();
    }

    // ── Control-plane group: member enumeration ─────────────────────────────

    async fn control_pool(base: &str, db: &str) -> PgPool {
        let pool = PgPoolOptions::new()
            .connect(&with_db(base, db))
            .await
            .unwrap();
        sqlx::migrate!("./migrations_control")
            .run(&pool)
            .await
            .unwrap();
        pool
    }
    async fn seed_user(pool: &PgPool) -> Uuid {
        sqlx::query_scalar(
            "INSERT INTO users(id, email) VALUES (gen_random_uuid(), $1) RETURNING id",
        )
        .bind(format!("user-{}@test.local", Uuid::new_v4()))
        .fetch_one(pool)
        .await
        .unwrap()
    }
    async fn seed_account(pool: &PgPool, owner: Uuid) -> Uuid {
        sqlx::query_scalar("INSERT INTO accounts(owner_user_id) VALUES ($1) RETURNING id")
            .bind(owner)
            .fetch_one(pool)
            .await
            .unwrap()
    }
    /// A CURRENT `account_members` row for `(account_id, user_id)` — the
    /// invariant `member_microsoft_owners` now requires (CodeRabbit round 1
    /// on PR #692). Omitting this call for a user is how a test represents
    /// "offboarded" / "never joined": their `oauth_tokens` row survives
    /// (nothing in this codebase deletes it on offboarding — see that
    /// function's own doc comment), but they must not be discovered.
    async fn seed_account_member(pool: &PgPool, account_id: Uuid, user_id: Uuid) {
        sqlx::query(
            "INSERT INTO account_members(account_id, user_id, role) VALUES ($1, $2, 'member')",
        )
        .bind(account_id)
        .bind(user_id)
        .execute(pool)
        .await
        .unwrap();
    }
    #[allow(clippy::too_many_arguments)]
    async fn seed_oauth_token(
        pool: &PgPool,
        account_id: Uuid,
        provider: &str,
        owner_user_id: Option<Uuid>,
        provider_workspace_id: &str,
    ) {
        sqlx::query(
            "INSERT INTO oauth_tokens \
                 (account_id, provider, scope, access_token, provider_workspace_id, owner_user_id) \
             VALUES ($1, $2, 'offline_access', $3, $4, $5)",
        )
        .bind(account_id)
        .bind(provider)
        .bind(b"dummy-not-decrypted-by-this-test".as_slice())
        .bind(provider_workspace_id)
        .bind(owner_user_id)
        .execute(pool)
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn member_microsoft_owners_finds_only_member_owned_microsoft_tokens_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping member_microsoft_owners_finds_only_member_owned_microsoft_tokens_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "m365_38_owners_filter";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let conn = recreate(&base, db).await;
        conn.close().await.ok();
        let pool = control_pool(&base, db).await;

        let founder = seed_user(&pool).await;
        let account_id = seed_account(&pool, founder).await;
        let member_a = seed_user(&pool).await;
        let member_b = seed_user(&pool).await;
        let other_account_member = seed_user(&pool).await;
        let other_account_id = seed_account(&pool, other_account_member).await;
        seed_account_member(&pool, account_id, member_a).await;
        seed_account_member(&pool, account_id, member_b).await;

        // Counted: two distinct members with a Microsoft Member token on
        // THIS account.
        seed_oauth_token(&pool, account_id, "microsoft", Some(member_a), "tenant-a").await;
        seed_oauth_token(&pool, account_id, "microsoft", Some(member_b), "tenant-a").await;
        // NOT counted: an account-wide (legacy/personal-shape) row — no
        // `owner_user_id` — coexisting on the same account.
        seed_oauth_token(&pool, account_id, "microsoft", None, "tenant-a").await;
        // NOT counted: a different provider's Member token on the same
        // account (Gmail's own per-member isolation is a different sweep).
        seed_oauth_token(&pool, account_id, "google_gmail", Some(member_a), "a@x.com").await;
        // NOT counted: a Microsoft Member token on a DIFFERENT account
        // (cross-account leakage would be a real isolation bug).
        seed_oauth_token(
            &pool,
            other_account_id,
            "microsoft",
            Some(member_a),
            "tenant-other",
        )
        .await;

        let mut owners = member_microsoft_owners(&pool, account_id).await.unwrap();
        owners.sort();
        let mut expected = vec![member_a, member_b];
        expected.sort();
        assert_eq!(owners, expected);

        teardown(&base, db, pool).await;
    }

    #[tokio::test]
    async fn member_microsoft_owners_excludes_offboarded_members_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping member_microsoft_owners_excludes_offboarded_members_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "m365_38_owners_offboarded";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let conn = recreate(&base, db).await;
        conn.close().await.ok();
        let pool = control_pool(&base, db).await;

        let founder = seed_user(&pool).await;
        let account_id = seed_account(&pool, founder).await;
        let current_member = seed_user(&pool).await;
        let offboarded_member = seed_user(&pool).await;

        // `current_member` has BOTH a Microsoft token and a live
        // `account_members` row — discovered.
        seed_account_member(&pool, account_id, current_member).await;
        seed_oauth_token(
            &pool,
            account_id,
            "microsoft",
            Some(current_member),
            "tenant-a",
        )
        .await;

        // `offboarded_member` has a Microsoft token but NO `account_members`
        // row — the exact shape offboarding leaves behind (`clerk_provision::
        // remove_org_membership` deletes the membership row but never
        // revokes the OAuth grant). Must NOT be discovered (CodeRabbit round
        // 1 finding on PR #692, offboarded members still synced).
        seed_oauth_token(
            &pool,
            account_id,
            "microsoft",
            Some(offboarded_member),
            "tenant-a",
        )
        .await;

        let owners = member_microsoft_owners(&pool, account_id).await.unwrap();
        assert_eq!(
            owners,
            vec![current_member],
            "a Microsoft token with no live account_members row must be excluded from discovery"
        );

        teardown(&base, db, pool).await;
    }

    /// A member whose credential a pass marked revoked (now including an
    /// inactivity-expired refresh token, AADSTS 70008/700082) leaves the
    /// org sweep until a reconnect clears the mark — the org twin of the
    /// personal resolver's exclusion, so a dead token is not retried and
    /// re-marked every tick.
    #[tokio::test]
    async fn member_microsoft_owners_skips_a_revoked_member_until_cleared_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping member_microsoft_owners_skips_a_revoked_member_until_cleared_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "mscal_owners_revoked";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let conn = recreate(&base, db).await;
        conn.close().await.ok();
        let pool = control_pool(&base, db).await;

        let founder = seed_user(&pool).await;
        let account_id = seed_account(&pool, founder).await;
        let live = seed_user(&pool).await;
        let dead = seed_user(&pool).await;
        for u in [live, dead] {
            seed_account_member(&pool, account_id, u).await;
            seed_oauth_token(&pool, account_id, "microsoft", Some(u), "tenant-a").await;
        }
        credential_health::mark_credential_invalid(
            &pool,
            account_id,
            TokenOwner::Member(dead),
            "microsoft",
            crate::connectors::CredentialDeathReason::MicrosoftRefreshTokenInactive,
            None,
        )
        .await
        .unwrap();
        assert_eq!(member_microsoft_owners(&pool, account_id).await.unwrap(), vec![live]);

        credential_health::clear_credential_invalid(&pool, account_id, TokenOwner::Member(dead), "microsoft")
            .await
            .unwrap();
        let mut owners = member_microsoft_owners(&pool, account_id).await.unwrap();
        owners.sort();
        let mut expected = vec![live, dead];
        expected.sort();
        assert_eq!(owners, expected, "a cleared mark puts the member back in the sweep");

        teardown(&base, db, pool).await;
    }

    #[tokio::test]
    async fn member_microsoft_owners_empty_for_an_account_with_no_member_tokens_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping member_microsoft_owners_empty_for_an_account_with_no_member_tokens_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "m365_38_owners_empty";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let conn = recreate(&base, db).await;
        conn.close().await.ok();
        let pool = control_pool(&base, db).await;

        let founder = seed_user(&pool).await;
        let account_id = seed_account(&pool, founder).await;
        // Only an account-wide row — the ordinary personal-plane shape.
        seed_oauth_token(&pool, account_id, "microsoft", None, "tenant-a").await;

        let owners = member_microsoft_owners(&pool, account_id).await.unwrap();
        assert!(owners.is_empty());

        teardown(&base, db, pool).await;
    }

    // ── Consumer-plane group: the personal sweep ────────────────────────────
    //
    // Real tokens (encrypted under the test KEK through the same
    // `store_token_bundle_tx` the OAuth callback uses) so a pass gets as far
    // as its Graph call, which `offline_state` refuses locally.

    use coralswarm_core::kek::{generate_dek, load_kek, wrap_dek};

    use crate::connectors::TokenBundle;
    use crate::routes::oauth::store_token_bundle_tx;

    fn set_test_kek() {
        // SAFETY: every test sets the same constant, so concurrent writes are benign.
        unsafe { std::env::set_var("MASTER_KEK_HEX", "cc".repeat(32)) };
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
    /// A bare `accounts` row (with a wrapped DEK) under `clerk_org_id` — an
    /// org account when paired with [`seed_tenant_row`], the adopted
    /// personal-org shape when paired with [`seed_personal_org`], and a real
    /// org that was never tenant-onboarded on its own.
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
    async fn seed_personal_org(control: &PgPool, org_id: &str, clerk_user_id: &str) {
        sqlx::query("INSERT INTO personal_orgs (clerk_org_id, clerk_user_id) VALUES ($1, $2)")
            .bind(org_id)
            .bind(clerk_user_id)
            .execute(control)
            .await
            .unwrap();
    }
    /// Store a token for `provider` the way the callback does: `None` =
    /// the account-wide row, `Some(member)` = the org-plane per-member row.
    async fn connect(pool: &PgPool, account_id: Uuid, provider: &str, owner: Option<Uuid>, sub: &str) {
        let bundle = TokenBundle {
            access_token: format!("access-{sub}"),
            refresh_token: Some(format!("refresh-{sub}")),
            user_token: None,
            scope: "Calendars.Read offline_access".into(),
            expires_at: None,
            provider_workspace_id: format!("tenant-{sub}"),
            provider_workspace_name: None,
            provider_user_id: format!("oid-{sub}"),
        };
        let mut tx = crate::tenancy::scope::begin_account_scoped(pool, account_id).await.unwrap();
        store_token_bundle_tx(
            &mut tx,
            &crate::crypto::DekPlane::consumer(pool.clone()),
            account_id,
            provider,
            owner.map(TokenOwner::Member).unwrap_or(TokenOwner::AccountWide),
            &bundle,
        )
        .await
        .map_err(|_| "store token")
        .unwrap();
        tx.commit().await.unwrap();
    }
    /// Real `AppState` on `ctl` whose HTTP client is routed to a closed
    /// local port: a pass that reaches its Graph call fails fast without a
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
    async fn sync_state_row(pool: &PgPool, account_id: Uuid) -> Option<(bool, Option<String>)> {
        sqlx::query_as("SELECT sync_enabled, last_error FROM microsoft_calendar_sync_state WHERE account_id = $1")
            .bind(account_id)
            .fetch_optional(pool)
            .await
            .unwrap()
    }
    async fn attempts_stamped(pool: &PgPool, accounts: &[Uuid]) -> i64 {
        sqlx::query_scalar("SELECT count(*) FROM microsoft_calendar_sync_attempts WHERE account_id = ANY($1)")
            .bind(accounts)
            .fetch_one(pool)
            .await
            .unwrap()
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
    fn synced() -> Result<PassOutcome, ConnectorError> {
        Ok(PassOutcome::Synced { fetched: 0, ingested: 0 })
    }
    /// Hold the pass lock for `(account_id, owner)` on a connection of its
    /// own — what a concurrent manual sync looks like to the sweep (and vice
    /// versa). Released when the returned transaction is dropped.
    async fn hold_pass_lock(pool: &PgPool, account_id: Uuid, owner: Uuid) -> sqlx::Transaction<'static, sqlx::Postgres> {
        let mut tx = pool.begin().await.unwrap();
        sqlx::query("SELECT pg_advisory_xact_lock(hashtext($1))")
            .bind(pass_lock_key(account_id, owner))
            .execute(&mut *tx)
            .await
            .unwrap();
        tx
    }

    /// The consumer entry sweeps every personal account-wide Microsoft
    /// connection — red on the base branch, where it was a structural no-op
    /// and a personal calendar synced only on "sync now". The pass is
    /// observable up to its Graph call (refused offline): the destination
    /// ocean is resolved (auto-created on this plane), the off-switch row is
    /// written, the attempt is stamped, and the Graph failure lands in
    /// `last_error`. A Google-only personal account, a member-owned token
    /// on a plain account, and an org with an active routing row (even one
    /// holding an account-wide row) are never touched.
    #[tokio::test]
    async fn consumer_tick_sweeps_personal_account_wide_microsoft_connections_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping consumer_tick_sweeps_personal_account_wide_microsoft_connections_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        set_test_kek();
        let db = "mscal_personal_tick_ctl";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        recreate(&base, db).await.close().await.ok();
        let ctl = control_pool(&base, db).await;
        let (u1, u2, u3, u4) = (seed_user(&ctl).await, seed_user(&ctl).await, seed_user(&ctl).await, seed_user(&ctl).await);
        let personal = seed_personal_account(&ctl, u1).await;
        connect(&ctl, personal, "microsoft", None, "p1").await;
        let google_only = seed_personal_account(&ctl, u2).await;
        connect(&ctl, google_only, "google", None, "p2").await;
        let member_only = seed_personal_account(&ctl, u4).await;
        connect(&ctl, member_only, "microsoft", Some(u4), "p4").await;
        let org = seed_account_row(&ctl, u3, "org_mscal_sweep").await;
        seed_tenant_row(&ctl, org, "org_mscal_sweep").await;
        connect(&ctl, org, "microsoft", Some(u3), "o-member").await;
        connect(&ctl, org, "microsoft", None, "o-wide").await;
        for a in [personal, google_only, member_only, org] {
            assert_eq!(primary_graphs(&ctl, a).await, 0, "fixture starts with no ocean");
        }

        let state = offline_state(&ctl);
        let consumer = WorkerTenant {
            tenant: TenantId::Consumer,
            pool: ctl.clone(),
            store: Arc::new(PostgresStore::new(ctl.clone())),
        };
        let tick = tick(&consumer, &ctl, &state).await.expect("consumer tick is Ok even when a pass fails");
        assert_eq!(tick.processed, 0, "the pass reached Graph and failed there — not worked");

        assert_eq!(primary_graphs(&ctl, personal).await, 1, "the personal connection was swept: destination ocean resolved");
        let (enabled, last_error) = sync_state_row(&ctl, personal).await.expect("the off-switch row was written");
        assert!(enabled);
        assert!(last_error.is_some(), "the Graph failure is recorded on the row");
        assert_eq!(attempts_stamped(&ctl, &[personal]).await, 1);
        for (a, what) in [(google_only, "a Google-only account"), (member_only, "a member-owned token"), (org, "an org with a routing row")] {
            assert_eq!(primary_graphs(&ctl, a).await, 0, "{what} is not a personal Microsoft connection");
            assert!(sync_state_row(&ctl, a).await.is_none());
        }
        assert_eq!(attempts_stamped(&ctl, &[google_only, member_only, org]).await, 0);

        teardown(&base, db, ctl).await;
    }

    /// Discovery through the real `SECURITY DEFINER` resolver: a plain
    /// personal account and an adopted personal-org account (`clerk_org_id`
    /// with a `personal_orgs` row) are visited once each; a real org never
    /// tenant-onboarded (`clerk_org_id`, no `personal_orgs` row, no routing
    /// row), an org with a routing row, a Google-only account, a
    /// disconnected one, one whose credential is marked revoked, and one
    /// whose off switch is flipped are not. The bound is honoured; nothing
    /// to find is idle (through the resolver — a missing function would be
    /// an `Err`).
    #[tokio::test]
    async fn personal_sweep_visits_only_consumer_plane_account_wide_microsoft_connections_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping personal_sweep_visits_only_consumer_plane_account_wide_microsoft_connections_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        set_test_kek();
        let db = "mscal_personal_disc_ctl";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        recreate(&base, db).await.close().await.ok();
        let ctl = control_pool(&base, db).await;
        let users: Vec<Uuid> = {
            let mut v = Vec::new();
            for _ in 0..8 {
                v.push(seed_user(&ctl).await);
            }
            v
        };
        let personal = seed_personal_account(&ctl, users[0]).await;
        connect(&ctl, personal, "microsoft", None, "p1").await;
        let adopted = seed_account_row(&ctl, users[1], "org_personal_u1").await;
        seed_personal_org(&ctl, "org_personal_u1", "user_clerk_u1").await;
        connect(&ctl, adopted, "microsoft", None, "p2").await;
        let never_onboarded_org = seed_account_row(&ctl, users[2], "org_never_onboarded").await;
        connect(&ctl, never_onboarded_org, "microsoft", None, "p3").await;
        let org = seed_account_row(&ctl, users[3], "org_mscal_disc").await;
        seed_tenant_row(&ctl, org, "org_mscal_disc").await;
        connect(&ctl, org, "microsoft", Some(users[3]), "o-member").await;
        connect(&ctl, org, "microsoft", None, "o-wide").await;
        let google_only = seed_personal_account(&ctl, users[4]).await;
        connect(&ctl, google_only, "google", None, "p5").await;
        let disconnected = seed_personal_account(&ctl, users[5]).await;
        connect(&ctl, disconnected, "microsoft", None, "p6").await;
        sqlx::query("DELETE FROM oauth_tokens WHERE account_id = $1").bind(disconnected).execute(&ctl).await.unwrap();
        let revoked = seed_personal_account(&ctl, users[6]).await;
        connect(&ctl, revoked, "microsoft", None, "p7").await;
        sqlx::query("UPDATE oauth_tokens SET credential_invalid_at = now(), credential_invalid_reason = 'microsoft_invalid_grant' WHERE account_id = $1")
            .bind(revoked)
            .execute(&ctl)
            .await
            .unwrap();
        let disabled = seed_personal_account(&ctl, users[7]).await;
        connect(&ctl, disabled, "microsoft", None, "p8").await;
        let disabled_graph: Uuid = sqlx::query_scalar(
            "INSERT INTO graphs (account_id, name, kind, is_primary) VALUES ($1, 'Primary', 'personal', true) RETURNING id",
        )
        .bind(disabled)
        .fetch_one(&ctl)
        .await
        .unwrap();
        assert!(sync_enabled(&ctl, disabled, users[7], disabled_graph).await.unwrap());
        sqlx::query("UPDATE microsoft_calendar_sync_state SET sync_enabled = false WHERE account_id = $1")
            .bind(disabled)
            .execute(&ctl)
            .await
            .unwrap();

        let visited = Visited::default();
        let tick = sweep_personal(&ctl, PERSONAL_SWEEP_LIMIT, visited.recording(|_| synced())).await.unwrap();
        let mut got = visited.take();
        got.sort();
        let mut want = vec![personal, adopted];
        want.sort();
        assert_eq!(got, want, "exactly the consumer-plane account-wide Microsoft connections, once each");
        assert_eq!(tick.processed, 2);

        assert_eq!(personal_calendar_accounts(&ctl, 1).await.unwrap().len(), 1, "the bound is honoured");

        sqlx::query("DELETE FROM oauth_tokens WHERE account_id = ANY($1)")
            .bind(vec![personal, adopted])
            .execute(&ctl)
            .await
            .unwrap();
        let tick = sweep_personal(&ctl, PERSONAL_SWEEP_LIMIT, visited.recording(|_| synced())).await.unwrap();
        assert_eq!(tick.processed, 0);
        assert!(visited.take().is_empty());

        teardown(&base, db, ctl).await;
    }

    /// Rotation: with the sweep bound at 1 and a pass that always fails, the
    /// second tick must reach the OTHER never-synced connection (the failing
    /// one was stamped as attempted and sorts behind it) and the third comes
    /// back to the first — a connection that never succeeds cannot pin the
    /// head of the queue. A pass that PANICS is that account's failure only:
    /// the tick is `Ok`, the account behind it still runs. A connection
    /// whose credential is marked revoked leaves the queue, and the
    /// reconnect upsert (`store_token_bundle_tx`, the callback's write)
    /// clears the mark and readmits it.
    #[tokio::test]
    async fn personal_sweep_rotates_past_a_failing_connection_isolates_a_panic_and_readmits_a_reconnect_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping personal_sweep_rotates_past_a_failing_connection_isolates_a_panic_and_readmits_a_reconnect_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        set_test_kek();
        let db = "mscal_personal_rotate_ctl";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        recreate(&base, db).await.close().await.ok();
        let ctl = control_pool(&base, db).await;
        let (u1, u2) = (seed_user(&ctl).await, seed_user(&ctl).await);
        let a = seed_personal_account(&ctl, u1).await;
        let b = seed_personal_account(&ctl, u2).await;
        connect(&ctl, a, "microsoft", None, "ra").await;
        connect(&ctl, b, "microsoft", None, "rb").await;

        let visited = Visited::default();
        let always_fails = visited.recording(|_| Err(ConnectorError::Exchange("graph unreachable".into())));
        let tick = sweep_personal(&ctl, 1, &always_fails).await.unwrap();
        assert_eq!(tick.processed, 0, "a failed pass is not worked");
        let first = visited.take();
        assert_eq!(first.len(), 1, "bound of 1");
        let first = first[0];
        let other = if first == a { b } else { a };

        sweep_personal(&ctl, 1, &always_fails).await.unwrap();
        assert_eq!(visited.take(), vec![other], "the connection that just failed sorts behind the one never attempted");
        sweep_personal(&ctl, 1, &always_fails).await.unwrap();
        assert_eq!(visited.take(), vec![first], "both attempted: least-recently-attempted first");
        assert_eq!(attempts_stamped(&ctl, &[a, b]).await, 2, "every attempt is stamped, success or failure");

        let panics_on_first = visited.recording(move |id| {
            if id == first {
                panic!("simulated pass panic for {id}");
            }
            synced()
        });
        let tick = sweep_personal(&ctl, 2, panics_on_first).await.expect("a panicking pass does not fail the tick");
        let mut got = visited.take();
        got.sort();
        let mut want = vec![a, b];
        want.sort();
        assert_eq!(got, want, "the account behind the panicking one still ran");
        assert_eq!(tick.processed, 1);

        sqlx::query("UPDATE oauth_tokens SET credential_invalid_at = now(), credential_invalid_reason = 'microsoft_invalid_grant' WHERE account_id = $1")
            .bind(first)
            .execute(&ctl)
            .await
            .unwrap();
        sweep_personal(&ctl, 2, visited.recording(|_| synced())).await.unwrap();
        assert_eq!(visited.take(), vec![other], "a revoked connection is not swept");
        connect(&ctl, first, "microsoft", None, if first == a { "ra" } else { "rb" }).await;
        sweep_personal(&ctl, 2, visited.recording(|_| synced())).await.unwrap();
        let mut got = visited.take();
        got.sort();
        assert_eq!(got, want, "a reconnect puts the connection back in the queue");

        teardown(&base, db, ctl).await;
    }

    /// Mutual exclusion: while another pass holds this connection's lock (a
    /// manual sync, here a bare transaction on its own connection) the
    /// personal pass is `Busy` and touches nothing — no ocean, no off-switch
    /// row; once the lock is released the same call runs the pass (and
    /// fails at the offline Graph call, which is as far as it can get here).
    #[tokio::test]
    async fn personal_pass_is_busy_while_another_pass_holds_the_connection_lock_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping personal_pass_is_busy_while_another_pass_holds_the_connection_lock_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        set_test_kek();
        let db = "mscal_personal_lock_ctl";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        recreate(&base, db).await.close().await.ok();
        let ctl = control_pool(&base, db).await;
        let owner = seed_user(&ctl).await;
        let account = seed_personal_account(&ctl, owner).await;
        connect(&ctl, account, "microsoft", None, "lk").await;
        let state = offline_state(&ctl);

        let held = hold_pass_lock(&ctl, account, owner).await;
        let outcome = sync_personal_account(&state, &ctl, account, None).await.unwrap();
        assert_eq!(outcome, PassOutcome::Busy);
        assert_eq!(primary_graphs(&ctl, account).await, 0, "a busy pass touches nothing");
        assert!(sync_state_row(&ctl, account).await.is_none());
        held.rollback().await.unwrap();

        let err = sync_personal_account(&state, &ctl, account, None).await.expect_err("Graph is offline");
        assert!(matches!(err, ConnectorError::Exchange(_)), "{err:?}");
        assert_eq!(primary_graphs(&ctl, account).await, 1, "the pass ran once the lock was free");
        let (_, last_error) = sync_state_row(&ctl, account).await.expect("row written by the pass");
        assert!(last_error.is_some());

        teardown(&base, db, ctl).await;
    }

    // ── Tenant-plane group: off switch + cross-member isolation ─────────────

    async fn tenant_pool(base: &str, db: &str, org_id: &str) -> PgPool {
        let pool = PgPoolOptions::new()
            .connect(&with_db(base, db))
            .await
            .unwrap();
        sqlx::migrate!("./migrations_tenant")
            .run(&pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO tenant_meta (id, org_id) VALUES (true, $1)")
            .bind(org_id)
            .execute(&pool)
            .await
            .unwrap();
        pool
    }
    async fn seed_graph(pool: &PgPool, account_id: Uuid, owner_user_id: Uuid) -> Uuid {
        sqlx::query_scalar(
            "INSERT INTO graphs(account_id, name, kind, is_primary, owner_user_id) \
             VALUES ($1, 'member ocean', 'member', true, $2) RETURNING id",
        )
        .bind(account_id)
        .bind(owner_user_id)
        .fetch_one(pool)
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn sync_enabled_defaults_true_and_the_off_switch_persists_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping sync_enabled_defaults_true_and_the_off_switch_persists_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "m365_38_sync_enabled";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let conn = recreate(&base, db).await;
        conn.close().await.ok();
        let pool = tenant_pool(&base, db, "org_switch").await;

        let account_id = Uuid::new_v4();
        let owner = Uuid::new_v4();
        let graph_id = seed_graph(&pool, account_id, owner).await;

        // First sight: default-on, row created.
        assert!(sync_enabled(&pool, account_id, owner, graph_id)
            .await
            .unwrap());

        // Flip the off switch directly (the toggle route itself is out of
        // this PR's scope — see this module's doc comment).
        sqlx::query(
            "UPDATE microsoft_calendar_sync_state SET sync_enabled = false \
             WHERE account_id = $1 AND owner_user_id = $2",
        )
        .bind(account_id)
        .bind(owner)
        .execute(&pool)
        .await
        .unwrap();

        // Re-reading (the `ON CONFLICT DO UPDATE` path) must NOT resurrect
        // the default — it only refreshes `graph_id`, never `sync_enabled`.
        assert!(!sync_enabled(&pool, account_id, owner, graph_id)
            .await
            .unwrap());

        teardown(&base, db, pool).await;
    }

    #[tokio::test]
    async fn one_members_off_switch_does_not_affect_another_members_row_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping one_members_off_switch_does_not_affect_another_members_row_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "m365_38_cross_member";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let conn = recreate(&base, db).await;
        conn.close().await.ok();
        let pool = tenant_pool(&base, db, "org_cross_member").await;

        let account_id = Uuid::new_v4();
        let member_a = Uuid::new_v4();
        let member_b = Uuid::new_v4();
        let graph_a = seed_graph(&pool, account_id, member_a).await;
        let graph_b = seed_graph(&pool, account_id, member_b).await;
        assert_ne!(graph_a, graph_b, "each member gets their OWN private ocean");

        assert!(sync_enabled(&pool, account_id, member_a, graph_a)
            .await
            .unwrap());
        assert!(sync_enabled(&pool, account_id, member_b, graph_b)
            .await
            .unwrap());

        sqlx::query(
            "UPDATE microsoft_calendar_sync_state SET sync_enabled = false \
             WHERE account_id = $1 AND owner_user_id = $2",
        )
        .bind(account_id)
        .bind(member_a)
        .execute(&pool)
        .await
        .unwrap();

        // Member A's opt-out does not touch member B's row, and each row
        // still points at that member's OWN graph — the isolation
        // `sync_member`'s per-owner `resolve_primary_graph` call relies on.
        assert!(!sync_enabled(&pool, account_id, member_a, graph_a)
            .await
            .unwrap());
        assert!(sync_enabled(&pool, account_id, member_b, graph_b)
            .await
            .unwrap());

        let rows: Vec<(Uuid, Uuid)> =
            sqlx::query_as("SELECT owner_user_id, graph_id FROM microsoft_calendar_sync_state WHERE account_id = $1")
                .bind(account_id)
                .fetch_all(&pool)
                .await
                .unwrap();
        assert_eq!(rows.len(), 2);
        for (owner, graph) in rows {
            if owner == member_a {
                assert_eq!(graph, graph_a);
            } else if owner == member_b {
                assert_eq!(graph, graph_b);
            } else {
                panic!("unexpected owner row: {owner}");
            }
        }

        teardown(&base, db, pool).await;
    }

    #[tokio::test]
    async fn mark_failed_then_mark_synced_clears_the_error_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping mark_failed_then_mark_synced_clears_the_error_pg: set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "m365_38_mark_error";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let conn = recreate(&base, db).await;
        conn.close().await.ok();
        let pool = tenant_pool(&base, db, "org_mark_error").await;

        let account_id = Uuid::new_v4();
        let owner = Uuid::new_v4();
        let graph_id = seed_graph(&pool, account_id, owner).await;
        // Row must exist first (mark_failed/mark_synced are UPDATE-only).
        sync_enabled(&pool, account_id, owner, graph_id)
            .await
            .unwrap();

        mark_failed(&pool, account_id, owner, "graph 503").await;
        let (err, err_at): (Option<String>, Option<chrono::DateTime<chrono::Utc>>) =
            sqlx::query_as(
                "SELECT last_error, last_error_at FROM microsoft_calendar_sync_state \
             WHERE account_id = $1 AND owner_user_id = $2",
            )
            .bind(account_id)
            .bind(owner)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(err.as_deref(), Some("graph 503"));
        assert!(err_at.is_some());

        mark_synced(&pool, account_id, owner).await;
        let (err, synced_at): (Option<String>, Option<chrono::DateTime<chrono::Utc>>) =
            sqlx::query_as(
                "SELECT last_error, last_synced_at FROM microsoft_calendar_sync_state \
             WHERE account_id = $1 AND owner_user_id = $2",
            )
            .bind(account_id)
            .bind(owner)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert!(err.is_none());
        assert!(synced_at.is_some());

        teardown(&base, db, pool).await;
    }
}
