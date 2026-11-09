//! Google Meet sweep worker (gsuite Train G3 PR 2).
//!
//! Durable pull backstop for transcript-ready push. Each connected owner's
//! watch is created or healed before `connectors::google_meet::sync_owner_once`
//! against every connected `google_meet` member's own `google_meet_sync_state`
//! cursor, claimed atomically off `google_meet_sync_state`
//! (`resolve_google_meet_sync_due()`, migration `0204`) — the SAME
//! atomic-claim + fencing-token shape `workers::gmail_everything_mode`/
//! `workers::outlook_everything_mode` already established; ported here rather
//! than re-derived (both precedents already survived their own adversarial
//! review rounds for this exact race).
//!
//! # Two ticks, one per plane (Meet org-plane lift)
//!
//! * **`TenantId::Consumer`** ([`tick_consumer`]) — unchanged. A cross-account
//!   sweep of the personal-plane `google_meet_sync_state` rows on the shared
//!   consumer database, claimed by the `SECURITY DEFINER` bootstrap
//!   (`resolve_google_meet_sync_due()`, control `0206`). A defensive guard
//!   ([`sweep_owner`]) drops any control-plane row whose account turns out to
//!   be an ORG account, so the personal fan-out can never run for an org
//!   reader.
//! * **`TenantId::Org(org_id)`** ([`tick_org`]) — the org lift, a no-op unless
//!   the org's `google_meet` flag is on (`routes::oauth::
//!   org_google_meet_enabled_for`, default OFF). It (1) resolves the
//!   tenant's one account, (2) SEEDS the org's tenant-plane
//!   `google_meet_sync_state` (tenant twin `0077`) from the control-plane
//!   `oauth_tokens` rows of members who connected `google_meet` — discovery by
//!   token, so no connect-time cross-plane write is needed and a member who
//!   connected before the flag flipped on is picked up on the next tick —
//!   (3) CLAIMS due members atomically off the tenant pool with the same
//!   `claim_id` fencing-token shape the consumer function and
//!   `gmail_everything_mode::enabled_owners_for_account` use, and (4) sweeps
//!   each with `connectors::google_meet::MeetPlane::Org`: the cursor and the
//!   ingested-conference ledger live on the tenant database, the token is
//!   loaded from the control plane (`oauth_tokens` is control-only) with the
//!   DEK resolved from the tenant plane, and attendees fan out ONLY to members
//!   of that same org, each into their own member-private ocean. See
//!   `connectors::google_meet`'s module doc, "Two planes, never mixed", for
//!   the attendee-in-both-planes rule.
//!
//! A disconnected member's tenant row self-heals exactly like the personal
//! one ([`delete_orphaned_sync_state`], pool-generic), and
//! `connectors::google_disconnect` also deletes it on an explicit disconnect.
//!
//! # Opt-in only (founder decision 2026-10-06)
//!
//! The sweep no longer ingests every transcribed meeting: it only ingests a
//! meeting for a member who allowed it (`google_meet_captures`), and re-checks
//! approved-but-not-ingested captures every tick. See `connectors::google_meet`'s
//! "Per-meeting opt-in" section. The cursor/claim machinery in this file is
//! unchanged: a record nobody approved is ledgered and the watermark advances
//! past it, because a later approval never depends on the watermark.
//!
//! # Cadence
//!
//! [`POLL_INTERVAL`] matches every other Google pull sweep in this codebase
//! (`gmail_everything_mode`, `outlook_everything_mode`: 30 minutes) — the
//! established cadence for background connector sync here.
//!
//! # What a tick does
//!
//! For each `(account_id, owner_user_id, claim_id)` the discovery function
//! claims: load the member's `google_meet` credentials
//! (`TokenOwner::Member` — Meet has no `AccountWide` arm, same as Gmail/Chat).
//! **No credentials found is a self-heal, not a retriable failure** (fixed
//! post-review, before merge — see [`sweep_owner`]'s own doc): token storage
//! is upsert-only, so a MISSING `google_meet` `oauth_tokens` row has exactly
//! one legitimate cause — the member disconnected, possibly racing
//! `connectors::google_meet::ensure_sync_state`'s own connect-time bootstrap
//! (see that function's own doc) — and [`delete_orphaned_sync_state`] deletes
//! the row on the spot instead of recording an identical `NotConfigured`
//! failure every 30 minutes forever. Otherwise: refresh if needed, persist a
//! refreshed token BEFORE fetching (same discipline `gmail_everything_mode::
//! sync_owner` documents: a fetch failure must never discard a newer token),
//! then hand off to `connectors::google_meet::sync_owner_once` for the actual
//! list/transcript/fan-out work. This module owns claim/discovery/watermark
//! persistence only — see `connectors::google_meet`'s own doc for why the
//! fan-out logic itself lives there instead (mirrors the
//! `gmail_everything_mode`/`google_gmail` split).

use std::sync::Arc;
use std::time::Duration;

use sqlx::PgPool;
use uuid::Uuid;

use crate::connectors::google_meet::{MeetPlane, OrgMeetPlane};
use crate::connectors::{self, ConnectorError, GoogleConnector, TokenOwner};
use crate::routes::connectors::load_creds_bg;
use crate::routes::oauth::store_token_bundle;
use crate::routes::slack::{account_plane, AccountPlane};
use crate::tenancy::scope::{begin_account_scoped, begin_tenant_system_scoped};
use crate::tenancy::TenantId;
use crate::workers::tenant_harness::{Tick, WorkerTenant};
use crate::AppState;

/// See this module's own "Cadence" doc section.
pub const POLL_INTERVAL: Duration = Duration::from_secs(30 * 60);

/// CONSUMER-plane cross-account discovery: every `(account_id,
/// owner_user_id, claim_id)` the `SECURITY DEFINER` bootstrap
/// (`resolve_google_meet_sync_due()`, migration `0204`) claims. Byte-identical
/// shape to `workers::gmail_everything_mode::due_accounts`.
async fn due_owners(control_pool: &PgPool) -> Result<Vec<(Uuid, Uuid, Uuid)>, ConnectorError> {
    let rows: Vec<(Uuid, Uuid, Uuid)> =
        sqlx::query_as("SELECT account_id, owner_user_id, claim_id FROM resolve_google_meet_sync_due()")
            .fetch_all(control_pool)
            .await
            .map_err(|e| ConnectorError::Internal(format!("due_owners: {e}")))?;
    Ok(rows)
}

/// This owner's live watermark, re-read inside a properly scoped transaction
/// — the single TOCTOU-safe read after the bootstrap bypass, same posture as
/// `gmail_everything_mode::owner_state`. `None` covers a row that vanished
/// between discovery and here (disconnect raced the sweep) — never an
/// error, just nothing to do this tick.
async fn owner_watermark(pool: &PgPool, account_id: Uuid, owner_user_id: Uuid) -> Result<Option<Option<chrono::DateTime<chrono::Utc>>>, ConnectorError> {
    let mut tx = begin_tenant_system_scoped(pool, account_id)
        .await
        .map_err(|e| ConnectorError::Internal(format!("owner_watermark scoped tx: {e}")))?;
    let row: Option<(Option<chrono::DateTime<chrono::Utc>>,)> = sqlx::query_as(
        "SELECT last_synced_end_time FROM google_meet_sync_state WHERE account_id = $1 AND owner_user_id = $2",
    )
    .bind(account_id)
    .bind(owner_user_id)
    .fetch_optional(&mut *tx)
    .await
    .map_err(|e| ConnectorError::Internal(format!("owner_watermark query: {e}")))?;
    tx.rollback().await.ok();
    Ok(row.map(|(w,)| w))
}

/// Persist this tick's outcome — hold the row until the next poll, advance the watermark
/// (only when [`connectors::google_meet::SweepOutcome::new_watermark`] is
/// `Some`; a `None` means nothing safely advanced this tick, so the stored
/// watermark is left untouched, never reset), and record success/failure for
/// [`due_owners`]'s own discovery ordering. `claim_id`-fenced, byte-identical
/// mechanics to `gmail_everything_mode::record_progress`/
/// `mark_failed_no_progress`.
///
/// A success holds `claimed_until` for [`POLL_INTERVAL`] instead of clearing
/// it. The claim predicate is the only cadence that holds here: the tenant
/// harness pulls this sweep forward on every enqueue for the org (a Graph
/// webhook does that about once a second), and each machine runs its own
/// harness. A cleared claim let every such wake re-sweep the owner against
/// Google.
///
/// Returns `Result` (CodeRabbit review, fixed before merge) — an earlier
/// version silently swallowed a scoped-tx-open/execute/commit failure as if
/// the tick had succeeded, so a lost write here could leave a row claimed
/// (and therefore invisible to [`due_owners`]) until its 15-minute lease
/// expired, with nothing anywhere logging that this happened as anything
/// other than a `warn!`. Callers now see the failure and log it with the
/// path-level context this file's own convention already uses elsewhere. The
/// STALE-CLAIM case (another worker already claimed this row) stays an
/// explicit, deliberate no-op, not an error — see the inline comment.
async fn record_outcome(
    pool: &PgPool,
    account_id: Uuid,
    owner_user_id: Uuid,
    claim_id: Uuid,
    new_watermark: Option<chrono::DateTime<chrono::Utc>>,
    error: Option<&str>,
) -> Result<(), ConnectorError> {
    let mut tx = begin_tenant_system_scoped(pool, account_id)
        .await
        .map_err(|e| ConnectorError::Internal(format!("record_outcome scoped tx: {e}")))?;
    let result = match error {
        None => {
            sqlx::query(
                "UPDATE google_meet_sync_state \
                 SET last_synced_end_time = COALESCE($3, last_synced_end_time), \
                     claimed_until = now() + make_interval(secs => $5), last_synced_at = now(), last_attempted_at = now(), \
                     last_error = NULL, last_error_at = NULL, updated_at = now() \
                 WHERE account_id = $1 AND owner_user_id = $2 AND claim_id = $4",
            )
            .bind(account_id)
            .bind(owner_user_id)
            .bind(new_watermark)
            .bind(claim_id)
            .bind(POLL_INTERVAL.as_secs_f64())
            .execute(&mut *tx)
            .await
        }
        Some(message) => {
            sqlx::query(
                "UPDATE google_meet_sync_state \
                 SET last_synced_end_time = COALESCE($3, last_synced_end_time), \
                     claimed_until = now() + interval '30 minutes', last_attempted_at = now(), \
                     last_error = $4, last_error_at = now(), updated_at = now() \
                 WHERE account_id = $1 AND owner_user_id = $2 AND claim_id = $5",
            )
            .bind(account_id)
            .bind(owner_user_id)
            .bind(new_watermark)
            .bind(message.chars().take(500).collect::<String>())
            .bind(claim_id)
            .execute(&mut *tx)
            .await
        }
    }
    .map_err(|e| ConnectorError::Internal(format!("record_outcome write: {e}")))?;
    if result.rows_affected() == 0 {
        // Explicit no-op, not an error: another worker already claimed this
        // row (fencing token mismatch) since our own discovery read it —
        // its OWN eventual record_outcome call is authoritative for this
        // row, ours has nothing left to do.
        tracing::warn!(
            %account_id, %owner_user_id, %claim_id,
            "google_meet_sweep: record_outcome found a stale claim_id — another worker owns \
             this row now, dropping write"
        );
    }
    tx.commit()
        .await
        .map_err(|e| ConnectorError::Internal(format!("record_outcome commit: {e}")))?;
    Ok(())
}

/// Self-heal an orphaned `google_meet_sync_state` row — one with no
/// corresponding `google_meet` `oauth_tokens` row anymore (the member
/// disconnected, or the connect/disconnect race
/// `connectors::google_meet::ensure_sync_state`'s own doc describes left an
/// orphan behind). Token storage is upsert-only (never delete-then-insert —
/// `routes::oauth::store_token_bundle`'s own `ON CONFLICT DO UPDATE`), so
/// "no `oauth_tokens` row" has exactly one legitimate cause: disconnect. This
/// is called from [`sweep_owner`] the moment credentials come back `None`,
/// rather than recording a `NotConfigured` failure that would otherwise
/// repeat identically every tick forever with no automatic recovery.
async fn delete_orphaned_sync_state(pool: &PgPool, account_id: Uuid, owner_user_id: Uuid) {
    let Ok(mut tx) = begin_tenant_system_scoped(pool, account_id).await else {
        tracing::warn!(%account_id, %owner_user_id, "google_meet_sweep: could not open a scoped tx to self-heal an orphaned sync-state row");
        return;
    };
    let result = sqlx::query("DELETE FROM google_meet_sync_state WHERE account_id = $1 AND owner_user_id = $2")
        .bind(account_id)
        .bind(owner_user_id)
        .execute(&mut *tx)
        .await;
    match result {
        Ok(r) if r.rows_affected() > 0 => {
            tracing::info!(
                %account_id, %owner_user_id,
                "google_meet_sweep: self-healed an orphaned google_meet_sync_state row (no \
                 matching oauth_tokens row — the member disconnected)"
            );
        }
        Ok(_) => {}
        Err(e) => {
            tracing::warn!(%account_id, %owner_user_id, error = %e, "google_meet_sweep: failed to self-heal an orphaned sync-state row");
        }
    }
    let _ = tx.commit().await;
}

/// What [`sweep_owner`] accomplished this pass.
enum SweepOwnerOutcome {
    /// A normal pass — `sync_owner_once` ran (successfully or with nothing
    /// due); the caller still owns the claim and must call
    /// [`record_outcome`].
    Processed {
        count: usize,
        watermark: Option<chrono::DateTime<chrono::Utc>>,
    },
    /// This owner has no `google_meet` credentials anymore — self-healed by
    /// deleting the row (see [`delete_orphaned_sync_state`]). The row is
    /// GONE, so there is nothing left for [`record_outcome`] to update; the
    /// caller must not call it.
    Cleaned,
}

/// One owner's sweep pass: load credentials, refresh, delegate to
/// `connectors::google_meet::sync_owner_once`, and report the outcome for
/// [`record_outcome`]. `Err` is reserved for "something before/during the
/// sweep itself failed" (token refresh, Meet API errors bubbling out of
/// `sync_owner_once`) — a MISSING credential is not an error here at all,
/// see [`SweepOwnerOutcome::Cleaned`].
async fn sweep_owner(
    plane: &MeetPlane,
    control_pool: &PgPool,
    state: &Arc<AppState>,
    account_id: Uuid,
    owner_user_id: Uuid,
) -> Result<SweepOwnerOutcome, ConnectorError> {
    // Where this plane's cursor row lives: the control pool for Personal, the
    // org's tenant pool for Org. `oauth_tokens` and the DEK-plane resolution
    // below always go through `control_pool` (control-only table).
    let state_pool = plane_state_pool(plane, control_pool);
    let org_id = match plane {
        MeetPlane::Personal => {
            // Defensive plane guard (Meet org-plane lift): a CONTROL-plane
            // cursor row must belong to a personal account. `ensure_sync_state`
            // never writes one for an org account, so this should never fire;
            // if it ever does (a stale or hand-made row), running the
            // PERSONAL fan-out for an org reader would read org-visible
            // meetings and write them onto the wrong plane. Drop the row —
            // `tick_org` re-seeds the member on the org's own tenant pool.
            match account_plane(control_pool, account_id).await {
                Ok(AccountPlane::Personal) => {}
                Ok(AccountPlane::Org) => {
                    tracing::warn!(
                        %account_id, %owner_user_id,
                        "google_meet_sweep: dropping a control-plane sync-state row that belongs to \
                         an ORG account — the org plane owns its own cursor on the tenant database"
                    );
                    delete_orphaned_sync_state(control_pool, account_id, owner_user_id).await;
                    return Ok(SweepOwnerOutcome::Cleaned);
                }
                Err(e) => {
                    return Err(ConnectorError::Internal(format!("sweep_owner: account_plane lookup failed: {e}")));
                }
            }
            None
        }
        MeetPlane::Org(org) => Some(org.org_id.as_str()),
    };

    if !connectors::google_chat::is_current_account_member(control_pool, account_id, owner_user_id).await? {
        delete_orphaned_sync_state(state_pool, account_id, owner_user_id).await;
        return Ok(SweepOwnerOutcome::Cleaned);
    }
    let Some(stored_watermark) = owner_watermark(state_pool, account_id, owner_user_id).await? else {
        return Ok(SweepOwnerOutcome::Processed { count: 0, watermark: None });
    };
    let since = stored_watermark.unwrap_or_else(|| chrono::Utc::now() - connectors::google_meet::default_lookback());

    let token_owner = TokenOwner::Member(owner_user_id);
    let dek_plane = crate::crypto::DekPlane::resolve_for_org(state, control_pool, account_id, org_id)
        .await
        .map_err(|e| ConnectorError::Internal(format!("resolve DEK plane: {e}")))?;
    let Some(mut creds) = load_creds_bg(control_pool, &dek_plane, account_id, "google_meet", token_owner).await? else {
        // Self-heal (CodeRabbit review, fixed before merge): treating this
        // as a retriable `NotConfigured` failure meant a legitimately
        // disconnected member's orphaned row (see `connectors::
        // google_meet::ensure_sync_state`'s own doc for how one can arise)
        // repeated the identical failure every 30 minutes forever, with no
        // path back to healthy short of manual DB surgery.
        delete_orphaned_sync_state(state_pool, account_id, owner_user_id).await;
        return Ok(SweepOwnerOutcome::Cleaned);
    };

    let connector = GoogleConnector;
    connector.refresh_if_needed(&state.http_client, &mut creds).await?;
    if let Some(refreshed) = creds.refreshed.as_ref() {
        if let Err(e) = store_token_bundle(control_pool, &dek_plane, account_id, "google_meet", token_owner, refreshed).await {
            tracing::warn!(%account_id, %owner_user_id, status = ?e.status(), "google_meet_sweep: failed to persist refreshed token");
        }
    }

    let mut token_tx = begin_tenant_system_scoped(control_pool, account_id).await.map_err(|e| ConnectorError::Internal(e.to_string()))?;
    let sub: Option<String> = sqlx::query_scalar("SELECT provider_user_id FROM oauth_tokens WHERE account_id=$1 AND provider='google_meet' AND owner_user_id=$2 AND provider_user_id<>'' LIMIT 1")
        .bind(account_id).bind(owner_user_id).fetch_optional(&mut *token_tx).await.map_err(|e| ConnectorError::Internal(e.to_string()))?;
    token_tx.rollback().await.ok();
    if let Some(sub) = sub {
        if let Err(e) = connectors::google_meet_events::ensure_subscription(&state.http_client, control_pool, account_id, owner_user_id, &sub, &creds.access_token).await {
            tracing::warn!(error = %e, %account_id, %owner_user_id, "Meet subscription self-heal failed; continuing durable pull fallback");
        }
    }
    let key = format!("google_meet:{account_id}");
    let outcome = crate::graph_rate_limit::scoped(
        key,
        connectors::google_meet::sync_owner_once(plane, &state.http_client, control_pool, state, account_id, owner_user_id, &creds.access_token, since),
    )
    .await?;
    Ok(SweepOwnerOutcome::Processed {
        count: outcome.conferences_processed,
        watermark: outcome.new_watermark,
    })
}

/// The pool a plane's `google_meet_sync_state` rows live on.
fn plane_state_pool<'a>(plane: &'a MeetPlane, control_pool: &'a PgPool) -> &'a PgPool {
    match plane {
        MeetPlane::Personal => control_pool,
        MeetPlane::Org(org) => &org.pool,
    }
}

/// One claimed owner, start to finish: sweep, then release/back off the claim
/// via [`record_outcome`]. Shared by both ticks so the claim-fencing and
/// outcome-recording shape cannot drift between planes. Returns how many
/// conference records were processed.
async fn run_claimed_owner(
    plane: &MeetPlane,
    control_pool: &PgPool,
    state: &Arc<AppState>,
    account_id: Uuid,
    owner_user_id: Uuid,
    claim_id: Uuid,
) -> usize {
    let state_pool = plane_state_pool(plane, control_pool);
    match sweep_owner(plane, control_pool, state, account_id, owner_user_id).await {
        Ok(SweepOwnerOutcome::Processed { count, watermark }) => {
            if let Err(e) = record_outcome(state_pool, account_id, owner_user_id, claim_id, watermark, None).await {
                tracing::warn!(%account_id, %owner_user_id, error = %e, "google_meet_sweep: failed to record a successful tick's outcome");
            }
            count
        }
        Ok(SweepOwnerOutcome::Cleaned) => {
            // The row is gone (self-healed) — nothing left to release.
            0
        }
        Err(e) => {
            tracing::warn!(%account_id, %owner_user_id, error = %e, "google_meet_sweep: owner sweep failed");
            if let Err(e2) = record_outcome(state_pool, account_id, owner_user_id, claim_id, None, Some(&e.to_string())).await {
                tracing::warn!(%account_id, %owner_user_id, error = %e2, "google_meet_sweep: failed to record a failed tick's outcome");
            }
            0
        }
    }
}

// ── Ticks ────────────────────────────────────────────────────────────────────

async fn tick_consumer(control_pool: &PgPool, state: &Arc<AppState>) -> Result<Tick, String> {
    let due = due_owners(control_pool).await.map_err(|e| e.to_string())?;
    if due.is_empty() {
        return Ok(Tick::idle());
    }
    let mut processed = 0usize;
    for (account_id, owner_user_id, claim_id) in due {
        processed += run_claimed_owner(&MeetPlane::Personal, control_pool, state, account_id, owner_user_id, claim_id).await;
    }
    Ok(if processed > 0 { Tick::worked(processed) } else { Tick::idle() })
}

// ── Org plane ────────────────────────────────────────────────────────────────

/// Per-tick owner budget for ONE org — same bounded-worst-case reasoning as
/// `workers::gmail_everything_mode::OWNERS_PER_TICK`, a little higher (10)
/// because a Meet pass is cheap when nothing new completed and an org's
/// members are naturally scoped to one tenant. `ORDER BY last_attempted_at
/// ASC NULLS FIRST` makes it self-rotating with no cursor state.
const ORG_OWNERS_PER_TICK: i64 = 10;

/// Identical resolution to `workers::gmail_everything_mode::
/// resolve_tenant_account` (kept as its own copy per those modules' own doc
/// comments: duplicating one two-line query is cheaper than a shared
/// dependency between independent connector workers).
async fn resolve_tenant_account(control_pool: &PgPool, org_id: &str) -> Result<Uuid, String> {
    sqlx::query_scalar::<_, Uuid>("SELECT account_id FROM tenant_databases WHERE clerk_org_id = $1")
        .bind(org_id)
        .fetch_optional(control_pool)
        .await
        .map_err(|e| e.to_string())?
        .ok_or_else(|| format!("no tenant_databases row for org {org_id}"))
}

/// Every org member with a stored `google_meet` connection, read from the
/// CONTROL plane (`oauth_tokens` is control-only — it has no tenant twin that
/// is ever written). Identification only: no token column is selected.
async fn connected_member_ids(control_pool: &PgPool, account_id: Uuid) -> Result<Vec<Uuid>, ConnectorError> {
    let mut tx = begin_account_scoped(control_pool, account_id)
        .await
        .map_err(|e| ConnectorError::Internal(format!("connected_member_ids scoped tx: {e}")))?;
    let rows: Vec<(Uuid,)> = sqlx::query_as(
        "SELECT DISTINCT t.owner_user_id FROM oauth_tokens t \
         WHERE t.account_id = $1 AND t.provider = 'google_meet' AND t.owner_user_id IS NOT NULL \
           AND EXISTS(SELECT 1 FROM account_members m WHERE m.account_id=t.account_id AND m.user_id=t.owner_user_id)",
    )
    .bind(account_id)
    .fetch_all(&mut *tx)
    .await
    .map_err(|e| ConnectorError::Internal(format!("connected_member_ids query: {e}")))?;
    tx.rollback().await.ok();
    Ok(rows.into_iter().map(|(u,)| u).collect())
}

/// Org discovery: make sure every connected member has a cursor row on the
/// org's tenant database. Idempotent (`DO NOTHING` — never disturbs a live
/// cursor, claim, or error state). Returns how many members were connected.
async fn seed_org_sync_state(
    control_pool: &PgPool,
    tenant_pool: &PgPool,
    account_id: Uuid,
) -> Result<usize, ConnectorError> {
    let owners = connected_member_ids(control_pool, account_id).await?;
    if owners.is_empty() {
        return Ok(0);
    }
    let mut tx = begin_tenant_system_scoped(tenant_pool, account_id)
        .await
        .map_err(|e| ConnectorError::Internal(format!("seed_org_sync_state scoped tx: {e}")))?;
    sqlx::query(
        "INSERT INTO google_meet_sync_state (account_id, owner_user_id) \
         SELECT $1, o FROM UNNEST($2::uuid[]) AS o \
         ON CONFLICT (account_id, owner_user_id) DO NOTHING",
    )
    .bind(account_id)
    .bind(&owners)
    .execute(&mut *tx)
    .await
    .map_err(|e| ConnectorError::Internal(format!("seed_org_sync_state insert: {e}")))?;
    tx.commit()
        .await
        .map_err(|e| ConnectorError::Internal(format!("seed_org_sync_state commit: {e}")))?;
    Ok(owners.len())
}

/// Atomically claim up to [`ORG_OWNERS_PER_TICK`] due members off the org's
/// TENANT pool — a single-account, already-scoped query (ADR-0007), same
/// claim + `claim_id` fencing-token mechanics as `resolve_google_meet_sync_due()`
/// and `gmail_everything_mode::enabled_owners_for_account`. Callers thread
/// the returned `claim_id` through to [`record_outcome`], which fences on it.
async fn claim_org_owners(tenant_pool: &PgPool, account_id: Uuid) -> Result<Vec<(Uuid, Uuid)>, ConnectorError> {
    let mut tx = begin_tenant_system_scoped(tenant_pool, account_id)
        .await
        .map_err(|e| ConnectorError::Internal(format!("claim_org_owners scoped tx: {e}")))?;
    let rows: Vec<(Uuid, Uuid)> = sqlx::query_as(
        "UPDATE google_meet_sync_state gms \
         SET claimed_until = now() + interval '15 minutes', claim_id = gen_random_uuid() \
         FROM ( \
             SELECT id FROM google_meet_sync_state \
             WHERE account_id = $1 \
               AND (claimed_until IS NULL OR claimed_until < now()) \
             ORDER BY last_attempted_at ASC NULLS FIRST \
             LIMIT $2 \
             FOR UPDATE SKIP LOCKED \
         ) claimable \
         WHERE gms.id = claimable.id \
         RETURNING gms.owner_user_id, gms.claim_id",
    )
    .bind(account_id)
    .bind(ORG_OWNERS_PER_TICK)
    .fetch_all(&mut *tx)
    .await
    .map_err(|e| ConnectorError::Internal(format!("claim_org_owners claim: {e}")))?;
    tx.commit()
        .await
        .map_err(|e| ConnectorError::Internal(format!("claim_org_owners commit: {e}")))?;
    Ok(rows)
}

/// See this module's own doc, "Two ticks, one per plane". A no-op (before
/// touching ANY database) while the org's `google_meet` flag is off.
async fn tick_org(
    t: &WorkerTenant,
    control_pool: &PgPool,
    state: &Arc<AppState>,
    org_id: &str,
) -> Result<Tick, String> {
    if !crate::routes::oauth::org_google_meet_enabled_for(state, org_id).await {
        return Ok(Tick::idle());
    }
    let account_id = resolve_tenant_account(control_pool, org_id).await?;
    seed_org_sync_state(control_pool, &t.pool, account_id)
        .await
        .map_err(|e| e.to_string())?;
    let owners = claim_org_owners(&t.pool, account_id).await.map_err(|e| e.to_string())?;
    if owners.is_empty() {
        return Ok(Tick::idle());
    }
    let plane = MeetPlane::Org(OrgMeetPlane {
        org_id: org_id.to_string(),
        account_id,
        pool: t.pool.clone(),
    });
    let mut processed = 0usize;
    for (owner_user_id, claim_id) in owners {
        processed += run_claimed_owner(&plane, control_pool, state, account_id, owner_user_id, claim_id).await;
    }
    Ok(if processed > 0 { Tick::worked(processed) } else { Tick::idle() })
}

/// One Meet sweep pass. Driven by `workers::tenant_harness::run_multi_tenant`
/// as a sweep worker (`HarnessConfig::sweep_worker(_, POLL_INTERVAL)`).
pub async fn tick(t: &WorkerTenant, control_pool: &PgPool, state: &Arc<AppState>) -> Result<Tick, String> {
    match &t.tenant {
        TenantId::Consumer => tick_consumer(control_pool, state).await,
        TenantId::Org(org_id) => tick_org(t, control_pool, state, org_id).await,
    }
}

// ---------------------------------------------------------------------------
// pg tests — Meet org-plane lift. Two real databases per test (control +
// org tenant), the production plane split for an ordinary org. In-module
// (`discover`/`claim`/`sweep_owner` are private) and named `*_pg_tests` so
// `scripts/check_pg_suite_coverage.sh` requires, and `ci.yml` wires, it.
//
//   ORGT_TEST_BASE_URL=postgres://alex@localhost:5432/postgres \
//     cargo test -p coralswarm-backend --lib \
//     workers::google_meet_sweep::google_meet_org_pg_tests -- --test-threads=1
//
// No test here makes an outbound Google call: the ones that reach
// `sweep_owner` stop at its plane guard, and discovery/claim are tested
// directly rather than through a full tick.
// ---------------------------------------------------------------------------

#[cfg(test)]
mod google_meet_org_pg_tests {
    use super::*;
    use chrono::Utc;
    use coralswarm_core::kek::{generate_dek, load_kek, wrap_dek};
    use sqlx::postgres::PgPoolOptions;
    use sqlx::{Connection, Executor, PgConnection};

    use crate::connectors::TokenBundle;
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
    /// `clerk_org_id = Some(..)` makes it an org account.
    async fn seed_account(pool: &PgPool, owner: Uuid, clerk_org_id: Option<&str>) -> Uuid {
        let kek = load_kek().expect("test KEK loads");
        let wrapped = wrap_dek(&kek, &generate_dek()).expect("wrap test dek");
        let id: Uuid = sqlx::query_scalar(
            "INSERT INTO accounts(owner_user_id, dek_wrapped, clerk_org_id) VALUES ($1, $2, $3) RETURNING id",
        )
        .bind(owner)
        .bind(&wrapped)
        .bind(clerk_org_id)
        .fetch_one(pool)
        .await
        .unwrap();
        // Personal accounts get their owner membership row; org accounts get
        // none (the seat-tier trigger would force fixture noise, and nothing
        // in the Meet path reads membership).
        if clerk_org_id.is_none() {
            sqlx::query("INSERT INTO account_members(account_id, user_id, role) VALUES ($1, $2, 'owner')")
                .bind(id)
                .bind(owner)
                .execute(pool)
                .await
                .unwrap();
        }
        id
    }
    async fn connect(pool: &PgPool, account_id: Uuid, owner: Uuid, provider: &str, sub: &str) {
        sqlx::query("INSERT INTO account_members(account_id,user_id,role,seat_tier_id) SELECT $1,$2,'member',CASE WHEN clerk_org_id IS NOT NULL THEN (SELECT id FROM seat_tiers WHERE code='standard' AND is_active=true) END FROM accounts WHERE id=$1 ON CONFLICT DO NOTHING").bind(account_id).bind(owner).execute(pool).await.unwrap();
        let bundle = TokenBundle {
            access_token: format!("access-{sub}"),
            refresh_token: Some(format!("refresh-{sub}")),
            user_token: None,
            scope: "openid email".into(),
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
            TokenOwner::Member(owner),
            &bundle,
        )
        .await
        .expect("store_token_bundle_tx");
        tx.commit().await.expect("commit");
    }
    async fn tenant_rows(pool: &PgPool, account_id: Uuid) -> Vec<Uuid> {
        sqlx::query_scalar("SELECT owner_user_id FROM google_meet_sync_state WHERE account_id = $1 ORDER BY owner_user_id")
            .bind(account_id)
            .fetch_all(pool)
            .await
            .unwrap()
    }
    fn worker_tenant(pool: &PgPool, org_id: &str) -> WorkerTenant {
        WorkerTenant {
            tenant: TenantId::Org(org_id.to_string()),
            pool: pool.clone(),
            store: Arc::new(crate::store::PostgresStore::new(pool.clone())),
        }
    }

    /// Discovery: connected `google_meet` members of THIS org (and only
    /// them) get a tenant-plane cursor row; a member who connected only
    /// Gmail, a member of another org, and a personal member get none. A
    /// second seed is idempotent and never disturbs a live claim/cursor.
    #[tokio::test]
    async fn org_discovery_seeds_only_this_orgs_google_meet_members_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping org_discovery_seeds_only_this_orgs_google_meet_members_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        set_test_kek();
        let ctl = control(&base, "meet_sweep_disc_ctl").await;
        let ten = tenant(&base, "meet_sweep_disc_ten", "org_disc").await;

        let owner = seed_user(&ctl).await;
        let org = seed_account(&ctl, owner, Some("org_disc")).await;
        let a = seed_user(&ctl).await;
        let b = seed_user(&ctl).await;
        let gmail_only = seed_user(&ctl).await;
        connect(&ctl, org, a, "google_meet", "sub-a").await;
        connect(&ctl, org, b, "google_meet", "sub-b").await;
        connect(&ctl, org, gmail_only, "google_gmail", "sub-g").await;

        let other_owner = seed_user(&ctl).await;
        let other_org = seed_account(&ctl, other_owner, Some("org_other")).await;
        let other_member = seed_user(&ctl).await;
        connect(&ctl, other_org, other_member, "google_meet", "sub-other").await;
        let personal = seed_user(&ctl).await;
        let personal_account = seed_account(&ctl, personal, None).await;
        connect(&ctl, personal_account, personal, "google_meet", "sub-personal").await;

        assert_eq!(seed_org_sync_state(&ctl, &ten, org).await.unwrap(), 2);
        let mut want = vec![a, b];
        want.sort();
        assert_eq!(tenant_rows(&ten, org).await, want);
        assert!(
            tenant_rows(&ten, other_org).await.is_empty() && tenant_rows(&ten, personal_account).await.is_empty(),
            "another org's and a personal account's members must never be seeded into this org's tenant DB"
        );

        // Claim both; a re-seed must not reset the live claim.
        let claimed = claim_org_owners(&ten, org).await.unwrap();
        assert_eq!(claimed.len(), 2);
        assert!(claimed.iter().all(|(_, claim)| *claim != Uuid::nil()));
        seed_org_sync_state(&ctl, &ten, org).await.unwrap();
        assert!(
            claim_org_owners(&ten, org).await.unwrap().is_empty(),
            "already-claimed rows are not re-claimed (and re-seeding did not clear the claim)"
        );

        // Control plane: the org tick wrote nothing to the control cursor table.
        let control_rows: i64 = sqlx::query_scalar("SELECT count(*) FROM google_meet_sync_state")
            .fetch_one(&ctl)
            .await
            .unwrap();
        assert_eq!(control_rows, 0);

        ctl.close().await;
        ten.close().await;
        drop_db(&base, "meet_sweep_disc_ten").await;
        drop_db(&base, "meet_sweep_disc_ctl").await;
    }

    /// Claim fencing on the tenant plane: `record_outcome` with the live
    /// `claim_id` advances the watermark and releases the claim; a stale
    /// `claim_id` is dropped; failure backs the claim off.
    #[tokio::test]
    async fn org_claim_fencing_and_outcome_recording_on_the_tenant_plane_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping org_claim_fencing_and_outcome_recording_on_the_tenant_plane_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        set_test_kek();
        let ctl = control(&base, "meet_sweep_fence_ctl").await;
        let ten = tenant(&base, "meet_sweep_fence_ten", "org_fence").await;
        let owner = seed_user(&ctl).await;
        let org = seed_account(&ctl, owner, Some("org_fence")).await;
        let m = seed_user(&ctl).await;
        connect(&ctl, org, m, "google_meet", "sub-m").await;
        seed_org_sync_state(&ctl, &ten, org).await.unwrap();
        let (_, claim) = claim_org_owners(&ten, org).await.unwrap().remove(0);

        // Stale claim: dropped, watermark untouched.
        let wm = Utc::now() - chrono::Duration::hours(2);
        record_outcome(&ten, org, m, Uuid::new_v4(), Some(wm), None).await.unwrap();
        assert_eq!(owner_watermark(&ten, org, m).await.unwrap(), Some(None));

        // Live claim: watermark advances, and the row is held until the next
        // poll rather than released for an immediate re-claim.
        record_outcome(&ten, org, m, claim, Some(wm), None).await.unwrap();
        let got = owner_watermark(&ten, org, m).await.unwrap().flatten().expect("watermark set");
        assert_eq!(got.timestamp(), wm.timestamp());
        let held: Option<chrono::DateTime<Utc>> =
            sqlx::query_scalar("SELECT claimed_until FROM google_meet_sync_state WHERE account_id = $1 AND owner_user_id = $2")
                .bind(org)
                .bind(m)
                .fetch_one(&ten)
                .await
                .unwrap();
        let poll = chrono::Duration::from_std(POLL_INTERVAL).unwrap();
        assert!(
            held.is_some_and(|u| u > Utc::now() + poll - chrono::Duration::minutes(1)),
            "a successful pass is not due again until POLL_INTERVAL has passed"
        );
        assert!(
            claim_org_owners(&ten, org).await.unwrap().is_empty(),
            "an enqueue-woken tick right after a successful pass must not re-claim the owner"
        );

        // Failure: claim backed off, error recorded, watermark kept.
        sqlx::query("UPDATE google_meet_sync_state SET claimed_until = now() - interval '1 second' WHERE account_id = $1")
            .bind(org)
            .execute(&ten)
            .await
            .unwrap();
        let (_, claim2) = claim_org_owners(&ten, org).await.unwrap().remove(0);
        record_outcome(&ten, org, m, claim2, None, Some("boom")).await.unwrap();
        let (err, until): (Option<String>, Option<chrono::DateTime<Utc>>) = sqlx::query_as(
            "SELECT last_error, claimed_until FROM google_meet_sync_state WHERE account_id = $1 AND owner_user_id = $2",
        )
        .bind(org)
        .bind(m)
        .fetch_one(&ten)
        .await
        .unwrap();
        assert_eq!(err.as_deref(), Some("boom"));
        assert!(until.is_some_and(|u| u > Utc::now()), "a failed pass backs the claim off");

        ctl.close().await;
        ten.close().await;
        drop_db(&base, "meet_sweep_fence_ten").await;
        drop_db(&base, "meet_sweep_fence_ctl").await;
    }

    #[tokio::test]
    async fn consumer_successful_pass_is_not_reclaimed_before_the_next_poll_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping consumer_successful_pass_is_not_reclaimed_before_the_next_poll_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        set_test_kek();
        let ctl = control(&base, "meet_sweep_cadence_ctl").await;
        let owner = seed_user(&ctl).await;
        let account = seed_account(&ctl, owner, None).await;
        connect(&ctl, account, owner, "google_meet", "sub-cadence").await;
        connectors::google_meet::ensure_sync_state(&ctl, account, owner).await.unwrap();

        let due = due_owners(&ctl).await.unwrap();
        assert_eq!(due.len(), 1);
        let (_, _, claim) = due[0];
        record_outcome(&ctl, account, owner, claim, None, None).await.unwrap();
        assert!(
            due_owners(&ctl).await.unwrap().is_empty(),
            "a successful pass must not make the owner due again before POLL_INTERVAL"
        );

        ctl.close().await;
        drop_db(&base, "meet_sweep_cadence_ctl").await;
    }

    /// Flag off => `tick_org` is a no-op that touches NOTHING (no tenant row
    /// seeded even though a member is connected, and it never even resolves
    /// the tenant account). Flag on => it gets past the gate (observable:
    /// with no `tenant_databases` routing row in this fixture it then fails
    /// at account resolution, which the gated-off path never reaches).
    #[tokio::test]
    async fn tick_org_is_a_noop_while_the_org_flag_is_off_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping tick_org_is_a_noop_while_the_org_flag_is_off_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        set_test_kek();
        let ctl = control(&base, "meet_sweep_flag_ctl").await;
        let ten = tenant(&base, "meet_sweep_flag_ten", "org_flag").await;
        let owner = seed_user(&ctl).await;
        let org = seed_account(&ctl, owner, Some("org_flag")).await;
        let m = seed_user(&ctl).await;
        connect(&ctl, org, m, "google_meet", "sub-m").await;

        let state = Arc::new(crate::test_app_state_owned_with_db_and_store(ctl.clone(), None));
        let wt = worker_tenant(&ten, "org_flag");

        // Flag off (no registry row; ORG_GOOGLE_MEET_ENABLED not set by this test).
        if std::env::var("ORG_GOOGLE_MEET_ENABLED").is_ok() {
            eprintln!("skipping: ORG_GOOGLE_MEET_ENABLED is set in this environment");
        } else {
            let t = tick_org(&wt, &ctl, &state, "org_flag").await.expect("flag off is Ok(idle)");
            assert_eq!(t.processed, 0);
            assert!(tenant_rows(&ten, org).await.is_empty(), "flag off must seed nothing");

            // Explicit override row = off also no-ops.
            crate::connector_flags::set_override(&ctl, &state.connector_flag_cache, "org_flag", "google_meet", false)
                .await
                .unwrap();
            assert_eq!(tick_org(&wt, &ctl, &state, "org_flag").await.unwrap().processed, 0);
            assert!(tenant_rows(&ten, org).await.is_empty());

            // Flip on: now past the gate.
            crate::connector_flags::set_override(&ctl, &state.connector_flag_cache, "org_flag", "google_meet", true)
                .await
                .unwrap();
            let err = tick_org(&wt, &ctl, &state, "org_flag").await.expect_err("flag on proceeds to account resolution");
            assert!(err.contains("no tenant_databases row"), "got: {err}");
        }

        ctl.close().await;
        ten.close().await;
        drop_db(&base, "meet_sweep_flag_ten").await;
        drop_db(&base, "meet_sweep_flag_ctl").await;
    }

    /// The consumer tick never runs the personal fan-out for an org reader:
    /// a control-plane cursor row belonging to an org account is dropped by
    /// `sweep_owner`'s plane guard before any credential/HTTP work. A
    /// personal account's row is not touched by the guard.
    #[tokio::test]
    async fn consumer_sweep_drops_a_control_row_that_belongs_to_an_org_account_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping consumer_sweep_drops_a_control_row_that_belongs_to_an_org_account_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        set_test_kek();
        let ctl = control(&base, "meet_sweep_guard_ctl").await;
        let owner = seed_user(&ctl).await;
        let org = seed_account(&ctl, owner, Some("org_guard")).await;
        let m = seed_user(&ctl).await;
        sqlx::query("INSERT INTO google_meet_sync_state (account_id, owner_user_id) VALUES ($1, $2)")
            .bind(org)
            .bind(m)
            .execute(&ctl)
            .await
            .unwrap();

        let state = Arc::new(crate::test_app_state_owned_with_db_and_store(ctl.clone(), None));
        let out = sweep_owner(&MeetPlane::Personal, &ctl, &state, org, m).await.unwrap();
        assert!(matches!(out, SweepOwnerOutcome::Cleaned));
        let left: i64 = sqlx::query_scalar("SELECT count(*) FROM google_meet_sync_state WHERE account_id = $1")
            .bind(org)
            .fetch_one(&ctl)
            .await
            .unwrap();
        assert_eq!(left, 0, "the stray org-account row on the control plane must be dropped");

        ctl.close().await;
        drop_db(&base, "meet_sweep_guard_ctl").await;
    }

    const TENANT_APP_ROLE_TEST_PASSWORD: &str = "meet_org_tenant_rls_test_only_pw";

    /// Discovery + claim + outcome run as the REAL NOBYPASSRLS tenant role
    /// (`coralswarm_tenant_app`), proving the tenant twin's grants cover what
    /// the worker writes (the owner role bypasses RLS and grants, so the
    /// other tests cannot).
    #[tokio::test]
    async fn org_worker_writes_succeed_as_the_real_tenant_app_role_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping org_worker_writes_succeed_as_the_real_tenant_app_role_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        set_test_kek();
        let ctl = control(&base, "meet_sweep_role_ctl").await;
        let ten_owner = tenant(&base, "meet_sweep_role_ten", "org_role").await;
        let stmt = format!("ALTER ROLE coralswarm_tenant_app WITH PASSWORD '{TENANT_APP_ROLE_TEST_PASSWORD}'");
        let mut last: Option<sqlx::Error> = None;
        let mut ok = false;
        for attempt in 0..10u32 {
            match sqlx::query(&stmt).execute(&ten_owner).await {
                Ok(_) => {
                    ok = true;
                    break;
                }
                Err(e) => {
                    last = Some(e);
                    tokio::time::sleep(std::time::Duration::from_millis(50 * u64::from(attempt + 1))).await;
                }
            }
        }
        assert!(ok, "set test password on coralswarm_tenant_app: {last:?}");
        let opts = <sqlx::postgres::PgConnectOptions as std::str::FromStr>::from_str(&with_db(&base, "meet_sweep_role_ten"))
            .unwrap()
            .username("coralswarm_tenant_app")
            .password(TENANT_APP_ROLE_TEST_PASSWORD);
        let app = PgPoolOptions::new().max_connections(2).connect_with(opts).await.expect("connect AS coralswarm_tenant_app");

        let owner = seed_user(&ctl).await;
        let org = seed_account(&ctl, owner, Some("org_role")).await;
        let m = seed_user(&ctl).await;
        connect(&ctl, org, m, "google_meet", "sub-m").await;

        assert_eq!(seed_org_sync_state(&ctl, &app, org).await.unwrap(), 1);
        let (_, claim) = claim_org_owners(&app, org).await.unwrap().remove(0);
        record_outcome(&app, org, m, claim, Some(Utc::now()), None).await.unwrap();
        assert!(owner_watermark(&app, org, m).await.unwrap().flatten().is_some());
        // And the orphan self-heal delete.
        delete_orphaned_sync_state(&app, org, m).await;
        assert!(tenant_rows(&ten_owner, org).await.is_empty());

        app.close().await;
        ctl.close().await;
        ten_owner.close().await;
        drop_db(&base, "meet_sweep_role_ten").await;
        drop_db(&base, "meet_sweep_role_ctl").await;
    }
    #[tokio::test]
    async fn departed_meet_owner_is_not_seeded_and_stale_cursor_is_retired_pg() {
        let Some(base)=base_url() else {return;};set_test_kek();
        let ctl_db="meet_departed_owner_ctl";
        let _cleanup_ctl_db = crate::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", ctl_db);let ten_db="meet_departed_owner_ten";
        let ctl=control(&base,ctl_db).await;let ten=tenant(&base,ten_db,"org_meet_departed").await;
        let user=seed_user(&ctl).await;let account=seed_account(&ctl,user,Some("org_meet_departed")).await;
        connect(&ctl,account,user,"google_meet","sub-departed").await;
        assert_eq!(seed_org_sync_state(&ctl,&ten,account).await.unwrap(),1);
        sqlx::query("DELETE FROM account_members WHERE account_id=$1 AND user_id=$2").bind(account).bind(user).execute(&ctl).await.unwrap();
        assert!(connected_member_ids(&ctl,account).await.unwrap().is_empty());
        let plane=MeetPlane::Org(crate::connectors::google_meet::OrgMeetPlane {org_id:"org_meet_departed".into(),account_id:account,pool:ten.clone()});
        let state=Arc::new(crate::test_app_state_owned_with_db(ctl.clone(),None));
        assert!(matches!(sweep_owner(&plane,&ctl,&state,account,user).await.unwrap(),SweepOwnerOutcome::Cleaned));
        assert_eq!(sqlx::query_scalar::<_,i64>("SELECT count(*) FROM google_meet_sync_state").fetch_one(&ten).await.unwrap(),0);
        ctl.close().await;ten.close().await;let mut admin=PgConnection::connect(&base).await.unwrap();for db in [ctl_db,ten_db] {admin.execute(format!("DROP DATABASE {db} WITH (FORCE)").as_str()).await.unwrap();}
    }

}
