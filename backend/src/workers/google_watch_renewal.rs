//! Google Workspace watch/channel renewal worker (gsuite Train G1 PR 2,
//! `.scratch/gsuite-enterprise/plan.md` decision 7).
//!
//! Renews the three push-notification registrations Google Workspace uses
//! (see `routes::google_push`'s module doc for the Pub/Sub half of this):
//! Gmail `users.watch` (~7-day expiry), a Chat Workspace Events API
//! subscription, and a Calendar `events.watch` webhook channel. All three are
//! tracked in one persistence table, `google_watch_channels`
//! (`migrations_control/0180_google_watch_channels.sql` /
//! `migrations_tenant/0062_google_watch_channels.sql`), which — as of this PR
//! — no per-surface ingest code populates yet: Trains G2 (Gmail), G3
//! (Calendar + Meet), and G4 (Chat) each create their own watch/subscription
//! rows as they ship. This worker is written ahead of that so renewal is
//! never a day-one gap once they do: **a lapsed watch is silent data loss**
//! (the plan's own words), so it is built, tested, and wired into the
//! harness now, even though it finds nothing to do in production until a
//! later train starts inserting rows.
//!
//! # Pattern: `workers::graph_subscriptions` post-#662
//!
//! Structurally this is that worker's Google analog: two harness
//! registrations off two native cadences —
//!
//! * [`tick_renew`] (`POLL_INTERVAL`, ~30 min): renews anything expiring
//!   within `RENEW_WINDOW_HOURS`.
//! * [`tick_backstop`] (`BACKSTOP_INTERVAL`, ~6 h): the loud-alert sweep —
//!   anything that renewal has ALREADY missed (`expires_at` in the past) is
//!   logged at `error!` (never merely `warn!`; see [`alert_lapsed`]), because
//!   by the time a channel reaches this tick, notifications for it have
//!   already stopped arriving. This is the "renewal failure MUST log
//!   loudly/alert" engineering obligation made concrete — there is no paging
//!   integration in this repo today, so `tracing::error!` is the load-bearing
//!   signal a future alerting hook (or an operator grepping logs) consumes.
//!
//! Both run on the tenant loop (`workers::tenant_harness::run_multi_tenant`)
//! from day one, unlike `graph_subscriptions` (which was consumer-only until
//! m365 Train 2 PR 2.2 converted it) — there is no legacy single-loop version
//! of this worker to migrate away from, so it starts tenant-aware.
//!
//! # Renewal mechanics differ per surface
//!
//! * **Gmail** — `users.watch` is idempotently re-callable: calling it again
//!   simply extends the same watch for another ~7 days. [`renew_gmail`].
//! * **Chat** — a Workspace Events API subscription's TTL is extended via
//!   `subscriptions.patch`. [`renew_chat`].
//! * **Calendar** — `events.watch` channels cannot be renewed in place; the
//!   only way to keep receiving notifications is to `channels.stop` the
//!   expiring one and create a fresh one via a new `events.watch` call
//!   (Google's own documented pattern). [`renew_calendar`] does both, in
//!   that order, and persists the new `channel_id`/`channel_resource_id`.
//!
//! All three load their access token via [`google_token`], refreshing it
//! exactly like `workers::graph_subscriptions::delegated_microsoft_token`
//! does for Microsoft — but WHICH `oauth_tokens` row depends on the channel's
//! `surface` (gsuite Train G2 PR 3): a `"gmail"` row (per-member,
//! `owner_user_id` set — `migrations_control/
//! 0186_google_gmail_watch_owner_and_threads.sql`) loads
//! `("google_gmail", TokenOwner::Member(row.owner_user_id))`; every other
//! surface (Chat, Calendar — still account-wide) loads
//! `("google", TokenOwner::AccountWide)`. See [`renew_one`]'s own comment
//! for the exact dispatch and the AccountWide-never-for-Gmail invariant.

use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::Deserialize;
use sqlx::PgPool;
use uuid::Uuid;

use crate::connectors::{ConnectorError, GoogleConnector, TokenOwner};
use crate::routes::{connectors::load_creds_bg, oauth::store_token_bundle};
use crate::tenancy::scope::begin_tenant_system_scoped;
use crate::tenancy::TenantId;
use crate::workers::tenant_harness::{Tick, WorkerTenant};

/// How often to scan for channels nearing expiry. Fed to the tenant harness
/// as [`tick_renew`]'s sweep cadence — same value as `graph_subscriptions::
/// POLL_INTERVAL` (no reason for these two renewal sweeps to run on
/// different clocks).
pub const POLL_INTERVAL: Duration = Duration::from_secs(30 * 60);
/// Renew anything expiring within this window. Comfortably larger than
/// `POLL_INTERVAL` so a channel is renewed at least once before it lapses,
/// even if a pass is skipped. Gmail/Chat run on a ~7-day clock and Calendar
/// channels are typically provisioned for a similar horizon, so this is well
/// inside all three.
const RENEW_WINDOW_HOURS: i64 = 24;
/// How often to run the backstop loud-alert sweep. Fed to the tenant harness
/// as [`tick_backstop`]'s sweep cadence — same value as `graph_subscriptions::
/// BACKSTOP_INTERVAL`.
pub const BACKSTOP_INTERVAL: Duration = Duration::from_secs(6 * 60 * 60);

// ── Persistence row shape ────────────────────────────────────────────────────

#[derive(Debug, Clone, sqlx::FromRow)]
struct WatchChannel {
    id: Uuid,
    account_id: Uuid,
    surface: String,
    resource_id: String,
    channel_id: String,
    channel_resource_id: String,
    topic_name: String,
    channel_token: String,
    expires_at: DateTime<Utc>,
    /// `NULL` = account-wide (Calendar, and any future account-wide
    /// surface); `Some` = the connecting member's own token (Gmail,
    /// `migrations_control/0186_google_gmail_watch_owner_and_threads.sql`
    /// — gsuite Train G2 PR 3). Drives [`google_token`]'s surface-aware
    /// credential lookup.
    owner_user_id: Option<Uuid>,
    /// When the last renewal attempt failed (`mark_failed`); cleared by
    /// `mark_renewed`. Drives [`retry_ready`]'s backoff.
    last_error_at: Option<DateTime<Utc>>,
}

/// Minimum gap between renewal attempts on a channel whose last attempt
/// failed. A failed renewal leaves `expires_at` untouched (it is still due),
/// so without this every sweep would re-attempt it immediately, hammering
/// Google (and the logs) while e.g. `GOOGLE_CALENDAR_WEBHOOK_URL` is unset.
/// A never-failed channel (`last_error_at` NULL, which `mark_renewed` also
/// restores) is never delayed, so a genuinely due renewal is not held back.
const RETRY_BACKOFF_MINUTES: i64 = 15;

/// Pure backoff rule: attempt now unless the previous attempt failed within
/// [`RETRY_BACKOFF_MINUTES`].
fn retry_ready(last_error_at: Option<DateTime<Utc>>, now: DateTime<Utc>) -> bool {
    match last_error_at {
        None => true,
        Some(at) => now - at >= chrono::Duration::minutes(RETRY_BACKOFF_MINUTES),
    }
}

// ── Account resolution (org tenant) ─────────────────────────────────────────

/// The single account an org tenant database belongs to — identical
/// resolution to `workers::graph_subscriptions::resolve_tenant_account`
/// (kept as its own copy rather than a shared helper: neither worker owns
/// the other, and duplicating one two-line query is cheaper than a shared
/// dependency between two otherwise-independent connector workers).
async fn resolve_tenant_account(control_pool: &PgPool, org_id: &str) -> Result<Uuid, String> {
    sqlx::query_scalar::<_, Uuid>("SELECT account_id FROM tenant_databases WHERE clerk_org_id = $1")
        .bind(org_id)
        .fetch_optional(control_pool)
        .await
        .map_err(|e| e.to_string())?
        .ok_or_else(|| format!("no tenant_databases row for org {org_id}"))
}

// ── Renewal tick ─────────────────────────────────────────────────────────────

/// One renewal pass on one tenant's `(pool, store)` pair. Driven by
/// `workers::tenant_harness::run_multi_tenant` as a sweep worker
/// (`HarnessConfig::sweep_worker(_, POLL_INTERVAL)`).
pub async fn tick_renew(
    t: &WorkerTenant,
    control_pool: &PgPool,
    http: &reqwest::Client,
) -> Result<Tick, String> {
    // The plane this tick's accounts keep their DEKs on, resolved ONCE from
    // the tenant the harness already picked — see `crypto::DekPlaneSource`.
    // This chain holds no `AppState`, so it cannot call
    // `DekPlane::resolve_for_account`; it reuses the harness's decision
    // instead of re-deriving one.
    let dek = crate::crypto::DekPlaneSource::for_worker_tenant(&t.tenant, &t.pool, control_pool);
    match &t.tenant {
        // Consumer: `t.pool` and `control_pool` are the same physical
        // database (`tenancy::extractor::Tenant`'s own doc), so
        // `resolve_google_watch_channels_due`'s cross-account sweep already
        // finds every surface, Gmail included — no split needed here.
        TenantId::Consumer => renew_due(&t.pool, control_pool, http, &dek, None).await,
        TenantId::Org(org_id) => {
            let account_id = resolve_tenant_account(control_pool, org_id).await?;
            renew_due(&t.pool, control_pool, http, &dek, Some(account_id)).await
        }
    }
}

/// Renew every channel due within `RENEW_WINDOW_HOURS`. `scope_account`
/// narrows to one account (the org-tenant case, where `t.pool` already only
/// contains that one account's rows — the filter is defensive, mirroring how
/// `graph_subscriptions`' `_for_account` queries are belt-and-suspenders on
/// an org tenant); `None` sweeps every account (the consumer case, where
/// `t.pool` holds every personal account).
async fn renew_due(
    pool: &PgPool,
    control_pool: &PgPool,
    http: &reqwest::Client,
    dek: &crate::crypto::DekPlaneSource,
    scope_account: Option<Uuid>,
) -> Result<Tick, String> {
    let cutoff = Utc::now() + chrono::Duration::hours(RENEW_WINDOW_HOURS);
    let now = Utc::now();
    let due: Vec<WatchChannel> = channels_due(pool, control_pool, cutoff, scope_account)
        .await
        .map_err(|e| e.to_string())?
        .into_iter()
        .filter(|c| retry_ready(c.last_error_at, now))
        .collect();
    if due.is_empty() {
        return Ok(Tick::idle());
    }
    tracing::info!(count = due.len(), "renewing google watch channels");
    let n = due.len();
    for channel in due {
        if let Err(e) = renew_one(pool, control_pool, http, dek, &channel).await {
            tracing::warn!(
                error = %e, channel_id = %channel.id, %channel.account_id, surface = %channel.surface,
                "google watch channel renewal failed"
            );
            let _ = mark_failed_if_same(watch_pool_for(&channel, pool, control_pool), channel.account_id, channel.id, Some(&channel.channel_id), &e.to_string()).await;
        }
    }
    Ok(Tick::worked(n))
}

/// Gmail, Chat, Meet and member-owned Calendar metadata live on control.
/// Personal account-wide Calendar metadata stays on the content pool.
fn watch_pool_for<'a>(channel: &WatchChannel, pool: &'a PgPool, control_pool: &'a PgPool) -> &'a PgPool {
    if is_control_plane_row(&channel.surface, channel.owner_user_id) {
        control_pool
    } else {
        pool
    }
}

/// Whether a `(surface, owner_user_id)` watch row lives on the CONTROL plane
/// (see [`watch_pool_for`]).
fn is_control_plane_row(surface: &str, owner_user_id: Option<Uuid>) -> bool {
    matches!(surface, "gmail" | "chat" | "meet") || (surface == "calendar" && owner_user_id.is_some())
}

/// Open an account-scoped, tenant-system-actor transaction on `pool` for
/// `account_id`. Every `google_watch_channels` read/write in this module
/// goes through this (or [`channels_due`]'s cross-account discovery hop) —
/// CodeRabbit review finding on PR #674: without it, the consumer/control
/// pool's `account_isolation` RLS policy (`0180`) makes an unscoped query
/// return ZERO ROWS whenever `app.account_id` is unset (the ambient state of
/// an `AppState.db`-style pool outside an opened scope), silently stopping
/// every renewal the moment this connects as the least-privilege
/// `coralswarm_app` role rather than the table owner. Mirrors
/// `connectors::microsoft`'s own `scoped_tx` helper exactly — same
/// `begin_tenant_system_scoped` call, same rationale (this is background
/// work with no single end-user `user_id` to present, so the tenant-system
/// bypass — not `begin_tenant_scoped` — is the right primitive; see that
/// function's own doc comment on `tenancy::scope`).
async fn scoped_tx(
    pool: &PgPool,
    account_id: Uuid,
    what: &str,
) -> Result<sqlx::Transaction<'static, sqlx::Postgres>, ConnectorError> {
    begin_tenant_system_scoped(pool, account_id)
        .await
        .map_err(|e| ConnectorError::Internal(format!("{what} scoped tx: {e}")))
}

/// Channels due within `cutoff`. `scope_account` narrows to one account (the
/// org-tenant case, where `t.pool` already only contains that one account's
/// rows — opens a single scoped transaction directly, no discovery hop
/// needed since the caller already knows the account); `None` sweeps every
/// account (the consumer case, where `t.pool` is the shared control/consumer
/// pool holding every personal account).
///
/// The `None` branch is cross-account BY DEFINITION — there is no single
/// account to scope a query against when the whole point is discovering
/// which accounts have work. It therefore uses the same two-hop pattern
/// `connectors::microsoft::expiring_subscriptions` established for the
/// identical problem: a `SECURITY DEFINER` bootstrap function
/// (`resolve_google_watch_channels_due`, migration `0181`) returns bare
/// `(id, account_id)` pairs ONLY — never `resource_id`, `channel_token`, or
/// any other column — and each id is then re-read inside its own
/// [`scoped_tx`]. One bad/racing account (e.g. a row deleted between the two
/// hops) is logged and skipped rather than failing the whole sweep, same
/// discipline `expiring_subscriptions` documents for its own per-account
/// loop.
async fn channels_due(
    pool: &PgPool,
    control_pool: &PgPool,
    cutoff: DateTime<Utc>,
    scope_account: Option<Uuid>,
) -> Result<Vec<WatchChannel>, ConnectorError> {
    match scope_account {
        // Org tenant: `pool` (t.pool) and `control_pool` genuinely diverge
        // (`tenancy::extractor::Tenant`'s own doc) — gsuite Train G2 PR 7's
        // single-plane decision means a Gmail row for this account lives on
        // `control_pool`, never `pool`, so both are queried and merged.
        // Calendar/Chat rows are unaffected and stay on `pool`.
        Some(account_id) => {
            let mut rows = {
                let mut tx = scoped_tx(pool, account_id, "channels_due_for_account").await?;
                let rows = sqlx::query_as::<_, WatchChannel>(
                    "SELECT id, account_id, surface, resource_id, channel_id, channel_resource_id, \
                            topic_name, channel_token, expires_at, owner_user_id, last_error_at \
                     FROM google_watch_channels \
                     WHERE account_id = $1 AND surface NOT IN ('gmail','chat','meet') \
                       AND NOT (surface = 'calendar' AND owner_user_id IS NOT NULL) \
                       AND expires_at <= $2",
                )
                .bind(account_id)
                .bind(cutoff)
                .fetch_all(&mut *tx)
                .await
                .map_err(|e| ConnectorError::Internal(format!("channels_due_for_account: {e}")))?;
                tx.rollback().await.ok();
                rows
            };
            let mut gmail_tx = scoped_tx(control_pool, account_id, "channels_due_for_account_gmail").await?;
            let mut gmail_rows = sqlx::query_as::<_, WatchChannel>(
                "SELECT id, account_id, surface, resource_id, channel_id, channel_resource_id, \
                        topic_name, channel_token, expires_at, owner_user_id, last_error_at \
                 FROM google_watch_channels \
                 WHERE account_id = $1 \
                   AND (surface IN ('gmail','chat','meet') OR (surface = 'calendar' AND owner_user_id IS NOT NULL)) \
                   AND expires_at <= $2",
            )
            .bind(account_id)
            .bind(cutoff)
            .fetch_all(&mut *gmail_tx)
            .await
            .map_err(|e| ConnectorError::Internal(format!("channels_due_for_account_gmail: {e}")))?;
            gmail_tx.rollback().await.ok();
            rows.append(&mut gmail_rows);
            Ok(rows)
        }
        None => {
            let due: Vec<(Uuid, Uuid)> =
                sqlx::query_as("SELECT * FROM resolve_google_watch_channels_due($1)")
                    .bind(cutoff)
                    .fetch_all(pool)
                    .await
                    .map_err(|e| {
                        ConnectorError::Internal(format!("resolve_google_watch_channels_due: {e}"))
                    })?;

            let mut out = Vec::with_capacity(due.len());
            for (id, account_id) in due {
                // Org watches live on control too, but require their tenant DEK.
                if crate::routes::slack::account_plane(control_pool, account_id).await.map_err(|e| ConnectorError::Internal(e.to_string()))? == crate::routes::slack::AccountPlane::Org { continue; }
                let mut tx = match scoped_tx(pool, account_id, "channels_due_sweep").await {
                    Ok(tx) => tx,
                    Err(e) => {
                        tracing::warn!(
                            error = %e, %account_id,
                            "channels_due: scoped tx failed — skipping this account for this tick"
                        );
                        continue;
                    }
                };
                let row: Option<WatchChannel> = match sqlx::query_as(
                    "SELECT id, account_id, surface, resource_id, channel_id, channel_resource_id, \
                            topic_name, channel_token, expires_at, owner_user_id, last_error_at \
                     FROM google_watch_channels WHERE id = $1 AND account_id = $2",
                )
                .bind(id)
                .bind(account_id)
                .fetch_optional(&mut *tx)
                .await
                {
                    Ok(r) => r,
                    Err(e) => {
                        tracing::warn!(
                            error = %e, %account_id, %id,
                            "channels_due: detail read failed — skipping this channel for this tick"
                        );
                        tx.rollback().await.ok();
                        continue;
                    }
                };
                tx.rollback().await.ok();
                // A row deleted between the two hops simply drops out of the
                // work set — the next tick re-derives it if it still exists.
                if let Some(row) = row {
                    out.push(row);
                }
            }
            Ok(out)
        }
    }
}

async fn mark_renewed(
    pool: &PgPool,
    account_id: Uuid,
    id: Uuid,
    new_expiry: DateTime<Utc>,
    new_channel_id: Option<&str>,
    new_channel_resource_id: Option<&str>,
    new_channel_token: Option<&str>,
) -> Result<(), ConnectorError> {
    let mut tx = scoped_tx(pool, account_id, "mark_renewed").await?;
    sqlx::query(
        "UPDATE google_watch_channels \
         SET expires_at = $2, last_renewed_at = now(), last_error = NULL, last_error_at = NULL, \
             updated_at = now(), \
             channel_id = COALESCE($3, channel_id), \
             channel_resource_id = COALESCE($4, channel_resource_id), \
             channel_token = COALESCE($6, channel_token) \
         WHERE id = $1 AND account_id = $5",
    )
    .bind(id)
    .bind(new_expiry)
    .bind(new_channel_id)
    .bind(new_channel_resource_id)
    .bind(account_id)
    .bind(new_channel_token)
    .execute(&mut *tx)
    .await
    .map_err(|e| ConnectorError::Internal(format!("mark_renewed: {e}")))?;
    tx.commit()
        .await
        .map_err(|e| ConnectorError::Internal(format!("mark_renewed commit: {e}")))?;
    Ok(())
}

#[cfg(test)]
async fn mark_failed(pool: &PgPool, account_id: Uuid, id: Uuid, error: &str) -> Result<(), ConnectorError> {
    mark_failed_if_same(pool,account_id,id,None,error).await
}
async fn mark_failed_if_same(pool: &PgPool, account_id: Uuid, id: Uuid, channel_id:Option<&str>, error: &str) -> Result<(), ConnectorError> {
    let mut tx = scoped_tx(pool, account_id, "mark_failed").await?;
    sqlx::query(
        "UPDATE google_watch_channels SET last_error = $2, last_error_at = now(), updated_at = now() \
         WHERE id = $1 AND account_id = $3 AND ($4::text IS NULL OR channel_id=$4)",
    )
    .bind(id)
    .bind(error.chars().take(2000).collect::<String>())
    .bind(account_id)
    .bind(channel_id)
    .execute(&mut *tx)
    .await
    .map_err(|e| ConnectorError::Internal(format!("mark_failed: {e}")))?;
    tx.commit()
        .await
        .map_err(|e| ConnectorError::Internal(format!("mark_failed commit: {e}")))?;
    Ok(())
}

/// Renew one channel, dispatching to the surface-specific renewal call.
async fn renew_one(
    pool: &PgPool,
    control_pool: &PgPool,
    http: &reqwest::Client,
    dek: &crate::crypto::DekPlaneSource,
    channel: &WatchChannel,
) -> Result<(), ConnectorError> {
    // gsuite Train G2 PR 3, design doc PR G2.3 item 6: `google_token` is
    // surface-aware. **Generalised from a `gmail`-vs-everything-else `if` to
    // a real per-surface match by gsuite Train G4 PR 4** (design doc §1.5,
    // the fix G4.3 deliberately deferred because it created no
    // `surface='chat'` rows and the bug was therefore unreachable):
    //
    // * `gmail` → `("google_gmail", Member(owner_user_id))`
    // * `chat`  → `("google_chat",  Member(owner_user_id))` — matching
    //   `connectors::google::Surface::Chat`'s own `token_provider()` /
    //   `token_owner_for()`. Before this, a chat row fell into the `else`
    //   arm and loaded the CALENDAR-grandfathered `oauth_tokens` row under
    //   provider `"google"` with `AccountWide`: on an account with a
    //   Calendar connection that renews a Chat subscription with a
    //   Calendar-scoped token (403); on an account without one it returns
    //   `NotConfigured` and **every chat row error-loops** through
    //   `mark_failed` into `tick_backstop`'s `connector_health = true`
    //   `error!`. G4.4 is the PR that makes `surface='chat'` rows exist, so
    //   it is the PR that must fix this.
    // * `calendar` (and anything else still account-wide) →
    //   `("google", AccountWide)`, unchanged.
    //
    // No fallback for either per-member surface: a row with no
    // `owner_user_id` is a data-integrity problem, not something to silently
    // treat as account-wide (the AccountWide-never-for-a-per-member-surface
    // invariant).
    // `calendar` WITH an owner (Google Calendar org lift) is one org member's
    // own channel: their per-member row under the grandfathered `"google"`
    // literal (`Surface::calendar_token_owner`). Without an owner it is the
    // personal account-wide channel, unchanged.
    // A member-owned Calendar channel whose owner has LEFT the account is not
    // renewed: it is stopped with Google (best-effort) and its row deleted,
    // matching `google_calendar_sync::sync_org_member`, which already skips a
    // departed member's syncs. Without this the channel would be re-created
    // every ~week for someone who is no longer in the org.
    if matches!(channel.surface.as_str(), "chat" | "meet") {
        if let TenantId::Org(org_id) = dek.tenant() {
            let surface = if channel.surface == "meet" { crate::connectors::google::Surface::Meet } else { crate::connectors::google::Surface::Chat };
            let enabled: Option<bool> = sqlx::query_scalar("SELECT enabled FROM org_connector_flags WHERE clerk_org_id=$1 AND provider=$2")
                .bind(org_id).bind(surface.flag_provider()).fetch_optional(control_pool).await.map_err(|e| ConnectorError::Internal(e.to_string()))?;
            if !enabled.unwrap_or_else(|| crate::connectors::google::surface_env_default(surface)) { return Ok(()); }
        }

        if let Some(owner) = channel.owner_user_id {
            if !crate::connectors::google_chat::is_current_account_member(control_pool, channel.account_id, owner).await? {
                let mut tx = scoped_tx(control_pool, channel.account_id, "retire_departed_workspace_subscription").await?;
                sqlx::query("DELETE FROM google_watch_channels WHERE id=$1 AND account_id=$2 AND channel_id=$3")
                    .bind(channel.id).bind(channel.account_id).bind(&channel.channel_id).execute(&mut *tx).await.map_err(|e| ConnectorError::Internal(e.to_string()))?;
                tx.commit().await.map_err(|e| ConnectorError::Internal(e.to_string()))?;
                return Ok(());
            }
        }
    }
    // A Gmail watch whose owner has LEFT the account is not renewed
    // (settings launch PR 2.8): renewing it refreshed the departed member's
    // token and kept their mailbox pushing into the org. The gate offboards
    // their capture — token rows, this watch (stopped upstream unless another
    // account still watches the mailbox) and the rest.
    if channel.surface == "gmail" {
        if let Some(owner) = channel.owner_user_id {
            if !crate::connectors::member_offboarding::admit_capture_or_offboard(
                http,
                control_pool,
                Some(&dek.plane_for(channel.account_id)),
                channel.account_id,
                owner,
                "gmail_watch_renewal",
            )
            .await
            {
                return Ok(());
            }
        }
    }
    if channel.surface == "calendar" {
        if let Some(owner_user_id) = channel.owner_user_id {
            if !crate::workers::google_calendar_sync::is_current_member(
                control_pool,
                channel.account_id,
                owner_user_id,
            )
            .await?
            {
                return retire_departed_calendar_channel(control_pool, http, dek, channel, owner_user_id).await;
            }
        }
    }
    let per_member_provider = match channel.surface.as_str() {
        "gmail" => Some("google_gmail"),
        "chat" => Some("google_chat"),
        "meet" => Some("google_meet"),
        "calendar" if channel.owner_user_id.is_some() => Some("google"),
        _ => None,
    };
    let (provider_key, owner) = match per_member_provider {
        Some(provider_key) => {
            let owner_user_id = channel.owner_user_id.ok_or_else(|| {
                ConnectorError::Internal(format!(
                    "{} watch channel {} has no owner_user_id — cannot resolve its per-member token",
                    channel.surface, channel.id
                ))
            })?;
            (provider_key, TokenOwner::Member(owner_user_id))
        }
        None => ("google", TokenOwner::AccountWide),
    };
    // A Gmail channel's `resource_id` is the member's mailbox email: key it
    // per Google user, like every other Gmail call (`google_gmail::rate_key`).
    let key = if channel.surface == "gmail" {
        crate::connectors::google_gmail::rate_key(channel.account_id, &channel.resource_id)
    } else {
        format!("{provider_key}:{}", channel.account_id)
    };
    crate::graph_rate_limit::scoped(key, async {
        let token = google_token(control_pool, http, dek, channel.account_id, provider_key, owner)
            .await?
            .ok_or_else(|| ConnectorError::NotConfigured("no google connection for account".into()))?;

        match channel.surface.as_str() {
            "gmail" => {
                let expiry = renew_gmail(http, &token, &channel.topic_name).await?;
                // gsuite Train G2 PR 7: `control_pool`, not `pool` — this
                // surface's `google_watch_channels` row lives on CONTROL
                // only.
                mark_renewed(control_pool, channel.account_id, channel.id, expiry, None, None, None).await?;
            }
            "chat" | "meet" => {
                // `channel.channel_id`, NOT `channel.resource_id` (gsuite
                // Train G4 PR 4, design doc §1.2 — the second fix G4.3
                // deferred). `0180`'s header and the merged `renew_chat`
                // disagreed about what a chat row's `resource_id` holds, and
                // NEITHER had ever executed. G4.4 resolves it:
                // `resource_id` = the member's Google `sub` (a STABLE upsert
                // key, so `0180:96`'s uniqueness really does mean "one row
                // per member" across subscription re-creation), `channel_id`
                // = the Workspace Events subscription name (globally unique,
                // which is what `0182:93`'s partial unique index wants).
                // PATCHing `v1/{resource_id}` would have sent
                // `v1/users/{sub}` to the subscriptions endpoint.
                // `connectors::google_chat::upsert_watch` writes both halves;
                // `recreated_subscription_updates_the_same_watch_row_pg`
                // pins the key argument.
                let expiry = renew_chat(http, &token, &channel.channel_id).await?;
                let mut tx = scoped_tx(control_pool, channel.account_id, "renew_workspace_subscription").await?;
                sqlx::query("UPDATE google_watch_channels SET expires_at=$1,last_renewed_at=now(),last_error=NULL,last_error_at=NULL,updated_at=now() WHERE account_id=$2 AND id=$3 AND channel_id=$4")
                    .bind(expiry).bind(channel.account_id).bind(channel.id).bind(&channel.channel_id).execute(&mut *tx).await.map_err(|e| ConnectorError::Internal(e.to_string()))?;
                tx.commit().await.map_err(|e| ConnectorError::Internal(e.to_string()))?;
            }
            "calendar" => {
                let (new_channel_id, new_resource_id, expiry, new_token) = renew_calendar(
                    http,
                    &token,
                    // An org member's row carries a `:<user_id>` suffix on
                    // its resource_id — never let it reach Google's URL.
                    crate::connectors::google_calendar::calendar_id_of(&channel.resource_id),
                    &channel.channel_id,
                    &channel.channel_resource_id,
                )
                .await?;
                mark_renewed(
                    watch_pool_for(channel, pool, control_pool),
                    channel.account_id,
                    channel.id,
                    expiry,
                    Some(&new_channel_id),
                    Some(&new_resource_id),
                    Some(&new_token),
                )
                .await?;
            }
            other => {
                return Err(ConnectorError::Internal(format!("unknown google watch surface {other:?}")));
            }
        }
        Ok(())
    })
    .await
}

/// Stop a departed member's Calendar channel with Google (best-effort: their
/// token is usually already gone, and an unstopped channel just expires on its
/// own clock) and delete its `google_watch_channels` row so it is never
/// discovered, renewed, or alerted on again. The row lives on the control
/// plane (a member-owned Calendar row always does, see [`watch_pool_for`]).
async fn retire_departed_calendar_channel(
    control_pool: &PgPool,
    http: &reqwest::Client,
    dek: &crate::crypto::DekPlaneSource,
    channel: &WatchChannel,
    owner_user_id: Uuid,
) -> Result<(), ConnectorError> {
    tracing::info!(
        account_id = %channel.account_id, %owner_user_id, channel_id = %channel.id,
        "google watch renewal: calendar channel owner is no longer a member — stopping, not renewing"
    );
    if !channel.channel_id.is_empty() && !channel.channel_resource_id.is_empty() {
        crate::graph_rate_limit::scoped(format!("google:{}", channel.account_id), async {
            match google_token(
                control_pool,
                http,
                dek,
                channel.account_id,
                "google",
                TokenOwner::Member(owner_user_id),
            )
            .await
            {
                Ok(Some(token)) => {
                    let stopped = crate::connectors::google::google_send(|| {
                        http.post(crate::connectors::google_transport::endpoint("https://www.googleapis.com/calendar/v3/channels/stop"))
                            .bearer_auth(&token)
                            .json(&serde_json::json!({
                                "id": channel.channel_id,
                                "resourceId": channel.channel_resource_id,
                            }))
                    })
                    .await
                    .and_then(|r| r.error_for_status().map_err(|e| ConnectorError::Exchange(e.to_string())));
                    if let Err(e) = stopped {
                        tracing::warn!(error = %e, channel_id = %channel.id, "departed member's calendar channel stop failed — it will expire on its own");
                    }
                }
                Ok(None) => {}
                Err(e) => {
                    tracing::warn!(error = %e, channel_id = %channel.id, "departed member's token unusable — skipping channel stop");
                }
            }
        })
        .await;
    }
    let mut tx = scoped_tx(control_pool, channel.account_id, "retire_departed_calendar_channel").await?;
    sqlx::query("DELETE FROM google_watch_channels WHERE id = $1 AND account_id = $2")
        .bind(channel.id)
        .bind(channel.account_id)
        .execute(&mut *tx)
        .await
        .map_err(|e| ConnectorError::Internal(format!("retire_departed_calendar_channel: {e}")))?;
    tx.commit()
        .await
        .map_err(|e| ConnectorError::Internal(format!("retire_departed_calendar_channel commit: {e}")))?;
    Ok(())
}

/// Load + refresh the account's Google OAuth token for `(provider_key,
/// owner)`, persisting any refresh. `Ok(None)` when there is no connection
/// for that exact provider/owner pair. Mirrors `workers::graph_subscriptions
/// ::delegated_microsoft_token`'s shape; surface-aware since gsuite Train G2
/// PR 3 (see [`renew_one`]'s own comment) — no longer hardcoded to
/// `("google", AccountWide)`.
async fn google_token(
    control_pool: &PgPool,
    http: &reqwest::Client,
    dek: &crate::crypto::DekPlaneSource,
    account_id: Uuid,
    provider_key: &str,
    owner: TokenOwner,
) -> Result<Option<String>, ConnectorError> {
    // `control_pool` owns the `oauth_tokens` ROW (control-plane-only for
    // every account); `dek` owns the KEY, which for an org account is in the
    // org's own tenant `account_keys`. Two different databases, so two
    // different parameters.
    let dek_plane = dek.plane_for(account_id);
    let Some(mut creds) = load_creds_bg(control_pool, &dek_plane, account_id, provider_key, owner).await? else {
        return Ok(None);
    };
    let connector = GoogleConnector;
    connector.refresh_if_needed(http, &mut creds).await?;
    if let Some(refreshed) = creds.refreshed.as_ref() {
        if let Err(_resp) = store_token_bundle(control_pool, &dek_plane, account_id, provider_key, owner, refreshed).await {
            tracing::warn!(%account_id, provider = %provider_key, "failed to persist refreshed google token during watch renewal");
        }
    }
    Ok(Some(creds.access_token))
}

// ── Per-surface renewal calls ───────────────────────────────────────────────
//
// Best-effort against Google's publicly documented REST shapes; not yet
// exercised against a live GCP project (no per-surface watch creation exists
// to populate `google_watch_channels` — see this module's own doc comment).
// G2/G3/G4 exercise these for real as they wire up watch creation; treat this
// as the renewal half of that contract, written ahead of the create half.

#[derive(Debug, Deserialize)]
struct GmailWatchResponse {
    /// Milliseconds-since-epoch, as a decimal string (Gmail's convention for
    /// this field).
    expiration: String,
}

/// `POST https://gmail.googleapis.com/gmail/v1/users/me/watch` — re-calling
/// `users.watch` with the same topic idempotently extends the existing watch
/// another ~7 days; Gmail does not distinguish "create" from "renew" here.
async fn renew_gmail(
    http: &reqwest::Client,
    access_token: &str,
    topic_name: &str,
) -> Result<DateTime<Utc>, ConnectorError> {
    let resp = crate::connectors::google::google_send(|| {
        http.post(crate::connectors::google_transport::endpoint("https://gmail.googleapis.com/gmail/v1/users/me/watch"))
            .bearer_auth(access_token)
            .json(&serde_json::json!({ "topicName": topic_name }))
    })
    .await?;
    if !resp.status().is_success() {
        return Err(crate::connectors::google::google_api_error("gmail watch renewal", resp).await);
    }
    let parsed: GmailWatchResponse = resp
        .json()
        .await
        .map_err(|e| ConnectorError::Exchange(format!("gmail watch response parse: {e}")))?;
    parse_epoch_millis(&parsed.expiration)
}

/// Shape of a `google.longrunning.Operation` — the Workspace Events API's
/// `subscriptions.patch`/`.create`/`.delete` are all asynchronous and return
/// one of these rather than the resource directly (CodeRabbit review finding
/// on PR #674: an earlier draft of this function treated the PATCH response
/// as a `Subscription`, which it never is). `response` carries the finished
/// `Subscription` (as raw JSON — its `@type` discriminant isn't needed here,
/// only `expireTime`) once `done` is true; `error` carries a `google.rpc.
/// Status` when the operation failed instead of succeeding.
///
/// `pub(crate)` (gsuite Train G4 PR 4): `connectors::google_chat::
/// ensure_subscription` parses the SAME envelope off
/// `subscriptions.create`, so it reuses this type and [`poll_operation`]
/// rather than carrying a second copy that could drift from the one the
/// renewal path uses. A connector reaching into a worker module is unusual
/// enough to name: the alternative was duplicating ~40 lines of
/// long-running-operation plumbing whose exact shape a CodeRabbit round
/// already had to correct once (#674 round 1, finding 4).
#[derive(Debug, Deserialize)]
pub(crate) struct Operation {
    pub(crate) name: String,
    #[serde(default)]
    pub(crate) done: bool,
    #[serde(default)]
    pub(crate) response: Option<serde_json::Value>,
    #[serde(default)]
    pub(crate) error: Option<serde_json::Value>,
}

/// How long to poll a Workspace Events `Operation` before giving up — these
/// normally resolve in well under a second; this is a generous ceiling for a
/// background worker tick, not an expected duration.
const OPERATION_POLL_MAX_ATTEMPTS: u32 = 6;
const OPERATION_POLL_INTERVAL: std::time::Duration = std::time::Duration::from_millis(500);

/// Poll `GET https://workspaceevents.googleapis.com/v1/{operation.name}`
/// until `done`, or give up after [`OPERATION_POLL_MAX_ATTEMPTS`].
pub(crate) async fn poll_operation(
    http: &reqwest::Client,
    access_token: &str,
    mut op: Operation,
) -> Result<Operation, ConnectorError> {
    let mut attempts = 0u32;
    while !op.done {
        if attempts >= OPERATION_POLL_MAX_ATTEMPTS {
            return Err(ConnectorError::Exchange(format!(
                "workspace events operation {} did not complete after {} polls",
                op.name, OPERATION_POLL_MAX_ATTEMPTS
            )));
        }
        tokio::time::sleep(OPERATION_POLL_INTERVAL).await;
        let url = format!("{}/{}", crate::connectors::google_chat::workspace_events_base(), op.name);
        let resp = crate::connectors::google::google_send(|| http.get(crate::connectors::google_transport::endpoint(&url)).bearer_auth(access_token)).await?;
        if !resp.status().is_success() {
            let status = resp.status();
            return Err(ConnectorError::upstream(status, format!(
                "workspace events operation poll failed: {status}"
            )));
        }
        op = resp
            .json()
            .await
            .map_err(|e| ConnectorError::Exchange(format!("operation poll response parse: {e}")))?;
        attempts += 1;
    }
    Ok(op)
}

/// Pure extraction of `expireTime` from a finished (`done: true`) Workspace
/// Events `Operation`'s `response` — split out from [`renew_chat`] so it is
/// unit-testable against a fixture without any network I/O.
fn expire_time_from_operation(op: &Operation) -> Result<DateTime<Utc>, ConnectorError> {
    if let Some(err) = &op.error {
        return Err(ConnectorError::Exchange(format!(
            "workspace events operation {} failed: {err}",
            op.name
        )));
    }
    let expire_time = op
        .response
        .as_ref()
        .and_then(|r| r.get("expireTime"))
        .and_then(|v| v.as_str())
        .ok_or_else(|| {
            ConnectorError::Exchange(format!(
                "workspace events operation {} response missing expireTime",
                op.name
            ))
        })?;
    DateTime::parse_from_rfc3339(expire_time)
        .map(|dt| dt.with_timezone(&Utc))
        .map_err(|e| ConnectorError::Exchange(format!("chat expireTime parse: {e}")))
}

/// `PATCH https://workspaceevents.googleapis.com/v1/{name}?updateMask=ttl` —
/// extends a Workspace Events API subscription's time-to-live. `name` is the
/// subscription's resource name (`subscriptions/…`), stored as this
/// channel's `resource_id`. `ttl: "0s"` requests the API's maximum allowed
/// expiration (per Google's documented semantics for this field) rather than
/// a specific duration — `tick_renew`'s `RENEW_WINDOW_HOURS` re-renews well
/// before whatever that maximum turns out to be, so this is not a fixed
/// window this code needs to track. The call itself only ever STARTS the
/// operation; [`poll_operation`] + [`expire_time_from_operation`] resolve
/// the actual new expiry.
async fn renew_chat(
    http: &reqwest::Client,
    access_token: &str,
    subscription_name: &str,
) -> Result<DateTime<Utc>, ConnectorError> {
    let url = format!("{}/{subscription_name}?updateMask=ttl", crate::connectors::google_chat::workspace_events_base());
    let resp = crate::connectors::google::google_send(|| {
        http.patch(crate::connectors::google_transport::endpoint(&url)).bearer_auth(access_token).json(&serde_json::json!({ "ttl": "0s" }))
    })
    .await?;
    if !resp.status().is_success() {
        let status = resp.status();
        return Err(ConnectorError::upstream(status, format!("chat subscription renewal failed: {status}")));
    }
    let op: Operation = resp
        .json()
        .await
        .map_err(|e| ConnectorError::Exchange(format!("chat subscription operation parse: {e}")))?;
    let op = poll_operation(http, access_token, op).await?;
    expire_time_from_operation(&op)
}

#[derive(Debug, Deserialize)]
struct CalendarChannelResponse {
    id: String,
    #[serde(rename = "resourceId")]
    resource_id: String,
    /// Milliseconds-since-epoch, as a decimal string (same convention as
    /// Gmail's `expiration`).
    expiration: String,
}

/// Calendar channels cannot be renewed in place. **Order matters here**
/// (CodeRabbit review finding on PR #674): create the fresh `events.watch`
/// channel FIRST and confirm it succeeded, THEN stop the expiring one —
/// never the reverse. Stopping the old channel before the new one is
/// confirmed live would leave a window with NO active channel at all if the
/// creation call then failed (rate limit, transient 5xx, revoked
/// credentials, …) — exactly the "renewal failure is silent data loss" case
/// the plan warns about, and worse than doing nothing, since the un-renewed
/// old channel would otherwise have kept working until its own natural
/// expiry. Overlap between old and new channels is intentional and
/// harmless: both may briefly be live, and Calendar ingest (Train G3) is
/// responsible for deduping on `event.updated`/`etag` like any other
/// at-least-once delivery source. If channel creation fails, the old
/// channel is left untouched and this renewal is retried on the next tick.
/// The channel token is regenerated fresh each renewal (not reused) and
/// returned for persistence — this
/// matches "each channel gets its own creation-time secret" the same way a
/// Graph subscription's `client_state` is minted once, at creation, never
/// reused across a resource's channel lineage.
async fn renew_calendar(
    http: &reqwest::Client,
    access_token: &str,
    calendar_id: &str,
    old_channel_id: &str,
    old_channel_resource_id: &str,
) -> Result<(String, String, DateTime<Utc>, String), ConnectorError> {
    let webhook_url = std::env::var("GOOGLE_CALENDAR_WEBHOOK_URL").map_err(|_| {
        ConnectorError::NotConfigured("GOOGLE_CALENDAR_WEBHOOK_URL not configured".into())
    })?;
    let new_channel_id = Uuid::new_v4().to_string();
    // Minted fresh and returned so the caller persists it with the new
    // channel_id: the push webhook verifies `X-Goog-Channel-Token` against the
    // STORED value, so a token that is sent but not stored (or stored but
    // never sent) makes every notification 401.
    let channel_token = crate::connectors::google_calendar::generate_channel_token();
    let body = calendar_watch_body(&new_channel_id, &webhook_url, &channel_token);
    let url = format!(
        "https://www.googleapis.com/calendar/v3/calendars/{}/events/watch",
        urlencoding::encode(calendar_id)
    );
    let resp = crate::connectors::google::google_send(|| {
        http.post(crate::connectors::google_transport::endpoint(&url)).bearer_auth(access_token).json(&body)
    })
    .await?;
    if !resp.status().is_success() {
        let status = resp.status();
        // The old channel is untouched — nothing was stopped, so nothing is
        // lost; the next tick retries.
        return Err(ConnectorError::upstream(status, format!("calendar channel creation failed: {status}")));
    }
    let parsed: CalendarChannelResponse = resp
        .json()
        .await
        .map_err(|e| ConnectorError::Exchange(format!("calendar channel response parse: {e}")))?;
    let expiry = parse_epoch_millis(&parsed.expiration)?;

    // New channel confirmed live — now retire the old one. Best-effort: a
    // failed stop leaves a harmless extra channel running (it expires
    // naturally on its own clock) rather than turning an already-successful
    // renewal into a reported failure.
    // Same guard as `ensure_watch`: never stop a channel that shares the new
    // channel's id or resourceId — that could take the new one down too.
    if !old_channel_id.is_empty()
        && !old_channel_resource_id.is_empty()
        && old_channel_id != parsed.id
        && old_channel_resource_id != parsed.resource_id
    {
        match crate::connectors::google::google_send(|| {
            http.post(crate::connectors::google_transport::endpoint("https://www.googleapis.com/calendar/v3/channels/stop"))
                .bearer_auth(access_token)
                .json(&serde_json::json!({ "id": old_channel_id, "resourceId": old_channel_resource_id }))
        })
        .await
        .and_then(|r| r.error_for_status().map_err(|e| ConnectorError::Exchange(e.to_string())))
        {
            Ok(_) => {}
            Err(e) => {
                tracing::warn!(
                    error = %e, old_channel_id,
                    "calendar channel stop failed after successful renewal — leaving it to expire naturally"
                );
            }
        }
    }

    Ok((parsed.id, parsed.resource_id, expiry, channel_token))
}

/// The `events.watch` request body. Factored out so the invariant "a
/// renewal always sends a non-empty token" is testable without HTTP.
fn calendar_watch_body(channel_id: &str, webhook_url: &str, channel_token: &str) -> serde_json::Value {
    serde_json::json!({
        "id": channel_id,
        "type": "web_hook",
        "address": webhook_url,
        "token": channel_token,
    })
}

fn parse_epoch_millis(s: &str) -> Result<DateTime<Utc>, ConnectorError> {
    let millis: i64 = s
        .parse()
        .map_err(|e| ConnectorError::Exchange(format!("epoch millis parse {s:?}: {e}")))?;
    DateTime::from_timestamp_millis(millis)
        .ok_or_else(|| ConnectorError::Exchange(format!("epoch millis out of range: {millis}")))
}

// ── Backstop: loud-alert sweep ──────────────────────────────────────────────

/// One backstop pass. Driven by `workers::tenant_harness::run_multi_tenant`
/// as a sweep worker (`HarnessConfig::sweep_worker(_, BACKSTOP_INTERVAL)`).
/// Unlike `graph_subscriptions::tick_backstop` (which re-lists content),
/// this backstop does not retry ingest — there is none yet — it does two
/// things: (1) loudly alerts on anything that has ALREADY lapsed (renewal
/// missed it — see [`alert_lapsed`]), and (2) makes one more renewal attempt
/// at each, on the theory that a transient renewal failure is better fixed
/// automatically than left for an operator.
pub async fn tick_backstop(
    t: &WorkerTenant,
    control_pool: &PgPool,
    http: &reqwest::Client,
) -> Result<Tick, String> {
    let dek = crate::crypto::DekPlaneSource::for_worker_tenant(&t.tenant, &t.pool, control_pool);
    let scope_account = match &t.tenant {
        TenantId::Consumer => None,
        TenantId::Org(org_id) => Some(resolve_tenant_account(control_pool, org_id).await?),
    };
    let now = Utc::now();
    let lapsed: Vec<WatchChannel> = lapsed_channels(&t.pool, control_pool, now, scope_account)
        .await
        .map_err(|e| e.to_string())?
        .into_iter()
        .filter(|c| retry_ready(c.last_error_at, now))
        .collect();
    if lapsed.is_empty() {
        return Ok(Tick::idle());
    }
    let n = lapsed.len();
    for channel in &lapsed {
        alert_lapsed(channel);
        if let Err(e) = renew_one(&t.pool, control_pool, http, &dek, channel).await {
            tracing::warn!(
                error = %e, channel_id = %channel.id, %channel.account_id, surface = %channel.surface,
                "google watch channel backstop renewal retry also failed"
            );
            let _ = mark_failed_if_same(watch_pool_for(channel, &t.pool, control_pool), channel.account_id, channel.id, Some(&channel.channel_id), &e.to_string()).await;
        } else {
            tracing::info!(
                channel_id = %channel.id, %channel.account_id, surface = %channel.surface,
                "google watch channel recovered by backstop retry"
            );
        }
    }
    Ok(Tick::worked(n))
}

async fn lapsed_channels(
    pool: &PgPool,
    control_pool: &PgPool,
    now: DateTime<Utc>,
    scope_account: Option<Uuid>,
) -> Result<Vec<WatchChannel>, ConnectorError> {
    channels_due(pool, control_pool, now, scope_account).await
}

/// The loud-failure signal the plan's "renewal failure MUST log loudly/alert"
/// obligation requires: `error!`, never `warn!` — by the time a channel
/// reaches the backstop sweep already past `expires_at`, Google has already
/// stopped delivering notifications for it, which is silent data loss in
/// progress, not a near-miss. `connector_health = true` mirrors the
/// convention `routes::graph_webhook`'s own connector-health warnings use, so
/// a future alerting hook keyed on that field picks this up the same way.
fn alert_lapsed(channel: &WatchChannel) {
    tracing::error!(
        channel_id = %channel.id,
        account_id = %channel.account_id,
        surface = %channel.surface,
        resource_id = %channel.resource_id,
        expired_at = %channel.expires_at,
        connector_health = true,
        "google watch channel LAPSED — notifications for this resource have stopped; \
         renewal missed it (see last_error/last_error_at on the row for why, if known)"
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn parse_epoch_millis_round_trips() {
        let dt = parse_epoch_millis("1700000000000").unwrap();
        assert_eq!(dt.timestamp_millis(), 1700000000000);
    }

    #[test]
    fn parse_epoch_millis_rejects_garbage() {
        assert!(parse_epoch_millis("not-a-number").is_err());
        assert!(parse_epoch_millis("").is_err());
    }

    // ── Workspace Events `Operation` handling (CodeRabbit review finding on
    // PR #674: `subscriptions.patch` returns a long-running Operation, not a
    // Subscription — these exercise the pure parsing without any network
    // I/O; `poll_operation`'s actual HTTP polling loop is exercised live
    // when Train G4 wires up real Chat subscriptions). ─────────────────────

    #[test]
    fn expire_time_from_operation_reads_a_finished_operations_response() {
        // Real shape per Google's `google.longrunning.Operation` convention:
        // `done: true` + `response` holding the finished Subscription.
        let op: Operation = serde_json::from_value(serde_json::json!({
            "name": "operations/abc123",
            "done": true,
            "response": {
                "@type": "type.googleapis.com/google.apps.events.subscriptions.v1.Subscription",
                "name": "subscriptions/xyz",
                "expireTime": "2026-08-29T00:00:00Z",
            }
        }))
        .unwrap();
        let expiry = expire_time_from_operation(&op).unwrap();
        assert_eq!(expiry.to_rfc3339(), "2026-08-29T00:00:00+00:00");
    }

    #[test]
    fn expire_time_from_operation_rejects_unfinished_operation_with_no_response() {
        let op: Operation = serde_json::from_value(serde_json::json!({
            "name": "operations/abc123",
            "done": false,
        }))
        .unwrap();
        assert!(expire_time_from_operation(&op).is_err());
    }

    #[test]
    fn expire_time_from_operation_surfaces_the_operations_own_error() {
        let op: Operation = serde_json::from_value(serde_json::json!({
            "name": "operations/abc123",
            "done": true,
            "error": {"code": 3, "message": "invalid ttl"}
        }))
        .unwrap();
        let err = expire_time_from_operation(&op).unwrap_err();
        assert!(err.to_string().contains("invalid ttl"), "{err}");
    }

    #[test]
    fn expire_time_from_operation_rejects_response_missing_expire_time() {
        let op: Operation = serde_json::from_value(serde_json::json!({
            "name": "operations/abc123",
            "done": true,
            "response": {"name": "subscriptions/xyz"}
        }))
        .unwrap();
        assert!(expire_time_from_operation(&op).is_err());
    }

    // ── DB-level tick logic (pg-gated, mirrors routes::graph_webhook's
    // ORGT_TEST_BASE_URL convention — see that module's test-header comment
    // for the exact invocation) ────────────────────────────────────────────
    //
    //   ORGT_TEST_BASE_URL=postgres://alex@localhost:5432/postgres \
    //     cargo test -p coralswarm-backend --lib workers::google_watch_renewal::tests

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
    async fn seed_user(pool: &PgPool) -> Uuid {
        sqlx::query_scalar("INSERT INTO users(id, email) VALUES (gen_random_uuid(), $1) RETURNING id")
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
    async fn seed_channel(
        pool: &PgPool,
        account_id: Uuid,
        surface: &str,
        resource_id: &str,
        expires_at: DateTime<Utc>,
    ) -> Uuid {
        sqlx::query_scalar(
            "INSERT INTO google_watch_channels (account_id, surface, resource_id, expires_at) \
             VALUES ($1, $2, $3, $4) RETURNING id",
        )
        .bind(account_id)
        .bind(surface)
        .bind(resource_id)
        .bind(expires_at)
        .fetch_one(pool)
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn channels_due_finds_only_channels_within_the_cutoff_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping channels_due_finds_only_channels_within_the_cutoff_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "gws_g12_channels_due";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let conn = recreate(&base, db).await;
        conn.close().await.ok();
        let pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();

        let owner = seed_user(&pool).await;
        let account_id = seed_account(&pool, owner).await;
        let now = Utc::now();
        let due_id = seed_channel(&pool, account_id, "gmail", "me", now + chrono::Duration::hours(1)).await;
        let _not_due =
            seed_channel(&pool, account_id, "gmail", "other-mailbox", now + chrono::Duration::days(10)).await;

        let due = channels_due(&pool, &pool, now + chrono::Duration::hours(24), None).await.unwrap();
        assert_eq!(due.len(), 1);
        assert_eq!(due[0].id, due_id);

        pool.close().await;
        let mut admin_conn = PgConnection::connect(&base).await.unwrap();
        admin_conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
    }

    // ── gsuite Train G2 PR 7: org-tenant discovery genuinely spans two
    // physical databases ─────────────────────────────────────────────────
    //
    // Every test above runs against ONE database standing in for both planes
    // (`channels_due`'s own `pool`/`control_pool` args are the same `&pool`)
    // — correct for Personal, but it cannot catch a regression that reads
    // Gmail rows from the wrong plane under Org, where the two are genuinely
    // different databases. This test builds a REAL control database and a
    // REAL tenant database, seeds a Gmail row on control-only and a Calendar
    // row on tenant-only (the shape gsuite Train G2 PR 7 establishes), and
    // proves `channels_due`'s `Some(account_id)` branch merges both. The
    // control-plane half runs through the least-privilege `coralswarm_app`
    // role (not the superuser/table-owner connection every other test in
    // this file uses) — `google_watch_channels`' control-tree
    // `account_isolation` RLS policy (migration `0180`) is real there, unlike
    // the tenant tree's twin (`0062`'s own header: "RLS deliberately NOT
    // covered"), so this is the one plane where an unscoped query could
    // silently zero-row instead of erroring.

    // `APP_ROLE_TEST_PASSWORD`/`set_app_role_password_with_retry`/
    // `app_role_pool` — shared with `connectors::google_gmail`'s own
    // cross-plane test, `crate::app_role_test_support` (gsuite Train G2 PR 7,
    // CodeRabbit round 1: was a byte-identical local copy in each file).
    use crate::app_role_test_support::app_role_pool;

    #[tokio::test]
    async fn channels_due_for_org_account_merges_gmail_from_control_and_calendar_from_tenant_under_the_coralswarm_app_role_pg(
    ) {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping channels_due_for_org_account_merges_gmail_from_control_and_calendar_from_tenant_\
                 under_the_coralswarm_app_role_pg: set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let control_db = "gws_g27_org_control";
        let _cleanup_control_db = crate::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", control_db);
        let tenant_db = "gws_g27_org_tenant";
        let _cleanup_tenant_db = crate::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", tenant_db);
        let _database_cleanup_0 = crate::test_db::TestDatabase::adopt_from_url(&base, control_db);
        let control_conn = recreate(&base, control_db).await;
        control_conn.close().await.ok();
        let _database_cleanup_1 = crate::test_db::TestDatabase::adopt_from_url(&base, tenant_db);
        let tenant_conn = recreate(&base, tenant_db).await;
        tenant_conn.close().await.ok();

        let control_url = with_db(&base, control_db);
        let control_owner: PgPool = PgPoolOptions::new().connect(&control_url).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&control_owner).await.unwrap();

        let tenant_url = with_db(&base, tenant_db);
        let tenant_owner: PgPool = PgPoolOptions::new().connect(&tenant_url).await.unwrap();
        sqlx::migrate!("./migrations_tenant").run(&tenant_owner).await.unwrap();
        sqlx::query("INSERT INTO tenant_meta (id, org_id) VALUES (true, 'org_g27_channels_due')")
            .execute(&tenant_owner)
            .await
            .unwrap();

        // ONE account, its Gmail watch row on control, its Calendar watch row
        // on the org's own tenant database — exactly the shape this PR
        // establishes.
        let owner = seed_user(&control_owner).await;
        let account_id = seed_account(&control_owner, owner).await;
        let now = Utc::now();
        let gmail_due_id =
            seed_channel(&control_owner, account_id, "gmail", "me@example.com", now + chrono::Duration::hours(1))
                .await;
        let _gmail_not_due_id = seed_channel(
            &control_owner,
            account_id,
            "gmail",
            "other@example.com",
            now + chrono::Duration::days(10),
        )
        .await;
        let calendar_due_id =
            seed_channel(&tenant_owner, account_id, "calendar", "primary", now + chrono::Duration::hours(1)).await;
        // A stray Gmail-surface row on the TENANT database — real code never
        // writes one (this PR's whole point), but this proves the
        // `surface NOT IN ('gmail','chat','meet')` filter on the `pool`-side query actually
        // excludes it rather than accidentally double-counting.
        let _stray_tenant_gmail_id = seed_channel(
            &tenant_owner,
            account_id,
            "gmail",
            "stray-on-tenant@example.com",
            now + chrono::Duration::hours(1),
        )
        .await;

        let control_app_pool = app_role_pool(&control_owner, &control_url).await;

        let due = channels_due(&tenant_owner, &control_app_pool, now + chrono::Duration::hours(24), Some(account_id))
            .await
            .expect("channels_due under the coralswarm_app role");

        let ids: std::collections::HashSet<Uuid> = due.iter().map(|c| c.id).collect();
        assert_eq!(due.len(), 2, "must merge exactly the two due rows across both planes, no more");
        assert!(ids.contains(&gmail_due_id), "the control-plane Gmail row must be discovered");
        assert!(ids.contains(&calendar_due_id), "the tenant-plane Calendar row must be discovered");
        assert!(
            !ids.contains(&_gmail_not_due_id),
            "a Gmail row past the cutoff must not be discovered"
        );
        assert!(
            !ids.contains(&_stray_tenant_gmail_id),
            "a stray gmail-surface row on the tenant database must never be discovered — Gmail is \
             control-plane only"
        );
        let gmail_row = due.iter().find(|c| c.id == gmail_due_id).unwrap();
        assert_eq!(gmail_row.surface, "gmail");
        let calendar_row = due.iter().find(|c| c.id == calendar_due_id).unwrap();
        assert_eq!(calendar_row.surface, "calendar");

        control_owner.close().await;
        control_app_pool.close().await;
        tenant_owner.close().await;
        let mut admin_conn = PgConnection::connect(&base).await.unwrap();
        admin_conn
            .execute(format!("DROP DATABASE IF EXISTS {control_db} WITH (FORCE)").as_str())
            .await
            .ok();
        admin_conn
            .execute(format!("DROP DATABASE IF EXISTS {tenant_db} WITH (FORCE)").as_str())
            .await
            .ok();
    }

    /// A departed member's Calendar channel is retired (row deleted, not
    /// renewed); a current member's is still attempted (no token in this test,
    /// so it records `last_error` rather than being deleted).
    #[tokio::test]
    async fn renew_due_retires_a_departed_members_calendar_channel_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping renew_due_retires_a_departed_members_calendar_channel_pg: set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let control_db = "gcal_depart_renew_ctl";
        let _cleanup_control_db = crate::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", control_db);
        let tenant_db = "gcal_depart_renew_ten";
        let _cleanup_tenant_db = crate::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", tenant_db);
        let _database_cleanup_2 = crate::test_db::TestDatabase::adopt_from_url(&base, control_db);
        recreate(&base, control_db).await.close().await.ok();
        let _database_cleanup_3 = crate::test_db::TestDatabase::adopt_from_url(&base, tenant_db);
        recreate(&base, tenant_db).await.close().await.ok();
        let control_url = with_db(&base, control_db);
        let control: PgPool = PgPoolOptions::new().connect(&control_url).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&control).await.unwrap();
        let control_app_pool = app_role_pool(&control, &control_url).await;
        let tenant: PgPool = PgPoolOptions::new().connect(&with_db(&base, tenant_db)).await.unwrap();
        sqlx::migrate!("./migrations_tenant").run(&tenant).await.unwrap();
        sqlx::query("INSERT INTO tenant_meta (id, org_id) VALUES (true, 'org_gcal_depart')")
            .execute(&tenant)
            .await
            .unwrap();

        let owner = seed_user(&control).await;
        let account_id = seed_account(&control, owner).await;
        let stays = seed_user(&control).await;
        let departed = seed_user(&control).await;
        // Plain account: no org seat-tier trigger. Only `stays` is a member.
        sqlx::query("INSERT INTO account_members(account_id, user_id, role) VALUES ($1, $2, 'member')")
            .bind(account_id)
            .bind(stays)
            .execute(&control)
            .await
            .unwrap();
        let soon = Utc::now() + chrono::Duration::hours(1);
        let mut ids = Vec::new();
        for u in [stays, departed] {
            let id: Uuid = sqlx::query_scalar(
                "INSERT INTO google_watch_channels \
                   (account_id, surface, resource_id, expires_at, owner_user_id, channel_id, channel_resource_id) \
                 VALUES ($1, 'calendar', $2, $3, $4, $5, 'res') RETURNING id",
            )
            .bind(account_id)
            .bind(format!("primary:{u}"))
            .bind(soon)
            .bind(u)
            .bind(format!("chan-{u}"))
            .fetch_one(&control)
            .await
            .unwrap();
            ids.push(id);
        }
        let (stays_row, departed_row) = (ids[0], ids[1]);

        let dek = crate::crypto::DekPlaneSource::for_worker_tenant(
            &TenantId::Org("org_gcal_depart".into()),
            &tenant,
            &control,
        );
        let http = reqwest::Client::new();
        let tick = renew_due(&tenant, &control_app_pool, &http, &dek, Some(account_id)).await.unwrap();
        assert_eq!(tick.processed, 2, "both due rows are processed");

        let departed_left: i64 = sqlx::query_scalar("SELECT count(*) FROM google_watch_channels WHERE id = $1")
            .bind(departed_row)
            .fetch_one(&control)
            .await
            .unwrap();
        assert_eq!(departed_left, 0, "the departed member's channel row must be deleted");
        let (stays_left, err): (i64, Option<String>) = sqlx::query_as(
            "SELECT count(*), max(last_error) FROM google_watch_channels WHERE id = $1",
        )
        .bind(stays_row)
        .fetch_one(&control)
        .await
        .unwrap();
        assert_eq!(stays_left, 1, "the current member's channel row must remain");
        assert!(err.is_some(), "the current member's channel must have been attempted (no token => last_error)");

        control_app_pool.close().await;
        control.close().await;
        tenant.close().await;
        let mut admin_conn = PgConnection::connect(&base).await.unwrap();
        for db in [control_db, tenant_db] {
            admin_conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
        }
    }

    #[tokio::test]
    async fn org_chat_and_meet_renew_own_grants_from_tenant_dek_with_lro_pg() {
        let Some(base)=base_url() else { return; };
        let control_db="gws_org_events_renew_ctl";
        let _cleanup_control_db = crate::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", control_db);let tenant_db="gws_org_events_renew_ten";
        let _database_cleanup_4: Vec<_> = [control_db, tenant_db].iter().map(|db| crate::test_db::TestDatabase::adopt_from_url(&base, db)).collect();
        for db in [control_db,tenant_db] { recreate(&base,db).await.close().await.ok(); }
        let control=PgPoolOptions::new().connect(&with_db(&base,control_db)).await.unwrap();
        let tenant=PgPoolOptions::new().connect(&with_db(&base,tenant_db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&control).await.unwrap();
        sqlx::migrate!("./migrations_tenant").run(&tenant).await.unwrap();
        sqlx::query("INSERT INTO tenant_meta(id,org_id) VALUES(true,'org_event_renew')").execute(&tenant).await.unwrap();
        let owner=seed_user(&control).await;let account=seed_account(&control,owner).await;
        sqlx::query("UPDATE accounts SET clerk_org_id='org_event_renew' WHERE id=$1").bind(account).execute(&control).await.unwrap();
        sqlx::query("INSERT INTO account_members(account_id,user_id,role,seat_tier_id) SELECT $1,$2,'owner',id FROM seat_tiers WHERE code='standard' AND is_active=true").bind(account).bind(owner).execute(&control).await.unwrap();
        for provider in ["google_chat","google_meet"] { sqlx::query("INSERT INTO org_connector_flags(clerk_org_id,provider,enabled) VALUES('org_event_renew',$1,true)").bind(provider).execute(&control).await.unwrap(); }
        std::env::set_var("MASTER_KEK_HEX","cc".repeat(32));
        let wrapped=coralswarm_core::kek::wrap_dek(&coralswarm_core::kek::load_kek().unwrap(),&coralswarm_core::kek::generate_dek()).unwrap();
        sqlx::query("INSERT INTO account_keys(account_id,dek_wrapped) VALUES($1,$2)").bind(account).bind(wrapped).execute(&tenant).await.unwrap();
        let control_app=app_role_pool(&control,&with_db(&base,control_db)).await;
        let dek=crate::crypto::DekPlaneSource::for_worker_tenant(&TenantId::Org("org_event_renew".into()),&tenant,&control_app);
        for surface in ["chat","meet"] {
            let token=crate::connectors::TokenBundle { access_token:format!("own-{surface}"),refresh_token:None,user_token:None,scope:"scope".into(),expires_at:Some(Utc::now()+chrono::Duration::hours(1)),provider_workspace_id:"owner@example.com".into(),provider_workspace_name:None,provider_user_id:format!("sub-{surface}") };
            store_token_bundle(&control_app,&dek.plane_for(account),account,&format!("google_{surface}"),TokenOwner::Member(owner),&token).await.unwrap();
            sqlx::query("INSERT INTO google_watch_channels(account_id,surface,resource_id,owner_user_id,channel_id,expires_at) VALUES($1,$2,$2,$3,$4,now()+interval '1 hour')").bind(account).bind(surface).bind(owner).bind(format!("subscriptions/{surface}")).execute(&control).await.unwrap();
        }
        async fn patch(axum::extract::Path(surface):axum::extract::Path<String>,headers:axum::http::HeaderMap,axum::Json(body):axum::Json<serde_json::Value>)->axum::Json<serde_json::Value> {
            assert_eq!(headers.get("authorization").unwrap().to_str().unwrap(),format!("Bearer own-{surface}"));assert_eq!(body["ttl"],"0s");
            axum::Json(serde_json::json!({"name":format!("operations/{surface}"),"done":false}))
        }
        async fn poll(axum::extract::Path(surface):axum::extract::Path<String>,headers:axum::http::HeaderMap)->axum::Json<serde_json::Value> {
            assert_eq!(headers.get("authorization").unwrap().to_str().unwrap(),format!("Bearer own-{surface}"));
            axum::Json(serde_json::json!({"name":format!("operations/{surface}"),"done":true,"response":{"expireTime":(Utc::now()+chrono::Duration::days(7)).to_rfc3339()}}))
        }
        let app=axum::Router::new().route("/subscriptions/{surface}",axum::routing::patch(patch)).route("/operations/{surface}",axum::routing::get(poll));
        let listener=tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();let url=format!("http://{}",listener.local_addr().unwrap());tokio::spawn(async move {axum::serve(listener,app).await.unwrap();});
        crate::connectors::google_chat::TEST_WORKSPACE_EVENTS_BASE.scope(url,async {
            let tick=renew_due(&tenant,&control_app,&reqwest::Client::new(),&dek,Some(account)).await.unwrap();assert_eq!(tick.processed,2);
        }).await;
        let renewed:i64=sqlx::query_scalar("SELECT count(*) FROM google_watch_channels WHERE expires_at>now()+interval '6 days' AND last_error IS NULL").fetch_one(&control).await.unwrap();assert_eq!(renewed,2);
        assert_eq!(sqlx::query_scalar::<_,i64>("SELECT count(*) FROM google_watch_channels").fetch_one(&tenant).await.unwrap(),0);
        control_app.close().await;control.close().await;tenant.close().await;
        let mut admin=PgConnection::connect(&base).await.unwrap();for db in [control_db,tenant_db] {admin.execute(format!("DROP DATABASE {db} WITH (FORCE)").as_str()).await.unwrap();}
    }

    #[tokio::test]
    async fn mark_renewed_clears_error_and_bumps_expiry_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping mark_renewed_clears_error_and_bumps_expiry_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "gws_g12_mark_renewed";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let conn = recreate(&base, db).await;
        conn.close().await.ok();
        let pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();

        let owner = seed_user(&pool).await;
        let account_id = seed_account(&pool, owner).await;
        let now = Utc::now();
        let id = seed_channel(&pool, account_id, "calendar", "primary", now - chrono::Duration::hours(1)).await;
        mark_failed(&pool, account_id, id, "boom").await.unwrap();

        let new_expiry = now + chrono::Duration::days(7);
        mark_renewed(&pool, account_id, id, new_expiry, Some("new-chan"), Some("new-res"), Some("fresh-token"))
            .await
            .unwrap();

        let row: (DateTime<Utc>, Option<String>, String, String, String) = sqlx::query_as(
            "SELECT expires_at, last_error, channel_id, channel_resource_id, channel_token FROM google_watch_channels WHERE id = $1",
        )
        .bind(id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(row.0.timestamp(), new_expiry.timestamp());
        assert!(row.1.is_none(), "last_error must be cleared on successful renewal");
        assert_eq!(row.2, "new-chan");
        assert_eq!(row.3, "new-res");
        assert_eq!(row.4, "fresh-token", "the renewal's fresh token must be persisted with the new channel");

        // A renewal that supplies no token (Gmail/Chat) must leave it alone.
        mark_renewed(&pool, account_id, id, new_expiry, None, None, None).await.unwrap();
        let kept: String = sqlx::query_scalar("SELECT channel_token FROM google_watch_channels WHERE id = $1")
            .bind(id)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(kept, "fresh-token");
        mark_failed_if_same(&pool,account_id,id,Some("old-chan"),"stale renewal").await.unwrap();
        let err:Option<String>=sqlx::query_scalar("SELECT last_error FROM google_watch_channels WHERE id=$1").bind(id).fetch_one(&pool).await.unwrap();
        assert!(err.is_none(),"a late old-channel error cannot poison the replacement watch");

        pool.close().await;
        let mut admin_conn = PgConnection::connect(&base).await.unwrap();
        admin_conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
    }

    #[tokio::test]
    async fn tick_renew_consumer_is_idle_when_nothing_is_due_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping tick_renew_consumer_is_idle_when_nothing_is_due_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "gws_g12_tick_idle";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let conn = recreate(&base, db).await;
        conn.close().await.ok();
        let pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();

        let store = Arc::new(crate::store::PostgresStore::new(pool.clone()));
        let t = WorkerTenant {
            tenant: TenantId::Consumer,
            pool: pool.clone(),
            store,
        };
        let http = reqwest::Client::new();
        let tick = tick_renew(&t, &pool, &http).await.unwrap();
        assert_eq!(tick.processed, 0, "no rows in the table — must report idle, not worked");

        pool.close().await;
        let mut admin_conn = PgConnection::connect(&base).await.unwrap();
        admin_conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
    }

    #[tokio::test]
    async fn lapsed_channels_finds_only_already_expired_rows_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping lapsed_channels_finds_only_already_expired_rows_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "gws_g12_lapsed";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let conn = recreate(&base, db).await;
        conn.close().await.ok();
        let pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();

        let owner = seed_user(&pool).await;
        let account_id = seed_account(&pool, owner).await;
        let now = Utc::now();
        let lapsed_id =
            seed_channel(&pool, account_id, "chat", "subscriptions/abc", now - chrono::Duration::hours(2)).await;
        let _future = seed_channel(&pool, account_id, "chat", "subscriptions/def", now + chrono::Duration::days(3)).await;

        let lapsed = lapsed_channels(&pool, &pool, now, None).await.unwrap();
        assert_eq!(lapsed.len(), 1);
        assert_eq!(lapsed[0].id, lapsed_id);

        pool.close().await;
        let mut admin_conn = PgConnection::connect(&base).await.unwrap();
        admin_conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
    }

    #[test]
    fn calendar_watch_body_carries_the_token_and_minted_tokens_are_nonempty_and_fresh() {
        let t1 = crate::connectors::google_calendar::generate_channel_token();
        let t2 = crate::connectors::google_calendar::generate_channel_token();
        assert!(!t1.is_empty() && !t2.is_empty());
        assert_ne!(t1, t2, "a fresh token per renewal");
        let body = calendar_watch_body("chan", "https://x/hook", &t1);
        assert_eq!(body["token"], serde_json::json!(t1));
        assert_eq!(body["id"], "chan");
        assert_eq!(body["address"], "https://x/hook");
        assert_eq!(body["type"], "web_hook");
    }

    #[test]
    fn retry_ready_backs_off_after_a_failure_but_never_delays_a_clean_row() {
        let now = Utc::now();
        assert!(retry_ready(None, now), "never-failed rows are due immediately");
        assert!(!retry_ready(Some(now - chrono::Duration::seconds(1)), now), "just failed => wait");
        assert!(!retry_ready(Some(now - chrono::Duration::minutes(RETRY_BACKOFF_MINUTES - 1)), now));
        assert!(retry_ready(Some(now - chrono::Duration::minutes(RETRY_BACKOFF_MINUTES)), now));
    }

    /// A failed renewal leaves `expires_at` in the past, so before the backoff
    /// it was due again on the very next sweep. Now the second sweep skips it.
    #[tokio::test]
    async fn failed_renewal_is_not_immediately_due_again_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping failed_renewal_is_not_immediately_due_again_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "gcal_renew_backoff";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        recreate(&base, db).await.close().await.ok();
        let pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();
        let owner = seed_user(&pool).await;
        let account_id = seed_account(&pool, owner).await;
        // A chat row with no owner always fails renewal (data-integrity error).
        let id = seed_channel(&pool, account_id, "chat", "sub", Utc::now() - chrono::Duration::hours(1)).await;
        let dek = crate::crypto::DekPlaneSource::for_worker_tenant(&TenantId::Consumer, &pool, &pool);
        let http = reqwest::Client::new();

        let first = renew_due(&pool, &pool, &http, &dek, None).await.unwrap();
        assert_eq!(first.processed, 1, "a due, never-failed row is attempted");
        let second = renew_due(&pool, &pool, &http, &dek, None).await.unwrap();
        assert_eq!(second.processed, 0, "just-failed row must back off, not be retried immediately");

        // Once the backoff window has passed it is attempted again.
        sqlx::query("UPDATE google_watch_channels SET last_error_at = now() - interval '16 minutes' WHERE id = $1")
            .bind(id)
            .execute(&pool)
            .await
            .unwrap();
        let third = renew_due(&pool, &pool, &http, &dek, None).await.unwrap();
        assert_eq!(third.processed, 1);

        pool.close().await;
        let mut admin_conn = PgConnection::connect(&base).await.unwrap();
        admin_conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
    }

    // ── Google Calendar org lift: org-member Calendar rows are control-plane ──

    #[test]
    fn calendar_rows_with_an_owner_are_control_plane_like_gmail_and_ownerless_ones_are_not() {
        let member = Uuid::new_v4();
        assert!(is_control_plane_row("gmail", Some(member)));
        assert!(is_control_plane_row("calendar", Some(member)), "an org member's channel routes via control");
        assert!(!is_control_plane_row("calendar", None), "the personal account-wide channel is unchanged");
        assert!(is_control_plane_row("chat", Some(member)));
        assert!(is_control_plane_row("meet", Some(member)));
    }

    /// Org tenant discovery spans two physical databases: an org MEMBER's
    /// Calendar row lives on CONTROL (so the push webhook's control-only
    /// resolver can route it), a legacy ownerless Calendar row stays on the
    /// tenant pool, and each is returned exactly once with its owner intact.
    #[tokio::test]
    async fn channels_due_for_org_account_finds_a_members_calendar_row_on_control_exactly_once_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping channels_due_for_org_account_finds_a_members_calendar_row_on_control_exactly_once_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let control_db = "gcal_org_renew_ctl";
        let _cleanup_control_db = crate::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", control_db);
        let tenant_db = "gcal_org_renew_ten";
        let _cleanup_tenant_db = crate::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", tenant_db);
        let _database_cleanup_5 = crate::test_db::TestDatabase::adopt_from_url(&base, control_db);
        recreate(&base, control_db).await.close().await.ok();
        let _database_cleanup_6 = crate::test_db::TestDatabase::adopt_from_url(&base, tenant_db);
        recreate(&base, tenant_db).await.close().await.ok();
        let control: PgPool = PgPoolOptions::new().connect(&with_db(&base, control_db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&control).await.unwrap();
        let tenant: PgPool = PgPoolOptions::new().connect(&with_db(&base, tenant_db)).await.unwrap();
        sqlx::migrate!("./migrations_tenant").run(&tenant).await.unwrap();
        sqlx::query("INSERT INTO tenant_meta (id, org_id) VALUES (true, 'org_gcal_renew')")
            .execute(&tenant)
            .await
            .unwrap();

        let owner = seed_user(&control).await;
        let account_id = seed_account(&control, owner).await;
        let member = seed_user(&control).await;
        let now = Utc::now();
        let soon = now + chrono::Duration::hours(1);
        let member_row: Uuid = sqlx::query_scalar(
            "INSERT INTO google_watch_channels (account_id, surface, resource_id, expires_at, owner_user_id) \
             VALUES ($1, 'calendar', $2, $3, $4) RETURNING id",
        )
        .bind(account_id)
        .bind(format!("primary:{member}"))
        .bind(soon)
        .bind(member)
        .fetch_one(&control)
        .await
        .unwrap();
        let mut workspace_rows = Vec::new();
        for surface in ["chat","meet"] {
            let id: Uuid = sqlx::query_scalar("INSERT INTO google_watch_channels(account_id,surface,resource_id,owner_user_id,expires_at) VALUES($1,$2,$3,$4,$5) RETURNING id")
                .bind(account_id).bind(surface).bind(format!("sub-{surface}")).bind(member).bind(soon).fetch_one(&control).await.unwrap();
            workspace_rows.push(id);
            sqlx::query("INSERT INTO google_watch_channels(account_id,surface,resource_id,owner_user_id,expires_at) VALUES($1,$2,$3,$4,$5)")
                .bind(account_id).bind(surface).bind(format!("stray-{surface}")).bind(member).bind(soon).execute(&tenant).await.unwrap();
        }
        // A legacy ownerless Calendar row on the tenant pool.
        let legacy_row: Uuid = sqlx::query_scalar(
            "INSERT INTO google_watch_channels (account_id, surface, resource_id, expires_at) \
             VALUES ($1, 'calendar', 'primary', $2) RETURNING id",
        )
        .bind(account_id)
        .bind(soon)
        .fetch_one(&tenant)
        .await
        .unwrap();

        let due = channels_due(&tenant, &control, now + chrono::Duration::hours(24), Some(account_id))
            .await
            .unwrap();
        let mut ids: Vec<Uuid> = due.iter().map(|c| c.id).collect();
        ids.sort();
        let mut want = vec![member_row, legacy_row];
        want.extend(workspace_rows);
        want.sort();
        assert_eq!(ids, want, "each row exactly once, from its own plane");
        let member_found = due.iter().find(|c| c.id == member_row).unwrap();
        assert_eq!(member_found.owner_user_id, Some(member));
        assert!(std::ptr::eq(
            watch_pool_for(member_found, &tenant, &control),
            &control
        ), "writes for a member's row go to control");

        tenant.close().await;
        control.close().await;
        let mut admin_conn = PgConnection::connect(&base).await.unwrap();
        for db in [control_db, tenant_db] {
            admin_conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
        }
    }
}
