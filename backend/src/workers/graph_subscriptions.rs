//! Microsoft Graph subscription renewal + backstop resync worker (CS-117 /
//! ADR-0008).
//!
//! Three responsibilities:
//!
//! * **Renewal** — Graph mail-message subscriptions live < 7 days and Teams
//!   message subscriptions only ~50 minutes, so the subscriptions backing live
//!   auto-capture must be renewed before they lapse. Every [`POLL_INTERVAL`]
//!   we `PATCH` any expiring within [`RENEW_WINDOW_HOURS`]. A subscription
//!   Graph has forgotten — or one whose account disconnected — has its row
//!   dropped.
//!
//! * **Recreation** — renewal only *extends* existing subscriptions; it never
//!   creates one. Mail recreates lazily on the next thread ingest, but a watched
//!   Teams channel has no such trigger, so a dropped Teams subscription would
//!   never come back. Every [`POLL_INTERVAL`] we therefore ensure a live
//!   subscription exists for each watched Teams resource, (re)creating any
//!   that are missing.
//!
//! * **Backstop resync** — change notifications can be missed (subscription
//!   gaps, delivery pauses, downtime). Microsoft's guidance is to pair webhooks
//!   with a polling backstop. Every [`BACKSTOP_INTERVAL`] we re-list each
//!   watched conversation (`$filter=conversationId eq …`) and ingest anything
//!   new. This is *tighter* than a folder-wide delta query: it only ever
//!   touches threads the user opted into. Dedup (`ON CONFLICT DO NOTHING`)
//!   makes it a no-op when nothing changed. The same resync runs on demand
//!   from `missed`/`subscriptionRemoved` lifecycle events (see
//!   `routes::graph_webhook`).
//!
//!   **Known limit — no missed-deletion sweep (m365 Train 4 PR 4.4).**
//!   Subscriptions now request `created,updated,deleted` (`connectors::
//!   microsoft::subscription_change_types`), and a live `deleted`
//!   notification propagates through `routes::graph_webhook::process_
//!   deleted_notification` → `services::deletion::delete_source`. This
//!   backstop, however, does NOT detect a deletion the webhook missed during
//!   a gap — it only ever re-lists what currently EXISTS (`fetch_conversation`/
//!   `fetch_channel_messages`/`fetch_chat_messages`), so a message that
//!   disappeared during a downtime window is invisible to it: there is
//!   nothing to "ingest" for something that is now gone. Detecting that
//!   would need either (a) a real Graph delta query
//!   (`/messages/delta`/equivalent, which reports `@removed` markers
//!   directly) with a persisted per-mailbox/per-container delta token, or
//!   (b) a locally-maintained ledger of every message id ever ingested per
//!   watched conversation/channel to diff against each re-list. Neither
//!   exists today: `outlook_watched_conversations`/`teams_watched_resources`
//!   carry a pagination cursor (m365 Train 2 PR 2.6) but not a delta token,
//!   and `atoms` itself carries no per-conversation/per-container grouping
//!   column to diff against (`NormalizedItem.thread_id` is deliberately not
//!   persisted — see `routes::graph_webhook::ingest_message_with`'s
//!   `GraphIngestTarget::Enqueue` arm). Either fix is a schema change (a new
//!   delta-token/ledger column), which is out of this PR's scope per its own
//!   "migrations: likely none — STOP and report if you conclude one is
//!   needed" rule. Accepted, narrow residual: a deletion that the webhook
//!   missed during a downtime window has NO other path to be corrected —
//!   this backstop only ever ADDS (re-lists current messages and ingests
//!   anything new via dedup), it never REMOVES on the strength of an atom's
//!   absence from a re-list, so nothing here notices later either. The
//!   message stays indexed until a future delta-token-backed sweep is
//!   built. This is the mirror-image twin of ADR-0008's own already-
//!   accepted backstop gap for CREATES: that gap is closed by THIS backstop
//!   (a missed create is caught on the next re-list); this one is not
//!   closed by anything yet — same downtime-window shape, opposite
//!   direction, asymmetric coverage.
//!
//! # Tenant loop (m365 Train 2 PR 2.2)
//!
//! This was the last consumer-only straggler on `workers::tenant_harness`'s
//! decision table (`huddle` moved earlier in the Slack train; see that
//! module's doc). Renewal and recreation have no dependency on the
//! content-ingest funnel, so both join the tenant loop as [`tick_renew`] —
//! one `HarnessConfig::sweep_worker` registration at [`POLL_INTERVAL`],
//! mirroring the old loop's pairing of the two passes. Backstop resync joins
//! as the separate [`tick_backstop`], registered at [`BACKSTOP_INTERVAL`] —
//! a second registration because `HarnessConfig` carries exactly one native
//! cadence and the two passes always ran on different clocks.
//!
//! **Two-pool split.** `graph_subscriptions`, `teams_watched_resources` and
//! `outlook_watched_conversations` read/write on `t.pool` (the tick's own
//! content pool — the consumer pool on the consumer entry, an org's own
//! tenant pool on an org entry); the Microsoft OAuth token (`oauth_tokens`,
//! control-only, same call as `workers::slack_seat_reconcile`/`workers::
//! huddle` make for the Slack bot token) always reads/writes on
//! `control_pool`. On the consumer entry these are the SAME physical pool —
//! passed twice, not duplicated — so behavior there is byte-identical to the
//! pre-conversion single loop. See `connectors::microsoft`'s own RLS-scoping
//! note for why every content query underneath now opens a
//! `tenancy::scope::begin_tenant_system_scoped` transaction rather than the
//! plain `begin_account_scoped` this worker's helpers used before.
//!
//! **Account enumeration.** The consumer plane keeps sweeping every account
//! that has a due subscription / watched Teams resource, via the existing
//! cross-account `SECURITY DEFINER` resolvers in `connectors::microsoft`
//! (`expiring_subscriptions`, `accounts_with_watched_conversations`,
//! `accounts_with_teams_watched`) — unchanged. An org tenant database holds
//! exactly one account (ADR-0007), so there is no discovery problem to solve
//! there: [`resolve_tenant_account`] reads it once from the control-plane
//! `tenant_databases` row (the same "resolve the org's account_id from
//! control before working on tenant" pattern `workers::slack_seat_reconcile`
//! and `workers::member_ocean_reconciler` use), and the org-tenant tick
//! functions call the `_for_account` siblings of the cross-account resolvers
//! (`connectors::microsoft::expiring_subscriptions_for_account`, etc.),
//! which have no `0086` dependency at all — that migration is control-plane
//! only.
//!
//! **Backstop resync now runs on org tenants too (m365 Train 2 PR 2.3).**
//! [`resync_account`]'s content-ingest step routes through
//! `routes::graph_webhook::ingest_message`/`ingest_container_messages`, which
//! are now plane-aware: a `Personal` account still ingests synchronously,
//! in-process; an `Org` account is handed off through
//! `JobKind::GraphMessageIngest` onto whichever plane's own `jobs` table its
//! tenant-routing status resolves to (see `routes::graph_webhook::
//! resolve_graph_ingest_target` and `workers::graph_ingest`'s own module doc
//! for the full design). [`tick_backstop`]'s `TenantId::Org` arm therefore no
//! longer no-ops — it resolves the tenant's singleton `account_id` (same
//! `resolve_tenant_account` [`tick_renew`] already uses) and calls
//! [`resync_account`] exactly like the consumer arm does. In practice this
//! finds nothing to resync until PR 2.4 makes `teams_watched_resources`/
//! `outlook_watched_conversations`' OWN install-time resolution plane-aware
//! (both tables are still control-plane-only writes today, regardless of
//! plane) — but the tick itself is no longer structurally blocked, and will
//! "just work" the moment PR 2.4 starts populating org-tenant watched rows.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use chrono::{Duration as ChronoDuration, Utc};
use sqlx::PgPool;
use uuid::Uuid;

use crate::connectors::{microsoft, ConnectorError, MicrosoftConnector, TokenOwner};
use crate::routes::{
    connectors::load_creds_bg,
    graph_webhook::{ingest_container_messages, ingest_message_stored},
    oauth::store_token_bundle,
    workflows,
};
use crate::tenancy::TenantId;
use crate::workers::tenant_harness::{Tick, WorkerTenant};
use crate::AppState;

/// How often to scan for subscriptions nearing expiry, and to ensure watched
/// Teams resources still have a live subscription. Fed to the tenant harness
/// as [`tick_renew`]'s sweep cadence.
pub const POLL_INTERVAL: Duration = Duration::from_secs(30 * 60);
/// Renew anything expiring within this window. Comfortably larger than
/// `POLL_INTERVAL` so a subscription is always renewed at least once before it
/// lapses, even if a pass is skipped.
const RENEW_WINDOW_HOURS: i64 = 6;
/// How often to run the backstop resync of all watched threads. Fed to the
/// tenant harness as [`tick_backstop`]'s sweep cadence.
pub const BACKSTOP_INTERVAL: Duration = Duration::from_secs(6 * 60 * 60);
/// How often to resume any backfill a previous pass left incomplete
/// (`teams_watched_resources`/`outlook_watched_conversations`/`outlook_
/// workflows` rows with `backfill_complete = FALSE`). Fed to the tenant
/// harness as [`tick_backfill_continue`]'s sweep cadence — m365 Train 2 PR
/// 2.6. Tighter than [`BACKSTOP_INTERVAL`]: an incomplete backfill is
/// user-visible ("why is my history still missing?"), so it should converge
/// faster than the "keep current" backstop needs to run.
#[cfg(not(feature = "e2e"))]
pub const BACKFILL_CONTINUE_INTERVAL: Duration = Duration::from_secs(5 * 60);
#[cfg(feature = "e2e")]
pub const BACKFILL_CONTINUE_INTERVAL: Duration = Duration::from_secs(5);
/// How often to re-list Teams containers whose doorbell is `state = 'polling'`.
/// Live doorbells stay on [`BACKSTOP_INTERVAL`]. Mail is not in this tick.
pub const DOORBELL_POLLING_INTERVAL: Duration = Duration::from_secs(45 * 60);

// ── Account resolution (org tenant) ───────────────────────────────────────────

/// The single account an org tenant database belongs to, resolved from the
/// control-plane routing row. Mirrors `workers::slack_seat_reconcile::
/// accounts_to_sweep`'s `TenantId::Org` arm and `workers::
/// member_ocean_reconciler::tick`'s same pattern: "resolve the org's
/// account_id from control before working on tenant". Defensive-only in
/// practice — the harness never schedules a tenant without an active routing
/// row — but a control-plane read racing a tenant's offboarding is a real,
/// if rare, timing window worth failing closed on rather than panicking.
async fn resolve_tenant_account(control_pool: &PgPool, org_id: &str) -> Result<Uuid, String> {
    sqlx::query_scalar::<_, Uuid>("SELECT account_id FROM tenant_databases WHERE clerk_org_id = $1")
        .bind(org_id)
        .fetch_optional(control_pool)
        .await
        .map_err(|e| e.to_string())?
        .ok_or_else(|| format!("no tenant_databases row for org {org_id}"))
}

// ── Renewal + recreation tick ─────────────────────────────────────────────────

/// One renewal + ensure/recreate pass on one tenant's `(pool, store)` pair.
/// Driven by `workers::tenant_harness::run_multi_tenant` as a sweep worker
/// (`HarnessConfig::sweep_worker(_, POLL_INTERVAL)`), which owns the loop,
/// the sweep cadence, per-tenant error backoff, and shutdown — see that
/// module's doc for the full contract. `control_pool` is the shared
/// control/consumer pool, needed only for the Microsoft OAuth token
/// (`oauth_tokens`, control-only); every subscription/watched-resource
/// read/write runs on `t.pool`.
pub async fn tick_renew(
    t: &WorkerTenant,
    control_pool: &PgPool,
    http: &reqwest::Client,
) -> Result<Tick, String> {
    // The plane this tick's accounts keep their DEKs on, resolved ONCE from
    // the tenant the harness already picked — see `crypto::DekPlaneSource`.
    // This chain holds no `AppState`, so it reuses the harness's decision
    // rather than re-deriving one per account.
    let dek = crate::crypto::DekPlaneSource::for_worker_tenant(&t.tenant, &t.pool, control_pool);
    match &t.tenant {
        TenantId::Consumer => consumer_tick_renew(&t.pool, control_pool, http, &dek).await,
        TenantId::Org(org_id) => {
            let account_id = resolve_tenant_account(control_pool, org_id).await?;
            org_tick_renew(&t.pool, control_pool, http, &dek, account_id).await
        }
    }
}

async fn consumer_tick_renew(
    pool: &PgPool,
    control_pool: &PgPool,
    http: &reqwest::Client,
    dek: &crate::crypto::DekPlaneSource,
) -> Result<Tick, String> {
    let mut touched = 0usize;
    match renew_due(pool, control_pool, http, dek).await {
        Ok(n) => touched += n,
        Err(e) => tracing::warn!(error = %e, "graph subscription renewal pass failed"),
    }
    match ensure_teams_subscriptions(pool, control_pool, http, dek).await {
        Ok(n) => touched += n,
        Err(e) => tracing::warn!(error = %e, "ensure teams subscriptions pass failed"),
    }
    Ok(if touched > 0 {
        Tick::worked(touched)
    } else {
        Tick::idle()
    })
}

async fn org_tick_renew(
    pool: &PgPool,
    control_pool: &PgPool,
    http: &reqwest::Client,
    dek: &crate::crypto::DekPlaneSource,
    account_id: Uuid,
) -> Result<Tick, String> {
    let mut touched = 0usize;
    match renew_due_for_account(pool, control_pool, http, dek, account_id).await {
        Ok(n) => touched += n,
        Err(e) => tracing::warn!(error = %e, %account_id, "graph subscription renewal pass failed"),
    }
    touched += ensure_teams_subscriptions_for_account(pool, control_pool, http, dek, account_id).await;
    // Keep the control-plane mail-notification routes (what the webhook uses to
    // find this org's tenant DB from a bare subscription id) in step with the
    // tenant's subscription rows: covers subscriptions created before routes
    // existed, and ids rotated by `subscriptionRemoved` / renewal-drop.
    if let Err(e) = microsoft::sync_mail_routes(pool, control_pool, account_id).await {
        tracing::warn!(error = %e, %account_id, "graph mail route sync failed");
    }
    Ok(if touched > 0 {
        Tick::worked(touched)
    } else {
        Tick::idle()
    })
}

// ── Renewal ───────────────────────────────────────────────────────────────────

/// Consumer-plane renewal pass: cross-account discovery, then one scoped
/// renewal per due subscription. Returns how many subscriptions were due
/// (regardless of individual outcome — each failure is logged and skipped by
/// [`renew_one`], never aborting the rest).
async fn renew_due(
    pool: &PgPool,
    control_pool: &PgPool,
    http: &reqwest::Client,
    dek: &crate::crypto::DekPlaneSource,
) -> Result<usize, ConnectorError> {
    let cutoff = Utc::now() + ChronoDuration::hours(RENEW_WINDOW_HOURS);
    let due = microsoft::expiring_subscriptions(pool, cutoff).await?;
    if due.is_empty() {
        return Ok(0);
    }
    tracing::info!(count = due.len(), "renewing graph subscriptions");
    let n = due.len();
    for (subscription_id, account_id, resource, tenant_id, owner_user_id) in due {
        if let Err(e) = renew_one(
            pool,
            control_pool,
            http,
            dek,
            &subscription_id,
            account_id,
            &resource,
            &tenant_id,
            owner_user_id,
        )
        .await
        {
            tracing::warn!(error = %e, %subscription_id, "subscription renewal failed");
        }
    }
    Ok(n)
}

/// The org-tenant analog of [`renew_due`]: no cross-account discovery, since
/// `account_id` is already known.
async fn renew_due_for_account(
    pool: &PgPool,
    control_pool: &PgPool,
    http: &reqwest::Client,
    dek: &crate::crypto::DekPlaneSource,
    account_id: Uuid,
) -> Result<usize, ConnectorError> {
    let cutoff = Utc::now() + ChronoDuration::hours(RENEW_WINDOW_HOURS);
    let due = microsoft::expiring_subscriptions_for_account(pool, account_id, cutoff).await?;
    if due.is_empty() {
        return Ok(0);
    }
    tracing::info!(count = due.len(), %account_id, "renewing graph subscriptions");
    let n = due.len();
    for (subscription_id, resource, tenant_id, owner_user_id) in due {
        if let Err(e) = renew_one(
            pool,
            control_pool,
            http,
            dek,
            &subscription_id,
            account_id,
            &resource,
            &tenant_id,
            owner_user_id,
        )
        .await
        {
            tracing::warn!(error = %e, %subscription_id, %account_id, "subscription renewal failed");
        }
    }
    Ok(n)
}

/// Renew (or drop) one subscription. `account_id` is always already known by
/// the caller (both `renew_due` variants resolve it before calling this), so
/// this uses the `_for_account` write helpers directly rather than
/// `connectors::microsoft`'s control-plane-only account re-derivation.
///
/// `owner_user_id` (m365 enterprise train PR 3.3, migration `0190`/`0068`):
/// straight off the `graph_subscriptions` row this subscription came from —
/// `None` for account-wide (Personal mail, or any Teams/transcript
/// subscription), `Some(uid)` for an org member's own delegated mail
/// subscription. Selects which member's token [`renew_one_inner`] loads for
/// a delegated (mail) renewal; app-only (RSC) renewals ignore it entirely
/// (they mint a fresh client-credentials token per tenant, never a stored
/// user token).
#[allow(clippy::too_many_arguments)]
async fn renew_one(
    pool: &PgPool,
    control_pool: &PgPool,
    http: &reqwest::Client,
    dek: &crate::crypto::DekPlaneSource,
    subscription_id: &str,
    account_id: Uuid,
    resource: &str,
    tenant_id: &str,
    owner_user_id: Option<Uuid>,
) -> Result<(), ConnectorError> {
    // Keyed via `rate_key` (CodeRabbit review, PR #673) — see
    // `graph_rate_limit`'s module doc and `rate_key`'s own doc. `tenant_id`
    // is already the exact "empty for delegated, non-empty for app-only"
    // signal `rate_key` expects (`renew_one_inner`'s own `!tenant_id.is_
    // empty()` branch for token choice relies on the same fact).
    crate::graph_rate_limit::scoped(
        crate::graph_rate_limit::rate_key(account_id, tenant_id),
        renew_one_inner(
            pool,
            control_pool,
            http,
            dek,
            subscription_id,
            account_id,
            resource,
            tenant_id,
            owner_user_id,
        ),
    )
    .await
}

/// Rate-limit scope for `m365 Train 2 PR 2.6` wraps this at the [`renew_one`]
/// call boundary — see `graph_rate_limit`'s module doc.
#[allow(clippy::too_many_arguments)]
async fn renew_one_inner(
    pool: &PgPool,
    control_pool: &PgPool,
    http: &reqwest::Client,
    dek: &crate::crypto::DekPlaneSource,
    subscription_id: &str,
    account_id: Uuid,
    resource: &str,
    tenant_id: &str,
    owner_user_id: Option<Uuid>,
) -> Result<(), ConnectorError> {
    // App-mode (RSC) subscriptions renew with an app token for the tenant;
    // delegated ones with the OWNING member's token (m365 enterprise train PR
    // 3.3 — previously always `TokenOwner::AccountWide`, which silently
    // renewed the wrong subscription's mailbox, or none at all, once a second
    // org member's own delegated mail subscription could exist).
    let owner = owner_user_id
        .map(TokenOwner::Member)
        .unwrap_or(TokenOwner::AccountWide);
    // Per-user transcript subscriptions live on the control plane so the
    // webhook can find them. An org member's row is renewed by the org
    // calendar tick (that tenant's DEK). The consumer sweep must not drop
    // it when it cannot decrypt the org key. A recorded kill switch is not
    // retried.
    if crate::connectors::teams_transcripts::is_delegated_organiser_transcript_resource(resource) {
        if crate::services::connector_health::transcript_access_disabled(
            control_pool,
            account_id,
            owner_user_id,
        )
        .await
        {
            return Ok(());
        }
        if owner_user_id.is_some()
            && matches!(
                crate::routes::slack::account_plane(control_pool, account_id).await,
                Ok(crate::routes::slack::AccountPlane::Org)
            )
        {
            return Ok(());
        }
    }
    // App-only rows (non-empty tenant id) are the RSC watches and the
    // tenant-wide transcript subscription. After the grace they are not
    // renewed. Delegated rows, including item 51's organiser transcript,
    // store an empty tenant id and fall through.
    if !tenant_id.is_empty()
        && !crate::services::org_install_grace::org_install_capture_open(control_pool).await
    {
        tracing::info!(
            %subscription_id,
            "org-install grace closed; not renewing app-only graph subscription"
        );
        return Ok(());
    }
    // A delegated subscription whose owner has LEFT the account is not
    // renewed with their token (settings launch PR 2.8): the gate offboards
    // their capture, which deletes this subscription upstream and locally.
    if let (Some(owner), true) = (owner_user_id, tenant_id.is_empty()) {
        if !crate::connectors::member_offboarding::admit_capture_or_offboard(
            http,
            control_pool,
            Some(&dek.plane_for(account_id)),
            account_id,
            owner,
            "graph_subscription_renewal",
        )
        .await
        {
            return Ok(());
        }
    }
    let token = if !tenant_id.is_empty() {
        microsoft::app_token(http, tenant_id).await?
    } else {
        // `control_pool` owns the `oauth_tokens` ROW; `dek` owns the KEY,
        // which for an org account lives in the org's own tenant
        // `account_keys` — a different database.
        let dek_plane = dek.plane_for(account_id);
        let Some(mut creds) =
            load_creds_bg(control_pool, &dek_plane, account_id, "microsoft", owner).await?
        else {
            tracing::info!(%account_id, "no microsoft connection; dropping stale subscription");
            microsoft::delete_subscription_row_for_account(pool, account_id, subscription_id)
                .await?;
            return Ok(());
        };
        let connector = MicrosoftConnector;
        connector.refresh_if_needed(http, &mut creds).await?;
        if let Some(refreshed) = creds.refreshed.as_ref() {
            if let Err(_resp) =
                store_token_bundle(control_pool, &dek_plane, account_id, "microsoft", owner, refreshed).await
            {
                tracing::warn!(%account_id, "failed to persist refreshed microsoft token during renewal");
            }
        }
        creds.access_token
    };

    match microsoft::renew_subscription(http, &token, subscription_id, resource).await {
        Ok(new_expiry) => {
            microsoft::set_subscription_expiry_for_account(
                pool,
                account_id,
                subscription_id,
                new_expiry,
            )
            .await?;
            tracing::info!(%subscription_id, %new_expiry, "graph subscription renewed");
        }
        Err(e) if microsoft::graph_access_to_transcripts_disabled(&e)
            && crate::connectors::teams_transcripts::is_delegated_organiser_transcript_resource(
                resource,
            ) =>
        {
            tracing::info!(
                %subscription_id,
                %account_id,
                "transcript kill switch — skipping this member, leaving other watches"
            );
            let _ = crate::services::connector_health::note_transcript_access_disabled(
                control_pool,
                account_id,
                owner_user_id,
            )
            .await;
        }
        Err(e) => {
            // Graph no longer knows this subscription (expired/deleted). Drop the
            // row; the next thread ingest will create a fresh one.
            tracing::warn!(error = %e, %subscription_id, "renew rejected; dropping subscription row");
            microsoft::delete_subscription_row_for_account(pool, account_id, subscription_id)
                .await?;
        }
    }
    Ok(())
}

// ── Teams subscription (re)creation ───────────────────────────────────────────

/// Ensure a live change-notification subscription exists for every watched Teams
/// resource across every account with one, (re)creating any that are missing or
/// lapsed. Consumer-plane cross-account sweep; see the org-tenant analog
/// [`ensure_teams_subscriptions_for_account`], called directly (no discovery
/// hop) by [`org_tick_renew`].
///
/// The renewal pass only *renews* subscriptions already in `graph_subscriptions`
/// and drops ones Graph has forgotten — it never creates a fresh one. Teams
/// subscriptions live only ~50 minutes, and the install-time create
/// (`routes::teams_ingest::ensure_live_subscriptions`) runs just once. So a
/// channel whose subscription lapsed — a failed renewal, a redeploy, downtime —
/// would otherwise never regain live capture and silently degrade to the 6-hour
/// backstop. Running this every pass closes that gap.
///
/// Idempotent: `ensure_subscription_for` no-ops when a healthy subscription
/// already exists, and renewal (which runs first each pass) keeps live ones well
/// clear of the refresh window — so in steady state this only ever creates
/// genuinely-missing subscriptions. Returns how many resources were actually
/// (re)created or attempted (skips resources already current).
async fn ensure_teams_subscriptions(
    pool: &PgPool,
    control_pool: &PgPool,
    http: &reqwest::Client,
    dek: &crate::crypto::DekPlaneSource,
) -> Result<usize, ConnectorError> {
    let mut touched = 0usize;
    for account_id in microsoft::accounts_with_teams_watched(pool).await? {
        touched +=
            ensure_teams_subscriptions_for_account(pool, control_pool, http, dek, account_id).await;
    }
    Ok(touched)
}

/// Best-effort per account: one bad resource is logged and skipped so it can't
/// stall the rest. Shared by the consumer cross-account sweep
/// ([`ensure_teams_subscriptions`]) and the org-tenant tick
/// ([`org_tick_renew`]).
async fn ensure_teams_subscriptions_for_account(
    pool: &PgPool,
    control_pool: &PgPool,
    http: &reqwest::Client,
    dek: &crate::crypto::DekPlaneSource,
    account_id: Uuid,
) -> usize {
    // Account-level fallback scope only — the inner function's watched
    // resources can span more than one AAD tenant, so it opens its OWN
    // per-resource `rate_key`-keyed scope around each actual Graph call
    // (CodeRabbit review, PR #673); this outer one just gives anything
    // outside that loop a sane key rather than falling through to
    // `UNSCOPED_KEY`. Nesting is harmless — see `graph_rate_limit`'s module
    // doc and `resync_account`'s identical note.
    crate::graph_rate_limit::scoped(
        account_id.to_string(),
        ensure_teams_subscriptions_for_account_inner(pool, control_pool, http, dek, account_id),
    )
    .await
}

/// Rate-limit scope (m365 Train 2 PR 2.6) wraps this at the [`ensure_teams_
/// subscriptions_for_account`] call boundary — see `graph_rate_limit`'s
/// module doc.
async fn ensure_teams_subscriptions_for_account_inner(
    pool: &PgPool,
    control_pool: &PgPool,
    http: &reqwest::Client,
    dek: &crate::crypto::DekPlaneSource,
    account_id: Uuid,
) -> usize {
    let watched = match microsoft::teams_watched_for_account(pool, account_id).await {
        Ok(w) => w,
        Err(e) => {
            tracing::warn!(error = %e, %account_id, "ensure teams subs: watched list failed");
            return 0;
        }
    };

    let mut tokens = TeamsTokenCache::default();
    // The transcript subscription is app-scoped — one Graph resource string per
    // app — and `graph_subscriptions` is keyed on (account_id, resource), so only
    // one transcript subscription is representable per account. We therefore
    // maintain it for a single tenant (the first app-mode resource seen).
    // Capturing transcripts from more than one tenant on the same account would
    // need tenant_id in the subscription key (a schema change).
    let mut transcript_handled = false;
    // Phase G item 48: RSC app-mode watches and the tenant-wide transcript
    // subscription (created only from an app-mode iteration below) stop
    // once the connect-email grace closes. Delegated doorbells stay.
    let capture_open =
        crate::services::org_install_grace::org_install_capture_open(control_pool).await;
    let mut touched = 0usize;
    let plane = crate::routes::slack::account_plane(control_pool, account_id)
        .await
        .unwrap_or(crate::routes::slack::AccountPlane::Personal);

    for (kind, team_id, channel_id, chat_id, _graph_id, user_id, tenant_id, auth_mode) in watched {
        if crate::services::org_install_grace::rsc_app_capture_blocked(capture_open, &auth_mode) {
            continue;
        }
        let Some(resource) =
            microsoft::teams_subscription_resource(&kind, &team_id, &channel_id, &chat_id)
        else {
            continue;
        };
        // Delegated channel/chat watches share one control-plane doorbell
        // (S5a). App-mode (RSC) still uses a per-account `graph_subscriptions`
        // row. Mail is untouched. `polling` doorbells are re-listed by
        // [`tick_doorbell_polling`] (about every 45 minutes), not this
        // renewal pass and not the 6-hour backstop.
        let sub_owner = teams_watcher_token_owner(plane, user_id, &auth_mode);

        // Per-resource rate-limit scope, keyed via `rate_key` on THIS
        // resource's own `tenant_id` (CodeRabbit review, PR #673). One
        // account's watched Teams resources can span more than one AAD
        // tenant, so a single account-level scope for the whole function
        // (the pre-fix shape) would key an app-only call on `account_id` —
        // wrong per this module's "Per-tenant key" doc, and under-throttling
        // whenever two accounts share a tenant. Opening the scope per
        // iteration instead keeps every Graph call below correctly bucketed.
        touched += crate::graph_rate_limit::scoped(
            crate::graph_rate_limit::rate_key(account_id, &tenant_id),
            async {
                let mut iter_touched = 0usize;
                if auth_mode != "app" {
                    iter_touched += 1;
                    if let Err(e) = crate::connectors::teams_doorbell::ensure_delegated_container(
                        http,
                        control_pool,
                        dek,
                        &resource,
                        account_id,
                        user_id,
                    )
                    .await
                    {
                        tracing::info!(
                            error = %e, %account_id, %resource,
                            "teams doorbell unavailable (re-poll covers it)"
                        );
                    }
                } else if !microsoft::live_subscription_is_current(
                    pool,
                    account_id,
                    &resource,
                    sub_owner,
                )
                .await
                .unwrap_or(false)
                {
                    if let Some(token) = tokens
                        .token_for(
                            http,
                            control_pool,
                            dek,
                            account_id,
                            &tenant_id,
                            &auth_mode,
                            sub_owner,
                        )
                        .await
                    {
                        iter_touched += 1;
                        if let Err(e) = microsoft::ensure_subscription_for(
                            http,
                            pool,
                            account_id,
                            &token,
                            &resource,
                            &tenant_id,
                            sub_owner,
                        )
                        .await
                        {
                            // Private/shared channels return 403 here — the backstop
                            // covers them, so this is expected, not an error.
                            tracing::info!(error = %e, %account_id, %resource, "teams live subscription unavailable (re-poll covers it)");
                        }
                    }
                }

                // The meeting-transcript subscription lapses the same way; handle it
                // once per account (see `transcript_handled`).
                if auth_mode == "app" && !tenant_id.is_empty() && !transcript_handled {
                    transcript_handled = true;
                    if let Some(tres) =
                        crate::services::org_install_grace::tenant_wide_transcript_to_subscribe(
                            capture_open,
                            microsoft::transcript_subscription_resource(),
                        )
                    {
                        if !microsoft::live_subscription_is_current(
                            pool,
                            account_id,
                            &tres,
                            TokenOwner::AccountWide,
                        )
                        .await
                        .unwrap_or(false)
                        {
                            if let Some(token) = tokens
                                .token_for(
                                    http,
                                    control_pool,
                                    dek,
                                    account_id,
                                    &tenant_id,
                                    &auth_mode,
                                    TokenOwner::AccountWide,
                                )
                                .await
                            {
                                iter_touched += 1;
                                if let Err(e) = microsoft::ensure_subscription_for(
                                    http,
                                    pool,
                                    account_id,
                                    &token,
                                    &tres,
                                    &tenant_id,
                                    TokenOwner::AccountWide,
                                )
                                .await
                                {
                                    tracing::info!(error = %e, %account_id, "teams transcript subscription unavailable");
                                }
                            }
                        }
                    }
                }
                iter_touched
            },
        )
        .await;
    }
    touched
}

/// Load + refresh the account's delegated Microsoft token, persisting any
/// refresh. `None` when the account has no Microsoft connection or the refresh
/// failed (e.g. an RSC/app-only account that never did delegated OAuth).
/// `control_pool`-only: `oauth_tokens` is control-plane (see this module's own
/// doc comment).
///
/// S4 / item 47a: pass [`TokenOwner::Member`] for an org-plane Teams watcher
/// read, [`TokenOwner::AccountWide`] for personal-plane and for Outlook mail
/// paths that still use the account-wide row.
async fn delegated_microsoft_token(
    http: &reqwest::Client,
    control_pool: &PgPool,
    dek: &crate::crypto::DekPlaneSource,
    account_id: Uuid,
    owner: TokenOwner,
) -> Option<String> {
    // `control_pool` owns the `oauth_tokens` ROW; `dek` owns the KEY. See
    // `routes::connectors::load_creds_bg`'s doc for why they are separate.
    // S4: `owner` is Member(watcher) on the org plane, AccountWide on personal.
    let dek_plane = dek.plane_for(account_id);
    match load_creds_bg(control_pool, &dek_plane, account_id, "microsoft", owner).await {
        Ok(Some(mut creds)) => {
            let connector = MicrosoftConnector;
            if connector.refresh_if_needed(http, &mut creds).await.is_ok() {
                if let Some(refreshed) = creds.refreshed.as_ref() {
                    let _ = store_token_bundle(
                        control_pool,
                        &dek_plane,
                        account_id,
                        "microsoft",
                        owner,
                        refreshed,
                    )
                    .await;
                }
                Some(creds.access_token)
            } else {
                None
            }
        }
        _ => None,
    }
}

/// Per-account, per-pass token cache shared by the recreation pass. App-mode
/// (RSC) resources use a per-tenant app token; delegated resources use each
/// watcher's own token (S4). Caching means a tenant with many channels mints
/// its app token once, and each member's delegated token is loaded at most
/// once per pass.
#[derive(Default)]
struct TeamsTokenCache {
    app: HashMap<String, String>,
    /// Keyed by `TokenOwner::owner_user_id()` — `None` = AccountWide (personal
    /// plane). Value `None` = already looked up and absent.
    delegated: HashMap<Option<Uuid>, Option<String>>,
}

impl TeamsTokenCache {
    /// Token for one watched resource, or `None` when it can't be obtained (the
    /// caller should skip that resource).
    async fn token_for(
        &mut self,
        http: &reqwest::Client,
        control_pool: &PgPool,
        dek: &crate::crypto::DekPlaneSource,
        account_id: Uuid,
        tenant_id: &str,
        auth_mode: &str,
        owner: TokenOwner,
    ) -> Option<String> {
        if auth_mode == "app" && !tenant_id.is_empty() {
            if let Some(t) = self.app.get(tenant_id) {
                return Some(t.clone());
            }
            match microsoft::app_token(http, tenant_id).await {
                Ok(t) => {
                    self.app.insert(tenant_id.to_string(), t.clone());
                    Some(t)
                }
                Err(e) => {
                    tracing::warn!(error = %e, %account_id, "teams token: app token failed");
                    None
                }
            }
        } else {
            let key = owner.owner_user_id();
            if let Some(cached) = self.delegated.get(&key) {
                return cached.clone();
            }
            let loaded = delegated_microsoft_token(http, control_pool, dek, account_id, owner).await;
            self.delegated.insert(key, loaded.clone());
            loaded
        }
    }
}

/// Which Microsoft token reads an opted-in thread's mailbox in the backstop
/// resync: the opting-in member's own on the org plane, the account-wide
/// connection on the personal plane (mirrors `routes::connectors::
/// load_creds_owner_aware`'s plane split).
pub(crate) fn mail_backstop_token_owner(
    plane: crate::routes::slack::AccountPlane,
    ingested_by: Uuid,
) -> TokenOwner {
    match plane {
        crate::routes::slack::AccountPlane::Org => TokenOwner::Member(ingested_by),
        crate::routes::slack::AccountPlane::Personal => TokenOwner::AccountWide,
    }
}

/// TokenOwner for a Teams watcher row's delegated Graph calls (S4).
/// App-mode ignores this (uses the app-token branch); delegated: Member on
/// org, AccountWide on personal.
fn teams_watcher_token_owner(
    plane: crate::routes::slack::AccountPlane,
    user_id: Uuid,
    auth_mode: &str,
) -> TokenOwner {
    if auth_mode == "app" {
        TokenOwner::AccountWide
    } else {
        match plane {
            crate::routes::slack::AccountPlane::Org => TokenOwner::Member(user_id),
            crate::routes::slack::AccountPlane::Personal => TokenOwner::AccountWide,
        }
    }
}

// ── Backstop resync ─────────────────────────────────────────────────────────────

/// One backstop-resync pass. Driven by `workers::tenant_harness::
/// run_multi_tenant` as a sweep worker (`HarnessConfig::sweep_worker(_,
/// BACKSTOP_INTERVAL)`). `TenantId::Org` now runs the same resync
/// (m365 Train 2 PR 2.3) — see this module's own doc comment ("Backstop
/// resync now runs on org tenants too").
pub async fn tick_backstop(
    t: &WorkerTenant,
    control_pool: &PgPool,
    state: &Arc<AppState>,
) -> Result<Tick, String> {
    crate::connectors::catalog::everything::refresh(t, control_pool, state, "microsoft").await?;
    let dek = crate::crypto::DekPlaneSource::for_worker_tenant(&t.tenant, &t.pool, control_pool);
    match &t.tenant {
        TenantId::Consumer => consumer_tick_backstop(&t.pool, control_pool, state, &dek).await,
        TenantId::Org(org_id) => {
            let account_id = resolve_tenant_account(control_pool, org_id).await?;
            org_tick_backstop(&t.pool, control_pool, state, &dek, account_id).await
        }
    }
}

/// The org-tenant analog of [`consumer_tick_backstop`]: no cross-account
/// discovery, since `account_id` is already known (resolved once from the
/// control-plane routing row, same pattern [`tick_renew`]'s org arm uses).
/// Checks for watched mail/Teams resources FIRST (mirroring
/// `consumer_tick_backstop`'s own `accounts.is_empty()` short-circuit) so an
/// org tenant with nothing watched — the ordinary case until PR 2.4 populates
/// org-tenant watched rows — reports `Tick::idle()` rather than a hollow
/// `worked(1)` for a resync that touched nothing.
async fn org_tick_backstop(
    pool: &PgPool,
    control_pool: &PgPool,
    state: &Arc<AppState>,
    dek: &crate::crypto::DekPlaneSource,
    account_id: Uuid,
) -> Result<Tick, String> {
    let has_watched_mail = !microsoft::watched_conversations_for_account(pool, account_id)
        .await
        .map_err(|e| e.to_string())?
        .is_empty();
    let has_watched_teams = !microsoft::teams_watched_for_account(pool, account_id)
        .await
        .map_err(|e| e.to_string())?
        .is_empty();
    if !has_watched_mail && !has_watched_teams {
        return Ok(Tick::idle());
    }
    tracing::info!(%account_id, "graph backstop resync starting (org tenant)");
    resync_account(state, pool, control_pool, dek, account_id).await;
    // `resync_account` is best-effort per resource (logs and moves on) and
    // does not report a count of what it found — same reasoning
    // `consumer_tick_backstop` accepts today (its own `n` counts accounts
    // touched, not messages ingested). One account had watched resources and
    // was resynced, so `worked(1)`.
    Ok(Tick::worked(1))
}

/// Resync every consumer-plane account that has watched threads or Teams
/// resources.
async fn consumer_tick_backstop(
    pool: &PgPool,
    control_pool: &PgPool,
    state: &Arc<AppState>,
    dek: &crate::crypto::DekPlaneSource,
) -> Result<Tick, String> {
    let mut accounts = microsoft::accounts_with_watched_conversations(pool)
        .await
        .map_err(|e| e.to_string())?;
    for a in microsoft::accounts_with_teams_watched(pool)
        .await
        .map_err(|e| e.to_string())?
    {
        if !accounts.contains(&a) {
            accounts.push(a);
        }
    }
    if accounts.is_empty() {
        return Ok(Tick::idle());
    }
    tracing::info!(count = accounts.len(), "graph backstop resync starting");
    let n = accounts.len();
    for account_id in accounts {
        resync_account(state, pool, control_pool, dek, account_id).await;
    }
    Ok(Tick::worked(n))
}

/// Re-list every watched conversation for an account and ingest anything new.
/// Best-effort: logs and moves on so one bad thread can't stall the rest.
/// Called by the periodic backstop and by `missed`/`subscriptionRemoved`
/// lifecycle events (`routes::graph_webhook::process_lifecycle`).
///
/// `pool` is the content pool for subscription/watched-resource reads (`t.pool`
/// on the tenant-loop backstop tick; `state.db` — unchanged — from the webhook
/// call site); `control_pool` is the Microsoft OAuth token pool. `state` is
/// still required for the actual ingest calls (`routes::graph_webhook::
/// ingest_message`/`ingest_container_messages`), which are plane-aware as of
/// m365 Train 2 PR 2.3 — see this module's own doc comment.
pub(crate) async fn resync_account(
    state: &Arc<AppState>,
    pool: &PgPool,
    control_pool: &PgPool,
    dek: &crate::crypto::DekPlaneSource,
    account_id: Uuid,
) {
    // Rate-limit scope for every Graph call this resync makes (m365 Train 2
    // PR 2.6) — covers both call sites (`tick_backstop`, with no outer scope
    // of its own, and `routes::graph_webhook::process_lifecycle`, which
    // already opens the same key — nesting is harmless, see that scope's own
    // comment). See `graph_rate_limit`'s module doc.
    crate::graph_rate_limit::scoped(account_id.to_string(), resync_account_inner(
        state,
        pool,
        control_pool,
        dek,
        account_id,
    ))
    .await
}

async fn resync_account_inner(
    state: &Arc<AppState>,
    pool: &PgPool,
    control_pool: &PgPool,
    dek: &crate::crypto::DekPlaneSource,
    account_id: Uuid,
) {
    // Delegated token is OPTIONAL — an account may be RSC/app-only (it added the
    // Teams app but never did delegated OAuth). Teams delegated watches load
    // per-watcher below. App-mode Teams resources use a per-resource app token.
    let plane = crate::routes::slack::account_plane(control_pool, account_id)
        .await
        .unwrap_or(crate::routes::slack::AccountPlane::Personal);

    // ── Mail conversation re-list (delegated only) ──
    // Whose mailbox a thread lives in: on the Personal plane the one
    // account-wide connection; on the ORG plane each member's OWN Outlook
    // connection (`oauth_tokens.owner_user_id`), i.e. the member who opted the
    // thread in (`ingested_by`). Loading AccountWide for an org account finds
    // no row, so the backstop silently skipped every org thread — leaving no
    // recovery path at all when a live notification was missed.
    let mut mail_tokens: HashMap<Option<Uuid>, Option<String>> = HashMap::new();
    match microsoft::watched_conversations_for_account(pool, account_id).await {
        Ok(watched) => {
            let mut processed = 0usize;
            for (conversation_id, graph_id, user_id, follow_since) in watched {
                let owner = mail_backstop_token_owner(plane, user_id);
                if !mail_tokens.contains_key(&owner.owner_user_id()) {
                    let tok = delegated_microsoft_token(
                        &state.http_client,
                        control_pool,
                        dek,
                        account_id,
                        owner,
                    )
                    .await;
                    mail_tokens.insert(owner.owner_user_id(), tok);
                }
                let Some(Some(token)) = mail_tokens.get(&owner.owner_user_id()) else {
                    continue;
                };
                match microsoft::fetch_conversation_since(&state.http_client, token, &conversation_id, None, follow_since)
                    .await
                {
                    Ok(page) => {
                        // Do NOT persist `page.next_cursor`/`page.complete` here
                        // (CodeRabbit review, PR #673). This backstop always
                        // fetches from scratch (`None` cursor above) — it is a
                        // safety net for missed live notifications, not the
                        // resumable backfill owner. Overwriting the stored
                        // cursor with THIS capped, from-scratch page would
                        // rewind `tick_backfill_continue`'s real progress (or
                        // flip an already-completed conversation back to
                        // pending) every time the backstop tick runs. Message
                        // ingestion is still fully applied — only the
                        // continuation bookkeeping is skipped.
                        for msg in page.items {
                            if !msg.is_future_reply(follow_since) { continue; }
                            let item = microsoft::message_to_item(msg);
                            if ingest_message_stored(state, account_id, user_id, graph_id, item).await
                            {
                                processed += 1;
                            }
                        }
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, %conversation_id, "resync: conversation fetch failed")
                    }
                }
            }
            tracing::info!(%account_id, processed, "mail backstop resync complete");
        }
        Err(e) => {
            tracing::warn!(error = %e, %account_id, "resync: failed to list watched conversations")
        }
    }

    // ── Teams re-poll (per-watcher token: app for RSC, Member/AccountWide for
    // delegated — S4 / item 47a). One row per watching member; each poll uses
    // that watcher's own grant. Live doorbells stay on this 6-hour cadence.
    // Polling doorbells are also re-listed sooner by [`tick_doorbell_polling`].
    let mut tokens = TeamsTokenCache::default();
    match microsoft::teams_watched_for_account(pool, account_id).await {
        Ok(watched) => {
            let capture_open =
                crate::services::org_install_grace::org_install_capture_open(control_pool).await;
            for (kind, team_id, channel_id, chat_id, graph_id, user_id, tenant_id, auth_mode) in
                watched
            {
                if crate::services::org_install_grace::rsc_app_capture_blocked(
                    capture_open, &auth_mode,
                ) {
                    continue;
                }
                let owner = teams_watcher_token_owner(plane, user_id, &auth_mode);
                // Per-resource rate-limit scope (CodeRabbit review, PR #673) —
                // same "one account can span more than one AAD tenant" reasoning
                // as `ensure_teams_subscriptions_for_account_inner`'s identical
                // per-iteration scope; see that call site's comment.
                let tenant_id_ref = &tenant_id;
                crate::graph_rate_limit::scoped(
                    crate::graph_rate_limit::rate_key(account_id, tenant_id_ref),
                    async {
                let Some(token) = tokens
                    .token_for(
                        &state.http_client,
                        control_pool,
                        dek,
                        account_id,
                        &tenant_id,
                        &auth_mode,
                        owner,
                    )
                    .await
                else {
                    return;
                };
                let container_id = if channel_id.is_empty() { &chat_id } else { &channel_id };
                let page = match kind.as_str() {
                    "channel" => {
                        microsoft::fetch_channel_messages(
                            &state.http_client,
                            &token,
                            &team_id,
                            &channel_id,
                            None,
                            None,
                        )
                        .await
                    }
                    "chat" => {
                        microsoft::fetch_chat_messages(
                            &state.http_client,
                            &token,
                            &chat_id,
                            None,
                            None,
                        )
                        .await
                    }
                    _ => return,
                };
                match page {
                    Ok(page) => {
                        // Do NOT persist `page.next_cursor`/`page.complete` here —
                        // see the mail loop's identical comment above. This
                        // backstop refetches from scratch (`None, None`); message
                        // ingestion still runs, only the continuation cursor/
                        // completion bookkeeping is skipped so it can't rewind
                        // `tick_backfill_continue`'s real progress.
                        ingest_container_messages(
                            state,
                            account_id,
                            user_id,
                            graph_id,
                            container_id,
                            page.items,
                        )
                        .await;
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, %account_id, kind, "teams re-poll fetch failed")
                    }
                }
                    },
                )
                .await;
            }
        }
        Err(e) => tracing::warn!(error = %e, %account_id, "resync: teams watched list failed"),
    }
}

/// Tighter re-list for doorbells in `state = 'polling'` only. Live doorbells
/// stay on [`tick_backstop`]. Mail is not touched. Each watcher is fetched
/// with their own token, same as the 6-hour Teams loop.
pub async fn tick_doorbell_polling(
    t: &WorkerTenant,
    control_pool: &PgPool,
    state: &Arc<AppState>,
) -> Result<Tick, String> {
    let dek = crate::crypto::DekPlaneSource::for_worker_tenant(&t.tenant, &t.pool, control_pool);
    let due = crate::connectors::teams_doorbell::polling_containers(control_pool)
        .await
        .map_err(|e| e.to_string())?;
    if due.is_empty() {
        return Ok(Tick::idle());
    }
    let accounts = polling_accounts_for_tenant(&t.tenant, control_pool).await?;
    if accounts.is_empty() {
        return Ok(Tick::idle());
    }
    let mut touched = 0usize;
    for account_id in accounts {
        let resources = crate::connectors::teams_doorbell::polling_resources_for_account(
            control_pool,
            account_id,
        )
        .await
        .map_err(|e| e.to_string())?;
        if resources.is_empty() {
            continue;
        }
        let allow: HashSet<String> = resources.into_iter().collect();
        touched += poll_account_doorbell_resources(&t.pool, control_pool, state, &dek, account_id, &allow).await;
    }
    Ok(if touched > 0 { Tick::worked(touched) } else { Tick::idle() })
}

async fn polling_accounts_for_tenant(
    tenant: &TenantId,
    control_pool: &PgPool,
) -> Result<Vec<Uuid>, String> {
    match tenant {
        TenantId::Consumer => sqlx::query_scalar(
            "SELECT DISTINCT w.account_id \
             FROM teams_container_watchers w \
             JOIN accounts a ON a.id = w.account_id \
             JOIN teams_container_subscriptions s \
               ON s.ms_tenant_id = w.ms_tenant_id AND s.resource = w.resource \
             WHERE s.state = 'polling' AND a.clerk_org_id IS NULL",
        )
        .fetch_all(control_pool)
        .await
        .map_err(|e| e.to_string()),
        TenantId::Org(org_id) => {
            let account_id = resolve_tenant_account(control_pool, org_id).await?;
            let polling: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM teams_container_watchers w \
                 JOIN teams_container_subscriptions s \
                   ON s.ms_tenant_id = w.ms_tenant_id AND s.resource = w.resource \
                 WHERE w.account_id = $1 AND s.state = 'polling'",
            )
            .bind(account_id)
            .fetch_one(control_pool)
            .await
            .map_err(|e| e.to_string())?;
            Ok(if polling > 0 { vec![account_id] } else { Vec::new() })
        }
    }
}

/// Re-list only the delegated watches whose Graph resource is in `allow`.
/// Returns how many containers were fetched. Does not touch mail.
async fn poll_account_doorbell_resources(
    pool: &PgPool,
    control_pool: &PgPool,
    state: &Arc<AppState>,
    dek: &crate::crypto::DekPlaneSource,
    account_id: Uuid,
    allow: &HashSet<String>,
) -> usize {
    let plane = crate::routes::slack::account_plane(control_pool, account_id)
        .await
        .unwrap_or(crate::routes::slack::AccountPlane::Personal);
    let watched = match microsoft::teams_watched_for_account(pool, account_id).await {
        Ok(w) => w,
        Err(e) => {
            tracing::warn!(error = %e, %account_id, "doorbell polling: watched list failed");
            return 0;
        }
    };
    let mut tokens = TeamsTokenCache::default();
    let mut touched = 0usize;
    for (kind, team_id, channel_id, chat_id, graph_id, user_id, tenant_id, auth_mode) in watched {
        if auth_mode == "app" {
            continue;
        }
        let Some(resource) =
            microsoft::teams_subscription_resource(&kind, &team_id, &channel_id, &chat_id)
        else {
            continue;
        };
        if !allow.contains(&resource) {
            continue;
        }
        let owner = teams_watcher_token_owner(plane, user_id, &auth_mode);
        let fetched = crate::graph_rate_limit::scoped(
            crate::graph_rate_limit::rate_key(account_id, &tenant_id),
            async {
                let Some(token) = tokens
                    .token_for(
                        &state.http_client,
                        control_pool,
                        dek,
                        account_id,
                        &tenant_id,
                        &auth_mode,
                        owner,
                    )
                    .await
                else {
                    return false;
                };
                let container_id = if channel_id.is_empty() { &chat_id } else { &channel_id };
                // Follow Graph's `next_cursor` in this tick only. Do not
                // persist it onto `teams_watched_resources.backfill_cursor`
                // — that cursor belongs to `tick_backfill_continue`.
                let mut resume: Option<String> = None;
                loop {
                    let page = match kind.as_str() {
                        "channel" => {
                            microsoft::fetch_channel_messages(
                                &state.http_client,
                                &token,
                                &team_id,
                                &channel_id,
                                None,
                                resume.as_deref(),
                            )
                            .await
                        }
                        "chat" => {
                            microsoft::fetch_chat_messages(
                                &state.http_client,
                                &token,
                                &chat_id,
                                None,
                                resume.as_deref(),
                            )
                            .await
                        }
                        _ => return false,
                    };
                    match page {
                        Ok(page) => {
                            ingest_container_messages(
                                state,
                                account_id,
                                user_id,
                                graph_id,
                                container_id,
                                page.items,
                            )
                            .await;
                            match page.next_cursor.filter(|_| !page.complete) {
                                Some(next) => resume = Some(next),
                                None => return true,
                            }
                        }
                        Err(e) => {
                            tracing::warn!(error = %e, %account_id, kind, "doorbell polling fetch failed");
                            return resume.is_some();
                        }
                    }
                }
            },
        )
        .await;
        if fetched {
            touched += 1;
        }
    }
    touched
}

// ── Backfill continuation (m365 Train 2 PR 2.6) ───────────────────────────────
//
// Resumes any backfill a previous pass left incomplete — the counterpart to
// the old silent-truncation behavior in `connectors::microsoft`
// (`MAX_CONVERSATION_MESSAGES`/`TEAMS_BACKFILL_CAP`/`BACKFILL_SCAN_CAP` all
// used to hit a hard cap, log a warning, and drop the remainder; they now
// return a [`microsoft::BackfillPage`] with an explicit `complete` flag that
// gets persisted onto the watched-resource row). This tick finds every row
// (across BOTH `teams_watched_resources` and `outlook_watched_conversations`,
// plus `outlook_workflows`' own candidate-scan state via
// `routes::workflows::continue_workflow_backfills`) with `backfill_complete =
// FALSE` and resumes it from its stored cursor, converging over however many
// passes it takes — a per-pass page budget is fine now that nothing is lost
// between passes. Same two-pool split as every other tick in this module
// (content on `t.pool`, the Microsoft OAuth token on `control_pool`); same
// plane-explicit `Consumer`/`Org` split as [`tick_backstop`].

/// One backfill-continuation pass. Driven by `workers::tenant_harness::
/// run_multi_tenant` as a sweep worker (`HarnessConfig::sweep_worker(_,
/// BACKFILL_CONTINUE_INTERVAL)`).
pub async fn tick_backfill_continue(
    t: &WorkerTenant,
    control_pool: &PgPool,
    state: &Arc<AppState>,
) -> Result<Tick, String> {
    // Exercise the real discovery pass promptly in the acceptance binary,
    // without accelerating the automatic unbounded message backstop.
    #[cfg(feature = "e2e")]
    crate::connectors::catalog::everything::refresh(t, control_pool, state, "microsoft").await?;
    let dek = crate::crypto::DekPlaneSource::for_worker_tenant(&t.tenant, &t.pool, control_pool);
    match &t.tenant {
        TenantId::Consumer => consumer_tick_backfill_continue(&t.pool, control_pool, state, &dek).await,
        TenantId::Org(org_id) => {
            let account_id = resolve_tenant_account(control_pool, org_id).await?;
            let n = continue_backfills_for_account(&t.pool, control_pool, state, &dek, account_id).await;
            Ok(if n > 0 { Tick::worked(n) } else { Tick::idle() })
        }
    }
}

async fn consumer_tick_backfill_continue(
    pool: &PgPool,
    control_pool: &PgPool,
    state: &Arc<AppState>,
    dek: &crate::crypto::DekPlaneSource,
) -> Result<Tick, String> {
    let mut accounts = microsoft::accounts_with_watched_conversations(pool)
        .await
        .map_err(|e| e.to_string())?;
    for a in microsoft::accounts_with_teams_watched(pool)
        .await
        .map_err(|e| e.to_string())?
    {
        if !accounts.contains(&a) {
            accounts.push(a);
        }
    }
    if accounts.is_empty() {
        return Ok(Tick::idle());
    }
    let mut touched = 0usize;
    for account_id in accounts {
        touched += continue_backfills_for_account(pool, control_pool, state, dek, account_id).await;
    }
    Ok(if touched > 0 { Tick::worked(touched) } else { Tick::idle() })
}

/// Resume every incomplete backfill for one account: watched Outlook
/// conversations, watched Teams resources, and pending workflow candidate
/// scans. Best-effort per resource, same as [`resync_account`] — one bad
/// resource is logged and skipped, never aborting the rest. Returns how many
/// distinct resources were retried (0 means nothing was pending).
async fn continue_backfills_for_account(
    pool: &PgPool,
    control_pool: &PgPool,
    state: &Arc<AppState>,
    dek: &crate::crypto::DekPlaneSource,
    account_id: Uuid,
) -> usize {
    crate::graph_rate_limit::scoped(
        account_id.to_string(),
        continue_backfills_for_account_inner(pool, control_pool, state, dek, account_id),
    )
    .await
}

async fn continue_backfills_for_account_inner(
    pool: &PgPool,
    control_pool: &PgPool,
    state: &Arc<AppState>,
    dek: &crate::crypto::DekPlaneSource,
    account_id: Uuid,
) -> usize {
    let mut touched = 0usize;

    // Query the pending lists FIRST — before minting any token (CodeRabbit
    // review, PR #673). This tick runs every `BACKFILL_CONTINUE_INTERVAL`
    // (5 minutes) over every account with ANY watched resource, not only
    // accounts with something actually pending; `delegated_microsoft_token`
    // below is a scoped DB transaction + DEK resolve + token decrypt (and
    // possibly an outbound refresh) — paying that per account per sweep just
    // to discover nothing is pending is the hot path this reorder avoids.
    // `tick_backstop`'s equivalent already checks for watched rows before
    // minting a token; this brings the (much more frequent) continuation
    // tick in line with it.
    let pending_conversations =
        match microsoft::pending_conversation_backfills_for_account(pool, account_id).await {
            Ok(p) => p,
            Err(e) => {
                tracing::warn!(error = %e, %account_id, "backfill continue: pending conversations list failed");
                Vec::new()
            }
        };
    let pending_teams = match microsoft::pending_teams_backfills_for_account(pool, account_id).await {
        Ok(p) => p,
        Err(e) => {
            tracing::warn!(error = %e, %account_id, "backfill continue: pending teams list failed");
            Vec::new()
        }
    };

    // Nothing pending for this account: skip the DEK resolve/decrypt (and any
    // token refresh) entirely.
    // Mail stays AccountWide this story; Teams loads per-watcher below (S4).
    let mail_token = if pending_conversations.is_empty() && pending_teams.is_empty() {
        None
    } else {
        delegated_microsoft_token(
            &state.http_client,
            control_pool,
            dek,
            account_id,
            TokenOwner::AccountWide,
        )
        .await
    };
    let plane = crate::routes::slack::account_plane(control_pool, account_id)
        .await
        .unwrap_or(crate::routes::slack::AccountPlane::Personal);

    // ── Outlook conversations ──
    if let Some(token) = mail_token.as_deref() {
        for (conversation_id, graph_id, user_id, cursor) in pending_conversations {
            touched += 1;
            match microsoft::fetch_conversation(
                &state.http_client,
                token,
                &conversation_id,
                cursor.as_deref(),
            )
            .await
            {
                Ok(page) => {
                    let complete = page.complete;
                    let page_len = page.items.len();
                    let mut ingested = 0usize;
                    for msg in page.items {
                        let item = microsoft::message_to_item(msg);
                        if ingest_message_stored(state, account_id, user_id, graph_id, item).await {
                            ingested += 1;
                        }
                    }
                    if ingested != page_len {
                        // Do NOT advance the cursor past messages that failed to
                        // ingest — same guard as `routes::workflows::continue_
                        // workflow_backfills`'s identical conversation loop (and
                        // `teams_ingest::backfill_history_inner`'s Teams-side
                        // twin): a transient failure here would otherwise be
                        // permanent, since neither this tick nor the backstop
                        // resync ever revisits this exact page. Leave the stored
                        // cursor/completion untouched (safe — `ingest_message`'s
                        // own dedup makes a re-ingested message a no-op) so the
                        // page is retried next pass.
                        tracing::warn!(
                            %conversation_id, ingested, page_len,
                            "backfill continue: partial ingest failure this page — cursor not \
                             advanced, page will be retried"
                        );
                    } else if let Err(e) = microsoft::set_conversation_backfill_state(
                        pool,
                        account_id,
                        &conversation_id,
                        page.next_cursor.as_deref(),
                        complete,
                    )
                    .await
                    {
                        tracing::warn!(error = %e, %conversation_id, "backfill continue: cursor persist failed");
                    }
                }
                Err(e) => tracing::warn!(error = %e, %conversation_id, "backfill continue: conversation fetch failed"),
            }
        }
    }

    // ── Teams resources ──
    let mut tokens = TeamsTokenCache::default();
    let capture_open =
        crate::services::org_install_grace::org_install_capture_open(control_pool).await;
    for (kind, team_id, channel_id, chat_id, graph_id, user_id, tenant_id, auth_mode, cursor, since, until) in
        pending_teams
    {
        if crate::services::org_install_grace::rsc_app_capture_blocked(capture_open, &auth_mode) {
            continue;
        }
        let owner = teams_watcher_token_owner(plane, user_id, &auth_mode);
        // Per-resource rate-limit scope, keyed via `rate_key` on THIS
        // resource's own `tenant_id` (m365 Train 2 PR 2.6 CodeRabbit round 2)
        // — same "one account can span more than one AAD tenant" reasoning as
        // `ensure_teams_subscriptions_for_account_inner`'s identical
        // per-iteration scope (see that call site's comment). The outer
        // `continue_backfills_for_account` scope keys on bare `account_id`,
        // which is correct for the Outlook conversations loop above
        // (delegated calls key on `account_id`) but wrong for these app-only
        // Teams calls: two accounts sharing one AAD tenant would otherwise
        // draw from two independent buckets against Graph's ONE shared
        // per-tenant-per-app quota. Nesting shadows the outer scope for just
        // this iteration's Graph calls (`token_for` + the fetch below) — see
        // `graph_rate_limit`'s module doc.
        let tenant_id_ref = &tenant_id;
        // Returns whether a token was obtained at all — mirrors the pre-fix
        // `touched` bookkeeping exactly (incremented once a resource is
        // actually attempted, independent of whether the fetch that follows
        // succeeds), just relocated inside the scope so the increment stays
        // outside the ambient-key closure below.
        let attempted = crate::graph_rate_limit::scoped(
            crate::graph_rate_limit::rate_key(account_id, tenant_id_ref),
            async {
                let Some(token) = tokens
                    .token_for(
                        &state.http_client,
                        control_pool,
                        dek,
                        account_id,
                        &tenant_id,
                        &auth_mode,
                        owner,
                    )
                    .await
                else {
                    return false;
                };
                let container_id = if channel_id.is_empty() { &chat_id } else { &channel_id };
                let page = match kind.as_str() {
                    "channel" => {
                        microsoft::fetch_channel_messages(
                            &state.http_client,
                            &token,
                            &team_id,
                            &channel_id,
                            since,
                            cursor.as_deref(),
                        )
                        .await
                    }
                    "chat" => {
                        microsoft::fetch_chat_messages(
                            &state.http_client,
                            &token,
                            &chat_id,
                            since,
                            cursor.as_deref(),
                        )
                        .await
                    }
                    _ => return true,
                };
                match page {
                    Ok(mut page) => {
                        page.items.retain(|item| microsoft::in_teams_history_window(item.timestamp, since, until));
                        let complete = page.complete;
                        let page_len = page.items.len();
                        let ingested = ingest_container_messages(
                            state,
                            account_id,
                            user_id,
                            graph_id,
                            container_id,
                            page.items,
                        )
                        .await;
                        if ingested != page_len {
                            // Do NOT advance the cursor past messages that failed to
                            // ingest — same guard as the Outlook conversations loop
                            // above (and `routes::workflows::continue_workflow_
                            // backfills`'s/`teams_ingest::backfill_history_inner`'s
                            // identical guards): a transient failure here would
                            // otherwise be permanent, since neither this tick nor
                            // the backstop resync ever revisits this exact page.
                            // Leave `backfill_cursor`/`backfill_complete` untouched
                            // (safe — `ingest_message`'s own dedup makes a
                            // re-ingested message a no-op) so the page is retried
                            // next pass.
                            tracing::warn!(
                                %account_id, kind, ingested, page_len,
                                "backfill continue: partial teams ingest failure this page — \
                                 cursor not advanced, page will be retried"
                            );
                        } else if let Err(e) = microsoft::set_teams_backfill_state_fenced(
                            pool,
                            account_id,
                            container_id,
                            user_id,
                            &auth_mode,
                            page.next_cursor.as_deref(),
                            complete,
                            Some((since, until)),
                        )
                        .await
                        {
                            tracing::warn!(error = %e, %account_id, kind, "backfill continue: teams cursor persist failed");
                        }
                    }
                    Err(e) => tracing::warn!(error = %e, %account_id, kind, "backfill continue: teams fetch failed"),
                }
                true
            },
        )
        .await;
        if attempted {
            touched += 1;
        }
    }

    // ── Outlook workflow candidate scans ──
    // `pool` is the CONTENT pool (workflow rows + the conversations they
    // discover); `control_pool` and `dek` are the token row's pool and the
    // account's key plane. It used to pass `pool` for all three, which was
    // only ever correct on the consumer entry (`pool` IS `control_pool`
    // there) — on an org tenant `oauth_tokens` does not exist on `t.pool` at
    // all, so a credential read would have hard-errored the moment
    // `outlook_workflows` gained an org row. It cannot today (Outlook mail
    // rules have no per-member delegated token yet — Train 3, not built), so
    // the org arm short-circuits on an empty pending list before reaching
    // the read; this makes it correct rather than merely unreachable.
    touched +=
        workflows::continue_workflow_backfills(state, pool, control_pool, dek, account_id).await;

    touched
}

#[cfg(test)]
mod mail_backstop_owner_tests {
    use super::*;
    use crate::routes::slack::AccountPlane;

    /// The backstop used `AccountWide` unconditionally, which finds no token
    /// for an org account (its Outlook connections are per-member), so every
    /// org thread was skipped. Org must read the opting-in member's mailbox.
    #[test]
    fn org_threads_use_the_opting_in_members_token_personal_stays_account_wide() {
        let member = Uuid::new_v4();
        assert_eq!(
            mail_backstop_token_owner(AccountPlane::Org, member),
            TokenOwner::Member(member)
        );
        assert_eq!(
            mail_backstop_token_owner(AccountPlane::Personal, member),
            TokenOwner::AccountWide
        );
    }
}
