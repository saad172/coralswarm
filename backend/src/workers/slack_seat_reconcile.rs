//! Seat reconciler — makes the seats match the rosters, in both directions.
//!
//! The live `member_joined_channel` / `member_left_channel` handlers
//! (`routes::slack_events`) keep seats correct going forward. This worker is
//! what makes them correct at all, for three populations the event path
//! cannot reach:
//!
//! 1. **Channels routed before `team_seat_grants` existed.** Their members
//!    hold seats with no provenance. That is the safe state — the revocation
//!    gate refuses to touch a seat it has no grant for — but it is also a
//!    permanent one: nobody in those channels can ever lose a seat by leaving.
//!    A sweep records the missing grants and makes them revocable.
//! 2. **Channels the bot joined before the destination picker shipped.** No
//!    prompt ever fired, so their rosters were never walked and their members
//!    were never seated at all.
//! 3. **Anything missed while the backend was down**, or that Slack simply
//!    never delivered. Membership events are not redelivered on demand.
//!
//! # The asymmetry that matters
//!
//! Granting and revoking are NOT treated the same way here, and the
//! difference is deliberate.
//!
//! A grant is additive and idempotent: re-asserting one that already exists
//! is free, and asserting one that should exist fixes a gap. So the sweep
//! grants generously.
//!
//! A revocation removes someone's access. So the sweep only ever revokes for
//! a `(team, channel)` whose roster it successfully walked in full. If
//! `conversations.members` fails, is rate-limited, or returns a partial
//! answer, the channel is SKIPPED entirely — never treated as "the roster is
//! empty, revoke everyone". `connectors::slack::resolve_channel_members`
//! already refuses to return a partial roster (it propagates `Unavailable`
//! rather than under-reporting), which is exactly the property this worker
//! depends on: an `Err` here means "we do not know", and the only safe
//! response to not knowing is to change nothing.
//!
//! # Two pools, tenant loop (org-slack step 4)
//!
//! This worker now joins `workers::tenant_harness::run_multi_tenant`.
//! `JobKind::SlackSeatReconcile` has no live enqueue site — it is hand-
//! inserted (see that variant's own doc comment) — so there is no sibling
//! enqueue-side commit to pair with; the flip is this worker moving to read
//! from `t.pool` (whichever plane an operator inserts the sweep job onto)
//! instead of the single shared consumer pool.
//!
//! The Slack bot token stays CONTROL-ONLY (`oauth_tokens`, beside
//! `connector_installs` — founder decision, same as `workers::huddle`), so
//! [`crate::workers::huddle::get_slack_bot_token_pub`] is always called with
//! `control_pool`. `accounts_to_sweep` is the other control-pool touch point:
//! on the consumer plane it enumerates every row in `accounts`; on an org
//! tenant there is exactly ONE account (that tenant's own), resolved from
//! `tenant_databases`. Every other read/write — `slack_watched_channels`,
//! `teams`, `team_members`, `team_seat_grants` — runs on `t.pool`, the
//! content pool for whichever plane this sweep is running against.
//!
//! `teams`/`team_members` are additionally on `migrations_tenant/
//! 0009_tenant_rls`'s policy roster, which `begin_account_scoped` alone does
//! not satisfy for a tenant-wide sweep with no acting end user (see
//! `tenancy::scope::begin_tenant_system_scoped`'s doc comment) —
//! [`seat_bearing_channels_for_account`] and the `seats`/
//! `routes::slack_interactivity` functions this worker calls into all use
//! `begin_tenant_system_scoped`, not the plain `begin_account_scoped` that
//! was sufficient when every caller ran on the consumer plane only.

use sqlx::PgPool;
use uuid::Uuid;

use crate::jobs::{queue, JobKind};
use crate::tenancy::scope::begin_tenant_system_scoped;
use crate::tenancy::TenantId;
use crate::workers::tenant_harness::{Tick, WorkerTenant};

/// Channels reconciled per job execution. Small on purpose: each channel
/// costs a full `conversations.members` pagination plus a `users.info` for
/// every member not already in the directory, so this bounds the worst-case
/// Slack API burst per job the same way `slack_name_backfill`'s batch does.
///
/// **Known gap, carried forward unfixed (org-slack step 4 — explicitly out
/// of scope for this PR):** this cap is GLOBAL across the whole sweep, not
/// per account/tenant, and there is no continuation job — a hand-inserted
/// `SlackSeatReconcile` reconciles at most this many channels and then stops,
/// silently, with no re-enqueue. On the consumer plane (many accounts sharing
/// one sweep) this means an account whose channels sort late never gets
/// reached in one pass; fixing it (a keyset cursor + continuation, exactly
/// like `slack_name_backfill`'s `after_atom_id`) is a real, contained
/// follow-up but would meaningfully bloat this PR's diff on top of the
/// tenant-plane move, so it is left as-is here.
const CHANNEL_BATCH_SIZE: i64 = 20;

const JOB_BATCH_SIZE: usize = 5;
/// How long to sleep between polls when the queue is empty — the tenant
/// harness's `base_poll` / idle-backoff floor for this worker.
pub const EMPTY_POLL_SLEEP: std::time::Duration = std::time::Duration::from_secs(30);

/// One seat-bearing channel: a `slack_watched_channels` row whose `mode` is
/// `'single'` and whose destination graph some team owns.
#[derive(Debug, Clone)]
struct SeatBearingChannel {
    account_id: Uuid,
    workspace_id: String,
    channel_id: String,
    team_id: Uuid,
}

/// What one channel's reconciliation did. Returned rather than only logged so
/// [`process_job`] can report a total instead of the caller having to grep.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ReconcileStats {
    pub granted: usize,
    pub revoked: usize,
    pub kept: usize,
    pub skipped_channels: usize,
}

/// Accounts this sweep will visit.
///
/// **Consumer plane:** reads `accounts` on `control_pool`, which carries NO
/// 0083 policy — deliberately, and it is the only reason a cross-account
/// sweep is expressible at all. The obvious shape (one unscoped `slack_
/// watched_channels JOIN teams` across every account) is not merely a
/// policy-gate complaint: under `coralswarm_app` those tables ARE
/// policy-covered, so with no `app.account_id` set the query returns **zero
/// rows** and the whole sweep silently does nothing in production while
/// looking perfectly healthy in a test that runs as the database owner.
/// Discover accounts here, then scope per account below. A supplied
/// `account_id` is passed through as-is — the consumer plane's `jobs` table
/// already spans every account on the SAME physical pool, so naming any one
/// of them is never a wrong-plane mistake, only a (harmless) narrower sweep.
///
/// **Org tenant (org-slack step 4, CodeRabbit review on PR #490):** there is
/// exactly ONE account — that tenant's own — resolved from the control-plane
/// routing row (`tenant_databases`), the same "resolve the org's account_id
/// from control before working on tenant" pattern `workers::
/// member_ocean_reconciler::tick` uses. `accounts` does not exist on a
/// tenant database at all. A supplied `account_id` is CHECKED against the
/// resolved one, not trusted: `JobKind::SlackSeatReconcile` is hand-inserted
/// (this module's own doc comment), so an operator mistyping an id — or
/// pasting the wrong org's account into org A's `jobs` table — is the likely
/// failure mode, not an exotic one. Sweeping a foreign account_id on org A's
/// content pool would read nothing (harmless) but WRITE seat grants keyed to
/// an account that means nothing inside org A's own database — reject it
/// instead.
async fn accounts_to_sweep(
    tenant: &TenantId,
    control_pool: &PgPool,
    account_id: Option<Uuid>,
) -> Result<Vec<Uuid>, String> {
    match tenant {
        TenantId::Consumer => match account_id {
            Some(id) => Ok(vec![id]),
            None => sqlx::query_scalar("SELECT id FROM accounts ORDER BY id")
                .fetch_all(control_pool)
                .await
                .map_err(|e| e.to_string()),
        },
        TenantId::Org(org_id) => {
            let acct: Option<Uuid> = sqlx::query_scalar(
                "SELECT account_id FROM tenant_databases WHERE clerk_org_id = $1",
            )
            .bind(org_id.as_str())
            .fetch_optional(control_pool)
            .await
            .map_err(|e| e.to_string())?;
            match acct {
                Some(id) => match account_id {
                    Some(requested) if requested != id => Err(format!(
                        "payload account {requested} is not org {org_id}'s own account \
                         ({id}) — refusing to sweep a foreign account_id onto this tenant's \
                         content pool"
                    )),
                    _ => Ok(vec![id]),
                },
                // Defensive only: the harness never schedules a tenant
                // without an active routing row.
                None => Err(format!("no tenant_databases row for org {org_id}")),
            }
        }
    }
}

/// The seat-bearing channels of ONE account, read inside that account's
/// tenant-system scope so `teams`' RLS policy (`0083` on the consumer plane,
/// `0009_tenant_rls`'s `member_ocean_isolation` on an org tenant) is
/// satisfied rather than silently filtering everything away — see this
/// module's own doc comment for why `begin_tenant_system_scoped`, not the
/// plain `begin_account_scoped` that was sufficient when this only ran on
/// the consumer plane.
async fn seat_bearing_channels_for_account(
    pool: &PgPool,
    account_id: Uuid,
    channel_id: Option<&str>,
    limit: i64,
) -> Result<Vec<SeatBearingChannel>, sqlx::Error> {
    let mut tx = begin_tenant_system_scoped(pool, account_id)
        .await
        .map_err(|e| sqlx::Error::Protocol(format!("seat_bearing_channels scoped tx: {e}")))?;

    let rows: Vec<(String, String, Uuid)> = sqlx::query_as(
        "SELECT swc.workspace_id, swc.channel_id, t.id \
         FROM slack_watched_channels swc \
         JOIN teams t ON t.graph_id = swc.graph_id AND t.account_id = swc.account_id \
         WHERE swc.account_id = $1 AND swc.mode = 'single' AND swc.graph_id IS NOT NULL \
           AND ($2::text IS NULL OR swc.channel_id = $2) \
         ORDER BY swc.channel_id \
         LIMIT $3",
    )
    .bind(account_id)
    .bind(channel_id)
    .bind(limit)
    .fetch_all(&mut *tx)
    .await?;
    tx.rollback().await.ok();

    Ok(rows
        .into_iter()
        .map(|(workspace_id, channel_id, team_id)| SeatBearingChannel {
            account_id,
            workspace_id,
            channel_id,
            team_id,
        })
        .collect())
}

/// Test-only public alias for [`seat_bearing_channels_for_account`] (which is
/// private to this module, so `backend/tests/*.rs` — which links this crate
/// as an ordinary external dependency — cannot see it). Same idiom, and same
/// `test-support` feature, as `workers::huddle::test_support_slack_bot_token`;
/// never present in a normal build. Returns just the count — the discovery
/// query's shape (`slack_watched_channels JOIN teams`) is the thing under
/// test, not the row contents.
#[cfg(feature = "test-support")]
pub async fn test_support_seat_bearing_channel_count(
    pool: &PgPool,
    account_id: Uuid,
) -> Result<usize, sqlx::Error> {
    Ok(seat_bearing_channels_for_account(pool, account_id, None, CHANNEL_BATCH_SIZE)
        .await?
        .len())
}

/// Reconcile one channel. Grants for everyone currently in it, revocations
/// for everyone this channel still vouches for who is no longer in it.
///
/// `control_pool` is needed ONLY for the Slack bot token (`oauth_tokens`,
/// control-only) and `resolve_channel_members`'s `account_members` check;
/// every seat read/write runs on `pool` (this sweep's content pool).
async fn reconcile_channel(
    control_pool: &PgPool,
    pool: &PgPool,
    tenant: &TenantId,
    http: &reqwest::Client,
    ch: &SeatBearingChannel,
) -> Result<ReconcileStats, String> {
    let mut stats = ReconcileStats::default();

    // ROW on control, KEY on this worker tick's own plane (org-plane DEK
    // fix): `tenant`/`pool` come straight off the `WorkerTenant` this tick
    // was built for, so for an org they are its tenant DB — where
    // `account_keys` lives — and for Consumer they are the control pool.
    let dek_plane = crate::crypto::DekPlane::from_tenant(tenant, pool.clone(), ch.account_id);
    let bot_token = match crate::workers::huddle::get_slack_bot_token_pub(
        control_pool,
        &dek_plane,
        ch.account_id,
    )
    .await
    {
            Ok(Some(t)) => t,
            Ok(None) => return Err("no bot token for this account".to_string()),
            Err(e) => return Err(format!("bot token lookup failed: {e}")),
        };

    // `Err` here means the roster is UNKNOWN, not empty — see this module's
    // header. Propagating it skips the channel and changes nothing.
    let (matched_user_ids, _total) = crate::connectors::slack::resolve_channel_members(
        control_pool,
        pool,
        http,
        ch.account_id,
        &ch.workspace_id,
        &bot_token,
        &ch.channel_id,
    )
    .await?;

    let source = crate::seats::slack_channel_source(&ch.channel_id);

    // Founding sweep vs late joiner (m365 enterprise train PR 4.1): decided
    // ONCE per source, BEFORE this sweep's grant loop runs — `None` (full
    // history) only when this pass seats the source's FOUNDING roster, same
    // rule `auto_seat_channel_members`'s routing-time call already applies;
    // `Some(now())` for anyone newly discovered after that (someone who
    // joined the channel without a `member_joined_channel` event ever
    // reaching us — backend downtime, a redelivery gap), matching
    // `seat_joining_member`'s own live-event rule as closely as a periodic
    // sweep can. See `founding_history_from`'s doc for why "founded" is a
    // durable `seat_grant_sources` marker and not grant-emptiness (grant
    // rows are DELETED on revocation — a fully-vacated channel must not
    // look brand-new).
    //
    // A single shared value for the whole batch is correct, not a
    // simplification: the decision is per `(team_id, source)`, not per
    // member, and `record_seat_grant`'s `ON CONFLICT DO NOTHING` means this
    // value is silently ignored for every member already granted — only a
    // genuinely NEW grant row ever uses it.
    let history_from =
        crate::seats::founding_history_from(pool, ch.account_id, ch.team_id, &source)
            .await
            .map_err(|e| format!("founding check failed (pre-sweep): {e}"))?;

    // ── Grant side ────────────────────────────────────────────────────────
    // Idempotent, and it seats anyone missing. This is what closes gaps 1
    // and 2: an already-seated member picks up the grant they never had, and
    // a never-seated member gets the seat routing always implied.
    let granted =
        crate::routes::slack_interactivity::seat_members_for_source(
            pool,
            ch.account_id,
            ch.team_id,
            &matched_user_ids,
            &source,
            history_from,
        )
        .await;
    stats.granted = granted;

    // A founding pass that actually seated someone makes the founding
    // durable — from here on, newly discovered members are late joiners. A
    // pass that seated NOBODY (roster resolved but wholly unmatched) leaves
    // the source unfounded so a later sweep can still seat the real
    // founding roster with full history.
    if history_from.is_none() && granted > 0 {
        if let Err(e) =
            crate::seats::record_source_founding(pool, ch.account_id, ch.team_id, &source).await
        {
            // Non-fatal: the belt-and-braces grant-existence signal in
            // `founding_history_from` classifies correctly until a later
            // sweep records the marker.
            tracing::warn!(
                error = %e,
                account_id = %ch.account_id,
                team_id = %ch.team_id,
                channel_id = %ch.channel_id,
                "slack seat reconciler: recording source founding failed — continuing"
            );
        }
    }

    // ── Revoke side ───────────────────────────────────────────────────────
    // Everyone this channel still vouches for who is no longer in it.
    let vouched =
        crate::seats::users_granted_by_source(pool, ch.account_id, ch.team_id, &source)
            .await
            .map_err(|e| format!("grant listing failed: {e}"))?;

    let still_present: std::collections::HashSet<Uuid> = matched_user_ids.into_iter().collect();

    for user_id in vouched {
        if still_present.contains(&user_id) {
            continue;
        }
        match crate::seats::revoke_seat_grant(pool, ch.account_id, ch.team_id, user_id, &source)
            .await
        {
            Ok(crate::seats::RevokeOutcome::SeatRevoked) => {
                stats.revoked += 1;
                // Connector compliance audit (m365 enterprise train PR 4.2):
                // system actor, durable-queue posture — same reasoning as
                // `workers::teams_seat_reconcile`'s identical emit point.
                crate::services::connector_audit::record_or_queue(
                    pool,
                    crate::services::connector_audit::AuditEntry {
                        account_id: ch.account_id,
                        actor: crate::services::connector_audit::Actor::System,
                        action: "seat_revoke",
                        provider: Some("slack".to_string()),
                        subject: Some(user_id.to_string()),
                        detail: Some(serde_json::json!({
                            "team_id": ch.team_id,
                            "channel_id": ch.channel_id,
                        })),
                    },
                )
                .await;
            }
            Ok(_) => stats.kept += 1,
            Err(e) => {
                // One member's failure must not abandon the rest of the
                // channel — same per-member fault isolation as seating.
                tracing::warn!(
                    error = %e,
                    account_id = %ch.account_id,
                    team_id = %ch.team_id,
                    %user_id,
                    channel_id = %ch.channel_id,
                    "seat reconciler: revocation failed for one member — continuing"
                );
            }
        }
    }

    Ok(stats)
}

pub(crate) async fn process_job(
    tenant: &TenantId,
    control_pool: &PgPool,
    pool: &PgPool,
    http: &reqwest::Client,
    account_id: Option<Uuid>,
    channel_id: Option<String>,
) -> Result<ReconcileStats, String> {
    let accounts = accounts_to_sweep(tenant, control_pool, account_id).await?;

    let mut channels: Vec<SeatBearingChannel> = Vec::new();
    for acct in accounts {
        // The batch cap is global across the sweep, not per account, so one
        // account with hundreds of routed channels cannot starve the rest by
        // consuming every job execution — and the cap still bounds the
        // worst-case Slack API burst per job.
        let remaining = CHANNEL_BATCH_SIZE - channels.len() as i64;
        if remaining <= 0 {
            break;
        }
        match seat_bearing_channels_for_account(pool, acct, channel_id.as_deref(), remaining).await
        {
            Ok(mut c) => channels.append(&mut c),
            Err(e) => {
                // One account's listing failing must not abandon the sweep —
                // the others are independent.
                tracing::warn!(
                    error = %e,
                    account_id = %acct,
                    "seat reconciler: channel listing failed for one account — continuing"
                );
            }
        }
    }

    if channels.is_empty() {
        tracing::debug!("seat reconciler: no seat-bearing channels in scope");
        return Ok(ReconcileStats::default());
    }

    let mut total = ReconcileStats::default();
    for ch in &channels {
        match reconcile_channel(control_pool, pool, tenant, http, ch).await {
            Ok(s) => {
                total.granted += s.granted;
                total.revoked += s.revoked;
                total.kept += s.kept;
                tracing::info!(
                    account_id = %ch.account_id,
                    team_id = %ch.team_id,
                    channel_id = %ch.channel_id,
                    granted = s.granted,
                    revoked = s.revoked,
                    kept = s.kept,
                    "seat reconciler: channel reconciled"
                );
            }
            Err(e) => {
                total.skipped_channels += 1;
                // Deliberately not an error-level alarm on its own: a
                // rate-limited or unreachable channel is expected
                // occasionally, and the correct response — change nothing —
                // has already happened. It IS surfaced, because a channel
                // that skips forever is a real problem.
                tracing::warn!(
                    error = %e,
                    account_id = %ch.account_id,
                    channel_id = %ch.channel_id,
                    "seat reconciler: channel SKIPPED, no seats changed (an unknown roster is \
                     never treated as an empty one)"
                );
            }
        }
    }

    tracing::info!(
        channels = channels.len(),
        granted = total.granted,
        revoked = total.revoked,
        kept = total.kept,
        skipped = total.skipped_channels,
        "seat reconciler: sweep complete"
    );

    Ok(total)
}

/// One `SlackSeatReconcile` pass on one tenant's `(pool, store)` pair:
/// dequeue up to [`JOB_BATCH_SIZE`] jobs and process them. Driven by
/// `workers::tenant_harness::run_multi_tenant` as a queue worker
/// (`HarnessConfig::queue_worker`), which owns the loop, the empty-poll
/// sleep, per-tenant error backoff, and shutdown (see that module's doc for
/// the full contract — this worker no longer owns its own loop or shutdown
/// check, exactly like `workers::slack_ingest`/`workers::huddle`).
///
/// `control_pool` is the shared control/consumer pool — needed for the
/// account roster and the Slack bot token (see this module's doc comment);
/// every seat read/write runs on `t.pool`.
pub async fn tick(t: &WorkerTenant, control_pool: &PgPool, http: &reqwest::Client) -> Result<Tick, String> {
    let jobs = queue::dequeue(&t.pool, &["SlackSeatReconcile"], JOB_BATCH_SIZE)
        .await
        .map_err(|e| format!("dequeue failed: {e}"))?;

    if jobs.is_empty() {
        return Ok(Tick::idle());
    }

    let n = jobs.len();
    for job in jobs {
        // `JobRow` stores `kind` and `payload` separately; reassembling
        // the internally-tagged shape is how every other worker decodes
        // its own variant (see `slack_name_backfill::parse_filter`).
        let tagged = serde_json::json!({ "kind": job.kind, "payload": job.payload });
        let (account_id, channel_id) = match serde_json::from_value::<JobKind>(tagged) {
            Ok(JobKind::SlackSeatReconcile { account_id, channel_id }) => {
                (account_id, channel_id)
            }
            Ok(_) => {
                tracing::error!(job_id = %job.id, "slack-seat-reconcile: wrong job kind dequeued");
                queue::fail(&t.pool, job.id, "wrong job kind").await.ok();
                continue;
            }
            Err(e) => {
                tracing::error!(job_id = %job.id, error = %e, "slack-seat-reconcile: malformed payload");
                queue::fail(&t.pool, job.id, &format!("malformed payload: {e}")).await.ok();
                continue;
            }
        };

        match process_job(&t.tenant, control_pool, &t.pool, http, account_id, channel_id).await {
            Ok(stats) => {
                // `ReconcileStats::default()` is exactly `process_job`'s
                // "no seat-bearing channels in scope" early return —
                // otherwise this sweep compared at least one channel against
                // Slack (a `kept` channel still means the comparison ran, not
                // that nothing happened).
                let job_outcome = if stats == ReconcileStats::default() {
                    queue::JobOutcome::NothingToDo
                } else {
                    queue::JobOutcome::DidWork
                };
                if let Err(e) = queue::complete(&t.pool, job.id, job_outcome).await {
                    tracing::error!(error = %e, job_id = %job.id, "slack-seat-reconcile: complete failed");
                }
            }
            Err(e) => {
                tracing::error!(error = %e, job_id = %job.id, "slack-seat-reconcile: job failed");
                queue::fail(&t.pool, job.id, &e).await.ok();
            }
        }
    }
    Ok(Tick::worked(n))
}
