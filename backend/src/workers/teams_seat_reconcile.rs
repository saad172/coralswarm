//! Teams seat reconciler — the Microsoft analog of `workers::
//! slack_seat_reconcile` (m365 Train 2 PR 2.4, decision #3).
//!
//! Slack's reconciler exists because the live `member_joined_channel`/
//! `member_left_channel` event path cannot reach three populations: channels
//! routed before `team_seat_grants` existed, channels the bot joined before a
//! destination picker shipped, and anything missed while the backend was
//! down. Teams has no live channel-roster-membership event at all (Graph
//! reports message/subscription changes, not team membership changes), so
//! this worker is the ONLY seating mechanism for Teams — not merely a
//! backstop. It re-walks every seat-bearing channel's roster on a hand-
//! inserted job, exactly like `SlackSeatReconcile`.
//!
//! # What "seat-bearing" means here — a deliberately empty set today
//!
//! A `teams_watched_resources` row is seat-bearing when its `graph_id` is a
//! graph some `teams` row owns (`teams.graph_id = twr.graph_id`) — the exact
//! join shape `slack_seat_reconcile`'s `SeatBearingChannel` discovery uses
//! for `slack_watched_channels`. Today, EVERY `teams_watched_resources.graph_id`
//! is written by `routes::teams_ingest::install_watch` via
//! `routes::slack::resolve_primary_graph`, which resolves the connecting
//! member's own primary ocean (or, under `IngestCaller::NonAttributable` on
//! the org plane, the org-wide commons primary) — NEVER a `kind='team'`
//! graph. So this worker's discovery query returns zero rows in production
//! today, structurally and deliberately: there is no Teams destination
//! picker yet that lets an admin route a channel into a named team ocean
//! (the Slack analog of that picker predates org-tenancy entirely).
//!
//! This is the exact precedent `migrations_tenant/0013_slack_watched_
//! channels.sql`'s own header documents for its `slack_watched_channels`
//! table: "populated only by the future destination picker, not by any code
//! shipped in this migration's PR". The wiring here is shipped ahead of that
//! picker so the moment a follow-up PR starts writing a team-ocean
//! `graph_id` into `teams_watched_resources`, seating "just works" with no
//! further plumbing — exactly like Slack's `team_seat_grants` reconciler was
//! ready before its own picker's org-plane rollout completed.
//!
//! # Roster-abort semantics (copied from Slack, not reinvented)
//!
//! Granting and revoking are NOT symmetric, for the same reason
//! `slack_seat_reconcile`'s header gives: a grant is additive and idempotent
//! (re-asserting one that already exists is free), so the sweep grants
//! generously. A revocation removes access, so it only ever fires for a
//! channel whose roster was walked IN FULL —
//! `connectors::microsoft::resolve_team_roster_members` propagates `Err`
//! (never an empty matched list standing in for "we could not tell") the
//! moment the member-token load, the Graph roster call, or a database
//! lookup fails mid-walk, and that `Err` here
//! means "we do not know", which is never treated as "the roster is empty,
//! revoke everyone" — the channel is skipped, unchanged, same as Slack's.
//! There is no negative caching anywhere in this path: a roster walk that
//! fails now is retried in full on the very next sweep.
//!
//! CS-429: a roster member Graph reports with no readable `email` (cause
//! unconfirmed per-member — the roster request has no `userType`/license
//! data, see `connectors::microsoft::TeamRosterMember::email`'s own doc
//! comment) is `Unavailable`, not "guest/unmatched" —
//! `resolve_team_roster_members` counts it separately and emits its own
//! `connector_health` warning naming how many. This is a CLASSIFICATION +
//! OBSERVABILITY fix only: an `Unavailable` member is still absent from
//! `matched_user_ids` exactly like an ordinary non-match, so a
//! previously-seated member whose address later becomes unreadable still
//! loses their seat on the next sweep — this reconciler cannot tell "their
//! address became unreadable" apart from "left the team" from the
//! matched-id list alone, and this fix does not change that (the fan-out/
//! seat-grant design itself is out of scope here — a held PR reworks it).
//! What CS-429 buys is that the drop is no longer SILENT: the warning names
//! the unreadable-address count instead of reading as an ordinary roster
//! change.
//!
//! # Two pools, tenant loop (mirrors `slack_seat_reconcile` exactly)
//!
//! `JobKind::TeamsSeatReconcile` has no live enqueue site — hand-inserted,
//! same as `SlackSeatReconcile` (see that variant's own doc comment for the
//! `INSERT INTO jobs` shape; this one's is identical with the kind swapped).
//! `control_pool` resolves the account roster (`accounts`/`tenant_databases`)
//! and is the pool `connectors::microsoft::resolve_team_roster_members`
//! passes as ITS `control_pool` (the CONTROL-only `users`/`account_members`
//! lookups). The roster itself is read with a connected member's delegated
//! Microsoft token (`TeamMember.Read.All`), never an app-only token.
//! Every seat read/write (`teams_watched_resources`, `teams`, `team_members`,
//! `team_seat_grants`) runs on `t.pool`, this tick's own content pool, under
//! `tenancy::scope::begin_tenant_system_scoped` — the same "no acting end
//! user, tenant-wide sweep" scope `record_seat_grant`/`revoke_seat_grant`
//! themselves already require (see `seats.rs`'s own doc comments).

use std::collections::HashSet;

use sqlx::PgPool;
use uuid::Uuid;

use crate::connectors::microsoft;
use crate::jobs::{queue, JobKind};
use crate::seats;
use crate::tenancy::scope::begin_tenant_system_scoped;
use crate::tenancy::TenantId;
use crate::workers::tenant_harness::{Tick, WorkerTenant};

/// Channels reconciled per job execution — same value and the same "GLOBAL
/// across the whole sweep, no continuation job, a known contained gap" caveat
/// as `slack_seat_reconcile::CHANNEL_BATCH_SIZE`'s own doc comment. Not
/// re-solved here for the same reason it wasn't there: a genuinely empty
/// discovery query in production today (see this module's own header) makes
/// the cap moot until a destination picker exists to populate it.
const CHANNEL_BATCH_SIZE: i64 = 20;

const JOB_BATCH_SIZE: usize = 5;
/// Tenant harness idle-poll floor for this worker.
pub const EMPTY_POLL_SLEEP: std::time::Duration = std::time::Duration::from_secs(30);

/// One seat-bearing Teams channel: a `teams_watched_resources` row whose
/// `graph_id` some `teams` row owns.
#[derive(Debug, Clone)]
struct SeatBearingChannel {
    account_id: Uuid,
    /// Microsoft AAD tenant id — the app-token audience for the roster walk.
    tenant_id: String,
    /// Graph team id (AAD group id) — the roster-walk target itself.
    ms_team_id: String,
    /// `teams_watched_resources.channel_id` — feeds the seat `source` string.
    channel_id: String,
    /// The CoralSwarm team ocean this channel routes into.
    team_id: Uuid,
}

/// What one channel's reconciliation did.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ReconcileStats {
    pub granted: usize,
    pub revoked: usize,
    pub kept: usize,
    pub skipped_channels: usize,
}

/// Accounts this sweep will visit — byte-identical logic and rationale to
/// `slack_seat_reconcile::accounts_to_sweep` (see that function's own doc
/// comment for the full "why a cross-account sweep needs `accounts` read on
/// `control_pool`, and why a supplied `account_id` is checked, not trusted,
/// on an org tenant" reasoning). Not shared as a common helper: `workers::
/// graph_subscriptions`'s own `resolve_tenant_account` is the established
/// precedent for each tenant-loop worker keeping its own small copy rather
/// than introducing a cross-worker dependency for a handful of lines.
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
                None => Err(format!("no tenant_databases row for org {org_id}")),
            }
        }
    }
}

/// The seat-bearing Teams channels of ONE account. `begin_tenant_system_
/// scoped` for the same reason `slack_seat_reconcile::seat_bearing_channels_
/// for_account` uses it: `teams`/`team_seat_grants` sit on the tenant plane's
/// `member_ocean_isolation` policy (anchored via `tenant_rls_visible_graph_
/// ids()`, which reads `app.tenant_actor`/`app.user_id` — neither of which
/// plain `begin_account_scoped` sets), and this sweep has no single acting
/// end user to present.
async fn seat_bearing_channels_for_account(
    pool: &PgPool,
    account_id: Uuid,
    channel_id: Option<&str>,
    limit: i64,
) -> Result<Vec<SeatBearingChannel>, sqlx::Error> {
    let mut tx = begin_tenant_system_scoped(pool, account_id)
        .await
        .map_err(|e| sqlx::Error::Protocol(format!("seat_bearing_channels scoped tx: {e}")))?;

    let rows: Vec<(String, String, String, Uuid)> = sqlx::query_as(
        "SELECT twr.tenant_id, twr.team_id, twr.channel_id, t.id \
         FROM teams_watched_resources twr \
         JOIN teams t ON t.graph_id = twr.graph_id AND t.account_id = twr.account_id \
         WHERE twr.account_id = $1 AND twr.kind = 'channel' \
           AND ($2::text IS NULL OR twr.channel_id = $2) \
         ORDER BY twr.channel_id \
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
        .map(|(tenant_id, ms_team_id, channel_id, team_id)| SeatBearingChannel {
            account_id,
            tenant_id,
            ms_team_id,
            channel_id,
            team_id,
        })
        .collect())
}

/// Test-only public alias for [`seat_bearing_channels_for_account`] — same
/// idiom as `slack_seat_reconcile::test_support_seat_bearing_channel_count`,
/// needed because `backend/tests/*.rs` links this crate as an ordinary
/// external dependency and cannot see a private module function.
#[cfg(feature = "test-support")]
pub async fn test_support_seat_bearing_channel_count(
    pool: &PgPool,
    account_id: Uuid,
) -> Result<usize, sqlx::Error> {
    Ok(seat_bearing_channels_for_account(pool, account_id, None, CHANNEL_BATCH_SIZE)
        .await?
        .len())
}

/// Reconcile one channel: grants for everyone the roster walk currently
/// matches, revocations for everyone this channel still vouches for who the
/// walk no longer finds.
async fn reconcile_channel(
    control_pool: &PgPool,
    pool: &PgPool,
    http: &reqwest::Client,
    tenant: &crate::tenancy::TenantId,
    ch: &SeatBearingChannel,
) -> Result<ReconcileStats, String> {
    let mut stats = ReconcileStats::default();

    // `Err` here means the roster is UNKNOWN — see this module's header.
    // Propagating it skips the channel and changes nothing. Rate-limit scope
    // (m365 Train 2 PR 2.6) keyed via `rate_key` on `ch.tenant_id` (CodeRabbit
    // review, PR #673 — the walk uses the connected member's delegated
    // token, rate-limited on this account and tenant) — see
    // `graph_rate_limit`'s module doc; a saturated limiter surfaces here as
    // `Err` (the roster walk aborts wholesale, per this module's own
    // `Unavailable ≠ Unmatched` semantics), never as a silently empty roster.
    //
    // CS-429: `_unavailable` (roster members whose address could not be
    // read — never folded into a guest/unmatched count) is disclosed by
    // `resolve_team_roster_members` itself via its own `connector_health`
    // warning (cooldown per Microsoft team); this reconciler's job is
    // granting/revoking seats, so it does not need the count again here.
    let dek = crate::crypto::DekPlane::from_tenant(tenant, pool.clone(), ch.account_id);
    let resolved = crate::graph_rate_limit::scoped(
        crate::graph_rate_limit::rate_key(ch.account_id, &ch.tenant_id),
        microsoft::resolve_team_roster_members(
            control_pool,
            pool,
            http,
            &dek,
            ch.account_id,
            &ch.tenant_id,
            &ch.ms_team_id,
        ),
    )
    .await?;
    let matched_user_ids = resolved.matched_user_ids;
    // Later roster walks (not only the Microsoft connect callback) enqueue
    // the invite job. A pending job with the same key is a no-op.
    if let Err(e) = queue::enqueue(
        pool,
        &JobKind::EmailUnmatchedTeamsMembers {
            account_id: ch.account_id,
            tenant_id: ch.tenant_id.clone(),
            team_id: ch.ms_team_id.clone(),
        },
        Some(ch.account_id),
        Some(&format!(
            "teams-unmatched:{}:{}",
            ch.account_id, ch.ms_team_id
        )),
        0,
    )
    .await
    {
        tracing::warn!(
            error = %e,
            account_id = %ch.account_id,
            team_id = %ch.ms_team_id,
            "teams invite: enqueue from seat reconcile failed"
        );
    }

    let source = seats::teams_channel_source(&ch.channel_id);

    // Founding sweep vs late joiner (m365 enterprise train PR 4.1) — same
    // rule and same reasoning as `slack_seat_reconcile::reconcile_channel`'s
    // identical pre-check (see that function's comments for the full
    // rationale, and `founding_history_from`'s doc for why "founded" is a
    // durable `seat_grant_sources` marker rather than grant-emptiness):
    // `None` (full history) only for the pass that seats the founding
    // roster; `Some(now())` for a member discovered any time after.
    let history_from = seats::founding_history_from(pool, ch.account_id, ch.team_id, &source)
        .await
        .map_err(|e| format!("founding check failed (pre-sweep): {e}"))?;

    // ── Grant side ────────────────────────────────────────────────────────
    // `record_seat_grant` is idempotent (`ON CONFLICT ... DO NOTHING`), so
    // this both re-asserts existing grants and seats anyone new the roster
    // walk found — one pass covers both, same as Slack's sweep.
    for user_id in &matched_user_ids {
        match seats::record_seat_grant(
            pool,
            ch.account_id,
            ch.team_id,
            *user_id,
            &source,
            history_from,
        )
        .await
        {
            Ok(inserted) => {
                stats.granted += 1;
                if inserted {
                    // Connector compliance audit (m365 enterprise train PR
                    // 4.2, plan decision #11 part 1): only for a GENUINE new
                    // grant, not every idempotent re-assertion this sweep
                    // performs on every tick — see `seats::record_seat_grant`'s
                    // own doc comment. System actor: this reconciler tick has
                    // no member session to attribute the grant to. Durability
                    // tier 3 (`record_or_queue`) — a mid-sweep audit failure
                    // must not abort the reconciler's own state changes; the
                    // documented residual is the next tick re-asserting the
                    // SAME grant (a no-op on `team_seat_grants` since it
                    // already exists) without re-emitting this row, an
                    // accepted narrow gap for this PR (see the module's own
                    // doc comment on `record_or_queue`).
                    crate::services::connector_audit::record_or_queue(
                        pool,
                        crate::services::connector_audit::AuditEntry {
                            account_id: ch.account_id,
                            actor: crate::services::connector_audit::Actor::System,
                            action: "seat_grant",
                            provider: Some("microsoft".to_string()),
                            subject: Some(user_id.to_string()),
                            detail: Some(serde_json::json!({
                                "team_id": ch.team_id,
                                "channel_id": ch.channel_id,
                            })),
                        },
                    )
                    .await;
                }
            }
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    account_id = %ch.account_id,
                    team_id = %ch.team_id,
                    user_id = %user_id,
                    channel_id = %ch.channel_id,
                    "teams seat reconciler: grant failed for one member — continuing"
                );
            }
        }
    }

    // Founding becomes durable once a founding pass actually seated someone
    // — same rule (and same non-fatal posture) as the Slack reconciler.
    if history_from.is_none() && stats.granted > 0 {
        if let Err(e) =
            seats::record_source_founding(pool, ch.account_id, ch.team_id, &source).await
        {
            tracing::warn!(
                error = %e,
                account_id = %ch.account_id,
                team_id = %ch.team_id,
                channel_id = %ch.channel_id,
                "teams seat reconciler: recording source founding failed — continuing"
            );
        }
    }

    // ── Revoke side ───────────────────────────────────────────────────────
    let vouched = seats::users_granted_by_source(pool, ch.account_id, ch.team_id, &source)
        .await
        .map_err(|e| format!("grant listing failed: {e}"))?;

    let still_present: HashSet<Uuid> = matched_user_ids.into_iter().collect();

    for user_id in vouched {
        if still_present.contains(&user_id) {
            continue;
        }
        match seats::revoke_seat_grant(pool, ch.account_id, ch.team_id, user_id, &source).await {
            Ok(seats::RevokeOutcome::SeatRevoked) => {
                stats.revoked += 1;
                // Connector compliance audit — same system-actor,
                // durable-queue posture as the grant side above. Only on a
                // genuine `SeatRevoked` (not `GrantDropped`/`AdminPreserved`/
                // `NotOurs`, which are not access changes).
                crate::services::connector_audit::record_or_queue(
                    pool,
                    crate::services::connector_audit::AuditEntry {
                        account_id: ch.account_id,
                        actor: crate::services::connector_audit::Actor::System,
                        action: "seat_revoke",
                        provider: Some("microsoft".to_string()),
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
                tracing::warn!(
                    error = %e,
                    account_id = %ch.account_id,
                    team_id = %ch.team_id,
                    %user_id,
                    channel_id = %ch.channel_id,
                    "teams seat reconciler: revocation failed for one member — continuing"
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
        let remaining = CHANNEL_BATCH_SIZE - channels.len() as i64;
        if remaining <= 0 {
            break;
        }
        match seat_bearing_channels_for_account(pool, acct, channel_id.as_deref(), remaining).await
        {
            Ok(mut c) => channels.append(&mut c),
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    account_id = %acct,
                    "teams seat reconciler: channel listing failed for one account — continuing"
                );
            }
        }
    }

    if channels.is_empty() {
        tracing::debug!("teams seat reconciler: no seat-bearing channels in scope");
        return Ok(ReconcileStats::default());
    }

    let mut total = ReconcileStats::default();
    for ch in &channels {
        match reconcile_channel(control_pool, pool, http, tenant, ch).await {
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
                    "teams seat reconciler: channel reconciled"
                );
            }
            Err(e) => {
                total.skipped_channels += 1;
                tracing::warn!(
                    error = %e,
                    account_id = %ch.account_id,
                    channel_id = %ch.channel_id,
                    "teams seat reconciler: channel SKIPPED, no seats changed (an unknown roster \
                     is never treated as an empty one)"
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
        "teams seat reconciler: sweep complete"
    );

    Ok(total)
}

/// One `TeamsSeatReconcile` pass on one tenant's `(pool, store)` pair.
/// Driven by `workers::tenant_harness::run_multi_tenant` as a queue worker
/// (`HarnessConfig::queue_worker`) — same contract as `workers::
/// slack_seat_reconcile::tick`.
///
/// `control_pool` is the shared control/consumer pool — needed for the
/// account roster and the CONTROL-only halves of `connectors::microsoft::
/// resolve_team_roster_members` (see this module's own doc comment); every
/// seat read/write runs on `t.pool`.
pub async fn tick(t: &WorkerTenant, control_pool: &PgPool, http: &reqwest::Client) -> Result<Tick, String> {
    let jobs = queue::dequeue(&t.pool, &["TeamsSeatReconcile"], JOB_BATCH_SIZE)
        .await
        .map_err(|e| format!("dequeue failed: {e}"))?;

    if jobs.is_empty() {
        return Ok(Tick::idle());
    }

    let n = jobs.len();
    for job in jobs {
        let tagged = serde_json::json!({ "kind": job.kind, "payload": job.payload });
        let (account_id, channel_id) = match serde_json::from_value::<JobKind>(tagged) {
            Ok(JobKind::TeamsSeatReconcile { account_id, channel_id }) => (account_id, channel_id),
            Ok(_) => {
                tracing::error!(job_id = %job.id, "teams-seat-reconcile: wrong job kind dequeued");
                queue::fail(&t.pool, job.id, "wrong job kind").await.ok();
                continue;
            }
            Err(e) => {
                tracing::error!(job_id = %job.id, error = %e, "teams-seat-reconcile: malformed payload");
                queue::fail(&t.pool, job.id, &format!("malformed payload: {e}")).await.ok();
                continue;
            }
        };

        match process_job(&t.tenant, control_pool, &t.pool, http, account_id, channel_id).await {
            Ok(stats) => {
                let job_outcome = if stats == ReconcileStats::default() {
                    queue::JobOutcome::NothingToDo
                } else {
                    queue::JobOutcome::DidWork
                };
                if let Err(e) = queue::complete(&t.pool, job.id, job_outcome).await {
                    tracing::error!(error = %e, job_id = %job.id, "teams-seat-reconcile: complete failed");
                }
            }
            Err(e) => {
                tracing::error!(error = %e, job_id = %job.id, "teams-seat-reconcile: job failed");
                queue::fail(&t.pool, job.id, &e).await.ok();
            }
        }
    }
    Ok(Tick::worked(n))
}
