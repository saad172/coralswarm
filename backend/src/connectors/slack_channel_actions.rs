//! Slack channel management — WRITE half. `PUT /v1/connectors/slack/
//! channels/{channel_id}/destination` (change destination), `DELETE
//! /v1/connectors/slack/channels/{channel_id}` (stop watching), and `GET
//! /v1/connectors/slack/channels/{channel_id}/impact` (real numbers for the
//! confirmation dialog in front of either action) — the mutating sibling of
//! [`super::slack_channel_management`]'s read-only detail page. Handlers
//! live in `routes::slack`; this module is the DB/HTTP-touching business
//! logic behind them, same split that module and `slack_onboarding` use.
//!
//! # ACTION 1 — change destination, and why it breaks contract I2 on purpose
//!
//! `slack_watched_channels` (migration `0101`) was built as an
//! insert-only table: [`crate::connectors::slack::ensure_slack_watched_channel`]'s
//! `ON CONFLICT ... DO NOTHING` is a direct read of contract I2 — "one
//! container, one destination, FIXED at authorisation time" — and every
//! existing write site (the Block-Kit picker in `routes::slack_interactivity`)
//! honours that by design: a second pick against an already-routed channel
//! silently no-ops.
//!
//! This module's [`upsert_channel_destination`] does NOT honour that. It is
//! a genuine `ON CONFLICT ... DO UPDATE`, and the founder decision behind it
//! (wt-slack-actions brief, 2026-08-04) is explicit: **a human may change a
//! channel's destination, but only for content ingested AFTER the change.**
//! Already-captured atoms are never moved, re-tagged, or re-embedded — this
//! module never touches the `atoms` table at all, only the routing row. The
//! very next message or huddle capture that resolves this channel's
//! destination (`resolve_channel_destinations`) is the first thing that
//! sees the new pick.
//!
//! That is a real, deliberate contradiction of I2's own wording, not an
//! oversight — which is why it is spelled out here AND at
//! [`upsert_channel_destination`] itself (the actual write site), rather
//! than left for a future reader to infer from an `ON CONFLICT DO UPDATE`
//! that looks, at a glance, like a copy-paste of the picker's insert.
//!
//! ## A known, deliberate gap: re-routing away from a team ocean never revokes seats
//!
//! [`set_channel_destination`] writes `team_seat_grants` forward — a
//! `Single` pick that resolves to a `kind = 'team'` graph auto-seats that
//! channel's roster (see "Admin enforcement and seating are reused" below).
//! Nothing here looks up the PREVIOUS destination before overwriting it, and
//! there is no revoke call anywhere in this module: a channel re-routed
//! AWAY from a team ocean (to a different ocean, to fan-out, or cleared back
//! to the account default) leaves that team's `slack:{channel_id}`-sourced
//! `team_seat_grants` rows exactly as they were.
//!
//! This is intentional, not an oversight, and it is deferred rather than
//! fixed here on purpose: [`crate::seats::revoke_seat_grant`]'s own header
//! (`seats.rs`) states the rule this module is built to respect rather than
//! route around — **revocation only ever fires as a consequence of deleting
//! a grant row we ourselves recorded**, and absence of provenance means "not
//! ours to revoke", never "safe to revoke". Teaching `set_channel_destination`
//! to revoke on re-route would mean resolving the OLD destination, deciding
//! whether this channel was the grant's only reason, and calling
//! `revoke_seat_grant` correctly for every affected member — real logic with
//! its own edge cases (a member seated by two routed channels into the same
//! team, a re-route racing the reconcile sweep in `slack_seat_reconcile`),
//! not a one-line addition to an already-large branch. Seat revocation in
//! this codebase has already produced subtle bugs once; it deserves its own
//! reviewed change, not a rider on this one. Until that lands, a stale seat
//! is the safe default: it leaves a former team member with read access to
//! an ocean their channel no longer feeds, which is a strictly smaller harm
//! than the alternative of a revoke path that guesses wrong and drops
//! someone's legitimately-still-earned seat.
//!
//! # ACTION 2 — stop watching, and the ordering invariant
//!
//! [`stop_watching_channel_after_leave`] is split from the Slack HTTP call
//! specifically so the one property that matters — the routing row is
//! deleted if and only if `conversations.leave` succeeded (or the bot was
//! already gone) — is unit-testable without a live network call. See that
//! function's own doc comment for the failure mode this prevents.
//!
//! # RLS enforces too, now — the explicit predicates are belt-and-braces, not a hope
//!
//! `migrations_control/0083_consumer_rls.sql`'s own header used to record
//! that the app connects as its owner/`BYPASSRLS` role, with the cutover to
//! the non-`BYPASSRLS` `coralswarm_app` role (OPS-4) still pending. That
//! cutover has since run — verified directly against production:
//! `DATABASE_URL`'s user IS `coralswarm_app`, `pg_roles.rolbypassrls` is
//! `false` for it, and an unscoped query against an RLS-covered table
//! genuinely ERRORS (`unrecognized configuration parameter
//! "app.account_id"`) rather than silently returning every account's rows.
//! RLS is a real, live second layer today, not a someday one.
//!
//! Every `begin_account_scoped`/`begin_tenant_system_scoped` call below still
//! carries its own explicit `account_id = $n` predicate anyway — not because
//! RLS is inert, but because this module is a REST surface a caller can
//! point at an arbitrary `graph_id`/`channel_id`, and this module's own
//! ownership/admin checks (`GraphNotOwned`, `NotTeamAdmin`, cross-account
//! `graph_id`s, etc.) need to fail with a specific, actionable error rather
//! than a raw Postgres exception either layer alone would produce. Both
//! layers now agree on the boundary; see each function's own comment for
//! which row its explicit predicate is protecting.
//!
//! # Admin enforcement and seating are reused, never re-implemented
//!
//! [`set_channel_destination`] calls
//! [`crate::routes::slack_interactivity::check_team_ocean_admin`] and
//! [`crate::routes::slack_interactivity::auto_seat_channel_members`]
//! directly — the SAME functions the Slack Block-Kit picker's
//! `cs_dest_org_team_existing` arm calls. A second admin check or a second
//! seating write here is exactly how the two entry points (Slack
//! interactivity vs. this REST surface) would quietly drift on who counts
//! as an admin or who gets seated, and `team_seat_grants` provenance
//! (migration `0106`) would stop being trustworthy the moment two code
//! paths wrote it slightly differently.
//!
//! # No HTTP-mock harness
//!
//! Same constraint `slack_channel_management`'s own header names: this
//! crate has no way to fake a Slack HTTP response, so every function here
//! that would otherwise need one is split into an impure orchestrator (real
//! HTTP + DB) and a pure/DB-only core that takes the HTTP call's OUTPUT as a
//! parameter — [`stop_watching_channel`] / [`stop_watching_channel_after_leave`]
//! is the shape to copy. `set_channel_destination`'s admin-refusal and
//! ownership-refusal paths need no such split: they return before any
//! network call is made at all, so they are directly pg-testable.

use sqlx::PgPool;
use uuid::Uuid;

use crate::connectors::slack::{self, ChannelDestination, LeaveError, LeaveOutcome};
use crate::routes::slack::AccountPlane;
use crate::routes::slack_interactivity::{auto_seat_channel_members, check_team_ocean_admin, TeamOceanCheck};

// ── ACTION 1: change destination ────────────────────────────────────────────

/// A destination pick submitted through `PUT /v1/connectors/slack/channels/
/// {channel_id}/destination` — the REST equivalent of
/// [`crate::connectors::slack::PickedDestination`], with one extra option
/// that write path never needed: `Default`, "stop pointing this channel
/// anywhere explicit, fall back to the account default."
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DestinationPick {
    /// One specific ocean, named by `graph_id`.
    Single(Uuid),
    /// "Everyone's individual oceans" — org plane only.
    Fanout,
    /// Delete the routing row; the channel resolves through
    /// [`crate::connectors::slack::resolve_channel_destinations`]'s no-row
    /// branch from here on.
    Default,
}

/// Why [`set_channel_destination`] refused a pick.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SetDestinationError {
    /// `graph_id` does not exist, is archived, or does not belong to this
    /// account. The explicit ownership check the brief requires — see this
    /// module's header on why RLS alone cannot be trusted for this today.
    GraphNotOwned,
    /// docs/connectors/slack.md §2's destination table: the org ocean is
    /// never a valid Slack destination, on either plane.
    OrgOcean,
    /// A `kind = 'member'` graph that is NOT the caller's own. There is no
    /// picker option — Slack or REST — that lets one member route a channel
    /// straight into a DIFFERENT member's private ocean; fan-out is the
    /// only multi-person org option, and it seats people through the
    /// identity matcher, never by a caller naming someone else's ocean
    /// directly.
    NotYourOcean,
    /// `graph.kind == "team"` but no `teams` row references it for this
    /// account. Should be unreachable (a `team`-kind graph is always
    /// created together with its `teams` row via `create_org_team_ocean`),
    /// handled as a clean refusal rather than an `unwrap` on a route that
    /// also makes a live Slack call.
    TeamNotFound,
    /// `graph.kind == "team"`, the team is real and belongs to this
    /// account, but the caller does not administer it — contract I3,
    /// enforced by [`check_team_ocean_admin`], never re-implemented here.
    NotTeamAdmin { name: String },
    /// `Fanout` picked on a Personal account — "everyone's individual
    /// oceans" has no "everyone" to fan out to there. Mirrors
    /// `ensure_slack_watched_channel`'s identical plane guard.
    FanoutRequiresOrgPlane,
    /// A database error at any step. Carries the real message for the log;
    /// the HTTP layer decides what (if anything) is safe to surface.
    Db(String),
}

/// What [`set_channel_destination`] actually did, for the caller to report.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct SetDestinationOutcome {
    pub(crate) destination: ChannelDestination,
    /// `Some` only when the pick was `Single` against a `kind = 'team'`
    /// graph — the auto-seat step ran. `Ok((seated, unresolved))` mirrors
    /// [`auto_seat_channel_members`]'s own return shape; `Err` means the
    /// roster walk itself failed (see that function's doc comment) — the
    /// ROUTING pick still succeeded and is never rolled back on a seat
    /// failure, exactly like the Block-Kit picker's own
    /// `cs_dest_org_team_existing` arm.
    pub(crate) seat_result: Option<Result<(usize, usize), String>>,
}

/// Change (or clear) a Slack channel's explicit destination — the business
/// logic behind `PUT /v1/connectors/slack/channels/{channel_id}/destination`.
/// See this module's header for the I2 contradiction this deliberately
/// commits, and for why the admin check / auto-seat calls below are reused,
/// not re-implemented.
///
/// `user_id` is the AUTHENTICATED caller (`UserContext::user_id`) — unlike
/// every existing write site in `routes::slack_interactivity`, which only
/// ever has a Slack identity to resolve (and sometimes cannot), this is a
/// JWT-authenticated REST endpoint, so the acting user is never in doubt.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn set_channel_destination(
    control_pool: &PgPool,
    tenant_pool: &PgPool,
    http: &reqwest::Client,
    account_id: Uuid,
    plane: AccountPlane,
    user_id: Uuid,
    workspace_id: &str,
    channel_id: &str,
    bot_token: &str,
    pick: DestinationPick,
) -> Result<SetDestinationOutcome, SetDestinationError> {
    // `seat_team` is `Some(team_id)` only for a `Single` pick that resolved
    // to a `kind = 'team'` graph the caller administers — the one case that
    // needs the auto-seat step after the routing write commits.
    let (mode, graph_id, seat_team): (&'static str, Option<Uuid>, Option<Uuid>) = match pick {
        DestinationPick::Default => {
            // No admin/ownership check needed to CLEAR a pick — the caller
            // is giving up an explicit routing decision, not making one.
            delete_watched_channel_row(tenant_pool, account_id, workspace_id, channel_id)
                .await
                .map_err(|e| SetDestinationError::Db(e.to_string()))?;
            let destination = slack::resolve_channel_destinations(
                control_pool,
                tenant_pool,
                account_id,
                plane,
                workspace_id,
                channel_id,
            )
            .await
            .map_err(|e| SetDestinationError::Db(e.to_string()))?;
            return Ok(SetDestinationOutcome { destination, seat_result: None });
        }
        DestinationPick::Fanout => {
            if plane != AccountPlane::Org {
                return Err(SetDestinationError::FanoutRequiresOrgPlane);
            }
            ("fanout", None, None)
        }
        DestinationPick::Single(graph_id) => {
            let graph = fetch_graph_for_destination(tenant_pool, account_id, graph_id)
                .await
                .map_err(|e| SetDestinationError::Db(e.to_string()))?
                .ok_or(SetDestinationError::GraphNotOwned)?;

            match graph.kind.as_str() {
                "org" => return Err(SetDestinationError::OrgOcean),
                "team" => {
                    let team_id = fetch_team_id_for_graph(tenant_pool, account_id, graph_id)
                        .await
                        .map_err(|e| SetDestinationError::Db(e.to_string()))?
                        .ok_or(SetDestinationError::TeamNotFound)?;
                    // Reused verbatim — see this module's header. `tenant_pool`
                    // is the same parameter `check_team_ocean_admin` already
                    // takes at every existing call site.
                    match check_team_ocean_admin(tenant_pool, account_id, Some(user_id), team_id)
                        .await
                        .map_err(|e| SetDestinationError::Db(e.to_string()))?
                    {
                        TeamOceanCheck::Admin { .. } => ("single", Some(graph_id), Some(team_id)),
                        TeamOceanCheck::NotAdmin { name } => {
                            return Err(SetDestinationError::NotTeamAdmin { name })
                        }
                        TeamOceanCheck::NotFound => return Err(SetDestinationError::TeamNotFound),
                    }
                }
                "member" if graph.owner_user_id != Some(user_id) => {
                    return Err(SetDestinationError::NotYourOcean)
                }
                // Personal-kind graphs, or a `member`-kind graph that IS the
                // caller's own — no further check needed beyond the
                // ownership check `fetch_graph_for_destination` already ran.
                _ => ("single", Some(graph_id), None),
            }
        }
    };

    upsert_channel_destination(tenant_pool, account_id, user_id, workspace_id, channel_id, mode, graph_id)
        .await
        .map_err(|e| SetDestinationError::Db(e.to_string()))?;

    let destination = slack::slack_watched_destination(control_pool, tenant_pool, account_id, workspace_id, channel_id)
        .await
        .map_err(|e| SetDestinationError::Db(e.to_string()))?
        .ok_or_else(|| {
            SetDestinationError::Db(
                "set_channel_destination: row vanished immediately after its own upsert — should be unreachable"
                    .to_string(),
            )
        })?;

    // Auto-seat — ONLY for a Single pick that resolved to a team ocean, and
    // ONLY after the routing write has committed. Mirrors
    // `cs_dest_org_team_existing`'s ordering exactly: the routing pick is
    // reported as successful independent of whether seating succeeds,
    // because a seat failure does not un-route the channel — see
    // `SetDestinationOutcome::seat_result`'s own doc comment.
    //
    // What this does NOT do: look up whatever this channel's PREVIOUS
    // destination was and revoke the seats it granted there. A re-route away
    // from a team ocean leaves that team's `team_seat_grants` rows in place,
    // deliberately — see this module's header ("A known, deliberate gap")
    // for why that dormancy is the safe default, not a missed case.
    let seat_result = match seat_team {
        Some(team_id) => Some(
            auto_seat_channel_members(
                control_pool,
                tenant_pool,
                http,
                account_id,
                workspace_id,
                bot_token,
                channel_id,
                team_id,
            )
            .await,
        ),
        None => None,
    };

    Ok(SetDestinationOutcome { destination, seat_result })
}

/// One `graphs` row, trimmed to what a destination pick needs to check.
///
/// `pub(crate)` (gsuite Train G4 PR 6, "G4.6"): `routes::google_chat::
/// set_space_destination` reuses this shape (and [`fetch_graph_for_destination`]
/// below) directly for its own org-plane authorization check, rather than
/// re-querying `graphs` with a second, independently-maintained shape — same
/// "reused, never re-implemented" discipline this module's own header
/// documents for [`check_team_ocean_admin`]/[`auto_seat_channel_members`].
pub(crate) struct GraphRow {
    pub(crate) kind: String,
    pub(crate) owner_user_id: Option<Uuid>,
}

/// Resolve a caller-supplied `graph_id` INTO this account, explicitly. The
/// ownership check the brief requires: `graph_id` is caller-supplied over
/// HTTP with no a-priori guarantee it belongs to `account_id` — the exact
/// hazard `ensure_slack_watched_channel`'s own "BLOCKER 1 fix" doc comment
/// describes for the Slack-interactivity write path, reused here for the
/// REST one. `archived_at IS NULL`: an archived ocean is not a valid
/// destination to route new content into.
///
/// Scoped (`begin_account_scoped`, real RLS enforcement post-OPS-4 — see
/// this module's header) PLUS an explicit `account_id = $2` predicate as a
/// second, defence-in-depth layer, so a cross-account `graph_id` is refused
/// with `GraphNotOwned` rather than a raw RLS-driven exception either way.
pub(crate) async fn fetch_graph_for_destination(
    tenant_pool: &PgPool,
    account_id: Uuid,
    graph_id: Uuid,
) -> Result<Option<GraphRow>, sqlx::Error> {
    let mut tx = crate::tenancy::scope::begin_tenant_system_scoped(tenant_pool, account_id)
        .await
        .map_err(|e| sqlx::Error::Protocol(format!("fetch_graph_for_destination scoped tx: {e}")))?;
    let row: Option<(String, Option<Uuid>)> = sqlx::query_as(
        "SELECT kind, owner_user_id FROM graphs \
         WHERE id = $1 AND account_id = $2 AND archived_at IS NULL",
    )
    .bind(graph_id)
    .bind(account_id)
    .fetch_optional(&mut *tx)
    .await?;
    tx.rollback().await.ok();
    Ok(row.map(|(kind, owner_user_id)| GraphRow { kind, owner_user_id }))
}

/// Resolve a `team`-kind graph's `teams.id`, scoped to this account. Needed
/// because [`check_team_ocean_admin`]/[`auto_seat_channel_members`] both
/// key off `team_id`, not `graph_id` — the REST body only ever carries the
/// destination `graph_id`, so this is the one extra hop the Slack picker
/// (which starts from a `team_id` selected in its own dropdown) never
/// needed.
pub(crate) async fn fetch_team_id_for_graph(
    tenant_pool: &PgPool,
    account_id: Uuid,
    graph_id: Uuid,
) -> Result<Option<Uuid>, sqlx::Error> {
    let mut tx = crate::tenancy::scope::begin_tenant_system_scoped(tenant_pool, account_id)
        .await
        .map_err(|e| sqlx::Error::Protocol(format!("fetch_team_id_for_graph scoped tx: {e}")))?;
    let team_id: Option<Uuid> =
        sqlx::query_scalar("SELECT id FROM teams WHERE graph_id = $1 AND account_id = $2")
            .bind(graph_id)
            .bind(account_id)
            .fetch_optional(&mut *tx)
            .await?;
    tx.rollback().await.ok();
    Ok(team_id)
}

/// A destination graph's display name, scoped to this account. `None` on
/// any failure (missing row, scope error, db error) — a display-name lookup
/// degrading to "unnamed" is the right failure mode for a label, unlike the
/// hard counts elsewhere in this module which must never round down.
pub(crate) async fn graph_name(tenant_pool: &PgPool, account_id: Uuid, graph_id: Uuid) -> Option<String> {
    let mut tx = crate::tenancy::scope::begin_tenant_system_scoped(tenant_pool, account_id).await.ok()?;
    let name: Option<String> = sqlx::query_scalar("SELECT name FROM graphs WHERE id = $1 AND account_id = $2")
        .bind(graph_id)
        .bind(account_id)
        .fetch_optional(&mut *tx)
        .await
        .ok()
        .flatten();
    tx.rollback().await.ok();
    name
}

/// Persist an explicit destination pick — the write side of `PUT .../
/// destination`. **Deliberately breaks contract I2** ("one destination,
/// fixed at authorisation time") via a genuine `ON CONFLICT ... DO UPDATE`,
/// unlike [`crate::connectors::slack::ensure_slack_watched_channel`]'s
/// insert-only `DO NOTHING` — see this module's header for the founder
/// decision this records: re-pointing is allowed, but ONLY for content
/// ingested from here on. This function never touches `atoms`; it is the
/// entire mechanism by which "already-captured content stays put" holds —
/// there is simply no code path here that could move it.
///
/// `display_name` is left untouched on conflict (not overwritten with `''`
/// on an update) — an existing stored name, however unreliable
/// (`slack_channel_management::ChannelDestination::stale_name`'s doc
/// comment already documents that most rows carry `''` today), is still
/// strictly better information than unconditionally blanking it on every
/// re-point.
///
/// Scoping: same choice `ensure_slack_watched_channel`/
/// `check_team_ocean_admin` make, for the identical reason — this write may
/// persist a `graph_id` that is not the acting user's OWN member ocean
/// (e.g. an account owner routing a channel into a team ocean they
/// administer but do not personally own), so `begin_tenant_scoped`'s
/// per-member `app.user_id` visibility restriction is the wrong tool for an
/// administrative action that can legitimately span more than one member's
/// ocean. The explicit `account_id = $1`/`EXISTS (... g.account_id = $1)`
/// predicates below stand alongside RLS (which now enforces this boundary
/// too, post-OPS-4 — see this module's header) as the second, explicit
/// layer.
async fn upsert_channel_destination(
    tenant_pool: &PgPool,
    account_id: Uuid,
    user_id: Uuid,
    workspace_id: &str,
    channel_id: &str,
    mode: &'static str,
    graph_id: Option<Uuid>,
) -> Result<(), sqlx::Error> {
    let mut tx = crate::tenancy::scope::begin_tenant_system_scoped(tenant_pool, account_id)
        .await
        .map_err(|e| sqlx::Error::Protocol(format!("upsert_channel_destination scoped tx: {e}")))?;
    sqlx::query(
        r#"
        INSERT INTO slack_watched_channels
            (account_id, user_id, mode, graph_id, workspace_id, channel_id, display_name)
        SELECT $1, $2, $3, $4, $5, $6, ''
        WHERE  $4::uuid IS NULL
           OR  EXISTS (SELECT 1 FROM graphs g WHERE g.id = $4 AND g.account_id = $1)
        ON CONFLICT (account_id, workspace_id, channel_id) DO UPDATE
        SET    mode             = EXCLUDED.mode,
               graph_id         = EXCLUDED.graph_id,
               user_id          = EXCLUDED.user_id,
               last_resolved_at = now()
        "#,
    )
    .bind(account_id)
    .bind(user_id)
    .bind(mode)
    .bind(graph_id)
    .bind(workspace_id)
    .bind(channel_id)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(())
}

/// Delete a channel's explicit destination row. Used both by
/// `DestinationPick::Default` (revert to the account default) and by
/// [`stop_watching_channel_after_leave`] (the second half of "stop
/// watching"). Returns `true` when a row actually existed and was removed —
/// `false` is not an error, it means the channel had no explicit routing
/// row to begin with (already on the account default).
///
/// Explicit `account_id = $1` predicate — see this module's header: RLS now
/// enforces this same boundary too (OPS-4 has run), but this predicate stays
/// as the second, explicit layer standing between account A deleting
/// account B's routing row for a colliding `(workspace_id, channel_id)`
/// pair.
pub(crate) async fn delete_watched_channel_row(
    tenant_pool: &PgPool,
    account_id: Uuid,
    workspace_id: &str,
    channel_id: &str,
) -> Result<bool, sqlx::Error> {
    let mut tx = crate::tenancy::scope::begin_tenant_system_scoped(tenant_pool, account_id)
        .await
        .map_err(|e| sqlx::Error::Protocol(format!("delete_watched_channel_row scoped tx: {e}")))?;
    let result = sqlx::query(
        "DELETE FROM slack_watched_channels \
         WHERE account_id = $1 AND workspace_id = $2 AND channel_id = $3",
    )
    .bind(account_id)
    .bind(workspace_id)
    .bind(channel_id)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(result.rows_affected() > 0)
}

// ── ACTION 2: stop watching ─────────────────────────────────────────────────

/// What [`stop_watching_channel`] actually did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct StopWatchingOutcome {
    pub(crate) left: LeaveOutcome,
    pub(crate) watch_row_deleted: bool,
}

/// Why [`stop_watching_channel`] refused, or could not confirm the bot left.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum StopWatchingError {
    /// See [`LeaveError::MissingScope`] — needs an actionable "reinstall"
    /// message at the HTTP layer, not a generic failure.
    MissingScope,
    /// Any other Slack `ok: false` error.
    Slack(String),
    /// Transport/parse/rate-limit failure.
    Transport(String),
    /// The delete itself failed — the leave already SUCCEEDED at this
    /// point, so the caller is told the bot left but the management row
    /// could not be cleaned up (never the other way around; see
    /// [`stop_watching_channel_after_leave`]'s doc comment for why the
    /// delete is unreachable unless the leave already committed).
    Db(String),
}

impl From<LeaveError> for StopWatchingError {
    fn from(e: LeaveError) -> Self {
        match e {
            LeaveError::MissingScope => StopWatchingError::MissingScope,
            LeaveError::Slack(s) => StopWatchingError::Slack(s),
            LeaveError::Transport(s) => StopWatchingError::Transport(s),
        }
    }
}

/// Stop watching a channel — the real orchestrator: call Slack, then (only
/// on success) delete the row. Thin on purpose; see this module's header
/// and [`stop_watching_channel_after_leave`]'s own doc comment for why the
/// ordering-critical part is split into a separately-testable function.
pub(crate) async fn stop_watching_channel(
    http: &reqwest::Client,
    bot_token: &str,
    tenant_pool: &PgPool,
    account_id: Uuid,
    workspace_id: &str,
    channel_id: &str,
) -> Result<StopWatchingOutcome, StopWatchingError> {
    let leave_result = slack::leave_conversation(http, bot_token, channel_id).await;
    stop_watching_channel_after_leave(tenant_pool, account_id, workspace_id, channel_id, leave_result).await
}

/// The ordering-critical core of "stop watching a channel", taking the
/// ALREADY-OBTAINED `conversations.leave` result as a parameter rather than
/// making the call itself — this crate has no HTTP-mock harness (this
/// module's header), so this is what makes the ordering guarantee
/// pg-testable at all: a test can hand this function a hand-built
/// `Err(LeaveError::Slack(...))` without ever touching the network, exactly
/// the same split `seat_resolved_members` uses to make
/// `auto_seat_channel_members`'s write half testable without
/// `conversations.members`.
///
/// **THE invariant this function exists to enforce:** the
/// `slack_watched_channels` row is deleted IF AND ONLY IF `leave_result` is
/// `Ok`. Deleting it after a FAILED leave would report (or silently allow a
/// caller to infer) success for a channel the bot is still reading — worse
/// than doing nothing at all, because the channel would keep ingesting via
/// the account-level default with no management row left to show that it
/// is still being read (see [`crate::connectors::slack::leave_conversation`]'s
/// own doc comment). `?` on `leave_result` is that whole guarantee: an `Err`
/// returns before `delete_watched_channel_row` is ever called.
pub(crate) async fn stop_watching_channel_after_leave(
    tenant_pool: &PgPool,
    account_id: Uuid,
    workspace_id: &str,
    channel_id: &str,
    leave_result: Result<LeaveOutcome, LeaveError>,
) -> Result<StopWatchingOutcome, StopWatchingError> {
    let left = leave_result?;
    let watch_row_deleted = delete_watched_channel_row(tenant_pool, account_id, workspace_id, channel_id)
        .await
        .map_err(|e| StopWatchingError::Db(e.to_string()))?;
    Ok(StopWatchingOutcome { left, watch_row_deleted })
}

// ── Impact preview ───────────────────────────────────────────────────────────

/// The `GET .../impact` payload — real numbers for the confirmation dialog
/// in front of EITHER action. See [`channel_impact`]'s own doc comment.
#[derive(Debug, Clone)]
pub(crate) struct ChannelImpact {
    pub(crate) atom_count: u64,
    pub(crate) current_destination: ChannelDestination,
    /// `Some` only when the caller supplied a `target_graph_id` that
    /// resolves to a `kind = 'team'` graph — the "N of M would be seated"
    /// preview for a PROSPECTIVE team-ocean pick. `Err` (never rounded
    /// down) when the roster walk itself could not complete — the identical
    /// honesty rule [`crate::connectors::slack::resolve_channel_members`]'s
    /// own doc comment states, inherited unchanged because this calls that
    /// exact function.
    pub(crate) prospective_team_seats: Option<Result<(usize, usize), String>>,
}

/// Derive [`ChannelImpact`] for one channel — the business logic behind
/// `GET /v1/connectors/slack/channels/{channel_id}/impact`.
///
/// `atom_count` and `current_destination` answer "the N corals already
/// captured stay in `<ocean>`" — always populated, regardless of
/// `target_graph_id`, because that disclosure is about the CURRENT state,
/// not a prospective one.
///
/// `target_graph_id`, when given, previews a prospective SINGLE/team pick by
/// calling [`crate::connectors::slack::resolve_channel_members`] — the SAME
/// function [`auto_seat_channel_members`] calls to actually seat people —
/// so the number shown here and the number a subsequent `PUT .../
/// destination` call would actually seat can never drift apart (this
/// module's header). A `target_graph_id` that does not resolve to a
/// `kind = 'team'` graph (or does not resolve at all) yields
/// `prospective_team_seats: None`, not a wrong number — there is nothing to
/// seat for a non-team target, and this is a preview endpoint, not the
/// enforcement point (that stays `PUT .../destination`'s own ownership/
/// admin checks).
#[allow(clippy::too_many_arguments)]
pub(crate) async fn channel_impact(
    control_pool: &PgPool,
    tenant_pool: &PgPool,
    http: &reqwest::Client,
    account_id: Uuid,
    plane: AccountPlane,
    workspace_id: &str,
    channel_id: &str,
    bot_token: &str,
    target_graph_id: Option<Uuid>,
) -> Result<ChannelImpact, String> {
    let current_destination = slack::resolve_channel_destinations(
        control_pool,
        tenant_pool,
        account_id,
        plane,
        workspace_id,
        channel_id,
    )
    .await
    .map_err(|e| format!("channel_impact: resolve current destination: {e}"))?;

    let atom_count = fetch_channel_atom_count(tenant_pool, account_id, workspace_id, channel_id)
        .await
        .map_err(|e| format!("channel_impact: atom count: {e}"))?;

    let prospective_team_seats = match target_graph_id {
        None => None,
        Some(graph_id) => {
            let graph = fetch_graph_for_destination(tenant_pool, account_id, graph_id)
                .await
                .map_err(|e| format!("channel_impact: target graph lookup: {e}"))?;
            match graph {
                Some(g) if g.kind == "team" => Some(
                    slack::resolve_channel_members(
                        control_pool,
                        tenant_pool,
                        http,
                        account_id,
                        workspace_id,
                        bot_token,
                        channel_id,
                    )
                    .await
                    .map(|(matched, total)| (matched.len(), total)),
                ),
                // Not a team ocean (or not this account's graph at all) —
                // nothing to preview seats for. Not an error: most
                // `target_graph_id`s a client previews will be personal/
                // member/fanout picks that never seat anyone.
                _ => None,
            }
        }
    };

    Ok(ChannelImpact { atom_count, current_destination, prospective_team_seats })
}

/// One channel's atom count — the same shape as
/// `slack_channel_management::fetch_atom_stats`, narrowed to a single
/// channel and dropping `last_activity_at` (this endpoint only needs the
/// count). See that function's own doc comment for why the `graphs` join is
/// the account boundary, not decoration — identical reasoning applies here
/// unchanged: two different CoralSwarm accounts can connect the SAME Slack
/// workspace (`slack_integrations` is `UNIQUE (account_id, workspace_id)`,
/// not unique on `workspace_id`), so `workspace_id`/`channel_id` alone are
/// not an account boundary.
///
/// The permissive `workspace_id` match is likewise copied verbatim, and
/// keeping the two in step is the point of copying it: this count is the "N
/// corals stay where they are" line in the re-route confirmation, and the
/// management table shows the same channel's count one click away. Left on
/// the strict `= $2`, this dialog would have quoted a SMALLER number than the
/// row the reader just clicked — the pre-`0058` NULL-`workspace_id` atoms
/// (see `slack_channel_management::fetch_atom_stats`) are exactly the ones
/// that would go missing here, and a confirmation dialog understating what it
/// is about to leave behind is worse than either surface being wrong alone.
async fn fetch_channel_atom_count(
    tenant_pool: &PgPool,
    account_id: Uuid,
    workspace_id: &str,
    channel_id: &str,
) -> Result<u64, sqlx::Error> {
    let mut tx = crate::tenancy::scope::begin_tenant_system_scoped(tenant_pool, account_id)
        .await
        .map_err(|e| sqlx::Error::Protocol(format!("fetch_channel_atom_count scoped tx: {e}")))?;
    let count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM atoms a JOIN graphs g ON g.id = a.graph_id \
         WHERE g.account_id = $1 AND (a.workspace_id = $2 OR a.workspace_id IS NULL) \
           AND a.channel_id = $3",
    )
    .bind(account_id)
    .bind(workspace_id)
    .bind(channel_id)
    .fetch_one(&mut *tx)
    .await?;
    tx.rollback().await.ok();
    Ok(count as u64)
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod pg_tests {
    //! DB-backed tests, gated on `ORGT_TEST_BASE_URL` — same convention as
    //! `slack_channel_management::db_pg_tests` and
    //! `slack_interactivity`'s own pg test modules. None of these touch a
    //! live Slack API: the admin/ownership refusals return before any
    //! network call is made, and the leave-ordering tests hand
    //! `stop_watching_channel_after_leave` a hand-built `Result` instead of
    //! calling `conversations.leave` for real (this module's header).
    //!
    //!   ORGT_TEST_BASE_URL=postgres://alex@localhost:5432/postgres \
    //!     cargo test -p coralswarm-backend --lib slack_channel_actions::pg_tests

    use sqlx::postgres::PgPoolOptions;
    use sqlx::{Connection, Executor, PgConnection, PgPool};
    use uuid::Uuid;

    use super::*;
    use crate::routes::slack::AccountPlane;

    fn base_url() -> Option<String> {
        std::env::var("ORGT_TEST_BASE_URL").ok()
    }
    fn with_db(base: &str, db: &str) -> String {
        let (prefix, rest) = base.split_at(base.rfind('/').expect("url has a path /"));
        let params = rest.find('?').map(|i| &rest[i..]).unwrap_or("");
        format!("{prefix}/{db}{params}")
    }
    async fn setup(db: &str) -> (PgConnection, PgPool) {
        let base = base_url().expect("caller already checked base_url().is_some()");
        let mut admin_conn = PgConnection::connect(&base).await.expect("connect admin");
        admin_conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.unwrap();
        admin_conn.execute(format!("CREATE DATABASE {db}").as_str()).await.unwrap();
        let pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();
        (admin_conn, pool)
    }
    async fn teardown(mut admin_conn: PgConnection, pool: PgPool, db: &str) {
        pool.close().await;
        admin_conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
    }

    async fn seed_user(pool: &PgPool) -> Uuid {
        sqlx::query_scalar("INSERT INTO users(id, email) VALUES (gen_random_uuid(), $1) RETURNING id")
            .bind(format!("user-{}@test.local", Uuid::new_v4()))
            .fetch_one(pool)
            .await
            .unwrap()
    }

    async fn seed_org_account(pool: &PgPool, owner: Uuid) -> Uuid {
        let account_id: Uuid = sqlx::query_scalar(
            "INSERT INTO accounts(owner_user_id, clerk_org_id) VALUES ($1, $2) RETURNING id",
        )
        .bind(owner)
        .bind(format!("org-{}", Uuid::new_v4()))
        .fetch_one(pool)
        .await
        .unwrap();
        sqlx::query("INSERT INTO account_members(account_id, user_id, role, seat_tier_id) VALUES ($1, $2, 'owner', (SELECT id FROM seat_tiers WHERE code = 'standard' AND is_active = true))")
            .bind(account_id)
            .bind(owner)
            .execute(pool)
            .await
            .unwrap();
        account_id
    }

    async fn seed_personal_account(pool: &PgPool, owner: Uuid) -> Uuid {
        sqlx::query_scalar("INSERT INTO accounts(owner_user_id) VALUES ($1) RETURNING id")
            .bind(owner)
            .fetch_one(pool)
            .await
            .unwrap()
    }

    async fn seed_primary_graph(pool: &PgPool, account_id: Uuid) -> Uuid {
        sqlx::query_scalar(
            "INSERT INTO graphs (account_id, name, kind, is_primary) \
             VALUES ($1, 'Primary', 'personal', true) RETURNING id",
        )
        .bind(account_id)
        .fetch_one(pool)
        .await
        .unwrap()
    }

    /// A second, NON-primary graph on the same account — `graphs` allows only
    /// one `is_primary = true` row per account
    /// (`uq_graphs_one_primary_account_wide`), so a test that needs the
    /// account's primary ocean AND a distinct owned graph to route to first
    /// (before clearing back to the default) needs this, not a second
    /// [`seed_primary_graph`] call.
    async fn seed_owned_graph(pool: &PgPool, account_id: Uuid) -> Uuid {
        sqlx::query_scalar(
            "INSERT INTO graphs (account_id, name, kind, is_primary) \
             VALUES ($1, 'Other', 'personal', false) RETURNING id",
        )
        .bind(account_id)
        .fetch_one(pool)
        .await
        .unwrap()
    }

    /// Seeds a team ocean (`graphs` + `teams` rows) with `creator` as its
    /// first admin — the exact shape `create_org_team_ocean` writes, copied
    /// from `slack_interactivity`'s own test helper of the same name.
    async fn seed_team(pool: &PgPool, account_id: Uuid, creator: Uuid) -> Uuid {
        let graph_id: Uuid = sqlx::query_scalar(
            "INSERT INTO graphs (account_id, name, kind) VALUES ($1, 'test team', 'team') RETURNING id",
        )
        .bind(account_id)
        .fetch_one(pool)
        .await
        .unwrap();
        let team_id: Uuid = sqlx::query_scalar(
            "INSERT INTO teams (account_id, graph_id, name, created_by) \
             VALUES ($1, $2, 'test team', $3) RETURNING id",
        )
        .bind(account_id)
        .bind(graph_id)
        .bind(creator)
        .fetch_one(pool)
        .await
        .unwrap();
        sqlx::query("INSERT INTO team_members (team_id, user_id, role) VALUES ($1, $2, 'admin')")
            .bind(team_id)
            .bind(creator)
            .execute(pool)
            .await
            .unwrap();
        team_id
    }

    async fn watched_row_exists(pool: &PgPool, account_id: Uuid, workspace_id: &str, channel_id: &str) -> bool {
        sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(SELECT 1 FROM slack_watched_channels \
             WHERE account_id = $1 AND workspace_id = $2 AND channel_id = $3)",
        )
        .bind(account_id)
        .bind(workspace_id)
        .bind(channel_id)
        .fetch_one(pool)
        .await
        .unwrap()
    }

    async fn insert_watched_row(pool: &PgPool, account_id: Uuid, user_id: Uuid, workspace_id: &str, channel_id: &str, graph_id: Uuid) {
        sqlx::query(
            "INSERT INTO slack_watched_channels (account_id, user_id, mode, graph_id, workspace_id, channel_id) \
             VALUES ($1, $2, 'single', $3, $4, $5)",
        )
        .bind(account_id)
        .bind(user_id)
        .bind(graph_id)
        .bind(workspace_id)
        .bind(channel_id)
        .execute(pool)
        .await
        .unwrap();
    }

    // ── non-admin refused a team-ocean target ───────────────────────────

    /// A plain (non-admin) team member picking "route to this team ocean"
    /// through the REST endpoint must be refused exactly like the Block-Kit
    /// picker refuses it — reusing `check_team_ocean_admin`, not a second,
    /// possibly-looser check.
    ///
    /// FAILS WITHOUT THE FIX: comment out the `"team" => { ... }` arm's
    /// `check_team_ocean_admin` call in `set_channel_destination` (treating
    /// every `kind = "team"` graph like a plain `Single` pick, i.e. folding
    /// it into the `_ => ("single", ...)` arm) and this test fails — the
    /// pick would succeed for a non-admin instead of returning
    /// `NotTeamAdmin`. Restored below; this file diffs clean against that
    /// experiment.
    #[tokio::test]
    async fn non_admin_is_refused_a_team_ocean_target_pg() {
        let Some(_) = base_url() else {
            eprintln!("skipping non_admin_is_refused_a_team_ocean_target_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "orgt_setdest_non_admin_refused";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", db);
        let (admin_conn, pool) = setup(db).await;

        let owner = seed_user(&pool).await;
        let account = seed_org_account(&pool, owner).await;
        let team_id = seed_team(&pool, account, owner).await;
        let team_graph_id: Uuid = sqlx::query_scalar("SELECT graph_id FROM teams WHERE id = $1")
            .bind(team_id)
            .fetch_one(&pool)
            .await
            .unwrap();

        // A second, non-admin org member — never added to `team_members`.
        let plain_member = seed_user(&pool).await;
        sqlx::query("INSERT INTO account_members(account_id, user_id, role, seat_tier_id) VALUES ($1, $2, 'member', (SELECT id FROM seat_tiers WHERE code = 'standard' AND is_active = true))")
            .bind(account)
            .bind(plain_member)
            .execute(&pool)
            .await
            .unwrap();

        let http = reqwest::Client::new();
        let result = set_channel_destination(
            &pool,
            &pool,
            &http,
            account,
            AccountPlane::Org,
            plain_member,
            "T1",
            "C1",
            "xoxb-fake",
            DestinationPick::Single(team_graph_id),
        )
        .await;

        assert!(
            matches!(result, Err(SetDestinationError::NotTeamAdmin { .. })),
            "a non-admin must be refused, not silently routed — got {result:?}"
        );
        assert!(
            !watched_row_exists(&pool, account, "T1", "C1").await,
            "a refused pick must never write a row"
        );

        teardown(admin_conn, pool, db).await;
    }

    /// The account OWNER (who administers the team, since `seed_team` seats
    /// its creator as admin) succeeds — proves the refusal above is about
    /// admin status specifically, not about the endpoint being broken.
    #[tokio::test]
    async fn team_admin_may_route_into_their_own_team_ocean_pg() {
        let Some(_) = base_url() else {
            eprintln!("skipping team_admin_may_route_into_their_own_team_ocean_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "orgt_setdest_admin_allowed";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", db);
        let (admin_conn, pool) = setup(db).await;

        let owner = seed_user(&pool).await;
        let account = seed_org_account(&pool, owner).await;
        let team_id = seed_team(&pool, account, owner).await;
        let team_graph_id: Uuid = sqlx::query_scalar("SELECT graph_id FROM teams WHERE id = $1")
            .bind(team_id)
            .fetch_one(&pool)
            .await
            .unwrap();

        let http = reqwest::Client::new();
        let result = set_channel_destination(
            &pool,
            &pool,
            &http,
            account,
            AccountPlane::Org,
            owner,
            "T1",
            "C1",
            "xoxb-fake",
            DestinationPick::Single(team_graph_id),
        )
        .await;

        match result {
            Ok(outcome) => {
                assert_eq!(outcome.destination, ChannelDestination::Single(team_graph_id));
                // Auto-seat runs, but this test uses a fake bot token and no
                // real Slack — `resolve_channel_members` fails outright
                // (transport error), so `seat_result` is `Some(Err(_))`, not
                // `None`: the write path DID attempt to seat, exactly as the
                // brief requires ("auto-seat on team-ocean targets"), it
                // just could not reach Slack in this test environment.
                assert!(
                    matches!(outcome.seat_result, Some(Err(_))),
                    "an admin's pick still triggers an auto-seat ATTEMPT — got {:?}",
                    outcome.seat_result
                );
            }
            Err(e) => panic!("an admin's own team-ocean pick must succeed, got {e:?}"),
        }
        assert!(watched_row_exists(&pool, account, "T1", "C1").await);

        teardown(admin_conn, pool, db).await;
    }

    // ── cross-account graph_id refused ──────────────────────────────────

    /// A `graph_id` that is real, but belongs to a DIFFERENT account, must
    /// be refused — the explicit ownership predicate the brief requires,
    /// which this test targets directly rather than relying on RLS to catch
    /// it incidentally (both layers now enforce this boundary; see this
    /// module's header).
    ///
    /// FAILS WITHOUT THE FIX: drop the `AND account_id = $2` predicate from
    /// `fetch_graph_for_destination`'s query (querying `graphs` by `id`
    /// alone) and this test fails — account A's pick against account B's
    /// `graph_id` would succeed and write a cross-account routing row.
    /// Restored below.
    #[tokio::test]
    async fn cross_account_graph_id_is_refused_pg() {
        let Some(_) = base_url() else {
            eprintln!("skipping cross_account_graph_id_is_refused_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "orgt_setdest_cross_account";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", db);
        let (admin_conn, pool) = setup(db).await;

        let owner_a = seed_user(&pool).await;
        let account_a = seed_personal_account(&pool, owner_a).await;

        let owner_b = seed_user(&pool).await;
        let account_b = seed_personal_account(&pool, owner_b).await;
        let graph_b = seed_primary_graph(&pool, account_b).await;

        let http = reqwest::Client::new();
        // Account A picks account B's graph_id.
        let result = set_channel_destination(
            &pool,
            &pool,
            &http,
            account_a,
            AccountPlane::Personal,
            owner_a,
            "T1",
            "C1",
            "xoxb-fake",
            DestinationPick::Single(graph_b),
        )
        .await;

        assert_eq!(
            result,
            Err(SetDestinationError::GraphNotOwned),
            "a graph belonging to a DIFFERENT account must be refused as not-owned, got {result:?}"
        );
        assert!(
            !watched_row_exists(&pool, account_a, "T1", "C1").await,
            "a refused cross-account pick must never write a row"
        );
        // And account B's own row set is untouched — nothing to assert
        // beyond "no row was written for account_a's attempt", already
        // covered above; account B never had a row to begin with.

        teardown(admin_conn, pool, db).await;
    }

    // ── clearing to default deletes the row ─────────────────────────────

    /// `DestinationPick::Default` deletes an existing explicit row and
    /// falls back to the account default (a personal account's primary
    /// ocean).
    #[tokio::test]
    async fn clearing_to_default_deletes_the_row_pg() {
        let Some(_) = base_url() else {
            eprintln!("skipping clearing_to_default_deletes_the_row_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "orgt_setdest_clear_default";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", db);
        let (admin_conn, pool) = setup(db).await;

        let owner = seed_user(&pool).await;
        let account = seed_personal_account(&pool, owner).await;
        let primary_graph = seed_primary_graph(&pool, account).await;
        let other_graph = seed_owned_graph(&pool, account).await; // a distinct owned graph, explicitly routed before the clear

        insert_watched_row(&pool, account, owner, "T1", "C1", other_graph).await;
        assert!(watched_row_exists(&pool, account, "T1", "C1").await, "sanity: row seeded");

        let http = reqwest::Client::new();
        let result = set_channel_destination(
            &pool,
            &pool,
            &http,
            account,
            AccountPlane::Personal,
            owner,
            "T1",
            "C1",
            "xoxb-fake",
            DestinationPick::Default,
        )
        .await
        .expect("clearing to default must succeed");

        assert!(
            !watched_row_exists(&pool, account, "T1", "C1").await,
            "clearing to default must delete the explicit routing row"
        );
        assert_eq!(
            result.destination,
            ChannelDestination::Single(primary_graph),
            "with no explicit row, a personal account falls back to its primary ocean"
        );
        assert_eq!(result.seat_result, None, "clearing to default never seats anyone");

        teardown(admin_conn, pool, db).await;
    }

    // ── conversations.leave failure leaves the row intact ───────────────

    /// THE ordering invariant `stop_watching_channel_after_leave` exists to
    /// enforce: a FAILED leave must never delete the row.
    ///
    /// FAILS WITHOUT THE FIX: change `let left = leave_result?;` to
    /// `let left = leave_result.unwrap_or(LeaveOutcome::Left);` (i.e. treat
    /// a leave failure as if it succeeded) and this test fails — the row
    /// would be deleted despite `conversations.leave` having failed.
    /// Restored below.
    #[tokio::test]
    async fn a_failed_leave_never_deletes_the_row_pg() {
        let Some(_) = base_url() else {
            eprintln!("skipping a_failed_leave_never_deletes_the_row_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "orgt_stopwatch_failed_leave_keeps_row";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", db);
        let (admin_conn, pool) = setup(db).await;

        let owner = seed_user(&pool).await;
        let account = seed_personal_account(&pool, owner).await;
        let graph_id = seed_primary_graph(&pool, account).await;
        insert_watched_row(&pool, account, owner, "T1", "C1", graph_id).await;

        let result = stop_watching_channel_after_leave(
            &pool,
            account,
            "T1",
            "C1",
            Err(LeaveError::Slack("channel_not_found".to_string())),
        )
        .await;

        assert_eq!(result, Err(StopWatchingError::Slack("channel_not_found".to_string())));
        assert!(
            watched_row_exists(&pool, account, "T1", "C1").await,
            "a FAILED conversations.leave must leave the watched-channel row untouched — deleting \
             it here would report a channel as stopped while the bot can still read it"
        );

        teardown(admin_conn, pool, db).await;
    }

    /// The success-shaped counterpart: a genuine `Left` (or idempotent
    /// `AlreadyLeft`) DOES delete the row, and reports which happened.
    #[tokio::test]
    async fn a_successful_leave_deletes_the_row_pg() {
        let Some(_) = base_url() else {
            eprintln!("skipping a_successful_leave_deletes_the_row_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "orgt_stopwatch_success_deletes_row";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", db);
        let (admin_conn, pool) = setup(db).await;

        let owner = seed_user(&pool).await;
        let account = seed_personal_account(&pool, owner).await;
        let graph_id = seed_primary_graph(&pool, account).await;
        insert_watched_row(&pool, account, owner, "T1", "C1", graph_id).await;

        let result =
            stop_watching_channel_after_leave(&pool, account, "T1", "C1", Ok(LeaveOutcome::Left))
                .await
                .expect("a successful leave must succeed end to end");

        assert_eq!(result.left, LeaveOutcome::Left);
        assert!(result.watch_row_deleted);
        assert!(!watched_row_exists(&pool, account, "T1", "C1").await);

        teardown(admin_conn, pool, db).await;
    }

    /// `AlreadyLeft` (the bot was already out of the channel) is also
    /// treated as success and still cleans up a stale row, if one exists —
    /// the idempotent case named in `LeaveOutcome::AlreadyLeft`'s own doc
    /// comment.
    #[tokio::test]
    async fn already_left_still_deletes_a_stale_row_pg() {
        let Some(_) = base_url() else {
            eprintln!("skipping already_left_still_deletes_a_stale_row_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "orgt_stopwatch_already_left";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", db);
        let (admin_conn, pool) = setup(db).await;

        let owner = seed_user(&pool).await;
        let account = seed_personal_account(&pool, owner).await;
        let graph_id = seed_primary_graph(&pool, account).await;
        insert_watched_row(&pool, account, owner, "T1", "C1", graph_id).await;

        let result =
            stop_watching_channel_after_leave(&pool, account, "T1", "C1", Ok(LeaveOutcome::AlreadyLeft))
                .await
                .expect("AlreadyLeft must be treated as success");

        assert_eq!(result.left, LeaveOutcome::AlreadyLeft);
        assert!(result.watch_row_deleted);
        assert!(!watched_row_exists(&pool, account, "T1", "C1").await);

        teardown(admin_conn, pool, db).await;
    }

    /// No watched row existed at all (the channel was already on the
    /// account default) — a successful leave reports `watch_row_deleted:
    /// false`, not an error.
    #[tokio::test]
    async fn stop_watching_an_unrouted_channel_is_not_an_error_pg() {
        let Some(_) = base_url() else {
            eprintln!("skipping stop_watching_an_unrouted_channel_is_not_an_error_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "orgt_stopwatch_unrouted";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", db);
        let (admin_conn, pool) = setup(db).await;

        let owner = seed_user(&pool).await;
        let account = seed_personal_account(&pool, owner).await;
        // No `slack_watched_channels` row at all — deliberately.

        let result =
            stop_watching_channel_after_leave(&pool, account, "T1", "C1", Ok(LeaveOutcome::Left))
                .await
                .expect("stop-watching an unrouted channel must still succeed");

        assert!(!result.watch_row_deleted, "there was never a row to delete");

        teardown(admin_conn, pool, db).await;
    }

    // ── org ocean is never a valid destination ──────────────────────────

    #[tokio::test]
    async fn org_ocean_is_refused_as_a_destination_pg() {
        let Some(_) = base_url() else {
            eprintln!("skipping org_ocean_is_refused_as_a_destination_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "orgt_setdest_org_ocean_refused";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", db);
        let (admin_conn, pool) = setup(db).await;

        let owner = seed_user(&pool).await;
        let account = seed_org_account(&pool, owner).await;
        let org_graph: Uuid = sqlx::query_scalar(
            "INSERT INTO graphs (account_id, name, kind) VALUES ($1, 'Org', 'org') RETURNING id",
        )
        .bind(account)
        .fetch_one(&pool)
        .await
        .unwrap();

        let http = reqwest::Client::new();
        let result = set_channel_destination(
            &pool,
            &pool,
            &http,
            account,
            AccountPlane::Org,
            owner,
            "T1",
            "C1",
            "xoxb-fake",
            DestinationPick::Single(org_graph),
        )
        .await;

        assert_eq!(result, Err(SetDestinationError::OrgOcean));
        assert!(!watched_row_exists(&pool, account, "T1", "C1").await);

        teardown(admin_conn, pool, db).await;
    }
}
