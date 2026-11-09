//! Shared Slack disconnect/purge routine — org governance decision
//! (2026-08-04), piece 3.
//!
//! Every place CoralSwarm learns a Slack workspace should stop being watched
//! — a human clicking "Disconnect" (`routes::slack::delete_integration`,
//! `routes::oauth::delete_connection`'s Slack arm) or Slack itself revoking
//! our access (`routes::slack_events`'s `app_uninstalled`/`tokens_revoked`
//! handlers) — needs the SAME cleanup. Before this module existed, that
//! cleanup was duplicated (or missing) at each call site; a fix applied to
//! one would silently not apply to the others, exactly the class of bug
//! `routes::oauth::delete_connection`'s own doc comment already documents
//! for `slack_integrations` ("Disconnect" not actually stopping ingest).
//!
//! # What this purges
//!
//! 1. **`slack_watched_channels`** — every explicit per-channel destination
//!    row for the disconnected workspace. Lives on the CONTENT plane (the
//!    tenant pool: the consumer pool for Personal, the org's own tenant
//!    database for Org — see [`purge_slack_workspace`]'s own doc comment).
//! 2. **`team_seat_grants`** sourced from those channels — REVOKED, not just
//!    deleted, following [`crate::seats::revoke_seat_grant`]'s own rule to
//!    the letter: a grant is only ever removed as the direct result of a
//!    `DELETE ... team_seat_grants` whose row count is the gate, never a
//!    count-then-revoke, and an `admin` seat is never touched. This module
//!    adds no seat-affecting logic of its own — it only decides WHICH
//!    `(team_id, user_id, source)` triples to hand to that existing,
//!    already-reviewed function, one call per grant.
//! 3. **`conversations.leave`** on every channel the bot is currently a
//!    member of in the workspace — best-effort, and ONLY when this account is
//!    the workspace's SOLE remaining installer (see the "shared bot" section
//!    below). A failure on any one channel is logged (`warn!`) and the purge
//!    continues; it never aborts the rest of the disconnect. Bounded by a
//!    total deadline (`LEAVE_PHASE_BUDGET`) so a workspace with an unusually
//!    large channel count cannot make one disconnect request hang
//!    indefinitely. By the time `app_uninstalled`/`tokens_revoked` reach this
//!    routine the bot's access is usually already gone from Slack's own
//!    side, so this step is often a no-op there — kept anyway so every caller
//!    runs the same one code path rather than a special-cased subset.
//!
//! # The bot is per-WORKSPACE, not per-account — the "shared bot" hazard
//!
//! CoralSwarm's Slack app installs ONCE per workspace and hands back ONE bot
//! token, but more than one CoralSwarm account can legitimately hold that
//! SAME install — `routes::slack::resolve_slack_account_ids`'s own doc
//! comment names the exact shape: "two different CoralSwarm accounts can
//! legitimately install the same Slack workspace — two separate customers,
//! or a customer plus someone's personal account". Confirmed in production:
//! workspace `T6D4612D34B` is installed under both a personal account and an
//! org account.
//!
//! If step 3 blindly called `conversations.leave` on every channel the bot
//! is CURRENTLY in whenever ANY ONE of those accounts disconnected, it would
//! rip the bot out of channels the OTHER still-connected account is actively
//! ingesting — account A clicking "Disconnect" would silently break account
//! B's live capture, with nothing in B's own UI or logs pointing at A's
//! action as the cause. [`purge_slack_workspace`] therefore checks for a
//! co-installer FIRST (`workspace_has_another_installer`) and skips the
//! ENTIRE leave phase — not just the newly-orphaned channels — when one
//! exists: the bot is shared, so leaving anything on this account's behalf
//! is unsafe. Only when this account is confirmed to be the workspace's SOLE
//! remaining installer does the full bot-member-channel leave run, exactly
//! as before this fix.
//!
//! # What this deliberately does NOT purge
//!
//! **`connector_identities` and every captured atom are left untouched.**
//! Disconnecting a Slack workspace withdraws consent to keep CAPTURING new
//! content — it is not an erasure request, and nothing in the org governance
//! decision this module implements asks for one. A caller that also wants to
//! delete already-ingested content needs the existing, separately-reviewed
//! deletion path (`services::deletion`), not this one.
//!
//! `oauth_tokens` / `slack_integrations` / `connector_installs` — the
//! CONTROL-plane rows that make the workspace resolvable at all — are also
//! NOT deleted here. Every caller of this routine already owns tearing those
//! down its own way (a single-workspace `DELETE` in the two manual-disconnect
//! routes, a `resolve_slack_account_ids`-driven loop in the Slack-initiated
//! handlers), and needs the bot token those rows hold BEFORE it disappears —
//! see [`purge_slack_workspace`]'s `bot_token` parameter. Centralizing that
//! deletion here too would force every caller into one specific ordering
//! relative to its own token-row cleanup; leaving it to the caller keeps this
//! routine a pure "clean up what a workspace routed/seated" step.

use sqlx::PgPool;
use uuid::Uuid;

use crate::seats::RevokeOutcome;
use crate::tenancy::scope::begin_tenant_system_scoped;

/// What [`purge_slack_workspace`] actually did — logged by every caller, and
/// asserted on directly by this module's own pg tests.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PurgeOutcome {
    /// `slack_watched_channels` rows deleted for this workspace.
    pub(crate) watched_channels_deleted: usize,
    /// `true` when the `slack_watched_channels` DELETE itself errored — NOT
    /// the same thing as "zero rows to delete". `watched_channels_deleted`
    /// is `0` either way, which is exactly the ambiguity this flag exists to
    /// break: a genuine DB failure here also means `seat_grants_examined`
    /// stays `0` (no channel ids to compute sources from), so a caller that
    /// only looked at the counts would read a failed purge as "nothing to
    /// purge" and answer its own caller 200 while the token/integration rows
    /// it deletes separately ARE gone — the stale watched-channel/seat rows
    /// then silently resurrect on reconnect. See [`purge_had_a_failure`].
    pub(crate) watched_channels_delete_failed: bool,
    /// `team_seat_grants` rows examined (i.e. whose `source` matched one of
    /// this workspace's channels) — includes grants that turned out to be
    /// `GrantDropped`/`AdminPreserved`/`NotOurs`, not only `SeatRevoked`.
    pub(crate) seat_grants_examined: usize,
    /// `true` when the `team_seat_grants` lookup itself errored — same
    /// "zero is ambiguous" reasoning as `watched_channels_delete_failed`.
    pub(crate) seat_grant_lookup_failed: bool,
    /// Seats actually revoked (the grant removed was the LAST reason for
    /// that seat, and the member did not hold it as an admin).
    pub(crate) seats_revoked: usize,
    /// Channels the bot successfully left (or was already not a member of).
    pub(crate) channels_left: usize,
    /// Channels `conversations.leave` failed on — best-effort, never fatal.
    pub(crate) channels_leave_failed: usize,
    /// `true` when the ENTIRE leave phase was skipped because some OTHER
    /// account still has this workspace installed — the bot is shared, so
    /// leaving anything on this account's behalf would break the co-install.
    /// See this module's own "shared bot" header section.
    pub(crate) leave_skipped_shared_install: bool,
    /// `true` when the co-installer check itself failed (a DB error) — the
    /// leave phase is skipped in this case too, on the same "never guess"
    /// principle `workers::slack_seat_reconcile`'s own header already
    /// documents for an unknown roster: not knowing whether the bot is
    /// shared is treated as "assume it might be", never as "assume it isn't".
    pub(crate) leave_skipped_check_failed: bool,
    /// `true` when the leave phase hit its total time budget
    /// (`LEAVE_PHASE_BUDGET`) before finishing every channel — the channels
    /// already processed by then are still reflected in `channels_left`/
    /// `channels_leave_failed`; whatever remained is simply not attempted.
    pub(crate) leave_timed_out: bool,
}

impl PurgeOutcome {
    /// `true` when either DB step of the purge itself failed (as opposed to
    /// the best-effort `conversations.leave` step, whose failures are
    /// expected often enough — a revoked/expired bot token, a channel the
    /// bot already lost access to — that they do not, on their own, mean the
    /// purge needs operator attention). Callers should log a `connector_health`
    /// warning when this is `true`, in addition to the `warn!`s this module
    /// already emits inline, so the failure is visible on whichever surface
    /// (HTTP disconnect route vs. the Slack-initiated event handler) is
    /// actually watching for `connector_health`-tagged log lines.
    pub(crate) fn purge_had_a_failure(&self) -> bool {
        self.watched_channels_delete_failed
            || self.seat_grant_lookup_failed
            || self.leave_skipped_check_failed
    }
}

/// Total wall-clock budget for the ENTIRE leave phase (every channel,
/// summed) — a workspace with an unusually large channel count must not
/// make one disconnect request hang indefinitely. Generous relative to a
/// single `conversations.leave` call's own transport timeout (this crate's
/// shared `http_client` default) because a sole-install disconnect can
/// legitimately have dozens of channels to walk; short enough that a caller
/// serving an HTTP response (`delete_integration`/`delete_connection`) is
/// never left hanging for minutes. Deliberately a flat `tokio::time::timeout`
/// around the whole loop, not a restructure onto a background task — see
/// [`purge_slack_workspace`]'s own doc comment for why: this phase is rare
/// now that [`workspace_has_another_installer`] skips it entirely for the
/// shared-bot case, and the pre-fix loop already RAN inline with no
/// complaints, so a targeted bound is the whole fix this deserves.
const LEAVE_PHASE_BUDGET: std::time::Duration = std::time::Duration::from_secs(15);

/// Is `workspace_id` still installed under some CoralSwarm account OTHER
/// than `account_id`? See this module's own "shared bot" header section for
/// why this gates the ENTIRE leave phase.
///
/// Reuses `routes::slack::resolve_slack_account_ids` — the SAME
/// `SECURITY DEFINER`-backed, self-healing, cross-account resolver the
/// inbound webhook router uses — rather than a bespoke query here: a plain
/// `SELECT ... WHERE account_id <> $1` against `slack_integrations` would
/// hit the EXACT RLS trap `find_other_org_slack_workspace` was just fixed
/// for (0083's `account_isolation` policy scopes reads to ONE account;
/// deliberately excluding the caller's own account here means this query
/// needs to see rows RLS would otherwise hide, which is precisely what that
/// resolver's bootstrap `SECURITY DEFINER` functions exist to do safely).
async fn workspace_has_another_installer(
    control_pool: &PgPool,
    account_id: Uuid,
    workspace_id: &str,
) -> Result<bool, sqlx::Error> {
    let installers = crate::routes::slack::resolve_slack_account_ids(control_pool, workspace_id).await?;
    Ok(installers.into_iter().any(|id| id != account_id))
}

/// Resolve `(account_id, workspace_id)`'s Slack bot token — CONTROL-plane
/// (see `routes::slack::oauth_callback`'s own doc comment for why
/// `slack_integrations`/`oauth_tokens` always live there, on both Personal
/// and Org).
///
/// Foundation PR 1.5: reads the encrypted `oauth_tokens` copy via
/// `workers::huddle::get_slack_bot_token_for_workspace` instead of
/// `slack_integrations.bot_token` directly — the plaintext column is now
/// dual-written only, never read. An account connected through the legacy
/// `routes::slack::oauth_callback` path before this PR shipped has no
/// `oauth_tokens` row until the one-time `backfill_slack_oauth_tokens`
/// binary (`bin/backfill_slack_oauth_tokens.rs`) runs for it — until then
/// this returns `Ok(None)`, same as "never connected", which is the correct,
/// fail-closed answer for a purge step (nothing to attempt via
/// `conversations.leave` rather than reading a stale plaintext token that a
/// prior disconnect may have already revoked provider-side).
///
/// Callers fetch this THEMSELVES, before deleting the row it comes from, and
/// pass it into [`purge_slack_workspace`] as `bot_token` — see that
/// function's doc comment for why the token's lifetime is the caller's
/// problem, not this module's.
pub(crate) async fn fetch_slack_bot_token(
    control_pool: &PgPool,
    dek_plane: &crate::crypto::DekPlane,
    account_id: Uuid,
    workspace_id: &str,
) -> Result<Option<String>, sqlx::Error> {
    // `control_pool` holds the `oauth_tokens` ROW; `dek_plane` names where
    // its KEY lives, which for an org account is that org's own tenant DB
    // (org-plane DEK fix). Both callers are disconnect paths that already
    // resolved the org's plane for the purge itself, and pass the same one
    // here so the token read and the purge cannot disagree.
    crate::workers::huddle::get_slack_bot_token_for_workspace(
        control_pool,
        dek_plane,
        account_id,
        workspace_id,
    )
    .await
    .map_err(sqlx::Error::Protocol)
}

/// Delete every `slack_watched_channels` row for `(account_id, workspace_id)`
/// and return the `channel_id`s that were routed — the set
/// [`purge_slack_workspace`] revokes seats for. `RETURNING` makes this one
/// round trip do both the delete and the discovery, atomically (no window
/// where a concurrent write could route a NEW channel into that gap and have
/// it silently escape both this delete and the seat-revocation pass below).
///
/// `begin_tenant_system_scoped`, not the plain `begin_account_scoped` — same
/// reason `connectors::slack_channel_actions::delete_watched_channel_row`
/// uses it: this table's tenant-plane RLS policy needs `app.tenant_actor`
/// set, and there is no acting end user here (a Slack-initiated disconnect
/// has none at all; a manual disconnect's caller already left the HTTP
/// request's own scope behind by the time this runs).
async fn delete_watched_channels_for_workspace(
    tenant_pool: &PgPool,
    account_id: Uuid,
    workspace_id: &str,
) -> Result<Vec<String>, sqlx::Error> {
    let mut tx = begin_tenant_system_scoped(tenant_pool, account_id)
        .await
        .map_err(|e| {
            sqlx::Error::Protocol(format!(
                "delete_watched_channels_for_workspace scoped tx: {e}"
            ))
        })?;
    let channel_ids: Vec<String> = sqlx::query_scalar(
        "DELETE FROM slack_watched_channels \
         WHERE account_id = $1 AND workspace_id = $2 \
         RETURNING channel_id",
    )
    .bind(account_id)
    .bind(workspace_id)
    .fetch_all(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(channel_ids)
}

/// Every `(team_id, user_id, source)` triple in `team_seat_grants` whose
/// `source` is one of `sources` — the exact set [`revoke_seat_grant`] needs
/// to be handed, one call per row. Deliberately a plain `SELECT`, not a
/// `DELETE`: the actual deletion (and the "was this the last grant"
/// decision) stays entirely inside `crate::seats::revoke_seat_grant`, so
/// this module adds no seat-affecting logic of its own — see this module's
/// own header.
async fn seat_grants_matching_sources(
    tenant_pool: &PgPool,
    account_id: Uuid,
    sources: &[String],
) -> Result<Vec<(Uuid, Uuid, String)>, sqlx::Error> {
    if sources.is_empty() {
        return Ok(Vec::new());
    }
    let mut tx = begin_tenant_system_scoped(tenant_pool, account_id)
        .await
        .map_err(|e| {
            sqlx::Error::Protocol(format!("seat_grants_matching_sources scoped tx: {e}"))
        })?;
    let rows: Vec<(Uuid, Uuid, String)> = sqlx::query_as(
        "SELECT team_id, user_id, source FROM team_seat_grants \
         WHERE account_id = $1 AND source = ANY($2)",
    )
    .bind(account_id)
    .bind(sources)
    .fetch_all(&mut *tx)
    .await?;
    tx.rollback().await.ok();
    Ok(rows)
}

/// The one shared disconnect purge — see this module's own header for the
/// full "what this does and does not touch" contract.
///
/// `tenant_pool` is the CONTENT plane for this account: the consumer pool
/// for Personal (byte-identical to the control pool there), or the org's own
/// tenant database for Org — the exact same pool
/// `routes::slack::resolve_connected_slack_workspace` resolves as its
/// `tenant_pool`, and the same one `connectors::slack_channel_actions`
/// writes `slack_watched_channels` through. Every caller of this function is
/// responsible for resolving it correctly for its own plane; this function
/// does not (and cannot, from a bare `PgPool` alone) tell Personal and Org
/// apart.
///
/// `control_pool` is used ONLY for [`workspace_has_another_installer`]'s
/// co-installer check — the CONTROL-plane tables (`slack_integrations`/
/// `connector_installs`) that make a workspace resolvable at all, same
/// plane `fetch_slack_bot_token` reads.
///
/// `bot_token` is the caller's responsibility to have already fetched (via
/// [`fetch_slack_bot_token`], or from wherever else it already has it) —
/// see [`fetch_slack_bot_token`]'s own doc comment for why. `None` skips the
/// `conversations.leave` step entirely (nothing to log as a failure; there
/// was nothing to attempt).
pub(crate) async fn purge_slack_workspace(
    http: &reqwest::Client,
    control_pool: &PgPool,
    tenant_pool: &PgPool,
    account_id: Uuid,
    workspace_id: &str,
    bot_token: Option<&str>,
) -> PurgeOutcome {
    let mut outcome = PurgeOutcome::default();

    // ── 1. slack_watched_channels ────────────────────────────────────────
    let channel_ids =
        match delete_watched_channels_for_workspace(tenant_pool, account_id, workspace_id).await {
            Ok(ids) => ids,
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    account_id = %account_id,
                    workspace_id = %workspace_id,
                    connector_health = true,
                    "slack disconnect purge: failed to delete slack_watched_channels rows — \
                     continuing with the rest of the purge (no channel ids to revoke seats for)"
                );
                outcome.watched_channels_delete_failed = true;
                Vec::new()
            }
        };
    outcome.watched_channels_deleted = channel_ids.len();

    // ── 2. team_seat_grants — revoke only grants WE recorded ───────────────
    // `crate::seats::revoke_seat_grant` is called once per (team_id,
    // user_id, source) triple found below, completely unmodified from the
    // reconciler's own call shape (`workers::slack_seat_reconcile::
    // reconcile_channel`) and the live `member_left_channel` handler's
    // (`routes::slack_events::handle_member_left`) — same function, same
    // "DELETE's row count is the gate" discipline, same admin guard. This
    // module never touches `team_members` directly.
    let sources: Vec<String> = channel_ids
        .iter()
        .map(|c| crate::seats::slack_channel_source(c))
        .collect();
    match seat_grants_matching_sources(tenant_pool, account_id, &sources).await {
        Ok(grants) => {
            outcome.seat_grants_examined = grants.len();
            for (team_id, user_id, source) in grants {
                match crate::seats::revoke_seat_grant(tenant_pool, account_id, team_id, user_id, &source)
                    .await
                {
                    Ok(RevokeOutcome::SeatRevoked) => outcome.seats_revoked += 1,
                    // GrantDropped (another reason survives), NotOurs (a
                    // concurrent revoke already won), and AdminPreserved
                    // (never touch an admin seat) are all correct, silent
                    // outcomes — nothing further for this module to do.
                    Ok(_) => {}
                    Err(e) => {
                        tracing::warn!(
                            error = %e,
                            account_id = %account_id,
                            team_id = %team_id,
                            user_id = %user_id,
                            source = %source,
                            "slack disconnect purge: seat revocation failed for one grant — \
                             continuing (a reconciler sweep will retry)"
                        );
                    }
                }
            }
        }
        Err(e) => {
            tracing::warn!(
                error = %e,
                account_id = %account_id,
                workspace_id = %workspace_id,
                connector_health = true,
                "slack disconnect purge: failed to look up team_seat_grants — no seats revoked \
                 for this disconnect (a reconciler sweep will retry once the channel rows are \
                 gone... though note: without a watched-channel row to reconcile against, this \
                 needs a person to notice, not just a sweep)"
            );
            outcome.seat_grant_lookup_failed = true;
        }
    }

    // ── 3. best-effort conversations.leave on every bot-member channel ─────
    // Gated on this account being the workspace's SOLE remaining installer —
    // see this module's own "shared bot" header section for why leaving
    // anything is unsafe when another account still has this workspace
    // installed. `bot_token: None` (nothing to attempt) skips straight past
    // — no co-installer check needed either, since there is no leave call to
    // gate.
    if let Some(bot_token) = bot_token {
        match workspace_has_another_installer(control_pool, account_id, workspace_id).await {
            Ok(true) => {
                outcome.leave_skipped_shared_install = true;
                tracing::info!(
                    account_id = %account_id,
                    workspace_id = %workspace_id,
                    "slack disconnect purge: another account still has this workspace installed \
                     — the bot is shared, so the ENTIRE conversations.leave phase is skipped for \
                     this disconnect (leaving anything would break the co-install)"
                );
            }
            Ok(false) => {
                // Sole installer — safe to leave everywhere the bot currently
                // is, exactly the pre-fix behavior. Bounded by
                // `LEAVE_PHASE_BUDGET`: `outcome.channels_left`/
                // `channels_leave_failed` are mutated INSIDE the timed
                // future, one channel at a time, so a timeout still leaves
                // whatever progress was made intact — only the channels not
                // yet reached go unattempted.
                let timed = tokio::time::timeout(LEAVE_PHASE_BUDGET, async {
                    match crate::connectors::slack::fetch_bot_member_channels(http, bot_token).await
                    {
                        Ok(channels) => {
                            for (channel_id, _name) in channels {
                                match crate::connectors::slack::leave_conversation(
                                    http,
                                    bot_token,
                                    &channel_id,
                                )
                                .await
                                {
                                    Ok(_) => outcome.channels_left += 1,
                                    Err(e) => {
                                        outcome.channels_leave_failed += 1;
                                        tracing::warn!(
                                            error = ?e,
                                            account_id = %account_id,
                                            workspace_id = %workspace_id,
                                            channel_id = %channel_id,
                                            "slack disconnect purge: conversations.leave failed \
                                             for one channel — continuing (best-effort, never \
                                             aborts the purge)"
                                        );
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            tracing::warn!(
                                error = %e,
                                account_id = %account_id,
                                workspace_id = %workspace_id,
                                "slack disconnect purge: failed to enumerate bot-member channels \
                                 — skipping conversations.leave entirely for this disconnect \
                                 (best-effort, never aborts the purge)"
                            );
                        }
                    }
                })
                .await;
                if timed.is_err() {
                    outcome.leave_timed_out = true;
                    tracing::warn!(
                        account_id = %account_id,
                        workspace_id = %workspace_id,
                        budget_secs = LEAVE_PHASE_BUDGET.as_secs(),
                        channels_left_before_timeout = outcome.channels_left,
                        connector_health = true,
                        "slack disconnect purge: conversations.leave phase exceeded its total \
                         time budget — some channels may not have been left; nothing else in \
                         this disconnect is affected"
                    );
                }
            }
            Err(e) => {
                outcome.leave_skipped_check_failed = true;
                tracing::warn!(
                    error = %e,
                    account_id = %account_id,
                    workspace_id = %workspace_id,
                    connector_health = true,
                    "slack disconnect purge: failed to check for a co-installed account — \
                     skipping conversations.leave entirely rather than guessing whether the bot \
                     is shared"
                );
            }
        }
    }

    // `connector_identities` and every captured atom: untouched. See this
    // module's own header — disconnect withdraws consent to keep capturing,
    // it is not an erasure request.

    outcome
}
