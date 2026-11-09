//! "Email the unmatched" worker (chat-per-user plan PR 6,
//! `docs/plans/chat-per-user-ingest-and-in-chat-ask.md` §8 row 6).
//!
//! Tenant-looped, same shape as [`crate::workers::slack_seat_reconcile`]:
//! each plane's `jobs` table is invisible to a dequeuer scoped to any other
//! one (`workers::tenant_harness::run_multi_tenant`), so a job landed on an
//! org's own tenant `jobs` table is only ever picked up by that org's own
//! tick. `control_pool` is the shared control/consumer pool, needed for the
//! connected member's `slack_user` token (control-only `oauth_tokens`), the account's
//! `clerk_org_id` (for resolving which web app subdomain the "create your
//! account" link should point at), AND — corrected in this PR —
//! [`services::unmatched_recipient_email::claim_notice_slot`]'s cooldown
//! claim. Only the roster-walk READ (`discover_unmatched_recipients`) runs
//! against whichever plane `t.pool` is for this tick; the cooldown
//! claim/suppression-list write and the send itself are CONTROL-plane,
//! unconditionally, regardless of whether this tick is a Consumer or an Org
//! tenant. See `services::unmatched_recipient_email`'s own "Control-plane
//! only, not per-plane" doc section for why: that table has no tenant-tree
//! twin, and the public unsubscribe route that must later resolve this same
//! row has no per-request plane to pick one with. An earlier version of this
//! worker claimed on `t.pool` instead, which silently wrote an org account's
//! notice row into that org's own tenant database — a database the public
//! route could never reach — rather than failing loudly. Do not revert this
//! without also restoring `migrations_tenant/0093`/`0095`, which this PR
//! deletes for exactly that reason.
//!
//! One job = one (account, channel) roster walk (see
//! `jobs::JobKind::EmailUnmatchedSlackChannelMembers`'s own doc comment for
//! why it is enqueued at the channel self-join gate event). This worker does
//! not invent its own roster walk or its own classification: it calls
//! `connectors::slack_unmatched_email::discover_unmatched_recipients` for
//! the former and `services::unmatched_recipient_email` for the cooldown
//! claim + send.
//!
//! # D4 — one email, one account, chosen deterministically (docs/plans/
//! org-onboarding-from-the-invite.md §3 item 2, CS-427)
//!
//! `handle_member_joined` (`routes/slack_events.rs`) enqueues one of THESE
//! jobs per account whose bot self-joins a channel — and one Slack
//! workspace can map to several CoralSwarm accounts (a personal install and
//! one or more orgs, that route's own doc comment). Before this correction,
//! every such job independently claimed and sent under its OWN `account_id`
//! (the cooldown key included it), so the SAME real person, unmatched
//! everywhere, could receive one nudge per account within the same cooldown
//! week — see migration `0230`'s own header for the exact production shape.
//!
//! [`process_job`] now resolves, once per tick, every sibling account
//! mapped to this same workspace (`routes::slack::resolve_slack_account_
//! planes`) and, for EACH recipient discovered, calls `connectors::slack_
//! unmatched_email::select_notice_account` to pick exactly one of them.
//! Only when the winner is THIS job's own `account_id` does it proceed to
//! claim + send; every sibling job, running its own tick (now or later,
//! before or after this one), computes the SAME candidate facts and reaches
//! the SAME winner — see `earliest_connected_account_among`'s own doc
//! comment for why tier 2 is deliberately not "whichever account happens to
//! be running right now". The persisted, now person-keyed cooldown (migration `0230`)
//! is the safety net underneath this — even if the selection ever
//! disagreed across two ticks (a sibling-account-list resolution failure on
//! one of them, say), at most one `claim_notice_slot` call could still ever
//! win for a given recipient.

use std::sync::Arc;

use sqlx::PgPool;
use uuid::Uuid;

use crate::jobs::{queue, JobKind};
use crate::tenancy::scope::begin_tenant_system_scoped;
use crate::tenancy::TenantId;
use crate::workers::tenant_harness::{Tick, WorkerTenant};
use crate::AppState;

const JOB_BATCH_SIZE: usize = 5;
/// How long to sleep between polls when the queue is empty — same idle-floor
/// convention as `slack_seat_reconcile::EMPTY_POLL_SLEEP`.
pub const EMPTY_POLL_SLEEP: std::time::Duration = std::time::Duration::from_secs(30);

/// Hard per-tick cap on how many recipients [`process_job`] even LOOKS at
/// (CodeRabbit round 1, #1005, Stability & Availability) — `discover_
/// unmatched_recipients` can return up to 50,000 deduplicated members (50
/// pages x 1,000), and every one costs at least a `claim_notice_slot` round
/// trip, up to a 10s `send_nudge_email` call when claimed. 200 bounds a
/// single tick to a worst case of well under an hour (200 x 10s) while still
/// being generous for a genuinely large real channel; see [`process_job`]'s
/// own doc comment for how the remainder is picked up by a continuation job
/// rather than silently dropped.
const MAX_RECIPIENTS_PER_TICK: usize = 200;

/// `accounts.clerk_org_id` for `account_id` — `None` for a personal account.
/// Control-only (bare `SELECT`, no tenant twin — same convention
/// `slack_seat_reconcile`'s own doc comment describes for `oauth_tokens`).
/// Resolved ONCE per job by `process_job` (not once per recipient) — shared
/// by [`resolve_web_base`] (which web app subdomain to link to) and, new in
/// CS-428 (D12, item 3), the per-recipient tombstone gate below, so a job
/// touching many recipients pays this lookup exactly once, same "once per
/// walk" discipline `connectors::slack::resolve_anchor_domain` documents for
/// itself.
async fn resolve_account_clerk_org_id(control_pool: &PgPool, account_id: Uuid) -> Option<String> {
    sqlx::query_scalar("SELECT clerk_org_id FROM accounts WHERE id = $1")
        .bind(account_id)
        .fetch_optional(control_pool)
        .await
        .ok()
        .flatten()
}

/// Whether this job's account is an org that is being deleted or is revoked.
/// `Err` (a failed read) fails the job so it retries rather than sending.
async fn job_org_is_closing(control_pool: &PgPool, account_id: Uuid) -> Result<bool, String> {
    let org: Option<Option<String>> = sqlx::query_scalar("SELECT clerk_org_id FROM accounts WHERE id = $1")
        .bind(account_id)
        .fetch_optional(control_pool)
        .await
        .map_err(|e| format!("account lookup failed: {e}"))?;
    match org.flatten() {
        Some(org_id) => crate::services::org_deletion::org_closing_down(control_pool, &org_id)
            .await
            .map_err(|e| format!("org deletion state lookup failed: {e}")),
        None => Ok(false),
    }
}

/// Which web app subdomain the "create your account" link should point at.
/// `clerk_org_id` is [`resolve_account_clerk_org_id`]'s own result for this
/// job's `account_id`, resolved once by the caller — see that function's own
/// doc comment for why this no longer queries it directly.
async fn resolve_web_base(state: &AppState, control_pool: &PgPool, clerk_org_id: Option<&str>) -> String {
    match clerk_org_id {
        Some(org_id) => state.org_web_base_url(control_pool, org_id).await,
        None => state.personal_web_base_url(),
    }
}

/// The account, among `candidate_account_ids`, whose own connection to this
/// Slack workspace is the OLDEST — D4 tier 2's "the account that authorised
/// this channel" (`docs/plans/org-onboarding-from-the-invite.md` §3 item 2,
/// CS-427), computed as a fact every one of those accounts' independent
/// worker ticks can derive IDENTICALLY.
///
/// **Restricted to `candidate_account_ids` by construction (CodeRabbit round
/// 1, CS-427, Functional Correctness)** — an earlier version queried the
/// oldest `oauth_tokens` row for this WHOLE workspace, independently of
/// which accounts `resolve_slack_account_planes` actually resolved as
/// siblings. The two sets can genuinely differ: `resolve_slack_account_ids`
/// prefers a non-empty `connector_installs` index over `oauth_tokens`
/// without reconciling the two, and `resolve_slack_account_planes` drops
/// any account whose `accounts` row is gone. If the independently-queried
/// "oldest" account was not itself a candidate, NO candidate would ever
/// have `authorized_this_channel = true` for a genuinely `Unmatched`
/// recipient (tier 1 is empty too — see `NoticeAccountCandidate::roster_
/// holds_verified_address`'s own doc comment) — `select_notice_account`
/// would return `None`, silently suppressing the notice on every tick whose
/// own account is not the omitted one (the omitted account's OWN tick would
/// still self-heal via the forced-insertion in [`process_job`], but that
/// tick may never legitimately run). Filtering the SECURITY DEFINER
/// resolver's full ordered result down to `candidate_account_ids` in Rust
/// (rather than pushing a second SQL predicate into the function call)
/// guarantees the answer, when `Some`, is ALWAYS a member of the same slice
/// [`connectors::slack_unmatched_email::select_notice_account`] iterates —
/// never an account outside it.
///
/// This is still a proxy, not a literal per-channel fact:
/// `slack_watched_channels` (a human's own destination pick for THIS
/// specific channel, once they answer the Block Kit prompt) would be more
/// precise, but nothing guarantees that row exists yet by the time this job
/// runs — the self-join event that enqueues this job fires BEFORE a human
/// can possibly have answered the prompt it posts. "Whichever candidate
/// connected this Slack workspace first" is the next most stable,
/// always-available fact this worker can compute without new schema.
/// Unlike `routes::slack_events::is_bot_self_join` (`join_user == this
/// account's own bot_user_id`) — inherently self-referential, since every
/// account's own self-join event names ITSELF — this gives every sibling
/// account's tick the SAME answer regardless of which one happens to be
/// running, which is the actual property `select_notice_account`'s callers
/// need: a deterministic, not a first-to-run, winner.
///
/// **`None` is a real, deliberate outcome, never patched over with a
/// per-tick default.** If no `oauth_tokens` row among `candidate_account_
/// ids` can be found (a DB error, or — should not normally happen, since
/// this job's own account always has one — every candidate genuinely
/// lacking a row), the caller must NOT fall back to "this tick's own
/// account": that would make the tier-2 winner vary by which tick is
/// asking, exactly the non-determinism this whole mechanism exists to
/// avoid (CodeRabbit round 1, CS-427). A tick that cannot determine a
/// winner skips the recipient instead — see [`select_notice_account`]'s own
/// "ask, don't guess" contract.
///
/// **Goes through the SECURITY DEFINER `resolve_oauth_token_account_ids`
/// (migration `0175`), never a raw `SELECT ... FROM oauth_tokens`.**
/// `oauth_tokens` is RLS-covered (migration `0083`) with the strict,
/// no-`missing_ok` `current_setting('app.account_id')` predicate this
/// file's own module doc already warns about for every OTHER control-only
/// lookup — a bare query with no scope set does not quietly return zero
/// rows, it ERRORS in production (`check_rls_coverage.sh`'s own header:
/// OPS-4 made this live enforcement, not merely a future backstop). That
/// function ALREADY orders its result `ORDER BY created_at, account_id`
/// (see its own definition) — exactly the ordering this needs — and, being
/// `SECURITY DEFINER`, answers correctly with no GUC set at all, the same
/// way `routes::slack::resolve_slack_account_ids` already relies on it for
/// the identical workspace-wide (not one-account-scoped) question. Fetching
/// the FULL ordered set (not `LIMIT 1`) and filtering in Rust — rather than
/// adding an `= ANY($2)` predicate to the SQL call — keeps this a plain
/// consumer of the existing function's contract, never a second, drifting
/// copy of its `WHERE` clause.
async fn earliest_connected_account_among(
    control_pool: &PgPool,
    workspace_id: &str,
    candidate_account_ids: &[Uuid],
) -> Option<Uuid> {
    let ordered: Vec<Uuid> = sqlx::query_scalar("SELECT resolve_oauth_token_account_ids('slack', $1)")
        .bind(workspace_id)
        .fetch_all(control_pool)
        .await
        .unwrap_or_else(|e| {
            tracing::warn!(
                error = %e, workspace_id,
                "slack-unmatched-email: failed to resolve the earliest-connected account for D4 \
                 selection"
            );
            Vec::new()
        });
    ordered.into_iter().find(|id| candidate_account_ids.contains(id))
}

/// Every `(account_id, normalized_email)` pair, among `candidate_account_
/// ids` x `emails`, where that account's own member roster (`account_
/// members` ⋈ `users`) already has a verified user at that address — D4
/// tier 1, batched ONCE per tick for the whole set of recipients discovered
/// this pass rather than once per (candidate, recipient) pair. See
/// `connectors::slack_unmatched_email::NoticeAccountCandidate::roster_
/// holds_verified_address`'s own doc comment for why the result is empty in
/// practice for a genuinely `Unmatched` recipient, and why this is
/// implemented faithfully anyway.
///
/// CONTROL-only: both `account_members` and `users` are control-plane
/// tables with no tenant-tree twin. Best-effort: a query failure logs and
/// returns empty (no roster match for anyone) rather than failing the whole
/// tick over this one, non-essential tier.
async fn accounts_whose_roster_holds(
    control_pool: &PgPool,
    candidate_account_ids: &[Uuid],
    emails: &[String],
) -> std::collections::HashSet<(Uuid, String)> {
    if candidate_account_ids.is_empty() || emails.is_empty() {
        return std::collections::HashSet::new();
    }
    let rows: Vec<(Uuid, String)> = sqlx::query_as(
        "SELECT am.account_id, lower(btrim(u.email)) \
         FROM account_members am \
         JOIN users u ON u.id = am.user_id AND u.deleted_at IS NULL \
         WHERE am.account_id = ANY($1) AND lower(btrim(u.email)) = ANY($2)",
    )
    .bind(candidate_account_ids)
    .bind(emails)
    .fetch_all(control_pool)
    .await
    .unwrap_or_else(|e| {
        tracing::warn!(
            error = %e,
            "slack-unmatched-email: roster-verified-address lookup failed — treating as no \
             match rather than blocking the whole tick"
        );
        Vec::new()
    });
    rows.into_iter().collect()
}

/// Whether `process_job` has ANY work at all this walk — either audience
/// non-empty. Pure and DB-free by construction: a plain predicate over two
/// already-discovered slices, extracted specifically so [`process_job`]'s
/// own early-return guard is directly unit-testable (see that guard's own
/// comment — CodeRabbit round 1, #1026, Functional Correctness) without
/// needing a full live-Postgres integration harness.
fn has_any_audience(
    recipients: &[crate::connectors::slack_unmatched_email::UnmatchedRecipient],
    matched_not_members: &[crate::connectors::slack_unmatched_email::MatchedNotMember],
) -> bool {
    !recipients.is_empty() || !matched_not_members.is_empty()
}

#[cfg(test)]
mod has_any_audience_tests {
    //! Pure, DB-and-network-free — pins the exact regression CodeRabbit
    //! round 1 (#1026) found: `process_job` must not early-return when
    //! `recipients` is empty but `matched_not_members` is not.

    use super::has_any_audience;
    use crate::connectors::slack_unmatched_email::{MatchedNotMember, UnmatchedRecipient};
    use uuid::Uuid;

    fn recipient() -> UnmatchedRecipient {
        UnmatchedRecipient { email: "unmatched@example.com".to_string() }
    }
    fn matched_not_member() -> MatchedNotMember {
        MatchedNotMember {
            user_id: Uuid::new_v4(),
            email: "not-a-member@example.com".to_string(),
            slack_user_id: "U1".to_string(),
            // CS-447: this suite is about the AUDIENCE predicate, not the
            // classification. `FullMember` is the ordinary case of the
            // audience it is testing.
            class: crate::connectors::slack::MembershipClass::FullMember,
        }
    }

    #[test]
    fn both_empty_yields_false() {
        assert!(!has_any_audience(&[], &[]));
    }

    /// FAILS WITHOUT THE FIX: zero Unmatched recipients but a real
    /// Matched-not-member must still count as work — this is the exact
    /// shape that used to return `Ok(0)` before ever reaching the
    /// join-this-org pass.
    #[test]
    fn matched_not_member_alone_yields_true() {
        assert!(has_any_audience(&[], &[matched_not_member()]));
    }

    #[test]
    fn recipient_alone_yields_true() {
        assert!(has_any_audience(&[recipient()], &[]));
    }

    #[test]
    fn both_present_yields_true() {
        assert!(has_any_audience(&[recipient()], &[matched_not_member()]));
    }
}

/// One channel's worth of work: discover, then claim-and-send per recipient,
/// bounded at [`MAX_RECIPIENTS_PER_TICK`] — a roster past that cap gets a
/// continuation `JobKind::EmailUnmatchedSlackChannelMembers` enqueued for
/// the untouched remainder (same idempotency key, so it naturally makes
/// progress across ticks: recipients already claimed this pass are cheap
/// cooldown-skips on the next one, not repeated Resend calls). Returns the
/// number of emails actually SENT (used only to pick `DidWork`/
/// `NothingToDo` for the job-completion log — never load-bearing for
/// correctness). `Err` — including a failed continuation enqueue — fails
/// this job for `tick`'s caller to retry, rather than completing it with an
/// unrecorded remainder silently dropped forever.
async fn process_job(
    state: &Arc<AppState>,
    _tenant: &TenantId,
    control_pool: &PgPool,
    pool: &PgPool,
    http: &reqwest::Client,
    account_id: Uuid,
    workspace_id: &str,
    channel_id: &str,
) -> Result<usize, String> {
    // In-app org deletion: a job queued before the deletion (or before the
    // org was revoked) must not send invite or join-request emails for an org
    // that is closing down. Dropped, not retried: nothing brings the org back
    // except an undo, and the next gate event re-enqueues then. A read error
    // retries.
    if job_org_is_closing(control_pool, account_id).await? {
        tracing::info!(
            account_id = %account_id, channel_id, workspace_id,
            "slack-unmatched-email: org is being deleted or revoked - dropping the job unsent"
        );
        return Ok(0);
    }

    // Item 50: the roster walk runs on a connected member's own grant.
    // The workspace bot token is not a reader. No connected member means
    // there is nothing to invite from yet — retry, do not fall back.
    let reader = match crate::connectors::slack_member_capture::invite_reader_user_id(
        control_pool,
        account_id,
        workspace_id,
        channel_id,
    )
    .await
    {
        Ok(Some(user_id)) => user_id,
        Ok(None) => {
            return Err("no connected member token for this workspace".to_string());
        }
        Err(e) => return Err(format!("member token lookup failed: {e}")),
    };
    let member_token = match crate::connectors::slack_member_capture::load_member_token(
        state,
        control_pool,
        account_id,
        reader,
    )
    .await
    {
        Some(token) => token,
        None => return Err("connected member token could not be decrypted".to_string()),
    };

    // `Err` here means the roster could not be fully trusted — see
    // `discover_unmatched_recipients`'s own doc comment. Propagating it
    // fails the job (retried with backoff, same as any other worker error)
    // rather than silently sending nothing and reporting success.
    // `mut`: this job's own per-recipient loop below increments
    // `outcome.tombstoned_skipped` (CS-428, D12, item 3) — see that field's
    // own doc comment for why it is populated here rather than inside
    // `discover_unmatched_recipients`.
    let mut outcome = crate::connectors::slack_unmatched_email::discover_unmatched_recipients(
        control_pool,
        pool,
        http,
        account_id,
        workspace_id,
        &member_token,
        channel_id,
    )
    .await?;

    // CS-426 (D2, D21): the guest/Slack-Connect domain filter's skip counts
    // — see `DiscoveryOutcome`'s own doc comment for why these are logged
    // here rather than persisted to a table yet (a durable, cross-restart
    // "N guests skipped" surface is left as deliberate follow-up). Logged
    // only when non-zero, same "quiet unless there is something to see"
    // discipline the per-tick recipient-cap log below this function
    // follows.
    if outcome.guests_skipped > 0
        || outcome.slack_connect_skipped > 0
        || outcome.unavailable_membership_skipped > 0
    {
        tracing::info!(
            account_id = %account_id, channel_id, workspace_id,
            guests_skipped = outcome.guests_skipped,
            slack_connect_skipped = outcome.slack_connect_skipped,
            unavailable_membership_skipped = outcome.unavailable_membership_skipped,
            "slack-unmatched-email: guest/Slack-Connect filter (CS-426) skipped candidates this \
             walk — not invited because their domain did not match this account's anchor, or \
             their membership fields were unavailable"
        );
    }

    let recipients = outcome.recipients;
    // CodeRabbit round 1 (#1026, Functional Correctness): must NOT early-
    // return on an empty `recipients` alone — `outcome.matched_not_members`
    // (D19/CS-440) is populated independently by the SAME discovery walk,
    // and a channel with zero Unmatched-but-at-least-one Matched-not-member
    // roster would otherwise skip the join-this-org pass entirely and
    // report `Ok(0)` before ever reaching it. Everything below this guard —
    // `account_clerk_org_id`/`web_base`/`sibling_account_ids`/
    // `authorized_account_id` — is computed unconditionally regardless of
    // which audience is non-empty, and the Unmatched `for` loop below is
    // itself a no-op over an empty iterator, so widening this guard is
    // sufficient; nothing further needs to move. Extracted into a pure,
    // directly unit-testable predicate (`has_any_audience`) rather than
    // inlined, so this exact regression is pinned without needing a full
    // live-Postgres `process_job` harness — no sibling suite in this file
    // builds one (`tombstone_gate_pg_tests`'s own doc comment: "Deliberately
    // does not exercise discover_unmatched_recipients, a mock Slack server,
    // or a bot token/DEK"), and this predicate is the one place that
    // decision actually lives.
    if !has_any_audience(&recipients, &outcome.matched_not_members) {
        return Ok(0);
    }

    // Resolved ONCE per job — see `resolve_account_clerk_org_id`'s own doc
    // comment. Shared by `resolve_web_base` below and, new in this PR, the
    // per-recipient D12 tombstone gate further down this loop.
    let account_clerk_org_id = resolve_account_clerk_org_id(control_pool, account_id).await;
    let web_base = resolve_web_base(state, control_pool, account_clerk_org_id.as_deref()).await;

    // D4 (docs/plans/org-onboarding-from-the-invite.md §3 item 2, CS-427):
    // resolve the FULL sibling candidate set once per tick, not once per
    // recipient — every account mapped to this SAME Slack workspace is a
    // plausible sender for anyone this tick discovers, and the set itself
    // does not vary across recipients. A resolver failure falls back to
    // "this job's own account is the only candidate" — i.e. exactly this
    // PR's pre-fix behavior for this one tick — rather than letting a
    // transient lookup failure block every send.
    let sibling_account_ids: Vec<Uuid> =
        match crate::routes::slack::resolve_slack_account_planes(control_pool, workspace_id).await
        {
            Ok(matches) => {
                let mut ids: Vec<Uuid> = matches.into_iter().map(|(id, _)| id).collect();
                // This job's own account MUST always be a candidate — it
                // already proved it has a live bot token for this workspace
                // (the lookup above this block succeeded). If the resolver
                // somehow omitted it (a transient inconsistency between
                // `connector_installs`/`oauth_tokens` and this job's own
                // dequeue), every recipient this tick would otherwise be
                // silently skipped forever (an empty candidate list makes
                // `select_notice_account` return `None` for everyone) —
                // defend against that rather than let a resolver gap turn
                // into a total feature lockout.
                if !ids.contains(&account_id) {
                    ids.push(account_id);
                }
                ids
            }
            Err(e) => {
                tracing::warn!(
                    error = %e, account_id = %account_id, workspace_id,
                    "slack-unmatched-email: failed to resolve sibling accounts for D4 selection \
                     — falling back to this job's own account only"
                );
                vec![account_id]
            }
        };
    // CodeRabbit round 1 (CS-427, Functional Correctness): computed AFTER
    // `sibling_account_ids` is final (including the forced self-insertion
    // above) and restricted to that exact slice — see `earliest_connected_
    // account_among`'s own doc comment for why an independently-queried
    // "oldest oauth_tokens row for this workspace" could name an account
    // outside the candidate set `select_notice_account` actually iterates,
    // silently suppressing every notice for a recipient no candidate could
    // then win. `None` here is NOT papered over with `.unwrap_or(account_
    // id)` — a per-tick default would make the tier-2 winner vary by which
    // account's tick happens to be running, exactly the non-determinism
    // this whole mechanism exists to prevent.
    let authorized_account_id: Option<Uuid> =
        earliest_connected_account_among(control_pool, workspace_id, &sibling_account_ids).await;
    let recipient_emails: Vec<String> = recipients.iter().map(|r| r.email.clone()).collect();
    let roster_matches =
        accounts_whose_roster_holds(control_pool, &sibling_account_ids, &recipient_emails).await;

    // Bound TOTAL recipients touched this tick, not only `sent` (CodeRabbit
    // round 1, #1005, Stability & Availability): `discover_unmatched_
    // recipients` can return up to 50,000 deduplicated members (50 pages x
    // 1,000), and every one of them can cost a `claim_notice_slot` round
    // trip plus, when claimed, a `send_nudge_email` call that can wait up to
    // 10s. A skipped-claim/failed-send recipient costs almost nothing, so
    // capping only `sent` would let a channel full of already-cooled-down or
    // already-suppressed members walk the WHOLE roster anyway — this counts
    // every recipient the loop even LOOKS at.
    let total = recipients.len();
    let mut sent = 0usize;
    let mut processed = 0usize;
    let mut recipients = recipients.into_iter();
    for recipient in recipients.by_ref().take(MAX_RECIPIENTS_PER_TICK) {
        processed += 1;

        // D4 — one email, one account, chosen deterministically
        // (docs/plans/org-onboarding-from-the-invite.md §3 item 2, CS-427).
        // Every sibling account's own tick for this SAME workspace builds
        // the IDENTICAL candidate list for this recipient and reaches the
        // IDENTICAL winner (see `earliest_connected_account_among`'s own doc
        // comment for why tier 2 is order-independent) — so exactly one of
        // them proceeds past this point for this recipient, regardless of
        // which tick happens to run first or how many times a re-delivered
        // event re-enqueues this same job.
        let candidates: Vec<crate::connectors::slack_unmatched_email::NoticeAccountCandidate> =
            sibling_account_ids
                .iter()
                .map(|&candidate_id| crate::connectors::slack_unmatched_email::NoticeAccountCandidate {
                    account_id: candidate_id,
                    roster_holds_verified_address: roster_matches
                        .contains(&(candidate_id, recipient.email.clone())),
                    authorized_this_channel: authorized_account_id == Some(candidate_id),
                })
                .collect();
        match crate::connectors::slack_unmatched_email::select_notice_account(&candidates) {
            Some(winner) if winner == account_id => {}
            Some(winner) => {
                tracing::debug!(
                    account_id = %account_id, winner = %winner, channel_id,
                    "slack-unmatched-email: a sibling account is D4-preferred for this \
                     recipient — skipping, that account's own pass is responsible for sending"
                );
                continue;
            }
            None => {
                // Defensive floor only — see `select_notice_account`'s own
                // doc comment; `sibling_account_ids` always includes at
                // least this job's own account (the fallback above), so
                // reaching `None` means every candidate somehow failed
                // BOTH tiers, which never legitimately happens for the
                // account this job itself belongs to.
                tracing::warn!(
                    account_id = %account_id, channel_id,
                    "slack-unmatched-email: D4 selection returned no candidate at all — \
                     skipping this recipient rather than guessing"
                );
                continue;
            }
        }

        // CS-428 (`docs/plans/org-onboarding-from-the-invite.md` §3 item 3,
        // D12): the match arm above already established `account_id` as the
        // WINNER D4 selected for this recipient — "the chosen account's
        // org" D12's rule means is therefore exactly `account_clerk_org_id`,
        // resolved once above. `None` (a personal account) short-circuits
        // inside `invite_suppressed_by_tombstone` without a query — see that
        // function's own doc comment. Suppressed here means never claimed
        // and never sent — a suppressed skip must write NO notice row and
        // must never start this address's cooldown, same reasoning CS-426
        // uses for a guest/Slack-Connect skip (`continue` before the claim
        // below, not after it).
        match crate::clerk_provision::invite_suppressed_by_tombstone(
            control_pool,
            account_clerk_org_id.as_deref(),
            None,
            Some(&recipient.email),
        )
        .await
        {
            Ok(true) => {
                outcome.tombstoned_skipped += 1;
                tracing::debug!(
                    account_id = %account_id, channel_id, workspace_id,
                    "slack-unmatched-email: recipient is tombstoned for this org (D12) — \
                     suppressed, never invited, never cools down"
                );
                continue;
            }
            Ok(false) => {}
            Err(e) => {
                // Best-effort, like every other non-essential read this loop
                // already tolerates (`accounts_whose_roster_holds`,
                // `earliest_connected_account_among`): a failed tombstone
                // check falls back to "not suppressed" rather than blocking
                // this recipient over an unrelated read failure — this is
                // exactly this feature's ABSENCE, i.e. the behavior every
                // recipient had before this PR, not a new hazard it
                // introduces.
                tracing::warn!(
                    error = %e, account_id = %account_id, channel_id,
                    "slack-unmatched-email: D12 tombstone check failed — treating as not \
                     suppressed rather than blocking this recipient"
                );
            }
        }

        // Claim FIRST, in its own committed transaction, THEN send — never
        // the other way around. If the send crashed the process between the
        // two, "claimed but never sent" is the safe failure (this person
        // waits one cooldown window for a second attempt); "sent but never
        // claimed" would be the unsafe one (a retried/redelivered job could
        // send the same person twice with no record stopping it).
        //
        // `control_pool`, NEVER `pool` (this tick's plane-specific pool) —
        // see this file's own module doc for why. `slack_unmatched_email_
        // notices` has no tenant-tree twin, so opening this on `pool` for an
        // Org tick would either write into a database the public unsubscribe
        // route can never resolve (the original bug) or, now that the twin
        // is deleted, fail outright with "relation does not exist" — loud
        // and immediate instead of silent.
        let mut tx = match begin_tenant_system_scoped(control_pool, account_id).await {
            Ok(tx) => tx,
            Err(e) => {
                tracing::warn!(
                    error = %e, account_id = %account_id, channel_id,
                    "slack-unmatched-email: failed to open scoped transaction for cooldown claim \
                     — skipping this recipient, continuing with the rest"
                );
                continue;
            }
        };
        // `claimed` is now the recipient's stable unsubscribe + signup-
        // prefill tokens (see `claim_notice_slot`'s own doc comment) rather
        // than a bare bool — `send_nudge_email` below needs both to build
        // the `List-Unsubscribe` header, the in-body unsubscribe link, and
        // the signup link (CodeRabbit round 1, #1005: no longer the raw
        // address) for THIS send. `None` covers every refusal reason
        // identically (lost the race, still in cooldown, or — this PR's
        // addition — already unsubscribed): none of them are this worker's
        // business to distinguish, only to skip.
        let claimed = match crate::services::unmatched_recipient_email::claim_notice_slot(
            &mut *tx,
            account_id,
            "slack",
            workspace_id,
            channel_id,
            &recipient.email,
        )
        .await
        {
            Ok(c) => c,
            Err(e) => {
                tx.rollback().await.ok();
                tracing::warn!(
                    error = %e, account_id = %account_id, channel_id,
                    "slack-unmatched-email: cooldown claim query failed — skipping this \
                     recipient, continuing with the rest"
                );
                continue;
            }
        };
        let Some(tokens) = claimed else {
            // Someone else's concurrent claim won, this recipient is still
            // inside their cooldown window from an earlier send, or they
            // have unsubscribed. Either way: no rows were changed, so there
            // is nothing to commit, but rolling back explicitly (rather
            // than letting drop do it) keeps the "every claimed path is
            // `commit`, every not-claimed path is `rollback`" invariant
            // grep-able.
            tx.rollback().await.ok();
            continue;
        };
        if let Err(e) = tx.commit().await {
            tracing::warn!(
                error = %e, account_id = %account_id, channel_id,
                "slack-unmatched-email: failed to commit cooldown claim — skipping this send \
                 rather than risk sending with no durable record of it"
            );
            continue;
        }

        if crate::services::unmatched_recipient_email::send_nudge_email(
            state,
            &recipient.email,
            &web_base,
            &tokens.unsubscribe_token,
            &tokens.prefill_token,
        )
        .await
        {
            sent += 1;
        }
        // A failed send after a successful claim is an accepted, narrow
        // residual — see `services::unmatched_recipient_email::send_nudge_
        // email`'s own doc comment: this person simply waits out the
        // cooldown window before a later roster walk retries them.
    }

    // CS-428 (D12, item 3): logged only when non-zero, same "quiet unless
    // there is something to see" discipline the guest/Slack-Connect skip log
    // above follows.
    if outcome.tombstoned_skipped > 0 {
        tracing::info!(
            account_id = %account_id, channel_id, workspace_id,
            tombstoned_skipped = outcome.tombstoned_skipped,
            "slack-unmatched-email: D12 tombstone gate (CS-428) suppressed candidates this walk \
             — previously removed from this org, never invited, never cooled down"
        );
    }

    // `recipients` is an `ExactSizeIterator` (`Vec::into_iter`) advanced by
    // exactly `processed` items above via `by_ref().take(..)` — its
    // remaining `.len()` is precisely the un-touched tail of the roster.
    let remaining = recipients.len();
    if remaining > 0 {
        tracing::info!(
            account_id = %account_id, channel_id, processed, remaining, total,
            "slack-unmatched-email: hit the per-tick recipient cap — enqueuing a continuation \
             rather than walking the rest of a large roster inline"
        );
        // SAME idempotency key `routes::slack_events`'s original enqueue
        // uses (`email-unmatched:{account_id}:{workspace_id}:{channel_id}`)
        // — deliberately, not a fresh one. `queue::enqueue`'s dedup is
        // scoped to `pending` jobs only (`idx_jobs_idem_pending`), and THIS
        // job already left `pending` the moment `queue::dequeue` claimed it
        // (before `process_job` ever ran), so re-using the key here cannot
        // collide with the job currently executing — it can only dedupe
        // against ANOTHER continuation already enqueued for this exact
        // (account, workspace, channel), which is the correct behavior if
        // two ticks somehow raced this decision.
        //
        // `Err` here propagates (see this function's own return type) —
        // `tick`'s caller then calls `queue::fail`, NOT `queue::complete`,
        // for the CURRENT job, so it retries with backoff rather than being
        // acknowledged with an unrecorded remainder silently dropped
        // forever. The already-claimed prefix costs almost nothing on
        // retry: those recipients are now inside their cooldown window, so
        // `claim_notice_slot` skips them without a Resend call.
        crate::jobs::queue::enqueue(
            pool,
            &JobKind::EmailUnmatchedSlackChannelMembers {
                account_id,
                workspace_id: workspace_id.to_string(),
                channel_id: channel_id.to_string(),
            },
            Some(account_id),
            Some(&format!("email-unmatched:{account_id}:{workspace_id}:{channel_id}")),
            0,
        )
        .await
        .map_err(|e| format!("failed to enqueue continuation job: {e}"))?;
    }

    // D19/CS-440 (org-onboarding plan §3 item 29, "as amended by D21") —
    // the "join this org" audience `discover_unmatched_recipients` also
    // collected on this SAME walk (never a second roster fetch — see that
    // function's own "Matched-not-member audience" doc section). Processed
    // after the Unmatched loop above, sharing its already-resolved
    // `sibling_account_ids`/`authorized_account_id`/`account_clerk_org_id`
    // facts rather than re-deriving any of them.
    if !outcome.matched_not_members.is_empty() {
        sent += process_matched_not_members(
            state,
            control_pool,
            account_id,
            workspace_id,
            channel_id,
            account_clerk_org_id.as_deref(),
            &sibling_account_ids,
            authorized_account_id,
            &outcome.matched_not_members,
        )
        .await;
    }

    Ok(sent)
}

/// The org's Clerk display name for CS-440's join-this-org email copy —
/// resolved via the Clerk Admin API, the SAME call `routes::org_invites::
/// org_display_name` makes for its own invite email (`org_invites.rs:849`),
/// duplicated here rather than imported (that function is private to that
/// module) — `services::unmatched_recipient_email` stays deliberately
/// Clerk-free (see `workers::member_connect_nudge`'s own module doc for why
/// "reaching out to the Clerk API from inside a control-plane sweep for
/// copy alone" was rejected for ITS email), so this lives at the WORKER
/// layer and hands the services layer an already-resolved `Option<&str>`,
/// same shape `org_invites.rs` itself uses.
///
/// This call IS worth the round trip, unlike `member_connect_nudge`'s: it
/// happens at most ONCE per JOB (gated on `!matched_not_members.is_empty()`
/// by this function's only caller), not once per grace-gated member on an
/// hourly sweep — the same "resolved once per job/walk, not once per
/// recipient" discipline this file already applies to
/// `resolve_account_clerk_org_id`/`resolve_web_base` above.
///
/// Sanitised the identical way `org_invites::safe_label` is (alnum plus a
/// small punctuation allowlist, 48 chars) — duplicated rather than imported
/// for the same "private to that module" reason as `org_display_name`
/// above; `services::unmatched_recipient_email::render_join_org_email_html`
/// still HTML-escapes whatever this returns before embedding it (defense in
/// depth — this filter alone does not make `&` safe to embed unescaped).
///
/// `None` on a missing `CLERK_SECRET_KEY`, a Clerk API error, or an empty/
/// unusable name — `services::unmatched_recipient_email::send_join_org_
/// email`'s caller (below) degrades to a generic "your team" label rather
/// than failing the whole send over a cosmetic detail.
async fn resolve_org_display_name(clerk_org_id: &str) -> Option<String> {
    let clerk = crate::clerk_admin::ClerkAdminClient::from_env()?;
    let name = clerk.get_organization(clerk_org_id).await.ok()?.name;
    let cleaned: String = name
        .chars()
        .filter(|c| c.is_alphanumeric() || matches!(c, ' ' | '.' | ',' | '\'' | '&' | '-'))
        .collect();
    let cleaned: String = cleaned.trim().chars().take(48).collect();
    (!cleaned.is_empty()).then_some(cleaned)
}

/// The join-this-org send pass (D19, CS-440) — everything after discovery:
/// D4 sender selection, the D12 tombstone gate, the D10/D19 existing-
/// request skip, the claim, and the send. A separate function from the
/// Unmatched loop above (not folded into the same `for` loop) because the
/// claim/send ORDER genuinely differs — see [`crate::services::
/// unmatched_recipient_email::send_join_org_email`]'s own doc comment: this
/// pass holds the claim's transaction open across the Resend call and only
/// commits on success (`workers::member_connect_nudge`'s shape), while the
/// Unmatched loop above commits the claim BEFORE sending (this file's own
/// module doc, "a failed send here is a lost nudge"). Returns the number of
/// emails actually sent, folded into `process_job`'s own `sent` count so a
/// tick that only did work here still reports `DidWork`, not
/// `NothingToDo`.
#[allow(clippy::too_many_arguments)]
async fn process_matched_not_members(
    state: &Arc<AppState>,
    control_pool: &PgPool,
    account_id: Uuid,
    workspace_id: &str,
    channel_id: &str,
    account_clerk_org_id: Option<&str>,
    sibling_account_ids: &[Uuid],
    authorized_account_id: Option<Uuid>,
    candidates: &[crate::connectors::slack_unmatched_email::MatchedNotMember],
) -> usize {
    // An ordinary org, always — `discover_unmatched_recipients` never
    // collects this audience for a personal org (D19), so a `None` here
    // means an inconsistent caller, not a real personal-org walk reaching
    // this far. Defensive rather than assumed: skip cleanly rather than
    // build a join link with no org to name.
    let Some(clerk_org_id) = account_clerk_org_id else {
        tracing::warn!(
            account_id = %account_id, channel_id, workspace_id,
            "slack-unmatched-email: CS-440 matched_not_members was non-empty for an account with \
             no clerk_org_id — refusing to guess an org, skipping this pass entirely"
        );
        return 0;
    };

    // Without a signing secret this pass cannot mint a single valid join
    // link — see `services::unmatched_recipient_email::join_notice_secret`'s
    // own doc comment. Skip the whole pass rather than send emails whose
    // one link can never verify.
    let Some(join_secret) = crate::services::unmatched_recipient_email::join_notice_secret() else {
        tracing::warn!(
            account_id = %account_id, channel_id, workspace_id,
            "slack-unmatched-email: CS-440 JOIN_NOTICE_SECRET is unset — skipping the \
             join-this-org pass entirely rather than sending emails with a dead link"
        );
        return 0;
    };

    // Same per-tick bound as the Unmatched audience — see `MAX_RECIPIENTS_
    // PER_TICK`'s own doc comment. No continuation-job shape for THIS
    // audience specifically (unlike the Unmatched loop's own `remaining >
    // 0` re-enqueue): a remainder is picked up by a later walk of this same
    // channel, or is simply smaller than a typical channel roster in
    // practice. Logged loudly rather than silently dropped either way
    // (`scripts::check_silent_drop_is_alertable.sh`'s own bar).
    let bounded = &candidates[..candidates.len().min(MAX_RECIPIENTS_PER_TICK)];
    let remaining = candidates.len() - bounded.len();
    if remaining > 0 {
        tracing::warn!(
            account_id = %account_id, channel_id, remaining,
            "slack-unmatched-email: CS-440 hit the per-tick cap on the join-this-org audience — \
             the remainder is not re-enqueued as its own continuation; a later walk of this same \
             channel, or the D4 cooldown's own 7-day floor, is the accepted follow-up"
        );
    }

    // Resolved once per job (gated on there being anyone to email at all),
    // never once per candidate — see `resolve_org_display_name`'s own doc
    // comment.
    let org_name = resolve_org_display_name(clerk_org_id).await;
    let org_label = org_name.as_deref().unwrap_or("your team");
    // `/join/{clerk_org_id}` is reachable on the SHARED app host, not this
    // org's own (retired) `{slug}.{web_apex}` subdomain — the recipient is, by
    // this audience's own definition, not yet a member of this org, so a
    // link scoped to that org is one this browser session cannot resolve to
    // anything useful. Same exception `AppState::web_base_url`'s own doc
    // comment names for `routes::shares`/`routes::session_shares`'s public
    // viewer route: "not every caller should pass the account's real
    // plane."
    let join_web_base = state.personal_web_base_url();

    let mut sent = 0usize;
    let mut tombstoned_skipped = 0usize;
    let mut already_requested_skipped = 0usize;

    for candidate in bounded {
        // D4 — sender selection, deliberately WITHOUT tier 1 (`roster_
        // holds_verified_address`). Tier 1 exists to prefer the account
        // whose OWN roster already holds the recipient's verified address —
        // for the Unmatched audience that account can never exist (see
        // `NoticeAccountCandidate::roster_holds_verified_address`'s own doc
        // comment), so it is a documented no-op there. For THIS audience it
        // is not a no-op — it would be actively WRONG:
        // `discover_unmatched_recipients` only ever adds someone to
        // `matched_not_members` BECAUSE this job's own `account_id`'s
        // roster does NOT hold them (the `account_members` check that gates
        // this audience at all). If a SIBLING account happens to be this
        // person's actual home org, THAT sibling's own roster genuinely
        // DOES hold their verified address — and letting tier 1 fire here
        // would route the "join this org" email to send FROM the org they
        // already belong to, inviting them to join their own organization.
        // (That sibling's own tick, running this same function against ITS
        // OWN roster, would have found this person already a member and
        // never added them to `matched_not_members` in the first place —
        // see `matched_member_skipped` — so it never even reaches this
        // selection for them.) Every candidate here is therefore built with
        // `roster_holds_verified_address: false` unconditionally, so
        // `select_notice_account` degrades to tier 2 only
        // (`authorized_this_channel`): "the org that discovered them via a
        // shared Slack channel sends", never "the org that already has
        // them."
        let selection_candidates: Vec<crate::connectors::slack_unmatched_email::NoticeAccountCandidate> =
            sibling_account_ids
                .iter()
                .map(|&candidate_id| crate::connectors::slack_unmatched_email::NoticeAccountCandidate {
                    account_id: candidate_id,
                    roster_holds_verified_address: false,
                    authorized_this_channel: authorized_account_id == Some(candidate_id),
                })
                .collect();
        match crate::connectors::slack_unmatched_email::select_notice_account(&selection_candidates) {
            Some(winner) if winner == account_id => {}
            Some(_) => continue,
            None => continue,
        }

        // D12 — same tombstone gate the Unmatched loop above uses, but WITH
        // `user_id` (this audience already knows who this is; the Unmatched
        // audience does not yet, hence `None` there) — same shape
        // `workers::member_connect_nudge::process_account`'s own tombstone
        // check uses for an already-identified candidate.
        match crate::clerk_provision::invite_suppressed_by_tombstone(
            control_pool,
            Some(clerk_org_id),
            Some(candidate.user_id),
            Some(&candidate.email),
        )
        .await
        {
            Ok(true) => {
                tombstoned_skipped += 1;
                continue;
            }
            Ok(false) => {}
            Err(e) => {
                tracing::warn!(
                    error = %e, account_id = %account_id, user_id = %candidate.user_id,
                    "slack-unmatched-email: CS-440 D12 tombstone check failed — treating as not \
                     suppressed rather than blocking this candidate"
                );
            }
        }

        // D10/D19 — an existing OPEN request means the email already did
        // its job; an existing REJECTED request is D10's durable, per-org
        // suppression. Read directly (this story's own instruction: "for
        // the send-time skip, read the table directly") — never
        // `services::org_join_requests::enqueue_join_request` here, which
        // is the CLICK's job (`routes::org_join_requests::create`), not
        // the send's.
        //
        // `begin_tenant_system_scoped`, not `begin_account_scoped`: this
        // file is `converted` on the tenant-RLS gate, and the claim
        // transaction just below already opens that primitive on
        // `control_pool`. `org_join_requests`' policy (0238) keys only on
        // `app.account_id`, which `begin_tenant_system_scoped` sets.
        let mut check_tx = match begin_tenant_system_scoped(control_pool, account_id).await
        {
            Ok(tx) => tx,
            Err(e) => {
                tracing::warn!(
                    error = %e, account_id = %account_id, user_id = %candidate.user_id,
                    "slack-unmatched-email: CS-440 failed to open scoped read for the existing- \
                     request check — skipping this candidate, continuing with the rest"
                );
                continue;
            }
        };
        let existing_request: Result<bool, sqlx::Error> = sqlx::query_scalar(
            "SELECT EXISTS ( \
                 SELECT 1 FROM org_join_requests \
                  WHERE account_id = $1 AND user_id = $2 \
                    AND (decision IS NULL OR decision = 'rejected') \
             )",
        )
        .bind(account_id)
        .bind(candidate.user_id)
        .fetch_one(&mut *check_tx)
        .await;
        check_tx.rollback().await.ok();
        match existing_request {
            Ok(true) => {
                already_requested_skipped += 1;
                continue;
            }
            Ok(false) => {}
            Err(e) => {
                tracing::warn!(
                    error = %e, account_id = %account_id, user_id = %candidate.user_id,
                    "slack-unmatched-email: CS-440 existing-join-request check failed — proceeding \
                     rather than blocking this candidate over an unrelated read failure"
                );
            }
        }

        let mut tx = match begin_tenant_system_scoped(control_pool, account_id).await {
            Ok(tx) => tx,
            Err(e) => {
                tracing::warn!(
                    error = %e, account_id = %account_id, user_id = %candidate.user_id,
                    "slack-unmatched-email: CS-440 failed to open scoped transaction for cooldown \
                     claim — skipping this candidate, continuing with the rest"
                );
                continue;
            }
        };
        let claimed = match crate::services::unmatched_recipient_email::claim_join_notice_slot(
            &mut *tx,
            account_id,
            crate::services::unmatched_recipient_email::JOIN_THIS_ORG_PROVIDER,
            workspace_id,
            channel_id,
            &candidate.email,
        )
        .await
        {
            Ok(c) => c,
            Err(e) => {
                tx.rollback().await.ok();
                tracing::warn!(
                    error = %e, account_id = %account_id, user_id = %candidate.user_id,
                    "slack-unmatched-email: CS-440 cooldown claim query failed — skipping this \
                     candidate, continuing with the rest"
                );
                continue;
            }
        };
        let Some(tokens) = claimed else {
            // Still inside cooldown, lost a concurrent claim, or already
            // suppressed under this provider.
            tx.rollback().await.ok();
            continue;
        };

        // The CLICK token is a freshly minted, signed JWT (`services::
        // unmatched_recipient_email::mint_join_notice_token`) — NOT
        // `tokens.prefill_token` (that column is still written by the
        // claim above for schema-shape symmetry with this table's other
        // rows, but nothing reads it back for this provider; see this
        // service module's own "join this org" doc section, "CORRECTED
        // mid-implementation"). Minted fresh on every renewal, same as the
        // email itself.
        let join_token = match crate::services::unmatched_recipient_email::mint_join_notice_token(
            &join_secret,
            account_id,
            candidate.user_id,
            clerk_org_id,
            &candidate.email,
            Some(workspace_id),
            Some(channel_id),
            org_name.as_deref(),
            // CS-447 (D16 rung 2): this walk's OWN classification of this
            // person, signed into the token so auto-add can read it back
            // at click time without a second Slack call. Carried, never
            // assumed — this audience includes guests on the anchor
            // domain, who must not satisfy rung 2.
            candidate.class,
        ) {
            Ok(t) => t,
            Err(e) => {
                tx.rollback().await.ok();
                tracing::warn!(
                    error = %e, account_id = %account_id, user_id = %candidate.user_id,
                    "slack-unmatched-email: CS-440 failed to mint the join-notice token — \
                     skipping this candidate, continuing with the rest"
                );
                continue;
            }
        };

        // Claim-then-SEND-then-commit (`workers::member_connect_nudge`'s
        // order, NOT the Unmatched loop's claim-then-commit-then-send
        // above) — see `services::unmatched_recipient_email::send_join_org_
        // email`'s own doc comment: a failed send must never burn this
        // person's 7-day cooldown window.
        let join_url = format!(
            "{join_web_base}/join/{}?t={}",
            urlencoding::encode(clerk_org_id),
            urlencoding::encode(&join_token)
        );
        if crate::services::unmatched_recipient_email::send_join_org_email(
            state,
            &candidate.email,
            org_label,
            &join_url,
            &join_web_base,
            &tokens.unsubscribe_token,
        )
        .await
        {
            if let Err(e) = tx.commit().await {
                tracing::warn!(
                    error = %e, account_id = %account_id, user_id = %candidate.user_id,
                    "slack-unmatched-email: CS-440 sent the join-this-org email but failed to \
                     commit its cooldown claim — a second email may arrive before a full cooldown \
                     window"
                );
            }
            sent += 1;
        } else {
            tx.rollback().await.ok();
        }
    }

    tracing::info!(
        account_id = %account_id, channel_id, workspace_id,
        found = candidates.len(),
        sent, tombstoned_skipped, already_requested_skipped,
        "slack-unmatched-email: CS-440 join-this-org pass"
    );

    sent
}

/// One `EmailUnmatchedSlackChannelMembers` pass on one tenant's `(pool,
/// store)` pair — dequeue up to [`JOB_BATCH_SIZE`] jobs and process them.
/// Driven by `workers::tenant_harness::run_multi_tenant`, same contract as
/// `slack_seat_reconcile::tick`/`workers::slack_ingest` (this worker owns
/// neither its own poll loop nor shutdown).
pub async fn tick(
    t: &WorkerTenant,
    state: &Arc<AppState>,
    control_pool: &PgPool,
    http: &reqwest::Client,
) -> Result<Tick, String> {
    let jobs = queue::dequeue(&t.pool, &["EmailUnmatchedSlackChannelMembers"], JOB_BATCH_SIZE)
        .await
        .map_err(|e| format!("dequeue failed: {e}"))?;

    if jobs.is_empty() {
        return Ok(Tick::idle());
    }

    let n = jobs.len();
    for job in jobs {
        // `JobRow` stores `kind`/`payload` separately; reassembling the
        // internally-tagged shape is how every other worker decodes its own
        // variant (see `slack_seat_reconcile::tick`'s identical pattern).
        let tagged = serde_json::json!({ "kind": job.kind, "payload": job.payload });
        let (account_id, workspace_id, channel_id) = match serde_json::from_value::<JobKind>(tagged)
        {
            Ok(JobKind::EmailUnmatchedSlackChannelMembers { account_id, workspace_id, channel_id }) => {
                (account_id, workspace_id, channel_id)
            }
            Ok(_) => {
                tracing::error!(job_id = %job.id, "slack-unmatched-email: wrong job kind dequeued");
                queue::fail(&t.pool, job.id, "wrong job kind").await.ok();
                continue;
            }
            Err(e) => {
                tracing::error!(job_id = %job.id, error = %e, "slack-unmatched-email: malformed payload");
                queue::fail(&t.pool, job.id, &format!("malformed payload: {e}")).await.ok();
                continue;
            }
        };

        match process_job(
            state,
            &t.tenant,
            control_pool,
            &t.pool,
            http,
            account_id,
            &workspace_id,
            &channel_id,
        )
        .await
        {
            Ok(sent) => {
                let outcome = if sent == 0 {
                    queue::JobOutcome::NothingToDo
                } else {
                    queue::JobOutcome::DidWork
                };
                if let Err(e) = queue::complete(&t.pool, job.id, outcome).await {
                    tracing::error!(error = %e, job_id = %job.id, "slack-unmatched-email: complete failed");
                }
            }
            Err(e) => {
                tracing::error!(error = %e, job_id = %job.id, "slack-unmatched-email: job failed");
                queue::fail(&t.pool, job.id, &e).await.ok();
            }
        }
    }
    Ok(Tick::worked(n))
}

#[cfg(test)]
mod continuation_pg_tests {
    //! Live-Postgres proof of the ONE assumption
    //! [`process_job`]'s continuation-enqueue leans on: re-using the SAME
    //! idempotency key is safe because `queue::dequeue` flips a claimed
    //! job's status to `running` — leaving `pending` — BEFORE `process_job`
    //! ever runs, so `idx_jobs_idem_pending`'s dedup (scoped to `pending`
    //! only) cannot see it. Exercises `jobs::queue` directly rather than the
    //! whole worker: no Slack mock, no bot token, no DEK plane needed for
    //! this property.
    //!
    //!   ORGT_TEST_BASE_URL=postgres://alex@localhost:5432/postgres \
    //!     cargo test -p coralswarm-backend --lib continuation_pg_tests

    use sqlx::postgres::PgPoolOptions;
    use sqlx::{Connection, Executor, PgConnection, PgPool};
    use uuid::Uuid;

    use crate::jobs::{queue, JobKind};

    fn base_url() -> Option<String> {
        std::env::var("ORGT_TEST_BASE_URL").ok()
    }
    fn with_db(base: &str, db: &str) -> String {
        let (prefix, rest) = base.split_at(base.rfind('/').expect("url has a path /"));
        let params = rest.find('?').map(|i| &rest[i..]).unwrap_or("");
        format!("{prefix}/{db}{params}")
    }
    async fn setup(db: &str) -> Option<(PgConnection, PgPool)> {
        let base = base_url()?;
        let mut admin_conn = PgConnection::connect(&base).await.expect("connect admin");
        admin_conn
            .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .unwrap();
        admin_conn.execute(format!("CREATE DATABASE {db}").as_str()).await.unwrap();
        let pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();
        Some((admin_conn, pool))
    }
    async fn teardown(mut admin_conn: PgConnection, pool: PgPool, db: &str) {
        pool.close().await;
        admin_conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
    }
    async fn seed_account(pool: &PgPool) -> Uuid {
        let owner: Uuid = sqlx::query_scalar(
            "INSERT INTO users(email) VALUES ('owner+' || gen_random_uuid() || '@example.com') RETURNING id",
        )
        .fetch_one(pool)
        .await
        .unwrap();
        sqlx::query_scalar("INSERT INTO accounts(owner_user_id) VALUES ($1) RETURNING id")
            .bind(owner)
            .fetch_one(pool)
            .await
            .unwrap()
    }

    /// FAILS WITHOUT THE FIX (or, more precisely, FAILS if `dequeue`'s own
    /// status transition ever regressed): a continuation enqueued with the
    /// SAME idempotency key as the job currently being processed must
    /// succeed, because that job already left `pending` the moment it was
    /// dequeued — this is the property that makes reusing the key safe.
    #[tokio::test]
    async fn reenqueue_with_the_same_key_succeeds_once_the_original_is_dequeued_pg() {
        let db = "cs_unmatched_email_continuation_test";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", db);
        let Some((admin_conn, pool)) = setup(db).await else {
            eprintln!(
                "skipping reenqueue_with_the_same_key_succeeds_once_the_original_is_dequeued_pg \
                 — no ORGT_TEST_BASE_URL"
            );
            return;
        };
        let account_id = seed_account(&pool).await;
        let key = format!("email-unmatched:{account_id}:T1:C1");
        let kind = JobKind::EmailUnmatchedSlackChannelMembers {
            account_id,
            workspace_id: "T1".to_string(),
            channel_id: "C1".to_string(),
        };

        // First enqueue: the "original" job a tick is about to process.
        let first = queue::enqueue(&pool, &kind, Some(account_id), Some(&key), 0)
            .await
            .unwrap();
        assert!(first.is_some(), "the first enqueue must succeed — nothing else holds this key yet");

        // A SECOND enqueue with the SAME key, before anything dequeues the
        // first, must be dropped — this is `idx_jobs_idem_pending` doing
        // its ordinary job, not the continuation scenario.
        let second_while_pending = queue::enqueue(&pool, &kind, Some(account_id), Some(&key), 0)
            .await
            .unwrap();
        assert!(
            second_while_pending.is_none(),
            "a second enqueue while the first is still pending must dedupe to nothing"
        );

        // Dequeue the first — same call `tick` makes, flips it to `running`.
        let dequeued = queue::dequeue(&pool, &["EmailUnmatchedSlackChannelMembers"], 5)
            .await
            .unwrap();
        assert_eq!(dequeued.len(), 1, "exactly the one job enqueued above must be claimed");

        // NOW enqueue the continuation, same key, while the original is
        // `running` (i.e., `process_job` is executing) — THIS must succeed,
        // which is the exact property the continuation fix depends on.
        let continuation = queue::enqueue(&pool, &kind, Some(account_id), Some(&key), 0)
            .await
            .unwrap();
        assert!(
            continuation.is_some(),
            "a continuation enqueued with the SAME key while the original job is `running` \
             (dequeued, not yet completed) must succeed — this is what lets process_job enqueue \
             its own continuation without colliding with itself"
        );

        teardown(admin_conn, pool, db).await;
    }
}

#[cfg(test)]
mod d4_selection_pg_tests {
    //! Live-Postgres proof that D4's selection (`earliest_connected_
    //! account`, `accounts_whose_roster_holds`, `connectors::slack_
    //! unmatched_email::select_notice_account`) and the per-person cooldown
    //! (migration `0230`) together converge on exactly ONE claimed notice
    //! for a recipient discoverable by three sibling accounts that all map
    //! to the SAME Slack workspace — the exact production shape migration
    //! `0230`'s own header describes ("a workspace mapped to three
    //! accounts").
    //!
    //! Deliberately does not exercise `discover_unmatched_recipients` or a
    //! mock Slack server: `connectors::slack_unmatched_email::discover_
    //! unmatched_recipients_pg_tests` already pins that half. This proves
    //! the NEW selection + claim integration on its own, against real
    //! Postgres, and that the winner is IDENTICAL regardless of which
    //! sibling account's own tick evaluates it first.
    //!
    //!   ORGT_TEST_BASE_URL=postgres://alex@localhost:5432/postgres \
    //!     cargo test -p coralswarm-backend --lib d4_selection_pg_tests \
    //!     -- --test-threads=1

    use sqlx::postgres::PgPoolOptions;
    use sqlx::{Connection, Executor, PgConnection, PgPool};
    use uuid::Uuid;

    use super::{accounts_whose_roster_holds, earliest_connected_account_among};
    use crate::connectors::slack_unmatched_email::{select_notice_account, NoticeAccountCandidate};
    use crate::services::unmatched_recipient_email::claim_notice_slot;

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
        conn.execute(format!("CREATE DATABASE {db}").as_str()).await.unwrap();
        conn
    }
    async fn setup(db: &str) -> Option<(PgConnection, PgPool)> {
        let base = base_url()?;
        let admin_conn = recreate(&base, db).await;
        let pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();
        Some((admin_conn, pool))
    }
    async fn teardown(mut admin_conn: PgConnection, pool: PgPool, db: &str) {
        pool.close().await;
        admin_conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
    }

    async fn seed_account(pool: &PgPool) -> Uuid {
        let owner: Uuid = sqlx::query_scalar(
            "INSERT INTO users(email) VALUES ('owner+' || gen_random_uuid() || '@example.com') RETURNING id",
        )
        .fetch_one(pool)
        .await
        .unwrap();
        sqlx::query_scalar("INSERT INTO accounts(owner_user_id) VALUES ($1) RETURNING id")
            .bind(owner)
            .fetch_one(pool)
            .await
            .unwrap()
    }

    /// Seed an `oauth_tokens` row for `account_id` on `workspace_id`, with an
    /// explicit `created_at` so `earliest_connected_account_among`'s
    /// ordering is deterministic even though this test seeds all three
    /// within the same wall-clock second.
    async fn seed_oauth_token(pool: &PgPool, account_id: Uuid, workspace_id: &str, age_days: i32) {
        sqlx::query(
            "INSERT INTO oauth_tokens \
             (account_id, provider, scope, access_token, provider_workspace_id, owner_user_id, created_at) \
             VALUES ($1, 'slack', 'channels:read users:read.email', '\\x00'::bytea, $2, NULL, \
                     now() - make_interval(days => $3))",
        )
        .bind(account_id)
        .bind(workspace_id)
        .bind(age_days)
        .execute(pool)
        .await
        .unwrap();
    }

    /// FAILS WITHOUT THE FIX: three accounts (a personal install plus two
    /// orgs, or three orgs — D4 does not distinguish) all mapped to the SAME
    /// Slack workspace all independently discover the SAME genuinely-
    /// unmatched person. Before this PR each would have claimed and sent
    /// under its own `account_id` (the cooldown was keyed by it, migration
    /// `0227`); after this PR, D4 selection picks exactly one account (the
    /// earliest-connected — tier 1 never fires for a genuinely `Unmatched`
    /// recipient, see `connectors::slack_unmatched_email::
    /// NoticeAccountCandidate::roster_holds_verified_address`'s own doc
    /// comment) regardless of which sibling's own tick evaluates the
    /// selection first, and only that account's claim ever succeeds.
    #[tokio::test]
    async fn three_sibling_accounts_converge_on_exactly_one_claim_pg() {
        let db = "cs_d4_three_siblings_test";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", db);
        let Some((admin_conn, pool)) = setup(db).await else {
            eprintln!(
                "skipping three_sibling_accounts_converge_on_exactly_one_claim_pg — no \
                 ORGT_TEST_BASE_URL"
            );
            return;
        };
        let workspace_id = "T_D4_SHARED";
        let account_a = seed_account(&pool).await; // earliest-connected
        let account_b = seed_account(&pool).await;
        let account_c = seed_account(&pool).await;
        seed_oauth_token(&pool, account_a, workspace_id, 10).await;
        seed_oauth_token(&pool, account_b, workspace_id, 5).await;
        seed_oauth_token(&pool, account_c, workspace_id, 1).await;

        let sibling_ids: Vec<Uuid> = crate::routes::slack::resolve_slack_account_planes(&pool, workspace_id)
            .await
            .unwrap()
            .into_iter()
            .map(|(id, _)| id)
            .collect();
        assert_eq!(sibling_ids.len(), 3, "all three accounts must be visible as siblings");

        let authorized = earliest_connected_account_among(&pool, workspace_id, &sibling_ids).await;
        assert_eq!(authorized, Some(account_a), "the account with the oldest connection wins tier 2");

        let email = "person@example.com";
        let roster_matches = accounts_whose_roster_holds(&pool, &sibling_ids, &[email.to_string()]).await;
        assert!(roster_matches.is_empty(), "no account's roster holds this address — tier 1 never fires");

        // Simulate all three accounts' independent worker ticks processing
        // the SAME discovered recipient, evaluated in an order that does
        // NOT match "earliest connected" (C, then B, then A) — selection
        // must still converge on A regardless of evaluation order.
        let mut claimed_by = Vec::new();
        for candidate_account in [account_c, account_b, account_a] {
            let candidates: Vec<NoticeAccountCandidate> = sibling_ids
                .iter()
                .map(|&id| NoticeAccountCandidate {
                    account_id: id,
                    roster_holds_verified_address: roster_matches.contains(&(id, email.to_string())),
                    authorized_this_channel: Some(id) == authorized,
                })
                .collect();
            let winner = select_notice_account(&candidates);
            assert_eq!(
                winner,
                Some(account_a),
                "every sibling's own selection must agree on the same winner, regardless of \
                 which one is evaluating it"
            );
            if winner != Some(candidate_account) {
                continue; // this account's own tick would skip, exactly like `process_job`
            }
            let tokens = claim_notice_slot(&pool, candidate_account, "slack", workspace_id, "C1", email)
                .await
                .unwrap();
            if tokens.is_some() {
                claimed_by.push(candidate_account);
            }
        }
        assert_eq!(
            claimed_by,
            vec![account_a],
            "exactly the D4-preferred account must claim — never zero, never more than one"
        );

        let rows: Vec<(Uuid, String)> =
            sqlx::query_as("SELECT account_id, email FROM slack_unmatched_email_notices")
                .fetch_all(&pool)
                .await
                .unwrap();
        assert_eq!(rows.len(), 1, "exactly one notice row must exist for this recipient");
        assert_eq!(rows[0].0, account_a, "the notice row must name the D4-preferred account");
        assert_eq!(rows[0].1, email);

        // Second pass, within the 7-day cooldown (a re-delivered self-join
        // event, or one of the siblings' own bot joining a second channel
        // that shares this same roster): selection still converges on A,
        // and A's own re-claim finds the row still within cooldown —
        // nothing changes.
        for candidate_account in [account_a, account_b, account_c] {
            let candidates: Vec<NoticeAccountCandidate> = sibling_ids
                .iter()
                .map(|&id| NoticeAccountCandidate {
                    account_id: id,
                    roster_holds_verified_address: false,
                    authorized_this_channel: Some(id) == authorized,
                })
                .collect();
            let winner = select_notice_account(&candidates);
            if winner != Some(candidate_account) {
                continue;
            }
            let tokens = claim_notice_slot(&pool, candidate_account, "slack", workspace_id, "C1", email)
                .await
                .unwrap();
            assert!(
                tokens.is_none(),
                "within the 7-day cooldown, even the preferred account's own re-claim must find \
                 nothing"
            );
        }
        let count_after_second_pass: i64 =
            sqlx::query_scalar("SELECT count(*) FROM slack_unmatched_email_notices WHERE email = $1")
                .bind(email)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(count_after_second_pass, 1, "the second pass within cooldown must add nothing");

        // Backdate past the cooldown — the preferred account's next claim
        // succeeds again ("one again").
        sqlx::query(
            "UPDATE slack_unmatched_email_notices SET last_notified_at = now() - INTERVAL '8 days' \
             WHERE email = $1",
        )
        .bind(email)
        .execute(&pool)
        .await
        .unwrap();
        let tokens = claim_notice_slot(&pool, account_a, "slack", workspace_id, "C1", email).await.unwrap();
        assert!(tokens.is_some(), "after the cooldown elapses, the preferred account can claim again");

        teardown(admin_conn, pool, db).await;
    }
}

#[cfg(test)]
mod tombstone_gate_pg_tests {
    //! CS-428 (`docs/plans/org-onboarding-from-the-invite.md` §3 item 3,
    //! D12) — live-Postgres proof that the tombstone gate wired into
    //! `process_job`'s per-recipient loop (`clerk_provision::invite_
    //! suppressed_by_tombstone`, called with the recipient's email and this
    //! job's own account's `clerk_org_id`, right after D4 selects that
    //! account as the winner) actually stops the claim: a suppressed
    //! recipient writes NO `slack_unmatched_email_notices` row and never
    //! starts a cooldown, while a genuinely un-tombstoned recipient still
    //! claims exactly as before this PR.
    //!
    //! Deliberately does not exercise `discover_unmatched_recipients`, a
    //! mock Slack server, a bot token/DEK, or D4 sibling selection — same
    //! reasoning `d4_selection_pg_tests`'s own doc comment gives for
    //! skipping those (that suite already proves the OTHER half): this one
    //! proves the NEW gate + claim integration on its own, against real
    //! Postgres, by calling the exact sequence `process_job`'s loop calls
    //! once D4 has already picked this job's own account as the winner for
    //! a given recipient — `invite_suppressed_by_tombstone` then, on
    //! `Ok(false)`, `claim_notice_slot` (called directly against the pool,
    //! same simplification `d4_selection_pg_tests` already uses — RLS is
    //! bypassed by the connecting role owning these tables locally, so the
    //! production `begin_tenant_system_scoped` wrapper is not needed to
    //! prove THIS decision).
    //!
    //!   ORGT_TEST_BASE_URL=postgresql://alex@localhost:5432/postgres \
    //!     cargo test -p coralswarm-backend --lib tombstone_gate_pg_tests \
    //!     -- --test-threads=1

    use sqlx::postgres::PgPoolOptions;
    use sqlx::{Connection, Executor, PgConnection, PgPool};
    use uuid::Uuid;

    use crate::clerk_provision::invite_suppressed_by_tombstone;
    use crate::services::unmatched_recipient_email::claim_notice_slot;

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
        conn.execute(format!("CREATE DATABASE {db}").as_str()).await.unwrap();
        conn
    }
    async fn setup(db: &str) -> Option<(PgConnection, PgPool)> {
        let base = base_url()?;
        let admin_conn = recreate(&base, db).await;
        let pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();
        Some((admin_conn, pool))
    }
    async fn teardown(mut admin_conn: PgConnection, pool: PgPool, db: &str) {
        pool.close().await;
        admin_conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
    }

    /// A minimal, owned account — same seeding order every sibling pg suite
    /// in this crate uses. `clerk_org_id: None` seeds a personal account.
    async fn seed_account(pool: &PgPool, clerk_org_id: Option<&str>) -> Uuid {
        let owner: Uuid = sqlx::query_scalar(
            "INSERT INTO users(email) VALUES ('owner+' || gen_random_uuid() || '@example.com') RETURNING id",
        )
        .fetch_one(pool)
        .await
        .unwrap();
        sqlx::query_scalar("INSERT INTO accounts(owner_user_id, clerk_org_id) VALUES ($1, $2) RETURNING id")
            .bind(owner)
            .bind(clerk_org_id)
            .fetch_one(pool)
            .await
            .unwrap()
    }

    /// A tombstone row for `clerk_org_id` at `email` (or `NULL` when `email`
    /// is `None`, simulating the documented no-address residue) — inserted
    /// directly, the same way `clerk_provision::tombstone_email_pg_tests`'
    /// own residue test does, since `remove_org_membership` cannot produce a
    /// `NULL` address today (`users.email` is `NOT NULL`).
    async fn seed_tombstone(pool: &PgPool, clerk_org_id: &str, email: Option<&str>) {
        let removed_user = Uuid::new_v4();
        sqlx::query("INSERT INTO users(id, email) VALUES ($1, $2)")
            .bind(removed_user)
            .bind(format!("placeholder-{}@example.com", removed_user.simple()))
            .execute(pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO org_membership_tombstones(clerk_org_id, user_id, email) VALUES ($1, $2, $3)")
            .bind(clerk_org_id)
            .bind(removed_user)
            .bind(email)
            .execute(pool)
            .await
            .unwrap();
    }

    async fn notice_row_count(pool: &PgPool, email: &str) -> i64 {
        sqlx::query_scalar("SELECT count(*) FROM slack_unmatched_email_notices WHERE email = $1")
            .bind(email)
            .fetch_one(pool)
            .await
            .unwrap()
    }

    /// Mirrors `process_job`'s own per-recipient sequence AFTER D4 has
    /// already picked `account_id` as the winner for `email` — the
    /// tombstone gate, then (only on `Ok(false)`) the claim. `true` means
    /// claimed (a notice row now exists / was renewed); `false` means
    /// suppressed OR the claim itself found nothing (cooldown/unsubscribe) —
    /// callers that need to tell those apart check `notice_row_count`
    /// directly, same as `process_job`'s own loop treats both as "nothing
    /// to send" without distinguishing them further.
    async fn suppress_or_claim(
        control_pool: &PgPool,
        account_id: Uuid,
        clerk_org_id: Option<&str>,
        workspace_id: &str,
        channel_id: &str,
        email: &str,
    ) -> bool {
        let suppressed = invite_suppressed_by_tombstone(control_pool, clerk_org_id, None, Some(email))
            .await
            .expect("tombstone lookup must not error");
        if suppressed {
            return false;
        }
        claim_notice_slot(control_pool, account_id, "slack", workspace_id, channel_id, email)
            .await
            .expect("claim query must not error")
            .is_some()
    }

    /// FAILS WITHOUT THE FIX: before this PR, nothing on the invite path
    /// ever read `org_membership_tombstones` — a removed member's address,
    /// still visible in a shared channel's roster, was invited and cooled
    /// down exactly like anyone else's. This proves the gate now stops
    /// BOTH: no claim (`suppress_or_claim` returns `false`) and, critically,
    /// no notice row at all — a suppressed skip must never start this
    /// address's cooldown (same "never cools down" reasoning #1007 used for
    /// a guest skip).
    #[tokio::test]
    async fn a_removed_members_address_is_suppressed_and_writes_no_notice_row_pg() {
        let db = "cs428_tombstone_gate_suppressed";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", db);
        let Some((admin_conn, pool)) = setup(db).await else {
            eprintln!("skipping a_removed_members_address_is_suppressed_and_writes_no_notice_row_pg — no ORGT_TEST_BASE_URL");
            return;
        };
        let clerk_org_id = format!("org_cs428_{}", Uuid::new_v4().simple());
        let account_id = seed_account(&pool, Some(&clerk_org_id)).await;
        let email = "removed.person@example.com";
        seed_tombstone(&pool, &clerk_org_id, Some(email)).await;

        let claimed = suppress_or_claim(&pool, account_id, Some(&clerk_org_id), "T1", "C1", email).await;
        let notices = notice_row_count(&pool, email).await;

        teardown(admin_conn, pool, db).await;

        assert!(!claimed, "a tombstoned recipient must never be claimed");
        assert_eq!(notices, 0, "a suppressed skip must write NO notice row — it must never cool down");
    }

    /// The org-scoping half of D12's rule: the SAME real address is still
    /// invited by a DIFFERENT org that never removed this person — the
    /// removing org's own tombstone must not leak into a sibling org's
    /// invite decision.
    #[tokio::test]
    async fn the_same_address_is_still_claimed_by_a_different_org_pg() {
        let db = "cs428_tombstone_gate_different_org";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", db);
        let Some((admin_conn, pool)) = setup(db).await else {
            eprintln!("skipping the_same_address_is_still_claimed_by_a_different_org_pg — no ORGT_TEST_BASE_URL");
            return;
        };
        let removing_org = format!("org_cs428_removing_{}", Uuid::new_v4().simple());
        let other_org = format!("org_cs428_other_{}", Uuid::new_v4().simple());
        let removing_account = seed_account(&pool, Some(&removing_org)).await;
        let other_account = seed_account(&pool, Some(&other_org)).await;
        let email = "shared.person@example.com";
        seed_tombstone(&pool, &removing_org, Some(email)).await;

        let claimed_by_removing_org =
            suppress_or_claim(&pool, removing_account, Some(&removing_org), "T1", "C1", email).await;
        let claimed_by_other_org =
            suppress_or_claim(&pool, other_account, Some(&other_org), "T1", "C2", email).await;
        let notices = notice_row_count(&pool, email).await;

        teardown(admin_conn, pool, db).await;

        assert!(!claimed_by_removing_org, "the removing org must still suppress this address");
        assert!(
            claimed_by_other_org,
            "a DIFFERENT org, which never removed this person, must still be able to invite them"
        );
        assert_eq!(notices, 1, "exactly one notice row must exist — the other org's successful claim");
    }

    /// D12's build note, at THIS call site's own shape: the invite pass
    /// never holds a `user_id` (`suppress_or_claim`/`process_job` both pass
    /// `None`), so a member who is removed and then deletes and recreates
    /// their CoralSwarm account (a brand-new `user_id`, same address) must
    /// still be suppressed through the email key alone.
    #[tokio::test]
    async fn a_recreated_account_same_email_is_still_suppressed_by_the_removing_org_pg() {
        let db = "cs428_tombstone_gate_recreated";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", db);
        let Some((admin_conn, pool)) = setup(db).await else {
            eprintln!("skipping a_recreated_account_same_email_is_still_suppressed_by_the_removing_org_pg — no ORGT_TEST_BASE_URL");
            return;
        };
        let clerk_org_id = format!("org_cs428_{}", Uuid::new_v4().simple());
        let account_id = seed_account(&pool, Some(&clerk_org_id)).await;
        let email = "resurrected@example.com";
        seed_tombstone(&pool, &clerk_org_id, Some(email)).await;

        // The "recreated account" is not modeled as a NEW `user_id` here —
        // this call site never has one to give (D12's build note: "at
        // invite time we hold an address, not a user_id"). Passing `None`
        // for `user_id`, exactly like `process_job`, is itself the proof:
        // the email key alone must be sufficient regardless of which
        // `user_id` (old, new, or none at all) the recreated account ends
        // up with.
        let claimed = suppress_or_claim(&pool, account_id, Some(&clerk_org_id), "T1", "C1", email).await;
        let notices = notice_row_count(&pool, email).await;

        teardown(admin_conn, pool, db).await;

        assert!(!claimed, "the email-only call shape must still catch a recreated account at the same address");
        assert_eq!(notices, 0, "a suppressed skip must write no notice row here either");
    }

    /// **Documented residue, asserted as a limit, not read as coverage.** A
    /// tombstone with NO address on file (D12's own text: "it does not
    /// close a removed person returning under a different address, which
    /// nothing can") cannot suppress an invite that only ever carries an
    /// email — the invite proceeds and claims normally. This is the SAME
    /// residue `clerk_provision::tombstone_email_pg_tests::lookup_
    /// documented_limit_no_address_on_file_is_not_caught_by_email_pg`
    /// documents at the lower level, restated here at the actual call site
    /// so the limit is visible where CS-428 wires the gate in, not only
    /// where the gate's own underlying function lives.
    #[tokio::test]
    async fn a_no_address_tombstone_does_not_block_a_later_invite_by_email_pg() {
        let db = "cs428_tombstone_gate_residue";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", db);
        let Some((admin_conn, pool)) = setup(db).await else {
            eprintln!("skipping a_no_address_tombstone_does_not_block_a_later_invite_by_email_pg — no ORGT_TEST_BASE_URL");
            return;
        };
        let clerk_org_id = format!("org_cs428_{}", Uuid::new_v4().simple());
        let account_id = seed_account(&pool, Some(&clerk_org_id)).await;
        // A no-address tombstone row — nothing in it to compare an incoming
        // email against.
        seed_tombstone(&pool, &clerk_org_id, None).await;
        let email = "a.new.address.nobody.tombstoned@example.com";

        let claimed = suppress_or_claim(&pool, account_id, Some(&clerk_org_id), "T1", "C1", email).await;
        let notices = notice_row_count(&pool, email).await;

        teardown(admin_conn, pool, db).await;

        assert!(
            claimed,
            "DOCUMENTED LIMIT (D12): a no-address tombstone cannot block an invite arriving by \
             email alone — this assertion proves the limit exists, it is not evidence of coverage"
        );
        assert_eq!(notices, 1, "the invite must proceed and claim normally despite the no-address row");
    }

    /// Personal accounts have no org tombstones — asserted via the OUTCOME
    /// shape (the invite claims and cools down exactly as if the gate did
    /// not exist), not by timing, per the story's own instruction. The
    /// "without a query at all" half of this property is proven separately,
    /// DB-free, by `clerk_provision::invite_suppressed_by_tombstone_tests::
    /// personal_plane_short_circuits_without_ever_touching_the_pool` (an
    /// unreachable-host pool double).
    #[tokio::test]
    async fn a_personal_account_claims_normally_with_no_org_to_check_pg() {
        let db = "cs428_tombstone_gate_personal";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", db);
        let Some((admin_conn, pool)) = setup(db).await else {
            eprintln!("skipping a_personal_account_claims_normally_with_no_org_to_check_pg — no ORGT_TEST_BASE_URL");
            return;
        };
        let account_id = seed_account(&pool, None).await;
        let email = "personal.plane@example.com";

        let claimed = suppress_or_claim(&pool, account_id, None, "T1", "C1", email).await;
        let notices = notice_row_count(&pool, email).await;

        teardown(admin_conn, pool, db).await;

        assert!(claimed, "a personal account's invite must proceed exactly as before this PR");
        assert_eq!(notices, 1, "the claim must write its notice row as usual");
    }
}

#[cfg(test)]
mod matched_not_member_pg_tests {
    //! CS-440 (org-onboarding plan §3 item 29, D19 as amended by D21) —
    //! live-Postgres proof of [`super::process_matched_not_members`]: the
    //! D4 sender-selection floor, the D12 tombstone gate, the D10/D19
    //! existing-open-or-rejected-request skip, the claim-then-send-then-
    //! commit cooldown (a failed send must NOT consume it — the deliberate
    //! divergence from the Unmatched loop's own order, matching
    //! `workers::member_connect_nudge`'s), and the provider/columns a
    //! successful claim actually writes.
    //!
    //! Calls [`super::process_matched_not_members`] directly (a private
    //! `async fn` in this same file) with hand-built [`crate::connectors::
    //! slack_unmatched_email::MatchedNotMember`] candidates — same
    //! simplification `tombstone_gate_pg_tests` above documents for skipping
    //! `discover_unmatched_recipients`/a mock Slack server/a bot token: the
    //! discovery half of this feature is proven separately, by
    //! `connectors::slack_unmatched_email`'s own pg tests.
    //!
    //! `JOIN_NOTICE_SECRET` and `RESEND_API_BASE_URL` are both process-global
    //! env vars this module sets/removes per test under `ENV_LOCK` below —
    //! without a secret, `process_matched_not_members` refuses to mint a
    //! single link and returns `0` immediately (see that function's own
    //! doc comment), which every test here would otherwise misread as
    //! "correctly suppressed" rather than "never tried at all".
    //!
    //!   ORGT_TEST_BASE_URL=postgresql://alex@localhost:5432/postgres \
    //!     cargo test -p coralswarm-backend --lib matched_not_member_pg_tests \
    //!     -- --test-threads=1

    use std::sync::Arc as StdArc;

    use sqlx::postgres::PgPoolOptions;
    use sqlx::{Connection, Executor, PgConnection, PgPool};
    use uuid::Uuid;

    use super::process_matched_not_members;
    use crate::connectors::slack_unmatched_email::MatchedNotMember;
    use crate::AppState;

    /// Serializes this module's tests against `JOIN_NOTICE_SECRET`.
    /// `RESEND_API_BASE_URL` is covered by
    /// `services::unmatched_recipient_email::RESEND_BASE_URL_LOCK`, taken
    /// before this one so a join-email test cannot land on this mock.
    static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

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
        conn.execute(format!("CREATE DATABASE {db}").as_str()).await.unwrap();
        conn
    }
    async fn setup(db: &str) -> Option<(PgConnection, PgPool)> {
        let base = base_url()?;
        let admin_conn = recreate(&base, db).await;
        let pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();
        Some((admin_conn, pool))
    }
    async fn teardown(mut admin_conn: PgConnection, pool: PgPool, db: &str) {
        pool.close().await;
        admin_conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
    }

    /// A minimal, owned ORDINARY-org account — same seeding order every
    /// sibling pg suite in this crate uses.
    async fn seed_org_account(pool: &PgPool, clerk_org_id: &str) -> Uuid {
        let owner: Uuid = sqlx::query_scalar(
            "INSERT INTO users(email) VALUES ('owner+' || gen_random_uuid() || '@example.com') RETURNING id",
        )
        .fetch_one(pool)
        .await
        .unwrap();
        sqlx::query_scalar("INSERT INTO accounts(owner_user_id, clerk_org_id) VALUES ($1, $2) RETURNING id")
            .bind(owner)
            .bind(clerk_org_id)
            .fetch_one(pool)
            .await
            .unwrap()
    }

    /// This person's existing CoralSwarm account — the row `matched_not_
    /// member`'s `user_id` points at (`org_join_requests.user_id` is a real
    /// FK to `users`, so every test needs a genuine row here).
    async fn seed_user(pool: &PgPool, email: &str) -> Uuid {
        sqlx::query_scalar("INSERT INTO users(email) VALUES ($1) RETURNING id")
            .bind(email)
            .fetch_one(pool)
            .await
            .unwrap()
    }

    fn candidate(user_id: Uuid, email: &str) -> MatchedNotMember {
        MatchedNotMember {
            user_id,
            email: email.to_string(),
            slack_user_id: "U1".to_string(),
            // CS-447: the ordinary case for this audience. Carried into
            // the join-notice token's `cls` claim, which this suite does
            // not read back — `routes::org_join_requests::pg_tests` is
            // where the claim's effect on rung 2 is proven.
            class: crate::connectors::slack::MembershipClass::FullMember,
        }
    }

    /// Minimal `AppState` — same field-for-field shape `services::
    /// unmatched_recipient_email::send_nudge_email_tests::test_state`
    /// already establishes, duplicated here (own copy matrix, this crate's
    /// established convention) rather than exported and shared.
    fn test_state(resend_api_key: &str) -> AppState {
        let resolver =
            crate::tenancy::secrets::StaticMapResolver::from_pairs(Vec::<(&str, &str)>::new());
        let registry = std::sync::Arc::new(crate::tenancy::registry::TenantRegistry::new(
            std::sync::Arc::new(resolver),
        ));
        let tenant_runtime = crate::tenancy::runtime::TenantRuntime::new_with_web_apex(
            true,
            "api.coralswarm.test",
            "coralswarm.test",
            registry,
        );
        AppState {
            db: None,
            store: None,
            resend_api_key: Some(resend_api_key.to_string()),
            r2: None,
            http_client: reqwest::Client::new(),
            embedder: StdArc::new(crate::StubEmbedder),
            llm: None,
            rate_limiter: StdArc::new(crate::rate_limit::InMemoryRateLimiter::new()),
            admission: Default::default(),
            in_flight: StdArc::new(Default::default()),
            metrics: StdArc::new(crate::telemetry::NoopMetrics),
            dek_cache: crate::dek_cache::DekCache::new(),
            connector_flag_cache: crate::connector_flags::ConnectorFlagCache::new(),
            tenant_runtime: Some(tenant_runtime),
            env: crate::AppEnv::default(),
            task_tracker: tokio_util::task::TaskTracker::new(),
            worker_activity: crate::workers::tenant_harness::TenantActivity::new(),
            mcp_cancellation_token: tokio_util::sync::CancellationToken::new(),
            mcp_task_manager: crate::mcp::server::McpTaskRegistry::new(),
            mcp_ask_in_flight: crate::mcp::server::AskInFlight::new(),
            stripe: None,
        }
    }

    /// Spawn a mock Resend that always answers `200` — the happy-path
    /// double every test but the 500 one below uses.
    async fn spawn_mock_resend_ok() -> String {
        use axum::{routing::post, Json, Router};
        let handler = || async { Json(serde_json::json!({"id": "mock"})) };
        let app = Router::new().route("/emails", post(handler));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.ok();
        });
        format!("http://{addr}")
    }

    /// Spawn a mock Resend that always answers `500`.
    async fn spawn_mock_resend_500() -> String {
        use axum::{http::StatusCode, routing::post, Router};
        let handler = || async { StatusCode::INTERNAL_SERVER_ERROR };
        let app = Router::new().route("/emails", post(handler));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.ok();
        });
        format!("http://{addr}")
    }

    async fn notice_row(pool: &PgPool, email: &str) -> Option<(String, Option<String>, Option<String>)> {
        sqlx::query_as(
            "SELECT provider, workspace_id, channel_id FROM slack_unmatched_email_notices \
             WHERE email = $1 AND provider = 'slack_join'",
        )
        .bind(email)
        .fetch_optional(pool)
        .await
        .unwrap()
    }

    /// FAILS WITHOUT THE FIX: the happy path — one candidate, no tombstone,
    /// no existing request, a healthy mock Resend — sends exactly once and
    /// claims a row under `provider = 'slack_join'` carrying THIS walk's
    /// `workspace_id`/`channel_id` (unlike the member-connect nudge's own
    /// `NULL` container columns).
    #[tokio::test]
    async fn happy_path_sends_and_claims_with_the_container_columns_pg() {
        let _resend_guard = crate::services::unmatched_recipient_email::RESEND_BASE_URL_LOCK
            .lock()
            .unwrap();
        let _env_guard = ENV_LOCK.lock().unwrap();
        let db = "cs440_matched_not_member_happy";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", db);
        let Some((admin_conn, pool)) = setup(db).await else {
            eprintln!("skipping happy_path_sends_and_claims_with_the_container_columns_pg — no ORGT_TEST_BASE_URL");
            return;
        };
        let clerk_org_id = format!("org_cs440_{}", Uuid::new_v4().simple());
        let account_id = seed_org_account(&pool, &clerk_org_id).await;
        let email = "dana@example.com";
        let user_id = seed_user(&pool, email).await;

        let resend_base = spawn_mock_resend_ok().await;
        // SAFETY: serialized by `_env_guard` above, restored before it drops.
        unsafe {
            std::env::set_var("RESEND_API_BASE_URL", &resend_base);
            std::env::set_var("JOIN_NOTICE_SECRET", "example_14");
        }
        let state = StdArc::new(test_state("re_mock_key"));

        let sent = process_matched_not_members(
            &state,
            &pool,
            account_id,
            "T1",
            "C1",
            Some(&clerk_org_id),
            &[account_id],
            Some(account_id),
            &[candidate(user_id, email)],
        )
        .await;

        unsafe {
            std::env::remove_var("RESEND_API_BASE_URL");
            std::env::remove_var("JOIN_NOTICE_SECRET");
        }

        let row = notice_row(&pool, email).await;
        teardown(admin_conn, pool, db).await;

        assert_eq!(sent, 1, "the happy path must send exactly once");
        let (provider, workspace_id, channel_id) = row.expect("a notice row must exist under provider slack_join");
        assert_eq!(provider, "slack_join");
        assert_eq!(workspace_id.as_deref(), Some("T1"));
        assert_eq!(channel_id.as_deref(), Some("C1"));
    }

    /// D4 — sender selection deliberately excludes tier 1: a sibling account
    /// is the winner (`authorized_this_channel`), so THIS account's own
    /// pass must send nothing, even though it is the only account that
    /// discovered the candidate at all.
    #[tokio::test]
    async fn a_non_winning_sibling_sends_nothing_pg() {
        let _resend_guard = crate::services::unmatched_recipient_email::RESEND_BASE_URL_LOCK
            .lock()
            .unwrap();
        let _env_guard = ENV_LOCK.lock().unwrap();
        let db = "cs440_matched_not_member_d4";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", db);
        let Some((admin_conn, pool)) = setup(db).await else {
            eprintln!("skipping a_non_winning_sibling_sends_nothing_pg — no ORGT_TEST_BASE_URL");
            return;
        };
        let clerk_org_id = format!("org_cs440_{}", Uuid::new_v4().simple());
        let account_id = seed_org_account(&pool, &clerk_org_id).await;
        let sibling_id = Uuid::new_v4();
        let email = "dana@example.com";
        let user_id = seed_user(&pool, email).await;

        let resend_base = spawn_mock_resend_ok().await;
        unsafe {
            std::env::set_var("RESEND_API_BASE_URL", &resend_base);
            std::env::set_var("JOIN_NOTICE_SECRET", "example_14");
        }
        let state = StdArc::new(test_state("re_mock_key"));

        // `authorized_account_id` names the SIBLING, not this job's own
        // `account_id` — `select_notice_account` must therefore pick the
        // sibling, and this account's own pass must send nothing.
        let sent = process_matched_not_members(
            &state,
            &pool,
            account_id,
            "T1",
            "C1",
            Some(&clerk_org_id),
            &[account_id, sibling_id],
            Some(sibling_id),
            &[candidate(user_id, email)],
        )
        .await;

        unsafe {
            std::env::remove_var("RESEND_API_BASE_URL");
            std::env::remove_var("JOIN_NOTICE_SECRET");
        }

        let row = notice_row(&pool, email).await;
        teardown(admin_conn, pool, db).await;

        assert_eq!(sent, 0, "a non-winning account must send nothing");
        assert!(row.is_none(), "a non-winning account must claim no row either");
    }

    /// D12 — a tombstoned address is suppressed and writes no row.
    #[tokio::test]
    async fn a_tombstoned_candidate_is_suppressed_and_writes_no_row_pg() {
        let _resend_guard = crate::services::unmatched_recipient_email::RESEND_BASE_URL_LOCK
            .lock()
            .unwrap();
        let _env_guard = ENV_LOCK.lock().unwrap();
        let db = "cs440_matched_not_member_tombstone";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", db);
        let Some((admin_conn, pool)) = setup(db).await else {
            eprintln!("skipping a_tombstoned_candidate_is_suppressed_and_writes_no_row_pg — no ORGT_TEST_BASE_URL");
            return;
        };
        let clerk_org_id = format!("org_cs440_{}", Uuid::new_v4().simple());
        let account_id = seed_org_account(&pool, &clerk_org_id).await;
        let email = "removed@example.com";
        let user_id = seed_user(&pool, email).await;
        sqlx::query("INSERT INTO org_membership_tombstones(clerk_org_id, user_id, email) VALUES ($1, $2, $3)")
            .bind(&clerk_org_id)
            .bind(user_id)
            .bind(email)
            .execute(&pool)
            .await
            .unwrap();

        let resend_base = spawn_mock_resend_ok().await;
        unsafe {
            std::env::set_var("RESEND_API_BASE_URL", &resend_base);
            std::env::set_var("JOIN_NOTICE_SECRET", "example_14");
        }
        let state = StdArc::new(test_state("re_mock_key"));

        let sent = process_matched_not_members(
            &state,
            &pool,
            account_id,
            "T1",
            "C1",
            Some(&clerk_org_id),
            &[account_id],
            Some(account_id),
            &[candidate(user_id, email)],
        )
        .await;

        unsafe {
            std::env::remove_var("RESEND_API_BASE_URL");
            std::env::remove_var("JOIN_NOTICE_SECRET");
        }

        let row = notice_row(&pool, email).await;
        teardown(admin_conn, pool, db).await;

        assert_eq!(sent, 0, "a tombstoned candidate must never be sent");
        assert!(row.is_none(), "a suppressed skip must write no notice row — it must never cool down");
    }

    /// D10/D19 — an existing OPEN request means the email already did its
    /// job; an existing REJECTED request is D10's durable suppression.
    /// Both must skip the send and write no new row.
    #[tokio::test]
    async fn an_existing_open_or_rejected_request_skips_the_send_pg() {
        let _resend_guard = crate::services::unmatched_recipient_email::RESEND_BASE_URL_LOCK
            .lock()
            .unwrap();
        let _env_guard = ENV_LOCK.lock().unwrap();
        let db = "cs440_matched_not_member_existing_request";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", db);
        let Some((admin_conn, pool)) = setup(db).await else {
            eprintln!("skipping an_existing_open_or_rejected_request_skips_the_send_pg — no ORGT_TEST_BASE_URL");
            return;
        };
        let clerk_org_id_open = format!("org_cs440_open_{}", Uuid::new_v4().simple());
        let clerk_org_id_rejected = format!("org_cs440_rejected_{}", Uuid::new_v4().simple());
        let account_open = seed_org_account(&pool, &clerk_org_id_open).await;
        let account_rejected = seed_org_account(&pool, &clerk_org_id_rejected).await;
        let email_open = "open-request@example.com";
        let email_rejected = "rejected-request@example.com";
        let user_open = seed_user(&pool, email_open).await;
        let user_rejected = seed_user(&pool, email_rejected).await;

        sqlx::query(
            "INSERT INTO org_join_requests (account_id, user_id, source) VALUES ($1, $2, 'self_request')",
        )
        .bind(account_open)
        .bind(user_open)
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO org_join_requests (account_id, user_id, source, decision, decided_at) \
             VALUES ($1, $2, 'self_request', 'rejected', now())",
        )
        .bind(account_rejected)
        .bind(user_rejected)
        .execute(&pool)
        .await
        .unwrap();

        let resend_base = spawn_mock_resend_ok().await;
        unsafe {
            std::env::set_var("RESEND_API_BASE_URL", &resend_base);
            std::env::set_var("JOIN_NOTICE_SECRET", "example_14");
        }
        let state = StdArc::new(test_state("re_mock_key"));

        let sent_open = process_matched_not_members(
            &state,
            &pool,
            account_open,
            "T1",
            "C1",
            Some(&clerk_org_id_open),
            &[account_open],
            Some(account_open),
            &[candidate(user_open, email_open)],
        )
        .await;
        let sent_rejected = process_matched_not_members(
            &state,
            &pool,
            account_rejected,
            "T1",
            "C1",
            Some(&clerk_org_id_rejected),
            &[account_rejected],
            Some(account_rejected),
            &[candidate(user_rejected, email_rejected)],
        )
        .await;

        unsafe {
            std::env::remove_var("RESEND_API_BASE_URL");
            std::env::remove_var("JOIN_NOTICE_SECRET");
        }

        let row_open = notice_row(&pool, email_open).await;
        let row_rejected = notice_row(&pool, email_rejected).await;
        teardown(admin_conn, pool, db).await;

        assert_eq!(sent_open, 0, "an existing OPEN request must skip the send");
        assert!(row_open.is_none());
        assert_eq!(sent_rejected, 0, "an existing REJECTED request must skip the send (D10)");
        assert!(row_rejected.is_none());
    }

    /// D4 cooldown — a second pass within 7 days sends nothing more, even
    /// with a healthy Resend both times.
    #[tokio::test]
    async fn a_second_pass_within_the_cooldown_sends_nothing_more_pg() {
        let _resend_guard = crate::services::unmatched_recipient_email::RESEND_BASE_URL_LOCK
            .lock()
            .unwrap();
        let _env_guard = ENV_LOCK.lock().unwrap();
        let db = "cs440_matched_not_member_cooldown";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", db);
        let Some((admin_conn, pool)) = setup(db).await else {
            eprintln!("skipping a_second_pass_within_the_cooldown_sends_nothing_more_pg — no ORGT_TEST_BASE_URL");
            return;
        };
        let clerk_org_id = format!("org_cs440_{}", Uuid::new_v4().simple());
        let account_id = seed_org_account(&pool, &clerk_org_id).await;
        let email = "dana@example.com";
        let user_id = seed_user(&pool, email).await;

        let resend_base = spawn_mock_resend_ok().await;
        unsafe {
            std::env::set_var("RESEND_API_BASE_URL", &resend_base);
            std::env::set_var("JOIN_NOTICE_SECRET", "example_14");
        }
        let state = StdArc::new(test_state("re_mock_key"));

        let first = process_matched_not_members(
            &state,
            &pool,
            account_id,
            "T1",
            "C1",
            Some(&clerk_org_id),
            &[account_id],
            Some(account_id),
            &[candidate(user_id, email)],
        )
        .await;
        let second = process_matched_not_members(
            &state,
            &pool,
            account_id,
            "T1",
            "C1",
            Some(&clerk_org_id),
            &[account_id],
            Some(account_id),
            &[candidate(user_id, email)],
        )
        .await;

        unsafe {
            std::env::remove_var("RESEND_API_BASE_URL");
            std::env::remove_var("JOIN_NOTICE_SECRET");
        }
        teardown(admin_conn, pool, db).await;

        assert_eq!(first, 1, "the first pass must send");
        assert_eq!(second, 0, "a second pass inside the 7-day cooldown must send nothing more");
    }

    /// FAILS WITHOUT THE FIX: a failed Resend send must NOT consume the
    /// cooldown — the deliberate claim-then-SEND-then-commit divergence
    /// from the Unmatched loop's own order (this file's module doc, and
    /// `services::unmatched_recipient_email::send_join_org_email`'s own doc
    /// comment). Proven by: a `500` tick sends nothing AND leaves no
    /// surviving row, then a SUBSEQUENT tick against a healthy Resend
    /// succeeds immediately — impossible if the first attempt had already
    /// burned the cooldown window.
    #[tokio::test]
    async fn a_failed_send_does_not_consume_the_cooldown_pg() {
        let _resend_guard = crate::services::unmatched_recipient_email::RESEND_BASE_URL_LOCK
            .lock()
            .unwrap();
        let _env_guard = ENV_LOCK.lock().unwrap();
        let db = "cs440_matched_not_member_failed_send";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", db);
        let Some((admin_conn, pool)) = setup(db).await else {
            eprintln!("skipping a_failed_send_does_not_consume_the_cooldown_pg — no ORGT_TEST_BASE_URL");
            return;
        };
        let clerk_org_id = format!("org_cs440_{}", Uuid::new_v4().simple());
        let account_id = seed_org_account(&pool, &clerk_org_id).await;
        let email = "dana@example.com";
        let user_id = seed_user(&pool, email).await;

        let resend_500_base = spawn_mock_resend_500().await;
        unsafe {
            std::env::set_var("RESEND_API_BASE_URL", &resend_500_base);
            std::env::set_var("JOIN_NOTICE_SECRET", "example_14");
        }
        let state_failing = StdArc::new(test_state("re_mock_key"));
        let first = process_matched_not_members(
            &state_failing,
            &pool,
            account_id,
            "T1",
            "C1",
            Some(&clerk_org_id),
            &[account_id],
            Some(account_id),
            &[candidate(user_id, email)],
        )
        .await;
        unsafe {
            std::env::remove_var("RESEND_API_BASE_URL");
            std::env::remove_var("JOIN_NOTICE_SECRET");
        }

        let row_after_failure = notice_row(&pool, email).await;

        let resend_ok_base = spawn_mock_resend_ok().await;
        unsafe {
            std::env::set_var("RESEND_API_BASE_URL", &resend_ok_base);
            std::env::set_var("JOIN_NOTICE_SECRET", "example_14");
        }
        let state_healthy = StdArc::new(test_state("re_mock_key"));
        let second = process_matched_not_members(
            &state_healthy,
            &pool,
            account_id,
            "T1",
            "C1",
            Some(&clerk_org_id),
            &[account_id],
            Some(account_id),
            &[candidate(user_id, email)],
        )
        .await;
        unsafe {
            std::env::remove_var("RESEND_API_BASE_URL");
            std::env::remove_var("JOIN_NOTICE_SECRET");
        }

        teardown(admin_conn, pool, db).await;

        assert_eq!(first, 0, "a Resend 500 must report zero sent");
        assert!(row_after_failure.is_none(), "a failed send must leave no surviving notice row");
        assert_eq!(
            second, 1,
            "a later tick against a healthy Resend must succeed immediately — the failed attempt \
             must not have consumed the cooldown"
        );
    }
}

#[cfg(test)]
mod org_deletion_gate_pg_tests {
    //! In-app org deletion: a queued unmatched-email job for a closing org is
    //! dropped, and one for a healthy or personal account is not.
    //!
    //!   ORGT_TEST_BASE_URL=postgres://alex@localhost:5432/postgres \
    //!     cargo test -p coralswarm-backend --lib org_deletion_gate_pg_tests

    use sqlx::postgres::PgPoolOptions;
    use sqlx::{Connection, Executor, PgConnection, PgPool};
    use uuid::Uuid;

    use super::job_org_is_closing;

    #[tokio::test]
    async fn a_queued_job_for_a_deleting_or_revoked_org_is_dropped_pg() {
        let Ok(base) = std::env::var("ORGT_TEST_BASE_URL") else {
            eprintln!("skipping a_queued_job_for_a_deleting_or_revoked_org_is_dropped_pg - no ORGT_TEST_BASE_URL");
            return;
        };
        let db = "cs_unmatched_email_org_deletion_gate";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mut admin = PgConnection::connect(&base).await.unwrap();
        admin.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.unwrap();
        admin.execute(format!("CREATE DATABASE {db}").as_str()).await.unwrap();
        let (prefix, rest) = base.split_at(base.rfind('/').unwrap());
        let params = rest.find('?').map(|i| &rest[i..]).unwrap_or("");
        let pool: PgPool = PgPoolOptions::new().connect(&format!("{prefix}/{db}{params}")).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();

        let owner: Uuid = sqlx::query_scalar("INSERT INTO users(email) VALUES ('o+' || gen_random_uuid() || '@example.com') RETURNING id")
            .fetch_one(&pool).await.unwrap();
        let personal: Uuid = sqlx::query_scalar("INSERT INTO accounts(owner_user_id) VALUES ($1) RETURNING id")
            .bind(owner).fetch_one(&pool).await.unwrap();
        let org = format!("org-{}", Uuid::new_v4());
        let account: Uuid = sqlx::query_scalar("INSERT INTO accounts(owner_user_id, clerk_org_id) VALUES ($1, $2) RETURNING id")
            .bind(owner).bind(&org).fetch_one(&pool).await.unwrap();

        assert!(!job_org_is_closing(&pool, personal).await.unwrap(), "personal account");
        assert!(!job_org_is_closing(&pool, account).await.unwrap(), "healthy org");
        let deletion: Uuid = sqlx::query_scalar("INSERT INTO org_deletions (account_id, clerk_org_id, initiated_by) VALUES ($1, $2, 'owner') RETURNING id")
            .bind(account).bind(&org).fetch_one(&pool).await.unwrap();
        assert!(job_org_is_closing(&pool, account).await.unwrap(), "deletion requested");
        sqlx::query("UPDATE org_deletions SET state = 'canceled', canceled_at = now() WHERE id = $1")
            .bind(deletion).execute(&pool).await.unwrap();
        assert!(!job_org_is_closing(&pool, account).await.unwrap(), "undone");
        sqlx::query("INSERT INTO org_revocations (clerk_org_id) VALUES ($1)").bind(&org).execute(&pool).await.unwrap();
        assert!(job_org_is_closing(&pool, account).await.unwrap(), "revoked");

        pool.close().await;
        admin.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
    }
}
