//! The "connect your integration" nudge — CS-436, org-onboarding plan §3
//! item 49, D21 (`docs/plans/org-onboarding-from-the-invite.md` ~line 1117
//! for D21, ~line 1329 for item 49's own row).
//!
//! # The failure this guards
//!
//! D21 retires the org install: every member connects each platform with
//! their OWN grant, so approval no longer bridges membership to capture —
//! nothing does, until the member connects. The plan states the failure
//! this worker exists to close, verbatim: "approval grants a billing seat
//! and delivers nothing, and the failure is invisible to everyone: the
//! admin sees an approved member, and Dana — who never connected — sees an
//! empty ocean." This worker is the email side of closing it; `routes::
//! teams::list_org_members`'s `connected_providers`/`connect_nudged_at`
//! fields (org-team-membership PR, this same story) are the admin-surfacing
//! side.
//!
//! # Candidate predicate
//!
//! An ordinary-org member (`accounts.clerk_org_id IS NOT NULL`, excluding
//! [`personal_orgs`](crate::tenancy::personal_org) — see [`candidates_for_account`]'s
//! own doc for why that exclusion is redundant-but-explicit given how this
//! worker is ticked) whose `account_members.joined_at` is older than
//! [`grace_hours`] (default 24h — a brand-new member should not get an
//! email before they have had a reasonable chance to sign in and connect on
//! their own) and who owns NO row in `oauth_tokens` for that account
//! (`owner_user_id = am.user_id` — see `connectors::TokenOwner`'s own doc
//! comment for why this is never `IS NULL OR =`: an account-wide token,
//! Slack's today, must never read as "this member has connected").
//!
//! # Same table, same cooldown, a new provider value
//!
//! Reuses `slack_unmatched_email_notices` (control migration `0227`) under
//! `provider = 'member_connect'` — see `services::unmatched_recipient_
//! email`'s own "member-connect nudge" section for the full claim/cooldown/
//! suppression design this worker calls into. "Same suppression list as the
//! parent plan's PR 6" (item 49) means [`services::unmatched_recipient_
//! email::is_suppressed_any_provider`] is checked before ever attempting a
//! claim — a person who opted out of the Slack-unmatched nudge under a
//! DIFFERENT provider value must never receive this one either.
//!
//! # Claim vs send order — a deliberate DIVERGENCE from `slack_unmatched_email`
//!
//! `workers::slack_unmatched_email::process_job` claims the cooldown slot in
//! its OWN committed transaction, then sends — by that module's own doc
//! comment, "a failed send here is a lost nudge... the next roster walk
//! inside the cooldown window will not re-claim, but the one after it
//! will." That is an accepted trade for a best-effort courtesy email to
//! someone with no CoralSwarm account yet. It is the WRONG trade here: this
//! story's brief requires a failed send to never consume the cooldown, so
//! [`process_account`] holds the claim's transaction OPEN across the Resend
//! call and only commits on a successful send — a failure rolls the
//! transaction back, which reverts the claimed row to whatever it was
//! before (absent, on a fresh insert; its old `last_notified_at`, on a
//! renewal), so the very next tick sees this recipient as eligible again.
//! The cost is a Postgres transaction held open for up to Resend's own 10s
//! timeout per recipient — acceptable at this worker's expected volume (one
//! grace-gated pass over one org's roster per hour by default), and
//! documented here rather than silently inherited from the sibling worker's
//! shape.
//!
//! # Shape: `tenant_harness` sweep worker, but ALWAYS control-plane
//!
//! Every table this worker reads or writes — `account_members`, `accounts`,
//! `oauth_tokens`, `personal_orgs`, `users`, `slack_unmatched_email_notices`
//! — is control-plane-only (`scripts/check_schema_parity.sh`'s
//! `CONTROL_TABLES`), so unlike `billing_derivation`/`platform_rollup`
//! (which collect from `t.pool` and write to a separately-captured control
//! pool) [`tick`] never touches `t.pool` at all. It still registers as a
//! [`crate::workers::tenant_harness::HarnessConfig::sweep_worker`] — the
//! SAME registration shape `billing_derivation` uses (`lib.rs`'s own
//! registration comment for that worker) — because the harness is what
//! already enumerates every active ORG tenant on a schedule; reinventing
//! that enumeration for a single control-plane query would duplicate
//! `tenancy::routing::list_active`'s own polling/backoff logic for no
//! reason. `TenantId::Consumer` (the shared personal-account plane) is a
//! guaranteed no-op — see [`tick`]'s own doc.
//!
//! Fire-and-forget `tokio::spawn`, no drained `JoinHandle` — same choice
//! `billing_derivation`'s own registration makes (`lib.rs`, no
//! `drain_one("billing_derivation", ..)` call exists either): this is a
//! periodic, idempotent sweep with no job queue behind it, so there is no
//! "in-flight work becomes unrecoverable if the process exits mid-tick"
//! risk the way an abandoned QUEUE claim has (`history_backfill`'s own
//! registration comment explains that risk for the queue-worker shape) — a
//! tick interrupted mid-loop simply leaves its remaining candidates for the
//! next tick, [`grace_hours`] later at most.

use std::sync::Arc;
use std::time::Duration;

use sqlx::PgPool;
use uuid::Uuid;

use crate::services::unmatched_recipient_email;
use crate::services::unmatched_recipient_email::{COOLDOWN, MEMBER_CONNECT_PROVIDER};
use crate::tenancy::scope::begin_tenant_system_scoped;
use crate::tenancy::TenantId;
use crate::workers::billing_derivation::resolve_org_account_id;
use crate::workers::tenant_harness::{Tick, WorkerTenant};
use crate::AppState;

/// A brand-new member gets no email before they have had a reasonable
/// chance to sign in and connect on their own. `MEMBER_CONNECT_NUDGE_GRACE_HOURS`
/// overrides; 0 is honoured (an admin who wants the nudge to fire
/// immediately), a negative or unparsable value falls back to the default.
pub const DEFAULT_GRACE_HOURS: i32 = 24;

pub fn grace_hours() -> i32 {
    std::env::var("MEMBER_CONNECT_NUDGE_GRACE_HOURS")
        .ok()
        .and_then(|v| v.trim().parse::<i32>().ok())
        .filter(|h| *h >= 0)
        .unwrap_or(DEFAULT_GRACE_HOURS)
}

/// Sweep cadence — same `_INTERVAL_SECS` env convention as
/// `billing_derivation::derivation_interval`/`platform_rollup::rollup_interval`.
pub const DEFAULT_INTERVAL: Duration = Duration::from_secs(60 * 60);

pub fn nudge_interval() -> Duration {
    std::env::var("MEMBER_CONNECT_NUDGE_INTERVAL_SECS")
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .filter(|secs| *secs > 0)
        .map(Duration::from_secs)
        .unwrap_or(DEFAULT_INTERVAL)
}

/// Hard per-tick cap — same bounding reasoning as `workers::
/// slack_unmatched_email::MAX_RECIPIENTS_PER_TICK`: this worker holds one
/// transaction open per candidate across a Resend call (see the module
/// doc's "claim vs send order" section), so an unbounded per-tick pass could
/// hold connections open for a very long tail. A sweep worker has no queue
/// to enqueue a continuation onto (unlike the job-based sibling) — it simply
/// picks up any remainder on the NEXT tick, [`nudge_interval`] later, since
/// every untouched candidate still matches [`candidates_for_account`]'s own
/// predicate.
const MAX_CANDIDATES_PER_TICK: i64 = 200;

/// One candidate: an org member with no owned `oauth_tokens` row past grace.
pub(crate) struct Candidate {
    user_id: Uuid,
    email: String,
}

/// The candidate predicate (module doc's "Candidate predicate" section),
/// scoped to ONE account. `personal_orgs` is joined to exclude even though a
/// personal org's `clerk_org_id` never resolves to an Org tick in the first
/// place — [`tenancy::personal_org`](crate::tenancy::personal_org)'s own
/// module doc: "A personal org gets no database and no routing row," so
/// [`resolve_org_account_id`] (backed by `tenant_databases`) cannot succeed
/// for one — this predicate re-guards anyway so it stays correct for any
/// OTHER caller (tests included) that hands it an account id directly,
/// rather than depending on that upstream routing fact holding forever.
///
/// **Already-cooled-down and already-suppressed emails are excluded HERE,
/// not just in [`process_account`]'s per-candidate loop** (CodeRabbit round
/// 1, CS-436, Functional Correctness — starvation fix). `LIMIT
/// MAX_CANDIDATES_PER_TICK` orders by `joined_at ASC`, so without this
/// exclusion an account with more eligible members than the cap would have
/// its OLDEST `MAX_CANDIDATES_PER_TICK` occupy the query's result on EVERY
/// tick forever, even once every one of them is either successfully
/// claimed (and now sitting in a 7-day cooldown) or permanently suppressed
/// — [`process_account`]'s loop would keep re-selecting and re-skipping
/// exactly those same rows, and no candidate past the cap would EVER be
/// reached. Excluding both conditions in the SQL itself means the `LIMIT`
/// always picks from members who can actually be acted on this tick, so the
/// window advances tick over tick instead of wedging on the head of the
/// list. `is_suppressed_any_provider`'s any-provider semantics are
/// duplicated here as a second `NOT EXISTS` (not folded into one clause)
/// because the two checks differ: the cooldown check is `provider =
/// 'member_connect'`-specific (this provider's own claim renewal window),
/// while the suppression check is intentionally provider-agnostic (an
/// opt-out under ANY provider excludes a candidate permanently, not just
/// for 7 days). [`process_account`]'s own per-candidate `is_suppressed_
/// any_provider`/claim-`WHERE` checks stay in place as the race-safety net
/// for a suppression or claim landing between this `SELECT` and that loop
/// reaching the row — belt AND suspenders, not either/or, same discipline
/// migration `0230`'s own header documents for the claim's atomicity.
///
/// Runs inside a `begin_tenant_system_scoped(control, account_id)`
/// read-only transaction (`scripts/check_rls_coverage.sh`'s ratchet — this
/// query touches `oauth_tokens`, which carries the `account_isolation`
/// policy from migration `0083_consumer_rls`) rather than the ambient pool,
/// same primitive [`process_account`]'s own claim uses just below. Rolled
/// back, never committed — a `SELECT` has nothing to persist.
pub(crate) async fn candidates_for_account(
    control: &PgPool,
    account_id: Uuid,
    grace_hours: i32,
) -> Result<Vec<Candidate>, String> {
    let mut tx = begin_tenant_system_scoped(control, account_id)
        .await
        .map_err(|e| format!("member_connect_nudge: failed to open scoped read transaction: {e}"))?;
    let rows: Vec<(Uuid, String)> = sqlx::query_as(&format!(
        "SELECT am.user_id, u.email \
         FROM account_members am \
         JOIN users u ON u.id = am.user_id \
         JOIN accounts a ON a.id = am.account_id \
         WHERE am.account_id = $1 \
           AND a.clerk_org_id IS NOT NULL \
           AND NOT EXISTS ( \
               SELECT 1 FROM personal_orgs po WHERE po.clerk_org_id = a.clerk_org_id \
           ) \
           AND am.joined_at < now() - make_interval(hours => $2) \
           AND NOT EXISTS ( \
               SELECT 1 FROM oauth_tokens t \
               WHERE t.account_id = am.account_id AND t.owner_user_id = am.user_id \
           ) \
           AND NOT EXISTS ( \
               SELECT 1 FROM slack_unmatched_email_notices n \
               WHERE n.email = u.email AND n.suppressed_at IS NOT NULL \
           ) \
           AND NOT EXISTS ( \
               SELECT 1 FROM slack_unmatched_email_notices n \
               WHERE n.provider = '{MEMBER_CONNECT_PROVIDER}' AND n.email = u.email \
                 AND n.last_notified_at >= now() - INTERVAL '{COOLDOWN}' \
                 AND n.suppressed_at IS NULL \
           ) \
         ORDER BY am.joined_at ASC \
         LIMIT $3"
    ))
    .bind(account_id)
    .bind(grace_hours)
    .bind(MAX_CANDIDATES_PER_TICK)
    .fetch_all(&mut *tx)
    .await
    .map_err(|e| format!("member_connect_nudge: candidate query failed: {e}"))?;
    tx.rollback().await.ok();

    Ok(rows
        .into_iter()
        .map(|(user_id, email)| Candidate { user_id, email })
        .collect())
}

/// Per-tick outcome — the "alertable skip counters" `scripts::
/// check_silent_drop_is_alertable.sh` asks for: a candidate that is skipped
/// silently is exactly the D21 failure this worker exists to prevent, so
/// every skip reason gets its own counter and [`tick`] logs all of them
/// (not just the non-zero ones — unlike the sibling worker's tombstone log,
/// a zero here is itself informative: "we looked, and everyone who needed
/// one got one").
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct Outcome {
    pub(crate) candidates: usize,
    pub(crate) sent: usize,
    pub(crate) cooldown_skipped: usize,
    pub(crate) suppressed_skipped: usize,
    pub(crate) tombstoned_skipped: usize,
    pub(crate) no_address: usize,
    pub(crate) send_failed: usize,
}

/// Process every candidate for one account. `pub(crate)` so the pg test
/// suite can call it directly with precise counts, rather than parsing log
/// lines emitted by [`tick`].
pub(crate) async fn process_account(
    control: &PgPool,
    state: &AppState,
    account_id: Uuid,
    clerk_org_id: &str,
) -> Result<Outcome, String> {
    let mut outcome = Outcome::default();
    let candidates = candidates_for_account(control, account_id, grace_hours()).await?;
    outcome.candidates = candidates.len();
    if candidates.is_empty() {
        return Ok(outcome);
    }

    // Resolved once per account, not once per candidate — same discipline
    // `workers::slack_unmatched_email::resolve_web_base` documents for
    // itself ("once per walk, not once per member").
    let web_base = state.org_web_base_url(control, clerk_org_id).await;

    for candidate in candidates {
        // Defensive only — `users.email` is `NOT NULL` (migration `0001`)
        // and every row here comes from Clerk's own verified primary
        // address (`clerk_provision`'s own webhook handlers), so this arm
        // is expected to be permanently dead. Kept rather than assumed, the
        // same "residue, not a live gap" posture this crate takes elsewhere
        // — see `connectors::slack_unmatched_email::NoticeAccountCandidate`'s
        // own doc for the precedent of naming a defensive check honestly.
        if candidate.email.trim().is_empty() {
            outcome.no_address += 1;
            continue;
        }

        // D12 tombstone seam — see this worker's own module doc pointer to
        // `clerk_provision::invite_suppressed_by_tombstone`'s doc comment
        // for why a CURRENT member cannot actually be tombstoned for this
        // SAME org (a tombstone records a REMOVAL, and this candidate is
        // still present in `account_members`). Called anyway: the plan
        // names it explicitly, and it keeps this worker's shape identical
        // to `workers::slack_unmatched_email::process_job`'s own tombstone
        // check rather than special-casing it away. Best-effort, like every
        // other non-essential read in that sibling's loop: a failed check
        // falls back to "not suppressed" rather than blocking this
        // candidate over an unrelated read failure.
        match crate::clerk_provision::invite_suppressed_by_tombstone(
            control,
            Some(clerk_org_id),
            Some(candidate.user_id),
            Some(&candidate.email),
        )
        .await
        {
            Ok(true) => {
                outcome.tombstoned_skipped += 1;
                continue;
            }
            Ok(false) => {}
            Err(e) => {
                tracing::warn!(
                    error = %e, account_id = %account_id, user_id = %candidate.user_id,
                    "member_connect_nudge: D12 tombstone check failed — treating as not \
                     suppressed rather than blocking this candidate"
                );
            }
        }

        // Any-provider suppression check — see `services::
        // unmatched_recipient_email::is_suppressed_any_provider`'s own doc
        // comment for why this is a SEPARATE query from the claim's
        // same-provider `WHERE suppressed_at IS NULL`. Best-effort on
        // failure: the claim's own same-provider check still applies below,
        // so a failed read here only risks missing a DIFFERENT provider's
        // opt-out on this one tick, not sending to someone this worker's
        // own provider row already suppressed.
        match unmatched_recipient_email::is_suppressed_any_provider(control, &candidate.email).await
        {
            Ok(true) => {
                outcome.suppressed_skipped += 1;
                continue;
            }
            Ok(false) => {}
            Err(e) => {
                tracing::warn!(
                    error = %e, account_id = %account_id, user_id = %candidate.user_id,
                    "member_connect_nudge: any-provider suppression check failed — proceeding \
                     to the claim, whose own same-provider check still applies"
                );
            }
        }

        let mut tx = match begin_tenant_system_scoped(control, account_id).await {
            Ok(tx) => tx,
            Err(e) => {
                tracing::warn!(
                    error = %e, account_id = %account_id, user_id = %candidate.user_id,
                    "member_connect_nudge: failed to open scoped transaction for cooldown claim \
                     — skipping this candidate, continuing with the rest"
                );
                continue;
            }
        };

        let claimed = match unmatched_recipient_email::claim_member_connect_notice_slot(
            &mut *tx,
            account_id,
            &candidate.email,
        )
        .await
        {
            Ok(c) => c,
            Err(e) => {
                tx.rollback().await.ok();
                tracing::warn!(
                    error = %e, account_id = %account_id, user_id = %candidate.user_id,
                    "member_connect_nudge: cooldown claim query failed — skipping this \
                     candidate, continuing with the rest"
                );
                continue;
            }
        };
        let Some(tokens) = claimed else {
            // Still inside cooldown, lost a concurrent claim, or already
            // suppressed under this SAME provider (the claim's own `WHERE`
            // re-checks `suppressed_at IS NULL` — see `claim_member_connect_
            // notice_slot`'s own doc comment). Every cause reads the same:
            // no rows changed, nothing to commit.
            tx.rollback().await.ok();
            outcome.cooldown_skipped += 1;
            continue;
        };

        // Send WHILE the claim is still uncommitted (module doc's "claim vs
        // send order" section) — commit only on success, roll back on
        // failure so a failed send never burns this recipient's cooldown.
        if unmatched_recipient_email::send_connect_nudge_email(
            state,
            &candidate.email,
            &web_base,
            &tokens.unsubscribe_token,
        )
        .await
        {
            if let Err(e) = tx.commit().await {
                // The email is already out; the claim failing to persist
                // means, worst case, a second nudge could arrive before a
                // full cooldown window — an accepted, narrow residue, same
                // shape as `services::unmatched_recipient_email::send_nudge_
                // email`'s own documented "a failed send is a lost nudge"
                // trade, just inverted (here a failed COMMIT after a
                // successful SEND, not a failed send after a successful
                // claim).
                tracing::warn!(
                    error = %e, account_id = %account_id, user_id = %candidate.user_id,
                    "member_connect_nudge: sent the nudge but failed to commit its cooldown \
                     claim — a second nudge may arrive before a full cooldown window"
                );
            }
            outcome.sent += 1;
        } else {
            tx.rollback().await.ok();
            outcome.send_failed += 1;
        }
    }

    Ok(outcome)
}

/// One pass for one [`WorkerTenant`] — see the module doc's "Shape" section
/// for why this is always control-plane work regardless of which tenant the
/// harness handed this tick, and why `TenantId::Consumer` is a guaranteed
/// no-op (no ordinary org ever lives on the shared personal-account plane —
/// see `tenancy::TenantId`'s own doc comment).
pub async fn tick(t: &WorkerTenant, control: &PgPool, state: &Arc<AppState>) -> Result<Tick, String> {
    let clerk_org_id = match &t.tenant {
        TenantId::Consumer => return Ok(Tick::idle()),
        TenantId::Org(id) => id.clone(),
    };

    let account_id = resolve_org_account_id(control, &clerk_org_id).await?;
    let outcome = process_account(control, state, account_id, &clerk_org_id).await?;

    tracing::info!(
        account_id = %account_id,
        clerk_org_id = %clerk_org_id,
        candidates = outcome.candidates,
        sent = outcome.sent,
        cooldown_skipped = outcome.cooldown_skipped,
        suppressed_skipped = outcome.suppressed_skipped,
        tombstoned_skipped = outcome.tombstoned_skipped,
        no_address = outcome.no_address,
        send_failed = outcome.send_failed,
        "member_connect_nudge: tick outcome"
    );

    Ok(Tick::worked(outcome.candidates))
}
