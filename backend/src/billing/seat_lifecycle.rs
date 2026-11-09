//! Seat acquire/release — the purchase-ledger writers `billing::renewal`'s
//! own module doc now defers to (billing self-serve launch PR F, seat
//! lifecycle — `.scratch/billing-self-serve/pr-f-notes.md`).
//!
//! # The Clerk reality (why there is no `invite_seat`/`remove_seat` HTTP route)
//!
//! Org membership is entirely Clerk-managed: `account_members` rows for an
//! org account are written by `clerk_provision::ensure_org_membership` (the
//! `organizationMembership.created`/`.updated` webhook) and
//! `clerk_provision::refresh_org_membership_from_claims` (first-sight
//! request-path provisioning), and deleted by `clerk_provision::
//! remove_org_membership` (the `organizationMembership.deleted` webhook).
//! There is no in-app "invite" or "remove" action this backend accepts for
//! an org account (`routes::teams::reject_org_account_membership_management`
//! 403s the whole `/v1/account/members` surface for one) — the founder
//! invites and removes members through Clerk's own UI. So "invite = seat
//! purchase" and "remove = seat release" (plan §2.F) are realized as side
//! effects of the TWO membership-writing webhook/claims paths, not as new
//! routes:
//!
//! - [`acquire_seat`] is called by `ensure_org_membership`, AFTER its own
//!   membership transaction commits, and ONLY when that transaction actually
//!   INSERTED a new row (finding 1, review round — a role-only `.updated`
//!   redelivery for an already-existing member must never re-run this). A
//!   failure here propagates as an `Err` all the way out of the Clerk webhook
//!   handler, which answers a 5xx — Clerk's own redelivery loop IS the
//!   visible failure surface (plan §2.F's "add fails visibly if Stripe
//!   fails", realized the only way it can be for a webhook-driven join: there
//!   is no synchronous in-app request to fail instead).
//! - [`release_seat`] is called by `remove_org_membership`, BEFORE its own
//!   deletion transaction — see that function's own doc comment for why the
//!   ordering still matters (the guarded stamp clear needs a row to affect)
//!   and for CS-444's rule: a removed member's slot goes back into the
//!   account's pool, it is NOT un-purchased. Release makes NO Stripe call
//!   and NO `grant_seat_tiers` write at all any more, so "remove = seat
//!   release" no longer means "remove = refund"; `purchased` moves only when
//!   an admin buys or reduces slots (`routes::org_slots`).
//! - `refresh_org_membership_from_claims` (the hot request-path auth
//!   provisioning path) calls NEITHER of these — see its own doc comment.
//!   A brand-new member provisioned there is picked up idempotently the
//!   moment the corresponding Clerk webhook lands (before or after that
//!   request), because [`acquire_seat`] keys its own idempotency off
//!   `account_members.seat_purchased_at`, not off which caller happened to
//!   create the row.
//!
//! The only NEW user-facing HTTP surface this PR adds is the tier-change
//! route (`routes::billing_seats`) — moving an ALREADY-purchased seat
//! between tiers, which is a real in-app action with no Clerk event to hang
//! it off.
//!
//! # Purchase-driven, not roster-driven
//!
//! `account_members.seat_tier_id` (0169) says which tier a member is
//! ASSIGNED — a fact that exists for every org member regardless of
//! billing. `account_members.seat_purchased_at` (0176) says whether that
//! assignment is BACKED by a live Stripe seat purchase — `NULL` until
//! [`acquire_seat`] sets it, forever `NULL` for a member who joined before
//! their org ever subscribed (FOUNDER-CONFIRM, pr-f-notes.md: "members who
//! existed BEFORE the org subscribed are never auto-purchased — entitlement
//! = what Stripe bills... bulk 'buy seats for existing members' belongs in
//! PR H's UI"). `grant_seat_tiers` (0168) is the resulting PURCHASE LEDGER
//! for the account's active `team` grant — incrementally maintained by this
//! module (and by `routes::billing_seats`' tier-change route), never
//! recomputed from the roster any more (see `billing::renewal`'s own module
//! doc, "`grant_seat_tiers` is a purchase-driven ledger").
//!
//! # No active/non-frozen team subscription ⇒ no-op (ACQUIRE only)
//!
//! [`acquire_seat`] reads `stripe_subscriptions` first and does nothing —
//! neither a Stripe call nor a ledger write — when: no row exists at all (an
//! org that has never gone through checkout), the row carries no
//! `stripe_subscription_id` yet (a Customer exists but no subscription has
//! completed), or [`crate::billing::enforcement::status_is_frozen`] reads
//! `true` (`past_due`/`canceled` — plan §1.11, freeze-not-forfeit). Pushing a
//! Stripe quantity ADD against a frozen subscription would either error or
//! resurrect billing nobody asked for, so acquisition stays blanket-blocked
//! on either frozen status.
//!
//! **[`release_seat`] does not read the subscription at all (CS-444).** This
//! section used to carry finding 7a's three-way split for release —
//! `past_due` still pushes a DECREASE, `canceled` skips the push and
//! decrements the local ledger instead, no-subscription no-ops — because a
//! release's whole job was choosing which Stripe/ledger write to make. A
//! release now makes NEITHER: it clears one member's `seat_purchased_at` and
//! stops, which frees their slot back into the account's pool with
//! `purchased` untouched. That is identical work for every status, so the
//! status is not consulted. The bug finding 7a closed (a member removed
//! while `past_due` left a permanently unreclaimable billed seat) is
//! structurally impossible under this design rather than merely handled: the
//! slot stays purchased ON PURPOSE and the next joiner consumes it.
//! [`release_overhang`] — the eventual-consistency backstop that fix paired
//! with — was already retired by CS-435; see its own doc comment.
//!
//! # S1 — Stripe-effective tier (billing self-serve PR F review, findings 4/7)
//!
//! A pending seat-tier downgrade (`routes::billing_seats`) pushes Stripe's
//! subscription item to the NEW (lower) tier IMMEDIATELY, with
//! `proration_behavior: none` — the member already paid the OLD tier through
//! period end, so Stripe's actual billed reality changes at request time,
//! while `account_members.seat_tier_id` itself (and `grant_seat_tiers`, which
//! tracks it) deliberately waits for the next roll (`billing::renewal`'s own
//! module doc, "Pending downgrades, applied at roll"). Every path in this
//! module that needs to know WHICH Stripe subscription item corresponds to a
//! member's seat must therefore resolve against
//! [`MemberSeatState::effective_tier_id`] — `pending_seat_tier_id ??
//! seat_tier_id` — never the raw `seat_tier_id` alone, or it will try to
//! touch an item that either no longer exists at that price or belongs to a
//! DIFFERENT tier's seats entirely.
//!
//! **Inside THIS module that now means [`acquire_seat`] only (CS-444).**
//! Finding 4 named the hazard as a mismatch between two keys living in one
//! function: [`release_seat`] resolved the STRIPE item by the effective tier
//! while its LOCAL LEDGER decrement stayed keyed to the lagging plain
//! `seat_tier_id` (deliberately — `grant_seat_tiers` lags a pending
//! downgrade exactly the way `seat_tier_id` does, so the ledger row that
//! ACTUALLY still counts the seat is the one under the lagging key). A
//! release no longer touches Stripe OR the ledger, so neither key appears in
//! it and the hazard has no foothold there. It is NOT fixed globally — it
//! still lives wherever a Stripe resolution and a ledger write share a
//! function: `routes::billing_seats::change_seat_tier`'s move computation
//! (that module's own doc comment), `billing::renewal`'s roll, the two
//! different `used` keys `billing::slot_pool` documents per tier, and
//! `routes::org_slots::reduce_slots`, which — like the admin purchase path
//! it mirrors — keys BOTH sides off the ledger's own tier, since an admin
//! reduces a tier they bought, not a tier some member is pending into.
//!
//! # S2 — seat mutations are serialized under the advisory lock, ACROSS the
//! # Stripe call (billing self-serve PR F review, finding 5)
//!
//! (**CS-444: [`release_seat`] makes no Stripe call any more, so "across the
//! Stripe call" now describes [`acquire_seat`] alone. Release still opens the
//! same account-scoped transaction and takes the same advisory lock FIRST —
//! freeing a slot changes `billing::slot_pool::SlotPool::available`, and
//! every other reader/writer of that number decides under this lock.**)
//!
//! An earlier version of [`acquire_seat`]/[`release_seat`] read the member's
//! purchase-guard state and the account's subscription status BEFORE opening
//! any transaction (via a short-lived scoped tx that committed immediately),
//! made the Stripe call, and only THEN opened a fresh locked transaction to
//! write the ledger. That left two real gaps: the idempotency guard
//! (`seat_purchased_at`) was read OUTSIDE any lock, so two near-simultaneous
//! calls for the same member (Clerk's `.created`/`.updated` redelivery
//! window) could both observe "not yet purchased" and both push a Stripe
//! quantity change before either wrote back; and the eventual ledger write
//! ran under a lock taken only AFTER Stripe had already answered, so the
//! window between "Stripe succeeded" and "the lock is held" was itself
//! unprotected.
//!
//! Both functions now: open an account-scoped transaction, take
//! [`crate::billing::renewal::lock_period_rotation`] FIRST, re-read the
//! idempotency guard and the account's subscription state INSIDE that lock,
//! and — if the call is going to do real work — make the Stripe call and
//! write the ledger BEFORE committing, all under the SAME held transaction.
//! This means a pooled DB connection sits idle for the duration of one Stripe
//! HTTP round trip. That is a deliberate, accepted tradeoff for these
//! LOW-FREQUENCY membership/tier events (a join, a leave, a tier change — not
//! a hot request path), the OPPOSITE of the decision `routes::checkout::
//! refuse_if_already_subscribed`'s own doc comment makes for
//! `create_subscription_checkout` (a genuinely user-facing, potentially
//! higher-frequency path, where that function's doc comment explicitly
//! declines to hold a lock or a transaction across the equivalent Stripe call
//! and instead documents the resulting race as a known, narrow gap for the
//! founder to resolve later). The inbound `customer.subscription.updated`
//! event our own push triggers takes the SAME advisory lock
//! (`routes::stripe_webhook::apply_subscription_updated`) before it does
//! anything — so it simply serializes BEHIND our commit rather than racing
//! it, and there is no deadlock: we never wait on that inbound webhook
//! finishing, only it waits on us.
//!
//! # Stripe push BEFORE the DB write, always (ACQUIRE only, as of CS-444)
//!
//! This section described BOTH functions until CS-444 removed
//! [`release_seat`]'s Stripe push and its ledger write; a release now makes
//! exactly one DB write (clearing `seat_purchased_at`) and no external call,
//! so there is no ordering left to get wrong there — and, correspondingly,
//! no "Stripe succeeded but the local commit did not" drift a release can
//! produce at all. What follows applies to [`acquire_seat`].
//!
//! It calls Stripe FIRST and only writes `grant_seat_tiers`/
//! `grants.seats`/`seat_purchased_at` after the Stripe call returns `Ok` —
//! now within the SAME held transaction (S2), rather than a separately-opened
//! one. This is the same ordering `routes::checkout`'s known, accepted race
//! (`refuse_if_already_subscribed`'s own doc comment) already lives with:
//! a genuinely-should-never-happen failure between the two (Stripe succeeds,
//! then this process crashes before the local commit) leaves Stripe's
//! quantity ahead of this ledger until the drift is caught by
//! `RenewalOutcome::seat_mismatch` at the next roll, `apply_subscription_updated`'s
//! own drift warning, or [`release_overhang`]'s sweep — visible, not silent,
//! and self-healing the next time this same member's membership event is
//! redelivered (idempotent via `seat_purchased_at`/quantity comparison
//! either way). The alternative order (write local state first, then call
//! Stripe) is worse: a Stripe failure after a committed local write would
//! leave this ledger claiming a seat nobody is actually being billed for,
//! with no natural redelivery to fix it (the membership event that
//! triggered the local write already succeeded and will not be redelivered).
//!
//! **Stamp UPDATEs are guarded and rows-affected-checked (S2 belt-and-
//! suspenders).** Both functions still do this, for slightly different
//! reasons. [`acquire_seat`]: after the Stripe call succeeds it stamps
//! `seat_purchased_at` FIRST, guarded (`AND seat_purchased_at IS NULL`), and
//! only proceeds to the `grant_seat_tiers`/`grants.seats` write if that
//! stamp actually affected a row. A 0-row stamp means some OTHER path won
//! this exact member's guard between the read at the top of the call and
//! there — effectively unreachable under the advisory lock in normal
//! operation, but checked anyway rather than trusted — and the ledger write
//! is skipped (logged loudly) rather than double-counting on top of whatever
//! that other path already recorded. Stripe has already been pushed by that
//! point and cannot be un-pushed transactionally; the resulting drift is
//! exactly what the signals named two paragraphs up exist to surface.
//! [`release_seat`]: the guard (`AND seat_purchased_at IS NOT NULL`) is the
//! whole operation rather than a precondition for a second write, and it
//! matters MORE under CS-444 than it did before — a double release that
//! cleared the stamp twice would return a slot to the pool that was never
//! taken from it, inflating `available()` once per redelivered
//! `organizationMembership.deleted`. A 0-row clear there leaves nothing to
//! undo (nothing was pushed, nothing else is written) and is logged at
//! `warn`, not `error`.
//!
//! # Idempotency-key recipes (billing self-serve PR F review, finding 8)
//!
//! (**CS-444: there is no release recipe any more — [`release_seat`] makes no
//! Stripe call, so it mints no key. `routes::org_slots`' buy and reduce
//! actions take a CLIENT-supplied key instead, folded into an account- and
//! action-namespaced recipe; see that module's own doc comment.**)
//!
//! **A FRESH UUID suffix per logical call, not a static per-(account, user,
//! action, tier) key.** An earlier version used
//! `seat:{account_id}:{user_id}:acquire:{tier_code}` / `...:release:{tier_code}`
//! — stable across every invocation for the same member and tier. Stripe
//! caches an `Idempotency-Key`'s response for roughly 24 hours; a member who
//! LEAVES and REJOINS the same account within that window (a real, ordinary
//! sequence — an accidental removal undone within the hour, a contractor
//! re-added the same tier they held before) would have their SECOND
//! `acquire_seat` replay the FIRST call's cached Stripe response instead of
//! actually pushing a new quantity increase, silently under-billing (or, for
//! a release-then-acquire pair sharing the release's stale key shape,
//! mis-billing in the other direction). The key's job is narrower than
//! cross-call dedup: protecting ONE logical acquire/release/reclaim attempt
//! against Stripe seeing the SAME HTTP POST twice (a client-side
//! timeout-and-retry within that one call), which a fresh
//! `uuid::Uuid::new_v4()` suffix generated ONCE per call and reused only
//! across THAT call's own internal retries still provides — cross-call dedup
//! (a redelivered Clerk webhook re-invoking [`acquire_seat`] for a member who
//! is ALREADY purchased) is S2's job instead: the in-lock re-check of
//! `seat_purchased_at` before any Stripe call is what makes a genuine
//! redelivery a clean no-op, not the idempotency key. `routes::billing_seats`'
//! tier-change route applies the identical fix to its own `idem_base`
//! recipe — see that module's own doc comment.

use std::time::Duration;

use sqlx::{PgConnection, PgPool};
use uuid::Uuid;

use super::stripe_client::{Idem, StripeClient, StripeError};

#[derive(Debug, thiserror::Error)]
pub enum SeatLifecycleError {
    #[error("database error: {0}")]
    Db(#[from] sqlx::Error),
    #[error("stripe error: {0}")]
    Stripe(#[from] StripeError),
    /// [`SeatPurchasePolicy::PoolOnly`] and the member's tier has no free
    /// purchased slot: nothing was bought and nothing was stamped. The
    /// caller asked not to buy, so this is a refusal, not a failure to retry.
    #[error("no free seat in the account's purchased pool")]
    NoFreeSeat,
}

/// Whether [`acquire_seat_with_policy`] may buy a seat when the member's
/// tier has no free slot in the purchased pool.
///
/// `AllowPurchase` is the long-standing behaviour (the Clerk webhook, join
/// approvals, auto-add, and an invite from a client that predates the
/// `allow_purchase` field). `PoolOnly` is what `POST /v1/orgs/invites` uses
/// when the caller sends `allow_purchase: false`: the web shows a seat quote
/// and buys seats explicitly first, so the invite itself must never buy one.
/// The pool read and the refusal happen inside the same locked transaction
/// as the consume, so two concurrent invites cannot both take the last slot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SeatPurchasePolicy {
    #[default]
    AllowPurchase,
    PoolOnly,
}

/// Total wall-clock budget for the ENTIRE Stripe call sequence inside one
/// locked seat mutation (billing self-serve PR F review, MAJOR finding).
/// `stripe_client::stripe_http_client`'s own doc comment: `StripeClient::
/// execute`'s retry loop has no total deadline of its own — "the consuming
/// routes... must apply their own outer budget for anything on a
/// synchronous user path — that is not bounded here." Without one, S2
/// (this module's own doc section) holding the account-scoped transaction
/// and advisory lock ACROSS the Stripe call becomes unbounded too: a
/// worst-case single request can take up to roughly 15s (the per-request
/// timeout) times [`crate::billing::stripe_client`]'s retry budget, PLUS a
/// Stripe-supplied `Retry-After` as large as `MAX_RETRY_AFTER_HONOURED`
/// (60s) honoured on EACH retry — 165s+ in the worst case, all of it spent
/// holding a pooled DB connection and blocking every OTHER writer of this
/// account's billing state (`roll_period`, a concurrent seat mutation) for
/// the duration. [`acquire_seat`]/[`release_seat`] each make up to ONE
/// retrieve + one mutation; [`release_overhang`] makes one retrieve plus one
/// mutation PER surplus tier — 45s comfortably covers the ordinary
/// (non-retried) case for either shape while still bounding the pathological
/// one to a bounded, retryable failure instead of an unbounded hang.
pub(crate) const SEAT_STRIPE_OP_DEADLINE: Duration = Duration::from_secs(45);

/// Runs `fut` (the FULL Stripe call sequence for one locked seat mutation)
/// under [`SEAT_STRIPE_OP_DEADLINE`], converting an expired deadline to
/// [`StripeError::Timeout`] — the same error variant a single request's own
/// `reqwest` timeout already produces, so callers (and their own error
/// mapping / tracing) need no new branch to handle this.
pub(crate) async fn with_stripe_deadline<F, T>(fut: F) -> Result<T, StripeError>
where
    F: std::future::Future<Output = Result<T, StripeError>>,
{
    match tokio::time::timeout(SEAT_STRIPE_OP_DEADLINE, fut).await {
        Ok(result) => result,
        Err(_elapsed) => Err(StripeError::Timeout),
    }
}

/// This member's own tier assignment + pending downgrade + purchase state —
/// read fresh at the top of [`acquire_seat`]/[`release_seat`]/
/// [`release_overhang`], inside the account's advisory lock (S2), never
/// trusted from a caller-passed value.
struct MemberSeatState {
    seat_tier_id: Uuid,
    pending_seat_tier_id: Option<Uuid>,
    seat_purchased_at: Option<chrono::DateTime<chrono::Utc>>,
}

impl MemberSeatState {
    /// S1 — the tier Stripe is ACTUALLY billing this member's seat at right
    /// now: the pending downgrade's target if one is in flight (already
    /// pushed to Stripe at request time — `routes::billing_seats`'s own doc
    /// comment), otherwise the member's plain `seat_tier_id`. See this
    /// module's own "S1" doc section for where this must (and must not) be
    /// used.
    fn effective_tier_id(&self) -> Uuid {
        self.pending_seat_tier_id.unwrap_or(self.seat_tier_id)
    }
}

const MEMBER_SEAT_STATE_SQL: &str = "\
    SELECT seat_tier_id, pending_seat_tier_id, seat_purchased_at FROM account_members \
     WHERE account_id = $1 AND user_id = $2";

/// `account_members` carries no RLS (see `billing::renewal`'s own reads of
/// it, and 0169's header: "already has no tenant twin... carries no RLS
/// policy of its own") — reads inside the caller's already-open transaction
/// need no additional scoping beyond what [`crate::tenancy::scope::
/// begin_account_scoped`] already established for the OTHER, RLS-covered
/// tables the same transaction touches (`grants`/`grant_seat_tiers`).
///
/// `None` when the member row does not exist — reachable only if a caller
/// invokes this out of the order its own doc comment requires (this
/// module's functions are always called with a still-existing
/// `account_members` row in view). Logged and treated as a no-op rather than
/// an error: a membership write that already committed (or a delete not yet
/// committed) must not be retried forever over billing state that cannot
/// resolve, the same "no way to resolve X -> Ok, silently" posture
/// `billing::renewal::seed_member_period` already uses throughout.
async fn read_member_seat_state(
    tx: &mut PgConnection,
    account_id: Uuid,
    user_id: Uuid,
) -> Result<Option<MemberSeatState>, sqlx::Error> {
    sqlx::query_as::<_, (Uuid, Option<Uuid>, Option<chrono::DateTime<chrono::Utc>>)>(MEMBER_SEAT_STATE_SQL)
        .bind(account_id)
        .bind(user_id)
        .fetch_optional(&mut *tx)
        .await
        .map(|row| {
            row.map(|(seat_tier_id, pending_seat_tier_id, seat_purchased_at)| MemberSeatState {
                seat_tier_id,
                pending_seat_tier_id,
                seat_purchased_at,
            })
        })
}

/// The account's Stripe subscription linkage + status — RLS-covered (0162),
/// read inside the caller's already-open, already-locked (S2) transaction.
const SUBSCRIPTION_STATE_SQL: &str =
    "SELECT stripe_subscription_id, status FROM stripe_subscriptions WHERE account_id = $1";

/// [`acquire_seat`]'s subscription gate: `Some(subscription_id)` only for a
/// LIVE, non-frozen subscription — see this module's own doc comment, "No
/// active/non-frozen team subscription ⇒ no-op", for the three ways this
/// reads `None` and what each means. Acquisition stays blanket-blocked on
/// EITHER frozen status (`past_due` or `canceled`) — pushing a quantity ADD
/// against a subscription this account is not currently paying for correctly
/// would either error at Stripe or resurrect billing nobody asked for.
async fn live_subscription_id_for_acquire(
    tx: &mut PgConnection,
    account_id: Uuid,
) -> Result<Option<String>, sqlx::Error> {
    let row: Option<(Option<String>, String)> = sqlx::query_as(SUBSCRIPTION_STATE_SQL)
        .bind(account_id)
        .fetch_optional(&mut *tx)
        .await?;
    let Some((subscription_id, status)) = row else {
        return Ok(None);
    };
    if crate::billing::enforcement::status_is_frozen(Some(&status)) {
        return Ok(None);
    }
    Ok(subscription_id)
}

/// **CS-444 — deleted with the Stripe push it existed to route.** This
/// module used to carry a `ReleaseSubscriptionState` enum and a
/// `release_subscription_state` reader alongside
/// [`live_subscription_id_for_acquire`], splitting a release three ways
/// (`None`/`Canceled`/`Live`) purely to decide WHICH Stripe call a release
/// should make. [`release_seat`] no longer makes one at all — a removed
/// member's slot returns to the pool rather than being un-purchased (see
/// that function's own doc comment) — so the whole split, and the
/// subscription read that fed it, is gone: nothing about a release's
/// behaviour depends on the account's Stripe status any more. Acquisition
/// still reads [`SUBSCRIPTION_STATE_SQL`] through
/// [`live_subscription_id_for_acquire`], which is why that constant stays.
///
/// `(code, stripe_price_id)` for a `seat_tiers` row — `seat_tiers` carries no
/// RLS (global reference data, same posture `routes::checkout`'s catalog
/// reads document), so this can run against any executor, including the
/// caller's already-open transaction.
///
/// Mirrors `routes::checkout`'s "fail loud on an unprovisioned price"
/// posture (that module's own doc comment): a tier that exists but carries a
/// NULL `stripe_price_id` is an INTERNAL error (provisioning has not run for
/// this environment), never treated as "unknown tier" — by the time a
/// member's `seat_tier_id` resolves to a real row, the tier itself is not in
/// question, only whether Stripe has been provisioned for it.
async fn seat_tier_price(
    tx: &mut PgConnection,
    seat_tier_id: Uuid,
) -> Result<(String, String), SeatLifecycleError> {
    let row: Option<(String, Option<String>)> =
        sqlx::query_as("SELECT code, stripe_price_id FROM seat_tiers WHERE id = $1")
            .bind(seat_tier_id)
            .fetch_optional(&mut *tx)
            .await?;
    match row {
        Some((code, Some(price_id))) => Ok((code, price_id)),
        Some((code, None)) => {
            tracing::error!(
                seat_tier_id = %seat_tier_id, tier_code = %code,
                "seat_lifecycle: seat_tiers row exists but stripe_price_id is NULL — provisioning has not run"
            );
            Err(SeatLifecycleError::Db(sqlx::Error::Protocol(format!(
                "seat tier {code} is not provisioned with a Stripe price"
            ))))
        }
        None => {
            // `seat_tier_id` is a real FK on `account_members` (0169) — a
            // missing row here means the referenced `seat_tiers` row was
            // deleted out from under a live FK, which this schema's
            // migrations never do (seat tiers are deactivated via
            // `is_active`, never removed). Loud, not silently skipped.
            Err(SeatLifecycleError::Db(sqlx::Error::RowNotFound))
        }
    }
}

/// The account's active `team` grant id — same "most recently granted,
/// non-revoked" selection every other current-grant lookup in this crate
/// uses (`billing::renewal::CURRENT_GRANT_SQL`,
/// `routes::stripe_webhook::apply_subscription_updated`'s own lookup),
/// narrowed to `plans.code = 'team'` since only a mixed-tier grant carries a
/// `grant_seat_tiers` breakdown at all. Read inside the caller's own
/// account-scoped transaction (`grants` is RLS-covered, 0150).
const CURRENT_TEAM_GRANT_SQL: &str = "\
    SELECT g.id FROM grants g JOIN plans p ON p.id = g.plan_id \
     WHERE g.account_id = $1 AND g.revoked_at IS NULL AND p.code = $2 \
     ORDER BY g.granted_at DESC, g.id DESC LIMIT 1";

/// Acquire a seat for `user_id` on `account_id`, at the tier
/// `account_members.seat_tier_id` already holds for them (assigned by the
/// caller — `clerk_provision::ensure_org_membership` resolves `standard` for
/// a plain join; `routes::billing_seats`' tier-change route is a DIFFERENT
/// operation, a MOVE of an already-purchased seat, not an acquire).
///
/// No-op (`Ok(())`, no Stripe call, no write) when: the member's seat is
/// ALREADY purchased (`seat_purchased_at IS NOT NULL` — idempotent under
/// Clerk redelivery), or the account has no live, non-frozen team
/// subscription (see this module's own doc comment).
///
/// # CS-435 (D1/D7/D9): join = consume a bought slot, else purchase
///
/// Was: **join (webhook) = purchase** — every INSERTED `account_members` row
/// pushed a Stripe quantity ADD, unconditionally. Now: a member holding a
/// live `slot_reservations` row, or joining while their own
/// **(CS-442) per-tier** `billing::slot_pool::read_slot_pool_for_tier_ledger(..,
/// member.effective_tier_id()).available() > 0`, is stamped
/// `seat_purchased_at` straight out of the account's already-purchased pool
/// — **no Stripe call, no `grant_seat_tiers` increment** — because a slot at
/// THAT member's own tier was already bought (either by `routes::org_slots`'
/// admin "buy N slots" action, or by an earlier over-purchase). Only when
/// that tier's own pool is empty does this fall through to the ORIGINAL
/// purchase path below, against that tier's Stripe price. See
/// `billing::slot_pool`'s own module doc for the full D1/D7/D9/CS-442
/// framing and why fulfilling a live reservation never re-checks
/// `available()` (D9's "not a third number").
///
/// # S2: everything below runs inside ONE locked transaction
///
/// See this module's own "S2" doc section for why the account-scoped
/// transaction and [`crate::billing::renewal::lock_period_rotation`] are
/// taken FIRST, before even the idempotency-guard re-read, and held across
/// the Stripe call — the CS-435 pool-consumption check above runs inside the
/// SAME transaction, for the identical reason.
///
/// # Ordering (caller contract)
///
/// Call this AFTER the membership row has been committed, and ONLY when that
/// commit actually INSERTED a new row (finding 1 — a role-only `.updated`
/// must never reach here). See this module's own doc comment for why
/// `ensure_org_membership` calls this outside its own transaction.
pub async fn acquire_seat(
    pool: &PgPool,
    stripe: Option<&StripeClient>,
    account_id: Uuid,
    user_id: Uuid,
) -> Result<(), SeatLifecycleError> {
    acquire_seat_with_policy(pool, stripe, account_id, user_id, SeatPurchasePolicy::AllowPurchase).await
}

/// [`acquire_seat`] with an explicit [`SeatPurchasePolicy`]. With
/// `PoolOnly`, an empty pool for the member's tier returns
/// [`SeatLifecycleError::NoFreeSeat`] after rolling back, before any Stripe
/// call or ledger write. Every no-op case of [`acquire_seat`] (already
/// purchased, no live subscription, no member row) stays a no-op: none of
/// them buys anything.
pub async fn acquire_seat_with_policy(
    pool: &PgPool,
    stripe: Option<&StripeClient>,
    account_id: Uuid,
    user_id: Uuid,
    policy: SeatPurchasePolicy,
) -> Result<(), SeatLifecycleError> {
    let mut tx = crate::tenancy::scope::begin_account_scoped(pool, account_id)
        .await
        .map_err(|e| sqlx::Error::Protocol(e.to_string()))?;
    crate::billing::renewal::lock_period_rotation(&mut tx, account_id).await?;

    let Some(member) = read_member_seat_state(&mut tx, account_id, user_id).await? else {
        tracing::warn!(
            %account_id, %user_id,
            "seat_lifecycle::acquire_seat: no account_members row found — skipping (caller contract violation, or the member left before this ran)"
        );
        tx.commit().await?;
        return Ok(());
    };
    if member.seat_purchased_at.is_some() {
        tx.commit().await?;
        return Ok(()); // already purchased — idempotent no-op.
    }

    let Some(subscription_id) = live_subscription_id_for_acquire(&mut tx, account_id).await? else {
        tx.commit().await?;
        return Ok(()); // no live, non-frozen subscription — tier assignment only.
    };

    // ── CS-435 (D1/D7/D9): consume before it buys ─────────────────────────
    // "join (webhook) = purchase" is now "join = consume a bought slot, else
    // purchase" — see this module's own doc comment's Ordering section and
    // `billing::slot_pool`'s module doc for D1/D7/D9's full framing. Reads
    // the SAME snapshot [`billing::slot_pool::read_slot_pool`] computes,
    // inside the SAME locked transaction opened above — no separate lock,
    // no separate commit boundary, so a concurrent `reserve_slot`/
    // `routes::org_slots` purchase can never observe a torn view of this
    // decision.
    //
    // A member holding a LIVE reservation (`billing::slot_pool::
    // reserve_slot`, D9 — not wired to any caller in this PR, but a
    // reservation taken by a future caller must still be honoured here) is
    // always fulfilled from the pool, regardless of a fresh `available()`
    // read: `reserve_slot` already charged this member's claim against
    // `reserved` the moment it was taken, so re-deriving `available()` here
    // and finding it non-positive would incorrectly refuse a slot that was
    // already promised. Fulfilling it is a straight `reserved -= 1, used +=
    // 1` — net zero against `purchased`, exactly D9's own "not a third
    // number" framing. The claim is atomic (`released_at IS NULL AND
    // expires_at > now()`); a 0-row UPDATE (expired, or the sweep already
    // resolved it) must not stamp `seat_purchased_at` — re-read
    // availability and continue through the existing pool or Stripe path.
    let live_reservation: Option<Uuid> = sqlx::query_scalar(
        "SELECT id FROM slot_reservations \
          WHERE account_id = $1 AND user_id = $2 AND released_at IS NULL AND expires_at > now()",
    )
    .bind(account_id)
    .bind(user_id)
    .fetch_optional(&mut *tx)
    .await?;

    let claimed_reservation = if let Some(reservation_id) = live_reservation {
        let claimed = sqlx::query(
            "UPDATE slot_reservations SET released_at = now(), release_reason = 'fulfilled' \
              WHERE id = $1 AND released_at IS NULL AND expires_at > now()",
        )
        .bind(reservation_id)
        .execute(&mut *tx)
        .await?;
        claimed.rows_affected() > 0
    } else {
        false
    };
    // CS-442 (item 40, D8): per-tier, not account-wide — a member's own
    // (Stripe-effective) tier must have a free purchased slot of ITS OWN
    // before this consumes from the pool, or a `pro` member could silently
    // eat a `standard` slot someone else paid for. `used` is ledger-keyed
    // (`seat_tier_id`, not the Stripe-effective COALESCE) so a pending
    // premium→standard member still occupies the premium seat until roll.
    let consume_from_pool = if claimed_reservation {
        true
    } else {
        crate::billing::slot_pool::read_slot_pool_for_tier_ledger(&mut tx, account_id, member.effective_tier_id())
            .await?
            .available()
            > 0
    };
    if consume_from_pool {
        // Guarded, rows-affected-checked — same belt-and-suspenders as the
        // purchase path below, even though there is no Stripe push here to
        // un-do: a 0-row stamp still means another path already recorded
        // this member as purchased, and the ledger (untouched by a pool
        // consumption) must not be written twice either way.
        let stamp = sqlx::query(
            "UPDATE account_members SET seat_purchased_at = now() \
              WHERE account_id = $1 AND user_id = $2 AND seat_purchased_at IS NULL",
        )
        .bind(account_id)
        .bind(user_id)
        .execute(&mut *tx)
        .await?;
        if stamp.rows_affected() == 0 {
            tracing::error!(
                %account_id, %user_id,
                "seat_lifecycle::acquire_seat: seat_purchased_at stamp affected 0 rows during a pool \
                 consumption despite holding the per-account advisory lock — another path already \
                 recorded this seat as purchased; no Stripe push and no ledger write happened here, \
                 so nothing needs to be undone"
            );
        }
        tx.commit().await?;
        return Ok(()); // consumed from the pre-purchased pool — no Stripe call, no ledger increment.
    }

    if policy == SeatPurchasePolicy::PoolOnly {
        // Decided under the same lock as the pool read above, so this is not
        // a check-then-act: no other seat mutation on this account can run
        // between the read and this refusal.
        tx.rollback().await?;
        return Err(SeatLifecycleError::NoFreeSeat);
    }

    let Some(stripe) = stripe else {
        tracing::error!(
            %account_id, %user_id,
            "seat_lifecycle::acquire_seat: account has a live subscription but Stripe is not configured — refusing"
        );
        tx.rollback().await?;
        return Err(SeatLifecycleError::Stripe(StripeError::NotConfigured));
    };

    // S1: an unpurchased seat can never carry a pending downgrade (the
    // tier-change route 409s on `seat_purchased_at IS NULL`), so
    // `effective_tier_id` is always plain `seat_tier_id` here — routed
    // through the same helper as `release_seat` for consistency, not because
    // this call site can actually observe a pending tier.
    let (tier_code, price_id) = seat_tier_price(&mut tx, member.effective_tier_id()).await?;
    // Finding 8: a fresh key per logical call, not a static per-member/tier
    // recipe — see this module's own doc comment.
    let idem_key = format!("seat:{account_id}:{user_id}:acquire:{tier_code}:{}", Uuid::new_v4());

    // Billing self-serve PR F review, MAJOR finding: bound the WHOLE Stripe
    // sequence below (one retrieve, one mutation) under
    // `SEAT_STRIPE_OP_DEADLINE` — see that constant's own doc comment for
    // why an unbounded retry/backoff sequence here would hold this
    // function's lock and transaction open indefinitely.
    with_stripe_deadline(async {
        let sub = stripe.retrieve_subscription(&subscription_id).await?;
        match sub.items.iter().find(|it| it.price_id == price_id) {
            Some(item) => {
                stripe
                    .update_subscription_item(&item.id, item.quantity + 1, "create_prorations", Idem::Key(&idem_key))
                    .await?;
            }
            None => {
                stripe
                    .add_subscription_item(&subscription_id, &price_id, 1, "create_prorations", Idem::Key(&idem_key))
                    .await?;
            }
        }
        Ok(())
    })
    .await?;

    // Stripe succeeded — now record it, still inside the SAME locked
    // transaction (S2). Stamp FIRST, guarded, rows-affected checked (see
    // this module's own doc comment) — only proceed to the ledger write if
    // this call genuinely won the stamp.
    let stamp = sqlx::query(
        "UPDATE account_members SET seat_purchased_at = now() \
          WHERE account_id = $1 AND user_id = $2 AND seat_purchased_at IS NULL",
    )
    .bind(account_id)
    .bind(user_id)
    .execute(&mut *tx)
    .await?;
    if stamp.rows_affected() == 0 {
        tracing::error!(
            %account_id, %user_id,
            "seat_lifecycle::acquire_seat: seat_purchased_at stamp affected 0 rows despite holding \
             the per-account advisory lock — another path already recorded this seat as purchased. \
             Stripe was already incremented above and cannot be un-pushed transactionally; skipping \
             the local ledger write to avoid double-counting. The resulting drift is exactly what \
             RenewalOutcome::seat_mismatch / apply_subscription_updated's drift warning exist to \
             surface."
        );
        tx.commit().await?;
        return Ok(());
    }

    let grant_id: Option<Uuid> = sqlx::query_scalar(CURRENT_TEAM_GRANT_SQL)
        .bind(account_id)
        .bind(crate::billing::MIXED_TIER_PLAN_CODE)
        .fetch_optional(&mut *tx)
        .await?;
    let Some(grant_id) = grant_id else {
        // Should not happen: a live, non-frozen subscription (checked above)
        // implies `checkout.session.completed` already issued the `team`
        // grant this account is being billed under. Loud, not silent — the
        // Stripe side already changed; see this module's own doc comment,
        // "Stripe push BEFORE the DB write, always", for why that ordering
        // is accepted and how the resulting drift is caught.
        tracing::error!(
            %account_id, %user_id,
            "seat_lifecycle::acquire_seat: Stripe seat push succeeded but no active team grant was found to record it against"
        );
        tx.rollback().await?;
        return Err(SeatLifecycleError::Db(sqlx::Error::RowNotFound));
    };

    sqlx::query(
        "INSERT INTO grant_seat_tiers (grant_id, seat_tier_id, seats) VALUES ($1, $2, 1) \
         ON CONFLICT (grant_id, seat_tier_id) DO UPDATE SET seats = grant_seat_tiers.seats + 1",
    )
    .bind(grant_id)
    .bind(member.seat_tier_id)
    .execute(&mut *tx)
    .await?;
    crate::billing::renewal::resync_grant_seats(&mut tx, grant_id).await?;

    tx.commit().await?;
    Ok(())
}

/// Whether adding one more `seat_tier_id` member to `account_id` right now
/// would have to BUY a seat: true only when the account has a live,
/// non-frozen subscription and that tier's purchased pool has no free slot.
/// An account with no live subscription never buys (see [`acquire_seat`]),
/// so it reads false.
///
/// A read-only early answer for `POST /v1/orgs/invites` with
/// `allow_purchase: false`, so an invite that cannot fit is refused before
/// any Clerk user or membership is created. It is NOT the guarantee: the
/// pool can change after this returns. [`acquire_seat_with_policy`] with
/// [`SeatPurchasePolicy::PoolOnly`] re-decides under the account lock.
pub async fn new_member_needs_purchase(
    pool: &PgPool,
    account_id: Uuid,
    seat_tier_id: Uuid,
) -> Result<bool, SeatLifecycleError> {
    let mut tx = crate::tenancy::scope::begin_account_scoped(pool, account_id)
        .await
        .map_err(|e| sqlx::Error::Protocol(e.to_string()))?;
    let needs = if live_subscription_id_for_acquire(&mut tx, account_id).await?.is_none() {
        false
    } else {
        crate::billing::slot_pool::read_slot_pool_for_tier_ledger(&mut tx, account_id, seat_tier_id)
            .await?
            .available()
            <= 0
    };
    tx.rollback().await?;
    Ok(needs)
}

/// Release `user_id`'s seat on `account_id` — **the slot goes back into the
/// account's pool. It is NOT un-purchased.**
///
/// # CS-444: a removed member frees a slot; only an admin changes the pool
///
/// Founder decision, 2026-09-22 (org-onboarding plan, Phase E): *"The pool
/// only changes when an admin explicitly adds or reduces slots."*
///
/// ```text
/// Buy 10   ->  purchased 10, used  0
/// Fill 10  ->  purchased 10, used 10
/// Remove 1 ->  purchased 10, used  9, available 1
/// Auto-add ->  consumes the free slot, no Stripe call
/// ```
///
/// This function used to do three things: push a Stripe subscription-item
/// quantity DECREMENT, decrement `grant_seat_tiers.seats`, and clear
/// `account_members.seat_purchased_at`. Since CS-435 made `purchased` =
/// Σ `grant_seat_tiers.seats` (`billing::slot_pool`'s own module doc), the
/// middle one meant an org that bought 10 slots up front dropped to 9 the
/// moment anybody left — the slot they paid for evaporated instead of
/// freeing up for the next person, and the org had to buy it a second time.
/// **Only the third thing survives**, and it is the whole of "the slot is
/// free": `used` is `COUNT(account_members WHERE seat_purchased_at IS NOT
/// NULL)`, so clearing the stamp is exactly what raises
/// [`crate::billing::slot_pool::SlotPool::available`] by one, with
/// `purchased` held steady. The next joiner consumes it through
/// [`acquire_seat`]'s own consume-before-buy branch — no Stripe call, no
/// second purchase.
///
/// # No Stripe client, by construction (not merely by convention)
///
/// This function does not TAKE a [`StripeClient`] any more. "Release never
/// pushes Stripe" is therefore not a property a future edit can quietly
/// break and a test has to catch — there is no client in scope to call. The
/// three Stripe-shaped branches that used to live here are gone with it:
///
/// - the `canceled` branch (finding 7a — "no push; decrement the ledger
///   locally instead") had nothing left to decide once there is neither a
///   push nor a ledger decrement to choose between. A release is now
///   identical for `active`, `past_due` and `canceled` accounts, so the
///   account's subscription status is not read at all.
/// - the `past_due` split (finding 7a's other half — "`past_due` is OUR
///   enforcement state, not a Stripe inability to accept a DECREASE")
///   likewise collapses. The bug that finding closed — a member removed
///   while `past_due` left a permanently unreclaimable billed seat, because
///   their `account_members` row was deleted moments later and nothing
///   could ever read their `seat_tier_id` again — **cannot recur under this
///   design at all**: the slot stays purchased on purpose, and stays
///   consumable by the next joiner, so there is nothing left to strand.
/// - the Stripe-drift `None` branch ("no matching subscription item for
///   this tier — Stripe already reflects the release; reconcile the local
///   ledger only") described a mismatch between Stripe's items and this
///   ledger. Release no longer reads or writes either, so it can no longer
///   observe that drift. The branch is not lost, though: the same
///   tolerate-and-warn shape now lives in `routes::org_slots::reduce_slots`,
///   which is the one path that still pushes a Stripe DECREASE.
///
/// The former `ReleaseSubscriptionState` enum and `release_subscription_state`
/// reader are deleted outright — see their replacement comment above
/// [`seat_tier_price`].
///
/// # What is deliberately KEPT
///
/// - **The guarded, rows-affected-checked stamp clear.** `UPDATE ... SET
///   seat_purchased_at = NULL WHERE ... AND seat_purchased_at IS NOT NULL`,
///   with `rows_affected()` checked: a double release (Clerk redelivering
///   `organizationMembership.deleted`) frees the slot exactly once, and the
///   0-row case is logged rather than trusted. It matters MORE now, not
///   less: freeing a slot twice would hand the pool a slot nobody gave
///   back.
/// - **The S2 lock discipline.** The account-scoped transaction and
///   [`crate::billing::renewal::lock_period_rotation`] are still taken
///   FIRST, even though this function no longer holds them across any
///   external call. Freeing a slot changes `available()`, and every other
///   reader/writer of that number ([`acquire_seat`]'s consume-before-buy
///   branch, `billing::slot_pool::reserve_slot`,
///   `routes::org_slots::reduce_slots`' own refusal gate) decides under the
///   same lock — an unlocked release could make a reduce's "this many slots
///   are free" read stale between its check and its write.
/// - **The no-op guards.** No `account_members` row, or
///   `seat_purchased_at IS NULL` already: nothing to free, `Ok(())`.
///
/// # The pending-downgrade lag is gone from THIS path
///
/// The S1 hazard this function used to carry — the Stripe item keys off
/// [`MemberSeatState::effective_tier_id`] (`pending_seat_tier_id ??
/// seat_tier_id`) while the ledger keys off the lagging plain
/// `seat_tier_id`, and mixing the two up breaks in one direction or the
/// other — required BOTH a Stripe resolution and a ledger write to exist in
/// the same function. Neither does now. Clearing `seat_purchased_at` is
/// tier-agnostic: it is one member's one row, whatever tier they sit on.
/// The lag itself is not fixed (it still lives in
/// `routes::billing_seats::change_seat_tier`, `billing::renewal`'s roll, and
/// `billing::slot_pool`'s per-tier split), it simply no longer has a
/// foothold here. Note the consequence, which is correct and intended: a
/// member with a pending premium→standard downgrade who LEAVES leaves their
/// slot behind in the ledger's `premium` bucket (the tier that was actually
/// bought), not in `standard`.
///
/// # Ordering (caller contract) — unchanged
///
/// Call this BEFORE deleting the `account_members` row.
/// `clerk_provision::remove_org_membership` still does. The reason is weaker
/// than it was (there is no `seat_tier_id` to read off the doomed row any
/// more), but the ordering is still required: the guarded stamp clear needs
/// a row to affect, and its `rows_affected()` check is what makes a
/// redelivery safe.
///
/// # What the stamp clear is actually load-bearing FOR (mutation review,
/// # PR #1030)
///
/// Worth stating plainly, because the answer is narrower than it looks and
/// the next reader should not have to infer it. **Every caller today —
/// there is exactly one, `clerk_provision::remove_org_membership` — deletes
/// the `account_members` row immediately afterwards.** Since
/// [`crate::billing::slot_pool::SlotPool::used`] is a COUNT over rows that
/// CARRY a stamp, the deletion alone would drop `used` and free the slot
/// even if this UPDATE did nothing at all. A mutation that turned the clear
/// into a no-op write was invisible to the offboarding-path test for exactly
/// that reason. So the clear's real jobs are these three, in descending
/// order of how much they currently matter:
///
/// 1. **The double-release guard.** `AND seat_purchased_at IS NOT NULL` plus
///    the `rows_affected()` check is what makes a redelivered
///    `organizationMembership.deleted` a no-op. This is the job that is
///    load-bearing on the path that exists.
/// 2. **The window between the two transactions.** The release commits on
///    its own, and `remove_org_membership` opens a SEPARATE transaction for
///    the tombstone + delete. Between those commits the row is still there,
///    and this UPDATE is the only thing making the slot free to a concurrent
///    reader (`reserve_slot`, `routes::org_slots::reduce_slots`' refusal
///    gate). Narrow, but real — and wider than it looks when the caller's
///    own delete fails or is retried, since that caller swallows a release
///    error and proceeds regardless.
/// 3. **Any future caller that releases WITHOUT deleting.** None exists
///    today. One plausibly will: revoking a member's slot while keeping
///    their membership (an admin downgrading someone to no seat) is the
///    obvious shape, and org-onboarding's auto-add flow could grow one. For
///    such a caller the clear would be the ENTIRE operation, so it is
///    written to be correct for that case now rather than retrofitted then.
///
/// `billing::seat_lifecycle::pg_tests::release_seat_frees_the_slot_and_leaves_purchased_untouched`
/// is the test that pins the mechanism itself, with no delete involved;
/// `clerk_provision::pg_tests::offboarding_frees_the_slot_without_calling_stripe`
/// pins it on the real path by reading the stamp mid-flight, while that
/// caller's delete transaction is parked.
///
/// # Return value (item 4, CS-449 review round 2)
///
/// `Ok(true)` iff this call GENUINELY freed a slot — the guarded `UPDATE`
/// below actually cleared a set stamp. `Ok(false)` for every no-op: no
/// membership row, never held a purchased slot, or the stamp was already
/// cleared by a concurrent/earlier release (the `rows_affected() == 0`
/// branch further down). [`crate::clerk_provision::remove_org_membership`]
/// is the one caller, and it is exactly this boolean that decides whether
/// to trigger [`crate::routes::org_join_requests::drain_org`] afterwards —
/// a removal that freed nothing (no purchased slot to begin with, or a
/// redelivered `.deleted` webhook whose first delivery already cleared the
/// stamp) has no reason to re-gate a backlog nothing changed for.
pub async fn release_seat(
    pool: &PgPool,
    account_id: Uuid,
    user_id: Uuid,
) -> Result<bool, SeatLifecycleError> {
    let mut tx = crate::tenancy::scope::begin_account_scoped(pool, account_id)
        .await
        .map_err(|e| sqlx::Error::Protocol(e.to_string()))?;
    crate::billing::renewal::lock_period_rotation(&mut tx, account_id).await?;

    let Some(member) = read_member_seat_state(&mut tx, account_id, user_id).await? else {
        tx.commit().await?;
        return Ok(false); // no membership row — nothing to free.
    };
    if member.seat_purchased_at.is_none() {
        tx.commit().await?;
        return Ok(false); // never held a purchased slot — nothing to give back.
    }

    // The ONE write a release makes. Guarded and rows-affected-checked for
    // the same belt-and-suspenders reason this module's own doc comment
    // gives for [`acquire_seat`]'s stamp — and for a sharper one here: a
    // second release that also cleared this stamp would return a slot to the
    // pool that was never taken from it, inflating `available()` by one
    // every time Clerk redelivered a `.deleted` event.
    let cleared = sqlx::query(
        "UPDATE account_members SET seat_purchased_at = NULL \
          WHERE account_id = $1 AND user_id = $2 AND seat_purchased_at IS NOT NULL",
    )
    .bind(account_id)
    .bind(user_id)
    .execute(&mut *tx)
    .await?;
    if cleared.rows_affected() == 0 {
        // Effectively unreachable while the advisory lock is held (the read
        // above saw the stamp set inside this same transaction), but checked
        // rather than trusted. Nothing was pushed anywhere and nothing else
        // is written below, so there is nothing to undo — strictly quieter
        // than it was when a Stripe push had already happened by this point,
        // hence `warn!` rather than the old `error!`.
        tracing::warn!(
            %account_id, %user_id,
            "seat_lifecycle::release_seat: seat_purchased_at clear affected 0 rows despite holding \
             the per-account advisory lock — another path already freed this slot. Nothing else is \
             written by a release, so the pool is already correct; not freeing it a second time."
        );
        tx.commit().await?;
        return Ok(false);
    }

    // No `grant_seat_tiers` decrement, no `resync_grant_seats`, no Stripe:
    // `purchased` is the admin's number (`routes::org_slots`' buy/reduce
    // actions), never a side effect of who is on the roster today. See this
    // function's own doc comment, and `billing::renewal::RenewalOutcome::
    // seat_mismatch`'s (which has fired only on `seats < members` since
    // CS-435, so the `purchased > members` pool this leaves behind is not a
    // false alarm waiting to happen).
    tracing::info!(
        %account_id, %user_id, seat_tier_id = %member.seat_tier_id,
        "seat_lifecycle::release_seat: slot returned to the account's pool (purchased unchanged)"
    );

    tx.commit().await?;
    Ok(true)
}


/// **Retired by CS-435 (D1/D7/D9, org-onboarding plan §3 item 7) — now a
/// documented no-op, kept only so its two existing callers
/// (`routes::stripe_webhook::apply_invoice_paid`/`apply_subscription_updated`)
/// need no call-site change.**
///
/// # Why this is now provably wrong to run, not merely stale
///
/// This function used to compare, per purchased tier, `grant_seat_tiers.seats`
/// (the ledger) against how many roster members currently hold that tier AND
/// are marked purchased ("attached"), and Stripe-decrement + ledger-decrement
/// any positive difference as an "unattached, orphaned" seat — see the
/// pre-CS-435 revision of this doc comment (git history) for the full
/// original design, which was correct under the assumption this crate NEVER
/// held. Summed across every tier, `Σ ledger_seats` IS `grants.seats`
/// (`billing::renewal::RECONCILE_GRANTS_SEATS_SQL`'s own invariant) and
/// `Σ attached` is exactly `billing::slot_pool::SlotPool::used` (every
/// member with `seat_purchased_at IS NOT NULL`, summed over their one tier
/// each) — so `Σ surplus` this function computed was **always, exactly,
/// `purchased - used`: the pool's own `available()` headroom**, not
/// evidence of anything gone wrong. Before CS-435 that was fine, because
/// `purchased > used` could only arise from the narrow failure this
/// function's own name describes (a `release_seat` Stripe push that failed
/// during offboarding, `clerk_provision::remove_org_membership`'s own doc
/// comment, finding 6). **After CS-435, `purchased > used` is the pool's
/// intended, common, HEALTHY steady state** — an admin's `routes::org_slots`
/// "buy N slots" action deliberately creates it, ahead of anyone joining.
/// This function's old body could not tell "an admin just bought headroom
/// nobody has joined into yet" apart from "a failed release orphaned a
/// seat" — both are `purchased > used` — and would Stripe-decrement (i.e.
/// silently CANCEL) the very slots `routes::org_slots` just sold, the moment
/// the next `invoice.paid`/`customer.subscription.updated` webhook landed.
/// There is no narrower fix: the per-tier signal this function read is
/// mathematically indistinguishable from legitimate pool headroom in
/// aggregate, so ANY reclaim threshold above zero risks the same failure at
/// a smaller scale.
///
/// # What replaces it
///
/// Nothing does, for the case this function used to catch — and per this
/// doc comment's own reasoning, nothing NEEDS to: an offboarding leaves that
/// slot counted in `purchased`, uncounted in `used` — which is to say, it
/// becomes ordinary pool `available()` headroom instead of a stranded,
/// unreclaimable billed seat. The NEXT member who joins
/// (`billing::seat_lifecycle::acquire_seat`'s consume-before-buy branch) or
/// the next admin `POST /v1/orgs/slots` simply consumes it — the pool
/// absorbs exactly the failure mode this function existed to reclaim, for
/// free. What this function's OLD doc comment named as its job (closing a
/// stranded, unusable purchased seat) is retired because the thing it
/// reclaimed FOR — usability — is no longer blocked in the first place.
/// `billing::renewal::RenewalOutcome::seat_mismatch` (also updated by
/// CS-435) remains the signal for the direction that IS still an anomaly:
/// `purchased < used`, which this sweep never reclaimed anyway.
///
/// **CS-444 turned that from a consolation into the design.** When this
/// paragraph was written, `purchased > used` after an offboarding was the
/// residue of a FAILED [`release_seat`] Stripe push — tolerable because the
/// pool absorbed it. [`release_seat`] no longer pushes or decrements at all,
/// so EVERY departure leaves exactly that state, deliberately: the founder's
/// rule is that a removed member's slot returns to the pool and only an
/// admin's explicit buy/reduce (`routes::org_slots`) moves `purchased`. The
/// reasoning above — that this sweep cannot tell healthy headroom from an
/// orphan, and would Stripe-cancel slots an admin just bought — is now true
/// of the ordinary remove-a-member flow too, which is one more reason this
/// function must stay a no-op rather than ever being revived.
pub async fn release_overhang(
    _pool: &PgPool,
    _stripe: Option<&StripeClient>,
    account_id: Uuid,
) -> Result<(), SeatLifecycleError> {
    tracing::info!(
        %account_id,
        "seat_lifecycle::release_overhang: no-op (retired by CS-435 — see this function's own doc \
         comment); purchased-ahead-of-used is now the pool's healthy steady state, not an orphan to \
         reclaim"
    );
    Ok(())
}

// ── Live-Postgres tests ──────────────────────────────────────────────────────
//
// Same discipline as `billing::renewal::pg_tests`, gated on
// `RELINFER_TEST_DATABASE_URL`, `--test-threads=1` in CI. There is nothing to
// unit-test here: the entire behaviour is which rows get written, and a mock
// would restate the code under test.
//
// CodeRabbit round 3, PR #660: the tests below that mutate the shared
// `seat_tiers.standard`/`premium` rows now take `crate::pg_catalog_test_support`'s
// `CATALOG_PRICE_LOCK` and use its `set_seat_tier_price` (capture the
// previous value, restore it, run fixture cleanup, THEN assert) — the same
// discipline `routes::billing_seats::pg_tests`/`routes::checkout::pg_tests`
// already followed. This module used to mutate those rows with no lock and
// no restore at all, which is exactly the "one suite captures another's
// temporary price as its own original" race the consolidation closes.
#[cfg(test)]
mod pg_tests {

    async fn execute_financial_fixture<'q>(
        pool: &sqlx::PgPool,
        account_id: Uuid,
        query: sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments>,
    ) -> Result<sqlx::postgres::PgQueryResult, sqlx::Error> {
        let mut tx = crate::tenancy::scope::begin_account_scoped(pool, account_id)
            .await
            .map_err(|e| sqlx::Error::Protocol(e.to_string()))?;
        crate::billing::renewal::lock_period_rotation(&mut tx, account_id).await?;
        match query.execute(&mut *tx).await {
            Ok(result) => {
                tx.commit().await?;
                Ok(result)
            }
            Err(error) => {
                tx.rollback().await?;
                Err(error)
            }
        }
    }

    async fn fetch_financial_fixture_id<'q>(
        pool: &sqlx::PgPool,
        account_id: Uuid,
        query: sqlx::query::QueryScalar<'q, sqlx::Postgres, Uuid, sqlx::postgres::PgArguments>,
    ) -> Result<Uuid, sqlx::Error> {
        let mut tx = crate::tenancy::scope::begin_account_scoped(pool, account_id)
            .await
            .map_err(|e| sqlx::Error::Protocol(e.to_string()))?;
        crate::billing::renewal::lock_period_rotation(&mut tx, account_id).await?;
        match query.fetch_one(&mut *tx).await {
            Ok(result) => {
                tx.commit().await?;
                Ok(result)
            }
            Err(error) => {
                tx.rollback().await?;
                Err(error)
            }
        }
    }

    use super::*;
    use crate::pg_catalog_test_support::{set_seat_tier_price, CATALOG_PRICE_LOCK};

    async fn pg_test_pool(test_name: &str) -> Option<PgPool> {
        let url = match std::env::var("RELINFER_TEST_DATABASE_URL") {
            Ok(u) => u,
            Err(_) => {
                eprintln!("skipping {test_name}: set RELINFER_TEST_DATABASE_URL to run it");
                return None;
            }
        };
        Some(
            sqlx::postgres::PgPoolOptions::new()
                .max_connections(4)
                .connect(&url)
                .await
                .expect("connect to test Postgres"),
        )
    }

    /// A fallback mock returning ONE fixed subscription snapshot for every
    /// request, regardless of method/path — `retrieve_subscription` is the
    /// only call in this module that parses a strict shape; every other call
    /// (`update_subscription_item`/`add_subscription_item`/
    /// `delete_subscription_item`) just needs a 200 with valid JSON, which
    /// this same body satisfies. Matches `routes::stripe_webhook`'s own
    /// `spawn_subscription_mock` shape (that module's `mod pg_tests` is
    /// private to its own file, hence this near-identical copy rather than a
    /// cross-module dependency on another route file's test internals).
    async fn spawn_seat_mock(price_id: &str, quantity: i64) -> StripeClient {
        let body = serde_json::json!({
            "id": "sub_mock_1",
            "object": "subscription",
            "status": "active",
            "items": {
                "object": "list",
                "data": [{
                    "id": "si_mock_1",
                    "quantity": quantity,
                    "current_period_start": 1_700_000_000,
                    "current_period_end": 1_702_678_400,
                    "price": {"id": price_id},
                }],
            },
        });
        let app = axum::Router::new().fallback(axum::routing::any(move || {
            let body = body.clone();
            async move {
                (
                    axum::http::StatusCode::OK,
                    [("content-type", "application/json")],
                    body.to_string(),
                )
            }
        }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind mock");
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.ok();
        });
        StripeClient::new(reqwest::Client::new(), "sk_test_mock").with_base_url(format!("http://{addr}"))
    }

    /// A `team`-plan org account with one member per entry of `tiers`
    /// (`"standard"`/`"premium"`), a `team` grant, and a linked
    /// `stripe_subscriptions` row (`status = 'active'` by column default) —
    /// mirrors `routes::stripe_webhook`'s `seed_team_linked_account` shape,
    /// but leaves `grant_seat_tiers` EMPTY (nobody purchased yet) rather than
    /// pre-seeding it, since that is exactly the state [`acquire_seat`]'s
    /// tests need to start from. Callers that need a PURCHASED starting
    /// state call [`seed_purchased_tiers`] themselves afterward.
    async fn seed_team_account(pool: &PgPool, tiers: &[&str]) -> (Uuid, Vec<Uuid>, Uuid) {
        let mut users = Vec::new();
        for i in 0..tiers.len().max(1) {
            let id = Uuid::new_v4();
            sqlx::query("INSERT INTO users (id, email) VALUES ($1, $2)")
                .bind(id)
                .bind(format!("seat-lifecycle-{}-{i}@test.example", id.simple()))
                .execute(pool)
                .await
                .expect("insert user");
            users.push(id);
        }
        let account_id = Uuid::new_v4();
        sqlx::query("INSERT INTO accounts (id, owner_user_id, clerk_org_id) VALUES ($1, $2, $3)")
            .bind(account_id)
            .bind(users[0])
            .bind(format!("org_{}", account_id.simple()))
            .execute(pool)
            .await
            .expect("insert org account");
        for (i, tier_code) in tiers.iter().enumerate() {
            let seat_tier_id: Uuid =
                sqlx::query_scalar("SELECT id FROM seat_tiers WHERE code = $1 AND is_active = true")
                    .bind(tier_code)
                    .fetch_one(pool)
                    .await
                    .unwrap_or_else(|e| panic!("seat_tiers {tier_code:?} must be seeded (0165): {e}"));
            sqlx::query(
                "INSERT INTO account_members (account_id, user_id, role, seat_tier_id) \
                 VALUES ($1, $2, $3, $4)",
            )
            .bind(account_id)
            .bind(users[i])
            .bind(if i == 0 { "owner" } else { "member" })
            .bind(seat_tier_id)
            .execute(pool)
            .await
            .expect("insert tiered membership");
        }

        let plan_id: Uuid = sqlx::query_scalar("SELECT id FROM plans WHERE code = $1")
            .bind(crate::billing::MIXED_TIER_PLAN_CODE)
            .fetch_one(pool)
            .await
            .expect("team plan must be seeded (0167)");
        let grant_id: Uuid = fetch_financial_fixture_id(
            pool,
            account_id,
            sqlx::query_scalar(
            "INSERT INTO grants (account_id, plan_id, seats, plan_is_seat_priced) \
             VALUES ($1, $2, 1, true) RETURNING id",
        )
        .bind(account_id)
        .bind(plan_id),
        )
        .await
        .expect("insert team grant");

        execute_financial_fixture(pool, account_id, sqlx::query(
            "INSERT INTO stripe_subscriptions (account_id, stripe_customer_id, stripe_subscription_id) \
             VALUES ($1, $2, $3)",
        )
        .bind(account_id)
        .bind(format!("cus_{}", Uuid::new_v4().simple()))
        .bind(format!("sub_{}", Uuid::new_v4().simple())))
        .await
        .expect("link stripe subscription");

        (account_id, users, grant_id)
    }

    async fn cleanup(pool: &PgPool, account_id: Uuid, users: &[Uuid]) {
        let _ = execute_financial_fixture(
            pool,
            account_id,
            sqlx::query("DELETE FROM accounts WHERE id = $1").bind(account_id),
        ).await;
        for u in users {
            let _ = sqlx::query("DELETE FROM users WHERE id = $1").bind(u).execute(pool).await;
        }
    }

    async fn member_seat_purchased_at(
        pool: &PgPool,
        account_id: Uuid,
        user_id: Uuid,
    ) -> Option<chrono::DateTime<chrono::Utc>> {
        sqlx::query_scalar("SELECT seat_purchased_at FROM account_members WHERE account_id = $1 AND user_id = $2")
            .bind(account_id)
            .bind(user_id)
            .fetch_one(pool)
            .await
            .expect("read seat_purchased_at")
    }

    async fn ledger_seats(pool: &PgPool, grant_id: Uuid, tier_code: &str) -> Option<i32> {
        sqlx::query_scalar(
            "SELECT gst.seats FROM grant_seat_tiers gst \
             JOIN seat_tiers st ON st.id = gst.seat_tier_id \
             WHERE gst.grant_id = $1 AND st.code = $2",
        )
        .bind(grant_id)
        .bind(tier_code)
        .fetch_optional(pool)
        .await
        .expect("read grant_seat_tiers")
    }

    async fn grants_seats(pool: &PgPool, grant_id: Uuid) -> i32 {
        sqlx::query_scalar("SELECT seats FROM grants WHERE id = $1")
            .bind(grant_id)
            .fetch_one(pool)
            .await
            .expect("read grants.seats")
    }

    /// `acquire_seat` pushes Stripe, writes the ledger, and stamps
    /// `seat_purchased_at` — then a SECOND call is a pure no-op (idempotent
    /// under Clerk redelivery), because the first call already set
    /// `seat_purchased_at`. **Also the finding-5 coverage item: two
    /// sequential acquire calls for the same member — simulating
    /// `.created`/`.updated` near-simultaneous redelivery — yield exactly ONE
    /// ledger seat.** Genuine CONCURRENT safety is provided by S2's advisory
    /// lock (documented in this module's own module doc); a flaky
    /// concurrency test is deliberately not added here — this sequential
    /// version proves the same idempotency guard the lock protects.
    #[tokio::test]
    async fn acquire_seat_is_idempotent_via_seat_purchased_at() {
        let Some(pool) = pg_test_pool("acquire_seat_is_idempotent_via_seat_purchased_at").await
        else {
            return;
        };
        let _catalog_guard = CATALOG_PRICE_LOCK.lock().await;
        let (account_id, users, grant_id) = seed_team_account(&pool, &["standard"]).await;
        let user_id = users[0];
        let original_standard =
            set_seat_tier_price(&pool, "standard", Some("price_seat_lifecycle_mock_1")).await;
        let stripe = spawn_seat_mock("price_seat_lifecycle_mock_1", 0).await;

        let purchased_before = member_seat_purchased_at(&pool, account_id, user_id).await;

        let first_result = acquire_seat(&pool, Some(&stripe), account_id, user_id).await;
        let purchased_after_first = member_seat_purchased_at(&pool, account_id, user_id).await;
        let ledger_after_first = ledger_seats(&pool, grant_id, "standard").await;
        let grants_after_first = grants_seats(&pool, grant_id).await;

        // Second call: idempotent no-op — no double-count.
        let second_result = acquire_seat(&pool, Some(&stripe), account_id, user_id).await;
        let ledger_after_second = ledger_seats(&pool, grant_id, "standard").await;
        let grants_after_second = grants_seats(&pool, grant_id).await;

        set_seat_tier_price(&pool, "standard", original_standard.as_deref()).await;
        cleanup(&pool, account_id, &users).await;

        assert_eq!(purchased_before, None);
        first_result.expect("first acquire must succeed");
        assert!(purchased_after_first.is_some());
        assert_eq!(ledger_after_first, Some(1));
        assert_eq!(grants_after_first, 1);
        second_result.expect("second (redelivered) acquire must also succeed");
        assert_eq!(
            ledger_after_second,
            Some(1),
            "a redelivered acquire must not double-count the seat"
        );
        assert_eq!(grants_after_second, 1);
    }

    /// No live subscription (no `stripe_subscriptions` row at all) ⇒ pure
    /// tier-assignment no-op: no Stripe call (passing `stripe: None` would
    /// itself error if the function tried to use it), no ledger write.
    #[tokio::test]
    async fn acquire_seat_is_a_noop_without_a_live_subscription() {
        let Some(pool) = pg_test_pool("acquire_seat_is_a_noop_without_a_live_subscription").await
        else {
            return;
        };
        let owner = Uuid::new_v4();
        sqlx::query("INSERT INTO users (id, email) VALUES ($1, $2)")
            .bind(owner)
            .bind(format!("no-sub-{}@test.example", owner.simple()))
            .execute(&pool)
            .await
            .expect("insert user");
        let account_id = Uuid::new_v4();
        sqlx::query("INSERT INTO accounts (id, owner_user_id, clerk_org_id) VALUES ($1, $2, $3)")
            .bind(account_id)
            .bind(owner)
            .bind(format!("org_{}", account_id.simple()))
            .execute(&pool)
            .await
            .expect("insert org account");
        let standard_id: Uuid =
            sqlx::query_scalar("SELECT id FROM seat_tiers WHERE code = 'standard' AND is_active = true")
                .fetch_one(&pool)
                .await
                .expect("standard seat tier");
        sqlx::query(
            "INSERT INTO account_members (account_id, user_id, role, seat_tier_id) VALUES ($1, $2, 'owner', $3)",
        )
        .bind(account_id)
        .bind(owner)
        .bind(standard_id)
        .execute(&pool)
        .await
        .expect("insert membership");

        // `stripe: None` — if this function tried to reach Stripe it would
        // hard-error rather than silently no-op, so `Ok(())` here proves the
        // no-live-subscription guard fired BEFORE any Stripe call.
        acquire_seat(&pool, None, account_id, owner)
            .await
            .expect("no live subscription must be a clean no-op, not an error");
        assert_eq!(member_seat_purchased_at(&pool, account_id, owner).await, None);

        cleanup(&pool, account_id, &[owner]).await;
    }

    /// A mock that fails EVERY request — proves zero Stripe calls happened,
    /// the same trick `release_overhang`'s own CS-435 tests use.
    async fn spawn_any_request_fails_mock() -> StripeClient {
        let app = axum::Router::new()
            .fallback(axum::routing::any(|| async { axum::http::StatusCode::INTERNAL_SERVER_ERROR }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind mock");
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.ok();
        });
        StripeClient::new(reqwest::Client::new(), "sk_test_mock").with_base_url(format!("http://{addr}"))
    }

    /// **CS-435 (D1/D7).** `purchased(2) > used(0)` — a pre-bought pool
    /// nobody has joined into yet. `acquire_seat` must consume straight from
    /// it: `seat_purchased_at` stamped, `grant_seat_tiers` UNCHANGED, and —
    /// proven by [`spawn_any_request_fails_mock`] — ZERO Stripe calls.
    #[tokio::test]
    async fn acquire_seat_consumes_a_purchased_slot_without_stripe_when_the_pool_has_room() {
        let Some(pool) =
            pg_test_pool("acquire_seat_consumes_a_purchased_slot_without_stripe_when_the_pool_has_room")
                .await
        else {
            return;
        };
        let _catalog_guard = CATALOG_PRICE_LOCK.lock().await;
        let (account_id, users, grant_id) = seed_team_account(&pool, &["standard"]).await;
        let user_id = users[0];
        let original_standard = set_seat_tier_price(&pool, "standard", None).await;
        execute_financial_fixture(
            &pool,
            account_id,
            sqlx::query("INSERT INTO grant_seat_tiers (grant_id, seat_tier_id, seats) \
                     SELECT $1, id, 2 FROM seat_tiers WHERE code = 'standard' AND is_active = true")
            .bind(grant_id),
        )
            .await
            .expect("seed a pre-bought pool of 2, nobody consumed yet");
        execute_financial_fixture(
            &pool,
            account_id,
            sqlx::query("UPDATE grants SET seats = 2 WHERE id = $1").bind(grant_id),
        ).await.unwrap();

        let stripe = spawn_any_request_fails_mock().await;

        let result = acquire_seat(&pool, Some(&stripe), account_id, user_id).await;
        let purchased_after = member_seat_purchased_at(&pool, account_id, user_id).await;
        let ledger_after = ledger_seats(&pool, grant_id, "standard").await;
        let grants_after = grants_seats(&pool, grant_id).await;

        set_seat_tier_price(&pool, "standard", original_standard.as_deref()).await;
        cleanup(&pool, account_id, &users).await;

        result.expect("must succeed consuming from the pool without ever calling Stripe");
        assert!(purchased_after.is_some(), "the member must be stamped purchased");
        assert_eq!(ledger_after, Some(2), "the ledger must NOT change — the slot was already bought");
        assert_eq!(grants_after, 2);
    }

    /// **CS-435 (D9).** A member holding a LIVE `slot_reservations` row is
    /// fulfilled — the reservation flips to `release_reason = 'fulfilled'`
    /// and the member is stamped purchased — with ZERO Stripe calls, even
    /// though `reserve_slot` itself already reduced `available()` to 0 for
    /// this account (D9's own "not a third number": fulfilling a
    /// reservation must never re-check `available()`, or the exact claim
    /// `reserve_slot` already promised would be refused here).
    #[tokio::test]
    async fn acquire_seat_fulfills_a_live_reservation_without_stripe() {
        let Some(pool) = pg_test_pool("acquire_seat_fulfills_a_live_reservation_without_stripe").await
        else {
            return;
        };
        let _catalog_guard = CATALOG_PRICE_LOCK.lock().await;
        let (account_id, users, grant_id) = seed_team_account(&pool, &["standard"]).await;
        let user_id = users[0];
        let original_standard = set_seat_tier_price(&pool, "standard", None).await;
        execute_financial_fixture(
            &pool,
            account_id,
            sqlx::query("INSERT INTO grant_seat_tiers (grant_id, seat_tier_id, seats) \
                     SELECT $1, id, 1 FROM seat_tiers WHERE code = 'standard' AND is_active = true")
            .bind(grant_id),
        )
            .await
            .expect("seed exactly one purchased slot");
        execute_financial_fixture(
            &pool,
            account_id,
            sqlx::query("UPDATE grants SET seats = 1 WHERE id = $1").bind(grant_id),
        ).await.unwrap();

        let reservation_id = crate::billing::slot_pool::reserve_slot(
            &pool,
            account_id,
            user_id,
            None,
            chrono::Duration::minutes(5),
        )
        .await
        .expect("reserve_slot must succeed")
        .expect("the pool has exactly one slot and nobody else is reserving it");

        let stripe = spawn_any_request_fails_mock().await;

        let result = acquire_seat(&pool, Some(&stripe), account_id, user_id).await;
        let purchased_after = member_seat_purchased_at(&pool, account_id, user_id).await;
        let ledger_after = ledger_seats(&pool, grant_id, "standard").await;
        let reservation_row: (Option<chrono::DateTime<chrono::Utc>>, Option<String>) = sqlx::query_as(
            "SELECT released_at, release_reason FROM slot_reservations WHERE id = $1",
        )
        .bind(reservation_id)
        .fetch_one(&pool)
        .await
        .expect("read the reservation row back");

        set_seat_tier_price(&pool, "standard", original_standard.as_deref()).await;
        cleanup(&pool, account_id, &users).await;

        result.expect("must succeed fulfilling the reservation without ever calling Stripe");
        assert!(purchased_after.is_some());
        assert_eq!(ledger_after, Some(1), "fulfilling a reservation must not touch the ledger");
        assert!(reservation_row.0.is_some(), "the reservation must be resolved");
        assert_eq!(reservation_row.1.as_deref(), Some("fulfilled"));
    }

    /// **CodeRabbit round 1 regression.** A reservation that has EXPIRED —
    /// even if `workers::slot_reservations`' sweep has not yet marked it
    /// `expired` — must never be silently fulfilled by `acquire_seat`'s
    /// claim: `expires_at > now()` is part of the SAME `SELECT` that finds
    /// the reservation in the first place, so a stale row is simply
    /// invisible here, not raced against a separate UPDATE. The member
    /// still ends up purchased, but via the ORDINARY pool-availability
    /// path (the account's one purchased slot is still `available()` — an
    /// expired, unswept reservation is excluded from `reserved` by
    /// `read_slot_pool`'s own `expires_at > now()` predicate too), not via
    /// the stale reservation — which is left untouched (`released_at`
    /// still `NULL`) for the sweep to reclaim on its own schedule.
    #[tokio::test]
    async fn acquire_seat_does_not_fulfill_an_expired_reservation() {
        let Some(pool) = pg_test_pool("acquire_seat_does_not_fulfill_an_expired_reservation").await
        else {
            return;
        };
        let _catalog_guard = CATALOG_PRICE_LOCK.lock().await;
        let (account_id, users, grant_id) = seed_team_account(&pool, &["standard"]).await;
        let user_id = users[0];
        let original_standard = set_seat_tier_price(&pool, "standard", None).await;
        execute_financial_fixture(
            &pool,
            account_id,
            sqlx::query("INSERT INTO grant_seat_tiers (grant_id, seat_tier_id, seats) \
                     SELECT $1, id, 1 FROM seat_tiers WHERE code = 'standard' AND is_active = true")
            .bind(grant_id),
        )
            .await
            .expect("seed exactly one purchased slot");
        execute_financial_fixture(
            &pool,
            account_id,
            sqlx::query("UPDATE grants SET seats = 1 WHERE id = $1").bind(grant_id),
        ).await.unwrap();

        let reservation_id = crate::billing::slot_pool::reserve_slot(
            &pool,
            account_id,
            user_id,
            None,
            chrono::Duration::minutes(5),
        )
        .await
        .expect("reserve_slot must succeed")
        .expect("the pool has exactly one slot and nobody else is reserving it");
        // Force it into the past directly — simulates the sweep genuinely
        // not having run yet, not a `release_reservation`/sweep call.
        sqlx::query("UPDATE slot_reservations SET expires_at = now() - interval '1 minute' WHERE id = $1")
            .bind(reservation_id)
            .execute(&pool)
            .await
            .expect("backdate the reservation past its own TTL");

        let stripe = spawn_any_request_fails_mock().await;

        let result = acquire_seat(&pool, Some(&stripe), account_id, user_id).await;
        let purchased_after = member_seat_purchased_at(&pool, account_id, user_id).await;
        let reservation_row: (Option<chrono::DateTime<chrono::Utc>>, Option<String>) = sqlx::query_as(
            "SELECT released_at, release_reason FROM slot_reservations WHERE id = $1",
        )
        .bind(reservation_id)
        .fetch_one(&pool)
        .await
        .expect("read the reservation row back");

        set_seat_tier_price(&pool, "standard", original_standard.as_deref()).await;
        cleanup(&pool, account_id, &users).await;

        result.expect("must still succeed — via the ordinary pool-availability path, not the stale reservation");
        assert!(purchased_after.is_some(), "the member still gets a slot, from the pool directly");
        assert!(
            reservation_row.0.is_none(),
            "an expired reservation must be left untouched by acquire_seat — released_at must stay NULL \
             for the sweep to reclaim it, not silently marked fulfilled"
        );
    }

    /// Snapshot the account-wide pool the way production reads it —
    /// `billing::slot_pool::read_slot_pool` inside an account-scoped
    /// transaction. Used by the CS-444 tests below, which are about the
    /// three numbers rather than about any one table.
    async fn pool_snapshot(pool: &PgPool, account_id: Uuid) -> crate::billing::slot_pool::SlotPool {
        let mut tx = crate::tenancy::scope::begin_account_scoped(pool, account_id)
            .await
            .expect("scope the account");
        let snapshot = crate::billing::slot_pool::read_slot_pool(&mut tx, account_id)
            .await
            .expect("read the slot pool");
        tx.commit().await.expect("commit the read");
        snapshot
    }

    /// **CS-444 — negative test 1 (unit level).** *Removing a member leaves
    /// `purchased` unchanged and Stripe un-called, while `available` rises by
    /// one.*
    ///
    /// **This test asserted the OPPOSITE until CS-444.** It was
    /// `release_seat_decrements_and_deletes_the_row_at_zero`, and it pinned
    /// the behaviour the founder's 2026-09-22 decision overturned: the first
    /// release decremented `grant_seat_tiers` 2 -> 1, the second deleted the
    /// row outright. That is precisely how an org that bought 10 slots up
    /// front used to drop to 9 the moment anyone left. Inverted rather than
    /// deleted, so the new rule is pinned exactly as hard as the old one was:
    /// **two members leave, and the ledger still reads 2 both times.**
    ///
    /// "Stripe un-called" is proven structurally, not by a mock:
    /// [`release_seat`] no longer TAKES a `StripeClient`, so there is nothing
    /// in scope for it to call. The offboarding-path version of this same
    /// assertion, which does still hold a (hit-counting) mock because its
    /// caller has one, is `clerk_provision::pg_tests::
    /// offboarding_frees_the_slot_without_calling_stripe`.
    #[tokio::test]
    async fn release_seat_frees_the_slot_and_leaves_purchased_untouched() {
        let Some(pool) =
            pg_test_pool("release_seat_frees_the_slot_and_leaves_purchased_untouched").await
        else {
            return;
        };
        let (account_id, users, grant_id) =
            seed_team_account(&pool, &["standard", "standard"]).await;
        // Both members already purchased, at standard:2 — a full pool.
        execute_financial_fixture(
            &pool,
            account_id,
            sqlx::query("INSERT INTO grant_seat_tiers (grant_id, seat_tier_id, seats) \
                     SELECT $1, id, 2 FROM seat_tiers WHERE code = 'standard' AND is_active = true")
            .bind(grant_id),
        )
            .await
            .expect("seed the purchased ledger");
        execute_financial_fixture(
            &pool,
            account_id,
            sqlx::query("UPDATE grants SET seats = 2 WHERE id = $1").bind(grant_id),
        ).await.unwrap();
        for u in &users {
            sqlx::query("UPDATE account_members SET seat_purchased_at = now() WHERE account_id = $1 AND user_id = $2")
                .bind(account_id)
                .bind(u)
                .execute(&pool)
                .await
                .expect("mark purchased");
        }

        let before = pool_snapshot(&pool, account_id).await;

        // First member leaves.
        let first_result = release_seat(&pool, account_id, users[0]).await;
        let purchased_after_first = member_seat_purchased_at(&pool, account_id, users[0]).await;
        let ledger_after_first = ledger_seats(&pool, grant_id, "standard").await;
        let grants_after_first = grants_seats(&pool, grant_id).await;
        let pool_after_first = pool_snapshot(&pool, account_id).await;

        // Second member leaves — the tier's LAST occupied slot. The row must
        // still NOT be deleted: the org bought two slots and still owns two.
        let second_result = release_seat(&pool, account_id, users[1]).await;
        let ledger_after_second = ledger_seats(&pool, grant_id, "standard").await;
        let pool_after_second = pool_snapshot(&pool, account_id).await;

        cleanup(&pool, account_id, &users).await;

        assert_eq!((before.purchased, before.used, before.available()), (2, 2, 0));

        assert!(
            first_result.expect("first release must succeed"),
            "item 4: this release genuinely freed a slot — `remove_org_membership` uses exactly \
             this boolean to decide whether to trigger the drain"
        );
        assert_eq!(
            purchased_after_first, None,
            "clearing seat_purchased_at is the whole of 'the slot is free'"
        );
        assert_eq!(
            ledger_after_first,
            Some(2),
            "purchased must NOT move when a member leaves — the org still owns the slot it bought"
        );
        assert_eq!(grants_after_first, 2, "grants.seats mirrors the untouched ledger");
        assert_eq!(
            (pool_after_first.purchased, pool_after_first.used, pool_after_first.available()),
            (2, 1, 1),
            "the departure must turn a used slot into an available one, not into a refund"
        );

        assert!(
            second_result.expect("second release must succeed"),
            "a DIFFERENT member's release also genuinely frees a slot — this is not the \
             double-release-of-the-SAME-member case"
        );
        assert_eq!(
            ledger_after_second,
            Some(2),
            "the tier row must survive its last occupant leaving — 'zero seats has no row' is about \
             zero PURCHASED slots, and this org purchased two"
        );
        assert_eq!(
            (pool_after_second.purchased, pool_after_second.used, pool_after_second.available()),
            (2, 0, 2),
            "an emptied org keeps the pool it paid for"
        );
    }

    /// **CS-444 — negative test 2.** *The next acquisition consumes that
    /// freed slot with **no** Stripe call.*
    ///
    /// The point of freeing a slot rather than un-purchasing it is that the
    /// NEXT person costs nothing. The fixture is three members against a
    /// two-slot pool: two seated (the pool is FULL, `available() == 0`) and
    /// one waiting.
    ///
    /// **The test proves its own premise before it proves its claim.** It
    /// first runs [`acquire_seat`] for the waiting member while the pool is
    /// still full and asserts it FAILS — the Stripe mock answers 500 to
    /// every request, and a full pool is exactly the condition under which
    /// `acquire_seat` falls through to a real purchase. Only then does a
    /// seated member leave; the same acquire, against the same 500-ing mock,
    /// must now SUCCEED. Without that first half, "the second acquire
    /// succeeded" would be equally consistent with a mock that was never
    /// reachable in the first place (a NULL `stripe_price_id`, say, which
    /// errors before the HTTP call) — the sort of passing-for-the-wrong-
    /// reason this repo has been bitten by. The tier's price is therefore
    /// set explicitly, under `CATALOG_PRICE_LOCK`, so the failing half fails
    /// AT STRIPE rather than short of it.
    #[tokio::test]
    async fn a_freed_slot_is_consumed_by_the_next_join_without_a_stripe_call() {
        let Some(pool) =
            pg_test_pool("a_freed_slot_is_consumed_by_the_next_join_without_a_stripe_call").await
        else {
            return;
        };
        let _catalog_guard = CATALOG_PRICE_LOCK.lock().await;
        let (account_id, users, grant_id) =
            seed_team_account(&pool, &["standard", "standard", "standard"]).await;
        let leaver = users[0];
        let joiner = users[2];
        let original_standard =
            set_seat_tier_price(&pool, "standard", Some("price_seat_lifecycle_freed_slot")).await;
        // purchased 2, seats 0 and 1 taken — a FULL pool, so the joiner can
        // only ever be seated out of what the leaver gives back.
        execute_financial_fixture(
            &pool,
            account_id,
            sqlx::query("INSERT INTO grant_seat_tiers (grant_id, seat_tier_id, seats) \
                     SELECT $1, id, 2 FROM seat_tiers WHERE code = 'standard' AND is_active = true")
            .bind(grant_id),
        )
            .await
            .expect("seed the purchased ledger");
        execute_financial_fixture(
            &pool,
            account_id,
            sqlx::query("UPDATE grants SET seats = 2 WHERE id = $1").bind(grant_id),
        ).await.unwrap();
        for u in &users[..2] {
            sqlx::query("UPDATE account_members SET seat_purchased_at = now() WHERE account_id = $1 AND user_id = $2")
                .bind(account_id)
                .bind(u)
                .execute(&pool)
                .await
                .expect("mark purchased");
        }

        // 500s on EVERY request: reaching Stripe at all surfaces as an Err.
        let stripe = spawn_any_request_fails_mock().await;

        let pool_when_full = pool_snapshot(&pool, account_id).await;
        // Premise: with no free slot, this acquire MUST try to buy one, and
        // the mock MUST break it.
        let acquire_while_full = acquire_seat(&pool, Some(&stripe), account_id, joiner).await;
        let joiner_stamp_while_full = member_seat_purchased_at(&pool, account_id, joiner).await;

        // A seated member leaves — one slot returns to the pool.
        let release_result = release_seat(&pool, account_id, leaver).await;
        let pool_after_release = pool_snapshot(&pool, account_id).await;

        // Claim: the same call, against the same broken Stripe, now works.
        let acquire_after_release = acquire_seat(&pool, Some(&stripe), account_id, joiner).await;
        let joiner_stamp = member_seat_purchased_at(&pool, account_id, joiner).await;
        let ledger_after = ledger_seats(&pool, grant_id, "standard").await;
        let pool_after_acquire = pool_snapshot(&pool, account_id).await;

        set_seat_tier_price(&pool, "standard", original_standard.as_deref()).await;
        cleanup(&pool, account_id, &users).await;

        assert_eq!(
            (pool_when_full.purchased, pool_when_full.used, pool_when_full.available()),
            (2, 2, 0),
            "the fixture must start with a genuinely full pool, or the premise below proves nothing"
        );
        assert!(
            matches!(acquire_while_full, Err(SeatLifecycleError::Stripe(_))),
            "premise: with no free slot, acquire must fall through to a Stripe purchase and break \
             AT STRIPE against the 500-ing mock. The variant is asserted, not merely `is_err()`: a \
             `Db` error here would mean the call died short of the HTTP request (an unprovisioned \
             price, a missing grant), which would make the claim below vacuous — it would be \
             proving that a call that never reaches Stripe does not reach Stripe. Got: \
             {acquire_while_full:?}"
        );
        assert_eq!(
            joiner_stamp_while_full, None,
            "a failed purchase must not seat anybody"
        );

        assert!(
            release_result.expect("the leaver's release must succeed"),
            "item 4: a genuine free"
        );
        assert_eq!(
            (pool_after_release.purchased, pool_after_release.used, pool_after_release.available()),
            (2, 1, 1),
            "the departure must leave exactly one free slot behind, with purchased unmoved"
        );

        acquire_after_release.expect(
            "the joiner must now be seated out of the freed slot — the Stripe mock is still the \
             same one that failed the identical call moments ago, so reaching it would fail here too",
        );
        assert!(joiner_stamp.is_some(), "the joiner must end up holding a purchased slot");
        assert_eq!(
            ledger_after,
            Some(2),
            "consuming a freed slot must not re-purchase it — the ledger stays at the two slots \
             the org actually bought"
        );
        assert_eq!(
            (pool_after_acquire.purchased, pool_after_acquire.used, pool_after_acquire.available()),
            (2, 2, 0),
            "the pool is full again, having been bought exactly once"
        );
    }

    /// **CS-444 — negative test 4 (sequential half).** *A double release
    /// cannot free the same slot twice.*
    ///
    /// Honest about which guard catches what: the SECOND call here is caught
    /// by [`release_seat`]'s earlier no-op guard (`seat_purchased_at IS NULL`
    /// already — nothing to give back), not by the `rows_affected() == 0`
    /// check further down, which no sequential test can reach while the
    /// advisory lock holds. See
    /// [`a_release_whose_stamp_was_cleared_underneath_it_is_a_clean_noop`]
    /// below for the one that does reach it. What this pins is the property
    /// both guards exist for, measured where it is observable: across two
    /// releases of ONE member, `available` rises by exactly one.
    #[tokio::test]
    async fn a_double_release_frees_the_slot_exactly_once() {
        let Some(pool) = pg_test_pool("a_double_release_frees_the_slot_exactly_once").await else {
            return;
        };
        let (account_id, users, grant_id) = seed_team_account(&pool, &["standard"]).await;
        let user_id = users[0];
        execute_financial_fixture(
            &pool,
            account_id,
            sqlx::query("INSERT INTO grant_seat_tiers (grant_id, seat_tier_id, seats) \
                     SELECT $1, id, 1 FROM seat_tiers WHERE code = 'standard' AND is_active = true")
            .bind(grant_id),
        )
            .await
            .expect("seed the purchased ledger");
        execute_financial_fixture(
            &pool,
            account_id,
            sqlx::query("UPDATE grants SET seats = 1 WHERE id = $1").bind(grant_id),
        ).await.unwrap();
        sqlx::query("UPDATE account_members SET seat_purchased_at = now() WHERE account_id = $1 AND user_id = $2")
            .bind(account_id)
            .bind(user_id)
            .execute(&pool)
            .await
            .expect("mark purchased");

        let first = release_seat(&pool, account_id, user_id).await;
        let after_first = pool_snapshot(&pool, account_id).await;
        // A redelivered `organizationMembership.deleted`.
        let second = release_seat(&pool, account_id, user_id).await;
        let after_second = pool_snapshot(&pool, account_id).await;
        let ledger_after = ledger_seats(&pool, grant_id, "standard").await;

        cleanup(&pool, account_id, &users).await;

        assert!(
            first.expect("first release must succeed"),
            "item 4: the first release genuinely frees the slot"
        );
        assert!(
            !second.expect("a redelivered release must also succeed, as a no-op"),
            "item 4: the redelivery frees NOTHING — this is exactly the case \
             `remove_org_membership` must not trigger a second drain for"
        );
        assert_eq!(
            (after_first.purchased, after_first.used, after_first.available()),
            (1, 0, 1)
        );
        assert_eq!(
            (after_second.purchased, after_second.used, after_second.available()),
            (1, 0, 1),
            "the second release must not hand the pool a slot that was never taken from it"
        );
        assert_eq!(ledger_after, Some(1), "purchased is untouched by either call");
    }

    /// **CS-444 — negative test 4 (the half that actually reaches the
    /// `rows_affected() == 0` branch).**
    ///
    /// The sequential double-release above never gets there: the earlier
    /// `seat_purchased_at IS NULL` guard short-circuits first. This one
    /// drives the real interleaving, deterministically, without a race:
    ///
    /// 1. an out-of-band transaction clears the stamp and **does not
    ///    commit** — under `READ COMMITTED` that write is invisible to
    ///    everyone else, but it holds the row lock;
    /// 2. [`release_seat`] starts, reads the member state, and therefore
    ///    still sees `seat_purchased_at` SET (so it passes the early guard);
    /// 3. its own guarded `UPDATE` blocks on that row lock;
    /// 4. the out-of-band transaction commits; `release_seat`'s `UPDATE`
    ///    re-evaluates its `AND seat_purchased_at IS NOT NULL` predicate
    ///    against the now-committed row and matches nothing —
    ///    `rows_affected() == 0`.
    ///
    /// Step 2 cannot lose the race the other way (an uncommitted write is
    /// not visible, so the read cannot see `NULL` early), and the sleep only
    /// ensures the task has reached step 3 before step 4. If it somehow had
    /// not, the assertions below still hold — this test can fail to exercise
    /// the branch, but it cannot fail spuriously.
    ///
    /// What it pins: that branch commits and returns `Ok` rather than
    /// erroring or rolling back (an offboarding must not start failing over
    /// a slot that is already free), and that it leaves the pool alone.
    #[tokio::test]
    async fn a_release_whose_stamp_was_cleared_underneath_it_is_a_clean_noop() {
        let Some(pool) =
            pg_test_pool("a_release_whose_stamp_was_cleared_underneath_it_is_a_clean_noop").await
        else {
            return;
        };
        let (account_id, users, grant_id) = seed_team_account(&pool, &["standard"]).await;
        let user_id = users[0];
        execute_financial_fixture(
            &pool,
            account_id,
            sqlx::query("INSERT INTO grant_seat_tiers (grant_id, seat_tier_id, seats) \
                     SELECT $1, id, 1 FROM seat_tiers WHERE code = 'standard' AND is_active = true")
            .bind(grant_id),
        )
            .await
            .expect("seed the purchased ledger");
        execute_financial_fixture(
            &pool,
            account_id,
            sqlx::query("UPDATE grants SET seats = 1 WHERE id = $1").bind(grant_id),
        ).await.unwrap();
        sqlx::query("UPDATE account_members SET seat_purchased_at = now() WHERE account_id = $1 AND user_id = $2")
            .bind(account_id)
            .bind(user_id)
            .execute(&pool)
            .await
            .expect("mark purchased");

        // (1) clear it, hold the row lock, do NOT commit.
        let mut blocker = pool.begin().await.expect("begin the out-of-band transaction");
        sqlx::query(
            "UPDATE account_members SET seat_purchased_at = NULL WHERE account_id = $1 AND user_id = $2",
        )
        .bind(account_id)
        .bind(user_id)
        .execute(&mut *blocker)
        .await
        .expect("clear the stamp out of band");

        // (2)/(3) release starts, reads the still-visible stamp, then blocks.
        let pool_for_task = pool.clone();
        let handle =
            tokio::spawn(async move { release_seat(&pool_for_task, account_id, user_id).await });
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;

        // (4) commit; the blocked UPDATE now matches zero rows.
        blocker.commit().await.expect("commit the out-of-band clear");
        let result = handle.await.expect("the release task must not panic");

        let after = pool_snapshot(&pool, account_id).await;
        let ledger_after = ledger_seats(&pool, grant_id, "standard").await;
        let stamp_after = member_seat_purchased_at(&pool, account_id, user_id).await;

        cleanup(&pool, account_id, &users).await;

        assert!(
            !result.expect(
                "a release that finds the slot already freed must commit and return Ok — offboarding \
                 swallows the error either way, but an Err here would log a failure every time Clerk \
                 redelivered",
            ),
            "item 4: this call froze nothing — the `rows_affected() == 0` branch — so it must \
             report `false`, the same as the double-release's second call"
        );
        assert_eq!(stamp_after, None);
        assert_eq!(ledger_after, Some(1), "purchased is untouched");
        assert_eq!(
            (after.purchased, after.used, after.available()),
            (1, 0, 1),
            "exactly one slot free — not two"
        );
    }

    /// A member whose seat was never purchased (`seat_purchased_at IS NULL`)
    /// releases as a pure no-op — they never held a slot, so there is none to
    /// give back, and the pool must not gain one.
    #[tokio::test]
    async fn release_seat_is_a_noop_when_never_purchased() {
        let Some(pool) = pg_test_pool("release_seat_is_a_noop_when_never_purchased").await else {
            return;
        };
        let (account_id, users, grant_id) = seed_team_account(&pool, &["standard"]).await;

        assert!(
            !release_seat(&pool, account_id, users[0])
                .await
                .expect("an unpurchased seat must release as a clean no-op"),
            "item 4: never held a purchased slot — nothing genuinely freed"
        );
        assert_eq!(member_seat_purchased_at(&pool, account_id, users[0]).await, None);
        assert_eq!(ledger_seats(&pool, grant_id, "standard").await, None);
        let after = pool_snapshot(&pool, account_id).await;
        assert_eq!(
            (after.purchased, after.used, after.available()),
            (0, 0, 0),
            "nothing was bought and nothing was held — a release cannot invent a slot"
        );

        cleanup(&pool, account_id, &users).await;
    }

    /// **CS-444 — inverts `release_seat_still_pushes_stripe_while_past_due`
    /// (finding 7a).** That test pinned "a `past_due` release must still push
    /// the Stripe decrement and decrement the ledger", which was the fix for
    /// a real bug: before it, a member removed while `past_due` had their
    /// release no-op ENTIRELY, and once their `account_members` row was
    /// deleted moments later nothing could ever read their `seat_tier_id`
    /// again — a permanently unreclaimable billed seat.
    ///
    /// CS-444 makes that bug unreachable rather than handled. The slot stays
    /// purchased on purpose and stays consumable by the next joiner, so
    /// there is nothing left to strand — and consequently a release is now
    /// identical for every subscription status. This test pins the new rule
    /// at the status that used to be special: **`past_due` frees the slot and
    /// touches neither Stripe nor the ledger.**
    #[tokio::test]
    async fn release_seat_frees_the_slot_while_past_due_without_touching_the_ledger() {
        let Some(pool) =
            pg_test_pool("release_seat_frees_the_slot_while_past_due_without_touching_the_ledger")
                .await
        else {
            return;
        };
        let (account_id, users, grant_id) = seed_team_account(&pool, &["standard"]).await;
        let user_id = users[0];
        execute_financial_fixture(
            &pool,
            account_id,
            sqlx::query("INSERT INTO grant_seat_tiers (grant_id, seat_tier_id, seats) \
                     SELECT $1, id, 1 FROM seat_tiers WHERE code = 'standard' AND is_active = true")
            .bind(grant_id),
        )
            .await
            .expect("seed the purchased ledger");
        execute_financial_fixture(
            &pool,
            account_id,
            sqlx::query("UPDATE grants SET seats = 1 WHERE id = $1").bind(grant_id),
        ).await.unwrap();
        sqlx::query("UPDATE account_members SET seat_purchased_at = now() WHERE account_id = $1 AND user_id = $2")
            .bind(account_id)
            .bind(user_id)
            .execute(&pool)
            .await
            .expect("mark purchased");
        execute_financial_fixture(
            &pool,
            account_id,
            sqlx::query("UPDATE stripe_subscriptions SET status = 'past_due' WHERE account_id = $1")
            .bind(account_id),
        )
            .await
            .expect("freeze the subscription as past_due");

        let result = release_seat(&pool, account_id, user_id).await;
        let purchased_after = member_seat_purchased_at(&pool, account_id, user_id).await;
        let ledger_after = ledger_seats(&pool, grant_id, "standard").await;
        let after = pool_snapshot(&pool, account_id).await;

        cleanup(&pool, account_id, &users).await;

        assert!(
            result.expect("release must succeed while past_due"),
            "item 4: a genuine free, regardless of subscription status"
        );
        assert_eq!(purchased_after, None);
        assert_eq!(
            ledger_after,
            Some(1),
            "the slot the org bought stays bought — a past_due org that loses a member keeps its \
             pool, exactly like an active one"
        );
        assert_eq!((after.purchased, after.used, after.available()), (1, 0, 1));
    }

    /// **CS-444 — inverts
    /// `release_seat_decrements_the_ledger_without_a_stripe_call_when_canceled`
    /// (finding 7a's other half).** That test pinned "a `canceled`
    /// subscription must decrement the LOCAL ledger directly, since Stripe's
    /// side is already zero". There is no ledger decrement any more, for any
    /// status: the slot is freed, `purchased` is left to the admin's own
    /// buy/reduce actions. Kept as a distinct test from the `past_due` one
    /// because `canceled` was the other arm of the branch this PR deleted,
    /// and "both arms now behave identically" is the claim worth pinning.
    #[tokio::test]
    async fn release_seat_frees_the_slot_when_canceled_without_touching_the_ledger() {
        let Some(pool) =
            pg_test_pool("release_seat_frees_the_slot_when_canceled_without_touching_the_ledger")
                .await
        else {
            return;
        };
        let (account_id, users, grant_id) = seed_team_account(&pool, &["standard"]).await;
        let user_id = users[0];
        execute_financial_fixture(
            &pool,
            account_id,
            sqlx::query("INSERT INTO grant_seat_tiers (grant_id, seat_tier_id, seats) \
                     SELECT $1, id, 1 FROM seat_tiers WHERE code = 'standard' AND is_active = true")
            .bind(grant_id),
        )
            .await
            .expect("seed the purchased ledger");
        execute_financial_fixture(
            &pool,
            account_id,
            sqlx::query("UPDATE grants SET seats = 1 WHERE id = $1").bind(grant_id),
        ).await.unwrap();
        sqlx::query("UPDATE account_members SET seat_purchased_at = now() WHERE account_id = $1 AND user_id = $2")
            .bind(account_id)
            .bind(user_id)
            .execute(&pool)
            .await
            .expect("mark purchased");
        execute_financial_fixture(
            &pool,
            account_id,
            sqlx::query("UPDATE stripe_subscriptions SET status = 'canceled' WHERE account_id = $1")
            .bind(account_id),
        )
            .await
            .expect("cancel the subscription");

        assert!(
            release_seat(&pool, account_id, user_id)
                .await
                .expect("release must succeed for a canceled subscription"),
            "item 4: a genuine free, regardless of subscription status"
        );

        assert_eq!(member_seat_purchased_at(&pool, account_id, user_id).await, None);
        assert_eq!(
            ledger_seats(&pool, grant_id, "standard").await,
            Some(1),
            "the ledger is no longer decremented for ANY status — `canceled` is not special any more"
        );
        let after = pool_snapshot(&pool, account_id).await;
        assert_eq!((after.purchased, after.used, after.available()), (1, 0, 1));

        cleanup(&pool, account_id, &users).await;
    }

    /// **CS-444 — inverts
    /// `release_seat_resolves_the_stripe_item_by_the_pending_effective_tier`
    /// (finding 4c / S1).** That test existed because a release had to
    /// resolve TWO different tier keys in one function — the Stripe item by
    /// `MemberSeatState::effective_tier_id` (`pending_seat_tier_id ??
    /// seat_tier_id`, since a pending downgrade moves Stripe immediately)
    /// and the ledger row by the lagging plain `seat_tier_id` — and mixing
    /// them up broke in one direction or the other. A release now resolves
    /// NEITHER: clearing `seat_purchased_at` is one member's one row,
    /// whatever tier they sit on, so that whole hazard has no foothold here.
    /// (It is not fixed globally — see [`release_seat`]'s own doc comment
    /// for where it still lives.)
    ///
    /// What replaces the old assertion is the CONSEQUENCE, which is worth
    /// pinning on its own: a member mid-downgrade who leaves gives their slot
    /// back to the tier that was actually BOUGHT (`premium`, the ledger's
    /// key), not to the tier they were heading for (`standard`). The
    /// `spawn_tracked_seat_mock` helper that made the old Stripe mutation
    /// observable is deleted with it — nothing calls Stripe here to observe.
    #[tokio::test]
    async fn a_pending_downgrade_member_leaves_their_slot_in_the_tier_that_was_bought() {
        let Some(pool) =
            pg_test_pool("a_pending_downgrade_member_leaves_their_slot_in_the_tier_that_was_bought")
                .await
        else {
            return;
        };
        let (account_id, users, grant_id) = seed_team_account(&pool, &["premium"]).await;
        let user_id = users[0];
        // The ledger shows this member under PREMIUM — a pending downgrade
        // never touches `grant_seat_tiers` before roll.
        execute_financial_fixture(
            &pool,
            account_id,
            sqlx::query("INSERT INTO grant_seat_tiers (grant_id, seat_tier_id, seats) \
                     SELECT $1, id, 1 FROM seat_tiers WHERE code = 'premium' AND is_active = true")
            .bind(grant_id),
        )
            .await
            .expect("seed the purchased ledger at premium");
        execute_financial_fixture(
            &pool,
            account_id,
            sqlx::query("UPDATE grants SET seats = 1 WHERE id = $1").bind(grant_id),
        ).await.unwrap();
        let standard_id: Uuid =
            sqlx::query_scalar("SELECT id FROM seat_tiers WHERE code = 'standard' AND is_active = true")
                .fetch_one(&pool)
                .await
                .expect("standard seat tier");
        sqlx::query(
            "UPDATE account_members SET seat_purchased_at = now(), pending_seat_tier_id = $3 \
              WHERE account_id = $1 AND user_id = $2",
        )
        .bind(account_id)
        .bind(user_id)
        .bind(standard_id)
        .execute(&pool)
        .await
        .expect("mark purchased with a pending downgrade already pushed to Stripe");

        let result = release_seat(&pool, account_id, user_id).await;
        let purchased_after = member_seat_purchased_at(&pool, account_id, user_id).await;
        let premium_after = ledger_seats(&pool, grant_id, "premium").await;
        let standard_after = ledger_seats(&pool, grant_id, "standard").await;

        cleanup(&pool, account_id, &users).await;

        result.expect("release must succeed regardless of a pending tier change");
        assert_eq!(purchased_after, None);
        assert_eq!(
            premium_after,
            Some(1),
            "the freed slot stays in the tier that was actually bought — the ledger key is the \
             plain, lagging seat_tier_id, and a release no longer moves it at all"
        );
        assert_eq!(
            standard_after, None,
            "nothing is minted in the tier the member was merely heading FOR"
        );
    }

    /// **CS-435 (D1/D7/D9).** The pre-CS-435 version of this test was named
    /// `release_overhang_reclaims_an_orphaned_seat` and asserted the OPPOSITE
    /// of what this asserts now — see `release_overhang`'s own doc comment
    /// for why: what this fixture builds (`ledger_seats = 2`, one attached
    /// purchased member) is now `billing::slot_pool::SlotPool::available() ==
    /// 1` — a healthy pool with one unconsumed slot, indistinguishable from
    /// the old "orphaned seat" scenario by the signal this sweep used to
    /// read. Proves BOTH halves of the retirement: the ledger is untouched
    /// (`grant_seat_tiers` stays at 2, not reclaimed to 1) AND Stripe is
    /// never called at all — the mock 500s on every request, so a passing
    /// test proves zero calls, not merely "the right calls."
    #[tokio::test]
    async fn release_overhang_does_not_reclaim_healthy_pool_headroom() {
        let Some(pool) = pg_test_pool("release_overhang_does_not_reclaim_healthy_pool_headroom").await
        else {
            return;
        };
        let _catalog_guard = CATALOG_PRICE_LOCK.lock().await;
        let (account_id, users, grant_id) = seed_team_account(&pool, &["standard"]).await;
        let user_id = users[0];
        let original_standard =
            set_seat_tier_price(&pool, "standard", Some("price_seat_lifecycle_overhang")).await;
        // An admin bought 2 standard slots (`routes::org_slots`); only one
        // member has joined and consumed one. This is `available() == 1` —
        // headroom, not an orphan.
        execute_financial_fixture(
            &pool,
            account_id,
            sqlx::query("INSERT INTO grant_seat_tiers (grant_id, seat_tier_id, seats) \
                     SELECT $1, id, 2 FROM seat_tiers WHERE code = 'standard' AND is_active = true")
            .bind(grant_id),
        )
            .await
            .expect("seed the purchased ledger at 2 (one consumed, one headroom)");
        execute_financial_fixture(
            &pool,
            account_id,
            sqlx::query("UPDATE grants SET seats = 2 WHERE id = $1").bind(grant_id),
        ).await.unwrap();
        sqlx::query("UPDATE account_members SET seat_purchased_at = now() WHERE account_id = $1 AND user_id = $2")
            .bind(account_id)
            .bind(user_id)
            .execute(&pool)
            .await
            .expect("mark the one joined member purchased");

        // Any request at all — retrieve OR mutate — fails the test loudly.
        // A pre-CS-435 caller would have hit `GET /v1/subscriptions/*` at
        // minimum just to compute the (now-irrelevant) surplus.
        let app = axum::Router::new()
            .fallback(axum::routing::any(|| async { axum::http::StatusCode::INTERNAL_SERVER_ERROR }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind mock");
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.ok();
        });
        let stripe =
            StripeClient::new(reqwest::Client::new(), "sk_test_mock").with_base_url(format!("http://{addr}"));

        let result = release_overhang(&pool, Some(&stripe), account_id).await;
        let ledger_after = ledger_seats(&pool, grant_id, "standard").await;
        let grants_after = grants_seats(&pool, grant_id).await;

        set_seat_tier_price(&pool, "standard", original_standard.as_deref()).await;
        cleanup(&pool, account_id, &users).await;

        result.expect("release_overhang must always succeed — it is a documented no-op now");
        assert_eq!(
            ledger_after,
            Some(2),
            "the pool's unconsumed headroom must survive untouched — reclaiming it would cancel \
             slots an admin already bought"
        );
        assert_eq!(grants_after, 2);
    }

    /// The (now-unconditional) no-op holds even for the pre-CS-435 "balanced"
    /// fixture (ledger and roster already agree, `available() == 0`) — no
    /// Stripe call, nothing changed. A narrower case of the property the
    /// test above already proves for a NON-zero-headroom pool; kept
    /// separately because it was here before CS-435 and remains true.
    #[tokio::test]
    async fn release_overhang_is_a_noop_when_ledger_and_roster_agree() {
        let Some(pool) =
            pg_test_pool("release_overhang_is_a_noop_when_ledger_and_roster_agree").await
        else {
            return;
        };
        let (account_id, users, grant_id) = seed_team_account(&pool, &["standard"]).await;
        let user_id = users[0];
        execute_financial_fixture(
            &pool,
            account_id,
            sqlx::query("INSERT INTO grant_seat_tiers (grant_id, seat_tier_id, seats) \
                     SELECT $1, id, 1 FROM seat_tiers WHERE code = 'standard' AND is_active = true")
            .bind(grant_id),
        )
            .await
            .expect("seed the purchased ledger");
        execute_financial_fixture(
            &pool,
            account_id,
            sqlx::query("UPDATE grants SET seats = 1 WHERE id = $1").bind(grant_id),
        ).await.unwrap();
        sqlx::query("UPDATE account_members SET seat_purchased_at = now() WHERE account_id = $1 AND user_id = $2")
            .bind(account_id)
            .bind(user_id)
            .execute(&pool)
            .await
            .expect("mark purchased");

        // A mock that 500s on ANY request — proves no Stripe call happens.
        let app = axum::Router::new()
            .fallback(axum::routing::any(|| async { axum::http::StatusCode::INTERNAL_SERVER_ERROR }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.ok();
        });
        let stripe =
            StripeClient::new(reqwest::Client::new(), "sk_test_mock").with_base_url(format!("http://{addr}"));

        release_overhang(&pool, Some(&stripe), account_id)
            .await
            .expect("a balanced ledger must be a clean no-op");

        assert_eq!(ledger_seats(&pool, grant_id, "standard").await, Some(1));
        assert_eq!(grants_seats(&pool, grant_id).await, 1);

        cleanup(&pool, account_id, &users).await;
    }

    // ── SeatPurchasePolicy::PoolOnly (PR #1515 review) ──────────────────────
    //
    // Run as `coralswarm_app` (a real login), the role prod connects as, so
    // the pool read and the refusal are proven under RLS, not as the owner.

    async fn app_pool_for(owner: &PgPool) -> PgPool {
        let url = std::env::var("RELINFER_TEST_DATABASE_URL").expect("checked by pg_test_pool");
        crate::app_role_test_support::app_role_pool(owner, &url).await
    }

    /// A Stripe stand-in that fails every request and counts them, so a test
    /// can assert "no Stripe call" directly instead of inferring it.
    async fn spawn_counting_fail_mock() -> (StripeClient, std::sync::Arc<std::sync::atomic::AtomicUsize>) {
        let hits = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counter = hits.clone();
        let app = axum::Router::new().fallback(axum::routing::any(move || {
            let counter = counter.clone();
            async move {
                counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                axum::http::StatusCode::INTERNAL_SERVER_ERROR
            }
        }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind mock");
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.ok();
        });
        (
            StripeClient::new(reqwest::Client::new(), "sk_test_mock").with_base_url(format!("http://{addr}")),
            hits,
        )
    }

    /// `PoolOnly` with nothing purchased for the member's tier: refused with
    /// `NoFreeSeat`, no Stripe call, no stamp, no ledger row, `grants.seats`
    /// unchanged. The live subscription is what would otherwise send this
    /// straight to a purchase.
    #[tokio::test]
    async fn acquire_seat_pool_only_refuses_an_empty_pool_without_buying() {
        let Some(owner) = pg_test_pool("acquire_seat_pool_only_refuses_an_empty_pool_without_buying").await else {
            return;
        };
        let app = app_pool_for(&owner).await;
        let (account_id, users, grant_id) = seed_team_account(&owner, &["standard"]).await;
        let user_id = users[0];
        let (stripe, hits) = spawn_counting_fail_mock().await;

        let needs = new_member_needs_purchase(&app, account_id, member_tier(&owner, account_id, user_id).await).await;
        let result =
            acquire_seat_with_policy(&app, Some(&stripe), account_id, user_id, SeatPurchasePolicy::PoolOnly).await;
        let purchased_after = member_seat_purchased_at(&owner, account_id, user_id).await;
        let ledger_after = ledger_seats(&owner, grant_id, "standard").await;
        let grants_after = grants_seats(&owner, grant_id).await;
        cleanup(&owner, account_id, &users).await;

        assert!(needs.expect("pre-check reads under the app role"), "an empty pool needs a purchase");
        assert!(
            matches!(result, Err(SeatLifecycleError::NoFreeSeat)),
            "PoolOnly must refuse an empty pool, got {result:?}"
        );
        assert_eq!(hits.load(std::sync::atomic::Ordering::SeqCst), 0, "no Stripe call");
        assert!(purchased_after.is_none(), "no seat stamped");
        assert_eq!(ledger_after, None, "no ledger row written");
        assert_eq!(grants_after, 1, "grants.seats unchanged");
    }

    /// `PoolOnly` with a free purchased slot behaves exactly like the
    /// default: the member takes the slot, no Stripe call, ledger unchanged.
    #[tokio::test]
    async fn acquire_seat_pool_only_takes_a_free_slot() {
        let Some(owner) = pg_test_pool("acquire_seat_pool_only_takes_a_free_slot").await else {
            return;
        };
        let app = app_pool_for(&owner).await;
        let (account_id, users, grant_id) = seed_team_account(&owner, &["standard"]).await;
        let user_id = users[0];
        execute_financial_fixture(
            &owner,
            account_id,
            sqlx::query(
                "INSERT INTO grant_seat_tiers (grant_id, seat_tier_id, seats) \
                 SELECT $1, id, 2 FROM seat_tiers WHERE code = 'standard' AND is_active = true",
            )
            .bind(grant_id),
        )
        .await
        .expect("seed a pre-bought pool of 2");
        let (stripe, hits) = spawn_counting_fail_mock().await;

        let needs = new_member_needs_purchase(&app, account_id, member_tier(&owner, account_id, user_id).await).await;
        let result =
            acquire_seat_with_policy(&app, Some(&stripe), account_id, user_id, SeatPurchasePolicy::PoolOnly).await;
        let purchased_after = member_seat_purchased_at(&owner, account_id, user_id).await;
        let ledger_after = ledger_seats(&owner, grant_id, "standard").await;
        cleanup(&owner, account_id, &users).await;

        assert!(!needs.expect("pre-check reads under the app role"), "a free slot needs no purchase");
        result.expect("PoolOnly must take a free slot");
        assert_eq!(hits.load(std::sync::atomic::Ordering::SeqCst), 0, "no Stripe call");
        assert!(purchased_after.is_some(), "the member is stamped");
        assert_eq!(ledger_after, Some(2), "the ledger is unchanged");
    }

    /// No live subscription: `PoolOnly` is a no-op success like the default
    /// (nothing is billed, so nothing could be bought), and the pre-check
    /// agrees.
    #[tokio::test]
    async fn acquire_seat_pool_only_is_a_noop_without_a_live_subscription() {
        let Some(owner) =
            pg_test_pool("acquire_seat_pool_only_is_a_noop_without_a_live_subscription").await
        else {
            return;
        };
        let app = app_pool_for(&owner).await;
        let (account_id, users, _grant_id) = seed_team_account(&owner, &["standard"]).await;
        let user_id = users[0];
        execute_financial_fixture(
            &owner,
            account_id,
            sqlx::query("DELETE FROM stripe_subscriptions WHERE account_id = $1").bind(account_id),
        )
        .await
        .expect("unlink the subscription");
        let (stripe, hits) = spawn_counting_fail_mock().await;

        let needs = new_member_needs_purchase(&app, account_id, member_tier(&owner, account_id, user_id).await).await;
        let result =
            acquire_seat_with_policy(&app, Some(&stripe), account_id, user_id, SeatPurchasePolicy::PoolOnly).await;
        cleanup(&owner, account_id, &users).await;

        assert!(!needs.expect("pre-check"), "no subscription, nothing to buy");
        result.expect("no live subscription is a no-op, not a refusal");
        assert_eq!(hits.load(std::sync::atomic::Ordering::SeqCst), 0);
    }

    async fn member_tier(pool: &PgPool, account_id: Uuid, user_id: Uuid) -> Uuid {
        sqlx::query_scalar("SELECT seat_tier_id FROM account_members WHERE account_id = $1 AND user_id = $2")
            .bind(account_id)
            .bind(user_id)
            .fetch_one(pool)
            .await
            .expect("member tier")
    }
}
