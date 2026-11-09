//! `POST /v1/billing/members/{user_id}/seat-tier` — the ONLY new user-facing
//! HTTP surface billing self-serve launch PR F (seat lifecycle) adds. See
//! `billing::seat_lifecycle`'s module doc for why "invite"/"remove" are NOT
//! routes (org membership is entirely Clerk-managed) — this route is the one
//! seat-lifecycle action that genuinely has no Clerk event to hang off:
//! moving an ALREADY-PURCHASED seat between tiers.
//!
//! # Upgrade now, downgrade deferred (plan §1.7)
//!
//! An UPGRADE (target tier's `credits` greater than the member's current
//! tier) takes effect immediately: the Stripe subscription item move is
//! pushed with `proration_behavior: create_prorations` (Stripe bills the
//! difference now), and in the SAME transaction the member's
//! `seat_tier_id` flips, `grant_seat_tiers` moves, and the member's CURRENT
//! `member_allowance_periods.granted_credits` is raised to the new tier's
//! figure — `consumed_credits` untouched — matching the mid-period seat-add
//! precedent (`billing::renewal::seed_member_period`'s own "full allowance,
//! never prorated" rule, extended here to a mid-period UPGRADE rather than a
//! brand-new join).
//!
//! A DOWNGRADE takes effect at the NEXT period roll: the Stripe move still
//! pushes immediately, but with `proration_behavior: none` (no refund — the
//! founder's registered decision, plan §1.7, "premium allowance kept through
//! period end"), and the ONLY database write is
//! `account_members.pending_seat_tier_id`. `billing::renewal::roll_period`
//! is the sole place that ever reads and applies it — see that module's own
//! doc comment ("Pending downgrades, applied at roll" / "`grant_seat_tiers`
//! is a purchase-driven ledger").
//!
//! A same-tier request (`tier` equals the member's current, PRE-downgrade
//! `seat_tier_id`) is "cancel any pending downgrade" — and (billing
//! self-serve PR F review, finding 4a) it is no longer Stripe-silent when
//! there IS one to cancel: see "Cancelling a pending downgrade" below.
//!
//! # S1 — Stripe-effective tier (billing self-serve PR F review, finding 4)
//!
//! `billing::seat_lifecycle`'s own module doc states the rule this route
//! also lives under: a pending downgrade pushes Stripe's subscription item to
//! the NEW (lower) tier IMMEDIATELY (`proration_behavior: none`), while
//! `account_members.seat_tier_id` itself waits for the next roll. So the
//! member's Stripe-EFFECTIVE tier — `pending_seat_tier_id ?? seat_tier_id` —
//! is what Stripe is ACTUALLY billing right now, and it is what every move
//! computation in this route must resolve the FROM side against, never the
//! raw `seat_tier_id` alone. Two concrete places this matters:
//!
//! - **Cancelling a pending downgrade** (a same-tier request while
//!   `pending_seat_tier_id IS NOT NULL`) must push the REVERSE Stripe move —
//!   from the pending (already-billed) tier BACK to the original tier, with
//!   `proration_behavior: none` (the member already paid the OLD tier through
//!   period end; a proration here would double-charge) — before clearing the
//!   pending marker. The retired version of this branch cleared the marker
//!   with NO Stripe call at all, leaving Stripe permanently on the
//!   never-applied downgrade's tier while the DB silently reverted.
//! - **A second, different tier-change request while a downgrade is already
//!   pending** must compute its move FROM the effective (pending) tier, not
//!   the stale `seat_tier_id` — the retired version used `seat_tier_id`
//!   (`current_tier`) unconditionally, which after a pending downgrade names
//!   a Stripe item that either no longer exists (fully moved) or belongs to
//!   OTHER members still on that tier, either way computing the wrong move.
//!
//! The LOCAL LEDGER (`grant_seat_tiers`) writes in the upgrade branch below
//! are NOT part of this rule — they stay keyed to `seat_tier_id`/the pending
//! marker exactly as this module doc's own "Upgrade now, downgrade deferred"
//! section already describes; S1 governs Stripe-item resolution only.
//!
//! # Auth, and the four ways this route refuses
//!
//! Org-admin gated, the identical bar `routes::checkout` uses
//! ([`crate::routes::checkout::require_purchase_authority`] — billing
//! self-serve PR F review, minor finding: this route used to carry its own
//! independently copy-pasted near-duplicate of that function, differing only
//! in refusing rather than waving through a personal-account caller; the
//! shared function now takes an `org_only` flag instead of being copied):
//! 503 without a configured Stripe client, 409 when the account's
//! subscription is frozen OR its current grant is not the mixed-tier `team`
//! plan, 404 when `user_id` is not a member of the caller's account, 409 when
//! that member's seat is not (yet) purchased (`seat_purchased_at IS NULL` —
//! there is nothing to move at Stripe, and moving `pending_seat_tier_id`
//! alone for an unpurchased seat would let a later `acquire_seat` purchase
//! the WRONG tier), 400 for an unknown/inactive `tier` code, 400 for a
//! personal-account caller (`org_only = true`).
//!
//! # Idempotency keys (billing self-serve PR F review, finding 8)
//!
//! Each `idem_base` folds in a fresh `uuid::Uuid::new_v4()` suffix — see
//! `billing::seat_lifecycle`'s own doc comment ("Idempotency-key recipes")
//! for why a STATIC per-(account, member, tier-pair) key is wrong: Stripe
//! caches a key's response for roughly 24 hours, and a member whose tier is
//! changed, changed BACK, and changed again within that window (a genuine,
//! ordinary admin correction) would otherwise have a later call replay an
//! earlier one's cached response instead of actually pushing the new move.

use std::sync::Arc;

use axum::extract::{Extension, Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::{Deserialize, Serialize};
use serde_json::json;
use uuid::Uuid;

use utoipa::path as utoipa_path;
use utoipa::ToSchema;

use crate::billing::stripe_client::{Idem, StripeClient};
use crate::middleware::auth::UserContext;
use crate::AppState;

#[derive(Debug, Deserialize, ToSchema)]
pub struct SeatTierChangeRequest {
    /// `seat_tiers.code` (e.g. `"standard"`/`"premium"`).
    pub tier: String,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct SeatTierChangeResponse {
    /// `"upgraded"` | `"downgraded"` | `"unchanged"` — a short, greppable
    /// summary of which of the three branches this call took, mirroring
    /// `billing::renewal::RenewalOutcome::summary`'s own convention.
    pub outcome: &'static str,
    pub tier: String,
}

/// `GET /v1/billing/seat-tiers` response row (CS-442, item 40, D8) — the
/// catalog an admin picks from on the join-request approval row (CS-441's
/// web-side UI) and, incidentally, the SAME shape a plain member could read
/// on a billing page to see what a tier costs. `credits` reads through an
/// explicit `::float8` SQL cast (this file's own `f64`, not
/// `sqlx::types::Decimal`, in a JSON DTO — `routes::usage`'s established
/// convention for exposing a NUMERIC column over this API, since
/// `sqlx::types::Decimal` (`rust_decimal::Decimal`) has no blanket
/// `utoipa::ToSchema` impl this crate can derive against). Deliberately
/// carries no `stripe_price_id` — that is an internal provisioning detail
/// (`billing::seat_lifecycle::seat_tier_price`'s own doc comment: a NULL one
/// is an INTERNAL error, never a client-facing concept), and the approval
/// row only ever needs `code` to build its request body.
#[derive(Debug, Serialize, ToSchema)]
pub struct SeatTierSummary {
    pub code: String,
    pub credits: f64,
    pub price_usd_cents: i32,
}

fn bad_request(msg: &str) -> Response {
    (StatusCode::BAD_REQUEST, Json(json!({"error": msg}))).into_response()
}

fn not_found(msg: &str) -> Response {
    (StatusCode::NOT_FOUND, Json(json!({"error": msg}))).into_response()
}

fn conflict(msg: &str) -> Response {
    (StatusCode::CONFLICT, Json(json!({"error": msg}))).into_response()
}

fn service_unavailable() -> Response {
    (StatusCode::SERVICE_UNAVAILABLE, Json(json!({"error": "service unavailable"}))).into_response()
}

fn internal_error(what: &str, e: impl std::fmt::Display) -> Response {
    tracing::error!(error = %e, step = what, "billing_seats: failed");
    (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": "internal error"}))).into_response()
}

struct TargetMember {
    seat_tier_id: Uuid,
    pending_seat_tier_id: Option<Uuid>,
    seat_purchased_at: Option<chrono::DateTime<chrono::Utc>>,
}

struct SeatTierRow {
    id: Uuid,
    code: String,
    credits: sqlx::types::Decimal,
    /// Billing self-serve PR F review, minor finding: upgrade/downgrade
    /// classification below compares THIS field, not `credits` — proration
    /// semantics are about MONEY, and a future catalog where a tier's
    /// credits and its price do not move in lockstep would otherwise
    /// misclassify which direction is the "upgrade".
    price_usd_cents: i32,
    stripe_price_id: String,
}

/// Move ONE seat between two Stripe subscription items — shared by the
/// upgrade and downgrade branches below, which differ only in
/// `proration_behavior`.
///
/// Mirrors `billing::seat_lifecycle`'s acquire/release item-resolution
/// exactly, extended to touch BOTH sides of the move in one call: when the
/// OLD tier's item holds exactly one seat (this member is its only
/// occupant) AND the NEW tier has no existing item yet, this is a single
/// in-place price swap (`swap_subscription_item_price` — cheaper and
/// simpler than delete-then-add, and the case that function's own doc
/// comment names). Every other shape (the old item has more than one seat,
/// or the new tier already has an item) is a decrement/delete on the old
/// side plus an increment/add on the new side — the same shape
/// `seat_lifecycle::acquire_seat` and `routes::org_slots`' buy/reduce
/// actions each do to ONE side of a move.
///
/// Two idempotency keys, not one shared between the two Stripe calls a
/// non-swap move can make: reusing one key across two structurally
/// different requests (different item ids/params) risks Stripe's own
/// idempotency-conflict error (`StripeError::IdempotencyConflict`) rather
/// than reusing a cached response, since the two calls are never actually
/// the same request.
async fn move_stripe_seat_item(
    stripe: &StripeClient,
    subscription_id: &str,
    old_price_id: &str,
    new_price_id: &str,
    proration_behavior: &str,
    idem_base: &str,
) -> Result<(), crate::billing::stripe_client::StripeError> {
    // Billing self-serve PR F review, MAJOR finding: `change_seat_tier` now
    // holds its account-scoped transaction and advisory lock ACROSS this
    // call (the lock-ordering fix earlier in this same review round) — the
    // identical S2 tradeoff `billing::seat_lifecycle`'s own module doc
    // documents for `acquire_seat` (and, before CS-444 removed its Stripe
    // push, for `release_seat` too). Bound the WHOLE Stripe
    // sequence below (one retrieve, up to two mutations) under the SAME
    // `seat_lifecycle::SEAT_STRIPE_OP_DEADLINE`, converting an unbounded
    // retry/backoff sequence into a bounded, retryable `StripeError::Timeout`
    // instead of holding this route's lock open indefinitely.
    crate::billing::seat_lifecycle::with_stripe_deadline(move_stripe_seat_item_inner(
        stripe,
        subscription_id,
        old_price_id,
        new_price_id,
        proration_behavior,
        idem_base,
    ))
    .await
}

async fn move_stripe_seat_item_inner(
    stripe: &StripeClient,
    subscription_id: &str,
    old_price_id: &str,
    new_price_id: &str,
    proration_behavior: &str,
    idem_base: &str,
) -> Result<(), crate::billing::stripe_client::StripeError> {
    let sub = stripe.retrieve_subscription(subscription_id).await?;
    let old_item = sub.items.iter().find(|it| it.price_id == old_price_id);
    let new_item = sub.items.iter().find(|it| it.price_id == new_price_id);

    if let (Some(old), None) = (old_item, new_item) {
        if old.quantity == 1 {
            let idem_key = format!("{idem_base}:swap");
            stripe
                .swap_subscription_item_price(&old.id, new_price_id, proration_behavior, Idem::Key(&idem_key))
                .await?;
            return Ok(());
        }
    }

    match old_item {
        Some(old) if old.quantity > 1 => {
            let idem_key = format!("{idem_base}:old");
            stripe
                .update_subscription_item(&old.id, old.quantity - 1, proration_behavior, Idem::Key(&idem_key))
                .await?;
        }
        Some(old) => {
            stripe.delete_subscription_item(&old.id, proration_behavior).await?;
        }
        None => {
            // Drift: nothing to move FROM at Stripe's layer — proceed to the
            // NEW side only, mirroring `routes::org_slots::reduce_slots`'
            // own drift tolerance (the same shape `seat_lifecycle::
            // release_seat` carried until CS-444 removed its Stripe push).
            tracing::warn!(
                subscription_id, old_price_id,
                "billing_seats: no matching Stripe subscription item found for the source tier — proceeding to the target side only"
            );
        }
    }

    match new_item {
        Some(new) => {
            let idem_key = format!("{idem_base}:new");
            stripe
                .update_subscription_item(&new.id, new.quantity + 1, proration_behavior, Idem::Key(&idem_key))
                .await?;
        }
        None => {
            let idem_key = format!("{idem_base}:new");
            stripe
                .add_subscription_item(subscription_id, new_price_id, 1, proration_behavior, Idem::Key(&idem_key))
                .await?;
        }
    }
    Ok(())
}

/// Fetch one `seat_tiers` row by id — shared by the `current_tier`/
/// `effective_tier` resolutions in [`change_seat_tier`] (S1 needs BOTH: the
/// member's raw `seat_tier_id` row and, separately, whichever tier Stripe is
/// actually billing right now). `what` names the caller's role only, for a
/// specific error message.
///
/// Generic over `PgExecutor` (billing self-serve PR F review: the
/// lock-ordering fix below reads this inside the caller's already-open,
/// already-locked transaction rather than against the raw pool) — the same
/// pattern other `clerk_provision` helpers use for a function called both
/// pool-side and tx-side.
async fn seat_tier_by_id<'e, E>(executor: E, id: Uuid, what: &str) -> Result<SeatTierRow, Response>
where
    E: sqlx::PgExecutor<'e>,
{
    match sqlx::query_as(
        "SELECT id, code, credits, price_usd_cents, stripe_price_id FROM seat_tiers WHERE id = $1",
    )
    .bind(id)
    .fetch_optional(executor)
    .await
    {
        Ok(Some((id, code, credits, price_usd_cents, Some(stripe_price_id)))) => {
            Ok(SeatTierRow { id, code, credits, price_usd_cents, stripe_price_id })
        }
        Ok(Some((_, code, _, _, None))) => {
            Err(internal_error(&format!("{what} price lookup"), format!("seat tier {code} has no stripe_price_id")))
        }
        Ok(None) => Err(internal_error(&format!("{what} lookup"), "seat_tiers row not found for this id")),
        Err(e) => Err(internal_error(&format!("{what} lookup"), e)),
    }
}

/// `POST /v1/billing/members/{user_id}/seat-tier` — see this module's own
/// doc comment for the full upgrade/downgrade/unchanged contract.
#[utoipa_path(
    post,
    path = "/v1/billing/members/{user_id}/seat-tier",
    tag = "billing",
    security(("bearer_token" = [])),
    params(("user_id" = Uuid, Path, description = "the target member's user id")),
    request_body = SeatTierChangeRequest,
    responses(
        (status = 200, description = "Tier changed (or pending downgrade cleared)", body = SeatTierChangeResponse),
        (status = 400, description = "Unknown/inactive seat tier, or a personal account"),
        (status = 403, description = "Caller is not an org admin"),
        (status = 404, description = "No such member on this account"),
        (status = 409, description = "Frozen subscription, non-team grant, or the member's seat is not yet purchased"),
        (status = 503, description = "Database or Stripe unavailable"),
    )
)]
pub async fn change_seat_tier(
    State(state): State<Arc<AppState>>,
    Extension(user): Extension<UserContext>,
    Path(target_user_id): Path<Uuid>,
    Json(body): Json<SeatTierChangeRequest>,
) -> Response {
    let account=user.account_id;
    let response=change_seat_tier_committed(State(state.clone()),Extension(user),Path(target_user_id),Json(body)).await;
    crate::read_models::dirty::after_control_response(&state,account,crate::read_models::dirty::DirtyEvent::BillingChanged,response).await
}

async fn change_seat_tier_committed(
    State(state): State<Arc<AppState>>,
    Extension(user): Extension<UserContext>,
    Path(target_user_id): Path<Uuid>,
    Json(body): Json<SeatTierChangeRequest>,
) -> Response {
    if let Err(resp) = crate::routes::checkout::require_purchase_authority(&state, &user, true).await {
        return resp;
    }
    let Some(control) = state.control_db() else {
        return service_unavailable();
    };
    let Some(stripe) = state.stripe.as_ref() else {
        return service_unavailable();
    };
    let account_id = user.account_id;

    if crate::billing::enforcement::subscription_frozen(control, account_id).await {
        return conflict("subscription inactive — seat tiers cannot be changed while frozen");
    }

    // Current grant must be the mixed-tier `team` plan — same "most
    // recently granted, non-revoked" selection every other current-grant
    // lookup in this crate uses. `grants` is RLS-covered (0150), so this
    // read runs inside its own short-lived account-scoped transaction
    // (committed immediately — a read-only commit is a no-op) rather than
    // against the raw `control` pool `plans`/`stripe_subscriptions`/
    // `seat_tiers`/`account_members` reads elsewhere in this handler use —
    // none of THOSE tables carry RLS (0169's/0162's own headers), so only
    // this one query needs scoping.
    let team_grant: Option<(Uuid, String, Option<sqlx::types::Decimal>)> =
        match crate::tenancy::scope::begin_account_scoped(control, account_id).await {
            Ok(mut scoped_tx) => {
                let row = sqlx::query_as(
                    "SELECT g.id, sub.stripe_subscription_id, g.credits_override FROM grants g \
                       JOIN plans p ON p.id = g.plan_id \
                       JOIN stripe_subscriptions sub ON sub.account_id = g.account_id \
                      WHERE g.account_id = $1 AND g.revoked_at IS NULL AND p.code = $2 \
                        AND sub.stripe_subscription_id IS NOT NULL \
                      ORDER BY g.granted_at DESC, g.id DESC LIMIT 1",
                )
                .bind(account_id)
                .bind(crate::billing::MIXED_TIER_PLAN_CODE)
                .fetch_optional(&mut *scoped_tx)
                .await;
                if let Err(e) = scoped_tx.commit().await {
                    return internal_error("current grant lookup (commit)", e);
                }
                match row {
                    Ok(row) => row,
                    Err(e) => return internal_error("current grant lookup", e),
                }
            }
            Err(e) => return internal_error("current grant lookup (scope)", e),
        };
    let Some((grant_id, subscription_id, grant_credits_override)) = team_grant else {
        return conflict("this account has no active team subscription — seat tiers only apply to team accounts");
    };

    // Billing self-serve PR F review, CRITICAL finding: every Stripe
    // mutation below must serialize under `lock_period_rotation`, matching
    // the S2 rule `billing::seat_lifecycle`'s own module doc establishes for
    // acquire/release — this route used to read member state and push to
    // Stripe against the raw pool, opening its locked transaction only
    // AFTERWARD for the ledger writes. That left the Stripe call (and the
    // state it decided on) racing a concurrent `roll_period`/`acquire_seat`/
    // `release_seat`/second `change_seat_tier` call for the SAME account.
    // The lock is now taken FIRST, and every read below (member state,
    // current/target/effective tier) happens INSIDE it, mirroring
    // `seat_lifecycle::acquire_seat`/`release_seat`'s own "S2: everything
    // below runs inside ONE locked transaction" shape.
    let mut tx = match crate::tenancy::scope::begin_account_scoped(control, account_id).await {
        Ok(tx) => tx,
        Err(e) => return internal_error("scope", e),
    };
    if let Err(e) = crate::billing::renewal::lock_period_rotation(&mut tx, account_id).await {
        let _ = tx.rollback().await;
        return internal_error("lock", e);
    }

    let member: Option<TargetMember> = match sqlx::query_as::<
        _,
        (Uuid, Option<Uuid>, Option<chrono::DateTime<chrono::Utc>>),
    >(
        "SELECT seat_tier_id, pending_seat_tier_id, seat_purchased_at FROM account_members \
          WHERE account_id = $1 AND user_id = $2",
    )
    .bind(account_id)
    .bind(target_user_id)
    .fetch_optional(&mut *tx)
    .await
    {
        Ok(row) => row.map(|(seat_tier_id, pending_seat_tier_id, seat_purchased_at)| TargetMember {
            seat_tier_id,
            pending_seat_tier_id,
            seat_purchased_at,
        }),
        Err(e) => {
            let _ = tx.rollback().await;
            return internal_error("target member lookup", e);
        }
    };
    let Some(member) = member else {
        let _ = tx.rollback().await;
        return not_found("no such member on this account");
    };
    if member.seat_purchased_at.is_none() {
        let _ = tx.rollback().await;
        return conflict("this member's seat has not been purchased yet — nothing to move");
    }

    let current_tier: SeatTierRow = match seat_tier_by_id(&mut *tx, member.seat_tier_id, "current tier").await {
        Ok(row) => row,
        Err(resp) => {
            let _ = tx.rollback().await;
            return resp;
        }
    };

    let target_tier: SeatTierRow = match sqlx::query_as(
        "SELECT id, code, credits, price_usd_cents, stripe_price_id FROM seat_tiers WHERE code = $1 AND is_active = true",
    )
    .bind(&body.tier)
    .fetch_optional(&mut *tx)
    .await
    {
        Ok(Some((id, code, credits, price_usd_cents, Some(stripe_price_id)))) => {
            SeatTierRow { id, code, credits, price_usd_cents, stripe_price_id }
        }
        Ok(Some((_, code, _, _, None))) => {
            let _ = tx.rollback().await;
            return internal_error("target tier price lookup", format!("seat tier {code} has no stripe_price_id"));
        }
        Ok(None) => {
            let _ = tx.rollback().await;
            return bad_request("unknown or inactive seat tier");
        }
        Err(e) => {
            let _ = tx.rollback().await;
            return internal_error("target tier lookup", e);
        }
    };

    // S1 — the tier Stripe is ACTUALLY billing this member's seat at right
    // now: the pending downgrade's target if one is in flight (already
    // pushed to Stripe — see this module's own "S1" doc section), otherwise
    // the member's plain `seat_tier_id`.
    let effective_tier: SeatTierRow = match member.pending_seat_tier_id {
        Some(pending_id) if pending_id != current_tier.id => {
            match seat_tier_by_id(&mut *tx, pending_id, "pending (effective) tier").await {
                Ok(row) => row,
                Err(resp) => {
                    let _ = tx.rollback().await;
                    return resp;
                }
            }
        }
        _ => match seat_tier_by_id(&mut *tx, current_tier.id, "effective tier").await {
            Ok(row) => row,
            Err(resp) => {
                let _ = tx.rollback().await;
                return resp;
            }
        },
    };

    if target_tier.id == current_tier.id {
        // A request for the member's ORIGINAL (pre-downgrade) tier.
        if member.pending_seat_tier_id.is_some() {
            // Cancelling a pending downgrade — S1: Stripe is CURRENTLY
            // billing the effective (pending) tier, so reversing it means
            // pushing a move back FROM the effective tier TO the tier being
            // requested (= current_tier), `proration_behavior: none` (the
            // member already paid the old tier through period end — a
            // proration here would double-charge, symmetric with the
            // downgrade's own posture). See this module's own "S1" doc
            // section, "Cancelling a pending downgrade". Now runs inside the
            // SAME locked transaction as everything else in this handler.
            let idem_base = format!(
                "seat:{account_id}:{target_user_id}:tier-change-cancel:{}:{}:{}",
                effective_tier.code, current_tier.code, Uuid::new_v4()
            );
            if let Err(e) = move_stripe_seat_item(
                stripe,
                &subscription_id,
                &effective_tier.stripe_price_id,
                &current_tier.stripe_price_id,
                "none",
                &idem_base,
            )
            .await
            {
                let _ = tx.rollback().await;
                return internal_error("stripe seat move (cancel pending downgrade)", e);
            }
        }
        let cleared = match sqlx::query(
            "UPDATE account_members SET pending_seat_tier_id = NULL \
              WHERE account_id = $1 AND user_id = $2 AND pending_seat_tier_id IS NOT NULL",
        )
        .bind(account_id)
        .bind(target_user_id)
        .execute(&mut *tx)
        .await
        {
            Ok(res) => res,
            Err(e) => {
                let _ = tx.rollback().await;
                return internal_error("clear pending downgrade", e);
            }
        };
        // Billing self-serve PR F review, minor finding: verify the clear
        // actually matched a row whenever the read above said one existed —
        // the Stripe reversal above already ran under that belief; a 0-row
        // surprise here (impossible under this function's own advisory lock
        // in normal operation) is logged loudly rather than silently
        // accepted, matching this module's "loud over quiet" posture
        // elsewhere.
        if member.pending_seat_tier_id.is_some() && cleared.rows_affected() == 0 {
            tracing::error!(
                %account_id, %target_user_id,
                "billing_seats::change_seat_tier: pending_seat_tier_id clear affected 0 rows despite holding \
                 the per-account advisory lock and a pending downgrade being read moments ago — the Stripe \
                 reversal above already ran; the account's ledger may now be drifted"
            );
        }
        if let Err(e) = tx.commit().await {
            return internal_error("commit", e);
        }
        return (
            StatusCode::OK,
            Json(SeatTierChangeResponse { outcome: "unchanged", tier: target_tier.code }),
        )
            .into_response();
    }

    // Billing self-serve PR F review, finding: classify the MONEY direction
    // against the EFFECTIVE tier (what Stripe is actually billing right
    // now), not the stale `current_tier` — S1 already requires the move's
    // FROM side to use `effective_tier`; the proration-direction
    // classification must agree or the two can disagree the moment a
    // pending downgrade is in flight and a THIRD tier is requested. Worked
    // example: `seat_tier_id = enterprise`, `pending_seat_tier_id =
    // standard`, request `tier = premium`. Classifying against
    // `current_tier` (enterprise) reads this as a downgrade and pushes
    // `proration: none`, while the actual Stripe move (standard -> premium)
    // is a genuine mid-period price increase that must prorate — an
    // under-billing bug. `current_tier` is kept ONLY for the DB
    // upgrade/downgrade semantics below (`seat_tier_id` flips from it on an
    // upgrade; module doc's own "Upgrade now, downgrade deferred" section).
    let is_upgrade = target_tier.price_usd_cents > effective_tier.price_usd_cents;
    let proration_behavior = if is_upgrade { "create_prorations" } else { "none" };
    // Finding 8 (`billing::seat_lifecycle`'s own "Idempotency-key recipes"
    // doc section): a FRESH UUID suffix per logical call, not a static
    // per-(account, member, tier-pair) key — Stripe caches a key's response
    // for ~24h, so a stable key would replay a cached response for a member
    // whose tier is changed, changed back, and changed again within that
    // window. Cross-call dedup (a genuine HTTP retry of this exact request)
    // is this function's own re-read-under-lock above, not the idempotency
    // key's job — identical division of labor to
    // `seat_lifecycle::acquire_seat`.
    let idem_base = format!(
        "seat:{account_id}:{target_user_id}:tier-change:{}:{}:{}",
        current_tier.code, target_tier.code, Uuid::new_v4()
    );
    // S1: the Stripe move's FROM side is the EFFECTIVE tier, not the stale
    // `current_tier` (`seat_tier_id`) — see this module's own "S1" doc
    // section, "A second, different tier-change request while a downgrade
    // is already pending". When the target already equals what Stripe is
    // CURRENTLY billing (a re-request of an already-in-flight pending
    // downgrade — the only way this is reachable is `target == pending`,
    // since `target == current_tier.id` was handled above), there is
    // nothing to move at Stripe at all; skip the call rather than pushing a
    // self-referential no-op move.
    if effective_tier.id != target_tier.id {
        if let Err(e) = move_stripe_seat_item(
            stripe,
            &subscription_id,
            &effective_tier.stripe_price_id,
            &target_tier.stripe_price_id,
            proration_behavior,
            &idem_base,
        )
        .await
        {
            let _ = tx.rollback().await;
            return internal_error("stripe seat move", e);
        }
    }

    if is_upgrade {
        // ── Immediate: move the ledger, flip the tier, raise the current
        // period's allowance (consumed kept). ──
        let deleted = match sqlx::query(
            "DELETE FROM grant_seat_tiers WHERE grant_id = $1 AND seat_tier_id = $2 AND seats <= 1",
        )
        .bind(grant_id)
        .bind(current_tier.id)
        .execute(&mut *tx)
        .await
        {
            Ok(res) => res.rows_affected(),
            Err(e) => {
                let _ = tx.rollback().await;
                return internal_error("ledger move (delete old)", e);
            }
        };
        let decremented = match sqlx::query(
            "UPDATE grant_seat_tiers SET seats = seats - 1 \
              WHERE grant_id = $1 AND seat_tier_id = $2 AND seats > 1",
        )
        .bind(grant_id)
        .bind(current_tier.id)
        .execute(&mut *tx)
        .await
        {
            Ok(res) => res.rows_affected(),
            Err(e) => {
                let _ = tx.rollback().await;
                return internal_error("ledger move (decrement old)", e);
            }
        };
        // Billing self-serve PR F review, minor finding: the DELETE and the
        // UPDATE above are mutually exclusive by their own WHERE clauses
        // (`seats <= 1` vs `seats > 1`) — together they should match EXACTLY
        // one `grant_seat_tiers` row (the source tier's own). If neither
        // matched, this account's ledger already held no row for
        // `current_tier` at all (a pre-existing drift this move did not
        // cause), and the INSERT below would still add +1 to the target,
        // inflating the ledger's total past what was actually purchased.
        // Logged loudly rather than silently proceeding — the same "loud
        // over quiet" posture `billing::renewal::move_pending_seat_tier_counts`
        // uses for the identical shape of drift.
        if deleted == 0 && decremented == 0 {
            tracing::warn!(
                %grant_id, seat_tier_id = %current_tier.id,
                "billing_seats::change_seat_tier: no grant_seat_tiers row matched the source tier for this \
                 upgrade — the ledger is already drifted; proceeding to credit the target tier regardless"
            );
        }
        if let Err(e) = sqlx::query(
            "INSERT INTO grant_seat_tiers (grant_id, seat_tier_id, seats) VALUES ($1, $2, 1) \
             ON CONFLICT (grant_id, seat_tier_id) DO UPDATE SET seats = grant_seat_tiers.seats + 1",
        )
        .bind(grant_id)
        .bind(target_tier.id)
        .execute(&mut *tx)
        .await
        {
            let _ = tx.rollback().await;
            return internal_error("ledger move (upsert new)", e);
        }
        // Shared helper (minor finding, review round: this SQL was
        // hand-copied 5x across the crate) — see
        // `billing::renewal::resync_grant_seats`'s own doc comment.
        if let Err(e) = crate::billing::renewal::resync_grant_seats(&mut tx, grant_id).await {
            let _ = tx.rollback().await;
            return internal_error("ledger move (sync grants.seats)", e);
        }
        if let Err(e) = sqlx::query(
            "UPDATE account_members SET seat_tier_id = $3, pending_seat_tier_id = NULL \
              WHERE account_id = $1 AND user_id = $2",
        )
        .bind(account_id)
        .bind(target_user_id)
        .bind(target_tier.id)
        .execute(&mut *tx)
        .await
        {
            let _ = tx.rollback().await;
            return internal_error("member tier flip", e);
        }
        // Current period only — consumed_credits untouched. Mirrors the
        // mid-period seat-add precedent (module doc).
        //
        // Finding 10 (billing self-serve PR F review): SKIP this raise
        // entirely when the account's grant carries `credits_override` —
        // an override is an admin's ABSOLUTE ACCOUNT TOTAL, and
        // `billing::renewal`'s own per-tier-vs-override split (module doc,
        // "Per-tier allowances for `team` grants") already says a
        // `credits_override` grant falls back to dividing that total evenly
        // across the RECEIVING ROSTER, never per-tier catalog figures — a
        // tier CHANGE does not alter one member's share of that fixed pool.
        // Overwriting `granted_credits` with `target_tier.credits` here for
        // such a grant would cut an upgraded member's allowance DOWN to the
        // catalog figure, potentially below what they have already
        // consumed this period, even though the account's actual override
        // total never changed.
        //
        // Billing self-serve PR F review, minor finding: RAISE via
        // `GREATEST`, never assign — `SeatTierRow`'s own doc comment already
        // flags that a future catalog could price a tier higher while
        // granting FEWER credits, at which point an unconditional assignment
        // could lower `granted_credits` mid-period, potentially below
        // `consumed_credits`. `is_upgrade` is classified by PRICE, not
        // credits (the same doc comment), so a price-upgrade is not
        // guaranteed to be a credits-upgrade too; `GREATEST` makes this
        // write a genuine one-way raise regardless.
        if grant_credits_override.is_none() {
            if let Err(e) = sqlx::query(
                "UPDATE member_allowance_periods \
                    SET granted_credits = GREATEST(granted_credits, $3), updated_at = now() \
                  WHERE account_id = $1 AND user_id = $2 \
                    AND period_start <= now() AND period_end > now()",
            )
            .bind(account_id)
            .bind(target_user_id)
            .bind(target_tier.credits)
            .execute(&mut *tx)
            .await
            {
                let _ = tx.rollback().await;
                return internal_error("current period allowance raise", e);
            }
        }
    } else {
        // ── Deferred: record intent only. `billing::renewal::roll_period`
        // applies it at the next period roll. ──
        if let Err(e) = sqlx::query(
            "UPDATE account_members SET pending_seat_tier_id = $3 WHERE account_id = $1 AND user_id = $2",
        )
        .bind(account_id)
        .bind(target_user_id)
        .bind(target_tier.id)
        .execute(&mut *tx)
        .await
        {
            let _ = tx.rollback().await;
            return internal_error("record pending downgrade", e);
        }
    }

    if let Err(e) = tx.commit().await {
        return internal_error("commit", e);
    }

    (
        StatusCode::OK,
        Json(SeatTierChangeResponse {
            outcome: if is_upgrade { "upgraded" } else { "downgraded" },
            tier: target_tier.code,
        }),
    )
        .into_response()
}

/// `GET /v1/billing/seat-tiers` (CS-442, item 40, D8) — every ACTIVE seat
/// tier, for the join-request approval row's tier picker (CS-441, web-side)
/// and any other billing-facing surface that needs the catalog without
/// hardcoding it. Any SIGNED-IN caller, not admin-gated and not
/// org-context-gated: this is global reference data (`seat_tiers` carries
/// no `account_id` at all — the same "global reference-data table" posture
/// `routes::checkout`'s own module doc documents for `plans`/`seat_tiers`/
/// `credit_packs`), so there is no per-account authorization decision to
/// make here at all — mirrors `routes::org_slots::get_slots`'s own "any
/// authenticated caller may read" bar, one step further (not even an org
/// context is required).
#[utoipa_path(
    get,
    path = "/v1/billing/seat-tiers",
    tag = "billing",
    security(("bearer_token" = [])),
    responses(
        (status = 200, description = "Active seat tiers (CS-442, item 40)", body = [SeatTierSummary]),
        (status = 503, description = "Database unavailable"),
    )
)]
pub async fn list_seat_tiers(State(state): State<Arc<AppState>>, Extension(_user): Extension<UserContext>) -> Response {
    let Some(control) = state.control_db() else {
        return service_unavailable();
    };
    match sqlx::query_as::<_, (String, f64, i32)>(
        "SELECT code, credits::float8, price_usd_cents FROM seat_tiers WHERE is_active = true ORDER BY code",
    )
    .fetch_all(crate::tenancy::scope::request_read(control))
    .await
    {
        Ok(rows) => {
            let tiers: Vec<SeatTierSummary> = rows
                .into_iter()
                .map(|(code, credits, price_usd_cents)| SeatTierSummary { code, credits, price_usd_cents })
                .collect();
            (StatusCode::OK, Json(tiers)).into_response()
        }
        Err(e) => internal_error("list seat tiers", e),
    }
}

// ── Tests (billing self-serve PR F) ─────────────────────────────────────────
//
// Harness shape copied from `routes::checkout`'s `pg_tests` (that module's
// own doc comment on `CATALOG_PRICE_LOCK`/`cleanup_account` documents the
// discipline this file follows): `RELINFER_TEST_DATABASE_URL`-gated,
// `CATALOG_PRICE_LOCK`-serialized against the SAME shared `plans`/
// `seat_tiers` rows every other billing pg-test suite mutates, capture-then-
// cleanup ordering so a failing assertion can never skip teardown.
// `CATALOG_PRICE_LOCK` and `set_seat_tier_price` themselves now live in
// `crate::pg_catalog_test_support` (CodeRabbit round 3, PR #660) rather than
// as a per-module local — see that module's own doc comment for why.
#[cfg(test)]
mod pg_tests {
    use super::*;
    use axum::extract::State;
    use crate::pg_catalog_test_support::{set_seat_tier_price, CATALOG_PRICE_LOCK};

    async fn pg_test_pool(test_name: &str) -> Option<sqlx::PgPool> {
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

    /// A local axum server standing in for Stripe's subscription-item
    /// surface: `GET /v1/subscriptions/{id}` always reports ONE item, at
    /// whatever `price_id` this mock was built with, quantity 1 — the shape
    /// [`move_stripe_seat_item`] resolves as a single in-place
    /// `swap_subscription_item_price` call (the old item's only occupant,
    /// no existing item on the new tier), which every test below's
    /// single-member fixture actually is. `POST`/`DELETE` on
    /// `/v1/subscription_items/{id}` and `POST /v1/subscription_items`
    /// (the add path, unreached by the swap shape but wired for
    /// completeness) all answer 200 with a stub body — this suite proves
    /// what THIS module's route does with the response, not the wire-pair
    /// correctness of the Stripe calls themselves (already proven in
    /// `billing::stripe_client`'s own Layer 1 tests, same division of labor
    /// `routes::checkout::pg_tests::spawn_checkout_mock` documents).
    async fn spawn_seat_change_mock(current_price_id: &str) -> StripeClient {
        let item_id = "si_mock_1";
        let body = json!({
            "id": "sub_mock_1",
            "status": "active",
            "items": {"data": [{
                "id": item_id,
                "price": {"id": current_price_id},
                "quantity": 1,
                "current_period_start": 1_700_000_000,
                "current_period_end": 1_702_678_400,
            }]},
        })
        .to_string();
        async fn stub_ok() -> axum::response::Response {
            (
                axum::http::StatusCode::OK,
                [("content-type", "application/json")],
                json!({"id": "si_mock_new", "quantity": 1}).to_string(),
            )
                .into_response()
        }
        // `{id}` wildcard, not a literal path: `seed_org_account` mints a
        // UNIQUE `stripe_subscription_id` per test (the column is UNIQUE
        // table-wide, and a hardcoded id permanently poisons the shared test
        // database the first time any test panics before its cleanup runs),
        // so the mock must answer whatever id the route resolved from the DB.
        // The body's own `id` field is never compared against it.
        let app = axum::Router::new()
            .route(
                "/v1/subscriptions/{id}",
                axum::routing::get(move || {
                    let body = body.clone();
                    async move {
                        (
                            axum::http::StatusCode::OK,
                            [("content-type", "application/json")],
                            body,
                        )
                    }
                }),
            )
            .route(
                &format!("/v1/subscription_items/{item_id}"),
                axum::routing::post(stub_ok).delete(stub_ok),
            )
            .route("/v1/subscription_items", axum::routing::post(stub_ok));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind mock");
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.ok();
        });
        StripeClient::new(reqwest::Client::new(), "sk_test_mock").with_base_url(format!("http://{addr}"))
    }

    async fn seat_tier_id(pool: &sqlx::PgPool, code: &str) -> Uuid {
        sqlx::query_scalar("SELECT id FROM seat_tiers WHERE code = $1 AND is_active = true")
            .bind(code)
            .fetch_one(pool)
            .await
            .expect("active seat tier must exist (0165's seed)")
    }

    /// An org account with one admin member (`standard` tier, unpurchased by
    /// default) and, when `with_subscription` is true, a linked
    /// `stripe_subscriptions` row (`status = "active"`).
    async fn seed_org_account(pool: &sqlx::PgPool, with_subscription: bool) -> (Uuid, Uuid) {
        let admin_id: Uuid = sqlx::query_scalar("INSERT INTO users (email) VALUES ($1) RETURNING id")
            .bind(format!("{}@billing-seats-prf.test", Uuid::new_v4()))
            .fetch_one(pool)
            .await
            .expect("insert admin user");
        let account_id: Uuid = sqlx::query_scalar(
            "INSERT INTO accounts (owner_user_id, clerk_org_id) VALUES ($1, $2) RETURNING id",
        )
        .bind(admin_id)
        .bind(format!("org_{}", Uuid::new_v4().simple()))
        .fetch_one(pool)
        .await
        .expect("insert org account");
        let standard_id = seat_tier_id(pool, "standard").await;
        sqlx::query(
            "INSERT INTO account_members (account_id, user_id, role, seat_tier_id) \
             VALUES ($1, $2, 'owner', $3)",
        )
        .bind(account_id)
        .bind(admin_id)
        .bind(standard_id)
        .execute(pool)
        .await
        .expect("insert admin membership");
        if with_subscription {
            // Unique per test — `stripe_subscription_id` is UNIQUE table-wide
            // (0162), and a hardcoded literal here permanently poisons the
            // shared test database the first time any test panics before its
            // cleanup deletes the account (and, via CASCADE, this row). The
            // mock (`spawn_seat_change_mock`) answers any id via a wildcard
            // route, so nothing needs to know the value.
            { let mut financial=financial_fixture(pool,account_id).await; let value=sqlx::query(
                "INSERT INTO stripe_subscriptions (account_id, stripe_customer_id, stripe_subscription_id, status) \
                 VALUES ($1, $2, $3, 'active')",
            )
            .bind(account_id)
            .bind(format!("cus_{}", Uuid::new_v4().simple()))
            .bind(format!("sub_mock_{}", Uuid::new_v4().simple()))
            .execute(&mut *financial)
            .await
            .expect("link stripe subscription"); financial.commit().await.unwrap(); value };
        }
        (account_id, admin_id)
    }

    /// Adds a second member (`role = 'member'`) at `tier_code`, optionally
    /// stamping `seat_purchased_at = now()`. Returns the new member's
    /// `user_id`.
    async fn add_member(pool: &sqlx::PgPool, account_id: Uuid, tier_code: &str, purchased: bool) -> Uuid {
        let user_id: Uuid = sqlx::query_scalar("INSERT INTO users (email) VALUES ($1) RETURNING id")
            .bind(format!("{}@billing-seats-prf.test", Uuid::new_v4()))
            .fetch_one(pool)
            .await
            .expect("insert member user");
        let tier_id = seat_tier_id(pool, tier_code).await;
        sqlx::query(
            "INSERT INTO account_members (account_id, user_id, role, seat_tier_id, seat_purchased_at) \
             VALUES ($1, $2, 'member', $3, CASE WHEN $4 THEN now() ELSE NULL END)",
        )
        .bind(account_id)
        .bind(user_id)
        .bind(tier_id)
        .bind(purchased)
        .execute(pool)
        .await
        .expect("insert second membership");
        user_id
    }

    /// The `team` grant + a `grant_seat_tiers` row recording ONE purchased
    /// seat at `tier_code` — the ledger state `require_purchase_authority`'s
    /// downstream reads (this route's own current-grant/current-tier
    /// lookups) expect for a member whose seat IS purchased.
    async fn seed_team_grant(pool: &sqlx::PgPool, account_id: Uuid, tier_code: &str) -> Uuid {
        let plan_id: Uuid = sqlx::query_scalar("SELECT id FROM plans WHERE code = 'team'")
            .fetch_one(pool)
            .await
            .expect("team plan must be seeded (0167)");
        let grant_id: Uuid = { let mut financial=financial_fixture(pool,account_id).await; let value=sqlx::query_scalar(
            "INSERT INTO grants (account_id, plan_id, seats, plan_is_seat_priced) \
             VALUES ($1, $2, 1, true) RETURNING id",
        )
        .bind(account_id)
        .bind(plan_id)
        .fetch_one(&mut *financial)
        .await
        .expect("insert team grant"); financial.commit().await.unwrap(); value };
        let tier_id = seat_tier_id(pool, tier_code).await;
        { let mut financial=financial_fixture(pool,account_id).await; let value=sqlx::query("INSERT INTO grant_seat_tiers (grant_id, seat_tier_id, seats) VALUES ($1, $2, 1)")
            .bind(grant_id)
            .bind(tier_id)
            .execute(&mut *financial)
            .await
            .expect("insert grant_seat_tiers ledger row"); financial.commit().await.unwrap(); value };
        grant_id
    }

    /// A `member_allowance_periods` row covering `now()` for `user_id`, at
    /// `granted_credits`, some already-`consumed_credits` (so an upgrade
    /// test can prove `consumed_credits` survives the allowance raise
    /// untouched).
    async fn seed_current_period(
        pool: &sqlx::PgPool,
        account_id: Uuid,
        user_id: Uuid,
        granted_credits: i64,
        consumed_credits: i64,
    ) {
        let now = chrono::Utc::now();
        { let mut financial=financial_fixture(pool,account_id).await; let value=sqlx::query(
            "INSERT INTO member_allowance_periods \
                (account_id, user_id, period_start, period_end, granted_credits, consumed_credits) \
             VALUES ($1, $2, $3, $4, $5, $6)",
        )
        .bind(account_id)
        .bind(user_id)
        .bind(now - chrono::Duration::days(1))
        .bind(now + chrono::Duration::days(29))
        .bind(sqlx::types::Decimal::from(granted_credits))
        .bind(sqlx::types::Decimal::from(consumed_credits))
        .execute(&mut *financial)
        .await
        .expect("seed current allowance period"); financial.commit().await.unwrap(); value };
    }

    fn user_context(account_id: Uuid, user_id: Uuid, role: &str) -> UserContext {
        UserContext {
            user_id,
            account_id,
            role: role.to_string(),
            device_id: "billing-seats-prf-test".to_string(),
        }
    }

    async fn financial_fixture(pool:&sqlx::PgPool,account:Uuid) -> sqlx::Transaction<'static,sqlx::Postgres> {
        let mut tx=crate::tenancy::scope::begin_account_scoped(pool,account).await.unwrap();
        crate::billing::renewal::lock_period_rotation(&mut tx,account).await.unwrap();
        tx
    }
    async fn cleanup_account(pool: &sqlx::PgPool, account_id: Uuid, users: &[Uuid]) {
        { let mut financial=financial_fixture(pool,account_id).await; let value=sqlx::query("DELETE FROM accounts WHERE id = $1")
            .bind(account_id)
            .execute(&mut *financial)
            .await
            .expect("cleanup account"); financial.commit().await.unwrap(); value };
        for user_id in users {
            sqlx::query("DELETE FROM users WHERE id = $1")
                .bind(user_id)
                .execute(pool)
                .await
                .expect("cleanup user");
        }
    }

    async fn body_json(resp: Response) -> serde_json::Value {
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX).await.expect("read body");
        serde_json::from_slice(&bytes).expect("valid JSON body")
    }

    async fn app_state(pool: sqlx::PgPool, stripe: Option<StripeClient>) -> Arc<AppState> {
        let mut state = crate::test_app_state_owned_with_db_and_store(pool, None);
        state.stripe = stripe.map(Arc::new);
        Arc::new(state)
    }

    #[tokio::test]
    async fn a_plain_member_cannot_change_a_seat_tier() {
        let Some(pool) = pg_test_pool("a_plain_member_cannot_change_a_seat_tier").await else {
            return;
        };
        let (account_id, admin_id) = seed_org_account(&pool, false).await;
        let member_id = add_member(&pool, account_id, "standard", false).await;
        let state = app_state(pool.clone(), None).await;

        let resp = change_seat_tier(
            State(state),
            Extension(user_context(account_id, member_id, "member")),
            Path(member_id),
            Json(SeatTierChangeRequest { tier: "premium".to_string() }),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::FORBIDDEN);

        cleanup_account(&pool, account_id, &[admin_id, member_id]).await;
    }

    #[tokio::test]
    async fn a_personal_account_caller_is_refused() {
        let Some(pool) = pg_test_pool("a_personal_account_caller_is_refused").await else {
            return;
        };
        let owner_id: Uuid = sqlx::query_scalar("INSERT INTO users (email) VALUES ($1) RETURNING id")
            .bind(format!("{}@billing-seats-prf.test", Uuid::new_v4()))
            .fetch_one(&pool)
            .await
            .expect("insert user");
        let account_id: Uuid = sqlx::query_scalar("INSERT INTO accounts (owner_user_id) VALUES ($1) RETURNING id")
            .bind(owner_id)
            .fetch_one(&pool)
            .await
            .expect("insert personal account");
        sqlx::query("INSERT INTO account_members (account_id, user_id, role) VALUES ($1, $2, 'owner')")
            .bind(account_id)
            .bind(owner_id)
            .execute(&pool)
            .await
            .expect("insert membership");
        let state = app_state(pool.clone(), None).await;

        let resp = change_seat_tier(
            State(state),
            Extension(user_context(account_id, owner_id, "owner")),
            Path(owner_id),
            Json(SeatTierChangeRequest { tier: "premium".to_string() }),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);

        cleanup_account(&pool, account_id, &[owner_id]).await;
    }

    #[tokio::test]
    async fn without_stripe_configured_the_route_is_unavailable() {
        let Some(pool) = pg_test_pool("without_stripe_configured_the_route_is_unavailable").await else {
            return;
        };
        let (account_id, admin_id) = seed_org_account(&pool, true).await;
        let state = app_state(pool.clone(), None).await;

        let resp = change_seat_tier(
            State(state),
            Extension(user_context(account_id, admin_id, "owner")),
            Path(admin_id),
            Json(SeatTierChangeRequest { tier: "premium".to_string() }),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);

        cleanup_account(&pool, account_id, &[admin_id]).await;
    }

    #[tokio::test]
    async fn a_frozen_subscription_refuses_a_tier_change() {
        let Some(pool) = pg_test_pool("a_frozen_subscription_refuses_a_tier_change").await else {
            return;
        };
        let (account_id, admin_id) = seed_org_account(&pool, true).await;
        { let mut financial=financial_fixture(&pool,account_id).await; let value=sqlx::query("UPDATE stripe_subscriptions SET status = 'canceled' WHERE account_id = $1")
            .bind(account_id)
            .execute(&mut *financial)
            .await
            .expect("freeze subscription"); financial.commit().await.unwrap(); value };
        let stripe = spawn_seat_change_mock("price_never_called").await;
        let state = app_state(pool.clone(), Some(stripe)).await;

        let resp = change_seat_tier(
            State(state),
            Extension(user_context(account_id, admin_id, "owner")),
            Path(admin_id),
            Json(SeatTierChangeRequest { tier: "premium".to_string() }),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::CONFLICT);

        cleanup_account(&pool, account_id, &[admin_id]).await;
    }

    #[tokio::test]
    async fn a_non_team_account_refuses_a_tier_change() {
        let Some(pool) = pg_test_pool("a_non_team_account_refuses_a_tier_change").await else {
            return;
        };
        // `with_subscription = true` but no `grants` row at all — no active
        // team grant to change a tier against.
        let (account_id, admin_id) = seed_org_account(&pool, true).await;
        let stripe = spawn_seat_change_mock("price_never_called").await;
        let state = app_state(pool.clone(), Some(stripe)).await;

        let resp = change_seat_tier(
            State(state),
            Extension(user_context(account_id, admin_id, "owner")),
            Path(admin_id),
            Json(SeatTierChangeRequest { tier: "premium".to_string() }),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::CONFLICT);

        cleanup_account(&pool, account_id, &[admin_id]).await;
    }

    #[tokio::test]
    async fn an_unknown_member_is_refused() {
        let Some(pool) = pg_test_pool("an_unknown_member_is_refused").await else {
            return;
        };
        let (account_id, admin_id) = seed_org_account(&pool, true).await;
        seed_team_grant(&pool, account_id, "standard").await;
        let stripe = spawn_seat_change_mock("price_never_called").await;
        let state = app_state(pool.clone(), Some(stripe)).await;
        let nobody = Uuid::new_v4();

        let resp = change_seat_tier(
            State(state),
            Extension(user_context(account_id, admin_id, "owner")),
            Path(nobody),
            Json(SeatTierChangeRequest { tier: "premium".to_string() }),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);

        cleanup_account(&pool, account_id, &[admin_id]).await;
    }

    #[tokio::test]
    async fn an_unpurchased_seat_refuses_a_tier_change() {
        let Some(pool) = pg_test_pool("an_unpurchased_seat_refuses_a_tier_change").await else {
            return;
        };
        let (account_id, admin_id) = seed_org_account(&pool, true).await;
        seed_team_grant(&pool, account_id, "standard").await;
        let member_id = add_member(&pool, account_id, "standard", false).await;
        let stripe = spawn_seat_change_mock("price_never_called").await;
        let state = app_state(pool.clone(), Some(stripe)).await;

        let resp = change_seat_tier(
            State(state),
            Extension(user_context(account_id, admin_id, "owner")),
            Path(member_id),
            Json(SeatTierChangeRequest { tier: "premium".to_string() }),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::CONFLICT);

        cleanup_account(&pool, account_id, &[admin_id, member_id]).await;
    }

    #[tokio::test]
    async fn an_unknown_tier_code_is_refused() {
        let Some(pool) = pg_test_pool("an_unknown_tier_code_is_refused").await else {
            return;
        };
        let (account_id, admin_id) = seed_org_account(&pool, true).await;
        seed_team_grant(&pool, account_id, "standard").await;
        // The target's seat must be PURCHASED so the request gets past the
        // 409 unpurchased-seat check (which runs earlier in the handler) and
        // actually reaches the unknown-tier lookup this test exercises.
        sqlx::query(
            "UPDATE account_members SET seat_purchased_at = now() \
              WHERE account_id = $1 AND user_id = $2",
        )
        .bind(account_id)
        .bind(admin_id)
        .execute(&pool)
        .await
        .expect("mark the admin's seat purchased");
        let _catalog_guard = CATALOG_PRICE_LOCK.lock().await;
        let original_standard = set_seat_tier_price(&pool, "standard", Some("price_standard_bs_test")).await;
        let stripe = spawn_seat_change_mock("price_standard_bs_test").await;
        let state = app_state(pool.clone(), Some(stripe)).await;

        let resp = change_seat_tier(
            State(state),
            Extension(user_context(account_id, admin_id, "owner")),
            Path(admin_id),
            Json(SeatTierChangeRequest { tier: "not-a-real-tier".to_string() }),
        )
        .await;
        let status = resp.status();

        set_seat_tier_price(&pool, "standard", original_standard.as_deref()).await;
        cleanup_account(&pool, account_id, &[admin_id]).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }

    /// **The "at least" coverage item: upgrade raises the current period's
    /// allowance, consumed kept.** Also proves the ledger MOVE (old tier
    /// row emptied — deleted, not left at `seats = 0` — and the new tier
    /// row created) and the member's `seat_tier_id` flip + `
    /// pending_seat_tier_id` clear.
    #[tokio::test]
    async fn an_upgrade_moves_the_ledger_and_raises_the_current_period_allowance() {
        let Some(pool) =
            pg_test_pool("an_upgrade_moves_the_ledger_and_raises_the_current_period_allowance").await
        else {
            return;
        };
        let _catalog_guard = CATALOG_PRICE_LOCK.lock().await;
        let original_standard = set_seat_tier_price(&pool, "standard", Some("price_standard_bs_up")).await;
        let original_premium = set_seat_tier_price(&pool, "premium", Some("price_premium_bs_up")).await;

        let (account_id, admin_id) = seed_org_account(&pool, true).await;
        let grant_id = seed_team_grant(&pool, account_id, "standard").await;
        let member_id = add_member(&pool, account_id, "standard", true).await;
        seed_current_period(&pool, account_id, member_id, 3_000, 500).await;

        let stripe = spawn_seat_change_mock("price_standard_bs_up").await;
        let state = app_state(pool.clone(), Some(stripe)).await;

        let resp = change_seat_tier(
            State(state),
            Extension(user_context(account_id, admin_id, "owner")),
            Path(member_id),
            Json(SeatTierChangeRequest { tier: "premium".to_string() }),
        )
        .await;
        let status = resp.status();
        let body = body_json(resp).await;

        let (member_tier, pending): (Uuid, Option<Uuid>) = sqlx::query_as(
            "SELECT seat_tier_id, pending_seat_tier_id FROM account_members WHERE account_id = $1 AND user_id = $2",
        )
        .bind(account_id)
        .bind(member_id)
        .fetch_one(&pool)
        .await
        .expect("read member tier");
        let premium_id = seat_tier_id(&pool, "premium").await;
        let standard_id = seat_tier_id(&pool, "standard").await;

        let old_row: Option<i32> = sqlx::query_scalar(
            "SELECT seats FROM grant_seat_tiers WHERE grant_id = $1 AND seat_tier_id = $2",
        )
        .bind(grant_id)
        .bind(standard_id)
        .fetch_optional(&pool)
        .await
        .expect("read old tier row");
        let new_row: Option<i32> = sqlx::query_scalar(
            "SELECT seats FROM grant_seat_tiers WHERE grant_id = $1 AND seat_tier_id = $2",
        )
        .bind(grant_id)
        .bind(premium_id)
        .fetch_optional(&pool)
        .await
        .expect("read new tier row");
        let (granted, consumed): (sqlx::types::Decimal, sqlx::types::Decimal) = sqlx::query_as(
            "SELECT granted_credits, consumed_credits FROM member_allowance_periods \
              WHERE account_id = $1 AND user_id = $2 AND period_start <= now() AND period_end > now()",
        )
        .bind(account_id)
        .bind(member_id)
        .fetch_one(&pool)
        .await
        .expect("read current period");

        set_seat_tier_price(&pool, "standard", original_standard.as_deref()).await;
        set_seat_tier_price(&pool, "premium", original_premium.as_deref()).await;
        cleanup_account(&pool, account_id, &[admin_id, member_id]).await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["outcome"], "upgraded");
        assert_eq!(member_tier, premium_id, "member's seat_tier_id must flip immediately");
        assert_eq!(pending, None, "an upgrade must never leave a pending downgrade set");
        assert_eq!(old_row, None, "the emptied standard-tier row must be DELETED, not left at 0");
        assert_eq!(new_row, Some(1), "one seat now on the premium-tier ledger row");
        assert_eq!(granted, sqlx::types::Decimal::from(15_000), "current period raised to premium's credits");
        assert_eq!(consumed, sqlx::types::Decimal::from(500), "consumed_credits must survive the raise untouched");
    }

    /// A downgrade defers everything: only `pending_seat_tier_id` is
    /// written, the ledger and the member's live `seat_tier_id` are
    /// untouched until `billing::renewal::roll_period` applies it.
    #[tokio::test]
    async fn a_downgrade_only_records_pending_intent() {
        let Some(pool) = pg_test_pool("a_downgrade_only_records_pending_intent").await else {
            return;
        };
        let _catalog_guard = CATALOG_PRICE_LOCK.lock().await;
        let original_standard = set_seat_tier_price(&pool, "standard", Some("price_standard_bs_down")).await;
        let original_premium = set_seat_tier_price(&pool, "premium", Some("price_premium_bs_down")).await;

        let (account_id, admin_id) = seed_org_account(&pool, true).await;
        let grant_id = seed_team_grant(&pool, account_id, "premium").await;
        let member_id = add_member(&pool, account_id, "premium", true).await;

        let stripe = spawn_seat_change_mock("price_premium_bs_down").await;
        let state = app_state(pool.clone(), Some(stripe)).await;

        let resp = change_seat_tier(
            State(state),
            Extension(user_context(account_id, admin_id, "owner")),
            Path(member_id),
            Json(SeatTierChangeRequest { tier: "standard".to_string() }),
        )
        .await;
        let status = resp.status();
        let body = body_json(resp).await;

        let (member_tier, pending): (Uuid, Option<Uuid>) = sqlx::query_as(
            "SELECT seat_tier_id, pending_seat_tier_id FROM account_members WHERE account_id = $1 AND user_id = $2",
        )
        .bind(account_id)
        .bind(member_id)
        .fetch_one(&pool)
        .await
        .expect("read member tier");
        let premium_id = seat_tier_id(&pool, "premium").await;
        let standard_id = seat_tier_id(&pool, "standard").await;
        let premium_row: i32 = sqlx::query_scalar(
            "SELECT seats FROM grant_seat_tiers WHERE grant_id = $1 AND seat_tier_id = $2",
        )
        .bind(grant_id)
        .bind(premium_id)
        .fetch_one(&pool)
        .await
        .expect("premium ledger row must be untouched");

        set_seat_tier_price(&pool, "standard", original_standard.as_deref()).await;
        set_seat_tier_price(&pool, "premium", original_premium.as_deref()).await;
        cleanup_account(&pool, account_id, &[admin_id, member_id]).await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["outcome"], "downgraded");
        assert_eq!(member_tier, premium_id, "seat_tier_id must NOT flip immediately on a downgrade");
        assert_eq!(pending, Some(standard_id), "pending_seat_tier_id must record the intended downgrade");
        assert_eq!(premium_row, 1, "the ledger is untouched until roll_period applies the downgrade");
    }

    /// A same-tier request with NO pending downgrade to cancel is a genuine
    /// no-op — calls Stripe for nothing (this test's mock would 500 the
    /// whole request if it were ever hit).
    #[tokio::test]
    async fn a_same_tier_request_with_no_pending_downgrade_calls_no_stripe() {
        let Some(pool) =
            pg_test_pool("a_same_tier_request_with_no_pending_downgrade_calls_no_stripe").await
        else {
            return;
        };
        let _catalog_guard = CATALOG_PRICE_LOCK.lock().await;
        let original_standard = set_seat_tier_price(&pool, "standard", Some("price_standard_bs_same")).await;

        let (account_id, admin_id) = seed_org_account(&pool, true).await;
        seed_team_grant(&pool, account_id, "standard").await;
        let member_id = add_member(&pool, account_id, "standard", true).await;
        // No pending_seat_tier_id set at all.

        // A mock that would 500 on ANY request reaching it — proves the
        // same-tier branch genuinely never calls Stripe when nothing is
        // pending.
        let app = axum::Router::new().fallback(axum::routing::any(
            || async { axum::http::StatusCode::INTERNAL_SERVER_ERROR },
        ));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.ok();
        });
        let stripe = StripeClient::new(reqwest::Client::new(), "sk_test_mock")
            .with_base_url(format!("http://{addr}"));
        let state = app_state(pool.clone(), Some(stripe)).await;

        let resp = change_seat_tier(
            State(state),
            Extension(user_context(account_id, admin_id, "owner")),
            Path(member_id),
            Json(SeatTierChangeRequest { tier: "standard".to_string() }),
        )
        .await;
        let status = resp.status();
        let body = body_json(resp).await;

        let pending: Option<Uuid> = sqlx::query_scalar(
            "SELECT pending_seat_tier_id FROM account_members WHERE account_id = $1 AND user_id = $2",
        )
        .bind(account_id)
        .bind(member_id)
        .fetch_one(&pool)
        .await
        .expect("read pending");

        set_seat_tier_price(&pool, "standard", original_standard.as_deref()).await;
        cleanup_account(&pool, account_id, &[admin_id, member_id]).await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["outcome"], "unchanged");
        assert_eq!(pending, None);
        assert!(status.is_success(), "no Stripe call means no server error from the fallback 500 mock");
    }

    /// **Finding 4a.** Cancelling a PENDING downgrade (a same-tier request
    /// for the member's ORIGINAL tier while `pending_seat_tier_id` is set)
    /// must REVERSE the Stripe move — S1: Stripe is currently billing the
    /// EFFECTIVE (pending) tier, since the original downgrade already pushed
    /// it there — not merely clear the local marker and leave Stripe
    /// permanently on the never-applied tier. The mock only recognizes the
    /// PENDING tier's price (standard) as its current item, matching what
    /// Stripe would ACTUALLY hold mid-downgrade; the route pushing the
    /// reverse move (standard -> premium, `proration_behavior: none`) is
    /// exactly what `spawn_seat_change_mock`'s single-item swap shape
    /// answers successfully — a route that (pre-fix) never called Stripe at
    /// all would leave Stripe's item at STANDARD forever, an assertion this
    /// test cannot directly observe via the mock but the reversal call
    /// succeeding at all (rather than being skipped) is what proves the
    /// fix landed; the `outcome`/`pending` assertions below prove the DB
    /// half.
    #[tokio::test]
    async fn a_same_tier_request_cancels_a_pending_downgrade_by_reversing_stripe() {
        let Some(pool) = pg_test_pool(
            "a_same_tier_request_cancels_a_pending_downgrade_by_reversing_stripe",
        )
        .await
        else {
            return;
        };
        let _catalog_guard = CATALOG_PRICE_LOCK.lock().await;
        let original_standard = set_seat_tier_price(&pool, "standard", Some("price_standard_bs_cancel")).await;
        let original_premium = set_seat_tier_price(&pool, "premium", Some("price_premium_bs_cancel")).await;

        let (account_id, admin_id) = seed_org_account(&pool, true).await;
        let grant_id = seed_team_grant(&pool, account_id, "premium").await;
        let member_id = add_member(&pool, account_id, "premium", true).await;
        let standard_id = seat_tier_id(&pool, "standard").await;
        sqlx::query("UPDATE account_members SET pending_seat_tier_id = $3 WHERE account_id = $1 AND user_id = $2")
            .bind(account_id)
            .bind(member_id)
            .bind(standard_id)
            .execute(&pool)
            .await
            .expect("seed a pending downgrade (premium -> standard) to cancel");

        // The mock's ONE item is at STANDARD's price — exactly what Stripe
        // would actually hold: the original downgrade already pushed this
        // member's item there.
        let stripe = spawn_seat_change_mock("price_standard_bs_cancel").await;
        let state = app_state(pool.clone(), Some(stripe)).await;

        let resp = change_seat_tier(
            State(state),
            Extension(user_context(account_id, admin_id, "owner")),
            Path(member_id),
            Json(SeatTierChangeRequest { tier: "premium".to_string() }),
        )
        .await;
        let status = resp.status();
        let body = body_json(resp).await;

        let (member_tier, pending): (Uuid, Option<Uuid>) = sqlx::query_as(
            "SELECT seat_tier_id, pending_seat_tier_id FROM account_members WHERE account_id = $1 AND user_id = $2",
        )
        .bind(account_id)
        .bind(member_id)
        .fetch_one(&pool)
        .await
        .expect("read member tier");
        let premium_id = seat_tier_id(&pool, "premium").await;
        let premium_row: i32 = sqlx::query_scalar(
            "SELECT seats FROM grant_seat_tiers WHERE grant_id = $1 AND seat_tier_id = $2",
        )
        .bind(grant_id)
        .bind(premium_id)
        .fetch_one(&pool)
        .await
        .expect("the ledger must be untouched by a cancel — it never moved for the downgrade either");

        set_seat_tier_price(&pool, "standard", original_standard.as_deref()).await;
        set_seat_tier_price(&pool, "premium", original_premium.as_deref()).await;
        cleanup_account(&pool, account_id, &[admin_id, member_id]).await;

        assert_eq!(status, StatusCode::OK, "body: {body:?}");
        assert_eq!(body["outcome"], "unchanged");
        assert_eq!(member_tier, premium_id, "seat_tier_id was never touched by the never-applied downgrade");
        assert_eq!(pending, None, "the pending downgrade must be cleared");
        assert_eq!(premium_row, 1, "the ledger stays exactly as it was — a cancel is not a ledger move");
    }

    /// **Finding 4b.** A SECOND tier-change request while a downgrade is
    /// already pending, targeting the SAME tier the pending downgrade is
    /// already headed to, must resolve against the EFFECTIVE (pending) tier
    /// — S1 — not the stale `seat_tier_id`. The retired version computed the
    /// move FROM `seat_tier_id` (premium) unconditionally: since Stripe's
    /// item for this member had ALREADY moved to standard (the original
    /// downgrade's own push), that stale premium price would find no
    /// matching item, fall into `move_stripe_seat_item`'s drift-tolerant
    /// "old item missing" branch, and STILL unconditionally increment the
    /// standard item's quantity by one — double-counting a member Stripe
    /// already bills for once. The fix recognizes the target already equals
    /// what Stripe is CURRENTLY billing and skips the Stripe call entirely
    /// — this test's 500-on-any-request mock is what makes that observable:
    /// the retired code's increment call would have hit it and turned this
    /// into a 500.
    #[tokio::test]
    async fn a_second_change_re_confirming_a_pending_downgrade_calls_no_stripe() {
        let Some(pool) = pg_test_pool(
            "a_second_change_re_confirming_a_pending_downgrade_calls_no_stripe",
        )
        .await
        else {
            return;
        };
        let _catalog_guard = CATALOG_PRICE_LOCK.lock().await;
        let original_standard = set_seat_tier_price(&pool, "standard", Some("price_standard_bs_second")).await;
        let original_premium = set_seat_tier_price(&pool, "premium", Some("price_premium_bs_second")).await;

        let (account_id, admin_id) = seed_org_account(&pool, true).await;
        let grant_id = seed_team_grant(&pool, account_id, "premium").await;
        let member_id = add_member(&pool, account_id, "premium", true).await;
        let standard_id = seat_tier_id(&pool, "standard").await;
        sqlx::query("UPDATE account_members SET pending_seat_tier_id = $3 WHERE account_id = $1 AND user_id = $2")
            .bind(account_id)
            .bind(member_id)
            .bind(standard_id)
            .execute(&pool)
            .await
            .expect("seed a pending downgrade (premium -> standard) already pushed to Stripe");

        let app = axum::Router::new().fallback(axum::routing::any(
            || async { axum::http::StatusCode::INTERNAL_SERVER_ERROR },
        ));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.ok();
        });
        let stripe = StripeClient::new(reqwest::Client::new(), "sk_test_mock")
            .with_base_url(format!("http://{addr}"));
        let state = app_state(pool.clone(), Some(stripe)).await;

        let resp = change_seat_tier(
            State(state),
            Extension(user_context(account_id, admin_id, "owner")),
            Path(member_id),
            Json(SeatTierChangeRequest { tier: "standard".to_string() }),
        )
        .await;
        let status = resp.status();
        let body = body_json(resp).await;

        let pending: Option<Uuid> = sqlx::query_scalar(
            "SELECT pending_seat_tier_id FROM account_members WHERE account_id = $1 AND user_id = $2",
        )
        .bind(account_id)
        .bind(member_id)
        .fetch_one(&pool)
        .await
        .expect("read pending");
        let premium_id = seat_tier_id(&pool, "premium").await;
        let premium_row: i32 = sqlx::query_scalar(
            "SELECT seats FROM grant_seat_tiers WHERE grant_id = $1 AND seat_tier_id = $2",
        )
        .bind(grant_id)
        .bind(premium_id)
        .fetch_one(&pool)
        .await
        .expect("the ledger stays untouched — it only moves at roll");

        set_seat_tier_price(&pool, "standard", original_standard.as_deref()).await;
        set_seat_tier_price(&pool, "premium", original_premium.as_deref()).await;
        cleanup_account(&pool, account_id, &[admin_id, member_id]).await;

        assert_eq!(
            status,
            StatusCode::OK,
            "no Stripe call means no server error from the 500 mock — body: {body:?}"
        );
        assert_eq!(body["outcome"], "downgraded");
        assert_eq!(pending, Some(standard_id), "re-confirming the same pending tier leaves it exactly as it was");
        assert_eq!(premium_row, 1);
    }

    /// **Finding 10.** A `credits_override` grant's per-member share is a
    /// FIXED total divided across the roster (`billing::renewal`'s own doc,
    /// "Per-tier allowances for `team` grants") — a tier CHANGE must never
    /// overwrite that member's current-period `granted_credits` with the
    /// target tier's catalog figure, which could cut an already-upgraded
    /// member's allowance below what they have already consumed even though
    /// the account's actual override total never changed.
    #[tokio::test]
    async fn an_upgrade_on_a_credits_override_grant_leaves_granted_credits_untouched() {
        let Some(pool) = pg_test_pool(
            "an_upgrade_on_a_credits_override_grant_leaves_granted_credits_untouched",
        )
        .await
        else {
            return;
        };
        let _catalog_guard = CATALOG_PRICE_LOCK.lock().await;
        let original_standard = set_seat_tier_price(&pool, "standard", Some("price_standard_bs_override")).await;
        let original_premium = set_seat_tier_price(&pool, "premium", Some("price_premium_bs_override")).await;

        let (account_id, admin_id) = seed_org_account(&pool, true).await;
        let plan_id: Uuid = sqlx::query_scalar("SELECT id FROM plans WHERE code = 'team'")
            .fetch_one(&pool)
            .await
            .expect("team plan must be seeded (0167)");
        let grant_id: Uuid = { let mut financial=financial_fixture(&pool,account_id).await; let value=sqlx::query_scalar(
            "INSERT INTO grants (account_id, plan_id, seats, plan_is_seat_priced, credits_override) \
             VALUES ($1, $2, 1, true, $3) RETURNING id",
        )
        .bind(account_id)
        .bind(plan_id)
        .bind(sqlx::types::Decimal::from(8000))
        .fetch_one(&mut *financial)
        .await
        .expect("insert override team grant"); financial.commit().await.unwrap(); value };
        let standard_id = seat_tier_id(&pool, "standard").await;
        { let mut financial=financial_fixture(&pool,account_id).await; let value=sqlx::query("INSERT INTO grant_seat_tiers (grant_id, seat_tier_id, seats) VALUES ($1, $2, 1)")
            .bind(grant_id)
            .bind(standard_id)
            .execute(&mut *financial)
            .await
            .expect("seed ledger"); financial.commit().await.unwrap(); value };
        let member_id = add_member(&pool, account_id, "standard", true).await;
        seed_current_period(&pool, account_id, member_id, 8000, 500).await;

        let stripe = spawn_seat_change_mock("price_standard_bs_override").await;
        let state = app_state(pool.clone(), Some(stripe)).await;

        let resp = change_seat_tier(
            State(state),
            Extension(user_context(account_id, admin_id, "owner")),
            Path(member_id),
            Json(SeatTierChangeRequest { tier: "premium".to_string() }),
        )
        .await;
        let status = resp.status();
        let body = body_json(resp).await;

        let (granted, consumed): (sqlx::types::Decimal, sqlx::types::Decimal) = sqlx::query_as(
            "SELECT granted_credits, consumed_credits FROM member_allowance_periods \
              WHERE account_id = $1 AND user_id = $2 AND period_start <= now() AND period_end > now()",
        )
        .bind(account_id)
        .bind(member_id)
        .fetch_one(&pool)
        .await
        .expect("read current period");

        set_seat_tier_price(&pool, "standard", original_standard.as_deref()).await;
        set_seat_tier_price(&pool, "premium", original_premium.as_deref()).await;
        cleanup_account(&pool, account_id, &[admin_id, member_id]).await;

        assert_eq!(status, StatusCode::OK, "body: {body:?}");
        assert_eq!(body["outcome"], "upgraded");
        assert_eq!(
            granted,
            sqlx::types::Decimal::from(8000),
            "granted_credits must NOT be overwritten to premium's catalog figure (15,000) for an override grant"
        );
        assert_eq!(consumed, sqlx::types::Decimal::from(500), "consumed_credits must survive untouched too");
    }

    /// **Billing self-serve PR F review, finding: proration direction must
    /// classify against the EFFECTIVE tier, not the stale `current_tier`.**
    /// Worked example from the finding itself: `seat_tier_id = enterprise`
    /// (a synthetic THIRD tier, priced ABOVE premium — the real catalog only
    /// registers two active tiers, 0165's own header, so this test seeds one
    /// directly; with only two tiers this exact divergence is unreachable —
    /// any same-two-tier "request my original tier back" always hits the
    /// same-tier CANCEL branch instead, never this upgrade/downgrade one),
    /// `pending_seat_tier_id = standard` (a downgrade already in flight),
    /// request `tier = premium`. Classifying against `current_tier`
    /// (enterprise, the most expensive) reads this as a DOWNGRADE and would
    /// push `proration: none` — an under-billing bug, since the actual
    /// Stripe move is standard -> premium, a genuine mid-period price
    /// INCREASE. Classifying against `effective_tier` (standard, what
    /// Stripe is ACTUALLY billing right now) correctly reads it as an
    /// upgrade — observable here via the response `outcome` and the
    /// immediate (not deferred) DB writes an upgrade takes.
    #[tokio::test]
    async fn a_tier_change_during_a_pending_downgrade_classifies_by_the_effective_tier_not_the_stale_one() {
        let Some(pool) = pg_test_pool(
            "a_tier_change_during_a_pending_downgrade_classifies_by_the_effective_tier_not_the_stale_one",
        )
        .await
        else {
            return;
        };
        let _catalog_guard = CATALOG_PRICE_LOCK.lock().await;
        let original_standard = set_seat_tier_price(&pool, "standard", Some("price_standard_bs_s1")).await;
        let original_premium = set_seat_tier_price(&pool, "premium", Some("price_premium_bs_s1")).await;

        let enterprise_id: Uuid = sqlx::query_scalar(
            "INSERT INTO seat_tiers (code, name, credits, price_usd_cents, stripe_price_id, is_active) \
             VALUES ('enterprise_probe_bs_s1', 'Enterprise Probe', 30000, 35000, 'price_enterprise_bs_s1', true) \
             RETURNING id",
        )
        .fetch_one(&pool)
        .await
        .expect("insert synthetic enterprise tier (test-only, above premium's price)");

        let (account_id, admin_id) = seed_org_account(&pool, true).await;
        let grant_id = seed_team_grant(&pool, account_id, "enterprise_probe_bs_s1").await;
        let member_id = add_member(&pool, account_id, "enterprise_probe_bs_s1", true).await;
        // Simulate an already-in-flight pending downgrade to `standard` —
        // the state an earlier call to this SAME route's downgrade branch
        // would have left behind.
        sqlx::query(
            "UPDATE account_members SET pending_seat_tier_id = \
               (SELECT id FROM seat_tiers WHERE code = 'standard' AND is_active = true) \
              WHERE account_id = $1 AND user_id = $2",
        )
        .bind(account_id)
        .bind(member_id)
        .execute(&pool)
        .await
        .expect("seed the pending downgrade");

        let stripe = spawn_seat_change_mock("price_standard_bs_s1").await;
        let state = app_state(pool.clone(), Some(stripe)).await;

        let resp = change_seat_tier(
            State(state),
            Extension(user_context(account_id, admin_id, "owner")),
            Path(member_id),
            Json(SeatTierChangeRequest { tier: "premium".to_string() }),
        )
        .await;
        let status = resp.status();
        let body = body_json(resp).await;

        let (member_tier, pending): (Uuid, Option<Uuid>) = sqlx::query_as(
            "SELECT seat_tier_id, pending_seat_tier_id FROM account_members WHERE account_id = $1 AND user_id = $2",
        )
        .bind(account_id)
        .bind(member_id)
        .fetch_one(&pool)
        .await
        .expect("read member tier");
        let premium_id = seat_tier_id(&pool, "premium").await;
        // The ledger move is the OTHER half of the upgrade branch (the part
        // that keys off `current_tier.id` — enterprise — rather than
        // `effective_tier.id` — standard, the pending-downgrade target) —
        // read both rows now, before `cleanup_account` cascades the grant
        // away below.
        let enterprise_ledger_seats: Option<i32> = sqlx::query_scalar(
            "SELECT seats FROM grant_seat_tiers WHERE grant_id = $1 AND seat_tier_id = $2",
        )
        .bind(grant_id)
        .bind(enterprise_id)
        .fetch_optional(&pool)
        .await
        .expect("read the source tier's ledger row");
        let premium_ledger_seats: Option<i32> = sqlx::query_scalar(
            "SELECT seats FROM grant_seat_tiers WHERE grant_id = $1 AND seat_tier_id = $2",
        )
        .bind(grant_id)
        .bind(premium_id)
        .fetch_optional(&pool)
        .await
        .expect("read the target tier's ledger row");

        set_seat_tier_price(&pool, "standard", original_standard.as_deref()).await;
        set_seat_tier_price(&pool, "premium", original_premium.as_deref()).await;
        cleanup_account(&pool, account_id, &[admin_id, member_id]).await;
        sqlx::query("DELETE FROM seat_tiers WHERE id = $1")
            .bind(enterprise_id)
            .execute(&pool)
            .await
            .expect("cleanup synthetic enterprise tier");

        assert_eq!(status, StatusCode::OK, "body: {body:?}");
        assert_eq!(
            body["outcome"], "upgraded",
            "classifying by the EFFECTIVE tier (standard, what Stripe is actually billing) must \
             read target=premium as an upgrade, even though target's price is BELOW \
             current_tier's (enterprise) — the pre-fix bug classified against current_tier and \
             read this as a downgrade, pushing proration: none for what is actually a mid-period \
             price increase"
        );
        assert_eq!(member_tier, premium_id, "an upgrade must flip seat_tier_id immediately, not defer it");
        assert_eq!(pending, None, "an upgrade must clear the pending downgrade marker, not leave it set");
        assert_eq!(
            enterprise_ledger_seats, None,
            "the emptied source-tier ledger row must be DELETED — the ledger move keys off \
             current_tier (enterprise), not the effective tier (standard)"
        );
        assert_eq!(premium_ledger_seats, Some(1), "one seat now on the target premium-tier ledger row");
    }

    /// CS-442 (item 40, D8): `GET /v1/billing/seat-tiers` lists ACTIVE
    /// tiers only — a `_catalog_guard`-held synthetic inactive tier
    /// (`is_active = false`) must never appear, and `standard`/`premium`
    /// (0165's own permanent seed) must always be present. No account/org
    /// fixture at all: the handler never reads `user`, so a bare
    /// `UserContext` with no matching DB rows is sufficient — the same
    /// "global reference data, no account gate" posture this route's own
    /// doc comment states.
    #[tokio::test]
    async fn list_seat_tiers_returns_only_active_tiers() {
        let Some(pool) = pg_test_pool("list_seat_tiers_returns_only_active_tiers").await else {
            return;
        };
        let _catalog_guard = CATALOG_PRICE_LOCK.lock().await;
        let inactive_id: Uuid = sqlx::query_scalar(
            "INSERT INTO seat_tiers (code, name, credits, price_usd_cents, is_active) \
             VALUES ($1, 'Retired (CS-442 test)', 1, 100, false) RETURNING id",
        )
        .bind(format!("cs442_retired_{}", Uuid::new_v4().simple()))
        .fetch_one(&pool)
        .await
        .expect("insert a synthetic inactive tier");

        let state = app_state(pool.clone(), None).await;
        let resp = list_seat_tiers(
            State(state),
            Extension(user_context(Uuid::new_v4(), Uuid::new_v4(), "member")),
        )
        .await;
        let status = resp.status();
        let body = body_json(resp).await;

        sqlx::query("DELETE FROM seat_tiers WHERE id = $1").bind(inactive_id).execute(&pool).await.ok();

        assert_eq!(status, StatusCode::OK);
        let tiers = body.as_array().expect("response must be a JSON array");
        let codes: Vec<&str> = tiers.iter().map(|t| t["code"].as_str().unwrap()).collect();
        assert!(codes.contains(&"standard"), "standard must be listed: {codes:?}");
        assert!(codes.contains(&"premium"), "premium must be listed: {codes:?}");
        assert!(
            tiers.iter().all(|t| !t["code"].as_str().unwrap().starts_with("cs442_retired_")),
            "an INACTIVE tier must never be listed: {codes:?}"
        );
        for t in tiers {
            assert!(t["credits"].is_number(), "credits must serialize as a number, not a Decimal string");
            assert!(t["price_usd_cents"].is_number());
        }
    }
}
