//! `GET /v1/orgs/slots` / `POST /v1/orgs/slots` / `POST /v1/orgs/slots/reduce`
//! — CS-435 (org-onboarding plan §3 item 7, D1/D7/D15) plus CS-444: the admin
//! "buy N slots" and "reduce N slots" actions, and the arithmetic they (and
//! the members page) read back. Availability is [`crate::billing::
//! slot_pool::SlotPool`], never a stored count — see that module's own doc
//! comment for D7's full "derived, never stored" framing.
//!
//! # CS-444 — this module is now the ONLY thing that moves `purchased`
//!
//! Founder decision, 2026-09-22: *"The pool only changes when an admin
//! explicitly adds or reduces slots."* Until then, the only thing that ever
//! SHRANK an org's bill was `billing::seat_lifecycle::release_seat`
//! un-purchasing a departing member's seat — which meant an org that bought
//! 10 slots up front dropped to 9 the moment anyone left. That is gone (see
//! that function's own doc comment): a removed member's slot now returns to
//! the pool, `purchased` untouched. Both halves of the founder's sentence
//! have to exist for it to be true, so this module supplies the second one:
//! without [`reduce_slots`], an org's bill would be a one-way ratchet that
//! nothing in the product could ever walk back.
//!
//! [`purchase_slots`] and [`reduce_slots`] are deliberate mirrors — same
//! admin guard, same personal-org refusal, same client-supplied idempotency
//! key folded into an account/action-namespaced recipe, same tier lookup,
//! same Stripe-then-ledger ordering and the same failure semantics for the
//! window between them. They differ in exactly three places, each documented
//! at its own site: the proration behaviour, which statuses they refuse, and
//! the floor [`reduce_slots`] will not go below.
//!
//! # Who can read, who can write
//!
//! Both `POST`s (they move money) are admin-only, via the SAME
//! [`require_org_admin`] gate `routes::org_invites`' own invite routes use,
//! plus a personal-org refusal (see below) — a personal org's single member
//! always holds `role = "owner"` and would otherwise sail past that check,
//! the identical trap `routes::checkout::require_purchase_authority`'s own
//! doc comment names for the seat-tier route.
//!
//! `GET` is any authenticated ORG member, not admin-only.
//! `web/app/(app)/settings/organization/members/page.tsx` renders its slot
//! Meter (sourced from this route, `used`/`purchased`) for every viewer of
//! that page — the page is not wrapped in an admin-only gate client-side
//! (`rosterForbidden` degrades gracefully rather than the whole page
//! refusing to render for a plain member), only the "Add member"/invite/
//! "Add slots" actions are admin-gated. This route's own read matches that
//! bar: `tenant.org_context()` present is enough, no `is_admin` check. A
//! personal org still refuses (see below) — there is nothing to read.
//!
//! # Personal orgs refuse (403), not 400
//!
//! Unlike `routes::checkout::require_purchase_authority`'s `org_only` flag
//! (400 for a personal-account caller), every route here refuses a personal
//! org with 403: a personal org's `TenantContext` IS populated
//! (`require_org_admin` alone does not exclude it — see that function's own
//! doc comment, and `routes::checkout`'s own "`accounts.is_org_account`
//! cannot tell the two apart" note), so this module additionally checks
//! [`crate::store::Store::account_is_real_org`], the same helper `checkout`
//! uses, and refuses outright rather than 400ing a request whose shape was
//! otherwise fine — a personal org's single, always-`owner` member has no
//! "admin" to be one of, so 403 ("not an admin of anything to manage") reads
//! more honestly here than a generic 400 would.

use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::{Extension, Json};
use serde::{Deserialize, Serialize};
use serde_json::json;
use utoipa::path as utoipa_path;
use utoipa::ToSchema;
use uuid::Uuid;

use crate::billing::stripe_client::Idem;
use crate::middleware::auth::UserContext;
use crate::tenancy::extractor::Tenant;
use crate::tenancy::runtime::TenantContext;
use crate::AppState;

use super::org_invites::{map_stripe_error_to_response, require_org_admin};

// ── Response/error helpers (module convention — see `org_invites.rs`,
//    `billing_seats.rs`; this crate has no central `ApiError`) ─────────────

fn bad_request(msg: &str) -> Response {
    (StatusCode::BAD_REQUEST, Json(json!({"error": msg}))).into_response()
}
fn forbidden(msg: &str) -> Response {
    (StatusCode::FORBIDDEN, Json(json!({"error": msg}))).into_response()
}
fn conflict(msg: &str) -> Response {
    (StatusCode::CONFLICT, Json(json!({"error": msg}))).into_response()
}
fn service_unavailable() -> Response {
    (StatusCode::SERVICE_UNAVAILABLE, Json(json!({"error": "service unavailable"}))).into_response()
}
fn internal_error(what: &str, e: impl std::fmt::Display) -> Response {
    tracing::error!(error = %e, step = what, "org_slots: failed");
    (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": "internal error"}))).into_response()
}

/// The minimum quantity a single "buy N slots"/"reduce N slots" call may
/// request. Zero is refused rather than treated as a no-op: a client that
/// computed a zero delta should not be sending the request at all, and
/// accepting it would push a pointless Stripe write.
const MIN_QUANTITY: i64 = 1;
/// The maximum — an arbitrary but generous ceiling against a fat-fingered
/// `quantity`, same defensive posture `routes::checkout`'s own request
/// validation takes for its own quantity-shaped fields. [`reduce_slots`] has
/// a second, tighter ceiling of its own (the tier's free slots) that this
/// one only backstops.
const MAX_QUANTITY: i64 = 500;

#[derive(Debug, Deserialize, ToSchema)]
pub struct PurchaseSlotsRequest {
    /// `seat_tiers.code` — e.g. `"standard"`/`"premium"`.
    pub tier: String,
    /// How many slots to add, `1..=500`.
    pub quantity: i64,
    /// Client-supplied idempotency key — a fresh UUID PER LOGICAL PURCHASE,
    /// reused only across retries of the SAME attempt (a dropped response,
    /// a client timeout-and-retry). Folded into the Stripe idempotency key
    /// this route builds (`slot:{account_id}:buy:{tier}:{idempotency_key}`),
    /// namespaced so the same client key can never collide across accounts
    /// or tiers. Unlike `billing::seat_lifecycle`'s own internal recipe
    /// (a FRESH `Uuid::new_v4()` minted once per call, never client-visible
    /// — see that module's own "Idempotency-key recipes" doc section), this
    /// key is caller-controlled BY DESIGN: only the caller can tell "this is
    /// a retry of the request I already sent" apart from "this is a genuinely
    /// new purchase", the same distinction any ordinary payment-API
    /// idempotency key exists to let the CLIENT make.
    ///
    /// **Known, accepted gap, stated rather than solved (matching
    /// `routes::checkout::refuse_if_already_subscribed`'s own doc comment's
    /// posture for a comparably narrow race):** if a caller reuses this key
    /// for a request Stripe has already fully processed AND this route's own
    /// prior call already committed its local `grant_seat_tiers`
    /// increment, Stripe's own idempotency cache answers the REPLAYED
    /// response (no second charge, no second quantity bump) but this
    /// route's local ledger write is not itself keyed on
    /// `idempotency_key` and would increment a SECOND time. Narrower than
    /// it sounds: it requires the FIRST call to have already round-tripped
    /// successfully (both Stripe and the local commit) before the SAME key
    /// is reused, which is not what a genuine retry (of a dropped/timed-out
    /// request) looks like — a genuine retry's first attempt never
    /// committed anything locally to double. A real fix needs a persisted
    /// per-key dedup table; out of scope for this PR.
    pub idempotency_key: String,
}

/// CS-444 — the mirror of [`PurchaseSlotsRequest`]. Same three fields, same
/// meanings; see that type's own docs for the `idempotency_key` contract and
/// its one stated, accepted gap (which applies here identically: a key reused
/// after a fully-round-tripped first call replays Stripe's cached response
/// while this route's local ledger write, not itself keyed on the key, runs a
/// second time — for a REDUCE that direction is under-counting the pool
/// rather than over-billing, but it is the same unfixed gap and wants the
/// same fix, a persisted per-key dedup table).
#[derive(Debug, Deserialize, ToSchema)]
pub struct ReduceSlotsRequest {
    /// `seat_tiers.code` — e.g. `"standard"`/`"premium"`. A reduce is always
    /// per-tier: an admin gives back slots of a tier they actually bought,
    /// and the refusal below is computed against THAT tier's own pool.
    pub tier: String,
    /// How many slots to give back, `1..=500` — and, separately, never more
    /// than that tier's currently FREE slots (see [`reduce_slots`]).
    pub quantity: i64,
    /// Client-supplied idempotency key, folded into
    /// `slot:{account_id}:reduce:{tier}:{idempotency_key}`. The `reduce`
    /// segment is what keeps a client that reuses one key across both
    /// actions from having its reduce answered out of its purchase's cached
    /// Stripe response.
    pub idempotency_key: String,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct TierBreakdown {
    pub tier: String,
    pub seats: i64,
}

/// CS-442 (item 40, D8): one active tier's own purchased/used/reserved/
/// available — the
/// [`crate::billing::slot_pool::read_slot_pool_for_tier`] snapshot
/// (the ADMIN-REPORT view, Stripe-effective `used` — never the capacity-gate
/// [`crate::billing::slot_pool::read_slot_pool_for_tier_ledger`] one; see
/// that module's own "CS-442" doc section for why the two must stay
/// separate), presentation-shaped for this HTTP surface. Additive alongside
/// [`TierBreakdown`] (which only ever carried `purchased`, under the ledger's
/// own name `seats`) — kept, not replaced, so an already-shipped client
/// reading `tier_breakdown` is unaffected.
#[derive(Debug, Serialize, ToSchema)]
pub struct TierPool {
    pub code: String,
    pub purchased: i64,
    pub used: i64,
    pub reserved: i64,
    pub available: i64,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct SlotPoolResponse {
    pub purchased: i64,
    pub used: i64,
    pub reserved: i64,
    pub available: i64,
    pub tier_breakdown: Vec<TierBreakdown>,
    /// CS-442 (item 40, D8): one entry per ACTIVE `seat_tiers` row, each
    /// tier's own `purchased`/`used`/`reserved`/`available` — see
    /// `billing::slot_pool`'s own "CS-442" doc section for the exact
    /// per-column rule this reads through. The top-level fields above stay
    /// the account-WIDE total (unchanged, the members-page meter's own
    /// read) — this is additive, not a replacement.
    pub tiers: Vec<TierPool>,
}

/// `pool` plus a per-tier breakdown, one extra read (`grant_seat_tiers` for
/// the current `team` grant) beyond what `billing::slot_pool::SlotPool`
/// itself carries — kept HERE, not in `billing::slot_pool`, because it is
/// presentation shape for this one HTTP surface, not arithmetic every other
/// caller (`acquire_seat`'s consume-before-buy branch, the expiry sweep)
/// needs.
pub(crate) async fn slot_pool_response(
    conn: &mut sqlx::PgConnection,
    account_id: Uuid,
) -> Result<SlotPoolResponse, sqlx::Error> {
    let pool = crate::billing::slot_pool::read_slot_pool(conn, account_id).await?;
    let grant_id: Option<Uuid> = sqlx::query_scalar(
        "SELECT g.id FROM grants g JOIN plans p ON p.id = g.plan_id \
          WHERE g.account_id = $1 AND g.revoked_at IS NULL AND p.code = $2 \
          ORDER BY g.granted_at DESC, g.id DESC LIMIT 1",
    )
    .bind(account_id)
    .bind(crate::billing::MIXED_TIER_PLAN_CODE)
    .fetch_optional(&mut *conn)
    .await?;
    let tier_breakdown = match grant_id {
        Some(grant_id) => {
            sqlx::query_as::<_, (String, i32)>(
                "SELECT st.code, gst.seats FROM grant_seat_tiers gst \
                  JOIN seat_tiers st ON st.id = gst.seat_tier_id \
                 WHERE gst.grant_id = $1 ORDER BY st.code",
            )
            .bind(grant_id)
            .fetch_all(&mut *conn)
            .await?
            .into_iter()
            .map(|(tier, seats)| TierBreakdown { tier, seats: i64::from(seats) })
            .collect()
        }
        None => Vec::new(),
    };
    // CS-442 (item 40, D8): every ACTIVE tier, not just ones with a
    // `grant_seat_tiers` row — a tier nobody has purchased yet must still
    // show `purchased: 0` (an admin choosing it on the approval row wants
    // to see "nothing bought here yet", not have the tier silently absent).
    let active_tiers: Vec<(Uuid, String)> =
        sqlx::query_as("SELECT id, code FROM seat_tiers WHERE is_active = true ORDER BY code")
            .fetch_all(&mut *conn)
            .await?;
    let mut tiers = Vec::with_capacity(active_tiers.len());
    for (tier_id, code) in active_tiers {
        // CodeRabbit (PR #1028): the ADMIN-REPORT view (Stripe-effective
        // `used`), never the capacity-gate one — see
        // `billing::slot_pool`'s own "CS-442" doc section.
        let tier_pool = crate::billing::slot_pool::read_slot_pool_for_tier(conn, account_id, tier_id).await?;
        tiers.push(TierPool {
            code,
            purchased: tier_pool.purchased,
            used: tier_pool.used,
            reserved: tier_pool.reserved,
            available: tier_pool.available(),
        });
    }
    Ok(SlotPoolResponse {
        purchased: pool.purchased,
        used: pool.used,
        reserved: pool.reserved,
        available: pool.available(),
        tier_breakdown,
        tiers,
    })
}

/// Refuses a personal org (403) — see this module's own doc comment for why
/// [`require_org_admin`] alone cannot tell one apart from a real org.
async fn refuse_personal_org(state: &AppState, ctx: &TenantContext) -> Result<(), Response> {
    let Some(store) = state.control_store() else {
        return Err(service_unavailable());
    };
    match store.account_is_real_org(ctx.account_id, ctx.user_id).await {
        Ok(true) => Ok(()),
        Ok(false) => Err(forbidden("personal accounts have no slot pool")),
        Err(e) => Err(internal_error("account lookup", e)),
    }
}

// ── GET /v1/orgs/slots ───────────────────────────────────────────────────

#[utoipa_path(
    get,
    path = "/v1/orgs/slots",
    tag = "billing",
    security(("bearer_token" = [])),
    responses(
        (status = 200, description = "The account's current slot pool", body = SlotPoolResponse),
        (status = 403, description = "Not an org context, or a personal org"),
        (status = 503, description = "Database unavailable"),
    )
)]
pub async fn get_slots(
    State(state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
) -> Response {
    // Read-only: any org member, not admin-only — see this module's own doc
    // comment. `require_org_admin` is deliberately NOT called here; only its
    // `org_context()` half is needed.
    let Some(ctx) = tenant.org_context() else {
        return forbidden("this endpoint is only available in an organization workspace");
    };
    if let Err(resp) = refuse_personal_org(&state, ctx).await {
        return resp;
    }
    let Some(control) = state.control_db() else {
        return service_unavailable();
    };
    let account_id = ctx.account_id;
    let _ = &user; // authenticated (`require_auth`'s own gate); no further use here.

    let mut tx = match crate::tenancy::scope::begin_request_account_read(control, account_id).await {
        Ok(tx) => tx,
        Err(e) => return internal_error("scope", e),
    };
    let body = match slot_pool_response(&mut tx, account_id).await {
        Ok(body) => body,
        Err(e) => {
            let _ = tx.rollback().await;
            return internal_error("slot pool read", e);
        }
    };
    if let Err(e) = tx.commit().await {
        return internal_error("commit", e);
    }
    (StatusCode::OK, Json(body)).into_response()
}

// ── POST /v1/orgs/slots ──────────────────────────────────────────────────

#[utoipa_path(
    post,
    path = "/v1/orgs/slots",
    tag = "billing",
    security(("bearer_token" = [])),
    request_body = PurchaseSlotsRequest,
    responses(
        (status = 200, description = "Slots purchased; the account's updated pool", body = SlotPoolResponse),
        (status = 400, description = "Unknown/inactive tier, or quantity out of range (1..=500)"),
        (status = 403, description = "Not an org admin, or a personal org"),
        (status = 409, description = "Frozen subscription, or no active team subscription"),
        (status = 502, description = "Transient Stripe/upstream failure"),
        (status = 503, description = "Database, Stripe, or Clerk unavailable"),
    )
)]
pub async fn purchase_slots(
    State(state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Json(body): Json<PurchaseSlotsRequest>,
) -> Response {
    let account=user.account_id;
    let response=purchase_slots_committed(State(state.clone()),tenant,Extension(user),Json(body)).await;
    crate::read_models::dirty::after_control_response(&state,account,crate::read_models::dirty::DirtyEvent::BillingChanged,response).await
}

async fn purchase_slots_committed(
    State(state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Json(body): Json<PurchaseSlotsRequest>,
) -> Response {
    let ctx = match require_org_admin(&tenant, &user) {
        Ok(c) => c,
        Err(r) => return r,
    };
    if let Err(resp) = refuse_personal_org(&state, ctx).await {
        return resp;
    }
    if !(MIN_QUANTITY..=MAX_QUANTITY).contains(&body.quantity) {
        return bad_request("quantity must be between 1 and 500");
    }
    if body.idempotency_key.trim().is_empty() {
        return bad_request("idempotency_key is required");
    }

    let Some(control) = state.control_db() else {
        return service_unavailable();
    };
    let Some(stripe) = state.stripe.as_ref() else {
        return service_unavailable();
    };
    let account_id = ctx.account_id;

    if crate::billing::enforcement::subscription_frozen(control, account_id).await {
        return conflict("subscription inactive — slots cannot be purchased while frozen");
    }

    // Same "most recently granted, non-revoked" `team` grant + linked
    // subscription lookup `routes::billing_seats::change_seat_tier` uses —
    // read-only, its own short-lived scoped transaction, committed
    // immediately, exactly like that route's own comment explains for the
    // identical shape.
    let team_grant: Option<(Uuid, String)> =
        match crate::tenancy::scope::begin_account_scoped(control, account_id).await {
            Ok(mut scoped_tx) => {
                let row = sqlx::query_as(
                    "SELECT g.id, sub.stripe_subscription_id FROM grants g \
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
    let Some((grant_id, subscription_id)) = team_grant else {
        return conflict("this account has no active team subscription — slots can only be purchased for team accounts");
    };

    let tier: Option<(Uuid, String, Option<String>)> = match sqlx::query_as(
        "SELECT id, code, stripe_price_id FROM seat_tiers WHERE code = $1 AND is_active = true",
    )
    .bind(&body.tier)
    .fetch_optional(control)
    .await
    {
        Ok(row) => row,
        Err(e) => return internal_error("seat tier lookup", e),
    };
    let Some((tier_id, tier_code, stripe_price_id)) = tier else {
        return bad_request("unknown or inactive seat tier");
    };
    let Some(price_id) = stripe_price_id else {
        return internal_error(
            "tier price lookup",
            format!("seat tier {tier_code} has no stripe_price_id"),
        );
    };

    // Billing self-serve PR F's S2 discipline, applied here too: the lock
    // and the transaction are taken FIRST and held across the Stripe call —
    // see `billing::seat_lifecycle`'s own module doc, "S2" section, for why.
    let mut tx = match crate::tenancy::scope::begin_account_scoped(control, account_id).await {
        Ok(tx) => tx,
        Err(e) => return internal_error("scope", e),
    };
    if let Err(e) = crate::billing::renewal::lock_period_rotation(&mut tx, account_id).await {
        let _ = tx.rollback().await;
        return internal_error("lock", e);
    }

    let idem_key = format!(
        "slot:{account_id}:buy:{tier_code}:{}",
        body.idempotency_key.trim()
    );
    let push = crate::billing::seat_lifecycle::with_stripe_deadline(async {
        let sub = stripe.retrieve_subscription(&subscription_id).await?;
        match sub.items.iter().find(|it| it.price_id == price_id) {
            Some(item) => {
                stripe
                    .update_subscription_item(
                        &item.id,
                        item.quantity + body.quantity,
                        "create_prorations",
                        Idem::Key(&idem_key),
                    )
                    .await
            }
            None => {
                stripe
                    .add_subscription_item(
                        &subscription_id,
                        &price_id,
                        body.quantity,
                        "create_prorations",
                        Idem::Key(&idem_key),
                    )
                    .await
            }
        }
    })
    .await;
    if let Err(e) = push {
        let _ = tx.rollback().await;
        return map_stripe_error_to_response(e);
    }

    // Stripe succeeded — mirror it locally, in the SAME locked transaction.
    // No Stripe push happens below; nothing here can fail Stripe-side.
    if let Err(e) = sqlx::query(
        "INSERT INTO grant_seat_tiers (grant_id, seat_tier_id, seats) VALUES ($1, $2, $3) \
         ON CONFLICT (grant_id, seat_tier_id) DO UPDATE SET seats = grant_seat_tiers.seats + $3",
    )
    .bind(grant_id)
    .bind(tier_id)
    .bind(body.quantity as i32)
    .execute(&mut *tx)
    .await
    {
        return internal_error("grant_seat_tiers write (Stripe already pushed — drift, not a lost purchase)", e);
    }
    if let Err(e) = crate::billing::renewal::resync_grant_seats(&mut tx, grant_id).await {
        return internal_error("resync_grant_seats (Stripe already pushed — drift, not a lost purchase)", e);
    }

    if let Err(e) = tx.commit().await {
        return internal_error("commit", e);
    }

    // Item 6 fix, CS-449 review round 2: read the presentation snapshot
    // BEFORE the drain (below) runs, not after. Presentation-only — the
    // mirror is already durable — and a read failure here must not roll
    // back `grant_seat_tiers`/`resync_grant_seats` after Stripe's quantity
    // has already been raised.
    //
    // Ordering is the fix. The drain can immediately re-spend what this
    // purchase just bought against a `pool_full` backlog; a read taken
    // AFTER it ran (as this used to) would report the admin's own purchase
    // as `available: 0` with nothing on the response explaining why — "I
    // bought 5 slots and got 0 back". This reads the pool as of right after
    // THIS purchase, before anything else has touched it, which is the
    // admin's own action's own outcome; the drain's effect, if any, is a
    // separate, later event the next `GET /v1/orgs/slots` reflects once it
    // has run.
    let mut read_tx = match crate::tenancy::scope::begin_account_scoped(control, account_id).await {
        Ok(tx) => tx,
        Err(e) => return internal_error("slot pool read (scope)", e),
    };
    let response_body = match slot_pool_response(&mut read_tx, account_id).await {
        Ok(body) => body,
        Err(e) => {
            let _ = read_tx.rollback().await;
            return internal_error("slot pool read (purchase already committed)", e);
        }
    };
    let _ = read_tx.commit().await;

    // CS-449 (item 38's drain; item 6's OTHER half, CS-449 review round 2):
    // this purchase just grew `tier_id`'s own pool (`grant_seat_tiers`,
    // committed above) — exactly the kind of slot freeing `routes::
    // org_join_requests::drain_org`'s own doc comment names.
    //
    // SPAWNED, not awaited. A drain can make N real Clerk membership-
    // creation round trips, and this is a synchronous HTTP response an
    // admin's own browser is waiting on, AFTER money has already moved
    // (Stripe's quantity is raised, the local mirror is committed) — the
    // drain must not add its own latency to that response. Concretely: a
    // slow drain here is exactly what invites a client-side timeout and
    // retry against a route that already charged Stripe (the retry is
    // itself idempotent via `idem_key`, but there is no reason to invite it
    // at all). Best-effort either way, spawned or awaited: a purchase
    // succeeds on its own terms whether or not the drain that follows it
    // does. Unlike `reduce_slots` (below), which must NEVER trigger this —
    // see that route's own doc comment.
    let drain_state = state.clone();
    let drain_control = control.clone();
    let drain_org_id = ctx.org_id.clone();
    //
    // Self-serve signup A2 (S4): Clerk's member cap follows the purchased
    // seats for a self-serve org (a no-op for any other org). #1540 SF2: the
    // cap is raised BEFORE the drain runs (same task, awaited), or the
    // drain's Clerk membership creates would race it and be refused at the
    // old cap.
    tokio::spawn(async move {
        let clerk = crate::clerk_admin::ClerkAdminClient::from_env();
        let drain_report = crate::routes::orgs_create::sync_cap_then(
            &drain_control,
            clerk.as_ref(),
            account_id,
            crate::routes::org_join_requests::drain_org(&drain_state, &drain_control, account_id, &drain_org_id),
        )
        .await;
        if !drain_report.admitted.is_empty() {
            tracing::info!(
                %account_id, admitted = drain_report.admitted.len(),
                "auto_add: a slot purchase grew the pool and the drain admitted from the pool_full backlog"
            );
        }
    });

    (StatusCode::OK, Json(response_body)).into_response()
}

// ── POST /v1/orgs/slots/reduce ───────────────────────────────────────────

/// `seat_tier_id`'s pool as the CAPACITY GATE reads it — the whole
/// [`crate::billing::slot_pool::SlotPool`], not just the one number the
/// refusal needs.
///
/// **Returning the snapshot rather than `available()` is deliberate**
/// (CodeRabbit round one, #1030). The gate uses `available()`, but the
/// Stripe push a few lines later needs `purchased` too: comparing the
/// subscription item's `quantity` against this tier's ledgered purchase
/// count is the only place a per-tier Stripe/ledger divergence is
/// observable on a path that can still act on it. Collapsing the snapshot to
/// a scalar here is what made that comparison impossible to write.
///
/// **Deliberately [`crate::billing::slot_pool::read_slot_pool_for_tier_ledger`],
/// NOT `read_slot_pool_for_tier`.** `billing::slot_pool`'s own "CS-442" doc
/// section splits the two because one `used` definition cannot serve both a
/// capacity gate and an admin report: the report keys `used` off the
/// Stripe-EFFECTIVE tier (`COALESCE(pending_seat_tier_id, seat_tier_id)`),
/// the gate keys it off the plain, LAGGING `seat_tier_id` that
/// `grant_seat_tiers` itself uses. A reduce writes `grant_seat_tiers`, so it
/// must read the key that ledger is written under. Concretely: a member with
/// `seat_tier_id = premium` and `pending_seat_tier_id = standard` still
/// occupies a PREMIUM ledger seat until `billing::renewal::roll_period`
/// moves it. The report would show premium's `used` as 0 and offer that slot
/// up as free; reducing it away would leave `roll_period`'s pending-downgrade
/// move with no premium seat to transfer, which that function handles by
/// logging a stranded-member error and minting nothing — i.e. the member
/// ends up standard with no ledger seat at all. The gate reading refuses
/// instead, which is the whole point of the split.
///
/// [`crate::billing::slot_pool::SlotPool::available`] is used as-is rather
/// than re-deriving `purchased - used`: a live-but-unfulfilled
/// `slot_reservations` row (CS-435 D9 — an approval in flight) is a slot
/// already spoken for, and `available()` is the one place that arithmetic
/// lives.
async fn reducible_pool_for_tier(
    conn: &mut sqlx::PgConnection,
    account_id: Uuid,
    seat_tier_id: Uuid,
) -> Result<crate::billing::slot_pool::SlotPool, sqlx::Error> {
    crate::billing::slot_pool::read_slot_pool_for_tier_ledger(conn, account_id, seat_tier_id).await
}

/// Give `quantity` slots of one tier back — **the other half of the founder's
/// "the pool only changes when an admin explicitly adds or reduces slots"**
/// (CS-444; see this module's own doc comment for why removing
/// `release_seat`'s un-purchase made this route mandatory rather than
/// optional).
///
/// # It refuses to shrink the pool below the slots in use
///
/// The gate is `quantity <= available()` for THAT tier, read through
/// [`reducible_slots_for_tier`] — equivalently `purchased - quantity >= used
/// + reserved`. Both subtrahends matter: `used` is somebody's seat right now,
/// and `reserved` is somebody's seat a few hundred milliseconds from now (a
/// join-request approval mid-flight, `billing::slot_pool::reserve_slot`).
/// Selling either out from under its holder would leave a member seated on a
/// slot the ledger no longer contains — the exact `purchased < used` drift
/// `billing::renewal::RenewalOutcome::seat_mismatch` exists to alarm on, and
/// the only direction it still alarms on since CS-435.
///
/// The refusal is a 409 carrying the tier's actual free count, so an admin
/// (or the members-page UI that will front this) can act on it without a
/// second round trip: "you asked to remove 5, 2 are free". A 500 here would
/// be a worse answer to a perfectly well-formed request about a state the
/// caller is allowed to know.
///
/// The read happens INSIDE the same locked transaction as the write (S2 —
/// `billing::seat_lifecycle`'s own module doc), so a `reserve_slot`,
/// `acquire_seat` or `release_seat` racing this call serializes against it
/// rather than invalidating the count between the check and the push.
///
/// # Which statuses refuse (the one place this deliberately differs from
/// # [`purchase_slots`])
///
/// [`purchase_slots`] refuses BOTH frozen statuses via
/// `billing::enforcement::subscription_frozen` — buying while `past_due`
/// spends money an account is already behind on, and buying while `canceled`
/// resurrects billing nobody asked for. A reduce inherits only the second
/// half. `past_due` is OUR OWN enforcement state, not a Stripe inability to
/// accept a quantity DECREASE — this is `billing::seat_lifecycle`'s own
/// finding 7a reasoning, applied to the one path that still pushes a
/// decrease — and refusing it would trap exactly the org most likely to want
/// this route (behind on a bill, trying to shrink it) in the one-way ratchet
/// this route exists to break. A reduce can only ever lower what Stripe
/// bills, so allowing it on `past_due` cannot cost anybody money.
/// `canceled` refuses with 409: Stripe bills nothing there, so there is no
/// item to decrement, and quietly rewriting the local ledger to match a
/// subscription that no longer exists would be a money-shaped write with no
/// money behind it. Flagged in CS-444's own report as a policy call the
/// founder can overturn — it is not implied by the decision this PR builds.
///
/// # Proration: `none`, matching every other DECREASE in this crate
///
/// [`purchase_slots`] uses `create_prorations` (an ADD should bill for the
/// part-period it adds). Every decrease this codebase makes uses `none`
/// instead: `routes::billing_seats::change_seat_tier`'s downgrade ("the
/// member already paid the OLD tier through period end"), and
/// `seat_lifecycle::release_seat`'s decrement before CS-444 removed it. A
/// reduce follows that precedent — the org keeps the slots it already paid
/// for until the period ends, and is billed for fewer from the next invoice.
/// It issues NO credit for the remainder of the current period. Also flagged
/// in CS-444's report: switching this to `create_prorations` is a one-word
/// change and a founder decision, not a technical one.
///
/// # This route MUST NEVER trigger the drain (CS-449, item 38)
///
/// [`purchase_slots`] calls `routes::org_join_requests::drain_org` after its
/// own commit, because a purchase is the one event on this route family
/// that GROWS a tier's pool. A reduce only ever SHRINKS one — there is
/// never a freed slot here for a `pool_full` backlog to be pulled through,
/// and `drain_org` itself would find nothing (`reserve_slot`'s own gate
/// reads the pool inside the same locked transaction a concurrent drain
/// would race). This function calls no such thing; if a future edit adds
/// one, it is wrong.
#[utoipa_path(
    post,
    path = "/v1/orgs/slots/reduce",
    tag = "billing",
    security(("bearer_token" = [])),
    request_body = ReduceSlotsRequest,
    responses(
        (status = 200, description = "Slots reduced; the account's updated pool", body = SlotPoolResponse),
        (status = 400, description = "Unknown/inactive tier, or quantity out of range (1..=500)"),
        (status = 403, description = "Not an org admin, or a personal org"),
        (status = 409, description = "Canceled subscription, no active team subscription, or fewer free slots than requested"),
        (status = 502, description = "Transient Stripe/upstream failure"),
        (status = 503, description = "Database, Stripe, or Clerk unavailable"),
    )
)]
pub async fn reduce_slots(
    State(state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Json(body): Json<ReduceSlotsRequest>,
) -> Response {
    let account=user.account_id;
    let response=reduce_slots_committed(State(state.clone()),tenant,Extension(user),Json(body)).await;
    crate::read_models::dirty::after_control_response(&state,account,crate::read_models::dirty::DirtyEvent::BillingChanged,response).await
}

async fn reduce_slots_committed(
    State(state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Json(body): Json<ReduceSlotsRequest>,
) -> Response {
    let ctx = match require_org_admin(&tenant, &user) {
        Ok(c) => c,
        Err(r) => return r,
    };
    if let Err(resp) = refuse_personal_org(&state, ctx).await {
        return resp;
    }
    if !(MIN_QUANTITY..=MAX_QUANTITY).contains(&body.quantity) {
        return bad_request("quantity must be between 1 and 500");
    }
    if body.idempotency_key.trim().is_empty() {
        return bad_request("idempotency_key is required");
    }

    let Some(control) = state.control_db() else {
        return service_unavailable();
    };
    let Some(stripe) = state.stripe.as_ref() else {
        return service_unavailable();
    };
    let account_id = ctx.account_id;

    // Same grant + subscription lookup [`purchase_slots`] makes, with
    // `sub.status` pulled along: this route's frozen rule is status-specific
    // (see this function's own doc comment) rather than
    // `enforcement::subscription_frozen`'s blanket one, and reading it here
    // costs nothing extra.
    let team_grant: Option<(Uuid, String, String)> =
        match crate::tenancy::scope::begin_account_scoped(control, account_id).await {
            Ok(mut scoped_tx) => {
                let row = sqlx::query_as(
                    "SELECT g.id, sub.stripe_subscription_id, sub.status FROM grants g \
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
    let Some((grant_id, subscription_id, status)) = team_grant else {
        return conflict("this account has no active team subscription — slots can only be reduced for team accounts");
    };
    if status == "canceled" {
        return conflict(
            "subscription canceled — Stripe is no longer billing for these slots, so there is nothing to reduce",
        );
    }

    let tier: Option<(Uuid, String, Option<String>)> = match sqlx::query_as(
        "SELECT id, code, stripe_price_id FROM seat_tiers WHERE code = $1 AND is_active = true",
    )
    .bind(&body.tier)
    .fetch_optional(control)
    .await
    {
        Ok(row) => row,
        Err(e) => return internal_error("seat tier lookup", e),
    };
    let Some((tier_id, tier_code, stripe_price_id)) = tier else {
        return bad_request("unknown or inactive seat tier");
    };
    let Some(price_id) = stripe_price_id else {
        return internal_error(
            "tier price lookup",
            format!("seat tier {tier_code} has no stripe_price_id"),
        );
    };

    // S2 (`billing::seat_lifecycle`'s own module doc): lock and transaction
    // FIRST, held across the Stripe call — and, unlike `purchase_slots`,
    // holding them across the availability READ too, which is the whole
    // reason this route cannot just check-then-push.
    let mut tx = match crate::tenancy::scope::begin_account_scoped(control, account_id).await {
        Ok(tx) => tx,
        Err(e) => return internal_error("scope", e),
    };
    if let Err(e) = crate::billing::renewal::lock_period_rotation(&mut tx, account_id).await {
        let _ = tx.rollback().await;
        return internal_error("lock", e);
    }

    let tier_pool = match reducible_pool_for_tier(&mut tx, account_id, tier_id).await {
        Ok(tier_pool) => tier_pool,
        Err(e) => {
            let _ = tx.rollback().await;
            return internal_error("slot pool read", e);
        }
    };
    let available = tier_pool.available();
    if body.quantity > available {
        let _ = tx.rollback().await;
        // 409, with the number the caller needs to retry correctly. Nothing
        // has been pushed or written at this point.
        return (
            StatusCode::CONFLICT,
            Json(json!({
                "error": "not_enough_free_slots",
                "detail": format!(
                    "cannot reduce {} {tier_code} slot(s): only {available} are free (slots in use \
                     or reserved for an in-flight approval cannot be removed)",
                    body.quantity
                ),
                "tier": tier_code,
                "requested": body.quantity,
                "available": available,
            })),
        )
            .into_response();
    }

    let idem_key = format!(
        "slot:{account_id}:reduce:{tier_code}:{}",
        body.idempotency_key.trim()
    );
    let push = crate::billing::seat_lifecycle::with_stripe_deadline(async {
        let sub = stripe.retrieve_subscription(&subscription_id).await?;
        match sub.items.iter().find(|it| it.price_id == price_id) {
            Some(item) => {
                // ── Drift signal, BEFORE the mutation ────────────────────
                // A state that silently diverges must be alertable. This is
                // the per-tier twin of `routes::stripe_webhook::
                // apply_subscription_updated`'s account-wide check (same
                // `stripe_quantity` vs ledger comparison, same `warn!`), and
                // it is reachable for the reasons that check exists: a crash
                // between a Stripe push and its local commit (`billing::
                // seat_lifecycle`'s own "Stripe push BEFORE the DB write,
                // always" section accepts exactly that window), or a human
                // editing the subscription in Stripe's Dashboard.
                //
                // A reduce is the ONLY remaining path in this crate that
                // pushes a Stripe DECREASE — CS-444 removed `release_seat`'s
                // — so this is the right place for the signal to live: it
                // fires while an operator can still act on it, against a
                // request that is about to change the very number in
                // question.
                if item.quantity != tier_pool.purchased {
                    tracing::warn!(
                        %account_id, tier_code, price_id,
                        stripe_quantity = item.quantity,
                        ledger_purchased = tier_pool.purchased,
                        requested = body.quantity,
                        "org_slots::reduce_slots: Stripe and the purchase ledger disagree about \
                         this tier's purchased count — proceeding with the reduction against \
                         Stripe's own quantity, which is what is actually billed"
                    );
                }
                if item.quantity > body.quantity {
                    stripe
                        .update_subscription_item(
                            &item.id,
                            item.quantity - body.quantity,
                            "none",
                            Idem::Key(&idem_key),
                        )
                        .await?;
                } else {
                    // Reducing this tier to zero AT STRIPE. 0168's "a tier
                    // with zero seats simply has no row" convention, applied
                    // at Stripe's layer too — the same DELETE
                    // `seat_lifecycle::release_seat` used to make for the
                    // last seat on a tier, and (like it) no `Idem::Key`: a
                    // DELETE is naturally idempotent at Stripe, which
                    // no-ops an already-gone item.
                    //
                    // **This arm can CREATE a divergence rather than pass
                    // one through, which is why it gets a second warn.**
                    // It is taken when `item.quantity <= body.quantity`. If
                    // the ledger holds MORE than is being reduced
                    // (`purchased > body.quantity` — only reachable when
                    // Stripe had already drifted low, since the gate proved
                    // `body.quantity <= available <= purchased`), then
                    // Stripe ends at 0 while the local write below leaves
                    // `purchased - body.quantity > 0` behind. The previous
                    // revision of this branch claimed "the ledger write
                    // below reconciles our side"; it does not, and it also
                    // missed the `item.quantity == body.quantity` case
                    // entirely. Both are fixed here.
                    if tier_pool.purchased > body.quantity {
                        tracing::warn!(
                            %account_id, tier_code, price_id,
                            stripe_quantity = item.quantity,
                            ledger_purchased = tier_pool.purchased,
                            requested = body.quantity,
                            local_remainder = tier_pool.purchased - body.quantity,
                            "org_slots::reduce_slots: deleting this tier's Stripe item takes it to \
                             zero while the local ledger keeps a remainder — nobody is billed for \
                             those slots. Reachable only from a pre-existing Stripe-below-ledger \
                             drift; the next admin purchase re-creates the item"
                        );
                    }
                    stripe.delete_subscription_item(&item.id, "none").await?;
                }
            }
            None => {
                // Drift: Stripe already has no item for this tier, so its
                // side is already what this call wanted and the local ledger
                // is the stale one. Proceed to the ledger write rather than
                // failing a request whose intent Stripe has already
                // satisfied — the same tolerate-and-warn shape
                // `seat_lifecycle::release_seat` carried before CS-444
                // removed its Stripe push, relocated to the path that still
                // makes one.
                tracing::warn!(
                    %account_id, tier_code, price_id,
                    "org_slots::reduce_slots: no matching Stripe subscription item for this tier — \
                     Stripe already reflects the reduction; reconciling the local ledger only"
                );
            }
        }
        Ok(())
    })
    .await;
    if let Err(e) = push {
        let _ = tx.rollback().await;
        return map_stripe_error_to_response(e);
    }

    // Stripe succeeded — mirror it locally, in the SAME locked transaction.
    // Delete-if-emptied / decrement-otherwise, the same partition
    // `seat_lifecycle` and `billing::renewal`'s pending-downgrade move use
    // and for the identical reason: a plain `seats - N` cannot land on `0`
    // without violating `grant_seat_tiers_seats_check` (0168). The gate
    // above already proved `quantity <= purchased` for this tier, so these
    // two statements between them always match exactly one row's worth of
    // intent.
    if let Err(e) = sqlx::query(
        "DELETE FROM grant_seat_tiers WHERE grant_id = $1 AND seat_tier_id = $2 AND seats <= $3",
    )
    .bind(grant_id)
    .bind(tier_id)
    .bind(body.quantity as i32)
    .execute(&mut *tx)
    .await
    {
        return internal_error("grant_seat_tiers delete (Stripe already pushed — drift, not a lost reduction)", e);
    }
    if let Err(e) = sqlx::query(
        "UPDATE grant_seat_tiers SET seats = seats - $3 \
          WHERE grant_id = $1 AND seat_tier_id = $2 AND seats > $3",
    )
    .bind(grant_id)
    .bind(tier_id)
    .bind(body.quantity as i32)
    .execute(&mut *tx)
    .await
    {
        return internal_error("grant_seat_tiers write (Stripe already pushed — drift, not a lost reduction)", e);
    }
    // `grants.seats` is left UNTOUCHED when the last `grant_seat_tiers` row
    // for this grant is gone — `resync_grant_seats`' own EXISTS guard, since
    // a seat-priced grant's `seats` can be neither NULL nor 0 (0156). That
    // staleness predates this route (`release_seat` reaching zero had it
    // too) and is harmless to the pool, which reads Σ `grant_seat_tiers`
    // directly; noted here so the next reader does not mistake it for a bug
    // this route introduced.
    if let Err(e) = crate::billing::renewal::resync_grant_seats(&mut tx, grant_id).await {
        return internal_error("resync_grant_seats (Stripe already pushed — drift, not a lost reduction)", e);
    }

    if let Err(e) = tx.commit().await {
        return internal_error("commit", e);
    }
    // Presentation-only, exactly as in `purchase_slots`: the mirror is
    // already durable, and a read failure here must not roll back a
    // reduction Stripe has already applied.
    let mut read_tx = match crate::tenancy::scope::begin_account_scoped(control, account_id).await {
        Ok(tx) => tx,
        Err(e) => return internal_error("slot pool read (scope)", e),
    };
    let response_body = match slot_pool_response(&mut read_tx, account_id).await {
        Ok(body) => body,
        Err(e) => {
            let _ = read_tx.rollback().await;
            return internal_error("slot pool read (reduction already committed)", e);
        }
    };
    let _ = read_tx.commit().await;
    // S4: Clerk's member cap follows the (reduced) purchased seats.
    crate::routes::orgs_create::spawn_clerk_membership_cap_sync(control.clone(), account_id);
    (StatusCode::OK, Json(response_body)).into_response()
}

// ── Live-Postgres tests ──────────────────────────────────────────────────────
//
// Gated on `RELINFER_TEST_DATABASE_URL` — IN-CRATE (not a `backend/tests/*.rs`
// file) for the identical reason `routes::org_invites::pg_tests`/
// `routes::billing_seats::pg_tests` already are: `routes` is a crate-private
// module (`mod routes;` in `lib.rs`), so `get_slots`/`purchase_slots` cannot
// be named from an external test binary at all, `pub` or not — see
// `scripts/check_pg_suite_coverage.sh`'s own header for the general rule.
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
    use axum::extract::State;

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

    async fn seat_tier_id(pool: &sqlx::PgPool, code: &str) -> Uuid {
        sqlx::query_scalar("SELECT id FROM seat_tiers WHERE code = $1 AND is_active = true")
            .bind(code)
            .fetch_one(pool)
            .await
            .expect("active seat tier must exist (0165's seed)")
    }

    /// A `team`-plan org account with one `owner` admin member (standard
    /// tier, purchased), a `team` grant, `purchased` pre-bought standard
    /// slots on the ledger, and a linked `stripe_subscriptions` row. Returns
    /// `(account_id, org_id, admin_user_id, grant_id)`.
    async fn seed_org_with_pool(pool: &sqlx::PgPool, purchased: i64) -> (Uuid, String, Uuid, Uuid) {
        let admin_id: Uuid = sqlx::query_scalar("INSERT INTO users (email) VALUES ($1) RETURNING id")
            .bind(format!("{}@org-slots-prf.test", Uuid::new_v4()))
            .fetch_one(pool)
            .await
            .expect("insert admin user");
        let org_id = format!("org_{}", Uuid::new_v4().simple());
        let account_id: Uuid = sqlx::query_scalar(
            "INSERT INTO accounts (owner_user_id, clerk_org_id) VALUES ($1, $2) RETURNING id",
        )
        .bind(admin_id)
        .bind(&org_id)
        .fetch_one(pool)
        .await
        .expect("insert org account");
        let standard_id = seat_tier_id(pool, "standard").await;
        sqlx::query(
            "INSERT INTO account_members (account_id, user_id, role, seat_tier_id, seat_purchased_at) \
             VALUES ($1, $2, 'owner', $3, now())",
        )
        .bind(account_id)
        .bind(admin_id)
        .bind(standard_id)
        .execute(pool)
        .await
        .expect("insert admin membership (already purchased — the fixture's own slot)");

        let plan_id: Uuid = sqlx::query_scalar("SELECT id FROM plans WHERE code = 'team'")
            .fetch_one(pool)
            .await
            .expect("team plan must be seeded (0167)");
        let grant_id: Uuid = fetch_financial_fixture_id(
            pool,
            account_id,
            sqlx::query_scalar(
            "INSERT INTO grants (account_id, plan_id, seats, plan_is_seat_priced) \
             VALUES ($1, $2, $3, true) RETURNING id",
        )
        .bind(account_id)
        .bind(plan_id)
        .bind(purchased as i32),
        )
        .await
        .expect("insert team grant");
        if purchased > 0 {
            execute_financial_fixture(pool, account_id, sqlx::query("INSERT INTO grant_seat_tiers (grant_id, seat_tier_id, seats) VALUES ($1, $2, $3)")
                .bind(grant_id)
                .bind(standard_id)
                .bind(purchased as i32))
                .await
                .expect("seed the purchased ledger");
        }
        execute_financial_fixture(pool, account_id, sqlx::query(
            "INSERT INTO stripe_subscriptions (account_id, stripe_customer_id, stripe_subscription_id, status) \
             VALUES ($1, $2, $3, 'active')",
        )
        .bind(account_id)
        .bind(format!("cus_{}", Uuid::new_v4().simple()))
        .bind(format!("sub_{}", Uuid::new_v4().simple())))
        .await
        .expect("link stripe subscription");

        (account_id, org_id, admin_id, grant_id)
    }

    /// A personal org's own account — same end state
    /// `routes::checkout::pg_tests::seed_personal_org_account` builds
    /// (`bin/adopt_personal_org_account.rs`'s own shape): `clerk_org_id` set,
    /// a `personal_orgs` row naming it, and a hand-assigned seat tier on the
    /// sole member (`account_members_seat_tier_iff_org`, 0169/0176).
    async fn seed_personal_org(pool: &sqlx::PgPool) -> (Uuid, String, Uuid) {
        // `Store::account_is_real_org`'s query JOINs `personal_orgs.
        // clerk_user_id` back to `users.clerk_user_id` — the two MUST agree,
        // or the JOIN never matches and this fixture is silently read as a
        // REAL org instead of a personal one (the exact bug this comment
        // documents: an earlier version bound a synthetic `clerk_user_id`
        // here that was never written onto the `users` row at all, so
        // `purchase_slots_refuses_a_personal_org` observed a 503 —
        // `state.stripe` being `None` on the fallthrough real-org path —
        // instead of the expected 403). Mirrors `routes::checkout::
        // pg_tests::seed_personal_org_account`'s own shape.
        let clerk_user_id = format!("user_{}", Uuid::new_v4().simple());
        let user_id: Uuid =
            sqlx::query_scalar("INSERT INTO users (email, clerk_user_id) VALUES ($1, $2) RETURNING id")
                .bind(format!("{}@org-slots-personal-prf.test", Uuid::new_v4()))
                .bind(&clerk_user_id)
                .fetch_one(pool)
                .await
                .expect("insert user");
        let org_id = format!("org_{}", Uuid::new_v4().simple());
        let account_id: Uuid = sqlx::query_scalar(
            "INSERT INTO accounts (owner_user_id, clerk_org_id) VALUES ($1, $2) RETURNING id",
        )
        .bind(user_id)
        .bind(&org_id)
        .fetch_one(pool)
        .await
        .expect("insert personal-org account");
        sqlx::query("INSERT INTO personal_orgs (clerk_org_id, clerk_user_id) VALUES ($1, $2)")
            .bind(&org_id)
            .bind(&clerk_user_id)
            .execute(pool)
            .await
            .expect("insert personal_orgs row");
        let standard_id = seat_tier_id(pool, "standard").await;
        sqlx::query(
            "INSERT INTO account_members (account_id, user_id, role, seat_tier_id) VALUES ($1, $2, 'owner', $3)",
        )
        .bind(account_id)
        .bind(user_id)
        .bind(standard_id)
        .execute(pool)
        .await
        .expect("insert owner membership");
        (account_id, org_id, user_id)
    }

    async fn cleanup(pool: &sqlx::PgPool, account_id: Uuid, users: &[Uuid]) {
        let _ = execute_financial_fixture(
            pool,
            account_id,
            sqlx::query("DELETE FROM accounts WHERE id = $1").bind(account_id),
        ).await;
        for u in users {
            let _ = sqlx::query("DELETE FROM users WHERE id = $1").bind(u).execute(pool).await;
        }
    }

    fn user_context(account_id: Uuid, user_id: Uuid, role: &str) -> UserContext {
        UserContext {
            user_id,
            account_id,
            role: role.to_string(),
            device_id: "org-slots-prf-test".to_string(),
        }
    }

    /// Mirrors `routes::org_invites::pg_tests::org_tenant` — a `Tenant` with
    /// an org `TenancyDecision`, carrying `control` as its own `pool` field
    /// (this route only ever touches control-plane tables, so there is no
    /// separate tenant pool to distinguish).
    fn org_tenant(control: sqlx::PgPool, org_id: &str, account_id: Uuid, user_id: Uuid) -> Tenant {
        let ctx = TenantContext {
            org_id: org_id.to_string(),
            account_id,
            user_id,
            pool: control,
            page_pool: None,
        };
        Tenant {
            store: None,
            pool: None,
            decision: Some(crate::tenancy::runtime::TenancyDecision::Org(ctx)),
            class: crate::route_class::RouteClass::Other,
        }
    }

    async fn app_state(pool: sqlx::PgPool, stripe: Option<crate::billing::stripe_client::StripeClient>) -> Arc<AppState> {
        let mut state = crate::test_app_state_owned_with_db_and_store(pool, None);
        state.stripe = stripe.map(Arc::new);
        Arc::new(state)
    }

    async fn body_json(resp: Response) -> serde_json::Value {
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX).await.expect("read body");
        serde_json::from_slice(&bytes).expect("valid JSON body")
    }

    /// A mock recording every request's method+path+body — lets a test
    /// assert the idempotency key genuinely reached Stripe, mirroring
    /// `clerk_provision::pg_tests::spawn_failing_stripe_mock`'s recorder
    /// shape but succeeding rather than failing.
    async fn spawn_recording_stripe_mock(
        price_id: &str,
        quantity: i64,
    ) -> (crate::billing::stripe_client::StripeClient, std::sync::Arc<tokio::sync::Mutex<Vec<String>>>) {
        let calls: std::sync::Arc<tokio::sync::Mutex<Vec<String>>> = Default::default();
        let calls_clone = calls.clone();
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
        let app = axum::Router::new().fallback(axum::routing::any(
            move |req: axum::extract::Request| {
                let calls = calls_clone.clone();
                let body = body.clone();
                async move {
                    let method = req.method().to_string();
                    let uri = req.uri().to_string();
                    let bytes = axum::body::to_bytes(req.into_body(), usize::MAX).await.unwrap_or_default();
                    let form = String::from_utf8_lossy(&bytes).to_string();
                    calls.lock().await.push(format!("{method} {uri} {form}"));
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
        (
            crate::billing::stripe_client::StripeClient::new(reqwest::Client::new(), "sk_test_mock")
                .with_base_url(format!("http://{addr}")),
            calls,
        )
    }

    async fn spawn_500_mock() -> crate::billing::stripe_client::StripeClient {
        let app = axum::Router::new()
            .fallback(axum::routing::any(|| async { axum::http::StatusCode::INTERNAL_SERVER_ERROR }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind mock");
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.ok();
        });
        crate::billing::stripe_client::StripeClient::new(reqwest::Client::new(), "sk_test_mock")
            .with_base_url(format!("http://{addr}"))
    }

    /// `GET /v1/orgs/slots` arithmetic: 5 purchased, 1 used (the fixture's
    /// own admin), 0 reserved ⇒ `available = 4`.
    #[tokio::test]
    async fn get_slots_reports_purchased_used_reserved_available() {
        let Some(pool) = pg_test_pool("get_slots_reports_purchased_used_reserved_available").await
        else {
            return;
        };
        let (account_id, org_id, admin_id, _grant_id) = seed_org_with_pool(&pool, 5).await;
        let state = app_state(pool.clone(), None).await;
        let tenant = org_tenant(pool.clone(), &org_id, account_id, admin_id);

        let resp = get_slots(State(state), tenant, Extension(user_context(account_id, admin_id, "member"))).await;
        assert_eq!(resp.status(), StatusCode::OK);
        let body = body_json(resp).await;
        assert_eq!(body["purchased"], 5);
        assert_eq!(body["used"], 1);
        assert_eq!(body["reserved"], 0);
        assert_eq!(body["available"], 4);

        // CS-442 (item 40, D8): the per-tier breakdown — `standard` carries
        // this fixture's own numbers, `premium` (untouched) still appears
        // with zeros rather than being silently absent.
        let tiers = body["tiers"].as_array().expect("tiers must be an array");
        let standard = tiers.iter().find(|t| t["code"] == "standard").expect("standard must be listed");
        assert_eq!(standard["purchased"], 5);
        assert_eq!(standard["used"], 1);
        assert_eq!(standard["reserved"], 0);
        assert_eq!(standard["available"], 4);
        let premium = tiers.iter().find(|t| t["code"] == "premium").expect("premium must be listed");
        assert_eq!(premium["purchased"], 0);
        assert_eq!(premium["used"], 0);
        assert_eq!(premium["available"], 0);

        cleanup(&pool, account_id, &[admin_id]).await;
    }

    /// `POST /v1/orgs/slots` happy path: the Stripe mock records the
    /// idempotency key AND the new quantity (existing 1 + purchasing 4 = 5),
    /// and the local `grant_seat_tiers` mirror lands at 5 in the SAME call.
    #[tokio::test]
    async fn purchase_slots_pushes_stripe_with_the_idempotency_key_and_updates_the_local_mirror() {
        let Some(pool) = pg_test_pool(
            "purchase_slots_pushes_stripe_with_the_idempotency_key_and_updates_the_local_mirror",
        )
        .await
        else {
            return;
        };
        let (account_id, org_id, admin_id, grant_id) = seed_org_with_pool(&pool, 1).await;
        let price_id = format!("price_org_slots_{}", Uuid::new_v4().simple());
        sqlx::query("UPDATE seat_tiers SET stripe_price_id = $2 WHERE code = $1")
            .bind("standard")
            .bind(&price_id)
            .execute(&pool)
            .await
            .expect("set standard's stripe_price_id for this test");
        let (stripe, calls) = spawn_recording_stripe_mock(&price_id, 1).await;
        let state = app_state(pool.clone(), Some(stripe)).await;
        let tenant = org_tenant(pool.clone(), &org_id, account_id, admin_id);
        let idem = format!("client-{}", Uuid::new_v4());

        let resp = purchase_slots(
            State(state),
            tenant,
            Extension(user_context(account_id, admin_id, "owner")),
            Json(PurchaseSlotsRequest { tier: "standard".to_string(), quantity: 4, idempotency_key: idem.clone() }),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK, "{:?}", body_json(resp).await);

        let recorded = calls.lock().await;
        assert!(
            recorded.iter().any(|c| c.contains("quantity=5")),
            "Stripe must be pushed to quantity 5 (existing 1 + purchased 4): {recorded:?}"
        );
        // The idempotency key itself travels in the `Idempotency-Key`
        // HEADER, not the form body — `spawn_recording_stripe_mock` only
        // records method/uri/body, so the folded key's ACCOUNT-scoped
        // recipe (`slot:{account_id}:buy:{tier}:{client key}`) is proven
        // indirectly here: a second call below with the SAME client key
        // must be treated as a retry of the identical logical purchase, not
        // a fresh one, by Stripe's own cache — this suite's own mock always
        // answers 200 regardless, so the header's WIRE FORMAT is covered by
        // `billing::stripe_client`'s own Layer 1 tests (this suite proves
        // what THIS route does with the response, the same division of
        // labor `routes::checkout::pg_tests` documents).
        drop(recorded);

        let ledger: i32 = sqlx::query_scalar(
            "SELECT seats FROM grant_seat_tiers WHERE grant_id = $1")
        .bind(grant_id)
        .fetch_one(&pool)
        .await
        .expect("ledger row must exist");
        assert_eq!(ledger, 5, "local grant_seat_tiers must mirror the new Stripe quantity");
        let grants_seats: i32 = sqlx::query_scalar("SELECT seats FROM grants WHERE id = $1")
            .bind(grant_id)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(grants_seats, 5);

        cleanup(&pool, account_id, &[admin_id]).await;
    }

    /// A Stripe 5xx maps through `map_stripe_error_to_response` (502) and
    /// changes NOTHING locally — the local `grant_seat_tiers` write never
    /// runs when the Stripe push itself failed.
    #[tokio::test]
    async fn purchase_slots_stripe_5xx_changes_nothing_locally() {
        let Some(pool) = pg_test_pool("purchase_slots_stripe_5xx_changes_nothing_locally").await
        else {
            return;
        };
        let (account_id, org_id, admin_id, grant_id) = seed_org_with_pool(&pool, 1).await;
        let price_id = format!("price_org_slots_5xx_{}", Uuid::new_v4().simple());
        sqlx::query("UPDATE seat_tiers SET stripe_price_id = $2 WHERE code = $1")
            .bind("standard")
            .bind(&price_id)
            .execute(&pool)
            .await
            .expect("set standard's stripe_price_id for this test");
        let stripe = spawn_500_mock().await;
        let state = app_state(pool.clone(), Some(stripe)).await;
        let tenant = org_tenant(pool.clone(), &org_id, account_id, admin_id);

        let resp = purchase_slots(
            State(state),
            tenant,
            Extension(user_context(account_id, admin_id, "owner")),
            Json(PurchaseSlotsRequest {
                tier: "standard".to_string(),
                quantity: 4,
                idempotency_key: Uuid::new_v4().to_string(),
            }),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::BAD_GATEWAY, "a Stripe 5xx must map to 502");

        let ledger: i32 = sqlx::query_scalar("SELECT seats FROM grant_seat_tiers WHERE grant_id = $1")
            .bind(grant_id)
            .fetch_one(&pool)
            .await
            .expect("ledger row must still exist, unchanged");
        assert_eq!(ledger, 1, "the pre-existing ledger must be untouched by a failed push");

        cleanup(&pool, account_id, &[admin_id]).await;
    }

    /// A personal org refuses with 403 — see this module's own doc comment
    /// for why `require_org_admin` alone cannot exclude one.
    #[tokio::test]
    async fn purchase_slots_refuses_a_personal_org() {
        let Some(pool) = pg_test_pool("purchase_slots_refuses_a_personal_org").await else {
            return;
        };
        let (account_id, org_id, user_id) = seed_personal_org(&pool).await;
        let state = app_state(pool.clone(), None).await;
        let tenant = org_tenant(pool.clone(), &org_id, account_id, user_id);

        let resp = purchase_slots(
            State(state),
            tenant,
            Extension(user_context(account_id, user_id, "owner")),
            Json(PurchaseSlotsRequest {
                tier: "standard".to_string(),
                quantity: 1,
                idempotency_key: Uuid::new_v4().to_string(),
            }),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::FORBIDDEN);

        cleanup(&pool, account_id, &[user_id]).await;
    }

    /// A plain member (not admin/owner) is refused with 403 by
    /// `require_org_admin`, the same bar `routes::org_invites`' own routes
    /// use.
    #[tokio::test]
    async fn purchase_slots_refuses_a_non_admin() {
        let Some(pool) = pg_test_pool("purchase_slots_refuses_a_non_admin").await else {
            return;
        };
        let (account_id, org_id, admin_id, _grant_id) = seed_org_with_pool(&pool, 1).await;
        let member_id: Uuid = sqlx::query_scalar("INSERT INTO users (email) VALUES ($1) RETURNING id")
            .bind(format!("{}@org-slots-member-prf.test", Uuid::new_v4()))
            .fetch_one(&pool)
            .await
            .expect("insert plain member");
        let standard_id = seat_tier_id(&pool, "standard").await;
        sqlx::query(
            "INSERT INTO account_members (account_id, user_id, role, seat_tier_id) VALUES ($1, $2, 'member', $3)",
        )
        .bind(account_id)
        .bind(member_id)
        .bind(standard_id)
        .execute(&pool)
        .await
        .expect("insert plain membership");
        let state = app_state(pool.clone(), None).await;
        let tenant = org_tenant(pool.clone(), &org_id, account_id, member_id);

        let resp = purchase_slots(
            State(state),
            tenant,
            Extension(user_context(account_id, member_id, "member")),
            Json(PurchaseSlotsRequest {
                tier: "standard".to_string(),
                quantity: 1,
                idempotency_key: Uuid::new_v4().to_string(),
            }),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::FORBIDDEN);

        cleanup(&pool, account_id, &[admin_id, member_id]).await;
    }

    // ── CS-444: POST /v1/orgs/slots/reduce ───────────────────────────────

    /// Read one tier's ledger row (`grant_seat_tiers.seats`) — `None` when
    /// the tier has been reduced away entirely.
    async fn tier_ledger(pool: &sqlx::PgPool, grant_id: Uuid, code: &str) -> Option<i32> {
        sqlx::query_scalar(
            "SELECT gst.seats FROM grant_seat_tiers gst \
              JOIN seat_tiers st ON st.id = gst.seat_tier_id \
             WHERE gst.grant_id = $1 AND st.code = $2",
        )
        .bind(grant_id)
        .bind(code)
        .fetch_optional(pool)
        .await
        .expect("read grant_seat_tiers")
    }

    /// `POST /v1/orgs/slots/reduce` happy path — the mirror of
    /// `purchase_slots_pushes_stripe_with_the_idempotency_key_and_updates_the_local_mirror`.
    /// The fixture has 5 purchased standard slots and 1 in use (its own
    /// admin), so 4 are free; reducing 3 must push Stripe DOWN (existing
    /// quantity 5 - 3 = 2) and land the local mirror at 2 in the same call.
    #[tokio::test]
    async fn reduce_slots_pushes_a_stripe_decrement_and_updates_the_local_mirror() {
        let Some(pool) =
            pg_test_pool("reduce_slots_pushes_a_stripe_decrement_and_updates_the_local_mirror")
                .await
        else {
            return;
        };
        let (account_id, org_id, admin_id, grant_id) = seed_org_with_pool(&pool, 5).await;
        let price_id = format!("price_org_slots_reduce_{}", Uuid::new_v4().simple());
        sqlx::query("UPDATE seat_tiers SET stripe_price_id = $2 WHERE code = $1")
            .bind("standard")
            .bind(&price_id)
            .execute(&pool)
            .await
            .expect("set standard's stripe_price_id for this test");
        let (stripe, calls) = spawn_recording_stripe_mock(&price_id, 5).await;
        let state = app_state(pool.clone(), Some(stripe)).await;
        let tenant = org_tenant(pool.clone(), &org_id, account_id, admin_id);

        let resp = reduce_slots(
            State(state),
            tenant,
            Extension(user_context(account_id, admin_id, "owner")),
            Json(ReduceSlotsRequest {
                tier: "standard".to_string(),
                quantity: 3,
                idempotency_key: format!("client-{}", Uuid::new_v4()),
            }),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK, "{:?}", body_json(resp).await);

        let recorded = calls.lock().await;
        assert!(
            recorded.iter().any(|c| c.contains("quantity=2")),
            "Stripe must be pushed DOWN to quantity 2 (existing 5 - reduced 3): {recorded:?}"
        );
        assert!(
            recorded.iter().any(|c| c.contains("proration_behavior=none")),
            "a reduce must not prorate — the org keeps what it paid for through period end, the \
             same posture every other decrease in this crate takes: {recorded:?}"
        );
        drop(recorded);

        assert_eq!(
            tier_ledger(&pool, grant_id, "standard").await,
            Some(2),
            "the local ledger must mirror the new Stripe quantity"
        );
        let grants_seats: i32 = sqlx::query_scalar("SELECT seats FROM grants WHERE id = $1")
            .bind(grant_id)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(grants_seats, 2);

        cleanup(&pool, account_id, &[admin_id]).await;
    }

    /// **CS-444 — negative test 3.** *Reducing the pool below the slots in
    /// use is refused.*
    ///
    /// The fixture buys 3 standard slots and seats its admin on one, so 2 are
    /// free. Asking to remove all 3 must be refused — 409, with the free
    /// count in the body so the caller can retry correctly — and must change
    /// NOTHING: no Stripe call, no ledger write. The recording mock is what
    /// proves the first half of that: a refusal that had already pushed
    /// Stripe would leave a recorded call behind.
    #[tokio::test]
    async fn reduce_slots_refuses_to_shrink_below_the_slots_in_use() {
        let Some(pool) = pg_test_pool("reduce_slots_refuses_to_shrink_below_the_slots_in_use").await
        else {
            return;
        };
        let (account_id, org_id, admin_id, grant_id) = seed_org_with_pool(&pool, 3).await;
        let price_id = format!("price_org_slots_reduce_floor_{}", Uuid::new_v4().simple());
        sqlx::query("UPDATE seat_tiers SET stripe_price_id = $2 WHERE code = $1")
            .bind("standard")
            .bind(&price_id)
            .execute(&pool)
            .await
            .expect("set standard's stripe_price_id for this test");
        let (stripe, calls) = spawn_recording_stripe_mock(&price_id, 3).await;
        let state = app_state(pool.clone(), Some(stripe)).await;
        let tenant = org_tenant(pool.clone(), &org_id, account_id, admin_id);

        let resp = reduce_slots(
            State(state),
            tenant,
            Extension(user_context(account_id, admin_id, "owner")),
            Json(ReduceSlotsRequest {
                tier: "standard".to_string(),
                quantity: 3,
                idempotency_key: Uuid::new_v4().to_string(),
            }),
        )
        .await;
        assert_eq!(
            resp.status(),
            StatusCode::CONFLICT,
            "removing the slot somebody is sitting on must be refused, not 500"
        );
        let body = body_json(resp).await;
        assert_eq!(body["error"], "not_enough_free_slots");
        assert_eq!(body["available"], 2, "the refusal must name the free count");
        assert_eq!(body["requested"], 3);

        assert!(
            calls.lock().await.is_empty(),
            "a refused reduce must not have pushed anything to Stripe first"
        );
        assert_eq!(
            tier_ledger(&pool, grant_id, "standard").await,
            Some(3),
            "the ledger must be untouched by a refused reduce"
        );

        cleanup(&pool, account_id, &[admin_id]).await;
    }

    /// A slot RESERVED for an in-flight join-request approval
    /// (`billing::slot_pool::reserve_slot`, CS-435 D9) is spoken for, exactly
    /// like a used one. The fixture has 3 purchased, 1 used and 1 reserved,
    /// so only 1 is genuinely free — reducing 2 must be refused. Without the
    /// `reserved` term (i.e. gating on a hand-rolled `purchased - used`
    /// instead of `SlotPool::available()`), this request would be ALLOWED and
    /// the approval mid-flight would land on a slot the ledger no longer
    /// contained.
    #[tokio::test]
    async fn reduce_slots_counts_a_live_reservation_as_spoken_for() {
        let Some(pool) = pg_test_pool("reduce_slots_counts_a_live_reservation_as_spoken_for").await
        else {
            return;
        };
        let (account_id, org_id, admin_id, grant_id) = seed_org_with_pool(&pool, 3).await;
        let price_id = format!("price_org_slots_reduce_resv_{}", Uuid::new_v4().simple());
        sqlx::query("UPDATE seat_tiers SET stripe_price_id = $2 WHERE code = $1")
            .bind("standard")
            .bind(&price_id)
            .execute(&pool)
            .await
            .expect("set standard's stripe_price_id for this test");
        let pending_id: Uuid = sqlx::query_scalar("INSERT INTO users (email) VALUES ($1) RETURNING id")
            .bind(format!("{}@org-slots-reserved.test", Uuid::new_v4()))
            .fetch_one(&pool)
            .await
            .expect("insert the pending joiner");
        let standard_id = seat_tier_id(&pool, "standard").await;
        sqlx::query(
            "INSERT INTO slot_reservations (account_id, user_id, expires_at, seat_tier_id) \
             VALUES ($1, $2, now() + interval '5 minutes', $3)",
        )
        .bind(account_id)
        .bind(pending_id)
        .bind(standard_id)
        .execute(&pool)
        .await
        .expect("reserve a slot for an in-flight approval");

        let (stripe, calls) = spawn_recording_stripe_mock(&price_id, 3).await;
        let state = app_state(pool.clone(), Some(stripe)).await;
        let tenant = org_tenant(pool.clone(), &org_id, account_id, admin_id);

        let resp = reduce_slots(
            State(state),
            tenant,
            Extension(user_context(account_id, admin_id, "owner")),
            Json(ReduceSlotsRequest {
                tier: "standard".to_string(),
                quantity: 2,
                idempotency_key: Uuid::new_v4().to_string(),
            }),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::CONFLICT);
        let body = body_json(resp).await;
        assert_eq!(
            body["available"], 1,
            "purchased 3 - used 1 - reserved 1 = 1 free; a reservation is not headroom"
        );
        assert!(calls.lock().await.is_empty());
        assert_eq!(tier_ledger(&pool, grant_id, "standard").await, Some(3));

        // `slot_reservations` references both the account and the user;
        // drop it explicitly so `cleanup`'s account/user deletes are not
        // silently blocked by the FK (they swallow their own errors).
        let _ = sqlx::query("DELETE FROM slot_reservations WHERE account_id = $1")
            .bind(account_id)
            .execute(&pool)
            .await;
        cleanup(&pool, account_id, &[admin_id, pending_id]).await;
    }

    /// Reducing a tier to exactly zero DELETES its `grant_seat_tiers` row
    /// rather than storing `seats = 0` (0168's own CHECK forbids the latter,
    /// so a plain `seats - N` would error). The fixture's 1 purchased slot is
    /// held by its admin, so this frees the admin's seat first to make the
    /// slot reducible — which is also a small end-to-end proof of CS-444's
    /// whole point: a released slot becomes reducible pool headroom.
    #[tokio::test]
    async fn reduce_slots_to_zero_deletes_the_tier_row() {
        let Some(pool) = pg_test_pool("reduce_slots_to_zero_deletes_the_tier_row").await else {
            return;
        };
        let (account_id, org_id, admin_id, grant_id) = seed_org_with_pool(&pool, 1).await;
        let price_id = format!("price_org_slots_reduce_zero_{}", Uuid::new_v4().simple());
        sqlx::query("UPDATE seat_tiers SET stripe_price_id = $2 WHERE code = $1")
            .bind("standard")
            .bind(&price_id)
            .execute(&pool)
            .await
            .expect("set standard's stripe_price_id for this test");
        // The admin leaves their slot (what `release_seat` does now).
        sqlx::query("UPDATE account_members SET seat_purchased_at = NULL WHERE account_id = $1")
            .bind(account_id)
            .execute(&pool)
            .await
            .expect("free the admin's slot");

        let (stripe, calls) = spawn_recording_stripe_mock(&price_id, 1).await;
        let state = app_state(pool.clone(), Some(stripe)).await;
        let tenant = org_tenant(pool.clone(), &org_id, account_id, admin_id);

        let resp = reduce_slots(
            State(state),
            tenant,
            Extension(user_context(account_id, admin_id, "owner")),
            Json(ReduceSlotsRequest {
                tier: "standard".to_string(),
                quantity: 1,
                idempotency_key: Uuid::new_v4().to_string(),
            }),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK, "{:?}", body_json(resp).await);

        let recorded = calls.lock().await;
        assert!(
            recorded.iter().any(|c| c.starts_with("DELETE")),
            "the last slot on a tier must DELETE the Stripe item, not set quantity 0: {recorded:?}"
        );
        drop(recorded);
        assert_eq!(
            tier_ledger(&pool, grant_id, "standard").await,
            None,
            "the tier row must be gone entirely, not present with seats = 0"
        );

        cleanup(&pool, account_id, &[admin_id]).await;
    }

    /// The delete arm when Stripe has already drifted BELOW the ledger, and
    /// the admin is not reducing the whole tier. Stripe quantity 3, ledger
    /// purchased 5, reduce 3: `item.quantity > body.quantity` is false, so
    /// the item is DELETED (Stripe -> 0) while the local partition leaves
    /// `5 - 3 = 2` behind. Both sides move, they just do not meet.
    ///
    /// Pinned deliberately, for two reasons. It is the exact shape the
    /// previous revision got wrong twice: its drift warn fired only on
    /// `item.quantity < body.quantity`, so this `==` case passed silently,
    /// and its comment claimed "the ledger write below reconciles our side"
    /// — which is false whenever a remainder survives. And the resulting
    /// state (ledger 2, Stripe 0, nobody billed for two slots) is a real
    /// divergence an operator needs told about, which is what the second
    /// `warn!` on that arm now does.
    ///
    /// **The assertion is on the arithmetic, not on the log line, and that
    /// is a deliberate refusal rather than an omission.** This test would
    /// pass against the pre-fix code too — it pins the partition, not the
    /// `warn!`. Capturing the log instead was considered and rejected on
    /// this repo's own measured evidence: `backend/tests/
    /// retired_host_log_redaction.rs`' module doc records that `tracing`
    /// caches per-callsite `Interest` process-globally while the default
    /// subscriber is thread-local, so asserting on captured output from
    /// inside the `--lib` suite is racy — three shapes were tried there and
    /// flaked at roughly 1 run in 3 and 1 in 5, and the only deterministic
    /// answer was a dedicated integration binary holding exactly one test.
    /// That escape hatch is not available here: `routes` is a crate-private
    /// module, so `reduce_slots` cannot be named from an external test
    /// binary at all (this module's own header says so). A flaky assertion
    /// trains people to re-run it, which is worse than none — so the two
    /// `warn!`s on that arm are covered by review, and this test covers the
    /// numbers that would change if the partition were ever rewritten.
    #[tokio::test]
    async fn reduce_slots_deletes_a_drifted_stripe_item_and_keeps_the_local_remainder() {
        let Some(pool) =
            pg_test_pool("reduce_slots_deletes_a_drifted_stripe_item_and_keeps_the_local_remainder")
                .await
        else {
            return;
        };
        let (account_id, org_id, admin_id, grant_id) = seed_org_with_pool(&pool, 5).await;
        let price_id = format!("price_org_slots_reduce_drift_{}", Uuid::new_v4().simple());
        sqlx::query("UPDATE seat_tiers SET stripe_price_id = $2 WHERE code = $1")
            .bind("standard")
            .bind(&price_id)
            .execute(&pool)
            .await
            .expect("set standard's stripe_price_id for this test");
        // Stripe reports 3 for a tier the ledger says 5 were bought on — a
        // pre-existing drift this call did not cause.
        let (stripe, calls) = spawn_recording_stripe_mock(&price_id, 3).await;
        let state = app_state(pool.clone(), Some(stripe)).await;
        let tenant = org_tenant(pool.clone(), &org_id, account_id, admin_id);

        let resp = reduce_slots(
            State(state),
            tenant,
            Extension(user_context(account_id, admin_id, "owner")),
            Json(ReduceSlotsRequest {
                tier: "standard".to_string(),
                quantity: 3,
                idempotency_key: Uuid::new_v4().to_string(),
            }),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK, "{:?}", body_json(resp).await);

        let recorded = calls.lock().await;
        assert!(
            recorded.iter().any(|c| c.starts_with("DELETE")),
            "Stripe's quantity (3) is not greater than the reduction (3), so the item must be \
             deleted rather than set to zero: {recorded:?}"
        );
        drop(recorded);
        assert_eq!(
            tier_ledger(&pool, grant_id, "standard").await,
            Some(2),
            "the local partition keeps purchased - requested = 2; it does not follow Stripe to \
             zero, because this call was never told to remove five slots"
        );

        cleanup(&pool, account_id, &[admin_id]).await;
    }

    /// A `past_due` subscription may still REDUCE, even though it may not
    /// purchase — `billing::seat_lifecycle`'s own finding 7a reasoning
    /// (`past_due` is our enforcement state, not a Stripe inability to accept
    /// a DECREASE) applied to the one path that still pushes a decrease. The
    /// org most likely to want this route is the one behind on its bill;
    /// refusing it would trap them in the one-way ratchet the route exists to
    /// break, and a reduce can only ever lower what Stripe bills.
    #[tokio::test]
    async fn reduce_slots_is_allowed_while_past_due() {
        let Some(pool) = pg_test_pool("reduce_slots_is_allowed_while_past_due").await else {
            return;
        };
        let (account_id, org_id, admin_id, grant_id) = seed_org_with_pool(&pool, 4).await;
        let price_id = format!("price_org_slots_reduce_pastdue_{}", Uuid::new_v4().simple());
        sqlx::query("UPDATE seat_tiers SET stripe_price_id = $2 WHERE code = $1")
            .bind("standard")
            .bind(&price_id)
            .execute(&pool)
            .await
            .expect("set standard's stripe_price_id for this test");
        execute_financial_fixture(
            &pool,
            account_id,
            sqlx::query("UPDATE stripe_subscriptions SET status = 'past_due' WHERE account_id = $1")
            .bind(account_id),
        )
            .await
            .expect("freeze the subscription as past_due");
        let (stripe, _calls) = spawn_recording_stripe_mock(&price_id, 4).await;
        let state = app_state(pool.clone(), Some(stripe)).await;
        let tenant = org_tenant(pool.clone(), &org_id, account_id, admin_id);

        let resp = reduce_slots(
            State(state),
            tenant,
            Extension(user_context(account_id, admin_id, "owner")),
            Json(ReduceSlotsRequest {
                tier: "standard".to_string(),
                quantity: 2,
                idempotency_key: Uuid::new_v4().to_string(),
            }),
        )
        .await;
        assert_eq!(
            resp.status(),
            StatusCode::OK,
            "past_due must not block a reduce: {:?}",
            body_json(resp).await
        );
        assert_eq!(tier_ledger(&pool, grant_id, "standard").await, Some(2));

        cleanup(&pool, account_id, &[admin_id]).await;
    }

    /// A `canceled` subscription refuses with 409 — Stripe bills nothing
    /// there, so there is no item to decrement, and rewriting the local
    /// ledger to match a subscription that no longer exists would be a
    /// money-shaped write with no money behind it. (Flagged in CS-444's
    /// report as a policy call the founder can overturn.)
    #[tokio::test]
    async fn reduce_slots_refuses_a_canceled_subscription() {
        let Some(pool) = pg_test_pool("reduce_slots_refuses_a_canceled_subscription").await else {
            return;
        };
        let (account_id, org_id, admin_id, grant_id) = seed_org_with_pool(&pool, 4).await;
        execute_financial_fixture(
            &pool,
            account_id,
            sqlx::query("UPDATE stripe_subscriptions SET status = 'canceled' WHERE account_id = $1")
            .bind(account_id),
        )
            .await
            .expect("cancel the subscription");
        let (stripe, calls) = spawn_recording_stripe_mock("price_never_used", 4).await;
        let state = app_state(pool.clone(), Some(stripe)).await;
        let tenant = org_tenant(pool.clone(), &org_id, account_id, admin_id);

        let resp = reduce_slots(
            State(state),
            tenant,
            Extension(user_context(account_id, admin_id, "owner")),
            Json(ReduceSlotsRequest {
                tier: "standard".to_string(),
                quantity: 1,
                idempotency_key: Uuid::new_v4().to_string(),
            }),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::CONFLICT);
        assert!(calls.lock().await.is_empty());
        assert_eq!(tier_ledger(&pool, grant_id, "standard").await, Some(4));

        cleanup(&pool, account_id, &[admin_id]).await;
    }

    /// A Stripe 5xx maps through `map_stripe_error_to_response` (502) and
    /// changes NOTHING locally — the mirror of
    /// `purchase_slots_stripe_5xx_changes_nothing_locally`, and the reason
    /// the ledger write sits AFTER the push rather than before it.
    #[tokio::test]
    async fn reduce_slots_stripe_5xx_changes_nothing_locally() {
        let Some(pool) = pg_test_pool("reduce_slots_stripe_5xx_changes_nothing_locally").await
        else {
            return;
        };
        let (account_id, org_id, admin_id, grant_id) = seed_org_with_pool(&pool, 4).await;
        let price_id = format!("price_org_slots_reduce_5xx_{}", Uuid::new_v4().simple());
        sqlx::query("UPDATE seat_tiers SET stripe_price_id = $2 WHERE code = $1")
            .bind("standard")
            .bind(&price_id)
            .execute(&pool)
            .await
            .expect("set standard's stripe_price_id for this test");
        let stripe = spawn_500_mock().await;
        let state = app_state(pool.clone(), Some(stripe)).await;
        let tenant = org_tenant(pool.clone(), &org_id, account_id, admin_id);

        let resp = reduce_slots(
            State(state),
            tenant,
            Extension(user_context(account_id, admin_id, "owner")),
            Json(ReduceSlotsRequest {
                tier: "standard".to_string(),
                quantity: 2,
                idempotency_key: Uuid::new_v4().to_string(),
            }),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::BAD_GATEWAY, "a Stripe 5xx must map to 502");
        assert_eq!(
            tier_ledger(&pool, grant_id, "standard").await,
            Some(4),
            "the pre-existing ledger must be untouched by a failed push"
        );

        cleanup(&pool, account_id, &[admin_id]).await;
    }

    /// A plain member cannot reduce the pool — same [`require_org_admin`]
    /// bar `purchase_slots` uses. Spending money and un-spending it are the
    /// same authority.
    #[tokio::test]
    async fn reduce_slots_refuses_a_non_admin() {
        let Some(pool) = pg_test_pool("reduce_slots_refuses_a_non_admin").await else {
            return;
        };
        let (account_id, org_id, admin_id, grant_id) = seed_org_with_pool(&pool, 3).await;
        let member_id: Uuid = sqlx::query_scalar("INSERT INTO users (email) VALUES ($1) RETURNING id")
            .bind(format!("{}@org-slots-reduce-member.test", Uuid::new_v4()))
            .fetch_one(&pool)
            .await
            .expect("insert plain member");
        let standard_id = seat_tier_id(&pool, "standard").await;
        sqlx::query(
            "INSERT INTO account_members (account_id, user_id, role, seat_tier_id) VALUES ($1, $2, 'member', $3)",
        )
        .bind(account_id)
        .bind(member_id)
        .bind(standard_id)
        .execute(&pool)
        .await
        .expect("insert plain membership");
        let state = app_state(pool.clone(), None).await;
        let tenant = org_tenant(pool.clone(), &org_id, account_id, member_id);

        let resp = reduce_slots(
            State(state),
            tenant,
            Extension(user_context(account_id, member_id, "member")),
            Json(ReduceSlotsRequest {
                tier: "standard".to_string(),
                quantity: 1,
                idempotency_key: Uuid::new_v4().to_string(),
            }),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::FORBIDDEN);
        assert_eq!(tier_ledger(&pool, grant_id, "standard").await, Some(3));

        cleanup(&pool, account_id, &[admin_id, member_id]).await;
    }

    /// A personal org refuses with 403, same as [`purchase_slots`] — there is
    /// no pool to reduce.
    #[tokio::test]
    async fn reduce_slots_refuses_a_personal_org() {
        let Some(pool) = pg_test_pool("reduce_slots_refuses_a_personal_org").await else {
            return;
        };
        let (account_id, org_id, user_id) = seed_personal_org(&pool).await;
        let state = app_state(pool.clone(), None).await;
        let tenant = org_tenant(pool.clone(), &org_id, account_id, user_id);

        let resp = reduce_slots(
            State(state),
            tenant,
            Extension(user_context(account_id, user_id, "owner")),
            Json(ReduceSlotsRequest {
                tier: "standard".to_string(),
                quantity: 1,
                idempotency_key: Uuid::new_v4().to_string(),
            }),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::FORBIDDEN);

        cleanup(&pool, account_id, &[user_id]).await;
    }

    /// Quantity is validated before anything else moves — `0` and `501` are
    /// 400s, not silent no-ops and not 500s.
    #[tokio::test]
    async fn reduce_slots_rejects_an_out_of_range_quantity() {
        let Some(pool) = pg_test_pool("reduce_slots_rejects_an_out_of_range_quantity").await else {
            return;
        };
        let (account_id, org_id, admin_id, _grant_id) = seed_org_with_pool(&pool, 3).await;
        let state = app_state(pool.clone(), None).await;

        for quantity in [0_i64, 501] {
            let resp = reduce_slots(
                State(state.clone()),
                org_tenant(pool.clone(), &org_id, account_id, admin_id),
                Extension(user_context(account_id, admin_id, "owner")),
                Json(ReduceSlotsRequest {
                    tier: "standard".to_string(),
                    quantity,
                    idempotency_key: Uuid::new_v4().to_string(),
                }),
            )
            .await;
            assert_eq!(resp.status(), StatusCode::BAD_REQUEST, "quantity {quantity} must be refused");
        }

        cleanup(&pool, account_id, &[admin_id]).await;
    }
}
