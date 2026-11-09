//! `POST /v1/billing/checkout/subscription` and `POST /v1/billing/checkout/pack`
//! — the OUTBOUND half of billing self-serve checkout
//! (`.scratch/billing-self-serve/plan.md` §1.1, §1.6, §1.8, §1.10, §2.D). The
//! completion side (crediting a grant or a pack once Stripe confirms payment)
//! lives entirely in `routes::stripe_webhook`'s `checkout.session.completed`
//! handling — this module only ever creates Stripe objects and returns a URL
//! to redirect the browser to. Nothing here writes a grant or credits a
//! balance; that would be trusting the client's word that a payment
//! succeeded, which is exactly the class of bug Stripe Checkout (server
//! creates the session, Stripe hosts the payment page, a signed webhook
//! confirms the result) exists to prevent.
//!
//! # Customer pre-creation kills the webhook ordering race (plan §1.8)
//!
//! Both routes below call [`ensure_stripe_customer`] BEFORE creating a
//! Checkout Session: it creates the Stripe Customer (idempotency-keyed
//! `cust:{account_id}` — see [`crate::billing::stripe_client::StripeClient::
//! create_customer`]) and writes the `stripe_subscriptions` mapping row
//! FIRST, so that by the time a session could possibly complete,
//! `resolve_stripe_customer_account_id` (`routes::stripe_webhook`'s
//! bootstrap resolver) already resolves this customer id back to this
//! account. Without this ordering, a webhook racing ahead of the mapping
//! write would hit the "unknown customer" 200-drop path
//! (`routes::stripe_webhook`'s own module doc) and silently lose the
//! purchase.
//!
//! # Fail loud on an unprovisioned price (plan §2.D item 1)
//!
//! `plans.stripe_price_id` / `seat_tiers.stripe_price_id` /
//! `credit_packs.stripe_price_id` are all nullable — `NULL` until
//! `scripts/provision_stripe.sh` (PR C) has run for this environment. A row
//! that exists but carries a NULL price id is treated as an INTERNAL error
//! (500 + a loud `tracing::error!` naming exactly which catalog entry is
//! unprovisioned), never as "unknown tier" (400) — the tier/pack is real,
//! money-collection for it just is not wired up yet, and that is this
//! deployment's bug to fix, not the caller's bad request.
//!
//! # Auth: org-admin for org accounts, the account's own user for personal
//!
//! Both routes are money-moving actions, so both get the same authority bar
//! `routes::teams`'s org-admin-gated mutations use: a REAL org account
//! (`accounts.clerk_org_id IS NOT NULL`, excluding a personal org — see
//! [`require_purchase_authority`]'s own doc comment) requires the caller's
//! `account_members.role` to be `owner`/`admin`; a PERSONAL account — plain,
//! or a personal org's own relabelled one — has exactly one user and that
//! user may always purchase for their own account. See
//! [`require_purchase_authority`].
//!
//! # Checkout-session creation is deliberately NOT idempotency-keyed
//!
//! Matches [`crate::billing::stripe_client::StripeClient::
//! create_checkout_session_subscription`]'s own doc comment: a session is
//! not a charge, and a user clicking "Upgrade" twice legitimately wants a
//! fresh session with a fresh expiry. Only the CUSTOMER creation above is
//! idempotency-keyed.
//!
//! # No UI yet (PR H) — but the routes are mounted and reachable today
//!
//! Nothing in this tree CALLS these routes yet — they exist so PR E/F/G/H
//! and a future frontend have something to call. That is a schedule fact
//! about who dials in, not a control: both handlers are wired into the live
//! router (`lib.rs`) behind normal auth, so any authenticated caller can
//! invoke them right now.
//!
//! `success_url`/`cancel_url` are caller-supplied (the future UI knows its
//! own return routes). [`validate_return_urls`] restricts both to an origin
//! this deployment owns (billing self-serve PR D review round, CodeRabbit
//! finding on `checkout.rs:391`) — Stripe redirects the PAYING browser to
//! `success_url` once checkout completes, so a caller-controlled redirect
//! target would let an authenticated attacker mint a genuine
//! `checkout.stripe.com` link (trusted domain, real Stripe-hosted page) that
//! sends a victim's post-payment browser to attacker infrastructure. See
//! that function's own doc comment for the exact guard.

use std::sync::Arc;

use axum::extract::{Extension, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::{Deserialize, Serialize};
use serde_json::json;
use uuid::Uuid;

use crate::billing::stripe_client::Idem;
use crate::middleware::auth::UserContext;
use crate::AppState;

// ── Wire types ───────────────────────────────────────────────────────────

/// `tier` is either the flat `plans.code` `"pro"`, or a `seat_tiers.code`
/// (`"standard"`/`"premium"`) for a team seat purchase. Checkout always buys
/// exactly ONE seat at the chosen tier (plan §1.6) — there is deliberately
/// no `quantity` field here, unlike the pack request below.
#[derive(Debug, Deserialize)]
pub struct SubscriptionCheckoutRequest {
    pub tier: String,
    pub success_url: String,
    pub cancel_url: String,
}

#[derive(Debug, Deserialize)]
pub struct PackCheckoutRequest {
    /// `credit_packs.code` (e.g. `"pack_500"`).
    pub pack: String,
    #[serde(default = "default_pack_quantity")]
    pub quantity: i64,
    pub success_url: String,
    pub cancel_url: String,
}

fn default_pack_quantity() -> i64 {
    1
}

#[derive(Debug, Serialize)]
pub struct CheckoutResponse {
    /// The URL to redirect the browser to — Stripe-hosted, one-time use.
    pub checkout_url: String,
    pub session_id: String,
}

// ── Shared helpers ──────────────────────────────────────────────────────

fn bad_request(msg: &str) -> Response {
    (StatusCode::BAD_REQUEST, Json(json!({"error": msg}))).into_response()
}

fn service_unavailable() -> Response {
    (StatusCode::SERVICE_UNAVAILABLE, Json(json!({"error": "service unavailable"}))).into_response()
}

fn internal_error(what: &str, e: impl std::fmt::Display) -> Response {
    tracing::error!(error = %e, checkout_step = what, "checkout: failed, refusing to fabricate a session");
    (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": "internal error"}))).into_response()
}

/// Money-moving authorization gate shared by both routes in this file, AND
/// (billing self-serve PR F review, minor finding — dedupe) by
/// `routes::billing_seats::require_org_admin`. `accounts` carries no RLS
/// policy (same posture `billing::enforcement::enforcement_enabled` already
/// reads it under — see that module's own doc comment); `account_members`/
/// role resolution goes through `state.control_store()`, matching
/// `routes::teams::has_account_authority`'s shape (that function is private
/// to `teams.rs`, hence a small, intentionally near-identical copy here
/// rather than a cross-module dependency on a route file's internals).
///
/// `org_only`: this file's two routes accept a PERSONAL account caller too
/// (a personal account has exactly one user, and it is always allowed to
/// purchase for itself — `false` short-circuits `Ok(())` for one).
/// `routes::billing_seats`' tier-change route only ever makes sense for a
/// seat-priced ORG member, so it passes `true` and gets a `400` instead of a
/// silent pass-through for a personal-account caller — an earlier version of
/// this gate lived as two independently-copy-pasted functions (one per
/// module) differing ONLY in this one branch; `org_only` collapses them back
/// into the single money-moving authority check both files defer to.
///
/// A personal org (control migration 0208 `personal_orgs`) takes the SAME
/// `org_only` branch a plain personal account does, not the org one below.
/// `accounts.is_org_account` cannot tell the two apart — the seat-tier
/// trigger that sets it (`0169_account_members_seat_tier.sql:342`) derives it
/// from the same `clerk_org_id IS NOT NULL` test a personal org's relabelled
/// one-person account also satisfies — so this calls
/// [`crate::store::Store::account_is_real_org`] (PR 829 review, finding 3:
/// this used to duplicate that method's SQL inline; now it calls it, so the
/// two can never drift apart) rather than reading `clerk_org_id` itself. Left
/// unexcluded, a personal-org owner (who always holds `role = 'owner'` on
/// their own account) would sail past the role check below and be able to
/// BUY a seat tier for an account the seat-lifecycle constraints
/// (`0169_account_members_seat_tier.sql:493`'s `account_members_seat_tier_
/// iff_org` CHECK, `0176_seat_lifecycle.sql`'s insert-time trigger) require
/// to already silently carry one — see `bin/adopt_personal_org_account.rs`'s
/// own doc comment for why that row exists without ever having been bought.
///
/// Fails CLOSED on a missing `accounts` row — `account_is_real_org` itself
/// now does too (PR 829 review, finding 4: it used to read `Ok(false)` for a
/// missing row, fail-OPEN for its other two callers; this function has always
/// treated the same case as an error, so making the shared method match this
/// one's posture closed that gap for everyone at once).
pub(crate) async fn require_purchase_authority(
    state: &AppState,
    user: &UserContext,
    org_only: bool,
) -> Result<(), Response> {
    // Settings launch PR 2.8: spending the account's money is a human,
    // browser-session action — never a PAT, add-in or OAuth client.
    if let Some(refusal) = crate::middleware::auth::interactive_session_refusal() {
        return Err(refusal);
    }
    let Some(store) = state.control_store() else {
        return Err(service_unavailable());
    };
    let is_real_org = match store.account_is_real_org(user.account_id, user.user_id).await {
        Ok(v) => v,
        Err(crate::store::StoreError::NotFound) => {
            return Err(internal_error(
                "account lookup",
                "no accounts row for an authenticated user — should be unreachable",
            ))
        }
        Err(e) => return Err(internal_error("account lookup", e)),
    };
    if !is_real_org {
        if org_only {
            return Err(bad_request("seat tiers only apply to org accounts"));
        }
        // A personal account — plain, or a personal org's own relabelled
        // account — has exactly one user, and it is the caller —
        // `require_auth` already proved that. Nothing further to check.
        return Ok(());
    }
    match store.get_account_member_role(user.account_id, user.user_id).await {
        Ok(role) if matches!(role.as_deref(), Some("owner") | Some("admin")) => Ok(()),
        Ok(_) => Err((
            StatusCode::FORBIDDEN,
            Json(json!({"error": "only an org admin can purchase billing for this account"})),
        )
            .into_response()),
        Err(e) => Err(internal_error("role check", e)),
    }
}

/// Pre-create the Stripe Customer FIRST (plan §1.8) and write the
/// `stripe_subscriptions` mapping row BEFORE any Checkout Session exists —
/// see this module's own doc comment for why the ordering matters.
///
/// Idempotent both directions: `create_customer`'s `cust:{account_id}`
/// idempotency key means a retried call resolves the SAME Stripe customer
/// id every time, and the INSERT below is `ON CONFLICT DO NOTHING` — so two
/// concurrent checkout attempts for an account with no existing mapping
/// never create two Stripe customers or fight over which mapping row wins;
/// both resolve to the same customer id and the row is written once.
pub(crate) async fn ensure_stripe_customer(state: &AppState, account_id: Uuid) -> Result<String, Response> {
    let Some(control) = state.control_db() else {
        return Err(service_unavailable());
    };

    // Reuse an existing mapping row if one is already there — the common
    // case for any account past its FIRST checkout ever (a pack top-up on an
    // already-subscribed account, a re-subscribe after a freeze).
    let existing: Option<String> = {
        let mut tx = crate::tenancy::scope::begin_account_scoped(control, account_id)
            .await
            .map_err(|e| internal_error("stripe_subscriptions scope", e))?;
        let row: Option<String> =
            sqlx::query_scalar("SELECT stripe_customer_id FROM stripe_subscriptions WHERE account_id = $1")
                .bind(account_id)
                .fetch_optional(&mut *tx)
                .await
                .map_err(|e| internal_error("stripe_subscriptions read", e))?;
        tx.commit().await.map_err(|e| internal_error("stripe_subscriptions commit", e))?;
        row
    };
    if let Some(customer_id) = existing {
        return Ok(customer_id);
    }

    // `state.stripe == None` ⇒ 503, charging/creating nothing — the outbound
    // fail-closed posture `billing::stripe_client`'s module doc describes.
    let Some(stripe) = state.stripe.as_ref() else {
        tracing::error!(
            %account_id,
            "checkout: STRIPE_SECRET_KEY not configured — refusing the request, creating nothing"
        );
        return Err(service_unavailable());
    };

    // The account owner's email — `users`/`accounts` carry no RLS (see
    // `require_purchase_authority`'s own doc comment), so this is a plain
    // join on the control pool.
    let email: Option<String> = sqlx::query_scalar(
        "SELECT u.email FROM users u JOIN accounts a ON a.owner_user_id = u.id WHERE a.id = $1",
    )
    .bind(account_id)
    .fetch_optional(control)
    .await
    .map_err(|e| internal_error("owner email lookup", e))?;
    let Some(email) = email else {
        return Err(internal_error(
            "owner email lookup",
            "no owner user found for account — should be unreachable",
        ));
    };

    let idem_key = format!("cust:{account_id}");
    let customer = stripe
        .create_customer(&account_id.to_string(), &email, None, Idem::Key(&idem_key))
        .await
        .map_err(|e| internal_error("stripe create_customer", e))?;

    let mut tx = crate::tenancy::scope::begin_account_scoped(control, account_id)
        .await
        .map_err(|e| internal_error("stripe_subscriptions scope", e))?;
    crate::billing::renewal::lock_period_rotation(&mut tx, account_id)
        .await
        .map_err(|e| internal_error("stripe_subscriptions funding lock", e))?;
    sqlx::query(
        // `pending`, never the column default (`active`): this row only maps a
        // customer to the account, no subscription exists yet. The default
        // made every abandoned Checkout read as a live subscription and
        // `refuse_if_already_subscribed` 409'd every retry.
        "INSERT INTO stripe_subscriptions (account_id, stripe_customer_id, status) \
         VALUES ($1, $2, 'pending') ON CONFLICT (account_id) DO NOTHING",
    )
    .bind(account_id)
    .bind(&customer.id)
    .execute(&mut *tx)
    .await
    .map_err(|e| internal_error("stripe_subscriptions insert", e))?;
    // Re-read the canonical stored value rather than trusting `customer.id`
    // directly — a concurrent request may have won the INSERT race.
    // `create_customer`'s idempotency key guarantees both requests resolved
    // the identical Stripe customer id regardless, so this is never a "which
    // one wins" question, only "read back the one row that is there".
    let stored: String =
        sqlx::query_scalar("SELECT stripe_customer_id FROM stripe_subscriptions WHERE account_id = $1")
            .bind(account_id)
            .fetch_one(&mut *tx)
            .await
            .map_err(|e| internal_error("stripe_subscriptions re-read", e))?;
    tx.commit().await.map_err(|e| internal_error("stripe_subscriptions commit", e))?;

    Ok(stored)
}

/// Both return URLs must resolve to an origin THIS deployment owns (billing
/// self-serve PR D review round, CodeRabbit finding on `checkout.rs:391`,
/// also applying to the pack route's identical check).
///
/// # The abuse this closes
///
/// Stripe redirects the paying browser to `success_url` once checkout
/// completes. Before this guard, the only check was "non-empty" (see this
/// module's own doc comment, "No UI yet"). An authenticated attacker could
/// call either route with `success_url` set to a host they control, then
/// hand the returned `https://checkout.stripe.com/...` link to a victim —
/// a genuine Stripe-hosted page on a trusted domain, so nothing about the
/// link itself looks wrong. After the victim pays with their OWN card,
/// Stripe redirects their browser to the attacker's host, carrying whatever
/// "payment succeeded" context the query string leaks. The trusted Stripe
/// origin is exactly what makes the redirect convincing; "no UI calls this
/// yet" never stopped the route from being reachable.
///
/// # Which origin is "ours"
///
/// Reuses [`AppState::personal_web_base_url`] / [`AppState::org_web_base_url`]
/// via the single dispatcher [`AppState::web_base_url`] — the SAME origin
/// family `is_allowed_cors_origin` already trusts for this deployment's own
/// UI (see that function's own doc comment), resolved for the CALLING
/// account's plane. Org subdomains are retired, so for an org account this is
/// the same `app.{web_apex}` origin as for a personal one
/// (`AppState::org_web_base_url`).
///
/// # Match shape
///
/// `url == base || url.starts_with(&format!("{base}/"))` — anchored on the
/// full `scheme://host` string, not a suffix match, so
/// `https://app.coralswarm.example.evil.com` (a real different origin that
/// merely CONTAINS the trusted host as a prefix) is correctly rejected: the
/// character immediately after `base` in that string is `.`, not `/`, and
/// the string is not byte-equal to `base` either.
pub(crate) async fn validate_return_urls(
    state: &AppState,
    control: &sqlx::PgPool,
    account_id: Uuid,
    success_url: &str,
    cancel_url: &str,
) -> Result<(), Response> {
    let clerk_org_id: Option<Option<String>> =
        sqlx::query_scalar("SELECT clerk_org_id FROM accounts WHERE id = $1")
            .bind(account_id)
            .fetch_optional(control)
            .await
            .map_err(|e| internal_error("account clerk_org_id lookup", e))?;
    // Missing row: unreachable in practice (`require_purchase_authority`
    // already proved this account exists on every path that reaches here),
    // but falling back to the personal base rather than erroring keeps this
    // helper from becoming a second place account-existence is asserted.
    let clerk_org_id: Option<String> = clerk_org_id.flatten();
    let base = state.web_base_url(control, clerk_org_id.as_deref()).await;

    for (name, url) in [("success_url", success_url), ("cancel_url", cancel_url)] {
        if url != base && !url.starts_with(&format!("{base}/")) {
            tracing::warn!(
                %account_id, field = name, url,
                "checkout: return URL is not on this deployment's own origin — refusing"
            );
            return Err(bad_request(&format!(
                "{name} must point at this application's own origin ({base})"
            )));
        }
    }
    Ok(())
}

/// Whether a `stripe_subscriptions` row stands for a subscription that is
/// really alive at Stripe, so a second Checkout would double-bill.
///
/// Needs BOTH a live status (`active`/`trialing`/`past_due`) AND a Stripe
/// subscription id. The row is also written as a bare customer mapping before
/// any Checkout exists (`ensure_stripe_customer`); that row is `pending`, and
/// rows written before `pending` existed say `active` with no subscription id.
/// Neither is a subscription, and treating them as one locked the account out
/// of every retry after an abandoned or failed Checkout. `canceled` stays
/// excluded: resubscribing is the intended recovery path.
fn blocks_new_subscription_checkout(status: &str, stripe_subscription_id: Option<&str>) -> bool {
    stripe_subscription_id.is_some() && matches!(status, "active" | "trialing" | "past_due")
}

/// Refuse a subscription checkout for an account that already holds a LIVE
/// subscription (finding 5, billing self-serve PR D review round). Without
/// this, a second checkout for an already-subscribed account creates a
/// genuinely SECOND Stripe subscription (Checkout Session creation has no
/// idea an active one already exists — see this module's own doc comment on
/// why session creation is deliberately not idempotency-keyed) and
/// double-bills the account the moment both come due.
///
/// **`active`/`past_due` only — `canceled` is deliberately EXCLUDED.** A
/// canceled account resubscribing through this exact route is the intended
/// recovery path (freeze-not-forfeit, plan §1.11): `checkout.session
/// .completed`/`invoice.paid` restore `status = "active"` on the SAME
/// `stripe_subscriptions` row exactly like an involuntary `past_due` lapse is
/// restored, so refusing a canceled account here would make resubscribing
/// impossible.
///
/// Tier CHANGES on an already-active subscription are out of scope for this
/// PR (plan §2.F, seat lifecycle / tier-change routes) — the error message
/// says so rather than leaving the caller to guess why a plausible-looking
/// request was refused.
///
/// # KNOWN GAP, not closed by this function — needs a founder decision
///
/// This check commits its own read-only transaction and returns BEFORE
/// [`create_subscription_checkout`] calls Stripe to create the session
/// (billing self-serve PR D review round, CodeRabbit finding on
/// `checkout.rs:312`, "Major / Heavy lift"). Two concurrent requests for an
/// account with `NULL`/`canceled` status can both pass this check and both
/// reach Stripe — the second `checkout.session.completed` webhook then
/// unconditionally overwrites `stripe_subscriptions.stripe_subscription_id`
/// (see `routes::stripe_webhook::apply_checkout_completed_subscription`'s
/// mirror UPDATE) without cancelling the first subscription at Stripe, so a
/// truly double-clicked (or two-device) resubscribe can leave a SECOND live
/// Stripe subscription billing the account that CoralSwarm's own row no
/// longer references.
///
/// **Deliberately NOT fixed here — both real fixes are a design decision,
/// not a quick patch, and this PR does not half-implement either:**
/// - Hold a lock (advisory or transactional) from this check through
///   Stripe's session-creation call. `billing::renewal::lock_period_rotation`
///   is the existing per-account advisory-lock precedent in this codebase,
///   but it is a `pg_advisory_XACT_lock` — scoped to (and released at the
///   end of) a DB transaction — and this handler's Stripe call is an
///   external HTTP round trip typically well outside any transaction's
///   welcome duration. Holding a transaction (or even a session-level
///   advisory lock on a checked-out connection) open across that call ties
///   up a pooled DB connection for Stripe's latency, which is a real
///   availability tradeoff under load, not a free win.
/// - Reservation logic (write a "checkout in flight" marker before calling
///   Stripe, reconcile/expire it in the webhook or a cleanup pass) needs new
///   state this PR's schema does not have.
///
/// The race requires literal concurrent double-submission by the account's
/// own authorized purchaser (double-click / two devices), not a
/// cross-tenant or unauthenticated attack — a real but narrow window. Left
/// open for the founder to pick a direction rather than shipped half-done.
pub(crate) async fn refuse_if_already_subscribed(state: &AppState, account_id: Uuid) -> Result<(), Response> {
    let Some(control) = state.control_db() else {
        return Err(service_unavailable());
    };
    let mut tx = crate::tenancy::scope::begin_account_scoped(control, account_id)
        .await
        .map_err(|e| internal_error("existing subscription scope", e))?;
    let row: Option<(String, Option<String>)> = sqlx::query_as(
        "SELECT status, stripe_subscription_id FROM stripe_subscriptions WHERE account_id = $1",
    )
    .bind(account_id)
    .fetch_optional(&mut *tx)
    .await
    .map_err(|e| internal_error("existing subscription lookup", e))?;
    tx.commit().await.map_err(|e| internal_error("existing subscription commit", e))?;

    if row.is_some_and(|(status, sub_id)| blocks_new_subscription_checkout(&status, sub_id.as_deref())) {
        return Err((
            StatusCode::CONFLICT,
            Json(json!({
                "error": "this account already has an active or past-due subscription — tier \
                          changes are coming in a future release"
            })),
        )
            .into_response());
    }
    Ok(())
}

/// The three catalog tables that carry a nullable `stripe_price_id`
/// (billing self-serve PR D review round, CodeRabbit finding on
/// `checkout.rs:354`: [`lookup_stripe_price_id`] used to take the table name
/// as a bare `&str` and `format!` it straight into the SQL text. Every call
/// site today passes a literal, so there was never an actual injection —
/// what was wrong is that the SIGNATURE invited one: nothing stopped a
/// future caller from threading a request-derived value into `table`, and
/// the type system had no opinion about it. A closed enum makes that
/// mistake unrepresentable rather than merely unpracticed — the `code`
/// binding stays a real bind parameter either way.
#[derive(Debug, Clone, Copy)]
enum PriceCatalog {
    Plans,
    SeatTiers,
    CreditPacks,
}

impl PriceCatalog {
    fn select_sql(self) -> &'static str {
        match self {
            Self::Plans => "SELECT stripe_price_id FROM plans WHERE code = $1 AND is_active = true",
            Self::SeatTiers => {
                "SELECT stripe_price_id FROM seat_tiers WHERE code = $1 AND is_active = true"
            }
            Self::CreditPacks => {
                "SELECT stripe_price_id FROM credit_packs WHERE code = $1 AND is_active = true"
            }
        }
    }
}

/// `plans.stripe_price_id` / `seat_tiers.stripe_price_id` /
/// `credit_packs.stripe_price_id` are all nullable (`NULL` until PR C's
/// provisioning script has run). `Ok(None)` means "no active row with this
/// code" (a real 400 — the caller asked for a tier/pack that does not
/// exist); a row that EXISTS but carries a NULL price id is folded into the
/// `Err` arm by the caller instead (see [`resolve_price_or_fail_loudly`]).
async fn lookup_stripe_price_id(
    control: &sqlx::PgPool,
    table: PriceCatalog,
    code: &str,
) -> Result<Option<Option<String>>, sqlx::Error> {
    sqlx::query_scalar(table.select_sql()).bind(code).fetch_optional(control).await
}

/// Wraps [`lookup_stripe_price_id`] with this module's fail-loud-on-NULL
/// posture (this module's own doc comment) — the one place that logic is
/// stated, so the subscription and pack routes cannot drift on it.
async fn resolve_price_or_fail_loudly(
    control: &sqlx::PgPool,
    table: PriceCatalog,
    code: &str,
    what: &str,
) -> Result<String, Response> {
    match lookup_stripe_price_id(control, table, code).await {
        Ok(Some(Some(price_id))) => Ok(price_id),
        Ok(Some(None)) => {
            tracing::error!(
                %code, ?table, "checkout: {what} exists but stripe_price_id is NULL — provisioning has not run"
            );
            Err(internal_error("price lookup", format!("{what} is not provisioned")))
        }
        Ok(None) => Err(bad_request(&format!("unknown or inactive {what}"))),
        Err(e) => Err(internal_error("price lookup", e)),
    }
}

/// The Stripe price of a `seat_tiers` code, with this module's
/// fail-loud-on-NULL posture. Used by `routes::org_trial_checkout`.
pub(crate) async fn resolve_seat_tier_price(control: &sqlx::PgPool, code: &str) -> Result<String, Response> {
    resolve_price_or_fail_loudly(control, PriceCatalog::SeatTiers, code, "seat tier").await
}

// ── Handlers ─────────────────────────────────────────────────────────────

/// `POST /v1/billing/checkout/subscription` — authed (org-admin for an org
/// account, the account's own user for personal). Creates a `mode:
/// subscription` Checkout Session for exactly one seat at `tier` and returns
/// its URL. Never writes a grant — see this module's own doc comment.
pub async fn create_subscription_checkout(
    State(state): State<Arc<AppState>>,
    Extension(user): Extension<UserContext>,
    Json(body): Json<SubscriptionCheckoutRequest>,
) -> Response {
    if body.success_url.trim().is_empty() || body.cancel_url.trim().is_empty() {
        return bad_request("success_url and cancel_url are required");
    }
    if let Err(resp) = require_purchase_authority(&state, &user, false).await {
        return resp;
    }
    let Some(control) = state.control_db() else {
        return service_unavailable();
    };
    if let Err(resp) =
        validate_return_urls(&state, control, user.account_id, &body.success_url, &body.cancel_url).await
    {
        return resp;
    }
    if let Err(resp) = refuse_if_already_subscribed(&state, user.account_id).await {
        return resp;
    }

    // "pro" is a flat plan (`plans`); anything else is a seat-tier code
    // (`seat_tiers`) for a team seat.
    let price_id = if body.tier == "pro" {
        match resolve_price_or_fail_loudly(control, PriceCatalog::Plans, "pro", "the 'pro' plan").await {
            Ok(p) => p,
            Err(resp) => return resp,
        }
    } else {
        match resolve_price_or_fail_loudly(control, PriceCatalog::SeatTiers, &body.tier, "seat tier").await {
            Ok(p) => p,
            Err(resp) => return resp,
        }
    };

    let customer_id = match ensure_stripe_customer(&state, user.account_id).await {
        Ok(id) => id,
        Err(resp) => return resp,
    };

    let Some(stripe) = state.stripe.as_ref() else {
        return service_unavailable();
    };
    // `allow_promotion_codes: true` — no reason recorded to refuse launch
    // promos, and the plan does not call this out either way.
    match stripe
        .create_checkout_session_subscription(
            &customer_id,
            &price_id,
            &user.account_id.to_string(),
            &body.success_url,
            &body.cancel_url,
            true,
        )
        .await
    {
        Ok(session) => {
            replace_pending_checkout_session(&state, stripe, user.account_id, &session.id).await;
            (
                StatusCode::OK,
                Json(CheckoutResponse { checkout_url: session.url, session_id: session.id }),
            )
                .into_response()
        }
        Err(e) => internal_error("stripe create checkout session", e),
    }
}

/// Remember `new_session_id` as the account's open subscription Checkout and
/// retire the one it replaces. A new Checkout supersedes an abandoned one, so
/// the old session must not stay payable: if both completed the account would
/// hold two subscriptions.
///
/// Best effort on purpose. The new session is already created and valid, so a
/// failure here (database, or Stripe refusing to expire a session that is
/// already expired or completed) is logged and never turned into an error for
/// the caller. The write runs inside the 0321 financial entry fence
/// (`lock_period_rotation`) like every other write to `stripe_subscriptions`.
pub(crate) async fn replace_pending_checkout_session(
    state: &AppState,
    stripe: &crate::billing::stripe_client::StripeClient,
    account_id: Uuid,
    new_session_id: &str,
) {
    let Some(control) = state.control_db() else { return };
    let previous: Result<Option<String>, sqlx::Error> = async {
        let mut tx = crate::tenancy::scope::begin_account_scoped(control, account_id)
            .await
            .map_err(|e| sqlx::Error::Protocol(e.to_string()))?;
        crate::billing::renewal::lock_period_rotation(&mut tx, account_id).await?;
        let previous: Option<String> = sqlx::query_scalar(
            "SELECT pending_checkout_session_id FROM stripe_subscriptions WHERE account_id = $1 FOR UPDATE",
        )
        .bind(account_id)
        .fetch_optional(&mut *tx)
        .await?
        .flatten();
        sqlx::query("UPDATE stripe_subscriptions SET pending_checkout_session_id = $2 WHERE account_id = $1")
            .bind(account_id)
            .bind(new_session_id)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(previous)
    }
    .await;
    match previous {
        Ok(Some(old)) if old != new_session_id => {
            if let Err(e) = stripe.expire_checkout_session(&old).await {
                tracing::warn!(
                    error = %e, %account_id, old_session = %old,
                    "checkout: could not expire the superseded Checkout session (best effort; it may already be expired or completed)"
                );
            }
        }
        Ok(_) => {}
        Err(e) => tracing::warn!(
            error = %e, %account_id, session = new_session_id,
            "checkout: could not record the open Checkout session (best effort; the session itself is valid)"
        ),
    }
}

/// `POST /v1/billing/checkout/pack` — authed, both audiences (plan §1.10).
/// Creates a `mode: payment` Checkout Session for `quantity` of `pack` and
/// returns its URL. Never credits `credit_balances` — see this module's own
/// doc comment; that happens only once `routes::stripe_webhook` sees a
/// signed `checkout.session.completed`.
pub async fn create_pack_checkout(
    State(state): State<Arc<AppState>>,
    Extension(user): Extension<UserContext>,
    Json(body): Json<PackCheckoutRequest>,
) -> Response {
    if body.success_url.trim().is_empty() || body.cancel_url.trim().is_empty() {
        return bad_request("success_url and cancel_url are required");
    }
    // Stripe's own line-item quantity ceiling (billing self-serve PR D
    // review round, CodeRabbit finding on `checkout.rs:461`). Without an
    // upper bound here, an out-of-range quantity reaches Stripe, which
    // rejects it as its own 400 — `StripeClient` maps that to
    // `StripeError::Api { status: 400, .. }`, which this module's
    // `internal_error` then reports to the CALLER as a 500 (and logs at
    // `error!`), turning a client input mistake into a manufactured server
    // fault. Rejecting it here answers the real 400 instead.
    const MAX_PACK_QUANTITY: i64 = 999_999;
    if !(1..=MAX_PACK_QUANTITY).contains(&body.quantity) {
        return bad_request("quantity must be between 1 and 999999");
    }
    if let Err(resp) = require_purchase_authority(&state, &user, false).await {
        return resp;
    }
    let Some(control) = state.control_db() else {
        return service_unavailable();
    };
    if let Err(resp) =
        validate_return_urls(&state, control, user.account_id, &body.success_url, &body.cancel_url).await
    {
        return resp;
    }
    // Packs land in the extra-usage balance, so refuse what the top-up
    // refuses: Free (403 plan_required), extra usage off (409 extra_usage_off),
    // credits frozen (409 credits_frozen).
    if let Err(resp) =
        crate::routes::extra_usage::refuse_if_balance_unspendable(&state, user.account_id).await
    {
        return resp;
    }

    let price_id =
        match resolve_price_or_fail_loudly(control, PriceCatalog::CreditPacks, &body.pack, "credit pack")
            .await
    {
        Ok(p) => p,
        Err(resp) => return resp,
    };

    let customer_id = match ensure_stripe_customer(&state, user.account_id).await {
        Ok(id) => id,
        Err(resp) => return resp,
    };

    let Some(stripe) = state.stripe.as_ref() else {
        return service_unavailable();
    };
    match stripe
        .create_checkout_session_payment(
            &customer_id,
            &price_id,
            body.quantity,
            &user.account_id.to_string(),
            &body.success_url,
            &body.cancel_url,
        )
        .await
    {
        Ok(session) => (
            StatusCode::OK,
            Json(CheckoutResponse { checkout_url: session.url, session_id: session.id }),
        )
            .into_response(),
        Err(e) => internal_error("stripe create checkout session", e),
    }
}

// ── Live-Postgres tests ──────────────────────────────────────────────────
//
// `RELINFER_TEST_DATABASE_URL`-gated, same convention as every other billing
// pg-test suite in this tree (`billing::stripe_webhook`, `billing::renewal`,
// `billing::issuance`, `billing::enforcement`). Drives the real HTTP
// handlers directly (not through a router + minted session — same shape
// `routes::usage`'s pg_tests use) against a real Postgres AND a real local
// axum mock standing in for Stripe (this repo's established substitute for a
// mock-HTTP crate — see `billing::stripe_client`'s own test module, "Layer
// 3", which this mirrors).
#[cfg(test)]
mod blocking_tests {
    use super::blocks_new_subscription_checkout as blocks;

    #[test]
    fn only_a_live_status_with_a_stripe_subscription_blocks() {
        for status in ["active", "trialing", "past_due"] {
            assert!(blocks(status, Some("sub_1")), "{status} with a subscription blocks");
            assert!(!blocks(status, None), "{status} without a subscription is only a customer mapping");
        }
        assert!(!blocks("pending", None));
        assert!(!blocks("pending", Some("sub_1")));
        assert!(!blocks("canceled", Some("sub_1")), "resubscribing after cancel stays allowed");
    }
}

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

    use super::*;
    use axum::extract::{Form, State};
    use axum::response::IntoResponse;
    use std::collections::HashMap;

    /// `plans`/`seat_tiers`/`credit_packs` are GLOBAL reference-data tables
    /// (no RLS, no per-test scoping possible — see this module's own doc
    /// comment on why a NULL `stripe_price_id` matters). Every test below
    /// that calls `set_plan_price`/[`set_seat_tier_price`]/`set_pack_price`
    /// mutates the SAME shared `pro`/`standard`/`pack_500` rows every other
    /// such test — in this module AND in every other billing pg-test suite —
    /// also mutates, so two of these tests running concurrently (the normal
    /// `cargo test` default) race each other's price value — exactly the
    /// failure mode `CATALOG_PRICE_LOCK` closes; see
    /// `crate::pg_catalog_test_support`'s own doc comment for why that lock
    /// (and [`set_seat_tier_price`]) now live there instead of one per
    /// module (CodeRabbit round 3, PR #660).
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

    /// A local axum server standing in for Stripe: `POST /v1/customers`
    /// echoes a customer id DERIVED from `metadata[account_id]` (so two
    /// tests running concurrently — different accounts, same process — never
    /// collide on `stripe_subscriptions.stripe_customer_id`'s UNIQUE
    /// constraint), and `POST /v1/checkout/sessions` returns a fixed
    /// session id/url. Good enough for this module's happy-path proofs; the
    /// wire-pair CORRECTNESS of what gets posted is already proven in
    /// `billing::stripe_client`'s own Layer 1 tests — this is about what
    /// THIS module's route handlers do with the response.
    async fn spawn_checkout_mock() -> crate::billing::stripe_client::StripeClient {
        async fn customers(Form(form): Form<HashMap<String, String>>) -> axum::response::Response {
            let account_id = form.get("metadata[account_id]").cloned().unwrap_or_default();
            (
                axum::http::StatusCode::OK,
                [("content-type", "application/json")],
                json!({"id": format!("cus_mock_{account_id}")}).to_string(),
            )
                .into_response()
        }
        async fn checkout_sessions() -> axum::response::Response {
            (
                axum::http::StatusCode::OK,
                [("content-type", "application/json")],
                json!({"id": "cs_mock_1", "url": "https://checkout.stripe.com/mock"}).to_string(),
            )
                .into_response()
        }
        let app = axum::Router::new()
            .route("/v1/customers", axum::routing::post(customers))
            .route("/v1/checkout/sessions", axum::routing::post(checkout_sessions));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind mock");
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.ok();
        });
        crate::billing::stripe_client::StripeClient::new(reqwest::Client::new(), "sk_test_mock")
            .with_base_url(format!("http://{addr}"))
    }

    /// A fresh personal account (one user, one `accounts` row,
    /// `is_org_account = false`).
    async fn seed_personal_account(pool: &sqlx::PgPool) -> (Uuid, Uuid) {
        let user_id: Uuid = sqlx::query_scalar("INSERT INTO users (email) VALUES ($1) RETURNING id")
            .bind(format!("{}@checkout-prd.test", Uuid::new_v4()))
            .fetch_one(pool)
            .await
            .expect("insert user");
        let account_id: Uuid = sqlx::query_scalar("INSERT INTO accounts (owner_user_id) VALUES ($1) RETURNING id")
            .bind(user_id)
            .fetch_one(pool)
            .await
            .expect("insert account");
        sqlx::query("INSERT INTO account_members (account_id, user_id, role) VALUES ($1, $2, 'owner')")
            .bind(account_id)
            .bind(user_id)
            .execute(pool)
            .await
            .expect("insert account_members row");
        (account_id, user_id)
    }

    /// An org account with one member at the given `account_members.role`.
    ///
    /// **Billing self-serve PR F:** `0176`'s trigger hardening rejects an
    /// org-member insert with no explicit `seat_tier_id` (the old silent
    /// default-to-`standard` is gone) — so this fixture resolves the active
    /// `standard` tier itself, mirroring what `clerk_provision::
    /// ensure_org_membership` now does for real org joins.
    async fn seed_org_account(pool: &sqlx::PgPool, role: &str) -> (Uuid, Uuid) {
        let user_id: Uuid = sqlx::query_scalar("INSERT INTO users (email) VALUES ($1) RETURNING id")
            .bind(format!("{}@checkout-prd.test", Uuid::new_v4()))
            .fetch_one(pool)
            .await
            .expect("insert user");
        let account_id: Uuid = sqlx::query_scalar(
            "INSERT INTO accounts (owner_user_id, clerk_org_id) VALUES ($1, $2) RETURNING id",
        )
        .bind(user_id)
        .bind(format!("org_{}", Uuid::new_v4().simple()))
        .fetch_one(pool)
        .await
        .expect("insert org account");
        let standard_tier_id: Uuid = sqlx::query_scalar(
            "SELECT id FROM seat_tiers WHERE code = 'standard' AND is_active = true",
        )
        .fetch_one(pool)
        .await
        .expect("active standard seat tier must exist (0165's seed)");
        sqlx::query(
            "INSERT INTO account_members (account_id, user_id, role, seat_tier_id) \
             VALUES ($1, $2, $3, $4)",
        )
        .bind(account_id)
        .bind(user_id)
        .bind(role)
        .bind(standard_tier_id)
        .execute(pool)
        .await
        .expect("insert account_members row");
        (account_id, user_id)
    }

    /// A personal org's own account, in the exact end state
    /// `bin/adopt_personal_org_account.rs` leaves one in: `clerk_org_id` set
    /// on what is otherwise the user's ordinary account, a `personal_orgs`
    /// row naming it as this user's own, and — because the seat-tier trigger
    /// (`0169_account_members_seat_tier.sql:342`) sets `is_org_account` from
    /// `clerk_org_id` alone, with no personal-org exception — an
    /// `account_members` row that MUST carry a hand-assigned seat tier to
    /// satisfy `account_members_seat_tier_iff_org`
    /// (`0169_account_members_seat_tier.sql:493`)/`0176_seat_lifecycle.sql`'s
    /// insert-time trigger, even though nobody ever bought it.
    async fn seed_personal_org_account(pool: &sqlx::PgPool) -> (Uuid, Uuid, String) {
        let clerk_user_id = format!("user_{}", Uuid::new_v4().simple());
        let clerk_org_id = format!("org_{}", Uuid::new_v4().simple());
        let user_id: Uuid =
            sqlx::query_scalar("INSERT INTO users (email, clerk_user_id) VALUES ($1, $2) RETURNING id")
                .bind(format!("{}@checkout-prd.test", Uuid::new_v4()))
                .bind(&clerk_user_id)
                .fetch_one(pool)
                .await
                .expect("insert user");
        let account_id: Uuid = sqlx::query_scalar(
            "INSERT INTO accounts (owner_user_id, clerk_org_id) VALUES ($1, $2) RETURNING id",
        )
        .bind(user_id)
        .bind(&clerk_org_id)
        .fetch_one(pool)
        .await
        .expect("insert personal-org account");
        sqlx::query("INSERT INTO personal_orgs (clerk_org_id, clerk_user_id) VALUES ($1, $2)")
            .bind(&clerk_org_id)
            .bind(&clerk_user_id)
            .execute(pool)
            .await
            .expect("insert personal_orgs row");
        let standard_tier_id: Uuid = sqlx::query_scalar(
            "SELECT id FROM seat_tiers WHERE code = 'standard' AND is_active = true",
        )
        .fetch_one(pool)
        .await
        .expect("active standard seat tier must exist (0165's seed)");
        sqlx::query(
            "INSERT INTO account_members (account_id, user_id, role, seat_tier_id) \
             VALUES ($1, $2, 'owner', $3)",
        )
        .bind(account_id)
        .bind(user_id)
        .bind(standard_tier_id)
        .execute(pool)
        .await
        .expect("insert account_members row");
        (account_id, user_id, clerk_org_id)
    }

    fn user_context(account_id: Uuid, user_id: Uuid) -> UserContext {
        UserContext { user_id, account_id, role: "owner".to_string(), device_id: "checkout-prd-test".to_string() }
    }

    /// Sets `plans.stripe_price_id` for `code` and returns the PREVIOUS
    /// value. `plans`/`seat_tiers`/`credit_packs` are GLOBAL reference-data
    /// rows shared with every other billing pg-test suite against the same
    /// mcp-e2e database (`crate::pg_catalog_test_support`'s own doc comment
    /// on `CATALOG_PRICE_LOCK`) — returning the prior value lets every
    /// caller restore it in cleanup instead of leaving the shared row
    /// permanently pointed at a test price id (billing self-serve PR D
    /// review round, CodeRabbit finding on `checkout.rs:666`).
    async fn set_plan_price(pool: &sqlx::PgPool, code: &str, price_id: Option<&str>) -> Option<String> {
        let previous: Option<String> =
            sqlx::query_scalar("SELECT stripe_price_id FROM plans WHERE code = $1")
                .bind(code)
                .fetch_one(pool)
                .await
                .expect("read plan stripe_price_id");
        sqlx::query("UPDATE plans SET stripe_price_id = $2 WHERE code = $1")
            .bind(code)
            .bind(price_id)
            .execute(pool)
            .await
            .expect("set plan stripe_price_id");
        previous
    }

    /// Same restore-by-return contract as [`set_plan_price`], for
    /// `credit_packs`.
    async fn set_pack_price(pool: &sqlx::PgPool, code: &str, price_id: Option<&str>) -> Option<String> {
        let previous: Option<String> =
            sqlx::query_scalar("SELECT stripe_price_id FROM credit_packs WHERE code = $1")
                .bind(code)
                .fetch_one(pool)
                .await
                .expect("read credit_packs stripe_price_id");
        sqlx::query("UPDATE credit_packs SET stripe_price_id = $2 WHERE code = $1")
            .bind(code)
            .bind(price_id)
            .execute(pool)
            .await
            .expect("set credit_packs stripe_price_id");
        previous
    }

    /// Deletes the seeded `accounts` row (and, transitively via
    /// `ON DELETE CASCADE`, its `account_members`/`stripe_subscriptions`
    /// rows) and every `users` row this test created — `seed_personal_account`
    /// / `seed_org_account` currently have no matching teardown (billing
    /// self-serve PR D review round, CodeRabbit finding on `checkout.rs:666`),
    /// so every run of this suite otherwise leaves permanent rows behind in
    /// the shared mcp-e2e database. Propagates errors (CodeRabbit round 3) —
    /// each call site captures its assertion inputs BEFORE running cleanup and
    /// `.expect()`s the result, so a cleanup failure surfaces loudly instead of
    /// silently leaking fixture rows, and a failing assertion can never skip
    /// cleanup by panicking first.
    async fn cleanup_account(
        pool: &sqlx::PgPool,
        account_id: Uuid,
        users: &[Uuid],
    ) -> Result<(), sqlx::Error> {
        execute_financial_fixture(
            pool,
            account_id,
            sqlx::query("DELETE FROM accounts WHERE id = $1")
            .bind(account_id),
        )
            .await?;
        for user_id in users {
            sqlx::query("DELETE FROM users WHERE id = $1")
                .bind(user_id)
                .execute(pool)
                .await?;
        }
        Ok(())
    }

    /// `personal_orgs` carries no FK to `accounts` (both columns are opaque
    /// Clerk identifiers — see `0208_personal_orgs.sql`'s own doc comment),
    /// so `cleanup_account`'s `DELETE FROM accounts` above never cascades
    /// into it. Every `seed_personal_org_account` test must call this too.
    async fn cleanup_personal_org(pool: &sqlx::PgPool, clerk_org_id: &str) {
        sqlx::query("DELETE FROM personal_orgs WHERE clerk_org_id = $1")
            .bind(clerk_org_id)
            .execute(pool)
            .await
            .expect("cleanup personal_orgs row");
    }

    /// Seeds/updates the account's `stripe_subscriptions` row status — same
    /// helper shape as `billing::enforcement::pg_tests::set_subscription_status`.
    async fn set_subscription_status(pool: &sqlx::PgPool, account_id: Uuid, status: &str) {
        execute_financial_fixture(
            pool,
            account_id,
        sqlx::query(
            "INSERT INTO stripe_subscriptions (account_id, stripe_customer_id, stripe_subscription_id, status) \
             VALUES ($1, $2, $3, $4) \
             ON CONFLICT (account_id) DO UPDATE SET status = EXCLUDED.status, \
                 stripe_subscription_id = EXCLUDED.stripe_subscription_id",
        )
        .bind(account_id)
        .bind(format!("cus_checkout_prd_{}", Uuid::new_v4().simple()))
        .bind(format!("sub_checkout_prd_{}", Uuid::new_v4().simple()))
        .bind(status),
        )
        .await
        .expect("seed stripe_subscriptions status");
    }

    async fn read_stripe_customer_id(pool: &sqlx::PgPool, account_id: Uuid) -> Option<String> {
        sqlx::query_scalar("SELECT stripe_customer_id FROM stripe_subscriptions WHERE account_id = $1")
            .bind(account_id)
            .fetch_optional(pool)
            .await
            .expect("read stripe_subscriptions")
    }

    async fn body_json(resp: axum::response::Response) -> serde_json::Value {
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX).await.expect("read body");
        serde_json::from_slice(&bytes).expect("valid JSON body")
    }

    /// A recording variant of [`spawn_checkout_mock`]: every Checkout Session
    /// gets a fresh id (`cs_mock_<n>`), and every request is kept as
    /// `(path, form body)` so a test can assert exactly what was sent.
    async fn spawn_recording_checkout_mock(
    ) -> (crate::billing::stripe_client::StripeClient, Arc<std::sync::Mutex<Vec<(String, String)>>>) {
        let seen: Arc<std::sync::Mutex<Vec<(String, String)>>> = Arc::new(std::sync::Mutex::new(Vec::new()));
        let seen2 = seen.clone();
        let counter = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let app = axum::Router::new().fallback(axum::routing::any(
            move |req: axum::http::Request<axum::body::Body>| {
                let seen = seen2.clone();
                let counter = counter.clone();
                async move {
                    let (parts, body) = req.into_parts();
                    let bytes = axum::body::to_bytes(body, 1 << 20).await.unwrap();
                    let body = String::from_utf8_lossy(&bytes).into_owned();
                    let path = parts.uri.path().to_string();
                    seen.lock().unwrap().push((path.clone(), body.clone()));
                    let reply = if path == "/v1/customers" {
                        let account_id = body
                            .split('&')
                            .find_map(|kv| kv.strip_prefix("metadata%5Baccount_id%5D="))
                            .unwrap_or_default()
                            .to_string();
                        json!({"id": format!("cus_mock_{account_id}")})
                    } else if path == "/v1/checkout/sessions" {
                        let n = counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
                        json!({"id": format!("cs_mock_{n}"), "url": format!("https://checkout.stripe.com/mock/{n}")})
                    } else {
                        json!({"id": "cs_expired", "status": "expired"})
                    };
                    (
                        axum::http::StatusCode::OK,
                        [("content-type", "application/json")],
                        reply.to_string(),
                    )
                        .into_response()
                }
            },
        ));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind mock");
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.ok();
        });
        (
            crate::billing::stripe_client::StripeClient::new(reqwest::Client::new(), "sk_test_mock")
                .with_base_url(format!("http://{addr}")),
            seen,
        )
    }

    fn pro_request() -> Json<SubscriptionCheckoutRequest> {
        Json(SubscriptionCheckoutRequest {
            tier: "pro".to_string(),
            success_url: "http://localhost:3000/success".to_string(),
            cancel_url: "http://localhost:3000/cancel".to_string(),
        })
    }

    /// Staging run: an abandoned Checkout left the mapping row at the column
    /// default (`active`), so every retry was a 409. Now the row is `pending`
    /// with no subscription, the retry is accepted, and the new session
    /// replaces the abandoned one (which is expired at Stripe).
    #[tokio::test]
    async fn an_abandoned_checkout_can_be_retried_and_replaces_the_stale_session() {
        let Some(pool) =
            pg_test_pool("an_abandoned_checkout_can_be_retried_and_replaces_the_stale_session").await
        else {
            return;
        };
        let _catalog_guard = CATALOG_PRICE_LOCK.lock().await;
        let (account_id, user_id) = seed_personal_account(&pool).await;
        let original_pro_price = set_plan_price(&pool, "pro", Some("price_pro_test")).await;
        let (stripe, seen) = spawn_recording_checkout_mock().await;
        let mut state = crate::test_app_state_owned_with_db_and_store(pool.clone(), None);
        state.stripe = Some(Arc::new(stripe));
        let state = Arc::new(state);

        let first = create_subscription_checkout(
            State(state.clone()),
            Extension(user_context(account_id, user_id)),
            pro_request(),
        )
        .await;
        let first_status = first.status();
        let first_body = body_json(first).await;
        let row_after_first: (String, Option<String>, Option<String>) = sqlx::query_as(
            "SELECT status, stripe_subscription_id, pending_checkout_session_id \
               FROM stripe_subscriptions WHERE account_id = $1",
        )
        .bind(account_id)
        .fetch_one(&pool)
        .await
        .unwrap();

        // The browser closed on the Stripe page; the user comes back and retries.
        let second = create_subscription_checkout(
            State(state.clone()),
            Extension(user_context(account_id, user_id)),
            pro_request(),
        )
        .await;
        let second_status = second.status();
        let second_body = body_json(second).await;
        let pending_after_second: Option<String> = sqlx::query_scalar(
            "SELECT pending_checkout_session_id FROM stripe_subscriptions WHERE account_id = $1",
        )
        .bind(account_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        let requests = seen.lock().unwrap().clone();

        set_plan_price(&pool, "pro", original_pro_price.as_deref()).await;
        cleanup_account(&pool, account_id, &[user_id]).await.expect("cleanup test fixture");

        assert_eq!(first_status, StatusCode::OK);
        assert_eq!(
            row_after_first,
            ("pending".to_string(), None, Some("cs_mock_1".to_string())),
            "a customer mapping with an open Checkout is pending, not an active subscription"
        );
        assert_eq!(second_status, StatusCode::OK, "a retry after an abandoned Checkout must not 409");
        assert_eq!(second_body["session_id"], "cs_mock_2");
        assert_ne!(first_body["session_id"], second_body["session_id"]);
        assert_eq!(pending_after_second.as_deref(), Some("cs_mock_2"), "the new session replaces the stale one");
        let expires: Vec<&(String, String)> =
            requests.iter().filter(|(path, _)| path.ends_with("/expire")).collect();
        assert_eq!(expires.len(), 1, "exactly the stale session is expired: {requests:?}");
        assert_eq!(expires[0].0, "/v1/checkout/sessions/cs_mock_1/expire");
        assert_eq!(expires[0].1, "", "expire takes no parameters");
    }

    /// Rows written before `pending` existed say `active` with no Stripe
    /// subscription. They are not subscriptions, so they do not block.
    #[tokio::test]
    async fn a_legacy_active_customer_row_without_a_subscription_does_not_block_checkout() {
        let Some(pool) =
            pg_test_pool("a_legacy_active_customer_row_without_a_subscription_does_not_block_checkout").await
        else {
            return;
        };
        let _catalog_guard = CATALOG_PRICE_LOCK.lock().await;
        let (account_id, user_id) = seed_personal_account(&pool).await;
        let original_pro_price = set_plan_price(&pool, "pro", Some("price_pro_test")).await;
        execute_financial_fixture(
            &pool,
            account_id,
            sqlx::query(
                "INSERT INTO stripe_subscriptions (account_id, stripe_customer_id, status) VALUES ($1, $2, 'active')",
            )
            .bind(account_id)
            .bind(format!("cus_legacy_{}", Uuid::new_v4().simple())),
        )
        .await
        .expect("seed legacy row");
        let stripe = spawn_checkout_mock().await;
        let mut state = crate::test_app_state_owned_with_db_and_store(pool.clone(), None);
        state.stripe = Some(Arc::new(stripe));

        let resp = create_subscription_checkout(
            State(Arc::new(state)),
            Extension(user_context(account_id, user_id)),
            pro_request(),
        )
        .await;
        let status = resp.status();

        set_plan_price(&pool, "pro", original_pro_price.as_deref()).await;
        cleanup_account(&pool, account_id, &[user_id]).await.expect("cleanup test fixture");
        assert_eq!(status, StatusCode::OK);
    }

    /// **Happy path + customer-pre-create ordering, subscription checkout,
    /// personal account, flat `pro` plan.** Proves: 200 with a checkout
    /// URL/session id, AND — the load-bearing half — the `stripe_subscriptions`
    /// mapping row exists, carrying the mock customer id, by the time this
    /// call returns (i.e. strictly BEFORE any webhook could possibly arrive
    /// for this session). This is plan §1.8's ordering guarantee, proven
    /// directly rather than assumed from reading the code.
    #[tokio::test]
    async fn pro_subscription_checkout_happy_path_pre_creates_the_customer_mapping() {
        let Some(pool) =
            pg_test_pool("pro_subscription_checkout_happy_path_pre_creates_the_customer_mapping").await
        else {
            return;
        };
        let _catalog_guard = CATALOG_PRICE_LOCK.lock().await;
        let (account_id, user_id) = seed_personal_account(&pool).await;
        let original_pro_price = set_plan_price(&pool, "pro", Some("price_pro_test")).await;
        let stripe = spawn_checkout_mock().await;
        let mut state = crate::test_app_state_owned_with_db_and_store(pool.clone(), None);
        state.stripe = Some(Arc::new(stripe));
        let state = Arc::new(state);

        let resp = create_subscription_checkout(
            State(state),
            Extension(user_context(account_id, user_id)),
            Json(SubscriptionCheckoutRequest {
                tier: "pro".to_string(),
                success_url: "http://localhost:3000/success".to_string(),
                cancel_url: "http://localhost:3000/cancel".to_string(),
            }),
        )
        .await;
        let status = resp.status();
        let body = body_json(resp).await;
        let stored = read_stripe_customer_id(&pool, account_id).await;

        set_plan_price(&pool, "pro", original_pro_price.as_deref()).await;
        cleanup_account(&pool, account_id, &[user_id])
            .await
            .expect("cleanup test fixture");

        assert_eq!(status, StatusCode::OK, "expected 200");
        assert_eq!(body["session_id"], "cs_mock_1");
        assert_eq!(body["checkout_url"], "https://checkout.stripe.com/mock");
        assert_eq!(
            stored.as_deref(),
            Some(format!("cus_mock_{account_id}").as_str()),
            "the mapping row must exist, carrying the customer this checkout used, before this \
             call even returns"
        );
    }

    /// **Happy path, team seat-tier subscription checkout, org account, an
    /// org-admin caller.**
    #[tokio::test]
    async fn team_seat_tier_checkout_happy_path_for_an_org_admin() {
        let Some(pool) = pg_test_pool("team_seat_tier_checkout_happy_path_for_an_org_admin").await else {
            return;
        };
        let _catalog_guard = CATALOG_PRICE_LOCK.lock().await;
        let (account_id, user_id) = seed_org_account(&pool, "owner").await;
        let original_standard_price = set_seat_tier_price(&pool, "standard", Some("price_standard_test")).await;
        let stripe = spawn_checkout_mock().await;
        let mut state = crate::test_app_state_owned_with_db_and_store(pool.clone(), None);
        state.stripe = Some(Arc::new(stripe));
        let state = Arc::new(state);

        let resp = create_subscription_checkout(
            State(state),
            Extension(user_context(account_id, user_id)),
            Json(SubscriptionCheckoutRequest {
                tier: "standard".to_string(),
                success_url: "http://localhost:3000/success".to_string(),
                cancel_url: "http://localhost:3000/cancel".to_string(),
            }),
        )
        .await;
        let status = resp.status();

        set_seat_tier_price(&pool, "standard", original_standard_price.as_deref()).await;
        cleanup_account(&pool, account_id, &[user_id])
            .await
            .expect("cleanup test fixture");
        assert_eq!(status, StatusCode::OK, "an org owner must be able to check out a seat tier");
    }

    /// **Org-admin gate.** A plain `member` of an org account must be
    /// refused (403), never charged.
    #[tokio::test]
    async fn a_plain_org_member_cannot_purchase_billing() {
        let Some(pool) = pg_test_pool("a_plain_org_member_cannot_purchase_billing").await else {
            return;
        };
        let _catalog_guard = CATALOG_PRICE_LOCK.lock().await;
        let (account_id, user_id) = seed_org_account(&pool, "member").await;
        let original_standard_price = set_seat_tier_price(&pool, "standard", Some("price_standard_test")).await;
        let state = Arc::new(crate::test_app_state_owned_with_db_and_store(pool.clone(), None));

        let resp = create_subscription_checkout(
            State(state),
            Extension(user_context(account_id, user_id)),
            Json(SubscriptionCheckoutRequest {
                tier: "standard".to_string(),
                success_url: "http://localhost:3000/success".to_string(),
                cancel_url: "http://localhost:3000/cancel".to_string(),
            }),
        )
        .await;
        let status = resp.status();
        let stored = read_stripe_customer_id(&pool, account_id).await;

        set_seat_tier_price(&pool, "standard", original_standard_price.as_deref()).await;
        cleanup_account(&pool, account_id, &[user_id])
            .await
            .expect("cleanup test fixture");

        assert_eq!(status, StatusCode::FORBIDDEN, "a plain member must never be able to purchase billing");
        assert_eq!(
            stored,
            None,
            "a refused request must never create a Stripe customer or a mapping row"
        );
    }

    /// **NULL `stripe_price_id` refusal (plan §2.D item 1) — the 'pro' plan
    /// row exists (it is active) but has not been provisioned in Stripe
    /// yet.** Must fail loudly (500), never silently create a session
    /// against a garbage/empty price, and never even reach the Stripe mock
    /// (proven by there being no customer mapping row afterward — the price
    /// check runs BEFORE customer creation).
    #[tokio::test]
    async fn null_price_id_is_refused_loudly_not_silently() {
        let Some(pool) = pg_test_pool("null_price_id_is_refused_loudly_not_silently").await else {
            return;
        };
        let _catalog_guard = CATALOG_PRICE_LOCK.lock().await;
        let (account_id, user_id) = seed_personal_account(&pool).await;
        let original_pro_price = set_plan_price(&pool, "pro", None).await;
        let state = Arc::new(crate::test_app_state_owned_with_db_and_store(pool.clone(), None));

        let resp = create_subscription_checkout(
            State(state),
            Extension(user_context(account_id, user_id)),
            Json(SubscriptionCheckoutRequest {
                tier: "pro".to_string(),
                success_url: "http://localhost:3000/success".to_string(),
                cancel_url: "http://localhost:3000/cancel".to_string(),
            }),
        )
        .await;
        let status = resp.status();
        let stored = read_stripe_customer_id(&pool, account_id).await;

        set_plan_price(&pool, "pro", original_pro_price.as_deref()).await;
        cleanup_account(&pool, account_id, &[user_id])
            .await
            .expect("cleanup test fixture");

        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR, "a NULL price must fail loudly, not silently");
        assert_eq!(
            stored,
            None,
            "provisioning has not run — nothing should have been created in Stripe at all"
        );
    }

    /// Unknown tier code -> 400, distinct from the NULL-price 500 above (a
    /// real request error, not an internal provisioning gap).
    #[tokio::test]
    async fn unknown_tier_is_a_bad_request() {
        let Some(pool) = pg_test_pool("unknown_tier_is_a_bad_request").await else {
            return;
        };
        let (account_id, user_id) = seed_personal_account(&pool).await;
        let state = Arc::new(crate::test_app_state_owned_with_db_and_store(pool.clone(), None));

        let resp = create_subscription_checkout(
            State(state),
            Extension(user_context(account_id, user_id)),
            Json(SubscriptionCheckoutRequest {
                tier: "not_a_real_tier".to_string(),
                success_url: "http://localhost:3000/success".to_string(),
                cancel_url: "http://localhost:3000/cancel".to_string(),
            }),
        )
        .await;
        let status = resp.status();

        cleanup_account(&pool, account_id, &[user_id])
            .await
            .expect("cleanup test fixture");
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }

    // ── `validate_return_urls` (billing self-serve PR D review round,
    // CodeRabbit finding on `checkout.rs:391`) ──────────────────────────────

    /// A `success_url` on a foreign origin is refused (400), never reaching
    /// Stripe — the open-redirect-after-payment abuse this guard exists to
    /// close. Off-origin is enough on its own; no price/customer setup
    /// needed, since the guard must run before any of that.
    #[tokio::test]
    async fn subscription_checkout_refuses_a_foreign_success_url() {
        let Some(pool) = pg_test_pool("subscription_checkout_refuses_a_foreign_success_url").await
        else {
            return;
        };
        let (account_id, user_id) = seed_personal_account(&pool).await;
        let state = Arc::new(crate::test_app_state_owned_with_db_and_store(pool.clone(), None));

        let resp = create_subscription_checkout(
            State(state),
            Extension(user_context(account_id, user_id)),
            Json(SubscriptionCheckoutRequest {
                tier: "pro".to_string(),
                success_url: "https://attacker.example/steal".to_string(),
                cancel_url: "http://localhost:3000/cancel".to_string(),
            }),
        )
        .await;
        let status = resp.status();

        cleanup_account(&pool, account_id, &[user_id])
            .await
            .expect("cleanup test fixture");
        assert_eq!(
            status,
            StatusCode::BAD_REQUEST,
            "a success_url off this deployment's own origin must be refused before Stripe is ever \
             called"
        );
    }

    /// Same guard, same refusal, for `cancel_url` — proves the check covers
    /// BOTH fields, not just `success_url`.
    #[tokio::test]
    async fn subscription_checkout_refuses_a_foreign_cancel_url() {
        let Some(pool) = pg_test_pool("subscription_checkout_refuses_a_foreign_cancel_url").await
        else {
            return;
        };
        let (account_id, user_id) = seed_personal_account(&pool).await;
        let state = Arc::new(crate::test_app_state_owned_with_db_and_store(pool.clone(), None));

        let resp = create_subscription_checkout(
            State(state),
            Extension(user_context(account_id, user_id)),
            Json(SubscriptionCheckoutRequest {
                tier: "pro".to_string(),
                success_url: "http://localhost:3000/success".to_string(),
                cancel_url: "https://attacker.example/steal".to_string(),
            }),
        )
        .await;
        let status = resp.status();

        cleanup_account(&pool, account_id, &[user_id])
            .await
            .expect("cleanup test fixture");
        assert_eq!(status, StatusCode::BAD_REQUEST, "a foreign cancel_url must also be refused");
    }

    /// Same guard on the pack route — proves the check is not
    /// subscription-only.
    #[tokio::test]
    async fn pack_checkout_refuses_a_foreign_success_url() {
        let Some(pool) = pg_test_pool("pack_checkout_refuses_a_foreign_success_url").await else {
            return;
        };
        let (account_id, user_id) = seed_personal_account(&pool).await;
        let state = Arc::new(crate::test_app_state_owned_with_db_and_store(pool.clone(), None));

        let resp = create_pack_checkout(
            State(state),
            Extension(user_context(account_id, user_id)),
            Json(PackCheckoutRequest {
                pack: "pack_500".to_string(),
                quantity: 1,
                success_url: "https://attacker.example/steal".to_string(),
                cancel_url: "http://localhost:3000/cancel".to_string(),
            }),
        )
        .await;
        let status = resp.status();

        cleanup_account(&pool, account_id, &[user_id])
            .await
            .expect("cleanup test fixture");
        assert_eq!(status, StatusCode::BAD_REQUEST, "a foreign success_url must be refused for a pack checkout too");
    }

    /// The `MAX_PACK_QUANTITY` bound rejects out-of-range values as a 400
    /// rather than letting Stripe's own 400 surface as our 500 (see this
    /// module's own comment above the check). Runs before
    /// `require_purchase_authority` and any DB/Stripe access, so a minimal
    /// seeded state is enough to prove it.
    #[tokio::test]
    async fn pack_checkout_rejects_an_out_of_range_quantity() {
        let Some(pool) = pg_test_pool("pack_checkout_rejects_an_out_of_range_quantity").await else {
            return;
        };
        let (account_id, user_id) = seed_personal_account(&pool).await;
        let state = Arc::new(crate::test_app_state_owned_with_db_and_store(pool.clone(), None));

        let mut statuses = Vec::new();
        for quantity in [0_i64, -1, 1_000_000] {
            let resp = create_pack_checkout(
                State(state.clone()),
                Extension(user_context(account_id, user_id)),
                Json(PackCheckoutRequest {
                    pack: "pack_500".to_string(),
                    quantity,
                    success_url: "http://localhost:3000/success".to_string(),
                    cancel_url: "http://localhost:3000/cancel".to_string(),
                }),
            )
            .await;
            statuses.push((quantity, resp.status()));
        }

        cleanup_account(&pool, account_id, &[user_id])
            .await
            .expect("cleanup test fixture");

        for (quantity, status) in statuses {
            assert_eq!(
                status,
                StatusCode::BAD_REQUEST,
                "quantity {quantity} must be refused as a client error, not reach Stripe"
            );
        }
    }

    /// A same-origin URL that merely carries the trusted host as a STRING
    /// PREFIX (`http://localhost:3000.attacker.example`) must still be
    /// refused — proves the match is anchored on the full origin, not a
    /// naive substring/prefix check.
    #[tokio::test]
    async fn subscription_checkout_refuses_a_prefix_lookalike_origin() {
        let Some(pool) = pg_test_pool("subscription_checkout_refuses_a_prefix_lookalike_origin").await
        else {
            return;
        };
        let (account_id, user_id) = seed_personal_account(&pool).await;
        let state = Arc::new(crate::test_app_state_owned_with_db_and_store(pool.clone(), None));

        let resp = create_subscription_checkout(
            State(state),
            Extension(user_context(account_id, user_id)),
            Json(SubscriptionCheckoutRequest {
                tier: "pro".to_string(),
                success_url: "http://localhost:3000.attacker.example/steal".to_string(),
                cancel_url: "http://localhost:3000/cancel".to_string(),
            }),
        )
        .await;
        let status = resp.status();

        cleanup_account(&pool, account_id, &[user_id])
            .await
            .expect("cleanup test fixture");
        assert_eq!(
            status,
            StatusCode::BAD_REQUEST,
            "a lookalike origin that merely starts with the trusted host must still be refused"
        );
    }

    /// **Happy path, pack checkout, personal account.**
    #[tokio::test]
    async fn pack_checkout_happy_path() {
        let Some(pool) = pg_test_pool("pack_checkout_happy_path").await else {
            return;
        };
        let _catalog_guard = CATALOG_PRICE_LOCK.lock().await;
        let (account_id, user_id) = seed_personal_account(&pool).await;
        crate::routes::extra_usage::pg_tests::make_pack_buyer(&pool, account_id).await;
        let original_pack_500_price = set_pack_price(&pool, "pack_500", Some("price_pack_500_test")).await;
        let stripe = spawn_checkout_mock().await;
        let mut state = crate::test_app_state_owned_with_db_and_store(pool.clone(), None);
        state.stripe = Some(Arc::new(stripe));
        let state = Arc::new(state);

        let resp = create_pack_checkout(
            State(state),
            Extension(user_context(account_id, user_id)),
            Json(PackCheckoutRequest {
                pack: "pack_500".to_string(),
                quantity: 1,
                success_url: "http://localhost:3000/success".to_string(),
                cancel_url: "http://localhost:3000/cancel".to_string(),
            }),
        )
        .await;
        let status = resp.status();
        let body = body_json(resp).await;

        set_pack_price(&pool, "pack_500", original_pack_500_price.as_deref()).await;
        cleanup_account(&pool, account_id, &[user_id])
            .await
            .expect("cleanup test fixture");

        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["session_id"], "cs_mock_1");
    }

    /// NULL pack price -> 500, same fail-loud posture as the plan/seat-tier
    /// case.
    #[tokio::test]
    async fn null_pack_price_id_is_refused_loudly() {
        let Some(pool) = pg_test_pool("null_pack_price_id_is_refused_loudly").await else {
            return;
        };
        let _catalog_guard = CATALOG_PRICE_LOCK.lock().await;
        let (account_id, user_id) = seed_personal_account(&pool).await;
        crate::routes::extra_usage::pg_tests::make_pack_buyer(&pool, account_id).await;
        let original_pack_500_price = set_pack_price(&pool, "pack_500", None).await;
        let state = Arc::new(crate::test_app_state_owned_with_db_and_store(pool.clone(), None));

        let resp = create_pack_checkout(
            State(state),
            Extension(user_context(account_id, user_id)),
            Json(PackCheckoutRequest {
                pack: "pack_500".to_string(),
                quantity: 1,
                success_url: "http://localhost:3000/success".to_string(),
                cancel_url: "http://localhost:3000/cancel".to_string(),
            }),
        )
        .await;
        let status = resp.status();

        set_pack_price(&pool, "pack_500", original_pack_500_price.as_deref()).await;
        cleanup_account(&pool, account_id, &[user_id])
            .await
            .expect("cleanup test fixture");
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    }

    /// A SECOND checkout call for an account that already has a
    /// `stripe_subscriptions` mapping row (e.g. a pack top-up after an
    /// earlier subscription checkout) must REUSE the existing customer id,
    /// never create a second Stripe customer or overwrite the mapping.
    #[tokio::test]
    async fn a_second_checkout_reuses_the_existing_customer_mapping() {
        let Some(pool) = pg_test_pool("a_second_checkout_reuses_the_existing_customer_mapping").await else {
            return;
        };
        let _catalog_guard = CATALOG_PRICE_LOCK.lock().await;
        let (account_id, user_id) = seed_personal_account(&pool).await;
        crate::routes::extra_usage::pg_tests::make_pack_buyer(&pool, account_id).await;
        let original_pro_price = set_plan_price(&pool, "pro", Some("price_pro_test")).await;
        let original_pack_500_price = set_pack_price(&pool, "pack_500", Some("price_pack_500_test")).await;
        let stripe = spawn_checkout_mock().await;
        let mut state = crate::test_app_state_owned_with_db_and_store(pool.clone(), None);
        state.stripe = Some(Arc::new(stripe));
        let state = Arc::new(state);

        let first = create_subscription_checkout(
            State(state.clone()),
            Extension(user_context(account_id, user_id)),
            Json(SubscriptionCheckoutRequest {
                tier: "pro".to_string(),
                success_url: "http://localhost:3000/success".to_string(),
                cancel_url: "http://localhost:3000/cancel".to_string(),
            }),
        )
        .await;
        let first_status = first.status();
        let customer_after_first = read_stripe_customer_id(&pool, account_id).await;

        let second = create_pack_checkout(
            State(state),
            Extension(user_context(account_id, user_id)),
            Json(PackCheckoutRequest {
                pack: "pack_500".to_string(),
                quantity: 2,
                success_url: "http://localhost:3000/success".to_string(),
                cancel_url: "http://localhost:3000/cancel".to_string(),
            }),
        )
        .await;
        let second_status = second.status();
        let customer_after_second = read_stripe_customer_id(&pool, account_id).await;

        set_plan_price(&pool, "pro", original_pro_price.as_deref()).await;
        set_pack_price(&pool, "pack_500", original_pack_500_price.as_deref()).await;
        cleanup_account(&pool, account_id, &[user_id])
            .await
            .expect("cleanup test fixture");

        assert_eq!(first_status, StatusCode::OK);
        assert_eq!(second_status, StatusCode::OK);
        assert_eq!(
            customer_after_first, customer_after_second,
            "the second checkout must reuse the SAME stored customer id, not overwrite it"
        );
    }

    // ── Finding 5: refuse a second live subscription ────────────────────────

    /// An `active` subscription refuses a second subscription checkout — 409,
    /// and never even reaches the Stripe mock (no second customer creation
    /// call, proven indirectly by the mock never being exercised for this
    /// call — the guard runs before `ensure_stripe_customer`).
    #[tokio::test]
    async fn a_second_subscription_checkout_is_refused_while_active() {
        let Some(pool) = pg_test_pool("a_second_subscription_checkout_is_refused_while_active").await
        else {
            return;
        };
        let _catalog_guard = CATALOG_PRICE_LOCK.lock().await;
        let (account_id, user_id) = seed_personal_account(&pool).await;
        let original_pro_price = set_plan_price(&pool, "pro", Some("price_pro_test")).await;
        set_subscription_status(&pool, account_id, "active").await;
        let state = Arc::new(crate::test_app_state_owned_with_db_and_store(pool.clone(), None));

        let resp = create_subscription_checkout(
            State(state),
            Extension(user_context(account_id, user_id)),
            Json(SubscriptionCheckoutRequest {
                tier: "pro".to_string(),
                success_url: "http://localhost:3000/success".to_string(),
                cancel_url: "http://localhost:3000/cancel".to_string(),
            }),
        )
        .await;
        let status = resp.status();

        set_plan_price(&pool, "pro", original_pro_price.as_deref()).await;
        cleanup_account(&pool, account_id, &[user_id])
            .await
            .expect("cleanup test fixture");
        assert_eq!(status, StatusCode::CONFLICT, "an already-active subscription must refuse a second checkout");
    }

    /// Same refusal for `past_due` — an involuntary lapse, subscription still
    /// technically alive at Stripe, so a second checkout would still create a
    /// genuinely second subscription.
    #[tokio::test]
    async fn a_second_subscription_checkout_is_refused_while_past_due() {
        let Some(pool) =
            pg_test_pool("a_second_subscription_checkout_is_refused_while_past_due").await
        else {
            return;
        };
        let _catalog_guard = CATALOG_PRICE_LOCK.lock().await;
        let (account_id, user_id) = seed_personal_account(&pool).await;
        let original_pro_price = set_plan_price(&pool, "pro", Some("price_pro_test")).await;
        set_subscription_status(&pool, account_id, "past_due").await;
        let state = Arc::new(crate::test_app_state_owned_with_db_and_store(pool.clone(), None));

        let resp = create_subscription_checkout(
            State(state),
            Extension(user_context(account_id, user_id)),
            Json(SubscriptionCheckoutRequest {
                tier: "pro".to_string(),
                success_url: "http://localhost:3000/success".to_string(),
                cancel_url: "http://localhost:3000/cancel".to_string(),
            }),
        )
        .await;
        let status = resp.status();

        set_plan_price(&pool, "pro", original_pro_price.as_deref()).await;
        cleanup_account(&pool, account_id, &[user_id])
            .await
            .expect("cleanup test fixture");
        assert_eq!(status, StatusCode::CONFLICT, "a past_due subscription must also refuse a second checkout");
    }

    /// **Positive control**: a `canceled` subscription is the intended
    /// resubscribe path and must NOT be refused — without this test
    /// alongside the two above, a bug that refused EVERY existing
    /// `stripe_subscriptions` row (not just live ones) would also pass.
    #[tokio::test]
    async fn a_canceled_subscription_may_resubscribe() {
        let Some(pool) = pg_test_pool("a_canceled_subscription_may_resubscribe").await else {
            return;
        };
        let _catalog_guard = CATALOG_PRICE_LOCK.lock().await;
        let (account_id, user_id) = seed_personal_account(&pool).await;
        let original_pro_price = set_plan_price(&pool, "pro", Some("price_pro_test")).await;
        set_subscription_status(&pool, account_id, "canceled").await;
        let stripe = spawn_checkout_mock().await;
        let mut state = crate::test_app_state_owned_with_db_and_store(pool.clone(), None);
        state.stripe = Some(Arc::new(stripe));
        let state = Arc::new(state);

        let resp = create_subscription_checkout(
            State(state),
            Extension(user_context(account_id, user_id)),
            Json(SubscriptionCheckoutRequest {
                tier: "pro".to_string(),
                success_url: "http://localhost:3000/success".to_string(),
                cancel_url: "http://localhost:3000/cancel".to_string(),
            }),
        )
        .await;
        let status = resp.status();

        set_plan_price(&pool, "pro", original_pro_price.as_deref()).await;
        cleanup_account(&pool, account_id, &[user_id])
            .await
            .expect("cleanup test fixture");
        assert_eq!(status, StatusCode::OK, "a canceled subscription must be allowed to resubscribe");
    }

    // ── Finding 7: refuse a pack purchase for a frozen account ──────────────

    /// A `canceled` account is refused a NEW pack purchase — the credits it
    /// would buy are immediately unspendable while frozen.
    #[tokio::test]
    async fn pack_checkout_is_refused_for_a_canceled_subscription() {
        let Some(pool) = pg_test_pool("pack_checkout_is_refused_for_a_canceled_subscription").await
        else {
            return;
        };
        let _catalog_guard = CATALOG_PRICE_LOCK.lock().await;
        let (account_id, user_id) = seed_personal_account(&pool).await;
        crate::routes::extra_usage::pg_tests::make_pack_buyer(&pool, account_id).await;
        let original_pack_500_price = set_pack_price(&pool, "pack_500", Some("price_pack_500_test")).await;
        set_subscription_status(&pool, account_id, "canceled").await;
        let state = Arc::new(crate::test_app_state_owned_with_db_and_store(pool.clone(), None));

        let resp = create_pack_checkout(
            State(state),
            Extension(user_context(account_id, user_id)),
            Json(PackCheckoutRequest {
                pack: "pack_500".to_string(),
                quantity: 1,
                success_url: "http://localhost:3000/success".to_string(),
                cancel_url: "http://localhost:3000/cancel".to_string(),
            }),
        )
        .await;
        let status = resp.status();

        set_pack_price(&pool, "pack_500", original_pack_500_price.as_deref()).await;
        cleanup_account(&pool, account_id, &[user_id])
            .await
            .expect("cleanup test fixture");
        assert_eq!(status, StatusCode::CONFLICT, "a canceled (frozen) account must be refused a new pack purchase");
    }

    /// Same refusal for `past_due` — freezes the pool identically to
    /// `canceled` (`billing::enforcement::subscription_frozen`).
    #[tokio::test]
    async fn pack_checkout_is_refused_for_a_past_due_subscription() {
        let Some(pool) = pg_test_pool("pack_checkout_is_refused_for_a_past_due_subscription").await
        else {
            return;
        };
        let _catalog_guard = CATALOG_PRICE_LOCK.lock().await;
        let (account_id, user_id) = seed_personal_account(&pool).await;
        crate::routes::extra_usage::pg_tests::make_pack_buyer(&pool, account_id).await;
        let original_pack_500_price = set_pack_price(&pool, "pack_500", Some("price_pack_500_test")).await;
        set_subscription_status(&pool, account_id, "past_due").await;
        let state = Arc::new(crate::test_app_state_owned_with_db_and_store(pool.clone(), None));

        let resp = create_pack_checkout(
            State(state),
            Extension(user_context(account_id, user_id)),
            Json(PackCheckoutRequest {
                pack: "pack_500".to_string(),
                quantity: 1,
                success_url: "http://localhost:3000/success".to_string(),
                cancel_url: "http://localhost:3000/cancel".to_string(),
            }),
        )
        .await;
        let status = resp.status();

        set_pack_price(&pool, "pack_500", original_pack_500_price.as_deref()).await;
        cleanup_account(&pool, account_id, &[user_id])
            .await
            .expect("cleanup test fixture");
        assert_eq!(status, StatusCode::CONFLICT, "a past_due (frozen) account must be refused a new pack purchase");
    }

    /// **Positive control**: an `active` subscription may still buy packs —
    /// without this, a bug that refused EVERY account regardless of status
    /// would also pass the two tests above.
    #[tokio::test]
    async fn pack_checkout_succeeds_for_an_active_subscription() {
        let Some(pool) = pg_test_pool("pack_checkout_succeeds_for_an_active_subscription").await
        else {
            return;
        };
        let _catalog_guard = CATALOG_PRICE_LOCK.lock().await;
        let (account_id, user_id) = seed_personal_account(&pool).await;
        crate::routes::extra_usage::pg_tests::make_pack_buyer(&pool, account_id).await;
        let original_pack_500_price = set_pack_price(&pool, "pack_500", Some("price_pack_500_test")).await;
        set_subscription_status(&pool, account_id, "active").await;
        let stripe = spawn_checkout_mock().await;
        let mut state = crate::test_app_state_owned_with_db_and_store(pool.clone(), None);
        state.stripe = Some(Arc::new(stripe));
        let state = Arc::new(state);

        let resp = create_pack_checkout(
            State(state),
            Extension(user_context(account_id, user_id)),
            Json(PackCheckoutRequest {
                pack: "pack_500".to_string(),
                quantity: 1,
                success_url: "http://localhost:3000/success".to_string(),
                cancel_url: "http://localhost:3000/cancel".to_string(),
            }),
        )
        .await;
        let status = resp.status();

        set_pack_price(&pool, "pack_500", original_pack_500_price.as_deref()).await;
        cleanup_account(&pool, account_id, &[user_id])
            .await
            .expect("cleanup test fixture");
        assert_eq!(status, StatusCode::OK, "an active subscription must still be able to buy packs");
    }

    /// **Positive control, no `stripe_subscriptions` row at all** (a
    /// personal/free-tier account that has never gone through Stripe) — must
    /// not be treated as frozen. Mirrors
    /// `billing::enforcement::pg_tests::no_stripe_subscriptions_row_is_not_treated_as_frozen`.
    #[tokio::test]
    async fn pack_checkout_succeeds_with_no_stripe_linkage_at_all() {
        let Some(pool) = pg_test_pool("pack_checkout_succeeds_with_no_stripe_linkage_at_all").await
        else {
            return;
        };
        let _catalog_guard = CATALOG_PRICE_LOCK.lock().await;
        let (account_id, user_id) = seed_personal_account(&pool).await;
        crate::routes::extra_usage::pg_tests::make_pack_buyer(&pool, account_id).await;
        let original_pack_500_price = set_pack_price(&pool, "pack_500", Some("price_pack_500_test")).await;
        let stripe = spawn_checkout_mock().await;
        let mut state = crate::test_app_state_owned_with_db_and_store(pool.clone(), None);
        state.stripe = Some(Arc::new(stripe));
        let state = Arc::new(state);

        let resp = create_pack_checkout(
            State(state),
            Extension(user_context(account_id, user_id)),
            Json(PackCheckoutRequest {
                pack: "pack_500".to_string(),
                quantity: 1,
                success_url: "http://localhost:3000/success".to_string(),
                cancel_url: "http://localhost:3000/cancel".to_string(),
            }),
        )
        .await;
        let status = resp.status();

        set_pack_price(&pool, "pack_500", original_pack_500_price.as_deref()).await;
        cleanup_account(&pool, account_id, &[user_id])
            .await
            .expect("cleanup test fixture");
        assert_eq!(status, StatusCode::OK, "an account with no Stripe linkage at all must not be treated as frozen");
    }

    // ── `require_purchase_authority`: a personal org must not buy a seat
    // tier ──────────────────────────────────────────────────────────────────
    //
    // Calls `require_purchase_authority` DIRECTLY rather than through
    // `routes::billing_seats::change_seat_tier` (the route this gate exists
    // for with `org_only: true`) — that route needs its own heavy fixture
    // (a `team` plan grant, a Stripe subscription item to move, etc., see
    // `routes::billing_seats::pg_tests::seed_org_account`) that would only
    // dilute what these tests are actually proving: what THIS function
    // decides for each of the three account shapes.

    /// **Shape 1 (unchanged control) — a plain personal account is still
    /// refused a seat-tier purchase.**
    #[tokio::test]
    async fn require_purchase_authority_org_only_refuses_a_plain_personal_account() {
        let Some(pool) =
            pg_test_pool("require_purchase_authority_org_only_refuses_a_plain_personal_account").await
        else {
            return;
        };
        let (account_id, user_id) = seed_personal_account(&pool).await;
        let state = crate::test_app_state_owned_with_db_and_store(pool.clone(), None);

        let result = require_purchase_authority(&state, &user_context(account_id, user_id), true).await;

        cleanup_account(&pool, account_id, &[user_id]).await.expect("cleanup test fixture");

        let resp = result.expect_err("a plain personal account must be refused a seat-tier purchase");
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    /// **Shape 2 (the fix) — a personal org's own account must be refused a
    /// seat-tier purchase, the SAME as a plain personal account.** Before
    /// this PR, `is_org_account` read `true` for this account (the seat-tier
    /// trigger derives it from `clerk_org_id IS NOT NULL` alone), the
    /// function took the org branch, and the personal-org owner's own
    /// `role = 'owner'` sailed straight through the admin check below —
    /// making a seat tier purchasable on a one-person account.
    #[tokio::test]
    async fn require_purchase_authority_org_only_refuses_a_personal_org() {
        let Some(pool) = pg_test_pool("require_purchase_authority_org_only_refuses_a_personal_org").await
        else {
            return;
        };
        let (account_id, user_id, clerk_org_id) = seed_personal_org_account(&pool).await;
        let state = crate::test_app_state_owned_with_db_and_store(pool.clone(), None);

        let result = require_purchase_authority(&state, &user_context(account_id, user_id), true).await;

        cleanup_personal_org(&pool, &clerk_org_id).await;
        cleanup_account(&pool, account_id, &[user_id]).await.expect("cleanup test fixture");

        let resp = result.expect_err(
            "a personal org's own account must be refused a seat-tier purchase, same as a plain \
             personal account",
        );
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST, "must be the SAME 400 a plain personal account gets");
    }

    /// **Shape 3 (unchanged control) — a REAL org's owner may still buy a
    /// seat tier.** The risk in this PR is loosening a check that should
    /// stay tight for real organizations — this proves it did not.
    #[tokio::test]
    async fn require_purchase_authority_org_only_allows_a_real_orgs_owner() {
        let Some(pool) = pg_test_pool("require_purchase_authority_org_only_allows_a_real_orgs_owner").await
        else {
            return;
        };
        let (account_id, user_id) = seed_org_account(&pool, "owner").await;
        let state = crate::test_app_state_owned_with_db_and_store(pool.clone(), None);

        let result = require_purchase_authority(&state, &user_context(account_id, user_id), true).await;

        cleanup_account(&pool, account_id, &[user_id]).await.expect("cleanup test fixture");

        assert!(result.is_ok(), "a real org's owner must still be allowed to buy a seat tier");
    }

    /// A personal org's own account still purchases its OWN (non-seat-tier)
    /// billing exactly like a plain personal account — `org_only: false`,
    /// the branch `create_subscription_checkout`/`create_pack_checkout` use.
    /// This is the other half of the fix: excluding a personal org from
    /// "is a real org" must not ALSO accidentally start requiring
    /// org-admin-role gymnastics for its own everyday purchases.
    #[tokio::test]
    async fn require_purchase_authority_non_org_only_allows_a_personal_orgs_own_purchase() {
        let Some(pool) =
            pg_test_pool("require_purchase_authority_non_org_only_allows_a_personal_orgs_own_purchase").await
        else {
            return;
        };
        let (account_id, user_id, clerk_org_id) = seed_personal_org_account(&pool).await;
        let state = crate::test_app_state_owned_with_db_and_store(pool.clone(), None);

        let result = require_purchase_authority(&state, &user_context(account_id, user_id), false).await;

        cleanup_personal_org(&pool, &clerk_org_id).await;
        cleanup_account(&pool, account_id, &[user_id]).await.expect("cleanup test fixture");

        assert!(result.is_ok(), "a personal org owner must still be able to purchase for their own account");
    }

    /// **Fail-closed on a missing account row (PR 829 review, finding 4).**
    /// Before this fix `account_is_real_org` (which this function now calls
    /// instead of duplicating its SQL) read `Ok(false)` for an `account_id`
    /// with no `accounts` row at all — this function has always treated that
    /// case as an internal error instead, and the shared method now matches.
    #[tokio::test]
    async fn require_purchase_authority_fails_closed_when_the_account_row_is_missing() {
        let Some(pool) =
            pg_test_pool("require_purchase_authority_fails_closed_when_the_account_row_is_missing").await
        else {
            return;
        };
        let state = crate::test_app_state_owned_with_db_and_store(pool.clone(), None);

        let result =
            require_purchase_authority(&state, &user_context(Uuid::new_v4(), Uuid::new_v4()), true).await;

        let resp = result.expect_err("a missing accounts row must be refused, not silently allowed");
        assert_eq!(
            resp.status(),
            StatusCode::INTERNAL_SERVER_ERROR,
            "a missing accounts row is an internal error, not a 400/403"
        );
    }
}
