//! `POST /v1/stripe/webhook` — the renewal clock.
//!
//! # What this endpoint is for
//!
//! The founder's build plan settles the renewal clock as **Stripe's billing
//! period**: a failed card produces no `invoice.paid`, so no refresh happens
//! and access degrades by itself, with no dunning logic needed to stop service.
//! This handler is that clock. Everything Phase 2b built is inert without it —
//! `member_allowance_periods` has no other writer.
//!
//! # Authentication
//!
//! There is no Clerk session here. The request is authenticated **only** by its
//! `Stripe-Signature` header, exactly as `routes::slack_events` is authenticated
//! only by `X-Slack-Signature`. That makes the verifier the entire security
//! boundary: a forged `invoice.paid` grants credits, so
//! [`verify_stripe_signature_with_secret`] is written to the same rules the
//! Slack verifier was hardened to after review (see `slack_events.rs`'s M1
//! notes) —
//!
//! * HMAC fed incrementally over **raw bytes**, never a `from_utf8_lossy`
//!   `String`. Two distinct bodies must not collapse onto one signing base.
//! * The received signature is hex-decoded to `[u8; 32]` **first**, so a wrong
//!   length is a mismatch rather than an early return that leaks the expected
//!   length through a distinct branch.
//! * Constant-time comparison via `subtle::ConstantTimeEq` on the decoded
//!   arrays.
//! * A timestamp outside a 300-second window is rejected before the body is
//!   looked at, which is what makes a captured-and-replayed request stop
//!   working. Stripe's own default tolerance is the same 300 seconds.
//!
//! Stripe's scheme differs from Slack's in exactly one way that matters: the
//! header carries a **list** of signatures (`t=...,v1=...,v1=...`), because a
//! secret roll leaves both the old and new signature valid for a window. Every
//! `v1` is checked and any match accepts. Checking only the first would break
//! every rotation.
//!
//! # Idempotency — why the claim and the work share one transaction
//!
//! Stripe's delivery contract is **at-least-once**. A retry after a timeout, a
//! manual resend from the dashboard, or a network blip all deliver the same
//! `event_id` again, so redelivery is the normal path rather than an edge case.
//!
//! `0161`'s UNIQUE index already makes the grant itself idempotent, but that
//! alone is not enough: it protects one specific write, while Phase 4b adds a
//! status transition and (billing self-serve PR D) checkout completion adds a
//! grant issuance / credit-pack ledger row, **none of which is naturally
//! idempotent** on its own. So the handler claims the event and does the work
//! in ONE transaction:
//!
//! ```text
//! INSERT INTO stripe_webhook_events ... ON CONFLICT DO NOTHING
//!   rows_affected == 0  -> replay: roll back, answer 200, do nothing
//!   rows_affected == 1  -> we own this event: do the work, commit
//! ```
//!
//! A concurrent duplicate delivery blocks on the uncommitted insert and then
//! reads zero rows, so the serialization is Postgres's rather than ours.
//!
//! # Why almost everything answers 200
//!
//! A non-2xx tells Stripe to redeliver, with backoff, for days. That is the
//! right answer **only** when redelivering could succeed. So:
//!
//! * unknown event type, unknown customer, non-subscription invoice, replay →
//!   **200**. Nothing about retrying these changes the outcome; a non-200 would
//!   buy an endlessly retried event and a noisy Stripe dashboard.
//! * a database failure mid-renewal → **500**, deliberately. The work did not
//!   happen, the claim rolled back with it, and redelivery is exactly what
//!   should occur.
//! * a bad signature → **401**, and a missing secret → **503**, matching
//!   `slack_events`.
//!
//! A signature-valid but MALFORMED payload — unparseable JSON, no `id`/`type`,
//! no `data.object`, no customer, an unusable period — returns **400**, which
//! is a deliberate exception to the rule above rather than an oversight of it.
//! Retrying cannot fix a payload-shape disagreement, so by the stated principle
//! it "should" be a 200. It is not, because that class of failure means Stripe
//! and this handler no longer agree about the wire format — almost always an
//! API-version change on the endpoint — and the Stripe dashboard's delivery log
//! is the ONLY monitoring surface this endpoint has. A silent 200 would hide a
//! version break indefinitely while every renewal quietly stopped; a visible
//! failure gets it noticed. The cost is some pointless retries, which is much
//! the cheaper mistake.
//!
//! # Scope, Phase 4a + 4b + billing self-serve PR D
//!
//! Phase 4a shipped `invoice.paid` alone. Phase 4b added:
//!
//! * `customer.subscription.updated` — syncs `grants.seats` from the
//!   subscription item's `quantity` (EXCEPT for a `team` grant, whose
//!   `grants.seats` is a purchase-ledger total maintained by
//!   `billing::seat_lifecycle`/`routes::billing_seats` instead — billing
//!   self-serve launch PR E, review round 2, finding 3, retargeted from the
//!   roster to the ledger by PR F; see `apply_subscription_updated`'s own
//!   doc comment), grants a mid-period seat ADD the FULL current-period
//!   allowance immediately (never prorated — Stripe already prorates the
//!   money), and maps Stripe's status onto `stripe_subscriptions.status` so
//!   an involuntary lapse (`past_due`, `unpaid`, ...) freezes.
//! * `customer.subscription.deleted` — **freeze-not-forfeit** (billing
//!   self-serve plan §1.11 — supersedes this module's original Phase 4b
//!   design, which forfeited the purchased credit pool on a DELIBERATE
//!   cancellation and froze on every other reason). Every deletion, whatever
//!   the reason, now does exactly the same thing: `stripe_subscriptions
//!   .status` flips to `"canceled"` and nothing else changes. Purchased
//!   credits survive EVERY cancellation, unspendable until the account
//!   resubscribes — see [`crate::billing::enforcement`]'s
//!   `subscription_frozen` for the read-side gate that makes "unspendable"
//!   real, and [`apply_subscription_deleted`]'s own doc comment for why the
//!   forfeiture arm was retired outright rather than merely disabled.
//!
//! Billing self-serve PR D (plan §2.D) adds a third:
//!
//! * `checkout.session.completed` — the completion side of
//!   `routes::checkout`'s outbound checkout routes. `mode = "subscription"`
//!   issues the purchased plan/seat-tier grant (via
//!   [`crate::billing::issuance::issue_grant`], under the SAME
//!   `lock_period_rotation` invariant every other period-rotating path in
//!   this file honours) and mirrors the new subscription onto
//!   `stripe_subscriptions`; `mode = "payment"` credits the extra-usage
//!   ledger and balance (a top-up, or a credit pack; see
//!   `billing::extra_usage::webhook`). Stripe's event payload carries no line
//!   items — the handler re-fetches the session with `expand[]=line_items`
//!   via `state.stripe` and parses ALL of them
//!   ([`crate::billing::stripe_client::parse_checkout_session_line_items`]),
//!   never just the first.
//!
//! Every other event type is still accepted and ignored, exactly as before —
//! see "Why almost everything answers 200" above.
//!
//! # Only a billing CYCLE rolls the clock (issue #644)
//!
//! `invoice.paid` fires for more than renewals. The `subscription_id` guard
//! above already turns away a one-off invoice, but a mid-cycle PRORATION
//! (`billing_reason = "subscription_update"`, raised whenever seats change)
//! carries a subscription id and sails straight past it.
//!
//! It must not roll anything, because the period on a proration's lines is
//! **the moment the seats changed**, not the cycle anchor. `invoice.paid`
//! reads the period from `lines.data[0].period`; `customer.subscription
//! .updated` reads it from `items.data[0].current_period_start`. On a
//! proration those two disagree — 55 seconds apart on the production incident
//! that found this — with the invoice-derived value NEWER. Mirror that value
//! into `stripe_subscriptions.current_period_start` and the staleness guard
//! below, which compares with strict `<`, then judges EVERY later
//! subscription event for that period superseded: no seat sync, and no freeze
//! on an involuntary lapse, until the next renewal resets the mirror.
//!
//! Note the fix is NOT to pick a different invoice line. On the invoice that
//! caused #644 both lines carried the same proration `start`; line choice
//! only ever affected the `end`. The reason field is the only thing that
//! distinguishes a cycle boundary from a mid-cycle event, and before #644 it
//! was never read at all — see [`rolls_the_renewal_clock`], including why a
//! MISSING `billing_reason` is deliberately permissive.
//!
//! # Two facts about the Subscription object that will silently break this
//! # handler if forgotten
//!
//! Both were established by probing the LIVE Stripe API directly (sandbox
//! fixture `sub_example8f3835d1` / `cus_example3ff2bfaf`), not from
//! Stripe's documentation, which still describes the OLD shape:
//!
//! * **`current_period_start`/`current_period_end` no longer exist on the
//!   Subscription object itself.** They moved to its first line item
//!   (`items.data[0].current_period_start`/`.current_period_end`) under the
//!   2025 "flexible" `billing_mode`. A handler that reads
//!   `subscription.current_period_end` gets `None` and does nothing — this
//!   repo's recurring "absence renders as success" failure (see
//!   [`subscription_period`]). An unresolvable period is therefore a LOUD
//!   error here, exactly like `invoice.paid`'s malformed-payload posture
//!   above, never a quiet skip.
//! * **`quantity` appears twice.** `items.data[0].quantity` is authoritative;
//!   the legacy top-level `subscription.quantity` is documented by Stripe as
//!   present only while the subscription has a single item, which is the
//!   only shape this handler ever reads anyway (see [`subscription_seats`]).
//!
//! # Out-of-order delivery: the staleness guard
//!
//! Stripe's delivery contract is at-least-once, **not** in-order — the idempotency
//! claim above only dedupes a repeat of the SAME `event_id`; it says nothing about
//! a genuinely OLDER event arriving after a newer one already landed. Both
//! [`apply_invoice_paid`] and [`apply_subscription_updated`] call
//! `renewal::close_open_period`, which DELETES any allowance row whose
//! `period_start` is strictly after the boundary it is given (see that
//! function's own doc comment). If a stale event — a retried `invoice.paid`
//! whose period Stripe has since moved past, or a `customer.subscription.updated`
//! delivered out of order — reaches that call with an OLDER `period_start` than
//! what has already been granted, it deletes rows a LATER, already-processed
//! event created and the customer has already been charged for. Renewing that
//! stale, older period afterward would also silently shrink the account's seat
//! count and allowance back to what it was before a since-applied seat change.
//!
//! The guard: before either function does anything destructive, it reads
//! `stripe_subscriptions.current_period_start` — the mirror of "what Stripe
//! last told us the period is" (`0162`'s header) — inside the SAME transaction
//! the claim happened in, row-locked (`FOR UPDATE`) so two deliveries for the
//! same account cannot race each other's staleness check. If that stored value
//! is non-NULL and the INCOMING `period_start` is **strictly older**, the event
//! is superseded: no `close_open_period`, no `roll_period`, no seat sync, no
//! period-mirror update — a stale event's seat count is exactly as stale as its
//! period, so touching either would reintroduce the same silent-regression risk
//! the period guard exists to close. The claim is still committed (the event
//! stays claimed forever, so a REtry of this same stale event id is a replay,
//! not a second stale-check), a `stale`-labelled outcome is recorded for the
//! support/audit trail, and the handler answers **200** — retrying a superseded
//! event can never make it un-superseded, so asking Stripe to redeliver would
//! only buy a noisy dashboard, exactly the reasoning "Why almost everything
//! answers 200" already gives for a replay.
//!
//! **Equal timestamps are NOT stale.** The comparison is strict `<`, not `<=`.
//! A same-period redelivery under a NEW `event_id` (`pg_tests::
//! a_new_event_for_an_already_granted_period_grants_nothing_extra`) and a
//! `customer.subscription.updated` that reports the SAME period a preceding
//! `invoice.paid` already recorded (`pg_tests::
//! subscription_updated_past_due_freezes_and_a_later_invoice_paid_restores_active`,
//! and the seat-sync tests, which change seats without moving the period at
//! all) both carry `incoming period_start == stored period_start` and MUST
//! still do their normal work — `close_open_period` is written to be a no-op
//! at that exact boundary (see `DELETE_UNSTARTED_OPEN_PERIOD_SQL`'s own doc
//! comment for why `>`, not `>=`, is the correct edge there too), and
//! `0161`'s UNIQUE index is what actually keeps the grant idempotent. Treating
//! an equal timestamp as stale would turn every one of those legitimate,
//! already-tested cases into a silent no-op instead.
//!
//! A NULL stored `current_period_start` — an account linked but never yet
//! billed — means there is no basis for comparison, so the guard is skipped
//! and the event proceeds normally.
//!
//! Both call sites return [`ClaimedOutcome::Stale`] rather than inventing a
//! zero-valued [`crate::billing::renewal::RenewalOutcome`]/
//! [`SubscriptionUpdateOutcome`] for this case: those types report what a
//! ROLL did, and a stale event never rolled anything, so reusing either shape
//! would read as a real (if empty) renewal — the same "absence renders as
//! success" failure this module's "Two facts" section above already warns
//! about — rather than the fundamentally different fact that the event was
//! refused outright.

use axum::{
    body::Bytes,
    extract::State,
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    Json,
};
use hmac::{Hmac, Mac};
use serde_json::{json, Value};
use sha2::Sha256;
use std::sync::Arc;
use uuid::Uuid;

use crate::AppState;

type HmacSha256 = Hmac<Sha256>;

/// Stripe's own default webhook tolerance, and the same window
/// `slack_events` uses.
const REPLAY_TOLERANCE_SECS: i64 = 300;

// ── Signature verification ───────────────────────────────────────────────────

/// Outcome of signature verification. Mirrors
/// `routes::slack_events::VerifyResult` so the two webhook entry points fail in
/// the same shapes and a reader who knows one knows the other.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum VerifyResult {
    Ok,
    /// `STRIPE_WEBHOOK_SECRET` is absent or empty — callers respond 503.
    SecretNotSet,
    /// `t=` missing, unparseable, or skew > [`REPLAY_TOLERANCE_SECS`].
    TimestampInvalid,
    /// `Stripe-Signature` missing, malformed, or no `v1=` entry matched.
    SignatureMismatch,
}

/// Verify with the signing secret read from the environment.
fn verify_stripe_signature(headers: &HeaderMap, body: &[u8]) -> VerifyResult {
    let secret = match std::env::var("STRIPE_WEBHOOK_SECRET") {
        Ok(s) if !s.is_empty() => s,
        _ => return VerifyResult::SecretNotSet,
    };
    verify_stripe_signature_with_secret(&secret, headers, body, chrono::Utc::now().timestamp())
}

/// Inner verification taking the secret and the current time directly.
///
/// Both are parameters rather than ambient reads so the tests never touch
/// process-global state: `slack_events`'s own test suite needed a mutex around
/// `set_var` because `cargo test` runs threads of ONE process and two tests
/// setting the same variable race. Passing `now` in additionally makes the
/// replay-window boundary testable without sleeping or freezing the clock.
///
/// # The header format
///
/// ```text
/// Stripe-Signature: t=1492774577,v1=5257a869...,v1=...,v0=...
/// ```
///
/// The signing base is `{t}.{raw_body}` — the literal timestamp string as sent,
/// NOT a re-formatted parse of it, since any reformatting would change the
/// bytes that were signed.
///
/// `v0` entries are ignored: they are Stripe's test-mode-only legacy scheme and
/// are not HMAC-SHA256 over this base. Accepting them would be accepting a
/// signature we never verified.
pub(crate) fn verify_stripe_signature_with_secret(
    secret: &str,
    headers: &HeaderMap,
    body: &[u8],
    now: i64,
) -> VerifyResult {
    let header = match headers.get("stripe-signature").and_then(|v| v.to_str().ok()) {
        Some(h) => h,
        None => return VerifyResult::SignatureMismatch,
    };

    // Split once into the timestamp and every candidate v1 signature. A secret
    // roll leaves two valid signatures on one request, so this must collect all
    // of them rather than stopping at the first.
    let mut timestamp: Option<&str> = None;
    let mut candidates: Vec<&str> = Vec::new();
    for part in header.split(',') {
        let part = part.trim();
        if let Some(t) = part.strip_prefix("t=") {
            // Only the FIRST t= is honoured. A header carrying two different
            // timestamps is malformed, and picking the last would let an
            // attacker append a fresh one to revive a captured request.
            if timestamp.is_none() {
                timestamp = Some(t);
            }
        } else if let Some(v) = part.strip_prefix("v1=") {
            candidates.push(v);
        }
    }

    let Some(timestamp) = timestamp else {
        return VerifyResult::TimestampInvalid;
    };
    let Ok(ts_secs) = timestamp.parse::<i64>() else {
        return VerifyResult::TimestampInvalid;
    };
    // Replay protection, checked BEFORE any HMAC work: a captured request stops
    // working once it ages out, regardless of how valid its signature is.
    // `checked_sub` rather than `-`: a hostile `t=` can be i64::MIN, and the
    // plain subtraction would overflow-panic in debug and wrap in release,
    // which would turn a garbage timestamp into an ACCEPTED one.
    match now.checked_sub(ts_secs) {
        Some(skew) if skew.unsigned_abs() <= REPLAY_TOLERANCE_SECS.unsigned_abs() => {}
        _ => return VerifyResult::TimestampInvalid,
    }

    if candidates.is_empty() {
        return VerifyResult::SignatureMismatch;
    }

    let mut mac = match HmacSha256::new_from_slice(secret.as_bytes()) {
        Ok(m) => m,
        Err(_) => return VerifyResult::SignatureMismatch,
    };
    // Raw bytes, incrementally — never a lossy String, so distinct bodies
    // cannot collapse onto one signing base.
    mac.update(timestamp.as_bytes());
    mac.update(b".");
    mac.update(body);
    let computed: [u8; 32] = mac.finalize().into_bytes().into();

    use subtle::ConstantTimeEq;
    let mut matched = false;
    for candidate in candidates {
        // Decode first: a wrong length becomes a mismatch here rather than an
        // early length-leaking branch. The loop deliberately does NOT break on
        // success — it keeps the number of comparisons independent of which
        // candidate matched.
        let received: [u8; 32] = match hex::decode(candidate) {
            Ok(v) if v.len() == 32 => {
                let mut arr = [0u8; 32];
                arr.copy_from_slice(&v);
                arr
            }
            _ => continue,
        };
        matched |= bool::from(computed.ct_eq(&received));
    }

    if matched {
        VerifyResult::Ok
    } else {
        VerifyResult::SignatureMismatch
    }
}

// ── Payload accessors ────────────────────────────────────────────────────────

/// Stripe expands-or-inlines: a reference field is either the bare id string or
/// the whole expanded object carrying `id`. Both shapes are legal on the same
/// field depending on the endpoint's expansion settings, so every id read goes
/// through here rather than assuming one.
fn reference_id(value: Option<&Value>) -> Option<&str> {
    match value? {
        Value::String(s) if !s.is_empty() => Some(s),
        Value::Object(o) => o.get("id").and_then(Value::as_str).filter(|s| !s.is_empty()),
        _ => None,
    }
}

/// The subscription this invoice belongs to, or `None` for a one-off invoice.
///
/// Read from both the field Stripe used historically (`invoice.subscription`)
/// and the one that replaced it in the 2025 API versions
/// (`invoice.parent.subscription_details.subscription`), because which one
/// arrives depends on the API version pinned on the webhook endpoint — a
/// setting in the Stripe dashboard, not in this repo. Supporting only one would
/// make the renewal silently stop working the day that setting changed.
///
/// **This is the load-bearing guard against a non-subscription invoice.** A
/// one-off charge — a future credit pack, say — must NOT roll everyone's
/// allowance period, which is exactly what would happen if the handler assumed
/// every paid invoice were a renewal.
fn subscription_id(invoice: &Value) -> Option<&str> {
    reference_id(invoice.get("subscription")).or_else(|| {
        reference_id(
            invoice
                .get("parent")?
                .get("subscription_details")?
                .get("subscription"),
        )
    })
}

/// The billing period this invoice covers, as `(start, end)` Unix seconds.
///
/// Prefers the **line item's** period over the invoice's. They usually agree,
/// but on a proration or a mid-cycle change the invoice-level window can span
/// more than the subscription period being paid for, and granting an allowance
/// against the wrong window would put `period_start` somewhere
/// `member_allowance_periods`' "current period" lookup never finds — an
/// allowance that exists in the table and is invisible to the gate.
fn billing_period(invoice: &Value) -> Option<(i64, i64)> {
    let from_line = invoice
        .get("lines")
        .and_then(|l| l.get("data"))
        .and_then(Value::as_array)
        .and_then(|lines| {
            lines.iter().find_map(|line| {
                let period = line.get("period")?;
                Some((
                    period.get("start")?.as_i64()?,
                    period.get("end")?.as_i64()?,
                ))
            })
        });

    from_line.or_else(|| {
        Some((
            invoice.get("period_start")?.as_i64()?,
            invoice.get("period_end")?.as_i64()?,
        ))
    })
}

/// Stripe's `billing_reason` — why this invoice exists. `subscription_cycle`
/// (the recurring renewal), `subscription_create` (the first invoice),
/// `subscription_update` (a mid-cycle proration), `subscription_threshold`,
/// `manual`, and others.
fn billing_reason(invoice: &Value) -> Option<&str> {
    invoice.get("billing_reason").and_then(Value::as_str)
}

/// Whether this paid invoice is a BILLING CYCLE boundary — the only kind that
/// may roll the renewal clock. **Issue #644.**
///
/// The module doc already states the sibling rule for one-off invoices ("A
/// one-off invoice must never roll an allowance period", enforced by
/// [`subscription_id`]). A mid-cycle proration is the same hazard wearing a
/// subscription id: it IS a subscription invoice, so that guard waves it
/// through, but the period on its lines is the moment the seats changed, not
/// the cycle anchor. Writing that into `stripe_subscriptions
/// .current_period_start` moves the mirror FORWARD of what the Subscription
/// object itself reports, and since the staleness guard compares with strict
/// `<`, every later `customer.subscription.updated` for that period is then
/// judged superseded — seat syncs and involuntary-lapse freezes both stop.
///
/// Note this is deliberately NOT fixed by re-picking the invoice line: on the
/// invoice that caused #644, BOTH lines carried the same proration `start`.
/// Line choice only ever affected the `end`.
///
/// **Absence is permissive.** A missing or null `billing_reason` rolls, the
/// pre-#644 behaviour. Every invoice used to roll, so treating absence as "do
/// not roll" would let a future API version that stops sending the field halt
/// EVERY renewal silently — exactly the failure mode [`billing_period`] and
/// [`subscription_period`] keep fallbacks for. This guard only ever subtracts
/// rolling from cases proven wrong; it never adds a new way for renewals to
/// stop.
fn rolls_the_renewal_clock(invoice: &Value) -> bool {
    match billing_reason(invoice) {
        Some(reason) => matches!(reason, "subscription_cycle" | "subscription_create"),
        None => true,
    }
}

/// Unix seconds to a UTC datetime, rejecting values outside the representable
/// range rather than saturating. A saturated bound would write a period the
/// gate could never match.
fn to_utc(secs: i64) -> Option<chrono::DateTime<chrono::Utc>> {
    chrono::DateTime::from_timestamp(secs, 0)
}

// ── Subscription payload accessors (Phase 4b) ──────────────────────────────

/// The subscription's first line item, `items.data[0]`. A subscription can
/// in general carry more than one item, but every subscription this system
/// creates is single-item (one price, `quantity` = seats), and Stripe's own
/// docs say the legacy top-level `quantity` field is present only in that
/// single-item shape — so reading anything but the first item here would be
/// reading a shape this handler was never built to price.
fn subscription_item(sub: &Value) -> Option<&Value> {
    sub.get("items")?.get("data")?.as_array()?.first()
}

/// `(current_period_start, current_period_end)` in Unix seconds.
///
/// Reads the ITEM first — see this module's own doc comment for why: at the
/// live API version probed for this build, the Subscription object no
/// longer carries these fields at all, only its first item does. The
/// top-level fallback exists for symmetry with [`billing_period`] (a
/// genuine API-version difference, not a guess) but is not expected to ever
/// fire against this account's live traffic.
///
/// Returns `None` if NEITHER shape resolves. The caller must treat that as a
/// loud error, never a silent skip — see the module doc's "Two facts" note.
fn subscription_period(sub: &Value) -> Option<(i64, i64)> {
    subscription_item(sub)
        .and_then(|item| {
            Some((
                item.get("current_period_start")?.as_i64()?,
                item.get("current_period_end")?.as_i64()?,
            ))
        })
        .or_else(|| {
            Some((
                sub.get("current_period_start")?.as_i64()?,
                sub.get("current_period_end")?.as_i64()?,
            ))
        })
}

/// Purchased seats: `items.data[0].quantity`, the authoritative field, with
/// a documented fallback to the legacy top-level `subscription.quantity`.
/// The fallback is safe rather than a guess: Stripe documents that field as
/// present only while the subscription has a single item, and
/// [`subscription_item`] never reads past the first item anyway, so the two
/// cannot legitimately disagree for any subscription this handler prices.
fn subscription_seats(sub: &Value) -> Option<i64> {
    subscription_item(sub)
        .and_then(|item| item.get("quantity"))
        .and_then(Value::as_i64)
        .or_else(|| sub.get("quantity").and_then(Value::as_i64))
}

/// The raw Stripe subscription `status` string (`"active"`, `"past_due"`,
/// `"trialing"`, ...) — see [`map_subscription_status`] for how this is
/// mapped onto `0162`'s closed three-value CHECK.
fn subscription_status(sub: &Value) -> Option<&str> {
    sub.get("status").and_then(Value::as_str)
}

/// `cancellation_details.reason` on a subscription payload — one of
/// `cancellation_requested` | `payment_disputed` | `payment_failed`, or
/// absent entirely for a natural (non-cancelled) expiry. Read only off
/// `customer.subscription.deleted` payloads.
///
/// **No longer branches any behavior** (billing self-serve plan §1.11,
/// freeze-not-forfeit — supersedes the Phase 4b forfeit/freeze split this
/// function used to drive). Every `customer.subscription.deleted` event
/// freezes identically regardless of reason now, so this value is read only
/// to carry into [`apply_subscription_deleted`]'s recorded outcome — visible
/// on `stripe_webhook_events.outcome` for support/audit, never branched on.
fn cancellation_reason(sub: &Value) -> Option<&str> {
    sub.get("cancellation_details")?.get("reason")?.as_str()
}

/// Maps ANY Stripe subscription `status` onto `0162`'s closed three-value
/// CHECK (`active` | `past_due` | `canceled`) — used for
/// `customer.subscription.updated` only.
///
/// `0162`'s header requires this mapping to exist: "an unrecognized Stripe
/// status can never be written as itself and then silently read as if it
/// meant something." The catch-all arm below is what makes that hold for a
/// status this build has never seen, and it deliberately lands on
/// `past_due` rather than `active` or `canceled` — freezing an account that
/// should have stayed active only delays it, while granting access that was
/// not paid for is the wrong direction to guess in. `past_due` is the safer
/// of the two to be wrong about.
///
/// `canceled` is UNREACHABLE from this function on purpose: it is written
/// only by [`apply_subscription_deleted`], unconditionally, on every
/// `customer.subscription.deleted` event — see that function's own doc
/// comment (billing self-serve plan §1.11, freeze-not-forfeit) for why a
/// deleted subscription always gets this value rather than `past_due`,
/// regardless of `cancellation_details.reason`.
pub(crate) fn map_subscription_status(raw: &str) -> &'static str {
    match raw {
        // `trialing` is stored as `active` ON PURPOSE (#1557). The mirror's
        // status means "paid-up / entitled", and every reader (provisioning's
        // `Live`, checkout's already-subscribed guard, the unpaid sweep's
        // `past_due | canceled` watch, `status_is_frozen`) reads `active` as
        // that. Storing `trialing` would need the 0162 CHECK widened and
        // every one of those readers taught the new value: miss one and a
        // trialing org loses access or provisioning. The trial itself (state,
        // credits, backstop) lives in `org_trials`, and the billing page reads
        // it from there.
        "active" | "trialing" => "active",
        "past_due" | "unpaid" | "incomplete" | "incomplete_expired" | "paused" => "past_due",
        _ => "past_due",
    }
}

// ── Entry-kind vocabulary (Phase 4b + billing self-serve PR D) ──────────────

/// `credit_ledger.entry_kind` a Phase 4b forfeiture debit was recorded
/// under. **RETIRED as of billing self-serve plan §1.11
/// (freeze-not-forfeit)** — [`apply_subscription_deleted`] no longer writes
/// this kind, and no NEW row will ever carry it again. Kept as a named
/// constant (rather than deleted outright) solely so historical rows already
/// in production — written before this PR — remain greppable by name from
/// the code that used to produce them; nothing in this build inserts one.
#[allow(dead_code)]
pub(crate) const ENTRY_KIND_FORFEIT: &str = "forfeit_credit_pool";

/// `credit_ledger.entry_kind` for a top-up pack purchase (plan §1.10,
/// `checkout.session.completed` payment mode) — a POSITIVE `credits` row,
/// the same sign convention [`crate::billing::issuance`]'s grants use, not a
/// usage debit. Its own kind rather than overloading
/// `workers::billing_derivation::ENTRY_KIND_*` (those are debit axes) or
/// [`ENTRY_KIND_FORFEIT`] (retired, and semantically the opposite event
/// anyway).
///
/// No longer written: since settings launch PR 3.4 a pack purchase credits
/// `extra_usage_ledger` (`entry_kind = 'pack_purchase'`). Kept so the legacy
/// `credit_ledger` rows (none in prod) stay nameable.
#[allow(dead_code)]
pub(crate) const ENTRY_KIND_PACK_PURCHASE: &str = "credit_pack_purchase";

const EVENT_CHARGE_REFUNDED: &str = "charge.refunded";
const EVENT_DISPUTE_CREATED: &str = "charge.dispute.created";

/// What the founder needs to find the payment in the Stripe dashboard and
/// adjust the balance by hand.
#[derive(Debug, Default)]
struct ManualReviewFacts {
    charge_id: Option<String>,
    dispute_id: Option<String>,
    payment_intent: Option<String>,
    amount: Option<i64>,
    amount_refunded: Option<i64>,
    currency: Option<String>,
    reason: Option<String>,
    status: Option<String>,
}

fn manual_review_facts(event_type: &str, object: &Value) -> ManualReviewFacts {
    let s = |k: &str| reference_id(object.get(k)).map(str::to_string);
    let i = |k: &str| object.get(k).and_then(Value::as_i64);
    if event_type == EVENT_DISPUTE_CREATED {
        ManualReviewFacts {
            charge_id: s("charge"),
            dispute_id: s("id"),
            payment_intent: s("payment_intent"),
            amount: i("amount"),
            currency: s("currency"),
            reason: s("reason"),
            status: s("status"),
            ..Default::default()
        }
    } else {
        ManualReviewFacts {
            charge_id: s("id"),
            payment_intent: s("payment_intent"),
            amount: i("amount"),
            amount_refunded: i("amount_refunded"),
            currency: s("currency"),
            ..Default::default()
        }
    }
}

/// The founder alert for a refund or dispute. The repo's alerting convention
/// is a structured `alert = true` log line (see `managed::gate`,
/// `unknown_model_alert`), which log-based paging keys on. Nothing here
/// touches a balance: refunds and disputes are reconciled by hand.
fn emit_manual_review_alert(
    event_id: &str,
    event_type: &str,
    account_id: Option<Uuid>,
    facts: &ManualReviewFacts,
) {
    tracing::error!(
        alert = true,
        billing_alert = "stripe_refund_or_dispute",
        event_id,
        event_type,
        account_id = ?account_id,
        charge_id = ?facts.charge_id,
        dispute_id = ?facts.dispute_id,
        payment_intent = ?facts.payment_intent,
        amount = ?facts.amount,
        amount_refunded = ?facts.amount_refunded,
        currency = ?facts.currency,
        reason = ?facts.reason,
        status = ?facts.status,
        "Stripe refund/dispute needs MANUAL review: no balance or entitlement was changed"
    );
}

/// `charge.refunded` / `charge.dispute.created`: log + alert only (decision
/// 18). When the Stripe customer maps to an account the event is claimed in
/// `stripe_webhook_events` so a redelivery does not alert twice; when it does
/// not (or the charge cannot be read for a dispute), the alert still fires
/// and the answer is 200, since retrying cannot make the customer resolve.
///
/// The alert is emitted after the claim insert and BEFORE the commit: if the
/// commit fails the event is retried and alerts again (a duplicate), which
/// is the safe direction compared with a claimed-but-never-alerted event.
async fn handle_manual_review_event(
    state: &Arc<AppState>,
    event_id: &str,
    event_type: &str,
    object: &Value,
) -> axum::response::Response {
    let Some(control) = state.control_db() else {
        tracing::error!(event_id, "Stripe webhook: no control pool on this process");
        return (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": "database unavailable"})))
            .into_response();
    };
    let facts = manual_review_facts(event_type, object);

    let mut customer: Option<String> = reference_id(object.get("customer")).map(str::to_string);
    if customer.is_none() && event_type == EVENT_DISPUTE_CREATED {
        if let (Some(stripe), Some(charge)) = (state.stripe.as_ref(), facts.charge_id.as_deref()) {
            match stripe
                .get(&format!("/v1/charges/{charge}"), &crate::billing::stripe_client::Params::new())
                .await
            {
                Ok(v) => customer = reference_id(v.get("customer")).map(str::to_string),
                Err(e) => tracing::warn!(error = %e, event_id, charge, "Stripe webhook: could not read the disputed charge"),
            }
        }
    }
    let account_id = match customer.as_deref() {
        Some(c) => match resolve_account(control, c).await {
            Ok(id) => id,
            Err(e) => {
                tracing::error!(error = %e, event_id, "Stripe webhook: account resolution failed");
                return (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": "account resolution failed"})))
                    .into_response();
            }
        },
        None => None,
    };
    let Some(account_id) = account_id else {
        emit_manual_review_alert(event_id, event_type, None, &facts);
        return (StatusCode::OK, Json(json!({"ok": true, "alerted": true, "unlinked": true}))).into_response();
    };

    let fail = |what: &str, e: &dyn std::fmt::Display| {
        tracing::error!(error = %e, event_id, %account_id, what, "Stripe webhook: manual-review claim failed, asking Stripe to redeliver");
        (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": "claim failed"}))).into_response()
    };
    let mut tx = match crate::tenancy::scope::begin_account_scoped(control, account_id).await {
        Ok(tx) => tx,
        Err(e) => return fail("scope", &e),
    };
    if let Err(e) = crate::billing::renewal::lock_period_rotation(&mut tx, account_id).await {
        return fail("financial entry", &e);
    }
    let claimed = match sqlx::query(
        "INSERT INTO stripe_webhook_events (event_id, event_type, account_id) \
         VALUES ($1, $2, $3) ON CONFLICT (event_id) DO NOTHING",
    )
    .bind(event_id)
    .bind(event_type)
    .bind(account_id)
    .execute(&mut *tx)
    .await
    {
        Ok(r) => r.rows_affected(),
        Err(e) => return fail("claim", &e),
    };
    if claimed == 0 {
        let _ = tx.rollback().await;
        tracing::info!(event_id, %account_id, "Stripe webhook: replay, already alerted");
        return (StatusCode::OK, Json(json!({"ok": true, "replay": true}))).into_response();
    }
    emit_manual_review_alert(event_id, event_type, Some(account_id), &facts);
    if let Err(e) = sqlx::query("UPDATE stripe_webhook_events SET outcome = $2 WHERE event_id = $1")
        .bind(event_id)
        .bind("manual_review_alert_only no_balance_change")
        .execute(&mut *tx)
        .await
    {
        return fail("outcome", &e);
    }
    if let Err(e) = tx.commit().await {
        return fail("commit", &e);
    }
    (StatusCode::OK, Json(json!({"ok": true, "alerted": true}))).into_response()
}

// ── Handler ──────────────────────────────────────────────────────────────────

/// `POST /v1/stripe/webhook`
pub async fn stripe_webhook(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    match verify_stripe_signature(&headers, &body) {
        VerifyResult::Ok => {}
        VerifyResult::SecretNotSet => {
            tracing::error!(
                "Stripe webhook: STRIPE_WEBHOOK_SECRET is not configured — refusing the request. \
                 Every event is being dropped until it is set."
            );
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({"error": "STRIPE_WEBHOOK_SECRET not configured"})),
            )
                .into_response();
        }
        VerifyResult::TimestampInvalid => {
            tracing::warn!(
                "Stripe webhook: rejected — timestamp missing or outside the {}s replay window",
                REPLAY_TOLERANCE_SECS
            );
            return (
                StatusCode::UNAUTHORIZED,
                Json(json!({"error": "invalid signature"})),
            )
                .into_response();
        }
        VerifyResult::SignatureMismatch => {
            tracing::warn!("Stripe webhook: rejected — signature mismatch");
            return (
                StatusCode::UNAUTHORIZED,
                Json(json!({"error": "invalid signature"})),
            )
                .into_response();
        }
    }

    let payload: Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => {
            // Signature-valid but unparseable means Stripe and this handler
            // disagree about the wire format, which redelivery cannot fix.
            tracing::error!(error = %e, "Stripe webhook: signature valid but body is not JSON");
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": "invalid JSON"})),
            )
                .into_response();
        }
    };

    let (Some(event_id), Some(event_type)) = (
        payload.get("id").and_then(Value::as_str),
        payload.get("type").and_then(Value::as_str),
    ) else {
        tracing::error!("Stripe webhook: event missing `id` or `type`");
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "missing event id or type"})),
        )
            .into_response();
    };

    // Phase 4b + billing self-serve PR D handle four events. Everything else
    // is accepted and ignored so that enabling a not-yet-wired event in the
    // dashboard cannot produce a retry storm.
    if event_type != "invoice.paid"
        && event_type != "customer.subscription.updated"
        && event_type != "customer.subscription.deleted"
        && event_type != "checkout.session.completed"
        && event_type != EVENT_SUBSCRIPTION_CREATED
        && event_type != EVENT_CHARGE_REFUNDED
        && event_type != EVENT_DISPUTE_CREATED
        && event_type != "checkout.session.async_payment_succeeded"
        && event_type != "checkout.session.async_payment_failed"
        && event_type != EVENT_CHECKOUT_EXPIRED
        && !event_type.starts_with("payment_intent.")
    {
        tracing::debug!(event_id, event_type, "Stripe webhook: event type not handled");
        return (StatusCode::OK, Json(json!({"ok": true, "ignored": true}))).into_response();
    }

    let object = payload.get("data").and_then(|d| d.get("object"));
    let Some(object) = object else {
        tracing::error!(event_id, event_type, "Stripe webhook: event with no data.object");
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "missing data.object"})),
        )
            .into_response();
    };

    // Refunds and disputes are handled manually for launch (settings launch
    // decision 18): log + alert, change no balance. A dispute object carries
    // no `customer`, so this must run before the customer requirement below.
    if event_type == EVENT_CHARGE_REFUNDED || event_type == EVENT_DISPUTE_CREATED {
        return handle_manual_review_event(&state, event_id, event_type, object).await;
}

    // Settings 3.5: only auto-reload PaymentIntents are handled. Every other
    // PaymentIntent event (subscription invoices, Checkout top-ups, which are
    // credited by their own events) is accepted and ignored, BEFORE the
    // customer check below: a PaymentIntent need not have a customer.
    if event_type.starts_with("payment_intent.")
        && !crate::billing::extra_usage::reload::is_reload_event(event_type, object)
    {
        tracing::debug!(event_id, event_type, "Stripe webhook: not an auto-reload payment intent; ignored");
        return (StatusCode::OK, Json(json!({"ok": true, "ignored": true}))).into_response();
    }

    let Some(customer_id) = reference_id(object.get("customer")) else {
        tracing::error!(event_id, event_type, "Stripe webhook: event carries no customer");
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "missing customer"})),
        )
            .into_response();
    };

    // CONTROL PLANE, deliberately — `state.control_db()` rather than `state.db`.
    // Every table this handler touches is control-tree only: `stripe_subscriptions`
    // and `stripe_webhook_events` (0162), `member_allowance_periods` (0161),
    // `grants` (0150) and `account_members` (0001) are all in
    // `check_schema_parity.sh`'s CONTROL_TABLES, and `plans` is global reference
    // data. There is no content-plane read here and no `Tenant` extractor to use:
    // Stripe carries no Clerk JWT, so this route is apex-only and
    // signature-authenticated. Using the named accessor states that intent instead
    // of reaching for the ambient pool — and keeps `check_no_ambient_pool.sh`
    // guarding this file, which an ALLOWLIST entry would have switched off
    // permanently.
    let Some(control) = state.control_db() else {
        tracing::error!(event_id, "Stripe webhook: no control pool on this process");
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": "database unavailable"})),
        )
            .into_response();
    };

    // Resolve the account BEFORE opening a scoped transaction — the
    // chicken-and-egg every webhook entry point has, solved the way
    // `0086_webhook_share_bootstrap_functions` solved it for Slack and Graph.
    // This SECURITY DEFINER resolver returns a bare account id and nothing else,
    // so it cannot be used to probe billing state. Shared by all three event
    // types — every one of them keys off the same Stripe customer id.
    let account_id: Option<Uuid> =
        match resolve_account(control, customer_id).await {
            Ok(id) => id,
            Err(e) => {
                tracing::error!(error = %e, event_id, "Stripe webhook: account resolution failed");
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({"error": "account resolution failed"})),
                )
                    .into_response();
            }
        };

    let Some(account_id) = account_id else {
        // Not an error: a customer created in Stripe but never linked here, or
        // an event from another environment sharing the endpoint. Retrying will
        // never resolve it, so answer 200 rather than buying an endless retry.
        tracing::warn!(
            event_id,
            customer_id,
            "Stripe webhook: no account linked to this Stripe customer — nothing done"
        );
        return (StatusCode::OK, Json(json!({"ok": true, "unlinked": true}))).into_response();
    };

    // Settings launch 2.4: an account whose deletion is pending, running or
    // done must not start a subscription, take a payment or be credited. The
    // only event it still processes is the `customer.subscription.deleted` our
    // own immediate cancel produces.
    // `checkout.session.expired` moves no money and starts nothing, so it is
    // exempt too (the refusal below is an alert about money).
    if event_type != "customer.subscription.deleted" && event_type != EVENT_CHECKOUT_EXPIRED {
        match crate::services::account_deletion::state_for_account(control, account_id).await {
            Ok(Some(deletion_state)) => {
                return refuse_for_deleted_account(
                    state.stripe.as_deref(),
                    account_id,
                    event_id,
                    event_type,
                    &deletion_state,
                    object,
                )
                .await;
            }
            Ok(None) => {}
            Err(e) => {
                tracing::error!(error = %e, event_id, "Stripe webhook: could not read the account's deletion state");
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({"error": "deletion state lookup failed"})),
                )
                    .into_response();
            }
        }
    }

    // In-app org deletion (control 0387): a Checkout that completes, or a
    // subscription that is created, for an org that is being deleted (or is
    // already revoked) must never activate. It is ended on the spot instead.
    if matches!(
        event_type,
        "checkout.session.completed" | "checkout.session.async_payment_succeeded" | EVENT_SUBSCRIPTION_CREATED
    ) {
        match crate::services::org_deletion::billing_block_reason(control, account_id).await {
            Ok(Some(reason)) => {
                return refuse_for_deleting_org(state.stripe.as_deref(), account_id, event_id, event_type, &reason, object)
                    .await;
            }
            Ok(None) => {}
            Err(e) => {
                tracing::error!(error = %e, event_id, "Stripe webhook: could not read the organization's deletion state");
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({"error": "deletion state lookup failed"})),
                )
                    .into_response();
            }
        }
    }
    if event_type == EVENT_SUBSCRIPTION_CREATED {
        // Nothing else consumes it: activation is `checkout.session.completed`'s.
        return (StatusCode::OK, Json(json!({"ok": true, "ignored": true}))).into_response();
    }

    match event_type {
        "invoice.paid" => {
            handle_invoice_paid(control, state.stripe.as_deref(), account_id, event_id, event_type, customer_id, object).await
        }
        "customer.subscription.updated" => {
            handle_subscription_updated(control, state.stripe.as_deref(), account_id, event_id, event_type, object).await
        }
        "customer.subscription.deleted" => {
            handle_subscription_deleted(control, account_id, event_id, event_type, object).await
        }
        "checkout.session.completed" | "checkout.session.async_payment_succeeded" => {
            handle_checkout_completed(&state, control, account_id, event_id, event_type, object).await
        }
        EVENT_CHECKOUT_EXPIRED => handle_checkout_expired(control, account_id, event_id, object).await,
        "checkout.session.async_payment_failed" => {
            claim_and_record_unprocessable(
                control,
                account_id,
                event_id,
                event_type,
                "delayed payment for a checkout session failed; nothing was credited",
            )
            .await
        }
        t if t.starts_with("payment_intent.") => {
            handle_reload_payment_intent(&state, control, account_id, event_id, event_type, object).await
        }
        // Unreachable: every other type already returned 200 above.
        _ => unreachable!("event type filtered before account resolution"),
    }
}

/// A Checkout Session ended without payment (Stripe expires an unpaid session
/// after 24h, or we expire a superseded one ourselves).
const EVENT_CHECKOUT_EXPIRED: &str = "checkout.session.expired";

/// `checkout.session.expired`: forget the account's open subscription
/// Checkout, if this is the one. Nothing was bought, so nothing else changes:
/// the account's row stays `pending`, which is what lets it start a new
/// Checkout. A session that is no longer the recorded one (a newer Checkout
/// replaced it, which is also how our own best-effort expiry of a superseded
/// session arrives) matches no row and leaves the newer pointer alone.
/// Naturally idempotent, so no event claim; the write is inside the 0321
/// financial entry fence like every other write to `stripe_subscriptions`.
async fn handle_checkout_expired(
    control: &sqlx::PgPool,
    account_id: Uuid,
    event_id: &str,
    object: &Value,
) -> axum::response::Response {
    let Some(session_id) = object.get("id").and_then(Value::as_str) else {
        tracing::error!(event_id, %account_id, "Stripe webhook: checkout.session.expired carries no session id");
        return (StatusCode::BAD_REQUEST, Json(json!({"error": "missing session id"}))).into_response();
    };
    let result: Result<u64, sqlx::Error> = async {
        let mut tx = crate::tenancy::scope::begin_account_scoped(control, account_id)
            .await
            .map_err(|e| sqlx::Error::Protocol(e.to_string()))?;
        crate::billing::renewal::lock_period_rotation(&mut tx, account_id).await?;
        let n = sqlx::query(
            "UPDATE stripe_subscriptions SET pending_checkout_session_id = NULL, updated_at = now() \
              WHERE account_id = $1 AND pending_checkout_session_id = $2",
        )
        .bind(account_id)
        .bind(session_id)
        .execute(&mut *tx)
        .await?
        .rows_affected();
        tx.commit().await?;
        Ok(n)
    }
    .await;
    match result {
        Ok(cleared) => {
            tracing::info!(event_id, %account_id, session_id, cleared, "Stripe webhook: checkout session expired");
            (StatusCode::OK, Json(json!({"ok": true, "cleared": cleared}))).into_response()
        }
        Err(e) => {
            tracing::error!(error = %e, event_id, %account_id, "Stripe webhook: checkout.session.expired update failed — asking Stripe to redeliver");
            (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": "expired update failed"}))).into_response()
        }
    }
}

/// `customer.subscription.created`: only ever acted on to END a subscription
/// that appears for an org being deleted (see [`refuse_for_deleting_org`]).
const EVENT_SUBSCRIPTION_CREATED: &str = "customer.subscription.created";

/// Refuse a subscription-starting billing event for an ORG that has a live or
/// finished deletion, or a revocation (deleted in Clerk). Nothing is activated
/// or credited; a subscription the event created or completed is ended NOW
/// with the same key as the deletion job's own cancel
/// (`org-deleted-cancel:{sub}`, `prorate=false`, `invoice_now=false`), so
/// whichever runs second replays. No refund is attempted (decision 12: no
/// refunds); a payment that was taken is an alert for manual follow-up.
/// A failed cancel (Stripe configured, a subscription id, and a re-read that
/// does not show it ended; "already canceled" counts as success) answers 500 so
/// Stripe redelivers: with `ORG_DELETION_ENABLED` off there is no `stripe_sweep`
/// backstop, and a revoked org's subscription would keep billing. The cancel is
/// keyed, so a redelivery replays. Without Stripe configured, or without a
/// subscription id, it is an alert and 200.
pub(crate) async fn refuse_for_deleting_org(
    stripe: Option<&crate::billing::stripe_client::StripeClient>,
    account_id: Uuid,
    event_id: &str,
    event_type: &str,
    reason: &str,
    object: &Value,
) -> axum::response::Response {
    let subscription = match event_type {
        "checkout.session.completed" | "checkout.session.async_payment_succeeded" => {
            reference_id(object.get("subscription")).map(str::to_string)
        }
        _ => object.get("id").and_then(Value::as_str).filter(|id| id.starts_with("sub_")).map(str::to_string),
    };
    let paid = object.get("payment_intent").is_some_and(|p| !p.is_null());
    let mut cancelled = false;
    let mut cancel_failed = false;
    if let (Some(stripe), Some(sub)) = (stripe, subscription.as_deref()) {
        cancelled = crate::services::org_deletion::end_subscription(stripe, sub).await.is_ok();
        cancel_failed = !cancelled;
    }
    tracing::error!(
        alert = true, %account_id, event_id, event_type, reason,
        subscription = subscription.as_deref().unwrap_or(""), cancelled, paid,
        "Stripe webhook: a subscription started for an organization being deleted; not activated (cancelled=false: Stripe is asked to redeliver, or the deletion's stripe_sweep ends it)"
    );
    if cancel_failed {
        return (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": "subscription cancel failed"}))).into_response();
    }
    (StatusCode::OK, Json(json!({"ok": true, "refused": "org_deleting"}))).into_response()
}

/// Refuse a billing event for an account with an `account_deletions` row.
///
/// Nothing is credited and no subscription is started. If money was taken it is
/// refunded (payment intent, deterministic idempotency key so a redelivery
/// refunds once) and a subscription this event created is ended, and the whole
/// thing is an ALERT: it means someone paid a deleted account, which only
/// happens from a Checkout opened before deletion started. Failures to refund
/// are alerts too (manual follow-up, decision 18) and still answer 200 so
/// Stripe does not retry into a loop.
pub(crate) async fn refuse_for_deleted_account(
    stripe: Option<&crate::billing::stripe_client::StripeClient>,
    account_id: Uuid,
    event_id: &str,
    event_type: &str,
    deletion_state: &str,
    object: &Value,
) -> axum::response::Response {
    let payment_intent = reference_id(object.get("payment_intent")).map(str::to_string);
    let subscription = match event_type {
        "invoice.paid" => subscription_id(object).map(str::to_string),
        "checkout.session.completed" => reference_id(object.get("subscription")).map(str::to_string),
        _ => object.get("id").and_then(Value::as_str).filter(|id| id.starts_with("sub_")).map(str::to_string),
    };
    let mut refunded = false;
    let mut cancelled_subscription = false;
    if let Some(stripe) = stripe {
        if let Some(pi) = payment_intent.as_deref() {
            let key = format!("deleted-account-refund:{event_id}");
            refunded = stripe.refund_payment_intent(pi, &key).await.is_ok();
        }
        if let Some(sub) = subscription.as_deref() {
            cancelled_subscription = stripe.cancel_subscription_now_without_proration(sub).await.is_ok();
        }
    }
    tracing::error!(
        alert = true, %account_id, event_id, event_type, deletion_state,
        payment_intent = payment_intent.as_deref().unwrap_or(""),
        subscription = subscription.as_deref().unwrap_or(""),
        refunded, cancelled_subscription,
        "Stripe webhook: a billing event arrived for an account being deleted; refused (manual follow-up if refunded=false and a payment was taken)"
    );
    (StatusCode::OK, Json(json!({"ok": true, "refused": "account_deleted"}))).into_response()
}

/// `payment_intent.succeeded` / `.payment_failed` / `.requires_action` /
/// `.canceled` for an auto-reload PaymentIntent (settings 3.5). See
/// [`crate::billing::extra_usage::reload::settle`] for what each does. A
/// database error answers 500 so Stripe redelivers (the event claim rolls back
/// with the transaction); everything else, including an event that does not
/// match its reload row, is claimed and answered 200.
async fn handle_reload_payment_intent(
    state: &Arc<AppState>,
    control: &sqlx::PgPool,
    account_id: Uuid,
    event_id: &str,
    event_type: &str,
    object: &Value,
) -> axum::response::Response {
    use crate::billing::extra_usage::reload::{settle::apply_event, stripe::PaymentIntentFacts};
    let pi = match PaymentIntentFacts::from_value(object) {
        Ok(p) => p,
        Err(e) => {
            tracing::error!(event_id, event_type, error = %e, "Stripe webhook: payment intent unparseable");
            return (StatusCode::BAD_REQUEST, Json(json!({"error": "invalid payment intent"}))).into_response();
        }
    };
    match apply_event(control, state.stripe.as_deref(), account_id, event_id, event_type, &pi).await {
        Ok(crate::billing::extra_usage::reload::settle::EventOutcome::Replay) => {
            (StatusCode::OK, Json(json!({"ok": true, "replay": true}))).into_response()
        }
        Ok(_) => (StatusCode::OK, Json(json!({"ok": true}))).into_response(),
        Err(e) => {
            tracing::error!(error = %e, event_id, %account_id, "Stripe webhook: auto-reload payment intent failed — asking Stripe to redeliver");
            (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": "processing failed"}))).into_response()
        }
    }
}

/// The `invoice.paid` branch of [`stripe_webhook`] — Phase 4a, only pulled
/// out into its own function so the three-way dispatch above reads as three
/// parallel branches rather than one growing straight line.
///
/// `stripe` (billing self-serve PR F review, finding 7b): threaded through
/// ONLY for the best-effort, POST-COMMIT [`crate::billing::seat_lifecycle::
/// release_overhang`] sweep on a successfully-`Applied` roll — see that call
/// site's own comment for why it runs here (after [`apply_invoice_paid`]'s
/// own transaction has already committed) rather than inside that function.
pub(crate) async fn handle_invoice_paid(
    control: &sqlx::PgPool,
    stripe: Option<&crate::billing::stripe_client::StripeClient>,
    account_id: Uuid,
    event_id: &str,
    event_type: &str,
    customer_id: &str,
    invoice: &Value,
) -> axum::response::Response {
    // A one-off invoice must never roll an allowance period — see
    // `subscription_id`'s doc comment.
    let Some(invoice_subscription_id) = subscription_id(invoice) else {
        tracing::info!(
            event_id,
            customer_id,
            "Stripe webhook: invoice.paid is not a subscription invoice — no period rolled"
        );
        return (StatusCode::OK, Json(json!({"ok": true, "ignored": true}))).into_response();
    };

    // Self-serve PR C′: while a new org is trialing or converting, its
    // invoices belong to the trial lifecycle, not the renewal clock. This
    // runs BEFORE the billing-cycle guard below on purpose: the invoice that
    // ending a trial early produces may carry `subscription_update`, which
    // that guard would drop, and it is the one that pays for the seats.
    match apply_trial_invoice_paid(control, stripe, account_id, event_id, event_type, invoice_subscription_id, invoice)
        .await
    {
        Ok(None) => {}
        Ok(Some(TrialInvoice::Replay)) => {
            return (StatusCode::OK, Json(json!({"ok": true, "replay": true}))).into_response();
        }
        Ok(Some(TrialInvoice::Handled(summary))) => {
            tracing::info!(event_id, %account_id, %summary, "Stripe webhook: invoice.paid handled by the org trial lifecycle");
            return (StatusCode::OK, Json(json!({"ok": true, "trial": summary}))).into_response();
        }
        Err(e) => {
            tracing::error!(error = %e, event_id, %account_id, "Stripe webhook: trial invoice handling failed — asking Stripe to redeliver");
            return (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": "trial invoice failed"}))).into_response();
        }
    }

    // ...and neither must a mid-cycle proration, which IS a subscription
    // invoice and so sails past the guard above. See
    // `rolls_the_renewal_clock` for why (issue #644): its lines carry the
    // moment the seats changed, not the cycle anchor, and storing that
    // silently disables every later seat sync and freeze.
    if !rolls_the_renewal_clock(invoice) {
        tracing::info!(
            event_id,
            customer_id,
            billing_reason = billing_reason(invoice).unwrap_or("<absent>"),
            "Stripe webhook: invoice.paid is not a billing-cycle invoice — no period rolled"
        );
        return (StatusCode::OK, Json(json!({"ok": true, "ignored": true}))).into_response();
    }

    let Some((start_secs, end_secs)) = billing_period(invoice) else {
        tracing::error!(event_id, %account_id, "Stripe webhook: invoice carries no billing period");
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "missing billing period"})),
        )
            .into_response();
    };
    let (Some(period_start), Some(period_end)) = (to_utc(start_secs), to_utc(end_secs)) else {
        tracing::error!(event_id, %account_id, start_secs, end_secs, "Stripe webhook: unrepresentable period");
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "invalid billing period"})),
        )
            .into_response();
    };
    // `0161`'s `member_allowance_period_end_after_start` would reject this at
    // the constraint anyway; catching it here makes the log say what was wrong
    // instead of surfacing a constraint name.
    if period_end <= period_start {
        tracing::error!(
            event_id, %account_id, %period_start, %period_end,
            "Stripe webhook: invoice period does not advance"
        );
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "invalid billing period"})),
        )
            .into_response();
    }

    match apply_invoice_paid(
        control,
        account_id,
        event_id,
        event_type,
        invoice_subscription_id,
        period_start,
        period_end,
    )
    .await
    {
        Ok(None) => {
            tracing::info!(event_id, %account_id, "Stripe webhook: replay — already processed");
            (StatusCode::OK, Json(json!({"ok": true, "replay": true}))).into_response()
        }
        Ok(Some(InvoicePaidOutcome::Stale { incoming_period_start, stored_period_start })) => {
            // Superseded by a period Stripe already told us about more
            // recently — see the module doc's "Out-of-order delivery"
            // section. Never a 500: retrying a stale event can never make it
            // un-stale.
            tracing::warn!(
                event_id, %account_id, %incoming_period_start, %stored_period_start,
                "Stripe webhook: invoice.paid carries an OLDER period than already recorded — \
                 stale/superseded event, no work done"
            );
            (StatusCode::OK, Json(json!({"ok": true, "stale": true}))).into_response()
        }
        Ok(Some(InvoicePaidOutcome::SupersededSubscription { reason })) => {
            // Finding 3 (billing self-serve PR D review round) — see
            // `apply_invoice_paid`'s own doc comment for the two cases this
            // covers (a stale event for a since-superseded subscription id,
            // and the final invoice after a deliberate cancellation). Never
            // a 500: retrying cannot change either fact.
            tracing::info!(
                event_id, %account_id, %reason,
                "Stripe webhook: invoice.paid did not flip status or roll a period"
            );
            (StatusCode::OK, Json(json!({"ok": true, "skipped": true}))).into_response()
        }
        Ok(Some(InvoicePaidOutcome::Applied(outcome))) => {
            if outcome.no_active_plan() {
                // A paid invoice against an account holding no plan grant. The
                // customer's expectation is unmet from this moment, and no
                // amount of redelivery fixes a provisioning gap — so it is
                // recorded and shouted about, not retried.
                tracing::error!(
                    event_id, %account_id, members = outcome.members,
                    "Stripe webhook: invoice paid but the account has NO ACTIVE PLAN GRANT — \
                     no allowance was granted to anyone"
                );
            } else if outcome.seat_mismatch() {
                // Cannot legitimately happen: adding a member IS purchasing a
                // seat. Every member was still granted; this says the member-add
                // path failed to buy one.
                tracing::error!(
                    event_id, %account_id,
                    members = outcome.members, seats = ?outcome.seats,
                    "Stripe webhook: member count and purchased seats disagree — every member was \
                     granted an allowance, but the member-add path failed to purchase a seat"
                );
            } else if billing_reason(invoice) == Some("subscription_create") {
                // The first invoice of a subscription can be delivered BEFORE
                // `checkout.session.completed`, i.e. before the plan grant.
                // The roll then uses the allowance of the grant held at this
                // moment (a Free account's 1,000), not the plan being bought.
                // That is correct, not a stale allowance: the checkout handler
                // closes this period and opens the purchased plan's period
                // (`issue_grant_in_tx`), so exactly one current period remains.
                // Saying "rolled ... per_member=1000" with no context read as a
                // Pro purchase being granted the Free allowance.
                tracing::info!(
                    event_id, %account_id, summary = %outcome.summary(),
                    "Stripe webhook: first-invoice period opened at the allowance of the plan held \
                     right now; checkout.session.completed replaces it with the purchased plan's period"
                );
            } else {
                tracing::info!(
                    event_id, %account_id, summary = %outcome.summary(),
                    "Stripe webhook: allowance period rolled"
                );
            }
            // Finding 7b: best-effort, POST-COMMIT overhang sweep —
            // `apply_invoice_paid`'s own transaction already committed above
            // (this whole match is over its `Ok` result), so this runs
            // entirely outside it, exactly as `release_overhang`'s own doc
            // comment requires. A failure here is logged, never propagated
            // into this response — this sweep reconciling a completely
            // unrelated drift must not turn an otherwise-successful
            // `invoice.paid` into a 500 that asks Stripe to redeliver an
            // event that already fully succeeded.
            if let Err(e) = crate::billing::seat_lifecycle::release_overhang(control, stripe, account_id).await {
                tracing::warn!(
                    error = %e, event_id, %account_id,
                    "Stripe webhook: post-commit seat overhang sweep failed (best-effort, not retried \
                     — the next invoice.paid/subscription.updated will try again)"
                );
            }
            (StatusCode::OK, Json(json!({"ok": true}))).into_response()
        }
        Err(e) => {
            // The work did not happen and the claim rolled back with it, so
            // redelivery is exactly the right outcome. This is the ONE case
            // that deliberately asks Stripe to retry.
            tracing::error!(error = %e, event_id, %account_id, "Stripe webhook: renewal failed — asking Stripe to redeliver");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "renewal failed"})),
            )
                .into_response()
        }
    }
}

/// What [`apply_trial_invoice_paid`] did with an invoice it owned.
#[derive(Debug, PartialEq)]
pub(crate) enum TrialInvoice {
    /// The event was already claimed.
    Replay,
    /// Claimed and handled; the summary is the recorded outcome.
    Handled(String),
}

/// `invoice.paid` for an account with a self-serve trial row. `Ok(None)`
/// (nothing claimed) sends the invoice down the ordinary renewal path; that
/// is every account without a trial row, and the cases below that say so.
///
/// * **`checkout_open` / `abandoned`**: an invoice that arrives BEFORE
///   `checkout.session.completed` (Stripe does not order the two). It is
///   claimed and rolls nothing: there is no grant yet, and the completion
///   that follows issues the grant and opens the right period itself.
/// * **`trialing` / `converting`, the trial's own subscription**:
///   - the opening $0 invoice (`subscription_create`) rolls nothing;
///   - any other invoice is the seats being charged ONLY if it took money
///     (`amount_paid > 0`) or Stripe reports the subscription `active` (read
///     before the transaction). A $0 `subscription_update` while the trial
///     runs (a seat change, say) is not a conversion, and neither is one
///     while the subscription is `past_due`, `incomplete` or `unpaid`
///     (review round 2, finding 5): nothing was paid;
///   - the charge finalizes the conversion for the invoice's period. A
///     `trialing` row first moves to `converting` (reason `backstop`:
///     Stripe ended the trial itself). Whichever of this and the worker runs
///     first converts; the other writes nothing.
///   - an invoice with NO `billing_reason` counts only if it took money.
/// * **`canceled` from `converting`, the trial's own subscription, money
///   taken, and the `stripe_subscriptions` mirror still on that
///   subscription**: the conversion invoice was paid after all. The
///   customer paid, so the allowance is granted and the trial is
///   `converted` (with an operator alert).
/// * **The same, but the mirror is on ANOTHER subscription** (the org
///   resubscribed since; review round 2, finding B): the payment is
///   recorded on the event, an error-level alert asks for a manual refund,
///   and nothing else changes. The live subscription's mirror, periods and
///   allowance are left exactly as they are.
/// * **`canceled`, a DIFFERENT subscription that the mirror names, money
///   taken**: the org resubscribed after the trial ended unpaid. That is its
///   first charge (decision #6: extra usage unlocks), stamped here; the
///   invoice then takes the ordinary renewal path (`Ok(None)`).
async fn apply_trial_invoice_paid(
    control: &sqlx::PgPool,
    stripe: Option<&crate::billing::stripe_client::StripeClient>,
    account_id: Uuid,
    event_id: &str,
    event_type: &str,
    invoice_subscription_id: &str,
    invoice: &Value,
) -> Result<Option<TrialInvoice>, sqlx::Error> {
    use crate::billing::org_trial::{db as trial_db, ConvertReason, TrialState};
    let reason = billing_reason(invoice);
    let took_money = invoice.get("amount_paid").and_then(Value::as_i64).unwrap_or(0) > 0;

    // Peek without the lock, so the one Stripe read this may need runs
    // outside any transaction, and only for a live trial's own $0 invoice.
    let peek = {
        let mut tx = crate::tenancy::scope::begin_account_scoped(control, account_id)
            .await
            .map_err(|e| sqlx::Error::Protocol(e.to_string()))?;
        let row = trial_db::load(&mut tx, account_id).await?;
        tx.commit().await?;
        row
    };
    let Some(peek) = peek else { return Ok(None) };
    let needs_status = !took_money
        && reason.is_some_and(|r| r != "subscription_create")
        && matches!(peek.state, TrialState::Trialing | TrialState::Converting)
        && peek.stripe_subscription_id.as_deref() == Some(invoice_subscription_id);
    // `None` = not read. A $0 invoice counts as the conversion only when it
    // IS the subscription's latest invoice and the S1 rule
    // ([`crate::billing::org_trial::charge_outcome`]) says that invoice was
    // paid. `active` alone is not a payment (#1528 review): voiding a
    // `past_due` subscription's conversion invoice can flip it back to
    // `active`, and a later $0 `subscription_update` invoice (a seat change,
    // say) must not convert the trial on that. Not read, not the latest
    // invoice, or not paid: nothing was paid, and the worker converts on its
    // own reading.
    let invoice_id_seen = invoice.get("id").and_then(Value::as_str).unwrap_or("");
    let (subscription_read, zero_dollar_paid): (Option<crate::billing::stripe_client::SubscriptionView>, bool) =
        match (needs_status, stripe) {
            (true, Some(stripe)) => {
                use crate::billing::seat_lifecycle::with_stripe_deadline;
                let sub = with_stripe_deadline(stripe.retrieve_subscription(invoice_subscription_id))
                    .await
                    .map_err(|e| sqlx::Error::Protocol(format!("trial invoice: retrieve subscription: {e}")))?;
                let paid = match sub.latest_invoice.as_deref() {
                    Some(latest) if !invoice_id_seen.is_empty() && latest == invoice_id_seen => {
                        let latest = with_stripe_deadline(stripe.retrieve_invoice(latest))
                            .await
                            .map_err(|e| sqlx::Error::Protocol(format!("trial invoice: retrieve latest invoice: {e}")))?;
                        crate::billing::org_trial::charge_outcome(Some(&latest.status), &sub.status)
                            == crate::billing::org_trial::ChargeOutcome::Paid
                    }
                    _ => false,
                };
                (Some(sub), paid)
            }
            _ => (None, false),
        };

    let mut tx = crate::tenancy::scope::begin_account_scoped(control, account_id)
        .await
        .map_err(|e| sqlx::Error::Protocol(e.to_string()))?;
    crate::billing::renewal::lock_period_rotation(&mut tx, account_id).await?;
    let Some(row) = trial_db::load_for_update(&mut tx, account_id).await? else {
        tx.rollback().await?;
        return Ok(None);
    };
    let ours = row.stripe_subscription_id.as_deref() == Some(invoice_subscription_id);
    // Finding B: the subscription the org runs on now. The late-paid and
    // first-charge edges are taken only for the invoice of THIS one.
    let mirror_sub: Option<String> =
        sqlx::query_scalar("SELECT stripe_subscription_id FROM stripe_subscriptions WHERE account_id = $1")
            .bind(account_id)
            .fetch_optional(&mut *tx)
            .await?
            .flatten();
    let mirrored = mirror_sub.as_deref() == Some(invoice_subscription_id);

    enum Act {
        BeforeCheckout,
        Live,
        LatePaid,
        /// A late-paid conversion invoice for a subscription the org has
        /// since replaced: record, alert for a refund, change nothing.
        LatePaidSuperseded,
        FirstChargeAfterTrial,
    }
    let act = match row.state {
        TrialState::CheckoutOpen | TrialState::Abandoned if row.stripe_subscription_id.is_none() || ours => {
            Act::BeforeCheckout
        }
        TrialState::Trialing | TrialState::Converting if ours => Act::Live,
        TrialState::Canceled
            if ours && row.convert_reason.is_some() && took_money && reason != Some("subscription_create") =>
        {
            if mirrored {
                Act::LatePaid
            } else {
                Act::LatePaidSuperseded
            }
        }
        TrialState::Canceled if !ours && mirrored && took_money && row.first_charge_at.is_none() => {
            Act::FirstChargeAfterTrial
        }
        _ => {
            tx.rollback().await?;
            return Ok(None);
        }
    };
    if let Act::FirstChargeAfterTrial = act {
        trial_db::stamp_first_charge(&mut tx, account_id).await?;
        tx.commit().await?;
        tracing::info!(event_id, %account_id, "org trial: first charge after the trial ended unpaid; extra usage unlocked");
        return Ok(None);
    }

    let claimed = sqlx::query(
        "INSERT INTO stripe_webhook_events (event_id, event_type, account_id) \
         VALUES ($1, $2, $3) ON CONFLICT (event_id) DO NOTHING",
    )
    .bind(event_id)
    .bind(event_type)
    .bind(account_id)
    .execute(&mut *tx)
    .await?
    .rows_affected();
    if claimed == 0 {
        tx.rollback().await?;
        return Ok(Some(TrialInvoice::Replay));
    }

    let is_charge = match act {
        Act::BeforeCheckout | Act::LatePaidSuperseded => false,
        Act::LatePaid => true,
        Act::Live => match reason {
            Some("subscription_create") => false,
            Some(_) => took_money || zero_dollar_paid,
            None => took_money,
        },
        Act::FirstChargeAfterTrial => unreachable!("returned above"),
    };
    let invoice_id = invoice.get("id").and_then(Value::as_str).unwrap_or("");
    let amount_paid = invoice.get("amount_paid").and_then(Value::as_i64).unwrap_or(0);
    let summary = if let Act::LatePaidSuperseded = act {
        tracing::error!(
            alert = true, event_id, %account_id, invoice = invoice_id, amount_paid,
            subscription = invoice_subscription_id, live = ?mirror_sub,
            "org trial: a canceled trial's conversion invoice was paid after the org resubscribed; \
             refund it manually. The live subscription is left untouched"
        );
        format!(
            "trial_late_paid_superseded refund_required invoice={invoice_id} amount_paid={amount_paid} \
             subscription={invoice_subscription_id} live={}",
            mirror_sub.as_deref().unwrap_or("none")
        )
    } else if let Act::BeforeCheckout = act {
        format!("trial_invoice_before_checkout no_roll state={}", row.state.as_str())
    } else if !is_charge {
        if reason == Some("subscription_create") {
            format!("trial_opening_invoice no_roll state={}", row.state.as_str())
        } else {
            format!("trial_zero_invoice no_roll state={}", row.state.as_str())
        }
    } else {
        // #1528 N5: a $0 invoice converts only on the subscription read
        // just now (it must be that subscription's latest invoice, paid). Such an invoice may be stale (an
        // older `subscription_update` delivered late), and its lines then
        // name a period that is not the one Stripe is billing. Finalize on
        // the period of the subscription that was read; a charge that took
        // money keeps its own invoice's period.
        let read_period = if took_money {
            None
        } else {
            subscription_read
                .as_ref()
                .and_then(|s| s.items.first())
                .map(|i| (i.current_period_start, i.current_period_end))
        };
        let Some((start_secs, end_secs)) = read_period.or_else(|| billing_period(invoice)) else {
            return Err(sqlx::Error::Protocol("trial conversion invoice carries no billing period".into()));
        };
        let (Some(period_start), Some(period_end)) = (to_utc(start_secs), to_utc(end_secs)) else {
            return Err(sqlx::Error::Protocol("trial conversion invoice period is unrepresentable".into()));
        };
        if row.state == TrialState::Trialing {
            trial_db::begin_conversion(&mut tx, account_id, ConvertReason::Backstop).await?;
        }
        match trial_db::finalize_conversion(&mut tx, account_id, invoice_subscription_id, period_start, period_end)
            .await?
        {
            trial_db::FinalizeOutcome::Converted { members, expired, refund_required } => {
                let late = if let Act::LatePaid = act { " after_cancel=true" } else { "" };
                // #1528 N4: paid on a subscription that is already canceled.
                let refund = if refund_required {
                    format!(" refund_required invoice={invoice_id} amount_paid={amount_paid}")
                } else {
                    String::new()
                };
                format!("trial_converted members={members} expired_credits={expired}{late}{refund}")
            }
            trial_db::FinalizeOutcome::NotConverting(state) => {
                format!("trial_not_converting state={:?}", state.map(TrialState::as_str))
            }
            trial_db::FinalizeOutcome::OrgDeleted => {
                tracing::error!(alert = true, event_id, %account_id, invoice = invoice_id, amount_paid,
                    "org trial: conversion invoice paid for an organization deleted in Clerk; not converted, refund it manually");
                format!("trial_org_deleted refund_required invoice={invoice_id} amount_paid={amount_paid}")
            }
            trial_db::FinalizeOutcome::Superseded { live } => {
                // The mirror moved between the read above and finalize's own
                // check (both under this lock, so not expected).
                tracing::error!(
                    alert = true, event_id, %account_id, invoice = invoice_id, amount_paid, ?live,
                    "org trial: a canceled trial's conversion invoice was paid after the org resubscribed; refund it manually"
                );
                format!(
                    "trial_late_paid_superseded refund_required invoice={invoice_id} amount_paid={amount_paid} \
                     subscription={invoice_subscription_id} live={}",
                    live.as_deref().unwrap_or("none")
                )
            }
        }
    };
    sqlx::query("UPDATE stripe_webhook_events SET outcome = $2 WHERE event_id = $1")
        .bind(event_id)
        .bind(&summary)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(Some(TrialInvoice::Handled(summary)))
}

/// The `customer.subscription.updated` branch of [`stripe_webhook`] — Phase
/// 4b, seat sync + freeze. See the module doc's "Two facts" section for why
/// the seat count and the billing period are each read defensively.
///
/// `stripe` (billing self-serve PR F review, finding 7b): same purpose as
/// [`handle_invoice_paid`]'s own parameter — the best-effort, POST-COMMIT
/// [`crate::billing::seat_lifecycle::release_overhang`] sweep on a
/// successfully-`Applied` update.
async fn handle_subscription_updated(
    control: &sqlx::PgPool,
    stripe: Option<&crate::billing::stripe_client::StripeClient>,
    account_id: Uuid,
    event_id: &str,
    event_type: &str,
    sub: &Value,
) -> axum::response::Response {
    let Some(seats) = subscription_seats(sub) else {
        tracing::error!(
            event_id, %account_id,
            "Stripe webhook: subscription.updated carries no resolvable seat quantity \
             (checked both the item and the subscription object)"
        );
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "missing seat quantity"})),
        )
            .into_response();
    };
    let Ok(seats) = i32::try_from(seats) else {
        tracing::error!(event_id, %account_id, seats, "Stripe webhook: seat quantity out of range");
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "invalid seat quantity"})),
        )
            .into_response();
    };
    // `grants.seats CHECK (seats IS NULL OR seats >= 1)` (`0156`) rejects a
    // zero or negative quantity as a `23514` database error, which
    // `apply_subscription_updated`'s caller (like every other DB failure in
    // this handler) turns into a 500 that asks Stripe to redeliver — and
    // Stripe genuinely sends `quantity: 0` (a subscription paused at zero
    // seats, or a race with a cancellation), so that 500 would retry forever
    // against an input that can never succeed. `i32::try_from` above already
    // catches an out-of-range value; this catches the in-range-but-illegal
    // one, at the same boundary and with the same error shape.
    if seats < 1 {
        tracing::error!(event_id, %account_id, seats, "Stripe webhook: seat quantity must be at least 1");
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "invalid seat quantity"})),
        )
            .into_response();
    }

    let Some(status_raw) = subscription_status(sub) else {
        tracing::error!(event_id, %account_id, "Stripe webhook: subscription.updated carries no status");
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "missing status"})),
        )
            .into_response();
    };
    let status = map_subscription_status(status_raw);

    // The billing period lives on the ITEM, not the subscription object —
    // see the module doc's "Two facts" section. An unresolvable period is a
    // LOUD error, never a quiet skip: reading the wrong (nonexistent) field
    // would silently do nothing while looking identical to success.
    let Some((start_secs, end_secs)) = subscription_period(sub) else {
        tracing::error!(
            event_id, %account_id, status_raw,
            "Stripe webhook: subscription.updated carries no resolvable billing period — \
             checked items.data[0] and the subscription object itself, found neither"
        );
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "missing billing period"})),
        )
            .into_response();
    };
    let (Some(period_start), Some(period_end)) = (to_utc(start_secs), to_utc(end_secs)) else {
        tracing::error!(event_id, %account_id, start_secs, end_secs, "Stripe webhook: unrepresentable subscription period");
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "invalid billing period"})),
        )
            .into_response();
    };
    if period_end <= period_start {
        tracing::error!(
            event_id, %account_id, %period_start, %period_end,
            "Stripe webhook: subscription period does not advance"
        );
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "invalid billing period"})),
        )
            .into_response();
    }

    // Cancellation fields. The payload's own copy can arrive out of order
    // (cancel then un-cancel inside one period leaves the period guard unable
    // to order them), so when Stripe is reachable the fields are re-read from
    // the live subscription, which is order-independent. The payload copy is
    // the fallback.
    let (mut cancel_at_period_end, mut cancel_at_secs) =
        crate::billing::stripe_client::parse_cancel_state(sub);
    let cancel = match sub.get("id").and_then(Value::as_str) {
        Some(sub_id) => {
            if let Some(stripe) = stripe {
                match crate::billing::seat_lifecycle::with_stripe_deadline(stripe.retrieve_subscription(sub_id))
                    .await
                {
                    Ok(live) => {
                        cancel_at_period_end = live.cancel_at_period_end;
                        cancel_at_secs = live.cancel_at;
                    }
                    Err(e) => tracing::warn!(
                        error = %e, event_id, %account_id,
                        "Stripe webhook: live subscription read failed, using the event's cancellation fields"
                    ),
                }
            }
            Some(CancelMirror {
                subscription_id: sub_id.to_string(),
                cancel_at_period_end,
                cancel_at: cancel_at_secs.and_then(to_utc),
            })
        }
        None => None,
    };

    match apply_subscription_updated_with_cancel(
        control,
        account_id,
        event_id,
        event_type,
        seats,
        status,
        period_start,
        period_end,
        cancel.as_ref(),
    )
    .await
    {
        Ok(None) => {
            tracing::info!(event_id, %account_id, "Stripe webhook: replay — already processed");
            (StatusCode::OK, Json(json!({"ok": true, "replay": true}))).into_response()
        }
        Ok(Some(ClaimedOutcome::Stale { incoming_period_start, stored_period_start })) => {
            // See the module doc's "Out-of-order delivery" section and
            // `handle_invoice_paid`'s identical arm.
            tracing::warn!(
                event_id, %account_id, %incoming_period_start, %stored_period_start,
                "Stripe webhook: subscription.updated carries an OLDER period than already \
                 recorded — stale/superseded event, no work done"
            );
            (StatusCode::OK, Json(json!({"ok": true, "stale": true}))).into_response()
        }
        Ok(Some(ClaimedOutcome::Applied(outcome))) => {
            tracing::info!(
                event_id, %account_id, summary = %outcome.summary(),
                "Stripe webhook: subscription updated"
            );
            // Finding 7b: same best-effort, POST-COMMIT overhang sweep as
            // `handle_invoice_paid` — see that call site's own comment.
            if let Err(e) = crate::billing::seat_lifecycle::release_overhang(control, stripe, account_id).await {
                tracing::warn!(
                    error = %e, event_id, %account_id,
                    "Stripe webhook: post-commit seat overhang sweep failed (best-effort, not retried \
                     — the next invoice.paid/subscription.updated will try again)"
                );
            }
            (StatusCode::OK, Json(json!({"ok": true}))).into_response()
        }
        Err(e) => {
            tracing::error!(error = %e, event_id, %account_id, "Stripe webhook: subscription update failed — asking Stripe to redeliver");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "subscription update failed"})),
            )
                .into_response()
        }
    }
}

/// The `customer.subscription.deleted` branch of [`stripe_webhook`] —
/// freeze-not-forfeit (plan §1.11). No period bounds are needed here: this
/// only flips `stripe_subscriptions.status`; see [`apply_subscription_deleted`]'s
/// own doc comment for why nothing else is touched regardless of
/// `cancellation_details.reason`.
async fn handle_subscription_deleted(
    control: &sqlx::PgPool,
    account_id: Uuid,
    event_id: &str,
    event_type: &str,
    sub: &Value,
) -> axum::response::Response {
    let reason = cancellation_reason(sub);
    // The deleted Subscription object's OWN id — `data.object.id`, not a
    // nested reference field (see finding 6, billing self-serve PR D review
    // round). A `customer.subscription.deleted` event's `data.object` IS the
    // Subscription, so this is a plain top-level string, unlike
    // `subscription_id(invoice)`'s expand-or-inline handling for an
    // INVOICE's reference to a DIFFERENT object.
    let Some(deleted_subscription_id) = sub.get("id").and_then(Value::as_str) else {
        tracing::error!(event_id, %account_id, "Stripe webhook: subscription.deleted carries no subscription id");
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "missing subscription id"})),
        )
            .into_response();
    };

    let (cancel_at_period_end, cancel_at_secs) = crate::billing::stripe_client::parse_cancel_state(sub);
    let cancel = CancelMirror {
        subscription_id: deleted_subscription_id.to_string(),
        cancel_at_period_end,
        cancel_at: cancel_at_secs.and_then(to_utc),
    };
    match apply_subscription_deleted_with_cancel(
        control,
        account_id,
        event_id,
        event_type,
        deleted_subscription_id,
        reason,
        Some(&cancel),
    )
    .await
    {
        Ok(None) => {
            tracing::info!(event_id, %account_id, "Stripe webhook: replay — already processed");
            (StatusCode::OK, Json(json!({"ok": true, "replay": true}))).into_response()
        }
        Ok(Some(outcome @ SubscriptionDeleteOutcome::Frozen { .. })) => {
            tracing::warn!(
                event_id, %account_id, summary = %outcome.summary(),
                "Stripe webhook: subscription deleted — frozen (purchased credits survive, \
                 unspendable until resubscribe)"
            );
            (StatusCode::OK, Json(json!({"ok": true}))).into_response()
        }
        Ok(Some(outcome @ SubscriptionDeleteOutcome::Superseded { .. })) => {
            // Finding 6 — a late deletion for a subscription this account
            // has already moved past (cancel-then-resubscribe). Never a
            // 500: retrying cannot make the event un-superseded.
            tracing::info!(
                event_id, %account_id, summary = %outcome.summary(),
                "Stripe webhook: subscription deleted — superseded, no freeze applied"
            );
            (StatusCode::OK, Json(json!({"ok": true, "superseded": true}))).into_response()
        }
        Err(e) => {
            tracing::error!(error = %e, event_id, %account_id, "Stripe webhook: subscription deletion failed — asking Stripe to redeliver");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "subscription deletion failed"})),
            )
                .into_response()
        }
    }
}

// ── `checkout.session.completed` (billing self-serve PR D, plan §2.D) ───────

/// **Accept-and-record posture for a PERMANENTLY-unprocessable session**
/// (findings 8 + 9, review round): claims the event (or reports an ordinary
/// replay if it is already claimed), records `reason` durably in
/// `stripe_webhook_events.outcome`, logs a loud `tracing::error!`, and
/// answers **200** — never the 400 an earlier version of this handler used
/// to return for these exact cases.
///
/// # Why 200, not 400 — and why this is NOT the same as the module's
/// # established malformed-payload 400s
///
/// The module doc's "Why almost everything answers 200" section states the
/// rule this applies: a non-2xx tells Stripe to redeliver, with backoff, for
/// DAYS, which is only useful when redelivering could plausibly succeed.
/// `mode = "setup"`, zero line items, an unrecognized checkout mode, and
/// `line_items.len() != 1` on a subscription-mode session are all facts
/// about the SESSION's shape that redelivering the SAME event can never
/// change — Stripe will keep re-sending the identical payload until it gives
/// up, at which point the event silently drops with nobody ever having been
/// credited AND no durable record explaining why. Contrast this with the
/// module's `unparseable line items` / `missing session id` / `missing
/// subscription id` 400s, which stay 400: those are wire-format
/// disagreements (this handler and Stripe no longer agree on the payload
/// shape), the same class the module doc's malformed-payload exception
/// already covers, and they happen BEFORE any claim exists to record an
/// outcome into. This function's cases, by contrast, are well-formed
/// payloads whose CONTENT this system has no way to price or act on — they
/// get claimed and recorded, not rejected.
async fn claim_and_record_unprocessable(
    control: &sqlx::PgPool,
    account_id: Uuid,
    event_id: &str,
    event_type: &str,
    reason: &str,
) -> axum::response::Response {
    let mut tx = match crate::tenancy::scope::begin_account_scoped(control, account_id).await {
        Ok(tx) => tx,
        Err(e) => {
            tracing::error!(error = %e, event_id, %account_id, "Stripe webhook: unprocessable-session scope failed");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "scope failed"})),
            )
                .into_response();
        }
    };
    if let Err(e) = crate::billing::renewal::lock_period_rotation(&mut tx, account_id).await {
        tracing::error!(error=%e,event_id,%account_id,"Stripe financial entry failed");
        return (StatusCode::INTERNAL_SERVER_ERROR,Json(json!({"error":"financial entry failed"}))).into_response();
    }
    let claimed = match sqlx::query(
        "INSERT INTO stripe_webhook_events (event_id, event_type, account_id) \
         VALUES ($1, $2, $3) ON CONFLICT (event_id) DO NOTHING",
    )
    .bind(event_id)
    .bind(event_type)
    .bind(account_id)
    .execute(&mut *tx)
    .await
    {
        Ok(r) => r.rows_affected(),
        Err(e) => {
            tracing::error!(error = %e, event_id, %account_id, "Stripe webhook: unprocessable-session claim failed");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "claim failed"})),
            )
                .into_response();
        }
    };
    if claimed == 0 {
        let _ = tx.rollback().await;
        tracing::info!(event_id, %account_id, "Stripe webhook: replay — already processed");
        return (StatusCode::OK, Json(json!({"ok": true, "replay": true}))).into_response();
    }

    tracing::error!(alert = true, event_id, %account_id, reason, "Stripe webhook: checkout session is permanently unprocessable — claimed and recorded, not retried");
    if let Err(e) = sqlx::query("UPDATE stripe_webhook_events SET outcome = $2 WHERE event_id = $1")
        .bind(event_id)
        .bind(format!("unprocessable: {reason}"))
        .execute(&mut *tx)
        .await
    {
        tracing::error!(error = %e, event_id, %account_id, "Stripe webhook: unprocessable-session outcome record failed — asking Stripe to redeliver");
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": "outcome record failed"})),
        )
            .into_response();
    }
    if let Err(e) = tx.commit().await {
        tracing::error!(error = %e, event_id, %account_id, "Stripe webhook: unprocessable-session commit failed — asking Stripe to redeliver");
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": "commit failed"})),
        )
            .into_response();
    }
    (StatusCode::OK, Json(json!({"ok": true, "unprocessable": true}))).into_response()
}

/// The `checkout.session.completed` branch of [`stripe_webhook`] — the
/// completion side of `routes::checkout`'s outbound checkout routes. Unlike
/// the other three handlers, this one legitimately needs `state.stripe`
/// (`&Arc<AppState>`, not just `control`): Stripe's `checkout.session
/// .completed` event payload carries no `line_items` at all — the session
/// must be RE-FETCHED with `expand[]=line_items`
/// ([`crate::billing::stripe_client::StripeClient::retrieve_checkout_session`])
/// before either mode below can determine what was purchased. This is the
/// concrete mechanism behind plan §2.D's "the current `items.data[0]`
/// single-item assumption must die": every entry of the re-fetched
/// `line_items.data` array is parsed
/// ([`crate::billing::stripe_client::parse_checkout_session_line_items`]),
/// never just the first.
///
/// # No transaction open across the re-fetch (findings 1 + 8 + 9, review
/// # round)
///
/// The session re-fetch is an OUTBOUND Stripe call and runs here, in the
/// dispatcher, BEFORE either mode-specific function opens ANY database
/// transaction — never inside one. Both mode-specific functions now claim
/// the event and do their entire remaining unit of work (grant issuance,
/// mirror update, ledger credit) in ONE transaction each, exactly like
/// [`apply_invoice_paid`]/[`apply_subscription_updated`]/
/// [`apply_subscription_deleted`] — see [`apply_checkout_completed_subscription`]'s
/// own doc comment for how it now achieves this despite ALSO needing a
/// SECOND outbound call (`retrieve_subscription`) of its own.
async fn handle_checkout_completed(
    state: &Arc<AppState>,
    control: &sqlx::PgPool,
    account_id: Uuid,
    event_id: &str,
    event_type: &str,
    session: &Value,
) -> axum::response::Response {
    let Some(session_id) = session.get("id").and_then(Value::as_str) else {
        tracing::error!(event_id, %account_id, "Stripe webhook: checkout.session.completed carries no session id");
        return (StatusCode::BAD_REQUEST, Json(json!({"error": "missing session id"}))).into_response();
    };
    let mode = session.get("mode").and_then(Value::as_str).unwrap_or("");
    // Only a payment-mode session can pay late; ignore the async event for any
    // other mode rather than re-running its completion handler.
    if event_type == "checkout.session.async_payment_succeeded" && mode != "payment" {
        return (StatusCode::OK, Json(json!({"ok": true, "ignored": true}))).into_response();
    }

    // `state.stripe == None` here is a genuine internal misconfiguration on
    // a path that has ALREADY charged the customer — this is not the outbound
    // "would charge nothing, so 503 is honest" posture
    // `billing::stripe_client`'s module doc describes for a route that is
    // ABOUT to charge; the charge already happened in Stripe. A 500 (ask
    // Stripe to redeliver once `STRIPE_SECRET_KEY` is set) is the correct
    // answer, not a silent 200 that would leave the purchase forever
    // ungranted. TRANSIENT — no claim has happened yet, so a plain retry
    // once the misconfiguration is fixed reprocesses cleanly.
    let Some(stripe) = state.stripe.as_ref() else {
        tracing::error!(
            event_id, %account_id, session_id,
            "Stripe webhook: checkout.session.completed but no outbound Stripe client is \
             configured — cannot re-fetch line items; asking Stripe to redeliver"
        );
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": "stripe not configured"})),
        )
            .into_response();
    };

    let expanded = match stripe.retrieve_checkout_session(session_id).await {
        Ok(v) => v,
        Err(e) => {
            // TRANSIENT (a Stripe API/network failure) — no claim yet, ask
            // Stripe to redeliver.
            tracing::error!(error = %e, event_id, %account_id, session_id, "Stripe webhook: failed to re-fetch checkout session for line items — asking Stripe to redeliver");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "session re-fetch failed"})),
            )
                .into_response();
        }
    };
    let line_items = match crate::billing::stripe_client::parse_checkout_session_line_items(&expanded) {
        Ok(items) => items,
        Err(e) => {
            // A wire-format disagreement (this handler and Stripe no longer
            // agree on the expanded session's shape) — the module doc's
            // malformed-payload exception, unchanged by findings 8/9. No
            // claim yet.
            tracing::error!(error = %e, event_id, %account_id, session_id, "Stripe webhook: checkout session line items unparseable");
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": "unparseable line items"})),
            )
                .into_response();
        }
    };

    match mode {
        "subscription" => {
            let Some(subscription_id) = reference_id(session.get("subscription")) else {
                // A subscription-mode session with no subscription id is a
                // wire-format disagreement (Stripe always attaches one to a
                // completed subscription-mode session) — malformed payload,
                // stays 400, no claim.
                tracing::error!(event_id, %account_id, session_id, "Stripe webhook: subscription-mode checkout session carries no subscription id");
                return (
                    StatusCode::BAD_REQUEST,
                    Json(json!({"error": "missing subscription id"})),
                )
                    .into_response();
            };
            // Finding 8: this handler prices off EXACTLY one line item.
            // `routes::checkout`'s own subscription checkout route only ever
            // creates a single-price session (plan §1.6: checkout buys
            // exactly ONE seat) until PR F, so zero or multiple items is a
            // shape this system does not know how to price — permanently
            // unprocessable (finding 9's posture), not a silent
            // first-item-only under-grant.
            //
            // The trial Checkout's free one-time welcome-offer line is not a
            // subscription item (Stripe puts it on the first invoice), so it
            // is left out of the count. Any other extra line, a PAID one-time
            // item included, is still refused.
            let priced = crate::billing::stripe_client::parse_checkout_session_priced_line_items(&expanded)
                .unwrap_or_else(|_| line_items.clone());
            let [line_item] = priced.as_slice() else {
                return claim_and_record_unprocessable(
                    control,
                    account_id,
                    event_id,
                    event_type,
                    &format!(
                        "subscription-mode checkout session carries {} line items (expected \
                         exactly 1 — our own checkout route always creates single-item \
                         sessions until PR F)",
                        priced.len()
                    ),
                )
                .await;
            };
            apply_checkout_completed_subscription(
                state,
                control,
                account_id,
                event_id,
                session_id,
                subscription_id,
                line_item,
            )
            .await
        }
        "payment" => {
            if line_items.is_empty() {
                return claim_and_record_unprocessable(
                    control,
                    account_id,
                    event_id,
                    event_type,
                    "payment-mode checkout session carries zero line items",
                )
                .await;
            }
            // The re-fetched session is the authority for what was paid and
            // what it was for. A top-up carries `metadata[purpose]`; anything
            // else is a credit pack (including sessions created before 3.4).
            use crate::billing::extra_usage::webhook::{apply_pack_purchase, apply_topup, PaymentFacts};
            let facts = PaymentFacts::from_session(&expanded);
            let result = if facts.is_topup() {
                apply_topup(control, account_id, event_id, event_type, session_id, &facts).await
            } else {
                apply_pack_purchase(control, account_id, event_id, event_type, session_id, &facts, &line_items)
                    .await
            };
            payment_response(account_id, event_id, result)
        }
        other => {
            // `mode = "setup"` (no line items ever) and any future mode this
            // build has never seen both land here — permanently
            // unprocessable, not a redeliverable failure.
            claim_and_record_unprocessable(
                control,
                account_id,
                event_id,
                event_type,
                &format!("unrecognized checkout mode {other:?}"),
            )
            .await
        }
    }
}

/// Subscription-mode `checkout.session.completed` — activates the purchased
/// plan/seat-tier grant and mirrors the new subscription onto
/// `stripe_subscriptions`. `line_item` is guaranteed by the caller
/// ([`handle_checkout_completed`]) to be the SOLE line item on this session —
/// finding 8 (billing self-serve PR D review round) moved the
/// "not exactly one item" refusal up into the dispatcher, via
/// [`claim_and_record_unprocessable`], so this function never has to choose
/// which item to price or silently under-grant against the first one.
///
/// # Claim, then compose in ONE transaction (findings 1 + 2, review round)
///
/// An earlier version of this function claimed the event in its OWN,
/// separately-committed transaction, then did the grant issuance and mirror
/// update AFTERWARD, outside any transaction — reasoning that `issue_grant`
/// managed its own begin/commit and the outbound `retrieve_subscription`
/// call could never run inside one. That composition had a real hole
/// (finding 1, CRITICAL): if ANYTHING after the claim committed failed —
/// `issue_grant` erroring, the `stripe_subscriptions` mirror write failing —
/// the handler answered 500 (correctly asking Stripe to redeliver), but the
/// claim was ALREADY committed, so the redelivery hit `claimed == 0`,
/// answered 200 "replay", and the grant work never ran at all. The purchase
/// was gone for good, permanently, with no further retry ever attempted.
///
/// This version restructures around the ONE constraint that is genuinely
/// immovable — an outbound Stripe call must never run inside an open DB
/// transaction — by moving EVERYTHING that does not need a transaction (the
/// `retrieve_subscription` call, and the plan/seat-tier price lookups, which
/// read `plans`/`seat_tiers`, both GLOBAL reference-data tables with NO RLS
/// policy — see `0150`/`0165`'s own headers — so these reads need no scope
/// at all) BEFORE opening any transaction, and then doing the claim AND
/// every remaining write — grant issuance, the seat-tier bookkeeping, the
/// `stripe_subscriptions` mirror, the recorded outcome — in ONE
/// account-scoped transaction, exactly like the other three handlers. Grant
/// issuance itself runs through
/// [`crate::billing::issuance::issue_grant_in_tx`] (finding 2's other half)
/// — the transaction-composable core the pool-taking `issue_grant` now
/// delegates to — rather than `issue_grant` itself, which opens and commits
/// its OWN transaction: exactly what this function can no longer afford now
/// that its own transaction already holds the event claim, since
/// committing/rolling back `issue_grant`'s inner transaction would be a
/// no-op on the outer one, silently decoupling the grant from the claim it
/// is supposed to share a commit boundary with.
///
/// # `plans`/`seat_tiers` reads need no scope, but `grants`/
/// # `grant_seat_tiers` do (finding 2)
///
/// `plans` and `seat_tiers` carry no RLS policy at all (shared reference
/// data, not per-account state — see their own migration headers), so the
/// price lookups below run as plain reads against `control`. `grants` and
/// `grant_seat_tiers`, by contrast, ARE RLS-covered (`0150`/`0168`), so
/// EVERY read or write against either — the re-read of the just-issued team
/// grant's id, and the `grant_seat_tiers` INSERT itself — now runs inside
/// the SAME account-scoped transaction the claim and the grant issuance
/// already share, never against the bare `control` pool. An earlier version
/// ran both of those unscoped: under the production `coralswarm_app` role
/// (not the `BYPASSRLS` role every pg-test in this tree runs as), an
/// unscoped read/write against an RLS-covered table does not silently
/// return/affect zero rows — `current_setting('app.account_id')` reads back
/// as `''` outside a scoped transaction, and `''::uuid` fails the cast, so
/// the statement HARD-ERRORS (see
/// `billing::enforcement::read_balance_scoped`'s doc comment for the
/// identical mechanism). Owner-role tests never caught this because
/// `BYPASSRLS` skips policy evaluation — and therefore the cast that trips
/// it — entirely.
///
/// # `grant_seat_tiers` bookkeeping is now a HARD error, not warn-and-continue
///
/// A previous version treated a failure to re-read the just-issued grant's
/// id, or to write its `grant_seat_tiers` row, as best-effort: log a
/// warning and move on, reasoning that "the grant already issued" and PR E
/// (not this PR) is what actually reads this table. That reasoning no
/// longer holds now that finding 1's fix makes redelivery safe: this
/// function's ENTIRE unit of work — including this bookkeeping — now shares
/// one commit boundary with the claim, so a failure here propagates as an
/// ordinary transient error (500, no commit, the whole transaction —
/// including the claim — rolls back) and Stripe's redelivery retries the
/// WHOLE thing cleanly, exactly like any other write in this transaction.
/// Swallowing it would leave a `team` grant permanently missing its
/// `grant_seat_tiers` row with no way for a retry to ever fix it, once the
/// surrounding claim has committed.
///
/// # Seat-ledger seed, and why the purchaser is `accounts.owner_user_id`
/// (billing self-serve PR F)
///
/// A team seat-tier line item ALSO seeds the purchase ledger: one
/// `grant_seat_tiers` row at the purchased tier, `grants.seats` resynced to
/// match, and the purchaser's `account_members.seat_purchased_at` stamped —
/// see the `Granted` match arm below for the full write sequence. "The
/// purchaser" is resolved as `accounts.owner_user_id`: Stripe Checkout's
/// subscription mode carries no per-user identity through to this webhook
/// (`routes::checkout::create_subscription_checkout` sets
/// `client_reference_id`/`metadata[account_id]` only), so the account owner
/// is the only user this webhook can name unambiguously as "who bought
/// this". An admin who is not the owner completing checkout on the org's
/// behalf is not distinguishable from here — accepted as a known
/// simplification, not a bug: `billing::seat_lifecycle::acquire_seat`
/// purchases every OTHER member's seat correctly regardless of who paid for
/// the first one.
///
/// # Which plan gets granted
///
/// The line item's price is matched against `plans.stripe_price_id` (a flat
/// plan, e.g. `pro`) and then `seat_tiers.stripe_price_id` (a team seat
/// purchase).
///
/// **A seat-tier match resolves the GRANT through the `team` plan row**,
/// which `0167_plans_team_remodel.sql` originally seeded `is_active = false`
/// DELIBERATELY, "until PR E flips it once team grants price correctly" (see
/// that migration's own header) — a known, accepted sequencing gap between
/// PR D and PR E (there was no UI, PR H, yet to reach this route from, so it
/// carried no live blast radius at the time). PR E's own
/// `0170_team_plan_activate.sql` has since flipped that flag, now that
/// `billing::renewal`/`billing::entitlement` price a `team` grant correctly
/// off `grant_seat_tiers` rather than `plans.credits` — so this branch finds
/// an active plan and grants normally, exactly like the flat-plan branch
/// above it. If `team` is ever deactivated again (e.g. a future migration),
/// this branch still degrades the same way [`apply_invoice_paid`] does for
/// any other grantless account: the Stripe subscription and the
/// `stripe_subscriptions` mirror still get created/updated correctly (the
/// customer really is charged and really is linked), only the
/// CoralSwarm-side grant is deferred, logged as the same loud `no active
/// plan` condition.
pub(crate) async fn apply_checkout_completed_subscription(
    state: &Arc<AppState>,
    control: &sqlx::PgPool,
    account_id: Uuid,
    event_id: &str,
    session_id: &str,
    subscription_id: &str,
    line_item: &crate::billing::stripe_client::SessionLineItem,
) -> axum::response::Response {
    // Not a parameter (unlike the other `apply_*` handlers, which are shared
    // across more than one possible caller-supplied value) — this function
    // is reachable from exactly one event type, so hardcoding it here is
    // what gets this function's argument count under clippy's
    // `too_many_arguments` threshold without inventing a param-bundling
    // struct no other call site in this file uses.
    const EVENT_TYPE: &str = "checkout.session.completed";
    let price_id = line_item.price_id.as_str();

    // ── Everything that does NOT need a transaction, first (findings 1 + 2)
    // — the outbound Stripe call, and the `plans`/`seat_tiers` reference-data
    // reads (neither table is RLS-covered; see this function's own doc
    // comment). ──
    let Some(stripe) = state.stripe.as_ref() else {
        // Unreachable in practice — `handle_checkout_completed` already
        // refused the request above when `state.stripe` was `None`, and
        // nothing between there and here can unset it. A defensive
        // TRANSIENT failure (500, ask Stripe to redeliver) rather than a
        // panic on an invariant this function cannot itself prove.
        tracing::error!(event_id, %account_id, "Stripe webhook: stripe client unexpectedly absent");
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": "stripe not configured"})),
        )
            .into_response();
    };
    let sub = match stripe.retrieve_subscription(subscription_id).await {
        Ok(s) => s,
        Err(e) => {
            tracing::error!(error = %e, event_id, %account_id, subscription_id, "Stripe webhook: failed to retrieve the new subscription — asking Stripe to redeliver");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "subscription retrieve failed"})),
            )
                .into_response();
        }
    };
    let Some(first_item) = sub.items.first() else {
        tracing::error!(event_id, %account_id, subscription_id, "Stripe webhook: new subscription carries no items — cannot resolve its billing period");
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": "subscription has no items"})),
        )
            .into_response();
    };
    let (Some(period_start), Some(period_end)) =
        (to_utc(first_item.current_period_start), to_utc(first_item.current_period_end))
    else {
        tracing::error!(event_id, %account_id, subscription_id, "Stripe webhook: new subscription's period is unrepresentable");
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": "invalid subscription period"})),
        )
            .into_response();
    };
    let status = map_subscription_status(&sub.status);

    let flat_plan: Option<(Uuid, String, sqlx::types::Decimal, bool, Option<i32>)> = match sqlx::query_as(
        "SELECT id, code, credits, is_seat_priced, min_seats FROM plans \
          WHERE stripe_price_id = $1 AND is_active = true",
    )
    .bind(price_id)
    .fetch_optional(control)
    .await
    {
        Ok(row) => row,
        Err(e) => {
            tracing::error!(error = %e, event_id, %account_id, price_id, "Stripe webhook: plan lookup by stripe_price_id failed — asking Stripe to redeliver");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "plan lookup failed"})),
            )
                .into_response();
        }
    };
    let seat_tier: Option<(Uuid, String)> = if flat_plan.is_none() {
        match sqlx::query_as(
            "SELECT id, code FROM seat_tiers WHERE stripe_price_id = $1 AND is_active = true",
        )
        .bind(price_id)
        .fetch_optional(control)
        .await
        {
            Ok(row) => row,
            Err(e) => {
                tracing::error!(error = %e, event_id, %account_id, price_id, "Stripe webhook: seat_tiers lookup by stripe_price_id failed — asking Stripe to redeliver");
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({"error": "seat tier lookup failed"})),
                )
                    .into_response();
            }
        }
    } else {
        None
    };
    let team_plan = if seat_tier.is_some() {
        match crate::billing::issuance::resolve_active_plan(control, "team").await {
            Ok(p) => p,
            Err(e) => {
                tracing::error!(error = %e, event_id, %account_id, "Stripe webhook: team plan lookup failed — asking Stripe to redeliver");
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({"error": "team plan lookup failed"})),
                )
                    .into_response();
            }
        }
    } else {
        None
    };

    // The purchased seat count IS Stripe's quantity. Our own Checkouts buy 1
    // (`create_subscription_checkout`) or the self-serve seat count (>= 2,
    // `start_org_trial_checkout`); the ledger takes the line item as-is.
    // Only a value the ledger column cannot hold is refused, permanently.
    let purchased_slots: i32 = match i32::try_from(line_item.quantity) {
        Ok(n) if n >= 1 => n,
        _ if seat_tier.is_none() => 1,
        _ => {
            return claim_and_record_unprocessable(
                control,
                account_id,
                event_id,
                EVENT_TYPE,
                &format!("seat-tier checkout line item carries quantity {}", line_item.quantity),
            )
            .await;
        }
    };

    // ── Self-serve PR C′: a new team org's first Checkout. Everything that
    // needs Stripe (the card's fingerprint) is read here, before the
    // transaction. ──
    let trial_facts = match self_serve_completion_facts(control, stripe, account_id, &sub, session_id).await {
        Ok(f) => f,
        Err(e) => {
            tracing::error!(error = %e, event_id, %account_id, "Stripe webhook: self-serve trial facts unavailable — asking Stripe to redeliver");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "trial facts unavailable"})),
            )
                .into_response();
        }
    };

    // #1528 S2 (review): whether this subscription took money, for the
    // first-charge stamp below. Read here, before the transaction, and only
    // for an org whose trial still waits for its first charge. `active` is
    // not enough: team Checkout allows promotion codes, and a customer
    // balance can zero the invoice, so a 100%-off resubscribe is `active`
    // with nothing paid. The latest invoice's `amount_paid` is what was
    // collected (the session's `amount_total` is before any customer
    // balance, so it is not used).
    let first_charge_paid = match resubscribe_took_money(control, stripe, account_id, &sub).await {
        Ok(paid) => paid,
        Err(e) => {
            tracing::error!(error = %e, event_id, %account_id, "Stripe webhook: resubscribe first-charge check failed — asking Stripe to redeliver");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "first charge check failed"})),
            )
                .into_response();
        }
    };

    // ── ONE account-scoped transaction: claim + grant issuance + seat-tier
    // bookkeeping + mirror + outcome (findings 1 + 2). ──
    let mut tx = match crate::tenancy::scope::begin_account_scoped(control, account_id).await {
        Ok(tx) => tx,
        Err(e) => {
            tracing::error!(error = %e, event_id, %account_id, "Stripe webhook: checkout completion scope failed");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "scope failed"})),
            )
                .into_response();
        }
    };
    if let Err(e) = crate::billing::renewal::lock_period_rotation(&mut tx, account_id).await {
        tracing::error!(error=%e,event_id,%account_id,"Stripe financial entry failed");
        return (StatusCode::INTERNAL_SERVER_ERROR,Json(json!({"error":"financial entry failed"}))).into_response();
    }
    let claimed = match sqlx::query(
        "INSERT INTO stripe_webhook_events (event_id, event_type, account_id) \
         VALUES ($1, $2, $3) ON CONFLICT (event_id) DO NOTHING",
    )
    .bind(event_id)
    .bind(EVENT_TYPE)
    .bind(account_id)
    .execute(&mut *tx)
    .await
    {
        Ok(r) => r.rows_affected(),
        Err(e) => {
            tracing::error!(error = %e, event_id, %account_id, "Stripe webhook: checkout completion claim failed");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "claim failed"})),
            )
                .into_response();
        }
    };
    if claimed == 0 {
        let _ = tx.rollback().await;
        tracing::info!(event_id, %account_id, "Stripe webhook: replay — already processed");
        return (StatusCode::OK, Json(json!({"ok": true, "replay": true}))).into_response();
    }

    // Billing self-serve PR F review, MAJOR finding: the per-account
    // advisory lock is taken HERE — immediately after the event claim,
    // before `issue_grant_in_tx` or ANY OTHER write to `grants`/
    // `grant_seat_tiers` below — matching the lock-ordering invariant this
    // file's own module doc states ("advisory lock before any
    // `stripe_subscriptions` row lock OR `grants` row write") and
    // `apply_invoice_paid`/`apply_subscription_updated` already honor. An
    // earlier version of this handler took the lock only deep inside the
    // team-plan branch, AFTER `issue_grant_in_tx` had already written
    // `grants` — a genuine deadlock window: a concurrent `invoice.paid` for
    // the SAME account could hold the advisory lock (its own
    // `lock_period_rotation`, taken first) while blocking on the `grants`
    // row this handler's `issue_grant_in_tx` had already locked, while THIS
    // handler blocked on the advisory lock the other delivery held —
    // Postgres detects the cycle (`40P01`), aborts one side, and that
    // handler answers 500, asking Stripe to redeliver a perfectly valid
    // event. Checkout completion and the first `invoice.paid` for a new
    // subscription arrive close together in practice, so this was a live
    // window, not a theoretical one.
    if let Err(e) = crate::billing::renewal::lock_period_rotation(&mut tx, account_id).await {
        tracing::error!(error = %e, event_id, %account_id, "Stripe webhook: checkout completion lock failed — asking Stripe to redeliver");
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": "lock failed"})),
        )
            .into_response();
    }

    // A second trial Checkout completing for an org whose trial already
    // moved on (a resumed session whose predecessor was not expired in
    // time) made a SECOND subscription that would bill on day 90. Decided
    // under the lock; the Stripe cancel runs after this transaction is
    // rolled back (no Stripe call inside a transaction), and the event is
    // claimed only once the cancel succeeded, so a failure is redelivered.
    match duplicate_trial_subscription(&mut tx, account_id, subscription_id).await {
        Ok(None) => {}
        Ok(Some(existing)) => {
            let _ = tx.rollback().await;
            return cancel_duplicate_trial_subscription(
                control,
                stripe,
                account_id,
                event_id,
                EVENT_TYPE,
                subscription_id,
                &existing,
            )
            .await;
        }
        Err(e) => {
            tracing::error!(error = %e, event_id, %account_id, "Stripe webhook: duplicate-trial check failed — asking Stripe to redeliver");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "duplicate trial check failed"})),
            )
                .into_response();
        }
    }

    // The trial decision is taken under the lock, on the row as it is now:
    // a second event for the same session finds it past `checkout_open`.
    let trial_plan: Option<(crate::billing::org_trial::db::TrialRow, crate::billing::org_trial::CompletionPlan)> =
        match &trial_facts {
            Some(facts) if seat_tier.is_some() => match plan_trial_completion(&mut tx, account_id, facts).await {
                Ok(p) => p,
                Err(e) => {
                    tracing::error!(error = %e, event_id, %account_id, "Stripe webhook: trial completion planning failed — asking Stripe to redeliver");
                    return (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(json!({"error": "trial planning failed"})),
                    )
                        .into_response();
                }
            },
            _ => None,
        };
    // 0 credits per seat until the seats are paid (`billing::org_trial`).
    let team_credits_override = trial_plan.as_ref().and_then(|(_, plan)| plan.credits_override());

    let grant_note = format!("stripe checkout session {session_id}");
    let mut grant_summary = String::from("no_grant=true");

    if let Some((plan_id, code, credits, is_seat_priced, min_seats)) = flat_plan {
        let plan = crate::billing::issuance::PlanRow { id: plan_id, code, credits, is_seat_priced, min_seats };
        match crate::billing::issuance::issue_grant_in_tx(&mut tx, account_id, &plan, None, None, Some(&grant_note))
            .await
        {
            Ok(crate::billing::issuance::GrantOutcome::Granted(r)) => {
                grant_summary = format!("granted plan={} {}", plan.code, r.summary());
            }
            Ok(crate::billing::issuance::GrantOutcome::AlreadyGranted) => {
                grant_summary = format!("already_granted plan={}", plan.code);
            }
            Err(e) => {
                tracing::error!(error = %e, event_id, %account_id, "Stripe webhook: issue_grant_in_tx failed for checkout completion — asking Stripe to redeliver");
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({"error": "grant issuance failed"})),
                )
                    .into_response();
            }
        }
    } else if let Some((seat_tier_id, tier_code)) = seat_tier {
        if let Some(team_plan) = team_plan {
            match crate::billing::issuance::issue_grant_in_tx(
                &mut tx,
                account_id,
                &team_plan,
                Some(purchased_slots),
                team_credits_override,
                Some(&grant_note),
            )
            .await
            {
                Ok(crate::billing::issuance::GrantOutcome::Granted(r)) => {
                    grant_summary =
                        format!("granted plan=team tier={tier_code} slots={purchased_slots} {}", r.summary());
                    // ── Seed the purchase ledger (billing self-serve PR F) ──
                    //
                    // `grant_seat_tiers`/`grants.seats` are no longer roster-
                    // derived (see `billing::renewal`'s own module doc,
                    // "`grant_seat_tiers` is a purchase-driven ledger") — this
                    // checkout bought exactly ONE seat, at `tier_code`, for
                    // whoever completed it, and this is the ONE place that
                    // purchase gets recorded. `issue_grant_in_tx` just above
                    // no longer touches `grant_seat_tiers` at all (its own
                    // roster reconciliation was retired the same PR this
                    // comment belongs to), so there is no double-count risk
                    // this arm used to guard against.
                    //
                    // `accounts.owner_user_id` stands in for "whoever
                    // completed checkout" — Stripe Checkout's subscription
                    // mode carries no per-user identity through to this
                    // webhook (`routes::checkout::create_subscription_checkout`
                    // sets `client_reference_id`/`metadata[account_id]` only,
                    // never a user id), and the owner is the only user this
                    // account can name unambiguously. A real purchaser who is
                    // an admin but not the owner is not distinguishable from
                    // here; this is a known simplification, not an oversight
                    // — see this function's own doc comment for the general
                    // "no per-user identity in a subscription checkout"
                    // constraint.
                    // Finding 2 (billing self-serve PR F review): propagate a
                    // lookup failure instead of `.unwrap_or(None)` — a
                    // masked DB error here used to read identically to "the
                    // just-issued grant genuinely does not exist", routing
                    // into the `None` arm's loud-but-WRONG "should not
                    // happen" error below instead of the transient-retry
                    // path this failure actually needs.
                    let grant_id: Option<Uuid> = match sqlx::query_scalar(
                        "SELECT id FROM grants WHERE account_id = $1 AND plan_id = $2 \
                          AND revoked_at IS NULL ORDER BY granted_at DESC, id DESC LIMIT 1",
                    )
                    .bind(account_id)
                    .bind(team_plan.id)
                    .fetch_optional(&mut *tx)
                    .await
                    {
                        Ok(row) => row,
                        Err(e) => {
                            tracing::error!(error = %e, event_id, %account_id, "Stripe webhook: checkout-completed grant lookup failed — asking Stripe to redeliver");
                            return (
                                StatusCode::INTERNAL_SERVER_ERROR,
                                Json(json!({"error": "grant lookup failed"})),
                            )
                                .into_response();
                        }
                    };
                    match grant_id {
                        Some(grant_id) => {
                            // Finding 2: ONE UPDATE that aligns
                            // `account_members.seat_tier_id` with the
                            // PURCHASED tier (the retired version never did
                            // this at all — the purchaser's assigned tier
                            // could silently disagree with what they
                            // actually bought), clears any stale
                            // `pending_seat_tier_id`, and stamps
                            // `seat_purchased_at` — guarded
                            // `AND seat_purchased_at IS NULL`,
                            // `rows_affected` CHECKED (the retired version
                            // did neither). A 0-row result means the
                            // purchaser's `account_members` row is missing or
                            // already stamped — virtually unreachable in
                            // practice, since `require_purchase_authority`
                            // already verified membership at checkout time —
                            // but handled by SKIPPING the ledger seed below
                            // entirely rather than seeding a
                            // `grant_seat_tiers` seat with no member row
                            // correctly backing it: failing toward
                            // UNDER-entitlement, with the next roll's
                            // `seat_mismatch` signal there to surface it, is
                            // the safe direction (over-entitlement is silent
                            // and unrecoverable by any later signal).
                            let stamp = sqlx::query(
                                "UPDATE account_members \
                                    SET seat_tier_id = $2, pending_seat_tier_id = NULL, seat_purchased_at = now() \
                                  WHERE account_id = $1 \
                                    AND user_id = (SELECT owner_user_id FROM accounts WHERE id = $1) \
                                    AND seat_purchased_at IS NULL",
                            )
                            .bind(account_id)
                            .bind(seat_tier_id)
                            .execute(&mut *tx)
                            .await;
                            let stamped = match stamp {
                                Ok(r) => r.rows_affected() > 0,
                                Err(e) => {
                                    tracing::error!(error = %e, event_id, %account_id, "Stripe webhook: checkout-completed purchaser seat_purchased_at seed failed — asking Stripe to redeliver");
                                    return (
                                        StatusCode::INTERNAL_SERVER_ERROR,
                                        Json(json!({"error": "seat ledger seed failed"})),
                                    )
                                        .into_response();
                                }
                            };
                            if !stamped {
                                tracing::error!(
                                    event_id, %account_id,
                                    "Stripe webhook: checkout-completed purchaser stamp affected 0 rows \
                                     (the purchaser's account_members row is missing, or was already \
                                     stamped) — skipping the grant_seat_tiers seed entirely rather than \
                                     record a purchased seat no member row backs. Failing toward \
                                     under-entitlement; the next roll's seat_mismatch signal surfaces \
                                     it."
                                );
                            } else {
                                // Billing self-serve PR F review, MAJOR
                                // finding: the per-account advisory lock is
                                // now taken ONCE, at the top of this handler
                                // (immediately after the event claim, before
                                // `issue_grant_in_tx`) — see that call's own
                                // comment for why a SECOND acquisition here
                                // used to violate this file's lock-ordering
                                // invariant. `lock_period_rotation` is safe
                                // to call more than once per transaction
                                // (`pg_advisory_xact_lock` is reentrant
                                // within one transaction), but a single
                                // acquisition at the top is simpler to
                                // reason about and matches every other
                                // period-rotating handler in this file.
                                if let Err(e) = sqlx::query(
                                    "INSERT INTO grant_seat_tiers (grant_id, seat_tier_id, seats) \
                                     VALUES ($1, $2, $3) \
                                     ON CONFLICT (grant_id, seat_tier_id) \
                                     DO UPDATE SET seats = grant_seat_tiers.seats + $3",
                                )
                                .bind(grant_id)
                                .bind(seat_tier_id)
                                .bind(purchased_slots)
                                .execute(&mut *tx)
                                .await
                                {
                                    tracing::error!(error = %e, event_id, %account_id, "Stripe webhook: checkout-completed seat-ledger seed failed — asking Stripe to redeliver");
                                    return (
                                        StatusCode::INTERNAL_SERVER_ERROR,
                                        Json(json!({"error": "seat ledger seed failed"})),
                                    )
                                        .into_response();
                                }
                                // Shared helper (minor finding: this SQL was
                                // hand-copied 5x across the crate, two copies
                                // missing the zero-guard `EXISTS` — see
                                // `billing::renewal::resync_grant_seats`'s
                                // own doc comment).
                                if let Err(e) =
                                    crate::billing::renewal::resync_grant_seats(&mut tx, grant_id).await
                                {
                                    tracing::error!(error = %e, event_id, %account_id, "Stripe webhook: checkout-completed grants.seats sync failed — asking Stripe to redeliver");
                                    return (
                                        StatusCode::INTERNAL_SERVER_ERROR,
                                        Json(json!({"error": "seat ledger seed failed"})),
                                    )
                                        .into_response();
                                }
                            }
                        }
                        None => {
                            // Should not happen — `issue_grant_in_tx` just
                            // returned `Granted`, which only ever follows a
                            // successful INSERT into `grants` for this exact
                            // `(account_id, plan_id)` in THIS transaction.
                            // Loud, not silent.
                            tracing::error!(
                                event_id, %account_id,
                                "Stripe webhook: team grant reported Granted but no grants row was found to seed the seat ledger against"
                            );
                            return (
                                StatusCode::INTERNAL_SERVER_ERROR,
                                Json(json!({"error": "grant lookup failed after issuance"})),
                            )
                                .into_response();
                        }
                    }
                }
                Ok(crate::billing::issuance::GrantOutcome::AlreadyGranted) => {
                    // The team grant survives a cancellation (freeze, not
                    // forfeit), so a NEW subscription after a deleted one
                    // lands here. Its quantity is what Stripe now bills, so
                    // it replaces the old ledger instead of leaving it stale.
                    match replace_seat_ledger_for_resubscribe(
                        &mut tx,
                        account_id,
                        team_plan.id,
                        subscription_id,
                        seat_tier_id,
                        purchased_slots,
                    )
                    .await
                    {
                        Ok(true) => {
                            grant_summary =
                                format!("resubscribed plan=team tier={tier_code} slots={purchased_slots}");
                            // #1528 S2: a trial that ended unpaid keeps extra
                            // usage locked until the org's first charge. A
                            // Checkout subscription's `invoice.paid` often
                            // lands BEFORE this event, while the mirror still
                            // names the old subscription, so `invoice.paid`
                            // (which stamps only for the mirrored
                            // subscription) cannot stamp it. A completed
                            // Checkout whose subscription is `active` (not
                            // trialing, not in dunning) AND whose latest
                            // invoice took money has been paid: stamp it
                            // here (`first_charge_paid`, read before the
                            // transaction). A no-op for an org with no trial
                            // row or one already stamped.
                            if first_charge_paid {
                                match crate::billing::org_trial::db::stamp_first_charge(&mut *tx, account_id).await {
                                    Ok(true) => {
                                        grant_summary.push_str(" first_charge=stamped");
                                    }
                                    Ok(false) => {}
                                    Err(e) => {
                                        tracing::error!(error = %e, event_id, %account_id, "Stripe webhook: resubscribe first-charge stamp failed — asking Stripe to redeliver");
                                        return (
                                            StatusCode::INTERNAL_SERVER_ERROR,
                                            Json(json!({"error": "first charge stamp failed"})),
                                        )
                                            .into_response();
                                    }
                                }
                            }
                        }
                        Ok(false) => {
                            grant_summary = format!("already_granted plan=team tier={tier_code}");
                        }
                        Err(e) => {
                            tracing::error!(error = %e, event_id, %account_id, "Stripe webhook: resubscribe seat-ledger replace failed — asking Stripe to redeliver");
                            return (
                                StatusCode::INTERNAL_SERVER_ERROR,
                                Json(json!({"error": "seat ledger seed failed"})),
                            )
                                .into_response();
                        }
                    }
                }
                Err(e) => {
                    tracing::error!(error = %e, event_id, %account_id, "Stripe webhook: issue_grant_in_tx failed for team checkout completion — asking Stripe to redeliver");
                    return (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(json!({"error": "grant issuance failed"})),
                    )
                        .into_response();
                }
            }
        } else {
            // See this function's own doc comment — the `team` plan is
            // deliberately inactive until PR E. Loud, not silent; never
            // retried (retrying cannot activate a plan row).
            tracing::error!(
                event_id, %account_id, tier_code, price_id,
                "Stripe webhook: checkout paid for a seat tier but the `team` plan has \
                 no ACTIVE row — no grant issued. This is a provisioning-sequencing gap \
                 (PR D shipped before PR E), not a redeliverable failure."
            );
            grant_summary = format!("no_active_team_plan tier={tier_code}");
        }
    } else {
        tracing::error!(
            event_id, %account_id, price_id,
            "Stripe webhook: subscription checkout completed for a price_id that matches \
             neither an active plan nor an active seat tier — no grant issued. This is a \
             pricing-catalog integrity problem, not a redeliverable failure."
        );
    }

    // ── Mirror the new subscription onto `stripe_subscriptions`, same
    // transaction ──
    if let Err(e) = sqlx::query(
        "UPDATE stripe_subscriptions \
            SET stripe_subscription_id = $2, status = $3, \
                current_period_start = $4, current_period_end = $5, \
                cancel_at_period_end = $6, cancel_at = $7, \
                pending_checkout_session_id = NULL, updated_at = now() \
          WHERE account_id = $1",
    )
    .bind(account_id)
    .bind(subscription_id)
    .bind(status)
    .bind(period_start)
    .bind(period_end)
    .bind(sub.cancel_at_period_end)
    .bind(sub.cancel_at.and_then(to_utc))
    .execute(&mut *tx)
    .await
    {
        tracing::error!(error = %e, event_id, %account_id, "Stripe webhook: stripe_subscriptions mirror update failed — asking Stripe to redeliver");
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": "mirror update failed"})),
        )
            .into_response();
    }
    let mut trial_summary = String::new();
    if let (Some((row, plan)), Some(facts)) = (&trial_plan, &trial_facts) {
        match record_trial_completion(&mut tx, row, *plan, facts).await {
            Ok(summary) => trial_summary = format!(" {summary}"),
            Err(e) => {
                tracing::error!(error = %e, event_id, %account_id, "Stripe webhook: trial completion write failed — asking Stripe to redeliver");
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({"error": "trial completion failed"})),
                )
                    .into_response();
            }
        }
    }
    let recorded_outcome =
        format!("checkout_completed_subscription {grant_summary} status={status}{trial_summary}");
    if let Err(e) = sqlx::query("UPDATE stripe_webhook_events SET outcome = $2 WHERE event_id = $1")
        .bind(event_id)
        .bind(&recorded_outcome)
        .execute(&mut *tx)
        .await
    {
        tracing::error!(error = %e, event_id, %account_id, "Stripe webhook: outcome record failed — asking Stripe to redeliver");
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": "outcome record failed"})),
        )
            .into_response();
    }
    if let Err(e) = tx.commit().await {
        tracing::error!(error = %e, event_id, %account_id, "Stripe webhook: checkout completion commit failed — asking Stripe to redeliver");
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": "commit failed"})),
        )
            .into_response();
    }

    tracing::info!(event_id, %account_id, summary = %recorded_outcome, "Stripe webhook: subscription checkout completed");

    // A repeat card ended its trial at once: charge the seats now rather
    // than on the worker's next pass. Best effort, off the response path;
    // the worker retries whatever this does not finish.
    if matches!(&trial_plan, Some((_, crate::billing::org_trial::CompletionPlan::EndTrialNow))) {
        let control = control.clone();
        let stripe_for_task = state.stripe.clone();
        tokio::spawn(async move {
            let mut report = crate::billing::org_trial::lifecycle::PassReport::default();
            if let Err(e) = crate::billing::org_trial::lifecycle::drive_conversion(
                &control,
                stripe_for_task.as_deref(),
                account_id,
                &mut report,
            )
            .await
            {
                tracing::warn!(%account_id, error = %e, "org trial: immediate conversion failed; the worker retries");
            }
        });
    }

    // Only a freshly claimed event reaches here (a replay returned above), so
    // an old event redelivered later cannot overwrite a card the customer has
    // since changed. Best effort: the grant is already committed and a 500
    // would not be retried (the event is claimed).
    copy_subscription_card_to_customer(stripe, &sub, event_id, account_id).await;
    (StatusCode::OK, Json(json!({"ok": true}))).into_response()
}

/// #1528 S2 (review): whether a completed Checkout's subscription took
/// money, for `checkout.session.completed`'s first-charge stamp. `false`
/// without a Stripe read unless the org's trial ended unpaid (`canceled` or
/// `abandoned`) and still waits for its first charge. Then the subscription
/// must be `active` and its latest invoice must have `amount_paid > 0`: a
/// promotion code or a customer balance can leave an `active` subscription
/// with nothing collected, and that is not a first charge.
async fn resubscribe_took_money(
    control: &sqlx::PgPool,
    stripe: &crate::billing::stripe_client::StripeClient,
    account_id: Uuid,
    sub: &crate::billing::stripe_client::SubscriptionView,
) -> Result<bool, String> {
    if sub.status != "active" {
        return Ok(false);
    }
    let waiting = {
        let mut tx = crate::tenancy::scope::begin_account_scoped(control, account_id)
            .await
            .map_err(|e| format!("scope: {e}"))?;
        let row = crate::billing::org_trial::db::load(&mut tx, account_id)
            .await
            .map_err(|e| format!("org_trials read: {e}"))?;
        tx.commit().await.map_err(|e| format!("commit: {e}"))?;
        // Only a trial that ended unpaid waits on a later subscription's
        // first charge (`TrialState::refuses_extra_usage`).
        row.is_some_and(|r| {
            r.first_charge_at.is_none()
                && matches!(
                    r.state,
                    crate::billing::org_trial::TrialState::Canceled | crate::billing::org_trial::TrialState::Abandoned
                )
        })
    };
    if !waiting {
        return Ok(false);
    }
    let Some(invoice_id) = sub.latest_invoice.as_deref() else { return Ok(false) };
    let invoice = crate::billing::seat_lifecycle::with_stripe_deadline(stripe.retrieve_invoice(invoice_id))
        .await
        .map_err(|e| format!("retrieve latest invoice {invoice_id}: {e}"))?;
    Ok(invoice.amount_paid > 0)
}

/// Self-serve PR C′: the facts a self-serve org's first Checkout completion
/// needs from Stripe, read before the webhook's transaction. `Ok(None)` for
/// every account that is not waiting on its self-serve Checkout (no
/// `org_trials` row, or one already past checkout), which is every account
/// on the existing paths.
///
/// The card fingerprint is read only for a trialing subscription: it is
/// what the one-trial-per-card rule compares, and a subscription that was
/// charged at Checkout has no trial to refuse.
async fn self_serve_completion_facts(
    control: &sqlx::PgPool,
    stripe: &crate::billing::stripe_client::StripeClient,
    account_id: Uuid,
    sub: &crate::billing::stripe_client::SubscriptionView,
    session_id: &str,
) -> Result<Option<crate::billing::org_trial::db::CompletionFacts>, String> {
    use crate::billing::org_trial::TrialState;
    let state = {
        let mut tx = crate::tenancy::scope::begin_account_scoped(control, account_id)
            .await
            .map_err(|e| format!("scope: {e}"))?;
        let state = crate::billing::org_trial::db::state_of(&mut tx, account_id)
            .await
            .map_err(|e| format!("org_trials read: {e}"))?;
        tx.commit().await.map_err(|e| format!("commit: {e}"))?;
        state
    };
    if !matches!(state, Some(TrialState::CheckoutOpen | TrialState::Abandoned)) {
        return Ok(None);
    }
    let trialing = sub.status == "trialing";
    let card_fingerprint = if trialing {
        trial_card_fingerprint(stripe, sub, session_id).await?
    } else {
        None
    };
    Ok(Some(crate::billing::org_trial::db::CompletionFacts {
        subscription_id: sub.id.clone(),
        subscription_trialing: trialing,
        trial_ends_at: sub.trial_end.and_then(to_utc),
        card_fingerprint,
    }))
}

/// The card a trial Checkout saved, as a fingerprint, looked for where
/// Stripe may have put it: the subscription's `default_payment_method`, then
/// the customer's `invoice_settings.default_payment_method`, then the
/// Checkout session's SetupIntent. `Ok(None)` when none of them names a card
/// with a fingerprint; the caller then fails closed (no trial, charge now).
/// A Stripe error is `Err`, so the event is redelivered rather than judged
/// on a transient failure.
async fn trial_card_fingerprint(
    stripe: &crate::billing::stripe_client::StripeClient,
    sub: &crate::billing::stripe_client::SubscriptionView,
    session_id: &str,
) -> Result<Option<String>, String> {
    use crate::billing::seat_lifecycle::with_stripe_deadline;
    let fingerprint_of = |pm: String| async move {
        with_stripe_deadline(stripe.retrieve_card_fingerprint(&pm))
            .await
            .map_err(|e| format!("card fingerprint: {e}"))
    };
    if let Some(pm) = sub.default_payment_method.clone() {
        if let Some(fp) = fingerprint_of(pm).await? {
            return Ok(Some(fp));
        }
    }
    if let Some(customer) = sub.customer_id.as_deref() {
        let pm = with_stripe_deadline(stripe.retrieve_customer_default_payment_method(customer))
            .await
            .map_err(|e| format!("customer default payment method: {e}"))?;
        if let Some(pm) = pm {
            if let Some(fp) = fingerprint_of(pm).await? {
                return Ok(Some(fp));
            }
        }
    }
    let session = with_stripe_deadline(stripe.retrieve_checkout_session(session_id))
        .await
        .map_err(|e| format!("checkout session: {e}"))?;
    let setup_intent = match session.get("setup_intent") {
        Some(Value::String(id)) => Some(id.clone()),
        Some(Value::Object(o)) => o.get("id").and_then(Value::as_str).map(str::to_string),
        _ => None,
    };
    if let Some(si) = setup_intent {
        let pm = with_stripe_deadline(stripe.retrieve_setup_intent_payment_method(&si))
            .await
            .map_err(|e| format!("setup intent: {e}"))?;
        if let Some(pm) = pm {
            if let Some(fp) = fingerprint_of(pm).await? {
                return Ok(Some(fp));
            }
        }
    }
    Ok(None)
}

/// Review finding 2: is `subscription_id` a SECOND subscription for an org
/// whose self-serve trial already completed on another one that is still
/// live? Returns that existing subscription's id.
///
/// `None` (not a duplicate) when there is no trial row, the trial is still
/// at checkout (this IS the first completion) or ended `canceled`, the
/// subscription is the trial's own (a redelivery), or the account's linked
/// subscription is already `canceled` (a legitimate resubscribe after the
/// paid subscription ended; `replace_seat_ledger_for_resubscribe`).
async fn duplicate_trial_subscription(
    tx: &mut sqlx::Transaction<'static, sqlx::Postgres>,
    account_id: Uuid,
    subscription_id: &str,
) -> Result<Option<String>, sqlx::Error> {
    use crate::billing::org_trial::TrialState;
    let Some(row) = crate::billing::org_trial::db::load_for_update(tx, account_id).await? else {
        return Ok(None);
    };
    if !matches!(row.state, TrialState::Trialing | TrialState::Converting | TrialState::Converted) {
        return Ok(None);
    }
    let Some(existing) = row.stripe_subscription_id else { return Ok(None) };
    if existing == subscription_id {
        return Ok(None);
    }
    let linked_status: Option<String> =
        sqlx::query_scalar("SELECT status FROM stripe_subscriptions WHERE account_id = $1")
            .bind(account_id)
            .fetch_optional(&mut **tx)
            .await?;
    if linked_status.as_deref() == Some("canceled") {
        return Ok(None);
    }
    Ok(Some(existing))
}

/// The idempotency key that cancels a duplicate trial subscription.
pub(crate) fn duplicate_trial_cancel_key(subscription_id: &str) -> String {
    format!("org-trial-duplicate-cancel:{subscription_id}")
}

/// The idempotency key that refunds a paid duplicate's invoice, once.
pub(crate) fn duplicate_trial_refund_key(invoice_id: &str) -> String {
    format!("org-trial-duplicate-refund:{invoice_id}")
}

/// What [`refund_duplicate_invoice`] did.
enum DuplicateRefund {
    /// The duplicate took no money ($0 invoice, or no invoice).
    NothingPaid,
    Refunded { invoice: String, amount_paid: i64 },
    /// Money was taken, but not through a PaymentIntent this can refund
    /// (customer balance, paid out of band). Retrying cannot change that:
    /// recorded, and alerted for a manual refund.
    NeedsManualRefund { invoice: String, amount_paid: i64 },
}

/// Review round 2, finding C: a duplicate that PAID (a no-trial Checkout,
/// say) is refunded in full, keyed on the invoice, after its subscription
/// was canceled. `Err` (a Stripe failure) means the caller must not claim
/// the event, so Stripe redelivers and this runs again under the same key.
async fn refund_duplicate_invoice(
    stripe: &crate::billing::stripe_client::StripeClient,
    duplicate: &str,
) -> Result<DuplicateRefund, String> {
    use crate::billing::seat_lifecycle::with_stripe_deadline;
    let sub = with_stripe_deadline(stripe.retrieve_subscription(duplicate))
        .await
        .map_err(|e| format!("re-read duplicate subscription: {e}"))?;
    let Some(invoice_id) = sub.latest_invoice else { return Ok(DuplicateRefund::NothingPaid) };
    let invoice = with_stripe_deadline(stripe.retrieve_invoice(&invoice_id))
        .await
        .map_err(|e| format!("read duplicate invoice {invoice_id}: {e}"))?;
    if invoice.amount_paid <= 0 {
        return Ok(DuplicateRefund::NothingPaid);
    }
    let Some(pi) = invoice.payment_intent.as_deref() else {
        return Ok(DuplicateRefund::NeedsManualRefund { invoice: invoice.id, amount_paid: invoice.amount_paid });
    };
    let key = duplicate_trial_refund_key(&invoice.id);
    match with_stripe_deadline(stripe.refund_payment_intent(pi, &key)).await {
        Ok(_) => {}
        // #1528 N3: a redelivery after the idempotency key expired (24 h)
        // sends a NEW refund request, which Stripe answers with
        // `charge_already_refunded`. The refund this call exists to make is
        // done, so that is success, not a 500 Stripe would redeliver forever.
        Err(crate::billing::stripe_client::StripeError::Api { code: Some(code), .. })
            if code == "charge_already_refunded" =>
        {
            tracing::info!(invoice = %invoice.id, payment_intent = pi, "duplicate trial invoice was already refunded");
        }
        Err(e) => return Err(format!("refund duplicate invoice {} ({pi}): {e}", invoice.id)),
    }
    Ok(DuplicateRefund::Refunded { invoice: invoice.id, amount_paid: invoice.amount_paid })
}

/// Cancel a duplicate trial subscription at Stripe NOW (no proration, no
/// final invoice; keyed on its id), refund its invoice in full if it took
/// money (keyed on the invoice; review round 2, finding C), then claim the
/// event and record what happened. A failed cancel or refund answers 500
/// without claiming, so Stripe redelivers and both keyed calls run again. The `stripe_subscriptions` mirror, the grant and the trial are
/// left exactly as the first completion wrote them. Raised as an alert:
/// something let two trial Checkouts complete for one org.
async fn cancel_duplicate_trial_subscription(
    control: &sqlx::PgPool,
    stripe: &crate::billing::stripe_client::StripeClient,
    account_id: Uuid,
    event_id: &str,
    event_type: &str,
    duplicate: &str,
    existing: &str,
) -> axum::response::Response {
    tracing::error!(
        alert = true, event_id, %account_id, duplicate, existing,
        "Stripe webhook: a second trial Checkout completed for an org whose trial already started;          canceling the new subscription at Stripe and leaving the mirror on the first"
    );
    let key = duplicate_trial_cancel_key(duplicate);
    if let Err(e) =
        crate::billing::seat_lifecycle::with_stripe_deadline(stripe.cancel_subscription_now_keyed(duplicate, &key)).await
    {
        // Already canceled (an earlier delivery got this far) is success.
        match crate::billing::seat_lifecycle::with_stripe_deadline(stripe.retrieve_subscription(duplicate)).await {
            Ok(s) if matches!(s.status.as_str(), "canceled" | "incomplete_expired") => {}
            other => {
                tracing::error!(
                    alert = true, error = %e, reread = ?other.err(), event_id, %account_id, duplicate,
                    "Stripe webhook: canceling the duplicate trial subscription failed — asking Stripe to redeliver"
                );
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({"error": "duplicate subscription cancel failed"})),
                )
                    .into_response();
            }
        }
    }
    let refund = match refund_duplicate_invoice(stripe, duplicate).await {
        Ok(r) => r,
        Err(e) => {
            tracing::error!(
                alert = true, error = %e, event_id, %account_id, duplicate,
                "Stripe webhook: refunding the paid duplicate trial subscription failed — asking Stripe to redeliver"
            );
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "duplicate subscription refund failed"})),
            )
                .into_response();
        }
    };
    let refund_note = match &refund {
        DuplicateRefund::NothingPaid => "refund=none".to_string(),
        DuplicateRefund::Refunded { invoice, amount_paid } => {
            tracing::warn!(event_id, %account_id, duplicate, invoice, amount_paid, "Stripe webhook: paid duplicate trial subscription refunded in full");
            format!("refunded={invoice} amount={amount_paid}")
        }
        DuplicateRefund::NeedsManualRefund { invoice, amount_paid } => {
            tracing::error!(
                alert = true, event_id, %account_id, duplicate, invoice, amount_paid,
                "Stripe webhook: the paid duplicate trial subscription was not paid by a PaymentIntent; refund it manually"
            );
            format!("refund_required={invoice} amount={amount_paid}")
        }
    };
    let outcome = format!("duplicate_trial_subscription canceled={duplicate} kept={existing} {refund_note}");
    let result: Result<bool, sqlx::Error> = async {
        let mut tx = crate::tenancy::scope::begin_account_scoped(control, account_id)
            .await
            .map_err(|e| sqlx::Error::Protocol(e.to_string()))?;
        crate::billing::renewal::lock_period_rotation(&mut tx, account_id).await?;
        let claimed = sqlx::query(
            "INSERT INTO stripe_webhook_events (event_id, event_type, account_id, outcome) \
             VALUES ($1, $2, $3, $4) ON CONFLICT (event_id) DO NOTHING",
        )
        .bind(event_id)
        .bind(event_type)
        .bind(account_id)
        .bind(&outcome)
        .execute(&mut *tx)
        .await?
        .rows_affected();
        tx.commit().await?;
        Ok(claimed == 1)
    }
    .await;
    match result {
        Ok(true) => (StatusCode::OK, Json(json!({"ok": true, "duplicate_canceled": true}))).into_response(),
        Ok(false) => (StatusCode::OK, Json(json!({"ok": true, "replay": true}))).into_response(),
        Err(e) => {
            tracing::error!(error = %e, event_id, %account_id, "Stripe webhook: duplicate-subscription outcome record failed — asking Stripe to redeliver");
            (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": "outcome record failed"}))).into_response()
        }
    }
}

/// Decide the trial completion under the transaction's lock. `None` when
/// the row vanished or the trial was already canceled (logged: a Checkout
/// cannot complete after that).
async fn plan_trial_completion(
    tx: &mut sqlx::Transaction<'static, sqlx::Postgres>,
    account_id: Uuid,
    facts: &crate::billing::org_trial::db::CompletionFacts,
) -> Result<Option<(crate::billing::org_trial::db::TrialRow, crate::billing::org_trial::CompletionPlan)>, sqlx::Error>
{
    let Some(row) = crate::billing::org_trial::db::load_for_update(tx, account_id).await? else {
        return Ok(None);
    };
    if row.state == crate::billing::org_trial::TrialState::Canceled {
        tracing::error!(%account_id, "Stripe webhook: Checkout completed for a canceled self-serve trial; treated as an ordinary purchase");
        return Ok(None);
    }
    let plan = crate::billing::org_trial::db::plan_completion(tx, &row, facts).await?;
    Ok(Some((row, plan)))
}

/// Write the trial's new state (and pool, for a first trial) and queue the
/// tenant database for an org that is trialing or already paid. A
/// repeat-card org is queued when its conversion is paid instead
/// (`org_trial::db::finalize_conversion`). Returns the outcome fragment.
async fn record_trial_completion(
    tx: &mut sqlx::Transaction<'static, sqlx::Postgres>,
    row: &crate::billing::org_trial::db::TrialRow,
    plan: crate::billing::org_trial::CompletionPlan,
    facts: &crate::billing::org_trial::db::CompletionFacts,
) -> Result<String, sqlx::Error> {
    use crate::billing::org_trial::CompletionPlan;
    if plan == CompletionPlan::AlreadyApplied {
        return Ok("trial=already_applied".into());
    }
    crate::billing::org_trial::db::record_completion(tx, row, plan, facts).await?;
    let provisioning = match plan {
        CompletionPlan::StartTrial | CompletionPlan::PaidNow => {
            crate::billing::org_trial::db::enqueue_provisioning(tx, row.account_id).await?.summary()
        }
        CompletionPlan::EndTrialNow => "provisioning=after_first_charge".into(),
        CompletionPlan::AlreadyApplied => unreachable!("returned above"),
    };
    Ok(format!("trial={} {provisioning}", plan.next_state().as_str()))
}

/// A seat-tier checkout for a NEW subscription, completing after the
/// account's previous one was deleted. The team grant survives the freeze,
/// so `issue_grant_in_tx` reports `AlreadyGranted` and, before this fix,
/// nothing was recorded: the pool kept the OLD subscription's purchased count
/// while Stripe billed the new quantity. The purchased count is Stripe's
/// quantity, and the old subscription bills nobody any more, so the grant's
/// ledger is REPLACED by this purchase rather than added to or left stale.
///
/// Returns `false`, writing nothing, unless the linked subscription is a
/// different one AND is `canceled`. That keeps a second event for the same
/// subscription, or a late event for an old session after a newer
/// subscription went live, a no-op. Runs before the mirror update, which is
/// what still shows the old subscription here.
///
/// Members still stamped from the old subscription keep their slot and
/// consume this purchase. EVERY member moves onto the purchased tier
/// (a single-item subscription prices only that tier), including those
/// released past the new count, whose old tier would otherwise keep paying
/// out its allowance at the next roll. The owner (the purchaser this
/// webhook can name) is stamped if they were not.
async fn replace_seat_ledger_for_resubscribe(
    tx: &mut sqlx::Transaction<'static, sqlx::Postgres>,
    account_id: Uuid,
    team_plan_id: Uuid,
    subscription_id: &str,
    seat_tier_id: Uuid,
    purchased_slots: i32,
) -> Result<bool, sqlx::Error> {
    let linked: Option<(Option<String>, String)> = sqlx::query_as(
        "SELECT stripe_subscription_id, status FROM stripe_subscriptions WHERE account_id = $1 FOR UPDATE",
    )
    .bind(account_id)
    .fetch_optional(&mut **tx)
    .await?;
    let replaces_an_ended_subscription = matches!(
        &linked,
        Some((Some(old), status)) if old != subscription_id && status == "canceled"
    );
    if !replaces_an_ended_subscription {
        return Ok(false);
    }

    let grant_id: Uuid = sqlx::query_scalar(
        "SELECT id FROM grants WHERE account_id = $1 AND plan_id = $2 \
          AND revoked_at IS NULL ORDER BY granted_at DESC, id DESC LIMIT 1",
    )
    .bind(account_id)
    .bind(team_plan_id)
    .fetch_one(&mut **tx)
    .await?;
    sqlx::query("DELETE FROM grant_seat_tiers WHERE grant_id = $1")
        .bind(grant_id)
        .execute(&mut **tx)
        .await?;
    sqlx::query("INSERT INTO grant_seat_tiers (grant_id, seat_tier_id, seats) VALUES ($1, $2, $3)")
        .bind(grant_id)
        .bind(seat_tier_id)
        .bind(purchased_slots)
        .execute(&mut **tx)
        .await?;
    // The owner (the purchaser) holds a slot first.
    sqlx::query(
        "UPDATE account_members SET seat_purchased_at = now() \
          WHERE account_id = $1 \
            AND user_id = (SELECT owner_user_id FROM accounts WHERE id = $1) \
            AND seat_purchased_at IS NULL",
    )
    .bind(account_id)
    .execute(&mut **tx)
    .await?;
    // No more members hold a slot than were bought (`slot_pool`: used <=
    // purchased). Past the new count, the most recently seated members are
    // released exactly as `seat_lifecycle::release_seat` releases one: the
    // `seat_purchased_at` stamp is cleared, and they come back through
    // `acquire_seat` (and the seat-quote flow) like any member without a
    // slot. The owner is kept first, then the longest-seated.
    let released = sqlx::query(
        "WITH ranked AS ( \
             SELECT m.user_id, row_number() OVER ( \
                        ORDER BY (m.user_id = a.owner_user_id) DESC, m.seat_purchased_at, m.user_id) AS rn \
               FROM account_members m JOIN accounts a ON a.id = m.account_id \
              WHERE m.account_id = $1 AND m.seat_purchased_at IS NOT NULL) \
         UPDATE account_members m SET seat_purchased_at = NULL \
           FROM ranked r \
          WHERE m.account_id = $1 AND m.user_id = r.user_id AND r.rn > $2",
    )
    .bind(account_id)
    .bind(i64::from(purchased_slots))
    .execute(&mut **tx)
    .await?
    .rows_affected();
    if released > 0 {
        tracing::warn!(
            %account_id, released, purchased_slots,
            "Stripe webhook: resubscribed with fewer seats than seated members; the newest were released to re-acquire a slot"
        );
    }
    // Every member moves onto this purchase's one tier, not only those who
    // keep a slot (review round 2, finding D). `roll_period` grants each
    // member the allowance of their `seat_tier_id` whether or not they hold
    // a slot, so a released member left on the OLD subscription's tier (a
    // premium seat, say) would be granted that tier's allowance with
    // nobody paying for it. A released member re-acquires through
    // `acquire_seat` and the seat-quote flow, on whatever tier is bought.
    sqlx::query(
        "UPDATE account_members SET seat_tier_id = $2, pending_seat_tier_id = NULL \
          WHERE account_id = $1",
    )
    .bind(account_id)
    .bind(seat_tier_id)
    .execute(&mut **tx)
    .await?;
    // A trial that ended unpaid left its 0-credit override on this grant
    // (`org_trial::db::end` clears it too; this covers any row that
    // predates that). Only a self-serve trial org's override is touched.
    if crate::billing::org_trial::db::state_of(tx, account_id).await?.is_some() {
        crate::billing::org_trial::db::clear_trial_override(tx, account_id, "resubscribed after trial").await?;
    }
    crate::billing::renewal::resync_grant_seats(tx, grant_id).await?;
    Ok(true)
}

/// Copy a new subscription's payment method onto the customer's
/// `invoice_settings.default_payment_method`.
///
/// Off-session charges (extra-usage auto-reload, settings launch PR 3.5) pay
/// with the customer's default, and Checkout only attaches the card to the
/// subscription. Without this copy the first reload would have nothing to
/// charge. When the subscription carries no default of its own, nothing is
/// copied (no guessing from the customer's card list). Skipped when the customer already
/// has that exact default, and for a subscription that is not active.
async fn copy_subscription_card_to_customer(
    stripe: &crate::billing::stripe_client::StripeClient,
    sub: &crate::billing::stripe_client::SubscriptionView,
    event_id: &str,
    account_id: Uuid,
) {
    use crate::billing::stripe_client::Idem;
    if map_subscription_status(&sub.status) != "active" {
        return;
    }
    let Some(customer) = sub.customer_id.as_deref() else {
        tracing::warn!(event_id, %account_id, "Stripe webhook: subscription has no customer id, default card not copied");
        return;
    };
    let result: Result<(), crate::billing::stripe_client::StripeError> = async {
        // Only the subscription's own default payment method is copied. With
        // none there is nothing to copy; guessing a card from the customer's
        // list could charge a card the customer did not choose for this
        // subscription (auto-reload's no-card notice covers the gap).
        let Some(pm) = sub.default_payment_method.clone() else {
            tracing::warn!(event_id, %account_id, customer, "Stripe webhook: subscription has no default payment method, customer default left unchanged");
            return Ok(());
        };
        if stripe.retrieve_customer_default_payment_method(customer).await?.as_deref() == Some(pm.as_str()) {
            return Ok(());
        }
        let key = format!("cust-default-pm:{customer}:{pm}");
        stripe.set_default_payment_method(customer, &pm, Idem::Key(&key)).await?;
        tracing::info!(event_id, %account_id, customer, payment_method = %pm, "Stripe webhook: copied the subscription card to the customer default");
        Ok(())
    }
    .await;
    if let Err(e) = result {
        tracing::warn!(error = %e, event_id, %account_id, customer, "Stripe webhook: could not copy the subscription card to the customer default");
    }
}

/// Payment-mode `checkout.session.completed` for a CREDIT PACK. Since settings
/// launch PR 3.4 a pack credits the extra-usage ledger and balance, not
/// `credit_balances` (the plan pool every debit subtracts from, which made
/// credits bought after the allowance ran out unspendable). All of the
/// crediting, claiming and idempotency lives in
/// [`crate::billing::extra_usage::webhook`]; this is only the HTTP mapping.
///
/// Test-only: the session facts are "paid in full" (an `amount_total` far above
/// any list price, which the credit caps at list).
#[cfg(test)]
async fn apply_checkout_completed_payment(
    control: &sqlx::PgPool,
    account_id: Uuid,
    event_id: &str,
    event_type: &str,
    session_id: &str,
    line_items: &[crate::billing::stripe_client::SessionLineItem],
) -> axum::response::Response {
    use crate::billing::extra_usage::webhook::{apply_pack_purchase, PaymentFacts};
    payment_response(
        account_id,
        event_id,
        apply_pack_purchase(
            control,
            account_id,
            event_id,
            event_type,
            session_id,
            &PaymentFacts {
                payment_status: Some("paid".into()),
                amount_total_cents: Some(100_000_000_000),
                amount_subtotal_cents: Some(100_000_000_000),
                ..PaymentFacts::default()
            },
            line_items,
        )
        .await,
    )
}

/// Map a payment-mode credit outcome to Stripe's retry contract: a database
/// failure is a 500 (the transaction, claim included, rolled back, so the
/// redelivery retries cleanly); everything else is a recorded 200.
fn payment_response(
    account_id: Uuid,
    event_id: &str,
    result: Result<crate::billing::extra_usage::webhook::PaymentOutcome, sqlx::Error>,
) -> axum::response::Response {
    use crate::billing::extra_usage::webhook::PaymentOutcome;
    match result {
        Ok(PaymentOutcome::Credited { .. }) => (StatusCode::OK, Json(json!({"ok": true}))).into_response(),
        Ok(PaymentOutcome::Replay) => (StatusCode::OK, Json(json!({"ok": true, "replay": true}))).into_response(),
        Ok(PaymentOutcome::DuplicateSession) => {
            (StatusCode::OK, Json(json!({"ok": true, "duplicate_session": true}))).into_response()
        }
        Ok(PaymentOutcome::Unprocessable(_)) => {
            (StatusCode::OK, Json(json!({"ok": true, "unprocessable": true}))).into_response()
        }
        Err(e) => {
            tracing::error!(error = %e, event_id, %account_id, "Stripe webhook: payment checkout credit failed, asking Stripe to redeliver");
            (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": "credit failed"}))).into_response()
        }
    }
}

/// Resolve which account owns a Stripe customer id, via `0162`'s SECURITY
/// DEFINER resolver.
///
/// This is the chicken-and-egg every webhook entry point has: the request
/// carries a customer id and nothing else that identifies us, and
/// `stripe_subscriptions` is RLS-covered, so the account must be discovered
/// BEFORE a scoped transaction can be opened. `0086` established the pattern —
/// a narrow SECURITY DEFINER wrapper returning a bare account id and nothing
/// else, so it can bootstrap a scope decision but cannot probe billing state.
/// Like 0086's resolvers it must NOT itself be wrapped in
/// `begin_account_scoped`.
///
/// Extracted rather than inlined so the tests exercise the SAME call the
/// handler makes. This function decides WHICH ACCOUNT GETS CREDITED, so a test
/// that re-typed its SQL would be the exact hole review already caught once in
/// this build (see `billing::entitlement`'s module doc).
///
/// `Ok(None)` means no account is linked to that customer — not an error; see
/// the call site for why that answers 200.
async fn resolve_account(
    control: &sqlx::PgPool,
    customer_id: &str,
) -> Result<Option<Uuid>, sqlx::Error> {
    sqlx::query_scalar("SELECT resolve_stripe_customer_account_id($1)")
        .bind(customer_id)
        .fetch_one(control)
        .await
}

/// What a claimed event went on to do — either its ordinary work, or nothing
/// at all because the staleness guard found it superseded. See the module
/// doc's "Out-of-order delivery" section for the full reasoning; this type
/// exists so that section's rule — never represent "stale" as a zero-valued
/// [`crate::billing::renewal::RenewalOutcome`]/[`SubscriptionUpdateOutcome`]
/// — is enforced by the type system rather than by a caller remembering it.
///
/// Shared by [`apply_invoice_paid`] and [`apply_subscription_updated`], the
/// two call sites the staleness guard protects.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ClaimedOutcome<T> {
    /// The event was newer than (or carried the same period as) whatever
    /// `stripe_subscriptions.current_period_start` already recorded, so it
    /// did its normal work.
    Applied(T),
    /// Superseded by a period Stripe already told us about more recently —
    /// see the module doc. No destructive work ran: no `close_open_period`,
    /// no `roll_period`, no seat sync, no period-mirror update. The claim
    /// was still committed, so a retry of this exact `event_id` is an
    /// ordinary replay (`Ok(None)`), never a second staleness check.
    Stale {
        incoming_period_start: chrono::DateTime<chrono::Utc>,
        stored_period_start: chrono::DateTime<chrono::Utc>,
    },
}

impl<T> ClaimedOutcome<T> {
    /// `true` iff this event was refused as superseded.
    pub(crate) fn is_stale(&self) -> bool {
        matches!(self, Self::Stale { .. })
    }

    /// The applied outcome, or `None` if this event was stale. Named to read
    /// naturally at a call site that already knows which one it expects —
    /// `outcome.applied().expect("not stale")` — matching this file's
    /// existing `.expect("not a replay")` idiom on the OUTER `Option`.
    pub(crate) fn applied(self) -> Option<T> {
        match self {
            Self::Applied(t) => Some(t),
            Self::Stale { .. } => None,
        }
    }

}

/// A short machine-greppable summary for `stripe_webhook_events.outcome` for
/// a [`ClaimedOutcome::Stale`] event specifically, matching
/// [`crate::billing::renewal::RenewalOutcome::summary`]'s style. Not a method
/// on `ClaimedOutcome<T>` itself — it does not depend on `T` at all, and the
/// `Applied` case has no summary of its own here: a caller with an
/// `Applied(t)` in hand already holds `t` and calls its own `t.summary()` (or,
/// for `apply_invoice_paid`, combines it with `closed.summary()` first),
/// exactly as before this type existed.
fn stale_summary(incoming: chrono::DateTime<chrono::Utc>, stored: chrono::DateTime<chrono::Utc>) -> String {
    format!("stale incoming_period_start={incoming} stored_period_start={stored}")
}

/// What one claimed `invoice.paid` event went on to do. A superset of
/// [`ClaimedOutcome<RenewalOutcome>`] specific to [`apply_invoice_paid`]
/// (finding 3, billing self-serve PR D review round) — deliberately NOT
/// folded into the shared generic type, because [`apply_subscription_updated`]
/// has no equivalent "wrong subscription" case (it is keyed on `account_id`
/// alone by design; only the invoice-triggered status flip needs to check
/// identity, and only `apply_invoice_paid` is the one that restores `status
/// = "active"`). Reusing the shared enum here would force every OTHER
/// caller of `ClaimedOutcome` to grow a dead match arm for a variant it can
/// never produce.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum InvoicePaidOutcome {
    /// Normal case: the invoice's subscription id matched (or nothing was
    /// stored yet to compare against — see the guard's own comment), so the
    /// period rolled and `status` flipped to `"active"`.
    Applied(crate::billing::renewal::RenewalOutcome),
    /// Superseded by a period Stripe already told us about more recently —
    /// see the module doc's "Out-of-order delivery" section. Identical
    /// meaning to [`ClaimedOutcome::Stale`]; restated here rather than
    /// shared because this enum cannot itself be generic over `T` the way
    /// `ClaimedOutcome<T>` is (its `Applied` arm is pinned to
    /// `RenewalOutcome`).
    Stale {
        incoming_period_start: chrono::DateTime<chrono::Utc>,
        stored_period_start: chrono::DateTime<chrono::Utc>,
    },
    /// The invoice's subscription id does not match what
    /// `stripe_subscriptions.stripe_subscription_id` currently records for
    /// this account, OR it matches but the stored `status` is already
    /// `"canceled"` — see [`apply_invoice_paid`]'s own doc comment for the
    /// two cases this covers. No status flip, no period roll: this is
    /// deliberately distinct from [`Self::Stale`] (a PERIOD comparison) —
    /// this is an IDENTITY/deliberate-freeze comparison, and conflating the
    /// two would make `stripe_webhook_events.outcome` say "stale" for a
    /// case that isn't about ordering at all.
    SupersededSubscription { reason: String },
}

impl InvoicePaidOutcome {
    /// `true` iff this event was refused as superseded by a NEWER period —
    /// mirrors [`ClaimedOutcome::is_stale`]. Deliberately `false` for
    /// [`Self::SupersededSubscription`]: that is a different refusal reason
    /// (identity/deliberate-freeze, not ordering), and a caller that wants
    /// to distinguish the two should match on the variant directly rather
    /// than lean on this method.
    pub(crate) fn is_stale(&self) -> bool {
        matches!(self, Self::Stale { .. })
    }

    /// The applied outcome, or `None` for either refusal case — mirrors
    /// [`ClaimedOutcome::applied`].
    pub(crate) fn applied(self) -> Option<crate::billing::renewal::RenewalOutcome> {
        match self {
            Self::Applied(t) => Some(t),
            Self::Stale { .. } | Self::SupersededSubscription { .. } => None,
        }
    }
}

/// Claim the event and roll the period, in one transaction.
///
/// `Ok(None)` means the event was already processed — the claim found a
/// conflicting row, so nothing was done and nothing needs to be.
///
/// Extracted from the handler so the pg tests exercise the SAME code the
/// endpoint runs rather than a re-typed copy of its queries. That rule is not
/// abstract here: this build has already shipped a test file that re-typed a
/// production query and consequently proved nothing (see
/// `billing::entitlement`'s module doc).
///
/// # Staleness guard
///
/// Runs BEFORE [`crate::billing::renewal::close_open_period`] — see the
/// module doc's "Out-of-order delivery" section for why an older incoming
/// period must never reach that call. Returns
/// [`ClaimedOutcome::Stale`] rather than proceeding when the guard trips.
///
/// # Closing the previous period before seeding this one
///
/// Migration `0163` added an exclusion constraint that makes two overlapping
/// `member_allowance_periods` rows for one member a hard INSERT error (see
/// its header). A SECOND subscription invoice whose period does not start
/// exactly where the first one's ends — a mid-period proration, most
/// concretely — used to land as a silent double-insert; after `0163` it would
/// instead make `roll_period` return `Err`, which this function's caller
/// turns into a 500 that asks Stripe to redeliver an event that would fail
/// identically forever. `close_open_period` is called first, with the
/// incoming `period_start` as the boundary, so any period this account is
/// still holding open is truncated (or, for a period whose own start has not
/// yet arrived — possible under Stripe's at-least-once, not-necessarily-
/// ordered delivery — deleted; see that function's doc comment) before the
/// new one is ever inserted.
///
/// Two redelivery shapes both stay safe through this, for different reasons:
///
///   * An exact replay (same `event_id`) never reaches `close_open_period` at
///     all — the claim above returns `Ok(None)` first, before either call.
///   * A NEW `event_id` for an ALREADY-granted period (`0161`'s UNIQUE index
///     is what makes THAT idempotent, not this claim — see
///     `pg_tests::a_new_event_for_an_already_granted_period_grants_nothing_extra`)
///     DOES reach `close_open_period` with a `boundary` that equals an
///     existing row's `period_start` exactly. `close_open_period` is written
///     to match neither of its statements against that exact equality — see
///     `DELETE_UNSTARTED_OPEN_PERIOD_SQL`'s doc comment for why `>=` there
///     was tried and measured to delete the very row `ON CONFLICT DO
///     NOTHING` needs intact, silently turning the idempotent re-run into a
///     second grant.
async fn apply_invoice_paid(
    control: &sqlx::PgPool,
    account_id: Uuid,
    event_id: &str,
    event_type: &str,
    invoice_subscription_id: &str,
    period_start: chrono::DateTime<chrono::Utc>,
    period_end: chrono::DateTime<chrono::Utc>,
) -> Result<Option<InvoicePaidOutcome>, sqlx::Error> {
    let mut tx = crate::tenancy::scope::begin_account_scoped(control, account_id)
        .await
        .map_err(|e| sqlx::Error::Protocol(e.to_string()))?;

    // The claim. A concurrent duplicate delivery blocks here on the uncommitted
    // insert and then reads zero rows, so Postgres does the serialization.
    crate::billing::renewal::lock_period_rotation(&mut tx, account_id).await?;
    let claimed = sqlx::query(
        "INSERT INTO stripe_webhook_events (event_id, event_type, account_id) \
         VALUES ($1, $2, $3) ON CONFLICT (event_id) DO NOTHING",
    )
    .bind(event_id)
    .bind(event_type)
    .bind(account_id)
    .execute(&mut *tx)
    .await?
    .rows_affected();

    if claimed == 0 {
        // Replay. Roll back rather than commit: the transaction has done
        // nothing worth keeping, and committing would rewrite `received_at`
        // semantics for a row we do not own.
        tx.rollback().await?;
        return Ok(None);
    }

    // Take the SAME per-account advisory lock `seed_member_period` takes
    // (finding 1, enforcement-completion PR review round 2) — moved to run
    // BEFORE the staleness guard's `stripe_subscriptions ... FOR UPDATE`
    // below (CodeRabbit re-review, finding 2: MAJOR), not merely before
    // `close_open_period`/`roll_period` as it originally was.
    //
    // # Why: a lock-ordering deadlock, not just a missing lock
    //
    // With the lock taken AFTER the staleness guard (the original placement),
    // this function's own lock-acquisition order was "row lock, then
    // advisory lock". `apply_subscription_deleted`'s forfeit path takes the
    // SAME advisory lock and then goes on to UPDATE `stripe_subscriptions`
    // (an implicit row lock) — "advisory lock, then row lock", the exact
    // REVERSE order. Two concurrent deliveries for the same account — one
    // hitting each handler — can each hold the lock the other one waits on:
    // this function holds the `stripe_subscriptions` row lock and blocks on
    // the advisory lock, while `apply_subscription_deleted` holds the
    // advisory lock and blocks on the same row. Postgres detects the cycle
    // and aborts one side with a `40P01` deadlock error, which this
    // function's caller turns into a 500 that asks Stripe to redeliver — a
    // spurious failure on perfectly valid, just unluckily-timed concurrent
    // events. Taking the advisory lock FIRST here, uniformly with the other
    // two period-rotating handlers, makes "advisory lock before any
    // `stripe_subscriptions` row lock OR `grants` row write" the one order
    // every site uses, so no cycle can form — `apply_subscription_deleted`'s
    // forfeit path (CodeRabbit round-4 re-review) is the `grants`-row-write
    // half of that invariant; see its own lock-acquisition comment for the
    // matching cycle that closes.
    //
    // The staleness guard itself is unmoved — it still runs, still short-
    // circuits, and still returns `Stale` before any destructive work below.
    // The only behavioral cost of taking the lock first is that a stale or
    // replayed-under-a-new-event-id delivery now briefly holds the advisory
    // lock for the duration of the (cheap) `FOR UPDATE` read and the
    // stale-outcome UPDATE, rather than never taking it at all — negligible
    // next to the deadlock it closes.
    crate::billing::renewal::lock_period_rotation(&mut tx, account_id).await?;

    // ── Staleness guard — see the module doc's "Out-of-order delivery"
    // section. `FOR UPDATE` locks the row for the rest of this transaction,
    // so a concurrent delivery for the SAME account (a different event id —
    // the claim above only serializes THIS one) cannot read the pre-update
    // value and race this check. Runs AFTER the advisory lock above (finding
    // 2) rather than before it — see that comment for why the order matters.
    //
    // `stripe_subscription_id`/`status` are read in the SAME `FOR UPDATE`
    // query (finding 3, billing self-serve PR D review round) rather than a
    // second SELECT, so the subscription-identity guard just below observes
    // the identical locked snapshot as the period guard, with no window for
    // a concurrent `customer.subscription.deleted` to change either between
    // the two reads.
    let (stored_period_start, stored_subscription_id, stored_status): (
        Option<chrono::DateTime<chrono::Utc>>,
        Option<String>,
        String,
    ) = sqlx::query_as(
        "SELECT current_period_start, stripe_subscription_id, status \
           FROM stripe_subscriptions WHERE account_id = $1 FOR UPDATE",
    )
    .bind(account_id)
    .fetch_one(&mut *tx)
    .await?;

    if let Some(stored) = stored_period_start {
        if period_start < stored {
            // Superseded. No close_open_period, no roll_period, no period
            // mirror update — see the module doc for why touching any of
            // them here would be wrong, not merely unnecessary. The claim
            // stays committed so this event id can never re-trigger this
            // check.
            let outcome = InvoicePaidOutcome::Stale {
                incoming_period_start: period_start,
                stored_period_start: stored,
            };
            sqlx::query("UPDATE stripe_webhook_events SET outcome = $2 WHERE event_id = $1")
                .bind(event_id)
                .bind(stale_summary(period_start, stored))
                .execute(&mut *tx)
                .await?;
            tx.commit().await?;
            return Ok(Some(outcome));
        }
    }

    // ── Subscription-identity guard (finding 3, billing self-serve PR D
    // review round) ──
    //
    // Two cases, both meaning "this paid invoice must NOT restore `status =
    // 'active'` or roll a fresh period":
    //
    //   1. **Id mismatch** — the invoice's subscription id does not match
    //      `stripe_subscriptions.stripe_subscription_id`. Reachable when an
    //      account cancels and later resubscribes: the SAME account row now
    //      points at a NEW Stripe subscription, but a late/retried
    //      `invoice.paid` for the OLD (superseded) subscription can still
    //      arrive under Stripe's at-least-once, not-necessarily-ordered
    //      delivery. Applying it would un-freeze — or worse, roll a fresh
    //      allowance period for — an account based on money paid against a
    //      subscription that no longer exists as far as this account is
    //      concerned.
    //   2. **Id matches, but the stored `status` is already `"canceled"`** —
    //      the FINAL invoice Stripe sometimes issues after a deliberate
    //      cancellation, for the same subscription id, arriving AFTER
    //      `customer.subscription.deleted` already froze this account.
    //      Freeze-not-forfeit (plan §1.11) means the purchased pool survives
    //      but stays unspendable until resubscribe — restoring `status =
    //      "active"` here would silently un-freeze an account the founder's
    //      model says should stay frozen until a NEW checkout, and rolling a
    //      fresh period would hand out a NEW allowance nobody actually paid
    //      for going forward (this invoice covers a period that already
    //      ended in cancellation).
    //
    // **`None` (no `stripe_subscription_id` stored yet) is deliberately
    // PERMISSIVE**, not treated as a mismatch — same "absence is permissive"
    // posture `rolls_the_renewal_clock` documents for a missing
    // `billing_reason`. `routes::checkout`'s subscription-mode completion
    // writes `stripe_subscription_id` in the SAME webhook module (`checkout
    // .session.completed`), and Stripe's delivery contract gives no ordering
    // guarantee against `invoice.paid` for the same account — refusing every
    // invoice until that write has landed would turn a benign delivery race
    // into a dropped renewal, exactly the failure mode this guard exists to
    // prevent, not cause. `past_due -> active` on a same-id payment is
    // UNCHANGED by this guard — that is the legitimate involuntary-lapse
    // recovery this module has always performed.
    if let Some(stored_sub_id) = stored_subscription_id.as_deref() {
        if stored_sub_id != invoice_subscription_id {
            let reason = format!(
                "invoice subscription id {invoice_subscription_id} does not match stored \
                 stripe_subscription_id {stored_sub_id} — stale event for a superseded \
                 subscription"
            );
            sqlx::query("UPDATE stripe_webhook_events SET outcome = $2 WHERE event_id = $1")
                .bind(event_id)
                .bind(format!("skipped {reason}"))
                .execute(&mut *tx)
                .await?;
            tx.commit().await?;
            return Ok(Some(InvoicePaidOutcome::SupersededSubscription { reason }));
        }
        if stored_status == "canceled" {
            let reason = "matching subscription id, but stored status is already 'canceled' — \
                           deliberate-cancel final invoice, not restoring to active"
                .to_string();
            sqlx::query("UPDATE stripe_webhook_events SET outcome = $2 WHERE event_id = $1")
                .bind(event_id)
                .bind(format!("skipped {reason}"))
                .execute(&mut *tx)
                .await?;
            tx.commit().await?;
            return Ok(Some(InvoicePaidOutcome::SupersededSubscription { reason }));
        }
    }

    // The per-account advisory lock (`renewal::lock_period_rotation`) that
    // guards this close-then-roll sequence — the double-grant race
    // `seed_member_period` needs it against, see that function's own doc
    // comment for the full mechanism — was already taken above, before the
    // staleness guard (finding 2). No second acquisition is needed here:
    // `pg_advisory_xact_lock` is held for the rest of the transaction.
    //
    // Close any period this account is still holding open as of the
    // INCOMING period's start, before seeding the new one — see this
    // function's own doc comment ("Closing the previous period before
    // seeding this one") for why 0163 makes this mandatory rather than
    // defensive.
    let closed =
        crate::billing::renewal::close_open_period(&mut tx, account_id, period_start).await?;

    let outcome =
        crate::billing::renewal::roll_period(&mut tx, account_id, period_start, period_end).await?;

    // Mirror what Stripe last told us the period is. Deliberately NOT collapsed
    // into `member_allowance_periods`' own bounds — 0162's header explains why
    // the two must stay independently recorded.
    //
    // `status = 'active'` is also the RESTORE half of the founder's
    // "involuntary lapse → frozen, then restored": a `past_due` account that
    // pays its invoice is active again by virtue of having paid. The freeze
    // half lands in Phase 4b; this side of it is correct on its own.
    sqlx::query(
        "UPDATE stripe_subscriptions \
            SET status = 'active', current_period_start = $2, current_period_end = $3, \
                updated_at = now() \
          WHERE account_id = $1",
    )
    .bind(account_id)
    .bind(period_start)
    .bind(period_end)
    .execute(&mut *tx)
    .await?;

    // `closed`'s summary is prefixed only when it actually did something —
    // the normal case is nothing was still open, and every prior record's
    // `outcome` column already reads as `renewed ...`; noising up the common
    // case would make a grep for something ELSE harder without buying
    // anything.
    let recorded_outcome = if closed.changed() {
        format!("{} {}", closed.summary(), outcome.summary())
    } else {
        outcome.summary()
    };
    sqlx::query("UPDATE stripe_webhook_events SET outcome = $2 WHERE event_id = $1")
        .bind(event_id)
        .bind(recorded_outcome)
        .execute(&mut *tx)
        .await?;

    tx.commit().await?;
    Ok(Some(InvoicePaidOutcome::Applied(outcome)))
}

/// What one [`apply_subscription_updated`] call did — reported rather than
/// interpreted, same discipline as [`crate::billing::renewal::RenewalOutcome`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SubscriptionUpdateOutcome {
    /// Whether the account's CURRENT grant is seat-priced at all. `false` is
    /// a normal, expected outcome (a flat-plan account can still have its
    /// Stripe subscription's `quantity` reported as 1) — NOT a failure, so
    /// this is reported rather than escalated.
    pub seat_priced: bool,
    /// **For a SEAT-PRICED grant, what was actually PERSISTED to
    /// `grants.seats` by the end of this call — not necessarily the raw
    /// Stripe-reported `quantity` (review round 2, finding 3).** For a
    /// `team_standard`/`team_premium` grant (or any non-`team` seat-priced
    /// plan) these agree: the Stripe quantity IS what gets written. For a
    /// `team` grant they can legitimately differ — see the doc comment at
    /// this function's seat-sync step for why a `team` grant's
    /// `grants.seats` is a purchase-ledger total and Stripe's reported
    /// quantity is deliberately NOT written for it. Reading `renewal.seats` (the
    /// post-reconciliation DB value, in the same transaction) rather than
    /// echoing back the raw parameter is what makes this field trustworthy
    /// for the audit trail on those plans.
    ///
    /// **For a FLAT plan (`seat_priced == false`) this field is NOT
    /// `grants.seats`** — that column is NULL for a flat grant by the
    /// `grants_seats_iff_seat_priced` CHECK (`0156`), so there is nothing to
    /// read back. It is the raw Stripe-reported `quantity` unchanged (and
    /// the same is true when there is no active grant at all). A caller can
    /// take this number at face value as "what `grants.seats` holds now"
    /// only when `seat_priced` is `true`.
    pub seats: i32,
    pub status: &'static str,
    pub renewal: crate::billing::renewal::RenewalOutcome,
    pub closed: crate::billing::renewal::ClosePeriodOutcome,
}

impl SubscriptionUpdateOutcome {
    fn summary(&self) -> String {
        format!(
            "subscription_updated seat_priced={} seats={} status={} {} {}",
            self.seat_priced,
            self.seats,
            self.status,
            self.closed.summary(),
            self.renewal.summary()
        )
    }
}

/// Claim the event, sync seats, grant a mid-period seat add in full, and map
/// the status onto `stripe_subscriptions.status` — all in one transaction.
/// See [`apply_invoice_paid`]'s doc comment for why the claim and the work
/// must never be split across a commit boundary; the same reasoning applies
/// here unchanged.
///
/// # Why seats are synced BEFORE the roll, not after
///
/// [`crate::billing::renewal::roll_period`] reads the CURRENT grant's seat
/// count to derive the per-member share
/// (`COALESCE(credits_override / seats, plan.credits)`). Writing the new
/// seat count first means a `credits_override` account's per-member share is
/// recomputed against the count Stripe just reported, so a newly-added
/// member is granted the correct (already-updated) share rather than one
/// computed against the stale count.
///
/// **Exception (review round 2, finding 3; comparison target changed in
/// billing self-serve PR F): a `team` grant is deliberately EXCLUDED from
/// this sync.** `grants.seats` for a `team` grant is now a PURCHASE LEDGER
/// total (`billing::seat_lifecycle::acquire_seat`, `routes::org_slots`'
/// admin buy/reduce actions, `routes::billing_seats`' tier-change route,
/// and `billing::renewal`'s own pending-downgrade move — see that module's
/// "`grant_seat_tiers` is a purchase-driven ledger" doc section), not
/// roster-derived any more —
/// blindly overwriting it with Stripe's raw reported `quantity` here would
/// clobber a value this handler's OWN outbound pushes (via the two modules
/// above) already keep correct, the moment `apply_subscription_updated`
/// fires for the very webhook THOSE pushes triggered. So a `team` grant is
/// excluded from the direct-UPDATE sync outright, same as before PR F —
/// what changed is what the drift check just below now compares Stripe's
/// quantity AGAINST: the purchase ledger (`Σ grant_seat_tiers.seats`), not
/// the roster. See that check's own comment for why.
///
/// # Why seat sync is a targeted, guarded UPDATE
///
/// `0156`'s `grants_seats_iff_seat_priced` CHECK
/// (`plan_is_seat_priced = (seats IS NOT NULL)`) makes writing a seat count
/// onto a flat plan's grant a hard INSERT/UPDATE error. The lookup below
/// resolves the account's CURRENT grant (same "most recently granted,
/// non-revoked" selection `roll_period`'s own `CURRENT_GRANT_SQL` uses) and
/// only issues the seat-count UPDATE for a non-`team` seat-priced grant (see
/// the exception above for `team`), so a flat-plan account's
/// `customer.subscription.updated` event correctly touches ZERO rows rather
/// than tripping the CHECK.
///
/// # Why `close_open_period` runs even though this is not a period ROLL
///
/// Mirrors `apply_invoice_paid`'s wiring exactly, for the identical reason:
/// `0163`'s exclusion constraint makes any INSERT that would overlap an
/// already-open period a hard error, and `close_open_period` is a no-op
/// (see its own doc comment) for the ordinary case where the reported
/// `period_start` equals the already-open period's start exactly — which is
/// what a same-period seat change actually looks like. It only does
/// something on the less-ordinary case where Stripe's reported period has
/// genuinely moved.
///
/// # Staleness guard
///
/// Runs BEFORE the seat-sync UPDATE — see the module doc's "Out-of-order
/// delivery" section for why an older incoming period must never reach
/// EITHER the seat sync or `close_open_period` here. Returns
/// [`ClaimedOutcome::Stale`] rather than proceeding when the guard trips.
#[cfg(test)]
async fn apply_subscription_updated(
    control: &sqlx::PgPool,
    account_id: Uuid,
    event_id: &str,
    event_type: &str,
    seats: i32,
    status: &'static str,
    period_start: chrono::DateTime<chrono::Utc>,
    period_end: chrono::DateTime<chrono::Utc>,
) -> Result<Option<ClaimedOutcome<SubscriptionUpdateOutcome>>, sqlx::Error> {
    apply_subscription_updated_with_cancel(
        control, account_id, event_id, event_type, seats, status, period_start, period_end, None,
    )
    .await
}

/// The cancellation fields a `customer.subscription.*` event carries, to be
/// mirrored onto `stripe_subscriptions` (settings launch PR 3.3).
#[derive(Debug, Clone)]
struct CancelMirror {
    /// The subscription the fields describe. The write is skipped when the
    /// account has since moved to a different subscription.
    subscription_id: String,
    cancel_at_period_end: bool,
    cancel_at: Option<chrono::DateTime<chrono::Utc>>,
}

/// [`apply_subscription_updated`] plus the cancellation mirror. `cancel =
/// None` leaves the stored cancellation fields untouched.
///
/// The mirror is written only on the non-stale path, in the same transaction
/// as the claim, so a replay (claim conflict) and a superseded-period event
/// both leave it alone.
#[allow(clippy::too_many_arguments)]
async fn apply_subscription_updated_with_cancel(
    control: &sqlx::PgPool,
    account_id: Uuid,
    event_id: &str,
    event_type: &str,
    seats: i32,
    status: &'static str,
    period_start: chrono::DateTime<chrono::Utc>,
    period_end: chrono::DateTime<chrono::Utc>,
    cancel: Option<&CancelMirror>,
) -> Result<Option<ClaimedOutcome<SubscriptionUpdateOutcome>>, sqlx::Error> {
    let mut tx = crate::tenancy::scope::begin_account_scoped(control, account_id)
        .await
        .map_err(|e| sqlx::Error::Protocol(e.to_string()))?;

    crate::billing::renewal::lock_period_rotation(&mut tx, account_id).await?;
    let claimed = sqlx::query(
        "INSERT INTO stripe_webhook_events (event_id, event_type, account_id) \
         VALUES ($1, $2, $3) ON CONFLICT (event_id) DO NOTHING",
    )
    .bind(event_id)
    .bind(event_type)
    .bind(account_id)
    .execute(&mut *tx)
    .await?
    .rows_affected();

    if claimed == 0 {
        tx.rollback().await?;
        return Ok(None);
    }

    // Take the SAME per-account advisory lock `seed_member_period` takes
    // (finding 1, enforcement-completion PR review round 2) — moved to run
    // BEFORE the staleness guard's `stripe_subscriptions ... FOR UPDATE`
    // below, not just before the seat-sync UPDATE/close-then-roll as it
    // originally was (CodeRabbit re-review, finding 2: MAJOR). See
    // `apply_invoice_paid`'s identical comment, immediately above its own
    // lock acquisition, for the full lock-ordering-deadlock mechanism this
    // closes: with the row lock taken first here and
    // `apply_subscription_deleted`'s forfeit path taking the advisory lock
    // first (as of CodeRabbit round-4 re-review — see that path's own
    // lock-acquisition comment; it used to revoke `grants` rows before
    // taking this same lock, which was the identical cycle one write ahead),
    // two concurrent deliveries for the same account could each hold what
    // the other waits on, and Postgres would abort one with `40P01` rather
    // than let either finish. Taking the advisory lock first, uniformly with
    // the other two period-rotating handlers, removes the cycle: the
    // invariant is now "advisory lock before any `stripe_subscriptions` row
    // lock OR `grants` row write", held at every period-rotating site in
    // this file. The staleness guard is otherwise unmoved: it still short-
    // circuits before the seat sync and before `close_open_period`/
    // `roll_period`, at the cost of a stale/replayed event now briefly
    // holding the advisory lock too.
    crate::billing::renewal::lock_period_rotation(&mut tx, account_id).await?;

    // ── Staleness guard — see the module doc's "Out-of-order delivery"
    // section and `apply_invoice_paid`'s identical guard. Deliberately runs
    // BEFORE the seat-sync UPDATE below, not just before `close_open_period`:
    // a stale event's seat count is exactly as stale as its period, so
    // syncing seats from it would silently roll `grants.seats` BACKWARD to
    // whatever it was before a since-applied, newer seat change. Runs AFTER
    // the advisory lock above (finding 2) rather than before it.
    let stored_period_start: Option<chrono::DateTime<chrono::Utc>> = sqlx::query_scalar(
        "SELECT current_period_start FROM stripe_subscriptions WHERE account_id = $1 FOR UPDATE",
    )
    .bind(account_id)
    .fetch_one(&mut *tx)
    .await?;

    if let Some(stored) = stored_period_start {
        if period_start < stored {
            let outcome = ClaimedOutcome::Stale {
                incoming_period_start: period_start,
                stored_period_start: stored,
            };
            sqlx::query("UPDATE stripe_webhook_events SET outcome = $2 WHERE event_id = $1")
                .bind(event_id)
                .bind(stale_summary(period_start, stored))
                .execute(&mut *tx)
                .await?;
            tx.commit().await?;
            return Ok(Some(outcome));
        }
    }

    // **Review round 2, finding 3.** `grants.seats` for a `team` grant is
    // ROSTER-derived — `billing::renewal::roll_period`'s own
    // `grant_seat_tiers` reconciliation overwrites whatever this UPDATE
    // would write, in the SAME transaction, at the `roll_period(...)` call a
    // few lines below. Writing Stripe's `quantity` here for a `team` grant
    // was therefore already silently discarded work (harmless today only
    // because nothing reads `grants.seats` in the brief window between the
    // two writes) — but it stops being harmless once PR F wires REAL,
    // synchronous seat purchases through this exact handler: at that point
    // silently overwriting-then-discarding Stripe's reported quantity would
    // hide a genuine drift between what Stripe billed and what the roster
    // says, right where an operator most needs it surfaced. So a `team`
    // grant is excluded from this UPDATE outright — the roster
    // (`account_members`, reconciled by `roll_period` below) is this plan's
    // source of truth per plan §1.6 ("Seats == members, always... every
    // invite IS a seat purchase"), and any disagreement between it and
    // Stripe's reported quantity is logged rather than silently overwritten
    // in either direction.
    // Same "most recently granted, non-revoked" selection, with the same
    // deterministic tie-break, as `billing::renewal`'s `CURRENT_GRANT_SQL`
    // and `billing::enforcement`'s `read_member_period_scoped` lateral: two
    // grants issued in the same tx-stable `now()` tie on `granted_at`, so
    // `plan_is_seat_priced DESC, g.id DESC` (not time-ordered, just a
    // deterministic pick) ensures every grant-resolution site in the
    // codebase agrees on the SAME "current" grant.
    let current_grant: Option<(Uuid, bool, bool)> = sqlx::query_as(
        "SELECT g.id, g.plan_is_seat_priced, (p.code = $2) AS is_team_plan \
           FROM grants g JOIN plans p ON p.id = g.plan_id \
          WHERE g.account_id = $1 AND g.revoked_at IS NULL \
          ORDER BY g.granted_at DESC, g.plan_is_seat_priced DESC, g.id DESC LIMIT 1",
    )
    .bind(account_id)
    .bind(crate::billing::MIXED_TIER_PLAN_CODE)
    .fetch_optional(&mut *tx)
    .await?;

    let seat_priced = match current_grant {
        Some((grant_id, plan_is_seat_priced, true)) => {
            // `team` (billing self-serve PR F): compare Stripe's reported
            // quantity against the PURCHASE LEDGER total
            // (`Σ grant_seat_tiers.seats` for this grant) rather than the
            // roster — see the doc comment above. This is now a MEANINGFUL
            // drift signal, not a placeholder: `grant_seat_tiers` is
            // incrementally maintained by `billing::seat_lifecycle`/
            // `routes::billing_seats`, which are the ONLY code paths that
            // ever push a Stripe quantity change for a `team` subscription
            // — so in steady state this handler's own `seats` parameter
            // should always equal what this account's ledger already
            // records (the webhook this handler is processing was, in the
            // common case, TRIGGERED by one of those two pushes). A
            // disagreement means either a purchase/release that changed
            // Stripe but never committed its local write (a crash between
            // the two — see `seat_lifecycle`'s own doc comment, "Stripe push
            // BEFORE the DB write, always"), or an out-of-band Stripe
            // Dashboard edit. Still never written here — a `team` grant's
            // `grants.seats` stays the ledger's own Σ, updated only by the
            // ledger's own writers, never by this raw Stripe passthrough.
            let ledger_seats: i64 = sqlx::query_scalar(
                "SELECT COALESCE(SUM(seats), 0) FROM grant_seat_tiers WHERE grant_id = $1",
            )
            .bind(grant_id)
            .fetch_one(&mut *tx)
            .await?;
            if i64::from(seats) != ledger_seats {
                tracing::warn!(
                    account_id = %account_id, stripe_quantity = seats, ledger_seats,
                    "apply_subscription_updated: Stripe-reported seat quantity disagrees with \
                     the purchase-ledger total (Σ grant_seat_tiers.seats) for a `team` grant — \
                     the ledger (maintained by billing::seat_lifecycle/routes::billing_seats) is \
                     this plan's source of truth; skipping the grants.seats UPDATE rather than \
                     fighting those writers"
                );
            }
            plan_is_seat_priced
        }
        Some((grant_id, true, false)) => {
            // A non-`team` seat-priced grant (`team_standard`/
            // `team_premium`) — `roll_period` never touches `grants.seats`
            // for these, so Stripe's reported quantity is still this
            // column's real source of truth. Unchanged behavior.
            sqlx::query("UPDATE grants SET seats = $2 WHERE id = $1")
                .bind(grant_id)
                .bind(seats)
                .execute(&mut *tx)
                .await?;
            true
        }
        _ => false,
    };

    // Mid-period seat add: grant the FULL current-period allowance
    // immediately, never a prorated slice — the founder's locked decision.
    // Members who already hold a row for this period conflict on `0161`'s
    // UNIQUE index and are skipped (`ON CONFLICT DO NOTHING`); a genuinely
    // new member is not, and is granted in full. See
    // `billing::renewal::pg_tests::a_member_added_mid_period_is_granted_in_full_on_the_next_roll`,
    // which names THIS event as the deliberate trigger for the property it
    // proves.
    //
    // The per-account advisory lock (`renewal::lock_period_rotation`) that
    // guards this close-then-roll sequence against the double-grant race
    // `seed_member_period` needs it against was already taken above, before
    // the staleness guard (finding 2). No second acquisition needed here.
    //
    // **Same period ⇒ do not re-slice it.** An event that reports the billing
    // period the account is ALREADY in (a seat-quantity edit made out of band
    // in the Dashboard, a status flip) changes no period boundary, so every
    // member's current allowance period stays exactly as it is.
    // `close_open_period` is for a boundary that MOVED: it deletes any period
    // starting after the boundary, and a member who joined mid-period holds
    // exactly such a row (it starts at their join time) with consumption
    // recorded against it. Running it here dropped those periods and left the
    // members at `member_cap_reached`. Members with no period yet (a seat added
    // mid-period) are still granted in full.
    let same_period = stored_period_start == Some(period_start);
    let (closed, renewal) = if same_period {
        (
            crate::billing::renewal::ClosePeriodOutcome::default(),
            crate::billing::renewal::roll_period_keeping_open(&mut tx, account_id, period_start, period_end).await?,
        )
    } else {
        let closed = crate::billing::renewal::close_open_period(&mut tx, account_id, period_start).await?;
        let renewal = crate::billing::renewal::roll_period(&mut tx, account_id, period_start, period_end).await?;
        (closed, renewal)
    };

    sqlx::query(
        "UPDATE stripe_subscriptions \
            SET status = $2, current_period_start = $3, current_period_end = $4, updated_at = now() \
          WHERE account_id = $1",
    )
    .bind(account_id)
    .bind(status)
    .bind(period_start)
    .bind(period_end)
    .execute(&mut *tx)
    .await?;

    if let Some(cancel) = cancel {
        // Same identity rule as `apply_subscription_deleted`: an event about
        // a superseded subscription must not mark the current one as
        // cancelling.
        sqlx::query(
            "UPDATE stripe_subscriptions SET cancel_at_period_end = $2, cancel_at = $3 \
              WHERE account_id = $1 \
                AND (stripe_subscription_id IS NULL OR stripe_subscription_id = $4)",
        )
        .bind(account_id)
        .bind(cancel.cancel_at_period_end)
        .bind(cancel.cancel_at)
        .bind(&cancel.subscription_id)
        .execute(&mut *tx)
        .await?;
    }

    // **Review round 2, finding 3 (round-2 re-review correction).** Report
    // what `renewal.seats` says was ACTUALLY persisted — the post-
    // `roll_period` DB value, read back in the same transaction — rather
    // than echoing the raw `seats` parameter Stripe sent, but ONLY for a
    // seat-priced grant (`seat_priced`, computed above). For a non-`team`
    // seat-priced grant `renewal.seats` and the raw parameter agree (the
    // UPDATE above wrote exactly `seats`); for a `team` grant they can
    // legitimately differ, per the seat-sync step's own doc comment.
    //
    // A FLAT plan must NOT read `renewal.seats`: `RenewalOutcome::seats`
    // is `COALESCE(grants.seats, 1)`, and `grants.seats` is NULL for a flat
    // grant by the `grants_seats_iff_seat_priced` CHECK (`0156`) — so
    // `renewal.seats` there is always the filler constant `1`, never
    // anything actually persisted for this account. Reading it would
    // silently discard whatever quantity Stripe genuinely reported. The
    // raw parameter is the only real value available for a flat plan, so it
    // is reported unchanged (also covers the "no active grant at all" case,
    // where `seat_priced` is `false` and `renewal.seats` is `None` anyway).
    let persisted_seats = if seat_priced {
        match renewal.seats {
            Some(s) => i32::try_from(s).map_err(|_| {
                tracing::error!(
                    event_id, %account_id, renewal_seats = s,
                    "apply_subscription_updated: renewal.seats does not fit in i32 — \
                     grants.seats is INT4, so this can only mean DB/decode corruption"
                );
                sqlx::Error::Protocol(format!(
                    "renewal.seats {s} out of range for grants.seats' INT4 column"
                ))
            })?,
            None => seats,
        }
    } else {
        seats
    };
    let outcome = SubscriptionUpdateOutcome {
        seat_priced,
        seats: persisted_seats,
        status,
        renewal,
        closed,
    };

    sqlx::query("UPDATE stripe_webhook_events SET outcome = $2 WHERE event_id = $1")
        .bind(event_id)
        .bind(outcome.summary())
        .execute(&mut *tx)
        .await?;

    tx.commit().await?;
    Ok(Some(ClaimedOutcome::Applied(outcome)))
}

/// What one [`apply_subscription_deleted`] call did.
///
/// **Freeze-not-forfeit (billing self-serve plan §1.11) — supersedes Phase
/// 4b's forfeit/freeze split entirely.** Every `customer.subscription.deleted`
/// event now does exactly the same thing regardless of
/// `cancellation_details.reason`: `stripe_subscriptions.status` flips to
/// `"canceled"` and nothing else is touched. There is no longer a
/// `forfeited`/`amount_forfeited` field to report — a caller that needs to
/// know whether an account is currently unable to spend its purchased pool
/// reads `stripe_subscriptions.status` (or, on the request path,
/// [`crate::billing::enforcement::is_blocked`], which now gates the pool on
/// exactly this column — see that module's `subscription_frozen`).
///
/// **Two shapes as of finding 6 (billing self-serve PR D review round).**
/// The original single-struct shape assumed every claimed
/// `customer.subscription.deleted` event freezes — true only when the
/// event's OWN subscription id still matches what
/// `stripe_subscriptions.stripe_subscription_id` currently records for this
/// account. A late-arriving deletion for a SUPERSEDED subscription (the
/// account canceled, then resubscribed BEFORE the old subscription's
/// deletion event was delivered — Stripe's at-least-once, not-necessarily-
/// ordered contract) must NOT freeze the freshly-resubscribed, currently
/// paying account.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SubscriptionDeleteOutcome {
    /// The event's subscription id matched (or nothing was stored yet to
    /// compare against — see [`apply_subscription_deleted`]'s own comment
    /// for why that reads as permissive here, the OPPOSITE default direction
    /// from `apply_invoice_paid`'s identity guard) — `stripe_subscriptions
    /// .status` was flipped to `"canceled"`.
    Frozen {
        status: &'static str,
        /// `cancellation_details.reason` as Stripe reported it, or `None` for
        /// a natural (non-cancelled) expiry — carried through for the
        /// recorded `stripe_webhook_events.outcome` audit trail only; see
        /// [`cancellation_reason`]'s own doc comment for why nothing
        /// branches on it any more.
        reason: Option<String>,
    },
    /// The event's subscription id does NOT match the one currently stored
    /// for this account — a late deletion for a subscription this account
    /// has already moved past. Nothing was written; the claim still
    /// committed so a retry of this exact `event_id` is an ordinary replay.
    Superseded {
        deleted_subscription_id: String,
        stored_subscription_id: Option<String>,
    },
}

impl SubscriptionDeleteOutcome {
    fn summary(&self) -> String {
        match self {
            Self::Frozen { status, reason } => format!(
                "subscription_deleted frozen status={status} reason={}",
                reason.as_deref().unwrap_or("<none>")
            ),
            Self::Superseded { deleted_subscription_id, stored_subscription_id } => format!(
                "subscription_deleted superseded deleted_subscription_id={deleted_subscription_id} \
                 stored_subscription_id={}",
                stored_subscription_id.as_deref().unwrap_or("<none>")
            ),
        }
    }
}

/// Claim the event and freeze, all in one transaction — see
/// [`apply_invoice_paid`]'s doc comment for why the claim and the work can
/// never be split across a commit boundary (a redelivered event must never
/// redo work that already happened).
///
/// # Freeze-not-forfeit, unconditionally (plan §1.11)
///
/// Every deletion — whatever `cancellation_details.reason` says, or its
/// absence for a natural expiry — freezes identically: `stripe_subscriptions
/// .status` becomes `"canceled"`. This supersedes the Phase 4b forfeit arm
/// that used to zero `credit_balances` and write an
/// [`ENTRY_KIND_FORFEIT`]-tagged debit on a deliberate cancellation
/// (`cancellation_details.reason == "cancellation_requested"`) — that code
/// path is retired outright, not merely disabled: purchased credits survive
/// EVERY cancellation now, unspendable until the account resubscribes. See
/// [`crate::billing::enforcement`]'s `subscription_frozen` for the read-side
/// half of "unspendable" — this function only ever WRITES the status; the
/// gate that makes it mean something lives there.
///
/// # Nothing else is touched
///
/// No `grants` revoke, no `close_open_period` call, no `credit_balances`
/// write. A member's CURRENT `member_allowance_periods` row (already
/// granted, from before cancellation) is left exactly as it was and simply
/// runs out at its own `period_end` — no more `invoice.paid` will ever
/// arrive to roll a fresh one once the subscription is gone. This mirrors
/// the ORIGINAL (Phase 4b) freeze arm's own reasoning, now applied
/// universally rather than to only the involuntary-lapse case: "the account
/// can still pay its way back to active, and there is nothing to revoke or
/// close for that path" — a resubscribe is a fresh checkout
/// (`routes::checkout`), which pre-creates against the SAME
/// `stripe_subscriptions` row (same `account_id` primary key) and the
/// subsequent `checkout.session.completed`/`invoice.paid` pair restores
/// `status = "active"` exactly like `apply_invoice_paid` already restores an
/// involuntary `past_due` lapse.
///
/// # Subscription-identity guard (finding 6, billing self-serve PR D review
/// # round)
///
/// The UPDATE below now filters on `stripe_subscription_id` too, not just
/// `account_id`. Without it, a late `customer.subscription.deleted` for a
/// SUPERSEDED subscription — the account canceled, then resubscribed via a
/// fresh checkout BEFORE the old subscription's deletion event finally
/// arrived — would freeze the freshly-resubscribed, currently PAYING
/// account, matching purely on `account_id` (the row's primary key, but not
/// the identity this event is actually about).
///
/// **`None` stored (no `stripe_subscription_id` linked yet) reads as a
/// MATCH, not a mismatch — the opposite default direction from
/// `apply_invoice_paid`'s identity guard.** That asymmetry is deliberate:
/// `apply_invoice_paid`'s guard protects against wrongly RESTORING access
/// (granting/un-freezing), so its permissive default leans toward "don't act
/// yet". This guard protects against wrongly WITHHOLDING a freeze, so its
/// permissive default leans the other way — an account with nothing stored
/// to compare against has no evidence this deletion is for a superseded
/// subscription, and freezing an account that should have stayed active
/// only delays it (fixed by any subsequent legitimate event), while failing
/// to freeze one that should be frozen leaves a purchased pool wrongly
/// spendable. Same "safer direction to be wrong in" reasoning
/// [`map_subscription_status`]'s catch-all arm already states for an
/// unrecognized Stripe status.
#[cfg(test)]
async fn apply_subscription_deleted(
    control: &sqlx::PgPool,
    account_id: Uuid,
    event_id: &str,
    event_type: &str,
    deleted_subscription_id: &str,
    cancellation_reason: Option<&str>,
) -> Result<Option<SubscriptionDeleteOutcome>, sqlx::Error> {
    apply_subscription_deleted_with_cancel(
        control, account_id, event_id, event_type, deleted_subscription_id, cancellation_reason, None,
    )
    .await
}

/// [`apply_subscription_deleted`] plus the cancellation mirror, written in
/// the freeze branch only (a superseded deletion touches nothing).
async fn apply_subscription_deleted_with_cancel(
    control: &sqlx::PgPool,
    account_id: Uuid,
    event_id: &str,
    event_type: &str,
    deleted_subscription_id: &str,
    cancellation_reason: Option<&str>,
    cancel: Option<&CancelMirror>,
) -> Result<Option<SubscriptionDeleteOutcome>, sqlx::Error> {
    let mut tx = crate::tenancy::scope::begin_account_scoped(control, account_id)
        .await
        .map_err(|e| sqlx::Error::Protocol(e.to_string()))?;

    crate::billing::renewal::lock_period_rotation(&mut tx, account_id).await?;

    let claimed = sqlx::query(
        "INSERT INTO stripe_webhook_events (event_id, event_type, account_id) \
         VALUES ($1, $2, $3) ON CONFLICT (event_id) DO NOTHING",
    )
    .bind(event_id)
    .bind(event_type)
    .bind(account_id)
    .execute(&mut *tx)
    .await?
    .rows_affected();

    if claimed == 0 {
        tx.rollback().await?;
        return Ok(None);
    }

    // `FOR UPDATE` locks the row for the rest of this transaction, so a
    // concurrent delivery for this same account cannot race this check —
    // same discipline as `apply_invoice_paid`'s staleness/identity guard.
    let stored_subscription_id: Option<String> = sqlx::query_scalar(
        "SELECT stripe_subscription_id FROM stripe_subscriptions WHERE account_id = $1 FOR UPDATE",
    )
    .bind(account_id)
    .fetch_one(&mut *tx)
    .await?;

    let outcome = if stored_subscription_id.is_none()
        || stored_subscription_id.as_deref() == Some(deleted_subscription_id)
    {
        const STATUS: &str = "canceled";

        sqlx::query(
            "UPDATE stripe_subscriptions SET status = $2, updated_at = now() \
              WHERE account_id = $1 AND (stripe_subscription_id = $3 OR stripe_subscription_id IS NULL)",
        )
        .bind(account_id)
        .bind(STATUS)
        .bind(deleted_subscription_id)
        .execute(&mut *tx)
        .await?;

        if let Some(cancel) = cancel {
            sqlx::query(
                "UPDATE stripe_subscriptions SET cancel_at_period_end = $2, cancel_at = $3 \
                  WHERE account_id = $1 \
                    AND (stripe_subscription_id = $4 OR stripe_subscription_id IS NULL)",
            )
            .bind(account_id)
            .bind(cancel.cancel_at_period_end)
            .bind(cancel.cancel_at)
            .bind(&cancel.subscription_id)
            .execute(&mut *tx)
            .await?;
        }

        SubscriptionDeleteOutcome::Frozen {
            status: STATUS,
            reason: cancellation_reason.map(str::to_string),
        }
    } else {
        // Superseded — see this function's own doc comment. No write to
        // `stripe_subscriptions` at all; the currently-linked subscription
        // is left exactly as it is.
        SubscriptionDeleteOutcome::Superseded {
            deleted_subscription_id: deleted_subscription_id.to_string(),
            stored_subscription_id: stored_subscription_id.clone(),
        }
    };

    sqlx::query("UPDATE stripe_webhook_events SET outcome = $2 WHERE event_id = $1")
        .bind(event_id)
        .bind(outcome.summary())
        .execute(&mut *tx)
        .await?;

    tx.commit().await?;
    Ok(Some(outcome))
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    const SECRET: &str = "whsec_test_do_not_use_in_production";
    const NOW: i64 = 1_700_000_000;

    /// Computes a genuinely valid signature by the same rule Stripe uses.
    ///
    /// Deliberately built from `hmac`/`sha2` directly rather than by calling
    /// the verifier's own internals: a helper that reused the production
    /// signing code would make every test below tautological — it would prove
    /// the verifier agrees with itself, which is true even when both are wrong.
    fn sign(secret: &str, timestamp: i64, body: &[u8]) -> String {
        let mut mac = HmacSha256::new_from_slice(secret.as_bytes()).unwrap();
        mac.update(timestamp.to_string().as_bytes());
        mac.update(b".");
        mac.update(body);
        hex::encode(mac.finalize().into_bytes())
    }

    fn headers(value: &str) -> HeaderMap {
        let mut h = HeaderMap::new();
        h.insert("stripe-signature", value.parse().unwrap());
        h
    }

    fn verify(header: &str, body: &[u8], now: i64) -> VerifyResult {
        verify_stripe_signature_with_secret(SECRET, &headers(header), body, now)
    }

    #[test]
    fn valid_signature_is_accepted() {
        let body = br#"{"id":"evt_1","type":"invoice.paid"}"#;
        let h = format!("t={NOW},v1={}", sign(SECRET, NOW, body));
        assert_eq!(verify(&h, body, NOW), VerifyResult::Ok);
    }

    /// The whole point of the endpoint's security: a forged `invoice.paid`
    /// grants credits, so a body edited after signing must not verify.
    #[test]
    fn tampered_body_is_rejected() {
        let signed = br#"{"id":"evt_1","type":"invoice.paid","amount":100}"#;
        let h = format!("t={NOW},v1={}", sign(SECRET, NOW, signed));
        let tampered = br#"{"id":"evt_1","type":"invoice.paid","amount":900}"#;
        assert_eq!(verify(&h, tampered, NOW), VerifyResult::SignatureMismatch);
    }

    #[test]
    fn wrong_secret_is_rejected() {
        let body = br#"{"id":"evt_1"}"#;
        let h = format!("t={NOW},v1={}", sign("whsec_someone_elses_secret", NOW, body));
        assert_eq!(verify(&h, body, NOW), VerifyResult::SignatureMismatch);
    }

    /// Replay protection. A captured request keeps its valid signature
    /// forever, so age is the only thing that can retire it.
    #[test]
    fn stale_timestamp_is_rejected_even_with_a_valid_signature() {
        let body = br#"{"id":"evt_1"}"#;
        let old = NOW - 3600;
        let h = format!("t={old},v1={}", sign(SECRET, old, body));
        assert_eq!(verify(&h, body, NOW), VerifyResult::TimestampInvalid);
    }

    #[test]
    fn far_future_timestamp_is_rejected() {
        let body = br#"{"id":"evt_1"}"#;
        let ahead = NOW + 3600;
        let h = format!("t={ahead},v1={}", sign(SECRET, ahead, body));
        assert_eq!(verify(&h, body, NOW), VerifyResult::TimestampInvalid);
    }

    /// The replay window's exact edges, in both directions. Asserted because
    /// an off-by-one here is invisible in every other test.
    #[test]
    fn replay_window_boundaries() {
        let body = br#"{"id":"evt_1"}"#;
        for (offset, expected) in [
            (REPLAY_TOLERANCE_SECS, VerifyResult::Ok),
            (REPLAY_TOLERANCE_SECS + 1, VerifyResult::TimestampInvalid),
            (-REPLAY_TOLERANCE_SECS, VerifyResult::Ok),
            (-REPLAY_TOLERANCE_SECS - 1, VerifyResult::TimestampInvalid),
        ] {
            let ts = NOW - offset;
            let h = format!("t={ts},v1={}", sign(SECRET, ts, body));
            assert_eq!(verify(&h, body, NOW), expected, "offset {offset}");
        }
    }

    /// A hostile `t=i64::MIN` makes `now - ts` overflow. Unguarded that panics
    /// in debug and WRAPS in release — and a wrapped skew lands inside the
    /// tolerance, turning a garbage timestamp into an accepted one.
    #[test]
    fn extreme_timestamps_do_not_overflow_into_acceptance() {
        let body = br#"{"id":"evt_1"}"#;
        for ts in [i64::MIN, i64::MAX] {
            let h = format!("t={ts},v1={}", sign(SECRET, ts, body));
            assert_eq!(verify(&h, body, NOW), VerifyResult::TimestampInvalid, "t={ts}");
        }
    }

    /// A secret roll leaves the old and new signatures both valid for a
    /// window, and Stripe sends BOTH. Stopping at the first `v1` would break
    /// every rotation — with the failure appearing only during the roll.
    #[test]
    fn any_of_several_v1_signatures_may_match() {
        let body = br#"{"id":"evt_1"}"#;
        let good = sign(SECRET, NOW, body);
        let stale = sign("whsec_the_previous_secret", NOW, body);
        assert_eq!(
            verify(&format!("t={NOW},v1={stale},v1={good}"), body, NOW),
            VerifyResult::Ok,
            "the matching signature is second"
        );
        assert_eq!(
            verify(&format!("t={NOW},v1={good},v1={stale}"), body, NOW),
            VerifyResult::Ok,
            "the matching signature is first"
        );
        assert_eq!(
            verify(&format!("t={NOW},v1={stale},v1={stale}"), body, NOW),
            VerifyResult::SignatureMismatch,
            "neither matches"
        );
    }

    /// `v0` is Stripe's test-mode-only legacy scheme and is NOT an HMAC over
    /// this signing base. Accepting one would be accepting a signature that
    /// was never verified — so a header carrying only `v0` must fail even
    /// when the `v0` value is itself well-formed.
    #[test]
    fn v0_only_header_is_rejected() {
        let body = br#"{"id":"evt_1"}"#;
        let h = format!("t={NOW},v0={}", sign(SECRET, NOW, body));
        assert_eq!(verify(&h, body, NOW), VerifyResult::SignatureMismatch);
    }

    #[test]
    fn missing_or_malformed_header_parts_are_rejected() {
        let body = br#"{"id":"evt_1"}"#;
        let good = sign(SECRET, NOW, body);

        // No header at all.
        assert_eq!(
            verify_stripe_signature_with_secret(SECRET, &HeaderMap::new(), body, NOW),
            VerifyResult::SignatureMismatch
        );
        // No `t=`.
        assert_eq!(
            verify(&format!("v1={good}"), body, NOW),
            VerifyResult::TimestampInvalid
        );
        // `t=` present but not a number.
        assert_eq!(
            verify(&format!("t=not-a-number,v1={good}"), body, NOW),
            VerifyResult::TimestampInvalid
        );
        // No `v1=` at all.
        assert_eq!(verify(&format!("t={NOW}"), body, NOW), VerifyResult::SignatureMismatch);
    }

    /// Wrong length and non-hex must both be mismatches, NOT panics and not
    /// an early return that distinguishes them — the decode-first rule.
    #[test]
    fn malformed_signature_values_are_mismatches() {
        let body = br#"{"id":"evt_1"}"#;
        for bad in ["", "zz", "not-hex-at-all", &"ab".repeat(16), &"ab".repeat(64)] {
            assert_eq!(
                verify(&format!("t={NOW},v1={bad}"), body, NOW),
                VerifyResult::SignatureMismatch,
                "v1={bad}"
            );
        }
    }

    /// Only the FIRST `t=` is honoured. If the last won, an attacker could
    /// append a fresh timestamp to a captured request and revive it — the
    /// signature would still fail, but the replay window would have been
    /// defeated, and defence in depth means neither layer may be optional.
    #[test]
    fn a_second_timestamp_cannot_revive_a_stale_request() {
        let body = br#"{"id":"evt_1"}"#;
        let old = NOW - 3600;
        let h = format!("t={old},t={NOW},v1={}", sign(SECRET, old, body));
        assert_eq!(verify(&h, body, NOW), VerifyResult::TimestampInvalid);
    }

    /// Bodies are signed as RAW BYTES. The Slack verifier had to be fixed for
    /// exactly this: hashing `String::from_utf8_lossy(body)` collapses every
    /// invalid byte onto U+FFFD, so two DIFFERENT bodies share one signing
    /// base and a signature for one authenticates the other.
    #[test]
    fn invalid_utf8_bodies_are_signed_bytewise() {
        let a: &[u8] = &[0x7b, 0x22, 0x78, 0x22, 0x3a, 0xff, 0xfe, 0x7d];
        let b: &[u8] = &[0x7b, 0x22, 0x78, 0x22, 0x3a, 0xfe, 0xff, 0x7d];
        assert_ne!(a, b);

        let h = format!("t={NOW},v1={}", sign(SECRET, NOW, a));
        assert_eq!(verify(&h, a, NOW), VerifyResult::Ok);
        assert_eq!(
            verify(&h, b, NOW),
            VerifyResult::SignatureMismatch,
            "a lossy-UTF8 signing base would collapse these two bodies onto one signature"
        );
    }

    // ── Recorded real-payload fixture ────────────────────────────────────────

    /// A **real** `invoice.paid` event, captured verbatim from a Stripe sandbox
    /// at API version `2026-07-29.dahlia`.
    ///
    /// Every other payload test in this file uses JSON *I* wrote from my
    /// understanding of Stripe's API. That proves the parsers are
    /// self-consistent; it cannot prove they match Stripe. This fixture is the
    /// only evidence here that they do.
    const REAL_INVOICE_PAID: &str = include_str!("testdata/stripe_invoice_paid_2026-07-29.dahlia.json");

    /// The finding that justifies `billing_period`'s line-item preference —
    /// and it is much stronger than the proration argument that comment
    /// originally gave.
    ///
    /// On this real event the INVOICE-level window is **degenerate**:
    /// `period_start == period_end`. The actual billing month exists only on
    /// the line item. So reading the invoice level — the obvious choice — does
    /// not merely risk a wrong window on prorations, it produces a
    /// ZERO-LENGTH one on an ordinary subscription invoice, which
    ///
    ///   * trips this handler's own `period_end <= period_start` guard, so the
    ///     renewal is refused with a 400, and
    ///   * would violate `0161`'s `member_allowance_period_end_after_start`
    ///     CHECK even if it got past that.
    ///
    /// In other words every renewal would have failed. This test pins the fact
    /// so the preference can never be "simplified" back.
    #[test]
    fn the_invoice_level_period_is_degenerate_on_a_real_event() {
        let event: Value = serde_json::from_str(REAL_INVOICE_PAID).expect("fixture parses");
        let invoice = event.get("data").and_then(|d| d.get("object")).expect("data.object");

        let inv_start = invoice.get("period_start").and_then(Value::as_i64).unwrap();
        let inv_end = invoice.get("period_end").and_then(Value::as_i64).unwrap();
        assert_eq!(
            inv_start, inv_end,
            "the invoice-level window on a real subscription invoice is a POINT, not a range — \
             this is the whole reason billing_period reads the line item"
        );

        let (start, end) = billing_period(invoice).expect("a period is found");
        assert!(
            end > start,
            "billing_period must return the LINE period, which is a real range"
        );
        assert_ne!((start, end), (inv_start, inv_end), "it must NOT be the invoice window");

        // ~31 days for a monthly subscription. A loose bound: the point is that
        // it is a month-ish range rather than zero, not the exact day count.
        let days = (end - start) / 86_400;
        assert!((28..=31).contains(&days), "expected a monthly window, got {days} days");
    }

    /// At `2026-07-29.dahlia` the historical `invoice.subscription` field is
    /// GONE. The `parent.subscription_details` fallback is therefore not a
    /// defensive nicety — it is the only path that works on this account
    /// today, and dropping it would stop every renewal.
    #[test]
    fn a_real_event_carries_its_subscription_only_under_parent() {
        let event: Value = serde_json::from_str(REAL_INVOICE_PAID).expect("fixture parses");
        let invoice = event.get("data").and_then(|d| d.get("object")).expect("data.object");

        assert!(
            invoice.get("subscription").and_then(Value::as_str).is_none(),
            "the historical field is absent at this API version"
        );
        let sub = subscription_id(invoice).expect("the fallback must find it");
        assert!(sub.starts_with("sub_"), "got {sub:?}");

        // And the guard that stops a one-off invoice rolling a period is only
        // meaningful because THIS returns Some — i.e. a real subscription
        // invoice is correctly recognised as one.
        assert!(subscription_id(invoice).is_some());
    }

    /// The customer reference arrives as a bare id string, which is what
    /// `resolve_account` is handed. Pinned because an expanded object would
    /// change the shape and silently break account resolution.
    #[test]
    fn a_real_event_carries_a_bare_customer_id() {
        let event: Value = serde_json::from_str(REAL_INVOICE_PAID).expect("fixture parses");
        let invoice = event.get("data").and_then(|d| d.get("object")).expect("data.object");

        let customer = reference_id(invoice.get("customer")).expect("a customer id");
        assert!(customer.starts_with("cus_"), "got {customer:?}");
    }

    /// The whole accessor chain the handler runs, against the real event, in
    /// handler order — so a regression anywhere in it fails here rather than
    /// in production on the first paid invoice.
    #[test]
    fn the_handler_accessor_chain_succeeds_on_a_real_event() {
        let event: Value = serde_json::from_str(REAL_INVOICE_PAID).expect("fixture parses");

        assert_eq!(event.get("type").and_then(Value::as_str), Some("invoice.paid"));
        assert!(event.get("id").and_then(Value::as_str).is_some_and(|i| i.starts_with("evt_")));

        let invoice = event.get("data").and_then(|d| d.get("object")).expect("data.object");
        assert!(reference_id(invoice.get("customer")).is_some(), "customer resolves");
        assert!(subscription_id(invoice).is_some(), "subscription resolves");

        let (start, end) = billing_period(invoice).expect("period resolves");
        assert!(to_utc(start).is_some() && to_utc(end).is_some(), "both bounds representable");
        assert!(to_utc(end).unwrap() > to_utc(start).unwrap(), "the period advances");
    }

    // ── Payload accessors ────────────────────────────────────────────────────

    #[test]
    fn reference_id_reads_both_inline_and_expanded_shapes() {
        assert_eq!(reference_id(Some(&json!("cus_123"))), Some("cus_123"));
        assert_eq!(reference_id(Some(&json!({"id": "cus_123"}))), Some("cus_123"));
        assert_eq!(reference_id(Some(&json!(""))), None);
        assert_eq!(reference_id(Some(&json!({"id": ""}))), None);
        assert_eq!(reference_id(Some(&json!(null))), None);
        assert_eq!(reference_id(None), None);
    }

    /// Which field carries the subscription depends on the API version pinned
    /// on the endpoint — a Stripe dashboard setting, not something this repo
    /// controls. Supporting only one shape would make renewals silently stop
    /// the day it changed.
    #[test]
    fn subscription_id_reads_both_api_generations() {
        assert_eq!(subscription_id(&json!({"subscription": "sub_1"})), Some("sub_1"));
        assert_eq!(
            subscription_id(&json!({"parent": {"subscription_details": {"subscription": "sub_2"}}})),
            Some("sub_2")
        );
        assert_eq!(
            subscription_id(&json!({"subscription": {"id": "sub_3"}})),
            Some("sub_3")
        );
    }

    /// The guard that stops a one-off invoice — a future credit pack — from
    /// rolling everyone's allowance period.
    #[test]
    fn a_one_off_invoice_has_no_subscription() {
        assert_eq!(subscription_id(&json!({"customer": "cus_1"})), None);
        assert_eq!(subscription_id(&json!({"subscription": null})), None);
        assert_eq!(subscription_id(&json!({"parent": null})), None);
        assert_eq!(subscription_id(&json!({"parent": {"quote_details": {}}})), None);
    }

    /// The line period wins over the invoice period. On a proration the
    /// invoice-level window can span more than the subscription period being
    /// paid for, and granting against that wider window would place
    /// `period_start` where the gate's "current period" lookup never finds it.
    #[test]
    fn billing_period_prefers_the_line_item() {
        let invoice = json!({
            "period_start": 100, "period_end": 999,
            "lines": {"data": [{"period": {"start": 200, "end": 300}}]}
        });
        assert_eq!(billing_period(&invoice), Some((200, 300)));

        let no_lines = json!({"period_start": 100, "period_end": 999, "lines": {"data": []}});
        assert_eq!(billing_period(&no_lines), Some((100, 999)));

        assert_eq!(billing_period(&json!({})), None);
    }

    /// Issue #644. Only a BILLING CYCLE may roll the renewal clock. A
    /// mid-cycle proration (`subscription_update`) must not, because the
    /// period it carries is the proration MOMENT, not the cycle anchor —
    /// storing it corrupts `stripe_subscriptions.current_period_start` and
    /// then the staleness guard rejects every later subscription event for
    /// that period.
    ///
    /// The three literals below are taken from the real production incident,
    /// not invented: invoice `in_example7c21d9ab` carried
    /// `billing_reason: "subscription_update"` with both lines starting at
    /// `1786836491`, while the subscription item reported `1786836436` — the
    /// 55-second skew that made `evt_example3e8f1a2c` come back
    /// `stale`.
    #[test]
    fn only_a_billing_cycle_rolls_the_renewal_clock() {
        // The two kinds that ARE a cycle boundary.
        assert!(rolls_the_renewal_clock(
            &json!({"billing_reason": "subscription_cycle"})
        ));
        assert!(rolls_the_renewal_clock(
            &json!({"billing_reason": "subscription_create"})
        ));

        // The one that caused #644.
        assert!(!rolls_the_renewal_clock(
            &json!({"billing_reason": "subscription_update"})
        ));

        // Any other mid-cycle reason is likewise not a cycle boundary.
        assert!(!rolls_the_renewal_clock(
            &json!({"billing_reason": "subscription_threshold"})
        ));

        // ABSENT is permissive, deliberately. Every invoice rolled the clock
        // before #644, so treating a missing field as "do not roll" would let
        // a future API version that drops it stop EVERY renewal silently —
        // the failure this module's `billing_period`/`subscription_period`
        // fallbacks already exist to prevent. Absence only ever restores the
        // old behaviour; it never invents a new way for renewals to stop.
        assert!(rolls_the_renewal_clock(&json!({})));
        assert!(rolls_the_renewal_clock(&json!({"billing_reason": null})));
    }

    // ── Subscription payload accessors (Phase 4b) ───────────────────────────

    /// The core fact this module's doc comment is built around: at the live
    /// API version this build was probed against, the period lives ONLY on
    /// the item. These are hand-built JSON doubles reflecting that probed
    /// shape (there is no captured subscription-event fixture file in this
    /// repo the way `invoice.paid` has one) — but the shape itself, "no
    /// top-level `current_period_end`", is the load-bearing fact under test.
    ///
    /// **Negative control.** Replace `subscription_period`'s body with the
    /// naive direct read (`sub.get("current_period_start")` /
    /// `.get("current_period_end")`, no `subscription_item` call at all —
    /// the shape a handler written against Stripe's OLD docs would use) and
    /// the FIRST assertion fails: `assertion \`left == right\` failed / left:
    /// None / right: Some((1700000000, 1702678400))`. Watched fail with
    /// exactly that message before restoring the item-first read. This is
    /// the literal shape of this repo's "absence renders as success"
    /// failure: the naive version compiles, returns `None` on every real
    /// subscription payload, and the caller's own guard turns that into a
    /// LOUD 400 rather than a silent no-op — see
    /// `handle_subscription_updated_with_no_resolvable_period_is_a_loud_400_not_a_silent_success`
    /// below for that half proven at the handler level.
    #[test]
    fn subscription_period_prefers_the_item_over_the_object() {
        let sub = json!({
            "items": {"data": [{"current_period_start": 1_700_000_000, "current_period_end": 1_702_678_400}]}
        });
        assert_eq!(subscription_period(&sub), Some((1_700_000_000, 1_702_678_400)));

        // The documented fallback: an older/different API-version shape
        // carrying the fields at the top level and no item at all.
        let legacy = json!({"current_period_start": 111, "current_period_end": 222});
        assert_eq!(subscription_period(&legacy), Some((111, 222)));

        // Neither shape resolves: this MUST be `None`, which the handler
        // then treats as a loud error, never a silent skip.
        assert_eq!(subscription_period(&json!({})), None);
        assert_eq!(subscription_period(&json!({"items": {"data": []}})), None);
    }

    /// `quantity` is read from the item first, with the documented
    /// legacy-shape fallback to the top-level field.
    #[test]
    fn subscription_seats_prefers_the_item_over_the_top_level_field() {
        let sub = json!({"quantity": 1, "items": {"data": [{"quantity": 3}]}});
        assert_eq!(
            subscription_seats(&sub),
            Some(3),
            "the item's quantity is authoritative, even when the stale top-level field disagrees"
        );

        let legacy = json!({"quantity": 2, "items": {"data": []}});
        assert_eq!(subscription_seats(&legacy), Some(2), "falls back when no item carries it");

        assert_eq!(subscription_seats(&json!({})), None);
    }

    #[test]
    fn cancellation_reason_reads_the_nested_field_and_tolerates_absence() {
        assert_eq!(
            cancellation_reason(&json!({"cancellation_details": {"reason": "cancellation_requested"}})),
            Some("cancellation_requested")
        );
        assert_eq!(cancellation_reason(&json!({"cancellation_details": {}})), None);
        assert_eq!(cancellation_reason(&json!({})), None, "a natural expiry carries no reason at all");
    }

    /// `0162`'s header requires an unrecognized Stripe status to map onto
    /// the NEAREST of the three, never be written as itself. `past_due` is
    /// the only one of the three that is safe to be wrong about (see
    /// `map_subscription_status`'s own doc comment for why), so the
    /// catch-all arm must land there.
    ///
    /// **Negative control.** Change the catch-all arm from `"past_due"` to
    /// `"active"` (the worst possible wrong guess — granting access that was
    /// never paid for). Writing `raw` verbatim does not even compile: the
    /// function's `&'static str` return type structurally forbids passing
    /// Stripe's own borrowed string through, which is a real property of
    /// this signature worth noting, not just a coincidence of this test.
    /// With `"active"` substituted, the FIRST assertion in the loop fails:
    /// `assertion \`left == right\` failed / left: "active" / right:
    /// "past_due"`. Watched fail with exactly that message (on the very
    /// first status checked, `"past_due"` itself) before restoring the
    /// catch-all.
    #[test]
    fn map_subscription_status_lands_unrecognized_statuses_on_past_due() {
        assert_eq!(map_subscription_status("active"), "active");
        assert_eq!(map_subscription_status("trialing"), "active");
        for s in ["past_due", "unpaid", "incomplete", "incomplete_expired", "paused"] {
            assert_eq!(map_subscription_status(s), "past_due", "status {s:?}");
        }
        // `canceled` is deliberately unreachable from THIS function (see its
        // doc comment) — it therefore falls into the catch-all like any
        // other status this table does not explicitly name.
        assert_eq!(map_subscription_status("canceled"), "past_due");
        assert_eq!(
            map_subscription_status("some_future_status"),
            "past_due",
            "an unrecognized status must map onto the nearest of the three, never itself"
        );
    }
}

/// The claim-and-roll transaction, against real Postgres.
///
/// # What these defend that `billing::renewal`'s own suite does not
///
/// `renewal`'s tests prove the GRANT is idempotent — `0161`'s UNIQUE index
/// makes a repeated roll insert nothing. These prove the EVENT is, which is a
/// different and broader guarantee: the claim protects every write the handler
/// makes, including the ones Phase 4b adds (a status transition and a
/// forfeiture ledger entry) that have no natural idempotency of their own.
/// Applying a forfeiture twice would double-debit, so the claim has to hold
/// before that code exists rather than after.
///
/// Gated on `RELINFER_TEST_DATABASE_URL`; see `scripts/check_pg_suite_coverage.sh`
/// for why the CI step also asserts the variable is set AND that the filter
/// matched a non-zero number of tests.
///
/// Run with:
///   RELINFER_TEST_DATABASE_URL=postgres://localhost:5432/postgres \
///     cargo test -p coralswarm-backend --lib "routes::stripe_webhook::pg_tests" -- --test-threads=1
#[cfg(test)]
pub(crate) async fn native_topup_writer(
    control: &sqlx::PgPool, account: Uuid, event: &str, price: &str,
) -> StatusCode {
    apply_checkout_completed_payment(control,account,event,"checkout.session.completed",event,
        &[crate::billing::stripe_client::SessionLineItem {price_id:price.to_owned(),quantity:1}])
        .await.status()
}

#[cfg(test)]
pub(crate) async fn native_cancel_writer(
    control: &sqlx::PgPool, account: Uuid, event: &str, subscription: &str,
) -> Result<bool,sqlx::Error> {
    apply_subscription_deleted(control,account,event,"customer.subscription.deleted",subscription,None)
        .await.map(|outcome|outcome.is_some())
}

#[cfg(test)]
pub(crate) async fn native_invoice_writer(
    control:&sqlx::PgPool,account:Uuid,event:&str,subscription:&str,
    start:chrono::DateTime<chrono::Utc>,end:chrono::DateTime<chrono::Utc>,
) -> Result<bool,sqlx::Error> {
    apply_invoice_paid(control,account,event,"invoice.paid",subscription,start,end)
        .await.map(|v|v.is_some())
}
#[cfg(test)]
pub(crate) async fn native_subscription_writer(
    control:&sqlx::PgPool,account:Uuid,event:&str,
    start:chrono::DateTime<chrono::Utc>,end:chrono::DateTime<chrono::Utc>,
) -> Result<bool,sqlx::Error> {
    apply_subscription_updated(control,account,event,"customer.subscription.updated",2,"active",start,end)
        .await.map(|v|v.is_some())
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
    use sqlx::types::Decimal;
    use sqlx::PgPool;

    /// Serializes every test that mutates GLOBAL catalog reference data
    /// (`plans`/`seat_tiers`/`credit_packs` — `stripe_price_id` via
    /// `set_catalog_price`, and `plans.team.is_active`) — none of it is
    /// per-test-scoped, and this repo's `cargo test` default runs threads of
    /// one process concurrently. Two tests racing the SAME row is a real
    /// collision here, not a theoretical one — e.g. two tests both setting
    /// `credit_packs.pack_500.stripe_price_id` to their own distinct mock
    /// value: whichever SET lands last silently wins, so a test whose own
    /// SET was overwritten by another test's concurrent one price-looks-up
    /// against a value it never wrote, and a checkout-completion handler
    /// correctly reports "unrecognized pack price" for what the test itself
    /// considers a perfectly valid one. Mirrors `routes::checkout::
    /// pg_tests`'s `CATALOG_PRICE_LOCK` (same "shared mutable global,
    /// parallel test threads" shape, see that module's own doc comment) —
    /// one lock for every catalog mutation in this file, not split per table
    /// or per code, matching that module's own "simplicity over fine-grained
    /// locking in test code" choice.
    static CATALOG_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

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

    fn period() -> (chrono::DateTime<chrono::Utc>, chrono::DateTime<chrono::Utc>) {
        (
            chrono::DateTime::from_timestamp(1_700_000_000, 0).unwrap(),
            chrono::DateTime::from_timestamp(1_702_678_400, 0).unwrap(),
        )
    }

    /// An account with `members` members, a `team_standard` grant, and a linked
    /// Stripe customer — the state an `invoice.paid` arrives into.
    async fn seed_linked_account(pool: &PgPool, members: usize) -> (Uuid, Vec<Uuid>, String) {
        let mut users = Vec::new();
        for i in 0..members {
            let id = Uuid::new_v4();
            sqlx::query("INSERT INTO users (id, email) VALUES ($1, $2)")
                .bind(id)
                .bind(format!("stripe-{}-{i}@test.example", id.simple()))
                .execute(pool)
                .await
                .expect("insert user");
            users.push(id);
        }
        let account_id = Uuid::new_v4();
        sqlx::query("INSERT INTO accounts (id, owner_user_id) VALUES ($1, $2)")
            .bind(account_id)
            .bind(users[0])
            .execute(pool)
            .await
            .expect("insert account");
        for (i, u) in users.iter().enumerate() {
            sqlx::query(
                "INSERT INTO account_members (account_id, user_id, role) VALUES ($1, $2, $3)",
            )
            .bind(account_id)
            .bind(u)
            .bind(if i == 0 { "owner" } else { "member" })
            .execute(pool)
            .await
            .expect("insert membership");
        }

        let plan_id: Uuid = sqlx::query_scalar("SELECT id FROM plans WHERE code = 'team_standard'")
            .fetch_one(pool)
            .await
            .expect("team_standard must be seeded (0155)");
        execute_financial_fixture(
            pool,
            account_id,
        sqlx::query(
            "INSERT INTO grants (account_id, plan_id, seats, plan_is_seat_priced) \
             VALUES ($1, $2, $3, true)",
        )
        .bind(account_id)
        .bind(plan_id)
        .bind(members as i32),
        )
        .await
        .expect("insert grant");

        let customer_id = format!("cus_{}", Uuid::new_v4().simple());
        execute_financial_fixture(pool, account_id, sqlx::query(
            "INSERT INTO stripe_subscriptions (account_id, stripe_customer_id, stripe_subscription_id) \
             VALUES ($1, $2, $3)",
        )
        .bind(account_id)
        .bind(&customer_id)
        .bind(format!("sub_{}", Uuid::new_v4().simple())))
        .await
        .expect("link stripe customer");

        (account_id, users, customer_id)
    }

    /// Reads back the `stripe_subscription_id` [`seed_linked_account`] wrote.
    /// Tests calling `apply_invoice_paid` must pass a MATCHING subscription
    /// id (finding 3, billing self-serve PR D review round) to reach the
    /// normal path — the new subscription-identity guard treats a mismatch
    /// as a stale/superseded event. Tests OF that new mismatch behavior pass
    /// a deliberately different string instead.
    async fn linked_subscription_id(pool: &PgPool, account_id: Uuid) -> String {
        sqlx::query_scalar(
            "SELECT stripe_subscription_id FROM stripe_subscriptions WHERE account_id = $1",
        )
        .bind(account_id)
        .fetch_one(pool)
        .await
        .expect("read stripe_subscription_id")
    }

    async fn count(pool: &PgPool, sql: &str, account_id: Uuid) -> i64 {
        sqlx::query_scalar(sql)
            .bind(account_id)
            .fetch_one(pool)
            .await
            .expect("count")
    }

    async fn cleanup(pool: &PgPool, account_id: Uuid, users: &[Uuid]) {
        let _ = execute_financial_fixture(
            pool,
            account_id,
            sqlx::query("DELETE FROM accounts WHERE id = $1")
            .bind(account_id),
        )
            .await;
        for u in users {
            let _ = sqlx::query("DELETE FROM users WHERE id = $1").bind(u).execute(pool).await;
        }
    }

    // ── Phase 4b test helpers ────────────────────────────────────────────────

    /// Adds one new member to an already-seeded account, returning the new
    /// user's id — the "latecomer" in the mid-period-seat-add tests.
    async fn add_member(pool: &PgPool, account_id: Uuid) -> Uuid {
        let id = Uuid::new_v4();
        sqlx::query("INSERT INTO users (id, email) VALUES ($1, $2)")
            .bind(id)
            .bind(format!("stripe-late-{}@test.example", id.simple()))
            .execute(pool)
            .await
            .expect("insert latecomer user");
        sqlx::query("INSERT INTO account_members (account_id, user_id, role) VALUES ($1, $2, 'member')")
            .bind(account_id)
            .bind(id)
            .execute(pool)
            .await
            .expect("insert latecomer membership");
        id
    }

    /// A single-member account holding a FLAT (`pro`) plan grant and a linked
    /// Stripe customer — `seats IS NULL`, `plan_is_seat_priced = false`, the
    /// shape [`0156`'s `grants_seats_iff_seat_priced`][pin] CHECK requires for
    /// a non-seat-priced plan.
    ///
    /// [pin]: ../../../../migrations_control/0156_grants_seats.sql
    async fn seed_flat_plan_account(pool: &PgPool) -> (Uuid, Vec<Uuid>, String) {
        let user_id = Uuid::new_v4();
        sqlx::query("INSERT INTO users (id, email) VALUES ($1, $2)")
            .bind(user_id)
            .bind(format!("stripe-flat-{}@test.example", user_id.simple()))
            .execute(pool)
            .await
            .expect("insert user");
        let account_id = Uuid::new_v4();
        sqlx::query("INSERT INTO accounts (id, owner_user_id) VALUES ($1, $2)")
            .bind(account_id)
            .bind(user_id)
            .execute(pool)
            .await
            .expect("insert account");
        sqlx::query("INSERT INTO account_members (account_id, user_id, role) VALUES ($1, $2, 'owner')")
            .bind(account_id)
            .bind(user_id)
            .execute(pool)
            .await
            .expect("insert membership");

        let plan_id: Uuid = sqlx::query_scalar("SELECT id FROM plans WHERE code = 'pro'")
            .fetch_one(pool)
            .await
            .expect("pro must be seeded (0155)");
        execute_financial_fixture(
            pool,
            account_id,
        sqlx::query(
            "INSERT INTO grants (account_id, plan_id, seats, plan_is_seat_priced) \
             VALUES ($1, $2, NULL, false)",
        )
        .bind(account_id)
        .bind(plan_id),
        )
        .await
        .expect("insert flat-plan grant");

        let customer_id = format!("cus_{}", Uuid::new_v4().simple());
        execute_financial_fixture(pool, account_id, sqlx::query(
            "INSERT INTO stripe_subscriptions (account_id, stripe_customer_id, stripe_subscription_id) \
             VALUES ($1, $2, $3)",
        )
        .bind(account_id)
        .bind(&customer_id)
        .bind(format!("sub_{}", Uuid::new_v4().simple())))
        .await
        .expect("link stripe customer");

        (account_id, vec![user_id], customer_id)
    }

    async fn set_balance(pool: &PgPool, account_id: Uuid, balance: Decimal) {
        execute_financial_fixture(
            pool,
            account_id,
        sqlx::query(
            "INSERT INTO credit_balances (account_id, balance) VALUES ($1, $2) \
             ON CONFLICT (account_id) DO UPDATE SET balance = EXCLUDED.balance, updated_at = now()",
        )
        .bind(account_id)
        .bind(balance),
        )
        .await
        .expect("seed balance");
    }

    async fn read_balance(pool: &PgPool, account_id: Uuid) -> Option<Decimal> {
        sqlx::query_scalar("SELECT balance FROM credit_balances WHERE account_id = $1")
            .bind(account_id)
            .fetch_optional(pool)
            .await
            .expect("read balance")
    }

    async fn read_seats(pool: &PgPool, account_id: Uuid) -> Option<i32> {
        let row: Option<Option<i32>> = sqlx::query_scalar(
            "SELECT seats FROM grants WHERE account_id = $1 AND revoked_at IS NULL \
              ORDER BY granted_at DESC LIMIT 1",
        )
        .bind(account_id)
        .fetch_optional(pool)
        .await
        .expect("read seats");
        row.flatten()
    }

    async fn read_status(pool: &PgPool, account_id: Uuid) -> String {
        sqlx::query_scalar("SELECT status FROM stripe_subscriptions WHERE account_id = $1")
            .bind(account_id)
            .fetch_one(pool)
            .await
            .expect("read status")
    }

    /// Whether the account's CURRENT (most recently granted) grant is
    /// revoked — the half `billing::renewal::pg_tests::a_revoked_grant_does_not_renew`
    /// proves the EFFECT of; this reads the CAUSE the forfeit path writes.
    async fn latest_grant_revoked(pool: &PgPool, account_id: Uuid) -> bool {
        let revoked_at: Option<chrono::DateTime<chrono::Utc>> = sqlx::query_scalar(
            "SELECT revoked_at FROM grants WHERE account_id = $1 ORDER BY granted_at DESC LIMIT 1",
        )
        .bind(account_id)
        .fetch_one(pool)
        .await
        .expect("read latest grant");
        revoked_at.is_some()
    }

    /// The happy path, end to end: the event is recorded, the period is rolled,
    /// and Stripe's own period mirror is updated — all in one transaction.
    #[tokio::test]
    async fn a_paid_invoice_claims_the_event_and_rolls_the_period() {
        let Some(pool) = pg_test_pool("a_paid_invoice_claims_the_event_and_rolls_the_period").await
        else {
            return;
        };
        let (account_id, users, _customer) = seed_linked_account(&pool, 3).await;
        let sub_id = linked_subscription_id(&pool, account_id).await;
        let (start, end) = period();
        let event_id = format!("evt_{}", Uuid::new_v4().simple());

        let outcome = apply_invoice_paid(&pool, account_id, &event_id, "invoice.paid", &sub_id, start, end)
            .await
            .expect("apply")
            .expect("a first delivery is not a replay")
            .applied()
            .expect("a fresh account's first invoice cannot be stale — there is nothing to be stale against");

        assert_eq!(outcome.rows_granted, 3);
        assert_eq!(
            count(
                &pool,
                "SELECT COUNT(*) FROM member_allowance_periods WHERE account_id = $1",
                account_id
            )
            .await,
            3
        );

        let (recorded_outcome, recorded_type): (Option<String>, String) = sqlx::query_as(
            "SELECT outcome, event_type FROM stripe_webhook_events WHERE event_id = $1",
        )
        .bind(&event_id)
        .fetch_one(&pool)
        .await
        .expect("the event must have been recorded");
        assert_eq!(recorded_type, "invoice.paid");
        assert!(
            recorded_outcome.as_deref().is_some_and(|o| o.contains("renewed rows=3")),
            "the outcome is written in the SAME transaction as the work it describes, so a \
             recorded event with no outcome would mean the two had drifted apart: {recorded_outcome:?}"
        );

        let (status, ps, pe): (String, Option<chrono::DateTime<chrono::Utc>>, Option<chrono::DateTime<chrono::Utc>>) =
            sqlx::query_as(
                "SELECT status, current_period_start, current_period_end FROM stripe_subscriptions \
                 WHERE account_id = $1",
            )
            .bind(account_id)
            .fetch_one(&pool)
            .await
            .expect("subscription row");
        assert_eq!(status, "active");
        assert_eq!(ps, Some(start), "Stripe's period mirror is updated");
        assert_eq!(pe, Some(end));

        cleanup(&pool, account_id, &users).await;
    }

    /// Stripe redelivers. The second delivery of the SAME event must claim
    /// nothing, do nothing, and report itself as a replay.
    #[tokio::test]
    async fn a_redelivered_event_is_claimed_once_and_does_nothing_twice() {
        let Some(pool) =
            pg_test_pool("a_redelivered_event_is_claimed_once_and_does_nothing_twice").await
        else {
            return;
        };
        let (account_id, users, _customer) = seed_linked_account(&pool, 2).await;
        let sub_id = linked_subscription_id(&pool, account_id).await;
        let (start, end) = period();
        let event_id = format!("evt_{}", Uuid::new_v4().simple());

        let first = apply_invoice_paid(&pool, account_id, &event_id, "invoice.paid", &sub_id, start, end)
            .await
            .expect("first");
        let second = apply_invoice_paid(&pool, account_id, &event_id, "invoice.paid", &sub_id, start, end)
            .await
            .expect("second");

        assert!(first.is_some(), "the first delivery does the work");
        assert!(
            second.is_none(),
            "the second must be reported as a replay, NOT as a fresh renewal that happened to \
             grant zero rows — Phase 4b's forfeiture has no natural idempotency and relies on \
             this distinction"
        );
        assert_eq!(
            count(
                &pool,
                "SELECT COUNT(*) FROM stripe_webhook_events WHERE account_id = $1",
                account_id
            )
            .await,
            1,
            "one event, one row"
        );
        assert_eq!(
            count(
                &pool,
                "SELECT COUNT(*) FROM member_allowance_periods WHERE account_id = $1",
                account_id
            )
            .await,
            2,
            "allowances must not be doubled"
        );

        cleanup(&pool, account_id, &users).await;
    }

    /// Defence in depth. Two INDEPENDENT idempotency layers protect the grant:
    /// the event claim, and `0161`'s UNIQUE index. This exercises the second by
    /// defeating the first — a genuinely different `event_id` carrying the same
    /// period, which is what a Stripe endpoint reconfiguration or a replayed
    /// invoice from the dashboard actually produces.
    #[tokio::test]
    async fn a_new_event_for_an_already_granted_period_grants_nothing_extra() {
        let Some(pool) =
            pg_test_pool("a_new_event_for_an_already_granted_period_grants_nothing_extra").await
        else {
            return;
        };
        let (account_id, users, _customer) = seed_linked_account(&pool, 2).await;
        let sub_id = linked_subscription_id(&pool, account_id).await;
        let (start, end) = period();

        apply_invoice_paid(
            &pool,
            account_id,
            &format!("evt_{}", Uuid::new_v4().simple()),
            "invoice.paid",
            &sub_id,
            start,
            end,
        )
        .await
        .expect("first")
        .expect("not a replay");

        let second = apply_invoice_paid(
            &pool,
            account_id,
            &format!("evt_{}", Uuid::new_v4().simple()),
            "invoice.paid",
            &sub_id,
            start,
            end,
        )
        .await
        .expect("second")
        .expect("a different event id is NOT a replay — it is claimed normally")
        .applied()
        .expect(
            "the SAME period_start as what is already stored is EQUAL, not older — the \
             staleness guard's strict `<` must treat this as fresh, exactly like a \
             same-period redelivery always has",
        );

        assert_eq!(
            second.rows_granted, 0,
            "the claim let this through, so 0161's UNIQUE index is what stops the double grant"
        );
        assert_eq!(
            count(
                &pool,
                "SELECT COUNT(*) FROM member_allowance_periods WHERE account_id = $1",
                account_id
            )
            .await,
            2,
            "still one allowance per member"
        );
        assert_eq!(
            count(
                &pool,
                "SELECT COUNT(*) FROM stripe_webhook_events WHERE account_id = $1",
                account_id
            )
            .await,
            2,
            "both events are recorded — the audit trail keeps them distinct"
        );

        cleanup(&pool, account_id, &users).await;
    }

    /// The restore half of the founder's "involuntary lapse -> frozen, then
    /// restored". An account that pays its overdue invoice is active again by
    /// virtue of having paid; the freeze half lands in Phase 4b.
    #[tokio::test]
    async fn a_paid_invoice_restores_a_past_due_subscription() {
        let Some(pool) = pg_test_pool("a_paid_invoice_restores_a_past_due_subscription").await
        else {
            return;
        };
        let (account_id, users, _customer) = seed_linked_account(&pool, 1).await;
        let sub_id = linked_subscription_id(&pool, account_id).await;
        execute_financial_fixture(
            &pool,
            account_id,
            sqlx::query("UPDATE stripe_subscriptions SET status = 'past_due' WHERE account_id = $1")
            .bind(account_id),
        )
            .await
            .expect("freeze");
        let (start, end) = period();

        apply_invoice_paid(
            &pool,
            account_id,
            &format!("evt_{}", Uuid::new_v4().simple()),
            "invoice.paid",
            &sub_id,
            start,
            end,
        )
        .await
        .expect("apply")
        .expect("not a replay");

        let status: String =
            sqlx::query_scalar("SELECT status FROM stripe_subscriptions WHERE account_id = $1")
                .bind(account_id)
                .fetch_one(&pool)
                .await
                .expect("status");
        assert_eq!(status, "active", "paying restores a frozen subscription");

        cleanup(&pool, account_id, &users).await;
    }

    /// **Regression test for finding 3 (billing self-serve PR D review
    /// round), case 1: id mismatch.** A late `invoice.paid` for a
    /// SUPERSEDED subscription (cancel-then-resubscribe: the account's
    /// current subscription id has moved on) must not restore `status =
    /// "active"` or roll a period, even though the invoice is genuinely
    /// paid — it was paid against a subscription this account has already
    /// moved past.
    #[tokio::test]
    async fn a_stale_invoice_for_a_superseded_subscription_does_not_restore_or_roll() {
        let Some(pool) =
            pg_test_pool("a_stale_invoice_for_a_superseded_subscription_does_not_restore_or_roll")
                .await
        else {
            return;
        };
        let (account_id, users, _customer) = seed_linked_account(&pool, 1).await;
        // Simulate a resubscribe: the account's CURRENT subscription id is
        // NOT the one this invoice belongs to.
        let current_sub_id = format!("sub_{}", Uuid::new_v4().simple());
        execute_financial_fixture(&pool, account_id, sqlx::query("UPDATE stripe_subscriptions SET stripe_subscription_id = $2, status = 'active' WHERE account_id = $1")
            .bind(account_id)
            .bind(&current_sub_id))
            .await
            .expect("link the current subscription");
        let old_sub_id = format!("sub_{}", Uuid::new_v4().simple());
        assert_ne!(old_sub_id, current_sub_id);

        let (start, end) = period();
        let outcome = apply_invoice_paid(
            &pool,
            account_id,
            &format!("evt_{}", Uuid::new_v4().simple()),
            "invoice.paid",
            &old_sub_id,
            start,
            end,
        )
        .await
        .expect("apply")
        .expect("still CLAIMED, not a replay — a fresh event_id");

        assert!(
            matches!(outcome, InvoicePaidOutcome::SupersededSubscription { .. }),
            "an id mismatch must be reported as superseded, not applied: {outcome:?}"
        );
        assert_eq!(
            count(&pool, "SELECT COUNT(*) FROM member_allowance_periods WHERE account_id = $1", account_id)
                .await,
            0,
            "no period may be rolled for a superseded subscription's invoice"
        );
        let (status, stored_sub): (String, Option<String>) = sqlx::query_as(
            "SELECT status, stripe_subscription_id FROM stripe_subscriptions WHERE account_id = $1",
        )
        .bind(account_id)
        .fetch_one(&pool)
        .await
        .expect("read subscription row");
        assert_eq!(status, "active", "status must be left exactly as it was");
        assert_eq!(stored_sub.as_deref(), Some(current_sub_id.as_str()), "the current subscription id must be untouched");

        cleanup(&pool, account_id, &users).await;
    }

    /// **Regression test for finding 3, case 2: same id, but already
    /// `canceled`.** The FINAL invoice Stripe sometimes issues after a
    /// deliberate cancellation — for the SAME subscription id, arriving
    /// AFTER `customer.subscription.deleted` already froze the account —
    /// must not restore `status = "active"`. Freeze-not-forfeit (plan
    /// §1.11) means the account stays frozen until a NEW checkout, not
    /// until its old subscription's trailing invoice happens to clear.
    #[tokio::test]
    async fn a_final_invoice_after_deliberate_cancel_does_not_restore_active() {
        let Some(pool) =
            pg_test_pool("a_final_invoice_after_deliberate_cancel_does_not_restore_active").await
        else {
            return;
        };
        let (account_id, users, _customer) = seed_linked_account(&pool, 1).await;
        let sub_id = linked_subscription_id(&pool, account_id).await;
        execute_financial_fixture(
            &pool,
            account_id,
            sqlx::query("UPDATE stripe_subscriptions SET status = 'canceled' WHERE account_id = $1")
            .bind(account_id),
        )
            .await
            .expect("simulate the deliberate-cancel freeze");

        let (start, end) = period();
        let outcome = apply_invoice_paid(
            &pool,
            account_id,
            &format!("evt_{}", Uuid::new_v4().simple()),
            "invoice.paid",
            &sub_id,
            start,
            end,
        )
        .await
        .expect("apply")
        .expect("still CLAIMED, not a replay");

        assert!(
            matches!(outcome, InvoicePaidOutcome::SupersededSubscription { .. }),
            "a final invoice against an already-canceled subscription must not be applied: {outcome:?}"
        );
        assert_eq!(
            count(&pool, "SELECT COUNT(*) FROM member_allowance_periods WHERE account_id = $1", account_id)
                .await,
            0,
            "no fresh period may be rolled for a deliberately canceled subscription"
        );
        let status: String =
            sqlx::query_scalar("SELECT status FROM stripe_subscriptions WHERE account_id = $1")
                .bind(account_id)
                .fetch_one(&pool)
                .await
                .expect("status");
        assert_eq!(status, "canceled", "must stay frozen — a resubscribe, not a trailing invoice, is what restores it");

        cleanup(&pool, account_id, &users).await;
    }

    /// The resolver is what decides WHICH ACCOUNT GETS CREDITED, so it gets its
    /// own tests rather than being covered incidentally.
    #[tokio::test]
    async fn the_resolver_maps_a_stripe_customer_to_its_account() {
        let Some(pool) = pg_test_pool("the_resolver_maps_a_stripe_customer_to_its_account").await
        else {
            return;
        };
        let (account_id, users, customer_id) = seed_linked_account(&pool, 1).await;

        assert_eq!(
            resolve_account(&pool, &customer_id).await.expect("resolve"),
            Some(account_id),
            "the linked account, via 0162's SECURITY DEFINER resolver"
        );

        // An unknown customer is Ok(None), NOT an error — the handler answers
        // 200 for it, because no amount of redelivery creates a link.
        assert_eq!(
            resolve_account(&pool, "cus_definitely_not_linked").await.expect("resolve"),
            None
        );

        cleanup(&pool, account_id, &users).await;
    }

    /// `stripe_customer_id` is UNIQUE, and that constraint is load-bearing
    /// rather than cosmetic: it is the ONLY join key the webhook has, so two
    /// accounts sharing one would make the resolver's `LIMIT 1` an arbitrary
    /// tiebreak that credits the WRONG account's members — a billing error
    /// invisible until a customer complained. Proven at the schema, not
    /// assumed.
    #[tokio::test]
    async fn two_accounts_cannot_share_one_stripe_customer_id() {
        let Some(pool) = pg_test_pool("two_accounts_cannot_share_one_stripe_customer_id").await
        else {
            return;
        };
        let (account_a, users_a, customer_id) = seed_linked_account(&pool, 1).await;
        let (account_b, users_b, _other_customer) = seed_linked_account(&pool, 1).await;

        let stolen = execute_financial_fixture(
            &pool,
            account_b,
            sqlx::query(
            "UPDATE stripe_subscriptions SET stripe_customer_id = $2 WHERE account_id = $1",
        )
        .bind(account_b)
        .bind(&customer_id),
        )
        .await;
        assert!(
            stolen.is_err(),
            "a second account must not be able to claim an already-linked Stripe customer"
        );

        // And the original mapping is untouched.
        assert_eq!(
            resolve_account(&pool, &customer_id).await.expect("resolve"),
            Some(account_a)
        );

        cleanup(&pool, account_a, &users_a).await;
        cleanup(&pool, account_b, &users_b).await;
    }

    /// Two subscription invoices carrying DIFFERENT, overlapping periods — a
    /// mid-period proration, most concretely, not a replay and not a
    /// same-period redelivery — must BOTH succeed, with the earlier period
    /// truncated to make room rather than the second `apply_invoice_paid`
    /// erroring out.
    ///
    /// **Negative control.** Remove the `close_open_period` call from
    /// `apply_invoice_paid` (leaving only `roll_period`) and the second
    /// `apply_invoice_paid` panics with `Database(PgDatabaseError { code:
    /// "23P01", ... constraint: Some("member_allowance_periods_no_overlap")
    /// ... })` instead of returning `Ok(Some(_))` — i.e. this function's
    /// `.expect("second apply must succeed once the old period is closed
    /// first")` is what fires. That is precisely the "redelivered-forever
    /// 500" this call exists to prevent. Watched fail with that exact error
    /// before restoring the call.
    #[tokio::test]
    async fn two_overlapping_invoices_for_different_periods_both_succeed() {
        let Some(pool) =
            pg_test_pool("two_overlapping_invoices_for_different_periods_both_succeed").await
        else {
            return;
        };
        let (account_id, users, _customer) = seed_linked_account(&pool, 2).await;
        let sub_id = linked_subscription_id(&pool, account_id).await;
        let (start, end) = period();

        let first = apply_invoice_paid(
            &pool,
            account_id,
            &format!("evt_{}", Uuid::new_v4().simple()),
            "invoice.paid",
            &sub_id,
            start,
            end,
        )
        .await
        .expect("first apply")
        .expect("not a replay")
        .applied()
        .expect("a fresh account's first invoice cannot be stale");
        assert_eq!(first.rows_granted, 2);

        // A SECOND invoice with a DIFFERENT period that overlaps the first —
        // e.g. a mid-cycle proration re-billing the remainder of the month.
        // NEWER than the first, so the staleness guard must not trip it.
        let second_start = start + chrono::Duration::days(10);
        let second_end = second_start + chrono::Duration::days(31);
        assert_ne!(second_start, start, "must be a genuinely different period, not a redelivery");

        let second = apply_invoice_paid(
            &pool,
            account_id,
            &format!("evt_{}", Uuid::new_v4().simple()),
            "invoice.paid",
            &sub_id,
            second_start,
            second_end,
        )
        .await
        .expect("second apply must succeed once the old period is closed first")
        .expect("a genuinely different period is not a replay")
        .applied()
        .expect("a NEWER period than what is stored must never be treated as stale");
        assert_eq!(second.rows_granted, 2, "the new period is granted to both members");

        // The first period must have been TRUNCATED to end at the second's
        // start, not deleted and not left open (which would have let both
        // periods count simultaneously against the same member).
        let first_ends: Vec<chrono::DateTime<chrono::Utc>> = sqlx::query_scalar(
            "SELECT period_end FROM member_allowance_periods \
              WHERE account_id = $1 AND period_start = $2 ORDER BY user_id",
        )
        .bind(account_id)
        .bind(start)
        .fetch_all(&pool)
        .await
        .expect("read the first period's rows");
        assert_eq!(first_ends, vec![second_start, second_start], "both members' first periods were truncated");

        assert_eq!(
            count(
                &pool,
                "SELECT COUNT(*) FROM member_allowance_periods WHERE account_id = $1",
                account_id
            )
            .await,
            4,
            "both periods retained (truncated first, full second) — 2 members x 2 periods"
        );

        cleanup(&pool, account_id, &users).await;
    }

    // ── Staleness guard (Fix B, out-of-order delivery) ──────────────────────

    /// The core Fix B scenario: a `invoice.paid` event carrying a period
    /// OLDER than what `stripe_subscriptions.current_period_start` already
    /// records — a Stripe redelivery of a stale event, or two deliveries
    /// racing and arriving out of order — must not touch anything a LATER,
    /// already-processed event created and the customer already paid for.
    ///
    /// **Negative control.** Disable the staleness-guard branch in
    /// `apply_invoice_paid` (`if false && period_start < stored { ... }`,
    /// falling through to the normal path unconditionally) and this test's
    /// `outcome.is_stale()` assertion fails first: `thread ... panicked ...
    /// an older-than-stored period must be reported as stale` — the event
    /// is no longer recognised as superseded at all, which is the root
    /// cause of the actual data loss. Confirmed directly against the
    /// database with the guard still disabled and that first assertion
    /// removed: `member_allowance_periods` for this account ends up holding
    /// exactly the STALE window (`start0`..`end0`), not the newer, already-
    /// granted one (`start2`..`end2`) — the newer period was deleted by
    /// `close_open_period(start0)` (its `period_start = start2` is strictly
    /// after the stale boundary `start0`, so
    /// `DELETE_UNSTARTED_OPEN_PERIOD_SQL` removes it) and replaced by a
    /// fresh grant for the stale window, exactly the "an out-of-order event
    /// can delete later, already-paid periods" bug this guard exists to
    /// close. Watched both fail exactly that way before restoring the
    /// guard.
    #[tokio::test]
    async fn a_stale_invoice_paid_does_not_delete_or_shrink_a_newer_already_granted_period() {
        let Some(pool) = pg_test_pool(
            "a_stale_invoice_paid_does_not_delete_or_shrink_a_newer_already_granted_period",
        )
        .await
        else {
            return;
        };
        let (account_id, users, _customer) = seed_linked_account(&pool, 2).await;
        let sub_id = linked_subscription_id(&pool, account_id).await;

        // The NEWER period, already granted — this is the "already-processed,
        // already-paid" state a later event created.
        let start2 = chrono::DateTime::from_timestamp(1_700_000_000, 0).unwrap();
        let end2 = chrono::DateTime::from_timestamp(1_702_678_400, 0).unwrap();
        apply_invoice_paid(
            &pool,
            account_id,
            &format!("evt_{}", Uuid::new_v4().simple()),
            "invoice.paid",
            &sub_id,
            start2,
            end2,
        )
        .await
        .expect("seed the newer, already-granted period")
        .expect("not a replay")
        .applied()
        .expect("a fresh account's first invoice cannot be stale");

        let newer_before: Vec<(Uuid, chrono::DateTime<chrono::Utc>)> = sqlx::query_as(
            "SELECT user_id, period_end FROM member_allowance_periods \
              WHERE account_id = $1 AND period_start = $2 ORDER BY user_id",
        )
        .bind(account_id)
        .bind(start2)
        .fetch_all(&pool)
        .await
        .expect("read the newer period's rows before the stale event");
        assert_eq!(newer_before.len(), 2, "both members hold the newer period before the stale event arrives");

        // A STALE event: an OLDER period than what is already stored,
        // arriving out of order. `start0 < start2` is the entire trigger.
        let start0 = start2 - chrono::Duration::days(5);
        let end0 = start2 + chrono::Duration::days(10);
        assert!(start0 < start2, "must be genuinely older than the stored period");

        let event_id = format!("evt_{}", Uuid::new_v4().simple());
        let outcome = apply_invoice_paid(&pool, account_id, &event_id, "invoice.paid", &sub_id, start0, end0)
            .await
            .expect("apply")
            .expect("a stale event is still CLAIMED, not a replay — it has a fresh event_id");

        assert!(outcome.is_stale(), "an older-than-stored period must be reported as stale");
        match outcome {
            InvoicePaidOutcome::Stale { incoming_period_start, stored_period_start } => {
                assert_eq!(incoming_period_start, start0);
                assert_eq!(stored_period_start, start2);
            }
            other => panic!("expected Stale, got {other:?}"),
        }

        // The newer, already-granted period must survive completely
        // untouched — neither deleted nor shrunk.
        let newer_after: Vec<(Uuid, chrono::DateTime<chrono::Utc>)> = sqlx::query_as(
            "SELECT user_id, period_end FROM member_allowance_periods \
              WHERE account_id = $1 AND period_start = $2 ORDER BY user_id",
        )
        .bind(account_id)
        .bind(start2)
        .fetch_all(&pool)
        .await
        .expect("read the newer period's rows after the stale event");
        assert_eq!(
            newer_after.len(), 2,
            "the newer, already-granted period must survive a stale redelivery untouched"
        );
        assert_eq!(newer_after, newer_before, "not even period_end may move — no shrink, no delete");

        // No new period was seeded for the stale window either.
        assert_eq!(
            count(&pool, "SELECT COUNT(*) FROM member_allowance_periods WHERE account_id = $1", account_id)
                .await,
            2,
            "the stale event must not have seeded any rows of its own"
        );

        // Stripe's own period mirror must still read the NEWER period, not
        // have been overwritten backward by the stale one.
        let (stored_start, stored_end): (Option<chrono::DateTime<chrono::Utc>>, Option<chrono::DateTime<chrono::Utc>>) =
            sqlx::query_as(
                "SELECT current_period_start, current_period_end FROM stripe_subscriptions WHERE account_id = $1",
            )
            .bind(account_id)
            .fetch_one(&pool)
            .await
            .expect("read subscription mirror");
        assert_eq!(stored_start, Some(start2), "the period mirror must not move backward");
        assert_eq!(stored_end, Some(end2));

        // The event is claimed (so a retry of THIS event id is an ordinary
        // replay) and its outcome is clearly labelled for support/audit.
        let recorded: Option<String> =
            sqlx::query_scalar("SELECT outcome FROM stripe_webhook_events WHERE event_id = $1")
                .bind(&event_id)
                .fetch_one(&pool)
                .await
                .expect("recorded outcome");
        assert!(
            recorded.as_deref().is_some_and(|o| o.starts_with("stale")),
            "the outcome must be clearly labelled stale for a support question: {recorded:?}"
        );

        cleanup(&pool, account_id, &users).await;
    }

    /// Redelivery of a STALE event (the exact same `event_id` twice) must
    /// still be an ordinary replay — the claim, not a second staleness
    /// check, is what makes it idempotent. Proves the claim commits even on
    /// the stale path (a rollback there would let the same stale event
    /// re-trigger the staleness check, and thus reach the database, forever).
    ///
    /// **Negative control.** Disable the staleness-guard branch in
    /// `apply_invoice_paid` (`if false && period_start < stored { ... }`)
    /// and the FIRST assertion fails: `thread ... panicked ... the first
    /// delivery is claimed and found stale` — with the guard gone the
    /// "stale" event is just an ordinary older-period invoice and gets
    /// applied normally, so `first.is_some_and(|o| o.is_stale())` is
    /// `false`. Watched fail with exactly that message before restoring the
    /// guard.
    #[tokio::test]
    async fn a_redelivered_stale_invoice_paid_is_a_replay_not_a_second_staleness_check() {
        let Some(pool) = pg_test_pool(
            "a_redelivered_stale_invoice_paid_is_a_replay_not_a_second_staleness_check",
        )
        .await
        else {
            return;
        };
        let (account_id, users, _customer) = seed_linked_account(&pool, 1).await;
        let sub_id = linked_subscription_id(&pool, account_id).await;
        let start2 = chrono::DateTime::from_timestamp(1_700_000_000, 0).unwrap();
        let end2 = chrono::DateTime::from_timestamp(1_702_678_400, 0).unwrap();
        apply_invoice_paid(
            &pool,
            account_id,
            &format!("evt_{}", Uuid::new_v4().simple()),
            "invoice.paid",
            &sub_id,
            start2,
            end2,
        )
        .await
        .expect("seed")
        .expect("not a replay");

        let start0 = start2 - chrono::Duration::days(5);
        let end0 = start2 + chrono::Duration::days(10);
        let event_id = format!("evt_{}", Uuid::new_v4().simple());

        let first = apply_invoice_paid(&pool, account_id, &event_id, "invoice.paid", &sub_id, start0, end0)
            .await
            .expect("first");
        let second = apply_invoice_paid(&pool, account_id, &event_id, "invoice.paid", &sub_id, start0, end0)
            .await
            .expect("second");

        assert!(first.is_some_and(|o| o.is_stale()), "the first delivery is claimed and found stale");
        assert!(second.is_none(), "the redelivered stale event must be an ordinary replay, not re-checked");

        cleanup(&pool, account_id, &users).await;
    }

    /// **Issue #644, at the handler seam.** A mid-cycle proration invoice must
    /// roll NOTHING: no allowance rows, and — the half that actually caused
    /// the outage — no write to `stripe_subscriptions.current_period_start`.
    ///
    /// Both halves of the control are here in one test, because the bug is a
    /// DIFFERENCE between two invoices that are otherwise identical. The
    /// proration and the cycle invoice below share a customer, a subscription
    /// id and a line period; only `billing_reason` differs. If the guard ever
    /// widens to reject cycle invoices too, the second half fails; if it ever
    /// narrows to accept prorations again, the first half fails.
    ///
    /// The numbers are the production incident, not invented. Invoice
    /// `in_example7c21d9ab` carried `billing_reason:
    /// "subscription_update"` with lines starting at `1786836491`, 55 seconds
    /// AFTER the subscription item's own `1786836436`. Storing the former is
    /// what made `evt_example3e8f1a2c` come back `stale`.
    ///
    /// **Negative control.** Delete the `rolls_the_renewal_clock` guard from
    /// `handle_invoice_paid` and the first assertion fails with
    /// `assertion failed: allowance rows after a proration invoice: expected
    /// 0, got 2`, and the stored `current_period_start` becomes
    /// `Some(1786836491)`. Watched fail with exactly that before the guard was
    /// added — this test was written first and observed red.
    #[tokio::test]
    async fn a_proration_invoice_rolls_nothing_but_a_cycle_invoice_still_does() {
        let Some(pool) =
            pg_test_pool("a_proration_invoice_rolls_nothing_but_a_cycle_invoice_still_does").await
        else {
            return;
        };

        // The real skewed values from the incident.
        const PRORATION_START: i64 = 1_786_836_491;
        const CYCLE_END: i64 = 1_789_514_836;

        let invoice = |reason: &str, sub: &str| {
            json!({
                "billing_reason": reason,
                "subscription": sub,
                "customer": "cus_ignored_here",
                "lines": {"data": [{"period": {"start": PRORATION_START, "end": CYCLE_END}}]},
            })
        };

        // ── half 1: the proration must roll nothing ──────────────────────
        let (account_id, users, customer_id) = seed_linked_account(&pool, 2).await;
        let sub_id = linked_subscription_id(&pool, account_id).await;

        handle_invoice_paid(
            &pool,
            None,
            account_id,
            &format!("evt_{}", Uuid::new_v4().simple()),
            "invoice.paid",
            &customer_id,
            &invoice("subscription_update", &sub_id),
        )
        .await;

        let rows = count(
            &pool,
            "SELECT count(*) FROM member_allowance_periods WHERE account_id = $1",
            account_id,
        )
        .await;
        assert_eq!(
            rows, 0,
            "allowance rows after a proration invoice: expected 0, got {rows}"
        );

        let stored: Option<chrono::DateTime<chrono::Utc>> = sqlx::query_scalar(
            "SELECT current_period_start FROM stripe_subscriptions WHERE account_id = $1",
        )
        .bind(account_id)
        .fetch_one(&pool)
        .await
        .expect("read the period mirror");
        assert!(
            stored.is_none(),
            "a proration must not move the period mirror — that is what makes every later \
             subscription event stale (#644); got {stored:?}"
        );

        cleanup(&pool, account_id, &users).await;

        // ── half 2: the identical invoice AS A CYCLE still rolls ─────────
        let (account_id, users, customer_id) = seed_linked_account(&pool, 2).await;
        let sub_id = linked_subscription_id(&pool, account_id).await;

        handle_invoice_paid(
            &pool,
            None,
            account_id,
            &format!("evt_{}", Uuid::new_v4().simple()),
            "invoice.paid",
            &customer_id,
            &invoice("subscription_cycle", &sub_id),
        )
        .await;

        let rows = count(
            &pool,
            "SELECT count(*) FROM member_allowance_periods WHERE account_id = $1",
            account_id,
        )
        .await;
        assert_eq!(
            rows, 2,
            "the guard must not break the renewal clock it protects: expected 2 allowance rows \
             for a cycle invoice, got {rows}"
        );

        cleanup(&pool, account_id, &users).await;
    }

    // ── `customer.subscription.updated` (Phase 4b) ──────────────────────────

    /// A seat increase syncs `grants.seats` and grants ONLY the newly added
    /// member — at the FULL per-seat allowance, never a prorated slice. This
    /// is the founder's locked "mid-period seat add" decision, exercised
    /// through the ACTUAL webhook code path (`apply_subscription_updated`),
    /// not a re-typed copy of `roll_period`'s own already-proven behaviour.
    ///
    /// **Negative control.** Delete the `close_open_period`/`roll_period`
    /// pair from `apply_subscription_updated` (stubbing `closed`/`renewal`
    /// to empty outcomes) and the SECOND assertion fails immediately:
    /// `assertion \`left == right\` failed: only the new member is granted —
    /// the two already holding an allowance for this period conflict on
    /// 0161's UNIQUE index and are skipped / left: 0 / right: 1`. Watched
    /// fail with exactly that message before restoring the call.
    #[tokio::test]
    async fn a_seat_increase_syncs_seats_and_grants_only_the_new_member_in_full() {
        let Some(pool) =
            pg_test_pool("a_seat_increase_syncs_seats_and_grants_only_the_new_member_in_full").await
        else {
            return;
        };
        let (account_id, mut users, _customer) = seed_linked_account(&pool, 2).await;
        let sub_id = linked_subscription_id(&pool, account_id).await;
        let (start, end) = period();
        apply_invoice_paid(
            &pool,
            account_id,
            &format!("evt_{}", Uuid::new_v4().simple()),
            "invoice.paid",
            &sub_id,
            start,
            end,
        )
        .await
        .expect("seed the initial period")
        .expect("not a replay");

        let latecomer = add_member(&pool, account_id).await;
        users.push(latecomer);

        let outcome = apply_subscription_updated(
            &pool,
            account_id,
            &format!("evt_{}", Uuid::new_v4().simple()),
            "customer.subscription.updated",
            3,
            "active",
            start,
            end,
        )
        .await
        .expect("apply")
        .expect("not a replay")
        .applied()
        .expect("the SAME period as what invoice.paid just stored is EQUAL, not older — not stale");

        assert!(outcome.seat_priced, "team_standard is seat-priced");
        assert_eq!(outcome.seats, 3);
        assert_eq!(read_seats(&pool, account_id).await, Some(3), "grants.seats synced");
        assert_eq!(
            outcome.renewal.rows_granted, 1,
            "only the new member is granted — the two already holding an allowance for this \
             period conflict on 0161's UNIQUE index and are skipped"
        );

        let rows: Vec<(Uuid, Decimal)> = sqlx::query_as(
            "SELECT user_id, granted_credits FROM member_allowance_periods \
              WHERE account_id = $1 AND period_start = $2",
        )
        .bind(account_id)
        .bind(start)
        .fetch_all(&pool)
        .await
        .expect("read allowance rows");
        assert_eq!(rows.len(), 3, "all three members now hold a row for this period");
        let late_row = rows
            .iter()
            .find(|(u, _)| *u == latecomer)
            .expect("the newly added member holds an allowance row for THIS period");
        assert_eq!(
            late_row.1,
            Decimal::from(3000),
            "the FULL per-seat allowance, never a prorated slice"
        );
        for (u, credits) in &rows {
            if *u != latecomer {
                assert_eq!(*credits, Decimal::from(3000), "the two original members' rows are untouched");
            }
        }

        cleanup(&pool, account_id, &users).await;
    }

    /// A seat DECREASE syncs `grants.seats` and touches NO allowance rows —
    /// a removed member's unused allowance is neither refunded nor
    /// redistributed (founder decision, module doc).
    ///
    /// **Negative control.** Hardcode the seat-sync UPDATE's second bind to
    /// `3i32` instead of the real `seats` parameter, and the FIRST assertion
    /// fails: `assertion \`left == right\` failed: seats synced down to 2 /
    /// left: Some(3) / right: Some(2)`. Watched fail with exactly that
    /// message before restoring the real bind.
    #[tokio::test]
    async fn a_seat_decrease_syncs_seats_and_leaves_existing_allowance_rows_untouched() {
        let Some(pool) = pg_test_pool(
            "a_seat_decrease_syncs_seats_and_leaves_existing_allowance_rows_untouched",
        )
        .await
        else {
            return;
        };
        let (account_id, users, _customer) = seed_linked_account(&pool, 3).await;
        let sub_id = linked_subscription_id(&pool, account_id).await;
        let (start, end) = period();
        apply_invoice_paid(
            &pool,
            account_id,
            &format!("evt_{}", Uuid::new_v4().simple()),
            "invoice.paid",
            &sub_id,
            start,
            end,
        )
        .await
        .expect("seed the initial period")
        .expect("not a replay");

        let before: Vec<(Uuid, Decimal)> = sqlx::query_as(
            "SELECT user_id, granted_credits FROM member_allowance_periods WHERE account_id = $1",
        )
        .bind(account_id)
        .fetch_all(&pool)
        .await
        .expect("read rows before");

        let outcome = apply_subscription_updated(
            &pool,
            account_id,
            &format!("evt_{}", Uuid::new_v4().simple()),
            "customer.subscription.updated",
            2,
            "active",
            start,
            end,
        )
        .await
        .expect("apply")
        .expect("not a replay")
        .applied()
        .expect("the SAME period as what invoice.paid just stored is EQUAL, not older — not stale");

        assert_eq!(read_seats(&pool, account_id).await, Some(2), "seats synced down to 2");
        assert_eq!(outcome.seats, 2);
        assert_eq!(
            outcome.renewal.rows_granted, 0,
            "no member's roster changed, so re-rolling grants nothing new"
        );

        let after: Vec<(Uuid, Decimal)> = sqlx::query_as(
            "SELECT user_id, granted_credits FROM member_allowance_periods WHERE account_id = $1",
        )
        .bind(account_id)
        .fetch_all(&pool)
        .await
        .expect("read rows after");
        assert_eq!(
            after.len(),
            3,
            "removing a seat does not remove anyone's already-granted allowance row"
        );
        assert_eq!(before, after, "every existing allowance row is byte-for-byte untouched");

        cleanup(&pool, account_id, &users).await;
    }

    /// An ORG account holding the mixed-tier `team` plan — the `apply_
    /// subscription_updated` counterpart to [`seed_linked_account`]'s
    /// `team_standard` shape, needed to exercise review round 2's finding 3
    /// (the `team`-specific seat-sync exception).
    async fn seed_team_linked_account(pool: &PgPool, members: usize) -> (Uuid, Vec<Uuid>, String) {
        // CodeRabbit round 1: `accounts.owner_user_id` is NOT NULL, so this
        // fixture always needs AT LEAST ONE user to own the account — even
        // for `members == 0` (a team grant with a pre-bought pool nobody
        // has joined yet, CS-435's own headline scenario). `.max(1)` seeds
        // that owner unconditionally; the `account_members` loop below
        // still writes exactly `members` roster rows via `.take(members)`,
        // so a `members == 0` caller gets a real account with a real owner
        // FK target but a genuinely empty roster — `users` itself still
        // returns every seeded user (including that owner-only one) so
        // callers' own cleanup deletes it.
        let mut users = Vec::new();
        for i in 0..members.max(1) {
            let id = Uuid::new_v4();
            sqlx::query("INSERT INTO users (id, email) VALUES ($1, $2)")
                .bind(id)
                .bind(format!("stripe-team-{}-{i}@test.example", id.simple()))
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
        let standard_id: Uuid = sqlx::query_scalar(
            "SELECT id FROM seat_tiers WHERE code = 'standard' AND is_active = true",
        )
        .fetch_one(pool)
        .await
        .expect("standard seat tier must be seeded (0165)");
        for (i, u) in users.iter().take(members).enumerate() {
            sqlx::query(
                "INSERT INTO account_members (account_id, user_id, role, seat_tier_id) \
                 VALUES ($1, $2, $3, $4)",
            )
            .bind(account_id)
            .bind(u)
            .bind(if i == 0 { "owner" } else { "member" })
            .bind(standard_id)
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
             VALUES ($1, $2, $3, true) RETURNING id",
        )
        .bind(account_id)
        .bind(plan_id)
        .bind(members as i32),
        )
        .await
        .expect("insert team grant");
        // Billing self-serve PR F: `grant_seat_tiers` is now a PURCHASE
        // LEDGER, not roster-derived — see `billing::renewal`'s own module
        // doc. This fixture represents every seeded member's seat as already
        // purchased at `standard`, so `Σ grant_seat_tiers.seats` (what
        // `apply_subscription_updated`'s drift check now compares Stripe's
        // quantity against) equals `members`, matching what this fixture's
        // callers already assume via `grants.seats = members` above.
        if members > 0 {
            execute_financial_fixture(pool, account_id, sqlx::query(
                "INSERT INTO grant_seat_tiers (grant_id, seat_tier_id, seats) VALUES ($1, $2, $3)",
            )
            .bind(grant_id)
            .bind(standard_id)
            .bind(members as i32))
            .await
            .expect("insert grant_seat_tiers ledger row");
        }

        let customer_id = format!("cus_{}", Uuid::new_v4().simple());
        execute_financial_fixture(pool, account_id, sqlx::query(
            "INSERT INTO stripe_subscriptions (account_id, stripe_customer_id, stripe_subscription_id) \
             VALUES ($1, $2, $3)",
        )
        .bind(account_id)
        .bind(&customer_id)
        .bind(format!("sub_{}", Uuid::new_v4().simple())))
        .await
        .expect("link stripe customer");

        (account_id, users, customer_id)
    }

    /// An ORG account with a seat-tiered roster (same shape
    /// [`seed_team_linked_account`] sets up) but, deliberately, **no
    /// pre-existing `team` grant** — the checkout-completion counterpart
    /// [`seed_team_linked_account`] cannot serve, because a checkout test
    /// that grants `team` needs `issue_grant_in_tx` to actually take the
    /// `Granted` (not `AlreadyGranted`) branch to exercise the
    /// `grant_seat_tiers` write at all. [`seed_linked_account`] cannot serve
    /// either, once PR E's `0170_team_plan_activate.sql` made `team` active
    /// by default: that fixture's account carries no `clerk_org_id`, so its
    /// member's `seat_tier_id` is NULL (`account_members_seat_tier_iff_org`,
    /// 0169), and `PER_MEMBER_GRANT_SQL`'s `LEFT JOIN seat_tiers` then reads
    /// `st.credits` as NULL for a `team` grant's `use_seat_tier_credits`
    /// arm — a `member_allowance_periods.granted_credits` NOT NULL violation
    /// inside `roll_period`, surfacing as this function's generic "grant
    /// issuance failed" 500, not a useful error naming the real cause.
    async fn seed_team_org_account_no_grant(pool: &PgPool, members: usize) -> (Uuid, Vec<Uuid>, String) {
        let mut users = Vec::new();
        for i in 0..members {
            let id = Uuid::new_v4();
            sqlx::query("INSERT INTO users (id, email) VALUES ($1, $2)")
                .bind(id)
                .bind(format!("stripe-teamorg-{}-{i}@test.example", id.simple()))
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
        let standard_id: Uuid = sqlx::query_scalar(
            "SELECT id FROM seat_tiers WHERE code = 'standard' AND is_active = true",
        )
        .fetch_one(pool)
        .await
        .expect("standard seat tier must be seeded (0165)");
        for (i, u) in users.iter().enumerate() {
            sqlx::query(
                "INSERT INTO account_members (account_id, user_id, role, seat_tier_id) \
                 VALUES ($1, $2, $3, $4)",
            )
            .bind(account_id)
            .bind(u)
            .bind(if i == 0 { "owner" } else { "member" })
            .bind(standard_id)
            .execute(pool)
            .await
            .expect("insert tiered membership");
        }

        let customer_id = format!("cus_{}", Uuid::new_v4().simple());
        execute_financial_fixture(pool, account_id, sqlx::query(
            "INSERT INTO stripe_subscriptions (account_id, stripe_customer_id, stripe_subscription_id) \
             VALUES ($1, $2, $3)",
        )
        .bind(account_id)
        .bind(&customer_id)
        .bind(format!("sub_{}", Uuid::new_v4().simple())))
        .await
        .expect("link stripe customer");

        (account_id, users, customer_id)
    }

    /// **PR E review round 2, finding 3; comparison target updated for
    /// billing self-serve PR F.** A `customer.subscription.updated` event
    /// for a `team` grant must NOT write Stripe's reported `quantity` into
    /// `grants.seats` at all — `grants.seats` for a `team` grant is now a
    /// PURCHASE LEDGER total (`Σ grant_seat_tiers.seats`), maintained
    /// exclusively by `billing::seat_lifecycle`/`routes::billing_seats`/
    /// `billing::renewal`'s own pending-downgrade move (see that module's
    /// "`grant_seat_tiers` is a purchase-driven ledger" doc section) — this
    /// handler only WARNS when Stripe's quantity disagrees with that ledger,
    /// never writes either side. Stripe reports a quantity that deliberately
    /// DISAGREES with the fixture's 2-seat ledger here (5 vs. 2) to prove the
    /// warning fires and `grants.seats` is untouched either way.
    ///
    /// **PR E review round 3, finding 4 (still load-bearing under PR F).**
    /// Directly setting `grants.seats` to 99 between the seed and the call —
    /// a value equal to neither the 2-seat ledger nor Stripe's reported
    /// quantity of 5 — is what makes "the write never happened" observable
    /// at all: `roll_period` (called via `apply_subscription_updated` for
    /// the SAME period `apply_invoice_paid` already seeded, so
    /// `already_rolled` is `true` and the pending-downgrade move is skipped
    /// too) has no OTHER writer of `grants.seats` left to blur the result
    /// with, so a persisted value of exactly 99 is unambiguous proof nothing
    /// wrote it. Capturing the `tracing::warn!` below additionally proves
    /// the drift was actually detected, not merely left alone by accident.
    #[tokio::test]
    async fn team_grant_subscription_updated_never_writes_grants_seats_and_warns_on_ledger_drift() {
        let Some(pool) = pg_test_pool(
            "team_grant_subscription_updated_never_writes_grants_seats_and_warns_on_ledger_drift",
        )
        .await
        else {
            return;
        };
        let (account_id, users, _customer) = seed_team_linked_account(&pool, 2).await;
        let sub_id = linked_subscription_id(&pool, account_id).await;
        let (start, end) = period();
        apply_invoice_paid(
            &pool,
            account_id,
            &format!("evt_{}", Uuid::new_v4().simple()),
            "invoice.paid",
            &sub_id,
            start,
            end,
        )
        .await
        .expect("seed the initial period")
        .expect("not a replay");

        // Force `grants.seats` to a value that is neither the real 2-member
        // roster nor the Stripe quantity (5) the call below will report —
        // see the test's own doc comment for why this is what makes the
        // assertion below able to detect a regression at all.
        execute_financial_fixture(
            &pool,
            account_id,
        sqlx::query(
            "UPDATE grants SET seats = 99 WHERE account_id = $1 AND revoked_at IS NULL",
        )
        .bind(account_id),
        )
        .await
        .expect("force grants.seats to a value distinct from both the roster and Stripe's quantity");
        assert_eq!(
            read_seats(&pool, account_id).await,
            Some(99),
            "sanity: the forced value must actually be stored before the call under test runs"
        );

        #[derive(Clone, Default)]
        struct Buf(std::sync::Arc<std::sync::Mutex<Vec<u8>>>);
        impl std::io::Write for Buf {
            fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
                self.0.lock().unwrap().extend_from_slice(b);
                Ok(b.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for Buf {
            type Writer = Buf;
            fn make_writer(&'a self) -> Buf {
                self.clone()
            }
        }
        let buf = Buf::default();
        let sub = tracing_subscriber::fmt()
            .with_writer(buf.clone())
            .with_max_level(tracing::Level::WARN)
            .with_ansi(false)
            .finish();
        // `set_default`'s guard is thread-local but stays active across
        // await points on this single-threaded `#[tokio::test]` task (see
        // `routes::queries`'s identical pattern), so it captures the warn
        // from inside `apply_subscription_updated`'s `.await` below.
        let _guard = tracing::subscriber::set_default(sub);

        let outcome = apply_subscription_updated(
            &pool,
            account_id,
            &format!("evt_{}", Uuid::new_v4().simple()),
            "customer.subscription.updated",
            5, // deliberately disagrees with the real 2-member roster
            "active",
            start,
            end,
        )
        .await
        .expect("apply")
        .expect("not a replay")
        .applied()
        .expect("the SAME period as what invoice.paid just stored is EQUAL, not older — not stale");

        drop(_guard);
        let logged = String::from_utf8(buf.0.lock().unwrap().clone()).unwrap();
        assert!(
            logged.contains("Stripe-reported seat quantity disagrees with"),
            "the drift warn must fire when Stripe's quantity (5) disagrees with the ledger (2):\n{logged}"
        );

        assert_eq!(
            read_seats(&pool, account_id).await,
            Some(99),
            "grants.seats must be left byte-for-byte at the forced pre-call value (99) — NOT \
             Stripe's reported quantity (5), and NOT the ledger total (2) — proving neither this \
             handler NOR roll_period's (skipped, same-period) pending-downgrade move wrote it"
        );
        assert_eq!(
            outcome.seats, 99,
            "the reported outcome must reflect what was actually PERSISTED (99), not the raw \
             Stripe quantity the caller passed in, and not the ledger total either"
        );
        assert!(outcome.seat_priced, "team is seat-priced");

        cleanup(&pool, account_id, &users).await;
    }

    /// Staging run: a seat quantity edited out of band in the Stripe Dashboard
    /// made `customer.subscription.updated` (SAME billing period, different
    /// quantity) delete the team members' allowance periods, so they showed
    /// `member_cap_reached`. A member who joined mid-period holds a period that
    /// starts AFTER the Stripe period boundary and has consumption recorded on
    /// it; `close_open_period` deletes exactly such rows. A same-period event
    /// must leave every existing period (id, window, consumption) untouched.
    #[tokio::test]
    async fn an_out_of_band_seat_quantity_change_keeps_every_members_current_period() {
        let Some(pool) =
            pg_test_pool("an_out_of_band_seat_quantity_change_keeps_every_members_current_period").await
        else {
            return;
        };
        let (account_id, users, _customer) = seed_team_linked_account(&pool, 2).await;
        let sub_id = linked_subscription_id(&pool, account_id).await;
        let (start, end) = period();
        apply_invoice_paid(
            &pool,
            account_id,
            &format!("evt_{}", Uuid::new_v4().simple()),
            "invoice.paid",
            &sub_id,
            start,
            end,
        )
        .await
        .expect("seed the initial period")
        .expect("not a replay");
        // The second member joined an hour into the period: their row starts at
        // their join time and has already spent some of it.
        let joined = start + chrono::Duration::hours(1);
        execute_financial_fixture(
            &pool,
            account_id,
            sqlx::query(
                "UPDATE member_allowance_periods SET period_start = $3, consumed_credits = 7 \
                  WHERE account_id = $1 AND user_id = $2",
            )
            .bind(account_id)
            .bind(users[1])
            .bind(joined),
        )
        .await
        .expect("move the second member's period to their join time");
        let snapshot = |pool: PgPool| async move {
            sqlx::query_as::<_, (Uuid, Uuid, chrono::DateTime<chrono::Utc>, chrono::DateTime<chrono::Utc>, Decimal)>(
                "SELECT id, user_id, period_start, period_end, consumed_credits \
                   FROM member_allowance_periods WHERE account_id = $1 ORDER BY user_id",
            )
            .bind(account_id)
            .fetch_all(&pool)
            .await
            .unwrap()
        };
        let mut before = snapshot(pool.clone()).await;
        before.sort();

        // Stripe reports the same period with a different quantity.
        apply_subscription_updated(
            &pool,
            account_id,
            &format!("evt_{}", Uuid::new_v4().simple()),
            "customer.subscription.updated",
            5,
            "active",
            start,
            end,
        )
        .await
        .expect("apply")
        .expect("not a replay")
        .applied()
        .expect("same period is not stale");

        let mut after = snapshot(pool.clone()).await;
        after.sort();
        cleanup(&pool, account_id, &users).await;
        assert_eq!(before.len(), 2, "both members hold a period before: {before:?}");
        assert_eq!(
            after, before,
            "a same-period quantity change must not delete, re-create or reset any member's period"
        );
    }

    /// **CS-435 (D1/D7, org-onboarding plan §3 item 7).** The drift warning
    /// above compares Stripe's reported quantity against the LOCAL LEDGER
    /// total (`Σ grant_seat_tiers.seats`) — never against the roster
    /// (`account_members` count). That comparison needs no change for
    /// CS-435: an admin's `routes::org_slots` "buy N slots" purchase pushes
    /// Stripe AND increments the ledger in the same transaction, so in
    /// steady state the two always agree regardless of how many members
    /// have actually joined. This test proves it directly: THREE slots
    /// purchased, the roster's one member UNPURCHASED
    /// (`account_members.seat_purchased_at IS NULL` —
    /// `seed_team_linked_account` never sets it; `billing::slot_pool::
    /// SlotPool::used == 0`, so `available() == 3`, CS-435's own healthy
    /// pre-bought-pool state), and Stripe reporting the SAME quantity (3)
    /// as the ledger — no warning fires, and `grants.seats` is left at the
    /// forced sentinel either way, exactly like the disagreeing-ledger case
    /// above.
    #[tokio::test]
    async fn team_grant_subscription_updated_does_not_warn_on_a_healthy_pre_bought_pool() {
        let Some(pool) = pg_test_pool(
            "team_grant_subscription_updated_does_not_warn_on_a_healthy_pre_bought_pool",
        )
        .await
        else {
            return;
        };
        // ONE member — `seed_team_linked_account(pool, 0)` panics
        // (`users[0]` — the fixture always needs an `owner_user_id` for the
        // `accounts` FK, regardless of roster size), and this member is
        // never stamped `seat_purchased_at` by the fixture anyway, so
        // `used` stays 0 — the "nobody has actually consumed this pool"
        // property this test needs holds regardless. `if members > 0` seeds
        // ONE `grant_seat_tiers` row at `seats = 1`; bumped to 3 below via
        // UPDATE (not a second INSERT, which would violate the
        // `(grant_id, seat_tier_id)` uniqueness `grant_seat_tiers` enforces).
        let (account_id, users, _customer) = seed_team_linked_account(&pool, 1).await;
        let standard_id: Uuid =
            sqlx::query_scalar("SELECT id FROM seat_tiers WHERE code = 'standard' AND is_active = true")
                .fetch_one(&pool)
                .await
                .expect("standard seat tier must be seeded (0165)");
        let grant_id: Uuid = sqlx::query_scalar(
            "SELECT id FROM grants WHERE account_id = $1 AND revoked_at IS NULL",
        )
        .bind(account_id)
        .fetch_one(&pool)
        .await
        .expect("the team grant seed_team_linked_account already inserted");
        execute_financial_fixture(&pool, account_id, sqlx::query("UPDATE grant_seat_tiers SET seats = 3 WHERE grant_id = $1 AND seat_tier_id = $2")
            .bind(grant_id)
            .bind(standard_id))
            .await
            .expect("bump the pre-bought pool to 3 — only 0 of them consumed (the roster member is unpurchased)");

        let sub_id = linked_subscription_id(&pool, account_id).await;
        let (start, end) = period();
        apply_invoice_paid(
            &pool,
            account_id,
            &format!("evt_{}", Uuid::new_v4().simple()),
            "invoice.paid",
            &sub_id,
            start,
            end,
        )
        .await
        .expect("seed the initial period")
        .expect("not a replay");

        execute_financial_fixture(
            &pool,
            account_id,
            sqlx::query("UPDATE grants SET seats = 42 WHERE account_id = $1 AND revoked_at IS NULL")
            .bind(account_id),
        )
            .await
            .expect("force a sentinel value distinct from both 0 (roster) and 3 (ledger/Stripe)");

        #[derive(Clone, Default)]
        struct Buf(std::sync::Arc<std::sync::Mutex<Vec<u8>>>);
        impl std::io::Write for Buf {
            fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
                self.0.lock().unwrap().extend_from_slice(b);
                Ok(b.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for Buf {
            type Writer = Buf;
            fn make_writer(&'a self) -> Buf {
                self.clone()
            }
        }
        let buf = Buf::default();
        let sub = tracing_subscriber::fmt()
            .with_writer(buf.clone())
            .with_max_level(tracing::Level::WARN)
            .with_ansi(false)
            .finish();
        let _guard = tracing::subscriber::set_default(sub);

        let outcome = apply_subscription_updated(
            &pool,
            account_id,
            &format!("evt_{}", Uuid::new_v4().simple()),
            "customer.subscription.updated",
            3, // agrees with the 3-seat ledger — even though 0 members exist
            "active",
            start,
            end,
        )
        .await
        .expect("apply")
        .expect("not a replay")
        .applied()
        .expect("the SAME period as what invoice.paid just stored is EQUAL, not older — not stale");

        drop(_guard);
        let logged = String::from_utf8(buf.0.lock().unwrap().clone()).unwrap();
        assert!(
            !logged.contains("Stripe-reported seat quantity disagrees with"),
            "a pre-bought pool (3 purchased, 0 used) must NOT warn — Stripe (3) and the ledger (3) \
             genuinely agree, and this comparison was never roster-based to begin with:\n{logged}"
        );

        assert_eq!(
            read_seats(&pool, account_id).await,
            Some(42),
            "grants.seats stays at the forced sentinel — this handler never writes it for a team grant"
        );
        assert_eq!(outcome.seats, 42);

        cleanup(&pool, account_id, &users).await;
    }

    /// A flat-plan grant is never given a seat count — `0156`'s
    /// `grants_seats_iff_seat_priced` CHECK would reject
    /// `plan_is_seat_priced = false` paired with a non-NULL `seats`, so the
    /// seat-sync UPDATE must be a targeted no-op for this account, not a
    /// blind write that trips the constraint.
    ///
    /// **Negative control.** Remove the `AND g.plan_is_seat_priced = true`
    /// clause from the seat-sync UPDATE in `apply_subscription_updated` and
    /// this test's `.expect(...)` on `apply_subscription_updated`'s own
    /// `Result` panics with a database error: `PgDatabaseError { code:
    /// "23514" (check_violation), constraint:
    /// Some("grants_seats_iff_seat_priced") ... }` — the write never reaches
    /// the `Ok` branch at all. Watched fail with exactly that constraint
    /// name before restoring the guard.
    #[tokio::test]
    async fn a_flat_plan_grant_never_receives_a_seat_count() {
        let Some(pool) = pg_test_pool("a_flat_plan_grant_never_receives_a_seat_count").await else {
            return;
        };
        let (account_id, users, _customer) = seed_flat_plan_account(&pool).await;
        let (start, end) = period();

        let outcome = apply_subscription_updated(
            &pool,
            account_id,
            &format!("evt_{}", Uuid::new_v4().simple()),
            "customer.subscription.updated",
            1,
            "active",
            start,
            end,
        )
        .await
        .expect("apply must succeed even for a flat-plan account — the CHECK must never fire")
        .expect("not a replay")
        .applied()
        .expect("a fresh account with no stored period has nothing to be stale against");

        assert!(!outcome.seat_priced, "the current grant is a flat plan");
        assert_eq!(
            read_seats(&pool, account_id).await,
            None,
            "seats must remain NULL — writing a seat count here would violate 0156's CHECK"
        );

        cleanup(&pool, account_id, &users).await;
    }

    /// #1557: Stripe's `trialing` is mirrored as `active` on purpose. Pin the
    /// whole contract, so a change to the mapping cannot silently drop a
    /// trialing org's access: the raw status lands as `active`, the freeze
    /// predicate reads it as paid-up, and provisioning treats it as `Live`.
    #[tokio::test]
    async fn a_trialing_subscription_is_mirrored_as_active_and_read_as_paid_up() {
        let Some(pool) = pg_test_pool("a_trialing_subscription_is_mirrored_as_active_and_read_as_paid_up").await else {
            return;
        };
        let (account_id, users, _customer) = seed_linked_account(&pool, 1).await;
        let (start, end) = period();

        let status = map_subscription_status("trialing");
        apply_subscription_updated(
            &pool,
            account_id,
            &format!("evt_{}", Uuid::new_v4().simple()),
            "customer.subscription.updated",
            1,
            status,
            start,
            end,
        )
        .await
        .expect("apply")
        .expect("not a replay");

        let mirrored = read_status(&pool, account_id).await;
        let frozen = crate::billing::enforcement::status_is_frozen(Some(&mirrored));
        let frozen_in_db = crate::billing::enforcement::subscription_frozen(&pool, account_id).await;
        let provisioning = crate::tenancy::provisioning::subscription_state(&pool, account_id).await;
        cleanup(&pool, account_id, &users).await;

        assert_eq!(mirrored, "active", "a trialing subscription is mirrored as active");
        assert!(!frozen, "a trialing org is not frozen");
        assert!(!frozen_in_db, "the DB-backed freeze read agrees");
        assert_eq!(provisioning.unwrap(), crate::tenancy::provisioning::SubscriptionState::Live);
    }

    /// The freeze half of the founder's "involuntary lapse -> frozen, then
    /// restored", wired through THIS event for the first time (Phase 4a only
    /// ever wrote `status = 'active'`). Chained with a subsequent
    /// `invoice.paid`, whose restore-to-`active` behaviour is already proven
    /// by `a_paid_invoice_restores_a_past_due_subscription` — this test's job
    /// is only to prove the freeze half now exists and the two compose.
    #[tokio::test]
    async fn subscription_updated_past_due_freezes_and_a_later_invoice_paid_restores_active() {
        let Some(pool) = pg_test_pool(
            "subscription_updated_past_due_freezes_and_a_later_invoice_paid_restores_active",
        )
        .await
        else {
            return;
        };
        let (account_id, users, _customer) = seed_linked_account(&pool, 1).await;
        let sub_id = linked_subscription_id(&pool, account_id).await;
        let (start, end) = period();

        let status = map_subscription_status("past_due");
        assert_eq!(status, "past_due");
        apply_subscription_updated(
            &pool,
            account_id,
            &format!("evt_{}", Uuid::new_v4().simple()),
            "customer.subscription.updated",
            1,
            status,
            start,
            end,
        )
        .await
        .expect("apply")
        .expect("not a replay");

        assert_eq!(read_status(&pool, account_id).await, "past_due", "the involuntary lapse froze the account");

        apply_invoice_paid(
            &pool,
            account_id,
            &format!("evt_{}", Uuid::new_v4().simple()),
            "invoice.paid",
            &sub_id,
            start,
            end,
        )
        .await
        .expect("apply")
        .expect("not a replay");

        assert_eq!(read_status(&pool, account_id).await, "active", "paying restores the frozen subscription");

        cleanup(&pool, account_id, &users).await;
    }

    /// Redelivery of a `customer.subscription.updated` event claims once and
    /// does the seat sync / roll exactly once — the identical guarantee
    /// `invoice.paid` already has, now proven for this event too.
    #[tokio::test]
    async fn a_redelivered_subscription_updated_event_is_claimed_once_and_does_nothing_twice() {
        let Some(pool) = pg_test_pool(
            "a_redelivered_subscription_updated_event_is_claimed_once_and_does_nothing_twice",
        )
        .await
        else {
            return;
        };
        let (account_id, users, _customer) = seed_linked_account(&pool, 2).await;
        let (start, end) = period();
        let event_id = format!("evt_{}", Uuid::new_v4().simple());

        let first = apply_subscription_updated(
            &pool, account_id, &event_id, "customer.subscription.updated", 5, "active", start, end,
        )
        .await
        .expect("first");
        let second = apply_subscription_updated(
            &pool, account_id, &event_id, "customer.subscription.updated", 5, "active", start, end,
        )
        .await
        .expect("second");

        assert!(first.is_some());
        assert!(second.is_none(), "the redelivered event must be reported as a replay");
        assert_eq!(
            count(&pool, "SELECT COUNT(*) FROM stripe_webhook_events WHERE account_id = $1", account_id).await,
            1
        );

        cleanup(&pool, account_id, &users).await;
    }

    /// `handle_subscription_updated` — the SAME per-event dispatch function
    /// `stripe_webhook`'s match arm calls — resolves a period that exists
    /// ONLY on the item, exactly the live-API shape this module's doc
    /// comment documents.
    #[tokio::test]
    async fn handle_subscription_updated_resolves_a_period_found_only_on_the_item() {
        let Some(pool) =
            pg_test_pool("handle_subscription_updated_resolves_a_period_found_only_on_the_item").await
        else {
            return;
        };
        let (account_id, users, _customer) = seed_linked_account(&pool, 1).await;
        let (start, end) = period();
        let sub = json!({
            "status": "active",
            "items": {"data": [{
                "quantity": 1,
                "current_period_start": start.timestamp(),
                "current_period_end": end.timestamp(),
            }]}
        });

        let response =
            handle_subscription_updated(&pool, None, account_id, &format!("evt_{}", Uuid::new_v4().simple()), "customer.subscription.updated", &sub)
                .await;
        assert_eq!(response.status(), StatusCode::OK, "an item-only period must resolve, not 400");

        cleanup(&pool, account_id, &users).await;
    }

    /// A `customer.subscription.updated` payload with NO resolvable period —
    /// neither on the item nor at the top level — must be a LOUD 400, never
    /// a silent 200 that quietly grants nothing. This is this repo's
    /// recurring "absence renders as success" failure, and it must fail
    /// loudly at the boundary rather than disappearing into a 200.
    ///
    /// **Negative control.** Change `handle_subscription_updated`'s period
    /// guard from `let Some((start_secs, end_secs)) = subscription_period(sub)
    /// else { ...400... }` to silently default to some placeholder period
    /// instead of returning early, and this test's FIRST assertion fails:
    /// `assertion \`left == right\` failed: an unresolvable period must be a
    /// loud 400, never a silent 200 / left: 200 / right: 400`. Watched fail
    /// with exactly that message before restoring the guard.
    #[tokio::test]
    async fn handle_subscription_updated_with_no_resolvable_period_is_a_loud_400_not_a_silent_success() {
        let Some(pool) = pg_test_pool(
            "handle_subscription_updated_with_no_resolvable_period_is_a_loud_400_not_a_silent_success",
        )
        .await
        else {
            return;
        };
        let (account_id, users, _customer) = seed_linked_account(&pool, 1).await;
        let event_id = format!("evt_{}", Uuid::new_v4().simple());
        // No `current_period_start`/`current_period_end` anywhere — neither on
        // an item nor at the top level.
        let sub = json!({"status": "active", "items": {"data": [{"quantity": 1}]}});

        let response =
            handle_subscription_updated(&pool, None, account_id, &event_id, "customer.subscription.updated", &sub)
                .await;
        assert_eq!(
            response.status(),
            StatusCode::BAD_REQUEST,
            "an unresolvable period must be a loud 400, never a silent 200"
        );
        assert_eq!(
            count(&pool, "SELECT COUNT(*) FROM stripe_webhook_events WHERE account_id = $1", account_id).await,
            0,
            "the event must never have been claimed — nothing was silently \"processed\""
        );

        cleanup(&pool, account_id, &users).await;
    }

    /// The same staleness guard, on the OTHER call site Fix B touches. A
    /// `customer.subscription.updated` carrying an OLDER period than what is
    /// already stored — a stale seat-change notification arriving after a
    /// newer one already landed — must sync NEITHER the seat count NOR the
    /// period: "a stale event's seat count is just as stale as its period"
    /// (module doc).
    ///
    /// **Negative control.** Disable the staleness-guard branch in
    /// `apply_subscription_updated` (`if false && period_start < stored {
    /// ... }`) and `outcome.is_stale()` fails first: `thread ... panicked
    /// ... an older-than-stored period must be reported as stale` — with
    /// the guard gone the event applies normally. Confirmed directly
    /// against the database with the guard still disabled: `grants.seats`
    /// for this account read back `Some(9)` — the stale event's seat count
    /// — not the `Some(2)` a newer, already-applied change had set it to;
    /// the seat-sync UPDATE ran unconditionally on the stale value and
    /// silently rolled the account's seat count BACKWARD. Watched both fail
    /// exactly that way before restoring the guard.
    #[tokio::test]
    async fn a_stale_subscription_updated_does_not_sync_seats_or_touch_the_newer_period() {
        let Some(pool) = pg_test_pool(
            "a_stale_subscription_updated_does_not_sync_seats_or_touch_the_newer_period",
        )
        .await
        else {
            return;
        };
        let (account_id, users, _customer) = seed_linked_account(&pool, 2).await;
        let sub_id = linked_subscription_id(&pool, account_id).await;
        let start2 = chrono::DateTime::from_timestamp(1_700_000_000, 0).unwrap();
        let end2 = chrono::DateTime::from_timestamp(1_702_678_400, 0).unwrap();
        apply_invoice_paid(
            &pool,
            account_id,
            &format!("evt_{}", Uuid::new_v4().simple()),
            "invoice.paid",
            &sub_id,
            start2,
            end2,
        )
        .await
        .expect("seed the newer, already-granted period")
        .expect("not a replay")
        .applied()
        .expect("a fresh account's first invoice cannot be stale");
        assert_eq!(read_seats(&pool, account_id).await, Some(2), "seeded at 2 seats");

        let newer_before: Vec<(Uuid, Decimal)> = sqlx::query_as(
            "SELECT user_id, granted_credits FROM member_allowance_periods WHERE account_id = $1",
        )
        .bind(account_id)
        .fetch_all(&pool)
        .await
        .expect("read rows before");

        // A STALE seat change: reports 9 seats against a period OLDER than
        // what is already stored.
        let start0 = start2 - chrono::Duration::days(5);
        let end0 = start2 + chrono::Duration::days(10);
        let event_id = format!("evt_{}", Uuid::new_v4().simple());
        let outcome = apply_subscription_updated(
            &pool, account_id, &event_id, "customer.subscription.updated", 9, "active", start0, end0,
        )
        .await
        .expect("apply")
        .expect("a stale event is still claimed, not a replay");

        assert!(outcome.is_stale(), "an older-than-stored period must be reported as stale");
        assert_eq!(
            read_seats(&pool, account_id).await,
            Some(2),
            "the stale event's seat count must never be synced"
        );

        let newer_after: Vec<(Uuid, Decimal)> = sqlx::query_as(
            "SELECT user_id, granted_credits FROM member_allowance_periods WHERE account_id = $1",
        )
        .bind(account_id)
        .fetch_all(&pool)
        .await
        .expect("read rows after");
        assert_eq!(newer_before, newer_after, "no allowance row may be touched by a stale event");

        let (stored_start,): (Option<chrono::DateTime<chrono::Utc>>,) =
            sqlx::query_as("SELECT current_period_start FROM stripe_subscriptions WHERE account_id = $1")
                .bind(account_id)
                .fetch_one(&pool)
                .await
                .expect("read subscription mirror");
        assert_eq!(stored_start, Some(start2), "the period mirror must not move backward");

        cleanup(&pool, account_id, &users).await;
    }

    /// The guard's other edge, proven directly against `apply_subscription_updated`
    /// (the invoice.paid side of this is `two_overlapping_invoices_for_different_periods_both_succeed`
    /// above): a GENUINELY NEWER period than what is already stored — real,
    /// in-order forward progress — must never be mistaken for stale. This is
    /// the guard against the guard itself: proves Fix B did not simply make
    /// `apply_subscription_updated` refuse everything once a period is on file.
    ///
    /// **Negative control.** Swap the guard's comparison operands, from
    /// `period_start < stored` to `stored < period_start` — a plausible
    /// transcription slip that reads almost identically but inverts the
    /// direction entirely, flagging anything NEWER as stale instead of
    /// anything OLDER — and this test panics: `thread ... panicked ... a
    /// genuinely newer period must still apply normally, never be treated
    /// as stale`. (A `<=` tightening, by contrast, does NOT trip this
    /// specific test — `start2 > start1` strictly, so `<=` and `<` agree
    /// here; `<=` is instead what `a_new_event_for_an_already_granted_
    /// period_grants_nothing_extra`'s own inline `.expect(...)` message
    /// catches, on the EQUAL-timestamp case.) Watched fail with the message
    /// above before restoring the correct operand order.
    #[tokio::test]
    async fn a_subscription_updated_event_with_a_genuinely_newer_period_still_applies() {
        let Some(pool) = pg_test_pool(
            "a_subscription_updated_event_with_a_genuinely_newer_period_still_applies",
        )
        .await
        else {
            return;
        };
        let (account_id, users, _customer) = seed_linked_account(&pool, 2).await;
        let sub_id = linked_subscription_id(&pool, account_id).await;
        let (start1, end1) = period();
        apply_invoice_paid(
            &pool,
            account_id,
            &format!("evt_{}", Uuid::new_v4().simple()),
            "invoice.paid",
            &sub_id,
            start1,
            end1,
        )
        .await
        .expect("seed")
        .expect("not a replay")
        .applied()
        .expect("a fresh account's first invoice cannot be stale");

        // Strictly NEWER than what is stored.
        let start2 = end1;
        let end2 = start2 + chrono::Duration::days(31);
        assert!(start2 > start1, "must be genuinely newer");

        let outcome = apply_subscription_updated(
            &pool, account_id, &format!("evt_{}", Uuid::new_v4().simple()),
            "customer.subscription.updated", 5, "active", start2, end2,
        )
        .await
        .expect("apply")
        .expect("not a replay");

        assert!(!outcome.is_stale(), "a genuinely newer period must still apply normally, never be treated as stale");
        let applied = outcome.applied().expect("just proved not stale");
        assert_eq!(read_seats(&pool, account_id).await, Some(5), "the newer seat count IS synced");
        assert_eq!(applied.seats, 5);

        cleanup(&pool, account_id, &users).await;
    }

    /// Fix C. `i32::try_from` (already in place before this fix) only catches
    /// a seat count OUT OF `i32` RANGE — it happily lets `0` and every
    /// negative `i32` through, and Stripe genuinely sends `quantity: 0` (a
    /// subscription paused at zero seats, or a race with a cancellation).
    /// Without the `seats < 1` guard, that value reaches
    /// `apply_subscription_updated`'s seat-sync UPDATE, trips `0156`'s
    /// `grants.seats CHECK (seats IS NULL OR seats >= 1)` as a raw `23514`
    /// database error, and the handler's `Err(e) => 500` arm turns that into
    /// an endlessly-retried event — Stripe can never send a value that makes
    /// the retry succeed.
    ///
    /// **Negative control.** Delete the `if seats < 1 { ...400... }` block
    /// from `handle_subscription_updated` (leaving only the `i32::try_from`
    /// guard, which `0` and `-1` both pass through unchanged) and the FIRST
    /// assertion fails on `quantity: 0`: `assertion \`left == right\` failed:
    /// quantity 0 must answer 400, never reach the database / left: 500 /
    /// right: 400`. Watched fail with exactly that message — the request
    /// reached `apply_subscription_updated`, tripped `grants_seats` (23514),
    /// and the handler's generic `Err(e) => 500` arm caught it — before
    /// restoring the guard.
    #[tokio::test]
    async fn handle_subscription_updated_rejects_a_zero_or_negative_seat_quantity_with_400_not_500() {
        for quantity in [0_i64, -1_i64, -100_i64] {
            let Some(pool) = pg_test_pool(
                "handle_subscription_updated_rejects_a_zero_or_negative_seat_quantity_with_400_not_500",
            )
            .await
            else {
                return;
            };
            let (account_id, users, _customer) = seed_linked_account(&pool, 1).await;
            let (start, end) = period();
            let event_id = format!("evt_{}", Uuid::new_v4().simple());
            let sub = json!({
                "status": "active",
                "items": {"data": [{
                    "quantity": quantity,
                    "current_period_start": start.timestamp(),
                    "current_period_end": end.timestamp(),
                }]}
            });

            let response = handle_subscription_updated(
                &pool, None, account_id, &event_id, "customer.subscription.updated", &sub,
            )
            .await;
            assert_eq!(
                response.status(),
                StatusCode::BAD_REQUEST,
                "quantity {quantity} must answer 400, never reach the database"
            );
            assert_eq!(
                count(&pool, "SELECT COUNT(*) FROM stripe_webhook_events WHERE account_id = $1", account_id)
                    .await,
                0,
                "quantity {quantity}: the event must never have been claimed"
            );
            assert_eq!(
                read_seats(&pool, account_id).await,
                Some(1),
                "quantity {quantity}: the account's real grant must be completely untouched"
            );

            cleanup(&pool, account_id, &users).await;
        }
    }

    // ── `customer.subscription.deleted` — freeze-not-forfeit (plan §1.11) ───

    /// **The regression test for freeze-not-forfeit.** EVERY deletion reason
    /// — a deliberate `cancellation_requested`, an involuntary
    /// `payment_failed`/`payment_disputed`, or an absent reason (a natural,
    /// non-cancelled expiry) — does EXACTLY the same thing: `status` flips
    /// to `"canceled"` and nothing else changes. The balance, the grant, and
    /// every in-flight allowance row are left completely alone for every
    /// reason in the loop, including `cancellation_requested`, which the
    /// RETIRED Phase 4b forfeiture arm used to treat differently (zero the
    /// balance, write a debit, revoke every grant, close the open period).
    ///
    /// **Negative control.** Reintroduce a `cancellation_reason ==
    /// Some("cancellation_requested")` branch inside `apply_subscription_
    /// deleted` that revokes the grant, and this loop's `cancellation_requested`
    /// iteration fails at `!latest_grant_revoked(...)`: "reason
    /// Some(\"cancellation_requested\"): grant must NOT be revoked — freeze
    /// never touches it". Watched fail with exactly that message before
    /// restoring the unconditional freeze.
    #[tokio::test]
    async fn deleted_freezes_identically_on_every_reason_leaving_everything_else_untouched() {
        for reason in [
            Some("cancellation_requested"),
            Some("payment_failed"),
            Some("payment_disputed"),
            None,
        ] {
            let Some(pool) = pg_test_pool(
                "deleted_freezes_identically_on_every_reason_leaving_everything_else_untouched",
            )
            .await
            else {
                return;
            };
            let (account_id, users, _customer) = seed_linked_account(&pool, 2).await;
            let sub_id = linked_subscription_id(&pool, account_id).await;
            // A real open period, so a regression that reintroduces
            // `close_open_period` on this path would visibly shrink it.
            // Whole-second precision: Postgres `timestamptz` stores
            // microseconds, `chrono::Utc::now()` carries nanoseconds — this
            // repo's standing flake source (see PR A's handoff notes) when a
            // Rust-side value is compared byte-for-byte against one read
            // back from the database, as this test's final assertion does.
            let now = chrono::DateTime::from_timestamp(chrono::Utc::now().timestamp(), 0).unwrap();
            let start = now - chrono::Duration::days(10);
            let end = now + chrono::Duration::days(20);
            apply_invoice_paid(
                &pool,
                account_id,
                &format!("evt_{}", Uuid::new_v4().simple()),
                "invoice.paid",
                &sub_id,
                start,
                end,
            )
            .await
            .expect("seed the open period")
            .expect("not a replay");
            set_balance(&pool, account_id, Decimal::from(500)).await;

            let event_id = format!("evt_{}", Uuid::new_v4().simple());
            let outcome = apply_subscription_deleted(
                &pool,
                account_id,
                &event_id,
                "customer.subscription.deleted",
                &sub_id,
                reason,
            )
            .await
            .expect("apply")
            .expect("not a replay");

            match &outcome {
                SubscriptionDeleteOutcome::Frozen { status, reason: recorded_reason } => {
                    assert_eq!(*status, "canceled", "reason {reason:?}: status always flips to canceled");
                    assert_eq!(
                        *recorded_reason,
                        reason.map(str::to_string),
                        "reason {reason:?}: the reported reason is carried through verbatim for the audit trail"
                    );
                }
                SubscriptionDeleteOutcome::Superseded { .. } => {
                    panic!("reason {reason:?}: a matching subscription id must freeze, not be superseded: {outcome:?}")
                }
            }
            assert_eq!(
                read_balance(&pool, account_id).await,
                Some(Decimal::from(500)),
                "reason {reason:?}: purchased credits SURVIVE — the balance must be untouched"
            );
            assert!(
                !latest_grant_revoked(&pool, account_id).await,
                "reason {reason:?}: the grant must NOT be revoked — freeze never touches it"
            );
            assert_eq!(read_status(&pool, account_id).await, "canceled");

            // The open period must be left exactly as it was — no shrink, no close.
            let stored_end: chrono::DateTime<chrono::Utc> = sqlx::query_scalar(
                "SELECT period_end FROM member_allowance_periods \
                  WHERE account_id = $1 AND period_start = $2 LIMIT 1",
            )
            .bind(account_id)
            .bind(start)
            .fetch_one(&pool)
            .await
            .expect("read allowance row");
            assert_eq!(
                stored_end, end,
                "reason {reason:?}: the in-flight period must be left completely alone"
            );

            cleanup(&pool, account_id, &users).await;
        }
    }

    /// Idempotency: a redelivered `customer.subscription.deleted` event must
    /// be reported as a replay, not re-applied — the same
    /// `stripe_webhook_events` claim every other handler in this file relies
    /// on. Simpler than the retired forfeiture-era version of this test
    /// (there is no ledger row whose count could double any more), but the
    /// claim itself is unchanged and still worth proving directly.
    #[tokio::test]
    async fn a_redelivered_subscription_deleted_event_is_a_replay_not_reapplied() {
        let Some(pool) =
            pg_test_pool("a_redelivered_subscription_deleted_event_is_a_replay_not_reapplied").await
        else {
            return;
        };
        let (account_id, users, _customer) = seed_linked_account(&pool, 1).await;
        let sub_id = linked_subscription_id(&pool, account_id).await;
        let event_id = format!("evt_{}", Uuid::new_v4().simple());

        let first = apply_subscription_deleted(
            &pool, account_id, &event_id, "customer.subscription.deleted", &sub_id, Some("cancellation_requested"),
        )
        .await
        .expect("first");
        let second = apply_subscription_deleted(
            &pool, account_id, &event_id, "customer.subscription.deleted", &sub_id, Some("cancellation_requested"),
        )
        .await
        .expect("second");

        assert!(first.is_some(), "the first delivery freezes");
        assert!(second.is_none(), "the redelivered event must be reported as a replay, not re-applied");
        assert_eq!(read_status(&pool, account_id).await, "canceled");

        cleanup(&pool, account_id, &users).await;
    }

    /// **Regression test for finding 6 (billing self-serve PR D review
    /// round): cancel -> resubscribe -> late `customer.subscription.deleted`
    /// for the OLD subscription.** The account cancels (subscription A
    /// deleted, freezes), then resubscribes (a fresh checkout links a NEW
    /// subscription id B and restores `status = "active"`) — and only THEN
    /// does Stripe's late-delivered `customer.subscription.deleted` for A
    /// finally arrive. It must NOT re-freeze the currently-paying account.
    #[tokio::test]
    async fn a_late_deleted_event_for_a_superseded_subscription_does_not_refreeze() {
        let Some(pool) =
            pg_test_pool("a_late_deleted_event_for_a_superseded_subscription_does_not_refreeze").await
        else {
            return;
        };
        let (account_id, users, _customer) = seed_linked_account(&pool, 1).await;
        let old_sub_id = linked_subscription_id(&pool, account_id).await;

        // Cancel A — freezes normally (positive control for the "matching
        // id" arm of this same guard).
        let deleted_a = apply_subscription_deleted(
            &pool, account_id, &format!("evt_{}", Uuid::new_v4().simple()),
            "customer.subscription.deleted", &old_sub_id, Some("cancellation_requested"),
        )
        .await
        .expect("apply")
        .expect("not a replay");
        assert!(
            matches!(deleted_a, SubscriptionDeleteOutcome::Frozen { .. }),
            "deleting the CURRENTLY linked subscription must freeze: {deleted_a:?}"
        );
        assert_eq!(read_status(&pool, account_id).await, "canceled");

        // Resubscribe — a fresh checkout links a NEW subscription id and
        // restores active, exactly what `apply_checkout_completed_subscription`
        // + `apply_invoice_paid` do together in production.
        let new_sub_id = format!("sub_{}", Uuid::new_v4().simple());
        execute_financial_fixture(
            &pool,
            account_id,
        sqlx::query(
            "UPDATE stripe_subscriptions SET stripe_subscription_id = $2, status = 'active' \
              WHERE account_id = $1",
        )
        .bind(account_id)
        .bind(&new_sub_id),
        )
        .await
        .expect("simulate the resubscribe");

        // The LATE deletion for the OLD (superseded) subscription A finally
        // arrives, under a fresh event_id (a genuinely new Stripe event, not
        // a redelivery of the one already claimed above).
        let late = apply_subscription_deleted(
            &pool, account_id, &format!("evt_{}", Uuid::new_v4().simple()),
            "customer.subscription.deleted", &old_sub_id, Some("cancellation_requested"),
        )
        .await
        .expect("apply")
        .expect("not a replay — a fresh event_id");

        assert!(
            matches!(late, SubscriptionDeleteOutcome::Superseded { .. }),
            "a late deletion for a superseded subscription id must be a no-op: {late:?}"
        );
        assert_eq!(
            read_status(&pool, account_id).await, "active",
            "the freshly-resubscribed, currently-paying account must NOT be refrozen"
        );
        let stored_sub: Option<String> = sqlx::query_scalar(
            "SELECT stripe_subscription_id FROM stripe_subscriptions WHERE account_id = $1",
        )
        .bind(account_id)
        .fetch_one(&pool)
        .await
        .expect("read stored subscription id");
        assert_eq!(
            stored_sub.as_deref(), Some(new_sub_id.as_str()),
            "the NEW subscription id must remain linked, untouched by the late event for the old one"
        );

        cleanup(&pool, account_id, &users).await;
    }

    // ── `checkout.session.completed` (billing self-serve PR D) ──────────────

    /// A local axum server standing in for `GET /v1/subscriptions/{id}` —
    /// this repo's established mock-HTTP substitute (see
    /// `billing::stripe_client`'s own "Layer 3" test module). Returns a
    /// single-item subscription with the given status/price/quantity/period,
    /// the exact shape [`crate::billing::stripe_client::StripeClient::
    /// retrieve_subscription`] parses.
    async fn spawn_subscription_mock(
        status: &'static str,
        price_id: String,
        quantity: i64,
        period_start: i64,
        period_end: i64,
    ) -> crate::billing::stripe_client::StripeClient {
        let body = json!({
            "id": "sub_mock_1",
            "object": "subscription",
            "status": status,
            "items": {
                "object": "list",
                "data": [{
                    "id": "si_mock_1",
                    "quantity": quantity,
                    "current_period_start": period_start,
                    "current_period_end": period_end,
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
        crate::billing::stripe_client::StripeClient::new(reqwest::Client::new(), "sk_test_mock")
            .with_base_url(format!("http://{addr}"))
    }

    /// The extra-usage balance in CREDITS (balance / the 22,000 micro-USD
    /// rate), `None` with no balance row. Pack purchases credit the extra
    /// ledger since settings launch PR 3.4, not `credit_balances`.
    async fn read_credit_balance(pool: &PgPool, account_id: Uuid) -> Option<Decimal> {
        let micro: Option<i64> =
            sqlx::query_scalar("SELECT balance_micro_usd FROM extra_usage_balances WHERE account_id = $1")
                .bind(account_id)
                .fetch_optional(pool)
                .await
                .expect("read extra_usage_balances");
        micro.map(|m| Decimal::from(m) / Decimal::from(22_000))
    }

    /// The plan pool must NOT be fed by a pack any more.
    async fn read_plan_pool(pool: &PgPool, account_id: Uuid) -> Option<Decimal> {
        sqlx::query_scalar("SELECT balance FROM credit_balances WHERE account_id = $1")
            .bind(account_id)
            .fetch_optional(pool)
            .await
            .expect("read credit_balances")
    }

    async fn set_catalog_price(pool: &PgPool, table: &str, code: &str, price_id: &str) {
        let sql = format!("UPDATE {table} SET stripe_price_id = $2 WHERE code = $1");
        sqlx::query(&sql).bind(code).bind(price_id).execute(pool).await.expect("set catalog price");
    }

    /// A local Stripe that answers every request with `body` (the re-fetched
    /// Checkout session).
    async fn spawn_session_mock(body: serde_json::Value) -> crate::billing::stripe_client::StripeClient {
        let app = axum::Router::new().fallback(axum::routing::any(move || {
            let body = body.clone();
            async move { (axum::http::StatusCode::OK, [("content-type", "application/json")], body.to_string()) }
        }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind mock");
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.ok();
        });
        crate::billing::stripe_client::StripeClient::new(reqwest::Client::new(), "sk_test_mock")
            .with_base_url(format!("http://{addr}"))
    }

    fn expanded_session(
        session_id: &str,
        price_id: &str,
        amount_total: i64,
        metadata: serde_json::Value,
    ) -> serde_json::Value {
        json!({
            "id": session_id, "object": "checkout.session", "mode": "payment",
            "payment_status": "paid", "payment_intent": format!("pi_{session_id}"),
            "amount_total": amount_total, "amount_subtotal": amount_total,
            "currency": "usd", "metadata": metadata,
            "line_items": {"object": "list", "data": [{"quantity": 1, "price": {"id": price_id}}]},
        })
    }

    async fn run_completed(
        pool: &PgPool,
        account_id: Uuid,
        event_id: &str,
        session: serde_json::Value,
    ) -> (StatusCode, serde_json::Value) {
        let stripe = spawn_session_mock(session.clone()).await;
        let state = Arc::new({
            let mut s = crate::test_app_state_owned_with_db(pool.clone(), None);
            s.stripe = Some(Arc::new(stripe));
            s
        });
        let resp = handle_checkout_completed(
            &state, pool, account_id, event_id, "checkout.session.completed", &session,
        )
        .await;
        let status = resp.status();
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX).await.expect("body");
        (status, serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null))
    }

    /// The dispatcher routes on the RE-FETCHED session's `metadata[purpose]`:
    /// a top-up credits what Stripe says was paid, a session without it is a
    /// legacy credit pack, and a top-up whose metadata names another account
    /// credits nothing.
    #[tokio::test]
    async fn checkout_completed_routes_topups_and_legacy_packs_to_the_extra_ledger() {
        let Some(pool) =
            pg_test_pool("checkout_completed_routes_topups_and_legacy_packs_to_the_extra_ledger").await
        else {
            return;
        };
        let _catalog_guard = CATALOG_LOCK.lock().await;
        let (account_id, users, _customer) = seed_linked_account(&pool, 1).await;
        set_catalog_price(&pool, "credit_packs", "pack_500", "price_pack_500_route").await;
        let topup_meta = json!({"purpose": "extra_usage_topup", "account_id": account_id.to_string()});

        // Top-up: $25.00 paid, even though the line item matches no catalog pack.
        let session_id = format!("cs_{}", Uuid::new_v4().simple());
        let session = expanded_session(&session_id, "price_adhoc_topup", 2500, topup_meta.clone());
        let event = format!("evt_{}", Uuid::new_v4().simple());
        let (status, _) = run_completed(&pool, account_id, &event, session.clone()).await;
        assert_eq!(status, StatusCode::OK);
        let micro = |pool: PgPool| async move {
            sqlx::query_scalar::<_, i64>("SELECT balance_micro_usd FROM extra_usage_balances WHERE account_id=$1")
                .bind(account_id)
                .fetch_optional(&pool)
                .await
                .unwrap()
                .unwrap_or(0)
        };
        assert_eq!(micro(pool.clone()).await, 25_000_000);
        // Redelivered, and re-minted under a new event id: still one credit.
        let (_, body) = run_completed(&pool, account_id, &event, session.clone()).await;
        assert_eq!(body["replay"], true);
        let (_, body) =
            run_completed(&pool, account_id, &format!("evt_{}", Uuid::new_v4().simple()), session).await;
        assert_eq!(body["duplicate_session"], true);
        assert_eq!(micro(pool.clone()).await, 25_000_000);

        // Legacy pack session: no metadata, catalog price, $11.
        let session = expanded_session(
            &format!("cs_{}", Uuid::new_v4().simple()),
            "price_pack_500_route",
            1100,
            json!({}),
        );
        let (status, _) =
            run_completed(&pool, account_id, &format!("evt_{}", Uuid::new_v4().simple()), session).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(micro(pool.clone()).await, 36_000_000);

        // A top-up that names a different account in its metadata credits nothing.
        let forged = expanded_session(
            &format!("cs_{}", Uuid::new_v4().simple()),
            "price_adhoc_topup",
            50_000,
            json!({"purpose": "extra_usage_topup", "account_id": Uuid::new_v4().to_string()}),
        );
        let (status, body) =
            run_completed(&pool, account_id, &format!("evt_{}", Uuid::new_v4().simple()), forged).await;
        assert_eq!((status, &body["unprocessable"]), (StatusCode::OK, &json!(true)));
        assert_eq!(micro(pool.clone()).await, 36_000_000);

        cleanup(&pool, account_id, &users).await;
    }

    /// Staging run: a $5.00 top-up that Stripe totalled at $5.44 with tax
    /// credited $5.44. Through the real dispatcher, the credit is the
    /// subtotal.
    #[tokio::test]
    async fn checkout_completed_topup_credits_the_pre_tax_subtotal() {
        let Some(pool) = pg_test_pool("checkout_completed_topup_credits_the_pre_tax_subtotal").await else {
            return;
        };
        let (account_id, users, _customer) = seed_linked_account(&pool, 1).await;
        let mut session = expanded_session(
            &format!("cs_{}", Uuid::new_v4().simple()),
            "price_adhoc_topup",
            544,
            json!({"purpose": "extra_usage_topup", "account_id": account_id.to_string()}),
        );
        session["amount_subtotal"] = json!(500);
        let (status, _) =
            run_completed(&pool, account_id, &format!("evt_{}", Uuid::new_v4().simple()), session).await;
        let credited: Option<i64> =
            sqlx::query_scalar("SELECT balance_micro_usd FROM extra_usage_balances WHERE account_id=$1")
                .bind(account_id)
                .fetch_optional(&pool)
                .await
                .unwrap();
        cleanup(&pool, account_id, &users).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(credited, Some(5_000_000), "$5.00 pre-tax, not the $5.44 total");
    }

    /// `checkout.session.expired` clears the recorded open Checkout only when
    /// it is that session, and leaves the account able to check out again.
    #[tokio::test]
    async fn checkout_expired_clears_only_the_matching_pending_session() {
        let Some(pool) = pg_test_pool("checkout_expired_clears_only_the_matching_pending_session").await else {
            return;
        };
        let (account_id, users, _customer) = seed_linked_account(&pool, 1).await;
        execute_financial_fixture(
            &pool,
            account_id,
            sqlx::query("UPDATE stripe_subscriptions SET pending_checkout_session_id = 'cs_current' WHERE account_id = $1")
                .bind(account_id),
        )
        .await
        .expect("seed pending session");
        let read = |pool: PgPool| async move {
            sqlx::query_scalar::<_, Option<String>>(
                "SELECT pending_checkout_session_id FROM stripe_subscriptions WHERE account_id = $1",
            )
            .bind(account_id)
            .fetch_one(&pool)
            .await
            .unwrap()
        };

        // An older, superseded session expiring must not clear the newer pointer.
        let resp = handle_checkout_expired(&pool, account_id, "evt_old", &json!({"id": "cs_old"})).await;
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(read(pool.clone()).await.as_deref(), Some("cs_current"));

        let resp = handle_checkout_expired(&pool, account_id, "evt_cur", &json!({"id": "cs_current"})).await;
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(read(pool.clone()).await, None);
        // Redelivery is harmless.
        let resp = handle_checkout_expired(&pool, account_id, "evt_cur", &json!({"id": "cs_current"})).await;
        assert_eq!(resp.status(), StatusCode::OK);

        cleanup(&pool, account_id, &users).await;
    }

    /// **Multi-line-item, genuinely** — the regression test PR C's fixture
    /// README asked for: TWO different packs in one session, both credited,
    /// summed correctly. Also proves the zero-axis `credit_ledger` row and
    /// the `credit_balances` upsert.
    #[tokio::test]
    async fn checkout_completed_payment_credits_every_line_item_not_just_the_first() {
        let Some(pool) =
            pg_test_pool("checkout_completed_payment_credits_every_line_item_not_just_the_first").await
        else {
            return;
        };
        let _catalog_guard = CATALOG_LOCK.lock().await;
        let (account_id, users, _customer) = seed_linked_account(&pool, 1).await;
        set_catalog_price(&pool, "credit_packs", "pack_500", "price_pack_500_mock").await;
        set_catalog_price(&pool, "credit_packs", "pack_2000", "price_pack_2000_mock").await;

        let line_items = vec![
            crate::billing::stripe_client::SessionLineItem {
                price_id: "price_pack_500_mock".to_string(),
                quantity: 1,
            },
            crate::billing::stripe_client::SessionLineItem {
                price_id: "price_pack_2000_mock".to_string(),
                quantity: 2,
            },
        ];
        let event_id = format!("evt_{}", Uuid::new_v4().simple());
        let resp = apply_checkout_completed_payment(
            &pool,
            account_id,
            &event_id,
            "checkout.session.completed",
            "cs_mock_1",
            &line_items,
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK, "expected 200");

        // 500 (pack_500 x1) + 2000*2 (pack_2000 x2) = 4500.
        assert_eq!(
            read_credit_balance(&pool, account_id).await,
            Some(Decimal::from(4500)),
            "both line items must be priced and summed, not just the first"
        );
        let ledger_credits: Vec<Decimal> = sqlx::query_scalar(
            "SELECT credits FROM extra_usage_ledger WHERE account_id = $1 AND entry_kind = 'pack_purchase'",
        )
        .bind(account_id)
        .fetch_all(&pool)
        .await
        .expect("read ledger");
        assert_eq!(ledger_credits, vec![Decimal::from(4500)], "one positive-credit row");
        assert_eq!(read_plan_pool(&pool, account_id).await, None, "a pack no longer feeds the plan pool");

        // Replay: the same event_id must not double-credit.
        let replay = apply_checkout_completed_payment(
            &pool,
            account_id,
            &event_id,
            "checkout.session.completed",
            "cs_mock_1",
            &line_items,
        )
        .await;
        let replay_body: serde_json::Value = {
            let bytes = axum::body::to_bytes(replay.into_body(), usize::MAX).await.expect("read body");
            serde_json::from_slice(&bytes).expect("json")
        };
        assert_eq!(replay_body["replay"], true, "the redelivered event must be reported as a replay");
        assert_eq!(
            read_credit_balance(&pool, account_id).await,
            Some(Decimal::from(4500)),
            "a replay must never double-credit"
        );

        cleanup(&pool, account_id, &users).await;
    }

    /// **Finding 9 (billing self-serve PR D review round).** A price that
    /// matches no ACTIVE `credit_packs` row refuses the WHOLE payment —
    /// never partially crediting the recognized line, silently
    /// under-crediting a real charge would be worse than a visible failure —
    /// but now via the accept-and-record posture (200, claim committed,
    /// durable outcome), not the 400 an earlier version used, which left
    /// Stripe retrying an unrecognizable price for days with no audit trail.
    #[tokio::test]
    async fn checkout_completed_payment_refuses_the_whole_purchase_on_an_unrecognized_price() {
        let Some(pool) = pg_test_pool(
            "checkout_completed_payment_refuses_the_whole_purchase_on_an_unrecognized_price",
        )
        .await
        else {
            return;
        };
        let _catalog_guard = CATALOG_LOCK.lock().await;
        let (account_id, users, _customer) = seed_linked_account(&pool, 1).await;
        set_catalog_price(&pool, "credit_packs", "pack_500", "price_pack_500_mock_2").await;

        let line_items = vec![
            crate::billing::stripe_client::SessionLineItem {
                price_id: "price_pack_500_mock_2".to_string(),
                quantity: 1,
            },
            crate::billing::stripe_client::SessionLineItem {
                price_id: "price_totally_unknown".to_string(),
                quantity: 1,
            },
        ];
        let event_id = format!("evt_{}", Uuid::new_v4().simple());
        let resp = apply_checkout_completed_payment(
            &pool,
            account_id,
            &event_id,
            "checkout.session.completed",
            "cs_mock_1",
            &line_items,
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK, "accept-and-record: claimed and recorded, not rejected");
        let body: serde_json::Value = {
            let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX).await.expect("read body");
            serde_json::from_slice(&bytes).expect("json")
        };
        assert_eq!(body["unprocessable"], true);
        assert_eq!(
            read_credit_balance(&pool, account_id).await,
            None,
            "no partial credit — the recognized pack_500 line must NOT have been credited either"
        );
        let recorded: Option<String> =
            sqlx::query_scalar("SELECT outcome FROM stripe_webhook_events WHERE event_id = $1")
                .bind(&event_id)
                .fetch_one(&pool)
                .await
                .expect("the event must still be claimed and recorded");
        assert!(
            recorded.as_deref().is_some_and(|o| o.starts_with("unprocessable")),
            "the claim must stay committed with a clear reason: {recorded:?}"
        );

        // Redelivery under a NEW event_id must still be a no-op (defence in
        // depth beyond the claim: nothing was ever credited to double).
        let event_id_2 = format!("evt_{}", Uuid::new_v4().simple());
        let resp2 = apply_checkout_completed_payment(
            &pool,
            account_id,
            &event_id_2,
            "checkout.session.completed",
            "cs_mock_1",
            &line_items,
        )
        .await;
        assert_eq!(resp2.status(), StatusCode::OK);
        assert_eq!(
            read_credit_balance(&pool, account_id).await,
            None,
            "still no credit after a second unprocessable delivery"
        );

        cleanup(&pool, account_id, &users).await;
    }

    /// **The full subscription-mode happy path**: issues the `pro` grant via
    /// `issuance::issue_grant` and mirrors the new subscription (id, status,
    /// period) onto `stripe_subscriptions` — proving this handler actually
    /// composes the outbound Stripe call with the grant/mirror writes
    /// correctly, not just that each piece works in isolation.
    #[tokio::test]
    async fn checkout_completed_subscription_issues_the_pro_grant_and_mirrors_the_subscription() {
        let Some(pool) = pg_test_pool(
            "checkout_completed_subscription_issues_the_pro_grant_and_mirrors_the_subscription",
        )
        .await
        else {
            return;
        };
        let _catalog_guard = CATALOG_LOCK.lock().await;
        let (account_id, users, _customer) = seed_linked_account(&pool, 1).await;
        set_catalog_price(&pool, "plans", "pro", "price_pro_mock").await;
        let stripe = spawn_subscription_mock("active", "price_pro_mock".to_string(), 1, 1_700_000_000, 1_702_678_400).await;
        let state = Arc::new({
            let mut s = crate::test_app_state_owned_with_db(pool.clone(), None);
            s.stripe = Some(Arc::new(stripe));
            s
        });

        let line_item = crate::billing::stripe_client::SessionLineItem {
            price_id: "price_pro_mock".to_string(),
            quantity: 1,
        };
        let event_id = format!("evt_{}", Uuid::new_v4().simple());
        let resp = apply_checkout_completed_subscription(
            &state,
            &pool,
            account_id,
            &event_id,
            "cs_mock_1",
            "sub_mock_1",
            &line_item,
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK, "expected 200");

        let plan_id: Uuid = sqlx::query_scalar("SELECT id FROM plans WHERE code = 'pro'")
            .fetch_one(&pool)
            .await
            .expect("pro plan");
        let granted: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM grants WHERE account_id = $1 AND plan_id = $2 AND revoked_at IS NULL)",
        )
        .bind(account_id)
        .bind(plan_id)
        .fetch_one(&pool)
        .await
        .expect("check grant");
        assert!(granted, "the pro plan must have been granted");

        let (stored_sub_id, status): (Option<String>, String) = sqlx::query_as(
            "SELECT stripe_subscription_id, status FROM stripe_subscriptions WHERE account_id = $1",
        )
        .bind(account_id)
        .fetch_one(&pool)
        .await
        .expect("read stripe_subscriptions");
        assert_eq!(stored_sub_id.as_deref(), Some("sub_mock_1"));
        assert_eq!(status, "active");

        cleanup(&pool, account_id, &users).await;
    }

    /// **Ordering: `invoice.paid` BEFORE `checkout.session.completed`** (the
    /// real first purchase of a Free account, 2026-10-27). The invoice rolls a
    /// period at the allowance of the grant held at that moment; the checkout
    /// grant must then leave exactly ONE current period, at the purchased
    /// plan's allowance, with no overlap and nothing consumed lost.
    #[tokio::test]
    async fn invoice_paid_before_checkout_completed_leaves_one_current_period_at_the_purchased_plan() {
        let Some(pool) = pg_test_pool(
            "invoice_paid_before_checkout_completed_leaves_one_current_period_at_the_purchased_plan",
        )
        .await
        else {
            return;
        };
        let _catalog_guard = CATALOG_LOCK.lock().await;
        let (account_id, users, _customer) = seed_linked_account(&pool, 1).await;
        set_catalog_price(&pool, "plans", "pro", "price_pro_mock_order").await;
        // Start from a Free account: the grant held when the invoice lands.
        execute_financial_fixture(
            &pool,
            account_id,
            sqlx::query(
                "UPDATE grants SET plan_id = (SELECT id FROM plans WHERE code = 'free'), seats = NULL, \
                        plan_is_seat_priced = false WHERE account_id = $1",
            )
            .bind(account_id),
        )
        .await
        .expect("make it a Free account");
        execute_financial_fixture(
            &pool,
            account_id,
            sqlx::query("UPDATE stripe_subscriptions SET stripe_subscription_id = 'sub_mock_1' WHERE account_id = $1")
                .bind(account_id),
        )
        .await
        .expect("link the new subscription");

        let now = chrono::Utc::now().timestamp();
        let (start, end) = (
            chrono::DateTime::from_timestamp(now - 5, 0).unwrap(),
            chrono::DateTime::from_timestamp(now + 30 * 86_400, 0).unwrap(),
        );
        // 1. invoice.paid first: rolls at the Free allowance.
        let outcome = apply_invoice_paid(
            &pool,
            account_id,
            &format!("evt_{}", Uuid::new_v4().simple()),
            "invoice.paid",
            "sub_mock_1",
            start,
            end,
        )
        .await
        .expect("apply")
        .expect("claimed");
        assert!(matches!(outcome, InvoicePaidOutcome::Applied(_)), "{outcome:?}");

        // 2. checkout.session.completed second: grants Pro.
        let stripe =
            spawn_subscription_mock("active", "price_pro_mock_order".to_string(), 1, now - 5, now + 30 * 86_400).await;
        let state = Arc::new({
            let mut s = crate::test_app_state_owned_with_db(pool.clone(), None);
            s.stripe = Some(Arc::new(stripe));
            s
        });
        let line_item =
            crate::billing::stripe_client::SessionLineItem { price_id: "price_pro_mock_order".to_string(), quantity: 1 };
        let resp = apply_checkout_completed_subscription(
            &state,
            &pool,
            account_id,
            &format!("evt_{}", Uuid::new_v4().simple()),
            "cs_mock_order",
            "sub_mock_1",
            &line_item,
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK);

        let pro_credits: sqlx::types::Decimal =
            sqlx::query_scalar("SELECT credits FROM plans WHERE code = 'pro'").fetch_one(&pool).await.expect("pro");
        let current: Vec<sqlx::types::Decimal> = sqlx::query_scalar(
            "SELECT granted_credits FROM member_allowance_periods \
              WHERE account_id = $1 AND period_start <= now() AND period_end > now()",
        )
        .bind(account_id)
        .fetch_all(&pool)
        .await
        .expect("current periods");
        assert_eq!(current, vec![pro_credits], "exactly one current period, at the Pro allowance");
        // No overlap anywhere in the history (0163 also enforces this).
        let overlaps: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM member_allowance_periods a JOIN member_allowance_periods b \
               ON a.account_id = b.account_id AND a.user_id = b.user_id AND a.id < b.id \
              AND a.period_start < b.period_end AND b.period_start < a.period_end \
              WHERE a.account_id = $1",
        )
        .bind(account_id)
        .fetch_one(&pool)
        .await
        .expect("overlaps");
        assert_eq!(overlaps, 0);

        cleanup(&pool, account_id, &users).await;
    }

    /// Idempotency: a redelivered subscription-mode `checkout.session
    /// .completed` must be a replay, never a second grant.
    #[tokio::test]
    async fn a_redelivered_checkout_completed_subscription_event_does_not_double_grant() {
        let Some(pool) = pg_test_pool(
            "a_redelivered_checkout_completed_subscription_event_does_not_double_grant",
        )
        .await
        else {
            return;
        };
        let _catalog_guard = CATALOG_LOCK.lock().await;
        let (account_id, users, _customer) = seed_linked_account(&pool, 1).await;
        set_catalog_price(&pool, "plans", "pro", "price_pro_mock_2").await;
        let stripe =
            spawn_subscription_mock("active", "price_pro_mock_2".to_string(), 1, 1_700_000_000, 1_702_678_400).await;
        let state = Arc::new({
            let mut s = crate::test_app_state_owned_with_db(pool.clone(), None);
            s.stripe = Some(Arc::new(stripe));
            s
        });
        let line_item = crate::billing::stripe_client::SessionLineItem {
            price_id: "price_pro_mock_2".to_string(),
            quantity: 1,
        };
        let event_id = format!("evt_{}", Uuid::new_v4().simple());

        let first = apply_checkout_completed_subscription(
            &state, &pool, account_id, &event_id, "cs_mock_1", "sub_mock_1",
            &line_item,
        )
        .await;
        assert_eq!(first.status(), StatusCode::OK);
        let second = apply_checkout_completed_subscription(
            &state, &pool, account_id, &event_id, "cs_mock_1", "sub_mock_1",
            &line_item,
        )
        .await;
        assert_eq!(second.status(), StatusCode::OK);
        let second_body: serde_json::Value = {
            let bytes = axum::body::to_bytes(second.into_body(), usize::MAX).await.expect("read body");
            serde_json::from_slice(&bytes).expect("json")
        };
        assert_eq!(second_body["replay"], true);

        let plan_id: Uuid = sqlx::query_scalar("SELECT id FROM plans WHERE code = 'pro'")
            .fetch_one(&pool)
            .await
            .expect("pro plan");
        let grant_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM grants WHERE account_id = $1 AND plan_id = $2",
        )
        .bind(account_id)
        .bind(plan_id)
        .fetch_one(&pool)
        .await
        .expect("count grants");
        assert_eq!(grant_count, 1, "a replay must never issue a second grant");

        cleanup(&pool, account_id, &users).await;
    }

    /// **Regression test for finding 1 (CRITICAL, billing self-serve PR D
    /// review round): a post-claim failure must not lose the claim.** An
    /// earlier version claimed the event in its OWN, separately-committed
    /// transaction, then did the grant issuance and mirror update outside
    /// any transaction — so a failure AFTER that first commit (here:
    /// `state.stripe == None`, simulated by never providing a client) still
    /// left the claim permanently committed. Redelivery under the SAME
    /// event_id then hit `claimed == 0`, answered "replay", and the purchase
    /// was never granted. This version's single-transaction restructuring
    /// closes that: the claim now lives in the SAME transaction as the
    /// grant/mirror work, so a failure before that transaction ever opens
    /// (this specific failure mode) never claims anything at all, and
    /// redelivery reprocesses cleanly once the underlying problem — here,
    /// `STRIPE_SECRET_KEY` missing — is fixed.
    #[tokio::test]
    async fn a_failure_after_the_claim_lets_redelivery_actually_reprocess() {
        let Some(pool) =
            pg_test_pool("a_failure_after_the_claim_lets_redelivery_actually_reprocess").await
        else {
            return;
        };
        let _catalog_guard = CATALOG_LOCK.lock().await;
        let (account_id, users, _customer) = seed_linked_account(&pool, 1).await;
        set_catalog_price(&pool, "plans", "pro", "price_pro_mock_3").await;
        let line_item = crate::billing::stripe_client::SessionLineItem {
            price_id: "price_pro_mock_3".to_string(),
            quantity: 1,
        };
        let event_id = format!("evt_{}", Uuid::new_v4().simple());

        // First attempt: no stripe client configured at all — a TRANSIENT
        // internal misconfiguration, must 500 WITHOUT claiming.
        let unconfigured_state = Arc::new(crate::test_app_state_owned_with_db(pool.clone(), None));
        let first = apply_checkout_completed_subscription(
            &unconfigured_state, &pool, account_id, &event_id, "cs_mock_1", "sub_mock_1", &line_item,
        )
        .await;
        assert_eq!(
            first.status(),
            StatusCode::INTERNAL_SERVER_ERROR,
            "no stripe client configured must 500, asking Stripe to redeliver"
        );
        let claimed_after_failure: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM stripe_webhook_events WHERE event_id = $1)",
        )
        .bind(&event_id)
        .fetch_one(&pool)
        .await
        .expect("check claim");
        assert!(
            !claimed_after_failure,
            "finding 1: a failure must NEVER leave the claim committed with the work undone — \
             the whole point of the single-transaction restructuring"
        );

        // Redelivery of the SAME event_id, now with a working stripe client
        // configured — must actually process, not report a false replay.
        let stripe = spawn_subscription_mock("active", "price_pro_mock_3".to_string(), 1, 1_700_000_000, 1_702_678_400).await;
        let working_state = Arc::new({
            let mut s = crate::test_app_state_owned_with_db(pool.clone(), None);
            s.stripe = Some(Arc::new(stripe));
            s
        });
        let second = apply_checkout_completed_subscription(
            &working_state, &pool, account_id, &event_id, "cs_mock_1", "sub_mock_1", &line_item,
        )
        .await;
        assert_eq!(second.status(), StatusCode::OK, "redelivery must actually process, not silently no-op");
        let second_body: serde_json::Value = {
            let bytes = axum::body::to_bytes(second.into_body(), usize::MAX).await.expect("read body");
            serde_json::from_slice(&bytes).expect("json")
        };
        assert_ne!(
            second_body["replay"], true,
            "the redelivery must NOT be reported as a replay — finding 1's whole bug was that it \
             falsely WAS one"
        );

        let plan_id: Uuid = sqlx::query_scalar("SELECT id FROM plans WHERE code = 'pro'")
            .fetch_one(&pool)
            .await
            .expect("pro plan");
        let granted: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM grants WHERE account_id = $1 AND plan_id = $2 AND revoked_at IS NULL)",
        )
        .bind(account_id)
        .bind(plan_id)
        .fetch_one(&pool)
        .await
        .expect("check grant");
        assert!(granted, "the redelivery must actually grant the plan the first attempt lost");

        cleanup(&pool, account_id, &users).await;
    }

    // ── Finding 2: RLS-scoped grant reads/writes under the seat-tier arm ────

    /// **Originally the regression test for finding 2 (billing self-serve PR
    /// D review round) — re-purposed under PR E, then again under PR F.**
    /// Finding 2's original fix scoped a manual re-read-then-`INSERT INTO
    /// grant_seat_tiers` here into the SAME account-scoped transaction the
    /// claim uses. PR E made that write redundant (a `roll_period`-driven
    /// roster reconciliation took over), so PR E deleted it; PR F's own
    /// `grant_seat_tiers` model retired THAT roster reconciliation in turn
    /// (see `billing::renewal`'s "`grant_seat_tiers` is a purchase-driven
    /// ledger" doc section) and reinstated a manual ledger seed — but a
    /// PURCHASE-driven one this time, not a roster-derived one: one
    /// `grant_seat_tiers` row for the single seat THIS checkout actually
    /// bought, written directly in the `Granted` arm just above (see that
    /// arm's own comment) rather than derived from whatever the roster
    /// happens to contain. This test proves that seed lands correctly, in
    /// the SAME transaction as the claim, for a fresh `team` grant. The
    /// fixture (`seed_team_org_account_no_grant`) is a genuine ORG account
    /// with a seat-tiered roster and no pre-existing `team` grant —
    /// deliberately not `seed_linked_account` (whose account carries no
    /// `clerk_org_id`, so its member's `seat_tier_id` is NULL, which a
    /// `team` grant's tier-join credits computation turns into a
    /// `granted_credits` NOT NULL violation, not a useful error) and not
    /// `seed_team_linked_account` either (which pre-seeds an ACTIVE `team`
    /// grant, so `issue_grant_in_tx` would report `AlreadyGranted` and never
    /// exercise the `Granted` arm's ledger seed at all). This does not
    /// exercise the `coralswarm_app` role directly (this suite's pool
    /// connects as the owner/`BYPASSRLS` role, like every other suite here —
    /// see `consumer_rls_pg.rs` for the ONE integration suite in this tree
    /// that logs in AS `coralswarm_app`, gated on `ORGT_TEST_BASE_URL`, a
    /// different DSN convention than this file's
    /// `RELINFER_TEST_DATABASE_URL`), so it cannot itself prove the RLS cast
    /// no longer errors — but it does prove the SCOPED code path (`&mut
    /// *tx`, not the bare `control` pool) is what actually runs and still
    /// produces the correct end state.
    #[tokio::test]
    async fn checkout_completed_subscription_for_a_seat_tier_writes_the_grant_and_its_seat_tier_row() {
        let Some(pool) = pg_test_pool(
            "checkout_completed_subscription_for_a_seat_tier_writes_the_grant_and_its_seat_tier_row",
        )
        .await
        else {
            return;
        };
        let _catalog_guard = CATALOG_LOCK.lock().await;
        let (account_id, users, _customer) = seed_team_org_account_no_grant(&pool, 1).await;
        set_catalog_price(&pool, "seat_tiers", "standard", "price_standard_mock_2").await;

        let stripe =
            spawn_subscription_mock("active", "price_standard_mock_2".to_string(), 1, 1_700_000_000, 1_702_678_400)
                .await;
        let state = Arc::new({
            let mut s = crate::test_app_state_owned_with_db(pool.clone(), None);
            s.stripe = Some(Arc::new(stripe));
            s
        });
        let line_item = crate::billing::stripe_client::SessionLineItem {
            price_id: "price_standard_mock_2".to_string(),
            quantity: 1,
        };
        let event_id = format!("evt_{}", Uuid::new_v4().simple());
        let resp = apply_checkout_completed_subscription(
            &state, &pool, account_id, &event_id, "cs_mock_1", "sub_mock_1", &line_item,
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK);

        let team_plan_id: Uuid = sqlx::query_scalar("SELECT id FROM plans WHERE code = 'team'")
            .fetch_one(&pool)
            .await
            .expect("team plan");
        let grant_id: Uuid = sqlx::query_scalar(
            "SELECT id FROM grants WHERE account_id = $1 AND plan_id = $2 AND revoked_at IS NULL",
        )
        .bind(account_id)
        .bind(team_plan_id)
        .fetch_one(&pool)
        .await
        .expect("the team grant must have been written");

        let standard_tier_id: Uuid = sqlx::query_scalar("SELECT id FROM seat_tiers WHERE code = 'standard'")
            .fetch_one(&pool)
            .await
            .expect("standard tier");
        let seats: i32 = sqlx::query_scalar(
            "SELECT seats FROM grant_seat_tiers WHERE grant_id = $1 AND seat_tier_id = $2",
        )
        .bind(grant_id)
        .bind(standard_tier_id)
        .fetch_one(&pool)
        .await
        .expect("the grant_seat_tiers row must have been written");
        assert_eq!(seats, 1, "one standard seat purchased");

        let (member_tier, pending, purchased_at): (Uuid, Option<Uuid>, Option<chrono::DateTime<chrono::Utc>>) =
            sqlx::query_as(
                "SELECT seat_tier_id, pending_seat_tier_id, seat_purchased_at FROM account_members \
                  WHERE account_id = $1 AND user_id = $2",
            )
            .bind(account_id)
            .bind(users[0])
            .fetch_one(&pool)
            .await
            .expect("read the purchaser's membership row");
        assert_eq!(member_tier, standard_tier_id, "the purchaser's seat_tier_id must align with what was bought");
        assert_eq!(pending, None);
        assert!(purchased_at.is_some(), "the purchaser must be stamped seat_purchased_at");

        cleanup(&pool, account_id, &users).await;
    }

    /// **Finding 2 (billing self-serve PR F review) — the 0-rows-affected
    /// arm.** When the purchaser's `account_members` row is missing at the
    /// moment `checkout.session.completed` lands (an unlikely but real data
    /// state — `require_purchase_authority` verified membership AT checkout
    /// time, not at the instant this webhook later processes), the guarded
    /// stamp UPDATE affects zero rows and the seat-ledger seed must be
    /// SKIPPED entirely — no `grant_seat_tiers` row, no `grants.seats` sync —
    /// rather than record a purchased seat no member row backs. The webhook
    /// itself still succeeds (200), never a 500: this is a genuine data
    /// state, not a transient failure Stripe redelivery could fix.
    #[tokio::test]
    async fn checkout_completed_skips_the_seat_ledger_seed_when_the_purchaser_row_is_missing() {
        let Some(pool) = pg_test_pool(
            "checkout_completed_skips_the_seat_ledger_seed_when_the_purchaser_row_is_missing",
        )
        .await
        else {
            return;
        };
        let _catalog_guard = CATALOG_LOCK.lock().await;
        let (account_id, users, _customer) = seed_team_org_account_no_grant(&pool, 1).await;
        set_catalog_price(&pool, "seat_tiers", "standard", "price_standard_mock_3").await;
        sqlx::query("DELETE FROM account_members WHERE account_id = $1 AND user_id = $2")
            .bind(account_id)
            .bind(users[0])
            .execute(&pool)
            .await
            .expect("remove the purchaser's membership row to simulate the 0-row stamp");

        let stripe =
            spawn_subscription_mock("active", "price_standard_mock_3".to_string(), 1, 1_700_000_000, 1_702_678_400)
                .await;
        let state = Arc::new({
            let mut s = crate::test_app_state_owned_with_db(pool.clone(), None);
            s.stripe = Some(Arc::new(stripe));
            s
        });
        let line_item = crate::billing::stripe_client::SessionLineItem {
            price_id: "price_standard_mock_3".to_string(),
            quantity: 1,
        };
        let event_id = format!("evt_{}", Uuid::new_v4().simple());
        let resp = apply_checkout_completed_subscription(
            &state, &pool, account_id, &event_id, "cs_mock_2", "sub_mock_2", &line_item,
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK, "a missing purchaser row must not fail the webhook");

        let team_plan_id: Uuid = sqlx::query_scalar("SELECT id FROM plans WHERE code = 'team'")
            .fetch_one(&pool)
            .await
            .expect("team plan");
        let grant_id: Uuid = sqlx::query_scalar(
            "SELECT id FROM grants WHERE account_id = $1 AND plan_id = $2 AND revoked_at IS NULL",
        )
        .bind(account_id)
        .bind(team_plan_id)
        .fetch_one(&pool)
        .await
        .expect("the team grant must still have been written — only the SEAT LEDGER seed is skipped");

        let ledger_row_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM grant_seat_tiers WHERE grant_id = $1")
                .bind(grant_id)
                .fetch_one(&pool)
                .await
                .expect("count grant_seat_tiers rows");
        assert_eq!(
            ledger_row_count, 0,
            "the ledger seed must be SKIPPED entirely when the purchaser row is missing — failing \
             toward under-entitlement rather than a ledger seat no member row backs"
        );

        cleanup(&pool, account_id, &users).await;
    }

    // ── Findings 8 + 9: permanently-unprocessable checkout sessions ─────────

    /// A local axum server standing in for `GET /v1/checkout/sessions/{id}`
    /// (the expanded-session re-fetch every `checkout.session.completed`
    /// delivery makes) — returns `line_items.data` built from the given
    /// `(price_id, quantity)` pairs, the exact shape
    /// [`crate::billing::stripe_client::parse_checkout_session_line_items`]
    /// parses.
    async fn spawn_checkout_session_mock(
        line_items: Vec<(&str, i64)>,
    ) -> crate::billing::stripe_client::StripeClient {
        let data: Vec<serde_json::Value> = line_items
            .into_iter()
            .map(|(price_id, quantity)| json!({"price": {"id": price_id}, "quantity": quantity}))
            .collect();
        let body = json!({
            "id": "cs_mock_1",
            "line_items": {"object": "list", "data": data},
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
        crate::billing::stripe_client::StripeClient::new(reqwest::Client::new(), "sk_test_mock")
            .with_base_url(format!("http://{addr}"))
    }

    async fn unprocessable_outcome(pool: &PgPool, event_id: &str) -> Option<String> {
        sqlx::query_scalar("SELECT outcome FROM stripe_webhook_events WHERE event_id = $1")
            .bind(event_id)
            .fetch_one(pool)
            .await
            .expect("the event must be claimed and recorded")
    }

    /// **Regression test for finding 8**: a subscription-mode session with
    /// TWO line items (a shape this system was never built to price — our
    /// own checkout route always creates single-item sessions until PR F)
    /// is permanently unprocessable, not a silent first-item-only
    /// under-grant. Accept-and-record (finding 9's posture): 200, claimed,
    /// recorded, no grant issued.
    #[tokio::test]
    async fn a_multi_item_subscription_session_is_permanently_unprocessable() {
        let Some(pool) =
            pg_test_pool("a_multi_item_subscription_session_is_permanently_unprocessable").await
        else {
            return;
        };
        let _catalog_guard = CATALOG_LOCK.lock().await;
        let (account_id, users, _customer) = seed_linked_account(&pool, 1).await;
        set_catalog_price(&pool, "plans", "pro", "price_pro_mock_4").await;
        let stripe = spawn_checkout_session_mock(vec![
            ("price_pro_mock_4", 1),
            ("price_pack_500_mock_3", 1),
        ])
        .await;
        let state = Arc::new({
            let mut s = crate::test_app_state_owned_with_db(pool.clone(), None);
            s.stripe = Some(Arc::new(stripe));
            s
        });
        let event_id = format!("evt_{}", Uuid::new_v4().simple());
        let session = json!({
            "id": "cs_mock_1",
            "mode": "subscription",
            "subscription": "sub_mock_1",
        });

        let resp = handle_checkout_completed(&state, &pool, account_id, &event_id, "checkout.session.completed", &session).await;
        assert_eq!(resp.status(), StatusCode::OK, "accept-and-record, not a 400 that Stripe would retry forever");
        let body: serde_json::Value = {
            let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX).await.expect("read body");
            serde_json::from_slice(&bytes).expect("json")
        };
        assert_eq!(body["unprocessable"], true);

        let plan_id: Uuid = sqlx::query_scalar("SELECT id FROM plans WHERE code = 'pro'")
            .fetch_one(&pool)
            .await
            .expect("pro plan");
        let granted: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM grants WHERE account_id = $1 AND plan_id = $2)",
        )
        .bind(account_id)
        .bind(plan_id)
        .fetch_one(&pool)
        .await
        .expect("check grant");
        assert!(!granted, "no grant may be issued for an unpriceable multi-item session");

        let recorded = unprocessable_outcome(&pool, &event_id).await;
        assert!(
            recorded.as_deref().is_some_and(|o| o.starts_with("unprocessable")),
            "the claim must stay committed with a clear reason: {recorded:?}"
        );

        cleanup(&pool, account_id, &users).await;
    }

    /// **Regression test for finding 9**, `mode = "setup"` (no line items
    /// ever, and no subscription/payment mode this handler was built for).
    #[tokio::test]
    async fn a_setup_mode_session_is_permanently_unprocessable() {
        let Some(pool) = pg_test_pool("a_setup_mode_session_is_permanently_unprocessable").await
        else {
            return;
        };
        let (account_id, users, _customer) = seed_linked_account(&pool, 1).await;
        let stripe = spawn_checkout_session_mock(vec![]).await;
        let state = Arc::new({
            let mut s = crate::test_app_state_owned_with_db(pool.clone(), None);
            s.stripe = Some(Arc::new(stripe));
            s
        });
        let event_id = format!("evt_{}", Uuid::new_v4().simple());
        let session = json!({"id": "cs_mock_1", "mode": "setup"});

        let resp = handle_checkout_completed(&state, &pool, account_id, &event_id, "checkout.session.completed", &session).await;
        assert_eq!(resp.status(), StatusCode::OK, "mode=setup must be accept-and-recorded, not 400");
        let recorded = unprocessable_outcome(&pool, &event_id).await;
        assert!(recorded.as_deref().is_some_and(|o| o.contains("unrecognized checkout mode")));

        cleanup(&pool, account_id, &users).await;
    }

    /// **Regression test for finding 9**, payment-mode session with ZERO
    /// line items — reachable if a pack's catalog row is ever deleted
    /// between checkout and completion, or any other shape this handler was
    /// not built to expect.
    #[tokio::test]
    async fn a_zero_item_payment_session_is_permanently_unprocessable() {
        let Some(pool) = pg_test_pool("a_zero_item_payment_session_is_permanently_unprocessable").await
        else {
            return;
        };
        let (account_id, users, _customer) = seed_linked_account(&pool, 1).await;
        let stripe = spawn_checkout_session_mock(vec![]).await;
        let state = Arc::new({
            let mut s = crate::test_app_state_owned_with_db(pool.clone(), None);
            s.stripe = Some(Arc::new(stripe));
            s
        });
        let event_id = format!("evt_{}", Uuid::new_v4().simple());
        let session = json!({"id": "cs_mock_1", "mode": "payment"});

        let resp = handle_checkout_completed(&state, &pool, account_id, &event_id, "checkout.session.completed", &session).await;
        assert_eq!(resp.status(), StatusCode::OK, "zero items must be accept-and-recorded, not 400");
        let recorded = unprocessable_outcome(&pool, &event_id).await;
        assert!(recorded.as_deref().is_some_and(|o| o.contains("zero line items")));
        assert_eq!(read_credit_balance(&pool, account_id).await, None);

        cleanup(&pool, account_id, &users).await;
    }

    /// **Positive control** for findings 8/9: a well-formed, single-item
    /// subscription session must NOT be treated as unprocessable — without
    /// this, a bug that rejected EVERY session regardless of shape would
    /// also pass the tests above.
    #[tokio::test]
    async fn a_well_formed_single_item_subscription_session_is_not_unprocessable() {
        let Some(pool) = pg_test_pool(
            "a_well_formed_single_item_subscription_session_is_not_unprocessable")
        .await
        else {
            return;
        };
        let _catalog_guard = CATALOG_LOCK.lock().await;
        let (account_id, users, _customer) = seed_linked_account(&pool, 1).await;
        set_catalog_price(&pool, "plans", "pro", "price_pro_mock_5").await;
        let stripe = spawn_checkout_session_mock(vec![("price_pro_mock_5", 1)]).await;
        let state = Arc::new({
            let mut s = crate::test_app_state_owned_with_db(pool.clone(), None);
            s.stripe = Some(Arc::new(stripe));
            s
        });
        // This test only proves dispatch reaches `apply_checkout_completed_subscription`
        // (i.e., is NOT diverted to `claim_and_record_unprocessable`) — it
        // does not need `retrieve_subscription` to succeed, so it asserts on
        // the FAILURE shape (500, subscription retrieve failed against a
        // mock that only serves `/v1/checkout/sessions/*`) rather than a
        // full happy path, which the dedicated
        // `checkout_completed_subscription_issues_the_pro_grant_and_mirrors_the_subscription`
        // test above already covers end to end.
        let event_id = format!("evt_{}", Uuid::new_v4().simple());
        let session = json!({"id": "cs_mock_1", "mode": "subscription", "subscription": "sub_mock_1"});

        let resp = handle_checkout_completed(&state, &pool, account_id, &event_id, "checkout.session.completed", &session).await;
        let body: serde_json::Value = {
            let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX).await.expect("read body");
            serde_json::from_slice(&bytes).expect("json")
        };
        assert_ne!(
            body.get("unprocessable"), Some(&serde_json::Value::Bool(true)),
            "a well-formed single-item session must never be diverted to the unprocessable path"
        );

        cleanup(&pool, account_id, &users).await;
    }

    // ── Finding 10: per-session dedup on `checkout.session.completed` (payment) ─

    /// **Regression test for finding 10.** Two DIFFERENT `event_id`s
    /// describing the SAME checkout session (the shape a dashboard resend,
    /// or any other way Stripe can mint a second event for one object,
    /// produces) must credit the account exactly ONCE — the
    /// `stripe_webhook_events` claim alone does not catch this, since each
    /// event id passes its OWN claim independently.
    #[tokio::test]
    async fn two_distinct_events_for_the_same_checkout_session_credit_only_once() {
        let Some(pool) =
            pg_test_pool("two_distinct_events_for_the_same_checkout_session_credit_only_once").await
        else {
            return;
        };
        let _catalog_guard = CATALOG_LOCK.lock().await;
        let (account_id, users, _customer) = seed_linked_account(&pool, 1).await;
        set_catalog_price(&pool, "credit_packs", "pack_500", "price_pack_500_mock_4").await;
        let line_items = vec![crate::billing::stripe_client::SessionLineItem {
            price_id: "price_pack_500_mock_4".to_string(),
            quantity: 1,
        }];

        let first_event_id = format!("evt_{}", Uuid::new_v4().simple());
        let first = apply_checkout_completed_payment(
            &pool,
            account_id,
            &first_event_id,
            "checkout.session.completed",
            "cs_dedup_1",
            &line_items,
        )
        .await;
        assert_eq!(first.status(), StatusCode::OK);
        assert_eq!(read_credit_balance(&pool, account_id).await, Some(Decimal::from(500)));

        // A SECOND, genuinely different event_id, same session_id.
        let second_event_id = format!("evt_{}", Uuid::new_v4().simple());
        assert_ne!(first_event_id, second_event_id);
        let second = apply_checkout_completed_payment(
            &pool,
            account_id,
            &second_event_id,
            "checkout.session.completed",
            "cs_dedup_1",
            &line_items,
        )
        .await;
        assert_eq!(second.status(), StatusCode::OK, "the second event must still be accepted, not erred");
        assert_eq!(
            read_credit_balance(&pool, account_id).await,
            Some(Decimal::from(500)),
            "the SAME session must never credit twice, even under two different event ids"
        );

        // Both events are independently recorded — the audit trail keeps
        // them distinct, and the second one's outcome says why it credited
        // nothing.
        let second_outcome: Option<String> =
            sqlx::query_scalar("SELECT outcome FROM stripe_webhook_events WHERE event_id = $1")
                .bind(&second_event_id)
                .fetch_one(&pool)
                .await
                .expect("second event must be recorded");
        assert!(
            second_outcome.as_deref().is_some_and(|o| o.contains("duplicate_session")),
            "{second_outcome:?}"
        );

        let ledger_rows: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM extra_usage_ledger WHERE account_id = $1 AND entry_kind = 'pack_purchase'",
        )
        .bind(account_id)
        .fetch_one(&pool)
        .await
        .expect("count ledger rows");
        assert_eq!(ledger_rows, 1, "exactly one ledger row for the one session, regardless of event count");

        cleanup(&pool, account_id, &users).await;
    }

    /// **Positive control for finding 10**: two DIFFERENT sessions must both
    /// credit — without this, a bug that deduped on ANYTHING account-wide
    /// (rather than per-session) would also pass the test above.
    #[tokio::test]
    async fn two_different_checkout_sessions_both_credit() {
        let Some(pool) = pg_test_pool("two_different_checkout_sessions_both_credit").await else {
            return;
        };
        let _catalog_guard = CATALOG_LOCK.lock().await;
        let (account_id, users, _customer) = seed_linked_account(&pool, 1).await;
        set_catalog_price(&pool, "credit_packs", "pack_500", "price_pack_500_mock_5").await;
        let line_items = vec![crate::billing::stripe_client::SessionLineItem {
            price_id: "price_pack_500_mock_5".to_string(),
            quantity: 1,
        }];

        let first = apply_checkout_completed_payment(
            &pool,
            account_id,
            &format!("evt_{}", Uuid::new_v4().simple()),
            "checkout.session.completed",
            "cs_dedup_a",
            &line_items,
        )
        .await;
        assert_eq!(first.status(), StatusCode::OK);
        let second = apply_checkout_completed_payment(
            &pool,
            account_id,
            &format!("evt_{}", Uuid::new_v4().simple()),
            "checkout.session.completed",
            "cs_dedup_b",
            &line_items,
        )
        .await;
        assert_eq!(second.status(), StatusCode::OK);

        assert_eq!(
            read_credit_balance(&pool, account_id).await,
            Some(Decimal::from(1000)),
            "two DIFFERENT sessions must both credit — 500 + 500"
        );

        cleanup(&pool, account_id, &users).await;
    }
    // ── Settings launch PR 3.3: cancellation mirror, card copy, refund/dispute alerts ──

    mod settings_3_3 {
        use super::*;
        use crate::billing::test_support::{subscription_json, MockStripe};
        use std::sync::{Arc as StdArc, Mutex as StdMutex};
        use tracing::instrument::WithSubscriber;

        fn event() -> String {
            format!("evt_{}", Uuid::new_v4().simple())
        }

        async fn cancel_state(
            pool: &PgPool,
            account_id: Uuid,
        ) -> (bool, Option<chrono::DateTime<chrono::Utc>>, String) {
            sqlx::query_as(
                "SELECT cancel_at_period_end, cancel_at, status FROM stripe_subscriptions WHERE account_id = $1",
            )
            .bind(account_id)
            .fetch_one(pool)
            .await
            .expect("read cancel state")
        }

        fn updated_payload(
            sub_id: &str,
            start: i64,
            end: i64,
            cancel: bool,
            cancel_at: Option<i64>,
        ) -> Value {
            json!({
                "id": sub_id,
                "status": "active",
                "cancel_at_period_end": cancel,
                "cancel_at": cancel_at,
                "items": {"data": [{
                    "quantity": 1,
                    "current_period_start": start,
                    "current_period_end": end,
                }]}
            })
        }

        #[tokio::test]
        async fn subscription_updated_persists_cancel_state_and_a_replay_changes_nothing() {
            let Some(pool) = pg_test_pool("cancel_state_persist").await else { return };
            let (account_id, users, _c) = seed_linked_account(&pool, 1).await;
            let sub_id = linked_subscription_id(&pool, account_id).await;
            let (start, end) = period();
            let cancel_at = end.timestamp();
            let e1 = event();
            let canceling = updated_payload(&sub_id, start.timestamp(), end.timestamp(), true, Some(cancel_at));

            let r1 = handle_subscription_updated(&pool, None, account_id, &e1, "customer.subscription.updated", &canceling).await;
            let after_cancel = cancel_state(&pool, account_id).await;

            // The user un-cancels (new event), then the ORIGINAL cancel event
            // is redelivered: a replay must not re-apply it.
            let uncancel = updated_payload(&sub_id, start.timestamp(), end.timestamp(), false, None);
            let r2 = handle_subscription_updated(&pool, None, account_id, &event(), "customer.subscription.updated", &uncancel).await;
            let after_uncancel = cancel_state(&pool, account_id).await;
            let r3 = handle_subscription_updated(&pool, None, account_id, &e1, "customer.subscription.updated", &canceling).await;
            let after_replay = cancel_state(&pool, account_id).await;
            cleanup(&pool, account_id, &users).await;

            assert_eq!(r1.status(), StatusCode::OK);
            assert_eq!(r2.status(), StatusCode::OK);
            assert_eq!(r3.status(), StatusCode::OK);
            assert_eq!((after_cancel.0, after_cancel.1), (true, to_utc(cancel_at)));
            assert_eq!((after_uncancel.0, after_uncancel.1), (false, None));
            assert_eq!(
                (after_replay.0, after_replay.1),
                (false, None),
                "a redelivered event must not resurrect a cancellation the customer reverted"
            );
            assert_eq!(after_replay.2, "active", "cancelling at period end does not freeze");
        }

        #[tokio::test]
        async fn the_live_subscription_wins_over_an_out_of_order_payload() {
            let Some(pool) = pg_test_pool("cancel_state_live_wins").await else { return };
            let (account_id, users, _c) = seed_linked_account(&pool, 1).await;
            let sub_id = linked_subscription_id(&pool, account_id).await;
            let (start, end) = period();
            // A late "cancelled" event arrives after the customer un-cancelled.
            let late_cancel = updated_payload(&sub_id, start.timestamp(), end.timestamp(), true, Some(end.timestamp()));
            let mock = MockStripe::spawn().await;
            mock.respond(
                "GET",
                &format!("/v1/subscriptions/{sub_id}"),
                200,
                subscription_json(&sub_id, "active", &[("si_1", "price_x", 1)], false, None, None),
            );
            let r = handle_subscription_updated(&pool, Some(&*mock.client), account_id, &event(), "customer.subscription.updated", &late_cancel).await;
            let state = cancel_state(&pool, account_id).await;
            cleanup(&pool, account_id, &users).await;
            assert_eq!(r.status(), StatusCode::OK);
            assert_eq!((state.0, state.1), (false, None));
        }

        #[tokio::test]
        async fn a_stale_period_or_a_superseded_subscription_does_not_write_cancel_state() {
            let Some(pool) = pg_test_pool("cancel_state_stale_and_superseded").await else { return };
            let (account_id, users, _c) = seed_linked_account(&pool, 1).await;
            let sub_id = linked_subscription_id(&pool, account_id).await;
            let (start, end) = period();
            // Newer period first.
            let newer_start = end.timestamp();
            let newer_end = newer_start + 2_592_000;
            let ok = updated_payload(&sub_id, newer_start, newer_end, false, None);
            handle_subscription_updated(&pool, None, account_id, &event(), "customer.subscription.updated", &ok).await;
            // An OLDER-period event claiming cancel: stale, ignored.
            let stale = updated_payload(&sub_id, start.timestamp(), end.timestamp(), true, Some(end.timestamp()));
            let r_stale = handle_subscription_updated(&pool, None, account_id, &event(), "customer.subscription.updated", &stale).await;
            let after_stale = cancel_state(&pool, account_id).await;
            // A current-period event about a DIFFERENT subscription id.
            let other = updated_payload("sub_someone_else", newer_start, newer_end, true, Some(newer_end));
            handle_subscription_updated(&pool, None, account_id, &event(), "customer.subscription.updated", &other).await;
            let after_other = cancel_state(&pool, account_id).await;
            cleanup(&pool, account_id, &users).await;
            assert_eq!(r_stale.status(), StatusCode::OK);
            assert!(!after_stale.0, "stale event must not set cancel_at_period_end");
            assert!(!after_other.0, "an event for another subscription must not mark this one cancelling");
        }

        #[tokio::test]
        async fn subscription_deleted_persists_the_final_cancel_fields_and_freezes() {
            let Some(pool) = pg_test_pool("cancel_state_deleted").await else { return };
            let (account_id, users, _c) = seed_linked_account(&pool, 1).await;
            let sub_id = linked_subscription_id(&pool, account_id).await;
            let at = 1_702_678_400;
            let sub = json!({"id": sub_id, "status": "canceled", "cancel_at_period_end": true, "cancel_at": at});
            let r = handle_subscription_deleted(&pool, account_id, &event(), "customer.subscription.deleted", &sub).await;
            let state = cancel_state(&pool, account_id).await;
            // Superseded deletion (other id) writes nothing further.
            let other = json!({"id": "sub_old", "status": "canceled", "cancel_at_period_end": false, "cancel_at": null});
            handle_subscription_deleted(&pool, account_id, &event(), "customer.subscription.deleted", &other).await;
            let after_other = cancel_state(&pool, account_id).await;
            cleanup(&pool, account_id, &users).await;
            assert_eq!(r.status(), StatusCode::OK);
            assert_eq!((state.0, state.1, state.2.as_str()), (true, to_utc(at), "canceled"));
            assert_eq!((after_other.0, after_other.1), (true, to_utc(at)));
        }

        #[tokio::test]
        async fn checkout_completion_copies_the_subscription_card_to_the_customer_once() {
            let Some(pool) = pg_test_pool("checkout_copies_card").await else { return };
            let _catalog_guard = CATALOG_LOCK.lock().await;
            let (account_id, users, customer) = seed_linked_account(&pool, 1).await;
            set_catalog_price(&pool, "plans", "pro", "price_pro_card").await;
            let mock = MockStripe::spawn().await;
            let mut sub = subscription_json("sub_card_1", "active", &[("si_1", "price_pro_card", 1)], false, None, Some("pm_new"));
            sub["customer"] = json!(customer);
            mock.respond("GET", "/v1/subscriptions/sub_card_1", 200, sub);
            // The customer has no default yet.
            mock.respond("GET", &format!("/v1/customers/{customer}"), 200, json!({"id": customer, "invoice_settings": {"default_payment_method": null}}));
            mock.respond("POST", &format!("/v1/customers/{customer}"), 200, json!({"id": customer}));
            let state = Arc::new({
                let mut s = crate::test_app_state_owned_with_db(pool.clone(), None);
                s.stripe = Some(mock.client.clone());
                s
            });
            let line = crate::billing::stripe_client::SessionLineItem { price_id: "price_pro_card".into(), quantity: 1 };
            let evt = event();
            let r1 = apply_checkout_completed_subscription(&state, &pool, account_id, &evt, "cs_1", "sub_card_1", &line).await;
            let r2 = apply_checkout_completed_subscription(&state, &pool, account_id, &evt, "cs_1", "sub_card_1", &line).await;
            let posts = mock.calls("POST", &format!("/v1/customers/{customer}"));
            let mirror = cancel_state(&pool, account_id).await;
            cleanup(&pool, account_id, &users).await;
            assert_eq!(r1.status(), StatusCode::OK);
            assert_eq!(r2.status(), StatusCode::OK);
            assert_eq!(posts.len(), 1, "the replay must not touch the customer again");
            assert_eq!(posts[0].form["invoice_settings[default_payment_method]"], "pm_new");
            assert_eq!(posts[0].idempotency_key.as_deref(), Some(format!("cust-default-pm:{customer}:pm_new").as_str()));
            assert!(!mirror.0, "a fresh subscription is not cancelling");
        }

        #[tokio::test]
        async fn checkout_completion_does_not_guess_a_card_when_the_subscription_has_no_default() {
            let Some(pool) = pg_test_pool("checkout_card_no_default").await else { return };
            let _catalog_guard = CATALOG_LOCK.lock().await;
            let (account_id, users, customer) = seed_linked_account(&pool, 1).await;
            set_catalog_price(&pool, "plans", "pro", "price_pro_card").await;
            let mock = MockStripe::spawn().await;
            let mut sub = subscription_json("sub_card_3", "active", &[("si_1", "price_pro_card", 1)], false, None, None);
            sub["customer"] = json!(customer);
            mock.respond("GET", "/v1/subscriptions/sub_card_3", 200, sub);
            // A card IS on file, but it must not be picked.
            mock.respond("GET", "/v1/payment_methods", 200, json!({"data": [{"id": "pm_other", "card": {"brand": "visa", "last4": "1111", "exp_month": 1, "exp_year": 2030}}], "has_more": false}));
            mock.respond("POST", &format!("/v1/customers/{customer}"), 200, json!({"id": customer}));
            let state = Arc::new({
                let mut s = crate::test_app_state_owned_with_db(pool.clone(), None);
                s.stripe = Some(mock.client.clone());
                s
            });
            let line = crate::billing::stripe_client::SessionLineItem { price_id: "price_pro_card".into(), quantity: 1 };
            let r = apply_checkout_completed_subscription(&state, &pool, account_id, &event(), "cs_3", "sub_card_3", &line).await;
            let posts = mock.calls("POST", &format!("/v1/customers/{customer}"));
            let lists = mock.calls("GET", "/v1/payment_methods");
            cleanup(&pool, account_id, &users).await;
            assert_eq!(r.status(), StatusCode::OK);
            assert!(posts.is_empty(), "no default set from a guessed card");
            assert!(lists.is_empty(), "the card list is not consulted");
        }

        #[tokio::test]
        async fn checkout_completion_leaves_an_unchanged_customer_default_alone() {
            let Some(pool) = pg_test_pool("checkout_card_already_default").await else { return };
            let _catalog_guard = CATALOG_LOCK.lock().await;
            let (account_id, users, customer) = seed_linked_account(&pool, 1).await;
            set_catalog_price(&pool, "plans", "pro", "price_pro_card").await;
            let mock = MockStripe::spawn().await;
            let mut sub = subscription_json("sub_card_2", "active", &[("si_1", "price_pro_card", 1)], false, None, Some("pm_same"));
            sub["customer"] = json!(customer);
            mock.respond("GET", "/v1/subscriptions/sub_card_2", 200, sub);
            mock.respond("GET", &format!("/v1/customers/{customer}"), 200, json!({"id": customer, "invoice_settings": {"default_payment_method": "pm_same"}}));
            let state = Arc::new({
                let mut s = crate::test_app_state_owned_with_db(pool.clone(), None);
                s.stripe = Some(mock.client.clone());
                s
            });
            let line = crate::billing::stripe_client::SessionLineItem { price_id: "price_pro_card".into(), quantity: 1 };
            let r = apply_checkout_completed_subscription(&state, &pool, account_id, &event(), "cs_2", "sub_card_2", &line).await;
            let posts = mock.calls("POST", &format!("/v1/customers/{customer}"));
            cleanup(&pool, account_id, &users).await;
            assert_eq!(r.status(), StatusCode::OK);
            assert!(posts.is_empty());
        }

        /// Captures every log line emitted while `fut` runs.
        async fn capture_logs<F: std::future::Future>(fut: F) -> (F::Output, String) {
            #[derive(Clone)]
            struct Buf(StdArc<StdMutex<Vec<u8>>>);
            impl std::io::Write for Buf {
                fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
                    self.0.lock().unwrap().extend_from_slice(b);
                    Ok(b.len())
                }
                fn flush(&mut self) -> std::io::Result<()> {
                    Ok(())
                }
            }
            impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for Buf {
                type Writer = Buf;
                fn make_writer(&'a self) -> Buf {
                    self.clone()
                }
            }
            let buf = Buf(StdArc::new(StdMutex::new(Vec::new())));
            let sub = tracing_subscriber::fmt()
                .with_writer(buf.clone())
                .with_ansi(false)
                .with_max_level(tracing::Level::TRACE)
                .finish();
            let out = fut.with_subscriber(sub).await;
            let text = String::from_utf8_lossy(&buf.0.lock().unwrap()).to_string();
            (out, text)
        }

        async fn webhook_state(pool: &PgPool, stripe: Option<&MockStripe>) -> Arc<AppState> {
            let mut s = crate::test_app_state_owned_with_db(pool.clone(), None);
            if let Some(m) = stripe {
                s.stripe = Some(m.client.clone());
            }
            Arc::new(s)
        }

        async fn money_rows(pool: &PgPool, account_id: Uuid) -> i64 {
            let a: i64 = count(pool, "SELECT count(*) FROM credit_ledger WHERE account_id = $1", account_id).await;
            let b: i64 = count(pool, "SELECT count(*) FROM extra_usage_ledger WHERE account_id = $1", account_id).await;
            let c: i64 = count(pool, "SELECT count(*) FROM extra_usage_balances WHERE account_id = $1", account_id).await;
            a + b + c
        }

        #[tokio::test]
        async fn a_refund_alerts_once_changes_no_balance_and_a_replay_is_silent() {
            let Some(pool) = pg_test_pool("refund_alert_only").await else { return };
            let (account_id, users, customer) = seed_linked_account(&pool, 1).await;
            let state = webhook_state(&pool, None).await;
            let before = money_rows(&pool, account_id).await;
            let charge = json!({"id": "ch_1", "customer": customer, "amount": 1100, "amount_refunded": 1100, "currency": "usd", "payment_intent": "pi_1"});
            let evt = event();
            let (r1, log1) = capture_logs(handle_manual_review_event(&state, &evt, "charge.refunded", &charge)).await;
            let (r2, log2) = capture_logs(handle_manual_review_event(&state, &evt, "charge.refunded", &charge)).await;
            let after = money_rows(&pool, account_id).await;
            let outcome = unprocessable_outcome_any(&pool, &evt).await;
            cleanup(&pool, account_id, &users).await;
            assert_eq!(r1.status(), StatusCode::OK);
            assert_eq!(r2.status(), StatusCode::OK);
            assert!(log1.contains("alert=true") && log1.contains("stripe_refund_or_dispute") && log1.contains("ch_1"), "{log1}");
            assert!(log1.contains(&account_id.to_string()), "alert carries the account: {log1}");
            assert!(!log2.contains("alert=true"), "a replay must not alert again: {log2}");
            assert_eq!(before, after, "no balance or ledger change");
            assert_eq!(outcome.as_deref(), Some("manual_review_alert_only no_balance_change"));
        }

        async fn unprocessable_outcome_any(pool: &PgPool, event_id: &str) -> Option<String> {
            sqlx::query_scalar("SELECT outcome FROM stripe_webhook_events WHERE event_id = $1")
                .bind(event_id)
                .fetch_optional(pool)
                .await
                .expect("read outcome")
                .flatten()
        }

        #[tokio::test]
        async fn a_dispute_resolves_its_account_through_the_charge_and_an_unknown_customer_still_alerts() {
            let Some(pool) = pg_test_pool("dispute_alert_only").await else { return };
            let (account_id, users, customer) = seed_linked_account(&pool, 1).await;
            let mock = MockStripe::spawn().await;
            mock.respond("GET", "/v1/charges/ch_d1", 200, json!({"id": "ch_d1", "customer": customer}));
            let state = webhook_state(&pool, Some(&mock)).await;
            let dispute = json!({"id": "dp_1", "charge": "ch_d1", "amount": 1100, "currency": "usd", "reason": "fraudulent", "status": "needs_response", "payment_intent": "pi_d"});
            let evt = event();
            let (r, log) = capture_logs(handle_manual_review_event(&state, &evt, "charge.dispute.created", &dispute)).await;
            let claimed = unprocessable_outcome_any(&pool, &evt).await;
            let money = money_rows(&pool, account_id).await;

            // Unknown customer: alert, 200, nothing claimed.
            let unknown = json!({"id": "ch_u", "customer": "cus_unknown_xyz", "amount": 5, "amount_refunded": 5, "currency": "usd"});
            let evt2 = event();
            let (r2, log2) = capture_logs(handle_manual_review_event(&state, &evt2, "charge.refunded", &unknown)).await;
            let claimed2 = unprocessable_outcome_any(&pool, &evt2).await;
            cleanup(&pool, account_id, &users).await;

            assert_eq!(r.status(), StatusCode::OK);
            assert!(log.contains("alert=true") && log.contains("dp_1") && log.contains("fraudulent"), "{log}");
            assert!(claimed.is_some());
            assert_eq!(money, 0);
            assert_eq!(r2.status(), StatusCode::OK);
            assert!(log2.contains("alert=true") && log2.contains("ch_u"), "{log2}");
            assert!(claimed2.is_none());
        }

        #[tokio::test]
        async fn a_dispute_without_a_customer_field_is_accepted_not_rejected() {
            let Some(pool) = pg_test_pool("refund_dispute_dispatch").await else { return };
            let state = webhook_state(&pool, None).await;
            // A dispute has no `customer`: the dispatcher must not 400 it.
            let object = json!({"id": "dp_nocust", "charge": "ch_nocust", "amount": 1, "currency": "usd"});
            let resp = handle_manual_review_event(&state, &event(), "charge.dispute.created", &object).await;
            assert_eq!(resp.status(), StatusCode::OK);
            assert!(EVENT_DISPUTE_CREATED == "charge.dispute.created" && EVENT_CHARGE_REFUNDED == "charge.refunded");
        }
    }
}

#[path = "stripe_webhook_resubscribe_pg_tests.rs"]
#[cfg(test)]
mod resubscribe_pg_tests;
