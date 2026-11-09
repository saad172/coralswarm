//! Auto-add (CS-447, org-onboarding plan §3 item 9,
//! `docs/plans/org-onboarding-from-the-invite.md`, D1/D11/D12/D16) — the
//! three gates, the toggle behind them, and the ledger of every add no
//! human made.
//!
//! # Read this module as the feature D1 exists to keep away from the card
//!
//! D1, in as many words: *"Auto-add consumes a paid-for seat and **never
//! calls Stripe**. Exhaustion falls back to approval."* Everything here is
//! shaped by that. The pool is not a predicate this module evaluates and
//! then acts on — it is a RESERVATION
//! ([`crate::billing::slot_pool::reserve_slot`]) whose `None` is the whole
//! decision, taken inside the same locked transaction that would have
//! granted it. There is no code path from this module to a Stripe client,
//! and [`decide`] cannot express one.
//!
//! # The three gates (D11, D12, D1)
//!
//! Auto-add fires only when ALL THREE hold:
//!
//! ```text
//! 1. domain    the D16 ladder admits (rung 1 or rung 2, never rung 3) —
//!              and rung 2's domain half is DNS-verified only
//! 2. pool      a slot is free IN THE CHOSEN TIER, and reserving it wins
//! 3. tombstone there is no `org_membership_tombstones` row for them
//! ```
//!
//! Anything else falls to the approval queue — **silently to the
//! requester** (D3/D10: always `202 sent_to_admin`, never the pool state,
//! never a rejection). Which gate refused is recorded for the ADMIN, in
//! `org_join_requests.source` and `evidence`, and never told to the person
//! who asked.
//!
//! [`decide`] is the whole rule, pure and database-free, so the rule can be
//! tested without a Postgres and cannot be re-derived at a call site. The
//! pool is deliberately NOT one of its inputs: a function taking
//! `slot_available: bool` would be a check-then-act window, and worse, it
//! would read as a gate while the real gate lived somewhere else (this
//! repo's own "gate predicate from its subject" lesson). [`decide`] answers
//! *may we reach for a slot*; reaching is the caller's next statement.
//!
//! # Off by default, structurally
//!
//! D11: *"The toggle itself is off by default — the founder's framing: a
//! setting an admin turns on to manage spend."* Two things implement that,
//! not one:
//!
//!   * `org_auto_add_settings.enabled` defaults to `false`, and
//!   * **a missing row is also off** — [`read_settings`] returns
//!     [`AutoAddSettings::disabled`] rather than an `Option` every caller
//!     would have to remember to treat as off.
//!
//! A fresh org has no row and therefore auto-adds nobody, without anything
//! having been run for it. A read FAILURE is also off, logged, for the same
//! reason `assertion_ladder::domain_signal` fails closed: a database this
//! module cannot reach must never be the reason a stranger gets a slot.
//!
//! # The tier is chosen on the toggle (founder, 2026-09-22)
//!
//! Approval lets an admin pick the slot tier per request (item 40, CS-442).
//! Auto-add has no human at the moment of spending, so the choice is made
//! in advance and stored: `org_auto_add_settings.default_seat_tier_id`,
//! NULL meaning `standard` (0241's own NULL semantics).
//!
//! **The pool check is against THAT TIER ONLY.** If the chosen tier has no
//! free slot, auto-add falls to approval with `source = 'pool_full'` even
//! when another tier has slots sitting idle — a `premium` toggle must never
//! quietly spend a `standard` slot an admin bought for somebody else. That
//! narrowing is `reserve_slot`'s own, via
//! [`crate::billing::slot_pool::read_slot_pool_for_tier_ledger`] — the
//! LEDGER reader, never the report reader (CS-444: a reduce/consume writes
//! `grant_seat_tiers`, so a capacity gate must read the lagging key the
//! ledger is written under, or a member mid-downgrade makes a slot look
//! free).
//!
//! # Every add is reported (D13, as amended 2026-09-22)
//!
//! An auto-added member never creates a join request, so nothing in
//! `org_join_requests` would ever tell an admin it happened.
//! [`record_auto_add`] writes `org_auto_adds` — who, which rung, which tier
//! — and `workers::org_join_request_digest` reads it as a second, separate
//! section of the existing daily digest.
//!
//! # RLS
//!
//! `org_auto_add_settings` and `org_auto_adds` (0247) both enable RLS with
//! a strict `app.account_id` predicate, and `state.control_db()` is
//! `coralswarm_app` (`rolbypassrls = f`). Every read and write below opens
//! [`crate::tenancy::scope::begin_account_scoped`] first; there is no
//! unscoped path to either table in production code.

use serde_json::Value as Json;
use sqlx::{PgConnection, PgPool};
use uuid::Uuid;

use crate::services::assertion_ladder::{DomainSignal, LadderOutcome, LadderSignals, Rung};
use crate::tenancy::scope::begin_account_scoped;

#[derive(Debug, thiserror::Error)]
pub(crate) enum AutoAddError {
    #[error("database error: {0}")]
    Db(#[from] sqlx::Error),
    #[error("store error: {0}")]
    Store(#[from] crate::store::StoreError),
    /// CS-449 (item 38): [`regate_queued_request`]'s tombstone re-check goes
    /// through [`crate::clerk_provision::invite_suppressed_by_tombstone`],
    /// whose own error type this crate does not otherwise plumb into this
    /// module.
    #[error("provision error: {0}")]
    Provision(#[from] crate::clerk_provision::ProvisionError),
    /// [`regate_queued_request`]'s rejection re-check goes through
    /// `services::org_join_requests::has_rejected_decision`, whose error
    /// type this module does not otherwise carry.
    #[error("join request error: {0}")]
    JoinRequest(#[from] crate::services::org_join_requests::JoinRequestError),
}

// ── The setting ───────────────────────────────────────────────────────────

/// One org's auto-add configuration. A value, never an `Option` — see the
/// module doc: "a missing row is also off" has to be expressible as a
/// SETTING, or every call site becomes a place the default can be got
/// wrong.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct AutoAddSettings {
    pub enabled: bool,
    /// `seat_tiers.id` this org's auto-adds spend against. `None` =
    /// `standard`, resolved at spend time by [`resolve_tier_id`] — the same
    /// NULL semantics `slot_reservations.seat_tier_id` carries (0241).
    pub default_seat_tier_id: Option<Uuid>,
}

impl AutoAddSettings {
    /// D11's default, and the answer for a missing row, an unreachable
    /// database, and an org nobody has ever configured. Named rather than
    /// written as a literal at three call sites so "what does off look
    /// like" has exactly one answer.
    pub(crate) fn disabled() -> Self {
        Self { enabled: false, default_seat_tier_id: None }
    }
}

/// This org's auto-add settings. A missing row is [`AutoAddSettings::disabled`].
pub(crate) async fn read_settings(
    control: &PgPool,
    account_id: Uuid,
) -> Result<AutoAddSettings, AutoAddError> {
    let mut tx = crate::tenancy::scope::begin_request_account_read(control,account_id).await?;
    let value = read_settings_tx(&mut tx,account_id).await?;
    tx.rollback().await?;
    Ok(value)
}

/// [`read_settings`], with a database failure folded into "off".
///
/// **Fail closed, and say so.** The gate this feeds spends money. An
/// unreachable settings table must produce the same answer as a settings
/// table that says "off" — never "carry on", and never a 500 that would
/// also break the ordinary approval path this route shares. Same discipline
/// `assertion_ladder::domain_signal` documents for its own read.
pub(crate) async fn read_settings_or_disabled(control: &PgPool, account_id: Uuid) -> AutoAddSettings {
    match read_settings(control, account_id).await {
        Ok(s) => s,
        Err(e) => {
            tracing::warn!(
                error = %e, account_id = %account_id,
                "org_auto_add: settings read failed — treating auto-add as OFF for this pass \
                 (fail closed; a database we cannot reach never spends a slot)"
            );
            AutoAddSettings::disabled()
        }
    }
}

/// Write this org's auto-add settings (admin action). Upsert on the natural
/// key — one settings row per org is the PRIMARY KEY, not a convention.
pub(crate) async fn upsert_settings(
    control: &PgPool,
    account_id: Uuid,
    enabled: bool,
    default_seat_tier_id: Option<Uuid>,
    updated_by: Uuid,
) -> Result<AutoAddSettings, AutoAddError> {
    let mut tx = begin_account_scoped(control, account_id).await?;
    let (enabled, default_seat_tier_id): (bool, Option<Uuid>) = sqlx::query_as(
        "INSERT INTO org_auto_add_settings (account_id, enabled, default_seat_tier_id, updated_by) \
         VALUES ($1, $2, $3, $4) \
         ON CONFLICT (account_id) DO UPDATE \
           SET enabled = EXCLUDED.enabled, \
               default_seat_tier_id = EXCLUDED.default_seat_tier_id, \
               updated_by = EXCLUDED.updated_by, \
               updated_at = now() \
         RETURNING enabled, default_seat_tier_id",
    )
    .bind(account_id)
    .bind(enabled)
    .bind(default_seat_tier_id)
    .bind(updated_by)
    .fetch_one(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(AutoAddSettings { enabled, default_seat_tier_id })
}

/// Resolve the tier auto-add spends against: the toggle's own choice, or
/// the active `standard` tier when it recorded none.
///
/// Returns `None` only when `standard` itself is missing or inactive, which
/// is a misconfigured catalogue — the caller treats that as "cannot spend"
/// and falls to approval rather than reserving against a tier it guessed.
pub(crate) async fn resolve_tier_id(
    control: &PgPool,
    settings: AutoAddSettings,
) -> Result<Option<Uuid>, AutoAddError> {
    if let Some(id) = settings.default_seat_tier_id {
        return Ok(Some(id));
    }
    let id: Option<Uuid> = sqlx::query_scalar("SELECT id FROM seat_tiers WHERE code = 'standard' AND is_active")
        .fetch_optional(control)
        .await?;
    Ok(id)
}

// ── The decision ──────────────────────────────────────────────────────────

/// Why auto-add did not fire — and, for two of them, what the queued
/// approval request records as its `source`.
///
/// Every variant is a fact for the ADMIN. None of them ever reaches the
/// requester: D3/D10 give them one sentence whatever happened here.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FallReason {
    /// D11's default. The org has not turned auto-add on.
    Disabled,
    /// **D12.** A removal tombstone. Blocks auto-add; does NOT block
    /// approval — the request is queued and flagged so the admin decides
    /// with "previously removed" in front of them.
    Tombstoned,
    /// They are already a member of this org, so there is nothing to add
    /// and certainly nothing to spend. (`create` has no membership
    /// precondition; an existing member POSTing this route must not
    /// consume a second slot.)
    AlreadyMember,
    /// The address does not sit on a domain that ARMS auto-add — no match,
    /// no verified domain at all, or a domain this org proved by emailed
    /// confirmation rather than DNS (D11's 2026-09-22 amendment). D11's
    /// own source value.
    DomainDoesNotArm,
    /// The domain armed, but the ladder still refused: rung 3 ("either one
    /// alone → approval only") or rung `none`. Keeps the request's ORIGINAL
    /// source — `unverified_domain` would be a false statement about a
    /// domain that matched perfectly well.
    LadderTooWeak,
    /// The catalogue has no tier to spend against (see [`resolve_tier_id`]).
    /// Keeps the original source: this is our misconfiguration, not a fact
    /// about the requester.
    NoTier,
    /// **D10.** An admin has already REJECTED this person for this org.
    /// Blocks auto-add absolutely — reversing a human "no" without a human
    /// saying so is the one outcome this whole feature must never produce.
    ///
    /// Distinct from [`Self::Tombstoned`] (D12), which is "we removed them
    /// after they were in". This is "we were asked and we said no". Both
    /// refuse; they are different facts and an admin is told which.
    ///
    /// Keeps the original source, and nothing is queued either: the fall
    /// path's own `enqueue_join_request` re-reads the same `decision =
    /// 'rejected'` predicate and returns `Suppressed`, so no new row is
    /// created. The requester's answer is the same 202 as every other
    /// falling gate.
    PreviouslyRejected,
    /// **The token's classification is too old to act on.** The link still
    /// works — this is not an expiry — but `now - iat` exceeds
    /// [`crate::services::unmatched_recipient_email::AUTO_ADD_MAX_CLASSIFICATION_AGE_SECS`],
    /// so "full member of that workspace" is a claim nobody has re-checked
    /// recently enough to spend a seat on. Falls to the admin queue, which
    /// is what an old link did before auto-add existed.
    StaleClassification,
}

impl FallReason {
    /// The `org_join_requests.source` this refusal writes, or `None` to
    /// keep whatever source the request already had.
    ///
    /// Only TWO of the six override the source, and both are statements
    /// about the REQUESTER that an admin needs in order to decide. The
    /// other four are statements about the ORG (off, misconfigured) or
    /// already recorded elsewhere (`tombstoned_returning` is set by
    /// `create` before this decision is taken; a member is not queued at
    /// all), and overriding the source with one of those would overwrite
    /// what the request is actually FOR. Migration 0238's own `CHECK`
    /// enumerates the permitted values; both of these are in it.
    pub(crate) fn source_override(self) -> Option<&'static str> {
        match self {
            Self::DomainDoesNotArm => Some("unverified_domain"),
            Self::Disabled
            | Self::Tombstoned
            | Self::AlreadyMember
            | Self::LadderTooWeak
            | Self::NoTier
            | Self::PreviouslyRejected
            | Self::StaleClassification => None,
        }
    }

    /// The label written into `evidence.auto_add.blocked_by` — what an
    /// admin reading a queued row is told about why nobody was added
    /// automatically.
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Disabled => "auto_add_off",
            Self::Tombstoned => "tombstoned",
            Self::AlreadyMember => "already_a_member",
            Self::DomainDoesNotArm => "domain_does_not_arm_auto_add",
            Self::LadderTooWeak => "ladder_approval_only",
            Self::NoTier => "no_seat_tier_configured",
            Self::PreviouslyRejected => "previously_rejected",
            Self::StaleClassification => "classification_too_old",
        }
    }
}

/// What [`decide`] concluded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AutoAddDecision {
    /// Gates 1 and 3 passed. **Reach for a slot** — gate 2 is the
    /// reservation the caller takes next, and its `None` is a
    /// [`FallReason`]-equivalent the caller records as `pool_full`.
    ///
    /// Carries the rung that admitted them, for `org_auto_adds.rung` and
    /// the digest's "which rung they qualified on".
    TryReserve(Rung),
    /// Never auto-add.
    Fall(FallReason),
}

/// The gate readings [`decide`] combines — one named field per gate, so no
/// call site can transpose two booleans.
///
/// A struct rather than five positional `bool` parameters on purpose. This
/// is the function that decides whether to grant access and spend money;
/// `decide(true, false, true, false, true, &o)` is unreadable at the call
/// site and a swapped pair would compile, pass every type check, and
/// silently invert a security gate. Named fields make that a compile error
/// instead.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct AutoAddGates {
    /// D11's toggle. Off for every org that has not turned it on.
    pub enabled: bool,
    /// Already a member of this org locally.
    pub already_member: bool,
    /// **D12.** A removal tombstone — "we took it away".
    pub tombstoned: bool,
    /// **D10.** A durable `decision = 'rejected'` row for this
    /// (account, user) — "we were asked and we said no".
    pub previously_rejected: bool,
    /// The token's `cls` classification is recent enough to act on
    /// (`now - iat <= AUTO_ADD_MAX_CLASSIFICATION_AGE_SECS`). `false` when
    /// there is no token at all — see
    /// [`token_classification_is_fresh`].
    pub classification_fresh: bool,
}

/// Is the workspace classification carried by a join-notice token recent
/// enough for auto-add to spend a seat on it?
///
/// `None` (no token, so no token-derived classification) answers **false**,
/// which is the fail-closed half and costs nothing today: the workspace
/// signal reaches the ladder ONLY from a token's `cls` claim, so without a
/// token rung 2 is unreachable and this answer is never the thing that
/// refuses. It is written this way so that if a second source of workspace
/// evidence ever appears, it has to come here and say how old it is,
/// rather than inheriting "fresh" by default.
pub(crate) fn token_classification_is_fresh(
    invited_at: Option<chrono::DateTime<chrono::Utc>>,
    now: chrono::DateTime<chrono::Utc>,
) -> bool {
    let Some(issued) = invited_at else {
        return false;
    };
    (now - issued).num_seconds() <= crate::services::unmatched_recipient_email::AUTO_ADD_MAX_CLASSIFICATION_AGE_SECS
}

/// **The rule.** Pure, database-free, and the only place the gates
/// are combined.
///
/// Order is deliberate and is itself part of the answer, because the order
/// decides which reason an admin is shown:
///
/// 1. **`enabled`** first. When the toggle is off — which is every org's
///    default — that is the ONLY true reason, and reporting "their domain
///    isn't verified" for an org that never turned auto-add on would send
///    an admin to fix a setting that was never being consulted.
/// 2. **`already_member`**, because there is nothing to add.
/// 3. **tombstone (D12)** before the ladder, because a tombstone outranks
///    every piece of evidence for admitting them. *"We took it away"* is
///    the worse of the two human "no"s to undo silently.
/// 4. **rejection (D10)**, the other human "no", immediately after it. A
///    tombstone is reported in preference only because it is the stronger
///    statement; both refuse outright and neither is overridable by any
///    amount of evidence. **This gate is the reason auto-add cannot
///    reverse an admin's decision** — see below.
/// 5. **the ladder's verdict**, and only then the domain half, to
///    distinguish D11's `unverified_domain` from a perfectly good domain
///    with no second signal.
/// 6. **classification freshness**, checked LAST and only on the path that
///    would otherwise add, so that a refusal names the thing that actually
///    refused. A stale token whose domain never armed is told
///    `domain_does_not_arm_auto_add`, which is the useful fact; only a
///    candidate who would have been added hears `classification_too_old`.
///
/// ## Why D10 has to be checked HERE and not left to the enqueue path
///
/// D10's durable per-org suppression lives in
/// [`crate::services::org_join_requests::enqueue_join_request`], which the
/// route calls on the FALLING path — after this function. The adding path
/// returns before ever reaching it (an auto-added member creates no
/// request, per D13's 2026-09-22 amendment). So on the one path that grants
/// access and spends money, the enqueue-side check is never consulted.
///
/// Concretely, without gate 4: an admin rejects someone on day 2; on day 10
/// the org enables auto-add, or buys seats, or finishes its DNS TXT record
/// — any of which closes whichever gate was refusing — and the same link
/// they still hold now admits them. The admin's "no" is reversed by a
/// setting change they made for unrelated reasons, and a seat is spent.
///
/// The asymmetry is what makes it a defect rather than a design: the SEND
/// side already honours rejection (`workers::slack_unmatched_email` skips
/// candidates with a rejected row), so no fresh link is ever mailed to a
/// rejected person. Only the CLICK side was blind, and only for the link
/// they already have.
///
/// Fixed here, reading the same `decision = 'rejected'` predicate the
/// enqueue path uses ([`crate::services::org_join_requests::
/// REJECTED_DECISION_EXISTS_SQL`], shared so the two can never drift),
/// rather than by making `reject` write a tombstone row: a tombstone means
/// "was a member, removed", and writing one for somebody who was never a
/// member would put a false fact in the ledger D12 reads.
///
/// `outcome.verdict.may_auto_add()` is the ONE predicate consulted for
/// step 5 — `assertion_ladder` deliberately exposes it as a method on the
/// verdict so a caller cannot re-derive "close enough" from the raw
/// signals. The `Rung` is read back only to RECORD which rung admitted, and
/// the `unreachable`-shaped arm below is the belt on that: a verdict of
/// `AutoAddEligible` at rung 3 or rung `none` is a contradiction inside
/// `assertion_ladder`, and if one ever appears this refuses to add rather
/// than writing a row `org_auto_adds`' own `CHECK` would reject anyway.
pub(crate) fn decide(gates: AutoAddGates, outcome: &LadderOutcome) -> AutoAddDecision {
    if !gates.enabled {
        return AutoAddDecision::Fall(FallReason::Disabled);
    }
    if gates.already_member {
        return AutoAddDecision::Fall(FallReason::AlreadyMember);
    }
    if gates.tombstoned {
        return AutoAddDecision::Fall(FallReason::Tombstoned);
    }
    if gates.previously_rejected {
        return AutoAddDecision::Fall(FallReason::PreviouslyRejected);
    }
    if outcome.verdict.may_auto_add() {
        return match outcome.rung {
            // Rung 2 rests on the token's `cls` claim, which is a snapshot
            // of a roster walk and decays. Rung 1 (directory) is a LIVE
            // read and does not — it is deliberately not gated on the
            // token's age, and is unreachable from the requester-facing
            // POST anyway (no directory is available there).
            Rung::MemberAndDomain if !gates.classification_fresh => {
                AutoAddDecision::Fall(FallReason::StaleClassification)
            }
            rung @ (Rung::Directory | Rung::MemberAndDomain) => AutoAddDecision::TryReserve(rung),
            // Unreachable through `assertion_ladder::evaluate`; never
            // trusted anyway. Falling is the safe half of the contradiction.
            Rung::SingleSignal | Rung::None => AutoAddDecision::Fall(FallReason::LadderTooWeak),
        };
    }
    // The ladder said no. WHICH no it said decides what the admin is told.
    if domain_arms_auto_add(&outcome.signals.domain) {
        AutoAddDecision::Fall(FallReason::LadderTooWeak)
    } else {
        AutoAddDecision::Fall(FallReason::DomainDoesNotArm)
    }
}

/// Does this domain signal ARM auto-add (as opposed to merely being a
/// match)?
///
/// [`DomainSignal::MatchEmailOnly`] answers **false**, which is the whole
/// point of D11's 2026-09-22 amendment: an emailed confirmation proves
/// somebody holds one mailbox at a domain, a TXT record proves control of
/// the domain itself, and only the second is a safe basis for adding
/// strangers without an admin looking. Written as an exhaustive match, not
/// `matches!(.., Match(_))`, so a fifth variant has to be classified here
/// deliberately rather than defaulting to "does not arm" by a wildcard —
/// the safe default, but a silent one.
fn domain_arms_auto_add(signal: &DomainSignal) -> bool {
    match signal {
        DomainSignal::Match(_) => true,
        DomainSignal::MatchEmailOnly(_) | DomainSignal::NoMatch | DomainSignal::NoVerifiedDomain => false,
    }
}

// ── The ledger ────────────────────────────────────────────────────────────

/// Per-account advisory lock that serializes [`record_auto_add`] against
/// the digest's auto-add SELECT + `sent_at` claim.
///
/// `begin_account_scoped` only pins RLS. It does not order two transactions
/// that both touch `org_auto_adds` / `org_join_request_digests`. Without
/// this lock a digest can SELECT before an in-flight INSERT commits, then
/// write `sent_at` after that INSERT's transaction-start `now()`, and the
/// row is permanently omitted (`added_at > last_sent` stays false).
///
/// Same `hashtextextended`-over-a-formatted-key shape as
/// [`crate::billing::renewal::lock_period_rotation`], namespaced so this
/// key can never collide with period rotation. Both call sites MUST use
/// this helper — two lookalike keys would not serialize against each other.
const AUTO_ADD_DIGEST_LOCK_SQL: &str = "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))";

pub(crate) fn auto_add_digest_lock_key(account_id: Uuid) -> String {
    format!("org_auto_add_digest:{account_id}")
}

pub(crate) async fn lock_auto_add_digest(
    conn: &mut PgConnection,
    account_id: Uuid,
) -> Result<(), sqlx::Error> {
    sqlx::query(AUTO_ADD_DIGEST_LOCK_SQL)
        .bind(auto_add_digest_lock_key(account_id))
        .execute(conn)
        .await?;
    Ok(())
}

/// Record one auto-add: who, which rung admitted them, which tier was
/// spent, and the evidence behind it.
///
/// Written AFTER the membership is real, never before — the same order
/// `routes::org_join_requests::approve` uses for `mark_decided`, and for
/// the same reason: this row is bookkeeping about a membership, so a
/// failure here is logged loudly and never unwinds a real membership. The
/// consequence is stated rather than hidden: a lost row means one add
/// missing from one digest, which is strictly better than a member being
/// removed over a reporting failure.
///
/// Takes [`lock_auto_add_digest`] before the INSERT and stamps `added_at`
/// with `clock_timestamp()` (wall-clock after the lock is held), not
/// transaction-start `now()`. A waiter that began before a digest claimed
/// `sent_at` must not inherit that earlier timestamp or the row is
/// invisible to every later `added_at > last_sent` read.
pub(crate) async fn record_auto_add(
    control: &PgPool,
    account_id: Uuid,
    user_id: Uuid,
    rung: Rung,
    seat_tier_id: Option<Uuid>,
    evidence: Json,
) -> Result<Uuid, AutoAddError> {
    let mut tx = begin_account_scoped(control, account_id).await?;
    lock_auto_add_digest(&mut tx, account_id).await?;
    let id: Uuid = sqlx::query_scalar(
        "INSERT INTO org_auto_adds (account_id, user_id, rung, seat_tier_id, evidence, added_at) \
         VALUES ($1, $2, $3, $4, $5, clock_timestamp()) RETURNING id",
    )
    .bind(account_id)
    .bind(user_id)
    .bind(rung.label())
    .bind(seat_tier_id)
    .bind(evidence)
    .fetch_one(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(id)
}

/// Bound on every per-candidate loop CS-449 (item 38) added — `routes::
/// org_auto_add::queued_preview`'s re-gate, `routes::org_auto_add::
/// admit_queued`'s `request_ids`, and `routes::org_join_requests::
/// drain_org`'s own candidate set. Each candidate costs two account-scoped
/// transactions plus two queries (`regate_queued_request`'s tombstone and
/// rejection reads) before an admit even reaches Clerk, so an unbounded
/// backlog turns one page load, one admin click, or one offboarding webhook
/// into thousands of queries. Same shape and the same order of magnitude as
/// this codebase's own precedents for the identical problem:
/// `workers::member_connect_nudge::MAX_CANDIDATES_PER_TICK` and
/// `workers::org_join_request_digest::MAX_ADMINS_PER_TICK`, both 200.
///
/// One shared constant, deliberately: `queued_preview` shows an admin at
/// most this many passers, so `admit_queued` accepting the same number back
/// means whatever an admin is shown in one page load, they can always
/// submit in one click — a smaller cap on the submit side would strand a
/// page load's own contents.
pub(crate) const MAX_AUTO_ADD_BATCH_CANDIDATES: i64 = 200;

// ── Re-gating an already-queued request (CS-449, item 38, D11's "ask
//    once", and the drain) ──────────────────────────────────────────────

/// Fresh [`AutoAddGates`] plus the STORED ladder outcome for a request that
/// is already sitting in `org_join_requests` — the one thing the
/// queued-preview, admit-queued, and drain paths all need before they may
/// call [`decide`].
///
/// **Every field but the ladder's `directory`/`workspace` halves is
/// re-read, never trusted from when the request was queued.** Time has
/// passed: the person may since have been rejected or tombstoned, may
/// already be a member through some other path, their workspace
/// classification may have gone stale (D11's own 48-hour rule) — and their
/// DOMAIN match may no longer hold, either because the org's own
/// verification lapsed or was removed, or because the person's address has
/// changed since. That last one is re-derived LIVE, not replayed — see the
/// live re-read below, right after `email`, for why.
///
/// The stored `directory`/`workspace` signals ARE replayed — see
/// [`crate::services::assertion_ladder::outcome_from_evidence`]'s own doc
/// comment for why: rung 2's workspace half rests on a join-notice token
/// this server does not hold any more once the row is queued, and a live
/// directory read is unavailable from this call site the same way it is
/// from `create`'s own requester-facing POST. Domain is different: reading
/// an org's own verified-domains table is a live, no-external-dependency,
/// pure database query — [`crate::services::assertion_ladder::
/// domain_signal`]'s own doc comment — so there is no reason to replay it,
/// and a real one not to (see below).
///
/// # Blocker fix, CS-449 review round 2 — the domain replay was a fail-open
///
/// Reusing the STORED domain signal (as this function used to, via
/// `outcome_from_evidence` alone) meant the ladder's rung-2 domain gate —
/// gate 1, the entire basis of D11 — was judged on evidence from QUEUE
/// TIME, not from now:
///
///   * an org that DNS-verifies a domain, then later removes it or lets
///     verification lapse, would still have the drain reconstruct a domain
///     match from the stale evidence and admit a stranger on a domain the
///     org no longer controls, with no admin present;
///   * a person whose address changes off the matching domain (the SAME
///     function reads `users.email` fresh two lines below, for exactly this
///     reason on the tombstone check) would still be admitted on the OLD,
///     no-longer-current address's domain;
///   * and the `Fall` branch for a domain that does not arm rewrites
///     `source` to `unverified_domain` — a WRITE with a lasting effect
///     (removes the row from the drain set for good), taken on the basis of
///     replayed evidence.
///
/// Fixed by re-deriving the domain signal with a live
/// [`crate::services::assertion_ladder::domain_signal`] call against the
/// CURRENT email (see below), then re-running the pure
/// [`crate::services::assertion_ladder::evaluate`] over
/// `{ directory: <replayed>, workspace: <replayed>, domain: <live> }` — so
/// the VERDICT and RUNG this function returns answer to current domain
/// ownership, not merely the domain half of the evidence struct.
///
/// Returns `Ok(None)` when the stored `evidence.ladder` cannot be parsed
/// back (a request queued before CS-447) — the caller must treat that as
/// "cannot tell", i.e. exclude the candidate, never admit it.
pub(crate) async fn regate_queued_request(
    control: &PgPool,
    account_id: Uuid,
    clerk_org_id: &str,
    user_id: Uuid,
    evidence: &Json,
    settings: AutoAddSettings,
    now: chrono::DateTime<chrono::Utc>,
) -> Result<Option<(AutoAddGates, LadderOutcome)>, AutoAddError> {
    let Some(ladder_json) = evidence.get("ladder") else {
        return Ok(None);
    };
    let Some(stored_outcome) = crate::services::assertion_ladder::outcome_from_evidence(ladder_json) else {
        return Ok(None);
    };

    let already_member = crate::routes::org_join_requests::member_exists(control, account_id, user_id).await?;

    // Fresh, not the `evidence.email` this row was queued under — an
    // address change since must be judged on the CURRENT verified email,
    // the same input `create`'s own tombstone check uses, and (below) the
    // same input the LIVE domain re-derivation uses.
    let email: Option<String> = sqlx::query_scalar("SELECT email FROM users WHERE id = $1")
        .bind(user_id)
        .fetch_optional(control)
        .await?;
    let tombstoned = crate::clerk_provision::invite_suppressed_by_tombstone(
        control,
        Some(clerk_org_id),
        Some(user_id),
        email.as_deref(),
    )
    .await?;

    // Blocker fix, CS-449 review round 2: re-derive the domain signal LIVE
    // — see this function's own doc comment, "the domain replay was a
    // fail-open". A missing email (unreachable in practice — see
    // `member_exists`'s own posture on a vanished user row — but not
    // impossible) falls through `domain_signal`'s own
    // `org_domains::email_domain_part` parse, which answers `None` for an
    // empty string; that yields `NoMatch`/`NoVerifiedDomain`, never a
    // match, which is the fail-closed reading this whole function already
    // uses everywhere else.
    let live_domain =
        crate::services::assertion_ladder::domain_signal(control, account_id, email.as_deref().unwrap_or("")).await;
    let outcome = crate::services::assertion_ladder::evaluate(LadderSignals {
        directory: stored_outcome.signals.directory,
        workspace: stored_outcome.signals.workspace,
        domain: live_domain,
    });

    let previously_rejected =
        crate::services::org_join_requests::has_rejected_decision(control, account_id, user_id).await?;

    // D11's 48-hour window, re-measured from NOW against the SAME
    // `invited_at` the request was queued with (`create`'s own
    // `matched_not_member` evidence) — a request that was fresh when
    // queued can go stale while it waits, which is exactly the case this
    // gate exists to catch on a re-check days later.
    let invited_at = evidence
        .get("invited_at")
        .and_then(|v| v.as_str())
        .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
        .map(|dt| dt.with_timezone(&chrono::Utc));
    let classification_fresh = token_classification_is_fresh(invited_at, now);

    Ok(Some((
        AutoAddGates {
            enabled: settings.enabled,
            already_member,
            tombstoned,
            previously_rejected,
            classification_fresh,
        },
        outcome,
    )))
}

/// This org's free slots in `seat_tier_id`, right now — ADVISORY only (this
/// module's own doc comment: "the pool is deliberately NOT one of
/// [`decide`]'s inputs"). [`crate::routes::org_auto_add::queued_preview`]'s
/// own read; nothing that actually spends a slot may use this number
/// instead of calling [`crate::billing::slot_pool::reserve_slot`] for real
/// — see that route's own doc comment for why the count can be stale by
/// the time an admin clicks.
pub(crate) async fn free_slots_for_tier(
    control: &PgPool,
    account_id: Uuid,
    seat_tier_id: Uuid,
) -> Result<i64, AutoAddError> {
    let mut tx = begin_account_scoped(control, account_id).await?;
    let pool = crate::billing::slot_pool::read_slot_pool_for_tier_ledger(&mut tx, account_id, seat_tier_id).await?;
    tx.rollback().await.ok();
    Ok(pool.available())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::services::assertion_ladder::{
        DirectorySignal, LadderSignals, Verdict, WorkspaceSignal,
    };

    fn outcome(verdict: Verdict, rung: Rung, domain: DomainSignal) -> LadderOutcome {
        LadderOutcome {
            verdict,
            rung,
            signals: LadderSignals {
                directory: DirectorySignal::Unavailable("test".to_string()),
                workspace: WorkspaceSignal::FullMember,
                domain,
            },
        }
    }

    fn armed() -> LadderOutcome {
        outcome(Verdict::AutoAddEligible, Rung::MemberAndDomain, DomainSignal::Match("acme.test".into()))
    }

    /// The gate readings of a PERFECT candidate: the toggle is on, they are
    /// not already a member, there is no tombstone, no admin has rejected
    /// them, and their classification is fresh.
    ///
    /// Every test below starts from this and names only the ONE field it is
    /// about (`AutoAddGates { tombstoned: true, ..perfect() }`), so the
    /// thing being varied IS the test, and adding a sixth gate later does
    /// not touch a single existing case.
    fn perfect() -> AutoAddGates {
        AutoAddGates {
            enabled: true,
            already_member: false,
            tombstoned: false,
            previously_rejected: false,
            classification_fresh: true,
        }
    }

    /// **CS-447 negative test 6, the pure half.** D11: off by default.
    /// `enabled = false` refuses the single most auto-addable person there
    /// is — rung 2, DNS-verified domain, no tombstone, not a member.
    #[test]
    fn off_by_default_refuses_even_a_perfect_candidate() {
        assert_eq!(decide(AutoAddGates { enabled: false, ..perfect() }, &armed()), AutoAddDecision::Fall(FallReason::Disabled));
        assert_eq!(
            decide(perfect(), &armed()),
            AutoAddDecision::TryReserve(Rung::MemberAndDomain),
            "…and the SAME candidate is admitted once the toggle is on — which is what proves \
             the refusal above came from the toggle and not from the candidate"
        );
    }

    /// **CS-447 negative test 3, the pure half (D12).** A tombstone blocks
    /// auto-add. It does not override the request's own source
    /// (`create` has already recorded `tombstoned_returning`), and it never
    /// blocks approval — nothing here can.
    #[test]
    fn a_tombstone_blocks_auto_add_and_keeps_the_requests_own_source() {
        assert_eq!(decide(AutoAddGates { tombstoned: true, ..perfect() }, &armed()), AutoAddDecision::Fall(FallReason::Tombstoned));
        assert_eq!(FallReason::Tombstoned.source_override(), None);
    }

    /// **CS-447 negative test 4, the pure half.** An `email`-verified
    /// domain match is a real match — the ladder reports it as
    /// `MatchEmailOnly`, not `NoMatch` — and it still must not auto-add.
    ///
    /// Both halves matter: the refusal, AND that it is reported as
    /// `unverified_domain` rather than as "the ladder was too weak", which
    /// would send an admin looking for a missing second signal instead of
    /// at the domain they proved the wrong way.
    #[test]
    fn an_email_verified_domain_does_not_arm_auto_add() {
        let email_only = outcome(
            Verdict::ApprovalOnly,
            Rung::SingleSignal,
            DomainSignal::MatchEmailOnly("acme.test".into()),
        );
        assert_eq!(decide(perfect(), &email_only), AutoAddDecision::Fall(FallReason::DomainDoesNotArm));
        assert_eq!(FallReason::DomainDoesNotArm.source_override(), Some("unverified_domain"));
        assert!(!domain_arms_auto_add(&DomainSignal::MatchEmailOnly("acme.test".into())));
        assert!(domain_arms_auto_add(&DomainSignal::Match("acme.test".into())));
    }

    /// A domain that matched by DNS, with no second signal, is rung 3 —
    /// "either one alone → approval only" (D16). It falls, but it is NOT
    /// `unverified_domain`: that domain is verified, by DNS, and telling an
    /// admin otherwise is a false sentence about their own DNS record.
    #[test]
    fn a_good_domain_with_no_second_signal_is_not_reported_as_unverified() {
        let rung_three =
            outcome(Verdict::ApprovalOnly, Rung::SingleSignal, DomainSignal::Match("acme.test".into()));
        assert_eq!(decide(perfect(), &rung_three), AutoAddDecision::Fall(FallReason::LadderTooWeak));
        assert_eq!(FallReason::LadderTooWeak.source_override(), None);
    }

    /// Rung 1 admits on its own (D16 as amended: the rungs are OR'd), and
    /// it admits even when the domain half says nothing at all — the
    /// directory IS the org's own identity system.
    #[test]
    fn the_directory_rung_admits_without_a_domain() {
        let directory =
            outcome(Verdict::AutoAddEligible, Rung::Directory, DomainSignal::NoVerifiedDomain);
        assert_eq!(decide(perfect(), &directory), AutoAddDecision::TryReserve(Rung::Directory));
    }

    /// An existing member must never cost a second slot, however strong
    /// their evidence.
    #[test]
    fn an_existing_member_is_never_auto_added_again() {
        assert_eq!(decide(AutoAddGates { already_member: true, ..perfect() }, &armed()), AutoAddDecision::Fall(FallReason::AlreadyMember));
    }

    /// Both gates that CAN override the source write a value migration
    /// 0238's own `CHECK (source IN (...))` permits. A source this database
    /// refuses would turn a fallback into a 500 at exactly the moment we
    /// are trying to be gentle about one.
    #[test]
    fn every_source_override_is_a_value_the_column_permits() {
        const PERMITTED: &[&str] =
            &["self_request", "pool_full", "unverified_domain", "tombstoned_returning", "matched_not_member"];
        for reason in [
            FallReason::Disabled,
            FallReason::Tombstoned,
            FallReason::AlreadyMember,
            FallReason::DomainDoesNotArm,
            FallReason::LadderTooWeak,
            FallReason::NoTier,
            FallReason::PreviouslyRejected,
            FallReason::StaleClassification,
        ] {
            if let Some(source) = reason.source_override() {
                assert!(PERMITTED.contains(&source), "{source} is not in 0238's CHECK");
            }
        }
        // `pool_full` is written by the CALLER (a reservation that came
        // back `None`), not by a `FallReason`, so it is pinned here too —
        // it is the one source in this feature with no variant to carry it.
        assert!(PERMITTED.contains(&crate::routes::org_join_requests::POOL_FULL_SOURCE));
    }

    /// **H1 (security review, 2026-09-22) — D10: auto-add must never
    /// reverse an admin's explicit rejection.**
    ///
    /// The perfect candidate again — toggle on, rung 2, DNS-verified
    /// domain, no tombstone, fresh token — with the ONE difference that an
    /// admin already said no. It must fall, and it must fall with its own
    /// reason rather than being folded into the tombstone's.
    ///
    /// The second half is the half that matters for the bug this test
    /// exists for: rejection must refuse on its own, with the tombstone
    /// gate OPEN. A version of this feature that only checked tombstones
    /// would pass a test that set both.
    #[test]
    fn a_previously_rejected_person_is_never_auto_added() {
        assert_eq!(
            decide(AutoAddGates { previously_rejected: true, ..perfect() }, &armed()),
            AutoAddDecision::Fall(FallReason::PreviouslyRejected),
            "an admin's 'no' outranks every piece of evidence for admitting them"
        );
        assert_eq!(
            decide(AutoAddGates { previously_rejected: true, tombstoned: true, ..perfect() }, &armed()),
            AutoAddDecision::Fall(FallReason::Tombstoned),
            "both apply: the tombstone is the stronger statement and is what the admin is told"
        );
        assert_eq!(
            FallReason::PreviouslyRejected.source_override(),
            None,
            "nothing is queued either — enqueue_join_request suppresses it — so there is no source to set"
        );
        assert_eq!(FallReason::PreviouslyRejected.label(), "previously_rejected");
    }

    /// **H2 (founder ruling, 2026-09-22) — 48 hours, and only for
    /// auto-add.**
    ///
    /// A stale classification refuses at rung 2 and admits at rung 1: the
    /// bound is about the TOKEN's snapshot of a roster walk, not about
    /// directory evidence, which is read live.
    #[test]
    fn a_stale_classification_refuses_rung_two_but_not_rung_one() {
        assert_eq!(
            decide(AutoAddGates { classification_fresh: false, ..perfect() }, &armed()),
            AutoAddDecision::Fall(FallReason::StaleClassification),
            "rung 2 rests on the token's cls claim, which decays"
        );
        let directory = outcome(Verdict::AutoAddEligible, Rung::Directory, DomainSignal::NoMatch);
        assert_eq!(
            decide(AutoAddGates { classification_fresh: false, ..perfect() }, &directory),
            AutoAddDecision::TryReserve(Rung::Directory),
            "a live directory read does not age out with the token"
        );
        // Freshness is checked LAST so a refusal names what actually
        // refused: a stale token whose domain never armed hears about the
        // domain, which is the fact an admin can act on.
        let email_only = outcome(
            Verdict::ApprovalOnly,
            Rung::SingleSignal,
            DomainSignal::MatchEmailOnly("acme.test".into()),
        );
        assert_eq!(
            decide(AutoAddGates { classification_fresh: false, ..perfect() }, &email_only),
            AutoAddDecision::Fall(FallReason::DomainDoesNotArm)
        );
        assert_eq!(FallReason::StaleClassification.label(), "classification_too_old");
    }

    /// The freshness predicate itself, at its boundary and at its
    /// fail-closed edge.
    #[test]
    fn token_freshness_is_measured_from_iat_and_absent_means_not_fresh() {
        use crate::services::unmatched_recipient_email::AUTO_ADD_MAX_CLASSIFICATION_AGE_SECS;
        let now = chrono::Utc::now();
        let at = |secs: i64| Some(now - chrono::Duration::seconds(secs));

        assert!(token_classification_is_fresh(at(0), now));
        assert!(
            token_classification_is_fresh(at(AUTO_ADD_MAX_CLASSIFICATION_AGE_SECS), now),
            "exactly 48h old is still inside the window"
        );
        assert!(
            !token_classification_is_fresh(at(AUTO_ADD_MAX_CLASSIFICATION_AGE_SECS + 1), now),
            "one second past it is not"
        );
        assert!(
            !token_classification_is_fresh(None, now),
            "no token, no token-derived classification — fail closed"
        );
        // The bound is deliberately far shorter than the LINK's own life.
        assert!(
            AUTO_ADD_MAX_CLASSIFICATION_AGE_SECS
                < crate::services::unmatched_recipient_email::JOIN_NOTICE_TOKEN_TTL_SECS,
            "collapsing these two back into one constant is the regression this asserts against"
        );
    }

    #[test]
    fn auto_add_digest_lock_key_is_account_scoped_and_namespaced() {
        let account_id = Uuid::nil();
        assert_eq!(auto_add_digest_lock_key(account_id), format!("org_auto_add_digest:{account_id}"));
        assert_ne!(
            auto_add_digest_lock_key(account_id),
            format!("billing_period_rotation:{account_id}"),
            "must not collide with lock_period_rotation's keyspace"
        );
    }
}

#[cfg(test)]
mod pg_tests {
    use super::*;
    use serde_json::json;
    use sqlx::PgPool;

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

    async fn seed_org(pool: &PgPool) -> (Uuid, Uuid) {
        let user_id: Uuid = sqlx::query_scalar(
            "INSERT INTO users (id, email, clerk_user_id) VALUES (gen_random_uuid(), $1, $2) RETURNING id",
        )
        .bind(format!("cs447-lock-{}@example.test", Uuid::new_v4().simple()))
        .bind(format!("clerk_cs447_lock_{}", Uuid::new_v4().simple()))
        .fetch_one(pool)
        .await
        .expect("insert user");
        let account_id: Uuid = sqlx::query_scalar(
            "INSERT INTO accounts (owner_user_id, clerk_org_id) VALUES ($1, $2) RETURNING id",
        )
        .bind(user_id)
        .bind(format!("org_cs447_lock_{}", Uuid::new_v4().simple()))
        .fetch_one(pool)
        .await
        .expect("insert account");
        (account_id, user_id)
    }

    /// Regression for CodeRabbit #1037 finding 2: `record_auto_add` and the
    /// digest claim share one account-scoped advisory lock. A holder of
    /// [`lock_auto_add_digest`] must block `record_auto_add` until it
    /// commits — `begin_account_scoped` alone does not.
    #[tokio::test]
    async fn record_auto_add_waits_for_the_digest_lock() {
        let Some(pool) = pg_test_pool("record_auto_add_waits_for_the_digest_lock").await else {
            return;
        };
        let (account_id, user_id) = seed_org(&pool).await;

        let mut holder = begin_account_scoped(&pool, account_id).await.expect("holder scope");
        lock_auto_add_digest(&mut holder, account_id).await.expect("holder takes the digest lock");

        let pool_for_record = pool.clone();
        let record = tokio::spawn(async move {
            record_auto_add(
                &pool_for_record,
                account_id,
                user_id,
                Rung::Directory,
                None,
                json!({}),
            )
            .await
        });

        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        assert!(
            !record.is_finished(),
            "record_auto_add must block while another txn holds lock_auto_add_digest"
        );

        holder.commit().await.expect("release the digest lock");
        let id = record.await.expect("join").expect("record_auto_add after lock release");
        let found: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM org_auto_adds WHERE id = $1)")
            .bind(id)
            .fetch_one(&pool)
            .await
            .expect("lookup recorded add");
        assert!(found, "the blocked insert must commit once the digest lock is released");
    }
}

pub(crate) async fn read_settings_tx(conn: &mut sqlx::PgConnection, account_id: Uuid) -> Result<AutoAddSettings, AutoAddError> {
    let row: Option<(bool, Option<Uuid>)> =
        sqlx::query_as("SELECT enabled, default_seat_tier_id FROM org_auto_add_settings WHERE account_id = $1")
            .bind(account_id)
            .fetch_optional(&mut *conn)
            .await?;
    Ok(match row {
        Some((enabled, default_seat_tier_id)) => AutoAddSettings { enabled, default_seat_tier_id },
        None => AutoAddSettings::disabled(),
    })
}
