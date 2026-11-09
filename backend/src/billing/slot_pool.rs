//! The slot pool — CS-435 (org-onboarding plan §3 items 7/15,
//! `docs/plans/org-onboarding-from-the-invite.md`, decisions D1/D7/D9/D15)
//! plus CS-442 (item 40, D8) — the per-tier honesty pass. One arithmetic
//! function pair (account-total and per-tier), one reservation writer pair,
//! one sweep helper — everything downstream
//! (`billing::seat_lifecycle::acquire_seat`'s consume-before-buy branch,
//! `routes::org_slots`' admin "buy N slots" action and its `GET` response,
//! `routes::org_join_requests::approve`, `workers::slot_reservations`'
//! expiry sweep) reads through this module rather than re-deriving any of
//! the three numbers itself.
//!
//! # D7 — derived, never stored
//!
//! `purchased`/`used`/`reserved` are each computed FRESH on every call to
//! [`read_slot_pool`] — never cached, never written to a column of their
//! own. D7's own text: "`seats_purchased` is derived, not stored... a
//! reservation is not a third number... mirrors nothing in Stripe."
//! `purchased` is Σ `grant_seat_tiers.seats` for the account's current
//! `team` grant (the same "most recently granted, non-revoked" selection
//! every other current-grant lookup in this crate uses —
//! `billing::seat_lifecycle::CURRENT_TEAM_GRANT_SQL`,
//! `billing::renewal::CURRENT_GRANT_SQL` — narrowed to just the grant id,
//! then summed against the real purchase ledger, NEVER read off
//! `grants.seats` directly — see [`read_slot_pool`]'s own doc comment for
//! the real bug this module's own tests caught: `grants.seats` is a
//! schema-required, non-purchase placeholder the moment a grant issues,
//! before anyone has bought a single tier); `used` is
//! `COUNT(account_members WHERE seat_purchased_at IS NOT NULL)` — the same
//! predicate `acquire_seat` itself stamps, regardless of whether that stamp
//! came from a fresh Stripe purchase or from consuming a pre-bought slot
//! (this module's own [`crate::billing::seat_lifecycle::acquire_seat`]
//! change); `reserved` is the count of live, unexpired
//! `slot_reservations` rows.
//!
//! # D9 — a reservation spans the gap `acquire_seat`'s own lock cannot
//!
//! [`reserve_slot`] takes the SAME lock discipline every other slot/seat
//! mutation in this crate uses (`tenancy::scope::begin_account_scoped` +
//! `billing::renewal::lock_period_rotation`, S2's shape in
//! `seat_lifecycle`'s own module doc) — but, unlike `acquire_seat`, it runs
//! BEFORE any external call, so "the pool is full" can still fall back to
//! approval instead of becoming a committed member holding no slot. See D9's
//! own "What the race actually is" for the double-spend this closes: two
//! concurrent reads of `available() > 0` with one slot left, both
//! proceeding, is exactly the auto-spend D1 exists to prevent, re-entering
//! through the back door. **Wired as of CS-442**: `routes::org_join_requests::
//! approve` is the first real caller, reserving against the ADMIN'S CHOSEN
//! tier (item 40) — D9's own consumer (the auto-add join flow,
//! org-onboarding plan §3 item 9) is still a separate, not-yet-built story.
//!
//! # CS-442 — per-tier accounting: TWO views, deliberately, not one
//!
//! [`read_slot_pool`] stays the account-wide TOTAL — the members-page meter
//! and `GET /v1/orgs/slots`' top-level fields keep reading it unchanged.
//! Per-tier accounting comes in TWO functions, both keyed off a
//! `seat_tiers.id`, sharing the SAME `purchased` (Σ `grant_seat_tiers.seats`
//! for that grant and tier — the ledger's own key, unchanged by this PR —
//! see `billing::seat_lifecycle::acquire_seat`'s own comment:
//! `grant_seat_tiers` is keyed by the member's plain, LAGGING
//! `seat_tier_id`, never the Stripe-effective/pending one; CS-444 removed
//! `release_seat`'s ledger write entirely, so acquire is now the only seat
//! writer of this table) and the SAME
//! `reserved` (live `slot_reservations` rows WHERE `COALESCE(seat_tier_id,
//! <active 'standard' id>) = $tier` — a pre-CS-442 reservation, migration
//! 0241's own NULL semantics, is treated as `standard`), but DIFFERENT
//! `used` — this split (CodeRabbit, PR #1028) exists because a single
//! `used` definition cannot serve both a CAPACITY GATE and an ADMIN REPORT
//! without creating phantom availability:
//!
//! - `purchased`: Σ `grant_seat_tiers.seats` for that grant AND that tier —
//!   the ledger's own key, unchanged by this PR (see
//!   `billing::seat_lifecycle::acquire_seat`'s own comment:
//!   `grant_seat_tiers` is keyed by the member's plain, LAGGING
//!   `seat_tier_id`, never the Stripe-effective/pending one).
//! - `used` (administrative `tiers[]` report only): `COUNT(account_members
//!   WHERE seat_purchased_at IS NOT NULL AND
//!   COALESCE(pending_seat_tier_id, seat_tier_id) = $tier)` — the SAME
//!   `effective_tier_id` rule `billing::seat_lifecycle::MemberSeatState`
//!   uses (S1: "the tier Stripe is ACTUALLY billing this member's seat at
//!   right now").
//! - `used` (capacity gate — [`reserve_slot`] and `acquire_seat`'s
//!   unreserved consume path): the same count keyed off plain
//!   `seat_tier_id`, matching the ledger. A pending premium→standard
//!   member still occupies the premium seat until `roll_period` moves it.
//! - `reserved`: live `slot_reservations` rows WHERE
//!   `COALESCE(seat_tier_id, <active 'standard' id>) = $tier` — a
//!   pre-CS-442 reservation (`seat_tier_id IS NULL`, migration 0241's own
//!   NULL semantics) is treated as `standard`, since every reservation ever
//!   written before this PR was taken by a caller with no tier concept at
//!   all, and every join-request approval that shipped before CS-442's own
//!   tier-control UI used `standard`'s own default.
//!
//! **The stated ambiguity (CS-442's own flagged gap):** the `tiers[]`
//! report's `purchased` and `used` key off two DIFFERENT tier columns
//! during a pending DOWNGRADE window (`routes::billing_seats::
//! change_seat_tier`, between the change and the next
//! `billing::renewal::roll_period`). A member with `seat_tier_id = pro`,
//! `pending_seat_tier_id = standard` is: still counted in `pro`'s
//! `purchased` (the ledger has not moved yet — it moves at roll), but
//! counted in `standard`'s `used`, not `pro`'s (S1: Stripe already bills
//! them at `standard`). For that window, `pro`'s per-tier view shows a slot
//! that LOOKS free but is not yet reclaimable (the ledger still holds it
//! until roll), and `standard`'s view shows a member USING a slot the
//! ledger has not yet credited it for. This is NOT a bug introduced by
//! CS-442 — it is the same lag `billing::renewal`'s pending-downgrade move
//! and `routes::billing_seats::change_seat_tier` carry between the Stripe
//! item (which moves at request time) and this ledger (which moves at
//! roll). CS-442 just makes it visible per-tier for the first time. (This
//! used to cite `release_seat`'s own "Ledger key is `seat_tier_id`
//! (current), NOT the effective tier" comment; CS-444 deleted that comment
//! along with the ledger write it described, so the citation now names the
//! writers that still have the problem.) The rule chosen for the report is the one
//! CS-442's own build note asked for explicitly (mirror `effective_tier_id`
//! for `used`); the alternative (key the report's `used` off plain
//! `seat_tier_id` too, matching `purchased`) would make a per-tier view
//! agree with the ledger but disagree with what Stripe is actually billing
//! right now — a worse mismatch for the one surface (`routes::org_slots`'
//! admin-facing `tiers[]`) this exists to inform.
//!
//! **Capacity gates do not share that report `used`.** Using the COALESCE
//! count at [`reserve_slot`] / `acquire_seat`'s unreserved consume path
//! would let a second premium approval consume a seat that is still
//! ledger-assigned; after `roll_period` that second member would stay
//! premium with no ledger seat. Those two writers read
//! [`USED_FOR_TIER_LEDGER_KEY_SQL`] instead.
//!
//! # D15 — `slot`, never `seat`, in this module's own vocabulary
//!
//! This module's public names (`SlotPool`, `reserve_slot`,
//! `release_reservation`) deliberately avoid `seat` — see D15's own text for
//! why the collision is not cosmetic. The Rust identifiers this module
//! reads (`account_members.seat_purchased_at`, `grant_seat_tiers`) are
//! pre-existing and are NOT renamed by this decision (founder decision
//! pending, per the plan's own D15 section: "only new things are named
//! slot").

use sqlx::{PgConnection, PgPool};
use uuid::Uuid;

/// Default TTL a [`reserve_slot`] reservation stays live for, absent
/// `SLOT_RESERVATION_TTL_SECS` — five minutes, comfortably longer than the
/// two Clerk network round trips D9's own "what none of it catches" section
/// says the gap actually spans, short enough that a crashed approval does
/// not shrink the visible pool for long. Not read by any caller in this PR
/// (see this module's own doc comment, "Not wired to any caller") — exposed
/// now so the auto-add flow (org-onboarding plan §3 item 9) that eventually
/// calls [`reserve_slot`] has one source of truth for the value rather than
/// re-deriving its own default.
pub const DEFAULT_RESERVATION_TTL_SECS: i64 = 300;

/// [`DEFAULT_RESERVATION_TTL_SECS`], overridable via `SLOT_RESERVATION_TTL_SECS`
/// — same env-override shape `workers::billing_derivation::derivation_interval`
/// uses for its own tunable.
pub fn reservation_ttl() -> chrono::Duration {
    let secs = std::env::var("SLOT_RESERVATION_TTL_SECS")
        .ok()
        .and_then(|v| v.trim().parse::<i64>().ok())
        .filter(|secs| *secs > 0)
        .unwrap_or(DEFAULT_RESERVATION_TTL_SECS);
    chrono::Duration::seconds(secs)
}

/// `SlotPool`'s own error type — mirrors
/// [`crate::billing::seat_lifecycle::SeatLifecycleError`]'s shape (a bare DB
/// wrapper; this module never calls Stripe, so there is no second variant to
/// carry).
#[derive(Debug, thiserror::Error)]
pub enum SlotPoolError {
    #[error("database error: {0}")]
    Db(#[from] sqlx::Error),
}

/// One account's slot pool, snapshotted at read time (D7 — never cached,
/// never stored).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SlotPool {
    /// Σ `grant_seat_tiers.seats` for the account's current `team` grant —
    /// 0 when the account has no live `team` grant at all, OR has one but
    /// has never purchased a single tier (see [`read_slot_pool`]'s own doc
    /// comment for why this is NOT `grants.seats`).
    pub purchased: i64,
    /// `COUNT(account_members WHERE seat_purchased_at IS NOT NULL)` — every
    /// member currently holding a purchased slot, however it was acquired
    /// (a fresh Stripe purchase or a pool consumption).
    pub used: i64,
    /// Live, unexpired `slot_reservations` rows — claims in flight that have
    /// not yet resolved to `fulfilled`/`released`/`expired`.
    pub reserved: i64,
}

impl SlotPool {
    /// How many slots a NEW claim could still take right now. Floored at 0:
    /// `used`/`reserved` can legitimately exceed `purchased` transiently
    /// (a downgrade in Stripe's own dashboard, a purchase that raced a
    /// member leaving) and this must never go negative — the whole point of
    /// [`reserve_slot`]'s own gate is `available() > 0`, and a negative
    /// value would make that gate ambiguous about which direction "not
    /// available" points.
    pub fn available(&self) -> i64 {
        (self.purchased - self.used - self.reserved).max(0)
    }
}

/// Same "most recently granted, non-revoked" selection as
/// `billing::seat_lifecycle::CURRENT_TEAM_GRANT_SQL` / `billing::renewal::
/// CURRENT_GRANT_SQL`, narrowed to the `team` plan — only the grant id,
/// deliberately, see [`read_slot_pool`]'s own doc comment for why `purchased`
/// is NOT read off `grants.seats` directly. `grants` carries RLS (0150) —
/// every call site of [`read_slot_pool`] runs it inside the caller's own
/// already-open, already-scoped transaction.
const CURRENT_TEAM_GRANT_ID_SQL: &str = "\
    SELECT g.id FROM grants g JOIN plans p ON p.id = g.plan_id \
     WHERE g.account_id = $1 AND g.revoked_at IS NULL AND p.code = $2 \
     ORDER BY g.granted_at DESC, g.id DESC LIMIT 1";

/// Σ `grant_seat_tiers.seats` for one grant — the SAME shape
/// `routes::stripe_webhook::apply_subscription_updated`'s own drift check
/// reads (`ledger_seats`), reused here rather than `grants.seats` for the
/// identical reason that check's own comment gives: `grant_seat_tiers` is
/// the actual purchase ledger. `grant_seat_tiers` carries RLS (0168) —
/// scoped by the caller, same as [`CURRENT_TEAM_GRANT_ID_SQL`].
const GRANT_SEAT_TIERS_SUM_SQL: &str =
    "SELECT COALESCE(SUM(seats), 0)::bigint FROM grant_seat_tiers WHERE grant_id = $1";

/// `account_members` carries no RLS (0169's own header, cited throughout
/// `billing::seat_lifecycle`) — a plain unscoped read against the caller's
/// executor is by design, not an oversight.
const USED_SQL: &str =
    "SELECT COUNT(*) FROM account_members WHERE account_id = $1 AND seat_purchased_at IS NOT NULL";

/// `slot_reservations` (0233) carries the same `account_isolation` RLS shape
/// as `grants` — read inside the caller's already-scoped transaction.
const RESERVED_SQL: &str = "\
    SELECT COUNT(*) FROM slot_reservations \
     WHERE account_id = $1 AND released_at IS NULL AND expires_at > now()";

/// Snapshot `account_id`'s slot pool. Generic over neither executor nor
/// connection type — takes `&mut PgConnection` directly, matching
/// `billing::renewal::resync_grant_seats`'s own shape, since every
/// production call site already holds one (either [`reserve_slot`]'s own
/// locked transaction, or a caller's — `routes::org_slots`' `GET`/`POST`
/// handlers, `billing::seat_lifecycle::acquire_seat`'s consume-before-buy
/// branch).
///
/// **`purchased` is Σ `grant_seat_tiers.seats`, never `grants.seats`
/// directly — a real bug this module's own tests caught.** `grants.seats`
/// for a seat-priced grant is schema-required to be non-NULL and `>= 1`
/// (`0156`'s `grants_seats_iff_seat_priced`/`>= 1` CHECK) the MOMENT the
/// grant row is issued, before anyone has purchased a single tier — and
/// `billing::renewal::resync_grant_seats`'s own EXISTS guard leaves it
/// UNTOUCHED whenever `grant_seat_tiers` holds no row at all (that
/// function's own doc comment: "a grant with NO grant_seat_tiers rows at
/// all... must leave grants.seats UNTOUCHED"). So a freshly issued team
/// grant with zero purchases can carry `grants.seats = 1` — a schema
/// placeholder, not a real slot — and reading `purchased` from it would let
/// the FIRST member to join consume a slot nobody ever bought. Summing
/// `grant_seat_tiers` directly is what `apply_subscription_updated`'s own
/// drift check already does for the identical reason.
pub async fn read_slot_pool(conn: &mut PgConnection, account_id: Uuid) -> Result<SlotPool, sqlx::Error> {
    let grant_id: Option<Uuid> = sqlx::query_scalar(CURRENT_TEAM_GRANT_ID_SQL)
        .bind(account_id)
        .bind(crate::billing::MIXED_TIER_PLAN_CODE)
        .fetch_optional(&mut *conn)
        .await?;
    let purchased: i64 = match grant_id {
        Some(grant_id) => {
            sqlx::query_scalar(GRANT_SEAT_TIERS_SUM_SQL).bind(grant_id).fetch_one(&mut *conn).await?
        }
        None => 0,
    };
    let used: i64 = sqlx::query_scalar(USED_SQL).bind(account_id).fetch_one(&mut *conn).await?;
    let reserved: i64 = sqlx::query_scalar(RESERVED_SQL).bind(account_id).fetch_one(&mut *conn).await?;
    Ok(SlotPool { purchased, used, reserved })
}

/// Per-tier sibling of [`GRANT_SEAT_TIERS_SUM_SQL`] — the ledger's own key
/// (`seat_tier_id`), narrowed to one tier. See this module's own "CS-442"
/// doc section for why this is deliberately NOT the effective/pending tier.
const GRANT_SEAT_TIERS_SUM_FOR_TIER_SQL: &str =
    "SELECT COALESCE(SUM(seats), 0)::bigint FROM grant_seat_tiers WHERE grant_id = $1 AND seat_tier_id = $2";

/// Per-tier sibling of [`USED_SQL`] — keyed off the Stripe-EFFECTIVE tier
/// (`pending_seat_tier_id ?? seat_tier_id`, the same rule
/// `billing::seat_lifecycle::MemberSeatState::effective_tier_id` uses), not
/// the plain `seat_tier_id` [`GRANT_SEAT_TIERS_SUM_FOR_TIER_SQL`] reads —
/// see this module's own "CS-442" doc section for the resulting, deliberate
/// ambiguity during a pending-downgrade window.
const USED_FOR_TIER_SQL: &str = "\
    SELECT COUNT(*) FROM account_members \
     WHERE account_id = $1 AND seat_purchased_at IS NOT NULL \
       AND COALESCE(pending_seat_tier_id, seat_tier_id) = $2";

/// Ledger-keyed sibling of [`USED_FOR_TIER_SQL`] — plain `seat_tier_id`,
/// matching [`GRANT_SEAT_TIERS_SUM_FOR_TIER_SQL`]. Capacity gates
/// ([`reserve_slot`], `acquire_seat`'s unreserved consume path) read this
/// so a pending premium→standard member still occupies the premium ledger
/// seat until `roll_period`. [`USED_FOR_TIER_SQL`] stays the `tiers[]`
/// report's `used` only.
const USED_FOR_TIER_LEDGER_KEY_SQL: &str = "\
    SELECT COUNT(*) FROM account_members \
     WHERE account_id = $1 AND seat_purchased_at IS NOT NULL \
       AND seat_tier_id = $2";

/// Per-tier sibling of [`RESERVED_SQL`] — a pre-CS-442 reservation
/// (`seat_tier_id IS NULL`, migration 0241's own NULL semantics) counts
/// against the active `standard` tier, never against "no tier"/every tier.
const RESERVED_FOR_TIER_SQL: &str = "\
    SELECT COUNT(*) FROM slot_reservations \
     WHERE account_id = $1 AND released_at IS NULL AND expires_at > now() \
       AND COALESCE(seat_tier_id, ( \
           SELECT id FROM seat_tiers WHERE code = 'standard' AND is_active = true \
       )) = $2";

/// Snapshot `account_id`'s slot pool for ONE `seat_tiers.id` — the CS-442
/// (item 40, D8) sibling of [`read_slot_pool`], reading the SAME
/// [`CURRENT_TEAM_GRANT_ID_SQL`] grant lookup but summing/counting/
/// reserving narrowed to `seat_tier_id`. See this module's own "CS-442" doc
/// section for the exact per-column rule (`purchased` off the ledger's
/// lagging key, `used` off the Stripe-effective key, `reserved` off the
/// reservation's own tier with a `standard` fallback for NULL legacy rows)
/// and the ambiguity that split deliberately accepts.
///
/// This is the administrative `tiers[]` reader (`routes::org_slots`'s
/// `GET`). Capacity gates ([`reserve_slot`], `acquire_seat`'s unreserved
/// consume path) use [`read_slot_pool_for_tier_ledger`] instead.
///
/// Takes `&mut PgConnection` for the same reason [`read_slot_pool`] does —
/// every call site already holds one, either its own locked transaction or
/// a caller's.
pub async fn read_slot_pool_for_tier(
    conn: &mut PgConnection,
    account_id: Uuid,
    seat_tier_id: Uuid,
) -> Result<SlotPool, sqlx::Error> {
    read_slot_pool_for_tier_used(conn, account_id, seat_tier_id, USED_FOR_TIER_SQL).await
}

/// Capacity-gate sibling of [`read_slot_pool_for_tier`] — `used` is keyed
/// off lagging `seat_tier_id` ([`USED_FOR_TIER_LEDGER_KEY_SQL`]), not the
/// Stripe-effective COALESCE. [`reserve_slot`] and `acquire_seat`'s
/// unreserved consume path read this so a pending-downgrade member still
/// occupies the ledger seat until `roll_period`.
pub async fn read_slot_pool_for_tier_ledger(
    conn: &mut PgConnection,
    account_id: Uuid,
    seat_tier_id: Uuid,
) -> Result<SlotPool, sqlx::Error> {
    read_slot_pool_for_tier_used(conn, account_id, seat_tier_id, USED_FOR_TIER_LEDGER_KEY_SQL).await
}

async fn read_slot_pool_for_tier_used(
    conn: &mut PgConnection,
    account_id: Uuid,
    seat_tier_id: Uuid,
    used_sql: &str,
) -> Result<SlotPool, sqlx::Error> {
    let grant_id: Option<Uuid> = sqlx::query_scalar(CURRENT_TEAM_GRANT_ID_SQL)
        .bind(account_id)
        .bind(crate::billing::MIXED_TIER_PLAN_CODE)
        .fetch_optional(&mut *conn)
        .await?;
    let purchased: i64 = match grant_id {
        Some(grant_id) => {
            sqlx::query_scalar(GRANT_SEAT_TIERS_SUM_FOR_TIER_SQL)
                .bind(grant_id)
                .bind(seat_tier_id)
                .fetch_one(&mut *conn)
                .await?
        }
        None => 0,
    };
    let used: i64 = sqlx::query_scalar(used_sql)
        .bind(account_id)
        .bind(seat_tier_id)
        .fetch_one(&mut *conn)
        .await?;
    let reserved: i64 = sqlx::query_scalar(RESERVED_FOR_TIER_SQL)
        .bind(account_id)
        .bind(seat_tier_id)
        .fetch_one(&mut *conn)
        .await?;
    Ok(SlotPool { purchased, used, reserved })
}

/// A live `slot_reservations.id`.
pub type ReservationId = Uuid;

/// Why a reservation stopped being live — the three-value domain
/// `0233_slot_reservations.sql`'s own `CHECK` enforces.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReleaseReason {
    /// The reservation turned into a real `acquire_seat` stamp —
    /// `billing::seat_lifecycle::acquire_seat`'s consume-before-buy branch.
    Fulfilled,
    /// The caller gave it up before it was fulfilled (e.g. the Clerk call it
    /// was reserved ahead of failed).
    Released,
    /// `workers::slot_reservations`' sweep reclaimed an unfulfilled one past
    /// its `expires_at` — D9's "expiry is load-bearing, not housekeeping".
    Expired,
}

impl ReleaseReason {
    fn as_str(self) -> &'static str {
        match self {
            ReleaseReason::Fulfilled => "fulfilled",
            ReleaseReason::Released => "released",
            ReleaseReason::Expired => "expired",
        }
    }
}

/// Reserve one slot in `account_id`'s pool for `user_id`, valid for `ttl`.
///
/// `Ok(None)` — not an error — when the pool is full
/// (`available() <= 0` at read time, inside the same locked transaction this
/// decision commits under). D9: the caller falls back to approval,
/// **silently to the requester** ("your request has been sent to your
/// admin") — the org's slot count is never disclosed to a non-member. That
/// disclosure discipline is the CALLER's job (not yet built — see this
/// module's own doc comment); this function only ever returns the bare
/// `Option`.
///
/// # Tier (CS-442, item 40, D8)
///
/// `seat_tier_id: Some(id)` reserves against THAT tier's own availability
/// ([`read_slot_pool_for_tier_ledger`]) and stamps the row with it — the
/// shape `routes::org_join_requests::approve` uses, so a `pro` approval can
/// never silently consume a `standard` slot someone else paid for, and a
/// pending premium→standard member still occupies the premium ledger seat.
/// `None` falls back to the account-wide TOTAL pool ([`read_slot_pool`])
/// and writes a `NULL` `seat_tier_id` — kept for a hypothetical future
/// caller with no tier resolved yet (there is none in this PR; every
/// production call site resolves a tier first). See migration `0241`'s own
/// header for what a `NULL` row means on read.
///
/// # Lock discipline (D9, S2)
///
/// Opens `tenancy::scope::begin_account_scoped` then
/// `billing::renewal::lock_period_rotation` FIRST, exactly like
/// `seat_lifecycle::acquire_seat`/`release_seat` — the read
/// ([`read_slot_pool`]/[`read_slot_pool_for_tier_ledger`]) and the write
/// (the `INSERT`) must serialize against every other writer of this
/// account's slot/seat state, or the read could observe room that a
/// concurrent caller is about to claim.
///
/// # Retry-safe for the SAME member (upsert, not insert-or-conflict-error)
///
/// `0233`'s own partial unique index (`account_id, user_id) WHERE
/// released_at IS NULL`) is both the "one live reservation per person per
/// org" invariant and this call's `ON CONFLICT` target: a member who already
/// holds a live reservation and calls this again (a genuine retry — a
/// dropped response, a redelivered event, or an admin re-approving with a
/// DIFFERENT tier before the first attempt's reservation resolved) has both
/// `expires_at` AND `seat_tier_id` refreshed to this call's values, rather
/// than erroring, or being silently counted as a SECOND claim against
/// `available()` (`read_slot_pool`/[`read_slot_pool_for_tier`]'s `reserved`
/// count is `COUNT(*)` over live rows, so a second row for the same member
/// would double-charge the pool for one person).
pub async fn reserve_slot(
    pool: &PgPool,
    account_id: Uuid,
    user_id: Uuid,
    seat_tier_id: Option<Uuid>,
    ttl: chrono::Duration,
) -> Result<Option<ReservationId>, SlotPoolError> {
    let mut tx = crate::tenancy::scope::begin_account_scoped(pool, account_id)
        .await
        .map_err(|e| sqlx::Error::Protocol(e.to_string()))?;
    crate::billing::renewal::lock_period_rotation(&mut tx, account_id).await?;

    let available = match seat_tier_id {
        Some(tier_id) => read_slot_pool_for_tier_ledger(&mut tx, account_id, tier_id).await?.available(),
        None => read_slot_pool(&mut tx, account_id).await?.available(),
    };
    if available <= 0 {
        tx.commit().await?;
        return Ok(None);
    }

    let expires_at = chrono::Utc::now() + ttl;
    let id: Uuid = sqlx::query_scalar(
        "INSERT INTO slot_reservations (account_id, user_id, expires_at, seat_tier_id) VALUES ($1, $2, $3, $4) \
         ON CONFLICT (account_id, user_id) WHERE released_at IS NULL \
         DO UPDATE SET expires_at = EXCLUDED.expires_at, seat_tier_id = EXCLUDED.seat_tier_id \
         RETURNING id",
    )
    .bind(account_id)
    .bind(user_id)
    .bind(expires_at)
    .bind(seat_tier_id)
    .fetch_one(&mut *tx)
    .await?;

    tx.commit().await?;
    Ok(Some(id))
}

/// Resolve a reservation as no-longer-live. Idempotent under redelivery —
/// `WHERE released_at IS NULL` means a second call for an already-resolved
/// row affects 0 rows rather than overwriting `release_reason`.
///
/// A single guarded `UPDATE` by primary key needs no advisory lock: unlike
/// [`reserve_slot`] (a read-then-decide-then-write against the WHOLE pool),
/// this is one row, one atomic conditional flip — the same reasoning
/// `seat_lifecycle::acquire_seat`'s own "stamp UPDATEs are guarded and
/// rows-affected-checked" belt-and-suspenders section gives for why a guard
/// clause substitutes for a lock at exactly this granularity.
///
/// Returns whether this call actually resolved the row (`false` — not an
/// error — for an already-resolved or nonexistent id).
pub async fn release_reservation(
    pool: &PgPool,
    id: ReservationId,
    reason: ReleaseReason,
) -> Result<bool, SlotPoolError> {
    let result = sqlx::query(
        "UPDATE slot_reservations SET released_at = now(), release_reason = $2 \
          WHERE id = $1 AND released_at IS NULL",
    )
    .bind(id)
    .bind(reason.as_str())
    .execute(pool)
    .await?;
    Ok(result.rows_affected() > 0)
}

/// Expire every live, past-due reservation VISIBLE on `conn` — i.e. the
/// caller's own already-open, already-scoped transaction decides which
/// account(s) this touches. `slot_reservations` carries the same
/// `account_isolation` RLS shape every other account-scoped control table in
/// this crate does, and `coralswarm_app` (`rolbypassrls = f`,
/// `scripts/check_rls_coverage.sh`'s own header) hard-errors an unscoped
/// query against it — there is no "sweep every account in one statement"
/// shape available here, only "sweep the one account this connection is
/// scoped to", which is exactly what `workers::slot_reservations::tick`'s
/// own per-account loop supplies (mirroring
/// `workers::billing_derivation::tick`'s identical per-account shape for the
/// identical reason).
///
/// No advisory lock: same reasoning as [`release_reservation`] — the `WHERE
/// released_at IS NULL AND expires_at <= now()` guard is itself the atomicity
/// boundary; a reservation [`reserve_slot`]/[`release_reservation`] is
/// concurrently resolving loses the race to whichever transaction commits
/// first, and the other's guarded `UPDATE` simply affects 0 rows.
///
/// Returns the number of rows this call expired.
pub async fn expire_reservations(conn: &mut PgConnection) -> Result<usize, sqlx::Error> {
    let result = sqlx::query(
        "UPDATE slot_reservations SET released_at = now(), release_reason = 'expired' \
          WHERE released_at IS NULL AND expires_at <= now()",
    )
    .execute(&mut *conn)
    .await?;
    Ok(result.rows_affected() as usize)
}
