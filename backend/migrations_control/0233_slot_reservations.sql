-- Migration: 0233_slot_reservations
-- Description: CS-435 (org-onboarding plan §3 items 7/15,
--   docs/plans/org-onboarding-from-the-invite.md) — the reservation table
--   D9 requires, plus its expiry sweep's own footprint (`released_at`/
--   `release_reason`).
--
--   ── WHAT ──
--   One row per in-flight claim on the org's slot pool: `reserve_slot`
--   (`billing::slot_pool`) inserts a live row (`released_at IS NULL`) under
--   the account's advisory lock, ONLY when the pool has room; the row is
--   later flipped to `released_at = now()` with a `release_reason` of
--   `fulfilled` (the reservation turned into a real `acquire_seat` stamp),
--   `released` (the caller gave it up — e.g. the Clerk call that would have
--   used it failed), or `expired` (the sweep in
--   `workers::slot_reservations` reclaimed an unfulfilled one). A row is
--   NEVER deleted — durable history of who tried to claim a slot and how it
--   resolved, same "never delete, only stamp" posture `org_join_requests`
--   (D10) and every other decision-audit table in this plan takes.
--
--   ── WHY A TABLE, NOT A CHECK (D9) ──
--   `docs/plans/org-onboarding-from-the-invite.md` D9: `acquire_seat`'s own
--   caller contract requires the `account_members` row to already be
--   COMMITTED before it runs, so "the pool was full" can no longer fall back
--   to approval once that call is reached — the loser would become a
--   committed org member holding no purchased slot. A reservation taken
--   BEFORE the Clerk call (D3 step 1, not built in this PR) spans exactly
--   the gap `acquire_seat`'s own lock cannot: two network calls to Clerk sit
--   between "the pool looked available" and "the membership row exists",
--   and no single lock can span a gap that contains someone else's HTTP
--   round trip. See D9's own "What the race actually is" for the double-
--   spend this closes: two callers reading `available() > 0` with one slot
--   left, both proceeding, both later stamping `seat_purchased_at` — the
--   exact auto-spend D1 exists to prevent, re-entering through the back
--   door. `expires_at` is D9's "expiry is load-bearing, not housekeeping":
--   a reservation whose Clerk call never returns must expire on its own, or
--   a crashed approval permanently shrinks the pool.
--
--   ── D7: NOT A THIRD NUMBER ──
--   `billing::seat_lifecycle`'s own module doc (quoted in D7) already fights
--   drift between two numbers (Stripe's quantity and this ledger); D7's own
--   text is explicit that a reservation does not reintroduce that problem:
--   "a reservation is short-lived, expires on its own, and mirrors nothing
--   in Stripe." Nothing here is ever synced to Stripe or to
--   `grant_seat_tiers` — `purchased`/`used`/`reserved` are computed fresh on
--   every read (`billing::slot_pool::read_slot_pool`), never cached.
--
--   ── D15: `slot`, not `seat` ──
--   Named `slot_reservations`, not `seat_reservations` (the working name in
--   the design doc, written before the founder's 2026-09-21 naming
--   decision) — `seat` stays reserved for team-ocean access
--   (`crate::seats`/`team_seat_grants`/`team_members`), and this is billing:
--   a claim on `account_members.seat_purchased_at` becoming true. See D15's
--   own text for why the collision is not cosmetic (both fire when a member
--   is approved, for two different reasons).
--
--   ── PARTIAL INDEXES ──
--   `slot_reservations_live_account_idx (account_id) WHERE released_at IS
--   NULL` backs `read_slot_pool`'s `reserved` count (`WHERE account_id = $1
--   AND released_at IS NULL AND expires_at > now()`) — a live-rows-only
--   index, since a resolved reservation is never read again except for
--   audit/history (an unindexed, rare path). `slot_reservations_live_
--   expiry_idx (expires_at) WHERE released_at IS NULL` backs the sweep's own
--   `WHERE released_at IS NULL AND expires_at <= now()` — not explicitly
--   named in the plan's item 15, added here because the sweep runs on a
--   default 60s cadence (`SLOT_RESERVATION_SWEEP_SECS`) and an unindexed
--   scan of every live reservation on every tick is the wrong default for a
--   table this hot.
--
--   `slot_reservations_live_member_idx UNIQUE (account_id, user_id) WHERE
--   released_at IS NULL` is BOTH the "one live reservation per person per
--   org" invariant the plan's item 1 asks for AND the `ON CONFLICT` target
--   `billing::slot_pool::reserve_slot` upserts against — a retried reserve
--   call for a member who already holds a live reservation refreshes its
--   `expires_at` instead of erroring or double-counting them against
--   `available()`.
--
--   ── `release_reason` CHECK ──
--   `released_at IS NULL) = (release_reason IS NULL)`: the two columns are
--   stamped together, always — a live row carries neither, a resolved row
--   carries both. Prevents the half-stamped state ("released_at set, no
--   reason recorded") that would make the audit trail unreadable.
--
--   ── RLS: mirrors 0227 (slack_unmatched_email_notices) ──
--   Account-level operational state with no `graph_id`/ocean concept, same
--   shape as that migration's own header describes: a bare account-scoped
--   `account_isolation` policy covering every command, not a graph-anchored
--   `member_ocean_isolation` one. Control-only, no tenant-tree twin — a
--   claim on the account's BILLING pool has no coherent per-tenant meaning
--   (same "control-only" posture 0227's header argues for its own table).
--   NOT on `scripts/check_rls_coverage.sh`'s 0083-derived roster (that
--   script only reads `backend/migrations_control/0083_consumer_rls.sql`'s
--   own `CREATE POLICY` lines) — same as `grants`/`grant_seat_tiers`/
--   `member_allowance_periods`, whose manifest entries already note this;
--   every reader/writer here still opens `tenancy::scope::
--   begin_account_scoped` regardless, since `coralswarm_app` (`rolbypassrls
--   = f`) hard-errors an unscoped query against ANY `account_isolation`-
--   shaped policy, on or off that one script's roster.
--
--   Added to `CONTROL_TABLES` in `scripts/check_schema_parity.sh` in this
--   same PR (0227's own precedent).
--
-- Depends on: 0001_init (accounts, users), 0083_consumer_rls (the
--             account_isolation-shaped policy convention),
--             0227_slack_unmatched_email_notices (the direct-account_id,
--             control-only precedent this migration follows).
-- Down:
--   DROP POLICY IF EXISTS account_isolation ON slot_reservations;
--   DROP TABLE IF EXISTS slot_reservations;

CREATE TABLE IF NOT EXISTS slot_reservations (
    id              UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    account_id      UUID        NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    user_id         UUID        NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at      TIMESTAMPTZ NOT NULL,
    released_at     TIMESTAMPTZ NULL,
    release_reason  TEXT        NULL,

    CHECK (release_reason IS NULL OR release_reason IN ('fulfilled', 'released', 'expired')),
    CHECK ((released_at IS NULL) = (release_reason IS NULL))
);

-- Live-rows-only lookup — backs `read_slot_pool`'s `reserved` count.
CREATE INDEX IF NOT EXISTS slot_reservations_live_account_idx
    ON slot_reservations (account_id)
    WHERE released_at IS NULL;

-- Live-rows-only expiry scan — backs the sweep's own cutoff query.
CREATE INDEX IF NOT EXISTS slot_reservations_live_expiry_idx
    ON slot_reservations (expires_at)
    WHERE released_at IS NULL;

-- One live reservation per person per org — also the `ON CONFLICT` target
-- `reserve_slot` upserts against on a retry.
CREATE UNIQUE INDEX IF NOT EXISTS slot_reservations_live_member_idx
    ON slot_reservations (account_id, user_id)
    WHERE released_at IS NULL;

ALTER TABLE slot_reservations ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS account_isolation ON slot_reservations;
CREATE POLICY account_isolation ON slot_reservations
    USING (account_id = current_setting('app.account_id')::uuid)
    WITH CHECK (account_id = current_setting('app.account_id')::uuid);
