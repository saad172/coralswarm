-- Migration: 0168_grant_seat_tiers
-- Description: billing self-serve launch PR B (4/5) — `grant_seat_tiers`, the
--   per-grant breakdown of HOW MANY seats at WHICH tier (plan §1.4: "Mixed
--   seats per org... `grant_seat_tiers (grant_id, seat_tier_id, seats)` holds
--   per-tier counts. Entitlement total = Σ(seats × tier.credits)"). This is
--   the table `0167`'s header names as the thing a `team` grant's real
--   entitlement must be computed FROM, in place of `plans.credits`.
--
--   ── Why a satellite table rather than columns on `grants` ──
--   `grants` (0150) already has a scalar `seats INT` (0156) — correct for
--   the single-tier case every OTHER seat-priced plan this schema has ever
--   had (`team_standard`, `team_premium`, both one price point). A MIXED
--   grant needs an unbounded number of (tier, count) pairs, which is a
--   one-to-many relationship no fixed set of columns on `grants` can express
--   without either a hard cap on tier count (arbitrary, and wrong the moment
--   a third tier is registered) or a JSON blob (loses FK integrity to
--   `seat_tiers`, loses CHECK-constraint expressiveness, and this tree does
--   not do that — every other multi-valued billing fact here, `credit_ledger`
--   entries, `member_allowance_periods` rows, is a real table). A satellite
--   table with `(grant_id, seat_tier_id)` as its key is the standard
--   normalization for exactly this shape, and costs nothing a `team` grant
--   was not already going to pay for (a handful of rows per grant, not per
--   member).
--
--   `grants.seats` itself is UNTOUCHED by this migration and keeps its
--   existing meaning for every OTHER seat-priced plan. For a `team` grant
--   specifically, PR E (not schema work) decides what `grants.seats` should
--   hold going forward — plausibly the sum of `grant_seat_tiers.seats` for
--   symmetry with the pre-existing `grants_seats_iff_seat_priced` CHECK, but
--   that is a RENEWAL-LOGIC decision (what gets written, and when) that this
--   schema-only PR does not make. Restated because leaving it unstated would
--   read as an oversight rather than a deliberate scope boundary — see
--   0167's header for the identical boundary on `plans.credits`.
--
--   ── RLS: a one-hop subquery policy, NOT a denormalized `account_id`
--   ── (review-round fix — an earlier version of this migration denormalized
--   ── `grants.account_id` onto this table on the claim that "a `USING`
--   ── policy CANNOT join to another table"; that claim is false and is
--   ── retracted here) ──
--   `grant_seat_tiers` has no `account_id` column of its own — only
--   `grant_id`, one join away from it on `grants`. `0083_consumer_rls.sql`
--   (the migration that established this schema's RLS conventions) already
--   ships this exact shape more than twenty times over for tables reached
--   only through a foreign key, not a direct `account_id` column — its own
--   header states the predicate verbatim for that case:
--
--       graph_id IN (SELECT id FROM graphs
--                    WHERE account_id = current_setting('app.account_id')::uuid)
--
--   `grant_seat_tiers`'s policy below is the identical construction, walked
--   one hop through `grants` instead of `graphs`. `grants` is itself
--   RLS'd under the same predicate (`0150`), so the subquery is filtered
--   consistently rather than opening a fresh, unfiltered read path — the
--   same non-issue 0083's header already addresses for its own nested
--   subqueries. No denormalized column, no `UNIQUE (id, account_id)` on
--   `grants`, no composite mirror FK, and no extra index are needed for
--   this — all four existed in an earlier version of this migration purely
--   to support the (incorrect) belief that a join was unavailable, and are
--   removed here.
--
--   ── Why `PRIMARY KEY (grant_id, seat_tier_id)` rather than a surrogate id
--   ── ──
--   One row per (grant, tier) is the actual invariant this table exists to
--   hold — a grant cannot sensibly record "12 standard seats" twice. A
--   composite natural key enforces that directly, the same choice
--   `stripe_webhook_events` made for `event_id` alone (0162's header: "the id
--   IS the identity") rather than adding a surrogate `id` that would let two
--   rows silently duplicate the same (grant, tier) pair with different seat
--   counts and no constraint to say which one is right.
--
--   ── `seats >= 1`, no upper bound, no minimum-per-tier ──
--   Plan §1.5 (no seat minimum, any count from 1) applies per TIER the same
--   way it applies to a single-tier grant — a `team` grant with 1 standard
--   seat and 0 premium seats is just "1 standard seat, no premium row at
--   all" (a tier with zero seats simply has no row here, rather than a row
--   with `seats = 0` — keeping "how many tiers does this grant touch" a
--   direct `COUNT(*)` rather than requiring a `WHERE seats > 0` filter
--   everywhere this table is read).
--
-- CI notes (same discipline 0161/0162/0164 each state explicitly):
--   * CONTROL-PLANE ONLY, no tenant twin — added to `scripts/
--     check_schema_parity.sh`'s `CONTROL_TABLES` roster in this same PR,
--     grouped with 0165's `seat_tiers` and 0166's `credit_packs`.
--   * RLS-enabled — added to `scripts/check_rls_coverage.sh`'s
--     `EXTRA_RLS_TABLES` supplement (this policy is added here, not in
--     0083_consumer_rls.sql, so it needs the same manual roster entry
--     `credit_ledger`/`credit_balances`/`grants`/`enforcement_events`
--     already have) and to `backend/tests/consumer_rls_pg.rs`'s `RLS_TABLES`
--     roster, both in this same PR.
--
-- Depends on: 0150_credit_ledger.sql (`grants`, its existing `account_id`
--   column and `account_isolation` policy this table's own policy joins
--   through), 0083_consumer_rls.sql (the one-hop subquery policy shape this
--   migration's `account_isolation` policy copies verbatim), 0165_seat_tiers
--   .sql (`seat_tiers`, the FK target for `seat_tier_id`).
-- Down:
--   DROP TABLE IF EXISTS grant_seat_tiers;

SET LOCAL lock_timeout = '3s';

-- ── grant_seat_tiers ──────────────────────────────────────────────────────
-- `seats`'s CHECK is given an explicit name (CodeRabbit round 1, finding 6)
-- rather than the Postgres-generated default so a test can assert on the
-- constraint by name (`grant_seat_tiers_seats_check`) instead of a
-- generated one it would have to keep resynced with this column's ordinal
-- position.
CREATE TABLE IF NOT EXISTS grant_seat_tiers (
    grant_id      UUID    NOT NULL REFERENCES grants(id) ON DELETE CASCADE,
    seat_tier_id  UUID    NOT NULL REFERENCES seat_tiers(id),
    seats         INT     NOT NULL CONSTRAINT grant_seat_tiers_seats_check CHECK (seats >= 1),

    PRIMARY KEY (grant_id, seat_tier_id)
);

ALTER TABLE grant_seat_tiers ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS account_isolation ON grant_seat_tiers;
-- One-hop subquery back to `grants` (itself RLS'd under the identical
-- predicate) — the exact shape `0083_consumer_rls.sql`'s header documents
-- for every table reached only through a foreign key, copied verbatim. See
-- this migration's own header for why no denormalized `account_id` column
-- is needed to express this.
CREATE POLICY account_isolation ON grant_seat_tiers
    USING (grant_id IN (SELECT id FROM grants
                         WHERE account_id = current_setting('app.account_id')::uuid));
