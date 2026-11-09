-- Migration: 0163_member_allowance_periods_no_overlap
-- Description: Billing Phase 4b — makes an overlapping pair of allowance
--   periods for the same (account, member) UNREPRESENTABLE, closing a
--   CodeRabbit Major on PR #639 (`billing-phase4b-seat-changes`).
--
--   `0161`'s UNIQUE index is `(account_id, user_id, period_start)`. That
--   stops the exact-redelivery case — same `period_start` twice — but does
--   NOTHING to stop two rows with DIFFERENT `period_start` values whose
--   `[period_start, period_end)` windows overlap. Two commits already ship
--   in `2900859b` that make an overlap TOLERABLE if one exists —
--   `billing_derivation::advance_member_consumption` pins its UPDATE to
--   `max(period_start)`, and `enforcement::read_member_period_scoped` reads
--   `ORDER BY period_start DESC LIMIT 1` — but both are read/write PINS, not
--   a fix. They agree on which row is authoritative only because they were
--   independently written to agree; nothing stops a THIRD call site from
--   reading the wrong one, and nothing stops the overlap from existing at
--   all. This migration is the fix: it makes the overlap impossible to
--   insert in the first place.
--
-- WHY AN EXCLUSION CONSTRAINT RATHER THAN A WIDER UNIQUE INDEX
--   A UNIQUE index (composite or otherwise) can only compare whole rows for
--   EQUALITY of the indexed columns — it has no way to express "these two
--   ranges overlap" for two rows whose `period_start` differ, which is
--   exactly the shape of the bug. Range overlap is an inequality-with-itself
--   relationship (`&&`), not an equality one, so it needs `gist`'s indexed
--   inequality operators, which is what `EXCLUDE USING gist` gives: for the
--   same `(account_id, user_id)`, any two rows whose `tstzrange(period_start,
--   period_end)` ranges satisfy `&&` are refused by the constraint itself, at
--   INSERT time, under the same MVCC/locking guarantees a UNIQUE index gets.
--   `btree_gist` is required because `account_id`/`user_id` need their usual
--   equality operator class inside a `gist` index, which core Postgres does
--   not ship; it is available and trusted on this instance (v1.7, verified
--   against production before this migration was written) and is not
--   superuser-only under `CREATE EXTENSION`.
--
-- WHY `[)` HALF-OPEN IS CORRECT HERE
--   `tstzrange(a, b)` with the default bound flags is `[)` — inclusive of
--   `period_start`, exclusive of `period_end`. That is precisely the
--   semantics `enforcement::read_member_period_scoped` already assumes
--   (`period_start <= now() AND period_end > now()`), and it is what makes a
--   period ending exactly when the next one begins NOT an overlap: at the
--   instant `t = old.period_end = new.period_start`, `t` falls inside the new
--   range and outside the old one, so `&&` is false and both rows coexist.
--   `renewal::pg_tests::the_next_period_grants_again` already depends on this
--   — it rolls a second period with `next_start = end` and expects it to
--   succeed — so an inclusive-inclusive (`[]`) range here would turn that
--   test into a constraint violation on every ordinary period rollover, which
--   is the normal case, not an edge case.
--
-- WHY THIS IS SAFE TO ADD WITHOUT A REPAIR STEP
--   Verified directly against PRODUCTION before writing this file: the
--   control-plane `member_allowance_periods` table holds exactly 2 rows
--   total, and 0 overlapping pairs among them. `CREATE EXTENSION btree_gist`
--   plus this exact constraint were dry-run inside a transaction against
--   live production and rolled back; both statements succeeded with no
--   error, which is only possible because there is nothing today for the
--   constraint to reject.
--
-- WHAT THIS DOES NOT DO
--   The constraint only makes an overlap impossible to INSERT; it does
--   nothing to CLOSE an already-open period before a new one starts. That is
--   `billing::renewal::close_open_period`'s job — without it, `roll_period`
--   seeding a new period over an old one still spanning `now()` would now
--   hard-FAIL with a constraint violation instead of silently double-
--   inserting, which is strictly better (loud beats silent) but is not by
--   itself a fix for the caller. See that function's doc comment.
--
-- Depends on: 0161_member_allowance_periods.sql (the table and its
--   `period_end > period_start` CHECK, which this constraint relies on to
--   keep every range non-empty).
-- Down:
--   ALTER TABLE member_allowance_periods
--       DROP CONSTRAINT IF EXISTS member_allowance_periods_no_overlap;
--   -- (btree_gist is left installed; dropping it is destructive to any other
--   --  exclusion constraint that may come to depend on it, and is
--   --  intentionally not reversed here — same posture as 0046's pg_trgm)

SET LOCAL lock_timeout = '3s';

CREATE EXTENSION IF NOT EXISTS btree_gist;

ALTER TABLE member_allowance_periods
    ADD CONSTRAINT member_allowance_periods_no_overlap
    EXCLUDE USING gist (
        account_id WITH =,
        user_id WITH =,
        tstzrange(period_start, period_end) WITH &&
    );
