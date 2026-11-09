-- Migration: 0161_member_allowance_periods
-- Description: Billing Phase 2b (seats + overage) — split the credit buckets.
--   Until now an account had exactly ONE number, `credit_balances.balance`,
--   and it only ever went up (`bin/grant_plan.rs`'s additive upsert). That
--   cannot express the model the founder settled on:
--
--     member exhausts their per-seat allowance
--       -> draws the org's purchased overage pool
--       -> BLOCKED at their cap, even if the pool has more
--       -> admin raises that member's cap, or buys more
--     period rolls -> allowances reset; purchased overage carries over
--
--   This table is the per-member, per-period allowance. `credit_balances`
--   keeps its meaning as the ORG-level pool that does NOT reset.
--
-- WHY PERIOD-SCOPED ROWS RATHER THAN A MUTABLE "remaining" COUNTER
--   The renewal clock is Stripe's billing period, and a webhook can redeliver
--   `invoice.paid` — so the reset must be safe to run twice. One row per
--   (account, member, period_start) with a UNIQUE index makes the grant an
--   `INSERT ... ON CONFLICT DO NOTHING`: idempotent by construction, with no
--   read-modify-write race between a webhook and a worker, and a full history
--   of what each member was granted in each period. A single mutable
--   "remaining" column would have to be reset by an UPDATE, which is neither
--   idempotent under redelivery nor auditable afterwards.
--
-- WHY `consumed_credits` IS STILL A MUTABLE COLUMN HERE
--   `granted_credits` is write-once; `consumed_credits` is a running total
--   maintained by `workers::billing_derivation` as it writes debits, always
--   via `consumed_credits = consumed_credits + $delta` (atomic under the row
--   lock, so concurrent ticks cannot lose an update). It is a CACHE, exactly
--   as `credit_balances` is a cache over `credit_ledger`: the authoritative
--   value is re-derivable at any time from
--   `SUM(credit_ledger.credits) WHERE account_id/attributed_user_id and
--   created_at inside the period` — which is only possible because migration
--   `0159` put `attributed_user_id` on `credit_ledger`.
--
--   The cache exists so `billing::enforcement::is_blocked` stays a small
--   indexed lookup. It runs on the ask/search request path, where it is
--   TODAY a single primary-key read; making it a SUM over the ledger per
--   request would put an aggregate in front of every query.
--
-- WHY `cap_credits` IS NULLABLE RATHER THAN DEFAULTED
--   The founder's default cap is "1x the seat allowance", i.e. exactly
--   `granted_credits`. Storing that as a literal default would freeze the
--   policy into every historical row and make a later policy change invisible
--   to old periods. NULL means "use the policy default" and is resolved in
--   code as `COALESCE(cap_credits, granted_credits)`; a non-NULL value is an
--   admin having deliberately raised (or lowered) ONE member's cap for ONE
--   period, which is the documented escape hatch when a member is blocked.
--
-- WHY NO FK ON `user_id`
--   Same posture the rest of the billing tables already take (`0150`'s
--   `credit_ledger` has no FK to anything but `accounts`, and `0159`'s
--   `attributed_user_id` deliberately has none): a member row can be removed
--   while their spend history must survive, and a removed member's unused
--   allowance is explicitly neither refunded nor redistributed. `account_id`
--   keeps its FK + ON DELETE CASCADE, matching every sibling table — deleting
--   an account erases its billing rows, a decision `0150`'s header already
--   records.
--
-- CONTROL-PLANE ONLY: `member_allowance_periods` joins its four siblings
--   (`credit_ledger`, `credit_balances`, `plans`, `grants`,
--   `enforcement_events`) in `scripts/check_schema_parity.sh`'s
--   `CONTROL_TABLES` roster, so there is NO tenant twin. Register it there in
--   this same PR or the parity gate will treat it as a shared table that the
--   tenant tree is missing.
--
-- Depends on: 0150_credit_ledger.sql (accounts FK posture, RLS convention),
--   0159_credit_ledger_attributed_user_id.sql (makes consumption re-derivable).
-- Down:
--   DROP TABLE IF EXISTS member_allowance_periods;

SET LOCAL lock_timeout = '3s';

CREATE TABLE IF NOT EXISTS member_allowance_periods (
    id               UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    account_id       UUID        NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    user_id          UUID        NOT NULL,
    period_start     TIMESTAMPTZ NOT NULL,
    period_end       TIMESTAMPTZ NOT NULL,
    granted_credits  NUMERIC     NOT NULL CHECK (granted_credits >= 0),
    consumed_credits NUMERIC     NOT NULL DEFAULT 0 CHECK (consumed_credits >= 0),
    cap_credits      NUMERIC     CHECK (cap_credits IS NULL OR cap_credits >= 0),
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT member_allowance_period_end_after_start
        CHECK (period_end > period_start)
);

-- The idempotency key for a period roll: a redelivered `invoice.paid` becomes
-- an `INSERT ... ON CONFLICT DO NOTHING` no-op rather than a second grant.
CREATE UNIQUE INDEX IF NOT EXISTS idx_member_allowance_periods_unique
    ON member_allowance_periods (account_id, user_id, period_start);

-- Serves `is_blocked`'s "the member's CURRENT period" lookup —
-- `WHERE account_id = $1 AND user_id = $2 AND period_start <= now()
--    AND period_end > now()`. The unique index above already leads with
-- (account_id, user_id), so this exists for the account-wide per-member
-- rollup ("what has every seat consumed this period") that the usage screen
-- and a future seat-management UI both need.
CREATE INDEX IF NOT EXISTS idx_member_allowance_periods_account_period
    ON member_allowance_periods (account_id, period_start DESC);

ALTER TABLE member_allowance_periods ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS account_isolation ON member_allowance_periods;
CREATE POLICY account_isolation ON member_allowance_periods
    USING (account_id = current_setting('app.account_id')::uuid);
