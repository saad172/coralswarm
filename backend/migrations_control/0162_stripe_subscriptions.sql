-- Migration: 0162_stripe_subscriptions
-- Description: Billing Phase 4a — the Stripe linkage and the webhook's
--   idempotency ledger. `0161` built `member_allowance_periods` but NOTHING
--   creates rows in it, so every `is_blocked` call still falls through to the
--   account-level pool check and per-seat limits are inert. This migration is
--   the half of the fix that lives in the schema; the renewal itself is
--   `billing::renewal::roll_period`, driven by `invoice.paid`.
--
-- WHY A SEPARATE `stripe_subscriptions` TABLE RATHER THAN COLUMNS ON `accounts`
--   `accounts` is the oldest table in the schema (`0001_init`) and is read on
--   essentially every request path. Stripe linkage is written by exactly one
--   caller (the webhook) and read by two, so widening the hottest table in the
--   database to carry it would put third-party billing state in the way of
--   every unrelated query. A satellite table also lets `stripe_customer_id`
--   carry its own UNIQUE constraint — see below, that constraint is load-bearing
--   rather than cosmetic.
--
-- WHY `account_id` IS THE PRIMARY KEY
--   One subscription per account, enforced by the schema rather than by
--   convention. The founder's model is a Stripe Customer per ORG (one billing
--   relationship per account), and a second concurrent subscription for one
--   account has no defined meaning: two `invoice.paid` events would each try to
--   roll the same members' allowances for overlapping periods, and the UNIQUE
--   index on `member_allowance_periods` would silently swallow the second as a
--   no-op. That is exactly the kind of silent-wrong-number failure this build
--   has been bitten by twice, so it is made unrepresentable here instead.
--
-- WHY `stripe_customer_id` IS UNIQUE
--   It is the ONLY join key the webhook has. A Stripe event arrives carrying a
--   customer id and nothing else that identifies us, and
--   `resolve_stripe_customer_account_id` (below) turns it into an account id
--   before any scoped transaction can be opened. If two accounts could share a
--   customer id that resolver would pick one arbitrarily with `LIMIT 1` and
--   credit the WRONG ACCOUNT's members — a billing error invisible to everyone
--   until a customer complained. UNIQUE makes the resolver's `LIMIT 1`
--   provably a formality rather than a tiebreak.
--
-- WHY `status` IS A STATE COLUMN AND NOT A DELETE
--   Phase 4b implements the founder's two lapse outcomes, which are
--   deliberately NOT the same: an involuntary lapse (`past_due`, usually an
--   expired card) FREEZES and is restorable, while a deliberate `canceled`
--   FORFEITS the purchased pool. Both are modelled as state transitions with
--   the row retained, so the audit trail survives the event. Destroying the
--   row on cancel would make "why did this customer lose their credits?"
--   unanswerable after the fact.
--
--   The three values are the only ones this system acts on. Stripe has more
--   (`trialing`, `incomplete`, `unpaid`, ...); the webhook maps everything it
--   does not act on onto the nearest of these three rather than widening the
--   CHECK, so an unrecognized Stripe status can never be written as itself and
--   then silently read as if it meant something.
--
-- WHY `current_period_*` IS STORED HERE **AND** ON `member_allowance_periods`
--   These two are not duplicates and must not be reconciled into one. This
--   column pair is a MIRROR of what Stripe last told us the billing period is.
--   `member_allowance_periods.period_start/period_end` is what we actually
--   GRANTED. They agree in the normal case, and when they disagree that
--   disagreement is the signal — a renewal that failed to run leaves Stripe's
--   period ahead of the granted one, which is detectable precisely because the
--   two are recorded independently. Collapsing them would delete the evidence.
--
-- WHY `stripe_webhook_events` EXISTS
--   Stripe redelivers. Its own guidance is that a webhook endpoint MUST be
--   idempotent, because at-least-once delivery is the contract — a retry after
--   a timeout, a manual resend from the dashboard, or a network blip all
--   produce the same `event_id` twice. `member_allowance_periods`'s UNIQUE
--   index already makes the GRANT itself idempotent, but that is not enough on
--   its own: it protects one specific write, whereas this table makes the
--   whole handler exactly-once, including the writes Phase 4b adds (a status
--   transition and a forfeiture ledger entry, NEITHER of which is naturally
--   idempotent — applying a forfeiture twice would double-debit).
--
--   The claim and the work happen in ONE transaction: insert the event row
--   `ON CONFLICT DO NOTHING`, and if zero rows were inserted this is a replay,
--   so roll back and answer 200 without doing the work again.
--
-- WHY `stripe_webhook_events.account_id` IS **NOT NULL**
--   The obvious shape is to record every event on arrival and resolve the
--   account afterwards, which forces the column nullable. That does not work
--   here: under OPS-4's `coralswarm_app` role a policy's `USING` clause is
--   also its `WITH CHECK` when no separate one is given, so a row with a NULL
--   `account_id` could not be INSERTED at all — it would fail the policy
--   rather than be recorded. The handler therefore resolves the account FIRST
--   (via the SECURITY DEFINER resolver below, which needs no scope), and an
--   event for a customer we do not know is logged and answered 200 without a
--   row. That is the correct outcome anyway: an event we cannot attribute is
--   not one we can process, and answering non-200 would make Stripe retry it
--   forever.
--
-- CONTROL-PLANE ONLY: both tables join their billing siblings
--   (`credit_ledger`, `credit_balances`, `plans`, `grants`,
--   `enforcement_events`, `member_allowance_periods`) in
--   `scripts/check_schema_parity.sh`'s `CONTROL_TABLES` roster, so there is NO
--   tenant twin. Both are also RLS-enabled, so both must additionally be added
--   to `backend/tests/consumer_rls_pg.rs`'s `RLS_TABLES` roster — that test
--   asserts the control tree's policy set as an exact set, and NO gate knows
--   about it (0161 failed it for precisely this reason).
--
-- Depends on: 0150_credit_ledger.sql (accounts FK posture, RLS convention),
--   0161_member_allowance_periods.sql (the table the renewal fills),
--   0086_webhook_share_bootstrap_functions.sql (the SECURITY DEFINER
--   webhook-bootstrap resolver contract this migration's function follows).
-- Down:
--   REVOKE EXECUTE ON FUNCTION resolve_stripe_customer_account_id(text) FROM coralswarm_app;
--   DROP FUNCTION IF EXISTS resolve_stripe_customer_account_id(text);
--   DROP TABLE IF EXISTS stripe_webhook_events;
--   DROP TABLE IF EXISTS stripe_subscriptions;

SET LOCAL lock_timeout = '3s';

-- ── stripe_subscriptions ─────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS stripe_subscriptions (
    account_id             UUID        PRIMARY KEY REFERENCES accounts(id) ON DELETE CASCADE,
    stripe_customer_id     TEXT        NOT NULL UNIQUE CHECK (stripe_customer_id <> ''),
    -- NULL only in the window between a customer existing and its first
    -- subscription. UNIQUE for the same reason `stripe_customer_id` is: it is
    -- the join key for `customer.subscription.*` events in Phase 4b.
    stripe_subscription_id TEXT        UNIQUE
        CHECK (stripe_subscription_id IS NULL OR stripe_subscription_id <> ''),
    status                 TEXT        NOT NULL DEFAULT 'active'
        CHECK (status IN ('active', 'past_due', 'canceled')),
    -- Stripe's mirror, NOT the enforcement boundary — see the header. Nullable
    -- because a customer can exist before any invoice has been paid.
    current_period_start   TIMESTAMPTZ,
    current_period_end     TIMESTAMPTZ,
    created_at             TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at             TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- Same shape as `member_allowance_period_end_after_start` on 0161. Both
    -- bounds are set together by the webhook or neither is.
    CONSTRAINT stripe_subscription_period_bounds CHECK (
        (current_period_start IS NULL AND current_period_end IS NULL) OR
        (current_period_start IS NOT NULL AND current_period_end IS NOT NULL
         AND current_period_end > current_period_start)
    )
);

ALTER TABLE stripe_subscriptions ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS account_isolation ON stripe_subscriptions;
CREATE POLICY account_isolation ON stripe_subscriptions
    USING (account_id = current_setting('app.account_id')::uuid);

-- ── stripe_webhook_events ────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS stripe_webhook_events (
    -- Stripe's own `evt_...` id. PRIMARY KEY rather than a surrogate with a
    -- UNIQUE index on top: the id IS the identity, and the insert that claims
    -- it is the idempotency check.
    event_id     TEXT        PRIMARY KEY CHECK (event_id <> ''),
    event_type   TEXT        NOT NULL CHECK (event_type <> ''),
    account_id   UUID        NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    received_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- A short machine-greppable summary of what the handler did, written in
    -- the same transaction as the work itself. Deliberately free-form TEXT
    -- rather than an enum: it carries counts ("renewed rows=4 members=4
    -- seats=4"), and a new event type must be able to describe its own outcome
    -- without a migration.
    outcome      TEXT
);

-- "What has this account's billing done lately", for support and for the
-- reconciliation sweep. The PRIMARY KEY leads with `event_id`, so it cannot
-- serve an account-ordered scan.
CREATE INDEX IF NOT EXISTS idx_stripe_webhook_events_account_received
    ON stripe_webhook_events (account_id, received_at DESC);

ALTER TABLE stripe_webhook_events ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS account_isolation ON stripe_webhook_events;
CREATE POLICY account_isolation ON stripe_webhook_events
    USING (account_id = current_setting('app.account_id')::uuid);

-- ── stripe_customer_id -> account_id (webhook bootstrap) ─────────────────────
-- Follows 0086's contract exactly: a narrow SECURITY DEFINER wrapper answering
-- exactly one "which account owns this row?" question for a call site that must
-- DISCOVER the owning account BEFORE it can open `begin_account_scoped` at all.
-- Returns ONLY a bare account_id — never `stripe_subscription_id`, never
-- `status`, never a period bound — so it cannot be used to probe billing state,
-- only to bootstrap a scope decision. Like 0086's functions it must NOT itself
-- be wrapped in `begin_account_scoped`.
--
-- `LIMIT 1` is a formality, not a tiebreak: `stripe_customer_id` is UNIQUE, so
-- at most one row can ever match. It is kept for exact parity with 0086's
-- resolvers rather than inviting a reader to work out whether this one differs.
CREATE OR REPLACE FUNCTION resolve_stripe_customer_account_id(p_customer_id text)
RETURNS uuid
LANGUAGE sql
SECURITY DEFINER
STABLE
SET search_path = public, pg_temp
AS $$
    SELECT account_id
    FROM stripe_subscriptions
    WHERE stripe_customer_id = p_customer_id
    LIMIT 1
$$;

-- `, CURRENT_USER` matches 0086's grants exactly: the migration runner and the
-- application connect as different roles, and 0086 learned to name both.
GRANT EXECUTE ON FUNCTION resolve_stripe_customer_account_id(text) TO coralswarm_app, CURRENT_USER;
