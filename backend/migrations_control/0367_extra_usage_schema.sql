-- Migration: 0367_extra_usage_schema
-- Description: Settings launch plan PR 3.1 (decisions 11-13). Schema only: the
--   tables extra usage needs, plus Stripe cancel-at-period-end mirroring.
--   No Rust reads or writes any of this yet; 3.2 (FundingPolicy), 3.4
--   (top-up), 3.5 (auto-reload) and 3.6 (notices) build on it.
--
--   ── WHY A SEPARATE LEDGER ──
--   `credit_balances.balance` cannot hold the extra-usage balance. Every
--   debit subtracts from it, so credits bought after the plan allowance is
--   spent could never be spent: the balance would already be negative or
--   zero-sum against the allowance. Extra usage is a prepaid dollar balance
--   spent only AFTER the allowance, so it has its own ledger and balance.
--   `credit_ledger`/`credit_balances` are untouched.
--
--   ── MONEY ──
--   Everything monetary is integer micro-USD (BIGINT). 1 USD = 1_000_000.
--   Price is 22000 micro-USD per credit ($0.022), seeded in
--   `extra_usage_rates`. A ledger row snapshots the rate it used, so a later
--   rate change never reprices history. The balance is held in micro-USD, not
--   credits, because the money is what was paid; credits are derived at the
--   rate in force when they are spent.
--
--   ── PROD PRECONDITION (checked 2026-10-25, read-only) ──
--   `credit_ledger` rows with entry_kind = 'credit_pack_purchase' in prod: 0
--   (of 241,280 rows; owner role without FORCE RLS, so the count is global).
--   No backfill into `extra_usage_ledger` is needed. Pack checkout will feed
--   the extra ledger from 3.4 on.
--
--   ── TABLES ──
--   extra_usage_rates     Reference data (like plans/credit_packs). No RLS.
--                         Rate in force = latest row with effective_from <= t.
--   extra_usage_settings  One row per account. enabled, monthly limit, member
--                         cap, auto-reload config. A limit is required while
--                         enabled (CHECK). Auto-reload requires enabled, and
--                         its amount can never exceed the monthly limit.
--                         The row is absent == extra usage off. Defaults
--                         ($100 org, $50 Pro, 1x seat allowance) are applied
--                         by the writer in 3.4, not by column defaults, so
--                         a Pro account is not stuck with an org default.
--   extra_usage_balances  One row per account: the spendable balance
--                         (micro-USD, CHECK >= 0). Always equals the sum of
--                         that account's ledger amount_micro_usd; written in
--                         the same transaction as each ledger row.
--   extra_usage_ledger    Append-only money movements. amount_micro_usd is
--                         signed (+ purchases/refunds-in, - spend). Every row
--                         carries a NOT NULL idempotency_key (UNIQUE), and the
--                         Stripe event, payment intent and checkout session
--                         ids are each unique when present. A webhook replay
--                         or an after-the-fact derivation retry therefore
--                         conflicts instead of double-crediting.
--   extra_usage_periods   One row per account per month-long period: running
--                         spend for the org monthly limit, and when the limit
--                         was first hit. Periods cannot overlap per account.
--                         Per-member spend is the sum of that member's debit
--                         ledger rows in the period (indexed), not a second
--                         counter that could drift.
--   extra_usage_reloads   One row per auto-reload attempt. At most ONE
--                         in-flight reload per account (partial unique
--                         index). `stripe_idempotency_key` is a generated
--                         column, 'reload:' || id, so the Stripe key is
--                         stable across retries and cannot be set to
--                         anything else.
--   billing_notices       Claim-then-send email ledger. UNIQUE (account_id,
--                         notice_kind, dedupe_key): the claim is an INSERT ..
--                         ON CONFLICT DO NOTHING, and only the winner sends.
--
--   ── NOTES FOR 3.2 ──
--   * extra_usage_balances.balance_micro_usd has CHECK >= 0. A debit larger
--     than the balance must be clamped by the caller (spend what is there,
--     record the remainder as unfunded); an unclamped debit fails the
--     transaction rather than driving the balance negative.
--   * Debit rows take the period's lock via `lock_period_rotation`, same as
--     allowance settlement. Only entry_kind = 'debit' rows count against the
--     limit and member cap.
--
--   ── RLS / GRANTS ──
--   Account-keyed tables get the strict `account_isolation` policy used by
--   0233 (USING + WITH CHECK on app.account_id); readers/writers must open
--   `tenancy::scope::begin_account_scoped`. They are NOT on check_rls_coverage
--   .sh's 0083-derived roster, so no manifest change until Rust touches them.
--   No explicit GRANT: 0083's ALTER DEFAULT PRIVILEGES already covers new
--   tables for the app role, and prod connects as the table owner, so there
--   is no app-role-only grant to break. All tables are control-only
--   (CONTROL_TABLES in scripts/check_schema_parity.sh).
--
--   ── FINANCIAL ENTRY FENCE ──
--   extra_usage_ledger/_balances/_periods/_reloads carry the 0321 guard
--   triggers: writers must call billing_enter_account(account) in the same
--   transaction (as every other billing writer does) or the write is refused.
--
--   ── stripe_subscriptions ──
--   cancel_at_period_end / cancel_at mirror Stripe's fields so the Billing
--   page and cancellation flow (3.3) can show "ends on <date>". Adding a
--   NOT NULL column with a constant default is a metadata-only change.
--
-- Depends on: 0001_init (accounts, users), 0083_consumer_rls, 0162
--             (stripe_subscriptions), 0233 (policy convention).
-- Down:
--   ALTER TABLE stripe_subscriptions DROP COLUMN IF EXISTS cancel_at,
--       DROP COLUMN IF EXISTS cancel_at_period_end;
--   DROP TABLE IF EXISTS billing_notices, extra_usage_ledger,
--       extra_usage_reloads, extra_usage_periods, extra_usage_balances,
--       extra_usage_settings, extra_usage_rates;

SET LOCAL lock_timeout = '3s';

CREATE EXTENSION IF NOT EXISTS btree_gist;

-- ── rates ────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS extra_usage_rates (
    id                    UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    effective_from        TIMESTAMPTZ NOT NULL UNIQUE,
    micro_usd_per_credit  BIGINT      NOT NULL CHECK (micro_usd_per_credit > 0),
    created_at            TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- $0.022 per credit; matches the credit_packs price ($11 / 500 credits).
INSERT INTO extra_usage_rates (effective_from, micro_usd_per_credit)
VALUES ('2026-01-01T00:00:00Z', 22000)
ON CONFLICT (effective_from) DO NOTHING;

-- ── settings ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS extra_usage_settings (
    account_id                       UUID        PRIMARY KEY REFERENCES accounts(id) ON DELETE CASCADE,
    enabled                          BOOLEAN     NOT NULL DEFAULT false,
    -- Required while enabled. Org default $100, Pro default $50 (set by the
    -- writer). Micro-USD.
    monthly_limit_micro_usd          BIGINT      CHECK (monthly_limit_micro_usd IS NULL OR monthly_limit_micro_usd > 0),
    -- Per-member cap as a multiple of the member's seat allowance. NULL = no
    -- cap (personal accounts). Org writers default it to 1. Org-wide only.
    member_cap_seat_multiple         NUMERIC(8,2) CHECK (member_cap_seat_multiple IS NULL OR member_cap_seat_multiple > 0),
    auto_reload_enabled              BOOLEAN     NOT NULL DEFAULT false,
    -- Reload when the balance falls below the threshold, by the amount.
    auto_reload_threshold_micro_usd  BIGINT      CHECK (auto_reload_threshold_micro_usd IS NULL OR auto_reload_threshold_micro_usd > 0),
    auto_reload_amount_micro_usd     BIGINT      CHECK (auto_reload_amount_micro_usd IS NULL OR auto_reload_amount_micro_usd > 0),
    updated_by_user_id               UUID        REFERENCES users(id) ON DELETE SET NULL,
    created_at                       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at                       TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT extra_usage_settings_limit_required CHECK (
        NOT enabled OR monthly_limit_micro_usd IS NOT NULL
    ),
    CONSTRAINT extra_usage_settings_reload_needs_enabled CHECK (
        NOT auto_reload_enabled OR enabled
    ),
    CONSTRAINT extra_usage_settings_reload_complete CHECK (
        NOT auto_reload_enabled
        OR (auto_reload_threshold_micro_usd IS NOT NULL AND auto_reload_amount_micro_usd IS NOT NULL)
    ),
    -- Auto-reload never goes past the monthly limit.
    CONSTRAINT extra_usage_settings_reload_within_limit CHECK (
        auto_reload_amount_micro_usd IS NULL
        OR monthly_limit_micro_usd IS NULL
        OR auto_reload_amount_micro_usd <= monthly_limit_micro_usd
    )
);

-- ── balances ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS extra_usage_balances (
    account_id          UUID        PRIMARY KEY REFERENCES accounts(id) ON DELETE CASCADE,
    balance_micro_usd   BIGINT      NOT NULL DEFAULT 0 CHECK (balance_micro_usd >= 0),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ── periods ──────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS extra_usage_periods (
    id                  UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    account_id          UUID        NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    period_start        TIMESTAMPTZ NOT NULL,
    period_end          TIMESTAMPTZ NOT NULL,
    spent_micro_usd     BIGINT      NOT NULL DEFAULT 0 CHECK (spent_micro_usd >= 0),
    spent_credits       NUMERIC     NOT NULL DEFAULT 0 CHECK (spent_credits >= 0),
    -- First time spend reached the monthly limit this period (drives the
    -- admin email); NULL until then.
    limit_reached_at    TIMESTAMPTZ,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT extra_usage_periods_end_after_start CHECK (period_end > period_start),
    CONSTRAINT extra_usage_periods_unique UNIQUE (account_id, period_start),
    CONSTRAINT extra_usage_periods_no_overlap EXCLUDE USING gist (
        account_id WITH =,
        tstzrange(period_start, period_end) WITH &&
    )
);

-- ── reloads ──────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS extra_usage_reloads (
    id                        UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    account_id                UUID        NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    amount_micro_usd          BIGINT      NOT NULL CHECK (amount_micro_usd > 0),
    -- created: row claimed, Stripe not yet called. submitted: PaymentIntent
    -- created, awaiting payment_intent.* webhook. Both are "in flight".
    state                     TEXT        NOT NULL DEFAULT 'created'
        CHECK (state IN ('created', 'submitted', 'succeeded', 'failed', 'abandoned')),
    -- Stable Stripe idempotency key: derived, so a retry always reuses it.
    stripe_idempotency_key    TEXT        GENERATED ALWAYS AS ('reload:' || id::text) STORED,
    stripe_payment_intent_id  TEXT        CHECK (stripe_payment_intent_id IS NULL OR stripe_payment_intent_id <> ''),
    period_start              TIMESTAMPTZ NOT NULL,
    attempts                  INT         NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    failure_code              TEXT,
    failure_message           TEXT,
    created_at                TIMESTAMPTZ NOT NULL DEFAULT now(),
    submitted_at              TIMESTAMPTZ,
    settled_at                TIMESTAMPTZ,
    updated_at                TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT extra_usage_reloads_settled_consistent CHECK (
        (state IN ('succeeded', 'failed', 'abandoned')) = (settled_at IS NOT NULL)
    ),
    CONSTRAINT extra_usage_reloads_success_has_intent CHECK (
        state <> 'succeeded' OR stripe_payment_intent_id IS NOT NULL
    ),
    CONSTRAINT extra_usage_reloads_submitted_has_intent CHECK (
        state <> 'submitted' OR stripe_payment_intent_id IS NOT NULL
    )
);

-- One in-flight reload per account.
CREATE UNIQUE INDEX IF NOT EXISTS extra_usage_reloads_one_in_flight_idx
    ON extra_usage_reloads (account_id)
    WHERE state IN ('created', 'submitted');

CREATE UNIQUE INDEX IF NOT EXISTS extra_usage_reloads_idempotency_key_idx
    ON extra_usage_reloads (stripe_idempotency_key);

CREATE UNIQUE INDEX IF NOT EXISTS extra_usage_reloads_payment_intent_idx
    ON extra_usage_reloads (stripe_payment_intent_id)
    WHERE stripe_payment_intent_id IS NOT NULL;

-- Reaper: stale in-flight rows.
CREATE INDEX IF NOT EXISTS extra_usage_reloads_in_flight_age_idx
    ON extra_usage_reloads (created_at)
    WHERE state IN ('created', 'submitted');

CREATE INDEX IF NOT EXISTS extra_usage_reloads_account_created_idx
    ON extra_usage_reloads (account_id, created_at DESC);

-- ── ledger ───────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS extra_usage_ledger (
    id                          UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    account_id                  UUID        NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    entry_kind                  TEXT        NOT NULL CHECK (entry_kind IN (
        'topup', 'auto_reload', 'pack_purchase', 'refund', 'adjustment', 'debit')),
    -- Signed micro-USD: positive adds balance, negative spends it.
    amount_micro_usd            BIGINT      NOT NULL CHECK (amount_micro_usd <> 0),
    -- Credits this row bought or spent, with the rate used (snapshotted).
    credits                     NUMERIC,
    rate_micro_usd_per_credit   BIGINT      CHECK (rate_micro_usd_per_credit IS NULL OR rate_micro_usd_per_credit > 0),
    -- Who spent it (debits): drives the per-member cap. No FK, like
    -- credit_ledger.attributed_user_id: a departed user keeps their history.
    user_id                     UUID,
    -- Period a debit counts against (monthly limit / member cap).
    period_start                TIMESTAMPTZ,
    reload_id                   UUID        REFERENCES extra_usage_reloads(id),
    -- Idempotency. idempotency_key is mandatory and globally unique
    -- (e.g. 'stripe_event:evt_..', 'debit:job:<uuid>', 'reload:<id>').
    idempotency_key             TEXT        NOT NULL UNIQUE CHECK (idempotency_key <> ''),
    stripe_event_id             TEXT        CHECK (stripe_event_id IS NULL OR stripe_event_id <> ''),
    stripe_payment_intent_id    TEXT        CHECK (stripe_payment_intent_id IS NULL OR stripe_payment_intent_id <> ''),
    stripe_checkout_session_id  TEXT        CHECK (stripe_checkout_session_id IS NULL OR stripe_checkout_session_id <> ''),
    source_job_id               UUID,
    source_query_id             UUID,
    note                        TEXT,
    created_at                  TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- Sign by kind: money in is positive, spend is negative; refund
    -- (clawback of a purchase) is negative; adjustment is either.
    CONSTRAINT extra_usage_ledger_sign CHECK (
        (entry_kind IN ('topup', 'auto_reload', 'pack_purchase') AND amount_micro_usd > 0)
        OR (entry_kind IN ('refund', 'debit') AND amount_micro_usd < 0)
        OR entry_kind = 'adjustment'
    ),
    -- Money in from Stripe must be traceable to a Stripe object.
    CONSTRAINT extra_usage_ledger_purchase_traceable CHECK (
        entry_kind NOT IN ('topup', 'auto_reload', 'pack_purchase')
        OR stripe_event_id IS NOT NULL
        OR stripe_payment_intent_id IS NOT NULL
        OR stripe_checkout_session_id IS NOT NULL
    ),
    -- A debit says when it counts and how many credits at what rate.
    CONSTRAINT extra_usage_ledger_debit_shape CHECK (
        entry_kind <> 'debit'
        OR (period_start IS NOT NULL AND credits IS NOT NULL AND credits > 0
            AND rate_micro_usd_per_credit IS NOT NULL)
    ),
    CONSTRAINT extra_usage_ledger_reload_kind CHECK (
        reload_id IS NULL OR entry_kind = 'auto_reload'
    ),
    CONSTRAINT extra_usage_ledger_single_debit_source CHECK (
        (CASE WHEN source_job_id   IS NOT NULL THEN 1 ELSE 0 END) +
        (CASE WHEN source_query_id IS NOT NULL THEN 1 ELSE 0 END) <= 1
    )
);

CREATE UNIQUE INDEX IF NOT EXISTS extra_usage_ledger_stripe_event_idx
    ON extra_usage_ledger (stripe_event_id) WHERE stripe_event_id IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS extra_usage_ledger_payment_intent_idx
    ON extra_usage_ledger (stripe_payment_intent_id) WHERE stripe_payment_intent_id IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS extra_usage_ledger_checkout_session_idx
    ON extra_usage_ledger (stripe_checkout_session_id) WHERE stripe_checkout_session_id IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS extra_usage_ledger_source_job_idx
    ON extra_usage_ledger (source_job_id) WHERE source_job_id IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS extra_usage_ledger_source_query_idx
    ON extra_usage_ledger (source_query_id) WHERE source_query_id IS NOT NULL;
-- One ledger credit per reload.
CREATE UNIQUE INDEX IF NOT EXISTS extra_usage_ledger_reload_idx
    ON extra_usage_ledger (reload_id) WHERE reload_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS extra_usage_ledger_account_created_idx
    ON extra_usage_ledger (account_id, created_at DESC);
-- Per-member spend in a period (member cap check).
CREATE INDEX IF NOT EXISTS extra_usage_ledger_member_period_idx
    ON extra_usage_ledger (account_id, user_id, period_start)
    WHERE entry_kind = 'debit';

-- ── billing notices ──────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS billing_notices (
    id                   UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    account_id           UUID        NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    notice_kind          TEXT        NOT NULL CHECK (notice_kind IN (
        'extra_usage_limit_reached', 'member_cap_reached', 'balance_depleted',
        'auto_reload_succeeded', 'auto_reload_failed')),
    -- Per-kind uniqueness scope within the account: the period start for
    -- limit/depleted notices, 'user:<id>:<period>' for member caps, the
    -- reload id for reload notices.
    dedupe_key           TEXT        NOT NULL CHECK (dedupe_key <> ''),
    period_start         TIMESTAMPTZ,
    state                TEXT        NOT NULL DEFAULT 'claimed'
        CHECK (state IN ('claimed', 'sent', 'failed')),
    attempts             INT         NOT NULL DEFAULT 1 CHECK (attempts >= 1),
    provider_message_id  TEXT,
    error                TEXT,
    claimed_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    sent_at              TIMESTAMPTZ,
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT billing_notices_once UNIQUE (account_id, notice_kind, dedupe_key),
    CONSTRAINT billing_notices_sent_consistent CHECK ((state = 'sent') = (sent_at IS NOT NULL))
);

-- Reclaim claims whose sender died.
CREATE INDEX IF NOT EXISTS billing_notices_unsent_idx
    ON billing_notices (claimed_at) WHERE state IN ('claimed', 'failed');

-- ── RLS (account-keyed tables) ───────────────────────────────────────────
DO $$
DECLARE t TEXT;
BEGIN
    FOREACH t IN ARRAY ARRAY['extra_usage_settings', 'extra_usage_balances',
        'extra_usage_periods', 'extra_usage_reloads', 'extra_usage_ledger',
        'billing_notices']
    LOOP
        EXECUTE format('ALTER TABLE %I ENABLE ROW LEVEL SECURITY', t);
        EXECUTE format('DROP POLICY IF EXISTS account_isolation ON %I', t);
        EXECUTE format($p$CREATE POLICY account_isolation ON %I
            USING (account_id = current_setting('app.account_id')::uuid)
            WITH CHECK (account_id = current_setting('app.account_id')::uuid)$p$, t);
    END LOOP;
END $$;

-- ── Financial entry fence (0321) ─────────────────────────────────────────
-- The monetary tables take the same guard as credit_ledger/credit_balances:
-- every write must run in a transaction that called billing_enter_account(
-- account) under a matching app.account_id, and TRUNCATE is refused. This
-- makes the entry requirement part of the schema for 3.2/3.4/3.5 writers.
-- settings and billing_notices hold no money and are not fenced.
DO $$ DECLARE t TEXT; BEGIN
 FOREACH t IN ARRAY ARRAY['extra_usage_ledger', 'extra_usage_balances',
   'extra_usage_periods', 'extra_usage_reloads'] LOOP
  EXECUTE format('CREATE TRIGGER billing_current_generation BEFORE INSERT OR UPDATE OR DELETE ON public.%I FOR EACH ROW EXECUTE FUNCTION public.billing_guard_account_row()', t);
  EXECUTE format('CREATE TRIGGER billing_no_truncate BEFORE TRUNCATE ON public.%I FOR EACH STATEMENT EXECUTE FUNCTION public.managed_refuse_truncate()', t);
 END LOOP;
END $$;

-- ── Stripe cancellation mirror ───────────────────────────────────────────
ALTER TABLE stripe_subscriptions
    ADD COLUMN IF NOT EXISTS cancel_at_period_end BOOLEAN NOT NULL DEFAULT false,
    ADD COLUMN IF NOT EXISTS cancel_at TIMESTAMPTZ;
