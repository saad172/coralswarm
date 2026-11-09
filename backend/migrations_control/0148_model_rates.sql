-- Migration: 0148_model_rates
-- Description: usage-metering PR1 — DB-versioned LLM rate sheet.
--
--   Replaces the Rust-hardcoded `MODEL_RATES_PER_MILLION` / `SONNET_5_*` /
--   `DEFAULT_RATE_PER_MILLION` constants in `backend/src/routes/admin.rs`
--   with two control-plane tables, so a price correction or a new model no
--   longer needs a code deploy. See `backend/src/billing/rates.rs`
--   (`PriceSheet::load`/`PriceSheet::rate`) for the reader — that module's
--   doc comment has the full ordered-substring-match and `as_of` contract.
--
--   ── model_rates ──
--   One row per (provider, model-pattern, time window). `model_pattern` is
--   matched against `llm_usage.model` by SUBSTRING CONTAINMENT, never
--   equality — `llm_usage.model` is a free-form provider string (e.g.
--   `"anthropic.claude-sonnet-5-v1"`), not a foreign key, and this exactly
--   reproduces the pre-PR1 Rust `&[(pattern, input, output)]` array's own
--   `model.contains(pattern)` lookup.
--
--   `priority` reproduces that Rust array's ITERATION ORDER, which was
--   load-bearing: `.find()` returns the first match, so a more specific
--   pattern (`"claude-3-5-haiku"`) had to be checked before a more general
--   one it's also a substring-match subset of (`"claude-haiku"`). Lower
--   `priority` is checked first. `claude-sonnet-5`'s two rows both get
--   priority 0 — lower than every general-table row (1-13 below) — because
--   the pre-PR1 code special-cased `model.contains("claude-sonnet-5")`
--   UNCONDITIONALLY before ever consulting the general table (its rate is
--   time-dependent: introductory pricing through 2026-08-31, standard from
--   2026-09-01). The two sonnet-5 rows are never simultaneously ACTIVE (see
--   `effective_from`/`effective_to` below), so the tied priority never
--   actually has to break a conflict — it only has to outrank the general
--   `claude-sonnet` row (priority 8), which it does.
--
--   `effective_from <= as_of AND (effective_to IS NULL OR effective_to >
--   as_of)` selects the ACTIVE row(s) for a given `as_of` — the REPORTED
--   period's start, never `now()` (see `PriceSheet::rate`'s doc comment):
--   pricing a past month must never change depending on when the report is
--   re-run. Every row except the two sonnet-5 rows is seeded with
--   `effective_from` at a fixed epoch marker (2000-01-01, long before this
--   product existed) and `effective_to = NULL` — this reproduces the pre-PR1
--   behavior EXACTLY: every rate except sonnet-5's was NOT time-gated at
--   all, so giving it an "always has been, always will be active" window
--   (rather than, say, this migration's own deploy date) is the faithful
--   port, not an approximation. The sonnet-5 intro row is ALSO seeded at the
--   same epoch marker for the identical reason: the pre-PR1 code never
--   enforced any LOWER bound on the introductory rate either, only the
--   2026-09-01 cutover to standard pricing.
--
--   `is_blend_fallback` (exactly one active row at a time) is the pre-PR1
--   `DEFAULT_RATE_PER_MILLION` blended average — the last-resort rate for
--   (a) the `"__unattributed__"` sentinel `admin_spend`'s org-plane rows use
--   (no per-model breakdown exists at that grain — see that handler's own
--   doc comment) and (b) any genuinely-unrecognized model string. `(a)` is
--   deliberate and silent; `(b)` is not supposed to happen and
--   `PriceSheet::rate` logs a `tracing::warn!(alert = true, ...)` when it
--   does (plan §3 item 4: "keep the sentinel path, alert only on genuinely
--   unrecognized models"). `model_pattern = '__blend__'` on this row is a
--   human-readable label ONLY — blend rows are excluded from the
--   substring-match phase entirely (see `PriceSheet::rate`), so this string
--   is never compared against a real `llm_usage.model` value.
--
--   `cache_write_multiplier`/`cache_read_multiplier` default to 1.25/0.1 —
--   Anthropic's prompt-cache write/read multipliers on a model's own base
--   *input* rate (verified against Anthropic's pricing page 2026-08-03,
--   same as the constants this replaces). Every seeded row uses the
--   default; the columns exist per-row (not a single global constant, as
--   pre-PR1) so a future provider/model with different cache economics
--   needs no schema change.
--
--   Seed data is the 13 rows from the pre-PR1 `MODEL_RATES_PER_MILLION`
--   array, both `claude-sonnet-5` rows (intro 2/10, standard 3/15,
--   cutover 2026-09-01), and the blend/fallback row — 16 rows total,
--   `provider = 'anthropic'` throughout (this product is Anthropic-only
--   until usage-metering PR2/PR3's provider-switch addendum).
--
--   ── credit_conversion ──
--   No code reads it yet in THIS PR (usage-metering PR8's `credit_ledger` is
--   the first consumer of the value, per this plan's §1 PR8 "Deps: PR1
--   (credit conversion)"), but `backend/src/billing/credit_conversion.rs`
--   ships a tested loader/selector now (`CreditConversionTable::load` /
--   `::active`) so PR8/PR9 inherit a known-good, already-reviewed contract
--   rather than each having to decide how to read this table for the first
--   time under their own deadline pressure.
--
--   `reference_cost_per_credit` is the COST BASIS — how much real provider
--   spend one credit represents — and is DELIBERATELY NOT the retail price:
--   retail top-up pricing is locked separately at $0.022/credit (500/$11,
--   2k/$44, 5k/$110, never-expire packs; see memory
--   coralswarm-topup-pricing-decisions). Seeded here at $0.0044/credit — a
--   5x markup, ~80% gross margin over the cost basis. At this basis, the
--   Free tier's 1,000-credit LIFETIME pool (never resets) caps a fully-
--   burned free account at ~$4.40 of real provider spend, which is the
--   reason this basis was chosen over e.g. 2x or 1:1 — see the seed
--   `note` column below for the same explanation, kept next to the number
--   itself rather than only in this comment.
--
--   `effective_from` is seeded at the SAME fixed epoch marker as
--   `model_rates` (2000-01-01, literal — never `now()`, which would make
--   the seed non-deterministic across environments and defeat any later
--   reproducibility check). This value WILL change over time (price
--   changes, provider switches) — that is the entire reason the table is
--   versioned by `effective_from` rather than a single scalar row, exactly
--   like `model_rates`. `CreditConversionTable::active(as_of)` selects the
--   NEWEST row with `effective_from <= as_of` (never a bare `LIMIT 1` with
--   no `ORDER BY`, which would pick an arbitrary row the moment a second
--   row exists) — see that function's own doc comment.
--
--   `reference_cost_per_credit` is read as `sqlx::types::Decimal`
--   (rust_decimal), NEVER cast to `float8`/`f64` anywhere in the loader —
--   unlike `model_rates`, which casts NUMERIC -> float8 because it only
--   ever backs a "rough, internally-consistent estimate" (see that table's
--   header above), this column is the one place in the usage-metering
--   train where exact decimal arithmetic is load-bearing: at $0.0044/
--   credit, a single float-rounding error is ~0.1% of a credit and
--   compounds across every debit PR9 ever writes.
--
--   ── RLS: neither table gets a policy ──
--   Both tables are GLOBAL rate/config data with no `account_id` column and
--   no per-account boundary to enforce — every account/tenant is priced off
--   the SAME sheet. Same category and same treatment as
--   `platform_metrics_daily`/`platform_active_users`/`user_access_log`
--   (migration `0095_platform_metrics.sql`): control-only, deliberately
--   outside any RLS roster. Access control is the existing
--   `X-Internal-Token` gate on `/internal/admin/spend` (and any future
--   billing/admin route), not RLS.
--
--   ── CI note ──
--   Both table names MUST be added to `CONTROL_TABLES` in
--   `scripts/check_schema_parity.sh` in this SAME PR (done) — neither has a
--   tenant-tree twin (control-only → tenant column in the plan's migration
--   ledger, §0).
--
-- Depends on: 0095_platform_metrics (the "global control-only table, no RLS,
--             no tenant twin" precedent both tables here follow).
-- Down:
--   DROP TABLE IF EXISTS credit_conversion;
--   DROP TABLE IF EXISTS model_rates;

CREATE TABLE model_rates (
    id                      UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    provider                TEXT        NOT NULL,
    model_pattern           TEXT        NOT NULL,
    priority                INT         NOT NULL,
    input_per_million       NUMERIC     NOT NULL,
    output_per_million      NUMERIC     NOT NULL,
    cache_write_multiplier  NUMERIC     NOT NULL DEFAULT 1.25,
    cache_read_multiplier   NUMERIC     NOT NULL DEFAULT 0.1,
    effective_from          TIMESTAMPTZ NOT NULL,
    effective_to            TIMESTAMPTZ,
    is_blend_fallback       BOOL        NOT NULL DEFAULT false,

    CONSTRAINT model_rates_valid_window
        CHECK (effective_to IS NULL OR effective_to > effective_from)
);

-- `PriceSheet::load` reads the full table ordered by priority — this index
-- exists for that read pattern, not because the table is large (it is not:
-- 16 seed rows, growing by a handful per PR/price change).
CREATE INDEX idx_model_rates_priority ON model_rates (priority);

CREATE TABLE credit_conversion (
    id                         UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    reference_cost_per_credit  NUMERIC     NOT NULL CHECK (reference_cost_per_credit > 0),
    effective_from             TIMESTAMPTZ NOT NULL,
    note                       TEXT
);

-- ── Seed: model_rates ─────────────────────────────────────────────────────
-- Epoch marker reproducing "was never time-gated pre-PR1" for every row
-- except the sonnet-5 standard rate's effective_from (see header above).
-- claude-sonnet-5 — priority 0, checked before every general-table pattern.
INSERT INTO model_rates
    (provider, model_pattern, priority, input_per_million, output_per_million, effective_from, effective_to, is_blend_fallback)
VALUES
    ('anthropic', 'claude-sonnet-5', 0, 2.0,  10.0, TIMESTAMPTZ '2000-01-01T00:00:00Z', TIMESTAMPTZ '2026-09-01T00:00:00Z', false),
    ('anthropic', 'claude-sonnet-5', 0, 3.0,  15.0, TIMESTAMPTZ '2026-09-01T00:00:00Z', NULL,                               false),

    -- General table, priority = pre-PR1 array index (1-13), all always-active.
    ('anthropic', 'claude-haiku-4-5',   1,  1.0,  5.0,  TIMESTAMPTZ '2000-01-01T00:00:00Z', NULL, false),
    ('anthropic', 'claude-sonnet-4-6',  2,  3.0,  15.0, TIMESTAMPTZ '2000-01-01T00:00:00Z', NULL, false),
    ('anthropic', 'claude-opus-4-8',    3,  5.0,  25.0, TIMESTAMPTZ '2000-01-01T00:00:00Z', NULL, false),
    ('anthropic', 'claude-opus-5',      4,  5.0,  25.0, TIMESTAMPTZ '2000-01-01T00:00:00Z', NULL, false),
    ('anthropic', 'claude-fable-5',     5,  10.0, 50.0, TIMESTAMPTZ '2000-01-01T00:00:00Z', NULL, false),
    ('anthropic', 'claude-3-5-sonnet',  6,  3.0,  15.0, TIMESTAMPTZ '2000-01-01T00:00:00Z', NULL, false),
    ('anthropic', 'claude-3-7-sonnet',  7,  3.0,  15.0, TIMESTAMPTZ '2000-01-01T00:00:00Z', NULL, false),
    ('anthropic', 'claude-sonnet',      8,  3.0,  15.0, TIMESTAMPTZ '2000-01-01T00:00:00Z', NULL, false),
    ('anthropic', 'claude-3-5-haiku',   9,  0.8,  4.0,  TIMESTAMPTZ '2000-01-01T00:00:00Z', NULL, false),
    ('anthropic', 'claude-3-haiku',     10, 0.25, 1.25, TIMESTAMPTZ '2000-01-01T00:00:00Z', NULL, false),
    ('anthropic', 'claude-haiku',       11, 0.8,  4.0,  TIMESTAMPTZ '2000-01-01T00:00:00Z', NULL, false),
    ('anthropic', 'claude-3-opus',      12, 15.0, 75.0, TIMESTAMPTZ '2000-01-01T00:00:00Z', NULL, false),
    ('anthropic', 'claude-opus',        13, 15.0, 75.0, TIMESTAMPTZ '2000-01-01T00:00:00Z', NULL, false),

    -- Blend/fallback — excluded from substring matching (see header above).
    ('anthropic', '__blend__', 100, 5.7, 27.5, TIMESTAMPTZ '2000-01-01T00:00:00Z', NULL, true);

-- ── Seed: credit_conversion ──────────────────────────────────────────────
-- Cost basis, not retail price — see header above. $0.0044/credit is a 5x
-- markup under the locked $0.022/credit top-up price (~80% gross margin);
-- at this basis the 1,000-credit lifetime Free tier caps a fully-burned
-- free account at ~$4.40 of real provider spend, which is why this basis
-- (not 2x or 1:1) was chosen. Same fixed epoch marker as model_rates —
-- literal, never now().
INSERT INTO credit_conversion (reference_cost_per_credit, effective_from, note)
VALUES (
    0.0044,
    TIMESTAMPTZ '2000-01-01T00:00:00Z',
    'Cost basis (USD of real provider spend per credit), NOT the retail price. ' ||
    'Retail top-up pricing is locked separately at $0.022/credit (500/$11, 2k/$44, ' ||
    '5k/$110, never-expire packs) -- 0.0044 is a 5x markup, ~80% gross margin. ' ||
    'At this basis the 1,000-credit lifetime Free tier caps a fully-burned free ' ||
    'account at ~$4.40 of real provider spend -- the reason this basis was chosen ' ||
    'over 2x or 1:1.'
);
