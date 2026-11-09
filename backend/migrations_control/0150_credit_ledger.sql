-- Migration: 0150_credit_ledger
-- Description: usage-metering PR8 of the 13-PR train
--   (`.scratch/usage-metering/plan.md` §1 PR8) — the credit ledger, balances,
--   plan definitions, grants, and enforcement-observation log. SCHEMA ONLY:
--   nothing in this PR writes to these tables. PR9 (the derivation worker)
--   consumes `credit_ledger`; PR10 seeds `plans`/`grants` and wires
--   enforcement. No Rust code changes ship with this migration.
--
--   ── Numbering note — read before assuming a bug ──
--   This repo's migration numbers are assigned at MERGE time, not authoring
--   time (plan §0, "Numbering reality"), and every in-flight usage-metering
--   branch forked from the same head. At authoring time this tree's real
--   head was control `0147` (PR7, merged). Two OTHER usage-metering branches
--   are independently in flight and NOT part of this tree:
--     - `cs-usage-pr1-rate-sheet` claims control `0148` (`model_rates`,
--       `credit_conversion`) — this migration's `credit_conversion` FK-less
--       dependency (see `credit_ledger.cost_usd` note below) assumes that
--       PR lands, but its file is intentionally NOT copied into this tree.
--     - `cs-usage-pr4-choke-point` claims control `0149`
--       (`llm_usage_pending`) — also intentionally NOT copied in.
--   This PR takes `0150`, per explicit instruction, leaving both `0148` and
--   `0149` as gaps in THIS tree's numbering until the coordinator merges the
--   sibling PRs and resolves the actual sequence. `scripts/check_migrations.sh`
--   is therefore EXPECTED to fail here with "gap at 0148" / "gap at 0149" —
--   that is correct given the current state of three parallel branches, not
--   a defect in this PR. Do not renumber to close it; do not touch 0148/0149.
--
--   ── credit_ledger ──
--   One row per credit-affecting event: a usage debit (PR9, one row per
--   completed job / request-path query / journal-compile group) or a credit
--   grant (PR10, plan assignment / hand-sale top-up). `entry_kind` is
--   deliberately left as free-form non-empty TEXT rather than a closed CHECK
--   vocabulary here: PR9 (debit axes) and PR10 (grant) are the PRs that
--   actually define and write that vocabulary, and this schema-only PR has
--   no authoritative source for it — guessing wrong would just force an
--   ALTER later. Contrast with `journal_granularity` below, which DOES get a
--   closed CHECK, because that vocabulary already shipped (verbatim) in
--   merged PR2's `llm_usage.source_journal_granularity`.
--
--   `credits`/`cost_usd` (and `credit_balances.balance`, `plans.credits`,
--   `grants.credits_override` below) are all NUMERIC, never `float8`/
--   `double precision`. Sibling PR1's `credit_conversion.reference_cost_per_
--   credit` is the direct precedent (read as `sqlx::types::Decimal` /
--   `rust_decimal::Decimal`, never cast to float) — at the founder-locked
--   $0.0044/credit cost basis, a single float-rounding error is ~0.1% of a
--   credit and compounds across every debit this ledger ever accumulates.
--   This PR adds no Rust code, so there is no loader to pin that type in yet,
--   but the DDL is written so PR9's loader has no reason to reach for f64.
--
--   ***Cross-tenant uniqueness — read this before "fixing" the idempotency
--   keys.*** `credit_ledger` lives on the control plane (register decision 6),
--   but `source_job_id` references `jobs.id` and `source_query_id` references
--   `query_history.id`, BOTH of which live in per-org TENANT databases, not
--   here. The two partial unique indexes on those columns are therefore
--   GLOBAL across every tenant, not scoped to one. That is safe: `jobs.id`
--   and `query_history.id` are v4 UUIDs, so a cross-tenant collision on the
--   literal id value is not a practical concern. It is NOT safe to "fix" this
--   later to `(account_id, source_job_id)` — that would quietly reopen the
--   double-charge window these indexes exist to close, since two different
--   tenant DBs' `jobs` tables have no relationship to each other or to any
--   one `account_id` at the UUID-collision level these indexes protect
--   against. Same reasoning applies to `graph_id` below: it is deliberately
--   NOT a foreign key. `graphs` exists as a table shape on BOTH the control
--   tree (0003_atoms.sql) and the tenant tree (0001_tenant_baseline.sql,
--   per-org DB-per-org), but they are physically different databases holding
--   physically different rows — a control-plane FK against the control
--   plane's OWN `graphs` table would reject every journal-axis row for an
--   org account (PR9's plan §1 text: "resolve the account via
--   graphs.account_id" for journal debits, which is exactly the case where
--   `graph_id` names a tenant-DB graph the control DB has never heard of).
--   `source_job_id`/`source_query_id`/`graph_id` are therefore all bare
--   UUID columns with no REFERENCES clause, by design, not by oversight —
--   same posture PR2's header already states for these same columns' source
--   table, `llm_usage.account_id`/`graph_id` (0044's own header).
--
--   ***The three partial unique indexes are the entire idempotency story***
--   (plan §1 PR8's own framing) — do not simplify any of them:
--     1. `source_job_id WHERE source_job_id IS NOT NULL` — a job's
--        `idempotency_key` (`extract:<atom_id>`) is only unique among
--        PENDING rows (`idx_jobs_idem_pending` is itself partial), so a
--        re-enqueued atom can produce a SECOND `done` `jobs` row with the
--        same key. Keying on the true PK (`jobs.id`) here, not the
--        idempotency key, is what prevents double-charging that atom (plan
--        §1 PR9, "the double-charge trap").
--     2. `source_query_id WHERE source_query_id IS NOT NULL` — same shape
--        for the request-path axis.
--     3. `(account_id, graph_id, journal_granularity, journal_period)
--        WHERE journal_period IS NOT NULL` — the journal-compile axis.
--        TWO columns here are load-bearing beyond what a naive reading of
--        "key on the period" would produce:
--          - `journal_granularity` is IN the key because
--            `compile_tier_group` emits day/week/month rollups whose
--            `period_start` is midnight UTC — which is ALSO an hour
--            boundary. An hour compile at `2026-08-13T00:00:00Z` and a day
--            compile for 2026-08-13 (`period_start` also
--            `2026-08-13T00:00:00Z`) would collide on a period-only key and
--            the unique index would silently reject (i.e. drop) one of
--            them — permanently under-charging that axis. This is the exact
--            collision negative control 2 (below) proves does NOT happen.
--          - `graph_id` is IN the key because journal rollups are per-ocean
--            and one account can own several oceans; without it, two
--            different oceans' same-instant compiles would collide into one
--            ledger row.
--        These two columns join against `llm_usage.source_journal_period` /
--        `llm_usage.source_journal_granularity`, added in merged PR2
--        (`0141_llm_usage_provider_cost.sql`).
--
--   ***Revision (post-review): a NULL `graph_id` silently bypassed the
--   journal-axis unique index — fixed with a CHECK.*** In Postgres, NULLs
--   compare as DISTINCT from each other in a unique index (absent `NULLS NOT
--   DISTINCT`, PG15+ syntax, not used here). `idx_credit_ledger_journal_axis_
--   unique` includes `graph_id` in its key, so two journal-axis rows sharing
--   the same `(account_id, journal_granularity, journal_period)` but BOTH
--   carrying `graph_id IS NULL` do not collide — they are treated as two
--   distinct index entries and BOTH insert. The unique index that exists
--   specifically to prevent double-charging the journal axis would silently
--   do nothing for exactly the rows it cannot identify a graph for — the
--   same double-charge failure mode plan §4 ranks as the single highest-
--   severity risk in this whole train, and the failure looks exactly like
--   success (both inserts succeed, nothing errors). This was latent rather
--   than live at authoring time — both journal call sites pass a
--   non-optional graph today (`Some(candidate.graph_id)` at
--   `workers/journal_sweep.rs:1295`, `Some(group.graph_id)` at `:1851`) — but
--   `llm_usage.graph_id` itself IS nullable and PR9 has to tolerate
--   NULL-`graph_id` rows on the tenant plane, so relying on caller discipline
--   here is not good enough. Fixed with `credit_ledger_journal_axis_needs_
--   graph` below: a journal-axis row (`journal_period IS NOT NULL`) now MUST
--   carry a non-NULL `graph_id`, rejected loudly at INSERT with a named
--   constraint rather than silently succeeding-but-deduplicating in a way a
--   reader of the index definition would not predict. (An index-level `NULLS
--   NOT DISTINCT` would also close this, but a CHECK was chosen deliberately:
--   it fails at the write site with a clear error, rather than relying on a
--   reader to know that `NULLS NOT DISTINCT` was set on this particular
--   index and not, say, `idx_credit_ledger_source_job_id_unique`.) See
--   negative control 2b for the before/after proof — the fix was verified to
--   actually close the hole, not just add a constraint that happens not to
--   fire.
--
--   A CHECK enforces that a row touches AT MOST ONE of the three source axes
--   (`source_job_id`, `source_query_id`, `journal_period`) — a grant-type row
--   legitimately has ALL THREE null (it has no unit-of-work source at all),
--   but a debit row naming two axes at once would be a logic bug in whatever
--   wrote it, not a valid state; the CHECK makes that structurally
--   impossible rather than merely undocumented. This constraint is NOT named
--   in the plan's column list — flagging it explicitly as an addition made
--   in this PR, easy to drop in review if it turns out to conflict with
--   PR9's actual write pattern (the table is empty; nothing has been
--   written against it yet).
--
--   `journal_period`/`journal_granularity` are paired by a second CHECK
--   (both null or both set) — a period without a granularity, or vice versa,
--   can never be a meaningful journal-axis row.
--
--   Two supporting (non-unique) indexes are ALSO not named in the plan's
--   column/index list but added here on the same low-risk basis as the
--   mutual-exclusivity CHECK above: `idx_credit_ledger_account_created`
--   backs the obvious "list this account's ledger, newest first" query PR12
--   (usage endpoint) and PR13 (admin credit section) will need, and costs
--   nothing on a currently-empty table.
--
--   ── credit_balances ──
--   Materialized per-account balance (plan §1 PR8: "Materialized"). PR9/PR10
--   own how it gets kept in sync with `credit_ledger` (this PR does not
--   write to either table); the shape here is deliberately minimal — a
--   single row per account, no history — because `credit_ledger` itself is
--   already the append-only history this table summarizes.
--
--   ── plans / grants ──
--   `plans` is GLOBAL reference data (the same finite set of plan
--   definitions applies to every account), same category as sibling PR1's
--   `model_rates`/`credit_conversion` (see that migration's own "RLS:
--   neither table gets a policy" precedent, `0095_platform_metrics`
--   before it) — so `plans` gets NO RLS policy, by the same reasoning:
--   there is no per-account boundary to enforce on a shared price/plan
--   sheet. `grants` DOES get the standard `account_isolation` policy: a
--   grant is a per-account fact (which plan an account is on, and since
--   when), not shared reference data.
--
--   `refresh_kind` models the Free-vs-paid distinction the plan is explicit
--   about NOT collapsing into "a period that happens to be long": the Free
--   pool is LIFETIME and never resets (1,000 credits, plan §1 PR8 / §2 item
--   14), which is a structurally different shape from an
--   anniversary-from-grant-start period that recurs. `refresh_kind =
--   'lifetime'` forces `period_length IS NULL` (there is no period to
--   recur); `refresh_kind = 'anniversary'` forces `period_length IS NOT
--   NULL` (an INTERVAL — e.g. 1 month — measured from the grant's own
--   `granted_at`, i.e. "anniversary-from-grant-start", never a calendar
--   month boundary). This PR does not seed any plan rows (no Free / no
--   Founder-Comp row) — that is explicitly PR10's job ("epoch at deploy, no
--   backfill"); seeding here would be scope creep into a PR this one has a
--   hard `Deps: PR1, PR2` on but is not itself.
--
--   `grants.credits_override` lets one grant diverge from its plan's default
--   `credits` amount without a new `plans` row per account — this is how
--   PR10's "Alex + testorg get a real Founder/Comp plan row with a huge
--   pool" is expected to be modeled: one `founder_comp` plan definition,
--   with the actual pool size set per grant if it needs to differ from
--   account to account.
--
--   Deliberately NOT added: a uniqueness constraint limiting an account to
--   one active (`revoked_at IS NULL`) grant at a time. It is plausible by
--   design that an account holds a base Free grant AND a separate hand-sale
--   top-up grant simultaneously (plan §1 PR10: "Hand-sales get this script
--   instead of Stripe") — PR10 owns that policy, and this schema should not
--   pre-empt it with a constraint that might have to be dropped again.
--
--   ── enforcement_events ──
--   Durable BECAUSE `admission.rs`'s existing observe-only precedent is
--   tracing-only (plan §1 PR8) — a would-have-blocked *view* (PR13: "Show
--   balances and would-have-blocked") needs a queryable table, not a log
--   line. `idx_enforcement_events_account_created` backs exactly that view
--   query, same reasoning as `idx_credit_ledger_account_created` above.
--
--   ── ON DELETE CASCADE on accounts(id) — deliberate, not inherited ──
--   Every FK to `accounts(id)` in this migration (`credit_ledger`,
--   `credit_balances`, `grants`, `enforcement_events`) cascades. Deleting an
--   account therefore erases its ENTIRE billing history — every ledger
--   entry, its materialized balance, its grants, and its enforcement audit
--   trail all vanish together, with no way to reconstruct "what this account
--   was charged" after the fact. This is a real accounting decision, made
--   here explicitly rather than inherited from copy-pasting the FK shape:
--   for a pre-revenue product with no Stripe integration (plan's "Not in
--   scope" list), CASCADE keeps account-deletion flows working end-to-end
--   and matches data-erasure expectations, so it is the right default today.
--   If retention of billing history past account deletion is ever required,
--   the fix is to DROP the FK (or add an `ON DELETE SET NULL` on a nullable
--   copy of the identifying fields) and retain the orphaned rows — NOT to
--   switch to `ON DELETE RESTRICT`, which would simply block account
--   deletion outright the moment any of these four tables holds a row.
--   `enforcement_events` is worth calling out on its own: cascading it costs
--   an AUDIT trail, not a billing one — a would-have-blocked row is evidence
--   about a decision the system made (or would have made), and losing it on
--   account deletion is a different kind of loss than losing a ledger entry,
--   even though the SQL shape (`ON DELETE CASCADE`) is identical.
--
--   FOUNDER DECISION 2026-08-13: keep the cascade, and REVISIT IT THE MOMENT
--   REAL MONEY IS COLLECTED. The reasoning is specific to today's state — no
--   Stripe, no revenue, no paying customer — so there is no financial record
--   worth preserving yet, and cascade keeps deletion flows and erasure
--   requests clean. That premise expires: once these tables hold entries
--   backing an actual invoice, normal accounting practice wants the record to
--   outlive the account, and the fix is the DROP-the-FK route above. Whoever
--   ships billing is responsible for re-opening this; it is deliberately
--   cheap now (the tables are empty) and expensive later.
--
--   ── RLS: account_isolation, following 0083/0140's precedent exactly ──
--   `credit_ledger`, `credit_balances`, `grants`, `enforcement_events` each
--   get the standard control-plane `account_isolation` policy, anchored
--   DIRECTLY on `account_id` (bare `USING`, no split `_select`/`_insert`/
--   `_update`/`_delete`) — same shape as `mcp_tasks`/`reef_judgement`
--   (0139/0140): every row on all four tables always carries an
--   `account_id`, and (per those tables' own headers) the bare-USING/
--   reused-as-WITH-CHECK shape is only unsafe when a table's write path can
--   attach a row to another account's resource via a DIFFERENT column
--   (`node_promotions`' cross-graph case) — none of these four tables have
--   that shape; `graph_id`/`source_job_id`/`source_query_id` are metadata,
--   not the RLS boundary. `plans` gets no policy at all (see above).
--   Registered in `scripts/check_schema_parity.sh`'s `CONTROL_TABLES` in
--   this same PR, since none of the five tables has (or needs) a tenant-tree
--   twin — this is a control-only PR (plan §1 PR8: "control-only →
--   CONTROL_TABLES").
--
-- Depends on: 0001_init (accounts), 0140_mcp_tasks (the account_isolation
--   pattern this migration follows), 0141_llm_usage_provider_cost.sql (the
--   source_journal_period/source_journal_granularity columns credit_ledger's
--   journal-axis key joins against), 0148_model_rates.sql (NOT in this tree
--   — see the numbering note above; `credit_conversion` is the eventual
--   reader-side counterpart to this table's `cost_usd`/`credits` pair, no
--   hard schema dependency exists here since neither table references the
--   other by FK).
-- Down:
--   DROP POLICY IF EXISTS account_isolation ON enforcement_events;
--   ALTER TABLE enforcement_events DISABLE ROW LEVEL SECURITY;
--   DROP TABLE IF EXISTS enforcement_events;
--   DROP POLICY IF EXISTS account_isolation ON grants;
--   ALTER TABLE grants DISABLE ROW LEVEL SECURITY;
--   DROP TABLE IF EXISTS grants;
--   DROP TABLE IF EXISTS plans;
--   DROP POLICY IF EXISTS account_isolation ON credit_balances;
--   ALTER TABLE credit_balances DISABLE ROW LEVEL SECURITY;
--   DROP TABLE IF EXISTS credit_balances;
--   DROP POLICY IF EXISTS account_isolation ON credit_ledger;
--   ALTER TABLE credit_ledger DISABLE ROW LEVEL SECURITY;
--   DROP TABLE IF EXISTS credit_ledger;

-- ── credit_ledger ────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS credit_ledger (
    id                    UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    account_id            UUID        NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    entry_kind            TEXT        NOT NULL CHECK (entry_kind <> ''),
    credits               NUMERIC     NOT NULL,

    -- Idempotency-key columns. No FK on any of the three below — see the
    -- "Cross-tenant uniqueness" note above.
    source_job_id         UUID,
    source_query_id       UUID,
    graph_id              UUID,
    journal_period        TIMESTAMPTZ,
    journal_granularity   TEXT        CHECK (journal_granularity IS NULL OR journal_granularity IN ('hour', 'day', 'week', 'month')),

    cost_usd              NUMERIC     CHECK (cost_usd IS NULL OR cost_usd >= 0),
    created_at            TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT credit_ledger_journal_axis_paired CHECK (
        (journal_period IS NULL AND journal_granularity IS NULL) OR
        (journal_period IS NOT NULL AND journal_granularity IS NOT NULL)
    ),
    -- NULLs compare DISTINCT in a unique index, so without this a journal-axis
    -- row with graph_id IS NULL would silently bypass
    -- idx_credit_ledger_journal_axis_unique's dedup entirely — see the
    -- "Revision (post-review)" note above.
    CONSTRAINT credit_ledger_journal_axis_needs_graph CHECK (
        journal_period IS NULL OR graph_id IS NOT NULL
    ),
    CONSTRAINT credit_ledger_single_source_axis CHECK (
        (CASE WHEN source_job_id   IS NOT NULL THEN 1 ELSE 0 END) +
        (CASE WHEN source_query_id IS NOT NULL THEN 1 ELSE 0 END) +
        (CASE WHEN journal_period  IS NOT NULL THEN 1 ELSE 0 END) <= 1
    )
);

-- Idempotency keys — see header for why each column/combination is exactly
-- this and no simpler.
CREATE UNIQUE INDEX IF NOT EXISTS idx_credit_ledger_source_job_id_unique
    ON credit_ledger (source_job_id)
    WHERE source_job_id IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS idx_credit_ledger_source_query_id_unique
    ON credit_ledger (source_query_id)
    WHERE source_query_id IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS idx_credit_ledger_journal_axis_unique
    ON credit_ledger (account_id, graph_id, journal_granularity, journal_period)
    WHERE journal_period IS NOT NULL;

-- Supporting (non-unique) lookup index — see header.
CREATE INDEX IF NOT EXISTS idx_credit_ledger_account_created
    ON credit_ledger (account_id, created_at DESC);

ALTER TABLE credit_ledger ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS account_isolation ON credit_ledger;
CREATE POLICY account_isolation ON credit_ledger
    USING (account_id = current_setting('app.account_id')::uuid);

-- ── credit_balances ──────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS credit_balances (
    account_id  UUID        PRIMARY KEY REFERENCES accounts(id) ON DELETE CASCADE,
    balance     NUMERIC     NOT NULL DEFAULT 0,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE credit_balances ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS account_isolation ON credit_balances;
CREATE POLICY account_isolation ON credit_balances
    USING (account_id = current_setting('app.account_id')::uuid);

-- ── plans ────────────────────────────────────────────────────────────────
-- Global reference data — see header's "RLS: account_isolation" note for why
-- this table alone gets no policy.
CREATE TABLE IF NOT EXISTS plans (
    id             UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    code           TEXT        NOT NULL UNIQUE CHECK (code <> ''),
    name           TEXT        NOT NULL,
    credits        NUMERIC     NOT NULL CHECK (credits >= 0),
    refresh_kind   TEXT        NOT NULL CHECK (refresh_kind IN ('lifetime', 'anniversary')),
    period_length  INTERVAL,
    is_active      BOOL        NOT NULL DEFAULT true,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT plans_period_length_matches_refresh_kind CHECK (
        (refresh_kind = 'lifetime'    AND period_length IS NULL) OR
        (refresh_kind = 'anniversary' AND period_length IS NOT NULL)
    )
);

-- ── grants ───────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS grants (
    id                UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    account_id        UUID        NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    plan_id           UUID        NOT NULL REFERENCES plans(id),
    -- Anniversary anchor ("grant-start"), always UTC (TIMESTAMPTZ is stored
    -- UTC internally regardless of session timezone).
    granted_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    credits_override  NUMERIC     CHECK (credits_override IS NULL OR credits_override >= 0),
    revoked_at        TIMESTAMPTZ,
    note              TEXT,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT grants_revoked_after_granted CHECK (revoked_at IS NULL OR revoked_at >= granted_at)
);

CREATE INDEX IF NOT EXISTS idx_grants_account_id ON grants (account_id, granted_at DESC);

ALTER TABLE grants ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS account_isolation ON grants;
CREATE POLICY account_isolation ON grants
    USING (account_id = current_setting('app.account_id')::uuid);

-- ── enforcement_events ───────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS enforcement_events (
    id                   UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    account_id           UUID        NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    would_have_blocked   BOOL        NOT NULL,
    reason               TEXT        NOT NULL CHECK (reason <> ''),
    created_at           TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_enforcement_events_account_created
    ON enforcement_events (account_id, created_at DESC);

ALTER TABLE enforcement_events ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS account_isolation ON enforcement_events;
CREATE POLICY account_isolation ON enforcement_events
    USING (account_id = current_setting('app.account_id')::uuid);
