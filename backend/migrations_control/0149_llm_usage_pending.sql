-- Migration: 0149_llm_usage_pending
-- Description: PR4 of the usage-metering train (`.scratch/usage-metering/plan.md`
--   §1 PR4) — the reserve half of the reserve/finalize choke point
--   (`llm::usage::metered_complete`/`metered_complete_via_stream` and the
--   hand-wired interactive-answer-stream reserve/finalize).
--
--   A pending row is inserted BEFORE the provider round-trip and committed on
--   its own connection — durable and visible to any other connection — before
--   the LLM call ever starts. On success, finalize (a SEPARATE transaction)
--   inserts the real `llm_usage` row with actuals and deletes this pending row
--   together, so the swap is atomic and a crash between the two steps is
--   impossible: either the pending row is still here (call in flight or the
--   process died mid-call), or it is gone and a matching `llm_usage` row
--   exists, never both gone / both present. A pending row whose provider call
--   errors, or whose owning process is killed mid-call, is left behind for
--   PR6's sweeper (`kind='sync'` orphans older than ~15 min; `kind='batch'`
--   older than ~25h, since `wait_until_ended` polls up to 24h) — that is the
--   entire point of separating reserve from finalize rather than writing the
--   pending row inside the same transaction that later finalizes it (which
--   would never be visible to another connection and would vanish on process
--   kill, defeating the sweeper before it exists).
--
--   Columns mirror `llm_usage` (0044/0073/0141) for the fields shared with the
--   eventual real row (`account_id`, `graph_id`, `task`, `model`) plus what
--   reserve alone needs:
--   * `user_id` — the tenant-plane RLS scope selector (mirrors `llm_usage`'s
--     own `user_id` handling in `llm::usage::record`/`reserve`/`finalize` —
--     NOT a stored-attribution column, just what `begin_tenant_scoped` needs
--     to satisfy `migrations_tenant/0009_tenant_rls`'s member-ocean policy).
--   * `provider` — `NOT NULL`, no default (unlike `llm_usage.provider`'s
--     `DEFAULT 'anthropic'`): reserve always has a concrete provider in hand
--     (the `LlmProvider` this call is about to run against), so there is no
--     backfill story to lean on a default for.
--   * `estimated_input_tokens` — `request character length / 4`, clamped to
--     `[1, RESERVE_TOKEN_ESTIMATE_CEILING]` (see `llm::usage`'s Rust doc).
--     Exists ONLY for in-flight spend visibility and a future
--     `check_and_debit` — it is NEVER billed; finalize always overwrites with
--     the provider's actual token counts on `llm_usage`. Cheap and obviously
--     approximate beats a tokenizer dependency for a number that never
--     reaches a ledger.
--   * `kind` — `'sync'` (the `metered_complete`/`metered_complete_via_stream`/
--     hand-wired-stream paths this PR wires) vs `'batch'` (PR6's Anthropic
--     Batch API sibling, NOT written by anything in this PR) — the sweeper
--     needs to tell them apart because a batch call can legitimately be
--     in-flight for up to ~24h (`wait_until_ended`), while a sync call stuck
--     past ~15 minutes is definitionally orphaned.
--
--   No FK on any column here, same posture as `llm_usage` itself (0044's
--   header: append-only accounting log, recorded loosely, some call sites
--   lack a readily-available account) — and doubly so here, since a pending
--   row is expected to be SHORT-LIVED (seconds to ~15 min for `kind='sync'`)
--   and referential-integrity enforcement would only complicate the exact
--   crash/orphan cases this table exists to make visible.
--
--   ── RLS: matching `llm_usage` on each plane, added here ──
--   Same precedent as `0140_mcp_tasks`/`0139_reef_judgement`: a new table's
--   policy is ENABLEd in its own migration (0083/0009 are already closed,
--   append-only). Anchored identically to `llm_usage`'s own policy on each
--   plane (`0083_consumer_rls` control-side, `migrations_tenant/0009_tenant_rls`
--   tenant-side) — see this file's tenant twin for that side's predicate.
--   Registered in `scripts/check_schema_parity.sh`'s `CONTROL_ONLY_RLS_TABLES`
--   in this same PR, per that list's own instruction.
--
--   `SET LOCAL lock_timeout = '3s'` is defensive precedent-following, not load
--   -bearing here: this is a brand-new, empty table (a `CREATE TABLE` takes no
--   lock on any EXISTING table), so unlike `0141`'s `ALTER TABLE llm_usage`
--   there is nothing else in the database that could be holding a conflicting
--   lock this migration would queue behind.
--
--   Plain (non-`CONCURRENTLY`) `CREATE INDEX`, deliberately unlike
--   `0142`/`0143`/`0144`'s split-file `-- no-transaction` dance: this table is
--   empty at migration time (nothing writes to it until this PR's Rust ships),
--   so there is no live write traffic for a blocking `SHARE` lock to contend
--   with — the entire reason `CONCURRENTLY` exists for `llm_usage`'s indexes
--   does not apply to a table with zero rows and zero prior callers.
--
-- PAIRED WITH: migrations_tenant/0055_llm_usage_pending.sql — the `CREATE
--   TABLE` + `CREATE INDEX` DDL below is BYTE-IDENTICAL to that file (no FK on
--   any column here, so unlike `0140_mcp_tasks`/`0047_mcp_tasks` there isn't
--   even the accounts/users-FK divergence to call out) — only the RLS
--   ENABLE/POLICY block differs, exactly like `0140`/`0047`'s own pairing.
-- Depends on: 0044_llm_usage.sql (the table this is the reserve twin of),
--   0083_consumer_rls.sql (the `account_isolation` RLS pattern this follows).
-- Down:
--   DROP POLICY IF EXISTS account_isolation ON llm_usage_pending;
--   ALTER TABLE llm_usage_pending DISABLE ROW LEVEL SECURITY;
--   DROP TABLE IF EXISTS llm_usage_pending;

SET LOCAL lock_timeout = '3s';

CREATE TABLE IF NOT EXISTS llm_usage_pending (
    id                     UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    account_id             UUID,
    user_id                UUID,
    graph_id               UUID,
    task                   TEXT        NOT NULL,
    provider               TEXT        NOT NULL,
    model                  TEXT        NOT NULL,
    estimated_input_tokens INTEGER     NOT NULL,
    started_at             TIMESTAMPTZ NOT NULL DEFAULT now(),
    kind                   TEXT        NOT NULL DEFAULT 'sync'
                                        CHECK (kind IN ('sync', 'batch'))
);

-- PR6's sweeper predicate: "kind='sync' rows older than ~15 min" / "kind='batch'
-- rows older than ~25h" are both a `kind` equality plus a `started_at` range —
-- leading `kind` makes both queries an index-range scan over a tiny partition
-- of the table instead of a full scan, same convention as `idx_mcp_tasks_expires_at`.
CREATE INDEX IF NOT EXISTS idx_llm_usage_pending_kind_started_at
    ON llm_usage_pending (kind, started_at);

ALTER TABLE llm_usage_pending ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS account_isolation ON llm_usage_pending;
CREATE POLICY account_isolation ON llm_usage_pending
    USING (account_id = current_setting('app.account_id')::uuid);
