-- Migration: 0221_mcp_tasks_idempotency_key
-- Description: security review follow-up on MCP listing-prep (#900, item 5,
--   reworked by the second security review's M3 and CodeRabbit's MAJOR #7)
--   — `ask_ocean`'s legacy (`!modern_era`) branch now waits on its own
--   background task for up to `MCP_ASK_SYNC_WAIT_MS` before falling back to
--   an ordinary `task_id` response (item 5 itself). A client whose OWN
--   timeout is shorter than that wait, or that simply retries after giving
--   up, has no way to say "this is the same logical ask" — each retry
--   `INSERT`s a fresh `mcp_tasks` row and spawns a fresh `services::ask::
--   run_ask`, duplicating billed LLM spend (and the `llm_usage` row it
--   writes) for what the caller experiences as one question.
--
--   `idempotency_key` is a caller-supplied opaque string (`ask_ocean`'s new
--   optional `idempotency_key` input). `mcp::tools::read::ask_ocean` uses it
--   to claim a slot ATOMICALLY (`INSERT ... ON CONFLICT ... DO NOTHING
--   RETURNING id`, `mcp::tools::read::claim_ask_task`) rather than a
--   separate SELECT-then-INSERT, which — against the original NON-UNIQUE
--   index this migration first shipped with — let two concurrent same-key
--   calls both miss the SELECT and both spawn a billed synthesis
--   (CodeRabbit re-review, MAJOR #7). The index below is UNIQUE for exactly
--   that reason.
--
--   `account_id` and `tool_name` are part of the uniqueness scope, not just
--   `owner_user_id` (second security review, M3): one user idempotency-keying
--   an `ask_ocean` call against ocean A must not collide with the SAME key
--   string used for ocean B under a different account, or with a future
--   task-based tool that also sets `idempotency_key` for an unrelated
--   purpose.
--
--   `idempotency_request_hash` (M3) is ALSO part of the unique index itself,
--   not a secondary Rust-level check — sha256 of the question + sorted
--   ocean ids + filters (`mcp::tools::read::ask_request_fingerprint`). This
--   is the key design choice that makes "the same key string reused across
--   two genuinely different questions/oceans" behave correctly WITHOUT an
--   error path: since the hash is part of what `ON CONFLICT` matches on,
--   two different questions under the same (account, key) never conflict at
--   all — each simply claims its own row and gets its own task, exactly the
--   "same key across two oceans yields two tasks" behavior M3 asks for.
--   Conversely, whenever a conflict DOES occur, the existing row is
--   GUARANTEED to be the same logical request (same hash) — Postgres's own
--   conflict-target match already proved that, so no additional hash
--   comparison is needed on the reuse path, only a staleness/TTL check.
--
--   The uniqueness window is still bounded, not permanent: a match older
--   than `ASK_IDEMPOTENCY_KEY_LOOKBACK_MINUTES`, or one that is already
--   terminally `failed`/`cancelled`, is treated as EXPIRED and reclaimed
--   (deleted, then the insert retried once) rather than either resurrecting
--   a stale answer or permanently blocking reuse of the key string — see
--   `claim_ask_task`'s own doc comment for why deletion-and-retry was chosen
--   over folding the expiry window into the conflict target itself.
--
-- PAIRED WITH: migrations_tenant/0089_mcp_tasks_idempotency_key.sql — the
--   DDL in the two files is IDENTICAL (`scripts/check_schema_parity.sh`).
--
-- Depends on: 0140_mcp_tasks (table this adds columns to).
-- Down:
--   DROP INDEX IF EXISTS idx_mcp_tasks_owner_account_tool_idempotency_key_hash;
--   ALTER TABLE mcp_tasks DROP COLUMN IF EXISTS idempotency_request_hash;
--   ALTER TABLE mcp_tasks DROP COLUMN IF EXISTS idempotency_key;

ALTER TABLE mcp_tasks
  ADD COLUMN IF NOT EXISTS idempotency_key TEXT CHECK (idempotency_key IS NULL OR idempotency_key <> '');

-- Always populated alongside `idempotency_key` (both set together by
-- `claim_ask_task`'s INSERT, both NULL otherwise) — no separate CHECK
-- enforcing that pairing since nothing else ever writes either column, and
-- it is load-bearing for the unique index below (see the migration header).
ALTER TABLE mcp_tasks
  ADD COLUMN IF NOT EXISTS idempotency_request_hash TEXT;

-- UNIQUE (not a plain lookup index — see the migration header): the
-- concurrency-safety property CodeRabbit #7 requires depends on Postgres
-- itself rejecting a second concurrent INSERT for the same
-- key+request-content, which only a real unique constraint (used as an
-- `ON CONFLICT` target) can guarantee.
DROP INDEX IF EXISTS idx_mcp_tasks_owner_idempotency_key;
DROP INDEX IF EXISTS idx_mcp_tasks_owner_account_tool_idempotency_key;
CREATE UNIQUE INDEX IF NOT EXISTS idx_mcp_tasks_owner_account_tool_idempotency_key_hash
    ON mcp_tasks (owner_user_id, account_id, tool_name, idempotency_key, idempotency_request_hash)
    WHERE idempotency_key IS NOT NULL;
