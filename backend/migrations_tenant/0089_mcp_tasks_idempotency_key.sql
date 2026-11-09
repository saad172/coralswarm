-- Migration: 0089_mcp_tasks_idempotency_key
-- Description: tenant-tree twin of migrations_control/0221_mcp_tasks_idempotency_key.sql
--   — see that file for the full rationale (security review follow-up on
--   MCP listing-prep, #900 item 5, reworked by the second security review's
--   M3 and CodeRabbit's MAJOR #7: an atomic, UNIQUE-indexed claim scoped by
--   `(owner_user_id, account_id, tool_name, idempotency_key,
--   idempotency_request_hash)` — the request hash is part of the conflict
--   target itself, so a key reused against a genuinely different question
--   just claims its own independent row instead of erroring or colliding).
--
-- PAIRED WITH: migrations_control/0221_mcp_tasks_idempotency_key.sql — the
--   DDL in the two files is IDENTICAL (`scripts/check_schema_parity.sh`).
--
-- Depends on: 0047_mcp_tasks (table this adds columns to).
-- Down:
--   DROP INDEX IF EXISTS idx_mcp_tasks_owner_account_tool_idempotency_key_hash;
--   ALTER TABLE mcp_tasks DROP COLUMN IF EXISTS idempotency_request_hash;
--   ALTER TABLE mcp_tasks DROP COLUMN IF EXISTS idempotency_key;

ALTER TABLE mcp_tasks
  ADD COLUMN IF NOT EXISTS idempotency_key TEXT CHECK (idempotency_key IS NULL OR idempotency_key <> '');

ALTER TABLE mcp_tasks
  ADD COLUMN IF NOT EXISTS idempotency_request_hash TEXT;

DROP INDEX IF EXISTS idx_mcp_tasks_owner_idempotency_key;
DROP INDEX IF EXISTS idx_mcp_tasks_owner_account_tool_idempotency_key;
CREATE UNIQUE INDEX IF NOT EXISTS idx_mcp_tasks_owner_account_tool_idempotency_key_hash
    ON mcp_tasks (owner_user_id, account_id, tool_name, idempotency_key, idempotency_request_hash)
    WHERE idempotency_key IS NOT NULL;
