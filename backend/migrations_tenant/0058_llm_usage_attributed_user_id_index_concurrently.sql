-- no-transaction
-- 0058_llm_usage_attributed_user_id_index_concurrently.sql — paired half of
-- `migrations_control/0158_llm_usage_attributed_user_id_index_concurrently.sql`.
-- See that migration's header for why `CREATE INDEX CONCURRENTLY` must be
-- alone in a `-- no-transaction` file, why this index leads with
-- `(account_id, attributed_user_id, created_at)`, why it is partial, and the
-- INVALID-index caveat if the build fails.
--
-- The DDL below is BYTE-IDENTICAL to the control migration (only the header
-- prose differs) — `llm_usage` is a SHARED table, present in both trees and
-- NOT in `scripts/check_schema_parity.sh`'s `CONTROL_TABLES` roster, so its
-- schema-only `pg_dump` diff fails on any divergence here.
--
-- PAIRED WITH: backend/migrations_control/0158_llm_usage_attributed_user_id_index_concurrently.sql
-- Depends on: 0057_llm_usage_attributed_user_id.sql (creates attributed_user_id
--   on the tenant plane).
-- Down: see control 0158.

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_llm_usage_attributed_user
    ON llm_usage (account_id, attributed_user_id, created_at)
    WHERE attributed_user_id IS NOT NULL;
