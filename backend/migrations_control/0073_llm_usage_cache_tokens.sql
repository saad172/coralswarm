-- Migration: 0073_llm_usage_cache_tokens
-- Description: Anthropic prompt caching (audit H6-subset / `system_field()` in
--              `backend/src/llm/anthropic.rs`) is already applied on the request
--              side, but `llm_usage` has no way to tell whether a call actually
--              hit the cache. Anthropic's `usage` object returns
--              `cache_creation_input_tokens` (tokens written to the cache on a
--              miss) and `cache_read_input_tokens` (tokens served from the cache
--              on a hit) on every response; this adds columns to persist both so
--              cache-hit-rate can be queried directly from CoralSwarm's own data
--              rather than relying on Anthropic's console. Non-Anthropic
--              providers (OpenAI, Ollama) and any pre-migration rows simply
--              record 0 in both columns.
-- Depends on: 0044_llm_usage.sql (creates llm_usage).
-- Down:
--   ALTER TABLE llm_usage DROP COLUMN IF EXISTS cache_creation_input_tokens;
--   ALTER TABLE llm_usage DROP COLUMN IF EXISTS cache_read_input_tokens;

ALTER TABLE llm_usage
    ADD COLUMN IF NOT EXISTS cache_creation_input_tokens BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS cache_read_input_tokens     BIGINT NOT NULL DEFAULT 0;
