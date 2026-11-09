-- Migration: 0044_llm_usage
-- Description: Per-call LLM usage / cost observability (audit S5). Every background
--              LLM call (extraction, relationship extraction, entity resolution,
--              reef digest) records the task, model, and token counts so cost can
--              be attributed per tenant (account/ocean) at scale. Synthesis already
--              logs to query_history; this generalizes the same accounting to the
--              non-interactive jobs. Recording is best-effort: a failed insert
--              never fails the originating job.
-- Depends on: nothing (account_id / graph_id are recorded loosely, no FKs, since
--              this is an append-only accounting log and some call sites lack a
--              readily-available account).
-- Down:
--   DROP TABLE IF EXISTS llm_usage;

CREATE TABLE IF NOT EXISTS llm_usage (
    id            UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    account_id    UUID,
    graph_id      UUID,
    task          TEXT        NOT NULL,
    model         TEXT        NOT NULL,
    input_tokens  INTEGER     NOT NULL DEFAULT 0,
    output_tokens INTEGER     NOT NULL DEFAULT 0,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Cost-attribution scans: "all LLM usage for this account over a time window".
CREATE INDEX IF NOT EXISTS idx_llm_usage_account_created
    ON llm_usage (account_id, created_at);
