-- Migration 0019: query_history table
-- Stores a summary record for each completed RAG query.  Populated
-- asynchronously (fire-and-forget) after POST /v1/queries succeeds so that
-- the insert never delays the response.

CREATE TABLE query_history (
  id              UUID        NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
  account_id      UUID        NOT NULL REFERENCES accounts(id)  ON DELETE CASCADE,
  graph_id        UUID        NOT NULL REFERENCES graphs(id)    ON DELETE CASCADE,
  user_id         UUID                 REFERENCES users(id)     ON DELETE SET NULL,
  query_text      TEXT        NOT NULL,
  answer_summary  TEXT        NOT NULL DEFAULT '',
  citation_count  INTEGER     NOT NULL DEFAULT 0,
  input_tokens    INTEGER     NOT NULL DEFAULT 0,
  output_tokens   INTEGER     NOT NULL DEFAULT 0,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_query_history_account_created ON query_history(account_id, created_at DESC);
CREATE INDEX idx_query_history_graph            ON query_history(graph_id);
