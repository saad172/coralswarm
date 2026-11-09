-- Conversational Ask (cs-ask-chat-a): persisted multi-turn chat conversations.
--
-- A conversation is scoped to exactly one ocean (graph) and owned by its
-- creator. Conversations are private to the owner by default; setting
-- `shared = true` exposes a read-only copy to other members of the same ocean
-- (only the owner may append turns).
--
-- NOTE (ADR-0007 amendment): unlike retrieval, conversation transcripts are
-- stored server-side for EVERY ocean tier, including the private Primary ocean.
-- This is a deliberate reversal of the "server-blind private ocean" guarantee
-- (ADR-0007 D9) accepted to ship the ChatGPT-style sidebar uniformly. See
-- docs/architecture/decisions/0007-*.md amendment.

CREATE TABLE conversations (
  id          UUID        NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
  account_id  UUID        NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
  graph_id    UUID        NOT NULL REFERENCES graphs(id)   ON DELETE CASCADE,
  user_id     UUID        NOT NULL REFERENCES users(id)    ON DELETE CASCADE,
  title       TEXT        NOT NULL DEFAULT '',
  pinned      BOOLEAN     NOT NULL DEFAULT false,
  shared      BOOLEAN     NOT NULL DEFAULT false,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Own-conversation listing (sidebar): newest activity first, pinned hoisted in SQL.
CREATE INDEX idx_conversations_owner ON conversations(graph_id, user_id, updated_at DESC);
-- Shared-conversation listing for other members of an ocean.
CREATE INDEX idx_conversations_shared ON conversations(graph_id, updated_at DESC) WHERE shared;

CREATE TABLE conversation_messages (
  id               UUID        NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
  conversation_id  UUID        NOT NULL REFERENCES conversations(id) ON DELETE CASCADE,
  seq              INTEGER     NOT NULL,
  role             TEXT        NOT NULL CHECK (role IN ('user','assistant')),
  text             TEXT        NOT NULL DEFAULT '',
  -- assistant turns carry the full render payload so a reopened conversation
  -- re-renders identically (cite chips, source rows, timing credit).
  citations        JSONB       NOT NULL DEFAULT '[]'::jsonb,  -- ordered atom_id strings
  sources          JSONB       NOT NULL DEFAULT '[]'::jsonb,  -- retrieval-only mode source objects
  mode             TEXT,                                       -- 'synthesized' | 'retrieval_only' | null
  timing           JSONB       NOT NULL DEFAULT '{}'::jsonb,   -- { embed_ms, retrieve_ms, synthesize_ms? }
  created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (conversation_id, seq)
);

CREATE INDEX idx_conversation_messages_thread ON conversation_messages(conversation_id, seq);
