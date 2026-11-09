-- Ask v2 (ADR-0011 story 2): a conversation now carries a SCOPE DESCRIPTOR
-- { mode: shared | private, ocean_ids: [...] } instead of a single ocean, and
-- each turn snapshots the scope actually used (so history stays truthful when
-- the scope changes mid-thread). This supersedes ADR-0009 §2.
--
-- Back-compat: `conversations.graph_id` is kept as the conversation's PRIMARY
-- ocean (account ownership, history, action cards still hang off it). The full
-- scope lives in the new `conversation_oceans` join. A conversation is
-- mode-locked for its lifetime.

-- ── Mode (shared | private), mode-locked ────────────────────────────────────
ALTER TABLE conversations
  ADD COLUMN mode TEXT NOT NULL DEFAULT 'shared'
  CHECK (mode IN ('shared', 'private'));

-- Backfill: a conversation scoped to the user's primary (private) ocean is a
-- private-mode conversation; everything else is shared-mode.
UPDATE conversations c
   SET mode = 'private'
  FROM graphs g
 WHERE c.graph_id = g.id
   AND g.is_primary = true;

-- ── Ocean set (scope descriptor) ────────────────────────────────────────────
CREATE TABLE conversation_oceans (
  conversation_id UUID NOT NULL REFERENCES conversations(id) ON DELETE CASCADE,
  graph_id        UUID NOT NULL REFERENCES graphs(id)        ON DELETE CASCADE,
  PRIMARY KEY (conversation_id, graph_id)
);

-- Reverse lookup: "which conversations touch this ocean".
CREATE INDEX idx_conversation_oceans_graph ON conversation_oceans(graph_id);

-- Backfill: every existing single-ocean conversation gets exactly its graph_id.
INSERT INTO conversation_oceans (conversation_id, graph_id)
  SELECT id, graph_id FROM conversations
  ON CONFLICT DO NOTHING;

-- ── Per-turn scope snapshot ─────────────────────────────────────────────────
-- The set of oceans actually queried for THIS turn (JSON array of uuid strings).
ALTER TABLE conversation_messages
  ADD COLUMN scope_graph_ids JSONB NOT NULL DEFAULT '[]'::jsonb;

-- Backfill existing turns with their conversation's single ocean.
UPDATE conversation_messages m
   SET scope_graph_ids = to_jsonb(ARRAY[c.graph_id])
  FROM conversations c
 WHERE m.conversation_id = c.id;
