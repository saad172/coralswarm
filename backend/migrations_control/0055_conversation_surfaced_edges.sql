-- Suppress already-shown relationship-confirm cards (ADR-0008 decision 9 follow-up).
--
-- Inline "confirm this relationship?" cards are derived statelessly each turn from
-- the inferred-unconfirmed edges touching the retrieved reefs, with no record of
-- what was already shown — so an unacknowledged edge whose reef keeps getting
-- retrieved re-prompts on every answer. This table records which edges have been
-- surfaced in a conversation so later turns can suppress the repeats; acting on a
-- card (confirm/reject) still flips the edge status and removes it on its own.
--
-- Depends on: 0041_conversations (conversations)
-- Down:
--   DROP TABLE IF EXISTS conversation_surfaced_edges;
--
-- `edge_id` is intentionally NOT a foreign key to relationship_edge: surfacing is
-- recorded in a single bulk insert seconds after retrieval, so an edge deleted in
-- the meantime would fail the whole batch and silently drop the turn's
-- suppression. A stale edge_id is harmless — it only ever fails to match a live
-- candidate edge — and rows still cascade away when the conversation is deleted.
-- This also keeps the schema identical to the libsql (desktop) mirror.

CREATE TABLE conversation_surfaced_edges (
  conversation_id UUID NOT NULL REFERENCES conversations(id) ON DELETE CASCADE,
  edge_id         UUID NOT NULL,
  PRIMARY KEY (conversation_id, edge_id)
);
