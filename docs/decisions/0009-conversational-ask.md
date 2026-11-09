# ADR-0009: Conversational Ask (persisted chat) — and an amendment to ADR-0007 D9

- **Status:** Accepted
- **Date:** 2026-06-19
- **Supersedes (in part):** ADR-0007 §D9 "server-blind private ocean" — for
  conversation transcripts only (see "Consequences").

## Context

The Ask page was single-shot: one question → one cited answer → a flat "recent
asks" list. We want a ChatGPT/Claude-style experience: multi-turn back-and-forth
with memory, and a sidebar of named, resumable conversations.

The design was settled in a requirements interview. The decisions:

1. **Persisted conversations**, not ephemeral threads — a sidebar of named,
   resumable conversations is the product, not just in-session memory.
2. A conversation is **scoped to exactly one ocean** (`graph_id`); switching
   oceans starts a fresh thread.
3. **Server-side storage for every ocean tier**, including the private Primary
   ocean. Uniform schema, cross-device, no per-tier branching.
4. **Server reconstructs memory** from `conversation_id`: the client sends only
   the new question + conversation id; the backend loads prior turns and builds
   the synthesis history. The client never ships a `history[]` array.
5. **Full render payload per assistant turn** (answer text + citations + sources
   + mode + timing) so a reopened conversation re-renders identically.
6. **Streaming answers** (SSE), token-by-token, with citations resolved at the
   end. The existing blocking `POST /v1/queries` is kept for non-chat callers
   (the ⌘K palette).
7. **Titles**: instant truncated-first-question, upgraded asynchronously by a
   Haiku call; manual rename always available.
8. **Lifecycle**: new chat (lazy creation on first send — no orphan rows),
   rename, delete (hard, with confirm), search (title + message text), pin.
9. **Visibility**: private to the creator; the owner may **share read-only** to
   other members of the same ocean (only the owner can append turns).
10. **Surfaces**: built for web/cloud; signed-in desktop inherits via the shared
    Next.js bundle + same API. Offline/free desktop falls back to ephemeral
    threads (no sidebar).
11. **Layout**: Variant A — a left conversation sidebar + center thread +
    bottom-pinned composer (the classic chat shape).

## Decision

### Data model (migration `0035_conversations.sql`, mirrored into the libsql `SCHEMA`)

- `conversations(id, account_id, graph_id, user_id, title, pinned, shared,
  created_at, updated_at)` — owned by `user_id`, scoped to `graph_id`.
- `conversation_messages(id, conversation_id, seq, role, text, citations jsonb,
  sources jsonb, mode, timing jsonb, created_at)` — `UNIQUE(conversation_id,
  seq)`; assistant turns carry the full render payload.

### API

- `GET /v1/conversations?graph_id=[&q=]` → `{ conversations, shared }` (own +
  shared-by-others; `q` searches own title/message text).
- `GET /v1/conversations/{id}` → conversation + ordered messages.
- `PATCH /v1/conversations/{id}` → rename / pin / share (owner only).
- `DELETE /v1/conversations/{id}` → hard delete, messages cascade (owner only).
- `POST /v1/queries/stream` (SSE) → the conversational answer path. Validates +
  authorizes like `POST /v1/queries`, lazily creates the conversation when no
  `conversation_id` is given, persists the user turn, runs retrieval, then:
  - **synthesized**: `meta` → `delta`* → `final`. The streaming synthesis prompt
    emits prose with `[n]` markers, then a `<<<CITES>>>` sentinel, then a JSON
    array of cited atom_ids. The server streams only the pre-sentinel prose to
    the client and parses the manifest at the end — reproducing the existing
    `{answer, citations}` contract while keeping the visible stream clean.
  - **retrieval-only / egress-blocked**: `meta` → `final` with ranked sources
    (no prose to stream).
  - The assistant turn is **persisted server-side on completion regardless of
    client disconnect** (the persistence happens inside the stream generator).

### Authz

Read: owner, or `shared` AND the ocean is reachable
(`graph_reachable_by_user`). Mutations and appends: owner only.

## Consequences

### Amendment to ADR-0007 §D9 (server-blind private ocean)

ADR-0007 D9 promised the private Primary ocean's content is end-to-end encrypted
and **the server is blind to it** (retrieval runs in-browser). This ADR
**deliberately narrows that guarantee**: conversation transcripts — the
questions a user types and the synthesized answers — are stored server-side in
Postgres for **all** ocean tiers, including the private Primary ocean. The
server can therefore read private-ocean chat transcripts.

This was chosen knowingly, with the trade-off stated, to ship the
sidebar-of-conversations model uniformly without first building a client-side
encrypted conversation store. Retrieval over the private ocean remains in-browser
and server-blind (unchanged); only the **conversation transcript** is now
server-visible.

A future ADR may restore E2E for private-ocean conversations by persisting them
in the client-side encrypted store (IndexedDB/libsql) instead of Postgres. Until
then, this reversal stands and is the source of truth.

### Other

- The old "recent asks" UI (from `query_history`) is removed; `query_history` is
  retained server-side for analytics and is still written by both query paths.
- `sqlx` gains the `json` feature (workspace `Cargo.toml`) for `jsonb` ↔
  `serde_json::Value`.
- Branch: `cs-ask-chat-a`.

## Follow-on: conversational quality + inline action cards

Built after the initial cut, on the same branch:

**Answer quality** — the streaming synthesis prompt now answers naturally and
briefly: shortest-correct by default, length tracks the question, "who is X"
leads with the defining fact + a one-line offer-more rather than dumping every
fact. Conversation history is first-class (meta/recall questions answer from it,
no citations). On follow-up turns, `condense_query()` rewrites recent turns + the
new message into a standalone retrieval query (so "yes/tell me more" and bare
references like "whose her parents" retrieve the right corals); gated to
follow-ups, LLM-present, egress-allowed.

**Inline action cards** — a new SSE `action` event lets the assistant *propose*
write-actions the user confirms with inline buttons; the LLM never writes data.
- **Capture**: `classify_capture()` detects a durable fact the user stated and
  proposes a "save to ocean" card → on confirm, ingests an atom (provenance:
  you · chat · now) → extraction links it to reefs.
- **Reviews**: pending decision candidates + possible-duplicate entity reefs
  (`merge_suggestion`) surface as cards — contextual to the turn's top reefs
  (the top `ACTION_RELEVANCE_K` reranked atoms' reefs) or all of them on "what
  needs reviewing". Buttons call the existing confirm/reject/merge/dismiss
  endpoints. (ADR-0008 relationship-edge reviews are out of scope here — not on
  this branch.)
