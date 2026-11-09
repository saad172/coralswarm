-- Migration: 0271_atoms_content_hash
-- Description: `atoms.content_hash` — the SHA-256 (hex) of the full request
--   content an atom's chunks were cut from, persisted at INGEST TIME so a
--   connector that re-fetches an item it already ingested can tell "same
--   thing again" from "the upstream item was EDITED" without reading the
--   (encrypted, R2-resident) body back.
--
-- THE BUG THIS EXISTS FOR (Google Calendar event edits, 2026-10-06)
--   `Store::insert_atoms_scoped` is `ON CONFLICT (graph_id, source_type,
--   source_id) DO NOTHING`, and a Google Calendar event keeps its id for
--   life. So when an already-ingested event was renamed/rescheduled/had its
--   attendees or description changed, the incremental sync fetched the new
--   version, `routes::ingest` hashed it, chunked it, and the INSERT quietly
--   skipped every chunk (`skipped_duplicate_chunks: 1`) — the stored coral
--   kept the OLD details forever. Deletions (`status: "cancelled"`) already
--   worked because they go through `services::deletion`, not the insert.
--
--   `routes::ingest` was ALREADY computing exactly this hash on every
--   request (its `embed:{graph_id}:{content_hash}` job idempotency key) and
--   throwing it away. Persisting it is the cheapest honest signal of "did
--   the content change" there is: one indexed-by-natural-key `SELECT` per
--   re-fetched item, no R2 round trip, no DEK, no decrypt.
--
-- WHAT IS HASHED
--   `routes::ingest::IngestAtomRequest::content` — the WHOLE request body,
--   BEFORE chunking and BEFORE the provenance header is prepended — so every
--   chunk atom of one request carries the SAME hash, and the hash is a
--   function purely of what the connector shaped (for Calendar:
--   `connectors::google_calendar::shape_calendar_item`'s
--   `{summary}\n\n{description}\nLocation: …\nJoin: …`). Participants and
--   `timestamp` are NOT part of the hash; `services::source_reingest`
--   compares those columns directly alongside it.
--
-- NULLABLE, NO BACKFILL, AND WHAT NULL MEANS TO THE READER
--   Plain nullable `ADD COLUMN ... TEXT`, no default — catalog-only on
--   Postgres 11+, no table rewrite (same reasoning as 0248's `symbols`,
--   0116's `excerpt`). There is no way to backfill: the content lives
--   encrypted in R2 (or `content_inline`), and recomputing it for every
--   atom would be a full re-read of every blob. So every atom ingested
--   before this migration has NULL here, and `services::source_reingest`
--   treats NULL as "unknown — assume changed": a pre-migration atom whose
--   source is re-fetched is replaced ONCE (and then carries a hash). For
--   Calendar that is the right call: an incremental `syncToken` pass only
--   ever reports an event Google says CHANGED, so the replacement is the
--   edit the user actually made. The one-time churn case is a full restart
--   after a `410 Gone` sync token, which re-lists every event — rare, and
--   it converges (every atom gets a hash on that pass).
--
-- NOT A UNIQUE / CONFLICT KEY
--   Deliberately NOT part of the `(graph_id, source_type, source_id)`
--   unique index and NOT a second `ON CONFLICT` arm: the identity of a
--   coral is still its source id. This column only lets a caller DECIDE to
--   retract-and-re-ingest; it never changes what the INSERT does.
--
-- Paired with `migrations_tenant/0112_atoms_content_hash.sql`. `atoms` is
-- one of the shared content tables, so a column added to only one tree
-- fails `scripts/check_schema_parity.sh` (and its CI job) by design.
--
-- Down:
--   ALTER TABLE atoms DROP COLUMN IF EXISTS content_hash;

ALTER TABLE atoms ADD COLUMN IF NOT EXISTS content_hash TEXT;

COMMENT ON COLUMN atoms.content_hash IS
    'Lowercase hex SHA-256 of the FULL ingest request content this chunk '
    'was cut from (routes::ingest::IngestAtomRequest::content, before '
    'chunking and before the provenance header) — identical across every '
    'chunk of one request. Persisted at ingest time by routes::ingest via '
    'Store::insert_atoms_scoped. Read by services::source_reingest to tell '
    'a re-fetched, unchanged upstream item (skip) from an EDITED one '
    '(retract + re-ingest). NULL = ingested before migration 0271/0112, '
    'content unknown — readers treat NULL as "assume changed". Never a '
    'conflict/identity key.';
