-- Migration 0222: `follow_replies` on the two opt-in watched-thread tables
--   (gmail-options backend train, PR 1 of 4 — backend → Gmail add-on card →
--   web list → Outlook add-in).
--
--   Locked product decision (founder, gmail-options design doc): an opt-in
--   save now carries two orthogonal knobs — `scope` (whole thread vs. just
--   the selected message; not a schema concern, it only decides WHAT gets
--   fetched) and `follow` (keep auto-saving new replies; this IS a schema
--   concern). A watched row is written for EVERY opt-in save, always —
--   including `follow=false` — because deletion propagation
--   (`connectors::google_gmail::propagate_deletions`,
--   `connectors::microsoft`'s deletion-notification path) resolves the
--   target ocean FROM this row's `graph_id`; no row means an upstream
--   deletion can never be propagated again (Gmail's `history.list`/Graph's
--   change feed each report a given deletion exactly once). Unfollowing
--   must therefore flip a flag, never delete the row.
--
--   `DEFAULT TRUE`: every row that exists before this migration was written
--   by the ONLY code path that existed before this PR — the "ingest this
--   thread and keep following it" opt-in — so `TRUE` is the byte-identical
--   backfill for every pre-existing row, not a guess. New rows written by
--   the plain (both-knobs-omitted) ingest path stay `TRUE` too — the
--   locked "byte-identical to today for callers that omit both" contract.
--
--   Consulted by:
--     * `connectors::google_gmail::watched_thread_ids_among` (gates which
--       `history.list`-reported new messages get auto-captured as replies —
--       `fetch_pending`'s own filter, since it calls this function).
--     * `connectors::microsoft::watched_conversation_graph` (the Graph
--       webhook's reply-auto-capture lookup, `routes::graph_webhook`).
--     * `connectors::microsoft::watched_conversations_for_account` (the
--       backstop resync's re-list-and-ingest work set, and the
--       has-anything-to-resync short-circuit — `workers::
--       graph_subscriptions`).
--   Deliberately NOT consulted by deletion propagation (both connectors'
--   `graph_id` lookups stay flag-blind by design — see those functions' own
--   doc comments) or by the resumable-backfill-continuation queries (an
--   incomplete backfill is finishing the content the user already,
--   synchronously asked for at save time — a bookkeeping concern
--   independent of whether they also want to keep following it).
--
--   Paired with `migrations_tenant/0090_watched_follow_replies.sql` — both
--   `gmail_watched_threads` and `outlook_watched_conversations` are shared
--   content tables that exist in both trees (control: `0186`/`0035`;
--   tenant: `0066`/`0001`), same "a column added to only one tree fails
--   `scripts/check_schema_parity.sh`" reasoning `0212_atoms_title_kind.sql`
--   documents for `atoms.title_kind`.
--
-- Depends on: 0186_google_gmail_watch_owner_and_threads.sql, 0035_outlook_thread_watch.sql.
-- Down:
--   ALTER TABLE gmail_watched_threads DROP COLUMN IF EXISTS follow_replies;
--   ALTER TABLE outlook_watched_conversations DROP COLUMN IF EXISTS follow_replies;

ALTER TABLE gmail_watched_threads
    ADD COLUMN IF NOT EXISTS follow_replies BOOLEAN NOT NULL DEFAULT TRUE;

ALTER TABLE outlook_watched_conversations
    ADD COLUMN IF NOT EXISTS follow_replies BOOLEAN NOT NULL DEFAULT TRUE;
