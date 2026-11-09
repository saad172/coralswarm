-- Migration 0207: google_meet_ingested_conferences (gsuite Train G3 PR 2 —
--   Meet ingest, `docs/connectors/google-workspace.md` §11).
--
--   A per-(reader account, conference record) ledger, one row per Meet
--   conference the sweep has already fully processed for a given connected
--   member's own `conferenceRecords.list` visibility. Two jobs:
--
--   1. **Cheap re-processing dedup.** `workers::google_meet_sweep` lists
--      conference records past `google_meet_sync_state.last_synced_end_time`
--      every tick; without this ledger, a conference record sitting exactly
--      AT the watermark boundary (or revisited because the watermark hasn't
--      advanced yet this tick) would re-fetch its transcript/participants on
--      every pass. The real content-level idempotency is `atoms`' own
--      `(graph_id, source_type, source_id) ON CONFLICT DO NOTHING` (see
--      `connectors::google_meet`'s own module doc) — this ledger exists to
--      avoid the wasted Meet API calls, not to be the correctness boundary.
--   2. **Observability.** `attendee_count`/`resolved_count` record, per
--      meeting, how many participants existed vs. how many resolved to a
--      CoralSwarm identity and actually received the transcript — the
--      concrete numbers behind the founder's "unresolved attendee gets
--      nothing, silently" decision, so a support/debug query can answer
--      "why didn't X get this meeting's notes" without re-deriving it from
--      Meet's API by hand. `transcript_id` is empty when a meeting had no
--      transcript generated at all (recorded so the sweep does not keep
--      re-checking a meeting that will never have one).
--
--   Deliberately NOT a per-attendee row: the fan-out destination (which
--   CoralSwarm account/ocean received a copy) is derivable from `atoms`
--   itself (one row per `graph_id` under `source_type = 'meet'`,
--   `source_id = 'meet:{conference_record_id}'`) — this ledger's job is
--   sweep-side dedup and aggregate counts, not a second copy of routing
--   state atoms already record.
--
--   UNIQUE (account_id, conference_record_id) — `account_id` here is the
--   READER's account (whoever's `google_meet` token discovered this
--   meeting), not any attendee's; two different connected members could in
--   principle both be attendees of, and therefore both discover, the SAME
--   physical meeting as its own organizer only ever reaches
--   `conferenceRecords.list` results for spaces IT created (see this train's
--   PR body for the citation), so in practice this is one row per meeting,
--   keyed by its organizer's account — but the unique key stays
--   account-scoped rather than global on `conference_record_id` for the same
--   reason `google_watch_channels`' own key is account-scoped: this table
--   answers "has THIS account's sweep already handled this", not "has
--   anyone, ever".
--
-- Depends on: 0206_google_meet_sync_state (same train, same table family).
-- Down:
--   DROP POLICY IF EXISTS account_isolation ON google_meet_ingested_conferences;
--   DROP TABLE IF EXISTS google_meet_ingested_conferences;

CREATE TABLE IF NOT EXISTS google_meet_ingested_conferences (
    id                    UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    account_id            UUID        NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    owner_user_id         UUID        NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    conference_record_id  TEXT        NOT NULL,
    transcript_id         TEXT        NOT NULL DEFAULT '',
    attendee_count        INTEGER     NOT NULL DEFAULT 0,
    resolved_count        INTEGER     NOT NULL DEFAULT 0,
    processed_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at            TIMESTAMPTZ NOT NULL DEFAULT now(),

    UNIQUE (account_id, conference_record_id)
);

ALTER TABLE google_meet_ingested_conferences ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS account_isolation ON google_meet_ingested_conferences;
CREATE POLICY account_isolation ON google_meet_ingested_conferences
    USING (account_id = current_setting('app.account_id')::uuid)
    WITH CHECK (account_id = current_setting('app.account_id')::uuid);
