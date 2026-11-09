-- Migration 0078: google_meet_ingested_conferences (tenant twin of
--   migrations_control/0207_google_meet_ingested_conferences.sql — gsuite
--   Train G3 PR 2, same schema-parity twin pattern as 0076/0204).
--
--   Byte-identical column set; see the control migration's header for the
--   full rationale. `account_id`/`owner_user_id` are bare `UUID`, no FKs,
--   same tenant-tree convention as every other twin in this train.
--
--   ── RLS: deliberately NOT covered ── same reasoning as
--   `migrations_tenant/0077_google_meet_sync_state.sql`: no `graph_id`/ocean
--   anchor, and an org tenant database holds exactly one account (ADR-0007)
--   so there is nothing for an `account_isolation`-style policy to separate
--   here either. Never written on a genuine org tenant pool in this PR for
--   the same reason 0076 documents (`google_meet` OAuth cannot complete
--   under an org account).
--
-- Depends on: 0077_google_meet_sync_state (same train, same table family).
-- Down:
--   DROP TABLE IF EXISTS google_meet_ingested_conferences;

CREATE TABLE IF NOT EXISTS google_meet_ingested_conferences (
    id                    UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    account_id            UUID        NOT NULL,
    owner_user_id         UUID        NOT NULL,
    conference_record_id  TEXT        NOT NULL,
    transcript_id         TEXT        NOT NULL DEFAULT '',
    attendee_count        INTEGER     NOT NULL DEFAULT 0,
    resolved_count        INTEGER     NOT NULL DEFAULT 0,
    processed_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at            TIMESTAMPTZ NOT NULL DEFAULT now(),

    UNIQUE (account_id, conference_record_id)
);
