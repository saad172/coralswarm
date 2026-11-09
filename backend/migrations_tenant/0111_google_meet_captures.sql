-- Google Meet per-meeting opt-in (founder decision 2026-10-06: "like Microsoft Teams,
-- where people have to allow individual meetings to be ingested, not all
-- automatically"). Tenant-plane twin of control 0269; the two DDLs are identical.
--
-- One row = ONE member allowed ONE specific meeting. It is that member's consent for
-- THEIR OWN copy of the transcript only; it never authorises a copy for any other
-- attendee (each attendee approves their own row). Without a matching row the Meet
-- sweep ingests nothing. Shape, RLS and naming mirror teams_meeting_captures (0260).
--
-- Meeting identity (at least one is required):
--   conference_record_id  'conferenceRecords/{id}' - one concrete meeting occurrence.
--   space_name            'spaces/{id}'            - pre-approval before/while it runs.
--   meeting_code          'abc-defg-hij'           - same, when only the code is known.
-- A pre-approval (no conference_record_id yet) covers the FIRST conference record in
-- that space that ENDS after the approval was given; the sweep then binds the record
-- id onto the row and marks it ingested. A recurring meeting needs a fresh approval
-- per occurrence. Google keeps transcripts ~30 days, so an unused row stops matching
-- at expires_at (default 30 days) - bounded, never open-ended consent.
--
-- No FKs: the two planes hold different parent tables (users/accounts are control-only),
-- and the schema-parity guard requires identical DDL; same choice as 0260.
CREATE TABLE google_meet_captures (
 id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
 account_id UUID NOT NULL,
 owner_user_id UUID NOT NULL,
 conference_record_id TEXT CHECK (conference_record_id ~ '^conferenceRecords/[A-Za-z0-9_-]{1,128}$'),
 space_name TEXT CHECK (space_name ~ '^spaces/[A-Za-z0-9_-]{1,128}$'),
 meeting_code TEXT CHECK (meeting_code ~ '^[a-z]{3}-[a-z]{4}-[a-z]{3}$'),
 status TEXT NOT NULL DEFAULT 'allowed' CHECK (status IN ('allowed','ingested','revoked')),
 -- Set when the capture can never produce a copy (nothing to retry): the row stays
 -- 'allowed' for the audit trail but the sweep stops re-checking it.
 terminal_reason TEXT CHECK (terminal_reason IN ('no_transcript','empty_transcript','not_ingestable')),
 created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
 updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
 expires_at TIMESTAMPTZ NOT NULL DEFAULT now() + interval '30 days',
 ingested_at TIMESTAMPTZ,
 revoked_at TIMESTAMPTZ,
 CHECK (conference_record_id IS NOT NULL OR space_name IS NOT NULL OR meeting_code IS NOT NULL),
 CHECK (status <> 'ingested' OR (ingested_at IS NOT NULL AND conference_record_id IS NOT NULL)),
 CHECK (status <> 'revoked' OR revoked_at IS NOT NULL)
);
CREATE UNIQUE INDEX google_meet_captures_record ON google_meet_captures(account_id, owner_user_id, conference_record_id)
 WHERE conference_record_id IS NOT NULL;
CREATE UNIQUE INDEX google_meet_captures_open_space ON google_meet_captures(account_id, owner_user_id, space_name)
 WHERE status = 'allowed' AND conference_record_id IS NULL AND space_name IS NOT NULL;
CREATE UNIQUE INDEX google_meet_captures_open_code ON google_meet_captures(account_id, owner_user_id, meeting_code)
 WHERE status = 'allowed' AND conference_record_id IS NULL AND meeting_code IS NOT NULL;
CREATE INDEX google_meet_captures_open ON google_meet_captures(account_id, owner_user_id, expires_at)
 WHERE status = 'allowed';
ALTER TABLE google_meet_captures ENABLE ROW LEVEL SECURITY;
ALTER TABLE google_meet_captures FORCE ROW LEVEL SECURITY;
CREATE POLICY google_meet_capture_owner ON google_meet_captures
 USING (account_id = nullif(current_setting('app.account_id',true),'')::uuid
        AND owner_user_id = nullif(current_setting('app.user_id',true),'')::uuid)
 WITH CHECK (account_id = nullif(current_setting('app.account_id',true),'')::uuid
        AND owner_user_id = nullif(current_setting('app.user_id',true),'')::uuid);
