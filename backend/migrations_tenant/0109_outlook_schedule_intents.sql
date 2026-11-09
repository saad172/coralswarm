-- Outlook compose capture v2: one durable intent per "capture the meeting I am
-- scheduling" click. The intent and the teams_meeting_captures row it governs live
-- in the SAME database, so activation, follow-a-move and Stop are single
-- transactions (no cross-database reconciliation is needed).
--
-- The pane writes `nonce` into an Outlook CustomProperty on the unsaved draft. The
-- tenant Teams worker later finds the saved event by that nonce, then tracks it by
-- its IMMUTABLE Graph id. A recurring draft saves as a seriesMaster and the intent
-- covers every occurrence until Stop / series end / cancellation.
--
-- Insert-once: `nonce` is UNIQUE. Terminal states (stopped / expired / error) are
-- never left again; the worker only ever claims pending_save / active rows.
CREATE TABLE outlook_schedule_intents (
 id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
 account_id UUID NOT NULL,
 user_id UUID NOT NULL,
 nonce UUID NOT NULL UNIQUE DEFAULT gen_random_uuid(),
 -- Microsoft identity the intent was created under; the worker refuses if it changed.
 tenant_id TEXT NOT NULL CHECK (tenant_id <> ''),
 aad_id TEXT NOT NULL CHECK (aad_id <> ''),
 -- The member-private ocean the transcripts go to (resolved at creation).
 graph_id UUID NOT NULL,
 status TEXT NOT NULL DEFAULT 'pending_save'
  CHECK (status IN ('pending_save','active','stopped','expired','error')),
 error_code TEXT CHECK (error_code IS NULL OR length(error_code) <= 64),
 created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
 updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
 expires_at TIMESTAMPTZ NOT NULL DEFAULT now() + interval '24 hours',
 next_check_at TIMESTAMPTZ NOT NULL DEFAULT now(),
 -- Immutable Graph id of the event (seriesMaster id for a series).
 event_id TEXT,
 recurring BOOLEAN NOT NULL DEFAULT false,
 join_url TEXT,
 -- Graph onlineMeeting id of the Teams meeting currently followed.
 meeting_id TEXT,
 occurrence_start TIMESTAMPTZ,
 occurrence_end TIMESTAMPTZ,
 -- teams_meeting_captures.id of the capture this intent currently owns.
 capture_id UUID,
 auto_transcription TEXT CHECK (auto_transcription IS NULL OR auto_transcription IN ('enabled','manual')),
 manual_reason TEXT CHECK (manual_reason IS NULL OR manual_reason IN
  ('recurring','not_organizer','missing_scope','policy','already_started','unknown')),
 stopped_at TIMESTAMPTZ,
 CHECK ((occurrence_start IS NULL AND occurrence_end IS NULL) OR occurrence_end > occurrence_start),
 CHECK (status <> 'active' OR event_id IS NOT NULL)
);
CREATE INDEX outlook_schedule_intents_due ON outlook_schedule_intents(account_id, user_id, next_check_at)
 WHERE status IN ('pending_save','active');
CREATE INDEX outlook_schedule_intents_event ON outlook_schedule_intents(account_id, user_id, event_id)
 WHERE event_id IS NOT NULL;
ALTER TABLE outlook_schedule_intents ENABLE ROW LEVEL SECURITY;
ALTER TABLE outlook_schedule_intents FORCE ROW LEVEL SECURITY;
CREATE POLICY outlook_schedule_intent_owner ON outlook_schedule_intents
 USING (account_id=nullif(current_setting('app.account_id',true),'')::uuid
        AND user_id=nullif(current_setting('app.user_id',true),'')::uuid)
 WITH CHECK (account_id=nullif(current_setting('app.account_id',true),'')::uuid
        AND user_id=nullif(current_setting('app.user_id',true),'')::uuid);
