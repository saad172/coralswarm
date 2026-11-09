-- Outlook compose capture v2 (control-tree twin of tenant 0109).
--
-- 1. Remove the dead native-capture / scheduling-enrollment tables that 0262/0263
--    created for the abandoned PKCE design (PR #1106 never merged; no merged code
--    reads or writes outlook_capture_enrollments / outlook_capture_intents, which
--    were empty in production when this was written). native_capture_auth held ONE
--    expired row (a Teams auth attempt from 2026-10-03, credential expired; the
--    capture it linked lives on the tenant plane and is unaffected). Sign-in is the
--    Microsoft SSO of 0264. Dropping them also removes control-only tables that the
--    schema-parity guard had no entry for. Tenant 0107's outlook_capture_receipts /
--    last_stopped_at barrier are kept.
DROP TABLE IF EXISTS outlook_capture_intents;
DROP TABLE IF EXISTS outlook_capture_enrollments;
DROP TABLE IF EXISTS native_capture_auth;

-- 2. The intent table, identical to the tenant tree.
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
