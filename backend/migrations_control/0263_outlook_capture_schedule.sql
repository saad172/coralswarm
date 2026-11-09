-- Explicit Outlook scheduling enrollment, not blanket calendar capture authority.
CREATE TABLE outlook_capture_enrollments (
 id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
 user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
 account_id UUID NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
 clerk_user_id TEXT NOT NULL, org_id TEXT NOT NULL,
 microsoft_tenant_id TEXT NOT NULL, microsoft_aad_id TEXT NOT NULL,
 created_at TIMESTAMPTZ NOT NULL DEFAULT now(), revoked_at TIMESTAMPTZ
);
CREATE UNIQUE INDEX outlook_capture_enrollment_active ON outlook_capture_enrollments(user_id,account_id) WHERE revoked_at IS NULL;
CREATE TABLE outlook_capture_intents (
 id UUID PRIMARY KEY DEFAULT gen_random_uuid(), nonce UUID NOT NULL UNIQUE DEFAULT gen_random_uuid(),
 origin TEXT NOT NULL, code_challenge TEXT NOT NULL,
 created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
 attempt_expires_at TIMESTAMPTZ NOT NULL DEFAULT now()+interval '5 minutes',
 enrollment_id UUID REFERENCES outlook_capture_enrollments(id) ON DELETE CASCADE,
 resume_nonce UUID, target_intent_id UUID REFERENCES outlook_capture_intents(id),
 organization_name TEXT, display_name TEXT,
 session_id TEXT, code_hash TEXT UNIQUE, code_expires_at TIMESTAMPTZ,
 credential_hash TEXT UNIQUE, credential_expires_at TIMESTAMPTZ, credential_revoked_at TIMESTAMPTZ,
 exchanged_at TIMESTAMPTZ, expires_at TIMESTAMPTZ,
 status TEXT NOT NULL DEFAULT 'awaiting_auth' CHECK(status IN ('awaiting_auth','awaiting_exchange','pending_save','resolving','scheduled','cancelled','expired','error')),
 next_resolve_at TIMESTAMPTZ NOT NULL DEFAULT now(), error_code TEXT,
 event_id TEXT, meeting_id TEXT, occurrence_start TIMESTAMPTZ, occurrence_end TIMESTAMPTZ, capture_id UUID,
 CHECK((occurrence_start IS NULL AND occurrence_end IS NULL) OR occurrence_end>occurrence_start)
);
CREATE INDEX outlook_capture_pending ON outlook_capture_intents(next_resolve_at) WHERE status IN ('pending_save','resolving');

-- Legacy single-database parity with tenant migration 0107.
-- Retain the Stop barrier after a different explicit action re-enables capture.
ALTER TABLE teams_meeting_captures ADD COLUMN last_stopped_at TIMESTAMPTZ;
UPDATE teams_meeting_captures SET last_stopped_at=revoked_at WHERE revoked_at IS NOT NULL;
CREATE FUNCTION retain_meeting_capture_stop() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NEW.revoked_at IS NOT NULL AND NEW.revoked_at IS DISTINCT FROM OLD.revoked_at THEN
  NEW.last_stopped_at=GREATEST(OLD.last_stopped_at,OLD.revoked_at,NEW.revoked_at,clock_timestamp());
 ELSE
  NEW.last_stopped_at=GREATEST(OLD.last_stopped_at,OLD.revoked_at);
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER meeting_capture_stop_barrier BEFORE UPDATE ON teams_meeting_captures
 FOR EACH ROW EXECUTE FUNCTION retain_meeting_capture_stop();

-- Permanent per-intent receipts make cross-database scheduling retries safe.
-- A cancellation can precede capture creation; its tombstone is never reusable.
CREATE TABLE outlook_capture_receipts (
 account_id UUID NOT NULL,
 user_id UUID NOT NULL,
 nonce UUID NOT NULL,
 capture_id UUID, -- Logical ID deliberately survives deletion of the capture/graph.
 capture_generation UUID,
 tenant_id TEXT,
 aad_id TEXT,
 meeting_id TEXT,
 graph_id UUID,
 occurrence_start TIMESTAMPTZ,
 occurrence_end TIMESTAMPTZ,
 intent_created_at TIMESTAMPTZ,
 created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
 cancelled_at TIMESTAMPTZ,
 PRIMARY KEY(account_id,user_id,nonce),
 CHECK ((capture_id IS NULL AND capture_generation IS NULL) OR
        (capture_id IS NOT NULL AND capture_generation IS NOT NULL AND tenant_id IS NOT NULL
         AND aad_id IS NOT NULL AND meeting_id IS NOT NULL AND graph_id IS NOT NULL
         AND occurrence_start IS NOT NULL AND occurrence_end > occurrence_start
         AND intent_created_at IS NOT NULL))
);
ALTER TABLE outlook_capture_receipts ENABLE ROW LEVEL SECURITY;
ALTER TABLE outlook_capture_receipts FORCE ROW LEVEL SECURITY;
CREATE POLICY outlook_capture_receipt_owner ON outlook_capture_receipts
 USING (account_id=nullif(current_setting('app.account_id',true),'')::uuid
        AND user_id=nullif(current_setting('app.user_id',true),'')::uuid)
 WITH CHECK (account_id=nullif(current_setting('app.account_id',true),'')::uuid
        AND user_id=nullif(current_setting('app.user_id',true),'')::uuid);
