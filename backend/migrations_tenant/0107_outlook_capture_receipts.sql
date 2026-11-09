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
