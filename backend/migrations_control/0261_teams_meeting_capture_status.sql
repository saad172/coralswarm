-- Control-plane twin of tenant0106: shared consent helpers also exist here.
-- Actual provider observations only. Claiming a scheduled job is not a check.
-- Existing member-owner FORCE RLS remains the authority for these columns.
ALTER TABLE teams_meeting_captures
 ADD COLUMN capture_generation UUID NOT NULL DEFAULT gen_random_uuid(),
 ADD COLUMN last_checked_at TIMESTAMPTZ,
 ADD COLUMN last_retrieved_at TIMESTAMPTZ,
 ADD COLUMN retrieved_transcript_count INTEGER NOT NULL DEFAULT 0 CHECK (retrieved_transcript_count >= 0),
 ADD COLUMN last_error_code TEXT CHECK (last_error_code IN (
  'transcript_access_disabled','permission_denied','not_found','rate_limited',
  'provider_unavailable','invalid_metadata','incomplete_response'
 ));
