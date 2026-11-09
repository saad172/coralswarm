-- Control-plane twin of tenant 0108: shared consent helpers also exist here.
ALTER TABLE teams_meeting_captures ADD COLUMN IF NOT EXISTS transcript_end TIMESTAMPTZ;
