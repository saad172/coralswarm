-- Latest transcript creation time a capture may ingest. Set only for an occurrence
-- of a recurring series (one onlineMeeting shared by every occurrence), where it
-- stops a transcript of the next occurrence being captured. NULL = single or
-- ad-hoc meeting: the original end + 24h availability window applies.
ALTER TABLE teams_meeting_captures ADD COLUMN IF NOT EXISTS transcript_end TIMESTAMPTZ;
