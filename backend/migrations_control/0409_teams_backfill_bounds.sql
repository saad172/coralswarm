SET LOCAL lock_timeout = '3s';
ALTER TABLE teams_watched_resources
 ADD COLUMN backfill_since TIMESTAMPTZ,
 ADD COLUMN backfill_until TIMESTAMPTZ;
