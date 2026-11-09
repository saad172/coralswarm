-- A push that misses the mailbox lock leaves a mark for the holder.
-- The holder clears it before history and rechecks it after releasing the lock.

ALTER TABLE google_watch_channels
    ADD COLUMN IF NOT EXISTS push_pending_at TIMESTAMPTZ;
