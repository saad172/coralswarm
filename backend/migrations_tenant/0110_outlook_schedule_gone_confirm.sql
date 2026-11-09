-- Outlook compose capture: confirm a "not found" before stopping. One Graph 404 on a
-- tracked event must not silently drop a real meeting, so the follower counts consecutive
-- 404s here and only stops after they persist (see workers::outlook_schedule).
ALTER TABLE outlook_schedule_intents
 ADD COLUMN gone_since TIMESTAMPTZ,
 ADD COLUMN gone_checks INTEGER NOT NULL DEFAULT 0 CHECK (gone_checks >= 0);
