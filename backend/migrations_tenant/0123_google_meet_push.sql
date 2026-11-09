-- Meet Workspace Events metadata shares the existing watch lifecycle.
ALTER TABLE google_watch_channels DROP CONSTRAINT google_watch_channels_surface_check;
ALTER TABLE google_watch_channels ADD CONSTRAINT google_watch_channels_surface_check CHECK (surface IN ('gmail','chat','calendar','meet'));

-- Distinguish explicit reapproval from sweep binding/rotation writes so a
-- disconnect revokes the old consent without deleting a concurrent new one.
ALTER TABLE google_meet_captures ADD COLUMN approved_at TIMESTAMPTZ NOT NULL DEFAULT now();

ALTER TABLE google_watch_channels ADD CONSTRAINT google_meet_watch_requires_owner CHECK(surface <> 'meet' OR owner_user_id IS NOT NULL);
