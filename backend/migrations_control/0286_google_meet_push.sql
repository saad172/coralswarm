-- Meet Workspace Events metadata shares the existing watch lifecycle.
ALTER TABLE google_watch_channels DROP CONSTRAINT google_watch_channels_surface_check;
ALTER TABLE google_watch_channels ADD CONSTRAINT google_watch_channels_surface_check CHECK (surface IN ('gmail','chat','calendar','meet'));
CREATE UNIQUE INDEX uq_google_meet_subscription ON google_watch_channels(channel_id) WHERE surface='meet' AND channel_id <> '';
CREATE OR REPLACE FUNCTION resolve_google_meet_subscription(p_name text)
RETURNS TABLE(id uuid,account_id uuid,owner_user_id uuid)
LANGUAGE sql SECURITY DEFINER STABLE SET search_path=public,pg_temp AS $$
 SELECT w.id,w.account_id,w.owner_user_id FROM google_watch_channels w
 WHERE w.surface='meet' AND w.channel_id=p_name AND p_name<>'' AND w.owner_user_id IS NOT NULL
$$;
REVOKE EXECUTE ON FUNCTION resolve_google_meet_subscription(text) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION resolve_google_meet_subscription(text) TO coralswarm_app,CURRENT_USER;

-- Distinguish explicit reapproval from sweep binding/rotation writes so a
-- disconnect revokes the old consent without deleting a concurrent new one.
ALTER TABLE google_meet_captures ADD COLUMN approved_at TIMESTAMPTZ NOT NULL DEFAULT now();

ALTER TABLE google_watch_channels ADD CONSTRAINT google_meet_watch_requires_owner CHECK(surface <> 'meet' OR owner_user_id IS NOT NULL);
