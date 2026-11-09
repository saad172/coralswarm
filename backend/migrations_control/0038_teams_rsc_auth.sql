-- Migration 0038: RSC (app-only) auth for Teams watches (CS-121)
--
-- The Teams *app* model (owner adds CoralSwarm to a channel/chat/meeting) reads
-- via an app-only Graph token scoped by Resource-Specific Consent, not a
-- delegated user token. Record which auth mode a watch uses and the tenant the
-- app token must target.
--
--   auth_mode = 'delegated'  → read with the connecting user's token (CS-119)
--   auth_mode = 'app'        → read with an app-only token for `tenant_id` (CS-121)

ALTER TABLE teams_watched_resources
    ADD COLUMN IF NOT EXISTS tenant_id TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS auth_mode TEXT NOT NULL DEFAULT 'delegated';

ALTER TABLE teams_watched_resources
    DROP CONSTRAINT IF EXISTS teams_watched_resources_auth_mode_check;
ALTER TABLE teams_watched_resources
    ADD CONSTRAINT teams_watched_resources_auth_mode_check
    CHECK (auth_mode IN ('delegated', 'app'));
