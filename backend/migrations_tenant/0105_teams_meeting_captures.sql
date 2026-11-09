CREATE TABLE teams_meeting_installations (
 account_id UUID NOT NULL,tenant_id TEXT NOT NULL,installation_id TEXT NOT NULL,
 context_id TEXT NOT NULL,active BOOLEAN NOT NULL DEFAULT true,updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
 PRIMARY KEY(account_id,tenant_id,installation_id)
);
ALTER TABLE teams_meeting_installations ENABLE ROW LEVEL SECURITY;
ALTER TABLE teams_meeting_installations FORCE ROW LEVEL SECURITY;
CREATE POLICY meeting_installation_account ON teams_meeting_installations
 USING(account_id=nullif(current_setting('app.account_id',true),'')::uuid)
 WITH CHECK(account_id=nullif(current_setting('app.account_id',true),'')::uuid);
-- Explicit per-meeting, per-member transcript consent. Calendar/recording/chat watches are not consent.
CREATE TABLE teams_meeting_captures (
 id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
 account_id UUID NOT NULL,
 user_id UUID NOT NULL,
 aad_id TEXT NOT NULL CHECK (aad_id <> ''),
 tenant_id TEXT NOT NULL CHECK (tenant_id <> ''),
 meeting_id TEXT NOT NULL CHECK (meeting_id <> ''),
 occurrence_start TIMESTAMPTZ NOT NULL,
 occurrence_end TIMESTAMPTZ NOT NULL CHECK (occurrence_end > occurrence_start),
 graph_id UUID NOT NULL REFERENCES graphs(id) ON DELETE CASCADE,
 source TEXT NOT NULL CHECK (source IN ('meeting_bot','meeting_addin','explicit_meeting')),
 installation_id TEXT,
 source_event_id TEXT,
 source_event_at TIMESTAMPTZ,
 created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
 next_poll_at TIMESTAMPTZ NOT NULL DEFAULT now(),
 revoked_at TIMESTAMPTZ,
 UNIQUE(account_id,user_id,tenant_id,meeting_id)
);
ALTER TABLE teams_meeting_captures ENABLE ROW LEVEL SECURITY;
ALTER TABLE teams_meeting_captures FORCE ROW LEVEL SECURITY;
CREATE POLICY meeting_capture_owner ON teams_meeting_captures
 USING (account_id = nullif(current_setting('app.account_id',true),'')::uuid
 AND user_id = nullif(current_setting('app.user_id',true),'')::uuid)
 WITH CHECK (account_id = nullif(current_setting('app.account_id',true),'')::uuid
 AND user_id = nullif(current_setting('app.user_id',true),'')::uuid
 AND EXISTS (SELECT 1 FROM graphs g WHERE g.id=graph_id AND g.account_id=teams_meeting_captures.account_id
 AND g.owner_user_id=teams_meeting_captures.user_id AND g.kind='member' AND g.archived_at IS NULL));
