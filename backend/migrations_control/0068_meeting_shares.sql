-- Migration: 0068_meeting_shares
-- Description: Revocable per-meeting share links for INGESTED meetings.
--
--   `meeting_shares` records one shareable link per (graph_id, source_id, token).
--   `token` is a 32-byte CSPRNG value, base64url (no padding) encoded, and is
--   never derived/guessable from the meeting id. `visibility` controls who may
--   resolve the link: `'link'` is anonymous (anyone holding the token, via the
--   public `GET /v1/shared/{token}` route), `'team'` requires the caller be an
--   authenticated member who can reach the ocean (`GET
--   /v1/meetings/shares/view/{token}`). A share is revoked by setting
--   `revoked_at`; revoked (and unknown) tokens resolve identically as "not
--   found" so a link's live/dead state is never leaked to an unauthorized
--   caller.
--
-- Depends on: 0056_meeting_transcripts (graph_id/source_id shape).
-- Down: DROP TABLE IF EXISTS meeting_shares;
CREATE TABLE IF NOT EXISTS meeting_shares (
    id          UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    graph_id    UUID        NOT NULL REFERENCES graphs(id) ON DELETE CASCADE,
    source_id   TEXT        NOT NULL,
    token       TEXT        NOT NULL UNIQUE,
    visibility  TEXT        NOT NULL DEFAULT 'link' CHECK (visibility IN ('link','team')),
    created_by  UUID        NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    revoked_at  TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_meeting_shares_graph_source ON meeting_shares (graph_id, source_id);
CREATE INDEX IF NOT EXISTS idx_meeting_shares_token_active ON meeting_shares (token) WHERE revoked_at IS NULL;
