-- Migration: 0077_coding_session_shares
-- Description: Revocable per-coding-session share links (audit amendment A7).
--
--   `coding_session_shares` is a faithful clone of migration
--   `0068_meeting_shares.sql`, retargeted at `coding_sessions` (migration
--   0076) instead of `meeting_transcripts`. Same token/visibility/revocation
--   shape and the same security invariants — see 0068's header comment for
--   the full rationale, repeated here for the coding-session case:
--
--   One row per shareable link. `token` is a 32-byte CSPRNG value, base64url
--   (no padding) encoded, and is never derived/guessable from the session id.
--   `visibility` controls who may resolve the link: `'link'` is anonymous
--   (anyone holding the token, via the public `GET /v1/shared/sessions/{token}`
--   route), `'team'` requires the caller be an authenticated member who can
--   reach the ocean. A share is revoked by setting `revoked_at`; revoked (and
--   unknown) tokens resolve identically as "not found" so a link's live/dead
--   state is never leaked to an unauthorized caller.
--
--   Unlike 0068 (which has no FK to `meeting_transcripts` — that table has no
--   natural-key UNIQUE to reference), this table DOES carry a composite FK to
--   `coding_sessions (graph_id, session_id)` — that UNIQUE constraint exists
--   (migration 0076), so referencing it directly means a share can never
--   outlive/precede its session and a deleted session cascades its shares.
--   The direct `graph_id -> graphs(id)` FK is kept too (belt-and-suspenders,
--   matching 0068) so a share row is never orphaned even in the moment
--   between a `graphs` cascade and a `coding_sessions` cascade completing.
--
--   `coding_sessions`/`projects` are cloud-only (Postgres) per migration
--   0076's header; this table is cloud-only for the same reason (a share
--   references a session row that only exists on this tier) — the libsql
--   (desktop) `Store` impl gets empty-stub methods, not a mirrored local
--   table (unlike 0068's `meeting_shares`, which DOES get a local table on
--   desktop — see `store/libsql.rs`'s coding-session-shares stub block for
--   why this one differs).
--
-- Depends on: 0076_coding_sessions (coding_sessions.(graph_id, session_id) UNIQUE).
-- Down: DROP TABLE IF EXISTS coding_session_shares;
CREATE TABLE IF NOT EXISTS coding_session_shares (
    id          UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    graph_id    UUID        NOT NULL REFERENCES graphs(id) ON DELETE CASCADE,
    session_id  TEXT        NOT NULL,
    token       TEXT        NOT NULL UNIQUE,
    visibility  TEXT        NOT NULL DEFAULT 'link' CHECK (visibility IN ('link','team')),
    created_by  UUID        NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    revoked_at  TIMESTAMPTZ,
    FOREIGN KEY (graph_id, session_id)
        REFERENCES coding_sessions (graph_id, session_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_coding_session_shares_graph_session
    ON coding_session_shares (graph_id, session_id);
CREATE INDEX IF NOT EXISTS idx_coding_session_shares_token_active
    ON coding_session_shares (token) WHERE revoked_at IS NULL;
