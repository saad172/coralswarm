-- Migration: 0060_mcp_user_prefs
-- Description: Per-user preference flag gating write access from the remote
--              MCP server (docs/plans/remote-mcp-v1.md §4.2). Effective MCP
--              write scope is "env ceiling (MCP_DEFAULT_SCOPES) AND per-user
--              opt-in" — this table is the per-user half, checked from the DB
--              on every `/mcp` request so revocation takes effect instantly
--              (no token re-mint required). Default false: a prompt-injected
--              tool call cannot write until the user has explicitly turned
--              this on, which Clerk's own (currently fixed-scope) consent
--              screen cannot express.
--
--              A dedicated boolean column (rather than the `account_prefs`
--              JSONB-blob pattern in 0029) because this flag sits on the hot
--              path of every authenticated MCP request and is looked up by a
--              single indexed column read, not a JSON round-trip. Keyed to
--              `users`, not `accounts` — the plan is explicit this is a
--              per-user lever, distinct from the org-wide `account_prefs`.
--
-- Depends on: 0001_init (users)
--
-- Down:
--   DROP TABLE IF EXISTS user_prefs;

CREATE TABLE IF NOT EXISTS user_prefs (
    user_id           UUID        PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
    mcp_write_enabled BOOLEAN     NOT NULL DEFAULT false,
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);
