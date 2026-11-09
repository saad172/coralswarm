-- Migration: 0061_mcp_tool_calls
-- Description: Append-only audit log for every remote-MCP tool call
--              (docs/plans/remote-mcp-v1.md §4.4). Best-effort insert (never
--              fails the request — see `mcp::audit`), parallel to the
--              `target="mcp.tool_call"` tracing line. Denied calls (scope,
--              tenancy, rate limit) are always logged — they are the security
--              signal this table exists for.
--
--              Deliberately NEVER stores raw tool arguments or results: those
--              carry private company content and would bypass the R2
--              per-account-DEK model. `args_digest` is a sha256 of the
--              canonicalized argument JSON (integrity/dedup signal only,
--              irreversible); `args_summary` holds ONLY ids/sizes/enums the
--              caller explicitly chooses to record (e.g. `{"ocean_count":2}`),
--              never free-text content.
--
--              Not `activity_events` (CHECK-constrained, product-facing feed;
--              see 0003_atoms) — this is an operational/security log with its
--              own retention and shape.
--
-- Depends on: 0001_init (users, accounts)
--
-- Down:
--   DROP TABLE IF EXISTS mcp_tool_calls;

CREATE TABLE IF NOT EXISTS mcp_tool_calls (
    id           UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    account_id   UUID        REFERENCES accounts(id) ON DELETE SET NULL,
    user_id      UUID        REFERENCES users(id) ON DELETE SET NULL,
    -- The OAuth client id that requested the token (`client_id`/`azp`), when
    -- the token carried one. Not a foreign key — Clerk-issued, not our own.
    client_id    TEXT,
    -- The token's `jti`, if present — audit correlation only (never used for
    -- replay prevention).
    token_jti    TEXT,
    tool         TEXT        NOT NULL,
    -- The scope actually used to authorize this call: 'read' | 'write'.
    scope_used   TEXT,
    -- The ocean (graph) the call targeted, when applicable.
    graph_id     UUID,
    -- The specific resource (reef/coral/meeting) the call targeted, when
    -- applicable — an opaque id, not a foreign key (may reference any of
    -- several tables depending on `tool`).
    target_id    UUID,
    -- sha256 of the canonicalized argument JSON. Irreversible; lets an
    -- operator confirm "was this the same call retried" without ever storing
    -- the arguments themselves.
    args_digest  TEXT,
    -- ONLY ids/sizes/enums the caller explicitly records (e.g.
    -- {"ocean_count":2,"top_k":20}) — never free-text argument content.
    args_summary JSONB       NOT NULL DEFAULT '{}'::jsonb,
    -- 'ok' | 'error' | 'denied_scope' | 'denied_tenancy' | 'rate_limited'
    outcome      TEXT        NOT NULL,
    error_code   TEXT,
    duration_ms  BIGINT,
    request_id   TEXT,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_mcp_tool_calls_account_created
    ON mcp_tool_calls (account_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_mcp_tool_calls_user_created
    ON mcp_tool_calls (user_id, created_at DESC);
