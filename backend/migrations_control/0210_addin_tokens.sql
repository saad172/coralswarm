-- 0210_addin_tokens.sql — long-lived, revocable credentials for platform
-- add-ons that cannot hold a live Clerk session (Gmail Add-on, CS "ingest
-- this thread" — the Gmail counterpart to the Outlook task-pane).
--
-- WHY THIS TABLE EXISTS, AND WHY IT ISN'T "JUST" ANOTHER `oauth_tokens` ROW.
--   The Outlook add-in (`extensions/outlook-addin`) holds a live Clerk
--   session in-pane and mints a fresh ~60s JWT immediately before every
--   call — see that add-in's README for why that shape was chosen. A Gmail
--   Add-on is Google Apps Script + CardService: a server-side function
--   returns a declarative `Card` on a contextual trigger, with no persistent
--   DOM and no place to keep a live session alive. Apps Script's only
--   durable per-user state is `PropertiesService.getUserProperties()` — a
--   plain key-value store — so the add-on needs its OWN long-lived,
--   revocable bearer credential, obtained once through a pairing flow
--   (`addin_pairing_codes`, migration 0211) and then stored there.
--
--   This is a personal access token (PAT), not an OAuth token for a THIRD
--   PARTY (that's what `oauth_tokens` is for — Slack/Google/Microsoft
--   creds this backend holds on the user's behalf). A PAT here authenticates
--   the ADD-ON to CoralSwarm itself, so it is a new, narrower shape:
--   `token_hash` only (never the plaintext, never logged — see
--   `routes::addin::hash_token`), no scope column (today it can only reach
--   the same per-item ingest surface `Authorization: Bearer <Clerk JWT>`
--   already reaches, personal-plane only), no refresh token (revoke and
--   re-pair is the whole rotation story for v1).
--
-- PERSONAL-PLANE ONLY, BY CONSTRUCTION.
--   `owner_user_id` is `NOT NULL` — never `AccountWide` (contrast
--   `oauth_tokens.owner_user_id`, which is nullable for exactly that shape,
--   migration 0175's header). An add-on token is inherently the ONE Gmail
--   mailbox one human paired, mirroring the personal-only scope discipline
--   `docs/connectors/gmail.md`/`reject_org_connector_access` already hold for
--   Gmail/Meet: `routes::addin::pair_approve` refuses (400) a caller in an
--   org `Tenant` context outright, so no row here can ever carry an org
--   account_id. Org-plane add-on tokens are an explicit non-goal for v1 —
--   scope creep this migration deliberately does not build room for.
--
-- CONTROL-ONLY, NO TENANT TWIN — same justification shape as
-- `oauth_state_jti` (0173) and `org_connector_flags` (0178): the bearer
-- token this table backs is verified in `middleware::auth::require_auth`,
-- BEFORE any `TenancyDecision`/tenant pool is resolved (see
-- `tenancy::extractor::Tenant`'s own doc comment on the ordering) — there is
-- no tenant pool yet at the point this table is read, so a per-tenant copy
-- could never be reached from the code path that needs it. Registered in
-- `scripts/check_schema_parity.sh`'s CONTROL_TABLES in the same PR (the
-- paired-migration discipline that script's header asks for).
--
-- NO RLS, deliberately — same class as `oauth_tokens` (0021) and
-- `oauth_state_jti` (0173): this table is read/written by the auth
-- middleware and the pairing routes only, never through a per-request
-- `SET LOCAL app.account_id` scope, and it holds no CONTENT (a token hash,
-- not the ocean it unlocks).
--
-- COLUMN CHOICES.
--   `token_hash` — SHA-256 hex digest (64 hex chars) of the actual bearer
--   secret (`csat_`-prefixed, minted in `routes::addin::pair_approve`).
--   Looked up by exact-match on an indexed column, same posture this repo
--   already uses for `mcp`'s own token hashes — never the plaintext, never
--   logged. `UNIQUE`: two tokens colliding on their hash would be a SHA-256
--   collision, and the constraint also makes the mint path's `INSERT` fail
--   loud rather than silently aliasing two credentials.
--
--   `name` — a human label for the token-management surface (`Gmail Add-on`
--   default; there is no UI to rename one in v1, but the column exists so a
--   future one doesn't need a migration).
--
--   `last_used_at` — best-effort, bumped by `require_auth`'s add-on-token
--   branch on every successful use; a failed write never fails the request
--   (see that call site's own comment). Nullable: a freshly-paired token
--   that has never ingested anything has no last use yet.
--
--   `revoked_at` — soft-delete. `DELETE /v1/addin/tokens/{id}` sets this
--   rather than removing the row, so a revoked token's provenance (when it
--   was minted, when it was revoked) survives for the caller's own audit
--   trail. `require_auth`'s lookup filters `WHERE revoked_at IS NULL`.
--
-- Depends on: 0021_oauth_tokens (the control-plane bearer-credential
--             precedent this table's shape follows), 0173_oauth_state_jti
--             / 0178_org_connector_flags (the control-only justification
--             shape this migration's header reuses).
--
-- Down:
--   DROP INDEX IF EXISTS idx_addin_tokens_owner;
--   DROP TABLE IF EXISTS addin_tokens;

CREATE TABLE IF NOT EXISTS addin_tokens (
    id             UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    account_id     UUID        NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    -- Always the pairing caller's own user — see this migration's header on
    -- why an add-on token is never `AccountWide`.
    owner_user_id  UUID        NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    token_hash     TEXT        NOT NULL,
    name           TEXT        NOT NULL DEFAULT 'Gmail Add-on',
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_used_at   TIMESTAMPTZ,
    revoked_at     TIMESTAMPTZ
);

-- The auth middleware's own lookup key.
CREATE UNIQUE INDEX IF NOT EXISTS uq_addin_tokens_token_hash
    ON addin_tokens (token_hash);

-- `DELETE /v1/addin/tokens/{id}` and the (future) token-management list both
-- scope to the caller's own rows.
CREATE INDEX IF NOT EXISTS idx_addin_tokens_owner
    ON addin_tokens (owner_user_id);
