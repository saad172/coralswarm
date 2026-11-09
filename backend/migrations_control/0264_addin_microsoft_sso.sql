-- 0262_addin_microsoft_sso.sql — Microsoft identity (Teams SSO / Office nested
-- app auth) → CoralSwarm credential exchange for the Teams meeting pane and the
-- Outlook panes.
--
-- THE PROBLEM THIS SOLVES.
--   The Teams meeting pane and the Outlook panes mount Clerk <SignIn> inside the
--   host iframe. Clerk's session cookie is a THIRD-PARTY cookie there, which
--   Teams on Mac (and Safari-class WebViews generally) blocks — so every new
--   meeting asked the user to sign in again. The add-ins instead obtain the
--   user's MICROSOFT identity from the host (Teams `getAuthToken()` / Office
--   nested app auth) and the backend exchanges that — after validating it
--   against Microsoft's JWKS — for a short-lived CoralSwarm credential. The
--   first time, a Gmail-style pairing ("Connect CoralSwarm" + approve once in a
--   browser, where the user also PICKS THE WORKSPACE) LINKS the Microsoft
--   identity to a CoralSwarm user + workspace. Teams and Outlook share ONE link
--   (same Microsoft identity = same link): the key is (tid, oid), never the
--   host app.
--
-- WHAT THIS MIGRATION ADDS.
--   1. `addin_microsoft_links`   — (tid, oid) → (user_id, account_id). One row
--      per Microsoft identity. `account_id` is the workspace (a personal
--      account OR an org account) the user picked; unlike `addin_tokens` (0210,
--      personal-plane only by construction) a link may name an ORG account, and
--      the credential it mints is bound to that org's tenant plane by
--      `middleware::auth::require_auth` (live membership re-checked on every
--      exchange and every request).
--   2. `addin_microsoft_sessions` — the short-lived (~1h) `csms_` credential.
--      Hash only (same non-negotiable as `addin_tokens.token_hash`); never the
--      plaintext. Rows cascade with the link; "Change workspace" / "Disconnect"
--      revoke every live row for the link, so Teams and Outlook both lose it.
--   3. `addin_pairing_codes` gains `kind` / `ms_tenant_id` / `ms_object_id`.
--      A Microsoft pairing row is BOUND, AT CREATION, to the (tid, oid) of the
--      Microsoft token `POST /v1/addin/microsoft/session` just validated —
--      `pair/approve` never accepts an identity from the client. That is what
--      closes the Gmail approve-race's "whose identity does this pairing carry"
--      half for this flavour (a code-guesser can no longer attach an arbitrary
--      identity); it does NOT, by itself, stop someone who guesses a live code
--      from approving it into their OWN workspace — see `routes::addin`'s
--      module doc for what that residual is and how it is made visible.
--   4. `resolve_microsoft_connected_users(tid, oid)` — control-only SECURITY
--      DEFINER resolver behind the AUTO-LINK path: "exactly one CoralSwarm user
--      has already connected this same Microsoft account via Connect
--      Microsoft". `oauth_tokens` is an RLS table on the control plane
--      (0083 — the backend connects as `coralswarm_app`, NOT a BYPASSRLS
--      role), and at the point this is read no `account_id` is known (that is
--      the thing being discovered), so a `begin_account_scoped` read cannot
--      work; the carve-out is the same bootstrap shape as
--      `resolve_google_identity_by_sub` (0198) / `resolve_oauth_token_account_ids`
--      (0121). NO NEW STORAGE for the oid is needed: the Microsoft connector
--      already stores `tid` in `oauth_tokens.provider_workspace_id` and `oid`
--      in `oauth_tokens.provider_user_id` (`connectors::microsoft`'s
--      `token_response_to_bundle`).
--
-- DISCONNECT IS A TOMBSTONE, NOT A DELETE.
--   `addin_microsoft_links.disconnected_at` is set by `DELETE
--   /v1/addin/microsoft/link`. A row with `disconnected_at IS NOT NULL` means
--   "no active link" for the session exchange AND "the user said disconnect —
--   do not silently re-auto-link", which is the only reason the row is kept: a
--   user who already did Connect Microsoft would otherwise be re-linked on the
--   very next pane open and ⋮ Disconnect would be a no-op. A completed pairing
--   approval (an explicit human act) clears it.
--
-- CONTROL-ONLY, NO TENANT TWIN, NO RLS — identical justification to 0210/0211:
--   all three tables are read by the auth middleware (before any tenant pool
--   exists) or by routes with no `Tenant` extractor (unauthenticated by design),
--   and hold no CONTENT. Registered in `scripts/check_schema_parity.sh`
--   CONTROL_TABLES / CONTROL_FUNCTIONS in the same PR.
--
-- IDENTIFIERS.
--   `tenant_id` / `object_id` are stored LOWER-CASED (the handler normalizes
--   after validating both are GUIDs). The resolver compares case-insensitively
--   against `oauth_tokens` because that table stores whatever casing the id
--   token carried.
--
-- Depends on: 0210_addin_tokens / 0211_addin_pairing_codes (the pairing table
--             extended here), 0021_oauth_tokens + 0175 (the connected-Microsoft
--             rows the resolver reads), 0198 (the SECURITY DEFINER precedent).
--
-- Down:
--   REVOKE EXECUTE ON FUNCTION resolve_microsoft_connected_users(text, text) FROM coralswarm_app, CURRENT_USER;
--   REVOKE EXECUTE ON FUNCTION resolve_microsoft_connected_users(text, text) FROM PUBLIC;
--   DROP FUNCTION IF EXISTS resolve_microsoft_connected_users(text, text);
--   DROP TABLE IF EXISTS addin_microsoft_sessions;
--   DROP TABLE IF EXISTS addin_microsoft_links;
--   DROP INDEX IF EXISTS idx_addin_pairing_codes_ms_identity;
--   ALTER TABLE addin_pairing_codes
--       DROP CONSTRAINT IF EXISTS addin_pairing_codes_ms_identity_chk,
--       DROP CONSTRAINT IF EXISTS addin_pairing_codes_kind_chk,
--       DROP COLUMN IF EXISTS ms_object_id,
--       DROP COLUMN IF EXISTS ms_tenant_id,
--       DROP COLUMN IF EXISTS kind;

-- ── 1. Pairing rows: kind + the identity a Microsoft pairing is bound to ────
ALTER TABLE addin_pairing_codes
    ADD COLUMN IF NOT EXISTS kind         TEXT NOT NULL DEFAULT 'gmail',
    ADD COLUMN IF NOT EXISTS ms_tenant_id TEXT,
    ADD COLUMN IF NOT EXISTS ms_object_id TEXT;

ALTER TABLE addin_pairing_codes DROP CONSTRAINT IF EXISTS addin_pairing_codes_kind_chk;
ALTER TABLE addin_pairing_codes
    ADD CONSTRAINT addin_pairing_codes_kind_chk CHECK (kind IN ('gmail', 'microsoft'));

-- A Gmail row carries no Microsoft identity; a Microsoft row ALWAYS does. The
-- constraint makes an unbound Microsoft pairing (the approve-race this design
-- exists to close) unrepresentable rather than merely unlikely.
ALTER TABLE addin_pairing_codes DROP CONSTRAINT IF EXISTS addin_pairing_codes_ms_identity_chk;
ALTER TABLE addin_pairing_codes
    ADD CONSTRAINT addin_pairing_codes_ms_identity_chk CHECK (
        (kind = 'gmail' AND ms_tenant_id IS NULL AND ms_object_id IS NULL)
        OR (kind = 'microsoft' AND ms_tenant_id IS NOT NULL AND ms_object_id IS NOT NULL)
    );

-- `POST /session` keeps ONE live pairing per identity (it deletes the identity's
-- older rows before inserting a fresh one).
CREATE INDEX IF NOT EXISTS idx_addin_pairing_codes_ms_identity
    ON addin_pairing_codes (ms_tenant_id, ms_object_id)
    WHERE kind = 'microsoft';

-- ── 2. The link: one Microsoft identity → one (user, workspace) ─────────────
CREATE TABLE IF NOT EXISTS addin_microsoft_links (
    tenant_id       TEXT        NOT NULL CHECK (tenant_id <> ''),
    object_id       TEXT        NOT NULL CHECK (object_id <> ''),
    user_id         UUID        NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    -- The workspace the user picked: a personal account or an org account.
    account_id      UUID        NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    -- 'pairing' = a human approved it in the browser; 'auto' = the single
    -- already-connected-Microsoft match (see the resolver below).
    link_method     TEXT        NOT NULL CHECK (link_method IN ('pairing', 'auto')),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- Tombstone — see this migration's header. NULL = active link.
    disconnected_at TIMESTAMPTZ,
    PRIMARY KEY (tenant_id, object_id)
);

CREATE INDEX IF NOT EXISTS idx_addin_microsoft_links_user
    ON addin_microsoft_links (user_id);
-- ON DELETE CASCADE from `accounts` scans by this column.
CREATE INDEX IF NOT EXISTS idx_addin_microsoft_links_account
    ON addin_microsoft_links (account_id);

-- ── 3. The short-lived session credential (`csms_`) ─────────────────────────
CREATE TABLE IF NOT EXISTS addin_microsoft_sessions (
    id            UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id     TEXT        NOT NULL,
    object_id     TEXT        NOT NULL,
    -- Denormalized from the link AT MINT TIME. `require_auth` resolves a request
    -- from this row alone (one indexed lookup), and "Change workspace" revokes
    -- every row for the link, so a stale copy can never outlive a re-bind.
    user_id       UUID        NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    account_id    UUID        NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    -- SHA-256 hex of the `csms_` secret; never the plaintext, never logged.
    token_hash    TEXT        NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at    TIMESTAMPTZ NOT NULL,
    last_used_at  TIMESTAMPTZ,
    revoked_at    TIMESTAMPTZ,
    FOREIGN KEY (tenant_id, object_id)
        REFERENCES addin_microsoft_links (tenant_id, object_id) ON DELETE CASCADE
);

-- The auth middleware's own lookup key.
CREATE UNIQUE INDEX IF NOT EXISTS uq_addin_microsoft_sessions_token_hash
    ON addin_microsoft_sessions (token_hash);
-- Revoke-all-for-link and the per-identity opportunistic cleanup.
CREATE INDEX IF NOT EXISTS idx_addin_microsoft_sessions_identity
    ON addin_microsoft_sessions (tenant_id, object_id);
CREATE INDEX IF NOT EXISTS idx_addin_microsoft_sessions_account
    ON addin_microsoft_sessions (account_id);
CREATE INDEX IF NOT EXISTS idx_addin_microsoft_sessions_user
    ON addin_microsoft_sessions (user_id);

-- ── 4. Auto-link resolver — control-only SECURITY DEFINER ───────────────────
--
-- Returns every (account_id, user_id) that has connected THIS Microsoft account
-- (same tenant + object id) through "Connect Microsoft", at most 3 rows (the
-- caller only needs to distinguish 0 / 1 / many). DISTINCT (account, user): a
-- personal account can legitimately hold BOTH a legacy account-wide row and a
-- per-member row for the same Microsoft account (0175), which both map to the
-- same (account, user) and must count as ONE candidate. The caller auto-links ONLY
-- when exactly ONE row comes back — two users, or one user in two workspaces,
-- is ambiguous and falls through to the explicit pairing, where the human
-- picks.
--
-- * `provider = 'microsoft'` — the connector's own key (`oauth_tokens.provider`).
-- * Per-member rows (`owner_user_id` set) name their own user. An account-wide
--   row (`owner_user_id IS NULL`) is attributable to a person ONLY for a
--   personal account (its owner); on an org account it is nobody's, and is
--   skipped.
-- * `credential_invalid_at` is deliberately NOT filtered: a dead credential is
--   still evidence that this human is that CoralSwarm user, and counting it can
--   only turn an auto-link into an explicit pairing (the safe direction).
-- * Empty arguments return no rows rather than matching rows with empty ids.
--
-- Same SECURITY DEFINER hygiene as 0198: pinned search_path, REVOKE from PUBLIC,
-- GRANT back to the app role only.
CREATE OR REPLACE FUNCTION resolve_microsoft_connected_users(p_tenant_id text, p_object_id text)
RETURNS TABLE(account_id uuid, owner_user_id uuid)
LANGUAGE sql
SECURITY DEFINER
STABLE
SET search_path = public, pg_temp
AS $$
    SELECT DISTINCT ot.account_id, COALESCE(ot.owner_user_id, a.owner_user_id) AS owner_user_id
    FROM oauth_tokens ot
    JOIN accounts a ON a.id = ot.account_id
    WHERE ot.provider = 'microsoft'
      AND p_tenant_id <> '' AND p_object_id <> ''
      AND ot.provider_workspace_id <> '' AND ot.provider_user_id <> ''
      AND lower(ot.provider_workspace_id) = lower(p_tenant_id)
      AND lower(ot.provider_user_id) = lower(p_object_id)
      AND (ot.owner_user_id IS NOT NULL OR a.clerk_org_id IS NULL)
    ORDER BY 1, 2
    LIMIT 3
$$;

REVOKE EXECUTE ON FUNCTION resolve_microsoft_connected_users(text, text) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION resolve_microsoft_connected_users(text, text) TO coralswarm_app, CURRENT_USER;
