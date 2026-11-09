-- Long-lived, revocable desktop sync credentials. Store only the SHA-256
-- hash of csdt_ + 32 CSPRNG bytes; plaintext is returned once at mint time.
-- Multiple live credentials per user/install label are deliberately allowed.
--
-- CONTROL-ONLY, NO TENANT TWIN: require_auth reads this table before resolving
-- any tenant pool, like 0210_addin_tokens. Registered in check_schema_parity.sh.
-- No RLS: identity metadata and token hashes, read by authentication and
-- owner-scoped credential-management routes, rather than ocean content.
--
-- Bind account_id and owner_user_id to the verified Clerk session. Preserve
-- both ordinary and personal-org ids in clerk_org_id; replay classifies the
-- latter against personal_orgs and rechecks current membership/account/role.
-- NULL means the session carried no org claim. Revocation is soft deletion.
--
-- Down: DROP TABLE desktop_tokens;

CREATE TABLE IF NOT EXISTS desktop_tokens (
    id             UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    token_hash     TEXT        NOT NULL,
    owner_user_id  UUID        NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    account_id     UUID        NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    -- NULL = personal plane (see this migration's header). Not a foreign key
    -- — an opaque Clerk organization id, same posture as
    -- `personal_orgs.clerk_org_id`.
    clerk_org_id   TEXT,
    label          TEXT        NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_used_at   TIMESTAMPTZ,
    revoked_at     TIMESTAMPTZ
);

-- The auth middleware's own lookup key.
CREATE UNIQUE INDEX IF NOT EXISTS uq_desktop_tokens_token_hash
    ON desktop_tokens (token_hash);

-- `GET /v1/desktop/tokens` (the caller's own live tokens) and
-- `DELETE /v1/desktop/tokens/{id}`'s ownership check both scope to this.
-- Partial on `revoked_at IS NULL`: the list is always "live tokens for this
-- user", so the index never has to carry the (larger, ever-growing) revoked
-- rows.
CREATE INDEX IF NOT EXISTS desktop_tokens_owner_live
    ON desktop_tokens (owner_user_id)
    WHERE revoked_at IS NULL;
