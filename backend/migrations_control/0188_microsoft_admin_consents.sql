-- Migration: 0188_microsoft_admin_consents
-- Description: m365 enterprise train PR 3.2 ("delegated per-member tokens +
--   admin-consent flow"). Records a tenant admin's org-wide Microsoft consent
--   grant so `routes::oauth::callback` can recognize a completed
--   `admin_consent=True&tenant=<id>` redirect (Microsoft's `prompt=
--   admin_consent` variant of the standard `/authorize` endpoint — see
--   `connectors::microsoft::MicrosoftConnector::admin_consent_url` — returns
--   this shape instead of a `code`) and durably remember it happened. This is
--   the "tenant-wide pre-consent so members get zero prompts" artifact the
--   plan names: Microsoft itself is what actually suppresses the per-user
--   consent prompt for members of an already-consented tenant (that's native
--   AAD behavior once an admin grants org-wide consent for a scope set, not
--   something this app enforces) — this table is CoralSwarm's own record of
--   the fact, for UI display ("your organization has already approved this
--   app") and future audit, not an enforcement gate.
--
--   ── Control-only, no tenant-tree twin ──
--   Same posture as `oauth_state_jti` (migration 0173) and
--   `org_connector_flags` (migration 0178): this table is written and read
--   exclusively by `routes::oauth::callback`'s admin-consent branch, which
--   (like `mint_state`/`callback` themselves — see `routes/oauth.rs`'s own
--   file-header route table) resolves `account_id` off the control-only
--   `accounts` table and runs before any tenant pool is chosen. There is no
--   coherent per-tenant copy of "which AAD tenant consented for this
--   account", so none is created. Added to `CONTROL_TABLES` in
--   `scripts/check_schema_parity.sh` in this same PR.
--
--   ── Column choices ──
--   `account_id` — FK to `accounts` (control-plane-only table), same shape as
--   `seat_grant_sources.account_id` (migration 0184).
--
--   `microsoft_tenant_id` — the AAD tenant GUID Microsoft echoes back in the
--   `tenant` query parameter, `TEXT NOT NULL`, non-empty (`CHECK`). Free text,
--   not a FK: it identifies an external Azure AD tenant, which this database
--   has no table for.
--
--   `consented_by_user_id` — the CoralSwarm user id who initiated the
--   admin-consent round trip, taken from the verified OAuth `state`'s `uid`
--   claim (`oauth::state::VerifiedState::user_id` — see that module's own
--   doc). Nullable: defensive, mirroring `oauth::state`'s own "an absent uid
--   decodes as None rather than a hard failure" posture — every real
--   `mint_state`-issued token carries one, but this column does not need to
--   be the thing that enforces that.
--
--   `granted_scope` — the `scope` query parameter Microsoft echoes back on a
--   successful admin-consent redirect, stored verbatim for audit (did the
--   admin actually approve the full frozen scope set this app requested, or
--   a Microsoft-trimmed subset?). Nullable: absent on a pre-this-column
--   redirect shape, or if Microsoft ever omits it.
--
--   `consented_at` — `TIMESTAMPTZ NOT NULL DEFAULT now()`, bumped by the
--   `ON CONFLICT ... DO UPDATE` on every re-consent for the same tenant.
--
--   ── Primary key ──
--   Natural composite `(account_id, microsoft_tenant_id)` — one CURRENT
--   consent row per account+tenant pair is all this needs to remember (a
--   re-run of admin-consent for the same tenant just refreshes the row, it
--   does not need a history of every past grant); also the `ON CONFLICT`
--   target `routes::oauth::record_microsoft_admin_consent` upserts through.
--
--   ── No RLS, deliberately ──
--   Same class as `org_connector_flags` (0178) and `connector_installs`
--   (0121): read/written at a point in the request (the OAuth callback,
--   before any account-scoped transaction is opened) where `SET LOCAL
--   app.account_id` is not available, and it holds no secrets — only "which
--   AAD tenant consented for which account, and when."
--
-- Depends on: 0173_oauth_state_jti (the state-signing/account-linking
--             machinery this table's writer reuses), 0175_oauth_tokens_owner_user_id
-- Down:
--   DROP TABLE IF EXISTS microsoft_admin_consents;

CREATE TABLE IF NOT EXISTS microsoft_admin_consents (
    account_id           UUID NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    microsoft_tenant_id  TEXT NOT NULL CHECK (microsoft_tenant_id <> ''),
    consented_by_user_id UUID,
    granted_scope        TEXT,
    consented_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (account_id, microsoft_tenant_id)
);

CREATE INDEX IF NOT EXISTS idx_microsoft_admin_consents_tenant
    ON microsoft_admin_consents (microsoft_tenant_id);
