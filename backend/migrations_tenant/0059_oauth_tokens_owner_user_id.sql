-- Migration: 0059_oauth_tokens_owner_user_id
-- PAIRED WITH: migrations_control/0175_oauth_tokens_owner_user_id.sql — same
--   column + partial-index shape. See that migration's header for the full
--   design rationale (the account-wide/per-member split, why a single
--   UNIQUE constraint cannot express both, why `ON CONFLICT` must restate
--   the target index's predicate). Not repeated here, same discipline
--   `migrations_tenant/0030_extraction_cache.sql` documents for its own
--   control-tree pairing.
--
-- Schema-only, no behavior change on this plane: `oauth_tokens` is a SHARED
-- table per `scripts/check_schema_parity.sh` (`CONTROL_ONLY_RLS_TABLES`),
-- meaning its column/index shape must stay byte-identical across both trees,
-- but the table is never written on a tenant pool — it exists here purely as
-- a schema artifact of the tenant baseline dump (see e.g. `routes/oauth.rs`'s
-- own doc comments on why every `oauth_tokens` read/write stays on the
-- control/consumer plane). This migration keeps that parity gate green; it
-- does not make `oauth_tokens` live on the tenant plane.

ALTER TABLE oauth_tokens ADD COLUMN IF NOT EXISTS owner_user_id UUID;

ALTER TABLE oauth_tokens
    DROP CONSTRAINT IF EXISTS oauth_tokens_account_id_provider_provider_workspace_id_key;

CREATE UNIQUE INDEX IF NOT EXISTS uq_oauth_tokens_account_wide
    ON oauth_tokens (account_id, provider, provider_workspace_id)
    WHERE owner_user_id IS NULL;

CREATE UNIQUE INDEX IF NOT EXISTS uq_oauth_tokens_per_owner
    ON oauth_tokens (account_id, owner_user_id, provider, provider_workspace_id)
    WHERE owner_user_id IS NOT NULL;
