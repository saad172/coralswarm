-- Migration: 0067_users_display_name
-- Description: Add a nullable `users.name` column — the first human-readable
--              display name CoralSwarm has ever stored server-side (0001_init's
--              `users` table has only `email`; see 0057's doc comment, which
--              explicitly noted the gap this migration closes).
--
--              Populated from Clerk: `clerk_provision::ensure_user` sets it on
--              first sight of a Clerk identity (from the verified token's OIDC
--              `name` claim when present, else a one-time Clerk Backend API
--              lookup — same fallback shape as the existing `email` handling),
--              and `clerk_provision::sync_user_display_name` refreshes it from
--              the `name` claim on every subsequent authenticated request
--              (`middleware::auth::resolve_clerk_context`) so it stays current
--              without a bulk backfill job or a live API call on every request.
--
--              Nullable and additive: existing rows are left NULL until the
--              owning user's next authenticated request populates it — no
--              synthetic fallback (unlike email, "no display name available"
--              is a legitimate, permanent outcome for some users/tokens).
--
-- Depends on: 0001_init
--
-- Down:
--   ALTER TABLE users DROP COLUMN IF EXISTS name;

ALTER TABLE users
    ADD COLUMN IF NOT EXISTS name TEXT;
