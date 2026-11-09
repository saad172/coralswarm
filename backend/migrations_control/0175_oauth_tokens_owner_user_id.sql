-- Migration: 0175_oauth_tokens_owner_user_id
-- Description: Foundation train PR 1.1 — add the owner dimension to
--   `oauth_tokens`, implementing the design already drafted in
--   `docs/plans/org-connectors-and-channel-ingest.md` §10.
--
--   Today `oauth_tokens` holds exactly one row per
--   `(account_id, provider, provider_workspace_id)` — an account-wide token.
--   That is correct for Slack (one workspace bot token) and today's
--   Google/Microsoft connectors (one personal-account token), but Train 3
--   (Outlook delegated permissions) needs one token PER MEMBER of an org
--   account, same account/provider/workspace. Reusing the account-wide row
--   for that would mean the newest member's connect silently overwrites the
--   token every other member already relies on — the exact cross-member
--   credential leak `org_slack_oauth_enabled`'s own doc comment
--   (`routes/oauth.rs`) already names as the reason Microsoft/Google refuse
--   under an org account today.
--
--   `owner_user_id uuid` is nullable, added with no backfill:
--   `NULL` means "account-wide" (every existing row, unchanged), a non-NULL
--   value means "this member's own token". Because it is a genuinely new
--   dimension (not a default-filled column), a single `UNIQUE` constraint
--   cannot express both shapes at once — Postgres does not let two rows with
--   the same `(account_id, provider, provider_workspace_id)` collide when one
--   has `owner_user_id = NULL` and the other `= <uuid>`, but it also must not
--   allow two DIFFERENT members to double-connect the same workspace. Two
--   disjoint PARTIAL unique indexes solve this without a fifth column:
--
--     * `uq_oauth_tokens_account_wide` — the pre-existing 3-column key,
--       scoped to `owner_user_id IS NULL`. At most one account-wide row per
--       workspace, exactly like the constraint it replaces.
--     * `uq_oauth_tokens_per_owner` — the same 3 columns PLUS `owner_user_id`,
--       scoped to `owner_user_id IS NOT NULL`. At most one row per member per
--       workspace.
--
--   Application code must pick exactly one of the two shapes explicitly on
--   every read/write (`crate::connectors::TokenOwner::AccountWide | Member`)
--   — there is deliberately no `owner_user_id IS NULL OR owner_user_id = $n`
--   fallback anywhere; that would silently hand a member the account-wide
--   token, which is the bug this column exists to make structurally
--   impossible. An `ON CONFLICT` upsert must restate the matching index's
--   column list AND predicate — a bare 3-column `ON CONFLICT` target no
--   longer infers uniquely once the old constraint below is dropped.
--
--   Control-plane only, per the Train 1 plan (no tenant migrations for this
--   PR): every write path for `oauth_tokens` runs on the control/consumer
--   plane exclusively (see e.g. `routes/oauth.rs`'s own doc comments), and no
--   per-member Outlook token exists yet to backfill. `oauth_tokens` IS one of
--   the shared tables `scripts/check_schema_parity.sh` diffs between the
--   control and tenant trees (`CONTROL_ONLY_RLS_TABLES`), though, so the
--   identical schema-only change is paired into
--   `migrations_tenant/0059_oauth_tokens_owner_user_id.sql` to keep that gate
--   green — that pairing carries no behavior change: nothing reads or writes
--   `oauth_tokens` on a tenant pool today (confirmed by this PR's own review
--   of every call site) or will until Train 3 threads a `Member` selector
--   through org-plane code, which is separate, later work.
--
-- Down:
--   DROP INDEX IF EXISTS uq_oauth_tokens_per_owner;
--   DROP INDEX IF EXISTS uq_oauth_tokens_account_wide;
--   ALTER TABLE oauth_tokens
--     ADD CONSTRAINT oauth_tokens_account_id_provider_provider_workspace_id_key
--       UNIQUE (account_id, provider, provider_workspace_id);
--   ALTER TABLE oauth_tokens DROP COLUMN IF EXISTS owner_user_id;

ALTER TABLE oauth_tokens ADD COLUMN IF NOT EXISTS owner_user_id UUID;

ALTER TABLE oauth_tokens
    DROP CONSTRAINT IF EXISTS oauth_tokens_account_id_provider_provider_workspace_id_key;

CREATE UNIQUE INDEX IF NOT EXISTS uq_oauth_tokens_account_wide
    ON oauth_tokens (account_id, provider, provider_workspace_id)
    WHERE owner_user_id IS NULL;

CREATE UNIQUE INDEX IF NOT EXISTS uq_oauth_tokens_per_owner
    ON oauth_tokens (account_id, owner_user_id, provider, provider_workspace_id)
    WHERE owner_user_id IS NOT NULL;

-- ── Amendment (same PR, pre-merge): pin resolve_oauth_token_account_ids to
-- account-wide rows only ───────────────────────────────────────────────────
--
-- `resolve_oauth_token_account_ids(p_provider, p_workspace_id)` (0121) is the
-- webhook-routing self-heal fallback behind `routes::slack::
-- resolve_slack_account_ids` and (added on `main` after this migration was
-- first opened, PR #659) `connectors::microsoft::resolve_microsoft_account_ids`.
-- It answers "which account(s) have ANY oauth_tokens row for this
-- provider+workspace", returning a SETOF account_id with NO predicate on
-- `owner_user_id` — it was written before that column existed.
--
-- Once a `Member` row can exist (Train 3), an account with N per-member rows
-- for the same provider+workspace would make this function return that
-- account_id N times (one row per matching token, deduped only ACROSS the
-- two fallback source tables by the Rust caller, never WITHIN one SETOF
-- result — see `resolve_slack_account_ids`'s and `resolve_microsoft_account_ids`'s
-- own Rust-side bodies). Both callers fan out and act once per returned
-- account_id (`teams_bot::process_activity` explicitly "fans out over every
-- match" per `microsoft.rs`'s own doc comment on this function) — silent
-- duplicates here would silently duplicate whatever that fan-out does
-- (e.g. double-processing one Teams/Slack event for the same account).
--
-- Fixed the same way every other call site in this PR states its scope
-- explicitly (`crate::connectors::TokenOwner::AccountWide`, i.e.
-- `owner_user_id IS NULL`): today this is a no-op for both callers (Slack
-- never has per-member rows at all, per `docs/plans/org-connectors-and-
-- channel-ingest.md` §14.1; Microsoft has none yet, Train 3 not built), so
-- byte-identical current behavior. It also means this self-heal fallback
-- will NOT resolve an account whose ONLY row is a per-member one once Train 3
-- exists — a deliberate, conservative default (self-healing `connector_installs`
-- from per-member rows is a real question with real design space: dedup
-- semantics, which member "wins" the repair write, etc.) left for Train 3 to
-- decide explicitly and re-open this function with its own review, rather
-- than inheriting silently-changed fan-out behavior from a column Train 3
-- never touched. The FAST path (`resolve_connector_install_account_ids`,
-- reading `connector_installs`) is unaffected either way — it is written on
-- every install regardless of owner and carries no `owner_user_id` column of
-- its own — so per-member installs still route correctly through it; only
-- this slow, rarely-hit self-heal path narrows.
CREATE OR REPLACE FUNCTION resolve_oauth_token_account_ids(p_provider text, p_workspace_id text)
RETURNS SETOF uuid
LANGUAGE sql
SECURITY DEFINER
STABLE
SET search_path = public, pg_temp
AS $$
    SELECT account_id
    FROM oauth_tokens
    WHERE provider = p_provider
      AND provider_workspace_id = p_workspace_id
      AND owner_user_id IS NULL
    ORDER BY created_at, account_id
$$;
