-- Migration: 0252_connector_installs_dedupe_resolver
-- Description: CS-454 (item 46a, D21 — docs/plans/org-onboarding-from-the-
--   invite.md §3) — members connect their own Slack as `slack_user`.
--
--   `connector_installs` already carries the `owner_user_id` column and
--   both partial unique indexes this story needs
--   (`uq_connector_installs_account_wide`, `uq_connector_installs_per_owner`
--   — migration `0121_connector_installs`, whose own header drafted the
--   per-owner shape "created now so that dimension does not need its own
--   migration later"). No schema change is needed here — only the
--   RESOLVER FUNCTION that reads it.
--
--   ── The bug this closes ──
--
--   `resolve_connector_install_account_ids(p_provider, p_workspace_id)`
--   (0121) is:
--
--     SELECT account_id FROM connector_installs
--     WHERE provider = p_provider AND external_workspace_id = p_workspace_id
--     ORDER BY created_at, account_id
--
--   with no `owner_user_id` predicate and no de-duplication — correct on
--   0121's own premise, stated in its header, that "every row this
--   migration's backfill produces, and every row the paired Rust write path
--   inserts, therefore has `owner_user_id IS NULL` today". CS-454 is the
--   first write path to make that premise false: `routes::oauth::callback`'s
--   `connector_installs_provider` write for a `slack_member` connect inserts
--   a SECOND row for the SAME `(provider='slack', external_workspace_id)`
--   pair an admin's bot install already holds — same account, same
--   workspace, different `owner_user_id`. The unmodified function above then
--   returns that account's `account_id` TWICE (once per row) for any
--   workspace with both a bot install and one-or-more member connects.
--
--   `routes::slack::resolve_slack_account_ids` (the function's only Rust
--   caller) treats every element of that result as a DISTINCT match to fan
--   out over — `routes::slack_events.rs`'s own module comment states this
--   in as many words: "Fan-out is correct, not duplication." A duplicated
--   account_id would silently double-process one inbound Slack event for
--   the SAME account (double-ingest, or worse for an at-most-once side
--   effect) the moment any account has both an account-wide row and a
--   per-owner row — which CS-454 is the first thing in this codebase to
--   ever create.
--
--   Precedent for the fix shape: `0175_oauth_tokens_owner_user_id` hit the
--   identical class of bug for `resolve_oauth_token_account_ids` (the
--   sibling self-heal fallback resolver) and closed it by PINNING that
--   function to `owner_user_id IS NULL` — safe there because Slack's bot
--   token is the ONLY thing that resolver was ever asked to find. Pinning
--   is wrong HERE, though: item 3 of this story is explicitly "connector_
--   installs gains per-owner rows so workspace→account routing still works
--   for member connects" — an account whose ONLY row for a workspace is a
--   per-owner one (not reachable yet, since a member connect requires an
--   existing bot install — see `routes::oauth::
--   slack_member_workspace_has_bot_install`'s own doc comment — but the
--   resolver must not assume that invariant forever) must still resolve.
--   The fix is therefore DEDUPLICATION, not a narrower predicate: every
--   `account_id` that installed the workspace in EITHER regime, returned
--   EXACTLY ONCE, ordered by its EARLIEST install (`MIN(created_at)`) so two
--   installs collapsing into one row preserves the same "earliest installer
--   first" ordering contract the un-deduplicated version documented.
--
--   `SELECT DISTINCT account_id ... ORDER BY created_at` is invalid SQL
--   once `created_at` is not itself in the SELECT list (Postgres requires
--   every `ORDER BY` expression to appear in the `DISTINCT` list) — a
--   `GROUP BY account_id` with `ORDER BY MIN(created_at), account_id` is
--   the equivalent that also carries the right semantics: "when did this
--   ACCOUNT first show up for this workspace", not any one row's timestamp.
--
--   `CREATE OR REPLACE FUNCTION` on an already-merged migration's function
--   is the SAME sanctioned pattern `0175_oauth_tokens_owner_user_id` already
--   used for this exact function's sibling
--   (`resolve_oauth_token_account_ids`) and for `resolve_connector_install_
--   account_ids`'s own SETOF/`SECURITY DEFINER`/`STABLE`/pinned-`search_path`
--   shape — see that migration's own header for why a `LANGUAGE sql`
--   function may be redefined this way (Postgres tracks it as a normal
--   object, not immutable the way a completed data migration is) while the
--   TABLE `0121` created stays untouched — no `ALTER TABLE`, no new index,
--   no migrations_tenant pairing (this table is control-only, `0121`'s own
--   "Why control-only" section — a function-body-only change over a
--   control-only table needs no tenant twin, same as `0175`'s own resolver
--   fix needed none).
--
-- Depends on: 0121_connector_installs (the table, indexes, and the function
--   this migration replaces the body of).
--
-- Down:
--   CREATE OR REPLACE FUNCTION resolve_connector_install_account_ids(p_provider text, p_workspace_id text)
--   RETURNS SETOF uuid
--   LANGUAGE sql
--   SECURITY DEFINER
--   STABLE
--   SET search_path = public, pg_temp
--   AS $$
--       SELECT account_id
--       FROM connector_installs
--       WHERE provider = p_provider
--         AND external_workspace_id = p_workspace_id
--       ORDER BY created_at, account_id
--   $$;

CREATE OR REPLACE FUNCTION resolve_connector_install_account_ids(p_provider text, p_workspace_id text)
RETURNS SETOF uuid
LANGUAGE sql
SECURITY DEFINER
STABLE
SET search_path = public, pg_temp
AS $$
    SELECT account_id
    FROM connector_installs
    WHERE provider = p_provider
      AND external_workspace_id = p_workspace_id
    GROUP BY account_id
    ORDER BY MIN(created_at), account_id
$$;
