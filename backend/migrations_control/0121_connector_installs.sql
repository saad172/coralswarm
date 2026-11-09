-- Migration: 0121_connector_installs
-- Description: the control-plane routing index that maps an inbound
--   connector notification (Slack `team_id` today, a Microsoft tenant id
--   tomorrow) to every CoralSwarm account that has installed it — the table
--   `docs/plans/org-connectors-and-channel-ingest.md` §9c calls
--   `connector_installs`. An inbound Slack webhook arrives UNAUTHENTICATED,
--   carrying only a Slack `team_id`, and the apex router must resolve which
--   tenant(s) own that workspace BEFORE it can touch any content or open a
--   tenant pool — this table, and only this table, answers that first hop.
--
--   ── Note on the migration number (renumbered TWICE) ──
--   Written against an instruction naming the control head as 0113 ("use
--   exactly 0114"). By the time the PR was built main had advanced to 0116
--   (`0114_promote_event_type`, `0115_query_history_created_at_index`,
--   `0116_atoms_excerpt`), so it was first numbered 0117. While the PR sat
--   open main advanced AGAIN and merged its own `0117_journal_tier_rollups`,
--   plus `0118_users_timezone` and
--   `0119_backfill_slack_atoms_ingested_by` — a direct collision, caught on
--   rebase before merge.
--
--   Hence 0121 (renumbered a THIRD time: while the PR sat green awaiting a
--   merge window, main merged `0120_query_history_model` too). The general rule this keeps demonstrating: a migration
--   number is only valid at the instant it is merged. It is decided by the
--   ACTUAL head at merge time, never by the head when the branch was cut and
--   never by an instruction written earlier. Re-check on every rebase; an
--   unmerged migration is free to renumber, a merged one is not (sqlx
--   checksums it and `release_command` is fail-closed).
--
--   ── Why control-only, WITH EVIDENCE (not just asserted) ──
--   Migration 0106 (`team_seat_grants`) declared its table control-only on
--   the premise that `teams`/`team_members` were absent from the tenant
--   tree — a premise that was FALSE (both exist in `migrations_tenant`) and
--   shipped a latent parity gap no guard caught, fixed later by pairing
--   `migrations_tenant/0017`. This table's control-only claim rests on
--   different, independently checkable ground:
--
--     1. `connector_installs.account_id` must reference `accounts(id)` (see
--        the column below) — and `accounts` exists ONLY in the control tree.
--        Checked directly against this repo at the commit this migration
--        was written on:
--          $ grep -rn 'CREATE TABLE.*accounts\b' migrations_tenant/*.sql
--          (no output)
--          $ grep -rn 'CREATE TABLE.*accounts\b' migrations_control/*.sql
--          migrations_control/0001_init.sql:28:CREATE TABLE IF NOT EXISTS accounts (
--        A tenant-tree twin of this table is not merely undesired, it is
--        IMPOSSIBLE without inventing a tenant-side `accounts` concept —
--        unlike `team_seat_grants`, which shadowed a table
--        (`teams`/`team_members`) that genuinely existed on both planes.
--        `backend/src/tenancy/extractor.rs` and this design doc's own §10
--        independently document the same fact: a tenant pool has no
--        `accounts` table (`relation "accounts" does not exist`).
--
--     2. FUNCTIONALLY circular, not just structurally absent: this table's
--        entire purpose (§9c) is to let the apex discover WHICH tenant pool
--        to open before it knows that — a tenant-side copy would require
--        already having resolved the tenant to query the very table whose
--        job is resolving the tenant. There is no coherent "per-tenant"
--        version of a pre-tenant-resolution index.
--
--     3. Verified against the ACTUAL write paths, not assumed: as of this
--        migration, every route that can write a connector install
--        (`routes/oauth.rs::callback`, `routes/slack.rs::oauth_callback`) is
--        refused outright under an org `TenancyDecision`
--        (`reject_org_oauth_management` / `reject_org_slack_management`,
--        both testing `tenant.org_context()`), so as of today NO code path
--        writes this table against anything but the shared consumer pool —
--        `tenant.pool` for Personal/flag-off IS `state.db`, the control/
--        consumer database this migration applies to.
--
--   Consequently this table is registered in
--   `scripts/check_schema_parity.sh`'s `CONTROL_TABLES` list in this same
--   PR, per that script's own instruction, and there is deliberately no
--   paired tenant migration.
--
--   ── Column choices ──
--   `provider` / `external_workspace_id` — the outer routing key: Slack
--   `team_id`, a future Microsoft tenant id, etc. Both `TEXT NOT NULL`,
--   non-empty (`CHECK`), matching the `team_seat_grants.source` non-empty
--   convention (0106).
--
--   `external_user_id` — the PROVIDER-native id of the human who completed
--   the install (Slack `authed_user.id`, Microsoft `oid`), nullable for a
--   workspace-level install with no single attributable installer. This is
--   NOT `owner_user_id` (below) — it is provider-native and carries no
--   scoping meaning by itself; it exists so an operator can trace "who
--   clicked install" without decrypting `oauth_tokens`.
--
--   `clerk_org_id` — nullable, `NULL` = consumer/personal plane, matching
--   the `clerk_org_id TEXT`-with-no-FK convention this repo already uses on
--   `accounts`/`org_membership_tombstones`/`org_revocations`/
--   `member_ocean_desired_state` (migrations 0027/0081/0082/0090/0092): a
--   Clerk org id is an external string CoralSwarm does not own the
--   lifecycle of, so a local FK would be a false promise of referential
--   integrity Clerk itself does not offer us.
--
--   `account_id` — `UUID NOT NULL REFERENCES accounts(id) ON DELETE CASCADE`.
--   Unlike `clerk_org_id`, this IS a local table CoralSwarm owns, so a real
--   FK is both possible and correct — same shape as `oauth_tokens.account_id`
--   and `slack_integrations.account_id`, the two tables this one is derived
--   from (see the backfill below).
--
--   `owner_user_id` — bare `UUID`, deliberately NO foreign key, the SAME
--   convention already used twice for the identical "nullable per-member
--   scoping dimension" shape: `graphs.owner_user_id` (0090/tenant 0005) and
--   the not-yet-built `oauth_tokens.owner_user_id` this design doc's own
--   §10 drafts with the identical "NULL = account-wide" semantics. It is
--   drafted here, alongside the same table that will eventually read it,
--   even though NOTHING WRITES IT YET: no write path in this codebase has a
--   per-member-scoped install to record today. Slack's bot token is
--   workspace-wide by construction, so it is account-wide by design, not by
--   omission (§14.1 of the design doc measures this directly: Slack's
--   `user_token_encrypted` is written and never read). Every row this
--   migration's backfill produces, and every row the paired Rust write path
--   inserts, therefore has `owner_user_id IS NULL` today — but the column
--   exists now so a future per-member connector (§10's Microsoft delegated-
--   token case) does not need another migration to gain the dimension this
--   table was always meant to carry.
--
--   `created_at` — `TIMESTAMPTZ NOT NULL DEFAULT now()`, the first-install
--   timestamp. Deliberately NOT touched by the re-install upsert (see the
--   Rust write path in `routes/oauth.rs::record_connector_install` /
--   `routes/slack.rs::oauth_callback`) — a reconnect updates
--   `external_user_id`/`clerk_org_id` but preserves how long this account
--   has actually held the workspace.
--
--   ── Primary key ──
--   A surrogate `id UUID PRIMARY KEY DEFAULT gen_random_uuid()`, same
--   precedent as `team_seat_grants` (0106): the natural business key is a
--   composite the uniqueness rule below already enforces, but every table
--   in this codebase that isn't purely append-only ledger-shaped
--   (`activity_events`, etc.) carries a surrogate PK for row addressability,
--   debugging, and (should this table ever need it) logical-replication
--   REPLICA IDENTITY — Postgres refuses to replicate UPDATE/DELETE on a
--   table with no PK and no explicit REPLICA IDENTITY.
--
--   ── Uniqueness: TWO partial unique indexes, not one plain UNIQUE ──
--   The "prevent duplicate rows for the same install, but allow the
--   legitimate multi-tenant case" requirement has a trap: a plain
--   `UNIQUE (provider, external_workspace_id, account_id, owner_user_id)`
--   does NOT prevent duplicates for account-wide installs, because Postgres
--   treats every `NULL` as distinct from every other `NULL` under an
--   ordinary unique constraint — and `owner_user_id` is NULL on EVERY row
--   this table holds today (see above). Two INSERTs for the same
--   `(provider, external_workspace_id, account_id)` with `owner_user_id`
--   both NULL would NOT collide under that shape, silently defeating the
--   exact de-duplication this column exists to provide. This is the
--   identical hazard §10 of the design doc already worked through for
--   `oauth_tokens.owner_user_id` (not yet built), and the fix is the same
--   one already drafted there: two PARTIAL unique indexes, split on whether
--   `owner_user_id` is NULL, so uniqueness is enforced separately within
--   each regime instead of across a column NULL never matches itself in.
--   `account_id` is part of BOTH indexes precisely so two different
--   accounts installing the SAME workspace — two customers, or a customer
--   plus someone's personal account — is not merely permitted but is the
--   documented multi-tenant case §9c's router fans out to.
--
--   ── The lookup index ──
--   Neither partial unique index above can serve the router's actual read
--   pattern (`(provider, external_workspace_id)` -> every matching account,
--   §9c step 1) as a covering index: a partial index is only usable by the
--   planner when the query's WHERE clause implies the index's own predicate,
--   and the router's query does not (and must not) filter on
--   `owner_user_id` at all — that is exactly the dimension it doesn't know
--   yet. So a third, non-partial, non-unique index carries the actual
--   lookup.
--
--   ── No RLS, deliberately (same class as 0084/0086's carve-outs) ──
--   `oauth_tokens`/`slack_integrations` need `SECURITY DEFINER` bootstrap
--   functions (0084/0086) specifically BECAUSE they carry `account_isolation`
--   RLS policies that would otherwise block the very "which account owns
--   this workspace?" query those functions exist to answer before a scope
--   is known. This table has the identical bootstrap ROLE but is spared the
--   identical problem: it carries no RLS policy in the first place, so
--   there is no policy to bypass. Adding `account_isolation` RLS to a table
--   whose whole reason to exist is being queried WITHOUT an account_id
--   would be actively wrong, not merely redundant — the router cannot
--   `SET LOCAL app.account_id` to a value it is trying to discover. This is
--   safe precisely because the table holds "routing tuples only, no
--   secrets" (§9c, verbatim): no OAuth token, no bot token, nothing an
--   unscoped read could exfiltrate beyond "which account/org installed
--   which workspace", which the resolver function below already limits any
--   caller to.
--
--   ── The resolver function, and why it is NEW rather than an edit ──
--   `resolve_oauth_account_id` (0084) and `resolve_slack_integration_account_id`
--   (0086) are both `SELECT ... LIMIT 1` with NO `ORDER BY` — an arbitrary,
--   non-deterministic pick the moment two accounts install the same
--   workspace, silently orphaning every account but the one Postgres
--   happens to return. Both are ALREADY-MERGED migrations and may not be
--   edited (sqlx checksums every file; editing either breaks every deployed
--   DB's `_sqlx_migrations` ledger and fail-closes every future deploy via
--   `release_command`). The fix therefore lands as a NEW function,
--   `resolve_connector_install_account_ids`, returning `SETOF uuid` (every
--   match, not one), ordered deterministically (`created_at, account_id`).
--   It reads ONLY `connector_installs`, which — per the backfill below —
--   is derived from BOTH `oauth_tokens` AND `slack_integrations`, so it is a
--   strict superset of what either older function alone could see (0086's
--   own header notes `resolve_oauth_account_id` would silently orphan
--   legacy `slack_integrations`-only installs; this function orphans
--   neither). Both old functions are left in place, unedited (0084/0086 may
--   not be touched — see above); `resolve_oauth_account_id` keeps its one
--   remaining caller (`teams_bot.rs`, for Microsoft — that provider's own
--   fix is the NEXT step's job, not this one's). `resolve_slack_integration_
--   account_id`'s only Rust caller, `slack_events.rs::ingest_slack_message`
--   (the highest-traffic call site of all — every inbound message, not just
--   the four lower-volume sites `resolve_slack_account_id` already covered),
--   is REPOINTED at `resolve_slack_account_id` in this same PR, so the
--   arbitrary-pick bug is fixed there too, not just at the four lighter
--   sites; `resolve_slack_integration_account_id` itself is left with no
--   remaining caller in this tree but is not dropped (an already-merged
--   migration's function; removing the call site is this PR's job, removing
--   the function is a separate one). Same `SECURITY DEFINER` / `STABLE` / pinned `search_path` / narrow
--   return-columns discipline as every function in 0084/0086, and the same
--   REVOKE-FROM-PUBLIC-then-GRANT-back closing, for the identical reason
--   (0084's header): a `SECURITY DEFINER` function defaults to PUBLIC
--   EXECUTE, which would let any role with mere `CONNECT` enumerate
--   cross-account workspace installs.
--
--   Registered in `scripts/check_schema_parity.sh`'s `CONTROL_FUNCTIONS`
--   list in this same PR — belt-and-suspenders alongside the table itself
--   already being dropped via `CONTROL_TABLES` (a `LANGUAGE sql` function's
--   body is parsed at CREATE time, so Postgres already tracks a `pg_depend`
--   dependency on `connector_installs` and the earlier `DROP TABLE ...
--   CASCADE` would take the function with it either way — explicit
--   registration just makes that not-accidental).
--
--   ── The backfill ──
--   Derives historical rows from `oauth_tokens` (the modern OAuth path) and
--   `slack_integrations` (the legacy `routes/slack.rs::oauth_callback` path
--   — 0086's own header is the citation that these two tables can each hold
--   installs the other does not know about), so the live production
--   workspace(s) already connected before this migration do not vanish from
--   the router's index the moment a later PR starts trusting it. Both
--   INSERTs target the SAME partial unique index
--   (`uq_connector_installs_account_wide`, `owner_user_id IS NULL` — the
--   only regime that exists today) with `ON CONFLICT ... DO NOTHING`, so:
--     * running this migration twice (impossible under sqlx's ledger, but
--       true of the raw SQL in isolation) is a no-op the second time;
--     * if the SAME (provider, workspace, account) has rows in BOTH source
--       tables, the `oauth_tokens`-derived INSERT (which runs first) wins
--       and the `slack_integrations`-derived one silently skips — an
--       acceptable choice since both rows describe the identical
--       underlying install fact, and `oauth_tokens.provider_user_id` is a
--       first-party OAuth claim, a strictly stronger provenance signal than
--       anything the legacy exchange parses.
--   `oauth_tokens.provider_workspace_id` is nullable (not every provider is
--   workspace-scoped — e.g. a per-user Google Calendar connection has none)
--   and `external_workspace_id` is `NOT NULL` here, so rows with a NULL
--   `provider_workspace_id` are correctly excluded rather than backfilled
--   as a broken row. The backfill block below is delimited with explicit
--   markers so `backend/tests/connector_installs_pg.rs` can read this
--   exact migration file at test time and re-execute the identical SQL text
--   against freshly-seeded rows — proving the test asserts the literal
--   production statement, not a hand-copied approximation that could drift
--   from it.
--
--   ── OPERATOR-FACING: is this table's index ever allowed to be stale? ──
--   NO — this table is now SELF-HEALING, not merely backfilled once. This
--   was NOT the original design (see the git history of this migration for
--   the superseded "backfill + two transactional callbacks are sufficient,
--   with a recommended-but-not-built periodic reconciler as a safety net"
--   analysis) — it was upgraded to inline self-healing on founder decision
--   during org-slack step 1 review, after SIX independent pre-existing
--   `#[cfg(test)]` fixtures (across `routes/slack_events.rs`'s plain
--   `mod tests` and its `slack_watched_channel_destination_tests` module —
--   see `routes::slack::connector_install_self_heal_tests`'s own module doc
--   for the full list) were found to do exactly the "obvious" thing —
--   hand-INSERT an `oauth_tokens`/`slack_integrations` row and expect
--   routing to find it — and broke the moment this migration's resolvers
--   went live. An investigation at the time found NO reachable production
--   or operational path that bypasses the two transactional callbacks
--   (`routes/oauth.rs::callback` / `routes/slack.rs::oauth_callback`, both
--   still write `connector_installs` in the SAME transaction as the row
--   they mirror — unchanged, see `routes::oauth::record_connector_install`'s
--   own doc comment) — but six-for-six was accepted as strong enough
--   evidence that a FUTURE write path (an admin/support tool, a
--   provider-migration script, a manual data repair) could reintroduce the
--   same gap without its author ever suspecting a second table needs
--   touching, and the failure would be exactly as silent as it was for
--   those six tests: no error, `connector_health` warns only on the NEXT
--   inbound event, and by then an operator is debugging "why did messages
--   stop", not "why didn't this install work".
--
--   THE MECHANISM (`routes/slack.rs::resolve_slack_account_ids`, its own
--   doc comment has the full design): on a `connector_installs` miss, fall
--   back to querying `oauth_tokens`/`slack_integrations` DIRECTLY via two
--   NEW `SETOF`-returning `SECURITY DEFINER` functions added by THIS
--   migration (`resolve_oauth_token_account_ids` / `resolve_slack_
--   integration_account_ids`, below) — deliberately NOT the existing
--   `LIMIT 1` `resolve_oauth_account_id`(0084)/`resolve_slack_integration_
--   account_id`(0086), which would silently reintroduce the arbitrary-pick
--   bug this whole migration exists to kill, only on the repair path where
--   nobody would think to look for it. Every account the fallback finds is
--   returned to the caller — correctness comes from THIS query — and then,
--   best-effort, repaired into `connector_installs` (`ON CONFLICT DO
--   NOTHING`) so the NEXT resolve for that workspace hits the fast path.
--   A failed repair write costs one extra fallback query on the next
--   resolve and nothing else — it deliberately does NOT fail the resolve
--   the way a failed install-time write does (that asymmetry is
--   intentional: at install time the index was the SOLE source, so
--   best-effort meant silent loss; here the fallback tables ARE the source
--   of truth and the index is a cache repairing itself).
--
--   A workspace with NO installer anywhere (Slack's app still present and
--   delivering signed events, but every local row gone — e.g. an
--   incomplete disconnect) has nothing to repair and would otherwise
--   re-run both fallback queries on every single inbound message forever;
--   a process-local negative-result cache
--   (`routes/slack.rs::negative_resolve_cache`, 5-minute TTL) bounds that.
--
--   Healed and genuinely-nowhere misses are DISTINCT, differently-worded
--   `connector_health=true` log lines (`resolve_slack_account_ids`'s own
--   comment) — a repair happening is itself the signal that some route is
--   bypassing `record_connector_install`, and that is worth knowing even
--   though the message was not lost.
--
-- Depends on: 0001_init (accounts), 0006/0021-numbered `oauth_tokens`
--   (this repo's `oauth_tokens` table, migration 0006 per its own header —
--   see that file), 0015_slack_integrations, 0083_consumer_rls (creates
--   `coralswarm_app`, whose default-privilege grant from that migration's
--   `ALTER DEFAULT PRIVILEGES` already covers this new table with no extra
--   GRANT statement needed here), 0084_bootstrap_scope_functions (the
--   `SECURITY DEFINER` bootstrap pattern this migration's function follows).
--
-- Down:
--   REVOKE EXECUTE ON FUNCTION resolve_connector_install_account_ids(text,text) FROM coralswarm_app;
--   REVOKE EXECUTE ON FUNCTION resolve_oauth_token_account_ids(text,text) FROM coralswarm_app;
--   REVOKE EXECUTE ON FUNCTION resolve_slack_integration_account_ids(text) FROM coralswarm_app;
--   DROP FUNCTION IF EXISTS resolve_connector_install_account_ids(text,text);
--   DROP FUNCTION IF EXISTS resolve_oauth_token_account_ids(text,text);
--   DROP FUNCTION IF EXISTS resolve_slack_integration_account_ids(text);
--   DROP INDEX IF EXISTS idx_connector_installs_workspace_lookup;
--   DROP INDEX IF EXISTS uq_connector_installs_per_owner;
--   DROP INDEX IF EXISTS uq_connector_installs_account_wide;
--   DROP TABLE IF EXISTS connector_installs;

CREATE TABLE IF NOT EXISTS connector_installs (
    id                     UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    provider               TEXT        NOT NULL CHECK (provider <> ''),
    external_workspace_id  TEXT        NOT NULL CHECK (external_workspace_id <> ''),
    external_user_id       TEXT,
    clerk_org_id           TEXT,                  -- bare text, no FK (0027/0081/0082/0090/0092 precedent)
    account_id             UUID        NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    owner_user_id          UUID,                  -- bare uuid, no FK (graphs.owner_user_id precedent); NULL = account-wide (always true today)
    created_at             TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- One row per (provider, workspace, account) when the install is
-- account-wide (owner_user_id IS NULL) — the only regime that exists today.
-- The `DO UPDATE` target for the Rust write path's re-install upsert; the
-- `DO NOTHING` target for the backfill below.
CREATE UNIQUE INDEX IF NOT EXISTS uq_connector_installs_account_wide
    ON connector_installs (provider, external_workspace_id, account_id)
    WHERE owner_user_id IS NULL;

-- Mirror for a future per-member-scoped install (owner_user_id IS NOT NULL)
-- — not reachable by any write path today (see this migration's header),
-- created now so that dimension does not need its own migration later.
CREATE UNIQUE INDEX IF NOT EXISTS uq_connector_installs_per_owner
    ON connector_installs (provider, external_workspace_id, account_id, owner_user_id)
    WHERE owner_user_id IS NOT NULL;

-- The router's actual read pattern: EVERY account that installed this
-- workspace, regardless of owner_user_id — see this migration's header for
-- why neither partial unique index above can serve this query.
CREATE INDEX IF NOT EXISTS idx_connector_installs_workspace_lookup
    ON connector_installs (provider, external_workspace_id);

-- ── The genuine multi-row resolver ──────────────────────────────────────────
-- Bootstrap carve-out, same shape and same reasoning as 0084/0086: this
-- function answers "which account(s) own this workspace?" for a caller that
-- has only a provider + workspace id and no account_id to scope a
-- connection with yet. Returns ONLY account_id — never external_user_id,
-- never clerk_org_id — so it cannot be used to enumerate anything beyond
-- "which account(s) installed this workspace", the same narrow surface
-- 0084/0086's functions expose for their own tables.
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
    ORDER BY created_at, account_id
$$;

REVOKE EXECUTE ON FUNCTION resolve_connector_install_account_ids(text, text) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION resolve_connector_install_account_ids(text, text) TO coralswarm_app, CURRENT_USER;

-- ── Self-healing fallback resolvers (founder decision, org-slack step 1
-- review) ────────────────────────────────────────────────────────────────
-- `resolve_slack_account_ids` (`routes/slack.rs`) reads `connector_installs`
-- FIRST and only falls back to these when that comes back empty — see this
-- migration's header ("OPERATOR-FACING") for the full design rationale and
-- why an inline fallback, done THIS way, does not reintroduce the arbitrary-
-- pick bug the rest of this migration exists to kill: both are `SETOF`
-- (every match, not one), same bootstrap `SECURITY DEFINER` carve-out as
-- `resolve_connector_install_account_ids` above (unauthenticated webhook
-- caller, no `app.account_id` to scope a connection with), same narrow
-- "account_id only" return surface. NOT a replacement for
-- `resolve_oauth_account_id` (0084) / `resolve_slack_integration_account_id`
-- (0086) — those keep their own callers (`teams_bot.rs`,
-- `ingest_slack_message`) unchanged; these are NEW, additional functions
-- because 0084/0086 are already-merged migrations that may not be edited.
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
    ORDER BY created_at, account_id
$$;

CREATE OR REPLACE FUNCTION resolve_slack_integration_account_ids(p_workspace_id text)
RETURNS SETOF uuid
LANGUAGE sql
SECURITY DEFINER
STABLE
SET search_path = public, pg_temp
AS $$
    SELECT account_id
    FROM slack_integrations
    WHERE workspace_id = p_workspace_id
    ORDER BY created_at, account_id
$$;

REVOKE EXECUTE ON FUNCTION resolve_oauth_token_account_ids(text, text) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION resolve_slack_integration_account_ids(text) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION resolve_oauth_token_account_ids(text, text) TO coralswarm_app, CURRENT_USER;
GRANT EXECUTE ON FUNCTION resolve_slack_integration_account_ids(text) TO coralswarm_app, CURRENT_USER;

-- ── Backfill: derive historical rows from the two existing install tables ──
-- Delimited with BEGIN/END markers so backend/tests/connector_installs_pg.rs
-- can extract and re-execute this EXACT text against freshly-seeded rows —
-- see this migration's header.
-- BACKFILL BEGIN
INSERT INTO connector_installs
    (provider, external_workspace_id, external_user_id, clerk_org_id, account_id, owner_user_id, created_at)
SELECT
    ot.provider,
    ot.provider_workspace_id,
    NULLIF(ot.provider_user_id, ''),
    a.clerk_org_id,
    ot.account_id,
    NULL,
    ot.created_at
FROM oauth_tokens ot
JOIN accounts a ON a.id = ot.account_id
WHERE ot.provider_workspace_id IS NOT NULL
ON CONFLICT (provider, external_workspace_id, account_id) WHERE owner_user_id IS NULL DO NOTHING;

INSERT INTO connector_installs
    (provider, external_workspace_id, external_user_id, clerk_org_id, account_id, owner_user_id, created_at)
SELECT
    'slack',
    si.workspace_id,
    NULL,
    a.clerk_org_id,
    si.account_id,
    NULL,
    si.created_at
FROM slack_integrations si
JOIN accounts a ON a.id = si.account_id
ON CONFLICT (provider, external_workspace_id, account_id) WHERE owner_user_id IS NULL DO NOTHING;
-- BACKFILL END
