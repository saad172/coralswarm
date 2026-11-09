-- Migration: 0083_consumer_rls
-- Description: Consumer-plane Row-Level Security BACKSTOP (org-tenancy PR 3.4,
--              docs/plans/org-tenancy-implementation-plan.md Phase 3). The
--              control DB doubles as the shared consumer/personal-content
--              plane (personal graphs/atoms/queries/etc. live here
--              indefinitely — see the plan's "migration-split problem"
--              section). This migration adds a database-enforced backstop so
--              that even a future application-layer scoping bug cannot read
--              one account's content under another account's request: if a
--              route/worker ever forgets an `account_id = $1` WHERE clause,
--              Postgres itself refuses the cross-account rows, for any
--              connection running as the `coralswarm_app` role.
--
--              Two independent pieces:
--
--              1. `coralswarm_app` — a NEW, non-BYPASSRLS role. Created with
--                 NO PASSWORD here (a migration file is committed to git;
--                 committing a real credential would defeat the point of
--                 having one). The password is set out-of-band, once, as part
--                 of OPS-4 (see below) — never in this file, never in a
--                 migration, never logged.
--
--              2. RLS + one policy per CONTENT table, covering the FULL
--                 39-table tenant/consumer content roster (see the
--                 enumeration below) — not just the tables with a direct
--                 `account_id` column. A first pass of this migration
--                 covered only the 15 direct-`account_id` tables, on the
--                 theory that `graphs` (RLS'd, the join anchor) was enough
--                 leverage — a security review correctly rejected that: RLS
--                 policies are evaluated per TABLE, so a query reading
--                 `atoms`/`nodes`/etc. directly by `graph_id`/`atom_id` (the
--                 normal, common access pattern — e.g.
--                 `SELECT embedding FROM atoms WHERE atom_id = ANY($1)`,
--                 never joining `graphs`) is NEVER filtered by `graphs`'
--                 policy. An unprotected `graph_id`-keyed content table is
--                 exactly the class of app-layer-bug leak this backstop
--                 exists to catch, so it is covered here too, via a
--                 subquery anchored back to `graphs.account_id`.
--
--              Predicate — the plan's exact form for tables with a direct
--              `account_id` column:
--
--                  account_id = current_setting('app.account_id')::uuid
--
--              and, for tables reached only through `graph_id` (or a further
--              hop through `nodes`/`conversations`/`teams`/`team_keys`), the
--              same predicate applied through a subquery back to `graphs`
--              (itself RLS'd under the identical predicate, so a nested
--              subquery against `graphs`/`nodes`/etc. is filtered
--              consistently, not a fresh policy surface):
--
--                  graph_id IN (SELECT id FROM graphs
--                               WHERE account_id = current_setting('app.account_id')::uuid)
--
--              Deliberately the STRICT single-argument `current_setting`
--              (no `missing_ok`/default): if `app.account_id` was never set
--              in the session, this RAISES (`unrecognized configuration
--              parameter`) rather than silently returning zero rows. A loud
--              failure the first time a code path forgets to scope a
--              connection is far better than a quiet, hard-to-notice "this
--              query returns nothing" bug — this is a deliberate fail-LOUD
--              choice, not fail-open. (Once a session HAS set the GUC at
--              least once, Postgres treats `app.*` as an invented
--              "placeholder" custom GUC for the rest of that session, and it
--              reverts to `''` — not "unset" — outside the transaction that
--              set it; casting `''::uuid` is still a hard error, so the
--              fail-loud property holds either way. See
--              `tenancy::scope`/`consumer_rls_pg.rs` for the exact proof.)
--
--              **Full 39-table content roster, by how each is covered:**
--
--              (a) Direct `account_id` column (15) — policy is the bare
--                  predicate above:
--                    activity_events, conversations, graph_subscriptions,
--                    graphs, llm_usage, mcp_tool_calls, oauth_tokens,
--                    outlook_watched_conversations, outlook_workflows,
--                    queries, query_history, slack_huddle_captures,
--                    slack_integrations, teams, teams_watched_resources.
--                  Two of these (`llm_usage`, `mcp_tool_calls`) have a
--                  NULLABLE `account_id` for system/unattributed rows —
--                  those rows become invisible to `coralswarm_app`
--                  (`account_id = x` is NULL, i.e. not-true, for a NULL
--                  `account_id`) — a known, accepted, fail-closed
--                  consequence, not a bug.
--
--              (b) Direct `graph_id` column, one hop to `graphs` (13):
--                    atoms, nodes, relationship_edge, node_merges,
--                    meeting_transcripts, transcript_corrections,
--                    meeting_shares, projects, coding_sessions,
--                    coding_session_shares, team_keys, conversation_oceans,
--                    op_log (`op_log.graph_id` is NULLABLE — same accepted
--                    NULL-hides-the-row consequence as (a)'s two tables;
--                    `meeting_transcripts`/`transcript_corrections`'
--                    `graph_id` carries no FK constraint to `graphs` — noted
--                    "soft" in 0056's own header — but the column exists and
--                    the subquery works identically whether or not it's
--                    FK-enforced).
--
--              (c) One hop to a table in (a)/(b), then to `graphs` (7):
--                    node_atoms (via `node_id` → `nodes`), supersession_edge
--                    (via `superseded_id` → `nodes` — an edge's two endpoints
--                    are always same-graph by construction, so either
--                    endpoint anchors it identically), non_supersession_pair
--                    (via `node_a_id` → `nodes`), conversation_messages (via
--                    `conversation_id` → `conversations`, which already
--                    carries `account_id` directly — no need to go all the
--                    way to `graphs`), conversation_surfaced_edges (same,
--                    via `conversation_id`), team_members (via `team_id` →
--                    `teams`, which carries `account_id` directly),
--                    team_member_key_envelopes (via `team_key_id` →
--                    `team_keys` → `graphs`).
--
--              (d) Two anchors, SPLIT by command: node_promotions —
--                  `source_node_id` → `nodes` → `graphs` and/or
--                  `target_graph_id` → `graphs` directly (a promotion
--                  crosses two oceans). READ (`FOR SELECT`) is permissive —
--                  either anchor makes a row visible, since it should be
--                  readable from either side. WRITE (`FOR INSERT`/`UPDATE`/
--                  `DELETE`) requires BOTH anchors — the acting account must
--                  own the source reef's graph AND the target graph — because
--                  every legitimate consumer-plane promotion is same-account
--                  (cross-account promotion is a separate, narrow cross-DB
--                  service that never writes this table). Four separate
--                  policies, not one bare `USING`-only policy: a bare policy
--                  is `FOR ALL` and reuses `USING` for `WITH CHECK` too, which
--                  would let account A insert/update a row whose
--                  `target_graph_id` belongs to account B merely because A
--                  owns the source side — a real cross-account write, caught
--                  by security review and closed here. See the policies'
--                  own comments below for the full reasoning and why this is
--                  the ONLY table in this migration needing the split (every
--                  other predicate anchors to exactly one account, so
--                  USING-reused-as-WITH-CHECK is correct for it).
--
--              (e) FLAGGED, NOT COVERED — no clean, non-fragile anchor to an
--                  `account_id` exists, so no policy is added (per the
--                  review: "flag it, don't leave it silently uncovered"):
--                    - `jobs`: no `graph_id`/`account_id` column at all — the
--                      job's target entity lives inside the untyped `payload`
--                      JSONB and its shape varies per `kind`. A JSONB-path
--                      policy keyed on an assumed `payload->>'graph_id'`
--                      would silently stop protecting (or worse, silently
--                      mis-scope) any job kind whose payload doesn't carry
--                      that key — a fragile policy is worse than an honestly
--                      absent one. Follow-up: give `jobs` a real `graph_id`
--                      column (generated/backfilled) if this table ever needs
--                      covering for real.
--                    - `connector_identities`: PK is `(provider, workspace_id,
--                      external_user_id)` — no `account_id`/`graph_id`
--                      column, and no FK to either. It CAN be loosely joined
--                      to `oauth_tokens` on `(provider, provider_workspace_id)`,
--                      but that's a heuristic text match, not an FK, and two
--                      accounts can legitimately install the OAuth app into
--                      the very same external workspace — the row is
--                      genuinely shared reference data (id → display name),
--                      not per-account content. Left uncovered deliberately.
--                    - `sync_cursors`: PK is `peer TEXT` alone (e.g. `'cloud'`)
--                      — one row per sync peer, not one row per account; the
--                      table's shape has no account dimension to anchor a
--                      policy on at all. Holds a cursor integer, not content.
--
--              (a)+(b)+(c)+(d)+(e) = 15+13+7+1+3 = 39, the full roster.
--
--              **This migration is INERT.** The application today connects
--              as its existing owner/BYPASSRLS role, and `BYPASSRLS` skips
--              RLS evaluation entirely regardless of policies present — so
--              enabling RLS and adding these policies changes ZERO query
--              results for the app as deployed today. `coralswarm_app` is
--              created but nothing connects as it yet. The switch to actually
--              run the app as `coralswarm_app` is a SEPARATE, NAMED ops task,
--              **OPS-4** — a PRIVILEGE REDUCTION (owner/BYPASSRLS →
--              NOBYPASSRLS `coralswarm_app`), tracked in the plan's
--              Phase-3/Parallelization sections and its feature-flag table:
--                1. Generate a strong password out-of-band; `ALTER ROLE
--                   coralswarm_app WITH PASSWORD '<generated>'` directly
--                   against prod (never through a migration/shell history).
--                2. Store it as a new Fly secret (`db_secret_ref`-style, not
--                   the existing owner DSN secret) and point the app's
--                   `DATABASE_URL` at it; rolling restart.
--                3. Verify BEFORE relying on it: `SELECT rolbypassrls FROM
--                   pg_roles WHERE rolname = current_user;` from a live app
--                   instance must return `false` (confirms the app is no
--                   longer running with BYPASSRLS — a role swap that silently
--                   kept BYPASSRLS on some connection path would make every
--                   policy here a no-op even post-switch).
--                4. Run `verify_tenant_isolation.sh`'s consumer-RLS probe (PR
--                   4.1) AS the app's live connection — a green probe only
--                   means enforcement is real once it runs under
--                   `coralswarm_app`, not the owner role.
--              Until OPS-4 runs, this migration has shipped a backstop with
--              the safety catch still on, on purpose (per the plan: land
--              inert, switch separately, never both in one PR).
--
--              **Follow-up, NOT this PR:** `tenancy::scope::begin_account_scoped`
--              (the `SET LOCAL app.account_id` plumbing) is additive — no
--              route/worker calls it yet. Converting the actual read/write
--              call sites to call it is separate, incremental work in PR 4.1/
--              OPS-4 territory. Because the predicate is STRICT, any scoped
--              table read that runs OUTSIDE a `begin_account_scoped`
--              transaction, post-OPS-4, hard-errors rather than silently
--              misbehaving — but that discipline needs to be caught by
--              tests, not discovered in prod: any new/converted route must
--              carry a test that asserts it 500s cleanly (not partial-data
--              200s) if its scope call is ever accidentally dropped.
--
--              **Informational limitation:** this is a backstop against
--              forgotten-WHERE-clause application bugs, NOT a defense
--              against SQL injection under the app role — a successful SQL
--              injection can still set/forge `app.account_id` itself (e.g.
--              `set_config('app.account_id', '<victim>', true)` if an
--              attacker can execute arbitrary SQL) or otherwise operate
--              within whatever the connection's role is permitted to do.
--              RLS narrows the blast radius of logic bugs; it does not
--              replace input validation/parameterization.
--
-- Depends on: 0001_init (accounts, users), 0003_atoms (graphs, atoms),
--             0004_jobs (jobs — flagged, not covered), 0005_queries (queries),
--             0011_nodes (nodes, node_atoms, supersession_edge,
--             non_supersession_pair), 0013_activity_feed (activity_events),
--             0015_slack_integrations, 0019_query_history,
--             0021_oauth_tokens, 0022_teams (teams, team_members, team_keys,
--             team_member_key_envelopes), 0023_sync_oplog (op_log,
--             sync_cursors — flagged, not covered), 0026_ocean_model
--             (node_promotions), 0035_relationship_edges (relationship_edge),
--             0036_outlook_workflows, 0041_conversations (conversations,
--             conversation_messages), 0048_node_merges, 0049_connector_
--             identity_self_reef (connector_identities — flagged, not
--             covered), 0054_conversation_scope (conversation_oceans),
--             0055_conversation_surfaced_edges, 0056_meeting_transcripts
--             (meeting_transcripts, transcript_corrections),
--             0068_meeting_shares, 0076_coding_sessions (projects,
--             coding_sessions), 0077_coding_session_shares, plus
--             graph_subscriptions/outlook_watched_conversations/
--             slack_huddle_captures/teams_watched_resources (each
--             REFERENCES accounts(id) directly)
--
-- Down:
--   -- 39-table roster minus the 3 flagged/uncovered (jobs,
--   -- connector_identities, sync_cursors) = 36 policies to drop:
--   DROP POLICY IF EXISTS account_isolation ON activity_events;
--   DROP POLICY IF EXISTS account_isolation ON atoms;
--   DROP POLICY IF EXISTS account_isolation ON conversation_messages;
--   DROP POLICY IF EXISTS account_isolation ON conversation_oceans;
--   DROP POLICY IF EXISTS account_isolation ON conversation_surfaced_edges;
--   DROP POLICY IF EXISTS account_isolation ON conversations;
--   DROP POLICY IF EXISTS account_isolation ON coding_session_shares;
--   DROP POLICY IF EXISTS account_isolation ON coding_sessions;
--   DROP POLICY IF EXISTS account_isolation ON graph_subscriptions;
--   DROP POLICY IF EXISTS account_isolation ON graphs;
--   DROP POLICY IF EXISTS account_isolation ON llm_usage;
--   DROP POLICY IF EXISTS account_isolation ON mcp_tool_calls;
--   DROP POLICY IF EXISTS account_isolation ON meeting_shares;
--   DROP POLICY IF EXISTS account_isolation ON meeting_transcripts;
--   DROP POLICY IF EXISTS account_isolation ON non_supersession_pair;
--   DROP POLICY IF EXISTS account_isolation ON node_atoms;
--   DROP POLICY IF EXISTS account_isolation ON node_merges;
--   DROP POLICY IF EXISTS account_isolation_select ON node_promotions;
--   DROP POLICY IF EXISTS account_isolation_insert ON node_promotions;
--   DROP POLICY IF EXISTS account_isolation_update ON node_promotions;
--   DROP POLICY IF EXISTS account_isolation_delete ON node_promotions;
--   DROP POLICY IF EXISTS account_isolation ON nodes;
--   DROP POLICY IF EXISTS account_isolation ON oauth_tokens;
--   DROP POLICY IF EXISTS account_isolation ON op_log;
--   DROP POLICY IF EXISTS account_isolation ON outlook_watched_conversations;
--   DROP POLICY IF EXISTS account_isolation ON outlook_workflows;
--   DROP POLICY IF EXISTS account_isolation ON projects;
--   DROP POLICY IF EXISTS account_isolation ON queries;
--   DROP POLICY IF EXISTS account_isolation ON query_history;
--   DROP POLICY IF EXISTS account_isolation ON relationship_edge;
--   DROP POLICY IF EXISTS account_isolation ON slack_huddle_captures;
--   DROP POLICY IF EXISTS account_isolation ON slack_integrations;
--   DROP POLICY IF EXISTS account_isolation ON supersession_edge;
--   DROP POLICY IF EXISTS account_isolation ON team_keys;
--   DROP POLICY IF EXISTS account_isolation ON team_member_key_envelopes;
--   DROP POLICY IF EXISTS account_isolation ON team_members;
--   DROP POLICY IF EXISTS account_isolation ON teams;
--   DROP POLICY IF EXISTS account_isolation ON teams_watched_resources;
--   DROP POLICY IF EXISTS account_isolation ON transcript_corrections;
--   -- (ALTER TABLE ... DISABLE ROW LEVEL SECURITY on each of the above, if
--   -- reverting fully.)
--   GRANT INSERT, UPDATE, DELETE ON tenant_databases TO coralswarm_app;
--   GRANT ALL ON escrow_shares TO coralswarm_app;
--   GRANT ALL ON _sqlx_migrations TO coralswarm_app;
--   ALTER DEFAULT PRIVILEGES IN SCHEMA public
--     REVOKE SELECT, INSERT, UPDATE, DELETE ON TABLES FROM coralswarm_app;
--   ALTER DEFAULT PRIVILEGES IN SCHEMA public
--     REVOKE USAGE, SELECT ON SEQUENCES FROM coralswarm_app;
--   REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA public FROM coralswarm_app;
--   -- REVOKE the default-privileges grants BEFORE DROP ROLE — otherwise DROP
--   -- ROLE fails ("cannot drop role ... because other objects depend on it":
--   -- an ALTER DEFAULT PRIVILEGES entry is itself a dependent object, not
--   -- just the table-level grants REVOKE ALL already covers).
--   DROP ROLE IF EXISTS coralswarm_app;

-- Fail fast, not slow: each `ENABLE ROW LEVEL SECURITY` below takes a brief
-- ACCESS EXCLUSIVE lock on its table. Sequentially, over hot tables (graphs,
-- queries, activity_events, llm_usage, mcp_tool_calls, atoms, nodes), each
-- would otherwise queue behind any in-flight long reader and block that
-- table for however long the reader takes. A short `lock_timeout` makes a
-- contended migration abort loudly (and retriably) instead of stalling prod.
SET LOCAL lock_timeout = '3s';

-- ── 1. The application role ──────────────────────────────────────────────────
-- `coralswarm_app` is a role, i.e. a row in the CLUSTER-WIDE `pg_authid`
-- catalog (plus dependency bookkeeping in the cluster-wide `pg_shdepend`
-- catalog for every GRANT below) — shared across EVERY database on the
-- instance, not scoped to this one. This migration is applied to many
-- independent scratch databases in test/CI (each `_pg.rs` integration test
-- creates its own), often CONCURRENTLY, so many sessions can race to
-- create/alter/grant against this exact shared role at the same instant.
-- Postgres does not always block-and-wait for concurrent shared-catalog DDL
-- the way it does for ordinary table rows — it can instead raise `tuple
-- concurrently updated` (reproduced in practice under `cargo test`'s
-- parallelism while developing this migration). This whole section is
-- therefore wrapped in a retry loop: an advisory lock makes the common case
-- collision-free, and the retry (catching specifically `XX000`, the SQLSTATE
-- observed) is the actual guarantee — safe because every statement inside is
-- idempotent (conditional `CREATE ROLE`; re-applying the same `ALTER ROLE`
-- attributes, `GRANT`, or `REVOKE` to an already-correct target is a no-op),
-- so retrying the whole block from scratch on any transient conflict is
-- always safe. (In real prod deploys — one migration run, one connection —
-- this loop iterates exactly once and the retry path never fires; it exists
-- for the many-concurrent-test-databases scenario.)
DO $$
DECLARE
    attempt int := 0;
BEGIN
    LOOP
        BEGIN
            -- Transaction-scoped: auto-released at this migration's own
            -- COMMIT (or at each failed attempt's implicit rollback-to-
            -- savepoint below), so it never outlives the migration.
            PERFORM pg_advisory_xact_lock(hashtext('coralswarm_app_role_ddl'));

            -- NOLOGIN would be safer-by-default, but the whole point of this
            -- role is that it eventually IS the app's login role (OPS-4) —
            -- so LOGIN is granted here, with no password, and the role is
            -- unusable for authentication until OPS-4 sets one. NOBYPASSRLS
            -- is the load-bearing attribute: it is what makes the policies
            -- below actually apply to this role (the default for
            -- `CREATE ROLE` is already NOBYPASSRLS; spelled out here so the
            -- intent can never silently flip if this statement is ever
            -- copy-edited).
            IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = 'coralswarm_app') THEN
                CREATE ROLE coralswarm_app
                    WITH LOGIN
                         NOSUPERUSER
                         NOCREATEDB
                         NOCREATEROLE
                         NOREPLICATION
                         NOBYPASSRLS
                         CONNECTION LIMIT -1;
            END IF;

            -- CRITICAL (CodeRabbit review of PR #277): the CREATE above is
            -- guarded by `IF NOT EXISTS`, so if `coralswarm_app` already
            -- existed for ANY reason (a partial prior run of this migration,
            -- a role hand-created in some environment, a role that was ever
            -- created/altered BYPASSRLS by mistake), the CREATE is skipped
            -- and its EXISTING attributes stand untouched — a pre-existing
            -- `coralswarm_app` with BYPASSRLS would silently keep bypassing
            -- every policy below, forever, even after OPS-4 switches the app
            -- onto it. That would nullify this entire migration's purpose
            -- without any error or signal. So the security-critical
            -- attributes are asserted UNCONDITIONALLY, every time this
            -- migration runs.
            ALTER ROLE coralswarm_app
                WITH NOBYPASSRLS NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION;

            -- Table/sequence privileges the role needs once it becomes the
            -- app's connection role (OPS-4). RLS restricts WHICH rows a
            -- policy-protected table shows this role; it is not a
            -- substitute for the base GRANT — without these,
            -- `coralswarm_app` could not read/write anything at all, RLS
            -- notwithstanding. Deliberately schema-wide (not just the RLS'd
            -- tables): the consumer plane is also the control plane, and
            -- this role must reach `users`/`accounts`/`sessions`/etc. once
            -- it is the app's role — those tables are simply not
            -- RLS-restricted (out of this backstop's scope; see the
            -- migration header).
            GRANT USAGE ON SCHEMA public TO coralswarm_app;
            GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO coralswarm_app;
            GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO coralswarm_app;
            ALTER DEFAULT PRIVILEGES IN SCHEMA public
                GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO coralswarm_app;
            ALTER DEFAULT PRIVILEGES IN SCHEMA public
                GRANT USAGE, SELECT ON SEQUENCES TO coralswarm_app;

            -- ── Narrow the broad grant off a handful of sensitive tables ──
            -- Least privilege: the broad GRANT above is safe today only
            -- because `coralswarm_app` is NOBYPASSRLS and not a table owner
            -- (it cannot self-neuter RLS) — but "safe because of one other
            -- control" is the opposite of least-privilege, and these three
            -- are cheap and unambiguous to narrow now.
            --
            -- `tenant_databases` is the org → tenant-DB ROUTING table
            -- (`db_secret_ref` etc.) — an app-layer compromise with write
            -- access here could repoint an org's database. The live
            -- app-request path only ever READS it (routing lookups in
            -- `tenancy::routing`/the binding gate); the only writer is
            -- `tenancy::routing::record_schema_version` (`UPDATE
            -- tenant_databases SET schema_version...`), called exclusively
            -- from the `migrate_tenants` orchestrator binary (an ops tool,
            -- run under its own elevated connection, never the live app
            -- process) — confirmed by grep: no `routes/` file writes this
            -- table. So SELECT stays; INSERT/UPDATE/DELETE do not.
            REVOKE INSERT, UPDATE, DELETE ON tenant_databases FROM coralswarm_app;

            -- `escrow_shares` (each org admin's Shamir share of the org
            -- recovery key) has NO current read or write call site anywhere
            -- in `backend/src` — confirmed by grep across `routes/`. Unlike
            -- `escrow_wrapped_keys`/`escrow_recovery_pubkey`/
            -- `escrow_recovery_log`/`user_private_keys` (all four ARE
            -- actively read/written by the allowlisted `routes/escrow.rs`
            -- and `routes/private_key.rs` — revoking those would break
            -- real, currently-shipping recovery/backup functionality the
            -- moment OPS-4 switches the app's role, which is a regression,
            -- not hardening, so they are deliberately left on the broad
            -- grant), `escrow_shares` is genuinely unused. Revoke it
            -- entirely now; a future PR that starts using it grants it back
            -- explicitly at that point.
            REVOKE ALL ON escrow_shares FROM coralswarm_app;

            -- `_sqlx_migrations` is sqlx's own bookkeeping ledger — the live
            -- app process never reads or writes it (only the
            -- `sqlx migrate run`/`migrate_tenants` tooling does, again under
            -- its own connection). No reason for the app's day-to-day role
            -- to touch it at all.
            REVOKE ALL ON _sqlx_migrations FROM coralswarm_app;

            EXIT; -- success
        EXCEPTION WHEN SQLSTATE 'XX000' THEN
            -- `XX000` (internal_error) is the specific SQLSTATE observed for
            -- the concurrent-shared-catalog-DDL race described above — catch
            -- ONLY that (not a broad `WHEN OTHERS`, which would also mask a
            -- genuine bug, e.g. a typo'd table name, behind ten silent
            -- retries before finally surfacing it).
            attempt := attempt + 1;
            IF attempt >= 10 THEN
                RAISE;
            END IF;
            PERFORM pg_sleep(0.05 * attempt);
        END;
    END LOOP;
END
$$;

-- ── 2. RLS + policy ──────────────────────────────────────────────────────────
-- `CREATE POLICY` has no `IF NOT EXISTS`; `DROP POLICY IF EXISTS` first is the
-- standard idempotent idiom (matches this repo's `CREATE OR REPLACE FUNCTION`
-- idempotency convention elsewhere, e.g. 0082). A bare policy (no `FOR`
-- clause, no `WITH CHECK`) applies to ALL commands and reuses `USING` as the
-- `WITH CHECK` expression too, so SELECT/INSERT/UPDATE/DELETE are all scoped
-- by the same predicate.

-- ── 2a. Direct `account_id` column (15) ─────────────────────────────────────

ALTER TABLE activity_events ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS account_isolation ON activity_events;
CREATE POLICY account_isolation ON activity_events
    USING (account_id = current_setting('app.account_id')::uuid);

ALTER TABLE conversations ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS account_isolation ON conversations;
CREATE POLICY account_isolation ON conversations
    USING (account_id = current_setting('app.account_id')::uuid);

ALTER TABLE graph_subscriptions ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS account_isolation ON graph_subscriptions;
CREATE POLICY account_isolation ON graph_subscriptions
    USING (account_id = current_setting('app.account_id')::uuid);

ALTER TABLE graphs ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS account_isolation ON graphs;
CREATE POLICY account_isolation ON graphs
    USING (account_id = current_setting('app.account_id')::uuid);

-- `llm_usage.account_id` is NULLABLE (system/unattributed rows) — see the
-- migration header: those rows are invisible under this policy, a
-- deliberate fail-closed consequence, not a bug.
ALTER TABLE llm_usage ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS account_isolation ON llm_usage;
CREATE POLICY account_isolation ON llm_usage
    USING (account_id = current_setting('app.account_id')::uuid);

-- `mcp_tool_calls.account_id` is likewise NULLABLE — same consequence.
ALTER TABLE mcp_tool_calls ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS account_isolation ON mcp_tool_calls;
CREATE POLICY account_isolation ON mcp_tool_calls
    USING (account_id = current_setting('app.account_id')::uuid);

ALTER TABLE oauth_tokens ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS account_isolation ON oauth_tokens;
CREATE POLICY account_isolation ON oauth_tokens
    USING (account_id = current_setting('app.account_id')::uuid);

ALTER TABLE outlook_watched_conversations ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS account_isolation ON outlook_watched_conversations;
CREATE POLICY account_isolation ON outlook_watched_conversations
    USING (account_id = current_setting('app.account_id')::uuid);

ALTER TABLE outlook_workflows ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS account_isolation ON outlook_workflows;
CREATE POLICY account_isolation ON outlook_workflows
    USING (account_id = current_setting('app.account_id')::uuid);

ALTER TABLE queries ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS account_isolation ON queries;
CREATE POLICY account_isolation ON queries
    USING (account_id = current_setting('app.account_id')::uuid);

ALTER TABLE query_history ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS account_isolation ON query_history;
CREATE POLICY account_isolation ON query_history
    USING (account_id = current_setting('app.account_id')::uuid);

ALTER TABLE slack_huddle_captures ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS account_isolation ON slack_huddle_captures;
CREATE POLICY account_isolation ON slack_huddle_captures
    USING (account_id = current_setting('app.account_id')::uuid);

ALTER TABLE slack_integrations ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS account_isolation ON slack_integrations;
CREATE POLICY account_isolation ON slack_integrations
    USING (account_id = current_setting('app.account_id')::uuid);

ALTER TABLE teams ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS account_isolation ON teams;
CREATE POLICY account_isolation ON teams
    USING (account_id = current_setting('app.account_id')::uuid);

ALTER TABLE teams_watched_resources ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS account_isolation ON teams_watched_resources;
CREATE POLICY account_isolation ON teams_watched_resources
    USING (account_id = current_setting('app.account_id')::uuid);

-- ── 2b. Direct `graph_id` column, one hop to `graphs` (13) ──────────────────
-- This is the P1 fix: `atoms`/`nodes`/etc. are read directly by `graph_id`/
-- `atom_id` all over the codebase WITHOUT joining `graphs` (the classic IDOR
-- read pivot) — RLS is evaluated per table, so `graphs`' own policy was
-- never consulted for those reads. Each of these now carries its own
-- policy, anchored back to `graphs.account_id` via a subquery.

ALTER TABLE atoms ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS account_isolation ON atoms;
CREATE POLICY account_isolation ON atoms
    USING (graph_id IN (
        SELECT id FROM graphs WHERE account_id = current_setting('app.account_id')::uuid
    ));

ALTER TABLE nodes ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS account_isolation ON nodes;
CREATE POLICY account_isolation ON nodes
    USING (graph_id IN (
        SELECT id FROM graphs WHERE account_id = current_setting('app.account_id')::uuid
    ));

ALTER TABLE relationship_edge ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS account_isolation ON relationship_edge;
CREATE POLICY account_isolation ON relationship_edge
    USING (graph_id IN (
        SELECT id FROM graphs WHERE account_id = current_setting('app.account_id')::uuid
    ));

ALTER TABLE node_merges ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS account_isolation ON node_merges;
CREATE POLICY account_isolation ON node_merges
    USING (graph_id IN (
        SELECT id FROM graphs WHERE account_id = current_setting('app.account_id')::uuid
    ));

-- `meeting_transcripts.graph_id` carries no FK to `graphs` (noted "soft" in
-- 0056's own header) — the column still exists and the subquery works
-- identically whether or not it's FK-enforced.
ALTER TABLE meeting_transcripts ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS account_isolation ON meeting_transcripts;
CREATE POLICY account_isolation ON meeting_transcripts
    USING (graph_id IN (
        SELECT id FROM graphs WHERE account_id = current_setting('app.account_id')::uuid
    ));

-- Same "soft" `graph_id` (no FK) as `meeting_transcripts` above.
ALTER TABLE transcript_corrections ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS account_isolation ON transcript_corrections;
CREATE POLICY account_isolation ON transcript_corrections
    USING (graph_id IN (
        SELECT id FROM graphs WHERE account_id = current_setting('app.account_id')::uuid
    ));

ALTER TABLE meeting_shares ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS account_isolation ON meeting_shares;
CREATE POLICY account_isolation ON meeting_shares
    USING (graph_id IN (
        SELECT id FROM graphs WHERE account_id = current_setting('app.account_id')::uuid
    ));

ALTER TABLE projects ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS account_isolation ON projects;
CREATE POLICY account_isolation ON projects
    USING (graph_id IN (
        SELECT id FROM graphs WHERE account_id = current_setting('app.account_id')::uuid
    ));

ALTER TABLE coding_sessions ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS account_isolation ON coding_sessions;
CREATE POLICY account_isolation ON coding_sessions
    USING (graph_id IN (
        SELECT id FROM graphs WHERE account_id = current_setting('app.account_id')::uuid
    ));

ALTER TABLE coding_session_shares ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS account_isolation ON coding_session_shares;
CREATE POLICY account_isolation ON coding_session_shares
    USING (graph_id IN (
        SELECT id FROM graphs WHERE account_id = current_setting('app.account_id')::uuid
    ));

ALTER TABLE team_keys ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS account_isolation ON team_keys;
CREATE POLICY account_isolation ON team_keys
    USING (graph_id IN (
        SELECT id FROM graphs WHERE account_id = current_setting('app.account_id')::uuid
    ));

ALTER TABLE conversation_oceans ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS account_isolation ON conversation_oceans;
CREATE POLICY account_isolation ON conversation_oceans
    USING (graph_id IN (
        SELECT id FROM graphs WHERE account_id = current_setting('app.account_id')::uuid
    ));

-- `op_log.graph_id` is NULLABLE (not every synced entity carries one) — same
-- accepted NULL-hides-the-row consequence as (a)'s two nullable tables.
ALTER TABLE op_log ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS account_isolation ON op_log;
CREATE POLICY account_isolation ON op_log
    USING (graph_id IN (
        SELECT id FROM graphs WHERE account_id = current_setting('app.account_id')::uuid
    ));

-- ── 2c. One hop to a table in 2a/2b, then to `graphs` (7) ───────────────────

ALTER TABLE node_atoms ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS account_isolation ON node_atoms;
CREATE POLICY account_isolation ON node_atoms
    USING (node_id IN (
        SELECT node_id FROM nodes
        WHERE graph_id IN (
            SELECT id FROM graphs WHERE account_id = current_setting('app.account_id')::uuid
        )
    ));

-- An edge's two endpoints (`superseded_id`/`superseder_id`) are always
-- same-graph by construction, so anchoring on either one identifies the
-- same graph/account.
ALTER TABLE supersession_edge ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS account_isolation ON supersession_edge;
CREATE POLICY account_isolation ON supersession_edge
    USING (superseded_id IN (
        SELECT node_id FROM nodes
        WHERE graph_id IN (
            SELECT id FROM graphs WHERE account_id = current_setting('app.account_id')::uuid
        )
    ));

-- Same same-graph-endpoints reasoning as `supersession_edge` above.
ALTER TABLE non_supersession_pair ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS account_isolation ON non_supersession_pair;
CREATE POLICY account_isolation ON non_supersession_pair
    USING (node_a_id IN (
        SELECT node_id FROM nodes
        WHERE graph_id IN (
            SELECT id FROM graphs WHERE account_id = current_setting('app.account_id')::uuid
        )
    ));

-- `conversations` already carries `account_id` directly (2a) — no need to
-- go all the way to `graphs`.
ALTER TABLE conversation_messages ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS account_isolation ON conversation_messages;
CREATE POLICY account_isolation ON conversation_messages
    USING (conversation_id IN (
        SELECT id FROM conversations WHERE account_id = current_setting('app.account_id')::uuid
    ));

ALTER TABLE conversation_surfaced_edges ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS account_isolation ON conversation_surfaced_edges;
CREATE POLICY account_isolation ON conversation_surfaced_edges
    USING (conversation_id IN (
        SELECT id FROM conversations WHERE account_id = current_setting('app.account_id')::uuid
    ));

-- `teams` already carries `account_id` directly (2a).
ALTER TABLE team_members ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS account_isolation ON team_members;
CREATE POLICY account_isolation ON team_members
    USING (team_id IN (
        SELECT id FROM teams WHERE account_id = current_setting('app.account_id')::uuid
    ));

ALTER TABLE team_member_key_envelopes ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS account_isolation ON team_member_key_envelopes;
CREATE POLICY account_isolation ON team_member_key_envelopes
    USING (team_key_id IN (
        SELECT id FROM team_keys
        WHERE graph_id IN (
            SELECT id FROM graphs WHERE account_id = current_setting('app.account_id')::uuid
        )
    ));

-- ── 2d. Two anchors — READ permissively (either), WRITE strictly (both) ────
-- MAJOR (CodeRabbit review of PR #277): a bare `USING`-only policy (no `FOR`,
-- no `WITH CHECK`) is a `FOR ALL` policy, and Postgres reuses `USING` as
-- `WITH CHECK` when none is given — empirically verified against this exact
-- schema (a matching INSERT succeeds, a non-matching one raises "new row
-- violates row-level security policy"), so this is NOT the "INSERT is
-- default-denied" failure mode for 35 of the 36 policies in this migration:
-- every other table's predicate anchors to exactly ONE account (a direct
-- `account_id`, or a single unambiguous `graph_id`/`node_id`/
-- `conversation_id`/`team_id` chain), so reusing it for both read and write
-- is correct — the same expression can never be satisfied by two different
-- accounts at once.
--
-- `node_promotions` is different, and genuinely broken: its predicate is an
-- OR across TWO INDEPENDENT anchors (source reef's graph, target graph) —
-- appropriate for READ (a promotion spanning two oceans should be visible
-- from either side), but reused as `WITH CHECK` it let account A INSERT a
-- row whose `target_graph_id` belongs to account B, merely because A owned
-- `source_node_id`'s graph — a real cross-account write, verified
-- reproducible before this fix. On the consumer/personal plane, every
-- legitimate promotion (`Store::promote_node`) is same-account by
-- construction (the plan's PR 2.5: cross-account promotion is a NARROW
-- cross-DB service that writes the TARGET tenant DB's own `node_promotions`
-- with fresh local ids — it never touches this consumer-plane row at all),
-- so requiring BOTH anchors for any write is not just safer but correct: no
-- legitimate write is ever rejected by it.
ALTER TABLE node_promotions ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS account_isolation ON node_promotions;

-- Read: permissive OR — visible from either side.
CREATE POLICY account_isolation_select ON node_promotions
    FOR SELECT
    USING (
        source_node_id IN (
            SELECT node_id FROM nodes
            WHERE graph_id IN (
                SELECT id FROM graphs WHERE account_id = current_setting('app.account_id')::uuid
            )
        )
        OR target_graph_id IN (
            SELECT id FROM graphs WHERE account_id = current_setting('app.account_id')::uuid
        )
    );

-- Write (INSERT): strict AND — the acting account must own BOTH the source
-- reef's graph and the target graph, matching the same-account invariant
-- every real `promote_node` call satisfies.
CREATE POLICY account_isolation_insert ON node_promotions
    FOR INSERT
    WITH CHECK (
        source_node_id IN (
            SELECT node_id FROM nodes
            WHERE graph_id IN (
                SELECT id FROM graphs WHERE account_id = current_setting('app.account_id')::uuid
            )
        )
        AND target_graph_id IN (
            SELECT id FROM graphs WHERE account_id = current_setting('app.account_id')::uuid
        )
    );

-- Write (UPDATE — `promote_node`'s re-publish upsert, and the node-merge
-- repoint path): `USING` permissive (OR) so an existing legitimate row is
-- still reachable to update from either recorded anchor; `WITH CHECK`
-- strict (AND) so the row can never be left pointing at another account's
-- graph after the update.
CREATE POLICY account_isolation_update ON node_promotions
    FOR UPDATE
    USING (
        source_node_id IN (
            SELECT node_id FROM nodes
            WHERE graph_id IN (
                SELECT id FROM graphs WHERE account_id = current_setting('app.account_id')::uuid
            )
        )
        OR target_graph_id IN (
            SELECT id FROM graphs WHERE account_id = current_setting('app.account_id')::uuid
        )
    )
    WITH CHECK (
        source_node_id IN (
            SELECT node_id FROM nodes
            WHERE graph_id IN (
                SELECT id FROM graphs WHERE account_id = current_setting('app.account_id')::uuid
            )
        )
        AND target_graph_id IN (
            SELECT id FROM graphs WHERE account_id = current_setting('app.account_id')::uuid
        )
    );

-- Write (DELETE — the node-merge collision-repoint path): only `USING`
-- applies (DELETE has no new row to WITH CHECK). Strict AND here too: a
-- merge/unmerge only ever deletes a row entirely within one account's graph,
-- so this never rejects a legitimate delete, and it closes the same
-- cross-account write surface for removal.
CREATE POLICY account_isolation_delete ON node_promotions
    FOR DELETE
    USING (
        source_node_id IN (
            SELECT node_id FROM nodes
            WHERE graph_id IN (
                SELECT id FROM graphs WHERE account_id = current_setting('app.account_id')::uuid
            )
        )
        AND target_graph_id IN (
            SELECT id FROM graphs WHERE account_id = current_setting('app.account_id')::uuid
        )
    );

-- ── 2e. Deliberately NOT covered (see migration header for why) ────────────
-- jobs, connector_identities, sync_cursors — no policy added.
