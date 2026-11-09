-- Migration: 0064_team_seat_grants_history_from
-- Description: the TENANT half of control's
--   `0184_team_seat_grants_history_from` (m365 enterprise train PR 4.1,
--   "history_from") — per-seat-grant retrieval window, enforced by RLS on
--   the live retrieval path rather than by an app-level `WHERE` clause.
--   Contract conformance tests #5/#6 (`docs/connectors/CONNECTOR_CONTRACT.md`
--   §6, invariants I6/I9/I10) ship against this migration.
--
--   ── The column ──
--   `team_seat_grants.history_from` — byte-equivalent shape to control's
--   twin (both `TIMESTAMPTZ NULL`; NULL = full history), per the
--   `0106`/`0017` convention this table pair already follows. See control's
--   migration header for why the column lives on the GRANT, not the seat: a
--   `(team_id, user_id)` pair's effective window is the WIDEST any surviving
--   grant justifies (`docs/plans/org-connectors-and-channel-ingest.md` §5),
--   which is a property of the grant SET, not a single row.
--
--   ── The enforcement ──
--   A new SECURITY DEFINER function, `tenant_rls_visible_graph_windows()`,
--   pairs every graph id `tenant_rls_visible_graph_ids()` (0009) would yield
--   with a `history_floor`: NULL (no window) for a system actor, a
--   non-team graph, or a team graph where the acting member holds no grant
--   at all (legacy/backward-compatible — a member seated before this PR
--   existed carries no grant rows and must not be retroactively windowed)
--   OR holds any grant with `history_from IS NULL`; otherwise the MIN of
--   their surviving grants' `history_from`. The visibility half of the
--   predicate is copied VERBATIM from `tenant_rls_visible_graph_ids()`
--   (migration 0009) rather than joined against it, so this function can
--   never accidentally WIDEN what that function already decided is visible
--   — see 0009's own header on why `SECURITY DEFINER` here must never widen,
--   and `migrations_tenant/0040_team_create_bootstrap_rls.sql`'s guard
--   against exactly that class of regression. `tenant_rls_visible_graph_ids()`
--   itself is UNTOUCHED — it has ~55 policy consumers (per this PR's brief);
--   the new function is additive and used only by the policy below.
--
--   `MIN()` ignoring NULLs is exactly wrong for the "any NULL grant absorbs"
--   rule (a standard SQL aggregate skips NULL rows entirely, so a plain
--   `MIN(history_from)` over a grant set containing one NULL and one
--   timestamped row would silently return the timestamp, not NULL) — the
--   function therefore checks for a NULL-carrying grant FIRST via `EXISTS`
--   and only falls through to `MIN()` once none exists. An actor with ZERO
--   grant rows for the team also correctly yields NULL: `MIN()` over an
--   empty set is NULL by definition, no special case needed.
--
--   ── Why `atoms` splits into four policies ──
--   0009's `atoms` policy (line ~1184) is a single bare `FOR ALL` — reusing
--   `USING` as `WITH CHECK` (§3's documented convention for every
--   single-anchor table). A window belongs on READS ONLY: ingest/edit of an
--   atom timestamped before some OTHER member's window must not be refused
--   just because a windowed member happens to share the transaction's scope
--   (the write path runs `begin_tenant_scoped`/`begin_tenant_system_scoped`
--   for the ACTOR performing the write, not for every member who can read
--   the ocean). So: `FOR SELECT` gets the new windowed predicate;
--   `FOR INSERT`/`FOR UPDATE`/`FOR DELETE` keep the CURRENT unwindowed
--   `tenant_rls_visible_graph_ids()` predicate verbatim.
--
--   **What the split actually buys, measured (CodeRabbit round 1, verified
--   empirically as `coralswarm_tenant_app`):** Postgres applies SELECT
--   policies to any DML that READS existing rows — a `WHERE` referencing
--   columns, or a `RETURNING` clause — in addition to the command's own
--   policy. The de-facto member-scoped write matrix under this migration is
--   therefore:
--     * system scope (`tenant_rls_is_system()`): every operation, any
--       timestamp — the floor is NULL, nothing changes. ALL real ingest/
--       backfill runs here.
--     * member INSERT of a pre-window-timestamped atom, no RETURNING:
--       succeeds (WITH CHECK is unwindowed).
--     * member INSERT ... RETURNING of a pre-window atom: REFUSED by
--       Postgres ("new row violates row-level security policy") — the
--       returned row must pass the SELECT policy. `routes/ingest.rs` maps
--       this to a 403 rather than a 500.
--     * member UPDATE/DELETE of pre-window rows: silently affect 0 rows —
--       the WHERE read is filtered by the SELECT policy, so a windowed
--       member cannot target rows they cannot see. Consistent with reads.
--   The write policies stay unwindowed anyway: they are what keeps system-
--   scope ingest and same-window member writes working, and a windowed
--   SELECT policy composing with unwindowed write policies is exactly the
--   fail-closed direction (a windowed member can never touch pre-window
--   content they cannot see). Whether a windowed member's own backdated
--   CONTRIBUTION (e.g. sharing an old email into the team ocean) should be
--   allowed via an `ingested_by = actor` visibility exemption is an open
--   product question recorded in the PR — deliberately NOT decided here.
--   Same four-way command split `node_promotions` already established in
--   0009 §3e (see that section's header) — `scripts/check_schema_parity.sh`'s
--   policy-strip loop already tries all four `_select`/`_insert`/`_update`/
--   `_delete` names unconditionally for every table in `TENANT_ONLY_RLS_TABLES`
--   (its own comment: "a table that migrates from bare to split needs no
--   edit here"), so no change to that script's table rosters is needed for
--   this — only the new function needs registering in its `TENANT_FUNCTIONS`
--   list.
--
--   Enforcement column is `atoms."timestamp"` (source-event time), NOT
--   `created_at` — the brief is explicit this must window the moment the
--   content is FROM, not the moment it was stored (ingest/backfill lag would
--   otherwise let stale content slip a window it should have been caught
--   by). An atom with a NULL `timestamp` is invisible under any non-NULL
--   floor (three-valued `>=` comparison) — fail-closed: content with no
--   provable moment cannot be proven in-window, so a windowed member does
--   not get default access to it. It remains visible under a NULL floor
--   (full history), exactly like every other atom.
--
--   ── What this migration deliberately does NOT touch ──
--   `nodes`/`relationship_edge`/`supersession_edge`'s own RLS policies are
--   untouched — `docs/plans/org-connectors-and-channel-ingest.md` §5's
--   "Source-timestamped vs derived tables" prescribes a `node_atoms`
--   `NOT EXISTS`-style treatment for those, but scoped this PR to what
--   retrieval actually reads (`Store::expand_scoped`,
--   `Store::keyword_search_node_atoms_scoped`, and the shared
--   `reef_atoms_ranked_by_similarity_pg` helper, all in
--   `backend/src/store/mod.rs`) rather than a blanket policy rewrite —
--   see those functions' own updated doc comments in this PR's Rust diff.
--   Once `atoms`' SELECT policy windows by `timestamp`, any query that JOINs
--   `node_atoms` to `atoms` (rather than reading `node_atoms` bare) inherits
--   the window FOR FREE — RLS is evaluated per table, and a pre-window atom
--   row simply will not be there to join against. The three call sites above
--   were changed from `LEFT JOIN`/no-join to an inner `JOIN atoms` for
--   exactly this reason: it is not independent window logic, it is making
--   sure those queries actually consult the one table that now carries it.
--
-- Depends on: 0009_tenant_rls (tenant_rls_visible_graph_ids, the atoms
--             policy this replaces), 0017_team_seat_grants
-- Down:
--   DROP TABLE IF EXISTS seat_grant_sources;
--   DROP POLICY IF EXISTS member_ocean_isolation_select ON atoms;
--   DROP POLICY IF EXISTS member_ocean_isolation_insert ON atoms;
--   DROP POLICY IF EXISTS member_ocean_isolation_update ON atoms;
--   DROP POLICY IF EXISTS member_ocean_isolation_delete ON atoms;
--   CREATE POLICY member_ocean_isolation ON atoms
--       USING (graph_id IN (SELECT * FROM tenant_rls_visible_graph_ids()));
--   DROP FUNCTION IF EXISTS tenant_rls_visible_graph_windows();
--   ALTER TABLE team_seat_grants DROP COLUMN IF EXISTS history_from;

ALTER TABLE team_seat_grants ADD COLUMN IF NOT EXISTS history_from TIMESTAMPTZ;

-- ── seat_grant_sources: durable founding marker ─────────────────────────────
-- TENANT half of control's twin (see 0184's header for the full rationale:
-- `revoke_seat_grant` deletes grant rows, so grant-emptiness alone would
-- mis-classify the next joiner of a fully-vacated source as a founding
-- member). Same structural differences from control as `team_seat_grants`
-- itself (0017): `account_id` is a bare uuid (accounts is control-only),
-- `teams` FK kept (tenant-local).
CREATE TABLE IF NOT EXISTS seat_grant_sources (
    account_id  UUID NOT NULL,
    team_id     UUID NOT NULL REFERENCES teams(id) ON DELETE CASCADE,
    source      TEXT NOT NULL CHECK (source <> ''),
    founded_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (team_id, source)
);

CREATE INDEX IF NOT EXISTS idx_seat_grant_sources_account
    ON seat_grant_sources (account_id);

-- Same tenant policy anchor as team_seat_grants (0017).
ALTER TABLE seat_grant_sources ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS member_ocean_isolation ON seat_grant_sources;
CREATE POLICY member_ocean_isolation ON seat_grant_sources
    USING (team_id IN (
        SELECT id FROM teams
        WHERE graph_id IN (SELECT * FROM tenant_rls_visible_graph_ids())
    ));

-- ── The windowed visibility function ────────────────────────────────────────
CREATE OR REPLACE FUNCTION tenant_rls_visible_graph_windows()
RETURNS TABLE(graph_id uuid, history_floor timestamptz)
LANGUAGE sql
SECURITY DEFINER
STABLE
SET search_path = public, pg_temp
AS $$
    SELECT
        g.id,
        CASE
            WHEN tenant_rls_is_system() THEN NULL::timestamptz
            WHEN g.kind <> 'team' THEN NULL::timestamptz
            -- Any grant with no floor absorbs — a member with even one
            -- full-history reason to be here reads the whole ocean.
            WHEN EXISTS (
                SELECT 1
                FROM teams t
                JOIN team_seat_grants tsg
                  ON tsg.team_id = t.id AND tsg.user_id = tenant_rls_actor_user_id()
                WHERE t.graph_id = g.id
                  AND tsg.history_from IS NULL
            ) THEN NULL::timestamptz
            -- No absorbing grant. `MIN()` over the actor's remaining grants
            -- for this team: NULL when there are none at all (legacy,
            -- pre-this-feature seats — backward compatible, full history),
            -- otherwise the earliest surviving `history_from`.
            ELSE (
                SELECT MIN(tsg.history_from)
                FROM teams t
                JOIN team_seat_grants tsg
                  ON tsg.team_id = t.id AND tsg.user_id = tenant_rls_actor_user_id()
                WHERE t.graph_id = g.id
            )
        END AS history_floor
    FROM graphs g
    WHERE tenant_rls_is_system()
       OR (
            (g.kind <> 'member' OR g.owner_user_id = tenant_rls_actor_user_id())
        AND (g.kind <> 'team'   OR EXISTS (
                SELECT 1
                FROM teams t
                JOIN team_members tm ON tm.team_id = t.id
                WHERE t.graph_id = g.id
                  AND tm.user_id = tenant_rls_actor_user_id()
                  AND tm.departing_at IS NULL
            ))
       )
$$;

REVOKE EXECUTE ON FUNCTION tenant_rls_visible_graph_windows() FROM PUBLIC;
GRANT EXECUTE ON FUNCTION tenant_rls_visible_graph_windows() TO coralswarm_tenant_app;

-- ── atoms: split into read (windowed) / write (unwindowed) ─────────────────
-- All four names dropped unconditionally first — same idempotency idiom
-- 0009 §3e uses for `node_promotions` (a re-apply during recovery, or a
-- scratch DB, must not fail partway through with SELECT policies live and no
-- write policies).
DROP POLICY IF EXISTS member_ocean_isolation ON atoms;
DROP POLICY IF EXISTS member_ocean_isolation_select ON atoms;
DROP POLICY IF EXISTS member_ocean_isolation_insert ON atoms;
DROP POLICY IF EXISTS member_ocean_isolation_update ON atoms;
DROP POLICY IF EXISTS member_ocean_isolation_delete ON atoms;

-- Written as an OR of two branches, not one `EXISTS` mixing both conditions,
-- for a measured planner reason (verified with `EXPLAIN`, no ANALYZE, as
-- `coralswarm_tenant_app`, against a real seeded team ocean — see this PR's
-- report for the full plan text):
--
--   * Branch 1, `atoms.graph_id IN (SELECT graph_id FROM
--     tenant_rls_visible_graph_windows() WHERE history_floor IS NULL)`, is a
--     PLAIN uncorrelated-subquery `IN` — identical shape to every other
--     policy's `graph_id IN (SELECT * FROM tenant_rls_visible_graph_ids())`.
--     Postgres plans this as a **hashed SubPlan**: the function is called
--     ONCE, its output hashed, and every row after that is a hash probe —
--     not a re-invocation. Since NULL (no window) is the default for every
--     non-team graph, every member-owned/org graph, and any team-ocean
--     member with no window (founding/auto-seated/legacy), this branch alone
--     resolves the overwhelming majority of rows in the overwhelming
--     majority of queries at hash-probe cost.
--   * Branch 2 is the genuinely windowed case: a member reading a team ocean
--     where THEIR OWN floor is non-NULL. This one DOES correlate on
--     `atoms."timestamp"` in addition to `graph_id`, which defeats the same
--     hash-a-uncorrelated-IN trick — Postgres plans it as a per-row
--     `SubPlan` (a genuine per-row Function Scan on
--     `tenant_rls_visible_graph_windows()`). This is the one part of this
--     migration NOT fully solved: see the report for why (Postgres's hashed-
--     subplan/semi-join pull-up only fires for an equality-only correlation;
--     a `>=` comparison against a per-group value has no equivalent
--     built-in hash-join shape short of materializing the function's output
--     into a real relation, which a bare RLS predicate cannot inject a CTE
--     for). The `OR` split still bounds this branch's cost to ONLY the rows
--     that reach it — a windowed member's own atoms, within ONE tenant
--     database (one org's worth of data, not a cross-tenant scan) — rather
--     than paying it for every row regardless of window status, which the
--     single-`EXISTS` formulation this replaced did.
CREATE POLICY member_ocean_isolation_select ON atoms
    FOR SELECT
    USING (
        atoms.graph_id IN (
            SELECT graph_id
            FROM tenant_rls_visible_graph_windows()
            WHERE history_floor IS NULL
        )
        OR EXISTS (
            SELECT 1
            FROM tenant_rls_visible_graph_windows() w
            WHERE w.graph_id = atoms.graph_id
              AND w.history_floor IS NOT NULL
              AND atoms."timestamp" >= w.history_floor
        )
    );

CREATE POLICY member_ocean_isolation_insert ON atoms
    FOR INSERT
    WITH CHECK (graph_id IN (SELECT * FROM tenant_rls_visible_graph_ids()));

CREATE POLICY member_ocean_isolation_update ON atoms
    FOR UPDATE
    USING (graph_id IN (SELECT * FROM tenant_rls_visible_graph_ids()))
    WITH CHECK (graph_id IN (SELECT * FROM tenant_rls_visible_graph_ids()));

CREATE POLICY member_ocean_isolation_delete ON atoms
    FOR DELETE
    USING (graph_id IN (SELECT * FROM tenant_rls_visible_graph_ids()));
