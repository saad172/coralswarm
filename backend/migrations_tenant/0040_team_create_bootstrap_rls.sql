-- 0040_team_create_bootstrap_rls.sql — let `create_team` write its own rows.
--
-- TENANT-ONLY, no control pair. Pure RLS, and RLS is plane-specific by design
-- throughout this repo (0009's header, 0032's "the RLS section is NOT part of
-- the byte-identical contract"). Precedent for a tenant-only migration:
-- `0009_tenant_rls`, `0022_promotion_identity`.
--
-- ── The bug ────────────────────────────────────────────────────────────────
-- `Store::create_team_tx_scoped` cannot complete under `0009_tenant_rls` when
-- run as the real non-`BYPASSRLS` `coralswarm_tenant_app` role. It is
-- PRE-EXISTING (it reproduces against 0009's original policies restored
-- verbatim) and is the item that gates `ORG_TEAMS_ENABLED`.
--
-- The root cause is single. `tenant_rls_visible_graph_ids()` gates a TEAM
-- ocean on membership:
--
--     (g.kind <> 'team' OR EXISTS (SELECT 1 FROM teams t
--                                  JOIN team_members tm ON tm.team_id = t.id
--                                  WHERE t.graph_id = g.id
--                                    AND tm.user_id = tenant_rls_actor_user_id()
--                                    AND tm.departing_at IS NULL))
--
-- and the `team_members` row that would satisfy it is the LAST insert of the
-- create sequence. Every table the sequence touches in between anchors,
-- directly or by one hop, on that same function. So creating a team requires
-- already being in it. Five of the six inserts fail:
--
--   1. graphs                    -- see "the RETURNING trap" below
--   2. teams                     -- 0032's WITH CHECK: graph_id IN visible
--   3. team_keys                 -- 0009 bare policy: graph_id IN visible
--   4. team_member_key_envelopes -- 0009 bare: team_key_id -> team_keys -> visible
--   5. team_members              -- 0009 bare: team_id -> teams -> visible
--   6. membership_events         -- OK: (5) has landed, so the graph is visible
--
-- (5) cannot self-satisfy: a `WITH CHECK` subquery runs against the statement
-- snapshot, which excludes the row the statement is inserting.
--
-- ── Why not just widen the predicate ───────────────────────────────────────
-- `tenant_rls_visible_graph_ids()` is the CONTENT anchor — `atoms`, `nodes`,
-- `conversations` and ~20 more resolve through it. Widening it to admit a
-- team's creator would hand every creator the contents of that ocean forever,
-- including after they leave. `tenant_rls_pg.rs::
-- org_admin_grant_widens_team_governance_without_leaking_team_ocean_content`
-- exists to fail if anyone tries. This migration does not touch that function.
--
-- ── Why not key the write arms on `created_by` ─────────────────────────────
-- `teams` and `team_keys` both carry `created_by`, so `created_by =
-- tenant_rls_actor_user_id()` looks like the obvious INSERT arm. Two reasons
-- it is not:
--
--   * `team_member_key_envelopes` and `team_members` have no such column, so
--     they would need a subquery hop — and a hop written as a plain subquery
--     does not work here (see "the SECURITY DEFINER trap" below), so the
--     "no new mechanism" argument for `created_by` does not survive contact.
--   * `created_by` is PROVENANCE, not authority. An arm keyed on it grants
--     the creator INSERT on that ocean's `team_keys` — the key-rotation
--     table — permanently, including long after they have left the team.
--     There is no expiry and nothing revokes it.
--
-- ── What this migration does instead ───────────────────────────────────────
-- A bootstrap grant that is narrow in THREE independent ways at once:
--
--   * narrow in WHAT: INSERT only. No SELECT, UPDATE or DELETE arm is added
--     to any table here. A bootstrapping session gains no read it did not
--     already have.
--   * narrow in WHERE: one graph id, named explicitly by the transaction in
--     `app.creating_graph_id`.
--   * narrow in WHEN: only while that graph has ZERO `team_members` rows.
--
-- The third conjunct is the important one, and it is what makes this
-- defensible rather than merely convenient. The grant is SELF-EXTINGUISHING:
-- the instant step (5) lands, `tenant_rls_bootstrap_graph_id()` returns NULL
-- for that graph and every arm below stops matching — permanently, and within
-- the very transaction that created it. It is not merely transaction-scoped
-- (a `SET LOCAL` that leaked, or an opener misused by a future call site,
-- would still buy nothing); it is STATE-scoped, and no established team ocean
-- can ever be named by it. Verified as an assertion, not a comment:
-- `tenant_rls_pg.rs::team_create_bootstrap_arm_cannot_target_an_established_ocean`.
--
-- ── The RETURNING trap (why `graphs` was failing too) ──────────────────────
-- `INSERT ... RETURNING` applies the table's SELECT policy to the returned
-- row. `create_team_tx_scoped` used `RETURNING id` three times, so `graphs`
-- failed at step (1) even though its `WITH CHECK` (`kind <> 'member' OR ...`)
-- passes for `kind='team'` — and the error is the SAME "new row violates
-- row-level security policy" text and the SAME SQLSTATE 42501 that a real
-- `WITH CHECK` refusal raises, which makes it read like a policy bug that
-- isn't there.
--
-- The companion Rust change mints `graph_id`/`team_id`/`team_key_id` with
-- `Uuid::new_v4()` and drops `RETURNING` entirely. That is load-bearing for
-- THIS migration's shape, not a tidy-up: had `RETURNING` stayed, the
-- bootstrap grant would have needed a SELECT arm as well, and a SELECT arm on
-- these tables is the widening direction this whole design exists to refuse.
--
-- ── The SECURITY DEFINER trap (why there are three functions, not one) ─────
-- A policy expression is evaluated as the QUERYING role, so a table named
-- inside it is itself filtered by ITS policies. Writing the two hop-shaped
-- arms as plain subqueries —
--
--     team_id IN (SELECT id FROM teams WHERE graph_id = <bootstrap graph>)
--
-- — returns zero rows mid-bootstrap, because `teams` is not yet visible. It
-- fails for exactly the reason it was written to fix. This is the same
-- observation 0009 makes about `tenant_rls_visible_graph_ids()` ("SECURITY
-- DEFINER is load-bearing, not decorative"), and it applies again here: the
-- hops must run as the function owner. Hence `_team_ids()` and
-- `_team_key_ids()` rather than inline subqueries.
--
-- ── Command split ──────────────────────────────────────────────────────────
-- A bare policy is `FOR ALL` and reuses `USING` as `WITH CHECK`, which is
-- what forces the bootstrap arm to be expressible on reads too. The four
-- affected tables therefore move to the 4-way `_select`/`_insert`/`_update`/
-- `_delete` split 0009 already uses for `node_promotions`, for the same
-- underlying reason (read and write are genuinely different rules here).
--
-- SEMANTICS ARE PRESERVED EXACTLY on every command except INSERT:
--   * `teams`' `USING` keeps 0032's org-admin OR-arm; its UPDATE `WITH CHECK`
--     keeps 0032's deliberately narrow `graph_id IN visible`, which is what
--     stops an org admin repointing `teams.graph_id` at another ocean. That
--     hazard is UPDATE-shaped, and splitting is what lets it stay narrow on
--     UPDATE without also blocking INSERT.
--   * `team_members` keeps 0032's org-admin OR-arm on all four commands.
--   * `team_keys` / `team_member_key_envelopes` keep 0009's predicate with NO
--     org-admin arm, exactly as today — 0032 deliberately confined that grant
--     to the three governance tables and this migration does not extend it.
--   * `membership_events` is NOT touched: step (6) already passes.
--
-- Observed and deliberately NOT changed: a non-member org admin satisfies
-- `teams`' UPDATE `USING` but not its `WITH CHECK`, so they cannot rename a
-- team. That is 0032's behaviour today; if it is wrong it is a separate
-- decision, and silently changing it inside a bootstrap fix would be the
-- wrong place to make it.
--
-- Depends on: 0009_tenant_rls (the predicate and the four policies rewritten
-- here), 0032_membership_events (the org-admin arms preserved here).
--
-- Down:
--   DROP POLICY IF EXISTS member_ocean_isolation_select ON teams;
--   DROP POLICY IF EXISTS member_ocean_isolation_insert ON teams;
--   DROP POLICY IF EXISTS member_ocean_isolation_update ON teams;
--   DROP POLICY IF EXISTS member_ocean_isolation_delete ON teams;
--   CREATE POLICY member_ocean_isolation ON teams
--       USING (graph_id IN (SELECT * FROM tenant_rls_visible_graph_ids())
--              OR tenant_rls_is_org_admin())
--       WITH CHECK (graph_id IN (SELECT * FROM tenant_rls_visible_graph_ids()));
--   DROP POLICY IF EXISTS member_ocean_isolation_select ON team_members;
--   DROP POLICY IF EXISTS member_ocean_isolation_insert ON team_members;
--   DROP POLICY IF EXISTS member_ocean_isolation_update ON team_members;
--   DROP POLICY IF EXISTS member_ocean_isolation_delete ON team_members;
--   CREATE POLICY member_ocean_isolation ON team_members
--       USING (team_id IN (SELECT id FROM teams
--              WHERE graph_id IN (SELECT * FROM tenant_rls_visible_graph_ids()))
--              OR tenant_rls_is_org_admin());
--   DROP POLICY IF EXISTS member_ocean_isolation_select ON team_keys;
--   DROP POLICY IF EXISTS member_ocean_isolation_insert ON team_keys;
--   DROP POLICY IF EXISTS member_ocean_isolation_update ON team_keys;
--   DROP POLICY IF EXISTS member_ocean_isolation_delete ON team_keys;
--   CREATE POLICY member_ocean_isolation ON team_keys
--       USING (graph_id IN (SELECT * FROM tenant_rls_visible_graph_ids()));
--   DROP POLICY IF EXISTS member_ocean_isolation_select ON team_member_key_envelopes;
--   DROP POLICY IF EXISTS member_ocean_isolation_insert ON team_member_key_envelopes;
--   DROP POLICY IF EXISTS member_ocean_isolation_update ON team_member_key_envelopes;
--   DROP POLICY IF EXISTS member_ocean_isolation_delete ON team_member_key_envelopes;
--   CREATE POLICY member_ocean_isolation ON team_member_key_envelopes
--       USING (team_key_id IN (SELECT id FROM team_keys
--              WHERE graph_id IN (SELECT * FROM tenant_rls_visible_graph_ids())));
--   REVOKE EXECUTE ON FUNCTION tenant_rls_bootstrap_team_key_ids() FROM coralswarm_tenant_app;
--   REVOKE EXECUTE ON FUNCTION tenant_rls_bootstrap_team_ids() FROM coralswarm_tenant_app;
--   REVOKE EXECUTE ON FUNCTION tenant_rls_bootstrap_graph_id() FROM coralswarm_tenant_app;
--   DROP FUNCTION IF EXISTS tenant_rls_bootstrap_team_key_ids();
--   DROP FUNCTION IF EXISTS tenant_rls_bootstrap_team_ids();
--   DROP FUNCTION IF EXISTS tenant_rls_bootstrap_graph_id();

-- ── 1. The bootstrap resolvers ──────────────────────────────────────────────

-- The one graph id this transaction is allowed to bootstrap, or NULL.
--
-- `SECURITY DEFINER` for the same reason `tenant_rls_visible_graph_ids()` is:
-- it reads `graphs`/`teams`/`team_members`, whose own policies would filter
-- (and, via `graphs`, recurse into) the very lookup being performed.
--
-- `NULLIF(..., '')` for the same reason `tenant_rls_actor_user_id()` has it:
-- `app.*` is an unregistered placeholder class, so a pooled connection whose
-- previous borrower set this GUC observes `''` — not "unset" — on its next
-- transaction, and `''::uuid` inside a policy is a hard ERROR rather than a
-- denial. NULL denies exactly what unset denies.
--
-- Three conditions, all required:
--   * the GUC names this graph;
--   * it is a TEAM ocean (a member/org/personal ocean is never bootstrapped
--     this way, and must not become writable by naming it here);
--   * it has NO `team_members` rows at all — not "none for the actor". This
--     is the self-extinguishing clause. `departing_at` is deliberately NOT
--     consulted: a team whose whole roster has departed is ESTABLISHED, not
--     fresh, and must never fall back into a bootstrappable state.
CREATE OR REPLACE FUNCTION tenant_rls_bootstrap_graph_id()
RETURNS uuid
LANGUAGE sql
SECURITY DEFINER
STABLE
SET search_path = public, pg_temp
AS $$
    SELECT g.id
    FROM graphs g
    WHERE g.id = NULLIF(current_setting('app.creating_graph_id', true), '')::uuid
      AND g.kind = 'team'
      AND NOT EXISTS (
          SELECT 1
          FROM teams t
          JOIN team_members tm ON tm.team_id = t.id
          WHERE t.graph_id = g.id
      )
$$;

-- The `teams.id`s of the bootstrapping graph. SECURITY DEFINER because the
-- caller cannot yet SELECT `teams` for this ocean — that is the whole problem.
CREATE OR REPLACE FUNCTION tenant_rls_bootstrap_team_ids()
RETURNS SETOF uuid
LANGUAGE sql
SECURITY DEFINER
STABLE
SET search_path = public, pg_temp
AS $$
    SELECT t.id FROM teams t WHERE t.graph_id = tenant_rls_bootstrap_graph_id()
$$;

-- The `team_keys.id`s of the bootstrapping graph, for the envelope insert.
CREATE OR REPLACE FUNCTION tenant_rls_bootstrap_team_key_ids()
RETURNS SETOF uuid
LANGUAGE sql
SECURITY DEFINER
STABLE
SET search_path = public, pg_temp
AS $$
    SELECT k.id FROM team_keys k WHERE k.graph_id = tenant_rls_bootstrap_graph_id()
$$;

REVOKE EXECUTE ON FUNCTION tenant_rls_bootstrap_graph_id() FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION tenant_rls_bootstrap_team_ids() FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION tenant_rls_bootstrap_team_key_ids() FROM PUBLIC;

-- Policies are evaluated as the QUERYING role, so the app role needs EXECUTE
-- and nobody else does. Role-guarded exactly as 0009 and 0032 guard theirs, so
-- a scratch/parity database that applies the tenant tree without the role
-- provisioning block does not fail here.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'coralswarm_tenant_app') THEN
        GRANT EXECUTE ON FUNCTION tenant_rls_bootstrap_graph_id() TO coralswarm_tenant_app;
        GRANT EXECUTE ON FUNCTION tenant_rls_bootstrap_team_ids() TO coralswarm_tenant_app;
        GRANT EXECUTE ON FUNCTION tenant_rls_bootstrap_team_key_ids() TO coralswarm_tenant_app;
    END IF;
END
$$;

-- ── 2. `teams` ──────────────────────────────────────────────────────────────
-- All five names dropped, for the idempotency reason 0009 spells out at
-- `node_promotions`: `CREATE POLICY` has no `IF NOT EXISTS`, so a re-apply
-- must not fail partway and leave the table with reads but no writes.
DROP POLICY IF EXISTS member_ocean_isolation ON teams;
DROP POLICY IF EXISTS member_ocean_isolation_select ON teams;
DROP POLICY IF EXISTS member_ocean_isolation_insert ON teams;
DROP POLICY IF EXISTS member_ocean_isolation_update ON teams;
DROP POLICY IF EXISTS member_ocean_isolation_delete ON teams;

CREATE POLICY member_ocean_isolation_select ON teams
    FOR SELECT
    USING (
        graph_id IN (SELECT * FROM tenant_rls_visible_graph_ids())
        OR tenant_rls_is_org_admin()
    );

-- The only widened command. `tenant_rls_bootstrap_graph_id()` is NULL for
-- every graph except one this transaction just created, so for every other
-- caller this arm is `graph_id = NULL` — never true, never an error.
CREATE POLICY member_ocean_isolation_insert ON teams
    FOR INSERT
    WITH CHECK (
        graph_id IN (SELECT * FROM tenant_rls_visible_graph_ids())
        OR graph_id = tenant_rls_bootstrap_graph_id()
    );

-- 0032's narrow `WITH CHECK`, unchanged and deliberately WITHOUT the org-admin
-- arm: this is the expression that stops `UPDATE teams SET graph_id = <another
-- team's ocean>` pulling that ocean into `tenant_rls_visible_graph_ids()`.
CREATE POLICY member_ocean_isolation_update ON teams
    FOR UPDATE
    USING (
        graph_id IN (SELECT * FROM tenant_rls_visible_graph_ids())
        OR tenant_rls_is_org_admin()
    )
    WITH CHECK (graph_id IN (SELECT * FROM tenant_rls_visible_graph_ids()));

CREATE POLICY member_ocean_isolation_delete ON teams
    FOR DELETE
    USING (
        graph_id IN (SELECT * FROM tenant_rls_visible_graph_ids())
        OR tenant_rls_is_org_admin()
    );

-- ── 3. `team_keys` ──────────────────────────────────────────────────────────
-- 0009's predicate, no org-admin arm (0032 confined that grant to the three
-- governance tables; `team_keys` is explicitly asserted as NOT leaked to org
-- admins by the regression test named in this migration's header).
DROP POLICY IF EXISTS member_ocean_isolation ON team_keys;
DROP POLICY IF EXISTS member_ocean_isolation_select ON team_keys;
DROP POLICY IF EXISTS member_ocean_isolation_insert ON team_keys;
DROP POLICY IF EXISTS member_ocean_isolation_update ON team_keys;
DROP POLICY IF EXISTS member_ocean_isolation_delete ON team_keys;

CREATE POLICY member_ocean_isolation_select ON team_keys
    FOR SELECT
    USING (graph_id IN (SELECT * FROM tenant_rls_visible_graph_ids()));

CREATE POLICY member_ocean_isolation_insert ON team_keys
    FOR INSERT
    WITH CHECK (
        graph_id IN (SELECT * FROM tenant_rls_visible_graph_ids())
        OR graph_id = tenant_rls_bootstrap_graph_id()
    );

CREATE POLICY member_ocean_isolation_update ON team_keys
    FOR UPDATE
    USING (graph_id IN (SELECT * FROM tenant_rls_visible_graph_ids()))
    WITH CHECK (graph_id IN (SELECT * FROM tenant_rls_visible_graph_ids()));

CREATE POLICY member_ocean_isolation_delete ON team_keys
    FOR DELETE
    USING (graph_id IN (SELECT * FROM tenant_rls_visible_graph_ids()));

-- ── 4. `team_member_key_envelopes` ──────────────────────────────────────────
DROP POLICY IF EXISTS member_ocean_isolation ON team_member_key_envelopes;
DROP POLICY IF EXISTS member_ocean_isolation_select ON team_member_key_envelopes;
DROP POLICY IF EXISTS member_ocean_isolation_insert ON team_member_key_envelopes;
DROP POLICY IF EXISTS member_ocean_isolation_update ON team_member_key_envelopes;
DROP POLICY IF EXISTS member_ocean_isolation_delete ON team_member_key_envelopes;

CREATE POLICY member_ocean_isolation_select ON team_member_key_envelopes
    FOR SELECT
    USING (team_key_id IN (
        SELECT id FROM team_keys
        WHERE graph_id IN (SELECT * FROM tenant_rls_visible_graph_ids())
    ));

CREATE POLICY member_ocean_isolation_insert ON team_member_key_envelopes
    FOR INSERT
    WITH CHECK (
        team_key_id IN (
            SELECT id FROM team_keys
            WHERE graph_id IN (SELECT * FROM tenant_rls_visible_graph_ids())
        )
        OR team_key_id IN (SELECT * FROM tenant_rls_bootstrap_team_key_ids())
    );

CREATE POLICY member_ocean_isolation_update ON team_member_key_envelopes
    FOR UPDATE
    USING (team_key_id IN (
        SELECT id FROM team_keys
        WHERE graph_id IN (SELECT * FROM tenant_rls_visible_graph_ids())
    ))
    WITH CHECK (team_key_id IN (
        SELECT id FROM team_keys
        WHERE graph_id IN (SELECT * FROM tenant_rls_visible_graph_ids())
    ));

CREATE POLICY member_ocean_isolation_delete ON team_member_key_envelopes
    FOR DELETE
    USING (team_key_id IN (
        SELECT id FROM team_keys
        WHERE graph_id IN (SELECT * FROM tenant_rls_visible_graph_ids())
    ));

-- ── 5. `team_members` ───────────────────────────────────────────────────────
-- 0032's org-admin arm is preserved on ALL FOUR commands: roster management on
-- any team in the org is precisely what that grant is for, and it is the
-- derived-WITH-CHECK of the bare policy that makes the roster WRITES work
-- today. Splitting must not quietly drop it from the write side.
DROP POLICY IF EXISTS member_ocean_isolation ON team_members;
DROP POLICY IF EXISTS member_ocean_isolation_select ON team_members;
DROP POLICY IF EXISTS member_ocean_isolation_insert ON team_members;
DROP POLICY IF EXISTS member_ocean_isolation_update ON team_members;
DROP POLICY IF EXISTS member_ocean_isolation_delete ON team_members;

CREATE POLICY member_ocean_isolation_select ON team_members
    FOR SELECT
    USING (
        team_id IN (
            SELECT id FROM teams
            WHERE graph_id IN (SELECT * FROM tenant_rls_visible_graph_ids())
        )
        OR tenant_rls_is_org_admin()
    );

CREATE POLICY member_ocean_isolation_insert ON team_members
    FOR INSERT
    WITH CHECK (
        team_id IN (
            SELECT id FROM teams
            WHERE graph_id IN (SELECT * FROM tenant_rls_visible_graph_ids())
        )
        OR tenant_rls_is_org_admin()
        OR team_id IN (SELECT * FROM tenant_rls_bootstrap_team_ids())
    );

CREATE POLICY member_ocean_isolation_update ON team_members
    FOR UPDATE
    USING (
        team_id IN (
            SELECT id FROM teams
            WHERE graph_id IN (SELECT * FROM tenant_rls_visible_graph_ids())
        )
        OR tenant_rls_is_org_admin()
    )
    WITH CHECK (
        team_id IN (
            SELECT id FROM teams
            WHERE graph_id IN (SELECT * FROM tenant_rls_visible_graph_ids())
        )
        OR tenant_rls_is_org_admin()
    );

CREATE POLICY member_ocean_isolation_delete ON team_members
    FOR DELETE
    USING (
        team_id IN (
            SELECT id FROM teams
            WHERE graph_id IN (SELECT * FROM tenant_rls_visible_graph_ids())
        )
        OR tenant_rls_is_org_admin()
    );
