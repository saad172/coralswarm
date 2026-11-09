-- 0032_membership_events.sql — paired half of
-- `migrations_control/0125_membership_events.sql` (org-team-membership design,
-- decisions #4/#11). See that migration's header for the full rationale: why
-- this table is PAIRED rather than control-only (it describes `team_members`,
-- which itself lives on whichever plane a team's account lives on — personal
-- accounts control-side, org accounts tenant-side, per
-- `0017_team_seat_grants`'s "team_members exists tenant-side too"
-- precedent), why `actor_user_id`/`target_user_id` carry no FK (`users` is
-- control-tree-only and does not exist in this database at all), why
-- `action` is a closed CHECK vocabulary with `cascade_removed` as the one
-- value expected to co-occur with a NULL `actor_user_id`, and why
-- `old_role`/`new_role` are deliberately unconstrained TEXT rather than
-- CHECKed against `team_members.role`'s vocabulary.
--
-- The `CREATE TABLE` + index DDL below is BYTE-IDENTICAL to the control
-- migration (only comments/header prose differ) — `teams` is a SHARED table
-- (present in both trees; NOT in `check_schema_parity.sh`'s `CONTROL_TABLES`
-- roster), so `check_schema_parity.sh`'s schema-only `pg_dump` diff would
-- fail on any divergence in the table body here. The RLS section below is
-- NOT part of that byte-identical contract — RLS is plane-specific by
-- design throughout this repo (0083 vs. 0009/0017 already diverge for
-- exactly this reason on `team_members`/`team_seat_grants`), and is stripped
-- out of the parity diff via `CONTROL_ONLY_RLS_TABLES`/
-- `TENANT_ONLY_RLS_TABLES` before comparison.
--
-- This migration is SCHEMA ONLY, same as its control counterpart: no Rust in
-- this repo reads or writes `membership_events` yet.
--
-- ── RLS: tenant-plane `member_ocean_isolation`, added here ──
-- Explicitly requested by this table's design doc §11 ("Load-bearing for
-- [org admins get roster management]"), and required reading before writing
-- this migration: `backend/migrations_tenant/0009_tenant_rls.sql` is the
-- tenant plane's RLS roster migration, and it is ALREADY MERGED (append-only
-- — see that file's own "Never edit an already-merged migration" discipline
-- and `scripts/check_migrations.sh`'s checksum guard, which PINS 0009's
-- content hash in `ALLOWED_EDITS`), so neither a new table nor a corrected
-- policy can be added to 0009 itself. The correct place is a new migration,
-- exactly the precedent `0017_team_seat_grants` set for the identical
-- situation (a new team_id-anchored table landing after 0009 was already
-- closed).
--
-- Anchored IDENTICALLY to `team_members`'s own 0009 policy (line ~1391) and
-- to `team_seat_grants`'s 0017 policy: `team_id -> teams.graph_id ->
-- tenant_rls_visible_graph_ids()`, the SECURITY DEFINER member-ocean
-- visibility predicate every 0009-shaped policy on this plane resolves
-- through — plus ONE further OR-arm for verified org admins, described in
-- its own section below.
--
-- Same fail-closed consequence as every other 0009 policy: an unscoped
-- tenant session (no `app.user_id` set, no `app.is_org_admin` set) sees zero
-- rows here, not an error (0009 §3).
--
-- Registered in `scripts/check_schema_parity.sh`'s `TENANT_ONLY_RLS_TABLES`
-- in this same PR, per that list's own "keep in lockstep with EVERY
-- migration that ENABLEs a tenant-plane member_ocean_isolation-shaped
-- policy" instruction; `tenant_rls_is_org_admin()` is registered in that
-- script's `TENANT_FUNCTIONS` for the same reason.
--
-- ── The org-admin governance arm (design doc decision #4) ──
-- This is the second half of this migration and the reason it also RE-STATES
-- the policies on `teams` and `team_members` that 0009 originally declared.
--
-- The bug it fixes: `routes::teams`'s roster and audit endpoints authorize a
-- NON-MEMBER org admin at the route layer (`is_org_admin`, a CONTROL-plane
-- check against `accounts.clerk_org_id` + `account_members.role`), but the
-- tenant-plane policies expose `teams`/`team_members`/`membership_events`
-- rows only through `tenant_rls_visible_graph_ids()`, which for a TEAM ocean
-- requires an ACTIVE `team_members` row. Once the app connects as the
-- least-privilege `coralswarm_tenant_app` role (Phase 3 / "OPS-4-T"; a
-- superuser DSN bypasses RLS entirely, which is why this does not reproduce
-- locally), the route says yes and the query returns nothing:
--   * `GET /v1/teams/{id}/events` answers `200 []` — "this team has no
--     history" — to the one caller class that exists to audit it;
--   * `GET /v1/teams/{id}/members` answers `404 team not found` for a team
--     that plainly exists;
--   * every roster WRITE (`POST`/`PATCH`/`DELETE .../members`) 404s at its
--     `SELECT 1 FROM teams WHERE id = $1 FOR UPDATE` row lock.
-- I.e. decision #4 — the headline feature — is inert post-cutover, and its
-- read half is inert SILENTLY.
--
-- What is deliberately NOT done: `tenant_rls_visible_graph_ids()` is left
-- exactly as 0009 wrote it. That function is the CONTENT anchor — `atoms`,
-- `nodes`, `conversations`, `op_log`, `team_keys` and ~25 other tables all
-- resolve through it — so an OR-arm there would hand every org admin the
-- contents of every team ocean in the tenant. That is precisely the
-- "org-admin read bypass" the design's "Rejected, and why" refuses ("would
-- add an OR-arm to the single authorization point that 41 call sites
-- trust"). The whole value of this fix is that it stays OUT of the content
-- path: it widens three GOVERNANCE tables and nothing else, so an org admin
-- can see and manage WHO IS IN a team without gaining one atom of what the
-- team knows. Break-glass remains what decision #4 says it is — the admin
-- adds themselves, which is a visible `team_members` row plus a
-- `membership_events` audit row.
--
-- Why `teams` is in the governance set at all: the roster/audit handlers
-- resolve a team by id before doing anything else (`team_exists_scoped`, and
-- `lock_team_row_scoped`'s `FOR UPDATE` for the decision-#9 last-admin
-- serialization). Without a `teams` arm the grant on the other two tables
-- would be unreachable in practice — every governance request would 404
-- first. What that arm exposes is a team's NAME, `graph_id` and `created_by`
-- to an admin of the same org; `graph_id` is not a capability (every content
-- table re-derives visibility from the untouched function above), and the
-- design doc's own open items already list an "orphaned-teams view for org
-- admins" as wanted.
--
-- `teams`' arm is `USING`-ONLY, with an explicit `WITH CHECK` restating
-- 0009's original predicate verbatim. This asymmetry is load-bearing, not
-- tidiness: a bare policy reuses `USING` as `WITH CHECK`, which would let an
-- org admin `UPDATE teams SET graph_id = <another team's ocean>` on a team
-- they DO belong to and thereby pull that ocean into
-- `tenant_rls_visible_graph_ids()` — a silent, unaudited content bypass
-- through the one column that anchors the whole predicate. `USING` alone
-- gives SELECT and the existing-row check that `SELECT ... FOR UPDATE`
-- requires (PostgreSQL applies the UPDATE policy's `USING` to `SELECT FOR
-- UPDATE/SHARE`), while `WITH CHECK` keeps every row an org admin can leave
-- behind anchored to an ocean they can actually reach.
--
-- `team_members` and `membership_events` stay BARE (derived `WITH CHECK`),
-- which is what makes the roster WRITES work: adding, re-roling and
-- soft-removing members of a team the admin is not in is exactly the right
-- granted by decision #4, and every one of those writes emits an audit row
-- into `membership_events` in the same transaction.
--
-- ── The blast radius, measured rather than reasoned about ──
-- The obvious worry with widening `teams` is transitive leakage: the
-- `team_id IN (SELECT id FROM teams WHERE ...)` subqueries used by this
-- migration and by `0017_team_seat_grants` run in INVOKER context (only
-- `tenant_rls_visible_graph_ids()` is SECURITY DEFINER), so `teams`' own —
-- now widened — policy DOES apply inside them. It does not leak, and the
-- reason is worth stating because it is not the reason one guesses: those
-- subqueries carry their OWN `WHERE graph_id IN (SELECT * FROM
-- tenant_rls_visible_graph_ids())`. RLS deciding a `teams` row is visible does
-- not make that row pass a WHERE clause it fails on the merits. So the
-- widened `teams` policy hands the subquery a row, and the subquery's own
-- predicate discards it again.
--
-- Two consequences, both confirmed against a real PG 17 database with the full
-- tenant tree applied and a connection as the non-BYPASSRLS
-- `coralswarm_tenant_app` role (org admin B, deliberately NOT a member of
-- member A's team):
--
--   1. The explicit `OR tenant_rls_is_org_admin()` on `team_members` and
--      `membership_events` is LOAD-BEARING, not belt-and-braces. Widening
--      `teams` alone leaves both at zero rows for an org admin, exactly as
--      today. Do not remove either arm on the theory that the `teams` arm
--      already covers them.
--   2. `team_seat_grants` (0017) does NOT widen — measured at 0 rows for the
--      org admin, unchanged — for the same reason. Its policy is left exactly
--      as 0017 wrote it, and no other table in the tenant roster anchors
--      through `teams` at all.
--
-- Measured for the same session, same transaction: `teams` 1, `team_members`
-- 1, `membership_events` 1, `SELECT ... FOR UPDATE` on the team row 1 —
-- and `graphs` 0, `atoms` 0, `team_keys` 0, `team_seat_grants` 0. That last
-- line is the property the whole change stands on: `team_keys` and
-- `team_member_key_envelopes` anchor on `graph_id`/`team_keys.graph_id`, not
-- through `teams`, and `graphs` anchors on `tenant_rls_visible_graph_ids()`
-- directly. A non-member org admin gains no ocean row, no atom, no node, and
-- — critically — no wrapped team key. They see the roster and its history;
-- they cannot open the ocean.
--
-- Writes were measured too: with the grant set and no `team_members` row of
-- their own, an org admin's `INSERT`/`UPDATE` on `teams` — including a benign
-- rename, and including repointing `graph_id` at another ocean — is refused
-- with "new row violates row-level security policy for table teams", while
-- `INSERT`/`UPDATE` on `team_members` and `INSERT` on `membership_events`
-- succeed. An ordinary member's own session is byte-for-byte unaffected
-- (same visible counts, same successful rename of their own team).
--
-- The GUC (`app.is_org_admin`) is set in ONE place in the backend —
-- `tenancy::scope::set_tenant_org_admin`, which cannot be called without an
-- `OrgAdminProof`, which in turn can only be minted by the control-plane
-- check itself. See that module's header for the full GUC contract; the
-- discipline is the same one `app.user_id` carries ("MUST be the
-- gate-verified acting user, never a handler parameter").
--
-- Restated rather than added as a SECOND policy on purpose: multiple
-- PERMISSIVE policies OR together, so `CREATE POLICY org_admin_governance
-- ...` would have been semantically equivalent for `team_members` — but it
-- would ALSO have OR'd into the derived `WITH CHECK` on `teams`, which is the
-- one place that must not widen, and it would leave `check_schema_parity.sh`
-- (which strips tenant RLS by policy NAME, `member_ocean_isolation`) and
-- `tenant_rls_pg.rs`'s policy-count invariant both silently wrong. One name
-- per table, replaced in place, keeps every one of those consumers honest.
--
-- Depends on: 0001_tenant_baseline (teams, team_members — the baseline
--             already carries the post-0022 shape of `teams`, so there is no
--             separate tenant-tree pairing needed for that migration),
--             0006_member_ocean_reachability_functions (unrelated function,
--             listed for completeness only),
--             0009_tenant_rls (tenant_rls_visible_graph_ids() — the
--             SECURITY DEFINER predicate this migration's policies resolve
--             through — and the `teams`/`team_members` policies restated
--             below, plus the `coralswarm_tenant_app` role the GRANTs
--             target).
--
-- Down:
--   DROP POLICY IF EXISTS member_ocean_isolation ON membership_events;
--   ALTER TABLE membership_events DISABLE ROW LEVEL SECURITY;
--   DROP INDEX IF EXISTS membership_events_team_created_idx;
--   DROP TABLE IF EXISTS membership_events;
--   -- restore 0009's originals verbatim:
--   DROP POLICY IF EXISTS member_ocean_isolation ON team_members;
--   CREATE POLICY member_ocean_isolation ON team_members
--       USING (team_id IN (SELECT id FROM teams
--              WHERE graph_id IN (SELECT * FROM tenant_rls_visible_graph_ids())));
--   DROP POLICY IF EXISTS member_ocean_isolation ON teams;
--   CREATE POLICY member_ocean_isolation ON teams
--       USING (graph_id IN (SELECT * FROM tenant_rls_visible_graph_ids()));
--   DROP FUNCTION IF EXISTS tenant_rls_is_org_admin();

CREATE TABLE membership_events (
    id              UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    team_id         UUID        NOT NULL REFERENCES teams(id) ON DELETE CASCADE,
    actor_user_id   UUID,       -- bare uuid, no FK (see header) — NULL for the reconciler cascade
    target_user_id  UUID        NOT NULL,   -- bare uuid, no FK (see header)
    action          TEXT        NOT NULL CHECK (action IN ('added', 'removed', 'role_changed', 'cascade_removed', 'created')),
    old_role        TEXT,
    new_role        TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- History endpoint access pattern: "the last N events for this team, newest
-- first" (GET /v1/teams/{id}/events).
CREATE INDEX membership_events_team_created_idx ON membership_events (team_id, created_at DESC);

-- Append-only: this table has no UPDATE/DELETE code path, by convention, not
-- by trigger (see header). Every row is a permanent audit fact.

-- ── The org-admin governance reader ─────────────────────────────────────────
-- Mirrors `tenant_rls_is_system()` (0009 §2) exactly, including the string
-- comparison rather than a `::boolean` cast: `current_setting(..., true)` on
-- an unregistered `app.*` placeholder returns `''` (not NULL) once a session
-- has set it even once, and `''::boolean` raises — inside a policy, that would
-- turn every query on a governance table into an error rather than a denial.
-- Comparing text can only ever yield true/false, so anything that is not the
-- exact string 'true' — unset, '', 't', 'TRUE' — is not an org admin.
--
-- NOT `SECURITY DEFINER`, and that is the same call 0009 made for its two GUC
-- readers: `SECURITY DEFINER` is load-bearing on
-- `tenant_rls_visible_graph_ids()` only because that function READS TABLES
-- (`graphs`/`teams`/`team_members`) whose own policies would otherwise
-- recurse. This one reads a session variable and touches no table at all, so
-- definer rights would grant the function's owner privileges for no reason —
-- strictly more authority than the job needs.
--
-- Same REVOKE-from-PUBLIC / GRANT-to-app-role treatment as 0009's three
-- functions: policies are evaluated as the QUERYING role, so the app role
-- needs EXECUTE, and nobody else does.
CREATE OR REPLACE FUNCTION tenant_rls_is_org_admin()
RETURNS boolean
LANGUAGE sql
STABLE
SET search_path = public, pg_temp
AS $$
    SELECT COALESCE(NULLIF(current_setting('app.is_org_admin', true), '') = 'true', false)
$$;

REVOKE EXECUTE ON FUNCTION tenant_rls_is_org_admin() FROM PUBLIC;

-- 0009 creates `coralswarm_tenant_app` inside a retry loop, so on any database
-- that has run 0009 the role exists; guarded anyway, for the same reason 0009
-- guards its own grants — a scratch/parity database that applies the tenant
-- tree without the role provisioning block must not fail here.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'coralswarm_tenant_app') THEN
        GRANT EXECUTE ON FUNCTION tenant_rls_is_org_admin() TO coralswarm_tenant_app;
    END IF;
END
$$;

-- ── The three governance policies ───────────────────────────────────────────
-- `teams`: 0009's predicate, plus a READ-ONLY org-admin arm. The explicit
-- `WITH CHECK` is 0009's original expression verbatim — see the header for why
-- letting the org-admin arm reach the write check would be a content bypass
-- through `teams.graph_id`.
ALTER TABLE teams ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS member_ocean_isolation ON teams;
CREATE POLICY member_ocean_isolation ON teams
    USING (
        graph_id IN (SELECT * FROM tenant_rls_visible_graph_ids())
        OR tenant_rls_is_org_admin()
    )
    WITH CHECK (graph_id IN (SELECT * FROM tenant_rls_visible_graph_ids()));

-- `team_members`: 0009's predicate, plus the org-admin arm on BOTH read and
-- (derived) write — roster management on any team in the org is the grant.
ALTER TABLE team_members ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS member_ocean_isolation ON team_members;
CREATE POLICY member_ocean_isolation ON team_members
    USING (
        team_id IN (
            SELECT id FROM teams
            WHERE graph_id IN (SELECT * FROM tenant_rls_visible_graph_ids())
        )
        OR tenant_rls_is_org_admin()
    );

-- `membership_events`: the audit trail itself. Same shape as `team_members` —
-- an org admin who may change the roster must be able to read (and, through
-- the roster writes, append to) the record of those changes.
ALTER TABLE membership_events ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS member_ocean_isolation ON membership_events;
CREATE POLICY member_ocean_isolation ON membership_events
    USING (
        team_id IN (
            SELECT id FROM teams
            WHERE graph_id IN (SELECT * FROM tenant_rls_visible_graph_ids())
        )
        OR tenant_rls_is_org_admin()
    );
