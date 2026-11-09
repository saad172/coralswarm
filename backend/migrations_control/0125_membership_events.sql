-- Migration: 0125_membership_events
-- Description: org-team-membership design (docs decisions #4/#11,
--              /private/tmp/.../org-team-membership-design.md as reviewed for
--              this PR) — an append-only audit trail of `team_members`
--              changes. Load-bearing for decision #4 ("org admins get roster
--              management on any team, no read bypass — break-glass = admin
--              adds self, which is a visible row + audit event") and for the
--              future `GET /v1/teams/{id}/events` history endpoint. SCHEMA
--              ONLY: no Rust in this repo reads or writes this table yet.
--
--              ── Why this table is PAIRED, not control-only ──
--              Unlike `member_ocean_lifecycle_log` (0092, deliberately
--              control-only because it must survive a deleted TENANT
--              database), this table describes `team_members`, and
--              `team_members` itself lives on WHICHEVER plane the team's
--              account lives on: a personal account's teams live in the
--              single shared control-plane Postgres, an org account's teams
--              live in that org's tenant Neon database (0017_team_seat_
--              grants's header — "team_members exists tenant-side too" — is
--              the load-bearing precedent this migration follows). An event
--              row about a personal-account team change would have no
--              database to live in if this table were control-only, and vice
--              versa for org teams. So this migration is paired byte-for-byte
--              with `migrations_tenant/0032_membership_events.sql`, exactly
--              the `graphs`-columns convention `0090_member_ocean_columns`
--              set (see that migration's header) and NOT the later
--              `0106_team_seat_grants` / `0017_team_seat_grants` "correction"
--              pattern (that pair's `CREATE TABLE` bodies genuinely differ —
--              cross-plane FKs present on one side, absent on the other —
--              because 0106 was originally written control-only by mistake
--              and 0017 repaired it after the fact; this table was designed
--              paired from the start, so there is no excuse for the same
--              drift here).
--
--              ── Column choices ──
--              `team_id UUID NOT NULL REFERENCES teams(id) ON DELETE CASCADE`
--              — safe and IDENTICAL in both trees because `teams` itself is a
--              shared table present in both (0022_teams control-side,
--              0001_tenant_baseline.sql:718 tenant-side). Matches
--              `team_members.team_id`'s own `ON DELETE CASCADE` (0022) —a
--              team's audit trail has no meaning once the team itself is
--              gone, same as its membership rows.
--
--              `actor_user_id UUID` and `target_user_id UUID` are
--              DELIBERATELY bare — no FK to `users(id)`, even here on the
--              control plane where that FK COULD be declared. `users` is a
--              control-tree-only table (absent from every tenant database),
--              so a tenant-tree `membership_events` could never carry that
--              FK, and this migration's whole point is that the two trees'
--              DDL bodies stay byte-identical. This is the exact same choice
--              `0090_member_ocean_columns` made for `graphs.owner_user_id`
--              ("bare uuid, no FK... an FK here could not even be DECLARED on
--              the tenant tree") — copied here rather than re-derived,
--              because it is the same structural constraint, not a
--              coincidence.
--
--              `actor_user_id` is NULLABLE (unlike `target_user_id`): the
--              member-ocean reconciler's archive-arm cascade (design decision
--              #7 — org offboard cascades `departing_at` onto `team_members`,
--              landing "inside the member-ocean reconciler's archive arm") has
--              no human actor to record. `target_user_id` is always the
--              person whose membership changed, so it is NOT NULL
--              unconditionally.
--
--              `action` is a closed CHECK vocabulary, not free TEXT: the four
--              request-driven values (`added`, `removed`, `role_changed`,
--              `created`) plus `cascade_removed` for the reconciler's
--              non-human cascade — the one action `actor_user_id IS NULL` is
--              expected to co-occur with. `created` covers the creator's
--              seeded first-admin row (decision #8: "creator is just the
--              first admin... no durable owner role"), which is otherwise
--              indistinguishable from a later `added`.
--
--              `old_role`/`new_role` are deliberately UNCONSTRAINED TEXT, not
--              CHECKed against `team_members.role`'s own `('admin','member')`
--              vocabulary: `old_role` must be representable as NULL for an
--              `added`/`created` event (no prior role exists), and binding
--              this table's CHECK to `team_members`'s role vocabulary would
--              require this migration to change in lockstep with any future
--              role-vocabulary migration, for a column whose only real
--              constraint is "whatever `team_members.role` allowed at
--              read/write time" — enforced at the application layer, not
--              here, the same way `member_ocean_lifecycle_log.actor` (0092)
--              is a documented-but-unconstrained `TEXT`.
--
--              ── Append-only, by convention not by trigger ──
--              No `UPDATE`/`DELETE` code path exists or is expected to. A
--              BEFORE trigger enforcing that was considered and rejected —
--              see this table's tenant-tree twin's header for the RLS
--              discussion, and note this is schema-only scope: the row is
--              never mutated in application code, so a trigger would be
--              pure defense-in-depth against a bug class (an app-level
--              `UPDATE membership_events`) that does not exist yet, on a
--              table with no writers yet either.
--
--              ── RLS: control-plane `account_isolation`, added here ──
--              Not explicitly requested by this table's design doc, but
--              added for symmetry with `team_members`/`team_seat_grants`
--              (both of which carry BOTH an account-plane `account_isolation`
--              policy, 0083, AND a tenant-plane `member_ocean_isolation`
--              policy, 0009/0017) rather than leave this one new table as the
--              sole team_id-anchored gap in that otherwise-complete pair of
--              rosters. Anchored identically to `team_members`'s own 0083
--              policy (line ~696): `team_id IN (SELECT id FROM teams WHERE
--              account_id = current_setting('app.account_id')::uuid)`. Same
--              inert-until-cutover caveat as every other 0083-listed policy:
--              this is `ENABLE`-only RLS, so it is a no-op today while the
--              app connects as the table owner (0083 §7 / OPS-4).
--              Registered in `scripts/check_schema_parity.sh`'s
--              `CONTROL_ONLY_RLS_TABLES` in this same PR, per that script's
--              own instruction.
--
--              ── No org-admin arm here, and why ──
--              The tenant twin of this migration adds an
--              `OR tenant_rls_is_org_admin()` arm to its policies (and to
--              `teams`/`team_members`), because the tenant-plane predicate
--              gates on ACTIVE TEAM MEMBERSHIP and a non-member org admin —
--              whom `routes::teams` authorizes for roster management, design
--              decision #4 — would otherwise be shown an empty roster and an
--              empty audit trail. Deliberately NOT mirrored here, for three
--              independent reasons, any one of which is sufficient:
--
--              1. There is nothing to unlock. This policy's predicate is
--                 `account_id = current_setting('app.account_id')` — an
--                 ACCOUNT boundary, not a membership one. Every caller
--                 already sees every row of their own account's teams
--                 regardless of team membership or org role, so an org-admin
--                 arm would widen nothing and would only add a second way to
--                 satisfy a predicate that is already satisfied.
--              2. It could only ever widen the ONE boundary that must not
--                 widen. The only rows this policy hides are OTHER ACCOUNTS'
--                 — the consumer plane holds every account in one physical
--                 database — so an `OR <anything>` arm here is an
--                 `OR <cross-account leak>` arm. Being an admin of org X is
--                 not a reason to read org Y's audit trail.
--              3. The caller class does not exist on this plane. An org
--                 admin acting under `ORG_TEAMS_ENABLED` is served from that
--                 org's own TENANT database (`0017_team_seat_grants`'s
--                 "team_members exists tenant-side too"); the rows they are
--                 being wrongly denied are tenant-side rows. A control-plane
--                 `membership_events` row belongs to a PERSONAL account's
--                 team, and `is_org_admin` requires
--                 `accounts.clerk_org_id IS NOT NULL`, so it can never be
--                 true for the account those rows belong to.
--
--              Consequently `tenant_rls_is_org_admin()` and the
--              `app.is_org_admin` GUC exist ONLY on the tenant tree (see
--              `check_schema_parity.sh`'s `TENANT_FUNCTIONS`, whose whole
--              purpose is to record exactly this kind of deliberate
--              asymmetry). The `CREATE TABLE`/index DDL below stays
--              byte-identical to the tenant twin, as that script's schema
--              diff requires; only the RLS section differs, which is the
--              documented plane-specific carve-out.
--
-- Depends on: 0022_teams (teams), 0083_consumer_rls (account_isolation
--             pattern, current_setting('app.account_id')).
--
-- Down:
--   DROP POLICY IF EXISTS account_isolation ON membership_events;
--   ALTER TABLE membership_events DISABLE ROW LEVEL SECURITY;
--   DROP INDEX IF EXISTS membership_events_team_created_idx;
--   DROP TABLE IF EXISTS membership_events;

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

ALTER TABLE membership_events ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS account_isolation ON membership_events;
CREATE POLICY account_isolation ON membership_events
    USING (team_id IN (
        SELECT id FROM teams WHERE account_id = current_setting('app.account_id')::uuid
    ));
