-- Migration: 0069_query_audit_log — the TENANT half of control's
-- `0191_query_audit_log.sql` (m365 enterprise train PR 4.3, plan decision
-- #11 part 2: the query-audit disclosure log). See that migration's header
-- for the full rationale — digest-not-payload, append-only via command
-- split, why `asked_by_user_id` carries no FK on EITHER plane (audit
-- permanence), the byte-equivalent-twin convention, and the reserved
-- migration-number note (0189/0067, 0190/0068 belong to open sibling PRs and
-- may be absent from this tree — expected in-flight state).
--
-- The `CREATE TABLE` + CHECK constraints + index below are byte-identical to
-- control's, minus the `account_id -> accounts(id)` FK (`accounts` does not
-- exist on this plane) — `account_id` is a bare `uuid` here, same structural
-- difference `0064_team_seat_grants_history_from.sql`'s `seat_grant_sources`
-- twin already established for the identical reason.
--
-- ── The SELECT/RLS split, stated honestly (tenant side) ──
-- Control's header explains why RLS cannot encode "org-admin-only" at all
-- (it cannot read a Clerk role) and settles the split: RLS backstops a
-- boundary the DB CAN see, the route enforces the one it can't. On the
-- tenant plane the boundary the DB can honestly see is narrower still: a
-- tenant DB holds exactly ONE org already (physical isolation), and this
-- table has no single ocean/`graph_id` anchor to resolve a per-member
-- `tenant_rls_visible_graph_ids()` predicate through (`graphs_searched` is a
-- bounded array that can span up to `MAX_ASK_OCEANS` oceans per row, not one
-- team/member ocean) — so there is no honest "member-ocean" membership test
-- to write here either, unlike almost every other table in this roster.
--
-- SELECT is therefore restricted to `tenant_rls_is_system()` ONLY: no
-- ordinary member-scoped session — not even the asking member's own — can
-- read a `query_audit_log` row through the app role at all. The admin route
-- (`routes::query_audit::list_query_audit`) opens its read via
-- `tenancy::scope::begin_tenant_system_scoped` AFTER its own
-- `services::deletion::is_admin_role(&user.role)` gate has already passed —
-- the Clerk-role check happens before any transaction is even opened, and
-- the system-scoped read is what makes the (real, disclosure-worthy) rows
-- visible to that verified caller. This is stronger than plain
-- account-scope (control's own backstop): on this plane there is no
-- DB-level path AT ALL for an ordinary session, admin or not, to read this
-- table — every read is mediated by the route's role check by construction.
--
-- INSERT is scoped to the acting session: either the tenant system scope
-- (background/service-principal callers, if any ever write here — none do
-- today) or the real asking member's own identity
-- (`tenant_rls_actor_user_id()`, migration 0009's GUC reader), mirroring
-- control's `asked_by_user_id = app.user_id` pairing.
--
-- ── Naming convention note ──
-- Named `member_ocean_isolation_select`/`_insert` — the SAME prefix every
-- other split table on this plane uses (`atoms`, `teams`,
-- `team_member_key_envelopes`, …) — even though this table's predicate is
-- NOT actually a member-ocean-membership test (see above). This is a
-- deliberate reuse of the shared NAME convention, not a claim about the
-- predicate: `scripts/check_schema_parity.sh`'s `TENANT_ONLY_RLS_TABLES`
-- strip loop already tries all five `member_ocean_isolation[_select/_insert/
-- _update/_delete]` names, unconditionally, for every table in that roster
-- (see 0064's header for the identical reasoning when `atoms` was split) —
-- reusing the name means this migration needs NO change to that loop, only
-- a roster-list addition, exactly like every other post-0009 split table.
--
-- Depends on: 0009_tenant_rls (tenant_rls_is_system(),
--             tenant_rls_actor_user_id() — the GUC readers this policy
--             resolves through), 0017_team_seat_grants (the split-policy +
--             byte-equivalent-twin precedent this migration follows)
-- Down:
--   DROP POLICY IF EXISTS member_ocean_isolation_select ON query_audit_log;
--   DROP POLICY IF EXISTS member_ocean_isolation_insert ON query_audit_log;
--   DROP TABLE IF EXISTS query_audit_log;

CREATE TABLE IF NOT EXISTS query_audit_log (
    id                UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    account_id        UUID        NOT NULL,
    asked_by_user_id  UUID        NOT NULL,
    question_digest   TEXT        NOT NULL CHECK (question_digest ~ '^(unkeyed:)?[0-9a-f]{64}$'),
    graphs_searched   UUID[]      NOT NULL DEFAULT '{}',
    atom_ids_cited    UUID[]      NOT NULL DEFAULT '{}',
    asked_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT query_audit_log_graphs_bounded
        CHECK (array_length(graphs_searched, 1) IS NULL OR array_length(graphs_searched, 1) <= 10),
    CONSTRAINT query_audit_log_atoms_bounded
        CHECK (array_length(atom_ids_cited, 1) IS NULL OR array_length(atom_ids_cited, 1) <= 200)
);

CREATE INDEX IF NOT EXISTS idx_query_audit_log_account_asked_at
    ON query_audit_log (account_id, asked_at DESC, id DESC);

ALTER TABLE query_audit_log ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS member_ocean_isolation_select ON query_audit_log;
DROP POLICY IF EXISTS member_ocean_isolation_insert ON query_audit_log;
CREATE POLICY member_ocean_isolation_select ON query_audit_log
    FOR SELECT
    USING (tenant_rls_is_system());
CREATE POLICY member_ocean_isolation_insert ON query_audit_log
    FOR INSERT
    WITH CHECK (
        tenant_rls_is_system()
        OR asked_by_user_id = tenant_rls_actor_user_id()
    );
