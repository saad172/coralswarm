-- Migration: 0191_query_audit_log
-- Description: PR 4.3 of the m365 enterprise connector plan (plan decision
--   #11, part 2) — the QUERY-AUDIT disclosure log. Spec: "Who asked, oceans
--   searched, atoms cited; org-admin-visible only; disclosed to users. Emit
--   from the ask/retrieval path; admin route + disclosure copy."
--
--   Sibling spec 4.2 (`connector_audit_log`, a SEPARATE, NOT-yet-built table)
--   is the append-only CONNECTOR event log; this migration is unrelated to
--   it. Do not confuse the two — see `services::query_audit`'s module doc for
--   the full contrast, including why THIS table's write does not inherit
--   4.2's "write failures fail the action" durability bar.
--
--   ── Digest, not payload ──
--   `question_digest` is a SHA-256 hex digest of the asked question text
--   (`services::query_audit::question_digest`) — the raw question is NEVER
--   stored here, same precedent as `mcp_tool_calls.args_digest`
--   (`mcp::audit`, migration 0058): an admin reading this log learns THAT a
--   question was asked and what it touched, never its content.
--
--   ── Append-only ──
--   Only `account_isolation_select`/`account_isolation_insert` policies
--   exist below — deliberately no UPDATE/DELETE policy for the app role, so
--   both commands are denied by RLS's default-deny (RLS enabled, no
--   applicable policy for that command) rather than by mere convention.
--   `membership_events` (migration 0125) is append-only by CONVENTION only
--   (its single bare policy is `FOR ALL`, so UPDATE/DELETE are technically
--   permitted at the DB layer, just never issued by any code path) — this
--   table is stronger: append-only is enforced by Postgres itself, not just
--   by discipline.
--
--   ── The SELECT/RLS split, stated honestly ──
--   The plan requires "org-admin-visible only". RLS policies read Postgres
--   session GUCs (`app.account_id`, `app.user_id`) — they cannot read a
--   Clerk org-role claim, so there is no honest way to encode "admin" as an
--   RLS predicate here (contrast `migrations_tenant/0032_membership_events`,
--   which COULD do this because `tenancy::scope::set_tenant_org_admin` mints
--   a verified `app.is_org_admin` GUC from an `OrgAdminProof` for that
--   specific governance surface — deliberately NOT reused here to keep this
--   table's RLS surface minimal; see the tenant twin's header for why the
--   admin gate lives at the route instead). So the SELECT policy below is a
--   plain account-scope backstop — ANY session scoped to this account could
--   read these rows at the DB layer — and the REAL "org-admin-only"
--   enforcement is `routes::query_audit::list_query_audit`'s
--   `services::deletion::is_admin_role(&user.role)` gate, checked BEFORE any
--   row is ever read. This is a deliberate, reviewed split, not an
--   oversight: RLS backstops the ACCOUNT boundary (this account's rows never
--   leak to another account's session, defense in depth); the ROUTE
--   enforces the ADMIN boundary (which the DB cannot see). Both must hold
--   for the feature to be correct; only one lives in SQL.
--
--   ── Column shapes ──
--   `graphs_searched`/`atom_ids_cited` are bounded `uuid[]` columns (CHECK
--   constraints below) rather than a normalized join table — this is a
--   disclosure/audit record, not a queryable graph relationship, and the
--   caller (`services::ask::run_ask`, `routes::queries`) already has both
--   lists in memory at the one point it writes this row.
--
--   ── Schema-parity twin ──
--   The paired tenant migration is `migrations_tenant/
--   0069_query_audit_log.sql`, per the `0106`/`0017`/`0184`/`0064`
--   byte-equivalent-shape convention (see e.g. `0184`'s header). Columns and
--   CHECK constraints are byte-identical; only the `account_id` FK (control
--   only — `accounts` does not exist on the tenant tree) and the RLS layer
--   (plane-specific throughout this repo) differ, both stripped by
--   `scripts/check_schema_parity.sh` before comparison.
--
--   `asked_by_user_id` deliberately carries NO FK to `users(id)` on this
--   plane either (unlike `query_history.user_id`, which is nullable with
--   `ON DELETE SET NULL`): this is an append-only audit trail, and it must
--   survive the acting user's own account departure/deletion — a cascading
--   or nulling FK would let a user's later removal quietly erase or orphan
--   the disclosure record of what they asked while still a member. Same
--   "bare uuid, no FK, audit permanence" reasoning
--   `migrations_tenant/0032_membership_events`'s header gives for
--   `actor_user_id`/`target_user_id`, applied here on BOTH planes rather
--   than only the plane where the FK target happens to be absent.
--
--   ── Reserved migration numbers (announced) ──
--   This PR is ONLY permitted to stamp 0191 (control) / 0069 (tenant).
--   0189/0067 and 0190/0068 belong to open sibling PRs (#692 and one other)
--   and may be ABSENT from this tree at review time — that is the expected
--   in-flight state, not a gap this PR introduces or must fix.
--
-- Depends on: 0017_query_history (precedent for an account+user-anchored
--             audit-shaped table), 0106_team_seat_grants/0184 (the
--             byte-equivalent-twin + control-side command-split convention
--             `scripts/check_schema_parity.sh`'s CONTROL_ONLY_RLS_TABLES
--             special-cases for `node_promotions`, extended here to this
--             table)
-- Down:
--   DROP POLICY IF EXISTS account_isolation_select ON query_audit_log;
--   DROP POLICY IF EXISTS account_isolation_insert ON query_audit_log;
--   DROP TABLE IF EXISTS query_audit_log;

CREATE TABLE IF NOT EXISTS query_audit_log (
    id                UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    account_id        UUID        NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
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

-- ── RLS: account-scope backstop (SELECT), acting-session pairing (INSERT) ──
-- See the header's "SELECT/RLS split, stated honestly" section: SELECT here
-- is deliberately NOT admin-aware (RLS cannot read a Clerk role) — the real
-- admin gate is `routes::query_audit::list_query_audit`'s route-layer check.
-- Command-split (2 named policies, no UPDATE/DELETE) rather than one bare
-- `FOR ALL` policy: append-only is enforced by the ABSENCE of an UPDATE/
-- DELETE policy (RLS's default-deny for an unmatched command), not by
-- convention. `scripts/check_schema_parity.sh`'s CONTROL_ONLY_RLS_TABLES
-- strip loop special-cases this table (alongside `node_promotions`) to drop
-- these split names — see that script's own comment at the call site.
ALTER TABLE query_audit_log ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS account_isolation_select ON query_audit_log;
DROP POLICY IF EXISTS account_isolation_insert ON query_audit_log;
CREATE POLICY account_isolation_select ON query_audit_log
    FOR SELECT
    USING (account_id = current_setting('app.account_id')::uuid);
CREATE POLICY account_isolation_insert ON query_audit_log
    FOR INSERT
    WITH CHECK (
        account_id = current_setting('app.account_id')::uuid
        AND asked_by_user_id = NULLIF(current_setting('app.user_id', true), '')::uuid
    );
