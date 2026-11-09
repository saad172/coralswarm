-- 0071_connector_audit_log.sql — the TENANT half of control's
-- `0193_connector_audit_log.sql` (m365 enterprise train PR 4.2, plan
-- decision #11 part 1: the connector compliance audit log). See that
-- migration's header for the full rationale — digest-not-payload,
-- append-only via command split, why `actor_user_id` carries no FK on
-- EITHER plane, and the reserved migration-number note (0191/0069, 0192/
-- 0070 belong to other in-flight PRs and may be absent from this tree).
--
-- The `CREATE TABLE` + CHECK constraints + index below are byte-identical
-- to control's, minus the `account_id -> accounts(id)` FK (`accounts` does
-- not exist on this plane) — `account_id` is a bare `uuid` here, same
-- structural difference `0069_query_audit_log.sql`'s twin already
-- established for the identical reason.
--
-- ── The SELECT/RLS split (tenant side) ──
-- Same reasoning as 0069's tenant twin: RLS cannot encode "org-admin-only"
-- (it cannot read a Clerk role), and this table has no single `graph_id`
-- anchor for a per-member `tenant_rls_visible_graph_ids()` predicate either
-- (`subject` is a bare TEXT id, not an ocean reference). SELECT is
-- therefore restricted to `tenant_rls_is_system()` ONLY — no ordinary
-- member session, admin or not, can read a `connector_audit_log` row
-- through the app role at all. The admin route
-- (`routes::connector_audit::list_connector_audit`) opens its read via
-- `tenancy::scope::begin_tenant_system_scoped` AFTER its own
-- `services::deletion::is_admin_role(&user.role)` gate has already passed.
--
-- INSERT is scoped to the acting session: either the tenant system scope
-- (`services::connector_audit`'s `Actor::System`/`Actor::Webhook` — a
-- reconciler tick or an inbound provider event, neither with a real member
-- identity to present) or the real acting member's own identity
-- (`tenant_rls_actor_user_id()`, `Actor::User`), mirroring
-- `query_audit_log`'s tenant twin's `asked_by_user_id` pairing exactly.
--
-- Named `member_ocean_isolation_select`/`_insert` for the same reason
-- `query_audit_log`'s tenant twin gives (0069's header, "Naming convention
-- note"): the shared name means `scripts/check_schema_parity.sh`'s
-- `TENANT_ONLY_RLS_TABLES` strip loop needs no change, only a roster-list
-- addition — even though, like that table, this one's predicate is not
-- actually a member-ocean-membership test.
--
-- Depends on: 0009_tenant_rls (tenant_rls_is_system(),
--             tenant_rls_actor_user_id() — the GUC readers this policy
--             resolves through), 0069_query_audit_log (the split-policy +
--             byte-equivalent-twin precedent this migration follows)
-- Down:
--   DROP POLICY IF EXISTS member_ocean_isolation_select ON connector_audit_log;
--   DROP POLICY IF EXISTS member_ocean_isolation_insert ON connector_audit_log;
--   DROP TABLE IF EXISTS connector_audit_log;

CREATE TABLE IF NOT EXISTS connector_audit_log (
    id              UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    account_id      UUID        NOT NULL,
    actor_user_id   UUID,
    actor_kind      TEXT        NOT NULL CHECK (actor_kind IN ('user', 'system', 'webhook')),
    action          TEXT        NOT NULL CHECK (action IN (
                        'install', 'consent', 'disconnect', 'purge',
                        'seat_grant', 'seat_revoke',
                        'route_container', 'unroute_container',
                        'ingestion_mode_change', 'deletion_propagated'
                    )),
    provider        TEXT,
    subject         TEXT        CHECK (subject IS NULL OR char_length(subject) <= 256),
    detail          JSONB       CHECK (detail IS NULL OR octet_length(detail::text) <= 4096),
    occurred_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- CodeRabbit round 1 on #701: `actor_kind`/`actor_user_id` pairing had no
    -- DB backstop — `('user', NULL)` and `('system'/'webhook', <uuid>)` were
    -- both insertable, so a write bypassing `services::connector_audit`'s
    -- `Actor` enum (the only thing enforcing this today) could misattribute
    -- or de-attribute a row. Mirrors the enum's own vocabulary exactly:
    -- `Actor::User(uid)` is the ONLY variant that carries a uuid. Identical
    -- to control's constraint (byte-identical twin).
    CONSTRAINT connector_audit_log_actor_pairing
        CHECK ((actor_kind = 'user') = (actor_user_id IS NOT NULL))
);

CREATE INDEX IF NOT EXISTS idx_connector_audit_log_account_occurred
    ON connector_audit_log (account_id, occurred_at DESC, id DESC);

ALTER TABLE connector_audit_log ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS member_ocean_isolation_select ON connector_audit_log;
DROP POLICY IF EXISTS member_ocean_isolation_insert ON connector_audit_log;
CREATE POLICY member_ocean_isolation_select ON connector_audit_log
    FOR SELECT
    USING (tenant_rls_is_system());
CREATE POLICY member_ocean_isolation_insert ON connector_audit_log
    FOR INSERT
    WITH CHECK (
        tenant_rls_is_system()
        OR actor_user_id = tenant_rls_actor_user_id()
    );
