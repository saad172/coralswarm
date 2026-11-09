-- 0007_share_account_id_functions.sql — paired half of the meeting/session
-- share token-bootstrap resolvers defined in `migrations_control/0086_
-- webhook_share_bootstrap_functions.sql` (`resolve_meeting_share_account_id`)
-- and `migrations_control/0087_session_share_bootstrap_functions.sql`
-- (`resolve_session_share_account_id`).
--
-- ── The bug this closes ──────────────────────────────────────────────────
-- Both resolvers were control-tree-only from the day they landed — unlike
-- the reachability pair (`resolve_tenant_reachable_graphs` /
-- `resolve_tenant_graph_reachable_by_user`, migration 0091/`migrations_tenant/
-- 0006`), they were never given a tenant-tree twin. `routes::shares::
-- resolve_share_by_token` and `routes::session_shares`'s private fn of the
-- same name both run `SELECT resolve_{meeting,session}_share_account_id($1)`
-- unconditionally against `tenant.pool` — which `backend/src/tenancy/
-- extractor.rs` binds to the ORG TENANT pool under an `Org` decision. With no
-- tenant-tree definition, that call hits Postgres error 42883 ("function does
-- not exist") before any visibility/reachability/archived_at check is ever
-- reached, so `GET /v1/meetings/shares/view/{token}` 500s for EVERY org-tenant
-- meeting share (live or archived, 'link' or 'team'). Confirmed live by
-- `backend/tests/member_ocean_probe_pg.rs`'s `archived_link_share_remains_viewable`.
--
-- ── Why a paired SQL twin (approach a) over branching Rust (approach b) ──
-- Migration 0091's header lays out the exact rationale that applies here
-- unchanged: replacing/adding a function's DEFINITION is a single atomic DDL
-- statement an OLD and a NEW pod see identically the instant it commits —
-- there is no rolling-deploy window where the two disagree, and ZERO Rust at
-- the call site needs to change (the existing `sqlx::query_scalar("SELECT
-- resolve_meeting_share_account_id($1)")` call sites in `routes/shares.rs`
-- and `routes/session_shares.rs` are untouched by this migration). Routing
-- the lookup through `Store::get_active_meeting_share_by_token[_scoped]`
-- directly instead (skipping the bootstrap function on the org-tenant arm)
-- would require the Rust call site to branch on `tenant.decision`
-- (Personal/legacy vs Org) — reintroducing exactly the hand-rolled,
-- plane-aware branching 0091 replaced for the reachability predicate, with
-- the same rolling-deploy hazard: an old pod's compiled branch would keep
-- calling the (still-missing) bootstrap function until every pod restarts.
-- Pairing the function is strictly safer and touches strictly less Rust.
--
-- `resolve_session_share_account_id`'s twin is added here too for the same
-- schema-parity discipline as 0091/0006 even though, at the moment this
-- migration lands, no live HTTP route calls it against an org-tenant pool:
-- `routes::session_shares` has no authed `.../view/{token}` route at all, and
-- its only viewer (`public_session_share`) is hardcoded to `Tenant::legacy`
-- (consumer-plane pool only — see that function's own doc comment). Pairing
-- it now means a future authed org-tenant session-share view route (the
-- natural next step, mirroring `routes::shares::view_share`) inherits a
-- working bootstrap resolver for free, and keeps `check_schema_parity.sh`
-- honest about both resolvers matching byte-for-byte across trees, exactly
-- like `resolve_graph_account_id`/`resolve_atoms_account_id` (0084/0003) and
-- `resolve_tenant_reachable_graphs`/`resolve_tenant_graph_reachable_by_user`
-- (0091/0006) already do.
--
-- The two graph-id resolvers 0086/0087 also define
-- (`resolve_meeting_share_graph_id`, `resolve_session_share_graph_id`, used
-- only by the revoke routes) are NOT paired here — `revoke_share`/
-- `revoke_session_share` have the same latent tenant-tree gap, but fixing the
-- VIEW surface is this migration's whole scope; see the PR body for that
-- separate, not-yet-fixed finding.
--
-- Bodies below are kept BYTE-IDENTICAL to their control-tree counterparts
-- (`resolve_meeting_share_account_id` in 0086, `resolve_session_share_account_id`
-- in 0087) — `check_schema_parity.sh` diffs `pg_dump --schema-only` output
-- (`--no-privileges`, so the GRANT/REVOKE statements below are deliberately
-- NOT required to match; only the function bodies are). `meeting_shares`,
-- `coding_session_shares`, and `graphs` are all shared tenant-tree tables
-- (present on both trees, `meeting_shares`/`coding_session_shares` are in
-- `check_schema_parity.sh`'s `CONTROL_ONLY_RLS_TABLES` list — RLS-policy-only
-- asymmetry, not a table-existence one), so both bodies are syntactically
-- valid here.
--
-- Unlike the control tree, this database has no `coralswarm_app`/RLS
-- privilege-separation model (same reasoning as `migrations_tenant/
-- 0003_bootstrap_scope_functions.sql` and `0006_member_ocean_reachability_
-- functions.sql`) — org-tenancy isolates tenants at the WHOLE-DATABASE level,
-- so there is no lower-privilege role here to `GRANT EXECUTE` to. Still
-- `SECURITY DEFINER` for exact parity with the control tree (harmless here:
-- whatever role the app connects to a tenant DB with already has full CRUD on
-- every table in it).
--
-- Depends on: 0001_tenant_baseline (graphs, meeting_shares,
--             coding_session_shares).
--
-- Down:
--   REVOKE EXECUTE ON FUNCTION resolve_meeting_share_account_id(text) FROM PUBLIC;
--   REVOKE EXECUTE ON FUNCTION resolve_session_share_account_id(text) FROM PUBLIC;
--   DROP FUNCTION IF EXISTS resolve_meeting_share_account_id(text);
--   DROP FUNCTION IF EXISTS resolve_session_share_account_id(text);

SET LOCAL lock_timeout = '3s';

-- ── 1. Meeting-share token -> owning account (ACTIVE shares only) ───────────
-- Byte-identical to migrations_control/0086's definition.
CREATE OR REPLACE FUNCTION resolve_meeting_share_account_id(p_token text)
RETURNS uuid
LANGUAGE sql
SECURITY DEFINER
STABLE
SET search_path = public, pg_temp
AS $$
    SELECT g.account_id
    FROM meeting_shares ms
    JOIN graphs g ON g.id = ms.graph_id
    WHERE ms.token = p_token
      AND ms.revoked_at IS NULL
    LIMIT 1
$$;

-- ── 2. Coding-session-share token -> owning account (ACTIVE shares only) ────
-- Byte-identical to migrations_control/0087's definition.
CREATE OR REPLACE FUNCTION resolve_session_share_account_id(p_token text)
RETURNS uuid
LANGUAGE sql
SECURITY DEFINER
STABLE
SET search_path = public, pg_temp
AS $$
    SELECT g.account_id
    FROM coding_session_shares css
    JOIN graphs g ON g.id = css.graph_id
    WHERE css.token = p_token
      AND css.revoked_at IS NULL
    LIMIT 1
$$;

-- ── Grants ────────────────────────────────────────────────────────────────
-- Same defense-in-depth reasoning as 0006's Grants section: Postgres grants
-- EXECUTE to PUBLIC by default on new functions, and both of these are
-- `SECURITY DEFINER`. There is no lower-privilege role in this tenant DB
-- today, but revoking the PUBLIC default costs nothing (the object OWNER
-- always retains implicit EXECUTE regardless of any REVOKE) and closes this
-- exposure in advance of any future lower-privilege tenant-DB role.
REVOKE EXECUTE ON FUNCTION resolve_meeting_share_account_id(text) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION resolve_session_share_account_id(text) FROM PUBLIC;
