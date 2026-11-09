-- Migration: 0201_user_active_graph
-- Description: Per-(user, account) ACTIVE OCEAN pointer — claim-based tenancy
--              decision 8 (docs/plans/claim-based-tenancy.md Phase 2). Until
--              now the user's active ocean lived ONLY in the browser's
--              `localStorage` under `cs_graph_id` (web/lib/providers.tsx), so
--              it did not survive a second browser, a second device, or a
--              reinstall. This table is the server-side home for it.
--
--              SCOPED PER (user, account), NOT PER USER. That is the whole
--              point: under claim-based tenancy one user moves between their
--              personal plane and N org planes within a single session, and
--              each plane has its OWN set of reachable oceans. A per-user
--              pointer would carry org A's ocean id into org B's boot — which
--              is exactly the defect the `localStorage` key has today (it is
--              one global key, and `providers.tsx`'s `resetOcean` never clears
--              it; only a lookup miss against the reachable list saves it).
--              The PK below makes that structurally impossible: a read for
--              (user, account B) cannot return account A's row.
--
--              WHY ITS OWN TABLE AND NOT A COLUMN ON `user_account_prefs`
--              (which has the identical `(user_id, account_id)` PK, and which
--              the written plan named): three independent reasons, any one of
--              them sufficient.
--
--              1. WRITING A POINTER WOULD SILENTLY REVOKE MCP WRITE. An
--                 ocean switch has to be able to create a row for a
--                 (user, account) pair that has none. On `user_account_prefs`
--                 that INSERT would materialize `mcp_write_enabled = false`
--                 (the column default), and `mcp::auth::
--                 mcp_write_enabled_for_account` treats a PRESENT row as
--                 AUTHORITATIVE (0080 precedence rule 1) while a MISSING row
--                 on a personal account falls back to the legacy
--                 `user_prefs.mcp_write_enabled` flag (rule 3). So for any
--                 personal-plane user whose legacy flag is on but who has no
--                 per-account row, merely switching oceans in the web UI
--                 would flip their MCP writes off, with no admin action and
--                 no signal. A UI preference must not be able to revoke a
--                 governance grant as a side effect.
--
--              2. `user_account_prefs.updated_at` IS A GOVERNANCE TIMESTAMP,
--                 NOT A ROW-TOUCHED TIMESTAMP. `clerk_provision::
--                 ensure_org_membership` reads it as `denied_at` and logs
--                 "org membership provisioned with an explicit MCP write DENY
--                 in force" so an admin can tell a stale deny from a current
--                 one. An ocean switch bumping that column would make a
--                 years-old deny read as freshly re-applied — turning the one
--                 visibility signal that table has into noise. Splitting the
--                 tables gives the pointer its own `updated_at` with its own,
--                 unrelated meaning.
--
--              3. `user_account_prefs`' DELETEs ARE DELIBERATELY PARTIAL.
--                 Org offboarding (`remove_org_membership`,
--                 `handle_org_deleted`) deletes ONLY rows with
--                 `mcp_write_enabled` true — "delete the grant, keep the
--                 deny", a fix for a real fail-open incident (PR #341). A
--                 pointer column riding on those rows would be deleted for
--                 granted members and preserved for denied ones: an
--                 arbitrary, invisible asymmetry in an unrelated feature.
--
--              NO FOREIGN KEY ON `graph_id`, AND THAT IS DELIBERATE. `graphs`
--              lives on the CONTENT plane, which for an org account is that
--              org's own tenant database (`backend/migrations_tenant/`) — a
--              different database entirely, where a cross-database FK is not
--              expressible. The pointer is therefore ADVISORY by
--              construction: it may name an ocean that has been deleted, or
--              one the user's access to has since been revoked.
--
--              THAT IS SAFE BECAUSE THE POINTER IS NEVER A CAPABILITY. It is
--              returned only to the same (user, account) that wrote it (via
--              `GET /v1/me`), and the client honours it only when it appears
--              in the reachable-ocean list the server independently returns
--              from `GET /v1/graphs` — the same intersection
--              `providers.tsx`'s `bootstrap()` already performs. A row here
--              grants nothing, selects nothing that authorization has not
--              already admitted, and an unresolvable pointer degrades to the
--              first reachable ocean rather than to an error.
--
--              NO RLS POLICY. This is a CONTROL-plane table keyed on
--              `user_id` first, like `user_prefs`/`user_account_prefs`/
--              `user_private_keys`; 0083's consumer-RLS roster is
--              account-scoped content tables, and every read/write here is
--              already pinned to the authenticated `UserContext`'s own
--              `user_id` AND `account_id`. Recorded in
--              `scripts/check_schema_parity.sh`'s CONTROL_TABLES in this same
--              PR (the paired-migration discipline that script's header
--              asks for).
--
-- Depends on: 0001_init (users, accounts)
--
-- Down:
--   DROP TABLE IF EXISTS user_active_graph;

CREATE TABLE IF NOT EXISTS user_active_graph (
    user_id    UUID        NOT NULL REFERENCES users(id)    ON DELETE CASCADE,
    account_id UUID        NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    -- The ocean (`graphs.id`) this user last selected while standing in this
    -- account. No FK — see the header: `graphs` is content-plane and lives in
    -- a different database for an org account.
    graph_id   UUID        NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (user_id, account_id)
);

-- The PK covers the only hot-path lookup, (user_id, account_id) — one row per
-- boot. This index covers the FK's delete side (account offboarding /
-- deletion cascades), the same rationale 0080 records for its own
-- `user_account_prefs_account_id_idx`.
CREATE INDEX IF NOT EXISTS user_active_graph_account_id_idx
    ON user_active_graph (account_id);

-- No backfill. There is nothing to backfill FROM: the pre-existing selection
-- lives in each browser's `localStorage`, which the server has never seen.
-- Every user's first boot after this ships reports their current selection
-- (client-side fallback order: server pointer, then the legacy `cs_graph_id`
-- value, then the first reachable ocean), which is what populates this table.
