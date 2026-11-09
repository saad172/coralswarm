-- Migration: 0022_promotion_identity
-- Description: Workstream C §4.5 (docs/plans/graph-temporal-memory.md) —
--              tenant-tree half of the identity columns added by
--              `migrations_control/0111_promotion_identity_and_source_
--              publications.sql`. See that migration's header for the full
--              defect/fix writeup; this file exists because `nodes` is a
--              SHARED table (present in both trees — control's
--              `0011_nodes.sql`, tenant's `0001_tenant_baseline.sql`) and
--              `scripts/check_schema_parity.sh` diffs a schema-only dump of
--              both, so a change to a shared table's DDL must land as a
--              paired migration in both trees or new tenant DBs silently
--              diverge.
--
--              This is, deliberately, the ONLY thing this migration does.
--              `source_publications` (0111's other half) is control-only —
--              its `consented_by` column needs `users(id)`, and `users` does
--              not exist anywhere in this tree (org tenant databases have no
--              control-plane identity tables at all) — so it is NOT paired
--              here, and is registered instead in `check_schema_parity.sh`'s
--              CONTROL_TABLES drop list by 0111 itself.
--
--              `promoted_from_node_id`/`promoted_from_graph_id` stay bare
--              (no FK) here for the SAME reason 0090's `graphs.owner_user_id`
--              is bare in this tree: this is precisely the plane where a
--              cross-DB `promoted_from_*` value most often names a row that
--              lives in the OTHER database entirely (the consumer-plane
--              source reef `services/promotion.rs::promote_node_to_org`
--              copies from) — an FK could not even be declared.
--
--              ── Backfill: partial, and CORRECTED from this migration's
--              original claim of "NONE, and none is possible" ──
--              That claim held only for the CROSS-DB consumer→org path: this
--              tree has NO row anywhere recording which consumer-plane source
--              reef a given tenant `nodes` row was cross-DB-published from —
--              that really is unrecoverable, and stays exactly the
--              pre-existing defect this workstream fixes going forward (see
--              0111's header). But `node_promotions` is a SHARED table
--              (`migrations_tenant/0001_tenant_baseline.sql`), and by the time
--              this migration ships, the SAME-POOL member→org-commons
--              promotion path (member-private-oceans PR 6, `Store::
--              promote_node`/`promote_node_scoped`) already writes
--              `node_promotions` rows entirely WITHIN this tenant database —
--              identity that IS recoverable, exactly like the control tree's
--              own same-pool backfill recovers it for consumer-plane
--              same-pool promotions. Backfilling only that subset here (same
--              deterministic-pick rule as the control twin, see below) is
--              strictly better than leaving it NULL: every tenant-plane
--              same-pool promoted copy gets its real identity immediately,
--              and only the genuinely-unrecoverable cross-DB rows are left to
--              the same one-time, bounded, self-healing re-promotion the
--              control tree's header already accepts.
--
-- Depends on: 0001_tenant_baseline (nodes), 0005_member_ocean_columns
--             (owner_user_id bare-uuid precedent in this tree),
--             0018_nodes_case_insensitive_identity (the index this REPLACES
--             as promotion's upsert target for cross-DB-published copies —
--             untouched, still serves organic node creation)
--
-- Down:
--   DROP INDEX IF EXISTS nodes_graph_promoted_from_unique;
--   ALTER TABLE nodes DROP COLUMN IF EXISTS sync_mode;
--   ALTER TABLE nodes DROP COLUMN IF EXISTS promoted_from_graph_id;
--   ALTER TABLE nodes DROP COLUMN IF EXISTS promoted_from_node_id;

ALTER TABLE nodes
  ADD COLUMN IF NOT EXISTS promoted_from_node_id  UUID,   -- bare, no FK (see header)
  ADD COLUMN IF NOT EXISTS promoted_from_graph_id UUID,   -- bare, no FK (see header)
  ADD COLUMN IF NOT EXISTS sync_mode TEXT NOT NULL DEFAULT 'static'
    CHECK (sync_mode IN ('static', 'live'));

-- Backfill the recoverable subset: same-pool (member -> org commons)
-- promotions, whose provenance already lives in THIS database's own
-- `node_promotions` table. No-op on a fresh/empty `nodes` table.
--
-- DETERMINISTIC PICK, same rule as the control twin's identical backfill
-- (`migrations_control/0111_promotion_identity_and_source_publications.sql`
-- — see that file's comment for the full non-determinism hazard a bare join
-- would have here): earliest `promoted_at` wins when more than one
-- `node_promotions` row named the same `target_node_id` (the pre-fix
-- title-keyed conflict target could collide two different sources onto one
-- row), tie-broken by lowest `node_promotions.id`.
UPDATE nodes AS target
SET promoted_from_node_id  = chosen.source_node_id,
    promoted_from_graph_id = chosen.source_graph_id
FROM (
    SELECT DISTINCT ON (np.target_node_id)
        np.target_node_id,
        np.source_node_id,
        src.graph_id AS source_graph_id
    FROM node_promotions np
    JOIN nodes src ON src.node_id = np.source_node_id
    ORDER BY np.target_node_id, np.promoted_at ASC, np.id ASC
) AS chosen
WHERE target.node_id = chosen.target_node_id
  AND target.promoted_from_node_id IS NULL;

CREATE UNIQUE INDEX IF NOT EXISTS nodes_graph_promoted_from_unique
  ON nodes (graph_id, promoted_from_node_id);
