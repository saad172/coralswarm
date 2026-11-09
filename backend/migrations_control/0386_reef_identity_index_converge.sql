-- PAIRED WITH: tenant 0203 (reef_identity_index_converge).
--
-- #1516 (founder 2026-10-27): every plane holds the same nodes identity
-- indexes, from a migration, whatever state a plane is in today.
--
-- * The title-only unique index nodes_graph_type_lower_title_unique is
--   gone. The identity contract (docs/implementation/p2-identity-contract.sql,
--   #1218) dropped it by hand on the prod org tenants and the prod control
--   plane, and nothing recorded it. Every database built from the migration
--   trees (new org tenants, scratch databases) still had it, so those ran the
--   pre-contract mint path ("ambiguous identity held"). It is dropped here.
-- * nodes_graph_type_lower_title_identity_unique (control 0294 / tenant 0129) stays the
--   one unique identity index. A person mint takes its evidence id as
--   identity_scope (per-evidence identity, ER decides). A first non-person
--   mint takes the shared scope md5('coralswarm:reef-title-kind:' || kind),
--   so this index already makes it the only (graph, type, title, kind) reef
--   minted that way; later mentions bind it in code
--   (store::door_exact_title_reef_tx).
-- * No new unique index on (graph_id, type, lower(title), kind). The existing
--   same-title copies would violate it, and they are merged only by the
--   reviewed one-off tool (dedupe_reefs --rule same-title) after a founder
--   yes. Creating it conditionally would leave planes in different states.
--   A follow-up migration can add it once the merge has run on every plane.
-- * idx_nodes_graph_type_lower_title serves the title lookups.
--
-- Idempotent: each statement is a no-op on a plane already in the target
-- state, and none fails on existing duplicates. Not part of the contract's
-- supersession reconcile: the application still writes source='auto'
-- supersession edges, so re-running that DELETE would destroy live rows.
DROP INDEX IF EXISTS nodes_graph_type_lower_title_unique;
CREATE UNIQUE INDEX IF NOT EXISTS nodes_graph_type_lower_title_identity_unique
    ON nodes (graph_id, type, lower(title), identity_scope);
CREATE INDEX IF NOT EXISTS idx_nodes_graph_type_lower_title
    ON nodes (graph_id, type, lower(title));
