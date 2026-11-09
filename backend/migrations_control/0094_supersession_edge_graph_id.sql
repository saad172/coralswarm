-- Migration: 0094_supersession_edge_graph_id
-- Description: Give `supersession_edge` the `graph_id` column every other edge
--              table already has, delete (after recording) the cross-ocean rows
--              that could only ever exist because it did not, and make the
--              column NOT NULL so every future row has a well-defined home
--              ocean.
-- Depends on: 0011_nodes (supersession_edge, nodes), 0003_atoms (graphs)
-- PAIRED WITH: migrations_tenant/0008_supersession_edge_graph_id.sql — the DDL
--              in the two files is IDENTICAL and must stay that way
--              (`scripts/check_schema_parity.sh` diffs a normalized pg_dump of
--              both trees and fails on any difference).
-- Down:
--   ALTER TABLE supersession_edge DROP COLUMN IF EXISTS graph_id;
--   DROP TABLE IF EXISTS supersession_edge_cross_ocean_deletions;
--   (the deleted edges are NOT restored by the down migration — they are
--    recoverable by hand from supersession_edge_cross_ocean_deletions, which
--    is why that table is written before the DELETE runs.)
--
-- ─────────────────────────────────────────────────────────────────────────────
-- WHY THIS EXISTS
-- ─────────────────────────────────────────────────────────────────────────────
--
-- `supersession_edge` was the ONLY edge table in the schema with no `graph_id`.
-- `relationship_edge` (0040) has one, `NOT NULL REFERENCES graphs(id) ON DELETE
-- CASCADE`, and is correctly ocean-scoped everywhere as a result. That
-- asymmetry is the root cause of a real authorization defect, not a cosmetic
-- inconsistency:
--
--   * `POST /v1/nodes/{id}/supersede` authorized BOTH endpoints with
--     `node_owned_by_account`. In an org tenant, every member-private ocean
--     (`graphs.kind = 'member'`) shares the ONE org `account_id` — so an
--     account-scoped predicate is satisfied by every member's private ocean
--     simultaneously, i.e. it is a no-op across the member-ocean boundary.
--     Member A could therefore record "the org commons reef is superseded by
--     MY private reef" using only ids she already held.
--   * `Store::list_supersession_targets` matches on `superseded_id` alone, so
--     any other member retrieving that org commons reef got A's private reef
--     id back in `search_atoms`' `superseded_by`. That read-side disclosure
--     was closed in #348; the WRITE side (this migration's companion change to
--     `routes/supersession.rs`) was not, and the rows could still be created.
--   * This migration's own RLS backstop is written on the same false premise:
--     0083's policy comment says "an edge's two endpoints are always same-graph
--     by construction, so anchoring on either one identifies the same graph/
--     account". That was an assumption, never a constraint. After this
--     migration it is true of every row in the table.
--
-- ─────────────────────────────────────────────────────────────────────────────
-- WHAT "CROSS-OCEAN" MEANS HERE, PRECISELY
-- ─────────────────────────────────────────────────────────────────────────────
--
-- The predicate this migration deletes on is EXACTLY:
--
--     the graph of the superseded endpoint <> the graph of the superseder
--     endpoint
--
-- and nothing else. In particular it is NOT "the two endpoints are in oceans
-- owned by different users", NOT "one endpoint is in a `kind='member'` ocean",
-- and NOT "some user cannot reach both" — all three of those would be
-- over-broad or under-broad, and an over-broad delete here destroys legitimate
-- knowledge-graph data that cannot be reconstructed.
--
-- Justification for same-graph as THE invariant:
--
--   1. It is the invariant the schema already claims. 0083's `account_isolation`
--      policy on this table anchors on `superseded_id` only, and is only
--      correct if the two endpoints agree on their graph.
--   2. It is the invariant the AUTOMATIC producer already upholds.
--      `services::supersession::insert_supersession_edge_for_atoms` resolves
--      both endpoints via `node_timestamps_for_atoms(store, atoms, graph_id)` —
--      a single-graph lookup — so no auto-detected (`source='auto'`) edge can
--      span two oceans. The only producer that ever could was the API route.
--   3. It is the invariant the SYNC protocol already assumes: `sync::
--      supersession_op` takes ONE `graph_id` for the whole edge.
--   4. It is what makes this column's value well-defined at all. An edge whose
--      endpoints live in different oceans has no single home ocean, so it
--      cannot be given a truthful `graph_id`, cannot be NOT NULL, and cannot be
--      filtered by ocean on the read path.
--   5. A cross-ocean edge is a disclosure EVEN BETWEEN two oceans one user
--      legitimately reaches. Reachability is per-caller; the row is not. An
--      edge from the org commons reef to the author's own member reef is
--      writable by someone who reaches both, and is then read by every OTHER
--      org member — who reaches the commons and not the member ocean — via
--      `GET /v1/nodes/{id}/supersessions` (which returns `superseder_title`,
--      i.e. content, not just an id) and via `superseded_by` on the retrieval
--      path. "Both endpoints reachable by the writer" is therefore NOT a safe
--      carve-out, which is why the delete is on graph inequality rather than on
--      a reachability predicate.
--
-- Consequence worth stating plainly: on any deployment that never enabled org
-- tenancy or member oceans, this delete is expected to affect ZERO rows — the
-- only way to produce a cross-graph edge was to call `POST /supersede` with two
-- node ids from different oceans of the same account, which required an account
-- whose graphs are not all mutually visible. It is written to be correct rather
-- than assumed to be a no-op.
--
-- ─────────────────────────────────────────────────────────────────────────────
-- WHY THE ROWS ARE DELETED RATHER THAN REPORTED, AND WHY THEY ARE STILL
-- RECOVERABLE
-- ─────────────────────────────────────────────────────────────────────────────
--
-- Deletion is a deliberate decision: these rows cannot be inspected before the
-- migration runs (production content is not readable from a review), and a
-- cross-ocean edge is an active disclosure for as long as it exists. But a
-- delete that leaves no trace destroys the evidence of an incident along with
-- the incident, so every row is copied into
-- `supersession_edge_cross_ocean_deletions` FIRST, in the SAME transaction —
-- both endpoints, BOTH resolved graph ids, the row's own `source`/`confirmed_*`
-- columns, and a timestamp. The edges are gone; what they said is not.

-- ── 1. The forensic ledger, written before anything is removed ───────────────
--
-- Deliberately carries NO foreign keys. Its whole purpose is to outlive the
-- rows it describes: an FK to `nodes` or `graphs` would ON DELETE CASCADE the
-- evidence away the moment the reef or the ocean it names is deleted (and a
-- member-ocean purge deletes exactly those). It holds only identifiers and the
-- edge's own metadata — no reef titles, no coral text, nothing that could
-- itself become a disclosure surface.
--
-- Not covered by 0083's `account_isolation` RLS, deliberately and unlike the
-- 36 content tables that are: no application code reads or writes this table on
-- any plane — it is written once, by this migration, and read by a human during
-- an incident. It is infrastructure in the same category as `_sqlx_migrations`,
-- not a query surface. If a future change gives it an application reader, it
-- must get a policy in the same PR.
CREATE TABLE IF NOT EXISTS supersession_edge_cross_ocean_deletions (
    superseded_id        UUID        NOT NULL,
    superseder_id        UUID        NOT NULL,
    -- Both sides recorded, not just the one the surviving column would have
    -- derived: the whole reason the row was illegal is that these two disagree,
    -- so recording one of them would discard the finding itself.
    superseded_graph_id  UUID        NOT NULL,
    superseder_graph_id  UUID        NOT NULL,
    source               TEXT        NOT NULL,
    confirmed_by         UUID,
    confirmed_at         TIMESTAMPTZ,
    deleted_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- Which migration removed it, so a second cleanup (should one ever be
    -- needed) is distinguishable from this one in the same ledger.
    deleted_by           TEXT        NOT NULL,

    PRIMARY KEY (superseded_id, superseder_id, deleted_at)
);

-- ── 2. The column itself, shaped exactly like `relationship_edge.graph_id` ───
--
-- Added NULLABLE here and tightened to NOT NULL in step 5, because the backfill
-- in step 4 has to run first. `ON DELETE CASCADE` matches `relationship_edge`;
-- it is belt-and-braces (dropping a graph already cascades through `nodes` to
-- both endpoint FKs) but keeps the two edge tables shaped identically.
ALTER TABLE supersession_edge
    ADD COLUMN IF NOT EXISTS graph_id UUID REFERENCES graphs(id) ON DELETE CASCADE;

-- ── 3. Record, then delete, every cross-ocean edge ───────────────────────────
--
-- INNER joins are exact, not lossy: `superseded_id`/`superseder_id` are both
-- `NOT NULL REFERENCES nodes(node_id)`, so a row with a missing endpoint cannot
-- exist and no edge can escape this scan.
INSERT INTO supersession_edge_cross_ocean_deletions
    (superseded_id, superseder_id, superseded_graph_id, superseder_graph_id,
     source, confirmed_by, confirmed_at, deleted_by)
SELECT e.superseded_id,
       e.superseder_id,
       superseded_node.graph_id,
       superseder_node.graph_id,
       e.source,
       e.confirmed_by,
       e.confirmed_at,
       'supersession_edge_graph_id (control 0094 / tenant 0008)'
  FROM supersession_edge e
  JOIN nodes superseded_node ON superseded_node.node_id = e.superseded_id
  JOIN nodes superseder_node ON superseder_node.node_id = e.superseder_id
 WHERE superseded_node.graph_id <> superseder_node.graph_id;

DELETE FROM supersession_edge e
 USING nodes superseded_node, nodes superseder_node
 WHERE superseded_node.node_id = e.superseded_id
   AND superseder_node.node_id = e.superseder_id
   AND superseded_node.graph_id <> superseder_node.graph_id;

-- ── 4. Backfill the surviving rows ───────────────────────────────────────────
--
-- Derived from the SUPERSEDED endpoint. After step 3 the two endpoints agree on
-- every remaining row, so this choice is provably immaterial to the RESULT —
-- the only rows on which the two endpoints could have disagreed are exactly the
-- ones just deleted. It is a tie-break for consistency, not a semantic bet, and
-- it is broken toward `superseded_id` because that is the endpoint 0083's RLS
-- policy anchors on and the endpoint `Store::list_supersession_targets` joins
-- on — so the column now agrees with both.
--
-- Always resolves: `superseded_id` is `NOT NULL REFERENCES nodes(node_id)` and
-- `nodes.graph_id` is itself `NOT NULL`, so there is no row for which the
-- source value could be NULL. Step 5 does not assume that — `SET NOT NULL`
-- scans the table and fails the migration loudly if it is ever untrue.
UPDATE supersession_edge e
   SET graph_id = superseded_node.graph_id
  FROM nodes superseded_node
 WHERE superseded_node.node_id = e.superseded_id
   AND e.graph_id IS NULL;

-- ── 5. Now it can be NOT NULL ────────────────────────────────────────────────
ALTER TABLE supersession_edge
    ALTER COLUMN graph_id SET NOT NULL;

-- ── 6. Ocean-scoped scans ("every supersession edge in this ocean") ──────────
-- Mirrors `idx_relationship_edge_graph_status`' purpose on the other edge
-- table; `supersession_edge` has no `status`, so the graph alone is the key.
CREATE INDEX IF NOT EXISTS idx_supersession_edge_graph_id
    ON supersession_edge (graph_id);
