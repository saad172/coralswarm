-- Migration: 0111_promotion_identity_and_source_publications
-- Description: Workstream C §4.5 (docs/plans/graph-temporal-memory.md) —
--              durable cross-plane identity for a promoted reef, plus a new
--              table for source-level (whole-connector) publication consent.
--              Step 1 of the workstream's own ordering: this lands ahead of
--              native derivation (§4.2), retraction (§4.9) and live sync
--              (§4.4), and fixes a real defect on its own.
--
--              ── The defect ──
--              `services::promotion::write_promotion_rows` (the cross-DB
--              consumer→org path) upserts the published copy keyed by
--              `ON CONFLICT (graph_id, type, lower(title))` — migration 0107's
--              case-insensitive identity index — and writes NO
--              `node_promotions` row at all (that table's `source_node_id` FK
--              targets `nodes(node_id)` in THIS db; a cross-plane source node
--              does not exist here, so the FK could not be satisfied — see
--              `services/promotion.rs`'s module doc). Renaming the source reef
--              and re-promoting therefore INSERTs a fresh row under the new
--              title instead of updating the old one: the target ocean ends up
--              with two reefs, one of them silently orphaned (no longer
--              produced by any promotion, never cleaned up). Live sync (§4.4)
--              would repeat this on every single rename.
--
--              The same-pool path (`Store::promote_node`) is commonly believed
--              safe because it DOES write `node_promotions`, keyed on
--              `(source_node_id, target_graph_id)` — but that table only
--              tracks *provenance*, not *target row identity*. Its own
--              `nodes` INSERT is the SAME title-keyed
--              `ON CONFLICT (graph_id, type, lower(title))`, so a rename still
--              creates a second `nodes` row under the new title; the
--              `node_promotions` upsert then just repoints `target_node_id` at
--              the NEW row, leaving the OLD one an orphaned duplicate reef
--              with no atoms flowing to it going forward (it keeps its
--              already-copied corals, but stops being what promotion writes
--              to). This migration's identity columns close the same-pool gap
--              too, so both paths upsert by durable id rather than by title.
--
--              ── The fix ──
--              `promoted_from_node_id`/`promoted_from_graph_id` name the
--              SOURCE reef a target copy was published from. Bare UUID
--              columns, NO foreign key — mirroring migration 0090's
--              `graphs.owner_user_id`, which crosses the identical
--              control-plane/tenant-plane boundary for the identical reason:
--              an FK cannot be declared across two independent Postgres
--              databases, and even for the same-pool (same-DB) case we keep
--              the shape uniform rather than have two different column
--              definitions depending on which path wrote them. NULL on every
--              row that exists today (organic reefs never had a source to
--              begin with; already-promoted reefs are backfilled below where
--              backfill data exists at all — see that section).
--
--              `CREATE UNIQUE INDEX ON nodes (graph_id, promoted_from_node_id)`
--              is the new conflict target `write_promotion_rows` and
--              `Store::promote_node` upsert against, INSTEAD OF the
--              title-keyed index. Postgres treats NULL as distinct from NULL
--              in a unique index by default (no NULLS NOT DISTINCT here), so
--              the countless organic reefs with `promoted_from_node_id IS
--              NULL` never collide with each other or with a promoted reef —
--              only two rows that name the SAME source, in the SAME target
--              graph, can conflict, which is exactly the "this is a
--              re-publish of the same reef" case the upsert needs to detect.
--
--              Accepted residual: a reef's FIRST promotion into a target
--              ocean whose `(graph_id, type, lower(title))` ALREADY collides
--              with an unrelated, pre-existing reef (organic, or promoted
--              from a different source) now raises a plain unique-violation
--              instead of silently upserting into that unrelated row. The OLD
--              behaviour — silently taking over a same-titled stranger reef
--              because titles happened to match — was never correct; a loud
--              failure on an already-rare title collision is a strict
--              improvement, not a new problem, and is left unhandled here
--              rather than adding a second conflict-resolution path for an
--              edge case the old code did not truly handle correctly either.
--
--              `sync_mode` is added now (needed by the unique-index change's
--              neighbourhood in the same ALTER) but stays fully inert until
--              §4.4 lands — every row defaults to 'static' and nothing reads
--              or writes 'live' yet.
--
--              ── Backfill (the DB wipe in §0 has NOT happened; this tree
--              APPENDS, it does not assume an empty `nodes` table) ──
--              Same-pool promoted copies ARE backfillable: `node_promotions`
--              already durably records `(source_node_id, target_node_id)` for
--              every one of them, so the UPDATE below walks that table and
--              stamps `promoted_from_node_id`/`promoted_from_graph_id` on the
--              target row it names. This is exactly the identity the new
--              unique index needs, recovered from data that already exists.
--
--              Cross-plane (org-tenant) promoted copies are NOT backfillable
--              — by construction, per the defect described above, no row
--              anywhere in this database (or the tenant database) has ever
--              recorded which consumer-plane source reef a given tenant-plane
--              `nodes` row came from. A pre-existing cross-plane promoted reef
--              therefore keeps `promoted_from_node_id IS NULL` after this
--              migration, and its FIRST re-promotion after this migration
--              ships creates ONE additional duplicate row (a single,
--              one-time, bounded recurrence of the exact bug being fixed —
--              not an ongoing one: from that re-promotion onward the identity
--              column is set and every subsequent re-promote of that reef
--              upserts correctly). No general fix exists for this without an
--              out-of-band reconciliation pass matching tenant reefs back to
--              consumer reefs by best-effort title equality, which is exactly
--              the unreliable signal this migration replaces — deliberately
--              not attempted here.
--
--              ── source_publications ──
--              A whole-source (whole-connector, e.g. a Slack channel) publish
--              has no row of its own anywhere — a source is only ever a
--              `source_id` string carried on `atoms`, never a first-class
--              entity — so its consent state needs its own table rather than
--              a column on something that does not exist. Control-plane ONLY:
--              `consented_by` needs `users(id)`, which does not exist in the
--              tenant tree (see `migrations_tenant/0001_tenant_baseline.sql`
--              — no `users` table), and a general cross-plane source-level
--              publication record (needed for the consumer→org path, exactly
--              the case `promoted_from_graph_id` above is bare for) is a named
--              follow-up, same as `services/promotion.rs`'s existing
--              "Cross-plane provenance is a named follow-up" note for
--              `node_promotions` — not invented here as a divergent,
--              un-speced tenant-side schema. Registered in
--              `scripts/check_schema_parity.sh`'s CONTROL_TABLES list in this
--              same migration.
--
--              RLS mirrors `node_promotions`' existing "two anchors" shape
--              from `migrations_control/0083_consumer_rls.sql` (source side OR
--              target side for READ, source side AND target side for WRITE) —
--              see that migration's `2d` section for the full rationale; this
--              table has the identical two-graph-anchor shape, just with both
--              anchors being direct `graph_id` columns instead of one hop
--              through `nodes`.
--
-- Depends on: 0011_nodes (nodes), 0026_ocean_model (node_promotions),
--             0090_member_ocean_columns (owner_user_id bare-uuid precedent),
--             0107_nodes_case_insensitive_identity (the index this REPLACES
--             as promotion's upsert target — that index itself is untouched
--             and keeps serving organic node creation elsewhere)
--
-- Down:
--   DROP POLICY IF EXISTS account_isolation_select ON source_publications;
--   DROP POLICY IF EXISTS account_isolation_insert ON source_publications;
--   DROP POLICY IF EXISTS account_isolation_update ON source_publications;
--   DROP POLICY IF EXISTS account_isolation_delete ON source_publications;
--   ALTER TABLE source_publications DISABLE ROW LEVEL SECURITY;
--   DROP TABLE IF EXISTS source_publications;
--   DROP INDEX IF EXISTS nodes_graph_promoted_from_unique;
--   ALTER TABLE nodes DROP COLUMN IF EXISTS sync_mode;
--   ALTER TABLE nodes DROP COLUMN IF EXISTS promoted_from_graph_id;
--   ALTER TABLE nodes DROP COLUMN IF EXISTS promoted_from_node_id;

ALTER TABLE nodes
  ADD COLUMN IF NOT EXISTS promoted_from_node_id  UUID,   -- bare, no FK (see header)
  ADD COLUMN IF NOT EXISTS promoted_from_graph_id UUID,   -- bare, no FK (see header)
  ADD COLUMN IF NOT EXISTS sync_mode TEXT NOT NULL DEFAULT 'static'
    CHECK (sync_mode IN ('static', 'live'));

-- Backfill from the one place same-pool identity already durably lives.
-- No-op on a fresh/empty `nodes` table; recovers identity for every
-- pre-existing same-pool promoted copy on an appended-to database.
--
-- DETERMINISTIC PICK when two sources collide onto one target row: this is
-- exactly the pre-fix defect this migration's header describes (the OLD
-- title-keyed conflict target let two DIFFERENT `node_promotions.
-- source_node_id`s silently upsert into the SAME `target_node_id`), so a
-- plain `UPDATE ... FROM node_promotions np JOIN nodes src ...` can join
-- MORE THAN ONE `np` row onto a single `target` row. Postgres's `UPDATE ...
-- FROM` does not document which matching FROM-row wins when more than one
-- exists for the same target — appearing to be stable is a plan-dependent
-- accident, not a guarantee, so a bare join here would leave which source
-- "wins" un-reviewable and potentially different across a re-run. The rule
-- picked: earliest `promoted_at` (the FIRST source ever promoted into this
-- row is the best available proxy for "the" origin — a later promotion that
-- collided onto the same row via the title-keyed bug is the anomaly, not the
-- origin), tie-broken by lowest `node_promotions.id` for full determinism
-- even between two rows sharing one `promoted_at` instant.
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

CREATE TABLE IF NOT EXISTS source_publications (
    source_id        TEXT NOT NULL,
    source_graph_id  UUID NOT NULL REFERENCES graphs(id) ON DELETE CASCADE,
    target_graph_id  UUID NOT NULL REFERENCES graphs(id) ON DELETE CASCADE,
    sync_mode        TEXT NOT NULL DEFAULT 'static'
        CHECK (sync_mode IN ('static', 'live')),
    consented_by     UUID REFERENCES users(id) ON DELETE SET NULL,
    consented_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    revoked_at       TIMESTAMPTZ,
    PRIMARY KEY (source_id, source_graph_id, target_graph_id)
);

-- The reconciler-shaped lookup: "every live publication out of this source
-- graph", independent of which target it went to. Not covered by the PK
-- above (which leads with source_id, not source_graph_id).
CREATE INDEX IF NOT EXISTS idx_source_publications_source_graph
    ON source_publications (source_graph_id) WHERE revoked_at IS NULL;

ALTER TABLE source_publications ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS account_isolation ON source_publications;

-- Read: permissive OR — visible from either side, same as node_promotions.
CREATE POLICY account_isolation_select ON source_publications
    FOR SELECT
    USING (
        source_graph_id IN (
            SELECT id FROM graphs WHERE account_id = current_setting('app.account_id')::uuid
        )
        OR target_graph_id IN (
            SELECT id FROM graphs WHERE account_id = current_setting('app.account_id')::uuid
        )
    );

-- Write (INSERT): strict AND — the acting account must own BOTH the source
-- graph and the target graph, matching every legitimate publish/revoke call.
CREATE POLICY account_isolation_insert ON source_publications
    FOR INSERT
    WITH CHECK (
        source_graph_id IN (
            SELECT id FROM graphs WHERE account_id = current_setting('app.account_id')::uuid
        )
        AND target_graph_id IN (
            SELECT id FROM graphs WHERE account_id = current_setting('app.account_id')::uuid
        )
    );

-- Write (UPDATE — revoke, sync_mode changes): USING permissive (OR) so an
-- existing row is reachable from either recorded anchor; WITH CHECK strict
-- (AND) so it can never be left pointing cross-account after the update.
CREATE POLICY account_isolation_update ON source_publications
    FOR UPDATE
    USING (
        source_graph_id IN (
            SELECT id FROM graphs WHERE account_id = current_setting('app.account_id')::uuid
        )
        OR target_graph_id IN (
            SELECT id FROM graphs WHERE account_id = current_setting('app.account_id')::uuid
        )
    )
    WITH CHECK (
        source_graph_id IN (
            SELECT id FROM graphs WHERE account_id = current_setting('app.account_id')::uuid
        )
        AND target_graph_id IN (
            SELECT id FROM graphs WHERE account_id = current_setting('app.account_id')::uuid
        )
    );

CREATE POLICY account_isolation_delete ON source_publications
    FOR DELETE
    USING (
        source_graph_id IN (
            SELECT id FROM graphs WHERE account_id = current_setting('app.account_id')::uuid
        )
        AND target_graph_id IN (
            SELECT id FROM graphs WHERE account_id = current_setting('app.account_id')::uuid
        )
    );
