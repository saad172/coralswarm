-- Migration: 0107_nodes_case_insensitive_identity
-- Description: Workstream E §6.2 (docs/plans/graph-temporal-memory.md) — reef
--              identity becomes case-insensitive: UNIQUE (graph_id, type,
--              lower(title)). Live data already violates this (D10: "CoralSwarm"
--              / 951 corals and "coralswarm" / 344 corals coexist as two
--              separate entity reefs holding the same subject), so the new
--              unique index cannot simply be created — every case-variant
--              collision group must be folded to one surviving reef FIRST.
--
--              This is, in effect, a bulk `merge_nodes` (see `store/mod.rs`)
--              run directly in SQL against every (graph_id, type,
--              lower(title)) group with more than one member. It re-points
--              the SAME set of references `merge_nodes` re-points —
--              `node_atoms`, `relationship_edge`, `supersession_edge`,
--              `node_promotions`, `graphs.self_node_id`, and `node_merges.
--              keep_id` (a bare, non-FK reference that could itself name a
--              fold loser) — teaches the survivor the loser's lowercased
--              title (plus any aliases the loser itself had already learned)
--              as an alias, matching migration 0048's alias-teaching and the
--              runtime resolve step in `Store::upsert_node_and_link_atom`,
--              then deletes the loser rows and only THEN creates the
--              case-insensitive unique index.
--
--              `non_supersession_pair` is included for completeness even
--              though D3 (docs/plans §1) records it as never wired (no
--              reader/writer exists yet) — belt and suspenders in case any row
--              exists, so this migration handles the identical reference set
--              `merge_nodes` does rather than a subset of it.
--
--              `dup_id` on existing `node_merges` rows is deliberately left
--              alone (only `keep_id` is re-pointed): by that table's own
--              design (migration 0048's header) a `dup_id` already names a
--              node that no longer exists — its OWN merge deleted it. That is
--              expected of every `node_merges` row, not something this
--              migration introduces, and re-pointing it would misrepresent
--              which node was actually snapshotted by that historical merge.
--
--              SURVIVOR RULE: the reef with the MOST linked corals wins
--              (highest COUNT(node_atoms) for that node_id); ties broken by
--              earliest created_at, then by node_id for total determinism.
--              This differs from the RUNTIME resolve step's "first casing
--              wins" (`upsert_node_and_link_atom` — the running process
--              prefers whichever casing already exists, one write at a time,
--              because it is resolving forward from a live, in-order stream).
--              For EXISTING data "first extracted" is not a meaningful signal
--              — it is an accident of which casing an early atom happened to
--              use — whereas coral count IS the signal that matters: it is the
--              reef with the most accumulated evidence, and (most likely) the
--              greater share of prior `relationship_edge`/`supersession_edge`
--              traffic, so keeping ITS node_id minimizes the number of live
--              references that need to move. On the measured D10 collision
--              ("CoralSwarm" 951 corals vs "coralswarm" 344), this rule keeps
--              "CoralSwarm" — the larger, more heavily-referenced reef.
--
--              NOT emitted: `op_log` sync ops for the fold. This migration
--              writes directly to the tables the `Store` trait normally
--              mediates, bypassing the sync-op construction in `store::sync`
--              entirely — the same choice migration 0094's cross-ocean
--              supersession cleanup made for its deletes.
--
--              OBSERVABLE CONSEQUENCE for a desktop replica that already
--              pulled both case variants down BEFORE this migration ran (its
--              local libsql copy holds both node rows, exactly like the
--              control-plane database did pre-fold):
--                - It keeps showing BOTH reefs locally — the fold does not
--                  reach it via sync, ever, on its own. `sync_cursors`
--                  advances normally on its next pull, but a cursor-based pull
--                  only replays ops APPENDED after that cursor; there is no
--                  "this graph was bulk-modified, go re-sync it" signal for a
--                  cursor to catch.
--                - Any FUTURE write to the loser's former `node_id` from the
--                  control plane (there won't be any — nothing writes it
--                  again) would be a moot point; the real risk is the
--                  INVERSE — a desktop-originated push that still references
--                  the (now-deleted) loser `node_id` (e.g. a locally-linked
--                  atom, an edge the device created before its last pull)
--                  will fail its FK on the server, surfacing as a push
--                  rejection for that op, not a silent loss server-side.
--                - The two reefs do NOT self-heal without help. Recovery
--                  requires either a full desktop resync (if/when that
--                  capability exists) or a manual per-device intervention;
--                  neither exists today.
--              Full op_log replication for a bulk historical fold is a
--              separate, larger design question this migration does not
--              attempt to solve — flagging the consequence here rather than
--              leaving it undiscoverable.
--
--              PAIRED WITH: migrations_tenant/0018_nodes_case_insensitive_identity.sql
--              — identical DDL/DML (`scripts/check_schema_parity.sh` diffs a
--              normalized pg_dump of both trees; the DML touches only tables
--              common to both). The FOLD affects whatever collisions exist in
--              each database independently; the SCHEMA change (the new
--              unique index replacing the case-sensitive one) is identical.
--
--              Idempotent and safe to re-run: every statement re-derives its
--              target set from CURRENT data (no hardcoded ids). A second run
--              after a successful first run finds zero collision groups (the
--              temp tables are empty) and the index already exists
--              (`CREATE UNIQUE INDEX IF NOT EXISTS`) — a true no-op.
--
-- Depends on: 0011_nodes (nodes, node_atoms, supersession_edge,
--             non_supersession_pair), 0017_nodes_unique_constraint (the
--             constraint this REPLACES), 0026_ocean_model (node_promotions),
--             0048_node_merges (node_merges), 0049_connector_identity_self_reef
--             (graphs.self_node_id), 0094_supersession_edge_graph_id
--             (supersession_edge.graph_id — this migration must populate it
--             on any row it re-points/re-inserts)
-- Down:
--   DROP INDEX IF EXISTS nodes_graph_type_lower_title_unique;
--   ALTER TABLE nodes ADD CONSTRAINT nodes_graph_type_title_unique
--       UNIQUE (graph_id, type, title);
--   (the fold itself is NOT reversed by the down migration — the losing
--    nodes are gone. `node_case_fold_log`, left in place, records exactly
--    what was folded so a wrong fold can be reconstructed by hand from a
--    backup; it is not a programmatic undo like `node_merges`/`unmerge_nodes`
--    — see the migration header for why.)

-- ── 0. Forensic ledger — written before anything is folded ────────────────────
-- Deliberately carries NO foreign keys (same reasoning as
-- `supersession_edge_cross_ocean_deletions`, migration 0094): a fold is
-- exactly the kind of operation whose own evidence must outlive the rows it
-- describes, and an FK to the now-deleted loser would defeat that.
CREATE TABLE IF NOT EXISTS node_case_fold_log (
    id                    UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    graph_id              UUID        NOT NULL,
    node_type             TEXT        NOT NULL,
    survivor_id           UUID        NOT NULL,
    survivor_title        TEXT        NOT NULL,
    survivor_coral_count  INTEGER     NOT NULL,
    loser_id              UUID        NOT NULL,
    loser_title           TEXT        NOT NULL,
    loser_coral_count     INTEGER     NOT NULL,
    folded_at             TIMESTAMPTZ NOT NULL DEFAULT now(),
    folded_by             TEXT        NOT NULL
);

-- Forensic-only — same REVOKE, same reasoning, as the tenant-tree twin
-- (`migrations_tenant/0018_nodes_case_insensitive_identity.sql`) and as
-- 0083's own `escrow_shares`/`_sqlx_migrations` precedent: 0083's
-- `ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT, INSERT, UPDATE,
-- DELETE ON TABLES TO coralswarm_app` applies to every table created after
-- it, including this one, and no per-row `account_isolation` predicate
-- cleanly scopes a fold record the way it does ordinary content (a fold
-- names two ALREADY-COLLAPSED node ids, one of which no longer exists to
-- resolve an account from). `coralswarm_app` already exists by this point
-- (created in 0083, which this migration depends on).
REVOKE ALL ON node_case_fold_log FROM coralswarm_app;

-- ── 1. Compute this run's collision groups + survivor/loser map ───────────────
-- Session-scoped TEMP TABLEs: sqlx sends a migration file's SQL as one
-- wire-protocol Query message, so every statement below runs in the SAME
-- session/transaction (migration 0099's header records the same fact, applied
-- there to why `CREATE INDEX CONCURRENTLY` can't share a file) — the temp
-- tables persist across statements within this file and are dropped
-- automatically when the transaction ends, so nothing leaks between runs.
CREATE TEMP TABLE IF NOT EXISTS node_case_fold_candidates AS
SELECT
    n.node_id,
    n.graph_id,
    n.type,
    n.title,
    lower(n.title) AS lower_title,
    n.created_at,
    COALESCE(na.coral_count, 0) AS coral_count
FROM nodes n
LEFT JOIN (
    SELECT node_id, count(*) AS coral_count FROM node_atoms GROUP BY node_id
) na ON na.node_id = n.node_id;

CREATE TEMP TABLE IF NOT EXISTS node_case_fold_ranked AS
SELECT
    c.*,
    row_number() OVER (
        PARTITION BY c.graph_id, c.type, c.lower_title
        ORDER BY c.coral_count DESC, c.created_at ASC, c.node_id ASC
    ) AS rn,
    count(*) OVER (PARTITION BY c.graph_id, c.type, c.lower_title) AS group_size
FROM node_case_fold_candidates c;

-- One row per LOSER, joined to its group's survivor (rn = 1). A group with
-- group_size = 1 never produces a loser row (rn > 1 is impossible for it), so
-- this table is naturally empty when there is nothing to fold. Every node_id
-- belongs to exactly one (graph_id, type, lower_title) group, so a node can
-- never be a survivor for one group and a loser for another simultaneously.
CREATE TEMP TABLE IF NOT EXISTS node_case_fold_map AS
SELECT
    l.node_id     AS loser_id,
    l.graph_id    AS graph_id,
    l.type        AS node_type,
    l.title       AS loser_title,
    l.coral_count AS loser_coral_count,
    s.node_id     AS survivor_id,
    s.title       AS survivor_title,
    s.coral_count AS survivor_coral_count
FROM node_case_fold_ranked l
JOIN node_case_fold_ranked s
  ON s.graph_id = l.graph_id AND s.type = l.type AND s.lower_title = l.lower_title AND s.rn = 1
WHERE l.rn > 1;

-- ── 2. Forensic ledger row per fold, before any repointing ────────────────────
INSERT INTO node_case_fold_log
    (graph_id, node_type, survivor_id, survivor_title, survivor_coral_count,
     loser_id, loser_title, loser_coral_count, folded_by)
SELECT graph_id, node_type, survivor_id, survivor_title, survivor_coral_count,
       loser_id, loser_title, loser_coral_count,
       'nodes_case_insensitive_identity (control 0107 / tenant 0017)'
FROM node_case_fold_map;

-- ── 3. Re-point node_atoms (dedup on the survivor) ────────────────────────────
-- No self-loop/collision concept for this table — the PK (node_id, atom_id)
-- and ON CONFLICT DO NOTHING already give exactly the dedup `merge_nodes`
-- wants (a coral already linked to the survivor is simply not re-linked).
INSERT INTO node_atoms (node_id, atom_id)
SELECT m.survivor_id, na.atom_id
FROM node_atoms na
JOIN node_case_fold_map m ON m.loser_id = na.node_id
ON CONFLICT DO NOTHING;

DELETE FROM node_atoms na
USING node_case_fold_map m
WHERE na.node_id = m.loser_id;

-- ── 4. Per-fold repoint of everything that can self-loop or collide ──────────
-- `relationship_edge`/`supersession_edge`/`node_promotions`/
-- `non_supersession_pair` all need the SAME "drop on self-loop after remap,
-- drop on unique-collision with what the survivor already has" discipline
-- `merge_nodes` applies per-edge in Rust. A per-loser procedural loop mirrors
-- that logic directly (each iteration reads the CURRENT state of the tables,
-- so multi-way collisions — more than one loser folding onto the same
-- survivor — are handled correctly one loser at a time) rather than
-- attempting an equivalent bulk-SQL formulation whose self-loop/collision
-- interactions across multiple simultaneous remaps would be substantially
-- harder to get right.
DO $$
DECLARE
    fold RECORD;
    edge RECORD;
    v_new_src UUID;
    v_new_dst UUID;
    v_new_er UUID;
    v_new_ed UUID;
    v_current_body JSONB;
    v_new_aliases JSONB;
BEGIN
    FOR fold IN SELECT * FROM node_case_fold_map ORDER BY graph_id, node_type, survivor_id, loser_id
    LOOP
        -- ── relationship_edge: re-point src/dst; drop self-loops (both
        -- endpoints resolve to the survivor) and drop on a
        -- `relationship_edge_unique` collision (the survivor already has this
        -- exact triple) rather than erroring — mirrors `merge_nodes` exactly,
        -- including the correlated NOT EXISTS collision check.
        FOR edge IN
            SELECT edge_id, src_node_id, dst_node_id
            FROM relationship_edge
            WHERE src_node_id = fold.loser_id OR dst_node_id = fold.loser_id
        LOOP
            v_new_src := CASE WHEN edge.src_node_id = fold.loser_id THEN fold.survivor_id ELSE edge.src_node_id END;
            v_new_dst := CASE WHEN edge.dst_node_id = fold.loser_id THEN fold.survivor_id ELSE edge.dst_node_id END;
            IF v_new_src = v_new_dst THEN
                DELETE FROM relationship_edge WHERE edge_id = edge.edge_id;
            ELSE
                UPDATE relationship_edge
                   SET src_node_id = v_new_src, dst_node_id = v_new_dst,
                       updated_at = now(), version = version + 1
                 WHERE edge_id = edge.edge_id
                   AND NOT EXISTS (
                       SELECT 1 FROM relationship_edge e2
                        WHERE e2.graph_id = relationship_edge.graph_id
                          AND e2.src_node_id = v_new_src
                          AND e2.predicate = relationship_edge.predicate
                          AND e2.dst_node_id = v_new_dst
                          AND e2.edge_id <> relationship_edge.edge_id
                   );
                IF NOT FOUND THEN
                    -- Collision: the survivor (or an already-repointed sibling
                    -- edge this same loop) already has this exact triple.
                    DELETE FROM relationship_edge WHERE edge_id = edge.edge_id;
                END IF;
            END IF;
        END LOOP;

        -- ── supersession_edge: re-point superseder/superseded; drop
        -- self-loops, drop on PK collision (ON CONFLICT DO NOTHING) — mirrors
        -- `merge_nodes`. `graph_id` on the re-inserted row is the fold's own
        -- `graph_id` (both endpoints are, by construction, in the same
        -- ocean — the fold never crosses graphs).
        FOR edge IN
            SELECT superseder_id, superseded_id, source, confirmed_by, confirmed_at
            FROM supersession_edge
            WHERE superseder_id = fold.loser_id OR superseded_id = fold.loser_id
        LOOP
            v_new_er := CASE WHEN edge.superseder_id = fold.loser_id THEN fold.survivor_id ELSE edge.superseder_id END;
            v_new_ed := CASE WHEN edge.superseded_id = fold.loser_id THEN fold.survivor_id ELSE edge.superseded_id END;
            DELETE FROM supersession_edge
             WHERE superseder_id = edge.superseder_id AND superseded_id = edge.superseded_id;
            IF v_new_er <> v_new_ed THEN
                INSERT INTO supersession_edge
                    (superseder_id, superseded_id, source, graph_id, confirmed_by, confirmed_at)
                VALUES (v_new_er, v_new_ed, edge.source, fold.graph_id, edge.confirmed_by, edge.confirmed_at)
                ON CONFLICT DO NOTHING;
            END IF;
        END LOOP;

        -- ── node_promotions: re-point source/target; drop on the
        -- (source_node_id, target_graph_id) unique collision — mirrors
        -- `merge_nodes`.
        FOR edge IN
            SELECT id, source_node_id, target_graph_id, target_node_id
            FROM node_promotions
            WHERE source_node_id = fold.loser_id OR target_node_id = fold.loser_id
        LOOP
            v_new_src := CASE WHEN edge.source_node_id = fold.loser_id THEN fold.survivor_id ELSE edge.source_node_id END;
            v_new_dst := CASE WHEN edge.target_node_id = fold.loser_id THEN fold.survivor_id ELSE edge.target_node_id END;
            IF EXISTS (
                SELECT 1 FROM node_promotions
                 WHERE source_node_id = v_new_src AND target_graph_id = edge.target_graph_id AND id <> edge.id
            ) THEN
                DELETE FROM node_promotions WHERE id = edge.id;
            ELSE
                UPDATE node_promotions SET source_node_id = v_new_src, target_node_id = v_new_dst WHERE id = edge.id;
            END IF;
        END LOOP;

        -- ── non_supersession_pair: re-point, re-canonicalize to
        -- (LEAST, GREATEST) per its own ordering CHECK, drop self-loops and
        -- collisions. D3 (docs/plans §1) records this table as never wired —
        -- expected empty in practice; handled anyway for completeness.
        FOR edge IN
            SELECT node_a_id, node_b_id, marked_by, marked_at
            FROM non_supersession_pair
            WHERE node_a_id = fold.loser_id OR node_b_id = fold.loser_id
        LOOP
            v_new_src := CASE WHEN edge.node_a_id = fold.loser_id THEN fold.survivor_id ELSE edge.node_a_id END;
            v_new_dst := CASE WHEN edge.node_b_id = fold.loser_id THEN fold.survivor_id ELSE edge.node_b_id END;
            DELETE FROM non_supersession_pair
             WHERE node_a_id = edge.node_a_id AND node_b_id = edge.node_b_id;
            IF v_new_src <> v_new_dst THEN
                INSERT INTO non_supersession_pair (node_a_id, node_b_id, marked_by, marked_at)
                VALUES (LEAST(v_new_src, v_new_dst), GREATEST(v_new_src, v_new_dst), edge.marked_by, edge.marked_at)
                ON CONFLICT DO NOTHING;
            END IF;
        END LOOP;

        -- ── graphs.self_node_id: re-point if it pointed at the loser (#135's
        -- concern in `merge_nodes` applies identically here) ─────────────────
        UPDATE graphs SET self_node_id = fold.survivor_id WHERE self_node_id = fold.loser_id;

        -- ── node_merges.keep_id: re-point a bare (non-FK) historical
        -- reference to a node that is itself a fold loser. `dup_id` is
        -- deliberately left alone — see the migration header.
        UPDATE node_merges SET keep_id = fold.survivor_id WHERE keep_id = fold.loser_id;

        -- ── Teach the survivor the loser's spelling (+ the loser's own
        -- already-learned aliases, so they are not lost) as an alias ─────────
        SELECT body INTO v_current_body FROM nodes WHERE node_id = fold.survivor_id;
        SELECT coalesce(jsonb_agg(DISTINCT e), '[]'::jsonb)
          INTO v_new_aliases
          FROM jsonb_array_elements_text(
                 coalesce(v_current_body -> 'aliases', '[]'::jsonb)
                 || to_jsonb(ARRAY[lower(fold.loser_title)])
                 || coalesce((SELECT body -> 'aliases' FROM nodes WHERE node_id = fold.loser_id), '[]'::jsonb)
               ) e
         WHERE e <> lower(fold.survivor_title);

        UPDATE nodes
           SET body = jsonb_set(coalesce(body, '{}'::jsonb), '{aliases}', v_new_aliases),
               updated_at = now(),
               version = version + 1
         WHERE node_id = fold.survivor_id;

        -- ── Delete the loser. Any node_atoms/relationship_edge/
        -- supersession_edge/non_supersession_pair row this loop somehow
        -- missed cascades away here (ON DELETE CASCADE on every one of those
        -- FKs) — none should remain; this is belt-and-suspenders, not the
        -- primary mechanism (the primary mechanism re-points with full
        -- provenance, a bare cascade would silently destroy it).
        DELETE FROM nodes WHERE node_id = fold.loser_id;
    END LOOP;
END $$;

-- ── 5. Identity becomes case-insensitive ──────────────────────────────────────
-- Only reachable once every collision above has been folded away — a
-- collision still present here aborts the migration loudly rather than
-- silently skipping the index (there is no `IF NOT EXISTS`-equivalent escape
-- hatch for "the data still violates this", and there should not be one).
ALTER TABLE nodes DROP CONSTRAINT IF EXISTS nodes_graph_type_title_unique;
CREATE UNIQUE INDEX IF NOT EXISTS nodes_graph_type_lower_title_unique
    ON nodes (graph_id, type, lower(title));
