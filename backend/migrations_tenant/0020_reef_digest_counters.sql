-- 0020_reef_digest_counters.sql
--
-- TENANT-TREE PAIR of control migration `0109_reef_digest_counters`.
-- See that file's header for the full rationale (write-time coral counters,
-- why they are backfilled rather than landed at DEFAULT 0, and why both the
-- counter maintenance and the digest-regeneration enqueue are triggers
-- rather than N hand-found call-site edits).
--
-- `nodes`, `node_atoms` and `jobs` are all SHARED tenant tables (none is in
-- `check_schema_parity.sh`'s CONTROL_TABLES roster), so this migration's
-- schema changes, and BOTH trigger functions, must stay byte-identical to
-- their control-tree twins or the parity check fails (same discipline
-- `migrations_tenant/0016`'s header documents for the bootstrap resolvers).
--
-- Down:
--   DROP TRIGGER IF EXISTS trg_nodes_digest_regeneration ON nodes;
--   DROP FUNCTION IF EXISTS nodes_maybe_enqueue_digest_regeneration();
--   DROP TRIGGER IF EXISTS trg_node_atoms_coral_count ON node_atoms;
--   DROP FUNCTION IF EXISTS node_atoms_bump_coral_count();
--   ALTER TABLE nodes DROP COLUMN IF EXISTS digest_coral_count;
--   ALTER TABLE nodes DROP COLUMN IF EXISTS coral_count;

ALTER TABLE nodes
  ADD COLUMN IF NOT EXISTS coral_count        INT NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS digest_coral_count INT NOT NULL DEFAULT 0;

-- ── Counter maintenance trigger (every `node_atoms` INSERT/DELETE, direct or
-- cascaded) ──────────────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION node_atoms_bump_coral_count() RETURNS trigger AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        UPDATE nodes SET coral_count = coral_count + 1 WHERE node_id = NEW.node_id;
        RETURN NEW;
    ELSIF TG_OP = 'DELETE' THEN
        -- GREATEST guards against ever surfacing a negative count if some
        -- future path double-decrements; a well-behaved caller never drives
        -- it below zero.
        UPDATE nodes SET coral_count = GREATEST(coral_count - 1, 0) WHERE node_id = OLD.node_id;
        RETURN OLD;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_node_atoms_coral_count ON node_atoms;
CREATE TRIGGER trg_node_atoms_coral_count
    AFTER INSERT OR DELETE ON node_atoms
    FOR EACH ROW
    EXECUTE FUNCTION node_atoms_bump_coral_count();

-- ── Digest-regeneration enqueue trigger (§2.3 eligibility/cadence) ──────────
CREATE OR REPLACE FUNCTION nodes_maybe_enqueue_digest_regeneration() RETURNS trigger AS $$
DECLARE
    v_renamed boolean;
    v_growth  boolean;
BEGIN
    -- Eligibility (§2.3): only entity reefs of a meaningful size. Decision
    -- (and thread/reference) reefs are skipped entirely — see the plan's
    -- §2.3 rationale (the title IS the statement).
    IF NEW.type <> 'entity' OR NEW.coral_count < 8 THEN
        RETURN NEW;
    END IF;

    v_renamed := OLD.title IS DISTINCT FROM NEW.title;
    v_growth  := (NEW.coral_count - NEW.digest_coral_count)
                   >= GREATEST(5, CEIL(NEW.digest_coral_count * 0.20)::int);

    IF v_renamed OR v_growth THEN
        INSERT INTO jobs (kind, payload, idempotency_key, run_after)
        VALUES (
            'RegenerateDigest',
            jsonb_build_object('node_id', NEW.node_id, 'graph_id', NEW.graph_id),
            'digest:' || NEW.node_id,
            now()
        )
        ON CONFLICT (idempotency_key) WHERE status = 'pending' DO NOTHING;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_nodes_digest_regeneration ON nodes;
CREATE TRIGGER trg_nodes_digest_regeneration
    AFTER UPDATE OF coral_count, title ON nodes
    FOR EACH ROW
    WHEN (NEW.coral_count IS DISTINCT FROM OLD.coral_count
          OR NEW.title IS DISTINCT FROM OLD.title)
    EXECUTE FUNCTION nodes_maybe_enqueue_digest_regeneration();

-- ── Backfill (see control twin's header: NOT optional on an already-
-- populated database) ───────────────────────────────────────────────────────
--
-- Deliberately AFTER `trg_nodes_digest_regeneration` exists — see the control
-- twin's header for why: this UPDATE is itself a real `coral_count` change
-- on every already-eligible existing reef, and running it before the trigger
-- existed would silently skip enqueueing a first digest job for every
-- pre-existing reef that backfills straight past the `>= 8` eligibility bar.
UPDATE nodes n
SET coral_count = counts.cnt
FROM (
    SELECT node_id, COUNT(*)::int AS cnt
    FROM node_atoms
    GROUP BY node_id
) counts
WHERE n.node_id = counts.node_id
  AND n.coral_count <> counts.cnt;
