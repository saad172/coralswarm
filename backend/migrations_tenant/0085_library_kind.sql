-- TENANT-TREE PAIR of control migration `0217_library_kind`.
-- See that file's header for the rationale. DDL must stay byte-identical
-- (`scripts/check_schema_parity.sh`).
--
-- Down:
--   DROP TRIGGER IF EXISTS trg_nodes_kind_reeval ON nodes;
--   DROP FUNCTION IF EXISTS nodes_maybe_enqueue_kind_reeval();
--   DROP TRIGGER IF EXISTS trg_node_atoms_last_happened ON node_atoms;
--   DROP FUNCTION IF EXISTS node_atoms_bump_last_happened();
--   DROP INDEX IF EXISTS idx_nodes_library;
--   ALTER TABLE nodes DROP COLUMN IF EXISTS kind_eval_coral_count;
--   ALTER TABLE nodes DROP COLUMN IF EXISTS last_happened_at;
--   ALTER TABLE nodes DROP COLUMN IF EXISTS entity_type_sticky;
--   ALTER TABLE nodes DROP COLUMN IF EXISTS entity_type;

ALTER TABLE nodes
  ADD COLUMN IF NOT EXISTS entity_type TEXT
    CHECK (entity_type IS NULL OR entity_type IN ('person', 'org', 'topic', 'project')),
  ADD COLUMN IF NOT EXISTS entity_type_sticky BOOLEAN NOT NULL DEFAULT FALSE,
  ADD COLUMN IF NOT EXISTS last_happened_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS kind_eval_coral_count INT NOT NULL DEFAULT 0;

CREATE INDEX IF NOT EXISTS idx_nodes_library
  ON nodes (graph_id, entity_type, updated_at DESC, node_id)
  WHERE entity_type IS NOT NULL AND coral_count > 0;

CREATE OR REPLACE FUNCTION node_atoms_bump_last_happened() RETURNS trigger AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        UPDATE nodes n
           SET last_happened_at = GREATEST(n.last_happened_at, a.timestamp)
          FROM atoms a
         WHERE a.atom_id = NEW.atom_id
           AND n.node_id = NEW.node_id;
        RETURN NEW;
    ELSIF TG_OP = 'DELETE' THEN
        UPDATE nodes n
           SET last_happened_at = (
                SELECT MAX(a.timestamp)
                  FROM node_atoms na
                  JOIN atoms a ON a.atom_id = na.atom_id
                 WHERE na.node_id = OLD.node_id
           )
         WHERE n.node_id = OLD.node_id;
        RETURN OLD;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_node_atoms_last_happened ON node_atoms;
CREATE TRIGGER trg_node_atoms_last_happened
    AFTER INSERT OR DELETE ON node_atoms
    FOR EACH ROW
    EXECUTE FUNCTION node_atoms_bump_last_happened();

CREATE OR REPLACE FUNCTION nodes_maybe_enqueue_kind_reeval() RETURNS trigger AS $$
BEGIN
    IF NEW.entity_type_sticky THEN
        RETURN NEW;
    END IF;
    IF NEW.coral_count < 100 THEN
        RETURN NEW;
    END IF;
    IF (NEW.coral_count / 100) <= (OLD.coral_count / 100) THEN
        RETURN NEW;
    END IF;
    INSERT INTO jobs (kind, payload, idempotency_key, run_after)
    VALUES (
        'ReevaluateKind',
        jsonb_build_object('node_id', NEW.node_id, 'graph_id', NEW.graph_id),
        'kind:' || NEW.node_id || ':' || (NEW.coral_count / 100),
        now()
    )
    ON CONFLICT (idempotency_key) WHERE status = 'pending' DO NOTHING;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_nodes_kind_reeval ON nodes;
CREATE TRIGGER trg_nodes_kind_reeval
    AFTER UPDATE OF coral_count ON nodes
    FOR EACH ROW
    WHEN (NEW.coral_count IS DISTINCT FROM OLD.coral_count)
    EXECUTE FUNCTION nodes_maybe_enqueue_kind_reeval();

UPDATE nodes
   SET entity_type = body->>'entity_type'
 WHERE entity_type IS NULL
   AND body->>'entity_type' IN ('person', 'org', 'topic', 'project');

UPDATE nodes
   SET entity_type = 'topic'
 WHERE entity_type IS NULL
   AND type = 'entity'
   AND coral_count > 0;

UPDATE nodes n
   SET last_happened_at = src.ts
  FROM (
        SELECT na.node_id, MAX(a.timestamp) AS ts
          FROM node_atoms na
          JOIN atoms a ON a.atom_id = na.atom_id
         GROUP BY na.node_id
       ) src
 WHERE n.node_id = src.node_id
   AND n.last_happened_at IS DISTINCT FROM src.ts;
