-- PAIRED WITH: control 0350 (digest_coverage).
-- Reef digests for every entity reef with 3 or more corals (COR-66 bound:
-- a digest on at least 80% of them). Paired tenant/control migrations with
-- byte-identical bodies; only the header names the pair.
--
-- 1. The regeneration trigger's eligibility bar drops from 8 corals to 3,
--    and a reef that has never had a digest (digest_coral_count = 0) is
--    always due: with the bar at 3, growth alone (+5) would never fire a
--    first digest. Twin of workers::digest::{digest_eligible,
--    should_regenerate_digest}. The job now carries the graph's account
--    (NULL when the writer cannot see the graph, as before), so the
--    worker's priority lane can order it on the consumer plane too.
-- 2. One pass is queued for every eligible reef without an attributed
--    digest: never digested, or digested before managed enrichment (#1352),
--    which the reader never serves and the counters call fresh. Deduped on
--    the trigger's own key; the worker claims them most corals first and
--    spends inside its daily dollar bound
--    (DIGEST_DAILY_USD_PER_ACCOUNT).
--
-- Down: re-run the function body of tenant 0020 / control 0109; the queued
-- passes are ordinary jobs and need no undo.

CREATE OR REPLACE FUNCTION nodes_maybe_enqueue_digest_regeneration() RETURNS trigger AS $$
DECLARE
    v_renamed boolean;
    v_growth  boolean;
BEGIN
    IF NEW.type <> 'entity' OR NEW.coral_count < 3 THEN
        RETURN NEW;
    END IF;

    v_renamed := OLD.title IS DISTINCT FROM NEW.title;
    v_growth  := NEW.digest_coral_count <= 0
                 OR (NEW.coral_count - NEW.digest_coral_count)
                    >= GREATEST(5, CEIL(NEW.digest_coral_count * 0.20)::int);

    IF v_renamed OR v_growth THEN
        INSERT INTO jobs (kind, payload, idempotency_key, run_after, account_id)
        VALUES (
            'RegenerateDigest',
            jsonb_build_object('node_id', NEW.node_id, 'graph_id', NEW.graph_id),
            'digest:' || NEW.node_id,
            now(),
            (SELECT g.account_id FROM graphs g WHERE g.id = NEW.graph_id)
        )
        ON CONFLICT (idempotency_key) WHERE status = 'pending' DO NOTHING;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

INSERT INTO jobs (kind, payload, idempotency_key, run_after, account_id)
SELECT 'RegenerateDigest',
       jsonb_build_object('node_id', n.node_id, 'graph_id', n.graph_id),
       'digest:' || n.node_id,
       now(),
       g.account_id
  FROM nodes n
  JOIN graphs g ON g.id = n.graph_id
 WHERE n.type = 'entity'
   AND n.coral_count >= 3
   AND (n.body->'digest'->>'managed_receipt') IS NULL
ON CONFLICT (idempotency_key) WHERE status = 'pending' DO NOTHING;
