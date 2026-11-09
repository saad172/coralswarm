-- Migration: 0135_expert_in_purge
-- Description: One-shot purge of the machine-generated `expert_in` corpus.
--
--   WHY. `infer_expertise` produced a queue no human will ever work: as of
--   2026-08-08 prod held 2,120 `expert_in` edges — 1,238 `suggested`, 875
--   `confirmed`, 7 `rejected` — and MEASUREMENT (not deduction) showed 864 of
--   the 875 confirmed rows were machine-confirmed, via three paths:
--     700  provenance.auto_resolution absent  — the ungated >=0.85 auto-confirm
--                                               in workers/infer_expertise.rs
--     121  'backlog_fastforward'              — migration 0130/0037
--      43  'deadline_confirm'                 — the hourly review_deadline sweep
--   Only 11 confirmed rows are NOT machine-confirmed. Nothing in the product
--   surfaces expertise, but `expert_in` IS traversable (it sits above
--   `TRAVERSABLE_MIN_CONFIDENCE`, see docs/plans/graph-temporal-memory.md
--   §5.3), so every one of these rows shapes ask answers today. Left alone,
--   `review_deadline`'s 7-day sweep lean-CONFIRMS the >=0.70 share into the
--   graph as fact — inaction is a write, and the oldest queued_at
--   (2026-08-07 17:35:55Z) puts that deadline at 2026-08-14 17:35Z.
--
--   WHAT THIS DOES NOT DO. This clears the accumulated corpus; it does not fix
--   the producer. The impact gate in `review_policy::any_high_impact` is
--   ANY-of-endpoints, and the person nodes in this queue carry degree 5,465 /
--   538 / 512 / 399 (four unmerged variants of one human) against
--   IMPACT_DEGREE = 5, so the gate is effectively constant-true and the queue
--   will refill from new ingest. The permanent invariants are deliberately a
--   separate change; this one is safe to ship on its own and is what
--   decouples the 08-14 deadline from that design debate.
--
--   LEDGER SEED KEYS ON THE TOPIC, NOT THE PERSON. `evaluation_ledger` is the
--   burn guard: a deleted edge leaves the graph blank, and without an anchor
--   the topic is re-evaluated (re-burning an LLM call) every cycle forever.
--   The subject MUST match what the producing gate READS. For `expert_in`
--   that is the TOPIC — the edge's DST — because `infer_expertise` plans and
--   rotates per topic (`expert_in_last_reviewed` keys `dst_node_id`; the
--   ledger is merged into `last_reviewed` as MAX per topic). Keying by src
--   (the person) would leave every blanked topic unanchored. Note this
--   differs from control's 0128, which seeded `src_node_id` — correct for the
--   composed predicates it targeted, wrong for `expert_in`; the same asymmetry
--   is encoded in `store/mod.rs`'s deadline-sweep `ledger_subject`.
--
--   DISTINCT ON IS LOAD-BEARING. Keying by topic means several person-variants
--   claiming one topic collapse to one ledger key. On the live corpus 2,102
--   purged rows span 2,094 distinct topics — 8 collisions — and without the
--   de-duplication the seed aborts the whole migration with "ON CONFLICT DO
--   UPDATE command cannot affect row a second time". Highest confidence wins,
--   which is both deterministic and the strongest suppression.
--
--   PRESERVED. `rejected` rows (durable human negative constraints, ADR-0008)
--   and confirmed rows WITHOUT `provenance.auto_confirmed = 'true'` — i.e.
--   anything a human decided. `origin = 'stated'` is filtered too, though the
--   live corpus has no stated `expert_in` rows.
--
--   RLS. Both migration trees run under `MIGRATOR_DATABASE_URL`, the
--   owner-role DSN (`coralswarm_backend`); `relationship_edge` is owned by
--   that role and does NOT set FORCE ROW LEVEL SECURITY, so the owner bypasses
--   the `account_isolation` policy and these bare statements see every graph.
--   Same posture as 0128/0130/0131.
--
--   STRAGGLERS. `release.sh` applies migrations BEFORE the old machines stop
--   serving, so old-binary workers can write a handful of rows after the
--   DELETE lands. Re-run the DELETE by hand ~1h post-deploy — and run it once
--   per tenant DB as well as the control plane, not control-only.
--
-- Depends on: 0126_evaluation_ledger, 0040_relationship_edges, 0004_jobs
-- Down:
--   -- Irreversible (rows deleted); the ledger seed preserves the verdicts.
--   -- A pre-change snapshot of every expert_in row was taken to
--   -- ~/Desktop/coralswarm-backups/ before this migration was authored.

WITH purged AS (
    SELECT edge_id, graph_id, dst_node_id, predicate, confidence
      FROM relationship_edge
     WHERE predicate = 'expert_in'
       AND origin    = 'inferred'
       AND (
                status = 'suggested'
             OR (status = 'confirmed' AND provenance->>'auto_confirmed' = 'true')
           )
),
-- One row per (graph, topic) — see "DISTINCT ON IS LOAD-BEARING" above.
seed AS (
    SELECT DISTINCT ON (graph_id, dst_node_id)
           graph_id, dst_node_id, predicate, confidence
      FROM purged
     ORDER BY graph_id, dst_node_id, confidence DESC
),
seeded AS (
    INSERT INTO evaluation_ledger (graph_id, subject_node_id, predicate, evaluated_at, confidence)
    SELECT graph_id, dst_node_id, predicate, now(), confidence FROM seed
    ON CONFLICT (graph_id, subject_node_id, predicate)
    DO UPDATE SET evaluated_at = GREATEST(evaluation_ledger.evaluated_at, EXCLUDED.evaluated_at),
                  confidence   = EXCLUDED.confidence
    RETURNING 1
)
DELETE FROM relationship_edge
 WHERE edge_id IN (SELECT edge_id FROM purged);

-- Drop queued expertise work so the drained corpus is not immediately rebuilt
-- by jobs that were already enqueued against the pre-purge graph. Only
-- `pending` rows: in-flight and finished rows are history, not future writes.
DELETE FROM jobs
 WHERE kind = 'InferExpertise'
   AND status = 'pending';
