-- Migration: 0139_reef_judgement
-- Description: `reef_judgement` — the nomenklatura-style judgement graph for entity
--   resolution (landscape-survey port #1). Each row is one POSITIVE / NEGATIVE / UNSURE
--   verdict about whether two entity reefs are the same real-world thing. An active POSITIVE
--   row between two still-live nodes is "merge pending materialization" (see
--   `workers::entity_resolution`'s read-gate doc, design doc §5). UNSURE rows are literally
--   the review queue.
--
--   This is Phase A of a phased rollout (design doc §3): physical merges
--   (`store::merge_nodes`) remain the materialization mechanism; this table both records WHY
--   a merge happened (or didn't) and drives WHEN one happens, replacing the silent
--   `tracing::debug!` hole at `workers/entity_resolution.rs:644-651`.
--
--   THIS PR (PR1, judgement-graph slice) IS DELIBERATELY INERT: this migration ships the
--   table, its RLS, and the `Store` plumbing (`write_judgement`/`annul_judgement`/
--   `get_active_judgement`/`list_active_judgements_for_node`) with NO caller anywhere in
--   `workers`/`routes` yet. The ER-worker read-gate + rewire (§5.0/§5.2), the undo hook
--   (§5.4), the `/review` surface (§6), and the backfill (§7) are PR2-PR5 — see the design
--   doc's §9 PR list. No behavior changes; no feature flag needed for this PR.
--
--   Review round 1 (post-PR1-open, independent xhigh review, 15/15 findings confirmed)
--   tightened the `Store` layer before any of that lands: `write_judgement`'s "same
--   verdict/source" no-op no longer swallows a genuine re-adjudication's refreshed
--   metadata; `expires_at` is now actually live-checked by every read AND by the write
--   path's supersession logic; `annul_judgement` now takes the SAME pair advisory lock
--   `write_judgement` does (recovering the pair from the row itself) and returns a typed
--   outcome instead of `()`. None of this changed the DDL except the two index edits
--   called out at their own call sites below (findings 9/10) — the shape review round 1
--   flagged (`node_a_id`/`node_b_id` bare UUID, `graph_id`-anchored RLS, the four CHECK
--   constraints) was confirmed correct as designed.
--
-- PAIRED WITH: migrations_tenant/0046_reef_judgement.sql — table shape is IDENTICAL
--   (`scripts/check_schema_parity.sh`) EXCEPT `created_by`/`annulled_by`, which are bare
--   `uuid` (no FK) on the tenant tree — `users`/`accounts` live only in the control DB
--   (`migrations_tenant/0001_tenant_baseline.sql`'s own header: "Cross-DB references ... are
--   bare uuid columns with no FK"). RLS also diverges (see below).
-- Depends on: 0011_nodes (nodes, graphs), 0048_node_merges (node_merges, for the
--   judgement_id back-link), 0083_consumer_rls (RLS pattern), 0112_conflict_dismissal
--   (append/expiry precedent this follows).
-- Down:
--   ALTER TABLE node_merges DROP COLUMN IF EXISTS judgement_id;
--   DROP POLICY IF EXISTS account_isolation ON reef_judgement;
--   ALTER TABLE reef_judgement DISABLE ROW LEVEL SECURITY;
--   DROP TABLE IF EXISTS reef_judgement;
--
-- ─────────────────────────────────────────────────────────────────────────────
-- node_a_id / node_b_id ARE BARE UUID — NO FK TO nodes(node_id) (v2, was C2)
-- ─────────────────────────────────────────────────────────────────────────────
-- v1 of this migration FK'd both endpoints ON DELETE CASCADE, mirroring `reef_identifiers`.
-- That is wrong here: `merge_nodes` hard-deletes the duplicate reef on every successful
-- merge, so a CASCADE would delete the very POSITIVE judgement row that just justified the
-- merge, at the moment it becomes historically most important — and because the
-- `node_merges.judgement_id` back-link (below) is written in the SAME transaction as that
-- delete, the cascade would violate ITS foreign key too, aborting the whole merge. Bare UUID
-- with no FK is the existing, deliberate precedent for exactly this shape: `node_merges`
-- itself (0048) stores `keep_id`/`dup_id` as un-FK'd bare UUIDs for the identical reason
-- ("dup_id is not FK-constrained ... it no longer exists once the merge commits"). Endpoint
-- liveness is therefore a CODE invariant (readers `LEFT JOIN nodes` and treat a miss as "this
-- side no longer exists, show what we have"), not a schema-enforced one. `graph_id` keeps its
-- FK (`ON DELETE CASCADE`) — a whole ocean's judgement history disappearing when the ocean
-- itself is deleted is the one cascade that IS correct here.
--
-- ─────────────────────────────────────────────────────────────────────────────
-- APPEND-ONLY LOG, ONE ACTIVE ROW PER PAIR
-- ─────────────────────────────────────────────────────────────────────────────
-- Rows are never UPDATEd except to stamp `annulled_at`/`annulled_by`/`annul_reason`/
-- `superseded_by`. `uq_reef_judgement_active_pair` below enforces "at most one active row per
-- ordered pair"; design doc §5.1 specifies the concurrency-safe write sequence (advisory
-- lock + annul-then-insert-then-stamp) that keeps this invariant race-free under concurrent
-- same-graph ER jobs. Full history survives every annul — deliberately compatible with a
-- LATER full bitemporal claims table (port #2) without being one itself.
--
-- ─────────────────────────────────────────────────────────────────────────────
-- VERDICT VOCABULARY, SOURCE IS FREE TEXT (not a closed enum)
-- ─────────────────────────────────────────────────────────────────────────────
-- `verdict` is the nomenklatura POSITIVE/NEGATIVE/UNSURE vocabulary (LLM's own
-- `same/different/unsure` is preserved verbatim in `evidence->>'llm_verdict'`); deliberately
-- disjoint from `nodes.body->>'review_status'` (`pending|confirmed|rejected`) and
-- `relationship_edge.status` (`suggested|confirmed|rejected`) so it's never mistaken for
-- either. `source` is `CHECK (source <> '')`, NOT a closed `IN (...)` enum — the
-- `reef_identifiers` (0138) precedent, deliberately: migrations here are append-only, so a
-- closed enum would require a NEW migration every time a new writer class appears (e.g. a
-- future `identifier_recheck` re-check worker, design doc §8). Conventional values in use
-- today: `identifier_auto`, `llm`, `human`, `backfill` — documented here, not enforced here.
--
-- ─────────────────────────────────────────────────────────────────────────────
-- EXPIRY: verdict-gated, query-time only (0112 precedent)
-- ─────────────────────────────────────────────────────────────────────────────
-- POSITIVE/NEGATIVE never expire by time; UNSURE expires because it's a statement about
-- available EVIDENCE, not about the pair. `reef_judgement_expiry_shape` pins this in.
-- "Live" (review round 1, finding 2) = `annulled_at IS NULL AND (expires_at IS NULL OR
-- expires_at > now())`, applied at query time by `Store::get_active_judgement`/
-- `list_active_judgements_for_node` — same `(expires_at IS NULL OR expires_at > now())`
-- shape `conflict_dismissal.is_conflict_dismissed` already uses, not baked into any index
-- (`idx_reef_judgement_review_queue`'s own comment already said this; the write path is
-- what actually enforces it now — see `write_judgement`'s own doc in `store/mod.rs`).
-- `uq_reef_judgement_active_pair` below deliberately does NOT encode expiry (an index
-- predicate cannot reference `now()`), so an expired-but-unannulled row still occupies the
-- unique slot until something annuls it — `write_judgement` does exactly that itself,
-- treating an expired active row (regardless of its `source`, INCLUDING `source='human'`)
-- as unconditionally supersedable rather than a blocker.
--
-- ─────────────────────────────────────────────────────────────────────────────
-- `node_merges.judgement_id`: merge <-> judgement back-link (CREATE precedes ALTER — v2, was C1)
-- ─────────────────────────────────────────────────────────────────────────────
-- v1 of this migration emitted this ALTER TABLE (which REFERENCES reef_judgement) BEFORE the
-- CREATE TABLE below — Postgres would reject the forward reference, aborting the whole
-- migration transaction and, under `release.sh`'s fail-closed `set -eu`, the deploy. Moved
-- below the CREATE. `ON DELETE SET NULL` (not the default NO ACTION/RESTRICT): if a
-- `reef_judgement` row is ever hard-deleted (it shouldn't be, append-only, but this is
-- defense in depth), the merge audit row survives with a NULL back-link rather than blocking
-- the delete or being cascaded away itself.
CREATE TABLE IF NOT EXISTS reef_judgement (
    judgement_id   UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    graph_id       UUID        NOT NULL REFERENCES graphs(id) ON DELETE CASCADE,
    node_a_id      UUID        NOT NULL,  -- bare uuid, no FK — see header.
    node_b_id      UUID        NOT NULL,  -- bare uuid, no FK — see header.

    verdict        TEXT        NOT NULL CHECK (verdict IN ('positive', 'negative', 'unsure')),
    source         TEXT        NOT NULL CHECK (source <> ''),  -- free text — see header.
    confidence     DOUBLE PRECISION,  -- was REAL in v1; matches 0112's conflict_dismissal.confidence
                                       -- type and the f64 the LLM verdict actually carries (m2).
    model_id       TEXT,
    prompt_version TEXT,       -- read by the §5.0 gate on every NEGATIVE(llm) suppression check.

    -- Structured, Splink-ready evidence/provenance. Shape by source:
    --   identifier_auto: {"identifier": {"kind": "email", "value_norm": "...", "shared": true}}
    --   llm:             {"llm_verdict": "same", "reason": "...", "facts_a": [...], "facts_b": [...]}
    --   human:           {"card_id": "...", "prior_judgement_id": "..."}
    --   backfill:        {"backfilled_from": "node_merges", "merge_id": "..."} or
    --                     {"backfilled_from": "not_same"}
    -- Reserves (unused this slice, port #3): a "splink" key for per-feature m/u weights.
    evidence       JSONB       NOT NULL DEFAULT '{}'::jsonb,

    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_by     UUID        REFERENCES users(id) ON DELETE SET NULL,  -- source='human' only.

    expires_at     TIMESTAMPTZ,  -- unsure only.

    annulled_at    TIMESTAMPTZ,  -- un-merge = annul; also stamped when superseded. NULL = active.
    annulled_by    UUID        REFERENCES users(id) ON DELETE SET NULL,  -- source user for human annuls.
    -- Q1 ANSWERED (founder, 2026-08-10): dedicated kind column, added now rather than
    -- retrofitted onto an append-only table later. Set iff annulled.
    annulled_by_kind TEXT     CHECK (annulled_by_kind IN ('human', 'system')),
    annul_reason   TEXT,
    superseded_by  UUID        REFERENCES reef_judgement(judgement_id),

    -- `node_a_id < node_b_id` already implies `node_a_id <> node_b_id` — v1 also had a
    -- separate `reef_judgement_no_self` CHECK here; dropped as dead weight (m1).
    CONSTRAINT reef_judgement_ordered_pair CHECK (node_a_id < node_b_id),
    CONSTRAINT reef_judgement_expiry_shape CHECK (verdict = 'unsure' OR expires_at IS NULL),
    CONSTRAINT reef_judgement_annul_shape  CHECK (annulled_by IS NULL OR annulled_at IS NOT NULL),
    CONSTRAINT reef_judgement_annul_kind_shape CHECK ((annulled_at IS NULL) = (annulled_by_kind IS NULL))
);

ALTER TABLE node_merges
    ADD COLUMN IF NOT EXISTS judgement_id UUID
        REFERENCES reef_judgement(judgement_id) ON DELETE SET NULL;

-- FK-covering indexes (review round 1, finding 9): Postgres does not create one
-- automatically for a referencing column, and without it every
-- `reef_judgement` delete/PK-update would force a sequential scan of
-- `node_merges` (and, for `superseded_by`, of `reef_judgement` itself) to
-- enforce the FK — partial on `IS NOT NULL` since most rows never populate
-- either column (only merged-and-materialized rows get a `node_merges` back-
-- link; only superseded rows get `superseded_by`).
CREATE INDEX IF NOT EXISTS idx_node_merges_judgement_id
    ON node_merges (judgement_id) WHERE judgement_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_reef_judgement_superseded_by
    ON reef_judgement (superseded_by) WHERE superseded_by IS NOT NULL;

-- "Latest active judgement for this pair" — immutable-safe partial predicate (no now()).
CREATE UNIQUE INDEX IF NOT EXISTS uq_reef_judgement_active_pair
    ON reef_judgement (graph_id, node_a_id, node_b_id)
    WHERE annulled_at IS NULL;

-- Per-node judgement lookups (undo-linkage, review-card provenance, and Phase B's future
-- component computation — NOT used by anything in Phase A's live code path, see §4).
-- review round 1, finding 10: a bare `idx_reef_judgement_node_a` was dropped from this
-- migration before merge — `uq_reef_judgement_active_pair`'s own leading `(graph_id,
-- node_a_id, ...)` columns already serve any `graph_id`+`node_a_id` lookup (every real
-- caller, `Store::get_active_judgement`/`list_active_judgements_for_node`, always filters
-- by `graph_id`), so a bare `node_a_id`-only index added nothing over it. `node_b_id` has
-- no such free ride — it is not a leading column of any other index here — so
-- `idx_reef_judgement_node_b` stays: it is what lets `list_active_judgements_for_node`'s
-- `node_a_id = $2 OR node_b_id = $2` planner choice reach the `node_b_id` branch via an
-- index (typically a BitmapOr with the unique index's `node_a_id` branch) instead of a
-- sequential scan.
CREATE INDEX IF NOT EXISTS idx_reef_judgement_node_b
    ON reef_judgement (node_b_id) WHERE annulled_at IS NULL;

-- Review-queue scan: active UNSURE rows, oldest first. Expiry is a query-time predicate
-- applied by the caller, not baked into this index.
CREATE INDEX IF NOT EXISTS idx_reef_judgement_review_queue
    ON reef_judgement (graph_id, created_at)
    WHERE annulled_at IS NULL AND verdict = 'unsure';

-- v2 addition: the §5.0 read-gate's "active un-materialized POSITIVE" candidate stream scans
-- exactly this shape every sweep — a pending-materialization pair must be found regardless of
-- whether lexical blocking would ever re-propose it.
CREATE INDEX IF NOT EXISTS idx_reef_judgement_pending_positive
    ON reef_judgement (graph_id, created_at)
    WHERE annulled_at IS NULL AND verdict = 'positive';

-- Full pair history (all verdicts, incl. annulled) — audit/debug, backfill idempotency check.
CREATE INDEX IF NOT EXISTS idx_reef_judgement_pair_history
    ON reef_judgement (graph_id, node_a_id, node_b_id, created_at DESC);

ALTER TABLE reef_judgement ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS account_isolation ON reef_judgement;
-- v2: anchored on `graph_id` DIRECTLY, not via a `node_a_id -> nodes -> graphs` join (the
-- `conflict_dismissal`/`reef_identifiers` pattern v1 copied). That pattern relies on the
-- endpoint FK to `nodes` staying resolvable; this table deliberately drops that FK (see
-- header), so a node-anchored policy would make a row invisible under RLS the moment either
-- endpoint is deleted — silently defeating the entire point of NOT cascading (preserving
-- audit history past node deletion). `graph_id` is NOT NULL and FK'd, so anchoring there is
-- both simpler and correct regardless of endpoint liveness.
CREATE POLICY account_isolation ON reef_judgement
    USING (graph_id IN (
        SELECT id FROM graphs WHERE account_id = current_setting('app.account_id')::uuid
    ));
