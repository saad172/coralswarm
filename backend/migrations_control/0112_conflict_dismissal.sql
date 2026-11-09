-- Migration: 0112_conflict_dismissal
-- Description: `conflict_dismissal` — a durable negative marker for §3.4's
--   conflict-detection worker, so a pair the LLM has already adjudicated as
--   "different" or "unsure" is not re-blocked, re-ranked, and re-sent to
--   Haiku on every subsequent debounced pass.
-- PAIRED WITH: migrations_tenant/0023_conflict_dismissal.sql — the table DDL
--              in the two files is IDENTICAL (`scripts/check_schema_parity.sh`);
--              the RLS layered on top of it deliberately is NOT (see below).
-- Depends on: 0011_nodes (nodes), 0083_consumer_rls (RLS pattern this follows).
-- Down:
--   DROP POLICY IF EXISTS account_isolation ON conflict_dismissal;
--   ALTER TABLE conflict_dismissal DISABLE ROW LEVEL SECURITY;
--   DROP TABLE IF EXISTS conflict_dismissal;
--
-- ─────────────────────────────────────────────────────────────────────────────
-- WHY THIS IS A NEW TABLE, NOT A ROW IN `non_supersession_pair`
-- ─────────────────────────────────────────────────────────────────────────────
--
-- `non_supersession_pair` (migration 0011, wired by D3/§3.7) records that a
-- HUMAN explicitly rejected a candidate supersession via
-- `DELETE /v1/nodes/{id}/supersede/{other_id}` — a person looking at two
-- claims and saying "these are not the same". `conflict_dismissal` records
-- that a MODEL (Haiku, via §3.4's adjudication prompt) answered "different"
-- or "unsure" for a pair. These are not the same kind of statement and must
-- never be conflated:
--
--   * A model verdict can be wrong in ways a human's explicit judgement
--     usually is not — an LLM adjudication is a cheap, fallible signal, not
--     a considered human decision.
--   * Writing a model "unsure" into `non_supersession_pair` would let an
--     uncertain machine guess masquerade as a human's considered rejection
--     everywhere that table is read as authoritative (§3.4's own gate,
--     §3.4a's anchored-correction gate) — silently raising the bar a human
--     would otherwise have had a chance to overrule.
--   * The two need DIFFERENT lifetimes (see below) — `non_supersession_pair`
--     has no expiry at all ("There is no delete variant: a rejection is a
--     permanent negative marker, never un-rejected by this system today" —
--     `store/sync.rs`'s own `non_supersession_pair_op` doc comment), which is
--     correct for a human's word but wrong for an "unsure" machine guess.
--
-- §3.4's detection pass therefore consults BOTH gates independently, in this
-- order: `is_non_supersession_pair` (human, unconditionally authoritative)
-- FIRST, then `is_conflict_dismissed` (model, expiry- and prompt-version-
-- bounded) SECOND — see `workers/conflict_detection.rs`. Neither table is
-- ever written into by the other's code path.
--
-- ─────────────────────────────────────────────────────────────────────────────
-- EXPIRY POLICY: "different" IS PERMANENT, "unsure" IS NOT
-- ─────────────────────────────────────────────────────────────────────────────
--
-- The two verdicts are not the same claim about the world:
--
--   * "different" is a judgement about the two claims THEMSELVES — they are
--     both true and can coexist (e.g. "ExamplePortal is hosted on Fly.io" / "the CLI
--     ships from GitHub Releases": two independent facts). That relationship
--     does not change as the corpus grows; re-adjudicating it later would
--     spend a real LLM call to re-derive the same answer. So a "different"
--     row's `expires_at` is always NULL — it never expires by time.
--
--   * "unsure" is a judgement about the EVIDENCE available right now — "you
--     cannot tell from the text alone" (the adjudication prompt's own
--     wording). More corals land in the graph every day; the entity/decision
--     reefs on either side of an "unsure" pair can accumulate the very
--     context that would make the pair answerable. Permanently suppressing
--     "unsure" the same way as "different" would risk hiding a real conflict
--     forever, for a reason (insufficient evidence) that is BY CONSTRUCTION
--     temporary. So an "unsure" row's `expires_at` is `dismissed_at +
--     CONFLICT_UNSURE_DISMISSAL_TTL_DAYS` (default 30 —
--     `llm::model_config::unsure_dismissal_ttl_days()`, the same 30-day
--     figure already established for `ACCESS_LOG_RETENTION_DAYS`/
--     `TOMBSTONE_RETENTION_DAYS`, i.e. this repo's existing default cadence
--     for "revisit later" windows, not a number invented for this table).
--
-- Enforcement is a QUERY-TIME PREDICATE (`is_conflict_dismissed`'s
-- `(expires_at IS NULL OR expires_at > now())`), not a background sweep: an
-- expired "unsure" row is simply invisible to the gate from the moment it
-- expires — no purge worker, no window in which a sweep hasn't run yet, and
-- the row itself survives (still auditable — see below) until something
-- else (e.g. a future retention pass, out of scope here) decides to reclaim
-- it. This is the same "simpler and less surprising than a background sweep"
-- tradeoff `workers::tombstone_purge`/`workers::access_log_purge` make in the
-- opposite direction (they DO sweep) — here a sweep buys nothing: an expired
-- row costs one extra LLM call the next time the pair is proposed, which is
-- the exact steady state before this migration existed at all, whereas a
-- missed non-supersession-pair sweep would risk silently exposing (or
-- deleting) rows more sensitive than "we might re-ask Haiku once."
--
-- The `conflict_dismissal_expiry_matches_verdict` CHECK constraint below
-- pins this invariant into the schema itself (verdict = 'unsure' OR
-- expires_at IS NULL), so a future writer that forgets the policy fails
-- loudly at INSERT time rather than silently mis-expiring a 'different' row.
--
-- ─────────────────────────────────────────────────────────────────────────────
-- WHY `prompt_version` ALSO GATES THIS (both verdicts, not just "unsure")
-- ─────────────────────────────────────────────────────────────────────────────
--
-- `is_conflict_dismissed` additionally requires `prompt_version` to match the
-- CALLER's current adjudication prompt version
-- (`workers::conflict_detection::CONFLICT_PROMPT_VERSION`, derived from
-- `prompts/detect_conflicts_v1.txt`'s own filename). This is a SEPARATE axis
-- from the time-based expiry above: it is not about the corpus growing, it is
-- about whether the JUDGE itself is still the one that produced the verdict.
-- If `detect_conflicts_v1.txt` is revised into a `_v2` prompt (a bug fix, a
-- clarified instruction, a tightened precision bar), every prior verdict —
-- "different" rows included — was produced by a classifier that no longer
-- exists in this form, so none of them should silently keep gating the new
-- one. Bumping the prompt version therefore re-opens EVERY previously
-- dismissed pair for free, with no migration, no backfill, and no sweep: old
-- rows simply stop matching the gate's `prompt_version = $N` predicate. They
-- are not deleted — they remain in the table as an audit trail of what an
-- earlier prompt version concluded.
--
-- ─────────────────────────────────────────────────────────────────────────────
-- AUDITABILITY
-- ─────────────────────────────────────────────────────────────────────────────
--
-- Every row records: which verdict (`verdict`), when (`dismissed_at`), and
-- against which model + prompt version (`model`, `prompt_version`) — the
-- three things a future reader needs to answer "why did the system stop
-- asking about this pair?" without re-running anything. `confidence` is
-- carried too (the same value `workers::conflict_detection::adjudicate`
-- already gets back from the model) since it was already in hand at zero
-- extra cost and materially changes how much to trust a stale-looking
-- dismissal on manual review.
--
-- ─────────────────────────────────────────────────────────────────────────────
-- RLS: WHY THIS TABLE NEEDS IT, AND WHY THE TWO PLANES DIFFER
-- ─────────────────────────────────────────────────────────────────────────────
--
-- Same reasoning as `non_supersession_pair` (0083's own section immediately
-- above this one): under OPS-4's `coralswarm_app` role, ANY table without RLS
-- + a policy is invisible to every query running as that role — an
-- accidentally-unscoped read would not leak cross-account, it would simply
-- hard-error, which is the intended failure mode. `conflict_dismissal` is
-- reached exclusively through `Store` (`is_conflict_dismissed[_scoped]` /
-- `mark_conflict_dismissal[_scoped]`), so it needs the same backstop as every
-- other content table `Store` mediates.
--
-- The policy predicate mirrors `non_supersession_pair`'s exactly: anchor on
-- ONE endpoint (`node_a_id`) rather than requiring both, because — same
-- invariant, same caveat — its only producer (`workers::conflict_detection`)
-- only ever proposes SAME-graph pairs (both ids come from
-- `scoped_list_nodes(..., graph_id, ...)` for one `graph_id` in one job), so
-- anchoring on either endpoint identifies the same account. This is an
-- invariant maintained by code, not the schema (see `non_supersession_pair`'s
-- own note on this exact point) — the hop stays safe regardless, because
-- `nodes` is itself RLS'd, so the subquery can never resolve to an endpoint
-- outside the caller's reachable set.
--
-- This is CONTROL-plane-only RLS, registered in
-- `scripts/check_schema_parity.sh`'s `CONTROL_ONLY_RLS_TABLES` (alongside
-- `non_supersession_pair`) in this same PR — the tenant-tree twin
-- (migrations_tenant/0023) carries the SAME table shape but a DIFFERENT
-- `member_ocean_isolation` policy, for the same reason 0009 diverges from
-- 0083 for every other shared content table.

CREATE TABLE IF NOT EXISTS conflict_dismissal (
    node_a_id      UUID             NOT NULL REFERENCES nodes(node_id) ON DELETE CASCADE,
    node_b_id      UUID             NOT NULL REFERENCES nodes(node_id) ON DELETE CASCADE,
    -- 'different': both claims are true and coexist (permanent — never
    -- expires by time). 'unsure': the model could not tell from the text
    -- alone (expires — see expires_at). Deliberately excludes 'conflicts':
    -- a confirmed conflict WRITES a supersession_edge (§3.4/§3.5); it has
    -- nothing to be "dismissed" from.
    verdict        TEXT             NOT NULL CHECK (verdict IN ('different', 'unsure')),
    -- The adjudication model id (e.g. `claude-haiku-4-5`) — whatever
    -- `llm::model_config::conflict_model()` resolved to at write time.
    model          TEXT             NOT NULL CHECK (model <> ''),
    -- The adjudication PROMPT version (e.g. `detect_conflicts_v1`, derived
    -- from `prompts/detect_conflicts_v1.txt`'s own filename) — see this
    -- migration's header for why this re-gates on a prompt change.
    prompt_version TEXT             NOT NULL CHECK (prompt_version <> ''),
    -- The model's own confidence in `verdict`, carried for free from the
    -- same adjudication call — auditability, not a gating predicate.
    confidence     DOUBLE PRECISION NOT NULL DEFAULT 0,
    dismissed_at   TIMESTAMPTZ      NOT NULL DEFAULT now(),
    -- NULL = never expires (always true for 'different'). Set for 'unsure'
    -- to `dismissed_at + CONFLICT_UNSURE_DISMISSAL_TTL_DAYS` at write time —
    -- see this migration's header for the policy and the query-time-only
    -- enforcement.
    expires_at     TIMESTAMPTZ,

    -- Order-insensitive uniqueness: callers must always insert with
    -- node_a_id < node_b_id (mirrors non_supersession_pair's own constraint
    -- and rationale verbatim).
    CONSTRAINT conflict_dismissal_ordered CHECK (node_a_id < node_b_id),
    -- Pins the expiry policy into the schema: an 'unsure' row MAY carry an
    -- expiry, a 'different' row MUST NOT (see header). Catches a future
    -- writer that forgets the asymmetry at INSERT time rather than letting
    -- a 'different' row silently start expiring.
    CONSTRAINT conflict_dismissal_expiry_matches_verdict
        CHECK (verdict = 'unsure' OR expires_at IS NULL),
    UNIQUE (node_a_id, node_b_id)
);

ALTER TABLE conflict_dismissal ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS account_isolation ON conflict_dismissal;
CREATE POLICY account_isolation ON conflict_dismissal
    USING (node_a_id IN (
        SELECT node_id FROM nodes
        WHERE graph_id IN (
            SELECT id FROM graphs WHERE account_id = current_setting('app.account_id')::uuid
        )
    ));
