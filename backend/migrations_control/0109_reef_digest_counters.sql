-- Migration: 0109_reef_digest_counters
-- Description: Write-time coral counters that drive reef digest regeneration
--   (graph-temporal-memory plan, Workstream A, §2.2-§2.3).
--
--   ── Why two counters, not one ──
--   `coral_count` is the reef's current, absolute atom-link count.
--   `digest_coral_count` is a snapshot of `coral_count` taken the last time a
--   digest was (re)generated. Both are absolute counts, not a since-last-
--   digest delta — so there is no reset, no snapshot window, and no in-flight
--   race: corals landing while a digest job is running simply increment
--   `coral_count` and count toward the NEXT cycle. Eligibility and cadence
--   (§2.3) read the two together:
--
--     eligible    = type = 'entity' AND coral_count >= 8
--     regenerate  = coral_count - digest_coral_count
--                     >= GREATEST(5, CEIL(digest_coral_count * 0.20))
--                   OR the reef was renamed
--
--   ── Why this migration does NOT land as DEFAULT 0 and stop there ──
--   This backend has NOT been wiped (unlike the plan's §0 assumption, which
--   only holds for a from-scratch cutover). Landing `coral_count` at
--   DEFAULT 0 with no backfill would leave every EXISTING reef below the
--   `coral_count >= 8` eligibility bar forever — silently disabling digest
--   regeneration for the entire installed base with no error and no metric,
--   the exact class of defect §1 of the plan exists to catch. The backfill
--   below derives `coral_count` from the current `node_atoms` join so
--   existing reefs are immediately eligible where their real coral count
--   warrants it. `digest_coral_count` correctly starts at 0 for every row —
--   nothing has been digested under this counter-driven scheme yet, so the
--   very first cadence check for an eligible existing reef legitimately
--   fires (coral_count - 0 >= 8 >= 5) and produces its first digest.
--
--   ── Why counter maintenance is a trigger, not N call-site edits ──
--   `node_atoms` is written from far more places than a first read of the
--   call sites suggests (this plan's own §12 note: "this spec's call-site
--   references have repeatedly proven non-exhaustive" — confirmed again
--   here: beyond the ~13 `Store` methods that link/merge/unmerge atoms,
--   `atoms` is deleted directly — cascading `node_atoms` via
--   `ON DELETE CASCADE` with NO application-level
--   `DELETE FROM node_atoms` statement to grep for — from
--   `workers::slack_name_backfill`, `workers::embed_retry`,
--   `workers::huddle`, and `routes::slack_events`, none of which the plan
--   names). A per-row trigger on `node_atoms` cannot miss a write path: it
--   fires on every INSERT/DELETE regardless of which Rust statement (or SQL
--   cascade) produced it. This mirrors the design principle the plan itself
--   uses for `search_tsv` (§5.6): "a generated column ... cannot drift. No
--   trigger, no worker, no call site to forget" — except the cross-row
--   aggregate a counter requires cannot be expressed as a generated column,
--   so a trigger is the closest equivalent tool.
--
--   ── Why the digest-regeneration enqueue is ALSO a trigger ──
--   For the same reason: enumerating every place `coral_count` can grow (or
--   a title can change) to hand-insert an enqueue call is exactly the
--   exercise that just proved incomplete for the counter itself. An
--   AFTER UPDATE trigger on `nodes` (fired, in the common case, BY the
--   counter trigger's own `UPDATE nodes SET coral_count = ...`, and directly
--   by a rename) evaluates the eligibility/cadence rule in one place and
--   enqueues via the EXISTING `digest:{node_id}` idempotency-key / partial-
--   unique-index dedup pattern (migration 0050) — the same mechanism
--   `infer:`/`er:`/`expertise:` already use, so a burst of corals landing on
--   the same reef debounces into one pending job instead of one per write.
--   `account_id` is left NULL on the enqueued row; `jobs` carries no RLS (no
--   ENABLE ROW LEVEL SECURITY anywhere in this tree), and
--   `queue::dequeue`'s existing pre-W.1-legacy-row backfill
--   (`resolve_graph_account_id`, keyed off the `graph_id` this trigger also
--   stamps into the payload) resolves it lazily on first claim — the exact
--   path already exercised for every other account-less legacy enqueue.
--
--   The equivalent eligibility/cadence arithmetic is duplicated as a pure,
--   unit-tested Rust function (`workers::digest::should_regenerate_digest` /
--   `digest_eligible`) that the worker re-checks before spending an LLM call,
--   since a job can sit `pending` for a while and the two must never
--   silently disagree about the boundary.
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

-- ── Backfill (see header: NOT optional on an already-populated database) ────
--
-- Deliberately AFTER `trg_nodes_digest_regeneration` exists, not before: this
-- UPDATE is itself a real `coral_count` change on every already-eligible
-- existing reef, and the whole point of making digest-regeneration a
-- WHEN-gated AFTER UPDATE trigger (see header, "Why the digest-regeneration
-- enqueue is ALSO a trigger") is that it cannot miss a write path. Running
-- this backfill BEFORE the trigger existed would have been exactly the write
-- path it misses: `coral_count` jumps from 0 to its real count with no
-- `RegenerateDigest` job ever enqueued for any pre-existing reef that
-- backfills straight past the `>= 8` eligibility bar — silently identical to
-- the DEFAULT-0-and-stop-there failure mode this whole migration exists to
-- avoid (see header above), just one step later. Ordered here, the backfill
-- IS a normal `coral_count` UPDATE per the trigger's own rule, so every
-- already-eligible existing reef gets its first digest job enqueued exactly
-- like any future growth would.
UPDATE nodes n
SET coral_count = counts.cnt
FROM (
    SELECT node_id, COUNT(*)::int AS cnt
    FROM node_atoms
    GROUP BY node_id
) counts
WHERE n.node_id = counts.node_id
  AND n.coral_count <> counts.cnt;
