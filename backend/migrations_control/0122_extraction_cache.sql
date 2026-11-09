-- Migration: 0122_extraction_cache
-- Description: `extraction_cache` — content-hash dedup for the entity/
--   relationship extraction workers (Billing Plan v2 "lever six", the cost
--   lever that makes team gross margin IMPROVE with seat count).
--
-- ─────────────────────────────────────────────────────────────────────────────
-- THE PROBLEM THIS TABLE SOLVES
-- ─────────────────────────────────────────────────────────────────────────────
--
-- `atoms`' uniqueness (`0003_atoms`: `UNIQUE (graph_id, source_type,
-- source_id)`) is scoped PER OCEAN. The same Slack message fanned out into N
-- member oceans (member-private-oceans, PRs #340/#343) becomes N distinct
-- `atoms` rows and is therefore LLM-extracted N times at full cost — once per
-- ocean, even though the underlying text is byte-identical every time.
-- Extraction cost should scale with UNIQUE content, not content x oceans.
--
-- `extraction_cache` is a read-through cache in front of the extraction LLM
-- calls (`workers::extract`, `workers::extract_relationships`,
-- `workers::extract_batch`): before calling the model, the worker hashes the
-- exact prompt-relevant atom text and looks up `(content_hash, task, model,
-- prompt_version)`. A hit replays the stored successful response through the
-- IDENTICAL parse + write path a fresh call would use and skips the LLM call
-- entirely. A miss calls the LLM as before, then writes the result here
-- (best-effort — a cache-write failure never fails the extraction job).
--
-- ─────────────────────────────────────────────────────────────────────────────
-- WHY `prompt_version` IS PART OF THE KEY
-- ─────────────────────────────────────────────────────────────────────────────
--
-- The cached payload is the model's raw response, parsed by whatever the
-- CURRENT `extract_entities_v1.txt`/`extract_relationships_v1.txt` prompt
-- produces. If the prompt changes shape (new fields, different instructions
-- that change what "correct" output looks like), a stale cached response from
-- the OLD prompt must never be replayed as if it came from the new one.
-- `crate::llm::extraction_cache::PROMPT_VERSION` is a plain incrementing
-- constant bumped by hand whenever an extraction prompt changes; bumping it
-- makes every existing row unreachable (a structural, not a TTL-based,
-- invalidation) without deleting anything — old rows simply age out via
-- ordinary retention/vacuum, and a fresh miss repopulates the cache under the
-- new version.
--
-- `model` is part of the key for the identical reason: changing
-- `EXTRACT_MODEL`/`RELATION_MODEL` must not replay a different model's output
-- under the new model's name. See that module's doc comment for why the
-- stored `model` is always the pipeline's CONFIGURED model for the task
-- (`model_config::extract_model()`/`relation_model()`), not necessarily the
-- literal model that produced the payload — the escalation design point
-- below depends on this.
--
-- ─────────────────────────────────────────────────────────────────────────────
-- ESCALATION INTERPLAY
-- ─────────────────────────────────────────────────────────────────────────────
--
-- `EXTRACT_ESCALATE_MODEL` (opt-in, default off) can re-run an uncertain
-- extraction on a stronger model and use ITS result instead (see
-- `workers::extract::extract_with_escalation`). The row cached here is always
-- the FINAL result — post-escalation, if escalation fired — keyed under the
-- PRIMARY/configured model, never the escalated one. That means every
-- subsequent fanned-out copy of the same content hits the cache under the
-- ordinary lookup key and gets the escalated-quality result without a second
-- escalation ever firing again: "replayed hits never re-escalate" is a direct
-- consequence of the key shape, not a special case the read path has to
-- detect.
--
-- ─────────────────────────────────────────────────────────────────────────────
-- SCOPING: WHY `account_id` IS DB-WIDE ON THE ORG-TENANT PLANE BUT PER-ACCOUNT
-- HERE
-- ─────────────────────────────────────────────────────────────────────────────
--
-- This is the CONTROL/consumer-plane half of the pair. One physical database
-- holds every consumer account, so a cache row here MUST be scoped per
-- account: two unrelated accounts' extraction results must never be
-- observable to each other, including through cache-hit TIMING (a hit skips
-- the LLM call and completes fast; if account B's lookup could hit a row
-- written by account A, both the payload AND the "someone already sent this
-- exact text" fact would leak cross-account). `account_id` is therefore a
-- real, always-populated, RLS-enforced column here (the 0083 `account_isolation`
-- shape — see this migration's RLS section).
--
-- On the ORG-TENANT plane (paired migration
-- `migrations_tenant/0030_extraction_cache.sql`), a tenant database holds
-- exactly ONE org, and the entire economic case for this feature is the
-- cross-OCEAN fan-out WITHIN that one org (the same Slack channel landing in
-- N members' private oceans). There is no second org's data in that database
-- for a policy to hide, so the tenant side carries NO row-level security at
-- all on this table — it is deliberately DB-wide, which is exactly where the
-- fan-out win lives. See that migration's header for how `account_id` is
-- still present there (for schema-parity's identical-shape requirement) but
-- always holds a single, fixed sentinel value rather than a real per-account
-- distinction.
--
-- ─────────────────────────────────────────────────────────────────────────────
-- WHY `account_id` IS PART OF THE UNIQUE KEY (NOT JUST AN RLS COLUMN)
-- ─────────────────────────────────────────────────────────────────────────────
--
-- The natural cache key from the workers' point of view is `(content_hash,
-- task, model, prompt_version)` alone — see `crate::llm::extraction_cache`'s
-- module doc. Folding `account_id` into the SQL `UNIQUE` constraint (rather
-- than leaving it as a bare RLS-only column) achieves BOTH plane's semantics
-- with one constraint and no NULL-handling special case:
--   * Consumer plane: `account_id` is always a real, distinct account, so the
--     constraint is effectively per-account — two accounts hashing the same
--     text each get their own row, never colliding on `ON CONFLICT`.
--   * Org-tenant plane: every row's `account_id` is the SAME fixed sentinel
--     (see the tenant migration), so the constraint collapses to exactly the
--     bare `(content_hash, task, model, prompt_version)` key the design calls
--     for — true DB-wide dedup, with no separate index or NULL-coalescing
--     index expression needed.
--
-- ─────────────────────────────────────────────────────────────────────────────
-- WHY `response_content` IS THE RAW PROVIDER STRING, NOT `JSONB`
-- ─────────────────────────────────────────────────────────────────────────────
--
-- `response_content` stores exactly `CompletionResponse::content` — the same
-- string `parse_extract_response`/`parse_relationships` already tolerate
-- (code-fenced, occasionally malformed JSON included, per those parsers'
-- existing lenient-parse contract). Storing it as `TEXT` rather than `JSONB`
-- means a hit replays through the IDENTICAL lenient parse a miss would have
-- used — no separate validation path that could accept something the parser
-- would have rejected, or reject something the parser would have tolerated.
--
-- ─────────────────────────────────────────────────────────────────────────────
-- WHY `hit_count`/`last_hit_at` EXIST
-- ─────────────────────────────────────────────────────────────────────────────
--
-- Pure cost-visibility metadata (never read by any gate or write path): an
-- operator asking "is this lever actually working" needs more than "the LLM
-- call didn't happen" — `hit_count` (incremented atomically by the lookup
-- itself, an `UPDATE ... RETURNING`) is the cheapest queryable proxy for hit
-- rate without a separate analytics table. `input_tokens`/`output_tokens`/
-- the two cache-token columns record what the ORIGINAL (cache-populating)
-- call cost, for the same reason `llm_usage` records them — see that table's
-- migration 0044/0073.
--
-- Depends on: 0083_consumer_rls (the `account_isolation` RLS pattern this
--   follows for a direct-`account_id` content table with no ocean anchor —
--   same class as `queries`/`oauth_tokens`/`llm_usage`), 0106_team_seat_grants
--   (most recent precedent for the "no FK on the tenant twin" convention this
--   migration's pair follows).
--
-- Down:
--   DROP POLICY IF EXISTS account_isolation ON extraction_cache;
--   ALTER TABLE extraction_cache DISABLE ROW LEVEL SECURITY;
--   DROP TABLE IF EXISTS extraction_cache;

CREATE TABLE IF NOT EXISTS extraction_cache (
    id                          UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    account_id                  UUID        NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
    -- SHA-256 hex digest (64 chars) of the exact prompt-relevant, normalized
    -- atom text the extraction request was built from — see
    -- `crate::llm::extraction_cache::hash_entity_content`/
    -- `hash_relationship_content` for the exact bytes hashed per task.
    content_hash                TEXT        NOT NULL CHECK (content_hash <> ''),
    -- 'extract_entities' | 'extract_relationships' — matches the `task`
    -- string already passed to `llm::usage::record` at these call sites, so a
    -- reader correlating this table against `llm_usage` finds the same
    -- string.
    task                        TEXT        NOT NULL CHECK (task <> ''),
    -- The pipeline's CONFIGURED model for this task at cache-write time
    -- (`model_config::extract_model()`/`relation_model()`) — see this
    -- migration's header on why this is not necessarily the literal model
    -- that generated `response_content` when escalation fired.
    model                       TEXT        NOT NULL CHECK (model <> ''),
    -- `crate::llm::extraction_cache::PROMPT_VERSION` at cache-write time.
    prompt_version              SMALLINT    NOT NULL,
    -- The raw provider response string — see this migration's header on why
    -- this is TEXT, not JSONB.
    response_content            TEXT        NOT NULL CHECK (response_content <> ''),
    input_tokens                INTEGER     NOT NULL DEFAULT 0,
    output_tokens               INTEGER     NOT NULL DEFAULT 0,
    -- BIGINT to match `llm_usage`'s own columns of the same name (migration
    -- 0073) exactly.
    cache_creation_input_tokens BIGINT      NOT NULL DEFAULT 0,
    cache_read_input_tokens     BIGINT      NOT NULL DEFAULT 0,
    -- Incremented on every cache HIT (never on the populating write, which
    -- starts at 0) — see this migration's header.
    hit_count                   INTEGER     NOT NULL DEFAULT 0,
    created_at                  TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_hit_at                 TIMESTAMPTZ,

    -- See this migration's header ("WHY account_id IS PART OF THE UNIQUE
    -- KEY") for why this single constraint correctly expresses both planes'
    -- dedup semantics with no NULL-handling special case.
    UNIQUE (account_id, content_hash, task, model, prompt_version)
);

ALTER TABLE extraction_cache ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS account_isolation ON extraction_cache;
CREATE POLICY account_isolation ON extraction_cache
    USING (account_id = current_setting('app.account_id')::uuid);
