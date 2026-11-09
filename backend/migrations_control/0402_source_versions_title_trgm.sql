-- no-transaction
-- Chat find: the Cmd-K title search (`GET /v1/find/titles`) matches
-- `ILIKE '%q%'` against a source version's title, on every keystroke. The title
-- is an expression over the version's own columns (session title, recorded
-- title, Slack channel; `services::find_results::TITLE_EXPR`), so this
-- expression index lets the planner use a trigram bitmap scan instead of
-- evaluating the expression on every source version of every readable ocean.
-- Expression index, not a generated column: no table rewrite. The query must
-- keep the expression identical to this one.
-- Paired control/tenant DDL is identical. pg_trgm is installed by the tenant
-- baseline and control 0046.
-- SQLx sends this file as one wire Query, so keep exactly ONE statement here:
-- multiple concurrent builds in one file acquire an implicit transaction.
--
-- A failed build can leave an INVALID index. IF NOT EXISTS does not repair
-- it: inspect pg_index.indisvalid and DROP INDEX CONCURRENTLY the named index
-- before retrying. Do not mark an invalid index as a successful migration.
-- Down: DROP INDEX CONCURRENTLY IF EXISTS idx_source_versions_title_trgm;

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_source_versions_title_trgm
    ON source_versions USING gin ((substring(COALESCE(
        NULLIF(capture_context->>'session_title',''),NULLIF(source_metadata->>'session_title',''),NULLIF(source_metadata->>'title',''),
        CASE WHEN source_type='slack' THEN CASE WHEN NULLIF(source_metadata->>'thread_ts','') IS NULL THEN '#' ELSE 'Thread in #' END
        ||COALESCE(NULLIF(source_metadata->>'channel_name',''),NULLIF(origin->'container'->>'label','')) END
    ) FOR 256)) gin_trgm_ops);
