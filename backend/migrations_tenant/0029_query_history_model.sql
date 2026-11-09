-- Migration: 0029_query_history_model (tenant tree)
-- Description: Tenant-tree half of the paired `query_history.model` change.
--   See `migrations_control/0120_query_history_model.sql` for the full
--   rationale (metering-completeness audit: synthesis's model is
--   user-tiered — Haiku/Sonnet/Opus — so `input_tokens`/`output_tokens`
--   alone cannot attribute cost; this is a usage-billing prerequisite).
--
--   `query_history` is one of the content tables shared by both trees, so
--   this MUST stay byte-equivalent to the control side or
--   `scripts/check_schema_parity.sh` fails: it applies each tree to a
--   scratch DB and diffs a normalized `pg_dump --schema-only` of the shared
--   tables. Keep the column type, nullability, and `COMMENT` text identical
--   to 0117.
--
--   Idempotent (`IF NOT EXISTS`) like every other tenant-tree column add,
--   since tenant databases are provisioned at different times and the
--   orchestrator may re-run a tree against a DB that is already partially
--   ahead. No backfill — same reasoning as 0117: the model actually used
--   for a historical row was never recorded, so there is nothing to derive
--   it from.

ALTER TABLE query_history ADD COLUMN IF NOT EXISTS model TEXT;

COMMENT ON COLUMN query_history.model IS
    'The synthesis model that answered this turn (Haiku/Sonnet/Opus, '
    'user-tiered — see llm::model_config::synthesis_model). NULL when no '
    'synthesis ran for this turn: retrieval-only mode, the honesty-guard '
    'abstention, an intake/metadata answer, or a failed synthesis attempt. '
    'Historical rows predating this column are also NULL and cannot be '
    'backfilled — the model actually used was never recorded.';
