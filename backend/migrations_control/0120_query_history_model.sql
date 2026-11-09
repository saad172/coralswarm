-- Migration: 0120_query_history_model
-- Description: `query_history.model` — the synthesis model that actually
--   answered a turn (billing-completeness / metering-completeness audit).
--
--   Why this exists. `query_history` already records `input_tokens`/
--   `output_tokens` per turn, but synthesis's model is USER-TIERED
--   (Haiku/Sonnet/Opus by plan — `llm::model_config::synthesis_model`), so
--   token counts alone cannot attribute cost: the same `output_tokens` value
--   bills very differently depending on which tier answered. This is a
--   prerequisite for usage-based billing — you cannot debit for a workload
--   whose price tier you never recorded.
--
--   Nullable with no default, same precedent as 0116/0097/0072/0066: a plain
--   `ADD COLUMN ... TEXT` is a metadata-only change on Postgres 11+, no table
--   rewrite. NULL means "no synthesis ran for this turn" — retrieval-only
--   mode, the honesty-guard abstention, an intake/metadata answer, or a
--   synthesis attempt that failed even after its internal retry. Every
--   reader must tolerate NULL; no backfill is possible for historical rows
--   (the model actually used was never recorded, so there is nothing to
--   derive it from).
--
--   Paired with `migrations_tenant/0029_query_history_model.sql`.
--   `query_history` is one of the shared content tables, so a column added
--   to only one tree fails `scripts/check_schema_parity.sh` by design. Keep
--   the column type, nullability, and absence of a default identical to
--   0028.
--
-- Depends on: 0019_query_history (query_history).
--
-- Down:
--   ALTER TABLE query_history DROP COLUMN IF EXISTS model;

ALTER TABLE query_history ADD COLUMN IF NOT EXISTS model TEXT;

COMMENT ON COLUMN query_history.model IS
    'The synthesis model that answered this turn (Haiku/Sonnet/Opus, '
    'user-tiered — see llm::model_config::synthesis_model). NULL when no '
    'synthesis ran for this turn: retrieval-only mode, the honesty-guard '
    'abstention, an intake/metadata answer, or a failed synthesis attempt. '
    'Historical rows predating this column are also NULL and cannot be '
    'backfilled — the model actually used was never recorded.';
