-- Pipeline stage health: the latest per-stage, per-source-type snapshot for
-- each content plane. `workers::pipeline_stage_health` writes one row per
-- plane on every tick; `/internal/admin/pipeline-stage-health` reads them, so
-- one request never spans planes. `tenant` uses `capture_admission_health`'s
-- keys: '__consumer__' or the Clerk org id. Control-only (operator data, no
-- tenant twin, not RLS-covered: counts and timings, never content).
CREATE TABLE pipeline_stage_health (
 tenant TEXT PRIMARY KEY,
 measured_at TIMESTAMPTZ NOT NULL,
 report JSONB NOT NULL
);
