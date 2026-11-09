-- COR-71: the latest capture-to-Atom health snapshot for each content plane.
-- `workers::capture_admission_health` writes one row per plane on every tick;
-- `/internal/admin/chain-health` reads them, so one request never spans
-- planes. `tenant` uses `platform_metrics_daily`'s keys: '__consumer__' or the
-- Clerk org id. Control-only (operator data, no tenant twin, not RLS-covered).
CREATE TABLE capture_admission_health (
 tenant TEXT PRIMARY KEY,
 measured_at TIMESTAMPTZ NOT NULL,
 report JSONB NOT NULL
);
