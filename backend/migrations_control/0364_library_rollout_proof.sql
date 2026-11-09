-- Control-only operator proof, not tenant content. No tenant twin: every
-- tenant's comparisons are recorded in the sealed operator account namespace.
-- No query text, titles, cursors, content payloads, credentials or raw errors.
-- Raw observations expire after 30 days; export acceptance artifacts first.
CREATE TABLE library_rollout_runs (
 run_id UUID PRIMARY KEY,
 account_id UUID NOT NULL,
 manifest JSONB NOT NULL CHECK(jsonb_typeof(manifest)='object'),
 manifest_sha256 TEXT NOT NULL CHECK(manifest_sha256 ~ '^[0-9a-f]{64}$'),
 created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
 UNIQUE(run_id,account_id)
);
CREATE FUNCTION library_rollout_manifest_immutable() RETURNS TRIGGER
LANGUAGE plpgsql SET search_path=public,pg_temp AS $$
BEGIN RAISE EXCEPTION 'Library rollout manifest is immutable; create a new run'; END $$;
REVOKE ALL ON FUNCTION library_rollout_manifest_immutable() FROM PUBLIC;
CREATE TRIGGER library_rollout_manifest_immutable BEFORE UPDATE ON library_rollout_runs
 FOR EACH ROW EXECUTE FUNCTION library_rollout_manifest_immutable();

CREATE TABLE library_rollout_samples (
 attempt_id UUID PRIMARY KEY,
 run_id UUID NOT NULL,
 account_id UUID NOT NULL,
 boot_id UUID NOT NULL,
 sequence BIGINT NOT NULL CHECK(sequence>0),
 tenant_key TEXT NOT NULL CHECK(length(tenant_key) BETWEEN 1 AND 256),
 shape JSONB NOT NULL CHECK(jsonb_typeof(shape)='object'),
 kind TEXT NOT NULL CHECK(kind IN ('online','offline')),
 started_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
 deadline_at TIMESTAMPTZ NOT NULL,
 finished_at TIMESTAMPTZ,
 outcome TEXT NOT NULL DEFAULT 'started' CHECK(outcome IN
  ('started','equal','mismatch','live_error','new_error','live_timeout','new_timeout',
   'authorization_changed','fallback','interrupted')),
 result JSONB,
 CHECK((outcome='started')=(finished_at IS NULL)),
 CHECK((outcome='started')=(result IS NULL)),
 FOREIGN KEY(run_id,account_id) REFERENCES library_rollout_runs(run_id,account_id) ON DELETE CASCADE,
 UNIQUE(run_id,boot_id,sequence)
);
CREATE INDEX library_rollout_samples_run ON library_rollout_samples(account_id,run_id,tenant_key,started_at);
CREATE INDEX library_rollout_samples_pending ON library_rollout_samples(account_id,deadline_at) WHERE outcome='started';
CREATE INDEX library_rollout_samples_retention ON library_rollout_samples(account_id,started_at,attempt_id);

CREATE TABLE library_rollout_windows (
 run_id UUID NOT NULL,
 account_id UUID NOT NULL,
 boot_id UUID NOT NULL,
 sequence BIGINT NOT NULL CHECK(sequence>=0),
 machine_id TEXT NOT NULL CHECK(length(machine_id) BETWEEN 1 AND 128),
 kind TEXT NOT NULL CHECK(kind IN ('boot','window','closed')),
 window_start TIMESTAMPTZ NOT NULL,
 window_end TIMESTAMPTZ NOT NULL CHECK(window_end>=window_start),
 payload JSONB NOT NULL CHECK(jsonb_typeof(payload)='object'),
 checksum TEXT NOT NULL CHECK(checksum ~ '^[0-9a-f]{64}$'),
 recorded_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
 CHECK((kind='boot')=(sequence=0)),
 PRIMARY KEY(run_id,boot_id,sequence),
 FOREIGN KEY(run_id,account_id) REFERENCES library_rollout_runs(run_id,account_id) ON DELETE CASCADE
);
CREATE INDEX library_rollout_windows_retention ON library_rollout_windows(account_id,recorded_at,run_id,boot_id,sequence);

ALTER TABLE library_rollout_runs ENABLE ROW LEVEL SECURITY;
ALTER TABLE library_rollout_samples ENABLE ROW LEVEL SECURITY;
ALTER TABLE library_rollout_windows ENABLE ROW LEVEL SECURITY;
CREATE POLICY account_isolation ON library_rollout_runs
 USING(account_id=current_setting('app.account_id')::uuid) WITH CHECK(account_id=current_setting('app.account_id')::uuid);
CREATE POLICY account_isolation ON library_rollout_samples
 USING(account_id=current_setting('app.account_id')::uuid) WITH CHECK(account_id=current_setting('app.account_id')::uuid);
CREATE POLICY account_isolation ON library_rollout_windows
 USING(account_id=current_setting('app.account_id')::uuid) WITH CHECK(account_id=current_setting('app.account_id')::uuid);
GRANT SELECT,INSERT,DELETE ON library_rollout_runs TO coralswarm_app;
GRANT SELECT,INSERT,UPDATE,DELETE ON library_rollout_samples TO coralswarm_app;
GRANT SELECT,INSERT,DELETE ON library_rollout_windows TO coralswarm_app;
