#!/usr/bin/env python3
"""Read-only durable Ask diagnostics monitor; needs Python 3 and psql.

DATABASE_URL is the control-plane DSN. With --all-tenants, tenant DSNs come
from their existing environment secret or ASK_DSN_RESOLVER (executable path).
Credentials and raw provider bodies are never printed. Exit 1 = sustained
failure rate; exit 2 = incomplete monitoring/configuration; exit 0 = no alarm.
"""
import argparse
import json
import os
import re
import subprocess
import sys
import tempfile
import time
from urllib.parse import parse_qsl, unquote, urlsplit


SUMMARY_SQL = """
WITH windows AS (
  SELECT date_trunc('hour', now()) +
    floor(extract(minute FROM now()) / 15) * interval '15 minutes' AS edge
), samples AS (
  SELECT CASE WHEN created_at >= edge - interval '15 minutes'
              THEN 'latest' ELSE 'previous' END AS "window",
         COALESCE(diagnostics->>'phase', task) AS phase,
         COALESCE(provider, 'unknown') AS provider, model,
         diagnostics->>'completion_reason' AS reason,
         source_query_id, diagnostics->>'error_class' AS error_class
  FROM llm_usage CROSS JOIN windows
  WHERE created_at >= edge - interval '30 minutes' AND created_at < edge
    AND diagnostics IS NOT NULL
    AND source_query_id IS NOT NULL
    AND task IN ('jev_ask', 'jev_ask_errored', 'jev_judge', 'jev_judge_errored')
), grouped AS (
  SELECT "window", phase, provider, model,
    count(*) FILTER (WHERE reason IN ('success', 'provider_error', 'deadline', 'phase_deadline')) AS calls,
    count(*) FILTER (WHERE reason IN ('provider_error', 'deadline', 'phase_deadline')) AS failures,
    count(*) FILTER (WHERE reason = 'caller_cancelled') AS cancelled,
    count(DISTINCT source_query_id) AS queries,
    count(DISTINCT source_query_id) FILTER
      (WHERE reason IN ('provider_error', 'deadline', 'phase_deadline')) AS affected_queries,
    array_agg(DISTINCT error_class) FILTER (WHERE error_class IS NOT NULL) AS error_classes
  FROM samples GROUP BY "window", phase, provider, model
)
SELECT COALESCE(json_agg(grouped), '[]'::json) FROM grouped;
"""


def connection_env(dsn):
    # PGDATABASE alone does not expand a URI. Decode into libpq environment
    # variables so credentials never appear in the process argument list.
    url = urlsplit(dsn)
    if url.scheme not in ("postgres", "postgresql"):
        raise RuntimeError("PostgreSQL URI required")
    env = dict(os.environ, PGDATABASE=unquote(url.path.lstrip('/')))
    for key, value in (("PGHOST", url.hostname), ("PGPORT", url.port),
                       ("PGUSER", unquote(url.username) if url.username else None),
                       ("PGPASSWORD", unquote(url.password) if url.password else None)):
        if value is not None:
            env[key] = str(value)
    parameters = dict(sslmode="PGSSLMODE", channel_binding="PGCHANNELBINDING",
                      sslrootcert="PGSSLROOTCERT", sslcert="PGSSLCERT", sslkey="PGSSLKEY",
                      application_name="PGAPPNAME",
                      connect_timeout="PGCONNECT_TIMEOUT", target_session_attrs="PGTARGETSESSIONATTRS")
    for key, value in parse_qsl(url.query):
        if key not in parameters:
            raise RuntimeError("unsupported PostgreSQL connection option")
        env[parameters[key]] = value
    env["PGCONNECT_TIMEOUT"] = "10"
    # Neon poolers reject the startup `options` parameter; configure safety
    # inside the transaction instead. Do not inherit an operator's PGOPTIONS.
    env.pop("PGOPTIONS", None)
    return env


class QueryFailed(RuntimeError):
    """A psql failure reduced to a fixed vocabulary, so output never carries server text."""

    def __init__(self, reason):
        super().__init__("database query failed (check connectivity, read permission, and diagnostics migration)")
        self.reason = reason


FAILURE_REASONS = (
    ("connect_timeout", ("timeout expired",)),
    ("authentication", ("authentication failed", "no pg_hba.conf entry")),
    ("permission", ("permission denied",)),
    ("missing_relation", ("does not exist",)),
    ("statement_timeout", ("statement timeout",)),
    ("connection", ("could not connect", "could not translate host", "connection refused",
                    "connection to server", "server closed the connection", "terminating connection")),
)


def failure_reason(stderr):
    text = stderr.lower()
    for reason, needles in FAILURE_REASONS:
        if any(needle in text for needle in needles):
            return reason
    return "query_error"


def psql_json_once(dsn, sql):
    env = connection_env(dsn)
    proc = subprocess.run([os.environ.get("PSQL", "psql"), "-X", "-A", "-t", "-q",
                           "-v", "ON_ERROR_STOP=1"],
                          input="BEGIN READ ONLY; SET LOCAL statement_timeout='30s'; "
                                "SET LOCAL row_security=off; " + sql + "COMMIT;", env=env,
                          capture_output=True, text=True, timeout=45)
    if proc.returncode:
        # Do not print PostgreSQL stderr: a connection failure may expose DSNs.
        raise QueryFailed(failure_reason(proc.stderr))
    return json.loads(proc.stdout)


def psql_json(dsn, sql, attempts=2):
    # A pooled serverless database can miss the 10s connect timeout while its
    # compute resumes. One short retry keeps a transient blip from reading as
    # "monitoring incomplete"; a persistent failure still raises after the last try.
    for attempt in range(attempts):
        try:
            return psql_json_once(dsn, sql)
        except (QueryFailed, subprocess.TimeoutExpired):
            if attempt + 1 == attempts:
                raise
            time.sleep(float(os.environ.get("ASK_PSQL_RETRY_DELAY", "5")))


def sustained_failures(rows):
    groups = {}
    for row in rows:
        key = tuple(row[field] for field in ("phase", "provider", "model"))
        groups.setdefault(key, {})[row["window"]] = row
    return [key for key, windows in groups.items()
            if all(window in windows and windows[window]["calls"] >= 20
                   and windows[window]["failures"] >= 5
                   and windows[window]["failures"] / windows[window]["calls"] >= .20
                   for window in ("previous", "latest"))]


def resolve_dsn(secret):
    if not re.fullmatch(r"TENANT_DB_[A-Z0-9_]+", secret):
        raise RuntimeError("invalid tenant secret reference")
    if os.environ.get(secret):
        return os.environ[secret]
    resolver = os.environ.get("ASK_DSN_RESOLVER")
    if not resolver:
        raise RuntimeError("tenant DSN resolver is missing")
    proc = subprocess.run([resolver, secret], capture_output=True, text=True, timeout=45)
    dsn = proc.stdout.strip()
    if proc.returncode or not dsn.startswith(("postgres://", "postgresql://")):
        raise RuntimeError("tenant DSN resolution failed")
    return dsn


def self_test():
    base = dict(phase="pack", provider="openrouter", model="model", calls=25, failures=5)
    rows = [dict(base, window=w) for w in ("previous", "latest")]
    assert len(sustained_failures(rows)) == 1
    assert not sustained_failures(rows[:1])
    assert not sustained_failures([rows[0], dict(rows[1], failures=4)])
    assert not sustained_failures([dict(r, calls=26) for r in rows])
    assert not sustained_failures([dict(r, calls=19) for r in rows])
    assert not sustained_failures([rows[0], dict(rows[1], phase="cite")])
    retry_self_test()
    print("Ask monitor threshold, consecutive-window and transient-retry checks passed")


def retry_self_test():
    # A fake psql: fails `failures` times (a connect timeout), then returns an empty window.
    with tempfile.TemporaryDirectory() as work:
        counter = os.path.join(work, "calls")
        fake = os.path.join(work, "fake-psql")
        with open(fake, "w") as handle:
            handle.write('#!/bin/sh\ncat >/dev/null\nn=$(cat "$COUNTER" 2>/dev/null || echo 0)\n'
                         'echo $((n + 1)) > "$COUNTER"\n'
                         'if [ "$n" -lt "$FAILURES" ]; then echo "connection timeout expired" >&2; exit 2; fi\n'
                         "echo '[]'\n")
        os.chmod(fake, 0o700)
        saved = {key: os.environ.get(key) for key in ("PSQL", "COUNTER", "FAILURES", "ASK_PSQL_RETRY_DELAY")}
        os.environ.update(PSQL=fake, COUNTER=counter, ASK_PSQL_RETRY_DELAY="0")
        dsn = "postgres://user:secret@127.0.0.1:5432/db"
        try:
            def calls():
                with open(counter) as handle:
                    return int(handle.read())
            os.environ["FAILURES"] = "1"
            assert psql_json(dsn, "SELECT 1;") == [] and calls() == 2
            os.remove(counter)
            os.environ["FAILURES"] = "2"
            try:
                psql_json(dsn, "SELECT 1;")
                raise AssertionError("a persistent failure must still raise")
            except QueryFailed as error:
                assert error.reason == "connect_timeout" and calls() == 2
        finally:
            for key, value in saved.items():
                os.environ.pop(key, None) if value is None else os.environ.__setitem__(key, value)
    assert failure_reason("FATAL: password authentication failed for user") == "authentication"
    assert failure_reason("ERROR: relation \"llm_usage\" does not exist") == "missing_relation"
    assert failure_reason("something unexpected") == "query_error"


def database_self_test():
    # Explicit disposable/local test DSN; never infer the production DSN.
    dsn = os.environ.get("ASK_TEST_DATABASE_URL")
    if not dsn:
        raise RuntimeError("ASK_TEST_DATABASE_URL must be explicitly configured")
    fixture = """
BEGIN;
SET LOCAL statement_timeout='30s';
CREATE TEMP TABLE llm_usage (
  created_at timestamptz, diagnostics jsonb, task text, provider text,
  model text, source_query_id uuid
);
INSERT INTO llm_usage
SELECT date_trunc('hour', now()) + floor(extract(minute FROM now()) / 15)
         * interval '15 minutes' - w * interval '15 minutes' + interval '1 minute',
       jsonb_build_object('phase', 'pack', 'completion_reason',
         CASE WHEN n <= 2 THEN 'provider_error' WHEN n <= 4 THEN 'deadline'
              WHEN n = 5 THEN 'phase_deadline' ELSE 'success' END,
         'error_class', CASE WHEN n <= 5 THEN 'upstream' ELSE NULL END),
       CASE WHEN n <= 5 THEN 'jev_ask_errored' ELSE 'jev_ask' END,
       'openrouter', 'fixture-model', '00000000-0000-0000-0000-000000000001'::uuid
FROM generate_series(1, 2) w CROSS JOIN generate_series(1, 25) n;
INSERT INTO llm_usage
SELECT created_at, jsonb_build_object('phase', 'pack', 'completion_reason', 'caller_cancelled'),
       'jev_ask_errored', provider, model, source_query_id FROM llm_usage;
INSERT INTO llm_usage
SELECT created_at, diagnostics, 'extract', provider, model, source_query_id
FROM llm_usage;
INSERT INTO llm_usage
SELECT created_at, diagnostics, 'jev_judge', provider, model, NULL
FROM llm_usage;
"""
    env = connection_env(dsn)
    proc = subprocess.run([os.environ.get("PSQL", "psql"), "-X", "-A", "-t", "-q",
                           "-v", "ON_ERROR_STOP=1"], input=fixture + SUMMARY_SQL + "ROLLBACK;",
                          env=env, capture_output=True, text=True, timeout=45)
    if proc.returncode:
        raise RuntimeError("temporary database fixture failed")
    rows = json.loads(proc.stdout)
    assert len(rows) == 2
    assert all(r['calls'] == 25 and r['failures'] == 5 and r['cancelled'] == 25 for r in rows)
    assert len(sustained_failures(rows)) == 1
    print("Ask SQL fixture passed: errored tasks counted, cancellations excluded, non-Ask tasks ignored")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--all-tenants", action="store_true")
    parser.add_argument("--self-test", action="store_true")
    parser.add_argument("--self-test-database", action="store_true")
    args = parser.parse_args()
    if args.self_test:
        self_test()
        return 0
    if args.self_test_database:
        database_self_test()
        return 0
    control = os.environ.get("DATABASE_URL")
    if not control:
        print("DATABASE_URL must be configured", file=sys.stderr)
        return 2
    failed = False
    incomplete = False

    def check(label, dsn):
        nonlocal failed, incomplete
        try:
            rows = psql_json(dsn, SUMMARY_SQL)
            alarms = sustained_failures(rows)
            failed = failed or bool(alarms)
            print(json.dumps(dict(database=label, coverage="observed" if rows else "no_recent_samples",
                                  samples=rows, sustained_failures=alarms)))
        except (RuntimeError, OSError, ValueError, subprocess.TimeoutExpired) as error:
            incomplete = True
            print(json.dumps(dict(database=label, monitoring_error=type(error).__name__,
                                  reason=getattr(error, "reason", "timeout" if isinstance(error, subprocess.TimeoutExpired) else "unexpected"))))

    check("control", control)
    if args.all_tenants:
        try:
            tenants = psql_json(control, "SELECT COALESCE(json_agg(t), '[]'::json) FROM "
                "(SELECT slug, db_secret_ref FROM tenant_databases WHERE status = 'active') t;")
            if not tenants:
                raise RuntimeError("tenant enumeration unexpectedly empty")
            for tenant in tenants:
                try:
                    check(tenant["slug"], resolve_dsn(tenant["db_secret_ref"]))
                except (RuntimeError, OSError, subprocess.TimeoutExpired):
                    incomplete = True
                    print(json.dumps(dict(database=tenant["slug"], monitoring_error="tenant_resolution_failed")))
        except (RuntimeError, OSError, ValueError, subprocess.TimeoutExpired):
            incomplete = True
            print("tenant enumeration failed", file=sys.stderr)
    return 2 if incomplete else 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
