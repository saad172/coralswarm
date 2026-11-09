#!/usr/bin/env python3
"""Fail when a Fly Postgres app is unhealthy or was recently OOM-killed.

Usage: check_fly_db_health.py APP [APP ...]
       check_fly_db_health.py --self-test

Reads `flyctl machine list -a APP --json` (read-only). An app is unhealthy if
it has no machines, a machine is not `started`, a machine has no health
checks or any check that is not `passing`, or a machine event in the last
OOM_WINDOW_HOURS reports `oom_killed: true`. Exit 0 healthy, 1 unhealthy,
2 when flyctl itself failed (the check did not run).

Postgres can die inside a machine that Fly still reports as `started`
(2026-10-29: the kernel OOM-killed it on coralswarm-db-staging and it stayed
down 14 hours), so the `pg` check is the primary signal; the OOM event covers
a guest that was killed and restarted between two runs.

A failing check is read again up to RECHECKS times, RECHECK_SECONDS apart, and
reported only if it is still failing. The `vm` check goes critical for a minute
or two on a burst of CPU steal (coralswarm-db, 2026-10-29), and an hourly alarm
that fires on those gets ignored.
"""

import json
import subprocess
import sys
import time

OOM_WINDOW_HOURS = 24
RECHECKS = 3
RECHECK_SECONDS = 60


def has_oom(value):
    if isinstance(value, dict):
        return value.get("oom_killed") is True or any(has_oom(v) for v in value.values())
    if isinstance(value, list):
        return any(has_oom(v) for v in value)
    return False


def current_problems(machines):
    if not machines:
        return ["no machines"]
    found = []
    for m in machines:
        mid = m.get("id", "?")
        if m.get("state") != "started":
            found.append(f"machine {mid} is {m.get('state')!r}, not started")
        checks = m.get("checks") or []
        if not checks:
            found.append(f"machine {mid} reports no health checks")
        for c in checks:
            if c.get("status") != "passing":
                output = (c.get("output") or "").replace("\n", " | ")
                found.append(f"machine {mid} check {c.get('name')} is {c.get('status')}: {output}")
    return found


def oom_kills(machines, now_ms):
    since = now_ms - OOM_WINDOW_HOURS * 3600 * 1000
    found = []
    for m in machines:
        for e in m.get("events") or []:
            if e.get("timestamp", 0) >= since and has_oom(e):
                at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(e["timestamp"] / 1000))
                found.append(f"machine {m.get('id', '?')} was OOM-killed at {at}")
    return found


def summary(machines):
    for m in machines:
        checks = ", ".join(f"{c.get('name')}={c.get('status')}" for c in m.get("checks") or [])
        mem = (m.get("config") or {}).get("guest", {}).get("memory_mb")
        yield f"  {m.get('id')} state={m.get('state')} memory_mb={mem} checks: {checks or 'none'}"


def read_machines(app):
    run = subprocess.run(["flyctl", "machine", "list", "-a", app, "--json"], capture_output=True, text=True)
    if run.returncode != 0:
        raise ValueError(run.stderr.strip())
    return json.loads(run.stdout)


def check_app(app, read=read_machines, sleep=time.sleep, now=lambda: int(time.time() * 1000)):
    """Return (machines, problems) from the last read."""
    for attempt in range(RECHECKS + 1):
        if attempt:
            sleep(RECHECK_SECONDS)
        machines = read(app)
        problems = current_problems(machines)
        if not problems:
            break
        print(f"{app}: read {attempt + 1}: " + "; ".join(problems))
    return machines, problems + oom_kills(machines, now())


def self_test():
    now = 1_800_000_000_000
    ok = {"id": "m1", "state": "started",
          "checks": [{"name": "pg", "status": "passing"}, {"name": "vm", "status": "passing"}],
          "events": [{"type": "start", "status": "started", "timestamp": now - 1000}]}
    assert current_problems([ok]) == [] and oom_kills([ok], now) == []
    assert current_problems([]) == ["no machines"]
    down = dict(ok, checks=[{"name": "pg", "status": "critical", "output": "connection refused"}])
    assert current_problems([down]) == ["machine m1 check pg is critical: connection refused"]
    assert current_problems([dict(ok, state="stopped")]) == ["machine m1 is 'stopped', not started"]
    assert current_problems([dict(ok, checks=[])]) == ["machine m1 reports no health checks"]
    oom = {"type": "exit", "status": "stopped", "timestamp": now - 3600_000,
           "request": {"exit_event": {"exit_code": 137, "oom_killed": True}}}
    assert oom_kills([dict(ok, events=[oom])], now) == ["machine m1 was OOM-killed at 2027-01-15T07:00:00Z"]
    assert oom_kills([dict(ok, events=[dict(oom, timestamp=now - 25 * 3600_000)])], now) == []
    clean_exit = dict(oom, request={"exit_event": {"exit_code": 0, "oom_killed": False}})
    assert oom_kills([dict(ok, events=[clean_exit])], now) == []

    def reads(*results):
        it = iter(results)
        return lambda app: next(it)
    no_sleep = lambda s: None
    # A blip that clears on a re-read is not reported; one that persists is.
    assert check_app("a", reads([down], [ok]), no_sleep, lambda: now)[1] == []
    _, persisting = check_app("a", reads(*[[down]] * (RECHECKS + 1)), no_sleep, lambda: now)
    assert persisting == ["machine m1 check pg is critical: connection refused"]
    # An OOM kill is history, not a blip: reported without waiting.
    assert check_app("a", reads([dict(ok, events=[oom])]), no_sleep, lambda: now)[1][0].startswith("machine m1 was OOM")
    print("check_fly_db_health self-test: ok")


def main(apps):
    if apps == ["--self-test"]:
        self_test()
        return 0
    if not apps:
        print(__doc__, file=sys.stderr)
        return 2
    status = 0
    for app in apps:
        try:
            machines, problems = check_app(app)
        except ValueError as err:
            print(f"::error::{app}: could not read machine status, so its health is unknown: {err}")
            status = max(status, 2)
            continue
        print(f"{app}:")
        print("\n".join(summary(machines)))
        for p in problems:
            print(f"::error::{app}: {p}")
        if problems:
            status = max(status, 1)
    return status


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
