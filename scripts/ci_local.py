#!/usr/bin/env python3
"""Run this repo's GitHub Actions CI locally, natively, with no GitHub minutes.

WHY THIS EXISTS
---------------
CI on GitHub is being switched off deliberately, to stop paying for it — not
because a quota blocked us. (This file originally said otherwise; the premise
was wrong and went unverified for hours. Actions was running the whole time.)

Turning CI off removes two things. The obvious one is validation. The subtler
one was deploys: `deploy.yml` fired on a *successful* CI `workflow_run` (it is
now a manual dispatch that does not wait for CI), so with CI disabled nothing
shipped either — including migrations, which fly.toml applies
through `release_command = '/app/release.sh'`. Merging was never affected;
`main` carries no branch protection and no rulesets.

So the pieces are this runner, `scripts/deploy_local.sh`, and the pre-push hook
in `scripts/githooks/` that ties them together.

Only the push-triggered workflows go away. `backup-control-plane.yml` (daily),
`chain-health-check.yml` (hourly), and `retrieval-eval-ort.yml` (weekly) run on
cron and MUST stay enabled — a laptop is not always on, and the first of those
is the only automated control-plane backup that exists.

WHY AN INTERPRETER RATHER THAN A TRANSCRIPTION
----------------------------------------------
ci.yml is ~3100 lines and ~170 `run:` steps across 32 jobs. Hand-copying those
commands into a shell script creates a second source of truth that silently
drifts from CI the first time someone edits the workflow. So this reads
`.github/workflows/ci.yml` and executes the real `run:` blocks. Edit ci.yml and
this runner picks the change up for free.

This is viable here because of two properties of this particular workflow,
both verified rather than assumed:

  * **CI consumes zero secrets.** `grep -oE 'secrets\\.[A-Z_]+' ci.yml` returns
    nothing. Every Postgres job stands up a throwaway `pgvector/pgvector:pg16`
    and points `ORGT_TEST_BASE_URL` at it. Nothing here needs credentials.
  * Only the PR `BASE_REF`, exact declared `env.NAME` aliases and the known
    PostgreSQL service-ID adapter are resolved locally. Other expressions
    are not a general Actions interpreter.

WHAT IS DELIBERATELY NOT REPRODUCED
-----------------------------------
  * `actions/checkout`, `dtolnay/rust-toolchain`, `Swatinem/rust-cache`,
    `actions/cache`, `actions/upload-artifact` — runner provisioning with no
    local meaning. Your working tree is the checkout and your `target/` is
    the cache (a *better* cache: it persists between runs). `setup-node` does
    not install anything locally: its selected literal major binds to a
    validated local sibling runtime before resource acquisition.
  * Ubuntu housekeeping (`sudo rm -rf` disk reclaim, `apt-get install`, `df -h`).
    Those exist because hosted runners have ~14 GB free; your laptop does not
    have that problem. Tools the apt steps would have installed are checked for
    in preflight instead, so a missing one is a clear error rather than a
    confusing failure 20 minutes in.
  * `changes` — replaced by an equivalent local path filter (see `--changed`).
  * `warm-backend-cache` — its entire job is populating the `backend-pg` cargo
    cache for the 13 Postgres jobs that restore it. Locally they all share one
    `target/`, so it is a no-op.

THE FAILURE MODE THIS RUNNER IS BUILT TO CATCH
----------------------------------------------
Every Postgres-backed test in this repo is env-gated in the same shape:

    let Some(base) = base_url() else {
        eprintln!("skipping <test>: set ORGT_TEST_BASE_URL");
        return;                      // <-- returns, so the test PASSES
    };

There are ~540 such gates (438 on ORGT_TEST_BASE_URL, 102 on
RELINFER_TEST_DATABASE_URL). If the DSN is unset or unreachable, the entire
Postgres surface of CI reports **green while asserting nothing**. On a hosted
runner the `services:` block makes that near-impossible; on a laptop it is the
default outcome.

Catching it is less obvious than it looks, and the obvious way does not work.
Scanning test output for those `skipping …` lines FAILS SILENTLY, because
libtest captures stdout/stderr of *passing* tests and prints it only for
failures. Measured on `tenant_registry_pg`:

    with a database:     running 5 tests … ok. 5 passed … finished in 1.53s
    without a database:  running 5 tests … ok. 5 passed … finished in 0.00s

Byte-identical but for the timing. The skip lines exist, but only surface under
`--nocapture`, which CI does not pass. An output scan would have reported green.

So the real guard runs BEFORE the tests, not after: for every Postgres job,
each DSN-shaped env var is probed from the host with psql and must answer
`select 1` and hold CREATEDB + CREATEROLE (the tests CREATE DATABASE and CREATE
ROLE; without those rights they fail in ways that look like product bugs). If
the exact string the tests will read works, `base_url()` returns Some and the
gates cannot trigger. The output scan is kept as a cheap second net for the
cases where output *is* shown. `--allow-skips` opts out for a deliberate
no-database run; `--self-test` exercises both.
"""

from __future__ import annotations

import argparse
import atexit
import concurrent.futures
import hashlib
import json
import os
import queue
import re
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from types import MappingProxyType

import yaml

# On Windows, Python's stdout/stderr encoding follows the process's ambient
# console codepage (cp1252, cp437, ...) rather than UTF-8 whenever it isn't
# already forced some other way — and that codepage is caller-dependent, not
# something this script controls: the exact same run has been observed to
# print the ✓/✗/▶/→ glyphs used throughout this file's output just fine in
# one invocation and crash with `UnicodeEncodeError` on the first '→' in
# another, purely because of how the parent process launched Python. Forcing
# UTF-8 here removes the dependency on that ambient state entirely, on every
# platform (a no-op where stdout is already UTF-8).
for _stream in (sys.stdout, sys.stderr):
    if hasattr(_stream, "reconfigure"):
        _stream.reconfigure(encoding="utf-8", errors="replace")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

WORKFLOW = ".github/workflows/ci.yml"

# 5432 is normally taken by a Homebrew postgres, and 5433/55987 by the
# platform-postgres and cs-eval-pg containers. Default somewhere unused, and
# rewrite the workflow's hardcoded :5432 DSNs to match (see rewrite_dsn).
DEFAULT_PG_PORT = 5442
PG_CONTAINER = "cs-localci-pg"
PG_IMAGE = "pgvector/pgvector:pg16"

# Unlike Postgres, MinIO's container name is not user-configurable (no
# `--minio-port` escape hatch) — every job that declares a `minio` service in
# ci.yml hardcodes the SAME host port (19000) straight into its `env:` R2_TEST_*
# values rather than templating it, so there is nothing for this runner to
# rewrite the way `rewrite_dsn` rewrites Postgres DSNs. One shared container,
# started on whatever port the job's own YAML declares, is therefore both
# sufficient and the only shape that keeps a job's real env values correct
# with zero rewriting.
MINIO_CONTAINER = "cs-localci-minio"

# "DID ANY JOB ACTUALLY RUN?" — the signal a caller needs before it publishes
# this run's verdict anywhere durable.
#
# The exit code cannot answer that question. Every environment gate in here
# aborts with status 1, which is the same status a genuinely failing test
# suite produces: not a repo checkout (`repo_root`), no bash >= 4
# (`modern_bash`), a missing toolchain (`preflight`), Docker unreachable or a
# container that never came up (`ensure_pg`), another run holding the lock
# (`RunLock.acquire`), an unknown `--job` name. None of those is a verdict on
# the code, and the pre-push hook publishing a red `local-ci` commit status
# for any of them is a lie that outlives the terminal it was printed in.
#
# So the signal is POSITIVE and out-of-band rather than a set of reserved
# exit codes: when $CI_LOCAL_RAN_MARKER names a path, this script creates
# that file at the instant execution begins and never one line earlier (see
# `mark_run_started`). A caller publishes a verdict only if the file exists.
#
# Positive, because absence is then the safe default and the class is closed
# by construction: it covers not only the aborts listed above but an
# unhandled traceback during selection, a Ctrl-C while Docker is starting,
# and any early exit added here in future by someone who never reads this
# comment. A reserved exit code would have to be threaded through each of
# those by hand, and the one that got missed would fail toward a false red.
RAN_MARKER_ENV = "CI_LOCAL_RAN_MARKER"

# Longest-running jobs first, so the pool is not left waiting on a straggler it
# started last. Measured on a warm 603s sequential run, not guessed:
# cross-tenant-probe alone is 194s of it, and the parallel floor is whatever
# that job takes. Anything not listed sorts after these and is short enough
# that its position does not matter.
SLOW_FIRST = [
    "cross-tenant-probe",   # 194s
    "member-oceans",        #  75s
    "rls-probe-harness",    #  74s
    "rust",                 #  59s
    "platform-rollup",      #  30s
    "mcp-e2e",              #  25s
    "migrator-dsn-split",   #  21s
    "web",                  #  18s
    "marketing",            #  14s
]

# Jobs with no local meaning. Reported in the summary as skipped-with-reason,
# never dropped quietly — a runner that hides what it didn't do is how you end
# up believing you ran CI when you ran a third of it.
UNRUNNABLE = {
    "deploy-check": "decides whether GitHub's nightly run proceeds; a local run always does",
    "changes": "replaced by this runner's own path filter",
    "warm-backend-cache": "warms the CI cargo cache; local target/ already is one",
}

# Jobs that cannot run in place, mapped to the paths their checkout needs.
#
# `marketing` is here because Node resolution walks up out of the repo and finds
# the main checkout's node_modules — see warn_node_resolution_leak(). That
# resolves an eslint `marketing-web` never declares, which then lints with the
# repo-root eslint.config.js whose react-hooks plugin is absent, and the build
# dies on a rule CI never even evaluates. Left alone this job is red on every
# run, and a permanently-red job is worse than no job: it teaches you to ignore
# the colour.
#
# tsconfig.base.json travels with it because marketing-web/tsconfig.json
# extends `../tsconfig.base.json`.
ISOLATED_JOBS: dict[str, tuple[list[str], str]] = {
    "marketing": (
        ["marketing-web", "tsconfig.base.json"],
        "building in place would resolve the parent checkout's eslint, which a runner has no access to",
    ),
}

# Jobs whose `cargo test` STEPS have been individually audited as fully
# independent of each other — each one creates its OWN scratch DB via
# CREATE ROLE/CREATE DATABASE and shares no fixture, env, or filesystem state
# with its siblings — so `main`'s job-level queue may hand different steps of
# the SAME job to different lanes to run concurrently, rather than the whole
# job running as one sequential unit on a single lane.
#
# This is opt-in, not assumed, on purpose: an un-audited job that turns out to
# have a real cross-step dependency (a fixture step 2 writes that step 9
# reads) would silently produce a false pass if split — step 9 would just
# never find the row, on a database step 2 never touched. Add a job here only
# after confirming every one of its steps is independently self-contained
# (grep its `run:` commands in ci.yml for `CREATE ROLE`/`CREATE DATABASE`, or
# an equivalent per-test scratch-DB helper, on EVERY step — not just most).
#
# The current split path only supports jobs shaped like this one: plain
# `cargo test` steps, no `boots_server` (readiness-curl) step and no
# `ISOLATED_JOBS` entry. A future candidate needing either would need
# `run_split_step`'s setup extended first, not just an allowlist entry.
SPLITTABLE_JOBS: dict[str, str] = {
    "cross-tenant-probe": (
        "audited 2026-08-18: every step is its own `cargo test --test/--lib "
        "<binary>` invocation against its own CREATE ROLE/CREATE DATABASE "
        "scratch DB (see ci.yml's own header comment on this job, which "
        "states the same independence claim for a different reason — disk, "
        "not concurrency). Confirmed the -- --test-threads=1 on each is about "
        "a cluster-wide CREATE ROLE race WITHIN one Postgres cluster (migration "
        "0083), not about the steps depending on each other."
    ),
}

# Where longest-first queue ordering's duration history persists — same
# convention as TARGET_DIR/RunLock's lock file (Path.home()/.cache), i.e.
# machine-local and gitignored, since a duration is only meaningful for the
# machine it was measured on.
DURATION_CACHE = Path(
    os.environ.get("CI_LOCAL_DURATION_CACHE", str(Path.home() / ".cache" / "coralswarm-ci-job-durations.json"))
)


def load_durations() -> dict[str, float]:
    """job-id or "job-id::step-label" -> last observed seconds. Missing or
    corrupt cache reads as empty (unknown durations just sort first — see
    `queue_key` — rather than failing the whole run over a cache file).
    """
    try:
        return json.loads(DURATION_CACHE.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}


def save_durations(durations: dict[str, float]) -> None:
    try:
        DURATION_CACHE.parent.mkdir(parents=True, exist_ok=True)
        DURATION_CACHE.write_text(json.dumps(durations, indent=2, sort_keys=True), encoding="utf-8")
    except OSError:
        pass  # best-effort — next run just falls back to unknown-sorts-first

# Runner-provisioning `run:` steps: disk reclaim, apt installs, disk probes.
# Anchored on the actual command, not the step name, so a renamed step does not
# slip an ubuntu-only command into the local run.
UBUNTU_ONLY = re.compile(r"(^|\n)\s*(sudo\s|df\s+-h|echo\s+\"===\s+disk)")

# The env-gate skip lines described in the module docstring.
SKIP_LINE = re.compile(
    r"^\s*skipping\b.*\bset (ORGT_TEST_BASE_URL|RELINFER_TEST_DATABASE_URL|DATABASE_URL)\b",
    re.MULTILINE,
)

# `if:` expressions we understand. Anything else is surfaced rather than guessed at.
IF_ALWAYS = re.compile(r"\balways\(\)")
IF_FAILURE = re.compile(r"\bfailure\(\)")
IF_CACHE_MISS = re.compile(r"steps\.[\w-]+\.outputs\.cache-hit\s*!=\s*'true'")

# Resolved once in main() by modern_bash(); see that function for why /bin/bash
# is not good enough on macOS.
BASH = "bash"

# One build cache shared by every local CI run, in every worktree.
#
# TWO problems this solves. First, per-worktree `target/` directories are
# enormous here — the main checkout's is 249 GB and each active worktree adds
# another 10 GB, so running CI from a fresh worktree would otherwise mean both
# a cold ~20-minute compile and another 10 GB on disk every time.
#
# Second, and the reason this is a SEPARATE directory rather than a symlink to
# the main checkout's: ci.yml pins `CARGO_PROFILE_DEV_DEBUG: line-tables-only`
# across the warm-cache writer and all 13 Postgres readers, precisely so they
# share one cache. That setting is part of cargo's fingerprint. Pointing CI
# runs at the same `target/` you use for everyday `cargo build` would make the
# two fight — every switch would invalidate the other's artifacts and rebuild
# from scratch. Isolating the CI fingerprint keeps both caches warm.
#
# Override with CI_LOCAL_TARGET_DIR; set it empty to use each worktree's own.
TARGET_DIR = os.environ.get(
    "CI_LOCAL_TARGET_DIR", str(Path.home() / ".cache" / "coralswarm-ci-target")
)

RESET, BOLD, RED, GREEN, YELLOW, BLUE, DIM = (
    "\033[0m",
    "\033[1m",
    "\033[31m",
    "\033[32m",
    "\033[33m",
    "\033[34m",
    "\033[2m",
)
if not sys.stdout.isatty() or os.environ.get("NO_COLOR"):
    RESET = BOLD = RED = GREEN = YELLOW = BLUE = DIM = ""


@dataclass
class JobResult:
    job: str
    status: str  # pass | fail | skip
    reason: str = ""
    seconds: float = 0.0
    failed_steps: list[str] = field(default_factory=list)
    # True only when a step died from an abnormal process crash (see
    # is_crash_exit) rather than reporting a real test failure — the signal
    # the adaptive-lane retry logic uses to decide whether to requeue this
    # job instead of reporting it failed. A job that ran to completion and
    # genuinely failed a test is never marked crashed, retried, or requeued.
    crashed: bool = False


# ---------------------------------------------------------------------------
# Repo / git helpers
# ---------------------------------------------------------------------------


def sh(cmd: list[str], cwd: Path | None = None) -> str:
    return subprocess.run(
        cmd, cwd=cwd, check=True, capture_output=True, text=True
    ).stdout.strip()


def repo_root() -> Path:
    try:
        return Path(sh(["git", "rev-parse", "--show-toplevel"]))
    except subprocess.CalledProcessError:
        sys.exit("not inside a git repository")


def merge_base(root: Path, base: str) -> str | None:
    """The commit this branch forked from, for path filtering and BASE_REF."""
    for ref in (base, f"origin/{base}"):
        try:
            return sh(["git", "merge-base", "HEAD", ref], cwd=root)
        except subprocess.CalledProcessError:
            continue
    return None


def changed_files(root: Path, base_sha: str | None) -> list[str]:
    """Committed diff vs the fork point, plus uncommitted work.

    Uncommitted files are included on purpose: the whole point of a local gate
    is to run it *before* committing. CI can only ever see pushed commits.
    """
    files: set[str] = set()
    if base_sha:
        files.update(
            sh(["git", "diff", "--name-only", base_sha, "HEAD"], cwd=root).splitlines()
        )
    for line in sh(["git", "status", "--porcelain=1"], cwd=root).splitlines():
        if len(line) < 4:
            continue
        path = line[3:]
        # Renames arrive as `R  old -> new`; both sides matter for path filtering.
        if " -> " in path:
            old, new = path.split(" -> ", 1)
            files.update({old, new})
        else:
            files.add(path)
    return sorted(f for f in files if f)


# ---------------------------------------------------------------------------
# Path filter — the local stand-in for the `changes` job
# ---------------------------------------------------------------------------


def parse_change_filters(wf: dict) -> dict[str, list[str]]:
    """Read the real dorny/paths-filter globs out of the `changes` job.

    Parsed from ci.yml rather than restated here, for the same
    single-source-of-truth reason the whole runner is an interpreter.
    """
    # ci.yml is guard-jobs-only since the CI redesign: no `changes` job, no
    # path-gated lanes. Every remaining job is unconditional.
    for step in wf["jobs"].get("changes", {}).get("steps", []):
        if "dorny/paths-filter" in step.get("uses", ""):
            return {
                k: [str(p) for p in v]
                for k, v in yaml.safe_load(step["with"]["filters"]).items()
            }
    return {}


def glob_to_re(glob: str) -> re.Pattern:
    out, i = "", 0
    while i < len(glob):
        if glob.startswith("**/", i):
            out += "(?:.*/)?"
            i += 3
        elif glob.startswith("**", i):
            out += ".*"
            i += 2
        elif glob[i] == "*":
            out += "[^/]*"
            i += 1
        elif glob[i] == "?":
            out += "[^/]"
            i += 1
        else:
            out += re.escape(glob[i])
            i += 1
    return re.compile(f"^{out}$")


def active_lanes(filters: dict[str, list[str]], files: list[str]) -> set[str]:
    lanes = set()
    for lane, globs in filters.items():
        pats = [glob_to_re(g) for g in globs]
        if any(p.match(f) for f in files for p in pats):
            lanes.add(lane)
    return lanes


def job_needs_lane(job: dict) -> set[str]:
    """Which `changes` outputs a job's `if:` depends on ('' = unconditional)."""
    return set(re.findall(r"needs\.changes\.outputs\.(\w+)\s*==\s*'true'", str(job.get("if", ""))))


# ---------------------------------------------------------------------------
# Postgres
# ---------------------------------------------------------------------------


def link_target_dir(root: Path) -> str | None:
    """Make `<root>/target` resolve to the shared cache, or give up on sharing.

    Jobs do not only invoke cargo — `mcp-e2e` boots the built server by
    RELATIVE PATH (`nohup target/debug/coralswarm-backend &`). With
    CARGO_TARGET_DIR pointed at the shared cache, cargo wrote the binary to
    ~/.cache/... and that line died with "No such file or directory", 91s into
    the job. Rewriting `target/` inside script text would be brittle and would
    have to anticipate every such reference; a symlink fixes all of them at
    once and makes the tree look the way CI's does.

    Returns the target dir to use, or None to let cargo use its default.
    """
    if not TARGET_DIR:
        return None
    link = root / "target"
    shared = Path(TARGET_DIR)
    shared.mkdir(parents=True, exist_ok=True)

    if link.is_symlink():
        if link.resolve() == shared.resolve():
            return TARGET_DIR
        link.unlink()                      # points somewhere stale
    elif link.exists():
        # A real target/ already here (this worktree has been built in
        # directly). Leave it alone and skip sharing rather than delete
        # someone's build cache.
        print(f"{YELLOW}!{RESET} {link} is a real directory — using it instead of the shared cache")
        return None

    link.symlink_to(shared, target_is_directory=True)
    return TARGET_DIR


def modern_bash() -> str:
    """Resolve a bash >= 4, the way ubuntu-latest has it.

    macOS still ships bash 3.2 (2007, GPLv2) as /bin/bash. Several scripts in
    this repo use associative arrays and other bash-4 features and refuse to
    run under 3.2 — `scripts/backup_control_plane.sh --self-test` exits with
    "requires bash >= 4" before asserting anything. Running CI's own steps
    under a shell older than CI's turns a green workflow into a local red for
    a reason that has nothing to do with the code under test, so prefer a real
    bash and say so plainly when there isn't one.
    """
    for cand in ("/opt/homebrew/bin/bash", "/usr/local/bin/bash", shutil.which("bash")):
        if not cand or not Path(cand).exists():
            continue
        try:
            v = sh([cand, "-c", "echo ${BASH_VERSINFO[0]}"])
            if int(v) >= 4:
                return cand
        except (subprocess.CalledProcessError, ValueError):
            continue
    sys.exit(
        f"{RED}no bash >= 4 found{RESET} — macOS's /bin/bash is 3.2 and several "
        f"repo scripts require 4+ (ubuntu runners have 5.x).\n  fix: brew install bash"
    )


def docker_ok() -> bool:
    return shutil.which("docker") is not None and subprocess.run(
        ["docker", "info"], capture_output=True
    ).returncode == 0


class PgStartupFailed(Exception):
    """Raised instead of exiting when `ensure_pg(required=False)`'s container
    never becomes healthy — see that parameter's own doc.
    """


def ensure_pg(port: int, quiet: bool = False, container: str = PG_CONTAINER, required: bool = True) -> None:
    """Start (or reuse) the throwaway pgvector the Postgres jobs need.

    Mirrors the `services.postgres` block in ci.yml: same image, same
    postgres/postgres credentials, same health gate before anything runs.

    `required=True` (the default, and every call site except adaptive
    ramp-up's) is a HARD failure — `sys.exit`, same as always: if the base
    lane's own Postgres can't come up, or Docker itself is down, there is no
    way to run anything, so crashing loudly and immediately is correct.

    `required=False` is for a RAMP-UP-ADDED lane specifically: a `sys.exit`
    here runs on the MAIN thread (ramp-up's poll loop calls `start_lane`
    directly, not from inside a lane thread) and previously killed the
    ENTIRE process — including every other lane's already-in-progress work —
    over one additional container failing to start. Observed for real: RAM
    stayed abundant so ramp-up raced to 10 lanes within ~50s, and Docker
    itself couldn't keep up — the 10th container's health check timed out
    and took the whole run down with it, discarding nine lanes' completed
    results. `required=False` raises `PgStartupFailed` instead, which
    `start_lane` catches: that one lane just doesn't start, ramp-up stops
    there, and everything already running keeps running.
    """
    if not docker_ok():
        if not required:
            raise PgStartupFailed("docker is not reachable")
        sys.exit(
            f"{RED}docker is not running{RESET} — the Postgres jobs need it "
            f"(ci.yml stands up {PG_IMAGE} as a service). Start Docker Desktop, "
            f"or select non-Postgres jobs only."
        )

    existing = subprocess.run(
        ["docker", "ps", "-aq", "-f", f"name=^{container}$"],
        capture_output=True,
        text=True,
    ).stdout.strip()

    if existing:
        # A container's port mapping is fixed at creation. If a previous run
        # used a different --pg-port, reusing it would publish on the OLD port
        # while every DSN points at the new one — which surfaces as a
        # connection failure that looks like Docker being broken. Recreate.
        # Space-separated, then de-duplicated: docker publishes a separate
        # binding per address family, so a single `-p 5442:5432` inspects as
        # two entries. Concatenating them yields "54425442", which matches no
        # port and silently recreates the container on every single run.
        mapped = {
            p for p in subprocess.run(
                ["docker", "inspect", "-f",
                 "{{range $p, $c := .NetworkSettings.Ports}}{{range $c}}{{.HostPort}} {{end}}{{end}}",
                 container],
                capture_output=True, text=True,
            ).stdout.split() if p
        }
        if mapped and str(port) not in mapped:
            if not quiet:
                print(f"{YELLOW}==>{RESET} {container} is published on :{','.join(sorted(mapped))}, not :{port} — recreating")
            subprocess.run(["docker", "rm", "-f", container], capture_output=True)
            existing = ""
        else:
            running = subprocess.run(
                ["docker", "ps", "-q", "-f", f"name=^{container}$"],
                capture_output=True,
                text=True,
            ).stdout.strip()
            if not running:
                subprocess.run(["docker", "start", container], capture_output=True, check=True)

    if not existing:
        if not quiet:
            print(f"{BLUE}==>{RESET} starting {container} ({PG_IMAGE}) on :{port}")
        subprocess.run(
            [
                "docker", "run", "-d", "--name", container,
                "-e", "POSTGRES_PASSWORD=postgres",
                "-e", "POSTGRES_DB=postgres",
                "-p", f"{port}:5432",
                PG_IMAGE,
            ],
            capture_output=True,
            check=True,
        )

    # ci.yml gates on `pg_isready -U postgres` with 10 retries; do the same.
    for _ in range(60):
        if subprocess.run(
            ["docker", "exec", container, "pg_isready", "-U", "postgres"],
            capture_output=True,
        ).returncode == 0:
            break
        time.sleep(1)
    else:
        if not required:
            raise PgStartupFailed(f"{container} never became ready")
        sys.exit(f"{RED}{container} never became ready{RESET}")

    # Health check, not just liveness. The tests CREATE DATABASE and CREATE
    # ROLE; if that is refused they take the env-gate branch and pass vacuously.
    #
    # A genuinely separate race from the pg_isready loop above, not just a
    # slower version of it — observed for real, running ramp-up: pg_isready
    # can report the container's listener socket up while Postgres hasn't
    # finished enough of its own startup sequence to execute a real query
    # yet, so a single immediate probe right after pg_isready succeeds can
    # lose that race (seen failing on lane 2, not just under the higher lane
    # counts that motivated `required=False` below — this is a timing issue,
    # not primarily a resource one). A few retries fixes the race itself;
    # `required=False` is the belt-and-braces fallback for whatever this
    # doesn't catch.
    for attempt in range(5):
        probe = subprocess.run(
            ["docker", "exec", container, "psql", "-U", "postgres", "-tAc", "select 1"],
            capture_output=True,
            text=True,
        )
        if probe.returncode == 0 and probe.stdout.strip() == "1":
            break
        if attempt < 4:
            time.sleep(1)
    else:
        if not required:
            raise PgStartupFailed(f"{container} up but not answering queries: {probe.stderr.strip()}")
        sys.exit(f"{RED}Postgres is up but not answering queries{RESET}: {probe.stderr.strip()}")


class MinioStartupFailed(Exception):
    """Raised instead of exiting when `ensure_minio(required=False)`'s
    container never becomes healthy — mirrors `PgStartupFailed`.
    """


def minio_port_mapping(service: dict) -> tuple[int, int]:
    """Extract `(host_port, container_port)` from a job's
    `services.minio.ports` entry (e.g. `"19000:9000"` → `(19000, 9000)`) —
    read from the job's own parsed YAML, never hardcoded here a second time
    (see `MINIO_CONTAINER`'s doc comment for why nothing needs to rewrite
    it).
    """
    ports = service.get("ports") or []
    if not ports:
        sys.exit(f"{RED}a job's minio service declares no ports:{RESET} {service!r}")
    host, _, container_port = str(ports[0]).partition(":")
    return int(host), int(container_port)


def ensure_minio(service: dict, quiet: bool = False, container: str = MINIO_CONTAINER,
                  required: bool = True) -> None:
    """Start (or reuse) the throwaway MinIO the `minio`-service jobs need.

    Mirrors `ensure_pg` above step for step (existing-container reuse,
    port-mismatch recreate, readiness loop, then a real health probe) —
    same shape, same reasoning, just a different image/health-check. Every
    value used (`image`, `env`, the published port) comes from the job's OWN
    `services.minio` block in ci.yml (`service`, already parsed by
    `yaml.safe_load`) rather than a second hardcoded copy here, so editing
    ci.yml's minio block picks this up for free — the same property the
    module docstring claims for step commands.

    `required` has the same ramp-up-safety meaning as `ensure_pg`'s
    parameter — always `True` here today (MinIO is one shared container, not
    lane-scoped, so nothing calls this with `required=False` yet), kept for
    symmetry and so a future per-lane MinIO would not need to change this
    function's contract.
    """
    if not docker_ok():
        if not required:
            raise MinioStartupFailed("docker is not reachable")
        sys.exit(
            f"{RED}docker is not running{RESET} — a selected job needs MinIO "
            f"(ci.yml stands up {service.get('image', '?')} as a service). Start Docker "
            f"Desktop, or select a job with no `minio` service."
        )

    image = service.get("image")
    if not image:
        sys.exit(f"{RED}a job's minio service declares no image:{RESET} {service!r}")
    env = service.get("env") or {}
    port, container_port = minio_port_mapping(service)

    existing = subprocess.run(
        ["docker", "ps", "-aq", "-f", f"name=^{container}$"],
        capture_output=True,
        text=True,
    ).stdout.strip()

    if existing:
        # Same reasoning as `ensure_pg`: a container's port mapping is fixed
        # at creation, so a stale container from a previous ci.yml revision
        # (or a different job's minio block, if one ever diverges) would
        # otherwise silently publish on the wrong port. Recreate.
        mapped = {
            p for p in subprocess.run(
                ["docker", "inspect", "-f",
                 "{{range $p, $c := .NetworkSettings.Ports}}{{range $c}}{{.HostPort}} {{end}}{{end}}",
                 container],
                capture_output=True, text=True,
            ).stdout.split() if p
        }
        if mapped and str(port) not in mapped:
            if not quiet:
                print(f"{YELLOW}==>{RESET} {container} is published on :{','.join(sorted(mapped))}, not :{port} — recreating")
            subprocess.run(["docker", "rm", "-f", container], capture_output=True)
            existing = ""
        else:
            running = subprocess.run(
                ["docker", "ps", "-q", "-f", f"name=^{container}$"],
                capture_output=True,
                text=True,
            ).stdout.strip()
            if not running:
                subprocess.run(["docker", "start", container], capture_output=True, check=True)

    if not existing:
        if not quiet:
            print(f"{BLUE}==>{RESET} starting {container} ({image}) on :{port}")
        run_cmd = ["docker", "run", "-d", "--name", container]
        for k, v in env.items():
            run_cmd += ["-e", f"{k}={v}"]
        run_cmd += ["-p", f"{port}:{container_port}", image]
        subprocess.run(run_cmd, capture_output=True, check=True)

    # ci.yml gates on `curl -f http://localhost:9000/minio/health/live` (run
    # INSIDE the container, exactly like Docker's own `--health-cmd` would);
    # mirror that with `docker exec` rather than hitting the host-published
    # port, same convention as `ensure_pg`'s `pg_isready`/`psql` probes.
    # `container_port` (not `port`, the HOST side) is what the process
    # listens on from inside its own container.
    health_url = f"http://localhost:{container_port}/minio/health/live"
    for _ in range(60):
        if subprocess.run(
            ["docker", "exec", container, "curl", "-sf", health_url],
            capture_output=True,
        ).returncode == 0:
            break
        time.sleep(1)
    else:
        if not required:
            raise MinioStartupFailed(f"{container} never became healthy")
        sys.exit(f"{RED}{container} never became healthy{RESET} (minio/health/live never answered)")


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


CPU_COUNT = os.cpu_count() or 1

# Floor below which the adaptive lane ramp-up (see `main`'s parallel-lanes
# branch) stops starting new lanes. Percentage of TOTAL ram, not a fixed GB
# amount, so the default travels sanely to a machine with a different amount
# of RAM. Overridable per the same convention as CI_LOCAL_JOBS/CI_LOCAL_PG_PORT.
RAM_FLOOR_PCT = float(os.environ.get("CI_LOCAL_MIN_FREE_RAM_PCT", "20"))


def free_ram_pct() -> float | None:
    """Percentage of total host RAM currently free, or `None` if it can't be
    determined on this platform/right now.

    This drives the adaptive lane ramp-up, and it must read HOST memory, not
    Docker's own VM stat — cargo/rustc run natively, not inside a container,
    and a -j4 run crashing (`STATUS_DLL_INIT_FAILED`) with 15.8GB "free" per
    Windows despite Docker's separate ~15.2GB VM cap having headroom too is
    what proved a lane's actual cost is host-side compile memory, not
    container memory. `None` must read as "cannot verify headroom" and stop
    ramp-up rather than assume it's fine — same fail-conservative posture as
    the rest of this file's resource guards.
    """
    if sys.platform == "win32":
        # `wmic` is not a fallback here: confirmed absent on this exact class
        # of machine ("wmic: The term ... is not recognized") during the
        # investigation that led to this function. PowerShell ships everywhere
        # Windows CI already requires it (verify_pr.sh, other helpers here).
        out = subprocess.run(
            ["powershell", "-NoProfile", "-Command",
             "$o = Get-CimInstance Win32_OperatingSystem; "
             "Write-Output \"$($o.FreePhysicalMemory) $($o.TotalVisibleMemorySize)\""],
            capture_output=True, text=True,
        ).stdout.strip()
        try:
            free_kb, total_kb = (float(x) for x in out.split())
            return 100.0 * free_kb / total_kb if total_kb else None
        except ValueError:
            return None
    if sys.platform == "darwin":
        out = subprocess.run(["vm_stat"], capture_output=True, text=True).stdout
        pages: dict[str, int] = {}
        for line in out.splitlines():
            if ":" not in line:
                continue
            k, v = line.split(":", 1)
            v = v.strip().rstrip(".")
            if v.isdigit():
                pages[k.strip()] = int(v)
        try:
            free = pages["Pages free"] + pages["Pages speculative"]
            total = sum(pages.values())
            return 100.0 * free / total if total else None
        except KeyError:
            return None
    try:
        info: dict[str, int] = {}
        for line in Path("/proc/meminfo").read_text(encoding="utf-8").splitlines():
            k, v = line.split(":", 1)
            info[k.strip()] = int(v.strip().split()[0])  # kB
        total = info.get("MemTotal", 0)
        avail = info.get("MemAvailable", info.get("MemFree", 0))
        return 100.0 * avail / total if total else None
    except (OSError, ValueError):
        return None


class LaneBudget:
    """The shared, thread-safe "how many Postgres lanes are active right
    now" count that both (a) `run_step` reads to size `CARGO_BUILD_JOBS`
    (`max(1, CPU_COUNT // active)`, recomputed on every step so it shrinks
    automatically as ramp-up adds lanes) and (b) the crash-retry path uses
    to decide whether a lane may safely step down (never step the LAST lane
    down — that would strand its requeued job with no one left to pick it
    up).
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._count = 1

    def set(self, n: int) -> None:
        with self._lock:
            self._count = max(1, n)

    def increment(self) -> None:
        """A lane just joined the CURRENTLY active pool. Deliberately relative
        rather than `set(len(lane_threads))` — `lane_threads` only ever grows
        (threads that stepped down after a crash retry are never removed from
        it), so an absolute set from its length would overwrite a correct
        post-stepdown count with a stale, too-high one.
        """
        with self._lock:
            self._count += 1

    def get(self) -> int:
        with self._lock:
            return self._count

    def build_jobs(self) -> int:
        return max(1, CPU_COUNT // self.get())

    def try_step_down(self) -> bool:
        """Decrement and return True, unless this is the last active lane
        (returns False, leaves the count unchanged) — a crashed lane with no
        surviving sibling must keep working its own requeued job itself
        rather than orphaning it in the queue.
        """
        with self._lock:
            if self._count <= 1:
                return False
            self._count -= 1
            return True


LANE_BUDGET = LaneBudget()


def is_crash_exit(code: int, out: str) -> bool:
    """Distinguish an abnormal process death from a real test failure.

    A normal `cargo test` failure exits with a small positive code (101) and
    prints a `test result: FAILED` line. A process killed by a Windows
    exception (e.g. `STATUS_DLL_INIT_FAILED` = 0xC0000142) never gets that
    far and surfaces through `subprocess` as a large NEGATIVE code — Python
    decodes the NTSTATUS as signed 32-bit (confirmed: `0xC0000142` as signed
    32-bit is -1073740990). Both signals must agree: retrying a genuine test
    failure would waste a full run and could mask real flakiness, so this
    stays conservative rather than pattern-matching on exit code alone.
    """
    return code < 0 and "test result:" not in out


def _pid_alive(pid: int) -> bool:
    """Cross-platform "is this pid still running".

    `os.kill(pid, 0)` is the POSIX idiom (no signal actually sent; a raised
    `ProcessLookupError` means the pid is gone). On Windows, `os.kill` funnels
    every non-CTRL signal through `TerminateProcess`, and 0 is not a signal
    value it accepts — it raises `OSError: [WinError 87] The parameter is
    incorrect` for EVERY pid, alive or dead, which crashed the run lock's
    stale-owner check outright rather than answering the question. `tasklist`
    ships on every Windows install; filter it by pid instead.
    """
    if sys.platform == "win32":
        out = subprocess.run(
            ["tasklist", "/fi", f"PID eq {pid}", "/fo", "csv", "/nh"],
            capture_output=True, text=True,
        ).stdout
        return str(pid) in out
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True


def pids_on(port: int) -> set[int]:
    # `lsof` doesn't exist on Windows at all — not slow, not degraded,
    # `subprocess.run` raises FileNotFoundError and takes the whole run down
    # with it (this crashed mid-sweep the first time a boots_server job, the
    # only kind that calls this, actually ran on this platform). `netstat`
    # ships on every Windows install; parse its ASCII table instead.
    if sys.platform == "win32":
        out = subprocess.run(["netstat", "-ano", "-p", "TCP"], capture_output=True, text=True).stdout
        pids: set[int] = set()
        for line in out.splitlines():
            parts = line.split()
            if (
                len(parts) >= 5
                and parts[0] == "TCP"
                and parts[3] == "LISTENING"
                and parts[1].rsplit(":", 1)[-1] == str(port)
                and parts[-1].isdigit()
            ):
                pids.add(int(parts[-1]))
        return pids
    out = subprocess.run(["lsof", "-nP", f"-iTCP:{port}", "-sTCP:LISTEN", "-t"],
                         capture_output=True, text=True).stdout.split()
    return {int(p) for p in out if p.isdigit()}


def port_in_use(port: int) -> str | None:
    """Who is listening on `port`, if anyone — for error messages worth reading."""
    with socket.socket() as s:
        s.settimeout(0.3)
        if s.connect_ex(("127.0.0.1", port)) != 0:
            return None
    if sys.platform == "win32":
        pids = pids_on(port)
        if not pids:
            return "unknown process"
        pid = next(iter(pids))
        row = subprocess.run(
            ["tasklist", "/fi", f"PID eq {pid}", "/fo", "csv", "/nh"],
            capture_output=True, text=True,
        ).stdout.strip()
        cmd = row.split(",")[0].strip('"') if row else "?"
        return f"{cmd} (pid {pid})"
    who = subprocess.run(["lsof", "-nP", f"-iTCP:{port}", "-sTCP:LISTEN", "-Fcp"],
                         capture_output=True, text=True)
    pid = cmd = "?"
    for line in who.stdout.splitlines():
        if line.startswith("p"): pid = line[1:]
        if line.startswith("c"): cmd = line[1:]
    return f"{cmd} (pid {pid})"


# Steps that LAUNCH the server, as opposed to merely compiling or testing it.
#
# The first version of this matched the substring "coralswarm-backend" anywhere
# in a step, which also matches `cargo test -p coralswarm-backend` — so all 15
# Postgres jobs took the boot path and failed instantly on a port they never
# touch. Anchor on the executable path instead.
BOOTS_SERVER = re.compile(r"(^|\s|/)target/(debug|release)/coralswarm-backend(\s|$)", re.M)


def job_boots_server(job: dict) -> bool:
    return any(BOOTS_SERVER.search(s.get("run", "")) for s in job.get("steps", []))


DSN_RE = re.compile(r"^postgres(ql)?://", re.I)


def ensure_job_database(job: dict, port: int) -> str | None:
    """Give the job the database its `services.postgres` block declares.

    On a runner every job gets its OWN container, so `POSTGRES_DB` is created
    empty for it and nothing survives between runs. Locally one container is
    shared, so that has to be reproduced explicitly — jobs declare different
    names (`retrieval-eval` and `mcp-e2e` both want `coralswarm`, most others
    want `postgres`), and without this their very first step fails with
    `database "coralswarm" does not exist`.

    Dropped and recreated rather than created-if-missing, deliberately: a
    database carrying rows from the previous run is exactly the kind of state a
    fresh container would never have, and it can turn a real failure green.
    Returns an error string, or None on success.
    """
    admin = f"postgres://postgres:postgres@localhost:{port}/postgres"

    # Drop EVERY user database first, not just the one this job declares.
    #
    # A runner gives each job its own container, so the server it sees is empty.
    # The shared local container instead accumulates — after a few runs it held
    # 38 leftover databases. Two ways that hurts, one loud and one quiet:
    #
    #   loud:  rls-probe-harness provisions with a bare `CREATE DATABASE
    #          judgement_graph_ci` (no IF NOT EXISTS), which fails outright the
    #          second time the job ever runs on this machine.
    #   quiet: a scratch database still holding rows from an earlier run is
    #          exactly the state a fresh container never has, and it can make a
    #          genuinely broken test pass.
    #
    # The second is why this resets before every Postgres job rather than once
    # per run. The container is ours alone (cs-localci-pg), so nothing else can
    # be living here.
    listing = subprocess.run(
        ["psql", admin, "-tAXc",
         "select datname from pg_database where not datistemplate and datname <> 'postgres'"],
        capture_output=True, text=True, timeout=60,
    )
    if listing.returncode != 0:
        return f"could not list databases: {listing.stderr.strip()}"
    for stale in listing.stdout.split():
        subprocess.run(["psql", admin, "-tAXc", f'DROP DATABASE IF EXISTS "{stale}" WITH (FORCE)'],
                       capture_output=True, text=True, timeout=60)

    dbname = ((job.get("services") or {}).get("postgres") or {}).get("env", {}).get("POSTGRES_DB", "postgres")
    if dbname == "postgres":
        return None  # the maintenance database always exists and must not be dropped
    res = subprocess.run(["psql", admin, "-tAXc", f'CREATE DATABASE "{dbname}"'],
                         capture_output=True, text=True, timeout=60)
    if res.returncode != 0:
        return f"could not create database {dbname!r}: {res.stderr.strip()}"
    return None


def verify_dsn_env(jid: str, job: dict, port: int) -> list[str]:
    """Prove every DSN this job hands its tests actually works, before running.

    This is the primary vacuous-pass guard (see the module docstring for why
    scanning test output instead does not work). Returns a list of problems;
    empty means the job's database gates cannot silently swallow its tests.
    """
    problems: list[str] = []
    dsns = {
        k: rewrite_dsn(str(v), port)
        for k, v in (job.get("env") or {}).items()
        if DSN_RE.match(str(v))
    }
    if not dsns:
        problems.append(
            f"{jid} declares a postgres service but sets no DSN env var — "
            f"its tests would all take the skip branch and pass vacuously"
        )
        return problems

    for var, dsn in dsns.items():
        # Probe the SERVER, not the named database.
        #
        # Some jobs point at a database an earlier step creates — mcp-e2e and
        # retrieval-eval both use `.../coralswarm`, which does not exist until
        # their own bootstrap step runs. Requiring it up front made this guard
        # fail two jobs that CI passes, for a condition that is not a vacuous
        # pass at all. What actually matters is that the server is reachable
        # and the role can create what the tests need, so connect to the
        # always-present `postgres` maintenance database instead.
        server_dsn = re.sub(r"(?<=/)[^/?]*(?=(\?|$))", "postgres", dsn, count=1)
        probe = subprocess.run(
            ["psql", server_dsn, "-tAXc",
             "select 1, rolcreatedb, rolcreaterole from pg_roles where rolname = current_user"],
            capture_output=True,
            text=True,
            timeout=30,
        )
        if probe.returncode != 0:
            problems.append(f"{var} is not usable from the host: {probe.stderr.strip().splitlines()[-1] if probe.stderr.strip() else 'connection failed'}")
            continue
        parts = probe.stdout.strip().split("|")
        if len(parts) != 3 or parts[0] != "1":
            problems.append(f"{var} connected but did not answer `select 1`")
        elif parts[1] != "t" or parts[2] != "t":
            # The tests CREATE DATABASE and CREATE ROLE; without these the
            # failures look like product bugs rather than a setup problem.
            problems.append(
                f"{var} lacks CREATEDB/CREATEROLE (createdb={parts[1]} createrole={parts[2]})"
            )
    return problems


def rewrite_dsn(value: str, port: int) -> str:
    """Point the workflow's hardcoded service DSNs at our container's port."""
    return value.replace("localhost:5432", f"localhost:{port}").replace(
        "127.0.0.1:5432", f"127.0.0.1:{port}"
    )


# ---------------------------------------------------------------------------
# Preflight
# ---------------------------------------------------------------------------

TOOL_HINTS = {
    "cargo": "install rustup (https://rustup.rs)",
    "node": "provide the declared workflow major with --node-bin",
    "npm": "use the selected runtime's sibling npm/npx installation",
    "shellcheck": "brew install shellcheck",
    "psql": "brew install libpq && brew link --force libpq",
    "sqlx": "cargo install sqlx-cli --version '^0.8' --no-default-features --features postgres,rustls --locked",
}


def tools_for(jobs: dict[str, dict]) -> set[str]:
    need = set()
    for job in jobs.values():
        blob = "\n".join(s.get("run", "") for s in job.get("steps", []))
        for tool in ("cargo", "shellcheck", "psql"):
            if re.search(rf"(^|[\s|&;(]){tool}\b", blob):
                need.add(tool)
        # Matched as a substring, not a word at a command boundary: ci.yml
        # invokes it as `SQLX=~/.cargo/bin/sqlx bash scripts/…`, where the
        # boundary regex above sees only a path component and would let the
        # job start and then fail on a missing binary 20 minutes in.
        if "sqlx" in blob:
            need.add("sqlx")
        # Postgres jobs need psql for the database preflight even when none of
        # their own steps invoke it.
        if "services" in job:
            need.add("psql")
        if any(str(step.get("uses", "")).startswith("actions/setup-node") for step in job.get("steps", [])):
            need.update({"node", "npm"})
    return need


NODE_CONTROLS = ("NODE_OPTIONS", "NODE_PATH")


class NodeBindingError(RuntimeError):
    """A setup error with no subprocess output or environment values."""


def node_control_error(*environments: dict) -> str:
    for values in environments:
        for name in NODE_CONTROLS:
            if values.get(name, "") != "":
                return f"unsupported Node runtime control {name}"
    return ""


def node_declaration(jid: str, job: dict) -> tuple[int, int] | None:
    setups = [(i, step) for i, step in enumerate(job.get("steps", []))
              if str(step.get("uses", "")).startswith("actions/setup-node")]
    if not setups:
        return None
    if len(setups) != 1:
        raise NodeBindingError(f"{jid}: requires one unconditional literal Node setup")
    index, step = setups[0]
    inputs = step.get("with") or {}
    literal = str(inputs.get("node-version", ""))
    if (step.get("uses") != "actions/setup-node@v4" or "if" in step
            or not re.fullmatch(r"[1-9][0-9]*", literal)
            or set(inputs) - {"node-version", "cache", "cache-dependency-path"}):
        raise NodeBindingError(f"{jid}: unsupported Node setup declaration")
    for earlier in job.get("steps", [])[:index]:
        script = "\n".join(line for line in earlier.get("run", "").splitlines()
                           if not line.lstrip().startswith("#"))
        if re.search(r"(^|[\s|&;(])(node|npm|npx)\b", script):
            raise NodeBindingError(f"{jid}: Node command precedes its setup action")
    return index, int(literal)


@dataclass(frozen=True)
class NodeRuntime:
    major: int
    bin: str
    node: str
    npm: str
    npx: str
    package: str
    node_version: str
    npm_version: str
    npx_version: str
    platform: str
    arch: str
    hashes: tuple[tuple[str, str], ...]


@dataclass(frozen=True)
class NodeJobBinding:
    job_id: str
    action_index: int
    source_sha: str
    runtime: NodeRuntime

    def provenance(self) -> dict:
        value = {"event": "local-node-binding", "source_sha": self.source_sha, "job": self.job_id,
                 "action_ordinal": self.action_index + 1, "declaration": str(self.runtime.major)}
        value.update({name: getattr(self.runtime, name) for name in
                      ("bin", "node", "npm", "npx", "package", "node_version", "npm_version",
                       "npx_version", "platform", "arch")})
        value["hashes"] = dict(self.runtime.hashes)
        return value


def validate_node_runtime(major: int, directory: str) -> NodeRuntime:
    """Probe only this exact local sibling installation, using no npm config."""
    try:
        supplied = Path(directory)
        if not supplied.is_absolute() or os.pathsep in str(supplied):
            raise ValueError()
        binpath = supplied.resolve(strict=True)
        paths = {name: (binpath / name).resolve(strict=True) for name in ("node", "npm", "npx")}
        package_root = (binpath.parent / "lib" / "node_modules" / "npm").resolve(strict=True)
        package = package_root / "package.json"
        if any(not path.is_file() or not os.access(path, os.X_OK) for path in paths.values()):
            raise ValueError()
        for name in ("npm", "npx"):
            if paths[name] != (package_root / "bin" / f"{name}-cli.js").resolve(strict=True):
                raise ValueError()
        package_version = json.loads(package.read_text(encoding="utf-8"))["version"]
        if not isinstance(package_version, str) or not re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+(?:[-+][A-Za-z0-9.-]+)?", package_version):
            raise ValueError()
        with tempfile.TemporaryDirectory(prefix="ci-node-probe-") as temporary:
            private = Path(temporary)
            for name in ("user.npmrc", "global.npmrc"):
                (private / name).touch(mode=0o600)
            env = {"PATH": str(binpath) + os.pathsep + os.defpath, "HOME": temporary, "TMPDIR": temporary,
                   "LANG": "C", "npm_config_userconfig": str(private / "user.npmrc"),
                   "npm_config_globalconfig": str(private / "global.npmrc"),
                   "npm_config_cache": str(private / "cache"), "npm_config_offline": "true",
                   "npm_config_update_notifier": "false", "npm_config_audit": "false", "npm_config_fund": "false"}

            def probe(args: list[str]) -> str:
                result = subprocess.run(args, env=env, cwd=private, capture_output=True, text=True, timeout=10)
                if result.returncode:
                    raise ValueError()
                return result.stdout.strip()

            identity = json.loads(probe([str(paths["node"]), "-e",
                "process.stdout.write(JSON.stringify({version:process.version,execPath:process.execPath,platform:process.platform,arch:process.arch}))"]))
            match = re.fullmatch(r"v([1-9][0-9]*)\.[0-9]+\.[0-9]+(?:[-+][A-Za-z0-9.-]+)?", identity["version"])
            if (not match or int(match.group(1)) != major or Path(identity["execPath"]).resolve() != paths["node"]
                    or not re.fullmatch(r"[a-z0-9_-]+", identity["platform"])
                    or not re.fullmatch(r"[a-z0-9_-]+", identity["arch"])):
                raise ValueError()
            npm_version = probe([str(paths["node"]), str(paths["npm"]), "--version"])
            npx_version = probe([str(paths["node"]), str(paths["npx"]), "--version"])
            if npm_version != package_version or npx_version != package_version:
                raise ValueError()
        hashes = []
        for name, path in [*paths.items(), ("package", package)]:
            digest = hashlib.sha256()
            with path.open("rb") as source:
                for chunk in iter(lambda: source.read(1024 * 1024), b""):
                    digest.update(chunk)
            hashes.append((name, digest.hexdigest()))
        return NodeRuntime(major, str(binpath), str(paths["node"]), str(paths["npm"]), str(paths["npx"]),
                           str(package), identity["version"], npm_version, npx_version,
                           identity["platform"], identity["arch"], tuple(hashes))
    except (OSError, ValueError, TypeError, KeyError, AttributeError, subprocess.SubprocessError):
        raise NodeBindingError(f"Node {major}: invalid or unavailable sibling runtime at {directory}") from None


def resolve_node_bindings(jobs: dict[str, dict], entries: list[str], source_sha: str):
    declarations = {jid: declared for jid, job in jobs.items()
                    if (declared := node_declaration(jid, job)) is not None}
    pins = {}
    for entry in entries:
        major, separator, directory = entry.partition("=")
        if (not separator or not re.fullmatch(r"[1-9][0-9]*", major)
                or not Path(directory).is_absolute() or int(major) in pins):
            raise NodeBindingError("--node-bin requires unique MAJOR=/absolute/runtime/bin entries")
        pins[int(major)] = directory
    if not declarations:
        return MappingProxyType({})
    for jid in declarations:
        error = node_control_error(os.environ, jobs[jid].get("env") or {},
                                   *(step.get("env") or {} for step in jobs[jid].get("steps", [])))
        if error:
            raise NodeBindingError(f"{jid}: {error}")
    if not re.fullmatch(r"[0-9a-f]{40}", source_sha or ""):
        raise NodeBindingError("Node binding requires the exact source commit")
    runtimes = {}
    for major in sorted({major for _, major in declarations.values()}):
        directory = pins.get(major)
        if directory is None:
            candidate = shutil.which("node")
            if candidate is None or not Path(candidate).is_absolute():
                raise NodeBindingError(f"Node {major}: pin --node-bin or provide an exact-major ambient runtime")
            directory = str(Path(candidate).parent)
        runtimes[major] = validate_node_runtime(major, directory)
    return MappingProxyType({jid: NodeJobBinding(jid, index, source_sha, runtimes[major])
                             for jid, (index, major) in declarations.items()})


class RunLock:
    """Refuse to start while another local CI run is live.

    Two runs against the same containers would rip databases out from under each
    other mid-test, and the resulting failures look exactly like defects in the
    code under test — the worst possible lie for this tool to tell. Pushing from
    one terminal while verifying a PR in another is an ordinary thing to do, so
    this is a real collision, not a theoretical one.

    O_CREAT|O_EXCL so the check and the claim are one atomic step; a lock whose
    owner is gone is reclaimed rather than left to wedge the tool forever.
    """

    def __init__(self, root: Path) -> None:
        # MACHINE-GLOBAL, not per-worktree. The first version of this lived in
        # the worktree's gitdir, which was useless: the thing being protected is
        # the set of Docker containers, and those are shared by every checkout on
        # the machine. Two worktrees would each have taken their own lock and
        # then corrupted each other anyway — a guard that reports success while
        # protecting nothing.
        self.path = Path.home() / ".cache" / "coralswarm-ci-local.lock"
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def acquire(self, force: bool) -> None:
        for _ in range(2):
            try:
                fd = os.open(self.path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
                os.write(fd, f"{os.getpid()}\n{time.strftime('%Y-%m-%dT%H:%M:%S')}\n".encode())
                os.close(fd)
                return
            except FileExistsError:
                try:
                    pid_s, when = (self.path.read_text(encoding="utf-8").splitlines() + ["", ""])[:2]
                    pid = int(pid_s)
                except (OSError, ValueError):
                    self.path.unlink(missing_ok=True)
                    continue
                alive = _pid_alive(pid)
                if not alive:
                    print(f"{YELLOW}!{RESET} clearing a stale lock from pid {pid} (no longer running)")
                    self.path.unlink(missing_ok=True)
                    continue
                if force:
                    print(f"{YELLOW}!{RESET} --force: proceeding despite the run in pid {pid}")
                    return
                sys.exit(
                    f"{RED}another local CI run is in progress{RESET} (pid {pid}, started {when}).\n"
                    f"  Concurrent runs share the Postgres containers and would corrupt each other.\n"
                    f"  Wait for it, or pass --force if you are certain it is dead."
                )

    def release(self) -> None:
        self.path.unlink(missing_ok=True)


def leaked_node_modules(root: Path) -> Path | None:
    """The nearest `node_modules` at or above this checkout, if any.

    Its existence is what makes local Node builds diverge from a runner's, whose
    checkout has no such directory anywhere up the tree.

    `root` ITSELF is checked, not just its ancestors, and that distinction is the
    whole bug this once had. The two checkouts differ in where the offending
    directory sits:

        worktree:       <repo>/worktrees/wt-x/marketing-web
                        → resolution climbs to <repo>/node_modules   (an ANCESTOR)
        main checkout:  <repo>/marketing-web
                        → resolution climbs to <repo>/node_modules   (the ROOT)

    Scanning only `root.parents` caught the first and missed the second, so
    `marketing` was correctly isolated in every worktree and silently built in
    place on main — where it failed exactly as it always had.
    """
    for candidate in (root, *root.parents):
        if (candidate / "node_modules").is_dir():
            return candidate
    return None


def warn_node_resolution_leak(root: Path, jobs: dict[str, dict]) -> None:
    """Warn when Node lanes can see packages a hosted runner never would.

    Every branch here is developed in a worktree under `<repo>/worktrees/`, so
    a worktree's `web/` and `marketing-web/` sit *inside* the main checkout's
    directory tree. Node resolution walks up the filesystem without stopping at
    the worktree boundary, so it reaches the main checkout's `node_modules` —
    a hosted runner has no such ancestor.

    This is not theoretical. It makes the `marketing` job fail locally while
    passing in CI: `marketing-web` declares no ESLint dependency, so on a
    runner `next build` finds no eslint and skips linting entirely. Locally it
    resolves the parent checkout's eslint, then lints using the *worktree's*
    root `eslint.config.js`, which references `react-hooks` rules whose plugin
    is not installed — "Definition for rule 'react-hooks/exhaustive-deps' was
    not found", and a red job for a green commit.

    Warn rather than silently neutralize it: quietly changing resolution would
    make the local run diverge from CI in a way nobody could see.
    """
    node_lanes = [
        jid for jid, job in jobs.items()
        if job.get("defaults", {}).get("run", {}).get("working-directory") in ("web", "marketing-web")
    ]
    if not node_lanes:
        return
    ancestor = leaked_node_modules(root)
    if not ancestor:
        return
    handled = [j for j in node_lanes if j in ISOLATED_JOBS]
    exposed = [j for j in node_lanes if j not in ISOLATED_JOBS]
    print(f"{YELLOW}!{RESET} {ancestor}/node_modules sits above this checkout.")
    print(f"  Node lanes can resolve packages a runner cannot, so a lint/build result")
    print(f"  here may not match CI.")
    if handled:
        print(f"  {DIM}{', '.join(handled)}: run in an isolated checkout, so unaffected.{RESET}")
    if exposed:
        print(f"  {DIM}{', '.join(exposed)}: run in place — treat a surprising failure with suspicion.{RESET}")
    print(f"  {DIM}Cleanest fix: keep worktrees outside the repo, e.g. ../coralswarm-worktrees/.{RESET}")


def have_tool(tool: str) -> bool:
    if shutil.which(tool):
        return True
    # ci.yml calls sqlx by absolute path (~/.cargo/bin/sqlx), which is where
    # `cargo install` puts it whether or not that directory is on PATH.
    return tool == "sqlx" and (Path.home() / ".cargo" / "bin" / "sqlx").exists()


def preflight(jobs: dict[str, dict], node_bindings=None) -> None:
    required = tools_for(jobs)
    unbound = tools_for({jid: job for jid, job in jobs.items() if jid not in (node_bindings or {})})
    for tool in ("node", "npm"):
        if node_bindings and tool not in unbound:
            required.discard(tool)  # the absolute sibling pair was already validated
    missing = [t for t in sorted(required) if not have_tool(t)]
    if missing:
        print(f"{RED}missing tools required by the selected jobs:{RESET}")
        for t in missing:
            print(f"  {BOLD}{t}{RESET} — {TOOL_HINTS.get(t, 'install it')}")
        sys.exit(1)


# ---------------------------------------------------------------------------
# Step execution
# ---------------------------------------------------------------------------


def should_run_step(step: dict, failed: bool) -> tuple[bool, str]:
    if "run" not in step:
        return False, "runner setup (uses:)"
    if UBUNTU_ONLY.search(step["run"]):
        return False, "ubuntu-only runner housekeeping"

    cond = str(step.get("if", "")).strip()
    if not cond:
        return (False, "a previous step failed") if failed else (True, "")
    if IF_FAILURE.search(cond):
        return (True, "") if failed else (False, "if: failure()")
    if IF_ALWAYS.search(cond):
        return True, ""
    if IF_CACHE_MISS.search(cond):
        # These guard `cargo install sqlx-cli` behind a cache miss. Locally the
        # binary either exists on PATH (preflight proved it) or it does not.
        return False, "tool already installed (cache-hit guard)"
    if failed:
        return False, "a previous step failed"
    return True, f"unrecognized if: {cond[:60]}"


FIXTURE_PG_OPTIONS = ('--health-cmd "pg_isready -U postgres" --health-interval 5s '
                      '--health-timeout 5s --health-retries 10')
FIXTURE_PRIMARY_DSN = "postgres://postgres:postgres@localhost:5432/postgres"


def fixture_pg_service(service: dict, published_port: int) -> bool:
    """Only the workflow's exact reviewed local fixture service contract."""
    return service == {
        "image": "pgvector/pgvector:pg16",
        "env": {"POSTGRES_PASSWORD": "postgres", "POSTGRES_DB": "postgres"},
        "ports": [f"{published_port}:5432"],
        "options": FIXTURE_PG_OPTIONS,
    }


def adapt_primary_fixture(step: dict, job: dict, script: str, env: dict, port: int) -> tuple[str, str]:
    """Bind two explicit expected-port guards to their assigned primary lane."""
    name = step.get("name")
    workspace = (name == "Workspace counts, permission and pagination budgets"
                 or "workspace_reads_pg" in script or "WORKSPACE_TEST_OWNED_PORT" in script
                 or "WORKSPACE_TEST_OWNED_PORT" in (step.get("env") or {}))
    reef = (name == "Reef identity ordinary-role security, lifecycle and lexical scale"
            or "reef_identity_pg" in script or "REEF_IDENTITY_TEST_OWNED_PORT" in script
            or "REEF_IDENTITY_TEST_OWNED_PORT" in (step.get("env") or {}))
    if not (workspace or reef):
        return script, ""
    if (workspace and reef or type(port) is not int or not 0 < port < 65536
            or not fixture_pg_service((job.get("services") or {}).get("postgres"), 5432)):
        return script, "unrecognized primary fixture service binding"
    if workspace:
        command = ("cargo test -p coralswarm-backend --no-default-features --features test-support "
                   "--test workspace_reads_pg -- --test-threads=1")
        assignment = "export WORKSPACE_TEST_OWNED_PORT=5432"
        if (name != "Workspace counts, permission and pagination budgets"
                or script.splitlines().count(command) != 1
                or script.splitlines().count(assignment) != 1
                or script.splitlines().count('export WORKSPACE_TEST_BASE_URL="$ORGT_TEST_BASE_URL"') != 1
                or (job.get("env") or {}).get("ORGT_TEST_BASE_URL") != FIXTURE_PRIMARY_DSN
                or env.get("ORGT_TEST_BASE_URL") != rewrite_dsn(FIXTURE_PRIMARY_DSN, port)):
            return script, "unrecognized Workspace fixture endpoint binding"
        return re.sub(r"^export WORKSPACE_TEST_OWNED_PORT=5432$",
                      f"export WORKSPACE_TEST_OWNED_PORT={port}", script, count=1, flags=re.M), ""
    declared = step.get("env") or {}
    command = ('cargo test -p coralswarm-backend --test reef_identity_pg -- --test-threads=1 '
               '--nocapture 2>&1 | tee "$evidence/native.log"')
    if (name != "Reef identity ordinary-role security, lifecycle and lexical scale"
            or script.splitlines().count(command) != 1
            or declared.get("REEF_IDENTITY_TEST_BASE_URL") != FIXTURE_PRIMARY_DSN
            or declared.get("REEF_IDENTITY_TEST_OWNED_PORT") != "5432"):
        return script, "unrecognized Reef fixture endpoint binding"
    env["REEF_IDENTITY_TEST_OWNED_PORT"] = str(port)
    return script, ""


TEAMS_MEETING_SCRIPTS = {
    "Teams meeting consent and revocation boundaries (Postgres)":
        "294a217847d8f75a5f0b4c7eaf03bbbf9210006ebf5e5081ac38134d6dd4385e",
    "Teams selected meeting polling confinement (Postgres)":
        "43ed9e8f66be2da5b3987dd5bc65d286fddaa9be60740da6e10afc45ea7546c8",
}
TEAMS_MEETING_CONTROLS = {"TEAMS_MEETING_TEST_BASE_URL", "TEAMS_MEETING_TEST_OWNED_PORT"}


def adapt_teams_meeting_fixture(step: dict, job: dict, job_id: str | None,
                                env: dict, carry: dict | None, port: int) -> str:
    """Bind only the reviewed same-service meeting aliases to this owned lane."""
    declared = step.get("env") or {}
    script = step["run"]
    # The ordinary non-Postgres suite names both cases only in --skip flags.
    selected_script = re.sub(r"--skip\s+\S+", "", script)
    requested = (step.get("name") in TEAMS_MEETING_SCRIPTS
                 or "teams_meeting_capture::tests::" in selected_script
                 or "teams_meeting_sync::tests::" in selected_script
                 or any(key in script or key in declared for key in TEAMS_MEETING_CONTROLS))
    if not requested:
        return ""
    service = {
        "image": "pgvector/pgvector:pg16",
        "env": {"POSTGRES_PASSWORD": "postgres", "POSTGRES_DB": "postgres"},
        "ports": ["5432:5432", "15444:5432"],
        "options": FIXTURE_PG_OPTIONS,
    }
    if (job_id != "cross-tenant-probe" or type(port) is not int or not 0 < port < 65536
            or (job.get("services") or {}).get("postgres") != service
            or (job.get("env") or {}).get("ORGT_TEST_BASE_URL") != FIXTURE_PRIMARY_DSN
            or env.get("ORGT_TEST_BASE_URL") != rewrite_dsn(FIXTURE_PRIMARY_DSN, port)
            or any(key in source for source in [os.environ, carry or {}, job.get("env") or {}]
                   for key in TEAMS_MEETING_CONTROLS)
            or declared != {"TEAMS_MEETING_TEST_BASE_URL":
                            "postgres://postgres:postgres@127.0.0.1:15444/postgres"}
            or TEAMS_MEETING_SCRIPTS.get(step.get("name")) != hashlib.sha256(script.encode()).hexdigest()):
        return "unrecognized Teams meeting primary fixture binding"
    env["TEAMS_MEETING_TEST_BASE_URL"] = f"postgres://postgres:postgres@127.0.0.1:{port}/postgres"
    env["TEAMS_MEETING_TEST_OWNED_PORT"] = str(port)
    return ""


SECONDARY_STEP = "Tenant standing migration with isolated role catalog"
SECONDARY_COMMAND = ("cargo test -p coralswarm-backend --features test-support "
                     "--test tenant_standing_role_layout_pg -- --test-threads=1 --nocapture")


def secondary_fixture_binding(step: dict, job: dict, job_id: str | None) -> tuple[bool, str]:
    declared = step.get("env") or {}
    services = job.get("services") or {}
    requested = (step.get("name") == SECONDARY_STEP or "tenant_standing_role_layout_pg" in step["run"]
                 or "tenant-only-postgres" in services and "ORGT_TEST_BASE_URL" in declared)
    if not requested:
        return False, ""
    if (job_id != "tenant-role-provisioning" or step.get("name") != SECONDARY_STEP
            or step["run"].strip() != SECONDARY_COMMAND
            or not fixture_pg_service(services.get("postgres"), 5432)
            or not fixture_pg_service(services.get("tenant-only-postgres"), 5433)
            or (job.get("env") or {}).get("ORGT_TEST_BASE_URL") != FIXTURE_PRIMARY_DSN
            or declared != {"ORGT_TEST_BASE_URL": "postgres://postgres:postgres@localhost:5433/postgres",
                            "REQUIRE_PG_SUITE": "1"}):
        return False, "unrecognized isolated secondary fixture binding"
    return True, ""


SECONDARY_LABEL = "com.coralswarm.ci.secondary-owner"


class SecondaryFixtureError(RuntimeError):
    """A safe category, never Docker output, credentials or a DSN."""


class SecondaryFixtures:
    """Exit/interrupt inventory for this one step's nonce-owned catalogs."""

    def __init__(self, journal: Path | None = None):
        self.lock = threading.RLock()
        self.closing = threading.Event()
        self.leases: list[OwnedSecondary] = []
        self.journal = journal
        self.installed = False

    def record(self, lease: OwnedSecondary, event: str, *,
               inspection: tuple[str, int | None, str, str] | None = None) -> None:
        with self.lock:
            if self.journal is None:
                self.journal = Path(tempfile.mkdtemp(prefix="ci-secondary-")) / "inventory.jsonl"
                print(f"  isolated fixture inventory: {self.journal}")
            entry = {"event": event, "name": lease.name, "label": lease.nonce,
                     "id": lease.container_id, "image": "pgvector/pgvector:pg16", "port": lease.port}
            if inspection is not None:
                stage, code, shape, classification = inspection
                entry["inspection"] = {"stage": stage, "returncode": code,
                                       "stdout_shape": shape, "classification": classification}
            fd = os.open(self.journal, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
            with os.fdopen(fd, "a", encoding="utf-8") as log:
                log.write(json.dumps(entry, sort_keys=True) + "\n")
                log.flush()
                os.fsync(log.fileno())

    def register(self, lease: OwnedSecondary) -> None:
        with self.lock:
            if self.closing.is_set():
                raise SecondaryFixtureError("isolated fixture shutdown started")
            self.leases.append(lease)
            self.record(lease, "intent")  # durable before a Docker create can begin

    def close(self) -> bool:
        self.closing.set()
        with self.lock:
            leases = list(self.leases)
        return all([lease.close() for lease in leases])

    def install(self) -> None:
        """Install on the main thread, before parallel job workers can create."""
        if self.installed:
            return
        self.installed = True
        atexit.register(self.close)
        for signum in (signal.SIGINT, signal.SIGTERM):
            previous = signal.getsignal(signum)
            if previous == signal.SIG_IGN:
                continue

            def interrupted(number, frame, previous=previous):
                self.close()
                if callable(previous):
                    previous(number, frame)
                else:
                    signal.signal(number, signal.SIG_DFL)
                    os.kill(os.getpid(), number)

            signal.signal(signum, interrupted)


class OwnedSecondary:
    """One fresh catalog; never adopts a primary lane or a named listener."""

    def __init__(self, registry: SecondaryFixtures, primary_port: int):
        self.registry = registry
        self.primary_port = primary_port
        self.nonce = uuid.uuid4().hex
        self.name = "coralswarm-ci-secondary-" + self.nonce
        self.port: int | None = None
        self.container_id: str | None = None
        self.create_attempted = False
        self.create_returned = False
        self.process: subprocess.Popen | None = None
        self.lock = threading.RLock()
        self.closed: bool | None = None

    def docker(self, args: list[str], *, timeout: float = 8, env: dict | None = None):
        try:
            return subprocess.run(["docker", *args], capture_output=True, text=True,
                                  timeout=timeout, env=env)
        except (OSError, subprocess.SubprocessError):
            raise SecondaryFixtureError("isolated fixture Docker operation failed") from None

    def check_open(self) -> None:
        if self.closed is not None or self.registry.closing.is_set():
            raise SecondaryFixtureError("isolated fixture shutdown started")

    def inspect(self, key: str, *, stage: str) -> dict | None:
        if stage not in {"precreate-name", "created-id", "readiness-id", "cleanup"}:
            raise SecondaryFixtureError("isolated fixture inspection stage invalid")
        try:
            result = self.docker(["inspect", key])
        except SecondaryFixtureError:
            self.registry.record(self, "inspect-result",
                                 inspection=(stage, None, "unavailable", "command-failed"))
            raise
        rows = None
        try:
            rows = json.loads(result.stdout)
            shape = ("empty-list" if rows == [] else "one-object"
                     if isinstance(rows, list) and len(rows) == 1 and isinstance(rows[0], dict)
                     else "other-json")
        except (ValueError, TypeError):
            shape = "empty-output" if not result.stdout.strip() else "invalid-json"
        # Docker CLI versions vary in capitalization. Absence must still name
        # this exact requested object, never a daemon error or another object.
        missing_prefixes = {prefix + noun
                            for prefix in ["", "error: ", "error response from daemon: "]
                            for noun in ["no such object", "no such container"]}
        message_prefix, separator, message_key = result.stderr.strip().rpartition(": ")
        missing = (result.returncode != 0 and shape in {"empty-list", "empty-output"}
                   and separator == ": " and message_key == key and message_prefix.casefold() in missing_prefixes)
        classification = ("missing-requested-object" if missing else "command-error"
                          if result.returncode != 0 else "object" if shape == "one-object"
                          else "invalid-identity")
        # Persist only controlled categories, never Docker stderr or inspect's
        # environment/labels. Record before any return or failure loses it.
        self.registry.record(self, "inspect-result",
                             inspection=(stage, result.returncode, shape, classification))
        if missing:
            return None
        if result.returncode != 0:
            raise SecondaryFixtureError("isolated fixture identity unavailable")
        if shape != "one-object":
            raise SecondaryFixtureError("isolated fixture identity invalid")
        return rows[0]

    def owned(self, row: dict) -> bool:
        try:
            return (re.fullmatch(r"[0-9a-f]{64}", row.get("Id", "")) is not None
                    and (self.container_id is None or row["Id"] == self.container_id)
                    and row.get("Name") == "/" + self.name
                    and row.get("Config", {}).get("Image") == "pgvector/pgvector:pg16"
                    and row.get("Config", {}).get("Labels", {}).get(SECONDARY_LABEL) == self.nonce
                    and row.get("HostConfig", {}).get("PortBindings") == {
                        "5432/tcp": [{"HostIp": "127.0.0.1", "HostPort": str(self.port)}]})
        except (AttributeError, TypeError, KeyError):
            return False

    def start(self) -> None:
        self.check_open()
        if type(self.primary_port) is not int or not 0 < self.primary_port < 65536:
            raise SecondaryFixtureError("isolated fixture primary lane invalid")
        self.port = free_port()
        if type(self.port) is not int or not 0 < self.port < 65536 or self.port in {5432, 5433, 5442, self.primary_port}:
            raise SecondaryFixtureError("isolated fixture port allocation rejected")
        # A remote daemon's loopback is not this native test process's loopback.
        if (os.environ.get("DOCKER_HOST", "unix://").split(":", 1)[0] != "unix"
                or os.environ.get("DOCKER_TLS_VERIFY")):
            raise SecondaryFixtureError("isolated fixture requires a local Docker socket")
        with self.lock:
            self.check_open()
            result = self.docker(["context", "inspect"])
            try:
                local = json.loads(result.stdout)[0]["Endpoints"]["docker"]["Host"].startswith("unix://")
            except (ValueError, TypeError, KeyError, IndexError, AttributeError):
                local = False
            if result.returncode or not local:
                raise SecondaryFixtureError("isolated fixture requires a local Docker socket")
            self.registry.register(self)
            if self.inspect(self.name, stage="precreate-name") is not None:
                raise SecondaryFixtureError("isolated fixture generated name already exists")
            self.check_open()
            self.create_attempted = True
            result = self.docker([
                "run", "-d", "--name", self.name, "--label", f"{SECONDARY_LABEL}={self.nonce}",
                "-e", "POSTGRES_PASSWORD", "-e", "POSTGRES_DB", "-p", f"127.0.0.1:{self.port}:5432",
                "--health-cmd", "pg_isready -U postgres", "--health-interval", "5s",
                "--health-timeout", "5s", "--health-retries", "10", "pgvector/pgvector:pg16",
            ], timeout=12, env={**os.environ, "POSTGRES_PASSWORD": "postgres", "POSTGRES_DB": "postgres"})
            if result.returncode or not re.fullmatch(r"[0-9a-f]{64}", result.stdout.strip()):
                raise SecondaryFixtureError("isolated fixture create failed")
            self.container_id = result.stdout.strip()
            self.create_returned = True
            row = self.inspect(self.container_id, stage="created-id")
            if row is None or not self.owned(row):
                raise SecondaryFixtureError("isolated fixture ownership mismatch")
            self.registry.record(self, "created")
        deadline = time.monotonic() + 60
        while True:
            with self.lock:
                self.check_open()
                row = self.inspect(self.container_id, stage="readiness-id")
                if row is None or not self.owned(row):
                    raise SecondaryFixtureError("isolated fixture ownership mismatch")
                health = row.get("State", {}).get("Health", {}).get("Status")
                if health == "healthy":
                    break
                if health != "starting" or time.monotonic() >= deadline:
                    raise SecondaryFixtureError("isolated fixture readiness failed")
            self.registry.closing.wait(min(0.25, max(0, deadline - time.monotonic())))
        with self.lock:
            self.check_open()
            admin = self.docker([
                "exec", self.container_id, "psql", "-X", "-U", "postgres", "-d", "postgres", "-Atqc",
                "SELECT current_database(), current_user, rolcreatedb, rolcreaterole, "
                "NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='coralswarm_app') "
                "FROM pg_roles WHERE rolname=current_user",
            ], timeout=8)
            if admin.returncode or admin.stdout.strip() != "postgres|postgres|t|t|t":
                raise SecondaryFixtureError("isolated fixture admin or fresh catalog validation failed")
            self.registry.record(self, "ready")

    @property
    def dsn(self) -> str:
        return f"postgres://postgres:postgres@127.0.0.1:{self.port}/postgres"

    def spawn(self, args: list[str], **kwargs):
        with self.lock:
            self.check_open()
            self.process = subprocess.Popen(args, start_new_session=True, **kwargs)
            return self.process

    def close(self) -> bool:
        if not self.lock.acquire(timeout=15):
            return False
        try:
            if self.closed is not None:
                return self.closed
            self.closed = False  # stop new work even if cleanup cannot be confirmed
            try:
                if self.process is not None and self.process.poll() is None:
                    pid = self.process.pid
                    if os.getpgid(pid) != pid or os.getsid(pid) != pid:
                        raise SecondaryFixtureError("isolated fixture process ownership mismatch")
                    os.killpg(pid, signal.SIGTERM)
                    try:
                        self.process.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        os.killpg(pid, signal.SIGKILL)
                        self.process.wait(timeout=5)
                if self.create_attempted:
                    # An uncertain create is recovered only through the fresh
                    # name AND nonce label, then removed by its exact full ID.
                    row = self.inspect(self.container_id or self.name, stage="cleanup")
                    if row is None:
                        if not self.create_returned:
                            raise SecondaryFixtureError("isolated fixture create outcome uncertain")
                    else:
                        if not self.owned(row):
                            raise SecondaryFixtureError("isolated fixture cleanup ownership mismatch")
                        self.container_id = row["Id"]
                        self.registry.record(self, "cleanup-owned")
                        if self.docker(["rm", "-f", "-v", self.container_id], timeout=12).returncode:
                            raise SecondaryFixtureError("isolated fixture cleanup failed")
                self.registry.record(self, "closed")
                self.closed = True
            except (OSError, SecondaryFixtureError, subprocess.SubprocessError):
                try:
                    self.registry.record(self, "cleanup-unconfirmed")
                except OSError:
                    pass
            return self.closed
        finally:
            self.lock.release()


SECONDARY_FIXTURES = SecondaryFixtures()


def run_step(
    step: dict,
    job: dict,
    root: Path,
    port: int,
    base_sha: str | None,
    allow_skips: bool,
    stream: bool,
    carry: dict[str, str] | None = None,
    *,
    job_id: str | None = None,
    node_binding: NodeJobBinding | None = None,
) -> tuple[bool, str, bool]:
    if node_binding:
        error = node_control_error(os.environ, job.get("env") or {}, carry or {}, step.get("env") or {})
        if error:
            return False, error, False
        if job_id is not None and node_binding.job_id != job_id:
            return False, "Node binding belongs to another job", False
    env = os.environ.copy()
    env.update({k: rewrite_dsn(str(v), port) for k, v in (job.get("env") or {}).items()})
    # Values a previous step exported through $GITHUB_ENV.
    env.update(carry or {})
    # Resolve the workflow's env context, never an ambient host value or a
    # sibling step-env entry whose availability would depend on iteration order.
    workflow_env = {**(job.get("env") or {}), **(carry or {})}
    for k, v in (step.get("env") or {}).items():
        v = str(v)
        if "${{" in v:
            reference = re.fullmatch(r"\$\{\{\s*env\.([A-Za-z_][A-Za-z0-9_]*)\s*\}\}", v)
            if k == "BASE_REF":
                # The local equivalent of the PR base is the fork point.
                v = base_sha or ""
            elif reference:
                name = reference.group(1)
                resolved = workflow_env.get(name)
                if resolved is None or not str(resolved) or "${{" in str(resolved):
                    return False, f"unresolved workflow env alias {k} <- {name}", False
                v = str(resolved)
            else:
                # Other Actions expressions remain unsupported. The exact
                # PostgreSQL service adapter below handles its own service ID.
                v = ""
        env[k] = rewrite_dsn(v, port)

    env.setdefault("CI", "true")
    # cargo colorizes only for a tty; we capture, so ask explicitly.
    env.setdefault("CARGO_TERM_COLOR", "always" if BOLD else "never")
    # Setting the outer interpreter is not enough: steps like
    # `run: bash scripts/check_migrations.sh` spawn whatever `bash` PATH finds,
    # which on macOS is 3.2. Put modern Bash before the effective PATH;
    # a selected Node binding must precede even that directory's host Node.
    prefix = ([node_binding.runtime.bin] if node_binding else []) + [str(Path(BASH).parent)]
    env["PATH"] = os.pathsep.join([*prefix, env.get("PATH", "")])
    if TARGET_DIR:
        env["CARGO_TARGET_DIR"] = TARGET_DIR
    # Bound this step's OWN compile parallelism to CPU_COUNT // active Postgres
    # lanes, read fresh on every step so it shrinks as ramp-up adds lanes.
    # Without this, cargo defaults to using every logical CPU per invocation —
    # two lanes compiling at once is already ~2x oversubscribed; this is what
    # actually crashed -j4 (STATUS_DLL_INIT_FAILED), not container count.
    # Applies uniformly to every step, lane or plain — `rust` (the biggest
    # non-Postgres job, ~560s of cargo test --workspace) runs concurrently
    # ALONGSIDE the lanes, so it needs the same budget, not an unbounded one
    # that would just reintroduce the oversubscription this exists to fix.
    # Explicit env already set by a step wins (setdefault).
    env.setdefault("CARGO_BUILD_JOBS", str(LANE_BUDGET.build_jobs()))

    wd = root / (step.get("working-directory") or job.get("defaults", {}).get("run", {}).get("working-directory") or ".")

    # Some steps hardcode the service DSN in the SCRIPT rather than in `env:`
    # (rls-probe-harness provisions its judgement-graph database that way).
    # Env-only rewriting left those pointed at :5432 — a real Postgres on this
    # machine, but the wrong one, which failed with a role that does not exist
    # there. Rewrite the script text too, and say so, since editing the command
    # is a deviation from what CI literally runs.
    script = rewrite_dsn(step["run"], port)
    # Hosted Actions supplies a Docker service ID. Locally the lane is already
    # reachable on its assigned loopback port; use the helper's explicit host
    # transport only for this exact known workflow step. Do not carry the
    # maintenance DSN into the job's actual tests or prepare intentional catalogs.
    if script.strip() == 'bash scripts/prepare_ci_pg_roles.sh --container "$POSTGRES_SERVICE_ID"':
        if ((step.get("env") or {}).get("POSTGRES_SERVICE_ID") != "${{ job.services.postgres.id }}"
                or "postgres" not in (job.get("services") or {})):
            return False, "unrecognized PostgreSQL role-preparation service binding", False
        script = "bash scripts/prepare_ci_pg_roles.sh --localhost"
        env["DATABASE_URL"] = f"postgres://postgres:postgres@localhost:{port}/postgres"
        if stream:
            print(f"{DIM}    (adapted role preparation to this lane's localhost:{port}){RESET}")
    if rewrite_dsn(step["run"], port) != step["run"] and stream:
        print(f"{DIM}    (rewrote hardcoded :5432 → :{port} in this step){RESET}")

    script, binding_error = adapt_primary_fixture(step, job, script, env, port)
    if binding_error:
        return False, binding_error, False

    binding_error = adapt_teams_meeting_fixture(step, job, job_id, env, carry, port)
    if binding_error:
        return False, binding_error, False

    secondary, binding_error = secondary_fixture_binding(step, job, job_id)
    if binding_error:
        return False, binding_error, False
    if not secondary:
        return execute_step_process(step, script, env, wd, allow_skips, stream, carry)
    lease = OwnedSecondary(SECONDARY_FIXTURES, port)
    result = (False, "isolated fixture startup failed", False)
    try:
        lease.start()
        env["ORGT_TEST_BASE_URL"] = lease.dsn
        result = execute_step_process(step, script, env, wd, allow_skips, stream, None, lease)
        result = (result[0], result[1], False)  # never automatically retry a fresh-catalog step
    except (SecondaryFixtureError, OSError, subprocess.SubprocessError) as error:
        reason = str(error) if isinstance(error, SecondaryFixtureError) else "isolated fixture process failed"
        result = (False, reason, False)
    finally:
        cleaned = lease.close()
    if not cleaned:
        return False, (result[1] + "; " if result[1] else "") + "isolated fixture cleanup unconfirmed", False
    return result


def execute_step_process(step: dict, script: str, env: dict, wd: Path, allow_skips: bool,
                         stream: bool, carry: dict | None, lease: OwnedSecondary | None = None) -> tuple[bool, str, bool]:
    # GitHub gives every step four writable files and exports their paths.
    # Without them, any step doing `echo x >> "$GITHUB_OUTPUT"` dies on an
    # ambiguous redirect against the empty string.
    gh_files: dict[str, Path] = {}
    try:
        for name in ("GITHUB_OUTPUT", "GITHUB_ENV", "GITHUB_PATH", "GITHUB_STEP_SUMMARY"):
            path = Path(tempfile.mkdtemp(prefix="ghenv-")) / name.lower()
            gh_files[name] = path
            path.touch()
            env[name] = str(path)

        proc = (lease.spawn if lease else subprocess.Popen)(
            [BASH, "-eo", "pipefail", "-c", script],
            cwd=wd,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        chunks: list[str] = []
        assert proc.stdout is not None
        for line in proc.stdout:
            chunks.append(line)
            if stream:
                sys.stdout.write(f"{DIM}  │{RESET} {line}")
        code = proc.wait()
        out = "".join(chunks)

        # Carry $GITHUB_ENV / $GITHUB_PATH forward to later steps, as GitHub does.
        if carry is not None:
            for line in gh_files["GITHUB_ENV"].read_text(encoding="utf-8").splitlines():
                if "=" in line and not line.startswith("#"):
                    k, v = line.split("=", 1)
                    carry[k.strip()] = v
            added = [p for p in gh_files["GITHUB_PATH"].read_text(encoding="utf-8").splitlines() if p.strip()]
            if added:
                carry["PATH"] = ":".join(added) + ":" + carry.get("PATH", env.get("PATH", ""))
    finally:
        for path in gh_files.values():
            shutil.rmtree(path.parent, ignore_errors=True)

    if code != 0:
        if not stream:
            sys.stdout.write("".join(f"  │ {ln}" for ln in chunks[-40:]))
        crashed = is_crash_exit(code, out)
        if crashed:
            print(f"{YELLOW}    ! this looks like a process crash (exit {code}), not a test failure{RESET}")
        return False, f"exit {code}", crashed

    # --- vacuous-pass guard (see module docstring) --------------------------
    if "cargo test" in step["run"] and not allow_skips:
        lines = sorted({m.group(0).strip() for m in SKIP_LINE.finditer(out)})
        if lines:
            print(f"{RED}  ✗ VACUOUS PASS{RESET} — the test binary ran but its database gate was never satisfied,")
            print(f"    so {len(lines)} test(s) returned early and reported success without asserting anything:")
            for ln in lines[:8]:
                print(f"      {DIM}{ln.strip()}{RESET}")
            if len(lines) > 8:
                print(f"      {DIM}… and {len(lines) - 8} more{RESET}")
            print(f"    The DSN env var is unset or unreachable. This is a FAILURE, not a pass.")
            return False, f"{len(lines)} tests skipped their database gate", False

    return True, "", False


# ---------------------------------------------------------------------------
# Job execution
# ---------------------------------------------------------------------------


def isolated_checkout(root: Path, paths: list[str]) -> Path:
    """Extract tracked files into a temp dir, reproducing a runner's checkout.

    `git archive` gives exactly what `actions/checkout` would: tracked files,
    and crucially no `node_modules` anywhere above them.

    Caveat worth stating out loud: this is HEAD, so uncommitted changes are not
    included. Callers warn when the tree is dirty.
    """
    tmp = Path(tempfile.mkdtemp(prefix="coralswarm-ci-iso-"))
    archive = subprocess.run(
        ["git", "archive", "HEAD", *paths], cwd=root, stdout=subprocess.PIPE, check=True
    )
    subprocess.run(["tar", "-x", "-C", str(tmp)], input=archive.stdout, check=True)
    return tmp


def run_job(
    jid: str,
    job: dict,
    root: Path,
    port: int,
    base_sha: str | None,
    allow_skips: bool,
    stream: bool,
    *,
    node_binding: NodeJobBinding | None = None,
) -> JobResult:
    try:
        declaration = node_declaration(jid, job)
    except NodeBindingError as error:
        return JobResult(jid, "fail", str(error), failed_steps=["Node setup"])
    if ((declaration is None) != (node_binding is None)
            or node_binding is not None and (node_binding.job_id != jid
                or declaration != (node_binding.action_index, node_binding.runtime.major))):
        return JobResult(jid, "fail", "missing verified Node binding", failed_steps=["Node setup"])
    name = job.get("name", jid)
    print(f"\n{BOLD}{BLUE}▶ {jid}{RESET} {DIM}{name}{RESET}")
    started = time.time()
    failed = False
    failed_steps: list[str] = []
    carry: dict[str, str] = {}   # values steps export via $GITHUB_ENV / $GITHUB_PATH

    # Prove the database gates cannot swallow this job's tests before spending
    # minutes compiling them. See the module docstring.
    if "services" in job and not allow_skips:
        problems = verify_dsn_env(jid, job, port)
        if problems:
            print(f"  {RED}✗ database preflight — these tests would pass without asserting anything:{RESET}")
            for p in problems:
                print(f"      {p}")
            print(f"    {DIM}Every Postgres test here returns early (and PASSES) when its DSN is unusable.{RESET}")
            return JobResult(jid, "fail", "", time.time() - started, ["database preflight"])
        print(f"  {DIM}✓ database preflight — DSN reachable, CREATEDB+CREATEROLE held{RESET}")

    # Jobs that boot the server need two ports a hosted runner always has free.
    # A laptop does not: a dev backend left running since yesterday held :8081,
    # so the binary exited with "failed to bind internal listener" and the job
    # spent 60s polling a process that was already dead. Move the internal
    # listener somewhere free, and fail fast with a name if the public port —
    # which the readiness curl hardcodes — is taken.
    boots_server = job_boots_server(job)
    pre_existing = pids_on(8080) if boots_server else set()
    if boots_server:
        holder = port_in_use(8080)
        if holder:
            print(f"  {RED}✗ port 8080 is held by {holder}{RESET}")
            print(f"    {DIM}This job's readiness check curls 127.0.0.1:8080 and cannot be moved.{RESET}")
            return JobResult(jid, "fail", "", time.time() - started, ["port 8080 in use"])
        job.setdefault("env", {})["INTERNAL_PORT"] = str(free_port())

    if "services" in job:
        db_err = ensure_job_database(job, port)
        if db_err:
            print(f"  {RED}✗ {db_err}{RESET}")
            return JobResult(jid, "fail", "", time.time() - started, ["job database"])

    # Some jobs cannot run in place (see ISOLATED_JOBS). Give them a checkout
    # that looks like a runner's, and clean it up afterwards.
    iso: Path | None = None
    if jid in ISOLATED_JOBS and leaked_node_modules(root):
        paths, why = ISOLATED_JOBS[jid]
        try:
            iso = isolated_checkout(root, paths)
        except subprocess.CalledProcessError as exc:
            return JobResult(jid, "fail", "", time.time() - started, [f"isolated checkout: {exc}"])
        print(f"  {DIM}⧉ isolated checkout — {why}{RESET}")
        if sh(["git", "status", "--porcelain=1", "--", *paths], cwd=root):
            print(f"  {YELLOW}!{RESET} uncommitted changes under {'/'.join(paths)} are NOT in this"
                  f" checkout — it is built from HEAD")

    job_crashed = False
    for step_index, step in enumerate(job.get("steps", [])):
        if node_binding and step_index == node_binding.action_index:
            print("  " + json.dumps(node_binding.provenance(), sort_keys=True))
            continue  # source setup position; provisioning was validated locally
        label = step.get("name") or (step.get("uses", "") or "run").split("@")[0]
        ok, why = should_run_step(step, failed)
        if not ok:
            if why and not why.startswith("runner setup"):
                print(f"  {DIM}⊘ {label} — {why}{RESET}")
            continue
        if why.startswith("unrecognized"):
            print(f"  {YELLOW}! {label} — {why}; running it{RESET}")

        print(f"  {BOLD}·{RESET} {label}")
        good, reason, crashed = run_step(step, job, iso or root, port, base_sha, allow_skips, stream, carry,
                                        job_id=jid, node_binding=node_binding)
        if not good:
            print(f"  {RED}✗ {label} — {reason}{RESET}")
            failed = True
            failed_steps.append(label)
            if crashed:
                # A crash likely means the toolchain/machine itself is under
                # duress (the exact -j4 STATUS_DLL_INIT_FAILED case this
                # exists for) — running more steps on it is unlikely to
                # produce a meaningful result, so stop here rather than
                # working through the rest of a possibly-poisoned run. The
                # caller (lane_worker) decides whether to requeue the whole
                # job based on `JobResult.crashed`.
                job_crashed = True
                break

    if iso:
        shutil.rmtree(iso, ignore_errors=True)

    # Reap the server this job backgrounded. `mcp-e2e` does
    # `nohup target/debug/coralswarm-backend &` and never kills it — harmless on
    # an ephemeral runner that is destroyed minutes later, but on a laptop the
    # process outlives the run and holds :8080, so the NEXT run of this job
    # cannot bind and fails on a port conflict it caused itself.
    #
    # Only pids that appeared during this job are touched: anything already
    # listening beforehand is someone else's (a dev backend, say) and must be
    # left alone.
    if boots_server:
        for pid in pids_on(8080) - pre_existing:
            try:
                os.kill(pid, signal.SIGTERM)
                print(f"  {DIM}⏹ stopped the backend this job started (pid {pid}){RESET}")
            except (ProcessLookupError, PermissionError):
                pass

    secs = time.time() - started
    if failed:
        print(f"{RED}✗ {jid} failed{RESET} {DIM}({secs:.0f}s){RESET}")
        return JobResult(jid, "fail", "", secs, failed_steps, crashed=job_crashed)
    print(f"{GREEN}✓ {jid} passed{RESET} {DIM}({secs:.0f}s){RESET}")
    return JobResult(jid, "pass", "", secs)


# ---------------------------------------------------------------------------
# Split-job execution (SPLITTABLE_JOBS only — see that constant's own doc)
# ---------------------------------------------------------------------------


def split_job_setup(jid: str, job: dict, port: int, allow_skips: bool) -> str | None:
    """Preflight + `ensure_job_database` for a SPLITTABLE_JOBS entry, run
    ONCE per (lane, job) pairing rather than once per step — a split job's
    steps are independent of EACH OTHER, but each still runs inside a base
    database this lane provisions once, same as any other Postgres job.
    Returns an error string, or `None` on success.
    """
    if not allow_skips:
        problems = verify_dsn_env(jid, job, port)
        if problems:
            return "database preflight — these tests would pass without asserting anything"
    return ensure_job_database(job, port)


def run_split_step(
    job: dict,
    step_idx: int,
    root: Path,
    port: int,
    base_sha: str | None,
    allow_skips: bool,
    node_binding: NodeJobBinding | None = None,
    *,
    job_id: str | None = None,
) -> tuple[str, bool, str, bool]:
    """Run exactly one step of a SPLITTABLE_JOBS job, independently of its
    siblings' pass/fail — `should_run_step` is called with `failed=False`
    ALWAYS, which still honors its other skip reasons (ubuntu-only runner
    housekeeping, cache-hit guards) but never the "a previous step failed"
    cascade (that cascade is exactly what independence-auditing is meant to
    make safe to skip — see SPLITTABLE_JOBS's doc). No $GITHUB_ENV carry
    between steps either: a job audited as sharing no state between steps has
    nothing to carry, by the same reasoning.

    Returns (label, good, reason, crashed). `good=True, reason="<skip
    reason>"` when `should_run_step` said not to run it at all — that still
    counts toward this job's step-count bookkeeping, it just isn't a real
    pass or fail.
    """
    step = job["steps"][step_idx]
    label = step.get("name") or (step.get("uses", "") or "run").split("@")[0]
    ok, why = should_run_step(step, False)
    if not ok:
        return label, True, why, False
    good, reason, crashed = run_step(step, job, root, port, base_sha, allow_skips, stream=False, carry=None,
                                     job_id=job_id if job_id == "cross-tenant-probe" else None,
                                     node_binding=node_binding)
    return label, good, reason, crashed


@dataclass
class SplitProgress:
    """Shared, thread-safe bookkeeping for one SPLITTABLE_JOBS job's steps,
    which may complete on any number of different lanes over the run — only
    once EVERY step has reported in can a single aggregated JobResult be
    built for it (see `main`'s `lane_worker`/`finish_split_step`).

    `setup_failed` exists because `split_job_setup` can itself fail (a real
    error, not per-step) — and with the job's remaining steps scattered
    arbitrarily across the shared queue (already claimed by other lanes,
    not necessarily contiguous), there is no reliable way for the lane that
    discovers it to "drain the rest" of this job's queued items itself. This
    flag lets every OTHER lane, on its own next step for the same job, see
    the failure was already reported and skip re-attempting setup or
    double-appending a JobResult, checked-and-set atomically under
    `split_progress_lock` so only the FIRST lane to hit it actually reports.
    """

    total: int
    done: int = 0
    failed_steps: list[str] = field(default_factory=list)
    crashed_steps: list[str] = field(default_factory=list)
    setup_failed: bool = False
    start: float = 0.0
    end: float = 0.0


# ---------------------------------------------------------------------------
# Receipt — the local stand-in for deploy.yml's `gate: CI success` job
# ---------------------------------------------------------------------------

def receipt_path(root: Path) -> Path:
    """Where the receipt lives for THIS working tree.

    Resolved through `git rev-parse`, not hardcoded as `.git/…`: inside a
    worktree `.git` is a *file* pointing at
    `<main>/.git/worktrees/<name>`, so a hardcoded path would raise NotADirectory
    here — and every branch in this repo is developed in a worktree. Using the
    per-worktree gitdir also keeps each worktree's verdict its own, which is what
    you want when three branches are in flight at once.
    """
    return Path(sh(["git", "rev-parse", "--absolute-git-dir"], cwd=root)) / "ci-local-receipt"


def write_receipt(root: Path, sha: str | None, ran: list[JobResult]) -> None:
    """Record which commit passed which jobs, for deploy_local.sh to check.

    Written under the gitdir rather than the working tree on purpose: it is
    machine state, must never be committed, and must not make the tree dirty
    (the deploy script refuses to ship a dirty tree).

    A failed run writes a receipt with no sha, so a stale green receipt from an
    earlier run can never be mistaken for the current verdict.
    """
    body = [
        f"sha={sha or ''}",
        f"passed={'yes' if sha else 'no'}",
        f"when={time.strftime('%Y-%m-%dT%H:%M:%S%z')}",
        f"jobs={','.join(r.job for r in ran if r.status == 'pass')}",
    ]
    try:
        receipt_path(root).write_text("\n".join(body) + "\n")
    except (OSError, subprocess.CalledProcessError):
        pass  # a missing receipt just means deploy asks for --skip-ci-check


def mark_run_started(selected: dict[str, dict]) -> None:
    """Tell the caller that jobs are about to execute — see RAN_MARKER_ENV.

    Call this EXACTLY ONCE, from the single point where every environment gate
    has already passed: after `preflight`, `RunLock.acquire`, `modern_bash` and
    the base `ensure_pg`. That placement is the whole contract. Everything
    before it can abort for reasons that say nothing about the code under test;
    everything after it either produces a `JobResult` or is a real failure.
    `--self-test` asserts the placement, so a later edit cannot quietly undo it.

    Unlike the receipt this is not repo state and nothing in here reads it back:
    it is a one-bit handshake with whoever invoked this script, and its lifetime
    belongs to that caller (the pre-push hook mktemps it and traps its removal).
    """
    path = os.environ.get(RAN_MARKER_ENV)
    if not path:
        return
    try:
        # Non-empty on purpose: callers test for a non-empty file, so a
        # placeholder some caller mktemp'd and never filled still reads as "no".
        Path(path).write_text(",".join(selected) + "\n", encoding="utf-8")
    except OSError as exc:
        # A marker we cannot write must never take down a run that is otherwise
        # fine. The caller then sees no marker and publishes nothing, which is
        # the safe direction — but say so, rather than leaving a silent gap.
        print(f"{YELLOW}!{RESET} could not write {RAN_MARKER_ENV}={path} ({exc}) — "
              f"no verdict will be published for this run")


# ---------------------------------------------------------------------------
# Self-test: prove the vacuous-pass detector actually fires
# ---------------------------------------------------------------------------


def self_test() -> int:
    """A guard nobody has watched fail is a guard you are trusting on faith."""
    ok = True

    # --- the real guard: the DSN preflight -----------------------------------
    print(f"{BOLD}self-test: database preflight (the primary vacuous-pass guard){RESET}")
    fake_job = {"services": {"postgres": {}}, "env": {"ORGT_TEST_BASE_URL": "postgres://nope:nope@localhost:1/postgres"}}
    if verify_dsn_env("fake", fake_job, 1):
        print(f"  {GREEN}✓ positive control{RESET}: an unreachable DSN is rejected before tests run")
    else:
        print(f"  {RED}✗ positive control: an unreachable DSN was accepted{RESET}")
        ok = False

    no_dsn = {"services": {"postgres": {}}, "env": {"CARGO_PROFILE_DEV_DEBUG": "line-tables-only"}}
    if verify_dsn_env("fake", no_dsn, 1):
        print(f"  {GREEN}✓ positive control{RESET}: a Postgres job with no DSN env var is rejected")
    else:
        print(f"  {RED}✗ positive control: a job with no DSN was accepted{RESET}")
        ok = False

    port = int(os.environ.get("CI_LOCAL_PG_PORT", DEFAULT_PG_PORT))
    live = {"services": {"postgres": {}}, "env": {"ORGT_TEST_BASE_URL": f"postgres://postgres:postgres@localhost:{port}/postgres"}}
    if shutil.which("psql") and docker_ok():
        probs = verify_dsn_env("fake", live, port)
        if probs:
            print(f"  {DIM}· negative control skipped — no live container on :{port} ({probs[0][:60]}){RESET}")
        else:
            print(f"  {GREEN}✓ negative control{RESET}: a working DSN on :{port} is accepted")
    else:
        print(f"  {DIM}· negative control skipped — psql or docker unavailable{RESET}")

    # --- the secondary net ---------------------------------------------------
    print(f"\n{BOLD}self-test: output scan (secondary net){RESET}")
    vacuous = (
        "running 3 tests\n"
        "skipping consumer_rls_enforces_under_app_role: set ORGT_TEST_BASE_URL\n"
        "skipping begin_account_scoped_round_trips: set ORGT_TEST_BASE_URL\n"
        "test result: ok. 3 passed; 0 failed; 0 ignored\n"
    )
    real = "running 3 tests\ntest result: ok. 3 passed; 0 failed; 0 ignored\n"

    if len(SKIP_LINE.findall(vacuous)) != 2:
        print(f"  {RED}✗ skip lines not detected when they ARE visible{RESET}")
        ok = False
    else:
        print(f"  {GREEN}✓{RESET} visible skip lines are caught (only surface under --nocapture)")
    if SKIP_LINE.findall(real):
        print(f"  {RED}✗ clean run flagged as vacuous{RESET}")
        ok = False
    else:
        print(f"  {GREEN}✓{RESET} a genuine passing run is not flagged")
    print(f"  {YELLOW}!{RESET} {DIM}this scan alone is NOT sufficient — libtest hides these lines for"
          f" passing tests, which is why the DSN preflight above is the real guard{RESET}")

    # Regression guard: this looked only at root.parents, so `marketing` was
    # isolated correctly in every worktree and silently built in place on main.
    print(f"\n{BOLD}self-test: node_modules detection{RESET}")
    with tempfile.TemporaryDirectory() as td:
        base = Path(td)
        (base / "repo" / "node_modules").mkdir(parents=True)
        (base / "repo" / "worktrees" / "wt").mkdir(parents=True)
        (base / "clean").mkdir()
        for label, path, want in (
            ("at the root (the main-checkout case)", base / "repo", True),
            ("above the root (the worktree case)", base / "repo" / "worktrees" / "wt", True),
            ("absent (a runner's checkout)", base / "clean", False),
        ):
            got = leaked_node_modules(path) is not None
            if got != want:
                print(f"  {RED}✗ {label}: expected {want}, got {got}{RESET}")
                ok = False
            else:
                print(f"  {GREEN}✓{RESET} {label}")

    print(f"\n{BOLD}self-test: path-filter globs{RESET}")

    # The glob translator backs the path filter that decides which lanes run;
    # a wrong answer here silently under-runs CI.
    cases: list[tuple[str, str, bool]] = [
        ("backend/**", "backend/src/main.rs", True),
        ("backend/**", "web/app/page.tsx", False),
        (".github/workflows/ci.yml", ".github/workflows/ci.yml", True),
        ("web/**", "web/lib/apiBase.ts", True),
        ("packages/**", "packages/core/src/embed_ort.rs", True),
    ]
    for glob, path, want in cases:
        got = bool(glob_to_re(glob).match(path))
        if got != want:
            print(f"  {RED}✗ glob {glob!r} vs {path!r}: expected {want}, got {got}{RESET}")
            ok = False
    if ok:
        print(f"  {GREEN}✓ path-filter globs{RESET}: {len(cases)} cases correct")

    print(f"\n{BOLD}self-test: this file defines nothing twice{RESET}")

    # #627 landed a second copy of the whole configuration-and-helpers block
    # spliced in at what had been the end of `RunLock.release`. Python takes the
    # later definition, so the runner behaved correctly and nothing failed —
    # which is exactly why it survived: 633 duplicated lines are invisible to
    # every check we had. The two halves had already drifted (`ensure_pg` grew a
    # `container` parameter in one of them only), so the next edit to a shadowed
    # function would have been a silent no-op.
    #
    # Imported here rather than at module scope: this is the only thing in the
    # runner that needs to read its own source.
    import ast

    dupes: dict[str, int] = {}
    for node in ast.parse(Path(__file__).read_text(encoding="utf-8")).body:
        names = []
        if isinstance(node, (ast.FunctionDef, ast.ClassDef)):
            names = [node.name]
        elif isinstance(node, ast.Assign):
            for target in node.targets:
                # The colour constants are one tuple assignment, so unpack it —
                # a duplicated config block repeats that line too.
                elts = target.elts if isinstance(target, ast.Tuple) else [target]
                names += [e.id for e in elts if isinstance(e, ast.Name)]
        for name in names:
            dupes[name] = dupes.get(name, 0) + 1
    shadowed = sorted(n for n, c in dupes.items() if c > 1)
    if shadowed:
        print(f"  {RED}✗ {len(shadowed)} top-level name(s) defined more than once{RESET} — "
              f"the earlier definition is dead code: {', '.join(shadowed[:6])}")
        ok = False
    else:
        print(f"  {GREEN}✓{RESET} {len(dupes)} top-level definitions, none shadowed")

    # --- the run marker: "did any job actually run?" -------------------------
    #
    # The pre-push hook publishes a `local-ci` commit status off this signal, so
    # getting it wrong means a red on GitHub blaming code that was never tested.
    # Two things have to hold, and both are checked here rather than trusted:
    # the marker behaves, and it is still written from the right PLACE.
    print(f"\n{BOLD}self-test: run marker (what tells a caller a verdict exists){RESET}")
    with tempfile.TemporaryDirectory() as td:
        marker = Path(td) / "ran"
        prior = os.environ.get(RAN_MARKER_ENV)
        try:
            os.environ[RAN_MARKER_ENV] = str(marker)
            mark_run_started({"rust": {}, "web": {}})
            if marker.is_file() and marker.read_text(encoding="utf-8").strip() == "rust,web":
                print(f"  {GREEN}✓ positive control{RESET}: a started run writes a non-empty marker")
            else:
                print(f"  {RED}✗ positive control: no usable marker after mark_run_started{RESET}")
                ok = False

            marker.unlink(missing_ok=True)
            del os.environ[RAN_MARKER_ENV]
            mark_run_started({"rust": {}})
            if marker.exists():
                print(f"  {RED}✗ negative control: wrote a marker nobody asked for{RESET}")
                ok = False
            else:
                print(f"  {GREEN}✓ negative control{RESET}: no ${RAN_MARKER_ENV}, no marker")

            # An unwritable marker must degrade to silence, never take the run
            # down — the caller then publishes nothing, which is the safe way
            # to be wrong.
            os.environ[RAN_MARKER_ENV] = str(Path(td) / "nope" / "ran")
            try:
                mark_run_started({"rust": {}})
                print(f"  {GREEN}✓{RESET} an unwritable marker path warns instead of raising")
            except OSError:
                print(f"  {RED}✗ an unwritable marker path killed the run{RESET}")
                ok = False
        finally:
            if prior is None:
                os.environ.pop(RAN_MARKER_ENV, None)
            else:
                os.environ[RAN_MARKER_ENV] = prior

    # PLACEMENT is the actual contract (see mark_run_started): every gate that
    # can abort `main` for an environment reason — no repo, no bash 4, missing
    # tools, Docker down, lock held — must run BEFORE the marker, and nothing
    # after it may `sys.exit`. Assert it against the parse tree so moving the
    # call, or adding a sixth gate underneath it, fails here instead of
    # silently posting red for a run that never started. Only calls `main`
    # makes itself count: `start_lane`'s nested `ensure_pg(required=False)`
    # raises rather than exiting, on purpose, and sits after the marker.
    main_fn = next(
        (n for n in ast.walk(ast.parse(Path(__file__).read_text(encoding="utf-8")))
         if isinstance(n, ast.FunctionDef) and n.name == "main"),
        None,
    )
    own_calls: list[tuple[str, int]] = []

    def _collect(node: "ast.AST") -> None:
        for child in ast.iter_child_nodes(node):
            if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef, ast.Lambda)):
                continue  # a nested def runs on its own terms, not main's
            if isinstance(child, ast.Call):
                fn = child.func
                name = getattr(fn, "id", None) or getattr(fn, "attr", None)
                if name:
                    own_calls.append((name, child.lineno))
            _collect(child)

    if main_fn is None:
        print(f"  {RED}✗ could not find main() to check marker placement{RESET}")
        ok = False
    else:
        _collect(main_fn)
        marked = [ln for name, ln in own_calls if name == "mark_run_started"]
        gates = ("repo_root", "preflight", "acquire", "modern_bash", "ensure_pg")
        if len(marked) != 1:
            print(f"  {RED}✗ main() calls mark_run_started {len(marked)} time(s), expected exactly 1{RESET}")
            ok = False
        else:
            at = marked[0]
            # A gate that vanished (renamed, refactored away) would make this
            # check pass by testing nothing, so require each one to be present.
            missing = [g for g in gates if not any(n == g for n, _ in own_calls)]
            late = sorted({g for g in gates for n, ln in own_calls if n == g and ln > at})
            after = sorted({ln for n, ln in own_calls if n == "exit" and ln > at})
            if missing:
                print(f"  {RED}✗ gate(s) no longer called in main{RESET}: {', '.join(missing)} — "
                      f"this check would pass vacuously")
                ok = False
            elif late:
                print(f"  {RED}✗ environment gate(s) run AFTER the marker{RESET}: {', '.join(late)}")
                ok = False
            elif after:
                print(f"  {RED}✗ main() can sys.exit after the marker{RESET} (line(s) {after}) — "
                      f"that exit code would publish as a verdict")
                ok = False
            else:
                print(f"  {GREEN}✓{RESET} all {len(gates)} environment gates precede the marker, "
                      f"and main() cannot exit after it")

    print(f"\n{GREEN if ok else RED}self-test {'passed' if ok else 'FAILED'}{RESET}")
    return 0 if ok else 1


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------


def main() -> int:
    p = argparse.ArgumentParser(
        prog="ci_local.py",
        description="Run .github/workflows/ci.yml locally, natively, with no GitHub minutes.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""examples:
  scripts/ci_local.py                    # jobs whose paths changed vs main
  scripts/ci_local.py --full             # everything runnable on this machine
  scripts/ci_local.py --job rust,web     # specific jobs
  scripts/ci_local.py --list             # what would run, and why
  scripts/ci_local.py --self-test        # prove the vacuous-pass guard fires
""",
    )
    p.add_argument("--job", help="comma-separated job ids (see --list)")
    p.add_argument("--full", action="store_true", help="run every locally-runnable job")
    p.add_argument("--list", action="store_true", help="show selection and exit")
    p.add_argument("--self-test", action="store_true", help="verify this runner's own guards")
    p.add_argument("--node-bin", action="append", default=[], metavar="MAJOR=/absolute/runtime/bin",
                   help="bind a selected workflow Node major to its exact local sibling installation (repeatable)")
    p.add_argument("--base", default="main", help="branch to diff against (default: main)")
    p.add_argument("--pg-port", type=int, default=int(os.environ.get("CI_LOCAL_PG_PORT", DEFAULT_PG_PORT)))
    p.add_argument(
        "-j", "--jobs", type=int, default=None,
        help="max concurrent Postgres lanes, each its own throwaway container. "
             "Default: adaptive — starts at 1 lane and ramps up while free host "
             "RAM stays above $CI_LOCAL_MIN_FREE_RAM_PCT (default 20%%), up to "
             "the CPU count. Pass a number (or set $CI_LOCAL_JOBS) for the old "
             "fixed, deterministic behavior instead — -j 1 for fully sequential. "
             "Non-Postgres jobs always run alongside every lane, since they "
             "touch no shared state.",
    )
    p.add_argument("--force", action="store_true",
                   help="start even if another run holds the lock (only if you are certain it is dead)")
    p.add_argument("--no-pg", action="store_true", help="skip jobs that need Postgres")
    p.add_argument("--keep-pg", action="store_true", help="leave the container running (default)")
    p.add_argument("--stop-pg", action="store_true", help="stop the postgres/minio containers when done")
    p.add_argument("--allow-skips", action="store_true",
                   help="do NOT fail on env-gated test skips (only for a deliberate no-database run)")
    p.add_argument("-q", "--quiet", action="store_true", help="only stream output for failing steps")
    args = p.parse_args()
    # None here means "-j wasn't passed" — distinct from an explicit value,
    # since that's what decides fixed-vs-adaptive lane count below. Only
    # fall back to $CI_LOCAL_JOBS (still a fixed, deterministic value, same
    # as passing -j) when the flag itself was omitted.
    if args.jobs is None and "CI_LOCAL_JOBS" in os.environ:
        args.jobs = int(os.environ["CI_LOCAL_JOBS"])

    if args.self_test:
        return self_test()

    root = repo_root()
    wf = yaml.safe_load((root / WORKFLOW).read_text(encoding="utf-8"))
    jobs: dict[str, dict] = wf["jobs"]

    base_sha = merge_base(root, args.base)
    results: list[JobResult] = []

    # ---- selection ------------------------------------------------------
    for jid, why in UNRUNNABLE.items():
        if jid in jobs:
            results.append(JobResult(jid, "skip", why))
    candidates = {k: v for k, v in jobs.items() if k not in UNRUNNABLE}

    if args.job:
        wanted = [j.strip() for j in args.job.split(",") if j.strip()]
        unknown = [j for j in wanted if j not in jobs]
        if unknown:
            sys.exit(f"unknown job(s): {', '.join(unknown)}\nknown: {', '.join(jobs)}")
        selected = {j: jobs[j] for j in wanted if j not in UNRUNNABLE}
        rationale = "explicitly requested"
    elif args.full:
        selected = dict(candidates)
        rationale = "--full"
    else:
        files = changed_files(root, base_sha)
        lanes = active_lanes(parse_change_filters(wf), files)
        selected = {}
        for jid, job in candidates.items():
            need = job_needs_lane(job)
            if not need or (need & lanes):
                selected[jid] = job
            else:
                results.append(
                    JobResult(jid, "skip", f"no changes under {'/'.join(sorted(need))}")
                )
        rationale = f"{len(files)} changed file(s) → lanes: {', '.join(sorted(lanes)) or 'none'}"

    needs_pg = {j for j, spec in selected.items() if "services" in spec}
    if args.no_pg:
        for j in sorted(needs_pg):
            results.append(JobResult(j, "skip", "--no-pg"))
            selected.pop(j)
        needs_pg = set()

    # Jobs that ALSO declare a `minio` service (member-oceans, rls-probe-
    # harness — see `MINIO_CONTAINER`'s doc comment). A subset of `needs_pg`
    # today (every such job needs Postgres too), computed separately rather
    # than folded into it because the two containers are started/torn down
    # independently.
    needs_minio = {j for j, spec in selected.items() if "minio" in (spec.get("services") or {})}

    # `lanes:` above counts only the dorny/paths-filter lanes that matched. It
    # is NOT a job count, and reading it as one makes the next line look like a
    # contradiction — "no lane matched, running 10 jobs anyway". Jobs whose
    # `if:` carries no `needs.changes.outputs.X == 'true'` condition are
    # unconditional in ci.yml, so they run on every PR on real Actions too and
    # are selected here regardless of which lanes fired. Say so rather than
    # leaving the reader to infer it from the workflow: two people read the
    # `lanes: none` / `selected: 10 job(s)` pairing as a bug in this runner's
    # selection logic, and one of them nearly filed it.
    #
    # Counted AFTER the macOS and --no-pg filtering above, so it can never
    # disagree with the `selected:` line it is there to explain.
    if not args.job and not args.full:
        unconditional = sum(1 for spec in selected.values() if not job_needs_lane(spec))
        if unconditional:
            rationale += f" (+{unconditional} unconditional)"

    print(f"{BOLD}local CI{RESET} {DIM}({rationale}){RESET}")
    print(f"  base:     {args.base} @ {(base_sha or '?')[:12]}")
    print(f"  selected: {len(selected)} job(s){f', {len(needs_pg)} need Postgres' if needs_pg else ''}")

    if args.list:
        for jid in selected:
            print(f"  {GREEN}run {RESET}{jid}{'  (postgres)' if jid in needs_pg else ''}")
        for r in results:
            print(f"  {DIM}skip {r.job} — {r.reason}{RESET}")
        return 0

    if not selected:
        print(f"{YELLOW}nothing to run{RESET} — no lane matched. Use --full to force.")
        return 0

    try:
        node_bindings = resolve_node_bindings(selected, args.node_bin, sh(["git", "rev-parse", "HEAD"], cwd=root))
    except NodeBindingError as error:
        p.error(str(error))
    preflight(selected, node_bindings)
    warn_node_resolution_leak(root, selected)
    run_lock = RunLock(root)
    run_lock.acquire(args.force)
    atexit.register(run_lock.release)
    global BASH, TARGET_DIR
    BASH = modern_bash()
    TARGET_DIR = link_target_dir(root) or ""
    print(f"  bash:     {BASH} ({sh([BASH, '-c', 'echo $BASH_VERSION'])})")
    if TARGET_DIR and any("cargo" in s.get("run", "") for j in selected.values() for s in j.get("steps", [])):
        print(f"  target:   {TARGET_DIR} {DIM}(shared across worktrees){RESET}")
    if needs_pg:
        ensure_pg(args.pg_port, quiet=args.quiet)
        print(f"  postgres: localhost:{args.pg_port} ({PG_CONTAINER})")
    if needs_minio:
        # One shared container for the whole run (not lane-scoped like
        # Postgres — see `MINIO_CONTAINER`'s doc comment for why that's
        # sufficient): every job that declares `minio` uses the identical
        # block today, so the first one found is authoritative.
        minio_service = selected[sorted(needs_minio)[0]]["services"]["minio"]
        ensure_minio(minio_service, quiet=args.quiet)
        minio_port, _ = minio_port_mapping(minio_service)
        print(f"  minio:    localhost:{minio_port} ({MINIO_CONTAINER})")

    # ---- execute --------------------------------------------------------
    #
    # The environment is now known good, so from here on this process's exit
    # code is a verdict on the code and a caller may publish it. Not one line
    # earlier — see RAN_MARKER_ENV.
    SECONDARY_FIXTURES.install()
    mark_run_started(selected)
    started = time.time()
    postgres_jobs = [(jid, job) for jid, job in selected.items() if jid in needs_pg]
    plain_jobs = [(jid, job) for jid, job in selected.items() if jid not in needs_pg]
    lane_containers: list[str] = [PG_CONTAINER]

    # A single SPLITTABLE_JOBS selection still has real, independently
    # schedulable work (its own many steps) — `len(selected) <= 1` alone
    # would wrongly send it down the plain sequential path below, which
    # runs `run_job` directly and never even looks at SPLITTABLE_JOBS.
    # Explicit -j 1 still forces fully sequential regardless (Q20's
    # escape hatch), even for a splittable job.
    single_unsplittable = len(selected) <= 1 and not any(jid in SPLITTABLE_JOBS for jid in selected)
    if single_unsplittable or (args.jobs is not None and args.jobs <= 1):
        # Unchanged sequential path — exactly the old behavior/output, and
        # what a plain `-j 1` (or a single non-splittable job run) still gets.
        for jid, job in selected.items():
            results.append(
                run_job(jid, job, root, args.pg_port, base_sha, args.allow_skips, not args.quiet,
                        node_binding=node_bindings.get(jid))
            )
    else:
        # ---- parallel lanes --------------------------------------------
        #
        # `ensure_job_database` (see its own doc comment) DROPS EVERY user
        # database on its container before each Postgres job runs, to
        # reproduce a fresh-runner's empty server. Two jobs sharing ONE
        # container concurrently would delete each other's databases
        # mid-test — silent, confusing, unrelated-to-the-code failures.
        # GitHub Actions avoids this by giving every job its own runner
        # (and so its own Postgres service container); the local
        # equivalent is giving every LANE its own container. Only mcp-e2e
        # binds a fixed port (8080) and there is only one such job in the
        # whole workflow, so no cross-lane lock is needed for it.
        #
        # A JOB-LEVEL QUEUE, not a static round-robin assignment, because
        # lane count is no longer fixed upfront in auto mode (see below) —
        # lanes come online progressively as RAM allows, so which lane runs
        # which job can't be decided before the run starts. Every lane
        # thread pulls its next item from this one shared queue; a lane that
        # steps down (crash-retry) or never starts just leaves more for the
        # others.
        #
        # An item is either ("job", jid, job, None) — a whole, un-split job,
        # run exactly as `run_job` always has — or ("step", jid, job, i) —
        # one step of a SPLITTABLE_JOBS entry, run independently via
        # `run_split_step` and NOT requiring the whole job to land on one
        # lane. A split job's steps are pushed together as one contiguous
        # block (not interleaved with other jobs'), which is what gives
        # job-affinity its effect below without needing an explicit
        # same-job preference in the dequeue itself: a lane pulling several
        # items in a row from a block mostly gets the same jid, so it only
        # re-runs per-job setup (`split_job_setup`/`ensure_job_database`)
        # when its own last-seen jid actually changes.
        #
        # Longest-first: blocks are ordered by each job's last-observed
        # wall-clock duration (persisted across runs, see DURATION_CACHE) —
        # unknown durations sort FIRST, both because that's a reasonable
        # default (better to learn the expensive ones early) and because it
        # means a job's first-ever run always gets measured promptly.
        durations = load_durations()
        ordered_jobs = sorted(postgres_jobs, key=lambda kv: -durations.get(kv[0], float("inf")))

        # The lane ceiling is bounded by how many INDEPENDENTLY schedulable
        # queue items actually exist, not how many jobs were selected — a
        # single SPLITTABLE_JOBS job with 25 steps is 25 potential units of
        # concurrent work, not 1; `len(postgres_jobs)` alone would silently
        # cap ramp-up at 1 lane for exactly the case splitting exists for.
        total_items = sum(
            len(job.get("steps", [])) if jid in SPLITTABLE_JOBS else 1
            for jid, job in postgres_jobs
        )
        max_lanes = (
            min(args.jobs, max(1, total_items)) if args.jobs is not None
            else min(CPU_COUNT, max(1, total_items))
        ) if postgres_jobs else 0

        # Item = (kind, jid, job, step_idx) — kind is "job" (step_idx unused,
        # always None) or "step" (step_idx indexes job["steps"]).
        job_queue: "queue.Queue[tuple[str, str, dict, int | None]]" = queue.Queue()
        split_progress: dict[str, SplitProgress] = {}
        split_progress_lock = threading.Lock()
        for jid, job in ordered_jobs:
            if jid in SPLITTABLE_JOBS:
                steps = job.get("steps", [])
                split_progress[jid] = SplitProgress(total=len(steps), start=time.time())
                for i in range(len(steps)):
                    job_queue.put(("step", jid, job, i))
            else:
                job_queue.put(("job", jid, job, None))

        results_lock = threading.Lock()
        retried_lock = threading.Lock()
        retried_once: set[str] = set()

        lane_ports: list[int] = []
        lane_containers = []
        lanes_lock = threading.Lock()
        lane_threads: list[threading.Thread] = []

        def finish_split_step(jid: str, job: dict, label: str, good: bool, crashed: bool) -> None:
            with split_progress_lock:
                sp = split_progress[jid]
                sp.done += 1
                if not good:
                    sp.failed_steps.append(label)
                if crashed:
                    sp.crashed_steps.append(label)
                complete = sp.done >= sp.total
                if complete:
                    sp.end = time.time()
            if not complete:
                return
            secs = sp.end - sp.start
            if sp.failed_steps:
                print(f"{RED}✗ {jid} failed{RESET} {DIM}({secs:.0f}s, "
                      f"{len(sp.failed_steps)}/{sp.total} step(s) failed){RESET}")
                result = JobResult(jid, "fail", "", secs, sp.failed_steps, crashed=False)
            else:
                print(f"{GREEN}✓ {jid} passed{RESET} {DIM}({secs:.0f}s, {sp.total} step(s), split){RESET}")
                result = JobResult(jid, "pass", "", secs)
            with results_lock:
                results.append(result)
            durations[jid] = secs

        def lane_worker(port: int) -> None:
            last_setup_jid: str | None = None
            while True:
                try:
                    kind, jid, job, step_idx = job_queue.get_nowait()
                except queue.Empty:
                    return

                if kind == "job":
                    r = run_job(jid, job, root, port, base_sha, args.allow_skips, stream=False,
                                node_binding=node_bindings.get(jid))
                    last_setup_jid = None  # run_job does its own setup every time
                    if r.crashed:
                        with retried_lock:
                            already = jid in retried_once
                            retried_once.add(jid)
                        if not already:
                            # First crash for this job: hand it back to the
                            # queue for a sibling lane, and this lane steps
                            # down (unless it's the last one — then it must
                            # keep working rather than orphan the requeued
                            # job). Never retry a REAL test failure — only a
                            # process crash (JobResult.crashed), once.
                            job_queue.put(("job", jid, job, None))
                            if LANE_BUDGET.try_step_down():
                                print(f"  {YELLOW}!{RESET} {jid} crashed — requeued, this lane "
                                      f"is stepping down ({LANE_BUDGET.get()} lane(s) remain)")
                                return
                            print(f"  {YELLOW}!{RESET} {jid} crashed — this is the last lane, "
                                  f"retrying it directly instead of requeuing")
                            continue
                        print(f"  {RED}✗{RESET} {jid} crashed again after one retry — reporting as failed")
                    with results_lock:
                        results.append(r)
                    durations[jid] = r.seconds
                    job_queue.task_done()
                    continue

                # kind == "step": a SPLITTABLE_JOBS step, independent of its
                # siblings (Q15 — no cascade-skip, no requeue-the-whole-job).
                #
                # `setup_failed` is checked FIRST, before this lane attempts
                # its own setup: another lane may already have discovered
                # (and reported) a setup failure for this job. Its remaining
                # steps are scattered arbitrarily across the shared queue —
                # not necessarily contiguous, since other lanes may already
                # have claimed some — so there is no reliable way to "drain"
                # them; every lane just checks this flag on its own turn.
                with split_progress_lock:
                    already_dead = split_progress[jid].setup_failed
                if already_dead:
                    with split_progress_lock:
                        split_progress[jid].done += 1
                    job_queue.task_done()
                    continue

                if jid != last_setup_jid:
                    err = split_job_setup(jid, job, port, args.allow_skips)
                    if err:
                        with split_progress_lock:
                            sp = split_progress[jid]
                            first = not sp.setup_failed
                            sp.setup_failed = True
                            sp.done += 1
                            if first:
                                sp.failed_steps.append(err)
                                sp.end = time.time()
                        if first:
                            print(f"  {RED}✗ {jid}: {err}{RESET}")
                            durations[jid] = sp.end - sp.start
                            with results_lock:
                                results.append(JobResult(jid, "fail", "", sp.end - sp.start, sp.failed_steps))
                        last_setup_jid = None
                        job_queue.task_done()
                        continue
                    last_setup_jid = jid

                step_key = f"{jid}::{step_idx}"
                label, good, reason, crashed = run_split_step(
                    job, step_idx, root, port, base_sha, args.allow_skips, node_binding=node_bindings.get(jid),
                    job_id=jid,
                )
                if not good:
                    # A should_run_step skip already returns good=True from
                    # run_split_step, so reaching here with good=False always
                    # means a genuine step failure, never a skip.
                    print(f"  {RED}✗ {jid} · {label} — {reason}{RESET}")
                if crashed:
                    with retried_lock:
                        already = step_key in retried_once
                        retried_once.add(step_key)
                    if not already:
                        job_queue.put(("step", jid, job, step_idx))
                        if LANE_BUDGET.try_step_down():
                            print(f"  {YELLOW}!{RESET} {jid} · {label} crashed — requeued this "
                                  f"step, this lane is stepping down ({LANE_BUDGET.get()} lane(s) remain)")
                            return
                        continue
                    good = False  # crashed twice: report as a real failure, do not retry a third time
                finish_split_step(jid, job, label, good, crashed)
                job_queue.task_done()

        def start_lane() -> bool:
            """Returns False (without touching LANE_BUDGET or lane_threads)
            if this lane's own container failed to start — the caller (the
            ramp-up loop) stops ramping up further, but everything already
            running is unaffected. See `ensure_pg(required=False)`'s own doc
            for why this must never let a container failure take the whole
            process down.
            """
            with lanes_lock:
                i = len(lane_threads)
                port = args.pg_port if i == 0 else free_port()
                container = PG_CONTAINER if i == 0 else f"{PG_CONTAINER}-{i}"
            if i > 0:
                try:
                    ensure_pg(port, quiet=args.quiet, container=container, required=False)
                except (PgStartupFailed, subprocess.CalledProcessError) as exc:
                    print(f"  {YELLOW}!{RESET} lane {i + 1} failed to start ({exc}) — "
                          f"stopping ramp-up at {i} lane(s), continuing with those")
                    return False
                # LANE_BUDGET starts at 1 (its default), matching this first
                # lane already — only additional lanes need to grow it. Kept
                # relative (not `set(i + 1)`) so a lane that stepped down
                # earlier isn't overwritten back up to a stale, too-high
                # count — see LaneBudget.increment's own doc.
                LANE_BUDGET.increment()
            with lanes_lock:
                lane_ports.append(port)
                lane_containers.append(container)
            print(f"  {DIM}postgres: lane {i + 1}/{max_lanes} started "
                  f"({container}:{port}){RESET}")
            t = threading.Thread(target=lane_worker, args=(port,), daemon=True)
            t.start()
            with lanes_lock:
                lane_threads.append(t)
            return True

        def run_plain(jid: str, job: dict) -> JobResult:
            return run_job(jid, job, root, args.pg_port, base_sha, args.allow_skips, stream=False,
                           node_binding=node_bindings.get(jid))

        if plain_jobs:
            print(f"  parallel: {len(plain_jobs)} non-Postgres job(s) run alongside every lane")
        plain_ex = (
            concurrent.futures.ThreadPoolExecutor(max_workers=len(plain_jobs)) if plain_jobs else None
        )
        plain_futures = (
            [plain_ex.submit(run_plain, jid, job) for jid, job in plain_jobs] if plain_ex else []
        )

        if max_lanes:
            start_lane()

        if args.jobs is not None:
            # Explicit -j N: today's deterministic behavior — every lane
            # starts immediately, no ramp-up. A lane failing to start (rare —
            # this path historically requested at most 2-4) stops the rest;
            # a repeat failure is more likely than a one-off at that point.
            for _ in range(1, max_lanes):
                if not start_lane():
                    break
        else:
            # Adaptive ramp-up: poll free HOST ram (not Docker's — cargo
            # runs natively) every 5s; start the next lane whenever headroom
            # clears the floor and there's still queued work. `None` from
            # free_ram_pct() means "can't verify" and must NOT ramp up —
            # fail toward fewer lanes, not more. A lane failing to start
            # (observed for real: Docker itself couldn't keep up once RAM
            # headroom let ramp-up race to 10 lanes in ~50s) stops ramp-up
            # for the rest of this run rather than retrying every 5s.
            while len(lane_threads) < max_lanes and not job_queue.empty():
                time.sleep(5)
                if job_queue.empty():
                    break
                pct = free_ram_pct()
                if pct is not None and pct > RAM_FLOOR_PCT:
                    if not start_lane():
                        break

        for t in lane_threads:
            t.join()
        if plain_ex:
            for fut in concurrent.futures.as_completed(plain_futures):
                results.append(fut.result())
            plain_ex.shutdown()

        if len(lane_containers) > 1:
            print(f"  postgres: {len(lane_containers)} lane(s) used in total "
                  f"({', '.join(f'{c}:{p}' for c, p in zip(lane_containers, lane_ports))})")

        # Parallel completion order is scheduling order, not selection
        # order — re-sort so the summary below reads the same regardless
        # of how many lanes ran or which one finished first.
        order = {jid: i for i, jid in enumerate(selected)}
        results.sort(key=lambda r: order.get(r.job, len(order)))

        save_durations(durations)

    if args.stop_pg and needs_pg:
        for c in lane_containers:
            subprocess.run(["docker", "stop", c], capture_output=True)
    if args.stop_pg and needs_minio:
        subprocess.run(["docker", "stop", MINIO_CONTAINER], capture_output=True)

    if not SECONDARY_FIXTURES.close():
        results.append(JobResult("isolated-secondary-cleanup", "fail", "cleanup unconfirmed"))

    # ---- summary --------------------------------------------------------
    ran = [r for r in results if r.status != "skip"]
    failed = [r for r in ran if r.status == "fail"]
    skipped = [r for r in results if r.status == "skip"]

    print(f"\n{BOLD}{'─' * 64}{RESET}\n{BOLD}summary{RESET} {DIM}({time.time() - started:.0f}s total){RESET}")
    for r in ran:
        mark = f"{GREEN}✓{RESET}" if r.status == "pass" else f"{RED}✗{RESET}"
        print(f"  {mark} {r.job:34s} {DIM}{r.seconds:5.0f}s{RESET}"
              + (f"  {RED}{', '.join(r.failed_steps)}{RESET}" if r.failed_steps else ""))
    if skipped:
        print(f"\n{DIM}not run ({len(skipped)}):{RESET}")
        for r in skipped:
            print(f"  {DIM}⊘ {r.job:34s} {r.reason}{RESET}")

    if failed:
        write_receipt(root, None, ran)
        print(f"\n{RED}{BOLD}FAILED{RESET} — {len(failed)} of {len(ran)} job(s): "
              f"{', '.join(r.job for r in failed)}")
        return 1

    # deploy.yml's `gate` job refuses to deploy unless CI concluded success on
    # this exact commit. With Actions unavailable that gate vanishes entirely,
    # so record the pass here and let scripts/deploy_local.sh enforce it.
    write_receipt(root, sh(["git", "rev-parse", "HEAD"], cwd=root), ran)
    print(f"\n{GREEN}{BOLD}PASSED{RESET} — {len(ran)} job(s). "
          f"{DIM}Deploy with scripts/deploy_local.sh{RESET}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
