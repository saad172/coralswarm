#!/usr/bin/env python3
"""Bounded Fly release receipts and web recovery admission. No third-party modules."""
import argparse
import datetime
import json
import os
from pathlib import Path
import re
import subprocess
import time
import tomllib
import urllib.error
import urllib.request
import uuid

DIGEST = re.compile(r"sha256:[0-9a-f]{64}\Z")
SHA = re.compile(r"[0-9a-f]{40}\Z")
SERVER_TAG = re.compile(r"server-[0-9]{4}\.[0-9]{2}\.[0-9]{2}\.[0-9]+\Z")
PROBES = {
    "coralswarm-web": ["https://app.coralswarm.example/login", "https://coralswarm-web.example/login"],
    "coralswarm-backend": ["https://api.coralswarm.example/readyz", "https://coralswarm-backend.example/readyz"],
    "coralswarm-backend-staging": ["https://coralswarm-backend-staging.example/readyz"],
}


def command(*args):
    result = subprocess.run(args, capture_output=True, text=True, timeout=90)
    if result.returncode:
        # Never copy CLI output/headers/credentials into the public receipt.
        raise ValueError(f"{args[0]} command failed (exit {result.returncode})")
    return result.stdout


def save(path, data):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2) + "\n")


def pushed_digest(log, app):
    matches = set(re.findall(
        rf"pushing manifest for registry\.fly\.io/{re.escape(app)}:[^\s@]+@(sha256:[0-9a-f]{{64}})(?=\s|$)", log))
    if len(matches) != 1:
        raise ValueError("expected exactly one immutable pushed image digest")
    return matches.pop()


def observe_machines(status, sha, digest):
    if not SHA.fullmatch(sha) or not DIGEST.fullmatch(digest):
        raise ValueError("expected full source SHA and immutable digest")
    machines = [m for m in status.get("Machines", []) if m.get("config", {}).get("services")]
    observations, errors = [], []
    if not machines or not any(m.get("state") == "started" for m in machines):
        errors.append("no started serving machine")
    for m in machines:
        ref = m.get("image_ref", {})
        observed = {"id": m.get("id"), "state": m.get("state"),
                    "host_status": m.get("host_status"), "digest": ref.get("digest"),
                    "source_sha": ref.get("labels", {}).get("GH_SHA")}
        observations.append(observed)
        if observed["digest"] != digest or observed["source_sha"] != sha:
            errors.append(f"machine {m.get('id')} image/source mismatch (including stopped serving machines)")
        if observed["state"] not in ("started", "stopped") or observed["host_status"] != "ok":
            errors.append(f"machine {m.get('id')} is not in a healthy stable state")
    return observations, errors


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        return None


def get(url, token=None):
    headers = {"User-Agent": "CoralSwarm-deployment-acceptance/1"}
    if token:
        headers["Authorization"] = "Bearer " + token
    start = time.monotonic()
    try:
        # Do not follow redirects, especially with an Authorization header.
        with urllib.request.build_opener(NoRedirect).open(
                urllib.request.Request(url, headers=headers), timeout=15) as response:
            body = response.read(1_000_001)
            if len(body) > 1_000_000:
                raise ValueError("response exceeds smoke-test limit")
            return response.status, body, round(time.monotonic() - start, 3)
    except urllib.error.HTTPError as error:
        return error.code, b"", round(time.monotonic() - start, 3)


def application_smoke():
    token = os.environ.get("DEPLOY_SMOKE_TOKEN", "")
    graph = os.environ.get("DEPLOY_SMOKE_GRAPH_ID", "")
    required = os.environ.get("DEPLOY_REQUIRE_APP_SMOKE", "false").lower() == "true"
    if not token and not graph:
        return {"status": "not_configured", "required": required}, (["authenticated smoke required but not configured"] if required else [])
    if not token or not graph:
        return {"status": "failed"}, ["authenticated smoke requires both dedicated token and workspace fixture ID"]
    try:
        graph = str(uuid.UUID(graph))
        probes = []
        for kind, path in [("stats", f"/v1/graphs/{graph}/stats"),
                           ("topics", f"/v1/nodes?graph_id={graph}&page=1&page_size=1")]:
            status, body, elapsed = get("https://api.coralswarm.example" + path, token)
            # Retain neither response data, fixture ID, nor token in receipts.
            probes.append({"kind": kind, "http_status": status, "seconds": elapsed})
            if status != 200:
                raise ValueError(f"authenticated {kind} returned HTTP {status}")
            data = json.loads(body)
            if kind == "stats" and not (isinstance(data, dict) and all(isinstance(data.get(k), int) and not isinstance(data[k], bool)
                                           and data[k] >= 0 for k in ("reef_total", "atom_count", "source_count"))):
                raise ValueError("authenticated stats response shape invalid")
            if kind == "topics" and not isinstance(data, list):
                raise ValueError("authenticated topics response shape invalid")
        return {"status": "passed", "probes": probes}, []
    except (ValueError, OSError, urllib.error.URLError):
        return {"status": "failed"}, ["authenticated workspace read smoke failed (status/shape/timeout); inspect fixture access without logging credentials"]


def verify(args):
    receipt = {"source_sha": args.sha, "app": args.app,
               "observed_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
               "infrastructure": "failed", "application": {"status": "not_run"}}
    errors = []
    try:
        digest = args.digest or json.loads(Path(args.candidate).read_text())["digest"]
        receipt["expected_digest"] = digest
        # Probe first: a scale-to-zero app (staging) has no started machine until a request wakes one.
        receipt["public_probes"], probe_errors = [], []
        for url in PROBES[args.app]:
            code, _, elapsed = get(url)
            receipt["public_probes"].append({"url": url, "http_status": code, "seconds": elapsed})
            if code != 200:
                probe_errors.append(f"public probe {url} returned HTTP {code}")
        status = json.loads(command("flyctl", "status", "--app", args.app, "--json"))
        receipt["machines"], errors = observe_machines(status, args.sha, digest)
        errors.extend(probe_errors)
        if not errors:
            receipt["infrastructure"] = "passed"
        if args.app == "coralswarm-web" and not errors:
            receipt["application"], app_errors = application_smoke()
            errors.extend(app_errors)
    except (ValueError, OSError, KeyError, urllib.error.URLError, subprocess.TimeoutExpired) as error:
        category = type(getattr(error, "reason", error)).__name__
        errors.append(f"release observation failed or expected image identity unavailable ({category})")
    receipt["errors"] = errors
    save(args.receipt, receipt)
    if os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(os.environ["GITHUB_STEP_SUMMARY"], "a") as summary:
            summary.write(f"### {args.app} release verification\n\n")
            summary.write(f"- Serving image and public routes: {receipt['infrastructure']}\n")
            summary.write(f"- Authenticated workspace acceptance: {receipt['application']['status']}\n")
            if receipt['application']['status'] == 'not_configured':
                summary.write("\n**Authenticated application acceptance is NOT tested.** Configure a dedicated fixture before requiring it.\n")
    print(f"{args.app}: infrastructure={receipt['infrastructure']}; authenticated application={receipt['application']['status']}")
    return int(bool(errors))


def admit_source(run, jobs, log, digest):
    # A release run's head_branch is the release tag. Pushes to main stopped deploying
    # with the release flow, but their historical runs remain valid recovery sources.
    branch, event = run.get("head_branch") or "", run.get("event")
    source_ok = ((event in ("push", "workflow_dispatch") and branch == "main")
                 or (event == "release" and SERVER_TAG.fullmatch(branch)))
    if (run.get("path") != ".github/workflows/deploy.yml" or not source_ok
            or run.get("status") != "completed" or not SHA.fullmatch(run.get("head_sha", ""))):
        raise ValueError("recovery source must be a completed main or server-release Deploy run")
    web = [j for j in jobs if j.get("name") == "Deploy web (Fly)" and j.get("conclusion") in ("success", "failure")]
    if len(web) != 1 or pushed_digest(log, "coralswarm-web") != digest:
        raise ValueError("digest is not uniquely proven by the source run's web push log")
    return run["head_sha"]


def prepare_recovery(args):
    if not DIGEST.fullmatch(args.digest) or not re.fullmatch(r"[0-9a-f]{14}", args.machine):
        raise ValueError("invalid immutable digest or machine ID")
    repo = os.environ["GITHUB_REPOSITORY"]
    run = json.loads(command("gh", "api", f"repos/{repo}/actions/runs/{args.run_id}"))
    jobs = json.loads(command("gh", "api", f"repos/{repo}/actions/runs/{args.run_id}/jobs?filter=latest&per_page=100"))["jobs"]
    web = [j for j in jobs if j["name"] == "Deploy web (Fly)"]
    if len(web) != 1:
        raise ValueError("source run has no unique production web job")
    log = command("gh", "run", "view", str(args.run_id), "--repo", repo, "--job", str(web[0]["id"]), "--log")
    sha = admit_source(run, jobs, log, args.digest)
    command("git", "merge-base", "--is-ancestor", sha, "origin/main")
    config = command("git", "show", f"{sha}:web/fly.toml")
    # A historical config change needs a separate review, not replay by this recovery tool.
    if config != Path("web/fly.toml").read_text():
        raise ValueError("source web config differs from current reviewed config")
    parsed = tomllib.loads(config)
    if parsed.get("app") != "coralswarm-web" or any(k in parsed for k in ("deploy", "mounts", "processes")):
        raise ValueError("web recovery config contains unsupported rollout side effects")
    status = json.loads(command("flyctl", "status", "--app", "coralswarm-web", "--json"))
    selected = [m for m in status.get("Machines", []) if m.get("id") == args.machine]
    if len(selected) != 1 or selected[0].get("state") != "started" or not selected[0].get("config", {}).get("services"):
        raise ValueError("target must be an existing started serving web machine")
    Path(args.config).write_text(config)
    save(args.receipt, {"source_run": args.run_id, "source_sha": sha, "digest": args.digest, "machine": args.machine})
    with open(os.environ["GITHUB_OUTPUT"], "a") as out:
        out.write(f"sha={sha}\n")


def release_outcome(needs, dry_run):
    gate = needs.get("gate", {})
    outputs = gate.get("outputs", {})
    results = {name: value["result"] for name, value in needs.items() if name != "gate"}
    expected = []
    if not dry_run:
        if outputs.get("deploy_production") == "true":
            expected += ["deploy-backend", "deploy-web", "deploy-marketing"]
        if outputs.get("deploy_staging") == "true":
            expected += ["deploy-backend-staging"]
            if outputs.get("deploy_web_staging") == "true":
                expected += ["deploy-web-staging"]
        mode = outputs.get("mode")
        if mode == "staging":
            expected += ["prepare-backend-staging"]
        if mode == "build":
            expected += ["build-backend-release"]
        elif mode == "release":
            # The gate between staging and production, and the record a rollback reads.
            expected += ["require-release-build", "smoke-staging", "record-release"]
        elif mode == "rollback":
            expected += ["rollback-backend", "rollback-web"]
    if gate.get("result") != "success":
        outcome = "failed_before_deploy"
    elif not expected:
        outcome = "not_deployed"
    elif all(results.get(name) == "success" for name in expected):
        outcome = "components_succeeded"
    else:
        outcome = "partial_or_failed"
    return {"desired_sha": outputs.get("sha"), "outcome": outcome, "expected_jobs": expected, "jobs": results,
            "application_acceptance": "See web verification receipt. Missing/not_configured is NOT a pass."}


def summary(args):
    receipt = release_outcome(json.loads(os.environ["RELEASE_NEEDS"]), os.environ.get("RELEASE_DRY_RUN") == "true")
    save(args.receipt, receipt)
    with open(os.environ["GITHUB_STEP_SUMMARY"], "a") as out:
        out.write("## Release outcome: " + receipt["outcome"] + "\n\n")
        for name, result in receipt["jobs"].items():
            out.write(f"- {name}: {result}\n")
        out.write("\nServing versions/public probes are in release receipts. Authenticated application acceptance is separate; not_configured is not tested.\n")
    return int(receipt["outcome"] in ("partial_or_failed", "failed_before_deploy"))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="action", required=True)
    candidate = sub.add_parser("candidate")
    candidate.add_argument("--app", choices=PROBES, required=True)
    candidate.add_argument("--log", required=True)
    candidate.add_argument("--receipt", required=True)
    verification = sub.add_parser("verify")
    verification.add_argument("--app", choices=PROBES, required=True)
    verification.add_argument("--sha", required=True)
    identity = verification.add_mutually_exclusive_group(required=True)
    identity.add_argument("--digest")
    identity.add_argument("--candidate")
    verification.add_argument("--receipt", required=True)
    recovery = sub.add_parser("prepare-recovery")
    recovery.add_argument("--run-id", type=int, required=True)
    recovery.add_argument("--digest", required=True)
    recovery.add_argument("--machine", required=True)
    recovery.add_argument("--config", required=True)
    recovery.add_argument("--receipt", required=True)
    reporting = sub.add_parser("summary")
    reporting.add_argument("--receipt", required=True)
    args = parser.parse_args()
    try:
        if args.action == "candidate":
            save(args.receipt, {"digest": pushed_digest(Path(args.log).read_text(), args.app)})
        elif args.action == "verify":
            return verify(args)
        elif args.action == "prepare-recovery":
            prepare_recovery(args)
        else:
            return summary(args)
        return 0
    except (ValueError, OSError, KeyError, subprocess.TimeoutExpired) as error:
        # Admission errors contain no source logs or credential material.
        print(f"deployment admission failed: {type(error).__name__}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
