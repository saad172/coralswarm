#!/usr/bin/env python3
"""Build once, retain a compatible recovery image, then deploy its immutable digest."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import stat
import tempfile
import time
import tomllib
import uuid

CAPABILITY = {"financial_release": "account-entry-c1", "managed_recovery": "legacy-and-v1", "admission_disable": "MANAGED_PROCESSING_DISABLED"}
EXECUTABLE = "/app/coralswarm-funded-backend"
SCHEMA = "financial-image-v1"
RETENTION = "life-of-account-entry-c1-schema"
PREPARE_WORKFLOW = ".github/workflows/financial-recovery-prepare.yml"
STORE_SCHEMA = "financial-retained-store-v1"
# Per workflow that may prepare in CI: the job whose success in the preparing attempt admits
# the evidence, the branch that attempt must run on (None: any), and the apps it may prepare (None: any).
STORE_GATES = {".github/workflows/release.yml": ("Verify prepared release independently", "main", None),
               ".github/workflows/deploy.yml": ("Prepare backend image (staging)", None, {"coralswarm-backend-staging"})}


def sha256(data):
    return hashlib.sha256(data).hexdigest()


def validate_receipt(data, config_path, expected_sha=None, expected_source=None, current_config=True):
    if expected_sha is not None and sha256(data) != expected_sha:
        raise ValueError("receipt bytes differ from the expected SHA256")
    receipt = json.loads(data)
    config = tomllib.loads(config_path.read_text())
    if not re.fullmatch(r"[a-z0-9][a-z0-9-]*", config["app"]):
        raise ValueError("invalid Fly application name")
    if (receipt["schema"] != SCHEMA or receipt["app"] != config["app"]
            or receipt["capability"] != CAPABILITY or receipt["executable"] != EXECUTABLE
            or receipt["retention"] != RETENTION):
        raise ValueError("receipt contract/application differs from the current financial release")
    immutable(receipt["image"], receipt["app"])
    immutable(receipt["recovery_floor"], receipt["app"])
    if not re.fullmatch(r"[0-9a-f]{40}", receipt["source_commit"]):
        raise ValueError("receipt requires the selected image's exact source commit")
    if expected_source is not None and receipt["source_commit"] != expected_source:
        raise ValueError("selected release source differs from the prepared image receipt")
    if not re.fullmatch(r"[0-9a-f]{64}", receipt["config_sha256"]):
        raise ValueError("receipt requires the original config SHA256")
    if current_config and receipt["config_sha256"] != sha256(config_path.read_bytes()):
        raise ValueError("current config differs from the retained receipt")
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,95}", receipt["generation"]):
        raise ValueError("invalid operator receipt generation")
    return receipt


def private_directory(path):
    if not path.exists():
        private_directory(path.parent)
        path.mkdir(mode=0o700)
        descriptor = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    info = path.lstat()
    if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) & 0o077:
        raise ValueError("operator receipt directory must be owned by this user with mode 0700: " + str(path))


def atomic_receipt(path, data):
    temporary = path.parent / ("." + path.name + "." + uuid.uuid4().hex)
    try:
        descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(descriptor, "wb") as output:
            output.write(data)
            output.flush()
            os.fsync(output.fileno())
        os.link(temporary, path, follow_symlinks=False)
        descriptor = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    finally:
        if temporary.exists():
            temporary.unlink()
    if path.read_bytes() != data:
        raise ValueError("durable receipt readback differs from published bytes")


def retention_evidence(data, path):
    receipt = json.loads(data)
    return {"schema": "financial-operator-retention-v1", "retention": RETENTION,
            "receipt_sha256": sha256(data), "path": str(path), "app": receipt["app"],
            "generation": receipt["generation"]}


def validate_retention(data, evidence):
    receipt = json.loads(data)
    if evidence != retention_evidence(data, Path(evidence["path"])) or not Path(evidence["path"]).is_absolute():
        raise ValueError("operator retention evidence differs from the receipt")
    if Path(evidence["path"]).parts[-3:] != (receipt["app"], receipt["generation"], "financial-image.json"):
        raise ValueError("operator retention location differs from application/generation")


def retain(data, config_path, root):
    receipt = validate_receipt(data, config_path)
    root = root.expanduser().absolute()
    # The operator chooses a persistent filesystem. A runner must only verify its copy.
    if os.environ.get("GITHUB_ACTIONS") == "true":
        raise ValueError("operator lifetime retention cannot be created on a CI runner")
    if any(parent.is_symlink() for parent in (root, *root.parents)):
        raise ValueError("operator retention path must not contain symlinks")
    missing = []
    ancestor = root
    while not ancestor.exists():
        missing.append(ancestor)
        ancestor = ancestor.parent
    for directory in reversed(missing):
        directory.mkdir(mode=0o700)
        descriptor = os.open(directory.parent, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    private_directory(root)
    destination = root / receipt["app"] / receipt["generation"] / "financial-image.json"
    private_directory(destination.parent)
    if destination.exists():
        if destination.is_symlink() or destination.read_bytes() != data:
            raise ValueError("refusing to replace the first retained receipt")
        info = destination.stat()
        if info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) != 0o600:
            raise ValueError("retained receipt must be owned by this user with mode 0600")
    else:
        atomic_receipt(destination, data)
    evidence = retention_evidence(destination.read_bytes(), destination)
    sidecar = destination.with_name("operator-retention.json")
    encoded = (json.dumps(evidence, indent=2) + "\n").encode()
    if sidecar.exists():
        if sidecar.is_symlink() or sidecar.read_bytes() != encoded:
            raise ValueError("refusing to replace operator retention evidence")
        info = sidecar.stat()
        if info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) != 0o600:
            raise ValueError("operator evidence must be owned by this user with mode 0600")
    else:
        atomic_receipt(sidecar, encoded)
    for path in [destination, sidecar]:
        with path.open("rb") as retained:
            os.fsync(retained.fileno())
    descriptor = os.open(destination.parent, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    if destination.read_bytes() != data or sidecar.read_bytes() != encoded:
        raise ValueError("operator retained-file readback differs from the original bytes")
    validate_retention(destination.read_bytes(), json.loads(sidecar.read_bytes()))
    return destination


def validate_ci_readback(data, evidence_data, proof, repository, prepare_run_id):
    receipt = json.loads(data)
    expected = {"schema": "financial-ci-readback-v1", "receipt_sha256": sha256(data),
                "operator_evidence_sha256": sha256(evidence_data), "app": receipt["app"],
                "image": receipt["image"], "source_commit": receipt["source_commit"],
                "config_sha256": receipt["config_sha256"], "repository": repository,
                "workflow": PREPARE_WORKFLOW}
    if (not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", repository)
            or any(proof.get(key) != value for key, value in expected.items())
            or str(proof.get("run_id")) != prepare_run_id or not re.fullmatch(r"[1-9][0-9]*", prepare_run_id)
            or not re.fullmatch(r"[0-9a-f]{40}", proof.get("workflow_commit", ""))):
        raise ValueError("CI readback does not bind the selected release receipt")
    run = json.loads(command("gh", "api", f"repos/{repository}/actions/runs/{proof['run_id']}", capture=True))
    if (run["status"] != "completed" or run["conclusion"] != "success"
            or run["path"] != PREPARE_WORKFLOW or run["head_sha"] != proof["workflow_commit"]):
        raise ValueError("prepare-only CI readback run has not succeeded at its recorded source")
    with tempfile.TemporaryDirectory(prefix="financial-ci-readback-") as directory:
        command("gh", "run", "download", prepare_run_id, "--repo", repository,
                "--name", "financial-release-verified", "--dir", directory)
        downloaded = Path(directory)
        if (downloaded.joinpath("financial-image.json").read_bytes() != data
                or downloaded.joinpath("operator-retention.json").read_bytes() != evidence_data
                or json.loads(downloaded.joinpath("ci-readback.json").read_bytes()) != proof):
            raise ValueError("successful CI artifact bytes differ from the selected receipt bundle")


def store_workflow(environment, app):
    repository = environment.get("GITHUB_REPOSITORY", "")
    workflow = environment.get("GITHUB_WORKFLOW_REF", "").removeprefix(repository + "/").split("@", 1)[0]
    if environment.get("GITHUB_ACTIONS") != "true" or workflow not in STORE_GATES:
        raise ValueError("retained-store preparation runs only in the release or staging deploy workflow")
    apps = STORE_GATES[workflow][2]
    if apps is not None and app not in apps:
        raise ValueError(f"{workflow} may prepare only {sorted(apps)}, not {app}")
    return repository, workflow


def store_evidence(data, capability_data, build_tag, environment):
    receipt = json.loads(data)
    repository, workflow = store_workflow(environment, receipt["app"])
    return {"schema": STORE_SCHEMA, "retention": RETENTION, "receipt_sha256": sha256(data),
            "capability_sha256": sha256(capability_data), "app": receipt["app"],
            "generation": receipt["generation"], "image": receipt["image"],
            "recovery_floor": receipt["recovery_floor"], "build_tag": build_tag,
            "source_commit": receipt["source_commit"], "config_sha256": receipt["config_sha256"],
            "repository": repository, "workflow": workflow, "run_id": environment["GITHUB_RUN_ID"],
            "run_attempt": environment["GITHUB_RUN_ATTEMPT"], "workflow_commit": environment["GITHUB_SHA"]}


def validate_store(data, evidence_data, capability_data):
    receipt = json.loads(data)
    evidence = json.loads(evidence_data)
    expected = {"schema": STORE_SCHEMA, "retention": RETENTION, "receipt_sha256": sha256(data),
                "capability_sha256": sha256(capability_data), "app": receipt["app"],
                "generation": receipt["generation"], "image": receipt["image"],
                "recovery_floor": receipt["recovery_floor"], "source_commit": receipt["source_commit"],
                "config_sha256": receipt["config_sha256"]}
    if (any(evidence.get(key) != value for key, value in expected.items())
            or evidence.get("workflow") not in STORE_GATES
            or not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", evidence.get("repository", ""))
            or not re.fullmatch(r"[1-9][0-9]*", str(evidence.get("run_id", "")))
            or not re.fullmatch(r"[1-9][0-9]*", str(evidence.get("run_attempt", "")))
            or not re.fullmatch(r"[0-9a-f]{40}", evidence.get("workflow_commit", ""))):
        raise ValueError("retained-store evidence does not bind the selected release receipt")
    apps = STORE_GATES[evidence["workflow"]][2]
    if apps is not None and receipt["app"] not in apps:
        raise ValueError(f"{evidence['workflow']} cannot admit an image for {receipt['app']}")
    if json.loads(capability_data) != CAPABILITY:
        raise ValueError("retained capability output differs from the current financial release")
    return evidence


def validate_store_run(evidence, repository, prepare_run_id):
    """The gate job succeeded in the very attempt that prepared the evidence; later re-runs neither add nor remove that."""
    if evidence["repository"] != repository or evidence["run_id"] != prepare_run_id:
        raise ValueError("retained-store evidence names a different repository or preparation run")
    attempt = f"repos/{repository}/actions/runs/{prepare_run_id}/attempts/{evidence['run_attempt']}"
    run = json.loads(command("gh", "api", attempt, capture=True))
    gate, branch, _ = STORE_GATES[evidence["workflow"]]
    if (run["path"] != evidence["workflow"] or run["head_sha"] != evidence["workflow_commit"]
            or run["event"] != "workflow_dispatch" or (branch and run["head_branch"] != branch)):
        raise ValueError("preparation run differs from the workflow, event, branch and commit the evidence requires")
    jobs = json.loads(command("gh", "api", attempt + "/jobs?per_page=100", capture=True))["jobs"]
    if [job["conclusion"] for job in jobs if job["name"] == gate] != ["success"]:
        raise ValueError(f"preparation attempt's '{gate}' job has not succeeded")


BUILD_ATTEMPTS = 3
BUILD_RETRY_DELAY_SECONDS = 30
# A healthy build-and-push takes 3-5 minutes. Three 12-minute attempts plus
# backoff stay inside the prepare job's 45-minute timeout.
BUILD_ATTEMPT_TIMEOUT_SECONDS = 12 * 60


def build_with_retries(build, attempts=BUILD_ATTEMPTS, delay=BUILD_RETRY_DELAY_SECONDS, sleep=time.sleep):
    """Run the remote image build, retrying a failed attempt under a fresh label.

    The remote builder drops connections mid-build (server-2026.10.29.2 failed on
    `rpc error: code = Unavailable ... EOF`) or hangs on the registry push
    (server-2026.10.29.7 stalled for 43 minutes), so a build that times out is
    retried like one that fails. Nothing is stored before the build
    returns, so a retry cannot collide with write-once evidence; each attempt gets
    its own label so a partial push from a failed attempt is never the target.
    """
    for attempt in range(1, attempts + 1):
        label = "financial-c1-" + uuid.uuid4().hex
        try:
            build(label)
            return label
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
            if attempt == attempts:
                raise
            print(f"image build attempt {attempt} of {attempts} failed or timed out; retrying in {delay * attempt}s", file=sys.stderr)
            sleep(delay * attempt)


def command(*args, capture=False, env=None, timeout=None):
    result = subprocess.run(args, check=True, text=True, stdout=subprocess.PIPE if capture else None, env=env,
                            timeout=timeout)
    return result.stdout.strip() if capture else None


def immutable(image, app):
    if not re.fullmatch(r"registry\.fly\.io/" + re.escape(app) + r"@sha256:[0-9a-f]{64}", image):
        raise ValueError("expected this application's immutable registry digest")
    return image


def pull(image, attempts=8, delay=15):
    # registry.fly.io answers "manifest unknown" for a freshly pushed tag for a short while.
    for _ in range(attempts - 1):
        try:
            return command("docker", "pull", image)
        except subprocess.CalledProcessError:
            time.sleep(delay)
    return command("docker", "pull", image)


def inspect_image(image, app):
    pull(image)
    digests = json.loads(command("docker", "image", "inspect", "--format", "{{json .RepoDigests}}", image, capture=True))
    matches = [d for d in digests if d.startswith(f"registry.fly.io/{app}@sha256:")]
    if len(matches) != 1:
        raise ValueError("image must resolve to one application digest")
    digest = immutable(matches[0], app)
    if "@sha256:" in image and digest != image:
        raise ValueError("resolved image differs from requested immutable digest")
    if json.loads(capability_output(digest)) != CAPABILITY:
        raise ValueError("image lacks the current financial recovery capability")
    return digest


def capability_output(digest):
    return command("docker", "run", "--rm", "--network", "none", "--entrypoint", EXECUTABLE,
                   digest, "--financial-release-capability", capture=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("operation", choices=["prepare", "retain", "verify", "deploy", "recover"])
    parser.add_argument("--config", required=True)
    parser.add_argument("--receipt", type=Path)
    parser.add_argument("--image", help="existing immutable image for preparation, or an explicit recovery override")
    parser.add_argument("--generation", help="unique operator receipt generation for prepare")
    parser.add_argument("--source-commit", help="exact source of the selected image, independently established for an existing image")
    parser.add_argument("--retention-root", type=Path, default=Path.home() / ".local/share/coralswarm/recovery/account-entry-c1")
    parser.add_argument("--operator-evidence", type=Path)
    parser.add_argument("--expected-sha256")
    parser.add_argument("--expected-source")
    parser.add_argument("--compare", type=Path, help="original receipt before the CI artifact round trip")
    parser.add_argument("--ci-readback", type=Path)
    parser.add_argument("--write-ci-readback", type=Path)
    parser.add_argument("--repository", default=os.environ.get("GITHUB_REPOSITORY"))
    parser.add_argument("--prepare-run-id")
    parser.add_argument("--candidate-receipt", type=Path, help="retain the admitted digest before attempting rollout")
    parser.add_argument("--store-dir", type=Path, help="prepare in CI for the release evidence store instead of operator disk")
    parser.add_argument("--store-evidence", type=Path, help="retained-store.json from a CI preparation")
    parser.add_argument("--capability", type=Path, help="capability.json beside --store-evidence")
    args = parser.parse_args()
    config_path = Path(args.config).resolve()
    config = tomllib.loads(config_path.read_text())
    app = config["app"]
    if not re.fullmatch(r"[a-z0-9][a-z0-9-]*", app):
        raise ValueError("invalid Fly application name")
    if config.get("processes", {}).get("app") != EXECUTABLE or config.get("deploy", {}).get("release_command") != "/app/release.sh":
        raise ValueError("use the current backend config and migration command")
    if args.candidate_receipt and args.operation != "deploy":
        raise ValueError("candidate receipt is only valid for normal deployment")
    if args.operation == "retain":
        print(retain(args.receipt.read_bytes(), config_path, args.retention_root))
        return
    if args.operation == "verify":
        if not args.expected_sha256 or not args.operator_evidence:
            raise ValueError("verify requires --expected-sha256 and --operator-evidence")
        data = args.receipt.read_bytes()
        receipt = validate_receipt(data, config_path, args.expected_sha256, args.expected_source)
        evidence_data = args.operator_evidence.read_bytes()
        validate_retention(data, json.loads(evidence_data))
        if args.compare and args.compare.read_bytes() != data:
            raise ValueError("downloaded artifact differs from the original receipt")
        if args.compare and args.compare.with_name("operator-retention.json").read_bytes() != evidence_data:
            raise ValueError("downloaded operator evidence differs from the original bytes")
        if args.ci_readback:
            if not all([args.expected_source, args.repository, args.prepare_run_id]):
                raise ValueError("verified bundle requires expected source, repository and prepare run ID")
            validate_ci_readback(data, evidence_data, json.loads(args.ci_readback.read_bytes()), args.repository, args.prepare_run_id)
        if args.write_ci_readback:
            if (not args.compare or not args.repository or os.environ.get("GITHUB_ACTIONS") != "true"
                    or not os.environ.get("GITHUB_WORKFLOW_REF", "").startswith(args.repository + "/" + PREPARE_WORKFLOW + "@")):
                raise ValueError("CI readback requires the prepare-only workflow's exact artifact comparison")
            proof = {"schema": "financial-ci-readback-v1", "receipt_sha256": sha256(data),
                     "operator_evidence_sha256": sha256(evidence_data), "app": app,
                     "image": receipt["image"], "source_commit": receipt["source_commit"],
                     "config_sha256": receipt["config_sha256"], "repository": args.repository,
                     "workflow": PREPARE_WORKFLOW, "run_id": os.environ["GITHUB_RUN_ID"],
                     "workflow_commit": os.environ["GITHUB_SHA"]}
            atomic_receipt(args.write_ci_readback, (json.dumps(proof, indent=2) + "\n").encode())
        print(json.dumps({"receipt_sha256": sha256(data), "app": app, "image": receipt["image"]}))
        return
    if args.operation == "prepare":
        if not args.generation or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,95}", args.generation):
            raise ValueError("prepare requires a unique --generation")
        source_commit = args.source_commit
        if not args.image:
            actual_source = command("git", "rev-parse", "HEAD", capture=True)
            if source_commit and source_commit != actual_source:
                raise ValueError("build source differs from --source-commit")
            if command("git", "status", "--porcelain", capture=True):
                raise ValueError("prepare requires a clean committed build source")
            source_commit = actual_source
        if not source_commit or not re.fullmatch(r"[0-9a-f]{40}", source_commit):
            raise ValueError("existing-image prepare requires its independently established --source-commit")
        if args.store_dir:
            # The workflow copies these files to the release and the locked bucket, then reads them back.
            store_workflow(os.environ, app)
            if args.image:
                raise ValueError("retained-store preparation builds its image; an existing image needs the operator route")
            destination = args.store_dir / "financial-image.json"
            if args.receipt or args.store_dir.exists():
                raise ValueError("retained-store preparation writes a new, empty --store-dir")
        else:
            destination = args.retention_root.expanduser() / app / args.generation / "financial-image.json"
            if args.receipt or destination.exists():
                raise ValueError("prepare derives its durable receipt path; use retain to resume an existing receipt")
            if os.environ.get("GITHUB_ACTIONS") == "true":
                raise ValueError("prepare on an operator persistent filesystem before CI verification")
        command("flyctl", "auth", "docker")
        label = None
        if args.image:
            target = immutable(args.image, app)
        else:
            def build(label):
                # deployment.py verify matches each serving machine's GH_SHA label to the release.
                # Inside GitHub Actions flyctl overwrites GH_SHA with $GITHUB_SHA, so pin that too.
                command("flyctl", "deploy", "--remote-only", "--build-only", "--push", "--config", str(config_path),
                        "--image-label", label, "--label", f"GH_SHA={source_commit}",
                        "--label", f"org.opencontainers.image.revision={source_commit}",
                        env={**os.environ, "GITHUB_SHA": source_commit}, timeout=BUILD_ATTEMPT_TIMEOUT_SECONDS)
            label = build_with_retries(build)
            target = f"registry.fly.io/{app}:{label}"
        digest = inspect_image(target, app)
        floor_tag = f"registry.fly.io/{app}:financial-recovery-floor-c1"
        found = subprocess.run(["docker", "manifest", "inspect", floor_tag], text=True, capture_output=True)
        if found.returncode == 0:
            floor = inspect_image(floor_tag, app)
        elif re.fullmatch(r"(?:no such manifest|manifest unknown)(?:: [^\n]+)?\n?", found.stderr.lower()) and not any(
                word in found.stderr.lower() for word in ["unauthorized", "denied", "forbidden", "timeout", "connection"]):
            command("docker", "tag", digest, floor_tag)
            command("docker", "push", floor_tag)
            floor = inspect_image(floor_tag, app)
            if floor != digest:
                raise ValueError("new retained recovery floor differs from prepared image")
        else:
            raise RuntimeError("cannot establish retained recovery floor: " + found.stderr)
        receipt = {"schema": SCHEMA, "generation": args.generation, "retention": RETENTION,
                   "source_commit": source_commit, "app": app, "image": digest,
                   "recovery_floor": floor, "capability": CAPABILITY, "executable": EXECUTABLE,
                   "config": str(config_path), "config_sha256": sha256(config_path.read_bytes())}
        if args.store_dir:
            data = (json.dumps(receipt, indent=2) + "\n").encode()
            validate_receipt(data, config_path, expected_source=source_commit)
            capability_data = (capability_output(digest) + "\n").encode()
            evidence = store_evidence(data, capability_data, label, os.environ)
            args.store_dir.mkdir(mode=0o700, parents=True)
            atomic_receipt(destination, data)
            atomic_receipt(args.store_dir / "capability.json", capability_data)
            atomic_receipt(args.store_dir / "retained-store.json", (json.dumps(evidence, indent=2) + "\n").encode())
            print(json.dumps({"receipt": str(destination), "receipt_sha256": sha256(data)}))
            return
        destination = retain((json.dumps(receipt, indent=2) + "\n").encode(), config_path, args.retention_root)
        print(json.dumps({"receipt": str(destination), "receipt_sha256": sha256(destination.read_bytes())}))
        return
    data = args.receipt.read_bytes()
    receipt = validate_receipt(data, config_path, args.expected_sha256, args.expected_source,
                               current_config=args.operation != "recover")
    if args.operation == "deploy" and args.store_evidence:
        if args.operator_evidence or args.ci_readback:
            raise ValueError("deploy takes retained-store evidence or operator evidence, not both")
        if not all([args.expected_sha256, args.expected_source, args.capability, args.repository, args.prepare_run_id]):
            raise ValueError("deploy requires expected receipt SHA/source, retained-store evidence, capability output, repository and preparation run")
        evidence = validate_store(data, args.store_evidence.read_bytes(), args.capability.read_bytes())
        validate_store_run(evidence, args.repository, args.prepare_run_id)
    elif args.operation == "deploy":
        if not all([args.expected_sha256, args.expected_source, args.operator_evidence, args.ci_readback, args.repository, args.prepare_run_id]):
            raise ValueError("deploy requires expected receipt SHA/source, operator evidence, successful CI readback and repository")
        evidence_data = args.operator_evidence.read_bytes()
        validate_retention(data, json.loads(evidence_data))
        validate_ci_readback(data, evidence_data, json.loads(args.ci_readback.read_bytes()), args.repository, args.prepare_run_id)
    command("flyctl", "auth", "docker")
    floor = inspect_image(immutable(receipt["recovery_floor"], app), app)
    target = receipt["image"]
    if args.operation == "recover":
        target = immutable(args.image or floor, app)
    elif args.image:
        raise ValueError("deploy uses the already retained prepared image")
    target = inspect_image(immutable(target, app), app)
    if args.candidate_receipt:
        args.candidate_receipt.parent.mkdir(parents=True, exist_ok=True)
        atomic_receipt(args.candidate_receipt, (json.dumps({"digest": target.split("@", 1)[1]}) + "\n").encode())
    deploy = ["flyctl", "deploy", "--config", str(config_path), "--image", target]
    if args.operation == "recover":
        deploy += ["--env", "MANAGED_PROCESSING_DISABLED=true"]
    command(*deploy)


if __name__ == "__main__":
    main()
