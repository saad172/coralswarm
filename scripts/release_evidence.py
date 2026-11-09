#!/usr/bin/env python3
"""Retain release evidence on the GitHub Release and in the locked R2 bucket, and verify it independently."""
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile
import tomllib

spec = importlib.util.spec_from_file_location("financial_deploy", Path(__file__).with_name("deploy_financial_backend.py"))
financial = importlib.util.module_from_spec(spec)
spec.loader.exec_module(financial)

EVIDENCE = ["financial-image.json", "retained-store.json", "capability.json"]
RECORD = "release-images.json"
DIST = "backend-dist.tar.gz"
SIDES = {"production": "fly.toml", "staging": "fly.staging.toml"}
FLOOR_TAG = "financial-recovery-floor-c1"
SCHEMA = "release-images-v2"
TAG = re.compile(r"server-[0-9]{4}\.[0-9]{2}\.[0-9]{2}\.[0-9]+\Z")
PROVISIONING = "docs/runbooks/release.md#one-time-provisioning"


def sha256(data):
    return hashlib.sha256(data).hexdigest()


def command(*args, capture=False, stdin=None):
    result = subprocess.run(args, check=True, text=True, input=stdin, stdout=subprocess.PIPE if capture else None)
    return result.stdout if capture else None


def require_environment(*names):
    missing = [name for name in names if not os.environ.get(name)]
    if missing:
        raise SystemExit(f"missing {', '.join(missing)}; provision the release evidence store first ({PROVISIONING})")


def app_of(config):
    return tomllib.loads(Path(config).read_text())["app"]


class Bucket:
    """R2 over its S3 API with curl's SigV4 signer. Keys under releases/ are write-once (bucket lock)."""

    def __init__(self):
        require_environment("RELEASE_EVIDENCE_BUCKET", "R2_ACCOUNT_ID", "R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY")
        self.name = os.environ["RELEASE_EVIDENCE_BUCKET"]
        self.endpoint = f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com/{self.name}"

    def curl(self, key, output, *extra):
        # Credentials go through stdin, never argv.
        config = f'user = "{os.environ["R2_ACCESS_KEY_ID"]}:{os.environ["R2_SECRET_ACCESS_KEY"]}"\n'
        status = command("curl", "--silent", "--show-error", "--retry", "3", "--max-time", "600",
                         "--aws-sigv4", "aws:amz:auto:s3", "-K", "-", "-o", str(output), "-w", "%{http_code}",
                         *extra, f"{self.endpoint}/{key}", capture=True, stdin=config)
        return int(status)

    def get(self, key):
        with tempfile.TemporaryDirectory() as directory:
            body = Path(directory) / "body"
            status = self.curl(key, body)
            if status == 404:
                return None
            if status != 200:
                raise RuntimeError(f"R2 GET {key} returned HTTP {status}")
            return body.read_bytes()

    def put_once(self, key, path):
        with tempfile.TemporaryDirectory() as directory:
            status = self.curl(key, Path(directory) / "body", "-X", "PUT", "-H", "If-None-Match: *",
                               "--data-binary", f"@{path}")
        # 412: the key exists. 409: the bucket lock refused an overwrite of a locked key.
        if status not in (200, 409, 412):
            raise RuntimeError(f"R2 PUT {key} returned HTTP {status}")
        if self.get(key) != Path(path).read_bytes():
            raise ValueError(f"R2 copy of {key} differs from the bytes this run prepared")


def release_asset(tag, name, repository, directory):
    """The asset's bytes, or None when the release has no asset of that name."""
    names = json.loads(command("gh", "release", "view", tag, "--repo", repository, "--json", "assets", capture=True))["assets"]
    if name not in {asset["name"] for asset in names}:
        return None
    command("gh", "release", "download", tag, "--repo", repository, "--pattern", name, "--dir", str(directory), "--clobber")
    return (Path(directory) / name).read_bytes()


def store(tag, files, repository):
    bucket = Bucket()
    for path in files:
        path = Path(path)
        data = path.read_bytes()
        bucket.put_once(f"releases/{tag}/{path.name}", path)
        with tempfile.TemporaryDirectory() as directory:
            existing = release_asset(tag, path.name, repository, directory)
            if existing is None:
                command("gh", "release", "upload", tag, str(path), "--repo", repository)
            elif existing != data:
                raise ValueError(f"release asset {path.name} already exists with different bytes")
        with tempfile.TemporaryDirectory() as directory:
            if release_asset(tag, path.name, repository, directory) != data:
                raise ValueError(f"release asset {path.name} differs from the bytes this run prepared")
        print(f"retained {path.name} sha256 {sha256(data)} on the release and in r2://{bucket.name}/releases/{tag}/")


def record(tag, sha, build_run_id, prepare_run_id, hotfix, dist, evidence_root, bucket):
    """Deterministic from its inputs, so a re-run after a partial store writes the same bytes."""
    doc = {"schema": SCHEMA, "tag": tag, "sha": sha, "build_run_id": int(build_run_id),
           "prepare_run_id": int(prepare_run_id), "hotfix": hotfix, "backend_dist_sha256": sha256(Path(dist).read_bytes()),
           "evidence_bucket": bucket, "financial": {}}
    for side, config in SIDES.items():
        app = app_of(config)
        data = (Path(evidence_root) / f"{app}.financial-image.json").read_bytes()
        evidence = json.loads((Path(evidence_root) / f"{app}.retained-store.json").read_bytes())
        receipt = json.loads(data)
        if (receipt["source_commit"], evidence["run_id"], evidence["receipt_sha256"]) != (sha, str(prepare_run_id), sha256(data)):
            raise ValueError(f"{side} evidence does not describe this release and run")
        doc["financial"][side] = {"app": app, "receipt_sha256": sha256(data), "image": receipt["image"],
                                  "recovery_floor": receipt["recovery_floor"], "build_tag": evidence["build_tag"],
                                  "assets": [f"{app}.{name}" for name in EVIDENCE]}
    return doc


def registry_digest(reference):
    """The manifest digest the registry serves for a tag, resolved here rather than read from the receipt."""
    descriptor = json.loads(command("docker", "buildx", "imagetools", "inspect", reference,
                                    "--format", "{{json .Manifest}}", capture=True))
    if not re.fullmatch(r"sha256:[0-9a-f]{64}", descriptor.get("digest", "")):
        raise ValueError(f"{reference} did not resolve to an immutable manifest digest")
    return descriptor["digest"]


def verify_side(side, config, tag, sha, record_doc, assets, bucket):
    app = app_of(config)
    files = {}
    for name in EVIDENCE:
        asset = f"{app}.{name}"
        copy = bucket.get(f"releases/{tag}/{asset}")
        if copy is None or (assets / asset).read_bytes() != copy:
            raise ValueError(f"{asset}: the R2 copy differs from the release asset or is missing")
        files[name] = copy
    data = files["financial-image.json"]
    receipt = financial.validate_receipt(data, Path(config), record_doc["financial"][side]["receipt_sha256"], sha)
    evidence = financial.validate_store(data, files["retained-store.json"], files["capability.json"])
    if evidence["run_id"] != str(record_doc["prepare_run_id"]) or evidence["build_tag"] != record_doc["financial"][side]["build_tag"]:
        raise ValueError(f"{side}: release-images.json and the retained evidence name different preparations")
    image, floor = receipt["image"], receipt["recovery_floor"]
    if registry_digest(f"registry.fly.io/{app}:{evidence['build_tag']}") != image.split("@", 1)[1]:
        raise ValueError(f"{side}: the registry resolves the build tag to a different manifest than the receipt")
    if registry_digest(f"registry.fly.io/{app}:{FLOOR_TAG}") != floor.split("@", 1)[1]:
        raise ValueError(f"{side}: the registry resolves the recovery floor tag to a different manifest than the receipt")
    command("docker", "pull", image)
    labels = json.loads(command("docker", "image", "inspect", "--format", "{{json .Config.Labels}}", image, capture=True)) or {}
    if labels.get("GH_SHA") != sha or labels.get("org.opencontainers.image.revision") != sha:
        raise ValueError(f"{side}: image labels GH_SHA={labels.get('GH_SHA')!r} and "
                         f"org.opencontainers.image.revision={labels.get('org.opencontainers.image.revision')!r} "
                         f"must both be the release commit {sha}")
    observed = command("docker", "run", "--rm", "--network", "none", "--entrypoint", financial.EXECUTABLE,
                       image, "--financial-release-capability", capture=True)
    if json.loads(observed) != json.loads(files["capability.json"]):
        raise ValueError(f"{side}: the image's capability output differs from the retained capability output")
    print(f"{side}: {image} built from {sha}; receipt {sha256(data)} identical on the release and in R2")


def verify(tag, sha, repository, run_id):
    require_environment("FLY_REGISTRY_READ_TOKEN")
    bucket = Bucket()
    command("docker", "login", "registry.fly.io", "--username", "x", "--password-stdin",
            stdin=os.environ["FLY_REGISTRY_READ_TOKEN"])
    with tempfile.TemporaryDirectory() as directory:
        assets = Path(directory)
        command("gh", "release", "download", tag, "--repo", repository, "--dir", str(assets))
        record_bytes = (assets / RECORD).read_bytes()
        if bucket.get(f"releases/{tag}/{RECORD}") != record_bytes:
            raise ValueError(f"{RECORD}: the R2 copy differs from the release asset or is missing")
        record_doc = json.loads(record_bytes)
        if (record_doc.get("schema"), record_doc.get("tag"), record_doc.get("sha")) != (SCHEMA, tag, sha):
            raise ValueError(f"{RECORD} does not describe {tag} at {sha}")
        if str(record_doc.get("prepare_run_id")) != str(run_id):
            raise ValueError(f"{RECORD} names preparation run {record_doc.get('prepare_run_id')}, not this run {run_id}")
        if sha256((assets / DIST).read_bytes()) != record_doc["backend_dist_sha256"]:
            raise ValueError(f"{DIST} differs from the checksum {RECORD} records")
        for side, config in SIDES.items():
            verify_side(side, config, tag, sha, record_doc, assets, bucket)


def select(tag, sha, side, directory, repository):
    """Download one side's evidence from the release, require it to equal the locked R2 copy, and name what
    deploy_financial_backend.py must admit. A replaced release asset cannot outvote the write-once copy."""
    bucket = Bucket()
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    record_bytes = release_asset(tag, RECORD, repository, directory)
    if record_bytes is None:
        raise SystemExit(f"{tag} has no {RECORD}; only releases cut by the Release workflow (release.yml) can deploy")
    if bucket.get(f"releases/{tag}/{RECORD}") != record_bytes:
        raise ValueError(f"{RECORD} on the release differs from its locked R2 copy")
    doc = json.loads(record_bytes)
    if (doc.get("schema"), doc.get("tag"), doc.get("sha")) != (SCHEMA, tag, sha):
        raise ValueError(f"{RECORD} does not describe {tag} at {sha}")
    chosen = doc["financial"][side]
    for name in chosen["assets"]:
        data = release_asset(tag, name, repository, directory)
        if data is None:
            raise ValueError(f"{tag} lacks the evidence asset {name}")
        if bucket.get(f"releases/{tag}/{name}") != data:
            raise ValueError(f"{name} on the release differs from its locked R2 copy")
    app = chosen["app"]
    if sha256((directory / f"{app}.financial-image.json").read_bytes()) != chosen["receipt_sha256"]:
        raise ValueError(f"{app} receipt on the release differs from the SHA256 {RECORD} records")
    selected = {"PREPARE_RUN_ID": str(doc["prepare_run_id"]), "RECEIPT_SHA256": chosen["receipt_sha256"],
                "BUILD_RUN_ID": str(doc["build_run_id"]),
                "RECEIPT": str(directory / f"{app}.financial-image.json"),
                "STORE_EVIDENCE": str(directory / f"{app}.retained-store.json"),
                "CAPABILITY": str(directory / f"{app}.capability.json")}
    if os.environ.get("GITHUB_ENV"):
        with open(os.environ["GITHUB_ENV"], "a") as environment:
            environment.writelines(f"{name}={value}\n" for name, value in selected.items())
    return selected


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="action", required=True)
    storing = sub.add_parser("store", help="write each file to R2 once and to the release, then read both back")
    storing.add_argument("--tag", required=True)
    storing.add_argument("files", nargs="+", type=Path)
    recording = sub.add_parser("record", help="write release-images.json from the retained evidence")
    for name in ["--tag", "--sha", "--build-run-id", "--prepare-run-id", "--dist", "--evidence-root", "--out"]:
        recording.add_argument(name, required=True)
    recording.add_argument("--hotfix", choices=["true", "false"], required=True)
    verifying = sub.add_parser("verify", help="independently re-derive every identity the release records")
    verifying.add_argument("--tag", required=True)
    verifying.add_argument("--sha", required=True)
    verifying.add_argument("--run-id", default=os.environ.get("GITHUB_RUN_ID"), help="the Release run being verified")
    selecting = sub.add_parser("select", help="download one side's evidence from the release for a deploy")
    selecting.add_argument("--tag", required=True)
    selecting.add_argument("--sha", required=True)
    selecting.add_argument("--side", choices=SIDES, required=True)
    selecting.add_argument("--dir", required=True)
    parser.add_argument("--repository", default=os.environ.get("GITHUB_REPOSITORY"))
    args = parser.parse_args()
    if not TAG.fullmatch(args.tag):
        raise SystemExit(f"{args.tag!r} is not a server-YYYY.MM.DD.N tag")
    if args.action == "store":
        store(args.tag, args.files, args.repository)
    elif args.action == "record":
        require_environment("RELEASE_EVIDENCE_BUCKET")
        doc = record(args.tag, args.sha, args.build_run_id, args.prepare_run_id, args.hotfix == "true",
                     args.dist, args.evidence_root, os.environ["RELEASE_EVIDENCE_BUCKET"])
        Path(args.out).write_text(json.dumps(doc, indent=2) + "\n")
    elif args.action == "select":
        print(json.dumps(select(args.tag, args.sha, args.side, args.dir, args.repository), indent=2))
    else:
        verify(args.tag, args.sha, args.repository, args.run_id)


if __name__ == "__main__":
    main()
