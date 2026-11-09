#!/usr/bin/env python3
"""Exercise the laptop caller and real receipt verifier with offline command drivers."""
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest

spec = importlib.util.spec_from_file_location("financial_deploy", Path(__file__).with_name("deploy_financial_backend.py"))
deploy = importlib.util.module_from_spec(spec)
spec.loader.exec_module(deploy)


class LocalFinancialTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        (self.root / "scripts").mkdir()
        (self.root / "web").mkdir()
        (self.root / "gitdir").mkdir()
        source = Path(__file__).resolve().parent
        for name in ["deploy_local.sh", "deploy_financial_backend.py"]:
            shutil.copy(source / name, self.root / "scripts" / name)
        shutil.copy(source.parent / "fly.manual.toml", self.root)
        self.bundle = self.root / "verified bundle"
        self.bundle.mkdir()
        self.source = "c" * 40
        self.image = "registry.fly.io/coralswarm-backend@sha256:" + "a" * 64
        receipt = {"schema": deploy.SCHEMA, "generation": "manual-fixture", "retention": deploy.RETENTION,
                   "source_commit": self.source, "app": "coralswarm-backend", "image": self.image,
                   "recovery_floor": self.image, "capability": deploy.CAPABILITY,
                   "executable": deploy.EXECUTABLE,
                   "config_sha256": deploy.sha256((self.root / "fly.manual.toml").read_bytes())}
        data = (json.dumps(receipt) + "\n").encode()
        retained = self.root / "retained/coralswarm-backend/manual-fixture/financial-image.json"
        evidence = (json.dumps(deploy.retention_evidence(data, retained)) + "\n").encode()
        proof = {"schema": "financial-ci-readback-v1", "receipt_sha256": deploy.sha256(data),
                 "operator_evidence_sha256": deploy.sha256(evidence), "app": receipt["app"],
                 "image": self.image, "source_commit": self.source, "config_sha256": receipt["config_sha256"],
                 "repository": "fixture/repo", "workflow": deploy.PREPARE_WORKFLOW,
                 "run_id": "123", "workflow_commit": self.source}
        (self.bundle / "financial-image.json").write_bytes(data)
        (self.bundle / "operator-retention.json").write_bytes(evidence)
        (self.bundle / "ci-readback.json").write_text(json.dumps(proof))
        self.log = self.root / "commands.jsonl"
        self.bin = self.root / "bin"
        self.bin.mkdir()
        driver = self.bin / "driver"
        driver.write_text("#!" + sys.executable + "\n" + '''import json, os, pathlib, shutil, sys
name = pathlib.Path(sys.argv[0]).name
args = sys.argv[1:]
root = pathlib.Path(os.environ["FIXTURE_ROOT"])
with (root / "commands.jsonl").open("a") as log:
    log.write(json.dumps([name, *args]) + "\\n")
if name == "git":
    if args == ["rev-parse", "--abbrev-ref", "HEAD"]: print("main")
    elif args == ["rev-parse", "--absolute-git-dir"]: print(root / "gitdir")
    elif args[:1] == ["rev-parse"]: print("c" * (12 if "--short" in args else 40))
    elif args[:1] == ["status"] and os.environ.get("FIXTURE_DIRTY"): print(" M changed")
elif name == "gh":
    if args[:1] == ["api"]:
        print(json.dumps({"status":"completed", "conclusion":os.environ.get("FIXTURE_CONCLUSION", "success"), "path":".github/workflows/financial-recovery-prepare.yml", "head_sha":"c"*40}))
    elif args[:2] == ["run", "download"]:
        destination = pathlib.Path(args[args.index("--dir")+1])
        for file in (root / "verified bundle").iterdir(): shutil.copy(file, destination)
elif name == "flyctl" and args == ["auth", "whoami"]: print("fixture")
elif name == "docker":
    if args[:2] == ["image", "inspect"]: print(json.dumps([args[-1]]))
    elif args[:1] == ["run"]: print(json.dumps({"financial_release":"account-entry-c1", "managed_recovery":"legacy-and-v1", "admission_disable":"MANAGED_PROCESSING_DISABLED"}))
''')
        driver.chmod(0o755)
        for name in ["git", "gh", "flyctl", "docker", "npm"]:
            (self.bin / name).symlink_to(driver)
        self.env = {**os.environ, "PATH": str(self.bin) + os.pathsep + os.environ["PATH"],
                    "FIXTURE_ROOT": str(self.root), "PYTHONDONTWRITEBYTECODE": "1",
                    "FINANCIAL_MANUAL_BUNDLE": str(self.bundle),
                    "FINANCIAL_MANUAL_RECEIPT_SHA256": hashlib.sha256(data).hexdigest(),
                    "FINANCIAL_MANUAL_PREPARE_RUN_ID": "123", "FINANCIAL_MANUAL_REPOSITORY": "fixture/repo",
                    "NEXT_PUBLIC_CLERK_PUBLISHABLE_KEY": "pk_test_REPLACE_WITH_YOUR_KEY"}
        (self.root / "gitdir/ci-local-receipt").write_text(f"sha={self.source}\npassed=yes\nwhen=fixture\n")

    def run_caller(self, *args):
        return subprocess.run(["bash", str(self.root / "scripts/deploy_local.sh"), *args],
                              env=self.env, text=True, capture_output=True)

    def calls(self):
        return [json.loads(line) for line in self.log.read_text().splitlines()] if self.log.exists() else []

    def assert_refused_before_provider(self, *args):
        result = self.run_caller("--backend-only", *args)
        self.assertNotEqual(result.returncode, 0, result.stdout)
        self.assertFalse(any(c[0] in ["flyctl", "docker", "npm"] for c in self.calls()), self.calls())
        return result

    def test_missing_bundle_has_preparation_route_even_with_local_ci_override(self):
        for key in ["FINANCIAL_MANUAL_BUNDLE", "FINANCIAL_MANUAL_RECEIPT_SHA256",
                    "FINANCIAL_MANUAL_PREPARE_RUN_ID", "FINANCIAL_MANUAL_REPOSITORY"]:
            original = self.env.pop(key)
            for value in [None, ""]:
                with self.subTest(key=key, value=value):
                    if value is not None:
                        self.env[key] = value
                    result = self.assert_refused_before_provider("--dry-run", "--skip-ci-check")
                    self.assertIn("Prepare and verify fly.manual.toml for HEAD", result.stderr)
            self.env[key] = original

    def test_bundle_mismatches_refuse_before_authentication(self):
        for key, bad in [("FINANCIAL_MANUAL_RECEIPT_SHA256", "0" * 64),
                         ("FINANCIAL_MANUAL_PREPARE_RUN_ID", "124"),
                         ("FINANCIAL_MANUAL_REPOSITORY", "other/repo")]:
            with self.subTest(key=key):
                original = self.env[key]
                self.env[key] = bad
                self.assert_refused_before_provider("--dry-run")
                self.env[key] = original

    def test_wrong_source_and_config_refuse_before_authentication(self):
        file = self.bundle / "financial-image.json"
        original = file.read_bytes()
        for key, bad in [("source_commit", "d" * 40), ("config_sha256", "0" * 64)]:
            with self.subTest(key=key):
                receipt = json.loads(original)
                receipt[key] = bad
                data = json.dumps(receipt).encode()
                file.write_bytes(data)
                self.env["FINANCIAL_MANUAL_RECEIPT_SHA256"] = hashlib.sha256(data).hexdigest()
                self.assert_refused_before_provider("--dry-run")

    def test_unsuccessful_run_refuses_before_authentication(self):
        self.env["FIXTURE_CONCLUSION"] = "failure"
        self.assert_refused_before_provider("--dry-run")

    def test_backend_allow_dirty_is_rejected(self):
        self.env["FIXTURE_DIRTY"] = "1"
        result = self.assert_refused_before_provider("--allow-dirty", "--dry-run")
        self.assertIn("remove --allow-dirty", result.stderr)

    def test_valid_dry_run_verifies_actual_bundle_and_prints_deploy(self):
        result = self.run_caller("--backend-only", "--dry-run")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("deploy --config fly.manual.toml", result.stdout)
        self.assertIn("--expected-source " + self.source, result.stdout)
        self.assertTrue(any(c[:3] == ["gh", "run", "download"] for c in self.calls()))
        self.assertFalse(any(c[0] == "docker" or c[:2] == ["flyctl", "deploy"] for c in self.calls()))

    def test_local_ci_receipt_still_gates_deploy(self):
        (self.root / "gitdir/ci-local-receipt").write_text("sha=" + "d" * 40 + "\npassed=yes\n")
        result = self.run_caller("--backend-only")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("local CI passed for", result.stderr)
        self.assertFalse(any(c[0] == "docker" or c[:2] == ["flyctl", "deploy"] for c in self.calls()))

    def test_backend_deploy_consumes_selected_image_before_web(self):
        result = self.run_caller()
        self.assertEqual(result.returncode, 0, result.stderr)
        calls = self.calls()
        backend = next(i for i, c in enumerate(calls) if c[:2] == ["flyctl", "deploy"])
        web = next(i for i, c in enumerate(calls) if c[:2] == ["npm", "ci"])
        self.assertLess(backend, web)
        self.assertEqual(calls[backend][-2:], ["--image", self.image])
        self.assertFalse(any("--build-only" in c for c in calls))

    def test_web_only_retains_allow_dirty_without_backend_bundle(self):
        self.env["FIXTURE_DIRTY"] = "1"
        del self.env["FINANCIAL_MANUAL_BUNDLE"]
        result = self.run_caller("--web-only", "--allow-dirty", "--dry-run")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertFalse(any(c[0] in ["gh", "docker"] for c in self.calls()))


if __name__ == "__main__":
    unittest.main()
