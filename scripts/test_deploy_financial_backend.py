#!/usr/bin/env python3
"""Finite deploy-command contract checks; actual image execution is separately required."""
import importlib.util
import json
import os
import stat
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location("financial_deploy", Path(__file__).with_name("deploy_financial_backend.py"))
deploy = importlib.util.module_from_spec(spec)
spec.loader.exec_module(deploy)


class DeploymentContract(unittest.TestCase):
    STORE_RUN = "777"
    ATTEMPT = "2"
    WORKFLOW_COMMIT = "e" * 40
    RELEASE_WORKFLOW = ".github/workflows/release.yml"

    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.root = Path(self.directory.name).resolve()
        self.environment = patch.dict(os.environ, {"GITHUB_ACTIONS": "false"})
        self.environment.start()
        self.config = self.root / "fly.toml"
        self.config.write_text("app='fixture-backend'\n[processes]\napp='/app/coralswarm-funded-backend'\n[deploy]\nrelease_command='/app/release.sh'\n")
        self.operator_root = self.root / "operator"
        self.receipt = self.operator_root / "fixture-backend" / "fixture-release" / "financial-image.json"
        self.source = "c" * 40
        self.digest = "registry.fly.io/fixture-backend@sha256:" + "a" * 64
        self.floor = "registry.fly.io/fixture-backend@sha256:" + "b" * 64
        self.calls = []
        self.environments = []
        self.capability = deploy.CAPABILITY
        self.manifest_error = "no such manifest"
        self.store_dir = self.root / "store" / "coralswarm-backend"
        self.store_run = {"path": self.RELEASE_WORKFLOW, "head_sha": self.WORKFLOW_COMMIT, "event": "workflow_dispatch", "head_branch": "main"}
        self.store_jobs = [{"name": "Prepare backend image", "conclusion": "success"},
                           {"name": "Verify prepared release independently", "conclusion": "success"}]

    def tearDown(self):
        self.environment.stop()
        self.directory.cleanup()

    def command(self, *args, capture=False, env=None, timeout=None):
        self.calls.append(args)
        self.environments.append((args, env))
        if args[:2] == ("git", "rev-parse"):
            return self.source
        if args[:2] == ("gh", "api") and "/jobs" in args[2]:
            return json.dumps({"jobs": self.store_jobs})
        if args[:2] == ("gh", "api") and args[2].endswith(f"/runs/{self.STORE_RUN}/attempts/{self.ATTEMPT}"):
            return json.dumps(self.store_run)
        if args[:2] == ("gh", "api"):
            return json.dumps({"status": "completed", "conclusion": "success", "path": deploy.PREPARE_WORKFLOW, "head_sha": self.source})
        if args[:3] == ("gh", "run", "download"):
            for name in ["financial-image.json", "operator-retention.json", "ci-readback.json"]:
                (Path(args[-1]) / name).write_bytes(self.receipt.with_name(name).read_bytes())
            return None
        if args[:3] == ("docker", "image", "inspect"):
            return json.dumps([args[-1] if "@sha256:" in args[-1] else self.floor if ":financial-recovery-floor-c1" in args[-1] else self.digest])
        if args[:2] == ("docker", "run"):
            return json.dumps(self.capability)
        return "" if capture else None

    def manifest(self, args, **kwargs):
        self.calls.append(tuple(args))
        return subprocess.CompletedProcess(args, 1, "", self.manifest_error)

    def run_main(self, operation, *extra):
        arguments = ["deploy", operation, "--config", str(self.config)]
        if operation == "prepare":
            arguments += ["--generation", "fixture-release", "--retention-root", str(self.operator_root), "--source-commit", self.source]
        else:
            arguments += ["--receipt", str(self.receipt)]
        with patch.object(sys, "argv", [*arguments, *extra]), patch.object(deploy, "command", self.command), patch.object(deploy.subprocess, "run", self.manifest):
            deploy.main()

    def retained(self):
        data = json.dumps({"schema": deploy.SCHEMA, "generation": "fixture-release", "retention": deploy.RETENTION, "source_commit": self.source, "app": "fixture-backend", "image": self.digest, "recovery_floor": self.floor, "capability": deploy.CAPABILITY, "executable": deploy.EXECUTABLE, "config": str(self.config), "config_sha256": deploy.sha256(self.config.read_bytes())}).encode()
        deploy.retain(data, self.config, self.operator_root)

    def verification_arguments(self):
        return ["--operator-evidence", str(self.receipt.with_name("operator-retention.json")),
                "--expected-sha256", deploy.sha256(self.receipt.read_bytes()),
                "--expected-source", self.source]

    def ci_proof(self):
        self.retained()
        original = self.root / "original"
        original.mkdir()
        for name in ["financial-image.json", "operator-retention.json"]:
            (original / name).write_bytes(self.receipt.with_name(name).read_bytes())
        proof = self.receipt.with_name("ci-readback.json")
        with patch.dict(os.environ, {"GITHUB_ACTIONS": "true", "GITHUB_REPOSITORY": "fixture/repo",
                                    "GITHUB_WORKFLOW_REF": "fixture/repo/" + deploy.PREPARE_WORKFLOW + "@refs/heads/main",
                                    "GITHUB_RUN_ID": "123", "GITHUB_SHA": self.source}):
            self.run_main("verify", *self.verification_arguments(), "--compare", str(original / "financial-image.json"),
                          "--write-ci-readback", str(proof))
        return [*self.verification_arguments(), "--ci-readback", str(proof), "--repository", "fixture/repo", "--prepare-run-id", "123"]

    def release_run_environment(self, workflow=None):
        return {"GITHUB_ACTIONS": "true", "GITHUB_REPOSITORY": "fixture/repo", "GITHUB_RUN_ID": self.STORE_RUN,
                "GITHUB_WORKFLOW_REF": "fixture/repo/" + (workflow or self.RELEASE_WORKFLOW) + "@refs/heads/main",
                "GITHUB_SHA": self.WORKFLOW_COMMIT, "GITHUB_RUN_ATTEMPT": "2"}

    def store_prepared(self):
        def existing(args, **kwargs):
            self.calls.append(tuple(args))
            return subprocess.CompletedProcess(args, 0, "{}", "")
        with patch.dict(os.environ, self.release_run_environment()), patch.object(self, "manifest", existing):
            self.run_main("prepare", "--store-dir", str(self.store_dir))
        self.calls.clear()
        return [*self.store_arguments(), "--expected-sha256", deploy.sha256(self.store_dir.joinpath("financial-image.json").read_bytes()),
                "--expected-source", self.source, "--repository", "fixture/repo", "--prepare-run-id", self.STORE_RUN]

    def store_arguments(self):
        return ["--store-evidence", str(self.store_dir / "retained-store.json"), "--capability", str(self.store_dir / "capability.json")]

    def run_store_deploy(self, *arguments):
        receipt = self.store_dir / "financial-image.json"
        with patch.object(sys, "argv", ["deploy", "deploy", "--config", str(self.config), "--receipt", str(receipt), *arguments]), \
                patch.object(deploy, "command", self.command), patch.object(deploy.subprocess, "run", self.manifest):
            deploy.main()

    def test_ci_store_prepare_binds_receipt_capability_and_run_without_operator_disk(self):
        self.store_prepared()
        data = self.store_dir.joinpath("financial-image.json").read_bytes()
        capability = self.store_dir.joinpath("capability.json").read_bytes()
        evidence = deploy.validate_store(data, self.store_dir.joinpath("retained-store.json").read_bytes(), capability)
        self.assertEqual(json.loads(capability), deploy.CAPABILITY)
        self.assertEqual((evidence["workflow"], evidence["run_id"], evidence["workflow_commit"], evidence["repository"]),
                         (self.RELEASE_WORKFLOW, self.STORE_RUN, self.WORKFLOW_COMMIT, "fixture/repo"))
        self.assertEqual((evidence["image"], evidence["source_commit"]), (self.digest, self.source))
        self.assertRegex(evidence["build_tag"], r"^financial-c1-[0-9a-f]{32}$")
        self.assertFalse(self.operator_root.exists())

    def test_store_prepare_refuses_outside_the_release_or_staging_workflow_before_building(self):
        for environment in [{"GITHUB_ACTIONS": "false"}, self.release_run_environment(".github/workflows/pr.yml"),
                            self.release_run_environment(deploy.PREPARE_WORKFLOW)]:
            self.calls.clear()
            with patch.dict(os.environ, environment), self.assertRaisesRegex(ValueError, "release or staging deploy workflow"):
                self.run_main("prepare", "--store-dir", str(self.store_dir))
            self.assertFalse(any(c[0] in ("flyctl", "docker") for c in self.calls))
        self.assertFalse(self.store_dir.exists())

    def test_store_prepare_never_reuses_a_store_directory(self):
        self.store_dir.mkdir(parents=True)
        with patch.dict(os.environ, self.release_run_environment()), self.assertRaisesRegex(ValueError, "new, empty"):
            self.run_main("prepare", "--store-dir", str(self.store_dir))
        self.assertFalse(any(c[0] in ("flyctl", "docker") for c in self.calls))

    def test_store_deploy_requires_the_independent_verifier_before_authentication(self):
        arguments = self.store_prepared()
        self.run_store_deploy(*arguments)
        self.assertEqual([c for c in self.calls if c[:2] == ("flyctl", "deploy")],
                         [("flyctl", "deploy", "--config", str(self.config), "--image", self.digest)])
        self.assertLess(max(i for i, c in enumerate(self.calls) if c[:2] == ("gh", "api")),
                        next(i for i, c in enumerate(self.calls) if c[:2] == ("flyctl", "auth")))
        self.assertIn(("gh", "api", f"repos/fixture/repo/actions/runs/{self.STORE_RUN}/attempts/{self.ATTEMPT}/jobs?per_page=100"),
                      self.calls, "the gate is read from the preparing attempt, not the run's latest attempt")

    def test_staging_deploy_workflow_can_neither_prepare_nor_admit_another_app(self):
        with patch.dict(os.environ, self.release_run_environment(".github/workflows/deploy.yml")), \
                self.assertRaisesRegex(ValueError, "may prepare only"):
            self.run_main("prepare", "--store-dir", str(self.store_dir))
        arguments = self.store_prepared()
        evidence_path = self.store_dir / "retained-store.json"
        evidence = json.loads(evidence_path.read_bytes())
        evidence["workflow"] = ".github/workflows/deploy.yml"
        evidence_path.write_text(json.dumps(evidence))
        self.store_run["path"] = ".github/workflows/deploy.yml"
        self.store_jobs = [{"name": "Prepare backend image (staging)", "conclusion": "success"}]
        with self.assertRaisesRegex(ValueError, "cannot admit an image for fixture-backend"):
            self.run_store_deploy(*arguments)
        self.assertFalse(any(c[0] in ("flyctl", "docker") for c in self.calls))

    def test_store_prepare_refuses_an_existing_image(self):
        with patch.dict(os.environ, self.release_run_environment()), self.assertRaisesRegex(ValueError, "builds its image"):
            self.run_main("prepare", "--store-dir", str(self.store_dir), "--image", self.digest)
        self.assertFalse(any(c[0] in ("flyctl", "docker") for c in self.calls))

    def test_store_deploy_refuses_an_unverified_or_mismatched_preparation_run(self):
        arguments = self.store_prepared()
        good_run, good_jobs = dict(self.store_run), list(self.store_jobs)
        cases = [("jobs", [{"name": "Prepare backend image", "conclusion": "success"}]),
                 ("jobs", [{"name": "Verify prepared release independently", "conclusion": "failure"}]),
                 ("run", dict(good_run, head_sha="d" * 40)),
                 ("run", dict(good_run, path=".github/workflows/deploy.yml")),
                 ("run", dict(good_run, head_branch="feature")),
                 ("run", dict(good_run, event="push"))]
        for kind, value in cases:
            with self.subTest(kind=kind, value=value):
                self.store_run, self.store_jobs = (value, good_jobs) if kind == "run" else (good_run, value)
                self.calls.clear()
                with self.assertRaises(ValueError):
                    self.run_store_deploy(*arguments)
                self.assertFalse(any(c[0] in ("flyctl", "docker") for c in self.calls))
        self.store_run, self.store_jobs = good_run, good_jobs
        with self.assertRaisesRegex(ValueError, "different repository or preparation run"):
            self.run_store_deploy(*arguments[:-1], "778")

    def test_store_deploy_refuses_tampered_evidence_capability_or_mixed_evidence(self):
        # A run-ID substring in a bound field must survive evidence tampering.
        self.source = self.STORE_RUN + "c" * (40 - len(self.STORE_RUN))
        arguments = self.store_prepared()
        evidence_path = self.store_dir / "retained-store.json"
        original = evidence_path.read_bytes()
        evidence = json.loads(original)
        evidence["run_id"] = "778"
        evidence_path.write_text(json.dumps(evidence))
        # Keep receipt/capability validation valid so the run-binding gate is exercised.
        deploy.validate_store(self.store_dir.joinpath("financial-image.json").read_bytes(),
                              evidence_path.read_bytes(), self.store_dir.joinpath("capability.json").read_bytes())
        with self.assertRaisesRegex(ValueError, "different repository or preparation run"):
            self.run_store_deploy(*arguments)
        self.assertEqual(self.calls, [], "a mismatched run must be refused before authentication or deployment")
        evidence = json.loads(original)
        evidence["image"] = self.floor
        evidence_path.write_text(json.dumps(evidence))
        with self.assertRaisesRegex(ValueError, "does not bind"):
            self.run_store_deploy(*arguments)
        evidence_path.write_bytes(original)
        capability = self.store_dir / "capability.json"
        capability.write_text(json.dumps({"financial_release": "old"}) + "\n")
        with self.assertRaisesRegex(ValueError, "does not bind"):
            self.run_store_deploy(*arguments)
        evidence = json.loads(original)
        evidence["capability_sha256"] = deploy.sha256(capability.read_bytes())
        evidence_path.write_text(json.dumps(evidence))
        with self.assertRaisesRegex(ValueError, "capability output differs"):
            self.run_store_deploy(*arguments)
        evidence_path.write_bytes(original)
        with self.assertRaisesRegex(ValueError, "not both"):
            self.run_store_deploy(*arguments, "--operator-evidence", str(evidence_path))
        self.assertFalse(any(c[0] in ("flyctl", "docker") for c in self.calls))

    def test_durable_retention_is_private_idempotent_and_preserves_first_bytes(self):
        self.retained()
        data = self.receipt.read_bytes()
        for directory in [self.operator_root, self.receipt.parent.parent, self.receipt.parent]:
            self.assertEqual(stat.S_IMODE(directory.stat().st_mode), 0o700)
        self.assertEqual(stat.S_IMODE(self.receipt.stat().st_mode), 0o600)
        self.assertEqual(deploy.retain(data, self.config, self.operator_root), self.receipt)
        changed = json.loads(data)
        changed["image"] = self.floor
        with self.assertRaisesRegex(ValueError, "first retained receipt"):
            deploy.retain(json.dumps(changed).encode(), self.config, self.operator_root)
        self.assertEqual(self.receipt.read_bytes(), data)

    def test_file_and_parent_fsync_precede_durable_readback(self):
        actual = deploy.os.fsync
        synchronized = []
        def fsync(descriptor):
            synchronized.append(stat.S_IFMT(os.fstat(descriptor).st_mode))
            actual(descriptor)
        with patch.object(deploy.os, "fsync", fsync):
            self.retained()
        self.assertIn(stat.S_IFREG, synchronized)
        self.assertEqual(synchronized[-1], stat.S_IFDIR)
        self.assertEqual(json.loads(self.receipt.with_name("operator-retention.json").read_bytes())["receipt_sha256"], deploy.sha256(self.receipt.read_bytes()))

    def test_failed_publication_keeps_first_receipt_and_leaves_no_partial_file(self):
        self.receipt.parent.mkdir(parents=True)
        self.receipt.write_bytes(b"first")
        with self.assertRaises(FileExistsError):
            deploy.atomic_receipt(self.receipt, b"replacement")
        self.assertEqual(self.receipt.read_bytes(), b"first")
        self.assertEqual(list(self.receipt.parent.iterdir()), [self.receipt])

    def test_file_fsync_failure_never_publishes_a_receipt(self):
        self.receipt.parent.mkdir(parents=True)
        with patch.object(deploy.os, "fsync", side_effect=OSError("disk refused fsync")):
            with self.assertRaisesRegex(OSError, "disk refused"):
                deploy.atomic_receipt(self.receipt, b"uncommitted")
        self.assertFalse(self.receipt.exists())
        self.assertEqual(list(self.receipt.parent.iterdir()), [])

    def test_interrupted_sidecar_publication_can_resume_without_replacing_receipt(self):
        self.retained()
        data = self.receipt.read_bytes()
        self.receipt.with_name("operator-retention.json").unlink()
        deploy.retain(data, self.config, self.operator_root)
        self.assertEqual(self.receipt.read_bytes(), data)
        deploy.validate_retention(data, json.loads(self.receipt.with_name("operator-retention.json").read_bytes()))

    def test_ci_runner_cannot_claim_operator_retention(self):
        self.retained()
        with patch.dict(os.environ, {"GITHUB_ACTIONS": "true"}), self.assertRaisesRegex(ValueError, "CI runner"):
            deploy.retain(self.receipt.read_bytes(), self.config, self.operator_root)

    def test_world_readable_or_symlink_operator_destination_refused(self):
        self.retained()
        self.operator_root.chmod(0o755)
        with self.assertRaisesRegex(ValueError, "0700"):
            deploy.retain(self.receipt.read_bytes(), self.config, self.operator_root)
        self.operator_root.chmod(0o700)
        link = self.root / "redirect"
        link.symlink_to(self.operator_root, target_is_directory=True)
        with self.assertRaisesRegex(ValueError, "symlinks"):
            deploy.retain(self.receipt.read_bytes(), self.config, link)

    def test_actual_offline_cli_verifies_retained_bytes_without_credentials(self):
        self.retained()
        result = subprocess.run([sys.executable, str(Path(deploy.__file__)), "verify", "--config", str(self.config),
                                 "--receipt", str(self.receipt), *self.verification_arguments()], text=True, capture_output=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(json.loads(result.stdout)["receipt_sha256"], deploy.sha256(self.receipt.read_bytes()))

    def test_actual_offline_retain_cli_repairs_missing_sidecar_without_external_access(self):
        self.retained()
        data = self.receipt.read_bytes()
        self.receipt.with_name("operator-retention.json").unlink()
        result = subprocess.run([sys.executable, str(Path(deploy.__file__)), "retain", "--config", str(self.config),
                                 "--receipt", str(self.receipt), "--retention-root", str(self.operator_root)], text=True, capture_output=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), str(self.receipt))
        self.assertEqual(self.receipt.read_bytes(), data)
        deploy.validate_retention(data, json.loads(self.receipt.with_name("operator-retention.json").read_bytes()))

    def test_build_cannot_claim_a_different_source_commit(self):
        with self.assertRaisesRegex(ValueError, "build source differs"):
            self.run_main("prepare", "--source-commit", "d" * 40)
        self.assertEqual(self.calls, [("git", "rev-parse", "HEAD")])

    def test_tampered_bytes_config_source_or_evidence_refused_without_external_calls(self):
        self.retained()
        arguments = self.verification_arguments()
        with self.assertRaisesRegex(ValueError, "SHA256"):
            self.run_main("verify", *arguments, "--expected-sha256", "0" * 64)
        with self.assertRaisesRegex(ValueError, "source"):
            self.run_main("verify", *arguments, "--expected-source", "d" * 40)
        self.config.write_text(self.config.read_text() + "\n# changed config\n")
        with self.assertRaisesRegex(ValueError, "config differs"):
            self.run_main("verify", *arguments)
        self.assertEqual(self.calls, [])

    def test_ci_roundtrip_refuses_a_changed_operator_sidecar(self):
        arguments = self.ci_proof()
        original = self.root / "original" / "financial-image.json"
        original.with_name("operator-retention.json").write_bytes(b"altered")
        with self.assertRaisesRegex(ValueError, "original bytes"):
            self.run_main("verify", *self.verification_arguments(), "--compare", str(original))

    def test_deploy_requires_successful_exact_prepare_run_before_migration(self):
        arguments = self.ci_proof()
        self.run_main("deploy", *arguments)
        self.assertEqual([c for c in self.calls if c[:2] == ("flyctl", "deploy")],
                         [("flyctl", "deploy", "--config", str(self.config), "--image", self.digest)])
        self.assertLess(next(i for i, c in enumerate(self.calls) if c[:2] == ("gh", "api")),
                        next(i for i, c in enumerate(self.calls) if c[:2] == ("flyctl", "auth")))

    def test_candidate_digest_is_retained_before_a_failed_single_rollout(self):
        arguments = self.ci_proof()
        candidate = self.root / "release-backend" / "candidate.json"
        normal_command = self.command
        def failed_rollout(*args, capture=False):
            if args[:2] == ("flyctl", "deploy"):
                self.calls.append(args)
                self.assertEqual(json.loads(candidate.read_bytes()), {"digest": self.digest.split("@", 1)[1]})
                raise subprocess.CalledProcessError(1, args)
            return normal_command(*args, capture=capture)
        with patch.object(self, "command", failed_rollout), self.assertRaises(subprocess.CalledProcessError):
            self.run_main("deploy", *arguments, "--candidate-receipt", str(candidate))
        self.assertEqual(len([c for c in self.calls if c[:2] == ("flyctl", "deploy")]), 1)
        self.assertEqual(json.loads(candidate.read_bytes())["digest"], self.digest.split("@", 1)[1])

    def test_rejected_release_does_not_publish_a_candidate(self):
        arguments = self.ci_proof()
        candidate = self.root / "candidate.json"
        with self.assertRaisesRegex(ValueError, "SHA256"):
            self.run_main("deploy", *arguments, "--expected-sha256", "0" * 64,
                          "--candidate-receipt", str(candidate))
        self.assertFalse(candidate.exists())
        self.assertFalse(any(c[:2] == ("flyctl", "deploy") for c in self.calls))

    def test_existing_candidate_is_never_replaced_or_deployed_again(self):
        arguments = self.ci_proof()
        candidate = self.root / "candidate.json"
        candidate.write_bytes(b"original receipt")
        with self.assertRaises(FileExistsError):
            self.run_main("deploy", *arguments, "--candidate-receipt", str(candidate))
        self.assertEqual(candidate.read_bytes(), b"original receipt")
        self.assertFalse(any(c[:2] == ("flyctl", "deploy") for c in self.calls))

    def test_missing_mismatched_or_unsuccessful_ci_evidence_refuses_before_authentication(self):
        arguments = self.ci_proof()
        with self.assertRaisesRegex(ValueError, "deploy requires"):
            self.run_main("deploy")
        with self.assertRaisesRegex(ValueError, "CI readback"):
            self.run_main("deploy", *arguments, "--prepare-run-id", "124")
        with patch.object(deploy, "validate_ci_readback", side_effect=ValueError("prepare run not successful")):
            with self.assertRaisesRegex(ValueError, "not successful"):
                self.run_main("deploy", *arguments)
        self.assertFalse(any(c[0] in ("flyctl", "docker") for c in self.calls))

    def test_actual_ci_run_status_failure_refuses(self):
        arguments = self.ci_proof()
        proof = json.loads(self.receipt.with_name("ci-readback.json").read_bytes())
        for key, bad in [("conclusion", "failure"), ("status", "in_progress"), ("path", ".github/workflows/deploy.yml"), ("head_sha", "d" * 40)]:
            run = {"status": "completed", "conclusion": "success", "path": deploy.PREPARE_WORKFLOW, "head_sha": self.source, key: bad}
            with patch.object(deploy, "command", return_value=json.dumps(run)), self.assertRaisesRegex(ValueError, "not succeeded"):
                deploy.validate_ci_readback(self.receipt.read_bytes(), self.receipt.with_name("operator-retention.json").read_bytes(), proof, "fixture/repo", "123")

    def test_successful_run_with_different_actual_artifact_refuses_before_authentication(self):
        arguments = self.ci_proof()
        normal_command = self.command
        def changed_artifact(*args, capture=False):
            result = normal_command(*args, capture=capture)
            if args[:3] == ("gh", "run", "download"):
                (Path(args[-1]) / "financial-image.json").write_bytes(b"different artifact")
            return result
        with patch.object(self, "command", changed_artifact), self.assertRaisesRegex(ValueError, "artifact bytes differ"):
            self.run_main("deploy", *arguments)
        self.assertFalse(any(c[0] in ("flyctl", "docker") for c in self.calls))

    def test_recovery_keeps_current_compatible_config_and_does_not_require_new_ci(self):
        self.retained()
        self.config.write_text(self.config.read_text() + "\n# current recovery config\n")
        self.run_main("recover")
        self.assertFalse(any(c[:2] == ("gh", "api") for c in self.calls))
        self.assertIn(("flyctl", "deploy", "--config", str(self.config), "--image", self.floor,
                       "--env", "MANAGED_PROCESSING_DISABLED=true"), self.calls)

    def test_prepare_retries_the_pull_of_a_tag_the_registry_has_not_published_yet(self):
        def existing(args, **kwargs):
            self.calls.append(tuple(args))
            return subprocess.CompletedProcess(args, 0, "{}", "")
        unpublished = {"misses": 2}
        normal_command = self.command
        def lagging(*args, **kwargs):
            if args[:2] == ("docker", "pull") and ":financial-c1-" in args[-1] and unpublished["misses"]:
                unpublished["misses"] -= 1
                self.calls.append(args)
                raise subprocess.CalledProcessError(1, args)
            return normal_command(*args, **kwargs)
        with patch.object(self, "manifest", existing), patch.object(self, "command", lagging), patch.object(deploy.time, "sleep", lambda _: None):
            self.run_main("prepare")
        pulls = [c for c in self.calls if c[:2] == ("docker", "pull") and ":financial-c1-" in c[-1]]
        self.assertEqual(len(pulls), 3)
        self.assertEqual(json.loads(self.receipt.read_text())["image"], self.digest)

    def test_prepare_never_migrates_and_builds_once(self):
        def existing(args, **kwargs):
            self.calls.append(tuple(args))
            return subprocess.CompletedProcess(args, 0, "{}", "")
        with patch.object(self, "manifest", existing):
            self.run_main("prepare")
        builds = [c for c in self.calls if c[:2] == ("flyctl", "deploy")]
        self.assertEqual(len(builds), 1)
        self.assertIn("--build-only", builds[0])
        self.assertIn("--push", builds[0])
        for label in [f"GH_SHA={self.source}", f"org.opencontainers.image.revision={self.source}"]:
            self.assertIn(label, builds[0])
            self.assertEqual(builds[0][builds[0].index(label) - 1], "--label")
        self.assertEqual(dict(self.environments)[builds[0]]["GITHUB_SHA"], self.source,
                         "flyctl overwrites GH_SHA from GITHUB_SHA inside GitHub Actions")
        self.assertFalse(any(c[:2] == ("docker", "push") for c in self.calls))
        self.assertEqual(json.loads(self.receipt.read_text())["image"], self.digest)
        self.assertTrue(any(c[:2] == ("docker", "run") and "--network" in c and "none" in c and deploy.EXECUTABLE in c for c in self.calls))

    def test_uncertain_floor_read_never_becomes_first_floor_write(self):
        self.manifest_error = "registry temporarily unavailable"
        with self.assertRaisesRegex(RuntimeError, "cannot establish"):
            self.run_main("prepare", "--image", self.digest)
        self.assertFalse(self.receipt.exists())
        self.assertFalse(any(c[:2] == ("docker", "push") for c in self.calls))
        self.assertFalse(any(c[:2] == ("flyctl", "deploy") for c in self.calls))

    def test_explicit_manifest_absence_retains_first_floor_once(self):
        self.floor = self.digest
        self.run_main("prepare", "--image", self.digest)
        self.assertEqual(len([c for c in self.calls if c[:2] == ("docker", "push")]), 1)
        self.assertEqual(json.loads(self.receipt.read_bytes())["recovery_floor"], self.digest)
        self.assertFalse(any(c[:2] == ("flyctl", "deploy") for c in self.calls))

    def test_authentication_error_or_generic_404_never_establishes_floor_absence(self):
        for error in ["404 Not Found", "unauthorized: manifest unknown", "no such manifest: unauthorized", "connection timeout: manifest unknown"]:
            self.manifest_error = error
            with self.assertRaisesRegex(RuntimeError, "cannot establish"):
                self.run_main("prepare", "--image", self.digest)
        self.assertFalse(any(c[:2] == ("docker", "push") for c in self.calls))
        self.assertFalse(self.receipt.exists())

    def test_bad_actual_capability_refuses_before_migration(self):
        self.capability = {"financial_release": "old"}
        with self.assertRaisesRegex(ValueError, "capability"):
            self.run_main("prepare", "--image", self.digest)
        self.assertFalse(self.receipt.exists())
        self.assertFalse(any(c[:2] == ("flyctl", "deploy") for c in self.calls))

    def test_recovery_uses_retained_immutable_floor_and_admission_disable(self):
        self.retained()
        self.run_main("recover")
        published = [c for c in self.calls if c[:2] == ("flyctl", "deploy")]
        self.assertEqual(published, [("flyctl", "deploy", "--config", str(self.config.resolve()), "--image", self.floor, "--env", "MANAGED_PROCESSING_DISABLED=true")])
        self.assertFalse(any("--build-only" in c for c in self.calls))

    def test_deploy_without_retained_receipt_cannot_migrate(self):
        with self.assertRaises(FileNotFoundError):
            self.run_main("deploy")
        self.assertFalse(any(c[:2] == ("flyctl", "deploy") for c in self.calls))

    def test_mutable_or_wrong_application_image_refused(self):
        for image in ["registry.fly.io/fixture-backend:old", "registry.fly.io/other@sha256:" + "a" * 64]:
            with self.assertRaises(ValueError):
                deploy.immutable(image, "fixture-backend")

    def test_pre_floor_config_refused(self):
        self.config.write_text("app='fixture-backend'\n[processes]\napp='/app/coralswarm-backend'\n[deploy]\nrelease_command='/app/release.sh'\n")
        with self.assertRaisesRegex(ValueError, "current backend config"):
            self.run_main("prepare")
        self.assertEqual(self.calls, [])



class BuildRetries(unittest.TestCase):
    def test_a_failed_build_is_retried_under_a_fresh_label(self):
        labels, sleeps = [], []

        def build(label):
            labels.append(label)
            if len(labels) < 3:
                raise subprocess.CalledProcessError(1, ["flyctl"])

        label = deploy.build_with_retries(build, attempts=3, delay=5, sleep=sleeps.append)
        self.assertEqual(label, labels[-1])
        self.assertEqual(len(set(labels)), 3)
        self.assertTrue(all(l.startswith("financial-c1-") for l in labels))
        self.assertEqual(sleeps, [5, 10])

    def test_a_hung_build_is_retried(self):
        labels = []

        def build(label):
            labels.append(label)
            if len(labels) == 1:
                raise subprocess.TimeoutExpired(["flyctl"], 720)

        label = deploy.build_with_retries(build, attempts=2, delay=0, sleep=lambda _: None)
        self.assertEqual(label, labels[1])
        self.assertEqual(len(labels), 2)

    def test_the_last_failure_is_raised(self):
        def build(label):
            raise subprocess.CalledProcessError(1, ["flyctl"])

        with self.assertRaises(subprocess.CalledProcessError):
            deploy.build_with_retries(build, attempts=2, delay=0, sleep=lambda _: None)

if __name__ == "__main__":
    unittest.main()
