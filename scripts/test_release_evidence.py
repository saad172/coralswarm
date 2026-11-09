#!/usr/bin/env python3
"""Release evidence store and independent verifier, against faked R2, GitHub Release and registry."""
import contextlib
import importlib.util
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location("release_evidence", Path(__file__).with_name("release_evidence.py"))
evidence = importlib.util.module_from_spec(spec)
spec.loader.exec_module(evidence)
financial = evidence.financial

TAG = "server-2026.10.28.1"
SHA = "c" * 40
RUN = "777"
APPS = {"production": "fixture-backend", "staging": "fixture-backend-staging"}
CONFIG = "app='{}'\n[processes]\napp='/app/coralswarm-funded-backend'\n[deploy]\nrelease_command='/app/release.sh'\n"
CREDENTIALS = {"RELEASE_EVIDENCE_BUCKET": "evidence", "R2_ACCOUNT_ID": "account", "R2_ACCESS_KEY_ID": "key",
               "R2_SECRET_ACCESS_KEY": "secret", "FLY_REGISTRY_READ_TOKEN": "fly", "GITHUB_REPOSITORY": "fixture/repo"}


class World:
    """R2 with create-once keys, one GitHub Release, and a registry of tags."""

    def __init__(self):
        self.r2, self.assets, self.tags, self.labels = {}, {}, {}, {}
        self.capability = json.dumps(financial.CAPABILITY)
        self.calls = []
        self.corrupt_r2_reads = False

    def command(self, *args, capture=False, stdin=None):
        self.calls.append(args)
        if args[0] == "curl":
            return self.curl(args, stdin)
        if args[:3] == ("gh", "release", "view"):
            return json.dumps({"assets": [{"name": name} for name in self.assets]})
        if args[:3] == ("gh", "release", "download"):
            directory = Path(args[args.index("--dir") + 1])
            names = [args[args.index("--pattern") + 1]] if "--pattern" in args else list(self.assets)
            for name in names:
                (directory / name).write_bytes(self.assets[name])
            return None
        if args[:3] == ("gh", "release", "upload"):
            path = Path(args[4])
            assert path.name not in self.assets, "uploads never clobber"
            self.assets[path.name] = path.read_bytes()
            return None
        if args[:4] == ("docker", "buildx", "imagetools", "inspect"):
            return json.dumps({"mediaType": "application/vnd.oci.image.manifest.v1+json", "digest": self.tags[args[4]]})
        if args[:3] == ("docker", "image", "inspect"):
            return json.dumps(self.labels[args[-1]])
        if args[:2] == ("docker", "run"):
            return self.capability + "\n"
        return "" if capture else None

    def curl(self, args, stdin):
        assert "secret" not in " ".join(args) and "secret" in stdin, "credentials travel on stdin"
        key = args[-1].split("/evidence/", 1)[1]
        output = Path(args[args.index("-o") + 1])
        if "PUT" in args:
            if key in self.r2:
                return "412"
            self.r2[key] = Path(args[args.index("--data-binary") + 1][1:]).read_bytes()
            return "200"
        if key not in self.r2:
            return "404"
        output.write_bytes(self.r2[key] + (b"x" if self.corrupt_r2_reads else b""))
        return "200"


class ReleaseEvidence(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.root = Path(self.directory.name).resolve()
        self.cwd = os.getcwd()
        os.chdir(self.root)
        Path("fly.toml").write_text(CONFIG.format(APPS["production"]))
        Path("fly.staging.toml").write_text(CONFIG.format(APPS["staging"]))
        self.world = World()
        self.stack = contextlib.ExitStack()
        self.stack.enter_context(patch.dict(os.environ, CREDENTIALS))
        self.stack.enter_context(patch.object(evidence, "command", self.world.command))
        self.evidence_root = self.root / "evidence"
        self.evidence_root.mkdir()

    def tearDown(self):
        self.stack.close()
        os.chdir(self.cwd)
        self.directory.cleanup()

    def prepare(self, side):
        app = APPS[side]
        image = f"registry.fly.io/{app}@sha256:" + ("a" if side == "production" else "d") * 64
        floor = f"registry.fly.io/{app}@sha256:" + "b" * 64
        config = Path(evidence.SIDES[side])
        receipt = {"schema": financial.SCHEMA, "generation": TAG.removeprefix("server-"), "retention": financial.RETENTION,
                   "source_commit": SHA, "app": app, "image": image, "recovery_floor": floor,
                   "capability": financial.CAPABILITY, "executable": financial.EXECUTABLE, "config": str(config.resolve()),
                   "config_sha256": financial.sha256(config.read_bytes())}
        data = (json.dumps(receipt, indent=2) + "\n").encode()
        capability = (self.world.capability + "\n").encode()
        environment = {"GITHUB_ACTIONS": "true", "GITHUB_REPOSITORY": "fixture/repo", "GITHUB_RUN_ID": RUN,
                       "GITHUB_WORKFLOW_REF": "fixture/repo/.github/workflows/release.yml@refs/heads/main", "GITHUB_SHA": "e" * 40, "GITHUB_RUN_ATTEMPT": "1"}
        store = financial.store_evidence(data, capability, "financial-c1-" + side, environment)
        for name, content in [("financial-image.json", data), ("capability.json", capability),
                              ("retained-store.json", (json.dumps(store, indent=2) + "\n").encode())]:
            (self.evidence_root / f"{app}.{name}").write_bytes(content)
        self.world.tags[f"registry.fly.io/{app}:financial-c1-{side}"] = image.split("@")[1]
        self.world.tags[f"registry.fly.io/{app}:{evidence.FLOOR_TAG}"] = floor.split("@")[1]
        self.world.labels[image] = {"GH_SHA": SHA, "org.opencontainers.image.revision": SHA}

    def released(self):
        for side in APPS:
            self.prepare(side)
        dist = self.evidence_root / evidence.DIST
        dist.write_bytes(b"binaries")
        doc = evidence.record(TAG, SHA, "555", RUN, False, dist, self.evidence_root, "evidence")
        (self.evidence_root / evidence.RECORD).write_text(json.dumps(doc, indent=2) + "\n")
        evidence.store(TAG, sorted(self.evidence_root.glob("*.json")), "fixture/repo")
        self.world.assets[evidence.DIST] = dist.read_bytes()

    def test_store_writes_each_file_once_to_both_copies_and_reads_them_back(self):
        self.released()
        expected = {path.name: path.read_bytes() for path in self.evidence_root.glob("*.json")}
        self.assertEqual({key.removeprefix(f"releases/{TAG}/"): value for key, value in self.world.r2.items()}, expected)
        self.assertEqual({name: data for name, data in self.world.assets.items() if name != evidence.DIST}, expected)
        uploads = len([c for c in self.world.calls if c[:3] == ("gh", "release", "upload")])
        evidence.store(TAG, sorted(self.evidence_root.glob("*.json")), "fixture/repo")
        self.assertEqual(len([c for c in self.world.calls if c[:3] == ("gh", "release", "upload")]), uploads, "a re-run is a no-op")

    def test_store_refuses_to_replace_different_bytes_in_either_copy(self):
        self.released()
        path = self.evidence_root / evidence.RECORD
        original = path.read_bytes()
        path.write_bytes(original + b" ")
        with self.assertRaisesRegex(ValueError, "R2 copy"):
            evidence.store(TAG, [path], "fixture/repo")
        self.world.r2.pop(f"releases/{TAG}/{evidence.RECORD}")
        with self.assertRaisesRegex(ValueError, "already exists with different bytes"):
            evidence.store(TAG, [path], "fixture/repo")
        self.assertEqual(self.world.assets[evidence.RECORD], original)

    def test_store_fails_when_the_r2_readback_differs(self):
        self.prepare("production")
        self.world.corrupt_r2_reads = True
        with self.assertRaisesRegex(ValueError, "R2 copy"):
            evidence.store(TAG, [next(self.evidence_root.glob("*.json"))], "fixture/repo")
        self.assertFalse(self.world.assets)

    def test_missing_store_credentials_fail_with_the_provisioning_pointer_before_any_call(self):
        for name in ["R2_SECRET_ACCESS_KEY", "RELEASE_EVIDENCE_BUCKET"]:
            with patch.dict(os.environ, {name: ""}), self.assertRaisesRegex(SystemExit, name + ".*one-time-provisioning"):
                evidence.store(TAG, [self.root / "fly.toml"], "fixture/repo")
        with patch.dict(os.environ, {"FLY_REGISTRY_READ_TOKEN": ""}), self.assertRaisesRegex(SystemExit, "FLY_REGISTRY_READ_TOKEN"):
            evidence.verify(TAG, SHA, "fixture/repo", RUN)
        self.assertEqual(self.world.calls, [])

    def test_record_refuses_evidence_from_another_commit_or_run(self):
        for side in APPS:
            self.prepare(side)
        dist = self.evidence_root / evidence.DIST
        dist.write_bytes(b"binaries")
        with self.assertRaisesRegex(ValueError, "does not describe"):
            evidence.record(TAG, "d" * 40, "555", RUN, False, dist, self.evidence_root, "evidence")
        with self.assertRaisesRegex(ValueError, "does not describe"):
            evidence.record(TAG, SHA, "555", "778", False, dist, self.evidence_root, "evidence")

    def test_verifier_accepts_a_consistent_release(self):
        self.released()
        evidence.verify(TAG, SHA, "fixture/repo", RUN)
        self.assertIn(("docker", "login", "registry.fly.io", "--username", "x", "--password-stdin"), self.world.calls)

    def test_verifier_fails_on_every_independent_mismatch(self):
        self.released()
        production = APPS["production"]
        image = json.loads(self.world.assets[f"{production}.financial-image.json"])["image"]
        receipt_key = f"releases/{TAG}/{production}.financial-image.json"
        tampers = {
            "R2 receipt differs from the release asset": lambda w: w.r2.__setitem__(receipt_key, w.r2[receipt_key] + b" "),
            "R2 record missing": lambda w: w.r2.pop(f"releases/{TAG}/{evidence.RECORD}"),
            "build tag resolves elsewhere": lambda w: w.tags.__setitem__(f"registry.fly.io/{production}:financial-c1-production", "sha256:" + "f" * 64),
            "floor tag moved": lambda w: w.tags.__setitem__(f"registry.fly.io/{production}:{evidence.FLOOR_TAG}", "sha256:" + "f" * 64),
            "GH_SHA label is another commit": lambda w: w.labels.__setitem__(image, {"GH_SHA": "d" * 40, "org.opencontainers.image.revision": SHA}),
            "revision label missing": lambda w: w.labels.__setitem__(image, {"GH_SHA": SHA}),
            "capability output changed": lambda w: setattr(w, "capability", json.dumps({"financial_release": "old"})),
            "dist checksum differs": lambda w: w.assets.__setitem__(evidence.DIST, b"other binaries"),
        }
        for name, tamper in tampers.items():
            with self.subTest(name):
                saved = (dict(self.world.r2), dict(self.world.assets), dict(self.world.tags), dict(self.world.labels), self.world.capability)
                tamper(self.world)
                with self.assertRaises(ValueError):
                    evidence.verify(TAG, SHA, "fixture/repo", RUN)
                self.world.r2, self.world.assets, self.world.tags, self.world.labels, self.world.capability = saved

    def test_verifier_refuses_a_record_whose_receipt_hash_differs(self):
        self.released()
        doc = json.loads(self.world.assets[evidence.RECORD])
        doc["financial"]["staging"]["receipt_sha256"] = "0" * 64
        forged = (json.dumps(doc, indent=2) + "\n").encode()
        self.world.assets[evidence.RECORD] = self.world.r2[f"releases/{TAG}/{evidence.RECORD}"] = forged
        with self.assertRaisesRegex(ValueError, "SHA256"):
            evidence.verify(TAG, SHA, "fixture/repo", RUN)

    def test_select_hands_deploy_the_recorded_side_from_the_release(self):
        self.released()
        environment = self.root / "github-env"
        with patch.dict(os.environ, {"GITHUB_ENV": str(environment)}):
            chosen = evidence.select(TAG, SHA, "staging", self.root / "selected", "fixture/repo")
        receipt = Path(chosen["RECEIPT"]).read_bytes()
        self.assertEqual(receipt, self.world.assets[f"{APPS['staging']}.financial-image.json"])
        self.assertEqual((chosen["PREPARE_RUN_ID"], chosen["BUILD_RUN_ID"], chosen["RECEIPT_SHA256"]), (RUN, "555", evidence.sha256(receipt)))
        self.assertIn(f"STORE_EVIDENCE={chosen['STORE_EVIDENCE']}\n", environment.read_text())

    def test_select_refuses_a_release_without_matching_evidence(self):
        self.released()
        asset = f"{APPS['production']}.financial-image.json"
        self.world.assets[asset] += b" "
        with self.assertRaisesRegex(ValueError, "differs from its locked R2 copy"):
            evidence.select(TAG, SHA, "production", self.root / "a", "fixture/repo")
        with self.assertRaisesRegex(ValueError, "does not describe"):
            evidence.select(TAG, "d" * 40, "production", self.root / "b", "fixture/repo")
        self.world.assets.pop(evidence.RECORD)
        with self.assertRaisesRegex(SystemExit, "only releases cut by the Release workflow"):
            evidence.select(TAG, SHA, "production", self.root / "c", "fixture/repo")

    def test_select_refuses_a_release_asset_that_differs_from_its_locked_r2_copy(self):
        self.released()
        asset = f"{APPS['production']}.retained-store.json"
        forged = json.loads(self.world.assets[asset])
        forged["run_id"] = "999"
        self.world.assets[asset] = json.dumps(forged).encode()
        with self.assertRaisesRegex(ValueError, f"{asset} on the release differs from its locked R2 copy"):
            evidence.select(TAG, SHA, "production", self.root / "a", "fixture/repo")
        self.world.assets[evidence.RECORD] += b" "
        with self.assertRaisesRegex(ValueError, "release-images.json on the release differs"):
            evidence.select(TAG, SHA, "production", self.root / "b", "fixture/repo")

    def test_verifier_refuses_a_record_from_another_run(self):
        self.released()
        with self.assertRaisesRegex(ValueError, "not this run 778"):
            evidence.verify(TAG, SHA, "fixture/repo", "778")

    def test_record_is_byte_identical_on_a_rerun(self):
        for side in APPS:
            self.prepare(side)
        dist = self.evidence_root / evidence.DIST
        dist.write_bytes(b"binaries")
        first = evidence.record(TAG, SHA, "555", RUN, False, dist, self.evidence_root, "evidence")
        self.assertEqual(json.dumps(first), json.dumps(evidence.record(TAG, SHA, "555", RUN, False, dist, self.evidence_root, "evidence")))

    def test_cli_refuses_a_non_release_tag(self):
        with patch.object(sys, "argv", ["release_evidence.py", "verify", "--tag", "server-latest", "--sha", SHA]), \
                self.assertRaisesRegex(SystemExit, "not a server-YYYY.MM.DD.N tag"):
            evidence.main()


if __name__ == "__main__":
    unittest.main()
