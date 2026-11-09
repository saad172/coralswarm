#!/usr/bin/env python3
"""Filesystem fixtures, not a claim that hosted APT/network setup passed."""
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

SPEC = importlib.util.spec_from_file_location("ci_apt_setup", Path(__file__).with_name("ci_apt_setup.py"))
SETUP = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(SETUP)


class AptSetupTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        (self.root / "sources.list.d").mkdir()
        (self.root / "apt.conf.d").mkdir()
        self.source = self.root / "sources.list.d/ubuntu.sources"
        self.original = (
            "Types: deb\nURIs: mirror+file:/etc/apt/apt-mirrors.txt\n"
            "Suites: noble noble-updates noble-backports noble-security\n"
            "Components: main restricted universe multiverse\n"
            "Signed-By: /usr/share/keyrings/ubuntu-archive-keyring.gpg\n"
        )
        self.source.write_text(self.original)

    def test_runner_mirror_cannot_be_reselected_for_indexes_or_packages(self):
        mirror = self.root / "apt-mirrors.txt"
        mirror.write_text("http://azure.archive.ubuntu.com/ubuntu/\tpriority:1\n"
                          "https://archive.ubuntu.com/ubuntu/\tpriority:2\n")
        SETUP.configure(self.root)
        self.assertEqual(self.source.read_text(), self.original.replace(
            "mirror+file:/etc/apt/apt-mirrors.txt", "https://archive.ubuntu.com/ubuntu/"))
        self.assertIn("priority:1", mirror.read_text())  # no unrelated file deletion

    def test_preserves_other_sources_and_signing_options(self):
        third_party = self.root / "sources.list.d/microsoft-prod.list"
        text = "deb [arch=amd64 signed-by=/usr/share/keyrings/microsoft.gpg] https://packages.microsoft.com/ubuntu/24.04/prod noble main\n"
        third_party.write_text(text)
        SETUP.configure(self.root)
        self.assertEqual(third_party.read_text(), text)
        self.assertIn("Signed-By: /usr/share/keyrings/ubuntu-archive-keyring.gpg", self.source.read_text())

    def test_direct_and_legacy_ubuntu_sources(self):
        self.source.unlink()
        source = self.root / "sources.list"
        original = "deb [signed-by=/keyring.gpg] http://azure.archive.ubuntu.com/ubuntu/ noble main\n"
        source.write_text(original)
        SETUP.configure(self.root)
        self.assertEqual(source.read_text(), original.replace(
            "http://azure.archive.ubuntu.com/ubuntu/", "https://archive.ubuntu.com/ubuntu/"))

    def test_idempotent_and_bounded_fail_closed_configuration(self):
        SETUP.configure(self.root)
        first = {p: p.read_bytes() for p in self.root.rglob("*") if p.is_file()}
        SETUP.configure(self.root)
        self.assertEqual(first, {p: p.read_bytes() for p in self.root.rglob("*") if p.is_file()})
        conf = (self.root / "apt.conf.d/zzzz-coralswarm-ci").read_text()
        self.assertIn('Acquire::Retries "1";', conf)
        self.assertIn('Acquire::http::Timeout "15";', conf)
        self.assertIn('Acquire::https::Timeout "15";', conf)
        self.assertIn('APT::Update::Error-Mode "any";', conf)
        for unsafe in ["AllowUnauthenticated", "AllowInsecure", "Verify-Peer", "Verify-Host", "Trusted", "Check-Valid-Until"]:
            self.assertNotIn(unsafe, conf)

    def test_unknown_image_fails_before_writing(self):
        self.source.write_text("Types: deb\nURIs: https://unrelated.example/ubuntu/\n")
        with self.assertRaisesRegex(ValueError, "No recognized Ubuntu"):
            SETUP.configure(self.root)
        self.assertFalse((self.root / "apt.conf.d/zzzz-coralswarm-ci").exists())


class WorkflowFailureTests(unittest.TestCase):
    """Run the actual workflow shell with command fixtures, never real sudo/APT."""

    def test_dependency_commands_are_bounded_and_fail_closed(self):
        workflows = Path(__file__).resolve().parent.parent / ".github/workflows"
        lines = (workflows / "pr.yml").read_text().splitlines() + (workflows / "deploy.yml").read_text().splitlines()
        blocks = []
        for i, line in enumerate(lines):
            if line.strip() != "run: |" or i + 1 == len(lines):
                continue
            if "sudo python3" not in lines[i + 1] or "ci_apt_setup.py" not in lines[i + 1]:
                continue
            block = []
            for command in lines[i + 1:]:
                if command and not command.startswith("          "):
                    break
                block.append(command[10:])
            blocks.append("\n".join(block))
        self.assertEqual(len(blocks), 7)  # 3 builds, shards, quarantine, N-1, staging smoke
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            driver = root / "driver"
            driver.write_text("#!" + sys.executable + "\n" + '''import json, os, pathlib, subprocess, sys
name = pathlib.Path(sys.argv[0]).name
args = sys.argv[1:]
with open(os.environ['CALL_LOG'], 'a') as log:
    log.write(json.dumps([name, *args]) + '\\n')
if name == os.environ.get('FAIL_COMMAND'):
    sys.exit(int(os.environ['FAIL_STATUS']))
if name == 'sudo':
    sys.exit(subprocess.call(args))
if name == 'timeout':
    assert args[:2] == ['--kill-after=10s', '180s'], args
    sys.exit(subprocess.call(args[2:]))
''')
            driver.chmod(0o755)
            for name in ("sudo", "timeout", "python3", "apt-config", "apt-get", "node", "npx"):
                (root / name).symlink_to(driver)
            env = {**os.environ, "PATH": str(root) + os.pathsep + os.environ["PATH"],
                   "CALL_LOG": str(root / "calls.jsonl")}
            for block in blocks:
                for cache_hit in ("true", "false"):
                    script = block.replace("${{ steps.pw-cache.outputs.cache-hit }}", cache_hit)
                    for failure, status in (("", 0), ("python3", 42), ("timeout", 124), ("apt-get", 100), ("node", 43)):
                        if failure == "apt-get" and "apt-get" not in block:
                            continue
                        if failure == "node" and "install-deps" not in block:
                            continue
                        with self.subTest(cache_hit=cache_hit, failure=failure, browser="install-deps" in block):
                            (root / "calls.jsonl").write_text("")
                            result = subprocess.run(["bash", "-e", "-c", script], env={**env,
                                "FAIL_COMMAND": failure, "FAIL_STATUS": str(status)}, capture_output=True, text=True)
                            self.assertEqual(result.returncode, status, result.stderr)
                            calls = [json.loads(line) for line in (root / "calls.jsonl").read_text().splitlines()]
                            if failure:
                                self.assertEqual(calls[-1][0], failure)
                            elif "install-deps" in block:
                                self.assertTrue(any(call[0] == "node" and call[-2:] == ["install-deps", "chromium"] for call in calls))
                                browser = [call for call in calls if call[0] == "npx" and "install" in call]
                                expected = int(cache_hit != "true") if "steps.pw-cache.outputs.cache-hit" in block else 1
                                self.assertEqual(len(browser), expected)


if __name__ == "__main__":
    unittest.main()
