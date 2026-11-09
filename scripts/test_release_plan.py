#!/usr/bin/env python3
"""release_plan.py against a real throwaway git repository."""
import datetime
import importlib.util
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

spec = importlib.util.spec_from_file_location("release_plan", Path(__file__).with_name("release_plan.py"))
plan = importlib.util.module_from_spec(spec)
spec.loader.exec_module(plan)
TODAY = datetime.date(2026, 10, 28)


class NextTag(unittest.TestCase):
    def test_first_release_of_the_day_and_numeric_ordering(self):
        self.assertEqual(plan.next_tag([], [], TODAY), ("server-2026.10.28.1", None))
        tags = ["server-2026.10.27.4", "server-2026.10.28.9", "server-2026.10.28.10", "desktop-v1.0.0", "server-latest"]
        self.assertEqual(plan.next_tag(tags, tags, TODAY), ("server-2026.10.28.11", "server-2026.10.28.10"))

    def test_a_burned_tag_takes_its_n_but_is_not_the_latest_release(self):
        tags = ["server-2026.10.27.1", "server-2026.10.28.1"]
        self.assertEqual(plan.next_tag(tags, ["server-2026.10.27.1"], TODAY), ("server-2026.10.28.2", "server-2026.10.27.1"))
        self.assertEqual(plan.next_tag(tags, [], TODAY), ("server-2026.10.28.2", None))

    def test_a_tag_dated_after_today_is_refused(self):
        with self.assertRaisesRegex(SystemExit, "runner clock"):
            plan.next_tag(["server-2026.10.29.1"], [], TODAY)


class Plan(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.cwd = os.getcwd()
        os.chdir(self.directory.name)
        self.git("init", "-q", "-b", "main")
        self.first = self.commit("first")

    def tearDown(self):
        os.chdir(self.cwd)
        self.directory.cleanup()

    def git(self, *args):
        return subprocess.run(["git", "-c", "user.name=t", "-c", "user.email=t@t", *args], check=True,
                              text=True, capture_output=True).stdout.strip()

    def commit(self, message):
        self.git("commit", "-q", "--allow-empty", "-m", message)
        sha = self.git("rev-parse", "HEAD")
        self.git("update-ref", "refs/remotes/origin/main", sha)
        return sha

    def test_releases_main_head_with_the_next_tag(self):
        self.git("tag", "server-2026.10.28.1", self.first)
        head = self.commit("second")
        self.assertEqual(plan.plan("", False, ["server-2026.10.28.1"], TODAY),
                         {"tag": "server-2026.10.28.2", "sha": head, "latest": "server-2026.10.28.1"})

    def test_refuses_when_main_has_not_moved_unless_forced(self):
        self.git("tag", "server-2026.10.27.1", self.first)
        with self.assertRaisesRegex(SystemExit, "has not moved"):
            plan.plan("", False, ["server-2026.10.27.1"], TODAY)
        self.assertEqual(plan.plan("", True, ["server-2026.10.27.1"], TODAY)["sha"], self.first)

    def test_a_burned_tag_at_main_does_not_block_the_next_attempt(self):
        self.git("tag", "server-2026.10.28.1", self.first)
        self.assertEqual(plan.plan("", False, [], TODAY), {"tag": "server-2026.10.28.2", "sha": self.first, "latest": ""})

    def test_a_pinned_commit_must_be_on_main(self):
        self.git("checkout", "-q", "-b", "feature")
        feature = self.git("rev-parse", "HEAD")
        self.git("commit", "-q", "--allow-empty", "-m", "unmerged")
        unmerged = self.git("rev-parse", "HEAD")
        with self.assertRaisesRegex(SystemExit, "not on main"):
            plan.plan(unmerged, False, [], TODAY)
        self.assertEqual(plan.plan(feature, False, [], TODAY)["sha"], self.first)


if __name__ == "__main__":
    unittest.main()
