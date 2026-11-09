"""Exercise the deployable ZIP, including distinct OAuth and bot identities."""
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
import zipfile

from jsonschema import Draft4Validator


SOURCE = Path(__file__).resolve().parent
APP_ID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
AAD_ID = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
BOT_ID = "cccccccc-cccc-4ccc-8ccc-cccccccccccc"
SCHEMA = json.loads((SOURCE / "MicrosoftTeams.v1.25.schema.json").read_text())


class PackageTests(unittest.TestCase):
    def build(self, output, *extra):
        return subprocess.run(
            [sys.executable, str(SOURCE / "package.py"),
             "--teams-app-id", APP_ID, "--aad-app-id", AAD_ID,
             "--version", "1.0.3", "--output", str(output), *extra],
            capture_output=True, text=True,
        )

    def test_distinct_bot_and_oauth_identities_preserve_capture_permissions(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "app.zip"
            result = self.build(output, "--bot-app-id", BOT_ID)
            self.assertEqual(result.returncode, 0, result.stderr)
            with zipfile.ZipFile(output) as package:
                self.assertIsNone(package.testzip())
                self.assertEqual(set(package.namelist()),
                                 {"manifest.json", "color.png", "outline.png"})
                manifest = json.loads(package.read("manifest.json"))
            self.assertEqual(manifest["id"], APP_ID)
            self.assertEqual(manifest["version"], "1.0.3")
            self.assertEqual(manifest["bots"][0]["botId"], BOT_ID)
            self.assertEqual(manifest["webApplicationInfo"]["id"], AAD_ID)
            # Teams SSO: token audience is api://app.coralswarm.example/<AAD app id>.
            self.assertEqual(manifest["webApplicationInfo"]["resource"],
                             f"api://app.coralswarm.example/{AAD_ID}")
            self.assertEqual(manifest["bots"][0]["scopes"], ["team", "groupChat"])
            source = json.loads((SOURCE / "manifest.json").read_text())
            self.assertEqual(manifest["authorization"], source["authorization"])
            self.assertEqual(manifest["configurableTabs"][0]["context"],
                             ["meetingDetailsTab", "meetingSidePanel"])
            self.assertEqual(manifest["configurableTabs"][0]["scopes"], ["groupChat"])
            self.assertEqual(manifest["validDomains"], ["app.coralswarm.example"])
            self.assertNotIn("staticTabs", manifest)
            self.assertEqual(manifest["manifestVersion"], "1.25")
            self.assertEqual(manifest["supportsChannelFeatures"], "tier1")
            Draft4Validator(SCHEMA).validate(manifest)

    def test_template_resource_is_the_sso_audience_placeholder(self):
        source = json.loads((SOURCE / "manifest.json").read_text())
        self.assertEqual(source["webApplicationInfo"],
                         {"id": "{{AAD_APP_ID}}",
                          "resource": "api://app.coralswarm.example/{{AAD_APP_ID}}"})
        self.assertNotEqual(source["version"], "1.0.0")

    def test_legacy_configure_page_is_gone(self):
        self.assertFalse((SOURCE / "configure.html").exists())

    def test_channel_feature_declaration_is_rejected_inside_bot(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "app.zip"
            result = self.build(output, "--bot-app-id", BOT_ID)
            self.assertEqual(result.returncode, 0, result.stderr)
            with zipfile.ZipFile(output) as package:
                manifest = json.loads(package.read("manifest.json"))
            manifest["bots"][0]["supportsChannelFeatures"] = manifest.pop(
                "supportsChannelFeatures"
            )
            errors = list(Draft4Validator(SCHEMA).iter_errors(manifest))
            self.assertTrue(any(list(error.path) == ["bots", 0] and
                                error.validator == "additionalProperties"
                                for error in errors))

    def test_omitted_bot_id_preserves_single_application_deployments(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "app.zip"
            result = self.build(output)
            self.assertEqual(result.returncode, 0, result.stderr)
            with zipfile.ZipFile(output) as package:
                manifest = json.loads(package.read("manifest.json"))
            self.assertEqual(manifest["bots"][0]["botId"], AAD_ID)
            self.assertEqual(manifest["webApplicationInfo"]["id"], AAD_ID)

    def test_invalid_bot_id_does_not_produce_a_package(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "app.zip"
            result = self.build(output, "--bot-app-id", "not-a-guid")
            self.assertNotEqual(result.returncode, 0)
            self.assertFalse(output.exists())


if __name__ == "__main__":
    unittest.main()
