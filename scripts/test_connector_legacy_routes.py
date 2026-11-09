"""Exercise X2 guard failures without modifying the working tree."""
import pathlib
import subprocess
import tempfile
import unittest

SCRIPT = pathlib.Path(__file__).with_name('check_connector_legacy_routes.py').read_text()


class GuardTests(unittest.TestCase):
    def check(self, router='', openapi='', consumer=''):
        with tempfile.TemporaryDirectory() as folder:
            root = pathlib.Path(folder)
            files = {
                'scripts/check.py': SCRIPT,
                'backend/src/lib.rs': router,
                'backend/src/openapi.rs': openapi,
                'web/lib/appQueries.ts': 'api.composio.list(); api.composio.status();',
                'web/new.ts': consumer,
            }
            for name, content in files.items():
                path = root / name
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(content)
            subprocess.run(['git', 'init', '-q', folder], check=True)
            subprocess.run(['git', 'add', '.'], cwd=root, check=True)
            return subprocess.run(['python3', 'scripts/check.py'], cwd=root, capture_output=True).returncode

    def test_internal_adapter_calls_allowed(self):
        self.assertEqual(self.check(router='routes::teams_ingest::list_chats(cx)'), 0)

    def test_registration_rejected(self):
        self.assertNotEqual(self.check(router='.route("/connectors/slack/backfill", post(handler))'), 0)

    def test_openapi_rejected(self):
        self.assertNotEqual(self.check(openapi='teams_ingest::list_chats'), 0)

    def test_direct_and_wrapper_consumers_rejected(self):
        for consumer in ['fetch("/v1/connectors/slack/backfill")', 'api.msTeams.chats(token)', 'slackOnboardingQueryOptions(ctx)']:
            with self.subTest(consumer=consumer):
                self.assertNotEqual(self.check(consumer=consumer), 0)


if __name__ == '__main__':
    unittest.main()
