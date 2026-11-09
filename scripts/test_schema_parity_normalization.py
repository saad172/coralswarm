#!/usr/bin/env python3
"""Check the dump normalizer without running migrations or a database."""
from pathlib import Path
import re
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parent.parent
CONTROL_REACHABILITY = "  AND COALESCE(resolve_graph_reachable_by_user(a.graph_id,NULLIF(current_setting('app.user_id',true),'')::uuid),false)"
TENANT_REACHABILITY = "  AND COALESCE(resolve_tenant_graph_reachable_by_user(a.graph_id,current_setting('app.account_id')::uuid,NULLIF(current_setting('app.user_id',true),'')::uuid),false)"
CONTROL = """CREATE FUNCTION public.library_legacy_headline_sha256(g uuid, a_id uuid) RETURNS text
    LANGUAGE sql STABLE
    SET \"TimeZone\" TO 'UTC'
    AS $$
 SELECT encode(sha256(convert_to(jsonb_build_array('legacy_headline_v1',a.graph_id,a.atom_id,a.title,a.title_kind,a.source_type,a.source_id,a.timestamp,a.created_at)::text,'UTF8')),'hex')
 FROM atoms a
 WHERE a.graph_id=g AND a.atom_id=a_id
  AND a.title IS NOT NULL AND btrim(a.title)<>''
  AND a.timestamp IS NOT NULL AND isfinite(a.timestamp) AND isfinite(a.created_at)
  AND NOT a.source_evidence_bound
  AND NOT EXISTS(SELECT 1 FROM source_version_chunks c WHERE c.graph_id=a.graph_id AND c.atom_id=a.atom_id)
  AND library_atom_permitted(a.graph_id,a.atom_id)
""" + CONTROL_REACHABILITY + "\n$$;\n"
TENANT = CONTROL.replace(CONTROL_REACHABILITY, TENANT_REACHABILITY)


class NormalizationTests(unittest.TestCase):
    def normalize(self, dump):
        script = (ROOT / 'scripts/check_schema_parity.sh').read_text()
        function = re.search(r'^norm\(\) \{\n.*?^\}', script, re.M | re.S).group()
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / 'dump.sql'
            source.write_text(dump)
            driver = Path(directory) / 'pg_dump'
            driver.write_text('#!/bin/sh\ncat "$DUMP_FIXTURE"\n')
            driver.chmod(0o755)
            result = subprocess.run(['bash', '-c', 'set -euo pipefail\n' + function + '\nnorm fixture'],
                                    env={'PG_DUMP': str(driver), 'DUMP_FIXTURE': str(source),
                                         'PATH': '/usr/bin:/bin'}, capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            return result.stdout

    def test_exact_legacy_headline_plane_difference_compares_equal(self):
        self.assertEqual(self.normalize(CONTROL), self.normalize(TENANT))

    def test_permission_predicate_and_scope_drift_still_differ(self):
        for before, after in [
            ('AND library_atom_permitted(a.graph_id,a.atom_id)', 'AND true'),
            ('WHERE a.graph_id=g AND a.atom_id=a_id', 'WHERE a.atom_id=a_id'),
            ('AND NOT a.source_evidence_bound', 'AND a.source_evidence_bound'),
            ("current_setting('app.user_id',true)", "current_setting('app.other_user_id',true)"),
            ("current_setting('app.account_id')", "current_setting('app.other_account_id')"),
            ("::uuid),false)", "::uuid),true)"),
        ]:
            with self.subTest(change=after):
                self.assertNotEqual(self.normalize(CONTROL), self.normalize(TENANT.replace(before, after)))

    def test_signature_and_other_function_reachability_still_differ(self):
        for before, after in [
            ('g uuid, a_id uuid', 'g uuid, other_id uuid'),
            ('library_legacy_headline_sha256', 'other_library_function'),
        ]:
            with self.subTest(change=after):
                self.assertNotEqual(self.normalize(CONTROL.replace(before, after)),
                                    self.normalize(TENANT.replace(before, after)))
        other = TENANT.replace('library_legacy_headline_sha256', 'other_library_function')
        self.assertNotEqual(self.normalize(CONTROL + other.replace(TENANT_REACHABILITY, CONTROL_REACHABILITY)),
                            self.normalize(TENANT + other))

    def test_access_class_exact_difference_and_drift(self):
        def fixture(plane, migration):
            sql = (ROOT / f'backend/migrations_{plane}/{migration}_library_access_class.sql').read_text()
            body = sql.split('CREATE FUNCTION public.library_evidence_class(g UUID) RETURNS JSONB', 1)[1].split('$$;', 1)[0]
            return 'CREATE FUNCTION public.library_evidence_class(g uuid) RETURNS jsonb' + body + '$$;\n'
        control = fixture('control', '0391')
        tenant = fixture('tenant', '0206')
        self.assertEqual(self.normalize(control), self.normalize(tenant))
        for before, after in [
            ('w.graph_id=$1', 'true'),
            ("AT TIME ZONE 'UTC'", "AT TIME ZONE 'America/Chicago'"),
            ("app.account_id", "app.other_account_id"),
            ('ELSE true END', 'ELSE false END'),
            ('LANGUAGE sql STABLE', 'LANGUAGE sql VOLATILE'),
            ('library_access_contract_digest()', 'other_digest()'),
        ]:
            with self.subTest(change=after):
                self.assertNotEqual(self.normalize(control), self.normalize(tenant.replace(before, after)))
        self.assertNotEqual(self.normalize(control.replace('),false))', '),true))')), self.normalize(tenant))
        for before, after in [('g uuid', 'other uuid'), ('library_evidence_class', 'other_function')]:
            self.assertNotEqual(self.normalize(control.replace(before, after)), self.normalize(tenant.replace(before, after)))
        other = tenant.replace('library_evidence_class', 'other_function')
        self.assertNotEqual(self.normalize(control + control.replace('library_evidence_class', 'other_function')), self.normalize(tenant + other))


if __name__ == '__main__':
    unittest.main()
