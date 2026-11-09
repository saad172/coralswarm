#!/usr/bin/env python3
"""Offline checks for the CI PostgreSQL role fixture driver and the local runner's handling of it.

The ci.yml-shape assertions (which jobs prepare roles, pg routing) were retired
with the Postgres jobs themselves (CI redesign, PR 4)."""
import json
import importlib.util
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import types
import unittest
from unittest import mock

ROOT = Path(__file__).resolve().parent.parent
class DriverTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.log = self.root / 'commands.jsonl'
        driver = self.root / 'driver'
        driver.write_text('#!' + sys.executable + '\n' + '''import json, os, pathlib, sys
name = pathlib.Path(sys.argv[0]).name
args = sys.argv[1:]
fixture = name == 'docker' or (name == 'psql' and not any(a in ['-qc', '-tAc'] for a in args))
data = sys.stdin.read() if fixture else ''
with pathlib.Path(os.environ['DRIVER_LOG']).open('a') as log:
    log.write(json.dumps({'command':[name,*args], 'stdin':data}) + '\\n')
if fixture: sys.exit(int(os.environ.get('FIXTURE_EXIT', '0')))
if name == 'sqlx': sys.exit(61)
''')
        driver.chmod(0o755)
        for name in ['docker', 'psql', 'sqlx']:
            (self.root / name).symlink_to(driver)
        self.env = {**os.environ, 'PATH': str(self.root) + os.pathsep + os.environ['PATH'],
                    'DRIVER_LOG': str(self.log), 'DATABASE_URL': 'postgres://postgres:fixture@localhost:5432/postgres',
                    'SQLX': str(self.root / 'sqlx')}

    def run_helper(self, *args):
        return subprocess.run(['bash', str(ROOT / 'scripts/prepare_ci_pg_roles.sh'), *args],
                              env=self.env, input='', capture_output=True, text=True)

    def calls(self):
        return [json.loads(line) for line in self.log.read_text().splitlines()] if self.log.exists() else []

    def test_both_transports_pass_identical_fixture_and_fail_closed_flags(self):
        self.assertEqual(self.run_helper('--container', 'a' * 64).returncode, 0)
        self.assertEqual(self.run_helper('--localhost').returncode, 0)
        container, host = self.calls()
        self.assertEqual(container['command'], ['docker', 'exec', '-i', 'a' * 64, 'psql', '-U', 'postgres', '-d', 'postgres', '-X', '-v', 'ON_ERROR_STOP=1'])
        self.assertEqual(host['command'], ['psql', self.env['DATABASE_URL'], '-X', '-v', 'ON_ERROR_STOP=1'])
        self.assertEqual(container['stdin'], host['stdin'])
        sql = host['stdin']
        self.assertIn("ARRAY['coralswarm_app', 'coralswarm_tenant_app']", sql)
        self.assertRegex(sql, r'IF NOT FOUND THEN\s+EXECUTE format\(\x27CREATE ROLE %I LOGIN NOSUPERUSER NOBYPASSRLS NOCREATEDB NOCREATEROLE NOREPLICATION')
        self.assertIn('RAISE EXCEPTION', sql)
        for attribute in ['rolcanlogin', 'rolsuper', 'rolbypassrls', 'rolcreatedb', 'rolcreaterole', 'rolreplication']:
            self.assertIn('existing.' + attribute, sql)
        self.assertNotRegex(sql, r'\b(ALTER|PASSWORD|GRANT|OWNER)\b')

    def test_invalid_inputs_never_invoke_a_command_or_print_dsn(self):
        for args in [[], ['--container'], ['--container', '-evil'], ['--container', 'not-a-container'],
                     ['--container', 'a' * 11], ['--container', 'a' * 65], ['--container', 'A' * 12],
                     ['--container', 'a' * 64, 'extra'], ['--localhost', 'extra']]:
            with self.subTest(args=args):
                result = self.run_helper(*args)
                self.assertEqual(result.returncode, 2)
                self.assertNotIn('unbound variable', result.stderr)
        for url in ['', 'postgres://postgres:secret@remote:5432/db', 'postgres://postgres:secret@localhost:0/db',
                    'postgres://postgres:secret@localhost:99999/db', 'postgres://postgres:secret@localhost:5432/db?host=remote']:
            with self.subTest(url=url):
                self.env['DATABASE_URL'] = url
                result = self.run_helper('--localhost')
                self.assertEqual(result.returncode, 2)
                self.assertNotIn('secret', result.stdout + result.stderr)
        self.assertEqual(self.calls(), [])

    def test_transport_failure_propagates_exact_status(self):
        self.env['FIXTURE_EXIT'] = '47'
        for args in [('--localhost',), ('--container', 'a' * 12)]:
            with self.subTest(args=args):
                self.assertEqual(self.run_helper(*args).returncode, 47)

    def test_direct_parity_aborts_before_scratch_or_migration_on_fixture_failure(self):
        self.env['FIXTURE_EXIT'] = '53'
        result = subprocess.run(['bash', str(ROOT / 'scripts/check_schema_parity.sh')],
                                env=self.env, input='', capture_output=True, text=True)
        self.assertEqual(result.returncode, 53, result.stderr)
        self.assertEqual(len(self.calls()), 1)
        self.assertIn('CREATE ROLE', self.calls()[0]['stdin'])

    def test_direct_parity_bootstraps_before_first_control_migration(self):
        result = subprocess.run(['bash', str(ROOT / 'scripts/check_schema_parity.sh')],
                                env=self.env, input='', capture_output=True, text=True)
        self.assertEqual(result.returncode, 61, result.stderr)
        calls = self.calls()
        self.assertIn('CREATE ROLE', calls[0]['stdin'])
        migrate = next(i for i, call in enumerate(calls) if call['command'][0] == 'sqlx')
        self.assertGreater(migrate, 0)
        self.assertEqual(calls[migrate]['command'][1:], ['migrate', 'run', '--source', 'backend/migrations_control'])


class LocalRunnerRoleTests(unittest.TestCase):
    """Exercise the real step runner without services or third-party YAML."""

    @classmethod
    def setUpClass(cls):
        spec = importlib.util.spec_from_file_location('ci_local_role_tests', ROOT / 'scripts/ci_local.py')
        cls.runner = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = cls.runner
        # Parsing workflows is outside run_step. Keep this existing stdlib-only
        # migration guard independent of an installed PyYAML distribution.
        with mock.patch.object(sys, 'dont_write_bytecode', True), \
                mock.patch.dict(sys.modules, {'yaml': types.ModuleType('yaml')}):
            spec.loader.exec_module(cls.runner)

    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.calls = []
        self.exit_code = 0
        self.addCleanup(mock.patch.stopall)
        mock.patch.dict(os.environ, {'PATH': os.defpath}, clear=True).start()

        def capture(command, **kwargs):
            self.calls.append({'command': command, 'env': kwargs['env'].copy()})
            return types.SimpleNamespace(stdout=iter(['mock step result\n']), wait=lambda: self.exit_code)

        mock.patch.object(self.runner.subprocess, 'Popen', side_effect=capture).start()

    def step(self):
        return {'run': 'bash scripts/prepare_ci_pg_roles.sh --container "$POSTGRES_SERVICE_ID"',
                'env': {'POSTGRES_SERVICE_ID': '${{ job.services.postgres.id }}'}}

    def run_step(self, step, job=None, port=55491, carry=None, base_sha=None):
        if job is None:
            job = {'services': {'postgres': {}}}
        return self.runner.run_step(step, job, self.root, port, base_sha, False, False, carry)

    def test_evacuation_alias_uses_declared_job_database_on_assigned_lane(self):
        job = {'env': {'ORGT_TEST_BASE_URL': 'postgres://test:fixture@localhost:5432/postgres'}}
        step = {'run': 'bash scripts/migrate_shared_db_org.sh --self-test',
                'env': {'ORGT_SELF_TEST_DSN': '${{ env.ORGT_TEST_BASE_URL }}'}}
        for port in [55491, 56601]:
            with self.subTest(port=port):
                self.assertEqual(self.run_step(step, job, port=port), (True, '', False))
                self.assertEqual(self.calls[-1]['env']['ORGT_SELF_TEST_DSN'],
                                 f'postgres://test:fixture@localhost:{port}/postgres')
                self.assertNotIn('DATABASE_URL', self.calls[-1]['env'])

    def test_unresolved_alias_refuses_before_process_or_temporary_files(self):
        os.environ['ORGT_TEST_BASE_URL'] = 'postgres://host:must_not_be_used@localhost:5432/host'
        step = {'run': 'bash scripts/migrate_shared_db_org.sh --self-test',
                'env': {'ORGT_SELF_TEST_DSN': '${{ env.ORGT_TEST_BASE_URL }}'}}
        cases = [({}, None),
                 ({'ORGT_TEST_BASE_URL': ''}, None),
                 ({'ORGT_TEST_BASE_URL': None}, None),
                 ({'ORGT_TEST_BASE_URL': '${{ env.OTHER }}'}, None),
                 ({'ORGT_TEST_BASE_URL': 'postgres://nested:private@localhost:5432/${{ env.DB }}'}, None),
                 ({'ORGT_TEST_BASE_URL': 'declared'}, {'ORGT_TEST_BASE_URL': ''}),
                 ({}, {'ORGT_TEST_BASE_URL': '${{ secrets.DATABASE }}'})]
        for declared, carry in cases:
            with self.subTest(declared=declared, carry=carry), \
                    mock.patch.object(self.runner.tempfile, 'mkdtemp', wraps=tempfile.mkdtemp) as allocate:
                self.assertEqual(self.run_step(step, {'env': declared}, carry=carry),
                                 (False, 'unresolved workflow env alias ORGT_SELF_TEST_DSN <- ORGT_TEST_BASE_URL', False))
                allocate.assert_not_called()
        self.assertEqual(self.calls, [])

    def test_alias_uses_prior_github_env_export_and_remaps_its_lane(self):
        job = {'env': {'ORGT_TEST_BASE_URL': 'postgres://job:fixture@localhost:5432/job'}}
        carry = {}
        original_capture = self.runner.subprocess.Popen.side_effect

        def export_then_capture(command, **kwargs):
            if command[-1] == 'export previous source':
                Path(kwargs['env']['GITHUB_ENV']).write_text(
                    'ORGT_TEST_BASE_URL=postgres://carried:fixture@127.0.0.1:5432/carried\n')
            return original_capture(command, **kwargs)

        self.runner.subprocess.Popen.side_effect = export_then_capture
        self.assertEqual(self.run_step({'run': 'export previous source'}, job, carry=carry),
                         (True, '', False))
        self.assertEqual(carry, {'ORGT_TEST_BASE_URL': 'postgres://carried:fixture@127.0.0.1:5432/carried'})
        before = json.loads(json.dumps(job)), carry.copy(), dict(os.environ)
        for target in ['P7_TEST_BASE_URL', 'TEAMS_HEALTH_TEST_BASE_URL']:
            with self.subTest(target=target):
                step = {'run': 'verify alias', 'env': {target: '${{env.ORGT_TEST_BASE_URL}}',
                                                     'REQUIRE_PG_SUITE': '1'}}
                self.assertEqual(self.run_step(step, job, port=56601, carry=carry), (True, '', False))
                self.assertEqual(self.calls[-1]['env'][target],
                                 'postgres://carried:fixture@127.0.0.1:56601/carried')
                self.assertEqual(self.calls[-1]['env']['REQUIRE_PG_SUITE'], '1')
                self.assertNotIn('DATABASE_URL', self.calls[-1]['env'])
        self.assertEqual((job, carry, dict(os.environ)), before)

    def test_step_env_order_does_not_change_the_workflow_alias_context(self):
        job = {'env': {'SOURCE': 'declared'}}
        for entries in [[('SOURCE', 'step-only'), ('DEST', '${{ env.SOURCE }}')],
                        [('DEST', '${{ env.SOURCE }}'), ('SOURCE', 'step-only')]]:
            with self.subTest(entries=entries):
                self.assertEqual(self.run_step({'run': 'verify alias', 'env': dict(entries)}, job),
                                 (True, '', False))
                self.assertEqual(self.calls[-1]['env']['DEST'], 'declared')
                self.assertEqual(self.calls[-1]['env']['SOURCE'], 'step-only')

    def test_sibling_step_value_cannot_supply_a_missing_workflow_alias(self):
        for entries in [[('SOURCE', 'step-only'), ('DEST', '${{ env.SOURCE }}')],
                        [('DEST', '${{ env.SOURCE }}'), ('SOURCE', 'step-only')]]:
            with self.subTest(entries=entries), \
                    mock.patch.object(self.runner.tempfile, 'mkdtemp', wraps=tempfile.mkdtemp) as allocate:
                self.assertEqual(self.run_step({'run': 'verify alias', 'env': dict(entries)}),
                                 (False, 'unresolved workflow env alias DEST <- SOURCE', False))
                allocate.assert_not_called()
        self.assertEqual(self.calls, [])

    def test_base_ref_literals_and_unknown_expressions_keep_existing_behavior(self):
        for base in [None, '0123456789abcdef0123456789abcdef01234567']:
            with self.subTest(base=base):
                step = {'run': 'verify original rendering',
                        'env': {'BASE_REF': '${{ github.event.pull_request.base.sha }}',
                                'DATABASE_URL': 'postgres://literal:fixture@localhost:5432/literal',
                                'SECRET': '${{ secrets.DATABASE }}',
                                'COMPOSITE': 'prefix-${{ env.SOURCE }}',
                                'CONTEXT': '${{ github.ref }}'}}
                self.assertEqual(self.run_step(step, {'env': {'SOURCE': 'declared'}}, base_sha=base),
                                 (True, '', False))
                env = self.calls[-1]['env']
                self.assertEqual(env['BASE_REF'], base or '')
                self.assertEqual(env['DATABASE_URL'], 'postgres://literal:fixture@localhost:55491/literal')
                for name in ['SECRET', 'COMPOSITE', 'CONTEXT']:
                    self.assertEqual(env[name], '')

    def test_exact_hosted_step_uses_assigned_lane_maintenance_database(self):
        for port in [55491, 56601]:
            with self.subTest(port=port):
                self.assertEqual(self.run_step(self.step(), port=port), (True, '', False))
                call = self.calls[-1]
                self.assertEqual(call['command'][-1], 'bash scripts/prepare_ci_pg_roles.sh --localhost')
                self.assertEqual(call['env']['DATABASE_URL'], f'postgres://postgres:postgres@localhost:{port}/postgres')
                self.assertNotIn('${{', call['command'][-1])

    def test_maintenance_environment_does_not_escape_to_later_test_step(self):
        for database_url in [None, 'postgres://reader:fixture@localhost:5432/test_database']:
            with self.subTest(database_url=database_url):
                job = {'services': {'postgres': {}}, 'env': {'ORGT_TEST_BASE_URL': 'postgres://test:fixture@localhost:5432/test_database'}}
                if database_url:
                    job['env']['DATABASE_URL'] = database_url
                before = json.loads(json.dumps(job))
                carry = {'PREVIOUS_STEP': 'retained'}
                parent_environment = dict(os.environ)
                self.assertTrue(self.run_step(self.step(), job, carry=carry)[0])
                self.assertTrue(self.run_step({'run': 'cargo test --test actual_fixture'}, job, carry=carry)[0])
                later = self.calls[-1]['env']
                self.assertEqual(later.get('DATABASE_URL'), database_url.replace(':5432/', ':55491/') if database_url else None)
                self.assertEqual(later['ORGT_TEST_BASE_URL'], 'postgres://test:fixture@localhost:55491/test_database')
                self.assertEqual(job, before)
                self.assertEqual(carry, {'PREVIOUS_STEP': 'retained'})
                self.assertEqual(dict(os.environ), parent_environment)

    def test_unknown_service_binding_fails_before_process_or_temporary_files(self):
        cases = [(self.step(), {'services': {}}),
                 ({'run': self.step()['run']}, {'services': {'postgres': {}}}),
                 ({'run': self.step()['run'], 'env': {'POSTGRES_SERVICE_ID': '${{ job.services.other.id }}'}}, {'services': {'postgres': {}}})]
        for step, job in cases:
            with self.subTest(step=step, job=job), mock.patch.object(self.runner.tempfile, 'mkdtemp') as allocate:
                self.assertEqual(self.run_step(step, job), (False, 'unrecognized PostgreSQL role-preparation service binding', False))
                allocate.assert_not_called()
        self.assertEqual(self.calls, [])

    def test_unrelated_or_near_match_step_retains_original_script_and_environment(self):
        job = {'services': {'postgres': {}}, 'env': {'DATABASE_URL': 'postgres://reader:fixture@localhost:5432/test_database'}}
        for command in ['cargo test --test ordinary', self.step()['run'] + ' extra',
                        'bash scripts/prepare_ci_pg_roles.sh --container "$OTHER_SERVICE"']:
            with self.subTest(command=command):
                self.assertTrue(self.run_step({'run': command}, job)[0])
                self.assertEqual(self.calls[-1]['command'][-1], command)
                self.assertEqual(self.calls[-1]['env']['DATABASE_URL'], 'postgres://reader:fixture@localhost:55491/test_database')

    def test_role_helper_failure_is_not_converted_to_success(self):
        self.exit_code = 47
        self.assertEqual(self.run_step(self.step()), (False, 'exit 47', False))
        self.assertEqual(len(self.calls), 1)


if __name__ == '__main__':
    unittest.main()
