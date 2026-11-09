import importlib.util
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import textwrap
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location('deployment', ROOT / 'scripts/deployment.py')
d = importlib.util.module_from_spec(spec)
spec.loader.exec_module(d)
SHA = 'a' * 40
DIGEST = 'sha256:' + 'b' * 64
OTHER = 'sha256:' + 'c' * 64


def machine(state='started', digest=DIGEST, sha=SHA):
    return {'id': '185e206b0302d8', 'state': state, 'host_status': 'ok',
            'config': {'services': [{'internal_port': 8080}]},
            'image_ref': {'digest': digest, 'labels': {'GH_SHA': sha}}}


class ReleaseDecisions(unittest.TestCase):
    def test_stale_stopped_machine_cannot_be_hidden_by_healthy_new_machine(self):
        _, errors = d.observe_machines({'Machines': [machine(), machine('stopped', OTHER)]}, SHA, DIGEST)
        self.assertTrue(errors)

    def test_no_started_machine_and_wrong_source_fail(self):
        for machines in [[], [machine('stopped')], [machine(sha='d' * 40)]]:
            self.assertTrue(d.observe_machines({'Machines': machines}, SHA, DIGEST)[1])

    def test_correct_running_and_stopped_fleet_passes(self):
        self.assertEqual(d.observe_machines({'Machines': [machine(), machine('stopped')]}, SHA, DIGEST)[1], [])

    def test_verify_wakes_a_scale_to_zero_app_before_observing_its_machines(self):
        woken = []
        def get(url, token=None):
            woken.append(url)
            return 200, b'ok', 0.1
        def command(*args, **kwargs):
            return json.dumps({'Machines': [machine('started' if woken else 'stopped')]})
        with tempfile.TemporaryDirectory() as tmp, patch.object(d, 'get', get), patch.object(d, 'command', command), \
                patch.dict(os.environ, {'GITHUB_STEP_SUMMARY': ''}):
            receipt = Path(tmp) / 'verification.json'
            args = type('A', (), {'sha': SHA, 'app': 'coralswarm-backend-staging', 'digest': DIGEST,
                                  'candidate': None, 'receipt': str(receipt)})()
            try:
                d.verify(args)
            except SystemExit:
                pass
            self.assertEqual(json.loads(receipt.read_text())['errors'], [])

    def test_push_receipt_rejects_missing_ambiguous_or_other_app_digest(self):
        good = f'#10 pushing manifest for registry.fly.io/coralswarm-web:deployment-123@{DIGEST} 0.1s done\n'
        self.assertEqual(d.pushed_digest(good * 3, 'coralswarm-web'), DIGEST)
        for bad in ['', good + good.replace(DIGEST, OTHER), good.replace('coralswarm-web', 'other-app')]:
            with self.assertRaises(ValueError): d.pushed_digest(bad, 'coralswarm-web')

    def test_recovery_requires_main_completed_run_and_original_web_push(self):
        run = {'path': '.github/workflows/deploy.yml', 'head_branch': 'main', 'event': 'push', 'status': 'completed', 'head_sha': SHA}
        jobs = [{'name': 'Deploy web (Fly)', 'conclusion': 'failure'}]
        log = f'pushing manifest for registry.fly.io/coralswarm-web:deployment-123@{DIGEST}\n'
        self.assertEqual(d.admit_source(run, jobs, log, DIGEST), SHA)
        release = dict(run, event='release', head_branch='server-2026.10.28.1')
        self.assertEqual(d.admit_source(release, jobs, log, DIGEST), SHA)
        for key, value in [('head_branch', 'feature'), ('head_branch', 'main'), ('head_branch', 'server-latest')]:
            with self.assertRaises(ValueError): d.admit_source(dict(release, **{key: value}), jobs, log, DIGEST)
        with self.assertRaises(ValueError): d.admit_source(dict(run, head_branch='server-2026.10.28.1'), jobs, log, DIGEST)
        for key, value in [('head_branch', 'feature'), ('path', 'other.yml'), ('status', 'in_progress'), ('event', 'pull_request')]:
            with self.assertRaises(ValueError): d.admit_source(dict(run, **{key: value}), jobs, log, DIGEST)
        for other_jobs, other_digest in [([], DIGEST), (jobs, OTHER), ([{'name': 'Deploy web (Fly)', 'conclusion': 'skipped'}], DIGEST)]:
            with self.assertRaises(ValueError): d.admit_source(run, other_jobs, log, other_digest)

    @patch.dict(os.environ, {}, clear=True)
    def test_missing_auth_is_never_reported_as_passed(self):
        result, errors = d.application_smoke()
        self.assertEqual(result['status'], 'not_configured')
        self.assertEqual(errors, [])
        with patch.dict(os.environ, {'DEPLOY_REQUIRE_APP_SMOKE': 'true'}):
            self.assertTrue(d.application_smoke()[1])

    @patch.dict(os.environ, {'DEPLOY_SMOKE_TOKEN': 'test-only-secret', 'DEPLOY_SMOKE_GRAPH_ID': '11111111-1111-4111-8111-111111111111'}, clear=True)
    def test_auth_detects_real_failure_and_validates_shapes_without_retaining_data(self):
        with patch.object(d, 'get', return_value=(500, b'{"error":"internal error"}', 1)):
            result, errors = d.application_smoke()
            self.assertTrue(errors)
            self.assertNotIn('test-only-secret', json.dumps(result))
        with patch.object(d, 'get', side_effect=[(200, b'{"reef_total":1,"atom_count":2,"source_count":1}', .2), (200, b'[]', .1)]):
            self.assertEqual(d.application_smoke()[0]['status'], 'passed')
        with patch.object(d, 'get', side_effect=[(200, b'{"reef_total":1,"atom_count":2,"source_count":1}', .2), (200, b'{"nodes":[]}', .1)]):
            self.assertTrue(d.application_smoke()[1])

    def test_marketing_success_cannot_hide_skipped_backend_or_web(self):
        needs = {'gate': {'result': 'success', 'outputs': {'deploy_production': 'true', 'sha': SHA}},
                 'deploy-marketing': {'result': 'success'}, 'deploy-backend': {'result': 'skipped'},
                 'deploy-web': {'result': 'skipped'}}
        self.assertEqual(d.release_outcome(needs, False)['outcome'], 'partial_or_failed')
        self.assertEqual(d.release_outcome(needs, True)['outcome'], 'not_deployed')
        for name in ['deploy-backend', 'deploy-web']: needs[name]['result'] = 'success'
        self.assertEqual(d.release_outcome(needs, False)['outcome'], 'components_succeeded')

    def test_redirects_are_not_followed_with_credentials(self):
        self.assertIsNone(d.NoRedirect().redirect_request(None, None, 302, 'redirect', {}, 'https://other.invalid'))


class PreparedBackendWorkflow(unittest.TestCase):
    def test_generated_library_gate_is_required_before_release_upload(self):
        workflow = (ROOT / '.github/workflows/deploy.yml').read_text()
        builder = workflow.split('  build-backend-release:\n', 1)[1].split('  deploy-backend:\n', 1)[0]
        self.assertIn('--bin library_read_model_diff', builder)
        gate = builder.split('      - name: Validate generated Library verdicts\n', 1)[1].split('      - name:', 1)[0]
        self.assertIn('target/release/library_read_model_diff --export-sql', gate)
        self.assertIn('--source-sha "$GITHUB_SHA"', gate)
        self.assertNotIn('GITHUB_SHA=', gate)
        self.assertLess(builder.index('name: Validate generated Library verdicts'), builder.index('actions/upload-artifact'))

    def test_proof_cli_does_not_enter_the_runtime_bundle(self):
        workflow = (ROOT / '.github/workflows/deploy.yml').read_text()
        builder = workflow.split('  build-backend-release:\n', 1)[1].split('  deploy-backend:\n', 1)[0]
        build = builder.split('      - name: Build release binaries\n', 1)[1].split('      - name:', 1)[0]
        import re
        import textwrap
        binaries = re.findall(r'--bin ([a-z_0-9-]+)', build)
        assembly = builder.split('      - name: Assemble dist/\n', 1)[1].split('      # Every ELF', 1)[0].split('        run: |\n', 1)[1]
        with tempfile.TemporaryDirectory() as temporary:
            target = Path(temporary)
            (target / 'target/release').mkdir(parents=True)
            (target / 'backend').mkdir()
            for name in binaries:
                (target / 'target/release' / name).write_text('fixture binary')
            (target / 'backend/release.sh').write_text('#!/bin/sh\n')
            subprocess.run(['bash', '-eu', '-c', textwrap.dedent(assembly)], cwd=target, check=True)
            packaged = {path.name for path in (target / 'dist').iterdir()}
            required = set(re.findall(r'^COPY dist/([^*\s]+)', (ROOT / 'backend/Dockerfile.prebuilt').read_text(), re.M))
            self.assertTrue(required <= packaged, required - packaged)
            self.assertNotIn('library_read_model_diff', packaged)
            self.assertNotIn('python3', packaged)

    def test_production_prepared_release_keeps_failed_rollout_identity_and_verification(self):
        workflow = (ROOT / '.github/workflows/deploy.yml').read_text()
        backend = workflow.split('  deploy-backend:\n', 1)[1].split('  build-web-release:\n', 1)[0]
        self.assertIn('id: rollout', backend)
        self.assertIn('--candidate-receipt "$RUNNER_TEMP/release-backend/candidate.json"', backend)
        self.assertIn("if: ${{ always() && steps.rollout.outcome != 'skipped' }}", backend)
        self.assertIn('--candidate "$RUNNER_TEMP/release-backend/candidate.json"', backend)
        self.assertIn('name: release-backend-${{ github.run_id }}-${{ github.run_attempt }}', backend)
        self.assertNotIn('for attempt', backend)
        self.assertNotIn('fly_deploy_with_receipt.sh', backend)
        self.assertIn('--store-evidence "$STORE_EVIDENCE"', backend)

    def test_staging_backend_verifies_the_serving_source_before_smoke_and_promotion(self):
        workflow = (ROOT / '.github/workflows/deploy.yml').read_text()
        staging = workflow.split('  deploy-backend-staging:\n', 1)[1].split('  deploy-web-staging:\n', 1)[0]
        self.assertIn('--candidate-receipt "$RUNNER_TEMP/release-backend-staging/candidate.json"', staging)
        verify = staging.split('python3 scripts/deployment.py verify', 1)[1].split('      - name:', 1)[0]
        self.assertIn('--app coralswarm-backend-staging --sha "$EXPECTED_SHA"', verify)
        self.assertIn('--candidate "$RUNNER_TEMP/release-backend-staging/candidate.json"', verify)
        self.assertIn('coralswarm-backend-staging', d.PROBES)


class SummaryCLI(unittest.TestCase):
    def test_failed_summary_still_uploads_outcome_artifact(self):
        workflow = (ROOT / '.github/workflows/deploy.yml').read_text()
        summary_job = workflow.split('\n  release-summary:', 1)[1]
        upload = summary_job.split('      - uses: actions/upload-artifact@v4', 1)[1]
        self.assertIn('        if: ${{ always() }}', upload.split('        with:', 1)[0])
        self.assertIn('name: release-outcome-', upload)

    def test_exit_status_and_receipts_for_every_release_outcome(self):
        cases = [
            ('expected_web_skipped', 'success', 'true', 'skipped', False, 'partial_or_failed', 1),
            ('gate_failed', 'failure', 'true', 'skipped', False, 'failed_before_deploy', 1),
            ('all_succeeded', 'success', 'true', 'success', False, 'components_succeeded', 0),
            ('dry_run', 'success', 'true', 'skipped', True, 'not_deployed', 0),
            ('no_deploy_target', 'success', 'false', 'skipped', False, 'not_deployed', 0),
        ]
        for name, gate, production, web, dry, outcome, exit_code in cases:
            with self.subTest(case=name), tempfile.TemporaryDirectory() as tmp:
                tmp = Path(tmp)
                needs = {'gate': {'result': gate, 'outputs': {'sha': SHA, 'deploy_production': production}},
                         'deploy-backend': {'result': 'success'}, 'deploy-web': {'result': web},
                         'deploy-marketing': {'result': 'success'}}
                receipt, summary = tmp / 'outcome.json', tmp / 'summary.md'
                env = dict(os.environ, RELEASE_NEEDS=json.dumps(needs), RELEASE_DRY_RUN=str(dry).lower(),
                           GITHUB_STEP_SUMMARY=str(summary))
                result = subprocess.run([sys.executable, str(ROOT / 'scripts/deployment.py'), 'summary',
                                         '--receipt', str(receipt)], env=env, capture_output=True, text=True)
                self.assertEqual(result.returncode, exit_code, result.stderr)
                self.assertEqual(json.loads(receipt.read_text())['outcome'], outcome)
                self.assertIn(outcome, summary.read_text())


class ReleaseModeOutcomes(unittest.TestCase):
    def outcome(self, mode, results, **outputs):
        needs = {'gate': {'result': 'success', 'outputs': dict(sha=SHA, mode=mode, **outputs)}}
        needs.update({name: {'result': result} for name, result in results.items()})
        return d.release_outcome(needs, False)['outcome']

    def test_release_is_complete_only_when_staging_smoke_production_and_the_record_all_succeed(self):
        everything = ['deploy-backend-staging', 'deploy-web-staging', 'require-release-build', 'smoke-staging',
                      'deploy-backend', 'deploy-web', 'deploy-marketing', 'record-release']
        outputs = dict(deploy_production='true', deploy_staging='true', deploy_web_staging='true')
        self.assertEqual(self.outcome('release', {n: 'success' for n in everything}, **outputs), 'components_succeeded')
        for missing in everything:
            with self.subTest(missing=missing):
                results = {n: 'success' for n in everything}
                results[missing] = 'skipped'
                self.assertEqual(self.outcome('release', results, **outputs), 'partial_or_failed')

    def test_tag_build_and_rollback_and_noop_modes(self):
        self.assertEqual(self.outcome('build', {'build-backend-release': 'success'}, build='true'), 'components_succeeded')
        self.assertEqual(self.outcome('build', {'build-backend-release': 'failure'}, build='true'), 'partial_or_failed')
        both = {'rollback-backend': 'success', 'rollback-web': 'success'}
        self.assertEqual(self.outcome('rollback', both), 'components_succeeded')
        self.assertEqual(self.outcome('rollback', dict(both, **{'rollback-web': 'skipped'})), 'partial_or_failed')
        self.assertEqual(self.outcome('none', {}), 'not_deployed')


class DispatchConditions(unittest.TestCase):
    """Drives the ACTUAL `gate` script (with a stubbed `gh`) and evaluates the ACTUAL job `if:` expressions."""

    TAG = 'server-2026.10.28.1'

    @classmethod
    def setUpClass(cls):
        cls.workflow = (ROOT / '.github/workflows/deploy.yml').read_text()
        gate = cls.workflow.split('\n  gate:\n', 1)[1].split('\n  # Pre-deploy invariant guard', 1)[0]
        cls.gate_script = textwrap.dedent(gate.split('        run: |\n', 1)[1])

    def gate(self, event, compare='behind', **env):
        """Run the gate; returns (exit code, outputs)."""
        with tempfile.TemporaryDirectory() as tmp:
            tmp = Path(tmp)
            stub = tmp / 'gh'
            stub.write_text('#!/bin/bash\ncase "$*" in\n *"/commits/"*) echo "$STUB_SHA" ;;\n *"/compare/"*) echo "$STUB_COMPARE" ;;\n'
                            ' *"/releases/tags/"*) echo "$STUB_RELEASE" ;;\nesac\n')
            stub.chmod(0o755)
            output = tmp / 'output'
            output.touch()
            base = dict(os.environ, PATH=str(tmp) + os.pathsep + os.environ['PATH'], GITHUB_OUTPUT=str(output),
                        STUB_SHA=SHA, STUB_COMPARE=compare, STUB_RELEASE='false false', EVENT_NAME=event, EVENT_SHA='f' * 40,
                        GITHUB_REF_NAME_IN='main', GITHUB_REF_TYPE_IN='branch', RELEASE_TAG='',
                        RELEASE_PRERELEASE='false', RELEASE_DRAFT='false', DISPATCH_TARGET='staging',
                        DISPATCH_RELEASE='', DISPATCH_DRY_RUN='false', REPO='o/r', GH_TOKEN='x')
            base.update(env)
            result = subprocess.run(['bash', '-c', self.gate_script], env=base, capture_output=True, text=True)
            outputs = dict(line.split('=', 1) for line in output.read_text().splitlines())
            return result.returncode, outputs

    def release(self, tag=None, **env):
        return self.gate('release', RELEASE_TAG=tag or self.TAG, GITHUB_REF_NAME_IN=tag or self.TAG, GITHUB_REF_TYPE_IN='tag', **env)

    def test_only_server_tag_pushes_build_and_nothing_deploys(self):
        code, out = self.gate('push', GITHUB_REF_NAME_IN=self.TAG, GITHUB_REF_TYPE_IN='tag')
        self.assertEqual(code, 0)
        self.assertEqual((out['mode'], out['build'], out['deploy_production'], out['deploy_staging']), ('build', 'true', 'false', 'false'))
        self.assertEqual(out['sha'], SHA)
        self.assertNotEqual(self.gate('push', GITHUB_REF_NAME_IN='main')[0], 0)
        self.assertNotEqual(self.gate('push', GITHUB_REF_NAME_IN='server-latest', GITHUB_REF_TYPE_IN='tag')[0], 0)

    def test_release_stages_then_promotes_only_for_server_tags_on_main(self):
        code, out = self.release()
        self.assertEqual(code, 0)
        self.assertEqual((out['mode'], out['deploy_production'], out['deploy_staging'], out['deploy_web_staging'], out['build']),
                         ('release', 'true', 'true', 'true', 'false'))
        self.assertEqual(out['sha'], SHA, 'the sha comes from the tag, not the event')
        code, out = self.release('desktop-v1.2.3')
        self.assertEqual(code, 0)
        self.assertEqual({out['mode'], out['deploy_production'], out['deploy_staging'], out['build'], out['rollback']}, {'none', 'false'})
        for bad in [dict(RELEASE_PRERELEASE='true'), dict(RELEASE_DRAFT='true'), dict(compare='ahead'), dict(compare='diverged')]:
            with self.subTest(bad=bad):
                self.assertNotEqual(self.release(**bad)[0], 0)
        self.assertNotEqual(self.release('server-2026.10.28')[0], 0)
        self.assertNotEqual(self.release('server-latest')[0], 0)

    def test_dispatch_is_staging_or_rollback_never_production(self):
        for branch, web in [('main', 'false'), ('feature', 'false'), ('develop', 'true')]:
            code, out = self.gate('workflow_dispatch', GITHUB_REF_NAME_IN=branch)
            self.assertEqual(code, 0)
            self.assertEqual((out['mode'], out['deploy_production'], out['deploy_staging'], out['deploy_web_staging'], out['build']),
                             ('staging', 'false', 'true', web, 'true'), branch)
        self.assertNotEqual(self.gate('workflow_dispatch', DISPATCH_TARGET='production')[0], 0)
        self.assertNotEqual(self.gate('workflow_dispatch', DISPATCH_TARGET='both')[0], 0)
        code, out = self.gate('workflow_dispatch', DISPATCH_RELEASE=self.TAG)
        self.assertEqual(code, 0)
        self.assertEqual((out['mode'], out['rollback'], out['deploy_production'], out['deploy_staging'], out['build']),
                         ('rollback', 'true', 'false', 'false', 'false'))
        for bad in [dict(GITHUB_REF_NAME_IN='feature'), dict(DISPATCH_DRY_RUN='true'), dict(compare='ahead'), dict(DISPATCH_RELEASE='main'),
                    dict(STUB_RELEASE='false true'), dict(STUB_RELEASE='true false'), dict(STUB_RELEASE='missing')]:
            with self.subTest(bad=bad):
                self.assertNotEqual(self.gate('workflow_dispatch', DISPATCH_RELEASE=bad.pop('DISPATCH_RELEASE', self.TAG), **bad)[0], 0)

    def on_tag(self, target, **env):
        return self.gate('workflow_dispatch', DISPATCH_TARGET=target, GITHUB_REF_NAME_IN=self.TAG, GITHUB_REF_TYPE_IN='tag', **env)

    def test_release_workflow_dispatches_build_then_deploy_on_the_tag(self):
        code, out = self.on_tag('release-build')
        self.assertEqual(code, 0)
        self.assertEqual((out['mode'], out['build'], out['deploy_production'], out['deploy_staging'], out['tag'], out['sha']),
                         ('build', 'true', 'false', 'false', self.TAG, SHA))
        code, out = self.on_tag('release')
        self.assertEqual(code, 0)
        self.assertEqual(out, self.release()[1], 'a dispatched release is the same mode as a published one')

    def test_tag_targets_refuse_a_branch_a_non_release_tag_or_an_unfinished_release(self):
        for target in ['release-build', 'release']:
            with self.subTest(target=target):
                self.assertNotEqual(self.gate('workflow_dispatch', DISPATCH_TARGET=target)[0], 0)
                self.assertNotEqual(self.gate('workflow_dispatch', DISPATCH_TARGET=target, GITHUB_REF_NAME_IN='server-latest', GITHUB_REF_TYPE_IN='tag')[0], 0)
                self.assertNotEqual(self.on_tag(target, compare='diverged')[0], 0)
        for state in ['false true', 'true false', 'missing']:
            with self.subTest(state=state):
                self.assertNotEqual(self.on_tag('release', STUB_RELEASE=state)[0], 0, 'a pre-release never deploys')
                self.assertEqual(self.on_tag('release-build', STUB_RELEASE=state)[0], 0, 'the build precedes publication')
        self.assertNotEqual(self.on_tag('release', DISPATCH_DRY_RUN='true')[0], 0)

    def test_triggers_are_the_release_flow(self):
        head = self.workflow.split('\npermissions:', 1)[0]
        self.assertNotIn('branches:', head, 'a merge to main must not deploy')
        self.assertIn("tags: ['server-*']", head)
        self.assertIn('types: [published]', head)
        self.assertIn('release:\n', head)

    STATUS_FUNCTIONS = ('!cancelled()', 'always()', 'failure()', 'success()')

    def job_info(self, job):
        """(direct needs, `if:` expression or None, has an explicit status function) for one job."""
        match = re.search(r'\n  ' + re.escape(job) + r':\n(.*?)(?=\n  [a-z][a-z-]*:\n|\Z)', self.workflow, re.S)
        head = match.group(1).split('\n    steps:', 1)[0]
        needs = re.search(r'\n    needs: (\[[^\]]*\]|\S+)', head)
        needs = [n.strip() for n in needs.group(1).strip('[]').split(',')] if needs else []
        block = re.search(r'\n    if: >-\n((?:      [^\n]*\n?)+)', head + '\n')
        if block:
            expression = block.group(1)
        else:
            inline = re.search(r'\n    if: ([^\n]+)', head)
            expression = inline.group(1) if inline else None
        if expression is not None:
            expression = re.sub(r'\n\s*#[^\n]*', '', expression).strip()
        return needs, expression, bool(expression) and any(f in expression for f in self.STATUS_FUNCTIONS)

    def ancestors(self, job):
        found = set()
        for need in self.job_info(job)[0]:
            found |= {need} | self.ancestors(need)
        return found

    def runs(self, job, outputs, dry, result_of):
        """GitHub semantics: with no status function in `if:` an implicit success() applies, and it
        skips the job when ANY ancestor (not just a direct need) was skipped or failed."""
        needs, expression, explicit = self.job_info(job)
        if not explicit and any(result_of(a) != 'success' for a in self.ancestors(job)):
            return False
        if expression is None:
            return True
        expression = re.sub(r'^\$\{\{\s*(.*?)\s*\}\}$', lambda m: m.group(1), expression)
        expression = expression.replace('!cancelled()', 'True').replace('always()', 'True')
        expression = re.sub(r'needs\.gate\.outputs\.(\w+)', lambda m: repr(outputs[m.group(1)]), expression)
        expression = re.sub(r'needs\.([a-z-]+)\.result', lambda m: repr(result_of(m.group(1))), expression)
        expression = expression.replace('inputs.dry_run', repr(dry))
        expression = expression.replace('&&', ' and ').replace('||', ' or ').replace('true', 'True')
        return bool(eval(' '.join(expression.split()), {'__builtins__': {}}, {}))

    def allows(self, job, event_outputs, dry=False, **results):
        """Would `job` run if its needs finished with these results (default: success)?"""
        given = {k.replace('_', '-'): v for k, v in results.items()}
        defaults = {'build-backend-release': 'skipped', 'require-release-build': 'skipped', 'smoke-staging': 'skipped',
                    'prepare-backend-staging': 'skipped'}
        return self.runs(job, event_outputs, dry, lambda n: 'success' if n == 'gate' else given.get(n, defaults.get(n, 'success')))

    def simulate(self, event_outputs, dry=False, fail=()):
        """Walk the whole job graph; a job that runs finishes `success` unless listed in `fail`."""
        names = re.findall(r'\n  ([a-z][a-z-]*):\n    (?:name|outputs|needs|if|runs-on):', self.workflow.split('\njobs:\n', 1)[1])
        memo = {}

        def result_of(name):
            if name == 'gate':
                return 'success'
            if name not in memo:
                if not self.runs(name, event_outputs, dry, result_of):
                    memo[name] = 'skipped'
                else:
                    memo[name] = 'failure' if name in fail else 'success'
            return memo[name]
        return {name: result_of(name) for name in names}

    def outputs(self, mode):
        if mode == 'release':
            return self.release()[1]
        if mode == 'build':
            return self.gate('push', GITHUB_REF_NAME_IN=self.TAG, GITHUB_REF_TYPE_IN='tag')[1]
        if mode == 'rollback':
            return self.gate('workflow_dispatch', DISPATCH_RELEASE=self.TAG)[1]
        if mode == 'staging':
            return self.gate('workflow_dispatch', GITHUB_REF_NAME_IN='develop')[1]
        return self.release('desktop-v1.0.0')[1]

    def test_production_jobs_need_a_release_and_a_passing_staging_smoke(self):
        for job in ['deploy-backend', 'deploy-web', 'deploy-marketing']:
            for mode in ['build', 'release', 'rollback', 'staging', 'none']:
                for smoke in ['success', 'failure', 'skipped', 'cancelled']:
                    for dry in [False, True]:
                        with self.subTest(job=job, mode=mode, smoke=smoke, dry=dry):
                            results = {'smoke_staging': smoke, 'require_release_build': 'success' if mode == 'release' else 'skipped'}
                            # deploy-web runs behind deploy-backend, which is what carries the smoke gate to it.
                            results['deploy_backend'] = 'success' if mode == 'release' and smoke == 'success' and not dry else 'skipped'
                            expected = mode == 'release' and smoke == 'success' and not dry
                            self.assertEqual(self.allows(job, self.outputs(mode), dry, **results), expected)

    def test_production_needs_the_tag_build_the_guards_and_a_deployed_backend(self):
        release = self.outputs('release')
        ok = {'smoke_staging': 'success', 'require_release_build': 'success'}
        self.assertTrue(self.allows('deploy-backend', release, **ok))
        for key in ['require_release_build', 'smoke_staging']:
            self.assertFalse(self.allows('deploy-backend', release, **dict(ok, **{key: 'failure'})))
        for key in ['pre_deploy_invariant_guard', 'pre_deploy_definer_guard']:
            for result in ['failure', 'skipped', 'cancelled']:
                self.assertFalse(self.allows('deploy-backend', release, **dict(ok, **{key: result})))
        for result in ['failure', 'skipped']:
            self.assertFalse(self.allows('deploy-web', release, **dict(ok, deploy_backend=result)))
            self.assertFalse(self.allows('deploy-web', release, **dict(ok, build_web_release=result)))

    def test_backend_ships_only_after_the_definer_guard_passes(self):
        for job in ['build-backend-release', 'deploy-backend', 'deploy-backend-staging']:
            body = self.workflow.split('\n  ' + job + ':', 1)[1].split('\n    runs-on:', 1)[0]
            self.assertIn('pre-deploy-definer-guard', body.split('    needs:', 1)[1].split('\n', 1)[0])
            self.assertIn("needs.pre-deploy-definer-guard.result == 'success' &&", body)
        for result in ['failure', 'skipped', 'cancelled']:
            with self.subTest(result=result):
                self.assertFalse(self.allows('deploy-backend', self.outputs('release'), smoke_staging='success',
                                             require_release_build='success', pre_deploy_definer_guard=result))
                self.assertFalse(self.allows('build-backend-release', self.outputs('build'), pre_deploy_definer_guard=result))
                self.assertFalse(self.allows('deploy-backend-staging', self.outputs('release'), require_release_build='success',
                                             pre_deploy_definer_guard=result))

    def test_release_build_runs_on_tag_push_and_staging_dispatch_but_not_on_publish(self):
        for mode, expected in [('build', True), ('staging', True), ('release', False), ('rollback', False), ('none', False)]:
            with self.subTest(mode=mode):
                self.assertEqual(self.allows('build-backend-release', self.outputs(mode)), expected)

    def test_staging_deploys_the_tag_build_or_the_dispatch_build_and_never_in_rollback(self):
        release, staging = self.outputs('release'), self.outputs('staging')
        self.assertTrue(self.allows('deploy-backend-staging', release, require_release_build='success'))
        self.assertFalse(self.allows('deploy-backend-staging', release, require_release_build='failure'))
        self.assertTrue(self.allows('prepare-backend-staging', staging, build_backend_release='success'))
        self.assertFalse(self.allows('prepare-backend-staging', staging, build_backend_release='failure'))
        self.assertFalse(self.allows('prepare-backend-staging', staging, True, build_backend_release='success'))
        self.assertTrue(self.allows('deploy-backend-staging', staging, prepare_backend_staging='success'))
        self.assertFalse(self.allows('deploy-backend-staging', staging, prepare_backend_staging='failure'))
        self.assertFalse(self.allows('deploy-backend-staging', staging, True, prepare_backend_staging='success'))
        for mode in ['release', 'rollback', 'build', 'none']:
            self.assertFalse(self.allows('prepare-backend-staging', self.outputs(mode), build_backend_release='success'))
        for mode in ['rollback', 'build', 'none']:
            self.assertFalse(self.allows('deploy-backend-staging', self.outputs(mode), require_release_build='success', prepare_backend_staging='success'))
        self.assertTrue(self.allows('deploy-web-staging', release))
        self.assertTrue(self.allows('deploy-web-staging', staging))
        self.assertFalse(self.allows('deploy-web-staging', self.gate('workflow_dispatch', GITHUB_REF_NAME_IN='feature')[1]))
        self.assertFalse(self.allows('deploy-web-staging', release, deploy_backend_staging='failure'))

    def test_rollback_reruns_no_build_guard_or_staging(self):
        rollback = self.outputs('rollback')
        for job in ['rollback-backend']:
            self.assertTrue(self.allows(job, rollback))
        self.assertTrue(self.allows('rollback-web', rollback, rollback_backend='success'))
        self.assertFalse(self.allows('rollback-web', rollback, rollback_backend='failure'))
        for mode in ['release', 'build', 'staging', 'none']:
            self.assertFalse(self.allows('rollback-backend', self.outputs(mode)))
        for job in ['rollback-backend', 'rollback-web']:
            body = self.workflow.split('\n  ' + job + ':', 1)[1].split('\n  # ', 1)[0].split('\n  release-summary:', 1)[0]
            self.assertNotIn('pre-deploy', body.split('    steps:', 1)[0])
            self.assertNotIn('--remote-only', body)
            self.assertNotIn('cargo', body)
            self.assertNotIn('npm run build', body)
        self.assertIn('--image "registry.fly.io/coralswarm-web@$WEB_DIGEST"', self.workflow)

    def test_release_chain_runs_end_to_end_despite_the_skipped_build_job(self):
        # Regression: build-backend-release is skipped in release mode. Every job downstream of it must
        # survive GitHub's implicit success() (which looks at ALL ancestors) or production never promotes.
        ran = self.simulate(self.outputs('release'))
        self.assertEqual(ran['build-backend-release'], 'skipped')
        for job in ['pre-deploy-invariant-guard', 'pre-deploy-definer-guard', 'require-release-build', 'deploy-backend-staging',
                    'deploy-web-staging', 'smoke-staging', 'deploy-backend', 'build-web-release', 'deploy-web',
                    'deploy-marketing', 'record-release']:
            self.assertEqual(ran[job], 'success', job)
        for job in ['rollback-backend', 'rollback-web']:
            self.assertEqual(ran[job], 'skipped', job)

    def test_failures_stop_the_chain_at_the_right_place(self):
        release = self.outputs('release')
        for failing, blocked in [('deploy-backend-staging', ['deploy-web-staging', 'smoke-staging', 'deploy-backend', 'deploy-web', 'deploy-marketing', 'record-release']),
                                 ('deploy-web-staging', ['smoke-staging', 'deploy-backend', 'deploy-web', 'deploy-marketing', 'record-release']),
                                 ('smoke-staging', ['deploy-backend', 'deploy-web', 'deploy-marketing', 'record-release']),
                                 ('require-release-build', ['deploy-backend-staging', 'deploy-backend', 'deploy-web']),
                                 ('pre-deploy-definer-guard', ['deploy-backend-staging', 'deploy-backend', 'deploy-web'])]:
            with self.subTest(failing=failing):
                ran = self.simulate(release, fail={failing})
                for job in blocked:
                    self.assertEqual(ran[job], 'skipped', job)
        ran = self.simulate(release, fail={'deploy-backend'})
        self.assertEqual((ran['deploy-web'], ran['record-release']), ('skipped', 'skipped'))
        ran = self.simulate(release, fail={'deploy-marketing'})
        self.assertEqual((ran['deploy-web'], ran['record-release']), ('success', 'skipped'))

    def test_other_modes_walk_only_their_own_chain(self):
        build = self.simulate(self.outputs('build'))
        self.assertEqual({j for j, r in build.items() if r == 'success'} - {'gate'},
                         {'pre-deploy-invariant-guard', 'pre-deploy-definer-guard', 'build-backend-release', 'release-summary'})
        self.assertEqual(build['deploy-backend'], 'skipped')
        staging = self.simulate(self.outputs('staging'))
        self.assertEqual((staging['build-backend-release'], staging['prepare-backend-staging'], staging['deploy-backend-staging'],
                          staging['deploy-web-staging']), ('success',) * 4)
        for job in ['deploy-backend', 'deploy-web', 'deploy-marketing', 'smoke-staging', 'record-release']:
            self.assertEqual(staging[job], 'skipped', job)
        rollback = self.simulate(self.outputs('rollback'))
        self.assertEqual((rollback['rollback-backend'], rollback['rollback-web']), ('success', 'success'))
        for job in ['build-backend-release', 'deploy-backend-staging', 'deploy-backend', 'smoke-staging', 'pre-deploy-invariant-guard']:
            self.assertEqual(rollback[job], 'skipped', job)
        for job, result in self.simulate(self.outputs('none')).items():
            if job not in ('release-summary', 'gate'):
                self.assertEqual(result, 'skipped', job)
        self.assertEqual(self.simulate(self.outputs('release'), dry=True)['deploy-backend'], 'skipped')

    def test_everything_that_reaches_production_shares_one_concurrency_group(self):
        group = self.workflow.split('\nconcurrency:\n', 1)[1].split('\n', 1)[0]
        self.assertIn("'deploy-production'", group)
        self.assertIn("github.event_name == 'release'", group)
        self.assertIn("inputs.release != ''", group)
        self.assertIn("inputs.target == 'release'", group)

    def test_release_deploys_bind_to_the_evidence_recorded_on_the_release(self):
        for job, side in [('deploy-backend', 'production'), ('deploy-backend-staging', 'staging'), ('rollback-backend', 'production')]:
            body = self.workflow.split('\n  ' + job + ':', 1)[1].split('\n  # ', 1)[0]
            self.assertIn(f'python3 scripts/release_evidence.py select --tag "$TAG" --sha "$RELEASE_SHA"\n          --side {side}', body)
        self.assertNotIn('vars.FINANCIAL_', self.workflow, 'no mutable repository variable selects what ships')

    def test_contract_guard_gets_every_tag(self):
        guard = self.workflow.split('\n  pre-deploy-invariant-guard:', 1)[1].split('\n  # Pre-deploy definer', 1)[0]
        self.assertIn('fetch-depth: 0', guard)
        self.assertIn('scripts/check_migration_contract.sh --self-test', guard)

    def test_staging_smoke_is_the_real_gate(self):
        smoke = self.workflow.split('\n  smoke-staging:', 1)[1].split('\n  record-release:', 1)[0]
        self.assertIn('npx playwright test --grep @smoke', smoke)
        self.assertIn('if [ ! -f e2e/package.json ]', smoke)
        self.assertIn('coralswarm-web-staging.example', smoke)
        self.assertTrue(self.allows('smoke-staging', self.outputs('release')))
        self.assertFalse(self.allows('smoke-staging', self.outputs('release'), deploy_web_staging='failure'))
        self.assertFalse(self.allows('smoke-staging', self.outputs('staging')))

    def test_retained_financial_image_gates_every_backend_deploy(self):
        for job in ['deploy-backend', 'deploy-backend-staging', 'rollback-backend']:
            body = self.workflow.split('\n  ' + job + ':', 1)[1].split('\n  # ', 1)[0]
            self.assertIn('python3 scripts/deploy_financial_backend.py deploy', body)
            for flag in ['--receipt', '--store-evidence', '--capability', '--expected-sha256', '--expected-source', '--prepare-run-id']:
                self.assertIn(flag, body)
            for removed in ['--operator-evidence', '--ci-readback', 'FINANCIAL_', 'financial-recovery-prepare.yml']:
                self.assertNotIn(removed, body)
            self.assertNotIn('flyctl deploy --config', body)

    def test_the_gate_job_names_match_what_deploy_admission_checks(self):
        spec = importlib.util.spec_from_file_location('financial', ROOT / 'scripts/deploy_financial_backend.py')
        financial = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(financial)
        for workflow, job_id in [('.github/workflows/release.yml', 'verify'), ('.github/workflows/deploy.yml', 'prepare-backend-staging')]:
            text = (ROOT / workflow).read_text()
            self.assertIn(f'\n  {job_id}:\n    name: {financial.STORE_GATES[workflow][0]}\n', text, workflow)
        release = (ROOT / '.github/workflows/release.yml').read_text()
        self.assertIn('needs: [plan, record]', release.split('\n  verify:\n', 1)[1].split('\n  publish:\n', 1)[0])
        self.assertEqual(financial.STORE_GATES['.github/workflows/deploy.yml'][2], {'coralswarm-backend-staging'})

    def test_release_web_digest_is_a_new_asset_not_an_edit_of_the_verified_record(self):
        record = self.workflow.split('\n  record-release:', 1)[1].split('\n  # ---- Rollback', 1)[0]
        self.assertIn('web-image.json', record)
        self.assertNotIn('--pattern release-images.json', record)
        self.assertIn('--pattern web-image.json', self.workflow.split('\n  rollback-web:', 1)[1])

    def test_recovery_cannot_build_redeploy_backend_or_create_machines(self):
        source = (ROOT / '.github/workflows/recover-web.yml').read_text()
        self.assertIn("if: github.ref == 'refs/heads/main'", source)
        self.assertIn('group: deploy-main', source)
        rollout = source.split('      - name: Roll out', 1)[1].split('      - name:', 1)[0]
        for flag in ['--image', '--only-machines', '--update-only', '--ha=false', '--deploy-retries 0']:
            self.assertIn(flag, rollout)
        self.assertNotIn('--remote-only', rollout)
        self.assertNotIn('coralswarm-backend', rollout)


class ReleaseWorkflow(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.workflow = (ROOT / '.github/workflows/release.yml').read_text()

    def job(self, name):
        return re.search(r'\n  ' + re.escape(name) + r':\n(.*?)(?=\n  [a-z][a-z-]*:\n|\Z)', self.workflow, re.S).group(1)

    def test_the_verifier_is_read_only_and_holds_no_write_credential(self):
        verify = self.job('verify')
        self.assertIn('    permissions:\n      contents: read\n    steps:', verify)
        self.assertIn('RELEASE_EVIDENCE_R2_READ_', verify)
        self.assertNotIn('WRITE', verify)
        self.assertNotIn('FLY_API_TOKEN', verify)
        self.assertIn('FLY_REGISTRY_READ_TOKEN', verify)

    def test_publication_and_deploy_follow_only_the_verifier(self):
        publish = self.job('publish')
        self.assertIn('needs: [plan, verify]', publish)
        self.assertIn('--prerelease=false', publish)
        self.assertIn('-f target=release', publish)
        self.assertIn('--prerelease', self.job('tag'))
        self.assertIn('needs: [plan, record]', self.job('verify'))

    def test_a_dry_run_tags_builds_and_prepares_nothing(self):
        self.assertIn("if: ${{ !inputs.dry_run }}", self.job('tag'))
        for name in ['prepare', 'record', 'verify', 'publish']:
            self.assertIn('needs: [plan, ', self.job(name))
            # The skipped `tag` must cascade: an explicit status function would break that.
            self.assertNotRegex(self.job(name).split('\n    steps:', 1)[0], r'always\(\)|cancelled\(\)|failure\(\)|\n    if:')
        self.assertIn('needs: [plan, tag', self.job('prepare'))

    def test_only_main_cuts_a_release_and_the_contract_check_runs(self):
        plan = self.job('plan')
        self.assertIn("github.ref != 'refs/heads/main' && !inputs.dry_run", plan)
        self.assertIn('scripts/check_migration_contract.sh', plan)
        self.assertIn('scripts/release_plan.py', plan)

    def test_no_repository_variable_is_written_or_read(self):
        self.assertNotIn('gh variable', self.workflow)
        self.assertNotIn('vars.', self.workflow)


class RetryBoundary(unittest.TestCase):
    def test_vm_failure_is_not_rebuilt_and_keeps_candidate(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp = Path(tmp)
            fly = tmp / 'flyctl'
            fly.write_text('#!/bin/bash\necho called >> "$CALLS"\necho "pushing manifest for registry.fly.io/coralswarm-web:deployment-123@' + DIGEST + '"\necho "failed to update machine: bolt token: verification failed"\nexit 1\n')
            fly.chmod(0o755)
            env = dict(os.environ, PATH=str(tmp) + os.pathsep + os.environ['PATH'], CALLS=str(tmp / 'calls'))
            result = subprocess.run(['bash', str(ROOT / 'scripts/fly_deploy_with_receipt.sh'), 'coralswarm-web', str(tmp / 'receipt'), '--config', 'web/fly.toml'], env=env, capture_output=True, text=True)
            self.assertNotEqual(result.returncode, 0)
            self.assertEqual((tmp / 'calls').read_text().splitlines(), ['called'])
            self.assertEqual(json.loads((tmp / 'receipt/candidate.json').read_text())['digest'], DIGEST)


if __name__ == '__main__':
    unittest.main()
