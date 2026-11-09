#!/usr/bin/env python3
"""Selected workflow Node bindings, using stdlib-only filesystem/process boundaries."""
import ast
import contextlib
import importlib.util
import io
import json
import os
from pathlib import Path
import sys
import tempfile
import threading
import types
import unittest
from unittest import mock

ROOT = Path(__file__).resolve().parent.parent
SOURCE = '3cb80cf55f804743197298c374784ab11d038468'


def job(major='20', command='npm run test'):
    return {'steps': [{'uses': 'actions/checkout@v4'},
                      {'uses': 'actions/setup-node@v4', 'with': {'node-version': major}},
                      {'name': 'native script', 'run': command}]}


class NodeBindings(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        spec = importlib.util.spec_from_file_location('ci_node_tests', ROOT / 'scripts/ci_local.py')
        cls.runner = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = cls.runner
        with mock.patch.object(sys, 'dont_write_bytecode', True), mock.patch.dict(sys.modules, {'yaml': types.ModuleType('yaml')}):
            spec.loader.exec_module(cls.runner)

    def setUp(self):
        tmp = tempfile.TemporaryDirectory(prefix='ci-node-test-')
        self.addCleanup(tmp.cleanup)
        self.root = Path(tmp.name)
        self.addCleanup(mock.patch.stopall)
        mock.patch.dict(os.environ, {'PATH': '/ambient/bin'}, clear=True).start()
        self.probes = []
        mock.patch.object(self.runner.subprocess, 'run', side_effect=self.probe).start()
        mock.patch.object(self.runner.shutil, 'which', return_value=None).start()

    def runtime(self, major, name=None):
        directory = self.root / (name or f'node {major}')
        binpath = directory / 'bin'; binpath.mkdir(parents=True)
        library = directory / 'lib/node_modules/npm'; (library / 'bin').mkdir(parents=True)
        (library / 'package.json').write_text(json.dumps({'version': '10.9.8'}))
        for name in ['node']:
            (binpath / name).write_text(str(major)); (binpath / name).chmod(0o700)
        for name in ['npm', 'npx']:
            target = library / 'bin' / (name + '-cli.js'); target.write_text(name); target.chmod(0o700)
            (binpath / name).symlink_to(target)
        return binpath.resolve()

    def probe(self, args, **kwargs):
        self.probes.append((args, kwargs))
        self.assertGreater(kwargs['timeout'], 0); self.assertLessEqual(kwargs['timeout'], 10)
        self.assertNotIn('NODE_OPTIONS', kwargs['env']); self.assertNotIn('NODE_PATH', kwargs['env'])
        self.assertNotIn('NPM_TOKEN', kwargs['env'])
        if args[1] == '-e':
            value = {'version': 'v' + Path(args[0]).read_text() + '.1.2', 'execPath': args[0], 'platform': 'darwin', 'arch': 'arm64'}
            return types.SimpleNamespace(returncode=0, stdout=json.dumps(value), stderr='')
        self.assertIn(args[1], [str(Path(args[0]).parent.parent / f'lib/node_modules/npm/bin/{name}-cli.js') for name in ['npm', 'npx']])
        self.assertEqual(args[2:], ['--version'])
        return types.SimpleNamespace(returncode=0, stdout='10.9.8\n', stderr='')

    def bind(self, jobs, pins=()):
        return self.runner.resolve_node_bindings(jobs, list(pins), SOURCE)

    def test_explicit_pairs_and_five_job_inventory(self):
        expected = {'gmail-calendar-actions': 20, 'web': 20, 'marketing': 20, 'mcp-e2e': 20, 'widgets': 22}
        # ci.yml no longer carries setup-node jobs (CI redesign, PR 4); the
        # binding logic is still exercised on this fixed inventory.
        actual = dict(expected)
        bins = {m: self.runtime(m) for m in [20, 22]}
        bindings = self.bind({k: job(str(v)) for k, v in actual.items()}, [f'{m}={p}' for m, p in bins.items()])
        self.assertEqual(len(self.probes), 6, 'resolve each major once, not each job')
        self.assertEqual({k: v.runtime.major for k, v in bindings.items()}, expected)
        with self.assertRaises(TypeError): bindings['other'] = None
        with self.assertRaises(AttributeError): bindings['web'].runtime.major = 22
        provenance = bindings['web'].provenance()
        self.assertEqual(provenance['source_sha'], SOURCE)
        self.assertEqual(provenance['action_ordinal'], 2)
        self.assertEqual(provenance['declaration'], '20')
        self.assertEqual(len(provenance['hashes']), 4)
        self.assertNotIn('PATH', json.dumps(provenance))

    def test_ambient_exact_major_only_and_explicit_bad_never_falls_back(self):
        twenty = self.runtime(20)
        self.runner.shutil.which.return_value = str(twenty / 'node')
        self.assertEqual(self.bind({'web': job()})['web'].runtime.major, 20)
        with self.assertRaises(self.runner.NodeBindingError):
            self.bind({'web': job()}, ['20=/missing/bin'])
        wrong = self.runtime(26)
        self.runner.shutil.which.return_value = str(wrong / 'node')
        with self.assertRaises(self.runner.NodeBindingError):
            self.bind({'web': job()})
        self.runner.shutil.which.return_value = None
        with self.assertRaises(self.runner.NodeBindingError):
            self.bind({'web': job()})

    def test_no_selected_node_requires_no_probe_or_pin_validation(self):
        self.assertEqual(dict(self.bind({'rust': {'steps': [{'run': 'cargo test'}]}}, ['24=/not-installed/bin'])), {})
        self.assertEqual(self.probes, [])

    def test_duplicate_relative_malformed_and_unsupported_setup_fail_before_probes(self):
        valid = self.runtime(20)
        for pins in [['20=' + str(valid), '20=' + str(valid)], ['20=relative'], ['twenty=/bin'], ['20']]:
            with self.subTest(pins=pins), self.assertRaises(self.runner.NodeBindingError): self.bind({'web': job()}, pins)
        for change in [{'if': 'true'}, {'uses': 'actions/setup-node@v3'}, {'with': {'node-version-file': '.nvmrc'}},
                       {'with': {'node-version': '20.x'}}, {'with': {'node-version': '${{ env.NODE }}'}}]:
            selected = job(); selected['steps'][1].update(change)
            with self.subTest(change=change), self.assertRaises(self.runner.NodeBindingError): self.bind({'web': selected}, ['20=' + str(valid)])
        selected = job(); selected['steps'].insert(0, {'run': 'npm ci'})
        with self.assertRaises(self.runner.NodeBindingError): self.bind({'web': selected}, ['20=' + str(valid)])
        selected = job(); selected['steps'].insert(1, selected['steps'][1].copy())
        with self.assertRaises(self.runner.NodeBindingError): self.bind({'web': selected}, ['20=' + str(valid)])
        self.assertEqual(self.probes, [])

    def test_control_names_only_refuse_before_probes_and_empty_controls_are_allowed(self):
        valid = self.runtime(20)
        for name in ['NODE_OPTIONS', 'NODE_PATH']:
            for source in ['ambient', 'job', 'step']:
                selected = job()
                with mock.patch.dict(os.environ):
                    if source == 'ambient': os.environ[name] = 'SENSITIVE_DO_NOT_PRINT'
                    elif source == 'job': selected['env'] = {name: 'SENSITIVE_DO_NOT_PRINT'}
                    else: selected['steps'][-1]['env'] = {name: 'SENSITIVE_DO_NOT_PRINT'}
                    with self.subTest(name=name, source=source), self.assertRaises(self.runner.NodeBindingError) as error:
                        self.bind({'web': selected}, ['20=' + str(valid)])
                    self.assertIn(name, str(error.exception)); self.assertNotIn('SENSITIVE', str(error.exception))
        self.assertEqual(self.probes, [])
        with mock.patch.dict(os.environ, {'NODE_OPTIONS': '', 'NODE_PATH': ''}):
            self.assertIn('web', self.bind({'web': job()}, ['20=' + str(valid)]))

    def test_missing_mixed_or_dangling_sibling_pair_and_version_mismatch_fail(self):
        for kind in ['missing', 'dangling', 'mixed', 'package-version']:
            directory = self.runtime(20, kind)
            npm = directory / 'npm'
            if kind == 'package-version': (directory.parent / 'lib/node_modules/npm/package.json').write_text('{"version":"9.0.0"}')
            else:
                npm.unlink()
                if kind == 'dangling': npm.symlink_to(directory / 'missing')
                if kind == 'mixed': npm.symlink_to(self.runtime(22, 'foreign') / 'npm')
            with self.subTest(kind=kind), self.assertRaises(self.runner.NodeBindingError):
                self.bind({'web': job()}, ['20=' + str(directory)])

    def test_probe_errors_timeouts_and_malformed_identity_are_safe(self):
        valid = self.runtime(20)
        for effect in [self.runner.subprocess.TimeoutExpired('PROBE_SECRET', 10), OSError('PROBE_SECRET'),
                       types.SimpleNamespace(returncode=1, stdout='', stderr='PRIVATE_TOKEN'),
                       types.SimpleNamespace(returncode=0, stdout='bad identity PRIVATE_TOKEN', stderr='')]:
            with self.subTest(effect=type(effect).__name__), mock.patch.object(self.runner.subprocess, 'run') as probe:
                if isinstance(effect, BaseException): probe.side_effect = effect
                else: probe.return_value = effect
                with self.assertRaises(self.runner.NodeBindingError) as error:
                    self.bind({'web': job()}, ['20=' + str(valid)])
                self.assertNotIn('PRIVATE_TOKEN', str(error.exception)); self.assertNotIn('PROBE_SECRET', str(error.exception))

    def test_child_path_after_every_overlay_and_runtime_controls_fail_before_files(self):
        valid = self.runtime(20)
        binding = self.bind({'web': job()}, ['20=' + str(valid)])['web']
        calls = []
        def capture(args, **kwargs):
            calls.append(kwargs['env'])
            return types.SimpleNamespace(stdout=iter(['node script\n']), wait=lambda: 0)
        selected = {'name': 'later token mint', 'run': 'node later.mjs', 'env': {'PATH': '/step/path'}}
        carry = {'PATH': '/carry/path'}
        ambient = os.environ.copy()
        with mock.patch.object(self.runner.subprocess, 'Popen', side_effect=capture), mock.patch.object(self.runner, 'BASH', '/bash-parent/bash'):
            self.assertTrue(self.runner.run_step(selected, job(), self.root, 5442, None, False, False, carry, node_binding=binding)[0])
            self.assertEqual(calls[-1]['PATH'], f'{valid}:/bash-parent:/step/path')
            selected.pop('env')
            self.assertTrue(self.runner.run_step(selected, job(), self.root, 5442, None, False, False, carry, node_binding=binding)[0])
            self.assertEqual(calls[-1]['PATH'], f'{valid}:/bash-parent:/carry/path')
            for name in ['NODE_OPTIONS', 'NODE_PATH']:
                with mock.patch.object(self.runner.tempfile, 'mkdtemp') as files:
                    result = self.runner.run_step(selected, job(), self.root, 5442, None, False, False, {name: 'PRIVATE'}, node_binding=binding)
                    self.assertFalse(result[0]); self.assertNotIn('PRIVATE', result[1]); files.assert_not_called()
        self.assertEqual(os.environ.copy(), ambient)
        self.assertEqual(carry, {'PATH': '/carry/path'})

    def test_two_concurrent_bindings_and_setup_dispatch_keep_job_provenance(self):
        bins = {m: self.runtime(m) for m in [20, 22]}
        jobs = {'web': job('20', 'node token.mjs'), 'widgets': job('22', 'npm run test')}
        bindings = self.bind(jobs, [f'{m}={p}' for m, p in bins.items()])
        calls = []; outcomes = []; lock = threading.Lock()
        def capture(args, **kwargs):
            with lock: calls.append(kwargs['env']['PATH'])
            return types.SimpleNamespace(stdout=iter(['native\n']), wait=lambda: 0)
        before = os.environ.copy()
        output = io.StringIO()
        with mock.patch.object(self.runner.subprocess, 'Popen', side_effect=capture), contextlib.redirect_stdout(output):
            threads = [threading.Thread(target=lambda jid=jid: outcomes.append(self.runner.run_job(jid, jobs[jid], self.root, 5442, None, False, False, node_binding=bindings[jid]))) for jid in jobs]
            for thread in threads: thread.start()
            for thread in threads: thread.join(3); self.assertFalse(thread.is_alive())
        self.assertEqual(sorted(r.status for r in outcomes), ['pass', 'pass'])
        self.assertEqual({p.split(':')[0] for p in calls}, {str(p) for p in bins.values()})
        self.assertEqual(os.environ.copy(), before)
        for jid in jobs: self.assertIn('"job": "' + jid + '"', output.getvalue())

    def test_main_list_does_not_probe_and_all_dispatches_carry_the_binding(self):
        runner = self.runner
        workflow = {'jobs': {'gmail-calendar-actions': job()}}
        with mock.patch.object(sys, 'argv', ['ci_local.py', '--job', 'gmail-calendar-actions', '--list', '--node-bin', '20=/unavailable/bin']), \
                mock.patch.object(runner, 'repo_root', return_value=ROOT), \
                mock.patch.object(runner.yaml, 'safe_load', create=True, return_value=workflow), \
                mock.patch.object(runner, 'merge_base', return_value=SOURCE), \
                mock.patch.object(runner, 'resolve_node_bindings') as resolve, \
                mock.patch.object(runner, 'RunLock') as lock, contextlib.redirect_stdout(io.StringIO()):
            self.assertEqual(runner.main(), 0); resolve.assert_not_called(); lock.assert_not_called()
        tree = ast.parse((ROOT / 'scripts/ci_local.py').read_text())
        main = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == 'main')
        dispatches = [n for n in ast.walk(main) if isinstance(n, ast.Call) and isinstance(n.func, ast.Name) and n.func.id == 'run_job']
        self.assertEqual(len(dispatches), 3)
        for call in dispatches: self.assertIn('node_binding', [k.arg for k in call.keywords])
        self.assertLess((ROOT / 'scripts/ci_local.py').read_text().index('node_bindings = resolve_node_bindings'),
                        (ROOT / 'scripts/ci_local.py').read_text().index('    run_lock = RunLock(root)'))

    def test_main_actual_serial_plain_and_pg_lane_dispatches_use_verified_job_binding(self):
        bins = {m: self.runtime(m) for m in [20, 22]}
        runner = self.runner
        selections = [({'web': job()}, '1'),
                      ({'web': job(), 'widgets': job('22')}, '2'),
                      ({'mcp-e2e': {**job(), 'services': {'postgres': {}}}, 'web': job()}, '2')]
        for jobs, parallel in selections:
            with self.subTest(jobs=list(jobs)), contextlib.ExitStack() as stack:
                calls = []
                def dispatched(jid, selected, *args, node_binding=None, **kwargs):
                    self.assertEqual(node_binding.job_id, jid)
                    self.assertEqual(node_binding.runtime.major, int(selected['steps'][1]['with']['node-version']))
                    calls.append(jid)
                    return runner.JobResult(jid, 'pass')
                stack.enter_context(mock.patch.object(sys, 'argv', ['ci_local.py', '--job', ','.join(jobs), '-j', parallel,
                    '--node-bin', '20=' + str(bins[20]), '--node-bin', '22=' + str(bins[22])]))
                for name, result in [('repo_root', ROOT), ('merge_base', SOURCE), ('sh', SOURCE), ('modern_bash', '/bin/bash'),
                                     ('link_target_dir', None), ('load_durations', {})]:
                    stack.enter_context(mock.patch.object(runner, name, return_value=result))
                for name in ['preflight', 'warn_node_resolution_leak', 'ensure_pg', 'mark_run_started', 'write_receipt', 'save_durations']:
                    stack.enter_context(mock.patch.object(runner, name))
                stack.enter_context(mock.patch.object(runner.yaml, 'safe_load', create=True, return_value={'jobs': jobs}))
                stack.enter_context(mock.patch.object(runner, 'RunLock'))
                stack.enter_context(mock.patch.object(runner, 'SECONDARY_FIXTURES'))
                stack.enter_context(mock.patch.object(runner.atexit, 'register'))
                stack.enter_context(mock.patch.object(runner, 'run_job', side_effect=dispatched))
                stack.enter_context(contextlib.redirect_stdout(io.StringIO()))
                self.assertEqual(runner.main(), 0)
                self.assertCountEqual(calls, jobs)

    def test_main_wrong_major_fails_before_lock_service_marker_and_job(self):
        wrong = self.runtime(26)
        self.runner.shutil.which.return_value = str(wrong / 'node')
        runner = self.runner
        with contextlib.ExitStack() as stack:
            stack.enter_context(mock.patch.object(sys, 'argv', ['ci_local.py', '--job', 'mcp-e2e']))
            stack.enter_context(mock.patch.object(runner, 'repo_root', return_value=ROOT))
            stack.enter_context(mock.patch.object(runner, 'merge_base', return_value=SOURCE))
            stack.enter_context(mock.patch.object(runner, 'sh', return_value=SOURCE))
            stack.enter_context(mock.patch.object(runner.yaml, 'safe_load', create=True, return_value={'jobs': {'mcp-e2e': {**job(), 'services': {'postgres': {}}}}}))
            guards = [stack.enter_context(mock.patch.object(runner, name)) for name in ['RunLock', 'ensure_pg', 'mark_run_started', 'run_job', 'preflight']]
            stack.enter_context(contextlib.redirect_stdout(io.StringIO()))
            stack.enter_context(contextlib.redirect_stderr(io.StringIO()))
            with self.assertRaises(SystemExit) as error: runner.main()
            self.assertEqual(error.exception.code, 2)
            for guard in guards: guard.assert_not_called()

    def test_preflight_declared_requirements_use_absolute_bindings_not_wrong_ambient_tools(self):
        selected = {'mcp-e2e': job()}
        valid = self.runtime(20)
        binding = self.bind(selected, ['20=' + str(valid)])
        self.assertEqual(self.runner.tools_for(selected), {'node', 'npm'})
        with mock.patch.object(self.runner, 'have_tool', return_value=False) as ambient:
            self.runner.preflight(selected, binding)
            ambient.assert_not_called()

    def test_wrong_execpath_platform_and_npx_version_are_rejected(self):
        valid = self.runtime(20)
        original = self.probe
        for kind in ['execPath', 'platform', 'npx']:
            def changed(args, kind=kind, **kwargs):
                result = original(args, **kwargs)
                if args[1] == '-e' and kind != 'npx':
                    identity = json.loads(result.stdout)
                    identity[kind] = '/other/node' if kind == 'execPath' else 'bad platform text'
                    result.stdout = json.dumps(identity)
                if kind == 'npx' and args[1].endswith('npx-cli.js'): result.stdout = '9.0.0'
                return result
            with self.subTest(kind=kind), mock.patch.object(self.runner.subprocess, 'run', side_effect=changed), self.assertRaises(self.runner.NodeBindingError):
                self.bind({'web': job()}, ['20=' + str(valid)])


if __name__ == '__main__':
    unittest.main()
