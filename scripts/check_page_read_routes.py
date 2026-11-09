#!/usr/bin/env python3
"""Read explicit class/helper declarations and the one Rust model registry.

This deliberately does not infer arbitrary Rust/TS call graphs. Dynamic class
wrappers need an explicit supported declaration shape; browser evidence closes
the declared/observed loop. No database, Cargo or JS package installation.
"""
import copy
import json
from pathlib import Path
import re
import sys


def uncomment(source):
    # Retain quoted strings while removing comments, so documentation and
    # example routes cannot turn a dead registration into a passing one.
    pattern = r'("(?:\\.|[^"\\])*"|\'(?:\\.|[^\'\\])*\'|`(?:\\.|[^`\\])*`)|//[^\n]*|/\*[\s\S]*?\*/'
    return re.sub(pattern, lambda m: m.group(1) or '', source)


def backend_paths(files):
    registered = []
    registry = uncomment(files['backend/src/read_models/registry.rs'])
    match = re.search(r'read_models!\s*\{([^}]+)\}', registry)
    assert match, 'single Rust read_models! registry missing'
    for name in re.findall(r'(\w+::\w+)', match[1]):
        module, model = name.split('::')
        source = uncomment(files[f'backend/src/read_models/{module}.rs'])
        impl = re.search(rf'impl\s+ReadModel\s+for\s+{model}\s*\{{([\s\S]*?)(?=\n\}})', source)
        assert impl, f'registered model has no implementation: {name}'
        path = re.search(r'const\s+PATH\s*:[^=]+?=\s*"([^"]+)"', impl[1])
        assert path, f'registered model has no literal PATH: {name}'
        registered.append('/v1' + path[1])
    assert len(set(registered)) == len(registered), 'duplicate model route'
    plain = set()
    for filename, value in files.items():
        if not filename.startswith('backend/src/') or not filename.endswith('.rs'):
            continue
        source = uncomment(value).split("#[cfg(test)]", 1)[0]
        for kind, path in re.findall(r'page_read::(plain|plain_open)\s*\(\s*"([^"]+)"', source):
            plain.add(path if kind == 'plain_open' else '/v1' + path)
        for absolute, path in re.findall(r'(classify(?:_absolute)?)\s*\(\s*(?:\w+::)*RouteClass::PageRead\s*,\s*(?:\w+::)*Method::GET\s*,\s*"([^"]+)"', source):
            plain.add(path if absolute.endswith('_absolute') else '/v1' + path)
    assert '.merge(read_models::registry::router())' in uncomment(files['backend/src/lib.rs']), 'model registry is not mounted'
    return set(registered), plain


def check(files, literals):
    models, plain = backend_paths(files)
    manifest = json.loads(files['web/lib/pageSections.generated.json'])
    web_models = {'/v1' + entry['path'] for entry in manifest.values()}
    shell = uncomment(files.get('web/lib/pageShellPaths.ts', files['web/lib/pageShell.ts']))
    block = re.search(r'export const PLAIN_LOAD_PATHS = \{([\s\S]+?)\}\s*as const', shell)
    assert block, 'plain page path declarations missing'
    web_plain = set(re.findall(r'"(/v1/[^"\n]+)"', block[1]))
    fan_in = re.search(r'export const FAN_IN_LOAD_PATHS = \{([\s\S]+?)\}\s*as const', shell)
    if fan_in: web_plain.update(re.findall(r'"(/v1/[^"\n]+)"', fan_in[1]))
    assert models == web_models, f'registered model path drift: missing={models-web_models}, invented={web_models-models}'
    assert plain == web_plain, f'plain class/path drift: missing={plain-web_plain}, invented={web_plain-plain}'
    used = set()
    for filename, value in files.items():
        if not filename.startswith(('web/app/', 'web/components/')) or not filename.endswith(('.ts', '.tsx')) or '.test.' in filename:
            continue
        source = uncomment(value)
        if re.search(r'["\'`]/v1/', source):
            assert filename in literals, f'page/component API literal outside canonical paths: {filename}'
            used.add(filename)
    assert used == set(literals), f'dead literal exception: {set(literals)-used}'
    return len(models), len(plain)


def load(root):
    paths = [*root.glob('backend/src/**/*.rs'), *root.glob('web/app/**/*.ts'), *root.glob('web/app/**/*.tsx'), *root.glob('web/components/**/*.ts'), *root.glob('web/components/**/*.tsx')]
    paths += [root/'web/lib/pageSections.generated.json', root/'web/lib/pageShell.ts', root/'web/lib/pageShellPaths.ts']
    files = {str(path.relative_to(root)): path.read_text() for path in paths}
    literals = {}
    for line in (root/'scripts/page_read_literals.manifest').read_text().splitlines():
        if not line.strip() or line.startswith('#'): continue
        name, reason = line.split('\t', 1)
        assert reason.strip() and '*' not in name, 'literal exceptions require exact path and reason'
        assert name not in literals, 'duplicate literal exception'
        literals[name] = reason
    return files, literals


def self_test():
    files = {
        'backend/src/read_models/registry.rs': 'read_models! { home::HomeMetrics, }',
        'backend/src/read_models/home.rs': 'impl ReadModel for HomeMetrics {\n const PATH: &str = "/home/metrics";\n}',
        'backend/src/lib.rs': '.merge(read_models::registry::router())\npage_read::plain("/bootstrap", handler)\npage_read::plain_open("/v1/shared/{token}", handler)',
        'web/lib/pageSections.generated.json': json.dumps({'home_metrics': {'path': '/home/metrics'}}),
        'web/lib/pageShell.ts': 'export const PLAIN_LOAD_PATHS = { boot: "/v1/bootstrap", share: "/v1/shared/{token}" } as const;',
        'web/components/Actions.tsx': 'apiRequest("/v1/action", token, { method: "POST" })',
    }
    literals = {'web/components/Actions.tsx': 'Explicit user action POST.'}
    assert check(files, literals) == (1, 2)
    mutants = []
    def mutate(name, old, new):
        item = copy.deepcopy(files); item[name] = item[name].replace(old, new); mutants.append(item)
    mutate('backend/src/lib.rs', 'page_read::plain("/bootstrap", handler)', '.route("/bootstrap", get(handler))')
    mutate('backend/src/lib.rs', 'page_read::plain_open("/v1/shared/{token}", handler)', '.route("/v1/shared/{token}", get(handler))')
    mutate('backend/src/lib.rs', '.merge(read_models::registry::router())', '// .merge(read_models::registry::router())')
    mutate('backend/src/read_models/registry.rs', 'home::HomeMetrics', 'home::DeadModel')
    mutate('web/lib/pageSections.generated.json', '/home/metrics', '/invented')
    mutate('web/lib/pageShell.ts', '/v1/bootstrap', '/v1/absent')
    mutate('web/lib/pageShell.ts', ' } as const', ', dead: "/v1/dead" } as const')
    item = copy.deepcopy(files); item['web/components/Leak.tsx'] = 'fetch("/v1/me")'; mutants.append(item)
    fan_in = copy.deepcopy(files)
    fan_in['backend/src/lib.rs'] += '\nclassify(RouteClass::PageRead, Method::GET, "/load/oceans", handler)'
    fan_in['web/lib/pageShell.ts'] += '\nexport const FAN_IN_LOAD_PATHS = { oceans: "/v1/load/oceans" } as const;'
    assert check(fan_in, literals) == (1, 3)
    for name, old, new in [
        ('backend/src/lib.rs', 'RouteClass::PageRead', 'RouteClass::Other'),
        ('web/lib/pageShell.ts', '/v1/load/oceans', '/v1/load/invented'),
        ('web/lib/pageShell.ts', 'oceans: "/v1/load/oceans"', 'oceans: "/v1/load/oceans", dead: "/v1/load/dead"'),
    ]:
        item = copy.deepcopy(fan_in); item[name] = item[name].replace(old, new); mutants.append(item)
    for number, item in enumerate(mutants):
        try: check(item, literals)
        except (AssertionError, KeyError): continue
        raise AssertionError(f'bad route fixture {number} passed')
    try: check(files, {**literals, 'web/components/Dead.tsx': 'dead'})
    except AssertionError: pass
    else: raise AssertionError('dead allowlist fixture passed')
    print(f'page-read declaration fixtures: {len(mutants)+1} negative cases rejected')


if __name__ == '__main__':
    try:
        if '--self-test' in sys.argv: self_test()
        else:
            counts = check(*load(Path('.')))
            print(f'page-read paths: {counts[0]} registered models, {counts[1]} explicit plain class declarations')
    except (AssertionError, KeyError, ValueError) as exc:
        print(f'page-read routes: {exc}', file=sys.stderr)
        sys.exit(1)
