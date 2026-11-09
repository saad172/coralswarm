#!/usr/bin/env python3
"""Freeze request-handler transaction bypass counts (page-load standard L1 §4.3).

Counts are per file/opener, so new bypasses in existing pool-bearing files fail.
This textual lint cannot follow aliases or helpers outside routes; review those.
Existing control-plane scopes and legacy webhook routes are debt, not precedent.
"""
import argparse
from collections import Counter
from pathlib import Path
import re
import tempfile

PATTERNS = {
    "raw_begin": r"\.\s*begin(?:_with)?\s*\(",
    "free_scope": r"(?<![.\w])begin_(?:account|tenant|tenant_system)_scoped(?:_without_account)?\s*\(",
    "legacy_tenant": r"\bTenant\s*::\s*legacy\s*\(",
    "answer_read": r"\bbegin_answer_read_tx\s*!\s*\(",
}
TOKENS = re.compile(r'"(?:\\.|[^"\\])*"|//[^\n]*|/\*.*?\*/', re.S)


def measure(root):
    counts = Counter()
    for path in sorted((root / "backend/src/routes").rglob("*.rs")):
        if path.name.endswith("_tests.rs") or "pg_tests" in path.parts:
            continue
        code = path.read_text().split("#[cfg(test)]", 1)[0]
        code = TOKENS.sub(" ", code)
        for kind, pattern in PATTERNS.items():
            n = len(re.findall(pattern, code))
            if n:
                counts[(path.relative_to(root).as_posix(), kind)] = n
    return counts


def read_baseline(path):
    counts = Counter()
    for line in path.read_text().splitlines():
        if line and not line.startswith("#"):
            file, kind, count = line.split()
            key = (file, kind)
            if key in counts or kind not in PATTERNS or int(count) <= 0:
                raise ValueError(f"invalid baseline entry: {line}")
            counts[key] = int(count)
    return counts


def render(counts):
    return (
        "# Request-handler transaction bypass ratchet; page-load standard L1 §4.3.\n"
        "# Freeze per-file/per-opener counts; reductions must tighten the baseline.\n"
        "# Existing control-plane scopes and legacy routes are included as debt.\n"
        "# Prefer Tenant methods, which preserve route class/lane/timeout.\n"
        + "".join(f"{file} {kind} {n}\n" for (file, kind), n in sorted(counts.items()))
    )


def audited_read_bypasses(code, name):
    """Balanced bodies, retaining offsets while masking SQL/comments."""
    masked = TOKENS.sub(lambda m: " " * len(m[0]), code)
    found, failures = 0, []
    for match in re.finditer(r"\basync fn " + re.escape(name) + r"\s*[(<]", masked):
        start = masked.find("{", match.end())
        semi = masked.find(";", match.end())
        if start < 0 or (semi >= 0 and semi < start):
            continue  # Store trait declaration
        found += 1
        depth, end = 1, start + 1
        while depth:
            depth += (masked[end] == "{") - (masked[end] == "}")
            end += 1
        body = masked[start:end]
        if any(re.search(PATTERNS[kind], body) for kind in ("raw_begin", "free_scope", "legacy_tenant")):
            failures.append("class-blind opener")
        # request_read(pool) and &mut *scoped_tx are deliberately not matched.
        for read in re.finditer(r"\.(?:fetch_(?:all|one|optional|many)|execute)\(\s*(&?[\w.]+)\s*\)", body):
            if any(word in read[1] for word in ("pool", "control")):
                failures.append("raw pool query: " + read[1])
    return found, failures


def check_audited_readers(root):
    failures, entries = [], set()
    for line in (root / "scripts/page_read_readers.manifest").read_text().splitlines():
        if not line or line.startswith("#"):
            continue
        file, name = line.split()
        entries.add((file, name))
        found, bypasses = audited_read_bypasses((root / file).read_text(), name)
        if not found:
            failures.append(f"{file}::{name}: missing audited function")
        failures.extend(f"{file}::{name}: {failure}" for failure in bypasses)
    # Every newly declared plain route must have its entry point audited;
    # helpers are additional manifest rows, not substitutes for entry points.
    for module, name in re.findall(
        r'page_read::plain(?:_open)?\(\s*"[^"]+",\s*routes::(\w+)::(\w+)',
        (root / "backend/src/lib.rs").read_text(),
    ):
        if (f"backend/src/routes/{module}.rs", name) not in entries:
            failures.append(f"plain route {module}::{name}: missing read audit")
    for failure in failures:
        print("FAIL: audited page-read helper " + failure)
    return not failures


def self_test():
    with tempfile.TemporaryDirectory() as temp:
        root = Path(temp)
        routes = root / "backend/src/routes"
        routes.mkdir(parents=True)
        file = routes / "fixture.rs"
        file.write_text('''// pool.begin().await; Tenant::legacy(state);
async fn handler(tenant: Tenant) {
    tenant.begin_member_scoped(account).await;
    tenant.begin_account_scoped(account).await;
    let url = "postgres://localhost/db";
    pool
        .begin_with("BEGIN READ ONLY").await;
    crate::tenancy::scope::begin_tenant_scoped(pool, account, user).await;
    Tenant::legacy(state);
    begin_answer_read_tx!(tenant);
}
#[cfg(test)]
mod tests { fn f() { pool.begin(); } }
''')
        (routes / "fixture_pg_tests.rs").write_text("fn f() { pool.begin(); }")
        expected = Counter({("backend/src/routes/fixture.rs", kind): 1 for kind in PATTERNS})
        assert measure(root) == expected, measure(root)
        file.write_text(file.read_text().replace("    let url", "    pool.begin().await;\n    let url"))
        assert measure(root)[("backend/src/routes/fixture.rs", "raw_begin")] == 2
        baseline = root / "baseline"
        baseline.write_text(render(expected))
        assert read_baseline(baseline) == expected
        assert measure(root) != expected, "a new opener in an existing file must fail"
        file.write_text("fn handler() {}")
        assert measure(root) != expected, "a stale high baseline must fail"
    assert audited_read_bypasses("async fn read() { query.fetch_one(pool).await; }", "read")[1]
    assert audited_read_bypasses("async fn read() { query.fetch_one(request_read(pool)).await; }", "read") == (1, [])
    assert audited_read_bypasses("async fn read() { begin_account_scoped(pool, id).await; }", "read")[1]
    assert audited_read_bypasses("async fn read() { let tx = pool.begin().await; query.fetch_one(&mut *tx).await; }", "read")[1]
    print("ok: handler transaction ratchet self-test")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--self-test", action="store_true")
    parser.add_argument("--update", action="store_true", help="tighten only")
    args = parser.parse_args()
    if args.self_test:
        self_test()
        return 0
    root = Path(__file__).resolve().parent.parent
    if not check_audited_readers(root):
        return 1
    baseline = root / "scripts/handler_transactions.baseline"
    before, now = read_baseline(baseline), measure(root)
    growth = any(now[key] > before[key] for key in now)
    if args.update and not growth:
        baseline.write_text(render(now))
        print("ok: tightened handler transaction baseline")
        return 0
    if before != now:
        for key in sorted(before.keys() | now.keys()):
            if before[key] != now[key]:
                print(f"FAIL: {' '.join(key)}: {before[key]} -> {now[key]}")
        print("Use Tenant transaction methods. Tighten deletions with --update; review new bypasses explicitly.")
        return 1
    print(f"ok: handler transaction bypasses frozen ({sum(now.values())} in {len(now)} file/opener entries)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
