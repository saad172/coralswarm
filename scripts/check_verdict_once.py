#!/usr/bin/env python3
"""Each per-row verdict runs once per row.

A verdict is a STABLE SQL function that reads tables to decide what a row may
show (the list is `VERDICT_FUNCTIONS` in backend/src/store/verdict.rs).
Postgres runs every textual call, so a verdict written twice with the same
arguments in one statement runs twice per row. In #1486 that, plus an uuid
column cast to text inside `library_answer_permitted_before_temporal`, made
opening a past Ask chat take 2 s.

Two checks:

1. A SQL string literal under backend/src that calls one verdict twice with
   the same arguments fails, unless scripts/verdict_once.baseline lists it.
   Fix it by computing the verdict once in
   `CROSS JOIN LATERAL (SELECT verdict(..) AS v OFFSET 0) alias` and reading
   `alias.v` elsewhere (backend/src/store/verdict.rs explains the shape). A baseline entry that no
   longer matches also fails, so the baseline only shrinks.
2. The current definition of a verdict function, in either migration tree,
   fails when it compares an id column cast to text (`x.id::text = ...`).
   Cast the other side to uuid instead, so the column's index is usable.

It is a lint over SQL text. A verdict reached through a different spelling
of the same arguments, or duplicated by the planner, passes it; the plan check
in backend/tests/p7_read_boundary_pg.rs covers the registered statements.

Usage:
  python3 scripts/check_verdict_once.py              # non-zero on violation
  python3 scripts/check_verdict_once.py --self-test  # the checks catch the shapes
  python3 scripts/check_verdict_once.py --reader-programs export.jsonl --source-sha HEAD_SHA
"""
import argparse
import copy
import hashlib
import importlib.util
import json
import pathlib
import re
import shutil
import sys
import tempfile

LITERAL = re.compile(r'r(#*)"(.*?)"\1|"((?:[^"\\]|\\.)*)"', re.S)
SQL = re.compile(r"\b(SELECT|UPDATE|DELETE|WITH)\b")
FUNCTION_DEF = re.compile(r"CREATE\s+(?:OR\s+REPLACE\s+)?FUNCTION\s+(?:public\.)?([a-z_][a-z0-9_]*)\s*\(", re.I)
NEXT_STATEMENT = re.compile(r"\n\s*(?:CREATE|DROP|ALTER|REVOKE|GRANT|DO|COMMENT)\s", re.I)
ID_AS_TEXT = re.compile(
    r"(?:\b[a-z_]+\.)?\b(?:[a-z_]*_id|id)::text\s*(?:=|<>)|(?:=|<>)\s*(?:\b[a-z_]+\.)?\b(?:[a-z_]*_id|id)::text\b", re.I
)
MIGRATION_TREES = ("backend/migrations_tenant", "backend/migrations_control")


def verdict_functions(root):
    source = (root / "backend/src/store/verdict.rs").read_text()
    block = re.search(r"VERDICT_FUNCTIONS: &\[&str\] = &\[(.*?)\];", source, re.S)
    if not block:
        sys.exit("FAIL: VERDICT_FUNCTIONS not found in backend/src/store/verdict.rs")
    return re.findall(r'"([a-z_0-9]+)"', block.group(1))


def calls(sql, function):
    out = []
    for match in re.finditer(r"\b%s\s*\(" % function, sql):
        i, depth = match.end(), 1
        while i < len(sql) and depth:
            depth += (sql[i] == "(") - (sql[i] == ")")
            i += 1
        out.append(re.sub(r"\s+", "", sql[match.end(): i - 1]))
    return out


def repeated_calls(root, functions):
    found = {}
    for path in sorted((root / "backend/src").rglob("*.rs")):
        source = path.read_text()
        rel = path.relative_to(root).as_posix()
        for literal in LITERAL.finditer(source):
            sql = literal.group(2) if literal.group(2) is not None else literal.group(3)
            if not SQL.search(sql):
                continue
            line = source.count("\n", 0, literal.start()) + 1
            for function in functions:
                args = calls(sql, function)
                for arg in sorted(set(args)):
                    if args.count(arg) > 1:
                        key = (rel, f"{function}({arg})")
                        found.setdefault(key, []).append((line, args.count(arg)))
    return found


def baseline(root):
    """Each line allows one statement in `path` to repeat `call` up to `count` times."""
    entries = {}
    path = root / "scripts/verdict_once.baseline"
    for raw in path.read_text().splitlines() if path.exists() else []:
        line = raw.split("#", 1)[0].strip()
        if line:
            rel, call, count = line.split()
            entries.setdefault((rel, call), []).append(int(count))
    return entries


def text_id_comparisons(root, functions):
    found = []
    for tree in MIGRATION_TREES:
        latest = {}
        for path in sorted((root / tree).glob("*.sql")):
            text = path.read_text()
            for match in FUNCTION_DEF.finditer(text):
                rest = NEXT_STATEMENT.search(text, match.end())
                latest[match.group(1).lower()] = (path, text[match.start(): rest.start() if rest else len(text)])
        for function in functions:
            if function in latest:
                path, body = latest[function]
                for hit in ID_AS_TEXT.finditer(body):
                    found.append(f"{path.relative_to(root).as_posix()}: {function} compares {hit.group(0).strip()!r}")
    return found


def check(root):
    functions = verdict_functions(root)
    failures = []
    allowed = baseline(root)
    repeated = repeated_calls(root, functions)
    for (rel, call), sites in sorted(repeated.items()):
        counts = sorted((count for _, count in sites), reverse=True)
        limits = sorted(allowed.get((rel, call), []), reverse=True)
        if len(counts) > len(limits) or any(count > limit for count, limit in zip(counts, limits)):
            lines = ", ".join(f"line {line} ({count}x)" for line, count in sites)
            failures.append(f"{rel}: {call} is called more than once in one statement at {lines}")
    for (rel, call), limits in sorted(allowed.items()):
        if len(limits) > len(repeated.get((rel, call), [])):
            failures.append(f"scripts/verdict_once.baseline: stale entry {rel} {call}; delete it")
    failures += text_id_comparisons(root, functions)
    return failures


def reader_contract(root):
    path = root / "scripts/library_reader_export_contract.py"
    spec = importlib.util.spec_from_file_location("reader_export_contract", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


QUOTED_OR_COMMENT = re.compile(r"'(?:''|[^'])*'|\"(?:\"\"|[^\"])*\"|--[^\n]*|/\*.*?\*/", re.S)


def masked(token):
    return re.sub(r"[^\n]", " ", token)


def sql_without_comments(sql):
    # Keep quoted strings intact: comment syntax inside a JSON/text literal
    # is data. The generated programs contain no dollar-quoted function DDL.
    return QUOTED_OR_COMMENT.sub(lambda m: masked(m[0]) if m[0].startswith(("--", "/*")) else m[0], sql)


def sql_code_view(sql):
    # Preserve offsets for the raw JSON-key boundaries, while code-only
    # assertions cannot borrow a relation, fence or limit from string data.
    return QUOTED_OR_COMMENT.sub(lambda m: masked(m[0]), sql)


def canonical_title_grains(sql):
    """Preserve four shipped Live phases, not four disjoint physical sets.

    This is the rendered counterpart of the two reviewed literal baselines:
    matching/work admission and the node/work page projections. Require one
    call in EACH actual grain; a total-count allowance would accept moving a
    second call into a single grain. Model bodies never receive this exception.
    A work admission can inspect more than six candidates before its LIMIT.
    """
    sql = sql_without_comments(sql)
    if re.search(r"\$[a-zA-Z_0-9]*\$|\bE'|\bU&", sql):
        return False  # These quoting forms are absent from this renderer.
    code = sql_code_view(sql)
    function, argument = "library_node_title", "n.graph_id,n.node_id"
    boundaries = [
        ("matched_nodes AS MATERIALIZED (", "source_page AS (",
         (r"\bFROM scope_nodes n\b", r"\bFROM title_matched_nodes t\b", r"\bOFFSET 0\s*\)\s+n\b")),
        ("work_page AS (", "page_nodes AS MATERIALIZED (",
         (r"\bFROM matched_nodes n\b", r"\bORDER BY n\.source_count DESC,n\.atom_count DESC,n\.node_id DESC OFFSET 0\s*\)\s+n\b", r"\bLIMIT 6\s*\),\s*$")),
        ("\n 'nodes',jsonb_build_object('total',(SELECT count(*) FROM matched_nodes),'items',",
         "\n 'prominent_work',", (r"\bFROM node_page n\b",)),
        ("\n 'prominent_work',", "\n 'facets',", (r"\bFROM work_page n\b",)),
    ]
    for left, right, required in boundaries:
        # Names such as title_matched_nodes / atom_page_nodes are distinct
        # grains, despite containing these identifiers as a suffix.
        starts = [m.start() for m in re.finditer((r"(?<![a-zA-Z_0-9])" if left[0].isalnum() else "") + re.escape(left), sql)]
        ends = [m.start() for m in re.finditer((r"(?<![a-zA-Z_0-9])" if right[0].isalnum() else "") + re.escape(right), sql)]
        if len(starts) != 1 or len(ends) != 1:
            return False
        start, end = starts[0], ends[0]
        if start >= end:
            return False
        grain = code[start:end]
        if calls(grain, function) != [argument] or not all(re.search(part, grain) for part in required):
            return False
    page = re.search(r"(?<![a-zA-Z_0-9])node_page AS \((.*?)\), project_mentions AS MATERIALIZED", code, re.S)
    if page is None or not re.search(r"\bLIMIT \$15\s*\)\s+n\b", page[1]) or not re.search(r"\bFROM matched_nodes n\b", page[1]):
        return False
    return calls(code, function).count(argument) == 4


def check_reader_programs(root, records, source_sha):
    """Inspect actual rendered bodies after the operator's strict wire check."""
    contract = reader_contract(root)
    try:
        contract.require(contract.valid_sha(source_sha), "expected_source_sha")
        contract.require(isinstance(records, list) and records and isinstance(records[0], dict), "reader_program_records")
        descriptor = {key: records[0].get(key) for key in
                      ("source_sha", "model_sql_sha256", "definition_version", "context_version", "program_version")}
        contract.require(descriptor["source_sha"] == source_sha, "reader_program_source")
        contract.validate_sql_export_records(records, descriptor)
    except contract.ContractError as error:
        return ["generated reader export contract: " + str(error)]
    failures = []
    for row in records:
        identity = f"{row['outcome_versions']}/{row['filtered']}/{row['text_strategy']}"
        for stage in ("clean_model_attempt_sql", "dirty_model_attempt_sql", "conditional_live_sql"):
            sql = sql_without_comments(row["program"][stage])
            for function in verdict_functions(root):
                args = calls(sql_code_view(sql), function)
                for arg in sorted(set(args)):
                    if args.count(arg) > 1:
                        if (stage == "conditional_live_sql" and function == "library_node_title" and
                                arg == "n.graph_id,n.node_id" and canonical_title_grains(sql)):
                            continue
                        failures.append(f"generated {identity}/{stage}: {function}({arg}) is called {args.count(arg)} times")
    return failures


VERDICT_RS = 'pub const VERDICT_FUNCTIONS: &[&str] = &["library_node_title", "library_answer_permitted_before_temporal"];\n'
FENCED = 'const A: &str = "SELECT n.node_id, t.title FROM nodes n CROSS JOIN LATERAL (SELECT library_node_title(n.graph_id,n.node_id) AS title OFFSET 0) t WHERE t.title IS NOT NULL";\n'
REPEATED = 'const B: &str = "SELECT node_id, library_node_title(graph_id,node_id) FROM nodes WHERE library_node_title(graph_id, node_id) IS NOT NULL";\n'
UUID_PROBE = "CREATE FUNCTION library_answer_permitted_before_temporal(r UUID) RETURNS BOOLEAN LANGUAGE SQL STABLE AS $$ SELECT EXISTS(SELECT 1 FROM source_items s WHERE s.source_item_id=(r::text)::uuid) $$;\n"
TEXT_PROBE = "CREATE OR REPLACE FUNCTION library_answer_permitted_before_temporal(r UUID) RETURNS BOOLEAN LANGUAGE SQL STABLE AS $$ SELECT EXISTS(SELECT 1 FROM source_items s WHERE s.source_item_id::text=r::text) $$;\n"


def self_test():
    def tree(files):
        root = pathlib.Path(tempfile.mkdtemp())
        for rel, text in files.items():
            (root / rel).parent.mkdir(parents=True, exist_ok=True)
            (root / rel).write_text(text)
        for rel in MIGRATION_TREES:
            (root / rel).mkdir(parents=True, exist_ok=True)
        return root

    base = {"backend/src/store/verdict.rs": VERDICT_RS, "backend/migrations_tenant/0001_f.sql": UUID_PROBE}
    cases = [
        ("fenced verdict passes", {"backend/src/a.rs": FENCED}, 0),
        ("repeated verdict fails", {"backend/src/a.rs": REPEATED}, 1),
        ("baselined repeat passes", {"backend/src/a.rs": REPEATED,
                                     "scripts/verdict_once.baseline": "backend/src/a.rs library_node_title(graph_id,node_id) 2 # why\n"}, 0),
        ("a second baselined statement fails", {"backend/src/a.rs": REPEATED + REPEATED.replace("const B", "const C"),
                                                "scripts/verdict_once.baseline": "backend/src/a.rs library_node_title(graph_id,node_id) 2 # why\n"}, 1),
        ("stale baseline fails", {"backend/src/a.rs": FENCED,
                                  "scripts/verdict_once.baseline": "backend/src/a.rs library_node_title(graph_id,node_id) 2 # why\n"}, 1),
        ("text id probe in the latest definition fails", {"backend/migrations_tenant/0002_f.sql": TEXT_PROBE}, 1),
        ("text id probe superseded by a uuid probe passes",
         {"backend/migrations_tenant/0000_f.sql": TEXT_PROBE}, 0),
    ]
    ok = True
    for name, files, expected in cases:
        root = tree({**base, **files})
        got = len(check(root))
        shutil.rmtree(root)
        status = "ok" if got == expected else "FAIL"
        ok &= got == expected
        print(f"{status}: {name} ({got} violations, want {expected})")
    root = pathlib.Path(__file__).resolve().parent.parent
    contract = reader_contract(root)
    source = "a" * 40
    records = []
    for mode in ("all_retained", "current"):
        for filtered in (False, True):
            for strategy in ("empty", "short", "indexed"):
                records.append({"kind": "reader_program", "source_sha": source,
                    "model_sql_sha256": "b" * 64, "definition_version": 3, "context_version": 2, "program_version": 4,
                    "outcome_versions": mode, "filtered": filtered, "text_strategy": strategy,
                    "program": {"contract": contract.PROGRAM_CONTRACT, "clean_model_attempt_sql": "SELECT 1",
                                "dirty_model_attempt_sql": "SELECT 2", "conditional_live_sql": "SELECT 3"},
                    "bind_types": contract.PROGRAM_CONTRACT["bind_types"],
                    "query_settings": contract.PROGRAM_CONTRACT["query_settings"],
                    "page_limit_bind": "requested_page_size_plus_one"})

    def sealed(rows):
        value = copy.deepcopy(rows)
        hasher = hashlib.sha256()
        for row in value:
            for part in (contract.canonical(row["program"]["contract"]),
                         *(row["program"][key].encode() for key in
                           ("clean_model_attempt_sql", "dirty_model_attempt_sql", "conditional_live_sql"))):
                hasher.update(part); hasher.update(b"\0")
        for row in value:
            row["model_sql_sha256"] = hasher.hexdigest()
        return value

    good = sealed(records)
    repeated = copy.deepcopy(records)
    repeated[0]["program"]["dirty_model_attempt_sql"] = "SELECT library_node_title(n.graph_id,n.node_id) FROM nodes n WHERE library_node_title(n.graph_id,n.node_id) IS NOT NULL"
    wrong_stage = copy.deepcopy(good)
    wrong_stage[0]["program"]["unknown_stage"] = "SELECT 4"
    for name, rows, expected_source, rejects in [
        ("all twelve rendered programs pass", good, source, False),
        ("rendered repeat fails after a valid reseal", sealed(repeated), source, True),
        ("missing rendered variant fails", good[:-1], source, True),
        ("duplicate rendered variant fails", good[:-1] + [good[0]], source, True),
        ("extra stage fails", wrong_stage, source, True),
        ("stale compiled source fails", good, "c" * 40, True),
    ]:
        failures = check_reader_programs(root, rows, expected_source)
        passed = bool(failures) == rejects
        ok &= passed
        print(f"{'ok' if passed else 'FAIL'}: {name}")
    # A balanced total is insufficient: moving a call between readers must fail.
    title = "library_node_title(n.graph_id,n.node_id)"
    order = "ORDER BY n.source_count DESC,n.atom_count DESC,n.node_id DESC OFFSET 0"
    grains = (f"matched_nodes AS MATERIALIZED (SELECT {title} FROM (SELECT n.* FROM scope_nodes n WHERE EXISTS(SELECT 1 FROM title_matched_nodes t) OFFSET 0) n), source_page AS (SELECT 1), "
              f"node_page AS (SELECT * FROM (SELECT n.* FROM matched_nodes n LIMIT $15) n), project_mentions AS MATERIALIZED (SELECT 1), "
              f"work_page AS (SELECT {title} FROM (SELECT n.* FROM matched_nodes n {order}) n LIMIT 6), page_nodes AS MATERIALIZED (SELECT 1) "
              f"\n 'nodes',jsonb_build_object('total',(SELECT count(*) FROM matched_nodes),'items',{title} FROM node_page n)"
              f"\n 'prominent_work',{title} FROM work_page n\n 'facets',null")
    mutations = [grains.replace(title, "NULL", 1).replace("FROM work_page n", f",{title} FROM work_page n"),
                 grains.replace(order, order.replace(" OFFSET 0", "")),
                 grains.replace("LIMIT $15", ""),
                 grains.replace(order, "OFFSET 0"),
                 grains.replace("FROM node_page n", "FROM nodes n"),
                 grains + "," + title]
    grain_ok = canonical_title_grains(grains) and not any(canonical_title_grains(value) for value in mutations)
    ok &= grain_ok
    print(f"{'ok' if grain_ok else 'FAIL'}: rendered title exception requires one call per exact bounded grain")
    for name, stage, sql, rejects in [
        ("reviewed Live phases pass", "conditional_live_sql", grains, False),
        ("fifth title call fails", "conditional_live_sql", grains + "," + title, True),
        ("same-count relocation fails", "conditional_live_sql", mutations[0], True),
        ("comment cannot replace a fence", "conditional_live_sql", grains.replace(order, order.replace("OFFSET 0", "-- OFFSET 0\n")), True),
        ("LIMIT 60 cannot borrow LIMIT 6", "conditional_live_sql", grains.replace("LIMIT 6", "LIMIT 60"), True),
        ("OFFSET 01 cannot borrow OFFSET 0", "conditional_live_sql", grains.replace(order, order + "1"), True),
        ("quoted text cannot replace a fence", "conditional_live_sql",
         grains.replace(order, order.replace(" OFFSET 0", "")).replace("work_page AS (SELECT " + title, "work_page AS (SELECT " + title + ",'" + order + "' AS fake"), True),
        ("quoted text cannot replace a relation", "conditional_live_sql",
         grains.replace("FROM matched_nodes n " + order, "FROM scope_nodes n " + order).replace("work_page AS (SELECT " + title, "work_page AS (SELECT " + title + ",'FROM matched_nodes n' AS fake"), True),
        ("changed title arguments fail", "conditional_live_sql", grains.replace(title, "library_node_title(x.graph_id,x.node_id)", 1), True),
        ("stale phase exception fails", "conditional_live_sql", grains.replace(title, "NULL", 1), True),
        ("clean body cannot borrow Live phases", "clean_model_attempt_sql", grains, True),
        ("dirty body cannot borrow Live phases", "dirty_model_attempt_sql", grains, True),
    ]:
        changed = copy.deepcopy(records)
        changed[0]["program"][stage] = sql
        passed = bool(check_reader_programs(root, sealed(changed), source)) == rejects
        ok &= passed
        print(f"{'ok' if passed else 'FAIL'}: {name}")
    return 0 if ok else 1


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--self-test", action="store_true")
    parser.add_argument("--reader-programs", type=pathlib.Path)
    parser.add_argument("--source-sha")
    args = parser.parse_args()
    if args.self_test:
        if args.reader_programs or args.source_sha:
            parser.error("--self-test cannot be combined with reader inputs")
        return self_test()
    root = pathlib.Path(__file__).resolve().parent.parent
    failures = check(root)
    if args.reader_programs:
        if not args.source_sha:
            parser.error("--reader-programs requires the actual checkout --source-sha")
        try:
            records = [json.loads(line) for line in args.reader_programs.read_text().splitlines() if line.strip()]
            failures += check_reader_programs(root, records, args.source_sha)
        except (OSError, ValueError) as error:
            failures.append("reader export could not be read: " + type(error).__name__)
    elif args.source_sha:
        parser.error("--source-sha requires --reader-programs")
    for failure in failures:
        print(f"FAIL: {failure}")
    if not failures:
        print("ok: every verdict runs once per row")
        if args.reader_programs:
            print("ok: source-bound twelve Library programs / thirty-six rendered bodies")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
