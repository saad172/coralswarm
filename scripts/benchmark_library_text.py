#!/usr/bin/env python3
"""Read-only Library text candidate EXPLAIN/hash comparison on an isolated copy.

Build the test-support library_text_search_sql binary first. Set
LIBRARY_TEXT_BENCH_DSN privately; this script never prints or stores it.
Index installation is a separate operator action on the disposable branch.
The probe measures outcome/source matching, excluding reef title projection.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import urllib.parse

TYPES = "uuid[],jsonb,uuid,uuid,uuid,text,text,timestamptz,uuid,timestamptz,uuid,bigint,bigint,uuid,bigint,timestamptz"
MATCH_RESULT = """ SELECT jsonb_build_object(
 'sources',(SELECT count(*) FROM matched_sources),
 'source_ids',(SELECT md5(COALESCE(string_agg(source_item_id::text,',' ORDER BY source_item_id),'')) FROM matched_sources),
 'outcomes',(SELECT count(*) FROM matched_atoms),
 'outcome_ids',(SELECT md5(COALESCE(string_agg(graph_id::text||':'||source_version_id::text||':'||semantic_atom_id::text,',' ORDER BY graph_id,source_version_id,semantic_atom_id),'')) FROM matched_atoms))"""


def decode_all(text):
    decoder = json.JSONDecoder()
    results = []
    while text.strip():
        text = text.lstrip()
        value, end = decoder.raw_decode(text)
        results.append(value)
        text = text[end:]
    return results


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--sql-bin", required=True)
    p.add_argument("--output", required=True)
    p.add_argument("--phase", choices=["before_indexes", "after_indexes"], required=True)
    p.add_argument("--graph", action="append", required=True)
    p.add_argument("--query", action="append")
    p.add_argument("--mode", action="append", choices=["all_retained", "current"])
    p.add_argument("--repeats", type=int, default=2)
    a = p.parse_args()
    dsn = os.environ["LIBRARY_TEXT_BENCH_DSN"]
    uri = urllib.parse.urlparse(dsn)
    env = os.environ.copy()
    env.update(PGHOST=uri.hostname, PGPORT=str(uri.port or 5432), PGUSER=uri.username,
               PGPASSWORD=urllib.parse.unquote(uri.password or ""), PGDATABASE=uri.path.lstrip("/"), PGSSLMODE="require")
    output = Path(a.output)
    output.mkdir(parents=True, exist_ok=True)
    records = []
    variants = ["baseline"] if a.phase == "before_indexes" else ["baseline", "optimized"]
    queries = a.query or ["deploy", "meeting", "de", "a"]
    for graph in a.graph:
        for mode in a.mode or ["all_retained", "current"]:
            for query in queries:
                expected = None
                for variant in variants:
                    scope = json.dumps({"graph_ids": [graph], "outcome_versions": mode})
                    args = [a.sql_bin, scope, query] + (["--baseline"] if variant == "baseline" else [])
                    sql = subprocess.check_output(args, text=True)
                    end = sql.index("), title_matched_nodes AS") + 1
                    sql = sql[:end] + MATCH_RESULT
                    pattern = "%" + query.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_") + "%"
                    literal = lambda s: "'" + s.replace("'", "''") + "'"
                    binds = f"ARRAY[{literal(graph)}]::uuid[],'{{}}'::jsonb,NULL,NULL,NULL,NULL,{literal(pattern)},NULL,NULL,NULL,NULL,NULL,NULL,NULL,21,NULL"
                    for repeat in range(a.repeats):
                        command = f"BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY; SET LOCAL statement_timeout='120s'; PREPARE bench({TYPES}) AS {sql}; EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) EXECUTE bench({binds}); EXECUTE bench({binds}); ROLLBACK;"
                        run = subprocess.run(["psql", "-XqAt", "-v", "ON_ERROR_STOP=1"], input=command, text=True, capture_output=True, env=env)
                        if run.returncode:
                            # Host/user are supplied separately, so errors cannot echo a URI.
                            raise RuntimeError(f"psql failed graph={graph} mode={mode} query={query!r}: {run.stderr[:500]}")
                        plan, result = decode_all(run.stdout)
                        if expected is None:
                            expected = result
                        if result != expected:
                            raise AssertionError(f"candidate mismatch graph={graph} mode={mode} query={query!r}")
                        label = f"{graph[:8]}-{mode}-{query}-{variant}-{repeat}"
                        digest = hashlib.sha256(label.encode()).hexdigest()[:12]
                        plan_path = output / f"plan-{digest}.json"
                        plan_path.write_text(json.dumps(plan, indent=2) + "\n")
                        indexes = []
                        scans = []
                        def walk(node):
                            if "Index Name" in node:
                                indexes.append(node["Index Name"])
                            if node.get("Relation Name") in ("semantic_atoms", "semantic_atom_passages"):
                                scans.append({"relation":node["Relation Name"], "scan":node["Node Type"], "rows":node.get("Actual Rows"), "loops":node.get("Actual Loops")})
                            for child in node.get("Plans", []):
                                walk(child)
                        walk(plan[0]["Plan"])
                        record = dict(graph=graph, mode=mode, query=query, variant=variant, repeat=repeat,
                                      execution_ms=plan[0]["Execution Time"], planning_ms=plan[0]["Planning Time"],
                                      sql_sha256=hashlib.sha256(sql.encode()).hexdigest(),
                                      result=result, indexes=sorted(set(indexes)), text_scans=scans, plan=plan_path.name)
                        records.append(record)
                        (output / "results.json").write_text(json.dumps({"phase":a.phase,"records":records}, indent=2) + "\n")
                        print(f"{a.phase} {graph[:8]} {mode} {query!r} {variant} repeat={repeat} ms={record['execution_ms']:.1f} sources={result['sources']} outcomes={result['outcomes']}", flush=True)
    print(f"completed {len(records)} matching-set comparisons; raw plans in {output}")


if __name__ == "__main__":
    main()
