#!/usr/bin/env bash
# Cold and warm timings of the clean Library Model text read on a local
# Postgres container, for #1524 blocker 5.
#
# Seed first: the ignored `seed_cold_text_bench` test in
# backend/tests/library_read_model_pg.rs leaves database library_text_bench
# and writes prepare.sql, execute.sql and graph.txt into BENCH_DIR.
#
# Each cold run restarts the container and drops the Docker VM page cache,
# then plans the statement once (catalogs warm, data cold) and runs
# EXPLAIN (ANALYZE, BUFFERS, TIMING OFF). The warm runs follow in one session.
#
# Usage: scripts/benchmark_library_text_cold.sh CONTAINER BENCH_DIR RUNS LABEL [DATABASE]
# Writes BENCH_DIR/runs-LABEL.jsonl and prints the summary.
set -euo pipefail

container=$1 dir=$2 runs=$3 label=$4 database=${5:-library_text_bench}
out="$dir/runs-$label.jsonl"
: > "$out"
graph=$(cat "$dir/graph.txt")
account=$(docker exec "$container" psql -X -tA -U postgres -d "$database" -c "SELECT account_id FROM graphs WHERE id='$graph'")
# The read program's own settings (read_model::program_contract), with the
# account context its readiness check compares against.
preamble="SELECT set_config('app.account_id','$account',false),set_config('app.tenant_actor','system',false) \\g /dev/null
SET jit=off; SET work_mem='64MB'; SET plan_cache_mode=force_custom_plan; SET max_parallel_workers_per_gather=0; SET statement_timeout='120s';"

ready() {
  for _ in $(seq 1 120); do
    docker exec "$container" pg_isready -U postgres -q && return
    sleep 0.5
  done
  echo "postgres did not come back" >&2; exit 1
}

run() {
  local temp=$1 count=$2
  {
    echo "$preamble"
    cat "$dir/prepare.sql"
    echo "EXPLAIN (COSTS OFF) $(sed 's/;$//' "$dir/execute.sql") \\g /dev/null"
    for _ in $(seq 1 "$count"); do echo "EXPLAIN (ANALYZE, BUFFERS, TIMING OFF, FORMAT JSON) $(cat "$dir/execute.sql")"; done
  } | docker exec -i "$container" psql -X -q -t -A -v ON_ERROR_STOP=1 -U postgres -d "$database" \
    | python3 -c '
import json, re, sys
text, decoder, at = sys.stdin.read(), json.JSONDecoder(), 0
while (m := re.compile(r"\[\s*\{").search(text, at)):
    plan, at = decoder.raw_decode(text, m.start())
    print(json.dumps({"temp": sys.argv[1], "plan": plan}))
' "$temp" >> "$out"
}

path=$( { echo "$preamble"; cat "$dir/prepare.sql"; cat "$dir/execute.sql"; } \
  | docker exec -i "$container" psql -X -q -t -A -v ON_ERROR_STOP=1 -U postgres -d "$database")
case "${path:0:200}" in
  '{"path": "model"'*) ;;
  *) echo "the statement did not take the Model path: ${path:0:200}" >&2; exit 1 ;;
esac

for _ in $(seq 1 "$runs"); do
  docker restart "$container" > /dev/null
  docker run --rm --privileged alpine:3.20 sh -c 'sync; echo 3 > /proc/sys/vm/drop_caches'
  ready
  run cold 1
done
run warm "$runs"

python3 - "$out" <<'EOF'
import json, statistics, sys
rows = [json.loads(line) for line in open(sys.argv[1])]
def walk(node):
    yield node
    for child in node.get("Plans", []):
        yield from walk(child)
for temp in ("cold", "warm"):
    plans = [r["plan"][0] for r in rows if r["temp"] == temp]
    times = sorted(p["Execution Time"] for p in plans)
    read = statistics.median(p["Plan"]["Shared Read Blocks"] for p in plans)
    total = statistics.median(p["Plan"]["Shared Read Blocks"] + p["Plan"]["Shared Hit Blocks"] for p in plans)
    text = {sum(n.get("Shared Read Blocks", 0) + n.get("Shared Hit Blocks", 0) for n in walk(p["Plan"])
                if n.get("Node Type") == "Bitmap Heap Scan" and "~~*" in n.get("Recheck Cond", "")) for p in plans}
    print(f"{temp}: n={len(times)} exec_ms median={statistics.median(times):.1f} min={times[0]:.1f} max={times[-1]:.1f} "
          f"blocks_read median={read:.0f} blocks_touched median={total:.0f} text_heap_blocks={sorted(text)}")
EOF
