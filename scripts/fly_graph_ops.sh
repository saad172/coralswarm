#!/usr/bin/env bash
# One-shot Fly machines for G1 (`backfill_provenance_links`) and G4
# (`fold_subtopics`). Uses the current `coralswarm-backend` image; does not
# change the serving CMD. Default is dry-run.
#
# Parallelism is one machine per `--graph-id` (safe for G1; required for G4
# — never two folds on the same ocean). Same-graph extra instances do not
# speed G1 (one SQL statement) and are refused for G4.
#
# The binaries ship in the image starting with the deploy that includes this
# script. An older image fails with "exec: no such file".
#
# Usage:
#   scripts/fly_graph_ops.sh g1 [--execute] [--graph-id UUID]...
#   scripts/fly_graph_ops.sh g4 [--execute --i-am-sure] [--graph-id UUID]... [--limit N]
#
# Env:
#   FLY_APP           default coralswarm-backend
#   FLY_REGION        default iad
#   GRAPH_OPS_IMAGE   override image (default: current app image)
#   GRAPH_OPS_DSN     if set, passed as MIGRATOR_DATABASE_URL (org Neon, etc.).
#                     Otherwise the machine inherits the app secret (consumer
#                     plane). Never printed.
#   GRAPH_OPS_MEMORY  VM memory MB, default 1024
set -euo pipefail

APP="${FLY_APP:-coralswarm-backend}"
REGION="${FLY_REGION:-iad}"
MEMORY="${GRAPH_OPS_MEMORY:-1024}"

usage() {
  sed -n '2,22p' "$0" | sed 's/^# \{0,1\}//'
  exit 2
}

cmd=""
execute=0
i_am_sure=0
limit=""
graph_ids=()

[[ $# -ge 1 ]] || usage
case "$1" in
  g1 | g4) cmd=$1; shift ;;
  -h | --help) usage ;;
  *) echo "unknown command: $1" >&2; usage ;;
esac

while [[ $# -gt 0 ]]; do
  case "$1" in
    --execute) execute=1; shift ;;
    --dry-run) execute=0; shift ;;
    --i-am-sure) i_am_sure=1; shift ;;
    --limit)
      [[ $# -ge 2 ]] || { echo "--limit requires N" >&2; exit 2; }
      [[ "$cmd" == g4 ]] || { echo "--limit is g4-only" >&2; exit 2; }
      limit=$2
      shift 2
      ;;
    --graph-id)
      [[ $# -ge 2 ]] || { echo "--graph-id requires a UUID" >&2; exit 2; }
      graph_ids+=("$2")
      shift 2
      ;;
    -h | --help) usage ;;
    *) echo "unknown flag: $1" >&2; usage ;;
  esac
done

if [[ "$cmd" == g4 && "$execute" -eq 1 && "$i_am_sure" -ne 1 ]]; then
  echo "g4 --execute requires --i-am-sure (same gate as the binary)." >&2
  exit 2
fi

if [[ ${#graph_ids[@]} -gt 1 ]]; then
  uniq=$(printf '%s\n' "${graph_ids[@]}" | sort | uniq -d)
  if [[ -n "$uniq" ]]; then
    echo "refusing duplicate --graph-id (would race on one ocean): $uniq" >&2
    exit 2
  fi
fi

image="${GRAPH_OPS_IMAGE:-}"
if [[ -z "$image" ]]; then
  image=$(fly image show -a "$APP" --json | python3 -c '
import json, sys
rows = json.load(sys.stdin)
if not rows:
    sys.exit("no images on app")
r = rows[0]
print(f"{r[\"Registry\"]}/{r[\"Repository\"]}:{r[\"Tag\"]}")
')
fi

bin=/app/backfill_provenance_links
[[ "$cmd" == g4 ]] && bin=/app/fold_subtopics

mode=DRY-RUN
[[ "$execute" -eq 1 ]] && mode=EXECUTE

echo "fly_graph_ops  cmd=$cmd  mode=$mode  app=$APP  image=$image"
if [[ ${#graph_ids[@]} -eq 0 ]]; then
  echo "  graphs: ALL on this DSN (one machine)"
else
  echo "  graphs: ${graph_ids[*]}  (${#graph_ids[@]} machine(s))"
fi
if [[ -n "${GRAPH_OPS_DSN:-}" ]]; then
  echo "  dsn: GRAPH_OPS_DSN override (not printed)"
else
  echo "  dsn: inherit $APP secrets (MIGRATOR_DATABASE_URL)"
fi

run_one() {
  local gid="${1:-}"
  local args=()
  [[ "$execute" -eq 1 ]] && args+=(--execute)
  [[ "$cmd" == g4 && "$i_am_sure" -eq 1 ]] && args+=(--i-am-sure)
  [[ -n "$gid" ]] && args+=(--graph-id "$gid")
  [[ -n "$limit" ]] && args+=(--limit "$limit")

  local env_args=()
  if [[ -n "${GRAPH_OPS_DSN:-}" ]]; then
    env_args+=(-e "MIGRATOR_DATABASE_URL=${GRAPH_OPS_DSN}")
  fi

  echo "--- machine  ${gid:-all-graphs}  ${args[*]:-}"
  fly machine run "$image" \
    --app "$APP" \
    --region "$REGION" \
    --rm \
    --skip-dns-registration \
    --autostart=false \
    --vm-memory "$MEMORY" \
    "${env_args[@]}" \
    -- "$bin" "${args[@]}"
}

if [[ ${#graph_ids[@]} -eq 0 ]]; then
  run_one ""
  exit 0
fi

if [[ ${#graph_ids[@]} -eq 1 ]]; then
  run_one "${graph_ids[0]}"
  exit 0
fi

# One machine per ocean. G1: independent INSERTs. G4: required isolation.
fail=0
pids=()
for gid in "${graph_ids[@]}"; do
  run_one "$gid" &
  pids+=($!)
done
for pid in "${pids[@]}"; do
  if ! wait "$pid"; then
    fail=1
  fi
done
exit "$fail"
