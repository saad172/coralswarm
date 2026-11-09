#!/usr/bin/env bash
#
# cut_release.sh — start the Release workflow from a terminal.
#
#   scripts/cut_release.sh [--hotfix] [--dry-run] [--force] [--sha SHA] [--no-watch]
#
# The release itself runs entirely on GitHub (.github/workflows/release.yml;
# runbook docs/runbooks/release.md). This script only dispatches that
# workflow on main and watches it, the same as the "Run workflow" button.
# It prepares nothing on this machine. The workflow computes the tag.
#
set -euo pipefail

usage() {
  sed -n '5p' "$0" | sed 's/^# \{0,1\}//' >&2
  exit 2
}

args=()
WATCH=1
while [ $# -gt 0 ]; do
  case "$1" in
    --hotfix) args+=(-f hotfix=true); shift ;;
    --dry-run) args+=(-f dry_run=true); shift ;;
    --force) args+=(-f force=true); shift ;;
    --sha) args+=(-f "sha=${2:?--sha needs a commit}"); shift 2 ;;
    --no-watch) WATCH=0; shift ;;
    -h|--help) usage ;;
    server-*) echo "error: the Release workflow chooses the tag; drop '$1'" >&2; exit 2 ;;
    *) echo "unknown argument: $1" >&2; usage ;;
  esac
done

command -v gh >/dev/null || { echo "error: gh is required" >&2; exit 1; }
REPO="$(gh repo view --json nameWithOwner --jq .nameWithOwner)"

started="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
gh workflow run release.yml --repo "$REPO" --ref main ${args[@]+"${args[@]}"}
run_id=""
for _ in $(seq 1 30); do
  run_id="$(gh run list --repo "$REPO" --workflow release.yml --event workflow_dispatch --branch main --limit 5 \
    --json databaseId,createdAt --jq "[.[] | select(.createdAt >= \"$started\")] | first | .databaseId // empty")"
  [ -z "$run_id" ] || break
  sleep 5
done
[ -n "$run_id" ] || { echo "error: the Release run did not appear; check: gh run list --workflow release.yml" >&2; exit 1; }
echo "==> Release run: $(gh run view "$run_id" --repo "$REPO" --json url --jq .url)"
[ "$WATCH" = 1 ] || exit 0
gh run watch "$run_id" --repo "$REPO" --interval 30 --exit-status
echo "==> Release finished; its summary links the Deploy run (staging, smoke, production)."
