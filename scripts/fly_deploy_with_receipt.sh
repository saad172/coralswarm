#!/usr/bin/env bash
# Preserve the immutable image identity even when the rollout fails.
set -euo pipefail
app=$1
receipt_dir=$2
shift 2
script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
mkdir -p "$receipt_dir"
log_file=$(mktemp)
trap 'rm -f "$log_file"' EXIT
for attempt in 1 2 3; do
  set +e
  flyctl deploy "$@" 2>&1 | tee "$log_file"
  result=${PIPESTATUS[0]}
  set -e
  # Missing/ambiguous digest must never produce a successful acceptance receipt.
  rm -f "$receipt_dir/candidate.json"
  python3 "$script_dir/deployment.py" candidate --app "$app" --log "$log_file" --receipt "$receipt_dir/candidate.json" || true
  if [ "$result" -eq 0 ]; then
    test -f "$receipt_dir/candidate.json"
    exit 0
  fi
  # VM/lease failures are rollout failures, not reasons to rebuild an image.
  if grep -Eq 'failed to update (machine|VM)|bolt token:|failed to acquire lease' "$log_file"; then
    echo '::error::Fly rollout failed; retain candidate receipt and use bounded web recovery after diagnosis.'
    exit "$result"
  fi
  echo "::warning::flyctl build/deploy attempt $attempt failed"
  if [ "$attempt" -lt 3 ]; then sleep $((attempt * 30)); fi
done
exit 1
