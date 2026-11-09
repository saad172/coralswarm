#!/usr/bin/env bash
# Rebuilds the four current templates and retained legacy widget and diffs them against the committed
# copies in backend/assets/widgets/ — the follow-up backend task
# `include_str!`s those files directly, so they must always match what
# `widgets/` actually builds. Exits 1 (with a diff) on drift.
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

npm run build >/dev/null

BACKEND_DIR="../backend/assets/widgets"
STATUS=0

for name in ask_ocean search_atoms get_reef recent_activity result-card-v1; do
  built="dist/${name}.html"
  committed="${BACKEND_DIR}/${name}.html"
  if [ ! -f "$committed" ]; then
    echo "MISSING: $committed (run: cp $built $committed)" >&2
    STATUS=1
    continue
  fi
  if ! diff -q "$committed" "$built" >/dev/null; then
    echo "DRIFT: ${name}.html differs from backend/assets/widgets/${name}.html" >&2
    # Straight to stderr — never a predictable /tmp path (a pre-created
    # symlink there could redirect this write to a target the invoking
    # account doesn't intend to overwrite).
    diff -u "$committed" "$built" >&2 || true
    STATUS=1
  fi
done

if [ "$STATUS" -eq 0 ]; then
  echo "widgets/dist/*.html matches backend/assets/widgets/*.html — no drift."
fi

exit "$STATUS"
