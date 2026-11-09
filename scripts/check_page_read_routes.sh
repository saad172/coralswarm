#!/usr/bin/env bash
# Database/toolchain-free declaration guard. Observed mount reachability is
# proved by page-load-budget.spec.ts, not by claiming regex proves a call graph.
set -euo pipefail
cd "$(dirname "$0")/.."
exec python3 scripts/check_page_read_routes.py "$@"
