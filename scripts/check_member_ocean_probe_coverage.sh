#!/usr/bin/env bash
#
# check_member_ocean_probe_coverage.sh — member-private-oceans PR 11's
# regression-proofing gate (docs/plans/member-private-oceans.md §6: "Probe
# 2's table should be derived from a checked-in list of every ocean-resolving
# route, so adding a route without a probe fails CI — the spirit of
# check_rls_coverage.sh").
#
# WHAT THIS SCRIPT IS: the ROUTE half of that requirement. It mechanically
# parses every `.route("path", ...)` registration inside
# `backend/src/lib.rs`'s `v1_routes()` — the `/v1` route table the cloud
# (`app`) router mounts — and cross-checks
# the resulting roster against a checked-in manifest,
# `backend/tests/member_ocean_probe_routes.manifest`, exactly the way
# check_rls_coverage.sh cross-checks its own table roster against
# scripts/rls_coverage.manifest:
#
#   1. The route roster is derived AT RUNTIME from `v1_routes()` itself, so
#      it can never drift from the actual router.
#   2. Every route must appear in the manifest with a status:
#        probed  — a test in backend/tests/member_ocean_probe_pg.rs (or a
#                  sibling *_pg.rs file named in the comment above the entry)
#                  exercises this exact path against the member-ocean fixture.
#        pending — genuinely ocean/graph-resolving, not yet given its own
#                  dedicated assertion in this suite (may still be covered
#                  structurally by the same ReachabilityResolver predicate
#                  proven correct elsewhere in the file — see the comment
#                  above each such entry).
#        exempt  — not ocean/graph-content-scoped at all (account-wide
#                  settings, connector OAuth plumbing, team-roster CRUD,
#                  etc.) — REQUIRES a `# exempt: <why>` comment line directly
#                  above it, same discipline as check_rls_coverage.sh's own
#                  exempt entries.
#   3. FAILURES: a route registered in `v1_routes()` that is absent from the
#      manifest (the exact "quietly ships, nobody notices" gap
#      reachability_pg.rs sat in for months, per this PR's brief); an
#      `exempt` entry with no rationale comment; a malformed/duplicate line;
#      a manifest entry whose route no longer exists (NOTE only, not a
#      failure — see check_rls_coverage.sh's identical reasoning for why a
#      grep can't tell "route removed" from "route renamed").
#
# WHAT A GREEN RUN DOES AND DOES NOT PROVE (same honesty bar as
# check_rls_coverage.sh/check_no_ambient_pool.sh):
#   * This is a grep over `v1_routes()`'s literal `.route(...)` calls, not a
#     data-flow proof. It cannot tell whether a `probed` entry's claimed test
#     ACTUALLY exercises that path, or whether a `pending`/`exempt`
#     classification is correct — those are reviewer-owned judgments, exactly
#     like check_rls_coverage.sh's `# converted:`/`# exempt:` comments.
#   * It only covers the ONE shared HTTP route table (`v1_routes()`). The
#     `/mcp` tool surface is registered completely differently (an `rmcp`
#     `#[tool_router]` macro over annotated methods, not a textual `.route()`
#     table) and has NO equivalent mechanical parse here — see
#     `member_ocean_probe_pg.rs`'s own module doc for the separate (weaker)
#     mechanism that covers MCP tools instead, and its own honesty note about
#     what that does and doesn't catch.
#   * Dynamically-built paths (there are none in `v1_routes()` today — every
#     `.route()` call uses a literal string) would be invisible to this
#     grep, same caveat as every sibling *_pg.rs coverage gate.
#
# Usage:
#   scripts/check_member_ocean_probe_coverage.sh           # CI mode
#   scripts/check_member_ocean_probe_coverage.sh --seed     # (re)generate the
#                                           # manifest; existing statuses
#                                           # preserved, new routes added as
#                                           # "pending", stale entries dropped
#
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

LIB_RS="backend/src/lib.rs"
MANIFEST="backend/tests/member_ocean_probe_routes.manifest"

MODE="verify"
[ "${1:-}" = "--seed" ] && MODE="seed"

fail=0
err() { echo "FAIL: $*" >&2; fail=1; }

if [ ! -f "$LIB_RS" ]; then
  echo "FAIL: $LIB_RS not found (run from the repo root)" >&2
  exit 1
fi

# ── 1. Derive the route roster from v1_routes() itself ──────────────────────
START_LINE=$(grep -n '^fn v1_routes() -> Router<Arc<AppState>> {' "$LIB_RS" | head -n1 | cut -d: -f1)
if [ -z "$START_LINE" ]; then
  echo "FAIL: could not find 'fn v1_routes() -> Router<Arc<AppState>> {' in $LIB_RS —" >&2
  echo "the parser anchors on that exact signature; fix the parser (or the function's" >&2
  echo "signature) before trusting this gate." >&2
  exit 1
fi
# The function's closing brace: the next line that is EXACTLY "}" at column 0
# after START_LINE. `v1_routes()` is one flat `Router::new()...` method chain
# with no nested top-level braces, so this is unambiguous today; if that ever
# changes, this parser needs to change with it (same caveat
# check_rls_coverage.sh's own anchor-line parsing carries).
END_OFFSET=$(tail -n "+$START_LINE" "$LIB_RS" | grep -n '^}' | head -n1 | cut -d: -f1)
if [ -z "$END_OFFSET" ]; then
  echo "FAIL: could not find v1_routes()'s closing brace — fix the parser." >&2
  exit 1
fi
END_LINE=$((START_LINE + END_OFFSET - 1))

# Flatten to one line (multi-line `.route(\n "path",\n ...)` registrations —
# most of them — would otherwise split the path string onto a different line
# than `.route(`, same reasoning as check_rls_coverage.sh's own `flatten()`).
FLAT=$(sed -n "${START_LINE},${END_LINE}p" "$LIB_RS" | tr '\n' ' ')
ROUTES=$(echo "$FLAT" | grep -oE '\.route\([[:space:]]*"[^"]*"' \
  | sed -E 's/^\.route\([[:space:]]*"//; s/"$//' | sort -u)
ROUTE_COUNT=$(echo "$ROUTES" | grep -c . || true)
if [ "$ROUTE_COUNT" -lt 50 ]; then
  echo "FAIL: parsed only $ROUTE_COUNT routes from v1_routes() — the .route(\"...\") pattern" >&2
  echo "this script greps for no longer matches; fix the parser (or the file's formatting)" >&2
  echo "before trusting this gate." >&2
  exit 1
fi

# ── seed mode ─────────────────────────────────────────────────────────────────
if [ "$MODE" = "seed" ]; then
  {
    echo "# member-ocean probe route-coverage manifest — consumed by"
    echo "# scripts/check_member_ocean_probe_coverage.sh. Format: <path> <status>"
    echo "# status: probed | pending | exempt (exempt requires a '# exempt: <why>' comment"
    echo "# directly above it). Every route registered in backend/src/lib.rs's"
    echo "# v1_routes() must be listed. Regenerate with:"
    echo "#   scripts/check_member_ocean_probe_coverage.sh --seed"
    while IFS= read -r route; do
      status="pending"
      if [ -f "$MANIFEST" ]; then
        prev=$(awk -v r="$route" '$1 == r { print $2 }' "$MANIFEST" | head -n1)
        [ -n "$prev" ] && status="$prev"
      fi
      if [ "$status" = "exempt" ]; then
        # NOT `grep -F -B1 "^$route exempt$"` (a prior version of this
        # script): `-F` treats the whole pattern as a LITERAL string, so `^`
        # and `$` never anchor anything and the pattern can never match a
        # real manifest line — every existing exemption would lose its
        # rationale on every reseed. awk does an exact whole-line match
        # instead and prints the line immediately above it.
        prev_comment=$(awk -v entry="$route exempt" '
          $0 == entry { print previous; exit }
          { previous = $0 }
        ' "$MANIFEST" 2>/dev/null)
        case "$prev_comment" in
          '# exempt:'*) echo "$prev_comment" ;;
          *) echo "# exempt: (rationale lost by --seed — restore it from git history)" ;;
        esac
      fi
      echo "$route $status"
    done < <(echo "$ROUTES")
  } > "$MANIFEST.tmp"
  mv "$MANIFEST.tmp" "$MANIFEST"
  echo "seeded $MANIFEST ($(grep -cE '^[^#]' "$MANIFEST" || true) entries, route roster: $ROUTE_COUNT)"
  exit 0
fi

# ── 2. Verify against the manifest ───────────────────────────────────────────
if [ ! -f "$MANIFEST" ]; then
  echo "FAIL: $MANIFEST missing — run scripts/check_member_ocean_probe_coverage.sh --seed" >&2
  exit 1
fi

manifest_norm="$(mktemp)"
trap 'rm -f "$manifest_norm"' EXIT

lineno=0
prev_line=""
while IFS= read -r line; do
  lineno=$((lineno + 1))
  case "$line" in ''|'#'*) prev_line="$line"; continue ;; esac
  route=$(echo "$line" | awk '{print $1}')
  status=$(echo "$line" | awk '{print $2}')
  extra=$(echo "$line" | awk '{print $3}')
  if [ -z "$route" ] || [ -z "$status" ] || [ -n "$extra" ]; then
    err "manifest line $lineno malformed (want '<path> <status>'): $line"
    continue
  fi
  if [ "$status" != "probed" ] && [ "$status" != "pending" ] && [ "$status" != "exempt" ]; then
    err "manifest line $lineno: unknown status '$status' for $route (want probed|pending|exempt)"
    continue
  fi
  if [ "$status" = "exempt" ]; then
    rationale="${prev_line#\# exempt:}"
    # Reject BOTH a missing "# exempt:" prefix (rationale == prev_line, the
    # substitution was a no-op) AND a present-but-blank/whitespace-only
    # rationale ("# exempt:" or "# exempt:   ") — the manifest's own contract
    # is "# exempt: <why>", and an empty <why> lets a graph-resolving route
    # be excluded from every probe with nothing for a reviewer to check.
    if [ "$rationale" = "$prev_line" ] || [ -z "$(printf '%s' "$rationale" | tr -d '[:space:]')" ]; then
      err "manifest line $lineno: $route is 'exempt' but the line above is not a '# exempt: <why>' comment with a non-blank <why>"
      continue
    fi
  fi
  if grep -qxF "$route" <(awk '{print $1}' "$manifest_norm" 2>/dev/null); then
    err "manifest line $lineno: duplicate entry for $route"
    continue
  fi
  echo "$route $status" >> "$manifest_norm"
  prev_line="$line"
done < "$MANIFEST"

# 2a. Every route in v1_routes() must be in the manifest.
while IFS= read -r route; do
  status=$(awk -v r="$route" '$1 == r { print $2 }' "$manifest_norm")
  if [ -z "$status" ]; then
    err "route '$route' is registered in $LIB_RS's v1_routes() but is NOT in $MANIFEST —
      a new (or newly-detected) ocean-resolving route must be reviewed and added, e.g.:
        echo \"$route pending\" >> $MANIFEST
      (or run scripts/check_member_ocean_probe_coverage.sh --seed), then add a probe row
      to backend/tests/member_ocean_probe_pg.rs and flip the status to 'probed'."
  fi
done < <(echo "$ROUTES")

# 2b. Manifest entries whose route no longer exists in v1_routes(): a notice,
#     not a failure (a route can be renamed/removed; this can't tell that
#     apart from "route now built dynamically").
while read -r route _status; do
  if ! echo "$ROUTES" | grep -qxF "$route"; then
    echo "note: $route is in $MANIFEST but is no longer registered in v1_routes() — remove" \
         "the entry if the route is truly gone."
  fi
done < "$manifest_norm"

probed=$(awk '$2 == "probed"' "$manifest_norm" | wc -l | tr -d ' ')
pending=$(awk '$2 == "pending"' "$manifest_norm" | wc -l | tr -d ' ')
exempt=$(awk '$2 == "exempt"' "$manifest_norm" | wc -l | tr -d ' ')
total=$((probed + pending + exempt))
echo
echo "member-ocean probe route coverage: $probed/$total probed ($pending pending, $exempt exempt;" \
     "route roster: $ROUTE_COUNT from v1_routes())"

if [ "$fail" -ne 0 ]; then
  echo "member-ocean-probe-coverage gate: FAILED" >&2
  exit 1
fi
echo "member-ocean-probe-coverage gate: OK (grep-strength check — see header for what a green run does not prove; MCP tool coverage is a SEPARATE mechanism, see member_ocean_probe_pg.rs's module doc)"
