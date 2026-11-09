#!/usr/bin/env bash
#
# check_topics_no_ambient_ocean.sh — regression guard for the topics-page
# ocean-derivation fix (fix/topics-derive-ocean).
#
# THE BUG THIS PINS. `/topics?id=<nodeId>` used to resolve which ocean to
# query from ambient session state (`useApp().currentGraph`) rather than
# from the reef the page was actually asked to show — so a topic living in
# any ocean OTHER than whichever one `pickActiveOcean` happened to select at
# boot rendered "That topic isn't in this workspace" for a topic that
# plainly was in a (different) workspace. There is no workspace switcher
# (`setCurrentGraphId` in web/lib/providers.tsx has zero callers outside that
# file), so that failure was not fixable by any user action — only by this
# page reading its ocean from somewhere else.
#
# THE FIX THIS PROTECTS: derive the ocean from the reef. `api.reefs.get`
# (GET /v1/nodes/{id}) returns the reef's own `graph_id`; every ocean-scoped
# read on the page is keyed off THAT, never off the ambient "current ocean"
# field. This guard forbids the one identifier that carried the ambient
# value onto this page from ever appearing there again.
#
# WHAT A GREEN RUN DOES AND DOES NOT PROVE — read before trusting it:
#   * It is a grep, not a data-flow proof (same caveat
#     check_no_unbounded_fanout.sh states for its own pattern). A regression
#     that reintroduced ambient-ocean behaviour through a DIFFERENT name — a
#     fresh destructure alias, a prop threaded down from a parent that itself
#     reads ambient state — would not be caught here.
#   * Word-boundary matched, and full-line `//` comments are stripped first
#     (same convention as check_no_unbounded_fanout.sh), so a doc comment
#     that merely EXPLAINS the fix, or an unrelated identifier that happens
#     to share the prefix (`currentGraphId` — PromoteModal's own prop name,
#     untouched by that fix), is never a violation.
#   * It watches ONLY this one file. `/session` and `/meeting` share the
#     underlying defect (ambient ocean resolution from a bare node/session
#     id) and are explicitly NOT covered — filed as separate follow-ups, not
#     silently included here.
#
# HOW THIS IS RUN. There is no CI on pull requests in this repo right now
# (see scripts/ci_local.py's own header: GitHub Actions runs only
# cron/deploy workflows). This script is deliberately NOT wired into
# ci.yml, scripts/ci_local.py, or scripts/githooks/pre-push — none of those
# are touched by the PR that added this guard. It is by-hand only:
#
#   bash scripts/check_topics_no_ambient_ocean.sh
#
# If this repo's local-verification story changes (CI comes back, or another
# script becomes the thing everyone actually runs), wire this in there — an
# unenforced guard that nobody knows to run is only a comment with extra
# steps.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TARGET="$ROOT/web/app/(app)/topics/page.tsx"

# Guard the guard: a target that's missing or renamed must fail LOUDLY, not
# silently report a meaningless "0 matches" pass (the same principle
# check_no_unbounded_fanout.sh applies to its own allowlist staleness check).
if [[ ! -f "$TARGET" ]]; then
  echo "FAIL: guard target not found at $TARGET" >&2
  echo "      web/app/(app)/topics/page.tsx moved or was renamed — update" >&2
  echo "      this script's TARGET path, don't just delete the guard." >&2
  exit 1
fi

# Strip full-line `//` comments first (mirrors check_no_unbounded_fanout.sh)
# so a doc comment that NAMES the forbidden field to explain why it's
# forbidden doesn't trip the guard it's documenting.
stripped="$(grep -vE '^[[:space:]]*//' "$TARGET")"

# Word-boundary matched by hand (`[^A-Za-z0-9_]` on each side, or start/end
# of line) rather than a bare substring grep: `currentGraphId` — PromoteModal's
# own prop, which this page legitimately still passes `reef.graph_id` into —
# contains `currentGraph` as a PREFIX and must not trip this.
matches="$(printf '%s\n' "$stripped" | grep -E '(^|[^A-Za-z0-9_])currentGraph([^A-Za-z0-9_]|$)' || true)"

if [[ -n "$matches" ]]; then
  echo "FAIL: web/app/(app)/topics/page.tsx reads the ambient 'currentGraph'" >&2
  echo "      field again." >&2
  echo "" >&2
  echo "This page must derive its ocean from the reef being shown" >&2
  echo "(api.reefs.get's graph_id), never from useApp()'s ambient ocean" >&2
  echo "selection — see the file's own header comment." >&2
  echo "" >&2
  echo "$matches" >&2
  exit 1
fi

echo "OK: web/app/(app)/topics/page.tsx never reads ambient currentGraph."
