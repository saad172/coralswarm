#!/usr/bin/env bash
#
# check_connectors_do_not_answer.sh — enforces contract invariant I11,
# "a connector captures; it does not answer"
# (docs/connectors/CONNECTOR_CONTRACT.md §2, conformance item 10).
#
# THE FAILURE MODE THIS CLOSES. One bot serves every person in a container.
# For it to answer someone out of their private ocean, the request has to
# carry a trustworthy claim about WHICH PERSON is asking — and on a chat
# platform that claim comes from the platform, not from the person. A chat
# platform's bot API cannot give a per-request proof of user identity; it
# vouches (capability C16). So an in-platform ask would make CoralSwarm's
# private-ocean boundary exactly as strong as the customer's own
# workspace-admin model: an admin who can manage users and set profile
# addresses could walk that vouching into oceans the product promises are
# private.
#
# The danger is not that someone ships that deliberately. It is that someone
# adds "just a quick /ask slash command" two years from now, having never
# read I11, in a file nobody thought of as a security boundary. A behavioural
# test can only cover the surfaces that already exist; this is the check that
# covers the one that does not exist yet.
#
# WHAT A GREEN RUN DOES AND DOES NOT PROVE (read before trusting it):
#   * It is a LINT over symbol references, not a call-graph proof. A read
#     reached through a trait object, a channel, a spawned task taking a
#     boxed closure, or a helper living outside the scope below will not be
#     seen. Reviewers still own those shapes per diff.
#   * It says nothing about whether the WRITE paths are correct. I11 is only
#     about reads leaving through a platform.
#   * It starts GREEN — as of the commit that added it, no connector surface
#     references the answer engine at all. That is the point: a guard whose
#     first run is green pins a property that currently holds, and its whole
#     value is the day it goes red.
#
# SCOPE (two halves, deliberately asymmetric — see the manifest header):
#   1. every *.rs under backend/src/connectors/ — globbed, so it grows by
#      itself and a new connector cannot land outside the guard's reach;
#   2. the connector-owned modules under backend/src/routes/ listed in
#      scripts/connector_surfaces.manifest — by hand, because that directory
#      also holds ordinary product routes that legitimately DO answer.
#      A name-pattern sweep cross-checks that list, so a new
#      routes/<platform>*.rs fails until a human classifies it.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MANIFEST="$ROOT/scripts/connector_surfaces.manifest"

# The answer engine's entry points. A connector surface referencing any of
# these is serving, or is one edit away from serving, a read back through its
# own platform.
#
# `crate::retrieval::` is included even though it is "just" scoring helpers:
# a connector has no honest reason to rank a user's atoms, and the shortest
# path to an in-chat ask is to assemble one out of those pieces rather than
# to call the front door.
FORBIDDEN_PATTERNS=(
  'routes::queries::'
  'create_query_stream'
  'create_query'
  'ask_ocean'
  'crate::retrieval::'
  'retrieval::rerank'
)

# Files under backend/src/routes/ whose names look like a connector surface.
# Anything matching this that is NOT in the manifest is an unclassified
# surface and fails — the cross-check that stops the hand-written half of the
# scope from silently going stale.
#
# `gmail_` / `outlook_` (CodeRabbit round 1, #1005, Security & Privacy):
# `routes::gmail_everything_mode`/`routes::outlook_everything_mode` are
# genuine connector-owned surfaces (gsuite Train G2.4 / m365 Train PR 3.4)
# that matched neither this pattern nor the manifest — an unclassified gap
# in this guard's OWN coverage, found by name-pattern review rather than by
# the guard itself, which is exactly the failure mode the cross-check below
# exists to close. Widened rather than special-cased: any FUTURE
# `routes/gmail_*.rs`/`routes/outlook_*.rs` file is now caught by the same
# mechanism instead of needing another manual review to notice it.
ROUTE_NAME_PATTERN='^(slack|teams|google_chat|graph_webhook|google_push|google_calendar_push|gmail|outlook)'

usage() {
  echo "usage: $0 [--self-test]" >&2
}

# ---------------------------------------------------------------------------
# scan_file <path> <label>
# Prints one line per violating match. Full-line `//` comments are stripped
# first, so the long doc comments that EXPLAIN I11 (this codebase writes
# them) are never themselves violations.
# ---------------------------------------------------------------------------
scan_file() {
  local file="$1" label="$2" pattern hits stripped
  stripped="$(sed -E 's!^[[:space:]]*//.*$!!' "$file")"
  for pattern in "${FORBIDDEN_PATTERNS[@]}"; do
    hits="$(printf '%s\n' "$stripped" | grep -n "$pattern" || true)"
    if [[ -n "$hits" ]]; then
      while IFS= read -r line; do
        [[ -z "$line" ]] && continue
        echo "  $label:${line%%:*}  matches /$pattern/"
      done <<< "$hits"
    fi
  done
}

# ---------------------------------------------------------------------------
# --self-test: prove the guard can actually FAIL.
#
# A guard nobody has seen go red is indistinguishable from a guard that
# matches nothing. This plants a real violation in a connector-shaped file
# and asserts the scanner reports it — and asserts the comment-stripping
# still works, so a future fix for a false positive cannot quietly blind it.
# ---------------------------------------------------------------------------
if [[ "${1:-}" == "--self-test" ]]; then
  tmp="$(mktemp -d)"
  trap 'rm -rf "$tmp"' EXIT
  {
    echo '// This doc comment mentions ask_ocean and must NOT count.'
    echo 'pub async fn handle_slash_command(q: &str) -> String {'
    echo '    crate::routes::queries::create_query(q).await'
    echo '}'
  } > "$tmp/fake_connector.rs"
  out="$(scan_file "$tmp/fake_connector.rs" "fake_connector.rs")"
  if [[ -z "$out" ]]; then
    echo "SELF-TEST FAIL: scanner did not flag a planted read path." >&2
    exit 1
  fi
  if grep -q 'matches /ask_ocean/' <<< "$out"; then
    echo "SELF-TEST FAIL: a full-line // comment counted as a violation." >&2
    echo "$out" >&2
    exit 1
  fi
  echo "OK: self-test passed — flags a planted read path, ignores comments."
  exit 0
fi

if [[ $# -gt 0 ]]; then
  usage
  exit 2
fi

if [[ ! -f "$MANIFEST" ]]; then
  echo "FAIL: connector surface manifest not found at $MANIFEST" >&2
  exit 1
fi

manifest_paths="$(grep -vE '^[[:space:]]*(#|$)' "$MANIFEST" || true)"

violations=0
header_printed=0
print_header() {
  if (( header_printed == 0 )); then
    echo "FAIL: a connector surface reaches the retrieval/answer engine." >&2
    echo "" >&2
    echo "Contract invariant I11 — a connector captures; it does not answer." >&2
    echo "A chat platform vouches for who is asking rather than proving it," >&2
    echo "so serving a read here makes the private-ocean boundary only as" >&2
    echo "strong as the customer's workspace-admin model. I11's four lifting" >&2
    echo "conditions are in docs/connectors/CONNECTOR_CONTRACT.md §2." >&2
    echo "" >&2
    header_printed=1
  fi
}

# --- Half 1: every connector source file, globbed ---------------------------
while IFS= read -r -d '' file; do
  rel="${file#"$ROOT"/}"
  out="$(scan_file "$file" "$rel")"
  if [[ -n "$out" ]]; then
    print_header
    echo "$out" >&2
    violations=$((violations + 1))
  fi
done < <(find "$ROOT/backend/src/connectors" -type f -name '*.rs' -print0 2>/dev/null)

# --- Half 2: the hand-listed connector route modules ------------------------
while IFS= read -r rel; do
  [[ -z "$rel" ]] && continue
  path="$ROOT/$rel"
  if [[ ! -f "$path" ]]; then
    print_header
    echo "  stale manifest entry — file does not exist: $rel" >&2
    violations=$((violations + 1))
    continue
  fi
  out="$(scan_file "$path" "$rel")"
  if [[ -n "$out" ]]; then
    print_header
    echo "$out" >&2
    violations=$((violations + 1))
  fi
done <<< "$manifest_paths"

# --- Cross-check: no unclassified connector-shaped route module -------------
# This is what stops the hand-written half of the scope from going stale. A
# new `routes/slack_something.rs` is inside the guard the day it lands, not
# the day someone remembers to list it.
unclassified=0
while IFS= read -r -d '' file; do
  rel="${file#"$ROOT"/}"
  base="$(basename "$file")"
  if [[ ! "$base" =~ $ROUTE_NAME_PATTERN ]]; then
    continue
  fi
  if grep -qxF "$rel" <<< "$manifest_paths"; then
    continue
  fi
  if (( unclassified == 0 )); then
    echo "FAIL: unclassified connector-shaped route module(s)." >&2
    echo "" >&2
    echo "These look like connector surfaces by name but are not listed in" >&2
    echo "scripts/connector_surfaces.manifest, so I11 is not enforced on" >&2
    echo "them. Add each to the manifest (or rename it if it is not in fact" >&2
    echo "a connector surface)." >&2
    echo "" >&2
    unclassified=1
  fi
  echo "  $rel" >&2
done < <(find "$ROOT/backend/src/routes" -maxdepth 1 -type f -name '*.rs' -print0 2>/dev/null)

if (( violations > 0 || unclassified > 0 )); then
  exit 1
fi

echo "OK: no connector surface reaches the retrieval/answer engine (I11)."
