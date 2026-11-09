#!/usr/bin/env bash
#
# check_startup_parameters.sh — no Postgres STARTUP-packet parameter outside
# Neon's pooler allowlist.
#
# WHY THIS EXISTS. Neon's pooler accepts exactly five startup parameters and
# rejects anything else outright:
#
#     unsupported startup parameter in options: <name>.
#     Please use unpooled connection...
#
# The pool then fails to BUILD. Not "the setting is ignored" — no connection is
# established at all, so every query against that tenant fails.
#
# This has already cost production once. `app.tenant_actor=system` was set
# through the startup packet on the worker pool; Neon refused it, and every org
# tenant's background work (embed, extract, summary, …) was dead — surfacing
# only as a log line, which is why it went unnoticed. PR #388 removed it and
# moved the GUC to a per-transaction `SET LOCAL`.
#
# It then nearly cost production a second time, in the opposite direction: W2
# added `statement_timeout` the same way, on the REQUEST pool. Same rejection,
# same silent-build-failure, but on the path that serves users. Caught in
# review, not by anything automated. This script is the automated thing.
#
# THE RULE: set session state with `SET LOCAL` inside an already-open
# transaction (see `tenancy::scope`), never through the startup packet. A
# pooler hands out transactions, so transaction-local state is the only kind
# that survives it.
#
# WHAT A GREEN RUN DOES AND DOES NOT PROVE:
#   * It catches the literal `.options([...])` builder call and an `options=-c`
#     fragment embedded in a DSN string in the tree. It cannot see a DSN
#     supplied at RUNTIME — an operator putting `?options=-c%20statement_timeout`
#     into a secret would not be caught here, and no static check could.
#   * The allowlist below is Neon's, verified against their connection-pooling
#     and connection-errors docs. A different managed pooler may allow more;
#     this guard is deliberately calibrated to the one actually in use.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# Neon pooler allowlist. https://neon.com/docs/connect/connection-errors
ALLOWED='client_encoding|datestyle|timezone|standard_conforming_strings|application_name'

fail=0

# 1. The sqlx builder form: `PgConnectOptions::options([("name", "value")])`.
while IFS= read -r hit; do
  [[ -z "$hit" ]] && continue
  file="${hit%%:*}"
  case "$file" in */check_startup_parameters.sh) continue ;; esac
  # EVERY tuple, not just the first. `.options([("application_name", "cs"),
  # ("statement_timeout", "10000")])` would otherwise hide a rejected option
  # behind an allowlisted one — and the rejection is all-or-nothing, so the
  # pool still fails to build.
  names="$(printf '%s' "$hit" | perl -ne 'while (/\(\s*"([^"]+)"\s*,/g) { print "$1\n" }')"
  if [[ -z "$names" ]]; then
    # A non-literal key (a constant, a variable) cannot be checked statically.
    # Refuse it rather than wave it through — the whole failure mode here is
    # silent.
    echo "FAIL: startup-parameter call with a non-literal name:" >&2
    echo "  $hit" >&2
    echo "  Use a per-transaction SET LOCAL instead (see tenancy::scope)." >&2
    fail=$((fail + 1))
    continue
  fi
  while IFS= read -r name; do
    [[ -z "$name" ]] && continue
    [[ "$name" =~ ^($ALLOWED)$ ]] && continue
    echo "FAIL: '$name' is not on Neon's pooler startup-parameter allowlist." >&2
    echo "  $hit" >&2
    echo "" >&2
    echo "  Neon accepts only: ${ALLOWED//|/, }" >&2
    echo "  Anything else is REJECTED and the pool fails to build, so every" >&2
    echo "  query against that tenant fails. Set it per transaction with" >&2
    echo "  SET LOCAL instead — see tenancy::scope::set_request_statement_timeout" >&2
    echo "  and PR #388." >&2
    fail=$((fail + 1))
  done <<< "$names"
done < <(grep -rn --include='*.rs' -E '\.options\(\[' "$ROOT/backend/src" "$ROOT/packages" 2>/dev/null || true)

# 2. An `options=-c …` fragment baked into a DSN in source or config.
while IFS= read -r hit; do
  [[ -z "$hit" ]] && continue
  file="${hit%%:*}"
  text="${hit#*:}"; text="${text#*:}"
  # Prose describing the hazard is not the hazard: skip docs, skip this guard
  # itself (its own comments quote the pattern), and skip comment lines.
  case "$file" in
    *docs/*|*.md) continue ;;
    */check_startup_parameters.sh) continue ;;
  esac
  case "$(printf '%s' "$text" | sed 's/^[[:space:]]*//')" in
    "#"*|"//"*) continue ;;   # `//` also covers `///` and `//!`
  esac
  echo "FAIL: a DSN carries startup options, which Neon's pooler rejects:" >&2
  echo "  $hit" >&2
  fail=$((fail + 1))
done < <(grep -rn --include='*.rs' --include='*.toml' --include='*.sh' -E 'options=-c|options=%2D' "$ROOT" 2>/dev/null || true)

if (( fail > 0 )); then
  exit 1
fi

echo "OK: no Postgres startup parameter outside Neon's pooler allowlist."
