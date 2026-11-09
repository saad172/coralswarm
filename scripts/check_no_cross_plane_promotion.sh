#!/usr/bin/env bash
#
# check_no_cross_plane_promotion.sh — the "promotion never crosses a database"
# grep-guard.
#
# THE INVARIANT: content moves between oceans only WITHIN one database. An
# org-plane promotion's source must live in that org's own tenant DB (the
# caller's own `member` ocean); a source on the shared consumer plane — most
# obviously the caller's personal ocean — is refused with 403 by
# `route_org_promotion` in backend/src/routes/nodes.rs.
#
# WHY A GUARD AND NOT JUST THE 403: the deleted `services::promotion` was a
# perfectly legitimate-looking module. It held a consumer pool handle, read a
# personal ocean through it, decrypted with the personal DEK, re-encrypted with
# the org DEK, and wrote into a tenant DB — every step reviewed and covered by
# tests. Nothing about it looked like a boundary violation locally; it only
# looked wrong once you asked "which databases does one request touch?". A
# future feature can reintroduce exactly that shape just as innocently, so the
# check is on the SHAPE (a consumer-plane pool handle reachable from request-
# serving code), not on any one function name.
#
# WHAT IT SCANS: `consumer_pool` / `consumer_store` identifiers in CODE under
# backend/src/routes/, backend/src/services/, and backend/src/mcp/ — the
# trees that serve requests (the last one added once the MCP `promote` tool
# gave backend/src/mcp/ its own request-serving call site into
# `routes::nodes::promote_node`). A hit means some request path has a handle
# to the shared consumer plane while it may be serving an org tenant.
#
# WHAT A GREEN RUN DOES AND DOES NOT PROVE (read before trusting it):
#   * It is a LINT on one specific, historically-real shape, NOT a data-flow
#     proof. A rebind (`let p = deps.consumer_pool; p`), a differently-named
#     field, or a pool passed in as an opaque `&PgPool` argument all evade it.
#     Reviewers still own those shapes per-diff — the question to ask of any
#     promotion/copy/merge diff is "how many databases does one request touch?"
#   * It deliberately does NOT scan backend/src/workers/. The per-tenant worker
#     harness (workers/tenant_harness.rs) legitimately holds handles to every
#     plane at once — that is its whole job, it runs OUTSIDE any single
#     request, and it dispatches each job to the pool that job belongs to.
#   * Control-plane IDENTITY reads (membership, accounts, users) are a
#     different question from CONTENT movement and are not what this guard is
#     about. They go through the named `state.control_db()` /
#     `state.control_store()` accessors, which `check_no_ambient_pool.sh`
#     documents as control-table-only.
#
# Usage:
#   scripts/check_no_cross_plane_promotion.sh   # CI mode; non-zero on violation
#
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

SCAN_DIRS=(backend/src/routes backend/src/services backend/src/mcp)

for dir in "${SCAN_DIRS[@]}"; do
  if [ ! -d "$dir" ]; then
    echo "FAIL: $dir does not exist (run from the repo root)" >&2
    exit 1
  fi
done

violations_file="$(mktemp)"
trap 'rm -f "$violations_file"' EXIT

# CODE only, never comments — the rationale above names `consumer_pool` many
# times over, and so do the doc comments in the files this scans. Same
# comment-stripping logic as check_no_ambient_pool.sh: skip full-line comments,
# strip a trailing `//...` only when the `//` is not inside a string literal.
scan_file() {
  local file="$1"
  # The filename is passed through the environment ($ENV{SCAN_FILE}), not
  # shell-interpolated into the perl program body — a filename containing a
  # single quote would otherwise break out of the perl -ne string and inject
  # into the program itself.
  SCAN_FILE="$file" perl -ne '
    my $line = $_;
    next if $line =~ m{^\s*//};                 # full-line (doc) comment
    my $i = index($line, "//");
    if ($i >= 0) {
      my $prefix = substr($line, 0, $i);
      my $quotes = () = $prefix =~ /"/g;
      $line = $prefix if $quotes % 2 == 0;      # trailing comment, not in-string
    }
    if ($line =~ /\bconsumer_(?:pool|store)\b/) {
      chomp(my $shown = $_);
      print "$ENV{SCAN_FILE}:$.: $shown\n";
    }
    close ARGV if eof;                          # reset $. per file
  ' "$file"
}

while IFS= read -r file; do
  scan_file "$file" >> "$violations_file"
done < <(find "${SCAN_DIRS[@]}" -name '*.rs' -type f | sort)

if [ -s "$violations_file" ]; then
  echo "FAIL: a consumer-plane pool/store handle is reachable from request-serving code" >&2
  echo >&2
  cat "$violations_file" >&2
  echo >&2
  cat >&2 <<'EOF'
Promotion — and content movement generally — never crosses a database. An
org-bound request's content lives only in that org's own tenant DB; a handle to
the shared consumer plane inside routes/ or services/ means one request can
touch two databases, which is how content leaves the tenancy boundary.

This is exactly the shape that `services::promotion` had before it was deleted:
read the caller's personal ocean off the consumer pool, re-encrypt from the
personal DEK to the org DEK, write into the tenant DB. It passed review and had
tests. It was still a cross-database copy.

If you are adding a feature that seems to need this, it doesn't — it needs the
data to ORIGINATE on the plane that will serve it (an org-seated connector,
capture written directly into the org, an org-plane MCP write). There is no
allowlist mechanism in this script — it is a flat grep, not a suppressible
lint. If you believe you have a genuine exception, that is itself a tenancy-
plan change: get the same review a tenancy-plan change would get, and either
move the code out of the scanned trees or extend this script's own logic
(not a data file) to name the exception explicitly, in the same PR.
EOF
  exit 1
fi

echo "OK: no consumer-plane pool/store handle in request-serving code under ${SCAN_DIRS[*]} (lint-strength check — see header for what it does and does not prove)"
