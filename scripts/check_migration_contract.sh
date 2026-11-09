#!/usr/bin/env bash
#
# check_migration_contract.sh — rollback-safe migrations (expand/contract).
#
# Production rollback is "redeploy the previous release image, no rebuild"
# (docs/runbooks/release.md). That only works if the database a release
# migrated is still readable by the release before it. So a release may only
# ADD schema; anything that removes or renames schema an older binary may still
# read ships in a LATER release, after the code stopped using it.
#
# Enforced for every migration NEWER than the grandfather cutoff, per tree
# (control backend/migrations_control, tenant backend/migrations_tenant):
#
#   A migration whose SQL (comments stripped) removes, renames or retypes
#   schema an older binary may still use must carry the marker line
#
#       -- contract: safe-after server-YYYY.MM.DD.N
#
#   naming the release in which the code stopped using what is removed, and
#   that release must be an existing `server-*` tag strictly OLDER than the
#   latest `server-*` tag. I.e. the expand release has already shipped and been
#   superseded before the contract migration may ride a release.
#
# "Removes, renames or retypes" means, per statement: any RENAME; DROP of a
# COLUMN, TABLE, TYPE, VIEW, MATERIALIZED VIEW, FUNCTION, SCHEMA, CONSTRAINT,
# INDEX, SEQUENCE or DOMAIN; `ALTER TABLE ... DROP <thing>` with COLUMN
# omitted (loosening forms DROP NOT NULL / DEFAULT / IDENTITY / EXPRESSION are
# fine); ALTER COLUMN ... TYPE; and SET NOT NULL (an older binary may still
# write NULLs). DROP POLICY / TRIGGER are not flagged: the repo drops and
# recreates them routinely. A hit inside a function body is a false positive
# the marker (or splitting the migration) resolves.
#
# Migrations numbered at or below the cutoff are grandfathered: they predate
# the policy and are already applied everywhere. Do NOT raise the cutoffs to
# silence the guard; add the marker or split the migration.
#
# Tags come from `git tag -l 'server-*'` (a shallow checkout needs
# `fetch-tags: true`), or from $SERVER_TAGS (newline-separated) when set —
# used by the self-test.
#
# Usage:
#   bash scripts/check_migration_contract.sh              # verify the tree
#   bash scripts/check_migration_contract.sh --self-test  # fixtures
#   (--control-dir/--tenant-dir/--control-cutoff/--tenant-cutoff exist for the
#    self-test's throwaway trees)
#
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# Highest migration number present when the policy landed (2026-10-28).
CONTROL_CUTOFF=392
TENANT_CUTOFF=207
CONTROL_DIR="$REPO_ROOT/backend/migrations_control"
TENANT_DIR="$REPO_ROOT/backend/migrations_tenant"

TAG_RE='^server-[0-9]{4}\.[0-9]{2}\.[0-9]{2}\.[0-9]+$'

# server-2026.10.28.3 -> 2026102800003 (sortable). Fails on a malformed tag.
tag_key() {
  local t="$1"
  [[ "$t" =~ $TAG_RE ]] || return 1
  local rest="${t#server-}" y m d n
  IFS=. read -r y m d n <<<"$rest"
  printf '%04d%02d%02d%06d' "$((10#$y))" "$((10#$m))" "$((10#$d))" "$((10#$n))"
}

list_tags() {
  if [ "${SERVER_TAGS+set}" = "set" ]; then
    printf '%s\n' "$SERVER_TAGS"
  else
    git -C "$REPO_ROOT" tag -l 'server-*'
  fi
}

latest_key() {
  local best="" t k
  while IFS= read -r t; do
    [ -n "$t" ] || continue
    k="$(tag_key "$t")" || continue
    if [ -z "$best" ] || [ "$k" \> "$best" ]; then best="$k"; fi
  done < <(list_tags)
  printf '%s' "$best"
}

tag_exists() { list_tags | grep -qxF "$1"; }

# Exit 0 when the file's SQL (comments removed) needs a contract marker.
needs_contract() {
  perl -0777 -e '
    local $/; my $sql = <>;
    $sql =~ s{/\*.*?\*/}{ }gs; $sql =~ s{--[^\n]*}{}g; $sql = uc $sql;
    for my $st (split /;/, $sql) {
      exit 0 if $st =~ /\bRENAME\b/;
      exit 0 if $st =~ /\bDROP\s+(?:COLUMN|TABLE|TYPE|VIEW|MATERIALIZED|FUNCTION|SCHEMA|CONSTRAINT|INDEX|SEQUENCE|DOMAIN)\b/;
      if ($st =~ /\bALTER\s+TABLE\b/) {
        exit 0 if $st =~ /\bDROP\s+(?!NOT\b|DEFAULT\b|IDENTITY\b|EXPRESSION\b)[A-Z_"]/;
        exit 0 if $st =~ /\bALTER\s+(?:COLUMN\s+)?\S+\s+(?:SET\s+DATA\s+)?TYPE\b/;
        exit 0 if $st =~ /\bSET\s+NOT\s+NULL\b/;
      }
    }
    exit 1;' "$1"
}

# Prints violations for one tree; returns 1 if any.
scan_tree() { # dir cutoff label
  local dir="$1" cutoff="$2" label="$3" bad=0 f base num marker rel k latest
  latest="$(latest_key)"
  [ -d "$dir" ] || { echo "ERROR: $label migration dir not found: $dir" >&2; return 1; }
  for f in "$dir"/[0-9][0-9][0-9][0-9]_*.sql; do
    [ -e "$f" ] || continue
    base="$(basename "$f")"
    num="$((10#${base%%_*}))"
    [ "$num" -gt "$cutoff" ] || continue
    needs_contract "$f" || continue
    marker="$(grep -E '^[[:space:]]*--[[:space:]]*contract:[[:space:]]*safe-after[[:space:]]+' "$f" | head -n1 || true)"
    if [ -z "$marker" ]; then
      echo "FAIL: $label/$base drops or renames schema without '-- contract: safe-after server-YYYY.MM.DD.N'" >&2
      echo "      Releases only ADD; ship the removal in a later release (docs/runbooks/release.md)." >&2
      bad=1; continue
    fi
    rel="$(printf '%s' "$marker" | sed -E 's/^.*safe-after[[:space:]]+//; s/[[:space:]]+$//')"
    if ! k="$(tag_key "$rel")"; then
      echo "FAIL: $label/$base marker '$rel' is not a server-YYYY.MM.DD.N release" >&2
      bad=1; continue
    fi
    if [ -z "$latest" ]; then
      echo "FAIL: $label/$base marker names $rel but NO server-* tags are visible. In CI this means tags were not fetched (shallow checkout); locally, run: git fetch --tags" >&2
      bad=1; continue
    fi
    if ! tag_exists "$rel"; then
      echo "FAIL: $label/$base marker names $rel, which is not an existing server-* tag" >&2
      bad=1; continue
    fi
    if [ -z "$latest" ] || ! [ "$k" \< "$latest" ]; then
      echo "FAIL: $label/$base marker $rel is not older than the latest server-* release;" >&2
      echo "      the contract migration must wait until a release after $rel has shipped." >&2
      bad=1; continue
    fi
  done
  return "$bad"
}

run_all() {
  local rc=0
  scan_tree "$CONTROL_DIR" "$CONTROL_CUTOFF" control || rc=1
  scan_tree "$TENANT_DIR" "$TENANT_CUTOFF" tenant || rc=1
  return "$rc"
}

self_test() {
  local fails=0
  tmp="$(mktemp -d)"  # global: the EXIT trap outlives this function
  trap 'rm -rf "$tmp"' EXIT
  mkdir -p "$tmp/c" "$tmp/t"
  CONTROL_DIR="$tmp/c"; TENANT_DIR="$tmp/t"; CONTROL_CUTOFF=5; TENANT_CUTOFF=5
  SERVER_TAGS=$'server-2026.10.01.1\nserver-2026.10.20.2\nserver-2026.10.28.10\nnot-a-release\ndesktop-v1.0.0'
  export SERVER_TAGS

  case_() { # name want(0|1) filename sql
    rm -f "$tmp"/c/*.sql "$tmp"/t/*.sql
    printf '%s\n' "$4" >"$tmp/c/$3"
    local got=0
    run_all >/dev/null 2>&1 || got=1
    if [ "$got" != "$2" ]; then
      echo "SELF-TEST FAIL: $1 expected exit $2, got $got" >&2; fails=1
    else
      echo "  ok: $1"
    fi
  }

  case_ "additive migration passes" 0 0010_add.sql 'ALTER TABLE t ADD COLUMN x int;'
  case_ "grandfathered DROP COLUMN passes" 0 0004_old.sql 'ALTER TABLE t DROP COLUMN x;'
  case_ "new DROP COLUMN without marker fails" 1 0010_drop.sql 'ALTER TABLE t DROP COLUMN x;'
  case_ "new DROP TABLE without marker fails" 1 0010_drop.sql 'drop table if exists t;'
  case_ "new RENAME without marker fails" 1 0010_ren.sql 'ALTER TABLE t RENAME COLUMN a TO b;'
  case_ "RENAME TO without marker fails" 1 0010_ren.sql 'ALTER TABLE t RENAME TO u;'
  case_ "DROP in a comment is ignored" 0 0010_c.sql $'-- we used to DROP COLUMN x here\nALTER TABLE t ADD COLUMN y int;'
  case_ "DROP in a block comment is ignored" 0 0010_b.sql $'/* DROP TABLE t */\nSELECT 1;'
  case_ "DROP INDEX needs a marker" 1 0010_i.sql 'DROP INDEX IF EXISTS i;'
  case_ "ALTER TABLE DROP without COLUMN needs a marker" 1 0010_d.sql 'ALTER TABLE t DROP x;'
  case_ "DROP TYPE needs a marker" 1 0010_ty.sql 'DROP TYPE IF EXISTS e;'
  case_ "DROP VIEW needs a marker" 1 0010_v.sql 'DROP VIEW v;'
  case_ "DROP MATERIALIZED VIEW needs a marker" 1 0010_mv.sql 'DROP MATERIALIZED VIEW v;'
  case_ "DROP FUNCTION needs a marker" 1 0010_f.sql 'DROP FUNCTION IF EXISTS f(int);'
  case_ "DROP SCHEMA needs a marker" 1 0010_sc.sql 'DROP SCHEMA s CASCADE;'
  case_ "DROP CONSTRAINT needs a marker" 1 0010_co.sql 'ALTER TABLE t DROP CONSTRAINT c;'
  case_ "ALTER COLUMN TYPE needs a marker" 1 0010_at.sql 'ALTER TABLE t ALTER COLUMN x TYPE bigint;'
  case_ "ALTER COLUMN SET DATA TYPE needs a marker" 1 0010_at2.sql 'ALTER TABLE t ALTER x SET DATA TYPE text;'
  case_ "SET NOT NULL needs a marker" 1 0010_nn.sql 'ALTER TABLE t ALTER COLUMN x SET NOT NULL;'
  case_ "second statement is still scanned" 1 0010_two.sql $'ALTER TABLE t ADD COLUMN y int;\nALTER TABLE t DROP COLUMN x;'
  case_ "DROP NOT NULL is a loosening and passes" 0 0010_dn.sql 'ALTER TABLE t ALTER COLUMN x DROP NOT NULL;'
  case_ "DROP DEFAULT passes" 0 0010_dd.sql 'ALTER TABLE t ALTER COLUMN x DROP DEFAULT;'
  case_ "DROP POLICY / TRIGGER (recreated routinely) pass" 0 0010_pol.sql $'DROP POLICY IF EXISTS p ON t;\nDROP TRIGGER IF EXISTS g ON t;'
  case_ "ADD COLUMN ... NOT NULL passes" 0 0010_add2.sql 'ALTER TABLE t ADD COLUMN x int NOT NULL DEFAULT 0;'
  case_ "identifier containing rename is not a RENAME" 0 0010_n.sql 'ALTER TABLE t ADD COLUMN renamed_at timestamptz;'
  case_ "marker older than latest passes" 0 0010_ok.sql $'-- contract: safe-after server-2026.10.20.2\nALTER TABLE t DROP COLUMN x;'
  case_ "marker equal to latest fails" 1 0010_eq.sql $'-- contract: safe-after server-2026.10.28.10\nALTER TABLE t DROP COLUMN x;'
  case_ "marker newer than latest fails" 1 0010_new.sql $'-- contract: safe-after server-2026.11.01.1\nALTER TABLE t DROP COLUMN x;'
  case_ "marker for a non-existent release fails" 1 0010_ghost.sql $'-- contract: safe-after server-2026.10.10.1\nALTER TABLE t DROP COLUMN x;'
  case_ "malformed marker fails" 1 0010_bad.sql $'-- contract: safe-after soon\nALTER TABLE t DROP COLUMN x;'
  case_ "numeric (not lexical) release order: .2 < .10" 0 0010_num.sql $'-- contract: safe-after server-2026.10.20.2\nDROP TABLE t;'

  # The tenant tree is checked too.
  rm -f "$tmp"/c/*.sql "$tmp"/t/*.sql
  printf 'ALTER TABLE t DROP COLUMN x;\n' >"$tmp/t/0010_drop.sql"
  if run_all >/dev/null 2>&1; then
    echo "SELF-TEST FAIL: tenant DROP COLUMN without marker should fail" >&2; fails=1
  else
    echo "  ok: tenant tree is checked"
  fi

  # No releases at all: any contract migration fails closed.
  SERVER_TAGS=""
  case_ "no tags visible: a marker fails loudly" 1 0010_none.sql $'-- contract: safe-after server-2026.10.20.2\nDROP TABLE t;'

  [ "$fails" -eq 0 ] || { echo "self-test FAILED" >&2; exit 1; }
  echo "self-test ok"
}

while [ $# -gt 0 ]; do
  case "$1" in
    --self-test) self_test; exit 0 ;;
    --control-dir) CONTROL_DIR="$2"; shift 2 ;;
    --tenant-dir) TENANT_DIR="$2"; shift 2 ;;
    --control-cutoff) CONTROL_CUTOFF="$2"; shift 2 ;;
    --tenant-cutoff) TENANT_CUTOFF="$2"; shift 2 ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done

if run_all; then
  echo "migration contract ok"
else
  exit 1
fi
