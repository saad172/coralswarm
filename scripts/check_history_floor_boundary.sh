#!/usr/bin/env bash
#
# check_history_floor_boundary.sh: the team-seat history-floor boundary guard
# (#1545, after #1525/#1541).
#
# THE RULE THIS ENFORCES: a member read of floored content goes through the one
# floor boundary, `crate::history_floor` (backend/src/history_floor.rs), or a
# SQL boundary that applies it (`library_permitted_atoms`, `served_semantic_atoms`,
# the `history_floor_admits*` functions). Production tenant requests connect as
# the table owner, so RLS does not apply and a read that skips the boundary
# shows a late joiner content from before their `team_seat_grants.history_from`.
#
# WHAT IT MEASURES. Every SQL string literal under backend/src that READS one of
# the floored tables (`FROM`/`JOIN` atoms, semantic_atoms, source_items,
# source_versions, meeting_transcripts, journal_rollups, journal_gists) and names none of the
# boundary tokens is an "unfloored read". The scanner is
# scripts/lib/history_floor_scan.pl; its header lists the tokens and what it
# skips (writes, FOR UPDATE locks, `#[cfg(test)] mod` bodies).
#
# A RATCHET, NOT A ZERO. Many unfloored reads are correct: system-scope workers
# that build caches (the floor is NULL there anyway), capture and write paths,
# id lookups, admin counts. They are frozen per file in
# scripts/history_floor_boundary.baseline. The gate fails when:
#   * a file's count of unfloored reads RISES (a new read bypasses the
#     boundary), or a file not in the baseline gains one;
#   * a file's count FALLS below its baseline (stale; tighten with --update),
#     so headroom never silently re-opens.
#
# HOW TO PASS WHEN IT FAILS.
#   1. The read returns member-visible content: apply the floor with a
#      `crate::history_floor` builder (`admits_atom`, `admits_version`,
#      `admits_source_item[_at]`, `admits_transcript`, `admits_period`) in a
#      `{floor}`-style format! slot, or read through a SQL boundary.
#   2. The read only re-reads keys another floored statement already admitted:
#      say so in the SQL, `-- history_floor: <why>` or
#      `/* history_floor: <why> */`. A reviewer sees the claim at the statement.
#   3. It is genuinely not a member read (a worker, a write path): raise that
#      file's count in the baseline BY HAND, with the reason in the PR.
#
# WHAT A GREEN RUN DOES NOT PROVE. It counts text. A table named through a
# format! argument, a statement assembled with push_str, or a boundary token
# that does not floor the table actually shown (`library_atom_permitted` on a
# source list) evades it. The owner-role suites (history_floor_owner_pg,
# history_floor_surfaces_pg) are the behavioural proof.
#
# USAGE
#   bash scripts/check_history_floor_boundary.sh             # the gate
#   bash scripts/check_history_floor_boundary.sh --list      # every unfloored read
#   bash scripts/check_history_floor_boundary.sh --update    # tighten only
#   bash scripts/check_history_floor_boundary.sh --self-test # scanner fixtures
#
# `--update` only ever LOWERS counts and REMOVES files; raising is a hand edit.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SCAN="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib/history_floor_scan.pl"
BASELINE=""
MODE="check"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --update) MODE="update"; shift ;;
    --list) MODE="list"; shift ;;
    --self-test) MODE="self-test"; shift ;;
    --root) ROOT="$2"; shift 2 ;;
    --baseline) BASELINE="$2"; shift 2 ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done
[[ -n "$BASELINE" ]] || BASELINE="$ROOT/scripts/history_floor_boundary.baseline"

# Every scanned file, repo-relative, C-sorted. Ops binaries and test-only
# files are out of scope; test modules inside a file are cut by the scanner.
scanned_files() {
  local root="$1"
  [[ -d "$root/backend/src" ]] || return 0
  ( cd "$root" && find backend/src -type f -name '*.rs' \
      -not -path 'backend/src/bin/*' -not -path '*/pg_tests/*' \
      -not -name '*pg_tests*.rs' -not -name 'tests.rs' -print ) | LC_ALL=C sort
}

# "<count> <path>" for every file with at least one unfloored read.
measure() {
  local root="$1" files
  files="$(scanned_files "$root")"
  [[ -n "$files" ]] || return 0
  ( cd "$root" && printf '%s\n' "$files" | tr '\n' '\0' | xargs -0 perl "$SCAN" ) \
    | cut -f1 | LC_ALL=C sort | uniq -c | awk '{print $2" "$1}'
}

# The fixtures are Rust source text: `$1` in them is a SQL bind, not a shell
# expansion, so single quotes are intended.
# shellcheck disable=SC2016
if [[ "$MODE" == "self-test" ]]; then
  fails=0
  tmp="$(mktemp -d)"; trap 'rm -rf "$tmp"' EXIT
  check_fixture() { # name, rust-source, expected-unfloored-count
    local name="$1" src="$2" want="$3" got
    printf '%s' "$src" > "$tmp/f.rs"
    got="$(perl "$SCAN" "$tmp/f.rs" | wc -l | tr -d ' ')"
    if [[ "$got" == "$want" ]]; then echo "  ok: $name"; else echo "SELF-TEST FAIL: $name: expected $want, got $got" >&2; fails=1; fi
  }
  check_fixture "a bare read of atoms is unfloored" 'fn f(){ q("SELECT * FROM atoms WHERE graph_id=$1"); }' 1
  check_fixture "a read through history_floor_admits is floored" 'fn f(){ q("SELECT * FROM atoms a WHERE public.history_floor_admits(a.graph_id,a.\"timestamp\",(SELECT public.reader_history_floors()))"); }' 0
  check_fixture "a {floor} format slot is floored" 'fn f(){ q(&format!("SELECT * FROM source_items s WHERE {item_floor}", item_floor=x)); }' 0
  check_fixture "a __FLOOR__ replace slot is floored" 'const Q:&str=r#"SELECT * FROM source_versions v WHERE __VERSION_FLOOR__"#;' 0
  check_fixture "a history_floor: claim is floored" 'const Q:&str=r#"SELECT k FROM semantic_atoms a -- history_floor: keys of a floored set
"#;' 0
  check_fixture "a block-comment claim is floored" 'fn f(){ q("SELECT 1 FROM meeting_transcripts /* history_floor: recheck */"); }' 0
  check_fixture "a write is not a read" 'fn f(){ q("UPDATE atoms SET title=$1 WHERE atom_id IN (SELECT atom_id FROM atoms)"); }' 0
  check_fixture "a continued UPDATE .. SET is a write" 'fn f(){ q("WITH x AS (SELECT 1 FROM meeting_transcripts) UPDATE meeting_transcripts \
      SET summary=NULL"); }' 0
  check_fixture "a FOR UPDATE lock is not a member read" 'fn f(){ q("SELECT * FROM source_versions WHERE version_id=$1 FOR UPDATE"); }' 0
  check_fixture "served_semantic_atoms is the boundary, not semantic_atoms" 'fn f(){ q("SELECT * FROM served_semantic_atoms"); }' 0
  check_fixture "a table-returning function is not the table" 'fn f(){ q("SELECT * FROM atoms_for(x)"); q("SELECT * FROM library_permitted_atoms(g,NULL)"); }' 0
  check_fixture "a raw string read is seen" 'const Q:&str=r#"SELECT "x" FROM journal_rollups"#;' 1
  check_fixture "a read in a line comment is not code" '// q("SELECT * FROM atoms")
fn f(){}' 0
  check_fixture "a #[cfg(test)] mod body is not scanned" 'fn f(){}
#[cfg(test)]
mod tests { fn t(){ q("SELECT * FROM atoms"); } }
fn g(){ q("SELECT * FROM atoms"); }' 1
  check_fixture "a char literal quote does not open a string" "fn f(){ let c='\"'; q(\"SELECT * FROM atoms\"); }" 1
  # Ratchet behaviour on a fixture tree.
  ratchet() { # name, file-body, baseline-body, expected-exit
    local name="$1" body="$2" base="$3" want="$4" got=0 d="$tmp/r$RANDOM$RANDOM"
    mkdir -p "$d/backend/src" "$d/scripts"
    printf '%s' "$body" > "$d/backend/src/a.rs"
    printf '%s' "$base" > "$d/scripts/history_floor_boundary.baseline"
    bash "${BASH_SOURCE[0]}" --root "$d" >/dev/null 2>&1 || got=$?
    if [[ "$got" == "$want" ]]; then echo "  ok: $name"; else echo "SELF-TEST FAIL: $name: expected exit $want, got $got" >&2; fails=1; fi
  }
  READ1='fn f(){ q("SELECT * FROM atoms"); }'
  READ2='fn f(){ q("SELECT * FROM atoms"); q("SELECT * FROM source_items"); }'
  ratchet "a count equal to the baseline passes" "$READ1" 'unfloored backend/src/a.rs 1' 0
  ratchet "a new unfloored read fails" "$READ2" 'unfloored backend/src/a.rs 1' 1
  ratchet "a new file with an unfloored read fails" "$READ1" '' 1
  ratchet "a stale (too-high) baseline fails" "$READ1" 'unfloored backend/src/a.rs 2' 1
  ratchet "a fully floored file listed in the baseline is stale" 'fn f(){}' 'unfloored backend/src/a.rs 1' 1
  [[ "$fails" -eq 0 ]] || { echo "self-test FAILED" >&2; exit 1; }
  echo "self-test ok"
  exit 0
fi

measured="$(measure "$ROOT")"

if [[ "$MODE" == "list" ]]; then
  ( cd "$ROOT" && scanned_files "$ROOT" | tr '\n' '\0' | xargs -0 perl "$SCAN" )
  exit 0
fi

write_baseline() {
  {
    cat <<'HEADER'
# The team-seat history-floor boundary ratchet (scripts/check_history_floor_boundary.sh).
#
# Per file: how many SQL literals read a floored table without the floor
# boundary (crate::history_floor). A RATCHET: counts may go down and may not
# go up, and no file may be added. Lower it with `--update`. RAISING IT IS A
# HAND EDIT, with the reason in the PR: a new member read of floored content
# must apply the floor instead.
#
# Committed at #1545 (2026-10-28). `--list` prints every counted statement.
HEADER
    printf '%s\n' "$1" | sed '/^$/d' | sed 's/^/unfloored /'
  } > "$BASELINE"
}

if [[ "$MODE" == "update" ]]; then
  if [[ ! -f "$BASELINE" ]]; then write_baseline "$measured"; echo "bootstrapped $BASELINE"; exit 0; fi
  refuse=0
  while read -r path count; do
    [[ -z "$path" ]] && continue
    base="$(awk -v p="$path" '$1=="unfloored" && $2==p {print $3}' "$BASELINE")"
    if [[ -z "$base" ]]; then echo "REFUSED: --update cannot add $path ($count)" >&2; refuse=1
    elif (( count > base )); then echo "REFUSED: --update cannot raise $path ($base -> $count)" >&2; refuse=1; fi
  done <<< "$measured"
  (( refuse )) && { echo "--update only tightens. Raising is a hand edit to $BASELINE." >&2; exit 1; }
  write_baseline "$measured"
  echo "tightened $BASELINE"
  exit 0
fi

[[ -f "$BASELINE" ]] || { echo "FAIL: baseline not found at $BASELINE (bootstrap with --update)" >&2; exit 1; }
violations=0
base_list="$(awk '$1=="unfloored" {print $2" "$3}' "$BASELINE" | LC_ALL=C sort)"
while read -r path count; do
  [[ -z "$path" ]] && continue
  base="$(printf '%s\n' "$base_list" | awk -v p="$path" '$1==p {print $2}')"
  if [[ -z "$base" ]]; then
    echo "FAIL: $path reads floored content without the history-floor boundary ($count statement(s)); it is not in the baseline." >&2
    violations=$((violations + 1))
  elif (( count > base )); then
    echo "FAIL: $path has $count unfloored read(s), baseline $base: a new read bypasses the history-floor boundary." >&2
    violations=$((violations + 1))
  elif (( count < base )); then
    echo "FAIL: stale baseline: $path is down to $count unfloored read(s) but the baseline says $base. Run --update." >&2
    violations=$((violations + 1))
  fi
done <<< "$measured"
while read -r path base; do
  [[ -z "$path" ]] && continue
  if ! printf '%s\n' "$measured" | awk -v p="$path" '$1==p {found=1} END {exit !found}'; then
    echo "FAIL: stale baseline: $path has no unfloored read left (baseline $base). Run --update." >&2
    violations=$((violations + 1))
  fi
done <<< "$base_list"

if (( violations > 0 )); then
  echo "" >&2
  echo "Run with --list to see each counted statement (file, line, table)." >&2
  echo "See this script's header for how to apply the floor or record a claim." >&2
  exit 1
fi
total="$(printf '%s\n' "$measured" | awk '{s+=$2} END {print s+0}')"
files="$(printf '%s\n' "$measured" | sed '/^$/d' | wc -l | tr -d ' ')"
echo "ok: history-floor boundary ratchet holds: $total unfloored read(s) frozen across $files file(s)"
