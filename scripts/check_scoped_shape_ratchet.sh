#!/usr/bin/env bash
#
# check_scoped_shape_ratchet.sh — the `_scoped` SHAPE ratchet.
#
# THE RULE THIS ENFORCES: the backend may not grow more of the thing that is
# about to be deleted. Not "no new features" — features ship freely. Only the
# three shapes the `Scope`/`Db` restructure exists to remove are frozen:
#
#   1. `_scoped` methods on the `Store` trait  — count may not rise
#   2. files defining `macro_rules! scoped_store` — the file SET is frozen
#   3. files mentioning `PgPool` under backend/src — the file SET is frozen
#
# WHY A RATCHET AND NOT A REVIEW NOTE. The restructure plan is priced
# per-method and per-call-site, and execution is deliberately deferred behind
# a gate (issue #550) while feature work continues. So the estimate decays
# every day the gate stays shut. Measured, consistently, on the definitions
# below:
#
#   | metric                      | a24d516    | 7739e577   | ddd3b13e   |
#   |                             | 2026-07-27 | 2026-08-06 | 2026-08-24 | 2026-08-25 |
#   |-----------------------------|------------|------------|------------|------------|
#   | `_scoped` trait methods     | 145        | 183        | 202        | 202        |
#   | `scoped_store!` definitions | 9          | 9          | 9          | 9          |
#   | PgPool files (backend/src)  | 72         | 88         | 129        | 135        |
#
#   2026-08-25 (#711) is the ratchet's FIRST hand-raise since it froze: the six
#   m365-train files (#700/#701/#703) landed with bare PgPools while the
#   pre-push hook was red on main for unrelated reasons, and were baselined
#   fix-forward rather than converted — see scoped_shape.baseline's provenance
#   line and PR #711 for the per-file reasons.
#
# That is +57 `_scoped` methods and +57 PgPool files in 28 days, against a
# plan that has not started. This guard is where that stops. It does not make
# the restructure happen; it stops the bill growing while it waits.
#
# ── ON THE NUMBERS IN THE MAP (issue #549) ──────────────────────────────────
# The map's baseline table reads 145→183 `_scoped` (reproduced here exactly)
# and 40→91 PgPool files. The PgPool row does NOT reproduce: measured with one
# definition, those two commits are 72→88, not 40→91. The audit column and the
# charting column were measured differently, so that row overstates the drift
# (+51 claimed, +16 actual over the same ten days). The `Store`-method row —
# which is what "~4 methods/day" was derived from — does reproduce, so that
# claim stands. The 88-vs-91 gap at 7739e577 is this guard skipping full-line
# comments: three files mention `PgPool` only in prose.
#
# ── WHAT A GREEN RUN DOES AND DOES NOT PROVE (read before trusting it) ──────
#   * It is a SHAPE freeze, not a proof of anything about scoping. A new
#     unscoped query on an existing PgPool file passes — the file was already
#     in the set. Correctness of scoping is #561/#564's problem, not this
#     guard's.
#   * It counts text, not semantics. A `_scoped` method reached through a
#     re-export, a type alias for `PgPool`, or a macro-generated name evades
#     it. Reviewers still own those shapes per diff.
#   * It fails on a baseline that is too HIGH as well as too low. A ratchet
#     that only catches increases is a ceiling: delete a `_scoped` method,
#     leave the baseline at the old number, and you have bought back headroom
#     for a new one without anyone deciding to. Tighten with `--update`.
#   * A frozen file set is not "these files are fine". It is "these files were
#     already carrying this shape when the freeze landed".
#
# ── USAGE ───────────────────────────────────────────────────────────────────
#   bash scripts/check_scoped_shape_ratchet.sh              # the gate
#   bash scripts/check_scoped_shape_ratchet.sh --update     # tighten only
#   bash scripts/check_scoped_shape_ratchet.sh --self-test  # fixtures
#
# `--update` can only ever LOWER the count and REMOVE paths. It refuses to
# raise or add, so it cannot be used to rubber-stamp a regression: loosening
# the baseline is a hand edit, in a diff, with a reviewer.
#
# Part of #552, under the map at #549.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BASELINE=""
MODE="check"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --update)   MODE="update"; shift ;;
    --self-test) MODE="self-test"; shift ;;
    # --root/--baseline exist for the self-test's throwaway fixture trees.
    # They are not part of the normal contract.
    --root)     ROOT="$2"; shift 2 ;;
    --baseline) BASELINE="$2"; shift 2 ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done

# Derived AFTER parsing so `--root` alone can never point the gate at a
# fixture tree while reading the real repo's baseline.
[[ -n "$BASELINE" ]] || BASELINE="$ROOT/scripts/scoped_shape.baseline"

# ── Measurement ─────────────────────────────────────────────────────────────
# ONE definition per metric, used by the gate, by `--update`, and by the
# self-test. Two copies would drift, and a drifted baseline writer silently
# re-permits the growth it exists to stop.
#
# Full-line comments are skipped everywhere (`^\s*//`), matching the existing
# grep-guards in this directory. Deliberately NOT stripping trailing comments
# or block comments: `s{//.*}{}` truncates a line at a `"postgres://…"` string
# and would hide a real token later on that same line.

STORE_TRAIT_FILE="backend/src/store/mod.rs"

# Count `_scoped` methods declared in the `Store` trait body.
#
# Fails CLOSED if the trait header cannot be found. Without that, a rename or
# a reformat of the `pub trait Store` line makes this report 0 — which reads
# as "everything was deleted", passes the increase check, and turns the whole
# guard into a no-op that prints OK. That is the failure mode this file exists
# to prevent, so it is an error, not a zero.
count_scoped_methods() {
  perl -e '
    my $f = shift;
    open(my $fh, "<", $f) or die "RATCHET-ERROR: cannot open $f\n";
    my ($in, $seen_trait, $n) = (0, 0, 0);
    while (my $l = <$fh>) {
      if (!$in && !$seen_trait && $l =~ /^pub trait Store\s*:/) { $in = 1; $seen_trait = 1; next }
      next unless $in;
      if ($l =~ /^\}/) { $in = 0; next }
      next if $l =~ m{^\s*//};
      $n++ if $l =~ /^\s*(?:pub\s+)?(?:async\s+)?fn\s+[A-Za-z0-9_]*_scoped\s*[<(]/;
    }
    die "RATCHET-ERROR: `pub trait Store:` not found in $f — the trait was renamed, moved, or reformatted. Fix this scanner before trusting a green run.\n" unless $seen_trait;
    print "$n\n";
  ' "$1"
}

# Repo-relative paths of every .rs file under backend/src matching a pattern,
# ignoring full-line comments. Sorted, one per line, C collation so the
# baseline diff is stable across machines.
list_files_matching() {
  local root="$1" pattern="$2"
  [[ -d "$root/backend/src" ]] || return 0
  find "$root/backend/src" -type f -name '*.rs' -print0 2>/dev/null \
    | RATCHET_PAT="$pattern" RATCHET_ROOT="$root" perl -0 -ne '
        BEGIN { $pat = qr/$ENV{RATCHET_PAT}/; $root = $ENV{RATCHET_ROOT}; }
        my $path = $_;
        chomp $path;
        open(my $fh, "<", $path) or next;
        # `-0` set $/ to NUL for the NUL-separated path list on stdin. Without
        # localising it back, `<$fh>` slurps each source file as ONE record,
        # `^\s*//` then matches only the FIRST line, and every file whose first
        # line is a comment — most of this tree — is skipped entirely. That
        # under-counted 129 PgPool files to 2 and all 9 macro files to 0, while
        # still exiting 0. Pinned by the "leading comment" fixtures below.
        local $/ = "\n";
        while (my $l = <$fh>) {
          next if $l =~ m{^\s*//};
          if ($l =~ $pat) { my $r = $path; $r =~ s/^\Q$root\E\///; print "$r\n"; last }
        }
        close $fh;
      ' \
    | LC_ALL=C sort
}

list_macro_files()  { list_files_matching "$1" 'macro_rules!\s*scoped_store'; }
list_pgpool_files() { list_files_matching "$1" 'PgPool'; }

measure_all() {
  local root="$1"
  echo "scoped_methods $(count_scoped_methods "$root/$STORE_TRAIT_FILE")"
  list_macro_files  "$root" | sed 's/^/macro_file /'
  list_pgpool_files "$root" | sed 's/^/pgpool_file /'
}

# ── Baseline I/O ────────────────────────────────────────────────────────────
read_baseline_field() { # file, key
  grep -E "^$2 " "$1" 2>/dev/null | awk '{print $2}' || true
}

write_baseline() { # file, measured-block
  local file="$1" measured="$2"
  {
    cat <<'HEADER'
# The `_scoped` shape ratchet baseline (scripts/check_scoped_shape_ratchet.sh).
#
# This is a RATCHET, not a budget. Every number here may go down and may not
# go up; every path here may be removed and none may be added. The guard fails
# on a baseline that is too high as well as too low, so the file cannot drift
# out of date in either direction while still reporting green.
#
# Lowering it is mechanical: `bash scripts/check_scoped_shape_ratchet.sh
# --update`, which refuses to raise or add. RAISING IT IS A HAND EDIT and
# needs a reviewer to agree that the restructure just got more expensive on
# purpose — that is the whole point of the file.
#
# Committed at ddd3b13e (2026-08-24) under #552, map #549.
# Raised 129 -> 135 PgPool files at #711 (2026-08-25): the six m365-train
# files (#700/#701/#703), baselined fix-forward with per-file reasons in the
# PR. First hand-raise since the freeze.
HEADER
    printf '%s\n' "$measured"
  } > "$file"
}

# ── Self-test ───────────────────────────────────────────────────────────────
# The guard's OWN fixtures. A shape guard whose scanner silently reports zero
# is worse than no guard: it prints OK forever while the thing it watches
# grows unchecked. These pin the cases where that could happen.
if [[ "$MODE" == "self-test" ]]; then
  fails=0
  self_root="$(mktemp -d)"
  trap 'rm -rf "$self_root"' EXIT

  fixture() { # name, store-trait-body, extra-files-setup, baseline-body, expected-exit
    local name="$1" store="$2" setup="$3" base="$4" want="$5" got=0
    local d="$self_root/$RANDOM$RANDOM"
    mkdir -p "$d/backend/src/store" "$d/scripts"
    printf '%s' "$store" > "$d/backend/src/store/mod.rs"
    ( cd "$d" && eval "$setup" )
    printf '%s' "$base" > "$d/scripts/scoped_shape.baseline"
    bash "${BASH_SOURCE[0]}" --root "$d" --baseline "$d/scripts/scoped_shape.baseline" >/dev/null 2>&1 || got=$?
    if [[ "$got" != "$want" ]]; then
      echo "SELF-TEST FAIL: $name expected exit $want, got $got" >&2
      fails=1
    else
      echo "  ok: $name"
    fi
  }

  TRAIT_2='pub trait Store: Send + Sync {
    async fn a_scoped(&self) -> u8;
    async fn b_scoped(&self) -> u8;
    async fn plain(&self) -> u8;
}
'
  TRAIT_3='pub trait Store: Send + Sync {
    async fn a_scoped(&self) -> u8;
    async fn b_scoped(&self) -> u8;
    async fn c_scoped(&self) -> u8;
}
'
  TRAIT_1='pub trait Store: Send + Sync {
    async fn a_scoped(&self) -> u8;
}
'
  BASE_2='scoped_methods 2'

  fixture "count equal to baseline passes"        "$TRAIT_2" 'true' "$BASE_2" 0
  fixture "a new _scoped method fails"            "$TRAIT_3" 'true' "$BASE_2" 1
  # A ratchet that only catches increases is a ceiling — deleting a method and
  # leaving the baseline high silently buys headroom for the next one.
  fixture "a stale (too-high) baseline fails"     "$TRAIT_1" 'true' "$BASE_2" 1

  # THE case that would turn the guard into a no-op: the scanner can no longer
  # find the trait, so it "measures" zero and every increase passes forever.
  fixture "an unfindable Store trait is an ERROR, not a zero" \
    'pub trait Storage: Send + Sync {
    async fn a_scoped(&self) -> u8;
}
' 'true' "$BASE_2" 1

  # Commented-out declarations are not declarations.
  fixture "a _scoped fn in a full-line comment does not count" \
    'pub trait Store: Send + Sync {
    async fn a_scoped(&self) -> u8;
    async fn b_scoped(&self) -> u8;
    // async fn c_scoped(&self) -> u8;
}
' 'true' "$BASE_2" 0

  # Only the trait body is frozen. Impls below it are generated from it and
  # would double- or triple-count every method.
  fixture "_scoped fns outside the trait body do not count" \
    "$TRAIT_2"'
impl Store for Pg {
    async fn a_scoped(&self) -> u8 { 0 }
    async fn b_scoped(&self) -> u8 { 0 }
}
' 'true' "$BASE_2" 0

  fixture "a new scoped_store! macro definition fails" "$TRAIT_2" \
    'mkdir -p backend/src/routes && printf "macro_rules! scoped_store { () => {} }\n" > backend/src/routes/new.rs' \
    "$BASE_2" 1
  fixture "a removed macro definition is a stale baseline entry" "$TRAIT_2" 'true' \
    "$BASE_2
macro_file backend/src/routes/gone.rs" 1
  fixture "a frozen macro file still defining the macro passes" "$TRAIT_2" \
    'mkdir -p backend/src/routes && printf "macro_rules! scoped_store { () => {} }\n" > backend/src/routes/known.rs' \
    "$BASE_2
macro_file backend/src/routes/known.rs" 0

  fixture "a new PgPool file fails" "$TRAIT_2" \
    'mkdir -p backend/src/routes && printf "use sqlx::PgPool;\n" > backend/src/routes/new.rs' \
    "$BASE_2" 1
  fixture "a frozen PgPool file passes" "$TRAIT_2" \
    'mkdir -p backend/src/routes && printf "use sqlx::PgPool;\n" > backend/src/routes/known.rs' \
    "$BASE_2
pgpool_file backend/src/routes/known.rs" 0
  # Prose about the migration is not the migration.
  fixture "PgPool named only in a full-line comment is not a new file" "$TRAIT_2" \
    'mkdir -p backend/src/routes && printf "// this route no longer takes a PgPool\nfn f() {}\n" > backend/src/routes/doc.rs' \
    "$BASE_2" 0
  # THE case that made the one above pass for the wrong reason. Nearly every
  # file in this tree opens with a `//!` or `//` header, and a scanner that
  # tests only the first line skips all of them — reporting 2 PgPool files
  # where there are 129, and exiting 0 while doing it.
  fixture "a leading comment does not hide a real PgPool below it" "$TRAIT_2" \
    'mkdir -p backend/src/routes && printf "//! module docs\n// more prose\nuse sqlx::PgPool;\n" > backend/src/routes/hdr.rs' \
    "$BASE_2" 1
  fixture "a leading comment does not hide a real macro below it" "$TRAIT_2" \
    'mkdir -p backend/src/routes && printf "//! module docs\nmacro_rules! scoped_store { () => {} }\n" > backend/src/routes/hdr.rs' \
    "$BASE_2" 1
  fixture "a deleted PgPool file is a stale baseline entry" "$TRAIT_2" 'true' \
    "$BASE_2
pgpool_file backend/src/routes/gone.rs" 1

  # --update tightens and refuses to loosen.
  upd="$self_root/upd"
  mkdir -p "$upd/backend/src/store" "$upd/scripts"
  printf '%s' "$TRAIT_1" > "$upd/backend/src/store/mod.rs"
  printf 'scoped_methods 2\npgpool_file backend/src/routes/gone.rs\n' > "$upd/scripts/scoped_shape.baseline"
  if bash "${BASH_SOURCE[0]}" --update --root "$upd" --baseline "$upd/scripts/scoped_shape.baseline" >/dev/null 2>&1 \
     && [[ "$(read_baseline_field "$upd/scripts/scoped_shape.baseline" scoped_methods)" == "1" ]] \
     && ! grep -q 'pgpool_file' "$upd/scripts/scoped_shape.baseline"; then
    echo "  ok: --update lowers the count and drops the stale path"
  else
    echo "SELF-TEST FAIL: --update did not tighten" >&2; fails=1
  fi

  printf '%s' "$TRAIT_3" > "$upd/backend/src/store/mod.rs"
  printf 'scoped_methods 1\n' > "$upd/scripts/scoped_shape.baseline"
  if bash "${BASH_SOURCE[0]}" --update --root "$upd" --baseline "$upd/scripts/scoped_shape.baseline" >/dev/null 2>&1; then
    echo "SELF-TEST FAIL: --update raised the baseline" >&2; fails=1
  elif [[ "$(read_baseline_field "$upd/scripts/scoped_shape.baseline" scoped_methods)" == "1" ]]; then
    echo "  ok: --update refuses to raise the count, and leaves the file alone"
  else
    echo "SELF-TEST FAIL: --update rewrote the file while refusing" >&2; fails=1
  fi

  [[ "$fails" -eq 0 ]] || { echo "self-test FAILED" >&2; exit 1; }
  echo "self-test ok"
  exit 0
fi

# ── Bootstrap / update ──────────────────────────────────────────────────────
measured="$(measure_all "$ROOT")"
now_count="$(printf '%s\n' "$measured" | awk '$1=="scoped_methods"{print $2}')"
now_macro="$(printf '%s\n' "$measured" | awk '$1=="macro_file"{print $2}')"
now_pgpool="$(printf '%s\n' "$measured" | awk '$1=="pgpool_file"{print $2}')"

if [[ "$MODE" == "update" ]]; then
  if [[ ! -f "$BASELINE" ]]; then
    write_baseline "$BASELINE" "$measured"
    echo "bootstrapped $BASELINE"
    exit 0
  fi
  base_count="$(read_baseline_field "$BASELINE" scoped_methods)"
  base_macro="$(grep -E '^macro_file ' "$BASELINE" | awk '{print $2}' || true)"
  base_pgpool="$(grep -E '^pgpool_file ' "$BASELINE" | awk '{print $2}' || true)"

  refuse=0
  if [[ -n "$base_count" ]] && (( now_count > base_count )); then
    echo "REFUSED: --update cannot raise scoped_methods ($base_count -> $now_count)." >&2
    refuse=1
  fi
  while IFS= read -r p; do
    [[ -z "$p" ]] && continue
    echo "REFUSED: --update cannot add a new macro_file: $p" >&2
    refuse=1
  done < <(comm -13 <(printf '%s\n' "$base_macro" | LC_ALL=C sort) <(printf '%s\n' "$now_macro" | LC_ALL=C sort))
  while IFS= read -r p; do
    [[ -z "$p" ]] && continue
    echo "REFUSED: --update cannot add a new pgpool_file: $p" >&2
    refuse=1
  done < <(comm -13 <(printf '%s\n' "$base_pgpool" | LC_ALL=C sort) <(printf '%s\n' "$now_pgpool" | LC_ALL=C sort))
  if (( refuse )); then
    echo "" >&2
    echo "--update only ever tightens. Loosening the ratchet is a hand edit to" >&2
    echo "$BASELINE, in a diff, with a reviewer." >&2
    exit 1
  fi
  write_baseline "$BASELINE" "$measured"
  echo "tightened $BASELINE"
  exit 0
fi

# ── The gate ────────────────────────────────────────────────────────────────
if [[ ! -f "$BASELINE" ]]; then
  echo "FAIL: baseline not found at $BASELINE" >&2
  echo "      Bootstrap it with --update, then commit it." >&2
  exit 1
fi

base_count="$(read_baseline_field "$BASELINE" scoped_methods)"
if [[ -z "$base_count" ]]; then
  echo "FAIL: $BASELINE has no 'scoped_methods <n>' line." >&2
  exit 1
fi
base_macro="$(grep -E '^macro_file ' "$BASELINE" | awk '{print $2}' | LC_ALL=C sort || true)"
base_pgpool="$(grep -E '^pgpool_file ' "$BASELINE" | awk '{print $2}' | LC_ALL=C sort || true)"

violations=0

if (( now_count > base_count )); then
  cat >&2 <<EOF
FAIL: the Store trait grew $((now_count - base_count)) new \`_scoped\` method(s) — $base_count -> $now_count.

\`_scoped\` twins are what the Scope/Db restructure exists to delete, and the
plan is priced per method. A new one makes an already-deferred job bigger.

Take the scope from the handle instead of passing it as a parameter, or — if
this genuinely has to ship as a twin today — raise \`scoped_methods\` in
scripts/scoped_shape.baseline by hand and say in the PR why.
EOF
  violations=$((violations + 1))
elif (( now_count < base_count )); then
  cat >&2 <<EOF
FAIL: stale baseline — \`_scoped\` methods are down to $now_count but scripts/scoped_shape.baseline still says $base_count.

Left alone, that gap is headroom: the next twin lands inside it and nobody
decides anything. Run \`bash scripts/check_scoped_shape_ratchet.sh --update\`.
EOF
  violations=$((violations + 1))
fi

report_set_drift() { # label, baseline-list, current-list, blurb
  local label="$1" base="$2" now="$3" blurb="$4"
  local added removed
  added="$(comm -13 <(printf '%s\n' "$base") <(printf '%s\n' "$now") | sed '/^$/d')"
  removed="$(comm -23 <(printf '%s\n' "$base") <(printf '%s\n' "$now") | sed '/^$/d')"
  if [[ -n "$added" ]]; then
    echo "FAIL: new $label file(s) — the set is frozen." >&2
    printf '%s\n' "$added" | sed 's/^/  + /' >&2
    echo "" >&2
    printf '%s\n' "$blurb" >&2
    echo "" >&2
    violations=$((violations + 1))
  fi
  if [[ -n "$removed" ]]; then
    echo "FAIL: stale baseline — $label file(s) no longer match and are still listed." >&2
    printf '%s\n' "$removed" | sed 's/^/  - /' >&2
    echo "  Run: bash scripts/check_scoped_shape_ratchet.sh --update" >&2
    echo "" >&2
    violations=$((violations + 1))
  fi
}

report_set_drift "macro_rules! scoped_store" "$base_macro" "$(printf '%s\n' "$now_macro" | LC_ALL=C sort)" \
"9 files define this macro today and all 9 are slated for deletion. A tenth
copy is a tenth call site to rewrite. Use an existing definition, or make the
case in the PR and add the path to scripts/scoped_shape.baseline by hand."

report_set_drift "PgPool" "$base_pgpool" "$(printf '%s\n' "$now_pgpool" | LC_ALL=C sort)" \
"A bare \`PgPool\` is the shape the restructure replaces with an opaque \`Db<'a>\`
obtained from \`store.begin(scope)\`. Every new file holding one is another
conversion site. Route through the \`Store\` trait, or add the path to
scripts/scoped_shape.baseline by hand with the reason in the PR."

if (( violations > 0 )); then
  echo "── Why this guard exists ────────────────────────────────────────────" >&2
  echo "Execution of the backend restructure is deferred behind the gate in" >&2
  echo "issue #550 while features keep shipping. The plan is priced per method" >&2
  echo "and per call site, so it decays the entire time it waits. This is the" >&2
  echo "thing that stops the bill growing. Map: issue #549, ticket #552." >&2
  exit 1
fi

echo "ok: _scoped shape frozen — $now_count trait methods, $(printf '%s\n' "$now_macro" | sed '/^$/d' | wc -l | tr -d ' ') scoped_store! definitions, $(printf '%s\n' "$now_pgpool" | sed '/^$/d' | wc -l | tr -d ' ') PgPool files"
