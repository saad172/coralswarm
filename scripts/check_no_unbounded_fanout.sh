#!/usr/bin/env bash
#
# check_no_unbounded_fanout.sh — the "no O(rows) request fan-out" grep-guard
# (W7, docs/plans/request-reliability-and-scale.md §6.7).
#
# THE RULE THIS ENFORCES: no screen may issue one request per row. Cost must
# not scale with data, because success — a bigger ocean — would then cause
# failure. This is exactly what happened: the Reefs workbench fanned out one
# request per reef, and at 3,440 reefs the browser answered with
# `net::ERR_INSUFFICIENT_RESOURCES`. Nothing in review caught it, and nothing
# in CI could have. This guard is that missing check.
#
# The shape it looks for is the one that actually shipped the bug:
#
#     Promise.all(<collection>.map(… api.<something>(…) …))
#     Promise.allSettled(<collection>.map(… api.<something>(…) …))
#
# i.e. a fan-out whose WIDTH IS DATA. A `Promise.all([a, b, c])` over a
# literal array is not flagged: its width is written in the source, so it is
# bounded by construction and readable in review.
#
# WHAT A GREEN RUN DOES AND DOES NOT PROVE (read before trusting it):
#   * It catches the common, literal shape. It is a LINT, not a data-flow
#     proof. A fan-out assembled indirectly — `const ps = ids.map(...); await
#     Promise.all(ps)`, a `for` loop pushing promises, `flatMap`, a helper
#     that takes an array of thunks — evades the pattern. Reviewers still own
#     those shapes per diff.
#   * It says nothing about how many requests a screen makes in TOTAL. A page
#     that serially awaits 200 pages of a paginated endpoint is just as
#     `O(rows)` and is invisible here. That is what the request-budget test
#     (`web/lib/requestBudget.test.ts`) exists for; the two are complementary
#     and neither replaces the other.
#   * An allowlisted entry is not "safe forever" — it is "reviewed once, on
#     the reasoning written next to it". A reason that stops being true is a
#     silent regression, which is why each entry carries one.
#
# WHY AN ALLOWLIST RATHER THAN A BAN. Two of the current fan-outs are
# genuinely bounded by something other than data — see the manifest — and W1's
# transport limiter now bounds what any of them can do to the network anyway.
# Banning the shape outright would force those to be rewritten for no safety
# gain, and a guard people route around is worse than no guard.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MANIFEST="$ROOT/scripts/fanout_allowlist.manifest"
SEARCH_DIRS=("$ROOT/web/app" "$ROOT/web/components" "$ROOT/web/lib")

if [[ ! -f "$MANIFEST" ]]; then
  echo "FAIL: allowlist manifest not found at $MANIFEST" >&2
  exit 1
fi

# Allowlisted paths, repo-relative. Comments (`#`) and blank lines ignored;
# each entry is `path<TAB or spaces>reason`.
allowlist="$(grep -vE '^\s*(#|$)' "$MANIFEST" | awk '{print $1}' || true)"
# path -> approved occurrence count (field 2).
allowlist_counts="$(grep -vE '^\s*(#|$)' "$MANIFEST" | awk '{print $1" "$2}' || true)"

# ONE definition of the pattern, used by both the scan and the staleness check
# — two copies would drift, and a drifted staleness check silently re-permits
# the bug in an allowlisted file.
#
# The window is `[\s\S]` rather than `[^;]`: stopping at the first `;` meant a
# perfectly ordinary block-bodied callback —
#   rows.map(async (r) => { const id = r.id; return api.reefs.atoms(id); })
# — was O(rows) and matched nothing. It is bounded to 400 characters so an
# unrelated `api.` call far below cannot create a false positive.
count_fanouts() {
  fingerprint_fanouts "$1" | grep -c . || true
}

# A stable FINGERPRINT per matched call site, sorted, one per line.
#
# A bare count is not enough: removing one approved fan-out and adding a
# DIFFERENT O(rows) fan-out in the same allowlisted file keeps the count equal,
# so the new request storm would pass. The fingerprint is the collection
# expression plus the `api.*` member being called — enough to identify WHICH
# call site was approved, and stable against reformatting, renamed locals in
# the callback body, and line moves.
fingerprint_fanouts() {
  perl -0777 -ne '
      s{^\s*//.*$}{}mg;
      while (/Promise\.(?:all|allSettled)\s*\(\s*(?:\/\*.*?\*\/\s*)?([A-Za-z_$][\w$.]*)\s*\.\s*(map|flatMap)\s*\(([\s\S]{0,400}?)\bapi\s*\.\s*([\w.]+)/g) {
        # NO line number, deliberately — see the manifest header. A
        # line-qualified fingerprint was tried and reverted: it fired on any
        # unrelated edit above the call site, which trains people to bump the
        # number without looking and defeats the guard.
        print "$1.$2->api.$4\n";
      }
    ' "$1" | LC_ALL=C sort
}

violations=0
found_file=""

# Slurp each file and match the shape across line breaks — `Promise.all(` and
# the `.map(` it wraps are routinely on different lines. `perl -0777` is used
# rather than grep because grep cannot span lines, and the multi-line form is
# not a rare edge case here (it is the majority of real call sites).
while IFS= read -r -d '' file; do
  rel="${file#"$ROOT"/}"

  # Strip full-line comments so a doc comment that MENTIONS the pattern (this
  # plan's own docs do, at length) is never a violation.
  count="$(count_fanouts "$file")"
  if (( count > 0 )); then
    if echo "$allowlist" | grep -qxF "$rel"; then
      continue
    fi
    if [[ -z "$found_file" ]]; then
      echo "FAIL: unbounded request fan-out (one request per row) found." >&2
      echo "" >&2
      echo "A screen may not issue O(rows) requests — cost must not scale with" >&2
      echo "data. See docs/plans/request-reliability-and-scale.md §6.3." >&2
      echo "" >&2
      found_file="$rel"
    fi
    echo "  $rel" >&2
    violations=$((violations + 1))
  fi
done < <(find "${SEARCH_DIRS[@]}" -type f \( -name '*.ts' -o -name '*.tsx' \) \
           ! -name '*.test.ts' ! -name '*.test.tsx' -print0 2>/dev/null)

if (( violations > 0 )); then
  echo "" >&2
  echo "If the fan-out is genuinely bounded by something other than row count" >&2
  echo "(a user selection, a fixed small set), add it to" >&2
  echo "scripts/fanout_allowlist.manifest WITH the reason it is bounded." >&2
  exit 1
fi

# Guard the guard: an allowlist entry pointing at a file that no longer has
# the pattern is stale, and a stale allowlist quietly re-permits the bug when
# someone reintroduces it in that file.
stale=0
while read -r rel expected; do
  [[ -z "$rel" ]] && continue
  if [[ -z "$expected" ]]; then
    echo "FAIL: allowlist entry '$rel' has no approved-fingerprint list in field 2." >&2
    stale=$((stale + 1))
    continue
  fi
  path="$ROOT/$rel"
  if [[ ! -f "$path" ]]; then
    echo "FAIL: allowlist entry for a file that does not exist: $rel" >&2
    stale=$((stale + 1))
    continue
  fi
  actual_fp="$(fingerprint_fanouts "$path")"
  actual="$(printf '%s' "$actual_fp" | grep -c . || true)"
  approved_fp="$(printf '%s' "$expected" | tr ',' '\n' | LC_ALL=C sort)"
  if (( actual == 0 )); then
    echo "FAIL: stale allowlist entry — $rel no longer fans out; remove it." >&2
    stale=$((stale + 1))
  elif [[ "$actual_fp" != "$approved_fp" ]]; then
    # Identity, not just arity. A bare count would let someone delete one
    # approved fan-out and add a different one in the same file — count
    # unchanged, new request storm shipped. The entry approves SPECIFIC
    # reviewed call sites.
    echo "FAIL: $rel's fan-outs do not match the reviewed set." >&2
    echo "      approved: $(printf '%s' "$approved_fp" | tr '\n' ',')" >&2
    echo "      found:    $(printf '%s' "$actual_fp" | tr '\n' ',')" >&2
    echo "      Review the change, then update the comma-separated fingerprint" >&2
    echo "      list in scripts/fanout_allowlist.manifest." >&2
    stale=$((stale + 1))
  fi
done <<< "$allowlist_counts"

if (( stale > 0 )); then
  exit 1
fi

echo "OK: no unbounded request fan-out outside the reviewed allowlist."
