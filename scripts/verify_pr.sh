#!/usr/bin/env bash
#
# Validate an open pull request locally and publish the verdict to it.
#
# WHY THIS EXISTS
# ---------------
# With the `CI` workflow disabled, `scripts/githooks/pre-push` is what validates
# code — but it only fires on `git push`. Any PR that was already on GitHub when
# CI was switched off therefore has NOTHING checking it, and every future PR
# opened from another machine (or edited through the web UI) has the same hole.
# This closes it: point it at a PR number and it runs the same local suite and
# posts the same `local-ci` status the hook would have.
#
# WHAT IT ACTUALLY TESTS
# ----------------------
# The PR's head MERGED INTO THE CURRENT BASE, computed here rather than taken
# from GitHub. ci.yml's pull_request trigger tested a merge result too, so the
# verdict keeps its old meaning — but GitHub's own `refs/pull/N/merge` is
# computed lazily and goes stale. Measured on #622: that ref still merged into
# a6a429f1 long after main had moved to 56dd8235, three commits later. Trusting
# it would report a confident green for a base that no longer exists, which is
# exactly the failure this script exists to prevent.
#
# The status is posted to the PR's HEAD sha, because that is the commit GitHub
# shows checks against; the local merge commit is ephemeral and has no PR page.
#
# The runner is invoked from THIS checkout, not from the PR's tree, so a PR that
# predates `scripts/ci_local.py` (or that modifies it) is still judged by the
# version on main. A PR proposing changes to the runner is validated by the
# runner main already trusts — which is the correct direction for that trust to
# flow.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [ -t 1 ] && [ -z "${NO_COLOR:-}" ]; then
  R=$'\033[31m'; G=$'\033[32m'; B=$'\033[34m'; D=$'\033[2m'; BD=$'\033[1m'; N=$'\033[0m'
else
  R=""; G=""; B=""; D=""; BD=""; N=""
fi
say()  { printf '%s==>%s %s\n' "$B" "$N" "$*"; }
die()  { printf '%s✗ %s%s\n' "$R" "$*" "$N" >&2; exit 1; }

# `hostname -s` (short name, no domain suffix) is GNU/BSD-only — Windows'
# own hostname.exe (what Git Bash's `hostname` resolves to) has no `-s` and
# just errors on it ("unknown option -- s"). Plain `hostname` already prints
# an unqualified name on Windows, so stripping any domain suffix by hand
# gets the same short-name result everywhere without depending on the flag.
short_hostname() { hostname | cut -d. -f1; }

FULL=0
NO_STATUS=0
JOBS=""
PRS=()
while [ $# -gt 0 ]; do
  case "$1" in
    --full)      FULL=1 ;;
    --no-status) NO_STATUS=1 ;;
    -j|--jobs)   JOBS="$2"; shift ;;
    -h|--help)
      cat <<'EOF'
usage: scripts/verify_pr.sh [--full] [--no-status] [-j N] <pr-number> [pr-number...]

  Merges each PR into its CURRENT base locally, runs local CI on the result,
  then posts a
  `local-ci` commit status to the PR so it shows a check.

  --full        run every job, not just the lanes the PR touches
  --no-status   run the suite but post nothing
  -j, --jobs N  max concurrent Postgres lanes (passed through to ci_local.py;
                see its own -j for the default and what it parallelizes)

  With no PR numbers, verifies every open PR.
EOF
      exit 0 ;;
    -*) die "unknown option: $1" ;;
    *)  PRS+=("$1") ;;
  esac
  shift
done

command -v gh >/dev/null || die "gh not found"
command -v python3 >/dev/null || die "python3 not found"

if [ ${#PRS[@]} -eq 0 ]; then
  mapfile -t PRS < <(gh pr list --json number --jq '.[].number')
  [ ${#PRS[@]} -gt 0 ] || { echo "no open pull requests"; exit 0; }
  say "verifying all ${#PRS[@]} open PR(s): ${PRS[*]}"
fi

failed=()
for pr in "${PRS[@]}"; do
  printf '\n%s%s── PR #%s ──%s\n' "$BD" "$B" "$pr" "$N"

  meta="$(gh pr view "$pr" --json headRefOid,baseRefName,title,state,mergeable 2>/dev/null)" \
    || die "#$pr: cannot read that PR"
  state="$(jq -r .state <<<"$meta")"
  [ "$state" = "OPEN" ] || { echo "  ${D}#$pr is $state — skipping${N}"; continue; }

  head_sha="$(jq -r .headRefOid <<<"$meta")"
  base="$(jq -r .baseRefName <<<"$meta")"
  mergeable="$(jq -r .mergeable <<<"$meta")"
  printf '  %s\n  %shead %s onto %s%s\n' "$(jq -r .title <<<"$meta")" "$D" "${head_sha:0:8}" "$base" "$N"

  if [ "$mergeable" = "CONFLICTING" ]; then
    printf '  %s✗ has conflicts with %s — rebase it first%s\n' "$R" "$base" "$N"
    failed+=("$pr(conflicts)")
    continue
  fi

  # MERGED LOCALLY, NOT VIA refs/pull/N/merge.
  #
  # GitHub's merge ref is computed lazily and goes stale: measured on #622,
  # `refs/pull/622/merge` still merged into a6a429f1 well after main had moved
  # to 56dd8235 — three commits behind. Testing that ref would have reported a
  # confident green for a base that no longer exists, which is precisely the
  # failure this script is meant to prevent. Fetch the head and merge it into
  # the CURRENT base ourselves, so what runs is always what merging would
  # actually produce right now.
  say "#$pr: fetching head"
  git -C "$ROOT" fetch -q origin "refs/pull/$pr/head:refs/local-ci/pr-$pr" --force \
    || { printf '  %s✗ #%s: cannot fetch head%s\n' "$R" "$pr" "$N"; failed+=("$pr(fetch)"); continue; }
  git -C "$ROOT" fetch -q origin "$base"

  wt="$(mktemp -d "${TMPDIR:-/tmp}/coralswarm-pr-$pr-XXXXXX")"
  rmdir "$wt"                     # git worktree add wants a non-existent path
  git -C "$ROOT" worktree add -q --detach "$wt" "origin/$base"
  # shellcheck disable=SC2064
  trap "git -C '$ROOT' worktree remove --force '$wt' 2>/dev/null || true" EXIT

  say "#$pr: merging into $base ($(git -C "$ROOT" rev-parse --short "origin/$base"))"
  if ! git -C "$wt" -c user.email=local-ci@invalid -c user.name=local-ci \
        merge --no-edit -q "refs/local-ci/pr-$pr" 2>/dev/null; then
    printf '  %s✗ #%s does not merge cleanly into current %s — rebase it%s\n' "$R" "$pr" "$base" "$N"
    git -C "$ROOT" worktree remove --force "$wt" 2>/dev/null || true
    failed+=("$pr(conflicts)")
    continue
  fi

  # Re-normalize line endings against whatever .gitattributes says AFTER the
  # merge, not before. `git worktree add` checks out `origin/$base` under
  # the attributes THAT COMMIT carries; a PR that changes .gitattributes
  # (e.g. adding an eol=lf rule) only rewrites files the merge diff actually
  # touches — a file whose tracked CONTENT never changed, only the attribute
  # governing it, is left exactly as the pre-merge checkout wrote it. On a
  # machine with core.autocrlf=true that means it can stay CRLF even though
  # the newly-merged attribute now says LF, silently reproducing the exact
  # bug a PR fixing this would be trying to prove fixed.
  #
  # `git checkout HEAD -- .` and even `checkout-index -f -a` were both
  # measured NOT to fix this: git skips rewriting a working-tree file it
  # considers content-unchanged relative to the index, `-f` included — the
  # force flag overrides a DIRTY file, not a clean one with stale bytes from
  # an old smudge filter. Deleting every tracked file first removes that
  # shortcut entirely, so the following checkout has nothing to skip.
  ( cd "$wt" && git ls-files -z | xargs -0 rm -f -- )
  git -C "$wt" checkout-index -f -a

  runner_args=(--base "$base")
  [ "$FULL" = 1 ] && runner_args+=(--full)
  [ -n "$JOBS" ] && runner_args+=(-j "$JOBS")
  say "#$pr: running local CI$([ "$FULL" = 1 ] && echo ' (--full)')"
  rc=0
  ( cd "$wt" && python3 "$ROOT/scripts/ci_local.py" "${runner_args[@]}" ) || rc=$?

  git -C "$ROOT" worktree remove --force "$wt" 2>/dev/null || true
  trap - EXIT

  if [ "$rc" -eq 0 ]; then
    printf '  %s✓ #%s passed%s\n' "$G" "$pr" "$N"
    [ "$NO_STATUS" = 1 ] || gh api -X POST "repos/{owner}/{repo}/statuses/$head_sha" \
      -f state=success -f context="local-ci" \
      -f description="local CI passed (merged into $base) on $(short_hostname)" >/dev/null \
      && printf '  %sposted local-ci=success to %s%s\n' "$D" "${head_sha:0:8}" "$N"
  else
    printf '  %s✗ #%s FAILED%s\n' "$R" "$pr" "$N"
    failed+=("$pr")
    # Unlike the pre-push hook — which has no commit on GitHub to annotate when
    # it fails — the head commit here already exists, so a failure CAN and
    # should be published. Silence would read as "not yet checked".
    [ "$NO_STATUS" = 1 ] || gh api -X POST "repos/{owner}/{repo}/statuses/$head_sha" \
      -f state=failure -f context="local-ci" \
      -f description="local CI failed (merged into $base) on $(short_hostname)" >/dev/null || true
  fi
done

printf '\n%s%s%s\n' "$BD" "────────────────────────" "$N"
if [ ${#failed[@]} -gt 0 ]; then
  printf '%s✗ %d of %d failed: %s%s\n' "$R" "${#failed[@]}" "${#PRS[@]}" "${failed[*]}" "$N"
  exit 1
fi
printf '%s✓ all %d PR(s) passed%s\n' "$G" "${#PRS[@]}" "$N"
