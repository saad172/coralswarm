#!/usr/bin/env bash
#
# Point this repo's git hooks at scripts/githooks/ (tracked, reviewable, and
# shared) instead of .git/hooks/ (untracked, per-clone, invisible in review).
#
# WORKTREES: `core.hooksPath` is set as a RELATIVE path on purpose. Git resolves
# a relative hooksPath against the top level of whichever working tree is
# running the hook, so this one setting covers the main checkout and all ~50
# worktrees at once. An absolute path would pin every worktree's hooks to
# whichever checkout happened to run this installer.
#
# Run once per clone:  bash scripts/install_git_hooks.sh
# Undo:                git config --unset core.hooksPath

set -euo pipefail

ROOT="$(git rev-parse --show-toplevel)"
cd "$ROOT"

if [ -t 1 ] && [ -z "${NO_COLOR:-}" ]; then
  G=$'\033[32m'; Y=$'\033[33m'; D=$'\033[2m'; N=$'\033[0m'
else
  G=""; Y=""; D=""; N=""
fi

chmod +x scripts/githooks/* 2>/dev/null || true

existing="$(git config --get core.hooksPath || true)"
if [ -n "$existing" ] && [ "$existing" != "scripts/githooks" ]; then
  printf '%s!%s core.hooksPath is currently %s — overwriting with scripts/githooks\n' "$Y" "$N" "$existing"
fi

git config core.hooksPath scripts/githooks

# The old hooks directory still exists in most clones and is now inert. Say so:
# a stale hook sitting there that no longer fires is a genuinely confusing state
# to debug later.
#
# Built from --absolute-git-dir rather than `--git-path hooks`, which HONOURS
# the core.hooksPath just set above and would therefore report our own
# directory back to us. Also not a literal `.git/hooks`: in a worktree `.git` is
# a FILE pointing at <main>/.git/worktrees/<name>, so that would check nothing.
hooks_dir="$(git rev-parse --absolute-git-dir)/hooks"
for h in "$hooks_dir"/*; do
  [ -f "$h" ] || continue
  case "$h" in *.sample) continue ;; esac
  printf '%s!%s %s contains non-sample hooks; they no longer run.\n' "$Y" "$N" "$hooks_dir"
  break
done

printf '%s✓%s hooks installed — core.hooksPath = scripts/githooks\n' "$G" "$N"
printf '  %spre-push%s  runs path-filtered local CI, blocks the push if red,\n' "$D" "$N"
printf '            and posts a local-ci commit status so the PR shows a check.\n'
printf '  %sskip with: git push --no-verify   (or SKIP_LOCAL_CI=1 git push)%s\n' "$D" "$N"
printf '  %sapplies to every worktree, since the path is relative%s\n' "$D" "$N"
