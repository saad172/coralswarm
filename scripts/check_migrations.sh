#!/usr/bin/env bash
#
# check_migrations.sh — guards the split migration trees.
#
# Enforces, per tree (control + tenant):
#   1. No duplicate migration numbers (the 2026-07 double-stamp incident:
#      a worktree re-stamped 0076 into a dev DB → VersionMissing panics).
#   2. Gapless numbering EXCEPT an explicit allowlist of known historical
#      gaps. Once the trees are split into per-org tenant DBs, a duplicated
#      or renumbered migration is an N-way ledger incident, not a one-DB one,
#      so this runs in CI on every PR.
#   3. Committed checksum manifest matches on-disk files: an already-merged
#      migration MUST NOT be edited (sqlx fingerprints on file content, so an
#      edit breaks the _sqlx_migrations ledger on every deployed DB). A new
#      migration is added to the manifest in the same PR that introduces it.
#
# Usage:
#   scripts/check_migrations.sh            # verify (CI mode; non-zero on fail)
#   scripts/check_migrations.sh --update   # regenerate the checksum manifests
#
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

# Known historical gaps per tree (space-separated 4-digit numbers). The control
# tree inherited 0043 as a missing number from the pre-split history; it is NOT
# an error. New gaps are failures — add here only with a recorded reason.
#
# #725 (gsuite Train G4 PR 3, this branch) merges past main's real control
# 0196 (`retraction_audit_log`, #718) and 0197 (`gmail_everything_mode`,
# #722) after this merge, and fills tenant's own deliberately-reserved 0073
# gap with its real `google_chat_watched_spaces` file (control renumbered to
# 0198 for the same reason) — so neither tree has a genuine gap left, and
# NEITHER of the two branches' temporary allowlist entries is needed
# anymore. Back to the historical baseline.
#
# 0226 (chat-per-user carry-over PR, 2026-09-21): control migration 0226 was
# claimed by PR #1003 (CS-430, org_membership_tombstones' address column)
# before this PR was cut. This PR's own unmatched-recipient-email persistence
# — originally numbered 0226/0228 on the held chat-per-user ingest train
# (PR #1000) — is renumbered here to 0227/0228 to avoid the collision, which
# is also the number the held train used for the (not carried) Teams
# per-meeting gate migration; that gate migration does not exist in this
# tree at all (HELD with the rest of the org-install Teams fan-out work, D21,
# `docs/plans/org-onboarding-from-the-invite.md`), so reusing 0227 for the
# carried table costs nothing. The resulting gap is at 0226, not 0227: this
# tree does not have PR #1003's file yet, and will not until that PR merges
# and this branch rebases past it. Allowlisted rather than blocked on
# ordering two independent PRs' merges.
#
# 0233 (CS-436, 2026-09-21): CLOSED (CS-437, 2026-09-21) — `cs-435-slot-pool`
# (the seat-pool work) merged to main at c9fb569c with its real
# `0233_slot_reservations.sql`, so the gap this entry used to hold open no
# longer exists. Left in the changelog above rather than deleted outright;
# only the allowlist entry itself is removed.
#
# 0243 (CS-447, 2026-09-22): CLOSED (CS-447, 2026-09-22) — same shape as
# 0233 above, and a better cautionary tale than either.
#
# Why it was opened: CS-446 (item 16, `connector_health`) and CS-447 (item 9,
# auto-add) were dispatched CONCURRENTLY, in separate worktrees, neither a
# dependency of the other. CS-446 took control 0243; CS-447 was told to take
# 0244 so the two would not collide. Whichever merged first would leave the
# other's number missing on main for the window in between, so 0243 was
# allowlisted rather than ordering two independent PRs' merges.
#
# Why it closed: #1034 merged CS-446 to main at 84590fa3 with the real
# `0243_connector_health.sql`, so the gap it held open no longer exists.
#
# The part worth reading if you are here deciding whether to reserve a
# number: RESERVING 0244 BETWEEN THE TWO PRs DID NOT PROTECT CS-447, and it
# then failed a SECOND time within a day.
#
#   * While the onboarding work was busy coordinating 0243/0244 between its
#     own two branches, #1035 (`9d162062`) — an unrelated retrieval change
#     nobody in that conversation was thinking about — merged
#     `0244_atoms_title_tsv_index.sql` and took the number. CS-447
#     renumbered to 0245.
#   * Before CS-447 could merge, #1039 (`63dc674c`) merged BOTH
#     `0245_atoms_excerpt_tsv_index.sql` and
#     `0246_atoms_source_path_tsv_index.sql` — taking 0245 out from under it
#     as well. CS-447 renumbered again, to 0247.
#
# Two files at one number is a DUPLICATE, which this script rejects under
# rule 1 and which sqlx would apply as a single version; it is a strictly
# worse failure than the gap the reservation was avoiding.
#
# The lesson, stated for whoever arrives here cold: an allowlisted gap
# coordinates the PRs you KNOW about and does nothing about the ones you do
# not. A migration number is not yours until it is on main. Take the next
# free number at REBASE time, expect to renumber, and keep the renumber
# cheap — which means never referring to a migration by number from code
# that would have to be edited in lockstep.
#
# 0295-0298 (control) and 0130-0133 (tenant): PR #1219 (05db0264, the
# source-delete trigger repair) landed its paired fix-forward at 0299/0134 and
# left these numbers to the connected-memory packages that were in flight at
# the time (docs/implementation/source-delete-trigger-repair.md: "subsequent
# unmerged packages use higher IDs for monotonic deployment accounting",
# "the identity contract 0297/0132 stays separately gated outside automatic
# migration directories"). Those packages then took 0300+/0135+ instead, so
# the reserved numbers were never filled. 0299/0134 are merged and
# checksum-pinned in scripts/migrations_*.manifest, so the gaps are permanent:
# renumbering is exactly the edit the manifest exists to refuse.
#
# 0370-0374 (control, settings launch 1.5, 2026-10-26): the "Local only" removal
# takes 0375 because main took 0369 (#1486) first and the numbers below are
# held by open PRs. Each entry tolerates the number existing.
#   0372  intentionally unused
# TODO: remove each entry once its PR merges (0372 stays: it is a permanent gap).
#
# 0395 (control) and 0210 (tenant), 2026-10-29: the Library text-search index
# (#1524 blocker 5) takes control 0396-0400 and tenant 0211-0215; these are
# held by #1600, which drops the retired desktop tables after a release.
# TODO: remove each entry once its PR merges.
ALLOWED_GAPS_control="0043 0226 0295 0296 0297 0298 0372 0395"
ALLOWED_GAPS_tenant="0130 0131 0132 0133 0210"

# portable sha256
#
# `--text` is forced explicitly rather than relied on as a default: GNU
# coreutils' `sha256sum` on Git for Windows defaults to BINARY mode (a `*`
# marker before the filename) where the same tool on Linux/macOS defaults to
# TEXT mode (two spaces, no marker) — same hash, different output line, which
# `diff -q` (a byte-exact comparison) treats as every single line differing.
# Forcing the mode makes the manifest format identical regardless of platform.
sha256() {
  if command -v sha256sum >/dev/null 2>&1; then sha256sum --text "$@"
  else shasum -a 256 --text "$@"; fi
}

fail=0
err() { echo "FAIL: $*" >&2; fail=1; }

check_tree() {
  local tree="$1"           # control | tenant
  local dir="backend/migrations_${tree}"
  local allowed_var="ALLOWED_GAPS_${tree}"
  local allowed="${!allowed_var}"
  local manifest="scripts/migrations_${tree}.manifest"

  if [ ! -d "$dir" ]; then
    echo "info: $dir does not exist yet — skipping (${tree} tree not created)"
    return 0
  fi

  local files
  files=$(cd "$dir" && ls *.sql 2>/dev/null | sort || true)
  if [ -z "$files" ]; then
    echo "info: $dir has no .sql files — skipping"
    return 0
  fi

  # 1 + 2: numbering (duplicates + gaps)
  local prev=0 seen_any=0
  while IFS= read -r f; do
    local num="${f:0:4}"
    if ! [[ "$num" =~ ^[0-9]{4}$ ]]; then
      err "$tree: '$f' does not start with a 4-digit number"; continue
    fi
    local n=$((10#$num))
    if [ "$seen_any" -eq 1 ]; then
      if [ "$n" -eq "$prev" ]; then
        err "$tree: duplicate migration number $num ('$f')"
      elif [ "$n" -gt $((prev + 1)) ]; then
        # every missing number in (prev, n) must be allowlisted
        local m
        for (( m = prev + 1; m < n; m++ )); do
          local mm; mm=$(printf '%04d' "$m")
          if [[ " $allowed " != *" $mm "* ]]; then
            err "$tree: gap at $mm (between $(printf '%04d' "$prev") and $num) — not in allowlist"
          fi
        done
      fi
    fi
    prev=$n; seen_any=1
  done <<< "$files"

  # 3: checksum manifest
  local tmp; tmp="$(mktemp)"
  (cd "$dir" && sha256 *.sql | sort -k2) > "$tmp"
  if [ "${1:-}" = "" ]; then :; fi
  if [ "$MODE" = "update" ]; then
    cp "$tmp" "$manifest"
    echo "updated $manifest ($(wc -l < "$manifest" | tr -d ' ') files)"
  else
    if [ ! -f "$manifest" ]; then
      err "$tree: missing checksum manifest $manifest (run --update)"
    else
      # The manifest is committed LF (no `.gitattributes` rule forces this,
      # but every commit of it so far has been LF) — `$tmp` above is always
      # LF too (raw sha256/shasum output, never touched by a git filter). A
      # checkout with `core.autocrlf=true` (Git for Windows' own recommended
      # default) silently converts the WORKING-TREE copy of `$manifest` to
      # CRLF, which `diff -q`'s byte-exact comparison then reports as EVERY
      # line differing — a false failure with nothing to do with whether a
      # migration actually changed. Strip `\r` from a copy before comparing
      # so the check is robust to the committer's local autocrlf setting;
      # the real `$manifest` file on disk is never rewritten here.
      local normalized; normalized="$(mktemp)"
      tr -d '\r' < "$manifest" > "$normalized"
      if ! diff -q "$normalized" "$tmp" >/dev/null; then
        err "$tree: checksum manifest mismatch (a committed migration was added/edited/removed without updating $manifest)"
        diff "$normalized" "$tmp" >&2 || true
      fi
      rm -f "$normalized"
    fi
  fi
  rm -f "$tmp"
}

#
# Format: "<path>:<sha256 of the file AFTER the sanctioned edit>". An entry
# permits exactly ONE known content state and nothing else, so it does not
# become a standing licence to edit that file: the next edit changes the hash
# and the guard fails again, forcing the same deliberate review.
#
# Adding an entry requires justifying, in the PR, why a NEW migration cannot do
# the job. That bar is high and is normally not met — the reason it is met here
# is an ordering property, not a preference:
#
#   0009_tenant_rls unconditionally ran
#     ALTER ROLE coralswarm_tenant_app WITH NOBYPASSRLS NOSUPERUSER … ;
#   Postgres 16+ refuses that statement for any non-superuser caller because it
#   checks whether the SUPERUSER/BYPASSRLS/REPLICATION clause was WRITTEN, not
#   whether the value would change. Tenant databases are Neon projects and Neon
#   has no superuser, so 0009 could never succeed on ANY tenant — it aborted
#   the v184 deploy at `permission denied to alter role`.
#
#   sqlx applies migrations in version order and 0009 fails its own
#   transaction, so a 0010 is NEVER REACHED. A new migration is not a worse fix
#   here, it is an unreachable one. The only alternatives were to forge a
#   `_sqlx_migrations` row for version 9 in every tenant (present and future)
#   so sqlx skips it, and re-do all 1200 lines in 0010 — strictly worse and
#   permanently manual.
#
#   The edit is safe to make in place because 0009 has never been recorded as
#   applied ANYWHERE: it is transactional (no `-- no-transaction` marker), so
#   the one tenant that attempted it rolled back and wrote no ledger row, which
#   `tenant_rls_role_provisioning_pg` asserts directly. Dev/CI databases that
#   applied it as superuser will see a checksum mismatch and must be recreated;
#   that is called out in the runbook.
# Tenant 0151 failed in both selected tenants in Deploy 38817096608 because
# coralswarm_app belongs to the consumer plane and need not exist in a tenant.
# SQLx rolls back this transactional migration without recording version 151;
# tenant_standing_role_layout_pg reproduces that failure. A new 0152 cannot run
# past the failing 0151. Only tenant 0151 is sanctioned; applied control 0319
# remains immutable. Disposable dev/CI databases with the original 151 checksum
# must be recreated, never repaired by changing their migration ledger.
ALLOWED_EDITS=(
  "backend/migrations_tenant/0151_standing_managed_admission.sql:414cee5809d20ddb16a3e48838e4ac9100b3cdf77e3b4c0a4667d09e462fa37d"
  "backend/migrations_tenant/0009_tenant_rls.sql:530b24fdea8cfa3af5463b23a1b4edc67d23f9d4c6fba0559a3de4af4a244701"
)

# Git-based immutability guard (CI). The committed checksum manifest catches an
# edit to a merged migration that FORGETS to regenerate the manifest — but not
# one that edits a migration AND regenerates the manifest to match (the manifest
# would then agree with the tampered file). When BASE_REF is set (CI passes the
# PR base), diff against it and reject any Modification or Deletion of a file
# already under a migration tree: merged migrations are append-only. New files
# (Additions) — including this PR's rename that adds every file under
# migrations_control — are allowed.
check_immutable_vs_base() {
  [ -n "${BASE_REF:-}" ] || return 0
  command -v git >/dev/null 2>&1 || { echo "info: git unavailable, skipping base immutability check"; return 0; }
  local changed
  changed=$(git diff --no-renames --name-status "$BASE_REF"...HEAD -- \
    backend/migrations_control backend/migrations_tenant 2>/dev/null || true)
  [ -n "$changed" ] || return 0
  while IFS=$'\t' read -r status path; do
    case "$status" in
      M*)
        [[ "$path" == *.sql ]] || continue
        if edit_is_allowed "$path"; then
          echo "info: sanctioned edit to '$path' (pinned in ALLOWED_EDITS)"
        else
          err "immutable: merged migration '$path' was ${status}-modified vs $BASE_REF (migrations are append-only; add a new migration instead)"
        fi
        ;;
      D*) [[ "$path" == *.sql ]] && err "immutable: merged migration '$path' was ${status}-modified vs $BASE_REF (migrations are append-only; add a new migration instead)" ;;
    esac
  done <<< "$changed"
}

# True only if $1 is allowlisted AND its CURRENT content matches the pinned
# hash. A stale pin therefore fails closed: an allowlisted file that has since
# been edited again is rejected exactly like an unlisted one.
edit_is_allowed() {
  local path="$1" entry allowed_path allowed_sha actual_sha blob
  [ -f "$path" ] || return 1
  # Hash the COMMITTED content, not the worktree. The caller decides what to
  # examine from `git diff BASE_REF...HEAD`, i.e. from committed history, so
  # hashing a mutable worktree would let an uncommitted edit decide whether the
  # commit under review is sanctioned — the pin would be validated against
  # bytes that are not the bytes being reviewed. Via a temp file rather than a
  # command substitution because `$(...)` strips trailing newlines and would
  # produce a hash that never matches a real file's.
  #
  # An uncommitted edit is not thereby ignored: `check_tree` hashes the
  # worktree into the manifest comparison and fails on exactly that. The two
  # checks split the job — the manifest covers the worktree, this covers
  # history — and a dirty checkout fails the run as a whole.
  blob="$(mktemp)"
  if git cat-file blob "HEAD:$path" > "$blob" 2>/dev/null; then
    actual_sha=$(sha256 "$blob" | awk '{print $1}')
  else
    actual_sha=$(sha256 "$path" | awk '{print $1}')
  fi
  rm -f "$blob"
  # `${arr[@]}` on an EMPTY array is an unbound-variable error under `set -u`
  # in bash 3.2 (still the system bash on macOS). Once this allowlist is emptied
  # — which is the desired end state — an unguarded expansion would abort the
  # script mid-check rather than simply finding no match.
  for entry in ${ALLOWED_EDITS[@]+"${ALLOWED_EDITS[@]}"}; do
    allowed_path="${entry%%:*}"
    allowed_sha="${entry##*:}"
    [ "$allowed_path" = "$path" ] || continue
    if [ "$allowed_sha" = "$actual_sha" ]; then
      return 0
    fi
    err "immutable: '$path' is in ALLOWED_EDITS but its content does not match the pinned hash (pinned $allowed_sha, found $actual_sha) — a sanctioned edit permits exactly one content state; re-justify and re-pin"
    return 1
  done
  return 1
}

MODE="verify"
[ "${1:-}" = "--update" ] && MODE="update"

check_tree control
check_tree tenant
check_immutable_vs_base

if [ "$fail" -ne 0 ]; then
  echo "migration guard: FAILED" >&2
  exit 1
fi
echo "migration guard: OK"
