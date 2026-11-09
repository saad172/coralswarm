#!/usr/bin/env bash
#
# check_tenant_rls_coverage.sh — the TENANT-plane RLS scope coverage gate
# (tenant RLS Phase 2, OPS-4-T). Sibling of scripts/check_rls_coverage.sh
# (the consumer-plane, `app.account_id` gate) — read that script's header
# first, this one intentionally mirrors its shape and only documents what
# differs.
#
# Before OPS-4-T can repoint a tenant DSN at the non-BYPASSRLS
# `coralswarm_tenant_app` role (Phase 3, see docs/tenant-rls-cutover-runbook.md),
# every tenant-plane query path that touches an ocean-scoped table must set the
# right session GUC — `app.user_id` (member/team identity) or
# `app.tenant_actor='system'` (the reviewed service-principal bypass) — via one
# of exactly five Rust primitives, all defined in
# backend/src/tenancy/scope.rs:
#
#   Tenant::begin_member_scoped              (Category A — the request choke point)
#   begin_tenant_scoped                      (Category B — free-function form, threads a user)
#   begin_tenant_system_scoped               (Category B/C — the reviewed bypass)
#   begin_tenant_system_scoped_without_account (Category C — the reviewed bypass for
#                                             call sites with no account_id to thread,
#                                             e.g. JobScope::Org arms)
#   begin_account_scoped                     (the OLD, pre-Phase-2 name — RESIDUE if still present)
#
# WHY A DIFFERENT DETECTION STRATEGY THAN check_rls_coverage.sh's "does this
# file reach an RLS-covered TABLE" question: that question is answered by
# grepping SQL keyword-adjacency against a table roster, because the consumer
# gate is asking "has this file been wired to `begin_account_scoped` AT ALL".
# This gate's question is narrower and simpler: tenant RLS Phase 1 already
# renamed nothing and broke nothing (it shipped zero call sites); Phase 2's
# entire job is converting EVERY existing `begin_account_scoped` call that can
# run on a tenant pool to one of the four tenant-aware primitives above. So
# residue of the OLD name, on a file that is reachable from a tenant-capable
# code path, IS the thing to detect — directly, by name, rather than
# reconstructing which SQL tables a file happens to touch. This is exactly the
# "grep-based guard over begin_account_scoped residue" the Phase-2 brief calls
# out as acceptable, made precise by the manifest below.
#
# Every backend/src file that contains ANY of the five names above (after
# stripping full-line `//` comments — same convention as check_rls_coverage.sh)
# must appear in scripts/tenant_rls_coverage.manifest with a status:
#
#   converted — every `begin_account_scoped` call in this file has been
#               converted to begin_member_scoped/begin_tenant_scoped/
#               begin_tenant_system_scoped. The script FAILS a `converted`
#               entry that still contains a residual `begin_account_scoped(`
#               call (regression guard).
#   pending   — known Category B site(s), not yet decided. Passes CI (tracked
#               progress, not a blocker — same discipline as the consumer
#               gate), but the file is real, tracked, un-silent work.
#   exempt    — reviewed: every `begin_account_scoped` call in this file is
#               genuinely consumer-plane-only (never reachable from an Org
#               tenancy decision) and correctly stays as-is. Requires a
#               `# exempt: <why>` comment directly above the entry (checked).
#
# FAILURES: a file using any of the five names but absent from the manifest; a
# `converted` file that still contains `begin_account_scoped(`; an `exempt`
# entry with no rationale comment; a manifest entry whose file no longer
# exists; a malformed/duplicate line. That is the complete list — see the
# SYSTEM-USAGE section below for what is deliberately NOT a gated failure here.
#
# SYSTEM-USAGE ROSTER (informational, NOT gated — read this before assuming
# otherwise): 0008_tenant_rls's own header requires `grep -rn
# begin_tenant_system_scoped backend/src` to "remain a complete and reviewed
# list" — every use is a real bypass of the member-ocean backstop. This script
# PRINTS that list on every run so a new one cannot be added without it showing
# up in the coverage summary where a reviewer will see it, but it does NOT
# compare the count against any threshold or fail the run — there is no
# defensible number to gate on (a codebase that legitimately needs 4 reviewed
# bypasses is not worse than one with 3; the count says nothing about whether
# each is justified). A prior draft of this comment claimed otherwise ("fewer
# than 3 ... tolerated silently") — that was untrue the moment it was written
# (`sysn` was printed, never compared), the exact "documents a guarantee it
# doesn't enforce" failure shape flagged in review. The actual review
# discipline lives in code review and docs/tenant-rls-cutover-runbook.md §2
# (the per-call-site justification table) — a human decision, not a count this
# script could correctly gate on.
#
# WHAT A GREEN RUN DOES AND DOES NOT PROVE (same honesty bar as
# check_rls_coverage.sh): this is a grep over five literal names, not a
# data-flow proof. It cannot see a `begin_account_scoped` call reached only
# through a local alias/re-export, and a `pending` entry is a real, tracked gap
# — Phase 3 must not proceed while any file is `pending` on a path an org
# tenant can reach (see the runbook's Phase 3 preconditions). It also cannot
# tell you whether a `converted` file chose `begin_tenant_scoped` vs
# `begin_tenant_system_scoped` correctly — that is a per-diff reviewer
# judgment call, recorded in the runbook, not something a grep can validate.
#
# Usage:
#   scripts/check_tenant_rls_coverage.sh           # CI mode; non-zero exit on failure
#   scripts/check_tenant_rls_coverage.sh --seed    # (re)generate the manifest

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

SCOPE_MOD="backend/src/tenancy/scope.rs"
MANIFEST="scripts/tenant_rls_coverage.manifest"
SRC_ROOT="backend/src"

MODE="verify"
[ "${1:-}" = "--seed" ] && MODE="seed"

fail=0
err() { echo "FAIL: $*" >&2; fail=1; }

if [ ! -f "$SCOPE_MOD" ]; then
  echo "FAIL: $SCOPE_MOD not found (run from the repo root)" >&2
  exit 1
fi

# The five names this gate cares about. Fixed list (not derived at runtime,
# unlike check_rls_coverage.sh's table roster) — these are Rust function
# names defined once in $SCOPE_MOD, not a migration-derived table set, so
# there is nothing to drift out from under a hand-maintained list here. A
# sanity check below still asserts all five are actually defined in
# $SCOPE_MOD, so a rename there fails loudly rather than silently going
# blind. begin_tenant_system_scoped_without_account is the reviewed bypass
# for call sites with no account_id to thread (e.g. JobScope::Org arms) —
# it does NOT share a `(`-adjacent prefix with begin_tenant_system_scoped,
# so it must be listed separately or it is invisible to PATTERN below.
NAMES="begin_account_scoped begin_member_scoped begin_tenant_scoped begin_tenant_system_scoped begin_tenant_system_scoped_without_account"
for n in $NAMES; do
  if ! grep -q "fn $n" "$SCOPE_MOD"; then
    echo "FAIL: '$n' is not defined in $SCOPE_MOD — this gate's fixed name list is stale" >&2
    exit 1
  fi
done

# Strip full-line // comments and flatten a file to one line for matching —
# identical convention to check_rls_coverage.sh's flatten().
flatten() {
  perl -ne 'print unless m{^\s*//}' "$1" | tr '\n' ' '
}

# A file "touches" this gate's surface if any of the five names appears
# followed by `(` at a non-identifier boundary (so `begin_account_scoped(`
# matches but a name embedded in a longer identifier would not — in practice
# none of these five names is ever a substring of another Rust identifier in
# this codebase).
NAME_ALT=$(echo "$NAMES" | tr ' ' '|')
PATTERN="(^|[^a-zA-Z0-9_])(${NAME_ALT})\\("

touching_files="$(mktemp)"
manifest_norm="$(mktemp)"
system_sites="$(mktemp)"
trap 'rm -f "$touching_files" "$manifest_norm" "$system_sites"' EXIT

while IFS= read -r file; do
  # scope.rs itself defines these names — its own definitions would
  # trivially match `fn begin_tenant_system_scoped(` etc. Exclude it, same
  # rationale as check_rls_coverage.sh excluding store/ (the definition site,
  # not a call site).
  case "$file" in
    "$SCOPE_MOD") continue ;;
  esac
  flat="$(flatten "$file")"
  if echo "$flat" | grep -Eq "$PATTERN"; then
    echo "$file" >> "$touching_files"
  fi
  # System-usage roster (informational — see header). Grepped from the
  # UNFLATTENED file so line numbers are real. `[_(]` (not a literal `(`)
  # so this also catches begin_tenant_system_scoped_without_account( — same
  # reviewed-bypass family, and 0008_tenant_rls's own `grep -rn
  # begin_tenant_system_scoped` requirement is a substring match that already
  # covers it. `|| true`: grep exits 1 on no match, which under
  # `set -o pipefail` would otherwise abort the whole script the first time a
  # file has zero system-scoped call sites (the overwhelming majority of
  # files).
  (grep -n 'begin_tenant_system_scoped[_(]' "$file" 2>/dev/null || true) | while IFS=: read -r ln _rest; do
    [ -n "$ln" ] && echo "$file:$ln" >> "$system_sites"
  done
done < <(find "$SRC_ROOT" -name '*.rs' | sort)

if [ "$MODE" = "seed" ]; then
  {
    echo "# Tenant-plane RLS scope conversion manifest — consumed by"
    echo "# scripts/check_tenant_rls_coverage.sh. See that script's header for the full"
    echo "# discipline; summary: status is pending | converted | exempt, an 'exempt' entry"
    echo "# needs a '# exempt: <why>' comment directly above it, and 'converted' is verified"
    echo "# (the file must contain no residual begin_account_scoped( call.)"
    echo "# Regenerate with: scripts/check_tenant_rls_coverage.sh --seed"
    while IFS= read -r file; do
      status="pending"
      if [ -f "$MANIFEST" ]; then
        prev=$(awk -v f="$file" '$1 == f { print $2 }' "$MANIFEST" | head -n1)
        [ -n "$prev" ] && status="$prev"
      fi
      if [ "$status" = "exempt" ]; then
        prev_comment=$(grep -B1 "^$file exempt$" "$MANIFEST" | head -n1)
        case "$prev_comment" in
          '# exempt:'*) echo "$prev_comment" ;;
          *) echo "# exempt: (rationale lost by --seed — restore it from git history)" ;;
        esac
      fi
      echo "$file $status"
    done < "$touching_files"
  } > "$MANIFEST.tmp"
  mv "$MANIFEST.tmp" "$MANIFEST"
  echo "seeded $MANIFEST ($(grep -cE '^[^#]' "$MANIFEST" || true) entries)"
  exit 0
fi

if [ ! -f "$MANIFEST" ]; then
  echo "FAIL: $MANIFEST missing — run scripts/check_tenant_rls_coverage.sh --seed" >&2
  exit 1
fi

lineno=0
prev_line=""
while IFS= read -r line; do
  lineno=$((lineno + 1))
  case "$line" in ''|'#'*) prev_line="$line"; continue ;; esac
  path=$(echo "$line" | awk '{print $1}')
  status=$(echo "$line" | awk '{print $2}')
  extra=$(echo "$line" | awk '{print $3}')
  if [ -z "$path" ] || [ -z "$status" ] || [ -n "$extra" ]; then
    err "manifest line $lineno malformed (want '<path> <status>'): $line"
    continue
  fi
  if [ "$status" != "pending" ] && [ "$status" != "converted" ] && [ "$status" != "exempt" ]; then
    err "manifest line $lineno: unknown status '$status' for $path (want pending|converted|exempt)"
    continue
  fi
  if [ "$status" = "exempt" ] && [ "${prev_line#\# exempt:}" = "$prev_line" ]; then
    err "manifest line $lineno: $path is 'exempt' but the line above is not a '# exempt: <why>' comment"
    continue
  fi
  if grep -q "^$path " "$manifest_norm"; then
    err "manifest line $lineno: duplicate entry for $path"
    continue
  fi
  if [ ! -f "$path" ]; then
    err "manifest lists $path but the file does not exist — remove/rename the entry"
    continue
  fi
  echo "$path $status" >> "$manifest_norm"
  prev_line="$line"
done < "$MANIFEST"

while IFS= read -r file; do
  status=$(awk -v f="$file" '$1 == f { print $2 }' "$manifest_norm")
  if [ -z "$status" ]; then
    err "$file uses one of ($NAMES) but is NOT in $MANIFEST — new (or newly-detected) tenant-\
scope surface must be reviewed and added, e.g.: echo \"$file pending\" >> $MANIFEST \
(or run scripts/check_tenant_rls_coverage.sh --seed)."
    continue
  fi
  if [ "$status" = "converted" ]; then
    if flatten "$file" | grep -Eq "(^|[^a-zA-Z0-9_])begin_account_scoped\\("; then
      err "$file is marked 'converted' in $MANIFEST but still contains a residual \
begin_account_scoped( call — either finish converting it or flip the entry back to 'pending'."
    fi
  fi
done < "$touching_files"

while read -r path _status; do
  if ! grep -qx "$path" "$touching_files"; then
    echo "note: $path is in $MANIFEST but no longer references begin_account_scoped/\
begin_member_scoped/begin_tenant_scoped/begin_tenant_system_scoped in a greppable position \
— remove the entry if its tenant-scope surface is truly gone."
  fi
done < "$manifest_norm"

converted=$(awk '$2 == "converted"' "$manifest_norm" | wc -l | tr -d ' ')
pending=$(awk '$2 == "pending"' "$manifest_norm" | wc -l | tr -d ' ')
exempt=$(awk '$2 == "exempt"' "$manifest_norm" | wc -l | tr -d ' ')
total=$((converted + pending + exempt))
echo
echo "tenant RLS scope coverage: $converted/$total files converted ($pending pending, $exempt exempt)"

sysn=$(wc -l < "$system_sites" | tr -d ' ')
echo
echo "begin_tenant_system_scoped call sites ($sysn, informational only — not gated on any"
echo "threshold, see this script's header): every one is a reviewed bypass of the member-ocean"
echo "backstop; cross-check against docs/tenant-rls-cutover-runbook.md §2's table:"
sort "$system_sites" | sed 's/^/  /'

if [ "$fail" -ne 0 ]; then
  echo
  echo "tenant-rls-coverage gate: FAILED" >&2
  exit 1
fi
echo
echo "tenant-rls-coverage gate: OK (grep-strength check — see header for what a green run does not prove)"
