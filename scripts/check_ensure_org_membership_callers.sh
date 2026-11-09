#!/usr/bin/env bash
#
# check_ensure_org_membership_callers.sh — enforces D3 (org-onboarding plan
# §3 item 31, CS-434): only the Clerk webhook and `routes::org_invites` may
# call `clerk_provision::ensure_org_membership`. Founder at review: "this
# needs to be impossible." Not a convention — a guard.
#
# THE FAILURE MODE THIS CLOSES. `ensure_org_membership`
# (backend/src/clerk_provision.rs) is the AUTHORITATIVE membership writer:
# it upserts the local `org_memberships` row AND, once that transaction
# commits, calls `crate::billing::seat_lifecycle::acquire_seat` to purchase
# a billing seat for the new member. A caller anywhere else in the request
# path writes a local membership row with no matching Clerk membership — a
# PHANTOM MEMBER: billed for a seat, but unable to ever enter the org,
# because org context is derived from the Clerk JWT `org_id` claim, not from
# this table. There is no way to notice the mistake from inside the org; the
# person just silently can't get in, and the org silently pays for them.
#
# THE TWO CALLERS THAT ARE ALLOWED, AND WHY THEY ARE SAFE:
#   * backend/src/routes/clerk_webhook.rs — `handle_membership_upsert`,
#     driven by Clerk's own `organizationMembership.created`/`.updated`
#     webhook. Clerk has already created the membership; this only mirrors
#     it locally.
#   * backend/src/routes/org_invites.rs — `create_invite` calls the Clerk
#     API directly (`create_org_membership`) in the SAME request, before it
#     calls `ensure_org_membership`, so the Clerk-side membership already
#     exists by the time the local row is written (see that module's own
#     doc comment for the unwind-on-failure story).
# A future caller must go through one of those two files. A shared helper
# extracted FROM `routes::org_invites` (Phase C's approval path, per the
# CS-434 brief) is fine — it still lives in the allowlisted file. A third
# route file calling `ensure_org_membership` directly is exactly what this
# guard exists to fail.
#
# WHAT A GREEN RUN DOES AND DOES NOT PROVE:
#   * It is a grep over symbol references, not a call-graph proof. A call
#     reached through a trait object, a function pointer, or a re-exported
#     alias for `ensure_org_membership` would not be seen.
#   * Scope is `backend/src/**/*.rs` only. `backend/tests/*.rs` integration
#     tests are out of scope on purpose — they are not a request path, and
#     `clerk_provision.rs`'s own `#[cfg(test)] mod pg_tests` calls are
#     already covered by that file's allowlist entry below.
#   * It does not parse `#[cfg(test)]` boundaries — the allowlist covers the
#     defining file wholesale instead, which is simpler and cannot be fooled
#     by a `#[cfg(test)]` attribute copy-pasted onto production code.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# A single substring pattern catches every real spelling of the call —
# `ensure_org_membership(`, `prov::ensure_org_membership(`,
# `clerk_provision::ensure_org_membership(`, and
# `crate::clerk_provision::ensure_org_membership(` — because all of them
# contain this literal text. It does NOT match the function's own
# definition line differently from a call; that's fine, because the
# defining file is allowlisted wholesale below. The `[a-z_]*` suffix also
# catches policy variants such as `ensure_org_membership_with_seat_policy(`.
FORBIDDEN_PATTERN='ensure_org_membership[a-z_]*('

# Exact repo-relative paths allowed to reference `ensure_org_membership(`.
# Hardcoded rather than manifest-driven (unlike the I11 connector guard)
# because this list is POLICY — it names the two trusted request paths from
# D3 — not an inventory that is expected to grow on its own.
ALLOWLIST=(
  # The definition itself, plus its own `#[cfg(test)] mod pg_tests` calls
  # (clerk_provision.rs:2223,2437,2456,2556,2615 as of CS-434). Allowlisting
  # the whole file — rather than trying to parse the `#[cfg(test)]`
  # boundary — is deliberate: a `#[cfg(test)]` attribute is just text to a
  # grep guard, so parsing it would let production code hide behind a
  # copy-pasted attribute. A file-level allowlist entry can't be fooled that
  # way; it can only be gamed by adding a genuinely new production caller to
  # this same file, which a reviewer of this file's diff is squarely placed
  # to catch.
  'backend/src/clerk_provision.rs'
  # `handle_membership_upsert`, driven by Clerk's own
  # `organizationMembership.created`/`.updated` webhook — Clerk has already
  # created the membership; this only mirrors it locally.
  'backend/src/routes/clerk_webhook.rs'
  # `create_invite` — creates the Clerk-side membership itself, in the same
  # request, before calling `ensure_org_membership`. Also Phase C's future
  # approval-path home (CS-434 brief): a helper extracted FROM this file
  # stays allowlisted; a new route file does not.
  'backend/src/routes/org_invites.rs'
  # `create_org` (self-serve signup A2): creates the Clerk org with the
  # creator as `created_by`, which makes Clerk's `org:admin` membership in
  # the same call, before calling `ensure_org_membership` to mirror it.
  'backend/src/routes/orgs_create.rs'
)

usage() {
  echo "usage: $0 [--self-test]" >&2
}

# ---------------------------------------------------------------------------
# is_allowlisted <repo-relative-path>
# ---------------------------------------------------------------------------
is_allowlisted() {
  local rel="$1" entry
  for entry in "${ALLOWLIST[@]}"; do
    if [[ "$rel" == "$entry" ]]; then
      return 0
    fi
  done
  return 1
}

# ---------------------------------------------------------------------------
# scan_file <path> <label>
# Prints one "  label:line" per violating match. Full-line `//` comments
# (including `///` and `//!` doc comments) are stripped first, so this
# script's own doc-comment mentions of "ensure_org_membership(" never count.
# ---------------------------------------------------------------------------
scan_file() {
  local file="$1" label="$2" stripped hits
  stripped="$(sed -E 's!^[[:space:]]*//.*$!!' "$file")"
  hits="$(printf '%s\n' "$stripped" | grep -n "$FORBIDDEN_PATTERN" || true)"
  if [[ -n "$hits" ]]; then
    while IFS= read -r line; do
      [[ -z "$line" ]] && continue
      echo "  $label:${line%%:*}"
    done <<< "$hits"
  fi
}

# ---------------------------------------------------------------------------
# validate_allowlist
# Fails loudly if an allowlisted path has drifted from the repo (renamed or
# moved), rather than silently scanning nothing for it — the same "don't
# rot into a blanket exemption without someone noticing" property the I11
# connector manifest has via its own stale-entry check.
# ---------------------------------------------------------------------------
validate_allowlist() {
  local entry
  for entry in "${ALLOWLIST[@]}"; do
    if [[ ! -f "$ROOT/$entry" ]]; then
      echo "FAIL: allowlisted path does not exist: $entry" >&2
      echo "The allowlist in this script has drifted from the repo (the file" >&2
      echo "was renamed or moved) — update ALLOWLIST." >&2
      exit 1
    fi
  done
}

# ---------------------------------------------------------------------------
# run_scan
# Walks backend/src/**/*.rs, skips allowlisted files, and prints every
# violation found in the rest. Shared by the normal run and by the
# self-test's assertions, so those assertions can never drift out of sync
# with the real scan logic.
#
# Fails closed (CodeRabbit round 1, PR #1016) if the walk finds zero files:
# `find` run through process substitution below doesn't propagate its exit
# status to this function under `set -e`, so a missing/inaccessible
# backend/src (wrong ROOT, a repo restructure, a broken checkout) would
# otherwise make this function silently emit nothing — and both the normal
# run and the self-test's baseline check treat "no output" as "no
# violations" and report OK. backend/src always has thousands of .rs files,
# so zero scanned is never legitimate; it always means the walk is broken,
# not that the codebase is clean.
# ---------------------------------------------------------------------------
run_scan() {
  validate_allowlist
  local file rel out scanned=0
  while IFS= read -r -d '' file; do
    scanned=$((scanned + 1))
    rel="${file#"$ROOT"/}"
    if is_allowlisted "$rel"; then
      continue
    fi
    out="$(scan_file "$file" "$rel")"
    if [[ -n "$out" ]]; then
      printf '%s\n' "$out"
    fi
  done < <(find "$ROOT/backend/src" -type f -name '*.rs' -print0)
  if (( scanned == 0 )); then
    echo "FAIL: scanned zero .rs files under backend/src — the walk found" >&2
    echo "nothing (missing directory, wrong ROOT, or a broken find). A guard" >&2
    echo "that scans nothing would otherwise report OK vacuously; refusing" >&2
    echo "to do that instead." >&2
    exit 1
  fi
}

# ---------------------------------------------------------------------------
# --self-test: prove the guard can actually fail, and prove the allowlist
# is load-bearing (not just present).
# ---------------------------------------------------------------------------
if [[ "${1:-}" == "--self-test" ]]; then
  baseline="$(run_scan)"
  if [[ -n "$baseline" ]]; then
    echo "SELF-TEST FAIL: the guard reports violations with nothing planted —" >&2
    echo "the allowlist is not suppressing the two real callers:" >&2
    echo "$baseline" >&2
    exit 1
  fi

  fake="$ROOT/backend/src/routes/__cs434_selftest_fake.rs"
  trap 'rm -f "$fake"' EXIT
  cat > "$fake" <<'EOF'
// A commented-out call must NOT count: ensure_org_membership(&state, &pool, org_id, user_id, "member", true).await?;
pub async fn sneaky_admin_promote(state: &AppState, pool: &PgPool, org_id: &str, user_id: Uuid) -> Result<(), ProvisionError> {
    crate::clerk_provision::ensure_org_membership(state, pool, org_id, user_id, "admin", false).await
}
EOF

  # Goes through run_scan (the real find-based walk), not a direct scan_file
  # call on the fixture (CodeRabbit round 1, PR #1016) — proving the walk
  # actually discovers a newly created file under backend/src/routes and
  # that the allowlist keeps staying silent on the real callers while this
  # one file is reported, not just that scan_file's regex works in
  # isolation.
  planted="$(run_scan)"
  if [[ -z "$planted" ]]; then
    echo "SELF-TEST FAIL: run_scan did not flag a planted call to ensure_org_membership." >&2
    exit 1
  fi
  if [[ "$(printf '%s\n' "$planted" | wc -l | tr -d '[:space:]')" != "1" ]]; then
    echo "SELF-TEST FAIL: expected exactly one hit (the planted file's real call" >&2
    echo "on line 3), got:" >&2
    echo "$planted" >&2
    exit 1
  fi
  if ! printf '%s\n' "$planted" | grep -q '__cs434_selftest_fake\.rs:3$'; then
    echo "SELF-TEST FAIL: the hit is not the planted file's line 3 — either the" >&2
    echo "real call was missed, the commented-out line 1 counted, or an" >&2
    echo "unrelated violation appeared elsewhere:" >&2
    echo "$planted" >&2
    exit 1
  fi

  rm -f "$fake"
  trap - EXIT

  echo "OK: self-test passed — flags a planted call, ignores a commented-out one, allowlist suppresses the two real callers."
  exit 0
fi

if [[ $# -gt 0 ]]; then
  usage
  exit 2
fi

violations="$(run_scan)"

if [[ -n "$violations" ]]; then
  echo "FAIL: a request path outside the Clerk webhook and routes::org_invites calls ensure_org_membership." >&2
  echo "" >&2
  echo "Invariant D3 (org-onboarding plan §3 item 31, CS-434). ensure_org_membership" >&2
  echo "is the AUTHORITATIVE membership writer — it also purchases a billing seat" >&2
  echo "(crate::billing::seat_lifecycle::acquire_seat). A caller outside the two" >&2
  echo "trusted paths writes a local membership row with no matching Clerk" >&2
  echo "membership: a phantom member, billed for a seat, who can never enter the" >&2
  echo "org (org context comes from the Clerk JWT org_id claim, not this table)." >&2
  echo "" >&2
  echo "Fix: route membership writes through routes::org_invites or the Clerk" >&2
  echo "webhook handler (backend/src/routes/clerk_webhook.rs) instead of calling" >&2
  echo "ensure_org_membership directly." >&2
  echo "" >&2
  echo "$violations" >&2
  exit 1
fi

echo "OK: only the Clerk webhook, routes::org_invites and routes::orgs_create call ensure_org_membership."
