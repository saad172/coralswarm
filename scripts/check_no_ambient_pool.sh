#!/usr/bin/env bash
#
# check_no_ambient_pool.sh — the "no ambient pool under routes/" grep-guard
# (org-tenancy PR 1.6).
#
# Post-split, a route handler must never reach for the ambient
# `state.db` / `state.store` for CONTENT-plane access: an org-bound request's
# content lives only in that org's own tenant database, and the `Tenant`
# axum extractor (backend/src/tenancy/extractor.rs) is the one place a
# handler resolves which pool/store to use. This guard is a CI LINT for that
# rule: it FAILS if `state.db` / `state.store` appears in CODE under
# backend/src/routes/ outside the explicit allowlist below.
#
# WHAT A GREEN RUN DOES AND DOES NOT PROVE (read before trusting it):
#   * It forbids the COMMON ambient-pool pattern (`state.db` / `state.store`
#     field access on an identifier ending in `state`) in route files. It is
#     a strong lint, NOT a data-flow proof: rebinds (`let s = state.clone();
#     s.db`), destructuring (`let AppState { db, .. } = &*state;`), and a
#     field access split across lines all evade the pattern. Reviewers still
#     own those shapes per-diff.
#   * `state.control_db()` / `state.control_store()` are a deliberate escape
#     hatch the pattern does not match — and they return the SAME physical
#     consumer pool. They are CONTROL-TABLE-ONLY accessors (accounts,
#     account_members, account_prefs, users, calendar_prefs, …); a
#     content-table read through them (e.g. `SELECT … FROM atoms`) would
#     silently reintroduce the ambient-pool bug and is forbidden by
#     convention, NOT caught here. See their doc comments in
#     backend/src/lib.rs.
#   * Scope is backend/src/routes/*.rs ONLY. Helpers elsewhere that take
#     &AppState and read content on state.db (e.g. services/ask.rs, a known
#     pre-existing case tracked for the cutover work) are covered by
#     convention + review, not by this scan.
#
# What counts as a violation (deliberately narrow, matching the reviews of
# PRs 1.4/1.5):
#   * CODE only, never comments — migrated files carry doc comments that
#     mention `state.db`/`state.store` by name; full-line `//`/`///`/`//!`
#     comments are stripped, and trailing `// ...` comments are stripped
#     when no string literal opens before them (when one does, the line is
#     kept whole — erring toward a false positive, never a false negative).
#   * `$state.db` / `$state.store` inside a macro DEFINITION body is a
#     metavariable, not an ambient access, and is excluded — what matters is
#     what each macro INVOCATION passes (`require_store!(tenant)` vs a
#     control-plane macro), which is reviewed, not grepped.
#   * Legitimate CONTROL-plane reads in routes (e.g. `account_prefs`,
#     `accounts`, `calendar_prefs`) do not use the raw fields at all — they
#     go through the named `state.control_db()` / `state.control_store()`
#     accessors (backend/src/lib.rs), which both documents the plane choice
#     and keeps this guard's pattern clean.
#
# THE ALLOWLIST (the one obvious place — keep it in sync with the org-tenancy
# implementation plan's "Control/consumer allowlist"): these files stay on the
# ambient consumer/control plane BY DESIGN and are exempt. Every entry below
# was re-audited (PR 1.6 security review) against its actual mounts in
# lib.rs, not its label:
#   * Control-plane account/identity routes (JWT-authed, but every table they
#     touch is control-tree): me.rs (users), prefs.rs (account_prefs),
#     escrow.rs (escrow_* / user_private_keys / account_members),
#     private_key.rs (user_private_keys), clerk_webhook.rs (svix-signed
#     webhook; control accounts/users provisioning).
#   * admin.rs: no public JWT routes at all (the former admin_list_users 501
#     stub was deleted, platform-admin-metrics PR 6 of 7). Every handler in
#     this file — rotate_dek, backfill_atom_metadata, and the five
#     /internal/admin/* platform-metrics reporting endpoints — is mounted
#     ONLY on the loopback/private-network /internal router (x-internal-token,
#     never behind the binding gate) — the plan tracks the internal router's
#     tenancy separately (PR 1.3 added rotate-dek's ?tenant selector).
#   * oauth.rs: allowlisted for its OPEN provider-redirect machinery
#     (authorize/admin_consent/callback — no Clerk JWT, consumer plane by
#     design). Its three JWT-authed handlers (mint_state, list_connections,
#     delete_connection) are org-gated IN CODE (reject_org_oauth_management,
#     lifted for Slack only behind ORG_SLACK_OAUTH_ENABLED — org-slack step 5)
#     and resolve their pool via state.control_db() (oauth_tokens is a
#     CONTROL-only table; the Tenant-resolved pool would be the org's TENANT
#     pool once an org account can reach these routes) — the allowlist only
#     exempts the file from this grep, it does not exempt it from the gate.
#   * Signature/subscription-authenticated webhook endpoints — they carry NO
#     Clerk JWT, hit the apex host, and resolve state on the consumer DB (a
#     tenant pool could never be selected for them):
#       slack_events.rs slack_interactivity.rs teams_bot.rs graph_webhook.rs
#       google_calendar_push.rs google_push.rs
#     (routes/slack.rs's deprecated `webhook` handler delegates into
#     slack_events.rs's funnel for the same reason — the funnel code lives in
#     the allowlisted file, keeping slack.rs's JWT-authed routes enforced.)
#     google_push.rs (Pub/Sub OIDC-bearer-verified) and
#     google_calendar_push.rs (per-channel X-Goog-Channel-Token, constant-
#     time compared) are both mounted on the `open` router in lib.rs (no
#     Clerk JWT, no `Tenant` extractor possible), with fail-closed auth
#     before any DB access. google_push.rs's Gmail path resolves each
#     watching account's plane through crypto::DekPlane (an org account's
#     own tenant pool, AccountPlane::Org) the same way its Chat handlers
#     do — the 2026-08-25 "hardcoded AccountPlane::Personal" gap recorded
#     here is closed; the entry stays correct either way (the route has no
#     Clerk JWT and cannot carry a Tenant extractor).
#   * teams_ingest.rs is deliberately NOT here (security-review P0): it is a
#     Clerk-JWT content connector (the webhook is teams_bot.rs), so it is
#     org-gated + Tenant-migrated like the other connectors and must pass
#     this scan.
#
# Usage:
#   scripts/check_no_ambient_pool.sh    # CI mode; non-zero exit on violation
#
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

ROUTES_DIR="backend/src/routes"

# Explicit file allowlist (basenames). Additions require the same review a
# plan change would get — a file on this list is a file whose AMBIENT pool
# access is a deliberate design decision, not a migration TODO.
ALLOWLIST=(
  me.rs
  prefs.rs
  escrow.rs
  private_key.rs
  admin.rs
  clerk_webhook.rs
  oauth.rs
  slack_events.rs
  slack_interactivity.rs
  teams_bot.rs
  graph_webhook.rs
  google_calendar_push.rs
  google_push.rs
)

if [ ! -d "$ROUTES_DIR" ]; then
  echo "FAIL: $ROUTES_DIR does not exist (run from the repo root)" >&2
  exit 1
fi

is_allowlisted() {
  local base="$1"
  for allowed in "${ALLOWLIST[@]}"; do
    if [ "$base" = "$allowed" ]; then
      return 0
    fi
  done
  return 1
}

# Sanity: every allowlisted file should exist — a rename/removal must update
# this list in the same PR, or the guard silently loses coverage intent.
missing=0
for allowed in "${ALLOWLIST[@]}"; do
  if [ ! -f "$ROUTES_DIR/$allowed" ]; then
    echo "FAIL: allowlisted file $ROUTES_DIR/$allowed does not exist — update the allowlist in $0" >&2
    missing=1
  fi
done

# The scan itself: perl for the comment-stripping + lookbehind logic.
#   - skip full-line comments (`//`, `///`, `//!` after optional whitespace);
#   - strip a trailing `//...` comment only when the prefix before it has an
#     even number of double quotes (i.e. the `//` is not inside a string);
#   - flag `state.db` / `state.store` (any identifier ending in `state`, so
#     `app_state.db` is caught too) unless preceded by `$` (macro metavar).
violations_file="$(mktemp)"
trap 'rm -f "$violations_file"' EXIT

scan_file() {
  local file="$1"
  perl -ne '
    my $line = $_;
    next if $line =~ m{^\s*//};                 # full-line (doc) comment
    my $i = index($line, "//");
    if ($i >= 0) {
      my $prefix = substr($line, 0, $i);
      my $quotes = () = $prefix =~ /"/g;
      $line = $prefix if $quotes % 2 == 0;      # trailing comment, not in-string
    }
    if ($line =~ /(?<!\$)\b\w*state\s*\.\s*(?:db|store)\b/) {
      chomp(my $shown = $_);
      print "'"$file"':$.: $shown\n";
    }
    close ARGV if eof;                          # reset $. per file
  ' "$file"
}

for file in "$ROUTES_DIR"/*.rs; do
  base="$(basename "$file")"
  if is_allowlisted "$base"; then
    continue
  fi
  scan_file "$file" >> "$violations_file"
done

if [ -s "$violations_file" ] || [ "$missing" -ne 0 ]; then
  if [ -s "$violations_file" ]; then
    echo "FAIL: ambient consumer-pool access (state.db / state.store) in route CODE" >&2
    echo "outside the control/consumer allowlist:" >&2
    echo >&2
    cat "$violations_file" >&2
    echo >&2
    cat >&2 <<'EOF'
Route handlers must resolve content-plane access through the `Tenant` axum
extractor (backend/src/tenancy/extractor.rs) — Personal requests get the same
legacy handles, org requests get their fingerprint-verified tenant pool.
Reaching for `state.db`/`state.store` directly would silently serve or write
an org request's data on the shared consumer plane (a cross-plane leak).

If the access is a legitimate CONTROL-plane read (accounts / account_members /
account_prefs / calendar_prefs — tables that exist only on the control DB),
use the named accessors `state.control_db()` / `state.control_store()` and
say so in a comment at the call site. Those accessors are CONTROL-TABLE-ONLY:
this guard cannot see what you query through them, so a content-table read
via control_db() is the same bug wearing a different name — don't.

If this is a NEW consumer-plane webhook route (signature/subscription auth,
no Clerk JWT, apex-only), add its file to the ALLOWLIST in
scripts/check_no_ambient_pool.sh — with review, in the same PR.
EOF
  fi
  exit 1
fi

echo "OK: no ambient state.db/state.store pattern in route code under $ROUTES_DIR (allowlist: ${#ALLOWLIST[@]} files; lint-strength check — see header for documented escape hatches)"
