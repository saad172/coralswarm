#!/usr/bin/env bash
#
# verify_tenant_isolation.sh — post-provisioning isolation probes for one tenant.
#
# Usage:
#   scripts/verify_tenant_isolation.sh <slug> [--base <https://api.coralswarm.example>]
#
# --base is the API apex (`TENANT_APEX_DOMAIN`) — the ONE address that serves
# every tenant since claim-based tenancy. It is NOT the web apex:
# `coralswarm.example` is served by the marketing Cloudflare Worker
# (`marketing-web/wrangler.jsonc` binds it as a Custom Domain), so probing it
# exercises the Worker, not the API.
#
# ── What changed, and why this file was rewritten rather than edited ─────────
#
# Every probe this script used to make was of the form "send a `Host:` header
# naming an org subdomain and assert a status". Claim-based tenancy Phase 4
# cutover step 3 (`docs/plans/claim-based-tenancy.md`) RETIRED
# `<slug>.api.<apex>`, so those probes were asserting a mechanism that no
# longer exists — and, left alone, would have gone green while testing nothing.
#
# The isolation property did not go away with them. It is now proved in the
# shape the plan names: **org-A token, org-B's REAL ids, on the shared apex →
# 403/404**, with a positive control alongside it. The old script had NO
# positive control at all, so it would have passed in full against a backend
# that refused every request — including a backend that was simply down.
#
# NO DNS REQUIRED. Fly routes on TLS SNI while the API reads the `Host` header,
# so a probe can hit the deployed backend directly:
#
#   scripts/verify_tenant_isolation.sh <slug> --base https://coralswarm-backend.example
#
# is a valid pre-DNS rehearsal, provided that deploy's `TENANT_APEX_DOMAIN`
# matches the base you pass.
#
# ── Inputs (never hard-coded) ────────────────────────────────────────────────
#   ORGT_TOKEN_THIS   a valid org token for <slug>'s org                 [required]
#   ORGT_GRAPH_THIS   a REAL graph (ocean) id inside <slug>'s tenant DB  [required]
#   ORGT_TOKEN_OTHER  a valid org token for a DIFFERENT, live org        [required]
#
# `ORGT_GRAPH_THIS` is what makes the cross-tenant probe mean anything. A
# forged/random id proves only that a random id is not found; a REAL id that
# the other org must not reach is the actual isolation question. Get one with:
#
#   psql "$TENANT_DSN" -c "select id from graphs where kind='org' limit 1"
#
# `ORGT_TOKEN_LEGACY` is gone: auth is Clerk-only (the opaque session-token path
# was removed with the binding gate), so there is no legacy token to refuse.
#
# Each probe asserts an expected HTTP status; any mismatch fails the run
# (non-zero exit). This is the concierge "prove isolation before go-live" gate.

set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=scripts/lib/orgt_tenant_lib.sh
source "$HERE/lib/orgt_tenant_lib.sh"

SLUG="" BASE="https://api.coralswarm.example"
while [ $# -gt 0 ]; do
  case "$1" in
    --base) BASE="${2:-}"; shift 2 ;;
    -*) orgt_die "unknown flag: $1" ;;
    *) SLUG="$1"; shift ;;
  esac
done
[ -n "$SLUG" ] || orgt_die "usage: verify_tenant_isolation.sh <slug> [--base <url>]"
orgt_require_bin curl

apex="${BASE#https://}"; apex="${apex#http://}"; apex="${apex%%/*}"
retired_host="$SLUG.$apex"
# A slug that has never been provisioned. The retired address must answer this
# EXACTLY as it answers the real one — a difference would be an org-existence
# oracle, which is one of the things removing subdomains was meant to close.
imaginary_host="orgt-nonexistent-$SLUG.$apex"

fail=0

# probe <desc> <expected-status> <host> <path> [bearer]
probe() {
  local desc="$1" want="$2" host="$3" path="$4" bearer="${5:-}"
  local hdr=(-H "Host: $host")
  [ -n "$bearer" ] && hdr+=(-H "Authorization: Bearer $bearer")
  local got
  # On a connection failure curl BOTH prints `000` and exits non-zero, so
  # `|| true` keeps curl's own `000` rather than masking it.
  got="$(curl -s -o /dev/null -w '%{http_code}' "${hdr[@]}" "$BASE$path" || true)"
  got="${got:-000}"
  if [ "$got" = "$want" ]; then
    orgt_log "PASS ($got) $desc"
  else
    orgt_warn "FAIL want=$want got=$got — $desc"
    fail=1
  fi
}

# body <host> <path> [bearer] — the response body, for equality comparisons.
body() {
  local host="$1" path="$2" bearer="${3:-}"
  local hdr=(-H "Host: $host")
  [ -n "$bearer" ] && hdr+=(-H "Authorization: Bearer $bearer")
  curl -s "${hdr[@]}" "$BASE$path" || true
}

for v in ORGT_TOKEN_THIS ORGT_GRAPH_THIS ORGT_TOKEN_OTHER; do
  [ -n "${!v:-}" ] || orgt_die "missing required env $v — see this script's header. \
Running without it would turn a real isolation proof into a skipped line."
done

orgt_log "isolation probes for org '$SLUG' on $apex"

# ── 1. POSITIVE CONTROL, first and deliberately.
#
#    Everything below is a negative assertion, and a backend that is down,
#    misconfigured or refusing everything satisfies all of them. This is the
#    only probe here that can tell the difference. If it fails, no other
#    result in this run means anything.
probe "control: this org's own token reads its own ocean -> 200" \
  200 "$apex" "/v1/graphs/$ORGT_GRAPH_THIS/stats" "$ORGT_TOKEN_THIS"

# ── 2. ISOLATION, in the shape that survived the cutover: another live org's
#    token, on the same shared apex, against THIS tenant's REAL ocean id.
#    Both tenants are live and reachable by the same process at this moment;
#    the claim, the routing row it resolves and the tenant_meta fingerprint on
#    its pool are the whole boundary.
probe "another org's token must not reach this org's real ocean id -> 403" \
  403 "$apex" "/v1/graphs/$ORGT_GRAPH_THIS/stats" "$ORGT_TOKEN_OTHER"

# ── 3. The retired org API address (Phase 4 cutover step 3): 410 for every
#    caller, with or without a token, and NAMING the base to use instead.
probe "the retired org address -> 410, with this org's own token" \
  410 "$retired_host" "/v1/graphs" "$ORGT_TOKEN_THIS"
probe "the retired org address -> 410, unauthenticated" \
  410 "$retired_host" "/v1/graphs"
probe "a never-provisioned slug -> the same 410" \
  410 "$imaginary_host" "/v1/graphs"

# ── 4. ...and the two are INDISTINGUISHABLE. A 410 that differed between a
#    slug that exists and one that does not would be exactly the org-existence
#    oracle the subdomain removal was supposed to close.
real_body="$(body "$retired_host" "/v1/graphs")"
imaginary_body="$(body "$imaginary_host" "/v1/graphs")"
if [ "$real_body" = "$imaginary_body" ] && [ -n "$real_body" ]; then
  orgt_log "PASS the retired address answers a real slug and an imaginary one identically"
else
  orgt_warn "FAIL retired-address responses differ (org-existence oracle):"
  orgt_warn "  real=$real_body"
  orgt_warn "  imaginary=$imaginary_body"
  fail=1
fi
case "$real_body" in
  *"$apex"*) orgt_log "PASS the 410 names the replacement base ($apex)" ;;
  *) orgt_warn "FAIL the 410 does not name the base to use instead: $real_body"; fail=1 ;;
esac

# ── 5. A host that was never ours stays a 404, not a 410 — saying "gone" about
#    someone else's domain would confirm we once served it.
probe "a foreign host -> 404, never 410" 404 "orgt-not-ours.example.invalid" "/v1/graphs" \
  "$ORGT_TOKEN_THIS"

if [ "$fail" -ne 0 ]; then
  orgt_die "isolation probes FAILED — do not activate this tenant"
fi
orgt_log "isolation probes passed"
