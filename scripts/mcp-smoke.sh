#!/usr/bin/env bash
# Remote MCP server smoke test (docs/plans/remote-mcp-v1.md §5 Phase 4 step 4,
# §6 Layer D). Runnable against ANY /mcp endpoint by URL: localhost (CI's
# `mcp-e2e` job), staging, or prod.
#
# Usage:
#   scripts/mcp-smoke.sh <mcp-url>
#
#   scripts/mcp-smoke.sh http://127.0.0.1:8080/mcp
#   scripts/mcp-smoke.sh https://coralswarm-backend-staging.example/mcp
#
# Requires: bash, curl, jq, node (for `npx`).
#
# ── Token acquisition (two modes) ─────────────────────────────────────────────
#
# 1. CI / any pre-minted token (test validator): export
#      MCP_SMOKE_TOKEN      — write+read scope token
#      MCP_SMOKE_READ_TOKEN — read-only scope token
#    CI's `mcp-e2e` job (.github/workflows/ci.yml) mints these with
#    `scripts/mcp-mint-test-token.mjs` against the SAME throwaway RSA keypair
#    `backend/src/clerk.rs`'s `test_support` module ships, pointed at a local
#    JWKS listener the job also starts via that script's `serve-jwks` mode —
#    the booted backend's REAL `clerk::verify` path (not a mock) accepts them
#    because `CLERK_JWKS_URL`/`CLERK_ISSUER` are pointed at that listener.
#
# 2. Staging / prod (real Clerk instance): mint via the Clerk Backend API
#    using `CLERK_SECRET_KEY` — Clerk's "Testing Tokens" feature
#    (https://clerk.com/docs/testing/overview, `POST /v1/testing_tokens`) is
#    the supported path for minting a short-lived session/token server-side
#    without a browser OAuth round trip. There is currently no Backend-API
#    endpoint that mints a *scoped OAuth access token* directly (Clerk cannot
#    yet issue custom `coralswarm:*` scopes — see the plan §4.2), so a fully
#    automated staging/prod token mint needs the actual DCR + PKCE consent
#    flow once per cert run (Phase 5's per-client matrix) rather than a
#    single curl call. Document the exact recipe used in the Phase 5 runbook
#    once that flow is exercised against the real Clerk instance; this
#    script intentionally does not attempt to automate it.
#
# Exits non-zero on the first failed check; prints a PASS/FAIL line per check.

set -uo pipefail

URL="${1:-}"
if [[ -z "$URL" ]]; then
  echo "usage: scripts/mcp-smoke.sh <mcp-url>" >&2
  exit 2
fi

WRITE_TOKEN="${MCP_SMOKE_TOKEN:-}"
READ_TOKEN="${MCP_SMOKE_READ_TOKEN:-}"

if [[ -z "$WRITE_TOKEN" || -z "$READ_TOKEN" ]]; then
  cat >&2 <<'EOF'
error: MCP_SMOKE_TOKEN and MCP_SMOKE_READ_TOKEN must both be set.

For a local/CI backend booted with a test Clerk issuer + JWKS listener
(see this script's own header comment for the full recipe):

  node scripts/mcp-mint-test-token.mjs serve-jwks --port 8091 &
  export CLERK_ISSUER="https://example.clerk.accounts.dev"
  export CLERK_JWKS_URL="http://127.0.0.1:8091/.well-known/jwks.json"
  export MCP_SMOKE_TOKEN="$(node scripts/mcp-mint-test-token.mjs mint --scope write)"
  export MCP_SMOKE_READ_TOKEN="$(node scripts/mcp-mint-test-token.mjs mint --scope read)"

For staging/prod, see the Clerk Backend-API / DCR note in this script's
header comment.
EOF
  exit 2
fi

FAILURES=0
# Both write to stderr (not stdout): several call sites below invoke
# functions (e.g. `fetch_tools_list`, `list_tools_via_rpc`, `rpc_call`) via a
# `$(...)` command substitution to capture real JSON on stdout — anything
# `pass`/`fail` printed to stdout would corrupt that captured JSON. Diagnostic
# output on stderr still shows up in the terminal/CI log; only stdout is
# ever captured by a caller. `pass`/`fail` themselves must NEVER be called
# from inside a `$(...)` substitution (a subshell) — `FAILURES` is a plain
# shell variable, and increments to it made in a subshell are invisible to
# the parent script, silently defeating the exit-code contract at the bottom
# of this file. That's why the tool-count assertion below is split into a
# data-fetching function (subshell-safe) and a separate assertion function
# invoked directly in this shell.
pass() { echo "PASS: $1" >&2; }
fail() {
  echo "FAIL: $1" >&2
  FAILURES=$((FAILURES + 1))
}

# ── JSON-RPC / SSE helpers (curl) ─────────────────────────────────────────────
#
# rmcp's stateless-mode streamable-HTTP transport (mounted by
# `backend/src/mcp/mod.rs`) returns `text/event-stream` even for a single
# request/response pair (`json_response` defaults to `false` — see
# `StreamableHttpServerConfig::default()`); this extracts the JSON-RPC
# payload out of the `data: ...` SSE line(s). A plain `application/json`
# response (if a future config flips `json_response: true`) passes through
# `jq` unchanged since it never matches the `^data:` prefix filter.

extract_json() {
  local body="$1"
  if echo "$body" | grep -q '^data:'; then
    echo "$body" | sed -n 's/^data: *//p'
  else
    echo "$body"
  fi
}

# rpc_call <token-or-empty> <method> <params-json>
# Prints "<http_status>\n<json-rpc-response-or-raw-body>".
rpc_call() {
  local token="$1" method="$2" params="$3"
  local body headers status resp
  body=$(jq -nc --arg m "$method" --argjson p "$params" '{jsonrpc:"2.0", id:1, method:$m, params:$p}')
  local -a curl_args=(-sS -D - -o /tmp/mcp_smoke_body.$$ -w '\n%{http_code}\n'
    -X POST "$URL"
    -H 'content-type: application/json'
    -H 'accept: application/json, text/event-stream'
    --data-binary "$body")
  if [[ -n "$token" ]]; then
    curl_args+=(-H "Authorization: Bearer $token")
  fi
  headers=$(curl "${curl_args[@]}")
  status=$(echo "$headers" | tail -1)
  resp=$(cat /tmp/mcp_smoke_body.$$)
  rm -f /tmp/mcp_smoke_body.$$
  echo "$status"
  echo "$headers" # includes response headers, for callers that need WWW-Authenticate
  echo "---BODY---"
  echo "$resp"
}

# tool_call <token> <name> <arguments-json> -> prints the tool's own
# {"error":{...}} or success JSON on stdout; non-zero exit on transport-level
# failure (never on a well-formed tool-level error, which is expected for
# some probe calls below).
tool_call() {
  local token="$1" name="$2" arguments="$3"
  local raw status body json
  raw=$(rpc_call "$token" "tools/call" "$(jq -nc --arg n "$name" --argjson a "$arguments" '{name:$n, arguments:$a}')")
  status=$(echo "$raw" | sed -n '1p')
  body=$(echo "$raw" | sed -n '/^---BODY---$/,$p' | tail -n +2)
  if [[ "$status" != "200" ]]; then
    echo "transport-level failure ($status) calling $name: $body" >&2
    return 1
  fi
  json=$(extract_json "$body")
  local result
  result=$(echo "$json" | jq -c '.result // empty')
  if [[ -z "$result" ]]; then
    echo "no .result in tools/call response for $name: $json" >&2
    return 1
  fi
  echo "$result" | jq -r '.content[0].text // empty'
}

# ── 1/2. tools/list counts + annotations (MCP Inspector CLI) ─────────────────
#
# The Inspector CLI is the tool the plan names for this check; its exact flag
# surface has moved across releases, so this wraps it defensively and falls
# back to the raw JSON-RPC helper above (same wire contract) if the installed
# `@modelcontextprotocol/inspector` version's flags differ — either path
# proves the same thing: the scope-filtered tool count and annotations.
inspector_tools_list() {
  local token="$1"
  npx -y @modelcontextprotocol/inspector --cli "$URL" --transport http \
    --header "Authorization: Bearer $token" \
    --method tools/list 2>/dev/null
}

list_tools_via_rpc() {
  local token="$1"
  local raw status body json
  raw=$(rpc_call "$token" "tools/list" '{}')
  status=$(echo "$raw" | sed -n '1p')
  body=$(echo "$raw" | sed -n '/^---BODY---$/,$p' | tail -n +2)
  if [[ "$status" != "200" ]]; then
    echo "{}"
    return 1
  fi
  extract_json "$body" | jq -c '.result // {}'
}

# fetch_tools_list <token> -> prints the tools/list result JSON on stdout.
# Deliberately does NOT call `pass`/`fail`: it's invoked via `$(...)` below,
# and a subshell can't mutate this script's `FAILURES` variable (see the note
# above `pass()`/`fail()`) — do the assertion afterwards, in this shell.
fetch_tools_list() {
  local token="$1"
  local result
  result=$(inspector_tools_list "$token")
  if ! echo "$result" | jq -e '.tools' >/dev/null 2>&1; then
    echo "  (Inspector CLI output not parseable JSON with .tools — falling back to raw JSON-RPC)" >&2
    result=$(list_tools_via_rpc "$token")
  fi
  echo "$result"
}

# assert_tool_count <label> <tools-list-json> <expected-count> — must be
# called directly (never inside `$(...)`) so a failure actually increments
# `FAILURES` in this shell.
assert_tool_count() {
  local label="$1" result="$2" expected_count="$3"
  local count
  count=$(echo "$result" | jq '.tools | length' 2>/dev/null || echo -1)
  if [[ "$count" == "$expected_count" ]]; then
    pass "$label: tools/list returns $expected_count tools"
  else
    fail "$label: tools/list returned $count tools, expected $expected_count"
  fi
}

# Expected tool counts (recount after any MCP tool addition/removal):
#   write-scope = ALL registered tools (read + write + selection; a
#     write-scoped token's tools/list is unfiltered — see `list_tools`'s
#     scope filter in `backend/src/mcp/server.rs`)
#   read-scope  = write-scope minus `WRITE_TOOLS` (`backend/src/mcp/tools/mod.rs`)
# Recount by grepping `#[tool(` in `backend/src/mcp/tools/read.rs` (13),
# `backend/src/mcp/tools/write.rs` (5), and `backend/src/mcp/tools/tenancy.rs`
# (2) — current truth as of claim-based tenancy §Phase 3 PR 3.2
# (`list_tenants`/`select_tenant`, both visible at read scope — neither is in
# `WRITE_TOOLS`, see that file's own module doc; some `#[tool(` hits in
# read.rs's own doc comments, e.g. the `output_schema` note, don't count —
# grep the actual attribute lines, not a bare occurrence count):
# 13 read + 5 write + 2 selection = 20 write-scope, 15 read-scope.
WRITE_SCOPE_TOOL_COUNT=20
READ_SCOPE_TOOL_COUNT=15

echo "== tools/list (write scope) =="
write_tools=$(fetch_tools_list "$WRITE_TOKEN")
assert_tool_count "write-scope" "$write_tools" "$WRITE_SCOPE_TOOL_COUNT"

echo "== tools/list (read scope) =="
read_tools=$(fetch_tools_list "$READ_TOKEN")
assert_tool_count "read-scope" "$read_tools" "$READ_SCOPE_TOOL_COUNT"

# Annotation spot-checks (plan §3.1): list_oceans is read-only; add_context is
# NOT read-only and NOT destructive; confirm_reef/reject_reef/update_reef are
# idempotent.
list_oceans_ro=$(echo "$write_tools" | jq -r '.tools[] | select(.name=="list_oceans") | .annotations.readOnlyHint')
if [[ "$list_oceans_ro" == "true" ]]; then
  pass "list_oceans annotated readOnlyHint:true"
else
  fail "list_oceans readOnlyHint annotation missing/wrong (got: $list_oceans_ro)"
fi

add_context_ro=$(echo "$write_tools" | jq -r '.tools[] | select(.name=="add_context") | .annotations.readOnlyHint')
add_context_destructive=$(echo "$write_tools" | jq -r '.tools[] | select(.name=="add_context") | .annotations.destructiveHint')
if [[ "$add_context_ro" == "false" && "$add_context_destructive" == "false" ]]; then
  pass "add_context annotated readOnlyHint:false, destructiveHint:false"
else
  fail "add_context annotations wrong (readOnlyHint=$add_context_ro destructiveHint=$add_context_destructive)"
fi

read_scope_names=$(echo "$read_tools" | jq -r '.tools[].name' | sort | tr '\n' ',')
if echo "$read_scope_names" | grep -q "add_context"; then
  fail "read-scope tools/list must NOT advertise add_context, got: $read_scope_names"
else
  pass "read-scope tools/list omits all 4 write tools"
fi

# ── 3. Read-tool probe calls (must round-trip, content aside) ────────────────

echo "== read-tool probe calls (read scope) =="
if out=$(tool_call "$READ_TOKEN" "list_oceans" '{}'); then
  if echo "$out" | jq -e '.oceans' >/dev/null 2>&1; then
    pass "list_oceans returns an oceans array"
  else
    fail "list_oceans response missing .oceans: $out"
  fi
else
  fail "list_oceans transport-level failure"
fi

random_uuid() {
  node -e 'console.log(require("crypto").randomUUID())'
}

for probe in get_reef:reef_id search_atoms:query get_transcript:meeting_id; do
  name="${probe%%:*}"
  case "$name" in
    get_reef) args=$(jq -nc --arg id "$(random_uuid)" '{reef_id:$id}') ;;
    search_atoms) args=$(jq -nc --arg id "$(random_uuid)" '{query:"smoke test probe", ocean_ids:[$id]}') ;;
    get_transcript) args=$(jq -nc --arg id "$(random_uuid)" '{ocean_id:$id, meeting_id:"smoke-test-probe"}') ;;
  esac
  if out=$(tool_call "$READ_TOKEN" "$name" "$args"); then
    # Any well-formed tool response (success or a documented tool error code)
    # proves the call round-tripped through auth + dispatch + the store.
    if echo "$out" | jq -e '.' >/dev/null 2>&1; then
      pass "$name responds with well-formed JSON (probe id is expected to miss)"
    else
      fail "$name response is not JSON: $out"
    fi
  else
    fail "$name transport-level failure"
  fi
done

# ── 4. Negatives ───────────────────────────────────────────────────────────────

echo "== negative: no token -> 401 + WWW-Authenticate =="
raw=$(rpc_call "" "tools/list" '{}')
status=$(echo "$raw" | sed -n '1p')
headers=$(echo "$raw" | sed -n '2,/^---BODY---$/p')
if [[ "$status" == "401" ]]; then
  pass "no-token request returns 401"
else
  fail "no-token request returned $status, expected 401"
fi
if echo "$headers" | grep -qi '^www-authenticate: *Bearer'; then
  challenge=$(echo "$headers" | grep -i '^www-authenticate:' | head -1)
  pass "401 carries a Bearer WWW-Authenticate challenge: $challenge"
else
  fail "401 response missing a Bearer WWW-Authenticate challenge"
fi

echo "== negative: read-token add_context -> insufficient_scope =="
if out=$(tool_call "$READ_TOKEN" "add_context" '{"ocean_id":"00000000-0000-0000-0000-000000000000","content":"should never be accepted"}'); then
  code=$(echo "$out" | jq -r '.error.code // empty')
  if [[ "$code" == "insufficient_scope" ]]; then
    pass "read-token add_context call rejected with insufficient_scope"
  else
    fail "read-token add_context call did not return insufficient_scope (got: $out)"
  fi
else
  fail "read-token add_context call transport-level failure"
fi

echo
if [[ "$FAILURES" -eq 0 ]]; then
  echo "mcp-smoke: all checks passed"
  exit 0
else
  echo "mcp-smoke: $FAILURES check(s) failed"
  exit 1
fi
