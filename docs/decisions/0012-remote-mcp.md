# ADR-0012: Remote MCP Server — Clerk-as-AS, In-Process `/mcp`, Read/Write Scopes

**Date:** 2026-07-15
**Status:** Accepted — **Implemented** (Phases 1-4: auth spine, 10 read tools, 4 write tools, CI +
e2e; Phase 5 staging certification and Phase 6 prod enable tracked separately — see
`docs/mcp-remote-cert-checklist.md` / `docs/mcp-remote-ops.md`)
**Deciders:** Alex
**Builds on:** ADR-0004 (local-first / server-trust), ADR-0006 (ocean model), ADR-0007 (web v1
tenancy/RAG/inference)

---

## Context

CoralSwarm already ships a local stdio MCP sidecar (`mcp-server/`, hand-rolled JSON-RPC protocol
over HTTP self-calls with session tokens) for desktop-local tool access. Remote clients — claude.ai
custom connectors, Claude Desktop, ChatGPT developer mode, Cursor, VS Code, MCP Inspector — need a
hosted endpoint they can connect to directly over OAuth, without running anything locally or
managing a CoralSwarm API key by hand.

The full design space (auth topology, tool catalog, security model, rollout phasing) is worked out
in `docs/plans/remote-mcp-v1.md`; this ADR records the decisions that are locked for v1, the risks
knowingly accepted to ship it, and how the new remote server relates to the pre-existing stdio
sidecar.

## Decision

### 1. Remote-first; the stdio sidecar stays untouched

`mcp-server/` is not modified, retired, or unified with the remote server as part of this work.
Both exist side by side: the sidecar for local/desktop tool access, the remote server (this ADR)
for hosted connector access. Evaluating whether to thin or retire the sidecar in favor of a shared
tool catalog is an explicit, separate follow-up (§ Follow-ups below) — not relitigated here.

### 2. Clerk is the OAuth Authorization Server; the backend is the Resource Server

CoralSwarm's existing Clerk integration becomes an OAuth 2.1 authorization server for MCP: it
handles Dynamic Client Registration, PKCE (`S256`), user consent, and access-token issuance. The
CoralSwarm backend is purely a Resource Server — it advertises Clerk via RFC 9728 protected-resource
metadata (`GET /.well-known/oauth-protected-resource[/mcp]`) and verifies Clerk-issued access
tokens against the same JWKS-cached verification path (`clerk::verify`) already used for session
JWTs, extended with OAuth-specific claims (`scope`, `client_id`/`azp`, `jti`).

No new identity system, no separate token issuer, no CoralSwarm-minted API keys for MCP.

### 3. `/mcp` is mounted in-process in the existing `backend/` Axum app

Not a new service, not a new workspace crate. `backend/src/mcp/` is a crate-private module mounted
by `app()` only when `McpConfig::from_env()` resolves — `MCP_ENABLED` truthy **and** `CLERK_ISSUER`
set (flag on with no issuer configured logs an error and refuses to mount, rather than mounting a
resource server with no authorization server to verify against). Flag off (the common case for
`desktop_app`/`internal_app` builds and any environment MCP isn't enabled in): `/mcp` and every
`/.well-known/oauth-*` route return a plain `404`, exactly as if the code didn't exist.

Tools call the store/services layer directly — no HTTP self-call hop the way the stdio sidecar
works.

### 4. Protocol layer: `rmcp`, streamable-HTTP, stateless mode, hand-written dispatch

The official `rmcp` SDK (`server`, `macros`, `schemars`, `transport-streamable-http-server`
features) provides the streamable-HTTP transport and the `#[tool_router]`/`#[tool]` macros used to
declare each tool's schema. `list_tools`/`call_tool` are **hand-written**, not the macro's default
`#[tool_handler]` expansion, because the security model requires wrapping every dispatch with (in
order) a scope filter/gate, per-tool rate-limit buckets, and an audit hook that fires for every
outcome — none of which the generated handler provides for free.

Session mode is **stateless** (`stateful_mode: false`, `NeverSessionManager`) — safe under Fly's
multi-replica deployment with no external session store, appropriate for a tools-only server, and
the most broadly compatible choice across claude.ai/ChatGPT/IDE clients tested. Revisit only if
subscriptions/elicitation land and force a stateful requirement.

### 5. 14 tools: 10 read + 4 write, in ocean/reef/coral vocabulary

Read (always visible to any authenticated OAuth token): `list_oceans`, `ask_ocean`, `list_reefs`,
`get_reef`, `list_meetings`, `get_meeting`, `get_atom`, `search_atoms`, `get_transcript` (chunked),
`recent_activity`.

Write (visible only with write scope): `add_context`, `confirm_reef`, `reject_reef`, `update_reef`.
Graph-surgery tools (merge/promote/supersede reefs) are explicitly deferred past v1.

Every tool re-derives authorization from the verified token's `UserContext` — never from
caller-supplied ids — either by delegating to an existing route's extracted `_impl` function or by
opening with the exact same reachability/ownership guard the corresponding HTTP route uses. IDOR
misses always surface as `not_found`, matching the HTTP routes' existing non-leak convention (never
a distinct "forbidden" that would confirm a resource exists under another tenant).

### 6. Two scopes: `coralswarm:read` / `coralswarm:write`, write gated two ways at once

`coralswarm:read` is granted to any authenticated OAuth token. `coralswarm:write` requires **both**:

1. An environment-wide ceiling (`MCP_DEFAULT_SCOPES` includes `coralswarm:write`), and
2. A per-user opt-in (`user_prefs.mcp_write_enabled`, migration `0060`, default **off**), checked
   fresh from the database on every request — so revoking a user's write access takes effect on
   their very next call, no token re-mint required.

This exists because Clerk cannot mint custom `coralswarm:*` scopes today (no fixed-scope consent
screen can express "read my knowledge graph" vs. "write to it" as separate user choices yet), and
because the threat model treats every MCP caller as a potentially prompt-injected, adversarial
agent — write access needs a lever a hostile prompt cannot flip on its own. Enforcement is
independent at two points (`list_tools` filters write tools out of the catalog without write scope;
`call_tool` rejects a call to one by name even if a client calls it unlisted), since a hostile or
caching client cannot be trusted to respect the filtered list alone.

### Addendum (2026-09-14): the per-user default flipped from off to on

**§6 above is preserved as written — this is a dated addendum, not a rewrite.** The original
decision named `user_prefs.mcp_write_enabled`'s default-**off** as "a lever a hostile prompt cannot
flip on its own" — the primary defence against a prompt-injected write. MCP listing-prep (#900)
reversed that specific default: a personal account with NO recorded preference (`user_prefs` row
absent, and its 0080 successor `user_account_prefs` row absent too) now defaults to
`coralswarm:write` **enabled**, not denied. Org accounts are unaffected — they were already
default-allow-on-join (`clerk_provision::ensure_org_write_grant`, a separate control predating this
addendum) with the grant admin-revocable, never a bare env/DB default.

Why the reversal doesn't reopen the threat §6 names: the write scope was never the ONLY thing
standing between a hostile prompt and a write — `readOnlyHint`/`destructiveHint` per-tool
annotations (MCP listing-prep item 2 of the SAME PR: every tool, including all five write tools,
now also carries `openWorldHint: false`) mean a client that respects those hints (most do) still
prompts the human for approval before running `add_context`/`confirm_reef`/`reject_reef`/
`update_reef`/`promote`, by name, with its arguments shown — the SAME per-call confirmation this
ADR's own §6 already relied on as the belt to the scope's suspenders ("What write-approval prompts
look like", `docs/mcp-remote.md`). Requiring a settings visit BEFORE that confirmation dialog could
ever appear at all was judged, in practice, to cost more (every new personal user's `add_context`
silently doing nothing, with no visible error explaining why, until they found and flipped a
setting) than it defended against, given that second layer was never removed.

**The replacement control is a real, user-accessible opt-out** — something #6's `user_prefs`
default-off had a writer for (the legacy settings toggle) but the flag it wrote never actually
shipped a reachable UI; MCP listing-prep #900 both flips the default AND ships the API half of the
FIRST real opt-out surface: `GET`/`PATCH /v1/me/prefs` (`routes::me::get_me_prefs`/
`patch_me_prefs`), backed by the SAME `user_account_prefs` row §6's per-account successor
(migration `0080`) already made authoritative. The reachable UI itself ships alongside it in #903
(`web/app/(app)/settings/access/page.tsx`'s "AI assistants (MCP)" section) — the two were built in
parallel against the same `/v1/me/prefs` contract. An account that explicitly opts out via that
endpoint stays opted out — the default flip only changes what an ABSENT row means, never overrides
a present one (`mcp::auth::mcp_write_enabled_for_account`'s precedence, unchanged: an explicit row
always wins). On a personal (or personal-org, see H1 below) account, `PATCH` is always the caller's
own call in either direction. On a REAL org account, `PATCH` is admin-only in BOTH directions
(second security review, M2): a plain member self-denying their own row would look, to the DB, just
like an admin's deny, breaking `clerk_provision`'s documented invariant that a surviving `false` row
on an org account is always an admin decision (`ensure_org_write_grant`'s `ON CONFLICT DO NOTHING`
and the offboarding DELETE's `AND mcp_write_enabled` scoping both assume that). A non-admin member
on an org account asks a workspace admin to change it, same as before this addendum for a grant, now
also for a deny.

**H1 (second security review):** the org-vs-personal question above is answered by
`Store::account_is_real_org`, NOT a raw `clerk_org_id IS NOT NULL` check — `adopt_personal_org_account`
relabels a user's own personal account to carry a real `clerk_org_id` in place, so the raw check
would treat an adopted personal-org account as a real org and default-deny (and admin-gate) exactly
the users who adopted. `mcp::auth::account_is_real_org_or_fail_closed` is the fail-closed wrapper
used at every call site that needs this answer.

### 7. Rollout: env flag → staging cert first → prod flip

`MCP_ENABLED` gates the whole feature. Staging (`coralswarm-backend-staging`, deploys from
`develop`) is certified against all six v1 clients and soaked ≥1 week before the prod flag ever
ships (`docs/mcp-remote-cert-checklist.md`, `docs/mcp-remote-ops.md`). Per-user/per-account rate
limits reuse and extend the existing `RateLimiter` trait/Postgres-backed implementation; every tool
call is logged via a structured `mcp.tool_call` tracing line and a best-effort `mcp_tool_calls`
audit row (migration `0061`), regardless of outcome.

## Accepted risks

### No `aud` (audience) validation in v1

Clerk OAuth access tokens carry no `aud` claim today — RFC 8707 resource-indicator support isn't
available from Clerk yet. This means `/mcp` accepts any valid OAuth access token issued by the
configured Clerk instance, regardless of which client application requested it; there is no
cryptographic binding between "this token was minted for the MCP resource" and "this token is being
presented to the MCP resource."

**Mitigations:**

- **OAuth-only claims requirement.** `/mcp` rejects any token that doesn't carry OAuth-shaped
  claims (`scope`/`client_id`/`azp`/`jti`) — and specifically, any token carrying `sid` (Clerk's
  session-JWT marker) is rejected outright regardless of what else it carries. This closes off the
  most likely confusion (a web-app session token accidentally working at `/mcp`) even without `aud`.
- **Dedicated Clerk OAuth application.** Operationally, MCP is required to register/authorize as
  its own OAuth application within the shared Clerk instance (not reuse the web app's client) —
  see `docs/mcp-remote-ops.md`'s pre-prod gate. This doesn't provide cryptographic audience binding,
  but keeps the blast radius and DCR client population legible.
- **Single-tenant blast radius.** Every tool re-derives authorization from the verified token's
  `sub` → `UserContext` mapping on every call — a token from the same Clerk instance can never act
  outside the tenancy of the user it actually belongs to, `aud` or not.
- **Structured for a fast follow-on.** The token validator is a small injectable trait
  (`auth::TokenValidator`) specifically so audience checking can be added as one more assertion the
  moment Clerk ships RFC 8707 support, without restructuring the auth spine. A wrong-audience test
  exists in the test suite today, deliberately `#[ignore]`d until then.

**Revisit before the prod flip and periodically after:** check whether Clerk has shipped
resource-indicator support; if so, enable `aud` validation and un-ignore the test.

### Fail-open rate limiter

The Postgres-backed rate limiter (`PgRateLimiter`) fails open on a database error — a query failure
lets the call through unlimited rather than rejecting it. This is an existing, pre-MCP property of
`PgRateLimiter`, not something introduced here, but MCP traffic is a new, agent-driven caller onto
the same limiter, and `MCP_ENABLED` now hard-requires `RATE_LIMIT_BACKEND=pg` (the in-memory
alternative is per-VM and would silently multiply limits across Fly replicas). **Accepted for v1**:
a sustained run of `PgRateLimiter query failed` warnings in the logs is the signal to treat as a
page (see `docs/mcp-remote-ops.md`'s observability section), not a silent characteristic to ignore.

### Permissive app-wide CORS behind `origin_guard`

The backend's app-wide CORS layer is `allow_origin(Any)` (`backend/src/lib.rs`), which on its face
looks unsafe for a bearer-token-protected endpoint. Two things make this acceptable at `/mcp`
specifically:

1. **Bearer-only authentication, no cookies.** `/mcp` never reads a session cookie — the only
   credential it accepts is an `Authorization: Bearer` header a browser cannot be tricked into
   attaching to a cross-origin request the way it can with cookies. This makes classic CSRF
   structurally inert here regardless of the CORS policy.
2. **`origin_guard` is defense-in-depth**, layered specifically on the `/mcp` sub-router
   (`mcp::auth::origin_guard`, precedent: `desktop_origin_guard`): a request whose `Origin` header
   is present but not in `MCP_ALLOWED_ORIGINS` is rejected outright. A request with **no** `Origin`
   header at all — every native/non-browser MCP client (Claude Desktop, Cursor, VS Code extensions,
   server-to-server callers) — is always allowed. Allowing the no-`Origin` path is safe **because
   of the bearer-only invariant in point 1**, not because every browser-origin attack necessarily
   sends an `Origin` header — the guard filters the browser-origin cases it can see, but it is not
   the authorization boundary; bearer verification is.

The app-wide `Any` CORS headers are therefore belt-and-suspenders-harmless at `/mcp`: they control
whether a browser-based caller's *response* is readable cross-origin, not whether the request is
authorized to run at all, and `origin_guard` runs ahead of the rmcp service on every request.

**Invariant to preserve:** `/mcp` must stay strictly bearer-only. If a future change ever lets
`/mcp` accept an ambient credential (a session cookie, a query-string token), the always-allowed
no-`Origin` path above silently becomes an authorization bypass — such a change must revisit this
decision, not just ship past it.

## The pre-existing stdio sidecar (`mcp-server/`)

`mcp-server/` is a separate Rust binary implementing MCP over stdio for local/desktop use: its own
hand-rolled JSON-RPC protocol layer (`mcp-server/src/{protocol,mcp,transport}.rs`) and its own tool
implementations, which call the CoralSwarm HTTP API via self-calls carrying a session token rather
than talking to the store directly. It is unrelated to and unmodified by this work — a different
transport, a different (smaller, HTTP-self-call-based) tool catalog, and a different auth model
(session token, not OAuth).

**Known bug, filed as a follow-up, not fixed in this workstream:** the sidecar's `get_node` tool
(`mcp-server/src/client.rs:124-126`) calls `GET /v1/nodes/{node_id}`, but `v1_routes()`
(`backend/src/lib.rs:266`) only registers `PATCH` on `/nodes/{id}` — there is no GET-single-node
route, so the sidecar's `get_node` tool always fails (404/405). Two fix options once picked up:
add a real `GET /v1/nodes/{id}` route (trivially backed by the new `Store::get_node` this workstream
introduced for the remote server's `get_reef`/`update_reef`/`confirm_reef` tools), or repoint the
sidecar at the existing `/v1/nodes/{id}/atoms` + `/v1/nodes/{id}/digest` routes instead.

**Sidecar unification** (thinning or retiring `mcp-server/` in favor of either an `rmcp` stdio
transport reusing `backend/src/mcp/tools/`'s handlers, or local-dev use of the remote endpoint
directly) is a candidate future cleanup once the remote server is certified and stable — explicitly
not part of this ADR's scope.

## Consequences

**Positive:** one hosted connector works across every OAuth-capable MCP client without any local
process; tool implementations call the store directly rather than round-tripping through HTTP
self-calls; the write-scope design gives users a real per-account kill lever Clerk's own
fixed-scope consent screen can't yet express; audit logging (tracing + `mcp_tool_calls`) gives
operators a concrete signal for denied/rate-limited/erroring calls from day one.

**Negative / risks:** no `aud` validation until Clerk ships resource indicators (mitigated, tracked
above); the rate limiter's fail-open behavior is now exposed to a new, potentially high-volume
agent-driven caller; two MCP tool catalogs (`mcp-server/` and `backend/src/mcp/tools/`) now exist
side by side with no shared implementation, which is accepted duplication pending the sidecar
unification follow-up; Clerk's inability to mint custom scopes means `coralswarm:*` are internal,
env/DB-derived effective scopes today rather than something visible on Clerk's own consent screen.

## Follow-ups

- Fix or repoint the stdio sidecar's `get_node` 404 (see above).
- Evaluate sidecar unification/retirement once the remote server is certified (§ above).
- Enable `aud` validation and un-ignore the wrong-audience test once Clerk ships RFC 8707 support.
- Flip scope derivation to the token's own `scope` claim once Clerk supports minting
  `coralswarm:read`/`coralswarm:write` as real, consent-screen-visible custom scopes.
- Per-account daily LLM token budget for `ask_ocean` (pre-existing `TODO(S4)`,
  `backend/src/rate_limit.rs`) — urgency rises as MCP-driven `ask_ocean` traffic grows.
- ChatGPT `search`/`fetch` alias pair — only needed if/when CoralSwarm publishes beyond ChatGPT
  developer mode into ChatGPT's connectors/Apps directory.
- CIMD (Client ID Metadata Documents) support — trigger: Clerk DCR client-registration bloat
  observed per `docs/mcp-remote-ops.md`'s observability signals.

## References

- `docs/plans/remote-mcp-v1.md` — full design, tool catalog, security model, phased rollout.
- `docs/mcp-remote.md` — user-facing connect guide.
- `docs/mcp-remote-ops.md` — operator runbook (enable/disable, observability, smoke test).
- `docs/mcp-remote-cert-checklist.md` — six-client staging certification checklist.
- ADR-0004 (local-first / server-trust); ADR-0006 (ocean model); ADR-0007 (web v1 tenancy/RAG/
  inference, D7 personal-ocean-only ingest reused by `add_context`).
- Implementation: `backend/src/mcp/{mod,auth,well_known,server,audit}.rs`,
  `backend/src/mcp/tools/{mod,read,write}.rs`; migrations `0060_mcp_user_prefs`,
  `0061_mcp_tool_calls`.

## Amendment (2026-10, settings launch plan decision 6): MCP write is always on

The per-user / per-account write opt-in described above (`user_prefs.mcp_write_enabled`,
`user_account_prefs.mcp_write_enabled`, `GET`/`PATCH /v1/me/prefs`, the org-join default grant and
its offboarding cleanup) was removed. The write gate is now the server write ceiling
(`MCP_DEFAULT_SCOPES`) plus the token's `coralswarm:write` scope when it carries product scopes;
where a write can land (the caller's own writable private ocean; shared org oceans only via
promote) is unchanged. Old explicit `false` rows are no longer read and block nothing. The
tables/columns remain until a separate later migration drops them. The text above is kept as
history.
