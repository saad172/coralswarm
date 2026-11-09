# MCP publication follow-up — 2026-10-10

This change fixes the tool annotation and configurable UI origin gaps. It does
not certify production OAuth or authorize a marketplace submission.

## Source changes

All 20 tools now explicitly declare `readOnlyHint`, `destructiveHint`, and
`openWorldHint`. `ask_ocean` is annotated `readOnlyHint: false`: it creates a
stateful task and persists query history (`src/mcp/tools/read.rs`, the
`mcp_tasks` insert and task-tracker spawn; `src/services/ask.rs`, the detached
query-history write). It remains non-destructive and available under the
existing content-read scope. The remaining retrieval tools and `list_tenants`
explicitly declare `destructiveHint: false`.

[`src/mcp/server.rs`](src/mcp/server.rs) accepts optional `MCP_WIDGET_DOMAIN`.
Set it to a company-controlled HTTPS origin reserved for this plugin. The
server rejects credentials, paths, query strings, fragments, and non-HTTPS
schemes; a trailing slash is normalized. Valid values populate both
`_meta.ui.domain` and `_meta["openai/widgetDomain"]` on every current and legacy
resource. Unset or invalid values preserve the default sandbox behavior and
do **not** meet the public UI submission requirement.

OpenAI requires a dedicated origin unique per plugin; its reference does not
require creating a new DNS hostname. The existing `https://app.coralswarm.example`
is a candidate only after the company confirms it is the intended plugin
origin. No production domain configuration was read or changed here. Resource
content continues to come from MCP `resources/read`; CSP is unchanged.

Sources checked on 2026-10-10:
[tool annotation requirements](https://developers.openai.com/plugins/plugin-guidelines#correct-annotation),
[component resource metadata](https://developers.openai.com/plugins/reference#component-resource-_meta-fields).

## Remaining OAuth publication blocker

[`src/mcp/auth.rs`](src/mcp/auth.rs)'s production `ClerkTokenValidator` calls
the shared [`src/clerk.rs`](src/clerk.rs) verifier, whose `decode_claims`
disables audience validation. `ClerkClaims` does not retain `aud`, and no
subsequent MCP check binds the access token to the protected resource.
[`../docs/plans/remote-mcp-v1.md`](../docs/plans/remote-mcp-v1.md)'s
“Contradictions resolved” table explicitly accepts absent OAuth audiences in
v1. That earlier acceptance does not satisfy OpenAI's current requirement to
verify the expected resource audience on every invocation.

Changing the shared validation flag would affect ordinary Clerk session
tokens, which do not have the OAuth resource audience. A safe fix needs:

1. A fresh company-owned staging OAuth authorization-code/PKCE flow that sends
   `resource` on both authorization and token requests. Inspect only the
   resulting access-token claim names and audience values locally; do not
   record bearer tokens. Test refreshed tokens too. Discovery metadata listing
   an `aud` claim is insufficient because it can describe OIDC ID tokens.
2. Confirm issuer support and application configuration mint an access-token
   audience equal to the exact advertised resource, currently
   `https://api.coralswarm.example/mcp`. If this cannot be configured, use a resource-
   binding-capable authorization server before declaring OpenAI readiness.
3. Add a separate OAuth-only verifier using the existing cached signing keys,
   issuer, expiry, and not-before checks, with required audience matching.
   Route only `ClerkTokenValidator` through it; leave the web session verifier
   intact. Never substitute `client_id` or `azp` for a resource audience.
4. Test correct audience, wrong audience, missing audience, multi-audience
   tokens, expired tokens, refreshed tokens, and unchanged session login.
   Run authenticated MCP discovery and real-tool checks under the company test
   user before submission.

No authenticated OAuth flow or issuer contract was available in this scoped
change, so no audience enforcement was added and this blocker remains open.
Clerk's current implementation documentation explains JWT access tokens but
does not establish CoralSwarm's actual resource-audience contract.

Sources:
[OpenAI token verification](https://developers.openai.com/plugins/build/auth#implementing-token-verification),
[Clerk OAuth implementation](https://clerk.com/docs/guides/configure/auth-strategies/oauth/how-clerk-implements-oauth).

## Validation

The targeted `mcp::server::tests` suite covers the serialized catalog's explicit
annotations and widget origin validation/metadata aliases. Run with:

```sh
cargo test -p coralswarm-backend --lib mcp::server::tests --offline
```

Production deployment, final widget-domain selection/configuration,
authenticated OAuth verification, and marketplace submission are separate
remaining steps.
