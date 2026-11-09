# 0003 — API Versioning Strategy

**Status:** Accepted
**Date:** 2026-05-13
**Deciders:** CoralSwarm engineering

---

## Context

As the CoralSwarm REST API grows, we need a clear strategy for introducing breaking changes without disrupting existing clients (CLI, MCP server, third-party integrations).

The two most common approaches are:

- **URL path prefix** — `/v1/`, `/v2/`, ...
- **`Accept:` / `Content-Type:` header versioning** — `application/vnd.coralswarm.v1+json`

---

## Decision

All public API endpoints live under a `/v1/` URL path prefix.  
Breaking changes will be introduced under a new `/v2/` prefix while `/v1/` continues to serve existing clients until formally deprecated.

---

## Rationale

- **CLI and MCP clients are the primary consumers.** Both generate URLs programmatically; a path prefix is trivially inspectable and curl-testable, with no custom header boilerplate.
- **Axum router nesting is a natural fit.** A `nest("/v1", v1_router)` call isolates the entire version tree, middleware, and types. Adding `/v2` is a parallel nest with its own handler set.
- **Simpler caching and proxy configuration.** CDNs and API gateways route/cache on URL structure by default; content-negotiation versioning requires explicit Vary rules that are often misconfigured.
- **Avoid `Accept:` header complexity.** Header versioning requires every client to set a custom header correctly, introduces ambiguity on default responses, and makes browser testing awkward.

---

## Consequences

- **Positive:** Paths are self-documenting and easy to test. No special headers required.  
- **Positive:** Middleware stacks, OpenAPI docs, and rate-limit rules can be scoped per version prefix.  
- **Negative:** Breaking changes require a new `/v2/` prefix and a migration period during which both versions must be maintained.  
- **Negative:** URL length grows by 3 characters. Acceptable for our use case.

---

## Notes

- The Axum router is wired in `backend/src/main.rs` as `Router::nest("/v1", v1)`.  
- OpenAPI documentation is generated under the `/v1/` prefix and served at `/openapi.json` (non-production only).  
- Additive changes (new optional fields, new endpoints) do **not** require a version bump.
