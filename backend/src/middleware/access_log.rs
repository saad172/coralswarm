//! Per-login access-log capture (platform-admin-metrics PR 4 of 7).
//!
//! `require_auth` never sees a discrete "login" event — Clerk hands it a
//! session JWT on every request, indistinguishable from any other request on
//! that same session. The signal this module uses instead is FIRST SIGHT of a
//! previously-unseen Clerk session id (`ClerkClaims::sid`): migration
//! `0095_platform_metrics`'s `UNIQUE (user_id, clerk_session_id)` constraint
//! on `user_access_log` makes an `INSERT ... ON CONFLICT DO NOTHING` of the
//! same `(user_id, sid)` pair idempotent, so calling [`spawn_capture`] on
//! EVERY authenticated request is correct: the first request of a session
//! inserts one row, every later request on the same session is a cheap
//! no-op conflict, and "one row per login" falls out of the constraint
//! rather than needing a read-then-write check here.
//!
//! ## No `sid` → skip capture entirely
//!
//! Clerk **session** JWTs always carry `sid`; Clerk **OAuth access tokens**
//! (the remote-MCP flow, `mcp::auth::mcp_auth` — see that module's
//! `has_oauth_claims`, which uses the very same "does this token carry a
//! `sid`" test to distinguish the two token kinds in the other direction)
//! legitimately never do. `spawn_capture` treats a missing `sid` as "nothing
//! to record" and returns before spawning anything — it does NOT invent a
//! placeholder session id (that would corrupt the "one row per login"
//! invariant the UNIQUE constraint is meant to enforce: every OAuth request
//! would either collide on a shared sentinel or need a fabricated id that
//! isn't actually a login boundary). `require_auth` is the session-JWT path
//! only, so in practice this early-return is defensive/future-proofing
//! rather than a path this crate's own callers are expected to hit today.
//!
//! ## Fire-and-forget discipline (mirrors `mcp::audit::record` /
//! `routes::queries::spawn_query_history`)
//!
//! This function does no I/O itself — header extraction and validation are
//! pure and synchronous — and the actual INSERT runs on a detached
//! `tokio::spawn`ed task. A DB error (pool exhaustion, transient network
//! blip) is logged at `warn` and dropped; it can NEVER fail, slow, or
//! surface into the request this call sits inside of. Callers must not
//! `.await` this function (it isn't `async`) and must not propagate any
//! result from it.
//!
//! ## `ip` — `Fly-Client-IP` only, matched by our own parse
//!
//! Fly's edge proxy sets `Fly-Client-IP` to the real client address; it is
//! not client-settable the way `X-Forwarded-For` is, so (mirroring
//! `middleware::auth::require_auth`'s own documented distrust of
//! `X-Forwarded-Host`) this is the ONLY header consulted for the caller's
//! IP. A missing header, or a value that doesn't parse as an IP address,
//! becomes `None` — never a best-effort guess, and never the raw header
//! value forwarded to any log line or the DB (a value that failed to parse
//! as `IpAddr` is dropped before it is used in an SQL statement or a log
//! line at all, so a malformed header can't reach either).
//!
//! ## `client` — exact allow-list, never a free-form string
//!
//! `X-CoralSwarm-Client` is a client-supplied, unauthenticated header (Unit C
//! of this series, PR #358, adds the emitting side: `web/lib/api.ts` sends
//! `web`. The retired desktop app's `desktop` and its bundled sidecar's
//! `mcp-local` are no longer allow-listed and store `NULL`; `web/lib/shares.ts`'s
//! unauthenticated public share reads deliberately send no header at all —
//! this module only reads whatever shows up). Only an EXACT match against
//! [`ALLOWED_CLIENTS`] is stored; anything else — an absent header, an old
//! build sending nothing, a typo, or a hostile arbitrary string — stores
//! `NULL`. `cli` is deliberately not in the allow-list: the CLI sends no HTTP
//! requests at all, so no `cli` value exists anywhere among the emitting
//! clients. This mirrors `mcp::auth::extract_client_name`'s cap-and-sanitize
//! discipline for a client-supplied identity string, one step stricter: that
//! helper caps length and stores whatever's left, because `mcp_client_name`
//! is enrichment metadata with no allow-list of its own; here the schema
//! only has meaning for three known values, so anything else is simply not
//! one of them.
//!
//! [`CORALSWARM_CLIENT_HEADER`]'s doc comment covers why the constant itself
//! is spelled all-lowercase (`x-coralswarm-client`) despite the wire header
//! being `X-CoralSwarm-Client` — a `HeaderMap::get(&str)` lookup is
//! case-insensitive either way, so this is a defensive-by-construction
//! choice, not a behavior difference.
//!
//! ## `country`/`region`/`city` — always NULL here
//!
//! Geo resolution is intentionally NOT done on this path — see
//! `workers::geo_backfill`'s module doc for why it is a batched sweep
//! instead of an inline lookup.

use std::net::IpAddr;

use axum::http::HeaderMap;
use sqlx::PgPool;
use uuid::Uuid;

/// Set by Fly's edge proxy to the caller's real IP; never client-settable
/// the way `X-Forwarded-For` is. The only header this module trusts for the
/// caller's address — see the module doc.
const FLY_CLIENT_IP_HEADER: &str = "Fly-Client-IP";

/// Self-reported client-surface header (Unit C, PR #358, adds the emitting
/// side: `web/lib/api.ts` sends `web`; `web/lib/shares.ts`'s unauthenticated public share reads deliberately
/// send no header at all). Unauthenticated and client-controlled — validated
/// against [`ALLOWED_CLIENTS`] below, never stored verbatim.
///
/// Spelled all-lowercase deliberately: `HeaderMap::get(&str)` matches
/// header names case-insensitively regardless of this constant's casing
/// (the wire header is `X-CoralSwarm-Client`), but a lowercase `&str`
/// constant here can never be mistaken for — or refactored into —
/// `HeaderName::from_static`, which panics at runtime on any uppercase byte
/// in its input. Panicking on every authenticated request over a header
/// name typo is exactly the footgun this constant is written to make
/// impossible by construction, not just by convention.
const CORALSWARM_CLIENT_HEADER: &str = "x-coralswarm-client";

/// Exact allow-list for `x-coralswarm-client`. `cli` is deliberately absent —
/// the CLI sends no HTTP requests and no `cli` value exists anywhere in the
/// emitting clients (see [`CORALSWARM_CLIENT_HEADER`]'s doc comment) — and
/// any value outside this list (including an absent header) stores `NULL`
/// rather than the client-supplied string.
const ALLOWED_CLIENTS: [&str; 1] = ["web"];

/// Bound on the fire-and-forget `user_access_log` insert `spawn_capture`
/// spawns off the request's critical path — long enough for an ordinary
/// `INSERT ... ON CONFLICT DO NOTHING` under normal load, short enough that a
/// wedged/saturated pool doesn't accumulate an unbounded number of stuck
/// background tasks (`spawn_capture` runs on EVERY first-sight session, so an
/// un-timed-out stall here would leak one task per new login for as long as
/// the DB stays wedged). Mirrors `middleware::auth::resolve_clerk_context`'s
/// `DISPLAY_NAME_REFRESH_TIMEOUT` for its own analogous fire-and-forget
/// spawned write.
const ACCESS_LOG_INSERT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(2);

/// Capture one `user_access_log` row for this request's session, best-effort
/// and fire-and-forget (see the module doc). Call once per authenticated
/// request from `middleware::auth::require_auth`, after the caller's
/// identity has been resolved.
///
/// - `pool` — the CONTROL-plane pool (`AppState::control_db()`); `None`
///   (no DB configured, e.g. local/test) is a silent no-op.
/// - `headers` — the incoming request's headers, read for `Fly-Client-IP`
///   and `X-CoralSwarm-Client` only.
/// - `user_id` — the resolved caller (`UserContext::user_id`).
/// - `session_id` — the verified token's `sid` claim. `None` skips capture
///   entirely (see the module doc's "No `sid`" section) rather than
///   inventing a value.
pub(crate) fn spawn_capture(
    pool: Option<PgPool>,
    headers: &HeaderMap,
    user_id: Uuid,
    session_id: Option<&str>,
) {
    let Some(pool) = pool else {
        return;
    };
    let Some(session_id) = session_id else {
        return;
    };
    let session_id = session_id.to_string();
    let ip = extract_client_ip(headers);
    let client = extract_client(headers);

    tokio::spawn(async move {
        // `$3::inet` casts the text representation server-side rather than
        // requiring the `sqlx` `ipnetwork` feature crate-wide for this one
        // column. `ip` was already parsed as a real `IpAddr` above (or is
        // `None`) — never the raw, unvalidated header value — so this cast
        // can only ever fail on a bug in this function, not on
        // attacker-controlled input.
        //
        // Bounded by `ACCESS_LOG_INSERT_TIMEOUT` (same pattern as
        // `resolve_clerk_context`'s `users.name` refresh): the pool's own
        // `acquire_timeout` only bounds getting a connection, not the query
        // round-trip once one is held, so an unbounded await here would let
        // a stalled Postgres (lock wait, network partition) hang this task
        // forever — and since a new task spawns on every first-sight
        // session, a wedged DB would leak them without limit.
        let res = tokio::time::timeout(
            ACCESS_LOG_INSERT_TIMEOUT,
            sqlx::query(
                "INSERT INTO user_access_log (user_id, clerk_session_id, client, ip) \
                 VALUES ($1, $2, $3, $4::inet) \
                 ON CONFLICT (user_id, clerk_session_id) DO NOTHING",
            )
            .bind(user_id)
            .bind(&session_id)
            .bind(client)
            .bind(ip.map(|ip| ip.to_string()))
            .execute(&pool),
        )
        .await;
        // Never log the resolved IP itself here — only the DB driver's own
        // error text, which for this statement shape (ON CONFLICT DO
        // NOTHING, a pre-validated ::inet cast) cannot echo bind values back.
        match res {
            Ok(Ok(_)) => {}
            Ok(Err(e)) => {
                tracing::warn!(error = %e, "user_access_log insert failed (non-fatal)");
            }
            Err(_) => {
                tracing::warn!(
                    timeout_secs = ACCESS_LOG_INSERT_TIMEOUT.as_secs(),
                    "user_access_log insert timed out (non-fatal)"
                );
            }
        }
    });
}

/// Parse `Fly-Client-IP` as an IP address. `None` when the header is absent,
/// not valid UTF-8, or doesn't parse — never a guess, and the unparsed
/// header text is never itself logged or stored.
///
/// `pub(crate)`, not private: `routes::addin::pair_start` reuses this exact
/// helper to key its own per-IP rate limit (CodeRabbit review on #820) —
/// **`Fly-Client-IP` is set by Fly's edge proxy and cannot be spoofed by the
/// client; `X-Forwarded-For` is client-controllable and must never be used
/// for this.** One helper, one trust decision, rather than a second
/// hand-rolled copy of the same header-trust question in a different file.
pub(crate) fn extract_client_ip(headers: &HeaderMap) -> Option<IpAddr> {
    headers
        .get(FLY_CLIENT_IP_HEADER)
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
        .and_then(|s| s.parse::<IpAddr>().ok())
}

/// Validate `X-CoralSwarm-Client` against the exact [`ALLOWED_CLIENTS`]
/// allow-list. Returns the matched static string (never the caller's own
/// bytes) so nothing client-controlled is ever bound into the query outside
/// this fixed set; anything else (absent, unknown, oversized, hostile)
/// yields `None`, which the caller stores as SQL `NULL`.
fn extract_client(headers: &HeaderMap) -> Option<&'static str> {
    let raw = headers
        .get(CORALSWARM_CLIENT_HEADER)
        .and_then(|v| v.to_str().ok())?;
    ALLOWED_CLIENTS.iter().find(|&&c| c == raw).copied()
}

// ── Unit tests (pure helpers only — the DB-backed insert/idempotency/`sid`
// behavior is proven end-to-end by `tests/access_log_capture_pg.rs`) ────────

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;

    fn headers_with(pairs: &[(&str, &str)]) -> HeaderMap {
        let mut h = HeaderMap::new();
        for (k, v) in pairs {
            h.insert(
                axum::http::HeaderName::from_bytes(k.as_bytes()).unwrap(),
                HeaderValue::from_str(v).unwrap(),
            );
        }
        h
    }

    #[test]
    fn extract_client_ip_parses_a_valid_v4_address() {
        let h = headers_with(&[("Fly-Client-IP", "203.0.113.7")]);
        assert_eq!(
            extract_client_ip(&h),
            Some("203.0.113.7".parse::<IpAddr>().unwrap())
        );
    }

    #[test]
    fn extract_client_ip_parses_a_valid_v6_address() {
        let h = headers_with(&[("Fly-Client-IP", "2001:db8::1")]);
        assert_eq!(
            extract_client_ip(&h),
            Some("2001:db8::1".parse::<IpAddr>().unwrap())
        );
    }

    #[test]
    fn extract_client_ip_is_none_when_header_absent() {
        let h = HeaderMap::new();
        assert_eq!(extract_client_ip(&h), None);
    }

    #[test]
    fn extract_client_ip_is_none_for_garbage() {
        let h = headers_with(&[("Fly-Client-IP", "not-an-ip")]);
        assert_eq!(extract_client_ip(&h), None);
    }

    #[test]
    fn extract_client_ip_never_trusts_x_forwarded_for() {
        // Only Fly-Client-IP is consulted — a spoofed X-Forwarded-For must
        // never be read, mirroring require_auth's distrust of
        // X-Forwarded-Host.
        let h = headers_with(&[("X-Forwarded-For", "1.2.3.4")]);
        assert_eq!(extract_client_ip(&h), None);
    }

    #[test]
    fn extract_client_matches_the_actual_mixed_case_wire_header_despite_the_lowercase_constant() {
        // The real clients (Unit C, PR #358) send `X-CoralSwarm-Client`
        // mixed-case on the wire; CORALSWARM_CLIENT_HEADER is deliberately
        // stored lowercase (`x-coralswarm-client`, see its doc comment).
        // `HeaderMap::get(&str)` matches case-insensitively, so this must
        // still resolve correctly — pinning that contract, not just relying
        // on it silently.
        let h = headers_with(&[("X-CoralSwarm-Client", "web")]);
        assert_eq!(extract_client(&h), Some("web"));
    }

    #[test]
    fn extract_client_accepts_every_allow_listed_value() {
        for allowed in ALLOWED_CLIENTS {
            let h = headers_with(&[("X-CoralSwarm-Client", allowed)]);
            assert_eq!(extract_client(&h), Some(allowed));
        }
    }

    #[test]
    fn extract_client_rejects_the_retired_desktop_clients() {
        for retired in ["desktop", "mcp-local"] {
            let h = headers_with(&[("X-CoralSwarm-Client", retired)]);
            assert_eq!(extract_client(&h), None, "{retired}");
        }
    }

    #[test]
    fn extract_client_rejects_cli_deliberately() {
        // `cli` is deliberately NOT in the allow-list (see module doc).
        let h = headers_with(&[("X-CoralSwarm-Client", "cli")]);
        assert_eq!(extract_client(&h), None);
    }

    #[test]
    fn extract_client_rejects_arbitrary_strings() {
        let h = headers_with(&[("X-CoralSwarm-Client", "definitely-not-real")]);
        assert_eq!(extract_client(&h), None);
    }

    #[test]
    fn extract_client_is_none_when_header_absent() {
        let h = HeaderMap::new();
        assert_eq!(extract_client(&h), None);
    }

    #[test]
    fn extract_client_never_stores_the_raw_client_supplied_string() {
        // Defensive: the return type itself is `Option<&'static str>`, so it
        // is impossible for this function to hand back a caller-controlled
        // allocation even by accident. This test just pins that contract.
        let hostile = "x".repeat(10_000);
        let h = headers_with(&[("X-CoralSwarm-Client", &hostile)]);
        assert_eq!(extract_client(&h), None);
    }
}
