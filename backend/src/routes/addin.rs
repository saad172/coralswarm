//! Gmail Add-on pairing + the add-on personal access token (PAT) it mints.
//!
//! # Why this exists
//!
//! The Outlook add-in (`extensions/outlook-addin`) hosts clerk-js directly in
//! its task-pane iframe and mints a fresh ~60s Clerk session JWT immediately
//! before every call — see that add-in's README. **That pattern does not
//! transfer to Gmail.** A Gmail Add-on is Google Apps Script + CardService: a
//! server-side function returns a declarative `Card` on a contextual trigger.
//! There is no persistent DOM, no iframe, and no place to keep a live Clerk
//! session alive. Apps Script's only durable per-user state is
//! `PropertiesService.getUserProperties()` — a plain key-value store — so the
//! add-on needs its own long-lived, revocable bearer credential instead of a
//! session.
//!
//! # The pairing flow (no copy-pasting a secret)
//!
//! Getting that credential into Apps Script without asking a human to copy a
//! bearer token out of dev tools is the actual new mechanism here — three
//! routes, two machines, one human clicking "Approve":
//!
//! 1. **`POST /v1/addin/pair/start`** (unauthenticated — the add-on has no
//!    CoralSwarm identity yet). Mints a short `code` (human-glanceable, shown
//!    on the Gmail card) and a separate, long, high-entropy `poll_secret`
//!    (returned to the caller, never persisted in the clear — see
//!    [`hash_token`]). Only the requesting Apps Script instance ever sees
//!    `poll_secret`.
//! 2. The add-on opens `https://app.coralswarm.example/addin/approve?code=<code>`
//!    in the user's browser. **`POST /v1/addin/pair/approve`** (authenticated
//!    — a normal Clerk bearer) binds the pairing attempt to the CALLER'S OWN
//!    session (`UserContext`/`Tenant` — never client-supplied ids) and mints
//!    the real PAT, writing its plaintext into the pairing row exactly once.
//! 3. **`POST /v1/addin/pair/poll`** (unauthenticated, but requires the
//!    `poll_secret` from step 1) retrieves that plaintext exactly once and
//!    deletes the pairing row in the same transaction — see [`pair_poll`]'s
//!    own doc comment for the single-retrieval guarantee.
//!
//! **Why splitting `code` and `poll_secret` matters**: `code` is short
//! because a human looks at it (an unremarkable part of a UI, not a security
//! boundary). `poll_secret` is the actual authorization to retrieve the
//! result. A leaked `code` alone (a shared screen, a support screenshot)
//! cannot be used to steal a paired session — see migration
//! `0211_addin_pairing_codes`'s header for the full threat-model writeup, and
//! [`pair_poll`]'s doc comment for how a wrong `code`, a wrong `poll_secret`,
//! and a genuinely expired/consumed row are made indistinguishable on the
//! wire.
//!
//! # The PAT itself
//!
//! Minted in [`pair_approve`] as `csat_` + 32 CSPRNG bytes, base64url-encoded
//! — greppable-prefix convention, never logged in full (see [`generate_pat`]).
//! Stored ONLY as a SHA-256 hash in `addin_tokens` (migration
//! `0210_addin_tokens`). `middleware::auth::require_auth` recognizes a
//! `csat_`-prefixed bearer as a second token SHAPE alongside a Clerk JWT (see
//! [`resolve_addin_token`], and that module's own doc comment for how the two
//! paths share the rest of the auth pipeline) — never a parallel auth system.
//! Personal-plane only, by construction: `addin_tokens.owner_user_id` is
//! `NOT NULL` (never `AccountWide`), and [`pair_approve`] refuses (400) a
//! caller in an org `Tenant` context outright.
//!
//! `DELETE /v1/addin/tokens/{id}` ([`revoke_token`]) lets the owning user
//! revoke their own token; `require_auth`'s lookup filters
//! `revoked_at IS NULL`. `GET /v1/addin/tokens` ([`list_tokens`]) is the
//! read side of the same pair — the caller's own live tokens, so the
//! personal `/connectors` page can render a Revoke button per row without
//! the human having to go back into Gmail to disconnect.
//!
//! # The approve race, and why the add-on names the account it connected as
//!
//! `pair/approve` is keyed on `code` ALONE. That is a deliberate trade-off
//! made when this flow shipped (#820): the add-on has no CoralSwarm
//! identity to bind the approval to, and a human has to be able to open the
//! approval link in whatever browser they are already signed into. The
//! consequence is real and was accepted knowingly — an attacker who GUESSES
//! a currently-live 8-character `code` before its owner approves it can
//! approve it themselves, binding that pairing to the ATTACKER's account.
//! Note the direction: the attacker never learns the victim's PAT (the
//! plaintext only ever goes back to whoever holds `poll_secret`, i.e. the
//! real add-on). What they can do is the inverse — make the victim's Gmail
//! add-on save threads into the ATTACKER's ocean. [`pair_approve`]'s
//! per-`code` rate limit slows that race down but, as its own doc comment
//! says, does not close it.
//!
//! The race is NOT closed here. What IS done is make its outcome visible
//! rather than silent: [`pair_poll`]'s `approved` response carries
//! `account_email` (the `users.email` of whoever actually approved), and
//! the add-on renders it as "Connected as <email>" on its connected card
//! (`extensions/gmail-addin/Card.gs`). A pairing hijacked by a stranger
//! therefore announces itself, in the add-on's own UI, as connected to an
//! address the user does not recognize, on the very first card they see
//! after connecting — a mis-bound pairing a human can notice and undo
//! (Disconnect, then reconnect), rather than a silent mis-delivery of their
//! mail. It is a detection control, not a prevention one; the prevention
//! fix (binding the pairing attempt to the approving session before the
//! code is ever shown) is a larger change to the handshake and is not what
//! this is.

//! # Microsoft add-in pairing shares this table (migration 0264)
//!
//! The Teams meeting pane and the Outlook panes reuse `addin_pairing_codes`,
//! `pair/approve` and `pair/poll` for a SECOND pairing flavour, `kind =
//! 'microsoft'`, created by `POST /v1/addin/microsoft/session` instead of
//! `pair/start` (see [`super::addin_microsoft`] and
//! `docs/connectors/microsoft-addin-sso.md`). Differences from the Gmail
//! flavour above:
//!
//! * the pairing row is BOUND, at creation, to the Microsoft identity
//!   `(tid, oid)` the SSO token proved — `pair/approve` writes THAT identity
//!   into the link and never reads one from the request. This closes the
//!   "which identity does this pairing carry" half of the approve race for this
//!   flavour. It does NOT stop a code-guesser approving a victim's live code into
//!   their OWN workspace; that residual is detection-only, exactly as above (the
//!   next `connected` response names the account email).
//! * `pair/approve` takes `account_id` (the workspace the user picked — personal
//!   or any org they are a live member of, verified server-side), and mints NO
//!   token: approval creates the LINK. A Gmail pairing refuses `account_id`, and
//!   a Microsoft pairing refuses to be approved without one — neither can be
//!   approved the other's way.
//! * `pair/poll` reports `pending` / `approved` (no token) / `expired`.

use std::sync::Arc;

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Extension, Json,
};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use chrono::{DateTime, Utc};
use rand::Rng;
use serde::Deserialize;
use serde_json::json;
use sqlx::PgPool;
use uuid::Uuid;

use crate::{
    middleware::auth::UserContext,
    routes::pat::{generate_pat as generate_pat_with_prefix, hash_token},
    tenancy::extractor::Tenant,
    AppState,
};

// ── Constants ─────────────────────────────────────────────────────────────────

/// Prefix on every add-on PAT — greppable in logs (a value starting with this
/// is never the plaintext of a Clerk JWT, so a log scrubber or an incident
/// grep can find one without a regex over base64) and the shape
/// `middleware::auth::require_auth` recognizes to route to this module
/// instead of Clerk JWT verification.
pub(crate) const ADDIN_TOKEN_PREFIX: &str = "csat_";

/// How long a pairing attempt stays alive before `pair/approve`/`pair/poll`
/// both start treating it as expired.
pub(super) const PAIRING_TTL_MINUTES: i64 = 10;

/// Alphabet for the human-glanceable `code` — uppercase + digits, minus
/// `0/O/1/I` (easy to mis-key/mis-read, and this value is never the security
/// boundary anyway — see this module's header).
const PAIRING_CODE_ALPHABET: &[u8] = b"ABCDEFGHJKLMNPQRSTUVWXYZ23456789";
const PAIRING_CODE_LEN: usize = 8;
/// Retries on the (astronomically unlikely) case `pair/start` generates a
/// code that collides with another still-live row. The primary key catches
/// it (`INSERT` fails with `23505`); this just means a collision is a quiet
/// retry, never a silently overwritten pairing attempt.
pub(super) const PAIRING_CODE_MAX_ATTEMPTS: usize = 5;

/// `pair/poll` AND `pair/approve` rate limit — both keyed on the `code`
/// itself, under DISTINCT prefixes (`"addin_poll:"` / `"addin_approve:"`, see
/// each call site) so the two endpoints never share one bucket.
///
/// `pair/poll` is unauthenticated, so there is no caller identity to key on
/// at all; `code` is the one enumerable value there, and this is a
/// brute-force target for it even though `poll_secret` is the real gate (see
/// this module's header). `pair/approve` IS authenticated, but that alone
/// doesn't bound the attempt rate — an authenticated attacker can still
/// script requests against many *different* `code` guesses trying to find
/// someone else's currently-live pairing attempt before its owner approves
/// it (a race, not a secret leak — see [`pair_approve`]'s own doc comment on
/// what that race can and cannot do). Per-code keying doesn't cap the total
/// guess rate across DIFFERENT codes either (each new guess gets a fresh
/// bucket) — it caps repeated hammering of the SAME code, symmetrically with
/// `pair/poll`'s identical limitation. An unthrottled, credential-adjacent
/// endpoint with zero rate limiting at all was the gap; this closes that gap
/// without pretending per-code keying alone solves keyspace enumeration.
pub(super) const PAIR_CODE_RATE_MAX_PER_WINDOW: u32 = 20;
pub(super) const PAIR_CODE_RATE_WINDOW_SECS: u64 = 60;

/// `pair/start` rate limit: keyed on the caller's IP (via
/// [`crate::middleware::access_log::extract_client_ip`] — `Fly-Client-IP`,
/// trusted; never `X-Forwarded-For`), NOT `code` — there is no `code` yet at
/// this point, this route MINTS one. Without this, an unauthenticated caller
/// could script unlimited calls, each writing a fresh `addin_pairing_codes`
/// row (and, before this fix, running the opportunistic sweep's `DELETE`
/// every time too) — sustained control-plane write load and row bloat, not
/// just a one-time cost, since swept rows live up to ~1 day past their
/// 10-minute TTL (CodeRabbit review on #820).
const PAIR_START_RATE_MAX_PER_WINDOW: u32 = 10;
const PAIR_START_RATE_WINDOW_SECS: u64 = 60;

// ── Rate-limiter backend, verified (CodeRabbit review on #820) ─────────────
//
// All three limits above go through the ambient `state.rate_limiter`, same
// as every other route in this codebase — no addin-specific backend choice.
// CodeRabbit flagged that `InMemoryRateLimiter` (the default when
// `RATE_LIMIT_BACKEND` is unset) is per-PROCESS, so on a multi-replica Fly
// deployment it would silently give each replica its OWN independent
// ceiling — `N` replicas would let `N ×` the intended traffic through. This
// is a REAL property of the abstraction (see `rate_limit.rs`'s own doc
// comment, and `mcp::mod::McpConfig::from_env`'s identical warning for MCP's
// unrelated rate limits) — the code deliberately doesn't special-case a
// backend per route, and nothing here enforces `RATE_LIMIT_BACKEND=pg` the
// way `MCP_ENABLED` soft-warns about it.
//
// **Verified against the actual running production app, not assumed**:
// `fly ssh console -a coralswarm-backend -C 'printenv RATE_LIMIT_BACKEND'`
// returns `pg` (2026-09-08) — `PgRateLimiter`, shared across every replica
// via `rate_limit_window` (migration 0042), the same as `pair/poll`'s
// pre-existing limit already relied on before this PR touched anything. So
// all three addin limits are, TODAY, genuinely multi-replica-safe as
// deployed. That is an operational fact about the current deployment, not a
// structural guarantee this code enforces — if a future deploy ever unsets
// `RATE_LIMIT_BACKEND` or flips it off `pg`, every rate limit in this file
// (and every other route using `state.rate_limiter`) silently degrades to
// per-replica, with no error and no warning specific to this module. Worth
// tracking as a fleet-wide startup invariant (mirroring the `MCP_ENABLED`
// precedent) rather than an addin-specific fix — out of scope here.
/// Shared bucket for the (should-not-happen-in-production) case
/// `extract_client_ip` finds no `Fly-Client-IP` header at all — local dev
/// with no Fly proxy in front, or a misconfigured deployment. One shared
/// ceiling for every such caller, rather than an unlimited fallback: a
/// missing trusted header degrades this endpoint to "one bucket for
/// everyone", never to "no limit at all".
const PAIR_START_UNKNOWN_IP_KEY: &str = "addin_start_ip:unknown";

// ── Add-on token resolution (used by `middleware::auth::require_auth`) ────────

/// The identity a verified `csat_...` bearer resolves to — the add-on-token
/// analogue of what `resolve_clerk_context` produces from a Clerk JWT.
pub(crate) struct AddinIdentity {
    pub(crate) token_id: Uuid,
    pub(crate) account_id: Uuid,
    pub(crate) user_id: Uuid,
}

/// Look up a `csat_`-prefixed bearer token by its SHA-256 hash. `Ok(None)`
/// covers "no such token", "wrong hash" and "revoked" identically (the
/// `revoked_at IS NULL` predicate is IN the query, not a second check after
/// the fact) — `require_auth` maps every `Ok(None)` to the same 401 an
/// invalid Clerk JWT gets, so a revoked token is indistinguishable from a
/// bogus one on the wire.
///
/// Hash-then-index-lookup, not a byte-by-byte compare in application code:
/// the DB round trip this query pays dwarfs any timing signal a manual
/// compare could leak, the same posture this repo's other hash-keyed
/// credential lookups already take (see [`pair_poll`]'s identical reasoning
/// for `poll_secret_hash`).
pub(crate) async fn resolve_addin_token(
    pool: &PgPool,
    raw_token: &str,
) -> Result<Option<AddinIdentity>, sqlx::Error> {
    let hash = hash_token(raw_token);
    let row: Option<(Uuid, Uuid, Uuid)> = sqlx::query_as(
        "SELECT id, account_id, owner_user_id FROM addin_tokens \
         WHERE token_hash = $1 AND revoked_at IS NULL",
    )
    .bind(&hash)
    .fetch_optional(crate::tenancy::scope::request_read(pool))
    .await?;
    Ok(row.map(|(token_id, account_id, user_id)| AddinIdentity {
        token_id,
        account_id,
        user_id,
    }))
}

/// Bound on the fire-and-forget `last_used_at` write [`touch_last_used`]
/// spawns off the request's critical path — same rationale and same 2s bound
/// as `middleware::auth::DISPLAY_NAME_REFRESH_TIMEOUT` (`sync_user_display_
/// name`'s equivalent write): long enough for an ordinary `UPDATE` under
/// normal load, short enough that a wedged/saturated pool doesn't accumulate
/// an unbounded number of stuck background tasks — one per authenticated
/// add-on request, with no bound at all before this (CodeRabbit review on
/// #820).
const LAST_USED_TOUCH_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(2);

/// Best-effort `last_used_at` bump — fire-and-forget, same posture as
/// `middleware::access_log::spawn_capture`: a DB write that observes a
/// credential was used must never fail the request it's observing.
pub(crate) fn touch_last_used(pool: PgPool, token_id: Uuid) {
    tokio::spawn(async move {
        let outcome = tokio::time::timeout(
            LAST_USED_TOUCH_TIMEOUT,
            sqlx::query("UPDATE addin_tokens SET last_used_at = now() WHERE id = $1")
                .bind(token_id)
                .execute(&pool),
        )
        .await;
        match outcome {
            Ok(Ok(_)) => {}
            Ok(Err(e)) => {
                tracing::warn!(
                    error = %e,
                    token_id = %token_id,
                    "addin_tokens last_used_at touch failed (best-effort, request unaffected)"
                );
            }
            Err(_) => {
                tracing::warn!(
                    token_id = %token_id,
                    timeout_secs = LAST_USED_TOUCH_TIMEOUT.as_secs(),
                    "addin_tokens last_used_at touch timed out (best-effort, request unaffected)"
                );
            }
        }
    });
}

// ── Secret generation / hashing ─────────────────────────────────────────────
//
// `hash_token` and the CSPRNG-secret generator live in `routes::pat` — see
// that module's doc comment. `addin_pairing_codes.
// poll_secret_hash` also goes through the same `hash_token`, since it is the
// identical "never persist the plaintext" requirement, just for a value that
// isn't itself a PAT.

/// Mint a fresh add-on PAT: 32 bytes of CSPRNG randomness, base64url (no
/// padding) encoded, `csat_`-prefixed.
fn generate_pat() -> String {
    generate_pat_with_prefix(ADDIN_TOKEN_PREFIX)
}

/// Mint the pairing handshake's high-entropy half — see this module's header
/// on why this, not `code`, is the real security boundary.
pub(super) fn generate_poll_secret() -> String {
    let mut bytes = [0u8; 32];
    rand::thread_rng().fill(&mut bytes);
    URL_SAFE_NO_PAD.encode(bytes)
}

/// Mint the pairing handshake's human-glanceable half.
pub(super) fn generate_pairing_code() -> String {
    let mut rng = rand::thread_rng();
    (0..PAIRING_CODE_LEN)
        .map(|_| PAIRING_CODE_ALPHABET[rng.gen_range(0..PAIRING_CODE_ALPHABET.len())] as char)
        .collect()
}

// ── Request bodies ──────────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct PairApproveRequest {
    pub code: String,
    /// Microsoft add-in pairings ONLY (`routes::addin_microsoft`): the
    /// workspace the approving user picked. Absent for a Gmail pairing, whose
    /// behavior is unchanged (personal plane only, no workspace choice).
    #[serde(default)]
    pub account_id: Option<Uuid>,
}

#[derive(Debug, Deserialize)]
pub struct PairPollRequest {
    pub code: String,
    pub poll_secret: String,
}

// ── Shared response helpers ──────────────────────────────────────────────────

fn service_unavailable() -> Response {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        Json(json!({"error": "service unavailable"})),
    )
        .into_response()
}
pub(super) fn bad_request(msg: &'static str) -> Response {
    (StatusCode::BAD_REQUEST, Json(json!({"error": msg}))).into_response()
}
pub(super) fn not_found(msg: &'static str) -> Response {
    (StatusCode::NOT_FOUND, Json(json!({"error": msg}))).into_response()
}
pub(super) fn conflict(msg: &'static str) -> Response {
    (StatusCode::CONFLICT, Json(json!({"error": msg}))).into_response()
}
/// The collapsed response `pair_poll` returns for every failure mode it
/// refuses to distinguish — see its own doc comment.
fn pairing_expired_response() -> Response {
    (StatusCode::OK, Json(json!({"status": "expired"}))).into_response()
}

// ── Orphaned-credential cleanup (CodeRabbit review on #820) ──────────────────
//
// `pair_approve` mints an ACTIVE `addin_tokens` row (`revoked_at IS NULL`)
// before `pair/poll` ever retrieves its plaintext. If the pairing row is
// cleaned up — expired, or swept — before that retrieval happens, the two
// helpers below are what stop the minted token from being left permanently
// active with no pairing record left to name it and no plaintext anyone can
// ever present again. Both REVOKE the linked token BEFORE deleting the
// pairing row that names it; see migration 0210's own header for the full
// design ("`addin_token_id` — the orphaned-credential fix").

/// Single-row cleanup: [`pair_poll`]'s own expiry check, which already knows
/// exactly which `code` and (if approved) which `addin_token_id` it's
/// looking at, inside the transaction it already holds `FOR UPDATE`.
async fn revoke_linked_token_and_delete_pairing_row(
    conn: &mut sqlx::PgConnection,
    code: &str,
    addin_token_id: Option<Uuid>,
) -> Result<(), sqlx::Error> {
    if let Some(token_id) = addin_token_id {
        sqlx::query("UPDATE addin_tokens SET revoked_at = now() WHERE id = $1 AND revoked_at IS NULL")
            .bind(token_id)
            .execute(&mut *conn)
            .await?;
    }
    sqlx::query("DELETE FROM addin_pairing_codes WHERE code = $1")
        .bind(code)
        .execute(&mut *conn)
        .await?;
    Ok(())
}

/// Bulk cleanup: [`pair_start`]'s opportunistic sweep of pairing rows more
/// than a day past their `expires_at`, which doesn't know in advance which
/// (if any) of the swept rows were approved. One transaction: a bulk
/// `UPDATE ... WHERE id IN (subquery)` revokes every linked, still-active
/// token the sweep is about to orphan, THEN the matching `DELETE` removes
/// the pairing rows — the same revoke-then-delete order as the single-row
/// helper above, just set-based instead of keyed on one known `code`.
pub(super) async fn sweep_stale_pairing_rows(pool: &PgPool) -> Result<(), sqlx::Error> {
    let mut tx = pool.begin().await?;
    sqlx::query(
        "UPDATE addin_tokens SET revoked_at = now() \
         WHERE revoked_at IS NULL \
           AND id IN ( \
             SELECT addin_token_id FROM addin_pairing_codes \
             WHERE expires_at < now() - INTERVAL '1 day' AND addin_token_id IS NOT NULL \
           )",
    )
    .execute(&mut *tx)
    .await?;
    sqlx::query("DELETE FROM addin_pairing_codes WHERE expires_at < now() - INTERVAL '1 day'")
        .execute(&mut *tx)
        .await?;
    tx.commit().await
}

// ── POST /v1/addin/pair/start (UNAUTHENTICATED) ──────────────────────────────

/// Start a pairing attempt. No CoralSwarm identity exists yet at this point —
/// this is the very first call the add-on ever makes.
pub async fn pair_start(State(state): State<Arc<AppState>>, headers: axum::http::HeaderMap) -> Response {
    let Some(pool) = state.control_db() else {
        return service_unavailable();
    };

    // Unauthenticated AND, before this fix, unlimited: anyone could script
    // unbounded calls, each writing a fresh `addin_pairing_codes` row (and
    // running the sweep below every time) — sustained control-plane write
    // load and row bloat, not a one-time cost (CodeRabbit review on #820).
    // Keyed on the caller's IP, not `code` — there is no code yet, this
    // route MINTS one.
    let rate_limit_key = match crate::middleware::access_log::extract_client_ip(&headers) {
        Some(ip) => format!("addin_start_ip:{ip}"),
        None => PAIR_START_UNKNOWN_IP_KEY.to_string(),
    };
    if !state
        .rate_limiter
        .check_and_increment_key(
            &rate_limit_key,
            PAIR_START_RATE_MAX_PER_WINDOW,
            PAIR_START_RATE_WINDOW_SECS,
        )
        .await
    {
        return (
            StatusCode::TOO_MANY_REQUESTS,
            Json(json!({"error": "rate limit exceeded"})),
        )
            .into_response();
    }

    // Opportunistic sweep of old, never-approved-or-polled rows — see
    // migration 0211's header on why there's no dedicated sweep worker yet.
    // Cheap (indexed on `expires_at`) and best-effort: a failure here must
    // never block minting a NEW pairing attempt. Revokes each swept row's
    // linked `addin_tokens` row FIRST — see [`sweep_stale_pairing_rows`]'s
    // own doc comment (the orphaned-credential fix, CodeRabbit review on
    // #820).
    if let Err(e) = sweep_stale_pairing_rows(pool).await {
        tracing::warn!(error = %e, "addin pair/start: opportunistic sweep failed (non-fatal)");
    }

    let poll_secret = generate_poll_secret();
    let poll_secret_hash = hash_token(&poll_secret);
    let expires_at = Utc::now() + chrono::Duration::minutes(PAIRING_TTL_MINUTES);

    for _ in 0..PAIRING_CODE_MAX_ATTEMPTS {
        let code = generate_pairing_code();
        match sqlx::query(
            "INSERT INTO addin_pairing_codes (code, poll_secret_hash, expires_at) \
             VALUES ($1, $2, $3)",
        )
        .bind(&code)
        .bind(&poll_secret_hash)
        .bind(expires_at)
        .execute(pool)
        .await
        {
            Ok(_) => {
                return (
                    StatusCode::OK,
                    Json(json!({
                        "code": code,
                        "poll_secret": poll_secret,
                        "expires_at": expires_at,
                    })),
                )
                    .into_response();
            }
            // 23505 unique_violation on the `code` primary key: another
            // still-live row already used this code. Retry with a fresh one
            // rather than ever overwriting an in-flight pairing attempt.
            Err(sqlx::Error::Database(e)) if e.code().as_deref() == Some("23505") => continue,
            Err(e) => {
                tracing::error!(error = %e, "addin pair/start: insert failed");
                return service_unavailable();
            }
        }
    }
    tracing::error!("addin pair/start: exhausted retries generating a unique pairing code");
    service_unavailable()
}

// ── POST /v1/addin/pair/approve (AUTHENTICATED) ──────────────────────────────

/// Approve a pairing attempt and mint the real PAT, bound to THIS caller's
/// own authenticated identity — never to anything the client sends. Personal-
/// plane only (mirrors `reject_org_connector_access`'s posture for Gmail: an
/// add-on token is inherently one human's own mailbox pairing, never an
/// org-wide credential).
///
/// **Rate-limited per `code`** (`PAIR_CODE_RATE_MAX_PER_WINDOW`/`_WINDOW_SECS`
/// — same shape and same constants as [`pair_poll`]'s own limit, under a
/// distinct key prefix so the two never share a bucket). Being authenticated
/// does not make this endpoint safe to leave unthrottled: an attacker still
/// needs no secret at all to CALL this route, only their own ordinary
/// session, and can script requests against many different `code` guesses
/// trying to find someone else's currently-live pairing attempt before its
/// owner approves it — a race that binds the pairing to the ATTACKER's
/// account rather than leaking the PAT itself (see the caller's own doc
/// comment on that trade-off), but a race worth slowing down regardless.
pub async fn pair_approve(
    State(state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(user_ctx): Extension<UserContext>,
    Json(body): Json<PairApproveRequest>,
) -> Response {
    // Settings launch PR 2.8: a human, browser-session action only
    // (`middleware::auth::authority_allowed`).
    if let Some(refusal) = crate::middleware::auth::interactive_session_refusal() {
        return refusal.into_response();
    }
    // Microsoft add-in pairing: the caller names the workspace to link, so the
    // ACTIVE workspace of their Clerk session is irrelevant (a user whose web
    // session is currently inside an org must still be able to pick their
    // personal workspace, and vice versa). Handled entirely in
    // `routes::addin_microsoft::approve_pairing`, which verifies the pairing
    // is a Microsoft one and the caller's membership of `account_id`. A body
    // WITHOUT `account_id` falls through to the Gmail path below, byte-for-byte
    // unchanged — and that path refuses a Microsoft row (see its `kind` check).
    if let Some(account_id) = body.account_id {
        return super::addin_microsoft::approve_pairing(
            &state,
            user_ctx.user_id,
            body.code.trim(),
            account_id,
        )
        .await;
    }

    // 400, not 403: the caller is authenticated fine, it's the ACTIVE
    // WORKSPACE that's wrong for this action — same distinction the org
    // gates elsewhere in this codebase draw between "who are you" and
    // "which ocean are you standing in". Checked BEFORE the rate limit and
    // before touching the database: an org-context caller is refused
    // identically regardless of how many times they call, so there is no
    // reason to spend a rate-limit slot (or a DB round trip) deciding that.
    if tenant.org_context().is_some() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({
                "error": "add-on pairing is available in your personal workspace only; \
                          switch out of the organization workspace to approve it"
            })),
        )
            .into_response();
    }

    let Some(pool) = state.control_db() else {
        return service_unavailable();
    };

    // Trimmed BEFORE it becomes the rate-limit key — CodeRabbit review on
    // #820 caught that the first version of this fix keyed the limiter on
    // the raw `body.code` while the DB lookup below used `.trim()`, so
    // `"ABC12345"` and `" ABC12345"` (and any other whitespace padding)
    // landed in DIFFERENT buckets while querying the IDENTICAL row —
    // trivially defeating the 20-req/60s cap by varying padding on every
    // request. One `code` value, used for both the key and the query, is
    // the whole fix; see `pair_poll`'s own (correct, single-value) shape
    // just below in this file.
    let code = body.code.trim();
    if code.is_empty() {
        return bad_request("code is required");
    }

    if !state
        .rate_limiter
        .check_and_increment_key(
            &format!("addin_approve:{code}"),
            PAIR_CODE_RATE_MAX_PER_WINDOW,
            PAIR_CODE_RATE_WINDOW_SECS,
        )
        .await
    {
        return (
            StatusCode::TOO_MANY_REQUESTS,
            Json(json!({"error": "rate limit exceeded"})),
        )
            .into_response();
    }

    let mut tx = match pool.begin().await {
        Ok(tx) => tx,
        Err(e) => {
            tracing::error!(error = %e, "addin pair/approve: begin failed");
            return service_unavailable();
        }
    };

    // `FOR UPDATE`: a second `approve` call for the same code (a double
    // click, a retried request) blocks on this row rather than racing the
    // `approved_at IS NULL` check below.
    let row: Option<(DateTime<Utc>, Option<DateTime<Utc>>, String)> = match sqlx::query_as(
        "SELECT expires_at, approved_at, kind FROM addin_pairing_codes WHERE code = $1 FOR UPDATE",
    )
    .bind(code)
    .fetch_optional(&mut *tx)
    .await
    {
        Ok(v) => v,
        Err(e) => {
            tracing::error!(error = %e, "addin pair/approve: lookup failed");
            return service_unavailable();
        }
    };

    let Some((expires_at, approved_at, kind)) = row else {
        return not_found("pairing code not found or already used");
    };
    if approved_at.is_some() {
        return conflict("this pairing code has already been approved");
    }
    // A Microsoft pairing must NEVER mint a `csat_` PAT here: it carries no
    // token at all, and approving it without naming a workspace would bind
    // nothing. The caller must send `account_id` (handled above).
    if kind != "gmail" {
        return bad_request("account_id is required to approve this pairing");
    }
    if Utc::now() > expires_at {
        // Never resurrect an expired code — delete it here so a slow retry
        // racing past this check can't approve a row this response already
        // refused.
        let _ = sqlx::query("DELETE FROM addin_pairing_codes WHERE code = $1")
            .bind(code)
            .execute(&mut *tx)
            .await;
        let _ = tx.commit().await;
        return bad_request("this pairing code has expired — restart pairing from Gmail");
    }

    let plaintext = generate_pat();
    let hash = hash_token(&plaintext);

    // The security-critical binding: `user_ctx.account_id`/`user_ctx.user_id`
    // come from the AUTHENTICATED session (`require_auth` populated
    // `UserContext` from a verified Clerk JWT before this handler ever ran),
    // never from `body` or any other client-supplied value.
    //
    // `RETURNING id`: the minted token's own id is persisted onto the
    // pairing row below (`addin_token_id`) — the orphaned-credential fix
    // (migration 0210's header, CodeRabbit review on #820). Without it, a
    // pairing row cleaned up between this INSERT and a successful `pair/
    // poll` retrieval would leave this token permanently active with
    // nothing left anywhere naming it.
    let token_id: Uuid = match sqlx::query_scalar(
        "INSERT INTO addin_tokens (account_id, owner_user_id, token_hash) \
         VALUES ($1, $2, $3) RETURNING id",
    )
    .bind(user_ctx.account_id)
    .bind(user_ctx.user_id)
    .bind(&hash)
    .fetch_one(&mut *tx)
    .await
    {
        Ok(id) => id,
        Err(e) => {
            tracing::error!(error = %e, "addin pair/approve: token insert failed");
            return service_unavailable();
        }
    };

    if let Err(e) = sqlx::query(
        "UPDATE addin_pairing_codes \
         SET account_id = $2, owner_user_id = $3, token_plaintext_once = $4, \
             addin_token_id = $5, approved_at = now() \
         WHERE code = $1",
    )
    .bind(code)
    .bind(user_ctx.account_id)
    .bind(user_ctx.user_id)
    .bind(&plaintext)
    .bind(token_id)
    .execute(&mut *tx)
    .await
    {
        tracing::error!(error = %e, "addin pair/approve: pairing row update failed");
        return service_unavailable();
    }

    if let Err(e) = tx.commit().await {
        tracing::error!(error = %e, "addin pair/approve: commit failed");
        return service_unavailable();
    }

    if crate::read_models::dirty::connector_account_changed(&state,user_ctx.account_id,user_ctx.user_id).await.is_err() {
        return service_unavailable();
    }
    (StatusCode::OK, Json(json!({"approved": true}))).into_response()
}

// ── POST /v1/addin/pair/poll (UNAUTHENTICATED, poll_secret-gated) ──────────

/// Retrieve a pairing attempt's outcome. `poll_secret` is the entire
/// authorization boundary — there is no `UserContext` here at all, by design
/// (the add-on has no CoralSwarm identity until this call succeeds).
///
/// **Every failure mode collapses to the SAME `{"status": "expired"}`
/// response**: an unknown `code`, a `code` whose `poll_secret` doesn't match,
/// a genuinely expired row, and an already-consumed row are all
/// indistinguishable on the wire. This is deliberate (see migration 0211's
/// header): distinguishing "wrong secret" from "no such code" would let an
/// attacker holding neither learn something about which 8-character codes
/// are currently live without ever presenting a matching `poll_secret` — the
/// exact oracle splitting the two secrets exists to prevent.
///
/// The `approved` response carries `account_email` alongside the plaintext:
/// the `users.email` of whoever actually approved this pairing, so the
/// add-on can render "Connected as <email>" and a pairing bound to the
/// wrong account announces itself instead of failing silently — see this
/// module's header, "The approve race". It is looked up strictly from the
/// `owner_user_id` `pair_approve` wrote onto this pairing row (i.e. from
/// the approving SESSION), never from anything the poller sends, and it is
/// best-effort: a lookup failure degrades the field to `null` rather than
/// failing a retrieval that can only ever happen once.
pub async fn pair_poll(State(state): State<Arc<AppState>>, Json(body): Json<PairPollRequest>) -> Response {
    let Some(pool) = state.control_db() else {
        return service_unavailable();
    };

    if !state
        .rate_limiter
        .check_and_increment_key(
            &format!("addin_poll:{}", body.code),
            PAIR_CODE_RATE_MAX_PER_WINDOW,
            PAIR_CODE_RATE_WINDOW_SECS,
        )
        .await
    {
        return (
            StatusCode::TOO_MANY_REQUESTS,
            Json(json!({"error": "rate limit exceeded"})),
        )
            .into_response();
    }

    let secret_hash = hash_token(&body.poll_secret);

    let mut tx = match pool.begin().await {
        Ok(tx) => tx,
        Err(e) => {
            tracing::error!(error = %e, "addin pair/poll: begin failed");
            return service_unavailable();
        }
    };

    // `FOR UPDATE`: a second poll for the SAME code racing this one blocks on
    // this row until this transaction commits (see the single-retrieval
    // comment on the `(Some(_), Some(token))` arm below) — the mechanism that
    // makes "read the plaintext exactly once" true even under concurrency,
    // not just under a single caller behaving well.
    // `owner_user_id` rides along purely to resolve `account_email` on the
    // approved path below — the approving SESSION's user, written onto this
    // row by `pair_approve` from its own `UserContext`, never anything the
    // poller supplies.
    #[allow(clippy::type_complexity)]
    let row: Option<(
        String,
        DateTime<Utc>,
        Option<DateTime<Utc>>,
        Option<String>,
        Option<Uuid>,
        Option<Uuid>,
        String,
    )> = match sqlx::query_as(
        "SELECT poll_secret_hash, expires_at, approved_at, token_plaintext_once, \
                addin_token_id, owner_user_id, kind \
         FROM addin_pairing_codes WHERE code = $1 FOR UPDATE",
    )
    .bind(&body.code)
    .fetch_optional(&mut *tx)
    .await
    {
        Ok(v) => v,
        Err(e) => {
            tracing::error!(error = %e, "addin pair/poll: lookup failed");
            return service_unavailable();
        }
    };

    let Some((stored_hash, expires_at, approved_at, token_once, addin_token_id, owner_user_id, kind)) =
        row
    else {
        let _ = tx.rollback().await;
        return pairing_expired_response();
    };

    // Hex-encoded SHA-256 equality via a plain `==` on two fixed-length hex
    // strings: the DB round trip this lookup already paid dwarfs any timing
    // signal a byte-by-byte compare could leak — the same posture
    // `resolve_addin_token`'s hash-indexed lookup takes above.
    if stored_hash != secret_hash {
        let _ = tx.rollback().await;
        return pairing_expired_response();
    }

    if Utc::now() > expires_at {
        // Revoke the linked token (if this pairing was approved but never
        // retrieved) BEFORE deleting the row that names it — the orphaned-
        // credential fix (migration 0210's header, CodeRabbit review on
        // #820). `addin_token_id` is `None` for a never-approved row, so
        // this is a no-op there, same as before.
        if let Err(e) =
            revoke_linked_token_and_delete_pairing_row(&mut tx, &body.code, addin_token_id).await
        {
            tracing::error!(error = %e, "addin pair/poll: expiry cleanup failed");
        }
        let _ = tx.commit().await;
        return pairing_expired_response();
    }

    // Microsoft add-in pairing (`routes::addin_microsoft`): approval created the
    // LINK, not a token, so there is nothing to retrieve — `approved` carries no
    // credential and the pane calls `POST /v1/addin/microsoft/session` again to
    // get `connected`. The pairing row has done its job once `approved` has been
    // reported, so it is deleted here (single-use, same as the Gmail row), and
    // a second poll collapses to `expired` like every other spent pairing.
    if kind == "microsoft" {
        return match approved_at {
            None => {
                let _ = tx.commit().await;
                (StatusCode::OK, Json(json!({"status": "pending"}))).into_response()
            }
            Some(_) => {
                if let Err(e) = sqlx::query("DELETE FROM addin_pairing_codes WHERE code = $1")
                    .bind(&body.code)
                    .execute(&mut *tx)
                    .await
                {
                    tracing::error!(error = %e, "addin pair/poll (microsoft): post-approval delete failed");
                    let _ = tx.rollback().await;
                    return service_unavailable();
                }
                if let Err(e) = tx.commit().await {
                    tracing::error!(error = %e, "addin pair/poll (microsoft): commit failed");
                    return service_unavailable();
                }
                (StatusCode::OK, Json(json!({"status": "approved"}))).into_response()
            }
        };
    }

    match (approved_at, token_once) {
        (None, _) => {
            let _ = tx.commit().await;
            (StatusCode::OK, Json(json!({"status": "pending"}))).into_response()
        }
        (Some(_), Some(token)) => {
            // The single-retrieval guarantee: delete the row NOW, inside the
            // SAME transaction that read `token_plaintext_once` under
            // `FOR UPDATE`. A second poll racing this one is blocked on the
            // row lock until this commits, and then finds no row at all (the
            // delete already landed) — never a second copy of the plaintext.
            //
            // The token is NOT revoked here — this is the intended delivery,
            // not an orphan: the caller is about to receive the plaintext,
            // so the credential is meant to stay active. Only the CLEANUP
            // paths above/below revoke.
            //
            // Resolve the approving user's email BEFORE the row naming them
            // is deleted, in the same transaction. Best-effort by
            // construction: `owner_user_id` is `Some` on every row that can
            // reach this arm (`pair_approve` writes it in the same statement
            // that sets `approved_at`), but a failed or empty lookup
            // degrades this field to `null` rather than failing a retrieval
            // the caller can never repeat — after this transaction commits
            // the plaintext is gone from the database, so answering 503 here
            // would destroy a perfectly good credential over a cosmetic
            // field.
            let account_email: Option<String> = match owner_user_id {
                Some(uid) => match sqlx::query_scalar("SELECT email FROM users WHERE id = $1")
                    .bind(uid)
                    .fetch_optional(&mut *tx)
                    .await
                {
                    Ok(email) => email,
                    Err(e) => {
                        tracing::warn!(
                            error = %e,
                            "addin pair/poll: account_email lookup failed (non-fatal)"
                        );
                        None
                    }
                },
                None => None,
            };

            if let Err(e) = sqlx::query("DELETE FROM addin_pairing_codes WHERE code = $1")
                .bind(&body.code)
                .execute(&mut *tx)
                .await
            {
                tracing::error!(error = %e, "addin pair/poll: post-retrieval delete failed");
                let _ = tx.rollback().await;
                return service_unavailable();
            }
            if let Err(e) = tx.commit().await {
                tracing::error!(error = %e, "addin pair/poll: commit failed");
                return service_unavailable();
            }
            // `token_id` rides alongside the plaintext (Major 5's fix,
            // CodeRabbit review on #820): the add-on stores it and can now
            // call the existing owner-scoped `DELETE /v1/addin/tokens/{id}`
            // for a REAL local disconnect, using the token itself as its own
            // bearer (see `routes::addin`'s module doc and
            // `extensions/gmail-addin/Pairing.gs`'s `disconnect`).
            //
            // `account_email` rides alongside BOTH: the address the add-on
            // renders as "Connected as <email>", which is what makes a
            // pairing bound to the wrong account visible to the human
            // instead of silent (this module's header, "The approve race").
            (
                StatusCode::OK,
                Json(json!({
                    "status": "approved",
                    "token": token,
                    "token_id": addin_token_id,
                    "account_email": account_email,
                })),
            )
                .into_response()
        }
        // Approved, but the plaintext is already gone: a second poll after
        // the first already consumed it. Collapsed into the same response as
        // every other non-retrievable state — there is no legitimate reason
        // for a well-behaved caller to poll twice after `approved`, and a
        // second read here must not become a distinct "already used" oracle.
        (Some(_), None) => {
            let _ = tx.commit().await;
            pairing_expired_response()
        }
    }
}

// ── GET /v1/addin/tokens (AUTHENTICATED) ─────────────────────────────────────

/// List the caller's OWN live add-on tokens — the read half of the pair
/// [`revoke_token`] completes. Consumed by the personal `/connectors` page,
/// which renders one row per token with a Revoke button calling the existing
/// `DELETE /v1/addin/tokens/{id}`; before this route existed, the ONLY way
/// to revoke was from inside Gmail itself (`Pairing.gs`'s `disconnect`),
/// which is useless in exactly the case that matters — the device with the
/// add-on installed is the one you no longer control.
///
/// **Ownership comes from the authenticated session, never the request.**
/// `owner_user_id`/`account_id` are read off `UserContext` (populated by
/// `require_auth` from a verified Clerk JWT *or* a verified `csat_` PAT —
/// both bearer shapes work here, same as [`revoke_token`]), and there is no
/// id, filter or scope parameter on this route at all. The `account_id`
/// predicate makes this list strictly NARROWER than the rows
/// [`revoke_token`] will act on (`owner_user_id` alone): every row this
/// returns is one the caller may revoke, which is the safe direction for a
/// list to be wrong in.
///
/// `revoked_at IS NULL` lives IN the query, the same posture
/// [`resolve_addin_token`] takes — a revoked token disappears from this list
/// rather than arriving as a "revoked" state the UI would then have to
/// remember to hide.
///
/// **`token_hash` is never selected, let alone returned**, and no plaintext
/// exists to leak: it is handed over exactly once by [`pair_poll`] and never
/// persisted (this module's header). The four fields here are what a human
/// needs to recognize a credential they already hold — which row Revoke
/// names, what it is, when it was minted, and whether it is still in use.
///
/// Personal-plane only, refused with the same 400 [`pair_approve`] returns:
/// an add-on token can never be an org credential (`addin_tokens.
/// owner_user_id` is `NOT NULL`), so an org workspace has no such list, and
/// answering `{"tokens": []}` there would read as "you have none" rather
/// than "you are standing in the wrong workspace".
///
/// **Deliberately unpaginated and uncapped.** One row per pairing this one
/// human has completed and not yet revoked — human-scale by construction,
/// not a table that grows with usage. A `LIMIT` here would be actively
/// harmful: this list is the ONLY web-side way to revoke, so a truncated
/// page would silently hide live credentials from the one UI that can kill
/// them, which is exactly backwards for a security surface. If this ever
/// does need paging, the page has to make the truncation visible rather
/// than rendering a short list as a complete one.
pub async fn list_tokens(
    State(state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(user_ctx): Extension<UserContext>,
) -> Response {
    // Checked before the pool is even resolved, exactly as `pair_approve`
    // checks it before the rate limiter and before any DB round trip.
    if tenant.org_context().is_some() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({
                "error": "add-on tokens live in your personal workspace only; \
                          switch out of the organization workspace to manage them"
            })),
        )
            .into_response();
    }

    let Some(pool) = state.control_db() else {
        return service_unavailable();
    };

    let rows: Vec<(Uuid, String, DateTime<Utc>, Option<DateTime<Utc>>)> = match sqlx::query_as(
        "SELECT id, name, created_at, last_used_at FROM addin_tokens \
         WHERE owner_user_id = $1 AND account_id = $2 AND revoked_at IS NULL \
         ORDER BY created_at DESC",
    )
    .bind(user_ctx.user_id)
    .bind(user_ctx.account_id)
    .fetch_all(crate::tenancy::scope::request_read(pool))
    .await
    {
        Ok(rows) => rows,
        Err(e) => {
            tracing::error!(error = %e, "addin token list failed");
            return service_unavailable();
        }
    };

    let tokens: Vec<serde_json::Value> = rows
        .into_iter()
        .map(|(id, name, created_at, last_used_at)| {
            json!({
                "id": id,
                "name": name,
                // chrono's own `Serialize` — RFC 3339, and `null` for a
                // token that has never authenticated a request.
                "created_at": created_at,
                "last_used_at": last_used_at,
            })
        })
        .collect();

    (StatusCode::OK, Json(json!({"tokens": tokens}))).into_response()
}

// ── DELETE /v1/addin/tokens/{id} (AUTHENTICATED) ─────────────────────────────

/// Revoke one of the caller's own add-on tokens. Scoped to `owner_user_id` —
/// the only authorization check this needs, since an add-on token is always
/// member-scoped (never `AccountWide`, see this module's header).
pub async fn revoke_token(
    State(state): State<Arc<AppState>>,
    Extension(user_ctx): Extension<UserContext>,
    Path(id): Path<Uuid>,
) -> Response {
    let Some(pool) = state.control_db() else {
        return service_unavailable();
    };
    let result = sqlx::query(
        "UPDATE addin_tokens SET revoked_at = now() \
         WHERE id = $1 AND owner_user_id = $2 AND revoked_at IS NULL",
    )
    .bind(id)
    .bind(user_ctx.user_id)
    .execute(pool)
    .await;
    match result {
        Ok(r) if r.rows_affected() == 1 => {
            if crate::read_models::dirty::connector_account_changed(&state,user_ctx.account_id,user_ctx.user_id).await.is_err() {
                return service_unavailable();
            }
            StatusCode::NO_CONTENT.into_response()
        },
        // Covers "no such id", "already revoked", and "belongs to someone
        // else" identically — a caller learns nothing about a token they
        // don't own beyond "you can't act on this".
        Ok(_) => not_found("add-on token not found"),
        Err(e) => {
            tracing::error!(error = %e, "addin token revoke failed");
            service_unavailable()
        }
    }
}

// ── Unit tests (no DB) ────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hash_token_is_deterministic_and_64_hex_chars() {
        let h1 = hash_token("csat_example");
        let h2 = hash_token("csat_example");
        assert_eq!(h1, h2);
        assert_eq!(h1.len(), 64);
        assert!(h1.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn hash_token_differs_for_different_inputs() {
        assert_ne!(hash_token("a"), hash_token("b"));
    }

    #[test]
    fn generate_pat_carries_the_addin_prefix_and_never_repeats() {
        let a = generate_pat();
        let b = generate_pat();
        assert!(a.starts_with(ADDIN_TOKEN_PREFIX));
        assert!(b.starts_with(ADDIN_TOKEN_PREFIX));
        assert_ne!(a, b, "two mints must never collide in a sample of two");
        // 32 raw bytes, base64url no-pad, plus the prefix.
        assert!(a.len() > ADDIN_TOKEN_PREFIX.len() + 32);
    }

    #[test]
    fn generate_poll_secret_never_repeats_and_carries_no_addin_prefix() {
        let a = generate_poll_secret();
        let b = generate_poll_secret();
        assert_ne!(a, b);
        // The poll secret is a bare high-entropy value, not a bearer token —
        // it must never accidentally look like one.
        assert!(!a.starts_with(ADDIN_TOKEN_PREFIX));
    }

    #[test]
    fn generate_pairing_code_is_the_right_length_and_alphabet_only() {
        for _ in 0..50 {
            let code = generate_pairing_code();
            assert_eq!(code.len(), PAIRING_CODE_LEN);
            assert!(code.bytes().all(|b| PAIRING_CODE_ALPHABET.contains(&b)));
        }
    }

    #[test]
    fn pairing_code_alphabet_excludes_ambiguous_characters() {
        for bad in [b'0', b'O', b'1', b'I'] {
            assert!(
                !PAIRING_CODE_ALPHABET.contains(&bad),
                "ambiguous char {} must not appear in the pairing-code alphabet",
                bad as char
            );
        }
    }

    /// `pair_approve` must refuse an org-context caller BEFORE it ever
    /// touches the database — proven here with no pool configured at all
    /// (a DB call would panic/return 503 on a `None` pool, not 400).
    #[tokio::test]
    async fn pair_approve_refuses_org_context_before_touching_the_database() {
        let org_tenant = Tenant {
            store: None,
            pool: None,
            decision: Some(crate::tenancy::runtime::TenancyDecision::Org(
                crate::tenancy::runtime::TenantContext {
                    org_id: "org_addin_unit_test".to_string(),
                    account_id: Uuid::new_v4(),
                    user_id: Uuid::new_v4(),
                    pool: sqlx::postgres::PgPoolOptions::new()
                        .connect_lazy("postgres://localhost/unused")
                        .expect("lazy pool construction never connects"),
                    page_pool: None,
                },
            )),
            class: crate::route_class::RouteClass::Other,
        };
        let user_ctx = UserContext {
            user_id: Uuid::new_v4(),
            account_id: Uuid::new_v4(),
            role: "member".to_string(),
            device_id: "test-device".to_string(),
        };
        let resp = pair_approve(
            State(crate::test_app_state()),
            org_tenant,
            Extension(user_ctx),
            Json(PairApproveRequest {
                code: "ABCD1234".to_string(),
                account_id: None,
            }),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    /// Same property for `list_tokens`: an org-context caller is refused
    /// (400) BEFORE `state.control_db()` is consulted at all. Proven with no
    /// pool configured — if the org check ran second, this would be the 503
    /// `service_unavailable()` every other path returns on a `None` pool.
    #[tokio::test]
    async fn list_tokens_refuses_org_context_before_touching_the_database() {
        let org_tenant = Tenant {
            store: None,
            pool: None,
            decision: Some(crate::tenancy::runtime::TenancyDecision::Org(
                crate::tenancy::runtime::TenantContext {
                    org_id: "org_addin_list_unit_test".to_string(),
                    account_id: Uuid::new_v4(),
                    user_id: Uuid::new_v4(),
                    pool: sqlx::postgres::PgPoolOptions::new()
                        .connect_lazy("postgres://localhost/unused")
                        .expect("lazy pool construction never connects"),
                    page_pool: None,
                },
            )),
            class: crate::route_class::RouteClass::Other,
        };
        let user_ctx = UserContext {
            user_id: Uuid::new_v4(),
            account_id: Uuid::new_v4(),
            role: "member".to_string(),
            device_id: "test-device".to_string(),
        };
        let resp = list_tokens(
            State(crate::test_app_state()),
            org_tenant,
            Extension(user_ctx),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    /// `resolve_addin_token` on a state with no control pool must never be
    /// reachable from `require_auth`'s own `state.db`-presence check — this
    /// pins the function's own contract (a `PgPool` argument, no `Option`
    /// dance inside it) rather than re-deriving `require_auth`'s branch.
    #[test]
    fn addin_token_prefix_is_recognized_and_ordinary_jwts_are_not() {
        assert!("csat_abc123".starts_with(ADDIN_TOKEN_PREFIX));
        assert!(!"eyJhbGciOiJSUzI1NiJ9.abc.def".starts_with(ADDIN_TOKEN_PREFIX));
    }
}

// ---------------------------------------------------------------------------
// Pairing-flow proof (pg-gated) — run against a real Postgres.
// ---------------------------------------------------------------------------
//
//   ORGT_TEST_BASE_URL=postgres://postgres:postgres@localhost:5432/postgres \
//     cargo test -p coralswarm-backend --lib "routes::addin::addin_pg_tests" \
//     -- --test-threads=1
//
// Declared directly at file scope (not nested inside `mod tests` above) so
// `scripts/check_pg_suite_coverage.sh`'s in-module parser can enumerate and
// verify this suite is wired into `.github/workflows/ci.yml` — same
// discipline `routes::connectors`'s own `..._pg_tests` modules follow (see
// that file's own comment on this exact convention). Wired into the
// `cross-tenant-probe` job in the same PR that adds this module.
#[cfg(test)]
mod addin_pg_tests {
    use super::*;
    use sqlx::{Connection, Executor, PgConnection};

    fn base_url() -> Option<String> {
        std::env::var("ORGT_TEST_BASE_URL").ok()
    }
    fn with_db(base: &str, db: &str) -> String {
        let (prefix, rest) = base.split_at(base.rfind('/').expect("url has a path /"));
        let params = rest.find('?').map(|i| &rest[i..]).unwrap_or("");
        format!("{prefix}/{db}{params}")
    }
    async fn recreate(base: &str, db: &str) -> PgConnection {
        let mut conn = PgConnection::connect(base).await.expect("connect admin");
        conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .unwrap();
        conn.execute(format!("CREATE DATABASE {db}").as_str())
            .await
            .unwrap();
        conn
    }
    async fn teardown(base: &str, db: &str, pool: PgPool) {
        pool.close().await;
        let mut admin_conn = PgConnection::connect(base).await.unwrap();
        admin_conn
            .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .ok();
    }

    async fn seed_user_and_account(pool: &PgPool) -> (Uuid, Uuid) {
        let (user_id, account_id, _email) = seed_user_account_and_email(pool).await;
        (user_id, account_id)
    }

    /// The same seed, plus the `users.email` the row was actually created
    /// with — what `pair_poll`'s `account_email` has to echo back, and the
    /// only way to assert that field against a KNOWN value rather than
    /// against whatever the handler happened to return.
    async fn seed_user_account_and_email(pool: &PgPool) -> (Uuid, Uuid, String) {
        let email = format!("user-{}@test.local", Uuid::new_v4());
        let user_id: Uuid =
            sqlx::query_scalar("INSERT INTO users(id, email) VALUES (gen_random_uuid(), $1) RETURNING id")
                .bind(&email)
                .fetch_one(pool)
                .await
                .unwrap();
        let account_id: Uuid =
            sqlx::query_scalar("INSERT INTO accounts(owner_user_id) VALUES ($1) RETURNING id")
                .bind(user_id)
                .fetch_one(pool)
                .await
                .unwrap();
        (user_id, account_id, email)
    }

    /// Mint a real add-on token for `user_ctx` the way production does —
    /// a pairing row, then the actual `pair_approve` handler — rather than
    /// fabricating an `addin_tokens` row the production path would never
    /// write. Returns the minted token's id.
    async fn mint_token_via_approve(
        state: &Arc<AppState>,
        pool: &PgPool,
        user_ctx: &UserContext,
    ) -> Uuid {
        let code = generate_pairing_code();
        let poll_secret = generate_poll_secret();
        sqlx::query(
            "INSERT INTO addin_pairing_codes (code, poll_secret_hash, expires_at) \
             VALUES ($1, $2, $3)",
        )
        .bind(&code)
        .bind(hash_token(&poll_secret))
        .bind(Utc::now() + chrono::Duration::minutes(10))
        .execute(pool)
        .await
        .unwrap();
        let resp = pair_approve(
            State(state.clone()),
            personal_tenant(),
            Extension(user_ctx.clone()),
            Json(PairApproveRequest { code: code.clone(), account_id: None }),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK, "the fixture approve must succeed");
        let token_id: Option<Uuid> =
            sqlx::query_scalar("SELECT addin_token_id FROM addin_pairing_codes WHERE code = $1")
                .bind(&code)
                .fetch_one(pool)
                .await
                .unwrap();
        token_id.expect("an approved pairing row always names the token it minted")
    }

    fn personal_tenant() -> Tenant {
        Tenant {
            store: None,
            pool: None,
            decision: None,
            class: crate::route_class::RouteClass::Other,
        }
    }
    fn org_tenant() -> Tenant {
        Tenant {
            store: None,
            pool: None,
            decision: Some(crate::tenancy::runtime::TenancyDecision::Org(
                crate::tenancy::runtime::TenantContext {
                    org_id: "org_addin_pg_test".to_string(),
                    account_id: Uuid::new_v4(),
                    user_id: Uuid::new_v4(),
                    pool: sqlx::postgres::PgPoolOptions::new()
                        .connect_lazy("postgres://localhost/unused")
                        .expect("lazy pool construction never connects"),
                    page_pool: None,
                },
            )),
            class: crate::route_class::RouteClass::Other,
        }
    }

    async fn json_body(resp: Response) -> serde_json::Value {
        let bytes = axum::body::to_bytes(resp.into_body(), 1024 * 1024)
            .await
            .unwrap();
        serde_json::from_slice(&bytes).unwrap()
    }

    /// The full pairing-flow proof this feature's security review asked for,
    /// end to end against a real Postgres: start → pending poll → wrong
    /// secret refused → org-context approve refused → approve → single
    /// retrieval → replay refused → the minted token resolves to the
    /// approving session's own identity → revoke → the token stops
    /// resolving → a different user cannot revoke it.
    #[tokio::test]
    async fn full_pairing_flow_mints_a_working_and_revocable_token_pg() {
        let db_name = "addin_pg_test_full_flow";
        let Some(base) = base_url() else {
            eprintln!(
                "skipping full_pairing_flow_mints_a_working_and_revocable_token_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let _database_cleanup_0 = crate::test_db::TestDatabase::adopt_from_url(&base, db_name);
        recreate(&base, db_name).await;
        let url = with_db(&base, db_name);
        let pool = PgPool::connect(&url).await.expect("connect scratch db");
        sqlx::migrate!("./migrations_control")
            .run(&pool)
            .await
            .expect("migrate control tree");

        let state = Arc::new(crate::test_app_state_owned_with_db(pool.clone(), None));

        // 1. Start a pairing attempt directly against the same SQL shape
        //    `pair_start` runs (proves the schema; the axum plumbing around
        //    it is covered by the unit tests above and by `pair_start`
        //    itself being a thin wrapper over this exact insert).
        let poll_secret = generate_poll_secret();
        let code = generate_pairing_code();
        sqlx::query(
            "INSERT INTO addin_pairing_codes (code, poll_secret_hash, expires_at) \
             VALUES ($1, $2, $3)",
        )
        .bind(&code)
        .bind(hash_token(&poll_secret))
        .bind(Utc::now() + chrono::Duration::minutes(10))
        .execute(&pool)
        .await
        .unwrap();

        // 2. Poll before approval → pending.
        let resp = pair_poll(
            State(state.clone()),
            Json(PairPollRequest {
                code: code.clone(),
                poll_secret: poll_secret.clone(),
            }),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(json_body(resp).await["status"], "pending");

        // 3. Wrong poll_secret at this point → "expired", never "pending" —
        //    proves the secret gate runs before ANY state is revealed.
        let resp = pair_poll(
            State(state.clone()),
            Json(PairPollRequest {
                code: code.clone(),
                poll_secret: "wrong-secret-entirely".to_string(),
            }),
        )
        .await;
        assert_eq!(
            json_body(resp).await["status"],
            "expired",
            "a wrong poll_secret must never distinguish itself from an unknown/expired code"
        );

        // 4. An org-context caller must be refused (400) and must NOT be
        //    able to approve — proven against a SECOND, otherwise identical
        //    pairing row so a bug that approves anyway is caught directly.
        let (user_id, account_id, user_email) = seed_user_account_and_email(&pool).await;
        let user_ctx = UserContext {
            user_id,
            account_id,
            role: "owner".to_string(),
            device_id: "test-device".to_string(),
        };
        let code2 = generate_pairing_code();
        let poll_secret2 = generate_poll_secret();
        sqlx::query(
            "INSERT INTO addin_pairing_codes (code, poll_secret_hash, expires_at) \
             VALUES ($1, $2, $3)",
        )
        .bind(&code2)
        .bind(hash_token(&poll_secret2))
        .bind(Utc::now() + chrono::Duration::minutes(10))
        .execute(&pool)
        .await
        .unwrap();
        let resp = pair_approve(
            State(state.clone()),
            org_tenant(),
            Extension(user_ctx.clone()),
            Json(PairApproveRequest { code: code2.clone(), account_id: None }),
        )
        .await;
        assert_eq!(
            resp.status(),
            StatusCode::BAD_REQUEST,
            "an org-context caller must be refused, never bound"
        );
        let still_pending: (Option<DateTime<Utc>>,) =
            sqlx::query_as("SELECT approved_at FROM addin_pairing_codes WHERE code = $1")
                .bind(&code2)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert!(
            still_pending.0.is_none(),
            "the refused org-context call must not have touched the row"
        );

        // 5. Approve, as an authenticated PERSONAL-plane caller — the
        //    security-critical binding: account_id/user_id come from
        //    `user_ctx`, never from the request body.
        let resp = pair_approve(
            State(state.clone()),
            personal_tenant(),
            Extension(user_ctx.clone()),
            Json(PairApproveRequest { code: code.clone(), account_id: None }),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK);

        // 6. A second approve for the SAME code must conflict, not mint a
        //    second token.
        let resp = pair_approve(
            State(state.clone()),
            personal_tenant(),
            Extension(user_ctx.clone()),
            Json(PairApproveRequest { code: code.clone(), account_id: None }),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::CONFLICT);

        // 7. Poll again (the real retrieval) → approved + token, exactly once.
        let resp = pair_poll(
            State(state.clone()),
            Json(PairPollRequest {
                code: code.clone(),
                poll_secret: poll_secret.clone(),
            }),
        )
        .await;
        let body = json_body(resp).await;
        assert_eq!(body["status"], "approved");
        let token = body["token"].as_str().unwrap().to_string();
        assert!(token.starts_with(ADDIN_TOKEN_PREFIX));
        // `account_email` names the user who ACTUALLY approved — the add-on
        // renders it as "Connected as <email>", which is the whole
        // mitigation for the approve race this flow knowingly accepts (see
        // this module's header). Asserted against the email this test
        // SEEDED, so a handler that echoed back something the poller sent,
        // or a hardcoded/empty value, fails here.
        assert_eq!(
            body["account_email"].as_str(),
            Some(user_email.as_str()),
            "the approved response must name the approving session's own account"
        );

        // 8. A second poll for the SAME code/secret → expired. The pairing
        //    row (and its plaintext) must be genuinely gone, not just hidden.
        let resp = pair_poll(
            State(state.clone()),
            Json(PairPollRequest {
                code: code.clone(),
                poll_secret: poll_secret.clone(),
            }),
        )
        .await;
        assert_eq!(
            json_body(resp).await["status"],
            "expired",
            "a consumed pairing code must never be retrievable a second time"
        );
        let row_gone: Option<(String,)> =
            sqlx::query_as("SELECT code FROM addin_pairing_codes WHERE code = $1")
                .bind(&code)
                .fetch_optional(&pool)
                .await
                .unwrap();
        assert!(row_gone.is_none(), "the consumed pairing row must be deleted, not merely marked");

        // 9. The minted token resolves, via the SAME path `require_auth`
        //    uses, to the approving session's own account/user — never
        //    anything the client could have supplied.
        let identity = resolve_addin_token(&pool, &token)
            .await
            .unwrap()
            .expect("a freshly minted token must resolve");
        assert_eq!(identity.account_id, account_id);
        assert_eq!(identity.user_id, user_id);

        // 10. A DIFFERENT user cannot revoke this token.
        let (other_user, other_account) = seed_user_and_account(&pool).await;
        let other_ctx = UserContext {
            user_id: other_user,
            account_id: other_account,
            role: "owner".to_string(),
            device_id: "test-device-2".to_string(),
        };
        let resp = revoke_token(
            State(state.clone()),
            Extension(other_ctx),
            Path(identity.token_id),
        )
        .await;
        assert_eq!(
            resp.status(),
            StatusCode::NOT_FOUND,
            "a caller must never revoke another user's token"
        );
        assert!(
            resolve_addin_token(&pool, &token).await.unwrap().is_some(),
            "the wrongly-attempted revoke must not have touched the token"
        );

        // 11. The OWNING user revokes it, and it stops resolving.
        let resp = revoke_token(
            State(state.clone()),
            Extension(user_ctx.clone()),
            Path(identity.token_id),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::NO_CONTENT);
        assert!(
            resolve_addin_token(&pool, &token).await.unwrap().is_none(),
            "a revoked token must never resolve again"
        );

        // 12. Revoking the same token again is a no-op 404, not a second
        //     "success" — `revoked_at IS NULL` in the UPDATE's own WHERE
        //     clause is what this pins.
        let resp = revoke_token(
            State(state.clone()),
            Extension(user_ctx.clone()),
            Path(identity.token_id),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);

        teardown(&base, db_name, pool).await;
    }

    /// `GET /v1/addin/tokens` shows the caller their OWN live tokens and
    /// nothing else: another user's token is invisible, the SAME user's
    /// token under a different account is invisible, a revoked token
    /// disappears, and no hash (nor any other column) ever leaves the
    /// handler.
    #[tokio::test]
    async fn list_tokens_shows_only_the_callers_own_live_tokens_pg() {
        let db_name = "addin_pg_test_list_tokens";
        let Some(base) = base_url() else {
            eprintln!(
                "skipping list_tokens_shows_only_the_callers_own_live_tokens_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let _database_cleanup_1 = crate::test_db::TestDatabase::adopt_from_url(&base, db_name);
        recreate(&base, db_name).await;
        let url = with_db(&base, db_name);
        let pool = PgPool::connect(&url).await.expect("connect scratch db");
        sqlx::migrate!("./migrations_control")
            .run(&pool)
            .await
            .expect("migrate control tree");
        let state = Arc::new(crate::test_app_state_owned_with_db(pool.clone(), None));

        let (user_id, account_id, _email) = seed_user_account_and_email(&pool).await;
        let user_ctx = UserContext {
            user_id,
            account_id,
            role: "owner".to_string(),
            device_id: "test-device".to_string(),
        };
        let (other_user, other_account, _) = seed_user_account_and_email(&pool).await;
        let other_ctx = UserContext {
            user_id: other_user,
            account_id: other_account,
            role: "owner".to_string(),
            device_id: "test-device-2".to_string(),
        };

        // A caller with no tokens gets an empty list, not a 404 — the
        // `/connectors` tile renders "not connected" from this, so the
        // empty case has to be a normal 200.
        let resp = list_tokens(
            State(state.clone()),
            personal_tenant(),
            Extension(user_ctx.clone()),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(
            json_body(resp).await["tokens"].as_array().unwrap().len(),
            0,
            "a caller with no add-on tokens must get an empty list, not an error"
        );

        let older = mint_token_via_approve(&state, &pool, &user_ctx).await;
        let newer = mint_token_via_approve(&state, &pool, &user_ctx).await;
        let strangers = mint_token_via_approve(&state, &pool, &other_ctx).await;

        // Two rows minted milliseconds apart can tie on `created_at`, which
        // would make an ordering assertion pass or fail by luck. Backdate
        // one explicitly, so `ORDER BY created_at DESC` is actually being
        // tested. `last_used_at` is set on the SAME row so the null and
        // non-null cases of that field are both covered below.
        sqlx::query(
            "UPDATE addin_tokens SET created_at = now() - INTERVAL '1 hour', \
                    last_used_at = now() - INTERVAL '30 minutes' WHERE id = $1",
        )
        .bind(older)
        .execute(&pool)
        .await
        .unwrap();

        let resp = list_tokens(
            State(state.clone()),
            personal_tenant(),
            Extension(user_ctx.clone()),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK);
        let body = json_body(resp).await;
        let tokens = body["tokens"].as_array().unwrap().clone();
        assert_eq!(tokens.len(), 2, "exactly the caller's own two tokens");
        assert_eq!(
            tokens[0]["id"].as_str().unwrap(),
            newer.to_string(),
            "newest first (ORDER BY created_at DESC)"
        );
        assert_eq!(tokens[1]["id"].as_str().unwrap(), older.to_string());
        assert!(
            !tokens
                .iter()
                .any(|t| t["id"].as_str() == Some(strangers.to_string().as_str())),
            "another user's add-on token must never appear in this list"
        );

        // The fields themselves: the name the pairing row actually carries
        // (not a string this handler invents), an RFC 3339 `created_at`,
        // and a `last_used_at` that is genuinely read from the column —
        // null on the never-used row, a timestamp on the one backdated
        // above.
        assert_eq!(tokens[0]["name"].as_str(), Some("Gmail Add-on"));
        chrono::DateTime::parse_from_rfc3339(tokens[0]["created_at"].as_str().unwrap())
            .expect("created_at must be RFC 3339");
        assert!(
            tokens[0]["last_used_at"].is_null(),
            "a token that has never authenticated a request reports a null last_used_at"
        );
        let used = tokens[1]["last_used_at"]
            .as_str()
            .expect("a used token reports its last_used_at, not null");
        chrono::DateTime::parse_from_rfc3339(used).expect("last_used_at must be RFC 3339");

        // No hash, no plaintext, no extra columns. Both directions: the
        // key set is pinned (so a future `SELECT *` refactor fails here),
        // AND the actual stored hash is searched for in the serialized
        // body (so a rename of the field wouldn't hide a leak).
        let obj = tokens[0].as_object().unwrap();
        assert_eq!(
            obj.len(),
            4,
            "exactly id/name/created_at/last_used_at — a new field in this response is a \
             deliberate decision, never an accident"
        );
        for key in ["id", "name", "created_at", "last_used_at"] {
            assert!(obj.contains_key(key), "missing field {key}");
        }
        let stored_hash: String =
            sqlx::query_scalar("SELECT token_hash FROM addin_tokens WHERE id = $1")
                .bind(newer)
                .fetch_one(&pool)
                .await
                .unwrap();
        let serialized = body.to_string();
        assert!(
            !serialized.contains(&stored_hash),
            "the stored token hash must never appear anywhere in this response"
        );

        // The other user's own list contains ONLY their own token — the
        // scoping is symmetric, not "user A happens to be first".
        let resp = list_tokens(
            State(state.clone()),
            personal_tenant(),
            Extension(other_ctx.clone()),
        )
        .await;
        let body = json_body(resp).await;
        let others = body["tokens"].as_array().unwrap();
        assert_eq!(others.len(), 1);
        assert_eq!(others[0]["id"].as_str().unwrap(), strangers.to_string());

        // Negative control for the `account_id = $2` predicate — the half of
        // the scoping `owner_user_id` alone does not give you.
        //
        // One human can own more than one account: exactly one PERSONAL one
        // (`uq_accounts_one_personal_per_owner`, migration 0028) plus any
        // number of org accounts. This fixture writes an `addin_tokens` row
        // under this user's SECOND (org) account and asserts the personal
        // list still shows only two rows. The row is inserted directly, not
        // minted through `pair_approve` — that handler refuses an
        // org-context caller outright, so nothing in production can mint
        // this row today. That is the point: the `account_id` predicate is
        // defence in depth, and this proves it is real rather than
        // decorative. Delete `AND account_id = $2` from the query and this
        // assertion fails.
        let org_account: Uuid = sqlx::query_scalar(
            "INSERT INTO accounts(owner_user_id, clerk_org_id) VALUES ($1, $2) RETURNING id",
        )
        .bind(user_id)
        .bind(format!("org_addin_list_{}", Uuid::new_v4()))
        .fetch_one(&pool)
        .await
        .unwrap();
        let foreign_account_token: Uuid = sqlx::query_scalar(
            "INSERT INTO addin_tokens (account_id, owner_user_id, token_hash) \
             VALUES ($1, $2, $3) RETURNING id",
        )
        .bind(org_account)
        .bind(user_id)
        .bind(hash_token(&generate_pat()))
        .fetch_one(&pool)
        .await
        .unwrap();

        let resp = list_tokens(
            State(state.clone()),
            personal_tenant(),
            Extension(user_ctx.clone()),
        )
        .await;
        let body = json_body(resp).await;
        let tokens = body["tokens"].as_array().unwrap();
        assert_eq!(
            tokens.len(),
            2,
            "a token owned by this user under a DIFFERENT account must not join this list"
        );
        assert!(
            !body.to_string().contains(&foreign_account_token.to_string()),
            "tokens are scoped to the caller's account as well as their user id"
        );

        // A revoked token leaves the list entirely — revoked via the REAL
        // route the `/connectors` page's Revoke button calls, not a direct
        // UPDATE, so this also pins that the two routes agree on which row
        // is which.
        let resp = revoke_token(
            State(state.clone()),
            Extension(user_ctx.clone()),
            Path(newer),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::NO_CONTENT);

        let resp = list_tokens(
            State(state.clone()),
            personal_tenant(),
            Extension(user_ctx.clone()),
        )
        .await;
        let body = json_body(resp).await;
        let tokens = body["tokens"].as_array().unwrap();
        assert_eq!(tokens.len(), 1, "the revoked token must drop out of the list");
        assert_eq!(tokens[0]["id"].as_str().unwrap(), older.to_string());
        assert!(
            sqlx::query_scalar::<_, Option<DateTime<Utc>>>(
                "SELECT revoked_at FROM addin_tokens WHERE id = $1"
            )
            .bind(newer)
            .fetch_one(&pool)
            .await
            .unwrap()
            .is_some(),
            "the row is still there and marked revoked — the list hides it, the DB keeps it"
        );

        teardown(&base, db_name, pool).await;
    }

    /// An org-context caller is refused with 400, and the refusal is a
    /// refusal — not an empty list dressed up as one. Proven against a
    /// caller who genuinely HAS a token, so "no tokens returned" cannot be
    /// mistaken for "there were none".
    #[tokio::test]
    async fn list_tokens_refuses_an_org_context_caller_pg() {
        let db_name = "addin_pg_test_list_tokens_org";
        let Some(base) = base_url() else {
            eprintln!("skipping list_tokens_refuses_an_org_context_caller_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let _database_cleanup_2 = crate::test_db::TestDatabase::adopt_from_url(&base, db_name);
        recreate(&base, db_name).await;
        let url = with_db(&base, db_name);
        let pool = PgPool::connect(&url).await.expect("connect scratch db");
        sqlx::migrate!("./migrations_control")
            .run(&pool)
            .await
            .expect("migrate control tree");
        let state = Arc::new(crate::test_app_state_owned_with_db(pool.clone(), None));

        let (user_id, account_id, _email) = seed_user_account_and_email(&pool).await;
        let user_ctx = UserContext {
            user_id,
            account_id,
            role: "owner".to_string(),
            device_id: "test-device".to_string(),
        };
        let token_id = mint_token_via_approve(&state, &pool, &user_ctx).await;

        let resp = list_tokens(
            State(state.clone()),
            org_tenant(),
            Extension(user_ctx.clone()),
        )
        .await;
        assert_eq!(
            resp.status(),
            StatusCode::BAD_REQUEST,
            "add-on tokens are personal-plane only — an org-context caller is refused"
        );
        let body = json_body(resp).await;
        assert!(
            body.get("tokens").is_none(),
            "the refusal must not carry a token list at all"
        );
        assert!(
            !body.to_string().contains(&token_id.to_string()),
            "a refused org-context call must not name any of the caller's tokens"
        );

        // The same caller, in their personal workspace, DOES see it — so
        // the 400 above is about the workspace, not about an empty table.
        let resp = list_tokens(
            State(state.clone()),
            personal_tenant(),
            Extension(user_ctx),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK);
        let body = json_body(resp).await;
        assert_eq!(body["tokens"].as_array().unwrap().len(), 1);
        assert_eq!(
            body["tokens"][0]["id"].as_str().unwrap(),
            token_id.to_string()
        );

        teardown(&base, db_name, pool).await;
    }

    /// A genuinely expired (never approved) pairing code must be refused by
    /// BOTH `pair/approve` and `pair/poll`, and the row must not survive an
    /// attempt to use it.
    #[tokio::test]
    async fn expired_pairing_code_is_refused_by_both_approve_and_poll_pg() {
        let db_name = "addin_pg_test_expired";
        let Some(base) = base_url() else {
            eprintln!("skipping expired_pairing_code_is_refused_by_both_approve_and_poll_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let _database_cleanup_3 = crate::test_db::TestDatabase::adopt_from_url(&base, db_name);
        recreate(&base, db_name).await;
        let url = with_db(&base, db_name);
        let pool = PgPool::connect(&url).await.expect("connect scratch db");
        sqlx::migrate!("./migrations_control")
            .run(&pool)
            .await
            .expect("migrate control tree");
        let state = Arc::new(crate::test_app_state_owned_with_db(pool.clone(), None));

        let poll_secret = generate_poll_secret();
        let code = generate_pairing_code();
        // Already expired at insert time.
        sqlx::query(
            "INSERT INTO addin_pairing_codes (code, poll_secret_hash, expires_at) \
             VALUES ($1, $2, now() - INTERVAL '1 minute')",
        )
        .bind(&code)
        .bind(hash_token(&poll_secret))
        .execute(&pool)
        .await
        .unwrap();

        let (user_id, account_id) = seed_user_and_account(&pool).await;
        let user_ctx = UserContext {
            user_id,
            account_id,
            role: "owner".to_string(),
            device_id: "test-device".to_string(),
        };
        let resp = pair_approve(
            State(state.clone()),
            personal_tenant(),
            Extension(user_ctx),
            Json(PairApproveRequest { code: code.clone(), account_id: None }),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);

        let resp = pair_poll(
            State(state.clone()),
            Json(PairPollRequest {
                code: code.clone(),
                poll_secret,
            }),
        )
        .await;
        assert_eq!(json_body(resp).await["status"], "expired");

        teardown(&base, db_name, pool).await;
    }

    /// `pair/poll` throttles repeated guesses against the SAME `code` —
    /// exactly `PAIR_CODE_RATE_MAX_PER_WINDOW` requests succeed (in the sense
    /// of reaching the handler's own logic; a nonexistent code still answers
    /// `{"status":"expired"}`), and the next one is refused with 429 before
    /// ever touching the pairing-codes table again.
    #[tokio::test]
    async fn pair_poll_is_rate_limited_per_code_pg() {
        let db_name = "addin_pg_test_poll_rate_limit";
        let Some(base) = base_url() else {
            eprintln!("skipping pair_poll_is_rate_limited_per_code_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let _database_cleanup_4 = crate::test_db::TestDatabase::adopt_from_url(&base, db_name);
        recreate(&base, db_name).await;
        let url = with_db(&base, db_name);
        let pool = PgPool::connect(&url).await.expect("connect scratch db");
        sqlx::migrate!("./migrations_control")
            .run(&pool)
            .await
            .expect("migrate control tree");
        let state = Arc::new(crate::test_app_state_owned_with_db(pool.clone(), None));

        // A code that was NEVER minted — every call below answers "expired"
        // on its own merits, so the only thing distinguishing request
        // `PAIR_CODE_RATE_MAX_PER_WINDOW + 1` is the rate limiter itself.
        let code = generate_pairing_code();
        let poll_secret = generate_poll_secret();

        for attempt in 1..=PAIR_CODE_RATE_MAX_PER_WINDOW {
            let resp = pair_poll(
                State(state.clone()),
                Json(PairPollRequest {
                    code: code.clone(),
                    poll_secret: poll_secret.clone(),
                }),
            )
            .await;
            assert_eq!(
                resp.status(),
                StatusCode::OK,
                "attempt {attempt} (within the window) must reach the handler's own logic, \
                 not the rate limiter"
            );
        }

        let resp = pair_poll(
            State(state.clone()),
            Json(PairPollRequest {
                code: code.clone(),
                poll_secret: poll_secret.clone(),
            }),
        )
        .await;
        assert_eq!(
            resp.status(),
            StatusCode::TOO_MANY_REQUESTS,
            "the {}th request against the SAME code must be rate-limited",
            PAIR_CODE_RATE_MAX_PER_WINDOW + 1
        );

        // A DIFFERENT code is unaffected — the bucket is per-code, not
        // global, exactly as documented.
        let other_code = generate_pairing_code();
        let resp = pair_poll(
            State(state.clone()),
            Json(PairPollRequest {
                code: other_code,
                poll_secret,
            }),
        )
        .await;
        assert_eq!(
            resp.status(),
            StatusCode::OK,
            "a different code must have its own, unexhausted bucket"
        );

        teardown(&base, db_name, pool).await;
    }

    /// The same property for `pair/approve` — the gap this test was added to
    /// close: before this PR's fix, `pair/approve` had NO rate limiting at
    /// all, despite being the endpoint an authenticated attacker can call
    /// directly (no `poll_secret` needed) to try to win the approve-before-
    /// the-real-owner race against a guessed `code` (see `pair_approve`'s own
    /// doc comment on that race).
    #[tokio::test]
    async fn pair_approve_is_rate_limited_per_code_pg() {
        let db_name = "addin_pg_test_approve_rate_limit";
        let Some(base) = base_url() else {
            eprintln!("skipping pair_approve_is_rate_limited_per_code_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let _database_cleanup_5 = crate::test_db::TestDatabase::adopt_from_url(&base, db_name);
        recreate(&base, db_name).await;
        let url = with_db(&base, db_name);
        let pool = PgPool::connect(&url).await.expect("connect scratch db");
        sqlx::migrate!("./migrations_control")
            .run(&pool)
            .await
            .expect("migrate control tree");
        let state = Arc::new(crate::test_app_state_owned_with_db(pool.clone(), None));

        let (user_id, account_id) = seed_user_and_account(&pool).await;
        let user_ctx = UserContext {
            user_id,
            account_id,
            role: "owner".to_string(),
            device_id: "test-device".to_string(),
        };

        // A code that was NEVER minted — every call below answers 404 on its
        // own merits, so the only thing distinguishing request
        // `PAIR_CODE_RATE_MAX_PER_WINDOW + 1` is the rate limiter itself.
        let code = generate_pairing_code();

        for attempt in 1..=PAIR_CODE_RATE_MAX_PER_WINDOW {
            let resp = pair_approve(
                State(state.clone()),
                personal_tenant(),
                Extension(user_ctx.clone()),
                Json(PairApproveRequest { code: code.clone(), account_id: None }),
            )
            .await;
            assert_eq!(
                resp.status(),
                StatusCode::NOT_FOUND,
                "attempt {attempt} (within the window) must reach the handler's own logic, \
                 not the rate limiter"
            );
        }

        let resp = pair_approve(
            State(state.clone()),
            personal_tenant(),
            Extension(user_ctx.clone()),
            Json(PairApproveRequest { code: code.clone(), account_id: None }),
        )
        .await;
        assert_eq!(
            resp.status(),
            StatusCode::TOO_MANY_REQUESTS,
            "the {}th approve attempt against the SAME code must be rate-limited",
            PAIR_CODE_RATE_MAX_PER_WINDOW + 1
        );

        // A DIFFERENT code is unaffected — same per-code (not global) bucket
        // `pair/poll`'s own test above pins.
        let other_code = generate_pairing_code();
        let resp = pair_approve(
            State(state.clone()),
            personal_tenant(),
            Extension(user_ctx),
            Json(PairApproveRequest { code: other_code, account_id: None }),
        )
        .await;
        assert_eq!(
            resp.status(),
            StatusCode::NOT_FOUND,
            "a different code must have its own, unexhausted bucket"
        );

        teardown(&base, db_name, pool).await;
    }

    /// **Negative-control-verified** (CodeRabbit review on #820): the first
    /// version of the `pair_approve` rate limit keyed the limiter on the RAW
    /// `body.code` while the DB lookup used `.trim()`, so `"CODE1234"` and
    /// `" CODE1234"` landed in DIFFERENT buckets while querying the IDENTICAL
    /// row. Reverting the fix (keying on `body.code` instead of the trimmed
    /// `code`) makes this test fail exactly as predicted: interleaving a
    /// padded and unpadded variant across `PAIR_CODE_RATE_MAX_PER_WINDOW`
    /// total requests never trips 429, because each variant's bucket only
    /// ever sees half the requests. Restoring the fix makes it pass.
    #[tokio::test]
    async fn pair_approve_rate_limit_key_uses_the_trimmed_code_pg() {
        let db_name = "addin_pg_test_approve_trim_key";
        let Some(base) = base_url() else {
            eprintln!("skipping pair_approve_rate_limit_key_uses_the_trimmed_code_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let _database_cleanup_6 = crate::test_db::TestDatabase::adopt_from_url(&base, db_name);
        recreate(&base, db_name).await;
        let url = with_db(&base, db_name);
        let pool = PgPool::connect(&url).await.expect("connect scratch db");
        sqlx::migrate!("./migrations_control")
            .run(&pool)
            .await
            .expect("migrate control tree");
        let state = Arc::new(crate::test_app_state_owned_with_db(pool.clone(), None));

        let (user_id, account_id) = seed_user_and_account(&pool).await;
        let user_ctx = UserContext {
            user_id,
            account_id,
            role: "owner".to_string(),
            device_id: "test-device".to_string(),
        };

        let code = generate_pairing_code();
        let padded = format!("  {code}  ");

        // Alternate padded/unpadded across the whole window. If padding
        // earned its own bucket (the bug), NEITHER bucket would exhaust
        // within this loop (each gets only half of
        // `PAIR_CODE_RATE_MAX_PER_WINDOW`), and the assertion just below
        // would fail.
        for attempt in 1..=PAIR_CODE_RATE_MAX_PER_WINDOW {
            let this_code = if attempt % 2 == 0 { padded.clone() } else { code.clone() };
            let resp = pair_approve(
                State(state.clone()),
                personal_tenant(),
                Extension(user_ctx.clone()),
                Json(PairApproveRequest { code: this_code, account_id: None }),
            )
            .await;
            assert_eq!(
                resp.status(),
                StatusCode::NOT_FOUND,
                "attempt {attempt} (within the window) must reach the handler's own logic"
            );
        }

        // The (max+1)th request, using the PADDED variant, must ALSO be
        // throttled — proving padded and unpadded share exactly one bucket.
        let resp = pair_approve(
            State(state.clone()),
            personal_tenant(),
            Extension(user_ctx),
            Json(PairApproveRequest { code: padded, account_id: None }),
        )
        .await;
        assert_eq!(
            resp.status(),
            StatusCode::TOO_MANY_REQUESTS,
            "a whitespace-padded code must share its bucket with the trimmed code, not evade \
             the rate limit by varying padding"
        );

        teardown(&base, db_name, pool).await;
    }

    /// **Negative-control-verified** (CodeRabbit review on #820): before this
    /// fix, `pair_start` called neither `check_and_increment_key` nor
    /// anything else rate-limiting it — an unauthenticated caller could
    /// script unlimited calls, each writing a fresh row. Reverting the fix
    /// (removing the rate-limit check from `pair_start`) makes attempt
    /// `PAIR_START_RATE_MAX_PER_WINDOW + 1` return 200 instead of 429, and
    /// makes the row-count assertion below fail (a new row IS written).
    /// Restoring the fix makes both pass.
    #[tokio::test]
    async fn pair_start_is_rate_limited_by_ip_pg() {
        let db_name = "addin_pg_test_start_rate_limit";
        let Some(base) = base_url() else {
            eprintln!("skipping pair_start_is_rate_limited_by_ip_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let _database_cleanup_7 = crate::test_db::TestDatabase::adopt_from_url(&base, db_name);
        recreate(&base, db_name).await;
        let url = with_db(&base, db_name);
        let pool = PgPool::connect(&url).await.expect("connect scratch db");
        sqlx::migrate!("./migrations_control")
            .run(&pool)
            .await
            .expect("migrate control tree");
        let state = Arc::new(crate::test_app_state_owned_with_db(pool.clone(), None));

        let headers = fly_client_ip_headers("203.0.113.5");

        for attempt in 1..=PAIR_START_RATE_MAX_PER_WINDOW {
            let resp = pair_start(State(state.clone()), headers.clone()).await;
            assert_eq!(
                resp.status(),
                StatusCode::OK,
                "attempt {attempt} (within the window) must succeed"
            );
        }

        let rows_before: i64 = sqlx::query_scalar("SELECT count(*) FROM addin_pairing_codes")
            .fetch_one(&pool)
            .await
            .unwrap();

        let resp = pair_start(State(state.clone()), headers.clone()).await;
        assert_eq!(
            resp.status(),
            StatusCode::TOO_MANY_REQUESTS,
            "the {}th pair/start call from the SAME IP must be rate-limited",
            PAIR_START_RATE_MAX_PER_WINDOW + 1
        );

        let rows_after: i64 = sqlx::query_scalar("SELECT count(*) FROM addin_pairing_codes")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(
            rows_before, rows_after,
            "a throttled pair/start must never reach the INSERT — no new row"
        );

        // A DIFFERENT IP has its own, unexhausted bucket.
        let other_headers = fly_client_ip_headers("198.51.100.9");
        let resp = pair_start(State(state.clone()), other_headers).await;
        assert_eq!(
            resp.status(),
            StatusCode::OK,
            "a different caller IP must have its own bucket"
        );

        teardown(&base, db_name, pool).await;
    }

    fn fly_client_ip_headers(ip: &str) -> axum::http::HeaderMap {
        let mut h = axum::http::HeaderMap::new();
        h.insert("Fly-Client-IP", ip.parse().expect("valid header value"));
        h
    }

    /// Major 3's proof, path 1: a pairing that WAS approved (so it minted a
    /// real `addin_tokens` row) but is polled only after it has expired must
    /// have that token REVOKED, not left orphaned. Before this fix, the
    /// expiry branch deleted the pairing row with no idea a token was ever
    /// linked to it, leaving `revoked_at IS NULL` forever.
    #[tokio::test]
    async fn pair_poll_expiry_revokes_the_orphaned_token_pg() {
        let db_name = "addin_pg_test_poll_expiry_revokes";
        let Some(base) = base_url() else {
            eprintln!("skipping pair_poll_expiry_revokes_the_orphaned_token_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let _database_cleanup_8 = crate::test_db::TestDatabase::adopt_from_url(&base, db_name);
        recreate(&base, db_name).await;
        let url = with_db(&base, db_name);
        let pool = PgPool::connect(&url).await.expect("connect scratch db");
        sqlx::migrate!("./migrations_control")
            .run(&pool)
            .await
            .expect("migrate control tree");
        let state = Arc::new(crate::test_app_state_owned_with_db(pool.clone(), None));

        let (user_id, account_id) = seed_user_and_account(&pool).await;
        let user_ctx = UserContext {
            user_id,
            account_id,
            role: "owner".to_string(),
            device_id: "test-device".to_string(),
        };

        let poll_secret = generate_poll_secret();
        let code = generate_pairing_code();
        sqlx::query(
            "INSERT INTO addin_pairing_codes (code, poll_secret_hash, expires_at) \
             VALUES ($1, $2, $3)",
        )
        .bind(&code)
        .bind(hash_token(&poll_secret))
        .bind(Utc::now() + chrono::Duration::minutes(10))
        .execute(&pool)
        .await
        .unwrap();

        // Approve it — this mints a real, ACTIVE `addin_tokens` row.
        let resp = pair_approve(
            State(state.clone()),
            personal_tenant(),
            Extension(user_ctx),
            Json(PairApproveRequest { code: code.clone(), account_id: None }),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK);

        let minted_token_id: Uuid = sqlx::query_scalar("SELECT id FROM addin_tokens")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert!(
            sqlx::query_scalar::<_, Option<DateTime<Utc>>>(
                "SELECT revoked_at FROM addin_tokens WHERE id = $1"
            )
            .bind(minted_token_id)
            .fetch_one(&pool)
            .await
            .unwrap()
            .is_none(),
            "the freshly minted token must start out active"
        );

        // Simulate elapsed time WITHOUT ever polling: the pairing row is
        // still there (approved, token minted), but now past its expiry.
        sqlx::query("UPDATE addin_pairing_codes SET expires_at = now() - INTERVAL '1 minute' WHERE code = $1")
            .bind(&code)
            .execute(&pool)
            .await
            .unwrap();

        // The first (and only) poll anyone ever makes — too late.
        let resp = pair_poll(
            State(state.clone()),
            Json(PairPollRequest {
                code: code.clone(),
                poll_secret,
            }),
        )
        .await;
        assert_eq!(json_body(resp).await["status"], "expired");

        // The pairing row is gone...
        let row_gone: Option<(String,)> =
            sqlx::query_as("SELECT code FROM addin_pairing_codes WHERE code = $1")
                .bind(&code)
                .fetch_optional(&pool)
                .await
                .unwrap();
        assert!(row_gone.is_none());

        // ...and the token it minted is REVOKED, not orphaned active.
        let revoked_at: Option<DateTime<Utc>> = sqlx::query_scalar(
            "SELECT revoked_at FROM addin_tokens WHERE id = $1",
        )
        .bind(minted_token_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert!(
            revoked_at.is_some(),
            "a pairing that expired between approve and poll must revoke its minted token, \
             not leave it active with nothing left to name it"
        );

        teardown(&base, db_name, pool).await;
    }

    /// Major 3's proof, path 2: `pair_start`'s opportunistic day-later sweep
    /// must revoke a swept row's linked token too, not just delete the
    /// pairing row. Sets a row's `expires_at` far enough in the past to hit
    /// the SWEEP threshold (>1 day, not just >10 minutes) without ever
    /// calling `pair_poll` at all — the sweep is the ONLY thing that ever
    /// touches this row again.
    #[tokio::test]
    async fn pair_start_sweep_revokes_orphaned_tokens_pg() {
        let db_name = "addin_pg_test_sweep_revokes";
        let Some(base) = base_url() else {
            eprintln!("skipping pair_start_sweep_revokes_orphaned_tokens_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let _database_cleanup_9 = crate::test_db::TestDatabase::adopt_from_url(&base, db_name);
        recreate(&base, db_name).await;
        let url = with_db(&base, db_name);
        let pool = PgPool::connect(&url).await.expect("connect scratch db");
        sqlx::migrate!("./migrations_control")
            .run(&pool)
            .await
            .expect("migrate control tree");
        let state = Arc::new(crate::test_app_state_owned_with_db(pool.clone(), None));

        let (user_id, account_id) = seed_user_and_account(&pool).await;
        let user_ctx = UserContext {
            user_id,
            account_id,
            role: "owner".to_string(),
            device_id: "test-device".to_string(),
        };

        let poll_secret = generate_poll_secret();
        let code = generate_pairing_code();
        sqlx::query(
            "INSERT INTO addin_pairing_codes (code, poll_secret_hash, expires_at) \
             VALUES ($1, $2, $3)",
        )
        .bind(&code)
        .bind(hash_token(&poll_secret))
        .bind(Utc::now() + chrono::Duration::minutes(10))
        .execute(&pool)
        .await
        .unwrap();

        let resp = pair_approve(
            State(state.clone()),
            personal_tenant(),
            Extension(user_ctx),
            Json(PairApproveRequest { code: code.clone(), account_id: None }),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK);

        let minted_token_id: Uuid = sqlx::query_scalar("SELECT id FROM addin_tokens")
            .fetch_one(&pool)
            .await
            .unwrap();

        // Push it well past the sweep's 1-day threshold — this row was
        // approved, minted a token, and then NOBODY ever polled it.
        sqlx::query(
            "UPDATE addin_pairing_codes SET expires_at = now() - INTERVAL '2 days' WHERE code = $1",
        )
        .bind(&code)
        .execute(&pool)
        .await
        .unwrap();

        // Any `pair/start` call runs the opportunistic sweep as a side
        // effect — use a fresh, unrelated IP so this doesn't collide with
        // the rate-limit test's bucket.
        let resp = pair_start(State(state.clone()), fly_client_ip_headers("192.0.2.77")).await;
        assert_eq!(resp.status(), StatusCode::OK);

        let row_gone: Option<(String,)> =
            sqlx::query_as("SELECT code FROM addin_pairing_codes WHERE code = $1")
                .bind(&code)
                .fetch_optional(&pool)
                .await
                .unwrap();
        assert!(row_gone.is_none(), "the swept pairing row must be gone");

        let revoked_at: Option<DateTime<Utc>> = sqlx::query_scalar(
            "SELECT revoked_at FROM addin_tokens WHERE id = $1",
        )
        .bind(minted_token_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert!(
            revoked_at.is_some(),
            "the opportunistic sweep must revoke a swept row's linked token, not just delete \
             the pairing row"
        );

        teardown(&base, db_name, pool).await;
    }
}
