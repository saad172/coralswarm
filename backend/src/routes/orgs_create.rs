//! Self-serve team org creation (signup PR A2; re-derives #1400 against the
//! founder's 2026-10-27 decisions: card up front, no per-org subdomain, no
//! credits before the card).
//!
//! | route | what |
//! |---|---|
//! | `POST /v1/orgs` | create the Clerk org, its `accounts` row and the creator's admin membership, then the trial Checkout (#1521) → `201 {org_id, status, checkout_url, ...}` |
//! | `GET  /v1/orgs/{org_id}/provisioning` | what the sign-up screen polls: `awaiting_payment` → `provisioning` → `ready` (or `payment_abandoned` / `canceled` / `failed`) |
//! | `POST /v1/orgs/{org_id}/provisioning/retry` | the owner's retry of a `failed` job; per-org authorization and rate limit |
//!
//! All three are 404 while `ORG_SELFSERVE_SIGNUP_ENABLED` is unset, and all
//! three need an interactive browser session (#1473): a PAT, an add-in
//! session or an MCP/OAuth client is refused with `403
//! interactive_session_required`, because each can end up costing a Neon
//! project.
//!
//! # Which session calls these
//!
//! The creator's PERSONAL session, the same as #1521's "resume payment"
//! route. The new org has no routing row until it is paid and provisioned,
//! so the binding gate refuses a token that names it (403 before a routing
//! row exists; `503 {"status":"provisioning"}` while the row is being built).
//! The web switches to the new org (`setActive`) only once the status here is
//! `ready`. None of these routes skips the gate.
//!
//! # What `POST /v1/orgs` does NOT do
//!
//! * grant a plan: no `free` grant, no credits (the trial's `team` grant
//!   arrives with the completed Checkout, #1521);
//! * enqueue provisioning: the Checkout webhook does that (#1521);
//! * pick a subdomain: every org is served from app.coralswarm.example. Clerk's
//!   slug is a hidden server-picked key, `cs-<intent id>`, and nothing routes
//!   on it.
//!
//! # Idempotency, and no connection held across a network call
//!
//! The `Idempotency-Key` header is required. The intent row
//! (`org_selfserve_creations`, control 0384) is written BEFORE Clerk is
//! asked. A request takes the row's LEASE in one short transaction (security
//! review S1), then makes its Clerk and Stripe calls holding no database
//! connection, and records each result with a write guarded by its lease
//! token. A second request for the same row while the lease is live gets
//! `409 create_in_progress` (with `retry_after_s`); once the first finishes,
//! a repeat returns the same org and the same Checkout. An attempt that died
//! after Clerk created the org is recognised by the `private_metadata` marker
//! carrying the intent id and adopted, never re-created. The steps after
//! Clerk (account row, admin membership, Checkout) are each idempotent and
//! re-run on a retry, so a crash anywhere converges.
//!
//! A repeat is refused once the cleanup sweep has touched the intent
//! (`410 checkout_expired` while it is being or has been deleted, `409
//! signup_complete` once it is paid), and once the intent is more than 24
//! hours old (`410 checkout_expired`): the sweep and a replay never act on
//! one org at once (S5). The sweep only claims an intent whose lease is free,
//! and a request only leases an intent the sweep has not claimed.
//!
//! # Caps (each org can eventually cost a Neon project)
//!
//! Counted from the persisted intent rows, under two transaction-scoped
//! advisory locks (the creator's, then the cap bucket's; #1540 N6) held only
//! for the count-and-insert transaction (never across a network call) and
//! bounded by a lock timeout, so a restart, a second machine or two parallel
//! requests cannot slip past them, and a burst on one bucket cannot queue
//! every other sign-up's pool connections behind it:
//! * at most `ORG_SELFSERVE_MAX_PENDING_PER_USER` (default 2) unpaid orgs per
//!   creator at once;
//! * at most `ORG_SELFSERVE_MAX_PER_USER_PER_DAY` (default 5) per creator per
//!   rolling 24 hours, and the same per free-mail MAILBOX (#1540 SF6: `+`
//!   aliases and Gmail dot variants are one person);
//! * at most `ORG_SELFSERVE_MAX_PER_DOMAIN_PER_HOUR` (default 10) per
//!   REGISTRABLE email domain (eTLD+1, so `x.evil.com` is `evil.com`) per
//!   rolling hour;
//! * at most `ORG_SELFSERVE_MAX_FREEMAIL_PER_HOUR` (default 200) across all
//!   free mail per rolling hour, a backstop well above organic sign-ups;
//! * `ORG_SELFSERVE_MAX_PER_HOUR` (default 30) across everyone per rolling
//!   hour is a SOFT threshold (S8): past it the route alerts and throttles
//!   (one pending org per creator, half the domain and free-mail caps)
//!   instead of refusing everyone, so a flood from one bucket cannot block
//!   every other sign-up.
//!
//! A retry is limited per org: one per `ORG_SELFSERVE_RETRY_MIN_INTERVAL_SECS`
//! (default 600) and `ORG_SELFSERVE_MAX_RETRIES` (default 3) in total, counted
//! in the same transaction as the retry itself.
//!
//! # Clerk's member cap
//!
//! The Clerk org is created with `max_allowed_memberships` = the seats being
//! bought, and [`sync_clerk_membership_cap`] keeps it at the purchased seat
//! count (or the current member count, if higher) whenever seats are bought
//! or reduced, so a Clerk-native invite cannot add members past what was
//! paid for (S4). Only orgs with an `org_selfserve_creations` row are touched.

use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::{Extension, Json};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sqlx::PgPool;
use uuid::Uuid;

use crate::billing::org_trial::{self, TrialState};
use crate::clerk_admin::{ClerkAdminClient, ClerkAdminError};
use crate::middleware::auth::UserContext;
use crate::tenancy::provisioning::{self, ClerkOrgCleanup, Job, JobStatus, SELFSERVE_ORG_MARKER};
use crate::AppState;

/// `private_metadata` key carrying the `org_selfserve_creations.id` the org
/// was created for: how a retry recognises an org whose creation it never
/// got to record.
pub const SELFSERVE_INTENT_MARKER: &str = "coralswarm_selfserve_intent";
/// `private_metadata` key carrying the creator's Clerk user id.
pub const SELFSERVE_CREATOR_MARKER: &str = "coralswarm_selfserve_creator";

pub const DEFAULT_MAX_PENDING_PER_USER: i64 = 2;
pub const DEFAULT_MAX_PER_USER_PER_DAY: i64 = 5;
pub const DEFAULT_MAX_PER_HOUR: i64 = 30;
pub const DEFAULT_MAX_PER_DOMAIN_PER_HOUR: i64 = 10;
pub const DEFAULT_MAX_FREEMAIL_PER_HOUR: i64 = 200;
pub const DEFAULT_RETRY_MIN_INTERVAL_SECS: i64 = 600;
pub const DEFAULT_MAX_RETRIES: i32 = 3;

/// How long a request's lease on its intent row lasts (S1). Longer than the
/// Clerk create plus the Stripe Checkout with their deadlines; a request
/// that crashed holds the row this long at most.
pub const LEASE_SECS: i64 = 120;
/// A repeat `POST /v1/orgs` for an intent older than this is refused (S5):
/// by then its Checkout is abandoned and the sweep owns the org.
pub const INTENT_REPLAY_HOURS: i64 = 24;
/// The per-domain cap's shared bucket for free-mail domains (S8).
pub const FREEMAIL_BUCKET: &str = "*freemail";
/// Free-mail domains: one person can hold any number of addresses at these,
/// so they share one bucket instead of each counting as its own domain.
pub const FREEMAIL_DOMAINS: &[&str] = &[
    "gmail.com", "googlemail.com", "outlook.com", "hotmail.com", "live.com", "msn.com", "yahoo.com",
    "ymail.com", "icloud.com", "me.com", "mac.com", "aol.com", "proton.me", "protonmail.com", "pm.me",
    "gmx.com", "gmx.net", "gmx.de", "mail.com", "yandex.com", "yandex.ru", "zoho.com", "zohomail.com",
    "tutanota.com", "tuta.io", "fastmail.com", "hey.com", "qq.com", "163.com", "126.com", "mail.ru",
    "web.de", "duck.com",
    // Regional variants (#1540 review). The registrable domain is what is
    // matched, so `yahoo.co.uk` must be listed itself: it is not `yahoo.com`.
    "yahoo.co.uk", "yahoo.fr", "yahoo.de", "yahoo.es", "yahoo.it", "yahoo.ca", "yahoo.com.au", "yahoo.co.in",
    "yahoo.co.jp", "yahoo.com.br", "hotmail.co.uk", "hotmail.fr", "hotmail.de", "hotmail.es", "hotmail.it",
    "outlook.fr", "outlook.de", "outlook.es", "outlook.it", "outlook.co.uk", "live.co.uk", "live.fr", "live.de",
    "live.ca", "live.com.au", "msn.co.uk", "gmx.at", "gmx.ch", "gmx.fr", "mail.de", "yandex.ua", "rambler.ru",
    "laposte.net", "orange.fr", "free.fr", "libero.it", "t-online.de", "btinternet.com",
];

const NAME_MIN_CHARS: usize = 2;
const NAME_MAX_CHARS: usize = 120;
/// `bin/create_personal_orgs`'s name suffix: a team org may never look like
/// a personal org in Clerk's picker.
const PERSONAL_ORG_NAME_SUFFIX: &str = "'s organization";

// ── Responses ─────────────────────────────────────────────────────────────

fn refuse(status: StatusCode, code: &str, message: &str) -> Response {
    (status, Json(json!({"error": message, "code": code}))).into_response()
}

fn not_found() -> Response {
    (StatusCode::NOT_FOUND, Json(json!({"error": "not found"}))).into_response()
}

fn org_not_found() -> Response {
    refuse(StatusCode::NOT_FOUND, "org_not_found", "organization not found")
}

fn unavailable() -> Response {
    (StatusCode::SERVICE_UNAVAILABLE, Json(json!({"error": "service unavailable"}))).into_response()
}

fn identity_unavailable(step: &str, e: impl std::fmt::Display) -> Response {
    tracing::warn!(step, error = %e, "orgs_create: Clerk unavailable");
    refuse(StatusCode::SERVICE_UNAVAILABLE, "identity_unavailable", "sign-in service unavailable; try again")
}

fn internal(step: &str, e: impl std::fmt::Display) -> Response {
    tracing::error!(step, error = %e, "orgs_create: failed");
    (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": "internal error"}))).into_response()
}

fn rate_limited(code: &str, message: &str, retry_after_s: i64) -> Response {
    (
        StatusCode::TOO_MANY_REQUESTS,
        [("retry-after", retry_after_s.to_string())],
        Json(json!({"error": message, "code": code, "retry_after_s": retry_after_s})),
    )
        .into_response()
}

/// `409 create_in_progress`: another request holds this sign-up's lease.
fn in_progress(retry_after_s: i64) -> Response {
    let retry_after_s = retry_after_s.clamp(1, LEASE_SECS);
    (
        StatusCode::CONFLICT,
        [("retry-after", retry_after_s.to_string())],
        Json(json!({
            "error": "this sign-up is already being set up; try again in a moment",
            "code": "create_in_progress",
            "retry_after_s": retry_after_s,
        })),
    )
        .into_response()
}

fn checkout_expired() -> Response {
    refuse(StatusCode::GONE, "checkout_expired", "this sign-up was not paid within 24 hours; create the organization again")
}

// ── Validation ────────────────────────────────────────────────────────────

/// Unicode general category `Cf` (format characters: zero-width joiners,
/// bidi overrides, the BOM, tag characters, ...). Invisible in Clerk's org
/// picker, so a name carrying one can impersonate another org's name (N6).
/// The full `Cf` list of Unicode 15.1.
pub fn is_format_char(c: char) -> bool {
    matches!(c as u32,
        0x00AD | 0x0600..=0x0605 | 0x061C | 0x06DD | 0x070F | 0x0890..=0x0891 | 0x08E2 | 0x180E
        | 0x200B..=0x200F | 0x202A..=0x202E | 0x2060..=0x2064 | 0x2066..=0x206F | 0xFEFF
        | 0xFFF9..=0xFFFB | 0x110BD | 0x110CD | 0x13430..=0x1343F | 0x1BCA0..=0x1BCA3
        | 0x1D173..=0x1D17A | 0xE0001 | 0xE0020..=0xE007F)
}

/// NFKC-normalise, trim and check the org name (N6). `None` ⇒ `400
/// invalid_name`. NFKC folds compatibility look-alikes (full-width letters,
/// ligatures) into their plain forms, so the stored name is the one Clerk
/// shows; control and format (`Cf`) characters are refused outright.
pub fn validate_name(raw: &str) -> Option<String> {
    use unicode_normalization::UnicodeNormalization;
    let normalized: String = raw.nfkc().collect();
    let name = normalized.trim();
    let chars = name.chars().count();
    if !(NAME_MIN_CHARS..=NAME_MAX_CHARS).contains(&chars) {
        return None;
    }
    if name.chars().any(|c| c.is_control() || is_format_char(c)) || !name.chars().any(char::is_alphanumeric) {
        return None;
    }
    if name.to_lowercase().ends_with(PERSONAL_ORG_NAME_SUFFIX) {
        return None;
    }
    Some(name.to_string())
}

/// The `Idempotency-Key` header: 8..=200 characters of `[A-Za-z0-9_.:-]`.
pub fn validate_idempotency_key(raw: Option<&str>) -> Option<String> {
    let k = raw?.trim();
    let ok = (8..=200).contains(&k.len())
        && k.chars().all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.' | ':'));
    ok.then(|| k.to_string())
}

/// Clerk's slug for the org: opaque and never shown. Derived from the intent
/// id, so it is unique without a lookup and stable across retries of one
/// intent. Deliberately not the `o-<16 hex>` shape `tenancy::routing`
/// reserves for routing keys.
pub fn clerk_slug_for_intent(intent: Uuid) -> String {
    format!("cs-{}", intent.simple())
}

/// The registrable domain (eTLD+1, from the Public Suffix List) of a
/// lowercased host: `x.evil.com` and `y.evil.com` are both `evil.com`, and
/// `a.acme.co.uk` is `acme.co.uk` (#1540 SF6). The host itself when the list
/// has no answer (a bare suffix).
pub fn registrable_domain(host: &str) -> String {
    psl::domain_str(host).unwrap_or(host).to_string()
}

/// The mailbox a free-mail address really delivers to (#1540 SF6): the local
/// part without its `+tag`, and for Gmail also without dots
/// (`J.Doe+x@googlemail.com` is `person@example.com`).
pub fn canonical_freemail_address(local: &str, registrable: &str) -> String {
    let local = local.split('+').next().unwrap_or("").to_ascii_lowercase();
    let (local, domain) = match registrable {
        "gmail.com" | "googlemail.com" => (local.replace('.', ""), "gmail.com"),
        d => (local, d),
    };
    format!("{local}@{domain}")
}

/// The cap bucket of a verified email address (S8, #1540 SF6). `None` for an
/// address with no usable domain.
///
/// * A company address: its REGISTRABLE domain, lowercased, so subdomains do
///   not each get a fresh bucket.
/// * A free-mail address: [`FREEMAIL_BUCKET`]`:` plus a keyed digest of the
///   canonical mailbox ([`canonical_freemail_address`]). One person's `+`
///   aliases and Gmail dot variants (each its own Clerk account, each
///   verified, all landing in one inbox) share one bucket, while two
///   different free-mail users never block each other. Every free-mail
///   bucket also counts toward one aggregate cap
///   ([`Caps::max_freemail_per_hour`]).
///
/// The digest is HMAC-SHA256 under [`cap_bucket_key`] (a key derived from
/// `MASTER_KEK_HEX`), not a bare SHA-256 (#1540 review SF-d): a mailbox is
/// low-entropy, so anyone reading the table could confirm a guessed address
/// against a plain hash offline. The row is still pseudonymous data about a
/// person (the server can recompute it for a known address), which is why
/// the cleanup sweep clears `cap_bucket` once the caps' longest window (24 h)
/// has passed.
pub fn cap_bucket_for(email: &str) -> Option<String> {
    cap_bucket_with_key(email, cap_bucket_key().as_ref())
}

/// The HMAC key for free-mail cap buckets: HKDF-SHA256 of the master KEK
/// under [`crate::crypto::kdf::purpose::SELFSERVE_CAP_BUCKET`] (info
/// `selfserve-cap-bucket:v1`), never the KEK itself. `None` when
/// `MASTER_KEK_HEX` is unset (bare dev environments); the bucket is then an
/// unkeyed digest marked `u:` so the weaker form is visible in the data.
/// Rotating the KEK changes every bucket, which only resets the rolling caps
/// once (no bucket is kept past 24 h).
///
/// `MASTER_KEK_HEX` SET but malformed is a misconfiguration, not a dev
/// environment: it still falls back to the unkeyed form (a signup must not
/// fail on it), but logs at error with `alert = true`. The log never carries
/// the key or the decode error (`hex::decode`'s error names the offending
/// character).
pub fn cap_bucket_key() -> Option<[u8; crate::crypto::kdf::DERIVED_KEY_LEN]> {
    static KEY: crate::crypto::kdf::DerivedKey =
        crate::crypto::kdf::DerivedKey::new(crate::crypto::kdf::purpose::SELFSERVE_CAP_BUCKET);
    KEY.get()
}

/// [`cap_bucket_for`] with an explicit key, so both key states are testable
/// without touching the process environment.
pub fn cap_bucket_with_key(email: &str, key: Option<&[u8; crate::crypto::kdf::DERIVED_KEY_LEN]>) -> Option<String> {
    let (local, domain) = email.trim().rsplit_once('@')?;
    let domain = domain.trim().trim_end_matches('.').to_ascii_lowercase();
    if domain.is_empty() || domain.len() > 255 {
        return None;
    }
    let registrable = registrable_domain(&domain);
    if FREEMAIL_DOMAINS.contains(&registrable.as_str()) {
        let canonical = canonical_freemail_address(local, &registrable);
        return Some(match key {
            Some(key) => {
                use hmac::{Hmac, Mac};
                let mut mac = Hmac::<sha2::Sha256>::new_from_slice(key).expect("HMAC-SHA256 accepts any key length");
                mac.update(canonical.as_bytes());
                format!("{FREEMAIL_BUCKET}:{}", hex::encode(&mac.finalize().into_bytes()[..16]))
            }
            None => {
                use sha2::{Digest, Sha256};
                let digest = Sha256::digest(canonical.as_bytes());
                format!("{FREEMAIL_BUCKET}:u:{}", hex::encode(&digest[..16]))
            }
        });
    }
    Some(registrable)
}

/// Whether a bucket from [`cap_bucket_for`] is a free-mail identity.
pub fn is_freemail_bucket(bucket: Option<&str>) -> bool {
    bucket.is_some_and(|b| b == FREEMAIL_BUCKET || b.starts_with(&format!("{FREEMAIL_BUCKET}:")))
}

// ── Caps ──────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Caps {
    pub max_pending_per_user: i64,
    pub max_per_user_per_day: i64,
    /// Soft: past it the route alerts and throttles, never refuses alone.
    pub max_per_hour: i64,
    pub max_per_domain_per_hour: i64,
    pub max_freemail_per_hour: i64,
}

fn env_i64(key: &str, default: i64) -> i64 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.trim().parse::<i64>().ok())
        .filter(|n| *n >= 0)
        .unwrap_or(default)
}

impl Caps {
    pub fn from_env() -> Self {
        Caps {
            max_pending_per_user: env_i64("ORG_SELFSERVE_MAX_PENDING_PER_USER", DEFAULT_MAX_PENDING_PER_USER),
            max_per_user_per_day: env_i64("ORG_SELFSERVE_MAX_PER_USER_PER_DAY", DEFAULT_MAX_PER_USER_PER_DAY),
            max_per_hour: env_i64("ORG_SELFSERVE_MAX_PER_HOUR", DEFAULT_MAX_PER_HOUR),
            max_per_domain_per_hour: env_i64("ORG_SELFSERVE_MAX_PER_DOMAIN_PER_HOUR", DEFAULT_MAX_PER_DOMAIN_PER_HOUR),
            max_freemail_per_hour: env_i64("ORG_SELFSERVE_MAX_FREEMAIL_PER_HOUR", DEFAULT_MAX_FREEMAIL_PER_HOUR),
        }
    }
}

/// What the intent rows say about one creator, their domain bucket and
/// everyone, right now.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, sqlx::FromRow)]
pub struct CreationCounts {
    /// Created by this user, not refused, not yet paid (no provisioning
    /// job), and not settled by the sweep.
    pub pending_for_user: i64,
    pub user_last_day: i64,
    pub user_oldest_in_day: Option<DateTime<Utc>>,
    pub bucket_last_hour: i64,
    pub bucket_oldest_in_hour: Option<DateTime<Utc>>,
    /// The bucket over the rolling day: for a free-mail identity this is
    /// the person's daily cap across their aliases (#1540 SF6).
    pub bucket_last_day: i64,
    pub bucket_oldest_in_day: Option<DateTime<Utc>>,
    /// Every free-mail bucket together, over the rolling hour.
    pub freemail_last_hour: i64,
    pub freemail_oldest_in_hour: Option<DateTime<Utc>>,
    pub global_last_hour: i64,
    pub global_oldest_in_hour: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CapDecision {
    /// `throttled`: the global soft threshold is passed, so the tighter
    /// limits applied (and the caller alerts).
    Allow { throttled: bool },
    Refuse { code: &'static str, retry_after_s: i64, throttled: bool },
}

fn retry_after(oldest: Option<DateTime<Utc>>, window: chrono::Duration, now: DateTime<Utc>) -> i64 {
    match oldest {
        Some(t) => (t + window - now).num_seconds().clamp(1, window.num_seconds()),
        None => window.num_seconds(),
    }
}

/// The cap decision (S8, #1540 SF6). The global count never refuses on its
/// own: past `max_per_hour` it only throttles (one pending org per creator,
/// half the bucket caps, at least 1) and the caller alerts. The per-creator
/// and per-bucket caps refuse:
/// * a company domain: `max_per_domain_per_hour` per registrable domain;
/// * a free-mail identity: `max_per_user_per_day` per canonical mailbox (one
///   person's aliases count as that person), and `max_freemail_per_hour`
///   across all free mail together (a raised backstop, not the everyday
///   limit).
pub fn decide_caps(c: &CreationCounts, caps: &Caps, freemail: bool, now: DateTime<Utc>) -> CapDecision {
    let throttled = c.global_last_hour >= caps.max_per_hour;
    let halve = |cap: i64| if throttled { (cap / 2).max(1).min(cap) } else { cap };
    let max_pending = if throttled { caps.max_pending_per_user.min(1) } else { caps.max_pending_per_user };
    let refuse = |oldest: Option<DateTime<Utc>>, window: chrono::Duration| CapDecision::Refuse {
        code: "org_create_rate_limited",
        retry_after_s: retry_after(oldest, window, now),
        throttled,
    };
    if c.pending_for_user >= max_pending {
        // Clears when one is paid, or swept about a day after it was made.
        return CapDecision::Refuse { code: "too_many_pending_orgs", retry_after_s: 3600, throttled };
    }
    if c.user_last_day >= caps.max_per_user_per_day {
        return refuse(c.user_oldest_in_day, chrono::Duration::hours(24));
    }
    if freemail {
        if c.bucket_last_day >= caps.max_per_user_per_day {
            return refuse(c.bucket_oldest_in_day, chrono::Duration::hours(24));
        }
        if c.freemail_last_hour >= halve(caps.max_freemail_per_hour) {
            return refuse(c.freemail_oldest_in_hour, chrono::Duration::hours(1));
        }
    } else if c.bucket_last_hour >= halve(caps.max_per_domain_per_hour) {
        return refuse(c.bucket_oldest_in_hour, chrono::Duration::hours(1));
    }
    CapDecision::Allow { throttled }
}

async fn creation_counts(
    conn: &mut sqlx::PgConnection,
    user: Uuid,
    bucket: Option<&str>,
) -> Result<CreationCounts, sqlx::Error> {
    sqlx::query_as(
        "SELECT \
           count(*) FILTER (WHERE c.user_id = $1 AND c.sweep_state IN ('none', 'deleting') \
                              AND NOT EXISTS (SELECT 1 FROM tenant_provisioning_jobs j \
                                               WHERE j.clerk_org_id = c.clerk_org_id)) AS pending_for_user, \
           count(*) FILTER (WHERE c.user_id = $1 AND c.created_at > now() - interval '24 hours') AS user_last_day, \
           min(c.created_at) FILTER (WHERE c.user_id = $1 AND c.created_at > now() - interval '24 hours') \
             AS user_oldest_in_day, \
           count(*) FILTER (WHERE c.cap_bucket = $2 AND c.created_at > now() - interval '1 hour') AS bucket_last_hour, \
           min(c.created_at) FILTER (WHERE c.cap_bucket = $2 AND c.created_at > now() - interval '1 hour') \
             AS bucket_oldest_in_hour, \
           count(*) FILTER (WHERE c.cap_bucket = $2 AND c.created_at > now() - interval '24 hours') AS bucket_last_day, \
           min(c.created_at) FILTER (WHERE c.cap_bucket = $2 AND c.created_at > now() - interval '24 hours') \
             AS bucket_oldest_in_day, \
           count(*) FILTER (WHERE (c.cap_bucket = $3 OR c.cap_bucket LIKE $3 || ':%') \
                              AND c.created_at > now() - interval '1 hour') AS freemail_last_hour, \
           min(c.created_at) FILTER (WHERE (c.cap_bucket = $3 OR c.cap_bucket LIKE $3 || ':%') \
                                       AND c.created_at > now() - interval '1 hour') AS freemail_oldest_in_hour, \
           count(*) FILTER (WHERE c.created_at > now() - interval '1 hour') AS global_last_hour, \
           min(c.created_at) FILTER (WHERE c.created_at > now() - interval '1 hour') AS global_oldest_in_hour \
         FROM org_selfserve_creations c \
         WHERE c.status <> 'refused' AND c.created_at > now() - interval '48 hours'",
    )
    .bind(user)
    .bind(bucket)
    .bind(FREEMAIL_BUCKET)
    .fetch_one(conn)
    .await
}

// ── Status (what the sign-up screen polls) ────────────────────────────────

/// The org's sign-up status, derived from its trial (#1521) and its
/// provisioning job (A1). Never carries `last_error` or anything else an
/// operator wrote.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SetupStatus {
    /// The Checkout is not completed (or not paid yet, for a creator or card
    /// that already had a trial).
    AwaitingPayment,
    /// The 24 h window passed without payment; the org will be deleted.
    PaymentAbandoned,
    /// The subscription ended before the org was built.
    Canceled,
    Provisioning,
    Ready,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct StatusView {
    pub status: SetupStatus,
    /// The job's next step while `provisioning` (`queued` before the job
    /// row exists).
    pub step: Option<String>,
    /// `payment` (a slow payment method is settling) or `update` (waiting
    /// for a newer server build) while `provisioning`; `payment` while a
    /// repeat card's first charge is pending.
    pub waiting_for: Option<&'static str>,
    /// Whether `POST …/provisioning/retry` would be accepted (state only;
    /// the rate limit is separate).
    pub retryable: bool,
    /// Until when "resume payment" (`POST /v1/orgs/{org}/trial-checkout`)
    /// is allowed, while `awaiting_payment`.
    pub resume_payment_until: Option<DateTime<Utc>>,
    pub message: &'static str,
}

/// The job facts the status needs (a projection of [`Job`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobFacts {
    pub status: Option<JobStatus>,
    pub step: String,
    pub waiting_for_payment: bool,
    pub waiting_for_build: bool,
    pub cleanup: Option<ClerkOrgCleanup>,
}

impl JobFacts {
    pub fn of(job: &Job) -> Self {
        JobFacts {
            status: job.status(),
            step: job.step.clone(),
            waiting_for_payment: job.subscription_wait_started_at.is_some(),
            waiting_for_build: job.version_wait_started_at.is_some(),
            cleanup: ClerkOrgCleanup::parse(&job.clerk_org_cleanup),
        }
    }
}

pub fn derive_status(
    trial: Option<(TrialState, DateTime<Utc>)>,
    job: Option<&JobFacts>,
    now: DateTime<Utc>,
) -> StatusView {
    let view = |status, message| StatusView {
        status,
        step: None,
        waiting_for: None,
        retryable: false,
        resume_payment_until: None,
        message,
    };
    if let Some(j) = job {
        return match j.status {
            Some(JobStatus::Ready) => view(SetupStatus::Ready, "Your workspace is ready."),
            Some(JobStatus::Provisioning) => StatusView {
                step: Some(j.step.clone()),
                waiting_for: if j.waiting_for_payment {
                    Some("payment")
                } else if j.waiting_for_build {
                    Some("update")
                } else {
                    None
                },
                ..view(SetupStatus::Provisioning, "We're setting up your workspace.")
            },
            // Unknown status text: report failed, never "ready".
            Some(JobStatus::Failed) | None => StatusView {
                retryable: j.cleanup.is_some_and(ClerkOrgCleanup::owner_may_retry),
                ..view(SetupStatus::Failed, "We couldn't finish setting up your workspace.")
            },
        };
    }
    let resume_until = |opened: DateTime<Utc>| opened + chrono::Duration::hours(org_trial::CHECKOUT_RESUME_HOURS);
    match trial {
        // The Checkout was never issued (a retry of the create call issues it).
        None => view(SetupStatus::AwaitingPayment, "Add a card to start your free trial."),
        Some((TrialState::CheckoutOpen, opened)) if org_trial::checkout_abandoned(opened, now) => {
            view(SetupStatus::PaymentAbandoned, "This sign-up wasn't paid within 24 hours.")
        }
        Some((TrialState::CheckoutOpen, opened)) => StatusView {
            resume_payment_until: Some(resume_until(opened)),
            ..view(SetupStatus::AwaitingPayment, "Add a card to start your free trial.")
        },
        Some((TrialState::Abandoned, _)) => {
            view(SetupStatus::PaymentAbandoned, "This sign-up wasn't paid within 24 hours.")
        }
        Some((TrialState::Canceled, _)) => view(SetupStatus::Canceled, "This subscription was canceled."),
        // A repeat card's first charge is still pending: not queued until paid.
        Some((TrialState::Converting, _)) => StatusView {
            waiting_for: Some("payment"),
            ..view(SetupStatus::AwaitingPayment, "We're waiting for your first payment to go through.")
        },
        // Paid or trialing; the webhook queues the job in the same
        // transaction, so this is the instant before the row is visible.
        Some((TrialState::Trialing | TrialState::Converted, _)) => StatusView {
            step: Some("queued".into()),
            ..view(SetupStatus::Provisioning, "We're setting up your workspace.")
        },
    }
}

// ── Shared lookups ────────────────────────────────────────────────────────

/// The org's account id when `user` is its owner or admin (which also
/// requires an interactive session: `org_invites::is_admin` consults
/// `authority_allowed`). `None` for a stranger, a plain member and an
/// unknown org alike, so the path never tells anyone which org ids exist.
async fn admin_account(state: &AppState, control: &PgPool, org_id: &str, user: Uuid) -> Result<Option<Uuid>, Response> {
    let Some(store) = state.control_store() else { return Err(unavailable()) };
    let account: Option<Uuid> =
        sqlx::query_scalar("SELECT id FROM accounts WHERE clerk_org_id = $1 AND is_org_account")
            .bind(org_id)
            .fetch_optional(crate::tenancy::scope::request_read(control))
            .await
            .map_err(|e| internal("account lookup", e))?;
    let Some(account) = account else { return Ok(None) };
    match store.get_account_member_role(account, user).await {
        Ok(Some(role)) if crate::routes::org_invites::is_admin(&role) => Ok(Some(account)),
        Ok(_) => Ok(None),
        Err(e) => Err(internal("role check", e)),
    }
}

async fn trial_of(control: &PgPool, account: Uuid) -> Result<Option<(TrialState, DateTime<Utc>, Option<String>)>, sqlx::Error> {
    let mut tx = crate::tenancy::scope::begin_request_account_read(control, account)
        .await
        .map_err(|e| sqlx::Error::Protocol(e.to_string()))?;
    let row: Option<(String, DateTime<Utc>, Option<String>)> =
        sqlx::query_as("SELECT state, checkout_opened_at, checkout_session_id FROM org_trials WHERE account_id = $1")
            .bind(account)
            .fetch_optional(&mut *tx)
            .await?;
    tx.commit().await?;
    Ok(row.and_then(|(s, at, session)| TrialState::parse(&s).map(|t| (t, at, session))))
}

async fn status_of(control: &PgPool, org_id: &str, account: Uuid) -> Result<StatusView, Response> {
    let job = provisioning::get(control, org_id).await.map_err(|e| internal("job read", e))?;
    let trial = trial_of(control, account).await.map_err(|e| internal("trial read", e))?;
    Ok(derive_status(trial.map(|(s, at, _)| (s, at)), job.as_ref().map(JobFacts::of).as_ref(), Utc::now()))
}

// ── Not-yet-ready orgs, for GET /v1/bootstrap (#1540, gap from B) ────────

/// Most not-ready orgs listed for one caller (a creator can have at most
/// `ORG_SELFSERVE_MAX_PENDING_PER_USER` unpaid at once; this bounds the rest).
pub const PENDING_ORGS_LIMIT: i64 = 20;

/// The caller's self-serve orgs that are not `ready` yet: those they created
/// or are a member of, whose sign-up was recorded in Clerk, that the sweep
/// has not deleted, and whose provisioning job is not `ready`. Each is
/// `{"org_id", "status"}` with `status` from the same derivation as `GET
/// /v1/orgs/{id}/provisioning` (never `ready`). Clerk's org switcher shows
/// such an org to every member and in every browser before its workspace
/// exists; the web uses this list to hide it or send the user to its setup
/// screen. Concierge orgs never appear (they have no intent row), and nor
/// does an org with an `active` routing row (`tenant_databases`): that org
/// is served whatever its job says (a `held_routed` org, #1540 SF5/SF-e).
pub(crate) async fn pending_orgs_for(control: &PgPool, user: Uuid) -> Result<Vec<Value>, String> {
    let rows: Vec<(String, Uuid)> = sqlx::query_as(
        "SELECT c.clerk_org_id, c.account_id FROM org_selfserve_creations c \
           LEFT JOIN tenant_provisioning_jobs j ON j.clerk_org_id = c.clerk_org_id \
          WHERE c.status = 'created' AND c.clerk_org_id IS NOT NULL AND c.account_id IS NOT NULL \
            AND c.sweep_state <> 'deleted' AND j.status IS DISTINCT FROM 'ready' \
            AND NOT EXISTS (SELECT 1 FROM tenant_databases td \
                             WHERE td.clerk_org_id = c.clerk_org_id AND td.status = 'active') \
            AND (c.user_id = $1 OR EXISTS (SELECT 1 FROM account_members m \
                                            WHERE m.account_id = c.account_id AND m.user_id = $1)) \
          ORDER BY c.created_at, c.clerk_org_id LIMIT $2",
    )
    .bind(user)
    .bind(PENDING_ORGS_LIMIT)
    .fetch_all(crate::tenancy::scope::request_read(control))
    .await
    .map_err(|e| format!("pending orgs: {e}"))?;
    let mut out = Vec::with_capacity(rows.len());
    for (org_id, account) in rows {
        let job = provisioning::get(control, &org_id).await.map_err(|e| format!("job read: {e}"))?;
        let trial = trial_of(control, account).await.map_err(|e| format!("trial read: {e}"))?;
        let view = derive_status(trial.map(|(s, at, _)| (s, at)), job.as_ref().map(JobFacts::of).as_ref(), Utc::now());
        if view.status != SetupStatus::Ready {
            out.push(json!({"org_id": org_id, "status": view.status}));
        }
    }
    Ok(out)
}

/// Whether `account` (the caller's ACTIVE account, for `GET /v1/bootstrap`'s
/// `selfserve_org`, #1540 review SF-a) is a team org made by `POST
/// /v1/orgs`: it has an `org_selfserve_creations` row in status `created`.
/// The web mounts Clerk's `<OrganizationProfile/>` only when this is
/// `false`; a self-serve org gets our own member list, because Clerk's
/// members page would invite past the seats the org paid for. A personal
/// account, and a concierge org, never have such a row.
pub(crate) async fn selfserve_org_for(control: &PgPool, account: Uuid) -> Result<bool, String> {
    sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM org_selfserve_creations WHERE account_id = $1 AND status = 'created')",
    )
    .bind(account)
    .fetch_one(crate::tenancy::scope::request_read(control))
    .await
    .map_err(|e| format!("selfserve org: {e}"))
}

// ── POST /v1/orgs ─────────────────────────────────────────────────────────

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateOrgRequest {
    pub name: String,
    /// Seats to buy at Checkout, at least 2 (decision #5).
    pub seats: i64,
    pub success_url: String,
    pub cancel_url: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CreateOrgResponse {
    pub org_id: String,
    pub status: SetupStatus,
    /// Stripe Checkout to send the creator to. `None` when the Checkout is
    /// already complete, or when "resume payment" issued a newer one (then
    /// `resume_payment` is `true`: call `POST /v1/orgs/{org}/trial-checkout`).
    pub checkout_url: Option<String>,
    pub session_id: Option<String>,
    /// `false`: the creator already had a trial, so this Checkout charges
    /// the seats today (different copy, #1521).
    pub trial: Option<bool>,
    pub seats: i64,
    pub trial_credits: i64,
    pub trial_days: i64,
    pub resume_payment: bool,
    /// `true` when this answered a repeat of an earlier request.
    pub replayed: bool,
}

#[derive(Debug, Clone, sqlx::FromRow)]
pub(crate) struct Intent {
    pub(crate) id: Uuid,
    name: String,
    seats: i32,
    pub(crate) status: String,
    pub(crate) clerk_org_id: Option<String>,
    pub(crate) account_id: Option<Uuid>,
    checkout_session_id: Option<String>,
    checkout_url: Option<String>,
    sweep_state: String,
    lease_until: Option<DateTime<Utc>>,
    created_at: DateTime<Utc>,
}

const INTENT_COLS: &str = "id, name, seats, status, clerk_org_id, account_id, checkout_session_id, checkout_url, \
                           sweep_state, lease_until, created_at";

/// A request's (or the sweep's) hold on one intent row (S1): every write it
/// makes after its network calls is guarded by `token`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Lease {
    pub(crate) intent: Uuid,
    pub(crate) token: Uuid,
}

pub async fn create_org(
    State(state): State<Arc<AppState>>,
    Extension(user): Extension<UserContext>,
    headers: HeaderMap,
    Json(body): Json<CreateOrgRequest>,
) -> Response {
    if let Some(r) = crate::middleware::auth::interactive_session_refusal() {
        return r;
    }
    if !provisioning::selfserve_signup_enabled() {
        return not_found();
    }
    let Some(clerk) = ClerkAdminClient::from_env() else {
        return refuse(StatusCode::SERVICE_UNAVAILABLE, "identity_unavailable", "sign-in service unavailable; try again");
    };
    let key = headers.get("idempotency-key").and_then(|v| v.to_str().ok());
    create_org_with(&state, &clerk, &user, key, body, &Caps::from_env()).await
}

/// The body of `POST /v1/orgs`, with its Clerk client and caps injected
/// (the pg suite drives it against local doubles). Everything but the flag
/// check lives here, the interactive-session rule included.
pub(crate) async fn create_org_with(
    state: &AppState,
    clerk: &ClerkAdminClient,
    user: &UserContext,
    idempotency_key: Option<&str>,
    body: CreateOrgRequest,
    caps: &Caps,
) -> Response {
    if let Some(r) = crate::middleware::auth::interactive_session_refusal() {
        return r;
    }
    let Some(name) = validate_name(&body.name) else {
        return refuse(StatusCode::BAD_REQUEST, "invalid_name", "the organization name must be 2 to 120 characters");
    };
    let seats = match org_trial::validate_seats(body.seats) {
        Ok(s) => s,
        Err(m) => return refuse(StatusCode::BAD_REQUEST, "invalid_seats", &m),
    };
    if body.success_url.trim().is_empty() || body.cancel_url.trim().is_empty() {
        return refuse(StatusCode::BAD_REQUEST, "return_urls_required", "success_url and cancel_url are required");
    }
    let Some(key) = validate_idempotency_key(idempotency_key) else {
        return refuse(
            StatusCode::BAD_REQUEST,
            "idempotency_key_required",
            "send an Idempotency-Key header (8 to 200 characters), the same one for every retry of this sign-up",
        );
    };
    let Some(control) = state.control_db() else { return unavailable() };

    // A verified primary email, read from Clerk: not from claims a client
    // could have shaped. Its domain is the per-domain cap's bucket (S8).
    let clerk_user_id: Option<String> = match sqlx::query_scalar("SELECT clerk_user_id FROM users WHERE id = $1")
        .bind(user.user_id)
        .fetch_optional(control)
        .await
    {
        Ok(v) => v.flatten(),
        Err(e) => return internal("clerk_user_id", e),
    };
    let Some(clerk_user_id) = clerk_user_id else {
        return refuse(StatusCode::FORBIDDEN, "identity_required", "sign in with your CoralSwarm account to create an organization");
    };
    let bucket = match verified_bucket(clerk, &clerk_user_id).await {
        Ok(Some(bucket)) => bucket,
        Ok(None) => {
            return refuse(StatusCode::FORBIDDEN, "email_unverified", "verify your email address before creating an organization")
        }
        Err(e) => return identity_unavailable("get_user", e),
    };

    // S1: claim the intent in one short transaction; nothing below holds a
    // database connection across a Clerk or Stripe call.
    let claimed = match claim_intent_lease(control, user.user_id, &key, &name, seats, bucket.as_deref(), caps).await {
        Ok(c) => c,
        Err(r) => return r,
    };
    let lease = claimed.lease;
    // #1540 N5: a client that disconnects drops this future; the guard gives
    // the lease back then instead of leaving the sign-up `create_in_progress`
    // for the whole lease.
    let guard = LeaseGuard::new(control.clone(), lease);
    let result = create_leased(state, control, clerk, user, &clerk_user_id, &name, seats, &body, claimed).await;
    guard.disarm();
    release_lease(control, lease).await;
    match result {
        Ok((resp, replayed)) => {
            let code = if replayed { StatusCode::OK } else { StatusCode::CREATED };
            (code, Json(resp)).into_response()
        }
        Err(r) => r,
    }
}

/// The outcome of [`claim_intent_lease`].
struct Claimed {
    lease: Lease,
    intent: Intent,
    /// The intent row was inserted by this claim (no earlier attempt).
    fresh: bool,
}

fn cap_refusal(decision: CapDecision, user: Uuid, bucket: Option<&str>, counts: &CreationCounts) -> Result<(), Response> {
    let throttled = match &decision {
        CapDecision::Allow { throttled } | CapDecision::Refuse { throttled, .. } => *throttled,
    };
    if throttled {
        // S8: the global threshold is an alert and a throttle, not a refusal.
        tracing::error!(alert = true, %user, ?bucket, global_last_hour = counts.global_last_hour,
            "orgs_create: self-serve org creations passed the global hourly threshold; throttling (per-creator and \
             per-domain caps tightened), not refusing");
    }
    if let CapDecision::Refuse { code, retry_after_s, .. } = decision {
        tracing::warn!(%user, ?bucket, ?counts, code, "orgs_create: creation cap reached");
        let msg = if code == "too_many_pending_orgs" {
            "finish paying for, or wait out, your other new organizations first"
        } else {
            "too many organizations created recently; try again later"
        };
        return Err(rate_limited(code, msg, retry_after_s));
    }
    Ok(())
}

/// How long a create waits for the creator's or the bucket's advisory lock
/// before answering `409 create_in_progress` (#1540 N6).
pub const CAPS_LOCK_TIMEOUT_MS: u64 = 3000;

/// `pg_advisory_xact_lock` on `key`, bounded by the transaction's
/// `lock_timeout`; a timeout is `409 create_in_progress` (retry the same
/// request), never a 500.
async fn lock_or_busy(tx: &mut sqlx::PgConnection, key: &str) -> Result<(), Response> {
    match sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))").bind(key).execute(&mut *tx).await {
        Ok(_) => Ok(()),
        Err(e) if e.as_database_error().and_then(|d| d.code()).as_deref() == Some("55P03") => {
            tracing::warn!(key, "orgs_create: cap lock busy; asking the client to retry");
            Err(in_progress(2))
        }
        Err(e) => Err(internal("caps lock", e)),
    }
}

/// The cap bucket's lock, so two creators in one bucket count it one at a
/// time. The global soft threshold and the free-mail aggregate are not
/// serialised: both are backstops, and a few concurrent requests past them
/// is the price of not funnelling every sign-up through one lock.
async fn lock_bucket(tx: &mut sqlx::PgConnection, bucket: Option<&str>) -> Result<(), Response> {
    lock_or_busy(tx, &format!("org_selfserve_create_bucket:{}", bucket.unwrap_or(""))).await
}

/// The one short transaction a request makes before any network call (S1,
/// S5, S8, N3): under the creator's lock (and the bucket's, when a cap is
/// counted; #1540 N6), held only here, find the
/// intent by (creator, key) and lease it, or check the caps and insert it
/// leased. Refuses a key reused for a different org, an intent the sweep has
/// touched or that is more than [`INTENT_REPLAY_HOURS`] old, and one another
/// request is working on (`409 create_in_progress`). A `refused` intent tried
/// again is re-checked against the caps before it is revived (N3).
async fn claim_intent_lease(
    control: &PgPool,
    user: Uuid,
    key: &str,
    name: &str,
    seats: i64,
    bucket: Option<&str>,
    caps: &Caps,
) -> Result<Claimed, Response> {
    let mut tx = control.begin().await.map_err(|e| internal("begin", e))?;
    // #1540 N6: no global lock. The creator's own lock serialises their
    // requests (the same key twice, and their per-creator caps); the cap
    // bucket's lock is taken below only when a cap is counted. Both are
    // bounded by `lock_timeout`, so a burst queues briefly and then gets
    // `409 create_in_progress` instead of holding pool connections.
    sqlx::query(&format!("SET LOCAL lock_timeout = '{CAPS_LOCK_TIMEOUT_MS}ms'"))
        .execute(&mut *tx)
        .await
        .map_err(|e| internal("lock timeout", e))?;
    lock_or_busy(&mut tx, &format!("org_selfserve_create_user:{user}")).await?;
    let db_now: DateTime<Utc> = sqlx::query_scalar("SELECT now()")
        .fetch_one(&mut *tx)
        .await
        .map_err(|e| internal("now", e))?;
    let q = format!("SELECT {INTENT_COLS} FROM org_selfserve_creations WHERE user_id = $1 AND idempotency_key = $2 FOR UPDATE");
    let existing: Option<Intent> = sqlx::query_as(&q)
        .bind(user)
        .bind(key)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| internal("intent lookup", e))?;
    let token = Uuid::new_v4();
    let seats_i32 = i32::try_from(seats).map_err(|e| internal("seats", e))?;
    let (intent, fresh) = match existing {
        Some(i) => {
            if i.name != name || i64::from(i.seats) != seats {
                return Err(refuse(
                    StatusCode::CONFLICT,
                    "idempotency_key_reused",
                    "this Idempotency-Key was already used for a different organization",
                ));
            }
            match i.sweep_state.as_str() {
                "none" => {}
                "kept" => {
                    return Err((
                        StatusCode::CONFLICT,
                        Json(json!({
                            "error": "this organization is already paid for; check its setup status instead",
                            "code": "signup_complete",
                            "org_id": i.clerk_org_id,
                        })),
                    )
                        .into_response())
                }
                // `deleting` / `deleted`: the sweep owns it (S5).
                _ => return Err(checkout_expired()),
            }
            if i.created_at < db_now - chrono::Duration::hours(INTENT_REPLAY_HOURS) {
                return Err(checkout_expired());
            }
            if let Some(until) = i.lease_until.filter(|u| *u > db_now) {
                return Err(in_progress((until - db_now).num_seconds() + 1));
            }
            if i.status == "refused" {
                lock_bucket(&mut tx, bucket).await?;
                let counts = creation_counts(&mut tx, user, bucket).await.map_err(|e| internal("caps", e))?;
                cap_refusal(decide_caps(&counts, caps, is_freemail_bucket(bucket), Utc::now()), user, bucket, &counts)?;
            }
            let q = format!(
                "UPDATE org_selfserve_creations \
                    SET lease_token = $2, lease_until = now() + make_interval(secs => $3), \
                        status = CASE WHEN status = 'refused' THEN 'creating' ELSE status END, updated_at = now() \
                  WHERE id = $1 AND sweep_state = 'none' RETURNING {INTENT_COLS}"
            );
            let row: Intent = sqlx::query_as(&q)
                .bind(i.id)
                .bind(token)
                .bind(LEASE_SECS as f64)
                .fetch_one(&mut *tx)
                .await
                .map_err(|e| internal("intent lease", e))?;
            (row, false)
        }
        None => {
            lock_bucket(&mut tx, bucket).await?;
            let counts = creation_counts(&mut tx, user, bucket).await.map_err(|e| internal("caps", e))?;
            cap_refusal(decide_caps(&counts, caps, is_freemail_bucket(bucket), Utc::now()), user, bucket, &counts)?;
            let q = format!(
                "INSERT INTO org_selfserve_creations (user_id, idempotency_key, name, seats, cap_bucket, lease_token, lease_until) \
                 VALUES ($1, $2, $3, $4, $5, $6, now() + make_interval(secs => $7)) RETURNING {INTENT_COLS}"
            );
            let row: Intent = sqlx::query_as(&q)
                .bind(user)
                .bind(key)
                .bind(name)
                .bind(seats_i32)
                .bind(bucket)
                .bind(token)
                .bind(LEASE_SECS as f64)
                .fetch_one(&mut *tx)
                .await
                .map_err(|e| internal("intent insert", e))?;
            (row, true)
        }
    };
    tx.commit().await.map_err(|e| internal("commit", e))?;
    Ok(Claimed { lease: Lease { intent: intent.id, token }, intent, fresh })
}

/// Gives a request's lease back if the request future is dropped before it
/// finishes (#1540 N5: axum drops the handler when the client disconnects).
/// The normal path disarms it and releases inline.
struct LeaseGuard {
    control: PgPool,
    lease: Lease,
    armed: bool,
}

impl LeaseGuard {
    fn new(control: PgPool, lease: Lease) -> Self {
        LeaseGuard { control, lease, armed: true }
    }
    fn disarm(mut self) {
        self.armed = false;
    }
}

impl Drop for LeaseGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let (control, lease) = (self.control.clone(), self.lease);
        match tokio::runtime::Handle::try_current() {
            Ok(rt) => {
                rt.spawn(async move {
                    tracing::info!(intent = %lease.intent, "orgs_create: request dropped mid-flight; lease released");
                    release_lease(&control, lease).await;
                });
            }
            Err(_) => tracing::warn!(intent = %lease.intent, "orgs_create: request dropped outside a runtime; the lease expires on its own"),
        }
    }
}

/// How long a verified primary email's cap bucket is remembered (#1540 N6).
const VERIFIED_BUCKET_TTL: std::time::Duration = std::time::Duration::from_secs(300);
/// Entries kept before the cache is emptied (it only saves Clerk calls).
const VERIFIED_BUCKET_MAX: usize = 10_000;

type BucketCache = std::sync::Mutex<std::collections::HashMap<String, (std::time::Instant, Option<String>)>>;

fn verified_bucket_cache() -> &'static BucketCache {
    static CACHE: std::sync::OnceLock<BucketCache> = std::sync::OnceLock::new();
    CACHE.get_or_init(Default::default)
}

/// The creator's verified primary email, as its cap bucket (`Ok(None)`:
/// not verified). A VERIFIED answer is cached for [`VERIFIED_BUCKET_TTL`]
/// (#1540 N6), so a double-submit, a retry after `create_in_progress` or a
/// "resume" does not each cost a Clerk `GET /v1/users`. An unverified answer
/// is never cached: the user may verify a moment later.
pub(crate) async fn verified_bucket(
    clerk: &ClerkAdminClient,
    clerk_user_id: &str,
) -> Result<Option<Option<String>>, ClerkAdminError> {
    if let Ok(cache) = verified_bucket_cache().lock() {
        if let Some((at, bucket)) = cache.get(clerk_user_id) {
            if at.elapsed() < VERIFIED_BUCKET_TTL {
                return Ok(Some(bucket.clone()));
            }
        }
    }
    let user = clerk.get_user(clerk_user_id).await?;
    let Some(email) = user.primary_email_verified() else { return Ok(None) };
    let bucket = cap_bucket_for(email);
    if let Ok(mut cache) = verified_bucket_cache().lock() {
        if cache.len() >= VERIFIED_BUCKET_MAX {
            cache.clear();
        }
        cache.insert(clerk_user_id.to_string(), (std::time::Instant::now(), bucket.clone()));
    }
    Ok(Some(bucket))
}

/// Give the lease back (best effort: an expired lease frees itself).
pub(crate) async fn release_lease(control: &PgPool, lease: Lease) {
    if let Err(e) = sqlx::query(
        "UPDATE org_selfserve_creations SET lease_token = NULL, lease_until = NULL, updated_at = now() \
          WHERE id = $1 AND lease_token = $2",
    )
    .bind(lease.intent)
    .bind(lease.token)
    .execute(control)
    .await
    {
        tracing::warn!(intent = %lease.intent, error = %e, "orgs_create: could not release the intent lease; it expires on its own");
    }
}

#[allow(clippy::too_many_arguments)]
async fn create_leased(
    state: &AppState,
    control: &PgPool,
    clerk: &ClerkAdminClient,
    user: &UserContext,
    clerk_user_id: &str,
    name: &str,
    seats: i64,
    body: &CreateOrgRequest,
    claimed: Claimed,
) -> Result<(CreateOrgResponse, bool), Response> {
    let lease = claimed.lease;
    let replayed = claimed.intent.status == "created";
    let intent = if replayed {
        claimed.intent
    } else if claimed.fresh {
        create_in_clerk(control, clerk, lease, name, seats, clerk_user_id, user.user_id).await?
    } else {
        // `creating` (an earlier attempt died before recording Clerk's
        // answer) or a revived `refused`. Only Clerk knows whether the org
        // exists.
        match find_by_marker(clerk, clerk_user_id, lease.intent).await? {
            Some(org) => match record_created(control, lease, &org, user.user_id).await? {
                Some(i) => i,
                None => return Err(in_progress(LEASE_SECS)),
            },
            None => create_in_clerk(control, clerk, lease, name, seats, clerk_user_id, user.user_id).await?,
        }
    };
    let org_id = intent.clerk_org_id.clone().ok_or_else(|| internal("intent", "created intent without an org id"))?;
    let account = intent.account_id.ok_or_else(|| internal("intent", "created intent without an account"))?;

    // The creator's admin membership. Clerk made them `org:admin` in the
    // create call (`created_by`), so this mirrors a membership that already
    // exists; it does not wait for the webhook, which skips a self-serve
    // org's events until its `accounts` row exists (N1). Both writers are
    // upserts.
    crate::clerk_provision::ensure_org_membership(state, control, &org_id, user.user_id, "admin", true, None, None)
        .await
        .map_err(|e| internal("ensure_org_membership", e))?;

    let base = CreateOrgResponse {
        org_id: org_id.clone(),
        status: SetupStatus::AwaitingPayment,
        checkout_url: None,
        session_id: None,
        trial: None,
        seats,
        trial_credits: org_trial::TRIAL_CREDITS,
        trial_days: org_trial::TRIAL_DAYS,
        resume_payment: false,
        replayed,
    };

    // The Checkout. A replay returns the one this intent already issued
    // while it is still the org's current, open one.
    let trial = trial_of(control, account).await.map_err(|e| internal("trial read", e))?;
    if let Some((state_now, opened, current_session)) = &trial {
        let view = status_of(control, &org_id, account).await?;
        let open = *state_now == TrialState::CheckoutOpen && !org_trial::checkout_abandoned(*opened, Utc::now());
        if !open {
            return Ok((CreateOrgResponse { status: view.status, ..base }, true));
        }
        if intent.checkout_session_id.is_some() && intent.checkout_session_id == *current_session {
            let eligible = trial_eligible(control, account).await.map_err(|e| internal("trial read", e))?;
            return Ok((
                CreateOrgResponse {
                    checkout_url: intent.checkout_url.clone(),
                    session_id: intent.checkout_session_id.clone(),
                    trial: Some(eligible),
                    ..base
                },
                true,
            ));
        }
        if intent.checkout_session_id.is_some() {
            // "Resume payment" issued a newer Checkout: that route owns it.
            return Ok((CreateOrgResponse { resume_payment: true, ..base }, true));
        }
    }
    let checkout = crate::routes::org_trial_checkout::start_org_trial_checkout(
        state,
        account,
        user.user_id,
        seats,
        &body.success_url,
        &body.cancel_url,
    )
    .await?;
    let recorded = sqlx::query(
        "UPDATE org_selfserve_creations SET checkout_session_id = $2, checkout_url = $3, updated_at = now() \
          WHERE id = $1 AND lease_token = $4 AND sweep_state = 'none'",
    )
    .bind(intent.id)
    .bind(&checkout.session_id)
    .bind(&checkout.checkout_url)
    .bind(lease.token)
    .execute(control)
    .await
    .map_err(|e| internal("intent checkout", e))?;
    if recorded.rows_affected() != 1 {
        // The lease ran out mid-request. The Checkout is still this org's
        // current one (#1521 keeps one per org), so it is returned; a replay
        // that finds no recorded session issues the next one.
        tracing::warn!(org = %org_id, intent = %intent.id, "orgs_create: lease lost before the Checkout was recorded");
    }
    tracing::info!(org = %org_id, %account, user = %user.user_id, seats, trial = checkout.trial,
        "orgs_create: team org created, Checkout issued");
    Ok((
        CreateOrgResponse {
            checkout_url: Some(checkout.checkout_url),
            session_id: Some(checkout.session_id),
            trial: Some(checkout.trial),
            ..base
        },
        replayed,
    ))
}

async fn trial_eligible(control: &PgPool, account: Uuid) -> Result<bool, sqlx::Error> {
    let mut tx = crate::tenancy::scope::begin_account_scoped(control, account)
        .await
        .map_err(|e| sqlx::Error::Protocol(e.to_string()))?;
    let v: Option<bool> = sqlx::query_scalar("SELECT trial_eligible FROM org_trials WHERE account_id = $1")
        .bind(account)
        .fetch_optional(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(v.unwrap_or(false))
}

/// Clerk `errors[0].code`s that mean the NAME (or the slug derived from the
/// intent) clashed with or failed Clerk's validation: the only refusals for
/// which "try a different name" is true. A Clerk 422 only; a 401/403, or any
/// code about the instance's settings (`organization_slugs_disabled`) or
/// plan/quota limits, is not the user's to fix.
const CLERK_NAME_REFUSAL_CODES: &[&str] = &[
    "form_identifier_exists",
    "form_param_value_invalid",
    "form_param_format_invalid",
    "form_param_exceeds_allowed_size",
];

fn is_name_refusal(status: u16, code: Option<&str>) -> bool {
    status == 422 && code.is_some_and(|c| CLERK_NAME_REFUSAL_CODES.contains(&c))
}

/// Ask Clerk for the org, then record it. On a Clerk refusal (a 4xx other
/// than 429) the intent becomes `refused` and stops counting against the
/// caps; on an outage it stays `creating`, and the next attempt adopts the
/// org by its marker if Clerk did create it. The org is created with
/// `max_allowed_memberships` = the seats being bought (S4).
async fn create_in_clerk(
    control: &PgPool,
    clerk: &ClerkAdminClient,
    lease: Lease,
    name: &str,
    seats: i64,
    clerk_user_id: &str,
    user: Uuid,
) -> Result<Intent, Response> {
    let metadata = json!({
        SELFSERVE_ORG_MARKER: true,
        SELFSERVE_INTENT_MARKER: lease.intent.to_string(),
        SELFSERVE_CREATOR_MARKER: clerk_user_id,
    });
    let cap = u32::try_from(seats).map_err(|e| internal("seats", e))?;
    let slug = clerk_slug_for_intent(lease.intent);
    match clerk.create_selfserve_organization(name, clerk_user_id, &slug, metadata, cap).await {
        Ok(org) => record_or_orphan(control, clerk, lease, &org.id, user).await,
        Err(e) => match &e {
            // #1556: a refusal that is not about the name or slug (401/403, a
            // disabled-slugs or plan/quota code, any other 4xx) is OUR
            // configuration, not the user's input. No name will ever work, so
            // it never says "try a different name": the intent stops counting
            // against the caps (a replay revives it, as for any refusal), an
            // alert is logged, and the caller gets a retry-later 503. Checked
            // before the slug lookup below: that lookup would fail the same
            // way and hide this error behind `identity_unavailable`.
            ClerkAdminError::Api { status, code, message }
                if (400..500).contains(status) && *status != 429 && !is_name_refusal(*status, code.as_deref()) =>
            {
                tracing::error!(alert = true, status, ?code, %message, intent = %lease.intent,
                    "orgs_create: Clerk refused the organization for a reason that is not the name \
                     (configuration, plan or quota); no name will fix it");
                sqlx::query(
                    "UPDATE org_selfserve_creations SET status = 'refused', updated_at = now() \
                      WHERE id = $1 AND status = 'creating' AND lease_token = $2",
                )
                .bind(lease.intent)
                .bind(lease.token)
                .execute(control)
                .await
                .map_err(|e| internal("intent refused", e))?;
                Err(refuse(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "org_create_unavailable",
                    "we couldn't create the team right now; try again later",
                ))
            }
            ClerkAdminError::Api { status, .. } if (400..500).contains(status) && *status != 429 => {
                // #1540 SF4: an earlier attempt for THIS intent may already
                // have created the org (its response lost to a timeout) and
                // Clerk's list may not show it yet, so the retry's create hits
                // "slug taken". The slug is derived from the intent, so look
                // the org up by it: ours (by its marker) is adopted, never
                // marked refused and orphaned.
                match clerk.get_organization(&slug).await {
                    Ok(existing) if marker_names_intent(&existing.private_metadata, lease.intent) => {
                        tracing::info!(org = %existing.id, intent = %lease.intent,
                            "orgs_create: Clerk refused a repeat create; adopting the org an earlier attempt made");
                        return record_or_orphan(control, clerk, lease, &existing.id, user).await;
                    }
                    Ok(existing) => {
                        tracing::error!(alert = true, org = %existing.id, intent = %lease.intent,
                            "orgs_create: this intent's slug is held by an org without its marker; refusing");
                    }
                    Err(ClerkAdminError::Api { status: 404, .. }) => {}
                    // Unknown: never mark refused on a guess (the org may be
                    // ours). The intent stays `creating`; a retry decides.
                    Err(lookup) => return Err(identity_unavailable("get_organization_by_slug", &lookup)),
                }
                tracing::warn!(error = %e, "orgs_create: Clerk refused the organization");
                sqlx::query(
                    "UPDATE org_selfserve_creations SET status = 'refused', updated_at = now() \
                      WHERE id = $1 AND status = 'creating' AND lease_token = $2",
                )
                .bind(lease.intent)
                .bind(lease.token)
                .execute(control)
                .await
                .map_err(|e| internal("intent refused", e))?;
                Err(refuse(StatusCode::UNPROCESSABLE_ENTITY, "org_create_refused", "this organization could not be created; try a different name"))
            }
            _ => Err(identity_unavailable("create_organization", &e)),
        },
    }
}

/// Record a Clerk org this request created (or adopted) on its intent. If
/// the lease ran out and another request recorded a DIFFERENT org for this
/// intent, ours is an orphan (no account row, no Checkout) and is deleted;
/// while the intent is still `creating`, the new holder may yet adopt ours
/// by its marker, so it is left alone.
async fn record_or_orphan(
    control: &PgPool,
    clerk: &ClerkAdminClient,
    lease: Lease,
    org_id: &str,
    user: Uuid,
) -> Result<Intent, Response> {
    if let Some(i) = record_created(control, lease, org_id, user).await? {
        return Ok(i);
    }
    let other: Option<String> =
        sqlx::query_scalar("SELECT clerk_org_id FROM org_selfserve_creations WHERE id = $1 AND status = 'created'")
            .bind(lease.intent)
            .fetch_optional(control)
            .await
            .map_err(|e| internal("intent read", e))?
            .flatten();
    if other.is_some_and(|o| o != org_id) {
        if let Err(e) = clerk.delete_organization(org_id).await {
            tracing::error!(alert = true, org = %org_id, intent = %lease.intent, error = %e,
                "orgs_create: could not delete an orphaned Clerk org after losing the intent lease");
        }
    }
    Err(in_progress(LEASE_SECS))
}

/// Whether Clerk `private_metadata` carries the self-serve marker for
/// exactly this intent.
pub(crate) fn marker_names_intent(private_metadata: &Value, intent: Uuid) -> bool {
    private_metadata.get(SELFSERVE_ORG_MARKER).and_then(Value::as_bool) == Some(true)
        && private_metadata.get(SELFSERVE_INTENT_MARKER).and_then(Value::as_str) == Some(intent.to_string().as_str())
}

/// Record the Clerk org on the intent AND create its `accounts` row in one
/// transaction, so any other path that reaches `ensure_org_account` for this
/// org afterwards sees it is a self-serve creation (no `free` grant). Only
/// for the holder of `lease`, and only while the sweep has not claimed the
/// intent. `Ok(None)`: the lease was lost and the intent does not name this
/// org (nothing written).
pub(crate) async fn record_created(control: &PgPool, lease: Lease, org_id: &str, user: Uuid) -> Result<Option<Intent>, Response> {
    let mut tx = control.begin().await.map_err(|e| internal("begin", e))?;
    let q = format!("SELECT {INTENT_COLS} FROM org_selfserve_creations WHERE id = $1 FOR UPDATE");
    let current: Intent = sqlx::query_as(&q)
        .bind(lease.intent)
        .fetch_one(&mut *tx)
        .await
        .map_err(|e| internal("intent read", e))?;
    if current.status == "created" {
        tx.rollback().await.ok();
        return Ok((current.clerk_org_id.as_deref() == Some(org_id)).then_some(current));
    }
    let r = sqlx::query(
        "UPDATE org_selfserve_creations SET status = 'created', clerk_org_id = $2, updated_at = now() \
          WHERE id = $1 AND status <> 'created' AND lease_token = $3 AND sweep_state = 'none'",
    )
    .bind(lease.intent)
    .bind(org_id)
    .bind(lease.token)
    .execute(&mut *tx)
    .await
    .map_err(|e| internal("intent created", e))?;
    if r.rows_affected() != 1 {
        tx.rollback().await.ok();
        return Ok(None);
    }
    let account = crate::clerk_provision::ensure_selfserve_org_account(&mut tx, org_id, user)
        .await
        .map_err(|e| internal("accounts row", e))?;
    sqlx::query("UPDATE org_selfserve_creations SET account_id = $2, updated_at = now() WHERE id = $1 AND account_id IS NULL")
        .bind(lease.intent)
        .bind(account)
        .execute(&mut *tx)
        .await
        .map_err(|e| internal("intent account", e))?;
    let row: Intent = sqlx::query_as(&format!("SELECT {INTENT_COLS} FROM org_selfserve_creations WHERE id = $1"))
        .bind(lease.intent)
        .fetch_one(&mut *tx)
        .await
        .map_err(|e| internal("intent read", e))?;
    tx.commit().await.map_err(|e| internal("commit", e))?;
    Ok(Some(row))
}

/// The org an earlier, crashed attempt for `intent` created, if any. The
/// marker is `private_metadata`: Backend-API-only, so a browser cannot
/// forge it onto another org.
pub(crate) async fn find_by_marker(clerk: &ClerkAdminClient, clerk_user_id: &str, intent: Uuid) -> Result<Option<String>, Response> {
    let orgs = clerk
        .list_user_organizations(clerk_user_id)
        .await
        .map_err(|e| identity_unavailable("list_user_organizations", e))?;
    Ok(orgs.into_iter().find(|o| marker_names_intent(&o.private_metadata, intent)).map(|o| o.id))
}

/// Whether Clerk's org `private_metadata` (or a webhook payload's org object)
/// carries the self-serve marker (N1): `POST /v1/orgs` created this org and
/// owns writing its `accounts` row.
pub fn has_selfserve_marker(org: &Value) -> bool {
    org.get("private_metadata").and_then(|m| m.get(SELFSERVE_ORG_MARKER)).and_then(Value::as_bool) == Some(true)
}

/// Whether the org a webhook names is a self-serve org (#1540 N2), failing
/// CLOSED when the payload cannot say.
///
/// Clerk's `organizationMembership.*` payloads carry an `organization`
/// object whose `private_metadata` is optional (`@clerk/backend`'s
/// `OrganizationJSON.private_metadata?`) and absent from Clerk's documented
/// membership-event example, so reading only the payload would say "not
/// self-serve" for exactly the org the check exists for. When the payload
/// has no `private_metadata` object, the org is fetched from Clerk; a fetch
/// that fails is an `Err` (the caller answers 5xx and Svix retries), never a
/// guess. A 404 also answers `true`: the org is gone from Clerk, and a late
/// event must not mint an `accounts` row (with a `free` grant) for it.
///
/// `org_id` is the id the caller already extracted from the payload; `org`
/// is the payload's org object (`Value::Null` when it has none).
pub async fn org_is_selfserve(org_id: &str, org: &Value, clerk: Option<&ClerkAdminClient>) -> Result<bool, String> {
    if org.get("private_metadata").is_some_and(Value::is_object) {
        return Ok(has_selfserve_marker(org));
    }
    let Some(clerk) = clerk else {
        // No `CLERK_SECRET_KEY`: only a local or test server. Every deployed
        // backend has the key (the membership handler already refuses to
        // run without it outside development), so this is never prod.
        tracing::warn!(org = org_id,
            "orgs_create: no Clerk client to read an org's private_metadata; trusting the payload");
        return Ok(has_selfserve_marker(org));
    };
    match clerk.get_organization(org_id).await {
        Ok(o) => Ok(o.private_metadata.get(SELFSERVE_ORG_MARKER).and_then(Value::as_bool) == Some(true)),
        Err(ClerkAdminError::Api { status: 404, .. }) => Ok(true),
        Err(e) => Err(format!("organization lookup for the self-serve marker failed: {e}")),
    }
}

// ── Clerk's member cap (S4) ───────────────────────────────────────────────

/// Clerk's `max_allowed_memberships` for a self-serve org: the purchased
/// seat count (the seats being bought, before the Checkout completes), never
/// below the members it already has (Clerk would otherwise block nothing new
/// and we would only confuse the count), plus `headroom` for an invite that
/// is about to buy its own seat through our seat-quote flow. At least 1.
pub fn desired_member_cap(purchased: i64, intent_seats: i64, members: i64, headroom: i64) -> u32 {
    let base = if purchased > 0 { purchased } else { intent_seats };
    u32::try_from(base.max(members).max(1).saturating_add(headroom.max(0))).unwrap_or(u32::MAX)
}

/// Re-sync Clerk's member cap with the purchased seats (S4). `Ok(None)` for
/// an org `POST /v1/orgs` did not create (concierge orgs are never touched)
/// or one the sweep is deleting.
pub(crate) async fn sync_clerk_membership_cap(
    control: &PgPool,
    clerk: &ClerkAdminClient,
    account: Uuid,
    headroom: i64,
) -> Result<Option<u32>, String> {
    let row: Option<(String, i32)> = sqlx::query_as(
        "SELECT clerk_org_id, seats FROM org_selfserve_creations \
          WHERE account_id = $1 AND status = 'created' AND sweep_state IN ('none', 'kept')",
    )
    .bind(account)
    .fetch_optional(control)
    .await
    .map_err(|e| format!("intent read: {e}"))?;
    let Some((org, seats)) = row else { return Ok(None) };
    let mut tx = crate::tenancy::scope::begin_account_scoped(control, account)
        .await
        .map_err(|e| format!("scope: {e}"))?;
    let pool = crate::billing::slot_pool::read_slot_pool(&mut tx, account)
        .await
        .map_err(|e| format!("slot pool: {e}"))?;
    let members: i64 = sqlx::query_scalar("SELECT count(*) FROM account_members WHERE account_id = $1")
        .bind(account)
        .fetch_one(&mut *tx)
        .await
        .map_err(|e| format!("members: {e}"))?;
    tx.commit().await.map_err(|e| format!("commit: {e}"))?;
    let cap = desired_member_cap(pool.purchased, i64::from(seats), members, headroom);
    clerk
        .update_organization_max_memberships(&org, cap)
        .await
        .map_err(|e| format!("Clerk update: {e}"))?;
    Ok(Some(cap))
}

/// [`sync_clerk_membership_cap`], logging instead of failing: seat purchases
/// and invites succeed on their own terms. A failure is an alert (the cap is
/// a seat-enforcement control).
pub(crate) async fn sync_clerk_membership_cap_logged_with(
    control: &PgPool,
    clerk: &ClerkAdminClient,
    account: Uuid,
    headroom: i64,
) {
    match sync_clerk_membership_cap(control, clerk, account, headroom).await {
        Ok(Some(cap)) => tracing::info!(%account, cap, headroom, "orgs_create: Clerk member cap re-synced with the purchased seats"),
        Ok(None) => {}
        Err(e) => tracing::error!(alert = true, %account, error = %e,
            "orgs_create: could not re-sync Clerk's member cap with the purchased seats"),
    }
}

/// The same with the env Clerk client (a no-op without one).
pub(crate) async fn sync_clerk_membership_cap_logged(control: &PgPool, account: Uuid, headroom: i64) {
    let Some(clerk) = ClerkAdminClient::from_env() else { return };
    sync_clerk_membership_cap_logged_with(control, &clerk, account, headroom).await;
}

/// The same, in the background (after a seat change has committed).
pub(crate) fn spawn_clerk_membership_cap_sync(control: PgPool, account: Uuid) {
    tokio::spawn(async move { sync_clerk_membership_cap_logged(&control, account, 0).await });
}

/// Admit one member who may buy their own seat, through Clerk (#1540 SF2,
/// N3). A self-serve org's Clerk cap equals its purchased seats, so a
/// purchase-mode admission (an invite allowed to buy, or a join-request
/// approval with the pool full) would be refused by Clerk itself: the cap is
/// raised by one first. The seat is bought when the membership lands, and
/// the cap re-synced to the new purchased count then. If `admit` fails, the
/// headroom is given back right away instead of staying until the next seat
/// change. A no-op wrapper for an org `POST /v1/orgs` did not create.
pub(crate) async fn with_member_cap_headroom<F>(control: &PgPool, clerk: &ClerkAdminClient, account: Uuid, admit: F) -> Response
where
    F: std::future::Future<Output = Response>,
{
    sync_clerk_membership_cap_logged_with(control, clerk, account, 1).await;
    let resp = admit.await;
    if !resp.status().is_success() {
        sync_clerk_membership_cap_logged_with(control, clerk, account, 0).await;
    }
    resp
}

/// Re-sync the cap with the purchased seats and only THEN run `then`
/// (#1540 SF2): after a seat purchase, the drain that admits backlogged
/// members through Clerk must not race the cap raise and be refused by it.
pub(crate) async fn sync_cap_then<F>(control: &PgPool, clerk: Option<&ClerkAdminClient>, account: Uuid, then: F) -> F::Output
where
    F: std::future::Future,
{
    if let Some(clerk) = clerk {
        sync_clerk_membership_cap_logged_with(control, clerk, account, 0).await;
    }
    then.await
}

// ── GET /v1/orgs/{org_id}/provisioning ────────────────────────────────────

pub async fn get_provisioning(
    State(state): State<Arc<AppState>>,
    Extension(user): Extension<UserContext>,
    Path(org_id): Path<String>,
) -> Response {
    if let Some(r) = crate::middleware::auth::interactive_session_refusal() {
        return r;
    }
    if !provisioning::selfserve_signup_enabled() {
        return not_found();
    }
    get_provisioning_with(&state, &user, &org_id).await
}

pub(crate) async fn get_provisioning_with(state: &AppState, user: &UserContext, org_id: &str) -> Response {
    let Some(control) = state.control_db() else { return unavailable() };
    let account = match admin_account(state, control, org_id, user.user_id).await {
        Ok(Some(a)) => a,
        Ok(None) => return org_not_found(),
        Err(r) => return r,
    };
    match status_of(control, org_id, account).await {
        Ok(view) => match with_org(control, org_id, &view).await {
            Ok(v) => (StatusCode::OK, Json(v)).into_response(),
            Err(r) => r,
        },
        Err(r) => r,
    }
}

/// The status body: the view, the org id, and `seats` (for web PR B's
/// "resume payment": the seat count the creator chose on `POST /v1/orgs`;
/// `null` for an org `POST /v1/orgs` did not create).
async fn with_org(control: &PgPool, org_id: &str, view: &StatusView) -> Result<Value, Response> {
    let seats: Option<i32> = sqlx::query_scalar(
        "SELECT seats FROM org_selfserve_creations WHERE clerk_org_id = $1 AND status = 'created'",
    )
    .bind(org_id)
    .fetch_optional(crate::tenancy::scope::request_read(control))
    .await
    .map_err(|e| internal("seats read", e))?;
    let mut v = serde_json::to_value(view).unwrap_or_else(|_| json!({}));
    v["org_id"] = json!(org_id);
    v["seats"] = json!(seats);
    Ok(v)
}

// ── POST /v1/orgs/{org_id}/provisioning/retry ─────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetryLimits {
    pub min_interval_secs: i64,
    pub max_retries: i32,
}

impl RetryLimits {
    pub fn from_env() -> Self {
        RetryLimits {
            min_interval_secs: env_i64("ORG_SELFSERVE_RETRY_MIN_INTERVAL_SECS", DEFAULT_RETRY_MIN_INTERVAL_SECS),
            max_retries: i32::try_from(env_i64("ORG_SELFSERVE_MAX_RETRIES", i64::from(DEFAULT_MAX_RETRIES)))
                .unwrap_or(DEFAULT_MAX_RETRIES),
        }
    }
}

pub async fn retry_provisioning(
    State(state): State<Arc<AppState>>,
    Extension(user): Extension<UserContext>,
    Path(org_id): Path<String>,
) -> Response {
    if let Some(r) = crate::middleware::auth::interactive_session_refusal() {
        return r;
    }
    if !provisioning::selfserve_signup_enabled() {
        return not_found();
    }
    retry_provisioning_with(&state, &user, &org_id, &RetryLimits::from_env()).await
}

pub(crate) async fn retry_provisioning_with(
    state: &AppState,
    user: &UserContext,
    org_id: &str,
    limits: &RetryLimits,
) -> Response {
    if let Some(r) = crate::middleware::auth::interactive_session_refusal() {
        return r;
    }
    let Some(control) = state.control_db() else { return unavailable() };
    let account = match admin_account(state, control, org_id, user.user_id).await {
        Ok(Some(a)) => a,
        Ok(None) => return org_not_found(),
        Err(r) => return r,
    };
    let mut tx = match control.begin().await {
        Ok(tx) => tx,
        Err(e) => return internal("begin", e),
    };
    // The rate limit and the retry commit together: a refused retry spends
    // nothing, and two concurrent retries serialise on the intent row.
    let counted: Option<i32> = match sqlx::query_scalar(
        "UPDATE org_selfserve_creations \
            SET retry_count = retry_count + 1, last_retry_at = now(), updated_at = now() \
          WHERE clerk_org_id = $1 AND account_id = $2 AND status = 'created' \
            AND retry_count < $3 \
            AND (last_retry_at IS NULL OR last_retry_at < now() - make_interval(secs => $4)) \
          RETURNING retry_count",
    )
    .bind(org_id)
    .bind(account)
    .bind(limits.max_retries)
    .bind(limits.min_interval_secs as f64)
    .fetch_optional(&mut *tx)
    .await
    {
        Ok(c) => c,
        Err(e) => return internal("retry count", e),
    };
    if counted.is_none() {
        let _ = tx.rollback().await;
        let row: Option<(i32, Option<DateTime<Utc>>)> = match sqlx::query_as(
            "SELECT retry_count, last_retry_at FROM org_selfserve_creations WHERE clerk_org_id = $1 AND account_id = $2",
        )
        .bind(org_id)
        .bind(account)
        .fetch_optional(control)
        .await
        {
            Ok(r) => r,
            Err(e) => return internal("retry count read", e),
        };
        return match row {
            // Not a self-serve org: there is nothing here to retry.
            None => org_not_found(),
            Some((n, _)) if n >= limits.max_retries => refuse(
                StatusCode::TOO_MANY_REQUESTS,
                "retry_limit_reached",
                "setup has been retried too many times; contact support",
            ),
            Some((_, last)) => {
                let wait = last
                    .map(|t| (t + chrono::Duration::seconds(limits.min_interval_secs) - Utc::now()).num_seconds())
                    .unwrap_or(limits.min_interval_secs)
                    .clamp(1, limits.min_interval_secs.max(1));
                rate_limited("retry_rate_limited", "setup was retried a moment ago; try again later", wait)
            }
        };
    }
    match sqlx::query_scalar::<_, bool>("SELECT tenant_provisioning_retry($1)")
        .bind(org_id)
        .fetch_one(&mut *tx)
        .await
    {
        Ok(_) => {}
        Err(e) => {
            let _ = tx.rollback().await;
            let code = e.as_database_error().and_then(|d| d.code()).map(|c| c.to_string());
            return match code.as_deref() {
                Some("P0002") => refuse(StatusCode::CONFLICT, "nothing_to_retry", "this organization has no failed setup to retry"),
                Some("23514") => refuse(StatusCode::CONFLICT, "not_retryable", "this organization's setup can't be retried now"),
                Some("23505") => refuse(StatusCode::CONFLICT, "already_set_up", "this organization is already set up"),
                _ => internal("retry", e),
            };
        }
    }
    if let Err(e) = tx.commit().await {
        return internal("commit", e);
    }
    tracing::info!(org = %org_id, user = %user.user_id, retry = counted, "orgs_create: provisioning retry requested");
    match status_of(control, org_id, account).await {
        Ok(view) => match with_org(control, org_id, &view).await {
            Ok(v) => (StatusCode::ACCEPTED, Json(v)).into_response(),
            Err(r) => r,
        },
        Err(r) => r,
    }
}

#[cfg(test)]
mod tests;
#[cfg(test)]
mod pg_tests;
