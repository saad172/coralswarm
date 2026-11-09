//! Signed OAuth `state` tokens (CS-110).
//!
//! # Why
//!
//! The OAuth `state` parameter round-trips through the user's browser and the
//! provider, then comes back to our callback where it tells us *which account*
//! the new connection belongs to. The v1 scheme put the raw account UUID in
//! `state` — so anyone could craft `?state=<someone-else's-account>` and bind a
//! connection they authorized to a victim's account (a CSRF / account-confusion
//! attack). That is tolerable for an internal single-tenant deployment but not
//! for a multi-company SaaS.
//!
//! # How
//!
//! The account id is bound into a short-lived **HMAC-signed** token (JWT, HS256)
//! that only the server can mint. The mint endpoint derives the account from the
//! authenticated session — never from a client parameter — so a caller cannot
//! choose an account other than their own. The callback verifies the signature
//! and expiry before trusting the embedded account id.
//!
//! The signing key is derived from `OAUTH_STATE_SECRET` (any sufficiently long
//! random string) with [`crate::crypto::kdf::derive_key`] under
//! [`crate::crypto::kdf::purpose::OAUTH_STATE`] — never the raw secret. The
//! raw secret also feeds other purposes (Teams tickets, and the query-audit
//! digest when `QUERY_AUDIT_HMAC_KEY` is unset); a raw-key HS256 signature is
//! just `HMAC(secret, "<header>.<payload>")`, so any other raw-key HMAC over
//! caller-chosen text would be a state-forging oracle. The core
//! `sign`/`verify` functions take the secret explicitly so they are pure and
//! unit-testable without environment mutation; [`state_secret`] reads it from
//! the environment for the request handlers.
//!
//! # No raw-key compatibility window
//!
//! Tokens signed with the raw secret (every token minted before the key
//! derivation landed) are rejected outright. A grace window would have to be
//! measured from process start (the binary cannot know its own deploy time),
//! which reopens on every machine restart, and the raw-key signatures it
//! would accept are exactly the forgeable ones. The cost is bounded by
//! [`STATE_TTL_SECS`]: a connect flow started in the 10 minutes before a
//! deploy fails at the callback with the ordinary invalid-state response and
//! the user clicks Connect again.

use jsonwebtoken::{decode, encode, Algorithm, DecodingKey, EncodingKey, Header, Validation};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Lifetime of a signed state token. Long enough to complete an interactive
/// consent (including an admin reading scopes), short enough to bound replay.
pub const STATE_TTL_SECS: i64 = 600; // 10 minutes

/// Claims embedded in the signed state token.
#[derive(Debug, Serialize, Deserialize)]
struct StateClaims {
    /// Subject — the account id this connection will be bound to.
    sub: String,
    /// Expiry (unix seconds). Validated by `jsonwebtoken`.
    exp: i64,
    /// Single-use nonce, so two concurrent connect attempts produce distinct
    /// tokens. Checked-and-consumed atomically against the replay store
    /// (`oauth::jti_store::consume`) by `routes::oauth::callback` — see
    /// [`VerifiedState`].
    jti: String,
    /// The connecting user's id (gsuite Train G2 PR 1, `oauth.rs::mint_state`).
    /// Optional so every state minted before this field existed — and every
    /// state minted for a provider that never needs per-member isolation —
    /// decodes with `uid: None`, byte-identical to the old three-claim shape.
    /// Lets `routes::oauth::callback` construct `TokenOwner::Member(uid)` for
    /// a surface that needs it (Gmail) without ever trusting a client-
    /// supplied user id.
    #[serde(default)]
    uid: Option<String>,
    /// Which provider surface this state was minted for (e.g. `"gmail"`) —
    /// gsuite Train G2 PR 1. Optional for the same reason as `uid`: absent on
    /// every pre-existing state and on every non-`?surface=` mint, and
    /// `routes::oauth::callback` treats an absent/unparseable claim as the
    /// default surface (Calendar, for Google), never widening what a state
    /// is trusted to authorize.
    #[serde(default)]
    sfc: Option<String>,
    #[serde(default)]
    teams_onboarding: Option<String>,
    #[serde(default)]
    meeting_return: Option<String>,
    /// Where the provider callback hands the browser back to, for a connect
    /// started from the generic connector API (connectors redesign, B2):
    /// `"wizard"` or `"onboarding"`. Absent on every state minted by the older
    /// per-provider UI, which keeps redirecting exactly as it always did. Not
    /// trusted as a URL: the callback only ever turns it into one of the two
    /// [`crate::connectors::catalog::ReturnTo`] values, and builds the
    /// destination itself.
    #[serde(default)]
    ret: Option<String>,
}

/// Failure verifying a signed state token.
#[derive(Debug, PartialEq, Eq)]
pub enum StateError {
    /// Signature invalid, token malformed, expired, or the subject is not a UUID.
    Invalid,
    /// The signature is VALID but the token is past its `exp`. Only a token
    /// this server minted can reach this arm (the signature is checked before
    /// expiry), so telling it apart leaks nothing to a forger — it lets the
    /// callback send a real user back to the app with a friendly message.
    Expired,
}

/// The verified contents of a signed state token (CS-110 follow-up,
/// foundation train PR 1.4): the account it is bound to, and its single-use
/// `jti`. [`verify_state`] used to return a bare `Uuid` — that was enough
/// when `jti` was defence-in-depth only, but the reason a `jti` is minted AT
/// ALL is to make the token single-use, and enforcing that means the
/// callback needs to see it and consume it (`oauth::jti_store::consume`)
/// against the replay store. Returning both fields keeps `verify_state`
/// itself pure (no DB access) while giving its caller everything the replay
/// check needs.
#[derive(Debug, PartialEq, Eq)]
pub struct VerifiedState {
    /// The account this connection will be bound to.
    pub account_id: Uuid,
    /// The token's single-use nonce, to be consumed exactly once against the
    /// jti replay store before the callback does anything else with it.
    pub jti: String,
    /// The connecting user's id, when the state embedded one (gsuite Train G2
    /// PR 1). `None` for every state minted before this field existed, for
    /// any state minted for a provider that never sets `?surface=`, and for a
    /// present-but-unparseable claim (defensive — this token is HMAC-signed
    /// by us, so a malformed `uid` would mean a bug in the minting side, not
    /// tampering; treated the same as absent rather than failing the whole
    /// verification over a claim `sub`/`exp`/`jti` do not depend on).
    pub user_id: Option<Uuid>,
    /// The provider surface this state was minted for, when it embedded one
    /// (gsuite Train G2 PR 1) — the raw claim value, unparsed (parsing it
    /// into a concrete surface type is `routes::oauth`'s job, so this module
    /// stays independent of `connectors::google`). `None` for every state
    /// minted before this field existed and for a plain (no `?surface=`)
    /// mint.
    pub surface: Option<String>,
    pub teams_onboarding: Option<String>,
    pub meeting_return: Option<String>,
    /// The raw `ret` claim (see [`StateClaims::ret`]); parse it with
    /// `catalog::ReturnTo::parse`, which accepts exactly the known values.
    pub return_to: Option<String>,
}

/// The HS256 key for state tokens: the `oauth-state` derivation of `secret`.
fn state_key(secret: &str) -> [u8; crate::crypto::kdf::DERIVED_KEY_LEN] {
    crate::crypto::kdf::derive_key(secret.as_bytes(), crate::crypto::kdf::purpose::OAUTH_STATE)
}

/// Read the state-signing secret from the environment.
///
/// Returns `None` when `OAUTH_STATE_SECRET` is unset or empty — handlers treat
/// that as a server misconfiguration (`503`), never as a pass.
pub fn state_secret() -> Option<String> {
    let s = std::env::var("OAUTH_STATE_SECRET").ok()?;
    if s.is_empty() {
        None
    } else {
        Some(s)
    }
}

/// Mint a signed state token binding `account_id`, expiring `STATE_TTL_SECS`
/// after `now_unix`. `nonce` should be unpredictable (e.g. a fresh UUID).
///
/// `user_id`/`surface` (gsuite Train G2 PR 1): optionally embed the
/// connecting user and the requested provider surface, so
/// `routes::oauth::callback` can resolve per-surface token ownership
/// strictly from these signed claims — never from a client query param.
/// Every pre-existing caller passes `(None, None)`, which round-trips through
/// [`verify_state`] as `(user_id: None, surface: None)`, byte-identical to
/// the old three-claim token shape.
pub fn sign_state(
    secret: &str,
    account_id: Uuid,
    now_unix: i64,
    nonce: &str,
    user_id: Option<Uuid>,
    surface: Option<&str>,
) -> Result<String, jsonwebtoken::errors::Error> {
    sign_state_with_teams_onboarding(secret, account_id, now_unix, nonce, user_id, surface, None)
}

pub fn sign_state_with_teams_onboarding(
    secret: &str,
    account_id: Uuid,
    now_unix: i64,
    nonce: &str,
    user_id: Option<Uuid>,
    surface: Option<&str>,
    ticket: Option<&str>,
) -> Result<String, jsonwebtoken::errors::Error> {
    let claims = StateClaims {
        sub: account_id.to_string(),
        exp: now_unix + STATE_TTL_SECS,
        jti: nonce.to_string(),
        uid: user_id.map(|u| u.to_string()),
        sfc: surface.map(|s| s.to_string()),
        teams_onboarding: ticket.map(str::to_owned),
        meeting_return: None,
        ret: None,
    };
    encode(
        &Header::new(Algorithm::HS256),
        &claims,
        &EncodingKey::from_secret(&state_key(secret)),
    )
}

/// Verify a signed state token and return the bound account id and its jti.
///
/// Checks the HMAC signature and expiry. Returns [`StateError::Invalid`] for any
/// failure (bad signature, tampering, non-UUID subject) — deliberately
/// without distinguishing the cause, to avoid an oracle. The one exception is
/// [`StateError::Expired`]: a correctly-signed token past its `exp`, which only
/// this server could have minted, so it reveals nothing to an attacker.
///
/// This function is intentionally pure (no DB access): it only proves the
/// token is authentic and unexpired. It does NOT check whether the returned
/// `jti` has already been consumed — that is a separate, atomic step
/// (`oauth::jti_store::consume`) the caller must perform against the replay
/// store before trusting the token as single-use.
pub fn verify_state(secret: &str, token: &str) -> Result<VerifiedState, StateError> {
    let mut validation = Validation::new(Algorithm::HS256);
    validation.validate_exp = true;
    // We don't use aud/iss; require only the exp claim we set.
    validation.set_required_spec_claims(&["exp"]);

    let data = decode::<StateClaims>(
        token,
        &DecodingKey::from_secret(&state_key(secret)),
        &validation,
    )
    .map_err(|e| match e.kind() {
        jsonwebtoken::errors::ErrorKind::ExpiredSignature => StateError::Expired,
        _ => StateError::Invalid,
    })?;

    let account_id = Uuid::parse_str(&data.claims.sub).map_err(|_| StateError::Invalid)?;
    // Soft-parse: an absent or malformed `uid` decodes as `None` rather than
    // failing the whole token — see [`VerifiedState::user_id`]'s own doc.
    let user_id = data
        .claims
        .uid
        .as_deref()
        .and_then(|s| Uuid::parse_str(s).ok());
    Ok(VerifiedState {
        account_id,
        jti: data.claims.jti,
        user_id,
        surface: data.claims.sfc,
        teams_onboarding: data.claims.teams_onboarding,
        meeting_return: data.claims.meeting_return,
        return_to: data.claims.ret,
    })
}

/// Mint a state for a connect started from the generic connector API: the
/// ordinary claims plus the `ret` claim naming where the browser lands after the
/// provider callback. `return_to` is the wire value of
/// `catalog::ReturnTo` (`"wizard"` | `"onboarding"`).
pub fn sign_state_for_connect(
    secret: &str,
    account_id: Uuid,
    now_unix: i64,
    nonce: &str,
    user_id: Option<Uuid>,
    surface: Option<&str>,
    return_to: &str,
) -> Result<String, jsonwebtoken::errors::Error> {
    let claims = StateClaims {
        sub: account_id.to_string(),
        exp: now_unix + STATE_TTL_SECS,
        jti: nonce.to_string(),
        uid: user_id.map(|u| u.to_string()),
        sfc: surface.map(str::to_owned),
        teams_onboarding: None,
        meeting_return: None,
        ret: Some(return_to.to_string()),
    };
    encode(
        &Header::new(Algorithm::HS256),
        &claims,
        &EncodingKey::from_secret(&state_key(secret)),
    )
}

/// The `ret` claim of a correctly signed token, whether or not it has expired.
///
/// The callback uses this to decide where to send the browser after a failure,
/// including "the sign-in took longer than the state's ten minutes". The
/// signature is still checked, so only a token this server minted is believed;
/// expiry is ignored because the answer only picks a destination page and
/// authorizes nothing. Never use it to accept a state.
pub fn return_to_claim(secret: &str, token: &str) -> Option<String> {
    let mut validation = Validation::new(Algorithm::HS256);
    validation.validate_exp = false;
    validation.set_required_spec_claims(&["exp"]);
    decode::<StateClaims>(
        token,
        &DecodingKey::from_secret(&state_key(secret)),
        &validation,
    )
    .ok()
    .and_then(|d| d.claims.ret)
}

/// Cheap structural check used by the callback *before* the DB-pool guard, so a
/// blatantly-malformed `state` is rejected (`400`) without a crypto step, while a
/// well-formed-but-unverified token defers to the later cryptographic check. A
/// JWT is three base64url segments separated by dots.
pub fn looks_like_token(state: &str) -> bool {
    let mut parts = state.split('.');
    let (Some(a), Some(b), Some(c), None) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        return false;
    };
    !a.is_empty() && !b.is_empty() && !c.is_empty()
}

#[cfg(test)]
mod tests {
    use super::*;

    const SECRET: &str = "test-oauth-state-secret-0123456789";
    // A timestamp far in the future so exp validation (against the real wall
    // clock) is never the reason a token is rejected in these tests.
    const FUTURE: i64 = 4_000_000_000; // ~year 2096

    #[test]
    fn sign_then_verify_roundtrips_account_id() {
        let account = Uuid::new_v4();
        let token = sign_state(SECRET, account, FUTURE, "nonce-1", None, None).unwrap();
        assert!(looks_like_token(&token));
        assert_eq!(verify_state(SECRET, &token).unwrap().account_id, account);
    }

    #[test]
    fn return_to_round_trips_and_is_absent_on_every_older_mint() {
        let account = Uuid::new_v4();
        let member = Uuid::new_v4();
        let token = sign_state_for_connect(
            SECRET,
            account,
            FUTURE,
            "n-ret",
            Some(member),
            Some("slack_install"),
            "wizard",
        )
        .unwrap();
        let v = verify_state(SECRET, &token).unwrap();
        assert_eq!(v.return_to.as_deref(), Some("wizard"));
        assert_eq!(v.account_id, account);
        assert_eq!(v.user_id, Some(member));
        assert_eq!(v.surface.as_deref(), Some("slack_install"));

        // The old UI's mints carry no claim, so their callbacks keep redirecting
        // exactly as before.
        let plain = sign_state(SECRET, account, FUTURE, "n", Some(member), None).unwrap();
        assert_eq!(verify_state(SECRET, &plain).unwrap().return_to, None);
    }

    #[test]
    fn return_to_claim_survives_expiry_but_not_a_forged_signature() {
        let account = Uuid::new_v4();
        // Signed in 1970: long expired.
        let stale =
            sign_state_for_connect(SECRET, account, 0, "n", None, None, "onboarding").unwrap();
        assert_eq!(verify_state(SECRET, &stale), Err(StateError::Expired));
        assert_eq!(
            return_to_claim(SECRET, &stale).as_deref(),
            Some("onboarding")
        );
        assert_eq!(
            return_to_claim("a-different-secret-aaaaaaaaaaaaaaaa", &stale),
            None
        );
        assert_eq!(return_to_claim(SECRET, "not.a.token"), None);
    }

    #[test]
    fn sign_then_verify_roundtrips_jti() {
        // The jti must survive the round trip byte-for-byte: it is the value
        // `oauth::jti_store::consume` claims against the replay store, so any
        // drift here would silently defeat replay checking.
        let account = Uuid::new_v4();
        let token = sign_state(SECRET, account, FUTURE, "nonce-xyz", None, None).unwrap();
        assert_eq!(verify_state(SECRET, &token).unwrap().jti, "nonce-xyz");
    }

    #[test]
    fn verify_rejects_wrong_secret() {
        let account = Uuid::new_v4();
        let token = sign_state(SECRET, account, FUTURE, "n", None, None).unwrap();
        assert_eq!(
            verify_state("a-different-secret-aaaaaaaaaaaaaaaa", &token),
            Err(StateError::Invalid)
        );
    }

    #[test]
    fn verify_rejects_expired_token() {
        // exp = now + TTL; sign far in the past so it's expired well beyond the
        // default 60s leeway.
        let account = Uuid::new_v4();
        let token = sign_state(SECRET, account, 0, "n", None, None).unwrap();
        assert_eq!(verify_state(SECRET, &token), Err(StateError::Expired));
    }

    #[test]
    fn expired_token_with_bad_signature_is_invalid_not_expired() {
        // Signature is checked before expiry, so a forged-and-stale token
        // cannot be told apart from any other forgery.
        let account = Uuid::new_v4();
        let token = sign_state(SECRET, account, 0, "n", None, None).unwrap();
        assert_eq!(
            verify_state("a-different-secret-aaaaaaaaaaaaaaaa", &token),
            Err(StateError::Invalid)
        );
    }

    #[test]
    fn verify_rejects_tampered_token() {
        let account = Uuid::new_v4();
        let mut token = sign_state(SECRET, account, FUTURE, "n", None, None).unwrap();
        // Flip a character in the payload segment.
        let mut parts: Vec<&str> = token.split('.').collect();
        let mut payload: Vec<u8> = parts[1].bytes().collect();
        payload[0] ^= 0x01;
        let tampered = String::from_utf8_lossy(&payload).to_string();
        parts[1] = &tampered;
        token = parts.join(".");
        assert_eq!(verify_state(SECRET, &token), Err(StateError::Invalid));
    }

    #[test]
    fn looks_like_token_structural_check() {
        assert!(looks_like_token("aaa.bbb.ccc"));
        assert!(!looks_like_token("not-a-uuid"));
        assert!(!looks_like_token("only.two"));
        assert!(!looks_like_token("a..c"));
        assert!(!looks_like_token(""));
        assert!(!looks_like_token("four.parts.here.nope"));
    }

    // ── per-purpose key separation (crypto::kdf) ────────────────────────────

    /// Build `"<header>.<payload>"` for a state token an attacker would want
    /// (bound to `victim`, far-future exp, fresh jti) — the HS256 signing
    /// input, i.e. the exact text they would type as a question.
    fn forged_signing_input(victim: Uuid) -> String {
        use base64::Engine as _;
        let b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD;
        let header = b64.encode(serde_json::to_vec(&Header::new(Algorithm::HS256)).unwrap());
        let claims = serde_json::json!({
            "sub": victim.to_string(),
            "exp": FUTURE,
            "jti": "forged-jti",
        });
        let payload = b64.encode(serde_json::to_vec(&claims).unwrap());
        format!("{header}.{payload}")
    }

    fn attach_hex_signature(signing_input: &str, hex_sig: &str) -> String {
        use base64::Engine as _;
        let sig = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(hex::decode(hex_sig).expect("digest is hex"));
        format!("{signing_input}.{sig}")
    }

    #[test]
    fn query_audit_digest_of_signing_input_is_not_a_state_signature() {
        // The attack: ask "<header>.<payload>" as a question, read its
        // question_digest from GET /v1/query-audit, append it as the
        // signature. With QUERY_AUDIT_HMAC_KEY unset (prod), the digest is
        // keyed from the same OAUTH_STATE_SECRET as state tokens.
        let victim = Uuid::new_v4();
        let input = forged_signing_input(victim);
        let digest = crate::services::query_audit::digest_for_secret(&input, Some(SECRET));
        let forged = attach_hex_signature(&input, &digest);
        assert_eq!(verify_state(SECRET, &forged), Err(StateError::Invalid));

        // Non-vacuity: under the pre-fix scheme (both sides keyed with the
        // raw secret) this exact construction WAS a valid token.
        use hmac::{Hmac, Mac};
        let mut mac = Hmac::<sha2::Sha256>::new_from_slice(SECRET.as_bytes()).unwrap();
        mac.update(input.as_bytes());
        let old_digest = hex::encode(mac.finalize().into_bytes());
        let old_forged = attach_hex_signature(&input, &old_digest);
        let mut validation = Validation::new(Algorithm::HS256);
        validation.set_required_spec_claims(&["exp"]);
        assert!(
            decode::<StateClaims>(
                &old_forged,
                &DecodingKey::from_secret(SECRET.as_bytes()),
                &validation
            )
            .is_ok(),
            "the raw-key construction this test models must be the real attack"
        );
    }

    #[test]
    fn raw_secret_signed_state_is_rejected_no_compat_window() {
        // Design choice (module doc, "No raw-key compatibility window"):
        // every raw-key token — legitimately minted before the deploy or
        // forged — is Invalid, with no time-based grace.
        let token = encode(
            &Header::new(Algorithm::HS256),
            &serde_json::json!({
                "sub": Uuid::new_v4().to_string(),
                "exp": FUTURE,
                "jti": "pre-deploy",
            }),
            &EncodingKey::from_secret(SECRET.as_bytes()),
        )
        .unwrap();
        assert_eq!(verify_state(SECRET, &token), Err(StateError::Invalid));
    }

    #[test]
    fn state_is_signed_with_the_oauth_state_derived_key() {
        let account = Uuid::new_v4();
        let token = sign_state(SECRET, account, FUTURE, "n", None, None).unwrap();
        let mut validation = Validation::new(Algorithm::HS256);
        validation.set_required_spec_claims(&["exp"]);
        let derived = crate::crypto::kdf::derive_key(
            SECRET.as_bytes(),
            crate::crypto::kdf::purpose::OAUTH_STATE,
        );
        assert!(decode::<StateClaims>(&token, &DecodingKey::from_secret(&derived), &validation).is_ok());
        assert!(
            decode::<StateClaims>(&token, &DecodingKey::from_secret(SECRET.as_bytes()), &validation)
                .is_err()
        );
    }

    #[test]
    fn meeting_return_state_round_trips_under_the_derived_key() {
        let account = Uuid::new_v4();
        let member = Uuid::new_v4();
        let token = sign_state_with_meeting_return(SECRET, account, member, "/meetings/x").unwrap();
        let v = verify_state(SECRET, &token).unwrap();
        assert_eq!(v.account_id, account);
        assert_eq!(v.user_id, Some(member));
        assert_eq!(v.meeting_return.as_deref(), Some("/meetings/x"));
    }

    // ── uid/sfc claims (gsuite Train G2 PR 1) ───────────────────────────────

    #[test]
    fn sign_then_verify_roundtrips_uid_and_surface() {
        let account = Uuid::new_v4();
        let user = Uuid::new_v4();
        let token = sign_state(SECRET, account, FUTURE, "n", Some(user), Some("gmail")).unwrap();
        let verified = verify_state(SECRET, &token).unwrap();
        assert_eq!(verified.user_id, Some(user));
        assert_eq!(verified.surface.as_deref(), Some("gmail"));
    }

    #[test]
    fn absent_uid_and_surface_verify_as_none() {
        // Every pre-PR mint (and every mint that doesn't request a specific
        // surface) omits both claims — `routes::oauth::callback` must see
        // exactly `(None, None)` for those, never a default that silently
        // guesses a surface or a user id.
        let account = Uuid::new_v4();
        let token = sign_state(SECRET, account, FUTURE, "n", None, None).unwrap();
        let verified = verify_state(SECRET, &token).unwrap();
        assert_eq!(verified.user_id, None);
        assert_eq!(verified.surface, None);
    }

    #[test]
    fn legacy_state_with_no_uid_sfc_fields_at_all_verifies_as_none() {
        // A state minted by the PRE-this-PR code path has no `uid`/`sfc` keys
        // in its JSON payload at all (not merely `null`) — `#[serde(default)]`
        // must still decode it as `(None, None)`, not fail to deserialize.
        #[derive(Serialize)]
        struct LegacyClaims {
            sub: String,
            exp: i64,
            jti: String,
        }
        let legacy = LegacyClaims {
            sub: Uuid::new_v4().to_string(),
            exp: FUTURE,
            jti: "legacy-nonce".into(),
        };
        let token = encode(
            &Header::new(Algorithm::HS256),
            &legacy,
            &EncodingKey::from_secret(&state_key(SECRET)),
        )
        .unwrap();
        let verified = verify_state(SECRET, &token).unwrap();
        assert_eq!(verified.user_id, None);
        assert_eq!(verified.surface, None);
    }

    #[test]
    fn malformed_uid_claim_verifies_as_none_not_a_hard_failure() {
        // Defensive: a present-but-non-UUID `uid` (should never happen from
        // our own minting) degrades to `None` rather than rejecting the
        // whole (validly signed, unexpired) token.
        #[derive(Serialize)]
        struct ClaimsWithBadUid {
            sub: String,
            exp: i64,
            jti: String,
            uid: String,
        }
        let claims = ClaimsWithBadUid {
            sub: Uuid::new_v4().to_string(),
            exp: FUTURE,
            jti: "n".into(),
            uid: "not-a-uuid".into(),
        };
        let token = encode(
            &Header::new(Algorithm::HS256),
            &claims,
            &EncodingKey::from_secret(&state_key(SECRET)),
        )
        .unwrap();
        let verified = verify_state(SECRET, &token).unwrap();
        assert_eq!(verified.user_id, None);
    }
}

pub fn sign_state_with_meeting_return(
    secret: &str,
    account: Uuid,
    member: Uuid,
    path: &str,
) -> Result<String, jsonwebtoken::errors::Error> {
    let claims = StateClaims {
        sub: account.to_string(),
        exp: chrono::Utc::now().timestamp() + STATE_TTL_SECS,
        jti: Uuid::new_v4().to_string(),
        uid: Some(member.to_string()),
        sfc: None,
        teams_onboarding: None,
        meeting_return: Some(path.into()),
        ret: None,
    };
    encode(
        &Header::new(Algorithm::HS256),
        &claims,
        &EncodingKey::from_secret(&state_key(secret)),
    )
}
