//! Master Key Encryption Key (KEK) loading and DEK envelope operations.
//!
//! # Key hierarchy
//! ```text
//! MASTER_KEK_HEX  (env var, 64 hex chars = 32 bytes, lives in Fly.io secrets)
//!     └─ wraps ─▶  account DEK  (32 random bytes, stored as dek_wrapped BYTEA
//!                                 — control-plane `accounts.dek_wrapped` for
//!                                 personal accounts, tenant-DB `account_keys`
//!                                 for org accounts, org-tenancy PR 1.3)
//!                     └─ used ─▶  content encryption / decryption
//! ```
//!
//! # Rotation (`MASTER_KEK_HEX_NEXT`, org-tenancy PR 1.3)
//! Rotating the master KEK is a two-phase, zero-downtime deploy:
//! 1. Set `MASTER_KEK_HEX_NEXT` to the new key alongside the unchanged
//!    `MASTER_KEK_HEX`. Every unwrap now tries NEXT first, then CURRENT — a
//!    no-op for existing data (nothing is wrapped under NEXT yet), but it
//!    primes the whole fleet to accept the new key before anything is wrapped
//!    with it.
//! 2. Flip the two env vars (`MASTER_KEK_HEX` = the new key,
//!    `MASTER_KEK_HEX_NEXT` = the old key). Fresh wraps ([`wrap_dek`] always
//!    wraps with CURRENT) now use the new key; unwrap still tries NEXT
//!    (=old) first, so every DEK not yet individually re-wrapped keeps
//!    decrypting without a bulk re-wrap step.
//! 3. Per-account step: call `/internal/accounts/{id}/rotate-dek` (no
//!    `?force=` param) for each account. **Its default behavior — when a
//!    wrapped DEK already exists — is a REWRAP: it recovers the existing DEK
//!    bytes (via this same NEXT-then-CURRENT unwrap) and re-wraps the SAME
//!    bytes under CURRENT. The DEK itself never changes, so content stays
//!    decryptable — this is the correct operation for a KEK migration.**
//!    Do **not** pass `?force=true` here: that path mints a brand-new DEK
//!    instead and orphans all content encrypted under the old one — it exists
//!    for a deliberate key-compromise response, not routine KEK rotation.
//!    Once every account has been rewrapped, `MASTER_KEK_HEX_NEXT` can be
//!    dropped and the old key retired.
//!
//! # Security notes
//! - The KEK is loaded from `MASTER_KEK_HEX` (and, during rotation,
//!   `MASTER_KEK_HEX_NEXT`) at call time; callers may cache it but must
//!   **zeroize** the returned array after use.
//! - Wrapped DEK bytes are safe to store in the database; they are AES-256-GCM
//!   ciphertext and cannot be reversed without the KEK.
//! - **Audit (key-material-never-logged):** no type in this module derives
//!   `Debug`/`Display` over raw key bytes — [`KekError`] messages carry only
//!   the *length* of malformed hex input and the offending env var *name*,
//!   never its value; [`load_kek`]/[`load_kek_next`] never log on success or
//!   failure (callers that do log a `KekError` — e.g. `crypto::resolve_dek`,
//!   `routes::admin::rotate_dek` — only ever log the `Display` string above,
//!   which is key-material-free by construction). Raw `[u8; 32]` KEK/DEK
//!   arrays are never wrapped in a type that derives `Debug` anywhere they are
//!   held across an `await` or stored on a struct (see `dek_cache::CachedDek`,
//!   `tenancy::runtime::TenantContext`'s hand-written `Debug` impl that omits
//!   the pool).
//! - All crypto is delegated to [`crate::crypto`]; no new crypto primitives are
//!   introduced here.

use crate::crypto::{decrypt, encrypt, EncryptedBlob};
use thiserror::Error;

// ---------------------------------------------------------------------------
// Error type
// ---------------------------------------------------------------------------

/// Errors arising from KEK/DEK operations.
#[derive(Debug, Error)]
pub enum KekError {
    #[error("KEK env var missing or invalid: {0}")]
    EnvVar(String),

    #[error("DEK wrap failed: {0}")]
    WrapFailed(String),

    #[error("DEK unwrap failed (wrong key or corrupt data)")]
    UnwrapFailed,
}

// ---------------------------------------------------------------------------
// KEK loading
// ---------------------------------------------------------------------------

/// Decode and length-check a KEK hex string. Shared by [`load_kek`] and
/// [`load_kek_next`] so both env vars are held to the identical format rule.
/// The error message names `var_name` (safe — it's an env var *name*, never a
/// value) but never echoes `hex_val` itself.
fn decode_kek_hex(var_name: &str, hex_val: &str) -> Result<[u8; 32], KekError> {
    if hex_val.len() != 64 {
        return Err(KekError::EnvVar(format!(
            "{var_name}: expected 64 hex chars (32 bytes), got {} chars",
            hex_val.len()
        )));
    }

    let bytes = hex::decode(hex_val)
        .map_err(|e| KekError::EnvVar(format!("{var_name}: invalid hex: {e}")))?;

    // `hex::decode` of a 64-char string always yields 32 bytes, but be
    // explicit so a future refactor cannot silently truncate.
    if bytes.len() != 32 {
        return Err(KekError::EnvVar(format!(
            "{var_name}: decoded {} bytes, expected 32",
            bytes.len()
        )));
    }

    let mut key = [0u8; 32];
    key.copy_from_slice(&bytes);
    Ok(key)
}

/// Load the master KEK from the `MASTER_KEK_HEX` environment variable.
///
/// `MASTER_KEK_HEX` must be exactly 64 lowercase or uppercase hexadecimal
/// characters (32 bytes = 256-bit AES key). This is always the key used to
/// **wrap** fresh DEKs (see the module-level rotation note) and is tried
/// second (after `MASTER_KEK_HEX_NEXT`, when set) when **unwrapping**.
///
/// # Errors
/// Returns [`KekError::EnvVar`] if the variable is absent, empty, or contains
/// non-hex characters or a byte count other than 32.
pub fn load_kek() -> Result<[u8; 32], KekError> {
    let hex_val = std::env::var("MASTER_KEK_HEX")
        .map_err(|e| KekError::EnvVar(format!("MASTER_KEK_HEX not set: {e}")))?;
    decode_kek_hex("MASTER_KEK_HEX", &hex_val)
}

/// Load the rotation-in-progress KEK from `MASTER_KEK_HEX_NEXT`, if configured.
///
/// `Ok(None)` — the common case — means no rotation is in progress (the var is
/// simply unset or blank); this is deliberately **not** an error. `Err` only
/// when the variable IS set but malformed, so a typo'd rotation config fails
/// loudly (503 at the caller) rather than silently degrading to single-key
/// operation.
pub fn load_kek_next() -> Result<Option<[u8; 32]>, KekError> {
    match std::env::var("MASTER_KEK_HEX_NEXT") {
        Err(_) => Ok(None),
        Ok(v) if v.trim().is_empty() => Ok(None),
        Ok(v) => decode_kek_hex("MASTER_KEK_HEX_NEXT", &v).map(Some),
    }
}

// ---------------------------------------------------------------------------
// DEK generation
// ---------------------------------------------------------------------------

/// Generate a new random 256-bit (32-byte) data encryption key.
///
/// Uses the OS CSPRNG via [`aes_gcm::aead::OsRng`] (internally `rand::OsRng`).
/// The caller is responsible for zeroizing the returned array after use.
pub fn generate_dek() -> [u8; 32] {
    use rand::RngCore;
    let mut dek = [0u8; 32];
    rand::rngs::OsRng.fill_bytes(&mut dek);
    dek
}

// ---------------------------------------------------------------------------
// Wrap / unwrap
// ---------------------------------------------------------------------------

/// Wrap (encrypt) a DEK with the KEK.
///
/// Returns the serialised [`EncryptedBlob`] as a `Vec<u8>` (fixed-layout
/// binary) suitable for storing in the `accounts.dek_wrapped` BYTEA column.
///
/// Layout: [nonce 12B][tag 16B][ciphertext 32B] = 60 bytes total.
///
/// # Errors
/// Returns [`KekError::WrapFailed`] if AES-GCM encryption fails (should not
/// happen in practice unless the key is malformed).
pub fn wrap_dek(kek: &[u8; 32], dek: &[u8; 32]) -> Result<Vec<u8>, KekError> {
    let blob = encrypt(dek, kek)
        .map_err(|e| KekError::WrapFailed(e.to_string()))?;

    let wrapped = blob_to_bytes(&blob);
    Ok(wrapped)
}

/// Unwrap (decrypt) a previously wrapped DEK using the KEK.
///
/// `wrapped` must be bytes previously produced by [`wrap_dek`].
///
/// # Errors
/// - [`KekError::UnwrapFailed`] if the bytes are malformed, the authentication
///   tag does not verify, or the decrypted plaintext is not 32 bytes.
pub fn unwrap_dek(kek: &[u8; 32], wrapped: &[u8]) -> Result<[u8; 32], KekError> {
    let blob = bytes_to_blob(wrapped).ok_or(KekError::UnwrapFailed)?;

    let plaintext = decrypt(&blob, kek).map_err(|_| KekError::UnwrapFailed)?;

    if plaintext.len() != 32 {
        return Err(KekError::UnwrapFailed);
    }

    let mut dek = [0u8; 32];
    dek.copy_from_slice(&plaintext);
    Ok(dek)
}

/// Unwrap a DEK during a KEK rotation window: try `next` first (when
/// configured), falling back to `current`. AES-GCM's authentication tag makes
/// this safe — a wrong key never produces a plausible-looking wrong plaintext,
/// it simply fails to verify — so trying both keys can only ever succeed with
/// the one that actually wrapped `wrapped`.
///
/// Callers load `current`/`next` via [`load_kek`]/[`load_kek_next`]; when no
/// rotation is in progress (`next` is `None`) this is identical to calling
/// [`unwrap_dek`] with `current` alone.
///
/// # Errors
/// [`KekError::UnwrapFailed`] only when NEITHER key unwraps the bytes
/// successfully (the error from the `current`-key attempt is returned, since
/// `current` is authoritative when both are absent-of-match).
pub fn unwrap_dek_with_rotation(
    current: &[u8; 32],
    next: Option<&[u8; 32]>,
    wrapped: &[u8],
) -> Result<[u8; 32], KekError> {
    if let Some(next_key) = next {
        if let Ok(dek) = unwrap_dek(next_key, wrapped) {
            return Ok(dek);
        }
    }
    unwrap_dek(current, wrapped)
}

// ---------------------------------------------------------------------------
// Internal serialisation helpers
//
// Layout (fixed-size, no length-prefix needed because all fields are known):
//   bytes  0..12  — 96-bit nonce
//   bytes 12..28  — 128-bit GCM authentication tag
//   bytes 28..60  — 256-bit (32-byte) AES-GCM ciphertext of the DEK
//   total: 60 bytes
// ---------------------------------------------------------------------------

pub(crate) const WRAPPED_LEN: usize = 12 + 16 + 32; // 60 bytes

pub(crate) fn blob_to_bytes(blob: &EncryptedBlob) -> Vec<u8> {
    let mut out = Vec::with_capacity(WRAPPED_LEN);
    out.extend_from_slice(&blob.nonce);
    out.extend_from_slice(&blob.tag);
    out.extend_from_slice(&blob.ciphertext);
    out
}

pub(crate) fn bytes_to_blob(bytes: &[u8]) -> Option<EncryptedBlob> {
    if bytes.len() != WRAPPED_LEN {
        return None;
    }
    let mut nonce = [0u8; 12];
    nonce.copy_from_slice(&bytes[0..12]);

    let mut tag = [0u8; 16];
    tag.copy_from_slice(&bytes[12..28]);

    let ciphertext = bytes[28..60].to_vec();

    Some(EncryptedBlob {
        ciphertext,
        nonce,
        tag,
    })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // ── Helpers ───────────────────────────────────────────────────────────────

    static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    fn set_kek_env(hex: &str) {
        unsafe { std::env::set_var("MASTER_KEK_HEX", hex) };
    }

    fn remove_kek_env() {
        unsafe { std::env::remove_var("MASTER_KEK_HEX") };
    }

    fn set_kek_next_env(hex: &str) {
        unsafe { std::env::set_var("MASTER_KEK_HEX_NEXT", hex) };
    }

    fn remove_kek_next_env() {
        unsafe { std::env::remove_var("MASTER_KEK_HEX_NEXT") };
    }

    fn valid_kek_hex() -> String {
        // 32 bytes of 0x42
        "42".repeat(32)
    }

    fn valid_kek_bytes() -> [u8; 32] {
        [0x42u8; 32]
    }

    // ── load_kek ──────────────────────────────────────────────────────────────

    #[test]
    fn load_kek_succeeds_with_valid_hex() {
        let _guard = ENV_LOCK.lock().unwrap();
        set_kek_env(&valid_kek_hex());
        let kek = load_kek().expect("should load valid KEK");
        assert_eq!(kek, valid_kek_bytes());
        remove_kek_env();
    }

    #[test]
    fn load_kek_fails_when_env_missing() {
        let _guard = ENV_LOCK.lock().unwrap();
        remove_kek_env();
        let err = load_kek().unwrap_err();
        assert!(
            matches!(err, KekError::EnvVar(_)),
            "expected EnvVar error, got {err:?}"
        );
    }

    #[test]
    fn load_kek_fails_with_too_short_hex() {
        let _guard = ENV_LOCK.lock().unwrap();
        set_kek_env("deadbeef"); // only 4 bytes, 8 hex chars
        let err = load_kek().unwrap_err();
        assert!(
            matches!(err, KekError::EnvVar(_)),
            "expected EnvVar error for short hex, got {err:?}"
        );
        remove_kek_env();
    }

    #[test]
    fn load_kek_fails_with_invalid_hex_chars() {
        let _guard = ENV_LOCK.lock().unwrap();
        // 64 chars but contains non-hex 'z'
        let bad = "zz".repeat(32);
        set_kek_env(&bad);
        let err = load_kek().unwrap_err();
        assert!(
            matches!(err, KekError::EnvVar(_)),
            "expected EnvVar error for invalid hex, got {err:?}"
        );
        remove_kek_env();
    }

    #[test]
    fn load_kek_fails_with_odd_length_hex() {
        let _guard = ENV_LOCK.lock().unwrap();
        let odd = "a".repeat(63);
        set_kek_env(&odd);
        let err = load_kek().unwrap_err();
        assert!(
            matches!(err, KekError::EnvVar(_)),
            "expected EnvVar error for odd-length hex, got {err:?}"
        );
        remove_kek_env();
    }

    // ── generate_dek ─────────────────────────────────────────────────────────

    #[test]
    fn generate_dek_produces_32_bytes() {
        let dek = generate_dek();
        assert_eq!(dek.len(), 32);
    }

    #[test]
    fn generate_dek_is_random() {
        let dek1 = generate_dek();
        let dek2 = generate_dek();
        // With overwhelming probability two random 32-byte values differ.
        assert_ne!(dek1, dek2, "two DEKs should be distinct");
    }

    // ── wrap_dek / unwrap_dek round-trip ─────────────────────────────────────


    #[test]
    fn wrap_unwrap_roundtrip() {
        let kek = valid_kek_bytes();
        let dek = generate_dek();

        let wrapped = wrap_dek(&kek, &dek).expect("wrap should succeed");
        let recovered = unwrap_dek(&kek, &wrapped).expect("unwrap should succeed");

        assert_eq!(dek, recovered, "recovered DEK must match original");
    }

    #[test]
    fn wrapped_bytes_are_fixed_60_bytes() {
        let kek = valid_kek_bytes();
        let dek = generate_dek();
        let wrapped = wrap_dek(&kek, &dek).unwrap();
        assert_eq!(
            wrapped.len(),
            WRAPPED_LEN,
            "wrapped DEK must be {WRAPPED_LEN} bytes"
        );
    }

    #[test]
    fn wrap_produces_different_ciphertext_each_call() {
        let kek = valid_kek_bytes();
        let dek = generate_dek();

        let w1 = wrap_dek(&kek, &dek).unwrap();
        let w2 = wrap_dek(&kek, &dek).unwrap();

        assert_ne!(w1, w2, "two wraps of the same DEK should differ (random nonce)");
    }

    #[test]
    fn unwrap_with_wrong_key_returns_error() {
        let kek = valid_kek_bytes();
        let wrong_kek = [0x99u8; 32];
        let dek = generate_dek();

        let wrapped = wrap_dek(&kek, &dek).unwrap();
        let err = unwrap_dek(&wrong_kek, &wrapped).unwrap_err();

        assert!(
            matches!(err, KekError::UnwrapFailed),
            "expected UnwrapFailed with wrong key, got {err:?}"
        );
    }

    #[test]
    fn unwrap_corrupt_data_returns_error() {
        let kek = valid_kek_bytes();
        let dek = generate_dek();

        let mut wrapped = wrap_dek(&kek, &dek).unwrap();
        // Flip a byte in the ciphertext portion (bytes 28+).
        wrapped[30] ^= 0xFF;

        let err = unwrap_dek(&kek, &wrapped).unwrap_err();
        assert!(
            matches!(err, KekError::UnwrapFailed),
            "expected UnwrapFailed for corrupt data, got {err:?}"
        );
    }

    #[test]
    fn unwrap_truncated_data_returns_error() {
        let kek = valid_kek_bytes();
        let err = unwrap_dek(&kek, &[0u8; 10]).unwrap_err();
        assert!(
            matches!(err, KekError::UnwrapFailed),
            "expected UnwrapFailed for truncated data, got {err:?}"
        );
    }

    // ── load_kek_next ─────────────────────────────────────────────────────────

    #[test]
    fn load_kek_next_absent_is_ok_none() {
        let _guard = ENV_LOCK.lock().unwrap();
        remove_kek_next_env();
        assert_eq!(load_kek_next().expect("absent NEXT is not an error"), None);
    }

    #[test]
    fn load_kek_next_blank_is_ok_none() {
        let _guard = ENV_LOCK.lock().unwrap();
        set_kek_next_env("   ");
        assert_eq!(load_kek_next().expect("blank NEXT is not an error"), None);
        remove_kek_next_env();
    }

    #[test]
    fn load_kek_next_valid_hex_returns_some() {
        let _guard = ENV_LOCK.lock().unwrap();
        set_kek_next_env(&"7a".repeat(32));
        assert_eq!(load_kek_next().unwrap(), Some([0x7au8; 32]));
        remove_kek_next_env();
    }

    #[test]
    fn load_kek_next_malformed_is_a_loud_error_not_a_silent_none() {
        let _guard = ENV_LOCK.lock().unwrap();
        set_kek_next_env("not-hex-at-all");
        assert!(
            matches!(load_kek_next(), Err(KekError::EnvVar(_))),
            "a configured-but-malformed MASTER_KEK_HEX_NEXT must error, not degrade to None"
        );
        remove_kek_next_env();
    }

    // ── unwrap_dek_with_rotation ──────────────────────────────────────────────

    #[test]
    fn rotation_unwrap_falls_back_to_current_when_next_absent() {
        let current = valid_kek_bytes();
        let dek = generate_dek();
        let wrapped = wrap_dek(&current, &dek).unwrap();

        let recovered = unwrap_dek_with_rotation(&current, None, &wrapped).unwrap();
        assert_eq!(recovered, dek);
    }

    #[test]
    fn rotation_unwrap_recovers_data_wrapped_under_next() {
        // Simulates phase 2 of a rotation: CURRENT is the new key (nothing yet
        // wrapped with it for this DEK), NEXT is the old key (what actually
        // wrapped the existing data).
        let old_key = [0x11u8; 32];
        let new_key = [0x22u8; 32];
        let dek = generate_dek();
        let wrapped_under_old = wrap_dek(&old_key, &dek).unwrap();

        let recovered =
            unwrap_dek_with_rotation(&new_key, Some(&old_key), &wrapped_under_old).unwrap();
        assert_eq!(recovered, dek, "must recover data wrapped under the NEXT key");
    }

    #[test]
    fn rotation_unwrap_recovers_data_wrapped_under_the_new_current() {
        // Phase 2, fresh data: wrapped with CURRENT (the new key) after the
        // flip; NEXT (the retiring old key) must not be required.
        let old_key = [0x11u8; 32];
        let new_key = [0x22u8; 32];
        let dek = generate_dek();
        let wrapped_under_new = wrap_dek(&new_key, &dek).unwrap();

        let recovered =
            unwrap_dek_with_rotation(&new_key, Some(&old_key), &wrapped_under_new).unwrap();
        assert_eq!(recovered, dek);
    }

    #[test]
    fn rotation_unwrap_fails_when_neither_key_matches() {
        let current = [0x11u8; 32];
        let next = [0x22u8; 32];
        let unrelated = [0x33u8; 32];
        let dek = generate_dek();
        let wrapped = wrap_dek(&unrelated, &dek).unwrap();

        let err = unwrap_dek_with_rotation(&current, Some(&next), &wrapped).unwrap_err();
        assert!(matches!(err, KekError::UnwrapFailed));
    }
}
