//! Team key cryptographic primitives.
//!
//! Provides generation and envelope wrapping/unwrapping of team symmetric keys.
//!
//! # Key hierarchy (team layer)
//! ```text
//! account DEK  (32 bytes, unwrapped from accounts.dek_wrapped at request time)
//!     └─ wraps ─▶  team key (TK, 32 bytes)
//!                     └─ stored as team_member_key_envelopes.wrapped_key
//! ```
//!
//! # Wire format for `wrapped_key`
//! The same compact layout used by [`crate::kek`]:
//! ```text
//! [nonce 12B][GCM tag 16B][ciphertext 32B]  =  60 bytes total
//! ```
//!
//! # Security notes
//! - Every call to [`wrap_team_key_for_member`] generates a fresh random nonce,
//!   so two wrappings of the same team key produce different bytes.
//! - Callers are responsible for zeroizing any raw `[u8; 32]` key material
//!   after use.  This module does not automatically zeroize key material on drop.

use crate::kek::{blob_to_bytes, bytes_to_blob};
use crate::crypto::{decrypt, encrypt};
use rand::RngCore;
use thiserror::Error;

// ---------------------------------------------------------------------------
// Error type
// ---------------------------------------------------------------------------

/// Errors arising from team key operations.
#[derive(Debug, Error)]
pub enum TeamCryptoError {
    #[error("wrap failed: {0}")]
    WrapFailed(String),

    #[error("unwrap failed — wrong key or corrupt data")]
    UnwrapFailed,
}

// ---------------------------------------------------------------------------
// Team key generation
// ---------------------------------------------------------------------------

/// Generate a fresh 32-byte random team key using the OS CSPRNG.
///
/// The returned value is **raw key material** — the caller must either wrap it
/// immediately with [`wrap_team_key_for_member`] or zeroize it once it is no
/// longer needed.
pub fn generate_team_key() -> [u8; 32] {
    let mut tk = [0u8; 32];
    rand::rngs::OsRng.fill_bytes(&mut tk);
    tk
}

// ---------------------------------------------------------------------------
// Wrap / unwrap
// ---------------------------------------------------------------------------

/// Wrap (encrypt) a team key with a member's DEK.
///
/// Returns the wrapped bytes (60 bytes: nonce || tag || ciphertext) suitable
/// for storing in `team_member_key_envelopes.wrapped_key`.
///
/// A fresh random nonce is generated on every call, so two calls with the same
/// inputs produce different ciphertext.
///
/// # Errors
/// Returns [`TeamCryptoError::WrapFailed`] if the AES-GCM operation fails
/// (should not happen in practice unless `member_dek` is malformed).
pub fn wrap_team_key_for_member(
    member_dek: &[u8; 32],
    team_key: &[u8; 32],
) -> Result<Vec<u8>, TeamCryptoError> {
    let blob = encrypt(team_key, member_dek)
        .map_err(|e| TeamCryptoError::WrapFailed(e.to_string()))?;

    Ok(blob_to_bytes(&blob))
}

/// Unwrap (decrypt) a team key using a member's DEK.
///
/// `wrapped` must be bytes previously produced by [`wrap_team_key_for_member`]
/// (60-byte nonce || tag || ciphertext layout).
///
/// # Errors
/// Returns [`TeamCryptoError::UnwrapFailed`] if the bytes are malformed, the
/// authentication tag does not verify, or the decrypted plaintext is not
/// 32 bytes (covers both wrong-key and corrupt-data scenarios).
pub fn unwrap_team_key(
    member_dek: &[u8; 32],
    wrapped: &[u8],
) -> Result<[u8; 32], TeamCryptoError> {
    let blob = bytes_to_blob(wrapped).ok_or(TeamCryptoError::UnwrapFailed)?;

    let plaintext = decrypt(&blob, member_dek).map_err(|_| TeamCryptoError::UnwrapFailed)?;

    if plaintext.len() != 32 {
        return Err(TeamCryptoError::UnwrapFailed);
    }

    let mut tk = [0u8; 32];
    tk.copy_from_slice(&plaintext);
    Ok(tk)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kek::WRAPPED_LEN;

    fn dek_a() -> [u8; 32] {
        let mut k = [0u8; 32];
        for (i, b) in k.iter_mut().enumerate() {
            *b = i as u8;
        }
        k
    }

    fn dek_b() -> [u8; 32] {
        let mut k = [0u8; 32];
        for (i, b) in k.iter_mut().enumerate() {
            *b = (255 - i) as u8;
        }
        k
    }

    // ── generate_team_key ────────────────────────────────────────────────────

    #[test]
    fn generate_team_key_produces_32_bytes() {
        let tk = generate_team_key();
        assert_eq!(tk.len(), 32);
    }

    #[test]
    fn generate_team_key_is_random() {
        let tk1 = generate_team_key();
        let tk2 = generate_team_key();
        assert_ne!(tk1, tk2, "two freshly generated team keys should differ");
    }

    // ── wrap / unwrap round-trip ─────────────────────────────────────────────

    #[test]
    fn wrap_unwrap_roundtrip() {
        let dek = dek_a();
        let tk = generate_team_key();

        let wrapped = wrap_team_key_for_member(&dek, &tk)
            .expect("wrap should succeed");

        let recovered = unwrap_team_key(&dek, &wrapped)
            .expect("unwrap should succeed");

        assert_eq!(tk, recovered, "recovered team key must match original");
    }

    #[test]
    fn wrapped_bytes_are_60_bytes() {
        let dek = dek_a();
        let tk = generate_team_key();
        let wrapped = wrap_team_key_for_member(&dek, &tk).unwrap();
        assert_eq!(
            wrapped.len(),
            WRAPPED_LEN,
            "wrapped team key must be {WRAPPED_LEN} bytes"
        );
    }

    #[test]
    fn wrap_produces_different_ciphertext_each_call() {
        let dek = dek_a();
        let tk = generate_team_key();

        let w1 = wrap_team_key_for_member(&dek, &tk).unwrap();
        let w2 = wrap_team_key_for_member(&dek, &tk).unwrap();

        assert_ne!(w1, w2, "fresh nonce must produce different ciphertext each call");
    }

    // ── wrong-key unwrap error ───────────────────────────────────────────────

    #[test]
    fn wrong_key_unwrap_returns_error() {
        let dek_correct = dek_a();
        let dek_wrong = dek_b();
        let tk = generate_team_key();

        let wrapped = wrap_team_key_for_member(&dek_correct, &tk).unwrap();
        let err = unwrap_team_key(&dek_wrong, &wrapped).unwrap_err();

        assert!(
            matches!(err, TeamCryptoError::UnwrapFailed),
            "expected UnwrapFailed with wrong DEK, got {err:?}"
        );
    }

    #[test]
    fn corrupt_wrapped_bytes_returns_error() {
        let dek = dek_a();
        let tk = generate_team_key();

        let mut wrapped = wrap_team_key_for_member(&dek, &tk).unwrap();
        // Flip a bit in the ciphertext region (bytes 28+).
        wrapped[30] ^= 0xFF;

        let err = unwrap_team_key(&dek, &wrapped).unwrap_err();
        assert!(
            matches!(err, TeamCryptoError::UnwrapFailed),
            "expected UnwrapFailed for corrupt data, got {err:?}"
        );
    }

    #[test]
    fn truncated_wrapped_bytes_returns_error() {
        let dek = dek_a();
        let err = unwrap_team_key(&dek, &[0u8; 10]).unwrap_err();
        assert!(
            matches!(err, TeamCryptoError::UnwrapFailed),
            "expected UnwrapFailed for truncated data, got {err:?}"
        );
    }

    // ── TeamCryptoError display ──────────────────────────────────────────────

    #[test]
    fn error_display_wrap_failed() {
        let e = TeamCryptoError::WrapFailed("test error".into());
        assert_eq!(e.to_string(), "wrap failed: test error");
    }

    #[test]
    fn error_display_unwrap_failed() {
        let e = TeamCryptoError::UnwrapFailed;
        assert_eq!(e.to_string(), "unwrap failed — wrong key or corrupt data");
    }
}
