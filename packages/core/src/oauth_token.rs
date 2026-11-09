//! OAuth token encryption/decryption helpers.
//!
//! Tokens are encrypted with AES-256-GCM using the account's Data Encryption
//! Key (DEK).  The wire format stored in Postgres BYTEA columns is:
//!
//! ```text
//! [ 12-byte nonce ][ ciphertext ][ 16-byte GCM tag ]
//! ```
//!
//! This is a flat byte sequence — no JSON envelope — so serialisation is
//! simple and the column stays compact.
//!
//! # Security notes
//! - A fresh random nonce is generated per `encrypt_token` call so the same
//!   DEK can safely protect many tokens.
//! - `decrypt_token` returns [`OauthTokenError::Decrypt`] on any tag mismatch
//!   or length error — never a detailed crypto message — to avoid oracle leaks.

use crate::crypto::{decrypt, encrypt, EncryptedBlob};
use thiserror::Error;

// ---------------------------------------------------------------------------
// Error type
// ---------------------------------------------------------------------------

/// Errors that can arise from OAuth token encryption / decryption.
#[derive(Debug, Error)]
pub enum OauthTokenError {
    #[error("encrypt failed: {0}")]
    Encrypt(String),

    #[error("decrypt failed")]
    Decrypt,

    #[error("invalid utf-8")]
    Utf8,
}

// ---------------------------------------------------------------------------
// Wire-format encode / decode (nonce || ciphertext || tag)
// ---------------------------------------------------------------------------

/// Serialise an [`EncryptedBlob`] into the flat byte layout stored in BYTEA:
/// `nonce (12) || ciphertext (variable) || tag (16)`.
fn blob_to_bytes(blob: &EncryptedBlob) -> Vec<u8> {
    let mut out = Vec::with_capacity(12 + blob.ciphertext.len() + 16);
    out.extend_from_slice(&blob.nonce);
    out.extend_from_slice(&blob.ciphertext);
    out.extend_from_slice(&blob.tag);
    out
}

/// Parse the flat byte layout back into an [`EncryptedBlob`].
///
/// Returns `None` if `bytes` is shorter than the minimum 28 bytes
/// (12 nonce + 0 ciphertext + 16 tag).
fn bytes_to_blob(bytes: &[u8]) -> Option<EncryptedBlob> {
    // minimum: 12-byte nonce + 16-byte tag with zero-length ciphertext = 28
    if bytes.len() < 28 {
        return None;
    }

    let mut nonce = [0u8; 12];
    nonce.copy_from_slice(&bytes[..12]);

    let body = &bytes[12..];
    let (ciphertext_bytes, tag_bytes) = body.split_at(body.len() - 16);

    let mut tag = [0u8; 16];
    tag.copy_from_slice(tag_bytes);

    Some(EncryptedBlob {
        ciphertext: ciphertext_bytes.to_vec(),
        nonce,
        tag,
    })
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Encrypt an OAuth token string with the account DEK.
///
/// Returns the ciphertext as a flat byte vector in the format
/// `nonce (12) || ciphertext || tag (16)` suitable for storing in a Postgres
/// `BYTEA` column.
///
/// A fresh random nonce is generated per call.
pub fn encrypt_token(dek: &[u8; 32], token: &str) -> Result<Vec<u8>, OauthTokenError> {
    let blob =
        encrypt(token.as_bytes(), dek).map_err(|e| OauthTokenError::Encrypt(e.to_string()))?;

    Ok(blob_to_bytes(&blob))
}

/// Decrypt an OAuth token previously encrypted with [`encrypt_token`].
///
/// `encrypted` must be in the `nonce (12) || ciphertext || tag (16)` format
/// produced by [`encrypt_token`].
///
/// Returns [`OauthTokenError::Decrypt`] for any authentication failure,
/// including wrong key, corrupted ciphertext, or malformed input.
pub fn decrypt_token(dek: &[u8; 32], encrypted: &[u8]) -> Result<String, OauthTokenError> {
    let blob = bytes_to_blob(encrypted).ok_or(OauthTokenError::Decrypt)?;

    let plaintext_bytes = decrypt(&blob, dek).map_err(|_| OauthTokenError::Decrypt)?;

    String::from_utf8(plaintext_bytes).map_err(|_| OauthTokenError::Utf8)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn test_dek() -> [u8; 32] {
        let mut k = [0u8; 32];
        for (i, b) in k.iter_mut().enumerate() {
            *b = i as u8;
        }
        k
    }

    fn other_dek() -> [u8; 32] {
        let mut k = [0u8; 32];
        for (i, b) in k.iter_mut().enumerate() {
            *b = (i + 128) as u8;
        }
        k
    }

    // ── Round-trip ────────────────────────────────────────────────────────────

    #[test]
    fn roundtrip_access_token() {
        let dek = test_dek();
        let token = "xoxb-1234567890-abcdefghij";

        let encrypted = encrypt_token(&dek, token).expect("encrypt should succeed");
        let recovered = decrypt_token(&dek, &encrypted).expect("decrypt should succeed");

        assert_eq!(recovered, token);
    }

    #[test]
    fn roundtrip_empty_token() {
        let dek = test_dek();
        let token = "";

        let encrypted = encrypt_token(&dek, token).expect("encrypt empty string should succeed");
        let recovered = decrypt_token(&dek, &encrypted).expect("decrypt should succeed");

        assert_eq!(recovered, token);
    }

    #[test]
    fn roundtrip_long_token() {
        let dek = test_dek();
        let token = "a".repeat(2048);

        let encrypted = encrypt_token(&dek, &token).expect("encrypt long token should succeed");
        let recovered = decrypt_token(&dek, &encrypted).expect("decrypt should succeed");

        assert_eq!(recovered, token);
    }

    // ── Each call produces a different ciphertext (random nonce) ─────────────

    #[test]
    fn each_encrypt_produces_unique_ciphertext() {
        let dek = test_dek();
        let token = "xoxb-same-token";

        let enc1 = encrypt_token(&dek, token).unwrap();
        let enc2 = encrypt_token(&dek, token).unwrap();

        // Different nonces means different ciphertexts even for the same input.
        assert_ne!(enc1, enc2, "two encryptions of the same token must differ");
    }

    // ── Wrong key returns Decrypt error ───────────────────────────────────────

    #[test]
    fn wrong_key_returns_decrypt_error() {
        let dek = test_dek();
        let bad_dek = other_dek();

        let encrypted = encrypt_token(&dek, "secret_token").unwrap();
        let err = decrypt_token(&bad_dek, &encrypted).unwrap_err();

        assert!(
            matches!(err, OauthTokenError::Decrypt),
            "expected Decrypt error with wrong key, got: {err:?}"
        );
    }

    // ── Truncated / corrupted bytes return Decrypt error ─────────────────────

    #[test]
    fn too_short_bytes_returns_decrypt_error() {
        let dek = test_dek();
        // 27 bytes < minimum 28 (12 nonce + 16 tag with zero ciphertext)
        let short = vec![0u8; 27];
        let err = decrypt_token(&dek, &short).unwrap_err();

        assert!(
            matches!(err, OauthTokenError::Decrypt),
            "expected Decrypt error for too-short input, got: {err:?}"
        );
    }

    #[test]
    fn corrupted_ciphertext_returns_decrypt_error() {
        let dek = test_dek();
        let token = "xoxb-1234567890";

        let mut encrypted = encrypt_token(&dek, token).unwrap();
        // Flip a byte in the ciphertext region (after the 12-byte nonce).
        encrypted[15] ^= 0xFF;

        let err = decrypt_token(&dek, &encrypted).unwrap_err();
        assert!(
            matches!(err, OauthTokenError::Decrypt),
            "expected Decrypt error for corrupted ciphertext, got: {err:?}"
        );
    }

    // ── Wire-format layout ────────────────────────────────────────────────────

    #[test]
    fn encrypted_bytes_have_expected_length() {
        let dek = test_dek();
        let token = "hello";

        let encrypted = encrypt_token(&dek, token).unwrap();
        // nonce(12) + ciphertext(len("hello")=5) + tag(16) = 33
        assert_eq!(encrypted.len(), 33, "wire-format length mismatch");
    }

    // ── Error display ─────────────────────────────────────────────────────────

    #[test]
    fn error_display_encrypt() {
        let msg = OauthTokenError::Encrypt("key init failed".into()).to_string();
        assert!(msg.contains("encrypt failed"), "unexpected message: {msg}");
        assert!(msg.contains("key init failed"), "unexpected message: {msg}");
    }

    #[test]
    fn error_display_decrypt() {
        let msg = OauthTokenError::Decrypt.to_string();
        assert_eq!(msg, "decrypt failed");
    }

    #[test]
    fn error_display_utf8() {
        let msg = OauthTokenError::Utf8.to_string();
        assert_eq!(msg, "invalid utf-8");
    }
}
