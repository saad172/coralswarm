//! Cryptographic primitives for Coralswarm.
//!
//! Provides AES-256-GCM symmetric encryption and an envelope key system for
//! the 3-layer key hierarchy (master → account key → graph key).
//!
//! # Security notes
//! - Nonces are always randomly generated (96-bit), never counter-based, because
//!   the same wrapping key is reused across many blobs.
//! - Raw key material in `[u8; 32]` arrays is NOT automatically zeroized by this
//!   module — callers are responsible for zeroizing any raw key returned by
//!   [`EnvelopeKey::generate`] or [`EnvelopeKey::unwrap`] once it is no longer
//!   needed.  Use the `zeroize` crate's [`Zeroize::zeroize`] method.
//!
//! # TODO: cargo fuzz target in fuzz/

use aes_gcm::{
    aead::{Aead, KeyInit, OsRng},
    Aes256Gcm, Nonce,
};
use rand::RngCore;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use zeroize::{Zeroize, ZeroizeOnDrop};

// ---------------------------------------------------------------------------
// Error type
// ---------------------------------------------------------------------------

/// Errors that can arise from cryptographic operations.
#[derive(Debug, Error)]
pub enum CryptoError {
    #[error("authentication tag mismatch — ciphertext may be corrupted or key is wrong")]
    TagMismatch,

    #[error("decryption failed: {0}")]
    DecryptFailed(String),

    #[error("key generation failed: {0}")]
    KeyGenFailed(String),
}

// ---------------------------------------------------------------------------
// EncryptedBlob
// ---------------------------------------------------------------------------

/// An AES-256-GCM ciphertext together with its nonce and authentication tag.
///
/// The tag is stored separately so that the shape is explicit and reviewable,
/// even though `aes-gcm` appends it to the ciphertext internally.  On
/// serialization round-trips the tag bytes are always present.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EncryptedBlob {
    /// The ciphertext bytes (same length as the original plaintext).
    pub ciphertext: Vec<u8>,
    /// 96-bit (12-byte) random nonce used during encryption.
    pub nonce: [u8; 12],
    /// 128-bit (16-byte) GCM authentication tag.
    pub tag: [u8; 16],
}

// ---------------------------------------------------------------------------
// Symmetric encrypt / decrypt
// ---------------------------------------------------------------------------

/// Encrypt `plaintext` with AES-256-GCM using `key`.
///
/// A fresh random 96-bit nonce is generated for every call, so the same key
/// can safely be used for multiple messages.
pub fn encrypt(plaintext: &[u8], key: &[u8; 32]) -> Result<EncryptedBlob, CryptoError> {
    let cipher = Aes256Gcm::new_from_slice(key)
        .map_err(|e| CryptoError::KeyGenFailed(e.to_string()))?;

    // Generate a random 96-bit nonce.
    let mut nonce_bytes = [0u8; 12];
    OsRng.fill_bytes(&mut nonce_bytes);
    let nonce = Nonce::from(nonce_bytes);

    // `aes-gcm` appends the 16-byte tag to the ciphertext.
    let ciphertext_with_tag = cipher
        .encrypt(&nonce, plaintext)
        .map_err(|_| CryptoError::DecryptFailed("encryption failed".into()))?;

    // Split tag from ciphertext.
    let (ciphertext_bytes, tag_bytes) =
        ciphertext_with_tag.split_at(ciphertext_with_tag.len() - 16);

    let mut tag = [0u8; 16];
    tag.copy_from_slice(tag_bytes);

    Ok(EncryptedBlob {
        ciphertext: ciphertext_bytes.to_vec(),
        nonce: nonce_bytes,
        tag,
    })
}

/// Decrypt `blob` with AES-256-GCM using `key`.
///
/// Returns [`CryptoError::TagMismatch`] if the authentication tag does not
/// verify, which covers both wrong-key and corrupted-ciphertext scenarios.
pub fn decrypt(blob: &EncryptedBlob, key: &[u8; 32]) -> Result<Vec<u8>, CryptoError> {
    let cipher = Aes256Gcm::new_from_slice(key)
        .map_err(|e| CryptoError::KeyGenFailed(e.to_string()))?;

    let nonce = Nonce::from(blob.nonce);

    // Reassemble ciphertext + tag for aes-gcm.
    let mut ciphertext_with_tag = blob.ciphertext.clone();
    ciphertext_with_tag.extend_from_slice(&blob.tag);

    // Guard against underflow: aes-gcm output is always at least 16 bytes
    // (the tag). A shorter buffer indicates a corrupted or truncated blob.
    if ciphertext_with_tag.len() < 16 {
        return Err(CryptoError::DecryptFailed(
            "ciphertext too short (< 16 bytes)".into(),
        ));
    }

    cipher
        .decrypt(&nonce, ciphertext_with_tag.as_slice())
        .map_err(|_| CryptoError::TagMismatch)
}

/// [`encrypt`] with associated data: `aad` is authenticated but not
/// encrypted, so the blob only decrypts under [`decrypt_with_aad`] with the
/// SAME `aad`. Binds a ciphertext to the context it was written for (e.g. the
/// row key it is stored under), so a blob copied to another context fails the
/// tag check instead of decrypting there.
pub fn encrypt_with_aad(plaintext: &[u8], key: &[u8; 32], aad: &[u8]) -> Result<EncryptedBlob, CryptoError> {
    use aes_gcm::aead::Payload;
    let cipher = Aes256Gcm::new_from_slice(key)
        .map_err(|e| CryptoError::KeyGenFailed(e.to_string()))?;
    let mut nonce_bytes = [0u8; 12];
    OsRng.fill_bytes(&mut nonce_bytes);
    let nonce = Nonce::from(nonce_bytes);
    let ciphertext_with_tag = cipher
        .encrypt(&nonce, Payload { msg: plaintext, aad })
        .map_err(|_| CryptoError::DecryptFailed("encryption failed".into()))?;
    let (ciphertext_bytes, tag_bytes) =
        ciphertext_with_tag.split_at(ciphertext_with_tag.len() - 16);
    let mut tag = [0u8; 16];
    tag.copy_from_slice(tag_bytes);
    Ok(EncryptedBlob {
        ciphertext: ciphertext_bytes.to_vec(),
        nonce: nonce_bytes,
        tag,
    })
}

/// [`decrypt`] for a blob written by [`encrypt_with_aad`]. A different `aad`
/// (or none) is [`CryptoError::TagMismatch`], exactly like a wrong key.
pub fn decrypt_with_aad(blob: &EncryptedBlob, key: &[u8; 32], aad: &[u8]) -> Result<Vec<u8>, CryptoError> {
    use aes_gcm::aead::Payload;
    let cipher = Aes256Gcm::new_from_slice(key)
        .map_err(|e| CryptoError::KeyGenFailed(e.to_string()))?;
    let nonce = Nonce::from(blob.nonce);
    let mut ciphertext_with_tag = blob.ciphertext.clone();
    ciphertext_with_tag.extend_from_slice(&blob.tag);
    cipher
        .decrypt(&nonce, Payload { msg: ciphertext_with_tag.as_slice(), aad })
        .map_err(|_| CryptoError::TagMismatch)
}

// ---------------------------------------------------------------------------
// Envelope key system
// ---------------------------------------------------------------------------

/// A data key (AES-256) that is stored only in encrypted form, wrapped under
/// a parent key.
///
/// Coralswarm uses a 3-layer hierarchy:
/// - **Master key** (`[u8; 32]`, kept in env/HSM, not managed here)
/// - **Account key** = `EnvelopeKey` wrapped under the master key
/// - **Graph key** = `EnvelopeKey` wrapped under the account key
///
/// Raw key material (`[u8; 32]`) returned by [`Self::generate`] and
/// [`Self::unwrap`] MUST be zeroized by the caller once it is no longer needed.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnvelopeKey {
    /// The wrapped (encrypted) key material.
    pub encrypted_key: EncryptedBlob,
}

/// Temporary holder for a raw 32-byte key that zeroizes on drop.
#[derive(Zeroize, ZeroizeOnDrop)]
struct RawKey([u8; 32]);

impl EnvelopeKey {
    /// Generate a new random 256-bit data key and wrap it under `wrapping_key`.
    ///
    /// Returns `(EnvelopeKey, raw_key)`.  The caller should use `raw_key` for
    /// any immediate encrypt/decrypt operation and then **zeroize it** — it
    /// will not be recoverable without calling [`Self::unwrap`] with the
    /// correct `wrapping_key`.
    pub fn generate(wrapping_key: &[u8; 32]) -> Result<(Self, [u8; 32]), CryptoError> {
        let mut raw = RawKey([0u8; 32]);
        OsRng.fill_bytes(&mut raw.0);

        let encrypted_key = encrypt(&raw.0, wrapping_key)?;

        let raw_out = raw.0;
        // `raw` is dropped (and zeroized) here.
        Ok((EnvelopeKey { encrypted_key }, raw_out))
    }

    /// Unwrap (decrypt) the data key using `wrapping_key`.
    ///
    /// Returns the raw 256-bit key.  The caller MUST zeroize this value once
    /// it is no longer needed.
    pub fn unwrap(&self, wrapping_key: &[u8; 32]) -> Result<[u8; 32], CryptoError> {
        let plaintext = decrypt(&self.encrypted_key, wrapping_key)?;

        if plaintext.len() != 32 {
            return Err(CryptoError::DecryptFailed(format!(
                "expected 32-byte key, got {}",
                plaintext.len()
            )));
        }

        let mut key = [0u8; 32];
        key.copy_from_slice(&plaintext);
        Ok(key)
    }

    /// Re-encrypt this envelope under a new wrapping key.
    ///
    /// The old key material is decrypted, re-encrypted under `new_wrapping_key`,
    /// and the original `EnvelopeKey` is consumed.  The raw key is zeroized
    /// immediately after re-encryption.
    pub fn rotate(
        self,
        old_wrapping_key: &[u8; 32],
        new_wrapping_key: &[u8; 32],
    ) -> Result<Self, CryptoError> {
        let plaintext = decrypt(&self.encrypted_key, old_wrapping_key)?;

        if plaintext.len() != 32 {
            return Err(CryptoError::DecryptFailed(format!(
                "expected 32-byte key, got {}",
                plaintext.len()
            )));
        }

        let mut raw = RawKey([0u8; 32]);
        raw.0.copy_from_slice(&plaintext);

        let new_encrypted = encrypt(&raw.0, new_wrapping_key)?;
        // `raw` is dropped (and zeroized) here.
        Ok(EnvelopeKey {
            encrypted_key: new_encrypted,
        })
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn aad_binds_the_ciphertext_to_its_context() {
        let key = test_key();
        let blob = encrypt_with_aad(b"secret", &key, b"ctx:A").unwrap();
        assert_eq!(decrypt_with_aad(&blob, &key, b"ctx:A").unwrap(), b"secret");
        assert!(matches!(decrypt_with_aad(&blob, &key, b"ctx:B"), Err(CryptoError::TagMismatch)));
        assert!(decrypt(&blob, &key).is_err(), "no-AAD decrypt must not open an AAD blob");
        let plain = encrypt(b"secret", &key).unwrap();
        assert!(decrypt_with_aad(&plain, &key, b"ctx:A").is_err());
    }

    fn test_key() -> [u8; 32] {
        let mut k = [0u8; 32];
        for (i, b) in k.iter_mut().enumerate() {
            *b = i as u8;
        }
        k
    }

    fn other_key() -> [u8; 32] {
        let mut k = [0u8; 32];
        for (i, b) in k.iter_mut().enumerate() {
            *b = (i + 128) as u8;
        }
        k
    }

    // ------------------------------------------------------------------
    // Symmetric encrypt / decrypt
    // ------------------------------------------------------------------

    #[test]
    fn roundtrip_encrypt_decrypt() {
        let key = test_key();
        let plaintext = b"hello, coralswarm!";

        let blob = encrypt(plaintext, &key).expect("encrypt should succeed");
        let recovered = decrypt(&blob, &key).expect("decrypt should succeed");

        assert_eq!(recovered, plaintext);
    }

    #[test]
    fn wrong_key_returns_tag_mismatch() {
        let key = test_key();
        let bad_key = other_key();
        let blob = encrypt(b"secret data", &key).unwrap();

        let err = decrypt(&blob, &bad_key).unwrap_err();
        assert!(
            matches!(err, CryptoError::TagMismatch),
            "expected TagMismatch, got {err:?}"
        );
    }

    #[test]
    fn corrupted_ciphertext_returns_tag_mismatch() {
        let key = test_key();
        let mut blob = encrypt(b"important secret", &key).unwrap();

        // Flip a byte in the ciphertext.
        blob.ciphertext[0] ^= 0xFF;

        let err = decrypt(&blob, &key).unwrap_err();
        assert!(
            matches!(err, CryptoError::TagMismatch),
            "expected TagMismatch, got {err:?}"
        );
    }

    #[test]
    fn corrupted_tag_returns_tag_mismatch() {
        let key = test_key();
        let mut blob = encrypt(b"important secret", &key).unwrap();

        // Flip a byte in the tag.
        blob.tag[0] ^= 0x01;

        let err = decrypt(&blob, &key).unwrap_err();
        assert!(
            matches!(err, CryptoError::TagMismatch),
            "expected TagMismatch, got {err:?}"
        );
    }

    #[test]
    fn each_encrypt_produces_different_nonce() {
        let key = test_key();
        let pt = b"same plaintext";

        let blob1 = encrypt(pt, &key).unwrap();
        let blob2 = encrypt(pt, &key).unwrap();

        // Nonces must differ (with overwhelming probability).
        assert_ne!(blob1.nonce, blob2.nonce);
    }

    // ------------------------------------------------------------------
    // Envelope key system
    // ------------------------------------------------------------------

    #[test]
    fn envelope_roundtrip() {
        let master = test_key();
        let (envelope, raw_key) = EnvelopeKey::generate(&master).expect("generate should succeed");

        let recovered = envelope.unwrap(&master).expect("unwrap should succeed");
        assert_eq!(raw_key, recovered);

        // Verify the raw key actually works for symmetric encryption.
        let blob = encrypt(b"graph data", &recovered).unwrap();
        let pt = decrypt(&blob, &recovered).unwrap();
        assert_eq!(pt, b"graph data");
    }

    #[test]
    fn envelope_wrong_wrapping_key_fails() {
        let master = test_key();
        let bad_master = other_key();

        let (envelope, _) = EnvelopeKey::generate(&master).unwrap();
        let err = envelope.unwrap(&bad_master).unwrap_err();

        assert!(
            matches!(err, CryptoError::TagMismatch),
            "expected TagMismatch, got {err:?}"
        );
    }

    #[test]
    fn envelope_key_rotation() {
        let old_master = test_key();
        let new_master = other_key();

        let (envelope, original_raw) = EnvelopeKey::generate(&old_master).unwrap();

        // Rotate to new master.
        let rotated = envelope
            .rotate(&old_master, &new_master)
            .expect("rotate should succeed");

        // New master unwraps successfully and gives the same underlying key.
        let recovered = rotated.unwrap(&new_master).expect("unwrap with new key should work");
        assert_eq!(original_raw, recovered);

        // Old master no longer works on the rotated envelope.
        let err = rotated.unwrap(&old_master).unwrap_err();
        assert!(
            matches!(err, CryptoError::TagMismatch),
            "expected TagMismatch with old key after rotation, got {err:?}"
        );
    }

    #[test]
    fn encrypted_blob_serializes_to_json() {
        let key = test_key();
        let blob = encrypt(b"serialize me", &key).unwrap();

        let json = serde_json::to_string(&blob).expect("should serialize");
        let decoded: EncryptedBlob = serde_json::from_str(&json).expect("should deserialize");

        let recovered = decrypt(&decoded, &key).unwrap();
        assert_eq!(recovered, b"serialize me");
    }
}
