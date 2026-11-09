//! Shared personal-access-token (PAT) primitives: secret generation and
//! hashing, common to every long-lived bearer credential this backend mints
//! for a client that cannot hold a live Clerk session.
//!
//! The shape: a prefixed, CSPRNG-random bearer secret, hashed with SHA-256
//! before it ever touches the database. Every PAT family calls the functions
//! here rather than keeping its own copy, so there is exactly one place that
//! decides how a PAT is generated and hashed.
//!
//! Each family owns its OWN prefix constant, its OWN table, and its OWN
//! authorization rules — this module is deliberately narrow: it knows nothing
//! about `addin_tokens`, only about turning a prefix into a secret and a
//! secret into its stored hash.

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use rand::Rng;
use sha2::{Digest, Sha256};

/// SHA-256 hex digest — the ONLY form any PAT's `token_hash` column ever
/// stores. Plaintext secrets are never persisted and never logged.
pub(crate) fn hash_token(raw: &str) -> String {
    hex::encode(Sha256::digest(raw.as_bytes()))
}

/// Mint a fresh PAT: 32 bytes of CSPRNG randomness, base64url (no padding)
/// encoded, prefixed with `prefix` (e.g. `"csat_"`, `"csdt_"`) — a greppable
/// prefix convention so a value starting with it is never mistaken for the
/// plaintext of a Clerk JWT, and so `middleware::auth::require_auth` can
/// route a bearer to the right resolver by its shape alone.
pub(crate) fn generate_pat(prefix: &str) -> String {
    let mut bytes = [0u8; 32];
    rand::thread_rng().fill(&mut bytes);
    format!("{prefix}{}", URL_SAFE_NO_PAD.encode(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hash_token_is_deterministic_and_64_hex_chars() {
        let h1 = hash_token("csdt_example");
        let h2 = hash_token("csdt_example");
        assert_eq!(h1, h2);
        assert_eq!(h1.len(), 64);
        assert!(h1.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn hash_token_differs_for_different_inputs() {
        assert_ne!(hash_token("a"), hash_token("b"));
    }

    #[test]
    fn generate_pat_carries_the_given_prefix_and_never_repeats() {
        let a = generate_pat("csdt_");
        let b = generate_pat("csdt_");
        assert!(a.starts_with("csdt_"));
        assert!(b.starts_with("csdt_"));
        assert_ne!(a, b, "two mints must never collide in a sample of two");
        // 32 raw bytes, base64url no-pad, plus the prefix.
        assert!(a.len() > "csdt_".len() + 32);
    }

    #[test]
    fn generate_pat_respects_a_different_prefix() {
        let a = generate_pat("csat_");
        assert!(a.starts_with("csat_"));
        assert!(!a.starts_with("csdt_"));
    }
}
