//! KEK-encrypted tenant DSNs in the control plane (`tenant_db_credentials`,
//! migration 0381; self-serve org signup PR A1a).
//!
//! The plaintext DSN exists in memory only: it is encrypted under
//! `MASTER_KEK_HEX` before it is written and decrypted after it is read, by
//! [`ControlTableSecretResolver`](super::secrets::ControlTableSecretResolver).
//! Nothing in this module logs, formats, or returns a DSN inside an error.
//!
//! Writes go through the SECURITY DEFINER functions migration 0381 installs
//! (`tenant_provisioning_store_credential`, `_delete_credential`), so the
//! serving role holds no write on the table itself: a stored ref is
//! insert-if-absent (never overwritten) and deletable only once no routing
//! row references it.
//!
//! Blob layout is the one `coralswarm_core::kek` uses for wrapped DEKs —
//! `[nonce 12B][tag 16B][ciphertext]` — but with a variable-length body, so
//! it has its own (de)serialiser here rather than reusing the fixed-60-byte
//! DEK one. The GCM associated data is `tenant-dsn:v1:<secret_ref>`
//! ([`dsn_aad`]): a blob only decrypts under the ref it was written for.

use coralswarm_core::crypto::{self, EncryptedBlob};
use sqlx::PgPool;

/// Minimum serialised length: nonce + tag + at least one ciphertext byte.
const MIN_LEN: usize = 12 + 16 + 1;

#[derive(Debug, thiserror::Error)]
pub enum CredentialError {
    #[error("tenant credential encryption failed")]
    Encrypt,
    #[error("tenant credential did not decrypt under the configured KEK")]
    Decrypt,
    #[error("tenant credential store: {0}")]
    Db(#[from] sqlx::Error),
    /// A credential already exists under this ref for a DIFFERENT database.
    #[error("tenant credential {0} already names a different database; refusing to overwrite")]
    Conflict(String),
}

/// The associated data every stored DSN is bound to: its own row key. A
/// ciphertext copied (or a `secret_ref` rewritten) onto another row fails the
/// GCM tag check there instead of resolving another tenant's DSN.
pub fn dsn_aad(secret_ref: &str) -> Vec<u8> {
    format!("tenant-dsn:v1:{secret_ref}").into_bytes()
}

/// Encrypt a DSN under the KEK, bound to `secret_ref` ([`dsn_aad`]). Fresh
/// nonce per call.
pub fn encrypt_dsn(kek: &[u8; 32], secret_ref: &str, dsn: &str) -> Result<Vec<u8>, CredentialError> {
    let blob = crypto::encrypt_with_aad(dsn.as_bytes(), kek, &dsn_aad(secret_ref))
        .map_err(|_| CredentialError::Encrypt)?;
    let mut out = Vec::with_capacity(28 + blob.ciphertext.len());
    out.extend_from_slice(&blob.nonce);
    out.extend_from_slice(&blob.tag);
    out.extend_from_slice(&blob.ciphertext);
    Ok(out)
}

/// Decrypt the DSN stored under `secret_ref`, trying `next` (a rotation in
/// progress) before `current`. AES-GCM's tag means a wrong key, or a blob
/// that was written for a different ref, fails to verify rather than
/// yielding a plausible wrong DSN.
pub fn decrypt_dsn(
    current: &[u8; 32],
    next: Option<&[u8; 32]>,
    secret_ref: &str,
    bytes: &[u8],
) -> Result<String, CredentialError> {
    if bytes.len() < MIN_LEN {
        return Err(CredentialError::Decrypt);
    }
    let mut nonce = [0u8; 12];
    nonce.copy_from_slice(&bytes[0..12]);
    let mut tag = [0u8; 16];
    tag.copy_from_slice(&bytes[12..28]);
    let blob = EncryptedBlob {
        ciphertext: bytes[28..].to_vec(),
        nonce,
        tag,
    };
    let aad = dsn_aad(secret_ref);
    let plaintext = next
        .and_then(|k| crypto::decrypt_with_aad(&blob, k, &aad).ok())
        .map(Ok)
        .unwrap_or_else(|| crypto::decrypt_with_aad(&blob, current, &aad).map_err(|_| CredentialError::Decrypt))?;
    String::from_utf8(plaintext).map_err(|_| CredentialError::Decrypt)
}

/// Raw ciphertext for `secret_ref`, `None` when no row exists.
pub async fn fetch_ciphertext(
    control: &PgPool,
    secret_ref: &str,
) -> Result<Option<Vec<u8>>, sqlx::Error> {
    sqlx::query_scalar("SELECT dsn_encrypted FROM tenant_db_credentials WHERE secret_ref = $1")
        .bind(secret_ref)
        .fetch_optional(control)
        .await
}

/// Outcome of [`store`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StoreOutcome {
    Inserted,
    /// The identical ciphertext was already present (a retried step).
    AlreadyPresent,
}

/// Encrypt and store `dsn` under `secret_ref` through the definer function.
///
/// Convergent: every encryption uses a fresh nonce, so a retried step cannot
/// replay identical bytes. Instead an existing row is decrypted and compared
/// — the same DSN is [`StoreOutcome::AlreadyPresent`], a different one is
/// [`CredentialError::Conflict`] (never silently replaced). The function's
/// own never-overwrite rule is the database-level backstop for the race
/// between that read and the insert.
pub async fn store(
    control: &PgPool,
    kek: &[u8; 32],
    next_kek: Option<&[u8; 32]>,
    secret_ref: &str,
    dsn: &str,
) -> Result<StoreOutcome, CredentialError> {
    if let Some(existing) = fetch_ciphertext(control, secret_ref).await? {
        let stored = decrypt_dsn(kek, next_kek, secret_ref, &existing)?;
        return if stored == dsn {
            Ok(StoreOutcome::AlreadyPresent)
        } else {
            Err(CredentialError::Conflict(secret_ref.to_string()))
        };
    }
    let ciphertext = encrypt_dsn(kek, secret_ref, dsn)?;
    let inserted: bool =
        sqlx::query_scalar("SELECT tenant_provisioning_store_credential($1, $2)")
            .bind(secret_ref)
            .bind(&ciphertext)
            .fetch_one(control)
            .await?;
    Ok(if inserted {
        StoreOutcome::Inserted
    } else {
        StoreOutcome::AlreadyPresent
    })
}

/// Delete the credential for `secret_ref`. Returns rows deleted (0 when it
/// was already gone). Refused by the function while a routing row still
/// references the ref.
pub async fn delete(control: &PgPool, secret_ref: &str) -> Result<i32, CredentialError> {
    let n: i32 = sqlx::query_scalar("SELECT tenant_provisioning_delete_credential($1)")
        .bind(secret_ref)
        .fetch_one(control)
        .await?;
    Ok(n)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dsn_round_trips_and_rejects_wrong_key() {
        let kek = [0x11u8; 32];
        let other = [0x22u8; 32];
        let dsn = "postgres://fixture:synthetic@localhost:5432/qa";
        let r = "TENANT_DB_O_0123456789ABCDEF";
        let bytes = encrypt_dsn(&kek, r, dsn).unwrap();
        assert!(bytes.len() > MIN_LEN);
        assert_eq!(decrypt_dsn(&kek, None, r, &bytes).unwrap(), dsn);
        assert!(decrypt_dsn(&other, None, r, &bytes).is_err());
        // Rotation: `next` wraps it, `current` does not — still resolves.
        assert_eq!(decrypt_dsn(&other, Some(&kek), r, &bytes).unwrap(), dsn);
        // Truncated blob fails closed.
        assert!(decrypt_dsn(&kek, None, r, &bytes[..20]).is_err());
    }

    /// N1: the ciphertext is bound to its ref. Moved onto another ref's row
    /// (an app-role write, a swapped `secret_ref`) it does not decrypt, under
    /// either the current or the rotation key.
    #[test]
    fn a_ciphertext_swapped_to_another_ref_fails_to_decrypt() {
        let kek = [0x11u8; 32];
        let dsn = "postgres://fixture:synthetic@localhost:5432/victim";
        let bytes = encrypt_dsn(&kek, "TENANT_DB_O_AAAAAAAAAAAAAAAA", dsn).unwrap();
        assert!(matches!(
            decrypt_dsn(&kek, None, "TENANT_DB_O_BBBBBBBBBBBBBBBB", &bytes),
            Err(CredentialError::Decrypt)
        ));
        assert!(decrypt_dsn(&[0x22u8; 32], Some(&kek), "TENANT_DB_O_BBBBBBBBBBBBBBBB", &bytes).is_err());
        // A pre-AAD (unbound) blob is not accepted either.
        let unbound = crypto::encrypt(dsn.as_bytes(), &kek).unwrap();
        let mut raw = unbound.nonce.to_vec();
        raw.extend_from_slice(&unbound.tag);
        raw.extend_from_slice(&unbound.ciphertext);
        assert!(decrypt_dsn(&kek, None, "TENANT_DB_O_AAAAAAAAAAAAAAAA", &raw).is_err());
    }

    #[test]
    fn ciphertext_never_contains_the_plaintext() {
        let kek = [0x33u8; 32];
        let dsn = "postgres://user:hunter2@db.example/qa";
        let bytes = encrypt_dsn(&kek, "TENANT_DB_X", dsn).unwrap();
        assert!(!bytes.windows(7).any(|w| w == b"hunter2"));
    }
}
