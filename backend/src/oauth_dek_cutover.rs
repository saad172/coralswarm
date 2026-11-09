//! Foundation train PR 1.2 (CS-031) — the one-time re-encryption pass that
//! moves every `oauth_tokens` row off the legacy `ACCOUNT_MASTER_KEK`-XOR DEK
//! (`crate::crypto::derive_legacy_oauth_account_dek`) onto the SAME wrapped
//! per-account DEK content encryption already uses (`crate::crypto::
//! resolve_dek`, `accounts.dek_wrapped`, wrapped under `MASTER_KEK_HEX`).
//!
//! Shared by `bin/backfill_oauth_tokens_wrapped_dek` (the operator-facing
//! CLI) and `backend/tests/oauth_token_wrapped_dek_pg.rs` (the pg-gated
//! correctness proof), so both exercise the IDENTICAL logic rather than the
//! test re-implementing a parallel copy that could silently drift from what
//! actually runs in production.
//!
//! # Why this cannot be pure SQL
//!
//! Converting a row requires DECRYPTING it under the OLD key (a real
//! AES-256-GCM open, only reachable from Rust) and RE-ENCRYPTING it under the
//! NEW one (itself only reachable by unwrapping `accounts.dek_wrapped` with
//! `MASTER_KEK_HEX` — also real crypto). Migration
//! `0177_oauth_tokens_dek_migrated_at.sql` therefore adds only the progress
//! marker column; this module does the actual data conversion, following the
//! same migration-adds-schema / binary-does-the-work precedent as
//! `bin/backfill_billing_enforcement.rs`.
//!
//! # Why per-account, not one global query
//!
//! `oauth_tokens` is RLS-covered (migration `0083_consumer_rls`) — a bare,
//! unscoped read/write against it hard-errors under the least-privilege
//! `coralswarm_app` role (see `crypto.rs`'s and every other RLS-covered
//! backfill's own doc comments for why: the policy's `current_setting`
//! predicate raises rather than silently returning nothing). `accounts`
//! carries NO such policy (it IS the account boundary, not content scoped by
//! one), so this enumerates account ids from there first, then opens a
//! `tenancy::scope::begin_account_scoped` transaction per account — exactly
//! the shape `bin/backfill_billing_enforcement.rs` uses for its own
//! RLS-covered `grants` reads.
//!
//! # Idempotent and resumable
//!
//! Every row this pass touches is selected with `dek_migrated_at IS NULL` and
//! the UPDATE re-asserts that same predicate, so:
//!   * a row already converted (by an earlier run, or a genuinely fresh row
//!     `store_token_bundle_tx` already wrote under the wrapped DEK post-
//!     cutover) is never re-read or re-decrypted;
//!   * re-running this pass costs nothing but a per-account `SELECT` once
//!     every row is converted.
//!
//! # Dry-run by default at the CLI layer
//!
//! This function itself takes an explicit `execute: bool` — the CLI wrapper
//! is what defaults it to `false`, same inverted-default discipline as
//! `bin/backfill_billing_enforcement`/`bin/grant_plan`.

use sqlx::PgPool;
use uuid::Uuid;

use coralswarm_core::kek::{generate_dek, load_kek, wrap_dek};
use coralswarm_core::oauth_token::{decrypt_token, encrypt_token};

use crate::crypto::{derive_legacy_oauth_account_dek, resolve_dek};
use crate::tenancy::scope::begin_account_scoped;

/// Tallies across the whole pass — same "counts, not just pass/fail"
/// discipline as `bin/backfill_billing_enforcement::Totals`.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct CutoverOutcome {
    pub accounts_seen: u64,
    /// Accounts that had NO `dek_wrapped` yet and were provisioned one as
    /// part of this pass (mirrors `clerk_provision::provision_account_dek`) —
    /// only incremented when `execute` is true.
    pub accounts_provisioned_dek: u64,
    pub rows_seen: u64,
    /// Rows successfully decrypted under the legacy DEK, re-encrypted under
    /// the wrapped DEK, and written back with `dek_migrated_at` set. Only
    /// incremented when `execute` is true; a dry run reports would-migrate
    /// counts via `rows_seen` alone (nothing is decrypted in dry-run mode, so
    /// a row that would in fact fail to decrypt is not distinguished from one
    /// that would succeed — the dry run is a cheap "how much work is left"
    /// count, not a full rehearsal).
    pub rows_migrated: u64,
    /// A row-level failure (legacy decrypt failed — wrong/missing
    /// `ACCOUNT_MASTER_KEK`, corrupt ciphertext — or a DB error). Recorded
    /// and the pass CONTINUES to the next row/account, mirroring
    /// `backfill_billing_enforcement`'s "one bad account must not strand the
    /// rest" posture. `messages` carries one human-readable line per error.
    pub errors: u64,
    pub messages: Vec<String>,
}

/// One `oauth_tokens` row's encrypted columns, as read under the
/// account-scoped transaction.
struct LegacyRow {
    id: Uuid,
    access_token: Vec<u8>,
    refresh_token: Option<Vec<u8>>,
    user_token_encrypted: Option<Vec<u8>>,
}

/// Every account id in `accounts`, optionally narrowed to one — mirrors
/// `bin/backfill_billing_enforcement::list_accounts`. `accounts` carries no
/// RLS policy (see this module's own doc comment), so this is a plain
/// unscoped read.
async fn list_account_ids(pool: &PgPool, only: Option<Uuid>) -> Result<Vec<Uuid>, sqlx::Error> {
    if let Some(id) = only {
        sqlx::query_scalar("SELECT id FROM accounts WHERE id = $1")
            .bind(id)
            .fetch_all(pool)
            .await
    } else {
        sqlx::query_scalar("SELECT id FROM accounts ORDER BY created_at").fetch_all(pool).await
    }
}

/// Ensure `account_id` has a wrapped DEK, provisioning one if absent —
/// mirrors `clerk_provision::provision_account_dek` exactly (that function is
/// private to a private module and so unreachable from here; duplicated
/// rather than exposed, since it is a handful of lines and re-exporting it
/// would widen that module's own visibility for a one-off caller). Returns
/// `true` if a DEK was freshly provisioned by THIS call.
///
/// Best-effort in the sense that a missing `MASTER_KEK_HEX` is reported back
/// as `Ok(false)` (nothing provisioned) rather than an error — the caller's
/// subsequent `resolve_dek` call will surface the real "KEK not configured"
/// failure with its own proper error classification; duplicating that here
/// would just produce a second, differently-worded version of the same
/// condition.
async fn ensure_account_dek(pool: &PgPool, account_id: Uuid) -> Result<bool, sqlx::Error> {
    let existing: Option<Vec<u8>> =
        sqlx::query_scalar("SELECT dek_wrapped FROM accounts WHERE id = $1")
            .bind(account_id)
            .fetch_one(pool)
            .await?;
    if existing.is_some() {
        return Ok(false);
    }
    let Ok(kek) = load_kek() else {
        return Ok(false);
    };
    let Ok(wrapped) = wrap_dek(&kek, &generate_dek()) else {
        return Ok(false);
    };
    let res = sqlx::query(
        "UPDATE accounts SET dek_wrapped = $1 WHERE id = $2 AND dek_wrapped IS NULL",
    )
    .bind(&wrapped)
    .bind(account_id)
    .execute(pool)
    .await?;
    Ok(res.rows_affected() > 0)
}

/// One account's worth of work: read every not-yet-migrated row (scoped),
/// resolve both DEKs, and re-encrypt+write each row (only when `execute`).
async fn process_account(pool: &PgPool, account_id: Uuid, execute: bool, out: &mut CutoverOutcome) {
    out.accounts_seen += 1;

    if execute {
        match ensure_account_dek(pool, account_id).await {
            Ok(true) => out.accounts_provisioned_dek += 1,
            Ok(false) => {}
            Err(e) => {
                out.errors += 1;
                out.messages.push(format!(
                    "account={account_id}: failed to check/provision dek_wrapped: {e}"
                ));
                return;
            }
        }
    }

    let rows = {
        let mut tx = match begin_account_scoped(pool, account_id).await {
            Ok(tx) => tx,
            Err(e) => {
                out.errors += 1;
                out.messages
                    .push(format!("account={account_id}: failed to open scoped tx: {e}"));
                return;
            }
        };
        let rows: Result<Vec<(Uuid, Vec<u8>, Option<Vec<u8>>, Option<Vec<u8>>)>, sqlx::Error> =
            sqlx::query_as(
                "SELECT id, access_token, refresh_token, user_token_encrypted \
                 FROM oauth_tokens WHERE account_id = $1 AND dek_migrated_at IS NULL",
            )
            .bind(account_id)
            .fetch_all(&mut *tx)
            .await;
        tx.rollback().await.ok();
        match rows {
            Ok(r) => r,
            Err(e) => {
                out.errors += 1;
                out.messages
                    .push(format!("account={account_id}: failed to list oauth_tokens rows: {e}"));
                return;
            }
        }
    };

    if rows.is_empty() {
        return;
    }
    out.rows_seen += rows.len() as u64;
    if !execute {
        return;
    }

    // The legacy DEK is the SAME for every row of this account (it is
    // derived purely from `account_id` + the process-global
    // `ACCOUNT_MASTER_KEK`), so resolve it once per account rather than once
    // per row.
    let Some(legacy_dek) = derive_legacy_oauth_account_dek(account_id) else {
        out.errors += 1;
        out.messages.push(format!(
            "account={account_id}: ACCOUNT_MASTER_KEK missing or invalid — cannot decrypt \
             {} legacy row(s)",
            rows.len()
        ));
        return;
    };
    let wrapped_dek = match resolve_dek(pool, account_id).await {
        Ok(Some(d)) => d,
        Ok(None) => {
            out.errors += 1;
            out.messages.push(format!(
                "account={account_id}: no dek_wrapped even after provisioning attempt — \
                 MASTER_KEK_HEX likely unset"
            ));
            return;
        }
        Err(e) => {
            out.errors += 1;
            out.messages
                .push(format!("account={account_id}: failed to resolve wrapped DEK: {e}"));
            return;
        }
    };

    for row in rows.into_iter().map(|(id, access_token, refresh_token, user_token_encrypted)| {
        LegacyRow { id, access_token, refresh_token, user_token_encrypted }
    }) {
        if let Err(msg) =
            reencrypt_one_row(pool, account_id, &row, &legacy_dek, &wrapped_dek).await
        {
            out.errors += 1;
            out.messages.push(format!("account={account_id} row={}: {msg}", row.id));
            continue;
        }
        out.rows_migrated += 1;
    }
}

/// Decrypt one row's three encrypted columns under `legacy_dek`, re-encrypt
/// each present one under `wrapped_dek`, and write the row back with
/// `dek_migrated_at = now()` — inside a fresh account-scoped transaction
/// (kept short: no network/API calls, purely a decrypt/encrypt/write). The
/// `dek_migrated_at IS NULL` predicate on the UPDATE is a belt-and-suspenders
/// idempotency guard against a concurrent run racing the same row (the
/// account-level enumeration above already filters on it, but re-asserting
/// it here means a lost race silently no-ops — `rows_affected() == 0` —
/// rather than double-encrypting).
async fn reencrypt_one_row(
    pool: &PgPool,
    account_id: Uuid,
    row: &LegacyRow,
    legacy_dek: &[u8; 32],
    wrapped_dek: &[u8; 32],
) -> Result<(), String> {
    let access_plain = decrypt_token(legacy_dek, &row.access_token)
        .map_err(|e| format!("legacy decrypt of access_token failed: {e}"))?;
    let refresh_plain = row
        .refresh_token
        .as_deref()
        .map(|enc| {
            decrypt_token(legacy_dek, enc)
                .map_err(|e| format!("legacy decrypt of refresh_token failed: {e}"))
        })
        .transpose()?;
    let user_token_plain = row
        .user_token_encrypted
        .as_deref()
        .map(|enc| {
            decrypt_token(legacy_dek, enc)
                .map_err(|e| format!("legacy decrypt of user_token_encrypted failed: {e}"))
        })
        .transpose()?;

    let access_new = encrypt_token(wrapped_dek, &access_plain)
        .map_err(|e| format!("re-encrypt of access_token failed: {e}"))?;
    let refresh_new = refresh_plain
        .map(|p| encrypt_token(wrapped_dek, &p))
        .transpose()
        .map_err(|e| format!("re-encrypt of refresh_token failed: {e}"))?;
    let user_token_new = user_token_plain
        .map(|p| encrypt_token(wrapped_dek, &p))
        .transpose()
        .map_err(|e| format!("re-encrypt of user_token_encrypted failed: {e}"))?;

    let mut tx = begin_account_scoped(pool, account_id)
        .await
        .map_err(|e| format!("failed to open scoped tx for update: {e}"))?;
    sqlx::query(
        "UPDATE oauth_tokens \
         SET access_token = $1, refresh_token = $2, user_token_encrypted = $3, \
             dek_migrated_at = now() \
         WHERE id = $4 AND account_id = $5 AND dek_migrated_at IS NULL",
    )
    .bind(&access_new)
    .bind(&refresh_new)
    .bind(&user_token_new)
    .bind(row.id)
    .bind(account_id)
    .execute(&mut *tx)
    .await
    .map_err(|e| format!("update failed: {e}"))?;
    tx.commit().await.map_err(|e| format!("commit failed: {e}"))?;
    Ok(())
}

/// Run one full pass over every account (or just `account_id`, when
/// narrowed), re-encrypting every `oauth_tokens` row still on the legacy
/// XOR-derived DEK. `execute = false` is a dry run: reports `rows_seen`
/// (how much work remains) without decrypting or writing anything —
/// `ACCOUNT_MASTER_KEK`/`MASTER_KEK_HEX` need not even be configured for a
/// dry run to report a useful count.
///
/// One bad account/row never strands the rest — see this module's own doc
/// comment and `CutoverOutcome::errors`'s.
pub async fn reencrypt_oauth_tokens(pool: &PgPool, account_id: Option<Uuid>) -> CutoverOutcome {
    reencrypt_oauth_tokens_inner(pool, account_id, true).await
}

/// Dry-run variant: identical enumeration, but writes nothing. Split out as
/// its own function (rather than a boolean on the public entry point) so the
/// common case — the CLI binary's `--execute`-gated real run, and the
/// pg-gated test's real run — reads as `reencrypt_oauth_tokens(pool, id)`
/// with no stray boolean at the call site; the CLI's dry-run report is the
/// one caller that actually needs the distinction.
pub async fn preview_oauth_tokens_cutover(pool: &PgPool, account_id: Option<Uuid>) -> CutoverOutcome {
    reencrypt_oauth_tokens_inner(pool, account_id, false).await
}

async fn reencrypt_oauth_tokens_inner(
    pool: &PgPool,
    account_id: Option<Uuid>,
    execute: bool,
) -> CutoverOutcome {
    let mut out = CutoverOutcome::default();
    let accounts = match list_account_ids(pool, account_id).await {
        Ok(a) => a,
        Err(e) => {
            out.errors += 1;
            out.messages.push(format!("failed to enumerate accounts: {e}"));
            return out;
        }
    };
    for account_id in accounts {
        process_account(pool, account_id, execute, &mut out).await;
    }
    out
}
