//! Foundation train PR 1.5 — backfills `oauth_tokens` rows for Slack
//! workspaces connected through the legacy `routes::slack::oauth_callback`
//! path, which (before this PR) wrote `slack_integrations` (and
//! `connector_installs`) only, and never `oauth_tokens` at all — the generic
//! `routes::oauth::callback` path was the only Slack-connect path that did.
//!
//! Shared by `bin/backfill_slack_oauth_tokens` (the operator-facing CLI) and
//! `backend/tests/slack_oauth_backfill_pg.rs` (the pg-gated correctness
//! proof), same split as `oauth_dek_cutover`/`bin/backfill_oauth_tokens_wrapped_dek`
//! (Foundation PR 1.2) — this PR's own precedent for "a migration adds
//! nothing to convert, and a dedicated Rust binary does the actual data
//! conversion as a separate, idempotent, re-runnable operational step". No
//! migration accompanies this backfill: no schema changed, only rows are
//! added to the ALREADY-EXISTING `oauth_tokens` table.
//!
//! # Why this matters (the incident class this PR closes from the other side)
//!
//! `routes/oauth.rs::delete_connection`'s own doc comment names the
//! "Disconnect didn't stop ingest" incident: `ingest_slack_message` (the live
//! ingest path) and every other read site this PR migrates now resolve their
//! bot token from `oauth_tokens`, not `slack_integrations.bot_token`. A
//! workspace connected ONLY through the legacy `oauth_callback` path (before
//! this PR's dual-write, added the same PR) has no `oauth_tokens` row at
//! all — so until this backfill runs for it, every migrated read site treats
//! it as "not connected" (`Ok(None)`), which means ingest, huddle capture,
//! and disconnect purge all silently stop working for it. This backfill
//! closes that gap by writing the missing row from what
//! `slack_integrations` already has on file.
//!
//! # What is (and is not) recoverable from `slack_integrations` alone
//!
//! `slack_integrations` carries `account_id`, `workspace_id`, `workspace_name`,
//! and the plaintext `bot_token` — nothing else. It never recorded the OAuth
//! `scope` string or the installing user's id, so a backfilled row's `scope`
//! column is set to [`BACKFILLED_SCOPE_PLACEHOLDER`] (never fabricated as a
//! real Slack scope list — see that constant's own doc comment) and
//! `provider_user_id` is left `NULL`. Nothing downstream branches on either
//! column's content for Slack (`missing_scope` handling always comes from a
//! live Slack API error, never from the stored `scope` string), so this is
//! safe.
//!
//! # Idempotent and resumable
//!
//! Every workspace this pass considers is selected with `NOT EXISTS (...
//! oauth_tokens ...)` (an account-wide row for that exact
//! `provider_workspace_id`), so:
//!   * a workspace already backfilled (by an earlier run, or one that
//!     connected through the modern `routes::oauth::callback` path and so
//!     already has an `oauth_tokens` row) is never re-written;
//!   * re-running this pass costs nothing but a per-account `SELECT` once
//!     every legacy workspace is converted.
//!
//! # Dry-run by default at the CLI layer
//!
//! Same inverted-default discipline as `bin/backfill_oauth_tokens_wrapped_dek`/
//! `bin/backfill_billing_enforcement`: the safe outcome of a mistyped
//! invocation is a printed report, not a write. `--execute` is required to
//! actually write anything. A dry run needs no `MASTER_KEK_HEX` configured —
//! it only counts rows that would be written.

use sqlx::PgPool;
use uuid::Uuid;

use coralswarm_core::kek::{generate_dek, load_kek, wrap_dek};
use coralswarm_core::oauth_token::encrypt_token;

use crate::crypto::resolve_dek;
use crate::tenancy::scope::begin_account_scoped;

/// Placeholder written into a backfilled row's `oauth_tokens.scope` column —
/// see this module's own doc comment ("What is (and is not) recoverable")
/// for why the real scope grant is unrecoverable from `slack_integrations`
/// alone. Deliberately NOT shaped like a real space-separated Slack scope
/// list, so nobody mistakes it for one; safe because nothing downstream
/// parses/splits this column for the `slack` provider.
pub const BACKFILLED_SCOPE_PLACEHOLDER: &str = "unknown (backfilled from legacy slack_integrations)";

/// Tallies across the whole pass — same "counts, not just pass/fail"
/// discipline as `oauth_dek_cutover::CutoverOutcome`.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct SlackBackfillOutcome {
    pub accounts_seen: u64,
    /// REAL organization accounts skipped outright (org-plane DEK fix). A
    /// real org's wrapped DEK lives in its own tenant database's
    /// `account_keys`, which this consumer-plane binary cannot reach — see
    /// [`process_account`]'s own comment for why running it against one would
    /// be actively destructive rather than merely useless. A personal org
    /// (control migration 0208) is NOT counted here, even though its account
    /// also carries a `clerk_org_id` — see [`account_is_org`]'s own doc for
    /// why its DEK is on the consumer plane like any other personal account.
    pub accounts_skipped_org: u64,
    /// Accounts that had NO `dek_wrapped` yet and were provisioned one as
    /// part of this pass. Only incremented when `execute` is true.
    pub accounts_provisioned_dek: u64,
    /// Legacy `slack_integrations` workspaces found with no matching
    /// account-wide `oauth_tokens` row.
    pub workspaces_seen: u64,
    /// Rows successfully encrypted and written. Only incremented when
    /// `execute` is true; a dry run reports would-write counts via
    /// `workspaces_seen` alone.
    pub rows_written: u64,
    /// A row-level failure (DEK unavailable, encryption error, DB error).
    /// Recorded and the pass CONTINUES to the next workspace/account,
    /// mirroring `backfill_oauth_tokens_wrapped_dek`'s "one bad account must
    /// not strand the rest" posture.
    pub errors: u64,
    pub messages: Vec<String>,
}

/// One legacy `slack_integrations` row with no matching `oauth_tokens` row.
/// `workspace_name` is not part of this struct: `oauth_tokens` has no column
/// for it (see `insert_backfilled_row`'s own comment — `TokenBundle.
/// provider_workspace_name` is never persisted to `oauth_tokens` by
/// `store_token_bundle_tx` either; it only feeds provider-specific side
/// tables), so there is nothing to carry it into.
struct LegacyIntegration {
    workspace_id: String,
    bot_token: String,
}

/// Every account id in `accounts`, optionally narrowed to one — mirrors
/// `oauth_dek_cutover::list_account_ids`. `accounts` carries no RLS policy
/// (it IS the account boundary, not content scoped by one), so this is a
/// plain unscoped read.
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

/// `Some(clerk_org_id)` when `account_id` is a REAL organization account —
/// `clerk_org_id` is set AND does not appear in `personal_orgs` (control
/// migration 0208).
///
/// A personal org's account ALSO carries a `clerk_org_id` (adoption relabels
/// the user's existing personal account rather than leaving it null — see
/// `bin/adopt_personal_org_account`), but it is not a real org for this
/// binary's purposes: it has no `tenant_databases` routing row, so its DEK
/// lives on the shared consumer plane exactly like an ordinary
/// `clerk_org_id IS NULL` account (`crypto::DekPlane::resolve_for_org`'s "no
/// tenant_databases row ⇒ consumer plane" arm, `crypto.rs:565` and `:582`,
/// applies to it precisely because it has no row). So there is nothing
/// wrong-plane about this binary resolving and, if needed, provisioning its
/// `accounts.dek_wrapped` — the two dangers [`process_account`]'s own comment
/// documents (a competing key, an unreadable row) are specific to a REAL
/// org's shadow-provisioning fence, which a personal org was never put behind.
///
/// Before this predicate excluded `personal_orgs`, every adopted user's
/// account was treated as an ORG account here and silently skipped — the
/// same "adopted user becomes unreachable through a lookup that only knows
/// the old shape" class of bug this codebase has hit elsewhere.
///
/// Fails closed on a DB error (the caller skips rather than guessing a
/// plane).
async fn account_is_org(pool: &PgPool, account_id: Uuid) -> Result<Option<String>, sqlx::Error> {
    sqlx::query_scalar::<_, String>(
        "SELECT a.clerk_org_id FROM accounts a \
         WHERE a.id = $1 AND a.clerk_org_id IS NOT NULL \
           AND NOT EXISTS ( \
             SELECT 1 FROM personal_orgs p WHERE p.clerk_org_id = a.clerk_org_id \
           )",
    )
    .bind(account_id)
    .fetch_optional(pool)
    .await
}

/// Ensure `account_id` has a wrapped DEK, provisioning one if absent — same
/// shape as `oauth_dek_cutover::ensure_account_dek` (duplicated rather than
/// exposed across crate-internal module boundaries; a handful of lines, same
/// "duplicated rather than re-exporting a private helper" reasoning
/// `connectors::TokenOwner`'s own doc comment gives for
/// `clerk_provision::provision_account_dek`). Returns `true` if a DEK was
/// freshly provisioned by THIS call.
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
    let res = sqlx::query("UPDATE accounts SET dek_wrapped = $1 WHERE id = $2 AND dek_wrapped IS NULL")
        .bind(&wrapped)
        .bind(account_id)
        .execute(pool)
        .await?;
    Ok(res.rows_affected() > 0)
}

/// Every `slack_integrations` row for `account_id` with no matching
/// account-wide `oauth_tokens` row — a single scoped read (both tables carry
/// the same `account_id` RLS policy, so one `begin_account_scoped`
/// transaction covers the join).
async fn list_legacy_integrations(
    pool: &PgPool,
    account_id: Uuid,
) -> Result<Vec<LegacyIntegration>, sqlx::Error> {
    let mut tx = begin_account_scoped(pool, account_id)
        .await
        .map_err(|e| sqlx::Error::Protocol(format!("list_legacy_integrations scoped tx: {e}")))?;
    let rows: Vec<(String, String)> = sqlx::query_as(
        r#"
        SELECT si.workspace_id, si.bot_token
        FROM   slack_integrations si
        WHERE  si.account_id = $1
          AND  NOT EXISTS (
                SELECT 1 FROM oauth_tokens ot
                WHERE ot.account_id = si.account_id
                  AND ot.provider = 'slack'
                  AND ot.provider_workspace_id = si.workspace_id
                  AND ot.owner_user_id IS NULL
              )
        "#,
    )
    .bind(account_id)
    .fetch_all(&mut *tx)
    .await?;
    tx.rollback().await.ok();
    Ok(rows
        .into_iter()
        .map(|(workspace_id, bot_token)| LegacyIntegration { workspace_id, bot_token })
        .collect())
}

/// Encrypt `integration.bot_token` under `account_id`'s wrapped DEK and
/// insert the `oauth_tokens` row — same `TokenOwner::AccountWide` shape
/// `routes::oauth::store_token_bundle_tx` writes (Slack has no per-member
/// OAuth), with `dek_migrated_at = now()` set directly: this row is written
/// UNDER the wrapped-DEK scheme from the moment it exists, never under the
/// retired `ACCOUNT_MASTER_KEK` XOR derivation, so there is nothing for the
/// PR 1.2 cutover backfill to ever "find" here — marking it migrated at
/// insert time is the accurate statement of that fact, not a guess.
///
/// `ON CONFLICT ... DO NOTHING`, scoped to the same partial unique index
/// `store_token_bundle_tx`'s `TokenOwner::AccountWide` branch targets: belt
/// and suspenders against a race with a concurrent connect/backfill re-run
/// landing the identical row between this function's caller reading "no
/// oauth_tokens row exists yet" and this INSERT executing — `list_legacy_
/// integrations`' own `NOT EXISTS` check already makes this the expected
/// case, not the common one.
async fn insert_backfilled_row(
    pool: &PgPool,
    account_id: Uuid,
    integration: &LegacyIntegration,
    dek: &[u8; 32],
) -> Result<bool, String> {
    let encrypted = encrypt_token(dek, &integration.bot_token)
        .map_err(|e| format!("failed to encrypt bot_token: {e}"))?;

    let mut tx = begin_account_scoped(pool, account_id)
        .await
        .map_err(|e| format!("failed to open scoped tx: {e}"))?;
    let res = sqlx::query(
        r#"
        INSERT INTO oauth_tokens
            (account_id, provider, scope, access_token, provider_workspace_id,
             owner_user_id, dek_migrated_at, created_at, updated_at)
        VALUES ($1, 'slack', $2, $3, $4, NULL, now(), now(), now())
        ON CONFLICT (account_id, provider, provider_workspace_id)
            WHERE owner_user_id IS NULL
        DO NOTHING
        "#,
    )
    .bind(account_id)
    .bind(BACKFILLED_SCOPE_PLACEHOLDER)
    .bind(&encrypted)
    .bind(&integration.workspace_id)
    .execute(&mut *tx)
    .await
    .map_err(|e| format!("failed to insert oauth_tokens row: {e}"))?;
    tx.commit().await.map_err(|e| format!("failed to commit: {e}"))?;
    Ok(res.rows_affected() > 0)
}

/// One account's worth of work: find legacy workspaces, resolve/provision the
/// DEK, and write each missing `oauth_tokens` row (only when `execute`).
async fn process_account(pool: &PgPool, account_id: Uuid, execute: bool, out: &mut SlackBackfillOutcome) {
    out.accounts_seen += 1;

    // ── REAL org accounts are skipped, in DRY RUN too (org-plane DEK fix) ───
    // This binary is consumer-plane by construction: it resolves the DEK with
    // `crypto::resolve_dek` (control `accounts.dek_wrapped`) and, worse, will
    // PROVISION one via `ensure_account_dek` when the column is NULL. For a
    // REAL org account (see [`account_is_org`]'s doc — a personal org does
    // NOT take this branch) both of those are wrong in a way that is not
    // merely a no-op:
    //
    //   1. A real org account's `accounts.dek_wrapped` is deliberately NULL —
    //      the shadow-provisioning fence in `clerk_provision::ensure_org_account`
    //      keeps it that way under `MULTITENANT_ENABLED`, because the real key
    //      lives in that org's tenant `account_keys`. Minting one here would
    //      create a COMPETING key on the shared plane, which is exactly what
    //      that fence exists to prevent.
    //   2. The row it then writes would be encrypted under that competing key,
    //      so every correct reader (which now resolves the tenant DEK — see
    //      `workers::huddle::resolve_account_wide_oauth_token`) would fail to
    //      decrypt it. That is the "connect succeeds, ingest silently does
    //      nothing" failure this fix removed; running this binary afterwards
    //      would manufacture a fresh instance of it.
    //
    // Skipped rather than errored: a real org account having no legacy
    // `slack_integrations`-only connection to backfill is the normal case, and
    // a one-shot sweep must not exit non-zero over it. Counted and reported so
    // an operator can see it was deliberate. An org's own legacy backfill, if
    // one is ever needed, requires a tenant-aware variant that resolves the
    // DEK through `crypto::DekPlane::resolve_for_org`.
    match account_is_org(pool, account_id).await {
        Ok(Some(org_id)) => {
            // Counted, not pushed onto `messages`: that vec is the binary's
            // ERROR channel (`backfill_slack_oauth_tokens.rs` prints every
            // entry prefixed `ERROR`), and a deliberate skip is not an error.
            // The summary line reports the count.
            out.accounts_skipped_org += 1;
            tracing::info!(
                account_id = %account_id,
                org_id = %org_id,
                "slack oauth backfill: skipping REAL org account — its DEK lives in the tenant \
                 database's account_keys, which this consumer-plane backfill cannot reach"
            );
            return;
        }
        Ok(None) => {}
        Err(e) => {
            out.errors += 1;
            out.messages.push(format!(
                "account={account_id}: failed to determine whether this is a real org account — \
                 skipping rather than risking a wrong-plane write: {e}"
            ));
            return;
        }
    }

    let integrations = match list_legacy_integrations(pool, account_id).await {
        Ok(rows) => rows,
        Err(e) => {
            out.errors += 1;
            out.messages
                .push(format!("account={account_id}: failed to list slack_integrations: {e}"));
            return;
        }
    };
    if integrations.is_empty() {
        return;
    }
    out.workspaces_seen += integrations.len() as u64;
    if !execute {
        return;
    }

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

    let dek = match resolve_dek(pool, account_id).await {
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

    for integration in &integrations {
        match insert_backfilled_row(pool, account_id, integration, &dek).await {
            Ok(true) => out.rows_written += 1,
            // `false` = a concurrent writer already landed this exact row
            // between this pass's read and its own write (see
            // `insert_backfilled_row`'s own doc comment) — not an error.
            Ok(false) => {}
            Err(msg) => {
                out.errors += 1;
                out.messages.push(format!(
                    "account={account_id} workspace={}: {msg}",
                    integration.workspace_id
                ));
            }
        }
    }
}

/// Dry run: reports how many legacy workspaces would be backfilled, writing
/// nothing.
pub async fn preview_slack_oauth_backfill(pool: &PgPool, only: Option<Uuid>) -> SlackBackfillOutcome {
    run_pass(pool, only, false).await
}

/// Execute: writes the missing `oauth_tokens` row for every legacy
/// `slack_integrations` workspace found.
pub async fn backfill_slack_oauth_tokens(pool: &PgPool, only: Option<Uuid>) -> SlackBackfillOutcome {
    run_pass(pool, only, true).await
}

async fn run_pass(pool: &PgPool, only: Option<Uuid>, execute: bool) -> SlackBackfillOutcome {
    let mut out = SlackBackfillOutcome::default();
    let account_ids = match list_account_ids(pool, only).await {
        Ok(ids) => ids,
        Err(e) => {
            out.errors += 1;
            out.messages.push(format!("failed to enumerate accounts: {e}"));
            return out;
        }
    };
    for account_id in account_ids {
        process_account(pool, account_id, execute, &mut out).await;
    }
    out
}

// ── A personal org must not be treated as a real org (control migration 0208) ──
//
// Same `RELINFER_TEST_DATABASE_URL`-gated, in-crate discipline as
// `personal_account::pg_tests` and `clerk_provision::pg_tests` — not a new file
// under `backend/tests/`, which CI's hand-maintained `--test <name>` list
// would silently never run.
#[cfg(test)]
mod pg_tests {
    use super::*;

    async fn pg_test_pool(test_name: &str) -> Option<PgPool> {
        let url = match std::env::var("RELINFER_TEST_DATABASE_URL") {
            Ok(u) => u,
            Err(_) => {
                eprintln!("skipping {test_name}: set RELINFER_TEST_DATABASE_URL to run it");
                return None;
            }
        };
        Some(
            sqlx::postgres::PgPoolOptions::new()
                .max_connections(4)
                .connect(&url)
                .await
                .expect("connect to test Postgres"),
        )
    }

    async fn seed_user(pool: &PgPool, tag: &str, clerk_user_id: Option<&str>) -> Uuid {
        sqlx::query_scalar("INSERT INTO users (email, clerk_user_id) VALUES ($1, $2) RETURNING id")
            .bind(format!("slack-backfill-{tag}-{}@example.test", Uuid::new_v4()))
            .bind(clerk_user_id)
            .fetch_one(pool)
            .await
            .expect("seed user")
    }

    async fn seed_legacy_integration(pool: &PgPool, account_id: Uuid, workspace_id: &str) {
        sqlx::query(
            "INSERT INTO slack_integrations (account_id, workspace_id, workspace_name, bot_token) \
             VALUES ($1, $2, 'Test Workspace', 'xoxb-legacy-token')",
        )
        .bind(account_id)
        .bind(workspace_id)
        .execute(pool)
        .await
        .expect("seed legacy slack_integrations row");
    }

    /// A personal org's account (adopted or not — either shape carries a
    /// `clerk_org_id` with a matching `personal_orgs` row) must NOT be
    /// treated as a real org: its DEK lives on the shared consumer plane
    /// exactly like an ordinary `clerk_org_id IS NULL` account, so its legacy
    /// Slack workspace must still be found and counted, not skipped.
    #[tokio::test]
    async fn a_personal_org_account_is_not_skipped_as_an_org_account_pg() {
        let Some(pool) = pg_test_pool("personal_org_not_skipped_pg").await else {
            return;
        };
        let clerk_user_id = format!("user_slackbf_{}", Uuid::new_v4());
        let clerk_org_id = format!("org_slackbf_{}", Uuid::new_v4());
        let user_id = seed_user(&pool, "personal-org", Some(&clerk_user_id)).await;
        sqlx::query("INSERT INTO personal_orgs (clerk_org_id, clerk_user_id) VALUES ($1, $2)")
            .bind(&clerk_org_id)
            .bind(&clerk_user_id)
            .execute(&pool)
            .await
            .expect("seed personal_orgs row");
        let account_id: Uuid = sqlx::query_scalar(
            "INSERT INTO accounts (owner_user_id, clerk_org_id) VALUES ($1, $2) RETURNING id",
        )
        .bind(user_id)
        .bind(&clerk_org_id)
        .fetch_one(&pool)
        .await
        .expect("seed personal-org-labelled account");
        seed_legacy_integration(&pool, account_id, "T_PERSONAL").await;

        let mut out = SlackBackfillOutcome::default();
        process_account(&pool, account_id, false, &mut out).await;

        assert_eq!(
            out.accounts_skipped_org, 0,
            "a personal org must never be counted as a skipped org account"
        );
        assert_eq!(out.workspaces_seen, 1, "its legacy workspace must still be seen (dry run)");
    }

    /// A REAL org account — `clerk_org_id` set, no `personal_orgs` row — must
    /// still be skipped. Pins that the fix narrowed the skip to exclude
    /// personal orgs specifically, rather than disabling it altogether.
    #[tokio::test]
    async fn a_real_org_account_is_still_skipped_pg() {
        let Some(pool) = pg_test_pool("real_org_still_skipped_pg").await else {
            return;
        };
        let user_id = seed_user(&pool, "real-org", None).await;
        let clerk_org_id = format!("org_real_slackbf_{}", Uuid::new_v4());
        let account_id: Uuid = sqlx::query_scalar(
            "INSERT INTO accounts (owner_user_id, clerk_org_id) VALUES ($1, $2) RETURNING id",
        )
        .bind(user_id)
        .bind(&clerk_org_id)
        .fetch_one(&pool)
        .await
        .expect("seed real org account");
        seed_legacy_integration(&pool, account_id, "T_REAL_ORG").await;

        let mut out = SlackBackfillOutcome::default();
        process_account(&pool, account_id, false, &mut out).await;

        assert_eq!(out.accounts_skipped_org, 1, "a real org account must still be skipped");
        assert_eq!(
            out.workspaces_seen, 0,
            "a skipped account's legacy workspace must never be counted as seen"
        );
    }
}
