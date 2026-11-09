//! The GitHub connector's conformance suite — `docs/connectors/github.md` §8,
//! which is `CONNECTOR_CONTRACT.md` §6's required deliverable ("a passing
//! conformance list").
//!
//! # Why this lives in the crate rather than `backend/tests/`
//!
//! Every function these tests certify is `pub(crate)` (`github_repos::
//! resolve_destination`, `github_disconnect::pause_repos`, `github_backfill::
//! persist_cursor_and_progress`, …). An integration test in `backend/tests/`
//! cannot reach a crate-private item, and Rust refuses to `pub use` one, so
//! reaching them from there would mean widening the real API surface for the
//! benefit of tests. This module is `#[cfg(test)]`-only, compiles into the
//! `--lib` test binary, and uses the same scratch-database harness shape
//! `connectors::github_repos`'s own pg tests and `seats.rs` already use.
//!
//! # What is covered HERE, and what is covered elsewhere
//!
//! The contract's list is satisfied across three places. Items already
//! pinned elsewhere are NOT duplicated here — they are named so the list can
//! be audited without grepping:
//!
//! | §8 item | Where |
//! |---|---|
//! | 1 cross-member token isolation | HERE: [`conformance_1_cross_member_token_isolation_pg`] + [`conformance_1_no_owner_fallback_query_shape_exists`] |
//! | 2 tenant-resolution refusal | `routes::github::tests::webhook_unknown_installation_writes_nothing` |
//! | 3 no consumer-plane fallback | `routes::github::tests::webhook_suspended_installation_writes_nothing` |
//! | 4 seating | HERE: [`conformance_4_seating_auto_seats_mapped_and_counts_unmapped_pg`] and [`conformance_4_personal_destination_seats_nobody_pg`]; the three refusal halves are `connectors::github_repos::github_repos_destination_gate_pg_tests` (org needs a CoralSwarm org admin, an existing team needs a team admin, personal is always allowed) |
//! | 5 window enforcement | HERE: [`conformance_5_window_enforcement_pg`] |
//! | 6 no silent truncation | HERE, PR B: [`conformance_6_file_ceiling_pauses_rather_than_truncating_pg`] (file-count over the cap) and [`conformance_6_truncated_tree_pauses_even_under_the_count_cap_pg`] (GitHub's own `truncated: true`) |
//! | 7 departure pauses one repo | HERE: [`conformance_7_departure_pauses_exactly_that_repo_pg`] |
//! | 8 disconnect does not remove content | HERE: [`conformance_8_disconnect_keeps_every_atom_pg`] |
//! | 9 sensitive-path discipline | HERE: [`conformance_9_no_everything_mode_path_exists`] |
//! | webhook signature rejection | `routes::github::tests::webhook_tampered_signature_returns_401` / `webhook_missing_signature_header_returns_401` |
//! | resume from cursors after a kill (narrative) | HERE: [`conformance_resume_from_cursor_after_a_kill_pg`] |
//! | resume from cursor after a kill (code, PR B) | HERE: [`conformance_code_resume_from_cursor_after_a_kill_pg`] |
//! | forced push re-fetches the compare range | `connectors::github_event_ingest::tests::forced_push_uses_the_same_compare_range` |
//! | a `current` streak still forces a rewrite | HERE: [`conformance_summary_force_after_guard_pg`] — PR B part 2's summary tier |
//! | (PR B part 2) staleness gate: stale/current/Jev-failure | HERE: [`conformance_summary_stale_verdict_rewrites_and_resets_pg`], [`conformance_summary_current_verdict_keeps_and_increments_pg`], [`conformance_summary_jev_failure_counts_as_unjudged_and_skips_pg`] |
//! | §3.9/D10 a shared (team/org) destination actually receives content | HERE: [`conformance_shared_ocean_destinations_actually_receive_atoms_pg`] |
//! | `atoms.symbols` (PR #1040) survives the Path-1 write | HERE: [`conformance_symbols_survive_the_path_1_write_pg`] |
//! | re-capturing unchanged content mints no version (2026-10-18 sweep churn) | HERE: [`conformance_unchanged_recapture_mints_one_version_pg`]; the push/sweep time derivation is `connectors::github_event_ingest::tests::push_code_time_is_the_after_commit_not_the_wall_clock` |
//! | a push webhook advances the head cursor, the sweep skips covered ranges and reconciles the rest | HERE: [`conformance_head_cursor_webhook_then_sweep_fetches_nothing_pg`], [`conformance_head_cursor_dropped_webhook_sweep_catches_up_pg`], [`conformance_head_cursor_failed_webhook_holds_and_sweep_recovers_pg`], [`conformance_head_cursor_out_of_order_and_force_push_pg`]; the CAS SQL under both roles is `connectors::github_event_ingest::tests::advance_head_cursor_is_a_compare_and_swap_under_both_roles_pg` |
//! | a failed tick is visible on the repo row, not only in `jobs` | HERE: [`conformance_a_failing_tick_is_visible_on_the_repo_row_pg`] |
//! | destination reachability is re-checked on every Path-1 write | HERE: [`conformance_a_departed_member_cannot_keep_writing_to_the_team_ocean_pg`] |
//! | losing a destination PAUSES the watch (the retry loop stops) | HERE: [`conformance_removing_a_team_member_pauses_their_github_watches_pg`] and [`conformance_an_unreachable_destination_pauses_itself_on_the_next_write_pg`] |
//!
//! # Running it
//!
//! Env-gated exactly like every other pg test in this crate. **A test that
//! prints `skipping` asserted nothing** — an unset DSN makes the whole suite
//! report `ok` in ~0.00s, which is the trap `pg_tests pass vacuously without
//! a DB` describes. Always run it with the DSN set:
//!
//! ```text
//! ORGT_TEST_BASE_URL=postgres://alex@localhost:5432/postgres \
//!   cargo test -p coralswarm-backend --lib github_conformance
//! ```


use std::sync::Arc;

use sqlx::postgres::PgPoolOptions;
use sqlx::{Connection, Executor, PgConnection, PgPool};
use uuid::Uuid;

use super::github_backfill::{page_url_for, NarrativeCursor, TypeKind};
use super::github_narrative::NormalizedAtom;
use super::github_disconnect::pause_repos;
use super::github_repos::{seat_repo_collaborators, DestinationKind, RequestedDestination};
use super::TokenOwner;
use crate::tenancy::TenantId;

/// One write under a watch row, resolving its grant for the call: the shape
/// production had when the grant was resolved per atom. The tick and the
/// webhook now resolve once (`resolve_repo_grant`) and write many; these
/// tests still drive single writes and read the refusal off the same call.
async fn ingest_one(
    state: &Arc<crate::AppState>,
    tenant: &TenantId,
    pool: &PgPool,
    account_id: Uuid,
    repo_row_id: Uuid,
    atom: NormalizedAtom,
) -> Result<super::github_backfill::IngestOutcome, String> {
    let grant = resolve_repo_grant(state, tenant, pool, account_id, repo_row_id).await?;
    super::github_backfill::ingest_one(state, tenant, pool, account_id, &grant, atom).await
}

/// Load the watch row and resolve its grant, as the tick and the webhook do
/// from the row they already hold.
async fn resolve_repo_grant(
    state: &Arc<crate::AppState>,
    tenant: &TenantId,
    pool: &PgPool,
    account_id: Uuid,
    repo_row_id: Uuid,
) -> Result<crate::ingest::OrgSharedGrant, String> {
    let binding = super::github_backfill::load_binding(pool, account_id, repo_row_id)
        .await
        .map_err(|e| format!("watch row {repo_row_id}: {e}"))?
        .ok_or_else(|| format!("watch row {repo_row_id} is not account {account_id}'s"))?;
    super::github_backfill::resolve_repo_grant(state, tenant, pool, account_id, &binding).await
}

/// [`ingest_one`]'s shape for `write_chunks_to_destinations`.
async fn write_chunks_to_destinations(
    state: &Arc<crate::AppState>,
    tenant: &TenantId,
    pool: &PgPool,
    account_id: Uuid,
    repo_row_id: Uuid,
    atom: &NormalizedAtom,
    chunks: &[coralswarm_core::chunker::Chunk],
) -> super::github_backfill::WriteOutcome {
    match resolve_repo_grant(state, tenant, pool, account_id, repo_row_id).await {
        Ok(grant) => super::github_backfill::write_chunks_to_destinations(state, tenant, pool, account_id, &grant, atom, chunks).await,
        Err(why) => super::github_backfill::WriteOutcome {
            chunks_created: chunks.len(),
            destinations_failed: 1,
            first_destination_error: Some(why),
            ..Default::default()
        },
    }
}

// ---------------------------------------------------------------------------
// Scratch-database harness — same shape as
// `connectors::github_repos::github_repos_destination_gate_pg_tests`.
// ---------------------------------------------------------------------------

fn base_url() -> Option<String> {
    std::env::var("ORGT_TEST_BASE_URL").ok()
}

fn with_db(base: &str, db: &str) -> String {
    let (prefix, rest) = base.split_at(base.rfind('/').expect("url has a path /"));
    let params = rest.find('?').map(|i| &rest[i..]).unwrap_or("");
    format!("{prefix}/{db}{params}")
}

async fn recreate(base: &str, db: &str) -> PgConnection {
    let mut conn = PgConnection::connect(base).await.expect("connect admin");
    conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.unwrap();
    conn.execute(format!("CREATE DATABASE {db}").as_str()).await.unwrap();
    conn
}

async fn drop_db(admin: &mut PgConnection, db: &str) {
    admin.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
}

/// One pool serving both planes — the Personal-tenancy shape, the same
/// single-DB-serves-both-planes precedent `connectors::google_disconnect`'s
/// and `github_repos`' own pg tests use. The full control tree runs, so
/// `github_watched_repos` (0235) and `github_installations` (0236) are the
/// real migrated tables, not a hand-written copy.
async fn build_pool(base: &str, db: &str) -> PgPool {
    let pool: PgPool = PgPoolOptions::new().max_connections(4).connect(&with_db(base, db)).await.unwrap();
    sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();
    pool
}

struct Fixture {
    account_id: Uuid,
    user_id: Uuid,
    graph_id: Uuid,
}

/// An account, its owner (also an `account_members` row so the C5 mapping
/// join can reach them), and a primary personal ocean.
async fn seed_account(pool: &PgPool, tag: &str) -> Fixture {
    let account_id = Uuid::new_v4();
    let user_id = Uuid::new_v4();
    sqlx::query("INSERT INTO users (id, email) VALUES ($1, $2)")
        .bind(user_id)
        .bind(format!("{tag}-owner-{user_id}@example.com"))
        .execute(pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO accounts (id, owner_user_id, clerk_org_id) VALUES ($1, $2, $3)")
        .bind(account_id)
        .bind(user_id)
        .bind(format!("org_{tag}_{}", &account_id.simple().to_string()[..8]))
        .execute(pool)
        .await
        .unwrap();
    // `seat_tier_id` is explicit on purpose: the
    // `account_members_default_seat_tier` trigger refuses a silent default
    // ("PR F (seat lifecycle) requires every writer of account_members to
    // pass one explicitly").
    sqlx::query(
        "INSERT INTO account_members (account_id, user_id, role, seat_tier_id) \
         VALUES ($1, $2, 'admin', (SELECT id FROM seat_tiers WHERE code = 'standard' AND is_active = true))",
    )
        .bind(account_id)
        .bind(user_id)
        .execute(pool)
        .await
        .unwrap();
    // This account carries a `clerk_org_id`, so it is on the ORG plane
    // (`routes::slack::account_plane`), and on that plane a member's
    // "personal" destination is their MEMBER ocean — the row
    // `workers::member_ocean_reconciler` provisions: `kind = 'member'`,
    // `owner_user_id = <the member>`, `is_primary = true`. A `'personal'`-kind
    // primary graph is the CONSUMER-plane shape and does not resolve here;
    // that mismatch is what turned `conformance_4_personal_destination_
    // seats_nobody_pg` red once it started calling the production
    // `resolve_destination` path (and what the first real save on the
    // selection page hit locally, for the same reason).
    let graph_id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO graphs (id, account_id, name, kind, owner_user_id, is_primary) \
         VALUES ($1, $2, 'My ocean', 'member', $3, true)",
    )
    .bind(graph_id)
    .bind(account_id)
    .bind(user_id)
    .execute(pool)
    .await
    .unwrap();
    Fixture { account_id, user_id, graph_id }
}

/// Provision a real DEK for `account_id` so a REAL `github_backfill::
/// ingest_one` call can actually land content — its Path-1 write bails
/// without committing anything when the account has no DEK (a null-key
/// "encryption" would be a security hole, so the absence is fatal to the
/// write rather than papered over). Same `MASTER_KEK_HEX_ENV_LOCK` +
/// `coralswarm_core::kek::{load_kek, generate_dek, wrap_dek}` +
/// `UPDATE accounts SET dek_wrapped` shape `connectors::google_chat`'s own
/// real-ingest pg tests already use. Caller must hold
/// `crate::workers::reembed::MASTER_KEK_HEX_ENV_LOCK` for its whole test
/// body (process-global env var) — this function does not acquire it
/// itself so a caller doing several real-ingest calls only pays the lock
/// once.
async fn provision_dek(pool: &PgPool, account_id: Uuid) {
    unsafe {
        std::env::set_var("MASTER_KEK_HEX", "1".repeat(64));
    }
    let kek = coralswarm_core::kek::load_kek().expect("MASTER_KEK_HEX set above");
    let dek = coralswarm_core::kek::generate_dek();
    let wrapped = coralswarm_core::kek::wrap_dek(&kek, &dek).unwrap();
    sqlx::query("UPDATE accounts SET dek_wrapped = $1 WHERE id = $2").bind(&wrapped).bind(account_id).execute(pool).await.unwrap();
}

/// A member of `account_id` with an exact `users.email` — the only thing the
/// C5 collaborator mapping matches on.
async fn seed_member(pool: &PgPool, account_id: Uuid, email: &str) -> Uuid {
    let user_id = Uuid::new_v4();
    sqlx::query("INSERT INTO users (id, email) VALUES ($1, $2)")
        .bind(user_id)
        .bind(email)
        .execute(pool)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO account_members (account_id, user_id, role, seat_tier_id) \
         VALUES ($1, $2, 'member', (SELECT id FROM seat_tiers WHERE code = 'standard' AND is_active = true))",
    )
        .bind(account_id)
        .bind(user_id)
        .execute(pool)
        .await
        .unwrap();
    user_id
}

async fn seed_team(pool: &PgPool, account_id: Uuid, created_by: Uuid, name: &str) -> (Uuid, Uuid) {
    let graph_id = Uuid::new_v4();
    sqlx::query("INSERT INTO graphs (id, account_id, name, kind, is_primary) VALUES ($1, $2, $3, 'team', false)")
        .bind(graph_id)
        .bind(account_id)
        .bind(name)
        .execute(pool)
        .await
        .unwrap();
    let team_id = Uuid::new_v4();
    sqlx::query("INSERT INTO teams (id, account_id, graph_id, name, created_by) VALUES ($1, $2, $3, $4, $5)")
        .bind(team_id)
        .bind(account_id)
        .bind(graph_id)
        .bind(name)
        .bind(created_by)
        .execute(pool)
        .await
        .unwrap();
    (team_id, graph_id)
}

#[allow(clippy::too_many_arguments)]
async fn seed_watch_row(
    pool: &PgPool,
    account_id: Uuid,
    installation_id: i64,
    repo_id: i64,
    name: &str,
    active: bool,
    graph_id: Option<Uuid>,
    owner_user_id: Option<Uuid>,
) -> Uuid {
    let row_id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO github_watched_repos \
         (id, account_id, installation_id, repo_id, owner, name, default_branch, visibility, \
          archived, fork, size_kb, active, graph_id, owner_user_id, destination_kind) \
         VALUES ($1, $2, $3, $4, 'acme', $5, 'main', 'private', false, false, 100, $6, $7, $8, 'personal')",
    )
    .bind(row_id)
    .bind(account_id)
    .bind(installation_id)
    .bind(repo_id)
    .bind(name)
    .bind(active)
    .bind(graph_id)
    .bind(owner_user_id)
    .execute(pool)
    .await
    .unwrap();
    row_id
}

/// One `atoms` row shaped like an ingested GitHub pull request.
async fn seed_atom(pool: &PgPool, graph_id: Uuid, source_type: &str, source_id: &str, ts: chrono::DateTime<chrono::Utc>) -> Uuid {
    let atom_id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO atoms (atom_id, graph_id, source_type, source_id, content_blob_key, participants, timestamp, scope) \
         VALUES ($1, $2, $3, $4, $5, ARRAY[]::text[], $6, 'private')",
    )
    .bind(atom_id)
    .bind(graph_id)
    .bind(source_type)
    .bind(source_id)
    .bind(format!("test/{atom_id}"))
    .bind(ts)
    .execute(pool)
    .await
    .unwrap();
    atom_id
}

/// Every GitHub source file, for the two static-discipline assertions. Read
/// from disk rather than `include_str!` so a NEW `github_*.rs` module cannot
/// quietly escape the scan by not being listed here.
fn github_source_files() -> Vec<(String, String)> {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut out = Vec::new();
    for dir in ["connectors", "workers", "routes"] {
        let d = root.join(dir);
        for entry in std::fs::read_dir(&d).expect("read source dir") {
            let path = entry.expect("dir entry").path();
            let Some(fname) = path.file_name().and_then(|f| f.to_str()) else { continue };
            // This file is deliberately excluded: it names the very
            // strings the two static assertions forbid (as their needles),
            // so scanning itself would always "find" them. Everything it
            // certifies lives in the other github_* modules.
            if fname == "github_conformance.rs" {
                continue;
            }
            if fname.starts_with("github") && fname.ends_with(".rs") {
                let body = std::fs::read_to_string(&path).expect("read source file");
                out.push((format!("{dir}/{fname}"), body));
            }
        }
    }
    assert!(out.len() >= 7, "expected every github_* module to be scanned, found {}: {:?}", out.len(), out.iter().map(|(n, _)| n).collect::<Vec<_>>());
    out
}

/// Snapshots and restores `GITHUB_API_BASE`, `GITHUB_APP_ID`, and
/// `GITHUB_APP_PRIVATE_KEY`. Holds `test_env_lock` for the guard's lifetime
/// so a panic during an awaited call still restores process-global env.
struct GithubEnvGuard {
    prev_api_base: Option<String>,
    prev_app_id: Option<String>,
    prev_app_key: Option<String>,
    _lock: std::sync::MutexGuard<'static, ()>,
}

impl GithubEnvGuard {
    fn apply(api_base: &str) -> Self {
        let _lock = super::github::test_env_lock().lock().unwrap();
        let prev_api_base = std::env::var("GITHUB_API_BASE").ok();
        let prev_app_id = std::env::var("GITHUB_APP_ID").ok();
        let prev_app_key = std::env::var("GITHUB_APP_PRIVATE_KEY").ok();
        // Process-global env is serialized by `test_env_lock` for this
        // guard's lifetime; Drop restores the snapshot if the awaited
        // seating call panics.
        std::env::set_var("GITHUB_API_BASE", api_base);
        std::env::set_var("GITHUB_APP_ID", "5023902");
        std::env::set_var("GITHUB_APP_PRIVATE_KEY", super::github::test_throwaway_rsa_pem());
        Self { prev_api_base, prev_app_id, prev_app_key, _lock }
    }
}

impl Drop for GithubEnvGuard {
    fn drop(&mut self) {
        restore_env("GITHUB_API_BASE", &self.prev_api_base);
        restore_env("GITHUB_APP_ID", &self.prev_app_id);
        restore_env("GITHUB_APP_PRIVATE_KEY", &self.prev_app_key);
    }
}

fn restore_env(key: &str, prev: &Option<String>) {
    match prev {
        Some(v) => std::env::set_var(key, v),
        None => std::env::remove_var(key),
    }
}

// ---------------------------------------------------------------------------
// A mock GitHub, bound to an ephemeral loopback port.
// ---------------------------------------------------------------------------

/// Serves the three endpoints the seating path calls: the installation-token
/// mint, the collaborator list, and each collaborator's public profile.
/// Reached because `connectors::github::github_api_base` reads
/// `GITHUB_API_BASE` at call time — the same seam
/// `connectors::slack::slack_api_base` provides for Slack.
async fn spawn_mock_github() -> (String, tokio::task::JoinHandle<()>) {
    use axum::extract::Path;
    use axum::routing::{get, post};
    use axum::{Json, Router};
    use serde_json::json;

    let app = Router::new()
        .route(
            "/app/installations/{installation_id}/access_tokens",
            post(|Path(_id): Path<String>| async move {
                Json(json!({
                    "token": "ghs_mock_installation_token",
                    "expires_at": (chrono::Utc::now() + chrono::Duration::hours(1)).to_rfc3339(),
                }))
            }),
        )
        .route(
            "/repos/{owner}/{repo}/collaborators",
            get(|Path((_o, _r)): Path<(String, String)>| async move {
                // Three collaborators: one maps, one has an email nobody
                // owns, one exposes no public email at all.
                Json(json!([
                    {"login": "mapped-dev"},
                    {"login": "stranger-dev"},
                    {"login": "private-dev"},
                ]))
            }),
        )
        .route(
            "/users/{login}",
            get(|Path(login): Path<String>| async move {
                let email = match login.as_str() {
                    "mapped-dev" => Some("mapped-dev@example.com"),
                    "stranger-dev" => Some("nobody-here@example.com"),
                    _ => None,
                };
                Json(json!({"login": login, "email": email}))
            }),
        );

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind mock github");
    let addr = listener.local_addr().expect("mock github addr");
    let handle = tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    (format!("http://{addr}"), handle)
}

// ---------------------------------------------------------------------------
// §8.1 — cross-member token isolation (I7)
// ---------------------------------------------------------------------------

/// Two members of ONE account each connect GitHub personally. Each load
/// returns that member's own token, and the storage shape makes a
/// cross-member read structurally impossible rather than merely unlikely:
/// `uq_oauth_tokens_per_owner` keys on `owner_user_id`, so the two rows
/// coexist, and an owner-scoped read can only ever match one of them.
///
/// In an org every member shares one `account_id`, so `owner_user_id` is the
/// ONLY thing separating them — which is why the contract states I7 as "no
/// fallback read" rather than "scope your queries".
#[tokio::test]
async fn conformance_1_cross_member_token_isolation_pg() {
    let Some(base) = base_url() else {
        eprintln!("skipping conformance_1_cross_member_token_isolation_pg: set ORGT_TEST_BASE_URL");
        return;
    };
    let db = "cs_gh_conf_token_isolation";
    let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
    let mut admin = recreate(&base, db).await;
    let pool = build_pool(&base, db).await;

    let f = seed_account(&pool, "tokiso").await;
    let m1 = seed_member(&pool, f.account_id, "member-one@example.com").await;
    let m2 = seed_member(&pool, f.account_id, "member-two@example.com").await;

    for (owner, token) in [(m1, b"token-for-m1".as_slice()), (m2, b"token-for-m2".as_slice())] {
        sqlx::query(
            "INSERT INTO oauth_tokens \
             (id, account_id, provider, scope, access_token, token_type, provider_workspace_id, owner_user_id) \
             VALUES ($1, $2, 'github', 'repo', $3, 'bearer', '163561452', $4)",
        )
        .bind(Uuid::new_v4())
        .bind(f.account_id)
        .bind(token)
        .bind(owner)
        .execute(&pool)
        .await
        .unwrap();
    }

    // Production I7 loader: TokenOwner::Member, no account-wide fallback.
    for (owner, expected) in [(m1, b"token-for-m1".as_slice()), (m2, b"token-for-m2".as_slice())] {
        let row = super::github::load_member_oauth_token_ciphertext(&pool, f.account_id, TokenOwner::Member(owner))
            .await
            .expect("production member token loader");
        assert_eq!(row.as_deref(), Some(expected), "each member must get its OWN token back");
    }

    // The account-wide shape (owner_user_id IS NULL) is a DIFFERENT row that
    // does not exist here — so a connector that forgot the owner predicate
    // would read nothing rather than silently borrowing a member's token.
    let account_wide: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM oauth_tokens \
         WHERE account_id = $1 AND provider = 'github' AND owner_user_id IS NULL",
    )
    .bind(f.account_id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(account_wide, 0);

    // A second row for the SAME owner is refused by the partial unique index.
    let dup = sqlx::query(
        "INSERT INTO oauth_tokens \
         (id, account_id, provider, scope, access_token, token_type, provider_workspace_id, owner_user_id) \
         VALUES ($1, $2, 'github', 'repo', $3, 'bearer', '163561452', $4)",
    )
    .bind(Uuid::new_v4())
    .bind(f.account_id)
    .bind(b"second-token-for-m1".as_slice())
    .bind(m1)
    .execute(&pool)
    .await;
    assert!(dup.is_err(), "uq_oauth_tokens_per_owner must refuse a second row for one owner");

    pool.close().await;
    drop_db(&mut admin, db).await;
}

/// The structural half of I7: **no GitHub module may contain an
/// `owner_user_id IS NULL OR …` shaped token read.** That exact predicate is
/// the cross-member credential leak `TokenOwner` exists to make impossible,
/// and it is the one thing a future edit could reintroduce while every
/// behavioural test above still passed (because it only widens what a read
/// CAN match, and the fixture above has no account-wide row to match).
#[test]
fn conformance_1_no_owner_fallback_query_shape_exists() {
    for (name, body) in github_source_files() {
        let flat = body.split_whitespace().collect::<Vec<_>>().join(" ");
        assert!(
            !flat.contains("owner_user_id IS NULL OR"),
            "{name} contains the forbidden owner-fallback read shape (contract I7)"
        );
    }
}

// ---------------------------------------------------------------------------
// §8.4 — seating
// ---------------------------------------------------------------------------

/// Routing a repo to a NEW team ocean auto-seats its **mapped** collaborators
/// (I4: nobody's content lands somewhere they cannot read it) and **reports**
/// the unmapped ones (C5: never guess, never silently drop).
///
/// Drives the real `seat_repo_collaborators` against a mock GitHub, so the
/// collaborator list, the public-email lookup, and the exact-match-against-
/// `users.email`-for-a-current-member rule are all genuinely exercised. Three
/// collaborators, one of each outcome: mapped, email-belongs-to-nobody, and
/// no-public-email.
#[tokio::test]
async fn conformance_4_seating_auto_seats_mapped_and_counts_unmapped_pg() {
    let Some(base) = base_url() else {
        eprintln!("skipping conformance_4_seating_auto_seats_mapped_and_counts_unmapped_pg: set ORGT_TEST_BASE_URL");
        return;
    };
    let db = "cs_gh_conf_seating";
    let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
    let mut admin = recreate(&base, db).await;
    let pool = build_pool(&base, db).await;

    let f = seed_account(&pool, "seating").await;
    // Only this one collaborator is a CoralSwarm member of this account.
    let mapped_user = seed_member(&pool, f.account_id, "mapped-dev@example.com").await;
    let (team_id, team_graph_id) = seed_team(&pool, f.account_id, f.user_id, "acme/repo").await;

    let (mock_base, mock) = spawn_mock_github().await;
    let outcome = {
        let _env = GithubEnvGuard::apply(&mock_base);
        let http = reqwest::Client::new();
        // A per-test installation id: `installation_token` caches by id in
        // process, so a shared one would let another test's token win.
        seat_repo_collaborators(
            &http,
            &pool,
            &pool,
            f.account_id,
            team_id,
            910_001,
            5_550_001,
            "acme",
            "repo",
        )
        .await
        .expect("seating runs against the mock")
    };
    mock.abort();

    assert_eq!(outcome.seated, 1, "exactly the one mapped collaborator is seated");
    assert_eq!(outcome.unmapped, 2, "the stranger and the no-public-email collaborator are REPORTED");

    // I4 in the database: the mapped member really can read the team ocean…
    let seated: Vec<Uuid> = sqlx::query_scalar("SELECT user_id FROM team_seat_grants WHERE team_id = $1")
        .bind(team_id)
        .fetch_all(&pool)
        .await
        .unwrap();
    assert_eq!(seated, vec![mapped_user], "only the mapped collaborator holds a seat");

    // …and C5: an unmapped collaborator was never invented as a user.
    let strangers: i64 = sqlx::query_scalar("SELECT count(*) FROM users WHERE email = 'nobody-here@example.com'")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(strangers, 0, "an unmapped collaborator must never be conjured into a user row");
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM graphs WHERE id = $1")
            .bind(team_graph_id)
            .fetch_one(&pool)
            .await
            .unwrap(),
        1
    );

    pool.close().await;
    drop_db(&mut admin, db).await;
}

/// The personal destination seats **nobody** — it is the chooser's own ocean,
/// so there is no promotion decision and no roster to widen (contract I3/I4
/// are both trivially satisfied, and that is worth pinning: a future edit
/// that "helpfully" seated collaborators on a personal pick would leak a
/// private ocean to a repo's collaborator list).
#[tokio::test]
async fn conformance_4_personal_destination_seats_nobody_pg() {
    let Some(base) = base_url() else {
        eprintln!("skipping conformance_4_personal_destination_seats_nobody_pg: set ORGT_TEST_BASE_URL");
        return;
    };
    let db = "cs_gh_conf_personal_seats";
    let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
    let mut admin = recreate(&base, db).await;
    let pool = build_pool(&base, db).await;

    let f = seed_account(&pool, "personalseat").await;
    seed_member(&pool, f.account_id, "mapped-dev@example.com").await;
    let resolved = super::github_repos::resolve_destination(
        &pool,
        &pool,
        f.account_id,
        f.user_id,
        RequestedDestination::Personal,
        "acme",
        "repo",
    )
    .await
    .expect("production personal destination path");
    assert_eq!(resolved.kind, DestinationKind::Personal);
    assert_eq!(resolved.graph_id, f.graph_id);
    assert!(resolved.newly_created_team.is_none());
    let _row_id = seed_watch_row(&pool, f.account_id, 910_002, 5_550_002, "repo", true, Some(resolved.graph_id), Some(f.user_id)).await;

    let seats: i64 = sqlx::query_scalar("SELECT count(*) FROM team_seat_grants WHERE account_id = $1")
        .bind(f.account_id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(seats, 0, "a personal destination must seat nobody");

    pool.close().await;
    drop_db(&mut admin, db).await;
}

// ---------------------------------------------------------------------------
// §8.5 — window enforcement
// ---------------------------------------------------------------------------

/// `history_from` bounds **when**, never **which source** (contract I6). A
/// member seated with a `history_from` AFTER an atom's timestamp cannot reach
/// that atom; the same member's later-timestamped atom is reachable. Asserted
/// through the window predicate over real rows, which is the shape the
/// retrieval path applies.
#[tokio::test]
async fn conformance_5_window_enforcement_pg() {
    let Some(base) = base_url() else {
        eprintln!("skipping conformance_5_window_enforcement_pg: set ORGT_TEST_BASE_URL");
        return;
    };
    let db = "cs_gh_conf_window";
    let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
    let mut admin = recreate(&base, db).await;
    let pool = build_pool(&base, db).await;

    let f = seed_account(&pool, "window").await;
    let late_joiner = seed_member(&pool, f.account_id, "late-joiner@example.com").await;
    let (team_id, team_graph_id) = seed_team(&pool, f.account_id, f.user_id, "acme/windowed").await;

    let cutoff = chrono::Utc::now() - chrono::Duration::days(7);
    let before = seed_atom(&pool, team_graph_id, "github_pr", "5550003:pr:1", cutoff - chrono::Duration::days(3)).await;
    let after = seed_atom(&pool, team_graph_id, "github_pr", "5550003:pr:2", cutoff + chrono::Duration::days(3)).await;

    sqlx::query(
        "INSERT INTO team_seat_grants (id, account_id, team_id, user_id, source, history_from) \
         VALUES ($1, $2, $3, $4, 'github_repo:5550003', $5)",
    )
    .bind(Uuid::new_v4())
    .bind(f.account_id)
    .bind(team_id)
    .bind(late_joiner)
    .bind(cutoff)
    .execute(&pool)
    .await
    .unwrap();

    let mut tx = crate::tenancy::scope::begin_tenant_scoped(&pool, f.account_id, late_joiner)
        .await
        .expect("user-scoped retrieval transaction");
    let visible: Vec<Uuid> = sqlx::query_scalar(
        "SELECT a.atom_id FROM atoms a \
         JOIN teams t ON t.graph_id = a.graph_id \
         JOIN team_seat_grants g ON g.team_id = t.id AND g.user_id = $1 \
         WHERE a.graph_id = $2 AND (g.history_from IS NULL OR a.timestamp >= g.history_from)",
    )
    .bind(late_joiner)
    .bind(team_graph_id)
    .fetch_all(&mut *tx)
    .await
    .unwrap();
    tx.rollback().await.ok();

    assert_eq!(visible, vec![after], "only atoms at or after history_from are inside the window");
    assert!(!visible.contains(&before), "an atom older than history_from must stay outside the window");

    pool.close().await;
    drop_db(&mut admin, db).await;
}

// ---------------------------------------------------------------------------
// §8.7 — departure
// ---------------------------------------------------------------------------

/// Removing one repo from the installation's grant pauses **that repo and
/// nothing else** — the blast radius of a departure is exactly one container.
#[tokio::test]
async fn conformance_7_departure_pauses_exactly_that_repo_pg() {
    let Some(base) = base_url() else {
        eprintln!("skipping conformance_7_departure_pauses_exactly_that_repo_pg: set ORGT_TEST_BASE_URL");
        return;
    };
    let db = "cs_gh_conf_departure";
    let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
    let mut admin = recreate(&base, db).await;
    let pool = build_pool(&base, db).await;

    let f = seed_account(&pool, "departure").await;
    let departing = seed_watch_row(&pool, f.account_id, 910_004, 5_550_005, "leaving", true, Some(f.graph_id), Some(f.user_id)).await;
    let staying = seed_watch_row(&pool, f.account_id, 910_004, 5_550_006, "staying", true, Some(f.graph_id), Some(f.user_id)).await;
    // A second installation's repo, to prove the pause is not
    // installation-wide either.
    let other_install = seed_watch_row(&pool, f.account_id, 910_005, 5_550_007, "elsewhere", true, Some(f.graph_id), Some(f.user_id)).await;

    let paused = pause_repos(&pool, f.account_id, 910_004, Some(5_550_005), "repo_removed_from_grant")
        .await
        .expect("pause the departing repo");
    assert_eq!(paused, 1, "exactly one row is paused");

    for (row_id, label, expect_active) in
        [(departing, "departing", false), (staying, "staying", true), (other_install, "other installation", true)]
    {
        let active: bool = sqlx::query_scalar("SELECT active FROM github_watched_repos WHERE id = $1")
            .bind(row_id)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(active, expect_active, "{label} repo's active flag");
    }

    let (reason, stopped): (Option<String>, Option<chrono::DateTime<chrono::Utc>>) =
        sqlx::query_as("SELECT paused_reason, sync_stopped_at FROM github_watched_repos WHERE id = $1")
            .bind(departing)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(reason.as_deref(), Some("repo_removed_from_grant"), "the pause states WHY");
    assert!(stopped.is_some(), "§3.8's visible 'sync stopped since <date>' needs a timestamp");

    pool.close().await;
    drop_db(&mut admin, db).await;
}

// ---------------------------------------------------------------------------
// §8.8 — disconnect does not remove content
// ---------------------------------------------------------------------------

/// **Never purge** (§3.8). Uninstall, repo-removal and toggle-off all pause
/// and keep every atom; removal stays a per-item choice through the library
/// and reef controls. This is the pinned version of that promise: pausing an
/// entire installation leaves the ingested atoms untouched.
#[tokio::test]
async fn conformance_8_disconnect_keeps_every_atom_pg() {
    let Some(base) = base_url() else {
        eprintln!("skipping conformance_8_disconnect_keeps_every_atom_pg: set ORGT_TEST_BASE_URL");
        return;
    };
    let db = "cs_gh_conf_disconnect";
    let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
    let mut admin = recreate(&base, db).await;
    let pool = build_pool(&base, db).await;

    let f = seed_account(&pool, "disconnect").await;
    let row_id = seed_watch_row(&pool, f.account_id, 910_006, 5_550_008, "kept", true, Some(f.graph_id), Some(f.user_id)).await;
    let pr_atom = seed_atom(&pool, f.graph_id, "github_pr", "5550008:pr:7", chrono::Utc::now()).await;
    let issue_atom = seed_atom(&pool, f.graph_id, "github_issue", "5550008:issue:9", chrono::Utc::now()).await;

    let paused = super::github_disconnect::pause_all_for_account(&pool, f.account_id)
        .await
        .expect("pause the whole account's github rows");
    assert_eq!(paused, 1);

    for (atom_id, label) in [(pr_atom, "pull request"), (issue_atom, "issue")] {
        let still_there: i64 = sqlx::query_scalar("SELECT count(*) FROM atoms WHERE atom_id = $1")
            .bind(atom_id)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(still_there, 1, "disconnect must keep the {label} atom");
    }

    let (active, reason, stopped): (bool, Option<String>, Option<chrono::DateTime<chrono::Utc>>) =
        sqlx::query_as("SELECT active, paused_reason, sync_stopped_at FROM github_watched_repos WHERE id = $1")
            .bind(row_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(!active);
    assert!(reason.is_some(), "a paused row must say why");
    assert!(stopped.is_some());

    pool.close().await;
    drop_db(&mut admin, db).await;
}

// ---------------------------------------------------------------------------
// §8.9 — sensitive-path discipline
// ---------------------------------------------------------------------------

/// The GitHub connector has **no correspondence mode**, so §8.9's test is the
/// absence of one: no everything-mode path may exist in any GitHub module.
/// Gmail and Outlook have `*_everything_mode` workers by deliberate design;
/// a repository is a container with a gate event (C2), so the same escape
/// hatch would bypass §3.3's "grant, then choose" entirely.
#[test]
fn conformance_9_no_everything_mode_path_exists() {
    for (name, body) in github_source_files() {
        for forbidden in ["everything_mode", "EverythingMode", "EVERYTHING_MODE"] {
            assert!(
                !body.contains(forbidden),
                "{name} references {forbidden}: the GitHub connector must have no everything-mode path (§8.9)"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Connector-specific: resume from cursors after a kill mid-backfill
// ---------------------------------------------------------------------------

/// A backfill killed mid-run resumes from its stored cursor rather than
/// restarting the repo. The cursor is persisted **per page**, so the worst
/// case a kill costs is the page in flight.
///
/// What this pins, precisely: the persisted `page_url` survives a round trip
/// through the database, and the resumed fetch uses it INSTEAD of the
/// type's initial URL. The two must differ — a resume that silently fell back
/// to `initial_url` would re-walk the whole repo from the top and still look
/// healthy, because `atoms`' unique key makes the re-ingest idempotent.
#[tokio::test]
async fn conformance_resume_from_cursor_after_a_kill_pg() {
    let Some(base) = base_url() else {
        eprintln!("skipping conformance_resume_from_cursor_after_a_kill_pg: set ORGT_TEST_BASE_URL");
        return;
    };
    let db = "cs_gh_conf_resume";
    let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
    let mut admin = recreate(&base, db).await;
    let pool = build_pool(&base, db).await;

    let f = seed_account(&pool, "resume").await;
    let row_id = seed_watch_row(&pool, f.account_id, 910_007, 5_550_009, "resumed", true, Some(f.graph_id), Some(f.user_id)).await;

    // Page 1 landed; the worker persisted its cursor and was then killed.
    let page_two = "https://api.github.com/repositories/5550009/pulls?state=all&page=2";
    let mut cursor = NarrativeCursor::default();
    cursor.pr.page_url = Some(page_two.to_string());
    cursor.pr.seen = 25;
    cursor.pr.total_hint = Some(100);
    super::github_backfill::persist_cursor_and_progress(&pool, f.account_id, row_id, &cursor)
        .await
        .expect("persist the mid-run cursor");

    // The process dies here. A fresh tick re-reads the row from scratch.
    let stored: Option<serde_json::Value> =
        sqlx::query_scalar("SELECT narrative_cursor FROM github_watched_repos WHERE id = $1")
            .bind(row_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    let reloaded: NarrativeCursor =
        serde_json::from_value(stored.expect("a cursor was persisted")).expect("cursor round-trips through jsonb");

    assert_eq!(
        reloaded.pr.page_url.as_deref(),
        Some(page_two),
        "the resumed run must continue from the stored page, not restart the repo"
    );
    assert_eq!(reloaded.pr.seen, 25, "progress accounting survives the kill");
    assert!(!reloaded.pr.done, "an interrupted type is not done");
    assert_eq!(
        page_url_for(TypeKind::Pr, &reloaded, "acme", "resumed", "main"),
        page_two,
        "a fresh tick's first request must use the persisted cursor URL"
    );
    assert_ne!(
        page_url_for(TypeKind::Pr, &reloaded, "acme", "resumed", "main"),
        TypeKind::Pr.initial_url("acme", "resumed", "main"),
        "a resume that fell back to the initial URL would re-walk the whole repo"
    );

    // Progress was written alongside the cursor, which is what the selection
    // page reads ("PRs 25%").
    let progress: Option<serde_json::Value> =
        sqlx::query_scalar("SELECT backfill_progress FROM github_watched_repos WHERE id = $1")
            .bind(row_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    let progress = progress.expect("progress is persisted with the cursor");
    assert_eq!(progress.get("prs").and_then(serde_json::Value::as_u64), Some(25));

    // An untouched type reports no progress rather than a misleading zero.
    assert!(progress.get("releases").map(|v| v.is_null()).unwrap_or(true));

    pool.close().await;
    drop_db(&mut admin, db).await;
}

// ---------------------------------------------------------------------------
// §3.9 / D10 — the destination invariant: a SHARED ocean must actually
// receive content (2026-09-23 production incident)
// ---------------------------------------------------------------------------

/// A `kind='org'` ocean, the destination
/// `github_repos::resolve_destination`'s `RequestedDestination::Org` arm
/// resolves to once `verify_org_admin` has passed. `owner_user_id` is NULL
/// and `is_primary` is false — the two properties that made
/// `ingest::is_own_writable_ocean` refuse it.
async fn seed_org_graph(pool: &PgPool, account_id: Uuid, name: &str) -> Uuid {
    let graph_id = Uuid::new_v4();
    sqlx::query("INSERT INTO graphs (id, account_id, name, kind, is_primary) VALUES ($1, $2, $3, 'org', false)")
        .bind(graph_id)
        .bind(account_id)
        .bind(name)
        .execute(pool)
        .await
        .unwrap();
    graph_id
}

/// The watch row's recorded destination kind, set after [`seed_watch_row`]
/// (which always seeds `'personal'`) so a fixture can tell the true story of
/// a team/org routing without widening that helper's argument list for every
/// existing caller.
async fn set_destination_kind(pool: &PgPool, row_id: Uuid, kind: &str) {
    sqlx::query("UPDATE github_watched_repos SET destination_kind = $2 WHERE id = $1")
        .bind(row_id)
        .bind(kind)
        .execute(pool)
        .await
        .unwrap();
}

/// One narrative atom shaped like a real normalised pull request.
fn pr_atom(source_id: &str, body: &str) -> NormalizedAtom {
    NormalizedAtom {
        source_type: "github_pr",
        source_id: source_id.to_string(),
        title: "Fix the destination invariant".to_string(),
        content: body.to_string(),
        occurred_at: chrono::Utc::now(),
        author_id: Some("42".to_string()),
        author_name: Some("mapped-dev".to_string()),
        participants: vec!["mapped-dev".to_string()],
        code_path: None,
    }
}

/// How many chunk rows landed for `source_id` in `graph_id`. Every chunk is
/// its OWN row keyed `{source_id}:chunk:{n}`, so this is a PREFIX count, not
/// an equality one.
async fn chunk_row_count(pool: &PgPool, graph_id: Uuid, source_type: &str, source_id: &str) -> i64 {
    sqlx::query_scalar(
        "SELECT count(*) FROM atoms WHERE graph_id = $1 AND source_type = $2 AND source_id LIKE $3",
    )
    .bind(graph_id)
    .bind(source_type)
    .bind(format!("{source_id}:chunk:%"))
    .fetch_one(pool)
    .await
    .unwrap()
}

/// **The regression test for the 2026-09-23 production incident.** Both
/// `GithubBackfill` jobs in production were `status='dead'` after three
/// attempts with `error_message = "ingest_atom failed: 400 Bad Request"`,
/// and had ingested exactly zero atoms, because every repo whose destination
/// was a TEAM or ORG ocean was refused by ADR-0007's D7 invariant
/// (`ingest::is_own_writable_ocean`, which admits only
/// `kind='personal'` or a `kind='member'` ocean the caller owns).
///
/// This test fails against the pre-fix tree: `ingest_one` returned
/// `Err("ingest_atom failed: 400 Bad Request")` for both shared destinations
/// and `atoms` stayed empty. It is deliberately run against BOTH shared
/// kinds — the D7 predicate refuses them for the same reason (`owner_user_id`
/// is NULL and the kind is neither `personal` nor `member`), so a fix that
/// somehow only reached one of them would be a half fix.
///
/// The personal/member destination is covered too, in the same run: that arm
/// WORKED before this change, and is exactly why the defect looked
/// designed-for for as long as it did. Pinning it here is what proves the
/// Path-1 conversion did not trade one broken destination kind for another.
#[tokio::test]
async fn conformance_shared_ocean_destinations_actually_receive_atoms_pg() {
    let Some(base) = base_url() else {
        eprintln!("skipping conformance_shared_ocean_destinations_actually_receive_atoms_pg: set ORGT_TEST_BASE_URL");
        return;
    };
    let _env_guard = crate::workers::reembed::MASTER_KEK_HEX_ENV_LOCK.lock().await;
    let db = "cs_gh_conf_shared_dest";
    let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
    let mut admin = recreate(&base, db).await;
    let pool = build_pool(&base, db).await;

    let f = seed_account(&pool, "shareddest").await;
    provision_dek(&pool, f.account_id).await;

    let org_graph = seed_org_graph(&pool, f.account_id, "Acme org ocean").await;
    let (team_id, team_graph) = seed_team(&pool, f.account_id, f.user_id, "Platform").await;
    // The chooser is a current team admin — the state
    // `github_repos::resolve_destination`'s `check_team_ocean_admin` gate
    // required before this destination could ever be saved, and the state
    // the write path's reachability gate re-checks on every atom. Seeded
    // explicitly rather than assumed: without it this test would fail for
    // the RIGHT reason (a stale destination is refused) while claiming to
    // measure the wrong one.
    sqlx::query("INSERT INTO team_members (team_id, user_id, role) VALUES ($1, $2, 'admin')")
        .bind(team_id)
        .bind(f.user_id)
        .execute(&pool)
        .await
        .unwrap();

    // Three watch rows, one per destination kind — the same repo routed
    // three ways is a legitimate shape (§3.4: "the same repo selected by two
    // different people into two different oceans is two watch rows").
    let org_row =
        seed_watch_row(&pool, f.account_id, 940_001, 6_600_001, "shared", true, Some(org_graph), Some(f.user_id)).await;
    set_destination_kind(&pool, org_row, "org").await;
    let team_row =
        seed_watch_row(&pool, f.account_id, 940_001, 6_600_001, "shared", true, Some(team_graph), Some(f.user_id)).await;
    set_destination_kind(&pool, team_row, "team").await;
    let personal_row =
        seed_watch_row(&pool, f.account_id, 940_001, 6_600_001, "shared", true, Some(f.graph_id), Some(f.user_id)).await;
    assert_ne!(org_row, personal_row);
    assert_ne!(team_row, personal_row);

    // Pin the property this test is really about, from the DB rather than
    // from prose: both shared graphs fail D7's predicate exactly the way
    // production's did (`graph_kind=org|team`, `graph_owner=NULL`).
    for (graph_id, expected_kind) in [(org_graph, "org"), (team_graph, "team")] {
        let (kind, owner): (String, Option<Uuid>) =
            sqlx::query_as("SELECT kind, owner_user_id FROM graphs WHERE id = $1")
                .bind(graph_id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(kind, expected_kind);
        assert_eq!(
            owner, None,
            "a shared ocean has no owner — which is precisely why D7 refuses direct ingest into it"
        );
    }

    let state = Arc::new(crate::test_app_state_owned_with_db_and_store(pool.clone(), None));
    let body = "The backfill never landed a single atom because the destination was an org \
                ocean. Reachability is not writability: the D7 gate is a SECOND check, sixty \
                lines after the one the old comment cited.";

    for (label, graph_id, row_id) in [("org", org_graph, org_row), ("team", team_graph, team_row), ("personal", f.graph_id, personal_row)] {
        let source_id = format!("6600001:pr:{label}");
        let outcome = ingest_one(
            &state,
            &TenantId::Consumer,
            &pool,
            f.account_id,
            row_id,
            pr_atom(&source_id, body),
        )
        .await;
        assert_eq!(
            outcome,
            Ok(super::github_backfill::IngestOutcome::Ingested),
            "{label} destination must ingest; before the Path-1 conversion the shared kinds \
             answered Err(\"ingest_atom failed: 400 Bad Request\") here"
        );

        let landed = chunk_row_count(&pool, graph_id, "github_pr", &source_id).await;
        assert!(landed >= 1, "{label}: expected at least one chunk row, got {landed}");

        // Not merely "a row exists": the row must be READABLE. `state.r2` is
        // None in this harness, so content lands inline — a row with neither
        // a blob nor inline content is the "ingested but empty" shape the
        // orphan cleanup exists to prevent.
        let readable: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM atoms WHERE graph_id = $1 AND source_type = 'github_pr' \
               AND source_id LIKE $2 AND content_inline IS NOT NULL AND content_inline <> ''",
        )
        .bind(graph_id)
        .bind(format!("{source_id}:chunk:%"))
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(readable, landed, "{label}: every landed chunk must carry readable content");

        // Provenance survives the Path-1 write (Path 2 stamped it at its
        // step 8b; a hand-rolled insert that forgot it would leave every
        // github atom authorless).
        let authored: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM atoms WHERE graph_id = $1 AND source_id LIKE $2 \
               AND author_name = 'mapped-dev' AND author_id = '42'",
        )
        .bind(graph_id)
        .bind(format!("{source_id}:chunk:%"))
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(authored, landed, "{label}: author provenance must be stamped on every chunk");

        // Idempotent: the resumability contract leans on `ON CONFLICT DO
        // NOTHING`, and a re-crossed page boundary must be free, not a
        // duplicate crawl. `SkippedExisting`, never `Ingested` again, and
        // never the `EmptyBody` misread that let an empty backfill report
        // 100%.
        let again = ingest_one(
            &state,
            &TenantId::Consumer,
            &pool,
            f.account_id,
            row_id,
            pr_atom(&source_id, body),
        )
        .await;
        assert_eq!(again, Ok(super::github_backfill::IngestOutcome::SkippedExisting), "{label}: re-ingest");
        assert_eq!(
            chunk_row_count(&pool, graph_id, "github_pr", &source_id).await,
            landed,
            "{label}: a re-ingest must not duplicate rows"
        );
    }

    // The three destinations are independent oceans: the org ocean must not
    // have received the team's atom or vice versa. (A hand-rolled write that
    // ignored its `graph_id` parameter would pass every assertion above.)
    assert_eq!(chunk_row_count(&pool, org_graph, "github_pr", "6600001:pr:team").await, 0);
    assert_eq!(chunk_row_count(&pool, team_graph, "github_pr", "6600001:pr:org").await, 0);
    assert_eq!(chunk_row_count(&pool, f.graph_id, "github_pr", "6600001:pr:org").await, 0);

    unsafe {
        std::env::remove_var("MASTER_KEK_HEX");
    }
    pool.close().await;
    drop_db(&mut admin, db).await;
}

/// How many `source_versions` rows (and with which `occurred_at`s, oldest
/// capture first) one GitHub native ID has accumulated in `graph_id`.
async fn source_version_times(pool: &PgPool, graph_id: Uuid, source_id: &str) -> Vec<Option<chrono::DateTime<chrono::Utc>>> {
    sqlx::query_scalar("SELECT occurred_at FROM source_versions WHERE graph_id = $1 AND source_id = $2 ORDER BY captured_at, version_id")
        .bind(graph_id)
        .bind(source_id)
        .fetch_all(pool)
        .await
        .unwrap()
}

/// **The 2026-10-18 production churn.** A reconcile sweep re-captured
/// unchanged GitHub content stamped with a later `occurred_at` (the sweep's
/// own wall clock, or the new head's commit time for a file the new head did
/// not touch), and the source fingerprint covered `occurred_at`, so each
/// sweep minted a new `source_versions` row for byte-identical content: 254
/// of 306 `github_code` versions that day repeated an earlier sha of the
/// same item, each one paying extraction again and minting duplicate reefs.
///
/// For a GitHub item the versioned thing is the content: the provider time
/// is a property of WHEN we looked (which head, which sweep), so a capture
/// that differs from the current version ONLY in `occurred_at` is the same
/// version, and the first observed time is retained. A real content change
/// still mints a new version, at its own time. Run for a code file AND a
/// pull request, the two narrative/code shapes the sweep re-captures.
#[tokio::test]
async fn conformance_unchanged_recapture_mints_one_version_pg() {
    let Some(base) = base_url() else {
        eprintln!("skipping conformance_unchanged_recapture_mints_one_version_pg: set ORGT_TEST_BASE_URL");
        return;
    };
    let _env_guard = crate::workers::reembed::MASTER_KEK_HEX_ENV_LOCK.lock().await;
    let db = "cs_gh_conf_recapture";
    let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
    let mut admin = recreate(&base, db).await;
    let pool = build_pool(&base, db).await;

    let f = seed_account(&pool, "recapture").await;
    provision_dek(&pool, f.account_id).await;
    let state = Arc::new(crate::test_app_state_owned_with_db_and_store(pool.clone(), None));
    let row_id = seed_watch_row(&pool, f.account_id, 940_003, 6_600_003, "recapture", true, Some(f.graph_id), Some(f.user_id)).await;

    let committed: chrono::DateTime<chrono::Utc> = "2026-10-18T17:15:27Z".parse().unwrap();
    let swept: chrono::DateTime<chrono::Utc> = "2026-10-18T17:35:31.093007Z".parse().unwrap();
    let changed_at: chrono::DateTime<chrono::Utc> = "2026-10-18T18:02:00Z".parse().unwrap();

    let code = |content: &str, at| NormalizedAtom {
        source_type: "github_code",
        source_id: "6600003:blob:src/lib.rs".to_string(),
        title: "src/lib.rs".to_string(),
        content: content.to_string(),
        occurred_at: at,
        author_id: None,
        author_name: None,
        participants: Vec::new(),
        code_path: Some("src/lib.rs".to_string()),
    };
    let pr = |content: &str, at| NormalizedAtom { occurred_at: at, ..pr_atom("6600003:pr:7", content) };

    for (label, atom) in [("github_code", &code as &dyn Fn(&str, chrono::DateTime<chrono::Utc>) -> NormalizedAtom), ("github_pr", &pr)] {
        let first = atom("pub fn answer() -> u32 { 42 }\n", committed);
        let source_id = first.source_id.clone();
        let ingest = |a: NormalizedAtom| ingest_one(&state, &TenantId::Consumer, &pool, f.account_id, row_id, a);

        assert_eq!(ingest(first).await, Ok(super::github_backfill::IngestOutcome::Ingested), "{label}: first capture");
        assert_eq!(source_version_times(&pool, f.graph_id, &source_id).await, vec![Some(committed)], "{label}: one version at the source time");

        // The sweep: same bytes, observed later.
        ingest(atom("pub fn answer() -> u32 { 42 }\n", swept)).await.unwrap();
        ingest(atom("pub fn answer() -> u32 { 42 }\n", swept + chrono::Duration::minutes(20))).await.unwrap();
        assert_eq!(
            source_version_times(&pool, f.graph_id, &source_id).await,
            vec![Some(committed)],
            "{label}: re-capturing unchanged content at a later time must not mint a version, and the first time is kept"
        );

        // A real edit is a real version.
        ingest(atom("pub fn answer() -> u32 { 43 }\n", changed_at)).await.unwrap();
        assert_eq!(
            source_version_times(&pool, f.graph_id, &source_id).await,
            vec![Some(committed), Some(changed_at)],
            "{label}: a content change mints exactly one new version"
        );
        let current: Option<chrono::DateTime<chrono::Utc>> = sqlx::query_scalar(
            "SELECT v.occurred_at FROM source_items s \
               JOIN source_versions v ON v.version_id = s.current_version_id AND v.graph_id = s.graph_id \
              WHERE s.graph_id = $1 AND s.source_id = $2",
        )
        .bind(f.graph_id)
        .bind(&source_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(current, Some(changed_at), "{label}: the edit is the current version");
    }

    unsafe {
        std::env::remove_var("MASTER_KEK_HEX");
    }
    pool.close().await;
    drop_db(&mut admin, db).await;
}

/// A failed body upload fails the write and stores nothing (ADR-0016: every
/// path aborts). Before the ingest door, GitHub deleted the bodiless rows and
/// committed the rest.
#[tokio::test]
async fn conformance_a_failed_upload_fails_the_write_with_nothing_stored_pg() {
    let Some(base) = base_url() else {
        eprintln!("skipping conformance_a_failed_upload_fails_the_write_with_nothing_stored_pg: set ORGT_TEST_BASE_URL");
        return;
    };
    let _env_guard = crate::workers::reembed::MASTER_KEK_HEX_ENV_LOCK.lock().await;
    let db = "cs_gh_conf_upload_abort";
    let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
    let mut admin = recreate(&base, db).await;
    let pool = build_pool(&base, db).await;

    let f = seed_account(&pool, "upload-abort").await;
    provision_dek(&pool, f.account_id).await;
    let row_id = seed_watch_row(&pool, f.account_id, 940_009, 6_600_009, "abort", true, Some(f.graph_id), Some(f.user_id)).await;
    let mut state = crate::test_app_state_owned_with_db_and_store(pool.clone(), None);
    // A local S3 stand-in that refuses every upload, as R2 does during an
    // outage. No network.
    let (r2, _) = crate::test_object_store::failing_after(0).await;
    state.r2 = Some(Arc::new(r2));
    let state = Arc::new(state);

    let body = "A pull request body long enough to chunk, whose upload is going to fail.";
    let outcome = ingest_one(&state, &TenantId::Consumer, &pool, f.account_id, row_id, pr_atom("6600009:pr:1", body)).await;
    assert!(outcome.is_err(), "a failed upload must fail the item so the tick retries it: {outcome:?}");
    assert_eq!(chunk_row_count(&pool, f.graph_id, "github_pr", "6600009:pr:1").await, 0);
    assert!(
        source_version_times(&pool, f.graph_id, "6600009:pr:1").await.is_empty(),
        "no source version survives an aborted capture"
    );

    unsafe {
        std::env::remove_var("MASTER_KEK_HEX");
    }
    pool.close().await;
    drop_db(&mut admin, db).await;
}

/// **Chunkless/partial GitHub versions (2026-10-18).** A second capture of a
/// changed file used to keep the old text at every existing position
/// (`ON CONFLICT DO NOTHING`) and link only positions past the old end to
/// the new version, so prod had 82 chunk rows over 718 positional atoms.
/// Now every capture leaves the positional atoms of the file equal to the
/// chunk rows of its current version: unchanged positions adopted, edited
/// ones replaced in place, surplus ones dropped, new ones inserted.
#[tokio::test]
async fn conformance_changed_recapture_links_every_chunk_pg() {
    let Some(base) = base_url() else {
        eprintln!("skipping conformance_changed_recapture_links_every_chunk_pg: set ORGT_TEST_BASE_URL");
        return;
    };
    let _env_guard = crate::workers::reembed::MASTER_KEK_HEX_ENV_LOCK.lock().await;
    let db = "cs_gh_conf_relink";
    let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
    let mut admin = recreate(&base, db).await;
    let pool = build_pool(&base, db).await;
    let f = seed_account(&pool, "relink").await;
    provision_dek(&pool, f.account_id).await;
    let state = Arc::new(crate::test_app_state_owned_with_db_and_store(pool.clone(), None));
    let row_id = seed_watch_row(&pool, f.account_id, 940_004, 6_600_004, "relink", true, Some(f.graph_id), Some(f.user_id)).await;
    let source_id = "6600004:blob:src/m.rs";
    let piece = |position: usize, text: &str| coralswarm_core::chunker::Chunk {
        text: text.to_string(), position, speaker: None, parent_message_id: None, token_count: 4, symbols: Vec::new(),
    };
    let write = |pieces: Vec<coralswarm_core::chunker::Chunk>| {
        let state = state.clone();
        let pool = pool.clone();
        let content = pieces.iter().map(|c| c.text.clone()).collect::<Vec<_>>().join("\n");
        async move {
            let atom = NormalizedAtom {
                source_type: "github_code", source_id: source_id.to_string(), title: "src/m.rs".to_string(),
                content, occurred_at: chrono::Utc::now(), author_id: None, author_name: None,
                participants: Vec::new(), code_path: Some("src/m.rs".to_string()),
            };
            write_chunks_to_destinations(
                &state, &TenantId::Consumer, &pool, f.account_id, row_id, &atom, &pieces,
            ).await
        }
    };
    // Positional atoms of the file vs chunk rows of its current version.
    let snapshot = || {
        let pool = pool.clone();
        async move {
            let atoms: Vec<(String, Uuid, Option<String>)> = sqlx::query_as(
                "SELECT source_id, atom_id, content_inline FROM atoms WHERE graph_id = $1 AND source_type = 'github_code' \
                   AND source_id LIKE $2 ORDER BY source_id",
            ).bind(f.graph_id).bind(format!("{source_id}:chunk:%")).fetch_all(&pool).await.unwrap();
            let mut linked: Vec<Uuid> = sqlx::query_scalar(
                "SELECT c.atom_id FROM source_items s JOIN source_version_chunks c \
                   ON c.graph_id = s.graph_id AND c.source_version_id = s.current_version_id \
                  WHERE s.graph_id = $1 AND s.source_id = $2",
            ).bind(f.graph_id).bind(source_id).fetch_all(&pool).await.unwrap();
            linked.sort();
            let mut ids: Vec<Uuid> = atoms.iter().map(|a| a.1).collect();
            ids.sort();
            assert_eq!(ids, linked, "positional atoms == chunk rows of the current version");
            atoms
        }
    };

    let first = write(vec![piece(0, "fn a() {}"), piece(1, "fn b() {}"), piece(2, "fn c() {}")]).await;
    assert!(first.fully_succeeded(), "{first:?}");
    let v1 = snapshot().await;
    assert_eq!(v1.len(), 3);

    // Edit position 1, drop position 2.
    let second = write(vec![piece(0, "fn a() {}"), piece(1, "fn b() { 1 }")]).await;
    assert!(second.fully_succeeded(), "{second:?}");
    assert_eq!((second.atoms_written, second.skipped_duplicate_chunks), (1, 1), "{second:?}");
    let v2 = snapshot().await;
    assert_eq!(v2.len(), 2, "the dropped position is retired");
    assert_eq!((v2[0].1, v2[1].1), (v1[0].1, v1[1].1), "positions keep their atom identity");
    assert_eq!(v2[1].2.as_deref(), Some("fn b() { 1 }"), "the edited position holds the new text");

    // Grow again: positions 0-1 adopted, 2 inserted fresh.
    let third = write(vec![piece(0, "fn a() {}"), piece(1, "fn b() { 1 }"), piece(2, "fn d() {}")]).await;
    assert!(third.fully_succeeded(), "{third:?}");
    let v3 = snapshot().await;
    assert_eq!(v3.len(), 3);
    assert_ne!(v3[2].1, v1[2].1, "a retired position is re-created, not resurrected");
    assert_eq!(super::github_backfill::classify_ingest_response(third.atoms_written, 3, third.skipped_duplicate_chunks as u64),
        Ok(super::github_backfill::IngestOutcome::Ingested));

    unsafe {
        std::env::remove_var("MASTER_KEK_HEX");
    }
    pool.close().await;
    drop_db(&mut admin, db).await;
}

/// **PR #1040 must not be silently reverted by this connector's Path-1
/// write.** `atoms.symbols` carries a code chunk's UNCAPPED declaration
/// list, so a symbol declared fourth-or-later (the ones a chunk header
/// truncates to "+N more") stays retrievable. Before this test existed,
/// `routes::ingest` was the ONLY insert site that wrote the column — and
/// none of the three Path-1 implementations this connector's write is
/// modelled on (`workers::huddle`, `routes::slack_events`,
/// `connectors::google_chat`) writes it. A faithful port of any of them
/// would have dropped it for every `github_code` atom, which is the one
/// source type where the column is ever non-empty.
///
/// The chunks are HAND-BUILT and handed to
/// `github_backfill::write_chunks_to_destinations` rather than derived from
/// the real chunker, for the reason that function's own doc comment gives:
/// `Chunk::symbols` is populated only by the tree-sitter walk, which is
/// behind the non-default `code-chunking` feature. CI runs this suite
/// WITHOUT that feature, so a test that chunked normally would compare
/// `NULL` against `NULL` and stay green through a real regression. This way
/// the assertion is identical in every build.
#[tokio::test]
async fn conformance_symbols_survive_the_path_1_write_pg() {
    let Some(base) = base_url() else {
        eprintln!("skipping conformance_symbols_survive_the_path_1_write_pg: set ORGT_TEST_BASE_URL");
        return;
    };
    let _env_guard = crate::workers::reembed::MASTER_KEK_HEX_ENV_LOCK.lock().await;
    let db = "cs_gh_conf_symbols";
    let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
    let mut admin = recreate(&base, db).await;
    let pool = build_pool(&base, db).await;

    let f = seed_account(&pool, "symbols").await;
    provision_dek(&pool, f.account_id).await;
    let org_graph = seed_org_graph(&pool, f.account_id, "Acme org ocean").await;

    let state = Arc::new(crate::test_app_state_owned_with_db_and_store(pool.clone(), None));

    // Chunk 0 mirrors the exact shape #1040 was written for: a merged group
    // whose header truncates to three declarations plus "+2 more", so
    // `function four`/`function five` appear NOWHERE in the chunk's header
    // and therefore nowhere in the first 280 characters `atoms.excerpt`
    // keeps. Chunk 1 declares nothing (a comment-only group) and must leave
    // the column NULL rather than storing an empty string.
    let header = "// src/n.rs — items function one, function two, function three, +2 more";
    let merged = coralswarm_core::chunker::Chunk {
        text: format!("{header}\nfn one() {{}}\nfn two() {{}}\nfn three() {{}}\nfn four() {{}}\nfn five() {{}}"),
        position: 0,
        speaker: None,
        parent_message_id: None,
        token_count: 32,
        symbols: vec![
            "function one".to_string(),
            "function two".to_string(),
            "function three".to_string(),
            "function four".to_string(),
            "function five".to_string(),
        ],
    };
    let decorative = coralswarm_core::chunker::Chunk {
        text: "// src/n.rs — comments\n// only comments here".to_string(),
        position: 1,
        speaker: None,
        parent_message_id: None,
        token_count: 8,
        symbols: Vec::new(),
    };
    // Fixture guard: the declarations the header truncated away must not be
    // spelled the way `symbols` spells them anywhere in the chunk text, or
    // the excerpt assertion below would hold for the wrong reason.
    assert!(
        !merged.text.contains("function four") && !merged.text.contains("function five"),
        "fixture guard: `function four`/`function five` must be reachable only through `symbols`"
    );

    let source_id = "6600002:blob:src/n.rs";
    let atom = NormalizedAtom {
        source_type: "github_code",
        source_id: source_id.to_string(),
        title: "src/n.rs".to_string(),
        content: format!("{}\n{}", merged.text, decorative.text),
        occurred_at: chrono::Utc::now(),
        author_id: None,
        author_name: None,
        participants: Vec::new(),
        code_path: Some("src/n.rs".to_string()),
    };

    let row_id = seed_watch_row(&pool, f.account_id, 940_002, 6_600_002, "symbols", true, Some(org_graph), Some(f.user_id)).await;
    let outcome = write_chunks_to_destinations(
        &state,
        &TenantId::Consumer,
        &pool,
        f.account_id,
        row_id,
        &atom,
        &[merged.clone(), decorative.clone()],
    )
    .await;
    assert!(outcome.fully_succeeded(), "the write must succeed outright: {outcome:?}");
    assert_eq!(outcome.atoms_written, 2);
    assert_eq!(outcome.destinations_ok, 1);
    assert_eq!(outcome.destinations_failed, 0);

    let snapshot:(String,String)=sqlx::query_as(
        "SELECT provider_namespace,raw_inline FROM source_versions WHERE graph_id=$1 AND source_id=$2",
    ).bind(org_graph).bind(source_id).fetch_one(&pool).await.unwrap();
    assert_eq!(snapshot.0,"github:repository:6600002");
    assert_eq!(snapshot.1,atom.content);
    let links:i64=sqlx::query_scalar("SELECT count(*) FROM source_version_chunks WHERE graph_id=$1")
        .bind(org_graph).fetch_one(&pool).await.unwrap();
    assert_eq!(links,2);

    let rows: Vec<(String, Option<String>, Option<String>)> = sqlx::query_as(
        "SELECT source_id, symbols, excerpt FROM atoms \
         WHERE graph_id = $1 AND source_type = 'github_code' AND source_id LIKE $2 \
         ORDER BY source_id",
    )
    .bind(org_graph)
    .bind(format!("{source_id}:chunk:%"))
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(rows.len(), 2, "both chunks landed as their own rows");

    let (merged_sid, merged_symbols, merged_excerpt) = &rows[0];
    assert_eq!(merged_sid, &format!("{source_id}:chunk:0"));
    assert_eq!(
        merged_symbols.as_deref(),
        Some("function one\nfunction two\nfunction three\nfunction four\nfunction five"),
        "atoms.symbols must carry the UNCAPPED declaration list — this is PR #1040, and a \
         Path-1 write that hand-rolled its INSERT the way huddle/slack/google_chat do would \
         leave this NULL"
    );
    // The point of the column, stated as the property rather than the
    // plumbing: the hidden declarations are reachable through `symbols` and
    // through nothing else this row carries.
    let excerpt = merged_excerpt.as_deref().unwrap_or("");
    assert!(
        !excerpt.contains("function four") && !excerpt.contains("function five"),
        "fixture guard: the excerpt must NOT already contain the hidden declarations, or this \
         test would pass without `symbols` existing at all — excerpt: {excerpt:?}"
    );
    assert!(
        merged_symbols.as_deref().unwrap_or("").contains("function five"),
        "the declaration hidden behind the header's \"+2 more\" is retrievable only here"
    );

    let (decorative_sid, decorative_symbols, _) = &rows[1];
    assert_eq!(decorative_sid, &format!("{source_id}:chunk:1"));
    assert_eq!(
        decorative_symbols.as_deref(),
        None,
        "a chunk that declares nothing leaves the column NULL — never an empty string, which \
         would index as a real (empty) document"
    );

    // The sibling write-time columns land in the SAME statement, so a
    // regression in one is a regression in all of them: pin that they are
    // populated rather than silently NULL.
    let titled: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM atoms WHERE graph_id = $1 AND source_id LIKE $2 \
           AND title IS NOT NULL AND title_kind = 'fallback'",
    )
    .bind(org_graph)
    .bind(format!("{source_id}:chunk:%"))
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(titled, 2, "every chunk gets a synchronous fallback title (title-always-present)");

    unsafe {
        std::env::remove_var("MASTER_KEK_HEX");
    }
    pool.close().await;
    drop_db(&mut admin, db).await;
}

/// A dead backfill must be VISIBLE on the repo row, not merely in `jobs` —
/// and what it shows must be a CLASSIFICATION, never the worker's error
/// text.
///
/// The production incident this pins: `/connectors/github` rendered both
/// repos as `backfilling` with a healthy-looking skip-counter line while
/// their jobs had been `status='dead'` for days. Every signal the page had
/// was consistent with progress — the code tier's skip counts advance during
/// the tree WALK (before any ingest is attempted), `code_cursor.done` stays
/// false for the same reason, and a NULL `last_synced_at` on a first
/// backfill is indistinguishable from "still working".
///
/// Also pins the two properties that make the badge safe to ship:
///   * the next tick that makes progress CLEARS it — a permanently-red repo
///     that is actually syncing teaches an operator to ignore the badge,
///     which costs more than never having shown it;
///   * the column physically cannot hold an error message, because
///     `routes::github::list_repos` serializes it to the client (CodeRabbit
///     round 1, Major/Security). The CHECK constraint is what makes that
///     structural rather than a convention a future writer can forget, so
///     this test attempts the raw-text write and asserts the DATABASE
///     refuses it.
#[tokio::test]
async fn conformance_a_failing_tick_is_visible_on_the_repo_row_pg() {
    let Some(base) = base_url() else {
        eprintln!("skipping conformance_a_failing_tick_is_visible_on_the_repo_row_pg: set ORGT_TEST_BASE_URL");
        return;
    };
    let db = "cs_gh_conf_sync_error";
    let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
    let mut admin = recreate(&base, db).await;
    let pool = build_pool(&base, db).await;

    let f = seed_account(&pool, "syncerr").await;
    let org_graph = seed_org_graph(&pool, f.account_id, "Acme org ocean").await;
    let row_id =
        seed_watch_row(&pool, f.account_id, 950_001, 6_700_001, "failing", true, Some(org_graph), Some(f.user_id)).await;
    set_destination_kind(&pool, row_id, "org").await;

    // A fresh row reports no failure — NULL means "not failing", and must
    // never be confused with "never checked".
    let before: (Option<String>, Option<chrono::DateTime<chrono::Utc>>) =
        sqlx::query_as("SELECT sync_error_code, sync_error_at FROM github_watched_repos WHERE id = $1")
            .bind(row_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(before, (None, None));

    super::github_backfill::record_sync_error(
        &pool,
        f.account_id,
        row_id,
        super::github_backfill::SyncErrorCode::SyncFailed,
    )
    .await
    .expect("record the failure");

    let after: (Option<String>, Option<chrono::DateTime<chrono::Utc>>) =
        sqlx::query_as("SELECT sync_error_code, sync_error_at FROM github_watched_repos WHERE id = $1")
            .bind(row_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(after.0.as_deref(), Some("sync_failed"));
    assert!(after.1.is_some(), "the stamp is written together with the code, never separately");

    // The row stays ACTIVE and unpaused: a failing repo is still scheduled
    // (the job queue's own backoff ladder is what retries it), and flipping
    // `active` here would both lie and stop the retries.
    let (active, paused, stopped): (bool, Option<String>, Option<chrono::DateTime<chrono::Utc>>) =
        sqlx::query_as("SELECT active, paused_reason, sync_stopped_at FROM github_watched_repos WHERE id = $1")
            .bind(row_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(active, "a failing repo is not a paused one");
    assert_eq!(paused, None);
    assert_eq!(stopped, None);

    // The security property, asserted against the DATABASE rather than
    // against the Rust type that happens to feed it today: this column is
    // serialized into an API response, so a raw worker error must be
    // unstorable here even by a caller that bypasses `SyncErrorCode`
    // entirely. The CHECK constraint (migration 0250/0102) is that guarantee.
    let leaked = sqlx::query(
        "UPDATE github_watched_repos SET sync_error_code = $2 WHERE id = $1",
    )
    .bind(row_id)
    .bind("ingest_atom failed: 400 Bad Request; detail: relation \"atoms\" ...")
    .execute(&pool)
    .await;
    let err = leaked.expect_err("the database must refuse a free-text value in this column");
    assert!(
        format!("{err}").contains("sync_error_code_check"),
        "expected the CHECK constraint to be what refused it, got: {err}"
    );
    // ... and the refusal left the previously-recorded code intact.
    let still: Option<String> = sqlx::query_scalar("SELECT sync_error_code FROM github_watched_repos WHERE id = $1")
        .bind(row_id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(still.as_deref(), Some("sync_failed"));

    // The next progressing tick clears BOTH columns together.
    super::github_backfill::clear_sync_error(&pool, f.account_id, row_id).await.expect("clear");
    let cleared: (Option<String>, Option<chrono::DateTime<chrono::Utc>>) =
        sqlx::query_as("SELECT sync_error_code, sync_error_at FROM github_watched_repos WHERE id = $1")
            .bind(row_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(cleared, (None, None), "a recovered repo must not keep a stale red badge");

    // Clearing an already-clear row is a no-op, not an error — it runs on
    // every single page of every healthy backfill.
    super::github_backfill::clear_sync_error(&pool, f.account_id, row_id).await.expect("idempotent clear");

    pool.close().await;
    drop_db(&mut admin, db).await;
}

// ---------------------------------------------------------------------------
// The reachability gate Path 1 does not get for free (CodeRabbit round 1,
// Major/Security)
// ---------------------------------------------------------------------------

/// **`routes::ingest::ingest_atom` applied TWO gates, and converting to Path
/// 1 dropped one of them by accident.**
///
/// The intended removal was D7 (`is_own_writable_ocean`), which refuses
/// shared oceans — that refusal was the bug. The UNINTENDED removal was the
/// REACHABILITY check one layer above it
/// (`resolve_tenant_graph_reachable_by_user`, migrations `0091`/`0006`),
/// which answers "may this person touch this ocean at all". Path 2 got it
/// free from the handler; a hand-rolled write gets nothing free.
///
/// It matters because `github_watched_repos.graph_id` is chosen ONCE, at
/// selection time, and nothing pauses the row when the chooser's standing
/// changes. A user removed from the team leaves behind an active watch row
/// that would otherwise keep pushing this repo's content into an ocean they
/// can no longer reach. The other Path-1 writers do not need an explicit
/// check because they re-derive their destinations from a live query every
/// time; this one reads a stored column.
///
/// This test fails against the first Path-1 draft (which wrote happily into
/// the team ocean after the departure) and against Path 2 for a different
/// reason (D7's blanket 400). It passes only with the gate restored.
#[tokio::test]
async fn conformance_a_departed_member_cannot_keep_writing_to_the_team_ocean_pg() {
    let Some(base) = base_url() else {
        eprintln!("skipping conformance_a_departed_member_cannot_keep_writing_to_the_team_ocean_pg: set ORGT_TEST_BASE_URL");
        return;
    };
    let _env_guard = crate::workers::reembed::MASTER_KEK_HEX_ENV_LOCK.lock().await;
    let db = "cs_gh_conf_departed";
    let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
    let mut admin = recreate(&base, db).await;
    let pool = build_pool(&base, db).await;

    let f = seed_account(&pool, "departed").await;
    provision_dek(&pool, f.account_id).await;
    let (team_id, team_graph) = seed_team(&pool, f.account_id, f.user_id, "Platform").await;
    let row_id =
        seed_watch_row(&pool, f.account_id, 960_001, 6_800_001, "shared", true, Some(team_graph), Some(f.user_id)).await;
    set_destination_kind(&pool, row_id, "team").await;

    // The chooser is a current team admin — the state
    // `github_repos::resolve_destination`'s `check_team_ocean_admin` gate
    // required at selection time.
    sqlx::query("INSERT INTO team_members (team_id, user_id, role) VALUES ($1, $2, 'admin')")
        .bind(team_id)
        .bind(f.user_id)
        .execute(&pool)
        .await
        .unwrap();

    let state = Arc::new(crate::test_app_state_owned_with_db_and_store(pool.clone(), None));
    let body = "A PR body long enough to chunk into at least one atom, so the assertions below \
                are about the gate rather than about an empty body.";

    // Baseline: while they are still a member, the write lands. Without this
    // half the test could pass for the wrong reason — a gate that refuses
    // EVERYTHING also refuses a departed member.
    let allowed = ingest_one(
        &state,
        &TenantId::Consumer,
        &pool,
        f.account_id,
        row_id,
        pr_atom("6800001:pr:before", body),
    )
    .await;
    assert_eq!(
        allowed,
        Ok(super::github_backfill::IngestOutcome::Ingested),
        "a current team member's write must still land — the gate must refuse departures, not everything"
    );
    assert!(chunk_row_count(&pool, team_graph, "github_pr", "6800001:pr:before").await >= 1);

    // They leave the team. NOTHING pauses the watch row — that is the whole
    // point: the row is still `active`, still routed at `team_graph`, and
    // the backfill would keep running.
    sqlx::query("UPDATE team_members SET departing_at = now() WHERE team_id = $1 AND user_id = $2")
        .bind(team_id)
        .bind(f.user_id)
        .execute(&pool)
        .await
        .unwrap();
    let (still_active, still_routed): (bool, Option<Uuid>) =
        sqlx::query_as("SELECT active, graph_id FROM github_watched_repos WHERE id = $1")
            .bind(row_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(still_active, "fixture guard: nothing pauses the row on departure — that IS the hazard");
    assert_eq!(still_routed, Some(team_graph));

    // The write must now be refused, and nothing may land.
    let refused = ingest_one(
        &state,
        &TenantId::Consumer,
        &pool,
        f.account_id,
        row_id,
        pr_atom("6800001:pr:after", body),
    )
    .await;
    let err = refused.expect_err("a departed member's write must be refused");
    assert!(
        err.contains("no longer reachable"),
        "the refusal should name the reason it can act on: {err}"
    );
    assert_eq!(
        chunk_row_count(&pool, team_graph, "github_pr", "6800001:pr:after").await,
        0,
        "not a single atom row may land for a destination the chooser can no longer reach"
    );

    // And the failure classifies as the ACTIONABLE code, grounded in a
    // re-check of the live row rather than in the error string's wording.
    let code = super::github_backfill::classify_tick_failure(
        &state,
        &TenantId::Consumer,
        &pool,
        f.account_id,
        row_id,
    )
    .await;
    assert_eq!(code, super::github_backfill::SyncErrorCode::DestinationUnreachable);

    // Re-joining the team and un-pausing the row (an admin's manual step:
    // the refusal paused it) restores the write. A gate that cannot be undone
    // by fixing the underlying state is a trap, not a gate.
    sqlx::query("UPDATE team_members SET departing_at = NULL WHERE team_id = $1 AND user_id = $2")
        .bind(team_id)
        .bind(f.user_id)
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("UPDATE github_watched_repos SET active = true, paused_reason = NULL WHERE id = $1")
        .bind(row_id)
        .execute(&pool)
        .await
        .unwrap();
    let allowed_again = ingest_one(
        &state,
        &TenantId::Consumer,
        &pool,
        f.account_id,
        row_id,
        pr_atom("6800001:pr:after", body),
    )
    .await;
    assert_eq!(allowed_again, Ok(super::github_backfill::IngestOutcome::Ingested));
    assert!(chunk_row_count(&pool, team_graph, "github_pr", "6800001:pr:after").await >= 1);
    assert_eq!(
        super::github_backfill::classify_tick_failure(&state, &TenantId::Consumer, &pool, f.account_id, row_id).await,
        super::github_backfill::SyncErrorCode::SyncFailed,
        "with the destination reachable again, a failure is no longer the admin's to act on"
    );

    // An ARCHIVED destination is refused by the same predicate — the other
    // half of "reachable" that a stored `graph_id` can outlive.
    sqlx::query("UPDATE graphs SET archived_at = now() WHERE id = $1")
        .bind(team_graph)
        .execute(&pool)
        .await
        .unwrap();
    let archived = ingest_one(
        &state,
        &TenantId::Consumer,
        &pool,
        f.account_id,
        row_id,
        pr_atom("6800001:pr:archived", body),
    )
    .await;
    assert!(archived.is_err(), "an archived destination ocean must be refused too");
    assert_eq!(chunk_row_count(&pool, team_graph, "github_pr", "6800001:pr:archived").await, 0);

    unsafe {
        std::env::remove_var("MASTER_KEK_HEX");
    }
    pool.close().await;
    drop_db(&mut admin, db).await;
}

// ---------------------------------------------------------------------------
// The pause half of finding 3: a lost destination must STOP the retry loop,
// not merely fail every attempt of it
// ---------------------------------------------------------------------------

/// Removing a member from a team PAUSES the GitHub watches they had pointed
/// at that team's ocean, and the backfill then stops retrying.
///
/// `destination_reachable` (the sibling test above) is the safety property —
/// it refuses the write. But a refusal alone leaves the watch row `active`,
/// so the queue keeps re-enqueuing a backfill that can only ever fail: three
/// attempts, dead, re-enqueued by the next selection save, indefinitely, with
/// nothing in the UI to act on. This is the liveness half.
///
/// Driven through the REAL `routes::teams::remove_team_member` handler,
/// called directly with a hand-built `Tenant`/`UserContext` rather than
/// through the router, so the assertion covers the handler's own call to the
/// pause hook and not just the hook in isolation. (A test of the hook alone
/// would stay green if someone deleted the call site.)
///
/// Ported in behaviour, not diff, from the Cursor Agent's `5a15e578`
/// (preserved at `origin/cs-github-cursor-fixes-preserved`), which is where
/// the idea of pausing rather than only refusing came from.
#[tokio::test]
async fn conformance_removing_a_team_member_pauses_their_github_watches_pg() {
    let Some(base) = base_url() else {
        eprintln!("skipping conformance_removing_a_team_member_pauses_their_github_watches_pg: set ORGT_TEST_BASE_URL");
        return;
    };
    let db = "cs_gh_conf_pause_on_removal";
    let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
    let mut admin_conn = recreate(&base, db).await;
    let pool = build_pool(&base, db).await;

    let f = seed_account(&pool, "pauseremoval").await;
    let (team_id, team_graph) = seed_team(&pool, f.account_id, f.user_id, "Platform").await;
    // `f.user_id` is the team admin doing the removing; `departing` is the
    // plain member who chose a repo destination and is about to lose it.
    // Target is a MEMBER, not an admin, so the handler's last-admin guard is
    // not what this test ends up measuring.
    let departing = seed_member(&pool, f.account_id, "departing@example.com").await;
    sqlx::query("INSERT INTO team_members (team_id, user_id, role) VALUES ($1, $2, 'admin')")
        .bind(team_id)
        .bind(f.user_id)
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO team_members (team_id, user_id, role) VALUES ($1, $2, 'member')")
        .bind(team_id)
        .bind(departing)
        .execute(&pool)
        .await
        .unwrap();

    // Three watch rows, so the UPDATE's narrowness is measured and not
    // assumed: the one that must pause, one the SAME member pointed at a
    // different ocean, and one a DIFFERENT member pointed at this same ocean.
    let doomed =
        seed_watch_row(&pool, f.account_id, 970_001, 6_900_001, "doomed", true, Some(team_graph), Some(departing)).await;
    set_destination_kind(&pool, doomed, "team").await;
    let other_ocean =
        seed_watch_row(&pool, f.account_id, 970_001, 6_900_002, "elsewhere", true, Some(f.graph_id), Some(departing))
            .await;
    let other_member =
        seed_watch_row(&pool, f.account_id, 970_001, 6_900_003, "theirs", true, Some(team_graph), Some(f.user_id)).await;
    set_destination_kind(&pool, other_member, "team").await;

    let state = Arc::new(crate::test_app_state_owned_with_db_and_store(pool.clone(), None));
    let tenant = crate::tenancy::extractor::Tenant {
        store: Some(Arc::new(crate::store::PostgresStore::new(pool.clone()))),
        pool: Some(pool.clone()),
        // Consumer plane: this harness runs the control tree in one database,
        // the same single-DB-serves-both-planes shape every other pg test in
        // this module uses.
        decision: None,
        class: crate::route_class::RouteClass::Other,
    };
    let caller = crate::middleware::auth::UserContext {
        user_id: f.user_id,
        account_id: f.account_id,
        role: "admin".into(),
        device_id: String::new(),
    };

    // `remove_team_member` returns `impl IntoResponse`, so the trait has to
    // be in scope to read a status off it.
    use axum::response::IntoResponse as _;
    let resp = crate::routes::teams::remove_team_member(
        axum::extract::State(state.clone()),
        tenant,
        axum::Extension(caller),
        axum::extract::Path((team_id, departing)),
    )
    .await
    .into_response();
    assert_eq!(
        resp.status(),
        axum::http::StatusCode::NO_CONTENT,
        "the removal itself must succeed — if it did not, everything below would pass vacuously"
    );

    // The removal really happened (guard against a handler that 204s without
    // writing).
    let departing_at: Option<chrono::DateTime<chrono::Utc>> =
        sqlx::query_scalar("SELECT departing_at FROM team_members WHERE team_id = $1 AND user_id = $2")
            .bind(team_id)
            .bind(departing)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(departing_at.is_some(), "fixture guard: the member must actually have been removed");

    // THE ASSERTION: the affected row is paused, with the actionable reason
    // and the matching classification written together.
    let (active, reason, stopped, code): (bool, Option<String>, Option<chrono::DateTime<chrono::Utc>>, Option<String>) =
        sqlx::query_as(
            "SELECT active, paused_reason, sync_stopped_at, sync_error_code FROM github_watched_repos WHERE id = $1",
        )
        .bind(doomed)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert!(!active, "the watch pointed at the ocean they just lost must be paused");
    assert_eq!(reason.as_deref(), Some("destination_unreachable"));
    assert!(stopped.is_some(), "a pause stamps sync_stopped_at — §3.8's 'stopped, content kept'");
    assert_eq!(
        code.as_deref(),
        Some("destination_unreachable"),
        "pause state and failure state must agree about why this repo stopped"
    );

    // Nothing else was touched. A pause that over-reaches is its own outage.
    for (row_id, label) in [(other_ocean, "same member, different ocean"), (other_member, "different member, same ocean")] {
        let (still_active, still_unpaused): (bool, Option<String>) =
            sqlx::query_as("SELECT active, paused_reason FROM github_watched_repos WHERE id = $1")
                .bind(row_id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert!(still_active, "{label}: must NOT be paused");
        assert_eq!(still_unpaused, None, "{label}: must keep a clean paused_reason");
    }

    // ── "and the backfill stops retrying" ────────────────────────────────
    // `run_tick` short-circuits on an inactive row, so the next scheduled
    // tick does no work and — via `workers::github_backfill`'s `Paused` arm
    // — completes `NothingToDo` instead of enqueueing a continuation. That
    // is what ends the loop.
    let outcome = super::github_backfill::run_tick(&TenantId::Consumer, &pool, &state, f.account_id, doomed)
        .await
        .expect("a paused row is a clean no-op tick, never an error");
    assert!(
        matches!(outcome, super::github_backfill::RunOutcome::Paused),
        "a paused row must tick to Paused — the outcome workers::github_backfill answers with \
         JobOutcome::NothingToDo and NO continuation enqueue"
    );

    // And the tick did NOT wipe the actionable code on its way past. Every
    // non-error outcome clears `sync_error_code`, `Paused` included — except
    // this one, because the row is paused precisely BECAUSE it is failing.
    let survived: Option<String> = sqlx::query_scalar("SELECT sync_error_code FROM github_watched_repos WHERE id = $1")
        .bind(doomed)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(
        survived.as_deref(),
        Some("destination_unreachable"),
        "a tick observing the pause must not clear the reason for it — the admin still has to act"
    );
    super::github_backfill::clear_sync_error(&pool, f.account_id, doomed).await.unwrap();
    let still_there: Option<String> = sqlx::query_scalar("SELECT sync_error_code FROM github_watched_repos WHERE id = $1")
        .bind(doomed)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(still_there.as_deref(), Some("destination_unreachable"), "even a direct clear leaves this one alone");

    // Re-activating the repo (an admin's deliberate act, after re-routing or
    // restoring access) is what makes the state clearable again — the guard
    // is scoped to the pause, not to the code.
    sqlx::query("UPDATE github_watched_repos SET active = true, paused_reason = NULL WHERE id = $1")
        .bind(doomed)
        .execute(&pool)
        .await
        .unwrap();
    super::github_backfill::clear_sync_error(&pool, f.account_id, doomed).await.unwrap();
    let cleared: Option<String> = sqlx::query_scalar("SELECT sync_error_code FROM github_watched_repos WHERE id = $1")
        .bind(doomed)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(cleared, None, "a re-activated repo starts clean");

    pool.close().await;
    drop_db(&mut admin_conn, db).await;
}

/// The write path is the BACKSTOP for every way reachability is lost that no
/// roster event covers — an org departure, an archived ocean — and for a
/// roster event that never fired at all.
///
/// Belt and braces, in both directions: the gate refuses without depending on
/// the pause, and the pause happens without depending on the roster hook.
#[tokio::test]
async fn conformance_an_unreachable_destination_pauses_itself_on_the_next_write_pg() {
    let Some(base) = base_url() else {
        eprintln!("skipping conformance_an_unreachable_destination_pauses_itself_on_the_next_write_pg: set ORGT_TEST_BASE_URL");
        return;
    };
    let _env_guard = crate::workers::reembed::MASTER_KEK_HEX_ENV_LOCK.lock().await;
    let db = "cs_gh_conf_selfpause";
    let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
    let mut admin_conn = recreate(&base, db).await;
    let pool = build_pool(&base, db).await;

    let f = seed_account(&pool, "selfpause").await;
    provision_dek(&pool, f.account_id).await;
    let (team_id, team_graph) = seed_team(&pool, f.account_id, f.user_id, "Platform").await;
    sqlx::query("INSERT INTO team_members (team_id, user_id, role) VALUES ($1, $2, 'admin')")
        .bind(team_id)
        .bind(f.user_id)
        .execute(&pool)
        .await
        .unwrap();
    let row_id =
        seed_watch_row(&pool, f.account_id, 980_001, 7_000_001, "orphaned", true, Some(team_graph), Some(f.user_id)).await;
    set_destination_kind(&pool, row_id, "team").await;

    let state = Arc::new(crate::test_app_state_owned_with_db_and_store(pool.clone(), None));

    // Reachability is lost WITHOUT any roster event the hook could see — the
    // ocean is archived out from under the watch.
    sqlx::query("UPDATE graphs SET archived_at = now() WHERE id = $1")
        .bind(team_graph)
        .execute(&pool)
        .await
        .unwrap();

    let refused = ingest_one(
        &state,
        &TenantId::Consumer,
        &pool,
        f.account_id,
        row_id,
        pr_atom("7000001:pr:1", "A body long enough to produce at least one chunk if it ever got that far."),
    )
    .await;
    assert!(refused.is_err(), "an archived destination must be refused");

    let (active, reason, code): (bool, Option<String>, Option<String>) =
        sqlx::query_as("SELECT active, paused_reason, sync_error_code FROM github_watched_repos WHERE id = $1")
            .bind(row_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(!active, "the refusing write must also stop the retry loop");
    assert_eq!(reason.as_deref(), Some("destination_unreachable"));
    assert_eq!(code.as_deref(), Some("destination_unreachable"));

    unsafe {
        std::env::remove_var("MASTER_KEK_HEX");
    }
    pool.close().await;
    drop_db(&mut admin_conn, db).await;
}

// ---------------------------------------------------------------------------
// Head cursor: the webhook advances it, the sweep skips what is covered
// ---------------------------------------------------------------------------

/// What the head-cursor mock records and serves. A tiny commit graph:
/// `c0 <- c1 <- c2 <- c3` on `main`, plus `x2` (parent `c1`), the target of
/// a force-push that drops `c2`. Every commit is a DIRECT push (no PR), so
/// each one becomes a `github_commit` atom, and every commit also changes
/// one file, `src/{sha}.rs`, which the connector must never fetch (it does
/// not capture source code, 2026-10-27).
#[derive(Default)]
struct HeadMock {
    live_head: String,
    /// Commit-detail fetches answer 502 (a transient GitHub failure) while
    /// set.
    fail_commits: bool,
    hits: std::collections::HashMap<&'static str, usize>,
}

type SharedHeadMock = Arc<std::sync::Mutex<HeadMock>>;

impl HeadMock {
    fn hit(m: &SharedHeadMock, key: &'static str) {
        *m.lock().unwrap().hits.entry(key).or_default() += 1;
    }
    fn hits(m: &SharedHeadMock, key: &'static str) -> usize {
        m.lock().unwrap().hits.get(key).copied().unwrap_or(0)
    }
    fn reset(m: &SharedHeadMock) {
        m.lock().unwrap().hits.clear();
    }
    fn set_head(m: &SharedHeadMock, sha: &str) {
        m.lock().unwrap().live_head = sha.to_string();
    }
}

fn head_mock_parent(sha: &str) -> Option<&'static str> {
    match sha {
        "c1" => Some("c0"),
        "c2" => Some("c1"),
        "c3" => Some("c2"),
        "x2" => Some("c1"),
        _ => None,
    }
}

/// `sha` and its ancestors, newest first.
fn head_mock_chain(sha: &str) -> Vec<String> {
    let mut out = vec![sha.to_string()];
    let mut cur = sha.to_string();
    while let Some(p) = head_mock_parent(&cur) {
        out.push(p.to_string());
        cur = p.to_string();
    }
    out
}

/// GitHub's three-dot compare: the commits reachable from `after` but not
/// from `before`, oldest first, and the files they touched.
fn head_mock_compare(before: &str, after: &str) -> serde_json::Value {
    use serde_json::json;
    // PR-merge ranges on top of `c0`, the two shapes `main` takes when every
    // change lands through a PR: a squash merge (`... (#N)`), and a merge
    // commit with the PR's branch commit behind its second parent.
    match (before, after) {
        ("c0", "sq1") => {
            return json!({"total_commits": 1, "commits": [
                {"sha": "sq1", "commit": {"message": "Add the widget (#42)"}, "parents": [{"sha": "c0"}]}
            ]});
        }
        ("sq1", "mg1") => {
            return json!({"total_commits": 2, "commits": [
                {"sha": "br1", "commit": {"message": "Widget branch work"}, "parents": [{"sha": "sq1"}]},
                {"sha": "mg1", "commit": {"message": "Merge pull request #43 from acme/widget"}, "parents": [{"sha": "sq1"}, {"sha": "br1"}]}
            ]});
        }
        _ => {}
    }
    let before_chain = head_mock_chain(before);
    let mut commits: Vec<String> = head_mock_chain(after).into_iter().take_while(|c| !before_chain.contains(c)).collect();
    commits.reverse();
    json!({
        "total_commits": commits.len(),
        "commits": commits.iter().map(|c| json!({
            "sha": c,
            // No squash-merge suffix: a direct push, so
            // `fetch_commit_if_direct` asks the association endpoint (empty)
            // and then fetches the commit detail.
            "commit": {"message": format!("Change {c}"), "committer": {"date": "2026-10-18T17:00:00Z"}}
        })).collect::<Vec<_>>(),
        "files": commits.iter().map(|c| json!({"filename": format!("src/{c}.rs"), "status": "added", "sha": format!("blob{c}")})).collect::<Vec<_>>(),
    })
}

/// Mock GitHub for the head-cursor and no-code tests: token mint, the
/// default branch, compare, commit association + detail, one PR (#7, with
/// its reviews/comments/commits), one issue's comments, and empty PR/issue/
/// release listings (the sweep walks those first). Blob, tree, and
/// `.gitattributes` fetches are COUNTED (`blob`, `tree`, `other`) so a test
/// can prove the connector never reads file contents.
async fn spawn_head_mock(start_head: &str) -> (String, SharedHeadMock, tokio::task::JoinHandle<()>) {
    use axum::extract::{Path, State};
    use axum::routing::{get, post};
    use axum::{Json, Router};
    use base64::Engine as _;
    use serde_json::json;

    let shared: SharedHeadMock = Arc::new(std::sync::Mutex::new(HeadMock { live_head: start_head.to_string(), ..Default::default() }));
    let app = Router::new()
        .route(
            "/app/installations/{installation_id}/access_tokens",
            post(|Path(_id): Path<String>| async move {
                Json(json!({"token": "ghs_mock_installation_token", "expires_at": (chrono::Utc::now() + chrono::Duration::hours(1)).to_rfc3339()}))
            }),
        )
        .route(
            "/repos/{owner}/{repo}/branches/{branch}",
            get(|State(m): State<SharedHeadMock>| async move {
                HeadMock::hit(&m, "branches");
                let head = m.lock().unwrap().live_head.clone();
                Json(json!({"commit": {"sha": head, "commit": {"tree": {"sha": format!("tree{head}")}, "committer": {"date": "2026-10-18T17:00:00Z"}}}}))
            }),
        )
        .route(
            "/repos/{owner}/{repo}/compare/{range}",
            get(|State(m): State<SharedHeadMock>, Path((_o, _r, range)): Path<(String, String, String)>| async move {
                HeadMock::hit(&m, "compare");
                let (before, after) = range.split_once("...").expect("three-dot range");
                Json(head_mock_compare(before, after))
            }),
        )
        .route(
            "/repos/{owner}/{repo}/commits/{sha}/pulls",
            get(|| async { Json(json!([])) }),
        )
        .route(
            "/repos/{owner}/{repo}/commits/{sha}",
            get(|State(m): State<SharedHeadMock>, Path((_o, _r, sha)): Path<(String, String, String)>| async move {
                HeadMock::hit(&m, "commit");
                if m.lock().unwrap().fail_commits {
                    return Err(axum::http::StatusCode::BAD_GATEWAY);
                }
                Ok(Json(json!({
                    "sha": sha,
                    "commit": {"message": format!("Change {sha}"), "author": {"name": "Dev", "date": "2026-10-18T17:00:00Z"}},
                    "stats": {"additions": 1, "deletions": 0},
                    "files": [{"filename": format!("src/{sha}.rs"), "patch": "+pub fn secret_source() {}"}]
                })))
            }),
        )
        .route(
            "/repos/{owner}/{repo}/git/blobs/{sha}",
            get(|State(m): State<SharedHeadMock>| async move {
                HeadMock::hit(&m, "blob");
                let content = base64::engine::general_purpose::STANDARD.encode("pub fn secret_source() {}\n");
                Json(json!({"content": content, "encoding": "base64"}))
            }),
        )
        .route(
            "/repos/{owner}/{repo}/git/trees/{sha}",
            get(|State(m): State<SharedHeadMock>| async move {
                HeadMock::hit(&m, "tree");
                Json(json!({"sha": "t", "truncated": false, "tree": [{"path": "src/lib.rs", "type": "blob", "sha": "b1", "size": 10}]}))
            }),
        )
        .route(
            "/repos/{owner}/{repo}/pulls/{number}",
            get(|Path((_o, _r, number)): Path<(String, String, i64)>| async move {
                Json(json!({
                    "number": number, "title": "Add the widget", "body": "Adds the widget.\n\n```diff\n+widget()\n```",
                    "state": "open", "merged": false, "user": {"login": "dev"},
                    "created_at": "2026-10-18T16:00:00Z", "updated_at": "2026-10-18T16:30:00Z",
                    "html_url": "https://github.com/acme/repo/pull/7"
                }))
            }),
        )
        .route("/repos/{owner}/{repo}/pulls/{number}/reviews", get(|| async { Json(json!([{"user": {"login": "rev"}, "state": "APPROVED", "body": "Looks good", "submitted_at": "2026-10-18T16:40:00Z"}])) }))
        .route("/repos/{owner}/{repo}/pulls/{number}/comments", get(|| async { Json(json!([])) }))
        .route("/repos/{owner}/{repo}/pulls/{number}/commits", get(|| async { Json(json!([])) }))
        .route("/repos/{owner}/{repo}/issues/{number}/comments", get(|| async { Json(json!([{"user": {"login": "dev"}, "body": "Repro attached", "created_at": "2026-10-18T16:50:00Z"}])) }))
        .route("/repos/{owner}/{repo}/commits", get(|| async { Json(json!([])) }))
        .route("/repos/{owner}/{repo}/pulls", get(|| async { Json(json!([])) }))
        .route("/repos/{owner}/{repo}/issues", get(|| async { Json(json!([])) }))
        .route("/repos/{owner}/{repo}/releases", get(|| async { Json(json!([])) }))
        .fallback(|State(m): State<SharedHeadMock>| async move {
            HeadMock::hit(&m, "other");
            axum::http::StatusCode::NOT_FOUND
        })
        .with_state(shared.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind mock github");
    let addr = listener.local_addr().expect("mock github addr");
    let handle = tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    (format!("http://{addr}"), shared, handle)
}

/// One fully-backfilled, active watch row whose head cursor is `head`.
struct HeadFixture {
    pool: PgPool,
    state: Arc<crate::AppState>,
    f: Fixture,
    row_id: Uuid,
    installation_id: i64,
    repo_id: i64,
}

async fn head_fixture(base: &str, db: &str, tag: &str, installation_id: i64, repo_id: i64, head: &str) -> (HeadFixture, PgConnection) {
    let admin = recreate(base, db).await;
    let pool = build_pool(base, db).await;
    let f = seed_account(&pool, tag).await;
    provision_dek(&pool, f.account_id).await;
    let row_id = seed_watch_row(&pool, f.account_id, installation_id, repo_id, tag, true, Some(f.graph_id), Some(f.user_id)).await;
    sqlx::query("UPDATE github_watched_repos SET head_tree_sha = $2, last_synced_at = now() - interval '1 hour' WHERE id = $1")
        .bind(row_id)
        .bind(head)
        .execute(&pool)
        .await
        .unwrap();
    let state = Arc::new(crate::test_app_state_owned_with_db_and_store(pool.clone(), None));
    (HeadFixture { pool, state, f, row_id, installation_id, repo_id }, admin)
}

impl HeadFixture {
    async fn head(&self) -> Option<String> {
        sqlx::query_scalar("SELECT head_tree_sha FROM github_watched_repos WHERE id = $1").bind(self.row_id).fetch_one(&self.pool).await.unwrap()
    }

    /// A real `push` webhook body, dispatched the way
    /// `workers::github_event_ingest` dispatches it.
    async fn webhook_push(&self, before: &str, after: &str, forced: bool) -> Result<(), String> {
        self.webhook_push_via(&self.state, before, after, forced).await
    }

    /// [`Self::webhook_push`] through a given `AppState` (a fresh one has an
    /// empty DEK cache, so a DEK removed from the account is really gone).
    async fn webhook_push_via(&self, state: &Arc<crate::AppState>, before: &str, after: &str, forced: bool) -> Result<(), String> {
        let payload = serde_json::json!({
            "ref": "refs/heads/main", "before": before, "after": after, "forced": forced,
            "repository": {"id": self.repo_id, "default_branch": "main"},
            "head_commit": {"timestamp": "2026-10-18T17:00:00Z"}
        });
        self.event("push", &payload, state).await
    }

    async fn event(&self, event: &str, payload: &serde_json::Value, state: &Arc<crate::AppState>) -> Result<(), String> {
        super::github_event_ingest::handle_event(&TenantId::Consumer, &self.pool, state, self.f.account_id, self.installation_id, event, payload)
            .await
            .map(|_| ())
    }

    async fn sweep(&self) {
        // Make the row due regardless of how recently a previous sweep in
        // the same test touched it.
        sqlx::query("UPDATE github_watched_repos SET last_synced_at = now() - interval '1 hour' WHERE id = $1").bind(self.row_id).execute(&self.pool).await.unwrap();
        let mut budget = 10usize;
        crate::workers::github_reconcile_sweep::sweep_account(&self.state, &TenantId::Consumer, &self.pool, self.f.account_id, &mut budget).await;
    }

    /// Whether the direct commit `sha` landed as a `github_commit` atom.
    async fn has_commit(&self, sha: &str) -> bool {
        chunk_row_count(&self.pool, self.f.graph_id, "github_commit", &format!("{}:commit:{sha}", self.repo_id)).await > 0
    }

    async fn count_type(&self, source_type: &str) -> i64 {
        sqlx::query_scalar("SELECT count(*) FROM atoms WHERE graph_id = $1 AND source_type = $2")
            .bind(self.f.graph_id)
            .bind(source_type)
            .fetch_one(&self.pool)
            .await
            .unwrap()
    }

    /// No code atom of either retired tier exists anywhere in this database.
    async fn assert_no_code_atoms(&self, label: &str) {
        let code: i64 = sqlx::query_scalar("SELECT count(*) FROM atoms WHERE source_type IN ('github_code', 'github_code_summary')")
            .fetch_one(&self.pool)
            .await
            .unwrap();
        assert_eq!(code, 0, "{label}: the connector must never write a github_code or github_code_summary atom");
    }
}

fn require_pg(name: &str) -> Option<String> {
    match base_url() {
        Some(b) => Some(b),
        None => {
            assert!(std::env::var("REQUIRE_PG_SUITE").is_err(), "{name}: REQUIRE_PG_SUITE is set but ORGT_TEST_BASE_URL is not");
            eprintln!("skipping {name}: set ORGT_TEST_BASE_URL");
            None
        }
    }
}

/// **The founder decision of 2026-10-27 (issue #1516): the GitHub connector
/// captures everything except the code.** A default-branch push whose
/// compare lists changed files, a sweep over a range the webhook missed, a
/// PR event, and an issue event all run through the real handlers. The push
/// and the sweep land the direct commit as a `github_commit` atom (message,
/// stats, changed-file names) and fetch no blob or tree; the PR and issue
/// events land their own atoms; and no `github_code` /
/// `github_code_summary` atom is written by any of them.
#[tokio::test]
#[allow(clippy::await_holding_lock)]
async fn conformance_no_source_code_is_captured_pg() {
    let Some(base) = require_pg("conformance_no_source_code_is_captured_pg") else { return };
    let _kek = crate::workers::reembed::MASTER_KEK_HEX_ENV_LOCK.lock().await;
    let (mock_base, mock, handle) = spawn_head_mock("c1").await;
    let _env = GithubEnvGuard::apply(&mock_base);
    let db = "cs_gh_conf_no_code";
    let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
    let (fx, mut admin) = head_fixture(&base, db, "nocode", 960_005, 6_800_005, "c0").await;

    // 1. A push webhook: the compare lists `src/c1.rs` as added.
    fx.webhook_push("c0", "c1", false).await.expect("push webhook");
    assert!(fx.has_commit("c1").await, "the direct commit is still captured");
    assert_eq!(fx.head().await.as_deref(), Some("c1"), "the push range still advances the head cursor");
    fx.assert_no_code_atoms("push webhook").await;

    // 2. A dropped webhook: the sweep compares c1...c2 itself.
    HeadMock::set_head(&mock, "c2");
    fx.sweep().await;
    assert!(fx.has_commit("c2").await, "the sweep still captures the missed direct commit");
    assert_eq!(fx.head().await.as_deref(), Some("c2"));
    fx.assert_no_code_atoms("reconcile sweep").await;

    // 3. A merged PR (formerly also the summary staleness gate's trigger)
    //    and an ordinary PR review event both land the PR atom.
    let pr = serde_json::json!({
        "action": "closed",
        "repository": {"id": fx.repo_id, "default_branch": "main"},
        "pull_request": {"number": 7, "merged": true, "title": "Add the widget", "body": "Adds the widget."}
    });
    fx.event("pull_request", &pr, &fx.state).await.expect("pull_request event");
    let review = serde_json::json!({"action": "submitted", "repository": {"id": fx.repo_id}, "pull_request": {"number": 7}});
    fx.event("pull_request_review", &review, &fx.state).await.expect("pull_request_review event");
    assert!(fx.count_type("github_pr").await > 0, "the PR (with its review) is still captured");

    // 4. An issue event lands the issue atom.
    let issue = serde_json::json!({
        "action": "opened",
        "repository": {"id": fx.repo_id},
        "issue": {"number": 9, "title": "Widget crashes", "body": "Steps to reproduce.", "state": "open",
                  "user": {"login": "dev"}, "created_at": "2026-10-18T16:45:00Z", "updated_at": "2026-10-18T16:50:00Z"}
    });
    fx.event("issues", &issue, &fx.state).await.expect("issues event");
    assert!(fx.count_type("github_issue").await > 0, "the issue is still captured");

    fx.assert_no_code_atoms("after every event kind").await;
    assert_eq!(HeadMock::hits(&mock, "blob"), 0, "no file content was ever fetched");
    assert_eq!(HeadMock::hits(&mock, "tree"), 0, "no repository tree was ever walked");

    handle.abort();
    fx.pool.close().await;
    drop_db(&mut admin, db).await;
}

/// **Every change to `main` lands through a PR** (CoralSwarm/coralswarm
/// since 2026-10-04): a squash-merge push and a merge-commit push run
/// through the real webhook handler. Neither makes a `github_commit` atom
/// (the PR path covers both), neither fetches a commit detail, and both
/// still advance the head cursor so the sweep has nothing left to compare.
/// The association endpoint here answers `[]` for everything, the way it
/// does for a PR merged seconds before the push webhook arrives.
#[tokio::test]
#[allow(clippy::await_holding_lock)]
async fn conformance_pr_merge_pushes_make_no_commit_atom_and_advance_the_cursor_pg() {
    let Some(base) = require_pg("conformance_pr_merge_pushes_make_no_commit_atom_and_advance_the_cursor_pg") else { return };
    let _kek = crate::workers::reembed::MASTER_KEK_HEX_ENV_LOCK.lock().await;
    let (mock_base, mock, handle) = spawn_head_mock("mg1").await;
    let _env = GithubEnvGuard::apply(&mock_base);
    let db = "cs_gh_conf_pr_merge_push";
    let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
    let (fx, mut admin) = head_fixture(&base, db, "prmergepush", 960_007, 6_800_007, "c0").await;

    fx.webhook_push("c0", "sq1", false).await.expect("squash-merge push");
    assert_eq!(fx.head().await.as_deref(), Some("sq1"), "a squash-merge push advances the head cursor");
    fx.webhook_push("sq1", "mg1", false).await.expect("merge-commit push");
    assert_eq!(fx.head().await.as_deref(), Some("mg1"), "a merge-commit push advances the head cursor");

    assert_eq!(fx.count_type("github_commit").await, 0, "PR merges are PR work: no github_commit atom");
    assert_eq!(HeadMock::hits(&mock, "commit"), 0, "no commit detail is fetched for PR work");

    HeadMock::reset(&mock);
    fx.sweep().await;
    assert_eq!(HeadMock::hits(&mock, "compare"), 0, "the sweep finds the head already covered");
    fx.assert_no_code_atoms("PR merge pushes").await;

    handle.abort();
    fx.pool.close().await;
    drop_db(&mut admin, db).await;
}

/// A backfill tick on a row whose retired code cursor was never finished
/// (the shape every repo connected before 2026-10-27 that was mid code walk
/// has in prod) goes straight to the narrative types: it walks no tree,
/// fetches no blob, and writes no code atom.
#[tokio::test]
#[allow(clippy::await_holding_lock)]
async fn conformance_backfill_tick_skips_the_retired_code_tier_pg() {
    let Some(base) = require_pg("conformance_backfill_tick_skips_the_retired_code_tier_pg") else { return };
    let _kek = crate::workers::reembed::MASTER_KEK_HEX_ENV_LOCK.lock().await;
    let (mock_base, mock, handle) = spawn_head_mock("c1").await;
    let _env = GithubEnvGuard::apply(&mock_base);
    let db = "cs_gh_conf_backfill_no_code";
    let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
    let (fx, mut admin) = head_fixture(&base, db, "backfillnocode", 960_006, 6_800_006, "c0").await;
    sqlx::query("UPDATE github_watched_repos SET code_cursor = $2, last_synced_at = NULL WHERE id = $1")
        .bind(fx.row_id)
        .bind(serde_json::json!({"tree_sha": "treec1", "next_index": 3, "total": 10, "done": false}))
        .execute(&fx.pool)
        .await
        .unwrap();

    let mut ticks = 0;
    loop {
        ticks += 1;
        assert!(ticks <= 20, "the backfill must finish on an all-empty narrative mock");
        match super::github_backfill::run_tick(&TenantId::Consumer, &fx.pool, &fx.state, fx.f.account_id, fx.row_id).await.expect("tick") {
            super::github_backfill::RunOutcome::Done => break,
            super::github_backfill::RunOutcome::Continue => continue,
            super::github_backfill::RunOutcome::RateLimited(_) => panic!("unexpected outcome: rate limited"),
            super::github_backfill::RunOutcome::Paused => panic!("unexpected outcome: paused"),
        }
    }

    let synced: Option<chrono::DateTime<chrono::Utc>> =
        sqlx::query_scalar("SELECT last_synced_at FROM github_watched_repos WHERE id = $1").bind(fx.row_id).fetch_one(&fx.pool).await.unwrap();
    assert!(synced.is_some(), "the narrative backfill ran to completion");
    assert_eq!(HeadMock::hits(&mock, "tree"), 0, "no repository tree was walked");
    assert_eq!(HeadMock::hits(&mock, "blob"), 0, "no file content was fetched");
    fx.assert_no_code_atoms("backfill").await;

    handle.abort();
    fx.pool.close().await;
    drop_db(&mut admin, db).await;
}

/// **The waste #1334 left behind.** A push webhook ingested `c0...c1`, but
/// only the sweep ever moved `head_tree_sha`, so the next sweep compared
/// `c0...c1` again and re-fetched everything in it. The webhook now advances
/// the cursor once the range has landed, so the sweep's only GitHub call for
/// the default branch is the `branches` head lookup.
#[tokio::test]
#[allow(clippy::await_holding_lock)]
async fn conformance_head_cursor_webhook_then_sweep_fetches_nothing_pg() {
    let Some(base) = require_pg("conformance_head_cursor_webhook_then_sweep_fetches_nothing_pg") else { return };
    let _kek = crate::workers::reembed::MASTER_KEK_HEX_ENV_LOCK.lock().await;
    let (mock_base, mock, handle) = spawn_head_mock("c1").await;
    let _env = GithubEnvGuard::apply(&mock_base);
    let db = "cs_gh_conf_head_webhook";
    let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
    let (fx, mut admin) = head_fixture(&base, db, "headwebhook", 960_001, 6_800_001, "c0").await;

    fx.webhook_push("c0", "c1", false).await.expect("webhook push");
    assert!(fx.has_commit("c1").await, "the webhook ingested the range");
    assert_eq!(fx.head().await.as_deref(), Some("c1"), "the webhook advanced the cursor past the range it ingested");

    HeadMock::reset(&mock);
    fx.sweep().await;
    assert_eq!(HeadMock::hits(&mock, "branches"), 1, "the sweep still checks the live head");
    assert_eq!(HeadMock::hits(&mock, "compare"), 0, "no compare for a range the webhook covered");
    assert_eq!(HeadMock::hits(&mock, "commit"), 0, "no commit re-fetch for a range the webhook covered");
    assert_eq!(fx.head().await.as_deref(), Some("c1"));

    handle.abort();
    fx.pool.close().await;
    drop_db(&mut admin, db).await;
}

/// A dropped delivery: no webhook ever arrives for `c0...c2`. The sweep is
/// the reconciler, so it compares from the stored cursor, ingests both
/// commits, advances, and its NEXT pass is free.
#[tokio::test]
#[allow(clippy::await_holding_lock)]
async fn conformance_head_cursor_dropped_webhook_sweep_catches_up_pg() {
    let Some(base) = require_pg("conformance_head_cursor_dropped_webhook_sweep_catches_up_pg") else { return };
    let _kek = crate::workers::reembed::MASTER_KEK_HEX_ENV_LOCK.lock().await;
    let (mock_base, mock, handle) = spawn_head_mock("c2").await;
    let _env = GithubEnvGuard::apply(&mock_base);
    let db = "cs_gh_conf_head_dropped";
    let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
    let (fx, mut admin) = head_fixture(&base, db, "headdropped", 960_002, 6_800_002, "c0").await;

    fx.sweep().await;
    assert!(HeadMock::hits(&mock, "compare") >= 1, "the sweep compared the missed range");
    assert_eq!(HeadMock::hits(&mock, "commit"), 2, "both missed commits fetched");
    assert!(fx.has_commit("c1").await && fx.has_commit("c2").await, "both missed commits ingested");
    assert_eq!(fx.head().await.as_deref(), Some("c2"));

    HeadMock::reset(&mock);
    fx.sweep().await;
    assert_eq!(HeadMock::hits(&mock, "compare"), 0, "caught up: the next pass compares nothing");
    assert_eq!(HeadMock::hits(&mock, "commit"), 0);

    handle.abort();
    fx.pool.close().await;
    drop_db(&mut admin, db).await;
}

/// A webhook whose ingest fails must not move the cursor, in either way it
/// can fail: GitHub will not serve a commit's detail (transient 502, which
/// fails the event so its job retries), or the write itself fails (here: the
/// account has no DEK). In both cases the sweep later re-compares the same
/// range and recovers it.
#[tokio::test]
#[allow(clippy::await_holding_lock)]
async fn conformance_head_cursor_failed_webhook_holds_and_sweep_recovers_pg() {
    let Some(base) = require_pg("conformance_head_cursor_failed_webhook_holds_and_sweep_recovers_pg") else { return };
    let _kek = crate::workers::reembed::MASTER_KEK_HEX_ENV_LOCK.lock().await;
    let (mock_base, mock, handle) = spawn_head_mock("c1").await;
    let _env = GithubEnvGuard::apply(&mock_base);
    let db = "cs_gh_conf_head_failed";
    let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
    let (fx, mut admin) = head_fixture(&base, db, "headfailed", 960_003, 6_800_003, "c0").await;

    // 1. The commit fetch fails transiently: the event fails (its job
    //    retries) and nothing moves.
    mock.lock().unwrap().fail_commits = true;
    assert!(fx.webhook_push("c0", "c1", false).await.is_err(), "a transient commit fetch failure fails the event");
    assert!(!fx.has_commit("c1").await);
    assert_eq!(fx.head().await.as_deref(), Some("c0"), "a range whose commit fetch failed must not advance the cursor");
    // The sweep hits the same outage: still held.
    fx.sweep().await;
    assert_eq!(fx.head().await.as_deref(), Some("c0"), "the sweep does not advance past a transient failure either");
    mock.lock().unwrap().fail_commits = false;
    HeadMock::reset(&mock);
    fx.sweep().await;
    assert_eq!(HeadMock::hits(&mock, "commit"), 1, "the sweep re-fetched the commit the webhook failed on");
    assert!(fx.has_commit("c1").await, "and ingested it");
    assert_eq!(fx.head().await.as_deref(), Some("c1"));

    // 2. The write fails (no DEK): the content is fetched but never lands.
    let dek: Option<Vec<u8>> = sqlx::query_scalar("SELECT dek_wrapped FROM accounts WHERE id = $1").bind(fx.f.account_id).fetch_one(&fx.pool).await.unwrap();
    sqlx::query("UPDATE accounts SET dek_wrapped = NULL WHERE id = $1").bind(fx.f.account_id).execute(&fx.pool).await.unwrap();
    HeadMock::set_head(&mock, "c2");
    let no_cached_dek = Arc::new(crate::test_app_state_owned_with_db_and_store(fx.pool.clone(), None));
    fx.webhook_push_via(&no_cached_dek, "c1", "c2", false).await.expect("the event itself is handled");
    assert!(!fx.has_commit("c2").await);
    assert_eq!(fx.head().await.as_deref(), Some("c1"), "a range whose write failed must not advance the cursor");
    sqlx::query("UPDATE accounts SET dek_wrapped = $2 WHERE id = $1").bind(fx.f.account_id).bind(dek).execute(&fx.pool).await.unwrap();
    fx.sweep().await;
    assert!(fx.has_commit("c2").await, "the sweep recovered the range once the write works again");
    assert_eq!(fx.head().await.as_deref(), Some("c2"));
    fx.assert_no_code_atoms("failed webhook recovery").await;

    handle.abort();
    fx.pool.close().await;
    drop_db(&mut admin, db).await;
}

/// Out-of-order and redelivered webhooks, then a force-push. The cursor only
/// moves by compare-and-swap from a push's `before`, so it never moves
/// backwards and never jumps over a range nobody ingested.
#[tokio::test]
#[allow(clippy::await_holding_lock)]
async fn conformance_head_cursor_out_of_order_and_force_push_pg() {
    let Some(base) = require_pg("conformance_head_cursor_out_of_order_and_force_push_pg") else { return };
    let _kek = crate::workers::reembed::MASTER_KEK_HEX_ENV_LOCK.lock().await;
    let (mock_base, mock, handle) = spawn_head_mock("c3").await;
    let _env = GithubEnvGuard::apply(&mock_base);
    let db = "cs_gh_conf_head_order";
    let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
    let (fx, mut admin) = head_fixture(&base, db, "headorder", 960_004, 6_800_004, "c0").await;

    // `c2...c3` arrives before `c0...c1` and `c1...c2`. Its content lands,
    // but the cursor cannot jump to c3: c1 and c2 are not ingested yet.
    fx.webhook_push("c2", "c3", false).await.unwrap();
    assert!(fx.has_commit("c3").await);
    assert_eq!(fx.head().await.as_deref(), Some("c0"), "an out-of-order push must not skip the ranges before it");

    fx.webhook_push("c0", "c1", false).await.unwrap();
    assert_eq!(fx.head().await.as_deref(), Some("c1"));
    // A redelivery of an already-applied range does not move it back.
    fx.webhook_push("c0", "c1", false).await.unwrap();
    assert_eq!(fx.head().await.as_deref(), Some("c1"), "a redelivered older push must not move the cursor backwards");

    // `c1...c2` was dropped. The sweep compares from c1, so c2 is not
    // skipped even though c3's push already landed.
    HeadMock::reset(&mock);
    fx.sweep().await;
    assert_eq!(HeadMock::hits(&mock, "commit"), 2, "the sweep fetched c2 and c3 (c3 again: safe, not skipped)");
    assert!(fx.has_commit("c1").await && fx.has_commit("c2").await && fx.has_commit("c3").await);
    assert_eq!(fx.head().await.as_deref(), Some("c3"));

    // A late delivery of the dropped `c1...c2` cannot drag the cursor back.
    fx.webhook_push("c1", "c2", false).await.unwrap();
    assert_eq!(fx.head().await.as_deref(), Some("c3"), "a late delivery must not move the cursor backwards");

    // Force-push: main goes from c3 to x2 (parent c1), dropping c2 and c3.
    HeadMock::set_head(&mock, "x2");
    fx.webhook_push("c3", "x2", true).await.unwrap();
    assert!(fx.has_commit("x2").await, "the forced range's new commit is ingested");
    assert_eq!(fx.head().await.as_deref(), Some("x2"), "a force-push advances to the new head, as the sweep's own compare would");
    HeadMock::reset(&mock);
    fx.sweep().await;
    assert_eq!(HeadMock::hits(&mock, "compare"), 0, "nothing left for the sweep after the force-push webhook");
    assert_eq!(HeadMock::hits(&mock, "commit"), 0);
    assert_eq!(HeadMock::hits(&mock, "blob"), 0, "no blob was fetched at any point");
    fx.assert_no_code_atoms("force push").await;

    handle.abort();
    fx.pool.close().await;
    drop_db(&mut admin, db).await;
}
