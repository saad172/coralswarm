//! `chunk_reconcile::backfill` (bin `backfill_chunk_links`) on real
//! PostgreSQL: legacy positional atoms are linked to chunkless versions ONLY
//! on exact content equality, so the node_atoms bridge can see them.
mod support;

use coralswarm_backend::chunk_reconcile::backfill::{self, Options, Report};
use coralswarm_backend::source_evidence::{capture_source_version_tx, CaptureSource};
use coralswarm_core::chunker::{chunk, ChunkStrategy};
use serde_json::json;
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::PgPool;
use std::str::FromStr;
use uuid::Uuid;

fn options() -> Option<PgConnectOptions> {
    let Ok(url) = std::env::var("RELINFER_TEST_DATABASE_URL") else {
        assert!(std::env::var_os("REQUIRE_PG_SUITE").is_none(), "RELINFER_TEST_DATABASE_URL required");
        eprintln!("skipping chunk link backfill test: set RELINFER_TEST_DATABASE_URL");
        return None;
    };
    let options = PgConnectOptions::from_str(&url).unwrap();
    assert!(matches!(options.get_host(), "127.0.0.1" | "localhost" | "::1"));
    Some(options)
}

async fn seed(pool: &PgPool) -> (Uuid, Uuid, Uuid) {
    let sub = format!("chunk-backfill-{}", Uuid::new_v4());
    let user: Uuid = sqlx::query_scalar("INSERT INTO users(email,clerk_user_id) VALUES($1,$2) RETURNING id")
        .bind(format!("{sub}@test.local")).bind(&sub).fetch_one(pool).await.unwrap();
    let account: Uuid = sqlx::query_scalar("INSERT INTO accounts(owner_user_id) VALUES($1) RETURNING id")
        .bind(user).fetch_one(pool).await.unwrap();
    let graph: Uuid = sqlx::query_scalar("INSERT INTO graphs(account_id,name) VALUES($1,'chunk-backfill') RETURNING id")
        .bind(account).fetch_one(pool).await.unwrap();
    (user, account, graph)
}

async fn capture(pool: &PgPool, graph: Uuid, account: Uuid, source_type: &str, native: &str, text: &str) -> (Uuid, Uuid) {
    let mut tx = pool.begin().await.unwrap();
    let captured = capture_source_version_tx(
        &mut tx, None, graph, account,
        &CaptureSource {
            source_type, provider_namespace: "declared:test:backfill", source_id: native,
            normalized_text: text, representation: "normalized_text", metadata: &json!({}),
            occurred_at: None, participants: &[], ingested_by: None, scope: "private",
            duration_ms: None, segments: None,
        },
        None, &[42; 32],
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();
    (captured.source_item_id, captured.version_id)
}

/// A pre-fix positional atom: inline text, never linked, no hash.
async fn legacy_atom(pool: &PgPool, graph: Uuid, source_type: &str, sid: &str, text: &str, header_len: Option<i32>) -> Uuid {
    sqlx::query_scalar(
        "INSERT INTO atoms(graph_id,source_type,source_id,content_blob_key,content_inline,header_len,timestamp) \
         VALUES($1,$2,$3,$4,$5,$6,now()) RETURNING atom_id",
    )
    .bind(graph).bind(source_type).bind(sid).bind(format!("{graph}/{}", Uuid::new_v4())).bind(text).bind(header_len)
    .fetch_one(pool).await.unwrap()
}

async fn links(pool: &PgPool, graph: Uuid, version: Uuid) -> Vec<Uuid> {
    sqlx::query_scalar("SELECT atom_id FROM source_version_chunks WHERE graph_id=$1 AND source_version_id=$2 ORDER BY atom_id")
        .bind(graph).bind(version).fetch_all(pool).await.unwrap()
}

fn para(tag: &str) -> String {
    (0..280).map(|i| format!("{tag}w{i}")).collect::<Vec<_>>().join(" ") + "."
}

fn opts(graph: Uuid, execute: bool, replace_current: bool) -> Options {
    Options { graph_id: Some(graph), batch_size: 2, max_batches: None, execute, replace_current }
}

#[tokio::test]
async fn backfill_links_only_verified_chunks_and_clears_bridge_gaps() {
    let Some(options) = options() else { return };
    let owner = PgPoolOptions::new().max_connections(4).connect_with(options.clone()).await.unwrap();
    support::app_role::set_app_role_password_with_retry(&owner).await;
    let (_user, account, graph) = seed(&owner).await;

    // (1) Reef-linked current version, three positions, all legacy atoms
    // verifiable: one plain, one with a recorded header.
    let doc = [para("alpha"), para("bravo"), para("charlie")].join("\n\n");
    let bodies: Vec<String> = chunk(&doc, ChunkStrategy::Generic).iter().map(|c| c.text_with_speaker()).collect();
    assert!(bodies.len() >= 3, "fixture: {} chunks", bodies.len());
    let (_, reefed) = capture(&owner, graph, account, "note", "reefed", &doc).await;
    let mut reef_atoms = Vec::new();
    for (pos, body) in bodies.iter().enumerate() {
        let (text, header_len) = if pos == 1 {
            (format!("Date: 2026-01-01.\n\n{body}"), Some("Date: 2026-01-01.".len() as i32 + 2))
        } else {
            (body.clone(), None)
        };
        reef_atoms.push(legacy_atom(&owner, graph, "note", &format!("reefed:chunk:{pos}"), &text, header_len).await);
    }
    let semantic: Uuid = sqlx::query_scalar("INSERT INTO semantic_atoms(graph_id,source_version_id,ordinal,kind,statement,extraction_version,model) VALUES($1,$2,0,'decision','alpha','source-atoms-v1','fixture') RETURNING semantic_atom_id")
        .bind(graph).bind(reefed).fetch_one(&owner).await.unwrap();
    let node: Uuid = sqlx::query_scalar("INSERT INTO nodes(graph_id,type,title) VALUES($1,'entity','Alpha') RETURNING node_id")
        .bind(graph).fetch_one(&owner).await.unwrap();
    sqlx::query("INSERT INTO node_semantic_atoms(graph_id,node_id,semantic_atom_id) VALUES($1,$2,$3)")
        .bind(graph).bind(node).bind(semantic).execute(&owner).await.unwrap();

    // (2) Slack-shaped atom: one-line header with no header_len.
    let (_, slack) = capture(&owner, graph, account, "slack", "C1:171.1", "ship it friday").await;
    let slack_atom = legacy_atom(&owner, graph, "slack", "C1:171.1:chunk:0", "Ada in #general · 2026-01-01 10:00 UTC\n\nship it friday", None).await;

    // (3) Current version whose stored chunk is stale: never guessed.
    let (_, stale) = capture(&owner, graph, account, "note", "stale", "the meeting moved to Tuesday").await;
    let stale_atom = legacy_atom(&owner, graph, "note", "stale:chunk:0", "the meeting is on Monday", None).await;

    // (4) Non-current version mismatch: v1 text differs from the atom, which
    // matches the current v2.
    let (_, old) = capture(&owner, graph, account, "note", "edited", "first draft").await;
    let (_, new) = capture(&owner, graph, account, "note", "edited", "final text").await;
    assert_ne!(old, new);
    let edited_atom = legacy_atom(&owner, graph, "note", "edited:chunk:0", "final text", None).await;

    let bridge_before = coralswarm_backend::node_atoms_bridge::preview(&owner, Some(graph)).await.unwrap();
    assert_eq!(bridge_before.pairs_without_chunks, 1, "fixture: the reef's version has no chunks");

    // RLS-filtered roles are refused.
    let app = PgPoolOptions::new().max_connections(1)
        .connect_with(options.clone().username("coralswarm_app").password(support::app_role::APP_ROLE_TEST_PASSWORD))
        .await.unwrap();
    assert!(coralswarm_backend::node_atoms_bridge::assert_policy_bypass_on(&app, backfill::TABLES).await.is_err());
    app.close().await;
    coralswarm_backend::node_atoms_bridge::assert_policy_bypass_on(&owner, backfill::TABLES).await.unwrap();

    // Dry run: counts, nothing written.
    let dry = backfill::run(&owner, None, &opts(graph, false, false), |_| {}).await.unwrap();
    let n = bodies.len() as u64;
    assert_eq!(dry.versions_examined, 5, "{dry:?}");
    assert_eq!(dry.versions_reef_linked, 1, "{dry:?}");
    assert_eq!(dry.linked, n + 2, "reef chunks + slack + edited v2: {dry:?}");
    assert_eq!(dry.mismatched_current, 1, "{dry:?}");
    assert_eq!(dry.mismatched_noncurrent, 1, "{dry:?}");
    assert_eq!(dry.replaced, 0);
    for v in [reefed, slack, stale, old, new] {
        assert!(links(&owner, graph, v).await.is_empty(), "dry run writes nothing");
    }
    let hashed: i64 = sqlx::query_scalar("SELECT count(*) FROM atoms WHERE graph_id=$1 AND chunk_sha256 IS NOT NULL")
        .bind(graph).fetch_one(&owner).await.unwrap();
    assert_eq!(hashed, 0);

    // Execute: exactly the verified positions.
    let done = backfill::run(&owner, None, &opts(graph, true, false), |_| {}).await.unwrap();
    assert_eq!(done.linked, dry.linked, "{done:?}");
    let mut expected = reef_atoms.clone();
    expected.sort();
    assert_eq!(links(&owner, graph, reefed).await, expected);
    assert_eq!(links(&owner, graph, slack).await, vec![slack_atom]);
    assert_eq!(links(&owner, graph, new).await, vec![edited_atom]);
    assert!(links(&owner, graph, old).await.is_empty(), "a non-current mismatch is left unlinked");
    assert!(links(&owner, graph, stale).await.is_empty(), "a current mismatch needs --replace-current");
    let stale_text: String = sqlx::query_scalar("SELECT content_inline FROM atoms WHERE atom_id=$1")
        .bind(stale_atom).fetch_one(&owner).await.unwrap();
    assert_eq!(stale_text, "the meeting is on Monday");

    let bridge_after = coralswarm_backend::node_atoms_bridge::preview(&owner, Some(graph)).await.unwrap();
    assert_eq!(bridge_after.pairs_without_chunks, 0, "{bridge_after:?}");
    assert_eq!(bridge_after.missing_links, n as i64, "the bridge can now link every chunk");

    // Idempotent.
    let again = backfill::run(&owner, None, &opts(graph, true, false), |_| {}).await.unwrap();
    assert_eq!(again.linked, 0, "{again:?}");

    // Opt-in replacement of the current mismatch.
    let replaced: Report = backfill::run(&owner, None, &opts(graph, true, true), |_| {}).await.unwrap();
    assert_eq!(replaced.replaced, 1, "{replaced:?}");
    assert_eq!(links(&owner, graph, stale).await, vec![stale_atom]);
    let (text, superseded): (String, Vec<String>) = sqlx::query_as("SELECT content_inline,superseded_blob_keys FROM atoms WHERE atom_id=$1")
        .bind(stale_atom).fetch_one(&owner).await.unwrap();
    assert_eq!(text, "the meeting moved to Tuesday");
    assert_eq!(superseded.len(), 1);
    assert!(links(&owner, graph, old).await.is_empty(), "non-current versions are never replaced into");

    // Cleanup.
    let mut tx = coralswarm_backend::tenancy::scope::begin_account_scoped(&owner, account).await.unwrap();
    coralswarm_backend::billing::renewal::lock_period_rotation(&mut tx, account).await.unwrap();
    sqlx::query("DELETE FROM jobs WHERE account_id=$1").bind(account).execute(&mut *tx).await.unwrap();
    sqlx::query("DELETE FROM accounts WHERE id=$1").bind(account).execute(&mut *tx).await.unwrap();
    tx.commit().await.unwrap();
    owner.close().await;
}
