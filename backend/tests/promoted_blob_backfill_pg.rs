//! Live test for `promoted_blob_backfill` (the `backfill_promoted_blobs`
//! binary's shared core): the candidate query finds a seeded pre-fix row,
//! a dry run touches nothing, and `--apply` rewrites the key to the correct
//! `blob_key_for` shape and re-uploads the content. Legacy derivation
//! enqueues record Unsupported terminal receipts; the digest pass (managed
//! enrichment) and embedding metadata remain queued without invoking a
//! worker or authorizing paid dispatch.
//!
//! Covers BOTH arms of `CANDIDATES_SQL` (see its own doc comment): arm 1's
//! intact `{graph}/{atom}` placeholder key, and arm 2's BLANK key — the
//! shape `workers::atom_title_backfill`'s terminal-unfetchable sweep leaves
//! behind, which arm 1 cannot see at all and which accounts for 1,191
//! still-broken prod rows. The arm-2 tests are
//! `blank_key_candidate_is_selected_and_repaired_end_to_end` (selected AND
//! repaired end to end),
//! `blank_key_without_promoted_provenance_is_not_selected` (the provenance
//! narrowing actually narrows — a blank key alone is never enough), and
//! `blank_source_key_is_unrepairable_not_an_error` (the opposite direction:
//! a blank key on the SOURCE side is the one case that cannot be repaired,
//! and must not flip the exit code).
//!
//! Gated on `RELINFER_TEST_DATABASE_URL` (a Postgres with `migrations_control`
//! applied) AND `R2_TEST_ENDPOINT_OVERRIDE`/`R2_TEST_ACCESS_KEY`/
//! `R2_TEST_SECRET_KEY`/`R2_TEST_BUCKET` (a local S3-compatible fake, e.g.
//! MinIO — see `r2_extract_success_pg.rs`'s module doc for the docker
//! commands). A normal `cargo test` with neither configured skips it.
//!
//! Run with:
//!   RELINFER_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5442/coralswarm_relinfer_test \
//!   R2_TEST_ENDPOINT_OVERRIDE=http://localhost:19000 \
//!   R2_TEST_ACCESS_KEY=minioadmin \
//!   R2_TEST_SECRET_KEY=minioadmin \
//!   R2_TEST_BUCKET=cs-test-bucket \
//!     cargo test -p coralswarm-backend --test promoted_blob_backfill_pg -- --nocapture

use coralswarm_backend::promoted_blob_backfill::{
    assert_policy_bypass, list_candidates, process_candidate, resolve_org_id, OutcomeKind, Totals,
    BLOB_IO_CONCURRENCY,
};
use coralswarm_backend::store::{PostgresStore, Store};
use coralswarm_backend::tenancy::TenantId;
use coralswarm_core::kek::{generate_dek, load_kek, wrap_dek};
use coralswarm_core::r2::{blob_key_for, R2Client};
use futures::StreamExt;
use sqlx::postgres::PgPoolOptions;
use uuid::Uuid;

/// Sets a fixed, deterministic test `MASTER_KEK_HEX` — idempotently, exactly
/// ONCE per process, via `std::sync::Once`, rather than an unconditional
/// `set_var` on every call (CodeRabbit PR #838 review). This binary has
/// several tests that each call this before their own `load_kek()`
/// (`candidate_query_finds_a_seeded_broken_row_and_apply_fixes_it` directly,
/// the rest via `setup_tenant_with_graphs`); `std::env::set_var` requires
/// exclusive process-environment access for the DURATION of the call
/// (UB otherwise if another thread is concurrently reading/writing the
/// environment — this is not merely a `--test-threads=1` convention, it is
/// the stdlib's own soundness contract), so relying on "no other test in
/// this binary is running right now" to make every one of those repeat
/// `set_var` calls sound is exactly what a `// SAFETY:` comment alone would
/// be asserting without enforcing. Calling `set_var` with the SAME value
/// every time never needed to be a race in the first place: `Once`
/// guarantees the ONE actual write happens before any `call_once` returns
/// (on any thread), and after that first call there is no writer left for a
/// later `load_kek()` read to race, in this binary, ever.
fn set_test_kek() {
    static INIT: std::sync::Once = std::sync::Once::new();
    INIT.call_once(|| {
        // SAFETY: `Once::call_once` runs this closure at most once, and its
        // completion happens-before every `call_once` return (including on
        // other threads) — no concurrent reader/writer of the environment
        // can observe a partial write, and no later call in this process
        // re-enters `set_var` at all.
        unsafe { std::env::set_var("MASTER_KEK_HEX", "ab".repeat(32)) };
    });
}

struct R2TestEnv {
    endpoint: String,
    access_key: String,
    secret_key: String,
    bucket: String,
}

fn r2_test_env() -> Option<R2TestEnv> {
    Some(R2TestEnv {
        endpoint: std::env::var("R2_TEST_ENDPOINT_OVERRIDE").ok()?,
        access_key: std::env::var("R2_TEST_ACCESS_KEY").ok()?,
        secret_key: std::env::var("R2_TEST_SECRET_KEY").ok()?,
        bucket: std::env::var("R2_TEST_BUCKET").ok()?,
    })
}

const SKIP_REASON: &str =
    "skipping: set RELINFER_TEST_DATABASE_URL (a migrated Postgres) AND R2_TEST_ENDPOINT_OVERRIDE \
     / R2_TEST_ACCESS_KEY / R2_TEST_SECRET_KEY / R2_TEST_BUCKET (a local S3-compatible fake, e.g. \
     MinIO — see r2_extract_success_pg.rs's module doc comment for the docker commands) to run \
     the live promoted_blob_backfill regression";

// Snapshot only this fresh fixture account; other concurrently running PG
// tests must not change these assertions. These reads never activate a worker.
async fn backfill_jobs(pool: &sqlx::PgPool, account: Uuid) -> Vec<serde_json::Value> {
    sqlx::query_scalar("SELECT to_jsonb(j) FROM jobs j WHERE account_id=$1 ORDER BY id")
        .bind(account).fetch_all(pool).await.unwrap()
}

async fn backfill_nonexecution_state(
    pool: &sqlx::PgPool, account: Uuid, source_graph: Uuid, target_graph: Uuid,
) -> serde_json::Value {
    let mut state = serde_json::Map::new();
    for table in ["credit_balances", "credit_ledger", "managed_budget_periods",
                  "managed_attempt_receipts", "managed_credit_reservations",
                  "managed_embedding_inputs", "managed_enrichment_inputs", "llm_usage", "llm_usage_pending"] {
        let rows: serde_json::Value = sqlx::query_scalar(&format!(
            "SELECT COALESCE(jsonb_agg(to_jsonb(r) ORDER BY to_jsonb(r)::text),'[]'::jsonb) \
             FROM {table} r WHERE account_id=$1",
        )).bind(account).fetch_one(pool).await.unwrap();
        state.insert(table.to_string(), rows);
    }
    let source_atoms: serde_json::Value = sqlx::query_scalar(
        "SELECT COALESCE(jsonb_agg(to_jsonb(a) ORDER BY atom_id),'[]'::jsonb) \
         FROM atoms a WHERE graph_id=$1",
    ).bind(source_graph).fetch_one(pool).await.unwrap();
    assert_eq!(source_atoms.as_array().unwrap().len(), 1, "the private source row control must be nonempty");
    state.insert("source_atoms".to_string(), source_atoms);
    for table in ["semantic_atoms", "relationship_edge"] {
        let rows: serde_json::Value = sqlx::query_scalar(&format!(
            "SELECT COALESCE(jsonb_agg(to_jsonb(r) ORDER BY to_jsonb(r)::text),'[]'::jsonb) \
             FROM {table} r WHERE graph_id=ANY($1)",
        )).bind(vec![source_graph, target_graph]).fetch_one(pool).await.unwrap();
        state.insert(table.to_string(), rows);
    }
    serde_json::Value::Object(state)
}

async fn assert_backfill_receipts(
    pool: &sqlx::PgPool, account: Uuid, graph: Uuid, atom: Uuid,
    node: Uuid, before: &[serde_json::Value], jobs_reenqueued: u64,
) -> Vec<serde_json::Value> {
    let after = backfill_jobs(pool, account).await;
    for old in before {
        let retained = after.iter().find(|row| row["id"] == old["id"])
            .expect("each original job, including the seeded dead row, must remain");
        assert_eq!(retained, old, "backfill must not resurrect or rewrite old job rows");
    }
    let fresh: Vec<_> = after.iter()
        .filter(|row| !before.iter().any(|old| old["id"] == row["id"]))
        .collect();
    assert_eq!(fresh.len(), 2, "one embedding metadata job and one admitted managed digest pass; no retired per-atom extraction");
    assert_eq!(jobs_reenqueued, 1, "the backfill tally counts the digest receipt, not dispatched work");
    for (kind, key, payload) in [
        // Admitted on managed_enrichment (source-agnostic PR 8): pending, never run here.
        ("RegenerateDigest", serde_json::json!(format!("digest:{node}")), serde_json::json!({"node_id":node,"graph_id":graph})),
        ("EmbedAtoms", serde_json::Value::Null, serde_json::json!({"atom_ids":[atom]})),
    ] {
        let matched: Vec<_> = fresh.iter().filter(|row|
            row["kind"] == kind && row["idempotency_key"] == key && row["payload"] == payload
        ).collect();
        assert_eq!(matched.len(), 1, "exact kind/key/payload tuple required for {kind}");
        let row = matched[0];
        assert_eq!(row["account_id"], serde_json::json!(account));
        assert_eq!(row["attempts"], 0, "enqueue must not execute {kind}");
        assert!(row["started_at"].is_null());
        assert!(row["outcome"].is_null(), "no completion outcome may be fabricated for {kind}");
        assert!(row["failure_class"].is_null());
        assert_eq!(row["status"], "pending", "{kind} is admitted without running a worker");
        assert!(row["error_message"].is_null());
        assert!(row["finished_at"].is_null());
    }
    after
}

#[tokio::test]
async fn candidate_query_finds_a_seeded_broken_row_and_apply_fixes_it() {
    let Ok(db_url) = std::env::var("RELINFER_TEST_DATABASE_URL") else {
        eprintln!("{SKIP_REASON}");
        return;
    };
    let Some(r2env) = r2_test_env() else {
        eprintln!("{SKIP_REASON}");
        return;
    };

    set_test_kek();
    let kek = load_kek().expect("test KEK loads");
    let dek = generate_dek();
    let wrapped = wrap_dek(&kek, &dek).expect("wrap test DEK");

    let pool = PgPoolOptions::new().max_connections(4).connect(&db_url).await.expect("connect");
    let store = PostgresStore::new(pool.clone());

    let email = format!("{}@test.local", Uuid::new_v4());
    let user_id: Uuid = sqlx::query_scalar("INSERT INTO users (email) VALUES ($1) RETURNING id")
        .bind(&email)
        .fetch_one(&pool)
        .await
        .expect("insert user");
    let account_id: Uuid = sqlx::query_scalar(
        "INSERT INTO accounts (owner_user_id, dek_wrapped) VALUES ($1, $2) RETURNING id",
    )
    .bind(user_id)
    .bind(&wrapped)
    .fetch_one(&pool)
    .await
    .expect("insert account with wrapped DEK");

    let source_graph = store
        .create_graph(account_id, "backfill-source", "personal")
        .await
        .expect("create source graph");
    let target_graph =
        store.create_graph(account_id, "backfill-target", "team").await.expect("create target");

    let r2 = R2Client::new_with_endpoint_override(
        "unused-account-id",
        &r2env.access_key,
        &r2env.secret_key,
        &r2env.bucket,
        &r2env.endpoint,
    )
    .await;

    // ── Seed the SOURCE atom with real R2 content ─────────────────────────────
    let source_atom_id = Uuid::new_v4();
    let plaintext = format!("backfill regression source content {}", Uuid::new_v4());
    sqlx::query(
        "INSERT INTO atoms (atom_id, graph_id, source_type, source_id, content_blob_key, \
                             participants, timestamp) \
         VALUES ($1, $2, 'test', $3, 'placeholder-never-read', '{}', now())",
    )
    .bind(source_atom_id)
    .bind(source_graph.id)
    .bind(source_atom_id.to_string())
    .execute(&pool)
    .await
    .expect("insert source atom row");
    let source_key = r2
        .put_atom_blob(None, source_graph.id, source_atom_id, plaintext.as_bytes(), &dek)
        .await
        .expect("seed source blob");
    sqlx::query("UPDATE atoms SET content_blob_key = $1 WHERE atom_id = $2")
        .bind(&source_key)
        .bind(source_atom_id)
        .execute(&pool)
        .await
        .expect("stamp source key");

    // ── Seed a BROKEN promoted copy — exactly what the pre-fix
    // `copy_atoms_to_graph_scoped` produced: same (source_type, source_id),
    // a placeholder `{target_graph}/{atom_id}` key, NO real R2 object at
    // that key, and a promoted node linking it back to the source graph. ────
    let broken_atom_id = Uuid::new_v4();
    let broken_key = format!("{}/{}", target_graph.id, broken_atom_id);
    sqlx::query(
        "INSERT INTO atoms (atom_id, graph_id, source_type, source_id, content_blob_key, \
                             participants, timestamp) \
         VALUES ($1, $2, 'test', $3, $4, '{}', now())",
    )
    .bind(broken_atom_id)
    .bind(target_graph.id)
    .bind(source_atom_id.to_string()) // same source_id as the source atom (the dedup key)
    .bind(&broken_key)
    .execute(&pool)
    .await
    .expect("insert broken promoted-copy atom row");

    let promoted_node_id: Uuid = sqlx::query_scalar(
        "INSERT INTO nodes (graph_id, type, title, promoted_from_graph_id) \
         VALUES ($1, 'entity', 'Backfill Target Reef', $2) RETURNING node_id",
    )
    .bind(target_graph.id)
    .bind(source_graph.id)
    .fetch_one(&pool)
    .await
    .expect("insert promoted node");
    sqlx::query("INSERT INTO node_atoms (node_id, atom_id) VALUES ($1, $2)")
        .bind(promoted_node_id)
        .bind(broken_atom_id)
        .execute(&pool)
        .await
        .expect("link broken atom to promoted node");

    // Old, now-dead derivation jobs for this atom — proves the backfill
    // re-enqueues fresh work rather than assuming none ever ran.
    sqlx::query(
        "INSERT INTO jobs (kind, payload, account_id, idempotency_key, status) \
         VALUES ('ExtractEntities', $1, $2, $3, 'dead')",
    )
    .bind(serde_json::json!({"atom_id": broken_atom_id}))
    .bind(account_id)
    .bind(format!("extract:{broken_atom_id}"))
    .execute(&pool)
    .await
    .expect("seed a dead ExtractEntities job");

    // ── resolve_org_id: this is a control-schema DB (no `tenant_meta`
    // table at all) => consumer/control plane, org_id = None. ────────────────
    let org_id = resolve_org_id(&pool, None).await.expect("resolve_org_id on a control-schema DB");
    assert_eq!(org_id, None, "a control-plane database has no tenant_meta row");
    let bad_override = resolve_org_id(&pool, Some("org_bogus")).await;
    assert!(
        bad_override.is_err(),
        "an --org-id override with no tenant_meta row to check it against must be refused"
    );

    // ── Candidate query finds the broken row, and ONLY the broken row
    // (never the source atom, never a hypothetical correctly-keyed one). ────
    let candidates = list_candidates(&pool, Some(target_graph.id), &[])
        .await
        .expect("list_candidates");
    assert_eq!(candidates.len(), 1, "exactly the one broken row must be selected");
    let cand = &candidates[0];
    assert_eq!(cand.atom_id, broken_atom_id);
    assert_eq!(cand.graph_id, target_graph.id);
    assert_eq!(cand.account_id, account_id);
    assert_eq!(cand.source_id, source_atom_id.to_string());
    assert_eq!(cand.candidate_source_graphs, vec![source_graph.id]);
    assert_eq!(cand.content_blob_key, broken_key);

    let before_jobs = backfill_jobs(&pool, account_id).await;
    let before_effects = backfill_nonexecution_state(&pool, account_id, source_graph.id, target_graph.id).await;

    // ── Dry run: touches nothing. ─────────────────────────────────────────────
    let tenant = TenantId::Consumer;
    let dek_cache = tokio::sync::Mutex::new(std::collections::HashMap::new());
    let dry_outcome =
        process_candidate(&pool, &r2, org_id.as_deref(), &tenant, &dek_cache, cand, false).await;
    let mut dry_totals = Totals::default();
    dry_totals.apply(&dry_outcome);
    assert!(dry_outcome.report.contains("dry run"), "{}", dry_outcome.report);
    assert_eq!(dry_totals.candidates, 1);
    assert_eq!(dry_totals.put_ok, 0, "a dry run must never write");
    let unchanged: String =
        sqlx::query_scalar("SELECT content_blob_key FROM atoms WHERE atom_id = $1")
            .bind(broken_atom_id)
            .fetch_one(&pool)
            .await
            .expect("row still exists");
    assert_eq!(unchanged, broken_key, "a dry run must never touch the row");
    assert_eq!(backfill_jobs(&pool, account_id).await, before_jobs);
    assert_eq!(backfill_nonexecution_state(&pool, account_id, source_graph.id, target_graph.id).await, before_effects);

    // ── Apply: rewrites the key, re-uploads the content, re-enqueues jobs. ───
    let apply_outcome =
        process_candidate(&pool, &r2, org_id.as_deref(), &tenant, &dek_cache, cand, true).await;
    let mut apply_totals = Totals::default();
    apply_totals.apply(&apply_outcome);
    let apply_report = &apply_outcome.report;
    assert_eq!(apply_totals.put_ok, 1, "{apply_report}");
    assert_eq!(apply_totals.errors, 0, "{apply_report}");
    assert_eq!(apply_totals.dead_jobs_seen, 1, "the seeded dead job must be counted, not resurrected");
    assert!(apply_totals.jobs_reenqueued >= 1, "1 digest job expected: {apply_report}");

    let fixed_key: String =
        sqlx::query_scalar("SELECT content_blob_key FROM atoms WHERE atom_id = $1")
            .bind(broken_atom_id)
            .fetch_one(&pool)
            .await
            .expect("row still exists");
    coralswarm_core::r2::validate_blob_key(&fixed_key).unwrap();
    assert!(fixed_key.starts_with(&format!("{}/", target_graph.id)));
    assert_ne!(fixed_key, broken_key, "repair must publish a fresh pointer");

    let read_back = r2
        .get_atom_blob(&fixed_key, &dek)
        .await
        .expect("the backfilled key must resolve to real, re-uploaded content");
    assert_eq!(String::from_utf8_lossy(&read_back), plaintext);

    // The original dead row remains unchanged; fresh terminal Unsupported
    // receipts are distinct from it. Full-row identity is checked below.
    let dead_still_dead: String =
        sqlx::query_scalar("SELECT status::text FROM jobs WHERE idempotency_key = $1 AND status = 'dead'")
            .bind(format!("extract:{broken_atom_id}"))
            .fetch_one(&pool)
            .await
            .expect("the original dead row must still exist, untouched");
    assert_eq!(dead_still_dead, "dead");
    let after_jobs = assert_backfill_receipts(
        &pool, account_id, target_graph.id, broken_atom_id, promoted_node_id,
        &before_jobs, apply_totals.jobs_reenqueued,
    ).await;
    assert_eq!(backfill_nonexecution_state(&pool, account_id, source_graph.id, target_graph.id).await, before_effects);
    let original_bytes = r2.get_atom_blob(&source_key, &dek).await.expect("private source blob remains readable");
    assert_eq!(String::from_utf8_lossy(&original_bytes), plaintext, "repair must preserve source blob bytes");

    let candidates_unexcluded = list_candidates(&pool, Some(target_graph.id), &[])
        .await
        .expect("list_candidates without exclusion");
    assert!(
        candidates_unexcluded.is_empty(),
        "a fresh pointer must leave the repair selection"
    );
    let candidates_excluded = list_candidates(&pool, Some(target_graph.id), &[broken_atom_id])
        .await
        .expect("list_candidates with the fixed atom excluded");
    assert!(
        candidates_excluded.is_empty(),
        "excluding an already-attempted atom_id must remove it from the next page — this is \
         what makes the operator loop terminate"
    );

    // Re-processing an already-fixed row (e.g. a re-run that does not
    // carry forward the previous run's exclusion list) must be a safe,
    // idempotent no-op: same key, same content, no duplicate atom.
    let re_outcome =
        process_candidate(&pool, &r2, org_id.as_deref(), &tenant, &dek_cache, cand, true).await;
    let mut retotals = Totals::default();
    retotals.apply(&re_outcome);
    assert_eq!(
        retotals.skipped_existing, 1,
        "re-processing an already-fixed row must be skipped"
    );
    assert_eq!(
        retotals.put_ok, 0,
        "re-processing must never overwrite an already-fixed row"
    );
    let atom_count_after_reprocess: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM atoms WHERE graph_id = $1")
            .bind(target_graph.id)
            .fetch_one(&pool)
            .await
            .expect("count target atoms");
    assert_eq!(atom_count_after_reprocess, 1, "re-processing must never duplicate the atom row");
    assert_eq!(backfill_jobs(&pool, account_id).await, after_jobs,
        "the repaired-row no-op must not add receipts; this is not a global queue dedup policy");

    // ── Ambiguous source: a commons atom reachable from promotions out of
    // TWO different member oceans, both carrying a matching (source_type,
    // source_id) atom — `candidate_source_graphs` must list both, and
    // `process_candidate` must refuse to guess (item 5). ─────────────────────
    let source_graph_b = store
        .create_graph(account_id, "backfill-source-b", "personal")
        .await
        .expect("create second source graph");
    let shared_source_id = format!("ambiguous-{}", Uuid::new_v4());

    // A real atom in EACH source graph, sharing the same (source_type,
    // source_id) — real content isn't load-bearing for this scenario (it
    // must never be read at all: resolution refuses before any GET), but
    // real rows are what makes `candidate_source_graphs` genuinely resolve
    // to two matches rather than zero.
    let source_a_atom_id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO atoms (atom_id, graph_id, source_type, source_id, content_blob_key, \
                             participants, timestamp) \
         VALUES ($1, $2, 'test', $3, 'unused-a', '{}', now())",
    )
    .bind(source_a_atom_id)
    .bind(source_graph.id)
    .bind(&shared_source_id)
    .execute(&pool)
    .await
    .expect("insert first candidate source atom");
    let source_b_atom_id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO atoms (atom_id, graph_id, source_type, source_id, content_blob_key, \
                             participants, timestamp) \
         VALUES ($1, $2, 'test', $3, 'unused-b', '{}', now())",
    )
    .bind(source_b_atom_id)
    .bind(source_graph_b.id)
    .bind(&shared_source_id)
    .execute(&pool)
    .await
    .expect("insert second candidate source atom");

    let ambiguous_atom_id = Uuid::new_v4();
    let ambiguous_broken_key = format!("{}/{}", target_graph.id, ambiguous_atom_id);
    sqlx::query(
        "INSERT INTO atoms (atom_id, graph_id, source_type, source_id, content_blob_key, \
                             participants, timestamp) \
         VALUES ($1, $2, 'test', $3, $4, '{}', now())",
    )
    .bind(ambiguous_atom_id)
    .bind(target_graph.id)
    .bind(&shared_source_id)
    .bind(&ambiguous_broken_key)
    .execute(&pool)
    .await
    .expect("insert ambiguous promoted-copy atom row");

    // Linked to TWO promoted nodes, one per source graph.
    let node_a: Uuid = sqlx::query_scalar(
        "INSERT INTO nodes (graph_id, type, title, promoted_from_graph_id) \
         VALUES ($1, 'entity', 'Ambiguous Reef A', $2) RETURNING node_id",
    )
    .bind(target_graph.id)
    .bind(source_graph.id)
    .fetch_one(&pool)
    .await
    .expect("insert first promoted node");
    let node_b: Uuid = sqlx::query_scalar(
        "INSERT INTO nodes (graph_id, type, title, promoted_from_graph_id) \
         VALUES ($1, 'entity', 'Ambiguous Reef B', $2) RETURNING node_id",
    )
    .bind(target_graph.id)
    .bind(source_graph_b.id)
    .fetch_one(&pool)
    .await
    .expect("insert second promoted node");
    for node_id in [node_a, node_b] {
        sqlx::query("INSERT INTO node_atoms (node_id, atom_id) VALUES ($1, $2)")
            .bind(node_id)
            .bind(ambiguous_atom_id)
            .execute(&pool)
            .await
            .expect("link ambiguous atom to a promoted node");
    }

    let ambiguous_candidates = list_candidates(&pool, Some(target_graph.id), &[])
        .await
        .expect("list_candidates for the ambiguous scenario");
    let ambiguous_cand = ambiguous_candidates
        .iter()
        .find(|c| c.atom_id == ambiguous_atom_id)
        .expect("the ambiguous atom must still be selected as a candidate");
    let mut graphs = ambiguous_cand.candidate_source_graphs.clone();
    graphs.sort();
    let mut expected_graphs = vec![source_graph.id, source_graph_b.id];
    expected_graphs.sort();
    assert_eq!(
        graphs, expected_graphs,
        "candidate_source_graphs must list BOTH promoted-from graphs, not pin to one"
    );

    let ambiguous_outcome =
        process_candidate(&pool, &r2, org_id.as_deref(), &tenant, &dek_cache, ambiguous_cand, true)
            .await;
    let mut ambiguous_totals = Totals::default();
    ambiguous_totals.apply(&ambiguous_outcome);
    let ambiguous_report = &ambiguous_outcome.report;
    assert_eq!(ambiguous_totals.ambiguous_source, 1, "{ambiguous_report}");
    assert_eq!(ambiguous_totals.errors, 1, "an ambiguous source must count as an error, not a silent skip");
    assert_eq!(ambiguous_totals.put_ok, 0, "an ambiguous source must never be guessed and written");
    assert!(
        ambiguous_report.to_uppercase().contains("AMBIGUOUS"),
        "{ambiguous_report}"
    );
    let ambiguous_key_unchanged: String =
        sqlx::query_scalar("SELECT content_blob_key FROM atoms WHERE atom_id = $1")
            .bind(ambiguous_atom_id)
            .fetch_one(&pool)
            .await
            .expect("ambiguous row still exists");
    assert_eq!(
        ambiguous_key_unchanged, ambiguous_broken_key,
        "an ambiguous candidate's row must never be touched"
    );

    sqlx::query("DELETE FROM accounts WHERE id = $1").bind(account_id).execute(&pool).await.ok();
    sqlx::query("DELETE FROM users WHERE id = $1").bind(user_id).execute(&pool).await.ok();
}

/// Shared fixture for the three tests below: `user -> account(dek_wrapped)
/// -> {source, target}` graphs, plus a real R2 client and the connected
/// pool. Returns `(pool, r2, dek, account_id, user_id, source_graph_id,
/// target_graph_id)`.
async fn setup_tenant_with_graphs(
    db_url: &str,
    r2env: &R2TestEnv,
    name_prefix: &str,
) -> (sqlx::PgPool, R2Client, [u8; 32], Uuid, Uuid, Uuid, Uuid) {
    set_test_kek();
    let kek = load_kek().expect("test KEK loads");
    let dek = generate_dek();
    let wrapped = wrap_dek(&kek, &dek).expect("wrap test DEK");

    let pool = PgPoolOptions::new().max_connections(4).connect(db_url).await.expect("connect");
    let store = PostgresStore::new(pool.clone());

    let email = format!("{}@test.local", Uuid::new_v4());
    let user_id: Uuid = sqlx::query_scalar("INSERT INTO users (email) VALUES ($1) RETURNING id")
        .bind(&email)
        .fetch_one(&pool)
        .await
        .expect("insert user");
    let account_id: Uuid = sqlx::query_scalar(
        "INSERT INTO accounts (owner_user_id, dek_wrapped) VALUES ($1, $2) RETURNING id",
    )
    .bind(user_id)
    .bind(&wrapped)
    .fetch_one(&pool)
    .await
    .expect("insert account with wrapped DEK");

    let source_graph = store
        .create_graph(account_id, &format!("{name_prefix}-source"), "personal")
        .await
        .expect("create source graph");
    let target_graph = store
        .create_graph(account_id, &format!("{name_prefix}-target"), "team")
        .await
        .expect("create target graph");

    let r2 = R2Client::new_with_endpoint_override(
        "unused-account-id",
        &r2env.access_key,
        &r2env.secret_key,
        &r2env.bucket,
        &r2env.endpoint,
    )
    .await;

    (pool, r2, dek, account_id, user_id, source_graph.id, target_graph.id)
}

/// Insert ONE atom row with an EXPLICIT `content_blob_key` — including the
/// blank (`''`) key `CANDIDATES_SQL`'s arm 2 selects, which
/// `seed_broken_candidate`'s hand-built `{graph}/{atom}` placeholder cannot
/// express. Takes the `atom_id` from the caller because a placeholder key
/// has to embed it.
async fn insert_atom_with_key(
    pool: &sqlx::PgPool,
    atom_id: Uuid,
    graph_id: Uuid,
    source_id: &str,
    content_blob_key: &str,
    content_inline: Option<&str>,
) {
    sqlx::query(
        "INSERT INTO atoms (atom_id, graph_id, source_type, source_id, content_blob_key, \
                             content_inline, participants, timestamp) \
         VALUES ($1, $2, 'test', $3, $4, $5, '{}', now())",
    )
    .bind(atom_id)
    .bind(graph_id)
    .bind(source_id)
    .bind(content_blob_key)
    .bind(content_inline)
    .execute(pool)
    .await
    .expect("insert atom row");
}

/// Link `atom_id` to a NEW node in `graph_id`. `promoted_from = Some(g)`
/// gives that node the `promoted_from_graph_id` provenance BOTH candidate
/// arms require; `None` makes it a plain, directly-ingested node — the
/// negative control that proves arm 2 narrows by provenance rather than
/// selecting every blank key it can see.
async fn link_to_new_node(
    pool: &sqlx::PgPool,
    graph_id: Uuid,
    atom_id: Uuid,
    promoted_from: Option<Uuid>,
) -> Uuid {
    let node_id: Uuid = sqlx::query_scalar(
        "INSERT INTO nodes (graph_id, type, title, promoted_from_graph_id) \
         VALUES ($1, 'entity', $2, $3) RETURNING node_id",
    )
    .bind(graph_id)
    .bind(format!("Reef for {atom_id}"))
    .bind(promoted_from)
    .fetch_one(pool)
    .await
    .expect("insert node");
    sqlx::query("INSERT INTO node_atoms (node_id, atom_id) VALUES ($1, $2)")
        .bind(node_id)
        .bind(atom_id)
        .execute(pool)
        .await
        .expect("link atom to node");
    node_id
}

/// Seed a broken promoted-copy candidate row in `target_graph` — arm 1's
/// shape: the INTACT pre-fix placeholder key — linked to a promoted node
/// pointing at `source_graph`, with the given `content_inline`. Returns
/// `(candidate_atom_id, broken_key)`.
async fn seed_broken_candidate(
    pool: &sqlx::PgPool,
    source_graph: Uuid,
    target_graph: Uuid,
    source_id: &str,
    content_inline: Option<&str>,
) -> (Uuid, String) {
    let atom_id = Uuid::new_v4();
    let broken_key = format!("{target_graph}/{atom_id}");
    insert_atom_with_key(pool, atom_id, target_graph, source_id, &broken_key, content_inline).await;
    link_to_new_node(pool, target_graph, atom_id, Some(source_graph)).await;
    (atom_id, broken_key)
}

/// Seed a BLANK-key candidate row — the exact shape
/// `workers::atom_title_backfill`'s `TERMINAL_UNFETCHABLE_SQL` leaves
/// behind (`content_blob_key = ''`, `title` downgraded to the "Archived
/// coral (content unavailable)" fallback) — linked to a promoted node
/// pointing at `source_graph`. This is `CANDIDATES_SQL` arm 2's row.
async fn seed_blank_key_candidate(
    pool: &sqlx::PgPool,
    source_graph: Uuid,
    target_graph: Uuid,
    source_id: &str,
) -> Uuid {
    let atom_id = Uuid::new_v4();
    insert_atom_with_key(pool, atom_id, target_graph, source_id, "", None).await;
    sqlx::query(
        "UPDATE atoms SET title = 'Archived coral (content unavailable)', \
                          title_kind = 'fallback' WHERE atom_id = $1",
    )
    .bind(atom_id)
    .execute(pool)
    .await
    .expect("stamp the sweep's fallback title");
    link_to_new_node(pool, target_graph, atom_id, Some(source_graph)).await;

    // Belt and braces: prove the row really is blank in the database before
    // any test draws a conclusion from it being (or not being) selected.
    let seeded: String = sqlx::query_scalar("SELECT content_blob_key FROM atoms WHERE atom_id = $1")
        .bind(atom_id)
        .fetch_one(pool)
        .await
        .expect("seeded row exists");
    assert_eq!(seeded, "", "sanity: the seeded candidate must carry the BLANK key arm 2 selects");
    atom_id
}

/// Seed a source atom in `source_graph` whose `content_blob_key` names a
/// REAL object (unlike [`seed_source_with_phantom_key`]). Returns the
/// plaintext that object holds, so a test can assert the repaired target
/// resolves to exactly it.
async fn seed_source_with_real_content(
    pool: &sqlx::PgPool,
    r2: &R2Client,
    dek: &[u8; 32],
    source_graph: Uuid,
    source_id: &str,
) -> String {
    let atom_id = Uuid::new_v4();
    let key = format!("{source_graph}/{atom_id}");
    insert_atom_with_key(pool, atom_id, source_graph, source_id, &key, None).await;
    let plaintext = format!("real source content for {source_id}");
    r2.put_atom_blob_at(&key, plaintext.as_bytes(), dek)
        .await
        .expect("seed real source content in the R2 fake");
    plaintext
}

/// Seed a source atom in `source_graph` with a `content_blob_key` that
/// names an object NEVER actually written to R2 — a real, shape-valid key
/// (`validate_blob_key` accepts it), just phantom, exactly the pre-cutover
/// prod shape the 2026-09-09 dry run found. Returns the `source_id` string
/// the candidate row must reuse (the dedup/identity key).
async fn seed_source_with_phantom_key(
    pool: &sqlx::PgPool,
    source_graph: Uuid,
    source_id: &str,
) -> String {
    let atom_id = Uuid::new_v4();
    let phantom_key = format!("{source_graph}/{atom_id}");
    sqlx::query(
        "INSERT INTO atoms (atom_id, graph_id, source_type, source_id, content_blob_key, \
                             participants, timestamp) \
         VALUES ($1, $2, 'test', $3, $4, '{}', now())",
    )
    .bind(atom_id)
    .bind(source_graph)
    .bind(source_id)
    .bind(&phantom_key)
    .execute(pool)
    .await
    .expect("insert source atom with a phantom (never-uploaded) key");
    source_id.to_string()
}

// ═══════════════════════════════════════════════════════════════════════════
// Item 6a: source object missing + candidate has content_inline →
// skipped_inline_only, never an error, never put_failed — exit-0 semantics.
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn source_missing_with_inline_fallback_is_skipped_not_failed() {
    let Ok(db_url) = std::env::var("RELINFER_TEST_DATABASE_URL") else {
        eprintln!("{SKIP_REASON}");
        return;
    };
    let Some(r2env) = r2_test_env() else {
        eprintln!("{SKIP_REASON}");
        return;
    };

    let (pool, r2, _dek, account_id, user_id, source_graph, target_graph) =
        setup_tenant_with_graphs(&db_url, &r2env, "src-missing-inline").await;

    let source_id = format!("phantom-{}", Uuid::new_v4());
    seed_source_with_phantom_key(&pool, source_graph, &source_id).await;
    let (cand_atom_id, broken_key) = seed_broken_candidate(
        &pool,
        source_graph,
        target_graph,
        &source_id,
        Some("cached inline copy — readable even though the source blob is gone"),
    )
    .await;

    let candidates = list_candidates(&pool, Some(target_graph), &[]).await.expect("list_candidates");
    let cand = candidates.iter().find(|c| c.atom_id == cand_atom_id).expect("candidate selected");
    assert!(cand.has_inline, "the candidate row must carry content_inline");

    let tenant = TenantId::Consumer;
    let dek_cache = tokio::sync::Mutex::new(std::collections::HashMap::new());
    let org_id: Option<&str> = None;
    let outcome = process_candidate(&pool, &r2, org_id, &tenant, &dek_cache, cand, true).await;

    assert_eq!(outcome.kind, OutcomeKind::SkippedInlineOnly, "{}", outcome.report);
    assert!(outcome.report.contains("skipped_inline_only"), "{}", outcome.report);

    let mut totals = Totals::default();
    totals.apply(&outcome);
    assert_eq!(totals.skipped_inline_only, 1);
    assert_eq!(totals.errors, 0);
    assert_eq!(totals.put_failed, 0);
    assert!(totals.is_clean(), "skipped_inline_only must never flip the exit code: {totals}");

    let unchanged: String = sqlx::query_scalar("SELECT content_blob_key FROM atoms WHERE atom_id = $1")
        .bind(cand_atom_id)
        .fetch_one(&pool)
        .await
        .expect("row still exists");
    assert_eq!(unchanged, broken_key, "an unrepairable-but-readable row must never be touched");

    sqlx::query("DELETE FROM accounts WHERE id = $1").bind(account_id).execute(&pool).await.ok();
    sqlx::query("DELETE FROM users WHERE id = $1").bind(user_id).execute(&pool).await.ok();
}

// ═══════════════════════════════════════════════════════════════════════════
// Item 6b: source object missing + candidate has NO content_inline →
// unrepairable_source_missing, printed loudly, still exit-0.
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn source_missing_with_no_inline_fallback_is_unrepairable() {
    let Ok(db_url) = std::env::var("RELINFER_TEST_DATABASE_URL") else {
        eprintln!("{SKIP_REASON}");
        return;
    };
    let Some(r2env) = r2_test_env() else {
        eprintln!("{SKIP_REASON}");
        return;
    };

    let (pool, r2, _dek, account_id, user_id, source_graph, target_graph) =
        setup_tenant_with_graphs(&db_url, &r2env, "src-missing-unrepairable").await;

    let source_id = format!("phantom-{}", Uuid::new_v4());
    seed_source_with_phantom_key(&pool, source_graph, &source_id).await;
    let (cand_atom_id, broken_key) =
        seed_broken_candidate(&pool, source_graph, target_graph, &source_id, None).await;

    let candidates = list_candidates(&pool, Some(target_graph), &[]).await.expect("list_candidates");
    let cand = candidates.iter().find(|c| c.atom_id == cand_atom_id).expect("candidate selected");
    assert!(!cand.has_inline, "this candidate must have NO content_inline fallback");

    let tenant = TenantId::Consumer;
    let dek_cache = tokio::sync::Mutex::new(std::collections::HashMap::new());
    let org_id: Option<&str> = None;
    let outcome = process_candidate(&pool, &r2, org_id, &tenant, &dek_cache, cand, true).await;

    assert_eq!(outcome.kind, OutcomeKind::UnrepairableSourceMissing, "{}", outcome.report);
    assert!(outcome.report.contains("UNREPAIRABLE"), "{}", outcome.report);
    assert!(
        outcome.report.contains(&cand_atom_id.to_string()),
        "the report must print the atom id so an operator can follow up: {}",
        outcome.report
    );

    let mut totals = Totals::default();
    totals.apply(&outcome);
    assert_eq!(totals.unrepairable_source_missing, 1);
    assert_eq!(totals.errors, 0, "unrepairable is not a transient/retryable condition");
    assert_eq!(totals.put_failed, 0, "unrepairable is not a write-path failure");
    assert!(
        totals.is_clean(),
        "an unrepairable row must NEVER flip the exit code (re-running cannot fix it): {totals}"
    );
    assert!(totals.summary_line().contains("UNREPAIRABLE"), "{}", totals.summary_line());

    let unchanged: String = sqlx::query_scalar("SELECT content_blob_key FROM atoms WHERE atom_id = $1")
        .bind(cand_atom_id)
        .fetch_one(&pool)
        .await
        .expect("row still exists");
    assert_eq!(unchanged, broken_key, "an unrepairable row must never be touched");

    sqlx::query("DELETE FROM accounts WHERE id = $1").bind(account_id).execute(&pool).await.ok();
    sqlx::query("DELETE FROM users WHERE id = $1").bind(user_id).execute(&pool).await.ok();
}

// ═══════════════════════════════════════════════════════════════════════════
// Item 6c: a batch of MORE than BLOB_IO_CONCURRENCY candidates, all
// repaired concurrently with correct, non-cross-contaminated keys/content
// and correctly-folded tallies.
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn concurrent_batch_all_repaired_with_correct_keys_and_tallies() {
    let Ok(db_url) = std::env::var("RELINFER_TEST_DATABASE_URL") else {
        eprintln!("{SKIP_REASON}");
        return;
    };
    let Some(r2env) = r2_test_env() else {
        eprintln!("{SKIP_REASON}");
        return;
    };

    let (pool, r2, dek, account_id, user_id, source_graph, target_graph) =
        setup_tenant_with_graphs(&db_url, &r2env, "concurrent-batch").await;

    // Strictly MORE than BLOB_IO_CONCURRENCY, so this batch genuinely
    // exercises `buffer_unordered`'s bound rather than fitting in one
    // "concurrency window".
    let n = BLOB_IO_CONCURRENCY + 4;
    let mut expected: Vec<(Uuid, String)> = Vec::new(); // (atom_id, plaintext)
    for i in 0..n {
        let source_id = format!("concurrent-{i}-{}", Uuid::new_v4());
        let source_atom_id = Uuid::new_v4();
        let source_key = format!("{source_graph}/{source_atom_id}");
        sqlx::query(
            "INSERT INTO atoms (atom_id, graph_id, source_type, source_id, content_blob_key, \
                                 participants, timestamp) \
             VALUES ($1, $2, 'test', $3, $4, '{}', now())",
        )
        .bind(source_atom_id)
        .bind(source_graph)
        .bind(&source_id)
        .bind(&source_key)
        .execute(&pool)
        .await
        .expect("insert source atom");
        let plaintext = format!("concurrent content #{i}");
        r2.put_atom_blob_at(&source_key, plaintext.as_bytes(), &dek)
            .await
            .expect("seed real source content");

        let (cand_atom_id, _broken_key) =
            seed_broken_candidate(&pool, source_graph, target_graph, &source_id, None).await;
        expected.push((cand_atom_id, plaintext));
    }

    let candidates = list_candidates(&pool, Some(target_graph), &[]).await.expect("list_candidates");
    assert_eq!(candidates.len(), n, "every seeded candidate must be selected");

    let tenant = TenantId::Consumer;
    let dek_cache = tokio::sync::Mutex::new(std::collections::HashMap::new());
    let org_id: Option<&str> = None;
    let outcomes = futures::stream::iter(candidates.iter())
        .map(|cand| process_candidate(&pool, &r2, org_id, &tenant, &dek_cache, cand, true))
        .buffer_unordered(BLOB_IO_CONCURRENCY)
        .collect::<Vec<_>>()
        .await;
    assert_eq!(outcomes.len(), n);

    let mut totals = Totals::default();
    for outcome in &outcomes {
        totals.apply(outcome);
    }
    assert_eq!(totals.put_ok, n as u64, "{totals}");
    assert_eq!(totals.errors, 0, "{totals}");
    assert_eq!(totals.put_failed, 0, "{totals}");
    assert!(totals.is_clean());

    // No cross-contamination: every candidate's row now carries ITS OWN
    // correct key, and that key resolves to ITS OWN content — proves the
    // shared `dek_cache` mutex and running these concurrently never mixed
    // up which content went where.
    for (atom_id, plaintext) in &expected {
        let key: String = sqlx::query_scalar("SELECT content_blob_key FROM atoms WHERE atom_id = $1")
            .bind(atom_id)
            .fetch_one(&pool)
            .await
            .expect("row exists");
        let expected_key = blob_key_for(None, target_graph, *atom_id).expect("valid key");
        coralswarm_core::r2::validate_blob_key(&key).unwrap();
        assert!(key.starts_with(&format!("{target_graph}/")));
        assert_ne!(key, expected_key);
        let content = r2
            .get_atom_blob(&key, &dek)
            .await
            .expect("content readable");
        assert_eq!(&String::from_utf8_lossy(&content), plaintext);
    }

    sqlx::query("DELETE FROM accounts WHERE id = $1").bind(account_id).execute(&pool).await.ok();
    sqlx::query("DELETE FROM users WHERE id = $1").bind(user_id).execute(&pool).await.ok();
}

// ── The RLS-bypass preflight, and its own negative control ──────────────────
//
// Modeled directly on judgement_backfill_pg.rs's
// `preflight_passes_for_the_elevated_role` /
// `preflight_refuses_a_role_that_cannot_bypass_rls` pair (see
// `scripts/rls_coverage.manifest`'s entry for
// `backend/src/promoted_blob_backfill.rs`, which cites this test as its
// negative control). Without the second test, `assert_policy_bypass` could
// be a function that always returns `Ok` and nothing here would notice.

/// The preflight must PASS for the elevated role every other test in this
/// file uses (the plain `RELINFER_TEST_DATABASE_URL` connection, which owns
/// `atoms` and does not force row-level security).
#[tokio::test]
async fn preflight_passes_for_the_elevated_role() {
    let Ok(db_url) = std::env::var("RELINFER_TEST_DATABASE_URL") else {
        eprintln!("skipping preflight_passes_for_the_elevated_role: {SKIP_REASON}");
        return;
    };
    let pool = PgPoolOptions::new().max_connections(2).connect(&db_url).await.expect("connect");
    assert_policy_bypass(&pool)
        .await
        .expect("the test DSN's role must be able to write atoms unscoped");
}

/// …and REFUSE for one that cannot bypass RLS. Uses a real, throwaway
/// `NOBYPASSRLS` role and `SET ROLE` on every pooled connection
/// (`after_connect`) rather than a single ad-hoc `SET`, because a pool hands
/// out whichever connection it likes — same technique as
/// `judgement_backfill_pg.rs`'s matching test.
#[tokio::test]
async fn preflight_refuses_a_role_that_cannot_bypass_rls() {
    let Ok(db_url) = std::env::var("RELINFER_TEST_DATABASE_URL") else {
        eprintln!("skipping preflight_refuses_a_role_that_cannot_bypass_rls: {SKIP_REASON}");
        return;
    };
    let pool = PgPoolOptions::new().max_connections(2).connect(&db_url).await.expect("connect");

    // Roles are CLUSTER-global: unique per run, and dropped afterwards.
    let role = coralswarm_backend::test_db::TestRole::from_url(&db_url, "pbb_norls");
    sqlx::query(&format!(
        "CREATE ROLE \"{role}\" NOLOGIN NOSUPERUSER NOBYPASSRLS NOCREATEDB NOCREATEROLE"
    ))
    .execute(&pool)
    .await
    .expect("create the throwaway role (needs a superuser/CREATEROLE test DSN)");

    let role_for_connect = role.to_string();
    let restricted = PgPoolOptions::new()
        .max_connections(1)
        .after_connect(move |conn, _meta| {
            let role = role_for_connect.clone();
            Box::pin(async move {
                sqlx::query(&format!("SET ROLE \"{role}\""))
                    .execute(&mut *conn)
                    .await?;
                Ok(())
            })
        })
        .connect(&db_url)
        .await
        .expect("connect as the restricted role");

    let outcome = assert_policy_bypass(&restricted).await;
    restricted.close().await;
    sqlx::query(&format!("DROP ROLE IF EXISTS \"{role}\""))
        .execute(&pool)
        .await
        .expect("drop the throwaway role");

    match outcome {
        Err(msg) => {
            assert!(
                msg.contains("does not bypass row-level security"),
                "unexpected refusal message: {msg}"
            );
        }
        Ok(()) => panic!(
            "the preflight accepted a role that cannot bypass RLS on `atoms` — it is vacuous"
        ),
    }
}

/// Proves `find_source_atom`'s account-boundary join (CodeRabbit PR #838
/// review, item 3): a `promoted_from_graph_id` pointing at a graph owned by
/// a DIFFERENT account — historical cross-account promotion data predating
/// whatever guard now prevents it — must never hand back that OTHER
/// account's `content_blob_key`. Plants a matching `(source_type,
/// source_id)` atom in the OTHER account's graph — exactly the row
/// `find_source_atom` would wrongly return without the `g.account_id =
/// cand.account_id` predicate — and asserts the candidate resolves to the
/// SAME harmless `SourceNotFound` a genuine no-match would, never a
/// distinct error and never a flipped exit code.
#[tokio::test]
async fn cross_account_source_graph_is_never_matched_pg() {
    let Ok(db_url) = std::env::var("RELINFER_TEST_DATABASE_URL") else {
        eprintln!("{SKIP_REASON}");
        return;
    };
    let Some(r2env) = r2_test_env() else {
        eprintln!("{SKIP_REASON}");
        return;
    };

    let (pool, r2, _dek, account_id, user_id, _source_graph, target_graph) =
        setup_tenant_with_graphs(&db_url, &r2env, "cross-account").await;
    let store = PostgresStore::new(pool.clone());

    // A SECOND, unrelated account/user/graph — `evil_graph` is NOT this
    // candidate's own account, standing in for stale cross-account
    // promotion data.
    let evil_email = format!("{}@test.local", Uuid::new_v4());
    let evil_user_id: Uuid = sqlx::query_scalar("INSERT INTO users (email) VALUES ($1) RETURNING id")
        .bind(&evil_email)
        .fetch_one(&pool)
        .await
        .expect("insert evil user");
    let evil_account_id: Uuid = sqlx::query_scalar(
        "INSERT INTO accounts (owner_user_id, dek_wrapped) VALUES ($1, NULL) RETURNING id",
    )
    .bind(evil_user_id)
    .fetch_one(&pool)
    .await
    .expect("insert evil account");
    let evil_graph = store
        .create_graph(evil_account_id, "cross-account-evil-source", "personal")
        .await
        .expect("create evil-account source graph");

    let source_id = format!("cross-account-{}", Uuid::new_v4());
    let (cand_atom_id, broken_key) =
        seed_broken_candidate(&pool, evil_graph.id, target_graph, &source_id, None).await;
    // The row an account-boundary-blind `find_source_atom` would wrongly
    // match and hand back.
    seed_source_with_phantom_key(&pool, evil_graph.id, &source_id).await;

    let candidates = list_candidates(&pool, Some(target_graph), &[]).await.expect("list candidates");
    let cand = candidates
        .iter()
        .find(|c| c.atom_id == cand_atom_id)
        .expect("seeded candidate found by the candidate query");
    assert_eq!(
        cand.candidate_source_graphs,
        vec![evil_graph.id],
        "sanity: the ONLY promoted_from_graph_id this candidate names is the other account's graph"
    );

    let tenant = TenantId::Consumer;
    let dek_cache = tokio::sync::Mutex::new(std::collections::HashMap::new());
    let outcome = process_candidate(&pool, &r2, None, &tenant, &dek_cache, cand, false).await;

    assert_eq!(
        outcome.kind,
        OutcomeKind::SourceNotFound,
        "a cross-account match must be as invisible to find_source_atom as no match at all: {}",
        outcome.report
    );
    let mut totals = Totals::default();
    totals.apply(&outcome);
    assert_eq!(totals.source_not_found, 1);
    assert!(totals.is_clean(), "a cross-account source must never flip the exit code");

    let key_unchanged: String =
        sqlx::query_scalar("SELECT content_blob_key FROM atoms WHERE atom_id = $1")
            .bind(cand_atom_id)
            .fetch_one(&pool)
            .await
            .expect("candidate row still exists");
    assert_eq!(
        key_unchanged, broken_key,
        "a source-not-found candidate's row must never be touched"
    );

    sqlx::query("DELETE FROM accounts WHERE id = $1").bind(account_id).execute(&pool).await.ok();
    sqlx::query("DELETE FROM users WHERE id = $1").bind(user_id).execute(&pool).await.ok();
    sqlx::query("DELETE FROM accounts WHERE id = $1")
        .bind(evil_account_id)
        .execute(&pool)
        .await
        .ok();
    sqlx::query("DELETE FROM users WHERE id = $1").bind(evil_user_id).execute(&pool).await.ok();
}

// ═══════════════════════════════════════════════════════════════════════════
// `CANDIDATES_SQL` arm 2 — the ERASED key (`content_blob_key = ''`).
//
// `workers::atom_title_backfill`'s `TERMINAL_UNFETCHABLE_SQL` blanks exactly
// the marker arm 1 selects on, and on the org plane it did so to 1,191 of
// tenant `coralswarm`'s 3,698 broken rows ~35 minutes before this backfill
// first ran — so those rows were INVISIBLE to the repair that exists for
// them. Arm 2 selects them by promotion PROVENANCE instead. The three tests
// below are the behavioural proof of that arm: it selects and repairs, it
// does not over-select, and the mirror-image case (a blank key on the SOURCE
// side) stays classified as unrepairable rather than repaired or errored.
// ═══════════════════════════════════════════════════════════════════════════

/// A blank-key atom in a team graph, linked to a promoted node, IS selected
/// and IS repaired end to end: the object lands at the `blob_key_for` key,
/// the row is corrected, and Unsupported derivation/digest receipts are
/// recorded without running them. Embedding metadata remains queued.
///
/// **This test fails if arm 2 is reverted** — `list_candidates` returns
/// nothing for the seeded row and the `.expect` below panics with that
/// message. It is deliberately not phrased as "the run exited 0".
#[tokio::test]
async fn blank_key_candidate_is_selected_and_repaired_end_to_end() {
    let Ok(db_url) = std::env::var("RELINFER_TEST_DATABASE_URL") else {
        eprintln!("{SKIP_REASON}");
        return;
    };
    let Some(r2env) = r2_test_env() else {
        eprintln!("{SKIP_REASON}");
        return;
    };

    let (pool, r2, dek, account_id, user_id, source_graph, target_graph) =
        setup_tenant_with_graphs(&db_url, &r2env, "blank-key-repair").await;

    // The source still has its own real, fetchable content — the whole
    // reason a blanked target row is repairable at all. `find_source_atom`
    // resolves it by `(source_type, source_id)`, never by the candidate's
    // own (now blank) key.
    let source_id = format!("blanked-{}", Uuid::new_v4());
    let plaintext = seed_source_with_real_content(&pool, &r2, &dek, source_graph, &source_id).await;
    let cand_atom_id = seed_blank_key_candidate(&pool, source_graph, target_graph, &source_id).await;

    // Prod's actual state for these rows: derivation jobs already `dead`
    // (2,211 dead `RegenerateDigest`s across the graph) and nothing pending,
    // so nothing self-heals without this run.
    sqlx::query(
        "INSERT INTO jobs (kind, payload, account_id, idempotency_key, status) \
         VALUES ('ExtractEntities', $1, $2, $3, 'dead')",
    )
    .bind(serde_json::json!({"atom_id": cand_atom_id}))
    .bind(account_id)
    .bind(format!("extract:{cand_atom_id}"))
    .execute(&pool)
    .await
    .expect("seed a dead ExtractEntities job");

    let candidates = list_candidates(&pool, Some(target_graph), &[]).await.expect("list_candidates");
    let cand = candidates.iter().find(|c| c.atom_id == cand_atom_id).expect(
        "a BLANK-key atom in a team graph linked to a promoted node must be selected — this is \
         CANDIDATES_SQL arm 2; without it the 1,191 rows another worker blanked are invisible to \
         the repair that exists for them",
    );
    assert_eq!(cand.content_blob_key, "", "the candidate must carry the blank key verbatim");
    assert_eq!(
        cand.candidate_source_graphs,
        vec![source_graph],
        "provenance is what selected this row, so it must be carried through"
    );
    assert!(!cand.has_inline, "prod's blanked rows have no content_inline fallback either");

    let tenant = TenantId::Consumer;
    let dek_cache = tokio::sync::Mutex::new(std::collections::HashMap::new());
    let org_id: Option<&str> = None;

    let before_jobs = backfill_jobs(&pool, account_id).await;
    let before_effects = backfill_nonexecution_state(&pool, account_id, source_graph, target_graph).await;
    let source_key: String = sqlx::query_scalar(
        "SELECT content_blob_key FROM atoms WHERE graph_id=$1 AND source_id=$2",
    ).bind(source_graph).bind(&source_id).fetch_one(&pool).await.unwrap();
    let promoted_node: Uuid = sqlx::query_scalar(
        "SELECT n.node_id FROM nodes n JOIN node_atoms l ON l.node_id=n.node_id \
         WHERE n.graph_id=$1 AND l.atom_id=$2",
    ).bind(target_graph).bind(cand_atom_id).fetch_one(&pool).await.unwrap();

    // Dry run: every read happens (source GET, target existence check),
    // nothing is written.
    let dry = process_candidate(&pool, &r2, org_id, &tenant, &dek_cache, cand, false).await;
    assert_eq!(dry.kind, OutcomeKind::WouldRepair, "{}", dry.report);
    let still_blank: String =
        sqlx::query_scalar("SELECT content_blob_key FROM atoms WHERE atom_id = $1")
            .bind(cand_atom_id)
            .fetch_one(&pool)
            .await
            .expect("row still exists");
    assert_eq!(still_blank, "", "a dry run must never touch the row");
    assert_eq!(backfill_jobs(&pool, account_id).await, before_jobs);
    assert_eq!(backfill_nonexecution_state(&pool, account_id, source_graph, target_graph).await, before_effects);

    // Apply.
    let outcome = process_candidate(&pool, &r2, org_id, &tenant, &dek_cache, cand, true).await;
    let report = outcome.report.clone();
    let mut totals = Totals::default();
    totals.apply(&outcome);
    assert_eq!(outcome.kind, OutcomeKind::PutOk, "{report}");
    assert_eq!(totals.put_ok, 1, "{report}");
    assert_eq!(totals.errors, 0, "{report}");
    assert_eq!(totals.put_failed, 0, "{report}");
    assert_eq!(totals.dead_jobs_seen, 1, "the seeded dead job is counted, never resurrected");
    assert!(totals.jobs_reenqueued >= 1, "1 digest job expected: {report}");
    assert!(
        report.contains("<blank>"),
        "the report line must render the old blank key explicitly, not as a gap: {report}"
    );

    // The row now names the key `blob_key_for` computes, and a real object
    // is actually there holding the SOURCE's content.
    let fixed_key: String =
        sqlx::query_scalar("SELECT content_blob_key FROM atoms WHERE atom_id = $1")
            .bind(cand_atom_id)
            .fetch_one(&pool)
            .await
            .expect("row still exists");
    let expected_key = blob_key_for(None, target_graph, cand_atom_id).expect("valid key");
    coralswarm_core::r2::validate_blob_key(&fixed_key).unwrap();
    assert!(fixed_key.starts_with(&format!("{target_graph}/")));
    assert_ne!(fixed_key, expected_key, "repair must use a fresh key");
    assert_ne!(fixed_key, "", "…and must no longer be blank");
    let content = r2.get_atom_blob(&fixed_key, &dek).await.expect("object lands at the target key");
    assert_eq!(String::from_utf8_lossy(&content), plaintext);

    // Exact current contract: three new Unsupported terminal receipts,
    // plus one pending embedding metadata job; no worker is invoked here.
    let after_jobs = assert_backfill_receipts(
        &pool, account_id, target_graph, cand_atom_id, promoted_node,
        &before_jobs, totals.jobs_reenqueued,
    ).await;
    assert_eq!(backfill_nonexecution_state(&pool, account_id, source_graph, target_graph).await, before_effects);
    let original_bytes = r2.get_atom_blob(&source_key, &dek).await.expect("private source blob remains readable");
    assert_eq!(String::from_utf8_lossy(&original_bytes), plaintext, "repair must preserve source blob bytes");

    // The sweep's fallback title is left alone on purpose (this backfill
    // repairs CONTENT, not titles) — and that is safe, because a non-blank
    // `content_blob_key` is exactly what puts the row back inside
    // `atom_title_backfill::CANDIDATE_SQL`'s predicate (`title_kind =
    // 'fallback' AND content_blob_key <> ''`), so the sweep can upgrade the
    // title on its next pass.
    let (title, title_kind): (Option<String>, Option<String>) =
        sqlx::query_as("SELECT title, title_kind FROM atoms WHERE atom_id = $1")
            .bind(cand_atom_id)
            .fetch_one(&pool)
            .await
            .expect("row still exists");
    assert_eq!(title.as_deref(), Some("Archived coral (content unavailable)"));
    assert_eq!(
        title_kind.as_deref(),
        Some("fallback"),
        "the row must stay title-sweep-eligible after the content repair"
    );

    let unexcluded = list_candidates(&pool, Some(target_graph), &[])
        .await
        .expect("re-query");
    assert!(
        !unexcluded.iter().any(|c| c.atom_id == cand_atom_id),
        "fresh pointers leave repair selection"
    );
    let excluded =
        list_candidates(&pool, Some(target_graph), &[cand_atom_id]).await.expect("re-query");
    assert!(
        !excluded.iter().any(|c| c.atom_id == cand_atom_id),
        "excluding an already-attempted atom_id must remove it from the next page"
    );
    let re = process_candidate(&pool, &r2, org_id, &tenant, &dek_cache, cand, true).await;
    assert_eq!(
        re.kind,
        OutcomeKind::SkippedExisting,
        "re-processing a repaired row must never overwrite the object it just wrote: {}",
        re.report
    );
    assert_eq!(backfill_jobs(&pool, account_id).await, after_jobs,
        "the repaired-row no-op must not add receipts; this is not a global queue dedup policy");

    sqlx::query("DELETE FROM accounts WHERE id = $1").bind(account_id).execute(&pool).await.ok();
    sqlx::query("DELETE FROM users WHERE id = $1").bind(user_id).execute(&pool).await.ok();
}

/// Arm 2 narrows by PROVENANCE, not by blankness: a blank-key atom in the
/// same team graph that is linked to a node WITHOUT
/// `promoted_from_graph_id` (a directly-ingested reef), or linked to no node
/// at all, must NOT be selected. Carries its own positive control in the
/// same graph, so "nothing was selected at all" cannot pass this test.
#[tokio::test]
async fn blank_key_without_promoted_provenance_is_not_selected() {
    let Ok(db_url) = std::env::var("RELINFER_TEST_DATABASE_URL") else {
        eprintln!("{SKIP_REASON}");
        return;
    };
    let Some(r2env) = r2_test_env() else {
        eprintln!("{SKIP_REASON}");
        return;
    };

    let (pool, _r2, _dek, account_id, user_id, source_graph, target_graph) =
        setup_tenant_with_graphs(&db_url, &r2env, "blank-key-narrowing").await;

    // (1) blank key, linked to a node with NO promotion provenance.
    let unpromoted_atom_id = Uuid::new_v4();
    insert_atom_with_key(
        &pool,
        unpromoted_atom_id,
        target_graph,
        &format!("direct-{}", Uuid::new_v4()),
        "",
        None,
    )
    .await;
    link_to_new_node(&pool, target_graph, unpromoted_atom_id, None).await;

    // (2) blank key, linked to nothing at all.
    let orphan_atom_id = Uuid::new_v4();
    insert_atom_with_key(
        &pool,
        orphan_atom_id,
        target_graph,
        &format!("orphan-{}", Uuid::new_v4()),
        "",
        None,
    )
    .await;

    // (3) the positive control: a blank key WITH promotion provenance.
    let promoted_atom_id = seed_blank_key_candidate(
        &pool,
        source_graph,
        target_graph,
        &format!("promoted-{}", Uuid::new_v4()),
    )
    .await;

    let selected: Vec<Uuid> = list_candidates(&pool, Some(target_graph), &[])
        .await
        .expect("list_candidates")
        .iter()
        .map(|c| c.atom_id)
        .collect();

    assert!(
        selected.contains(&promoted_atom_id),
        "positive control: the provenance-carrying blank-key row MUST still be selected — if \
         this fails the query is broken, not merely narrow (selected = {selected:?})"
    );
    assert!(
        !selected.contains(&unpromoted_atom_id),
        "a blank key on a directly-ingested atom must NOT be selected: arm 2 selects by \
         promotion provenance, not by blankness (selected = {selected:?})"
    );
    assert!(
        !selected.contains(&orphan_atom_id),
        "a blank-key atom linked to no node at all must NOT be selected (selected = {selected:?})"
    );
    assert_eq!(
        selected.len(),
        1,
        "exactly one of the three seeded blank-key rows carries promotion provenance \
         (selected = {selected:?})"
    );

    sqlx::query("DELETE FROM accounts WHERE id = $1").bind(account_id).execute(&pool).await.ok();
    sqlx::query("DELETE FROM users WHERE id = $1").bind(user_id).execute(&pool).await.ok();
}

/// The mirror image, and the one case arm 2 must NOT be read as repairable:
/// the resolved SOURCE atom's own key is blank, so there is nothing left to
/// re-upload from. Classified `SourceKeyBlank` — reported loudly, row
/// untouched, and NEVER `errors`/`put_failed` (which would demand a re-run
/// for a permanent condition). Also proves the classification happens
/// BEFORE the source GET: `get_atom_blob("")` fails `validate_blob_key`
/// with `InvalidKey`, not `NotFound`, so a version that fell through to the
/// GET would land on `OutcomeKind::Error` here instead.
#[tokio::test]
async fn blank_source_key_is_unrepairable_not_an_error() {
    let Ok(db_url) = std::env::var("RELINFER_TEST_DATABASE_URL") else {
        eprintln!("{SKIP_REASON}");
        return;
    };
    let Some(r2env) = r2_test_env() else {
        eprintln!("{SKIP_REASON}");
        return;
    };

    let (pool, r2, _dek, account_id, user_id, source_graph, target_graph) =
        setup_tenant_with_graphs(&db_url, &r2env, "blank-source-key").await;

    // The source row's OWN key was blanked too — a blanking sweep that
    // reached the source side.
    let source_id = format!("blank-source-{}", Uuid::new_v4());
    insert_atom_with_key(&pool, Uuid::new_v4(), source_graph, &source_id, "", None).await;
    let cand_atom_id = seed_blank_key_candidate(&pool, source_graph, target_graph, &source_id).await;

    let candidates = list_candidates(&pool, Some(target_graph), &[]).await.expect("list_candidates");
    let cand = candidates
        .iter()
        .find(|c| c.atom_id == cand_atom_id)
        .expect("selection is by provenance, so this row is still a candidate");

    let tenant = TenantId::Consumer;
    let dek_cache = tokio::sync::Mutex::new(std::collections::HashMap::new());
    let outcome = process_candidate(&pool, &r2, None, &tenant, &dek_cache, cand, true).await;

    assert_eq!(outcome.kind, OutcomeKind::SourceKeyBlank, "{}", outcome.report);
    assert!(outcome.report.contains("BLANK"), "{}", outcome.report);
    assert!(
        outcome.report.contains(&cand_atom_id.to_string()),
        "the report must print the atom id so an operator can follow up: {}",
        outcome.report
    );

    let mut totals = Totals::default();
    totals.apply(&outcome);
    assert_eq!(totals.source_key_blank, 1, "{totals}");
    assert_eq!(totals.errors, 0, "a blank source key is permanent, not transient: {totals}");
    assert_eq!(totals.put_failed, 0, "{totals}");
    assert_eq!(totals.put_ok, 0, "nothing can be written without source content: {totals}");
    assert!(totals.is_clean(), "an unrepairable row must never flip the exit code: {totals}");
    assert!(totals.summary_line().contains("BLANK"), "{}", totals.summary_line());

    let unchanged: String =
        sqlx::query_scalar("SELECT content_blob_key FROM atoms WHERE atom_id = $1")
            .bind(cand_atom_id)
            .fetch_one(&pool)
            .await
            .expect("row still exists");
    assert_eq!(unchanged, "", "an unrepairable row must never be touched");

    sqlx::query("DELETE FROM accounts WHERE id = $1").bind(account_id).execute(&pool).await.ok();
    sqlx::query("DELETE FROM users WHERE id = $1").bind(user_id).execute(&pool).await.ok();
}
