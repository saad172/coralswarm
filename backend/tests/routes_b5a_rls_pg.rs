//! RLS probes for the org-tenancy OPS-4 phase-B.5a conversion:
//! `services/{ask,transcripts,supersession}.rs`, `self_context.rs`, and
//! `routes/{meetings,transcript}.rs`.
//!
//! Two layers of proof, both against a REAL `coralswarm_app` login (the
//! non-BYPASSRLS role OPS-4 cuts the app over to), via the shared
//! `tests/support/rls_probe.rs` harness (PR #286):
//!
//! 1. **Query-shape probes** — the exact table/filter shapes the converted
//!    `_scoped` Store calls execute get the harness's three standard proofs:
//!    scoped-to-A returns exactly A's rows; scoped-to-A aimed at B's ids
//!    returns NOTHING; and the same query with `app.account_id` never set
//!    HARD-ERRORS (0083's strict `current_setting` predicate). This layer is
//!    how the `pub(crate)` service functions (`services::ask::run_ask`,
//!    `services::supersession::create_supersession_edges_scoped`,
//!    `services::transcripts::get_transcript`, `self_context::build_system_context`)
//!    are covered: an integration test cannot call them, but it CAN prove the
//!    query shapes they now issue behave correctly under the real policies —
//!    and, for the writes, that a forged cross-account write is refused with
//!    SQLSTATE 42501 rather than quietly landing.
//!
//! 2. **Route-level probes** — the real public router (`test_support_app`,
//!    real Clerk-shaped JWTs against a local JWKS listener) served entirely
//!    off the `coralswarm_app` pool. An unconverted (still-unscoped) query
//!    path inside these handlers hard-errors here instead of serving, so a
//!    green run is a genuine end-to-end proof that the conversion is complete
//!    for every path these requests take. Cross-account requests must fail
//!    closed (404, never B's data, never a write).
//!
//! Env-gated, like every `*_pg.rs` here:
//!
//!   ORGT_TEST_BASE_URL=postgres://alex@localhost:5432/postgres \
//!     cargo test -p coralswarm-backend --test routes_b5a_rls_pg

mod support;

use axum::body::Body;
use axum::http::{Request as HttpRequest, StatusCode};
use coralswarm_backend::clerk::test_support::{mint, test_jwks, ISSUER};
use coralswarm_backend::store::PostgresStore;
use serde_json::{json, Value};
use sqlx::PgPool;
use std::sync::Arc;
use support::rls_probe::RlsProbe;
use tower::ServiceExt;
use uuid::Uuid;

// ── Shared seeding helpers (owner/BYPASSRLS pool — seeding must bypass the
//    policies the tests then probe) ─────────────────────────────────────────

/// One `meeting_transcripts` row with INLINE raw text, so
/// `services::transcripts::resolve_raw_text` never needs R2/a DEK (the probe
/// deliberately runs with `state.r2 == None`).
async fn seed_transcript(owner: &PgPool, graph_id: Uuid, source_id: &str, raw: &str) {
    sqlx::query(
        "INSERT INTO meeting_transcripts(graph_id, source_id, raw_inline) \
         VALUES ($1, $2, $3)",
    )
    .bind(graph_id)
    .bind(source_id)
    .bind(raw)
    .execute(owner)
    .await
    .expect("insert meeting transcript");
}

/// Exact retained evidence identity does not verify legacy projection lineage.
/// Seed same-graph item/version pointers without inventing a summary receipt.
async fn seed_transcript_evidence(
    owner: &PgPool,
    graph_id: Uuid,
    source_id: &str,
    raw: &str,
) -> (Uuid, Uuid) {
    let mut tx = owner.begin().await.expect("begin retained evidence fixture");
    let item: Uuid = sqlx::query_scalar(
        "INSERT INTO source_items(graph_id,source_type,provider_namespace,source_id) \
         VALUES($1,'transcript','legacy:meeting',$2) RETURNING source_item_id",
    )
    .bind(graph_id)
    .bind(source_id)
    .fetch_one(&mut *tx)
    .await
    .expect("seed retained source item");
    let version: Uuid = sqlx::query_scalar(
        "INSERT INTO source_versions(graph_id,source_item_id,source_type,provider_namespace,source_id,raw_inline,representation) \
         VALUES($1,$2,'transcript','legacy:meeting',$3,$4,'normalized_text') RETURNING version_id",
    )
    .bind(graph_id)
    .bind(item)
    .bind(source_id)
    .bind(raw)
    .fetch_one(&mut *tx)
    .await
    .expect("seed retained source version");
    sqlx::query("UPDATE source_items SET current_version_id=$2 WHERE source_item_id=$1 AND graph_id=$3")
        .bind(item).bind(version).bind(graph_id).execute(&mut *tx).await.expect("set retained current version");
    let attached = sqlx::query("UPDATE meeting_transcripts SET source_item_id=$3,current_version_id=$4 WHERE graph_id=$1 AND source_id=$2")
        .bind(graph_id).bind(source_id).bind(item).bind(version).execute(&mut *tx).await.expect("attach exact transcript evidence");
    assert_eq!(attached.rows_affected(), 1, "the fixture must bind an existing transcript");
    tx.commit().await.expect("commit retained evidence fixture");
    (item, version)
}

/// Independent permitted notes provide literal name authority. The retained
/// version and chunk are seeded from identical bytes before their mapping.
async fn seed_known_name_support(
    owner: &PgPool,
    graph: Uuid,
    user: Uuid,
    node: Uuid,
    source: &str,
    raw: &str,
) {
    let mut tx = owner.begin().await.expect("begin literal name support fixture");
    let atom = Uuid::new_v4();
    sqlx::query("INSERT INTO atoms(atom_id,graph_id,source_type,source_id,content_blob_key,content_inline,ingested_by,timestamp) VALUES($1,$2,'note',$3,$4,$5,$6,now())")
        .bind(atom).bind(graph).bind(source).bind(format!("{graph}/{atom}"))
        .bind(raw).bind(user).execute(&mut *tx).await.expect("seed independent literal note");
    let item: Uuid = sqlx::query_scalar("INSERT INTO source_items(graph_id,source_type,provider_namespace,source_id,access_state) VALUES($1,'note','fixture:b5a-known-names',$2,'permitted') RETURNING source_item_id")
        .bind(graph).bind(source).fetch_one(&mut *tx).await.expect("seed permitted name source");
    let version: Uuid = sqlx::query_scalar("INSERT INTO source_versions(graph_id,source_item_id,source_id,source_type,provider_namespace,raw_inline,content_sha256,representation,ingested_by) VALUES($1,$2,$3,'note','fixture:b5a-known-names',$4,encode(sha256(convert_to($4,'UTF8')),'hex'),'normalized_text',$5) RETURNING version_id")
        .bind(graph).bind(item).bind(source).bind(raw).bind(user)
        .fetch_one(&mut *tx).await.expect("retain identical literal note bytes");
    sqlx::query("UPDATE source_items SET current_version_id=$2 WHERE source_item_id=$1")
        .bind(item).bind(version).execute(&mut *tx).await.expect("set current name evidence");
    sqlx::query("INSERT INTO source_version_chunks(graph_id,source_version_id,atom_id) VALUES($1,$2,$3)")
        .bind(graph).bind(version).bind(atom).execute(&mut *tx).await.expect("map literal note before read");
    sqlx::query("INSERT INTO node_atoms(node_id,atom_id) VALUES($1,$2)")
        .bind(node).bind(atom).execute(&mut *tx).await.expect("link name to genuine support");
    let consistent: bool = sqlx::query_scalar("SELECT a.content_inline=v.raw_inline AND v.content_sha256=encode(sha256(convert_to(v.raw_inline,'UTF8')),'hex') AND a.source_evidence_bound AND s.access_state='permitted' FROM source_version_chunks c JOIN atoms a ON a.atom_id=c.atom_id AND a.graph_id=c.graph_id JOIN source_versions v ON v.version_id=c.source_version_id AND v.graph_id=c.graph_id JOIN source_items s ON s.source_item_id=v.source_item_id AND s.graph_id=v.graph_id WHERE c.graph_id=$1 AND c.atom_id=$2 AND c.source_version_id=$3")
        .bind(graph).bind(atom).bind(version).fetch_one(&mut *tx).await.expect("check immutable support consistency");
    assert!(consistent, "permitted retained version body/hash must agree with the literal support chunk");
    tx.commit().await.expect("commit literal support fixture");
}

fn assert_unverified_projection(body: &Value) {
    assert_eq!(body["projection_status"], "unverified_lineage", "{body}");
    assert_eq!(body["summary_status"], "unavailable", "{body}");
    for field in ["title", "raw_text", "corrected_text", "summary", "summary_model", "summary_error", "segments"] {
        assert_eq!(body.get(field), Some(&Value::Null), "{field} must remain masked: {body}");
    }
    for field in ["corrections", "merges", "new_entities"] {
        assert_eq!(body[field], json!([]), "{field} must remain masked: {body}");
    }
}

async fn seed_correction(
    owner: &PgPool,
    graph_id: Uuid,
    source_id: &str,
    surface: &str,
    canonical: &str,
) -> Uuid {
    // `atom_id` is NOT NULL with no FK; a user relabel writes the nil-UUID
    // sentinel, exactly like `Store::insert_speaker_relabel_scoped` (see 0070).
    sqlx::query_scalar(
        "INSERT INTO transcript_corrections \
             (graph_id, atom_id, source_id, surface_form, canonical, status, kind, origin) \
         VALUES ($1, $2, $3, $4, $5, 'applied', 'speaker', 'user') \
         RETURNING correction_id",
    )
    .bind(graph_id)
    .bind(Uuid::nil())
    .bind(source_id)
    .bind(surface)
    .bind(canonical)
    .fetch_one(owner)
    .await
    .expect("insert transcript correction")
}

async fn seed_second_node(owner: &PgPool, graph_id: Uuid, title: &str) -> Uuid {
    sqlx::query_scalar(
        "INSERT INTO nodes(graph_id, type, title, body) VALUES ($1, 'entity', $2, '{}') \
         RETURNING node_id",
    )
    .bind(graph_id)
    .bind(title)
    .fetch_one(owner)
    .await
    .expect("insert second node")
}

async fn seed_node_atom(owner: &PgPool, node_id: Uuid, atom_id: Uuid) {
    sqlx::query("INSERT INTO node_atoms(node_id, atom_id) VALUES ($1, $2)")
        .bind(node_id)
        .bind(atom_id)
        .execute(owner)
        .await
        .expect("link reef to coral");
}

/// True when `e` is the RLS `WITH CHECK` refusal (`42501`), i.e. the write was
/// rejected by policy rather than by a foreign key / unique constraint. Mirrors
/// `store::is_pg_rls_denied`'s discrimination (PR #285) — a `23503` here would
/// mean the test proved the wrong thing.
fn is_rls_denied(e: &sqlx::Error) -> bool {
    matches!(e, sqlx::Error::Database(db) if db.code().as_deref() == Some("42501"))
}

// ── 1. Query-shape probes, one section per converted file ───────────────────

#[tokio::test]
async fn b5a_query_shapes_are_account_scoped() {
    let Some(probe) = RlsProbe::setup("orgt_rls_b5a_shapes").await else {
        eprintln!("skipping b5a_query_shapes_are_account_scoped: set ORGT_TEST_BASE_URL");
        return;
    };

    const SOURCE_A: &str = "meeting:probe-a";
    const SOURCE_B: &str = "meeting:probe-b";
    seed_transcript(&probe.owner, probe.a.graph_id, SOURCE_A, "A: hello").await;
    seed_transcript(&probe.owner, probe.b.graph_id, SOURCE_B, "B: hello").await;
    let a_correction =
        seed_correction(&probe.owner, probe.a.graph_id, SOURCE_A, "Speaker 1", "Ada").await;
    let b_correction =
        seed_correction(&probe.owner, probe.b.graph_id, SOURCE_B, "Speaker 1", "Bob").await;

    // ── routes/meetings.rs + services/transcripts.rs:
    //    get_meeting_transcript_scoped / list_transcript_corrections_scoped ──
    //
    // NOTE the cross-account probe binds B's OWN graph_id: holding a correctly
    // resolved id from the other account must buy nothing once the scope is A's.
    probe
        .assert_scoped_returns_own_rows(
            probe.a.account_id,
            "SELECT graph_id FROM meeting_transcripts WHERE graph_id = $1",
            &[probe.a.graph_id],
            &[probe.a.graph_id],
        )
        .await;
    probe
        .assert_cross_account_returns_nothing(
            probe.a.account_id,
            "SELECT graph_id FROM meeting_transcripts WHERE graph_id = $1",
            &[probe.b.graph_id],
        )
        .await;
    probe
        .assert_unscoped_hard_errors(
            "SELECT graph_id FROM meeting_transcripts WHERE graph_id = $1",
            &[probe.a.graph_id],
        )
        .await;

    probe
        .assert_scoped_returns_own_rows(
            probe.a.account_id,
            "SELECT correction_id FROM transcript_corrections WHERE graph_id = $1",
            &[probe.a.graph_id],
            &[a_correction],
        )
        .await;
    probe
        .assert_cross_account_returns_nothing(
            probe.a.account_id,
            "SELECT correction_id FROM transcript_corrections WHERE graph_id = $1",
            &[probe.b.graph_id],
        )
        .await;
    // The ID-ANCHORED shape `routes::meetings::set_correction_status` runs
    // (`transcript_correction_graph_id_scoped`): B's correction id under A's
    // scope must resolve to nothing, which is what fails the handler closed
    // onto its non-leaking 404.
    probe
        .assert_cross_account_returns_nothing(
            probe.a.account_id,
            "SELECT graph_id FROM transcript_corrections WHERE correction_id = $1",
            &[b_correction],
        )
        .await;
    probe
        .assert_unscoped_hard_errors(
            "SELECT graph_id FROM transcript_corrections WHERE correction_id = $1",
            &[a_correction],
        )
        .await;

    // ── routes/meetings.rs: list_meeting_merges_scoped (node_merges) ────────
    probe
        .assert_cross_account_returns_nothing(
            probe.a.account_id,
            "SELECT m.merge_id FROM node_merges m JOIN nodes n ON n.node_id = m.keep_id \
             WHERE m.graph_id = $1 AND m.undone_at IS NULL",
            &[probe.b.graph_id],
        )
        .await;
    probe
        .assert_unscoped_hard_errors(
            "SELECT m.merge_id FROM node_merges m JOIN nodes n ON n.node_id = m.keep_id \
             WHERE m.graph_id = $1 AND m.undone_at IS NULL",
            &[probe.a.graph_id],
        )
        .await;

    // ── routes/transcript.rs: list_nodes_scoped (the known-names dictionary) ─
    probe
        .assert_scoped_returns_own_rows(
            probe.a.account_id,
            "SELECT node_id FROM nodes WHERE graph_id = $1 AND type = 'entity'",
            &[probe.a.graph_id],
            &[probe.a.node_id],
        )
        .await;
    probe
        .assert_cross_account_returns_nothing(
            probe.a.account_id,
            "SELECT node_id FROM nodes WHERE graph_id = $1 AND type = 'entity'",
            &[probe.b.graph_id],
        )
        .await;
    probe
        .assert_unscoped_hard_errors(
            "SELECT node_id FROM nodes WHERE graph_id = $1 AND type = 'entity'",
            &[probe.a.graph_id],
        )
        .await;

    // ── self_context.rs: the three ocean_stats_scoped aggregates ────────────
    // (each runs in its own `begin_account_scoped` transaction — see that
    // module's doc — so each is probed independently.)
    probe
        .assert_cross_account_returns_nothing(
            probe.a.account_id,
            "SELECT node_id FROM nodes WHERE graph_id = $1",
            &[probe.b.graph_id],
        )
        .await;
    probe
        .assert_cross_account_returns_nothing(
            probe.a.account_id,
            "SELECT atom_id FROM atoms WHERE graph_id = $1",
            &[probe.b.graph_id],
        )
        .await;
    probe
        .assert_unscoped_hard_errors(
            "SELECT edge_id FROM relationship_edge WHERE graph_id = $1",
            &[probe.a.graph_id],
        )
        .await;

    // ── services/ask.rs: query_history (the spawned attribution write) ──────
    probe
        .assert_scoped_returns_own_rows(
            probe.a.account_id,
            "SELECT id FROM query_history WHERE account_id = $1",
            &[probe.a.account_id],
            &[],
        )
        .await;
    probe
        .assert_unscoped_hard_errors(
            "SELECT id FROM query_history WHERE account_id = $1",
            &[probe.a.account_id],
        )
        .await;

    probe.teardown().await;
}

/// The two WRITES this conversion introduced into scoped transactions, proven
/// at the policy level: a correctly-scoped write COMMITS, and the same write
/// aimed at the other account's rows is refused with SQLSTATE **42501** (the
/// RLS `WITH CHECK` refusal — not a `23503` foreign-key error, which would
/// mean the test proved the wrong thing).
#[tokio::test]
async fn b5a_scoped_writes_commit_and_cross_account_writes_are_refused() {
    let Some(probe) = RlsProbe::setup("orgt_rls_b5a_writes").await else {
        eprintln!(
            "skipping b5a_scoped_writes_commit_and_cross_account_writes_are_refused: \
             set ORGT_TEST_BASE_URL"
        );
        return;
    };

    let a_node2 = seed_second_node(&probe.owner, probe.a.graph_id, "Probe Node A2").await;
    let b_node2 = seed_second_node(&probe.owner, probe.b.graph_id, "Probe Node B2").await;

    // ── services/supersession.rs: insert_auto_supersession_edge_scoped ──────
    // (a) Correctly scoped, both endpoints in A's ocean: commits.
    let mut tx = probe.scoped_tx(probe.a.account_id).await;
    // `graph_id` is NOT NULL as of migrations 0094/0008. Bound EXPLICITLY here
    // rather than derived with `(SELECT graph_id FROM nodes WHERE ...)`: under
    // A's scope that subquery would return NULL for B's node in case (b) below
    // and raise a not-null violation, so the test would stop proving that RLS
    // is what refuses the forged row.
    sqlx::query(
        "INSERT INTO supersession_edge(superseded_id, superseder_id, source, graph_id) \
         VALUES ($1, $2, 'auto', $3) ON CONFLICT DO NOTHING",
    )
    .bind(probe.a.node_id)
    .bind(a_node2)
    .bind(probe.a.graph_id)
    .execute(&mut *tx)
    .await
    .expect("A's own auto-supersession edge must insert under A's scope");
    tx.commit().await.expect("commit A's edge");

    // (b) A's scope, B's nodes: refused by policy, and nothing lands.
    let mut tx = probe.scoped_tx(probe.a.account_id).await;
    let forged = sqlx::query(
        "INSERT INTO supersession_edge(superseded_id, superseder_id, source, graph_id) \
         VALUES ($1, $2, 'auto', $3)",
    )
    .bind(probe.b.node_id)
    .bind(b_node2)
    .bind(probe.b.graph_id)
    .execute(&mut *tx)
    .await;
    let err = forged.expect_err("A must not be able to write an edge between B's reefs");
    assert!(
        is_rls_denied(&err),
        "cross-account supersession_edge INSERT must be refused by RLS (42501), got {err:?}"
    );
    tx.rollback().await.ok();

    let leaked: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM supersession_edge WHERE superseded_id = $1 AND source = 'auto'",
    )
    .bind(probe.b.node_id)
    .fetch_one(&probe.owner)
    .await
    .expect("count B's edges via the owner pool");
    assert_eq!(
        leaked, 0,
        "the refused cross-account edge must not have landed"
    );

    // ── services/ask.rs: insert_query_history_scoped ────────────────────────
    let mut tx = probe.scoped_tx(probe.a.account_id).await;
    sqlx::query(
        "INSERT INTO query_history(account_id, graph_id, user_id, query_text, answer_summary, \
                                   citation_count, input_tokens, output_tokens) \
         VALUES ($1, $2, $3, 'probe', 'probe', 0, 0, 0)",
    )
    .bind(probe.a.account_id)
    .bind(probe.a.graph_id)
    .bind(probe.a.user_id)
    .execute(&mut *tx)
    .await
    .expect("A's own query_history row must insert under A's scope");
    tx.commit().await.expect("commit A's query_history row");

    let mut tx = probe.scoped_tx(probe.a.account_id).await;
    let forged = sqlx::query(
        "INSERT INTO query_history(account_id, graph_id, user_id, query_text, answer_summary, \
                                   citation_count, input_tokens, output_tokens) \
         VALUES ($1, $2, $3, 'probe', 'probe', 0, 0, 0)",
    )
    .bind(probe.b.account_id)
    .bind(probe.b.graph_id)
    .bind(probe.b.user_id)
    .execute(&mut *tx)
    .await;
    let err = forged.expect_err("A must not be able to attribute a query to B's account");
    assert!(
        is_rls_denied(&err),
        "cross-account query_history INSERT must be refused by RLS (42501), got {err:?}"
    );
    tx.rollback().await.ok();

    // ── routes/meetings.rs: insert_speaker_relabel_scoped ───────────────────
    let mut tx = probe.scoped_tx(probe.a.account_id).await;
    let forged = sqlx::query(
        "INSERT INTO transcript_corrections \
             (graph_id, atom_id, source_id, surface_form, canonical, status, kind, origin) \
         VALUES ($1, $2, 'meeting:probe-b', 'Speaker 1', 'Mallory', 'applied', 'speaker', 'user')",
    )
    .bind(probe.b.graph_id)
    .bind(Uuid::nil())
    .execute(&mut *tx)
    .await;
    let err = forged.expect_err("A must not be able to write a relabel into B's ocean");
    assert!(
        is_rls_denied(&err),
        "cross-account transcript_corrections INSERT must be refused by RLS (42501), got {err:?}"
    );
    tx.rollback().await.ok();

    probe.teardown().await;
}

// ── 2. Route-level probes through the real public router, as coralswarm_app ─

fn future_exp() -> usize {
    (chrono::Utc::now().timestamp() + 3600) as usize
}

/// Local JWKS listener serving the shared test keypair — same fixture
/// `routes_b2_rls_pg.rs`/`route_migration_org_pg.rs` use, duplicated for the
/// same separate-compilation-unit reason.
async fn spawn_local_jwks() -> String {
    let jwks_body = test_jwks();
    let jwks_app = axum::Router::new().route(
        "/.well-known/jwks.json",
        axum::routing::get(move || {
            let body = jwks_body.clone();
            async move { ([("content-type", "application/json")], body) }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind local JWKS listener");
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, jwks_app).await.ok();
    });
    format!("http://{addr}/.well-known/jwks.json")
}

async fn body_json(resp: axum::response::Response) -> Value {
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    serde_json::from_slice(&bytes).unwrap_or(Value::Null)
}

/// Echoes the prompt back as the completion. `routes::transcript::cleanup_transcript`
/// short-circuits with the ORIGINAL transcript when `state.llm` is `None`, so
/// without a provider the handler never reaches the `list_nodes_scoped`
/// dictionary load this suite exists to prove. Echoing also satisfies the
/// handler's `is_plausible_cleanup` length guard by construction.
struct EchoLlm;

#[async_trait::async_trait]
impl coralswarm_backend::llm::LlmProvider for EchoLlm {
    async fn complete(
        &self,
        req: coralswarm_backend::llm::CompletionRequest,
    ) -> Result<coralswarm_backend::llm::CompletionResponse, coralswarm_backend::llm::LlmError>
    {
        Ok(coralswarm_backend::llm::CompletionResponse {
            content: req.messages[0].content.clone(),
            input_tokens: 0,
            output_tokens: 0,
            model: req.model,
            cache_creation_input_tokens: 0,
            cache_read_input_tokens: 0,
            stop_reason: None,
            cost_usd: None,
        })
    }

    async fn stream_complete(
        &self,
        _req: coralswarm_backend::llm::CompletionRequest,
    ) -> Result<
        std::pin::Pin<
            Box<
                dyn futures::Stream<
                        Item = Result<
                            coralswarm_backend::llm::StreamEvent,
                            coralswarm_backend::llm::LlmError,
                        >,
                    > + Send,
            >,
        >,
        coralswarm_backend::llm::LlmError,
    > {
        unreachable!("cleanup_transcript never streams")
    }
}

#[tokio::test]
async fn b5a_converted_handlers_work_and_isolate_as_coralswarm_app() {
    let Some(probe) = RlsProbe::setup("orgt_rls_b5a_routes").await else {
        eprintln!(
            "skipping b5a_converted_handlers_work_and_isolate_as_coralswarm_app: \
             set ORGT_TEST_BASE_URL"
        );
        return;
    };

    // ── Env: local JWKS so `require_auth` verifies real Clerk-shaped tokens.
    // This binary's only env-mutating test, so no cross-test lock is needed
    // (each test binary is its own process). ─────────────────────────────────
    let jwks_url = spawn_local_jwks().await;
    // SAFETY: single-threaded with respect to env mutation — this is the only
    // test in this binary that touches CLERK_* (see comment above).
    unsafe {
        std::env::set_var("CLERK_ISSUER", ISSUER);
        std::env::set_var("CLERK_JWKS_URL", &jwks_url);
    }

    // Wire the harness fixtures up to Clerk-shaped identities so
    // `require_auth`'s provisioning resolves to EXACTLY the probe's seeded
    // users/accounts, and seed the `account_members` rows the SECURITY DEFINER
    // reachability carve-out consults.
    let sub_a = "b5a_rls_probe_user_a";
    let sub_b = "b5a_rls_probe_user_b";
    for (sub, fx) in [(sub_a, &probe.a), (sub_b, &probe.b)] {
        sqlx::query("UPDATE users SET clerk_user_id = $1 WHERE id = $2")
            .bind(sub)
            .bind(fx.user_id)
            .execute(&probe.owner)
            .await
            .expect("stamp clerk_user_id");
        sqlx::query(
            "INSERT INTO account_members(account_id, user_id, role) VALUES ($1, $2, 'owner')",
        )
        .bind(fx.account_id)
        .bind(fx.user_id)
        .execute(&probe.owner)
        .await
        .expect("seed account_members");
    }

    const SOURCE_A: &str = "meeting:b5a-a";
    const SOURCE_B: &str = "meeting:b5a-b";
    seed_transcript(
        &probe.owner,
        probe.a.graph_id,
        SOURCE_A,
        "Kickoff sync\n\nSpeaker 1: shipping friday",
    )
    .await;
    seed_transcript(&probe.owner, probe.b.graph_id, SOURCE_B, "B private notes").await;
    // `summary_status` defaults to 'pending', so park B's at 'done': that makes
    // "A reset B's row" observable below (a successful cross-account reset would
    // move it BACK to 'pending').
    sqlx::query(
        "UPDATE meeting_transcripts SET summary_status = 'done' \
         WHERE graph_id = $1 AND source_id = $2",
    )
    .bind(probe.b.graph_id)
    .bind(SOURCE_B)
    .execute(&probe.owner)
    .await
    .expect("park B's summary_status at done");
    let a_correction =
        seed_correction(&probe.owner, probe.a.graph_id, SOURCE_A, "Speaker 1", "Ada").await;
    let b_correction =
        seed_correction(&probe.owner, probe.b.graph_id, SOURCE_B, "Speaker 1", "Bob").await;
    seed_node_atom(&probe.owner, probe.a.node_id, probe.a.atom_id).await;

    // The generic RLS probe atom has no literal name authority. Preserve the
    // transcript/correction fixture and add independent, source-bound notes.
    sqlx::query("UPDATE nodes SET title='Probe Node B' WHERE node_id=$1 AND graph_id=$2")
        .bind(probe.b.node_id).bind(probe.b.graph_id).execute(&probe.owner).await.expect("give B a distinct private name");
    seed_known_name_support(&probe.owner, probe.a.graph_id, probe.a.user_id, probe.a.node_id,
        "b5a-name-note-a", "Probe Node is named in this independent A-owned note.").await;
    seed_known_name_support(&probe.owner, probe.b.graph_id, probe.b.user_id, probe.b.node_id,
        "b5a-name-note-b", "Probe Node B is named in this independent B-owned private note.").await;
    for (fx, expected) in [(&probe.a, "Probe Node"), (&probe.b, "Probe Node B")] {
        let mut tx = coralswarm_backend::tenancy::scope::begin_tenant_scoped(
            &probe.app_pool, fx.account_id, fx.user_id,
        ).await.expect("scope the genuine owning member");
        let title: Option<String> = sqlx::query_scalar("SELECT library_node_title($1,$2)")
            .bind(fx.graph_id).bind(fx.node_id).fetch_one(&mut *tx).await.expect("read authorized literal name");
        assert_eq!(title.as_deref(), Some(expected), "each member's private name must be genuinely supported before isolation is tested");
        tx.rollback().await.expect("close name precondition read");
    }

    // ── AppState served ENTIRELY off the coralswarm_app pool: every content
    // query a converted handler runs is now subject to 0083's policies — an
    // unconverted (unscoped) query path hard-errors instead of serving. ──────
    let mut state = (*coralswarm_backend::test_app_state()).clone();
    state.db = Some(probe.app_pool.clone());
    state.store = Some(Arc::new(PostgresStore::new(probe.app_pool.clone())));
    // See `EchoLlm`: without a provider `cleanup_transcript` never reaches its
    // scoped known-names load.
    state.llm = Some(Arc::new(EchoLlm));
    let app = coralswarm_backend::test_support_app(Arc::new(state));

    let token_a = mint(json!({ "iss": ISSUER, "sub": sub_a, "exp": future_exp() }));

    macro_rules! req_a {
        ($method:expr, $uri:expr) => {
            HttpRequest::builder()
                .method($method)
                .uri($uri)
                .header("authorization", format!("Bearer {token_a}"))
                .body(Body::empty())
                .unwrap()
        };
        ($method:expr, $uri:expr, $json:expr) => {
            HttpRequest::builder()
                .method($method)
                .uri($uri)
                .header("authorization", format!("Bearer {token_a}"))
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&$json).unwrap()))
                .unwrap()
        };
    }

    // ── routes/meetings.rs: GET /v1/meetings/summary ────────────────────────
    // Cloud legacy projections have no complete dependency receipt. They
    // remain unavailable even to the owner; exact retained evidence below
    // exercises the replacement read surface under the same app role.
    let resp = app
        .clone()
        .oneshot(req_a!(
            "GET",
            format!(
                "/v1/meetings/summary?graph_id={}&source_id={}",
                probe.a.graph_id, SOURCE_A
            )
        ))
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "A's own meeting summary must serve under the coralswarm_app role"
    );
    let body = body_json(resp).await;
    assert_unverified_projection(&body);
    assert_eq!(body["evidence_status"], "unknown", "{body}");
    assert_eq!(body["source_items"], json!([]), "an unbound projection has no verified source reference: {body}");
    for secret in ["Kickoff sync", "shipping friday", "Ada", &a_correction.to_string()] {
        assert!(!body.to_string().contains(secret), "unverified projection must not reveal {secret}: {body}");
    }

    let (a_item, a_version) = seed_transcript_evidence(
        &probe.owner, probe.a.graph_id, SOURCE_A,
        "Kickoff sync\n\nSpeaker 1: shipping friday",
    ).await;
    let (b_item, b_version) = seed_transcript_evidence(
        &probe.owner, probe.b.graph_id, SOURCE_B, "B private notes",
    ).await;
    let resp = app.clone().oneshot(req_a!(
        "GET", format!("/v1/meetings/summary?graph_id={}&source_id={}", probe.a.graph_id, SOURCE_A)
    )).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp).await;
    assert_unverified_projection(&body);
    assert_eq!(body["evidence_status"], "retained", "{body}");
    assert_eq!(body["source_items"], json!([{
        "graph_id": probe.a.graph_id, "source_item_id": a_item, "source_version_id": a_version,
    }]), "the scoped canonical lookup must return exactly A's evidence: {body}");
    for secret in ["shipping friday", "Ada", &a_correction.to_string(), "B private notes", &b_item.to_string(), &b_version.to_string()] {
        assert!(!body.to_string().contains(secret), "unverified projection must not reveal {secret}: {body}");
    }

    let resp = app.clone().oneshot(req_a!(
        "GET", format!("/v1/sources/evidence?graph_id={}&source_item_id={a_item}&version_id={a_version}", probe.a.graph_id)
    )).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "A's exact retained evidence must serve under the app role");
    let evidence = body_json(resp).await;
    assert_eq!(evidence["source_item_id"], json!(a_item), "{evidence}");
    assert_eq!(evidence["selected_version"]["version_id"], json!(a_version), "{evidence}");
    assert_eq!(evidence["selected_version"]["raw_text"], "Kickoff sync\n\nSpeaker 1: shipping friday", "{evidence}");
    for secret in ["B private notes", &b_item.to_string(), &b_version.to_string(), &b_correction.to_string()] {
        assert!(!evidence.to_string().contains(secret), "A's evidence must not reveal {secret}: {evidence}");
    }

    // Exact private identifiers and mixed graph/item/version selectors are
    // never authority. All three attempts must deny without echoing B's IDs.
    for (graph, item, version) in [
        (probe.b.graph_id, b_item, b_version),
        (probe.a.graph_id, b_item, b_version),
        (probe.a.graph_id, a_item, b_version),
    ] {
        let resp = app.clone().oneshot(req_a!(
            "GET", format!("/v1/sources/evidence?graph_id={graph}&source_item_id={item}&version_id={version}")
        )).await.unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND, "cross-account or mixed selectors must fail closed");
        let denied = body_json(resp).await;
        for secret in ["B private notes", &b_item.to_string(), &b_version.to_string(), &b_correction.to_string()] {
            assert!(!denied.to_string().contains(secret), "denial must not reveal {secret}: {denied}");
        }
    }

    // Cross-account: A aiming at B's ocean 404s and leaks nothing.
    let resp = app
        .clone()
        .oneshot(req_a!(
            "GET",
            format!(
                "/v1/meetings/summary?graph_id={}&source_id={}",
                probe.b.graph_id, SOURCE_B
            )
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    let body = body_json(resp).await;
    assert!(
        !body.to_string().contains("B private notes"),
        "the cross-account 404 must not leak B's transcript: {body}"
    );
    for private_id in [b_item, b_version, b_correction] {
        assert!(!body.to_string().contains(&private_id.to_string()), "cross-account summary must not reveal private identifiers: {body}");
    }

    // ── routes/meetings.rs: POST /v1/transcript-corrections ─────────────────
    let resp = app
        .clone()
        .oneshot(req_a!(
            "POST",
            "/v1/transcript-corrections",
            json!({
                "graph_id": probe.a.graph_id,
                "source_id": SOURCE_A,
                "surface_form": "Speaker 2",
                "canonical": "Grace",
            })
        ))
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::CREATED,
        "A's own speaker relabel must commit under the coralswarm_app role"
    );

    // A aiming at B's ocean: 404 and NO row written.
    let resp = app
        .clone()
        .oneshot(req_a!(
            "POST",
            "/v1/transcript-corrections",
            json!({
                "graph_id": probe.b.graph_id,
                "source_id": SOURCE_B,
                "surface_form": "Speaker 2",
                "canonical": "Mallory",
            })
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    let forged: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM transcript_corrections WHERE graph_id = $1 AND canonical = 'Mallory'",
    )
    .bind(probe.b.graph_id)
    .fetch_one(&probe.owner)
    .await
    .expect("count B's corrections via the owner pool");
    assert_eq!(
        forged, 0,
        "the refused cross-account relabel must not have landed"
    );

    // ── routes/meetings.rs: POST /v1/transcript-corrections/{id}/revert ─────
    let resp = app
        .clone()
        .oneshot(req_a!(
            "POST",
            format!("/v1/transcript-corrections/{a_correction}/revert")
        ))
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "A must be able to revert A's own correction under the app role"
    );
    let status: String =
        sqlx::query_scalar("SELECT status FROM transcript_corrections WHERE correction_id = $1")
            .bind(a_correction)
            .fetch_one(&probe.owner)
            .await
            .expect("read A's correction status via the owner pool");
    assert_eq!(status, "reverted");

    // A holding B's correction id: 404, and B's row is untouched.
    let resp = app
        .clone()
        .oneshot(req_a!(
            "POST",
            format!("/v1/transcript-corrections/{b_correction}/revert")
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    let status: String =
        sqlx::query_scalar("SELECT status FROM transcript_corrections WHERE correction_id = $1")
            .bind(b_correction)
            .fetch_one(&probe.owner)
            .await
            .expect("read B's correction status via the owner pool");
    assert_eq!(
        status, "applied",
        "A must not be able to revert B's correction"
    );

    // ── routes/meetings.rs: POST /v1/meetings/summary/generate ──────────────
    let resp = app
        .clone()
        .oneshot(req_a!(
            "POST",
            "/v1/meetings/summary/generate",
            json!({ "graph_id": probe.a.graph_id, "source_id": SOURCE_A })
        ))
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "A's own summary (re)generation must commit under the app role"
    );
    // The W.1 stamp is now always present on this path (see the handler's
    // declared behavior delta) — and it is A's account, never B's.
    let stamped: Option<Uuid> = sqlx::query_scalar(
        "SELECT account_id FROM jobs WHERE kind = 'GenerateMeetingSummary' \
         ORDER BY created_at DESC LIMIT 1",
    )
    .fetch_one(&probe.owner)
    .await
    .expect("read the enqueued job via the owner pool");
    assert_eq!(stamped, Some(probe.a.account_id));

    let resp = app
        .clone()
        .oneshot(req_a!(
            "POST",
            "/v1/meetings/summary/generate",
            json!({ "graph_id": probe.b.graph_id, "source_id": SOURCE_B })
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    let b_status: Option<String> = sqlx::query_scalar(
        "SELECT summary_status FROM meeting_transcripts WHERE graph_id = $1 AND source_id = $2",
    )
    .bind(probe.b.graph_id)
    .bind(SOURCE_B)
    .fetch_one(&probe.owner)
    .await
    .expect("read B's transcript via the owner pool");
    assert_eq!(
        b_status.as_deref(),
        Some("done"),
        "A must not be able to reset B's summary state back to pending"
    );

    // ── routes/transcript.rs: POST /v1/transcript/cleanup ───────────────────
    // A's own ocean: the scoped `list_nodes_scoped` dictionary load must
    // succeed under the app role, and the seeded entity reef's title must
    // reach the model (which is what proves the read returned rows rather
    // than being silently emptied by the policy).
    let resp = app
        .clone()
        .oneshot(req_a!(
            "POST",
            "/v1/transcript/cleanup",
            json!({ "graph_id": probe.a.graph_id, "transcript": "Speaker 1: hi there everyone" })
        ))
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "A's own transcript cleanup must serve under the coralswarm_app role"
    );
    let body = body_json(resp).await;
    let cleaned = body["transcript"].as_str().unwrap_or_default();
    assert!(
        cleaned.contains("## Known names") && cleaned.contains("Probe Node"),
        "the scoped known-names dictionary must have loaded A's own entity reef: {body}"
    );
    assert!(
        !cleaned.contains("Probe Node B"),
        "the dictionary must never contain B's reef titles: {body}"
    );

    // A aiming at B's ocean: the generic 403, with no dictionary read at all.
    let resp = app
        .clone()
        .oneshot(req_a!(
            "POST",
            "/v1/transcript/cleanup",
            json!({ "graph_id": probe.b.graph_id, "transcript": "Speaker 1: hi there everyone" })
        ))
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::FORBIDDEN,
        "A must not be able to run cleanup against B's ocean"
    );

    // ── Contract §8.4 / C15 propagation ACCOUNT correctness (CodeRabbit
    // round 1, finding 1) ────────────────────────────────────────────────
    //
    // A is now made a GENUINE cross-account member of B (ADR-0007 Model B —
    // the same shape that makes `graph_reachable_by_user` legitimately admit
    // a caller whose active session account differs from the ocean's
    // owner). Revert/restore of B's correction must then succeed for A
    // (unlike the refused attempt above, before this membership existed),
    // and — the actual finding — propagation triggered by that revert/
    // restore MUST use B's account, never A's: `services::corrections::
    // propagate`'s account-scoped writes (and, in production, its DEK
    // resolution) all key off the SAME `account_id` parameter
    // `routes::meetings::spawn_propagate_corrections` passes in, so a wrong
    // account there is observable as B's atom silently NOT being rewritten
    // (an account-scoped UPDATE with no matching RLS-visible row affects
    // zero rows, per migration `0083`'s policies) rather than as an error.
    sqlx::query("INSERT INTO account_members(account_id, user_id, role) VALUES ($1, $2, 'member')")
        .bind(probe.b.account_id)
        .bind(probe.a.user_id)
        .execute(&probe.owner)
        .await
        .expect("seed A's cross-account membership into B");

    let b_atom_source = format!("{SOURCE_B}:chunk:0");
    let b_atom_id: Uuid = sqlx::query_scalar(
        "INSERT INTO atoms (graph_id, source_type, source_id, content_blob_key, content_inline, \
                             participants, \"timestamp\", scope, fts_language) \
         VALUES ($1, 'transcript', $2, 'blob-unused', $3, '{}', now(), 'private', 'english') \
         RETURNING atom_id",
    )
    .bind(probe.b.graph_id)
    .bind(&b_atom_source)
    .bind("Speaker 1: B's private turn.")
    .fetch_one(&probe.owner)
    .await
    .expect("seed B's transcript atom");

    // A can now legitimately reach B's ocean — revert B's correction
    // (`b_correction`, surface_form "Speaker 1" -> canonical "Bob").
    let resp = app
        .clone()
        .oneshot(req_a!(
            "POST",
            format!("/v1/transcript-corrections/{b_correction}/revert")
        ))
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "A, now a genuine cross-account member of B, must be able to revert B's correction"
    );
    let status: String =
        sqlx::query_scalar("SELECT status FROM transcript_corrections WHERE correction_id = $1")
            .bind(b_correction)
            .fetch_one(&probe.owner)
            .await
            .expect("read B's correction status via the owner pool");
    assert_eq!(status, "reverted");

    // Restore it — unlike the revert above (which leaves NO correction
    // `applied` for this source, so propagation is a legitimate early no-op
    // per `services::corrections::propagate`'s own doc), restore DOES
    // populate the `applied` set and has an observable effect.
    let resp = app
        .clone()
        .oneshot(req_a!(
            "POST",
            format!("/v1/transcript-corrections/{b_correction}/restore")
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // Propagation is now a DURABLE job (CodeRabbit round 2, finding 1),
    // enqueued in the SAME transaction as each correction write — verify
    // B's job actually landed (scoped to B's account, since A's own earlier
    // relabel above enqueued its OWN unrelated pending job too), then
    // process it directly (as `workers::propagate_corrections::run`'s poll
    // loop would) to observe propagation's effect. The revert and restore
    // above share ONE idempotency key (`graph_id` + `source_id`), so they
    // must coalesce into exactly ONE pending job for B — restore's enqueue
    // attempt finds revert's job still pending and is dropped by the
    // partial unique index, not stacked as a second row.
    let b_pending_jobs: Vec<(Uuid, serde_json::Value)> = sqlx::query_as(
        "SELECT id, payload FROM jobs \
         WHERE kind = 'PropagateCorrections' AND status = 'pending' AND account_id = $1",
    )
    .bind(probe.b.account_id)
    .fetch_all(&probe.owner)
    .await
    .expect("read B's pending PropagateCorrections jobs via the owner pool");
    assert_eq!(
        b_pending_jobs.len(),
        1,
        "revert then restore on the same source must coalesce into exactly ONE pending job for \
         B's account — the enqueued job must be stamped for B, the graph's REAL owner, never \
         A's active session account (CodeRabbit round 1, finding 1): {b_pending_jobs:?}"
    );
    let (b_job_id, b_job_payload) = &b_pending_jobs[0];
    assert_eq!(
        b_job_payload["graph_id"].as_str(),
        Some(probe.b.graph_id.to_string().as_str())
    );

    // A's own earlier relabel also enqueued a job — dequeue everything
    // pending (both) and process each, exactly as the real worker's poll
    // loop would; A's job propagating A's own correction is harmless and
    // orthogonal to what this assertion is about.
    let claimed = coralswarm_backend::jobs::queue::dequeue(&probe.owner, &["PropagateCorrections"], 10)
        .await
        .expect("dequeue pending PropagateCorrections jobs");
    assert!(
        claimed.iter().any(|j| j.id == *b_job_id),
        "B's job must be among the claimed batch: {claimed:?}"
    );
    for job in claimed {
        coralswarm_backend::workers::propagate_corrections::process_job(&probe.owner, None, job).await;
    }

    let content: Option<String> =
        sqlx::query_scalar("SELECT content_inline FROM atoms WHERE atom_id = $1")
            .bind(b_atom_id)
            .fetch_one(&probe.owner)
            .await
            .expect("read B's atom via the owner pool");
    assert_eq!(
        content.as_deref(),
        Some("Bob: B's private turn."),
        "B's atom must be rewritten by account-B-scoped propagation — if the WRONG (A's) \
         account were used instead, this RLS-scoped write would silently affect zero rows and \
         the content would never change (CodeRabbit round 1, finding 1)"
    );

    probe.teardown().await;
}
