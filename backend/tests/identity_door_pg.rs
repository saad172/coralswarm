//! Live end-to-end proof of the identity-resolution-at-the-door seam
//! (`docs/plans/identity-resolution-at-the-door.md` §10.2/§10.3, builder B1)
//! against a REAL Postgres — the full ladder, the principal/self-reef
//! contract, the mint provenance shape, and the merge-path ledger touch.
//!
//! Uses [`support::rls_probe::RlsProbe`] the same way every other `*_pg.rs`
//! suite in this directory does: a scratch database with the FULL control
//! migration tree applied (so `reef_identifiers`/`evaluation_ledger`/
//! `pg_trgm` are the real schema, not a hand-copy), an owner/BYPASSRLS pool
//! for the unscoped `Store` calls this suite drives (`upsert_node`,
//! `merge_nodes`, `resolve_reef_ref`, `mint_reef`, ... — none of these are
//! account-scoped RLS reads, so the owner pool is the correct connection,
//! matching `merge_unmerge_pg.rs`'s own precedent), and two ready-seeded
//! accounts (`probe.a`/`probe.b`) this suite doesn't need but the harness
//! always builds.
//!
//! Gated on `ORGT_TEST_BASE_URL`; a plain `cargo test` with no DB configured
//! skips every test here (never a `0 passed` false green — see the
//! `test -n` guard this repo's memory notes call out).
//!
//! Run with:
//!   ORGT_TEST_BASE_URL="postgres://alex@localhost:5432/postgres" \
//!     cargo test -p coralswarm-backend --test identity_door_pg

mod support;

use coralswarm_backend::relationships::{predicates, EdgeOrigin, EdgeStatus};
use coralswarm_backend::self_reef_backfill::{
    assert_policy_bypass, list_eligible_graphs, process_graph, GraphCase, Outcome, Totals,
};
use coralswarm_backend::store::{
    PostgresStore, ReefRefCtx, ReefResolution, ResolveHow, Store,
};
use serde_json::Value;
use support::rls_probe::RlsProbe;
use uuid::Uuid;

/// One fresh graph per scenario (own `store`/`account_id`), so the door's
/// trigram/permutation scans over "every entity reef in this graph" never
/// see another scenario's nodes.
async fn fresh_graph(store: &PostgresStore, probe: &RlsProbe, name: &str) -> Uuid {
    store
        .create_graph(probe.a.account_id, name, "personal")
        .await
        .expect("create scenario graph")
        .id
}

async fn node_body(pool: &sqlx::PgPool, node_id: Uuid) -> Value {
    sqlx::query_scalar::<_, Value>("SELECT body FROM nodes WHERE node_id = $1")
        .bind(node_id)
        .fetch_one(pool)
        .await
        .expect("node must exist")
}

async fn entity_count_by_lower_title(pool: &sqlx::PgPool, graph_id: Uuid, lower_title: &str) -> i64 {
    sqlx::query_scalar(
        "SELECT count(*) FROM nodes WHERE graph_id = $1 AND type = 'entity' AND lower(title) = $2",
    )
    .bind(graph_id)
    .bind(lower_title)
    .fetch_one(pool)
    .await
    .expect("count query")
}

/// (a) A title merged away and taught as an alias, re-resolved through
/// `upsert_node`, attaches to the survivor and creates no node — AND (g) the
/// negative control: with `DOOR_LADDER_DISABLE=1`, the SAME re-resolution
/// mints a second node instead. Folded into one test function (rather than
/// two) so the env-var-mutating negative control can never race another
/// `#[tokio::test]` in this binary reading the ladder in its normal,
/// enabled state — `DOOR_LADDER_DISABLE` is a process-global env var, and
/// Rust's default test harness runs functions in this file concurrently.
#[tokio::test]
async fn merged_alias_reresolves_without_a_new_node_and_the_negative_control_mints_one() {
    let Some(probe) = RlsProbe::setup("orgt_identity_door_alias").await else {
        eprintln!("skipping: set ORGT_TEST_BASE_URL");
        return;
    };
    let store = PostgresStore::new(probe.owner.clone());
    let g = fresh_graph(&store, &probe, "identity-door-alias").await;

    // Two fragments of the same person: the survivor, and "alex" — a real
    // fragment from the design doc's own §1 evidence table. Deliberately
    // NOT a token permutation of "Alex Example" (disjoint token sets,
    // {Alex,Alex} vs {alex}) — the door's step 4 already collapses a
    // permutation at MINT time (proven by
    // `permutation_step_resolves_reordered_person_names` below), so a title
    // this test can still merge as a genuine duplicate has to be a shape
    // step 4 does NOT catch: this scenario is specifically about the
    // ALIAS a merge teaches (step 3), not the permutation step.
    let keep = store.upsert_node(g, "entity", "Alex Example").await.expect("mint keep");
    let dup = store.upsert_node(g, "entity", "alex").await.expect("mint dup");
    assert_ne!(keep, dup, "disjoint token sets must mint as two separate reefs pre-merge");

    store.merge_nodes(keep, dup).await.expect("merge_nodes teaches the alias");

    let dup_gone: i64 = sqlx::query_scalar("SELECT count(*) FROM nodes WHERE node_id = $1")
        .bind(dup)
        .fetch_one(&probe.owner)
        .await
        .unwrap();
    assert_eq!(dup_gone, 0, "merge must remove the duplicate");

    // ── (a): ladder enabled — re-resolves, no new node ──────────────────
    let resolved_again = store
        .upsert_node(g, "entity", "alex")
        .await
        .expect("re-mention must resolve through the alias, not mint");
    assert_eq!(
        resolved_again, keep,
        "the merged title, re-extracted, must attach to the survivor"
    );
    let entity_total: i64 = sqlx::query_scalar("SELECT count(*) FROM nodes WHERE graph_id = $1 AND type = 'entity'")
        .bind(g)
        .fetch_one(&probe.owner)
        .await
        .unwrap();
    assert_eq!(entity_total, 1, "no second node must exist for the merged title");

    // ── (g) negative control: disable steps 2-4, the SAME mention mints ──
    // The survivor here is intentionally low-mass (0 corals, not the self
    // reef) so step 5's near-match gate — which `DOOR_LADDER_DISABLE` does
    // NOT touch — never becomes consequential and can't mask the control.
    std::env::set_var("DOOR_LADDER_DISABLE", "1");
    let minted_again = store
        .upsert_node(g, "entity", "alex")
        .await
        .expect("with the ladder disabled this must still succeed, by MINTING");
    std::env::remove_var("DOOR_LADDER_DISABLE");

    assert_ne!(
        minted_again, keep,
        "a test that cannot fail has not been run: with the ladder disabled, \
         the alias step must NOT fire, so a new node is minted"
    );
    let entity_total_after: i64 = entity_count_by_lower_title(&probe.owner, g, "alex").await
        + entity_count_by_lower_title(&probe.owner, g, "Alex Example").await;
    assert_eq!(
        entity_total_after, 2,
        "the negative control must have created a second node"
    );

    probe.teardown().await;
}

/// (b) "Alex Alex" against an existing "Alex Example" (both
/// `entity_type=person`) resolves at the permutation step, with no LLM call
/// (there is none to call — `resolve_reef_ref` is pure store logic).
#[tokio::test]
async fn permutation_step_resolves_reordered_person_names() {
    let Some(probe) = RlsProbe::setup("orgt_identity_door_perm").await else {
        eprintln!("skipping: set ORGT_TEST_BASE_URL");
        return;
    };
    let store = PostgresStore::new(probe.owner.clone());
    let g = fresh_graph(&store, &probe, "identity-door-perm").await;

    let existing = store.upsert_node(g, "entity", "Alex Example").await.expect("mint");
    store
        .record_entity_type_hint(existing, "person")
        .await
        .expect("hint entity_type");

    let ctx = ReefRefCtx {
        entity_type: Some("person".to_string()),
        worker: "test",
        ..Default::default()
    };
    let resolution = store
        .resolve_reef_ref(g, "entity", "Alex Alex", &ctx)
        .await
        .expect("resolve_reef_ref");

    match resolution {
        ReefResolution::Resolved { node_id, how } => {
            assert_eq!(node_id, existing);
            assert_eq!(how, ResolveHow::Permutation);
        }
        other => panic!("expected a Permutation resolve, got {other:?}"),
    }

    probe.teardown().await;
}

/// (c) An email-shaped mention resolves via `reef_identifiers` (step 2).
#[tokio::test]
async fn identifier_shaped_mention_resolves_via_reef_identifiers() {
    let Some(probe) = RlsProbe::setup("orgt_identity_door_ident").await else {
        eprintln!("skipping: set ORGT_TEST_BASE_URL");
        return;
    };
    let store = PostgresStore::new(probe.owner.clone());
    let g = fresh_graph(&store, &probe, "identity-door-ident").await;

    let node = store.upsert_node(g, "entity", "Someone Else").await.expect("mint");
    store
        .record_reef_identifier(g, node, "email", "user@example.com", "test")
        .await
        .expect("record identifier");

    let ctx = ReefRefCtx {
        worker: "test",
        ..Default::default()
    };
    let resolution = store
        .resolve_reef_ref(g, "entity", "User@Example.com", &ctx)
        .await
        .expect("resolve_reef_ref");

    match resolution {
        ReefResolution::Resolved { node_id, how } => {
            assert_eq!(node_id, node);
            assert_eq!(how, ResolveHow::Identifier);
        }
        other => panic!("expected an Identifier resolve, got {other:?}"),
    }

    probe.teardown().await;
}

/// (d) `is_author`/author-label binds to `author_reef_id` by id, and is
/// Rejected when no author reef exists.
#[tokio::test]
async fn author_mention_binds_by_id_and_rejects_with_no_author_reef() {
    let Some(probe) = RlsProbe::setup("orgt_identity_door_author").await else {
        eprintln!("skipping: set ORGT_TEST_BASE_URL");
        return;
    };
    let store = PostgresStore::new(probe.owner.clone());
    let g = fresh_graph(&store, &probe, "identity-door-author").await;

    let author_reef = store.upsert_node(g, "entity", "Alex Example").await.expect("mint author reef");

    // is_author=true, author_reef_id known → binds by id regardless of the
    // mention string (an entirely different spelling on purpose, to prove
    // this is an ID bind, not a string match).
    let ctx_bound = ReefRefCtx {
        is_author: true,
        author_reef_id: Some(author_reef),
        worker: "test",
        ..Default::default()
    };
    let resolution = store
        .resolve_reef_ref(g, "entity", "person@example.com", &ctx_bound)
        .await
        .expect("resolve_reef_ref");
    match resolution {
        ReefResolution::Resolved { node_id, how } => {
            assert_eq!(node_id, author_reef);
            assert_eq!(how, ResolveHow::Author);
        }
        other => panic!("expected an Author resolve, got {other:?}"),
    }

    // author_label equality (not is_author) with NO author reef → Rejected.
    let ctx_unbound = ReefRefCtx {
        author_label: Some("person@example.com".to_string()),
        author_reef_id: None,
        worker: "test",
        ..Default::default()
    };
    let resolution = store
        .resolve_reef_ref(g, "entity", "person@example.com", &ctx_unbound)
        .await
        .expect("resolve_reef_ref");
    match resolution {
        ReefResolution::Rejected(reason) => {
            assert!(reason.contains("author"), "{reason}");
        }
        other => panic!("expected Rejected, got {other:?}"),
    }

    probe.teardown().await;
}

/// (e) `mint_reef` records `body.born`/`body.provisional`/`body.entity_type`.
#[tokio::test]
async fn mint_reef_records_provenance_and_provisional_flag() {
    let Some(probe) = RlsProbe::setup("orgt_identity_door_mint").await else {
        eprintln!("skipping: set ORGT_TEST_BASE_URL");
        return;
    };
    let store = PostgresStore::new(probe.owner.clone());
    let g = fresh_graph(&store, &probe, "identity-door-mint").await;

    let atom_id: Uuid = sqlx::query_scalar(
        "INSERT INTO atoms(graph_id, source_type, source_id, content_blob_key, timestamp) \
         VALUES ($1, 'test', 'mint-1', 'blob-key', now()) RETURNING atom_id",
    )
    .bind(g)
    .fetch_one(&probe.owner)
    .await
    .expect("seed atom");

    let ctx = ReefRefCtx {
        atom_id: Some(atom_id),
        entity_type: Some("person".to_string()),
        worker: "test_worker",
        model: Some("test-model".to_string()),
        surface_form: Some("Brand New Person".to_string()),
        ..Default::default()
    };
    let node_id = store
        .mint_reef(g, "entity", "Brand New Person", &ctx)
        .await
        .expect("mint_reef");

    let body = node_body(&probe.owner, node_id).await;
    assert_eq!(body["provisional"], Value::Bool(true), "body: {body}");
    assert_eq!(body["entity_type"], Value::String("person".to_string()), "body: {body}");
    assert_eq!(body["born"]["worker"], Value::String("test_worker".to_string()), "body: {body}");
    assert_eq!(body["born"]["model"], Value::String("test-model".to_string()), "body: {body}");
    assert_eq!(
        body["born"]["surface_form"],
        Value::String("Brand New Person".to_string()),
        "body: {body}"
    );
    assert_eq!(body["born"]["atom_id"], Value::String(atom_id.to_string()), "body: {body}");
    assert!(body["born"]["near_candidates"].is_array(), "body: {body}");

    probe.teardown().await;
}

/// (f) A merge clears the `evaluation_ledger` rows for topics adjacent to
/// the keep (works_on/responsible_for/mentions), and clears
/// `body.provisional` on the keep.
#[tokio::test]
async fn merge_clears_adjacent_evaluation_ledger_and_provisional_flag() {
    let Some(probe) = RlsProbe::setup("orgt_identity_door_ledger").await else {
        eprintln!("skipping: set ORGT_TEST_BASE_URL");
        return;
    };
    let store = PostgresStore::new(probe.owner.clone());
    let g = fresh_graph(&store, &probe, "identity-door-ledger").await;

    let ctx = ReefRefCtx {
        entity_type: Some("person".to_string()),
        worker: "test",
        ..Default::default()
    };
    let keep = store
        .mint_reef(g, "entity", "Provisional Person", &ctx)
        .await
        .expect("mint keep as provisional");
    let dup = store.upsert_node(g, "entity", "Provisional Person Duplicate").await.expect("mint dup");
    let topic = store.upsert_node(g, "entity", "CoralSwarm").await.expect("mint topic");

    store
        .upsert_relationship_edge(
            g,
            keep,
            predicates::WORKS_ON,
            topic,
            EdgeOrigin::Stated,
            EdgeStatus::Confirmed,
            0.9,
            serde_json::json!({ "atom_ids": [] }),
        )
        .await
        .expect("seed works_on edge");

    store
        .upsert_evaluation_ledger(g, topic, predicates::EXPERT_IN, Some(0.4))
        .await
        .expect("seed a stale evaluation-ledger row");

    let before = store
        .list_evaluation_ledger(g, predicates::EXPERT_IN)
        .await
        .expect("list ledger");
    assert!(before.iter().any(|(id, _)| *id == topic), "ledger row must exist before the merge");

    let body_before = node_body(&probe.owner, keep).await;
    assert_eq!(body_before["provisional"], Value::Bool(true));

    store.merge_nodes(keep, dup).await.expect("merge_nodes");

    let after = store
        .list_evaluation_ledger(g, predicates::EXPERT_IN)
        .await
        .expect("list ledger");
    assert!(
        !after.iter().any(|(id, _)| *id == topic),
        "the adjacent evaluation_ledger row must be cleared by the merge"
    );

    let body_after = node_body(&probe.owner, keep).await;
    assert!(
        body_after.get("provisional").is_none(),
        "body.provisional must be cleared on the keep after a merge: {body_after}"
    );

    probe.teardown().await;
}

/// (i) `self_reef_backfill`'s "existing self reef" gap fix
/// (`docs/plans/identity-resolution-at-the-door.md` §10.3's follow-up, the
/// primary-ocean `93cc446a` shape): a graph whose self reef was created
/// BEFORE identifiers existed (Slack ingest's own `ensure_self_node` call,
/// pre-B1) has no `reef_identifiers` row at all. The backfill's
/// `ExistingSelfReef` case must (a) select it, (b) bind `clerk_user` onto
/// the self reef with no creation/retitling, (c) detect that the owner's
/// email is already held by a DIFFERENT node (an owner fragment) and
/// report the collision WITHOUT moving the email, and (d) be idempotent —
/// a second pass selects nothing.
#[tokio::test]
async fn existing_self_reef_binds_clerk_user_and_reports_an_email_collision_without_moving_it() {
    let Some(probe) = RlsProbe::setup("orgt_self_reef_backfill_existing").await else {
        eprintln!("skipping: set ORGT_TEST_BASE_URL");
        return;
    };
    let store = PostgresStore::new(probe.owner.clone());
    let g = fresh_graph(&store, &probe, "self-reef-backfill-existing").await;

    // `probe.a.user_id` is also `accounts.owner_user_id` (seed_account) —
    // the same owner-fallback path a real primary ocean resolves through.
    // Stamp it with the identity a real owner has: name left NULL (this
    // case never resolves a title), a verified email, and a Clerk id.
    let owner_email = "person@example.com";
    let clerk_id = "user_test_existing_self_reef";
    sqlx::query("UPDATE users SET email = $1, clerk_user_id = $2 WHERE id = $3")
        .bind(owner_email)
        .bind(clerk_id)
        .bind(probe.a.user_id)
        .execute(&probe.owner)
        .await
        .expect("stamp owner identity");

    // A self reef that predates identifiers — minted directly and pointed
    // at by `graphs.self_node_id`, the way pre-door Slack ingest left it.
    let self_node = store.upsert_node(g, "entity", "Alex Example").await.expect("mint self node");
    sqlx::query("UPDATE graphs SET self_node_id = $1 WHERE id = $2")
        .bind(self_node)
        .bind(g)
        .execute(&probe.owner)
        .await
        .expect("stamp self_node_id");

    // A DIFFERENT node already holding the owner's email — an owner
    // fragment born before the door existed (this graph's own "Alex" node
    // in the design doc's evidence table).
    let fragment = store.upsert_node(g, "entity", "Alex").await.expect("mint fragment node");
    sqlx::query(
        "INSERT INTO reef_identifiers (graph_id, node_id, kind, value_norm, source) \
         VALUES ($1, $2, 'email', $3, 'extraction')",
    )
    .bind(g)
    .bind(fragment)
    .bind(owner_email)
    .execute(&probe.owner)
    .await
    .expect("seed the fragment's email identifier");

    // Give the fragment a nonzero degree so the collision report's edge
    // count isn't vacuously 0 — a test that can't distinguish 0 from 1
    // hasn't proven the count is wired up.
    let topic = store.upsert_node(g, "entity", "CoralSwarm").await.expect("mint topic");
    store
        .upsert_relationship_edge(
            g,
            fragment,
            predicates::MENTIONS,
            topic,
            EdgeOrigin::Stated,
            EdgeStatus::Confirmed,
            0.9,
            serde_json::json!({ "atom_ids": [] }),
        )
        .await
        .expect("seed an edge on the fragment");

    // ── Selection: ExistingSelfReef, not NoSelfReef ─────────────────────
    let eligible =
        list_eligible_graphs(&probe.owner, Some(g)).await.expect("list eligible graphs");
    assert_eq!(
        eligible.len(),
        1,
        "the graph must be selected: self reef exists but lacks a clerk_user identifier"
    );
    assert_eq!(eligible[0].case, GraphCase::ExistingSelfReef { self_node_id: self_node });

    // ── Dry run: predicts the collision, writes nothing ─────────────────
    let mut dry_totals = Totals::default();
    let dry_report = process_graph(&probe.owner, &eligible[0], true, false, &mut dry_totals).await;
    match &dry_report.outcome {
        Outcome::ExistingSelfReef { clerk_user, email, email_collision, .. } => {
            assert!(*clerk_user, "clerk_user must be reported as would_bind");
            assert!(!*email, "email must NOT be reported as would_bind — it collides");
            let c = email_collision.as_ref().expect("a collision must be reported");
            assert_eq!(c.node_id, fragment);
            assert_eq!(c.title, "Alex");
            assert_eq!(c.degree, 1, "the fragment's one seeded edge must be counted");
            assert!(
                c.report_line().contains("merge_reefs"),
                "the collision line must name the merge operator: {}",
                c.report_line()
            );
        }
        other => panic!("expected ExistingSelfReef outcome, got {other:?}"),
    }
    assert_eq!(dry_totals.email_collisions, 1, "a dry run must still surface the collision");
    assert_eq!(dry_totals.existing_bound, 0, "a dry run must not count a write");

    let clerk_after_dry_run: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM reef_identifiers WHERE node_id = $1 AND kind = 'clerk_user'",
    )
    .bind(self_node)
    .fetch_one(&probe.owner)
    .await
    .unwrap();
    assert_eq!(clerk_after_dry_run, 0, "dry run must write nothing");

    // ── Execute: clerk_user binds onto the self reef, email stays put ───
    let mut totals = Totals::default();
    let report = process_graph(&probe.owner, &eligible[0], true, true, &mut totals).await;
    match &report.outcome {
        Outcome::ExistingSelfReef { clerk_user, email, email_collision, .. } => {
            assert!(*clerk_user);
            assert!(!*email);
            assert!(email_collision.is_some());
        }
        other => panic!("expected ExistingSelfReef outcome, got {other:?}"),
    }
    assert_eq!(totals.existing_bound, 1);
    assert_eq!(totals.email_collisions, 1);

    let self_clerk: Option<String> = sqlx::query_scalar(
        "SELECT value_norm FROM reef_identifiers WHERE node_id = $1 AND kind = 'clerk_user'",
    )
    .bind(self_node)
    .fetch_optional(&probe.owner)
    .await
    .unwrap();
    assert_eq!(self_clerk.as_deref(), Some(clerk_id), "clerk_user must bind onto the self reef");

    let self_email: Option<String> = sqlx::query_scalar(
        "SELECT value_norm FROM reef_identifiers WHERE node_id = $1 AND kind = 'email'",
    )
    .bind(self_node)
    .fetch_optional(&probe.owner)
    .await
    .unwrap();
    assert_eq!(self_email, None, "the email must NOT move onto the self reef");

    let fragment_email: Option<String> = sqlx::query_scalar(
        "SELECT value_norm FROM reef_identifiers WHERE node_id = $1 AND kind = 'email'",
    )
    .bind(fragment)
    .fetch_optional(&probe.owner)
    .await
    .unwrap();
    assert_eq!(
        fragment_email.as_deref(),
        Some(owner_email),
        "the fragment must keep the email — it is never moved by this backfill"
    );

    let self_title: String = sqlx::query_scalar("SELECT title FROM nodes WHERE node_id = $1")
        .bind(self_node)
        .fetch_one(&probe.owner)
        .await
        .unwrap();
    assert_eq!(self_title, "Alex Example", "the self reef must never be retitled by this case");

    // ── Idempotent: a second pass selects nothing ───────────────────────
    let eligible_again =
        list_eligible_graphs(&probe.owner, Some(g)).await.expect("list eligible graphs again");
    assert!(
        eligible_again.is_empty(),
        "a re-run must select nothing once clerk_user is bound on the self reef"
    );

    probe.teardown().await;
}

/// The operator bins' RLS-bypass preflight: PASSES for the elevated owner
/// role every other test here uses, and REFUSES for the real least-privilege
/// `coralswarm_app` login the fixture provides. Without the second half the
/// preflight could be a function that always returns `Ok` and nothing would
/// notice — the exact failure mode it exists to prevent (a backfill that
/// reports "nothing to do" because the policy hid every row).
#[tokio::test]
async fn preflight_passes_for_owner_and_refuses_the_app_role() {
    let Some(probe) = RlsProbe::setup("orgt_identity_door_preflight").await else {
        eprintln!("skipping: set ORGT_TEST_BASE_URL");
        return;
    };

    assert_policy_bypass(&probe.owner)
        .await
        .expect("the owner/BYPASSRLS pool must pass the preflight");

    let refused = assert_policy_bypass(&probe.app_pool).await;
    let msg = refused.expect_err("the coralswarm_app login must be REFUSED by the preflight");
    assert!(
        msg.contains("REFUSING") && msg.contains("nodes"),
        "refusal must name the table and be operator-readable, got: {msg}"
    );

    probe.teardown().await;
}

/// Typo-distance pairs are NOT the same reef at the door: "Ada Aster" /
/// "Ada Astor", "Dev Dunn" / "Dev Dunne", "Cy Cortez" / "Cy Cortes" must
/// mint as two reefs each (they are ER's job to adjudicate with evidence,
/// not the door's to fold on spelling). Pinned because
/// judgement_graph_worker_pg's merge-review fixture seeds exactly these
/// shapes and relies on getting two endpoints back.
#[tokio::test]
async fn typo_distance_pairs_stay_separate_reefs_at_the_door() {
    let Some(probe) = RlsProbe::setup("orgt_identity_door_typo_pairs").await else {
        eprintln!("skipping: set ORGT_TEST_BASE_URL");
        return;
    };
    let store = PostgresStore::new(probe.owner.clone());
    let g = fresh_graph(&store, &probe, "identity-door-typo-pairs").await;
    for (x, y) in [("Ada Aster", "Ada Astor"), ("Bo Brandt", "Bo Brandtt"), ("Cy Cortez", "Cy Cortes"), ("Dev Dunn", "Dev Dunne"), ("Eli Ebert", "Eli Ebbert")] {
        let a = store.upsert_node(g, "entity", x).await.expect("mint x");
        let how = store
            .resolve_reef_ref(g, "entity", y, &ReefRefCtx::default())
            .await
            .expect("resolve y");
        let b = store.upsert_node(g, "entity", y).await.expect("mint y");
        assert_ne!(a, b, "{x:?} / {y:?} must be two reefs; resolve_reef_ref({y:?}) gave {how:?}");
    }
    // …whereas a whitespace/case variant of an existing title IS the same
    // reef (step 3): this is the fold that made judgement_graph_worker_pg's
    // old "Ken Kruse " fixture collapse, and it is intended.
    let k = store.upsert_node(g, "entity", "Ken Kruse").await.expect("mint");
    let k2 = store.upsert_node(g, "entity", "Ken Kruse ").await.expect("mint variant");
    let k3 = store.upsert_node(g, "entity", "ken  kruse").await.expect("mint variant 2");
    assert_eq!(k, k2, "a trailing-space variant must resolve to the existing reef");
    assert_eq!(k, k3, "a case/double-space variant must resolve to the existing reef");
    probe.teardown().await;
}
