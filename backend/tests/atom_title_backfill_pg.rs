//! Actual-role contract for the atom-title sweep while it is switched off.
//!
//! BackfillAtomTitles runs on `managed_enrichment` (source-agnostic plan PR 7,
//! tenant 0171 / control 0338): typed queue admission now keeps the job
//! pending, and the worker claims and spends only while
//! `ENRICHMENT_PROCESSING_ENABLED=true` and `ATOM_TITLES_ENABLED` is not off.
//! This suite runs with the shared switch OFF (its default) and proves a
//! directly-invoked worker still does nothing: the job completes, a child
//! hands its same cursor back, a seed fans out no child, and no atom body,
//! title, kind or provenance changes, with zero provider work and zero
//! charges, under the real app role and RLS isolation. The managed path
//! (one call per batch, receipts, stale inputs, bounds, single billing) is
//! proven in `managed::gate::pg_tests::enrichment_consumers`.
mod support;
use coralswarm_backend::jobs::{
    JobKind,
    queue::{self, JobRow},
};
use coralswarm_backend::llm::{
    CompletionRequest, CompletionResponse, LlmError, LlmProvider, StreamEvent,
};
use coralswarm_backend::tenancy::{TenantId, scope::begin_account_scoped};
use coralswarm_backend::workers::atom_title_backfill;
use futures::Stream;
use serde_json::{Value, json};
use sqlx::Row;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use support::rls_probe::RlsProbe;
use uuid::Uuid;

#[derive(Default)]
struct RefusalSpy(AtomicUsize);
#[async_trait::async_trait]
impl LlmProvider for RefusalSpy {
    async fn complete(&self, _: CompletionRequest) -> Result<CompletionResponse, LlmError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        panic!("a switched-off title sweep must never invoke the provider")
    }
    async fn stream_complete(
        &self,
        _: CompletionRequest,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<StreamEvent, LlmError>> + Send>>, LlmError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        panic!("a switched-off title sweep must never stream")
    }
}

/// Finance is genuinely available, so zero charges are not an empty-pool refusal.
async fn fund(probe: &RlsProbe) {
    for a in [&probe.a, &probe.b] {
        sqlx::query("INSERT INTO account_members(account_id,user_id,role) VALUES($1,$2,'owner') ON CONFLICT DO NOTHING")
            .bind(a.account_id).bind(a.user_id).execute(&probe.owner).await.unwrap();
        let mut tx = begin_account_scoped(&probe.owner, a.account_id)
            .await
            .unwrap();
        coralswarm_backend::billing::renewal::lock_period_rotation(&mut tx, a.account_id)
            .await
            .unwrap();
        sqlx::query("INSERT INTO credit_balances(account_id,balance) VALUES($1,1000000) ON CONFLICT(account_id) DO UPDATE SET balance=excluded.balance")
            .bind(a.account_id).execute(&mut *tx).await.unwrap();
        tx.commit().await.unwrap();
    }
}
async fn atoms(probe: &RlsProbe) -> Value {
    sqlx::query_scalar("SELECT jsonb_agg(to_jsonb(a) ORDER BY atom_id) FROM atoms a")
        .fetch_one(&probe.owner)
        .await
        .unwrap()
}
async fn finances(probe: &RlsProbe) -> Value {
    sqlx::query_scalar("SELECT jsonb_build_object('balances',(SELECT jsonb_agg(jsonb_build_array(account_id,balance) ORDER BY account_id) FROM credit_balances),'attempts',(SELECT count(*) FROM managed_attempt_receipts),'reservations',(SELECT count(*) FROM managed_credit_reservations),'usage',(SELECT count(*) FROM llm_usage))")
        .fetch_one(&probe.owner).await.unwrap()
}
/// Persist a real typed queue job, verify it is admitted pending, then force
/// its real worker entry with the managed switch off: a direct invocation
/// cannot bypass the switch.
async fn refused(probe: &RlsProbe, account: Option<Uuid>, spy: Option<&RefusalSpy>) -> Uuid {
    std::env::remove_var("ENRICHMENT_PROCESSING_ENABLED");
    fund(probe).await;
    let privileges:(bool,bool,i64)=sqlx::query_as("SELECT rolsuper,rolbypassrls,(SELECT count(*) FROM pg_tables WHERE schemaname='public' AND tableowner=current_user) FROM pg_roles WHERE rolname=current_user")
        .fetch_one(&probe.app_pool).await.unwrap();
    assert_eq!(privileges, (false, false, 0));
    let before = atoms(probe).await;
    assert!(
        before.as_array().unwrap().len() >= 2,
        "both accounts have real retained atoms"
    );
    let money = finances(probe).await;
    assert_eq!(money["balances"].as_array().unwrap().len(), 2);
    assert!(
        money["balances"]
            .as_array()
            .unwrap()
            .iter()
            .all(|r| r[1] == 1_000_000)
    );
    assert_eq!(money["attempts"], 0);
    assert_eq!(money["reservations"], 0);
    assert_eq!(money["usage"], 0);
    // The ordinary app role still sees its own concrete atom and denies B.
    probe
        .assert_scoped_returns_own_rows(
            probe.a.account_id,
            "SELECT atom_id FROM atoms WHERE atom_id=$1",
            &[probe.a.atom_id],
            &[probe.a.atom_id],
        )
        .await;
    probe
        .assert_cross_account_returns_nothing(
            probe.a.account_id,
            "SELECT atom_id FROM atoms WHERE atom_id=$1",
            &[probe.b.atom_id],
        )
        .await;
    probe
        .assert_unscoped_hard_errors("SELECT atom_id FROM atoms", &[])
        .await;
    let kind = JobKind::BackfillAtomTitles {
        after_atom_id: None,
    };
    let id = queue::enqueue(&probe.app_pool, &kind, account, None, 0)
        .await
        .unwrap()
        .unwrap();
    let queued: (String, Option<String>, Option<Uuid>) =
        sqlx::query_as("SELECT status::text,error_message,account_id FROM jobs WHERE id=$1")
            .bind(id)
            .fetch_one(&probe.owner)
            .await
            .unwrap();
    assert_eq!(queued, ("pending".into(), None, account), "managed enrichment: admitted, not dead-lettered");
    // Claimed, as `queue::dequeue` leaves it: a hold acts on a running row.
    sqlx::query("UPDATE jobs SET status='running',started_at=now() WHERE id=$1")
        .bind(id)
        .execute(&probe.owner)
        .await
        .unwrap();
    atom_title_backfill::process_job(
        &TenantId::Consumer,
        &probe.app_pool,
        &probe.app_pool,
        spy.map(|s| s as &dyn LlmProvider),
        None,
        JobRow {
            id,
            kind: "BackfillAtomTitles".into(),
            payload: json!({"after_atom_id":null}),
            account_id: account,
            attempts: 0,
        },
    )
    .await;
    let finished: (String, Option<String>) = sqlx::query_as("SELECT status::text,error_message FROM jobs WHERE id=$1")
        .bind(id)
        .fetch_one(&probe.owner)
        .await
        .unwrap();
    let fresh: Vec<(Option<Uuid>, Value)> = sqlx::query_as(
        "SELECT account_id,payload FROM jobs WHERE kind='BackfillAtomTitles' AND status='pending' AND id<>$1",
    )
    .bind(id)
    .fetch_all(&probe.owner)
    .await
    .unwrap();
    assert!(fresh.is_empty(), "no fresh row: a child keeps its cursor on the same job, a seed fans out nothing");
    match account {
        // Held in place under the shared retry rule (`jobs::queue::hold`).
        Some(_) => assert_eq!(finished, ("pending".into(), Some("held: switched_off".into())), "switched off: the child is held in place, never dead-lettered"),
        None => assert_eq!(finished, ("done".into(), None), "switched off: the seed completes and fans out nothing"),
    }
    sqlx::query("DELETE FROM jobs WHERE kind='BackfillAtomTitles' AND status='pending'").execute(&probe.owner).await.unwrap();
    assert_eq!(
        atoms(probe).await,
        before,
        "no own/other atom body, title, kind or provenance may change"
    );
    assert_eq!(
        finances(probe).await,
        money,
        "refusal reserves/spends no finite credits and writes no usage"
    );
    if let Some(spy) = spy {
        assert_eq!(spy.0.load(Ordering::SeqCst), 0);
    }
    id
}

/// Insert one atom directly as the OWNER (setup, not under test).
async fn seed_atom(
    owner: &sqlx::PgPool,
    graph_id: Uuid,
    source_id: &str,
    content: &str,
    header_len: Option<i32>,
) -> Uuid {
    sqlx::query_scalar(
        "INSERT INTO atoms(graph_id, source_type, source_id, content_blob_key, \
                           content_inline, header_len, timestamp) \
         VALUES ($1, 'note', $2, 'blob', $3, $4, now()) RETURNING atom_id",
    )
    .bind(graph_id)
    .bind(source_id)
    .bind(content)
    .bind(header_len)
    .fetch_one(owner)
    .await
    .expect("seed atom")
}

/// Insert one atom directly as the OWNER with an EXPLICIT `title`/`title_kind`
/// (setup, not under test) — used to seed a shape `seed_atom` cannot produce:
/// a coral that already has a persisted title, simulating either a row a
/// prior sweep pass already touched, or (with `title_kind: None`) a row
/// titled before migration `0212_atoms_title_kind`/`0080_atoms_title_kind`
/// ever existed.
async fn seed_atom_with_title(
    owner: &sqlx::PgPool,
    graph_id: Uuid,
    source_id: &str,
    content: &str,
    title: Option<&str>,
    title_kind: Option<&str>,
) -> Uuid {
    sqlx::query_scalar(
        "INSERT INTO atoms(graph_id, source_type, source_id, content_blob_key, \
                           content_inline, timestamp, title, title_kind) \
         VALUES ($1, 'note', $2, 'blob', $3, now(), $4, $5) RETURNING atom_id",
    )
    .bind(graph_id)
    .bind(source_id)
    .bind(content)
    .bind(title)
    .bind(title_kind)
    .fetch_one(owner)
    .await
    .expect("seed atom with title")
}

async fn title_of(owner: &sqlx::PgPool, atom_id: Uuid) -> Option<String> {
    sqlx::query("SELECT title FROM atoms WHERE atom_id = $1")
        .bind(atom_id)
        .fetch_one(owner)
        .await
        .expect("read title")
        .get::<Option<String>, _>("title")
}

async fn title_kind_of(owner: &sqlx::PgPool, atom_id: Uuid) -> Option<String> {
    sqlx::query("SELECT title_kind FROM atoms WHERE atom_id = $1")
        .bind(atom_id)
        .fetch_one(owner)
        .await
        .expect("read title_kind")
        .get::<Option<String>, _>("title_kind")
}

#[tokio::test]
async fn switched_off_sweep_preserves_header_plain_and_other_account_atoms() {
    let Some(probe) = RlsProbe::setup("orgt_atom_title_sweep").await else {
        return;
    };
    let header = "Session: Ship member-private oceans. Repo: /x/coralswarm. Branch: main.\n\n";
    let with_header = seed_atom(
        &probe.owner,
        probe.a.graph_id,
        "a-with-header",
        &format!("{header}Merge train finished, main at cb1a479."),
        Some(header.len() as i32),
    )
    .await;
    let plain = seed_atom(
        &probe.owner,
        probe.a.graph_id,
        "a-plain",
        "Scoped the retrieval core under consumer RLS.",
        None,
    )
    .await;
    let header_only = seed_atom(
        &probe.owner,
        probe.a.graph_id,
        "a-header-only",
        header,
        Some(header.len() as i32),
    )
    .await;
    let other = seed_atom(
        &probe.owner,
        probe.b.graph_id,
        "b-plain",
        "A different account's coral entirely.",
        None,
    )
    .await;
    let spy = RefusalSpy::default();
    refused(&probe, Some(probe.a.account_id), Some(&spy)).await;
    for id in [with_header, plain, header_only, other] {
        assert_eq!(title_of(&probe.owner, id).await, None);
    }
    probe.teardown().await;
}
#[tokio::test]
async fn repeating_switched_off_sweeps_preserves_an_existing_headline() {
    let Some(probe) = RlsProbe::setup("orgt_atom_title_rerun").await else {
        return;
    };
    let atom = seed_atom_with_title(
        &probe.owner,
        probe.a.graph_id,
        "a-1",
        "Original body about the deploy.",
        Some("First title"),
        Some("llm"),
    )
    .await;
    let spy = RefusalSpy::default();
    for _ in 0..2 {
        refused(&probe, Some(probe.a.account_id), Some(&spy)).await;
        assert_eq!(
            title_of(&probe.owner, atom).await.as_deref(),
            Some("First title")
        );
        assert_eq!(
            title_kind_of(&probe.owner, atom).await.as_deref(),
            Some("llm")
        );
    }
    probe.teardown().await;
}
#[tokio::test]
async fn switched_off_sweep_without_a_provider_does_not_enter_the_fallback_batch() {
    let Some(probe) = RlsProbe::setup("orgt_atom_title_fallback").await else {
        return;
    };
    let atom = seed_atom(
        &probe.owner,
        probe.a.graph_id,
        "a-1",
        "Scoped the retrieval core under consumer RLS. Then deployed v193.",
        None,
    )
    .await;
    refused(&probe, Some(probe.a.account_id), None).await;
    assert_eq!(title_of(&probe.owner, atom).await, None);
    probe.teardown().await;
}
#[tokio::test]
async fn switched_off_accountless_seed_has_no_account_fanout_or_provider_work() {
    let Some(probe) = RlsProbe::setup("orgt_atom_title_seed").await else {
        return;
    };
    let spy = RefusalSpy::default();
    let seed = refused(&probe, None, Some(&spy)).await;
    let rows: Vec<(Uuid, Option<Uuid>, String)> = sqlx::query_as(
        "SELECT id,account_id,status::text FROM jobs WHERE kind='BackfillAtomTitles'",
    )
    .fetch_all(&probe.owner)
    .await
    .unwrap();
    assert_eq!(
        rows,
        vec![(seed, None, "done".into())],
        "both funded accounts have real atoms, but a switched-off seed mints no child"
    );
    probe.teardown().await;
}
#[tokio::test]
async fn switched_off_sweep_preserves_synchronous_fallback_with_and_without_a_provider() {
    let Some(probe) = RlsProbe::setup("orgt_atom_title_upgrade").await else {
        return;
    };
    let atom = seed_atom_with_title(
        &probe.owner,
        probe.a.graph_id,
        "a-1",
        "Deployed the ingest-time fallback title write. Then verified prod.",
        Some("Deployed the ingest-time fallback title write"),
        Some("fallback"),
    )
    .await;
    refused(&probe, Some(probe.a.account_id), None).await;
    let spy = RefusalSpy::default();
    refused(&probe, Some(probe.a.account_id), Some(&spy)).await;
    assert_eq!(
        title_of(&probe.owner, atom).await.as_deref(),
        Some("Deployed the ingest-time fallback title write")
    );
    assert_eq!(
        title_kind_of(&probe.owner, atom).await.as_deref(),
        Some("fallback")
    );
    probe.teardown().await;
}
#[tokio::test]
async fn switched_off_sweep_preserves_a_normalized_legacy_fallback() {
    let Some(probe) = RlsProbe::setup("orgt_atom_title_legacy").await else {
        return;
    };
    let atom = seed_atom_with_title(
        &probe.owner,
        probe.a.graph_id,
        "a-legacy",
        "Legacy body behind a pre-title_kind fallback title.",
        Some("Some pre-column fallback title"),
        None,
    )
    .await;
    assert_eq!(title_kind_of(&probe.owner, atom).await, None);
    sqlx::query(
        "UPDATE atoms SET title_kind = 'fallback' WHERE title IS NOT NULL AND title_kind IS NULL",
    )
    .execute(&probe.owner)
    .await
    .unwrap();
    assert_eq!(
        title_kind_of(&probe.owner, atom).await.as_deref(),
        Some("fallback")
    );
    let spy = RefusalSpy::default();
    refused(&probe, Some(probe.a.account_id), Some(&spy)).await;
    assert_eq!(
        title_of(&probe.owner, atom).await.as_deref(),
        Some("Some pre-column fallback title")
    );
    assert_eq!(
        title_kind_of(&probe.owner, atom).await.as_deref(),
        Some("fallback")
    );
    probe.teardown().await;
}

/// Compile-time guard: the sweep's job payload keeps the shape this file (and
/// the documented operator `INSERT`) drives. Cheap, and it fails at build time
/// rather than skipping silently when no DB is configured.
#[test]
fn the_job_payload_round_trips_both_cursor_states() {
    for kind in [
        JobKind::BackfillAtomTitles {
            after_atom_id: None,
        },
        JobKind::BackfillAtomTitles {
            after_atom_id: Some(Uuid::nil()),
        },
    ] {
        let v = serde_json::to_value(&kind).expect("serialise");
        assert_eq!(v["kind"], "BackfillAtomTitles", "tag must stay stable: {v}");
        let back: JobKind = serde_json::from_value(v).expect("round trip");
        assert!(matches!(back, JobKind::BackfillAtomTitles { .. }));
    }
}
