//! The attributed reef-digest read (`crate::reef_digest_read`) against
//! digests the real `RegenerateDigest` worker published under
//! `managed_enrichment`. Each reader runs in its own scoped transaction: the
//! consumer plane's app role, the tenant app role (NOBYPASSRLS) and a
//! Neon-shaped table-owner runtime. A digest is served with its attribution
//! only while every bound input still verifies for that reader; a restricted
//! coral, an edited, unlinked or retracted coral, a renamed reef, or a changed
//! edge fact withholds it and a regeneration converges back to a served one.
use super::*;
use crate::reef_digest_read::{self, DigestOutcome, ReaderDigest, Withheld};
use crate::tenancy::scope::begin_tenant_scoped;

async fn reader(f: &Fixture, pool: &PgPool, node: Uuid) -> ReaderDigest {
    let mut tx = match f.tenant {
        TenantId::Consumer => begin_account_scoped(pool, f.account).await.unwrap(),
        TenantId::Org(_) => begin_tenant_scoped(pool, f.account, f.payer).await.unwrap(),
    };
    let read = reef_digest_read::read_for_reader(&mut tx, node).await.unwrap().expect("reef visible to its member");
    tx.rollback().await.unwrap();
    read
}

fn withheld(read: &ReaderDigest) -> Option<Withheld> {
    match read.outcome {
        DigestOutcome::Unavailable(why) => Some(why),
        DigestOutcome::Served(_) => None,
    }
}

/// Both surfaces carry the same object: MCP `get_reef.digest` is the HTTP
/// body minus `cached`, and a withheld digest is `null` / `unavailable`.
fn assert_surfaces_agree(read: &ReaderDigest) {
    let mcp = read.mcp_value();
    let mut http = read.http_body(false);
    match &read.outcome {
        DigestOutcome::Served(_) => {
            assert_eq!(http["cached"], true);
            http.as_object_mut().unwrap().remove("cached");
            assert_eq!(http, mcp, "MCP and HTTP serve one digest object");
        }
        DigestOutcome::Unavailable(_) => {
            assert!(mcp.is_null(), "MCP digest is null when withheld");
            assert_eq!(http["unavailable"], true, "{http}");
            assert!(http["summary"].is_null() && http["excerpts"] == serde_json::json!([]));
            assert!(http.get("attribution").is_none() && http.get("reason").is_none(), "{http}");
        }
    }
}

/// One more coral naming the reef (`i` >= 8; `reef_with_corals` made 0..8).
async fn extra_coral(f: &Fixture, node: Uuid, i: usize) -> Uuid {
    let mut v = axis(0);
    v[i + 1] = 1.0;
    let v: Vec<f32> = v.into_iter().map(|x| x / (2.0f32).sqrt()).collect();
    let id: Uuid = sqlx::query_scalar(
        "INSERT INTO atoms(graph_id,source_type,source_id,content_blob_key,content_inline,timestamp,ingested_by,embedding_next) \
         VALUES($1,'note',$2,'fixture-only',$3,now(),$4,$5) RETURNING atom_id",
    )
    .bind(f.graph)
    .bind(format!("digest-{i}"))
    .bind(format!("Postgres fact {i}: the tenant database runs on Postgres."))
    .bind(f.payer)
    .bind(pgvector::Vector::from(v))
    .fetch_one(f.owner())
    .await
    .unwrap();
    sqlx::query("INSERT INTO node_atoms(node_id,atom_id) VALUES($1,$2)").bind(node).bind(id).execute(f.owner()).await.unwrap();
    id
}

/// The corals the current digest's receipt bound.
async fn bound_corals(f: &Fixture, node: Uuid) -> Vec<Uuid> {
    sqlx::query_scalar(
        "SELECT i.input_id FROM nodes n JOIN managed_enrichment_inputs i ON i.receipt_id=(n.body->'digest'->>'managed_receipt')::uuid \
          WHERE n.node_id=$1 AND i.input_kind='atom' ORDER BY i.ordinal",
    )
    .bind(node)
    .fetch_all(f.owner())
    .await
    .unwrap()
}

async fn restrict_source_of(f: &Fixture, atom: Uuid) {
    sqlx::query(
        "INSERT INTO source_items(graph_id,source_type,provider_namespace,source_id,access_state) \
         SELECT graph_id,source_type,'fixture:digest-read',source_id,'restricted' FROM atoms WHERE atom_id=$1",
    )
    .bind(atom)
    .execute(f.owner())
    .await
    .unwrap();
}

/// Queue a regeneration exactly as the surfaces do on a miss, and run one pass.
async fn regenerate_after_miss(f: &Fixture, pool: &PgPool, node: Uuid, read: &ReaderDigest, llm: &Script) {
    assert!(read.should_regenerate(), "an eligible withheld digest asks for a regeneration: {:?}", read.outcome);
    assert!(crate::workers::digest::queue_regeneration(Some(pool), node, read.graph_id, f.account).await);
    let pending: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM jobs WHERE kind='RegenerateDigest' AND status='pending' AND payload->>'node_id'=$1::text",
    )
    .bind(node)
    .fetch_one(f.owner())
    .await
    .unwrap();
    assert_eq!(pending, 1, "one deduped pending pass");
    assert_eq!(digest_tick(f, pool, llm).await, 1);
}

async fn served_digest_is_attributed_and_withheld_from_a_restricted_reader(org: bool, owner: bool) {
    let _env = Env::set(ON);
    let f = funded(org).await;
    let runtime = if owner { super::super::entity_resolution::owner_runtime(&f).await } else { f.app().clone() };
    let (node, _) = reef_with_corals(&f).await;
    extra_coral(&f, node, 8).await;
    let llm = Script::new();
    assert_eq!(digest_tick(&f, &runtime, &llm).await, 1);

    // A permitted reader is served the digest with its attribution.
    let read = reader(&f, &runtime, node).await;
    let DigestOutcome::Served(served) = &read.outcome else { panic!("permitted reader is served: {:?}", read.outcome) };
    assert_eq!(served.summary.as_deref(), Some("Postgres runs the tenant databases."));
    let (stored, _) = digest_of(&f, node).await;
    let stored = stored.unwrap();
    assert_eq!(serde_json::json!(served.attribution.receipt_id), stored["managed_receipt"]);
    let (model, at, bound): (Option<String>, DateTime<Utc>, i64) = sqlx::query_as(
        "SELECT r.served_model, r.settled_at, (SELECT count(*) FROM managed_enrichment_inputs i WHERE i.receipt_id=r.receipt_id AND i.input_kind='atom') \
           FROM managed_attempt_receipts r WHERE r.receipt_id=$1",
    )
    .bind(served.attribution.receipt_id)
    .fetch_one(f.owner())
    .await
    .unwrap();
    assert_eq!(Some(served.attribution.model.clone()), model);
    assert!(!served.attribution.model.is_empty());
    assert_eq!(served.attribution.generated_at, at);
    assert_eq!(served.attribution.input_count as i64, bound);
    assert!(bound >= 8, "the writer read the reef's corals: {bound}");
    let json = served.to_json();
    assert_eq!(json["attribution"]["receipt_id"], stored["managed_receipt"]);
    assert_eq!(json["attribution"]["input_count"], bound);
    assert_surfaces_agree(&read);

    // The reader loses permission to ONE bound coral (its source is now
    // restricted). The stored digest still names it, so an unverified read
    // would leak it; the attributed read withholds the whole digest.
    let lost = bound_corals(&f, node).await[0];
    restrict_source_of(&f, lost).await;
    let (still_stored, _) = digest_of(&f, node).await;
    assert!(still_stored.is_some(), "red control: the stored digest is still there to leak");
    assert!(bound_corals(&f, node).await.contains(&lost));
    let read = reader(&f, &runtime, node).await;
    assert_eq!(withheld(&read), Some(Withheld::NotPermitted));
    assert_surfaces_agree(&read);

    // Regeneration selects only permitted corals, so it converges to a
    // digest this reader is served.
    regenerate_after_miss(&f, &runtime, node, &read, &llm).await;
    assert!(!bound_corals(&f, node).await.contains(&lost), "a restricted coral is never selected again");
    let read = reader(&f, &runtime, node).await;
    assert!(matches!(read.outcome, DigestOutcome::Served(_)), "{:?}", read.outcome);
    assert_surfaces_agree(&read);
    drop(runtime);
    f.finish().await;
}

#[tokio::test]
async fn digest_read_attributed_and_permission_fenced_consumer() {
    served_digest_is_attributed_and_withheld_from_a_restricted_reader(false, false).await;
}
#[tokio::test]
async fn digest_read_attributed_and_permission_fenced_tenant_app_role() {
    served_digest_is_attributed_and_withheld_from_a_restricted_reader(true, false).await;
}
#[tokio::test]
async fn digest_read_attributed_and_permission_fenced_tenant_owner_runtime() {
    served_digest_is_attributed_and_withheld_from_a_restricted_reader(true, true).await;
}

/// An edited coral, a renamed reef, an unlinked coral and a retracted coral
/// each withhold the digest; a miss queues one regeneration that serves again.
#[tokio::test]
async fn digest_read_withholds_stale_or_invalidated_digests_and_regenerates_both_planes() {
    let _env = Env::set(ON);
    for org in [false, true] {
        let f = funded(org).await;
        let (node, _) = reef_with_corals(&f).await;
        // Two spare corals keep the reef eligible after one is unlinked and
        // one retracted.
        extra_coral(&f, node, 8).await;
        extra_coral(&f, node, 9).await;
        let llm = Script::new();
        assert_eq!(digest_tick(&f, f.app(), &llm).await, 1);
        assert!(matches!(reader(&f, f.app(), node).await.outcome, DigestOutcome::Served(_)));

        // 1. A bound coral is edited after generation.
        let edited = bound_corals(&f, node).await[1];
        sqlx::query("UPDATE atoms SET content_inline='Postgres fact, edited after the digest.' WHERE atom_id=$1")
            .bind(edited).execute(f.owner()).await.unwrap();
        let read = reader(&f, f.app(), node).await;
        assert_eq!(withheld(&read), Some(Withheld::Stale), "org={org}");
        assert_surfaces_agree(&read);
        regenerate_after_miss(&f, f.app(), node, &read, &llm).await;
        assert!(matches!(reader(&f, f.app(), node).await.outcome, DigestOutcome::Served(_)), "org={org}");

        // 2. The reef is renamed (still a Library-visible title).
        sqlx::query("UPDATE nodes SET title='Postgres fact' WHERE node_id=$1").bind(node).execute(f.owner()).await.unwrap();
        let read = reader(&f, f.app(), node).await;
        assert_eq!(withheld(&read), Some(Withheld::Stale), "org={org}");
        regenerate_after_miss(&f, f.app(), node, &read, &llm).await;
        assert!(matches!(reader(&f, f.app(), node).await.outcome, DigestOutcome::Served(_)), "org={org}");

        // 3. A bound coral is unlinked from the reef.
        let unlinked = bound_corals(&f, node).await[0];
        sqlx::query("DELETE FROM node_atoms WHERE node_id=$1 AND atom_id=$2").bind(node).bind(unlinked).execute(f.owner()).await.unwrap();
        let read = reader(&f, f.app(), node).await;
        assert_eq!(withheld(&read), Some(Withheld::Stale), "org={org}");
        assert_surfaces_agree(&read);
        regenerate_after_miss(&f, f.app(), node, &read, &llm).await;
        assert!(matches!(reader(&f, f.app(), node).await.outcome, DigestOutcome::Served(_)), "org={org}");

        // 4. A bound coral's evidence is retracted: the invalidation trigger
        //    strips the digest and queues the regeneration itself.
        let retracted = bound_corals(&f, node).await[0];
        sqlx::query("UPDATE atoms SET evidence_retracted=true WHERE atom_id=$1").bind(retracted).execute(f.owner()).await.unwrap();
        let read = reader(&f, f.app(), node).await;
        assert!(withheld(&read).is_some(), "org={org}: {:?}", read.outcome);
        assert_surfaces_agree(&read);
        let pending: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM jobs WHERE kind='RegenerateDigest' AND status='pending' AND payload->>'node_id'=$1::text",
        )
        .bind(node)
        .fetch_one(f.owner())
        .await
        .unwrap();
        assert_eq!(pending, 1, "org={org}: retraction queued one regeneration");
        assert_eq!(digest_tick(&f, f.app(), &llm).await, 1);
        assert!(!bound_corals(&f, node).await.contains(&retracted));
        assert!(matches!(reader(&f, f.app(), node).await.outcome, DigestOutcome::Served(_)), "org={org}");

        // Switched off: still withheld, nothing queued.
        sqlx::query("UPDATE atoms SET content_inline='Postgres, edited with digests off.' WHERE atom_id=$1")
            .bind(bound_corals(&f, node).await[0]).execute(f.owner()).await.unwrap();
        {
            let _off = Env::set(&[("DIGESTS_ENABLED", None)]);
            let read = reader(&f, f.app(), node).await;
            assert_eq!(withheld(&read), Some(Withheld::Stale));
            assert!(!read.should_regenerate(), "DIGESTS_ENABLED off queues nothing");
        }
        f.finish().await;
    }
}

/// The writer's prompt states established edges, naming the related reef.
/// Hiding that reef's title from the reader, or rejecting the edge, withholds
/// the digest; a digest from before #1352 (no receipt) is never served.
#[tokio::test]
async fn digest_read_fences_edge_facts_and_unattributed_digests_both_planes() {
    let _env = Env::set(ON);
    for org in [false, true] {
        let f = funded(org).await;
        let (node, _) = reef_with_corals(&f).await;
        let neon: Uuid = sqlx::query_scalar("INSERT INTO nodes(graph_id,type,title) VALUES($1,'entity','Neon') RETURNING node_id")
            .bind(f.graph).fetch_one(f.owner()).await.unwrap();
        let neon_coral: Uuid = sqlx::query_scalar(
            "INSERT INTO atoms(graph_id,source_type,source_id,content_blob_key,content_inline,timestamp,ingested_by) \
             VALUES($1,'note','neon-only','fixture-only','Neon hosts the branch databases.',now(),$2) RETURNING atom_id",
        )
        .bind(f.graph).bind(f.payer).fetch_one(f.owner()).await.unwrap();
        sqlx::query("INSERT INTO node_atoms(node_id,atom_id) VALUES($1,$2)").bind(neon).bind(neon_coral).execute(f.owner()).await.unwrap();
        let edge: Uuid = sqlx::query_scalar(
            "INSERT INTO relationship_edge(graph_id,src_node_id,predicate,dst_node_id,origin,status) VALUES($1,$2,'runs_on',$3,'inferred','confirmed') RETURNING edge_id",
        )
        .bind(f.graph).bind(node).bind(neon).fetch_one(f.owner()).await.unwrap();
        let llm = Script::new();
        assert_eq!(digest_tick(&f, f.app(), &llm).await, 1);
        let edges: Vec<Uuid> = sqlx::query_scalar(
            "SELECT i.input_id FROM nodes n JOIN managed_enrichment_inputs i ON i.receipt_id=(n.body->'digest'->>'managed_receipt')::uuid \
              WHERE n.node_id=$1 AND i.input_kind='digest_edge'",
        )
        .bind(node).fetch_all(f.owner()).await.unwrap();
        assert_eq!(edges, vec![edge], "org={org}: the stated edge line is a bound input");
        assert!(matches!(reader(&f, f.app(), node).await.outcome, DigestOutcome::Served(_)), "org={org}");

        // The related reef's only coral is restricted: its title is hidden,
        // so the digest's edge fact naming it is withheld.
        restrict_source_of(&f, neon_coral).await;
        let read = reader(&f, f.app(), node).await;
        assert!(withheld(&read).is_some(), "org={org}: {:?}", read.outcome);
        assert_surfaces_agree(&read);
        // Re-permit it (a DELETE would purge the source's corals).
        sqlx::query("UPDATE source_items SET access_state='permitted' WHERE provider_namespace='fixture:digest-read'").execute(f.owner()).await.unwrap();
        assert!(matches!(reader(&f, f.app(), node).await.outcome, DigestOutcome::Served(_)), "org={org}: restored access serves again");

        // The confirmed edge is demoted to suggested: no longer a fact.
        sqlx::query("UPDATE relationship_edge SET status='suggested' WHERE edge_id=$1").bind(edge).execute(f.owner()).await.unwrap();
        let read = reader(&f, f.app(), node).await;
        assert_eq!(withheld(&read), Some(Withheld::NotPermitted), "org={org}");
        sqlx::query("UPDATE relationship_edge SET status='confirmed' WHERE edge_id=$1").bind(edge).execute(f.owner()).await.unwrap();
        assert!(matches!(reader(&f, f.app(), node).await.outcome, DigestOutcome::Served(_)), "org={org}");

        // An unattributed (pre-#1352) digest, and one naming a foreign receipt.
        for digest in [
            serde_json::json!({"summary":"Legacy unattributed summary","excerpts":[]}),
            serde_json::json!({"summary":"Forged","excerpts":[],"managed_receipt":Uuid::new_v4()}),
        ] {
            sqlx::query("UPDATE nodes SET body=jsonb_set(body,'{digest}',$2) WHERE node_id=$1").bind(node).bind(&digest).execute(f.owner()).await.unwrap();
            let read = reader(&f, f.app(), node).await;
            assert_eq!(withheld(&read), Some(Withheld::Unattributed), "org={org}: {digest}");
            assert_surfaces_agree(&read);
        }
        // A real receipt grafted under excerpts that name a coral it never read.
        let receipt = bound_receipt(&f, node).await;
        sqlx::query("UPDATE nodes SET body=jsonb_set(body,'{digest}',$2) WHERE node_id=$1")
            .bind(node)
            .bind(serde_json::json!({"summary":"Grafted","excerpts":[{"atom_id":neon_coral,"excerpt":"Neon hosts"}],"managed_receipt":receipt}))
            .execute(f.owner()).await.unwrap();
        assert_eq!(withheld(&reader(&f, f.app(), node).await), Some(Withheld::Unattributed), "org={org}");
        f.finish().await;
    }
}

async fn bound_receipt(f: &Fixture, node: Uuid) -> Uuid {
    sqlx::query_scalar(
        "SELECT DISTINCT receipt_id FROM managed_enrichment_inputs WHERE input_kind='digest_node' AND input_id=$1 AND outcome='written'",
    )
    .bind(node)
    .fetch_one(f.owner())
    .await
    .unwrap()
}
